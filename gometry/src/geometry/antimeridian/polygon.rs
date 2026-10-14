use std::collections::HashMap;

use crate::error::Result;
use crate::geographic::{
    ChartConnection, ChartPole, GeodeticPole, GeographicFrame, GeographicRing, GeographicViewError,
    Latitude, RetainedChartDisplacement, RingDomain, RingSummary, ScalarGeographicView,
};
use crate::geometry::antimeridian::publication::SplitCapture;
use crate::geometry::antimeridian::{
    SeamRing, collect_points, ring_follows_seam, segment_coords_with_metric,
    segment_coords_with_occurrences, split_ring_at_seam_with_metric,
};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordSeqBuilder, CoordinateAxes, Coordinates as _, GeometryErrorKind,
    LineSeq, MOrdinate, MultiLineString, MultiMember, MultiPolygon, Point, Polygon, Ring,
    RootMetricState, Shape, ShapeOwnerKey, SourceArcMetricAccess, Strictness, TopologyDomain,
    ZOrdinate, interpolate_subdivision_ordinate, open_point_cycle_winding, polygon_is_valid,
    ring_encloses_pole, ring_winding, same_point,
};

mod assemble;
mod chart_events;
mod chart_shape;
mod split;

use chart_events::lineal_shape_has_certified_no_chart_events;
#[cfg(test)]
use chart_events::{
    certified_no_chart_events, chart_connection, line_has_certified_no_chart_events, seam_event,
};
pub(super) use chart_events::{chart_latitude, chart_point};
#[cfg(test)]
use chart_shape::{chart_line, chart_polygon, chart_ring};
use chart_shape::{chart_shape, map_xy_with_shared_owners};
use split::{polygon_parts_shape, split_polygon, split_polygon_member};

use super::lineage::{LineageCatalog, ShapeLineage};
use super::normalize;

type SplitLinePart = (LineSeq, Option<super::lineage::LineageSequence>);

#[derive(Default)]
struct ShapeTransformMemo {
    owners: Option<HashMap<ShapeOwnerKey, (Shape, Option<ShapeLineage>)>>,
}

impl ShapeTransformMemo {
    fn get(&self, owner: Option<ShapeOwnerKey>) -> Option<(Shape, Option<ShapeLineage>)> {
        owner.and_then(|owner| self.owners.as_ref()?.get(&owner).cloned())
    }

    fn insert(
        &mut self,
        owner: Option<ShapeOwnerKey>,
        shape: Shape,
        lineage: Option<ShapeLineage>,
    ) {
        if let Some(owner) = owner {
            self.owners
                .get_or_insert_with(HashMap::new)
                .insert(owner, (shape, lineage));
        }
    }
}

impl Shape {
    /// Validate and split in the periodic coordinate frame. Canonical chart
    /// coordinates exist only for this operation; source coordinates remain
    /// the storage authority.
    pub(crate) fn split_antimeridian_in_frame(&self, frame: GeographicFrame) -> Result<Self> {
        self.split_antimeridian_in_frame_with_metric::<crate::crs::GeodesicMetric>(frame, None)
    }

    pub(crate) fn split_antimeridian_in_crs84(&self) -> Result<Self> {
        let metric = std::sync::OnceLock::new();
        let metric_access = || {
            metric.get_or_try_init(|| {
                let model = crate::crs::metric_model(Some("OGC:CRS84"))?;
                crate::crs::GeodesicMetric::from_model(&model)
            })
        };
        self.split_antimeridian_in_frame_with_metric(GeographicFrame::crs84(), Some(&metric_access))
    }

    pub(crate) fn split_antimeridian_in_frame_with_metric<M: ChartMetric>(
        &self,
        frame: GeographicFrame,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<Self> {
        let view =
            ScalarGeographicView::try_new(frame, self).map_err(|error| split_view_error(&error))?;
        split_geographic_view(view, metric_access)
    }

    pub(crate) fn split_antimeridian_captured_in_crs84(
        &self,
    ) -> Result<super::publication::SplitDrawing> {
        let metric = std::sync::OnceLock::new();
        let metric_access = || {
            metric.get_or_try_init(|| {
                let model = crate::crs::metric_model(Some("OGC:CRS84"))?;
                crate::crs::GeodesicMetric::from_model(&model)
            })
        };
        self.split_antimeridian_captured_in_frame_with_metric(
            GeographicFrame::crs84(),
            Some(&metric_access),
        )
    }

    pub(crate) fn split_antimeridian_captured_in_frame_with_metric<M: ChartMetric>(
        &self,
        frame: GeographicFrame,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<super::publication::SplitDrawing> {
        let view =
            ScalarGeographicView::try_new(frame, self).map_err(|error| split_view_error(&error))?;
        split_geographic_view_captured(view, metric_access)
    }

    pub fn split_antimeridian(&self) -> Result<Self> {
        self.split_antimeridian_in_crs84()
    }

    /// Split at the antimeridian: parts that cross ±180 come back as
    /// multiple parts whose edges follow the seam (each side keeping its
    /// own seam sign), with affine chart crossings and automatic pole closure.
    /// Geometries that do not cross are returned unchanged.
    /// The caller supplies an already-admitted canonical operation chart.
    fn split_antimeridian_chart<M: ChartMetric>(
        &self,
        memo: &mut ShapeTransformMemo,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<Self> {
        split_shape_with_lineage(
            self,
            None,
            None,
            GeographicFrame::crs84(),
            memo,
            metric_access,
        )
        .map(|(shape, _)| shape)
    }

    /// Split at the antimeridian forcing closure over a specific pole, for
    /// callers that already know which pole a ring encircles. A region that
    /// *contains* a pole (e.g. a polar cell coverage) has a ring edge spanning
    /// `> 180°` of longitude that the automatic inference can close over the
    /// wrong pole — producing a valid but wrong-region polygon the `Auto` retry
    /// (which only re-tries on *invalid* output) never corrects. Non-areal and
    /// non-crossing geometries behave exactly as [`Self::split_antimeridian`].
    ///
    /// Contract: each part must genuinely touch the forced pole. The closure
    /// assumes the regular seam topology of a single pole-touching ring.
    /// Forcing the wrong pole, or a ring that does not reach the forced pole,
    /// is caller error and yields garbage.
    pub fn split_antimeridian_over_pole(&self, north: bool) -> Result<Self> {
        let closure = if north {
            PoleClosure::Chart(ChartPole::North)
        } else {
            PoleClosure::Chart(ChartPole::South)
        };
        match self {
            Self::Polygon(polygon) => {
                let polygons = split_polygon(
                    self,
                    polygon,
                    closure,
                    None::<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>>,
                )?;
                Ok(Self::narrow_polygons(polygon.axes(), polygons))
            },
            Self::MultiPolygon(polygons) => {
                let mut split = Vec::with_capacity(polygons.member_count());
                for member in polygons.members_with_values() {
                    match member {
                        MultiMember::Empty => split.push(MultiMember::Empty),
                        MultiMember::Value(polygon) => {
                            split.extend(
                                split_polygon(
                                    self,
                                    polygon,
                                    closure,
                                    None::<
                                        &SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>,
                                    >,
                                )?
                                .into_iter()
                                .map(MultiMember::Value),
                            );
                        },
                    }
                }
                Ok(Self::MultiPolygon(MultiPolygon::try_new(
                    polygons.axes(),
                    split,
                )?))
            },
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => self.split_antimeridian(),
        }
    }
}

fn line_part(
    points: &[Point],
    lineage: Option<super::lineage::LineageSequence>,
) -> Result<SplitLinePart> {
    Ok((
        LineSeq::from_proven(CoordSeq::try_from_points(points)?),
        lineage,
    ))
}

fn split_line_member<M: ChartMetric>(
    line: &LineSeq,
    source_lineage: Option<&super::lineage::LineageSequence>,
    catalog: Option<&mut LineageCatalog>,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Option<Vec<SplitLinePart>>> {
    if let Some(source_lineage) = source_lineage {
        let pieces = segment_coords_with_occurrences(
            &collect_points(line),
            GeographicFrame::crs84(),
            metric_access,
        )?;
        if pieces.is_empty() {
            return Ok(None);
        }
        let catalog = catalog.ok_or_else(|| {
            GeometryErrorKind::Invalid("captured line lineage has no event catalog".into())
        })?;
        let source_closed = line_sequence_is_closed(line);
        pieces
            .into_iter()
            .map(|piece| {
                let lineage =
                    split::seam_piece_lineage(&piece, source_lineage, source_closed, catalog)?;
                line_part(&piece.points, Some(lineage))
            })
            .collect::<Result<Vec<_>>>()
            .map(Some)
    } else {
        let pieces = segment_coords_with_metric(
            &collect_points(line),
            GeographicFrame::crs84(),
            metric_access,
        )?;
        if pieces.is_empty() {
            return Ok(None);
        }
        pieces
            .into_iter()
            .map(|piece| line_part(&piece, None))
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }
}

fn split_multiline<M: ChartMetric>(
    lines: &MultiLineString,
    lineage: Option<&ShapeLineage>,
    mut catalog: Option<&mut LineageCatalog>,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<(Shape, Option<ShapeLineage>)> {
    let (axes, source_members) = match lineage {
        None => (lines.axes(), None),
        Some(ShapeLineage::MultiLineString { members }) => {
            if lines.member_count() != members.len() {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian line members and lineage disagree".into(),
                )
                .into());
            }
            (lines.axes(), Some(members.as_slice()))
        },
        Some(_) => {
            return Err(GeometryErrorKind::Invalid(
                "antimeridian shape and member lineage disagree".into(),
            )
            .into());
        },
    };
    let mut members = Vec::with_capacity(lines.member_count());
    let mut output_lineages = source_members.map(|_| Vec::with_capacity(lines.member_count()));
    for (index, member) in lines.members_with_values().enumerate() {
        match member {
            MultiMember::Empty => {
                members.push(MultiMember::Empty);
                if let Some(lineages) = &mut output_lineages {
                    lineages.push(None);
                }
            },
            MultiMember::Value(line) => {
                let source_lineage = match source_members {
                    Some(source_members) => Some(
                        source_members
                            .get(index)
                            .and_then(Option::as_ref)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "antimeridian line members and lineage disagree".into(),
                                )
                            })?,
                    ),
                    None => None,
                };
                match split_line_member(
                    line,
                    source_lineage,
                    catalog.as_deref_mut(),
                    metric_access,
                )? {
                    None => {
                        members.push(MultiMember::Value(line.clone()));
                        if let Some(lineages) = &mut output_lineages {
                            lineages.push(source_lineage.cloned());
                        }
                    },
                    Some(pieces) => {
                        for (line, lineage) in pieces {
                            members.push(MultiMember::Value(line));
                            if let Some(lineages) = &mut output_lineages {
                                lineages.push(lineage);
                            }
                        }
                    },
                }
            },
        }
    }
    let shape = Shape::MultiLineString(MultiLineString::try_new(axes, members)?);
    let lineage = output_lineages.map(|members| ShapeLineage::MultiLineString { members });
    Ok((shape, lineage))
}

fn split_shape_with_lineage<M: ChartMetric>(
    shape: &Shape,
    lineage: Option<&ShapeLineage>,
    mut catalog: Option<&mut LineageCatalog>,
    source_frame: GeographicFrame,
    memo: &mut ShapeTransformMemo,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<(Shape, Option<ShapeLineage>)> {
    match (shape, lineage) {
        (Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..), lineage) => {
            Ok((shape.clone(), lineage.cloned()))
        },
        (Shape::LineString(line), lineage) => {
            let source = match lineage {
                None => None,
                Some(ShapeLineage::LineString(source)) => Some(source),
                Some(_) => {
                    return Err(GeometryErrorKind::Invalid(
                        "antimeridian line and lineage disagree".into(),
                    )
                    .into());
                },
            };
            let Some(pieces) =
                split_line_member(line, source, catalog.as_deref_mut(), metric_access)?
            else {
                return Ok((shape.clone(), lineage.cloned()));
            };
            let axes = line.as_coords().axes();
            let captured = source.is_some();
            let mut output_lines = Vec::with_capacity(pieces.len());
            let mut output_lineages = Vec::with_capacity(pieces.len());
            for (line, lineage) in pieces {
                output_lines.push(line);
                if captured {
                    output_lineages.push(Some(lineage.ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "captured split line has no member lineage".into(),
                        )
                    })?));
                }
            }
            let shape = Shape::multi_line_string(axes, output_lines);
            let lineage = captured.then_some(ShapeLineage::MultiLineString {
                members: output_lineages,
            });
            Ok((shape, lineage))
        },
        (Shape::MultiLineString(lines), lineage) => {
            split_multiline(lines, lineage, catalog.as_deref_mut(), metric_access)
        },
        (Shape::Polygon(polygon), lineage) => {
            let source = match lineage {
                None => None,
                Some(ShapeLineage::Polygon(source)) => Some(source),
                Some(_) => {
                    return Err(GeometryErrorKind::Invalid(
                        "antimeridian polygon and lineage disagree".into(),
                    )
                    .into());
                },
            };
            let parts = split_polygon_member(
                shape,
                polygon,
                source,
                source_frame,
                metric_access,
                catalog.as_deref_mut(),
            )?;
            polygon_parts_shape(parts, polygon.axes(), source.is_some())
        },
        (Shape::MultiPolygon(polygons), lineage) => {
            let (axes, source_members) = match lineage {
                None => (polygons.axes(), None),
                Some(ShapeLineage::MultiPolygon { members }) => {
                    if polygons.member_count() != members.len() {
                        return Err(GeometryErrorKind::Invalid(
                            "antimeridian polygon members and lineage disagree".into(),
                        )
                        .into());
                    }
                    (polygons.axes(), Some(members.as_slice()))
                },
                Some(_) => {
                    return Err(GeometryErrorKind::Invalid(
                        "antimeridian shape and member lineage disagree".into(),
                    )
                    .into());
                },
            };
            let mut split = Vec::with_capacity(polygons.member_count());
            let mut output_lineages =
                source_members.map(|_| Vec::with_capacity(polygons.member_count()));
            for (index, member) in polygons.members_with_values().enumerate() {
                match member {
                    MultiMember::Empty => {
                        split.push(MultiMember::Empty);
                        if let Some(lineages) = &mut output_lineages {
                            lineages.push(None);
                        }
                    },
                    MultiMember::Value(polygon) => {
                        let source = match source_members {
                            Some(source_members) => Some(
                                source_members
                                    .get(index)
                                    .and_then(Option::as_ref)
                                    .ok_or_else(|| {
                                        GeometryErrorKind::Invalid(
                                            "antimeridian polygon members and lineage disagree"
                                                .into(),
                                        )
                                    })?,
                            ),
                            None => None,
                        };
                        for (polygon, lineage) in split_polygon_member(
                            shape,
                            polygon,
                            source,
                            source_frame,
                            metric_access,
                            catalog.as_deref_mut(),
                        )? {
                            split.push(MultiMember::Value(polygon));
                            if let Some(lineages) = &mut output_lineages {
                                lineages.push(Some(lineage.ok_or_else(|| {
                                    GeometryErrorKind::Invalid(
                                        "captured split polygon has no member lineage".into(),
                                    )
                                })?));
                            }
                        }
                    },
                }
            }
            let shape = Shape::MultiPolygon(MultiPolygon::try_new(axes, split)?);
            let lineage = output_lineages.map(|members| ShapeLineage::MultiPolygon { members });
            Ok((shape, lineage))
        },
        (Shape::GeometryCollection(collection), lineage) => {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            let source_members = match lineage {
                None => None,
                Some(ShapeLineage::GeometryCollection(members)) => {
                    if collection.len() != members.len() {
                        return Err(GeometryErrorKind::Invalid(
                            "antimeridian collection members and lineage disagree".into(),
                        )
                        .into());
                    }
                    Some(members.as_slice())
                },
                Some(_) => {
                    return Err(GeometryErrorKind::Invalid(
                        "antimeridian shape and member lineage disagree".into(),
                    )
                    .into());
                },
            };
            if let Some((shape, cached_lineage)) = memo.get(owner) {
                match (source_members, cached_lineage) {
                    (None, _) => return Ok((shape, None)),
                    (Some(_), Some(lineage)) => return Ok((shape, Some(lineage))),
                    (Some(_), None) => {},
                }
            }
            let mut members = Vec::with_capacity(collection.len());
            let mut lineages = source_members.map(|_| Vec::with_capacity(collection.len()));
            for (index, member) in collection.iter().enumerate() {
                let source = source_members.map(|source_members| {
                    source_members
                        .get(index)
                        .expect("collection and lineage member counts were checked")
                });
                let (member, lineage) = split_shape_with_lineage(
                    member,
                    source,
                    catalog.as_deref_mut(),
                    source_frame,
                    memo,
                    metric_access,
                )?;
                members.push(member);
                if let Some(lineages) = &mut lineages {
                    lineages.push(lineage.ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "captured collection member has no split lineage".into(),
                        )
                    })?);
                }
            }
            let shape = Shape::geometry_collection(members)?;
            let lineage = lineages.map(ShapeLineage::GeometryCollection);
            memo.insert(owner, shape.clone(), lineage.clone());
            Ok((shape, lineage))
        },
    }
}

fn line_sequence_is_closed(line: &LineSeq) -> bool {
    let Some(last_index) = line.len().checked_sub(1) else {
        return false;
    };
    if last_index == 0 {
        return false;
    }
    let first = line.point_at(0);
    let last = line.point_at(last_index);
    first.x.to_bits() == last.x.to_bits()
        && first.y.to_bits() == last.y.to_bits()
        && first.z() == last.z()
        && first.m() == last.m()
}

fn split_view_error(error: &GeographicViewError) -> crate::error::Error {
    GeometryErrorKind::antimeridian_split_failed(error.to_string())
}

fn source_longitudes_are_canonical(view: ScalarGeographicView<'_>) -> bool {
    let frame = view.frame();
    let mut canonical = true;
    view.shape().for_each_first_xy_point_run(|point| {
        let native = point.x;
        let wrapped = frame
            .longitude()
            .canonicalize(point.x)
            .expect("geographic view admitted every longitude")
            .get();
        let positive_seam = native.to_bits() == frame.longitude().half_period().to_bits();
        let ordinary_identity = native.to_bits() == wrapped.to_bits()
            || (native.to_bits() == (-0.0_f64).to_bits() && wrapped.to_bits() == 0.0_f64.to_bits());
        canonical &= positive_seam || ordinary_identity;
    });
    canonical
}

pub(crate) fn split_geographic_view<M: ChartMetric>(
    view: ScalarGeographicView<'_>,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    split_geographic_view_with_capture(view, metric_access, None).map(|(shape, _)| shape)
}

fn split_geographic_view_captured<M: ChartMetric>(
    view: ScalarGeographicView<'_>,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<super::publication::SplitDrawing> {
    let frame = view.frame();
    let longitude_scale = frame.longitude().to_degrees(1.0) * frame.longitude_orientation_sign();
    let latitude_scale =
        frame.latitude_unit().native_to_degrees(1.0) * frame.latitude_orientation_sign();
    let mut capture = SplitCapture::enabled();
    let (shape, lineage) =
        split_geographic_view_with_capture(view, metric_access, Some(&mut capture))?;
    let lineage = lineage.expect("captured antimeridian split has member lineage");
    Ok(capture.into_drawing(shape, frame, longitude_scale, latitude_scale, lineage))
}

#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn split_geographic_view_with_capture<M: ChartMetric>(
    view: ScalarGeographicView<'_>,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    mut capture: Option<&mut SplitCapture>,
) -> Result<(Shape, Option<ShapeLineage>)> {
    let frame = view.frame();
    let canonical = source_longitudes_are_canonical(view);
    if frame == GeographicFrame::crs84() && canonical && !view.shape().crosses_antimeridian() {
        let lineage = if let Some(capture) = capture.as_deref_mut() {
            capture.mark_identity();
            Some(capture.identity_lineage(view.shape())?)
        } else {
            None
        };
        return Ok((view.shape().clone(), lineage));
    }
    let longitude_scale = frame.longitude().to_degrees(1.0) * frame.longitude_orientation_sign();
    let latitude_scale =
        frame.latitude_unit().native_to_degrees(1.0) * frame.latitude_orientation_sign();
    if canonical && lineal_shape_has_certified_no_chart_events(view.shape(), frame, longitude_scale)
    {
        let lineage = if let Some(capture) = capture.as_deref_mut() {
            capture.mark_identity();
            Some(capture.identity_lineage(view.shape())?)
        } else {
            None
        };
        return Ok((view.shape().clone(), lineage));
    }
    let (chart, chart_lineage) = chart_shape(
        view.shape(),
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
        capture.as_deref_mut(),
        &mut ShapeTransformMemo::default(),
    )?;
    if canonical
        && !crate::geometry::crosses_antimeridian_in_frame(view.shape(), frame)
        && !chart.crosses_antimeridian()
    {
        let lineage = if let Some(capture) = capture.as_deref_mut() {
            capture.mark_identity();
            Some(capture.identity_lineage(view.shape())?)
        } else {
            None
        };
        return Ok((view.shape().clone(), lineage));
    }
    let (split, lineage) = if let (Some(capture), Some(lineage)) = (capture, chart_lineage.as_ref())
    {
        split_shape_with_lineage(
            &chart,
            Some(lineage),
            Some(capture.lineage_mut()),
            frame,
            &mut ShapeTransformMemo::default(),
            metric_access,
        )?
    } else {
        (
            chart.split_antimeridian_chart(&mut ShapeTransformMemo::default(), metric_access)?,
            None,
        )
    };
    let shape = map_xy_with_shared_owners(
        &split,
        &|x, y| {
            let latitude = if y == 90.0 {
                frame.native_latitude_pole(GeodeticPole::North)
            } else if y == -90.0 {
                frame.native_latitude_pole(GeodeticPole::South)
            } else {
                y / latitude_scale
            };
            Ok((x / longitude_scale, latitude))
        },
        &mut ShapeTransformMemo::default(),
    )?;
    Ok((shape, lineage))
}

/// Which native-chart pole a stitched ring may close over.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PoleClosure {
    Auto,
    Chart(ChartPole),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HoleOverlayMode {
    Select,
    Quotient,
    Chart,
}

#[cfg(test)]
mod tests;
