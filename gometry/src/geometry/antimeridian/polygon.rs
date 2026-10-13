use std::collections::HashMap;

use crate::error::Result;
use crate::geographic::{
    ChartConnection, ChartPole, GeodeticPole, GeographicFrame, GeographicRing, GeographicViewError,
    Latitude, RetainedChartDisplacement, RingDomain, RingSummary, ScalarGeographicView,
};
use crate::geometry::antimeridian::publication::SplitCapture;
use crate::geometry::antimeridian::{
    SeamRing, collect_points, ring_follows_seam, segment_coords_with_metric,
    split_ring_at_seam_with_metric,
};
use crate::geometry::ordinates::{SourceArcRequest, source_arc_ordinates};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordSeqBuilder, CoordinateAxes, Coordinates as _, GeometryErrorKind,
    LineSeq, MOrdinate, MultiLineString, MultiMember, MultiPolygon, Point, Polygon, Ring,
    RootMetricState, Shape, ShapeOwnerKey, SourceArcMetricAccess, Strictness, TopologyDomain,
    Unmetered, ZOrdinate, interpolate_subdivision_ordinate, open_point_cycle_winding,
    polygon_is_valid, ring_encloses_pole, ring_winding, same_point,
};

mod assemble;
mod chart_events;
mod chart_shape;
mod split;

#[cfg(test)]
use chart_events::{
    certified_no_chart_events, chart_connection, line_has_certified_no_chart_events, seam_event,
};
use chart_events::{chart_latitude, lineal_shape_has_certified_no_chart_events};
#[cfg(test)]
use chart_shape::{chart_line, chart_polygon, chart_ring};
use chart_shape::{chart_shape, map_xy_with_shared_owners};
use split::{split_polygon, split_polygon_with_lineage};

use super::lineage::{LineageCatalog, LineageSequence, MemberId, ShapeLineage};
use super::normalize;

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
        if frame == GeographicFrame::crs84() {
            let metric = std::sync::OnceLock::new();
            let metric_access = || {
                metric.get_or_try_init(|| {
                    let model = crate::crs::metric_model(Some("OGC:CRS84"))?;
                    crate::crs::GeodesicMetric::from_model(&model)
                })
            };
            return self.split_antimeridian_in_frame_with_metric(frame, Some(&metric_access));
        }
        self.split_antimeridian_in_frame_with_metric::<crate::crs::GeodesicMetric>(frame, None)
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

    pub(crate) fn split_antimeridian_captured_in_frame(
        &self,
        frame: GeographicFrame,
    ) -> Result<super::publication::SplitDrawing> {
        if frame == GeographicFrame::crs84() {
            let metric = std::sync::OnceLock::new();
            let metric_access = || {
                metric.get_or_try_init(|| {
                    let model = crate::crs::metric_model(Some("OGC:CRS84"))?;
                    crate::crs::GeodesicMetric::from_model(&model)
                })
            };
            return self
                .split_antimeridian_captured_in_frame_with_metric(frame, Some(&metric_access));
        }
        self.split_antimeridian_captured_in_frame_with_metric::<crate::crs::GeodesicMetric>(
            frame, None,
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
        self.split_antimeridian_in_frame(GeographicFrame::crs84())
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
        (Shape::LineString(line), None) => {
            let pieces = segment_coords_with_metric(
                &collect_points(line),
                GeographicFrame::crs84(),
                metric_access,
            )?;
            if pieces.is_empty() {
                return Ok((shape.clone(), None));
            }
            let lines = pieces
                .into_iter()
                .map(|piece| CoordSeq::try_from_points(&piece))
                .map(|coords| coords.map(LineSeq::from_proven))
                .collect::<Result<Vec<_>>>()?;
            Ok((
                Shape::multi_line_string(line.as_coords().axes(), lines),
                None,
            ))
        },
        (Shape::LineString(line), Some(ShapeLineage::LineString(source))) => {
            let pieces = segment_coords_with_metric(
                &collect_points(line),
                GeographicFrame::crs84(),
                metric_access,
            )?;
            if pieces.is_empty() {
                return Ok((shape.clone(), lineage.cloned()));
            }
            let member = source.member;
            let lines = pieces
                .into_iter()
                .map(|piece| CoordSeq::try_from_points(&piece))
                .map(|coords| coords.map(LineSeq::from_proven))
                .collect::<Result<Vec<_>>>()?;
            let lineages = lines
                .iter()
                .map(|line| Some(empty_lineage(member, line.len())))
                .collect();
            Ok((
                Shape::multi_line_string(line.as_coords().axes(), lines),
                Some(ShapeLineage::MultiLineString {
                    axes: line.as_coords().axes(),
                    members: lineages,
                }),
            ))
        },
        (Shape::MultiLineString(lines), None) => {
            let mut split = Vec::with_capacity(lines.member_count());
            for member in lines.members_with_values() {
                match member {
                    MultiMember::Empty => split.push(MultiMember::Empty),
                    MultiMember::Value(line) => {
                        let pieces = segment_coords_with_metric(
                            &collect_points(line),
                            GeographicFrame::crs84(),
                            metric_access,
                        )?;
                        if pieces.is_empty() {
                            split.push(MultiMember::Value(line.clone()));
                        } else {
                            split.extend(
                                pieces
                                    .into_iter()
                                    .map(|piece| CoordSeq::try_from_points(&piece))
                                    .map(|coords| coords.map(LineSeq::from_proven))
                                    .map(|line| line.map(MultiMember::Value))
                                    .collect::<Result<Vec<_>>>()?,
                            );
                        }
                    },
                }
            }
            Ok((
                Shape::MultiLineString(MultiLineString::try_new(lines.axes(), split)?),
                None,
            ))
        },
        (
            Shape::MultiLineString(lines),
            Some(ShapeLineage::MultiLineString {
                axes,
                members: source_members,
            }),
        ) => {
            let mut split = Vec::with_capacity(lines.member_count());
            let mut lineages = Vec::with_capacity(lines.member_count());
            for (source, source_lineage) in lines.members_with_values().zip(source_members) {
                match (source, source_lineage) {
                    (MultiMember::Empty, None) => {
                        split.push(MultiMember::Empty);
                        lineages.push(None);
                    },
                    (MultiMember::Value(line), Some(source_lineage)) => {
                        let pieces = segment_coords_with_metric(
                            &collect_points(line),
                            GeographicFrame::crs84(),
                            metric_access,
                        )?;
                        if pieces.is_empty() {
                            split.push(MultiMember::Value(line.clone()));
                            lineages.push(Some(source_lineage.clone()));
                        } else {
                            for piece in pieces {
                                let line = LineSeq::from_proven(CoordSeq::try_from_points(&piece)?);
                                lineages
                                    .push(Some(empty_lineage(source_lineage.member, line.len())));
                                split.push(MultiMember::Value(line));
                            }
                        }
                    },
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "antimeridian line members and lineage disagree".into(),
                        )
                        .into());
                    },
                }
            }
            Ok((
                Shape::MultiLineString(MultiLineString::try_new(*axes, split)?),
                Some(ShapeLineage::MultiLineString {
                    axes: *axes,
                    members: lineages,
                }),
            ))
        },
        (Shape::Polygon(polygon), None) => {
            let parts = split_polygon(shape, polygon, PoleClosure::Auto, metric_access)?;
            Ok((Shape::narrow_polygons(polygon.axes(), parts), None))
        },
        (Shape::Polygon(polygon), Some(ShapeLineage::Polygon(source))) => {
            let catalog = catalog.as_deref_mut().ok_or_else(|| {
                GeometryErrorKind::Invalid("captured polygon lineage has no event catalog".into())
            })?;
            let parts = split_polygon_with_lineage(
                shape,
                polygon,
                source,
                PoleClosure::Auto,
                HoleOverlayMode::Select,
                source_frame,
                metric_access,
                catalog,
            )?;
            let lineages = parts
                .iter()
                .map(|part| Some(part.lineage.clone()))
                .collect::<Vec<_>>();
            let polygons = parts
                .into_iter()
                .map(|part| part.polygon)
                .collect::<Vec<_>>();
            let lineage = match lineages.as_slice() {
                [] => ShapeLineage::Empty,
                [Some(lineage)] => ShapeLineage::Polygon(lineage.clone()),
                _ => ShapeLineage::MultiPolygon {
                    axes: polygon.axes(),
                    members: lineages,
                },
            };
            Ok((
                Shape::narrow_polygons(polygon.axes(), polygons),
                Some(lineage),
            ))
        },
        (Shape::MultiPolygon(polygons), None) => {
            let mut split = Vec::with_capacity(polygons.member_count());
            for member in polygons.members_with_values() {
                match member {
                    MultiMember::Empty => split.push(MultiMember::Empty),
                    MultiMember::Value(polygon) => {
                        split.extend(
                            split_polygon(shape, polygon, PoleClosure::Auto, metric_access)?
                                .into_iter()
                                .map(MultiMember::Value),
                        );
                    },
                }
            }
            Ok((
                Shape::MultiPolygon(MultiPolygon::try_new(polygons.axes(), split)?),
                None,
            ))
        },
        (
            Shape::MultiPolygon(polygons),
            Some(ShapeLineage::MultiPolygon {
                axes,
                members: source_members,
            }),
        ) => {
            let mut split = Vec::with_capacity(polygons.member_count());
            let mut lineages = Vec::with_capacity(polygons.member_count());
            for (source, source_lineage) in polygons.members_with_values().zip(source_members) {
                match (source, source_lineage) {
                    (MultiMember::Empty, None) => {
                        split.push(MultiMember::Empty);
                        lineages.push(None);
                    },
                    (MultiMember::Value(polygon), Some(source_lineage)) => {
                        let catalog = catalog.as_deref_mut().ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "captured multipolygon lineage has no event catalog".into(),
                            )
                        })?;
                        let parts = split_polygon_with_lineage(
                            shape,
                            polygon,
                            source_lineage,
                            PoleClosure::Auto,
                            HoleOverlayMode::Select,
                            source_frame,
                            metric_access,
                            catalog,
                        )?;
                        for part in parts {
                            lineages.push(Some(part.lineage));
                            split.push(MultiMember::Value(part.polygon));
                        }
                    },
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "antimeridian polygon members and lineage disagree".into(),
                        )
                        .into());
                    },
                }
            }
            Ok((
                Shape::MultiPolygon(MultiPolygon::try_new(*axes, split)?),
                Some(ShapeLineage::MultiPolygon {
                    axes: *axes,
                    members: lineages,
                }),
            ))
        },
        (Shape::GeometryCollection(collection), Some(ShapeLineage::GeometryCollection(source))) => {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some((shape, Some(lineage))) = memo.get(owner) {
                return Ok((shape, Some(lineage)));
            }
            if collection.len() != source.len() {
                return Err(GeometryErrorKind::Invalid(
                    "antimeridian collection members and lineage disagree".into(),
                )
                .into());
            }
            let mut members = Vec::with_capacity(collection.len());
            let mut lineages = Vec::with_capacity(collection.len());
            for (member, lineage) in collection.iter().zip(source) {
                let lineage = lineage.as_ref().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "antimeridian collection member has no lineage".into(),
                    )
                })?;
                let (member, lineage) = split_shape_with_lineage(
                    member,
                    Some(lineage),
                    catalog.as_deref_mut(),
                    source_frame,
                    memo,
                    metric_access,
                )?;
                members.push(member);
                lineages.push(lineage);
            }
            let shape = Shape::geometry_collection(members)?;
            let lineage = ShapeLineage::GeometryCollection(lineages);
            memo.insert(owner, shape.clone(), Some(lineage.clone()));
            Ok((shape, Some(lineage)))
        },
        (Shape::GeometryCollection(collection), None) => {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some((shape, _)) = memo.get(owner) {
                return Ok((shape, None));
            }
            let mut members = Vec::with_capacity(collection.len());
            for member in collection {
                members.push(
                    split_shape_with_lineage(
                        member,
                        None,
                        catalog.as_deref_mut(),
                        source_frame,
                        memo,
                        metric_access,
                    )?
                    .0,
                );
            }
            let shape = Shape::geometry_collection(members)?;
            memo.insert(owner, shape.clone(), None);
            Ok((shape, None))
        },
        _ => Err(GeometryErrorKind::Invalid(
            "antimeridian shape and member lineage disagree".into(),
        )
        .into()),
    }
}

fn empty_lineage(member: MemberId, coordinate_count: usize) -> LineageSequence {
    LineageSequence {
        member,
        vertices: vec![Vec::new(); coordinate_count],
        edges: Vec::new(),
    }
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
    let (split, lineage) = if let (Some(capture), Some(lineage)) =
        (capture.as_deref_mut(), chart_lineage.as_ref())
    {
        capture.capture_source_edges(&chart);
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
