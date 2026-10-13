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
    ZOrdinate, interpolate_subdivision_ordinate, open_point_cycle_winding, polygon_is_valid,
    ring_encloses_pole, ring_winding, same_point,
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
use split::split_polygon;

use super::normalize;

#[derive(Default)]
struct ShapeTransformMemo {
    owners: Option<HashMap<ShapeOwnerKey, Shape>>,
}

impl ShapeTransformMemo {
    fn get(&self, owner: Option<ShapeOwnerKey>) -> Option<Shape> {
        owner.and_then(|owner| self.owners.as_ref()?.get(&owner).cloned())
    }

    fn insert(&mut self, owner: Option<ShapeOwnerKey>, shape: Shape) {
        if let Some(owner) = owner {
            self.owners
                .get_or_insert_with(HashMap::new)
                .insert(owner, shape);
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
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Ok(self.clone()),
            Self::LineString(points) => {
                let pieces = segment_coords_with_metric(
                    &collect_points(points),
                    GeographicFrame::crs84(),
                    metric_access,
                )?;
                if pieces.is_empty() {
                    Ok(self.clone())
                } else {
                    Ok(Self::multi_line_string(
                        points.axes(),
                        pieces
                            .into_iter()
                            .map(|piece| CoordSeq::try_from_points(&piece))
                            // Splitting a line at meridian crossings yields
                            // pieces of at least two vertices each.
                            .map(|coords| coords.map(LineSeq::from_proven))
                            .collect::<Result<Vec<_>>>()?,
                    ))
                }
            },
            Self::MultiLineString(lines) => {
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
                                let lines = pieces
                                    .into_iter()
                                    .map(|piece| CoordSeq::try_from_points(&piece))
                                    .map(|coords| coords.map(LineSeq::from_proven))
                                    .map(|line| line.map(MultiMember::Value))
                                    .collect::<Result<Vec<_>>>()?;
                                split.extend(lines);
                            }
                        },
                    }
                }
                Ok(Self::MultiLineString(MultiLineString::try_new(
                    lines.axes(),
                    split,
                )?))
            },
            Self::Polygon(polygon) => {
                let polygons = split_polygon(self, polygon, PoleClosure::Auto, metric_access)?;
                Ok(Self::narrow_polygons(polygon.axes(), polygons))
            },
            Self::MultiPolygon(polygons) => {
                let mut split = Vec::with_capacity(polygons.member_count());
                for member in polygons.members_with_values() {
                    match member {
                        MultiMember::Empty => split.push(MultiMember::Empty),
                        MultiMember::Value(polygon) => {
                            split.extend(
                                split_polygon(self, polygon, PoleClosure::Auto, metric_access)?
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
            Self::GeometryCollection(geometries) => {
                let owner = geometries
                    .members_are_shared()
                    .then(|| ShapeOwnerKey::new(geometries.as_slice()));
                if let Some(split) = memo.get(owner) {
                    return Ok(split);
                }
                let split = Self::GeometryCollection(geometries.try_map_members(|member| {
                    member.split_antimeridian_chart(memo, metric_access)
                })?);
                memo.insert(owner, split.clone());
                Ok(split)
            },
        }
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
    split_geographic_view_with_capture(view, metric_access, None)
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
    let shape = split_geographic_view_with_capture(view, metric_access, Some(&mut capture))?;
    Ok(capture.into_drawing(shape, frame, longitude_scale, latitude_scale))
}

#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
fn split_geographic_view_with_capture<M: ChartMetric>(
    view: ScalarGeographicView<'_>,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    mut capture: Option<&mut SplitCapture>,
) -> Result<Shape> {
    let frame = view.frame();
    let canonical = source_longitudes_are_canonical(view);
    if frame == GeographicFrame::crs84() && canonical && !view.shape().crosses_antimeridian() {
        if let Some(capture) = capture.as_deref_mut() {
            capture.mark_identity();
        }
        return Ok(view.shape().clone());
    }
    let longitude_scale = frame.longitude().to_degrees(1.0) * frame.longitude_orientation_sign();
    let latitude_scale =
        frame.latitude_unit().native_to_degrees(1.0) * frame.latitude_orientation_sign();
    if canonical && lineal_shape_has_certified_no_chart_events(view.shape(), frame, longitude_scale)
    {
        if let Some(capture) = capture.as_deref_mut() {
            capture.mark_identity();
        }
        return Ok(view.shape().clone());
    }
    let chart = chart_shape(
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
        if let Some(capture) = capture.as_deref_mut() {
            capture.mark_identity();
        }
        return Ok(view.shape().clone());
    }
    if let Some(capture) = capture {
        capture.capture_source_edges(&chart);
    }
    let split =
        chart.split_antimeridian_chart(&mut ShapeTransformMemo::default(), metric_access)?;
    map_xy_with_shared_owners(
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
    )
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
