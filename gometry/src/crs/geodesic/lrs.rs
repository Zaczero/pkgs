use crate::Finite;
use crate::crs::RuntimeState;
use crate::crs::geodesic::{
    DistanceMode, EllipsoidMetric, Result, ensure_geographic_coordseq_in_frame,
    ensure_geographic_lonlat_in_frame, geographic_metric_frame_at, normalize, with_geodesic_at,
    with_geographic_metric_erased_at,
};
use crate::geometry::{
    ChartLineIndex, CoordSeq, FrameDependentCaches, GeographicPrepared, MeasureRange, Point,
    PreparedPlan, Shape,
};

/// Interpolate a point by ellipsoidal arclength along `shape`'s chart edges.
pub(crate) fn chart_line_interpolate(
    crs: &str,
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    distance: f64,
    distance_mode: impl Into<DistanceMode>,
    state: &RuntimeState,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    let frame = geographic_metric_frame_at(crs, state)?;
    GeographicPrepared::try_new(shape, frame_cache, frame)?;
    let distance = Finite::try_new("distance", distance)?.get();
    let crs = normalize(crs)?;
    with_geodesic_at(&crs, state, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, state.generation, frame);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index = shape.chart_line_index(
            frame_cache,
            &crs,
            semi_major,
            flattening,
            state.generation,
            &metric,
        )?;
        Ok(Shape::Point(index.interpolate_route_position(
            distance,
            distance_mode.is_normalized(),
            &metric,
        )?))
    })
}

/// Chart-edge linear-referencing sub-line between `start` and `end`
/// (meters, or `[0, 1]` fractions when `normalized`).
pub(crate) fn chart_line_substring(
    crs: &str,
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    range: MeasureRange,
    distance_mode: impl Into<DistanceMode>,
    state: &RuntimeState,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    let frame = geographic_metric_frame_at(crs, state)?;
    GeographicPrepared::try_new(shape, frame_cache, frame)?;
    let crs = normalize(crs)?;
    with_geodesic_at(&crs, state, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, state.generation, frame);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index = shape.chart_line_index(
            frame_cache,
            &crs,
            semi_major,
            flattening,
            state.generation,
            &metric,
        )?;
        index.substring(range, distance_mode.is_normalized(), &metric)
    })
}

/// Chart-edge linear-referencing inverse: along-track distance (meters, or
/// a `[0, 1]` fraction when `normalized`) to the projection of `point`.
pub(crate) fn chart_line_locate(
    crs: &str,
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    point: Point,
    distance_mode: impl Into<DistanceMode>,
    state: &RuntimeState,
) -> Result<f64> {
    let distance_mode = distance_mode.into();
    let frame = geographic_metric_frame_at(crs, state)?;
    GeographicPrepared::try_new(shape, frame_cache, frame)?;
    if !point.x.is_nan() {
        ensure_geographic_lonlat_in_frame(frame, point.x, point.y)?;
    }
    let crs = normalize(crs)?;
    with_geodesic_at(&crs, state, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, state.generation, frame);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index = shape.chart_line_index(
            frame_cache,
            &crs,
            semi_major,
            flattening,
            state.generation,
            &metric,
        )?;
        index.locate_chart_point(point, distance_mode.is_normalized(), &metric)
    })
}

pub(crate) fn with_geographic_coordseq_metric_at<T>(
    crs: &str,
    state: &RuntimeState,
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    with_geographic_metric_erased_at(crs, &[], state, |error| error, |_, metric| f(metric))
}

pub(crate) fn with_geographic_coordseq_collect_rows_at<T>(
    crs: &str,
    state: &RuntimeState,
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<Vec<T>, (usize, crate::error::Error)>,
) -> Result<Vec<T>, (usize, crate::error::Error)> {
    with_geographic_metric_erased_at(crs, &[], state, |error| (0, error), |_, metric| f(metric))
}

/// Packed-line column kernel for chart-edge `line_interpolate_point`: one
/// [`ChartLineIndex::build_chart_coordseq`] per row — no `ShapeData` wrapper.
pub(crate) fn chart_line_interpolate_coordseq(
    line: &CoordSeq,
    distance: f64,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<Point> {
    let distance_mode = distance_mode.into();
    ensure_geographic_coordseq_in_frame(metric.frame(), line)?;
    let distance = Finite::try_new("distance", distance)?.get();
    let index = ChartLineIndex::build_chart_coordseq(line, metric)?;
    index.interpolate_route_position(distance, distance_mode.is_normalized(), metric)
}

/// Packed-line column kernel for chart-edge `line_substring`.
pub(crate) fn chart_line_substring_coordseq(
    line: &CoordSeq,
    range: MeasureRange,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    ensure_geographic_coordseq_in_frame(metric.frame(), line)?;
    let index = ChartLineIndex::build_chart_coordseq(line, metric)?;
    index.substring(range, distance_mode.is_normalized(), metric)
}

/// Packed-line column kernel for chart-edge `line_locate_point`.
pub(crate) fn chart_line_locate_coordseq(
    line: &CoordSeq,
    query: Point,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<f64> {
    let distance_mode = distance_mode.into();
    ensure_geographic_coordseq_in_frame(metric.frame(), line)?;
    if !query.x.is_nan() {
        ensure_geographic_lonlat_in_frame(metric.frame(), query.x, query.y)?;
    }
    let index = ChartLineIndex::build_chart_coordseq(line, metric)?;
    index.locate_chart_point(query, distance_mode.is_normalized(), metric)
}

/// CRS-aware `interpolate_m`: resolved geographic metrics use ellipsoidal
/// chart-edge arclength, while planar metrics use 2D coordinate distance,
/// including an explicit planar override on a geographic CRS. Unlike the
/// constructive ops this NEVER reprojects — X/Y/Z stay exact; only edge lengths
/// feed the stationing.
pub(crate) fn metric_interpolate_m(
    model: &crate::crs::MetricModel,
    shape: &Shape,
    range: crate::geometry::MeasureRange,
    overwrite: bool,
) -> Result<Shape> {
    match model {
        // The M fraction (traveled / total) is scale-invariant, so planar
        // stationing uses raw 2D distance regardless of `to_metre`.
        crate::crs::MetricModel::Planar { .. } => shape.interpolate_m(range, overwrite, |a, b| {
            Ok(crate::geometry::point_distance(a, b))
        }),
        crate::crs::MetricModel::Geodesic {
            crs: geodesic_crs,
            state,
            ..
        } => crate::crs::with_ellipsoid_metric_at(geodesic_crs, &[shape], state, |metric| {
            use crate::geometry::ChartMetric as _;
            shape.interpolate_m(range, overwrite, |a, b| {
                let edge = metric.chart_edge(a, b)?;
                metric.chart_edge_length(&edge)
            })
        }),
    }
}
