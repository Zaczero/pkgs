use crate::Finite;
use crate::crs::RuntimeState;
use crate::crs::geodesic::{
    DistanceMode, EllipsoidMetric, Result, ensure_geographic_coordseq_in_frame,
    ensure_geographic_domain_in_frame, ensure_geographic_lonlat_in_frame,
    geographic_metric_frame_at, normalize, with_geodesic_at, with_geodesic_cache_at,
};
use crate::geometry::{
    CoordSeq, FrameDependentCaches, LineIndex, MeasureRange, Point, PreparedPlan, Shape,
};

/// Exact geodesic linear-referencing on `crs`'s ellipsoid: interpolate a point
/// at `distance` (meters, or a `[0, 1]` fraction when `normalized`) along
/// `shape`. Consistent with [`geodesic_length`].
pub(crate) fn geodesic_line_interpolate(
    crs: &str,
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    distance: f64,
    distance_mode: impl Into<DistanceMode>,
    state: &RuntimeState,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    let frame = geographic_metric_frame_at(crs, state)?;
    ensure_geographic_domain_in_frame(frame, shape)?;
    let distance = Finite::try_new("distance", distance)?.get();
    let crs = normalize(crs)?;
    with_geodesic_at(&crs, &state, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, state.generation, frame);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index = shape.geodesic_line_index(
            frame_cache,
            &crs,
            semi_major,
            flattening,
            state.generation,
            &metric,
        )?;
        Ok(Shape::Point(index.interpolate(
            distance,
            distance_mode.is_normalized(),
            &metric,
        )))
    })
}

/// Exact geodesic linear-referencing sub-line between `start` and `end`
/// (meters, or `[0, 1]` fractions when `normalized`).
pub(crate) fn geodesic_line_substring(
    crs: &str,
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    range: MeasureRange,
    distance_mode: impl Into<DistanceMode>,
    state: &RuntimeState,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    let frame = geographic_metric_frame_at(crs, state)?;
    ensure_geographic_domain_in_frame(frame, shape)?;
    let crs = normalize(crs)?;
    with_geodesic_at(&crs, &state, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, state.generation, frame);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index = shape.geodesic_line_index(
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

/// Exact geodesic linear-referencing inverse: along-track distance (meters, or
/// a `[0, 1]` fraction when `normalized`) to the projection of `point`.
pub(crate) fn geodesic_line_locate(
    crs: &str,
    shape: &PreparedPlan,
    frame_cache: &FrameDependentCaches,
    point: Point,
    distance_mode: impl Into<DistanceMode>,
    state: &RuntimeState,
) -> Result<f64> {
    let distance_mode = distance_mode.into();
    let frame = geographic_metric_frame_at(crs, state)?;
    ensure_geographic_domain_in_frame(frame, shape)?;
    ensure_geographic_lonlat_in_frame(frame, point.x, point.y)?;
    let crs = normalize(crs)?;
    with_geodesic_at(&crs, &state, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, state.generation, frame);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index = shape.geodesic_line_index(
            frame_cache,
            &crs,
            semi_major,
            flattening,
            state.generation,
            &metric,
        )?;
        index.locate_point(point, distance_mode.is_normalized(), &metric)
    })
}

pub(crate) fn with_geodesic_erased_at<T, E>(
    crs: &str,
    shapes: &[&Shape],
    state: &RuntimeState,
    map_err: impl Fn(crate::error::Error) -> E,
    f: impl FnOnce(&str, &EllipsoidMetric<'_>) -> Result<T, E>,
) -> Result<T, E> {
    let frame = geographic_metric_frame_at(crs, state).map_err(&map_err)?;
    for shape in shapes {
        ensure_geographic_domain_in_frame(frame, shape).map_err(&map_err)?;
    }
    let crs = normalize(crs).map_err(&map_err)?;
    with_geodesic_cache_at(&crs, state, map_err, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, state.generation, frame);
        f(&crs, &metric)
    })
}

pub(crate) fn with_geodesic_coordseq_metric_at<T>(
    crs: &str,
    state: &RuntimeState,
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    with_geodesic_erased_at(crs, &[], state, |error| error, |_, metric| f(metric))
}

pub(crate) fn with_geodesic_coordseq_collect_rows_at<T>(
    crs: &str,
    state: &RuntimeState,
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<Vec<T>, (usize, crate::error::Error)>,
) -> Result<Vec<T>, (usize, crate::error::Error)> {
    with_geodesic_erased_at(crs, &[], state, |error| (0, error), |_, metric| f(metric))
}

/// Packed-line column kernel for geodesic `line_interpolate_point`: one
/// [`LineIndex::build_coordseq`] per row — no `ShapeData` wrapper.
pub(crate) fn geodesic_line_interpolate_coordseq(
    line: &CoordSeq,
    distance: f64,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<Point> {
    let distance_mode = distance_mode.into();
    ensure_geographic_coordseq_in_frame(metric.frame(), line)?;
    let distance = Finite::try_new("distance", distance)?.get();
    let index = LineIndex::build_coordseq(line, metric)?;
    Ok(index.interpolate(distance, distance_mode.is_normalized(), metric))
}

/// Packed-line column kernel for geodesic `line_substring`.
pub(crate) fn geodesic_line_substring_coordseq(
    line: &CoordSeq,
    range: MeasureRange,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    ensure_geographic_coordseq_in_frame(metric.frame(), line)?;
    let index = LineIndex::build_coordseq(line, metric)?;
    index.substring(range, distance_mode.is_normalized(), metric)
}

/// Packed-line column kernel for geodesic `line_locate_point`.
pub(crate) fn geodesic_line_locate_coordseq(
    line: &CoordSeq,
    query: Point,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<f64> {
    let distance_mode = distance_mode.into();
    ensure_geographic_coordseq_in_frame(metric.frame(), line)?;
    ensure_geographic_lonlat_in_frame(metric.frame(), query.x, query.y)?;
    let index = LineIndex::build_coordseq(line, metric)?;
    index.locate_point(query, distance_mode.is_normalized(), metric)
}
