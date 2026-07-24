use super::*;
use crate::Finite;
use crate::geometry::{
    CoordSeq, FrameDependentCaches, LineIndex, MeasureRange, Point, Shape, ShapeData,
};

/// Exact geodesic linear-referencing on `crs`'s ellipsoid: interpolate a point
/// at `distance` (meters, or a `[0, 1]` fraction when `normalized`) along
/// `shape`. Consistent with [`geodesic_length`].
pub(crate) fn geodesic_line_interpolate(
    crs: &str,
    shape: &ShapeData,
    frame_cache: &FrameDependentCaches,
    distance: f64,
    distance_mode: impl Into<DistanceMode>,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    ensure_geodesic_lonlat_crs(crs)?;
    ensure_geographic_domain(shape)?;
    let distance = Finite::try_new("distance", distance)?.get();
    let crs = normalize(crs)?;
    with_geodesic(&crs, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic(geodesic);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index =
            shape.geodesic_line_index(frame_cache, &crs, semi_major, flattening, &metric)?;
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
    shape: &ShapeData,
    frame_cache: &FrameDependentCaches,
    range: MeasureRange,
    distance_mode: impl Into<DistanceMode>,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    ensure_geodesic_lonlat_crs(crs)?;
    ensure_geographic_domain(shape)?;
    let crs = normalize(crs)?;
    with_geodesic(&crs, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic(geodesic);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index =
            shape.geodesic_line_index(frame_cache, &crs, semi_major, flattening, &metric)?;
        index.substring(range, distance_mode.is_normalized(), &metric)
    })
}

/// Exact geodesic linear-referencing inverse: along-track distance (meters, or
/// a `[0, 1]` fraction when `normalized`) to the projection of `point`.
pub(crate) fn geodesic_line_locate(
    crs: &str,
    shape: &ShapeData,
    frame_cache: &FrameDependentCaches,
    point: Point,
    distance_mode: impl Into<DistanceMode>,
) -> Result<f64> {
    let distance_mode = distance_mode.into();
    ensure_geodesic_lonlat_crs(crs)?;
    ensure_geographic_domain(shape)?;
    ensure_geographic_lonlat(point.x, point.y)?;
    let crs = normalize(crs)?;
    with_geodesic(&crs, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic(geodesic);
        let (semi_major, flattening) = metric.ellipsoid_parameters();
        let index =
            shape.geodesic_line_index(frame_cache, &crs, semi_major, flattening, &metric)?;
        Ok(index.locate_point(point, distance_mode.is_normalized(), &metric))
    })
}

/// Internal entry for geodesic metric helpers: geographic CRS check, optional
/// input lon/lat domain validation, thread-cache refresh, CRS normalization,
/// and cached [`Geodesic`] lookup — then invoke `f` with the normalized CRS
/// string and ellipsoid metric.
pub(crate) fn with_geodesic_erased<T, E>(
    crs: &str,
    shapes: &[&Shape],
    map_err: impl Fn(crate::error::Error) -> E,
    f: impl FnOnce(&str, &EllipsoidMetric<'_>) -> Result<T, E>,
) -> Result<T, E> {
    ensure_geodesic_lonlat_crs(crs).map_err(&map_err)?;
    for shape in shapes {
        ensure_geographic_domain(shape).map_err(&map_err)?;
    }
    let crs = normalize(crs).map_err(&map_err)?;
    with_geodesic_cache(&crs, map_err, |geodesic| {
        let metric = EllipsoidMetric::for_geodesic(geodesic);
        f(&crs, &metric)
    })
}

/// Resolve the ellipsoid metric once for packed-line LRS batch lanes (CRS
/// validation + geodesic cache lookup happen outside the per-row loop).
pub(crate) fn with_geodesic_coordseq_metric<T>(
    crs: &str,
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    with_geodesic_erased(crs, &[], |error| error, |_, metric| f(metric))
}

/// [`with_geodesic_coordseq_metric`] sibling for [`CollectRows`] batch lanes.
pub(crate) fn with_geodesic_coordseq_collect_rows<T>(
    crs: &str,
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<Vec<T>, (usize, crate::error::Error)>,
) -> Result<Vec<T>, (usize, crate::error::Error)> {
    with_geodesic_erased(crs, &[], |error| (0, error), |_, metric| f(metric))
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
    ensure_geographic_coordseq(line)?;
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
    ensure_geographic_coordseq(line)?;
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
    ensure_geographic_coordseq(line)?;
    ensure_geographic_lonlat(query.x, query.y)?;
    let index = LineIndex::build_coordseq(line, metric)?;
    Ok(index.locate_point(query, distance_mode.is_normalized(), metric))
}
