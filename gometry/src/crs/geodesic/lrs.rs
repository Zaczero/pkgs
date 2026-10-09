use crate::Finite;
use crate::crs::RuntimeState;
use crate::crs::geodesic::{
    DistanceMode, EllipsoidMetric, Result, ensure_geographic_lonlat_in_frame,
    with_geographic_metric_erased_at,
};
use crate::geometry::{ChartLineIndex, CoordSeq, MeasureRange, Point, Shape};

pub(crate) fn with_geographic_coordseq_metric_at<T>(
    crs: &str,
    state: &RuntimeState,
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    #[cfg(test)]
    crate::test_support::linear_reference_counters::geographic_metric_scope();
    with_geographic_metric_erased_at(crs, &[], state, |error| error, |_, metric| f(metric))
}

pub(crate) fn chart_line_interpolate_coordseq_admitted(
    line: &CoordSeq,
    distance: f64,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<Point> {
    let distance_mode = distance_mode.into();
    let distance = Finite::try_new("distance", distance)?.get();
    #[cfg(test)]
    crate::test_support::linear_reference_counters::coordseq_chart_index_build();
    let index = ChartLineIndex::build_chart_coordseq(line, metric)?;
    index.interpolate_route_position(distance, distance_mode.is_normalized(), metric)
}

pub(crate) fn chart_line_substring_coordseq_admitted(
    line: &CoordSeq,
    range: MeasureRange,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<Shape> {
    let distance_mode = distance_mode.into();
    #[cfg(test)]
    crate::test_support::linear_reference_counters::coordseq_chart_index_build();
    let index = ChartLineIndex::build_chart_coordseq(line, metric)?;
    index.substring(range, distance_mode.is_normalized(), metric)
}

pub(crate) fn chart_line_locate_coordseq_admitted(
    line: &CoordSeq,
    query: Point,
    distance_mode: impl Into<DistanceMode>,
    metric: &EllipsoidMetric<'_>,
) -> Result<f64> {
    let distance_mode = distance_mode.into();
    if !query.x.is_nan() {
        ensure_geographic_lonlat_in_frame(metric.frame(), query.x, query.y)?;
    }
    #[cfg(test)]
    crate::test_support::linear_reference_counters::coordseq_chart_index_build();
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
