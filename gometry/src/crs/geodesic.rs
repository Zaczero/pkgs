//! Geographic metric math: GeographicLib point-to-point geodesics and the
//! shared chart-edge area/length integrals, with the `MetricModel` that selects
//! each CRS-aware operation. The geodesic instance cache (`with_geodesic`)
//! reaches its thread-local storage in the parent via `super`.

pub(crate) use geographiclib_rs::{DirectGeodesic, InverseGeodesic};

use crate::crs::{
    CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY, CachedGeodesicObject, CrsError, CrsInfo,
    GeodesicDirectInfo, ensure_thread_caches_current, lru_insert, lru_promote, normalize,
};
use crate::error::Result;

mod bearing;
mod cache;
mod chart_edge;
mod distance;
mod domain;
mod edge_distance;
mod ellipsoid_metric;
mod envelope;
mod lrs;
mod measure;
mod metric_model;
mod route;
#[cfg(test)]
mod tests;

pub(crate) use bearing::{
    EllipsoidShape, GeodesicCalculation, ellipsoid_shape_at, finite, geodesic_bearing,
    geodesic_cross_track_crs_at, geodesic_cross_track_radius_crs_at,
    geodesic_cross_track_with_radius, geodesic_destination, geodesic_destination_crs_at,
    geodesic_for_crs_at, geodesic_point_between,
};
pub(crate) use cache::{with_geodesic_at, with_geodesic_cache_at, with_geodesic_chart_cache_at};
pub(crate) use chart_edge::{AuthalicSeries, ChartEdge};
#[cfg(test)]
pub(crate) use chart_edge::{
    QuadratureTestProgress, quadrature_test_progress, reset_quadrature_test_progress,
};
pub(crate) use distance::{
    geodesic_frechet_at_with_scratch, geodesic_hausdorff_at, geodesic_point_distances,
    geodesic_point_pair_distance, geodesic_point_pair_distances, geodesic_point_pair_dwithin,
    geodesic_point_pair_dwithin_one,
};
pub(crate) use domain::{
    ensure_geodesic_lonlat_crs_at, ensure_geographic_columns_in_frame,
    ensure_geographic_coordseq_in_frame, ensure_geographic_domain_in_frame,
    ensure_geographic_lonlat, ensure_geographic_lonlat_in_frame, ensure_geographic_row_in_frame,
    geographic_lonlat_result,
};
pub(crate) use edge_distance::PointEdgeScratch;
pub(crate) use ellipsoid_metric::{
    EllipsoidMetric, inverse_azimuths, inverse_distance, inverse_distance_azimuths,
};
pub(crate) use envelope::{GeodesicBoxEnvelopeBound, GeodesicChartBox, GeodesicEnvelopeBound};
pub(crate) use lrs::{
    chart_line_interpolate, chart_line_interpolate_coordseq, chart_line_locate,
    chart_line_locate_coordseq, chart_line_substring, chart_line_substring_coordseq,
    metric_interpolate_m, with_geographic_coordseq_collect_rows_at,
    with_geographic_coordseq_metric_at,
};
#[cfg(test)]
pub(crate) use measure::with_ellipsoid_metric;
pub(crate) use measure::{
    DistanceMode, chart_line_length_columns, chart_shape_area, chart_shape_length,
    geodesic_interpolate_on_line, geodesic_witness_distance_at, with_ellipsoid_metric_at,
    with_resolved_ellipsoid_metric_at,
};
pub(crate) use metric_model::{
    GeodesicMetric, MetricModel, ResolvedMetric, ensure_3d_metric, geographic_metric_frame_at,
    metric_model, metric_model_at, metric_model_meters, metric_model_meters_at,
    with_geographic_metric_erased_at,
};
pub(crate) use route::{checked_geodesic_route, sincos_degrees};
