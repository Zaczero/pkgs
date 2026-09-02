//! Ellipsoidal geodesic math (geographiclib): inverse/direct/interpolate,
//! area/length, certified segment distance and intersection, the geodesic LRS,
//! and the `MetricModel` that decides planar-vs-geodesic. The geodesic instance
//! cache (`with_geodesic`) reaches its thread-local storage in the parent via
//! `super`.

pub(crate) use geographiclib_rs::{DirectGeodesic, InverseGeodesic};
use geographiclib_rs::{Geodesic, GeodesicLine, geodesic_capability as caps};

use crate::collections::{HashMap, HashMapExt as _};
use crate::crs::{
    CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY, CachedGeodesicObject, CrsError, CrsInfo,
    GeodesicDirectColumns, GeodesicDirectInfo, GeodesicInterpolateInfo, GeodesicInverseInfo,
    GeometryErrorKind, Point, ensure_same_geodesic_len, ensure_thread_caches_current, lru_resolve,
    normalize,
};
use crate::error::Result;

const GEODESIC_LINE_CAPS: u64 = caps::LATITUDE
    | caps::LONGITUDE
    | caps::AZIMUTH
    | caps::REDUCEDLENGTH
    | caps::GEODESICSCALE
    | caps::DISTANCE_IN;
const GEODESIC_LINE_OUTMASK: u64 =
    caps::LATITUDE | caps::LONGITUDE | caps::AZIMUTH | caps::REDUCEDLENGTH | caps::GEODESICSCALE;
const GEODESIC_LINE_CACHE_CAPACITY: usize = 4096;
const REDUCED_LAT_TABLE_CELLS: usize = 8192;
const REDUCED_LAT_MIN_ONE_MINUS_F: f64 = 0.9;

#[cfg(test)]
mod geodesic_counters {
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, Default)]
    pub(super) struct Snapshot {
        pub line_cache_hits: u64,
        pub line_cache_misses: u64,
    }

    thread_local! {
        static LINE_CACHE_HITS: Cell<u64> = const { Cell::new(0) };
        static LINE_CACHE_MISSES: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn line_cache_hit() {
        LINE_CACHE_HITS.set(LINE_CACHE_HITS.get() + 1);
    }

    pub(super) fn line_cache_miss() {
        LINE_CACHE_MISSES.set(LINE_CACHE_MISSES.get() + 1);
    }

    pub(super) fn reset() {
        LINE_CACHE_HITS.set(0);
        LINE_CACHE_MISSES.set(0);
    }

    pub(super) fn snapshot() -> Snapshot {
        Snapshot {
            line_cache_hits: LINE_CACHE_HITS.get(),
            line_cache_misses: LINE_CACHE_MISSES.get(),
        }
    }
}

#[cfg(test)]
macro_rules! count_line_cache_hit {
    () => {
        geodesic_counters::line_cache_hit();
    };
}

#[cfg(not(test))]
macro_rules! count_line_cache_hit {
    () => {};
}

#[cfg(test)]
macro_rules! count_line_cache_miss {
    () => {
        geodesic_counters::line_cache_miss();
    };
}

#[cfg(not(test))]
macro_rules! count_line_cache_miss {
    () => {};
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct GeodesicLineKey {
    semi_major_bits: u64,
    flattening_bits: u64,
    start_lon_bits: u64,
    start_lat_bits: u64,
    azimuth_bits: u64,
    caps: u64,
}

thread_local! {
    static GEODESIC_LINE_CACHE: std::cell::RefCell<HashMap<GeodesicLineKey, GeodesicLine>> =
        std::cell::RefCell::new(HashMap::new());
}

mod bearing;
mod bulk;
mod cache;
mod distance;
mod domain;
mod ellipsoid_metric;
mod lower_bound;
mod measure;
mod metric_model;
mod nearest_lrs;
mod segment;
#[cfg(test)]
mod tests;

pub(crate) use bearing::{
    EllipsoidShape, ellipsoid_shape_at, finite, geodesic_bearing, geodesic_bearing_crs_at,
    geodesic_cross_track_crs_at, geodesic_cross_track_radius_crs_at,
    geodesic_cross_track_with_radius, geodesic_destination, geodesic_destination_crs_at,
    geodesic_for_crs_at, geodesic_point_between,
};
pub(crate) use bulk::{
    geodesic_direct, geodesic_direct_columns, geodesic_interpolates, geodesic_inverse,
    geodesic_inverses, geodesic_length_at,
};
pub(crate) use cache::{with_geodesic, with_geodesic_at, with_geodesic_cache_at};
pub(crate) use distance::{
    geodesic_frechet_at, geodesic_hausdorff_at, geodesic_point_distances,
    geodesic_point_pair_distance, geodesic_point_pair_distances, geodesic_point_pair_dwithin,
    geodesic_point_pair_dwithin_one,
};
pub(crate) use domain::{
    ensure_geodesic_lonlat_crs_at, ensure_geographic_columns_in_frame,
    ensure_geographic_coordseq_in_frame, ensure_geographic_domain_in_frame,
    ensure_geographic_lonlat, ensure_geographic_lonlat_in_frame, ensure_latitudes_in_domain,
};
pub(crate) use ellipsoid_metric::{
    EllipsoidMetric, inverse_azimuths, inverse_distance, inverse_distance_azimuths,
};
pub(crate) use lower_bound::interpolate_optional_ordinate;
use lower_bound::{LowerBoundKernel, cached_lower_bound_kernel};
#[cfg(test)]
pub(crate) use measure::with_ellipsoid_metric;
pub(crate) use measure::{
    AngleUnit, DistanceMode, endpoints_are_repeated, geodesic_direct_on_ellipsoid,
    geodesic_direct_on_ellipsoid_const, geodesic_interpolate_on_line_const,
    geodesic_inverse_on_ellipsoid, geodesic_inverse_on_ellipsoid_const,
    geodesic_line_length_columns, geodesic_line_solution_const, geodesic_ring_measure_columns,
    geodesic_shape_area, geodesic_shape_length, with_ellipsoid_metric_at,
    with_resolved_ellipsoid_metric_at,
};
pub(crate) use metric_model::{
    GeodesicMetric, MetricModel, ResolvedMetric, ensure_3d_metric, geographic_metric_frame_at,
    metric_model, metric_model_meters,
};
pub(crate) use nearest_lrs::{
    geodesic_line_interpolate, geodesic_line_interpolate_coordseq, geodesic_line_locate,
    geodesic_line_locate_coordseq, geodesic_line_substring, geodesic_line_substring_coordseq,
    with_geodesic_coordseq_collect_rows_at, with_geodesic_coordseq_metric_at,
    with_geodesic_erased_at,
};
use segment::{
    IntersectSolver, checked_geodesic_segment, geodesic_foot_on_segment,
    geodesic_locate_on_segment, geodesic_point_to_segment, geodesic_point_within_segment,
    geodesic_segment_intersection_with_solver,
};
