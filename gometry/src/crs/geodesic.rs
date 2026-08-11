//! Ellipsoidal geodesic math (geographiclib): inverse/direct/interpolate,
//! area/length, point-to-segment Newton (golden fallback), the geodesic LRS,
//! and the `MetricModel` that decides planar-vs-geodesic. The geodesic instance
//! cache (`with_geodesic`) reaches its thread-local storage in the parent via
//! `super`.

pub(crate) use geographiclib_rs::{DirectGeodesic, InverseGeodesic};
use geographiclib_rs::{Geodesic, GeodesicLine, geodesic_capability as caps};

use crate::collections::{HashMap, HashMapExt as _};
use crate::crs::{
    CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY, CachedGeodesicObject, CrsError, CrsInfo,
    DEGREE_TO_RADIAN, GOLDEN_RATIO, GOLDEN_SECTION_TOLERANCE_METRES, GeodesicDirectColumns,
    GeodesicDirectInfo, GeodesicInterpolateInfo, GeodesicInverseInfo, GeometryErrorKind, Point,
    ensure_same_geodesic_len, ensure_thread_caches_current, info, lru_resolve, normalize,
};
use crate::error::Result;

const NEWTON_MAX_ITERATIONS: usize = 12;
const GEODESIC_LINE_CAPS: u64 =
    caps::LATITUDE | caps::LONGITUDE | caps::AZIMUTH | caps::DISTANCE_IN;
const GEODESIC_LINE_OUTMASK: u64 = caps::LATITUDE | caps::LONGITUDE | caps::AZIMUTH;
const GEODESIC_LINE_CACHE_CAPACITY: usize = 4096;
const REDUCED_LAT_TABLE_CELLS: usize = 8192;
const REDUCED_LAT_MIN_ONE_MINUS_F: f64 = 0.9;
const MIN_REDUCED_LENGTH_METRES: f64 = 1e-12;
const GOLDEN_FALLBACK_TOLERANCE_METRES: f64 = 1e-4;
const GOLDEN_FALLBACK_ITERATIONS: u32 = 96;

#[cfg(test)]
mod geodesic_counters {
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, Default)]
    pub(super) struct Snapshot {
        pub endpoint_inverses: u64,
        pub newton_inverses: u64,
        pub fallback_golden_probes: u64,
        pub line_cache_hits: u64,
        pub line_cache_misses: u64,
    }

    thread_local! {
        static ENDPOINT_INVERSES: Cell<u64> = const { Cell::new(0) };
        static NEWTON_INVERSES: Cell<u64> = const { Cell::new(0) };
        static FALLBACK_GOLDEN_PROBES: Cell<u64> = const { Cell::new(0) };
        static LINE_CACHE_HITS: Cell<u64> = const { Cell::new(0) };
        static LINE_CACHE_MISSES: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn endpoint_inverses(count: u64) {
        ENDPOINT_INVERSES.set(ENDPOINT_INVERSES.get() + count);
    }

    pub(super) fn newton_inverse() {
        NEWTON_INVERSES.set(NEWTON_INVERSES.get() + 1);
    }

    pub(super) fn fallback_golden_probe() {
        FALLBACK_GOLDEN_PROBES.set(FALLBACK_GOLDEN_PROBES.get() + 1);
    }

    pub(super) fn line_cache_hit() {
        LINE_CACHE_HITS.set(LINE_CACHE_HITS.get() + 1);
    }

    pub(super) fn line_cache_miss() {
        LINE_CACHE_MISSES.set(LINE_CACHE_MISSES.get() + 1);
    }

    pub(super) fn reset() {
        ENDPOINT_INVERSES.set(0);
        NEWTON_INVERSES.set(0);
        FALLBACK_GOLDEN_PROBES.set(0);
        LINE_CACHE_HITS.set(0);
        LINE_CACHE_MISSES.set(0);
    }

    pub(super) fn snapshot() -> Snapshot {
        Snapshot {
            endpoint_inverses: ENDPOINT_INVERSES.get(),
            newton_inverses: NEWTON_INVERSES.get(),
            fallback_golden_probes: FALLBACK_GOLDEN_PROBES.get(),
            line_cache_hits: LINE_CACHE_HITS.get(),
            line_cache_misses: LINE_CACHE_MISSES.get(),
        }
    }
}

#[cfg(test)]
macro_rules! count_endpoint_inverses {
    ($count:expr) => {
        geodesic_counters::endpoint_inverses($count);
    };
}

#[cfg(not(test))]
macro_rules! count_endpoint_inverses {
    ($count:expr) => {};
}

#[cfg(test)]
macro_rules! count_newton_inverse {
    () => {
        geodesic_counters::newton_inverse();
    };
}

#[cfg(not(test))]
macro_rules! count_newton_inverse {
    () => {};
}

#[cfg(test)]
macro_rules! count_fallback_golden_probe {
    () => {
        geodesic_counters::fallback_golden_probe();
    };
}

#[cfg(not(test))]
macro_rules! count_fallback_golden_probe {
    () => {};
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
    EllipsoidShape, ellipsoid_shape, finite, geodesic_bearing, geodesic_bearing_crs,
    geodesic_cross_track_crs, geodesic_cross_track_radius_crs, geodesic_cross_track_with_radius,
    geodesic_destination, geodesic_destination_crs, geodesic_for_crs, geodesic_point_between,
};
pub(crate) use bulk::{
    geodesic_direct, geodesic_direct_columns, geodesic_interpolates, geodesic_inverse,
    geodesic_inverses, geodesic_length,
};
use cache::{with_geodesic, with_geodesic_cache};
pub(crate) use distance::{
    geodesic_frechet, geodesic_hausdorff, geodesic_point_distances, geodesic_point_pair_distances,
    geodesic_point_pair_dwithin,
};
pub(crate) use domain::{
    ensure_geodesic_lonlat_crs, ensure_geographic_coordseq, ensure_geographic_domain,
    ensure_geographic_lonlat, ensure_latitudes_in_domain,
};
pub(crate) use ellipsoid_metric::{
    EllipsoidMetric, geo_azimuths, geo_inverse, inverse_azimuths, inverse_distance,
    inverse_distance_azimuths,
};
pub(crate) use lower_bound::interpolate_optional_ordinate;
use lower_bound::{LowerBoundKernel, cached_lower_bound_kernel};
pub(crate) use measure::{
    AngleUnit, DistanceMode, endpoints_are_repeated, ensure_geographic_columns,
    geodesic_direct_on_ellipsoid, geodesic_direct_on_ellipsoid_const,
    geodesic_interpolate_on_line_const, geodesic_inverse_on_ellipsoid,
    geodesic_inverse_on_ellipsoid_const, geodesic_line_length_columns,
    geodesic_line_solution_const, geodesic_ring_measure_columns, geodesic_shape_area,
    geodesic_shape_length, with_ellipsoid_metric, with_resolved_ellipsoid_metric,
};
pub(crate) use metric_model::{
    MetricModel, ResolvedMetric, ensure_3d_metric, ensure_geographic_degree_units, metric_model,
    metric_model_meters,
};
pub(crate) use nearest_lrs::{
    geodesic_line_interpolate, geodesic_line_interpolate_coordseq, geodesic_line_locate,
    geodesic_line_locate_coordseq, geodesic_line_substring, geodesic_line_substring_coordseq,
    with_geodesic_coordseq_collect_rows, with_geodesic_coordseq_metric, with_geodesic_erased,
};
use segment::{
    geodesic_foot_on_segment, geodesic_locate_on_segment, geodesic_point_to_segment,
    geodesic_segments_cross,
};
