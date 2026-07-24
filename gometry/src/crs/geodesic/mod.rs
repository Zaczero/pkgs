#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Ellipsoidal geodesic math (geographiclib): inverse/direct/interpolate,
//! area/length, point-to-segment Newton (golden fallback), the geodesic LRS,
//! and the `MetricModel` that decides planar-vs-geodesic. The geodesic instance
//! cache (`with_geodesic`) reaches its thread-local storage in the parent via
//! `super`.

pub(crate) use geographiclib_rs::{DirectGeodesic, InverseGeodesic};
use geographiclib_rs::{Geodesic, GeodesicLine, geodesic_capability as caps};

use super::*;
use crate::collections::{HashMap, HashMapExt};
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

pub(crate) use bearing::*;
pub(crate) use bulk::*;
use cache::*;
pub(crate) use distance::*;
pub(crate) use domain::*;
pub(crate) use ellipsoid_metric::*;
use lower_bound::*;
pub(crate) use measure::*;
pub(crate) use metric_model::*;
pub(crate) use nearest_lrs::*;
use segment::*;
