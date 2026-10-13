use std::hash::Hash;
use std::marker::PhantomData;
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;
use std::simd::{Select as _, StdFloat as _};

use crate::collections::{HashMap, HashMapExt as _};
use crate::error::{ErrorKind, ResourceError, Result as CoreResult};
use crate::geographic::retained_exact::sites::retained_site_longitude;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::numerics::product_sum::ExactCentroidSums as FixedExactCentroidSums;
use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio, round_sqrt_ratio};
use crate::geometry::{
    Bounds, Component, CoordSeq, Coordinates as _, Dimension, GeometryErrorKind, Point, Polygon,
    REDUCE_LANES, REDUCE_SIMD_MIN, ReduceSimd, Ring, Shape, ShapeOwnerKey, axis_pow2_scale,
    canonicalize_zero, centroid_ring_sums, closed_columns_winding, column_mean2, point_distance,
    simd_reduce_f64, xy_finite_bounds_columns,
};

mod area;
mod exact;
mod line;
mod merge;
mod owners;
mod points;

#[cfg(test)]
use self::area::areal_centroid_summary;
use self::area::{AREAL_EXACT_MAX_COORD, polygon_ring_columns};
pub(crate) use self::area::{
    areal_centroid, areal_centroid_with_bounds, centroid_polygon_row_columns,
    centroid_polygon_row_columns_with_bounds,
};
#[cfg(test)]
use self::exact::exact_owner_centroid_with_work;
pub(crate) use self::exact::exact_point_chart_centroid;
use self::exact::{
    CentroidAttempt, ExactAtRoot, GrowableCentroidSums, exact_owner_centroid,
    exact_summary_centroid,
};
pub(super) use self::line::centroid_line_columns;
#[cfg(test)]
use self::line::exact_lineal_centroid_runs;
#[cfg(test)]
use self::line::visit_lineal_owner_runs;
pub(crate) use self::line::{centroid_line_row_columns, lineal_centroid};
use self::line::{
    centroid_polygon_lineal_columns, exact_segment_length, lineal_part_requires_exact_replay,
};
use self::merge::{
    NeumaierSum, WeightedCentroidMergeFallback, neumaier_sum, weighted_centroid_merge,
};
pub(crate) use self::owners::centroid_is_empty;
#[cfg(test)]
use self::owners::{
    CENTROID_ADMISSION_INLINE_OWNERS, CENTROID_FAST_REPEAT_WORK_BUDGET, centroid_is_empty_with_work,
};
use self::owners::{
    CentroidDimension, CentroidOwnerMap, OwnerAdmission, charge_repeated_work,
    observe_collection_owner,
};
use self::points::polygon_rings;
#[cfg(test)]
use self::points::visit_fallback_points;
pub(crate) use self::points::{PointSetCenter, point_centroid, point_set_center_summary};

#[cfg(test)]
#[path = "centroid/tests/exact_lineal_centroid_tests.rs"]
mod exact_lineal_centroid_tests;

#[cfg(test)]
#[path = "centroid/tests/weighted_centroid_merge_tests.rs"]
mod weighted_centroid_merge_tests;

#[cfg(test)]
#[path = "centroid/tests/owner_fold_tests.rs"]
mod owner_fold_tests;
