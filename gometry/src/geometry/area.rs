//! Typed signed-area lanes: measurement magnitude vs deterministic orientation
//! decisions. Topology never branches on a raw signed `f64`.

use std::simd::num::SimdFloat as _;

use crate::error::Result;
use crate::geographic::{ChartPole, FullTurnPole, GeographicFrame, Latitude};
use crate::geometry::access::{REDUCE_LANES, ReduceSimd, crossover, simd_reduce_f64};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio};
use crate::geometry::types::{Coordinates, Point, XY};
use crate::geometry::{axis_pow2_scale, scaled_residual, unscale_area};

mod exact;
mod lifted;
mod polygon;
mod shoelace;
mod sign;

pub(crate) use self::exact::{ExactFactorialArea, ExactRationalArea};
pub(in crate::geometry) use self::lifted::lifted_ring_area_sign;
#[cfg(test)]
pub(crate) use self::polygon::open_point_cycle_magnitude;
pub(crate) use self::polygon::{
    open_xy_cycle_magnitude, polygon_area_measure_columns, polygon_area_measure_with,
};
use self::shoelace::{
    SHOELACE_SIMD_MIN, open_cycle_sum, shoelace_shifted_with_roundoff, unsigned_area,
};
#[cfg(test)]
pub(crate) use self::sign::open_xy_cycle_sign;
pub(crate) use self::sign::{
    AreaSign, RingWinding, closed_columns_winding, exact_ring_area_sign_columns,
    open_point_cycle_winding, open_xy_cycle_winding, ring_winding,
};
use self::sign::{CycleSignFilter, EPSILON};

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub(crate) struct AreaMeasure(f64);

impl AreaMeasure {
    pub(crate) const fn get(self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod tests;
