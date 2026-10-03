//! Typed signed-area lanes: measurement magnitude vs deterministic orientation
//! decisions. Topology never branches on a raw signed `f64`.

use std::simd::num::SimdFloat as _;

use crate::error::Result;
use crate::geometry::access::{REDUCE_LANES, ReduceSimd, crossover, simd_reduce_f64};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio};
use crate::geometry::types::{Coordinates, Point, XY};
use crate::geometry::{axis_pow2_scale, scaled_residual, unscale_area};

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub(crate) struct AreaMeasure(f64);

impl AreaMeasure {
    pub(crate) const fn get(self) -> f64 {
        self.0
    }
}

/// Exact rational accumulator for areas of selected partition faces.
///
/// A constructed partition node is a rational point, not a binary64 point:
/// its homogeneous coordinates are dyadic numerators and denominators. The
/// selected-face cross products therefore form a rational sum. Keep that sum
/// reduced after every term, then round its final absolute half-area once with
/// the exact ratio owner. This is deliberately separate from [`ExactSum`],
/// whose degree-two dyadic contract serves ring winding signs and source-ring
/// magnitudes rather than rational face coordinates.
pub(crate) struct ExactRationalArea {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

impl ExactRationalArea {
    pub(in crate::geometry) fn new() -> Self {
        Self {
            numerator: ExactDyadic::from_f64(0.0),
            denominator: ExactDyadic::from_f64(1.0),
        }
    }

    pub(in crate::geometry) fn add_fraction(
        &mut self,
        mut numerator: ExactDyadic,
        mut denominator: ExactDyadic,
    ) -> Result<()> {
        if numerator.is_zero() {
            return Ok(());
        }
        if denominator.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        if denominator.sign() == ExactSign::Negative {
            numerator = numerator.neg();
            denominator = denominator.neg();
        }

        let common = self.denominator.gcd(&denominator);
        let left_scale = self.denominator.try_exact_quotient(&common)?;
        let right_scale = denominator.try_exact_quotient(&common)?;
        let left = self.numerator.try_product(&right_scale)?;
        let right = numerator.try_product(&left_scale)?;
        let next_numerator = left.try_add(right)?;
        let next_denominator = self.denominator.try_product(&right_scale)?;

        // Keep the common denominator bounded by the least common multiple,
        // rather than the product of every face denominator. This is the
        // rational-sum contract: every update remains an exact reduced
        // fraction, and only the final quotient is rounded.
        let reduction = next_numerator.clone().abs().gcd(&next_denominator);
        self.numerator = next_numerator.try_exact_quotient(&reduction)?;
        self.denominator = next_denominator.try_exact_quotient(&reduction)?;
        Ok(())
    }

    pub(in crate::geometry) fn add_homogeneous_cross(
        &mut self,
        left: &[ExactDyadic; 3],
        right: &[ExactDyadic; 3],
    ) -> Result<()> {
        let forward = left[0].try_product(&right[1])?;
        let reverse = left[1].try_product(&right[0])?;
        let numerator = forward.try_subtract(reverse)?;
        let denominator = left[2].try_product(&right[2])?;
        self.add_fraction(numerator, denominator)
    }

    pub(in crate::geometry) fn round_abs_half(&self) -> Result<f64> {
        let numerator = self.numerator.clone().abs();
        let denominator = self.denominator.try_product(&ExactDyadic::from_f64(2.0))?;
        match round_ratio(&numerator, &denominator) {
            Ok(value) => Ok(value),
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::ExactCoordinateOutOfRange
                    )
                ) =>
            {
                // A scalar area uses IEEE binary64 overflow semantics. The
                // shared ratio owner reports the same threshold as an
                // unrepresentable coordinate, so translate only that
                // magnitude-specific result to positive infinity.
                Ok(f64::INFINITY)
            },
            Err(error) => Err(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AreaSign {
    Negative,
    Zero,
    Positive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingWinding {
    Clockwise,
    CounterClockwise,
    Degenerate,
}

impl RingWinding {
    pub(crate) const fn is_ccw(self) -> bool {
        matches!(self, Self::CounterClockwise)
    }

    pub(crate) const fn is_degenerate(self) -> bool {
        matches!(self, Self::Degenerate)
    }

    /// Reverse the ring when its winding does not match `clockwise`.
    pub(crate) const fn reverse_for_clockwise(self, clockwise: bool) -> bool {
        matches!(self, Self::CounterClockwise) == clockwise
    }
}

impl AreaSign {
    #[cfg(test)]
    /// True when a face walk with this sign is a better outer-face candidate
    /// for its component than one with `other`. A nondegenerate component's
    /// outer face is its negative walk; when every walk is nonnegative the
    /// component encloses no area and a zero walk seeds it.
    pub(crate) const fn prefers_as_outer_face_than(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Negative, Self::Zero | Self::Positive) | (Self::Zero, Self::Positive)
        )
    }
}

const fn area_sign_to_winding(sign: AreaSign) -> RingWinding {
    match sign {
        AreaSign::Negative => RingWinding::Clockwise,
        AreaSign::Zero => RingWinding::Degenerate,
        AreaSign::Positive => RingWinding::CounterClockwise,
    }
}

const EPSILON: f64 = f64::EPSILON;

/// The exact area sign of a CLOSED ring stored as columns, whose edges are the
/// contiguous pairs `(i, i + 1)`.
///
/// Staging, validity, canonicalization and areal topology share this
/// original-coordinate predicate, independently of rounded area measurement.
pub(crate) fn exact_ring_area_sign_columns(xs: &[f64], ys: &[f64], pairs: usize) -> AreaSign {
    if pairs == 0 {
        return AreaSign::Zero;
    }
    if let Some(sign) = rounded_translated_cycle_sign_columns(xs, ys, pairs) {
        return sign;
    }

    let mut sum = ExactSum::zero();
    for (&[x0, x1], &[y0, y1]) in xs
        .array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .take(pairs)
    {
        sum.add_product(x0, y1);
        sum.subtract_product(x1, y0);
    }
    area_sign_from_ordering(sum.ordering())
}

fn cycle_sign(edge_count: usize, edges: impl Iterator<Item = (XY, XY)> + Clone) -> AreaSign {
    if edge_count == 0 {
        return AreaSign::Zero;
    }
    if let Some(sign) = rounded_translated_cycle_sign(edge_count, edges.clone()) {
        return sign;
    }
    exact_cycle_sign(edges)
}

fn exact_cycle_sign(edges: impl Iterator<Item = (XY, XY)>) -> AreaSign {
    let mut sum = ExactSum::zero();
    for (a, b) in edges {
        sum.add_product(a.x, b.y);
        sum.subtract_product(b.x, a.y);
    }
    area_sign_from_ordering(sum.ordering())
}

const fn area_sign_from_ordering(ordering: std::cmp::Ordering) -> AreaSign {
    match ordering {
        std::cmp::Ordering::Less => AreaSign::Negative,
        std::cmp::Ordering::Equal => AreaSign::Zero,
        std::cmp::Ordering::Greater => AreaSign::Positive,
    }
}

/// Running filter over a cycle whose vertices are exposed by an edge reader.
fn rounded_translated_cycle_sign(
    edge_count: usize,
    edges: impl Iterator<Item = (XY, XY)> + Clone,
) -> Option<AreaSign> {
    if edge_count > 1 << 26 {
        return None;
    }
    let (origin, _) = edges.clone().next()?;
    let mut filter = CycleSignFilter::new(origin, edge_count)?;
    for (_, destination) in edges {
        filter.push(destination);
    }
    filter.finish()
}

/// Column-specialized filter: the contiguous slices make each vertex pair
/// bounds-free in the loop, and the running product magnitude removes the
/// NaN-aware maxima from the prior per-edge error estimate.
fn rounded_translated_cycle_sign_columns(
    xs: &[f64],
    ys: &[f64],
    edge_count: usize,
) -> Option<AreaSign> {
    if edge_count > 1 << 26 {
        return None;
    }
    let (&origin_x, &origin_y) = xs.first().zip(ys.first())?;
    let origin = XY::new(origin_x, origin_y);
    let mut filter = CycleSignFilter::new(origin, edge_count)?;
    for (&x, &y) in xs.iter().skip(1).zip(ys.iter().skip(1)).take(edge_count) {
        filter.push(XY::new(x, y));
    }
    filter.finish()
}

struct CycleSignFilter {
    origin: XY,
    previous: XY,
    edge_count: usize,
    sum: f64,
    product_magnitude: f64,
}

impl CycleSignFilter {
    fn new(origin: XY, edge_count: usize) -> Option<Self> {
        (origin.x.is_finite() && origin.y.is_finite()).then_some(Self {
            origin,
            previous: XY::new(0.0, 0.0),
            edge_count,
            sum: 0.0,
            product_magnitude: 0.0,
        })
    }

    fn push(&mut self, destination: XY) {
        let next = XY::new(destination.x - self.origin.x, destination.y - self.origin.y);
        let left = self.previous.x * next.y;
        let right = next.x * self.previous.y;
        let difference = left - right;
        let sum = self.sum + difference;
        let product_magnitude = left.abs() + right.abs();
        let product_magnitude = self.product_magnitude + product_magnitude;

        self.previous = next;
        self.sum = sum;
        self.product_magnitude = product_magnitude;
    }

    fn finish(self) -> Option<AreaSign> {
        if !(self.sum.is_finite() && self.product_magnitude.is_finite()) {
            return None;
        }
        // A finite binary64 subtraction is exact when its result is subnormal
        // (Sterbenz or the shared subnormal grid); normal residuals have the
        // usual relative bound. For n <= 2^26, the relative term covers
        // translated residuals, rounded products and differences, the
        // sequential sum, and rounding of the positive magnitude fold. The
        // absolute term covers underflow in products and accumulations.
        let count = self.edge_count as f64;
        let relative = (32.0 * count * EPSILON) * self.product_magnitude;
        let underflow = (8.0 * f64::MIN_POSITIVE * count) * count;
        let bound = (relative + underflow).next_up();
        if !bound.is_finite() {
            return None;
        }
        if self.sum > bound {
            Some(AreaSign::Positive)
        } else if self.sum < -bound {
            Some(AreaSign::Negative)
        } else {
            None
        }
    }
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "This test oracle preserves adjacent closed-ring indexing for the original-coordinate exact fallback"
)]
fn reference_exact_ring_area_sign_columns(xs: &[f64], ys: &[f64], pairs: usize) -> AreaSign {
    reference_cycle_sign(pairs, |index| {
        (
            XY::new(xs[index], ys[index]),
            XY::new(xs[index + 1], ys[index + 1]),
        )
    })
}

#[cfg(test)]
fn reference_cycle_sign(edge_count: usize, mut edge_at: impl FnMut(usize) -> (XY, XY)) -> AreaSign {
    if edge_count == 0 {
        return AreaSign::Zero;
    }
    if let Some(sign) = reference_rounded_translated_cycle_sign(edge_count, &mut edge_at) {
        return sign;
    }
    let mut sum = ExactSum::zero();
    for index in 0..edge_count {
        let (a, b) = edge_at(index);
        sum.add_product(a.x, b.y);
        sum.subtract_product(b.x, a.y);
    }
    area_sign_from_ordering(sum.ordering())
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "The test oracle reproduces the former translated shoelace filter evaluation"
)]
fn reference_rounded_translated_cycle_sign(
    edge_count: usize,
    mut edge_at: impl FnMut(usize) -> (XY, XY),
) -> Option<AreaSign> {
    if edge_count > 1 << 26 {
        return None;
    }
    let (origin, _) = edge_at(0);
    if !origin.x.is_finite() || !origin.y.is_finite() {
        return None;
    }
    let mut previous = XY::new(0.0, 0.0);
    let mut maximum_x_residual = 0.0_f64;
    let mut maximum_y_residual = 0.0_f64;
    let mut maximum_product = 0.0_f64;
    let mut sum = 0.0_f64;
    for index in 0..edge_count {
        let (_, destination) = edge_at(index);
        let next = if index + 1 == edge_count {
            XY::new(0.0, 0.0)
        } else {
            XY::new(destination.x - origin.x, destination.y - origin.y)
        };
        maximum_x_residual = maximum_x_residual.max(next.x.abs());
        maximum_y_residual = maximum_y_residual.max(next.y.abs());
        let left = previous.x * next.y;
        let right = next.x * previous.y;
        maximum_product = maximum_product.max(left.abs()).max(right.abs());
        let difference = left - right;
        sum += difference;
        previous = next;
    }
    if !sum.is_finite() {
        return None;
    }
    let error_x = rounded_residual_error_bound(maximum_x_residual);
    let error_y = rounded_residual_error_bound(maximum_y_residual);
    let translation_term = outward_add(
        outward_add(
            outward_mul(error_x, maximum_y_residual),
            outward_mul(error_y, maximum_x_residual),
        ),
        outward_mul(error_x, error_y),
    );
    let translation_error = outward_mul(2.0 * edge_count as f64, translation_term);
    let edge_count_squared = (edge_count as f64) * (edge_count as f64);
    let arithmetic_error = outward_add(
        outward_mul(8.0 * f64::EPSILON * edge_count_squared, maximum_product),
        outward_mul(8.0 * f64::MIN_POSITIVE, edge_count_squared),
    );
    let bound = outward_add(arithmetic_error, translation_error);
    if !bound.is_finite() {
        return None;
    }
    if sum > bound {
        Some(AreaSign::Positive)
    } else if sum < -bound {
        Some(AreaSign::Negative)
    } else {
        None
    }
}

#[cfg(test)]
/// A conservative absolute error bound for a finite rounded subtraction.
fn rounded_residual_error_bound(maximum_residual: f64) -> f64 {
    outward_add(
        outward_mul(f64::EPSILON, maximum_residual),
        f64::from_bits(1),
    )
}

#[cfg(test)]
fn outward_mul(left: f64, right: f64) -> f64 {
    (left * right).next_up()
}

#[cfg(test)]
fn outward_add(left: f64, right: f64) -> f64 {
    (left + right).next_up()
}

fn exact_open_xy_sign(points: &[XY]) -> AreaSign {
    let count = points.len();
    if count < 2 {
        return AreaSign::Zero;
    }
    cycle_sign(
        count,
        points
            .iter()
            .copied()
            .zip(points.iter().copied().cycle().skip(1))
            .take(count),
    )
}

fn exact_open_point_sign(points: &[Point]) -> AreaSign {
    let count = points.len();
    if count < 2 {
        return AreaSign::Zero;
    }
    cycle_sign(
        count,
        points
            .iter()
            .map(Point::xy)
            .zip(points.iter().map(Point::xy).cycle().skip(1))
            .take(count),
    )
}

/// Ring vertex-pair counts below this use the scalar fold — LLVM auto-vectorizes
/// it to packed `mulpd`/`addpd` (asm-verified) with no per-ring horizontal
/// reduce; at or above it the 512-bit `simd_reduce_f64` path's extra ILP wins
/// (measured: scalar ties to ~64 verts, SIMD wins 15–25% at 256+). Centralized
/// in [`crossover`].
const SHOELACE_SIMD_MIN: usize = crossover::OFFSET_PAIR_MEASURE;

/// Scalar shoelace half-sum for the short-ring path. Unit scales retain the
/// existing operation order; non-unit scales form `x*s - origin*s`.
#[expect(
    clippy::indexing_slicing,
    reason = "pairs counts adjacent entries in equal closed-ring columns, including the terminal vertex"
)]
fn shoelace_shifted(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> f64 {
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    xs[..pairs]
        .iter()
        .zip(&xs[1..=pairs])
        .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
        .fold(0.0_f64, |area, ((&x0, &x1), (&y0, &y1))| {
            let (sx0, sy0) = if identity {
                (x0 - ox, y0 - oy)
            } else {
                (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
            };
            let (sx1, sy1) = if identity {
                (x1 - ox, y1 - oy)
            } else {
                (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
            };
            area.algebraic_add(sx0.algebraic_mul(sy1).algebraic_sub(sx1.algebraic_mul(sy0)))
        })
        / 2.0
}

/// Shifted shoelace measure and its absolute error bound. Long rings compute
/// both in one traversal, reusing the SIMD residuals for the bound without
/// changing the cross-sum reduction.
#[expect(
    clippy::indexing_slicing,
    reason = "pairs and SIMD reduction ranges are bounded by the admitted closed-ring columns"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "The binary64 cross fold and product scale form the certified measure and roundoff bound"
)]
fn shoelace_shifted_with_roundoff(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> (f64, f64) {
    if pairs < SHOELACE_SIMD_MIN {
        return (
            shoelace_shifted(xs, ys, pairs, ox, oy, sx, sy),
            shoelace_shifted_roundoff(xs, ys, pairs, ox, oy, sx, sy),
        );
    }

    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    let (x0, _) = xs[..pairs].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..pairs].as_chunks::<REDUCE_LANES>();
    let (y1, _) = ys[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (ovx, ovy) = (ReduceSimd::splat(ox), ReduceSimd::splat(oy));
    let sx_v = ReduceSimd::splat(sx);
    let sy_v = ReduceSimd::splat(sy);
    let residual_x = |v: ReduceSimd, o: ReduceSimd| {
        if identity {
            v - o
        } else {
            // x*s - origin*s (scale before subtraction).
            v * sx_v - o * sx_v
        }
    };
    let residual_y = |v: ReduceSimd, o: ReduceSimd| {
        if identity { v - o } else { v * sy_v - o * sy_v }
    };
    let simd_fold = |mut acc: ReduceSimd, mut product_scale: f64, start: usize| {
        let chunk = start / REDUCE_LANES;
        let (rx0, rx1) = (
            residual_x(ReduceSimd::from_array(x0[chunk]), ovx),
            residual_x(ReduceSimd::from_array(x1[chunk]), ovx),
        );
        let (ry0, ry1) = (
            residual_y(ReduceSimd::from_array(y0[chunk]), ovy),
            residual_y(ReduceSimd::from_array(y1[chunk]), ovy),
        );
        acc += rx0 * ry1 - rx1 * ry0;
        for (((x0, x1), y0), y1) in rx0
            .to_array()
            .into_iter()
            .zip(rx1.to_array())
            .zip(ry0.to_array())
            .zip(ry1.to_array())
        {
            product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
        }
        (acc, product_scale)
    };
    let lanes = x0.len() * REDUCE_LANES;
    simd_reduce_f64(
        pairs,
        ReduceSimd::splat(0.0),
        0.0_f64,
        simd_fold,
        |mut product_scale, range| {
            if range.start != 0 {
                debug_assert_eq!(range.start, lanes);
            }
            let start = range.start;
            let end = range.end;
            let x_start = &xs[range.clone()];
            let y_start = &ys[range];
            for ((&x0, &x1), (&y0, &y1)) in x_start
                .iter()
                .zip(xs[start + 1..=end].iter())
                .zip(y_start.iter().zip(ys[start + 1..=end].iter()))
            {
                let (x0, y0) = if identity {
                    (x0 - ox, y0 - oy)
                } else {
                    (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
                };
                let (x1, y1) = if identity {
                    (x1 - ox, y1 - oy)
                } else {
                    (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
                };
                product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
            }
            product_scale
        },
        |mut acc, product_scale| {
            if lanes < pairs {
                let (tx0, tx1) = (
                    residual_x(ReduceSimd::load_or_default(&xs[lanes..pairs]), ovx),
                    residual_x(ReduceSimd::load_or_default(&xs[lanes + 1..=pairs]), ovx),
                );
                let (ty0, ty1) = (
                    residual_y(ReduceSimd::load_or_default(&ys[lanes..pairs]), ovy),
                    residual_y(ReduceSimd::load_or_default(&ys[lanes + 1..=pairs]), ovy),
                );
                acc += tx0 * ty1 - tx1 * ty0;
            }
            let area = acc.reduce_sum() / 2.0;
            let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
            (area, product_scale * EPSILON * operations)
        },
    )
}

/// Absolute roundoff bound for the scalar shoelace fold on one ring. The
/// decisive scale is the sum of the two products *before* each subtraction: a
/// finite nonzero cross term can already have lost its answer to product
/// cancellation, so bounding only the result misses the `2^-27` rotated
/// triangle.
fn shoelace_shifted_roundoff(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> f64 {
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    let mut product_scale = 0.0_f64;
    for (&[x0, x1], &[y0, y1]) in xs
        .array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .take(pairs)
    {
        let (x0, y0) = if identity {
            (x0 - ox, y0 - oy)
        } else {
            (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
        };
        let (x1, y1) = if identity {
            (x1 - ox, y1 - oy)
        } else {
            (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
        };
        product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
    }
    // Each edge has two rounded products, one rounded subtraction, and one
    // contribution to the fold.  This conservative IEEE-754 roundoff bound
    // is a property of the performed arithmetic, not a geometry-size knob.
    let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
    product_scale * EPSILON * operations
}

#[cfg(test)]
mod reference {
    use std::simd::num::SimdFloat as _;

    use super::{REDUCE_LANES, ReduceSimd, SHOELACE_SIMD_MIN, scaled_residual, simd_reduce_f64};

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "This test-only SIMD oracle preserves the former binary64 shoelace reduction order"
    )]
    pub(super) fn shoelace_shifted(
        xs: &[f64],
        ys: &[f64],
        pairs: usize,
        ox: f64,
        oy: f64,
        sx: f64,
        sy: f64,
    ) -> f64 {
        let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
        if pairs < SHOELACE_SIMD_MIN {
            let cross = xs[..pairs]
                .iter()
                .zip(&xs[1..=pairs])
                .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
                .fold(0.0_f64, |area, ((&x0, &x1), (&y0, &y1))| {
                    let (sx0, sy0) = if identity {
                        (x0 - ox, y0 - oy)
                    } else {
                        (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
                    };
                    let (sx1, sy1) = if identity {
                        (x1 - ox, y1 - oy)
                    } else {
                        (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
                    };
                    area.algebraic_add(sx0.algebraic_mul(sy1).algebraic_sub(sx1.algebraic_mul(sy0)))
                });
            return cross / 2.0;
        }
        let (x0, _) = xs[..pairs].as_chunks::<REDUCE_LANES>();
        let (x1, _) = xs[1..=pairs].as_chunks::<REDUCE_LANES>();
        let (y0, _) = ys[..pairs].as_chunks::<REDUCE_LANES>();
        let (y1, _) = ys[1..=pairs].as_chunks::<REDUCE_LANES>();
        let (ovx, ovy) = (ReduceSimd::splat(ox), ReduceSimd::splat(oy));
        let sx_v = ReduceSimd::splat(sx);
        let sy_v = ReduceSimd::splat(sy);
        let residual_x = |values: ReduceSimd, origin: ReduceSimd| {
            if identity {
                values - origin
            } else {
                values * sx_v - origin * sx_v
            }
        };
        let residual_y = |values: ReduceSimd, origin: ReduceSimd| {
            if identity {
                values - origin
            } else {
                values * sy_v - origin * sy_v
            }
        };
        let simd_fold = |mut acc: ReduceSimd, start: usize| {
            let chunk = start / REDUCE_LANES;
            let (x0, x1) = (
                residual_x(ReduceSimd::from_array(x0[chunk]), ovx),
                residual_x(ReduceSimd::from_array(x1[chunk]), ovx),
            );
            let (y0, y1) = (
                residual_y(ReduceSimd::from_array(y0[chunk]), ovy),
                residual_y(ReduceSimd::from_array(y1[chunk]), ovy),
            );
            acc += x0 * y1 - x1 * y0;
            acc
        };
        let lanes = x0.len() * REDUCE_LANES;
        simd_reduce_f64(
            pairs,
            ReduceSimd::splat(0.0),
            (),
            |acc, (), start| (simd_fold(acc, start), ()),
            |(), range| {
                if range.start != 0 {
                    debug_assert_eq!(range.start, lanes);
                }
            },
            |mut acc, ()| {
                if lanes < pairs {
                    let (tx0, tx1) = (
                        residual_x(ReduceSimd::load_or_default(&xs[lanes..pairs]), ovx),
                        residual_x(ReduceSimd::load_or_default(&xs[lanes + 1..=pairs]), ovx),
                    );
                    let (ty0, ty1) = (
                        residual_y(ReduceSimd::load_or_default(&ys[lanes..pairs]), ovy),
                        residual_y(ReduceSimd::load_or_default(&ys[lanes + 1..=pairs]), ovy),
                    );
                    acc += tx0 * ty1 - tx1 * ty0;
                }
                acc.reduce_sum() / 2.0
            },
        )
    }
}

#[cfg(test)]
fn reference_shoelace_shifted_roundoff(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> f64 {
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    let mut product_scale = 0.0_f64;
    for ((&x0, &x1), (&y0, &y1)) in xs[..pairs]
        .iter()
        .zip(&xs[1..=pairs])
        .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
    {
        let (x0, y0) = if identity {
            (x0 - ox, y0 - oy)
        } else {
            (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
        };
        let (x1, y1) = if identity {
            (x1 - ox, y1 - oy)
        } else {
            (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
        };
        product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
    }
    let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
    product_scale * EPSILON * operations
}

fn open_cycle_sum_columns(xs: &[f64], ys: &[f64]) -> ExactSum {
    let mut sum = ExactSum::zero();
    let count = xs.len();
    if count < 2 {
        return sum;
    }
    for (&[x0, x1], &[y0, y1]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        sum.add_cross(x0, y0, x1, y1);
    }
    let (first_x, first_y) = xs
        .first()
        .zip(ys.first())
        .map(|(&x, &y)| (x, y))
        .expect("a cycle with at least two coordinates has a first coordinate");
    let (last_x, last_y) = xs
        .last()
        .zip(ys.last())
        .map(|(&x, &y)| (x, y))
        .expect("a cycle with at least two coordinates has a last coordinate");
    sum.add_cross(last_x, last_y, first_x, first_y);
    sum
}

fn open_cycle_sum<C: Coordinates + ?Sized>(points: &C) -> ExactSum {
    if let Some((xs, ys)) = points.xy_columns() {
        return open_cycle_sum_columns(xs, ys);
    }
    let mut coords = points.iter_coords();
    let Some(first) = coords.next() else {
        return ExactSum::zero();
    };
    let Some(second) = coords.next() else {
        return ExactSum::zero();
    };
    let mut sum = ExactSum::zero();
    sum.add_cross(first.x, first.y, second.x, second.y);
    let mut prev = second;
    for next in coords {
        sum.add_cross(prev.x, prev.y, next.x, next.y);
        prev = next;
    }
    sum.add_cross(prev.x, prev.y, first.x, first.y);
    sum
}

fn unsigned_area(sum: &ExactSum) -> AreaMeasure {
    AreaMeasure(sum.round_to_area().abs())
}

/// Polygon area in one common, power-of-two-normalized frame for the shell
/// and every hole. Per-ring rescues are insufficient at extreme magnitudes:
/// a shell and a hole can each legitimately measure as infinity even though
/// their mathematical difference has one unambiguous sign. The shared frame
/// performs `shell - holes` before one final unscale.
pub(crate) fn polygon_area_measure_with(
    visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
) -> f64 {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    let mut any = false;
    visit(&mut |xs, ys, _hole| {
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            any = true;
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    });
    if !(any && min_x.is_finite() && min_y.is_finite()) {
        return f64::NAN;
    }
    let ox = f64::midpoint(min_x, max_x);
    let oy = f64::midpoint(min_y, max_y);
    let sx = axis_pow2_scale(max_x.abs().max(min_x.abs()).max(ox.abs()));
    let sy = axis_pow2_scale(max_y.abs().max(min_y.abs()).max(oy.abs()));
    let mut area = 0.0_f64;
    let mut magnitude = 0.0_f64;
    let mut error = 0.0_f64;
    let mut rings = 0_usize;
    visit(&mut |xs, ys, hole| {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return;
        }
        let (signed_ring, ring_error) =
            shoelace_shifted_with_roundoff(xs, ys, pairs, ox, oy, sx, sy);
        let ring = signed_ring.abs();
        rings = rings.saturating_add(1);
        magnitude += ring;
        error += ring_error;
        if hole {
            area -= ring;
        } else {
            area += ring;
        }
    });
    // The bound covers every ring's roundoff and the shell-minus-holes fold.
    // Ring overflow is an arithmetic-loss signal rather than proof that the
    // area is unrepresentable, and it makes the bound nonfinite, so the exact
    // sum can still cancel it to a finite answer.
    error += magnitude * EPSILON * rings.saturating_add(1) as f64;
    if !(area.is_finite() && error.is_finite() && area.abs() > error) {
        return exact_polygon_area_measure_with(visit);
    }
    unscale_area(area, sx, sy)
}

/// Exact shell-minus-holes area, rounded once. The product-sum owner works in
/// the original coordinate frame, so no subnormal intermediate is needed and
/// shell/hole cancellation stays exact.
fn exact_polygon_area_measure_with(visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool))) -> f64 {
    let mut total = ExactSum::zero();
    visit(&mut |xs, ys, hole| {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return;
        }
        let mut ring = ExactSum::zero();
        for (&[x0, x1], &[y0, y1]) in xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .take(pairs)
        {
            ring.add_cross(x0, y0, x1, y1);
        }
        let ordering = ring.ordering();
        if ordering == std::cmp::Ordering::Equal {
            return;
        }
        let negate = (ordering == std::cmp::Ordering::Less) ^ hole;
        for (&[x0, x1], &[y0, y1]) in xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .take(pairs)
        {
            if negate {
                total.subtract_product(x0, y1);
                total.add_product(x1, y0);
            } else {
                total.add_product(x0, y1);
                total.subtract_product(x1, y0);
            }
        }
    });
    total.round_to_area()
}

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "The test oracle preserves the original binary64 area/error accumulation order"
)]
fn reference_polygon_area_measure_with(
    visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
) -> f64 {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    let mut any = false;
    visit(&mut |xs, ys, _hole| {
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            any = true;
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    });
    if !(any && min_x.is_finite() && min_y.is_finite()) {
        return f64::NAN;
    }
    let ox = f64::midpoint(min_x, max_x);
    let oy = f64::midpoint(min_y, max_y);
    let sx = axis_pow2_scale(max_x.abs().max(min_x.abs()).max(ox.abs()));
    let sy = axis_pow2_scale(max_y.abs().max(min_y.abs()).max(oy.abs()));
    let mut area = 0.0_f64;
    let mut magnitude = 0.0_f64;
    let mut error = 0.0_f64;
    let mut rings = 0_usize;
    visit(&mut |xs, ys, hole| {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return;
        }
        let ring = reference::shoelace_shifted(xs, ys, pairs, ox, oy, sx, sy).abs();
        rings += 1;
        magnitude += ring;
        error += reference_shoelace_shifted_roundoff(xs, ys, pairs, ox, oy, sx, sy);
        if hole {
            area -= ring;
        } else {
            area += ring;
        }
    });
    error += magnitude * EPSILON * rings.saturating_add(1) as f64;
    if !(area.is_finite() && error.is_finite() && area.abs() > error) {
        return exact_polygon_area_measure_with(visit);
    }
    unscale_area(area, sx, sy)
}

fn selected_ring_columns<'a>(
    ring_offsets: &[i32],
    xs: &'a [f64],
    ys: &'a [f64],
    ring_index: usize,
) -> (&'a [f64], &'a [f64], usize) {
    let start = *ring_offsets
        .get(ring_index)
        .expect("selected ring ID fits the offset column") as usize;
    let end = *ring_offsets
        .get(ring_index.wrapping_add(1))
        .expect("selected ring successor fits the offset column") as usize;
    let pairs = end.saturating_sub(start.wrapping_add(1));
    (
        xs.get(start..end).expect("ring offsets fit the x column"),
        ys.get(start..end).expect("ring offsets fit the y column"),
        pairs,
    )
}

/// Packed polygon area with a fused ordinary path.
///
/// Most packed polygons have short, finite rings whose per-ring origin shift
/// is enough to condition the shoelace products.  Fuse the cross-term and
/// roundoff-bound folds so those rows do not first build a polygon-wide frame
/// and then replay every vertex.  A shell/hole cancellation, overflow, long
/// ring, or uncertain bound returns to [`polygon_area_measure_with`], whose
/// shared frame and exact product sum preserve the extreme-coordinate contract.
pub(crate) fn polygon_area_measure_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> f64 {
    let mut area = 0.0_f64;
    let mut magnitude = 0.0_f64;
    let mut error = 0.0_f64;
    let mut fast = !rings.is_empty();

    for ring_index in rings.clone() {
        let (ring_xs, ring_ys, pairs) = selected_ring_columns(ring_offsets, xs, ys, ring_index);
        let Some((signed, bound)) = fast_ring_area_columns(ring_xs, ring_ys, pairs) else {
            fast = false;
            break;
        };
        let ring = signed.abs();
        magnitude += ring;
        error += bound;
        if ring_index == rings.start {
            area += ring;
        } else {
            area -= ring;
        }
    }

    // Include the final shell-minus-holes fold in the bound.  If that bound
    // cannot distinguish the result from zero, use the shared-frame exact
    // owner rather than letting independent ring frames cancel poorly.
    let fold_error = magnitude * EPSILON * rings.len().saturating_add(1) as f64;
    error += fold_error;
    if fast && area.is_finite() && error.is_finite() && area.abs() > error {
        return area;
    }

    polygon_area_measure_with(|visit| {
        for ring_index in rings.clone() {
            let (ring_xs, ring_ys, _) = selected_ring_columns(ring_offsets, xs, ys, ring_index);
            visit(ring_xs, ring_ys, ring_index != rings.start);
        }
    })
}

/// One-pass short-ring reducer. `None` is the cold-path signal, not a result
/// sentinel: it covers arithmetic that needs the shared-frame exact owner.
fn fast_ring_area_columns(xs: &[f64], ys: &[f64], pairs: usize) -> Option<(f64, f64)> {
    if pairs >= SHOELACE_SIMD_MIN {
        return None;
    }
    if pairs == 0 {
        return Some((0.0, 0.0));
    }
    let (&ox, &oy) = xs.first().zip(ys.first())?;
    let x0s = xs.get(..pairs).expect("pairs fit the closed x column");
    let x1s = xs.get(1..=pairs).expect("pairs fit the closed x column");
    let y0s = ys.get(..pairs).expect("pairs fit the closed y column");
    let y1s = ys.get(1..=pairs).expect("pairs fit the closed y column");
    let mut cross = 0.0_f64;
    let mut product_scale = 0.0_f64;
    for ((&x0, &x1), (&y0, &y1)) in x0s.iter().zip(x1s).zip(y0s.iter().zip(y1s)) {
        let x0 = x0 - ox;
        let y0 = y0 - oy;
        let x1 = x1 - ox;
        let y1 = y1 - oy;
        let term = x0 * y1 - x1 * y0;
        let products = x0.abs() * y1.abs() + x1.abs() * y0.abs();
        cross += term;
        product_scale += products;
    }
    // A non-finite term keeps the non-negative `product_scale` non-finite, so
    // one check after the loop leaves the loop free of branches.
    if !(cross.is_finite() && product_scale.is_finite()) {
        return None;
    }
    let measured = cross / 2.0;
    if measured == 0.0 || product_scale == 0.0 {
        return None;
    }
    let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
    let error = product_scale * EPSILON * operations;
    if !error.is_finite() || measured.abs() <= error {
        return None;
    }
    Some((measured, error))
}

/// The exact sign of a closed ring's area, with none of the magnitude work.
pub(crate) fn ring_area_sign<C: Coordinates + ?Sized>(points: &C) -> AreaSign {
    let pairs = points.coord_count().saturating_sub(1);
    if pairs == 0 {
        return AreaSign::Zero;
    }
    if let Some((xs, ys)) = points.xy_columns() {
        exact_ring_area_sign_columns(xs, ys, pairs)
    } else {
        let count = points.coord_count();
        let mut xs = Vec::with_capacity(count);
        let mut ys = Vec::with_capacity(count);
        for point in points.iter_coords() {
            xs.push(point.x);
            ys.push(point.y);
        }
        exact_ring_area_sign_columns(&xs, &ys, pairs)
    }
}

pub(crate) fn ring_winding<C: Coordinates + ?Sized>(points: &C) -> RingWinding {
    area_sign_to_winding(ring_area_sign(points))
}

pub(crate) fn closed_columns_winding(xs: &[f64], ys: &[f64], pairs: usize) -> RingWinding {
    area_sign_to_winding(exact_ring_area_sign_columns(xs, ys, pairs))
}

pub(crate) fn open_xy_cycle_winding(points: &[XY]) -> RingWinding {
    area_sign_to_winding(exact_open_xy_sign(points))
}

#[cfg(test)]
pub(crate) fn open_xy_cycle_sign(points: &[XY]) -> AreaSign {
    exact_open_xy_sign(points)
}

pub(crate) fn open_point_cycle_winding(points: &[Point]) -> RingWinding {
    area_sign_to_winding(exact_open_point_sign(points))
}

#[cfg(test)]
pub(crate) fn open_point_cycle_magnitude(points: &[Point]) -> AreaMeasure {
    unsigned_area(&open_cycle_sum(points))
}

pub(crate) fn open_xy_cycle_magnitude(points: &[XY]) -> AreaMeasure {
    unsigned_area(&open_cycle_sum(points))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Independent binary64 shoelace reference after translating to one vertex.
    fn shifted_scalar_area_reference(xs: &[f64], ys: &[f64]) -> f64 {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return 0.0;
        }
        let (ox, oy) = (xs[0], ys[0]);
        xs.array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + ((x0 - ox) * (y1 - oy) - (x1 - ox) * (y0 - oy))
            })
            / 2.0
    }

    fn assert_ring_sign_matches_reference(xs: &[f64], ys: &[f64]) {
        let pairs = xs.len().saturating_sub(1);
        assert_eq!(
            exact_ring_area_sign_columns(xs, ys, pairs),
            reference_exact_ring_area_sign_columns(xs, ys, pairs),
            "ring columns differ: {xs:?} {ys:?}"
        );
    }

    fn assert_area_bits_match_reference(rings: &[(&[f64], &[f64], bool)]) {
        let actual = polygon_area_measure_with(|visit| {
            for &(xs, ys, hole) in rings {
                visit(xs, ys, hole);
            }
        });
        let expected = reference_polygon_area_measure_with(|visit| {
            for &(xs, ys, hole) in rings {
                visit(xs, ys, hole);
            }
        });
        assert_eq!(
            actual.to_bits(),
            expected.to_bits(),
            "{actual:e} != {expected:e}"
        );
    }

    #[test]
    fn certified_ring_sign_matches_reference_on_slivers_and_generated_cycles() {
        let tiny = f64::from_bits(1);
        let sliver = 2.0_f64.powi(-54);
        let translated = 2.0_f64.powi(-54);
        for (xs, ys) in [
            (
                vec![
                    -1.446_158_859_221_779_4e-110,
                    -2.594_428_477_756_554e-110,
                    -2.307_361_073_122_860_3e-110,
                    -1.446_158_859_221_779_4e-110,
                ],
                vec![
                    2.371_948_131_117_221_8e-110,
                    -1.487_324_694_703_768_7e-110,
                    -5.225_064_882_485_208e-111,
                    2.371_948_131_117_221_8e-110,
                ],
            ),
            (vec![0.0, 1.0, 1.0 - 2.0_f64.powi(-27), 0.0], vec![
                0.0,
                1.0 + 2.0_f64.powi(-27),
                1.0,
                0.0,
            ]),
            (vec![1.0, 0.0, sliver, 1.0], vec![0.0, 1.0, 1.0, 0.0]),
            (vec![1.0e7, 1.0e7 + 1.0, 1.0e7 + 1.0, 1.0e7, 1.0e7], vec![
                1.0e7,
                1.0e7,
                1.0e7 + translated,
                1.0e7 + translated,
                1.0e7,
            ]),
            (vec![0.0, tiny, 0.0, 0.0], vec![0.0, 0.0, tiny, 0.0]),
            (vec![f64::MAX, -f64::MAX, 0.0, f64::MAX], vec![
                1.0, 1.0, 0.0, 1.0,
            ]),
        ] {
            assert_ring_sign_matches_reference(&xs, &ys);
        }

        let mut seed = 0x67E1_8A29_0D43_B5CF_u64;
        let mut next_coordinate = || {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            f64::from(u32::try_from(seed >> 32).expect("upper word fits u32") % 31) - 15.0
        };
        for ring_index in 0..32 {
            let mut xs = Vec::with_capacity(25);
            let mut ys = Vec::with_capacity(25);
            for vertex in 0..24 {
                let x = next_coordinate();
                let y = if ring_index % 4 == 0 {
                    f64::from(u32::try_from(vertex).expect("24 vertices fit u32")) - 12.0
                } else {
                    next_coordinate()
                };
                xs.push(x);
                ys.push(y);
            }
            if ring_index % 3 == 0 {
                let duplicate_x = *xs.get(4).expect("24 generated vertices");
                let duplicate_y = *ys.get(4).expect("24 generated vertices");
                *xs.get_mut(5).expect("24 generated vertices") = duplicate_x;
                *ys.get_mut(5).expect("24 generated vertices") = duplicate_y;
                let collinear_y = *ys.get(12).expect("24 generated vertices");
                *ys.get_mut(13).expect("24 generated vertices") = collinear_y;
            }
            let first_x = *xs.first().expect("24 generated vertices");
            let first_y = *ys.first().expect("24 generated vertices");
            xs.push(first_x);
            ys.push(first_y);
            assert_ring_sign_matches_reference(&xs, &ys);
        }
    }

    #[test]
    fn fused_long_area_keeps_reference_bits_for_random_and_degenerate_rings() {
        let radial = |count: usize, radius: f64, phase: f64| {
            let mut xs = Vec::with_capacity(count + 1);
            let mut ys = Vec::with_capacity(count + 1);
            let count_f64 = f64::from(u32::try_from(count).expect("ring size fits u32"));
            for index in 0..count {
                let index_f64 = f64::from(u32::try_from(index).expect("index fits u32"));
                let angle = phase + 2.0 * std::f64::consts::PI * index_f64 / count_f64;
                let radius = radius
                    * (1.0 + 0.07 * (5.0 * angle + 0.2).sin() + 0.025 * (13.0 * angle).cos());
                xs.push(radius * angle.cos());
                ys.push(radius * angle.sin());
            }
            xs.push(*xs.first().expect("radial ring has vertices"));
            ys.push(*ys.first().expect("radial ring has vertices"));
            (xs, ys)
        };
        let (shell_xs, shell_ys) = radial(4096, 100.0, 0.0);
        let (hole_xs, hole_ys) = radial(257, 15.0, 0.17);
        assert_area_bits_match_reference(&[
            (&shell_xs, &shell_ys, false),
            (&hole_xs, &hole_ys, true),
        ]);

        let mut seed = 0x1B74_6C2D_950F_A813_u64;
        let mut coordinate = || {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            f64::from(u32::try_from(seed >> 32).expect("upper word fits u32") % 257) - 128.0
        };
        for ring_index in 0..16 {
            let mut xs = Vec::with_capacity(65);
            let mut ys = Vec::with_capacity(65);
            for vertex in 0..64 {
                xs.push(500_000.0 + coordinate());
                ys.push(if ring_index % 3 == 0 {
                    5_200_000.0 + f64::from(u32::try_from(vertex).expect("64 vertices fit u32"))
                } else {
                    5_200_000.0 + coordinate()
                });
            }
            if ring_index % 4 == 0 {
                let duplicate_x = *xs.get(7).expect("64 generated vertices");
                let duplicate_y = *ys.get(7).expect("64 generated vertices");
                *xs.get_mut(8).expect("64 generated vertices") = duplicate_x;
                *ys.get_mut(8).expect("64 generated vertices") = duplicate_y;
            }
            xs.push(*xs.first().expect("64 generated vertices"));
            ys.push(*ys.first().expect("64 generated vertices"));
            assert_area_bits_match_reference(&[(&xs, &ys, false)]);
        }

        let sliver_xs = [
            19.343_75,
            -30.765_625,
            -18.265_625,
            0.140_625,
            -3.109_375,
            30.359_375,
            19.343_75,
        ];
        let sliver_ys = [
            59.031_25,
            -91.296_875,
            -53.796_875,
            1.421_875_000_000_000_4,
            -8.328_125,
            92.078_125,
            59.031_25,
        ];
        assert_area_bits_match_reference(&[(&sliver_xs, &sliver_ys, false)]);

        let (lo, hi) = (2.0_f64.powi(-60), 1.0 - 2.0_f64.powi(-53));
        let shell_xs = [0.0, 1.0, 1.0, 0.5, 0.0, 0.0];
        let shell_ys = [0.0, 0.0, 1.0, 1.0 + 2.0_f64.powi(-52), 1.0, 0.0];
        let hole_xs = [0.0, 0.0, 1.0, 1.0, 0.0];
        let hole_ys = [0.0, 1.0, 1.0, 0.0, 0.0];
        let thin_hole_xs = [lo, lo, hi, hi, lo];
        let thin_hole_ys = [lo, hi, hi, lo, lo];
        assert_area_bits_match_reference(&[
            (&shell_xs, &shell_ys, false),
            (&hole_xs, &hole_ys, true),
        ]);
        assert_area_bits_match_reference(&[
            (
                &[0.0, 1.0, 1.0, 0.0, 0.0],
                &[0.0, 0.0, 1.0, 1.0, 0.0],
                false,
            ),
            (&thin_hole_xs, &thin_hole_ys, true),
        ]);
    }

    #[test]
    fn rational_area_accumulator_rounds_selected_fraction_once() {
        let exact = |value| ExactDyadic::from_f64(value);
        let mut area = ExactRationalArea::new();
        area.add_homogeneous_cross(&[exact(0.0), exact(0.0), exact(1.0)], &[
            exact(1.0),
            exact(0.0),
            exact(1.0),
        ])
        .expect("first edge");
        area.add_homogeneous_cross(&[exact(1.0), exact(0.0), exact(1.0)], &[
            exact(0.0),
            exact(1.0),
            exact(1.0),
        ])
        .expect("second edge");
        area.add_homogeneous_cross(&[exact(0.0), exact(1.0), exact(1.0)], &[
            exact(0.0),
            exact(0.0),
            exact(1.0),
        ])
        .expect("third edge");
        assert_eq!(area.round_abs_half().expect("rounded area"), 0.5);

        let mut rational = ExactRationalArea::new();
        rational
            .add_fraction(exact(1.0), exact(3.0))
            .expect("third");
        rational
            .add_fraction(exact(1.0), exact(6.0))
            .expect("sixth");
        assert_eq!(rational.round_abs_half().expect("rounded rational"), 0.25);
    }

    #[test]
    fn near_collinear_ring_preserves_original_product_sum() {
        // Exact doubled area is -485 / 2^56; translating to rounded
        // coordinates erased the only noncollinear vertex.
        let xs = [
            19.34375,
            -30.765_625,
            -18.265_625,
            0.140_625,
            -3.109_375,
            30.359_375,
            19.34375,
        ];
        let ys = [
            59.03125,
            -91.296_875,
            -53.796_875,
            1.421_875_000_000_000_4,
            -8.328_125,
            92.078_125,
            59.03125,
        ];
        assert_eq!(
            exact_ring_area_sign_columns(&xs, &ys, 6),
            AreaSign::Negative
        );
    }

    #[test]
    fn ring_sign_retains_product_tails_and_subnormal_products() {
        for epsilon in [2.0_f64.powi(-27), 2.0_f64.powi(-28)] {
            let xs = [0.0, 1.0, 1.0 - epsilon, 0.0];
            let ys = [0.0, 1.0 + epsilon, 1.0, 0.0];
            assert_eq!(
                exact_ring_area_sign_columns(&xs, &ys, 3),
                AreaSign::Positive
            );
        }
        let tiny = f64::from_bits(1);
        assert_eq!(
            exact_ring_area_sign_columns(&[0.0, tiny, 0.0, 0.0], &[0.0, 0.0, tiny, 0.0], 3),
            AreaSign::Positive
        );
    }

    #[test]
    fn translated_thin_triangle_retains_clockwise_sign() {
        let tiny = 2.0_f64.powi(-54);
        let xs = [1.0, 0.0, tiny, 1.0];
        let ys = [0.0, 1.0, 1.0, 0.0];
        assert_eq!(
            exact_ring_area_sign_columns(&xs, &ys, 3),
            AreaSign::Negative
        );
    }

    #[test]
    fn ring_sign_defers_underflowed_products_to_exact_sum() {
        for exponent in [-537, -600] {
            let tiny = 2.0_f64.powi(exponent);
            assert_eq!(
                exact_ring_area_sign_columns(&[0.0, tiny, 0.0, 0.0], &[0.0, 0.0, tiny, 0.0], 3),
                AreaSign::Positive,
                "2^{exponent} triangle"
            );
        }
    }

    #[test]
    fn ring_sign_defers_overflowing_differences_and_sums_to_exact_sum() {
        let huge = f64::MAX;
        // Both products are finite, but MAX - -MAX is not.
        assert_eq!(
            exact_ring_area_sign_columns(&[huge, -huge, 0.0, huge], &[1.0, 1.0, 0.0, 1.0], 3),
            AreaSign::Positive
        );
        // Each directed cross product is finite, but their sequential sum is not.
        assert_eq!(
            exact_ring_area_sign_columns(
                &[huge, huge, 0.0, 0.0, huge],
                &[0.0, 1.0, 1.0, 0.0, 0.0],
                4
            ),
            AreaSign::Positive
        );
    }

    #[test]
    fn rounded_filter_defers_residual_product_and_sum_overflow_to_exact_sum() {
        let huge = f64::MAX;
        // The second residual is -MAX - MAX, which is nonfinite.
        assert_eq!(
            exact_ring_area_sign_columns(&[huge, -huge, huge, huge], &[0.0, 0.0, 1.0, 0.0], 3),
            AreaSign::Negative
        );
        // Residual products overflow before their cross difference is formed.
        assert_eq!(
            exact_ring_area_sign_columns(
                &[0.0, huge, huge, 0.0, 0.0],
                &[0.0, 0.0, huge, huge, 0.0],
                4
            ),
            AreaSign::Positive
        );
        // Two finite MAX cross terms overflow the running translated sum.
        assert_eq!(
            exact_ring_area_sign_columns(
                &[0.0, huge, huge, 0.0, 0.0],
                &[0.0, 0.0, 1.0, 1.0, 0.0],
                4
            ),
            AreaSign::Positive
        );
    }

    #[test]
    fn rounded_residual_error_bound_covers_stored_dyadic_subtractions() {
        let tiny = f64::from_bits(1);
        // 2^-54 is lost when subtracting from one; its known exact residual
        // error must remain inside the residual-relative enclosure.
        let rounded = 2.0_f64.powi(-54) - 1.0;
        assert!(2.0_f64.powi(-54) <= rounded_residual_error_bound(rounded.abs()));
        let subnormal = 2.0 * tiny - 5.0 * tiny;
        assert_eq!(subnormal, -3.0 * tiny);
        assert!(rounded_residual_error_bound(subnormal.abs()) >= tiny);
        let signed_zero = -0.0_f64 - 0.0;
        assert_eq!(signed_zero, 0.0);
        assert!(rounded_residual_error_bound(signed_zero.abs()) >= tiny);
    }

    #[test]
    fn closed_column_zero_one_and_two_edge_cycles_are_zero() {
        assert_eq!(
            exact_ring_area_sign_columns(&[0.0], &[0.0], 0),
            AreaSign::Zero
        );
        assert_eq!(
            exact_ring_area_sign_columns(&[1.0, 1.0], &[-1.0, -1.0], 1),
            AreaSign::Zero
        );
        assert_eq!(
            exact_ring_area_sign_columns(&[0.0, 1.0, 0.0], &[0.0, 0.0, 0.0], 2),
            AreaSign::Zero
        );
    }

    #[test]
    fn rounded_translated_rings_preserve_both_orientations() {
        let offset = 1.0e7;
        let positive_xs = [offset, offset + 1.0, offset, offset];
        let positive_ys = [offset, offset, offset + 1.0, offset];
        let negative_xs = [offset, offset, offset + 1.0, offset];
        let negative_ys = [offset, offset + 1.0, offset, offset];
        assert_eq!(
            exact_ring_area_sign_columns(&positive_xs, &positive_ys, 3),
            AreaSign::Positive
        );
        assert_eq!(
            exact_ring_area_sign_columns(&negative_xs, &negative_ys, 3),
            AreaSign::Negative
        );
    }

    #[test]
    fn open_thin_triangle_retains_clockwise_sign() {
        let points = [
            XY::new(1.0, 0.0),
            XY::new(0.0, 1.0),
            XY::new(2.0_f64.powi(-54), 1.0),
        ];
        assert_eq!(open_xy_cycle_sign(&points), AreaSign::Negative);
        let reversed = [points[2], points[1], points[0]];
        assert_eq!(open_xy_cycle_sign(&reversed), AreaSign::Positive);
        let point_cycle = [
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(2.0_f64.powi(-54), 1.0),
        ];
        assert_eq!(exact_open_point_sign(&point_cycle), AreaSign::Negative);
    }

    /// Raw products for these rings overflow, so the scalar certificate must
    /// decline and leave their orientation to the exact sum.
    #[test]
    fn rings_spanning_the_float_range_keep_their_orientation() {
        let huge = f64::MAX;
        // A counter-clockwise triangle and its mirror, both far wider than any
        // finite subtraction can express.
        let ccw_xs = [-huge, huge, 0.0, -huge];
        let ccw_ys = [-huge, -huge, huge, -huge];
        let cw_xs = [-huge, 0.0, huge, -huge];
        let cw_ys = [-huge, huge, -huge, -huge];
        assert_eq!(
            exact_ring_area_sign_columns(&ccw_xs, &ccw_ys, 3),
            AreaSign::Positive
        );
        assert_eq!(
            exact_ring_area_sign_columns(&cw_xs, &cw_ys, 3),
            AreaSign::Negative
        );
    }

    #[test]
    fn packed_polygon_measure_preserves_extreme_semantic_values() {
        let mixed_xs = [0.0, f64::from_bits(1), 2.0_f64.powi(-20), 0.0];
        let mixed_ys = [0.0, f64::from_bits(2), 2.0_f64.powi(-20), 0.0];
        assert_eq!(
            polygon_area_measure_columns(&mixed_xs, &mixed_ys, &[0, 4], 0..1),
            0.0
        );

        let huge_xs = [0.0, 1e300, 1e300, 0.0, 0.0];
        let huge_ys = [0.0, 0.0, 1e300, 1e300, 0.0];
        let huge = polygon_area_measure_columns(&huge_xs, &huge_ys, &[0, 5], 0..1);
        assert!(huge.is_infinite() && huge.is_sign_positive());
    }

    #[test]
    fn shell_hole_cancellation_rounds_the_exact_area_once() {
        type Columns<'a> = (&'a [f64], &'a [f64]);
        let (lo, hi) = (2.0_f64.powi(-60), 1.0 - 2.0_f64.powi(-53));
        let cases: [(Columns<'_>, Columns<'_>, f64); 2] = [
            // The shell bumps 2^-52 above its equal hole: both rings round to
            // one binary64 area, and their exact difference is 2^-53.
            (
                (&[0.0, 1.0, 1.0, 0.5, 0.0, 0.0], &[
                    0.0,
                    0.0,
                    1.0,
                    1.0 + 2.0_f64.powi(-52),
                    1.0,
                    0.0,
                ]),
                (&[0.0, 0.0, 1.0, 1.0, 0.0], &[0.0, 1.0, 1.0, 0.0, 0.0]),
                2.0_f64.powi(-53),
            ),
            // A valid thin annulus whose rounded rings leave a finite
            // difference with no reliable bits.
            (
                (&[0.0, 1.0, 1.0, 0.0, 0.0], &[0.0, 0.0, 1.0, 1.0, 0.0]),
                (&[lo, lo, hi, hi, lo], &[lo, hi, hi, lo, lo]),
                2.0_f64.powi(-52) + 2.0_f64.powi(-59),
            ),
        ];
        for ((shell_xs, shell_ys), (hole_xs, hole_ys), expected) in cases {
            let scalar = polygon_area_measure_with(|visit| {
                visit(shell_xs, shell_ys, false);
                visit(hole_xs, hole_ys, true);
            });
            assert_eq!(scalar.to_bits(), expected.to_bits(), "{scalar:e}");
            let xs = [shell_xs, hole_xs].concat();
            let ys = [shell_ys, hole_ys].concat();
            let offsets = [0, shell_xs.len() as i32, xs.len() as i32];
            let packed = polygon_area_measure_columns(&xs, &ys, &offsets, 0..2);
            assert_eq!(packed.to_bits(), expected.to_bits(), "{packed:e}");
        }
    }

    #[test]
    fn packed_polygon_area_fuses_short_rings_without_losing_holes() {
        let xs = [
            500_000.0, 500_010.0, 500_010.0, 500_000.0, 500_000.0, 500_002.0, 500_002.0, 500_008.0,
            500_008.0, 500_002.0,
        ];
        let ys = [
            5_200_000.0,
            5_200_000.0,
            5_200_010.0,
            5_200_010.0,
            5_200_000.0,
            5_200_002.0,
            5_200_008.0,
            5_200_008.0,
            5_200_002.0,
            5_200_002.0,
        ];
        assert_eq!(
            polygon_area_measure_columns(&xs, &ys, &[0, 5, 10], 0..2),
            64.0
        );
    }

    #[test]
    fn packed_polygon_area_falls_back_on_shell_hole_cancellation() {
        let xs = [0.0, 10.0, 10.0, 0.0, 0.0, 0.0, 10.0, 10.0, 0.0, 0.0];
        let ys = [0.0, 0.0, 10.0, 10.0, 0.0, 0.0, 0.0, 10.0, 10.0, 0.0];
        // The second ring has the same magnitude as the shell. An unsigned
        // ring fold would return 200 rather than the contract's zero.
        assert_eq!(
            polygon_area_measure_columns(&xs, &ys, &[0, 5, 10], 0..2),
            0.0
        );
    }

    #[test]
    fn thin_open_cycle_and_closed_polygon_round_to_known_area() {
        let tiny = 2.0_f64.powi(-54);
        let open = [XY::new(1.0, 0.0), XY::new(0.0, 1.0), XY::new(tiny, 1.0)];
        assert_eq!(
            open_xy_cycle_magnitude(&open).get().to_bits(),
            2.0_f64.powi(-55).to_bits()
        );
        let open_points = [
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(tiny, 1.0),
        ];
        assert_eq!(
            open_point_cycle_magnitude(&open_points).get().to_bits(),
            2.0_f64.powi(-55).to_bits()
        );

        // The middle rounded cross product is zero, while the exact doubled
        // area is 2^-54. The packed owner must take its exact fallback.
        let closed_xs = [0.0, 1.0, 0.999_999_992_549_419_4, 0.0];
        let closed_ys = [0.0, 1.000_000_007_450_580_6, 1.0, 0.0];
        assert_eq!(
            polygon_area_measure_columns(&closed_xs, &closed_ys, &[0, 4], 0..1).to_bits(),
            2.0_f64.powi(-55).to_bits()
        );
    }

    #[test]
    fn least_subnormal_rectangle_survives_scalar_polygon_area() {
        let tiny = f64::from_bits(1);
        let xs = [0.0, 1.0, 1.0, 0.0, 0.0];
        let ys = [0.0, 0.0, tiny, tiny, 0.0];
        let area = polygon_area_measure_with(|visit| visit(&xs, &ys, false));
        assert_eq!(area.to_bits(), tiny.to_bits());
    }

    #[test]
    fn utm_scale_irregular_polygon_area_matches_shifted_reference() {
        // Translating before the independent binary64 fold avoids catastrophic
        // cancellation at UTM coordinates; the raw-coordinate fold below must
        // visibly lose precision on this four-edge ring.
        let xs = [500_000.1_f64, 500_020.3, 500_019.7, 500_000.5, 500_000.1];
        let ys = [
            5_200_000.2_f64,
            5_200_000.1,
            5_200_010.4,
            5_200_010.3,
            5_200_000.2,
        ];
        let area = polygon_area_measure_with(|visit| visit(&xs, &ys, false));
        let reference = shifted_scalar_area_reference(&xs, &ys).abs();
        let unshifted = xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + (x0 * y1 - x1 * y0)
            })
            .abs()
            / 2.0;
        assert!(
            (area - reference).abs() <= 1e-12 * reference.max(1.0),
            "area={area} reference={reference}"
        );
        assert!(
            (unshifted - reference).abs() > 1e-5,
            "unshifted={unshifted} reference={reference}"
        );
        assert!(
            (unshifted - reference).abs() > (area - reference).abs() * 1e6,
            "unshifted={unshifted} area={area} reference={reference}"
        );
    }

    #[test]
    fn long_utm_circle_area_matches_shifted_reference() {
        // The 128-edge ring declines the packed short-ring fold and exercises
        // the shared-frame fallback at UTM coordinate magnitudes.
        let k = 128;
        let ox = 500_000.0_f64;
        let oy = 5_200_000.0_f64;
        let radius = 10.0_f64;
        let mut xs: Vec<f64> = (0..k)
            .map(|i| ox + (f64::from(i) * std::f64::consts::TAU / f64::from(k)).cos() * radius)
            .collect();
        let mut ys: Vec<f64> = (0..k)
            .map(|i| oy + (f64::from(i) * std::f64::consts::TAU / f64::from(k)).sin() * radius)
            .collect();
        xs.push(xs[0]);
        ys.push(ys[0]);
        let offsets = [0, xs.len() as i32];
        let area = polygon_area_measure_columns(&xs, &ys, &offsets, 0..1);
        let reference = shifted_scalar_area_reference(&xs, &ys).abs();
        let unshifted = xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + (x0 * y1 - x1 * y0)
            })
            .abs()
            / 2.0;
        assert!(
            (area - reference).abs() <= 1e-12 * reference.max(1.0),
            "area={area} reference={reference}"
        );
        assert!(
            (unshifted - reference).abs() > (area - reference).abs(),
            "unshifted={unshifted} area={area} reference={reference}"
        );
    }
}
