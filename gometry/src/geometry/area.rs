//! Typed signed-area lanes: measurement magnitude vs deterministic orientation
//! decisions. Topology never branches on a raw signed `f64`.

use std::simd::num::SimdFloat as _;

use crate::error::Result;
use crate::geometry::access::{REDUCE_LANES, ReduceSimd, crossover, simd_reduce_f64};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio};
use crate::geometry::types::{Coordinates, Point, XY};
use crate::geometry::{axis_pow2_scale, scaled_residual, unscale_area, wrap_index};

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

        let common = self.denominator.try_gcd(&denominator)?;
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
        let reduction = next_numerator.clone().abs().try_gcd(&next_denominator)?;
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct RingDecisionArea {
    sign: AreaSign,
    magnitude: AreaMeasure,
}

impl RingDecisionArea {
    pub(crate) const fn sign(self) -> AreaSign {
        self.sign
    }

    pub(crate) const fn magnitude(self) -> AreaMeasure {
        self.magnitude
    }
}

impl AreaSign {
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
    cycle_sign(pairs, |index| {
        (
            XY::new(xs[index], ys[index]),
            XY::new(xs[index + 1], ys[index + 1]),
        )
    })
}

fn cycle_sign(edge_count: usize, mut edge_at: impl FnMut(usize) -> (XY, XY)) -> AreaSign {
    if edge_count == 0 {
        return AreaSign::Zero;
    }
    if let Some(sign) = rounded_translated_cycle_sign(edge_count, &mut edge_at) {
        return sign;
    }

    let mut sum = ExactSum::zero();
    for index in 0..edge_count {
        let (a, b) = edge_at(index);
        sum.add_product(a.x, b.y);
        sum.subtract_product(b.x, a.y);
    }
    match sum.ordering() {
        std::cmp::Ordering::Less => AreaSign::Negative,
        std::cmp::Ordering::Equal => AreaSign::Zero,
        std::cmp::Ordering::Greater => AreaSign::Positive,
    }
}

/// One rounded-residual filter over a contiguous closed cycle. For finite
/// `r = fl(d)`, `(u*|r| + min_subnormal/2)/(1-u) <= EPSILON*|r| +
/// min_subnormal` bounds translation error against the original shoelace sum.
fn rounded_translated_cycle_sign(
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

    // A nonfinite residual contaminates a strict product and difference, then
    // persists through the sequential sum; reject before max-derived bounds.
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
    // Products, subtraction, and n-1 sequential additions contribute at most
    // 16u*n^2*m + 8*MIN_POSITIVE*n^2 for n <= 2^26.
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

/// A conservative absolute error bound for a finite rounded subtraction.
fn rounded_residual_error_bound(maximum_residual: f64) -> f64 {
    outward_add(
        outward_mul(f64::EPSILON, maximum_residual),
        f64::from_bits(1),
    )
}

fn outward_mul(left: f64, right: f64) -> f64 {
    (left * right).next_up()
}

fn outward_add(left: f64, right: f64) -> f64 {
    (left + right).next_up()
}

fn exact_open_xy_sign(points: &[XY]) -> AreaSign {
    let count = points.len();
    if count < 2 {
        return AreaSign::Zero;
    }
    cycle_sign(count, |index| {
        (points[index], points[wrap_index(index + 1, count)])
    })
}

fn exact_open_point_sign(points: &[Point]) -> AreaSign {
    let count = points.len();
    if count < 2 {
        return AreaSign::Zero;
    }
    cycle_sign(count, |index| {
        (
            points[index].xy(),
            points[wrap_index(index + 1, count)].xy(),
        )
    })
}

/// Ring vertex-pair counts below this use the scalar fold — LLVM auto-vectorizes
/// it to packed `mulpd`/`addpd` (asm-verified) with no per-ring horizontal
/// reduce; at or above it the 512-bit `simd_reduce_f64` path's extra ILP wins
/// (measured: scalar ties to ~64 verts, SIMD wins 15–25% at 256+). Centralized
/// in [`crossover`].
const SHOELACE_SIMD_MIN: usize = crossover::OFFSET_PAIR_MEASURE;

/// Origin-shifted, optionally per-axis-scaled shoelace half-sum / 2.
/// Unit scales keep the ordinary path bit-identical. Non-unit scales form
/// `x*sx - origin_x*sx` and `y*sy - origin_y*sy`.
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
    // Small rings (the overwhelmingly common case — most polygons have well
    // under ~64 vertices): a tight reassociation-friendly scalar fold over two
    // offset slices. LLVM auto-vectorizes it at the natural width and elides
    // bounds checks (the offset slices are length-`pairs`). The forced 512-bit
    // path below pays per-ring tail padding + a horizontal `reduce_sum`, which
    // only amortizes on long rings — many tiny per-ring reductions are NOT
    // stream-shaped (see the 512-bit-for-streams-only float policy).
    if pairs < SHOELACE_SIMD_MIN {
        let cross = xs[..pairs]
            .iter()
            .zip(&xs[1..=pairs])
            .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
            .fold(0.0_f64, |area, ((&x0, &x1), (&y0, &y1))| {
                // Ordinary path: plain origin shift (bit-identical). Scaled
                // rescue: `x*s - origin*s`, never `(x-origin)*s`.
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

/// Absolute roundoff bound of [`shoelace_shifted`] for one ring. The decisive
/// scale is the sum of the two products *before* each subtraction: a finite
/// nonzero cross term can already have lost its answer to product
/// cancellation, so bounding only the resulting value misses the `2^-27`
/// rotated triangle.
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
    // Each edge has two rounded products, one rounded subtraction, and one
    // contribution to the fold.  This conservative IEEE-754 roundoff bound
    // is a property of the performed arithmetic, not a geometry-size knob.
    let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
    product_scale * EPSILON * operations
}

fn closed_ring_sum_columns(xs: &[f64], ys: &[f64], pairs: usize) -> ExactSum {
    let mut sum = ExactSum::zero();
    for index in 0..pairs {
        sum.add_cross(xs[index], ys[index], xs[index + 1], ys[index + 1]);
    }
    sum
}

fn closed_ring_sum<C: Coordinates + ?Sized>(points: &C) -> ExactSum {
    if let Some((xs, ys)) = points.xy_columns() {
        return closed_ring_sum_columns(xs, ys, points.coord_count().saturating_sub(1));
    }
    let mut sum = ExactSum::zero();
    let mut coords = points.iter_coords();
    let Some(mut prev) = coords.next() else {
        return sum;
    };
    for next in coords {
        sum.add_cross(prev.x, prev.y, next.x, next.y);
        prev = next;
    }
    sum
}

fn open_cycle_sum_columns(xs: &[f64], ys: &[f64]) -> ExactSum {
    let mut sum = ExactSum::zero();
    let count = xs.len();
    if count < 2 {
        return sum;
    }
    for index in 0..count - 1 {
        sum.add_cross(xs[index], ys[index], xs[index + 1], ys[index + 1]);
    }
    sum.add_cross(xs[count - 1], ys[count - 1], xs[0], ys[0]);
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

#[cfg(test)]
pub(crate) fn ring_area_measure_columns(xs: &[f64], ys: &[f64], pairs: usize) -> AreaMeasure {
    unsigned_area(&closed_ring_sum_columns(xs, ys, pairs))
}

pub(crate) fn ring_area_measure<C: Coordinates + ?Sized>(points: &C) -> AreaMeasure {
    unsigned_area(&closed_ring_sum(points))
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
        let ring = shoelace_shifted(xs, ys, pairs, ox, oy, sx, sy).abs();
        rings += 1;
        magnitude += ring;
        error += shoelace_shifted_roundoff(xs, ys, pairs, ox, oy, sx, sy);
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
        for index in 0..pairs {
            ring.add_cross(xs[index], ys[index], xs[index + 1], ys[index + 1]);
        }
        let ordering = ring.ordering();
        if ordering == std::cmp::Ordering::Equal {
            return;
        }
        let negate = (ordering == std::cmp::Ordering::Less) ^ hole;
        for index in 0..pairs {
            if negate {
                total.subtract_product(xs[index], ys[index + 1]);
                total.add_product(xs[index + 1], ys[index]);
            } else {
                total.add_product(xs[index], ys[index + 1]);
                total.subtract_product(xs[index + 1], ys[index]);
            }
        }
    });
    total.round_to_area()
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
        let start = ring_offsets[ring_index] as usize;
        let end = ring_offsets[ring_index + 1] as usize;
        let pairs = end.saturating_sub(start + 1);
        let Some((signed, bound)) = fast_ring_area_columns(&xs[start..end], &ys[start..end], pairs)
        else {
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
            let start = ring_offsets[ring_index] as usize;
            let end = ring_offsets[ring_index + 1] as usize;
            visit(&xs[start..end], &ys[start..end], ring_index != rings.start);
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
    let ox = xs[0];
    let oy = ys[0];
    let mut cross = 0.0_f64;
    let mut product_scale = 0.0_f64;
    for ((&x0, &x1), (&y0, &y1)) in xs[..pairs]
        .iter()
        .zip(&xs[1..=pairs])
        .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
    {
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

pub(crate) fn ring_decision_area<C: Coordinates + ?Sized>(points: &C) -> RingDecisionArea {
    RingDecisionArea {
        sign: ring_area_sign(points),
        magnitude: ring_area_measure(points),
    }
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
    area_sign_to_winding(open_xy_cycle_sign(points))
}

pub(crate) fn open_xy_cycle_sign(points: &[XY]) -> AreaSign {
    exact_open_xy_sign(points)
}

pub(crate) fn open_point_cycle_winding(points: &[Point]) -> RingWinding {
    area_sign_to_winding(exact_open_point_sign(points))
}

pub(crate) fn open_xy_cycle_decision(points: &[XY]) -> RingDecisionArea {
    RingDecisionArea {
        sign: exact_open_xy_sign(points),
        magnitude: open_xy_cycle_magnitude(points),
    }
}

pub(crate) fn open_point_cycle_decision(points: &[Point]) -> RingDecisionArea {
    RingDecisionArea {
        sign: exact_open_point_sign(points),
        magnitude: open_point_cycle_magnitude(points),
    }
}

pub(crate) fn open_point_cycle_magnitude(points: &[Point]) -> AreaMeasure {
    unsigned_area(&open_cycle_sum(points))
}

pub(crate) fn open_xy_cycle_magnitude(points: &[XY]) -> AreaMeasure {
    unsigned_area(&open_cycle_sum(points))
}

fn cmp_closed_ring_magnitude<C: Coordinates + ?Sized>(left: &C, right: &C) -> std::cmp::Ordering {
    closed_ring_sum(left).compare_unsigned(&closed_ring_sum(right))
}

/// Float order when it distinguishes; original-coordinate ExactSum on a tie
/// (including `0`/`0` and `∞`/`∞`).
pub(crate) fn cmp_area_keys_then_closed<C: Coordinates + ?Sized>(
    left_key: f64,
    right_key: f64,
    left: &C,
    right: &C,
) -> std::cmp::Ordering {
    match left_key.total_cmp(&right_key) {
        std::cmp::Ordering::Equal => cmp_closed_ring_magnitude(left, right),
        ordering => ordering,
    }
}

pub(crate) fn cmp_area_keys_then_open_columns(
    left_key: f64,
    right_key: f64,
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> std::cmp::Ordering {
    match left_key.total_cmp(&right_key) {
        std::cmp::Ordering::Equal => open_cycle_sum_columns(left_xs, left_ys)
            .compare_unsigned(&open_cycle_sum_columns(right_xs, right_ys)),
        ordering => ordering,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Independent origin-shifted scalar shoelace reference for area tests.
    fn shifted_scalar_area(xs: &[f64], ys: &[f64]) -> f64 {
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
    fn exact_ring_area_matches_independent_reference_on_long_ring() {
        let xs: Vec<f64> = vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0,
        ];
        let ys: Vec<f64> = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
        ];
        let pairs = xs.len() - 1;
        let winding = closed_columns_winding(&xs, &ys, pairs);
        assert_eq!(winding, RingWinding::CounterClockwise);
        let measure = ring_area_measure_columns(&xs, &ys, pairs).get();
        // The independent fold is a semantic reference, not a component-layout
        // or SIMD representation contract.
        let scalar_reference = shifted_scalar_area(&xs, &ys);
        assert!(scalar_reference > 0.0);
        assert_eq!(measure.to_bits(), scalar_reference.to_bits());
    }

    #[test]
    fn exact_product_tails_keep_a_closed_near_collinear_ring() {
        // Every rounded cross term is zero, so this input proves the exact
        // original-coordinate product sum is used rather than merely
        // exercising the fast filtered sign path. Its stored-double twice-area
        // is 2^-54.
        let e = 2.0_f64.powi(-27);
        let xs = [0.0, 1.0, 1.0 - e, 0.0];
        let ys = [0.0, 1.0 + e, 1.0, 0.0];
        let pairs = xs.len() - 1;
        let rounded_terms: Vec<f64> = xs[..pairs]
            .iter()
            .zip(&xs[1..])
            .zip(ys[..pairs].iter().zip(&ys[1..]))
            .map(|((&x0, &x1), (&y0, &y1))| x0 * y1 - x1 * y0)
            .collect();
        assert_eq!(rounded_terms, [0.0, 0.0, 0.0]);
        assert_eq!(
            exact_ring_area_sign_columns(&xs, &ys, pairs),
            AreaSign::Positive
        );
        assert_eq!(
            ring_area_measure_columns(&xs, &ys, pairs).get().to_bits(),
            2.0_f64.powi(-55).to_bits()
        );
    }

    #[test]
    fn exact_ring_area_matches_shifted_reference_on_long_ring() {
        // A long stored ring exercises exact accumulation independently of the
        // internal measurement crossover.
        let k = 128;
        let mut xs: Vec<f64> = (0..k)
            .map(|i| (f64::from(i) * std::f64::consts::TAU / f64::from(k)).cos() * 100.0)
            .collect();
        let mut ys: Vec<f64> = (0..k)
            .map(|i| (f64::from(i) * std::f64::consts::TAU / f64::from(k)).sin() * 100.0)
            .collect();
        xs.push(xs[0]);
        ys.push(ys[0]);
        let pairs = xs.len() - 1;
        let measure = ring_area_measure_columns(&xs, &ys, pairs).get();
        let scalar = shifted_scalar_area(&xs, &ys).abs();
        assert!(
            (measure - scalar).abs() <= 1e-6 * scalar,
            "measure={measure} scalar={scalar}"
        );
    }

    #[test]
    fn exact_ring_area_large_absolute_coords_matches_reference() {
        // ~200 m² irregular building at UTM-scale easting/northing. Naive
        // shoelace cross-products (~2.6e12) cancel to ~200 with ~1e-4 absolute
        // error; origin-shifted measurement recovers the true area.
        let xs = [500_000.1_f64, 500_020.3, 500_019.7, 500_000.5, 500_000.1];
        let ys = [
            5_200_000.2_f64,
            5_200_000.1,
            5_200_010.4,
            5_200_010.3,
            5_200_000.2,
        ];
        let pairs = xs.len() - 1;
        let measure = ring_area_measure_columns(&xs, &ys, pairs).get();
        let reference = shifted_scalar_area(&xs, &ys).abs();
        let naive = xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + (x0 * y1 - x1 * y0)
            })
            .abs()
            / 2.0;
        assert!(
            (measure - reference).abs() <= 1e-12 * reference.max(1.0),
            "measure={measure} reference={reference} naive={naive}"
        );
        // The unshifted fold loses ~1e-4 absolute on these stored values.
        assert!(
            (naive - reference).abs() > 1e-5,
            "naive={naive} measure={measure} reference={reference}"
        );
        assert!(
            (naive - reference).abs() > (measure - reference).abs() * 1e6,
            "naive={naive} measure={measure} reference={reference}"
        );
    }

    #[test]
    fn exact_ring_area_large_absolute_long_ring_matches_reference() {
        // UTM-scale circle. Original-coordinate ExactSum agrees with an
        // independent shifted reference; unshifted products diverge.
        let k = 128;
        let ox = 500_000.0_f64;
        let oy = 5_200_000.0_f64;
        let r = 10.0_f64;
        let mut xs: Vec<f64> = (0..k)
            .map(|i| ox + (f64::from(i) * std::f64::consts::TAU / f64::from(k)).cos() * r)
            .collect();
        let mut ys: Vec<f64> = (0..k)
            .map(|i| oy + (f64::from(i) * std::f64::consts::TAU / f64::from(k)).sin() * r)
            .collect();
        xs.push(xs[0]);
        ys.push(ys[0]);
        let pairs = xs.len() - 1;
        let measure = ring_area_measure_columns(&xs, &ys, pairs).get();
        let shifted = shifted_scalar_area(&xs, &ys).abs();
        let naive = xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + (x0 * y1 - x1 * y0)
            })
            .abs()
            / 2.0;
        assert!(
            (measure - shifted).abs() <= 1e-12 * shifted.max(1.0),
            "measure={measure} shifted={shifted}"
        );
        assert!(
            (naive - shifted).abs() > (measure - shifted).abs(),
            "naive={naive} measure={measure} shifted={shifted}"
        );
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
    fn equal_float_keys_defer_to_exact_ring_order() {
        let width = 2.0_f64.powi(1023);
        let inset = 2.0_f64.powi(971);
        let outer = [
            XY::new(0.0, 0.0),
            XY::new(width, 0.0),
            XY::new(width, 8.0),
            XY::new(0.0, 8.0),
            XY::new(0.0, 0.0),
        ];
        let inner = [
            XY::new(inset, 1.0),
            XY::new(width - inset, 1.0),
            XY::new(width - inset, 7.0),
            XY::new(inset, 7.0),
            XY::new(inset, 1.0),
        ];
        let outer_key = ring_area_measure(&outer[..]).get();
        let inner_key = ring_area_measure(&inner[..]).get();
        assert!(outer_key.is_infinite() && inner_key.is_infinite());
        assert_eq!(outer_key.total_cmp(&inner_key), std::cmp::Ordering::Equal);
        assert_eq!(
            cmp_area_keys_then_closed(outer_key, inner_key, &outer[..], &inner[..]),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn thin_triangle_original_coords_round_to_known_area() {
        // Exact Σ = -2^-54 on the stored vertices; |Σ|/2 is exactly 2^-55.
        let tiny = 2.0_f64.powi(-54);
        let closed = [
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(tiny, 1.0),
            Point::new_raw_xy(1.0, 0.0),
        ];
        assert_eq!(
            ring_area_measure(&closed[..]).get().to_bits(),
            2.0_f64.powi(-55).to_bits()
        );
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
    }

    #[test]
    fn subnormal_ring_original_coords_round_to_least_subnormal() {
        // Unit-width rectangle of height 2^-1074 has exact area 2^-1074.
        let tiny = f64::from_bits(1);
        let points = [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(1.0, tiny),
            Point::new_raw_xy(0.0, tiny),
            Point::new_raw_xy(0.0, 0.0),
        ];
        assert_eq!(
            ring_area_measure(&points[..]).get().to_bits(),
            tiny.to_bits()
        );
    }
}
