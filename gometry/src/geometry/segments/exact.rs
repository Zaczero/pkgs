//! Certified orientation and unfused double-double arithmetic for segments.
//! Orientation uses an outward interval and exact stored-dyadic fallback over
//! the complete finite binary64 domain.

use crate::geometry::segments::{Orientation, Segment, XY, interpolate_segment_point};
use crate::geometry::tessellation::exact::{Interval, two_sum};

#[path = "exact_orientation.rs"]
mod exact_orientation;
use exact_orientation::orientation_xy as certified_orientation_xy;

// Dekker's split constant for binary64: 2^ceil(53 / 2) + 1.
const SPLITTER: f64 = 134_217_729.0;
const MAX_SPLITTABLE: f64 = f64::MAX / SPLITTER;
const MAX_CROSS_COORD_EXPONENT: i32 = 480;
const MIN_CROSS_COORD_EXPONENT: i32 = -400;

/// Error-free product without FMA for finite, non-underflowing binary64
/// products, including mixed-exponent operands that overflow the splitter.
fn two_product(lhs: f64, rhs: f64) -> (f64, f64) {
    let head = lhs * rhs;
    if !head.is_finite() || head == 0.0 {
        return (head, 0.0);
    }
    if lhs.abs() <= MAX_SPLITTABLE && rhs.abs() <= MAX_SPLITTABLE {
        return two_product_safe(lhs, rhs, head);
    }

    // Normalize each factor independently so an ordinary mixed-exponent
    // product such as 2^1000 * 2^-1000 cannot overflow Dekker's splitter.
    let lhs_shift = scale_shift_for_magnitude(lhs.abs());
    let rhs_shift = scale_shift_for_magnitude(rhs.abs());
    let total_shift = lhs_shift + rhs_shift;
    let scaled_lhs = lhs * exact_power_of_two(lhs_shift);
    let scaled_rhs = rhs * exact_power_of_two(rhs_shift);
    let scaled_head = scaled_lhs * scaled_rhs;
    let (_, scaled_tail) = two_product_safe(scaled_lhs, scaled_rhs, scaled_head);
    let head_in_scaled_frame = head * exact_power_of_two(total_shift);
    let (delta, delta_tail) = two_sum(scaled_head, -head_in_scaled_frame);
    let error_in_scaled_frame = delta + (delta_tail + scaled_tail);
    (
        head,
        error_in_scaled_frame * exact_power_of_two(-total_shift),
    )
}

fn two_product_safe(lhs: f64, rhs: f64, head: f64) -> (f64, f64) {
    let (lhs_hi, lhs_lo) = split(lhs);
    let (rhs_hi, rhs_lo) = split(rhs);
    let error_1 = head - lhs_hi * rhs_hi;
    let error_2 = error_1 - lhs_lo * rhs_hi;
    let error_3 = error_2 - lhs_hi * rhs_lo;
    (head, lhs_lo * rhs_lo - error_3)
}

/// Dekker product with an explicit representability certificate for every
/// intermediate that the error-free-transform proof depends on. A subnormal
/// intermediate is not rejected for its magnitude; it is rejected because its
/// exact rounding residual may itself be below binary64 and therefore cannot
/// be carried by the returned tail.
fn two_product_certified(lhs: f64, rhs: f64) -> Option<(f64, f64)> {
    if !lhs.is_normal() || !rhs.is_normal() {
        return None;
    }
    let head = lhs * rhs;
    if !head.is_normal() {
        return None;
    }
    // Multiplication by an exact power of two only shifts the exponent. A
    // normal result therefore has no rounding tail and needs none of
    // Dekker's split intermediates.
    if normal_power_of_two(lhs) || normal_power_of_two(rhs) {
        return Some((head, 0.0));
    }
    let (lhs_hi, lhs_lo) = split_certified(lhs)?;
    let (rhs_hi, rhs_lo) = split_certified(rhs)?;
    let high_high = certified_product(lhs_hi, rhs_hi)?;
    let error_1 = certified_difference(head, high_high)?;
    let low_high = certified_product(lhs_lo, rhs_hi)?;
    let error_2 = certified_difference(error_1, low_high)?;
    let high_low = certified_product(lhs_hi, rhs_lo)?;
    let error_3 = certified_difference(error_2, high_low)?;
    let low_low = certified_product(lhs_lo, rhs_lo)?;
    let tail = certified_difference(low_low, error_3)?;
    Some((head, tail))
}

const fn normal_power_of_two(value: f64) -> bool {
    let bits = value.to_bits() & 0x7FFF_FFFF_FFFF_FFFF;
    let exponent = bits >> 52;
    exponent != 0 && exponent != 0x7FF && bits & ((1_u64 << 52) - 1) == 0
}

fn split_certified(value: f64) -> Option<(f64, f64)> {
    let scaled = SPLITTER * value;
    if !scaled.is_normal() {
        return None;
    }
    let large = certified_difference(scaled, value)?;
    let high = certified_difference(scaled, large)?;
    let low = certified_difference(value, high)?;
    Some((high, low))
}

fn certified_product(left: f64, right: f64) -> Option<f64> {
    let product = left * right;
    (product.is_normal() || binary_zero(left) || binary_zero(right)).then_some(product)
}

fn certified_difference(left: f64, right: f64) -> Option<f64> {
    let difference = left - right;
    (difference.is_normal() || same_stored_value(left, right)).then_some(difference)
}

const fn same_stored_value(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits() || (binary_zero(left) && binary_zero(right))
}

fn split(value: f64) -> (f64, f64) {
    let scaled = SPLITTER * value;
    let large = scaled - value;
    let high = scaled - large;
    (high, value - high)
}

pub(super) fn dd_diff(end: f64, start: f64) -> (f64, f64) {
    two_sum(end, -start)
}

fn dd_mul(lhs: (f64, f64), rhs: (f64, f64)) -> (f64, f64) {
    let (head, head_err) = two_product(lhs.0, rhs.0);
    two_sum(head, head_err + (lhs.0 * rhs.1 + lhs.1 * rhs.0))
}

pub(super) fn dd_cross(
    left_a: (f64, f64),
    left_b: (f64, f64),
    right_a: (f64, f64),
    right_b: (f64, f64),
) -> (f64, f64) {
    let left = dd_mul(left_a, left_b);
    let right = dd_mul(right_a, right_b);
    let (sum, sum_err) = two_sum(left.0, -right.0);
    two_sum(sum, sum_err + (left.1 - right.1))
}

pub(super) fn dd_div(numerator: (f64, f64), denominator: (f64, f64)) -> f64 {
    let estimate = numerator.0 / denominator.0;
    let product = dd_mul((estimate, 0.0), denominator);
    let (residual, residual_err) = two_sum(numerator.0, -product.0);
    let residual = residual + (residual_err + (numerator.1 - product.1));
    estimate + residual / denominator.0
}

#[derive(Clone, Copy)]
struct DdInterval {
    head: f64,
    tail: Interval,
}

impl DdInterval {
    const fn exact(value: f64) -> Self {
        Self {
            head: value,
            tail: Interval::exact(0.0),
        }
    }

    fn difference(left: f64, right: f64) -> Option<Self> {
        let (head, tail) = two_sum(left, -right);
        head.is_finite().then_some(Self {
            head,
            tail: Interval::exact(tail),
        })
    }

    fn add(self, other: Self) -> Option<Self> {
        let (head, tail) = two_sum(self.head, other.head);
        if !head.is_finite() {
            return None;
        }
        let tail = interval_add_nonzero(Interval::exact(tail), self.tail);
        let tail = interval_add_nonzero(tail, other.tail);
        tail.is_finite().then_some(Self { head, tail })
    }

    fn sub(self, other: Self) -> Option<Self> {
        let (head, tail) = two_sum(self.head, -other.head);
        if !head.is_finite() {
            return None;
        }
        let tail = interval_add_nonzero(Interval::exact(tail), self.tail);
        let tail = if interval_is_exact_zero(other.tail) {
            tail
        } else {
            tail.sub(other.tail)
        };
        tail.is_finite().then_some(Self { head, tail })
    }

    /// Outward enclosure of the exact product of the two represented values.
    /// The high-high product uses Dekker's error-free transform only when its
    /// result is normal (or algebraically zero); every cross term, including
    /// low-low, is then accumulated through the shared outward interval.
    fn mul(self, other: Self) -> Option<Self> {
        let head_is_zero = binary_zero(self.head) || binary_zero(other.head);
        let high_product = self.head * other.head;
        if !head_is_zero && !high_product.is_normal() {
            return None;
        }
        if !high_product.is_finite() {
            return None;
        }
        let (head, product_tail) = if head_is_zero {
            (high_product, 0.0)
        } else {
            two_product_certified(self.head, other.head)?
        };
        let mut tail = Interval::exact(product_tail);
        tail = interval_add_nonzero(
            tail,
            interval_mul_nonzero(Interval::exact(self.head), other.tail),
        );
        tail = interval_add_nonzero(
            tail,
            interval_mul_nonzero(Interval::exact(other.head), self.tail),
        );
        tail = interval_add_nonzero(tail, interval_mul_nonzero(self.tail, other.tail));
        tail.is_finite().then_some(Self { head, tail })
    }

    fn square(self) -> Option<Self> {
        let head_is_zero = binary_zero(self.head);
        let high_product = self.head * self.head;
        if !head_is_zero && !high_product.is_normal() {
            return None;
        }
        if !high_product.is_finite() {
            return None;
        }
        let (head, product_tail) = if head_is_zero {
            (high_product, 0.0)
        } else {
            two_product_certified(self.head, self.head)?
        };
        let doubled_head = self.head * 2.0;
        if !doubled_head.is_finite() {
            return None;
        }
        let mut tail = Interval::exact(product_tail);
        tail = interval_add_nonzero(
            tail,
            interval_mul_nonzero(Interval::exact(doubled_head), self.tail),
        );
        tail = interval_add_nonzero(tail, interval_mul_nonzero(self.tail, self.tail));
        tail.is_finite().then_some(Self { head, tail })
    }

    fn scale(self, value: f64) -> Option<Self> {
        self.mul(Self::exact(value))
    }

    fn bounds(self) -> Interval {
        if interval_is_exact_zero(self.tail) {
            Interval::exact(self.head)
        } else {
            Interval::exact(self.head).add(self.tail)
        }
    }

    fn approximate_pair(self) -> (f64, f64) {
        let midpoint = self.tail.lo * 0.5 + self.tail.hi * 0.5;
        (self.head, midpoint)
    }
}

const fn binary_zero(value: f64) -> bool {
    value.to_bits().trailing_zeros() >= 63
}

const fn interval_is_exact_zero(value: Interval) -> bool {
    binary_zero(value.lo) && binary_zero(value.hi)
}

fn interval_add_nonzero(left: Interval, right: Interval) -> Interval {
    if interval_is_exact_zero(left) {
        right
    } else if interval_is_exact_zero(right) {
        left
    } else {
        left.add(right)
    }
}

fn interval_mul_nonzero(left: Interval, right: Interval) -> Interval {
    if interval_is_exact_zero(left) || interval_is_exact_zero(right) {
        Interval::exact(0.0)
    } else if left.lo.to_bits() == left.hi.to_bits() && right.lo.to_bits() == right.hi.to_bits() {
        let product = left.lo * right.lo;
        Interval {
            lo: product.next_down(),
            hi: product.next_up(),
        }
    } else {
        left.mul(right)
    }
}

fn dd_interval_dot(
    left_a: DdInterval,
    left_b: DdInterval,
    right_a: DdInterval,
    right_b: DdInterval,
) -> Option<DdInterval> {
    left_a.mul(left_b)?.add(right_a.mul(right_b)?)
}

fn dd_interval_squared_norm(x: DdInterval, y: DdInterval) -> Option<DdInterval> {
    x.square()?.add(y.square()?)
}

fn half_ulp_margin(value: f64) -> Option<f64> {
    if !value.is_normal() {
        return None;
    }
    let below = (value - value.next_down()) * 0.5;
    let above = (value.next_up() - value) * 0.5;
    let margin = below.min(above);
    (margin > 0.0 && margin.is_finite()).then_some(margin)
}

fn residual_inside_rounding_cell(
    residual: DdInterval,
    denominator: DdInterval,
    rounded: f64,
) -> bool {
    let Some(margin) = half_ulp_margin(rounded) else {
        return false;
    };
    let residual = residual.bounds();
    let denominator = denominator.bounds();
    if !residual.is_finite() || !denominator.is_finite() || denominator.lo <= 0.0 {
        return false;
    }
    let residual_magnitude = residual.lo.abs().max(residual.hi.abs());
    let rounding_radius = Interval::exact(margin).mul(denominator).lo;
    residual_magnitude < rounding_radius
}

/// Prove that `fraction` is the correctly rounded exact ratio and return the
/// correctly rounded exact foot on both axes. The ordinary interpolation is
/// retained when it already occupies those cells; otherwise a compensated
/// coordinate proposal is admitted only after the same proof. The comparison
/// is strict, so midpoint ties and every range-error ambiguity decline.
fn certified_projection_coordinate(
    start: f64,
    delta: DdInterval,
    initial: f64,
    fraction: f64,
    ratio_residual: DdInterval,
    denominator: DdInterval,
) -> Option<f64> {
    let coordinate_residual = |rounded| {
        let scaled_delta = delta.scale(fraction)?;
        let affine = DdInterval::exact(start)
            .add(scaled_delta)?
            .sub(DdInterval::exact(rounded))?;
        // D * (exact_foot - rounded) =
        // D * (start + fraction * delta - rounded)
        //   + delta * (N - fraction * D).
        affine.mul(denominator)?.add(delta.mul(ratio_residual)?)
    };

    let initial_residual = coordinate_residual(initial)?;
    if residual_inside_rounding_cell(initial_residual, denominator, initial) {
        return Some(initial);
    }
    let correction = dd_div(
        initial_residual.approximate_pair(),
        denominator.approximate_pair(),
    );
    let corrected = initial + correction;
    let corrected_residual = coordinate_residual(corrected)?;
    residual_inside_rounding_cell(corrected_residual, denominator, corrected).then_some(corrected)
}

fn certified_projection_fraction(
    fraction: f64,
    segment: Segment,
    dx: DdInterval,
    dy: DdInterval,
    numerator: DdInterval,
    denominator: DdInterval,
) -> Option<XY> {
    let ratio_residual = certified_projection_ratio(fraction, numerator, denominator)?;

    let foot = interpolate_segment_point(segment.start, segment.end, fraction);
    Some(XY::new(
        certified_projection_coordinate(
            segment.start.x,
            dx,
            foot.x,
            fraction,
            ratio_residual,
            denominator,
        )?,
        certified_projection_coordinate(
            segment.start.y,
            dy,
            foot.y,
            fraction,
            ratio_residual,
            denominator,
        )?,
    ))
}

fn certified_projection_ratio(
    fraction: f64,
    numerator: DdInterval,
    denominator: DdInterval,
) -> Option<DdInterval> {
    if !fraction.is_normal() || !(0.0..1.0).contains(&fraction) {
        return None;
    }
    let scaled_denominator = denominator.scale(fraction)?;
    let ratio_residual = numerator.sub(scaled_denominator)?;
    if !residual_inside_rounding_cell(ratio_residual, denominator, fraction) {
        return None;
    }
    Some(ratio_residual)
}

/// Certified B-stage for opposing projection products. The raw quotient is
/// tried first to retain ordinary ratio and witness bits. A compensated
/// quotient is merely a second candidate, and its reconstructed coordinates
/// cannot leave this function until the shared outward interval proves every
/// affected rounding cell.
pub(super) fn certified_projection(
    point: XY,
    segment: Segment,
    raw_fraction: f64,
) -> Option<(f64, XY)> {
    let dx = DdInterval::difference(segment.end.x, segment.start.x)?;
    let dy = DdInterval::difference(segment.end.y, segment.start.y)?;
    let qx = DdInterval::difference(point.x, segment.start.x)?;
    let qy = DdInterval::difference(point.y, segment.start.y)?;
    let numerator = dd_interval_dot(qx, dx, qy, dy)?;
    let denominator = dd_interval_squared_norm(dx, dy)?;

    if certified_projection_ratio(raw_fraction, numerator, denominator).is_some() {
        return Some((
            raw_fraction,
            interpolate_segment_point(segment.start, segment.end, raw_fraction),
        ));
    }
    let compensated = dd_div(numerator.approximate_pair(), denominator.approximate_pair());
    if compensated.to_bits() == raw_fraction.to_bits() {
        return None;
    }
    certified_projection_fraction(compensated, segment, dx, dy, numerator, denominator)
        .map(|foot| (compensated, foot))
}

pub(super) fn segment_pair_scale_shifts(left: Segment, right: Segment) -> (i16, i16) {
    let points = [left.start, left.end, right.start, right.end];
    let largest_x = points
        .into_iter()
        .map(|point| point.x.abs())
        .fold(0.0_f64, f64::max);
    let largest_y = points
        .into_iter()
        .map(|point| point.y.abs())
        .fold(0.0_f64, f64::max);
    (
        scale_shift_for_magnitude(largest_x),
        scale_shift_for_magnitude(largest_y),
    )
}

fn scale_shift_for_magnitude(largest: f64) -> i16 {
    if largest == 0.0 {
        return 0;
    }
    let exponent = floor_binary_exponent(largest);
    let shift = if exponent > MAX_CROSS_COORD_EXPONENT {
        MAX_CROSS_COORD_EXPONENT - exponent
    } else if exponent < MIN_CROSS_COORD_EXPONENT {
        MIN_CROSS_COORD_EXPONENT - exponent
    } else {
        0
    };
    i16::try_from(shift).expect("binary64 normalization shift fits i16")
}

fn floor_binary_exponent(value: f64) -> i32 {
    debug_assert!(value.is_finite() && value > 0.0);
    let bits = value.to_bits();
    let stored = ((bits >> 52) & 0x7FF) as i32;
    if stored != 0 {
        stored - 1023
    } else {
        let significand = bits & ((1_u64 << 52) - 1);
        debug_assert_ne!(significand, 0);
        let highest_bit = 63 - significand.leading_zeros() as i32;
        -1074 + highest_bit
    }
}

pub(super) fn exact_power_of_two(shift: i16) -> f64 {
    let stored = i32::from(shift) + 1023;
    debug_assert!((1..=2046).contains(&stored));
    f64::from_bits((stored as u64) << 52)
}

pub(in crate::geometry) fn orientation_xy(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    cx: f64,
    cy: f64,
) -> Orientation {
    certified_orientation_xy(ax, ay, bx, by, cx, cy)
}

pub(crate) fn orientation(a: impl Into<XY>, b: impl Into<XY>, c: impl Into<XY>) -> Orientation {
    let (a, b, c) = (a.into(), b.into(), c.into());
    orientation_xy(a.x, a.y, b.x, b.y, c.x, c.y)
}

/// Shewchuk's A-stage `orient2d` filter constant: a float determinant whose
/// magnitude exceeds this multiple of the two term magnitudes summed has the
/// exact sign (`(3 + 16eps) * eps` with `eps = 2^-53`).
pub(in crate::geometry) const CCW_ERRBOUND_A: f64 =
    (3.0 + 16.0 * (f64::EPSILON / 2.0)) * (f64::EPSILON / 2.0);

/// Exact ray-crossing decision for one straddling ring edge: does `a -> b`
/// cross the horizontal ray through `p` strictly RIGHT of `p`?
///
/// Callers must have established the straddle precondition
/// `(ay > py) != (by > py)` (which also guarantees `by != ay`). Equivalent to
/// the division form `px < (bx - ax) * ((py - ay) / (by - ay)) + ax`, but
/// decided from the robust orientation sign: an upward edge (`by > ay`)
/// crosses right of `p` iff `p` lies strictly counter-clockwise of `a -> b`;
/// a downward edge flips the reading. The division form materializes the
/// intersection X, so opposite-sign huge coordinates overflow `bx - ax` to
/// infinity and corrupt the crossing parity (wrong `contains` near
/// |x| ~ 1e308); the orientation route inherits `orientation_xy`'s
/// exponent-normalized exact fallback instead.
pub(in crate::geometry) fn ray_crossing_is_right(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    px: f64,
    py: f64,
) -> bool {
    debug_assert!((ay > py) != (by > py), "ray-crossing straddle precondition");
    // All-input Ozaki filter first, textually matching the SIMD kernel. The
    // minimum-normal term covers underflow; uncertainty reaches the exact
    // stored-dyadic predicate.
    // Every escape hatch falls through the same comparison: a NaN or +-inf
    // `det` (term overflow) and a zero/near-tie `det` all fail `>`, taking
    // the exponent-normalized exact route below.
    let t1 = (bx - ax) * (py - ay);
    let t2 = (px - ax) * (by - ay);
    let det = t1 - t2;
    if det.abs() > ((t1.abs() + t2.abs()) + f64::MIN_POSITIVE) * CCW_ERRBOUND_A {
        return (det > 0.0) == (by > ay);
    }
    match orientation_xy(ax, ay, bx, by, px, py) {
        Orientation::Collinear => false,
        orientation => (orientation == Orientation::CounterClockwise) == (by > ay),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ray_filter_declines_wrong_nonzero_underflow_sign() {
        let mu = f64::from_bits(1);
        let a = (1.0, -4096.0 * mu);
        let b = (-2.0_f64.powi(-12), 6144.0 * mu);
        let p = (f64::from_bits(0x3FD9_9733_3333_3333), 2048.0 * mu);
        assert_ne!(a.1 > p.1, b.1 > p.1);
        let t1 = (b.0 - a.0) * (p.1 - a.1);
        let t2 = (p.0 - a.0) * (b.1 - a.1);
        let determinant = t1 - t2;
        let bound = ((t1.abs() + t2.abs()) + f64::MIN_POSITIVE) * CCW_ERRBOUND_A;
        assert_eq!(determinant.to_bits(), (-mu).to_bits());
        assert!(determinant.abs() <= bound);
        assert!(ray_crossing_is_right(a.0, a.1, b.0, b.1, p.0, p.1));
    }
}
