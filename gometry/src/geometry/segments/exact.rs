//! Exact-orientation fallbacks and unfused double-double arithmetic for the
//! segment kernel. Ordinary coordinates stay on robust::orient2d's fast path;
//! exponent normalization is reserved for overflow/underflow lanes.

use robust::{Coord, orient2d};

use super::{Orientation, Segment, XY};

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

fn split(value: f64) -> (f64, f64) {
    let scaled = SPLITTER * value;
    let large = scaled - value;
    let high = scaled - large;
    (high, value - high)
}

/// Error-free sum (Knuth two-sum): `head + tail == lhs + rhs` exactly.
fn two_sum(lhs: f64, rhs: f64) -> (f64, f64) {
    let sum = lhs + rhs;
    let rhs_part = sum - lhs;
    (sum, (lhs - (sum - rhs_part)) + (rhs - rhs_part))
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
    let a = Coord { x: ax, y: ay };
    let b = Coord { x: bx, y: by };
    let c = Coord { x: cx, y: cy };
    let raw = orient2d(a, b, c);
    // Preserve a finite nonzero adaptive result before any scaling, which
    // could erase a meaningful 2^-1000 offset beside a 2^1000 coordinate.
    if raw.is_finite() && raw != 0.0 {
        return classify_orientation(raw);
    }

    let largest_x = [ax, bx, cx]
        .into_iter()
        .map(f64::abs)
        .fold(0.0_f64, f64::max);
    let largest_y = [ay, by, cy]
        .into_iter()
        .map(f64::abs)
        .fold(0.0_f64, f64::max);
    let x_shift = scale_shift_for_magnitude(largest_x);
    let y_shift = scale_shift_for_magnitude(largest_y);
    if raw == 0.0 && x_shift == 0 && y_shift == 0 {
        // In the normal exponent frame, adaptive zero certifies collinearity;
        // avoid two more predicates for every point on a shared edge.
        return Orientation::Collinear;
    }

    // A far third point can make two mixed-exponent products `inf - inf`.
    // Cyclic permutations preserve orientation while changing that origin.
    for translated in [orient2d(b, c, a), orient2d(c, a, b)] {
        if translated.is_finite() && translated != 0.0 {
            return classify_orientation(translated);
        }
    }

    let (ax, ay, bx, by, cx, cy) = if x_shift == 0 && y_shift == 0 {
        (ax, ay, bx, by, cx, cy)
    } else {
        let x_scale = exact_power_of_two(x_shift);
        let y_scale = exact_power_of_two(y_shift);
        (
            ax * x_scale,
            ay * y_scale,
            bx * x_scale,
            by * y_scale,
            cx * x_scale,
            cy * y_scale,
        )
    };
    classify_orientation(orient2d(
        Coord { x: ax, y: ay },
        Coord { x: bx, y: by },
        Coord { x: cx, y: cy },
    ))
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
    // A-stage filter first, textually matching the SIMD `ray_crossing_lanes`
    // kernel: a determinant whose magnitude clears the Shewchuk bound has the
    // exact sign, so the common case never pays the adaptive `orient2d` call.
    // Every escape hatch falls through the same comparison: a NaN or +-inf
    // `det` (term overflow) and a zero/near-tie `det` all fail `>`, taking
    // the exponent-normalized exact route below.
    let t1 = (bx - ax) * (py - ay);
    let t2 = (px - ax) * (by - ay);
    let det = t1 - t2;
    if det.abs() > (t1.abs() + t2.abs()) * CCW_ERRBOUND_A {
        return (det > 0.0) == (by > ay);
    }
    match orientation_xy(ax, ay, bx, by, px, py) {
        Orientation::Collinear => false,
        orientation => (orientation == Orientation::CounterClockwise) == (by > ay),
    }
}

fn classify_orientation(value: f64) -> Orientation {
    if value == 0.0 {
        Orientation::Collinear
    } else if value > 0.0 {
        Orientation::CounterClockwise
    } else {
        Orientation::Clockwise
    }
}
