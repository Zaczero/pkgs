#[cfg(test)]
use super::h3_boundary::degrees_to_radians_bound;
#[cfg(test)]
use super::vectors::strict_minor_longitude_delta;
use super::{
    Ball, Bound, HALF_PI_HI, HALF_PI_LO, Midpoint, PI_HI, PI_LO, QUARTER_PI_HI, QUARTER_PI_LO,
};

/// Certified sine/cosine of a bounded radian argument.  The returned balls
/// enclose mathematical values; inability to prove a unique reduction is an
/// ordinary fail-open result (`None`).
pub(super) fn sin_cos(input: Bound) -> Option<(Ball, Ball)> {
    let (reduced, quadrant) = reduce_quadrant(input)?;
    let (sin, cos) = sin_cos_small(reduced)?;
    match quadrant.rem_euclid(4) {
        0 => Some((sin, cos)),
        1 => Some((cos, signed_ball(sin, -1)?)),
        2 => Some((signed_ball(sin, -1)?, signed_ball(cos, -1)?)),
        3 => Some((signed_ball(cos, -1)?, sin)),
        _ => unreachable!("remainder modulo four is in 0..4"),
    }
}
/// The inverse-bracket verifier may need a stricter enclosure than the B3
/// scalar fast filter's fixed 128-epsilon radius.  It keeps the same certified
/// reduction and quadrant proof, then uses only the outward degree-27/28
/// evaluator.  This is a bounded proof refinement, never a libm fallback.
pub(super) fn sin_cos_for_inverse(input: Bound) -> Option<(Ball, Ball)> {
    let (reduced, quadrant) = reduce_quadrant(input)?;
    let (sin, cos) = sin_cos_fallback(reduced)?;
    match quadrant.rem_euclid(4) {
        0 => Some((sin, cos)),
        1 => Some((cos, signed_ball(sin, -1)?)),
        2 => Some((signed_ball(sin, -1)?, signed_ball(cos, -1)?)),
        3 => Some((signed_ball(cos, -1)?, sin)),
        _ => unreachable!("remainder modulo four is in 0..4"),
    }
}
/// Degree-13/14 fast polynomial over the certified `[-pi/4, pi/4]` range.
/// The error radius follows the frozen bound: input enclosure + the derived
/// 128 ulps.  There is intentionally no libm call in this path.
fn sin_cos_small(input: Bound) -> Option<(Ball, Ball)> {
    if input.width() > 1.0 / 1_048_576.0 || input.lo < -QUARTER_PI_LO || input.hi > QUARTER_PI_HI {
        return sin_cos_fallback(input);
    }
    let x = input.lo.certified_midpoint(input.hi)?;
    let input_radius = outward_half_width(input)?;
    if x.abs() > QUARTER_PI_HI || !x.is_finite() {
        return None;
    }
    let z = x * x;
    let sin = x * horner(
        z,
        [
            -1.0 / 6.0,
            1.0 / 120.0,
            -1.0 / 5040.0,
            1.0 / 362_880.0,
            -1.0 / 39_916_800.0,
            1.0 / 6_227_020_800.0,
            -1.0 / 1_307_674_368_000.0,
        ],
        1.0,
    );
    let cos = horner(
        z,
        [
            -1.0 / 2.0,
            1.0 / 24.0,
            -1.0 / 720.0,
            1.0 / 40_320.0,
            -1.0 / 3_628_800.0,
            1.0 / 479_001_600.0,
            -1.0 / 87_178_291_200.0,
        ],
        1.0,
    );
    let radius = upward_sum([input_radius, 128.0 * f64::EPSILON])?;
    Some((Ball::new(sin, radius)?, Ball::new(cos, radius)?))
}
// Binary64 brackets of the large factorial denominators.  Writing the decimal
// integers directly would quietly round them before division; the explicit
// bits keep that fact visible.
const FACT_22: f64 = f64::from_bits(0x444E_7752_6159_F06C);
const FACT_23: f64 = f64::from_bits(0x4495_E5C3_35F8_A4CE);
const FACT_24: f64 = f64::from_bits(0x44E0_6C52_687A_7B9A);
const FACT_25: f64 = f64::from_bits(0x4529_A940_C33F_6121);
const FACT_26: f64 = f64::from_bits(0x4574_D984_9EA3_7EEB);
const FACT_27: f64 = f64::from_bits(0x45C1_9787_E5D9_F316);
const FACT_28: f64 = f64::from_bits(0x460E_C92D_D23D_6967);
const FACT_29: f64 = f64::from_bits(0x465B_E651_8687_A785);
const FACT_30: f64 = f64::from_bits(0x46AA_27EC_6E1F_2D0D);
/// `k!` in binary64 for `k` in `2..=30`, each within half an ulp of the
/// integer it denotes (exact through `18!`).
const FACTORIALS: [f64; 29] = [
    2.0,
    6.0,
    24.0,
    120.0,
    720.0,
    5_040.0,
    40_320.0,
    362_880.0,
    3_628_800.0,
    39_916_800.0,
    479_001_600.0,
    6_227_020_800.0,
    87_178_291_200.0,
    1_307_674_368_000.0,
    20_922_789_888_000.0,
    355_687_428_096_000.0,
    6_402_373_705_728_000.0,
    121_645_100_408_832_000.0,
    2_432_902_008_176_640_000.0,
    51_090_942_171_709_440_000.0,
    FACT_22,
    FACT_23,
    FACT_24,
    FACT_25,
    FACT_26,
    FACT_27,
    FACT_28,
    FACT_29,
    FACT_30,
];
const RECIPROCAL_FACTORIALS: [Bound; 29] = {
    let mut table = [Bound { lo: 0.0, hi: 0.0 }; 29];
    let mut index = 0;
    while index < FACTORIALS.len() {
        let factorial = FACTORIALS[index];
        table[index] = Bound {
            lo: (1.0 / factorial.next_up()).next_down(),
            hi: (1.0 / factorial.next_down()).next_up(),
        };
        index += 1;
    }
    table
};
/// `(-1)^(k/2) / k!` for the Taylor term of order `k`.
fn taylor_coefficient(order: usize) -> Bound {
    let index = order
        .checked_sub(2)
        .expect("Taylor coefficients start at order two");
    let value = *RECIPROCAL_FACTORIALS
        .get(index)
        .expect("Taylor order fits the reciprocal-factorial table");
    if (order / 2) % 2 == 1 {
        value.neg()
    } else {
        value
    }
}
/// Bounded high-degree fallback for a wide, but still uniquely reduced,
/// interval.  The next alternating term is included symmetrically, so this
/// path remains a proof instead of a retry loop with a fitted tolerance.
fn sin_cos_fallback(input: Bound) -> Option<(Ball, Ball)> {
    // A uniquely chosen quadrant may still enclose an artificial +/-pi/4
    // reduction edge because every pi operation is outward.  The degree-27/28
    // alternating proof remains valid through pi/2, so retain the enclosure
    // instead of silently selecting a rounded side of that non-structural cut.
    if input.lo < -HALF_PI_LO || input.hi > HALF_PI_HI {
        return None;
    }
    let z = input.mul(input)?;
    let sin = input.mul(alternating_horner(z, (3..28).step_by(2))?)?;
    let cos = alternating_horner(z, (2..29).step_by(2))?;
    let absolute = input.abs();
    let sin_remainder = integer_power_bound(absolute, 29)?.mul(RECIPROCAL_FACTORIALS[29 - 2])?;
    let cos_remainder = integer_power_bound(absolute, 30)?.mul(RECIPROCAL_FACTORIALS[30 - 2])?;
    let sin = widen_symmetric(sin, sin_remainder)?;
    let cos = widen_symmetric(cos, cos_remainder)?;
    Some((Ball::from_bound(sin)?, Ball::from_bound(cos)?))
}
/// Horner evaluation of `1 + z*(c_a + z*(c_b + ...))` over the Taylor
/// coefficients of the given orders. This is the fallback's outward interval
/// path, deliberately separate from B3's compact scalar-plus-radius fast
/// filter.
fn alternating_horner(z: Bound, orders: impl DoubleEndedIterator<Item = usize>) -> Option<Bound> {
    let mut orders = orders.rev();
    let mut value = taylor_coefficient(orders.next()?);
    for order in orders {
        value = taylor_coefficient(order).add(z.mul(value)?)?;
    }
    Bound::exact(1.0)?.add(z.mul(value)?)
}
pub(super) fn integer_power_bound(mut base: Bound, mut exponent: u8) -> Option<Bound> {
    let mut result = Bound::exact(1.0)?;
    while exponent != 0 {
        if exponent & 1 != 0 {
            result = result.mul(base)?;
        }
        exponent >>= 1;
        if exponent != 0 {
            base = base.mul(base)?;
        }
    }
    Some(result)
}
fn widen_symmetric(value: Bound, radius: Bound) -> Option<Bound> {
    if radius.lo < 0.0 {
        return None;
    }
    Bound::new(down(value.lo - radius.hi)?, up(value.hi + radius.hi)?)
}
/// Reduces a bounded value to a single quadrant.  `round()` is only a proposed
/// integer; the enclosing subtraction proves that it is the unique period.
fn reduce_quadrant(input: Bound) -> Option<(Bound, i64)> {
    let tau = Bound::new(2.0 * PI_LO, 2.0 * PI_HI)?;
    let midpoint = input.lo.certified_midpoint(input.hi)?;
    let period = (midpoint / (2.0 * PI_LO)).round();
    if !period.is_finite() || period.abs() > i64::MAX as f64 {
        return None;
    }
    let period = period as i64;
    let reduced = input.sub(tau.mul(Bound::exact(period as f64)?)?)?;
    if reduced.lo < -PI_LO || reduced.hi > PI_LO {
        return None;
    }
    // An enclosure which meets a non-zero multiple of 2*pi has no unique
    // periodic branch.  Exact zero is structural and remains a valid direct
    // sine/cosine input; `Bound` preserves it rather than widening 0*anything.
    if period != 0 && reduced.contains_zero() {
        return None;
    }

    // The second reduction is independent proof that the input lies in one
    // (and only one) quadrant.  A boundary-spanning enclosure is deliberately
    // undecidable rather than split at a rounded multiple of pi/2.
    let midpoint = reduced.lo.certified_midpoint(reduced.hi)?;
    let quadrant = (midpoint / HALF_PI_LO).round();
    if !quadrant.is_finite() || quadrant.abs() > i64::MAX as f64 {
        return None;
    }
    let quadrant = quadrant as i64;
    let quadrant_turn = Bound::new(HALF_PI_LO, HALF_PI_HI)?.mul(Bound::exact(quadrant as f64)?)?;
    // A value meeting a multiple of pi/2 has no unique quadrant.  In
    // particular, do not quietly choose a side merely because `round()` made
    // one plausible for the interval midpoint.
    if quadrant != 0 && reduced.intersection(quadrant_turn).is_some() {
        return None;
    }
    let small = reduced.sub(quadrant_turn)?;
    if quadrant == 0 && small.contains_zero() && !small.is_exact_zero() {
        return None;
    }
    (small.lo >= -HALF_PI_LO && small.hi <= HALF_PI_HI).then_some((small, quadrant))
}
fn signed_ball(value: Ball, sign: i8) -> Option<Ball> {
    match sign {
        1 => Some(value),
        -1 => Ball::new(-value.mid, value.rad),
        _ => None,
    }
}
fn horner<const N: usize>(x: f64, coefficients: [f64; N], constant: f64) -> f64 {
    let mut reversed = coefficients.into_iter().rev();
    let mut value = reversed
        .next()
        .expect("fixed nonempty polynomial coefficient table");
    for coefficient in reversed {
        value = coefficient + x * value;
    }
    constant + x * value
}
pub(super) fn finite_minmax(values: [f64; 4]) -> Option<(f64, f64)> {
    let mut iter = values.into_iter();
    let first = iter.next()?;
    if !first.is_finite() {
        return None;
    }
    iter.try_fold((first, first), |(lo, hi), value| {
        value.is_finite().then_some((lo.min(value), hi.max(value)))
    })
}
pub(super) fn up(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.next_up())
}
pub(super) fn down(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.next_down())
}
pub(super) fn ulp(value: f64) -> Option<f64> {
    if !value.is_finite() {
        return None;
    }
    Some((value.next_up() - value).max(value - value.next_down()))
}
pub(super) fn upward_sum<const N: usize>(values: [f64; N]) -> Option<f64> {
    let mut sum = 0.0;
    for value in values {
        if !value.is_finite() || value < 0.0 {
            return None;
        }
        sum = up(sum + value)?;
    }
    Some(sum)
}
pub(super) fn outward_half_width(value: Bound) -> Option<f64> {
    up((value.hi - value.lo) * 0.5)
}

#[cfg(test)]
mod tests;
