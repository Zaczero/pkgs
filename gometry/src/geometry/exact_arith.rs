//! Error-free transformations and double-double arithmetic: the one home for
//! `two_sum`, `two_product`, and the `dd_*` kernels every exact predicate and
//! expansion builds on.
//!
//! The product is Dekker splitting with power-of-two rescaling, exact on every
//! finite, non-underflowing binary64 product including mixed-exponent operands
//! that would overflow the splitter. It is chosen over fused multiply-add
//! because `mul_add` is a libm call on the x86-64-v2 baseline. The two agree
//! bit for bit whenever the residual cannot underflow, which is every head of
//! magnitude at least 2^-969; below that the split may round the residual to
//! the subnormal grid, at most one subnormal ulp off, and the expansion
//! arithmetic takes [`two_product_exact`] instead. A test below pins both.

/// Error-free sum (Knuth two-sum): `head + tail == left + right` exactly
/// whenever the rounded head is finite.  Callers that need a fallible lane
/// check that head before using the pair; the shared transform itself keeps
/// the infallible EFT contract used by the segment and rectangle kernels.
pub(crate) fn two_sum(left: f64, right: f64) -> (f64, f64) {
    let sum = left + right;
    let right_virtual = sum - left;
    (
        sum,
        (left - (sum - right_virtual)) + (right - right_virtual),
    )
}

pub(crate) fn fast_two_sum(left: f64, right: f64) -> (f64, f64) {
    let sum = left + right;
    (sum, right - (sum - left))
}

pub(crate) const SPLITTER: f64 = 134_217_729.0;

pub(crate) const MAX_SPLITTABLE: f64 = f64::MAX / SPLITTER;

pub(crate) const MAX_CROSS_COORD_EXPONENT: i32 = 480;

pub(crate) const MIN_CROSS_COORD_EXPONENT: i32 = -400;

/// Error-free product without FMA for finite, non-underflowing binary64
/// products, including mixed-exponent operands that overflow the splitter.
pub(crate) fn two_product(lhs: f64, rhs: f64) -> (f64, f64) {
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

pub(crate) fn two_product_safe(lhs: f64, rhs: f64, head: f64) -> (f64, f64) {
    let (lhs_hi, lhs_lo) = split(lhs);
    let (rhs_hi, rhs_lo) = split(rhs);
    let error_1 = head - lhs_hi * rhs_hi;
    let error_2 = error_1 - lhs_lo * rhs_hi;
    let error_3 = error_2 - lhs_hi * rhs_lo;
    (head, lhs_lo * rhs_lo - error_3)
}

pub(crate) fn split(value: f64) -> (f64, f64) {
    let scaled = SPLITTER * value;
    let large = scaled - value;
    let high = scaled - large;
    (high, value - high)
}

pub(crate) fn dd_mul(lhs: (f64, f64), rhs: (f64, f64)) -> (f64, f64) {
    let (head, head_err) = two_product(lhs.0, rhs.0);
    two_sum(head, head_err + (lhs.0 * rhs.1 + lhs.1 * rhs.0))
}

pub(crate) fn dd_add(lhs: (f64, f64), rhs: (f64, f64)) -> (f64, f64) {
    let (head, head_err) = two_sum(lhs.0, rhs.0);
    two_sum(head, head_err + (lhs.1 + rhs.1))
}

pub(crate) fn dd_diff(end: f64, start: f64) -> (f64, f64) {
    two_sum(end, -start)
}

pub(crate) fn scale_shift_for_magnitude(largest: f64) -> i16 {
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

pub(crate) fn floor_binary_exponent(value: f64) -> i32 {
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

pub(crate) fn exact_power_of_two(shift: i16) -> f64 {
    let stored = i32::from(shift) + 1023;
    debug_assert!((1..=2046).contains(&stored));
    f64::from_bits((stored as u64) << 52)
}

/// The always-exact product residual, by fused multiply-add.
///
/// Where a product's residual underflows into the subnormal range, Dekker's
/// split rounds it to that grid while this stays exact, so the expansion
/// arithmetic — the exact-predicate fallback, where one subnormal ulp can be
/// the whole answer — uses this form. `mul_add` is a libm call on the
/// x86-64-v2 baseline, which is why the filtered predicates, whose error
/// bound already covers that rounding, take [`two_product`] instead.
pub(crate) fn two_product_exact(left: f64, right: f64) -> (f64, f64) {
    let product = left * right;
    let tail = if product.is_finite() {
        left.mul_add(right, -product)
    } else {
        0.0
    };
    (product, tail)
}

/// [`two_product`] that declines a non-finite head instead of returning it
/// with a zero tail: the rectangle solver bails on such a product.
pub(crate) fn checked_two_product(left: f64, right: f64) -> Option<(f64, f64)> {
    let (head, tail) = two_product(left, right);
    head.is_finite().then_some((head, tail))
}

pub(crate) fn dd_sub(left: (f64, f64), right: (f64, f64)) -> (f64, f64) {
    dd_add(left, (-right.0, -right.1))
}

/// [`dd_mul`] declining a non-finite head, for the rectangle solver.
pub(crate) fn checked_dd_mul(left: (f64, f64), right: (f64, f64)) -> Option<(f64, f64)> {
    checked_two_product(left.0, right.0)?;
    let product = dd_mul(left, right);
    product.0.is_finite().then_some(product)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The one contract the Dekker product carries against the fused form:
    /// on every finite product that does not underflow, the two residuals
    /// are bit-identical, since both are the exact rounding error. Where the
    /// product underflows into the subnormal range the split loses bits the
    /// fused instruction keeps; that edge is pinned here as the documented
    /// divergence rather than left to be discovered.
    #[test]
    fn dekker_product_matches_the_fused_residual_off_the_underflow_edge() {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut agreed = 0_usize;
        let mut underflow_cases = 0_usize;
        for case in 0..200_000 {
            // Magnitudes across the whole exponent range, so mixed-exponent
            // pairs that overflow the splitter are exercised as well.
            let exp = |bits: u64| -> f64 {
                let e = i32::try_from(bits % 2000).unwrap() - 1000;
                f64::from_bits((((e + 1023) as u64) << 52) | ((bits >> 12) & ((1 << 52) - 1)))
            };
            let (a, b) = (exp(next()), exp(next()));
            let (a, b) = if case % 2 == 0 { (a, -b) } else { (a, b) };
            let head = a * b;
            if !head.is_finite() {
                continue;
            }
            let (dekker_head, dekker_tail) = two_product(a, b);
            assert_eq!(dekker_head.to_bits(), head.to_bits(), "the head is the rounded product");
            let fused_tail = a.mul_add(b, -head);
            // The residual is at most half an ulp of the head, |head| * 2^-53,
            // so it can reach the subnormal range only when |head| < 2^-969.
            // That is the zone where the split may round it, in either form:
            // a subnormal tail, or one that rounds away to a signed zero.
            let residual_may_underflow =
                a != 0.0 && b != 0.0 && head.abs() < f64::MIN_POSITIVE * 2.0_f64.powi(53);
            if residual_may_underflow {
                assert!(
                    (dekker_tail - fused_tail).abs() <= f64::from_bits(1),
                    "case {case}: underflow-zone residuals differ by more than a subnormal ulp"
                );
                underflow_cases += 1;
                continue;
            }
            // Bit-identical except the sign of a zero: an exact product has a
            // zero tail, and the two evaluations may round to +0 or -0.
            let same = dekker_tail.to_bits() == fused_tail.to_bits()
                || (dekker_tail == 0.0 && fused_tail == 0.0);
            assert!(
                same,
                "case {case}: {a:e} * {b:e}: dekker {dekker_tail:e} vs fused {fused_tail:e}"
            );
            agreed += 1;
        }
        assert!(agreed > 100_000, "only {agreed} normal-range products were checked");
        assert!(underflow_cases > 1_000, "only {underflow_cases} products reached the underflow zone");
    }

    #[test]
    fn exact_product_is_the_fused_residual_everywhere() {
        for (a, b) in [(3.0, 4.0), (1.6e-279, -1.15e-35), (3.65e-278, -2.13e-30), (1e200, 1e-300), (f64::MAX, 0.5)] {
            let (head, tail) = two_product_exact(a, b);
            assert_eq!(head.to_bits(), (a * b).to_bits());
            assert_eq!(tail.to_bits(), a.mul_add(b, -head).to_bits());
        }
    }

    #[test]
    fn checked_product_declines_only_a_non_finite_head() {
        assert_eq!(checked_two_product(1e200, 1e200), None);
        assert_eq!(checked_two_product(3.0, 4.0), Some((12.0, 0.0)));
        assert_eq!(checked_two_product(f64::MAX, 0.5).map(|(h, _)| h), Some(f64::MAX * 0.5));
    }
}
