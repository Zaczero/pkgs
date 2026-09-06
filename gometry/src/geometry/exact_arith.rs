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
//! arithmetic takes [`two_product_exact`] instead. Named tests below pin both
//! residual contracts.

pub(crate) mod product_sum;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum ExactSign {
    Negative,
    Zero,
    Positive,
}

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

    /// The Dekker product matches the fused residual when the residual is
    /// representable normally. The underflow row records the documented
    /// one-subnormal-ulp tolerance instead of pinning Dekker's spelling.
    #[test]
    fn dekker_product_matches_known_fused_residuals() {
        let cases = [
            (
                "safe nonzero residual",
                0x3FF1_9999_9999_999A,
                0x3FF4_CCCC_CCCC_CCCD,
                0x3FF6_E147_AE14_7AE2,
                0x3C54_7AE1_47AE_1480,
                false,
            ),
            (
                "rescaled nonzero residual",
                0x7FEF_FFFF_FFFF_FFFF,
                0x3FB9_9999_9999_999A,
                0x7FB9_9999_9999_9999,
                0x7C49_9999_9999_9998,
                false,
            ),
            (
                "rescaled exact power",
                0x7E70_0000_0000_0000,
                0x0170_0000_0000_0000,
                0x3FF0_0000_0000_0000,
                0x0000_0000_0000_0000,
                false,
            ),
            (
                "underflow residual",
                0x2082_6B6C_1DD3_EF04,
                0x9FC0_6EE1_DBDA_1774,
                0x8052_EB12_A8DA_CDFD,
                0x0000_0000_0000_0002,
                true,
            ),
        ];
        for &(name, lhs_bits, rhs_bits, expected_head, expected_fused_tail, underflow) in &cases {
            let (lhs, rhs) = (f64::from_bits(lhs_bits), f64::from_bits(rhs_bits));
            let (head, tail) = two_product(lhs, rhs);
            assert_eq!(head.to_bits(), expected_head, "{name}: head");
            let fused_tail = lhs.mul_add(rhs, -head);
            assert_eq!(
                fused_tail.to_bits(),
                expected_fused_tail,
                "{name}: fused tail"
            );
            if underflow {
                assert!(
                    (tail - fused_tail).abs() <= f64::from_bits(1),
                    "{name}: Dekker residual exceeds one subnormal ulp"
                );
            } else {
                assert_eq!(tail.to_bits(), expected_fused_tail, "{name}: Dekker tail");
            }
        }
    }

    #[test]
    fn exact_product_matches_known_fused_residuals() {
        let cases = [
            (
                "ordinary",
                0x3FF1_9999_9999_999A,
                0x3FF4_CCCC_CCCC_CCCD,
                0x3FF6_E147_AE14_7AE2,
                0x3C54_7AE1_47AE_1480,
            ),
            (
                "mixed exponent",
                0x6974_E718_D7D7_625A,
                0x01A5_6E1F_C2F8_F359,
                0x2B2B_FF2E_E48E_0530,
                0xA7B9_6B30_449B_05B0,
            ),
            (
                "underflow residual",
                0x2082_6B6C_1DD3_EF04,
                0x9FC0_6EE1_DBDA_1774,
                0x8052_EB12_A8DA_CDFD,
                0x0000_0000_0000_0002,
            ),
            (
                "maximum finite half",
                0x7FEF_FFFF_FFFF_FFFF,
                0x3FE0_0000_0000_0000,
                0x7FDF_FFFF_FFFF_FFFF,
                0x0000_0000_0000_0000,
            ),
        ];
        for &(name, lhs, rhs, expected_head, expected_tail) in &cases {
            let (lhs, rhs) = (f64::from_bits(lhs), f64::from_bits(rhs));
            let (head, tail) = two_product_exact(lhs, rhs);
            assert_eq!(head.to_bits(), expected_head, "{name}: head");
            if expected_tail == 0 {
                assert_eq!(tail, 0.0, "{name}: tail");
            } else {
                assert_eq!(tail.to_bits(), expected_tail, "{name}: tail");
            }
        }
    }

    #[test]
    fn checked_product_declines_only_a_non_finite_head() {
        assert_eq!(checked_two_product(1e200, 1e200), None);
        assert_eq!(checked_two_product(3.0, 4.0), Some((12.0, 0.0)));
        assert_eq!(
            checked_two_product(f64::MAX, 0.5).map(|(h, _)| h),
            Some(f64::from_bits(0x7FDF_FFFF_FFFF_FFFF))
        );
    }
}
