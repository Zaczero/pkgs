//! Error-free transformations and double-double arithmetic: the one home for
//! `two_sum`, `two_product`, and the `dd_*` kernels every exact predicate and
//! expansion builds on, and for [`bounded`] certificates.
//!
//! Products take their residual from a fused multiply-add. At the x86-64-v2
//! baseline `mul_add` is a libm call, but glibc dispatches it to the hardware
//! FMA instruction where the CPU has one, and call plus instruction retire
//! fewer instructions than a Dekker split with the rescaling its splitter
//! needs at large magnitudes. [`two_product_exact`] declines a product whose
//! residual might not be representable; [`two_product`] serves filtered
//! predicates, whose error bound covers a rounded subnormal residual.

pub(crate) mod bounded;
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

pub(crate) const MAX_CROSS_COORD_EXPONENT: i32 = 480;

pub(crate) const MIN_CROSS_COORD_EXPONENT: i32 = -400;

/// Lowest rounded-product exponent whose 106-bit exact product still fits the
/// binary64 subnormal grid in its residual.
const MIN_EXACT_PRODUCT_EXPONENT: i32 = -969;

/// Error-free product for finite, non-underflowing binary64 products: the
/// fused residual. Below the non-underflowing range the residual is rounded
/// to the subnormal grid, within half a subnormal ulp.
pub(crate) fn two_product(lhs: f64, rhs: f64) -> (f64, f64) {
    let head = lhs * rhs;
    if !head.is_finite() || head == 0.0 {
        return (head, 0.0);
    }
    (head, lhs.mul_add(rhs, -head))
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

/// A product decomposition whose residual is exactly representable as binary64.
///
/// A binary64 product has at most 106 significant bits.  If its rounded head
/// has exponent `e >= -969`, its exact low bit is no smaller than `2^-1074`,
/// so the fused residual can be carried by the returned tail.  A lower normal
/// head, a subnormal head, or a nonzero product that rounds to zero may have a
/// residual below the binary64 grid and is declined rather than silently
/// returning a false expansion. Exact zero products are admitted separately.
pub(crate) fn two_product_exact(left: f64, right: f64) -> Option<(f64, f64)> {
    let product = left * right;
    if !product.is_finite() {
        return None;
    }
    if product == 0.0 {
        return (left == 0.0 || right == 0.0).then_some((product, 0.0));
    }
    if !product.is_normal() || floor_binary_exponent(product.abs()) < MIN_EXACT_PRODUCT_EXPONENT {
        return None;
    }
    let tail = left.mul_add(right, -product);
    tail.is_finite().then_some((product, tail))
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

    /// Known residuals, including magnitudes a splitter would overflow and a
    /// residual rounded to the subnormal grid.
    #[test]
    fn product_matches_known_fused_residuals() {
        let cases = [
            (
                "safe nonzero residual",
                0x3FF1_9999_9999_999A,
                0x3FF4_CCCC_CCCC_CCCD,
                0x3FF6_E147_AE14_7AE2,
                0x3C54_7AE1_47AE_1480,
            ),
            (
                "rescaled nonzero residual",
                0x7FEF_FFFF_FFFF_FFFF,
                0x3FB9_9999_9999_999A,
                0x7FB9_9999_9999_9999,
                0x7C49_9999_9999_9998,
            ),
            (
                "rescaled exact power",
                0x7E70_0000_0000_0000,
                0x0170_0000_0000_0000,
                0x3FF0_0000_0000_0000,
                0x0000_0000_0000_0000,
            ),
            (
                "underflow residual",
                0x2082_6B6C_1DD3_EF04,
                0x9FC0_6EE1_DBDA_1774,
                0x8052_EB12_A8DA_CDFD,
                0x0000_0000_0000_0002,
            ),
        ];
        for &(name, lhs_bits, rhs_bits, expected_head, expected_tail) in &cases {
            let (lhs, rhs) = (f64::from_bits(lhs_bits), f64::from_bits(rhs_bits));
            let (head, tail) = two_product(lhs, rhs);
            assert_eq!(head.to_bits(), expected_head, "{name}: head");
            assert_eq!(tail.to_bits(), expected_tail, "{name}: tail");
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
                "maximum finite half",
                0x7FEF_FFFF_FFFF_FFFF,
                0x3FE0_0000_0000_0000,
                0x7FDF_FFFF_FFFF_FFFF,
                0x0000_0000_0000_0000,
            ),
        ];
        for &(name, lhs, rhs, expected_head, expected_tail) in &cases {
            let (lhs, rhs) = (f64::from_bits(lhs), f64::from_bits(rhs));
            let Some((head, tail)) = two_product_exact(lhs, rhs) else {
                panic!("{name}: product residual was outside the exact fast domain")
            };
            assert_eq!(head.to_bits(), expected_head, "{name}: head");
            if expected_tail == 0 {
                assert_eq!(tail, 0.0, "{name}: tail");
            } else {
                assert_eq!(tail.to_bits(), expected_tail, "{name}: tail");
            }
        }
    }

    #[test]
    fn exact_product_declines_an_unrepresentable_normal_head_residual() {
        // x² = 2^-1022 + 2^-1073 + 2^-1126. The rounded head is normal,
        // but the final term is below the binary64 subnormal grid; an FMA tail
        // cannot carry the exact residual as a single f64.
        let x = f64::from_bits(0x2000_0000_0000_0001);
        let head = x * x;
        assert!(head.is_normal());
        assert_eq!(head.to_bits(), 0x0010_0000_0000_0002);
        assert_eq!(x.mul_add(x, -head), 0.0);
        assert_eq!(two_product_exact(x, x), None);
    }

    #[test]
    fn exact_product_admits_zero_and_representable_boundaries() {
        assert_eq!(two_product_exact(0.0, f64::MAX), Some((0.0, 0.0)));
        assert_eq!(two_product_exact(-0.0, 3.0), Some((-0.0, 0.0)));

        let near_threshold = f64::from_bits(0x0360_0000_0000_0001);
        assert!(near_threshold.is_normal());
        let (_, threshold_tail) = two_product_exact(near_threshold, 1.0 + f64::EPSILON)
            .expect("the threshold residual is representable");
        assert_eq!(threshold_tail.to_bits(), 0x2);
        let (negative_head, _) = two_product_exact(-near_threshold, 1.0 + f64::EPSILON)
            .expect("negative normal product is in the exact fast domain");
        assert!(negative_head.is_sign_negative());

        // The exact product is representable, but a subnormal head is outside
        // the fast contract because its residual may require a finer grid.
        assert_eq!(two_product_exact(f64::MIN_POSITIVE, 0.5), None);
        assert_eq!(
            two_product_exact(f64::MIN_POSITIVE, f64::MIN_POSITIVE),
            None
        );
        assert!(two_product_exact(f64::MAX, 0.5).is_some());
        assert_eq!(two_product_exact(f64::MAX, 2.0), None);
        assert_eq!(two_product_exact(f64::INFINITY, 1.0), None);
        assert_eq!(two_product_exact(f64::NAN, 1.0), None);
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
