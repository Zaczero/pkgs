//! Full-binary64 orientation sign.
//!
//! The fast filter first applies the standard determinant error bound, then
//! rounds an outward interval for ambiguous lanes. Unresolved cases fall back
//! to integer arithmetic on exact stored dyadics (all binary64 values share
//! `2^-1074`).

use super::CCW_ERRBOUND_A;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::Orientation;
use crate::geometry::tessellation::exact::Interval;

const LIMBS: usize = 68;

/// Fixed-width signed integer wide enough for any product of two differences
/// of finite binary64 values (2 × (2^1024 / 2^-1074) needs 4197 bits; 68 limbs
/// give 4352). `low..high` is the window of limbs that may be nonzero, so the
/// arithmetic below costs the live width of the operands rather than the
/// full 68 limbs: ordinary coordinates occupy two limbs and their products
/// four, and only exponent spreads across the whole binary64 range reach the
/// full width.
#[derive(Clone, Copy)]
struct ExactInteger {
    negative: bool,
    low: usize,
    high: usize,
    limbs: [u64; LIMBS],
}

impl ExactInteger {
    const fn zero() -> Self {
        Self {
            negative: false,
            low: 0,
            high: 0,
            limbs: [0; LIMBS],
        }
    }
    fn from_f64(value: f64) -> Self {
        debug_assert!(value.is_finite());
        if value == 0.0 {
            return Self::zero();
        }
        let bits = value.to_bits();
        let fraction = bits & ((1_u64 << 52) - 1);
        let encoded_exponent = ((bits >> 52) & 0x7FF) as usize;
        let (significand, shift) = if encoded_exponent == 0 {
            (fraction, 0)
        } else {
            ((1_u64 << 52) | fraction, encoded_exponent - 1)
        };
        let mut result = Self::zero();
        result.negative = bits >> 63 != 0;
        let (word, bit) = (shift / 64, shift % 64);
        result.limbs[word] = significand << bit;
        result.low = word;
        result.high = word + 1;
        if bit != 0 {
            result.limbs[word + 1] = significand >> (64 - bit);
            result.high = word + 2;
        }
        result
    }
    fn is_zero(&self) -> bool {
        self.limbs[self.low..self.high].iter().all(|&limb| limb == 0)
    }
    fn cmp_magnitude(&self, other: &Self) -> std::cmp::Ordering {
        let (low, high) = (self.low.min(other.low), self.high.max(other.high));
        self.limbs[low..high]
            .iter()
            .rev()
            .cmp(other.limbs[low..high].iter().rev())
    }
    fn subtract(self, other: Self) -> Self {
        if self.negative != other.negative {
            return Self::add_magnitudes(&self, &other, self.negative);
        }
        match self.cmp_magnitude(&other) {
            std::cmp::Ordering::Equal => Self::zero(),
            std::cmp::Ordering::Greater => Self::subtract_magnitudes(&self, &other, self.negative),
            std::cmp::Ordering::Less => Self::subtract_magnitudes(&other, &self, !self.negative),
        }
    }
    fn add_magnitudes(left: &Self, right: &Self, negative: bool) -> Self {
        let mut result = Self::zero();
        let (low, high) = (left.low.min(right.low), left.high.max(right.high));
        let mut carry = 0_u128;
        for index in low..high {
            let sum = u128::from(left.limbs[index]) + u128::from(right.limbs[index]) + carry;
            result.limbs[index] = sum as u64;
            carry = sum >> 64;
        }
        result.low = low;
        result.high = high;
        if carry != 0 {
            result.limbs[high] = carry as u64;
            result.high = high + 1;
        }
        result.negative = negative;
        result
    }
    fn subtract_magnitudes(larger: &Self, smaller: &Self, negative: bool) -> Self {
        let mut result = Self::zero();
        let (low, high) = (larger.low.min(smaller.low), larger.high.max(smaller.high));
        let mut borrow = 0_u128;
        for index in low..high {
            let subtrahend = u128::from(smaller.limbs[index]) + borrow;
            let minuend = u128::from(larger.limbs[index]);
            result.limbs[index] = minuend.wrapping_sub(subtrahend) as u64;
            borrow = u128::from(minuend < subtrahend);
        }
        debug_assert_eq!(borrow, 0);
        result.low = low;
        result.high = high;
        result.negative = negative;
        result
    }
    fn product(&self, other: &Self) -> Self {
        let mut result = Self::zero();
        if self.is_zero() || other.is_zero() {
            return result;
        }
        let low = self.low + other.low;
        let high = (self.high + other.high).min(LIMBS);
        for (left_index, &left) in self.limbs.iter().enumerate().take(self.high).skip(self.low) {
            if left == 0 {
                continue;
            }
            let mut carry = 0_u128;
            let right_end = other.high.min(LIMBS - left_index);
            for (right_index, &right) in other.limbs[other.low..right_end].iter().enumerate() {
                let index = left_index + other.low + right_index;
                let value = u128::from(result.limbs[index]) + u128::from(left) * u128::from(right) + carry;
                result.limbs[index] = value as u64;
                carry = value >> 64;
            }
            if carry != 0 {
                let index = left_index + other.high;
                debug_assert!(index < LIMBS, "a product of two binary64 differences fits");
                result.limbs[index] = carry as u64;
            }
        }
        result.low = low;
        result.high = high;
        result.negative = self.negative != other.negative && !result.is_zero();
        result
    }
    fn sign(self) -> ExactSign {
        if self.is_zero() {
            ExactSign::Zero
        } else if self.negative {
            ExactSign::Negative
        } else {
            ExactSign::Positive
        }
    }
}

/// Exact sign of the stored-binary64 orientation after the caller's fast path
/// has declined. The outward interval proves ordinary cases; fixed-width
/// integer arithmetic resolves its remaining full-range cases.
pub(in crate::geometry) fn stored_orientation_sign(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    cx: f64,
    cy: f64,
) -> ExactSign {
    let exact = Interval::exact;
    let (bax, bay) = (exact(bx).sub(exact(ax)), exact(by).sub(exact(ay)));
    let (cax, cay) = (exact(cx).sub(exact(ax)), exact(cy).sub(exact(ay)));
    if bax.is_finite()
        && bay.is_finite()
        && cax.is_finite()
        && cay.is_finite()
        && let Some(sign) = bax.mul(cay).sub(bay.mul(cax)).sign()
    {
        return sign;
    }
    let (bax, bay) = (
        ExactInteger::from_f64(bx).subtract(ExactInteger::from_f64(ax)),
        ExactInteger::from_f64(by).subtract(ExactInteger::from_f64(ay)),
    );
    let (cax, cay) = (
        ExactInteger::from_f64(cx).subtract(ExactInteger::from_f64(ax)),
        ExactInteger::from_f64(cy).subtract(ExactInteger::from_f64(ay)),
    );
    bax.product(&cay).subtract(bay.product(&cax)).sign()
}

#[expect(clippy::float_cmp, reason = "an exact predicate's degeneracy test: these operands are compared for identity, not proximity, and a tolerance would make the predicate inexact")]
pub(super) fn orientation_xy(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64) -> Orientation {
    // Certify ordinary finite coordinates with the first Shewchuk filter before
    // constructing outward intervals. Overflow, underflow, and near-collinear
    // cases continue through the complete binary64 path below.
    let (bax, bay) = (bx - ax, by - ay);
    let (cax, cay) = (cx - ax, cy - ay);
    let left = bax * cay;
    let right = bay * cax;
    let det = left - right;
    let magnitude = left.abs() + right.abs();
    if det.is_finite()
        && magnitude.is_finite()
        && det.abs() > (magnitude + f64::MIN_POSITIVE) * CCW_ERRBOUND_A
    {
        return if det > 0.0 {
            Orientation::CounterClockwise
        } else {
            Orientation::Clockwise
        };
    }
    if (ax == bx && ax == cx) || (ay == by && ay == cy) {
        return Orientation::Collinear;
    }
    match stored_orientation_sign(ax, ay, bx, by, cx, cy) {
        ExactSign::Negative => Orientation::Clockwise,
        ExactSign::Zero => Orientation::Collinear,
        ExactSign::Positive => Orientation::CounterClockwise,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mixed_scale_diagonal_reaches_exact_fallback() {
        let mu = f64::from_bits(1);
        let length = 2.0_f64.powi(-20);
        let exact = Interval::exact;
        let bax = exact(length).sub(exact(0.0));
        let bay = exact(length).sub(exact(0.0));
        let cax = exact(mu).sub(exact(0.0));
        let cay = exact(2.0 * mu).sub(exact(0.0));
        assert_eq!(bax.mul(cay).sub(bay.mul(cax)).sign(), None);
        assert_eq!(
            orientation_xy(0.0, 0.0, length, length, mu, 2.0 * mu),
            Orientation::CounterClockwise
        );
    }

    #[test]
    fn overflowing_differences_reach_exact_fallback() {
        let max = f64::MAX;
        assert_eq!(
            orientation_xy(-max, -max, max, max, max, -max),
            Orientation::Clockwise
        );
    }

    #[test]
    fn axis_aligned_ties_are_collinear() {
        let subnormal = f64::from_bits(1);
        let max = f64::MAX;
        for (a, b, c) in [
            ((-0.0, -0.0), (1.0, -0.0), (-1.0, 0.0)),
            ((-0.0, -1.0), (0.0, 0.0), (0.0, 1.0)),
            (
                (subnormal, subnormal),
                (2.0 * subnormal, subnormal),
                (3.0 * subnormal, subnormal),
            ),
            ((max, -max), (max, 0.0), (max, max)),
        ] {
            assert_eq!(
                orientation_xy(a.0, a.1, b.0, b.1, c.0, c.1),
                Orientation::Collinear
            );
        }
    }

    #[test]
    fn one_ulp_axis_near_tie_remains_noncollinear() {
        let ay = 1.0_f64;
        let by = f64::from_bits(ay.to_bits() + 1);
        assert_eq!(
            orientation_xy(0.0, ay, 1.0, by, 2.0, ay),
            Orientation::Clockwise
        );
    }

    #[test]
    fn stored_orientation_preserves_all_underflow_ray_permutations() {
        let mu = f64::from_bits(1);
        let a = (1.0, -4096.0 * mu);
        let b = (-2.0_f64.powi(-12), 6144.0 * mu);
        let p = (f64::from_bits(0x3FD9_9733_3333_3333), 2048.0 * mu);
        for ((left, middle, right), expected) in [
            ((a, b, p), ExactSign::Positive),
            ((a, p, b), ExactSign::Negative),
            ((b, a, p), ExactSign::Negative),
            ((b, p, a), ExactSign::Positive),
            ((p, a, b), ExactSign::Positive),
            ((p, b, a), ExactSign::Negative),
        ] {
            assert_eq!(
                stored_orientation_sign(left.0, left.1, middle.0, middle.1, right.0, right.1),
                expected
            );
        }
    }
}
