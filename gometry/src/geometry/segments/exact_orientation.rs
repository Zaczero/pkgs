//! Full-binary64 orientation sign.
//!
//! The fast filter rounds every operation outward and decides only when the
//! determinant interval excludes zero. Ambiguous lanes fall back to integer
//! arithmetic on exact stored dyadics (all binary64 values share `2^-1074`).

use crate::geometry::Orientation;
use crate::geometry::tessellation::exact::{ExactSign, Interval};

const LIMBS: usize = 68;

#[derive(Clone, Copy)]
struct ExactInteger {
    negative: bool,
    limbs: [u64; LIMBS],
}

impl ExactInteger {
    const fn zero() -> Self {
        Self {
            negative: false,
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
        if bit != 0 {
            result.limbs[word + 1] = significand >> (64 - bit);
        }
        result
    }
    fn is_zero(&self) -> bool {
        self.limbs.iter().all(|&limb| limb == 0)
    }
    fn cmp_magnitude(&self, other: &Self) -> std::cmp::Ordering {
        self.limbs.iter().rev().cmp(other.limbs.iter().rev())
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
        let mut carry = 0_u128;
        for index in 0..LIMBS {
            let sum = u128::from(left.limbs[index]) + u128::from(right.limbs[index]) + carry;
            result.limbs[index] = sum as u64;
            carry = sum >> 64;
        }
        debug_assert_eq!(carry, 0);
        result.negative = negative;
        result
    }
    fn subtract_magnitudes(larger: &Self, smaller: &Self, negative: bool) -> Self {
        let mut result = Self::zero();
        let mut borrow = 0_u128;
        for index in 0..LIMBS {
            let subtrahend = u128::from(smaller.limbs[index]) + borrow;
            let minuend = u128::from(larger.limbs[index]);
            result.limbs[index] = minuend.wrapping_sub(subtrahend) as u64;
            borrow = u128::from(minuend < subtrahend);
        }
        debug_assert_eq!(borrow, 0);
        result.negative = negative;
        result
    }
    fn product(&self, other: &Self) -> Self {
        let mut result = Self::zero();
        for (left_index, &left) in self.limbs.iter().enumerate() {
            if left == 0 {
                continue;
            }
            let mut carry = 0_u128;
            for (right_index, &right) in other.limbs[..LIMBS - left_index].iter().enumerate() {
                let index = left_index + right_index;
                let value =
                    u128::from(result.limbs[index]) + u128::from(left) * u128::from(right) + carry;
                result.limbs[index] = value as u64;
                carry = value >> 64;
            }
            debug_assert_eq!(carry, 0);
        }
        result.negative = self.negative != other.negative && !result.is_zero();
        result
    }
    fn orientation(self) -> Orientation {
        if self.is_zero() {
            Orientation::Collinear
        } else if self.negative {
            Orientation::Clockwise
        } else {
            Orientation::CounterClockwise
        }
    }
}

pub(super) fn orientation_xy(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64) -> Orientation {
    let exact = Interval::exact;
    let (bax, bay) = (exact(bx).sub(exact(ax)), exact(by).sub(exact(ay)));
    let (cax, cay) = (exact(cx).sub(exact(ax)), exact(cy).sub(exact(ay)));
    if bax.is_finite()
        && bay.is_finite()
        && cax.is_finite()
        && cay.is_finite()
        && let Some(sign) = bax.mul(cay).sub(bay.mul(cax)).sign()
    {
        return match sign {
            ExactSign::Negative => Orientation::Clockwise,
            ExactSign::Positive => Orientation::CounterClockwise,
            ExactSign::Zero => unreachable!("an interval sign excludes zero"),
        };
    }
    let (bax, bay) = (
        ExactInteger::from_f64(bx).subtract(ExactInteger::from_f64(ax)),
        ExactInteger::from_f64(by).subtract(ExactInteger::from_f64(ay)),
    );
    let (cax, cay) = (
        ExactInteger::from_f64(cx).subtract(ExactInteger::from_f64(ax)),
        ExactInteger::from_f64(cy).subtract(ExactInteger::from_f64(ay)),
    );
    bax.product(&cay).subtract(bay.product(&cax)).orientation()
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
}
