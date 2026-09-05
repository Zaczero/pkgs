//! Exact sums of products of finite binary64 coordinates.

use std::cmp::Ordering;

pub(crate) const LIMBS: usize = 68;
pub(crate) const PRODUCT_MIN_EXP: i32 = -2148;

/// Products share quantum 2^-2148 and have integer magnitude below 2^4196.
/// Fewer than 2^65 signed unit-coefficient terms fit below 2^4261, within
/// the 4352-bit accumulators. Sign is available without floating rounding.
#[derive(Clone)]
pub(crate) struct ExactSum {
    positive: [u64; LIMBS],
    negative: [u64; LIMBS],
}

impl ExactSum {
    pub(crate) const fn negate(&mut self) {
        std::mem::swap(&mut self.positive, &mut self.negative);
    }

    pub(crate) fn compare_positive_magnitude(&self, magnitude: &[u64; LIMBS]) -> Ordering {
        cmp_magnitude(magnitude, &self.positive)
    }

    pub(crate) const fn zero() -> Self {
        Self {
            positive: [0; LIMBS],
            negative: [0; LIMBS],
        }
    }

    pub(crate) fn add_product(&mut self, coefficient: i8, lhs: f64, rhs: f64) {
        debug_assert!(lhs.is_finite() && rhs.is_finite());
        let (lhs_negative, lhs_mantissa, lhs_exponent) = decompose(lhs);
        let (rhs_negative, rhs_mantissa, rhs_exponent) = decompose(rhs);
        if lhs_mantissa == 0 || rhs_mantissa == 0 {
            return;
        }
        let negative = (coefficient < 0) ^ lhs_negative ^ rhs_negative;
        let product = u128::from(lhs_mantissa) * u128::from(rhs_mantissa);
        let shift = usize::try_from(lhs_exponent + rhs_exponent - PRODUCT_MIN_EXP)
            .expect("finite f64 product fits the fixed exact accumulator");
        add_shifted(
            if negative {
                &mut self.negative
            } else {
                &mut self.positive
            },
            product,
            shift,
        );
    }

    pub(crate) fn ordering(&self) -> Ordering {
        cmp_magnitude(&self.positive, &self.negative)
    }

    pub(crate) fn signed_magnitude(&self) -> (Ordering, [u64; LIMBS]) {
        match self.ordering() {
            Ordering::Equal => (Ordering::Equal, [0; LIMBS]),
            Ordering::Greater => (
                Ordering::Greater,
                subtract_magnitude(&self.positive, &self.negative),
            ),
            Ordering::Less => (
                Ordering::Less,
                subtract_magnitude(&self.negative, &self.positive),
            ),
        }
    }
}

const fn decompose(value: f64) -> (bool, u64, i32) {
    let bits = value.to_bits();
    let negative = bits >> 63 != 0;
    let stored = ((bits >> 52) & 0x7FF) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    if stored == 0 {
        (negative, fraction, -1074)
    } else {
        (negative, (1_u64 << 52) | fraction, stored - 1075)
    }
}

fn add_shifted(target: &mut [u64; LIMBS], product: u128, shift: usize) {
    let word = shift / 64;
    let bits = shift % 64;
    let low = product as u64;
    let high = (product >> 64) as u64;
    add_word(target, word, low << bits);
    if bits == 0 {
        add_word(target, word + 1, high);
    } else {
        add_word(target, word + 1, low >> (64 - bits));
        add_word(target, word + 1, high << bits);
        add_word(target, word + 2, high >> (64 - bits));
    }
}

fn add_word(target: &mut [u64; LIMBS], mut index: usize, mut value: u64) {
    while value != 0 {
        let (sum, carry) = target[index].overflowing_add(value);
        target[index] = sum;
        value = u64::from(carry);
        index += 1;
        debug_assert!(index <= LIMBS);
    }
}

pub(crate) fn cmp_magnitude(left: &[u64; LIMBS], right: &[u64; LIMBS]) -> Ordering {
    for index in (0..LIMBS).rev() {
        match left[index].cmp(&right[index]) {
            Ordering::Equal => {},
            ordering => return ordering,
        }
    }
    Ordering::Equal
}

fn subtract_magnitude(larger: &[u64; LIMBS], smaller: &[u64; LIMBS]) -> [u64; LIMBS] {
    debug_assert_ne!(cmp_magnitude(larger, smaller), Ordering::Less);
    let mut result = [0; LIMBS];
    let mut borrow = false;
    for index in 0..LIMBS {
        let (first, first_borrow) = larger[index].overflowing_sub(smaller[index]);
        let (difference, second_borrow) = first.overflowing_sub(u64::from(borrow));
        result[index] = difference;
        borrow = first_borrow || second_borrow;
    }
    debug_assert!(!borrow);
    result
}
