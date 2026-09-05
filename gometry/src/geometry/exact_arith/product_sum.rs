//! Exact sums of products of finite binary64 coordinates.

use std::cmp::Ordering;

pub(crate) const LIMBS: usize = 68;
pub(crate) const PRODUCT_MIN_EXP: i32 = -2148;
const CUBIC_LIMBS: usize = 101;
const CUBIC_MIN_EXP: i32 = -3222;

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
        add_shifted_words(
            if negative {
                &mut self.negative
            } else {
                &mut self.positive
            },
            &[product as u64, (product >> 64) as u64],
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

    fn merge_signed(&mut self, source: &Self, negate: bool) {
        if negate {
            add_words(&mut self.positive, &source.negative);
            add_words(&mut self.negative, &source.positive);
        } else {
            add_words(&mut self.positive, &source.positive);
            add_words(&mut self.negative, &source.negative);
        }
    }
}

/// Exact signed centroid sums over stored binary64 coordinates.  Area is a
/// degree-two dyadic sum; each moment is degree three and is rounded only by
/// [`Self::finish`].
#[derive(Clone)]
pub(crate) struct ExactCentroidSums {
    area: ExactSum,
    x: ExactCubicSum,
    y: ExactCubicSum,
}

impl ExactCentroidSums {
    pub(crate) const fn zero() -> Self {
        Self {
            area: ExactSum::zero(),
            x: ExactCubicSum::zero(),
            y: ExactCubicSum::zero(),
        }
    }

    pub(crate) fn add_edge(&mut self, x0: f64, y0: f64, x1: f64, y1: f64) {
        self.area.add_product(1, x0, y1);
        self.area.add_product(-1, x1, y0);
        // (x0*y1 - x1*y0) * (x0 + x1)
        self.x.add_product(1, x0, x0, y1);
        self.x.add_product(1, x0, x1, y1);
        self.x.add_product(-1, x0, x1, y0);
        self.x.add_product(-1, x1, x1, y0);
        // (x0*y1 - x1*y0) * (y0 + y1)
        self.y.add_product(1, x0, y0, y1);
        self.y.add_product(1, x0, y1, y1);
        self.y.add_product(-1, x1, y0, y0);
        self.y.add_product(-1, x1, y0, y1);
    }

    /// Shells contribute positive area and holes negative area, irrespective
    /// of their input winding.  The raw exact area is the orientation owner.
    pub(crate) fn merge_ring(&mut self, ring: &Self, hole: bool) {
        let ordering = ring.area.ordering();
        if ordering == Ordering::Equal {
            return;
        }
        let negate = (ordering == Ordering::Less) ^ hole;
        self.area.merge_signed(&ring.area, negate);
        self.x.merge_signed(&ring.x, negate);
        self.y.merge_signed(&ring.y, negate);
    }

    pub(crate) fn finish(&self) -> Option<(f64, f64)> {
        let (area_sign, area) = self.area.signed_magnitude();
        if area_sign == Ordering::Equal {
            return None;
        }
        Some((
            exact_centroid_quotient(&self.x, &area, area_sign)?,
            exact_centroid_quotient(&self.y, &area, area_sign)?,
        ))
    }
}

/// Triple products share quantum 2^-3222. Four terms per moment edge over
/// fewer than 2^128 possible traversed edges occupy fewer than 6424 bits.
/// The doubled moment and adjacent-candidate boundary each add at most one bit,
/// so their comparison still fits within the 6464-bit carrier.
#[derive(Clone)]
struct ExactCubicSum {
    positive: [u64; CUBIC_LIMBS],
    negative: [u64; CUBIC_LIMBS],
}

impl ExactCubicSum {
    const fn zero() -> Self {
        Self {
            positive: [0; CUBIC_LIMBS],
            negative: [0; CUBIC_LIMBS],
        }
    }

    fn add_product(&mut self, coefficient: i8, a: f64, b: f64, c: f64) {
        debug_assert!(a.is_finite() && b.is_finite() && c.is_finite());
        let (an, am, ae) = decompose(a);
        let (bn, bm, be) = decompose(b);
        let (cn, cm, ce) = decompose(c);
        if am == 0 || bm == 0 || cm == 0 {
            return;
        }
        let shift =
            usize::try_from(ae + be + ce - CUBIC_MIN_EXP).expect("finite cubic product fits");
        let product = multiply_three(am, bm, cm);
        let negative = (coefficient < 0) ^ an ^ bn ^ cn;
        add_shifted_words(
            if negative {
                &mut self.negative
            } else {
                &mut self.positive
            },
            &product,
            shift,
        );
    }

    fn ordering(&self) -> Ordering {
        cmp_magnitude(&self.positive, &self.negative)
    }

    fn signed_magnitude(&self) -> (Ordering, [u64; CUBIC_LIMBS]) {
        match self.ordering() {
            Ordering::Equal => (Ordering::Equal, [0; CUBIC_LIMBS]),
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

    fn merge_signed(&mut self, source: &Self, negate: bool) {
        if negate {
            add_words(&mut self.positive, &source.negative);
            add_words(&mut self.negative, &source.positive);
        } else {
            add_words(&mut self.positive, &source.positive);
            add_words(&mut self.negative, &source.negative);
        }
    }
}

fn multiply_three(a: u64, b: u64, c: u64) -> [u64; 3] {
    let first = u128::from(a) * u128::from(b);
    let low = u128::from(first as u64) * u128::from(c);
    let high = u128::from((first >> 64) as u64) * u128::from(c);
    let (middle, carry) = ((low >> 64) as u64).overflowing_add(high as u64);
    [low as u64, middle, (high >> 64) as u64 + u64::from(carry)]
}

fn exact_centroid_quotient(
    moment: &ExactCubicSum,
    area: &[u64; LIMBS],
    area_sign: Ordering,
) -> Option<f64> {
    let (moment_sign, magnitude) = moment.signed_magnitude();
    if moment_sign == Ordering::Equal {
        return Some(0.0);
    }
    let negative = (moment_sign == Ordering::Less) ^ (area_sign == Ordering::Less);
    let max_bits = f64::MAX.to_bits();
    let max_product = area_candidate_product(area, f64::MAX);
    let bits = match cmp_magnitude(&magnitude, &max_product) {
        Ordering::Less | Ordering::Equal => {
            let mut low = 0_u64;
            let mut high = max_bits;
            while high - low > 1 {
                let middle = low + (high - low) / 2;
                if cmp_magnitude(
                    &magnitude,
                    &area_candidate_product(area, f64::from_bits(middle)),
                ) == Ordering::Less
                {
                    high = middle;
                } else {
                    low = middle;
                }
            }
            nearest_candidate(&magnitude, area, low, high)
        },
        Ordering::Greater => {
            let virtual_product = area_scaled_product(area, 1, 1024);
            let mut twice = [0_u64; CUBIC_LIMBS];
            add_words(&mut twice, &magnitude);
            add_words(&mut twice, &magnitude);
            let mut boundary = max_product;
            add_words(&mut boundary, &virtual_product);
            if !cmp_magnitude(&twice, &boundary).is_lt() {
                return None;
            }
            max_bits
        },
    };
    let value = f64::from_bits(bits);
    Some(if value == 0.0 {
        0.0
    } else if negative {
        -value
    } else {
        value
    })
}

fn nearest_candidate(
    magnitude: &[u64; CUBIC_LIMBS],
    area: &[u64; LIMBS],
    low: u64,
    high: u64,
) -> u64 {
    let mut twice = [0_u64; CUBIC_LIMBS];
    add_words(&mut twice, magnitude);
    add_words(&mut twice, magnitude);
    let mut boundary = area_candidate_product(area, f64::from_bits(low));
    let upper = area_candidate_product(area, f64::from_bits(high));
    add_words(&mut boundary, &upper);
    match cmp_magnitude(&twice, &boundary) {
        Ordering::Less => low,
        Ordering::Greater => high,
        Ordering::Equal => {
            if low & 1 == 0 {
                low
            } else {
                high
            }
        },
    }
}

fn area_candidate_product(area: &[u64; LIMBS], candidate: f64) -> [u64; CUBIC_LIMBS] {
    let (_, mantissa, exponent) = decompose(candidate);
    area_scaled_product(area, mantissa, exponent)
}

fn area_scaled_product(area: &[u64; LIMBS], mantissa: u64, exponent: i32) -> [u64; CUBIC_LIMBS] {
    let mut result = [0_u64; CUBIC_LIMBS];
    if mantissa == 0 {
        return result;
    }
    let shift =
        usize::try_from(exponent + 1074).expect("candidate aligns to cubic quantum");
    let factor = mantissa * 3;
    for (index, &word) in area.iter().enumerate() {
        let product = u128::from(word) * u128::from(factor);
        add_shifted_words(
            &mut result,
            &[product as u64, (product >> 64) as u64],
            shift + index * 64,
        );
    }
    result
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

fn add_shifted_words<const N: usize>(target: &mut [u64; N], words: &[u64], shift: usize) {
    let word = shift / 64;
    let bits = shift % 64;
    for (offset, &value) in words.iter().enumerate() {
        add_word(target, word + offset, value << bits);
        if bits != 0 {
            add_word(target, word + offset + 1, value >> (64 - bits));
        }
    }
}

fn add_word<const N: usize>(target: &mut [u64; N], mut index: usize, mut value: u64) {
    while value != 0 {
        let (sum, carry) = target[index].overflowing_add(value);
        target[index] = sum;
        value = u64::from(carry);
        index += 1;
        debug_assert!(index <= N);
    }
}

fn add_words<const N: usize>(target: &mut [u64; N], source: &[u64; N]) {
    for (index, &word) in source.iter().enumerate() {
        add_word(target, index, word);
    }
}

pub(crate) fn cmp_magnitude<const N: usize>(left: &[u64; N], right: &[u64; N]) -> Ordering {
    for index in (0..N).rev() {
        match left[index].cmp(&right[index]) {
            Ordering::Equal => {},
            ordering => return ordering,
        }
    }
    Ordering::Equal
}

fn subtract_magnitude<const N: usize>(larger: &[u64; N], smaller: &[u64; N]) -> [u64; N] {
    debug_assert_ne!(cmp_magnitude(larger, smaller), Ordering::Less);
    let mut result = [0; N];
    let mut borrow = false;
    for index in 0..N {
        let (first, first_borrow) = larger[index].overflowing_sub(smaller[index]);
        let (difference, second_borrow) = first.overflowing_sub(u64::from(borrow));
        result[index] = difference;
        borrow = first_borrow || second_borrow;
    }
    debug_assert!(!borrow);
    result
}


#[cfg(test)]
mod tests {
    use super::*;

    fn moment(magnitude: [u64; CUBIC_LIMBS]) -> ExactCubicSum {
        ExactCubicSum {
            positive: magnitude,
            negative: [0; CUBIC_LIMBS],
        }
    }

    fn negative_moment(magnitude: [u64; CUBIC_LIMBS]) -> ExactCubicSum {
        ExactCubicSum { positive: [0; CUBIC_LIMBS], negative: magnitude }
    }

    fn decrement(words: &mut [u64; CUBIC_LIMBS]) {
        for word in words {
            if *word != 0 {
                *word -= 1;
                return;
            }
            *word = u64::MAX;
        }
    }

    fn increment(words: &mut [u64; CUBIC_LIMBS]) {
        for word in words {
            let (next, carry) = word.overflowing_add(1);
            *word = next;
            if !carry {
                return;
            }
        }
    }

    fn power(bit: usize) -> [u64; CUBIC_LIMBS] {
        let mut words = [0_u64; CUBIC_LIMBS];
        words[bit / 64] = 1_u64 << (bit % 64);
        words
    }

    fn subtract(words: &mut [u64; CUBIC_LIMBS], rhs: &[u64; CUBIC_LIMBS]) {
        let mut borrow = false;
        for (left, &right) in words.iter_mut().zip(rhs) {
            let (first, first_borrow) = left.overflowing_sub(right);
            let (difference, second_borrow) = first.overflowing_sub(u64::from(borrow));
            *left = difference;
            borrow = first_borrow || second_borrow;
        }
        assert!(!borrow);
    }

    fn multiply_small(words: &mut [u64; CUBIC_LIMBS], factor: u64) {
        let mut carry = 0_u64;
        for word in words {
            let product = u128::from(*word) * u128::from(factor) + u128::from(carry);
            *word = product as u64;
            carry = (product >> 64) as u64;
        }
        assert_eq!(carry, 0);
    }

    #[test]
    fn centroid_quotient_rounds_the_subnormal_zero_boundary() {
        // With area magnitude 2, one cubic unit is 2^-1074 / 6.  Three units
        // is the exact midpoint between zero and the smallest subnormal.
        let mut area = [0_u64; LIMBS];
        area[0] = 2;
        for (units, expected) in [(2, 0.0), (3, 0.0), (4, f64::from_bits(1))] {
            let mut magnitude = [0_u64; CUBIC_LIMBS];
            magnitude[0] = units;
            assert_eq!(
                exact_centroid_quotient(&moment(magnitude), &area, Ordering::Greater),
                Some(expected)
            );
        }
    }

    #[test]
    fn centroid_quotient_uses_the_virtual_overflow_successor() {
        let mut area = [0_u64; LIMBS];
        area[0] = 2;
        // q = 2^1024 - 2^970 is the exact midpoint of f64::MAX and virtual
        // 2^1024. With area magnitude 2 it is 6*(2^2098 - 2^2044) cubic
        // units; assemble it without production candidate-product helpers.
        let mut midpoint = power(2098);
        subtract(&mut midpoint, &power(2044));
        multiply_small(&mut midpoint, 6);
        let mut below = midpoint;
        decrement(&mut below);
        assert_eq!(
            exact_centroid_quotient(&moment(below), &area, Ordering::Greater),
            Some(f64::MAX)
        );
        assert_eq!(
            exact_centroid_quotient(&moment(midpoint), &area, Ordering::Greater),
            None
        );
        let mut above = midpoint;
        increment(&mut above);
        assert_eq!(
            exact_centroid_quotient(&moment(above), &area, Ordering::Greater),
            None
        );
    }

    #[test]
    fn centroid_quotient_rounds_maximum_and_ordinary_ties_to_even() {
        let mut area = [0_u64; LIMBS];
        area[0] = 2;
        // 6*q*2^1074 units: exact MAX, then the two adjacent-binade ties.
        let mut maximum = power(2098);
        subtract(&mut maximum, &power(2045));
        multiply_small(&mut maximum, 6);
        assert_eq!(exact_centroid_quotient(&moment(maximum), &area, Ordering::Greater), Some(f64::MAX));

        let mut first_tie = power(1074);
        let half_ulp = power(1021);
        add_words(&mut first_tie, &half_ulp);
        multiply_small(&mut first_tie, 6);
        assert_eq!(exact_centroid_quotient(&moment(first_tie), &area, Ordering::Greater), Some(1.0));

        let mut second_tie = power(1074);
        add_words(&mut second_tie, &half_ulp);
        add_words(&mut second_tie, &half_ulp);
        add_words(&mut second_tie, &half_ulp);
        multiply_small(&mut second_tie, 6);
        let upper = f64::from_bits(1.0_f64.to_bits() + 2);
        assert_eq!(exact_centroid_quotient(&moment(second_tie), &area, Ordering::Greater), Some(upper));
        assert_eq!(exact_centroid_quotient(&negative_moment(second_tie), &area, Ordering::Greater), Some(-upper));
    }

    #[test]
    fn centroid_quotient_canonicalizes_negative_underflow() {
        let mut area = [0_u64; LIMBS];
        area[0] = 2;
        let mut magnitude = [0_u64; CUBIC_LIMBS];
        magnitude[0] = 3;
        assert_eq!(exact_centroid_quotient(&negative_moment(magnitude), &area, Ordering::Greater).unwrap().to_bits(), 0);
    }
}
