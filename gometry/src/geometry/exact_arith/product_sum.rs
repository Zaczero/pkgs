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
        add_shifted_product(
            if negative {
                &mut self.negative
            } else {
                &mut self.positive
            },
            product,
            shift,
        );
    }

    pub(crate) fn add_cross(&mut self, x0: f64, y0: f64, x1: f64, y1: f64) {
        self.add_product(1, x0, y1);
        self.add_product(-1, x1, y0);
    }

    pub(crate) fn compare_unsigned(&self, other: &Self) -> Ordering {
        let (_, left) = self.signed_magnitude();
        let (_, right) = other.signed_magnitude();
        cmp_magnitude(&left, &right)
    }

    /// Round doubled area `/ 2` to binary64, ties-to-even. Exact zero and
    /// either-signed underflow become `+0.0`; overflow becomes `±∞`.
    pub(crate) fn round_to_area(&self) -> f64 {
        let (sign, magnitude) = self.signed_magnitude();
        if sign == Ordering::Equal {
            return 0.0;
        }
        let value = round_nonnegative(&magnitude, PRODUCT_MIN_EXP - 1);
        if value == 0.0 {
            0.0
        } else if sign == Ordering::Less {
            -value
        } else {
            value
        }
    }

    /// Round this exact product sum to binary64, ties-to-even. Exact zero is
    /// canonical `+0.0`; a nonzero negative result that underflows keeps its
    /// `-0.0` sign. The caller owns finite-coordinate admission for infinity.
    pub(crate) fn round_to_binary64(&self) -> f64 {
        let (sign, magnitude) = self.signed_magnitude();
        if sign == Ordering::Equal {
            return 0.0;
        }
        let value = round_nonnegative(&magnitude, PRODUCT_MIN_EXP);
        if sign == Ordering::Less {
            -value
        } else {
            value
        }
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

    /// Finish the centroid together with the rounded absolute area used as
    /// its component weight. Both values come from this one accumulation.
    pub(crate) fn finish_with_area(&self) -> Option<(f64, f64, f64)> {
        let (x, y) = self.finish()?;
        Some((self.area.round_to_area().abs(), x, y))
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
    let shift = usize::try_from(exponent + 1074).expect("candidate aligns to cubic quantum");
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

/// Place a 106-bit finite product at bit `shift` of the degree-2 accumulator.
///
/// Finite binary64 products have `shift <= 4090`, so the 2- or 3-limb window
/// ends at index 65, inside [`LIMBS`]. Carry uses the same 4352-bit capacity
/// already proven for the signed accumulators.
fn add_shifted_product(target: &mut [u64; LIMBS], product: u128, shift: usize) {
    debug_assert!(shift <= 4090);
    let word = shift / 64;
    let bits = shift % 64;
    let lo = product as u64;
    let hi = (product >> 64) as u64;
    let (low, mid, high) = if bits == 0 {
        (lo, hi, 0_u64)
    } else {
        (
            lo << bits,
            (lo >> (64 - bits)) | (hi << bits),
            hi >> (64 - bits),
        )
    };
    let mut acc = u128::from(target[word]) + u128::from(low);
    target[word] = acc as u64;
    acc >>= 64;
    if mid == 0 && high == 0 && acc == 0 {
        return;
    }
    acc += u128::from(target[word + 1]) + u128::from(mid);
    target[word + 1] = acc as u64;
    acc >>= 64;
    if high == 0 && acc == 0 {
        return;
    }
    acc += u128::from(target[word + 2]) + u128::from(high);
    target[word + 2] = acc as u64;
    acc >>= 64;
    let mut index = word + 3;
    while acc != 0 {
        acc += u128::from(target[index]);
        target[index] = acc as u64;
        acc >>= 64;
        index += 1;
        debug_assert!(index <= LIMBS);
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

/// Round a nonnegative exact sum whose limb quantum is `2^quantum_exp`.
/// Rounding is nearest-even, including subnormal and overflow boundaries.
fn round_nonnegative(magnitude: &[u64; LIMBS], quantum_exp: i32) -> f64 {
    let Some(k) = most_significant_bit(magnitude) else {
        return 0.0;
    };
    let normal_threshold =
        usize::try_from(-1022 - quantum_exp).expect("product-sum normal threshold fits usize");
    if k < normal_threshold {
        let subnormal_shift =
            usize::try_from(-1074 - quantum_exp).expect("product-sum subnormal shift fits usize");
        let mut units = bits_from(magnitude, subnormal_shift + 51, 52);
        let guard = limb_bit(magnitude, subnormal_shift - 1);
        let sticky = or_bits_through(magnitude, subnormal_shift - 2);
        if guard && (sticky || units & 1 != 0) {
            units += 1;
        }
        f64::from_bits(units)
    } else {
        let mut mantissa = bits_from(magnitude, k, 53);
        let guard = limb_bit(magnitude, k - 53);
        let sticky = or_bits_through(magnitude, k - 54);
        if guard && (sticky || mantissa & 1 != 0) {
            mantissa += 1;
        }
        let mut exponent = k as i32 + quantum_exp;
        if mantissa == 1_u64 << 53 {
            mantissa = 1_u64 << 52;
            exponent += 1;
        }
        if exponent > 1023 {
            return f64::INFINITY;
        }
        let biased = (exponent + 1023) as u64;
        let fraction = mantissa & ((1_u64 << 52) - 1);
        f64::from_bits((biased << 52) | fraction)
    }
}

#[cfg(test)]
fn round_nonnegative_area(magnitude: &[u64; LIMBS]) -> f64 {
    round_nonnegative(magnitude, PRODUCT_MIN_EXP - 1)
}

fn most_significant_bit(limbs: &[u64; LIMBS]) -> Option<usize> {
    for word in (0..LIMBS).rev() {
        let value = limbs[word];
        if value != 0 {
            return Some(word * 64 + (63 - value.leading_zeros() as usize));
        }
    }
    None
}

fn limb_bit(limbs: &[u64; LIMBS], bit: usize) -> bool {
    debug_assert!(bit < LIMBS * 64);
    (limbs[bit / 64] >> (bit % 64)) & 1 != 0
}

fn or_bits_through(limbs: &[u64; LIMBS], last: usize) -> bool {
    debug_assert!(last < LIMBS * 64);
    let last_word = last / 64;
    let last_off = last % 64;
    if limbs[..last_word].iter().any(|&word| word != 0) {
        return true;
    }
    let mask = if last_off == 63 {
        u64::MAX
    } else {
        (1_u64 << (last_off + 1)) - 1
    };
    limbs[last_word] & mask != 0
}

fn bits_from(limbs: &[u64; LIMBS], high: usize, width: u32) -> u64 {
    debug_assert!((1..=53).contains(&width));
    debug_assert!(high < LIMBS * 64);
    let low = high + 1 - width as usize;
    let word = low / 64;
    let offset = low % 64;
    let mut value = limbs[word] >> offset;
    if offset != 0 && word + 1 < LIMBS {
        value |= limbs[word + 1] << (64 - offset);
    }
    value & ((1_u64 << width) - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "test helpers move limb arrays into the accumulator they construct"
    )]
    fn moment(magnitude: [u64; CUBIC_LIMBS]) -> ExactCubicSum {
        ExactCubicSum {
            positive: magnitude,
            negative: [0; CUBIC_LIMBS],
        }
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "test helpers move limb arrays into the accumulator they construct"
    )]
    fn negative_moment(magnitude: [u64; CUBIC_LIMBS]) -> ExactCubicSum {
        ExactCubicSum {
            positive: [0; CUBIC_LIMBS],
            negative: magnitude,
        }
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
        assert_eq!(
            exact_centroid_quotient(&moment(maximum), &area, Ordering::Greater),
            Some(f64::MAX)
        );

        let mut first_tie = power(1074);
        let half_ulp = power(1021);
        add_words(&mut first_tie, &half_ulp);
        multiply_small(&mut first_tie, 6);
        assert_eq!(
            exact_centroid_quotient(&moment(first_tie), &area, Ordering::Greater),
            Some(1.0)
        );

        let mut second_tie = power(1074);
        add_words(&mut second_tie, &half_ulp);
        add_words(&mut second_tie, &half_ulp);
        add_words(&mut second_tie, &half_ulp);
        multiply_small(&mut second_tie, 6);
        let upper = f64::from_bits(1.0_f64.to_bits() + 2);
        assert_eq!(
            exact_centroid_quotient(&moment(second_tie), &area, Ordering::Greater),
            Some(upper)
        );
        assert_eq!(
            exact_centroid_quotient(&negative_moment(second_tie), &area, Ordering::Greater),
            Some(-upper)
        );
    }

    #[test]
    fn centroid_quotient_canonicalizes_negative_underflow() {
        let mut area = [0_u64; LIMBS];
        area[0] = 2;
        let mut magnitude = [0_u64; CUBIC_LIMBS];
        magnitude[0] = 3;
        assert_eq!(
            exact_centroid_quotient(&negative_moment(magnitude), &area, Ordering::Greater)
                .unwrap()
                .to_bits(),
            0
        );
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "test helpers move limb arrays into the accumulator they construct"
    )]
    fn doubled_sum(magnitude: [u64; LIMBS], sign: Ordering) -> ExactSum {
        match sign {
            Ordering::Equal => ExactSum::zero(),
            Ordering::Greater => ExactSum {
                positive: magnitude,
                negative: [0; LIMBS],
            },
            Ordering::Less => ExactSum {
                positive: [0; LIMBS],
                negative: magnitude,
            },
        }
    }

    fn set_bit(limbs: &mut [u64; LIMBS], bit: usize) {
        limbs[bit / 64] |= 1_u64 << (bit % 64);
    }

    fn area_bits(bits: &[usize]) -> [u64; LIMBS] {
        let mut limbs = [0_u64; LIMBS];
        for &bit in bits {
            set_bit(&mut limbs, bit);
        }
        limbs
    }

    fn area_range(low: usize, high: usize) -> [u64; LIMBS] {
        let mut limbs = [0_u64; LIMBS];
        for bit in low..=high {
            set_bit(&mut limbs, bit);
        }
        limbs
    }

    fn add_product_bits(limbs: &mut [u64; LIMBS], product: u128, shift: usize) {
        let mut value = product;
        let mut bit = shift;
        while value != 0 {
            if value & 1 != 0 {
                let mut index = bit / 64;
                let mut add = 1_u64 << (bit % 64);
                while add != 0 {
                    let (sum, carry) = limbs[index].overflowing_add(add);
                    limbs[index] = sum;
                    add = u64::from(carry);
                    index += 1;
                }
            }
            value >>= 1;
            bit += 1;
        }
    }

    fn expected_sum(terms: &[(i8, f64, f64)]) -> ExactSum {
        let mut positive = [0_u64; LIMBS];
        let mut negative = [0_u64; LIMBS];
        for &(coefficient, lhs, rhs) in terms {
            let (lhs_negative, lhs_mantissa, lhs_exponent) = decompose(lhs);
            let (rhs_negative, rhs_mantissa, rhs_exponent) = decompose(rhs);
            if lhs_mantissa == 0 || rhs_mantissa == 0 {
                continue;
            }
            let product = u128::from(lhs_mantissa) * u128::from(rhs_mantissa);
            let shift = usize::try_from(lhs_exponent + rhs_exponent - PRODUCT_MIN_EXP).unwrap();
            if (coefficient < 0) ^ lhs_negative ^ rhs_negative {
                add_product_bits(&mut negative, product, shift);
            } else {
                add_product_bits(&mut positive, product, shift);
            }
        }
        ExactSum { positive, negative }
    }

    fn actual_sum(terms: &[(i8, f64, f64)]) -> ExactSum {
        let mut sum = ExactSum::zero();
        for &(coefficient, lhs, rhs) in terms {
            sum.add_product(coefficient, lhs, rhs);
        }
        sum
    }

    fn assert_terms(terms: &[(i8, f64, f64)]) {
        let expected = expected_sum(terms);
        let actual = actual_sum(terms);
        assert_eq!(actual.positive, expected.positive, "positive {terms:?}");
        assert_eq!(actual.negative, expected.negative, "negative {terms:?}");
    }

    #[test]
    fn add_product_places_min_subnormal_square_at_bit_zero() {
        let tiny = f64::from_bits(1);
        assert_terms(&[(1, tiny, tiny)]);
        let sum = actual_sum(&[(1, tiny, tiny)]);
        assert_eq!(sum.positive[0], 1);
        assert_eq!(sum.round_to_area().to_bits(), 0);
    }

    #[test]
    fn add_product_places_max_square_at_hand_computed_bits() {
        // (2^53-1)^2 * 2^4090 = (2^106 - 2^54 + 1) * 2^4090
        // = 2^4090 + 2^4144 * (2^52 - 1): bits 4090 and 4144..=4195.
        assert_terms(&[(1, f64::MAX, f64::MAX)]);
        let sum = actual_sum(&[(1, f64::MAX, f64::MAX)]);
        assert!(limb_bit(&sum.positive, 4090));
        assert!(limb_bit(&sum.positive, 4144));
        assert!(limb_bit(&sum.positive, 4195));
        assert!(!limb_bit(&sum.positive, 4143));
        assert!(!limb_bit(&sum.positive, 4196));
    }

    #[test]
    fn raw_sum_rounds_at_midpoint_and_subnormal_boundaries() {
        let midpoint = actual_sum(&[(1, 1.0, 1.0), (1, 2.0_f64.powi(-53), 1.0)]);
        assert_eq!(midpoint.round_to_binary64().to_bits(), 1.0_f64.to_bits());

        let min_subnormal = actual_sum(&[(1, f64::from_bits(1), 1.0)]);
        assert_eq!(min_subnormal.round_to_binary64().to_bits(), 1_u64);

        let negative_underflow = actual_sum(&[(-1, f64::from_bits(1), 0.5)]);
        assert_eq!(
            negative_underflow.round_to_binary64().to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(
            negative_underflow.round_to_area().to_bits(),
            0.0_f64.to_bits()
        );
    }

    #[test]
    fn raw_sum_rounds_the_binary64_overflow_midpoint() {
        let below = actual_sum(&[(1, f64::MAX, 1.0), (1, 2.0_f64.powi(969), 1.0)]);
        assert_eq!(below.round_to_binary64().to_bits(), f64::MAX.to_bits());

        let midpoint = actual_sum(&[(1, f64::MAX, 1.0), (1, 2.0_f64.powi(970), 1.0)]);
        assert!(midpoint.round_to_binary64().is_infinite());
    }

    fn product_bit_offset(lhs: f64, rhs: f64) -> usize {
        let (_, _, left) = decompose(lhs);
        let (_, _, right) = decompose(rhs);
        usize::try_from(left + right - PRODUCT_MIN_EXP).unwrap()
    }

    #[test]
    fn add_product_aligned_and_end_shifts() {
        assert_eq!(product_bit_offset(1.0, 16.0) % 64, 0);
        assert_eq!(product_bit_offset(1.0, 32.0) % 64, 1);
        assert_eq!(product_bit_offset(1.0, 8.0) % 64, 63);
        assert_terms(&[(1, 1.0, 16.0)]);
        assert_terms(&[(1, 1.0, 32.0)]);
        assert_terms(&[(1, 1.0, 8.0)]);
    }

    #[test]
    fn add_product_carry_into_second_third_and_higher_limbs() {
        let mut second = [0_u64; LIMBS];
        second[2] = u64::MAX;
        add_shifted_product(&mut second, 1, 128);
        assert_eq!(second[2], 0);
        assert_eq!(second[3], 1);

        let mut third = [0_u64; LIMBS];
        third[4] = u64::MAX;
        third[5] = u64::MAX;
        add_shifted_product(&mut third, 1, 256);
        assert_eq!(third[4], 0);
        assert_eq!(third[5], 0);
        assert_eq!(third[6], 1);

        let mut higher = [0_u64; LIMBS];
        higher[2..8].fill(u64::MAX);
        add_shifted_product(&mut higher, 1, 128);
        assert_eq!(higher[2], 0);
        assert_eq!(higher[7], 0);
        assert_eq!(higher[8], 1);

        assert_terms(&[(1, 1.0, 1.0), (1, 1.0, 1.0)]);
    }

    #[test]
    fn add_product_cancels_across_sign_banks_to_plus_zero() {
        assert_terms(&[(1, 3.0, 5.0), (-1, 3.0, 5.0)]);
        assert_terms(&[(1, 3.0, 5.0), (1, -3.0, 5.0)]);
        let cancelled = actual_sum(&[(1, 3.0, 5.0), (-1, 3.0, 5.0)]);
        assert_eq!(cancelled.ordering(), Ordering::Equal);
        assert_eq!(cancelled.round_to_area().to_bits(), 0);
        let opposite = actual_sum(&[(1, 3.0, 5.0), (1, -3.0, 5.0)]);
        assert_eq!(opposite.ordering(), Ordering::Equal);
        assert_eq!(opposite.round_to_area().to_bits(), 0);
    }

    #[test]
    fn add_product_signed_zero_is_a_no_op() {
        assert_terms(&[(1, 0.0, 1.0), (1, -0.0, 1.0), (1, 1.0, -0.0)]);
        let mut sum = ExactSum::zero();
        sum.add_product(1, 0.0, 1.0);
        sum.add_product(1, -0.0, -0.0);
        assert_eq!(sum.ordering(), Ordering::Equal);
        assert_eq!(sum.round_to_area().to_bits(), 0);
    }

    #[test]
    fn area_finish_rounds_exact_half_min_to_plus_zero() {
        assert_eq!(round_nonnegative_area(&area_bits(&[1074])).to_bits(), 0);
        assert_eq!(
            doubled_sum(area_bits(&[1074]), Ordering::Less)
                .round_to_area()
                .to_bits(),
            0
        );
    }

    #[test]
    fn area_finish_sticky_above_half_min_is_least_subnormal() {
        assert_eq!(
            round_nonnegative_area(&area_bits(&[1074, 0])),
            f64::from_bits(1)
        );
        assert_eq!(
            doubled_sum(area_bits(&[1074, 0]), Ordering::Less).round_to_area(),
            -f64::from_bits(1)
        );
    }

    #[test]
    fn area_finish_exact_min_subnormal() {
        assert_eq!(
            round_nonnegative_area(&area_bits(&[1075])),
            f64::from_bits(1)
        );
    }

    #[test]
    fn area_finish_exact_min_normal() {
        assert_eq!(
            round_nonnegative_area(&area_bits(&[1127])),
            f64::MIN_POSITIVE
        );
        assert_eq!(
            doubled_sum(area_bits(&[1127]), Ordering::Less).round_to_area(),
            -f64::MIN_POSITIVE
        );
    }

    #[test]
    fn area_finish_subnormal_tie_carries_to_min_normal() {
        assert_eq!(
            round_nonnegative_area(&area_range(1074, 1126)),
            f64::MIN_POSITIVE
        );
        assert_eq!(
            doubled_sum(area_range(1074, 1126), Ordering::Less).round_to_area(),
            -f64::MIN_POSITIVE
        );
    }

    #[test]
    fn area_finish_rounds_normal_tie_even_and_odd() {
        assert_eq!(round_nonnegative_area(&area_bits(&[2149, 2096])), 1.0);
        assert_eq!(
            round_nonnegative_area(&area_bits(&[2149, 2097, 2096])),
            f64::from_bits(1.0_f64.to_bits() + 2)
        );
    }

    #[test]
    fn area_finish_sticky_strictly_above_one_midpoint() {
        assert_eq!(
            round_nonnegative_area(&area_bits(&[2149, 2096, 2049])),
            1.0_f64.next_up()
        );
    }

    #[test]
    fn area_finish_power_of_two_carry() {
        assert_eq!(round_nonnegative_area(&area_range(2096, 2149)), 2.0);
    }

    #[test]
    fn area_finish_double_rounding_countercase_is_just_above_half_min_normal() {
        assert_eq!(
            round_nonnegative_area(&area_bits(&[1126, 1074, 0])),
            f64::from_bits((1_u64 << 51) + 1)
        );
    }

    #[test]
    fn area_finish_overflows_at_the_max_midpoint() {
        let midpoint = area_range(3119, 3172);
        assert_eq!(round_nonnegative_area(&midpoint), f64::INFINITY);
        assert_eq!(
            doubled_sum(midpoint, Ordering::Less).round_to_area(),
            f64::NEG_INFINITY
        );
    }

    #[test]
    fn area_finish_one_below_overflow_midpoint_is_max() {
        let mut just_below = area_range(0, 3172);
        just_below[3119 / 64] &= !(1_u64 << (3119 % 64));
        assert_eq!(round_nonnegative_area(&just_below), f64::MAX);
        assert_eq!(
            doubled_sum(just_below, Ordering::Less).round_to_area(),
            -f64::MAX
        );
    }

    #[test]
    fn area_finish_preserves_representable_max() {
        let mut maximum = [0_u64; LIMBS];
        add_shifted_words(&mut maximum, &[(1_u64 << 53) - 1], 3120);
        assert_eq!(round_nonnegative_area(&maximum), f64::MAX);
        assert_eq!(
            doubled_sum(maximum, Ordering::Less).round_to_area(),
            -f64::MAX
        );
    }
}
