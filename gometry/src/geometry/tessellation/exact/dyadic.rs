//! Exact dyadic rationals: a sign, an exponent and a compact limb buffer.
//!
//! Every value is a finite binary fraction held exactly, so addition,
//! subtraction and multiplication never round. Division does not appear here:
//! a quotient is carried as a ratio and compared rather than evaluated.

use std::cmp::Ordering;
use std::fmt;
use std::ops::{Deref, DerefMut};

use super::{ExactPoint, ExactSign, GeometryErrorKind, ResourceError, Result, XY, try_reserve_vec};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactDyadic {
    pub(in crate::geometry) negative: bool,
    pub(in crate::geometry) limbs: CompactLimbs,
    // Every operation in this fixed construction DAG remains far inside i64.
    // A stored binary64 contributes at most 1,074 exponent bits and the source
    // audit has fewer than twenty arithmetic levels, covered conservatively by
    // 1,127 * 2^20.  The only data-length recurrence is cycle_orientation's
    // denominator product.  Source clip and rounded-shell cycles have w=1, so
    // that recurrence is inert there; generated subdivision visits are charged
    // to the operation-wide 16M budget.  Thus
    // 1,127 * 2^20 * (16,000,000 + 1) < 1.90e16, over 480 times inside i64.
    // The same derivation keeps nonnegative alignment deltas inside usize on
    // the supported 64-bit targets, so exact ordering needs no error channel.
    pub(in crate::geometry) exponent: i64,
}

/// Every observed value used at most one trimmed limb in ordinary Cartesian
/// relation, overlay, and polygonize workloads. Two inline limbs also cover
/// the untrimmed sum/product of two observed one-limb operands; larger exact
/// values spill to a heap vector.
pub(in crate::geometry) const INLINE_DYADIC_LIMBS: usize = 2;

pub(in crate::geometry) enum CompactLimbs {
    Inline {
        limbs: [u64; INLINE_DYADIC_LIMBS],
        len: u8,
    },
    Heap(Vec<u64>),
}

impl Clone for CompactLimbs {
    fn clone(&self) -> Self {
        match self {
            Self::Inline { limbs, len } => Self::Inline {
                limbs: *limbs,
                len: *len,
            },
            Self::Heap(limbs) => Self::from_vec(limbs.clone()),
        }
    }
}

impl fmt::Debug for CompactLimbs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.as_slice()).finish()
    }
}

impl PartialEq for CompactLimbs {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for CompactLimbs {}

impl CompactLimbs {
    pub(in crate::geometry) const fn zero() -> Self {
        Self::Inline {
            limbs: [0; INLINE_DYADIC_LIMBS],
            len: 0,
        }
    }

    pub(in crate::geometry) const fn single(value: u64) -> Self {
        Self::Inline {
            limbs: [value; INLINE_DYADIC_LIMBS],
            len: 1,
        }
    }

    pub(in crate::geometry) fn try_with_capacity(required: usize) -> Result<Self> {
        if required <= INLINE_DYADIC_LIMBS {
            return Ok(Self::Inline {
                limbs: [0; INLINE_DYADIC_LIMBS],
                len: 0,
            });
        }
        let mut limbs = Vec::new();
        try_reserve_vec(&mut limbs, required)?;
        Ok(Self::Heap(limbs))
    }

    pub(in crate::geometry) fn from_vec(limbs: Vec<u64>) -> Self {
        if limbs.len() <= INLINE_DYADIC_LIMBS {
            let mut inline = [0; INLINE_DYADIC_LIMBS];
            inline[..limbs.len()].copy_from_slice(&limbs);
            Self::Inline {
                limbs: inline,
                len: limbs.len() as u8,
            }
        } else {
            Self::Heap(limbs)
        }
    }

    pub(in crate::geometry) fn as_slice(&self) -> &[u64] {
        match self {
            Self::Inline { limbs, len } => &limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    pub(in crate::geometry) fn as_mut_slice(&mut self) -> &mut [u64] {
        match self {
            Self::Inline { limbs, len } => &mut limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    pub(in crate::geometry) const fn is_empty(&self) -> bool {
        match self {
            Self::Inline { len, .. } => *len == 0,
            Self::Heap(limbs) => limbs.is_empty(),
        }
    }

    pub(in crate::geometry) fn push(&mut self, value: u64) {
        match self {
            Self::Inline { limbs, len } => {
                let index = usize::from(*len);
                assert!(
                    index < INLINE_DYADIC_LIMBS,
                    "inline dyadic capacity exceeded"
                );
                limbs[index] = value;
                *len += 1;
            },
            Self::Heap(limbs) => limbs.push(value),
        }
    }

    pub(in crate::geometry) fn pop(&mut self) -> Option<u64> {
        match self {
            Self::Inline { limbs, len } => {
                let index = usize::from(*len).checked_sub(1)?;
                *len -= 1;
                Some(limbs[index])
            },
            Self::Heap(limbs) => limbs.pop(),
        }
    }

    pub(in crate::geometry) fn remove_prefix(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        match self {
            Self::Inline { limbs, len } => {
                let length = usize::from(*len);
                limbs.copy_within(count..length, 0);
                limbs[length - count..length].fill(0);
                *len -= count as u8;
            },
            Self::Heap(limbs) => {
                limbs.drain(..count);
            },
        }
    }
}

impl Deref for CompactLimbs {
    type Target = [u64];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for CompactLimbs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// An exact homogeneous line `a*x + b*y + c = 0` over stored binary64 input.
///
/// Keeping the coefficients opaque prevents construction code from mixing
/// rounded and exact values while still exposing the complete construction ladder.
#[derive(Clone, Debug)]
pub(in crate::geometry) struct ExactLine(pub(in crate::geometry) [ExactDyadic; 3]);

impl ExactLine {
    pub(in crate::geometry) fn perpendicular_bisector(a: XY, b: XY) -> Result<Self> {
        let ax = ExactDyadic::from_f64(a.x);
        let ay = ExactDyadic::from_f64(a.y);
        let bx = ExactDyadic::from_f64(b.x);
        let by = ExactDyadic::from_f64(b.y);
        let two = ExactDyadic::from_f64(2.0);
        Ok(Self([
            ExactDyadic::try_subtract_refs(&bx, &ax)?.try_product(&two)?,
            ExactDyadic::try_subtract_refs(&by, &ay)?.try_product(&two)?,
            ax.try_product(&ax)?
                .try_add(ay.try_product(&ay)?)?
                .try_subtract(bx.try_product(&bx)?.try_add(by.try_product(&by)?)?)?,
        ]))
    }

    pub(in crate::geometry) fn through_points(a: &ExactPoint, b: &ExactPoint) -> Result<Self> {
        if !a.is_finite() || !b.is_finite() || a.same_position(b)? {
            return Err(GeometryErrorKind::voronoi(
                "exact support line requires two distinct finite points",
            ));
        }
        let (a, b) = (a.coords()?, b.coords()?);
        Ok(Self([
            a.y.try_product(&b.w)?
                .try_subtract(b.y.try_product(&a.w)?)?,
            b.x.try_product(&a.w)?
                .try_subtract(a.x.try_product(&b.w)?)?,
            a.x.try_product(&b.y)?
                .try_subtract(b.x.try_product(&a.y)?)?,
        ]))
    }
}

/// A virtual left-shifted view of a limb slice. `word(i)` reads the aligned
/// word directly, so every operation uses the same words in the same order
/// with no temporary.
#[derive(Clone, Copy)]
pub(in crate::geometry) struct Aligned<'a> {
    pub(in crate::geometry) limbs: &'a [u64],
    pub(in crate::geometry) word_shift: usize,
    pub(in crate::geometry) bit_shift: u32,
    pub(in crate::geometry) len: usize,
}

impl<'a> Aligned<'a> {
    pub(in crate::geometry) fn new(limbs: &'a [u64], shift: usize) -> Self {
        Self {
            limbs,
            word_shift: shift / 64,
            bit_shift: (shift % 64) as u32,
            len: ExactDyadic::shifted_len(limbs, shift),
        }
    }

    pub(in crate::geometry) fn word(&self, index: usize) -> u64 {
        ExactDyadic::shifted_word(self.limbs, self.word_shift, self.bit_shift, index)
    }
}

impl ExactDyadic {
    pub(in crate::geometry) fn zero() -> Self {
        Self {
            negative: false,
            limbs: CompactLimbs::zero(),
            exponent: 0,
        }
    }

    pub(in crate::geometry) fn one() -> Self {
        Self {
            negative: false,
            limbs: CompactLimbs::single(1),
            exponent: 0,
        }
    }

    /// Construct an adversarial exact value from its canonical limb sequence.
    ///
    /// Internal tests use this for exponent and multi-limb boundaries that
    /// cannot be reached from one binary64 input. The storage chooses inline
    /// or heap form without changing the supplied exact value.
    #[cfg(test)]
    pub(in crate::geometry) fn from_parts(negative: bool, limbs: Vec<u64>, exponent: i64) -> Self {
        Self {
            negative,
            limbs: CompactLimbs::from_vec(limbs),
            exponent,
        }
    }

    pub(in crate::geometry) fn from_f64(value: f64) -> Self {
        debug_assert!(value.is_finite());
        if value == 0.0 {
            return Self::zero();
        }
        let bits = value.to_bits();
        let fraction = bits & ((1_u64 << 52) - 1);
        let encoded_exponent = ((bits >> 52) & 0x7FF) as i64;
        let (significand, exponent) = if encoded_exponent == 0 {
            (fraction, -1074)
        } else {
            ((1_u64 << 52) | fraction, encoded_exponent - 1023 - 52)
        };
        let mut value = Self {
            negative: bits >> 63 != 0,
            limbs: CompactLimbs::single(significand),
            exponent,
        };
        value.trim();
        value
    }

    pub(in crate::geometry) fn trim(&mut self) {
        while self.limbs.last() == Some(&0) {
            self.limbs.pop();
        }
        if self.limbs.is_empty() {
            self.negative = false;
            self.exponent = 0;
            return;
        }
        let zero_words = self.limbs.iter().take_while(|&&limb| limb == 0).count();
        if zero_words != 0 {
            self.limbs.remove_prefix(zero_words);
            self.exponent += 64 * zero_words as i64;
        }
        let zero_bits = self.limbs[0].trailing_zeros();
        if zero_bits != 0 {
            let mut carry = 0_u64;
            for limb in self.limbs.iter_mut().rev() {
                let next = *limb << (64 - zero_bits);
                *limb = (*limb >> zero_bits) | carry;
                carry = next;
            }
            self.exponent += i64::from(zero_bits);
            while self.limbs.last() == Some(&0) {
                self.limbs.pop();
            }
        }
        debug_assert_eq!(self.limbs[0] & 1, 1);
    }

    pub(in crate::geometry) const fn is_zero(&self) -> bool {
        self.limbs.is_empty()
    }

    pub(in crate::geometry) fn is_one(&self) -> bool {
        !self.negative && self.exponent == 0 && self.limbs.as_slice() == [1]
    }

    pub(in crate::geometry) const fn sign(&self) -> ExactSign {
        if self.is_zero() {
            ExactSign::Zero
        } else if self.negative {
            ExactSign::Negative
        } else {
            ExactSign::Positive
        }
    }

    pub(in crate::geometry) const fn neg(mut self) -> Self {
        if !self.is_zero() {
            self.negative = !self.negative;
        }
        self
    }

    pub(in crate::geometry) const fn abs(mut self) -> Self {
        self.negative = false;
        self
    }

    /// Read limb `index` of a slice as if it had been shifted left by
    /// `word_shift` words and `bit_shift` bits, without materializing the
    /// shifted vector. Same words, same order, no allocation.
    pub(in crate::geometry) fn shifted_word(
        limbs: &[u64],
        word_shift: usize,
        bit_shift: u32,
        index: usize,
    ) -> u64 {
        if index < word_shift {
            return 0;
        }
        let position = index - word_shift;
        let low = limbs.get(position).copied().unwrap_or(0);
        if bit_shift == 0 {
            return low;
        }
        let carry = if position == 0 || position > limbs.len() {
            0
        } else {
            limbs[position - 1] >> (64 - bit_shift)
        };
        (low << bit_shift) | carry
    }

    pub(in crate::geometry) fn shifted_len(limbs: &[u64], shift: usize) -> usize {
        if limbs.is_empty() {
            return 0;
        }
        limbs.len() + shift / 64 + usize::from(!shift.is_multiple_of(64))
    }

    pub(in crate::geometry) fn write_sum_aligned(
        left: Aligned<'_>,
        right: Aligned<'_>,
        out: &mut CompactLimbs,
    ) {
        debug_assert!(out.is_empty());
        let length = left.len.max(right.len);
        let mut carry = 0_u128;
        for index in 0..length {
            let sum = u128::from(left.word(index)) + u128::from(right.word(index)) + carry;
            out.push(sum as u64);
            carry = sum >> 64;
        }
        if carry != 0 {
            out.push(carry as u64);
        }
    }

    pub(in crate::geometry) fn write_diff_aligned(
        larger: Aligned<'_>,
        smaller: Aligned<'_>,
        out: &mut CompactLimbs,
    ) {
        debug_assert!(out.is_empty());
        let mut borrow = 0_i128;
        for index in 0..larger.len {
            let difference =
                i128::from(larger.word(index)) - i128::from(smaller.word(index)) - borrow;
            if difference < 0 {
                out.push((difference + (1_i128 << 64)) as u64);
                borrow = 1;
            } else {
                out.push(difference as u64);
                borrow = 0;
            }
        }
        debug_assert_eq!(borrow, 0);
    }

    pub(in crate::geometry) fn try_add_aligned(
        left: Aligned<'_>,
        right: Aligned<'_>,
    ) -> Result<CompactLimbs> {
        let length = left.len.max(right.len);
        if length <= INLINE_DYADIC_LIMBS {
            // The extra limb is needed only when the final word carries. The
            // previous unconditional `length + 1` reservation made a
            // no-carry two-limb result spill even though it fit inline.
            let mut words = [0_u64; INLINE_DYADIC_LIMBS + 1];
            let mut carry = 0_u128;
            for (index, word) in words.iter_mut().take(length).enumerate() {
                let sum = u128::from(left.word(index)) + u128::from(right.word(index)) + carry;
                *word = sum as u64;
                carry = sum >> 64;
            }
            let result_length = length + usize::from(carry != 0);
            if carry != 0 {
                words[length] = carry as u64;
            }
            let mut result = CompactLimbs::try_with_capacity(result_length)?;
            for &word in &words[..result_length] {
                result.push(word);
            }
            return Ok(result);
        }
        let needed = length.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let mut result = CompactLimbs::try_with_capacity(needed)?;
        Self::write_sum_aligned(left, right, &mut result);
        Ok(result)
    }

    pub(in crate::geometry) fn try_subtract_aligned(
        larger: Aligned<'_>,
        smaller: Aligned<'_>,
    ) -> Result<CompactLimbs> {
        let mut result = CompactLimbs::try_with_capacity(larger.len)?;
        Self::write_diff_aligned(larger, smaller, &mut result);
        Ok(result)
    }

    pub(in crate::geometry) fn compare_aligned(left: Aligned<'_>, right: Aligned<'_>) -> Ordering {
        let mut index = left.len.max(right.len);
        while index > 0 {
            index -= 1;
            match left.word(index).cmp(&right.word(index)) {
                Ordering::Equal => {},
                order => return order,
            }
        }
        Ordering::Equal
    }

    pub(in crate::geometry) fn try_add(self, other: Self) -> Result<Self> {
        if self.is_zero() {
            return Ok(other);
        }
        if other.is_zero() {
            return Ok(self);
        }
        Self::try_combine_refs(&self, &other, false)
    }

    pub(in crate::geometry) fn try_add_refs(left: &Self, right: &Self) -> Result<Self> {
        if left.is_zero() {
            return Ok(right.clone());
        }
        if right.is_zero() {
            return Ok(left.clone());
        }
        Self::try_combine_refs(left, right, false)
    }

    /// The magnitude when it fits in the two inline limbs.
    pub(in crate::geometry) fn small_magnitude(&self) -> Option<u128> {
        match *self.limbs.as_slice() {
            [low] => Some(u128::from(low)),
            [low, high] => Some(u128::from(low) | (u128::from(high) << 64)),
            _ => None,
        }
    }

    /// A normalized value from a `u128` magnitude: trailing zero bits move
    /// into the exponent, leaving the odd lowest limb every value carries.
    pub(in crate::geometry) fn from_u128(negative: bool, magnitude: u128, exponent: i64) -> Self {
        if magnitude == 0 {
            return Self::zero();
        }
        let zeros = magnitude.trailing_zeros();
        let magnitude = magnitude >> zeros;
        let (low, high) = (magnitude as u64, (magnitude >> 64) as u64);
        Self {
            negative,
            limbs: if high == 0 {
                CompactLimbs::single(low)
            } else {
                CompactLimbs::Inline {
                    limbs: [low, high],
                    len: 2,
                }
            },
            exponent: exponent + i64::from(zeros),
        }
    }

    pub(in crate::geometry) fn try_combine_refs(
        left: &Self,
        right: &Self,
        negate_right: bool,
    ) -> Result<Self> {
        debug_assert!(!left.is_zero() && !right.is_zero());
        let exponent = left.exponent.min(right.exponent);
        let left_negative = left.negative;
        let right_negative = right.negative ^ negate_right;
        let left_shift =
            usize::try_from(left.exponent - exponent).map_err(|_| ResourceError::SizeOverflow)?;
        let right_shift =
            usize::try_from(right.exponent - exponent).map_err(|_| ResourceError::SizeOverflow)?;
        // One limb each, aligned within 63 bits: both magnitudes stay below
        // 2^127, so their sum or difference is exact in u128.
        if let ([left_limb], [right_limb]) = (left.limbs.as_slice(), right.limbs.as_slice())
            && left_shift < 64
            && right_shift < 64
        {
            let left_value = u128::from(*left_limb) << left_shift;
            let right_value = u128::from(*right_limb) << right_shift;
            return Ok(if left_negative == right_negative {
                Self::from_u128(left_negative, left_value + right_value, exponent)
            } else if left_value >= right_value {
                Self::from_u128(left_negative, left_value - right_value, exponent)
            } else {
                Self::from_u128(right_negative, right_value - left_value, exponent)
            });
        }
        let left = Aligned::new(&left.limbs, left_shift);
        let right = Aligned::new(&right.limbs, right_shift);
        let (negative, limbs) = if left_negative == right_negative {
            (left_negative, Self::try_add_aligned(left, right)?)
        } else {
            match Self::compare_aligned(left, right) {
                Ordering::Greater => (left_negative, Self::try_subtract_aligned(left, right)?),
                Ordering::Less => (right_negative, Self::try_subtract_aligned(right, left)?),
                Ordering::Equal => return Ok(Self::zero()),
            }
        };
        let mut result = Self {
            negative,
            limbs,
            exponent,
        };
        result.trim();
        Ok(result)
    }

    pub(in crate::geometry) fn try_subtract_ref(&self, other: &Self) -> Result<Self> {
        Self::try_subtract_refs(self, other)
    }

    pub(in crate::geometry) fn try_subtract_refs(left: &Self, right: &Self) -> Result<Self> {
        if left.is_zero() {
            let mut result = right.clone();
            if !result.is_zero() {
                result.negative = !result.negative;
            }
            return Ok(result);
        }
        if right.is_zero() {
            return Ok(left.clone());
        }
        Self::try_combine_refs(left, right, true)
    }

    pub(in crate::geometry) fn try_subtract(self, other: Self) -> Result<Self> {
        if self.is_zero() {
            return Ok(other.neg());
        }
        if other.is_zero() {
            return Ok(self);
        }
        Self::try_combine_refs(&self, &other, true)
    }

    /// Compare `self` with `other * 2^shift` by magnitude without constructing
    /// the scalar product. The ratio-rounding residual uses this for the exact
    /// power-of-two gap between adjacent binary64 values.
    pub(in crate::geometry) fn cmp_magnitude_scaled(
        &self,
        other: &Self,
        shift: i64,
    ) -> Option<Ordering> {
        if self.is_zero() {
            return Some(if other.is_zero() {
                Ordering::Equal
            } else {
                Ordering::Less
            });
        }
        if other.is_zero() {
            return Some(Ordering::Greater);
        }
        let other_exponent = other.exponent.checked_add(shift)?;
        let exponent = self.exponent.min(other_exponent);
        let left_shift = usize::try_from(self.exponent.checked_sub(exponent)?).ok()?;
        let right_shift = usize::try_from(other_exponent.checked_sub(exponent)?).ok()?;
        Some(Self::compare_aligned(
            Aligned::new(&self.limbs, left_shift),
            Aligned::new(&other.limbs, right_shift),
        ))
    }

    pub(in crate::geometry) fn write_product(short: &[u64], long: &[u64], out: &mut CompactLimbs) {
        debug_assert!(out.is_empty());
        let mut carry = 0_u128;
        for &right in long {
            let value = u128::from(short[0]) * u128::from(right) + carry;
            out.push(value as u64);
            carry = value >> 64;
        }
        out.push(carry as u64);
        for (left_index, &left) in short.iter().enumerate().skip(1) {
            let mut carry = 0_u128;
            for (right_index, &right) in long.iter().enumerate() {
                let index = left_index + right_index;
                let value = u128::from(out[index]) + u128::from(left) * u128::from(right) + carry;
                out[index] = value as u64;
                carry = value >> 64;
            }
            out.push(carry as u64);
        }
    }

    pub(in crate::geometry) fn try_product(&self, other: &Self) -> Result<Self> {
        if self.is_zero() || other.is_zero() {
            return Ok(Self::zero());
        }
        if let ([left], [right]) = (self.limbs.as_slice(), other.limbs.as_slice()) {
            return Ok(Self::from_u128(
                self.negative != other.negative,
                u128::from(*left) * u128::from(*right),
                self.exponent + other.exponent,
            ));
        }
        let (short, long) = if self.limbs.len() <= other.limbs.len() {
            (&self.limbs, &other.limbs)
        } else {
            (&other.limbs, &self.limbs)
        };
        let needed = short
            .len()
            .checked_add(long.len())
            .ok_or(ResourceError::SizeOverflow)?;
        let mut limbs = CompactLimbs::try_with_capacity(needed)?;
        Self::write_product(short, long, &mut limbs);
        let mut result = Self {
            negative: self.negative != other.negative,
            limbs,
            exponent: self.exponent + other.exponent,
        };
        result.trim();
        Ok(result)
    }

    pub(in crate::geometry) fn compare_unsigned(left: &[u64], right: &[u64]) -> Ordering {
        let left_len = left.len();
        let right_len = right.len();
        match left_len.cmp(&right_len) {
            Ordering::Equal => {},
            order => return order,
        }
        for (&left, &right) in left.iter().rev().zip(right.iter().rev()) {
            match left.cmp(&right) {
                Ordering::Equal => {},
                order => return order,
            }
        }
        Ordering::Equal
    }

    /// Quotient and remainder of trimmed magnitudes, one 64-bit digit at a
    /// time (Knuth, Algorithm D): the divisor is normalized so its top bit is
    /// set, which keeps every estimated digit at most two above the true one.
    pub(in crate::geometry) fn try_div_rem_unsigned(
        dividend: &[u64],
        divisor: &[u64],
    ) -> Result<(Vec<u64>, Vec<u64>)> {
        debug_assert!(divisor.last().is_some_and(|&high| high != 0));
        let copy = |limbs: &[u64]| -> Result<Vec<u64>> {
            let mut out = Vec::new();
            try_reserve_vec(&mut out, limbs.len())?;
            out.extend_from_slice(limbs);
            Ok(out)
        };
        if Self::compare_unsigned(dividend, divisor) == Ordering::Less {
            return Ok((Vec::new(), copy(dividend)?));
        }
        let trim = |limbs: &mut Vec<u64>| {
            while limbs.last() == Some(&0) {
                limbs.pop();
            }
        };
        if let [digit] = *divisor {
            let digit = u128::from(digit);
            let mut quotient = copy(dividend)?;
            let mut remainder = 0_u128;
            for limb in quotient.iter_mut().rev() {
                let value = (remainder << 64) | u128::from(*limb);
                *limb = (value / digit) as u64;
                remainder = value % digit;
            }
            trim(&mut quotient);
            let remainder = if remainder == 0 {
                Vec::new()
            } else {
                copy(&[remainder as u64])?
            };
            return Ok((quotient, remainder));
        }
        let n = divisor.len();
        let m = dividend.len();
        let shift = divisor[n - 1].leading_zeros();
        let shifted = |limbs: &[u64], extra: usize| -> Result<Vec<u64>> {
            let mut out = Vec::new();
            try_reserve_vec(&mut out, limbs.len() + extra)?;
            let mut carry = 0_u64;
            for &limb in limbs {
                out.push((limb << shift) | carry);
                carry = if shift == 0 { 0 } else { limb >> (64 - shift) };
            }
            if extra > 0 {
                out.push(carry);
            }
            Ok(out)
        };
        let v = shifted(divisor, 0)?;
        let mut u = shifted(dividend, 1)?;
        let mut quotient = Vec::new();
        try_reserve_vec(&mut quotient, m - n + 1)?;
        quotient.resize(m - n + 1, 0);
        let (top, next) = (u128::from(v[n - 1]), u128::from(v[n - 2]));
        for j in (0..=m - n).rev() {
            let numerator = (u128::from(u[j + n]) << 64) | u128::from(u[j + n - 1]);
            let mut estimate = numerator / top;
            let mut rest = numerator % top;
            while estimate > u128::from(u64::MAX)
                || estimate * next > ((rest << 64) | u128::from(u[j + n - 2]))
            {
                estimate -= 1;
                rest += top;
                if rest > u128::from(u64::MAX) {
                    break;
                }
            }
            // Multiply and subtract; a negative result means the estimate was
            // one too large, and one addback of the divisor corrects it.
            let mut borrow = 0_i128;
            for i in 0..n {
                let product = estimate * u128::from(v[i]);
                let difference = i128::from(u[i + j]) - borrow - i128::from(product as u64);
                u[i + j] = difference as u64;
                borrow = (product >> 64) as i128 - (difference >> 64);
            }
            let difference = i128::from(u[j + n]) - borrow;
            u[j + n] = difference as u64;
            quotient[j] = estimate as u64;
            if difference < 0 {
                quotient[j] -= 1;
                let mut carry = 0_u128;
                for i in 0..n {
                    let sum = u128::from(u[i + j]) + u128::from(v[i]) + carry;
                    u[i + j] = sum as u64;
                    carry = sum >> 64;
                }
                u[j + n] = u[j + n].wrapping_add(carry as u64);
            }
        }
        trim(&mut quotient);
        u.truncate(n);
        if shift != 0 {
            for i in 0..n {
                let high = if i + 1 < n {
                    u[i + 1] << (64 - shift)
                } else {
                    0
                };
                u[i] = (u[i] >> shift) | high;
            }
        }
        trim(&mut u);
        Ok((quotient, u))
    }

    /// Greatest common divisor in the exact dyadic domain.
    ///
    /// `trim` keeps each nonzero limb bank odd, so the common power-of-two
    /// factor is the smaller exponent and the odd banks can use ordinary
    /// integer Euclid. The result is positive and exact.
    pub(in crate::geometry) fn try_gcd(&self, other: &Self) -> Result<Self> {
        if self.is_zero() {
            return Ok(other.clone().abs());
        }
        if other.is_zero() {
            return Ok(self.clone().abs());
        }
        if self.limbs.as_slice() == [1] && self.exponent == 0 {
            return Ok(self.clone().abs());
        }
        if other.limbs.as_slice() == [1] && other.exponent == 0 {
            return Ok(other.clone().abs());
        }
        let exponent = self.exponent.min(other.exponent);
        if let (Some(mut left), Some(mut right)) = (self.small_magnitude(), other.small_magnitude())
        {
            // Binary GCD: both magnitudes are odd, so no common power of two
            // needs restoring.
            loop {
                if left > right {
                    std::mem::swap(&mut left, &mut right);
                }
                right -= left;
                if right == 0 {
                    return Ok(Self::from_u128(false, left, exponent));
                }
                right >>= right.trailing_zeros();
            }
        }
        let mut left = Vec::new();
        try_reserve_vec(&mut left, self.limbs.len())?;
        left.extend_from_slice(&self.limbs);
        let mut right = Vec::new();
        try_reserve_vec(&mut right, other.limbs.len())?;
        right.extend_from_slice(&other.limbs);
        while !right.is_empty() {
            let (_, remainder) = Self::try_div_rem_unsigned(&left, &right)?;
            left = right;
            right = remainder;
        }
        let value = Self {
            negative: false,
            limbs: CompactLimbs::from_vec(left),
            exponent,
        };
        Ok(value)
    }

    /// Divide two exact dyadics when the quotient is itself dyadic.
    ///
    /// Rational-area reduction only calls this with a common divisor, so a
    /// nonzero remainder is an internal exactness failure rather than a
    /// rounded result.
    pub(in crate::geometry) fn try_exact_quotient(&self, divisor: &Self) -> Result<Self> {
        if divisor.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        if self.is_zero() {
            return Ok(Self::zero());
        }
        if divisor.limbs.as_slice() == [1] {
            let mut value = self.clone();
            value.negative = self.negative != divisor.negative;
            value.exponent = self
                .exponent
                .checked_sub(divisor.exponent)
                .ok_or(ResourceError::SizeOverflow)?;
            return Ok(value);
        }
        let exponent = self
            .exponent
            .checked_sub(divisor.exponent)
            .ok_or(ResourceError::SizeOverflow)?;
        if let (Some(dividend), Some(magnitude)) =
            (self.small_magnitude(), divisor.small_magnitude())
        {
            if dividend % magnitude != 0 {
                return Err(crate::geometry::GeometryErrorKind::Invalid(
                    "exact dyadic quotient has a remainder".into(),
                )
                .into());
            }
            return Ok(Self::from_u128(
                self.negative != divisor.negative,
                dividend / magnitude,
                exponent,
            ));
        }
        let (limbs, remainder) = Self::try_div_rem_unsigned(&self.limbs, &divisor.limbs)?;
        if !remainder.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "exact dyadic quotient has a remainder".into(),
            )
            .into());
        }
        let value = Self {
            negative: self.negative != divisor.negative,
            limbs: CompactLimbs::from_vec(limbs),
            exponent,
        };
        Ok(value)
    }

    pub(in crate::geometry) fn cmp(&self, other: &Self) -> Ordering {
        if self.negative != other.negative {
            return if self.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        let exponent = self.exponent.min(other.exponent);
        let order = Self::compare_aligned(
            Aligned::new(&self.limbs, (self.exponent - exponent) as usize),
            Aligned::new(&other.limbs, (other.exponent - exponent) as usize),
        );
        if self.negative {
            order.reverse()
        } else {
            order
        }
    }

    pub(in crate::geometry) fn cmp_magnitude(&self, other: &Self) -> Ordering {
        let exponent = self.exponent.min(other.exponent);
        Self::compare_aligned(
            Aligned::new(&self.limbs, (self.exponent - exponent) as usize),
            Aligned::new(&other.limbs, (other.exponent - exponent) as usize),
        )
    }
}

/// Panicking exact arithmetic for building test expectations.
#[cfg(test)]
pub(in crate::geometry) trait ExactOracle: Sized {
    fn add(self, other: Self) -> Self;
    fn subtract(self, other: Self) -> Self;
    fn product(&self, other: &Self) -> Self;
    fn square(&self) -> Self;
}

#[cfg(test)]
impl ExactOracle for ExactDyadic {
    fn add(self, other: Self) -> Self {
        self.try_add(other).unwrap()
    }

    fn subtract(self, other: Self) -> Self {
        self.try_subtract(other).unwrap()
    }

    fn product(&self, other: &Self) -> Self {
        self.try_product(other).unwrap()
    }

    fn square(&self) -> Self {
        self.try_product(self).unwrap()
    }
}

pub(in crate::geometry) fn try_difference(left: f64, right: f64) -> Result<ExactDyadic> {
    ExactDyadic::from_f64(left).try_subtract(ExactDyadic::from_f64(right))
}
