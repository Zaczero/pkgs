//! Exact dyadic rationals: a sign, an exponent and a compact limb buffer.
//!
//! Every value is a finite binary fraction held exactly, so addition,
//! subtraction and multiplication never round. Division does not appear here:
//! a quotient is carried as a ratio and compared rather than evaluated.

use std::cmp::Ordering;
use std::fmt;
use std::ops::{Deref, DerefMut};

use super::{ExactPoint, ExactSign, GeometryErrorKind, ResourceError, Result, XY};

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
            Self::Heap(limbs) if limbs.len() <= INLINE_DYADIC_LIMBS => {
                Self::inline_from_slice(limbs)
            },
            Self::Heap(limbs) => Self::Heap(limbs.clone()),
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

    pub(in crate::geometry) fn with_capacity(required: usize) -> Self {
        if required <= INLINE_DYADIC_LIMBS {
            return Self::Inline {
                limbs: [0; INLINE_DYADIC_LIMBS],
                len: 0,
            };
        }
        Self::Heap(Vec::with_capacity(required))
    }

    fn inline_from_slice(limbs: &[u64]) -> Self {
        debug_assert!(limbs.len() <= INLINE_DYADIC_LIMBS);
        match limbs {
            [] => Self::zero(),
            [limb] => Self::Inline {
                limbs: [*limb, 0],
                len: 1,
            },
            [low, high] => Self::Inline {
                limbs: [*low, *high],
                len: 2,
            },
            _ => unreachable!("inline dyadic storage has two limbs"),
        }
    }

    pub(in crate::geometry) fn from_vec(limbs: Vec<u64>) -> Self {
        if limbs.len() <= INLINE_DYADIC_LIMBS {
            Self::inline_from_slice(&limbs)
        } else {
            Self::Heap(limbs)
        }
    }

    fn shrink_inline(&mut self) {
        if let Self::Heap(limbs) = self
            && limbs.len() <= INLINE_DYADIC_LIMBS
        {
            *self = Self::inline_from_slice(limbs);
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "private inline lengths are initialized and maintained in 0..=2, matching the fixed limb array capacity"
    )]
    pub(in crate::geometry) fn as_slice(&self) -> &[u64] {
        match self {
            Self::Inline { limbs, len } => &limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "private inline lengths are initialized and maintained in 0..=2, matching the fixed limb array capacity"
    )]
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

    #[expect(
        clippy::indexing_slicing,
        reason = "write_product reserves the operand-length sum after the one-limb case; aligned addition reserves its carry limb, and subtraction reserves the larger length, so every inline writer stays within two limbs"
    )]
    pub(in crate::geometry) fn push(&mut self, value: u64) {
        match self {
            Self::Inline { limbs, len } => {
                let index = usize::from(*len);
                limbs[index] = value;
                *len = (*len).wrapping_add(1);
            },
            Self::Heap(limbs) => limbs.push(value),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "checked_sub selects the preceding limb of the private 0..=2 inline length, so the index is within the two-limb array"
    )]
    pub(in crate::geometry) fn pop(&mut self) -> Option<u64> {
        match self {
            Self::Inline { limbs, len } => {
                let index = usize::from(*len).checked_sub(1)?;
                *len = (*len).wrapping_sub(1);
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
                // `copy_within` returns only when count <= length.
                let new_length = length.wrapping_sub(count);
                limbs.split_at_mut(new_length).1.fill(0);
                // The successful copy and two-limb invariant also bound this cast.
                *len = (*len).wrapping_sub(count as u8);
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
struct Aligned<'a> {
    limbs: &'a [u64],
    word_shift: usize,
    bit_shift: u32,
    len: usize,
}

impl<'a> Aligned<'a> {
    fn new(limbs: &'a [u64], shift: usize) -> Self {
        Self {
            limbs,
            word_shift: shift / 64,
            bit_shift: (shift % 64) as u32,
            len: ExactDyadic::shifted_len(limbs, shift),
        }
    }

    fn word(&self, index: usize) -> u64 {
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
            (
                (1_u64 << 52) | fraction,
                encoded_exponent.wrapping_sub(1023).wrapping_sub(52),
            )
        };
        let mut value = Self {
            negative: bits >> 63 != 0,
            limbs: CompactLimbs::single(significand),
            exponent,
        };
        value
            .try_trim()
            .expect("finite binary64 values have representable exponents");
        value
    }

    pub(in crate::geometry) fn try_trim(&mut self) -> Result<()> {
        while self.limbs.last() == Some(&0) {
            self.limbs.pop();
        }
        if self.limbs.is_empty() {
            self.limbs = CompactLimbs::zero();
            self.negative = false;
            self.exponent = 0;
            return Ok(());
        }
        let zero_words = self.limbs.iter().take_while(|&&limb| limb == 0).count();
        let zero_bits = self
            .limbs
            .get(zero_words)
            .expect("a nonzero limb survives trailing trim")
            .trailing_zeros();
        if zero_words == 0 && zero_bits == 0 {
            self.limbs.shrink_inline();
            return Ok(());
        }
        let exponent = i128::from(self.exponent)
            .wrapping_add((zero_words as i128).wrapping_mul(64))
            .wrapping_add(i128::from(zero_bits));
        self.exponent = i64::try_from(exponent).map_err(|_| ResourceError::SizeOverflow)?;
        if zero_words != 0 {
            self.limbs.remove_prefix(zero_words);
        }
        if zero_bits != 0 {
            let mut carry = 0_u64;
            for limb in self.limbs.iter_mut().rev() {
                let next = *limb << 64_u32.wrapping_sub(zero_bits);
                *limb = (*limb >> zero_bits) | carry;
                carry = next;
            }
            while self.limbs.last() == Some(&0) {
                self.limbs.pop();
            }
        }
        debug_assert_eq!(
            *self
                .limbs
                .first()
                .expect("a trimmed nonzero dyadic has a first limb")
                & 1,
            1
        );
        self.limbs.shrink_inline();
        Ok(())
    }

    pub(in crate::geometry) const fn is_zero(&self) -> bool {
        self.limbs.is_empty()
    }

    pub(in crate::geometry) fn is_one(&self) -> bool {
        !self.negative && self.exponent == 0 && self.limbs.as_slice() == [1]
    }

    pub(in crate::geometry) fn is_two(&self) -> bool {
        !self.negative && self.exponent == 1 && self.limbs.as_slice() == [1]
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
    fn shifted_word(limbs: &[u64], word_shift: usize, bit_shift: u32, index: usize) -> u64 {
        if index < word_shift {
            return 0;
        }
        let position = index.wrapping_sub(word_shift);
        let low = limbs.get(position).copied().unwrap_or(0);
        if bit_shift == 0 {
            return low;
        }
        let carry = position
            .checked_sub(1)
            .and_then(|previous| limbs.get(previous))
            .map_or(0, |limb| limb >> 64_u32.wrapping_sub(bit_shift));
        (low << bit_shift) | carry
    }

    fn shifted_len(limbs: &[u64], shift: usize) -> usize {
        if limbs.is_empty() {
            return 0;
        }
        // A Vec<u64> has at most isize::MAX / 8 limbs and shift is a usize;
        // this aligned span stays below 2^61 on supported 64-bit targets,
        // leaving room for a carry limb as well.
        limbs
            .len()
            .wrapping_add(shift / 64)
            .wrapping_add(usize::from(!shift.is_multiple_of(64)))
    }

    fn write_sum_aligned(left: Aligned<'_>, right: Aligned<'_>, out: &mut CompactLimbs) {
        debug_assert!(out.is_empty());
        let length = left.len.max(right.len);
        // Two u64 words and a one-bit carry always fit exactly in u128.
        let mut carry = 0_u128;
        for index in 0..length {
            let sum = u128::from(left.word(index))
                .wrapping_add(u128::from(right.word(index)))
                .wrapping_add(carry);
            out.push(sum as u64);
            carry = sum >> 64;
        }
        if carry != 0 {
            out.push(carry as u64);
        }
    }

    fn write_diff_aligned(larger: Aligned<'_>, smaller: Aligned<'_>, out: &mut CompactLimbs) {
        debug_assert!(out.is_empty());
        // Two u64 words and a one-bit borrow stay within the i128 range.
        let mut borrow = 0_i128;
        for index in 0..larger.len {
            let difference = i128::from(larger.word(index))
                .wrapping_sub(i128::from(smaller.word(index)))
                .wrapping_sub(borrow);
            if difference < 0 {
                out.push(difference.wrapping_add(1_i128 << 64) as u64);
                borrow = 1;
            } else {
                out.push(difference as u64);
                borrow = 0;
            }
        }
        debug_assert_eq!(borrow, 0);
    }

    fn add_aligned(left: Aligned<'_>, right: Aligned<'_>) -> CompactLimbs {
        let length = left.len.max(right.len);
        if length <= INLINE_DYADIC_LIMBS {
            // The extra limb is needed only when the final word carries. The
            // previous unconditional `length + 1` reservation made a
            // no-carry two-limb result spill even though it fit inline.
            let mut words = [0_u64; INLINE_DYADIC_LIMBS + 1];
            let mut carry = 0_u128;
            for (index, word) in words.iter_mut().take(length).enumerate() {
                let sum = u128::from(left.word(index))
                    .wrapping_add(u128::from(right.word(index)))
                    .wrapping_add(carry);
                *word = sum as u64;
                carry = sum >> 64;
            }
            if carry != 0 {
                let (_, suffix) = words.split_at_mut(length);
                *suffix.first_mut().expect("the inline sum has a carry limb") = carry as u64;
            }
            let result_length = length.wrapping_add(usize::from(carry != 0));
            let (result_words, _) = words.split_at(result_length);
            let mut result = CompactLimbs::with_capacity(result_length);
            for &word in result_words {
                result.push(word);
            }
            return result;
        }
        // `Aligned::len` is bounded by the vector and exponent ranges in
        // `shifted_len`, so the carry limb count fits usize.
        let capacity = length.wrapping_add(1);
        let mut result = CompactLimbs::with_capacity(capacity);
        Self::write_sum_aligned(left, right, &mut result);
        result
    }

    fn subtract_aligned(larger: Aligned<'_>, smaller: Aligned<'_>) -> CompactLimbs {
        let mut result = CompactLimbs::with_capacity(larger.len);
        Self::write_diff_aligned(larger, smaller, &mut result);
        result
    }

    fn compare_aligned(left: Aligned<'_>, right: Aligned<'_>) -> Ordering {
        for index in (0..left.len.max(right.len)).rev() {
            let order = left.word(index).cmp(&right.word(index));
            match order {
                Ordering::Equal => {},
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
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

    fn from_odd_u128(negative: bool, magnitude: u128, exponent: i64) -> Self {
        debug_assert!(magnitude & 1 != 0);
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
            exponent,
        }
    }

    /// A normalized value from a `u128` magnitude: trailing zero bits move
    /// into the exponent, leaving the odd lowest limb every value carries.
    pub(in crate::geometry) fn try_from_u128(
        negative: bool,
        magnitude: u128,
        exponent: i64,
    ) -> Result<Self> {
        if magnitude == 0 {
            return Ok(Self::zero());
        }
        let zeros = magnitude.trailing_zeros();
        let exponent = exponent
            .checked_add(i64::from(zeros))
            .ok_or(ResourceError::SizeOverflow)?;
        Ok(Self::from_odd_u128(negative, magnitude >> zeros, exponent))
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
        let left_shift = left.exponent.abs_diff(exponent) as usize;
        let right_shift = right.exponent.abs_diff(exponent) as usize;
        // One limb each, aligned within 63 bits: both magnitudes stay below
        // 2^127, so their sum or difference is exact in u128.
        if let ([left_limb], [right_limb]) = (left.limbs.as_slice(), right.limbs.as_slice())
            && left_shift < 64
            && right_shift < 64
        {
            let left_value = u128::from(*left_limb) << left_shift;
            let right_value = u128::from(*right_limb) << right_shift;
            return if left_negative == right_negative {
                Self::try_from_u128(
                    left_negative,
                    left_value.wrapping_add(right_value),
                    exponent,
                )
            } else if left_value >= right_value {
                Self::try_from_u128(
                    left_negative,
                    left_value.wrapping_sub(right_value),
                    exponent,
                )
            } else {
                Self::try_from_u128(
                    right_negative,
                    right_value.wrapping_sub(left_value),
                    exponent,
                )
            };
        }
        let left = Aligned::new(&left.limbs, left_shift);
        let right = Aligned::new(&right.limbs, right_shift);
        let (negative, limbs) = if left_negative == right_negative {
            (left_negative, Self::add_aligned(left, right))
        } else {
            match Self::compare_aligned(left, right) {
                Ordering::Greater => (left_negative, Self::subtract_aligned(left, right)),
                Ordering::Less => (right_negative, Self::subtract_aligned(right, left)),
                Ordering::Equal => return Ok(Self::zero()),
            }
        };
        let mut result = Self {
            negative,
            limbs,
            exponent,
        };
        result.try_trim()?;
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
    pub(in crate::geometry) fn cmp_magnitude_scaled(&self, other: &Self, shift: i64) -> Ordering {
        self.cmp_magnitude_at(
            other,
            i128::from(other.exponent).wrapping_add(i128::from(shift)),
        )
    }

    fn cmp_magnitude_at(&self, other: &Self, other_exponent: i128) -> Ordering {
        if self.is_zero() {
            return if other.is_zero() {
                Ordering::Equal
            } else {
                Ordering::Less
            };
        }
        if other.is_zero() {
            return Ordering::Greater;
        }
        let left_exponent = i128::from(self.exponent);
        if left_exponent == other_exponent {
            return Self::compare_unsigned(&self.limbs, &other.limbs);
        }
        let top_bit = |limbs: &CompactLimbs, exponent: i128| {
            let high = limbs.last().expect("nonzero dyadic has a highest limb");
            exponent
                .wrapping_add((limbs.len().wrapping_sub(1) as i128).wrapping_mul(64))
                .wrapping_add(i128::from(64_u32.wrapping_sub(high.leading_zeros())))
        };
        let top = top_bit(&self.limbs, left_exponent).cmp(&top_bit(&other.limbs, other_exponent));
        if top != Ordering::Equal {
            return top;
        }
        let exponent = left_exponent.min(other_exponent);
        let left_shift = usize::try_from(left_exponent.abs_diff(exponent))
            .expect("equal top bits require a representable limb span");
        let right_shift = usize::try_from(other_exponent.abs_diff(exponent))
            .expect("equal top bits require a representable limb span");
        Self::compare_aligned(
            Aligned::new(&self.limbs, left_shift),
            Aligned::new(&other.limbs, right_shift),
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "try_product rejects zero operands, so canonical short limbs are nonempty; schoolbook row r touches offsets below r + long.len(), while prior rows publish that prefix and the operand-length sum is reserved"
    )]
    fn write_product(short: &[u64], long: &[u64], out: &mut CompactLimbs) {
        debug_assert!(out.is_empty());
        let first = short[0];
        // A u64 product plus an existing limb and a one-limb carry is at most
        // 2^128 - 1, so these low-word operations preserve the full sum.
        let mut carry = 0_u128;
        for &right in long {
            let value = u128::from(first)
                .wrapping_mul(u128::from(right))
                .wrapping_add(carry);
            out.push(value as u64);
            carry = value >> 64;
        }
        out.push(carry as u64);
        for (left_index, &left) in short.iter().enumerate().skip(1) {
            let mut carry = 0_u128;
            for (right_index, &right) in long.iter().enumerate() {
                let index = left_index.wrapping_add(right_index);
                let value = u128::from(out[index])
                    .wrapping_add(u128::from(left).wrapping_mul(u128::from(right)))
                    .wrapping_add(carry);
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
        let exponent = self
            .exponent
            .checked_add(other.exponent)
            .ok_or(ResourceError::SizeOverflow)?;
        if let ([left], [right]) = (self.limbs.as_slice(), other.limbs.as_slice()) {
            return Self::try_from_u128(
                self.negative != other.negative,
                u128::from(*left).wrapping_mul(u128::from(*right)),
                exponent,
            );
        }
        let (short, long) = if self.limbs.len() <= other.limbs.len() {
            (&self.limbs, &other.limbs)
        } else {
            (&other.limbs, &self.limbs)
        };
        // Each heap limb vector is bounded by its allocation layout, so the
        // combined limb-count upper bound fits usize on supported 64-bit targets.
        let needed = short.len().wrapping_add(long.len());
        let mut limbs = CompactLimbs::with_capacity(needed);
        Self::write_product(short, long, &mut limbs);
        let mut result = Self {
            negative: self.negative != other.negative,
            limbs,
            exponent,
        };
        result.try_trim()?;
        Ok(result)
    }

    pub(in crate::geometry) fn compare_unsigned(left: &[u64], right: &[u64]) -> Ordering {
        let left_len = left.len();
        let right_len = right.len();
        match left_len.cmp(&right_len) {
            Ordering::Equal => {},
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
        }
        for (&left_word, &right_word) in left.iter().rev().zip(right.iter().rev()) {
            match left_word.cmp(&right_word) {
                Ordering::Equal => {},
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
            }
        }
        Ordering::Equal
    }

    /// Quotient and remainder of trimmed magnitudes, one 64-bit digit at a
    /// time (Knuth, Algorithm D): the divisor is normalized so its top bit is
    /// set, which keeps every estimated digit at most two above the true one.
    /// Callers pass a nonzero trimmed divisor; the one-limb case and normalized
    /// high word therefore both divide by nonzero values.
    #[expect(
        clippy::indexing_slicing,
        reason = "for a nonzero divisor, n >= 2 after the one-limb case and m >= n after the magnitude comparison; shifted u has m + 1 limbs, v has n, and j <= m - n with i < n bounds every offset"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the one-limb divisor is nonzero after the top-limb invariant, and Algorithm D's normalized divisor top is nonzero"
    )]
    pub(in crate::geometry) fn div_rem_unsigned(
        dividend: &[u64],
        divisor: &[u64],
    ) -> (Vec<u64>, Vec<u64>) {
        debug_assert!(divisor.last().is_some_and(|&high| high != 0));
        if Self::compare_unsigned(dividend, divisor) == Ordering::Less {
            return (Vec::new(), dividend.to_vec());
        }
        let trim = |limbs: &mut Vec<u64>| {
            while limbs.last() == Some(&0) {
                limbs.pop();
            }
        };
        if let [digit] = *divisor {
            let digit = u128::from(digit);
            let mut quotient = dividend.to_vec();
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
                vec![remainder as u64]
            };
            return (quotient, remainder);
        }
        let n = divisor.len();
        let m = dividend.len();
        let shift = divisor[n.wrapping_sub(1)].leading_zeros();
        let shifted = |limbs: &[u64], extra: usize| -> Vec<u64> {
            let mut out = Vec::with_capacity(limbs.len().wrapping_add(extra));
            let mut carry = 0_u64;
            for &limb in limbs {
                out.push((limb << shift) | carry);
                carry = if shift == 0 {
                    0
                } else {
                    limb >> 64_u32.wrapping_sub(shift)
                };
            }
            if extra > 0 {
                out.push(carry);
            }
            out
        };
        let v = shifted(divisor, 0);
        let mut u = shifted(dividend, 1);
        let final_digit = m.wrapping_sub(n);
        let mut quotient = vec![0; final_digit.wrapping_add(1)];
        let (top, next) = (
            u128::from(v[n.wrapping_sub(1)]),
            u128::from(v[n.wrapping_sub(2)]),
        );
        for j in (0..=final_digit).rev() {
            let numerator = (u128::from(u[j.wrapping_add(n)]) << 64)
                | u128::from(u[j.wrapping_add(n).wrapping_sub(1)]);
            let mut estimate = numerator / top;
            let mut rest = numerator % top;
            // Algorithm D bounds q̂ by 2^64 + 1, so q̂ times one divisor
            // digit fits u128. A failed comparison cannot decrement zero;
            // rest + top fits u128 and the loop breaks before rest reaches 2^64.
            while estimate > u128::from(u64::MAX)
                || estimate.wrapping_mul(next)
                    > ((rest << 64) | u128::from(u[j.wrapping_add(n).wrapping_sub(2)]))
            {
                estimate = estimate.wrapping_sub(1);
                rest = rest.wrapping_add(top);
                if rest > u128::from(u64::MAX) {
                    break;
                }
            }
            // Multiply and subtract; a negative result means the estimate was
            // one too large, and one addback of the divisor corrects it.
            // A u64-sized estimate times one limb fits u128; one limb and its
            // borrow fit i128 exactly.
            let mut borrow = 0_i128;
            for (limb, &divisor) in u.iter_mut().skip(j).take(n).zip(&v) {
                let product = estimate.wrapping_mul(u128::from(divisor));
                let difference = i128::from(*limb)
                    .wrapping_sub(borrow)
                    .wrapping_sub(i128::from(product as u64));
                *limb = difference as u64;
                borrow = ((product >> 64) as i128).wrapping_sub(difference >> 64);
            }
            let high = j.wrapping_add(n);
            let difference = i128::from(u[high]).wrapping_sub(borrow);
            u[high] = difference as u64;
            quotient[j] = estimate as u64;
            if difference < 0 {
                quotient[j] = quotient[j].wrapping_sub(1);
                let mut carry = 0_u128;
                for (limb, &divisor) in u.iter_mut().skip(j).take(n).zip(&v) {
                    let sum = u128::from(*limb)
                        .wrapping_add(u128::from(divisor))
                        .wrapping_add(carry);
                    *limb = sum as u64;
                    carry = sum >> 64;
                }
                u[high] = u[high].wrapping_add(carry as u64);
            }
        }
        trim(&mut quotient);
        u.truncate(n);
        if shift != 0 {
            for i in 0..n {
                let high = if i.wrapping_add(1) < n {
                    u[i.wrapping_add(1)] << 64_u32.wrapping_sub(shift)
                } else {
                    0
                };
                let limb = u[i];
                u[i] = (limb >> shift) | high;
            }
        }
        trim(&mut u);
        (quotient, u)
    }

    /// Greatest common divisor in the exact dyadic domain.
    ///
    /// `trim` keeps each nonzero limb bank odd, so the common power-of-two
    /// factor is the smaller exponent and the odd banks can use ordinary
    /// integer Euclid. The result is positive and exact.
    pub(in crate::geometry) fn gcd(&self, other: &Self) -> Self {
        if self.is_zero() {
            return other.clone().abs();
        }
        if other.is_zero() {
            return self.clone().abs();
        }
        if self.limbs.as_slice() == [1] && self.exponent == 0 {
            return self.clone().abs();
        }
        if other.limbs.as_slice() == [1] && other.exponent == 0 {
            return other.clone().abs();
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
                right = right.wrapping_sub(left);
                if right == 0 {
                    return Self::from_odd_u128(false, left, exponent);
                }
                right >>= right.trailing_zeros();
            }
        }
        let mut left = self.limbs.to_vec();
        let mut right = other.limbs.to_vec();
        while !right.is_empty() {
            let (_, remainder) = Self::div_rem_unsigned(&left, &right);
            left = right;
            right = remainder;
        }
        Self {
            negative: false,
            limbs: CompactLimbs::from_vec(left),
            exponent,
        }
    }

    /// Divide two exact dyadics when the quotient is itself dyadic.
    ///
    /// Rational-area reduction only calls this with a common divisor, so a
    /// nonzero remainder is an internal exactness failure rather than a
    /// rounded result.
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the divisor is rejected as zero first, and a canonical nonzero dyadic has a positive small magnitude"
    )]
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
            return Self::try_from_u128(
                self.negative != divisor.negative,
                dividend / magnitude,
                exponent,
            );
        }
        let (limbs, remainder) = Self::div_rem_unsigned(&self.limbs, &divisor.limbs);
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
        let order = self.cmp_magnitude_at(other, i128::from(other.exponent));
        if self.negative {
            order.reverse()
        } else {
            order
        }
    }

    pub(in crate::geometry) fn cmp_magnitude(&self, other: &Self) -> Ordering {
        self.cmp_magnitude_at(other, i128::from(other.exponent))
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
