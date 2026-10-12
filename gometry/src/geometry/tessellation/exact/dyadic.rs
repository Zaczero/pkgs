//! Exact dyadic rationals: a sign, an exponent and a compact limb buffer.
//!
//! Every value is a finite binary fraction held exactly, so addition,
//! subtraction and multiplication never round. General non-dyadic quotients
//! remain explicit as ratios; exact integer quotients serve discrete decisions.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};

use super::{ExactPoint, ExactSign, GeometryErrorKind, ResourceError, Result, XY};
use crate::geometry::embedding_search::{Unmetered, Work};
#[cfg(all(test, debug_assertions))]
use crate::test_support::exact_operation_counts;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactDyadic {
    pub(in crate::geometry) negative: bool,
    pub(in crate::geometry) limbs: CompactLimbs,
    // Retained exponent range and aligned limb spans are separate bounds.
    // Constructors check exponent arithmetic and alignment sizes; comparison
    // uses i128 top-bit positions and virtual words without materializing gaps.
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

impl Hash for ExactDyadic {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.negative.hash(state);
        self.limbs.as_slice().hash(state);
        self.exponent.hash(state);
    }
}

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

    #[cfg(test)]
    pub(in crate::geometry) fn from_vec(limbs: Vec<u64>) -> Self {
        if limbs.len() <= INLINE_DYADIC_LIMBS {
            Self::inline_from_slice(&limbs)
        } else {
            Self::Heap(limbs)
        }
    }

    fn from_vec_with_work<W: Work>(limbs: Vec<u64>, work: &mut W) -> Result<Self> {
        if limbs.len() <= INLINE_DYADIC_LIMBS {
            work.charge_into_error(1)?;
            Ok(Self::inline_from_slice(&limbs))
        } else {
            Ok(Self::Heap(limbs))
        }
    }

    fn try_clone_with_work<W: Work>(&self, work: &mut W) -> Result<Self> {
        match self {
            Self::Inline { limbs, len } => {
                work.charge_into_error(1)?;
                Ok(Self::Inline {
                    limbs: *limbs,
                    len: *len,
                })
            },
            Self::Heap(limbs) if limbs.len() <= INLINE_DYADIC_LIMBS => {
                work.charge_into_error(1)?;
                Ok(Self::inline_from_slice(limbs))
            },
            Self::Heap(limbs) => {
                work.charge_product_into_error(limbs.len(), 1)?;
                work.charge_product_into_error(limbs.len(), 1)?;
                Ok(Self::Heap(limbs.clone()))
            },
        }
    }

    fn try_shrink_inline_with_work<W: Work>(&mut self, work: &mut W) -> Result<()> {
        if let Self::Heap(limbs) = self
            && limbs.len() <= INLINE_DYADIC_LIMBS
        {
            work.charge_into_error(1)?;
            *self = Self::inline_from_slice(limbs);
        }
        Ok(())
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

    fn try_remove_prefix_with_work<W: Work>(&mut self, count: usize, work: &mut W) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        match self {
            Self::Inline { limbs, len } => {
                let length = usize::from(*len);
                work.charge_product_into_error(length.wrapping_sub(count), 1)?;
                limbs.copy_within(count..length, 0);
                // `copy_within` returns only when count <= length.
                let new_length = length.wrapping_sub(count);
                work.charge_product_into_error(INLINE_DYADIC_LIMBS.wrapping_sub(new_length), 1)?;
                limbs.split_at_mut(new_length).1.fill(0);
                // The successful copy and two-limb invariant also bound this cast.
                *len = (*len).wrapping_sub(count as u8);
            },
            Self::Heap(limbs) => {
                work.charge_product_into_error(limbs.len().wrapping_sub(count), 1)?;
                limbs.drain(..count);
            },
        }
        Ok(())
    }

    fn try_trim_high_with_work<W: Work>(&mut self, work: &mut W) -> Result<()> {
        loop {
            work.charge_into_error(1)?;
            if self.as_slice().last() != Some(&0) {
                return Ok(());
            }
            self.pop();
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
        Self::perpendicular_bisector_dyadics([&ax, &ay], [&bx, &by])
    }

    pub(in crate::geometry) fn perpendicular_bisector_points(
        a: &ExactPoint,
        b: &ExactPoint,
    ) -> Result<Self> {
        let a = a.coords()?;
        let b = b.coords()?;
        let aw_bw = a.w.try_product(&b.w)?;
        let a_squared = a.x.try_product(&a.x)?.try_add(a.y.try_product(&a.y)?)?;
        let b_squared = b.x.try_product(&b.x)?.try_add(b.y.try_product(&b.y)?)?;
        let aw_squared = a.w.try_product(&a.w)?;
        let bw_squared = b.w.try_product(&b.w)?;
        let two = ExactDyadic::from_f64(2.0);
        Ok(Self([
            ExactDyadic::try_subtract_refs(&b.x.try_product(&a.w)?, &a.x.try_product(&b.w)?)?
                .try_product(&two)?
                .try_product(&aw_bw)?,
            ExactDyadic::try_subtract_refs(&b.y.try_product(&a.w)?, &a.y.try_product(&b.w)?)?
                .try_product(&two)?
                .try_product(&aw_bw)?,
            a_squared
                .try_product(&bw_squared)?
                .try_subtract(b_squared.try_product(&aw_squared)?)?,
        ]))
    }

    pub(in crate::geometry) fn perpendicular_bisector_dyadics(
        a: [&ExactDyadic; 2],
        b: [&ExactDyadic; 2],
    ) -> Result<Self> {
        let [ax, ay] = a;
        let [bx, by] = b;
        let two = ExactDyadic::from_f64(2.0);
        Ok(Self([
            ExactDyadic::try_subtract_refs(bx, ax)?.try_product(&two)?,
            ExactDyadic::try_subtract_refs(by, ay)?.try_product(&two)?,
            ax.try_product(ax)?
                .try_add(ay.try_product(ay)?)?
                .try_subtract(bx.try_product(bx)?.try_add(by.try_product(by)?)?)?,
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

/// A virtual left-shifted view of a limb slice. Arithmetic consumers read
/// aligned words directly; integer division materializes a trimmed magnitude.
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

    fn trimmed_words_with_work<W: Work>(self, work: &mut W) -> Result<Vec<u64>> {
        work.charge_product_into_error(self.len, 1)?;
        work.charge_product_into_error(self.len, 1)?;
        let mut words = Vec::with_capacity(self.len);
        for index in 0..self.len {
            words.push(self.word(index));
        }
        loop {
            work.charge_into_error(1)?;
            if words.pop_if(|word| *word == 0).is_none() {
                break;
            }
        }
        Ok(words)
    }
}

impl ExactDyadic {
    pub(in crate::geometry) fn try_clone_with_work<W: Work>(&self, work: &mut W) -> Result<Self> {
        Ok(Self {
            negative: self.negative,
            limbs: self.limbs.try_clone_with_work(work)?,
            exponent: self.exponent,
        })
    }

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

    pub(in crate::geometry) fn from_i64(value: i64) -> Self {
        let magnitude = value.unsigned_abs();
        if magnitude == 0 {
            return Self::zero();
        }
        let trailing = magnitude.trailing_zeros();
        Self {
            negative: value.is_negative(),
            limbs: CompactLimbs::single(magnitude >> trailing),
            exponent: i64::from(trailing),
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
        #[cfg(all(test, debug_assertions))]
        exact_operation_counts::conversion();
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
        let trailing = significand.trailing_zeros();
        Self {
            negative: bits >> 63 != 0,
            limbs: CompactLimbs::single(significand >> trailing),
            exponent: exponent.wrapping_add(i64::from(trailing)),
        }
    }

    /// Recover a binary64 only when the scaled dyadic is exactly representable.
    pub(in crate::geometry) fn to_f64_exact_scaled(&self, scale: i64) -> Option<f64> {
        let magnitude = match self.limbs.as_slice() {
            [] => return Some(0.0),
            [magnitude] => *magnitude,
            _ => return None,
        };
        let width = 64_u32.wrapping_sub(magnitude.leading_zeros());
        if width > 53 {
            return None;
        }
        let exponent = self.exponent.checked_add(scale)?;
        let top = exponent.checked_add(i64::from(width).wrapping_sub(1))?;
        if exponent < -1074 || top > 1023 {
            return None;
        }
        let sign = u64::from(self.negative) << 63;
        let bits = if top >= -1022 {
            let significand = magnitude << 53_u32.wrapping_sub(width);
            (u64::try_from(top.wrapping_add(1023)).ok()? << 52)
                | (significand & ((1_u64 << 52) - 1))
        } else {
            magnitude << u32::try_from(exponent.wrapping_add(1074)).ok()?
        };
        Some(f64::from_bits(sign | bits))
    }

    pub(in crate::geometry) fn try_trim(&mut self) -> Result<()> {
        self.try_trim_with_work(&mut Unmetered)
    }

    pub(in crate::geometry) fn try_trim_with_work<W: Work>(&mut self, work: &mut W) -> Result<()> {
        self.limbs.try_trim_high_with_work(work)?;
        work.charge_into_error(1)?;
        if self.limbs.is_empty() {
            self.limbs = CompactLimbs::zero();
            self.negative = false;
            self.exponent = 0;
            return Ok(());
        }
        let mut zero_words: usize = 0;
        for &limb in self.limbs.iter() {
            work.charge_into_error(1)?;
            if limb != 0 {
                break;
            }
            zero_words = zero_words.wrapping_add(1);
        }
        let zero_bits = self
            .limbs
            .get(zero_words)
            .expect("a nonzero limb survives trailing trim")
            .trailing_zeros();
        if zero_words == 0 && zero_bits == 0 {
            self.limbs.try_shrink_inline_with_work(work)?;
            return Ok(());
        }
        let exponent = i128::from(self.exponent)
            .wrapping_add((zero_words as i128).wrapping_mul(64))
            .wrapping_add(i128::from(zero_bits));
        self.exponent = i64::try_from(exponent).map_err(|_| ResourceError::SizeOverflow)?;
        if zero_words != 0 {
            self.limbs.try_remove_prefix_with_work(zero_words, work)?;
        }
        if zero_bits != 0 {
            work.charge_product_into_error(self.limbs.len(), 1)?;
            let mut carry = 0_u64;
            for limb in self.limbs.iter_mut().rev() {
                let next = *limb << 64_u32.wrapping_sub(zero_bits);
                *limb = (*limb >> zero_bits) | carry;
                carry = next;
            }
            self.limbs.try_trim_high_with_work(work)?;
        }
        debug_assert_eq!(
            *self
                .limbs
                .first()
                .expect("a trimmed nonzero dyadic has a first limb")
                & 1,
            1
        );
        self.limbs.try_shrink_inline_with_work(work)?;
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

    fn write_sum_aligned<W: Work>(
        left: Aligned<'_>,
        right: Aligned<'_>,
        out: &mut CompactLimbs,
        work: &mut W,
    ) -> Result<()> {
        debug_assert!(out.is_empty());
        let length = left.len.max(right.len);
        work.charge_product_into_error(length, 1)?;
        work.charge_into_error(1)?;
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
        Ok(())
    }

    fn write_diff_aligned<W: Work>(
        larger: Aligned<'_>,
        smaller: Aligned<'_>,
        out: &mut CompactLimbs,
        work: &mut W,
    ) -> Result<()> {
        debug_assert!(out.is_empty());
        work.charge_product_into_error(larger.len, 1)?;
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
        Ok(())
    }

    fn add_aligned_with_work<W: Work>(
        left: Aligned<'_>,
        right: Aligned<'_>,
        work: &mut W,
    ) -> Result<CompactLimbs> {
        let length = left.len.max(right.len);
        if length <= INLINE_DYADIC_LIMBS {
            // The extra limb is needed only when the final word carries. The
            // previous unconditional `length + 1` reservation made a
            // no-carry two-limb result spill even though it fit inline.
            work.charge_into_error(1)?;
            let mut words = [0_u64; INLINE_DYADIC_LIMBS + 1];
            work.charge_product_into_error(length, 1)?;
            let mut carry = 0_u128;
            for (index, word) in words.iter_mut().take(length).enumerate() {
                let sum = u128::from(left.word(index))
                    .wrapping_add(u128::from(right.word(index)))
                    .wrapping_add(carry);
                *word = sum as u64;
                carry = sum >> 64;
            }
            if carry != 0 {
                work.charge_into_error(1)?;
                let (_, suffix) = words.split_at_mut(length);
                *suffix.first_mut().expect("the inline sum has a carry limb") = carry as u64;
            }
            let result_length = length.wrapping_add(usize::from(carry != 0));
            let (result_words, _) = words.split_at(result_length);
            if result_length > INLINE_DYADIC_LIMBS {
                work.charge_product_into_error(result_length, 1)?;
            }
            work.charge_product_into_error(result_length, 1)?;
            let mut result = CompactLimbs::with_capacity(result_length);
            for &word in result_words {
                result.push(word);
            }
            return Ok(result);
        }
        // `Aligned::len` is bounded by the vector and exponent ranges in
        // `shifted_len`, so the carry limb count fits usize.
        let capacity = length.wrapping_add(1);
        work.charge_product_into_error(capacity, 1)?;
        let mut result = CompactLimbs::with_capacity(capacity);
        Self::write_sum_aligned(left, right, &mut result, work)?;
        Ok(result)
    }

    fn subtract_aligned_with_work<W: Work>(
        larger: Aligned<'_>,
        smaller: Aligned<'_>,
        work: &mut W,
    ) -> Result<CompactLimbs> {
        work.charge_product_into_error(larger.len, 1)?;
        let mut result = CompactLimbs::with_capacity(larger.len);
        Self::write_diff_aligned(larger, smaller, &mut result, work)?;
        Ok(result)
    }

    fn compare_aligned_with_work<W: Work>(
        left: Aligned<'_>,
        right: Aligned<'_>,
        work: &mut W,
    ) -> Result<Ordering> {
        for index in (0..left.len.max(right.len)).rev() {
            work.charge_into_error(1)?;
            let order = left.word(index).cmp(&right.word(index));
            match order {
                Ordering::Equal => {},
                Ordering::Less => return Ok(Ordering::Less),
                Ordering::Greater => return Ok(Ordering::Greater),
            }
        }
        Ok(Ordering::Equal)
    }

    pub(in crate::geometry) fn try_add(self, other: Self) -> Result<Self> {
        Self::try_add_with_work(self, other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_add_with_work<W: Work>(
        self,
        other: Self,
        work: &mut W,
    ) -> Result<Self> {
        if self.is_zero() {
            return Ok(other);
        }
        if other.is_zero() {
            return Ok(self);
        }
        Self::try_combine_refs_with_work(&self, &other, false, work)
    }

    pub(in crate::geometry) fn try_add_refs(left: &Self, right: &Self) -> Result<Self> {
        Self::try_add_refs_with_work(left, right, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_add_refs_with_work<W: Work>(
        left: &Self,
        right: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if left.is_zero() {
            return right.try_clone_with_work(work);
        }
        if right.is_zero() {
            return left.try_clone_with_work(work);
        }
        Self::try_combine_refs_with_work(left, right, false, work)
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

    pub(in crate::geometry) fn try_combine_refs_with_work<W: Work>(
        left: &Self,
        right: &Self,
        negate_right: bool,
        work: &mut W,
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
            work.charge_into_error(1)?;
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
            (
                left_negative,
                Self::add_aligned_with_work(left, right, work)?,
            )
        } else {
            match Self::compare_aligned_with_work(left, right, work)? {
                Ordering::Greater => (
                    left_negative,
                    Self::subtract_aligned_with_work(left, right, work)?,
                ),
                Ordering::Less => (
                    right_negative,
                    Self::subtract_aligned_with_work(right, left, work)?,
                ),
                Ordering::Equal => return Ok(Self::zero()),
            }
        };
        let mut result = Self {
            negative,
            limbs,
            exponent,
        };
        result.try_trim_with_work(work)?;
        Ok(result)
    }

    pub(in crate::geometry) fn try_subtract_ref(&self, other: &Self) -> Result<Self> {
        self.try_subtract_ref_with_work(other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_subtract_ref_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Self> {
        Self::try_subtract_refs_with_work(self, other, work)
    }

    pub(in crate::geometry) fn try_subtract_refs(left: &Self, right: &Self) -> Result<Self> {
        Self::try_subtract_refs_with_work(left, right, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_subtract_refs_with_work<W: Work>(
        left: &Self,
        right: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if left.is_zero() {
            let mut result = right.try_clone_with_work(work)?;
            if !result.is_zero() {
                result.negative = !result.negative;
            }
            return Ok(result);
        }
        if right.is_zero() {
            return left.try_clone_with_work(work);
        }
        Self::try_combine_refs_with_work(left, right, true, work)
    }

    pub(in crate::geometry) fn try_subtract(self, other: Self) -> Result<Self> {
        Self::try_subtract_with_work(self, other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_subtract_with_work<W: Work>(
        self,
        other: Self,
        work: &mut W,
    ) -> Result<Self> {
        if self.is_zero() {
            return Ok(other.neg());
        }
        if other.is_zero() {
            return Ok(self);
        }
        Self::try_combine_refs_with_work(&self, &other, true, work)
    }

    /// Compare `self` with `other * 2^shift` by magnitude without constructing
    /// the scalar product. The ratio-rounding residual uses this for the exact
    /// power-of-two gap between adjacent binary64 values.
    #[cfg(test)]
    pub(in crate::geometry) fn cmp_magnitude_scaled(&self, other: &Self, shift: i64) -> Ordering {
        self.cmp_magnitude_scaled_with_work(other, shift, &mut Unmetered)
            .expect("unmetered dyadic comparison cannot fail")
    }

    pub(in crate::geometry) fn cmp_magnitude_scaled_with_work<W: Work>(
        &self,
        other: &Self,
        shift: i64,
        work: &mut W,
    ) -> Result<Ordering> {
        self.cmp_magnitude_at_with_work(
            other,
            i128::from(other.exponent).wrapping_add(i128::from(shift)),
            work,
        )
    }

    fn cmp_magnitude_at_with_work<W: Work>(
        &self,
        other: &Self,
        other_exponent: i128,
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        if self.is_zero() {
            return Ok(if other.is_zero() {
                Ordering::Equal
            } else {
                Ordering::Less
            });
        }
        if other.is_zero() {
            return Ok(Ordering::Greater);
        }
        let left_exponent = i128::from(self.exponent);
        if left_exponent == other_exponent {
            return Self::compare_unsigned_with_work(&self.limbs, &other.limbs, work);
        }
        let top_bit = |limbs: &CompactLimbs, exponent: i128| {
            let high = limbs.last().expect("nonzero dyadic has a highest limb");
            exponent
                .wrapping_add((limbs.len().wrapping_sub(1) as i128).wrapping_mul(64))
                .wrapping_add(i128::from(64_u32.wrapping_sub(high.leading_zeros())))
        };
        let top = top_bit(&self.limbs, left_exponent).cmp(&top_bit(&other.limbs, other_exponent));
        if top != Ordering::Equal {
            return Ok(top);
        }
        let exponent = left_exponent.min(other_exponent);
        let left_shift = usize::try_from(left_exponent.abs_diff(exponent))
            .expect("equal top bits require a representable limb span");
        let right_shift = usize::try_from(other_exponent.abs_diff(exponent))
            .expect("equal top bits require a representable limb span");
        Self::compare_aligned_with_work(
            Aligned::new(&self.limbs, left_shift),
            Aligned::new(&other.limbs, right_shift),
            work,
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "try_product rejects zero operands, so canonical short limbs are nonempty; schoolbook row r touches offsets below r + long.len(), while prior rows publish that prefix and the operand-length sum is reserved"
    )]
    fn write_product<W: Work>(
        short: &[u64],
        long: &[u64],
        out: &mut CompactLimbs,
        work: &mut W,
    ) -> Result<()> {
        debug_assert!(out.is_empty());
        work.charge_product_into_error(short.len(), long.len())?;
        work.charge_product_into_error(short.len(), 1)?;
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
        Ok(())
    }

    pub(in crate::geometry) fn try_product(&self, other: &Self) -> Result<Self> {
        self.try_product_with_work(other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_product_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if self.is_zero() || other.is_zero() {
            return Ok(Self::zero());
        }
        let exponent = self
            .exponent
            .checked_add(other.exponent)
            .ok_or(ResourceError::SizeOverflow)?;
        if let ([left], [right]) = (self.limbs.as_slice(), other.limbs.as_slice()) {
            work.charge_into_error(1)?;
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
        work.charge_product_into_error(needed, 1)?;
        let mut limbs = CompactLimbs::with_capacity(needed);
        Self::write_product(short, long, &mut limbs, work)?;
        let mut result = Self {
            negative: self.negative != other.negative,
            limbs,
            exponent,
        };
        result.try_trim_with_work(work)?;
        Ok(result)
    }

    #[cfg(test)]
    pub(in crate::geometry) fn compare_unsigned(left: &[u64], right: &[u64]) -> Ordering {
        Self::compare_unsigned_with_work(left, right, &mut Unmetered)
            .expect("unmetered unsigned comparison cannot fail")
    }

    pub(in crate::geometry) fn compare_unsigned_with_work<W: Work>(
        left: &[u64],
        right: &[u64],
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        let left_len = left.len();
        let right_len = right.len();
        match left_len.cmp(&right_len) {
            Ordering::Equal => {},
            Ordering::Less => return Ok(Ordering::Less),
            Ordering::Greater => return Ok(Ordering::Greater),
        }
        for (&left_word, &right_word) in left.iter().rev().zip(right.iter().rev()) {
            work.charge_into_error(1)?;
            match left_word.cmp(&right_word) {
                Ordering::Equal => {},
                Ordering::Less => return Ok(Ordering::Less),
                Ordering::Greater => return Ok(Ordering::Greater),
            }
        }
        Ok(Ordering::Equal)
    }

    fn try_clone_words_with_work<W: Work>(words: &[u64], work: &mut W) -> Result<Vec<u64>> {
        work.charge_product_into_error(words.len(), 1)?;
        work.charge_product_into_error(words.len(), 1)?;
        Ok(words.to_vec())
    }

    fn try_trim_words_with_work<W: Work>(words: &mut Vec<u64>, work: &mut W) -> Result<()> {
        loop {
            work.charge_into_error(1)?;
            if words.last() != Some(&0) {
                return Ok(());
            }
            words.pop();
        }
    }

    /// Quotient and remainder of trimmed magnitudes, one 64-bit digit at a
    /// time (Knuth, Algorithm D): the divisor is normalized so its top bit is
    /// set, which keeps every estimated digit at most two above the true one.
    /// Callers pass a nonzero trimmed divisor; the one-limb case and normalized
    /// high word therefore both divide by nonzero values.
    #[cfg(test)]
    pub(in crate::geometry) fn div_rem_unsigned(
        dividend: &[u64],
        divisor: &[u64],
    ) -> (Vec<u64>, Vec<u64>) {
        Self::div_rem_unsigned_with_work(dividend, divisor, &mut Unmetered)
            .expect("unmetered unsigned division cannot fail")
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "for a nonzero divisor, n >= 2 after the one-limb case and m >= n after the magnitude comparison; shifted u has m + 1 limbs, v has n, and j <= m - n with i < n bounds every offset"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the one-limb divisor is nonzero after the top-limb invariant, and Algorithm D's normalized divisor top is nonzero"
    )]
    pub(in crate::geometry) fn div_rem_unsigned_with_work<W: Work>(
        dividend: &[u64],
        divisor: &[u64],
        work: &mut W,
    ) -> Result<(Vec<u64>, Vec<u64>)> {
        debug_assert!(divisor.last().is_some_and(|&high| high != 0));
        if Self::compare_unsigned_with_work(dividend, divisor, work)? == Ordering::Less {
            return Ok((Vec::new(), Self::try_clone_words_with_work(dividend, work)?));
        }
        if let [digit] = *divisor {
            let digit = u128::from(digit);
            let mut quotient = Self::try_clone_words_with_work(dividend, work)?;
            let mut remainder = 0_u128;
            for limb in quotient.iter_mut().rev() {
                work.charge_into_error(1)?;
                let value = (remainder << 64) | u128::from(*limb);
                *limb = (value / digit) as u64;
                remainder = value % digit;
            }
            Self::try_trim_words_with_work(&mut quotient, work)?;
            work.charge_into_error(1)?;
            let remainder = if remainder == 0 {
                Vec::new()
            } else {
                work.charge_product_into_error(1, 1)?;
                work.charge_product_into_error(1, 1)?;
                vec![remainder as u64]
            };
            return Ok((quotient, remainder));
        }
        let n = divisor.len();
        let m = dividend.len();
        let shift = divisor[n.wrapping_sub(1)].leading_zeros();
        let shifted = |limbs: &[u64], extra: usize, work: &mut W| -> Result<Vec<u64>> {
            let capacity = limbs.len().wrapping_add(extra);
            work.charge_product_into_error(capacity, 1)?;
            work.charge_product_into_error(limbs.len(), 1)?;
            if extra != 0 {
                work.charge_into_error(1)?;
            }
            let mut out = Vec::with_capacity(capacity);
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
            Ok(out)
        };
        let v = shifted(divisor, 0, work)?;
        let mut u = shifted(dividend, 1, work)?;
        let final_digit = m.wrapping_sub(n);
        let quotient_length = final_digit.wrapping_add(1);
        work.charge_product_into_error(quotient_length, 1)?;
        work.charge_product_into_error(quotient_length, 1)?;
        let mut quotient = vec![0; quotient_length];
        let (top, next) = (
            u128::from(v[n.wrapping_sub(1)]),
            u128::from(v[n.wrapping_sub(2)]),
        );
        for j in (0..=final_digit).rev() {
            work.charge_into_error(1)?;
            work.charge_product_into_error(n, 1)?;
            work.charge_into_error(2)?;
            let numerator = (u128::from(u[j.wrapping_add(n)]) << 64)
                | u128::from(u[j.wrapping_add(n).wrapping_sub(1)]);
            let mut estimate = numerator / top;
            let mut rest = numerator % top;
            // Algorithm D bounds q̂ by 2^64 + 1, so q̂ times one divisor
            // digit fits u128. A failed comparison cannot decrement zero;
            // rest + top fits u128 and the loop breaks before rest reaches 2^64.
            loop {
                work.charge_into_error(1)?;
                let too_large = estimate > u128::from(u64::MAX)
                    || estimate.wrapping_mul(next)
                        > ((rest << 64) | u128::from(u[j.wrapping_add(n).wrapping_sub(2)]));
                if !too_large {
                    break;
                }
                work.charge_into_error(1)?;
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
                work.charge_product_into_error(n, 1)?;
                work.charge_into_error(2)?;
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
        Self::try_trim_words_with_work(&mut quotient, work)?;
        work.charge_into_error(1)?;
        u.truncate(n);
        if shift != 0 {
            work.charge_product_into_error(n, 1)?;
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
        Self::try_trim_words_with_work(&mut u, work)?;
        Ok((quotient, u))
    }

    /// Nearest integer quotient of two exact dyadics, with halfway cases toward zero.
    pub(in crate::geometry) fn nearest_integer_quotient_ties_to_zero(
        &self,
        divisor: &Self,
    ) -> Result<Self> {
        self.nearest_integer_quotient_ties_to_zero_with_work(divisor, &mut Unmetered)
    }

    pub(in crate::geometry) fn nearest_integer_quotient_ties_to_zero_with_work<W: Work>(
        &self,
        divisor: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if divisor.is_zero() {
            return Err(GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        if self.is_zero() {
            return Ok(Self::zero());
        }
        let exponent = self.exponent.min(divisor.exponent);
        let numerator_shift = usize::try_from(self.exponent.abs_diff(exponent))
            .map_err(|_| ResourceError::SizeOverflow)?;
        let divisor_shift = usize::try_from(divisor.exponent.abs_diff(exponent))
            .map_err(|_| ResourceError::SizeOverflow)?;
        let numerator = Aligned::new(self.limbs.as_slice(), numerator_shift);
        let denominator = Aligned::new(divisor.limbs.as_slice(), divisor_shift);
        let numerator_words = numerator.trimmed_words_with_work(work)?;
        let denominator_words = denominator.trimmed_words_with_work(work)?;
        let (mut quotient, remainder) =
            Self::div_rem_unsigned_with_work(&numerator_words, &denominator_words, work)?;
        let remainder = Self {
            negative: false,
            limbs: CompactLimbs::from_vec_with_work(remainder, work)?,
            exponent,
        };
        if remainder.cmp_magnitude_scaled_with_work(divisor, -1, work)? == Ordering::Greater {
            let mut carry = true;
            for limb in &mut quotient {
                work.charge_into_error(1)?;
                let (incremented, overflowed) = limb.overflowing_add(1);
                *limb = incremented;
                if !overflowed {
                    carry = false;
                    break;
                }
            }
            if carry {
                if quotient.len() == quotient.capacity() {
                    work.charge_product_into_error(quotient.len().wrapping_add(1), 1)?;
                    work.charge_product_into_error(quotient.len(), 1)?;
                }
                quotient.reserve(1);
                work.charge_into_error(1)?;
                quotient.push(1);
            }
        }
        let mut quotient = Self {
            negative: self.negative != divisor.negative,
            limbs: CompactLimbs::from_vec_with_work(quotient, work)?,
            exponent: 0,
        };
        quotient.try_trim_with_work(work)?;
        Ok(quotient)
    }

    /// Greatest common divisor in the exact dyadic domain.
    ///
    /// `trim` keeps each nonzero limb bank odd, so the common power-of-two
    /// factor is the smaller exponent and the odd banks can use ordinary
    /// integer Euclid. The result is positive and exact.
    pub(in crate::geometry) fn gcd(&self, other: &Self) -> Self {
        self.gcd_with_work(other, &mut Unmetered)
            .expect("unmetered dyadic GCD cannot fail")
    }

    pub(in crate::geometry) fn gcd_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if self.is_zero() {
            return Ok(other.try_clone_with_work(work)?.abs());
        }
        if other.is_zero() {
            return Ok(self.try_clone_with_work(work)?.abs());
        }
        if self.limbs.as_slice() == [1] && self.exponent == 0 {
            return Ok(self.try_clone_with_work(work)?.abs());
        }
        if other.limbs.as_slice() == [1] && other.exponent == 0 {
            return Ok(other.try_clone_with_work(work)?.abs());
        }
        let exponent = self.exponent.min(other.exponent);
        if let (Some(mut left), Some(mut right)) = (self.small_magnitude(), other.small_magnitude())
        {
            // Binary GCD: both magnitudes are odd, so no common power of two
            // needs restoring.
            loop {
                work.charge_into_error(1)?;
                if left > right {
                    std::mem::swap(&mut left, &mut right);
                }
                right = right.wrapping_sub(left);
                if right == 0 {
                    return Ok(Self::from_odd_u128(false, left, exponent));
                }
                right >>= right.trailing_zeros();
            }
        }
        let mut left = Self::try_clone_words_with_work(&self.limbs, work)?;
        let mut right = Self::try_clone_words_with_work(&other.limbs, work)?;
        while !right.is_empty() {
            let (_, remainder) = Self::div_rem_unsigned_with_work(&left, &right, work)?;
            work.charge_into_error(1)?;
            left = right;
            right = remainder;
        }
        Ok(Self {
            negative: false,
            limbs: CompactLimbs::from_vec_with_work(left, work)?,
            exponent,
        })
    }

    /// Divide two exact dyadics when the quotient is itself dyadic.
    ///
    /// Rational-area reduction only calls this with a common divisor, so a
    /// nonzero remainder is an internal exactness failure rather than a
    /// rounded result.
    pub(in crate::geometry) fn try_exact_quotient(&self, divisor: &Self) -> Result<Self> {
        self.try_exact_quotient_with_work(divisor, &mut Unmetered)
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the divisor is rejected as zero first, and a canonical nonzero dyadic has a positive small magnitude"
    )]
    pub(in crate::geometry) fn try_exact_quotient_with_work<W: Work>(
        &self,
        divisor: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if divisor.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        if self.is_zero() {
            return Ok(Self::zero());
        }
        if divisor.limbs.as_slice() == [1] {
            let mut value = self.try_clone_with_work(work)?;
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
            work.charge_into_error(1)?;
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
        let (limbs, remainder) =
            Self::div_rem_unsigned_with_work(&self.limbs, &divisor.limbs, work)?;
        if !remainder.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "exact dyadic quotient has a remainder".into(),
            )
            .into());
        }
        let value = Self {
            negative: self.negative != divisor.negative,
            limbs: CompactLimbs::from_vec_with_work(limbs, work)?,
            exponent,
        };
        Ok(value)
    }

    pub(in crate::geometry) fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_with_work(other, &mut Unmetered)
            .expect("unmetered dyadic comparison cannot fail")
    }

    pub(in crate::geometry) fn cmp_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Ordering> {
        work.charge_into_error(1)?;
        if self.negative != other.negative {
            return Ok(if self.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            });
        }
        let order = self.cmp_magnitude_at_with_work(other, i128::from(other.exponent), work)?;
        if self.negative {
            Ok(order.reverse())
        } else {
            Ok(order)
        }
    }

    #[cfg(test)]
    pub(in crate::geometry) fn cmp_magnitude(&self, other: &Self) -> Ordering {
        self.cmp_magnitude_with_work(other, &mut Unmetered)
            .expect("unmetered dyadic comparison cannot fail")
    }

    pub(in crate::geometry) fn cmp_magnitude_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Ordering> {
        self.cmp_magnitude_at_with_work(other, i128::from(other.exponent), work)
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
    try_difference_with_work(left, right, &mut Unmetered)
}

pub(in crate::geometry) fn try_difference_with_work<W: Work>(
    left: f64,
    right: f64,
    work: &mut W,
) -> Result<ExactDyadic> {
    ExactDyadic::from_f64(left).try_subtract_with_work(ExactDyadic::from_f64(right), work)
}
