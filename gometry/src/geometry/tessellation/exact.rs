//! Exact stored-binary64 predicates and homogeneous constructions.
//!
//! Every finite binary64 is an integer times a power of two. This module owns
//! that representation for tessellation so topology decisions and shared
//! constructed vertices never depend on a lossy coordinate frame.

#![expect(
    clippy::missing_const_for_fn,
    clippy::useless_conversion,
    reason = "exact predicates favor uniform fallible expressions over lint-driven local rewrites"
)]

use std::cmp::Ordering;
use std::fmt;
use std::ops::{Deref, DerefMut};

use crate::error::ResourceError;
#[cfg(test)]
use crate::geometry::GENERATED_ITEM_LIMIT;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::stored_orientation_sign;
use crate::geometry::{GeometryErrorKind, Point, Result, XY};
use crate::try_reserve_vec;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PointInCycle {
    Outside,
    Boundary,
    Inside,
}

#[derive(Clone, Debug)]
pub(super) enum SegmentIntersection {
    None,
    Point(ExactPoint),
    Overlap { start: ExactPoint, end: ExactPoint },
}

impl ExactSign {
    pub(super) const fn as_i8(self) -> i8 {
        match self {
            Self::Negative => -1,
            Self::Zero => 0,
            Self::Positive => 1,
        }
    }

    /// Sign of an exact quotient. Zero absorbs the otherwise relative signs:
    /// a homogeneous denominator's sign cannot leak into the rational zero.
    const fn quotient(self, denominator: Self) -> Self {
        match (self, denominator) {
            (Self::Zero, _) | (_, Self::Zero) => Self::Zero,
            (Self::Negative, Self::Positive) | (Self::Positive, Self::Negative) => Self::Negative,
            (Self::Negative, Self::Negative) | (Self::Positive, Self::Positive) => Self::Positive,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactDyadic {
    negative: bool,
    limbs: CompactLimbs,
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
    exponent: i64,
}

/// Every observed value used at most one trimmed limb in ordinary Cartesian
/// relation, overlay, and polygonize workloads. Two inline limbs also cover
/// the untrimmed sum/product of two observed one-limb operands; larger exact
/// values retain the old fallible heap representation.
const INLINE_DYADIC_LIMBS: usize = 2;

#[derive(Clone)]
enum CompactLimbs {
    Inline {
        limbs: [u64; INLINE_DYADIC_LIMBS],
        len: u8,
    },
    Heap(Vec<u64>),
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
    const fn zero() -> Self {
        Self::Inline {
            limbs: [0; INLINE_DYADIC_LIMBS],
            len: 0,
        }
    }

    const fn single(value: u64) -> Self {
        Self::Inline {
            limbs: [value; INLINE_DYADIC_LIMBS],
            len: 1,
        }
    }

    fn with_capacity(required: usize) -> Self {
        if required <= INLINE_DYADIC_LIMBS {
            Self::Inline {
                limbs: [0; INLINE_DYADIC_LIMBS],
                len: 0,
            }
        } else {
            Self::Heap(Vec::with_capacity(required))
        }
    }

    fn try_with_capacity(required: usize) -> Result<Self> {
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

    fn from_vec(limbs: Vec<u64>) -> Self {
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

    fn as_slice(&self) -> &[u64] {
        match self {
            Self::Inline { limbs, len } => &limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u64] {
        match self {
            Self::Inline { limbs, len } => &mut limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    const fn is_empty(&self) -> bool {
        match self {
            Self::Inline { len, .. } => *len == 0,
            Self::Heap(limbs) => limbs.is_empty(),
        }
    }

    fn push(&mut self, value: u64) {
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

    fn pop(&mut self) -> Option<u64> {
        match self {
            Self::Inline { limbs, len } => {
                let index = usize::from(*len).checked_sub(1)?;
                *len -= 1;
                Some(limbs[index])
            },
            Self::Heap(limbs) => limbs.pop(),
        }
    }

    fn remove_prefix(&mut self, count: usize) {
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

    fn try_clone(&self) -> Result<Self> {
        match self {
            Self::Inline { limbs, len } => Ok(Self::Inline {
                limbs: *limbs,
                len: *len,
            }),
            Self::Heap(limbs) => {
                let mut clone = Vec::new();
                try_reserve_vec(&mut clone, limbs.len())?;
                clone.extend_from_slice(limbs);
                Ok(Self::from_vec(clone))
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
pub(super) struct ExactLine([ExactDyadic; 3]);

impl ExactLine {
    pub(super) fn perpendicular_bisector(a: XY, b: XY) -> Self {
        let ax = ExactDyadic::from_f64(a.x);
        let ay = ExactDyadic::from_f64(a.y);
        let bx = ExactDyadic::from_f64(b.x);
        let by = ExactDyadic::from_f64(b.y);
        Self([
            bx.clone()
                .subtract(ax.clone())
                .product(&ExactDyadic::from_f64(2.0)),
            by.clone()
                .subtract(ay.clone())
                .product(&ExactDyadic::from_f64(2.0)),
            ax.square()
                .add(ay.square())
                .subtract(bx.square().add(by.square())),
        ])
    }

    pub(super) fn try_perpendicular_bisector(a: XY, b: XY) -> Result<Self> {
        let ax = ExactDyadic::from_f64(a.x);
        let ay = ExactDyadic::from_f64(a.y);
        let bx = ExactDyadic::from_f64(b.x);
        let by = ExactDyadic::from_f64(b.y);
        Ok(Self([
            bx.clone()
                .try_subtract(ax.clone())?
                .try_product(&ExactDyadic::from_f64(2.0))?,
            by.clone()
                .try_subtract(ay.clone())?
                .try_product(&ExactDyadic::from_f64(2.0))?,
            ax.try_product(&ax)?
                .try_add(ay.try_product(&ay)?)?
                .try_subtract(bx.try_product(&bx)?.try_add(by.try_product(&by)?)?)?,
        ]))
    }

    pub(super) fn through_points(a: &ExactPoint, b: &ExactPoint) -> Result<Self> {
        if !a.is_finite() || !b.is_finite() || a.same_position(b) {
            return Err(GeometryErrorKind::voronoi(
                "exact support line requires two distinct finite points",
            ));
        }
        Ok(Self([
            a.y().product(b.w()).subtract(b.y().product(a.w())),
            b.x().product(a.w()).subtract(a.x().product(b.w())),
            a.x().product(b.y()).subtract(b.x().product(a.y())),
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
    pub(super) fn from_parts(negative: bool, limbs: Vec<u64>, exponent: i64) -> Self {
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

    fn trim(&mut self) {
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
    fn shifted_word(limbs: &[u64], word_shift: usize, bit_shift: u32, index: usize) -> u64 {
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

    fn shifted_len(limbs: &[u64], shift: usize) -> usize {
        if limbs.is_empty() {
            return 0;
        }
        limbs.len() + shift / 64 + usize::from(!shift.is_multiple_of(64))
    }

    fn write_sum_aligned(left: Aligned<'_>, right: Aligned<'_>, out: &mut CompactLimbs) {
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

    fn write_diff_aligned(larger: Aligned<'_>, smaller: Aligned<'_>, out: &mut CompactLimbs) {
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

    fn add_aligned(left: Aligned<'_>, right: Aligned<'_>) -> CompactLimbs {
        let length = left.len.max(right.len);
        let mut result = CompactLimbs::with_capacity(length + 1);
        Self::write_sum_aligned(left, right, &mut result);
        result
    }

    fn subtract_aligned(larger: Aligned<'_>, smaller: Aligned<'_>) -> CompactLimbs {
        let mut result = CompactLimbs::with_capacity(larger.len);
        Self::write_diff_aligned(larger, smaller, &mut result);
        result
    }

    fn try_add_aligned(left: Aligned<'_>, right: Aligned<'_>) -> Result<CompactLimbs> {
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

    fn try_subtract_aligned(larger: Aligned<'_>, smaller: Aligned<'_>) -> Result<CompactLimbs> {
        let mut result = CompactLimbs::try_with_capacity(larger.len)?;
        Self::write_diff_aligned(larger, smaller, &mut result);
        Ok(result)
    }

    fn compare_aligned(left: Aligned<'_>, right: Aligned<'_>) -> Ordering {
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

    pub(in crate::geometry) fn add(self, other: Self) -> Self {
        if self.is_zero() {
            return other;
        }
        if other.is_zero() {
            return self;
        }
        let exponent = self.exponent.min(other.exponent);
        let left = Aligned::new(&self.limbs, (self.exponent - exponent) as usize);
        let right = Aligned::new(&other.limbs, (other.exponent - exponent) as usize);
        let (negative, limbs) = if self.negative == other.negative {
            (self.negative, Self::add_aligned(left, right))
        } else {
            match Self::compare_aligned(left, right) {
                Ordering::Greater => (self.negative, Self::subtract_aligned(left, right)),
                Ordering::Less => (other.negative, Self::subtract_aligned(right, left)),
                Ordering::Equal => return Self::zero(),
            }
        };
        let mut result = Self {
            negative,
            limbs,
            exponent,
        };
        result.trim();
        result
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
            return right.try_clone();
        }
        if right.is_zero() {
            return left.try_clone();
        }
        Self::try_combine_refs(left, right, false)
    }

    fn try_combine_refs(left: &Self, right: &Self, negate_right: bool) -> Result<Self> {
        debug_assert!(!left.is_zero() && !right.is_zero());
        let exponent = left.exponent.min(right.exponent);
        let left_negative = left.negative;
        let right_negative = right.negative ^ negate_right;
        let left_shift =
            usize::try_from(left.exponent - exponent).map_err(|_| ResourceError::SizeOverflow)?;
        let right_shift =
            usize::try_from(right.exponent - exponent).map_err(|_| ResourceError::SizeOverflow)?;
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

    fn try_subtract_ref(&self, other: &Self) -> Result<Self> {
        Self::try_subtract_refs(self, other)
    }

    pub(in crate::geometry) fn try_subtract_refs(left: &Self, right: &Self) -> Result<Self> {
        if left.is_zero() {
            let mut result = right.try_clone()?;
            if !result.is_zero() {
                result.negative = !result.negative;
            }
            return Ok(result);
        }
        if right.is_zero() {
            return left.try_clone();
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

    pub(in crate::geometry) fn subtract(self, other: Self) -> Self {
        self.add(other.neg())
    }

    /// Compare `self` with `other * 2^shift` by magnitude without constructing
    /// the scalar product. The ratio-rounding residual uses this for the exact
    /// power-of-two gap between adjacent binary64 values.
    fn cmp_magnitude_scaled(&self, other: &Self, shift: i64) -> Option<Ordering> {
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

    fn write_product(short: &[u64], long: &[u64], out: &mut CompactLimbs) {
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

    pub(in crate::geometry) fn product(&self, other: &Self) -> Self {
        if self.is_zero() || other.is_zero() {
            return Self::zero();
        }
        let (short, long) = if self.limbs.len() <= other.limbs.len() {
            (&self.limbs, &other.limbs)
        } else {
            (&other.limbs, &self.limbs)
        };
        let mut limbs = CompactLimbs::with_capacity(short.len() + long.len());
        Self::write_product(short, long, &mut limbs);
        let mut result = Self {
            negative: self.negative != other.negative,
            limbs,
            exponent: self.exponent + other.exponent,
        };
        result.trim();
        result
    }

    pub(in crate::geometry) fn try_product(&self, other: &Self) -> Result<Self> {
        if self.is_zero() || other.is_zero() {
            return Ok(Self::zero());
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

    fn unsigned_bit_length(limbs: &[u64]) -> usize {
        let Some(&high) = limbs.last() else {
            return 0;
        };
        limbs.len().saturating_sub(1) * 64 + (64 - high.leading_zeros() as usize)
    }

    fn compare_unsigned(left: &[u64], right: &[u64]) -> Ordering {
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

    fn subtract_unsigned_in_place(left: &mut [u64], right: &[u64]) {
        let mut borrow = false;
        for (index, value) in left.iter_mut().enumerate() {
            let (difference, first_borrow) =
                value.overflowing_sub(right.get(index).copied().unwrap_or(0));
            let (difference, second_borrow) = difference.overflowing_sub(u64::from(borrow));
            *value = difference;
            borrow = first_borrow || second_borrow;
        }
        debug_assert!(!borrow);
    }

    fn shifted_unsigned(limbs: &[u64], shift: usize) -> Result<Vec<u64>> {
        if limbs.is_empty() {
            return Ok(Vec::new());
        }
        let words = shift / 64;
        let bits = shift % 64;
        let length = limbs
            .len()
            .checked_add(words)
            .and_then(|length| length.checked_add(usize::from(bits != 0)))
            .ok_or(ResourceError::SizeOverflow)?;
        let mut shifted = Vec::new();
        try_reserve_vec(&mut shifted, length)?;
        shifted.resize(length, 0);
        for (index, &limb) in limbs.iter().enumerate() {
            let target = index + words;
            shifted[target] |= limb << bits;
            if bits != 0 {
                shifted[target + 1] |= limb >> (64 - bits);
            }
        }
        while shifted.last() == Some(&0) {
            shifted.pop();
        }
        Ok(shifted)
    }

    /// Divide two non-negative integers represented by little-endian limbs.
    ///
    /// The quotient is only needed for exact rational reduction, so this uses
    /// shift/subtract long division instead of introducing a second arbitrary
    /// precision dependency. Every result vector is fallibly reserved.
    fn try_div_rem_unsigned(dividend: &[u64], divisor: &[u64]) -> Result<(Vec<u64>, Vec<u64>)> {
        debug_assert!(!divisor.is_empty());
        if dividend.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let dividend_bits = Self::unsigned_bit_length(dividend);
        let divisor_bits = Self::unsigned_bit_length(divisor);
        if dividend_bits < divisor_bits {
            let mut remainder = Vec::new();
            try_reserve_vec(&mut remainder, dividend.len())?;
            remainder.extend_from_slice(dividend);
            return Ok((Vec::new(), remainder));
        }
        let quotient_bits = dividend_bits - divisor_bits + 1;
        let quotient_len = quotient_bits.div_ceil(64);
        let mut quotient = Vec::new();
        try_reserve_vec(&mut quotient, quotient_len)?;
        quotient.resize(quotient_len, 0);
        let mut remainder = Vec::new();
        try_reserve_vec(&mut remainder, dividend.len())?;
        remainder.extend_from_slice(dividend);
        while Self::compare_unsigned(&remainder, divisor) != Ordering::Less {
            let remainder_bits = Self::unsigned_bit_length(&remainder);
            let mut shift = remainder_bits - divisor_bits;
            let mut shifted = Self::shifted_unsigned(divisor, shift)?;
            if Self::compare_unsigned(&remainder, &shifted) == Ordering::Less {
                shift -= 1;
                shifted = Self::shifted_unsigned(divisor, shift)?;
            }
            Self::subtract_unsigned_in_place(&mut remainder, &shifted);
            while remainder.last() == Some(&0) {
                remainder.pop();
            }
            quotient[shift / 64] |= 1_u64 << (shift % 64);
        }
        while quotient.last() == Some(&0) {
            quotient.pop();
        }
        Ok((quotient, remainder))
    }

    /// Greatest common divisor in the exact dyadic domain.
    ///
    /// `trim` keeps each nonzero limb bank odd, so the common power-of-two
    /// factor is the smaller exponent and the odd banks can use ordinary
    /// integer Euclid. The result is positive and exact.
    pub(in crate::geometry) fn try_gcd(&self, other: &Self) -> Result<Self> {
        if self.is_zero() {
            return other.try_clone().map(Self::abs);
        }
        if other.is_zero() {
            return self.try_clone().map(Self::abs);
        }
        if self.limbs.as_slice() == [1] && self.exponent == 0 {
            return self.try_clone().map(Self::abs);
        }
        if other.limbs.as_slice() == [1] && other.exponent == 0 {
            return other.try_clone().map(Self::abs);
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
            exponent: self.exponent.min(other.exponent),
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
            let mut value = self.try_clone()?;
            value.negative = self.negative != divisor.negative;
            value.exponent = self
                .exponent
                .checked_sub(divisor.exponent)
                .ok_or(ResourceError::SizeOverflow)?;
            return Ok(value);
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
            exponent: self
                .exponent
                .checked_sub(divisor.exponent)
                .ok_or(ResourceError::SizeOverflow)?,
        };
        Ok(value)
    }

    pub(in crate::geometry) fn try_from_f64(value: f64) -> Result<Self> {
        debug_assert!(value.is_finite());
        if value == 0.0 {
            return Ok(Self::zero());
        }
        let bits = value.to_bits();
        let fraction = bits & ((1_u64 << 52) - 1);
        let encoded_exponent = ((bits >> 52) & 0x7FF) as i64;
        let (significand, exponent) = if encoded_exponent == 0 {
            (fraction, -1074)
        } else {
            ((1_u64 << 52) | fraction, encoded_exponent - 1023 - 52)
        };
        let mut limbs = CompactLimbs::try_with_capacity(1)?;
        limbs.push(significand);
        let mut value = Self {
            negative: bits >> 63 != 0,
            limbs,
            exponent,
        };
        value.trim();
        Ok(value)
    }

    pub(in crate::geometry) fn try_clone(&self) -> Result<Self> {
        let value = Self {
            negative: self.negative,
            limbs: self.limbs.try_clone()?,
            exponent: self.exponent,
        };
        Ok(value)
    }

    pub(in crate::geometry) fn square(&self) -> Self {
        self.product(self)
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

fn difference(left: f64, right: f64) -> ExactDyadic {
    ExactDyadic::from_f64(left).subtract(ExactDyadic::from_f64(right))
}

fn try_difference(left: f64, right: f64) -> Result<ExactDyadic> {
    ExactDyadic::try_from_f64(left)?.try_subtract(ExactDyadic::try_from_f64(right)?)
}

/// An exact ratio of dyadics.
///
/// Source-boundary parameters are usually dyadic, but a pre-existing mesh
/// vertex can lie at a non-dyadic ratio of two stored binary64 endpoints.
/// Keeping the numerator and denominator as exact integers avoids making that
/// vertex a rounded source authority. Every constructor reduces the ratio, so
/// repeated midpoint splits stay linear in their binary parameter depth rather
/// than multiplying equivalent denominators.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactRational {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

impl ExactRational {
    pub(in crate::geometry) fn new(
        numerator: ExactDyadic,
        denominator: ExactDyadic,
    ) -> Result<Self> {
        if denominator.is_zero() {
            return Err(GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        if numerator.is_zero() {
            return Ok(Self {
                numerator: ExactDyadic::zero(),
                denominator: ExactDyadic::one(),
            });
        }
        let (numerator, denominator) = if denominator.sign() == ExactSign::Negative {
            (numerator.neg(), denominator.neg())
        } else {
            (numerator, denominator)
        };
        let common = numerator.try_gcd(&denominator)?;
        Ok(Self {
            numerator: numerator.try_exact_quotient(&common)?,
            denominator: denominator.try_exact_quotient(&common)?,
        })
    }

    pub(in crate::geometry) fn zero() -> Self {
        Self {
            numerator: ExactDyadic::zero(),
            denominator: ExactDyadic::one(),
        }
    }

    pub(in crate::geometry) fn one() -> Self {
        Self {
            numerator: ExactDyadic::one(),
            denominator: ExactDyadic::one(),
        }
    }

    pub(in crate::geometry) fn from_f64(value: f64) -> Result<Self> {
        Self::from_dyadic(ExactDyadic::try_from_f64(value)?)
    }

    pub(in crate::geometry) fn from_dyadic(value: ExactDyadic) -> Result<Self> {
        if value.is_zero() {
            return Ok(Self::zero());
        }
        Ok(Self {
            numerator: value,
            denominator: ExactDyadic::one(),
        })
    }

    pub(in crate::geometry) fn is_zero(&self) -> bool {
        self.numerator.is_zero()
    }

    pub(in crate::geometry) fn sign(&self) -> ExactSign {
        self.numerator.sign()
    }

    pub(in crate::geometry) fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            numerator: self.numerator.try_clone()?,
            denominator: self.denominator.try_clone()?,
        })
    }

    pub(in crate::geometry) fn try_add(&self, other: &Self) -> Result<Self> {
        if self.denominator.is_one() && other.denominator.is_one() {
            let numerator = if self.numerator.is_zero() {
                other.numerator.try_clone()?
            } else if other.numerator.is_zero() {
                self.numerator.try_clone()?
            } else {
                ExactDyadic::try_combine_refs(&self.numerator, &other.numerator, false)?
            };
            return Self::from_dyadic(numerator);
        }
        let left = self.numerator.try_product(&other.denominator)?;
        let right = other.numerator.try_product(&self.denominator)?;
        let numerator = left.try_add(right)?;
        let denominator = self.denominator.try_product(&other.denominator)?;
        Self::new(numerator, denominator)
    }

    pub(in crate::geometry) fn try_subtract(&self, other: &Self) -> Result<Self> {
        if self.denominator.is_one() && other.denominator.is_one() {
            return Self::from_dyadic(self.numerator.try_subtract_ref(&other.numerator)?);
        }
        let left = self.numerator.try_product(&other.denominator)?;
        let right = other.numerator.try_product(&self.denominator)?;
        let numerator = left.try_subtract(right)?;
        let denominator = self.denominator.try_product(&other.denominator)?;
        Self::new(numerator, denominator)
    }

    pub(in crate::geometry) fn try_product(&self, other: &Self) -> Result<Self> {
        if self.denominator.is_one() && other.denominator.is_one() {
            return Self::from_dyadic(self.numerator.try_product(&other.numerator)?);
        }
        Self::new(
            self.numerator.try_product(&other.numerator)?,
            self.denominator.try_product(&other.denominator)?,
        )
    }

    pub(in crate::geometry) fn try_square(&self) -> Result<Self> {
        if self.denominator.is_one() {
            return Self::from_dyadic(self.numerator.try_product(&self.numerator)?);
        }
        self.try_product(self)
    }

    pub(in crate::geometry) fn try_quotient(&self, other: &Self) -> Result<Self> {
        Self::new(
            self.numerator.try_product(&other.denominator)?,
            self.denominator.try_product(&other.numerator)?,
        )
    }

    pub(in crate::geometry) fn try_midpoint(&self, other: &Self) -> Result<Self> {
        let numerator = self.try_add(other)?;
        Self::new(
            numerator.numerator.try_clone()?,
            numerator
                .denominator
                .try_product(&ExactDyadic::from_f64(2.0))?,
        )
    }

    pub(in crate::geometry) fn try_cmp(&self, other: &Self) -> Result<Ordering> {
        Ok(self
            .numerator
            .try_product(&other.denominator)?
            .cmp(&other.numerator.try_product(&self.denominator)?))
    }

    pub(in crate::geometry) fn parts(&self) -> (&ExactDyadic, &ExactDyadic) {
        (&self.numerator, &self.denominator)
    }

    pub(in crate::geometry) fn round(&self) -> Result<f64> {
        round_ratio(&self.numerator, &self.denominator)
    }

    pub(in crate::geometry) fn outward_bounds(&self) -> Result<(f64, f64)> {
        outward_ratio_bounds(&self.numerator, &self.denominator)
    }

    pub(in crate::geometry) fn round_sqrt(&self) -> Result<f64> {
        if self.sign() == ExactSign::Negative {
            return Err(
                GeometryErrorKind::Invalid("negative exact squared distance".into()).into(),
            );
        }
        round_sqrt_ratio(&self.numerator, &self.denominator)
    }

    /// The exact parameter of `point` on the stored segment `start -> end`.
    ///
    /// The caller establishes collinearity; this routine only chooses the
    /// nonzero axis and preserves its exact ratio.
    #[expect(
        clippy::float_cmp,
        reason = "exact coordinate equality selects the nonzero source axis; tolerance would change the parameter"
    )]
    pub(super) fn from_segment(start: XY, end: XY, point: XY) -> Result<Self> {
        let (start, end, point) = if start.x != end.x {
            (start.x, end.x, point.x)
        } else if start.y != end.y {
            (start.y, end.y, point.y)
        } else {
            return Err(GeometryErrorKind::triangulation(
                "source constraint has identical endpoints",
            ));
        };
        Self::new(try_difference(point, start)?, try_difference(end, start)?)
    }

    /// Round the exact source point at this parameter, evaluating both
    /// coordinates from the original endpoints before either is emitted.
    pub(super) fn round_segment(&self, start: XY, end: XY) -> Result<XY> {
        let remainder = self.denominator.try_subtract_ref(&self.numerator)?;
        let x = ExactDyadic::from_f64(start.x)
            .try_product(&remainder)?
            .try_add(ExactDyadic::from_f64(end.x).try_product(&self.numerator)?)?;
        let y = ExactDyadic::from_f64(start.y)
            .try_product(&remainder)?
            .try_add(ExactDyadic::from_f64(end.y).try_product(&self.numerator)?)?;
        Ok(XY::new(
            round_ratio(&x, &self.denominator)?,
            round_ratio(&y, &self.denominator)?,
        ))
    }
}

/// Shewchuk's stage-A orientation filter: the plain float determinant is the
/// answer whenever its magnitude clears an a-priori bound on its rounding
/// error, `(3 + 16ε)ε` times the sum of the term magnitudes, which covers
/// the two differences and the two products. Costs a handful of flops and
/// decides almost every call; the interval filter and the dyadic path behind
/// it take what it declines, including anything non-finite.
fn orient2d_float(a: XY, b: XY, c: XY) -> Option<ExactSign> {
    const ERRBOUND: f64 = (3.0 + 16.0 * f64::EPSILON) * f64::EPSILON;
    // Components in [2^-450, 2^450] lie on the 2^-502 lattice.
    // Degree-two products and cancellations cannot underflow; the upper
    // bound also leaves room for the determinant sum and its error bound.
    const MIN_COMPONENT: f64 = f64::from_bits((1023 - 450) << 52);
    const MAX_COMPONENT: f64 = f64::from_bits((1023 + 450) << 52);
    let (acx, acy, bcx, bcy) = (a.x - c.x, a.y - c.y, b.x - c.x, b.y - c.y);
    if [acx, acy, bcx, bcy]
        .iter()
        .any(|value| *value != 0.0 && !(MIN_COMPONENT..=MAX_COMPONENT).contains(&value.abs()))
    {
        return None;
    }
    let (left, right) = (acx * bcy, acy * bcx);
    let det = left - right;
    let detsum = left.abs() + right.abs();
    if !det.is_finite() || !detsum.is_finite() {
        return None;
    }
    let bound = ERRBOUND * detsum;
    if det > bound {
        Some(ExactSign::Positive)
    } else if det < -bound {
        Some(ExactSign::Negative)
    } else {
        None
    }
}

/// Shewchuk's stage-A in-circle filter: the lifted 3x3 determinant with the
/// `(10 + 96ε)ε` bound on its permanent.
fn incircle_float(a: XY, b: XY, c: XY, d: XY) -> Option<ExactSign> {
    const ERRBOUND: f64 = (10.0 + 96.0 * f64::EPSILON) * f64::EPSILON;
    // The relative-error filter requires normal intermediate arithmetic.
    // Components in [2^-200, 2^200] lie on the 2^-252 lattice, so every
    // nonzero degree-four intermediate is at least 2^-1008. Products and
    // their sums stay below 2^804, and a nonzero permanent is at least
    // 2^-800, keeping its error bound normal too.
    const MIN_COMPONENT: f64 = f64::from_bits((1023 - 200) << 52);
    const MAX_COMPONENT: f64 = f64::from_bits((1023 + 200) << 52);
    let (adx, ady) = (a.x - d.x, a.y - d.y);
    let (bdx, bdy) = (b.x - d.x, b.y - d.y);
    let (cdx, cdy) = (c.x - d.x, c.y - d.y);
    if [adx, ady, bdx, bdy, cdx, cdy]
        .iter()
        .any(|value| *value != 0.0 && !(MIN_COMPONENT..=MAX_COMPONENT).contains(&value.abs()))
    {
        return None;
    }
    let (bdxcdy, cdxbdy) = (bdx * cdy, cdx * bdy);
    let alift = adx * adx + ady * ady;
    let (cdxady, adxcdy) = (cdx * ady, adx * cdy);
    let blift = bdx * bdx + bdy * bdy;
    let (adxbdy, bdxady) = (adx * bdy, bdx * ady);
    let clift = cdx * cdx + cdy * cdy;
    let det = alift * (bdxcdy - cdxbdy) + blift * (cdxady - adxcdy) + clift * (adxbdy - bdxady);
    let permanent = (bdxcdy.abs() + cdxbdy.abs()) * alift
        + (cdxady.abs() + adxcdy.abs()) * blift
        + (adxbdy.abs() + bdxady.abs()) * clift;
    if !det.is_finite() || !permanent.is_finite() {
        return None;
    }
    let bound = ERRBOUND * permanent;
    if det > bound {
        Some(ExactSign::Positive)
    } else if det < -bound {
        Some(ExactSign::Negative)
    } else {
        None
    }
}

/// The inputs scaled to integers on one common power of two, sign preserved,
/// when every scaled magnitude is below 2^21. Lattice data (rasters, integer
/// metres, regular grids) lands here, and it is exactly the data whose
/// cocircular and collinear cases defeat every floating filter; the degree-4
/// in-circle determinant of six such terms stays below 2^92, so `i128` is
/// exact without allocating.
fn lattice_integers<const N: usize>(values: [f64; N]) -> Option<[i128; N]> {
    const LIMIT_BITS: u32 = 21;
    let mut mantissas = [0_i64; N];
    let mut exponents = [i32::MAX; N];
    for (index, &value) in values.iter().enumerate() {
        if !value.is_finite() {
            return None;
        }
        if value == 0.0 {
            continue;
        }
        let bits = value.to_bits();
        let biased = ((bits >> 52) & 0x7FF) as i32;
        let fraction = bits & ((1_u64 << 52) - 1);
        let (mut mantissa, mut exponent) = if biased == 0 {
            (fraction, -1074)
        } else {
            (fraction | (1_u64 << 52), biased - 1075)
        };
        let trailing = mantissa.trailing_zeros();
        mantissa >>= trailing;
        exponent += trailing as i32;
        mantissas[index] = if value < 0.0 {
            -(mantissa as i64)
        } else {
            mantissa as i64
        };
        exponents[index] = exponent;
    }
    let common = exponents.iter().copied().min().unwrap_or(i32::MAX);
    let mut scaled = [0_i128; N];
    for index in 0..N {
        if mantissas[index] == 0 {
            continue;
        }
        let shift = exponents[index] - common;
        let bits = 64 - mantissas[index].unsigned_abs().leading_zeros();
        if shift < 0 || shift as u32 + bits > LIMIT_BITS {
            return None;
        }
        scaled[index] = i128::from(mantissas[index]) << shift;
    }
    Some(scaled)
}

fn sign_of_i128(value: i128) -> ExactSign {
    match value.cmp(&0) {
        Ordering::Greater => ExactSign::Positive,
        Ordering::Less => ExactSign::Negative,
        Ordering::Equal => ExactSign::Zero,
    }
}

fn orient2d_lattice(a: XY, b: XY, c: XY) -> Option<ExactSign> {
    let [ax, ay, bx, by, cx, cy] = lattice_integers([a.x, a.y, b.x, b.y, c.x, c.y])?;
    Some(sign_of_i128((bx - ax) * (cy - ay) - (by - ay) * (cx - ax)))
}

fn incircle_lattice(a: XY, b: XY, c: XY, d: XY) -> Option<ExactSign> {
    let [ax, ay, bx, by, cx, cy, dx, dy] =
        lattice_integers([a.x, a.y, b.x, b.y, c.x, c.y, d.x, d.y])?;
    let (adx, ady) = (ax - dx, ay - dy);
    let (bdx, bdy) = (bx - dx, by - dy);
    let (cdx, cdy) = (cx - dx, cy - dy);
    let alift = adx * adx + ady * ady;
    let blift = bdx * bdx + bdy * bdy;
    let clift = cdx * cdx + cdy * cdy;
    let bcdet = bdx * cdy - cdx * bdy;
    let cadet = cdx * ady - adx * cdy;
    let abdet = adx * bdy - bdx * ady;
    Some(sign_of_i128(alift * bcdet + blift * cadet + clift * abdet))
}

pub(super) fn orient2d(a: XY, b: XY, c: XY) -> ExactSign {
    if let Some(sign) = orient2d_float(a, b, c) {
        debug_assert_eq!(
            sign,
            stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y),
            "float-filtered orient2d disagrees"
        );
        return sign;
    }
    if let Some(sign) = orient2d_lattice(a, b, c) {
        debug_assert_eq!(
            sign,
            stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y),
            "lattice orient2d disagrees"
        );
        return sign;
    }
    stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y)
}

/// The interval filter for the in-circle determinant, with the same
/// containment argument and the same exact fallback.
fn incircle_filtered(a: XY, b: XY, c: XY, d: XY) -> Option<ExactSign> {
    let value = Interval::exact;
    let displacement = |point: XY| {
        (
            value(point.x).sub(value(d.x)),
            value(point.y).sub(value(d.y)),
        )
    };
    let (adx, ady) = displacement(a);
    let (bdx, bdy) = displacement(b);
    let (cdx, cdy) = displacement(c);
    if !(adx.is_finite()
        && ady.is_finite()
        && bdx.is_finite()
        && bdy.is_finite()
        && cdx.is_finite()
        && cdy.is_finite())
    {
        return None;
    }
    let alift = adx.mul(adx).add(ady.mul(ady));
    let blift = bdx.mul(bdx).add(bdy.mul(bdy));
    let clift = cdx.mul(cdx).add(cdy.mul(cdy));
    let bcdet = bdx.mul(cdy).sub(cdx.mul(bdy));
    let cadet = cdx.mul(ady).sub(adx.mul(cdy));
    let abdet = adx.mul(bdy).sub(bdx.mul(ady));
    if !(alift.is_finite()
        && blift.is_finite()
        && clift.is_finite()
        && bcdet.is_finite()
        && cadet.is_finite()
        && abdet.is_finite())
    {
        return None;
    }
    let determinant = alift.mul(bcdet).add(blift.mul(cadet)).add(clift.mul(abdet));
    determinant.sign()
}

#[cfg(test)]
pub(super) fn incircle(a: XY, b: XY, c: XY, d: XY) -> ExactSign {
    if let Some(sign) = incircle_float(a, b, c, d) {
        debug_assert_eq!(
            sign,
            incircle_dyadic(a, b, c, d),
            "float-filtered incircle disagrees"
        );
        return sign;
    }
    if let Some(sign) = incircle_lattice(a, b, c, d) {
        debug_assert_eq!(
            sign,
            incircle_dyadic(a, b, c, d),
            "lattice incircle disagrees"
        );
        return sign;
    }
    if let Some(sign) = incircle_filtered(a, b, c, d) {
        if VERIFY_INTERVALS {
            assert_eq!(
                sign,
                incircle_dyadic(a, b, c, d),
                "filtered incircle disagrees"
            );
        }
        return sign;
    }
    incircle_dyadic(a, b, c, d)
}

fn try_orient2d_dyadic(a: XY, b: XY, c: XY) -> Result<ExactSign> {
    let bax = try_difference(b.x, a.x)?;
    let bay = try_difference(b.y, a.y)?;
    let cax = try_difference(c.x, a.x)?;
    let cay = try_difference(c.y, a.y)?;
    Ok(bax
        .try_product(&cay)?
        .try_subtract(bay.try_product(&cax)?)?
        .sign())
}

/// Fallible exact orientation for a stored-binary64 triple.
///
/// The ordinary filters are allocation-free.  A full dyadic fallback keeps the
/// same exact sign contract while allowing the owned arbitrary-precision
/// storage to report an allocation failure rather than panicking.
pub(in crate::geometry) fn try_orient2d(a: XY, b: XY, c: XY) -> Result<ExactSign> {
    if let Some(sign) = orient2d_float(a, b, c) {
        return Ok(sign);
    }
    if let Some(sign) = orient2d_lattice(a, b, c) {
        return Ok(sign);
    }
    try_orient2d_dyadic(a, b, c)
}

fn same_xy(left: XY, right: XY) -> bool {
    crate::geometry::canonical_f64_bits(left.x) == crate::geometry::canonical_f64_bits(right.x)
        && crate::geometry::canonical_f64_bits(left.y)
            == crate::geometry::canonical_f64_bits(right.y)
}

fn diameter_contains_filtered(a: XY, b: XY, point: XY) -> Option<bool> {
    let exact = Interval::exact;
    let pa = (
        exact(point.x).sub(exact(a.x)),
        exact(point.y).sub(exact(a.y)),
    );
    let pb = (
        exact(point.x).sub(exact(b.x)),
        exact(point.y).sub(exact(b.y)),
    );
    if !(pa.0.is_finite() && pa.1.is_finite() && pb.0.is_finite() && pb.1.is_finite()) {
        return None;
    }
    let dot = pa.0.mul(pb.0).add(pa.1.mul(pb.1));
    dot.is_finite()
        .then(|| dot.sign())
        .flatten()
        .map(|sign| sign != ExactSign::Positive)
}

/// Exact containment in the circle through one, two, or three source points.
///
/// A one-point candidate is an identity test.  A two-point candidate is the
/// diameter disk, whose exact predicate is `(p-a)·(p-b) <= 0`.  A three-point
/// candidate uses the oriented exact in-circle determinant.
pub(in crate::geometry) fn try_circle_contains(support: &[XY], point: XY) -> Result<bool> {
    match support {
        [only] => Ok(same_xy(*only, point)),
        [a, b] => {
            if let Some(contains) = diameter_contains_filtered(*a, *b, point) {
                return Ok(contains);
            }
            let pa = (try_difference(point.x, a.x)?, try_difference(point.y, a.y)?);
            let pb = (try_difference(point.x, b.x)?, try_difference(point.y, b.y)?);
            let dot = pa.0.try_product(&pb.0)?.try_add(pa.1.try_product(&pb.1)?)?;
            Ok(dot.sign() != ExactSign::Positive)
        },
        [a, b, c] => {
            let orientation = try_orient2d(*a, *b, *c)?;
            if orientation == ExactSign::Zero {
                return Ok(false);
            }
            let determinant = try_incircle(*a, *b, *c, point)?;
            Ok(match orientation {
                ExactSign::Positive => determinant != ExactSign::Negative,
                ExactSign::Negative => determinant != ExactSign::Positive,
                ExactSign::Zero => unreachable!(),
            })
        },
        _ => Err(GeometryErrorKind::Invalid(
            "minimum circle support must contain one, two, or three points".into(),
        )
        .into()),
    }
}

/// Compare squared distances between two source pairs without rounding or
/// forming a floating-point square.
pub(in crate::geometry) fn try_squared_distance_cmp(
    left_a: XY,
    left_b: XY,
    right_a: XY,
    right_b: XY,
) -> Result<Ordering> {
    let squared = |a: XY, b: XY| -> Result<ExactDyadic> {
        let dx = try_difference(a.x, b.x)?;
        let dy = try_difference(a.y, b.y)?;
        dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)
    };
    Ok(squared(left_a, left_b)?.cmp(&squared(right_a, right_b)?))
}

struct ExactSupportCircle {
    center: ExactPoint,
    radius_numerator: ExactDyadic,
    radius_denominator: ExactDyadic,
}

fn try_exact_point(point: XY) -> Result<ExactPoint> {
    Ok(ExactPoint::homogeneous(
        ExactDyadic::try_from_f64(point.x)?,
        ExactDyadic::try_from_f64(point.y)?,
        ExactDyadic::one(),
    ))
}

fn try_circumcenter_exact(a: XY, b: XY, c: XY) -> Result<ExactPoint> {
    if try_orient2d(a, b, c)? == ExactSign::Zero {
        return Err(GeometryErrorKind::voronoi(
            "cannot construct a circumcenter for collinear sites",
        ));
    }
    let first = ExactLine::try_perpendicular_bisector(a, b)?;
    let second = ExactLine::try_perpendicular_bisector(a, c)?;
    try_line_intersection(&first, &second)
}

fn try_radius_squared(center: &ExactPoint, point: XY) -> Result<(ExactDyadic, ExactDyadic)> {
    let px = ExactDyadic::try_from_f64(point.x)?;
    let py = ExactDyadic::try_from_f64(point.y)?;
    let dx = center
        .x()
        .try_clone()?
        .try_subtract(px.try_product(center.w())?)?;
    let dy = center
        .y()
        .try_clone()?
        .try_subtract(py.try_product(center.w())?)?;
    Ok((
        dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?,
        center.w().try_product(center.w())?,
    ))
}

fn try_support_circle(support: &[XY]) -> Result<ExactSupportCircle> {
    match support {
        [point] => {
            let center = try_exact_point(*point)?;
            Ok(ExactSupportCircle {
                center,
                radius_numerator: ExactDyadic::zero(),
                radius_denominator: ExactDyadic::one(),
            })
        },
        [a, b] => {
            let ax = ExactDyadic::try_from_f64(a.x)?;
            let ay = ExactDyadic::try_from_f64(a.y)?;
            let bx = ExactDyadic::try_from_f64(b.x)?;
            let by = ExactDyadic::try_from_f64(b.y)?;
            let center = ExactPoint::homogeneous(
                ax.try_clone()?.try_add(bx.try_clone()?)?,
                ay.try_clone()?.try_add(by.try_clone()?)?,
                ExactDyadic::try_from_f64(2.0)?,
            );
            let (radius_numerator, radius_denominator) = try_radius_squared(&center, *a)?;
            Ok(ExactSupportCircle {
                center,
                radius_numerator,
                radius_denominator,
            })
        },
        [a, b, c] => {
            let center = try_circumcenter_exact(*a, *b, *c)?;
            let (radius_numerator, radius_denominator) = try_radius_squared(&center, *a)?;
            Ok(ExactSupportCircle {
                center,
                radius_numerator,
                radius_denominator,
            })
        },
        _ => Err(GeometryErrorKind::Invalid(
            "minimum circle support must contain one, two, or three points".into(),
        )
        .into()),
    }
}

/// Return the exact homogeneous center and squared-radius ratio of a support.
///
/// The tuple is intentionally made of the arithmetic owner types rather than
/// rounded coordinates. Callers that publish a realization can therefore
/// carry the construction through a unit scale and round only at that
/// publication boundary.
pub(in crate::geometry) fn try_support_circle_components(
    support: &[XY],
) -> Result<(
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
)> {
    let circle = try_support_circle(support)?;
    Ok((
        circle.center.x().try_clone()?,
        circle.center.y().try_clone()?,
        circle.center.w().try_clone()?,
        circle.radius_numerator,
        circle.radius_denominator,
    ))
}

fn try_orient_source_center(a: XY, b: XY, center: &ExactPoint) -> Result<ExactSign> {
    let ax = ExactDyadic::try_from_f64(a.x)?;
    let ay = ExactDyadic::try_from_f64(a.y)?;
    let bx = ExactDyadic::try_from_f64(b.x)?;
    let by = ExactDyadic::try_from_f64(b.y)?;
    let edge_x = bx.try_subtract(ax.try_clone()?)?;
    let edge_y = by.try_subtract(ay.try_clone()?)?;
    let center_x = center
        .x()
        .try_clone()?
        .try_subtract(ax.try_product(center.w())?)?;
    let center_y = center
        .y()
        .try_clone()?
        .try_subtract(ay.try_product(center.w())?)?;
    let determinant = edge_x
        .try_product(&center_y)?
        .try_subtract(edge_y.try_product(&center_x)?)?;
    Ok(determinant.sign().quotient(center.w().sign()))
}

/// Independently certify a support candidate against all original points.
///
/// The final pass deliberately does not rely on the loop's current candidate
/// state.  It checks source containment again and then verifies the closed
/// KKT condition: a one-point support is coincident, a two-point support is a
/// diameter, and a three-point support is non-collinear with its exact
/// circumcenter in the closed support triangle.
pub(in crate::geometry) fn try_certify_support(
    support: &[XY],
    points: impl IntoIterator<Item = XY>,
) -> Result<bool> {
    if support.is_empty() || support.len() > 3 {
        return Err(GeometryErrorKind::Invalid(
            "minimum circle support must contain one, two, or three points".into(),
        )
        .into());
    }
    match support {
        [only] => Ok(points.into_iter().all(|point| same_xy(*only, point))),
        [a, b] => {
            if same_xy(*a, *b) {
                return Ok(false);
            }
            points.into_iter().try_fold(true, |valid, point| {
                if !valid {
                    return Ok(false);
                }
                try_circle_contains(support, point)
            })
        },
        [a, b, c] => {
            let orientation = try_orient2d(*a, *b, *c)?;
            if orientation == ExactSign::Zero {
                return Ok(false);
            }
            let contained = points.into_iter().try_fold(true, |valid, point| {
                if !valid {
                    return Ok(false);
                }
                try_circle_contains(support, point)
            })?;
            if !contained {
                return Ok(false);
            }
            let center = try_circumcenter_exact(*a, *b, *c)?;
            for (left, right) in [(*a, *b), (*b, *c), (*c, *a)] {
                let edge_orientation = try_orient_source_center(left, right, &center)?;
                let inside = match orientation {
                    ExactSign::Positive => edge_orientation != ExactSign::Negative,
                    ExactSign::Negative => edge_orientation != ExactSign::Positive,
                    ExactSign::Zero => unreachable!(),
                };
                if !inside {
                    return Ok(false);
                }
            }
            Ok(true)
        },
        _ => unreachable!(),
    }
}

/// Round the exact center and squared radius of a source support.
///
/// The center and squared radius are each rounded once from their exact
/// constructions.  In particular, the radius is never recovered from the
/// rounded center.
#[cfg(test)]
pub(in crate::geometry) fn round_support_circle(support: &[XY]) -> Result<(XY, f64, bool)> {
    let circle = try_support_circle(support)?;
    let center = circle.center.round_nearest_even()?;
    let radius = round_sqrt_ratio(&circle.radius_numerator, &circle.radius_denominator)?;
    Ok((center, radius, circle.radius_numerator.is_zero()))
}

pub(super) fn try_incircle(a: XY, b: XY, c: XY, d: XY) -> Result<ExactSign> {
    if let Some(sign) = incircle_float(a, b, c, d) {
        return Ok(sign);
    }
    if let Some(sign) = incircle_lattice(a, b, c, d) {
        return Ok(sign);
    }
    if let Some(sign) = incircle_filtered(a, b, c, d) {
        return Ok(sign);
    }
    try_incircle_dyadic(a, b, c, d)
}

#[cfg(test)]
fn incircle_dyadic(a: XY, b: XY, c: XY, d: XY) -> ExactSign {
    let (adx, ady) = (difference(a.x, d.x), difference(a.y, d.y));
    let (bdx, bdy) = (difference(b.x, d.x), difference(b.y, d.y));
    let (cdx, cdy) = (difference(c.x, d.x), difference(c.y, d.y));
    let alift = adx.square().add(ady.square());
    let blift = bdx.square().add(bdy.square());
    let clift = cdx.square().add(cdy.square());
    let bcdet = bdx.product(&cdy).subtract(cdx.product(&bdy));
    let cadet = cdx.product(&ady).subtract(adx.product(&cdy));
    let abdet = adx.product(&bdy).subtract(bdx.product(&ady));
    alift
        .product(&bcdet)
        .add(blift.product(&cadet))
        .add(clift.product(&abdet))
        .sign()
}

fn try_incircle_dyadic(a: XY, b: XY, c: XY, d: XY) -> Result<ExactSign> {
    let (adx, ady) = (try_difference(a.x, d.x)?, try_difference(a.y, d.y)?);
    let (bdx, bdy) = (try_difference(b.x, d.x)?, try_difference(b.y, d.y)?);
    let (cdx, cdy) = (try_difference(c.x, d.x)?, try_difference(c.y, d.y)?);
    let alift = adx.try_product(&adx)?.try_add(ady.try_product(&ady)?)?;
    let blift = bdx.try_product(&bdx)?.try_add(bdy.try_product(&bdy)?)?;
    let clift = cdx.try_product(&cdx)?.try_add(cdy.try_product(&cdy)?)?;
    let bcdet = bdx
        .try_product(&cdy)?
        .try_subtract(cdx.try_product(&bdy)?)?;
    let cadet = cdx
        .try_product(&ady)?
        .try_subtract(adx.try_product(&cdy)?)?;
    let abdet = adx
        .try_product(&bdy)?
        .try_subtract(bdx.try_product(&ady)?)?;
    alift
        .try_product(&bcdet)?
        .try_add(blift.try_product(&cadet)?)?
        .try_add(clift.try_product(&abdet)?)
        .map(|determinant| determinant.sign())
}

// ---------------------------------------------------------------------------
// Certified interval filter.
//
// Every `ExactPoint` carries an OUTWARD-ROUNDED binary64 interval that PROVABLY
// CONTAINS its exact rational coordinate. A comparison whose two intervals are
// strictly separated is therefore decided; anything else falls through to the
// unchanged exact comparator. The filter can only DECLINE, never decide wrongly:
// it is the same shape as `rect_bound` exclusion — a bound that contains the
// true object may establish a verdict, never a guess.
//
// Containment chain, per coordinate (all steps outward-rounded):
//   1. `leading_window` splits a magnitude `M * 2^exp` into `(m, e)` with
//      m in [2^63, 2^64) and  m*2^e <= M*2^exp <= (m+1)*2^e.   (exact by
//      construction: `m` is the truncated top-64-bit window and `e` restores
//      the discarded bit positions.)
//   2. `n/d` therefore lies in  [ m_n/(m_d+1) * 2^(e_n-e_d),
//                                 (m_n+1)/m_d * 2^(e_n-e_d) ].
//   3. Each u64 -> f64 conversion is widened by one `next_down`/`next_up`.
//      Both mantissas are >= 2^63, so one ulp there is >= 2^11 = 2048 > 1,
//      which is what makes `next_up(m as f64) >= m + 1` sound.
//   4. The division and the power-of-two rescale are each widened outward
//      again; overflow saturates to +/-INFINITY and underflow to 0 / MIN_POSITIVE
//      on the side that keeps containment.
//   Anything not provable (denominator zero, exponent arithmetic overflow)
//   yields the universal interval, which decides nothing.
#[derive(Clone, Copy, Debug)]
pub(in crate::geometry) struct Interval {
    pub(in crate::geometry) lo: f64,
    pub(in crate::geometry) hi: f64,
}

impl Interval {
    const UNIVERSAL: Self = Self {
        lo: f64::NEG_INFINITY,
        hi: f64::INFINITY,
    };

    /// `Some(Less)` / `Some(Greater)` only when the two intervals are strictly
    /// separated, which proves the order of the contained exact values.
    #[expect(
        clippy::float_cmp,
        reason = "an exact interval is one double, and two of them are equal exactly when their doubles are"
    )]
    fn compare(self, other: Self) -> Option<Ordering> {
        if self.hi < other.lo {
            Some(Ordering::Less)
        } else if self.lo > other.hi {
            Some(Ordering::Greater)
        } else if self.lo == self.hi && other.lo == other.hi && self.lo == other.lo {
            // Two exact intervals on one double: equal, certified. A frame
            // hit's side coordinate against that side's corner is the common
            // case, and it must not materialise the hit's exact coordinates.
            Some(Ordering::Equal)
        } else {
            None
        }
    }

    pub(in crate::geometry) const fn exact(value: f64) -> Self {
        Self {
            lo: value,
            hi: value,
        }
    }

    /// Outward-rounded sum.
    pub(in crate::geometry) fn add(self, other: Self) -> Self {
        Self {
            lo: (self.lo + other.lo).next_down(),
            hi: (self.hi + other.hi).next_up(),
        }
    }

    pub(in crate::geometry) const fn is_finite(self) -> bool {
        self.lo.is_finite() && self.hi.is_finite()
    }

    /// Outward-rounded difference. `next_down`/`next_up` absorb the rounding of
    /// the binary64 subtraction, so the result still contains every difference
    /// of contained values.
    pub(in crate::geometry) fn sub(self, other: Self) -> Self {
        Self {
            lo: (self.lo - other.hi).next_down(),
            hi: (self.hi - other.lo).next_up(),
        }
    }

    /// Outward-rounded product. Both operands are finite here (checked by the
    /// caller), so no `0 * inf` NaN can arise; the four corner products bound
    /// the product of any contained pair.
    pub(in crate::geometry) fn mul(self, other: Self) -> Self {
        let corners = [
            self.lo * other.lo,
            self.lo * other.hi,
            self.hi * other.lo,
            self.hi * other.hi,
        ];
        let mut lo = corners[0];
        let mut hi = corners[0];
        for &corner in &corners[1..] {
            lo = lo.min(corner);
            hi = hi.max(corner);
        }
        Self {
            lo: lo.next_down(),
            hi: hi.next_up(),
        }
    }

    /// The sign of every contained value, when the interval excludes zero.
    pub(in crate::geometry) fn sign(self) -> Option<ExactSign> {
        if self.lo > 0.0 {
            Some(ExactSign::Positive)
        } else if self.hi < 0.0 {
            Some(ExactSign::Negative)
        } else {
            None
        }
    }

    fn disjoint_from(self, other: Self) -> bool {
        self.hi < other.lo || self.lo > other.hi
    }
}

/// Split `|value|` into `(m, e)` with `m` in `[2^63, 2^64)` and
/// `m * 2^e <= |value| <= (m + 1) * 2^e`.
fn leading_window(value: &ExactDyadic) -> Option<(u64, i64)> {
    let (&high, rest) = value.limbs.split_last()?;
    let next = rest.last().copied().unwrap_or(0);
    let high_bits = 64 - high.leading_zeros();
    let window = (u128::from(high) << 64) | u128::from(next);
    let mantissa = (window >> high_bits) as u64;
    let bits = i64::try_from(rest.len())
        .ok()?
        .checked_mul(64)?
        .checked_add(i64::from(high_bits))?;
    Some((mantissa, value.exponent.checked_add(bits)?.checked_sub(64)?))
}

/// Multiply by `2^shift`, rounding outward in the requested direction and
/// saturating so containment survives overflow and underflow.
fn scale_pow2(mut value: f64, mut shift: i64, upward: bool) -> f64 {
    debug_assert!(value >= 0.0);
    if value == 0.0 {
        return 0.0;
    }
    while shift != 0 {
        let step = shift.clamp(-512, 512);
        value *= 2.0_f64.powi(step as i32);
        shift -= step;
        if value.is_infinite() {
            return if upward { f64::INFINITY } else { f64::MAX };
        }
        if value == 0.0 {
            return if upward { f64::MIN_POSITIVE } else { 0.0 };
        }
    }
    // The scale itself is exact for normal results; widen anyway so subnormal
    // rounding cannot escape the bound.
    if upward {
        value.next_up()
    } else {
        value.next_down().max(0.0)
    }
}

/// An interval provably containing `numerator / denominator`.
fn ratio_interval(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Interval {
    if numerator.is_zero() {
        return Interval { lo: 0.0, hi: 0.0 };
    }
    if denominator.is_zero() {
        return Interval::UNIVERSAL;
    }
    let (Some((mantissa_n, exponent_n)), Some((mantissa_d, exponent_d))) =
        (leading_window(numerator), leading_window(denominator))
    else {
        return Interval::UNIVERSAL;
    };
    let Some(shift) = exponent_n.checked_sub(exponent_d) else {
        return Interval::UNIVERSAL;
    };
    // m >= 2^63, so one ulp at that magnitude is >= 2^11: a single next_up
    // dominates the "+1", and a single next_down stays strictly below m.
    let low_numerator = (mantissa_n as f64).next_down();
    let high_numerator = (mantissa_n as f64).next_up();
    let low_denominator = (mantissa_d as f64).next_down();
    let high_denominator = (mantissa_d as f64).next_up();
    let low = (low_numerator / high_denominator).next_down().max(0.0);
    let high = (high_numerator / low_denominator).next_up();
    let magnitude = Interval {
        lo: scale_pow2(low, shift, false),
        hi: scale_pow2(high, shift, true),
    };
    if magnitude.lo.is_nan() || magnitude.hi.is_nan() {
        return Interval::UNIVERSAL;
    }
    if numerator.negative == denominator.negative {
        magnitude
    } else {
        Interval {
            lo: -magnitude.hi,
            hi: -magnitude.lo,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ExactPoint {
    coords: Coords,
    /// Certified outward-rounded bounds on `x/w` and `y/w`.
    bounds: [Interval; 2],
}

/// Exact homogeneous coordinates.
#[derive(Clone, Debug)]
struct Homogeneous {
    x: ExactDyadic,
    y: ExactDyadic,
    w: ExactDyadic,
}

/// Where a point's exact coordinates come from. A certified circumcenter
/// carries its sites and its certified nearest-even rounding, and builds the
/// dyadic coordinates only for a comparison its bounds cannot decide.
#[derive(Clone, Debug)]
enum Coords {
    Exact(Homogeneous),
    /// A stored binary64 point: exact as it stands, its dyadic form built
    /// only for a comparison the exact interval cannot decide.
    Double {
        xy: XY,
        exact: std::cell::OnceCell<Box<Homogeneous>>,
    },
    Certified {
        recipe: Recipe,
        rounded: XY,
        exact: std::cell::OnceCell<Box<Homogeneous>>,
    },
}

/// A bisector meeting a frame side: the exact construction decides a
/// declined hit, a parallel bisector has none, otherwise the lazy point.
#[derive(Debug)]
pub(super) enum FrameHit {
    Declined,
    Parallel,
    Point(ExactPoint),
}

/// The exact construction a certified point stands for.
#[derive(Clone, Copy, Debug)]
enum Recipe {
    Circumcenter([XY; 3]),
    /// The perpendicular bisector of the two sites meeting the frame side
    /// through the two corners.
    BisectorHit {
        sites: [XY; 2],
        side: [XY; 2],
    },
}

impl Recipe {
    /// The same geometric construction, up to the order of its sites and of a
    /// side's two corners: a bisector is symmetric in its sites, a
    /// circumcenter in its three, so the exact points are equal.
    fn same_construction(&self, other: &Self) -> bool {
        let same_pair = |a: [XY; 2], b: [XY; 2]| {
            (a[0] == b[0] && a[1] == b[1]) || (a[0] == b[1] && a[1] == b[0])
        };
        match (self, other) {
            (Self::Circumcenter(a), Self::Circumcenter(b)) => {
                let key = |sites: &[XY; 3]| {
                    let mut keys = sites.map(|xy| (xy.x.to_bits(), xy.y.to_bits()));
                    keys.sort_unstable();
                    keys
                };
                key(a) == key(b)
            },
            (
                Self::BisectorHit { sites: a, side: sa },
                Self::BisectorHit { sites: b, side: sb },
            ) => same_pair(*a, *b) && same_pair(*sa, *sb),
            _ => false,
        }
    }
}

impl ExactPoint {
    /// The ONLY constructor. Every homogeneous triple gets its certified
    /// interval here, so a point can never exist without its filter bound.
    fn homogeneous(x: ExactDyadic, y: ExactDyadic, w: ExactDyadic) -> Self {
        let bounds = [ratio_interval(&x, &w), ratio_interval(&y, &w)];
        if VERIFY_INTERVALS {
            verify_interval(&x, &w, bounds[0]);
            verify_interval(&y, &w, bounds[1]);
        }
        Self {
            coords: Coords::Exact(Homogeneous { x, y, w }),
            bounds,
        }
    }

    /// A circumcenter whose nearest-even rounding is certified: the exact
    /// value lies within half an ulp of `center.rounded`, so within its two
    /// binary64 neighbours, which are the bounds every comparison reads first.
    fn certified_circumcenter(
        sites: [XY; 3],
        center: &super::circumcenter::CertifiedCenter,
    ) -> Self {
        Self {
            coords: Coords::Certified {
                recipe: Recipe::Circumcenter(sites),
                rounded: center.rounded,
                exact: std::cell::OnceCell::new(),
            },
            bounds: [
                Interval {
                    lo: center.bounds[0].0,
                    hi: center.bounds[0].1,
                },
                Interval {
                    lo: center.bounds[1].0,
                    hi: center.bounds[1].1,
                },
            ],
        }
    }

    fn coords(&self) -> &Homogeneous {
        match &self.coords {
            Coords::Exact(coords) => coords,
            Coords::Double { xy, exact } => exact.get_or_init(|| {
                Box::new(Homogeneous {
                    x: ExactDyadic::from_f64(xy.x),
                    y: ExactDyadic::from_f64(xy.y),
                    w: ExactDyadic::one(),
                })
            }),
            Coords::Certified { recipe, exact, .. } => exact.get_or_init(|| {
                let coords = match *recipe {
                    Recipe::Circumcenter(sites) => intersection_homogeneous(
                        &ExactLine::perpendicular_bisector(sites[0], sites[1]),
                        &ExactLine::perpendicular_bisector(sites[0], sites[2]),
                    ),
                    Recipe::BisectorHit { sites, side } => intersection_homogeneous(
                        &ExactLine::perpendicular_bisector(sites[0], sites[1]),
                        &ExactLine::through_points(
                            &Self::from_xy(side[0]),
                            &Self::from_xy(side[1]),
                        )
                        .expect("a frame side has two distinct corners"),
                    ),
                };
                Box::new(coords)
            }),
        }
    }

    /// The stored double behind a point made from one, `None` for any other
    /// construction.
    pub(super) fn as_double(&self) -> Option<XY> {
        match &self.coords {
            Coords::Double { xy, .. } => Some(*xy),
            Coords::Exact(_) | Coords::Certified { .. } => None,
        }
    }

    /// Where the perpendicular bisector of `sites` meets the frame side
    /// through `side`, certified.
    #[expect(
        clippy::float_cmp,
        reason = "an axis-aligned side's two corners share a coordinate exactly, by construction"
    )]
    pub(super) fn certified_bisector_hit(sites: [XY; 2], side: [XY; 2]) -> FrameHit {
        use super::circumcenter::{BisectorHit, SideAxis, certified_bisector_hit};
        let (axis, value) = if side[0].x == side[1].x {
            (SideAxis::FixedX, side[0].x)
        } else if side[0].y == side[1].y {
            (SideAxis::FixedY, side[0].y)
        } else {
            return FrameHit::Declined;
        };
        let free = match certified_bisector_hit(sites[0], sites[1], axis, value) {
            BisectorHit::Declined => return FrameHit::Declined,
            BisectorHit::Parallel => return FrameHit::Parallel,
            BisectorHit::Free(free) => free,
        };
        let rounded = match axis {
            SideAxis::FixedX => XY::new(value, free),
            SideAxis::FixedY => XY::new(free, value),
        };
        let bounds = match axis {
            SideAxis::FixedX => [Interval::exact(value), Interval {
                lo: free.next_down(),
                hi: free.next_up(),
            }],
            SideAxis::FixedY => [
                Interval {
                    lo: free.next_down(),
                    hi: free.next_up(),
                },
                Interval::exact(value),
            ],
        };
        FrameHit::Point(Self {
            coords: Coords::Certified {
                recipe: Recipe::BisectorHit { sites, side },
                rounded,
                exact: std::cell::OnceCell::new(),
            },
            bounds,
        })
    }

    pub(super) fn x(&self) -> &ExactDyadic {
        &self.coords().x
    }

    pub(super) fn y(&self) -> &ExactDyadic {
        &self.coords().y
    }

    pub(super) fn w(&self) -> &ExactDyadic {
        &self.coords().w
    }
}

/// Development self-check: prove `lo <= numerator/denominator <= hi` with the
/// exact comparator when interval verification is enabled.
const VERIFY_INTERVALS: bool = false;

fn verify_interval(numerator: &ExactDyadic, denominator: &ExactDyadic, bounds: Interval) {
    if denominator.is_zero() {
        return;
    }
    let one = ExactDyadic::one();
    if bounds.lo.is_finite() {
        assert!(
            compare_ratios(
                &ExactDyadic::from_f64(bounds.lo),
                &one,
                numerator,
                denominator
            ) != Ordering::Greater,
            "interval lower bound {} exceeds the exact value",
            bounds.lo
        );
    }
    if bounds.hi.is_finite() {
        assert!(
            compare_ratios(
                &ExactDyadic::from_f64(bounds.hi),
                &one,
                numerator,
                denominator
            ) != Ordering::Less,
            "interval upper bound {} is below the exact value",
            bounds.hi
        );
    }
}

impl ExactPoint {
    pub(super) fn from_xy(point: XY) -> Self {
        Self {
            coords: Coords::Double {
                xy: point,
                exact: std::cell::OnceCell::new(),
            },
            bounds: [Interval::exact(point.x), Interval::exact(point.y)],
        }
    }

    pub(super) fn compare_lex(&self, other: &Self) -> Ordering {
        self.compare_x(other).then_with(|| self.compare_y(other))
    }

    pub(super) fn same_position(&self, other: &Self) -> bool {
        if let (Coords::Certified { recipe: left, .. }, Coords::Certified { recipe: right, .. }) =
            (&self.coords, &other.coords)
            && left.same_construction(right)
        {
            // One construction reached from two cells: the same exact point,
            // without materialising either side.
            return true;
        }
        !self.separated_from(other)
            && self.compare_x(other).is_eq()
            && self.compare_y(other).is_eq()
    }

    /// Whether the point is finite (nonzero weight). A certified circumcenter
    /// is finite by construction, so this never materializes its coordinates.
    pub(super) fn is_finite(&self) -> bool {
        match &self.coords {
            Coords::Certified { .. } | Coords::Double { .. } => true,
            Coords::Exact(coords) => !coords.w.is_zero(),
        }
    }

    pub(super) fn midpoint(&self, other: &Self) -> Self {
        let denominator = self
            .w()
            .product(other.w())
            .product(&ExactDyadic::from_f64(2.0));
        Self::homogeneous(
            self.x().product(other.w()).add(other.x().product(self.w())),
            self.y().product(other.w()).add(other.y().product(self.w())),
            denominator,
        )
    }
    pub(super) fn compare_x(&self, other: &Self) -> Ordering {
        self.bounds[0]
            .compare(other.bounds[0])
            .unwrap_or_else(|| compare_ratios(self.x(), self.w(), other.x(), other.w()))
    }

    pub(super) fn compare_y(&self, other: &Self) -> Ordering {
        self.bounds[1]
            .compare(other.bounds[1])
            .unwrap_or_else(|| compare_ratios(self.y(), self.w(), other.y(), other.w()))
    }

    /// The certified outward bounds on `x` and `y`.
    pub(super) fn bounds(&self) -> [Interval; 2] {
        self.bounds
    }

    /// Certified positive: both coordinates lie strictly inside
    /// `[minx, maxx, miny, maxy]`, decided on the bounds alone.
    pub(super) fn strictly_inside(&self, bounds: [f64; 4]) -> bool {
        self.bounds[0].lo > bounds[0]
            && self.bounds[0].hi < bounds[1]
            && self.bounds[1].lo > bounds[2]
            && self.bounds[1].hi < bounds[3]
    }

    /// Certified negative: two points whose x- or y-intervals are strictly
    /// separated cannot be the same position.
    pub(super) fn separated_from(&self, other: &Self) -> bool {
        self.bounds[0].disjoint_from(other.bounds[0])
            || self.bounds[1].disjoint_from(other.bounds[1])
    }

    /// Materialize one shared exact vertex exactly once. Construction code must
    /// intern the returned coordinate by vertex identity and reuse it for every
    /// incident half-edge. Under gometry's topological-snap contract a clip event may
    /// differ from the source clip at the ULP level when exact incidence has no
    /// binary64 representation; validity, event order, empty differences, and
    /// interior disjointness remain mandatory, while bit-exact `equals` does
    /// not. Recomputing a shared event per cell can reintroduce cracks between
    /// adjacent cells.
    pub(super) fn round_nearest_even(&self) -> Result<XY> {
        match &self.coords {
            Coords::Certified { rounded, .. } => return Ok(*rounded),
            Coords::Double { xy, .. } => return Ok(*xy),
            Coords::Exact(_) => {},
        }
        if !self.is_finite() {
            return Err(GeometryErrorKind::voronoi("exact vertex lies at infinity"));
        }
        Ok(XY::new(
            round_ratio(self.x(), self.w())?,
            round_ratio(self.y(), self.w())?,
        ))
    }
}

fn on_segment(a: &ExactPoint, b: &ExactPoint, point: &ExactPoint) -> bool {
    let between = |pa: Ordering, pb: Ordering| {
        (pa != Ordering::Less && pb != Ordering::Greater)
            || (pb != Ordering::Less && pa != Ordering::Greater)
    };
    orient_points(a, b, point) == ExactSign::Zero
        && between(point.compare_x(a), point.compare_x(b))
        && between(point.compare_y(a), point.compare_y(b))
}

pub(super) fn compare_along(
    start: &ExactPoint,
    end: &ExactPoint,
    left: &ExactPoint,
    right: &ExactPoint,
) -> Ordering {
    let (axis_order, direction) = if start.compare_x(end).is_ne() {
        (left.compare_x(right), start.compare_x(end))
    } else {
        (left.compare_y(right), start.compare_y(end))
    };
    if direction.is_gt() {
        axis_order.reverse()
    } else {
        axis_order
    }
}

pub(super) fn segment_intersection(
    a0: &ExactPoint,
    a1: &ExactPoint,
    b0: &ExactPoint,
    b1: &ExactPoint,
) -> SegmentIntersection {
    // A homogeneous intersection of parallel support lines has `w == 0`.
    // It is not a point on either finite segment; admitting it as a degenerate
    // segment endpoint can place an event beyond both stored endpoints.
    if !a0.is_finite() || !a1.is_finite() || !b0.is_finite() || !b1.is_finite() {
        return SegmentIntersection::None;
    }
    let [o0, o1, o2, o3] = [
        orient_points(a0, a1, b0),
        orient_points(a0, a1, b1),
        orient_points(b0, b1, a0),
        orient_points(b0, b1, a1),
    ];
    if [o0, o1, o2, o3].iter().all(|sign| *sign == ExactSign::Zero) {
        let (b_start, b_end) = if compare_along(a0, a1, b0, b1).is_gt() {
            (b1, b0)
        } else {
            (b0, b1)
        };
        let start = if compare_along(a0, a1, a0, b_start).is_gt() {
            a0
        } else {
            b_start
        };
        let end = if compare_along(a0, a1, a1, b_end).is_lt() {
            a1
        } else {
            b_end
        };
        return match compare_along(a0, a1, start, end) {
            Ordering::Greater => SegmentIntersection::None,
            Ordering::Equal => SegmentIntersection::Point(start.clone()),
            Ordering::Less => SegmentIntersection::Overlap {
                start: start.clone(),
                end: end.clone(),
            },
        };
    }
    if o0 == ExactSign::Zero && on_segment(a0, a1, b0) {
        return SegmentIntersection::Point(b0.clone());
    }
    if o1 == ExactSign::Zero && on_segment(a0, a1, b1) {
        return SegmentIntersection::Point(b1.clone());
    }
    if o2 == ExactSign::Zero && on_segment(b0, b1, a0) {
        return SegmentIntersection::Point(a0.clone());
    }
    if o3 == ExactSign::Zero && on_segment(b0, b1, a1) {
        return SegmentIntersection::Point(a1.clone());
    }
    if o0.as_i8() * o1.as_i8() < 0 && o2.as_i8() * o3.as_i8() < 0 {
        return SegmentIntersection::Point(line_intersection(
            &ExactLine::through_points(a0, a1).expect("distinct segment"),
            &ExactLine::through_points(b0, b1).expect("distinct segment"),
        ));
    }
    SegmentIntersection::None
}

pub(super) fn angle_ccw_cmp(
    origin: &ExactPoint,
    left: &ExactPoint,
    right: &ExactPoint,
) -> Result<Ordering> {
    let half = |target: &ExactPoint| {
        let dy = target
            .y()
            .product(origin.w())
            .subtract(origin.y().product(target.w()));
        let dx = target
            .x()
            .product(origin.w())
            .subtract(origin.x().product(target.w()));
        let denominator_sign = target.w().sign().as_i8() * origin.w().sign().as_i8();
        let signed = |value: &ExactDyadic| {
            let sign = value.sign();
            if denominator_sign < 0 {
                match sign {
                    ExactSign::Negative => ExactSign::Positive,
                    ExactSign::Positive => ExactSign::Negative,
                    ExactSign::Zero => ExactSign::Zero,
                }
            } else {
                sign
            }
        };
        signed(&dy) == ExactSign::Positive
            || (signed(&dy) == ExactSign::Zero && signed(&dx) != ExactSign::Negative)
    };
    match (half(left), half(right)) {
        (true, false) => Ok(Ordering::Less),
        (false, true) => Ok(Ordering::Greater),
        _ => match orient_points(origin, left, right) {
            ExactSign::Positive => Ok(Ordering::Less),
            ExactSign::Negative => Ok(Ordering::Greater),
            ExactSign::Zero => Err(GeometryErrorKind::voronoi(
                "exact angular tie proves an un-noded overlap or T-junction",
            )
            .into()),
        },
    }
}

/// The shoelace sign in interval arithmetic. Every term is the cross
/// product of two affine vertices, each inside its certified interval, so the
/// accumulated interval contains the exact signed area; a sign is returned only
/// when it excludes zero. Declining falls through to the exact accumulation,
/// which is also the only path that can report the zero-orientation error.
fn cycle_orientation_filtered(open_cycle: &[ExactPoint]) -> Option<ExactSign> {
    let mut total = Interval::exact(0.0);
    for index in 0..open_cycle.len() {
        let left = &open_cycle[index];
        let right = &open_cycle[(index + 1) % open_cycle.len()];
        if !left.is_finite() || !right.is_finite() {
            return None;
        }
        let term = left.bounds[0]
            .mul(right.bounds[1])
            .sub(left.bounds[1].mul(right.bounds[0]));
        if !term.is_finite() {
            return None;
        }
        total = total.add(term);
        if !total.is_finite() {
            return None;
        }
    }
    total.sign()
}

pub(super) fn cycle_orientation(open_cycle: &[ExactPoint]) -> Result<ExactSign> {
    if open_cycle.len() < 3 {
        return Err(GeometryErrorKind::voronoi("exact cycle has fewer than three vertices").into());
    }
    if let Some(sign) = cycle_orientation_filtered(open_cycle) {
        if VERIFY_INTERVALS {
            let exact = cycle_orientation_exact(open_cycle)?;
            if sign != exact {
                return Err(GeometryErrorKind::voronoi(
                    "filtered cycle orientation disagrees with exact orientation",
                )
                .into());
            }
        }
        return Ok(sign);
    }
    cycle_orientation_exact(open_cycle)
}

fn cycle_orientation_exact(open_cycle: &[ExactPoint]) -> Result<ExactSign> {
    let mut numerator = ExactDyadic::zero();
    let mut denominator = ExactDyadic::one();
    for index in 0..open_cycle.len() {
        let left = &open_cycle[index];
        let right = &open_cycle[(index + 1) % open_cycle.len()];
        let term_n = left
            .x()
            .product(right.y())
            .subtract(left.y().product(right.x()));
        let term_d = left.w().product(right.w());
        numerator = numerator.product(&term_d).add(term_n.product(&denominator));
        denominator = denominator.product(&term_d);
    }
    match numerator.sign() {
        ExactSign::Zero => {
            Err(GeometryErrorKind::voronoi("exact cycle has zero orientation").into())
        },
        sign if denominator.sign() == ExactSign::Negative => Ok(match sign {
            ExactSign::Negative => ExactSign::Positive,
            ExactSign::Positive => ExactSign::Negative,
            ExactSign::Zero => unreachable!(),
        }),
        sign => Ok(sign),
    }
}

pub(super) fn point_in_cycle(open_cycle: &[ExactPoint], point: &ExactPoint) -> PointInCycle {
    let mut winding = 0_i32;
    for index in 0..open_cycle.len() {
        let start = &open_cycle[index];
        let end = &open_cycle[(index + 1) % open_cycle.len()];
        if on_segment(start, end, point) {
            return PointInCycle::Boundary;
        }
        let sy = start.compare_y(point);
        let ey = end.compare_y(point);
        let orientation = orient_points(start, end, point);
        if sy != Ordering::Greater && ey == Ordering::Greater && orientation == ExactSign::Positive
        {
            winding += 1;
        } else if ey != Ordering::Greater
            && sy == Ordering::Greater
            && orientation == ExactSign::Negative
        {
            winding -= 1;
        }
    }
    if winding == 0 {
        PointInCycle::Outside
    } else {
        PointInCycle::Inside
    }
}

/// Filter for "which stored site is nearer this exact point".
/// The exact fallback evaluates the stored sites' perpendicular bisector at
/// the homogeneous point; its signed line product accounts for the sign of
/// the point weight.
fn squared_distance_cmp_point_filtered(
    origin: &ExactPoint,
    left: XY,
    right: XY,
) -> Option<Ordering> {
    if !origin.is_finite() {
        return None;
    }
    let squared = |point: XY| {
        let dx = Interval::exact(point.x).sub(origin.bounds[0]);
        let dy = Interval::exact(point.y).sub(origin.bounds[1]);
        dx.mul(dx).add(dy.mul(dy))
    };
    let difference = squared(left).sub(squared(right));
    if !difference.is_finite() {
        return None;
    }
    match difference.sign()? {
        ExactSign::Negative => Some(Ordering::Less),
        ExactSign::Positive => Some(Ordering::Greater),
        ExactSign::Zero => None,
    }
}

pub(super) fn squared_distance_cmp_point(origin: &ExactPoint, left: XY, right: XY) -> Ordering {
    if let Some(order) = squared_distance_cmp_point_filtered(origin, left, right) {
        if VERIFY_INTERVALS {
            assert_eq!(
                order,
                squared_distance_cmp_point_exact(origin, left, right),
                "filtered squared-distance comparison disagrees"
            );
        }
        return order;
    }
    squared_distance_cmp_point_exact(origin, left, right)
}

fn squared_distance_cmp_point_exact(origin: &ExactPoint, left: XY, right: XY) -> Ordering {
    debug_assert!(origin.is_finite());
    match signed_line_product(&ExactLine::perpendicular_bisector(left, right), origin) {
        ExactSign::Negative => Ordering::Less,
        ExactSign::Zero => Ordering::Equal,
        ExactSign::Positive => Ordering::Greater,
    }
}

/// A private frame whose four corners are stored binary64 (`w == 1`).
///
/// The frame is private scaffolding. The only property the pipeline needs from
/// it is that it strictly encloses every input point. That property is
/// established here by the same exact comparator the rest of the module uses,
/// so the cheap construction is CERTIFIED, not assumed. `None` means no
/// admissible binary64 box exists and the caller must fall back to the exact
/// degree-8 construction.
#[expect(
    clippy::needless_pass_by_value,
    reason = "the iterator is re-walked once per widening round"
)]
pub(super) fn enclosing_frame_binary64<'a>(
    points: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> Option<[ExactPoint; 4]> {
    // Seeding only. A wrong estimate costs extra certified widening rounds,
    // never a wrong frame.
    let approx = |numerator: &ExactDyadic, denominator: &ExactDyadic| -> Option<f64> {
        let magnitude = approximate_ratio(numerator, denominator)?;
        let negative = numerator.negative != denominator.negative;
        Some(if negative { -magnitude } else { magnitude })
    };
    let mut lo = [f64::INFINITY; 2];
    let mut hi = [f64::NEG_INFINITY; 2];
    for point in points.clone() {
        if !point.is_finite() {
            return None;
        }
        // The certified bounds are the estimate whenever they are finite; the
        // exact quotient is read only for a point whose bounds are not.
        let coordinates = if point.bounds[0].is_finite() && point.bounds[1].is_finite() {
            [point.bounds[0].hi, point.bounds[1].hi]
        } else {
            [approx(point.x(), point.w())?, approx(point.y(), point.w())?]
        };
        for axis in 0..2 {
            if !coordinates[axis].is_finite() {
                return None;
            }
            lo[axis] = lo[axis].min(coordinates[axis]);
            hi[axis] = hi[axis].max(coordinates[axis]);
        }
    }
    let span = (hi[0] - lo[0]).max(hi[1] - lo[1]).max(1.0);
    if !span.is_finite() {
        return None;
    }
    let mut box_lo = [lo[0] - span, lo[1] - span];
    let mut box_hi = [hi[0] + span, hi[1] + span];
    // Certify strict containment exactly; widen by one ulp whenever an input
    // lands on or outside a candidate bound. Bounded, so this stays a
    // certificate rather than an unbounded search.
    for _ in 0..64 {
        for axis in 0..2 {
            if !box_lo[axis].is_finite() || !box_hi[axis].is_finite() {
                return None;
            }
        }
        let low = ExactPoint::from_xy(XY::new(box_lo[0], box_lo[1]));
        let high = ExactPoint::from_xy(XY::new(box_hi[0], box_hi[1]));
        let mut widened = false;
        for point in points.clone() {
            for axis in 0..2 {
                let (below, above) = if axis == 0 {
                    (point.compare_x(&low), point.compare_x(&high))
                } else {
                    (point.compare_y(&low), point.compare_y(&high))
                };
                if !below.is_gt() {
                    box_lo[axis] = box_lo[axis].next_down();
                    widened = true;
                }
                if !above.is_lt() {
                    box_hi[axis] = box_hi[axis].next_up();
                    widened = true;
                }
            }
        }
        if !widened {
            return Some([
                ExactPoint::from_xy(XY::new(box_lo[0], box_lo[1])),
                ExactPoint::from_xy(XY::new(box_hi[0], box_lo[1])),
                ExactPoint::from_xy(XY::new(box_hi[0], box_hi[1])),
                ExactPoint::from_xy(XY::new(box_lo[0], box_hi[1])),
            ]);
        }
    }
    None
}

pub(super) fn enclosing_frame(points: &[ExactPoint]) -> Result<[ExactPoint; 4]> {
    let first = points
        .first()
        .ok_or_else(|| GeometryErrorKind::voronoi("empty exact frame input"))?;
    let (mut min_x, mut max_x, mut min_y, mut max_y) =
        (first.clone(), first.clone(), first.clone(), first.clone());
    for point in &points[1..] {
        if point.compare_x(&min_x).is_lt() {
            min_x = point.clone();
        }
        if point.compare_x(&max_x).is_gt() {
            max_x = point.clone();
        }
        if point.compare_y(&min_y).is_lt() {
            min_y = point.clone();
        }
        if point.compare_y(&max_y).is_gt() {
            max_y = point.clone();
        }
    }
    let span_x_n = max_x
        .x()
        .product(min_x.w())
        .subtract(min_x.x().product(max_x.w()));
    let span_x_d = max_x.w().product(min_x.w());
    let span_y_n = max_y
        .y()
        .product(min_y.w())
        .subtract(min_y.y().product(max_y.w()));
    let span_y_d = max_y.w().product(min_y.w());
    let (pad_n, pad_d) = if compare_ratios(&span_x_n, &span_x_d, &span_y_n, &span_y_d).is_gt() {
        (span_x_n, span_x_d)
    } else {
        (span_y_n, span_y_d)
    };
    if pad_n.is_zero() {
        return Err(GeometryErrorKind::voronoi("exact frame input has zero span").into());
    }
    let coordinate = |base_n: &ExactDyadic, base_d: &ExactDyadic, add: bool| {
        ExactPoint::homogeneous(
            if add {
                base_n.product(&pad_d).add(pad_n.product(base_d))
            } else {
                base_n.product(&pad_d).subtract(pad_n.product(base_d))
            },
            ExactDyadic::zero(),
            base_d.product(&pad_d),
        )
    };
    let lx = coordinate(min_x.x(), min_x.w(), false);
    let ux = coordinate(max_x.x(), max_x.w(), true);
    let ly = coordinate(min_y.y(), min_y.w(), false);
    let uy = coordinate(max_y.y(), max_y.w(), true);
    let make = |x: &ExactPoint, y: &ExactPoint| {
        ExactPoint::homogeneous(
            x.x().product(y.w()),
            y.x().product(x.w()),
            x.w().product(y.w()),
        )
    };
    Ok([
        make(&lx, &ly),
        make(&ux, &ly),
        make(&ux, &uy),
        make(&lx, &uy),
    ])
}

/// Tight axis-aligned bounds of exact points: no pad, no enclosing super-frame.
pub(super) fn axis_aligned_bounds<'a, I>(points: I) -> Result<[ExactPoint; 4]>
where
    I: IntoIterator<Item = &'a ExactPoint>,
{
    let mut points = points.into_iter();
    let first = points
        .next()
        .ok_or_else(|| GeometryErrorKind::voronoi("empty exact frame input"))?;
    let (mut min_x, mut max_x, mut min_y, mut max_y) =
        (first.clone(), first.clone(), first.clone(), first.clone());
    for point in points {
        if point.compare_x(&min_x).is_lt() {
            min_x = point.clone();
        }
        if point.compare_x(&max_x).is_gt() {
            max_x = point.clone();
        }
        if point.compare_y(&min_y).is_lt() {
            min_y = point.clone();
        }
        if point.compare_y(&max_y).is_gt() {
            max_y = point.clone();
        }
    }
    if min_x.compare_x(&max_x).is_eq() || min_y.compare_y(&max_y).is_eq() {
        return Err(GeometryErrorKind::voronoi("exact frame input has zero span").into());
    }
    let corner = |x: &ExactPoint, y: &ExactPoint| {
        ExactPoint::homogeneous(
            x.x().product(y.w()),
            y.y().product(x.w()),
            x.w().product(y.w()),
        )
    };
    Ok([
        corner(&min_x, &min_y),
        corner(&max_x, &min_y),
        corner(&max_x, &max_y),
        corner(&min_x, &max_y),
    ])
}

pub(super) fn rectangular_boundary(points: &[Point], padded: bool) -> Result<[ExactPoint; 4]> {
    let first = points
        .first()
        .ok_or_else(|| GeometryErrorKind::voronoi("empty Voronoi site set"))?;
    let (mut min_x, mut max_x, mut min_y, mut max_y) = (first.x, first.x, first.y, first.y);
    for point in &points[1..] {
        min_x = min_x.min(point.x);
        max_x = max_x.max(point.x);
        min_y = min_y.min(point.y);
        max_y = max_y.max(point.y);
    }
    if padded {
        // Half the larger span, computed exactly, then each corner rounded
        // OUTWARD to a double: the padded rectangle is a convenience frame
        // and a double-valued one keeps every clip side representable, so the
        // cells partition exactly the rectangle they report.
        let span_x = ExactDyadic::from_f64(max_x).subtract(ExactDyadic::from_f64(min_x));
        let span_y = ExactDyadic::from_f64(max_y).subtract(ExactDyadic::from_f64(min_y));
        let span = if span_x.cmp(&span_y).is_gt() {
            span_x
        } else {
            span_y
        };
        if span.is_zero() {
            return Err(GeometryErrorKind::voronoi("exact frame input has zero span").into());
        }
        let half = span.product(&ExactDyadic::from_f64(0.5));
        let outward = |base: f64, add: bool| -> Result<f64> {
            let exact = if add {
                ExactDyadic::from_f64(base).add(half.clone())
            } else {
                ExactDyadic::from_f64(base).subtract(half.clone())
            };
            let rounded = round_ratio(&exact, &ExactDyadic::from_f64(1.0))?;
            let overshoot = ExactDyadic::from_f64(rounded).cmp(&exact);
            Ok(match (add, overshoot) {
                (true, Ordering::Less) => rounded.next_up(),
                (false, Ordering::Greater) => rounded.next_down(),
                _ => rounded,
            })
        };
        min_x = outward(min_x, false)?;
        max_x = outward(max_x, true)?;
        min_y = outward(min_y, false)?;
        max_y = outward(max_y, true)?;
        if !(min_x.is_finite() && max_x.is_finite() && min_y.is_finite() && max_y.is_finite()) {
            return Err(
                GeometryErrorKind::voronoi("padded Voronoi frame overflows binary64").into(),
            );
        }
    }
    Ok([
        ExactPoint::from_xy(XY::new(min_x, min_y)),
        ExactPoint::from_xy(XY::new(max_x, min_y)),
        ExactPoint::from_xy(XY::new(max_x, max_y)),
        ExactPoint::from_xy(XY::new(min_x, max_y)),
    ])
}

/// Exact orientation of three finite homogeneous points.  The sign is
/// normalized for the (otherwise arbitrary) signs of their denominators.
/// Interval evaluation of the affine orientation determinant.
///
/// `orient_points` normalizes away the homogeneous denominators, so its verdict
/// IS the orientation of the three affine points `(x/w, y/w)`. Each of those
/// six coordinates lies in its point's certified interval, so evaluating the
/// determinant in outward-rounded interval arithmetic yields an interval that
/// CONTAINS the exact determinant. A sign is returned only when that interval
/// excludes zero, which proves it. Overlapping zero, or any non-finite operand,
/// declines to the exact determinant.
fn orient_points_filtered(a: &ExactPoint, b: &ExactPoint, c: &ExactPoint) -> Option<ExactSign> {
    if !a.is_finite() || !b.is_finite() || !c.is_finite() {
        return None;
    }
    let [ax, ay] = a.bounds;
    let [bx, by] = b.bounds;
    let [cx, cy] = c.bounds;
    if !(ax.is_finite()
        && ay.is_finite()
        && bx.is_finite()
        && by.is_finite()
        && cx.is_finite()
        && cy.is_finite())
    {
        return None;
    }
    let (bax, bay) = (bx.sub(ax), by.sub(ay));
    let (cax, cay) = (cx.sub(ax), cy.sub(ay));
    if !(bax.is_finite() && bay.is_finite() && cax.is_finite() && cay.is_finite()) {
        return None;
    }
    let determinant = bax.mul(cay).sub(bay.mul(cax));
    determinant.sign()
}

pub(super) fn orient_points(a: &ExactPoint, b: &ExactPoint, c: &ExactPoint) -> ExactSign {
    if let Some(sign) = orient_points_filtered(a, b, c) {
        if VERIFY_INTERVALS {
            assert_eq!(
                sign,
                orient_points_exact(a, b, c),
                "filtered orientation disagrees"
            );
        }
        return sign;
    }
    orient_points_exact(a, b, c)
}

fn orient_points_exact(a: &ExactPoint, b: &ExactPoint, c: &ExactPoint) -> ExactSign {
    let bax = b.x().product(a.w()).subtract(a.x().product(b.w()));
    let bay = b.y().product(a.w()).subtract(a.y().product(b.w()));
    let cax = c.x().product(a.w()).subtract(a.x().product(c.w()));
    let cay = c.y().product(a.w()).subtract(a.y().product(c.w()));
    let mut sign = bax.product(&cay).subtract(bay.product(&cax)).sign();
    // Both displacement numerators carry one factor of `a.w`; it is
    // squared in the determinant and therefore cannot affect the sign.
    let denominator_sign = b.w().sign().as_i8() * c.w().sign().as_i8();
    if denominator_sign < 0 {
        sign = match sign {
            ExactSign::Negative => ExactSign::Positive,
            ExactSign::Zero => ExactSign::Zero,
            ExactSign::Positive => ExactSign::Negative,
        };
    }
    sign
}

/// Cross product of two exact homogeneous lines as raw coordinates. The result
/// is not normalized: signed products and ratio comparison operate directly on
/// it.
fn intersection_homogeneous(a: &ExactLine, b: &ExactLine) -> Homogeneous {
    let a = &a.0;
    let b = &b.0;
    Homogeneous {
        x: a[1].product(&b[2]).subtract(a[2].product(&b[1])),
        y: a[2].product(&b[0]).subtract(a[0].product(&b[2])),
        w: a[0].product(&b[1]).subtract(a[1].product(&b[0])),
    }
}

fn try_intersection_homogeneous(a: &ExactLine, b: &ExactLine) -> Result<Homogeneous> {
    let a = &a.0;
    let b = &b.0;
    Ok(Homogeneous {
        x: a[1]
            .try_product(&b[2])?
            .try_subtract(a[2].try_product(&b[1])?)?,
        y: a[2]
            .try_product(&b[0])?
            .try_subtract(a[0].try_product(&b[2])?)?,
        w: a[0]
            .try_product(&b[1])?
            .try_subtract(a[1].try_product(&b[0])?)?,
    })
}

/// Cross product of two exact homogeneous lines, with certified bounds.
pub(super) fn line_intersection(a: &ExactLine, b: &ExactLine) -> ExactPoint {
    let Homogeneous { x, y, w } = intersection_homogeneous(a, b);
    ExactPoint::homogeneous(x, y, w)
}

fn try_line_intersection(a: &ExactLine, b: &ExactLine) -> Result<ExactPoint> {
    let Homogeneous { x, y, w } = try_intersection_homogeneous(a, b)?;
    Ok(ExactPoint::homogeneous(x, y, w))
}

pub(super) fn signed_line_product(line: &ExactLine, point: &ExactPoint) -> ExactSign {
    let sign = line.0[0]
        .product(point.x())
        .add(line.0[1].product(point.y()))
        .add(line.0[2].product(point.w()))
        .sign();
    if point.w().sign() == ExactSign::Negative {
        match sign {
            ExactSign::Negative => ExactSign::Positive,
            ExactSign::Positive => ExactSign::Negative,
            ExactSign::Zero => ExactSign::Zero,
        }
    } else {
        sign
    }
}

pub(in crate::geometry) fn compare_ratios(
    an: &ExactDyadic,
    ad: &ExactDyadic,
    bn: &ExactDyadic,
    bd: &ExactDyadic,
) -> Ordering {
    let mut order = an.product(bd).cmp(&bn.product(ad));
    if ad.sign().as_i8() * bd.sign().as_i8() < 0 {
        order = order.reverse();
    }
    order
}

pub(in crate::geometry) fn try_compare_ratios(
    an: &ExactDyadic,
    ad: &ExactDyadic,
    bn: &ExactDyadic,
    bd: &ExactDyadic,
) -> Result<Ordering> {
    let left = an.try_product(bd)?;
    let right = bn.try_product(ad)?;
    let mut order = left.cmp(&right);
    if ad.sign().as_i8() * bd.sign().as_i8() < 0 {
        order = order.reverse();
    }
    Ok(order)
}

const SQUARED_DISTANCE_ERRBOUND: f64 = 128.0 * f64::EPSILON;

fn normal_or_zero(value: f64) -> bool {
    value == 0.0 || value.is_normal()
}

/// Certifies a squared-distance comparison only while every floating operation
/// stays finite and either normal or exact zero. The error bound covers the two
/// rounded subtractions, squares, sums, and final subtraction.
fn squared_distance_cmp_filtered(origin: XY, left: XY, right: XY) -> Option<Ordering> {
    let squared = |point: XY| {
        let dx = point.x - origin.x;
        let dy = point.y - origin.y;
        let x_squared = dx * dx;
        let y_squared = dy * dy;
        let sum = x_squared + y_squared;
        (normal_or_zero(dx)
            && normal_or_zero(dy)
            && normal_or_zero(x_squared)
            && normal_or_zero(y_squared)
            && normal_or_zero(sum))
        .then_some(sum)
    };
    let left = squared(left)?;
    let right = squared(right)?;
    let difference = left - right;
    let magnitude = left + right;
    if !(normal_or_zero(difference) && normal_or_zero(magnitude)) {
        return None;
    }
    let bound = (magnitude * SQUARED_DISTANCE_ERRBOUND).next_up();
    (difference.abs() > bound).then(|| difference.total_cmp(&0.0))
}

pub(super) fn squared_distance_cmp(origin: XY, left: XY, right: XY) -> Ordering {
    if let Some(order) = squared_distance_cmp_filtered(origin, left, right) {
        if VERIFY_INTERVALS {
            assert_eq!(
                order,
                squared_distance_cmp_exact(origin, left, right),
                "filtered squared-distance comparison disagrees"
            );
        }
        return order;
    }
    squared_distance_cmp_exact(origin, left, right)
}

fn squared_distance_cmp_exact(origin: XY, left: XY, right: XY) -> Ordering {
    let squared = |point: XY| {
        let dx = difference(point.x, origin.x);
        let dy = difference(point.y, origin.y);
        dx.square().add(dy.square())
    };
    squared(left).cmp(&squared(right))
}

fn distance_within_filtered(origin: XY, point: XY, radius: f64) -> Option<bool> {
    let dx = point.x - origin.x;
    let dy = point.y - origin.y;
    let x_squared = dx * dx;
    let y_squared = dy * dy;
    let distance_squared = x_squared + y_squared;
    let radius_squared = radius * radius;
    let difference = distance_squared - radius_squared;
    let magnitude = distance_squared + radius_squared;
    if ![
        dx,
        dy,
        x_squared,
        y_squared,
        distance_squared,
        radius_squared,
        difference,
        magnitude,
    ]
    .into_iter()
    .all(normal_or_zero)
    {
        return None;
    }
    let bound = (magnitude * SQUARED_DISTANCE_ERRBOUND).next_up();
    if difference > bound {
        Some(false)
    } else if difference < -bound {
        Some(true)
    } else {
        None
    }
}

pub(super) fn distance_within(origin: XY, point: XY, radius: f64) -> bool {
    if let Some(within) = distance_within_filtered(origin, point, radius) {
        if VERIFY_INTERVALS {
            assert_eq!(
                within,
                distance_within_exact(origin, point, radius),
                "filtered distance inclusion disagrees"
            );
        }
        return within;
    }
    distance_within_exact(origin, point, radius)
}

fn distance_within_exact(origin: XY, point: XY, radius: f64) -> bool {
    let dx = difference(point.x, origin.x);
    let dy = difference(point.y, origin.y);
    dx.square()
        .add(dy.square())
        .cmp(&ExactDyadic::from_f64(radius).square())
        == Ordering::Less
}

pub(super) fn circumcenter(a: XY, b: XY, c: XY) -> Result<ExactPoint> {
    if orient2d(a, b, c) == ExactSign::Zero {
        return Err(GeometryErrorKind::voronoi(
            "cannot construct a circumcenter for collinear sites",
        ));
    }
    if let Some(center) = super::circumcenter::certified_circumcenter(a, b, c) {
        return Ok(ExactPoint::certified_circumcenter([a, b, c], &center));
    }
    Ok(line_intersection(
        &ExactLine::perpendicular_bisector(a, b),
        &ExactLine::perpendicular_bisector(a, c),
    ))
}

pub(super) fn try_circumcenter(a: XY, b: XY, c: XY) -> Result<ExactPoint> {
    if orient2d(a, b, c) == ExactSign::Zero {
        return Err(GeometryErrorKind::voronoi(
            "cannot construct a circumcenter for collinear sites",
        ));
    }
    if let Some(center) = super::circumcenter::certified_circumcenter(a, b, c) {
        return Ok(ExactPoint::certified_circumcenter([a, b, c], &center));
    }
    let first = ExactLine::try_perpendicular_bisector(a, b)?;
    let second = ExactLine::try_perpendicular_bisector(a, c)?;
    try_line_intersection(&first, &second)
}

fn approximate_ratio_components(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> Option<(f64, i64)> {
    let leading = |value: &ExactDyadic| -> Option<(u64, i64)> {
        let (&high, rest) = value.limbs.split_last()?;
        let next = rest.last().copied().unwrap_or(0);
        let high_bits = 64 - high.leading_zeros();
        let window = (u128::from(high) << 64) | u128::from(next);
        let significand = (window >> high_bits) as u64;
        let bit_length = i64::try_from(rest.len())
            .ok()?
            .checked_mul(64)?
            .checked_add(i64::from(high_bits))?;
        Some((significand, value.exponent.checked_add(bit_length)?))
    };
    let (numerator_significand, numerator_exponent) = leading(numerator)?;
    let (denominator_significand, denominator_exponent) = leading(denominator)?;
    let mut significand = numerator_significand as f64 / denominator_significand as f64;
    let mut exponent = numerator_exponent.checked_sub(denominator_exponent)?;
    if significand < 1.0 {
        significand *= 2.0;
        exponent = exponent.checked_sub(1)?;
    } else if significand >= 2.0 {
        significand *= 0.5;
        exponent = exponent.checked_add(1)?;
    }
    Some((significand, exponent))
}

fn scale_approximation(significand: f64, exponent: i64) -> Option<f64> {
    if exponent > 1023 {
        Some(f64::MAX)
    } else if exponent >= -1022 {
        let scaled = significand * 2.0_f64.powi(i32::try_from(exponent).ok()?);
        Some(if scaled.is_finite() { scaled } else { f64::MAX })
    } else if exponent < -1075 {
        Some(0.0)
    } else {
        Some(significand * f64::MIN_POSITIVE * 2.0_f64.powi(i32::try_from(exponent + 1022).ok()?))
    }
}

fn approximate_ratio(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Option<f64> {
    let (significand, exponent) = approximate_ratio_components(numerator, denominator)?;
    scale_approximation(significand, exponent)
}

/// Approximate a positive exact ratio's square root without first rounding
/// the squared ratio. The latter can underflow to zero even when its square
/// root is a representable normal, so the ratio exponent must be halved before
/// the final binary64 scaling.
fn approximate_sqrt_ratio(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Option<f64> {
    let (significand, exponent) = approximate_ratio_components(numerator, denominator)?;
    if exponent.rem_euclid(2) == 0 {
        scale_approximation(significand.sqrt(), exponent / 2)
    } else {
        scale_approximation((significand * 2.0).sqrt(), exponent.checked_sub(1)? / 2)
    }
}

fn exact_ratio_bracket(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    proposal: f64,
    sign: ExactSign,
) -> Result<Option<(u64, u64, Option<ExactDyadic>)>> {
    const MAX_ULP_STEPS: usize = 4;
    let max_bits = f64::MAX.to_bits();
    let compare = |bits: u64| -> Result<(Ordering, ExactDyadic)> {
        let magnitude = f64::from_bits(bits);
        let candidate = if sign == ExactSign::Negative {
            -magnitude
        } else {
            magnitude
        };
        let product = ExactDyadic::try_from_f64(candidate)?.try_product(denominator)?;
        let order = product.cmp(numerator);
        Ok((
            if numerator.negative {
                order.reverse()
            } else {
                order
            },
            product,
        ))
    };
    let mut bits = proposal.to_bits();
    let (order, product) = compare(bits)?;
    match order {
        Ordering::Equal => return Ok(Some((bits, bits, None))),
        Ordering::Less => {
            let mut lower_product = product;
            for _ in 0..MAX_ULP_STEPS {
                if bits == max_bits {
                    return Ok(None);
                }
                let lower = bits;
                bits += 1;
                let (order, product) = compare(bits)?;
                match order {
                    Ordering::Less => lower_product = product,
                    Ordering::Equal => return Ok(Some((bits, bits, None))),
                    Ordering::Greater => return Ok(Some((lower, bits, Some(lower_product)))),
                }
            }
        },
        Ordering::Greater => {
            for _ in 0..MAX_ULP_STEPS {
                if bits == 0 {
                    return Ok(None);
                }
                let upper = bits;
                bits -= 1;
                let (order, product) = compare(bits)?;
                match order {
                    Ordering::Greater => {},
                    Ordering::Equal => return Ok(Some((bits, bits, None))),
                    Ordering::Less => return Ok(Some((bits, upper, Some(product)))),
                }
            }
        },
    }
    Ok(None)
}

fn round_ratio_binary(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Result<f64> {
    let sign = numerator.sign().quotient(denominator.sign());
    let numerator = numerator.try_clone()?.abs();
    let denominator = denominator.try_clone()?.abs();
    if numerator.is_zero() {
        return Ok(0.0);
    }
    let max = ExactDyadic::try_from_f64(f64::MAX)?.try_product(&denominator)?;
    if numerator.cmp(&max) == Ordering::Greater {
        let overflow_midpoint = ExactDyadic::try_from_f64(f64::MAX)?
            .try_add(ExactDyadic::try_from_f64(2.0_f64.powi(970))?)?
            .try_product(&denominator)?;
        if numerator.cmp(&overflow_midpoint) == Ordering::Less {
            return Ok(if sign == ExactSign::Negative {
                -f64::MAX
            } else {
                f64::MAX
            });
        }
        return Err(GeometryErrorKind::ExactCoordinateOutOfRange.into());
    }
    let mut low = 0_u64;
    let mut high = f64::MAX.to_bits();
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        let product = ExactDyadic::try_from_f64(f64::from_bits(mid))?.try_product(&denominator)?;
        if product.cmp(&numerator) == Ordering::Greater {
            high = mid - 1;
        } else {
            low = mid;
        }
    }
    let lower = f64::from_bits(low);
    let rounded = if low == f64::MAX.to_bits() {
        lower
    } else {
        let upper = f64::from_bits(low + 1);
        // Compare 2*n/d with lower+upper. Equality is the rounding tie.
        let twice_numerator = numerator.try_product(&ExactDyadic::try_from_f64(2.0)?)?;
        let midpoint_scaled = ExactDyadic::try_from_f64(lower)?
            .try_add(ExactDyadic::try_from_f64(upper)?)?
            .try_product(&denominator)?;
        match twice_numerator.cmp(&midpoint_scaled) {
            Ordering::Less => lower,
            Ordering::Greater => upper,
            Ordering::Equal => {
                if low & 1 == 0 {
                    lower
                } else {
                    upper
                }
            },
        }
    };
    Ok(if sign == ExactSign::Negative {
        -rounded
    } else {
        rounded
    })
}

pub(in crate::geometry) fn round_ratio(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> Result<f64> {
    if denominator.is_zero() {
        return Err(GeometryErrorKind::ExactRatioZeroDenominator.into());
    }
    let sign = numerator.sign().quotient(denominator.sign());
    if numerator.is_zero() {
        return Ok(0.0);
    }
    if numerator.cmp_magnitude(denominator) == Ordering::Equal {
        return Ok(if sign == ExactSign::Negative {
            -1.0
        } else {
            1.0
        });
    }
    if let Some(proposal) = approximate_ratio(numerator, denominator)
        && let Some((low, high, lower_product)) =
            exact_ratio_bracket(numerator, denominator, proposal, sign)?
    {
        if low == high {
            let rounded = f64::from_bits(low);
            return Ok(if sign == ExactSign::Negative {
                -rounded
            } else {
                rounded
            });
        }
        let lower = f64::from_bits(low);
        let upper = f64::from_bits(high);
        let lower_product = lower_product.expect("a two-point bracket has a lower product");
        let residual = numerator.try_subtract_ref(&lower_product)?;
        let spacing_exponent = {
            // The lower binade owns the gap at every power-of-two boundary.
            let encoded_exponent = (low >> 52) & 0x7FF;
            if encoded_exponent == 0 {
                -1074
            } else {
                encoded_exponent as i64 - 1075
            }
        };
        let midpoint_order =
            if let Some(order) = residual.cmp_magnitude_scaled(denominator, spacing_exponent - 1) {
                order
            } else {
                let spacing = ExactDyadic::try_from_f64(upper - lower)?;
                let scaled_spacing = spacing.try_product(denominator)?;
                let twice_residual = residual.try_product(&ExactDyadic::try_from_f64(2.0)?)?;
                twice_residual.cmp_magnitude(&scaled_spacing)
            };
        let rounded = match midpoint_order {
            Ordering::Less => lower,
            Ordering::Equal if low & 1 == 0 => lower,
            Ordering::Greater | Ordering::Equal => upper,
        };
        return Ok(if sign == ExactSign::Negative {
            -rounded
        } else {
            rounded
        });
    }
    round_ratio_binary(numerator, denominator)
}

/// Return the tight binary64 interval containing an exact dyadic ratio.
///
/// `round_ratio` supplies the nearest binary64 value. Comparing that value
/// back against the exact ratio identifies whether only its predecessor or
/// successor is needed; the comparison itself remains exact, so the returned
/// interval never uses a rounded value as a decision about the ratio.
pub(in crate::geometry) fn outward_ratio_bounds(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> Result<(f64, f64)> {
    let rounded = round_ratio(numerator, denominator)?;
    let rounded_exact = ExactDyadic::try_from_f64(rounded)?;
    match try_compare_ratios(numerator, denominator, &rounded_exact, &ExactDyadic::one())? {
        Ordering::Less => Ok((rounded.next_down(), rounded)),
        Ordering::Equal => Ok((rounded, rounded)),
        Ordering::Greater => Ok((rounded, rounded.next_up())),
    }
}

/// Round `sqrt(numerator / denominator)` to nearest binary64, ties to even.
///
/// The caller supplies exact nonnegative `numerator` and positive `denominator`.
/// The comparison stays in squared space, including subnormal and overflow
/// boundaries, so the squared ratio is never rounded before taking its root.
pub(in crate::geometry) fn round_sqrt_ratio(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> Result<f64> {
    if numerator.is_zero() {
        return Ok(0.0);
    }
    let max = ExactDyadic::try_from_f64(f64::MAX)?;
    let overflow_midpoint = max.try_add(ExactDyadic::try_from_f64(2.0_f64.powi(970))?)?;
    let overflow_squared = overflow_midpoint
        .try_product(&overflow_midpoint)?
        .try_product(denominator)?;
    if numerator.cmp(&overflow_squared) != Ordering::Less {
        return Ok(f64::INFINITY);
    }
    if let Some(proposal) = approximate_sqrt_ratio(numerator, denominator)
        && proposal.is_finite()
    {
        let proposal = if proposal == 0.0 {
            f64::from_bits(1)
        } else {
            proposal
        };
        if let Some((low, high)) = exact_sqrt_ratio_bracket(numerator, denominator, proposal)? {
            if low == high {
                return Ok(f64::from_bits(low));
            }
            let lower = f64::from_bits(low);
            let upper = f64::from_bits(high);
            // Compare the exact squared ratio with the square of the
            // midpoint without rounding the midpoint itself.
            let midpoint_sum =
                ExactDyadic::try_from_f64(lower)?.try_add(ExactDyadic::try_from_f64(upper)?)?;
            let midpoint_squared = midpoint_sum
                .try_product(&midpoint_sum)?
                .try_product(denominator)?;
            let four_numerator = numerator.try_product(&ExactDyadic::try_from_f64(4.0)?)?;
            return Ok(match four_numerator.cmp(&midpoint_squared) {
                Ordering::Less => lower,
                Ordering::Greater => upper,
                Ordering::Equal => {
                    if low & 1 == 0 {
                        lower
                    } else {
                        upper
                    }
                },
            });
        }
    }
    round_sqrt_ratio_binary(numerator, denominator)
}

fn exact_sqrt_ratio_bracket(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    proposal: f64,
) -> Result<Option<(u64, u64)>> {
    let max_bits = f64::MAX.to_bits();
    let compare = |bits: u64| -> Result<Ordering> {
        let candidate = ExactDyadic::try_from_f64(f64::from_bits(bits))?;
        let square = candidate
            .try_product(&candidate)?
            .try_product(denominator)?;
        Ok(square.cmp(numerator))
    };
    let bits = proposal.to_bits().min(max_bits);
    let (mut lower, mut upper) = match compare(bits)? {
        Ordering::Equal => return Ok(Some((bits, bits))),
        Ordering::Less => {
            let mut lower = bits;
            let mut step = 1_u64;
            loop {
                let distance = max_bits - lower;
                if distance == 0 {
                    return Ok(None);
                }
                let candidate = lower + step.min(distance);
                match compare(candidate)? {
                    Ordering::Less => {
                        lower = candidate;
                        step = step.saturating_mul(2);
                    },
                    Ordering::Equal => return Ok(Some((candidate, candidate))),
                    Ordering::Greater => break (lower, candidate),
                }
            }
        },
        Ordering::Greater => {
            let mut upper = bits;
            let mut step = 1_u64;
            loop {
                if upper == 0 {
                    return Ok(None);
                }
                let candidate = upper - step.min(upper);
                match compare(candidate)? {
                    Ordering::Greater => {
                        upper = candidate;
                        step = step.saturating_mul(2);
                    },
                    Ordering::Equal => return Ok(Some((candidate, candidate))),
                    Ordering::Less => break (candidate, upper),
                }
            }
        },
    };

    // Exponential search finds a finite enclosing pair without assuming that
    // the floating proposal is close to the exact root. Refine the pair until
    // the returned endpoints are adjacent in binary64's nonnegative bit
    // ordering; unlike a fixed ULP walk, this also covers both subnormal and
    // binade boundaries without an arbitrary retry limit.
    while upper - lower > 1 {
        let middle = lower + (upper - lower) / 2;
        match compare(middle)? {
            Ordering::Less => lower = middle,
            Ordering::Equal => return Ok(Some((middle, middle))),
            Ordering::Greater => upper = middle,
        }
    }
    Ok(Some((lower, upper)))
}

fn round_sqrt_ratio_binary(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Result<f64> {
    let mut low = 0_u64;
    let mut high = f64::MAX.to_bits();
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        let candidate = ExactDyadic::try_from_f64(f64::from_bits(mid))?;
        let square = candidate
            .try_product(&candidate)?
            .try_product(denominator)?;
        if square.cmp(numerator) == Ordering::Greater {
            high = mid - 1;
        } else {
            low = mid;
        }
    }
    if low == f64::MAX.to_bits() {
        return Ok(f64::MAX);
    }
    let lower = f64::from_bits(low);
    let upper = f64::from_bits(low + 1);
    let midpoint_sum =
        ExactDyadic::try_from_f64(lower)?.try_add(ExactDyadic::try_from_f64(upper)?)?;
    let midpoint_squared = midpoint_sum
        .try_product(&midpoint_sum)?
        .try_product(denominator)?;
    let four_numerator = numerator.try_product(&ExactDyadic::try_from_f64(4.0)?)?;
    Ok(match four_numerator.cmp(&midpoint_squared) {
        Ordering::Less => lower,
        Ordering::Greater => upper,
        Ordering::Equal => {
            if low & 1 == 0 {
                lower
            } else {
                upper
            }
        },
    })
}

#[cfg(test)]
mod sqrt_ratio_tests {
    use super::*;

    #[test]
    fn rounds_tie_to_even_without_squared_rounding() {
        let lower = ExactDyadic::try_from_f64(1.0).expect("finite one");
        let upper = ExactDyadic::try_from_f64(1.0_f64.next_up()).expect("finite successor");
        let midpoint = lower
            .try_add(upper)
            .expect("midpoint sum")
            .try_product(&ExactDyadic::try_from_f64(0.5).expect("finite half"))
            .expect("midpoint");
        let squared = midpoint.try_product(&midpoint).expect("squared midpoint");
        assert_eq!(
            round_sqrt_ratio(&squared, &ExactDyadic::try_from_f64(1.0).unwrap()).unwrap(),
            1.0
        );
    }

    #[test]
    fn handles_non_dyadic_distance() {
        let squared = ExactDyadic::try_from_f64(2.0).expect("finite two");
        assert_eq!(
            round_sqrt_ratio(&squared, &ExactDyadic::try_from_f64(1.0).unwrap()).unwrap(),
            2.0_f64.sqrt()
        );
    }

    #[test]
    fn min_subnormal_sqrt_ratio_uses_an_adjacent_bracket() {
        let u = f64::from_bits(1);
        let exact_u = ExactDyadic::try_from_f64(u).expect("minimum subnormal");
        let two = ExactDyadic::try_from_f64(2.0).expect("exact two");
        let two_u = exact_u.try_product(&two).expect("two minimum subnormals");
        let squared = two_u
            .try_product(&two_u)
            .expect("squared two minimum subnormals")
            .try_add(
                exact_u
                    .try_product(&exact_u)
                    .expect("squared minimum subnormal"),
            )
            .expect("sqrt five squared ratio");
        let denominator = ExactDyadic::one();
        let bracket = exact_sqrt_ratio_bracket(&squared, &denominator, u)
            .unwrap()
            .expect("finite root bracket");
        assert_eq!(bracket, (2, 3));
        assert_eq!(
            bracket.1 - bracket.0,
            1,
            "a non-singleton bracket is adjacent"
        );
        assert_eq!(
            round_sqrt_ratio(&squared, &denominator).unwrap().to_bits(),
            2,
            "sqrt(5)u rounds to the even 2u endpoint"
        );
    }

    #[test]
    fn accepting_the_nonadjacent_min_subnormal_bracket_would_change_the_result() {
        let u = ExactDyadic::try_from_f64(f64::from_bits(1)).expect("minimum subnormal");
        let bad_lower = u.clone();
        let bad_upper = u
            .try_product(&ExactDyadic::try_from_f64(3.0).expect("exact three"))
            .expect("three minimum subnormals");
        let exact_squared = u
            .try_product(&u)
            .expect("squared minimum subnormal")
            .try_product(&ExactDyadic::try_from_f64(5.0).expect("exact five"))
            .expect("sqrt five squared ratio");
        let midpoint = bad_lower
            .try_add(bad_upper)
            .expect("nonadjacent bracket sum")
            .try_product(&ExactDyadic::try_from_f64(0.5).expect("exact half"))
            .expect("nonadjacent bracket midpoint");
        assert_eq!(
            exact_squared
                .try_product(&ExactDyadic::try_from_f64(4.0).expect("exact four"))
                .expect("scaled exact ratio")
                .cmp(
                    &midpoint
                        .try_product(&midpoint)
                        .expect("nonadjacent midpoint square")
                ),
            Ordering::Greater,
            "sqrt(5)u lies above the midpoint of the bad [u, 3u] bracket"
        );
        assert_eq!(
            round_sqrt_ratio(&exact_squared, &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            2,
            "the adjacent [2u, 3u] bracket is required for the correct result"
        );
    }

    #[test]
    fn sqrt_brackets_are_adjacent_at_subnormal_and_normal_boundaries() {
        let one = ExactDyadic::one();
        assert_eq!(
            round_sqrt_ratio(&ExactDyadic::zero(), &one)
                .unwrap()
                .to_bits(),
            0
        );
        let square = |value: f64| {
            let value = ExactDyadic::try_from_f64(value).expect("finite boundary value");
            value.try_product(&value).expect("boundary square")
        };
        let assert_bracket = |name: &str, numerator: &ExactDyadic, proposal: f64| {
            let (lower, upper) = exact_sqrt_ratio_bracket(numerator, &one, proposal)
                .unwrap()
                .expect("finite boundary bracket");
            if lower != upper {
                assert_eq!(
                    upper - lower,
                    1,
                    "{name}: non-singleton endpoints must be adjacent"
                );
            }
            let lower_value = ExactDyadic::try_from_f64(f64::from_bits(lower)).unwrap();
            let lower_squared = lower_value
                .try_product(&lower_value)
                .unwrap()
                .try_product(&one)
                .unwrap();
            assert_ne!(
                lower_squared.cmp(numerator),
                Ordering::Greater,
                "{name}: lower endpoint is above the root"
            );
            let upper_value = ExactDyadic::try_from_f64(f64::from_bits(upper)).unwrap();
            let upper_squared = upper_value
                .try_product(&upper_value)
                .unwrap()
                .try_product(&one)
                .unwrap();
            assert_ne!(
                upper_squared.cmp(numerator),
                Ordering::Less,
                "{name}: upper endpoint is below the root"
            );
        };

        let minimum_subnormal = f64::from_bits(1);
        let below_minimum = ExactDyadic::from_parts(false, vec![1], -1075);
        assert_bracket(
            "below minimum subnormal",
            &below_minimum.square(),
            minimum_subnormal,
        );
        assert_eq!(
            round_sqrt_ratio(&below_minimum.square(), &one)
                .unwrap()
                .to_bits(),
            0
        );

        let subnormal_midpoint = {
            let lower = ExactDyadic::from_f64(minimum_subnormal);
            let upper = ExactDyadic::from_f64(f64::from_bits(2));
            lower
                .try_add(upper)
                .unwrap()
                .try_product(&ExactDyadic::from_f64(0.5))
                .unwrap()
        };
        assert_bracket(
            "minimum subnormal midpoint",
            &subnormal_midpoint.square(),
            minimum_subnormal,
        );
        assert_eq!(
            round_sqrt_ratio(&subnormal_midpoint.square(), &one)
                .unwrap()
                .to_bits(),
            2,
            "the even upper endpoint wins the 1u/2u tie"
        );

        let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
        let minimum_normal = f64::from_bits(0x0010_0000_0000_0000);
        let normal_boundary_midpoint = ExactDyadic::from_f64(largest_subnormal)
            .try_add(ExactDyadic::from_f64(minimum_normal))
            .unwrap()
            .try_product(&ExactDyadic::from_f64(0.5))
            .unwrap();
        assert_bracket(
            "subnormal/normal midpoint",
            &normal_boundary_midpoint.square(),
            largest_subnormal,
        );
        assert_eq!(
            round_sqrt_ratio(&normal_boundary_midpoint.square(), &one)
                .unwrap()
                .to_bits(),
            minimum_normal.to_bits(),
            "the even minimum-normal endpoint wins the tie"
        );

        let normal_midpoint = ExactDyadic::from_f64(1.0)
            .try_add(ExactDyadic::from_f64(1.0_f64.next_up()))
            .unwrap()
            .try_product(&ExactDyadic::from_f64(0.5))
            .unwrap();
        assert_bracket("normal midpoint", &normal_midpoint.square(), 1.0);
        assert_eq!(
            round_sqrt_ratio(&normal_midpoint.square(), &one)
                .unwrap()
                .to_bits(),
            1.0_f64.to_bits(),
            "the even lower endpoint wins the normal tie"
        );

        assert_eq!(
            round_sqrt_ratio(&square(largest_subnormal), &one)
                .unwrap()
                .to_bits(),
            largest_subnormal.to_bits()
        );
        assert_eq!(
            round_sqrt_ratio(&square(minimum_normal), &one)
                .unwrap()
                .to_bits(),
            minimum_normal.to_bits()
        );
    }

    #[test]
    fn sqrt_ratio_rounding_handles_the_overflow_boundary() {
        let one = ExactDyadic::one();
        let maximum = ExactDyadic::from_f64(f64::MAX);
        let maximum_squared = maximum.square();
        assert_eq!(
            round_sqrt_ratio(&maximum_squared, &one).unwrap().to_bits(),
            f64::MAX.to_bits()
        );

        let below_overflow = maximum
            .clone()
            .add(ExactDyadic::from_parts(false, vec![1], 969));
        assert_eq!(
            round_sqrt_ratio(&below_overflow.square(), &one)
                .unwrap()
                .to_bits(),
            f64::MAX.to_bits()
        );

        let overflow_midpoint = maximum.add(ExactDyadic::from_parts(false, vec![1], 970));
        assert!(
            round_sqrt_ratio(&overflow_midpoint.square(), &one)
                .unwrap()
                .is_infinite(),
            "the exact overflow midpoint rounds to infinity"
        );
    }

    #[test]
    fn sqrt_bracketing_has_no_proposal_distance_limit() {
        let numerator = ExactDyadic::from_f64(2.0);
        let denominator = ExactDyadic::one();
        for proposal in [0.0, f64::MAX] {
            let (lower, upper) = exact_sqrt_ratio_bracket(&numerator, &denominator, proposal)
                .unwrap()
                .expect("finite root bracket");
            assert_eq!(
                upper - lower,
                1,
                "proposal {proposal:?} must still produce adjacent endpoints"
            );
            assert_ne!(
                ExactDyadic::from_f64(f64::from_bits(lower))
                    .square()
                    .cmp(&numerator),
                Ordering::Greater
            );
            assert_ne!(
                ExactDyadic::from_f64(f64::from_bits(upper))
                    .square()
                    .cmp(&numerator),
                Ordering::Less
            );
        }
    }

    #[test]
    fn proposed_sqrt_bracket_matches_the_complete_binary_reference() {
        for (numerator, denominator) in [
            (1.0, 1.0),
            (2.0, 1.0),
            (3.0, 1.0),
            (1.0, 3.0),
            (f64::from_bits(1), 1.0),
            (f64::MAX, 1.0),
            (f64::MAX, 3.0),
        ] {
            let numerator = ExactDyadic::try_from_f64(numerator).unwrap();
            let denominator = ExactDyadic::try_from_f64(denominator).unwrap();
            assert_eq!(
                round_sqrt_ratio(&numerator, &denominator)
                    .unwrap()
                    .to_bits(),
                round_sqrt_ratio_binary(&numerator, &denominator)
                    .unwrap()
                    .to_bits()
            );
        }
    }

    #[test]
    fn proposed_sqrt_bracket_preserves_allocation_failure_reporting() {
        let numerator = ExactDyadic::try_from_f64(3.0).unwrap();
        let denominator = ExactDyadic::one();
        let (_, allocations) =
            crate::test_support::allocation_count(|| round_sqrt_ratio(&numerator, &denominator));
        assert!(allocations > 0, "exact neighbor comparison must allocate");
        for allowed in 0..allocations {
            let error = crate::test_support::fail_allocations_after(allowed, || {
                round_sqrt_ratio(&numerator, &denominator)
            })
            .unwrap_err();
            assert!(
                matches!(
                    error.kind(),
                    crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
                ),
                "allocation prefix {allowed}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::exact_arith::ExactSign;

    fn dyadic_orientation_sign(a: XY, b: XY, c: XY) -> ExactSign {
        let difference = |left: f64, right: f64| {
            ExactDyadic::from_f64(left).subtract(ExactDyadic::from_f64(right))
        };
        difference(b.x, a.x)
            .product(&difference(c.y, a.y))
            .subtract(difference(b.y, a.y).product(&difference(c.x, a.x)))
            .sign()
    }

    #[test]
    fn predicate_filters_preserve_signs_across_exponent_ranges() {
        for exponent in [-600, -451, -450, -201, -200, 0, 199, 200, 449, 450, 600] {
            let scale = 2.0_f64.powi(exponent);
            let origin = XY::new(0.0, 0.0);
            let east = XY::new(scale, 0.0);
            let north = XY::new(0.0, scale);
            let inside = XY::new(scale * 0.25, scale * 0.25);
            assert_eq!(super::orient2d(origin, east, north), ExactSign::Positive);
            assert_eq!(
                super::incircle(origin, east, north, inside),
                ExactSign::Positive
            );
            if let Some(sign) = super::orient2d_float(origin, east, north) {
                assert_eq!(sign, ExactSign::Positive, "orientation exponent {exponent}");
            }
            if let Some(sign) = super::incircle_float(origin, east, north, inside) {
                assert_eq!(sign, ExactSign::Positive, "incircle exponent {exponent}");
            }
        }
    }

    #[test]
    fn source_parameter_keeps_the_exact_ratio_after_rounding() {
        let start = XY::new(0.25, 1.000_000_000_000_000_2);
        let end = XY::new(0.25, 1.25);
        let midpoint = ExactRational::zero()
            .try_midpoint(&ExactRational::one())
            .expect("midpoint parameter");
        let rounded = midpoint
            .round_segment(start, end)
            .expect("rounded source point");
        let actual = ExactRational::from_segment(start, end, rounded).expect("source parameter");
        assert_ne!(
            actual
                .try_cmp(&midpoint)
                .expect("exact parameter comparison"),
            Ordering::Equal
        );
        let next = ExactRational::zero()
            .try_midpoint(&actual)
            .expect("child midpoint parameter");
        assert_eq!(
            next.round_segment(start, end)
                .expect("rounded child source point"),
            XY::new(0.25, 1.0625)
        );
    }

    fn check_cocircular_underflow(exponent: i32) {
        let scale = 2.0_f64.powi(exponent);
        let points = [
            XY::new(5.0 * scale, 0.0),
            XY::new(3.0 * scale, 4.0 * scale),
            XY::new(0.0, 5.0 * scale),
            XY::new(-3.0 * scale, 4.0 * scale),
        ];
        let [a, b, c, d] = points;
        assert_eq!(super::incircle_dyadic(a, b, c, d), ExactSign::Zero);
        if let Some(sign) = super::incircle_float(a, b, c, d) {
            assert_eq!(sign, ExactSign::Zero, "scale exponent {exponent}");
        }
        assert_eq!(super::incircle(a, b, c, d), ExactSign::Zero);
    }

    #[test]
    fn incircle_cocircular_underflow_positive() {
        check_cocircular_underflow(-270);
    }

    #[test]
    fn incircle_cocircular_underflow_negative() {
        check_cocircular_underflow(-271);
    }

    #[test]
    fn incircle_cocircular_squares_underflow_to_opposite_wrong_signs() {
        for scale in [3.0 * 2.0_f64.powi(-270), 13.0 * 2.0_f64.powi(-272)] {
            let [a, b, c, d] = [
                XY::new(0.0, 0.0),
                XY::new(scale, 0.0),
                XY::new(scale, scale),
                XY::new(0.0, scale),
            ];
            assert_eq!(super::incircle_dyadic(a, b, c, d), ExactSign::Zero);
            if let Some(sign) = super::incircle_float(a, b, c, d) {
                assert_eq!(sign, ExactSign::Zero, "scale {scale:?}");
            }
            assert_eq!(super::incircle(a, b, c, d), ExactSign::Zero);
        }
    }

    #[test]
    fn orient2d_uses_the_shared_underflow_ray_resolver_for_all_permutations() {
        let mu = f64::from_bits(1);
        let a = XY::new(1.0, -4096.0 * mu);
        let b = XY::new(-2.0_f64.powi(-12), 6144.0 * mu);
        let p = XY::new(f64::from_bits(0x3FD9_9733_3333_3333), 2048.0 * mu);
        for ((left, middle, right), expected) in [
            ((a, b, p), ExactSign::Positive),
            ((a, p, b), ExactSign::Negative),
            ((b, a, p), ExactSign::Negative),
            ((b, p, a), ExactSign::Positive),
            ((p, a, b), ExactSign::Positive),
            ((p, b, a), ExactSign::Negative),
        ] {
            assert_eq!(super::orient2d(left, middle, right), expected);
            assert_eq!(
                stored_orientation_sign(left.x, left.y, middle.x, middle.y, right.x, right.y),
                expected
            );
        }
    }

    /// A decided stage-A result must agree with the independent exact sign.
    /// Cases that decline are still checked through the complete predicate;
    /// decline is conservative and is not itself a contract here.
    #[test]
    fn filtered_predicates_never_contradict_exact_signs() {
        let mu = f64::from_bits(1);
        let orientation_cases = [
            (
                "unit ccw",
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(0.0, 1.0),
                ExactSign::Positive,
            ),
            (
                "unit cw",
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(0.0, -1.0),
                ExactSign::Negative,
            ),
            (
                "unit collinear",
                XY::new(0.0, 0.0),
                XY::new(1.0, 1.0),
                XY::new(2.0, 2.0),
                ExactSign::Zero,
            ),
            (
                "one ulp",
                XY::new(0.0, 1.0),
                XY::new(1.0, f64::from_bits(0x3FF0_0000_0000_0001)),
                XY::new(2.0, 1.0),
                ExactSign::Negative,
            ),
            (
                "subnormal triangle",
                XY::new(0.0, 0.0),
                XY::new(mu, 0.0),
                XY::new(0.0, mu),
                ExactSign::Positive,
            ),
            (
                "underflow ray",
                XY::new(1.0, f64::from_bits(0x8000_0000_0000_1000)),
                XY::new(-2.0_f64.powi(-12), f64::from_bits(0x1800)),
                XY::new(f64::from_bits(0x3FD9_9733_3333_3333), f64::from_bits(0x800)),
                ExactSign::Positive,
            ),
            (
                "overflow differences",
                XY::new(-f64::MAX, -f64::MAX),
                XY::new(f64::MAX, f64::MAX),
                XY::new(f64::MAX, -f64::MAX),
                ExactSign::Negative,
            ),
            (
                "lattice exponent spread",
                XY::new(1.0, 1.0),
                XY::new(2.0_f64.powi(30), 0.0),
                XY::new(0.0, 3.0),
                ExactSign::Positive,
            ),
        ];
        for &(name, a, b, c, expected) in &orientation_cases {
            assert_eq!(dyadic_orientation_sign(a, b, c), expected, "{name}: dyadic");
            assert_eq!(
                super::orient2d(a, b, c),
                expected,
                "{name}: complete predicate"
            );
            if let Some(sign) = super::orient2d_float(a, b, c) {
                assert_eq!(sign, expected, "{name}: float filter");
            }
            if let Some(sign) = super::orient2d_lattice(a, b, c) {
                assert_eq!(sign, expected, "{name}: lattice filter");
            }
        }

        let scale = 2.0_f64.powi(-270);
        let incircle_cases = [
            (
                "unit inside",
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(0.0, 1.0),
                XY::new(0.25, 0.25),
                ExactSign::Positive,
            ),
            (
                "unit outside",
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(0.0, 1.0),
                XY::new(2.0, 2.0),
                ExactSign::Negative,
            ),
            (
                "unit cocircular",
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(1.0, 1.0),
                XY::new(0.0, 1.0),
                ExactSign::Zero,
            ),
            (
                "tiny cocircular",
                XY::new(5.0 * scale, 0.0),
                XY::new(3.0 * scale, 4.0 * scale),
                XY::new(0.0, 5.0 * scale),
                XY::new(-3.0 * scale, 4.0 * scale),
                ExactSign::Zero,
            ),
            (
                "extreme overflow",
                XY::new(-1e308, 0.0),
                XY::new(0.0, 1e308),
                XY::new(1e308, 0.0),
                XY::new(0.0, 0.0),
                ExactSign::Negative,
            ),
            (
                "half lattice inside",
                XY::new(0.0, 0.0),
                XY::new(2.0, 0.0),
                XY::new(0.0, 2.0),
                XY::new(0.5, 0.5),
                ExactSign::Positive,
            ),
            (
                "non-lattice",
                XY::new(0.1, 0.2),
                XY::new(1.0, 0.0),
                XY::new(1.0, 1.0),
                XY::new(0.0, 1.0),
                ExactSign::Negative,
            ),
        ];
        for &(name, a, b, c, d, expected) in &incircle_cases {
            assert_eq!(
                super::incircle_dyadic(a, b, c, d),
                expected,
                "{name}: dyadic"
            );
            assert_eq!(
                super::incircle(a, b, c, d),
                expected,
                "{name}: complete predicate"
            );
            if let Some(sign) = super::incircle_float(a, b, c, d) {
                assert_eq!(sign, expected, "{name}: float filter");
            }
            if let Some(sign) = super::incircle_lattice(a, b, c, d) {
                assert_eq!(sign, expected, "{name}: lattice filter");
            }
        }
    }

    #[test]
    fn exact_signs_cover_full_binary64_range() {
        assert_eq!(
            orient2d(
                XY::new(0.0, 0.0),
                XY::new(f64::MAX, 0.0),
                XY::new(0.0, f64::from_bits(1))
            ),
            ExactSign::Positive
        );
        assert_eq!(
            incircle(
                XY::new(-1e308, 0.0),
                XY::new(0.0, 1e308),
                XY::new(1e308, 0.0),
                XY::new(0.0, 0.0)
            ),
            ExactSign::Negative
        );
    }

    #[test]
    fn ratio_rounding_rejects_zero_denominators() {
        for numerator in [ExactDyadic::zero(), ExactDyadic::one()] {
            let error = round_ratio(&numerator, &ExactDyadic::zero()).unwrap_err();
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactRatioZeroDenominator)
            ));
        }
    }

    #[test]
    fn ratio_rounding_propagates_each_allocation_failure() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::{allocation_count, fail_allocations_after};

        let cases = [
            (ExactDyadic::one(), ExactDyadic::from_f64(3.0)),
            (
                ExactDyadic::from_f64(f64::from_bits(1)),
                ExactDyadic::from_f64(2.0),
            ),
            (ExactDyadic::from_f64(f64::MAX), ExactDyadic::from_f64(0.5)),
            (ExactDyadic::one(), ExactDyadic::zero()),
        ];
        for (numerator, denominator) in cases {
            let (_, count) = allocation_count(|| round_ratio(&numerator, &denominator));
            for prefix in 0..count {
                let error =
                    fail_allocations_after(prefix, || round_ratio(&numerator, &denominator))
                        .unwrap_err();
                assert!(
                    matches!(error.kind(), ErrorKind::Resource(ResourceError::Allocation)),
                    "allocation prefix {prefix}"
                );
            }
        }
    }

    #[test]
    fn ratio_rounding_is_ties_to_even_and_handles_extremes() {
        let one = ExactDyadic::one();
        let halfway = ExactDyadic::from_f64(1.0)
            .add(ExactDyadic::from_f64(f64::from_bits(1.0_f64.to_bits() + 1)));
        let two = ExactDyadic::from_f64(2.0);
        assert_eq!(
            round_ratio(&halfway, &two).unwrap().to_bits(),
            1.0_f64.to_bits()
        );
        assert_eq!(
            round_ratio(&one, &ExactDyadic::from_parts(false, vec![1], 1074))
                .unwrap()
                .to_bits(),
            1
        );
        assert_eq!(
            round_ratio(&ExactDyadic::from_f64(f64::MAX), &one)
                .unwrap()
                .to_bits(),
            f64::MAX.to_bits()
        );
        let below_overflow =
            ExactDyadic::from_f64(f64::MAX).add(ExactDyadic::from_parts(false, vec![1], 969));
        let overflow_tie =
            ExactDyadic::from_f64(f64::MAX).add(ExactDyadic::from_parts(false, vec![1], 970));
        assert_eq!(
            round_ratio(&below_overflow, &one).unwrap().to_bits(),
            f64::MAX.to_bits()
        );
        round_ratio(&overflow_tie, &one).unwrap_err();
    }

    #[test]
    fn dyadics_are_odd_low_canonical_and_exponent_capacity_is_live() {
        let values = [
            ExactDyadic::from_f64(2.0),
            ExactDyadic::from_f64(f64::from_bits(1)),
            ExactDyadic::from_f64(1.5).add(ExactDyadic::from_f64(0.5)),
            ExactDyadic::from_f64(8.0).subtract(ExactDyadic::from_f64(2.0)),
            ExactDyadic::from_f64(6.0).product(&ExactDyadic::from_f64(10.0)),
        ];
        assert!(
            values
                .iter()
                .all(|value| value.is_zero() || value.limbs[0] & 1 == 1)
        );

        let beyond_i32 = ExactDyadic::from_parts(false, vec![1], i64::from(i32::MAX) + 1);
        assert_eq!(beyond_i32.square().exponent, 2 * (i64::from(i32::MAX) + 1));
        let bound = 1_127_u128 * (1_u128 << 20) * (GENERATED_ITEM_LIMIT as u128 + 1);
        assert!(bound < i64::MAX as u128);
    }

    #[test]
    fn inline_and_spilled_dyadics_have_one_numeric_identity() {
        let inline = ExactDyadic::from_parts(false, vec![1, 3], -211);
        let spilled = ExactDyadic {
            negative: false,
            limbs: CompactLimbs::Heap(vec![1, 3]),
            exponent: -211,
        };
        assert_eq!(inline, spilled);
        assert_eq!(inline.cmp(&spilled), Ordering::Equal);
        assert_eq!(inline.cmp_magnitude(&spilled), Ordering::Equal);
        assert_eq!(
            round_ratio(&inline, &ExactDyadic::one()).unwrap().to_bits(),
            round_ratio(&spilled, &ExactDyadic::one())
                .unwrap()
                .to_bits()
        );
    }

    #[test]
    fn ordinary_one_limb_try_arithmetic_does_not_allocate() {
        use crate::test_support::allocation_count;

        let (_, allocations) = allocation_count(|| {
            let left = ExactDyadic::try_from_f64(3.0).unwrap();
            let right = ExactDyadic::try_from_f64(5.0).unwrap();
            let sum = left.try_add(right).unwrap();
            sum.try_product(&ExactDyadic::one()).unwrap()
        });
        assert_eq!(allocations, 0);
    }

    #[test]
    fn exact_sum_spills_only_when_an_inline_result_carries() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::{allocation_count, fail_allocations_after};

        let left = ExactDyadic::from_parts(false, vec![u64::MAX, u64::MAX - 1], 0);
        let no_carry = ExactDyadic::from_parts(false, vec![1], 0);
        let (sum, allocations) =
            allocation_count(|| left.clone().try_add(no_carry.clone()).unwrap());
        assert_eq!(allocations, 0);
        assert_eq!(sum, ExactDyadic::from_parts(false, vec![u64::MAX], 64));

        let carry = ExactDyadic::from_parts(false, vec![1, 1], 0);
        let (sum, allocations) = allocation_count(|| left.clone().try_add(carry.clone()).unwrap());
        assert_eq!(allocations, 1);
        assert_eq!(sum, ExactDyadic::from_parts(false, vec![1], 128));
        let error = fail_allocations_after(0, || left.try_add(carry)).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    #[test]
    fn exact_ratio_clone_propagates_each_allocation_failure() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::{allocation_count, fail_allocations_after};

        let ratio = ExactRational::new(
            ExactDyadic::from_parts(false, vec![1, 2, 3], -4),
            ExactDyadic::from_parts(false, vec![1, 4, 5], -4),
        )
        .unwrap();
        assert!(ratio.numerator.limbs.len() > 2);
        assert!(ratio.denominator.limbs.len() > 2);

        let (result, allocations) = allocation_count(|| ratio.try_clone());
        result.unwrap();
        assert!(allocations > 0);
        for prefix in 0..allocations {
            let error = fail_allocations_after(prefix, || ratio.try_clone()).unwrap_err();
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn exact_rational_arithmetic_reduces_and_rounds() {
        let third = ExactRational::new(ExactDyadic::one(), ExactDyadic::from_f64(3.0)).unwrap();
        let sixth = ExactRational::new(ExactDyadic::one(), ExactDyadic::from_f64(6.0)).unwrap();
        let half = third.try_add(&sixth).unwrap();
        assert_eq!(half.numerator, ExactDyadic::one());
        assert_eq!(half.denominator, ExactDyadic::from_f64(2.0));
        assert_eq!(half.sign(), ExactSign::Positive);
        assert!(!half.is_zero());
        assert_eq!(half.round().unwrap().to_bits(), 0.5_f64.to_bits());
        assert_eq!(
            half.outward_bounds().unwrap(),
            (0.5_f64, 0.5_f64),
            "an exactly representable ratio has a singleton outward interval"
        );
        assert_eq!(
            half.try_square().unwrap().round().unwrap().to_bits(),
            0.25_f64.to_bits()
        );

        let zero = half.try_subtract(&half).unwrap();
        assert!(zero.is_zero());
        assert_eq!(zero, ExactRational::zero());
    }

    #[test]
    fn denominator_one_rational_arithmetic_avoids_cross_product_allocations() {
        use crate::test_support::allocation_count;

        let exponent = i64::MIN + 1024;
        let positive =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1, 3], exponent))
                .unwrap();
        let negative =
            ExactRational::from_dyadic(ExactDyadic::from_parts(true, vec![1, 3], exponent))
                .unwrap();
        let zero = ExactRational::zero();
        let expected_subtraction =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1, 3], exponent + 1))
                .unwrap();

        let (sum, allocations) = allocation_count(|| positive.try_add(&negative));
        assert_eq!(allocations, 0);
        assert_eq!(sum.unwrap(), zero);

        let (difference, allocations) = allocation_count(|| positive.try_subtract(&negative));
        assert_eq!(allocations, 0);
        assert_eq!(difference.unwrap(), expected_subtraction);

        let product_left =
            ExactRational::from_dyadic(ExactDyadic::from_parts(true, vec![3], i64::MAX / 4))
                .unwrap();
        let product_right =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![5], i64::MAX / 4))
                .unwrap();
        let expected_product =
            ExactRational::from_dyadic(ExactDyadic::from_parts(true, vec![15], (i64::MAX / 4) * 2))
                .unwrap();
        let (product, allocations) = allocation_count(|| product_left.try_product(&product_right));
        assert_eq!(allocations, 0);
        assert_eq!(product.unwrap(), expected_product);

        let square_value =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![3], i64::MIN / 4))
                .unwrap();
        let expected_square =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![9], (i64::MIN / 4) * 2))
                .unwrap();
        let (square, allocations) = allocation_count(|| square_value.try_square());
        assert_eq!(allocations, 0);
        assert_eq!(square.unwrap(), expected_square);
    }

    #[test]
    fn denominator_one_rational_heap_results_still_report_allocation_failures() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::{allocation_count, fail_allocations_after};

        let left =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![u64::MAX, u64::MAX], 0))
                .unwrap();
        let right =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1, 1], 0)).unwrap();
        let expected =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1, 1], 64)).unwrap();

        let (result, allocations) = allocation_count(|| left.try_add(&right));
        assert_eq!(allocations, 1);
        assert_eq!(result.unwrap(), expected);

        for prefix in 0..allocations {
            let error = fail_allocations_after(prefix, || left.try_add(&right)).unwrap_err();
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn exact_rational_quotient_rejects_zero_and_negative_squares() {
        let error = ExactRational::one()
            .try_quotient(&ExactRational::zero())
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactRatioZeroDenominator)
        ));

        let error = ExactRational::from_f64(-1.0)
            .unwrap()
            .round_sqrt()
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
    }

    #[test]
    fn exact_dyadic_power_two_quotients_preserve_signs() {
        for (numerator, divisor, expected) in [
            (1.0, -1.0, -1.0),
            (-1.0, 1.0, -1.0),
            (-1.0, -1.0, 1.0),
            (8.0, -1.0, -8.0),
            (-8.0, 1.0, -8.0),
            (-8.0, -1.0, 8.0),
            (1.0, 8.0, 0.125),
            (-1.0, 8.0, -0.125),
            (1.0, -8.0, -0.125),
            (-1.0, -8.0, 0.125),
            (1.0, 0.125, 8.0),
            (-1.0, -0.125, 8.0),
        ] {
            let actual = ExactDyadic::from_f64(numerator)
                .try_exact_quotient(&ExactDyadic::from_f64(divisor))
                .unwrap();
            assert_eq!(
                actual,
                ExactDyadic::from_f64(expected),
                "{numerator:?} / {divisor:?}"
            );
        }
    }

    #[test]
    fn exact_dyadic_power_two_quotients_preserve_extreme_exponents() {
        let upper = ExactDyadic::from_parts(false, vec![1], i64::MAX);
        let upper_divisor = ExactDyadic::from_parts(true, vec![1], i64::MAX - 3);
        assert_eq!(
            upper.try_exact_quotient(&upper_divisor).unwrap(),
            ExactDyadic::from_parts(true, vec![1], 3)
        );

        let lower = ExactDyadic::from_parts(true, vec![1], i64::MIN + 3);
        let lower_divisor = ExactDyadic::from_parts(false, vec![1], i64::MIN);
        assert_eq!(
            lower.try_exact_quotient(&lower_divisor).unwrap(),
            ExactDyadic::from_parts(true, vec![1], 3)
        );

        let ratio = ExactRational::from_dyadic(upper).unwrap();
        let divisor =
            ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1], i64::MAX - 3))
                .unwrap();
        let quotient = ratio.try_quotient(&divisor).unwrap();
        assert_eq!(
            quotient,
            ExactRational::new(
                ExactDyadic::from_parts(false, vec![1], 3),
                ExactDyadic::one()
            )
            .unwrap()
        );
    }

    #[test]
    fn certified_ratio_proposal_matches_complete_binary_search() {
        let cases = [
            (ExactDyadic::one(), ExactDyadic::from_f64(3.0)),
            (
                ExactDyadic::from_f64(f64::from_bits(1)),
                ExactDyadic::from_f64(2.0),
            ),
            (ExactDyadic::from_f64(f64::MAX), ExactDyadic::from_f64(3.0)),
            (ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(10.0)),
        ];
        for (numerator, denominator) in cases {
            assert_eq!(
                round_ratio(&numerator, &denominator).unwrap().to_bits(),
                round_ratio_binary(&numerator, &denominator)
                    .unwrap()
                    .to_bits(),
            );
        }
    }

    #[test]
    fn exact_zero_ratio_is_canonical_positive_zero() {
        let zero = ExactDyadic::zero();
        let negative_denominator = ExactDyadic::from_f64(-3.0);
        assert_eq!(
            round_ratio(&zero, &negative_denominator).unwrap().to_bits(),
            0.0_f64.to_bits()
        );
    }

    #[test]
    fn exact_singleton_and_equal_magnitude_brackets_preserve_sign() {
        let numerator = ExactDyadic::from_f64(3.0);
        let denominator = ExactDyadic::from_f64(-3.0);
        assert_eq!(round_ratio(&numerator, &denominator).unwrap(), -1.0);
        assert_eq!(round_ratio(&denominator, &numerator).unwrap(), -1.0);
        assert_eq!(round_ratio(&numerator.neg(), &denominator).unwrap(), 1.0);

        let half = ExactDyadic::from_f64(1.0);
        let two = ExactDyadic::from_f64(2.0);
        assert_eq!(round_ratio(&half, &two).unwrap(), 0.5);
        assert_eq!(round_ratio(&half.neg(), &two).unwrap(), -0.5);
    }

    #[test]
    fn outward_ratio_bounds_contain_adversarial_exact_ratios() {
        fn assert_contains(name: &str, numerator: &ExactDyadic, denominator: &ExactDyadic) {
            let (lower, upper) = outward_ratio_bounds(numerator, denominator).unwrap();
            assert!(
                lower <= upper,
                "{name}: invalid interval [{lower:?}, {upper:?}]"
            );
            let one = ExactDyadic::one();
            let lower_exact = ExactDyadic::from_f64(lower);
            let upper_exact = ExactDyadic::from_f64(upper);
            assert_ne!(
                try_compare_ratios(numerator, denominator, &lower_exact, &one).unwrap(),
                Ordering::Less,
                "{name}: ratio below lower bound"
            );
            assert_ne!(
                try_compare_ratios(numerator, denominator, &upper_exact, &one).unwrap(),
                Ordering::Greater,
                "{name}: ratio above upper bound"
            );
        }

        let one = ExactDyadic::one();
        let half_ulp_at_one = ExactDyadic::from_parts(false, vec![1], -53);
        let below_tie = one.clone().subtract(half_ulp_at_one.clone());
        let above_tie = one.clone().add(half_ulp_at_one);
        let below_minimum_subnormal = ExactDyadic::from_parts(false, vec![1], -1075);
        let minimum_subnormal = ExactDyadic::from_f64(f64::from_bits(1));

        for (name, numerator, denominator) in [
            (
                "negative nonrepresentable",
                ExactDyadic::from_f64(-1.0),
                ExactDyadic::from_f64(3.0),
            ),
            (
                "negative exactly representable",
                ExactDyadic::from_f64(-1.5),
                one.clone(),
            ),
            (
                "negative zero",
                ExactDyadic::zero(),
                ExactDyadic::from_f64(-3.0),
            ),
            (
                "positive exactly representable",
                ExactDyadic::from_f64(1.5),
                one.clone(),
            ),
            ("below one midpoint", below_tie, one.clone()),
            ("above one midpoint", above_tie, one.clone()),
            ("minimum subnormal", minimum_subnormal, one.clone()),
            ("below minimum subnormal", below_minimum_subnormal, one),
        ] {
            assert_contains(name, &numerator, &denominator);
        }
    }

    #[test]
    fn adjacent_cells_and_midpoint_ties_keep_nearest_even_policy() {
        let one_f64: f64 = 1.0;
        let one_up_f64 = one_f64.next_up();
        let one_up_twice_f64 = one_up_f64.next_up();
        let one = ExactDyadic::from_f64(one_f64);
        let one_up = ExactDyadic::from_f64(one_up_f64);
        let midpoint = one
            .clone()
            .add(one_up.clone())
            .product(&ExactDyadic::from_f64(0.5));
        let quarter_gap = one_up
            .clone()
            .subtract(one)
            .product(&ExactDyadic::from_f64(0.25));
        assert_eq!(
            round_ratio(
                &midpoint.clone().subtract(quarter_gap.clone()),
                &ExactDyadic::one()
            )
            .unwrap()
            .to_bits(),
            1.0_f64.to_bits()
        );
        assert_eq!(
            round_ratio(&midpoint.clone().add(quarter_gap), &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            one_up_f64.to_bits()
        );
        assert_eq!(
            round_ratio(&midpoint, &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            1.0_f64.to_bits()
        );
        assert_eq!(
            round_ratio(&midpoint.neg(), &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            (-1.0_f64).to_bits()
        );

        let odd_lower = one_up;
        let even_upper_f64 = one_up_twice_f64;
        let even_upper = ExactDyadic::from_f64(even_upper_f64);
        let odd_midpoint = odd_lower
            .add(even_upper)
            .product(&ExactDyadic::from_f64(0.5));
        assert_eq!(
            round_ratio(&odd_midpoint, &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            even_upper_f64.to_bits()
        );
        assert_eq!(
            round_ratio(&odd_midpoint.neg(), &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            (-even_upper_f64).to_bits()
        );
    }

    #[test]
    fn ratio_rounding_preserves_signs_subnormals_and_power_boundaries() {
        let min_subnormal = ExactDyadic::from_f64(f64::from_bits(1));
        assert_eq!(
            round_ratio(&min_subnormal, &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            1
        );
        let below_minimum = ExactDyadic::from_parts(false, vec![1], -1075);
        assert_eq!(
            round_ratio(&below_minimum, &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            0
        );
        assert_eq!(
            round_ratio(&below_minimum.neg(), &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            (-0.0_f64).to_bits()
        );

        let predecessor = 1.0_f64.next_down();
        let lower_midpoint = ExactDyadic::from_f64(predecessor)
            .add(ExactDyadic::from_f64(1.0))
            .product(&ExactDyadic::from_f64(0.5));
        assert_eq!(
            round_ratio(&lower_midpoint, &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            1.0_f64.to_bits()
        );

        for (numerator, denominator) in [(5.0, 3.0), (-5.0, 3.0), (5.0, -3.0), (-5.0, -3.0)] {
            let numerator = ExactDyadic::from_f64(numerator);
            let denominator = ExactDyadic::from_f64(denominator);
            assert_eq!(
                round_ratio(&numerator, &denominator).unwrap().to_bits(),
                round_ratio_binary(&numerator, &denominator)
                    .unwrap()
                    .to_bits()
            );
        }
    }

    #[test]
    fn ratio_rounding_matches_binary_reference_on_curated_bit_patterns() {
        let patterns = [
            0x0000_0000_0000_0000,
            0x0000_0000_0000_0001,
            0x000F_FFFF_FFFF_FFFF,
            0x0010_0000_0000_0000,
            0x3FE0_0000_0000_0000,
            0x3FF0_0000_0000_0000,
            0x3FF0_0000_0000_0001,
            0x4000_0000_0000_0000,
            0x7FE0_0000_0000_0000,
            0x7FEF_FFFF_FFFF_FFFF,
        ];
        for &numerator_bits in &patterns {
            for &denominator_bits in &patterns {
                for (numerator_bits, denominator_bits) in [
                    (numerator_bits, denominator_bits),
                    (numerator_bits | (1_u64 << 63), denominator_bits),
                    (numerator_bits, denominator_bits | (1_u64 << 63)),
                ] {
                    let numerator = ExactDyadic::from_f64(f64::from_bits(numerator_bits));
                    let denominator = ExactDyadic::from_f64(f64::from_bits(denominator_bits));
                    if denominator.is_zero() {
                        continue;
                    }
                    let actual = round_ratio(&numerator, &denominator);
                    let reference = round_ratio_binary(&numerator, &denominator);
                    match (actual, reference) {
                        (Ok(actual), Ok(reference)) => {
                            assert_eq!(actual.to_bits(), reference.to_bits());
                        },
                        (Err(_), Err(_)) => {},
                        (actual, reference) => {
                            panic!(
                                "fast and binary ratio paths disagree: {actual:?} vs {reference:?}"
                            );
                        },
                    }
                }
            }
        }
    }

    #[test]
    fn nonzero_ratio_is_invariant_under_projective_sign_change() {
        let numerator = ExactDyadic::from_f64(5.0);
        let denominator = ExactDyadic::from_f64(3.0);
        let expected = 0x3FFA_AAAA_AAAA_AAAB;
        assert_eq!(
            round_ratio(&numerator, &denominator).unwrap().to_bits(),
            expected
        );
        assert_eq!(
            round_ratio(&numerator.neg(), &denominator.neg())
                .unwrap()
                .to_bits(),
            expected
        );
    }

    #[test]
    fn nonzero_negative_ratio_underflow_retains_negative_zero() {
        let tiny_negative = ExactDyadic::from_parts(true, vec![1], -1075);
        assert_eq!(
            round_ratio(&tiny_negative, &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            (-0.0_f64).to_bits()
        );
    }

    #[test]
    fn line_intersection_rounding_is_projective_sign_invariant() {
        let x_axis = ExactLine::through_points(
            &ExactPoint::from_xy(XY::new(0.0, 0.0)),
            &ExactPoint::from_xy(XY::new(1.0, 0.0)),
        )
        .unwrap();
        let vertical = ExactLine::through_points(
            &ExactPoint::from_xy(XY::new(0.5, -1.0)),
            &ExactPoint::from_xy(XY::new(0.5, 1.0)),
        )
        .unwrap();
        let forward = line_intersection(&x_axis, &vertical)
            .round_nearest_even()
            .unwrap();
        let reversed = line_intersection(&vertical, &x_axis)
            .round_nearest_even()
            .unwrap();
        assert_eq!(
            (forward.x.to_bits(), forward.y.to_bits()),
            (0x3FE0_0000_0000_0000, 0x0000_0000_0000_0000)
        );
        assert_eq!(
            (reversed.x.to_bits(), reversed.y.to_bits()),
            (forward.x.to_bits(), forward.y.to_bits())
        );
    }

    #[test]
    fn near_fixture_circumcenter_is_correctly_rounded() {
        let center = circumcenter(
            XY::new(22.0, -61.0),
            XY::new(-22.0, 61.0),
            XY::new(-62.0, f64::from_bits(19.0_f64.to_bits() + 1)),
        )
        .unwrap()
        .round_nearest_even()
        .unwrap();
        assert_eq!(
            center,
            XY::new(-1.224_017_584_342_978_8e-15, -4.414_489_648_450_088e-16)
        );
    }

    #[test]
    fn certified_materialization_preserves_exact_homogeneous_coordinates() {
        let center = circumcenter(XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(0.0, 2.0)).unwrap();
        assert_eq!(
            compare_ratios(
                center.x(),
                center.w(),
                &ExactDyadic::from_f64(2.0),
                &ExactDyadic::one(),
            ),
            Ordering::Equal,
        );
        assert_eq!(
            compare_ratios(
                center.y(),
                center.w(),
                &ExactDyadic::from_f64(1.0),
                &ExactDyadic::one(),
            ),
            Ordering::Equal,
        );

        let hit =
            match ExactPoint::certified_bisector_hit([XY::new(0.0, 0.0), XY::new(4.0, 2.0)], [
                XY::new(-10.0, 7.0),
                XY::new(10.0, 7.0),
            ]) {
                FrameHit::Point(point) => point,
                other => panic!("expected a certified bisector hit, got {other:?}"),
            };
        assert_eq!(
            compare_ratios(
                hit.x(),
                hit.w(),
                &ExactDyadic::from_f64(-1.0),
                &ExactDyadic::one(),
            ),
            Ordering::Equal,
        );
        assert_eq!(
            compare_ratios(
                hit.y(),
                hit.w(),
                &ExactDyadic::from_f64(7.0),
                &ExactDyadic::one(),
            ),
            Ordering::Equal,
        );
    }

    #[test]
    fn distinct_exact_vertices_can_collide_after_rounding() {
        let one = ExactDyadic::one();
        let base =
            ExactPoint::homogeneous(ExactDyadic::from_f64(1.0), ExactDyadic::zero(), one.clone());
        let shifted = ExactPoint::homogeneous(
            ExactDyadic::from_f64(1.0).add(ExactDyadic::from_parts(false, vec![1], -1074)),
            ExactDyadic::zero(),
            one,
        );
        assert_ne!(base.x(), shifted.x());
        assert_eq!(
            base.round_nearest_even().unwrap(),
            shifted.round_nearest_even().unwrap()
        );
    }

    #[test]
    fn homogeneous_products_and_ratio_order_are_exact() {
        let x_axis = ExactLine([ExactDyadic::zero(), ExactDyadic::one(), ExactDyadic::zero()]);
        let vertical = ExactLine([
            ExactDyadic::one(),
            ExactDyadic::zero(),
            ExactDyadic::from_f64(-0.5),
        ]);
        let intersection = line_intersection(&x_axis, &vertical);
        assert_eq!(
            intersection.round_nearest_even().unwrap(),
            XY::new(0.5, 0.0)
        );
        assert_eq!(
            signed_line_product(&vertical, &intersection),
            ExactSign::Zero
        );
        let reversed = line_intersection(&vertical, &x_axis);
        let above = ExactLine::through_points(
            &ExactPoint::from_xy(XY::new(0.0, 1.0)),
            &ExactPoint::from_xy(XY::new(1.0, 1.0)),
        )
        .unwrap();
        assert_eq!(
            signed_line_product(&above, &intersection),
            signed_line_product(&above, &reversed)
        );
        assert_eq!(
            compare_ratios(
                &ExactDyadic::one(),
                &ExactDyadic::from_f64(3.0),
                &ExactDyadic::one(),
                &ExactDyadic::from_f64(2.0),
            ),
            Ordering::Less
        );
    }

    #[test]
    fn squared_distance_cmp_point_exact_returns_tie_on_site_bisector() {
        let origin = ExactPoint::from_xy(XY::new(1.0, 3.0));
        assert_eq!(
            squared_distance_cmp_point_exact(&origin, XY::new(0.0, 0.0), XY::new(2.0, 0.0)),
            Ordering::Equal
        );
    }

    #[test]
    fn squared_distance_cmp_point_exact_accounts_for_negative_projective_weight() {
        let origin = ExactPoint::homogeneous(
            ExactDyadic::from_f64(-1.0),
            ExactDyadic::from_f64(-1.0),
            ExactDyadic::from_f64(-1.0),
        );
        assert_eq!(
            squared_distance_cmp_point_exact(&origin, XY::new(0.0, 0.0), XY::new(3.0, 0.0)),
            Ordering::Less
        );
    }

    #[test]
    fn squared_distance_cmp_point_exact_orders_full_binary64_site_spread() {
        let origin = ExactPoint::from_xy(XY::new(1.0, 0.0));
        assert_eq!(
            squared_distance_cmp_point_exact(
                &origin,
                XY::new(f64::MAX, 0.0),
                XY::new(f64::from_bits(1), 0.0),
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn squared_distance_cmp_point_exact_orders_subnormal_site() {
        let origin = ExactPoint::from_xy(XY::new(0.0, 0.0));
        assert_eq!(
            squared_distance_cmp_point_exact(
                &origin,
                XY::new(f64::from_bits(1), 0.0),
                XY::new(0.0, 0.0),
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn squared_distance_cmp_point_exact_orders_midpoint_origin() {
        let first = ExactPoint::from_xy(XY::new(-4.0, 2.0));
        let second = ExactPoint::from_xy(XY::new(6.0, 4.0));
        let origin = first.midpoint(&second);
        assert_eq!(
            squared_distance_cmp_point_exact(&origin, XY::new(0.0, 0.0), XY::new(4.0, 0.0)),
            Ordering::Less
        );
    }

    #[test]
    fn exact_segment_intersection_covers_extremes_endpoints_and_overlap() {
        let point = |x, y| ExactPoint::from_xy(XY::new(x, y));
        assert!(matches!(
            segment_intersection(
                &point(-f64::MAX, 0.0),
                &point(f64::MAX, 0.0),
                &point(0.0, -f64::from_bits(1)),
                &point(0.0, f64::from_bits(1)),
            ),
            SegmentIntersection::Point(hit) if hit.same_position(&point(0.0, 0.0))
        ));
        assert!(matches!(
            segment_intersection(&point(0.0, 0.0), &point(2.0, 0.0), &point(2.0, 0.0), &point(3.0, 1.0)),
            SegmentIntersection::Point(hit) if hit.same_position(&point(2.0, 0.0))
        ));
        assert!(matches!(
            segment_intersection(&point(0.0, 0.0), &point(3.0, 0.0), &point(1.0, 0.0), &point(2.0, 0.0)),
            SegmentIntersection::Overlap { start, end }
                if start.same_position(&point(1.0, 0.0)) && end.same_position(&point(2.0, 0.0))
        ));
        for (b0, b1) in [((1.0, 0.0), (2.0, 0.0)), ((2.0, 0.0), (1.0, 0.0))] {
            assert!(matches!(
                segment_intersection(
                    &point(0.0, 0.0),
                    &point(3.0, 0.0),
                    &point(b0.0, b0.1),
                    &point(b1.0, b1.1),
                ),
                SegmentIntersection::Overlap { start, end }
                    if start.same_position(&point(1.0, 0.0))
                        && end.same_position(&point(2.0, 0.0))
            ));
        }
    }

    #[test]
    fn exact_cycle_and_point_classification_are_orientation_independent() {
        let ring = [
            ExactPoint::from_xy(XY::new(0.0, 0.0)),
            ExactPoint::from_xy(XY::new(2.0, 0.0)),
            ExactPoint::from_xy(XY::new(2.0, 2.0)),
            ExactPoint::from_xy(XY::new(0.0, 2.0)),
        ];
        assert_eq!(cycle_orientation(&ring).unwrap(), ExactSign::Positive);
        assert_eq!(
            point_in_cycle(&ring, &ExactPoint::from_xy(XY::new(1.0, 1.0))),
            PointInCycle::Inside
        );
        assert_eq!(
            point_in_cycle(&ring, &ExactPoint::from_xy(XY::new(2.0, 1.0))),
            PointInCycle::Boundary
        );
        assert_eq!(
            point_in_cycle(&ring, &ExactPoint::from_xy(XY::new(3.0, 1.0))),
            PointInCycle::Outside
        );
    }

    #[test]
    fn exact_angle_orders_high_valence_star_without_scale() {
        let origin = ExactPoint::from_xy(XY::new(0.0, 0.0));
        let mut rays = [
            ExactPoint::from_xy(XY::new(0.0, -f64::MAX)),
            ExactPoint::from_xy(XY::new(-f64::from_bits(1), 0.0)),
            ExactPoint::from_xy(XY::new(0.0, f64::MAX)),
            ExactPoint::from_xy(XY::new(f64::from_bits(1), 0.0)),
        ];
        rays.sort_by(|left, right| angle_ccw_cmp(&origin, left, right).unwrap());
        assert_eq!(
            rays[0].round_nearest_even().unwrap(),
            XY::new(f64::from_bits(1), 0.0)
        );
        assert_eq!(
            rays[1].round_nearest_even().unwrap(),
            XY::new(0.0, f64::MAX)
        );
    }

    #[test]
    fn ratio_intervals_contain_extreme_exact_values() {
        let cases = [
            (ExactDyadic::from_f64(f64::from_bits(1)), ExactDyadic::one()),
            (ExactDyadic::from_f64(f64::MAX), ExactDyadic::one()),
            (ExactDyadic::one(), ExactDyadic::from_f64(f64::MAX)),
            (
                ExactDyadic::from_parts(false, vec![1, u64::MAX, 3, 7, 1], -211),
                ExactDyadic::from_f64(3.0),
            ),
            (
                ExactDyadic::from_parts(true, vec![1, 0, 0, 0, u64::MAX], 900),
                ExactDyadic::one(),
            ),
        ];
        for (numerator, denominator) in cases {
            verify_interval(
                &numerator,
                &denominator,
                ratio_interval(&numerator, &denominator),
            );
        }
    }

    #[test]
    fn filtered_predicates_agree_with_exact_and_cover_subnormals() {
        let tiny = f64::from_bits(1);
        let a = XY::new(0.0, 0.0);
        let b = XY::new(tiny, 0.0);
        let c = XY::new(1.0e-300, 1.0e-300);
        assert_eq!(orient2d(a, b, c), ExactSign::Positive);

        let ordinary = [
            XY::new(-3.0, 2.0),
            XY::new(5.0, -1.0),
            XY::new(4.0, 7.0),
            XY::new(-2.0, -6.0),
        ];
        assert_eq!(
            stored_orientation_sign(
                ordinary[0].x,
                ordinary[0].y,
                ordinary[1].x,
                ordinary[1].y,
                ordinary[2].x,
                ordinary[2].y,
            ),
            ExactSign::Positive
        );
        let filtered = incircle_filtered(ordinary[0], ordinary[1], ordinary[2], ordinary[3])
            .expect("ordinary incircle should hit the interval filter");
        assert_eq!(
            filtered,
            incircle_dyadic(ordinary[0], ordinary[1], ordinary[2], ordinary[3])
        );
    }

    #[test]
    fn binary64_frame_strictly_encloses_ordinary_points_and_can_decline() {
        let points = [
            ExactPoint::from_xy(XY::new(-2.0, -1.0)),
            ExactPoint::from_xy(XY::new(3.0, 4.0)),
            ExactPoint::from_xy(XY::new(1.0, 2.0)),
        ];
        let frame = enclosing_frame_binary64(points.iter()).expect("ordinary frame");
        for point in &points {
            assert!(point.compare_x(&frame[0]).is_gt());
            assert!(point.compare_y(&frame[0]).is_gt());
            assert!(point.compare_x(&frame[2]).is_lt());
            assert!(point.compare_y(&frame[2]).is_lt());
        }

        let extremes = [
            ExactPoint::from_xy(XY::new(-f64::MAX, 0.0)),
            ExactPoint::from_xy(XY::new(f64::MAX, 1.0)),
        ];
        assert!(enclosing_frame_binary64(extremes.iter()).is_none());
        enclosing_frame(&extremes).expect("exact fallback frame");
    }

    #[test]
    fn minimum_circle_support_predicates_use_exact_diameter_and_incircle_signs() {
        let diameter = [XY::new(-1.0, 0.0), XY::new(1.0, 0.0)];
        assert_eq!(
            diameter_contains_filtered(diameter[0], diameter[1], diameter[0]),
            None
        );
        assert_eq!(
            diameter_contains_filtered(diameter[0], diameter[1], XY::new(0.0, 0.5)),
            Some(true)
        );
        assert!(try_circle_contains(&diameter, XY::new(0.0, 1.0)).unwrap());
        assert!(!try_circle_contains(&diameter, XY::new(0.0, 1.0 + 2.5e-13)).unwrap());

        let acute = [XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(1.0, 3.0)];
        assert!(try_circle_contains(&acute, XY::new(2.0, 1.0)).unwrap());
        assert!(!try_circle_contains(&acute, XY::new(5.0, 5.0)).unwrap());
        assert!(try_certify_support(&acute, acute.into_iter()).unwrap());

        let obtuse = [XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(1.0, 0.5)];
        assert!(!try_certify_support(&obtuse, obtuse.into_iter()).unwrap());
    }

    #[test]
    fn minimum_circle_rounds_exact_midpoint_and_circumradius_without_recomputing() {
        let pair = [XY::new(0.0, 0.0), XY::new(4.0, 0.0)];
        let (center, radius, zero) = round_support_circle(&pair).unwrap();
        assert_eq!(center, XY::new(2.0, 0.0));
        assert_eq!(radius.to_bits(), 2.0_f64.to_bits());
        assert!(!zero);

        let triangle = [XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(0.0, 2.0)];
        let (center, radius, zero) = round_support_circle(&triangle).unwrap();
        assert_eq!(center, XY::new(2.0, 1.0));
        assert_eq!(radius.to_bits(), 5.0_f64.sqrt().to_bits());
        assert!(!zero);

        let singleton = [XY::new(-0.0, 3.0)];
        let (center, radius, zero) = round_support_circle(&singleton).unwrap();
        assert_eq!(center, XY::new(0.0, 3.0));
        assert_eq!(radius.to_bits(), 0.0_f64.to_bits());
        assert!(zero);
    }

    #[test]
    fn minimum_circle_diameter_filter_handles_mixed_exponents_without_allocation() {
        let scale = 2.0_f64.powi(200);
        let support = [
            XY::new(-scale, -scale),
            XY::new(scale, -scale + 2.0_f64.powi(148)),
        ];
        let point = XY::new(0.0, 0.0);
        let (result, allocations) =
            crate::test_support::allocation_count(|| try_circle_contains(&support, point));
        assert!(result.unwrap());
        assert_eq!(
            allocations, 0,
            "the certified interval path is allocation-free"
        );
    }

    #[test]
    fn minimum_circle_three_support_rounding_stays_fallible_at_mixed_exponents() {
        let scale = 2.0_f64.powi(200);
        let tiny = 2.0_f64.powi(-200);
        let support = [
            XY::new(-scale, 0.0),
            XY::new(scale, 0.0),
            XY::new(0.0, tiny),
        ];
        let (result, allocations) =
            crate::test_support::allocation_count(|| round_support_circle(&support));
        assert!(
            result.is_ok(),
            "mixed-exponent circumcircle is representable"
        );
        assert!(
            allocations > 0,
            "three-support exact construction must allocate"
        );
        let error =
            crate::test_support::fail_allocations_after(0, || round_support_circle(&support))
                .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
        ));
    }
}
