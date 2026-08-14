//! Exact stored-binary64 predicates and homogeneous constructions.
//!
//! Every finite binary64 is an integer times a power of two. This module owns
//! that representation for tessellation so topology decisions and shared
//! constructed vertices never depend on a lossy coordinate frame.

#![allow(
    clippy::missing_const_for_fn,
    clippy::useless_conversion,
    reason = "exact predicates favor uniform fallible expressions over lint-driven local rewrites"
)]

use std::cmp::Ordering;

#[cfg(test)]
use crate::geometry::GENERATED_ITEM_LIMIT;
use crate::geometry::{GeometryErrorKind, Point, Result, XY};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum ExactSign {
    Negative,
    Zero,
    Positive,
}

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

/// Error-free sum (Knuth two-sum): `head + tail == left + right` exactly
/// whenever the rounded head is finite.  Callers that need a fallible lane
/// check that head before using the pair; the shared transform itself keeps
/// the infallible EFT contract used by the segment and rectangle kernels.
pub(in crate::geometry) fn two_sum(left: f64, right: f64) -> (f64, f64) {
    let sum = left + right;
    let right_virtual = sum - left;
    (
        sum,
        (left - (sum - right_virtual)) + (right - right_virtual),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactDyadic {
    negative: bool,
    limbs: Vec<u64>,
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

/// An exact homogeneous line `a*x + b*y + c = 0` over stored binary64 input.
///
/// Keeping the coefficients opaque prevents Pass C from mixing rounded and
/// exact construction while still exposing the complete construction ladder.
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

    pub(super) fn through_points(a: &ExactPoint, b: &ExactPoint) -> Result<Self> {
        if a.w.is_zero() || b.w.is_zero() || a.same_position(b) {
            return Err(GeometryErrorKind::voronoi(
                "exact support line requires two distinct finite points",
            ));
        }
        Ok(Self([
            a.y.product(&b.w).subtract(b.y.product(&a.w)),
            b.x.product(&a.w).subtract(a.x.product(&b.w)),
            a.x.product(&b.y).subtract(b.x.product(&a.y)),
        ]))
    }
}

/// V6: a virtual left-shifted view of a limb slice. `word(i)` reads the aligned
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
    const fn zero() -> Self {
        Self {
            negative: false,
            limbs: Vec::new(),
            exponent: 0,
        }
    }

    fn one() -> Self {
        Self {
            negative: false,
            limbs: vec![1],
            exponent: 0,
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
            limbs: vec![significand],
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
            self.limbs.drain(..zero_words);
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

    pub(in crate::geometry) const fn sign(&self) -> ExactSign {
        if self.is_zero() {
            ExactSign::Zero
        } else if self.negative {
            ExactSign::Negative
        } else {
            ExactSign::Positive
        }
    }

    const fn neg(mut self) -> Self {
        if !self.is_zero() {
            self.negative = !self.negative;
        }
        self
    }

    const fn abs(mut self) -> Self {
        self.negative = false;
        self
    }

    /// V6: read limb `index` of a slice as if it had been shifted left by
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

    fn add_aligned(left: Aligned<'_>, right: Aligned<'_>) -> Vec<u64> {
        let length = left.len.max(right.len);
        let mut result = Vec::with_capacity(length + 1);
        let mut carry = 0_u128;
        for index in 0..length {
            let sum = u128::from(left.word(index)) + u128::from(right.word(index)) + carry;
            result.push(sum as u64);
            carry = sum >> 64;
        }
        if carry != 0 {
            result.push(carry as u64);
        }
        result
    }

    fn subtract_aligned(larger: Aligned<'_>, smaller: Aligned<'_>) -> Vec<u64> {
        let mut result = Vec::with_capacity(larger.len);
        let mut borrow = 0_i128;
        for index in 0..larger.len {
            let difference =
                i128::from(larger.word(index)) - i128::from(smaller.word(index)) - borrow;
            if difference < 0 {
                result.push((difference + (1_i128 << 64)) as u64);
                borrow = 1;
            } else {
                result.push(difference as u64);
                borrow = 0;
            }
        }
        debug_assert_eq!(borrow, 0);
        result
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

    pub(in crate::geometry) fn subtract(self, other: Self) -> Self {
        self.add(other.neg())
    }

    pub(in crate::geometry) fn product(&self, other: &Self) -> Self {
        if self.is_zero() || other.is_zero() {
            return Self::zero();
        }
        // V6: schoolbook without the zero-fill. The first partial-product row is
        // written, later rows accumulate. Identical arithmetic; one fewer pass
        // and a plain (not zeroed) allocation.
        let (short, long) = if self.limbs.len() <= other.limbs.len() {
            (&self.limbs, &other.limbs)
        } else {
            (&other.limbs, &self.limbs)
        };
        let mut limbs: Vec<u64> = Vec::with_capacity(short.len() + long.len());
        let mut carry = 0_u128;
        for &right in long {
            let value = u128::from(short[0]) * u128::from(right) + carry;
            limbs.push(value as u64);
            carry = value >> 64;
        }
        limbs.push(carry as u64);
        for (left_index, &left) in short.iter().enumerate().skip(1) {
            let mut carry = 0_u128;
            for (right_index, &right) in long.iter().enumerate() {
                let index = left_index + right_index;
                let value = u128::from(limbs[index]) + u128::from(left) * u128::from(right) + carry;
                limbs[index] = value as u64;
                carry = value >> 64;
            }
            limbs.push(carry as u64);
        }
        let mut result = Self {
            negative: self.negative != other.negative,
            limbs,
            exponent: self.exponent + other.exponent,
        };
        result.trim();
        result
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
}

fn difference(left: f64, right: f64) -> ExactDyadic {
    ExactDyadic::from_f64(left).subtract(ExactDyadic::from_f64(right))
}

/// V10: interval filter for the stored-binary64 orientation.
///
/// The three inputs are exact binary64, so each starts as a degenerate
/// interval; every subsequent step rounds outward, and the resulting interval
/// therefore CONTAINS the exact determinant. A sign is returned only when the
/// interval excludes zero.
///
/// This is deliberately NOT `robust::orient2d`. Shewchuk's adaptive predicates
/// are exact only while their error-free transformations hold, which requires
/// no underflow: on the tessellation corpus's subnormal case
/// (`5e-324`/`1e-300` sites) `robust::orient2d` returns `0` for a determinant
/// that is genuinely negative — measured, not conjectured. gometry's
/// tessellation predicate is specified over the WHOLE binary64 range
/// (`exact_signs_cover_full_binary64_range` pins `f64::from_bits(1)`), so the
/// filter must degrade to the exact dyadic determinant there, and an outward
/// interval does exactly that: when every product underflows, the interval
/// straddles zero and declines.
fn orient2d_filtered(a: XY, b: XY, c: XY) -> Option<ExactSign> {
    let value = Interval::exact;
    let bax = value(b.x).sub(value(a.x));
    let bay = value(b.y).sub(value(a.y));
    let cax = value(c.x).sub(value(a.x));
    let cay = value(c.y).sub(value(a.y));
    if !(bax.is_finite() && bay.is_finite() && cax.is_finite() && cay.is_finite()) {
        return None;
    }
    let determinant = bax.mul(cay).sub(bay.mul(cax));
    determinant.sign()
}

pub(super) fn orient2d(a: XY, b: XY, c: XY) -> ExactSign {
    if let Some(sign) = orient2d_filtered(a, b, c) {
        if VERIFY_INTERVALS {
            assert_eq!(
                sign,
                orient2d_dyadic(a, b, c),
                "filtered orient2d disagrees"
            );
        }
        return sign;
    }
    orient2d_dyadic(a, b, c)
}

fn orient2d_dyadic(a: XY, b: XY, c: XY) -> ExactSign {
    difference(b.x, a.x)
        .product(&difference(c.y, a.y))
        .subtract(difference(b.y, a.y).product(&difference(c.x, a.x)))
        .sign()
}

pub(crate) fn incircle_sign(a: XY, b: XY, c: XY, d: XY) -> i8 {
    incircle(a, b, c, d).as_i8()
}

/// V10: the same interval filter for the in-circle determinant, with the same
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

pub(super) fn incircle(a: XY, b: XY, c: XY, d: XY) -> ExactSign {
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

// ---------------------------------------------------------------------------
// V8: certified interval filter.
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
    fn compare(self, other: Self) -> Option<Ordering> {
        if self.hi < other.lo {
            Some(Ordering::Less)
        } else if self.lo > other.hi {
            Some(Ordering::Greater)
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
    x: ExactDyadic,
    y: ExactDyadic,
    w: ExactDyadic,
    /// Certified outward-rounded bounds on `x/w` and `y/w`.
    bounds: [Interval; 2],
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
        Self { x, y, w, bounds }
    }
}

/// Self-check hook: prove `lo <= numerator/denominator <= hi` with the exact
/// comparator itself. Off in shipped builds; turned on to validate the bound
/// derivation over the whole deterministic corpus.
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
        Self::homogeneous(
            ExactDyadic::from_f64(point.x),
            ExactDyadic::from_f64(point.y),
            ExactDyadic::one(),
        )
    }

    pub(super) fn compare_lex(&self, other: &Self) -> Ordering {
        self.compare_x(other).then_with(|| self.compare_y(other))
    }

    pub(super) fn same_position(&self, other: &Self) -> bool {
        !self.separated_from(other)
            && self.compare_x(other).is_eq()
            && self.compare_y(other).is_eq()
    }

    pub(super) fn is_finite(&self) -> bool {
        !self.w.is_zero()
    }

    pub(super) fn midpoint(&self, other: &Self) -> Self {
        let denominator = self
            .w
            .product(&other.w)
            .product(&ExactDyadic::from_f64(2.0));
        Self::homogeneous(
            self.x.product(&other.w).add(other.x.product(&self.w)),
            self.y.product(&other.w).add(other.y.product(&self.w)),
            denominator,
        )
    }
    pub(super) fn compare_x(&self, other: &Self) -> Ordering {
        self.bounds[0]
            .compare(other.bounds[0])
            .unwrap_or_else(|| compare_ratios(&self.x, &self.w, &other.x, &other.w))
    }

    pub(super) fn compare_y(&self, other: &Self) -> Ordering {
        self.bounds[1]
            .compare(other.bounds[1])
            .unwrap_or_else(|| compare_ratios(&self.y, &self.w, &other.y, &other.w))
    }

    /// Certified negative: two points whose x- or y-intervals are strictly
    /// separated cannot be the same position.
    pub(super) fn separated_from(&self, other: &Self) -> bool {
        self.bounds[0].disjoint_from(other.bounds[0])
            || self.bounds[1].disjoint_from(other.bounds[1])
    }

    /// Materialize one shared exact vertex exactly once. Pass C must intern the
    /// returned coordinate by vertex identity and reuse it for every incident
    /// half-edge. Under gometry's topological-snap contract a clip event may
    /// differ from the source clip at the ULP level when exact incidence has no
    /// binary64 representation; validity, event order, empty differences, and
    /// interior disjointness remain mandatory, while bit-exact `equals` does
    /// not. Per-cell calls would recreate the Round-20 crack.
    pub(super) fn round_nearest_even(&self) -> Result<XY> {
        if self.w.is_zero() {
            return Err(GeometryErrorKind::voronoi("exact vertex lies at infinity"));
        }
        Ok(XY::new(
            round_ratio(&self.x, &self.w)?,
            round_ratio(&self.y, &self.w)?,
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
            .y
            .product(&origin.w)
            .subtract(origin.y.product(&target.w));
        let dx = target
            .x
            .product(&origin.w)
            .subtract(origin.x.product(&target.w));
        let denominator_sign = target.w.sign().as_i8() * origin.w.sign().as_i8();
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

/// V11: the shoelace sign in interval arithmetic. Every term is the cross
/// product of two affine vertices, each inside its certified interval, so the
/// accumulated interval contains the exact signed area; a sign is returned only
/// when it excludes zero. Declining falls through to the exact accumulation,
/// which is also the only path that can report the zero-orientation error.
fn cycle_orientation_filtered(open_cycle: &[ExactPoint]) -> Option<ExactSign> {
    let mut total = Interval::exact(0.0);
    for index in 0..open_cycle.len() {
        let left = &open_cycle[index];
        let right = &open_cycle[(index + 1) % open_cycle.len()];
        if left.w.is_zero() || right.w.is_zero() {
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
        let term_n = left.x.product(&right.y).subtract(left.y.product(&right.x));
        let term_d = left.w.product(&right.w);
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

/// V11: the same filter for "which stored site is nearer this exact point".
/// Comparing the two affine squared distances is equivalent to the exact
/// homogeneous form (both are scaled by the same positive `w^2`).
fn squared_distance_cmp_point_filtered(
    origin: &ExactPoint,
    left: XY,
    right: XY,
) -> Option<Ordering> {
    if origin.w.is_zero() {
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
    let squared = |point: XY| {
        let x = ExactDyadic::from_f64(point.x)
            .product(&origin.w)
            .subtract(origin.x.clone());
        let y = ExactDyadic::from_f64(point.y)
            .product(&origin.w)
            .subtract(origin.y.clone());
        x.square().add(y.square())
    };
    squared(left).cmp(&squared(right))
}

/// V3: a private frame whose four corners are stored binary64 (`w == 1`).
///
/// The frame is private scaffolding. The ONLY property the pipeline needs from
/// it is that it strictly encloses every input point. That property is
/// established here by the same exact comparator the rest of the module uses,
/// so the cheap construction is CERTIFIED, not assumed. `None` means no
/// admissible binary64 box exists and the caller must fall back to the exact
/// degree-8 construction.
pub(super) fn enclosing_frame_binary64(points: &[ExactPoint]) -> Option<[ExactPoint; 4]> {
    // Seeding only. A wrong estimate costs extra certified widening rounds,
    // never a wrong frame.
    let approx = |numerator: &ExactDyadic, denominator: &ExactDyadic| -> Option<f64> {
        let magnitude = approximate_ratio(&numerator.clone().abs(), &denominator.clone().abs())?;
        let negative = numerator.negative != denominator.negative;
        Some(if negative { -magnitude } else { magnitude })
    };
    let mut lo = [f64::INFINITY; 2];
    let mut hi = [f64::NEG_INFINITY; 2];
    for point in points {
        if point.w.is_zero() {
            return None;
        }
        let coordinates = [approx(&point.x, &point.w)?, approx(&point.y, &point.w)?];
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
        for point in points {
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
        .x
        .product(&min_x.w)
        .subtract(min_x.x.product(&max_x.w));
    let span_x_d = max_x.w.product(&min_x.w);
    let span_y_n = max_y
        .y
        .product(&min_y.w)
        .subtract(min_y.y.product(&max_y.w));
    let span_y_d = max_y.w.product(&min_y.w);
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
    let lx = coordinate(&min_x.x, &min_x.w, false);
    let ux = coordinate(&max_x.x, &max_x.w, true);
    let ly = coordinate(&min_y.y, &min_y.w, false);
    let uy = coordinate(&max_y.y, &max_y.w, true);
    let make = |x: &ExactPoint, y: &ExactPoint| {
        ExactPoint::homogeneous(x.x.product(&y.w), y.x.product(&x.w), x.w.product(&y.w))
    };
    Ok([
        make(&lx, &ly),
        make(&ux, &ly),
        make(&ux, &uy),
        make(&lx, &uy),
    ])
}

pub(super) fn rectangular_boundary(points: &[Point], padded: bool) -> Result<[ExactPoint; 4]> {
    let first = points
        .first()
        .ok_or_else(|| GeometryErrorKind::voronoi("empty Voronoi site set"))?;
    let (mut min_x, mut max_x) = (
        ExactDyadic::from_f64(first.x),
        ExactDyadic::from_f64(first.x),
    );
    let (mut min_y, mut max_y) = (
        ExactDyadic::from_f64(first.y),
        ExactDyadic::from_f64(first.y),
    );
    for point in &points[1..] {
        let x = ExactDyadic::from_f64(point.x);
        let y = ExactDyadic::from_f64(point.y);
        if x.cmp(&min_x).is_lt() {
            min_x = x.clone();
        }
        if x.cmp(&max_x).is_gt() {
            max_x = x;
        }
        if y.cmp(&min_y).is_lt() {
            min_y = y.clone();
        }
        if y.cmp(&max_y).is_gt() {
            max_y = y;
        }
    }
    if padded {
        let span_x = max_x.clone().subtract(min_x.clone());
        let span_y = max_y.clone().subtract(min_y.clone());
        let span = if span_x.cmp(&span_y).is_gt() {
            span_x
        } else {
            span_y
        };
        let half = span.product(&ExactDyadic::from_f64(0.5));
        min_x = min_x.subtract(half.clone());
        max_x = max_x.add(half.clone());
        min_y = min_y.subtract(half.clone());
        max_y = max_y.add(half);
    }
    let point = |x: ExactDyadic, y: ExactDyadic| ExactPoint::homogeneous(x, y, ExactDyadic::one());
    Ok([
        point(min_x.clone(), min_y.clone()),
        point(max_x.clone(), min_y),
        point(max_x, max_y.clone()),
        point(min_x, max_y),
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
    if a.w.is_zero() || b.w.is_zero() || c.w.is_zero() {
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
    let bax = b.x.product(&a.w).subtract(a.x.product(&b.w));
    let bay = b.y.product(&a.w).subtract(a.y.product(&b.w));
    let cax = c.x.product(&a.w).subtract(a.x.product(&c.w));
    let cay = c.y.product(&a.w).subtract(a.y.product(&c.w));
    let mut sign = bax.product(&cay).subtract(bay.product(&cax)).sign();
    // Both displacement numerators carry one factor of `a.w`; it is
    // squared in the determinant and therefore cannot affect the sign.
    let denominator_sign = b.w.sign().as_i8() * c.w.sign().as_i8();
    if denominator_sign < 0 {
        sign = match sign {
            ExactSign::Negative => ExactSign::Positive,
            ExactSign::Zero => ExactSign::Zero,
            ExactSign::Positive => ExactSign::Negative,
        };
    }
    sign
}

/// Cross product of two exact homogeneous lines. The returned point is not
/// normalized: signed products and ratio comparison operate directly on it.
pub(super) fn line_intersection(a: &ExactLine, b: &ExactLine) -> ExactPoint {
    let a = &a.0;
    let b = &b.0;
    ExactPoint::homogeneous(
        a[1].product(&b[2]).subtract(a[2].product(&b[1])),
        a[2].product(&b[0]).subtract(a[0].product(&b[2])),
        a[0].product(&b[1]).subtract(a[1].product(&b[0])),
    )
}

pub(super) fn signed_line_product(line: &ExactLine, point: &ExactPoint) -> ExactSign {
    let sign = line.0[0]
        .product(&point.x)
        .add(line.0[1].product(&point.y))
        .add(line.0[2].product(&point.w))
        .sign();
    if point.w.sign() == ExactSign::Negative {
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

pub(super) fn squared_distance_cmp(origin: XY, left: XY, right: XY) -> Ordering {
    let squared = |point: XY| {
        let dx = difference(point.x, origin.x);
        let dy = difference(point.y, origin.y);
        dx.square().add(dy.square())
    };
    squared(left).cmp(&squared(right))
}

pub(super) fn distance_within(origin: XY, point: XY, radius: f64) -> bool {
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
    Ok(line_intersection(
        &ExactLine::perpendicular_bisector(a, b),
        &ExactLine::perpendicular_bisector(a, c),
    ))
}

fn approximate_ratio(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Option<f64> {
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

fn exact_ratio_bracket(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    proposal: f64,
) -> Option<(u64, u64)> {
    const MAX_ULP_STEPS: usize = 4;
    let max_bits = f64::MAX.to_bits();
    let compare = |bits: u64| {
        ExactDyadic::from_f64(f64::from_bits(bits))
            .product(denominator)
            .cmp(numerator)
    };
    let mut bits = proposal.to_bits();
    match compare(bits) {
        Ordering::Equal => return Some((bits, bits)),
        Ordering::Less => {
            for _ in 0..MAX_ULP_STEPS {
                if bits == max_bits {
                    return None;
                }
                let lower = bits;
                bits += 1;
                match compare(bits) {
                    Ordering::Less => {},
                    Ordering::Equal => return Some((bits, bits)),
                    Ordering::Greater => return Some((lower, bits)),
                }
            }
        },
        Ordering::Greater => {
            for _ in 0..MAX_ULP_STEPS {
                if bits == 0 {
                    return None;
                }
                let upper = bits;
                bits -= 1;
                match compare(bits) {
                    Ordering::Greater => {},
                    Ordering::Equal => return Some((bits, bits)),
                    Ordering::Less => return Some((bits, upper)),
                }
            }
        },
    }
    None
}

fn round_ratio_binary(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Result<f64> {
    let sign = numerator.sign().quotient(denominator.sign());
    let numerator = numerator.clone().abs();
    let denominator = denominator.clone().abs();
    if numerator.is_zero() {
        return Ok(0.0);
    }
    let max = ExactDyadic::from_f64(f64::MAX).product(&denominator);
    if numerator.cmp(&max) == Ordering::Greater {
        let overflow_midpoint = ExactDyadic::from_f64(f64::MAX)
            .add(ExactDyadic {
                negative: false,
                limbs: vec![1],
                exponent: 970,
            })
            .product(&denominator);
        if numerator.cmp(&overflow_midpoint) == Ordering::Less {
            return Ok(if sign == ExactSign::Negative {
                -f64::MAX
            } else {
                f64::MAX
            });
        }
        return Err(GeometryErrorKind::voronoi(
            "exact vertex is outside the finite binary64 range",
        ));
    }
    let mut low = 0_u64;
    let mut high = f64::MAX.to_bits();
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        let product = ExactDyadic::from_f64(f64::from_bits(mid)).product(&denominator);
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
        let twice_numerator = numerator.product(&ExactDyadic::from_f64(2.0));
        let midpoint_scaled = ExactDyadic::from_f64(lower)
            .add(ExactDyadic::from_f64(upper))
            .product(&denominator);
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
    let sign = numerator.sign().quotient(denominator.sign());
    let magnitude_numerator = numerator.clone().abs();
    let magnitude_denominator = denominator.clone().abs();
    if let Some(proposal) = approximate_ratio(&magnitude_numerator, &magnitude_denominator)
        && let Some((low, high)) =
            exact_ratio_bracket(&magnitude_numerator, &magnitude_denominator, proposal)
    {
        let lower = f64::from_bits(low);
        let upper = f64::from_bits(high);
        let twice_numerator = magnitude_numerator.product(&ExactDyadic::from_f64(2.0));
        let midpoint_scaled = ExactDyadic::from_f64(lower)
            .add(ExactDyadic::from_f64(upper))
            .product(&magnitude_denominator);
        let rounded = match twice_numerator.cmp(&midpoint_scaled) {
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

#[cfg(test)]
mod tests {
    use super::*;

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
            round_ratio(&one, &ExactDyadic {
                negative: false,
                limbs: vec![1],
                exponent: 1074
            })
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
        let below_overflow = ExactDyadic::from_f64(f64::MAX).add(ExactDyadic {
            negative: false,
            limbs: vec![1],
            exponent: 969,
        });
        let overflow_tie = ExactDyadic::from_f64(f64::MAX).add(ExactDyadic {
            negative: false,
            limbs: vec![1],
            exponent: 970,
        });
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

        let beyond_i32 = ExactDyadic {
            negative: false,
            limbs: vec![1],
            exponent: i64::from(i32::MAX) + 1,
        };
        assert_eq!(beyond_i32.square().exponent, 2 * (i64::from(i32::MAX) + 1));
        let bound = 1_127_u128 * (1_u128 << 20) * (GENERATED_ITEM_LIMIT as u128 + 1);
        assert!(bound < i64::MAX as u128);
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
        assert_eq!(
            round_ratio_binary(&zero, &negative_denominator)
                .unwrap()
                .to_bits(),
            0.0_f64.to_bits()
        );
    }

    #[test]
    fn nonzero_ratio_is_invariant_under_projective_sign_change() {
        let numerator = ExactDyadic::from_f64(5.0);
        let denominator = ExactDyadic::from_f64(3.0);
        let expected = round_ratio(&numerator, &denominator).unwrap().to_bits();
        assert_eq!(
            round_ratio(&numerator.neg(), &denominator.neg())
                .unwrap()
                .to_bits(),
            expected
        );
    }

    #[test]
    fn nonzero_negative_ratio_underflow_retains_negative_zero() {
        let tiny_negative = ExactDyadic {
            negative: true,
            limbs: vec![1],
            exponent: -1075,
        };
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
            (reversed.x.to_bits(), reversed.y.to_bits())
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
    fn distinct_exact_vertices_can_collide_after_rounding() {
        let one = ExactDyadic::one();
        let base =
            ExactPoint::homogeneous(ExactDyadic::from_f64(1.0), ExactDyadic::zero(), one.clone());
        let shifted = ExactPoint::homogeneous(
            ExactDyadic::from_f64(1.0).add(ExactDyadic {
                negative: false,
                limbs: vec![1],
                exponent: -1074,
            }),
            ExactDyadic::zero(),
            one,
        );
        assert_ne!(base.x, shifted.x);
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
                ExactDyadic {
                    negative: false,
                    limbs: vec![1, u64::MAX, 3, 7, 1],
                    exponent: -211,
                },
                ExactDyadic::from_f64(3.0),
            ),
            (
                ExactDyadic {
                    negative: true,
                    limbs: vec![1, 0, 0, 0, u64::MAX],
                    exponent: 900,
                },
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
        assert_eq!(orient2d_filtered(a, b, c), None);

        let ordinary = [
            XY::new(-3.0, 2.0),
            XY::new(5.0, -1.0),
            XY::new(4.0, 7.0),
            XY::new(-2.0, -6.0),
        ];
        let filtered = orient2d_filtered(ordinary[0], ordinary[1], ordinary[2])
            .expect("ordinary orientation should hit the interval filter");
        assert_eq!(
            filtered,
            orient2d_dyadic(ordinary[0], ordinary[1], ordinary[2])
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
        let frame = enclosing_frame_binary64(&points).expect("ordinary frame");
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
        assert!(enclosing_frame_binary64(&extremes).is_none());
        enclosing_frame(&extremes).expect("exact fallback frame");
    }
}
