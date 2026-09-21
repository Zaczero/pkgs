//! Exact expansion arithmetic for the grid source.
//!
//! Shewchuk expansions over a compact limb buffer: a value is a sum of
//! non-overlapping doubles, so addition, subtraction and multiplication are
//! exact while they fit the bounded carrier. The source's fail-open contract
//! treats a value that exceeds that bound as arithmetic uncertainty.

use std::cmp::Ordering;

use super::FULL_TURN_DEGREES;
use crate::geometry::expansion::{expansion_sign, expansion_sum_zeroelim, two_product};
use crate::grid::spherical_arc::Bound;

/// Limb ceiling for one expansion.
pub(crate) const EXPANSION_LIMBS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactFailure {
    Arithmetic,
}

pub(crate) type ExactResult<T> = std::result::Result<T, ExactFailure>;

pub(super) fn exact<T>(value: Option<T>) -> ExactResult<T> {
    value.ok_or(ExactFailure::Arithmetic)
}

pub(super) fn exact_optional<T>(result: ExactResult<T>) -> Option<T> {
    result.ok()
}

/// Exact dyadic expansion with a bounded, initialized limb carrier. No epsilon
/// may ever choose a parent parameter.
pub(crate) const INLINE_EXPANSION_LIMBS: usize = 4;

#[derive(Clone, Debug)]
pub(crate) enum CompactLimbs {
    Inline {
        limbs: [f64; INLINE_EXPANSION_LIMBS],
        len: u8,
    },
    Heap(Vec<f64>),
}

impl CompactLimbs {
    pub(super) const fn zero() -> Self {
        Self::Inline {
            limbs: [0.0; INLINE_EXPANSION_LIMBS],
            len: 0,
        }
    }

    pub(super) const fn single(value: f64) -> Self {
        let mut limbs = [0.0; INLINE_EXPANSION_LIMBS];
        limbs[0] = value;
        Self::Inline { limbs, len: 1 }
    }

    pub(super) fn as_slice(&self) -> &[f64] {
        match self {
            Self::Inline { limbs, len } => &limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [f64] {
        match self {
            Self::Inline { limbs, len } => &mut limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    pub(super) fn is_zero(&self) -> bool {
        self.as_slice().iter().all(|&limb| limb == 0.0)
    }

    pub(super) fn clear(&mut self) {
        match self {
            Self::Inline { len, .. } => *len = 0,
            Self::Heap(limbs) => limbs.clear(),
        }
    }

    pub(super) fn prepare(&mut self, required: usize) -> ExactResult<()> {
        if required > EXPANSION_LIMBS {
            return Err(ExactFailure::Arithmetic);
        }
        match self {
            Self::Inline { len, .. } if required <= INLINE_EXPANSION_LIMBS => {
                *len = u8::try_from(required).map_err(|_| ExactFailure::Arithmetic)?;
            },
            Self::Inline { limbs, len } => {
                let mut expanded = vec![0.0; required];
                expanded[..usize::from(*len)].copy_from_slice(&limbs[..usize::from(*len)]);
                *self = Self::Heap(expanded);
            },
            Self::Heap(limbs) => {
                limbs.resize(required, 0.0);
            },
        }
        Ok(())
    }

    pub(super) fn truncate(&mut self, len: usize) -> ExactResult<()> {
        match self {
            Self::Inline { len: current, .. } => {
                *current = u8::try_from(len).map_err(|_| ExactFailure::Arithmetic)?;
            },
            Self::Heap(limbs) => limbs.truncate(len),
        }
        Ok(())
    }

    pub(super) fn canonicalize(&mut self) {
        if self.is_zero() {
            self.clear();
        }
    }

    pub(super) fn negated(&self) -> Self {
        let mut result = self.clone();
        for limb in result.as_mut_slice() {
            *limb = -*limb;
        }
        result.canonicalize();
        result
    }

    pub(super) fn into_negated(mut self) -> Self {
        for limb in self.as_mut_slice() {
            *limb = -*limb;
        }
        self.canonicalize();
        self
    }
}

impl PartialEq for CompactLimbs {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice() || (self.is_zero() && other.is_zero())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExactExpansion {
    pub(crate) limbs: CompactLimbs,
}

/// Sum two increasing-magnitude expansions into initialized `CompactLimbs`.
/// Capacity, prepare, truncate, and canonicalize stay here; the Shewchuk
/// merge is `expansion_sum_zeroelim`.
pub(super) fn sum_expansions<const NEGATE_RIGHT: bool>(
    left: &[f64],
    right: &[f64],
    output: &mut CompactLimbs,
) -> ExactResult<usize> {
    let required = left
        .len()
        .checked_add(right.len())
        .ok_or(ExactFailure::Arithmetic)?;
    if required > EXPANSION_LIMBS {
        return Err(ExactFailure::Arithmetic);
    }
    output.clear();
    output.prepare(required)?;
    let output_len = exact(expansion_sum_zeroelim::<NEGATE_RIGHT>(
        left,
        right,
        output.as_mut_slice(),
        true,
    ))?;
    output.truncate(output_len)?;
    output.canonicalize();
    Ok(output.as_slice().len())
}

impl ExactExpansion {
    pub(super) const fn zero() -> Self {
        Self {
            limbs: CompactLimbs::zero(),
        }
    }

    pub(super) fn from_f64(value: f64) -> ExactResult<Self> {
        if !value.is_finite() {
            return Err(ExactFailure::Arithmetic);
        }
        if value == 0.0 {
            return Ok(Self::zero());
        }
        Ok(Self {
            limbs: CompactLimbs::single(value),
        })
    }

    pub(super) fn from_i64(value: i64) -> ExactResult<Self> {
        const LOW_BITS: i64 = 1_i64 << 32;
        let high = value / LOW_BITS;
        let low = value % LOW_BITS;
        let low = Self::from_f64(low as f64)?;
        let high = Self::from_f64((high as f64) * (LOW_BITS as f64))?;
        low.add(&high)
    }

    pub(super) fn exact_degree_turns(turns: i64) -> ExactResult<Self> {
        let scale = Self::from_f64(FULL_TURN_DEGREES)?;
        Self::from_i64(turns)?.product(&scale)
    }

    pub(super) fn as_slice(&self) -> &[f64] {
        self.limbs.as_slice()
    }

    pub(super) fn as_f64(&self) -> Option<f64> {
        match self.as_slice() {
            [] => Some(0.0),
            [value] => Some(*value),
            _ => None,
        }
    }

    pub(super) fn neg(&self) -> Self {
        Self {
            limbs: self.limbs.negated(),
        }
    }

    pub(super) fn into_negated(mut self) -> Self {
        self.limbs = self.limbs.into_negated();
        self
    }

    pub(super) fn add(&self, rhs: &Self) -> ExactResult<Self> {
        let mut limbs = CompactLimbs::zero();
        sum_expansions::<false>(self.as_slice(), rhs.as_slice(), &mut limbs)?;
        Ok(Self { limbs })
    }

    pub(super) fn sub(&self, rhs: &Self) -> ExactResult<Self> {
        let mut limbs = CompactLimbs::zero();
        sum_expansions::<true>(self.as_slice(), rhs.as_slice(), &mut limbs)?;
        Ok(Self { limbs })
    }

    pub(super) fn product(&self, rhs: &Self) -> ExactResult<Self> {
        let mut result = CompactLimbs::zero();
        let mut scratch = CompactLimbs::zero();
        for &left in self.as_slice() {
            for &right in rhs.as_slice() {
                let (head, tail) = exact(two_product(left, right))?;
                if !head.is_finite() || !tail.is_finite() {
                    return Err(ExactFailure::Arithmetic);
                }
                let term = if tail == 0.0 {
                    [if head == 0.0 { 0.0 } else { head }, 0.0]
                } else if head != 0.0 {
                    [tail, head]
                } else {
                    [tail, 0.0]
                };
                let term_len = if tail == 0.0 {
                    usize::from(head != 0.0)
                } else {
                    1 + usize::from(head != 0.0)
                };
                if term_len == 0 {
                    continue;
                }
                if result.is_empty() {
                    result.prepare(term_len)?;
                    result.as_mut_slice()[..term_len].copy_from_slice(&term[..term_len]);
                } else {
                    sum_expansions::<false>(result.as_slice(), &term[..term_len], &mut scratch)?;
                    std::mem::swap(&mut result, &mut scratch);
                }
            }
        }
        Ok(Self { limbs: result })
    }

    pub(super) fn ordering(&self) -> Ordering {
        expansion_sign(self.as_slice())
    }

    pub(super) fn is_exact(&self, value: f64) -> ExactResult<bool> {
        let value = Self::from_f64(value)?;
        Ok(self.sub(&value)?.ordering() == Ordering::Equal)
    }

    pub(super) fn checked_estimate(&self) -> Option<f64> {
        self.as_slice().iter().try_fold(0.0, |sum, &limb| {
            let next = sum + limb;
            next.is_finite().then_some(next)
        })
    }
}

impl PartialEq for ExactExpansion {
    fn eq(&self, other: &Self) -> bool {
        self.limbs == other.limbs
    }
}

/// Exact ratio retained as source-parent identity.  `Bound` is intentionally
/// absent here: an outward enclosure may evaluate an arc, but cannot identify
/// which parent split produced that arc.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExactRatio {
    pub(super) numerator: ExactExpansion,
    pub(super) denominator: ExactExpansion,
}

impl ExactRatio {
    pub(super) fn new(
        numerator: &ExactExpansion,
        denominator: &ExactExpansion,
    ) -> ExactResult<Self> {
        match denominator.ordering() {
            Ordering::Equal => Err(ExactFailure::Arithmetic),
            Ordering::Greater => Ok(Self {
                numerator: numerator.clone(),
                denominator: denominator.clone(),
            }),
            Ordering::Less => Ok(Self {
                numerator: numerator.neg(),
                denominator: denominator.neg(),
            }),
        }
    }

    pub(super) fn from_parts(
        numerator: ExactExpansion,
        denominator: ExactExpansion,
    ) -> ExactResult<Self> {
        match denominator.ordering() {
            Ordering::Equal => Err(ExactFailure::Arithmetic),
            Ordering::Greater => Ok(Self {
                numerator,
                denominator,
            }),
            Ordering::Less => Ok(Self {
                numerator: numerator.into_negated(),
                denominator: denominator.into_negated(),
            }),
        }
    }

    pub(super) fn from_numerator(
        numerator: ExactExpansion,
        denominator: &ExactExpansion,
    ) -> ExactResult<Self> {
        match denominator.ordering() {
            Ordering::Equal => Err(ExactFailure::Arithmetic),
            Ordering::Greater => Ok(Self {
                numerator,
                denominator: denominator.clone(),
            }),
            Ordering::Less => Ok(Self {
                numerator: numerator.into_negated(),
                denominator: denominator.neg(),
            }),
        }
    }

    pub(super) fn from_i64(numerator: i64, denominator: i64) -> ExactResult<Self> {
        let numerator = ExactExpansion::from_i64(numerator)?;
        let denominator = ExactExpansion::from_i64(denominator)?;
        Self::from_parts(numerator, denominator)
    }

    pub(super) fn from_f64(value: f64) -> ExactResult<Self> {
        let numerator = ExactExpansion::from_f64(value)?;
        let denominator = ExactExpansion::from_i64(1)?;
        Self::from_parts(numerator, denominator)
    }

    pub(super) fn from_expansion(numerator: &ExactExpansion) -> ExactResult<Self> {
        let denominator = ExactExpansion::from_i64(1)?;
        Self::new(numerator, &denominator)
    }

    pub(super) fn add(&self, rhs: &Self) -> ExactResult<Self> {
        let left = self.numerator.product(&rhs.denominator)?;
        let right = rhs.numerator.product(&self.denominator)?;
        let numerator = left.add(&right)?;
        let denominator = self.denominator.product(&rhs.denominator)?;
        Self::from_parts(numerator, denominator)
    }

    pub(super) fn sub(&self, rhs: &Self) -> ExactResult<Self> {
        let left = self.numerator.product(&rhs.denominator)?;
        let right = rhs.numerator.product(&self.denominator)?;
        let numerator = left.sub(&right)?;
        let denominator = self.denominator.product(&rhs.denominator)?;
        Self::from_parts(numerator, denominator)
    }

    pub(super) fn mul(&self, rhs: &Self) -> ExactResult<Self> {
        let numerator = self.numerator.product(&rhs.numerator)?;
        let denominator = self.denominator.product(&rhs.denominator)?;
        Self::from_parts(numerator, denominator)
    }

    pub(super) fn div(&self, rhs: &Self) -> ExactResult<Self> {
        if rhs.numerator.ordering() == Ordering::Equal {
            return Err(ExactFailure::Arithmetic);
        }
        let numerator = self.numerator.product(&rhs.denominator)?;
        let denominator = self.denominator.product(&rhs.numerator)?;
        Self::from_parts(numerator, denominator)
    }

    pub(super) fn ordering(&self, rhs: &Self) -> ExactResult<Ordering> {
        let left = self.numerator.product(&rhs.denominator)?;
        let right = rhs.numerator.product(&self.denominator)?;
        Ok(left.sub(&right)?.ordering())
    }

    pub(super) fn is_closed_unit(&self) -> ExactResult<bool> {
        let zero = Self::from_i64(0, 1)?;
        let one = Self::from_i64(1, 1)?;
        Ok(self.ordering(&zero)? != Ordering::Less && self.ordering(&one)? != Ordering::Greater)
    }

    /// Returns an outward binary64 interval only after exact cross-products
    /// prove that the two stored endpoints enclose this parent identity.
    /// The enclosure never participates in ordering or endpoint identity.
    pub(super) fn certified_bound(&self) -> ExactResult<Bound> {
        if self.numerator.ordering() == Ordering::Equal {
            return exact(Bound::exact(0.0));
        }
        let numerator = exact(self.numerator.checked_estimate())?;
        let denominator = exact(self.denominator.checked_estimate())?;
        let proposal = numerator / denominator;
        if !proposal.is_finite() {
            return Err(ExactFailure::Arithmetic);
        }
        let mut lower = proposal.next_down();
        let mut upper = proposal.next_up();
        for _ in 0..=64 {
            let lower_ratio = Self::from_f64(lower)?;
            let upper_ratio = Self::from_f64(upper)?;
            if self.ordering(&lower_ratio)? == Ordering::Less {
                lower = lower.next_down();
                continue;
            }
            if self.ordering(&upper_ratio)? == Ordering::Greater {
                upper = upper.next_up();
                continue;
            }
            return exact(Bound::new(lower, upper));
        }
        Err(ExactFailure::Arithmetic)
    }

    /// Exact floor of this ratio divided by one full longitude turn.  The
    /// binary64 quotient merely proposes a nearby bucket; exact
    /// cross-products decide each correction.
    pub(super) fn floor_turn(&self) -> ExactResult<i64> {
        let proposal =
            exact(self.numerator.checked_estimate())? / exact(self.denominator.checked_estimate())?;
        let proposal = (proposal / FULL_TURN_DEGREES).floor();
        if !proposal.is_finite() || !(i64::MIN as f64..=i64::MAX as f64).contains(&proposal) {
            return Err(ExactFailure::Arithmetic);
        }
        let mut level = proposal as i64;
        for _ in 0..4 {
            let next = exact(level.checked_add(1))?;
            let next_ratio = Self::from_i64(exact(next.checked_mul(360))?, 1)?;
            if self.ordering(&next_ratio)? != Ordering::Less {
                level = next;
                continue;
            }
            let current_ratio =
                Self::from_i64(level.checked_mul(360).ok_or(ExactFailure::Arithmetic)?, 1)?;
            if self.ordering(&current_ratio)? == Ordering::Less {
                level = exact(level.checked_sub(1))?;
                continue;
            }
            return Ok(level);
        }
        Err(ExactFailure::Arithmetic)
    }
}
