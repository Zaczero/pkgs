//! Exact-ingress arithmetic owned by the geodesic certification lane.
//!
//! Stored binary64 values enter this module as an integer significand and a
//! checked binary exponent.  Exact operations stay dyadic; operations whose
//! mathematical result is not dyadic return an outward enclosure whose guard
//! and sticky information was obtained from integer quotient/remainder or a
//! verified integer square-root residual.  This module deliberately has no
//! dependency on the ordinary geodesic candidate implementation.

#![expect(
    dead_code,
    reason = "this private arithmetic spine is consumed by later certified geodesic layers"
)]
#![expect(
    unused_imports,
    reason = "the private certified re-exports are the interface for later geodesic layers"
)]

use std::cmp::Ordering;

use crate::crs::CrsError;
use crate::error::{Error, ErrorKind, ResourceError, Result};

mod natural;
mod sphere;
#[cfg(test)]
mod tests;
mod trig;

use natural::Magnitude;
pub(crate) use sphere::{
    Canonicalization, CertifiedBinary64, SphericalBranch, SphericalBranchIdentity,
    SphericalGeodesic, SphericalInput, SphericalModel, SphericalMultiplicity, SphericalPoint,
    classify_spherical_geodesic,
};
pub(crate) use trig::{
    AngleBracket, AngleBracketStatus, SineCosine, VectorSector, pi_bracket, sin_cos_degrees,
    sin_cos_radians, sin_cos_radians_enclosure, vector_angle_bracket,
};

fn numerical_indeterminacy() -> Error {
    ErrorKind::NumericalIndeterminacy.into()
}

pub(crate) fn numerical_indeterminacy_error() -> Error {
    numerical_indeterminacy()
}

fn exponent_overflow() -> Error {
    CrsError::GeodesicExponentOverflow.into()
}

pub(crate) fn geodesic_exponent_overflow_error() -> Error {
    exponent_overflow()
}

/// Caller-visible limits for one certified geodesic attempt.
///
/// `precision_bits` is the active significand budget.  `max_precision_bits`
/// is the largest precision to which a retry may be refined; it is separate
/// from the exponent range, fuel, and workspace budget.  `workspace_bytes` is
/// a per-attempt cumulative allocation-volume allowance for fallible word
/// reservations, not a peak-live-memory claim.  Allocator refusal remains
/// [`ResourceError::Allocation`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PrecisionPolicy {
    pub(crate) precision_bits: u32,
    pub(crate) max_precision_bits: u32,
    pub(crate) arithmetic_fuel: u64,
    /// Cumulative allocation volume allowed for one arithmetic attempt.
    pub(crate) workspace_bytes: usize,
    pub(crate) min_exponent: i64,
    pub(crate) max_exponent: i64,
}

impl PrecisionPolicy {
    // These are internal starting values, not a released accuracy or resource
    // contract.  Certification callers supply the policy that owns their
    // retry and deployment budget.
    pub(crate) const DEFAULT_PRECISION_BITS: u32 = 128;
    pub(crate) const DEFAULT_MAX_PRECISION_BITS: u32 = 4_096;
    pub(crate) const DEFAULT_ARITHMETIC_FUEL: u64 = 1_000_000;
    pub(crate) const DEFAULT_WORKSPACE_BYTES: usize = 16 * 1024 * 1024;

    pub(crate) const fn new(
        precision_bits: u32,
        max_precision_bits: u32,
        arithmetic_fuel: u64,
        workspace_bytes: usize,
    ) -> Self {
        Self {
            precision_bits,
            max_precision_bits,
            arithmetic_fuel,
            workspace_bytes,
            min_exponent: i64::MIN,
            max_exponent: i64::MAX,
        }
    }

    pub(crate) const fn with_exponent_range(
        mut self,
        min_exponent: i64,
        max_exponent: i64,
    ) -> Self {
        self.min_exponent = min_exponent;
        self.max_exponent = max_exponent;
        self
    }
}

impl Default for PrecisionPolicy {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_PRECISION_BITS,
            Self::DEFAULT_MAX_PRECISION_BITS,
            Self::DEFAULT_ARITHMETIC_FUEL,
            Self::DEFAULT_WORKSPACE_BYTES,
        )
    }
}

/// Mutable state for one precision attempt.  A retry is made by retaining
/// original [`ExactBinary64`] or [`Degree`] inputs and constructing a fresh
/// context (or calling [`Self::refine`] before recomputing from those inputs).
#[derive(Debug)]
pub(crate) struct PrecisionContext {
    policy: PrecisionPolicy,
    active_precision_bits: u32,
    fuel_remaining: u64,
    /// Cumulative allocation volume charged to this attempt.  Values retained
    /// by callers are outside this accounting; [`Self::refine`] starts a new
    /// attempt allowance rather than claiming to reclaim those values.
    workspace_used: usize,
}

impl PrecisionContext {
    pub(crate) fn new(policy: PrecisionPolicy) -> Result<Self> {
        if policy.precision_bits == 0
            || policy.max_precision_bits == 0
            || policy.precision_bits > policy.max_precision_bits
            || policy.min_exponent > policy.max_exponent
        {
            return Err(CrsError::message(
                "invalid certified geodesic precision policy",
            ));
        }
        Ok(Self {
            active_precision_bits: policy.precision_bits,
            fuel_remaining: policy.arithmetic_fuel,
            workspace_used: 0,
            policy,
        })
    }

    pub(crate) fn default_context() -> Self {
        // The default policy is a compile-time-valid constant policy.
        let policy = PrecisionPolicy::default();
        Self {
            active_precision_bits: policy.precision_bits,
            fuel_remaining: policy.arithmetic_fuel,
            workspace_used: 0,
            policy,
        }
    }

    pub(crate) const fn active_precision_bits(&self) -> u32 {
        self.active_precision_bits
    }

    pub(crate) const fn fuel_remaining(&self) -> u64 {
        self.fuel_remaining
    }

    /// Return cumulative allocation volume charged to this attempt.  This is
    /// not a measurement of all live memory after callers retain values.
    pub(crate) const fn workspace_used(&self) -> usize {
        self.workspace_used
    }

    pub(crate) const fn policy(&self) -> PrecisionPolicy {
        self.policy
    }

    /// Double the active precision, up to the policy's explicit maximum, and
    /// start fresh per-attempt fuel and allocation-volume accounting.  Callers
    /// must replay the operation from original exact ingress values after
    /// refining; retained source values are not reclaimed by this method.
    pub(crate) fn refine(&mut self) -> Result<()> {
        if self.active_precision_bits >= self.policy.max_precision_bits {
            return Err(numerical_indeterminacy());
        }
        self.active_precision_bits = self
            .active_precision_bits
            .saturating_mul(2)
            .min(self.policy.max_precision_bits);
        self.fuel_remaining = self.policy.arithmetic_fuel;
        self.workspace_used = 0;
        Ok(())
    }

    fn step(&mut self, units: u64) -> Result<()> {
        if self.fuel_remaining < units {
            return Err(numerical_indeterminacy());
        }
        self.fuel_remaining -= units;
        Ok(())
    }

    pub(crate) fn consume_fuel(&mut self, units: u64) -> Result<()> {
        self.step(units)
    }

    fn check_precision(&self, bits: usize) -> Result<()> {
        if u128::from(bits as u64) > u128::from(self.active_precision_bits) {
            return Err(numerical_indeterminacy());
        }
        Ok(())
    }

    pub(crate) fn require_precision(&self, bits: usize) -> Result<()> {
        self.check_precision(bits)
    }

    fn check_exponent(&self, exponent: i64) -> Result<()> {
        if exponent < self.policy.min_exponent || exponent > self.policy.max_exponent {
            return Err(exponent_overflow());
        }
        Ok(())
    }

    pub(crate) fn require_exponent(&self, exponent: i64) -> Result<()> {
        self.check_exponent(exponent)
    }

    /// Reserve additional `u64` words under the caller's allocation-volume
    /// policy.  The reservation is checked before calling the allocator, so
    /// policy exhaustion cannot be mistaken for an operating-system
    /// allocation failure.  Existing capacity costs no additional allowance.
    fn try_reserve_words(&mut self, vector: &mut Vec<u64>, additional: usize) -> Result<()> {
        let spare = vector.capacity().saturating_sub(vector.len());
        if additional <= spare {
            return Ok(());
        }
        let required = additional - spare;
        let bytes = required
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or(ResourceError::SizeOverflow)?;
        let used = self
            .workspace_used
            .checked_add(bytes)
            .ok_or(ResourceError::SizeOverflow)?;
        if used > self.policy.workspace_bytes {
            return Err(numerical_indeterminacy());
        }
        vector
            .try_reserve_exact(additional)
            .map_err(|_| Error::from(ResourceError::Allocation))?;
        self.workspace_used = used;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Sign {
    Negative,
    Zero,
    Positive,
}

impl Sign {
    const fn product(self, other: Self) -> Self {
        match (self, other) {
            (Self::Zero, _) | (_, Self::Zero) => Self::Zero,
            (Self::Negative, Self::Positive) | (Self::Positive, Self::Negative) => Self::Negative,
            (Self::Negative, Self::Negative) | (Self::Positive, Self::Positive) => Self::Positive,
        }
    }
}

/// A canonical signed integer times a power of two.
///
/// Nonzero significands are odd, so equivalent values have one stored form.
/// The sign and checked exponent remain separate from the significand.  The
/// common significand is an inline `u128`; only values wider than 128 bits use
/// a fallibly reserved vector.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Dyadic {
    sign: Sign,
    magnitude: Magnitude,
    exponent: i64,
}

impl Dyadic {
    pub(crate) const fn zero() -> Self {
        Self {
            sign: Sign::Zero,
            magnitude: Magnitude::zero(),
            exponent: 0,
        }
    }

    pub(crate) fn one(context: &mut PrecisionContext) -> Result<Self> {
        Self::from_parts(Sign::Positive, Magnitude::from_u128(1), 0, context)
    }

    pub(crate) fn two(context: &mut PrecisionContext) -> Result<Self> {
        Self::from_parts(Sign::Positive, Magnitude::from_u128(1), 1, context)
    }

    pub(crate) fn from_u64(value: u64, context: &mut PrecisionContext) -> Result<Self> {
        Self::from_parts(
            if value == 0 {
                Sign::Zero
            } else {
                Sign::Positive
            },
            Magnitude::from_u128(u128::from(value)),
            0,
            context,
        )
    }

    pub(crate) fn from_i128(value: i128, context: &mut PrecisionContext) -> Result<Self> {
        if value == 0 {
            return Ok(Self::zero());
        }
        let sign = if value < 0 {
            Sign::Negative
        } else {
            Sign::Positive
        };
        Self::from_parts(sign, Magnitude::from_u128(value.unsigned_abs()), 0, context)
    }

    pub(crate) fn from_f64(value: f64, context: &mut PrecisionContext) -> Result<Self> {
        ExactBinary64::from_f64(value)?.exact(context)
    }

    fn from_parts(
        sign: Sign,
        magnitude: Magnitude,
        exponent: i64,
        context: &mut PrecisionContext,
    ) -> Result<Self> {
        Self::from_parts_with_limit(
            sign,
            magnitude,
            exponent,
            Some(context.active_precision_bits()),
            context,
        )
    }

    /// Construct an exact scratch value without applying the active published
    /// precision limit.  The natural operations and allocation-volume policy
    /// still bound the work; the caller must narrow the value before it leaves
    /// an enclosure operation.
    fn from_parts_scratch(
        sign: Sign,
        magnitude: Magnitude,
        exponent: i64,
        context: &mut PrecisionContext,
    ) -> Result<Self> {
        Self::from_parts_with_limit(sign, magnitude, exponent, None, context)
    }

    fn from_parts_with_limit(
        sign: Sign,
        mut magnitude: Magnitude,
        mut exponent: i64,
        precision_limit: Option<u32>,
        context: &mut PrecisionContext,
    ) -> Result<Self> {
        if magnitude.is_zero() {
            return Ok(Self::zero());
        }
        if sign == Sign::Zero {
            return Err(CrsError::message(
                "nonzero certified dyadic cannot have zero sign",
            ));
        }
        let trailing = magnitude.trailing_zeros();
        if trailing != 0 {
            context.consume_fuel(
                u64::try_from(magnitude.bits()).map_err(|_| numerical_indeterminacy())?,
            )?;
            magnitude.shift_right_assign(trailing)?;
            let shift = i64::try_from(trailing).map_err(|_| exponent_overflow())?;
            exponent = exponent.checked_add(shift).ok_or_else(exponent_overflow)?;
        }
        context.check_exponent(exponent)?;
        if let Some(precision_limit) = precision_limit
            && magnitude.bits() > usize::try_from(precision_limit).unwrap_or(usize::MAX)
        {
            return Err(numerical_indeterminacy());
        }
        Ok(Self {
            sign,
            magnitude,
            exponent,
        })
    }

    pub(crate) fn try_clone(&self, context: &mut PrecisionContext) -> Result<Self> {
        Ok(Self {
            sign: self.sign,
            magnitude: self.magnitude.try_clone(context)?,
            exponent: self.exponent,
        })
    }

    pub(crate) const fn sign(&self) -> Sign {
        self.sign
    }

    pub(crate) const fn exponent(&self) -> i64 {
        self.exponent
    }

    pub(crate) fn significand_bits(&self) -> usize {
        self.magnitude.bits()
    }

    pub(crate) fn significand_u128(&self) -> Option<u128> {
        self.magnitude.to_u128()
    }

    pub(crate) fn significand_bit(&self, index: usize) -> bool {
        self.magnitude.bit(index)
    }

    pub(crate) fn significand_is_all_ones(&self) -> bool {
        self.magnitude.is_all_ones(self.magnitude.bits())
    }

    pub(crate) const fn is_zero(&self) -> bool {
        matches!(self.sign, Sign::Zero)
    }

    pub(crate) fn abs(&self, context: &mut PrecisionContext) -> Result<Self> {
        let mut result = self.try_clone(context)?;
        if result.sign == Sign::Negative {
            result.sign = Sign::Positive;
        }
        Ok(result)
    }

    pub(crate) fn negate(mut self) -> Self {
        self.sign = match self.sign {
            Sign::Negative => Sign::Positive,
            Sign::Positive => Sign::Negative,
            Sign::Zero => Sign::Zero,
        };
        self
    }

    fn add_exponent(context: &PrecisionContext, left: i64, right: i64) -> Result<i64> {
        let value = left.checked_add(right).ok_or_else(exponent_overflow)?;
        context.check_exponent(value)?;
        Ok(value)
    }

    fn abs_cmp(&self, other: &Self) -> Ordering {
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
        let left_top = i128::from(self.exponent) + self.significand_bits() as i128 - 1;
        let right_top = i128::from(other.exponent) + other.significand_bits() as i128 - 1;
        if left_top != right_top {
            return left_top.cmp(&right_top);
        }
        if self.exponent >= other.exponent {
            let shift = i128::from(self.exponent) - i128::from(other.exponent);
            let Ok(shift) = usize::try_from(shift) else {
                return Ordering::Greater;
            };
            self.magnitude.cmp_shifted(shift, &other.magnitude)
        } else {
            let shift = i128::from(other.exponent) - i128::from(self.exponent);
            let Ok(shift) = usize::try_from(shift) else {
                return Ordering::Less;
            };
            other
                .magnitude
                .cmp_shifted(shift, &self.magnitude)
                .reverse()
        }
    }

    pub(crate) fn add(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        self.add_with_limit(other, Some(context.active_precision_bits()), context)
    }

    fn add_scratch(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        self.add_with_limit(other, None, context)
    }

    fn add_with_limit(
        &self,
        other: &Self,
        precision_limit: Option<u32>,
        context: &mut PrecisionContext,
    ) -> Result<Self> {
        if self.is_zero() {
            if let Some(precision_limit) = precision_limit
                && other.significand_bits() > usize::try_from(precision_limit).unwrap_or(usize::MAX)
            {
                return Err(numerical_indeterminacy());
            }
            return other.try_clone(context);
        }
        if other.is_zero() {
            if let Some(precision_limit) = precision_limit
                && self.significand_bits() > usize::try_from(precision_limit).unwrap_or(usize::MAX)
            {
                return Err(numerical_indeterminacy());
            }
            return self.try_clone(context);
        }
        let base = self.exponent.min(other.exponent);
        let left_shift = usize::try_from(
            self.exponent
                .checked_sub(base)
                .ok_or_else(exponent_overflow)?,
        )
        .map_err(|_| exponent_overflow())?;
        let right_shift = usize::try_from(
            other
                .exponent
                .checked_sub(base)
                .ok_or_else(exponent_overflow)?,
        )
        .map_err(|_| exponent_overflow())?;
        let left = self.magnitude.shift_left(left_shift, context)?;
        let right = other.magnitude.shift_left(right_shift, context)?;
        if self.sign == other.sign {
            let magnitude = left.add(&right, context)?;
            return Self::from_parts_with_limit(
                self.sign,
                magnitude,
                base,
                precision_limit,
                context,
            );
        }
        match self.abs_cmp(other) {
            Ordering::Equal => Ok(Self::zero()),
            Ordering::Greater => {
                let magnitude = left.sub(&right, context)?;
                Self::from_parts_with_limit(self.sign, magnitude, base, precision_limit, context)
            },
            Ordering::Less => {
                let magnitude = right.sub(&left, context)?;
                Self::from_parts_with_limit(other.sign, magnitude, base, precision_limit, context)
            },
        }
    }

    pub(crate) fn sub(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        self.sub_with_limit(other, Some(context.active_precision_bits()), context)
    }

    fn sub_scratch(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        self.sub_with_limit(other, None, context)
    }

    fn sub_with_limit(
        &self,
        other: &Self,
        precision_limit: Option<u32>,
        context: &mut PrecisionContext,
    ) -> Result<Self> {
        let mut negated = other.try_clone(context)?;
        negated.sign = match negated.sign {
            Sign::Negative => Sign::Positive,
            Sign::Positive => Sign::Negative,
            Sign::Zero => Sign::Zero,
        };
        self.add_with_limit(&negated, precision_limit, context)
    }

    pub(crate) fn mul(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        self.mul_with_limit(other, Some(context.active_precision_bits()), context)
    }

    fn mul_scratch(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        self.mul_with_limit(other, None, context)
    }

    fn mul_with_limit(
        &self,
        other: &Self,
        precision_limit: Option<u32>,
        context: &mut PrecisionContext,
    ) -> Result<Self> {
        if self.is_zero() || other.is_zero() {
            return Ok(Self::zero());
        }
        let magnitude = self.magnitude.mul(&other.magnitude, context)?;
        let exponent = Self::add_exponent(context, self.exponent, other.exponent)?;
        Self::from_parts_with_limit(
            self.sign.product(other.sign),
            magnitude,
            exponent,
            precision_limit,
            context,
        )
    }

    pub(crate) fn square(&self, context: &mut PrecisionContext) -> Result<Self> {
        self.mul(self, context)
    }

    pub(crate) fn scale_pow2(&self, shift: i64, context: &mut PrecisionContext) -> Result<Self> {
        let mut result = self.try_clone(context)?;
        if result.is_zero() {
            return Ok(result);
        }
        result.exponent = Self::add_exponent(context, result.exponent, shift)?;
        Ok(result)
    }

    /// Round an exact scratch value to the active precision in a directed
    /// direction.  This is the only narrowing used for enclosure composition;
    /// exact ingress arithmetic continues to use [`Self::from_parts`] directly.
    fn round_outward(&self, downward: bool, context: &mut PrecisionContext) -> Result<Self> {
        if self.is_zero() {
            return self.try_clone(context);
        }
        let precision = usize::try_from(context.active_precision_bits())
            .map_err(|_| numerical_indeterminacy())?;
        let bits = self.significand_bits();
        if bits <= precision {
            return self.try_clone(context);
        }
        let drop = bits - precision;
        let mut magnitude = self.magnitude.shift_right(drop, context)?;
        let has_remainder = self.magnitude.any_below(drop);
        let round_up_magnitude = has_remainder
            && match self.sign {
                Sign::Positive => !downward,
                Sign::Negative => downward,
                Sign::Zero => false,
            };
        if round_up_magnitude {
            magnitude.add_one_assign(context)?;
        }
        let shift = i64::try_from(drop).map_err(|_| numerical_indeterminacy())?;
        let exponent = self
            .exponent
            .checked_add(shift)
            .ok_or_else(exponent_overflow)?;
        Self::from_parts(self.sign, magnitude, exponent, context)
    }

    fn round_scaled_integer(
        magnitude: &Magnitude,
        shift: i64,
        context: &mut PrecisionContext,
    ) -> Result<Magnitude> {
        if shift >= 0 {
            let shift = usize::try_from(shift).map_err(|_| numerical_indeterminacy())?;
            return magnitude.shift_left(shift, context);
        }
        let drop = shift.unsigned_abs();
        if drop > magnitude.bits() as u64 {
            return Ok(Magnitude::zero());
        }
        let drop = usize::try_from(drop).map_err(|_| numerical_indeterminacy())?;
        let mut quotient = magnitude.shift_right(drop, context)?;
        let guard = drop != 0 && magnitude.bit(drop - 1);
        let sticky = drop > 1 && magnitude.any_below(drop - 1);
        if guard && (sticky || quotient.is_odd()) {
            quotient.add_one_assign(context)?;
        }
        Ok(quotient)
    }

    /// Round an exact dyadic to binary64 with integer guard/sticky logic.
    /// `Overflow` means the nearest-even result is the infinity encoding, not
    /// that an intermediate integer reservation failed.
    pub(crate) fn round_binary64(&self, context: &mut PrecisionContext) -> Result<RoundedBinary64> {
        if self.is_zero() {
            return Ok(RoundedBinary64::Finite(0));
        }
        let bits = self.significand_bits();
        let top = i128::from(self.exponent) + bits as i128 - 1;
        let sign_bit = if self.sign == Sign::Negative {
            1_u64 << 63
        } else {
            0
        };
        if top < -1022 {
            let unit_shift = self
                .exponent
                .checked_add(1074)
                .ok_or_else(exponent_overflow)?;
            let units = Self::round_scaled_integer(&self.magnitude, unit_shift, context)?;
            let value = units.to_u128().ok_or_else(numerical_indeterminacy)?;
            if value == 0 {
                return Ok(RoundedBinary64::Finite(sign_bit));
            }
            if value >= (1_u128 << 52) {
                return Ok(RoundedBinary64::Finite(sign_bit | (1_u64 << 52)));
            }
            return Ok(RoundedBinary64::Finite(sign_bit | value as u64));
        }

        let bits_i64 = i64::try_from(bits).map_err(|_| numerical_indeterminacy())?;
        let drop = bits_i64 - 53;
        let significand = Self::round_scaled_integer(&self.magnitude, -drop, context)?
            .to_u128()
            .ok_or_else(numerical_indeterminacy)?;
        let mut significand = significand;
        let mut top = top;
        if significand >= (1_u128 << 53) {
            significand >>= 1;
            top = top.checked_add(1).ok_or_else(exponent_overflow)?;
        }
        if top > 1023 {
            return Ok(RoundedBinary64::Overflow(self.sign));
        }
        let exponent_field = u64::try_from(top + 1023).map_err(|_| exponent_overflow())?;
        let fraction = (significand - (1_u128 << 52)) as u64;
        Ok(RoundedBinary64::Finite(
            sign_bit | (exponent_field << 52) | fraction,
        ))
    }

    /// Outward enclosure of an exact nonnegative square root.  The integer
    /// square root is checked again through both residual inequalities before
    /// its dyadic endpoints are published.
    pub(crate) fn sqrt(&self, context: &mut PrecisionContext) -> Result<Enclosure> {
        if self.sign == Sign::Negative {
            return Err(CrsError::message(
                "certified dyadic square root requires a nonnegative value",
            ));
        }
        if self.is_zero() {
            return Enclosure::exact(Self::zero(), context);
        }
        let mut normalized = self.magnitude.try_clone(context)?;
        let mut exponent = self.exponent;
        if exponent % 2 != 0 {
            normalized.shift_left_assign(1, context)?;
            exponent = exponent.checked_sub(1).ok_or_else(exponent_overflow)?;
        }
        let root_bits = normalized.bits().div_ceil(2);
        let precision = usize::try_from(context.active_precision_bits())
            .map_err(|_| numerical_indeterminacy())?;
        if precision < root_bits {
            return Err(numerical_indeterminacy());
        }
        let scale = precision - root_bits;
        let scaled_shift = scale.checked_mul(2).ok_or(ResourceError::SizeOverflow)?;
        let scaled = normalized.shift_left(scaled_shift, context)?;
        let root = Magnitude::integer_sqrt(&scaled, context)?;
        let root_square = root.mul(&root, context)?;
        if root_square.cmp(&scaled) == Ordering::Greater {
            return Err(numerical_indeterminacy());
        }
        let residual = scaled.sub(&root_square, context)?;
        let base_exponent = exponent
            .checked_div(2)
            .and_then(|value| value.checked_sub(i64::try_from(scale).ok()?))
            .ok_or_else(exponent_overflow)?;
        let lower = Self::from_parts(
            Sign::Positive,
            root.try_clone(context)?,
            base_exponent,
            context,
        )?;
        if residual.is_zero() {
            return Enclosure::exact(lower, context);
        }
        let mut next = root;
        next.add_one_assign(context)?;
        let next_square = next.mul(&next, context)?;
        if next_square.cmp(&scaled) != Ordering::Greater {
            return Err(numerical_indeterminacy());
        }
        let upper = Self::from_parts(Sign::Positive, next, base_exponent, context)?;
        Ok(Enclosure::new_unchecked(lower, upper))
    }
}

impl Ord for Dyadic {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.sign != other.sign {
            return self.sign.cmp(&other.sign);
        }
        match self.sign {
            Sign::Zero => Ordering::Equal,
            Sign::Positive => self.abs_cmp(other),
            Sign::Negative => other.abs_cmp(self),
        }
    }
}

impl PartialOrd for Dyadic {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Result of final nearest-even binary64 rounding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RoundedBinary64 {
    Finite(u64),
    Overflow(Sign),
}

/// A closed interval whose endpoints are exact dyadics.
///
/// Public interval operations publish endpoints at the active precision.  To
/// avoid rejecting a product or cancellation merely because its exact scratch
/// significand is wider than that precision, they round the scratch result
/// outward after all comparisons have been made.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Enclosure {
    lo: Dyadic,
    hi: Dyadic,
}

impl Enclosure {
    pub(crate) fn exact(value: Dyadic, context: &mut PrecisionContext) -> Result<Self> {
        Ok(Self {
            lo: value.try_clone(context)?,
            hi: value,
        })
    }

    fn new_unchecked(lo: Dyadic, hi: Dyadic) -> Self {
        debug_assert!(lo <= hi);
        Self { lo, hi }
    }

    pub(crate) fn new(lo: Dyadic, hi: Dyadic) -> Option<Self> {
        (lo <= hi).then_some(Self { lo, hi })
    }

    pub(crate) fn try_clone(&self, context: &mut PrecisionContext) -> Result<Self> {
        Ok(Self {
            lo: self.lo.try_clone(context)?,
            hi: self.hi.try_clone(context)?,
        })
    }

    pub(crate) const fn lo(&self) -> &Dyadic {
        &self.lo
    }

    pub(crate) const fn hi(&self) -> &Dyadic {
        &self.hi
    }

    pub(crate) fn contains_zero(&self) -> bool {
        self.lo <= Dyadic::zero() && self.hi >= Dyadic::zero()
    }

    pub(crate) fn sign(&self) -> Option<Sign> {
        if self.lo.sign() == Sign::Positive && self.hi.sign() == Sign::Positive {
            Some(Sign::Positive)
        } else if self.lo.sign() == Sign::Negative && self.hi.sign() == Sign::Negative {
            Some(Sign::Negative)
        } else if self.lo.is_zero() && self.hi.is_zero() {
            Some(Sign::Zero)
        } else {
            None
        }
    }

    /// Return an ordering only when the intervals prove it.  Equality is
    /// reserved for equal exact singleton cells; matching nonsingleton bounds
    /// still describe independent unknown values.
    pub(crate) fn ordering(&self, other: &Self) -> Option<Ordering> {
        if self.hi < other.lo {
            Some(Ordering::Less)
        } else if self.lo > other.hi {
            Some(Ordering::Greater)
        } else if self.lo == self.hi && other.lo == other.hi && self.lo == other.lo {
            Some(Ordering::Equal)
        } else {
            None
        }
    }

    pub(crate) fn add(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        let lo = self
            .lo
            .add_scratch(&other.lo, context)?
            .round_outward(true, context)?;
        let hi = self
            .hi
            .add_scratch(&other.hi, context)?
            .round_outward(false, context)?;
        Ok(Self::new_unchecked(lo, hi))
    }

    pub(crate) fn scale_pow2(&self, shift: i64, context: &mut PrecisionContext) -> Result<Self> {
        let lo = self.lo.scale_pow2(shift, context)?;
        let hi = self.hi.scale_pow2(shift, context)?;
        Ok(Self::new_unchecked(lo, hi))
    }

    pub(crate) fn sub(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        let lo = self
            .lo
            .sub_scratch(&other.hi, context)?
            .round_outward(true, context)?;
        let hi = self
            .hi
            .sub_scratch(&other.lo, context)?
            .round_outward(false, context)?;
        Ok(Self::new_unchecked(lo, hi))
    }

    pub(crate) fn mul(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        let products = [
            self.lo.mul_scratch(&other.lo, context)?,
            self.lo.mul_scratch(&other.hi, context)?,
            self.hi.mul_scratch(&other.lo, context)?,
            self.hi.mul_scratch(&other.hi, context)?,
        ];
        let mut lo_index = 0;
        let mut hi_index = 0;
        for index in 1..products.len() {
            if products[index] < products[lo_index] {
                lo_index = index;
            }
            if products[index] > products[hi_index] {
                hi_index = index;
            }
        }
        let lo = products[lo_index].round_outward(true, context)?;
        let hi = products[hi_index].round_outward(false, context)?;
        Ok(Self::new_unchecked(lo, hi))
    }

    pub(crate) fn div(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        if other.lo.is_zero() && other.hi.is_zero() {
            return Err(CrsError::message("certified dyadic division by zero"));
        }
        if other.contains_zero() {
            return Err(numerical_indeterminacy());
        }
        let quotients = [
            self.lo.divide(&other.lo, context)?,
            self.lo.divide(&other.hi, context)?,
            self.hi.divide(&other.lo, context)?,
            self.hi.divide(&other.hi, context)?,
        ];
        let mut lo = &quotients[0].lo;
        let mut hi = &quotients[0].hi;
        for quotient in &quotients[1..] {
            if quotient.lo < *lo {
                lo = &quotient.lo;
            }
            if quotient.hi > *hi {
                hi = &quotient.hi;
            }
        }
        Ok(Self::new_unchecked(
            lo.try_clone(context)?,
            hi.try_clone(context)?,
        ))
    }

    pub(crate) fn sqrt(&self, context: &mut PrecisionContext) -> Result<Self> {
        if self.lo.sign() == Sign::Negative {
            return Err(CrsError::message(
                "certified enclosure square root requires a nonnegative lower endpoint",
            ));
        }
        let lo = self.lo.sqrt(context)?;
        let hi = self.hi.sqrt(context)?;
        Ok(Self::new_unchecked(
            lo.lo.try_clone(context)?,
            hi.hi.try_clone(context)?,
        ))
    }

    pub(crate) fn intersection(
        &self,
        other: &Self,
        context: &mut PrecisionContext,
    ) -> Result<Option<Self>> {
        if self.lo > other.hi || other.lo > self.hi {
            return Ok(None);
        }
        let lo = if self.lo >= other.lo {
            self.lo.try_clone(context)?
        } else {
            other.lo.try_clone(context)?
        };
        let hi = if self.hi <= other.hi {
            self.hi.try_clone(context)?
        } else {
            other.hi.try_clone(context)?
        };
        Ok(Self::new(lo, hi))
    }

    pub(crate) fn hull(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        let lo = if self.lo <= other.lo {
            self.lo.try_clone(context)?
        } else {
            other.lo.try_clone(context)?
        };
        let hi = if self.hi >= other.hi {
            self.hi.try_clone(context)?
        } else {
            other.hi.try_clone(context)?
        };
        Ok(Self::new_unchecked(lo, hi))
    }

    pub(crate) fn rounding_cell(&self, context: &mut PrecisionContext) -> Result<RoundingCell> {
        let lower = self.lo.round_binary64(context)?;
        let upper = self.hi.round_binary64(context)?;
        match (lower, upper) {
            (RoundedBinary64::Overflow(left), RoundedBinary64::Overflow(right))
                if left == right =>
            {
                Ok(RoundingCell::Overflow)
            },
            (RoundedBinary64::Overflow(_), _) | (_, RoundedBinary64::Overflow(_)) => {
                Ok(RoundingCell::Indeterminate)
            },
            (RoundedBinary64::Finite(lower), RoundedBinary64::Finite(upper)) => {
                if lower == upper {
                    Ok(RoundingCell::Unique(lower))
                } else {
                    Ok(RoundingCell::Indeterminate)
                }
            },
        }
    }
}

impl Dyadic {
    /// Outward quotient of two exact dyadics.  The scaled integer division
    /// retains the quotient and remainder; a nonzero remainder widens exactly
    /// one least-significant output unit.
    pub(crate) fn divide(
        &self,
        denominator: &Self,
        context: &mut PrecisionContext,
    ) -> Result<Enclosure> {
        if denominator.is_zero() {
            return Err(CrsError::message("certified dyadic division by zero"));
        }
        if self.is_zero() {
            return Enclosure::exact(Self::zero(), context);
        }
        let numerator_bits = self.significand_bits();
        let denominator_bits = denominator.significand_bits();
        let numerator_bits =
            i64::try_from(numerator_bits).map_err(|_| numerical_indeterminacy())?;
        let denominator_bits =
            i64::try_from(denominator_bits).map_err(|_| numerical_indeterminacy())?;
        let precision = i64::from(context.active_precision_bits());
        let mantissa_delta = numerator_bits - denominator_bits;
        let scale = precision - 1 - mantissa_delta;
        let (scaled_numerator, scaled_denominator) = if scale >= 0 {
            (
                self.magnitude.shift_left(
                    usize::try_from(scale).map_err(|_| numerical_indeterminacy())?,
                    context,
                )?,
                denominator.magnitude.try_clone(context)?,
            )
        } else {
            (
                self.magnitude.try_clone(context)?,
                denominator.magnitude.shift_left(
                    usize::try_from(scale.unsigned_abs()).map_err(|_| numerical_indeterminacy())?,
                    context,
                )?,
            )
        };
        let (quotient, remainder) =
            Magnitude::div_rem(&scaled_numerator, &scaled_denominator, context)?;
        let exponent = self
            .exponent
            .checked_sub(denominator.exponent)
            .and_then(|value| value.checked_sub(scale))
            .ok_or_else(exponent_overflow)?;
        let quotient_sign = self.sign.product(denominator.sign);
        let lower_abs = Self::from_parts(
            Sign::Positive,
            quotient.try_clone(context)?,
            exponent,
            context,
        )?;
        if remainder.is_zero() {
            return Enclosure::exact(
                if quotient_sign == Sign::Negative {
                    lower_abs.negate()
                } else {
                    lower_abs
                },
                context,
            );
        }
        let mut upper_quotient = quotient;
        upper_quotient.add_one_assign(context)?;
        let upper_abs = Self::from_parts(Sign::Positive, upper_quotient, exponent, context)?;
        if quotient_sign == Sign::Negative {
            Ok(Enclosure::new_unchecked(
                upper_abs.negate(),
                lower_abs.negate(),
            ))
        } else {
            Ok(Enclosure::new_unchecked(lower_abs, upper_abs))
        }
    }
}

/// The only non-ambiguous final rounding outcomes for an enclosure.
/// `Overflow` is emitted only when both endpoints occupy the same signed
/// infinity cell; a finite/overflow or opposite-sign pair is indeterminate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RoundingCell {
    Unique(u64),
    Indeterminate,
    Overflow,
}

/// Exact finite binary64 provenance.  The source bits are retained even when
/// the mathematical value is zero, so signed zero and every subnormal can be
/// replayed without passing through a rounded intermediate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExactBinary64 {
    bits: u64,
}

impl ExactBinary64 {
    pub(crate) fn from_bits(bits: u64) -> Result<Self> {
        if (bits >> 52) & 0x7FF == 0x7FF {
            return Err(CrsError::message("certified geodesic input must be finite"));
        }
        Ok(Self { bits })
    }

    pub(crate) fn from_f64(value: f64) -> Result<Self> {
        Self::from_bits(value.to_bits())
    }

    pub(crate) const fn bits(self) -> u64 {
        self.bits
    }

    pub(crate) const fn to_f64(self) -> f64 {
        f64::from_bits(self.bits)
    }

    pub(crate) fn exact(self, context: &mut PrecisionContext) -> Result<Dyadic> {
        let fraction = self.bits & ((1_u64 << 52) - 1);
        let encoded_exponent = ((self.bits >> 52) & 0x7FF) as i64;
        let sign = if self.bits >> 63 == 0 {
            Sign::Positive
        } else {
            Sign::Negative
        };
        if encoded_exponent == 0 && fraction == 0 {
            return Ok(Dyadic::zero());
        }
        let (significand, exponent) = if encoded_exponent == 0 {
            (fraction, -1074)
        } else {
            ((1_u64 << 52) | fraction, encoded_exponent - 1023 - 52)
        };
        Dyadic::from_parts(
            sign,
            Magnitude::from_u128(u128::from(significand)),
            exponent,
            context,
        )
    }
}

/// A degree-valued exact ingress token.  It intentionally exposes no radian
/// conversion: later certified trigonometry receives the original source
/// bits and recomputes its dyadic value at each precision attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Degree {
    source: ExactBinary64,
}

impl Degree {
    pub(crate) fn from_bits(bits: u64) -> Result<Self> {
        Ok(Self {
            source: ExactBinary64::from_bits(bits)?,
        })
    }

    pub(crate) fn from_f64(value: f64) -> Result<Self> {
        Self::from_bits(value.to_bits())
    }

    pub(crate) const fn bits(self) -> u64 {
        self.source.bits()
    }

    pub(crate) fn exact(self, context: &mut PrecisionContext) -> Result<Dyadic> {
        self.source.exact(context)
    }
}

/// Difference in endpoint order (`lambda2 - lambda1`) without binary64
/// subtraction.  The returned value can be replayed at a larger precision
/// because both degree tokens retain their original bits.
pub(crate) fn longitude_difference(
    lambda1: Degree,
    lambda2: Degree,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    let first = lambda1.exact(context)?;
    let second = lambda2.exact(context)?;
    second.sub(&first, context)
}

/// Reduce into the half-open mathematical interval `[-180°, 180°)`.
/// Keeping `-180°` as the seam representative makes a positive `180° + δ`
/// difference become `-180° + δ` without ever rounding the subtraction.
pub(crate) fn principal_longitude(
    value: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    if value.is_zero() {
        return Ok(Dyadic::zero());
    }
    let magnitude = positive_remainder_mod_360(value, context)?;
    if magnitude.is_zero() {
        return Ok(Dyadic::zero());
    }
    let half = Dyadic::from_i128(180, context)?;
    let full = Dyadic::from_i128(360, context)?;
    if value.sign() == Sign::Positive {
        if magnitude >= half {
            magnitude.sub(&full, context)
        } else {
            magnitude.try_clone(context)
        }
    } else if magnitude <= half {
        Ok(magnitude.negate())
    } else {
        full.sub(&magnitude, context)
    }
}

pub(crate) fn principal_longitude_difference(
    lambda1: Degree,
    lambda2: Degree,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    let difference = longitude_difference(lambda1, lambda2, context)?;
    principal_longitude(&difference, context)
}

fn positive_remainder_mod_360(value: &Dyadic, context: &mut PrecisionContext) -> Result<Dyadic> {
    let exponent = value.exponent();
    let magnitude = &value.magnitude;
    let remainder = if exponent >= 3 {
        let reduced = magnitude.mod_small(45, context)?;
        let shift = u64::try_from(exponent - 3).map_err(|_| exponent_overflow())?;
        const POW2_MOD_45: [u64; 12] = [1, 2, 4, 8, 16, 32, 19, 38, 31, 17, 34, 23];
        let factor = POW2_MOD_45[(shift % 12) as usize];
        (reduced * factor % 45) * 8
    } else {
        let shift = 3_i64.checked_sub(exponent).ok_or_else(exponent_overflow)?;
        let shift_usize = usize::try_from(shift).unwrap_or(usize::MAX);
        if Magnitude::from_u128(45).cmp_shifted(shift_usize, magnitude) == Ordering::Greater {
            return Dyadic::from_parts(
                Sign::Positive,
                magnitude.try_clone(context)?,
                exponent,
                context,
            );
        }
        let shift = shift_usize;
        let high = magnitude.shift_right(shift, context)?;
        let high_remainder = high.mod_small(45, context)?;
        let low = magnitude.low_bits(shift, context)?;
        let high_part =
            Magnitude::from_u128(u128::from(high_remainder)).shift_left(shift, context)?;
        let remainder = high_part.add(&low, context)?;
        return Dyadic::from_parts(Sign::Positive, remainder, exponent, context);
    };
    Dyadic::from_parts(
        Sign::Positive,
        Magnitude::from_u128(u128::from(remainder)),
        0,
        context,
    )
}
