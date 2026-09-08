use std::cmp::Ordering;

use super::PrecisionContext;
use crate::error::Result;

fn fuel_units(work: usize) -> Result<u64> {
    u64::try_from(work).map_err(|_| super::numerical_indeterminacy())
}

/// A non-negative integer.  Values encountered by the ordinary binary64
/// path fit in the inline arm; a wider value is published only after its
/// storage has been fallibly reserved.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Magnitude {
    Inline(u128),
    Wide(Vec<u64>),
}

impl Magnitude {
    pub(super) const fn zero() -> Self {
        Self::Inline(0)
    }

    pub(super) const fn from_u128(value: u128) -> Self {
        Self::Inline(value)
    }

    pub(super) fn try_clone(&self, context: &mut PrecisionContext) -> Result<Self> {
        match self {
            Self::Inline(value) => Ok(Self::Inline(*value)),
            Self::Wide(limbs) if limbs.is_empty() => Ok(Self::zero()),
            Self::Wide(limbs) => {
                context.step(fuel_units(limbs.len())?)?;
                let mut copy = Vec::new();
                context.try_reserve_words(&mut copy, limbs.len())?;
                copy.extend_from_slice(limbs);
                Ok(Self::Wide(copy))
            },
        }
    }

    pub(super) fn from_limbs(limbs: &[u64], context: &mut PrecisionContext) -> Result<Self> {
        let len = limbs
            .iter()
            .rposition(|limb| *limb != 0)
            .map_or(0, |index| index + 1);
        if len == 0 {
            return Ok(Self::zero());
        }
        context.step(fuel_units(len)?)?;
        if len <= 2 {
            let low = u128::from(limbs[0]);
            let high = if len == 2 {
                u128::from(limbs[1]) << 64
            } else {
                0
            };
            return Ok(Self::Inline(low | high));
        }
        let mut copy = Vec::new();
        context.try_reserve_words(&mut copy, len)?;
        copy.extend_from_slice(&limbs[..len]);
        Ok(Self::Wide(copy))
    }

    fn from_owned_limbs(mut limbs: Vec<u64>) -> Result<Self> {
        while limbs.last() == Some(&0) {
            limbs.pop();
        }
        if limbs.is_empty() {
            return Ok(Self::zero());
        }
        if limbs.len() <= 2 {
            let low = u128::from(limbs[0]);
            let high = if limbs.len() == 2 {
                u128::from(limbs[1]) << 64
            } else {
                0
            };
            return Ok(Self::Inline(low | high));
        }
        // The vector was reserved by the caller.  Keeping it in the wide arm
        // is the publication boundary; no infallible copy is needed here.
        Ok(Self::Wide(limbs))
    }

    pub(super) const fn is_zero(&self) -> bool {
        match self {
            Self::Inline(value) => *value == 0,
            Self::Wide(limbs) => limbs.is_empty(),
        }
    }

    pub(super) fn bits(&self) -> usize {
        match self {
            Self::Inline(value) => {
                if *value == 0 {
                    0
                } else {
                    128 - value.leading_zeros() as usize
                }
            },
            Self::Wide(limbs) => {
                let Some(&high) = limbs.last() else {
                    return 0;
                };
                (limbs.len() - 1) * 64 + (64 - high.leading_zeros() as usize)
            },
        }
    }

    fn limb_len(&self) -> usize {
        match self {
            Self::Inline(value) => {
                if *value == 0 {
                    0
                } else if *value >> 64 == 0 {
                    1
                } else {
                    2
                }
            },
            Self::Wide(limbs) => limbs.len(),
        }
    }

    fn limb(&self, index: usize) -> u64 {
        match self {
            Self::Inline(value) => match index {
                0 => *value as u64,
                1 => (*value >> 64) as u64,
                _ => 0,
            },
            Self::Wide(limbs) => limbs.get(index).copied().unwrap_or(0),
        }
    }

    pub(super) fn bit(&self, index: usize) -> bool {
        let limb = index / 64;
        limb < self.limb_len() && (self.limb(limb) >> (index % 64)) & 1 != 0
    }

    pub(super) fn any_below(&self, count: usize) -> bool {
        if count == 0 || self.is_zero() {
            return false;
        }
        if count >= self.bits() {
            return true;
        }
        let whole = count / 64;
        for index in 0..whole {
            if self.limb(index) != 0 {
                return true;
            }
        }
        let remainder = count % 64;
        remainder != 0 && self.limb(whole) & ((1_u64 << remainder) - 1) != 0
    }

    pub(super) fn is_odd(&self) -> bool {
        self.bit(0)
    }

    pub(super) fn trailing_zeros(&self) -> usize {
        match self {
            Self::Inline(value) => value.trailing_zeros() as usize,
            Self::Wide(limbs) => {
                let mut result = 0;
                for limb in limbs {
                    if *limb == 0 {
                        result += 64;
                    } else {
                        return result + limb.trailing_zeros() as usize;
                    }
                }
                result
            },
        }
    }

    pub(super) fn to_u128(&self) -> Option<u128> {
        match self {
            Self::Inline(value) => Some(*value),
            Self::Wide(limbs) if limbs.is_empty() => Some(0),
            Self::Wide(limbs) if limbs.len() <= 2 => {
                let low = u128::from(limbs[0]);
                let high = if limbs.len() == 2 {
                    u128::from(limbs[1]) << 64
                } else {
                    0
                };
                Some(low | high)
            },
            Self::Wide(_) => None,
        }
    }

    pub(super) fn is_all_ones(&self, count: usize) -> bool {
        if count == 0 || count != self.bits() {
            return false;
        }
        let full_words = count / 64;
        for index in 0..full_words {
            if self.limb(index) != u64::MAX {
                return false;
            }
        }
        let remainder = count % 64;
        remainder == 0 || self.limb(full_words) == (1_u64 << remainder) - 1
    }

    pub(super) fn cmp(&self, other: &Self) -> Ordering {
        let left_bits = self.bits();
        let right_bits = other.bits();
        if left_bits != right_bits {
            return left_bits.cmp(&right_bits);
        }
        for index in (0..left_bits.div_ceil(64)).rev() {
            let ordering = self.limb(index).cmp(&other.limb(index));
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }

    /// Compare `self << shift` with `other` without materializing the shift.
    pub(super) fn cmp_shifted(&self, shift: usize, other: &Self) -> Ordering {
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
        let shifted_bits = self.bits().saturating_add(shift);
        let other_bits = other.bits();
        if shifted_bits != other_bits {
            return shifted_bits.cmp(&other_bits);
        }
        for index in (0..other_bits).rev() {
            let left = if index >= shift {
                self.bit(index - shift)
            } else {
                false
            };
            let right = other.bit(index);
            if left != right {
                return left.cmp(&right);
            }
        }
        Ordering::Equal
    }

    pub(super) fn shift_left(&self, shift: usize, context: &mut PrecisionContext) -> Result<Self> {
        if self.is_zero() || shift == 0 {
            return self.try_clone(context);
        }
        context.step(fuel_units(self.limb_len())?)?;
        let total_bits = self
            .bits()
            .checked_add(shift)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?;
        let limb_count = total_bits
            .checked_add(63)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?
            / 64;
        if limb_count <= 2 {
            let Some(value) = self.to_u128() else {
                return Err(super::numerical_indeterminacy());
            };
            return Ok(Self::Inline(value << shift));
        }
        let mut result = Vec::new();
        context.try_reserve_words(&mut result, limb_count)?;
        result.resize(limb_count, 0);
        let word_shift = shift / 64;
        let bit_shift = shift % 64;
        for index in 0..self.limb_len() {
            let value = self.limb(index);
            let destination = index + word_shift;
            result[destination] |= value << bit_shift;
            if bit_shift != 0 {
                let carry = value >> (64 - bit_shift);
                if carry != 0 {
                    result[destination + 1] |= carry;
                }
            }
        }
        Self::from_owned_limbs(result)
    }

    pub(super) fn shift_right(&self, shift: usize, context: &mut PrecisionContext) -> Result<Self> {
        if shift >= self.bits() {
            return Ok(Self::zero());
        }
        let target_bits = self.bits() - shift;
        if target_bits <= 128 {
            context.step(fuel_units(target_bits)?)?;
            let mut value = 0_u128;
            for index in 0..target_bits {
                if self.bit(index + shift) {
                    value |= 1_u128 << index;
                }
            }
            return Ok(Self::Inline(value));
        }
        let limb_count = target_bits
            .checked_add(63)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?
            / 64;
        context.step(fuel_units(limb_count)?)?;
        let mut result = Vec::new();
        context.try_reserve_words(&mut result, limb_count)?;
        result.resize(limb_count, 0);
        let word_shift = shift / 64;
        let bit_shift = shift % 64;
        for index in 0..limb_count {
            let source = index + word_shift;
            let value = self.limb(source);
            result[index] = value >> bit_shift;
            if bit_shift != 0 {
                result[index] |= self.limb(source + 1) << (64 - bit_shift);
            }
        }
        Self::from_owned_limbs(result)
    }

    pub(super) fn shift_right_assign(&mut self, shift: usize) -> Result<()> {
        if shift == 0 || self.is_zero() {
            return Ok(());
        }
        if shift >= self.bits() {
            *self = Self::zero();
            return Ok(());
        }
        let target_bits = self.bits() - shift;
        if target_bits <= 128 {
            let mut value = 0_u128;
            for index in 0..target_bits {
                if self.bit(index + shift) {
                    value |= 1_u128 << index;
                }
            }
            *self = Self::Inline(value);
            return Ok(());
        }
        let word_shift = shift / 64;
        let bit_shift = shift % 64;
        let new_len = target_bits / 64 + usize::from(target_bits % 64 != 0);
        if let Self::Wide(limbs) = self {
            for index in 0..new_len {
                let source = index + word_shift;
                let value = limbs[source] >> bit_shift;
                let next = limbs.get(source + 1).copied().unwrap_or(0);
                limbs[index] = if bit_shift == 0 {
                    value
                } else {
                    value | next << (64 - bit_shift)
                };
            }
            limbs.truncate(new_len);
        } else {
            return Err(super::numerical_indeterminacy());
        }
        Ok(())
    }

    pub(super) fn shift_left_assign(
        &mut self,
        shift: usize,
        context: &mut PrecisionContext,
    ) -> Result<()> {
        if shift == 0 || self.is_zero() {
            return Ok(());
        }
        let total_bits = self
            .bits()
            .checked_add(shift)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?;
        context.step(fuel_units(self.limb_len())?)?;
        if total_bits <= 128 {
            let Some(value) = self.to_u128() else {
                return Err(super::numerical_indeterminacy());
            };
            *self = Self::Inline(value << shift);
            return Ok(());
        }
        if !matches!(self, Self::Wide(_)) {
            *self = self.shift_left(shift, context)?;
            return Ok(());
        }
        let word_shift = shift / 64;
        let bit_shift = shift % 64;
        let old_len = self.limb_len();
        let new_len = total_bits
            .checked_add(63)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?
            / 64;
        let Self::Wide(limbs) = self else {
            return Err(super::numerical_indeterminacy());
        };
        context.try_reserve_words(limbs, new_len - old_len)?;
        limbs.resize(new_len, 0);
        limbs.copy_within(0..old_len, word_shift);
        limbs[..word_shift].fill(0);
        if bit_shift != 0 {
            let start = word_shift;
            let end = word_shift + old_len;
            let mut carry = 0_u64;
            for index in start..end {
                let value = limbs[index];
                limbs[index] = (value << bit_shift) | carry;
                carry = value >> (64 - bit_shift);
            }
            if end < new_len {
                limbs[end] = carry;
            }
        }
        Ok(())
    }

    pub(super) fn add(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        let length = self.limb_len().max(other.limb_len());
        context.step(fuel_units(length)?)?;
        if length <= 2 {
            let mut result = [0_u64; 3];
            let mut carry = 0_u64;
            for index in 0..length {
                let sum = u128::from(self.limb(index))
                    + u128::from(other.limb(index))
                    + u128::from(carry);
                result[index] = sum as u64;
                carry = (sum >> 64) as u64;
            }
            result[length] = carry;
            return Self::from_limbs(&result, context);
        }
        let mut result = Vec::new();
        let output_length = length
            .checked_add(1)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?;
        context.try_reserve_words(&mut result, output_length)?;
        result.resize(output_length, 0);
        let mut carry = 0_u64;
        for index in 0..length {
            let sum =
                u128::from(self.limb(index)) + u128::from(other.limb(index)) + u128::from(carry);
            result[index] = sum as u64;
            carry = (sum >> 64) as u64;
        }
        result[length] = carry;
        Self::from_owned_limbs(result)
    }

    pub(super) fn add_one_assign(&mut self, context: &mut PrecisionContext) -> Result<()> {
        *self = self.add(&Self::from_u128(1), context)?;
        Ok(())
    }

    pub(super) fn sub(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        debug_assert!(self.cmp(other) != Ordering::Less);
        let length = self.limb_len();
        context.step(fuel_units(length)?)?;
        if length <= 2 {
            let mut result = [0_u64; 2];
            let mut borrow = 0_u64;
            for index in 0..length {
                let (difference, borrowed) = self.limb(index).overflowing_sub(other.limb(index));
                let (difference, borrowed_again) = difference.overflowing_sub(borrow);
                result[index] = difference;
                borrow = u64::from(borrowed || borrowed_again);
            }
            return Self::from_limbs(&result, context);
        }
        let mut result = Vec::new();
        context.try_reserve_words(&mut result, length)?;
        result.resize(length, 0);
        let mut borrow = 0_u64;
        for index in 0..length {
            let (difference, borrowed) = self.limb(index).overflowing_sub(other.limb(index));
            let (difference, borrowed_again) = difference.overflowing_sub(borrow);
            result[index] = difference;
            borrow = u64::from(borrowed || borrowed_again);
        }
        Self::from_owned_limbs(result)
    }

    pub(super) fn mul(&self, other: &Self, context: &mut PrecisionContext) -> Result<Self> {
        if self.is_zero() || other.is_zero() {
            return Ok(Self::zero());
        }
        let left_len = self.limb_len();
        let right_len = other.limb_len();
        let length = left_len
            .checked_add(right_len)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?;
        let cost = left_len
            .checked_mul(right_len)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?;
        context.step(fuel_units(cost)?)?;
        if length <= 4 {
            let mut result = [0_u64; 4];
            Self::mul_into(self, other, &mut result[..length]);
            return Self::from_limbs(&result, context);
        }
        let mut result = Vec::new();
        context.try_reserve_words(&mut result, length)?;
        result.resize(length, 0);
        Self::mul_into(self, other, &mut result);
        Self::from_owned_limbs(result)
    }

    fn mul_into(left: &Self, right: &Self, result: &mut [u64]) {
        for left_index in 0..left.limb_len() {
            let mut carry = 0_u128;
            for right_index in 0..right.limb_len() {
                let index = left_index + right_index;
                let value = u128::from(left.limb(left_index)) * u128::from(right.limb(right_index))
                    + u128::from(result[index])
                    + carry;
                result[index] = value as u64;
                carry = value >> 64;
            }
            let mut index = left_index + right.limb_len();
            while carry != 0 {
                let value = u128::from(result[index]) + carry;
                result[index] = value as u64;
                carry = value >> 64;
                index += 1;
            }
        }
    }

    pub(super) fn mod_small(&self, modulus: u64, context: &mut PrecisionContext) -> Result<u64> {
        debug_assert!(modulus != 0);
        let mut remainder = 0_u64;
        for index in (0..self.limb_len()).rev() {
            context.step(1)?;
            remainder = (((u128::from(remainder) << 64) + u128::from(self.limb(index)))
                % u128::from(modulus)) as u64;
        }
        Ok(remainder)
    }

    pub(super) fn low_bits(&self, count: usize, context: &mut PrecisionContext) -> Result<Self> {
        if count == 0 || self.is_zero() {
            return Ok(Self::zero());
        }
        if count >= self.bits() {
            return self.try_clone(context);
        }
        if count <= 128 {
            context.step(fuel_units(count)?)?;
            let mut value = 0_u128;
            for index in 0..count {
                if self.bit(index) {
                    value |= 1_u128 << index;
                }
            }
            return Ok(Self::Inline(value));
        }
        let limb_count = count
            .checked_add(63)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?
            / 64;
        let mut result = Vec::new();
        context.try_reserve_words(&mut result, limb_count)?;
        result.resize(limb_count, 0);
        for (index, slot) in result.iter_mut().enumerate() {
            *slot = self.limb(index);
        }
        let remainder = count % 64;
        if remainder != 0 {
            result[limb_count - 1] &= (1_u64 << remainder) - 1;
        }
        Self::from_owned_limbs(result)
    }

    /// Exact integer quotient and remainder.  The loop is deliberately based
    /// on integer comparisons and subtraction; no reciprocal or floating
    /// estimate participates in the result.
    pub(super) fn div_rem(
        numerator: &Self,
        denominator: &Self,
        context: &mut PrecisionContext,
    ) -> Result<(Self, Self)> {
        if denominator.is_zero() {
            return Err(crate::crs::CrsError::message(
                "certified dyadic division by zero",
            ));
        }
        if numerator.is_zero() {
            return Ok((Self::zero(), Self::zero()));
        }
        if numerator.cmp(denominator) == Ordering::Less {
            return Ok((Self::zero(), numerator.try_clone(context)?));
        }
        let quotient_bits = numerator.bits() - denominator.bits() + 1;
        let quotient_len = quotient_bits
            .checked_add(63)
            .ok_or_else(|| crate::error::Error::from(crate::error::ResourceError::SizeOverflow))?
            / 64;
        let mut quotient_small = [0_u64; 2];
        let mut quotient_wide = Vec::new();
        if quotient_len > 2 {
            context.try_reserve_words(&mut quotient_wide, quotient_len)?;
            quotient_wide.resize(quotient_len, 0);
        }
        let mut remainder = Self::zero();
        for bit_index in (0..numerator.bits()).rev() {
            context.step(1)?;
            remainder.shift_left_assign(1, context)?;
            if numerator.bit(bit_index) {
                remainder.add_one_assign(context)?;
            }
            if remainder.cmp(denominator) != Ordering::Less {
                remainder = remainder.sub(denominator, context)?;
                let quotient_bit = bit_index;
                let limb = quotient_bit / 64;
                let mask = 1_u64 << (quotient_bit % 64);
                if quotient_len > 2 {
                    quotient_wide[limb] |= mask;
                } else {
                    quotient_small[limb] |= mask;
                }
            }
        }
        let quotient = if quotient_len > 2 {
            Self::from_owned_limbs(quotient_wide)?
        } else {
            Self::from_limbs(&quotient_small[..quotient_len], context)?
        };
        Ok((quotient, remainder))
    }

    pub(super) fn integer_sqrt(value: &Self, context: &mut PrecisionContext) -> Result<Self> {
        if value.is_zero() {
            return Ok(Self::zero());
        }
        let groups = value.bits().div_ceil(2);
        let mut root = Self::zero();
        let mut remainder = Self::zero();
        for group in (0..groups).rev() {
            context.step(1)?;
            remainder.shift_left_assign(2, context)?;
            let pair = u64::from(value.bit(group * 2)) | (u64::from(value.bit(group * 2 + 1)) << 1);
            if pair != 0 {
                let pair_value = Self::from_u128(u128::from(pair));
                remainder = remainder.add(&pair_value, context)?;
            }
            let mut trial = root.shift_left(2, context)?;
            trial.add_one_assign(context)?;
            if remainder.cmp(&trial) != Ordering::Less {
                remainder = remainder.sub(&trial, context)?;
                root.shift_left_assign(1, context)?;
                root.add_one_assign(context)?;
            } else {
                root.shift_left_assign(1, context)?;
            }
        }
        Ok(root)
    }
}

impl Ord for Magnitude {
    fn cmp(&self, other: &Self) -> Ordering {
        Self::cmp(self, other)
    }
}

impl PartialOrd for Magnitude {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
