use super::{
    ExactDyadic, ExactFactorialArea, GeographicAreaDensity, GeographicFrame, GeometryErrorKind,
    MIN_SUBNORMAL, Result, canonicalize_zero, outward_ratio_bounds, round_ratio, two_product,
    two_sum,
};

#[derive(Clone, Copy, Default)]
pub(super) struct Bounded {
    pub(super) value: f64,
    pub(super) error: f64,
}

impl Bounded {
    pub(super) const fn exact(value: f64) -> Self {
        Self { value, error: 0.0 }
    }

    pub(super) fn add(self, other: Self) -> Self {
        let (value, residual) = two_sum(self.value, other.value);
        Self {
            value,
            error: upper_add(
                upper_add(self.error, other.error),
                upper_add(
                    residual.abs(),
                    if value.is_finite() {
                        MIN_SUBNORMAL
                    } else {
                        f64::INFINITY
                    },
                ),
            ),
        }
    }

    pub(super) fn sub(self, other: Self) -> Self {
        self.add(Self {
            value: -other.value,
            error: other.error,
        })
    }

    pub(super) fn mul(self, other: Self) -> Self {
        let (value, residual) = two_product(self.value, other.value);
        let round = if value.is_finite() {
            upper_add(residual.abs(), MIN_SUBNORMAL)
        } else {
            f64::INFINITY
        };
        let input = upper_add(
            upper_add(
                upper_mul(self.value.abs(), other.error),
                upper_mul(other.value.abs(), self.error),
            ),
            upper_mul(self.error, other.error),
        );
        Self {
            value,
            error: upper_add(input, round),
        }
    }

    pub(super) fn div_u64(self, denominator: u64) -> Self {
        let denominator = denominator as f64;
        let value = self.value / denominator;
        let error = upper_add(
            upper_div_u64(self.error, denominator as u64),
            rounding_error(value),
        );
        Self { value, error }
    }
}

#[derive(Default)]
pub(super) struct BoundedSum {
    pub(super) head: f64,
    pub(super) low: f64,
    pub(super) lost: f64,
    pub(super) input_error: f64,
    pub(super) failed: bool,
}

impl BoundedSum {
    pub(super) fn push(&mut self, value: Bounded) {
        if self.failed {
            return;
        }
        if !value.value.is_finite() || !value.error.is_finite() || value.error < 0.0 {
            self.failed = true;
            return;
        }
        if !(self.head + value.value).is_finite() {
            self.failed = true;
            return;
        }
        let (head, remainder) = two_sum(self.head, value.value);
        if !(self.low + remainder).is_finite() {
            self.failed = true;
            return;
        }
        let (low, discarded) = two_sum(self.low, remainder);
        self.input_error = upper_add(self.input_error, value.error);
        self.lost = upper_add(self.lost, discarded.abs());
        self.head = head;
        self.low = low;
        self.failed = !self.input_error.is_finite() || !self.lost.is_finite();
    }

    pub(super) fn value(&self) -> f64 {
        if self.failed {
            f64::NAN
        } else {
            self.head + self.low
        }
    }

    pub(super) fn error(&self) -> f64 {
        if self.failed || !(self.head + self.low).is_finite() {
            return f64::INFINITY;
        }
        upper_add(
            self.input_error,
            upper_add(self.lost, two_sum(self.head, self.low).1.abs()),
        )
    }

    pub(super) fn bounded(&self) -> Bounded {
        Bounded {
            value: self.value(),
            error: self.error(),
        }
    }
}

pub(super) fn positive_sum<const N: usize>(values: [f64; N]) -> Result<PositiveBound> {
    let mut sum = PositiveBound::ZERO;
    for value in values {
        let value =
            PositiveBound::from_f64(value).ok_or(crate::error::ResourceError::SizeOverflow)?;
        sum = sum
            .add_upper(value)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    Ok(sum)
}

pub(super) fn positive_exact_bound(value: &ExactDyadic) -> Result<PositiveBound> {
    if value.is_zero() {
        return Ok(PositiveBound::ZERO);
    }
    let (_, upper) = outward_ratio_bounds(value, &ExactDyadic::one())?;
    PositiveBound::from_f64(upper).ok_or_else(|| {
        GeometryErrorKind::message("geographic source bound is outside the proven range")
    })
}

pub(super) fn exact_difference_product(left: f64, right: f64, scale: f64) -> Result<Bounded> {
    let exact = ExactDyadic::from_f64(left)
        .try_subtract(ExactDyadic::from_f64(right))?
        .try_product(&ExactDyadic::from_f64(scale))?;
    exact_to_bounded(&exact)
}

pub(super) fn scaled_x(
    frame: GeographicFrame,
    canonical_x: f64,
    turn: i64,
    origin: f64,
    coordinate_scale: Bounded,
) -> Bounded {
    let turn_error = if turn.unsigned_abs() <= (1_u64 << 53) {
        0.0
    } else {
        f64::INFINITY
    };
    let mut lifted = Bounded::exact(canonical_x);
    if turn != 0 {
        lifted = lifted.add(Bounded::exact(frame.longitude().period()).mul(Bounded {
            value: turn as f64,
            error: turn_error,
        }));
    }
    lifted.sub(Bounded::exact(origin)).mul(coordinate_scale)
}

pub(super) fn scaled_y(latitude: f64, origin: f64, coordinate_scale: Bounded) -> Bounded {
    Bounded::exact(latitude)
        .sub(Bounded::exact(origin))
        .mul(coordinate_scale)
}

pub(super) fn exact_to_bounded(value: &ExactDyadic) -> Result<Bounded> {
    let rounded = round_ratio(value, &ExactDyadic::one())?;
    let (lower, upper) = outward_ratio_bounds(value, &ExactDyadic::one())?;
    let error = (rounded - lower).abs().max((upper - rounded).abs());
    Ok(Bounded {
        value: rounded,
        error,
    })
}

pub(super) fn ceil_log2(value: usize) -> u32 {
    debug_assert!(value > 0);
    value.wrapping_sub(1).bit_width()
}

pub(super) fn scale_area_fast(
    integral: f64,
    density: &GeographicAreaDensity,
    x_exponent: i32,
) -> Option<(f64, PositiveBound)> {
    if !integral.is_normal() {
        return None;
    }
    let scale_exponent = x_exponent.checked_neg()?.checked_add(3)?;
    let power = 2.0_f64.powi(scale_exponent);
    if !power.is_normal() {
        return None;
    }
    let scale = Bounded::exact(density.semi_major)
        .mul(Bounded::exact(density.semi_major))
        .mul(Bounded::exact(power));
    if !scale.value.is_normal() {
        return None;
    }
    if !scale.error.is_finite() || scale.error >= scale.value {
        return None;
    }
    let lower_scale = (scale.value - scale.error).next_down();
    if !lower_scale.is_normal() {
        return None;
    }
    let relative_scale_error = PositiveBound::from_f64(scale.error)?
        .divide_by_upper(PositiveBound::from_f64(lower_scale)?)?;
    let normalized_scale_error =
        relative_scale_error.multiply_upper(PositiveBound::from_f64(integral.abs())?)?;
    let area = scale.value * integral;
    if !area.is_finite() || !area.is_normal() {
        return None;
    }
    Some((canonicalize_zero(area), normalized_scale_error))
}

pub(super) fn area_scale_bound(
    density: &GeographicAreaDensity,
    x_exponent: i32,
) -> Result<PositiveBound> {
    let scale_exponent = x_exponent
        .checked_neg()
        .and_then(|exponent| exponent.checked_add(3))
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let semi_major = PositiveBound::from_f64(density.semi_major)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    semi_major
        .multiply_upper(semi_major)
        .and_then(|value| value.scale_power_of_two(scale_exponent))
        .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
}

pub(super) fn operation_count(order: usize) -> Result<u64> {
    let count = order
        .checked_add(1)
        .and_then(|degree_count| degree_count.checked_mul(3))
        .and_then(|count| count.checked_add(3))
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let count = u64::try_from(count).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    if count >= (1_u64 << 53) {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    Ok(count)
}

pub(super) fn accept_work_budget(
    error: PositiveBound,
    norm: PositiveBound,
    scale: &ExactDyadic,
    order: usize,
) -> Result<bool> {
    let operation_count = operation_count(order)?;
    let error = error.to_exact()?;
    let norm = norm.to_exact()?;
    if ExactFactorialArea::dyadic_within_gamma(&error, &norm, operation_count)? {
        return Ok(true);
    }
    let physical_error = scale.try_product(&error)?;
    let half_minimum_subnormal = ExactDyadic::try_from_u128(false, 1, -1075)?;
    Ok(physical_error.cmp(&half_minimum_subnormal).is_le())
}

pub(super) fn rounding_error(value: f64) -> f64 {
    if value.is_finite() {
        upper_add(value.abs() * f64::EPSILON, MIN_SUBNORMAL)
    } else {
        f64::INFINITY
    }
}

pub(super) fn upper_add(left: f64, right: f64) -> f64 {
    if left == 0.0 {
        return right;
    }
    if right == 0.0 {
        return left;
    }
    let sum = left + right;
    if sum.is_finite() {
        sum.next_up()
    } else {
        f64::INFINITY
    }
}

pub(super) fn upper_mul(left: f64, right: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        return 0.0;
    }
    let product = left * right;
    if product.is_finite() {
        product.next_up()
    } else {
        f64::INFINITY
    }
}

pub(super) fn upper_div_u64(value: f64, denominator: u64) -> f64 {
    if value == 0.0 {
        return 0.0;
    }
    let quotient = value / denominator as f64;
    if quotient.is_finite() {
        quotient.next_up()
    } else {
        f64::INFINITY
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PositiveBound {
    pub(super) mantissa: f64,
    pub(super) exponent: i32,
}

impl PositiveBound {
    pub(super) const ZERO: Self = Self {
        mantissa: 0.0,
        exponent: 0,
    };

    pub(super) fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() || value < 0.0 {
            return None;
        }
        if value == 0.0 {
            return Some(Self::ZERO);
        }
        let bits = value.to_bits();
        let fraction = bits & ((1_u64 << 52) - 1);
        let encoded_exponent = ((bits >> 52) & 0x7FF) as i32;
        if encoded_exponent != 0 {
            return Some(Self {
                mantissa: f64::from_bits((1023_u64 << 52) | fraction),
                exponent: encoded_exponent.wrapping_sub(1023),
            });
        }
        let leading = fraction.ilog2();
        let significand = fraction << 52_u32.wrapping_sub(leading);
        Some(Self {
            mantissa: f64::from_bits((1023_u64 << 52) | (significand & ((1_u64 << 52) - 1))),
            exponent: (leading as i32).wrapping_sub(1074),
        })
    }

    pub(super) fn from_parts(mantissa: f64, exponent: i32) -> Option<Self> {
        if mantissa == 0.0 {
            return Some(Self::ZERO);
        }
        let normalized = Self::from_f64(mantissa)?;
        Some(Self {
            exponent: normalized.exponent.checked_add(exponent)?,
            ..normalized
        })
    }

    pub(super) fn is_zero(self) -> bool {
        self.mantissa == 0.0
    }

    pub(super) fn multiply_upper(self, other: Self) -> Option<Self> {
        if self.is_zero() || other.is_zero() {
            return Some(Self::ZERO);
        }
        Self::from_parts(
            (self.mantissa * other.mantissa).next_up(),
            self.exponent.checked_add(other.exponent)?,
        )
    }

    pub(super) fn add_upper(self, other: Self) -> Option<Self> {
        if self.is_zero() {
            return Some(other);
        }
        if other.is_zero() {
            return Some(self);
        }
        let (large, small) = if self.exponent >= other.exponent {
            (self, other)
        } else {
            (other, self)
        };
        let difference = small.exponent.checked_sub(large.exponent)?;
        if difference < -1074 {
            return Self::from_parts(large.mantissa.next_up(), large.exponent);
        }
        let aligned = small.mantissa * 2.0_f64.powi(difference);
        if aligned == 0.0 {
            return Self::from_parts(large.mantissa.next_up(), large.exponent);
        }
        Self::from_parts((large.mantissa + aligned).next_up(), large.exponent)
    }

    pub(super) fn divide_upper(self, denominator: u64) -> Option<Self> {
        if denominator == 0 {
            return None;
        }
        if self.is_zero() {
            return Some(Self::ZERO);
        }
        Self::from_parts(
            (self.mantissa / denominator as f64).next_up(),
            self.exponent,
        )
    }

    pub(super) fn power_upper(self, mut exponent: usize) -> Option<Self> {
        let mut result = Self::from_f64(1.0)?;
        let mut base = self;
        while exponent != 0 {
            if exponent & 1 != 0 {
                result = result.multiply_upper(base)?;
            }
            exponent >>= 1;
            if exponent != 0 {
                base = base.multiply_upper(base)?;
            }
        }
        Some(result)
    }

    pub(super) fn divide_by_upper(self, denominator: Self) -> Option<Self> {
        if denominator.is_zero() {
            return None;
        }
        if self.is_zero() {
            return Some(Self::ZERO);
        }
        Self::from_parts(
            (self.mantissa / denominator.mantissa).next_up(),
            self.exponent.checked_sub(denominator.exponent)?,
        )
    }

    pub(super) fn scale_power_of_two(self, exponent: i32) -> Option<Self> {
        if self.is_zero() {
            return Some(self);
        }
        Some(Self {
            mantissa: self.mantissa,
            exponent: self.exponent.checked_add(exponent)?,
        })
    }

    pub(super) fn maximum(self, other: Self) -> Self {
        if self.is_zero() {
            return other;
        }
        if other.is_zero() {
            return self;
        }
        if self.exponent > other.exponent
            || (self.exponent == other.exponent && self.mantissa >= other.mantissa)
        {
            self
        } else {
            other
        }
    }

    pub(super) fn ceil_log2(self) -> Option<i32> {
        if self.is_zero() {
            return Some(i32::MIN);
        }
        self.exponent.checked_add(i32::from(self.mantissa > 1.0))
    }

    pub(super) fn ceil_to_usize(self) -> Option<usize> {
        if self.is_zero() {
            return Some(0);
        }
        if self.exponent < 0 {
            let value = self.mantissa * 2.0_f64.powi(self.exponent);
            return Some(value.ceil() as usize);
        }
        let exponent = u32::try_from(self.exponent).ok()?;
        if exponent >= usize::BITS {
            return None;
        }
        let value = self.mantissa * 2.0_f64.powi(self.exponent);
        if !value.is_finite() || value.ceil() > usize::MAX as f64 {
            return None;
        }
        Some(value.ceil() as usize)
    }

    pub(super) fn to_exact(self) -> Result<ExactDyadic> {
        if self.is_zero() {
            return Ok(ExactDyadic::zero());
        }
        let mut exact = ExactDyadic::from_f64(self.mantissa);
        exact.exponent = exact
            .exponent
            .checked_add(i64::from(self.exponent))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(exact)
    }
}
