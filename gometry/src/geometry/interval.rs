//! Outward binary64 intervals for certified geometric computations.
//!
//! The lower and upper binary64 values below bracket the real π constants.
//! Each arithmetic method expands a correctly rounded IEEE-754 operation by
//! one representable value in both directions.

pub(crate) const PI_LOWER: f64 = std::f64::consts::PI;
pub(crate) const PI_UPPER: f64 = 3.141_592_653_589_793_6;
pub(crate) const HALF_PI_LOWER: f64 = std::f64::consts::FRAC_PI_2;
pub(crate) const HALF_PI_UPPER: f64 = 1.570_796_326_794_896_8;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Interval {
    lower: f64,
    upper: f64,
}

impl Interval {
    /// Treat a represented binary64 input as an exact endpoint.
    pub(crate) const fn exact(value: f64) -> Self {
        Self {
            lower: value,
            upper: value,
        }
    }

    /// Build an enclosure from endpoints already rounded outwards by a caller.
    pub(crate) const fn from_bounds(lower: f64, upper: f64) -> Self {
        Self { lower, upper }
    }

    pub(crate) const fn lower(self) -> f64 {
        self.lower
    }

    pub(crate) const fn upper(self) -> f64 {
        self.upper
    }

    pub(crate) const fn contains_zero(self) -> bool {
        self.lower <= 0.0 && self.upper >= 0.0
    }

    /// Enclose addition by stepping each rounded endpoint result outwards.
    pub(crate) fn add(self, other: Self) -> Self {
        if self.is_exact_zero() {
            return other;
        }
        if other.is_exact_zero() {
            return self;
        }
        Self {
            lower: (self.lower + other.lower).next_down(),
            upper: (self.upper + other.upper).next_up(),
        }
    }

    /// Enclose subtraction using the smallest and largest endpoint differences.
    pub(crate) fn sub(self, other: Self) -> Self {
        if other.is_exact_zero() {
            return self;
        }
        if self.is_exact_zero() {
            return other.neg();
        }
        Self {
            lower: (self.lower - other.upper).next_down(),
            upper: (self.upper - other.lower).next_up(),
        }
    }

    /// Enclose all endpoint products; indeterminate infinite products widen fully.
    pub(crate) fn mul(self, other: Self) -> Self {
        if (self.is_exact_zero() && other.lower.is_finite() && other.upper.is_finite())
            || (other.is_exact_zero() && self.lower.is_finite() && self.upper.is_finite())
        {
            return Self::exact(0.0);
        }
        Self::enclose_extrema([
            self.lower * other.lower,
            self.lower * other.upper,
            self.upper * other.lower,
            self.upper * other.upper,
        ])
    }

    fn enclose_extrema(values: [f64; 4]) -> Self {
        let mut lower = f64::INFINITY;
        let mut upper = f64::NEG_INFINITY;
        for value in values {
            if value.is_nan() {
                return Self::from_bounds(f64::NEG_INFINITY, f64::INFINITY);
            }
            lower = lower.min(value);
            upper = upper.max(value);
        }
        Self {
            lower: lower.next_down(),
            upper: upper.next_up(),
        }
    }

    /// Enclose all endpoint quotients; zero-crossing divisors
    /// have no finite enclosure.
    pub(crate) fn div(self, other: Self) -> Self {
        if other.contains_zero() {
            return Self::from_bounds(f64::NEG_INFINITY, f64::INFINITY);
        }
        if self.is_exact_zero() && !other.lower.is_nan() && !other.upper.is_nan() {
            return Self::exact(0.0);
        }
        Self::enclose_extrema([
            self.lower / other.lower,
            self.lower / other.upper,
            self.upper / other.lower,
            self.upper / other.upper,
        ])
    }

    /// Enclose the nonnegative square root, clipping a rounded lower endpoint at zero.
    pub(crate) fn sqrt(self) -> Self {
        if self.upper < 0.0 {
            return Self::from_bounds(f64::NAN, f64::NAN);
        }
        if self.is_exact_zero() {
            return Self::exact(0.0);
        }
        Self {
            lower: self.lower.max(0.0).sqrt().next_down().max(0.0),
            upper: self.upper.sqrt().next_up(),
        }
    }

    /// Enclose sign reversal by swapping and negating endpoints outwards.
    pub(crate) fn neg(self) -> Self {
        if self.is_exact_zero() {
            return Self::exact(0.0);
        }
        Self {
            lower: (-self.upper).next_down(),
            upper: (-self.lower).next_up(),
        }
    }

    /// Upper bound the absolute value over the interval.
    pub(crate) const fn abs_upper(self) -> f64 {
        self.lower.abs().max(self.upper.abs())
    }

    pub(crate) const fn max_abs_upper(self) -> f64 {
        self.abs_upper().next_up()
    }

    /// Enclose the pointwise maximum of the two represented intervals.
    pub(crate) const fn max(self, other: Self) -> Self {
        Self {
            lower: self.lower.max(other.lower),
            upper: self.upper.max(other.upper),
        }
    }

    /// Whether two interval enclosures may share a represented value.
    pub(crate) fn intersects(self, other: Self) -> bool {
        self.lower <= other.upper && other.lower <= self.upper
    }

    const fn is_exact_zero(self) -> bool {
        self.lower.to_bits().trailing_zeros() >= 63 && self.upper.to_bits().trailing_zeros() >= 63
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_zero_arithmetic_preserves_exact_interval_identities() {
        let zero = Interval::exact(0.0);
        let value = Interval::from_bounds(2.0, 3.0);

        assert_eq!(zero.add(value), value);
        assert_eq!(value.add(zero), value);
        let negative = zero.sub(value);
        assert!(negative.lower() <= -3.0);
        assert!(negative.upper() >= -2.0);
        assert_eq!(value.sub(zero), value);
        assert_eq!(zero.mul(value), zero);
        assert_eq!(value.mul(zero), zero);
        assert_eq!(zero.sqrt(), zero);
        assert_eq!(zero.neg(), zero);
        assert_eq!(zero.div(value), zero);
    }

    #[test]
    fn endpoint_extrema_enclose_mixed_sign_products_and_quotients() {
        let left = Interval::from_bounds(-3.0, 5.0);
        let right = Interval::from_bounds(-4.0, -2.0);
        let product = left.mul(right);
        assert!(product.lower() <= -20.0);
        assert!(product.upper() >= 12.0);
        let quotient = left.div(right);
        assert!(quotient.lower() <= -2.5);
        assert!(quotient.upper() >= 1.5);
    }

    #[test]
    fn direct_quotients_enclose_subnormal_and_overflow_limits() {
        let tiny = f64::from_bits(1);
        for value in [tiny, f64::MAX] {
            let quotient = Interval::exact(value).div(Interval::exact(value));
            assert!(quotient.lower() > 0.0 && quotient.lower() <= 1.0);
            assert!(quotient.upper() >= 1.0 && quotient.upper().is_finite());
        }
        let overflow = Interval::exact(f64::MAX).div(Interval::exact(tiny));
        assert_eq!(overflow.lower(), f64::MAX);
        assert_eq!(overflow.upper(), f64::INFINITY);
        let undefined = Interval::exact(1.0).div(Interval::from_bounds(-1.0, 1.0));
        assert_eq!(undefined.lower(), f64::NEG_INFINITY);
        assert_eq!(undefined.upper(), f64::INFINITY);
    }
}
