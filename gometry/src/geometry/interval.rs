//! Outward binary64 intervals for geographic distance certificates.
//!
//! The π literals below are adjacent binary64 values bracketing the real
//! constants. Each arithmetic method expands a correctly rounded IEEE-754
//! operation by one representable value in both directions.

#[expect(
    clippy::approx_constant,
    reason = "the lower adjacent binary64 value must bracket real pi outwards"
)]
pub(crate) const PI_LOWER: f64 = 3.141_592_653_589_793;
pub(crate) const PI_UPPER: f64 = 3.141_592_653_589_793_6;
#[expect(
    clippy::approx_constant,
    reason = "the lower adjacent binary64 value must bracket real pi/2 outwards"
)]
pub(crate) const HALF_PI_LOWER: f64 = 1.570_796_326_794_896_6;
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
        let products = [
            (self.lower, other.lower),
            (self.lower, other.upper),
            (self.upper, other.lower),
            (self.upper, other.upper),
        ];
        let mut lower = f64::INFINITY;
        let mut upper = f64::NEG_INFINITY;
        for (left, right) in products {
            let product = left * right;
            if product.is_nan() {
                return Self::from_bounds(f64::NEG_INFINITY, f64::INFINITY);
            }
            lower = lower.min(product.next_down());
            upper = upper.max(product.next_up());
        }
        Self { lower, upper }
    }

    /// Enclose division through an outward reciprocal; zero-crossing divisors
    /// have no finite enclosure.
    pub(crate) fn div(self, other: Self) -> Self {
        if other.contains_zero() {
            return Self::from_bounds(f64::NEG_INFINITY, f64::INFINITY);
        }
        let reciprocal = Self {
            lower: (1.0 / other.upper).next_down(),
            upper: (1.0 / other.lower).next_up(),
        };
        self.mul(reciprocal)
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
    }
}
