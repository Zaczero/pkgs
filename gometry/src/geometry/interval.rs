//! Outward binary64 intervals for geographic distance certificates.

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Interval {
    lower: f64,
    upper: f64,
}

impl Interval {
    pub(crate) const fn exact(value: f64) -> Self {
        Self {
            lower: value,
            upper: value,
        }
    }

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

    pub(crate) fn add(self, other: Self) -> Self {
        Self {
            lower: (self.lower + other.lower).next_down(),
            upper: (self.upper + other.upper).next_up(),
        }
    }

    pub(crate) fn sub(self, other: Self) -> Self {
        Self {
            lower: (self.lower - other.upper).next_down(),
            upper: (self.upper - other.lower).next_up(),
        }
    }

    pub(crate) fn mul(self, other: Self) -> Self {
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

    pub(crate) fn sqrt(self) -> Self {
        if self.upper < 0.0 {
            return Self::from_bounds(f64::NAN, f64::NAN);
        }
        Self {
            lower: self.lower.max(0.0).sqrt().next_down().max(0.0),
            upper: self.upper.sqrt().next_up(),
        }
    }

    pub(crate) fn abs_upper(self) -> f64 {
        self.lower.abs().max(self.upper.abs())
    }

    pub(crate) fn max_abs_upper(self) -> f64 {
        self.abs_upper().next_up()
    }
}
