//! Exact and interval numerics for the bounded search.
//!
//! A branch is discarded only when its interval proves it cannot hold the
//! answer, so the bound types carry their own accuracy: a float box for the
//! cheap test and an unreduced rational for the exact one.

use std::cmp::Ordering;

use super::{
    ExactDyadic, ExactPoint, ExactSign, Rational, Result, Segment, nonnegative_upper_bound,
};

pub(super) struct Accuracy {
    pub(super) error: ErrorBound,
    pub(super) scale_squared: Rational,
}

pub(super) enum ErrorBound {
    Absolute(Rational),
    Relative(Rational),
}

impl Accuracy {
    const DEFAULT_RELATIVE_ERROR: f64 = 1e-9;

    pub(super) fn new(max_error: Option<f64>, scale: f64) -> Result<Self> {
        let error = match max_error {
            Some(value) if value.is_finite() && value > 0.0 => {
                ErrorBound::Absolute(Rational::from_f64(value))
            },
            Some(_) => {
                return Err(crate::geometry::GeometryErrorKind::message(
                    "max_error must be positive and finite",
                ));
            },
            None => ErrorBound::Relative(Rational::from_f64(Self::DEFAULT_RELATIVE_ERROR)),
        };
        if !(scale.is_finite() && scale > 0.0) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "Hausdorff unit scale must be positive and finite",
            ));
        }
        Ok(Self {
            error,
            scale_squared: Rational::from_f64(scale).try_square()?,
        })
    }

    pub(super) fn absolute_error(
        &self,
        lower_squared: &Rational,
        candidate: f64,
    ) -> Result<Rational> {
        match &self.error {
            ErrorBound::Absolute(error) => Ok(error.clone()),
            ErrorBound::Relative(relative) => {
                // Derive the allowance from a proven lower bracket L, not
                // from the returned approximation: L <= H makes r*L <= r*H.
                // candidate_certified then proves both sides against r*L,
                // which is therefore also a relative-error certificate.
                let mut lower = Rational::from_f64(candidate);
                if lower.try_square()?.try_cmp(lower_squared)? == Ordering::Greater {
                    let predecessor = candidate.next_down();
                    lower = if predecessor > 0.0 {
                        Rational::from_f64(predecessor)
                    } else {
                        // The predecessor of the minimum subnormal is zero.
                        // Correct rounding to that subnormal proves that half
                        // of it is still a lower bound on the exact root.
                        lower.try_product(&Rational::from_f64(0.5))?
                    };
                }
                lower.try_product(relative)
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct FloatBox {
    pub(super) min_x: f64,
    pub(super) max_x: f64,
    pub(super) min_y: f64,
    pub(super) max_y: f64,
}

impl FloatBox {
    pub(super) fn from_point(point: &ExactPoint) -> Result<Self> {
        let (min_x, max_x) = point.x.outward_bounds()?;
        let (min_y, max_y) = point.y.outward_bounds()?;
        if !(min_x.is_finite() && max_x.is_finite() && min_y.is_finite() && max_y.is_finite()) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "selected support has no finite conservative index enclosure",
            ));
        }
        Ok(Self {
            min_x,
            max_x,
            min_y,
            max_y,
        })
    }

    pub(super) const fn include(self, other: Self) -> Self {
        Self {
            min_x: self.min_x.min(other.min_x),
            max_x: self.max_x.max(other.max_x),
            min_y: self.min_y.min(other.min_y),
            max_y: self.max_y.max(other.max_y),
        }
    }

    pub(super) const fn meets(self, other: Self) -> bool {
        self.min_x <= other.max_x
            && other.min_x <= self.max_x
            && self.min_y <= other.max_y
            && other.min_y <= self.max_y
    }

    pub(super) const fn corners(self) -> [(f64, f64); 4] {
        [
            (self.min_x, self.min_y),
            (self.min_x, self.max_y),
            (self.max_x, self.min_y),
            (self.max_x, self.max_y),
        ]
    }
}

#[derive(Clone, Debug)]
pub(super) struct SupportSpan {
    pub(super) source: Segment,
    pub(super) carrier: ExactCarrier,
    pub(super) t_lo: Rational,
    pub(super) t_hi: Rational,
    pub(super) start: ExactPoint,
    pub(super) bounds: FloatBox,
}

#[derive(Clone, Debug)]
pub(super) struct ExactCarrier {
    pub(super) ax: Rational,
    pub(super) ay: Rational,
    pub(super) dx: Rational,
    pub(super) dy: Rational,
    pub(super) norm_squared: Rational,
}

/// An exact ratio kept unreduced across transient distance calculations.
///
/// The denominator is always positive. Deferring normalization until a value
/// crosses back into the certificate path avoids paying for greatest-common-
/// divisor work on every losing nearest-support candidate.
#[derive(Clone, Debug)]
pub(super) struct UnreducedRatio {
    pub(super) numerator: ExactDyadic,
    pub(super) denominator: ExactDyadic,
}

impl UnreducedRatio {
    pub(super) fn zero() -> Self {
        Self {
            numerator: ExactDyadic::zero(),
            denominator: ExactDyadic::one(),
        }
    }

    pub(super) fn new(numerator: ExactDyadic, denominator: ExactDyadic) -> Result<Self> {
        if denominator.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        if numerator.is_zero() {
            return Ok(Self::zero());
        }
        let (numerator, denominator) = if denominator.sign() == ExactSign::Negative {
            (numerator.neg(), denominator.neg())
        } else {
            (numerator, denominator)
        };
        Ok(Self {
            numerator,
            denominator,
        })
    }

    pub(super) fn from_rational(value: &Rational) -> Result<Self> {
        let (numerator, denominator) = value.parts();
        Self::new(numerator.clone(), denominator.clone())
    }

    pub(super) fn try_difference(left: &Rational, right: &Rational) -> Result<Self> {
        Self::from_rational(left)?.try_subtract(&Self::from_rational(right)?)
    }

    pub(super) const fn is_zero(&self) -> bool {
        self.numerator.is_zero()
    }

    pub(super) fn to_rational(&self) -> Result<Rational> {
        Rational::new(self.numerator.clone(), self.denominator.clone())
    }

    pub(super) fn try_add(&self, other: &Self) -> Result<Self> {
        if self.is_zero() {
            return Ok(other.clone());
        }
        if other.is_zero() {
            return Ok(self.clone());
        }
        if self.denominator == other.denominator {
            return Self::new(
                ExactDyadic::try_add_refs(&self.numerator, &other.numerator)?,
                self.denominator.clone(),
            );
        }
        if self.denominator.is_one() {
            let scaled = self.numerator.try_product(&other.denominator)?;
            return Self::new(
                ExactDyadic::try_add_refs(&scaled, &other.numerator)?,
                other.denominator.clone(),
            );
        }
        if other.denominator.is_one() {
            let scaled = other.numerator.try_product(&self.denominator)?;
            return Self::new(
                ExactDyadic::try_add_refs(&self.numerator, &scaled)?,
                self.denominator.clone(),
            );
        }
        Self::new(
            self.numerator
                .try_product(&other.denominator)?
                .try_add(other.numerator.try_product(&self.denominator)?)?,
            self.denominator.try_product(&other.denominator)?,
        )
    }

    pub(super) fn try_subtract(&self, other: &Self) -> Result<Self> {
        if self.is_zero() {
            let mut result = other.clone();
            if !result.is_zero() {
                result.numerator = result.numerator.neg();
            }
            return Ok(result);
        }
        if other.is_zero() {
            return Ok(self.clone());
        }
        if self.denominator == other.denominator {
            return Self::new(
                ExactDyadic::try_subtract_refs(&self.numerator, &other.numerator)?,
                self.denominator.clone(),
            );
        }
        if self.denominator.is_one() {
            let scaled = self.numerator.try_product(&other.denominator)?;
            return Self::new(
                ExactDyadic::try_subtract_refs(&scaled, &other.numerator)?,
                other.denominator.clone(),
            );
        }
        if other.denominator.is_one() {
            let scaled = other.numerator.try_product(&self.denominator)?;
            return Self::new(
                ExactDyadic::try_subtract_refs(&self.numerator, &scaled)?,
                self.denominator.clone(),
            );
        }
        Self::new(
            self.numerator
                .try_product(&other.denominator)?
                .try_subtract(other.numerator.try_product(&self.denominator)?)?,
            self.denominator.try_product(&other.denominator)?,
        )
    }

    pub(super) fn try_product(&self, other: &Self) -> Result<Self> {
        if self.is_zero() || other.is_zero() {
            return Ok(Self::zero());
        }
        let denominator = if self.denominator.is_one() {
            other.denominator.clone()
        } else if other.denominator.is_one() {
            self.denominator.clone()
        } else {
            self.denominator.try_product(&other.denominator)?
        };
        Self::new(self.numerator.try_product(&other.numerator)?, denominator)
    }

    pub(super) fn try_square(&self) -> Result<Self> {
        if self.is_zero() {
            return Ok(Self::zero());
        }
        let denominator = if self.denominator.is_one() {
            ExactDyadic::one()
        } else {
            self.denominator.try_product(&self.denominator)?
        };
        Self::new(self.numerator.try_product(&self.numerator)?, denominator)
    }

    pub(super) fn try_quotient(&self, other: &Self) -> Result<Self> {
        if other.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        Self::new(
            self.numerator.try_product(&other.denominator)?,
            self.denominator.try_product(&other.numerator)?,
        )
    }

    pub(super) fn try_cmp(&self, other: &Self) -> Result<Ordering> {
        if self.denominator == other.denominator {
            return Ok(self.numerator.cmp(&other.numerator));
        }
        if self.denominator.is_one() {
            return Ok(self
                .numerator
                .try_product(&other.denominator)?
                .cmp(&other.numerator));
        }
        if other.denominator.is_one() {
            return Ok(self
                .numerator
                .cmp(&other.numerator.try_product(&self.denominator)?));
        }
        Ok(self
            .numerator
            .try_product(&other.denominator)?
            .cmp(&other.numerator.try_product(&self.denominator)?))
    }

    #[cfg(test)]
    pub(super) fn try_cmp_rational(&self, other: &Rational) -> Result<Ordering> {
        Self::from_rational(other).and_then(|other| self.try_cmp(&other))
    }
}

/// Rigorous bounds on a squared distance, with `upper` also rounded upward
/// to binary64 for pruning.
pub(super) struct SquaredBounds {
    pub(super) lower: Rational,
    pub(super) upper: Rational,
    pub(super) upper_f64: f64,
}

impl SquaredBounds {
    pub(super) fn exact(value: Rational) -> Result<Self> {
        let upper_f64 = nonnegative_upper_bound(&value)?;
        Ok(Self {
            lower: value.clone(),
            upper: value,
            upper_f64,
        })
    }

    pub(super) fn enclosing(lower: f64, upper: f64) -> Self {
        Self {
            lower: Rational::from_f64(lower),
            upper: Rational::from_f64(upper),
            upper_f64: upper,
        }
    }

    /// Bounds on the smaller of two squared distances.
    pub(super) fn min(self, other: Self) -> Result<Self> {
        let lower = if other.lower.try_cmp(&self.lower)? == Ordering::Less {
            other.lower
        } else {
            self.lower
        };
        let (upper, upper_f64) = if other.upper.try_cmp(&self.upper)? == Ordering::Less {
            (other.upper, other.upper_f64)
        } else {
            (self.upper, self.upper_f64)
        };
        Ok(Self {
            lower,
            upper,
            upper_f64,
        })
    }
}

/// How wide a binary64 enclosure of a squared source distance may be before
/// its evaluation falls back to exact arithmetic: a fraction of the answer's
/// error budget, relative or absolute, so interval bounds never keep the
/// certificate from closing.
#[derive(Clone, Copy)]
pub(super) struct Tightness {
    pub(super) relative: f64,
    pub(super) absolute: f64,
}

impl Tightness {
    /// Accepts any enclosure the binary64 evaluation can produce.
    pub(super) const ANY: Self = Self {
        relative: f64::INFINITY,
        absolute: f64::INFINITY,
    };

    /// Squared-distance widths map to distance errors through `2 d`, so an
    /// absolute distance allowance scales with the root of the upper bound.
    pub(super) fn admits(self, lower: f64, upper: f64) -> bool {
        let width = upper - lower;
        width <= self.relative * upper || width <= self.absolute * upper.sqrt()
    }
}
