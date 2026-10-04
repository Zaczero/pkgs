//! Exact and interval numerics for the bounded search.
//!
//! A branch is discarded only when its interval proves it cannot hold the
//! answer, so the bound types carry their own accuracy: a float box for the
//! cheap test and an unreduced rational for the exact one.

use std::cmp::Ordering;

use super::{ExactCarrier, ExactPoint, Rational, Result, Segment, nonnegative_upper_bound};
use crate::geometry::DdInterval;
use crate::geometry::tessellation::exact::Interval;

pub(super) trait DistanceInterval: Copy {
    fn delta(left: f64, right: f64) -> Option<Self>;
    fn add_bounds(self, other: Self) -> Option<Self>;
    fn sub_bounds(self, other: Self) -> Option<Self>;
    fn mul_bounds(self, other: Self) -> Option<Self>;
    fn square_bounds(self) -> Option<Self>;
    fn enclosure(self) -> (f64, f64);
}

impl DistanceInterval for Interval {
    fn delta(left: f64, right: f64) -> Option<Self> {
        let value = Self::exact(left).sub(Self::exact(right));
        value.is_finite().then_some(value)
    }

    fn add_bounds(self, other: Self) -> Option<Self> {
        let value = self.add(other);
        value.is_finite().then_some(value)
    }

    fn sub_bounds(self, other: Self) -> Option<Self> {
        let value = self.sub(other);
        value.is_finite().then_some(value)
    }

    fn mul_bounds(self, other: Self) -> Option<Self> {
        let value = self.mul(other);
        value.is_finite().then_some(value)
    }

    fn square_bounds(self) -> Option<Self> {
        let (lo, hi) = if self.lo >= 0.0 {
            (self.lo, self.hi)
        } else if self.hi <= 0.0 {
            (-self.hi, -self.lo)
        } else {
            (0.0, (-self.lo).max(self.hi))
        };
        let value = Self {
            lo: (lo * lo).next_down().max(0.0),
            hi: (hi * hi).next_up(),
        };
        value.is_finite().then_some(value)
    }

    fn enclosure(self) -> (f64, f64) {
        (self.lo, self.hi)
    }
}

impl DistanceInterval for DdInterval {
    fn delta(left: f64, right: f64) -> Option<Self> {
        Self::difference(left, right)
    }

    fn add_bounds(self, other: Self) -> Option<Self> {
        self.add(other)
    }

    fn sub_bounds(self, other: Self) -> Option<Self> {
        self.sub(other)
    }

    fn mul_bounds(self, other: Self) -> Option<Self> {
        self.mul(other)
    }

    fn square_bounds(self) -> Option<Self> {
        self.square()
    }

    fn enclosure(self) -> (f64, f64) {
        self.outward_bounds()
    }
}

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
        let ((min_x, max_x), (min_y, max_y)) = if let Some(point) = point.stored_xy() {
            ((point.x, point.x), (point.y, point.y))
        } else {
            (point.x().outward_bounds()?, point.y().outward_bounds()?)
        };
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

    #[expect(
        clippy::arithmetic_side_effects,
        clippy::float_cmp,
        reason = "endpoint identity selects one or two coordinates per axis, whose count cannot overflow"
    )]
    pub(super) fn distinct_corners(self) -> impl Iterator<Item = (f64, f64)> {
        let nx = usize::from(self.min_x != self.max_x) + 1;
        let ny = usize::from(self.min_y != self.max_y) + 1;
        (0..nx).flat_map(move |x| {
            let x = if x == 0 { self.min_x } else { self.max_x };
            (0..ny).map(move |y| (x, if y == 0 { self.min_y } else { self.max_y }))
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct SupportSpan {
    pub(super) source: Segment,
    pub(super) carrier: std::sync::OnceLock<Box<ExactCarrier>>,
    pub(super) range: Option<Box<SelectedSpan>>,
    pub(super) bounds: FloatBox,
}

#[derive(Clone, Debug)]
pub(super) struct SelectedSpan {
    pub(super) parameters: [Rational; 2],
    pub(super) start: ExactPoint,
}

impl SupportSpan {
    pub(super) fn start_point(&self) -> ExactPoint {
        self.range.as_ref().map_or_else(
            || ExactPoint::from_xy(self.source.start),
            |selected| selected.start.clone(),
        )
    }

    pub(super) fn lo(&self) -> &Rational {
        let [lo, _] = self.parameters();
        lo
    }

    pub(super) fn hi(&self) -> &Rational {
        let [_, hi] = self.parameters();
        hi
    }

    fn parameters(&self) -> &[Rational; 2] {
        static FULL: std::sync::LazyLock<[Rational; 2]> =
            std::sync::LazyLock::new(|| [Rational::zero(), Rational::one()]);
        self.range
            .as_deref()
            .map_or_else(|| &*FULL, |selected| &selected.parameters)
    }

    pub(super) fn carrier(&self) -> Result<&ExactCarrier> {
        self.carrier
            .get_or_try_init(|| ExactCarrier::from_segment(self.source).map(Box::new))
            .map(Box::as_ref)
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

    /// Bounds on the smaller distance, and whether `other` supplies its upper bound.
    pub(super) fn min_with_upper_source(self, other: Self) -> Result<(Self, bool)> {
        let lower = if other.lower.try_cmp(&self.lower)? == Ordering::Less {
            other.lower
        } else {
            self.lower
        };
        let other_is_upper_minimum = other.upper.try_cmp(&self.upper)? == Ordering::Less;
        let (upper, upper_f64) = if other_is_upper_minimum {
            (other.upper, other.upper_f64)
        } else {
            (self.upper, self.upper_f64)
        };
        Ok((
            Self {
                lower,
                upper,
                upper_f64,
            },
            other_is_upper_minimum,
        ))
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
