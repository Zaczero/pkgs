//! Exact point-to-segment distance over stored Cartesian segments.

use std::cmp::Ordering;

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational as Rational};
use crate::geometry::{GeometryErrorKind, Segment, XY};

#[derive(Clone, Debug)]
pub(in crate::geometry) enum ExactPoint {
    Stored {
        xy: XY,
        exact: std::sync::OnceLock<Box<[Rational; 2]>>,
    },
    Constructed(Box<[Rational; 2]>),
}

impl ExactPoint {
    pub(in crate::geometry) const fn from_xy(point: XY) -> Self {
        Self::Stored {
            xy: point,
            exact: std::sync::OnceLock::new(),
        }
    }

    pub(super) fn new(x: Rational, y: Rational) -> Self {
        Self::Constructed(Box::new([x, y]))
    }

    pub(super) const fn stored_xy(&self) -> Option<XY> {
        match self {
            Self::Stored { xy, .. } => Some(*xy),
            Self::Constructed(_) => None,
        }
    }

    pub(super) fn rounded_xy(&self) -> Result<XY> {
        if let Some(xy) = self.stored_xy() {
            return Ok(xy);
        }
        Ok(XY::new(self.x().round()?, self.y().round()?))
    }

    fn components(&self) -> &[Rational; 2] {
        match self {
            Self::Stored { xy, exact } => {
                exact.get_or_init(|| Box::new([Rational::from_f64(xy.x), Rational::from_f64(xy.y)]))
            },
            Self::Constructed(exact) => exact,
        }
    }

    pub(super) fn x(&self) -> &Rational {
        let [x, _] = self.components();
        x
    }

    pub(super) fn y(&self) -> &Rational {
        let [_, y] = self.components();
        y
    }

    pub(super) fn from_homogeneous(value: [ExactDyadic; 3]) -> Result<Self> {
        let [x, y, w] = value;
        Ok(Self::new(
            Rational::new(x, w.clone())?,
            Rational::new(y, w)?,
        ))
    }

    pub(in crate::geometry) fn from_homogeneous_ref(value: &[ExactDyadic; 3]) -> Result<Self> {
        let [x, y, w] = value;
        Ok(Self::new(
            Rational::new(x.clone(), w.clone())?,
            Rational::new(y.clone(), w.clone())?,
        ))
    }

    pub(super) fn same_position(&self, other: &Self) -> Result<bool> {
        Ok(self.x().try_cmp(other.x())?.is_eq() && self.y().try_cmp(other.y())?.is_eq())
    }
}

#[derive(Clone, Debug)]
pub(super) struct ExactCarrier {
    pub(super) ax: Rational,
    pub(super) ay: Rational,
    pub(super) dx: Rational,
    pub(super) dy: Rational,
    pub(super) norm_squared: Rational,
}

impl ExactCarrier {
    pub(super) fn from_segment(source: Segment) -> Result<Self> {
        let ax = Rational::from_f64(source.start.x);
        let ay = Rational::from_f64(source.start.y);
        let dx = Rational::from_f64(source.end.x).try_subtract(&ax)?;
        let dy = Rational::from_f64(source.end.y).try_subtract(&ay)?;
        let norm_squared = dx.try_square()?.try_add(&dy.try_square()?)?;
        Ok(Self {
            ax,
            ay,
            dx,
            dy,
            norm_squared,
        })
    }
}

/// An exact ratio kept unreduced across transient distance calculations.
///
/// The denominator is always positive. Deferring normalization until a value
/// crosses back into the certificate path avoids paying for greatest-common-
/// divisor work on every losing nearest-segment candidate.
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
            return Err(GeometryErrorKind::ExactRatioZeroDenominator.into());
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
            return Err(GeometryErrorKind::ExactRatioZeroDenominator.into());
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

fn span_coordinate_difference(
    point: &Rational,
    origin: &Rational,
    direction: &Rational,
    parameter: &Rational,
) -> Result<UnreducedRatio> {
    let point = UnreducedRatio::from_rational(point)?;
    let origin = UnreducedRatio::from_rational(origin)?;
    let direction = UnreducedRatio::from_rational(direction)?;
    let parameter = UnreducedRatio::from_rational(parameter)?;
    origin
        .try_add(&direction.try_product(&parameter)?)?
        .try_subtract(&point)
}

/// Exact squared distance from `point` to a parameter span of one stored
/// segment. Hausdorff shares its cached carrier; one-shot segment queries
/// build that carrier in [`point_segment_distance_squared_exact`].
pub(super) fn point_span_distance_squared(
    point: &ExactPoint,
    carrier: &ExactCarrier,
    low: &Rational,
    high: &Rational,
) -> Result<UnreducedRatio> {
    let px = UnreducedRatio::try_difference(point.x(), &carrier.ax)?;
    let py = UnreducedRatio::try_difference(point.y(), &carrier.ay)?;
    let dx = UnreducedRatio::from_rational(&carrier.dx)?;
    let dy = UnreducedRatio::from_rational(&carrier.dy)?;
    let norm_squared = UnreducedRatio::from_rational(&carrier.norm_squared)?;
    let dot = px.try_product(&dx)?.try_add(&py.try_product(&dy)?)?;
    let low_scaled = UnreducedRatio::from_rational(low)?.try_product(&norm_squared)?;
    let high_scaled = UnreducedRatio::from_rational(high)?.try_product(&norm_squared)?;
    let parameter = if carrier.norm_squared.is_zero() || dot.try_cmp(&low_scaled)? == Ordering::Less
    {
        low
    } else if dot.try_cmp(&high_scaled)? == Ordering::Greater {
        high
    } else {
        let cross = px.try_product(&dy)?.try_subtract(&py.try_product(&dx)?)?;
        return cross.try_square()?.try_quotient(&norm_squared);
    };
    let dx = span_coordinate_difference(point.x(), &carrier.ax, &carrier.dx, parameter)?;
    let dy = span_coordinate_difference(point.y(), &carrier.ay, &carrier.dy, parameter)?;
    dx.try_square()?.try_add(&dy.try_square()?)
}

/// Exact squared distance from an exact point to a stored segment.
pub(in crate::geometry) fn point_segment_distance_squared_exact(
    point: &ExactPoint,
    segment: Segment,
) -> Result<Rational> {
    let carrier = ExactCarrier::from_segment(segment)?;
    point_span_distance_squared(point, &carrier, &Rational::zero(), &Rational::one())?.to_rational()
}

#[cfg(test)]
mod tests;
