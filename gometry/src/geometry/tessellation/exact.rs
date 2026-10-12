//! Exact stored-binary64 predicates and homogeneous constructions.
//!
//! Every finite binary64 is an integer times a power of two. This module owns
//! that representation for tessellation so topology decisions and shared
//! constructed vertices never depend on a lossy coordinate frame.

#![expect(
    clippy::missing_const_for_fn,
    clippy::useless_conversion,
    reason = "exact predicates favor uniform fallible expressions over lint-driven local rewrites"
)]

use std::cell::OnceCell;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use crate::error::{Error, ErrorKind, ResourceError};
use crate::geometry::embedding_search::{Unmetered, Work};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::stored_orientation_sign;
use crate::geometry::{GeometryErrorKind, Point, Result, XY};

mod dyadic;
mod predicates;

pub(in crate::geometry) use dyadic::*;
pub(in crate::geometry) use predicates::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PointInCycle {
    Outside,
    Boundary,
    Inside,
}

#[derive(Clone, Debug)]
pub(super) enum SegmentIntersection {
    None,
    Point(ExactPoint),
    Overlap { start: ExactPoint, end: ExactPoint },
}

impl ExactSign {
    pub(super) const fn as_i8(self) -> i8 {
        match self {
            Self::Negative => -1,
            Self::Zero => 0,
            Self::Positive => 1,
        }
    }

    /// Sign of an exact quotient. Zero absorbs the otherwise relative signs:
    /// a homogeneous denominator's sign cannot leak into the rational zero.
    const fn quotient(self, denominator: Self) -> Self {
        match (self, denominator) {
            (Self::Zero, _) | (_, Self::Zero) => Self::Zero,
            (Self::Negative, Self::Positive) | (Self::Positive, Self::Negative) => Self::Negative,
            (Self::Negative, Self::Negative) | (Self::Positive, Self::Positive) => Self::Positive,
        }
    }

    /// The sign read through a homogeneous weight: a negative weight
    /// reverses it, and a zero weight leaves it as computed.
    const fn weighted_by(self, weight: Self) -> Self {
        match (self, weight) {
            (Self::Negative, Self::Negative) => Self::Positive,
            (Self::Positive, Self::Negative) => Self::Negative,
            (sign, _) => sign,
        }
    }
}

/// An exact ratio of dyadics.
///
/// Source-boundary parameters are usually dyadic, but a pre-existing mesh
/// vertex can lie at a non-dyadic ratio of two stored binary64 endpoints.
/// Keeping the numerator and denominator as exact integers avoids making that
/// vertex a rounded source authority. Every constructor reduces the ratio, so
/// repeated midpoint splits stay linear in their binary parameter depth rather
/// than multiplying equivalent denominators. Equality and hashing use the ratio
/// value because constructors may distribute binary exponents differently.
#[derive(Clone, Debug)]
pub(crate) struct ExactRational {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

impl PartialEq for ExactRational {
    fn eq(&self, other: &Self) -> bool {
        self.try_eq_with_work(other, &mut Unmetered)
            .expect("unmetered rational equality cannot fail")
    }
}

impl Eq for ExactRational {}

impl Hash for ExactRational {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.numerator.negative ^ self.denominator.negative).hash(state);
        self.numerator.limbs.as_slice().hash(state);
        self.denominator.limbs.as_slice().hash(state);
        rational_exponent(self).hash(state);
    }
}

fn rational_exponent(value: &ExactRational) -> i128 {
    // Equivalent ratios may distribute a power of two across both operands.
    i128::from(value.numerator.exponent)
        .checked_sub(i128::from(value.denominator.exponent))
        .expect("i64 dyadic exponents differ within i128")
}

fn slices_equal_with_work<W: Work>(left: &[u64], right: &[u64], work: &mut W) -> Result<bool> {
    work.charge_into_error(1)?;
    if left.len() != right.len() {
        return Ok(false);
    }
    work.charge_product_into_error(left.len(), 1)?;
    Ok(left == right)
}

impl ExactRational {
    pub(in crate::geometry) fn new(
        numerator: ExactDyadic,
        denominator: ExactDyadic,
    ) -> Result<Self> {
        Self::new_with_work(numerator, denominator, &mut Unmetered)
    }

    pub(in crate::geometry) fn new_with_work<W: Work>(
        numerator: ExactDyadic,
        denominator: ExactDyadic,
        work: &mut W,
    ) -> Result<Self> {
        if denominator.is_zero() {
            return Err(GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        work.charge_into_error(1)?;
        if numerator.is_zero() {
            return Ok(Self {
                numerator: ExactDyadic::zero(),
                denominator: ExactDyadic::one(),
            });
        }
        let (mut numerator, mut denominator) = if denominator.sign() == ExactSign::Negative {
            (numerator.neg(), denominator.neg())
        } else {
            (numerator, denominator)
        };
        // A power-of-two denominator shares only a power of two with the
        // numerator, so reducing it is an exponent shift; this yields exactly
        // what `gcd` would, including its unit-numerator shortcut.
        if denominator.limbs.as_slice() == [1] {
            let unit_numerator = numerator.limbs.as_slice() == [1] && numerator.exponent == 0;
            if !unit_numerator && denominator.exponent != 0 {
                let shift = numerator.exponent.min(denominator.exponent);
                let numerator_exponent = numerator
                    .exponent
                    .checked_sub(shift)
                    .ok_or(ResourceError::SizeOverflow)?;
                let denominator_exponent = denominator
                    .exponent
                    .checked_sub(shift)
                    .ok_or(ResourceError::SizeOverflow)?;
                numerator.exponent = numerator_exponent;
                denominator.exponent = denominator_exponent;
            }
            return Ok(Self {
                numerator,
                denominator,
            });
        }
        let common = numerator.gcd_with_work(&denominator, work)?;
        let numerator = numerator.try_exact_quotient_with_work(&common, work)?;
        let denominator = denominator.try_exact_quotient_with_work(&common, work)?;
        Ok(Self {
            numerator,
            denominator,
        })
    }

    pub(in crate::geometry) fn try_clone_with_work<W: Work>(&self, work: &mut W) -> Result<Self> {
        Ok(Self {
            numerator: self.numerator.try_clone_with_work(work)?,
            denominator: self.denominator.try_clone_with_work(work)?,
        })
    }

    pub(in crate::geometry) fn try_eq_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<bool> {
        work.charge_into_error(1)?;
        let sign = self.numerator.negative ^ self.denominator.negative;
        if sign != (other.numerator.negative ^ other.denominator.negative) {
            return Ok(false);
        }
        if !slices_equal_with_work(
            self.numerator.limbs.as_slice(),
            other.numerator.limbs.as_slice(),
            work,
        )? {
            return Ok(false);
        }
        if !slices_equal_with_work(
            self.denominator.limbs.as_slice(),
            other.denominator.limbs.as_slice(),
            work,
        )? {
            return Ok(false);
        }
        work.charge_into_error(1)?;
        Ok(rational_exponent(self) == rational_exponent(other))
    }

    pub(in crate::geometry) fn zero() -> Self {
        Self {
            numerator: ExactDyadic::zero(),
            denominator: ExactDyadic::one(),
        }
    }

    pub(in crate::geometry) fn one() -> Self {
        Self {
            numerator: ExactDyadic::one(),
            denominator: ExactDyadic::one(),
        }
    }

    pub(in crate::geometry) fn from_f64(value: f64) -> Self {
        Self::from_dyadic(ExactDyadic::from_f64(value))
    }

    pub(in crate::geometry) fn from_dyadic(value: ExactDyadic) -> Self {
        if value.is_zero() {
            return Self::zero();
        }
        Self {
            numerator: value,
            denominator: ExactDyadic::one(),
        }
    }

    pub(in crate::geometry) fn is_zero(&self) -> bool {
        self.numerator.is_zero()
    }

    pub(in crate::geometry) fn is_one(&self) -> bool {
        self.numerator.is_one() && self.denominator.is_one()
    }

    pub(in crate::geometry) fn is_integer(&self) -> bool {
        self.denominator.is_one() && (self.numerator.is_zero() || self.numerator.exponent >= 0)
    }

    pub(in crate::geometry) fn sign(&self) -> ExactSign {
        self.numerator.sign()
    }

    pub(in crate::geometry) fn try_add(&self, other: &Self) -> Result<Self> {
        self.try_add_with_work(other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_add_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if self.denominator.is_one() && other.denominator.is_one() {
            let numerator = if self.numerator.is_zero() {
                other.numerator.try_clone_with_work(work)?
            } else if other.numerator.is_zero() {
                self.numerator.try_clone_with_work(work)?
            } else {
                ExactDyadic::try_combine_refs_with_work(
                    &self.numerator,
                    &other.numerator,
                    false,
                    work,
                )?
            };
            return Ok(Self::from_dyadic(numerator));
        }
        let left = self
            .numerator
            .try_product_with_work(&other.denominator, work)?;
        let right = other
            .numerator
            .try_product_with_work(&self.denominator, work)?;
        let numerator = left.try_add_with_work(right, work)?;
        let denominator = self
            .denominator
            .try_product_with_work(&other.denominator, work)?;
        Self::new_with_work(numerator, denominator, work)
    }

    pub(in crate::geometry) fn try_subtract(&self, other: &Self) -> Result<Self> {
        self.try_subtract_with_work(other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_subtract_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if self.denominator.is_one() && other.denominator.is_one() {
            return Ok(Self::from_dyadic(
                self.numerator
                    .try_subtract_ref_with_work(&other.numerator, work)?,
            ));
        }
        let left = self
            .numerator
            .try_product_with_work(&other.denominator, work)?;
        let right = other
            .numerator
            .try_product_with_work(&self.denominator, work)?;
        let numerator = left.try_subtract_with_work(right, work)?;
        let denominator = self
            .denominator
            .try_product_with_work(&other.denominator, work)?;
        Self::new_with_work(numerator, denominator, work)
    }

    pub(in crate::geometry) fn try_product(&self, other: &Self) -> Result<Self> {
        self.try_product_with_work(other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_product_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if self.denominator.is_one() && other.denominator.is_one() {
            return Ok(Self::from_dyadic(
                self.numerator
                    .try_product_with_work(&other.numerator, work)?,
            ));
        }
        let numerator = self
            .numerator
            .try_product_with_work(&other.numerator, work)?;
        let denominator = self
            .denominator
            .try_product_with_work(&other.denominator, work)?;
        Self::new_with_work(numerator, denominator, work)
    }

    pub(in crate::geometry) fn try_square(&self) -> Result<Self> {
        self.try_square_with_work(&mut Unmetered)
    }

    pub(in crate::geometry) fn try_square_with_work<W: Work>(&self, work: &mut W) -> Result<Self> {
        if self.denominator.is_one() {
            return Ok(Self::from_dyadic(
                self.numerator
                    .try_product_with_work(&self.numerator, work)?,
            ));
        }
        self.try_product_with_work(self, work)
    }

    pub(in crate::geometry) fn try_quotient(&self, other: &Self) -> Result<Self> {
        self.try_quotient_with_work(other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_quotient_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Self> {
        if other.is_one() {
            return self.try_clone_with_work(work);
        }
        let numerator = self
            .numerator
            .try_product_with_work(&other.denominator, work)?;
        let denominator = self
            .denominator
            .try_product_with_work(&other.numerator, work)?;
        Self::new_with_work(numerator, denominator, work)
    }

    pub(in crate::geometry) fn try_midpoint(&self, other: &Self) -> Result<Self> {
        self.try_midpoint_with_work(other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_midpoint_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Self> {
        let sum = self.try_add_with_work(other, work)?;
        if sum.denominator.is_one() {
            return Ok(Self::from_dyadic(
                sum.numerator
                    .try_product_with_work(&ExactDyadic::from_f64(0.5), work)?,
            ));
        }
        let numerator = sum.numerator.try_clone_with_work(work)?;
        let denominator = sum
            .denominator
            .try_product_with_work(&ExactDyadic::from_f64(2.0), work)?;
        Self::new_with_work(numerator, denominator, work)
    }

    pub(in crate::geometry) fn try_cmp(&self, other: &Self) -> Result<Ordering> {
        self.try_cmp_with_work(other, &mut Unmetered)
    }

    pub(in crate::geometry) fn try_cmp_with_work<W: Work>(
        &self,
        other: &Self,
        work: &mut W,
    ) -> Result<Ordering> {
        if self.denominator.is_one() && other.denominator.is_one() {
            return self.numerator.cmp_with_work(&other.numerator, work);
        }
        let left = self
            .numerator
            .try_product_with_work(&other.denominator, work)?;
        let right = other
            .numerator
            .try_product_with_work(&self.denominator, work)?;
        left.cmp_with_work(&right, work)
    }

    pub(in crate::geometry) fn parts(&self) -> (&ExactDyadic, &ExactDyadic) {
        (&self.numerator, &self.denominator)
    }

    pub(in crate::geometry) fn round(&self) -> Result<f64> {
        self.round_with_work(&mut Unmetered)
    }

    pub(in crate::geometry) fn round_with_work<W: Work>(&self, work: &mut W) -> Result<f64> {
        round_ratio_with_work(&self.numerator, &self.denominator, work)
    }

    pub(in crate::geometry) fn outward_bounds(&self) -> Result<(f64, f64)> {
        self.outward_bounds_with_work(&mut Unmetered)
    }

    pub(in crate::geometry) fn outward_bounds_with_work<W: Work>(
        &self,
        work: &mut W,
    ) -> Result<(f64, f64)> {
        outward_ratio_bounds_with_work(&self.numerator, &self.denominator, work)
    }

    pub(in crate::geometry) fn round_sqrt(&self) -> Result<f64> {
        self.round_sqrt_with_work(&mut Unmetered)
    }

    pub(in crate::geometry) fn round_sqrt_with_work<W: Work>(&self, work: &mut W) -> Result<f64> {
        if self.sign() == ExactSign::Negative {
            return Err(
                GeometryErrorKind::Invalid("negative exact squared distance".into()).into(),
            );
        }
        round_sqrt_ratio_with_work(&self.numerator, &self.denominator, work)
    }

    /// The exact parameter of `point` on the stored segment `start -> end`.
    ///
    /// The caller establishes collinearity; this routine only chooses the
    /// nonzero axis and preserves its exact ratio.
    #[expect(
        clippy::float_cmp,
        reason = "exact coordinate equality selects the nonzero source axis; tolerance would change the parameter"
    )]
    pub(super) fn from_segment(start: XY, end: XY, point: XY) -> Result<Self> {
        let (start, end, point) = if start.x != end.x {
            (start.x, end.x, point.x)
        } else if start.y != end.y {
            (start.y, end.y, point.y)
        } else {
            return Err(GeometryErrorKind::triangulation(
                "source constraint has identical endpoints",
            ));
        };
        Self::new(try_difference(point, start)?, try_difference(end, start)?)
    }

    /// Round the exact source point at this parameter, evaluating both
    /// coordinates from the original endpoints before either is emitted.
    pub(super) fn round_segment(&self, start: XY, end: XY) -> Result<XY> {
        let remainder = self.denominator.try_subtract_ref(&self.numerator)?;
        let x = ExactDyadic::from_f64(start.x)
            .try_product(&remainder)?
            .try_add(ExactDyadic::from_f64(end.x).try_product(&self.numerator)?)?;
        let y = ExactDyadic::from_f64(start.y)
            .try_product(&remainder)?
            .try_add(ExactDyadic::from_f64(end.y).try_product(&self.numerator)?)?;
        Ok(XY::new(
            round_ratio(&x, &self.denominator)?,
            round_ratio(&y, &self.denominator)?,
        ))
    }
}

struct ExactSupportCircle {
    center: ExactPoint,
    radius_numerator: ExactDyadic,
    radius_denominator: ExactDyadic,
}

fn try_radius_squared(center: &ExactPoint, point: XY) -> Result<(ExactDyadic, ExactDyadic)> {
    let px = ExactDyadic::from_f64(point.x);
    let py = ExactDyadic::from_f64(point.y);
    let center = center.coords()?;
    let dx = ExactDyadic::try_subtract_refs(&center.x, &px.try_product(&center.w)?)?;
    let dy = ExactDyadic::try_subtract_refs(&center.y, &py.try_product(&center.w)?)?;
    Ok((
        dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?,
        center.w.try_product(&center.w)?,
    ))
}

fn try_support_circle(support: &[XY]) -> Result<ExactSupportCircle> {
    match support {
        [point] => {
            let center = ExactPoint::homogeneous(
                ExactDyadic::from_f64(point.x),
                ExactDyadic::from_f64(point.y),
                ExactDyadic::one(),
            );
            Ok(ExactSupportCircle {
                center,
                radius_numerator: ExactDyadic::zero(),
                radius_denominator: ExactDyadic::one(),
            })
        },
        [a, b] => {
            let ax = ExactDyadic::from_f64(a.x);
            let ay = ExactDyadic::from_f64(a.y);
            let bx = ExactDyadic::from_f64(b.x);
            let by = ExactDyadic::from_f64(b.y);
            let center = ExactPoint::homogeneous(
                ax.try_add(bx)?,
                ay.try_add(by)?,
                ExactDyadic::from_f64(2.0),
            );
            let (radius_numerator, radius_denominator) = try_radius_squared(&center, *a)?;
            Ok(ExactSupportCircle {
                center,
                radius_numerator,
                radius_denominator,
            })
        },
        [a, b, c] => {
            let center = circumcenter(*a, *b, *c)?;
            let (radius_numerator, radius_denominator) = try_radius_squared(&center, *a)?;
            Ok(ExactSupportCircle {
                center,
                radius_numerator,
                radius_denominator,
            })
        },
        _ => Err(GeometryErrorKind::Invalid(
            "minimum circle support must contain one, two, or three points".into(),
        )
        .into()),
    }
}

/// Return the exact homogeneous center and squared-radius ratio of a support.
///
/// The tuple is intentionally made of the arithmetic owner types rather than
/// rounded coordinates. Callers that publish a realization can therefore
/// carry the construction through a unit scale and round only at that
/// publication boundary.
pub(in crate::geometry) fn try_support_circle_components(
    support: &[XY],
) -> Result<(
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
)> {
    let circle = try_support_circle(support)?;
    let center = circle.center.coords()?;
    Ok((
        center.x.clone(),
        center.y.clone(),
        center.w.clone(),
        circle.radius_numerator,
        circle.radius_denominator,
    ))
}

fn try_orient_source_center(a: XY, b: XY, center: &ExactPoint) -> Result<ExactSign> {
    let ax = ExactDyadic::from_f64(a.x);
    let ay = ExactDyadic::from_f64(a.y);
    let bx = ExactDyadic::from_f64(b.x);
    let by = ExactDyadic::from_f64(b.y);
    let edge_x = bx.try_subtract(ax.clone())?;
    let edge_y = by.try_subtract(ay.clone())?;
    let center = center.coords()?;
    let center_x = ExactDyadic::try_subtract_refs(&center.x, &ax.try_product(&center.w)?)?;
    let center_y = ExactDyadic::try_subtract_refs(&center.y, &ay.try_product(&center.w)?)?;
    let determinant = edge_x
        .try_product(&center_y)?
        .try_subtract(edge_y.try_product(&center_x)?)?;
    Ok(determinant.sign().quotient(center.w.sign()))
}

/// Independently certify a support candidate against all original points.
///
/// The final pass deliberately does not rely on the loop's current candidate
/// state.  It checks source containment again and then verifies the closed
/// KKT condition: a one-point support is coincident, a two-point support is a
/// diameter, and a three-point support is non-collinear with its exact
/// circumcenter in the closed support triangle.
pub(in crate::geometry) fn try_certify_support(
    support: &[XY],
    points: impl IntoIterator<Item = XY>,
) -> Result<bool> {
    if support.is_empty() || support.len() > 3 {
        return Err(GeometryErrorKind::Invalid(
            "minimum circle support must contain one, two, or three points".into(),
        )
        .into());
    }
    match support {
        [only] => Ok(points.into_iter().all(|point| same_xy(*only, point))),
        [a, b] => {
            if same_xy(*a, *b) {
                return Ok(false);
            }
            points.into_iter().try_fold(true, |valid, point| {
                if !valid {
                    return Ok(false);
                }
                try_circle_contains(support, point)
            })
        },
        [a, b, c] => {
            let orientation = orient2d(*a, *b, *c);
            if orientation == ExactSign::Zero {
                return Ok(false);
            }
            let contained = points.into_iter().try_fold(true, |valid, point| {
                if !valid {
                    return Ok(false);
                }
                try_circle_contains(support, point)
            })?;
            if !contained {
                return Ok(false);
            }
            let center = circumcenter(*a, *b, *c)?;
            for (left, right) in [(*a, *b), (*b, *c), (*c, *a)] {
                let edge_orientation = try_orient_source_center(left, right, &center)?;
                let inside = match orientation {
                    ExactSign::Positive => edge_orientation != ExactSign::Negative,
                    ExactSign::Negative => edge_orientation != ExactSign::Positive,
                    ExactSign::Zero => unreachable!(),
                };
                if !inside {
                    return Ok(false);
                }
            }
            Ok(true)
        },
        _ => unreachable!(),
    }
}

/// Round the exact center and squared radius of a source support.
///
/// The center and squared radius are each rounded once from their exact
/// constructions.  In particular, the radius is never recovered from the
/// rounded center.
#[cfg(test)]
pub(in crate::geometry) fn round_support_circle(support: &[XY]) -> Result<(XY, f64, bool)> {
    let circle = try_support_circle(support)?;
    let center = circle.center.round_nearest_even()?;
    let radius = round_sqrt_ratio(&circle.radius_numerator, &circle.radius_denominator)?;
    Ok((center, radius, circle.radius_numerator.is_zero()))
}

pub(in crate::geometry) fn incircle(a: XY, b: XY, c: XY, d: XY) -> Result<ExactSign> {
    if let Some(sign) = incircle_float(a, b, c, d) {
        return Ok(sign);
    }
    if let Some(sign) = incircle_lattice(a, b, c, d) {
        return Ok(sign);
    }
    if let Some(sign) = incircle_filtered(a, b, c, d) {
        return Ok(sign);
    }
    incircle_dyadic(a, b, c, d)
}

fn incircle_dyadic(a: XY, b: XY, c: XY, d: XY) -> Result<ExactSign> {
    incircle_dyadic_points(
        [ExactDyadic::from_f64(a.x), ExactDyadic::from_f64(a.y)],
        [ExactDyadic::from_f64(b.x), ExactDyadic::from_f64(b.y)],
        [ExactDyadic::from_f64(c.x), ExactDyadic::from_f64(c.y)],
        [ExactDyadic::from_f64(d.x), ExactDyadic::from_f64(d.y)],
    )
}

/// Exact geometric in-circle determinant over the original dyadic site
/// coordinates. Raw XY keeps its filtered entry point; retained chart sites
/// use this same final arithmetic after supplying native-plus-lift X.
pub(in crate::geometry) fn incircle_dyadic_points(
    a: [ExactDyadic; 2],
    b: [ExactDyadic; 2],
    c: [ExactDyadic; 2],
    d: [ExactDyadic; 2],
) -> Result<ExactSign> {
    let [ax, ay] = a;
    let [bx, by] = b;
    let [cx, cy] = c;
    let [dx, dy] = d;
    let adx = ax.try_subtract(dx.clone())?;
    let ady = ay.try_subtract(dy.clone())?;
    let bdx = bx.try_subtract(dx.clone())?;
    let bdy = by.try_subtract(dy.clone())?;
    let cdx = cx.try_subtract(dx)?;
    let cdy = cy.try_subtract(dy)?;
    let alift = adx.try_product(&adx)?.try_add(ady.try_product(&ady)?)?;
    let blift = bdx.try_product(&bdx)?.try_add(bdy.try_product(&bdy)?)?;
    let clift = cdx.try_product(&cdx)?.try_add(cdy.try_product(&cdy)?)?;
    let bcdet = bdx
        .try_product(&cdy)?
        .try_subtract(cdx.try_product(&bdy)?)?;
    let cadet = cdx
        .try_product(&ady)?
        .try_subtract(adx.try_product(&cdy)?)?;
    let abdet = adx
        .try_product(&bdy)?
        .try_subtract(bdx.try_product(&ady)?)?;
    alift
        .try_product(&bcdet)?
        .try_add(blift.try_product(&cadet)?)?
        .try_add(clift.try_product(&abdet)?)
        .map(|determinant| determinant.sign())
}

// ---------------------------------------------------------------------------
// Certified interval filter.
//
// Every `ExactPoint` carries an OUTWARD-ROUNDED binary64 interval that PROVABLY
// CONTAINS its exact rational coordinate. A comparison whose two intervals are
// strictly separated is therefore decided; anything else falls through to the
// unchanged exact comparator. The filter can only DECLINE, never decide wrongly:
// it is the same shape as `rect_bound` exclusion — a bound that contains the
// true object may establish a verdict, never a guess.
//
// Containment chain, per coordinate (all steps outward-rounded):
//   1. `leading_window` splits a magnitude `M * 2^exp` into `(m, e)` with
//      m in [2^63, 2^64) and  m*2^e <= M*2^exp <= (m+1)*2^e.   (exact by
//      construction: `m` is the truncated top-64-bit window and `e` restores
//      the discarded bit positions.)
//   2. `n/d` therefore lies in  [ m_n/(m_d+1) * 2^(e_n-e_d),
//                                 (m_n+1)/m_d * 2^(e_n-e_d) ].
//   3. Each u64 -> f64 conversion is widened by one `next_down`/`next_up`.
//      Both mantissas are >= 2^63, so one ulp there is >= 2^11 = 2048 > 1,
//      which is what makes `next_up(m as f64) >= m + 1` sound.
//   4. The division and the power-of-two rescale are each widened outward
//      again; overflow saturates to +/-INFINITY and underflow to 0 / MIN_POSITIVE
//      on the side that keeps containment.
//   Anything not provable (denominator zero, exponent arithmetic overflow)
//   yields the universal interval, which decides nothing.
#[derive(Clone, Copy, Debug)]
pub(in crate::geometry) struct Interval {
    pub(in crate::geometry) lo: f64,
    pub(in crate::geometry) hi: f64,
}

impl Interval {
    const UNIVERSAL: Self = Self {
        lo: f64::NEG_INFINITY,
        hi: f64::INFINITY,
    };

    /// `Some(Less)` / `Some(Greater)` only when the two intervals are strictly
    /// separated, which proves the order of the contained exact values.
    #[expect(
        clippy::float_cmp,
        reason = "an exact interval is one double, and two of them are equal exactly when their doubles are"
    )]
    fn compare(self, other: Self) -> Option<Ordering> {
        if self.hi < other.lo {
            Some(Ordering::Less)
        } else if self.lo > other.hi {
            Some(Ordering::Greater)
        } else if self.lo == self.hi && other.lo == other.hi && self.lo == other.lo {
            // Two exact intervals on one double: equal, certified. A frame
            // hit's side coordinate against that side's corner is the common
            // case, and it must not materialise the hit's exact coordinates.
            Some(Ordering::Equal)
        } else {
            None
        }
    }

    pub(in crate::geometry) const fn exact(value: f64) -> Self {
        Self {
            lo: value,
            hi: value,
        }
    }

    /// Outward-rounded sum.
    pub(in crate::geometry) fn add(self, other: Self) -> Self {
        Self {
            lo: (self.lo + other.lo).next_down(),
            hi: (self.hi + other.hi).next_up(),
        }
    }

    pub(in crate::geometry) const fn is_finite(self) -> bool {
        self.lo.is_finite() && self.hi.is_finite()
    }

    /// Outward-rounded difference. `next_down`/`next_up` absorb the rounding of
    /// the binary64 subtraction, so the result still contains every difference
    /// of contained values.
    pub(in crate::geometry) fn sub(self, other: Self) -> Self {
        Self {
            lo: (self.lo - other.hi).next_down(),
            hi: (self.hi - other.lo).next_up(),
        }
    }

    /// Outward-rounded product. Both operands are finite here (checked by the
    /// caller), so no `0 * inf` NaN can arise; the four corner products bound
    /// the product of any contained pair.
    pub(in crate::geometry) fn mul(self, other: Self) -> Self {
        let corners = [
            self.lo * other.lo,
            self.lo * other.hi,
            self.hi * other.lo,
            self.hi * other.hi,
        ];
        let mut lo = corners[0];
        let mut hi = corners[0];
        for &corner in &corners[1..] {
            lo = lo.min(corner);
            hi = hi.max(corner);
        }
        Self {
            lo: lo.next_down(),
            hi: hi.next_up(),
        }
    }

    /// The sign of every contained value, when the interval excludes zero.
    pub(in crate::geometry) fn sign(self) -> Option<ExactSign> {
        if self.lo > 0.0 {
            Some(ExactSign::Positive)
        } else if self.hi < 0.0 {
            Some(ExactSign::Negative)
        } else {
            None
        }
    }

    fn disjoint_from(self, other: Self) -> bool {
        self.hi < other.lo || self.lo > other.hi
    }
}

/// Split `|value|` into `(m, e)` with `m` in `[2^63, 2^64)` and
/// `m * 2^e <= |value| <= (m + 1) * 2^e`.
fn leading_window(value: &ExactDyadic) -> Option<(u64, i64)> {
    let (&high, rest) = value.limbs.split_last()?;
    let next = rest.last().copied().unwrap_or(0);
    let high_bits = 64_u32.wrapping_sub(high.leading_zeros());
    let window = (u128::from(high) << 64) | u128::from(next);
    let mantissa = (window >> high_bits) as u64;
    let bits = i64::try_from(rest.len())
        .ok()?
        .checked_mul(64)?
        .checked_add(i64::from(high_bits))?;
    Some((mantissa, value.exponent.checked_add(bits)?.checked_sub(64)?))
}

/// Multiply by `2^shift`, rounding outward in the requested direction and
/// saturating so containment survives overflow and underflow.
fn scale_pow2(mut value: f64, mut shift: i64, upward: bool) -> f64 {
    debug_assert!(value >= 0.0);
    if value == 0.0 {
        return 0.0;
    }
    while shift != 0 {
        let step = shift.clamp(-512, 512);
        value *= 2.0_f64.powi(step as i32);
        shift = shift.wrapping_sub(step);
        if value.is_infinite() {
            return if upward { f64::INFINITY } else { f64::MAX };
        }
        if value == 0.0 {
            return if upward { f64::MIN_POSITIVE } else { 0.0 };
        }
    }
    // The scale itself is exact for normal results; widen anyway so subnormal
    // rounding cannot escape the bound.
    if upward {
        value.next_up()
    } else {
        value.next_down().max(0.0)
    }
}

/// An interval provably containing `numerator / denominator`.
fn ratio_interval(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Interval {
    if numerator.is_zero() {
        return Interval { lo: 0.0, hi: 0.0 };
    }
    if denominator.is_zero() {
        return Interval::UNIVERSAL;
    }
    let (Some((mantissa_n, exponent_n)), Some((mantissa_d, exponent_d))) =
        (leading_window(numerator), leading_window(denominator))
    else {
        return Interval::UNIVERSAL;
    };
    let Some(shift) = exponent_n.checked_sub(exponent_d) else {
        return Interval::UNIVERSAL;
    };
    // m >= 2^63, so one ulp at that magnitude is >= 2^11: a single next_up
    // dominates the "+1", and a single next_down stays strictly below m.
    let low_numerator = (mantissa_n as f64).next_down();
    let high_numerator = (mantissa_n as f64).next_up();
    let low_denominator = (mantissa_d as f64).next_down();
    let high_denominator = (mantissa_d as f64).next_up();
    let low = (low_numerator / high_denominator).next_down().max(0.0);
    let high = (high_numerator / low_denominator).next_up();
    let magnitude = Interval {
        lo: scale_pow2(low, shift, false),
        hi: scale_pow2(high, shift, true),
    };
    if magnitude.lo.is_nan() || magnitude.hi.is_nan() {
        return Interval::UNIVERSAL;
    }
    if numerator.negative == denominator.negative {
        magnitude
    } else {
        Interval {
            lo: -magnitude.hi,
            hi: -magnitude.lo,
        }
    }
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct ExactPoint {
    coords: Coords,
    /// Certified outward-rounded bounds on `x/w` and `y/w`.
    bounds: [Interval; 2],
}

/// Exact homogeneous coordinates.
#[derive(Clone, Debug)]
struct Homogeneous {
    x: ExactDyadic,
    y: ExactDyadic,
    w: ExactDyadic,
}

/// Where a point's exact coordinates come from. A certified circumcenter
/// carries its sites and its certified nearest-even rounding, and builds the
/// dyadic coordinates only for a comparison its bounds cannot decide. Exact
/// coordinates are shared, so cloning a point never allocates.
#[derive(Clone, Debug)]
enum Coords {
    Exact(Rc<Homogeneous>),
    /// A stored binary64 point: exact as it stands, its dyadic form built
    /// only for a comparison the exact interval cannot decide.
    Double {
        xy: XY,
        exact: OnceCell<Rc<Homogeneous>>,
    },
    Certified {
        recipe: Recipe,
        rounded: XY,
        exact: OnceCell<Rc<Homogeneous>>,
    },
}

/// A bisector meeting a frame side: the exact construction decides a
/// declined hit, a parallel bisector has none, otherwise the lazy point.
#[derive(Debug)]
pub(super) enum FrameHit {
    Declined,
    Parallel,
    Point(ExactPoint),
}

/// The exact construction a certified point stands for.
#[derive(Clone, Copy, Debug)]
enum Recipe {
    Circumcenter([XY; 3]),
    /// The perpendicular bisector of the two sites meeting the frame side
    /// through the two corners.
    BisectorHit {
        sites: [XY; 2],
        side: [XY; 2],
    },
}

impl Recipe {
    /// The same geometric construction, up to the order of its sites and of a
    /// side's two corners: a bisector is symmetric in its sites, a
    /// circumcenter in its three, so the exact points are equal.
    fn same_construction(&self, other: &Self) -> bool {
        let same_pair = |a: [XY; 2], b: [XY; 2]| {
            (a[0] == b[0] && a[1] == b[1]) || (a[0] == b[1] && a[1] == b[0])
        };
        match (self, other) {
            (Self::Circumcenter(a), Self::Circumcenter(b)) => {
                let key = |sites: &[XY; 3]| {
                    let mut keys = sites.map(|xy| (xy.x.to_bits(), xy.y.to_bits()));
                    keys.sort_unstable();
                    keys
                };
                key(a) == key(b)
            },
            (
                Self::BisectorHit { sites: a, side: sa },
                Self::BisectorHit { sites: b, side: sb },
            ) => same_pair(*a, *b) && same_pair(*sa, *sb),
            _ => false,
        }
    }
}

impl ExactPoint {
    /// The ONLY constructor. Every homogeneous triple gets its certified
    /// interval here, so a point can never exist without its filter bound.
    fn homogeneous(x: ExactDyadic, y: ExactDyadic, w: ExactDyadic) -> Self {
        let bounds = [ratio_interval(&x, &w), ratio_interval(&y, &w)];
        if VERIFY_INTERVALS {
            verify_interval(&x, &w, bounds[0]);
            verify_interval(&y, &w, bounds[1]);
        }
        Self {
            coords: Coords::Exact(Rc::new(Homogeneous { x, y, w })),
            bounds,
        }
    }

    /// A circumcenter whose nearest-even rounding is certified: the exact
    /// value lies within half an ulp of `center.rounded`, so within its two
    /// binary64 neighbours, which are the bounds every comparison reads first.
    fn certified_circumcenter(
        sites: [XY; 3],
        center: &super::circumcenter::CertifiedCenter,
    ) -> Self {
        Self {
            coords: Coords::Certified {
                recipe: Recipe::Circumcenter(sites),
                rounded: center.rounded,
                exact: OnceCell::new(),
            },
            bounds: [
                Interval {
                    lo: center.bounds[0].0,
                    hi: center.bounds[0].1,
                },
                Interval {
                    lo: center.bounds[1].0,
                    hi: center.bounds[1].1,
                },
            ],
        }
    }

    fn coords(&self) -> Result<&Homogeneous> {
        let coords = match &self.coords {
            Coords::Exact(coords) => coords,
            Coords::Double { xy, exact } => exact.get_or_init(|| {
                Rc::new(Homogeneous {
                    x: ExactDyadic::from_f64(xy.x),
                    y: ExactDyadic::from_f64(xy.y),
                    w: ExactDyadic::one(),
                })
            }),
            Coords::Certified { recipe, exact, .. } => exact.get_or_try_init(|| {
                let coords = match *recipe {
                    Recipe::Circumcenter(sites) => intersection_homogeneous(
                        &ExactLine::perpendicular_bisector(sites[0], sites[1])?,
                        &ExactLine::perpendicular_bisector(sites[0], sites[2])?,
                    )?,
                    Recipe::BisectorHit { sites, side } => intersection_homogeneous(
                        &ExactLine::perpendicular_bisector(sites[0], sites[1])?,
                        &ExactLine::through_points(
                            &Self::from_xy(side[0]),
                            &Self::from_xy(side[1]),
                        )?,
                    )?,
                };
                Ok::<_, Error>(Rc::new(coords))
            })?,
        };
        Ok(coords)
    }

    pub(super) fn projected_coordinate(
        &self,
        x_coefficient: &ExactDyadic,
        y_coefficient: &ExactDyadic,
    ) -> Result<ExactRational> {
        let coords = self.coords()?;
        let x = coords.x.try_product(x_coefficient)?;
        let y = coords.y.try_product(y_coefficient)?;
        ExactRational::new(x.try_add(y)?, coords.w.clone())
    }

    /// The stored double behind a point made from one, `None` for any other
    /// construction.
    pub(super) fn as_double(&self) -> Option<XY> {
        match &self.coords {
            Coords::Double { xy, .. } => Some(*xy),
            Coords::Exact(_) | Coords::Certified { .. } => None,
        }
    }

    /// Where the perpendicular bisector of `sites` meets the frame side
    /// through `side`, certified.
    #[expect(
        clippy::float_cmp,
        reason = "an axis-aligned side's two corners share a coordinate exactly, by construction"
    )]
    pub(super) fn certified_bisector_hit(sites: [XY; 2], side: [XY; 2]) -> FrameHit {
        use super::circumcenter::{BisectorHit, SideAxis, certified_bisector_hit};
        let (axis, value) = if side[0].x == side[1].x {
            (SideAxis::FixedX, side[0].x)
        } else if side[0].y == side[1].y {
            (SideAxis::FixedY, side[0].y)
        } else {
            return FrameHit::Declined;
        };
        let free = match certified_bisector_hit(sites[0], sites[1], axis, value) {
            BisectorHit::Declined => return FrameHit::Declined,
            BisectorHit::Parallel => return FrameHit::Parallel,
            BisectorHit::Free(free) => free,
        };
        let rounded = match axis {
            SideAxis::FixedX => XY::new(value, free),
            SideAxis::FixedY => XY::new(free, value),
        };
        let bounds = match axis {
            SideAxis::FixedX => [Interval::exact(value), Interval {
                lo: free.next_down(),
                hi: free.next_up(),
            }],
            SideAxis::FixedY => [
                Interval {
                    lo: free.next_down(),
                    hi: free.next_up(),
                },
                Interval::exact(value),
            ],
        };
        FrameHit::Point(Self {
            coords: Coords::Certified {
                recipe: Recipe::BisectorHit { sites, side },
                rounded,
                exact: OnceCell::new(),
            },
            bounds,
        })
    }

    pub(super) fn x(&self) -> Result<&ExactDyadic> {
        Ok(&self.coords()?.x)
    }

    pub(super) fn y(&self) -> Result<&ExactDyadic> {
        Ok(&self.coords()?.y)
    }

    pub(super) fn w(&self) -> Result<&ExactDyadic> {
        Ok(&self.coords()?.w)
    }
}

/// Development self-check: prove `lo <= numerator/denominator <= hi` with the
/// exact comparator when interval verification is enabled.
const VERIFY_INTERVALS: bool = false;

fn verify_interval(numerator: &ExactDyadic, denominator: &ExactDyadic, bounds: Interval) {
    if denominator.is_zero() {
        return;
    }
    let one = ExactDyadic::one();
    if bounds.lo.is_finite() {
        assert!(
            compare_ratios(
                &ExactDyadic::from_f64(bounds.lo),
                &one,
                numerator,
                denominator
            )
            .is_ok_and(|order| order != Ordering::Greater),
            "interval lower bound {} exceeds the exact value",
            bounds.lo
        );
    }
    if bounds.hi.is_finite() {
        assert!(
            compare_ratios(
                &ExactDyadic::from_f64(bounds.hi),
                &one,
                numerator,
                denominator
            )
            .is_ok_and(|order| order != Ordering::Less),
            "interval upper bound {} is below the exact value",
            bounds.hi
        );
    }
}

impl ExactPoint {
    pub(in crate::geometry) fn from_xy(point: XY) -> Self {
        Self {
            coords: Coords::Double {
                xy: point,
                exact: OnceCell::new(),
            },
            bounds: [Interval::exact(point.x), Interval::exact(point.y)],
        }
    }

    pub(super) fn compare_lex(&self, other: &Self) -> Result<Ordering> {
        Ok(match self.compare_x(other)? {
            Ordering::Equal => self.compare_y(other)?,
            order @ (Ordering::Less | Ordering::Greater) => order,
        })
    }

    pub(in crate::geometry) fn same_position(&self, other: &Self) -> Result<bool> {
        if let (Coords::Certified { recipe: left, .. }, Coords::Certified { recipe: right, .. }) =
            (&self.coords, &other.coords)
            && left.same_construction(right)
        {
            // One construction reached from two cells: the same exact point,
            // without materialising either side.
            return Ok(true);
        }
        Ok(!self.separated_from(other)
            && self.compare_x(other)?.is_eq()
            && self.compare_y(other)?.is_eq())
    }

    /// Whether the point is finite (nonzero weight). A certified circumcenter
    /// is finite by construction, so this never materializes its coordinates.
    pub(super) fn is_finite(&self) -> bool {
        match &self.coords {
            Coords::Certified { .. } | Coords::Double { .. } => true,
            Coords::Exact(coords) => !coords.w.is_zero(),
        }
    }

    pub(super) fn midpoint(&self, other: &Self) -> Result<Self> {
        let (left, right) = (self.coords()?, other.coords()?);
        let denominator = left
            .w
            .try_product(&right.w)?
            .try_product(&ExactDyadic::from_f64(2.0))?;
        Ok(Self::homogeneous(
            left.x
                .try_product(&right.w)?
                .try_add(right.x.try_product(&left.w)?)?,
            left.y
                .try_product(&right.w)?
                .try_add(right.y.try_product(&left.w)?)?,
            denominator,
        ))
    }

    pub(in crate::geometry) fn compare_x(&self, other: &Self) -> Result<Ordering> {
        if let Some(order) = self.bounds[0].compare(other.bounds[0]) {
            return Ok(order);
        }
        compare_ratios(self.x()?, self.w()?, other.x()?, other.w()?)
    }

    pub(in crate::geometry) fn compare_y(&self, other: &Self) -> Result<Ordering> {
        if let Some(order) = self.bounds[1].compare(other.bounds[1]) {
            return Ok(order);
        }
        compare_ratios(self.y()?, self.w()?, other.y()?, other.w()?)
    }

    /// The certified outward bounds on `x` and `y`.
    pub(super) fn bounds(&self) -> [Interval; 2] {
        self.bounds
    }

    /// Certified positive: both coordinates lie strictly inside
    /// `[minx, maxx, miny, maxy]`, decided on the bounds alone.
    pub(super) fn strictly_inside(&self, bounds: [f64; 4]) -> bool {
        self.bounds[0].lo > bounds[0]
            && self.bounds[0].hi < bounds[1]
            && self.bounds[1].lo > bounds[2]
            && self.bounds[1].hi < bounds[3]
    }

    /// Certified negative: two points whose x- or y-intervals are strictly
    /// separated cannot be the same position.
    pub(super) fn separated_from(&self, other: &Self) -> bool {
        self.bounds[0].disjoint_from(other.bounds[0])
            || self.bounds[1].disjoint_from(other.bounds[1])
    }

    /// Materialize one shared exact vertex exactly once. Construction code must
    /// intern the returned coordinate by vertex identity and reuse it for every
    /// incident half-edge. Under gometry's topological-snap contract a clip event may
    /// differ from the source clip at the ULP level when exact incidence has no
    /// binary64 representation; validity, event order, empty differences, and
    /// interior disjointness remain mandatory, while bit-exact `equals` does
    /// not. Recomputing a shared event per cell can reintroduce cracks between
    /// adjacent cells.
    pub(super) fn round_nearest_even(&self) -> Result<XY> {
        match &self.coords {
            Coords::Certified { rounded, .. } => return Ok(*rounded),
            Coords::Double { xy, .. } => return Ok(*xy),
            Coords::Exact(_) => {},
        }
        if !self.is_finite() {
            return Err(GeometryErrorKind::voronoi("exact vertex lies at infinity"));
        }
        Ok(XY::new(
            round_ratio(self.x()?, self.w()?)?,
            round_ratio(self.y()?, self.w()?)?,
        ))
    }

    /// The nearest-even rounding, or `None` when the point lies at infinity
    /// or rounds outside the finite binary64 range.
    pub(super) fn finite_rounding(&self) -> Result<Option<XY>> {
        if !self.is_finite() {
            return Ok(None);
        }
        match self.round_nearest_even() {
            Ok(point) => Ok(Some(point)),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Geometry(GeometryErrorKind::ExactCoordinateOutOfRange)
                ) =>
            {
                Ok(None)
            },
            Err(error) => Err(error),
        }
    }
}

fn on_segment(a: &ExactPoint, b: &ExactPoint, point: &ExactPoint) -> Result<bool> {
    let between = |pa: Ordering, pb: Ordering| {
        (pa != Ordering::Less && pb != Ordering::Greater)
            || (pb != Ordering::Less && pa != Ordering::Greater)
    };
    Ok(orient_points(a, b, point)? == ExactSign::Zero
        && between(point.compare_x(a)?, point.compare_x(b)?)
        && between(point.compare_y(a)?, point.compare_y(b)?))
}

pub(super) fn compare_along(
    start: &ExactPoint,
    end: &ExactPoint,
    left: &ExactPoint,
    right: &ExactPoint,
) -> Result<Ordering> {
    let (axis_order, direction) = match start.compare_x(end)? {
        Ordering::Equal => (left.compare_y(right)?, start.compare_y(end)?),
        direction @ (Ordering::Less | Ordering::Greater) => (left.compare_x(right)?, direction),
    };
    Ok(if direction.is_gt() {
        axis_order.reverse()
    } else {
        axis_order
    })
}

pub(super) fn segment_intersection(
    a0: &ExactPoint,
    a1: &ExactPoint,
    b0: &ExactPoint,
    b1: &ExactPoint,
) -> Result<SegmentIntersection> {
    // A homogeneous intersection of parallel support lines has `w == 0`.
    // It is not a point on either finite segment; admitting it as a degenerate
    // segment endpoint can place an event beyond both stored endpoints.
    if !a0.is_finite() || !a1.is_finite() || !b0.is_finite() || !b1.is_finite() {
        return Ok(SegmentIntersection::None);
    }
    let [o0, o1, o2, o3] = [
        orient_points(a0, a1, b0)?,
        orient_points(a0, a1, b1)?,
        orient_points(b0, b1, a0)?,
        orient_points(b0, b1, a1)?,
    ];
    if [o0, o1, o2, o3].iter().all(|sign| *sign == ExactSign::Zero) {
        let (b_start, b_end) = if compare_along(a0, a1, b0, b1)?.is_gt() {
            (b1, b0)
        } else {
            (b0, b1)
        };
        let start = if compare_along(a0, a1, a0, b_start)?.is_gt() {
            a0
        } else {
            b_start
        };
        let end = if compare_along(a0, a1, a1, b_end)?.is_lt() {
            a1
        } else {
            b_end
        };
        return Ok(match compare_along(a0, a1, start, end)? {
            Ordering::Greater => SegmentIntersection::None,
            Ordering::Equal => SegmentIntersection::Point(start.clone()),
            Ordering::Less => SegmentIntersection::Overlap {
                start: start.clone(),
                end: end.clone(),
            },
        });
    }
    for (zero, (from, to, point)) in [
        (o0, (a0, a1, b0)),
        (o1, (a0, a1, b1)),
        (o2, (b0, b1, a0)),
        (o3, (b0, b1, a1)),
    ] {
        if zero == ExactSign::Zero && on_segment(from, to, point)? {
            return Ok(SegmentIntersection::Point(point.clone()));
        }
    }
    if o0.as_i8().wrapping_mul(o1.as_i8()) < 0 && o2.as_i8().wrapping_mul(o3.as_i8()) < 0 {
        return Ok(SegmentIntersection::Point(line_intersection(
            &ExactLine::through_points(a0, a1)?,
            &ExactLine::through_points(b0, b1)?,
        )?));
    }
    Ok(SegmentIntersection::None)
}

pub(super) fn angle_ccw_cmp(
    origin: &ExactPoint,
    left: &ExactPoint,
    right: &ExactPoint,
) -> Result<Ordering> {
    let half = |target: &ExactPoint| -> Result<bool> {
        let (target, center) = (target.coords()?, origin.coords()?);
        let dy = target
            .y
            .try_product(&center.w)?
            .try_subtract(center.y.try_product(&target.w)?)?;
        let dx = target
            .x
            .try_product(&center.w)?
            .try_subtract(center.x.try_product(&target.w)?)?;
        let weight = target.w.sign().quotient(center.w.sign());
        let signed = |value: &ExactDyadic| value.sign().weighted_by(weight);
        Ok(signed(&dy) == ExactSign::Positive
            || (signed(&dy) == ExactSign::Zero && signed(&dx) != ExactSign::Negative))
    };
    match (half(left)?, half(right)?) {
        (true, false) => Ok(Ordering::Less),
        (false, true) => Ok(Ordering::Greater),
        _ => match orient_points(origin, left, right)? {
            ExactSign::Positive => Ok(Ordering::Less),
            ExactSign::Negative => Ok(Ordering::Greater),
            ExactSign::Zero => Err(GeometryErrorKind::voronoi(
                "exact angular tie proves an un-noded overlap or T-junction",
            )
            .into()),
        },
    }
}

/// The shoelace sign in interval arithmetic. Every term is the cross
/// product of two affine vertices, each inside its certified interval, so the
/// accumulated interval contains the exact signed area; a sign is returned only
/// when it excludes zero. Declining falls through to the exact accumulation,
/// which is also the only path that can report the zero-orientation error.
fn cycle_pairs<'a>(
    open_cycle: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> impl Iterator<Item = (&'a ExactPoint, &'a ExactPoint)> {
    let mut shifted = open_cycle.clone();
    let first = shifted
        .next()
        .expect("exact cycle has at least three vertices");
    open_cycle.zip(shifted.chain(std::iter::once(first)))
}

fn cycle_orientation_filtered<'a>(
    open_cycle: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> Option<ExactSign> {
    let mut total = Interval::exact(0.0);
    for (left, right) in cycle_pairs(open_cycle) {
        if !left.is_finite() || !right.is_finite() {
            return None;
        }
        let term = left.bounds[0]
            .mul(right.bounds[1])
            .sub(left.bounds[1].mul(right.bounds[0]));
        if !term.is_finite() {
            return None;
        }
        total = total.add(term);
        if !total.is_finite() {
            return None;
        }
    }
    total.sign()
}

pub(super) fn cycle_orientation<'a, I>(open_cycle: I) -> Result<ExactSign>
where
    I: IntoIterator<Item = &'a ExactPoint>,
    I::IntoIter: Clone + ExactSizeIterator,
{
    let open_cycle = open_cycle.into_iter();
    if open_cycle.len() < 3 {
        return Err(GeometryErrorKind::voronoi("exact cycle has fewer than three vertices").into());
    }
    if let Some(sign) = cycle_orientation_filtered(open_cycle.clone()) {
        if VERIFY_INTERVALS {
            let exact = cycle_orientation_exact(open_cycle)?;
            if sign != exact {
                return Err(GeometryErrorKind::voronoi(
                    "filtered cycle orientation disagrees with exact orientation",
                )
                .into());
            }
        }
        return Ok(sign);
    }
    cycle_orientation_exact(open_cycle)
}

fn cycle_orientation_exact<'a>(
    open_cycle: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> Result<ExactSign> {
    let mut numerator = ExactDyadic::zero();
    let mut denominator = ExactDyadic::from_f64(1.0);
    for (left, right) in cycle_pairs(open_cycle) {
        let left = left.coords()?;
        let right = right.coords()?;
        let term_n = left
            .x
            .try_product(&right.y)?
            .try_subtract(left.y.try_product(&right.x)?)?;
        let term_d = left.w.try_product(&right.w)?;
        numerator = numerator
            .try_product(&term_d)?
            .try_add(term_n.try_product(&denominator)?)?;
        denominator = denominator.try_product(&term_d)?;
    }
    match numerator.sign() {
        ExactSign::Zero => {
            Err(GeometryErrorKind::voronoi("exact cycle has zero orientation").into())
        },
        sign @ (ExactSign::Negative | ExactSign::Positive) => {
            Ok(sign.weighted_by(denominator.sign()))
        },
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "a slice of nonzero-sized ExactPoints has at most isize::MAX elements, so the winding count cannot overflow i64 on 64-bit targets"
)]
pub(super) fn point_in_cycle(
    open_cycle: &[ExactPoint],
    point: &ExactPoint,
) -> Result<PointInCycle> {
    let mut winding = 0_i64;
    for (start, end) in open_cycle.iter().zip(open_cycle.iter().cycle().skip(1)) {
        if on_segment(start, end, point)? {
            return Ok(PointInCycle::Boundary);
        }
        let sy = start.compare_y(point)?;
        let ey = end.compare_y(point)?;
        let orientation = orient_points(start, end, point)?;
        if sy != Ordering::Greater && ey == Ordering::Greater && orientation == ExactSign::Positive
        {
            winding += 1;
        } else if ey != Ordering::Greater
            && sy == Ordering::Greater
            && orientation == ExactSign::Negative
        {
            winding -= 1;
        }
    }
    Ok(if winding == 0 {
        PointInCycle::Outside
    } else {
        PointInCycle::Inside
    })
}

/// Filter for "which stored site is nearer this exact point".
/// The exact fallback evaluates the stored sites' perpendicular bisector at
/// the homogeneous point; its signed line product accounts for the sign of
/// the point weight.
fn squared_distance_cmp_point_filtered(
    origin: &ExactPoint,
    left: XY,
    right: XY,
) -> Option<Ordering> {
    if !origin.is_finite() {
        return None;
    }
    let squared = |point: XY| {
        let dx = Interval::exact(point.x).sub(origin.bounds[0]);
        let dy = Interval::exact(point.y).sub(origin.bounds[1]);
        dx.mul(dx).add(dy.mul(dy))
    };
    let difference = squared(left).sub(squared(right));
    if !difference.is_finite() {
        return None;
    }
    match difference.sign()? {
        ExactSign::Negative => Some(Ordering::Less),
        ExactSign::Positive => Some(Ordering::Greater),
        ExactSign::Zero => None,
    }
}

pub(in crate::geometry) fn squared_distance_cmp_point(
    origin: &ExactPoint,
    left: XY,
    right: XY,
) -> Result<Ordering> {
    if let Some(order) = squared_distance_cmp_point_filtered(origin, left, right) {
        if VERIFY_INTERVALS && order != squared_distance_cmp_point_exact(origin, left, right)? {
            return Err(GeometryErrorKind::voronoi(
                "filtered squared-distance comparison disagrees",
            ));
        }
        return Ok(order);
    }
    squared_distance_cmp_point_exact(origin, left, right)
}

fn squared_distance_cmp_point_exact(origin: &ExactPoint, left: XY, right: XY) -> Result<Ordering> {
    debug_assert!(origin.is_finite());
    Ok(
        match signed_line_product(&ExactLine::perpendicular_bisector(left, right)?, origin)? {
            ExactSign::Negative => Ordering::Less,
            ExactSign::Zero => Ordering::Equal,
            ExactSign::Positive => Ordering::Greater,
        },
    )
}

/// A private frame whose four corners are stored binary64 (`w == 1`).
///
/// The frame is private scaffolding. The only property the pipeline needs from
/// it is that it strictly encloses every input point. That property is
/// established here by the same exact comparator the rest of the module uses,
/// so the cheap construction is CERTIFIED, not assumed. `None` means no
/// admissible binary64 box exists and the caller must fall back to the exact
/// degree-8 construction.
#[expect(
    clippy::needless_pass_by_value,
    reason = "the iterator is re-walked once per widening round"
)]
#[expect(
    clippy::indexing_slicing,
    reason = "each axis loop ranges over 0..2, matching the lengths of the coordinate and bound arrays"
)]
pub(super) fn enclosing_frame_binary64<'a>(
    points: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> Result<Option<[ExactPoint; 4]>> {
    // Seeding only. A wrong estimate costs extra certified widening rounds,
    // never a wrong frame.
    let approx = |numerator: &ExactDyadic, denominator: &ExactDyadic| -> Option<f64> {
        let magnitude = approximate_ratio(numerator, denominator)?;
        let negative = numerator.negative != denominator.negative;
        Some(if negative { -magnitude } else { magnitude })
    };
    let mut lo = [f64::INFINITY; 2];
    let mut hi = [f64::NEG_INFINITY; 2];
    for point in points.clone() {
        if !point.is_finite() {
            return Ok(None);
        }
        // The certified bounds are the estimate whenever they are finite; the
        // exact quotient is read only for a point whose bounds are not.
        let coordinates = if point.bounds[0].is_finite() && point.bounds[1].is_finite() {
            [point.bounds[0].hi, point.bounds[1].hi]
        } else {
            let Some(x) = approx(point.x()?, point.w()?) else {
                return Ok(None);
            };
            let Some(y) = approx(point.y()?, point.w()?) else {
                return Ok(None);
            };
            [x, y]
        };
        for axis in 0..2 {
            if !coordinates[axis].is_finite() {
                return Ok(None);
            }
            lo[axis] = lo[axis].min(coordinates[axis]);
            hi[axis] = hi[axis].max(coordinates[axis]);
        }
    }
    let span = (hi[0] - lo[0]).max(hi[1] - lo[1]).max(1.0);
    if !span.is_finite() {
        return Ok(None);
    }
    let mut box_lo = [lo[0] - span, lo[1] - span];
    let mut box_hi = [hi[0] + span, hi[1] + span];
    // Certify strict containment exactly; widen by one ulp whenever an input
    // lands on or outside a candidate bound. Bounded, so this stays a
    // certificate rather than an unbounded search.
    for _ in 0..64 {
        for axis in 0..2 {
            if !box_lo[axis].is_finite() || !box_hi[axis].is_finite() {
                return Ok(None);
            }
        }
        let low = ExactPoint::from_xy(XY::new(box_lo[0], box_lo[1]));
        let high = ExactPoint::from_xy(XY::new(box_hi[0], box_hi[1]));
        let mut widened = false;
        for point in points.clone() {
            for axis in 0..2 {
                let (below, above) = if axis == 0 {
                    (point.compare_x(&low)?, point.compare_x(&high)?)
                } else {
                    (point.compare_y(&low)?, point.compare_y(&high)?)
                };
                if !below.is_gt() {
                    box_lo[axis] = box_lo[axis].next_down();
                    widened = true;
                }
                if !above.is_lt() {
                    box_hi[axis] = box_hi[axis].next_up();
                    widened = true;
                }
            }
        }
        if !widened {
            return Ok(Some([
                ExactPoint::from_xy(XY::new(box_lo[0], box_lo[1])),
                ExactPoint::from_xy(XY::new(box_hi[0], box_lo[1])),
                ExactPoint::from_xy(XY::new(box_hi[0], box_hi[1])),
                ExactPoint::from_xy(XY::new(box_lo[0], box_hi[1])),
            ]));
        }
    }
    Ok(None)
}

pub(super) fn enclosing_frame(points: &[ExactPoint]) -> Result<[ExactPoint; 4]> {
    let [min_x, max_x, min_y, max_y] = extreme_points(points)?;
    let (min_x, max_x, min_y, max_y) = (
        min_x.coords()?,
        max_x.coords()?,
        min_y.coords()?,
        max_y.coords()?,
    );
    let span_x_n = max_x
        .x
        .try_product(&min_x.w)?
        .try_subtract(min_x.x.try_product(&max_x.w)?)?;
    let span_x_d = max_x.w.try_product(&min_x.w)?;
    let span_y_n = max_y
        .y
        .try_product(&min_y.w)?
        .try_subtract(min_y.y.try_product(&max_y.w)?)?;
    let span_y_d = max_y.w.try_product(&min_y.w)?;
    let (pad_n, pad_d) = if compare_ratios(&span_x_n, &span_x_d, &span_y_n, &span_y_d)?.is_gt() {
        (span_x_n, span_x_d)
    } else {
        (span_y_n, span_y_d)
    };
    if pad_n.is_zero() {
        return Err(GeometryErrorKind::voronoi("exact frame input has zero span").into());
    }
    let coordinate = |base_n: &ExactDyadic, base_d: &ExactDyadic, add: bool| -> Result<_> {
        let base = base_n.try_product(&pad_d)?;
        let pad = pad_n.try_product(base_d)?;
        let numerator = if add {
            base.try_add(pad)?
        } else {
            base.try_subtract(pad)?
        };
        Ok((numerator, base_d.try_product(&pad_d)?))
    };
    let lx = coordinate(&min_x.x, &min_x.w, false)?;
    let ux = coordinate(&max_x.x, &max_x.w, true)?;
    let ly = coordinate(&min_y.y, &min_y.w, false)?;
    let uy = coordinate(&max_y.y, &max_y.w, true)?;
    Ok([
        corner((&lx.0, &lx.1), (&ly.0, &ly.1))?,
        corner((&ux.0, &ux.1), (&ly.0, &ly.1))?,
        corner((&ux.0, &ux.1), (&uy.0, &uy.1))?,
        corner((&lx.0, &lx.1), (&uy.0, &uy.1))?,
    ])
}

/// The exact points attaining the minimum and maximum x, then y.
fn extreme_points<'a>(
    points: impl IntoIterator<Item = &'a ExactPoint>,
) -> Result<[&'a ExactPoint; 4]> {
    let mut points = points.into_iter();
    let first = points
        .next()
        .ok_or_else(|| GeometryErrorKind::voronoi("empty exact frame input"))?;
    let mut extremes = [first; 4];
    for point in points {
        if point.compare_x(extremes[0])?.is_lt() {
            extremes[0] = point;
        }
        if point.compare_x(extremes[1])?.is_gt() {
            extremes[1] = point;
        }
        if point.compare_y(extremes[2])?.is_lt() {
            extremes[2] = point;
        }
        if point.compare_y(extremes[3])?.is_gt() {
            extremes[3] = point;
        }
    }
    Ok(extremes)
}

/// The point at x = `x.0 / x.1` and y = `y.0 / y.1`.
fn corner(x: (&ExactDyadic, &ExactDyadic), y: (&ExactDyadic, &ExactDyadic)) -> Result<ExactPoint> {
    Ok(ExactPoint::homogeneous(
        x.0.try_product(y.1)?,
        y.0.try_product(x.1)?,
        x.1.try_product(y.1)?,
    ))
}

/// Tight axis-aligned bounds of exact points: no pad, no enclosing super-frame.
pub(super) fn axis_aligned_bounds<'a>(
    points: impl IntoIterator<Item = &'a ExactPoint>,
) -> Result<[ExactPoint; 4]> {
    let [min_x, max_x, min_y, max_y] = extreme_points(points)?;
    if min_x.compare_x(max_x)?.is_eq() || min_y.compare_y(max_y)?.is_eq() {
        return Err(GeometryErrorKind::voronoi("exact frame input has zero span").into());
    }
    let (min_x, max_x, min_y, max_y) = (
        min_x.coords()?,
        max_x.coords()?,
        min_y.coords()?,
        max_y.coords()?,
    );
    Ok([
        corner((&min_x.x, &min_x.w), (&min_y.y, &min_y.w))?,
        corner((&max_x.x, &max_x.w), (&min_y.y, &min_y.w))?,
        corner((&max_x.x, &max_x.w), (&max_y.y, &max_y.w))?,
        corner((&min_x.x, &min_x.w), (&max_y.y, &max_y.w))?,
    ])
}

pub(super) fn rectangular_boundary(points: &[Point], padded: bool) -> Result<[ExactPoint; 4]> {
    let first = points
        .first()
        .ok_or_else(|| GeometryErrorKind::voronoi("empty Voronoi site set"))?;
    let (mut min_x, mut max_x, mut min_y, mut max_y) = (first.x, first.x, first.y, first.y);
    for point in points.iter().skip(1) {
        min_x = min_x.min(point.x);
        max_x = max_x.max(point.x);
        min_y = min_y.min(point.y);
        max_y = max_y.max(point.y);
    }
    if padded {
        // Half the larger span, computed exactly, then each corner rounded
        // OUTWARD to a double: the padded rectangle is a convenience frame
        // and a double-valued one keeps every clip side representable, so the
        // cells partition exactly the rectangle they report.
        let span_x = try_difference(max_x, min_x)?;
        let span_y = try_difference(max_y, min_y)?;
        let span = if span_x.cmp(&span_y).is_gt() {
            span_x
        } else {
            span_y
        };
        if span.is_zero() {
            return Err(GeometryErrorKind::voronoi("exact frame input has zero span").into());
        }
        let half = span.try_product(&ExactDyadic::from_f64(0.5))?;
        let outward = |base: f64, add: bool| -> Result<f64> {
            let base = ExactDyadic::from_f64(base);
            let exact = if add {
                ExactDyadic::try_add_refs(&base, &half)?
            } else {
                ExactDyadic::try_subtract_refs(&base, &half)?
            };
            let rounded = round_ratio(&exact, &ExactDyadic::one())?;
            let overshoot = ExactDyadic::from_f64(rounded).cmp(&exact);
            Ok(match (add, overshoot) {
                (true, Ordering::Less) => rounded.next_up(),
                (false, Ordering::Greater) => rounded.next_down(),
                _ => rounded,
            })
        };
        min_x = outward(min_x, false)?;
        max_x = outward(max_x, true)?;
        min_y = outward(min_y, false)?;
        max_y = outward(max_y, true)?;
        if !(min_x.is_finite() && max_x.is_finite() && min_y.is_finite() && max_y.is_finite()) {
            return Err(
                GeometryErrorKind::voronoi("padded Voronoi frame overflows binary64").into(),
            );
        }
    }
    Ok([
        ExactPoint::from_xy(XY::new(min_x, min_y)),
        ExactPoint::from_xy(XY::new(max_x, min_y)),
        ExactPoint::from_xy(XY::new(max_x, max_y)),
        ExactPoint::from_xy(XY::new(min_x, max_y)),
    ])
}

/// Exact orientation of three finite homogeneous points.  The sign is
/// normalized for the (otherwise arbitrary) signs of their denominators.
/// Interval evaluation of the affine orientation determinant.
///
/// `orient_points` normalizes away the homogeneous denominators, so its verdict
/// IS the orientation of the three affine points `(x/w, y/w)`. Each of those
/// six coordinates lies in its point's certified interval, so evaluating the
/// determinant in outward-rounded interval arithmetic yields an interval that
/// CONTAINS the exact determinant. A sign is returned only when that interval
/// excludes zero, which proves it. Overlapping zero, or any non-finite operand,
/// declines to the exact determinant.
fn orient_points_filtered(a: &ExactPoint, b: &ExactPoint, c: &ExactPoint) -> Option<ExactSign> {
    if !a.is_finite() || !b.is_finite() || !c.is_finite() {
        return None;
    }
    let [ax, ay] = a.bounds;
    let [bx, by] = b.bounds;
    let [cx, cy] = c.bounds;
    if !(ax.is_finite()
        && ay.is_finite()
        && bx.is_finite()
        && by.is_finite()
        && cx.is_finite()
        && cy.is_finite())
    {
        return None;
    }
    let (bax, bay) = (bx.sub(ax), by.sub(ay));
    let (cax, cay) = (cx.sub(ax), cy.sub(ay));
    if !(bax.is_finite() && bay.is_finite() && cax.is_finite() && cay.is_finite()) {
        return None;
    }
    let determinant = bax.mul(cay).sub(bay.mul(cax));
    determinant.sign()
}

pub(super) fn orient_points(a: &ExactPoint, b: &ExactPoint, c: &ExactPoint) -> Result<ExactSign> {
    if let Some(sign) = orient_points_filtered(a, b, c) {
        if VERIFY_INTERVALS && sign != orient_points_exact(a, b, c)? {
            return Err(GeometryErrorKind::voronoi("filtered orientation disagrees"));
        }
        return Ok(sign);
    }
    orient_points_exact(a, b, c)
}

fn orient_points_exact(a: &ExactPoint, b: &ExactPoint, c: &ExactPoint) -> Result<ExactSign> {
    let (a, b, c) = (a.coords()?, b.coords()?, c.coords()?);
    let displacement = |to: &ExactDyadic, from: &ExactDyadic, to_w: &ExactDyadic| {
        to.try_product(&a.w)?.try_subtract(from.try_product(to_w)?)
    };
    let bax = displacement(&b.x, &a.x, &b.w)?;
    let bay = displacement(&b.y, &a.y, &b.w)?;
    let cax = displacement(&c.x, &a.x, &c.w)?;
    let cay = displacement(&c.y, &a.y, &c.w)?;
    let sign = bax
        .try_product(&cay)?
        .try_subtract(bay.try_product(&cax)?)?
        .sign();
    // Both displacement numerators carry one factor of `a.w`; it is
    // squared in the determinant and therefore cannot affect the sign.
    Ok(sign.weighted_by(b.w.sign().quotient(c.w.sign())))
}

/// Cross product of two exact homogeneous lines as raw coordinates. The result
/// is not normalized: signed products and ratio comparison operate directly on
/// it.
fn intersection_homogeneous(a: &ExactLine, b: &ExactLine) -> Result<Homogeneous> {
    let [a0, a1, a2] = &a.0;
    let [b0, b1, b2] = &b.0;
    Ok(Homogeneous {
        x: a1.try_product(b2)?.try_subtract(a2.try_product(b1)?)?,
        y: a2.try_product(b0)?.try_subtract(a0.try_product(b2)?)?,
        w: a0.try_product(b1)?.try_subtract(a1.try_product(b0)?)?,
    })
}

/// Cross product of two exact homogeneous lines, with certified bounds.
pub(in crate::geometry) fn line_intersection(a: &ExactLine, b: &ExactLine) -> Result<ExactPoint> {
    let Homogeneous { x, y, w } = intersection_homogeneous(a, b)?;
    Ok(ExactPoint::homogeneous(x, y, w))
}

pub(super) fn signed_line_product(line: &ExactLine, point: &ExactPoint) -> Result<ExactSign> {
    let point = point.coords()?;
    let sign = line.0[0]
        .try_product(&point.x)?
        .try_add(line.0[1].try_product(&point.y)?)?
        .try_add(line.0[2].try_product(&point.w)?)?
        .sign();
    Ok(sign.weighted_by(point.w.sign()))
}

pub(in crate::geometry) fn compare_ratios(
    an: &ExactDyadic,
    ad: &ExactDyadic,
    bn: &ExactDyadic,
    bd: &ExactDyadic,
) -> Result<Ordering> {
    compare_ratios_with_work(an, ad, bn, bd, &mut Unmetered)
}

pub(in crate::geometry) fn compare_ratios_with_work<W: Work>(
    an: &ExactDyadic,
    ad: &ExactDyadic,
    bn: &ExactDyadic,
    bd: &ExactDyadic,
    work: &mut W,
) -> Result<Ordering> {
    work.charge_into_error(1)?;
    if let Some(order) = ratio_order_filter(an, ad, bn, bd) {
        return Ok(order);
    }
    exact_ratio_order_with_work(an, ad, bn, bd, work)
}

/// The order of `an / ad` and `bn / bd` from two exact cross products.
fn exact_ratio_order_with_work<W: Work>(
    an: &ExactDyadic,
    ad: &ExactDyadic,
    bn: &ExactDyadic,
    bd: &ExactDyadic,
    work: &mut W,
) -> Result<Ordering> {
    let left = an.try_product_with_work(bd, work)?;
    let right = bn.try_product_with_work(ad, work)?;
    let mut order = left.cmp_with_work(&right, work)?;
    if ad.sign().as_i8().wrapping_mul(bd.sign().as_i8()) < 0 {
        order = order.reverse();
    }
    Ok(order)
}

#[cfg(test)]
fn exact_ratio_order(
    an: &ExactDyadic,
    ad: &ExactDyadic,
    bn: &ExactDyadic,
    bd: &ExactDyadic,
) -> Result<Ordering> {
    exact_ratio_order_with_work(an, ad, bn, bd, &mut Unmetered)
}

/// Relative separation two approximate ratio magnitudes need before their
/// order is certain: each carries under `3.1 * 2^-53` from its leading-bit
/// quotient, and the comparison rounds once more.
const RATIO_ORDER_TOLERANCE: f64 = 8.0 * (f64::EPSILON / 2.0);

/// The order of two exact ratios wherever signs or leading bits settle it;
/// `None` leaves near ties, equal ratios included, to the exact products.
fn ratio_order_filter(
    an: &ExactDyadic,
    ad: &ExactDyadic,
    bn: &ExactDyadic,
    bd: &ExactDyadic,
) -> Option<Ordering> {
    if ad.is_zero() || bd.is_zero() {
        return None;
    }
    let left_sign = an.sign().quotient(ad.sign());
    let right_sign = bn.sign().quotient(bd.sign());
    if left_sign != right_sign || left_sign == ExactSign::Zero {
        return Some(left_sign.as_i8().cmp(&right_sign.as_i8()));
    }
    let (left, left_exponent) = approximate_ratio_components(an, ad)?;
    let (right, right_exponent) = approximate_ratio_components(bn, bd)?;
    let magnitude = match left_exponent.checked_sub(right_exponent)? {
        shift @ -1..=1 => {
            let left = left * 2.0_f64.powi(shift as i32);
            if left > right * (1.0 + RATIO_ORDER_TOLERANCE) {
                Ordering::Greater
            } else if left < right * (1.0 - RATIO_ORDER_TOLERANCE) {
                Ordering::Less
            } else {
                return None;
            }
        },
        shift => shift.cmp(&0),
    };
    Some(if left_sign == ExactSign::Negative {
        magnitude.reverse()
    } else {
        magnitude
    })
}

const SQUARED_DISTANCE_ERRBOUND: f64 = 128.0 * f64::EPSILON;

fn normal_or_zero(value: f64) -> bool {
    value == 0.0 || value.is_normal()
}

fn squared_distance(origin: XY, point: XY) -> Result<ExactDyadic> {
    let dx = try_difference(point.x, origin.x)?;
    let dy = try_difference(point.y, origin.y)?;
    dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)
}

fn distance_within_filtered(origin: XY, point: XY, radius: f64) -> Option<bool> {
    let dx = point.x - origin.x;
    let dy = point.y - origin.y;
    let x_squared = dx * dx;
    let y_squared = dy * dy;
    let distance_squared = x_squared + y_squared;
    let radius_squared = radius * radius;
    let difference = distance_squared - radius_squared;
    let magnitude = distance_squared + radius_squared;
    if ![
        dx,
        dy,
        x_squared,
        y_squared,
        distance_squared,
        radius_squared,
        difference,
        magnitude,
    ]
    .into_iter()
    .all(normal_or_zero)
    {
        return None;
    }
    let bound = (magnitude * SQUARED_DISTANCE_ERRBOUND).next_up();
    if difference > bound {
        Some(false)
    } else if difference < -bound {
        Some(true)
    } else {
        None
    }
}

pub(super) fn distance_within(origin: XY, point: XY, radius: f64) -> Result<bool> {
    if let Some(within) = distance_within_filtered(origin, point, radius) {
        if VERIFY_INTERVALS && within != distance_within_exact(origin, point, radius)? {
            return Err(GeometryErrorKind::voronoi(
                "filtered distance inclusion disagrees",
            ));
        }
        return Ok(within);
    }
    distance_within_exact(origin, point, radius)
}

fn distance_within_exact(origin: XY, point: XY, radius: f64) -> Result<bool> {
    let radius = ExactDyadic::from_f64(radius);
    Ok(squared_distance(origin, point)?.cmp(&radius.try_product(&radius)?) == Ordering::Less)
}

pub(in crate::geometry) fn circumcenter(a: XY, b: XY, c: XY) -> Result<ExactPoint> {
    if orient2d(a, b, c) == ExactSign::Zero {
        return Err(GeometryErrorKind::voronoi(
            "cannot construct a circumcenter for collinear sites",
        ));
    }
    if let Some(center) = super::circumcenter::certified_circumcenter(a, b, c) {
        return Ok(ExactPoint::certified_circumcenter([a, b, c], &center));
    }
    line_intersection(
        &ExactLine::perpendicular_bisector(a, b)?,
        &ExactLine::perpendicular_bisector(a, c)?,
    )
}

fn approximate_ratio_components(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> Option<(f64, i64)> {
    let leading = |value: &ExactDyadic| -> Option<(u64, i64)> {
        let (&high, rest) = value.limbs.split_last()?;
        let next = rest.last().copied().unwrap_or(0);
        let high_bits = 64_u32.wrapping_sub(high.leading_zeros());
        let window = (u128::from(high) << 64) | u128::from(next);
        let significand = (window >> high_bits) as u64;
        let bit_length = i64::try_from(rest.len())
            .ok()?
            .checked_mul(64)?
            .checked_add(i64::from(high_bits))?;
        Some((significand, value.exponent.checked_add(bit_length)?))
    };
    let (numerator_significand, numerator_exponent) = leading(numerator)?;
    let (denominator_significand, denominator_exponent) = leading(denominator)?;
    let mut significand = numerator_significand as f64 / denominator_significand as f64;
    let mut exponent = numerator_exponent.checked_sub(denominator_exponent)?;
    if significand < 1.0 {
        significand *= 2.0;
        exponent = exponent.checked_sub(1)?;
    } else if significand >= 2.0 {
        significand *= 0.5;
        exponent = exponent.checked_add(1)?;
    }
    Some((significand, exponent))
}

fn scale_approximation(significand: f64, exponent: i64) -> Option<f64> {
    if exponent > 1023 {
        Some(f64::MAX)
    } else if exponent >= -1022 {
        let scaled = significand * 2.0_f64.powi(i32::try_from(exponent).ok()?);
        Some(if scaled.is_finite() { scaled } else { f64::MAX })
    } else if exponent < -1075 {
        Some(0.0)
    } else {
        Some(
            significand
                * f64::MIN_POSITIVE
                * 2.0_f64.powi(i32::try_from(exponent.wrapping_add(1022)).ok()?),
        )
    }
}

fn approximate_ratio(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Option<f64> {
    let (significand, exponent) = approximate_ratio_components(numerator, denominator)?;
    scale_approximation(significand, exponent)
}

/// Approximate a positive exact ratio's square root without first rounding
/// the squared ratio. The latter can underflow to zero even when its square
/// root is a representable normal, so the ratio exponent must be halved before
/// the final binary64 scaling.
fn approximate_sqrt_ratio(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Option<f64> {
    let (significand, exponent) = approximate_ratio_components(numerator, denominator)?;
    if exponent.rem_euclid(2) == 0 {
        scale_approximation(significand.sqrt(), exponent / 2)
    } else {
        scale_approximation((significand * 2.0).sqrt(), exponent.checked_sub(1)? / 2)
    }
}

fn exact_ratio_bracket_with_work<W: Work>(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    proposal: f64,
    sign: ExactSign,
    work: &mut W,
) -> Result<Option<(u64, u64, Option<ExactDyadic>)>> {
    const MAX_ULP_STEPS: usize = 4;
    let max_bits = f64::MAX.to_bits();
    let compare = |bits: u64, work: &mut W| -> Result<(Ordering, ExactDyadic)> {
        let magnitude = f64::from_bits(bits);
        let candidate = if sign == ExactSign::Negative {
            -magnitude
        } else {
            magnitude
        };
        let product = ExactDyadic::from_f64(candidate).try_product_with_work(denominator, work)?;
        let order = product.cmp_with_work(numerator, work)?;
        Ok((
            if numerator.negative {
                order.reverse()
            } else {
                order
            },
            product,
        ))
    };
    let mut bits = proposal.to_bits();
    work.charge_into_error(1)?;
    let (order, product) = compare(bits, work)?;
    match order {
        Ordering::Equal => return Ok(Some((bits, bits, None))),
        Ordering::Less => {
            let mut lower_product = product;
            for _ in 0..MAX_ULP_STEPS {
                work.charge_into_error(1)?;
                if bits == max_bits {
                    return Ok(None);
                }
                let lower = bits;
                bits = bits.wrapping_add(1);
                let (order, product) = compare(bits, work)?;
                match order {
                    Ordering::Less => lower_product = product,
                    Ordering::Equal => return Ok(Some((bits, bits, None))),
                    Ordering::Greater => return Ok(Some((lower, bits, Some(lower_product)))),
                }
            }
        },
        Ordering::Greater => {
            for _ in 0..MAX_ULP_STEPS {
                work.charge_into_error(1)?;
                if bits == 0 {
                    return Ok(None);
                }
                let upper = bits;
                bits = bits.wrapping_sub(1);
                let (order, product) = compare(bits, work)?;
                match order {
                    Ordering::Greater => {},
                    Ordering::Equal => return Ok(Some((bits, bits, None))),
                    Ordering::Less => return Ok(Some((bits, upper, Some(product)))),
                }
            }
        },
    }
    Ok(None)
}

fn round_ratio_binary_with_work<W: Work>(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    work: &mut W,
) -> Result<f64> {
    let sign = numerator.sign().quotient(denominator.sign());
    let numerator = numerator.try_clone_with_work(work)?.abs();
    let denominator = denominator.try_clone_with_work(work)?.abs();
    if numerator.is_zero() {
        return Ok(0.0);
    }
    let max = ExactDyadic::from_f64(f64::MAX).try_product_with_work(&denominator, work)?;
    if numerator.cmp_with_work(&max, work)? == Ordering::Greater {
        let overflow_midpoint = ExactDyadic::from_f64(f64::MAX)
            .try_add_with_work(ExactDyadic::from_f64(2.0_f64.powi(970)), work)?
            .try_product_with_work(&denominator, work)?;
        if numerator.cmp_with_work(&overflow_midpoint, work)? == Ordering::Less {
            return Ok(if sign == ExactSign::Negative {
                -f64::MAX
            } else {
                f64::MAX
            });
        }
        return Err(GeometryErrorKind::ExactCoordinateOutOfRange.into());
    }
    let mut low = 0_u64;
    let mut high = f64::MAX.to_bits();
    while low < high {
        work.charge_into_error(1)?;
        let mid = low.wrapping_add(high.wrapping_sub(low).div_ceil(2));
        let product =
            ExactDyadic::from_f64(f64::from_bits(mid)).try_product_with_work(&denominator, work)?;
        if product.cmp_with_work(&numerator, work)? == Ordering::Greater {
            high = mid.wrapping_sub(1);
        } else {
            low = mid;
        }
    }
    let lower = f64::from_bits(low);
    let rounded = if low == f64::MAX.to_bits() {
        lower
    } else {
        let upper = f64::from_bits(low.wrapping_add(1));
        // Compare 2*n/d with lower+upper. Equality is the rounding tie.
        let twice_numerator = numerator.try_product_with_work(&ExactDyadic::from_f64(2.0), work)?;
        let midpoint_scaled = ExactDyadic::from_f64(lower)
            .try_add_with_work(ExactDyadic::from_f64(upper), work)?
            .try_product_with_work(&denominator, work)?;
        match twice_numerator.cmp_with_work(&midpoint_scaled, work)? {
            Ordering::Less => lower,
            Ordering::Greater => upper,
            Ordering::Equal => {
                if low & 1 == 0 {
                    lower
                } else {
                    upper
                }
            },
        }
    };
    Ok(if sign == ExactSign::Negative {
        -rounded
    } else {
        rounded
    })
}

#[cfg(test)]
fn round_ratio_binary(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Result<f64> {
    round_ratio_binary_with_work(numerator, denominator, &mut Unmetered)
}

pub(in crate::geometry) fn round_ratio(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> Result<f64> {
    round_ratio_with_work(numerator, denominator, &mut Unmetered)
}

pub(in crate::geometry) fn round_ratio_with_work<W: Work>(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    work: &mut W,
) -> Result<f64> {
    #[cfg(all(test, debug_assertions))]
    {
        use crate::test_support::exact_operation_counts;
        exact_operation_counts::ratio_rounding();
    }
    if denominator.is_zero() {
        return Err(GeometryErrorKind::ExactRatioZeroDenominator.into());
    }
    let sign = numerator.sign().quotient(denominator.sign());
    if numerator.is_zero() {
        return Ok(0.0);
    }
    if numerator.cmp_magnitude_with_work(denominator, work)? == Ordering::Equal {
        return Ok(if sign == ExactSign::Negative {
            -1.0
        } else {
            1.0
        });
    }
    work.charge_into_error(1)?;
    if let Some(proposal) = approximate_ratio(numerator, denominator)
        && let Some((low, high, lower_product)) =
            exact_ratio_bracket_with_work(numerator, denominator, proposal, sign, work)?
    {
        if low == high {
            let rounded = f64::from_bits(low);
            return Ok(if sign == ExactSign::Negative {
                -rounded
            } else {
                rounded
            });
        }
        let lower = f64::from_bits(low);
        let upper = f64::from_bits(high);
        let lower_product = lower_product.expect("a two-point bracket has a lower product");
        let residual = numerator.try_subtract_ref_with_work(&lower_product, work)?;
        let spacing_exponent = {
            // The lower binade owns the gap at every power-of-two boundary.
            let encoded_exponent = (low >> 52) & 0x7FF;
            if encoded_exponent == 0 {
                -1074
            } else {
                (encoded_exponent as i64).wrapping_sub(1075)
            }
        };
        let midpoint_order = residual.cmp_magnitude_scaled_with_work(
            denominator,
            spacing_exponent.wrapping_sub(1),
            work,
        )?;
        let rounded = match midpoint_order {
            Ordering::Less => lower,
            Ordering::Equal if low & 1 == 0 => lower,
            Ordering::Greater | Ordering::Equal => upper,
        };
        return Ok(if sign == ExactSign::Negative {
            -rounded
        } else {
            rounded
        });
    }
    round_ratio_binary_with_work(numerator, denominator, work)
}

/// Return the tight binary64 interval containing an exact dyadic ratio.
///
/// `round_ratio` supplies the nearest binary64 value. Comparing that value
/// back against the exact ratio identifies whether only its predecessor or
/// successor is needed; the comparison itself remains exact, so the returned
/// interval never uses a rounded value as a decision about the ratio.
pub(in crate::geometry) fn outward_ratio_bounds(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> Result<(f64, f64)> {
    outward_ratio_bounds_with_work(numerator, denominator, &mut Unmetered)
}

pub(in crate::geometry) fn outward_ratio_bounds_with_work<W: Work>(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    work: &mut W,
) -> Result<(f64, f64)> {
    let rounded = round_ratio_with_work(numerator, denominator, work)?;
    work.charge_into_error(1)?;
    let rounded_exact = ExactDyadic::from_f64(rounded);
    match compare_ratios_with_work(
        numerator,
        denominator,
        &rounded_exact,
        &ExactDyadic::one(),
        work,
    )? {
        Ordering::Less => Ok((rounded.next_down(), rounded)),
        Ordering::Equal => Ok((rounded, rounded)),
        Ordering::Greater => Ok((rounded, rounded.next_up())),
    }
}

/// Round `sqrt(numerator / denominator)` to nearest binary64, ties to even.
///
/// The caller supplies exact nonnegative `numerator` and positive `denominator`.
/// The comparison stays in squared space, including subnormal and overflow
/// boundaries, so the squared ratio is never rounded before taking its root.
pub(in crate::geometry) fn round_sqrt_ratio(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
) -> Result<f64> {
    round_sqrt_ratio_with_work(numerator, denominator, &mut Unmetered)
}

pub(in crate::geometry) fn round_sqrt_ratio_with_work<W: Work>(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    work: &mut W,
) -> Result<f64> {
    if numerator.is_zero() {
        return Ok(0.0);
    }
    work.charge_into_error(1)?;
    let max = ExactDyadic::from_f64(f64::MAX);
    let overflow_midpoint =
        max.try_add_with_work(ExactDyadic::from_f64(2.0_f64.powi(970)), work)?;
    let overflow_squared = overflow_midpoint
        .try_product_with_work(&overflow_midpoint, work)?
        .try_product_with_work(denominator, work)?;
    if numerator.cmp_with_work(&overflow_squared, work)? != Ordering::Less {
        return Ok(f64::INFINITY);
    }
    work.charge_into_error(1)?;
    if let Some(proposal) = approximate_sqrt_ratio(numerator, denominator)
        && proposal.is_finite()
    {
        let proposal = if proposal == 0.0 {
            f64::from_bits(1)
        } else {
            proposal
        };
        if let Some((low, high)) =
            exact_sqrt_ratio_bracket_with_work(numerator, denominator, proposal, work)?
        {
            if low == high {
                return Ok(f64::from_bits(low));
            }
            let lower = f64::from_bits(low);
            let upper = f64::from_bits(high);
            // Compare the exact squared ratio with the square of the
            // midpoint without rounding the midpoint itself.
            let midpoint_sum = ExactDyadic::from_f64(lower)
                .try_add_with_work(ExactDyadic::from_f64(upper), work)?;
            let midpoint_squared = midpoint_sum
                .try_product_with_work(&midpoint_sum, work)?
                .try_product_with_work(denominator, work)?;
            let four_numerator =
                numerator.try_product_with_work(&ExactDyadic::from_f64(4.0), work)?;
            return Ok(
                match four_numerator.cmp_with_work(&midpoint_squared, work)? {
                    Ordering::Less => lower,
                    Ordering::Greater => upper,
                    Ordering::Equal => {
                        if low & 1 == 0 {
                            lower
                        } else {
                            upper
                        }
                    },
                },
            );
        }
    }
    round_sqrt_ratio_binary_with_work(numerator, denominator, work)
}

#[cfg(test)]
fn exact_sqrt_ratio_bracket(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    proposal: f64,
) -> Result<Option<(u64, u64)>> {
    exact_sqrt_ratio_bracket_with_work(numerator, denominator, proposal, &mut Unmetered)
}

fn exact_sqrt_ratio_bracket_with_work<W: Work>(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    proposal: f64,
    work: &mut W,
) -> Result<Option<(u64, u64)>> {
    let max_bits = f64::MAX.to_bits();
    let compare = |bits: u64, work: &mut W| -> Result<Ordering> {
        let candidate = ExactDyadic::from_f64(f64::from_bits(bits));
        let square = candidate
            .try_product_with_work(&candidate, work)?
            .try_product_with_work(denominator, work)?;
        square.cmp_with_work(numerator, work)
    };
    let bits = proposal.to_bits().min(max_bits);
    work.charge_into_error(1)?;
    let (mut lower, mut upper) = match compare(bits, work)? {
        Ordering::Equal => return Ok(Some((bits, bits))),
        Ordering::Less => {
            let mut lower = bits;
            let mut step = 1_u64;
            loop {
                work.charge_into_error(1)?;
                let distance = max_bits.wrapping_sub(lower);
                if distance == 0 {
                    return Ok(None);
                }
                let candidate = lower.wrapping_add(step.min(distance));
                match compare(candidate, work)? {
                    Ordering::Less => {
                        lower = candidate;
                        step = step.saturating_mul(2);
                    },
                    Ordering::Equal => return Ok(Some((candidate, candidate))),
                    Ordering::Greater => break (lower, candidate),
                }
            }
        },
        Ordering::Greater => {
            let mut upper = bits;
            let mut step = 1_u64;
            loop {
                work.charge_into_error(1)?;
                if upper == 0 {
                    return Ok(None);
                }
                let candidate = upper.wrapping_sub(step.min(upper));
                match compare(candidate, work)? {
                    Ordering::Greater => {
                        upper = candidate;
                        step = step.saturating_mul(2);
                    },
                    Ordering::Equal => return Ok(Some((candidate, candidate))),
                    Ordering::Less => break (candidate, upper),
                }
            }
        },
    };

    // Exponential search finds a finite enclosing pair without assuming that
    // the floating proposal is close to the exact root. Refine the pair until
    // the returned endpoints are adjacent in binary64's nonnegative bit
    // ordering; unlike a fixed ULP walk, this also covers both subnormal and
    // binade boundaries without an arbitrary retry limit.
    while upper.wrapping_sub(lower) > 1 {
        work.charge_into_error(1)?;
        let middle = lower.wrapping_add(upper.wrapping_sub(lower) / 2);
        match compare(middle, work)? {
            Ordering::Less => lower = middle,
            Ordering::Equal => return Ok(Some((middle, middle))),
            Ordering::Greater => upper = middle,
        }
    }
    Ok(Some((lower, upper)))
}

#[cfg(test)]
fn round_sqrt_ratio_binary(numerator: &ExactDyadic, denominator: &ExactDyadic) -> Result<f64> {
    round_sqrt_ratio_binary_with_work(numerator, denominator, &mut Unmetered)
}

fn round_sqrt_ratio_binary_with_work<W: Work>(
    numerator: &ExactDyadic,
    denominator: &ExactDyadic,
    work: &mut W,
) -> Result<f64> {
    let mut low = 0_u64;
    let mut high = f64::MAX.to_bits();
    while low < high {
        work.charge_into_error(1)?;
        let mid = low.wrapping_add(high.wrapping_sub(low).div_ceil(2));
        let candidate = ExactDyadic::from_f64(f64::from_bits(mid));
        let square = candidate
            .try_product_with_work(&candidate, work)?
            .try_product_with_work(denominator, work)?;
        if square.cmp_with_work(numerator, work)? == Ordering::Greater {
            high = mid.wrapping_sub(1);
        } else {
            low = mid;
        }
    }
    if low == f64::MAX.to_bits() {
        return Ok(f64::MAX);
    }
    let lower = f64::from_bits(low);
    let upper = f64::from_bits(low.wrapping_add(1));
    let midpoint_sum =
        ExactDyadic::from_f64(lower).try_add_with_work(ExactDyadic::from_f64(upper), work)?;
    let midpoint_squared = midpoint_sum
        .try_product_with_work(&midpoint_sum, work)?
        .try_product_with_work(denominator, work)?;
    let four_numerator = numerator.try_product_with_work(&ExactDyadic::from_f64(4.0), work)?;
    Ok(
        match four_numerator.cmp_with_work(&midpoint_squared, work)? {
            Ordering::Less => lower,
            Ordering::Greater => upper,
            Ordering::Equal => {
                if low & 1 == 0 {
                    lower
                } else {
                    upper
                }
            },
        },
    )
}

#[cfg(test)]
mod sqrt_ratio_tests {
    use super::*;

    #[test]
    fn rounds_tie_to_even_without_squared_rounding() {
        let lower = ExactDyadic::from_f64(1.0);
        let upper = ExactDyadic::from_f64(1.0_f64.next_up());
        let midpoint = lower
            .try_add(upper)
            .expect("midpoint sum")
            .try_product(&ExactDyadic::from_f64(0.5))
            .expect("midpoint");
        let squared = midpoint.try_product(&midpoint).expect("squared midpoint");
        assert_eq!(
            round_sqrt_ratio(&squared, &ExactDyadic::from_f64(1.0)).unwrap(),
            1.0
        );
    }

    #[test]
    fn handles_non_dyadic_distance() {
        let squared = ExactDyadic::from_f64(2.0);
        assert_eq!(
            round_sqrt_ratio(&squared, &ExactDyadic::from_f64(1.0)).unwrap(),
            2.0_f64.sqrt()
        );
    }

    #[test]
    fn min_subnormal_sqrt_ratio_uses_an_adjacent_bracket() {
        let u = f64::from_bits(1);
        let exact_u = ExactDyadic::from_f64(u);
        let two = ExactDyadic::from_f64(2.0);
        let two_u = exact_u.try_product(&two).expect("two minimum subnormals");
        let squared = two_u
            .try_product(&two_u)
            .expect("squared two minimum subnormals")
            .try_add(
                exact_u
                    .try_product(&exact_u)
                    .expect("squared minimum subnormal"),
            )
            .expect("sqrt five squared ratio");
        let denominator = ExactDyadic::one();
        let bracket = exact_sqrt_ratio_bracket(&squared, &denominator, u)
            .unwrap()
            .expect("finite root bracket");
        assert_eq!(bracket, (2, 3));
        assert_eq!(
            bracket.1 - bracket.0,
            1,
            "a non-singleton bracket is adjacent"
        );
        assert_eq!(
            round_sqrt_ratio(&squared, &denominator).unwrap().to_bits(),
            2,
            "sqrt(5)u rounds to the even 2u endpoint"
        );
    }

    #[test]
    fn accepting_the_nonadjacent_min_subnormal_bracket_would_change_the_result() {
        let u = ExactDyadic::from_f64(f64::from_bits(1));
        let bad_lower = u.clone();
        let bad_upper = u
            .try_product(&ExactDyadic::from_f64(3.0))
            .expect("three minimum subnormals");
        let exact_squared = u
            .try_product(&u)
            .expect("squared minimum subnormal")
            .try_product(&ExactDyadic::from_f64(5.0))
            .expect("sqrt five squared ratio");
        let midpoint = bad_lower
            .try_add(bad_upper)
            .expect("nonadjacent bracket sum")
            .try_product(&ExactDyadic::from_f64(0.5))
            .expect("nonadjacent bracket midpoint");
        assert_eq!(
            exact_squared
                .try_product(&ExactDyadic::from_f64(4.0))
                .expect("scaled exact ratio")
                .cmp(
                    &midpoint
                        .try_product(&midpoint)
                        .expect("nonadjacent midpoint square")
                ),
            Ordering::Greater,
            "sqrt(5)u lies above the midpoint of the bad [u, 3u] bracket"
        );
        assert_eq!(
            round_sqrt_ratio(&exact_squared, &ExactDyadic::one())
                .unwrap()
                .to_bits(),
            2,
            "the adjacent [2u, 3u] bracket is required for the correct result"
        );
    }

    #[test]
    fn sqrt_brackets_are_adjacent_at_subnormal_and_normal_boundaries() {
        let one = ExactDyadic::one();
        assert_eq!(
            round_sqrt_ratio(&ExactDyadic::zero(), &one)
                .unwrap()
                .to_bits(),
            0
        );
        let square = |value: f64| {
            let value = ExactDyadic::from_f64(value);
            value.try_product(&value).expect("boundary square")
        };
        let assert_bracket = |name: &str, numerator: &ExactDyadic, proposal: f64| {
            let (lower, upper) = exact_sqrt_ratio_bracket(numerator, &one, proposal)
                .unwrap()
                .expect("finite boundary bracket");
            if lower != upper {
                assert_eq!(
                    upper - lower,
                    1,
                    "{name}: non-singleton endpoints must be adjacent"
                );
            }
            let lower_value = ExactDyadic::from_f64(f64::from_bits(lower));
            let lower_squared = lower_value
                .try_product(&lower_value)
                .unwrap()
                .try_product(&one)
                .unwrap();
            assert_ne!(
                lower_squared.cmp(numerator),
                Ordering::Greater,
                "{name}: lower endpoint is above the root"
            );
            let upper_value = ExactDyadic::from_f64(f64::from_bits(upper));
            let upper_squared = upper_value
                .try_product(&upper_value)
                .unwrap()
                .try_product(&one)
                .unwrap();
            assert_ne!(
                upper_squared.cmp(numerator),
                Ordering::Less,
                "{name}: upper endpoint is below the root"
            );
        };

        let minimum_subnormal = f64::from_bits(1);
        let below_minimum = ExactDyadic::from_parts(false, vec![1], -1075);
        assert_bracket(
            "below minimum subnormal",
            &below_minimum.square(),
            minimum_subnormal,
        );
        assert_eq!(
            round_sqrt_ratio(&below_minimum.square(), &one)
                .unwrap()
                .to_bits(),
            0
        );

        let subnormal_midpoint = {
            let lower = ExactDyadic::from_f64(minimum_subnormal);
            let upper = ExactDyadic::from_f64(f64::from_bits(2));
            lower
                .try_add(upper)
                .unwrap()
                .try_product(&ExactDyadic::from_f64(0.5))
                .unwrap()
        };
        assert_bracket(
            "minimum subnormal midpoint",
            &subnormal_midpoint.square(),
            minimum_subnormal,
        );
        assert_eq!(
            round_sqrt_ratio(&subnormal_midpoint.square(), &one)
                .unwrap()
                .to_bits(),
            2,
            "the even upper endpoint wins the 1u/2u tie"
        );

        let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
        let minimum_normal = f64::from_bits(0x0010_0000_0000_0000);
        let normal_boundary_midpoint = ExactDyadic::from_f64(largest_subnormal)
            .try_add(ExactDyadic::from_f64(minimum_normal))
            .unwrap()
            .try_product(&ExactDyadic::from_f64(0.5))
            .unwrap();
        assert_bracket(
            "subnormal/normal midpoint",
            &normal_boundary_midpoint.square(),
            largest_subnormal,
        );
        assert_eq!(
            round_sqrt_ratio(&normal_boundary_midpoint.square(), &one)
                .unwrap()
                .to_bits(),
            minimum_normal.to_bits(),
            "the even minimum-normal endpoint wins the tie"
        );

        let normal_midpoint = ExactDyadic::from_f64(1.0)
            .try_add(ExactDyadic::from_f64(1.0_f64.next_up()))
            .unwrap()
            .try_product(&ExactDyadic::from_f64(0.5))
            .unwrap();
        assert_bracket("normal midpoint", &normal_midpoint.square(), 1.0);
        assert_eq!(
            round_sqrt_ratio(&normal_midpoint.square(), &one)
                .unwrap()
                .to_bits(),
            1.0_f64.to_bits(),
            "the even lower endpoint wins the normal tie"
        );

        assert_eq!(
            round_sqrt_ratio(&square(largest_subnormal), &one)
                .unwrap()
                .to_bits(),
            largest_subnormal.to_bits()
        );
        assert_eq!(
            round_sqrt_ratio(&square(minimum_normal), &one)
                .unwrap()
                .to_bits(),
            minimum_normal.to_bits()
        );
    }

    #[test]
    fn sqrt_ratio_rounding_handles_the_overflow_boundary() {
        let one = ExactDyadic::one();
        let maximum = ExactDyadic::from_f64(f64::MAX);
        let maximum_squared = maximum.square();
        assert_eq!(
            round_sqrt_ratio(&maximum_squared, &one).unwrap().to_bits(),
            f64::MAX.to_bits()
        );

        let below_overflow = maximum
            .clone()
            .add(ExactDyadic::from_parts(false, vec![1], 969));
        assert_eq!(
            round_sqrt_ratio(&below_overflow.square(), &one)
                .unwrap()
                .to_bits(),
            f64::MAX.to_bits()
        );

        let overflow_midpoint = maximum.add(ExactDyadic::from_parts(false, vec![1], 970));
        assert!(
            round_sqrt_ratio(&overflow_midpoint.square(), &one)
                .unwrap()
                .is_infinite(),
            "the exact overflow midpoint rounds to infinity"
        );
    }

    #[test]
    fn sqrt_bracketing_has_no_proposal_distance_limit() {
        let numerator = ExactDyadic::from_f64(2.0);
        let denominator = ExactDyadic::one();
        for proposal in [0.0, f64::MAX] {
            let (lower, upper) = exact_sqrt_ratio_bracket(&numerator, &denominator, proposal)
                .unwrap()
                .expect("finite root bracket");
            assert_eq!(
                upper - lower,
                1,
                "proposal {proposal:?} must still produce adjacent endpoints"
            );
            assert_ne!(
                ExactDyadic::from_f64(f64::from_bits(lower))
                    .square()
                    .cmp(&numerator),
                Ordering::Greater
            );
            assert_ne!(
                ExactDyadic::from_f64(f64::from_bits(upper))
                    .square()
                    .cmp(&numerator),
                Ordering::Less
            );
        }
    }

    #[test]
    fn proposed_sqrt_bracket_matches_the_complete_binary_reference() {
        for (numerator, denominator) in [
            (1.0, 1.0),
            (2.0, 1.0),
            (3.0, 1.0),
            (1.0, 3.0),
            (f64::from_bits(1), 1.0),
            (f64::MAX, 1.0),
            (f64::MAX, 3.0),
        ] {
            let numerator = ExactDyadic::from_f64(numerator);
            let denominator = ExactDyadic::from_f64(denominator);
            assert_eq!(
                round_sqrt_ratio(&numerator, &denominator)
                    .unwrap()
                    .to_bits(),
                round_sqrt_ratio_binary(&numerator, &denominator)
                    .unwrap()
                    .to_bits()
            );
        }
    }
}

#[cfg(test)]
mod tests;
