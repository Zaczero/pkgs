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

use crate::error::ResourceError;
use crate::geometry::embedding_search::{Unmetered, Work};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::stored_orientation_sign;
use crate::geometry::{GeometryErrorKind, Result, XY};

mod dyadic;
pub(in crate::geometry::tessellation) mod frame;
pub(in crate::geometry) mod incidence;
pub(in crate::geometry) mod point;
mod predicates;
pub(in crate::geometry) mod support_circle;

pub(in crate::geometry) use dyadic::*;
use incidence::line_intersection;
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
    #[cfg(test)]
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

/// Exact orientation over source or generated chart coordinates retained as
/// dyadics, without first rounding retained native-plus-lift locations to XY.
pub(in crate::geometry) fn orient2d_dyadic_points(
    [ax, ay]: [ExactDyadic; 2],
    [bx, by]: [ExactDyadic; 2],
    [cx, cy]: [ExactDyadic; 2],
) -> Result<ExactSign> {
    let abx = bx.try_subtract(ax.clone())?;
    let aby = by.try_subtract(ay.clone())?;
    let acx = cx.try_subtract(ax)?;
    let acy = cy.try_subtract(ay)?;
    abx.try_product(&acy)?
        .try_subtract(aby.try_product(&acx)?)
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

/// Development self-check: prove `lo <= numerator/denominator <= hi` with the
/// exact comparator when interval verification is enabled.
const VERIFY_INTERVALS: bool = false;

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
