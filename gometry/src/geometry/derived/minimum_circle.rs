use std::cmp::Ordering;
use std::num::NonZeroU32;

use super::polylabel::exact_difference;
use crate::error::Result;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::tessellation::exact::{self, ExactDyadic, Interval};
use crate::geometry::{
    CoordSeq, Coordinates as _, ExpansionBudget, Point, Polygon, Ring, Shape, XY,
};

mod publication;
pub(crate) use self::publication::emit_minimum_bounding_circle;
pub(super) use self::publication::next_realization_radius;
#[cfg(test)]
use self::publication::{certify_minimum_circle_polygon, tangent_circle_polygon};
#[cfg(test)]
mod tests;

/// The support of a minimum enclosing circle.  A candidate never stores a
/// rounded center or radius: those values are derived only after the support
/// has passed the independent exact certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CircleSupport {
    One(usize),
    Two(usize, usize),
    Three(usize, usize, usize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MinimumCircle {
    support: CircleSupport,
}

/// A source-certified disk retained in exact homogeneous form until it is
/// emitted.
///
/// `center_x / center_w` and `center_y / center_w` are the exact center, and
/// `radius_squared_numerator / radius_squared_denominator` is the exact
/// squared radius. The source spelling is retained for the zero-radius
/// publication; positive center and radius values are rounded only at their
/// respective publication boundaries. No predicate below derives the disk
/// from either one.
#[derive(Debug)]
pub(crate) struct CertifiedDisk {
    center_x: ExactDyadic,
    center_y: ExactDyadic,
    center_w: ExactDyadic,
    radius_squared_numerator: ExactDyadic,
    radius_squared_denominator: ExactDyadic,
    source_center: Option<Point>,
    exact_zero_radius: bool,
}

impl MinimumCircle {
    #[expect(
        clippy::indexing_slicing,
        reason = "support ordinals are created by the enclosing-circle solver from this points slice"
    )]
    const fn support_xy(self, points: &[Point]) -> ([XY; 3], usize) {
        match self.support {
            CircleSupport::One(a) => ([points[a].xy(), XY::new(0.0, 0.0), XY::new(0.0, 0.0)], 1),
            CircleSupport::Two(a, b) => ([points[a].xy(), points[b].xy(), XY::new(0.0, 0.0)], 2),
            CircleSupport::Three(a, b, c) => ([points[a].xy(), points[b].xy(), points[c].xy()], 3),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the support and queried ordinals are created from this admitted points slice"
    )]
    fn contains(self, points: &[Point], point: usize) -> Result<bool> {
        let (support, count) = self.support_xy(points);
        exact::try_circle_contains(&support[..count], points[point].xy())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "candidate pair ordinals are source sites in this points slice"
    )]
    fn widest_pair(points: &[Point], a: usize, b: usize, c: usize) -> Result<Self> {
        let mut pairs = [(a, b), (a, c), (b, c)].into_iter();
        let mut widest = pairs.next().expect("three support sites have a pair");
        for (left, right) in pairs {
            if exact::try_squared_distance_cmp(
                points[left].xy(),
                points[right].xy(),
                points[widest.0].xy(),
                points[widest.1].xy(),
            )?
            .is_gt()
            {
                widest = (left, right);
            }
        }
        Ok(Self {
            support: CircleSupport::Two(widest.0, widest.1),
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the support slice length is derived from the CircleSupport variant"
    )]
    fn certificate(self, points: &[Point]) -> Result<bool> {
        let (support, count) = self.support_xy(points);
        exact::support_circle::try_certify_support(
            &support[..count],
            points.iter().copied().map(|point| point.xy()),
        )
    }

    /// Materialize the exact disk only after the enclosing-support certificate
    /// succeeds. This is the sole path from a support candidate to a disk.
    #[expect(
        clippy::indexing_slicing,
        reason = "support ordinals are created by the enclosing-circle solver from this points slice"
    )]
    pub(crate) fn certified_disk(self, points: &[Point]) -> Result<CertifiedDisk> {
        if !self.certificate(points)? {
            return Err(crate::geometry::GeometryErrorKind::Invalid(
                "minimum enclosing circle support certificate failed".into(),
            )
            .into());
        }
        let (support, count) = self.support_xy(points);
        let (
            mut center_x,
            mut center_y,
            mut center_w,
            radius_squared_numerator,
            radius_squared_denominator,
        ) = exact::support_circle::try_support_circle_components(&support[..count])?;
        // A positive homogeneous scale makes the edge half-plane formula
        // below match the geometric CCW sign directly.
        if center_w.sign() == ExactSign::Negative {
            center_x = center_x.neg();
            center_y = center_y.neg();
            center_w = center_w.neg();
        }
        if center_w.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        // Preserve the source spelling for the zero-radius case. Exact
        // mathematical zero has no sign, while the derived point is still a
        // source point and therefore retains its stored coordinate bits.
        let source_center = if let CircleSupport::One(index) = self.support {
            Some(Point::new_raw_xy(points[index].x, points[index].y))
        } else {
            None
        };
        let exact_zero_radius = radius_squared_numerator.is_zero();
        Ok(CertifiedDisk {
            center_x,
            center_y,
            center_w,
            radius_squared_numerator,
            radius_squared_denominator,
            source_center,
            exact_zero_radius,
        })
    }
}

impl CertifiedDisk {
    pub(super) fn exact_values(
        &self,
    ) -> Result<(
        crate::geometry::ExactRational,
        crate::geometry::ExactRational,
        crate::geometry::ExactRational,
    )> {
        use crate::geometry::ExactRational;
        Ok((
            ExactRational::new(self.center_x.clone(), self.center_w.clone())?,
            ExactRational::new(self.center_y.clone(), self.center_w.clone())?,
            ExactRational::new(
                self.radius_squared_numerator.clone(),
                self.radius_squared_denominator.clone(),
            )?,
        ))
    }

    fn enclosure(&self) -> Result<Option<DiskEnclosure>> {
        // A finite disk need not have a finite squared-radius enclosure.
        let largest_squared =
            ExactDyadic::from_f64(f64::MAX).try_product(&self.radius_squared_denominator)?;
        if self.radius_squared_numerator.cmp(&largest_squared) == Ordering::Greater {
            return Ok(None);
        }
        let interval = |numerator: &ExactDyadic, denominator: &ExactDyadic| {
            exact::outward_ratio_bounds(numerator, denominator).map(|(lo, hi)| Interval { lo, hi })
        };
        let enclosure = DiskEnclosure {
            x: interval(&self.center_x, &self.center_w)?,
            y: interval(&self.center_y, &self.center_w)?,
            squared_radius: interval(
                &self.radius_squared_numerator,
                &self.radius_squared_denominator,
            )?,
        };
        Ok((enclosure.x.is_finite()
            && enclosure.y.is_finite()
            && enclosure.squared_radius.is_finite())
        .then_some(enclosure))
    }

    pub(crate) fn center(&self) -> Result<Point> {
        self.source_center.map_or_else(
            || {
                Ok(Point::new_raw_xy(
                    exact::round_ratio(&self.center_x, &self.center_w)?,
                    exact::round_ratio(&self.center_y, &self.center_w)?,
                ))
            },
            Ok,
        )
    }

    pub(crate) fn radius(&self) -> Result<f64> {
        self.radius_in_scale(1.0)
    }

    pub(crate) const fn is_exact_zero(&self) -> bool {
        self.exact_zero_radius
    }

    /// Round the exact radius after applying one represented planar unit
    /// scale. Scaling the squared-radius numerator first avoids the
    /// rounded-native-radius-then-scale double rounding route.
    pub(crate) fn radius_in_scale(&self, to_metre: f64) -> Result<f64> {
        let scale = ExactDyadic::from_f64(to_metre);
        let scaled_numerator = self
            .radius_squared_numerator
            .try_product(&scale)?
            .try_product(&scale)?;
        exact::round_sqrt_ratio(&scaled_numerator, &self.radius_squared_denominator)
    }

    /// Whether a binary64 vertex is strictly outside this exact disk.
    fn vertex_is_strictly_outside(&self, point: XY) -> Result<bool> {
        let point_x = ExactDyadic::from_f64(point.x);
        let point_y = ExactDyadic::from_f64(point.y);
        let dx = self
            .center_x
            .clone()
            .try_subtract(point_x.try_product(&self.center_w)?)?;
        let dy = self
            .center_y
            .clone()
            .try_subtract(point_y.try_product(&self.center_w)?)?;
        let distance_squared = dx
            .try_product(&dx)?
            .try_add(dy.try_product(&dy)?)?
            .try_product(&self.radius_squared_denominator)?;
        let center_w_squared = self.center_w.try_product(&self.center_w)?;
        let radius_squared = self
            .radius_squared_numerator
            .try_product(&center_w_squared)?;
        Ok(distance_squared.cmp(&radius_squared) == Ordering::Greater)
    }

    /// Whether a CCW binary64 edge has the exact disk on its left at a
    /// distance at least the exact radius.
    ///
    /// With `center = (N_x/W, N_y/W)` and `r² = S/T`, let
    /// `H = cross(b-a, N-W*a)`. The line is a valid disk half-plane exactly
    /// when `H >= 0` and `T*H² >= S*W²*|b-a|²`.
    fn ccw_edge_encloses(&self, left: XY, right: XY) -> Result<bool> {
        let edge_x = exact_difference(right.x, left.x)?;
        let edge_y = exact_difference(right.y, left.y)?;
        let left_x = ExactDyadic::from_f64(left.x);
        let left_y = ExactDyadic::from_f64(left.y);
        let center_offset_x = self
            .center_x
            .clone()
            .try_subtract(left_x.try_product(&self.center_w)?)?;
        let center_offset_y = self
            .center_y
            .clone()
            .try_subtract(left_y.try_product(&self.center_w)?)?;
        let h = edge_x
            .try_product(&center_offset_y)?
            .try_subtract(edge_y.try_product(&center_offset_x)?)?;
        if h.sign() == ExactSign::Negative {
            return Ok(false);
        }
        let edge_squared = edge_x
            .try_product(&edge_x)?
            .try_add(edge_y.try_product(&edge_y)?)?;
        let left_side = self
            .radius_squared_denominator
            .try_product(&h.try_product(&h)?)?;
        let center_w_squared = self.center_w.try_product(&self.center_w)?;
        let right_side = self
            .radius_squared_numerator
            .try_product(&center_w_squared)?
            .try_product(&edge_squared)?;
        Ok(left_side.cmp(&right_side) != Ordering::Less)
    }
}

/// Outward enclosures of the source-certified disk, not of the emitted ring.
/// An inconclusive comparison still reaches the homogeneous exact certificate.
struct DiskEnclosure {
    x: Interval,
    y: Interval,
    squared_radius: Interval,
}

impl DiskEnclosure {
    fn proves_vertex_outside(&self, point: XY) -> bool {
        let dx = Interval::exact(point.x).sub(self.x);
        let dy = Interval::exact(point.y).sub(self.y);
        if !dx.is_finite() || !dy.is_finite() {
            return false;
        }
        let distance = dx.mul(dx).add(dy.mul(dy));
        distance.is_finite() && distance.lo > self.squared_radius.hi
    }

    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "the lower bound for cross squared must be at least the upper bound for radius squared times edge norm"
    )]
    fn proves_ccw_edge_encloses(&self, left: XY, right: XY) -> bool {
        let dx = Interval::exact(right.x).sub(Interval::exact(left.x));
        let dy = Interval::exact(right.y).sub(Interval::exact(left.y));
        let cx = self.x.sub(Interval::exact(left.x));
        let cy = self.y.sub(Interval::exact(left.y));
        if !dx.is_finite() || !dy.is_finite() || !cx.is_finite() || !cy.is_finite() {
            return false;
        }
        let cross = dx.mul(cy).sub(dy.mul(cx));
        let norm = dx.mul(dx).add(dy.mul(dy));
        if !cross.is_finite() || !norm.is_finite() || cross.lo < 0.0 {
            return false;
        }
        let left_side = cross.mul(cross);
        let right_side = self.squared_radius.mul(norm);
        left_side.is_finite() && right_side.is_finite() && left_side.lo >= right_side.hi
    }
}

/// Solver step counts inspected by unit tests; zero-sized outside them.
#[derive(Debug, Default)]
struct CircleWork {
    #[cfg(test)]
    contains: usize,
    #[cfg(test)]
    one_supports: usize,
    #[cfg(test)]
    two_supports: usize,
    #[cfg(test)]
    three_supports: usize,
    #[cfg(test)]
    collinear_fallbacks: usize,
}

#[cfg_attr(
    not(test),
    expect(
        clippy::needless_pass_by_ref_mut,
        reason = "the counter mutates in test builds and is intentionally zero-sized in production"
    )
)]
#[cfg_attr(
    test,
    expect(
        clippy::arithmetic_side_effects,
        reason = "test-only solver work counters are incremented by fixed, small regression fixtures"
    )
)]
impl CircleWork {
    const fn count_contains(&mut self) {
        #[cfg(test)]
        {
            self.contains += 1;
        }
    }

    const fn count_one_support(&mut self) {
        #[cfg(test)]
        {
            self.one_supports += 1;
        }
    }

    const fn count_two_support(&mut self) {
        #[cfg(test)]
        {
            self.two_supports += 1;
        }
    }

    const fn count_three_support(&mut self) {
        #[cfg(test)]
        {
            self.three_supports += 1;
        }
    }

    const fn count_collinear_fallback(&mut self) {
        #[cfg(test)]
        {
            self.collinear_fallbacks += 1;
        }
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "the order is a permutation of the non-empty source point ordinals"
)]
fn solve_enclosing_circle_order(
    points: &[Point],
    order: &[usize],
    work: &mut CircleWork,
) -> Result<MinimumCircle> {
    let mut circle = MinimumCircle::new_one(order[0]);
    work.count_one_support();
    for i in 1..order.len() {
        work.count_contains();
        if circle.contains(points, order[i])? {
            continue;
        }
        circle = MinimumCircle::new_one(order[i]);
        work.count_one_support();
        for j in 0..i {
            work.count_contains();
            if circle.contains(points, order[j])? {
                continue;
            }
            circle = MinimumCircle::new_two(order[i], order[j]);
            work.count_two_support();
            for k in 0..j {
                work.count_contains();
                if circle.contains(points, order[k])? {
                    continue;
                }
                let a = order[i];
                let b = order[j];
                let c = order[k];
                circle = if exact::orient2d(points[a].xy(), points[b].xy(), points[c].xy())
                    == ExactSign::Zero
                {
                    work.count_collinear_fallback();
                    MinimumCircle::widest_pair(points, a, b, c)?
                } else {
                    work.count_three_support();
                    MinimumCircle::new_three(a, b, c)
                };
            }
        }
    }
    Ok(circle)
}

/// Welzl's smallest enclosing circle in an index-only support representation.
///
/// The move-to-front loop has expected linear work after a process-keyed
/// Fisher-Yates/LCG order.  Every containment decision is exact in the stored
/// binary64 source coordinates, so the result is independent of the input
/// order and is not based on a normalized or rounded candidate. The returned
/// support becomes a published disk only through `certified_disk`, which
/// performs the independent all-source certificate.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the Fisher-Yates upper bound iterates from order length down through two"
)]
pub(crate) fn smallest_enclosing_circle(points: &[Point]) -> Result<MinimumCircle> {
    if points.is_empty() {
        return Err(crate::geometry::GeometryErrorKind::Invalid(
            "minimum enclosing circle requires a non-empty source".into(),
        )
        .into());
    }
    if points
        .iter()
        .any(|point| !point.x.is_finite() || !point.y.is_finite())
    {
        return Err(crate::geometry::GeometryErrorKind::NonFiniteCoordinate.into());
    }

    let hash_state = crate::collections::collection_hash_state()?;
    let mut state = hash_state.hash_one((points.len(), "minimum_enclosing_circle"));
    let mut order = Vec::with_capacity(points.len());
    order.extend(0..points.len());
    for (index, upper) in (1..order.len()).rev().zip((2..=order.len()).rev()) {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let other = (state >> 33) as usize % upper;
        order.swap(index, other);
    }

    solve_enclosing_circle_order(points, &order, &mut CircleWork::default())
}

impl MinimumCircle {
    const fn new_one(index: usize) -> Self {
        Self {
            support: CircleSupport::One(index),
        }
    }

    const fn new_two(left: usize, right: usize) -> Self {
        Self {
            support: CircleSupport::Two(left, right),
        }
    }

    const fn new_three(a: usize, b: usize, c: usize) -> Self {
        Self {
            support: CircleSupport::Three(a, b, c),
        }
    }
}
