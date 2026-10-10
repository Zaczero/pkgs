use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::num::NonZeroU32;

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
#[cfg(test)]
use crate::geometry::tessellation::exact::compare_ratios;
use crate::geometry::tessellation::exact::{
    self, ExactDyadic, ExactRational as Rational, Interval,
};
use crate::geometry::{
    AxisFrame, BVH_MIN_INDEXED_SEGMENTS, Bounds, CoordSeq, CoordSeqBuilder, Coordinates as _,
    ExpansionBudget, FacetBvh, Orientation, Point, Polygon, PreparedLinework, PreparedSupport,
    Ring, Segment, Shape, XY, orientation, predicates,
};

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
        exact::try_certify_support(
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
        ) = exact::try_support_circle_components(&support[..count])?;
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

fn unrepresentable_minimum_circle() -> crate::error::Error {
    crate::geometry::GeometryErrorKind::unrepresentable_output("minimum_bounding_circle")
}

fn circle_emission_error(error: crate::error::Error) -> crate::error::Error {
    if matches!(
        error.kind(),
        crate::error::ErrorKind::Resource(_)
            | crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge { .. }
            )
    ) {
        error
    } else {
        unrepresentable_minimum_circle()
    }
}

fn certify_minimum_circle_polygon(
    output: &Shape,
    disk: &CertifiedDisk,
    quadrant_segments: NonZeroU32,
) -> Result<()> {
    let Shape::Polygon(polygon) = output else {
        return Err(unrepresentable_minimum_circle());
    };
    let mut rings = polygon.rings();
    let Some(shell) = rings.next() else {
        return Err(unrepresentable_minimum_circle());
    };
    if rings.next().is_some() {
        return Err(unrepresentable_minimum_circle());
    }
    let steps = (quadrant_segments.get() as usize)
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let expected_length = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    if shell.coords().len() != expected_length {
        return Err(unrepresentable_minimum_circle());
    }

    let mut vertices = Vec::with_capacity(expected_length);
    for point in shell.coords().iter_coords() {
        if !point.x.is_finite() || !point.y.is_finite() {
            return Err(unrepresentable_minimum_circle());
        }
        vertices.push(point.xy());
    }
    let Some((last, open)) = vertices.split_last() else {
        return Err(unrepresentable_minimum_circle());
    };
    let [first, middle @ .., last_open] = open else {
        return Err(unrepresentable_minimum_circle());
    };
    let same_coordinate = |left: f64, right: f64| {
        crate::geometry::canonical_f64_bits(left) == crate::geometry::canonical_f64_bits(right)
    };
    if !same_coordinate(first.x, last.x) || !same_coordinate(first.y, last.y) {
        return Err(unrepresentable_minimum_circle());
    }
    if middle.is_empty() {
        return Err(unrepresentable_minimum_circle());
    }
    for (current, next) in open
        .iter()
        .copied()
        .zip(open.iter().copied().cycle().skip(1))
    {
        if same_coordinate(current.x, next.x) && same_coordinate(current.y, next.y) {
            return Err(unrepresentable_minimum_circle());
        }
    }

    let mut direction = None;
    let mut previous = *last_open;
    let next_points = open.iter().skip(1).chain(std::iter::once(first));
    for (&current, &next) in open.iter().zip(next_points) {
        let turn = exact::orient2d(previous, current, next);
        if turn == ExactSign::Zero {
            return Err(unrepresentable_minimum_circle());
        }
        if let Some(expected) = direction {
            if turn != expected {
                return Err(unrepresentable_minimum_circle());
            }
        } else {
            direction = Some(turn);
        }
        previous = current;
    }
    let direction = direction.expect("three or more open circle vertices");
    if direction != ExactSign::Positive {
        return Err(unrepresentable_minimum_circle());
    }

    let enclosure = disk.enclosure().map_err(circle_emission_error)?;
    for (left, right) in open
        .iter()
        .copied()
        .zip(open.iter().copied().cycle().skip(1))
    {
        if (!enclosure
            .as_ref()
            .is_some_and(|enclosure| enclosure.proves_vertex_outside(left))
            && !disk
                .vertex_is_strictly_outside(left)
                .map_err(circle_emission_error)?)
            || (!enclosure
                .as_ref()
                .is_some_and(|enclosure| enclosure.proves_ccw_edge_encloses(left, right))
                && !disk
                    .ccw_edge_encloses(left, right)
                    .map_err(circle_emission_error)?)
        {
            return Err(unrepresentable_minimum_circle());
        }
    }

    if output.validate().map_err(circle_emission_error)?.is_some() {
        return Err(unrepresentable_minimum_circle());
    }
    Ok(())
}

/// Build the regular tangent-intersection polygon for one candidate radius.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "quadrants is nonzero and steps is checked as four times quadrants, bounding each step division and offset"
)]
fn tangent_circle_polygon(
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let quadrants = quadrant_segments.get() as usize;
    let steps = quadrants
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let vertices = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.add(vertices)?;

    // Each edge is tangent to the candidate circle. Its two adjacent
    // intersections are at half a step on either side of the edge normal,
    // so the relative coordinate factors are
    // `(cos(theta) / cos_half, sin(theta) / cos_half)`.
    let half_angle = std::f64::consts::PI / steps as f64;
    let (sin_half, cos_half) = half_angle.sin_cos();
    if !cos_half.is_finite() || cos_half <= 0.0 {
        return Err(unrepresentable_minimum_circle());
    }
    let tangent_half = sin_half / cos_half;
    if !tangent_half.is_finite() {
        return Err(unrepresentable_minimum_circle());
    }
    // Trigonometric construction is rounded before the exact certificate
    // sees the edge. Two representable successors provide a finite outward
    // construction margin without an epsilon predicate or segment inflation.
    let outward_component = |factor: f64| {
        if factor > 0.0 {
            factor.next_up().next_up().min(1.0)
        } else if factor < 0.0 {
            factor.next_down().next_down().max(-1.0)
        } else {
            factor
        }
    };

    let mut points = Vec::with_capacity(vertices);
    for step in 0..steps {
        let quadrant = step / quadrants;
        let local_step = step % quadrants;
        let (factor_x, factor_y) = if local_step == 0 || local_step + 1 == quadrants {
            // Reuse the half-angle value at both ends of each quadrant.
            // This makes factors that are mathematically one exactly one,
            // which matters when `radius == f64::MAX`.
            match (quadrant, local_step == 0) {
                (0, true) => (1.0, tangent_half),
                (0, false) => (tangent_half, 1.0),
                (1, true) => (-tangent_half, 1.0),
                (1, false) => (-1.0, tangent_half),
                (2, true) => (-1.0, -tangent_half),
                (2, false) => (-tangent_half, -1.0),
                (3, true) => (tangent_half, -1.0),
                (3, false) => (1.0, -tangent_half),
                _ => unreachable!("four quadrants"),
            }
        } else {
            let angle = std::f64::consts::TAU * (step as f64 + 0.5) / steps as f64;
            let (sin, cos) = angle.sin_cos();
            (cos / cos_half, sin / cos_half)
        };
        let x = center.x + radius * outward_component(factor_x);
        let y = center.y + radius * outward_component(factor_y);
        if !x.is_finite() || !y.is_finite() {
            return Err(unrepresentable_minimum_circle());
        }
        points.push(Point::new_raw_xy(x, y));
    }
    let first = points
        .first()
        .copied()
        .ok_or_else(unrepresentable_minimum_circle)?;
    points.push(first);
    let seq = CoordSeq::try_from_points(&points)?;
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(seq)?,
        Vec::new(),
    )))
}

/// Emit a regular tangent-intersection polygon at a candidate disk radius,
/// then certify the stored output before publishing it. Every retry charges
/// the existing operation-wide generated-work budget; the requested segment
/// count never changes. The loop ends only when the next radius is
/// non-finite or that declared resource bound is reached.
/// Retries that move a realized circle's radius one ulp at a time before the
/// adjustment starts doubling.
const ULP_REALIZATION_RETRIES: u32 = 64;

/// The next radius to try when realizing a circle of `exact` radius as a ring
/// failed at `candidate`: outward for a circumscribed ring, inward for an
/// inscribed one. The first retries move one ulp; after that the offset from
/// `exact` doubles, so a circle far below the coordinate spacing at its center
/// reaches a representable ring in logarithmically many retries.
fn next_realization_radius(exact: f64, candidate: f64, retries: u32, outward: bool) -> f64 {
    match (outward, retries < ULP_REALIZATION_RETRIES) {
        (true, true) => candidate.next_up(),
        (false, true) => candidate.next_down(),
        (true, false) => (exact + 2.0 * (candidate - exact)).max(candidate.next_up()),
        (false, false) => (exact - 2.0 * (exact - candidate)).min(candidate.next_down()),
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "each realization retry charges at least five items against the 16-million item budget"
)]
pub(crate) fn emit_minimum_bounding_circle(
    disk: &CertifiedDisk,
    quadrant_segments: NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let radius = disk.radius().map_err(circle_emission_error)?;
    let center = disk.center().map_err(circle_emission_error)?;
    if !center.x.is_finite() || !center.y.is_finite() || !radius.is_finite() || radius <= 0.0 {
        return Err(unrepresentable_minimum_circle());
    }
    // `tangent_circle_polygon` commits one complete fixed-size ring before
    // allocating it, and the operation's generated-item budget charges each
    // retry.
    // Both the center and translated vertices are rounded at the coordinate
    // scale. Cover that scale before the certificate; radius-only ulp retries
    // can otherwise repeat the same realized ring dozens of times.
    let coordinate_scale = center.x.abs().max(center.y.abs()) + radius;
    #[expect(
        clippy::float_cmp,
        reason = "f64::MAX has no finite successor, so the initial margin uses its finite predecessor spacing"
    )]
    let coordinate_spacing = if coordinate_scale == f64::MAX {
        coordinate_scale - coordinate_scale.next_down()
    } else {
        coordinate_scale.next_up() - coordinate_scale
    };
    let mut candidate_radius = (radius + 8.0 * coordinate_spacing)
        .max(radius.next_up())
        .min(f64::MAX);
    let mut retries = 0_u32;
    loop {
        let output =
            match tangent_circle_polygon(center, candidate_radius, quadrant_segments, budget) {
                Ok(output) => output,
                Err(error) => return Err(circle_emission_error(error)),
            };
        match certify_minimum_circle_polygon(&output, disk, quadrant_segments) {
            Ok(()) => return Ok(output),
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                return Err(error);
            },
            Err(_) => {},
        }
        retries += 1;
        // Rounding the center and each translated vertex costs coordinate
        // ulps, which can dwarf a radius ulp on a small translated disk.
        // Cover that construction scale before retrying the exact certificate.
        let coordinate_scale = center.x.abs().max(center.y.abs()) + candidate_radius;
        #[expect(
            clippy::float_cmp,
            reason = "f64::MAX has no finite successor, so a retry uses its finite predecessor spacing"
        )]
        let coordinate_spacing = if coordinate_scale == f64::MAX {
            coordinate_scale - coordinate_scale.next_down()
        } else {
            coordinate_scale.next_up() - coordinate_scale
        };
        let next = next_realization_radius(radius, candidate_radius, retries, true)
            .max(candidate_radius + 4.0 * coordinate_spacing);
        if !next.is_finite() {
            break;
        }
        candidate_radius = next;
    }
    Err(unrepresentable_minimum_circle())
}

pub(crate) fn polylabel_point(shape: &Shape, tolerance: Option<f64>) -> Result<Point> {
    let Some(bounds) = shape.bounds() else {
        return Ok(Point::new_raw_xy(0.0, 0.0));
    };
    if !has_interior(shape) {
        return Ok(degenerate_polygon_center(bounds));
    }
    let tolerance = tolerance.unwrap_or_else(|| scale_aware_default_tolerance(shape));
    let center = degenerate_polygon_center(bounds);
    let hx = bounds.maxx() / 2.0 - bounds.minx() / 2.0;
    let hy = bounds.maxy() / 2.0 - bounds.miny() / 2.0;
    // Ordinary coordinates use the existing squared SIMD distance kernel
    // directly. Only arithmetic outside its range needs a local-frame copy.
    let magnitude = bounds
        .minx()
        .abs()
        .max(bounds.maxx().abs())
        .max(bounds.miny().abs())
        .max(bounds.maxy().abs());
    if (magnitude * magnitude).is_finite() && hx * hx > 0.0 && hy * hy > 0.0 {
        return polylabel_search(shape, bounds, None, tolerance);
    }
    let Some(frame) = AxisFrame::from_origin_extents(
        center,
        bounds.minx().abs().max(bounds.maxx().abs()).max(hx.abs()),
        bounds.miny().abs().max(bounds.maxy().abs()).max(hy.abs()),
    ) else {
        return Ok(center);
    };
    let normalized = shape.try_map_coordseqs(
        |seq| {
            let mut builder = CoordSeqBuilder::with_capacity(seq.axes(), seq.len());
            for point in seq.iter_coords() {
                let xy = frame.frame_xy(point.x, point.y);
                builder.push(point.with_xy(xy.x, xy.y)?);
            }
            builder.finish()
        },
        |point| {
            let xy = frame.frame_xy(point.x, point.y);
            point.with_xy(xy.x, xy.y)
        },
    )?;
    let normalized_bounds = normalized.bounds().expect("nonempty framed polygon");
    let point = polylabel_search(&normalized, normalized_bounds, Some(frame), tolerance)?;
    let world = frame.unframe_xy(point.xy());
    Ok(Point::new_raw_xy(world.x, world.y))
}

#[derive(Clone, Copy)]
struct LabelCell {
    center: Point,
    half_x: f64,
    half_y: f64,
    distance: f64,
    upper: f64,
}

impl PartialEq for LabelCell {
    fn eq(&self, other: &Self) -> bool {
        self.upper.to_bits() == other.upper.to_bits()
    }
}
impl Eq for LabelCell {}
impl Ord for LabelCell {
    fn cmp(&self, other: &Self) -> Ordering {
        self.upper.total_cmp(&other.upper)
    }
}
impl PartialOrd for LabelCell {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct PolylabelField<'a> {
    shape: &'a Shape,
    linework: PreparedLinework,
    bvh: Option<FacetBvh>,
    stack: Vec<u32>,
    frame: Option<AxisFrame>,
    upper: f64,
}

impl PolylabelField<'_> {
    fn metric(&self, x: f64, y: f64) -> f64 {
        self.frame.map_or_else(
            || x.hypot(y),
            |frame| (x / frame.scale_x()).hypot(y / frame.scale_y()),
        )
    }

    fn boundary_distance(&mut self, point: Point, bound: f64) -> Result<f64> {
        if let Some(frame) = self.frame {
            let mut best = f64::INFINITY;
            self.linework.for_each_segment(|segment| {
                best = best.min(framed_metric_segment_distance(frame, point, segment));
            });
            return Ok(best);
        }
        let squared = bound * bound;
        let squared = if bound > 0.0 && squared == 0.0 {
            f64::INFINITY
        } else {
            squared
        };
        Ok(match &self.bvh {
            Some(bvh) => bvh.min_point_distance_with_stack::<true>(
                &self.linework,
                point.x,
                point.y,
                &mut self.stack,
                squared,
            )?,
            None => self
                .linework
                .min_points_distance::<true>(std::iter::once((point.x, point.y)), squared)?,
        }
        .sqrt())
    }

    fn cell(
        &mut self,
        center: Point,
        half_x: f64,
        half_y: f64,
        parent: Option<&LabelCell>,
    ) -> Result<LabelCell> {
        let (bound, sign) = parent.map_or((f64::INFINITY, None), |parent| {
            let gap = self.metric(center.x - parent.center.x, center.y - parent.center.y);
            (
                (parent.distance.abs() + gap) * (1.0 + 1e-9),
                (parent.distance.abs() > gap).then_some(parent.distance > 0.0),
            )
        });
        let distance = self.boundary_distance(center, bound)?;
        let inside = sign.unwrap_or_else(|| self.shape.contains_point(center));
        let distance = if inside { distance } else { -distance };
        Ok(LabelCell {
            center,
            half_x,
            half_y,
            distance,
            upper: (distance + self.metric(half_x, half_y)).min(self.upper),
        })
    }
}

fn polylabel_search(
    shape: &Shape,
    bounds: Bounds,
    frame: Option<AxisFrame>,
    tolerance: f64,
) -> Result<Point> {
    let center = degenerate_polygon_center(bounds);
    // Midpoint rounding can make the two sides differ by an ulp. Cover
    // both extrema rather than assuming a perfectly centered rectangle.
    let half_x = (center.x - bounds.minx())
        .max(bounds.maxx() - center.x)
        .next_up();
    let half_y = (center.y - bounds.miny())
        .max(bounds.maxy() - center.y)
        .next_up();
    let world_x = frame.map_or(half_x, |frame| half_x / frame.scale_x());
    let world_y = frame.map_or(half_y, |frame| half_y / frame.scale_y());
    let upper = world_x.min(world_y);
    let linework = PreparedLinework::build(shape)?;
    let bvh = (frame.is_none() && linework.segment_count() >= BVH_MIN_INDEXED_SEGMENTS)
        .then(|| FacetBvh::build(&linework))
        .flatten();
    let mut field = PolylabelField {
        shape,
        linework,
        bvh,
        stack: Vec::new(),
        frame,
        upper,
    };
    let mut best = field.cell(representative_point(shape, bounds)?, 0.0, 0.0, None)?;
    let root = field.cell(center, half_x, half_y, None)?;
    if root.distance > best.distance {
        best = root;
    }
    if upper - best.distance <= tolerance {
        return Ok(best.center);
    }
    if best.distance <= 0.0 {
        let mut budget = ExpansionBudget::new("polylabel", "tolerance");
        if PreparedSupport::from_shape_with_boundary(shape, &mut budget)?
            .is_none_or(|source| !source.has_positive_area())
        {
            return Ok(center);
        }
    }
    if world_x > 2.0 * world_y || world_y > 2.0 * world_x {
        let dx = frame.map_or(upper, |frame| upper * frame.scale_x());
        let dy = frame.map_or(upper, |frame| upper * frame.scale_y());
        let ends = if world_x > world_y {
            [
                Point::new_raw_xy(bounds.minx() + dx, center.y),
                Point::new_raw_xy(bounds.maxx() - dx, center.y),
            ]
        } else {
            [
                Point::new_raw_xy(center.x, bounds.miny() + dy),
                Point::new_raw_xy(center.x, bounds.maxy() - dy),
            ]
        };
        for point in ends {
            let seed = field.cell(point, 0.0, 0.0, None)?;
            if seed.distance > best.distance {
                best = seed;
            }
        }
    }
    if field.bvh.is_none()
        && frame.is_none()
        && upper - best.distance > tolerance
        && let Some((upper, center)) =
            label_carrier_enclosure(&field.linework, best.center, tolerance)
    {
        field.upper = field.upper.min(upper);
        let seed = field.cell(center, 0.0, 0.0, None)?;
        if seed.distance > best.distance {
            best = seed;
        }
    }
    if field.upper - best.distance <= tolerance {
        return Ok(best.center);
    }
    let mut budget = ExpansionBudget::new("polylabel", "tolerance");
    let mut cells = BinaryHeap::new();
    cells.push(LabelCell {
        upper: root.upper.min(field.upper),
        ..root
    });
    budget.add(1)?;
    while let Some(cell) = cells.pop() {
        if cell.distance > best.distance {
            best = cell;
        }
        if cell.upper - best.distance <= tolerance {
            continue;
        }
        let x = frame.map_or(cell.half_x, |frame| cell.half_x / frame.scale_x());
        let y = frame.map_or(cell.half_y, |frame| cell.half_y / frame.scale_y());
        let split_x = x >= y;
        let split_both = x <= 2.0 * y && y <= 2.0 * x;
        let hx = if split_x || split_both {
            cell.half_x / 2.0
        } else {
            cell.half_x
        };
        let hy = if !split_x || split_both {
            cell.half_y / 2.0
        } else {
            cell.half_y
        };
        if hx <= 0.0 || hy <= 0.0 {
            continue;
        }
        budget.add(if split_both { 4 } else { 2 })?;
        for sx in [-1.0, 1.0] {
            let dx = if split_x || split_both { sx * hx } else { 0.0 };
            for sy in [-1.0, 1.0] {
                let dy = if !split_x || split_both { sy * hy } else { 0.0 };
                cells.push(field.cell(
                    Point::new_raw_xy(cell.center.x + dx, cell.center.y + dy),
                    hx,
                    hy,
                    Some(&cell),
                )?);
                if split_x && !split_both {
                    break;
                }
            }
            if !split_x && !split_both {
                break;
            }
        }
    }
    Ok(best.center)
}

/// Supporting half-planes bound every contained disk. The cyclic normal
/// cross-products cancel its center; a three-plane solve only nominates a
/// center, whose actual clearance is measured by the cell field.
fn label_carrier_enclosure(
    linework: &PreparedLinework,
    probe: Point,
    tolerance: f64,
) -> Option<(f64, Point)> {
    let mut segments = Vec::new();
    linework.for_each_segment(|segment| segments.push(segment));
    let mut planes = Vec::new();
    let mut scale = 0.0_f64;
    for &segment in &segments {
        let dx = segment.end.x - segment.start.x;
        let dy = segment.end.y - segment.start.y;
        let length = dx.hypot(dy);
        if length == 0.0 || !length.is_finite() {
            continue;
        }
        let side = orientation(segment.start, segment.end, probe);
        if side == Orientation::Collinear {
            continue;
        }
        if segments.iter().any(|other| {
            [other.start, other.end].into_iter().any(|point| {
                let other_side = orientation(segment.start, segment.end, point);
                other_side != side && other_side != Orientation::Collinear
            })
        }) {
            continue;
        }
        let sign = if side == Orientation::CounterClockwise {
            1.0
        } else {
            -1.0
        };
        let nx = -dy / length * sign;
        let ny = dx / length * sign;
        let x = probe.x - segment.start.x;
        let y = probe.y - segment.start.y;
        scale = scale.max(x.abs()).max(y.abs()).max(length);
        planes.push((nx, ny, nx * x + ny * y));
    }
    // If arithmetic uncertainty is material to the requested tolerance,
    // retain the cell bound instead of trusting the floating carrier solve.
    let error = scale * (128.0 * f64::EPSILON);
    if !error.is_finite() || error >= tolerance / 4.0 {
        return None;
    }
    let mut best = None;
    let cross = |a: (f64, f64, f64), b: (f64, f64, f64)| a.0 * b.1 - a.1 * b.0;
    for (i, &a) in planes.iter().enumerate() {
        for (j, &b) in planes.iter().enumerate().skip(i.wrapping_add(1)) {
            for &c in planes.iter().skip(j.wrapping_add(1)) {
                let mut weights = [cross(b, c), cross(c, a), cross(a, b)];
                if weights.iter().all(|&w| w <= 0.0) {
                    for w in &mut weights {
                        *w = -*w;
                    }
                }
                if weights.iter().any(|&w| w < 0.0) {
                    continue;
                }
                let sum = weights.iter().sum::<f64>();
                if sum <= f64::EPSILON.sqrt() {
                    continue;
                }
                let radius = (weights[0] * a.2 + weights[1] * b.2 + weights[2] * c.2) / sum;
                let padding = error / sum;
                if !radius.is_finite() || padding >= tolerance / 4.0 {
                    continue;
                }
                let upper = (radius + padding).next_up();
                if best.as_ref().is_some_and(|&(bound, _)| upper >= bound) {
                    continue;
                }
                let mut nominee = None;
                let mut determinant = 0.0;
                for (left, right) in [(a, b), (a, c), (b, c)] {
                    let det = cross(left, right);
                    if det.abs() > determinant {
                        determinant = det.abs();
                        let ar = radius - left.2;
                        let br = radius - right.2;
                        nominee = Some(Point::new_raw_xy(
                            probe.x + (ar * right.1 - left.1 * br) / det,
                            probe.y + (left.0 * br - ar * right.0) / det,
                        ));
                    }
                }
                if let Some(point) = nominee
                    && point.x.is_finite()
                    && point.y.is_finite()
                {
                    best = Some((upper, point));
                }
            }
        }
    }
    best
}

fn framed_metric_segment_distance(frame: AxisFrame, point: Point, segment: Segment) -> f64 {
    let ux = (point.x - segment.start.x) / frame.scale_x();
    let uy = (point.y - segment.start.y) / frame.scale_y();
    let vx = (segment.end.x - segment.start.x) / frame.scale_x();
    let vy = (segment.end.y - segment.start.y) / frame.scale_y();
    let magnitude = vx.abs().max(vy.abs());
    let fraction = if magnitude == 0.0 {
        0.0
    } else {
        let nx = vx / magnitude;
        let ny = vy / magnitude;
        let dot = if nx == 0.0 {
            0.0
        } else {
            (ux / magnitude) * nx
        } + if ny == 0.0 {
            0.0
        } else {
            (uy / magnitude) * ny
        };
        (dot / (nx * nx + ny * ny)).clamp(0.0, 1.0)
    };
    let x = segment.start.x + (segment.end.x - segment.start.x) * fraction;
    let y = segment.start.y + (segment.end.y - segment.start.y) * fraction;
    ((point.x - x) / frame.scale_x()).hypot((point.y - y) / frame.scale_y())
}

fn has_interior(shape: &Shape) -> bool {
    let ring_spans_area = |ring: &Ring| {
        let mut points = ring.coords().iter_coords();
        let Some(first) = points.next() else {
            return false;
        };
        let mut points = points.skip_while(|point| point.xy() == first.xy());
        let Some(second) = points.next() else {
            return false;
        };
        points.any(|point| orientation(first, second, point) != Orientation::Collinear)
    };
    match shape {
        Shape::Polygon(polygon) => polygon.rings().any(ring_spans_area),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .flat_map(Polygon::rings)
            .any(ring_spans_area),
        Shape::GeometryCollection(geometries) => geometries.iter().any(has_interior),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => false,
    }
}

fn representative_point(shape: &Shape, bounds: Bounds) -> Result<Point> {
    Ok(match shape.point_on_surface()? {
        Shape::Point(point) => point,
        Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => degenerate_polygon_center(bounds),
    })
}

/// The omitted search tolerance used by both polylabel and the certified
/// maximum-inscribed-circle owner. The frame is only used to form stable
/// extent differences; the returned value is in the caller's Euclidean units.
pub(crate) fn scale_aware_default_tolerance(shape: &Shape) -> f64 {
    let Some(bounds) = shape.bounds() else {
        return f64::from_bits(1);
    };
    let midpoint = degenerate_polygon_center(bounds);
    let half_width = bounds.maxx() / 2.0 - bounds.minx() / 2.0;
    let half_height = bounds.maxy() / 2.0 - bounds.miny() / 2.0;
    let Some(frame) = AxisFrame::from_origin_extents(
        midpoint,
        half_width
            .abs()
            .max(bounds.minx().abs())
            .max(bounds.maxx().abs()),
        half_height
            .abs()
            .max(bounds.miny().abs())
            .max(bounds.maxy().abs()),
    ) else {
        return f64::from_bits(1);
    };
    let min = frame.frame_xy(bounds.minx(), bounds.miny());
    let max = frame.frame_xy(bounds.maxx(), bounds.maxy());
    (((max.x - min.x).abs() / frame.scale_x()).min((max.y - min.y).abs() / frame.scale_y()) / 500.0)
        .max(f64::from_bits(1))
}

#[cfg(test)]
type ExactRatio = (ExactDyadic, ExactDyadic);

#[cfg(test)]
fn compare_exact_ratios(left: &ExactRatio, right: &ExactRatio) -> Result<Ordering> {
    compare_ratios(&left.0, &left.1, &right.0, &right.1)
}

fn exact_difference(left: f64, right: f64) -> Result<ExactDyadic> {
    ExactDyadic::from_f64(left).try_subtract(ExactDyadic::from_f64(right))
}

#[cfg(test)]
fn exact_squared_sum(left: &ExactDyadic, right: &ExactDyadic) -> Result<ExactDyadic> {
    left.try_product(left)?.try_add(right.try_product(right)?)
}

#[cfg(test)]
fn exact_point_pair_squared(left: XY, right: XY) -> Result<ExactRatio> {
    let dx = exact_difference(left.x, right.x)?;
    let dy = exact_difference(left.y, right.y)?;
    Ok((exact_squared_sum(&dx, &dy)?, ExactDyadic::from_f64(1.0)))
}

#[cfg(test)]
fn exact_point_segment_squared(point: Point, segment: Segment) -> Result<ExactRatio> {
    let point = point.xy();
    let dx = exact_difference(segment.end.x, segment.start.x)?;
    let dy = exact_difference(segment.end.y, segment.start.y)?;
    let qx = exact_difference(point.x, segment.start.x)?;
    let qy = exact_difference(point.y, segment.start.y)?;
    let denominator = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
    let zero = ExactDyadic::from_f64(0.0);
    if denominator.is_zero() {
        return exact_point_pair_squared(point, segment.start);
    }
    let numerator = qx.try_product(&dx)?.try_add(qy.try_product(&dy)?)?;
    if numerator.cmp(&zero) != Ordering::Greater {
        return exact_point_pair_squared(point, segment.start);
    }
    if numerator.cmp(&denominator) != Ordering::Less {
        return exact_point_pair_squared(point, segment.end);
    }
    let point_squared = exact_squared_sum(&qx, &qy)?;
    Ok((
        point_squared
            .try_product(&denominator)?
            .try_subtract(numerator.try_product(&numerator)?)?,
        denominator,
    ))
}

#[cfg(test)]
fn exact_boundary_distance_ratio(shape: &Shape, center: Point) -> Result<Option<ExactRatio>> {
    let mut best: Option<ExactRatio> = None;
    let mut error = None;
    shape.for_each_segment(|segment| {
        if error.is_some() {
            return;
        }
        let candidate = match exact_point_segment_squared(center, segment) {
            Ok(candidate) => candidate,
            Err(failure) => {
                error = Some(failure);
                return;
            },
        };
        let replace =
            best.as_ref()
                .is_none_or(|current| match compare_exact_ratios(&candidate, current) {
                    Ok(order) => order == Ordering::Less,
                    Err(failure) => {
                        error = Some(failure);
                        false
                    },
                });
        if replace {
            best = Some(candidate);
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(best)
}

#[cfg(test)]
fn exact_boundary_squared(shape: &Shape, center: Point) -> Result<Rational> {
    exact_boundary_distance_ratio(shape, center)?.map_or_else(
        || Ok(Rational::zero()),
        |(numerator, denominator)| Rational::new(numerator, denominator),
    )
}

#[cfg(test)]
fn exact_boundary_radius(shape: &Shape, center: Point) -> Result<f64> {
    exact_boundary_squared(shape, center).and_then(|distance| distance.round_sqrt())
}

#[cfg(test)]
fn exact_cross(
    left_x: &ExactDyadic,
    left_y: &ExactDyadic,
    right_x: &ExactDyadic,
    right_y: &ExactDyadic,
) -> Result<ExactDyadic> {
    left_x
        .try_product(right_y)?
        .try_subtract(left_y.try_product(right_x)?)
}

#[cfg(test)]
fn update_lower(lower: &mut ExactRatio, lower_closed: &mut bool, bound: ExactRatio) -> Result<()> {
    match compare_exact_ratios(&bound, lower)? {
        Ordering::Greater => {
            *lower = bound;
            *lower_closed = false;
        },
        Ordering::Equal => *lower_closed = false,
        Ordering::Less => {},
    }
    Ok(())
}

#[cfg(test)]
fn update_upper(upper: &mut ExactRatio, upper_closed: &mut bool, bound: ExactRatio) -> Result<()> {
    match compare_exact_ratios(&bound, upper)? {
        Ordering::Less => {
            *upper = bound;
            *upper_closed = false;
        },
        Ordering::Equal => *upper_closed = false,
        Ordering::Greater => {},
    }
    Ok(())
}

#[cfg(test)]
fn interval_is_empty(
    lower: &ExactRatio,
    lower_closed: bool,
    upper: &ExactRatio,
    upper_closed: bool,
) -> Result<bool> {
    Ok(match compare_exact_ratios(lower, upper)? {
        Ordering::Greater => true,
        Ordering::Equal => !(lower_closed && upper_closed),
        Ordering::Less => false,
    })
}

/// Whether a source segment has any point in the candidate's open interior.
/// The candidate is convex, so each edge contributes one exact affine strict
/// half-plane in the source segment parameter. The interval remains closed at
/// the source endpoints; only candidate boundary contacts are strict.
#[cfg(test)]
fn segment_enters_candidate(
    segment: Segment,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let zero = ExactDyadic::from_f64(0.0);
    let one = ExactDyadic::from_f64(1.0);
    let lower_bound = (zero.clone(), one.clone());
    let upper_bound = (one.clone(), one);
    let mut lower = lower_bound;
    let mut upper = upper_bound;
    let mut lower_closed = true;
    let mut upper_closed = true;
    let source_dx = exact_difference(segment.end.x, segment.start.x)?;
    let source_dy = exact_difference(segment.end.y, segment.start.y)?;

    for (start, end) in candidate
        .iter()
        .copied()
        .zip(candidate.iter().copied().cycle().skip(1))
    {
        let edge_dx = exact_difference(end.x, start.x)?;
        let edge_dy = exact_difference(end.y, start.y)?;
        let start_x = exact_difference(segment.start.x, start.x)?;
        let start_y = exact_difference(segment.start.y, start.y)?;
        let c0 = exact_cross(&edge_dx, &edge_dy, &start_x, &start_y)?;
        let c1 = exact_cross(&edge_dx, &edge_dy, &source_dx, &source_dy)?;
        let (c0, c1) = if counter_clockwise {
            (c0, c1)
        } else {
            (
                zero.clone().try_subtract(c0)?,
                zero.clone().try_subtract(c1)?,
            )
        };
        match c1.sign() {
            ExactSign::Zero => {
                if c0.sign() != ExactSign::Positive {
                    return Ok(false);
                }
            },
            ExactSign::Positive => {
                update_lower(
                    &mut lower,
                    &mut lower_closed,
                    (zero.clone().try_subtract(c0)?, c1),
                )?;
            },
            ExactSign::Negative => {
                update_upper(
                    &mut upper,
                    &mut upper_closed,
                    (c0, zero.clone().try_subtract(c1)?),
                )?;
            },
        }
        if interval_is_empty(&lower, lower_closed, &upper, upper_closed)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn candidate_center_is_strictly_inside(
    candidate: &[Point],
    center: Point,
    counter_clockwise: bool,
) -> bool {
    candidate
        .iter()
        .copied()
        .zip(candidate.iter().copied().cycle().skip(1))
        .all(|(start, end)| {
            matches!(
                (counter_clockwise, orientation(start, end, center)),
                (true, Orientation::CounterClockwise) | (false, Orientation::Clockwise)
            )
        })
}

#[cfg(test)]
fn candidate_covers_point(candidate: &[Point], point: Point, counter_clockwise: bool) -> bool {
    candidate
        .iter()
        .copied()
        .zip(candidate.iter().copied().cycle().skip(1))
        .all(|(start, end)| {
            !matches!(
                (counter_clockwise, orientation(start, end, point)),
                (true, Orientation::Clockwise) | (false, Orientation::CounterClockwise)
            )
        })
}

#[cfg(test)]
fn source_boundary_enters_candidate(
    shape: &Shape,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let mut enters = false;
    let mut error = None;
    shape.for_each_segment(|segment| {
        if enters || error.is_some() {
            return;
        }
        match segment_enters_candidate(segment, candidate, counter_clockwise) {
            Ok(true) => enters = true,
            Ok(false) => {},
            Err(failure) => error = Some(failure),
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(enters)
}

/// A crossing scan alone cannot establish that a candidate has not enclosed a
/// complete hole cycle. Convexity makes the explicit cycle test cheap: if all
/// vertices of a hole are covered by the candidate, its filled interior is
/// covered as well. Rejecting this case is conservative for degenerate input
/// and closes the hole-without-crossing gap in the source certificate.
#[cfg(test)]
fn source_hole_is_enclosed(shape: &Shape, candidate: &[Point], counter_clockwise: bool) -> bool {
    match shape {
        Shape::Polygon(polygon) => polygon.rings().skip(1).any(|hole| {
            hole.iter()
                .all(|point| candidate_covers_point(candidate, point, counter_clockwise))
        }),
        Shape::MultiPolygon(polygons) => polygons.iter().any(|polygon| {
            polygon.rings().skip(1).any(|hole| {
                hole.iter()
                    .all(|point| candidate_covers_point(candidate, point, counter_clockwise))
            })
        }),
        Shape::GeometryCollection(geometries) => geometries
            .iter()
            .any(|geometry| source_hole_is_enclosed(geometry, candidate, counter_clockwise)),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => false,
    }
}

#[cfg(test)]
fn source_vertex_outside_candidate(shape: &Shape, candidate: &[Point]) -> bool {
    for &point in candidate {
        if !shape.covers_point(point) {
            return true;
        }
    }
    false
}

pub(crate) fn emit_zero_radius_point(shape: &Shape, center: Point) -> Result<Shape> {
    // A zero or subnormal clearance cannot support a stable fixed-resolution
    // ring; closed membership, not the rounded radius, decides whether the
    // emitted point belongs to source.
    if center.x.is_finite() && center.y.is_finite() && shape.covers_point(center) {
        Ok(Shape::Point(center))
    } else {
        Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ))
    }
}

/// Build and source-certify one fixed-resolution inscribed ring. Planar
/// callers pass the prepared filled/boundary pair retained by the directed
/// search; `None` is the geographic local-projection path, which intentionally
/// keeps its existing approximate containment contract.
#[cfg(test)]
pub(crate) fn build_inscribed_circle(
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    support: Option<(&PreparedSupport, &PreparedSupport, &Rational)>,
) -> Result<Shape> {
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "quadrant_segments");
    build_inscribed_circle_budgeted(center, radius, quadrant_segments, support, &mut budget)
}

pub(crate) fn build_inscribed_circle_budgeted(
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    support: Option<(&PreparedSupport, &PreparedSupport, &Rational)>,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let steps = 4_usize
        .checked_mul(quadrant_segments.get() as usize)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let vertices = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.check_additional(vertices)?;
    if !center.x.is_finite() || !center.y.is_finite() || !radius.is_finite() {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let mut candidate_radius = radius;
    let mut retries = 0_u32;
    loop {
        budget.add(vertices)?;
        let attempt = build_inscribed_circle_once(
            center,
            candidate_radius,
            steps,
            vertices,
            quadrant_segments,
            support,
        );
        match attempt {
            Ok(output) => return Ok(output),
            Err(error)
                if support.is_some()
                    && matches!(
                        error.kind(),
                        crate::error::ErrorKind::Geometry(
                            crate::geometry::GeometryErrorKind::UnrepresentableOutput {
                                operation: "maximum_inscribed_circle",
                                ..
                            }
                        )
                    ) =>
            {
                retries = retries.wrapping_add(1);
                let next = next_realization_radius(radius, candidate_radius, retries, false);
                if !(next > 0.0 && next.is_finite()) {
                    return Err(error);
                }
                candidate_radius = next;
            },
            Err(error) => return Err(error),
        }
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "quadrant is nonzero and steps is checked as four times quadrant, bounding the step division and modulo"
)]
fn build_inscribed_circle_once(
    center: Point,
    radius: f64,
    steps: usize,
    vertices: usize,
    quadrant_segments: NonZeroU32,
    support: Option<(&PreparedSupport, &PreparedSupport, &Rational)>,
) -> Result<Shape> {
    let mut points = Vec::with_capacity(steps);
    let quadrant = quadrant_segments.get() as usize;
    for step in 0..steps {
        let (unit_x, unit_y) = if step.is_multiple_of(quadrant) {
            match (step / quadrant) % 4 {
                0 => (1.0, 0.0),
                1 => (0.0, 1.0),
                2 => (-1.0, 0.0),
                _ => (0.0, -1.0),
            }
        } else {
            let (sin, cos) = (std::f64::consts::TAU * step as f64 / steps as f64).sin_cos();
            (cos, sin)
        };
        let x = center.x + radius * unit_x;
        let y = center.y + radius * unit_y;
        if !x.is_finite() || !y.is_finite() {
            return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
                "maximum_inscribed_circle",
            ));
        }
        points.push(Point::new_raw_xy(x, y));
    }
    let mut closed = Vec::with_capacity(vertices);
    closed.extend_from_slice(&points);
    closed.push(
        points
            .first()
            .copied()
            .expect("a nonzero quadrant count emits circle vertices"),
    );
    let seq = CoordSeq::try_from_points(&closed)?;
    let winding = crate::geometry::ring_winding(&seq);
    let strict_convex = predicates::shell_is_strictly_convex(&seq);
    if winding.is_degenerate() || !strict_convex {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let counter_clockwise = winding.is_ccw();
    let center_in_candidate =
        candidate_center_is_strictly_inside(&points, center, counter_clockwise);
    let source_contained = match support {
        None => true,
        Some((source, boundary, clearance)) => source.candidate_is_contained(
            boundary,
            center,
            clearance,
            &points,
            counter_clockwise,
        )?,
    };
    if !center_in_candidate || !source_contained {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(seq)?,
        Vec::new(),
    )))
}

pub(crate) const fn degenerate_polygon_center(bounds: Bounds) -> Point {
    Point::new_raw_xy(
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    )
}

#[cfg(test)]
mod tests;
