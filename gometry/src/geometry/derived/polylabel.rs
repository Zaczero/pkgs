#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::num::NonZeroU32;

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::exact::{
    self, ExactDyadic, round_sqrt_ratio, try_compare_ratios,
};
use crate::geometry::{
    AxisFrame, BVH_MIN_INDEXED_SEGMENTS, Bounds, CoordSeq, CoordSeqBuilder, Coordinates as _,
    ExpansionBudget, FacetBvh, Orientation, Point, Polygon, PolylabelCell, PreparedLinework, Ring,
    Segment, Shape, XY, orientation,
};
impl Eq for PolylabelCell {}

impl PartialEq for PolylabelCell {
    fn eq(&self, other: &Self) -> bool {
        self.max_distance.to_bits() == other.max_distance.to_bits()
    }
}

impl Ord for PolylabelCell {
    fn cmp(&self, other: &Self) -> Ordering {
        self.max_distance.total_cmp(&other.max_distance)
    }
}

impl PartialOrd for PolylabelCell {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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
    fn support_xy(self, points: &[Point]) -> ([XY; 3], usize) {
        let mut support = [XY::new(0.0, 0.0); 3];
        let indices = match self.support {
            CircleSupport::One(a) => [a, 0, 0],
            CircleSupport::Two(a, b) => [a, b, 0],
            CircleSupport::Three(a, b, c) => [a, b, c],
        };
        let count = match self.support {
            CircleSupport::One(_) => 1,
            CircleSupport::Two(..) => 2,
            CircleSupport::Three(..) => 3,
        };
        for index in 0..count {
            support[index] = points[indices[index]].xy();
        }
        (support, count)
    }

    fn contains(self, points: &[Point], point: usize) -> Result<bool> {
        let (support, count) = self.support_xy(points);
        exact::try_circle_contains(&support[..count], points[point].xy())
    }

    fn widest_pair(points: &[Point], a: usize, b: usize, c: usize) -> Result<Self> {
        let pairs = [(a, b), (a, c), (b, c)];
        let mut widest = pairs[0];
        for &(left, right) in &pairs[1..] {
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

    fn certificate(self, points: &[Point]) -> Result<bool> {
        let (support, count) = self.support_xy(points);
        exact::try_certify_support(
            &support[..count],
            points.iter().copied().map(|point| point.xy()),
        )
    }

    /// Materialize the exact disk only after the enclosing-support certificate
    /// succeeds. This is the sole path from a support candidate to a disk.
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
        let scale = ExactDyadic::try_from_f64(to_metre)?;
        let scaled_numerator = self
            .radius_squared_numerator
            .try_product(&scale)?
            .try_product(&scale)?;
        exact::round_sqrt_ratio(&scaled_numerator, &self.radius_squared_denominator)
    }

    /// Whether a binary64 vertex is strictly outside this exact disk.
    fn vertex_is_strictly_outside(&self, point: XY) -> Result<bool> {
        let point_x = ExactDyadic::try_from_f64(point.x)?;
        let point_y = ExactDyadic::try_from_f64(point.y)?;
        let dx = self
            .center_x
            .try_clone()?
            .try_subtract(point_x.try_product(&self.center_w)?)?;
        let dy = self
            .center_y
            .try_clone()?
            .try_subtract(point_y.try_product(&self.center_w)?)?;
        let distance_squared = dx
            .try_product(&dx)?
            .try_add(dy.try_product(&dy)?)?
            .try_product(&self.radius_squared_denominator)?;
        let radius_squared = self
            .radius_squared_numerator
            .try_product(&self.radius_squared_denominator)?;
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
        let left_x = ExactDyadic::try_from_f64(left.x)?;
        let left_y = ExactDyadic::try_from_f64(left.y)?;
        let center_offset_x = self
            .center_x
            .try_clone()?
            .try_subtract(left_x.try_product(&self.center_w)?)?;
        let center_offset_y = self
            .center_y
            .try_clone()?
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
        let right_side = self
            .radius_squared_numerator
            .try_product(&self.radius_squared_denominator)?
            .try_product(&edge_squared)?;
        Ok(left_side.cmp(&right_side) != Ordering::Less)
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct CircleWork {
    contains: usize,
    one_supports: usize,
    two_supports: usize,
    three_supports: usize,
    collinear_fallbacks: usize,
}

#[cfg(not(test))]
type CircleWork = ();

#[inline]
const fn count_contains(_work: &mut CircleWork) {
    #[cfg(test)]
    {
        _work.contains += 1;
    }
}

#[inline]
const fn count_one_support(_work: &mut CircleWork) {
    #[cfg(test)]
    {
        _work.one_supports += 1;
    }
}

#[inline]
const fn count_two_support(_work: &mut CircleWork) {
    #[cfg(test)]
    {
        _work.two_supports += 1;
    }
}

#[inline]
const fn count_three_support(_work: &mut CircleWork) {
    #[cfg(test)]
    {
        _work.three_supports += 1;
    }
}

#[inline]
const fn count_collinear_fallback(_work: &mut CircleWork) {
    #[cfg(test)]
    {
        _work.collinear_fallbacks += 1;
    }
}

fn solve_enclosing_circle_order(
    points: &[Point],
    order: &[usize],
    work: &mut CircleWork,
) -> Result<MinimumCircle> {
    let mut circle = MinimumCircle::new_one(order[0]);
    count_one_support(work);
    for i in 1..order.len() {
        count_contains(work);
        if circle.contains(points, order[i])? {
            continue;
        }
        circle = MinimumCircle::new_one(order[i]);
        count_one_support(work);
        for j in 0..i {
            count_contains(work);
            if circle.contains(points, order[j])? {
                continue;
            }
            circle = MinimumCircle::new_two(order[i], order[j]);
            count_two_support(work);
            for k in 0..j {
                count_contains(work);
                if circle.contains(points, order[k])? {
                    continue;
                }
                let a = order[i];
                let b = order[j];
                let c = order[k];
                circle = if exact::try_orient2d(points[a].xy(), points[b].xy(), points[c].xy())?
                    == ExactSign::Zero
                {
                    count_collinear_fallback(work);
                    MinimumCircle::widest_pair(points, a, b, c)?
                } else {
                    count_three_support(work);
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
    let mut order = Vec::new();
    crate::try_reserve_vec(&mut order, points.len())?;
    order.extend(0..points.len());
    for index in (1..order.len()).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let other = (state >> 33) as usize % (index + 1);
        order.swap(index, other);
    }

    #[cfg(test)]
    {
        let mut work = CircleWork::default();
        solve_enclosing_circle_order(points, &order, &mut work)
    }
    #[cfg(not(test))]
    {
        solve_enclosing_circle_order(points, &order, &mut ())
    }
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
    if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
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

    let mut vertices = Vec::new();
    crate::try_reserve_vec(&mut vertices, expected_length)?;
    for point in shell.coords().iter_coords() {
        if !point.x.is_finite() || !point.y.is_finite() {
            return Err(unrepresentable_minimum_circle());
        }
        vertices.push(point.xy());
    }
    let Some((&first, rest)) = vertices.split_first() else {
        return Err(unrepresentable_minimum_circle());
    };
    let Some(&last) = rest.last() else {
        return Err(unrepresentable_minimum_circle());
    };
    let same_coordinate = |left: f64, right: f64| {
        crate::geometry::canonical_f64_bits(left) == crate::geometry::canonical_f64_bits(right)
    };
    if !same_coordinate(first.x, last.x) || !same_coordinate(first.y, last.y) {
        return Err(unrepresentable_minimum_circle());
    }
    let open = &vertices[..vertices.len() - 1];
    if open.len() < 3 {
        return Err(unrepresentable_minimum_circle());
    }
    for index in 0..open.len() {
        let next = (index + 1) % open.len();
        if same_coordinate(open[index].x, open[next].x)
            && same_coordinate(open[index].y, open[next].y)
        {
            return Err(unrepresentable_minimum_circle());
        }
    }

    let mut direction = None;
    for index in 0..open.len() {
        let previous = open[(index + open.len() - 1) % open.len()];
        let current = open[index];
        let next = open[(index + 1) % open.len()];
        let turn = exact::try_orient2d(previous, current, next).map_err(circle_emission_error)?;
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
    }
    let direction = direction.expect("three or more open circle vertices");
    if direction != ExactSign::Positive {
        return Err(unrepresentable_minimum_circle());
    }

    for index in 0..open.len() {
        let left = open[index];
        let right = open[(index + 1) % open.len()];
        if !disk
            .vertex_is_strictly_outside(left)
            .map_err(circle_emission_error)?
            || !disk
                .ccw_edge_encloses(left, right)
                .map_err(circle_emission_error)?
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

    let mut points = Vec::new();
    crate::try_reserve_vec(&mut points, vertices)?;
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
        crate::try_push_vec(&mut points, Point::new_raw_xy(x, y))?;
    }
    let first = points
        .first()
        .copied()
        .ok_or_else(unrepresentable_minimum_circle)?;
    crate::try_push_vec(&mut points, first)?;
    let seq = CoordSeq::try_from_points(&points)?;
    Ok(Shape::Polygon(Polygon::new(
        Ring::from_closed(seq),
        Vec::new(),
    )?))
}

/// Emit a regular tangent-intersection polygon at a candidate disk radius,
/// then certify the stored output before publishing it. A bounded binary64
/// retry covers the one-ULP loss at the tangent boundary; the requested
/// segment count never changes.
pub(crate) fn emit_minimum_bounding_circle(
    disk: &CertifiedDisk,
    quadrant_segments: NonZeroU32,
) -> Result<Shape> {
    const MAX_CIRCUMRADIUS_ULP_RETRIES: usize = 32;
    let radius = disk.radius().map_err(circle_emission_error)?;
    let center = disk.center().map_err(circle_emission_error)?;
    if !center.x.is_finite() || !center.y.is_finite() || !radius.is_finite() || radius <= 0.0 {
        return Err(unrepresentable_minimum_circle());
    }
    let mut budget = ExpansionBudget::new("minimum_bounding_circle", "quadrant_segments");
    let mut candidate_radius = radius;
    for _ in 0..=MAX_CIRCUMRADIUS_ULP_RETRIES {
        let output = match tangent_circle_polygon(
            center,
            candidate_radius,
            quadrant_segments,
            &mut budget,
        ) {
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
        let next = candidate_radius.next_up();
        if !next.is_finite() {
            break;
        }
        candidate_radius = next;
    }
    Err(unrepresentable_minimum_circle())
}

/// The pole of inaccessibility, returned after denormalizing the search center.
/// Maximum-circle clearance is recomputed separately in source coordinates;
/// a rounded nearest-point witness is not a continuum-distance certificate.
struct PolylabelFrame {
    shape: Shape,
    frame: AxisFrame,
    default_tolerance: f64,
    max_inradius: f64,
}

/// Euclidean distance in an [`AxisFrame`]'s local coordinates.
///
/// The frame intentionally has a distinct scale for each axis.  That keeps a
/// reciprocal rectangle representable, but it also means its local space is
/// *not* Euclidean: a plain local nearest-point query would choose a vertical
/// edge over the physically-near horizontal one.  Keep the metric here, next
/// to the only consumer that needs it, rather than silently changing the
/// geometry it is measuring.
fn framed_metric_distance(frame: AxisFrame, left: XY, right: XY) -> f64 {
    ((left.x - right.x) / frame.scale_x()).hypot((left.y - right.y) / frame.scale_y())
}

/// Nearest point on a local-frame segment under the frame's original
/// Euclidean metric.  Normalizing the segment direction before its dot
/// product avoids both the huge-axis overflow and the tiny-axis false zero.
fn framed_metric_nearest_on_segment(
    frame: AxisFrame,
    point: Point,
    segment: Segment,
) -> (Point, f64) {
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
        // Do not form `infinity * 0.0` for a probe very far along the other
        // axis of a short horizontal/vertical segment.
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
    let nearest = Point::new_raw_xy(
        segment.start.x + (segment.end.x - segment.start.x) * fraction,
        segment.start.y + (segment.end.y - segment.start.y) * fraction,
    );
    (
        nearest,
        framed_metric_distance(frame, point.xy(), nearest.xy()),
    )
}

fn framed_metric_cell_radius(frame: AxisFrame, half_size: f64) -> f64 {
    (half_size / frame.scale_x()).hypot(half_size / frame.scale_y())
}

fn normalized_polylabel_frame(
    shape: &Shape,
    bounds: Bounds,
) -> crate::error::Result<Option<PolylabelFrame>> {
    let midpoint = Point::new_raw_xy(
        f64::midpoint(bounds.minx(), bounds.maxx()),
        f64::midpoint(bounds.miny(), bounds.maxy()),
    );
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
        return Ok(None);
    };
    let xy = shape.force_2d()?;
    let normalized = xy.try_map_coordseqs(
        |seq| {
            let mut builder = CoordSeqBuilder::try_with_capacity(seq.axes(), seq.len())?;
            for point in seq.iter_coords() {
                let xy = frame.frame_xy(point.x, point.y);
                builder.push(point.with_xy(xy.x, xy.y)?)?;
            }
            builder.finish()
        },
        |point| {
            let xy = frame.frame_xy(point.x, point.y);
            point.with_xy(xy.x, xy.y)
        },
    )?;
    let normalized_bounds = normalized
        .bounds()
        .expect("non-empty normalized polygon retains bounds");
    Ok(Some(PolylabelFrame {
        shape: normalized,
        frame,
        // Tolerances and bounds are public Euclidean distances.  The local
        // frame is only a representation rescue, so convert each axis back
        // independently before comparing them.
        default_tolerance: ((normalized_bounds.maxx() - normalized_bounds.minx()).abs()
            / frame.scale_x())
        .min((normalized_bounds.maxy() - normalized_bounds.miny()).abs() / frame.scale_y())
            / 500.0,
        max_inradius: ((normalized_bounds.maxx() - normalized_bounds.minx()).abs()
            / frame.scale_x())
        .min((normalized_bounds.maxy() - normalized_bounds.miny()).abs() / frame.scale_y())
            / 2.0,
    }))
}

pub(crate) fn polylabel_point(shape: &Shape, tolerance: Option<f64>) -> Result<Point> {
    let Some(bounds) = shape.bounds() else {
        let origin = Point::new_raw_xy(0.0, 0.0);
        return Ok(origin);
    };
    // Degenerate input — a zero-width bounding box (a sliver or a point): the
    // representative point is the answer. Build the original-frame field only
    // on this rare branch.
    let Some(frame) = normalized_polylabel_frame(shape, bounds)? else {
        return representative_point(shape, bounds);
    };
    let normalized_bounds = frame
        .shape
        .bounds()
        .expect("non-empty normalized polygon retains bounds");
    let mut field = PolylabelField::new(&frame.shape, frame.max_inradius, Some(frame.frame))?;
    if field.is_empty() {
        return representative_point(shape, bounds);
    }

    // Tolerance remains in public Euclidean coordinates.  The framed field
    // measures in that same metric, so an explicit tolerance is not silently
    // reinterpreted as a coordinate-space tolerance.
    let tolerance = tolerance.unwrap_or(frame.default_tolerance);

    let mut best = field.cell(
        representative_point(&frame.shape, normalized_bounds)?,
        0.0,
        None,
    )?;
    let bbox_center = Point::new_raw_xy(0.0, 0.0);
    let bbox_cell = field.cell(bbox_center, 0.0, None)?;
    if bbox_cell.distance > best.distance {
        best = bbox_cell;
    }

    let mut budget = ExpansionBudget::new("polylabel", "tolerance");
    budget.add(1)?;
    let mut cells = BinaryHeap::with_capacity(1);
    cells.push(field.cell(bbox_center, 1.0, None)?);

    while let Some(cell) = cells.pop() {
        if cell.distance > best.distance {
            best = cell;
        }
        if cell.max_distance - best.distance <= tolerance {
            continue;
        }

        let half_size = cell.half_size / 2.0;
        if half_size <= 0.0 {
            continue;
        }
        budget.add(4)?;
        for dx in [-half_size, half_size] {
            for dy in [-half_size, half_size] {
                cells.push(field.cell(
                    Point::new_raw_xy(cell.center.x + dx, cell.center.y + dy),
                    half_size,
                    Some(&cell),
                )?);
            }
        }
    }

    let denormalize = |point: Point| {
        let world = frame.frame.unframe_xy(point.xy());
        Point::new_raw_xy(world.x, world.y)
    };
    let center = denormalize(best.center);
    Ok(center)
}

type ExactRatio = (ExactDyadic, ExactDyadic);

fn compare_exact_ratios(left: &ExactRatio, right: &ExactRatio) -> Result<Ordering> {
    try_compare_ratios(&left.0, &left.1, &right.0, &right.1)
}

fn exact_difference(left: f64, right: f64) -> Result<ExactDyadic> {
    ExactDyadic::try_from_f64(left)?.try_subtract(ExactDyadic::try_from_f64(right)?)
}

fn exact_squared_sum(left: &ExactDyadic, right: &ExactDyadic) -> Result<ExactDyadic> {
    left.try_product(left)?.try_add(right.try_product(right)?)
}

fn exact_point_pair_squared(left: XY, right: XY) -> Result<ExactRatio> {
    let dx = exact_difference(left.x, right.x)?;
    let dy = exact_difference(left.y, right.y)?;
    Ok((
        exact_squared_sum(&dx, &dy)?,
        ExactDyadic::try_from_f64(1.0)?,
    ))
}

fn exact_point_segment_squared(point: Point, segment: Segment) -> Result<ExactRatio> {
    let point = point.xy();
    let dx = exact_difference(segment.end.x, segment.start.x)?;
    let dy = exact_difference(segment.end.y, segment.start.y)?;
    let qx = exact_difference(point.x, segment.start.x)?;
    let qy = exact_difference(point.y, segment.start.y)?;
    let denominator = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
    let zero = ExactDyadic::try_from_f64(0.0)?;
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

fn exact_boundary_radius(shape: &Shape, center: Point) -> Result<f64> {
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
    best.map_or(Ok(0.0), |distance| {
        round_sqrt_ratio(&distance.0, &distance.1)
    })
}

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
fn segment_enters_candidate(
    segment: Segment,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let zero = ExactDyadic::try_from_f64(0.0)?;
    let one = ExactDyadic::try_from_f64(1.0)?;
    let lower_bound = (zero.try_clone()?, one.try_clone()?);
    let upper_bound = (one.try_clone()?, one);
    let mut lower = lower_bound;
    let mut upper = upper_bound;
    let mut lower_closed = true;
    let mut upper_closed = true;
    let source_dx = exact_difference(segment.end.x, segment.start.x)?;
    let source_dy = exact_difference(segment.end.y, segment.start.y)?;

    for index in 0..candidate.len() {
        let start = candidate[index];
        let end = candidate[(index + 1) % candidate.len()];
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
                zero.try_clone()?.try_subtract(c0)?,
                zero.try_clone()?.try_subtract(c1)?,
            )
        };
        match c1.sign() {
            crate::geometry::exact_arith::ExactSign::Zero => {
                if c0.sign() != crate::geometry::exact_arith::ExactSign::Positive {
                    return Ok(false);
                }
            },
            crate::geometry::exact_arith::ExactSign::Positive => {
                update_lower(
                    &mut lower,
                    &mut lower_closed,
                    (zero.try_clone()?.try_subtract(c0)?, c1),
                )?;
            },
            crate::geometry::exact_arith::ExactSign::Negative => {
                update_upper(
                    &mut upper,
                    &mut upper_closed,
                    (c0, zero.try_clone()?.try_subtract(c1)?),
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
    candidate.iter().enumerate().all(|(index, &start)| {
        let end = candidate[(index + 1) % candidate.len()];
        matches!(
            (counter_clockwise, orientation(start, end, center)),
            (true, Orientation::CounterClockwise) | (false, Orientation::Clockwise)
        )
    })
}

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

pub(crate) fn polylabel_center_radius(
    shape: &Shape,
    tolerance: Option<f64>,
) -> Result<(Point, f64)> {
    let center = polylabel_point(shape, tolerance)?;
    Ok((center, exact_boundary_radius(shape, center)?))
}

pub(crate) fn emit_zero_radius_point(shape: &Shape, center: Point) -> Result<Shape> {
    // A positive subnormal clearance may round to zero; closed membership, not
    // the rounded radius, decides whether the emitted point belongs to source.
    if center.x.is_finite() && center.y.is_finite() && shape.covers_point(center)? {
        Ok(Shape::Point(center))
    } else {
        Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ))
    }
}

pub(crate) fn build_inscribed_circle(
    shape: &Shape,
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    certify_source: bool,
) -> Result<Shape> {
    let steps = 4_usize
        .checked_mul(quadrant_segments.get() as usize)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let vertices = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "quadrant_segments");
    budget.add(vertices)?;
    if !center.x.is_finite() || !center.y.is_finite() || !radius.is_finite() {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let mut points = Vec::new();
    crate::try_reserve_vec(&mut points, steps)?;
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
    let mut closed = Vec::new();
    crate::try_reserve_vec(&mut closed, vertices)?;
    closed.extend_from_slice(&points);
    closed.push(points[0]);
    let seq = CoordSeq::try_from_points(&closed)?;
    let winding = crate::geometry::ring_winding(&seq);
    let strict_convex = crate::geometry::predicates::shell_is_strictly_convex(&seq);
    if winding.is_degenerate() || !strict_convex {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let counter_clockwise = winding.is_ccw();
    let center_in_candidate =
        candidate_center_is_strictly_inside(&points, center, counter_clockwise);
    if !center_in_candidate
        || (certify_source
            && (!shape.contains_point(center)?
                || source_boundary_enters_candidate(shape, &points, counter_clockwise)?))
    {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    Ok(Shape::Polygon(Polygon::new(
        Ring::from_closed(seq),
        Vec::new(),
    )?))
}

pub(crate) fn representative_point(shape: &Shape, bounds: Bounds) -> Result<Point> {
    match shape.point_on_surface()? {
        Shape::Point(point) => Ok(point),
        _ => Ok(Point::new_raw_xy(
            bounds.minx() + (bounds.maxx() - bounds.minx()) / 2.0,
            bounds.miny() + (bounds.maxy() - bounds.miny()) / 2.0,
        )),
    }
}

/// The indexed boundary a polylabel search probes once per cell. The facet
/// BVH is built ONCE over the linework, so each cell's distance is an
/// `O(log segments)` SIMD branch-and-bound descent rather than the per-cell
/// `O(segments)` scan it replaces. Containment (the distance sign) rides a
/// direct ray cast — needed for only the seed and boundary-straddling cells,
/// far below a full hierarchical `PointBatchTester` build's payoff. The
/// maximum-inscribed-circle owner performs its own exact source-coordinate
/// clearance fold after the public center is denormalized.
struct PolylabelField<'a> {
    shape: &'a Shape,
    linework: PreparedLinework,
    bvh: Option<FacetBvh>,
    /// Branch-and-bound traversal stack, reused across every cell probe.
    stack: Vec<u32>,
    max_inradius: f64,
    /// `Some` keeps nearest-point and cell bounds in the original Euclidean
    /// metric while the stored shape remains in its lossless local frame.
    frame: Option<AxisFrame>,
}

impl<'a> PolylabelField<'a> {
    fn new(
        shape: &'a Shape,
        max_inradius: f64,
        frame: Option<AxisFrame>,
    ) -> crate::error::Result<Self> {
        let linework = PreparedLinework::build(shape)?;
        // Below the indexing crossover the flat SIMD facet scan beats a tree
        // descent (its stack churn does not pay for a handful of facets).
        let bvh = (linework.segment_count() >= BVH_MIN_INDEXED_SEGMENTS)
            .then(|| FacetBvh::build(&linework))
            .flatten();
        Ok(Self {
            shape,
            linework,
            bvh,
            stack: Vec::new(),
            max_inradius,
            frame,
        })
    }

    const fn is_empty(&self) -> bool {
        self.linework.segment_count() == 0
    }

    /// Unsigned distance from `point` to the boundary linework. The unframed
    /// path runs its SIMD **squared** kernel (eight segment distances per
    /// vector op) and the single `sqrt` lifts the winner back to Euclidean
    /// units. A framed path stays in unsquared original-metric units because
    /// its independent local-axis scales make squared local distances
    /// meaningless.
    ///
    /// `bound` seeds the branch-and-bound with a known upper bound on the
    /// result, so the descent prunes every node farther than it from the
    /// outset (the true nearest is never pruned — it lies at or below the
    /// bound). `f64::INFINITY` means "no prior knowledge" (the seed cells).
    fn boundary_distance(&mut self, point: Point, bound: f64) -> Result<f64> {
        if let Some(frame) = self.frame {
            let linework = &self.linework;
            let mut refine = |facet: crate::geometry::facet_bvh::Facet| {
                let mut best = f64::INFINITY;
                for index in 0..facet.segment_count as usize {
                    best = best.min(
                        framed_metric_nearest_on_segment(
                            frame,
                            point,
                            linework.segment(facet, index),
                        )
                        .1,
                    );
                }
                best
            };
            let distance = if let Some(bvh) = self.bvh.as_ref() {
                bvh.min_point_distance_with_stack_framed(
                    linework,
                    point,
                    frame,
                    &mut self.stack,
                    bound,
                    &mut refine,
                )?
            } else {
                let mut best = f64::INFINITY;
                linework.for_each_segment(|segment| {
                    best = best.min(framed_metric_nearest_on_segment(frame, point, segment).1);
                });
                best
            };
            return Ok(distance);
        }
        // A positive subnormal seed can square to zero. Zero is a valid exact
        // contact seed, but an underflowed positive seed would prune every
        // positive AABB bound before the squared refinement gets a chance to
        // run.
        let bound_squared = bound * bound;
        let bound_squared = if bound > 0.0 && bound_squared == 0.0 {
            f64::INFINITY
        } else {
            bound_squared
        };
        let squared = match &self.bvh {
            Some(bvh) => bvh.min_point_distance_with_stack::<true>(
                &self.linework,
                point.x,
                point.y,
                &mut self.stack,
                bound_squared,
            ),
            None => self
                .linework
                .min_points_distance::<true>(std::iter::once((point.x, point.y)), bound_squared),
        };
        Ok(squared?.sqrt())
    }

    /// Build a cell with its signed boundary distance (`+d` inside the area,
    /// `-d` outside — the even-odd `contains_point` rule).
    ///
    /// `parent` is the cell being refined, when any. A child center lies
    /// within `parent.half_size·(1/√2)` of the parent center, so when the
    /// boundary provably cannot reach across that gap
    /// (`|parent.distance| >` the gap) the child shares the parent's side and
    /// the containment probe is skipped. The search spends most of its cells
    /// refining deep inside the optimum, where this holds — so the
    /// containment probe runs only for the few cells straddling the boundary.
    fn cell(
        &mut self,
        center: Point,
        half_size: f64,
        parent: Option<&PolylabelCell>,
    ) -> Result<PolylabelCell> {
        // The child center lies within `parent.half_size·(1/√2)` of the parent
        // center (the diagonal quadrant offset), so by the triangle inequality
        // its boundary distance is at most `|parent.distance| + that gap`. Seed
        // the search with that bound (padded by a hair so float rounding never
        // prunes the true nearest facet). The same gap decides the sign for
        // free when the boundary cannot reach across it.
        let (bound, inherited_sign) = parent.map_or((f64::INFINITY, None), |parent| {
            let gap = self.frame.map_or(
                parent.half_size * std::f64::consts::FRAC_1_SQRT_2,
                |frame| framed_metric_cell_radius(frame, parent.half_size / 2.0),
            );
            let bound = (parent.distance.abs() + gap) * (1.0 + 1e-9);
            let sign = (parent.distance.abs() > gap).then_some(parent.distance > 0.0);
            (bound, sign)
        });
        let distance = self.boundary_distance(center, bound)?;
        // Containment is needed only for the seed cells and the few cells that
        // straddle the boundary (deep cells inherit their parent's sign), so a
        // direct even-odd ray cast beats building a hierarchical
        // `PointBatchTester` whose O(segments·log) sort never amortizes over
        // so few probes.
        let inside = match inherited_sign {
            Some(inside) => inside,
            None => self.shape.contains_point(center)?,
        };
        let signed = if inside { distance } else { -distance };
        let cell_radius = self
            .frame
            .map_or(half_size * std::f64::consts::SQRT_2, |frame| {
                framed_metric_cell_radius(frame, half_size)
            });
        Ok(PolylabelCell {
            center,
            half_size,
            distance: signed,
            max_distance: (cell_radius + signed).min(self.max_inradius),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::{CoordSeq, Polygon, Ring};

    fn circle_points(center: Point, radius: f64) -> Vec<Point> {
        let mut points = Vec::with_capacity(64);
        for step in 0..64 {
            let (unit_x, unit_y) = if step % 16 == 0 {
                match (step / 16) % 4 {
                    0 => (1.0, 0.0),
                    1 => (0.0, 1.0),
                    2 => (-1.0, 0.0),
                    _ => (0.0, -1.0),
                }
            } else {
                let (sin, cos) = (std::f64::consts::TAU * f64::from(step) / 64.0).sin_cos();
                (cos, sin)
            };
            points.push(Point::new_raw_xy(
                center.x + radius * unit_x,
                center.y + radius * unit_y,
            ));
        }
        points
    }

    fn degenerate_line_shape() -> Shape {
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(2.0, 0.0),
                        Point::new_raw_xy(1.0, 0.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn dense_reciprocal_polygon(subdivisions: usize) -> Shape {
        let extent = 1.0e162;
        let half_height = 1.0e-162;
        let mut points = Vec::with_capacity(2 * (subdivisions + 1) + 1);
        for index in 0..=subdivisions {
            let fraction = index as f64 / subdivisions as f64;
            let x = -extent + 2.0 * extent * fraction;
            points.push(Point::new_raw_xy(x, -half_height));
        }
        for index in (0..=subdivisions).rev() {
            let fraction = index as f64 / subdivisions as f64;
            let x = -extent + 2.0 * extent * fraction;
            points.push(Point::new_raw_xy(x, half_height));
        }
        points.push(points[0]);
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn dense_unit_polygon(subdivisions: usize) -> Shape {
        let mut points = Vec::with_capacity(2 * (subdivisions + 1) + 1);
        for index in 0..=subdivisions {
            let fraction = index as f64 / subdivisions as f64;
            points.push(Point::new_raw_xy(-1.0 + 2.0 * fraction, -1.0));
        }
        for index in (0..=subdivisions).rev() {
            let fraction = index as f64 / subdivisions as f64;
            points.push(Point::new_raw_xy(-1.0 + 2.0 * fraction, 1.0));
        }
        points.push(points[0]);
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn normalized_dense_reciprocal_polygon() -> PolylabelFrame {
        let shape = dense_reciprocal_polygon(256);
        let bounds = shape.bounds().expect("dense polygon has bounds");
        normalized_polylabel_frame(&shape, bounds)
            .unwrap()
            .expect("reciprocal axes have a frame")
    }

    fn flat_framed_distance(frame: AxisFrame, linework: &PreparedLinework, point: Point) -> f64 {
        let mut best = f64::INFINITY;
        linework.for_each_segment(|segment| {
            best = best.min(framed_metric_nearest_on_segment(frame, point, segment).1);
        });
        best
    }

    fn indexed_framed_distance(
        frame: AxisFrame,
        linework: &PreparedLinework,
        bvh: &FacetBvh,
        point: Point,
    ) -> f64 {
        let mut stack = Vec::new();
        bvh.min_point_distance_with_stack_framed(
            linework,
            point,
            frame,
            &mut stack,
            f64::INFINITY,
            |facet| {
                let mut best = f64::INFINITY;
                for index in 0..facet.segment_count as usize {
                    best = best.min(
                        framed_metric_nearest_on_segment(
                            frame,
                            point,
                            linework.segment(facet, index),
                        )
                        .1,
                    );
                }
                best
            },
        )
        .unwrap()
    }

    #[test]
    fn reciprocal_ribbon_keeps_the_physical_nearest_edge() {
        let extent = 1.0e162;
        let half_height = 1.0 / extent;
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(-extent, -half_height),
                        Point::new_raw_xy(extent, -half_height),
                        Point::new_raw_xy(extent, half_height),
                        Point::new_raw_xy(-extent, half_height),
                        Point::new_raw_xy(-extent, -half_height),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        let center = polylabel_point(&shape, None).expect("finite ribbon");
        assert_eq!(center, Point::new_raw_xy(0.0, 0.0));
    }

    #[test]
    fn reciprocal_axis_dense_indexed_field_preserves_framed_distance() {
        let normalized = normalized_dense_reciprocal_polygon();
        let linework = PreparedLinework::build(&normalized.shape).unwrap();
        let bvh = FacetBvh::build(&linework).expect("dense linework has an index");
        let mut field = PolylabelField::new(
            &normalized.shape,
            normalized.max_inradius,
            Some(normalized.frame),
        )
        .unwrap();

        for point in [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(0.25, -0.2),
            Point::new_raw_xy(-0.75, 0.4),
        ] {
            let flat_distance = flat_framed_distance(normalized.frame, &linework, point);
            let indexed_distance =
                indexed_framed_distance(normalized.frame, &linework, &bvh, point);
            let field_distance = field.boundary_distance(point, f64::INFINITY).unwrap();
            assert_eq!(indexed_distance.to_bits(), flat_distance.to_bits());
            assert_eq!(field_distance.to_bits(), flat_distance.to_bits());
        }
    }

    #[test]
    fn framed_subnormal_seed_stays_in_unsquared_distance_units() {
        let frame = AxisFrame::from_origin_extents(Point::new_raw_xy(0.0, 0.0), 1.0, 1.0e-320)
            .expect("positive frame extents");
        let linework = PreparedLinework::from_open_xy_columns(&[0.0, 1.0], &[1.0, 1.0]).unwrap();
        let bvh = FacetBvh::build(&linework).expect("one facet has a tree root");
        let point = Point::new_raw_xy(0.0, 0.0);
        let expected =
            framed_metric_nearest_on_segment(frame, point, linework.segment(linework.facets[0], 0))
                .1;
        assert!(expected > 0.0 && expected < f64::MIN_POSITIVE);
        let mut stack = Vec::new();
        let actual = bvh
            .min_point_distance_with_stack_framed(
                &linework,
                point,
                frame,
                &mut stack,
                expected,
                |facet| {
                    let mut best = f64::INFINITY;
                    for index in 0..facet.segment_count as usize {
                        best = best.min(
                            framed_metric_nearest_on_segment(
                                frame,
                                point,
                                linework.segment(facet, index),
                            )
                            .1,
                        );
                    }
                    best
                },
            )
            .unwrap();
        assert_eq!(actual.to_bits(), expected.to_bits());
    }

    #[test]
    fn unframed_positive_seed_underflow_does_not_prune_positive_distance() {
        let shape = dense_unit_polygon(256);
        let mut field = PolylabelField::new(&shape, 1.0, None).unwrap();
        let tiny_seed = f64::from_bits(1);
        assert_eq!(tiny_seed * tiny_seed, 0.0);
        let distance = field
            .boundary_distance(Point::new_raw_xy(0.0, 0.0), tiny_seed)
            .unwrap();
        assert_eq!(distance.to_bits(), 1.0_f64.to_bits());
    }

    #[test]
    fn reciprocal_axis_index_refines_fewer_facets_and_segments_than_flat() {
        let normalized = normalized_dense_reciprocal_polygon();
        let linework = PreparedLinework::build(&normalized.shape).unwrap();
        let bvh = FacetBvh::build(&linework).expect("dense linework has an index");
        let point = Point::new_raw_xy(0.0, 0.0);
        let mut refined_facets = 0;
        let mut refined_segments = 0;
        let mut stack = Vec::new();
        let indexed_distance = bvh
            .min_point_distance_with_stack_framed(
                &linework,
                point,
                normalized.frame,
                &mut stack,
                f64::INFINITY,
                |facet| {
                    refined_facets += 1;
                    refined_segments += facet.segment_count as usize;
                    let mut best = f64::INFINITY;
                    for index in 0..facet.segment_count as usize {
                        best = best.min(
                            framed_metric_nearest_on_segment(
                                normalized.frame,
                                point,
                                linework.segment(facet, index),
                            )
                            .1,
                        );
                    }
                    best
                },
            )
            .unwrap();
        let flat_distance = flat_framed_distance(normalized.frame, &linework, point);
        assert_eq!(indexed_distance.to_bits(), flat_distance.to_bits());
        assert!(refined_facets < linework.facets.len());
        assert!(refined_segments < linework.segment_count());
    }

    #[test]
    fn zero_radius_boundary_point_uses_closed_membership() {
        let shape = degenerate_line_shape();
        let result = shape.maximum_inscribed_circle(None).unwrap();
        let Shape::Point(point) = result else {
            panic!("degenerate maximum circle must emit its point center");
        };
        assert!(shape.covers_point(point).unwrap());
        assert!(!shape.contains_point(point).unwrap());
    }

    #[test]
    fn zero_radius_outside_point_is_rejected_at_emission_boundary() {
        let shape = degenerate_line_shape();
        let result = emit_zero_radius_point(&shape, Point::new_raw_xy(1.0, 1.0));
        assert!(matches!(
            result,
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::UnrepresentableOutput {
                            operation: "maximum_inscribed_circle"
                        }
                    )
                )
        ));
    }

    #[test]
    fn source_tangent_does_not_enter_inscribed_circle() {
        let center = Point::new_raw_xy(1.0, 1.0);
        let points = circle_points(center, 1.0);
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        };
        assert!(!segment_enters_candidate(segment, &points, true).unwrap());
        for segment in [
            Segment {
                start: XY::new(0.0, 0.0),
                end: XY::new(2.0, 0.0),
            },
            Segment {
                start: XY::new(2.0, 0.0),
                end: XY::new(2.0, 2.0),
            },
            Segment {
                start: XY::new(2.0, 2.0),
                end: XY::new(0.0, 2.0),
            },
            Segment {
                start: XY::new(0.0, 2.0),
                end: XY::new(0.0, 0.0),
            },
        ] {
            assert!(!segment_enters_candidate(segment, &points, true).unwrap());
        }
        let mut closed = points.clone();
        closed.push(points[0]);
        let seq = CoordSeq::try_from_points(&closed).unwrap();
        assert!(!crate::geometry::ring_winding(&seq).is_degenerate());
        assert!(crate::geometry::shell_is_convex(&seq));
        assert!(candidate_center_is_strictly_inside(&points, center, true));
        let source = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(2.0, 0.0),
                        Point::new_raw_xy(2.0, 2.0),
                        Point::new_raw_xy(0.0, 2.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        assert!(source.contains_point(center).unwrap());
        assert!(!source_boundary_enters_candidate(&source, &points, true).unwrap());
    }

    #[test]
    fn exact_boundary_radius_uses_the_smallest_stored_distance() {
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.1, 0.1),
                        Point::new_raw_xy(0.3, 0.1),
                        Point::new_raw_xy(0.3, 0.3),
                        Point::new_raw_xy(0.1, 0.3),
                        Point::new_raw_xy(0.1, 0.1),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        assert_eq!(
            exact_boundary_radius(&shape, Point::new_raw_xy(0.2, 0.2))
                .unwrap()
                .to_bits(),
            0x3FB9_9999_9999_9998
        );
    }

    #[test]
    fn exact_boundary_radius_handles_reciprocal_source_axes() {
        let extent = 1.0e150;
        let half_height = 1.0e-150;
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(-extent, -half_height),
                        Point::new_raw_xy(extent, -half_height),
                        Point::new_raw_xy(extent, half_height),
                        Point::new_raw_xy(-extent, half_height),
                        Point::new_raw_xy(-extent, -half_height),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        assert_eq!(
            exact_boundary_radius(&shape, Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .to_bits(),
            half_height.to_bits()
        );
    }

    #[test]
    fn source_hole_inside_candidate_is_rejected_without_boundary_contact() {
        let source = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(10.0, 0.0),
                        Point::new_raw_xy(10.0, 10.0),
                        Point::new_raw_xy(0.0, 10.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                vec![Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(4.5, 4.5),
                        Point::new_raw_xy(5.5, 4.5),
                        Point::new_raw_xy(5.5, 5.5),
                        Point::new_raw_xy(4.5, 5.5),
                        Point::new_raw_xy(4.5, 4.5),
                    ])
                    .unwrap(),
                )],
            )
            .unwrap(),
        );
        let candidate = circle_points(Point::new_raw_xy(5.0, 3.0), 3.0);
        assert!(source.contains_point(Point::new_raw_xy(5.0, 3.0)).unwrap());
        assert!(source_boundary_enters_candidate(&source, &candidate, true).unwrap());
    }

    #[test]
    fn candidate_center_must_be_strictly_inside_every_edge() {
        let candidate = circle_points(Point::new_raw_xy(5.0, 3.0), 3.0);
        assert!(candidate_center_is_strictly_inside(
            &candidate,
            Point::new_raw_xy(5.0, 3.0),
            true,
        ));
        assert!(!candidate_center_is_strictly_inside(
            &candidate,
            Point::new_raw_xy(5.0, 6.0),
            true,
        ));
    }

    #[test]
    fn exact_boundary_radius_propagates_allocation_failure() {
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(2.0, 0.0),
                        Point::new_raw_xy(2.0, 2.0),
                        Point::new_raw_xy(0.0, 2.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        let result = crate::test_support::fail_allocations_after(0, || {
            exact_boundary_radius(&shape, Point::new_raw_xy(1.0, 1.0))
        });
        assert!(matches!(
            result,
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::Resource(ResourceError::Allocation)
                )
        ));
    }

    #[test]
    fn representative_point_propagates_surface_allocation_failure() {
        let shape = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(4.0, 0.0),
                        Point::new_raw_xy(0.0, 4.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        let bounds = shape.bounds().expect("nonempty polygon");
        let bbox_seed = Point::new_raw_xy(
            bounds.minx() + (bounds.maxx() - bounds.minx()) / 2.0,
            bounds.miny() + (bounds.maxy() - bounds.miny()) / 2.0,
        );
        let (_, count) = crate::test_support::allocation_count(|| {
            representative_point(&shape, bounds).expect("count staging")
        });
        assert!(count > 0, "surface staging must allocate");
        let error =
            crate::test_support::fail_allocations_after(0, || representative_point(&shape, bounds))
                .expect_err("must not swallow staging failure as a bounding-box seed");
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        let recovered = representative_point(&shape, bounds).expect("retry");
        assert_eq!((recovered.x, recovered.y), (1.0, 2.0));
        assert_ne!((recovered.x, recovered.y), (bbox_seed.x, bbox_seed.y));
    }

    #[test]
    fn minimum_circle_radius_propagates_allocation_failures() {
        let shape = Shape::MultiPoint(
            crate::geometry::MultiPoint::try_new(crate::geometry::CoordinateAxes::XY, vec![
                crate::geometry::MultiMember::Value(Point::new_raw_xy(0.0, 0.0)),
                crate::geometry::MultiMember::Value(Point::new_raw_xy(4.0, 0.0)),
                crate::geometry::MultiMember::Value(Point::new_raw_xy(1.0, 3.0)),
            ])
            .unwrap(),
        );
        let (_, allocations) = crate::test_support::allocation_count(|| {
            shape.minimum_bounding_radius().expect("finite radius")
        });
        assert!(allocations > 0, "minimum-circle staging must allocate");
        for allowed in 0..allocations {
            let error = crate::test_support::fail_allocations_after(allowed, || {
                shape.minimum_bounding_radius()
            })
            .expect_err("a rejected allocation must remain a native error");
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn minimum_circle_polygon_propagates_emission_allocation_failures() {
        let shape = Shape::MultiPoint(
            crate::geometry::MultiPoint::try_new(crate::geometry::CoordinateAxes::XY, vec![
                crate::geometry::MultiMember::Value(Point::new_raw_xy(0.0, 0.0)),
                crate::geometry::MultiMember::Value(Point::new_raw_xy(4.0, 0.0)),
                crate::geometry::MultiMember::Value(Point::new_raw_xy(1.0, 3.0)),
            ])
            .unwrap(),
        );
        let (_, allocations) = crate::test_support::allocation_count(|| {
            shape
                .minimum_bounding_circle()
                .expect("finite circle polygon")
        });
        assert!(allocations > 0, "circle emission must allocate");
        let error =
            crate::test_support::fail_allocations_after(0, || shape.minimum_bounding_circle())
                .expect_err("the first rejected allocation must remain a native error");
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    #[test]
    fn minimum_circle_emission_propagates_allocation_prefix_failures() {
        let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
        let circle = smallest_enclosing_circle(&source).expect("diameter support");
        let disk = circle.certified_disk(&source).expect("exact disk");
        let (_, allocations) = crate::test_support::allocation_count(|| {
            emit_minimum_bounding_circle(&disk, NonZeroU32::new(16).expect("non-zero"))
                .expect("finite circle polygon")
        });
        assert!(allocations > 0, "circle emission must allocate");
        for allowed in 0..allocations.min(16) {
            let error = crate::test_support::fail_allocations_after(allowed, || {
                emit_minimum_bounding_circle(&disk, NonZeroU32::new(16).expect("non-zero"))
            })
            .expect_err("a rejected emission allocation must remain a native error");
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn minimum_circle_certified_disk_propagates_allocation_failures() {
        let scale = 2.0_f64.powi(200);
        let low_bit = 2.0_f64.powi(148);
        let points = [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(4.0 * scale, 0.0),
            Point::new_raw_xy(scale + low_bit, 3.0 * scale),
        ];
        let circle = MinimumCircle::new_three(0, 1, 2);
        assert!(circle.certificate(&points).expect("support certificate"));
        let (_, allocations) =
            crate::test_support::allocation_count(|| circle.certified_disk(&points));
        assert!(allocations > 0, "exact disk construction must allocate");
        for allowed in 0..allocations {
            let error = crate::test_support::fail_allocations_after(allowed, || {
                circle.certified_disk(&points)
            })
            .expect_err("a rejected disk allocation must remain a native error");
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn minimum_circle_edge_certificate_rejects_an_inscribed_source_passing_ring() {
        let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
        let circle = smallest_enclosing_circle(&source).expect("diameter support");
        let disk = circle.certified_disk(&source).expect("exact disk");
        let mut budget = ExpansionBudget::new("minimum_bounding_circle", "quadrant_segments");
        let output = crate::geometry::constructive::point_buffer(
            disk.center().expect("finite center"),
            1.0 + 1.0e-6,
            NonZeroU32::new(16).expect("non-zero"),
            &mut budget,
        )
        .expect("inscribed ring construction")
        .expect("ring");
        let Shape::Polygon(polygon) = &output else {
            panic!("point buffer must emit a polygon");
        };
        let shell = polygon.rings().next().expect("shell");
        let vertices: Vec<_> = shell
            .coords()
            .iter_coords()
            .map(|point| point.xy())
            .collect();
        let open = &vertices[..vertices.len() - 1];
        assert!(
            open.iter().enumerate().all(|(index, &left)| {
                let right = open[(index + 1) % open.len()];
                source.iter().all(|point| {
                    exact::try_orient2d(left, right, point.xy()).expect("source-side predicate")
                        != ExactSign::Negative
                })
            }),
            "the old source-point-only check accepts this ring"
        );
        assert!(
            certify_minimum_circle_polygon(&output, &disk, NonZeroU32::new(16).expect("non-zero"))
                .is_err(),
            "the exact disk edge check must reject its inward chords"
        );
    }

    #[test]
    fn minimum_circle_certificate_rejects_an_inward_vertex_mutation() {
        let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
        let circle = smallest_enclosing_circle(&source).expect("diameter support");
        let disk = circle.certified_disk(&source).expect("exact disk");
        let valid = emit_minimum_bounding_circle(&disk, NonZeroU32::new(16).expect("non-zero"))
            .expect("valid ring");
        let Shape::Polygon(polygon) = valid else {
            panic!("minimum circle must emit a polygon");
        };
        let shell = polygon.rings().next().expect("shell");
        let mut vertices: Vec<_> = shell.coords().iter_coords().collect();
        vertices[0] = Point::new_raw_xy(0.5, 0.0);
        let mutated = Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(&vertices).expect("mutated ring")),
                Vec::new(),
            )
            .expect("polygon"),
        );
        assert!(
            certify_minimum_circle_polygon(&mutated, &disk, NonZeroU32::new(16).expect("non-zero"))
                .is_err(),
            "an inward vertex must fail the exact disk certificate"
        );
    }

    #[test]
    fn minimum_circle_emission_keeps_the_requested_quadrant_count() {
        let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
        let circle = smallest_enclosing_circle(&source).expect("diameter support");
        let disk = circle.certified_disk(&source).expect("exact disk");
        for quadrants in [1, 2, 16] {
            let output =
                emit_minimum_bounding_circle(&disk, NonZeroU32::new(quadrants).expect("non-zero"))
                    .expect("representable tangent polygon");
            let Shape::Polygon(polygon) = output else {
                panic!("minimum circle must emit a polygon");
            };
            assert_eq!(
                polygon.rings().next().expect("shell").coords().len(),
                quadrants as usize * 4 + 1
            );
        }
    }

    #[test]
    fn minimum_circle_emission_retries_when_rounded_radius_is_below_tangent() {
        let source = [Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 5.0)];
        let circle = smallest_enclosing_circle(&source).expect("diameter support");
        let disk = circle.certified_disk(&source).expect("exact disk");
        let quadrants = NonZeroU32::new(16).expect("non-zero");
        let mut budget = ExpansionBudget::new("minimum_bounding_circle", "quadrant_segments");
        let candidate = tangent_circle_polygon(
            disk.center().expect("finite center"),
            disk.radius().expect("finite radius"),
            quadrants,
            &mut budget,
        )
        .expect("candidate polygon");
        assert!(
            certify_minimum_circle_polygon(&candidate, &disk, quadrants).is_err(),
            "the rounded-down radius must expose an inward tangent edge"
        );
        let emitted = emit_minimum_bounding_circle(&disk, quadrants).expect("outward radius retry");
        certify_minimum_circle_polygon(&emitted, &disk, quadrants)
            .expect("retry must publish a certified ring");
    }

    #[test]
    fn minimum_circle_polygon_rejects_a_collapsed_output_ring() {
        let points = [
            Point::new_raw_xy(2.0_f64.powi(60), 2.0_f64.powi(60)),
            Point::new_raw_xy(2.0_f64.powi(60) + 1024.0, 2.0_f64.powi(60)),
            Point::new_raw_xy(2.0_f64.powi(60) + 256.0, 2.0_f64.powi(60) + 768.0),
        ];
        let shape = Shape::MultiPoint(
            crate::geometry::MultiPoint::try_new(
                crate::geometry::CoordinateAxes::XY,
                points
                    .into_iter()
                    .map(crate::geometry::MultiMember::Value)
                    .collect(),
            )
            .unwrap(),
        );
        let error = shape.minimum_bounding_circle().unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(crate::geometry::GeometryErrorKind::UnrepresentableOutput {
                operation: "minimum_bounding_circle"
            })
        ));
    }

    #[test]
    fn minimum_circle_polygon_maps_nonfinite_source_to_unrepresentable_output() {
        let error = Shape::Point(Point::new_raw_xy(f64::INFINITY, 0.0))
            .minimum_bounding_circle()
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Geometry(crate::geometry::GeometryErrorKind::UnrepresentableOutput {
                operation: "minimum_bounding_circle"
            })
        ));
    }

    #[test]
    fn minimum_circle_support_matches_independent_boundary_cases() {
        let cases = [
            (
                vec![
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(4.0, 0.0),
                    Point::new_raw_xy(1.0, 3.0),
                ],
                vec![XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(1.0, 3.0)],
            ),
            (
                vec![
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(4.0, 0.0),
                    Point::new_raw_xy(1.0, 0.5),
                ],
                vec![XY::new(0.0, 0.0), XY::new(4.0, 0.0)],
            ),
            (
                vec![
                    Point::new_raw_xy(-3.0, -3.0),
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(4.0, 4.0),
                    Point::new_raw_xy(1.0, 1.0),
                ],
                vec![XY::new(-3.0, -3.0), XY::new(4.0, 4.0)],
            ),
        ];
        for (points, mut expected) in cases {
            let circle = smallest_enclosing_circle(&points).unwrap();
            let (support, count) = circle.support_xy(&points);
            let mut actual = support[..count].to_vec();
            actual.sort_by(|left, right| {
                left.x
                    .total_cmp(&right.x)
                    .then_with(|| left.y.total_cmp(&right.y))
            });
            expected.sort_by(|left, right| {
                left.x
                    .total_cmp(&right.x)
                    .then_with(|| left.y.total_cmp(&right.y))
            });
            assert_eq!(actual, expected);
        }
    }

    fn shuffle_with_seed(length: usize, seed: u64) -> Vec<usize> {
        let mut state = seed ^ length as u64;
        let mut order: Vec<usize> = (0..length).collect();
        for index in (1..length).rev() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            order.swap(index, (state >> 33) as usize % (index + 1));
        }
        order
    }

    fn fixed_shuffle(length: usize) -> Vec<usize> {
        shuffle_with_seed(length, 0x9E37_79B9_7F4A_7C15)
    }

    #[test]
    fn minimum_circle_work_counters_cover_normal_and_inverse_fixed_shuffle_orders() {
        // These thresholds characterize this fixed parabola fixture: they
        // document the old fixed-shuffle adversary without making a universal
        // speed claim about the randomized algorithm.
        let length = 64;
        let source: Vec<_> = (0..length)
            .map(|index| {
                let x = index as f64;
                Point::new_raw_xy(x, x * x)
            })
            .collect();
        let ordinary_order = shuffle_with_seed(length, 0xD1B5_4A32_D192_ED03);
        let mut ordinary_work = CircleWork::default();
        let ordinary =
            solve_enclosing_circle_order(&source, &ordinary_order, &mut ordinary_work).unwrap();

        let permutation = fixed_shuffle(length);
        let mut inverse_source = vec![Point::new_raw_xy(0.0, 0.0); length];
        for (after, &before) in permutation.iter().enumerate() {
            inverse_source[before] = source[after];
        }
        let mut adversarial_work = CircleWork::default();
        let adversarial =
            solve_enclosing_circle_order(&inverse_source, &permutation, &mut adversarial_work)
                .unwrap();

        assert_eq!(
            ordinary
                .certified_disk(&source)
                .unwrap()
                .radius()
                .unwrap()
                .to_bits(),
            adversarial
                .certified_disk(&inverse_source)
                .unwrap()
                .radius()
                .unwrap()
                .to_bits()
        );
        assert!(ordinary_work.contains > 0);
        assert!(adversarial_work.contains > 0);
        assert!(ordinary_work.contains < length * length / 4);
        assert!(adversarial_work.contains > length * length / 4);
        assert!(ordinary_work.one_supports > 0);
        assert!(ordinary_work.two_supports > 0);

        let three_source = [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(4.0, 0.0),
            Point::new_raw_xy(1.0, 3.0),
        ];
        let mut three_work = CircleWork::default();
        solve_enclosing_circle_order(&three_source, &[0, 1, 2], &mut three_work).unwrap();
        assert!(three_work.three_supports > 0);
    }
}
