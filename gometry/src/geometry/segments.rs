#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![expect(
    clippy::self_named_module_files,
    reason = "the cohesive segment kernel predates its exact-arithmetic child module; a mechanical file move adds no ownership boundary"
)]
//! The shared segment-geometry kernel: nearest-point, segment intersection /
//! projection, and interpolation primitives used across distance, overlay,
//! linear-referencing, and predicate modules.

use crate::geometry::tessellation::exact::{ExactDyadic, ExactSign, compare_ratios, round_ratio};
use crate::geometry::{
    Bounds, Coordinates, MOrdinate, Ordering, Point, PointKey, Segment, Shape, XY, ZOrdinate,
    axis_pow2_scale, same_point, same_topological_coordinate, scaled_residual,
};

mod exact;

pub(crate) use exact::orientation;
pub(super) use exact::{CCW_ERRBOUND_A, orientation_xy, ray_crossing_is_right};
use exact::{
    certified_projection, dd_cross, dd_diff, dd_div, exact_power_of_two, segment_pair_scale_shifts,
};

fn projection_difference(left: f64, right: f64) -> ExactDyadic {
    ExactDyadic::from_f64(left).subtract(ExactDyadic::from_f64(right))
}

/// Filtered dot product used only to certify the allocation-free common
/// projection path. An unresolved sign or range error declines to exact
/// stored-dyadic arithmetic.
#[derive(Clone, Copy)]
struct ProjectionDot {
    value: f64,
    radius: f64,
    range_certified: bool,
    left: f64,
    right: f64,
}

impl ProjectionDot {
    const fn sign(self) -> Option<Ordering> {
        if self.value > self.radius {
            Some(Ordering::Greater)
        } else if self.value < -self.radius {
            Some(Ordering::Less)
        } else {
            None
        }
    }

    const fn products_are_safe(self, a: f64, b: f64, c: f64, d: f64) -> bool {
        let left_is_exact_zero = projection_binary_zero(a) || projection_binary_zero(b);
        let right_is_exact_zero = projection_binary_zero(c) || projection_binary_zero(d);
        let left_is_safe = if left_is_exact_zero {
            projection_binary_zero(self.left)
        } else {
            self.left.is_normal()
        };
        let right_is_safe = if right_is_exact_zero {
            projection_binary_zero(self.right)
        } else {
            self.right.is_normal()
        };
        left_is_safe && right_is_safe
    }

    const fn terms_do_not_cancel(self, a: f64, b: f64, c: f64, d: f64) -> bool {
        projection_binary_zero(a)
            || projection_binary_zero(b)
            || projection_binary_zero(c)
            || projection_binary_zero(d)
            || self.left.is_sign_positive() == self.right.is_sign_positive()
    }

    /// Filter `(a * b) + (c * d)`, where every operand is a finite result of
    /// one stored-binary64 subtraction. Five rounded operations feed the dot;
    /// the `gamma(5)`-dominating radius covers their normal error, while eight
    /// minimum subnormals cover every product/sum range-error rounding. This
    /// is a proof bound, not a scale cutoff: overflow or an unresolved sign
    /// declines, and `range_certified` separately refuses a ratio whose
    /// significant bits could have been consumed by gradual underflow.
    fn new(a: f64, b: f64, c: f64, d: f64) -> Self {
        const UNIT_ROUNDOFF: f64 = f64::EPSILON / 2.0;
        const DOT_ERRBOUND: f64 = (8.0 + 64.0 * UNIT_ROUNDOFF) * UNIT_ROUNDOFF;
        const RANGE_ERROR: f64 = 8.0 * f64::from_bits(1);
        let left = a * b;
        let right = c * d;
        let value = left + right;
        let ordinary_radius = (left.abs() + right.abs()) * DOT_ERRBOUND;
        Self {
            value,
            radius: ordinary_radius + RANGE_ERROR,
            range_certified: ordinary_radius >= RANGE_ERROR,
            left,
            right,
        }
    }
}

const fn projection_binary_zero(value: f64) -> bool {
    value.to_bits().trailing_zeros() >= 63
}

#[derive(Clone, Copy)]
struct FilteredInteriorProjection {
    fraction: f64,
    terms_do_not_cancel: bool,
}

#[derive(Clone, Copy)]
enum ClassifiedSegmentProjection {
    Start,
    FilteredInterior(FilteredInteriorProjection),
    End,
    Declined,
}

enum ExactClassifiedSegmentProjection {
    Start,
    Interior(ExactProjectionTerms),
    End,
}

#[derive(Clone, Debug)]
pub(crate) struct ExactSegmentProjection {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
    fraction: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CertifiedSegmentProjection {
    point: XY,
    segment: Segment,
    fraction: f64,
    foot: XY,
}

impl CertifiedSegmentProjection {
    fn exact(self) -> ExactSegmentProjection {
        exact_interior_projection(self.point, self.segment)
    }
}

impl ExactSegmentProjection {
    fn interpolate(&self, start: f64, end: f64) -> f64 {
        let numerator = ExactDyadic::from_f64(start)
            .product(&self.denominator)
            .add(projection_difference(end, start).product(&self.numerator));
        round_ratio(&numerator, &self.denominator)
            .expect("a convex interpolation of finite coordinates is finite")
    }

    fn scale(&self, value: f64) -> f64 {
        let numerator = ExactDyadic::from_f64(value).product(&self.numerator);
        round_ratio(&numerator, &self.denominator)
            .expect("scaling a finite value by a projection fraction is finite")
    }
}

/// One certified clamped projection. The provenance-carrying variants retain
/// their ratio until coordinate and along-distance reconstruction; a rounded
/// `t == 0` or `t == 1` therefore cannot turn a genuine interior foot into an
/// endpoint.
#[derive(Clone, Debug)]
pub(crate) enum SegmentProjection {
    Start,
    Interior(f64),
    CertifiedInterior(CertifiedSegmentProjection),
    ExactInterior(Box<ExactSegmentProjection>),
    End,
}

impl SegmentProjection {
    pub(super) const fn is_start(&self) -> bool {
        matches!(self, Self::Start)
    }

    pub(super) const fn is_end(&self) -> bool {
        matches!(self, Self::End)
    }

    pub(super) const fn uses_exact_ratio(&self) -> bool {
        matches!(self, Self::CertifiedInterior(_) | Self::ExactInterior(_))
    }

    pub(super) fn fraction(&self) -> f64 {
        match self {
            Self::Start => 0.0,
            Self::Interior(fraction) => *fraction,
            Self::CertifiedInterior(projection) => projection.fraction,
            Self::ExactInterior(projection) => projection.fraction,
            Self::End => 1.0,
        }
    }

    pub(super) fn cmp_along(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Start, Self::Start) | (Self::End, Self::End) => Ordering::Equal,
            (Self::Start, _) | (_, Self::End) => Ordering::Less,
            (Self::End, _) | (_, Self::Start) => Ordering::Greater,
            (Self::Interior(left), Self::Interior(right)) => left.total_cmp(right),
            (Self::CertifiedInterior(left), Self::CertifiedInterior(right)) => {
                let left = left.exact();
                let right = right.exact();
                compare_ratios(
                    &left.numerator,
                    &left.denominator,
                    &right.numerator,
                    &right.denominator,
                )
            },
            (Self::CertifiedInterior(left), Self::ExactInterior(right)) => {
                let left = left.exact();
                compare_ratios(
                    &left.numerator,
                    &left.denominator,
                    &right.numerator,
                    &right.denominator,
                )
            },
            (Self::ExactInterior(left), Self::CertifiedInterior(right)) => {
                let right = right.exact();
                compare_ratios(
                    &left.numerator,
                    &left.denominator,
                    &right.numerator,
                    &right.denominator,
                )
            },
            (Self::ExactInterior(left), Self::ExactInterior(right)) => compare_ratios(
                &left.numerator,
                &left.denominator,
                &right.numerator,
                &right.denominator,
            ),
            (Self::ExactInterior(left), Self::Interior(right)) => compare_ratios(
                &left.numerator,
                &left.denominator,
                &ExactDyadic::from_f64(*right),
                &ExactDyadic::from_f64(1.0),
            ),
            (Self::Interior(left), Self::ExactInterior(right)) => compare_ratios(
                &ExactDyadic::from_f64(*left),
                &ExactDyadic::from_f64(1.0),
                &right.numerator,
                &right.denominator,
            ),
            (Self::CertifiedInterior(left), Self::Interior(right)) => {
                let left = left.exact();
                compare_ratios(
                    &left.numerator,
                    &left.denominator,
                    &ExactDyadic::from_f64(*right),
                    &ExactDyadic::from_f64(1.0),
                )
            },
            (Self::Interior(left), Self::CertifiedInterior(right)) => {
                let right = right.exact();
                compare_ratios(
                    &ExactDyadic::from_f64(*left),
                    &ExactDyadic::from_f64(1.0),
                    &right.numerator,
                    &right.denominator,
                )
            },
        }
    }

    pub(super) fn interpolate_xy(&self, segment: Segment) -> XY {
        match self {
            Self::Start => segment.start,
            Self::End => segment.end,
            Self::Interior(fraction) => {
                interpolate_segment_point(segment.start, segment.end, *fraction)
            },
            Self::CertifiedInterior(projection) => projection.foot,
            Self::ExactInterior(projection) => XY::new(
                projection.interpolate(segment.start.x, segment.end.x),
                projection.interpolate(segment.start.y, segment.end.y),
            ),
        }
    }

    pub(super) fn interpolate_point(&self, start: Point, end: Point) -> Point {
        let exact;
        let projection = match self {
            Self::ExactInterior(projection) => projection.as_ref(),
            Self::CertifiedInterior(certified) => {
                exact = certified.exact();
                &exact
            },
            _ => return lerp_point(start, end, self.fraction()),
        };
        let interpolate_optional = |start: Option<f64>, end: Option<f64>| match (start, end) {
            (Some(start), Some(end)) => Some(projection.interpolate(start, end)),
            _ => None,
        };
        Point::new_axes(
            projection.interpolate(start.x, end.x),
            projection.interpolate(start.y, end.y),
            ZOrdinate(interpolate_optional(start.z(), end.z())),
            MOrdinate(interpolate_optional(start.m(), end.m())),
        )
        .unwrap_or(if projection.fraction <= 0.5 {
            start
        } else {
            end
        })
    }

    pub(super) fn scale(&self, value: f64) -> f64 {
        match self {
            Self::Start => 0.0,
            Self::Interior(fraction) => value * fraction,
            Self::CertifiedInterior(projection) => projection.exact().scale(value),
            Self::ExactInterior(projection) => projection.scale(value),
            Self::End => value,
        }
    }
}

pub(super) fn nearest_points(left: &Shape, right: &Shape) -> Option<(Point, Point)> {
    let mut nearest = NearestPoints::default();
    // Vertex-pair lists are hoisted once per side; this lane carries FULL
    // ordinates so witness feet interpolate Z/M like the vertices they
    // sit between (the planar `Segment` is XY-only by design).
    let mut left_pairs: Vec<(Point, Point)> = Vec::with_capacity(left.segment_count());
    left.for_each_vertex_pair(|start, end| left_pairs.push((start, end)));
    let mut right_pairs: Vec<(Point, Point)> = Vec::with_capacity(right.segment_count());
    right.for_each_vertex_pair(|start, end| right_pairs.push((start, end)));

    let mut missing = false;
    left.for_each_point(
        |point| match nearest_point_with_segments(point, right, &right_pairs) {
            Some(candidate) => nearest.add(point, candidate.point, &candidate.distance_key),
            None => missing = true,
        },
    );
    right.for_each_point(
        |point| match nearest_point_with_segments(point, left, &left_pairs) {
            Some(candidate) => nearest.add(candidate.point, point, &candidate.distance_key),
            None => missing = true,
        },
    );
    if missing {
        return None;
    }
    for left_pair in &left_pairs {
        for right_pair in &right_pairs {
            let candidate = pair_nearest_points(*left_pair, *right_pair);
            nearest.add(candidate.left, candidate.right, &candidate.distance_key);
        }
    }

    nearest.points
}

/// [`segment_nearest_points`] over full-ordinate vertex pairs: the planar
/// kernel finds the XY witnesses, and each foot lifts back through its
/// host pair so Z/M interpolate.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn pair_nearest_points(left: (Point, Point), right: (Point, Point)) -> FullPairNearest {
    let left_xy = Segment {
        start: left.0.xy(),
        end: left.1.xy(),
    };
    let right_xy = Segment {
        start: right.0.xy(),
        end: right.1.xy(),
    };
    let nearest = segment_pair_nearest(left_xy, right_xy);
    FullPairNearest {
        left: nearest.left.interpolate_point(&left, left_xy),
        right: nearest.right.interpolate_point(&right, right_xy),
        distance_key: nearest.distance_key,
    }
}

#[derive(Clone, Debug)]
enum SegmentWitness {
    Start,
    Projection(SegmentProjection),
    End,
    RoundedIntersection(XY),
}

impl SegmentWitness {
    #[cfg(test)]
    fn xy(&self, segment: Segment) -> XY {
        match self {
            Self::Start => segment.start,
            Self::Projection(projection) => projection.interpolate_xy(segment),
            Self::End => segment.end,
            Self::RoundedIntersection(point) => *point,
        }
    }

    fn interpolate_point(&self, pair: &(Point, Point), segment: Segment) -> Point {
        match self {
            Self::Start => pair.0,
            Self::Projection(projection) => projection.interpolate_point(pair.0, pair.1),
            Self::End => pair.1,
            Self::RoundedIntersection(point) => {
                segment_projection(*point, segment).interpolate_point(pair.0, pair.1)
            },
        }
    }
}

struct SegmentPairNearest {
    left: SegmentWitness,
    right: SegmentWitness,
    distance_key: SquaredDistanceKey,
}

struct FullPairNearest {
    left: Point,
    right: Point,
    distance_key: SquaredDistanceKey,
}

#[derive(Default)]
pub(super) struct NearestPoints {
    points: Option<(Point, Point)>,
    distance_key: Option<SquaredDistanceKey>,
}

impl NearestPoints {
    fn add(&mut self, left: Point, right: Point, distance_key: &SquaredDistanceKey) {
        if self
            .distance_key
            .is_none_or(|incumbent| distance_key.cmp(&incumbent).is_lt())
        {
            self.points = Some((left, right));
            self.distance_key = Some(*distance_key);
        }
    }
}

struct NearestPointResult {
    point: Point,
    distance_key: SquaredDistanceKey,
}

/// Nearest point on `geometry` to `point`, with the geometry's segments
/// precomputed so batch callers hoist the segment gather out of their
/// per-vertex loop.
fn nearest_point_with_segments(
    point: Point,
    geometry: &Shape,
    pairs: &[(Point, Point)],
) -> Option<NearestPointResult> {
    if geometry.contains_point(point) {
        return Some(NearestPointResult {
            point,
            distance_key: point_pair_distance_key(point.xy(), point.xy()),
        });
    }

    let mut nearest = None;
    let mut best_key = None;
    for pair in pairs {
        let segment = Segment {
            start: pair.0.xy(),
            end: pair.1.xy(),
        };
        let selection = point_segment_selection(point.xy(), segment);
        if best_key.is_none_or(|incumbent| selection.cmp(point.xy(), segment, &incumbent).is_lt()) {
            nearest = Some(
                selection
                    .projection(point.xy(), segment)
                    .interpolate_point(pair.0, pair.1),
            );
            best_key = Some(selection.distance_key(point.xy(), segment));
        }
    }
    geometry.for_each_point(|candidate| {
        let distance_key = point_pair_distance_key(point.xy(), candidate.xy());
        if best_key.is_none_or(|incumbent| distance_key.cmp(&incumbent).is_lt()) {
            nearest = Some(candidate);
            best_key = Some(distance_key);
        }
    });
    nearest
        .zip(best_key)
        .map(|(point, distance_key)| NearestPointResult {
            point,
            distance_key,
        })
}

#[expect(
    clippy::too_many_lines,
    reason = "axis-aligned clamp + diagonal edge-bisection share one clip contract"
)]
pub(super) fn clip_segment(
    start: impl Into<XY>,
    end: impl Into<XY>,
    rect: Bounds,
) -> Option<(XY, XY)> {
    let (start, end) = (start.into(), end.into());
    // Axis-aligned clamp: when the free span dwarfs the rect (±1e20 vs unit
    // box), parametric t for both edges collapses to the same value and
    // edge-hit reconstruction degenerates. Clamping the free ordinate keeps
    // exact window coordinates. Bit-exact axis equality (not epsilon).
    if same_topological_coordinate(start.y, end.y) {
        let y = start.y;
        if y < rect.miny() || y > rect.maxy() {
            return None;
        }
        let (lo, hi) = (start.x.min(end.x), start.x.max(end.x));
        if hi < rect.minx() || lo > rect.maxx() {
            return None;
        }
        let x0 = lo.max(rect.minx());
        let x1 = hi.min(rect.maxx());
        if same_topological_coordinate(x0, x1) {
            return None;
        }
        return if start.x <= end.x {
            Some((XY::new(x0, y), XY::new(x1, y)))
        } else {
            Some((XY::new(x1, y), XY::new(x0, y)))
        };
    }
    if same_topological_coordinate(start.x, end.x) {
        let x = start.x;
        if x < rect.minx() || x > rect.maxx() {
            return None;
        }
        let (lo, hi) = (start.y.min(end.y), start.y.max(end.y));
        if hi < rect.miny() || lo > rect.maxy() {
            return None;
        }
        let y0 = lo.max(rect.miny());
        let y1 = hi.min(rect.maxy());
        if same_topological_coordinate(y0, y1) {
            return None;
        }
        return if start.y <= end.y {
            Some((XY::new(x, y0), XY::new(x, y1)))
        } else {
            Some((XY::new(x, y1), XY::new(x, y0)))
        };
    }
    // Diagonal: parameterize intersections on the SHORT rect edges, not on
    // the segment. When |segment| ≫ |rect|, t=(edge-start)/dx collapses both
    // boundaries to the same value and loses the window; bisection on a unit
    // edge with robust orient2d stays accurate.
    let corners = [
        XY::new(rect.minx(), rect.miny()),
        XY::new(rect.maxx(), rect.miny()),
        XY::new(rect.maxx(), rect.maxy()),
        XY::new(rect.minx(), rect.maxy()),
    ];
    let edges = [
        (corners[0], corners[1]),
        (corners[1], corners[2]),
        (corners[2], corners[3]),
        (corners[3], corners[0]),
    ];
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    let mut hits: [(f64, XY); 6] = [(0.0, start); 6];
    let mut n_hits = 0_usize;
    // Order hits on the dominant monotone coordinate so huge diagonal segments
    // (e.g. ±2^53 through the unit box) do not cancel the projection key to
    // the same value for distinct edge hits.
    let abs_dx = dx.abs();
    let abs_dy = dy.abs();
    let push = |p: XY, hits: &mut [(f64, XY); 6], n: &mut usize| {
        if *n >= hits.len() {
            return;
        }
        if hits[..*n].iter().any(|&(_, q)| {
            same_topological_coordinate(q.x, p.x) && same_topological_coordinate(q.y, p.y)
        }) {
            return;
        }
        // Sort by the dominant world ordinate (not a parametric t). For a
        // huge diagonal, `t = (p - origin)·d / |d|²` and even `(p.x-ox)/dx`
        // collapse distinct unit-box hits to the same value once |origin|
        // exceeds the box (1 lost against 1e20). The monotone coordinate
        // keeps (0,0) and (1,1) ordered.
        let key = if abs_dx >= abs_dy {
            if dx >= 0.0 { p.x } else { -p.x }
        } else if dy >= 0.0 {
            p.y
        } else {
            -p.y
        };
        hits[*n] = (key, p);
        *n += 1;
    };
    let point_in = |p: XY| {
        p.x >= rect.minx() && p.x <= rect.maxx() && p.y >= rect.miny() && p.y <= rect.maxy()
    };
    if point_in(start) {
        push(start, &mut hits, &mut n_hits);
    }
    if point_in(end) {
        push(end, &mut hits, &mut n_hits);
    }
    for (a, b) in edges {
        if let Some(p) = segment_rect_edge_hit(start, end, a, b) {
            // Hit must also lie on the segment's bounding range.
            let on_seg = p.x >= start.x.min(end.x)
                && p.x <= start.x.max(end.x)
                && p.y >= start.y.min(end.y)
                && p.y <= start.y.max(end.y);
            if on_seg {
                push(p, &mut hits, &mut n_hits);
            }
        }
    }
    if n_hits < 2 {
        return None;
    }
    let (mut i_min, mut i_max) = (0_usize, 0_usize);
    for i in 1..n_hits {
        if hits[i].0 < hits[i_min].0 {
            i_min = i;
        }
        if hits[i].0 > hits[i_max].0 {
            i_max = i;
        }
    }
    let (p_min, p_max) = (hits[i_min].1, hits[i_max].1);
    if same_point(p_min, p_max) {
        return None;
    }
    Some((p_min, p_max))
}

/// Intersection of the infinite line `start→end` with the short rect edge
/// `edge_a→edge_b`, found by bisection on the edge (unit-scale) using robust
/// orientation. Returns `None` if the edge is not crossed.
fn segment_rect_edge_hit(start: XY, end: XY, edge_a: XY, edge_b: XY) -> Option<XY> {
    let o_a = orientation(start, end, edge_a);
    let o_b = orientation(start, end, edge_b);
    // No crossing if both strictly on the same side.
    match (o_a, o_b) {
        (Orientation::Clockwise, Orientation::Clockwise)
        | (Orientation::CounterClockwise, Orientation::CounterClockwise) => return None,
        (Orientation::Collinear, _) => return Some(edge_a),
        (_, Orientation::Collinear) => return Some(edge_b),
        _ => {},
    }
    // Bisect the short edge until the midpoint is collinear (or max iters).
    let mut lo = edge_a;
    let mut hi = edge_b;
    let mut o_lo = o_a;
    for _ in 0..64 {
        let mid = XY::new(f64::midpoint(lo.x, hi.x), f64::midpoint(lo.y, hi.y));
        match orientation(start, end, mid) {
            Orientation::Collinear => return Some(mid),
            o if o == o_lo => {
                lo = mid;
                o_lo = o;
            },
            _ => hi = mid,
        }
    }
    Some(XY::new(
        f64::midpoint(lo.x, hi.x),
        f64::midpoint(lo.y, hi.y),
    ))
}

#[cfg(test)]
pub(super) fn segment_nearest_points(left: Segment, right: Segment) -> (XY, XY) {
    let nearest = segment_pair_nearest(left, right);
    (nearest.left.xy(left), nearest.right.xy(right))
}

fn segment_pair_nearest(left: Segment, right: Segment) -> SegmentPairNearest {
    // One contact computation, not an `intersects` probe followed by a
    // separate placement. When the pair meets, the shared point IS the
    // nearest pair; otherwise fall through to the projection feet below.
    if let Some(point) = segment_contact_point(left, right) {
        return SegmentPairNearest {
            left: SegmentWitness::RoundedIntersection(point),
            right: SegmentWitness::RoundedIntersection(point),
            distance_key: point_pair_distance_key(point, point),
        };
    }

    let left_start = segment_projection(left.start, right);
    let left_end = segment_projection(left.end, right);
    let right_start = segment_projection(right.start, left);
    let right_end = segment_projection(right.end, left);
    let candidates = [
        SegmentPairNearest {
            left: SegmentWitness::Start,
            right: SegmentWitness::Projection(left_start),
            distance_key: point_segment_distance_key(left.start, right),
        },
        SegmentPairNearest {
            left: SegmentWitness::End,
            right: SegmentWitness::Projection(left_end),
            distance_key: point_segment_distance_key(left.end, right),
        },
        SegmentPairNearest {
            left: SegmentWitness::Projection(right_start),
            right: SegmentWitness::Start,
            distance_key: point_segment_distance_key(right.start, left),
        },
        SegmentPairNearest {
            left: SegmentWitness::Projection(right_end),
            right: SegmentWitness::End,
            distance_key: point_segment_distance_key(right.end, left),
        },
    ];
    let mut candidates = candidates.into_iter();
    let mut best = candidates.next().expect("four segment-pair candidates");
    for candidate in candidates {
        if candidate.distance_key.cmp(&best.distance_key).is_lt() {
            best = candidate;
        }
    }
    best
}

/// A point lying on BOTH segments, or `None` when they do not meet.
///
/// The one witness primitive. Existence and placement both come from
/// [`segment_contact_exact`], so every returned point is a real shared point:
/// a transversal crossing placed in double-double arithmetic, a touching
/// endpoint returned bit-exactly, or the near end of a collinear overlap run.
///
/// It returns `None` rather than fabricating. The predecessor
/// (`segment_intersection_point`) answered `left.start` for a zero
/// denominator and had no finiteness guard, so a caller could receive a point
/// on neither segment — or a non-finite one — with no way to tell. It backed
/// the public `nearest_points`/`shortest_line` witness.
pub(super) fn segment_contact_point(left: Segment, right: Segment) -> Option<XY> {
    match segment_contact_exact(left, right) {
        Contact::Cross { point } | Contact::Touch { point } => Some(point),
        // A positive-length overlap has no single crossing; its near end is a
        // real point of both segments and is the conventional witness.
        Contact::Collinear { span } => Some(span.start),
        Contact::Disjoint => None,
    }
}

/// Certified clamped point-to-segment projection over the complete finite
/// binary64 domain. The outward interval is an allocation-free proof for the
/// common case. Any unresolved overlap, underflow, overflow, or cancellation
/// falls through to the exact stored-dyadic numerator/denominator owner.
pub(super) fn segment_projection(point: impl Into<XY>, segment: Segment) -> SegmentProjection {
    let point = point.into();
    projection_from_classification(point, segment, classify_segment_projection(point, segment))
}

// This compact decide/decline filter is the projection hot path. Keeping its
// consumer-specific return handling in the caller removes the otherwise
// dominant per-segment enum call ABI; the exact fallback remains cold and
// out-of-line below.
#[expect(
    clippy::inline_always,
    reason = "measured specialization reduced the ordinary distance ABBA ratio from 1.3043x to 1.224192x"
)]
#[inline(always)]
fn classify_segment_projection(point: XY, segment: Segment) -> ClassifiedSegmentProjection {
    let dx = segment.end.x - segment.start.x;
    let dy = segment.end.y - segment.start.y;
    let qx = point.x - segment.start.x;
    let qy = point.y - segment.start.y;
    if dx.is_finite() && dy.is_finite() && qx.is_finite() && qy.is_finite() {
        let numerator = ProjectionDot::new(qx, dx, qy, dy);
        match numerator.sign() {
            Some(Ordering::Less) => return ClassifiedSegmentProjection::Start,
            Some(Ordering::Greater) => {},
            None | Some(Ordering::Equal) => {
                return ClassifiedSegmentProjection::Declined;
            },
        }

        let rx = segment.end.x - point.x;
        let ry = segment.end.y - point.y;
        if !(rx.is_finite() && ry.is_finite()) {
            return ClassifiedSegmentProjection::Declined;
        }
        let remainder = ProjectionDot::new(rx, dx, ry, dy);
        match remainder.sign() {
            Some(Ordering::Less) => return ClassifiedSegmentProjection::End,
            Some(Ordering::Greater) => {},
            None | Some(Ordering::Equal) => {
                return ClassifiedSegmentProjection::Declined;
            },
        }

        return classify_filtered_interior(segment, dx, dy, qx, qy, numerator);
    }

    ClassifiedSegmentProjection::Declined
}

/// Ratio and witness certification is reached only after both endpoint signs
/// have been decided as strictly interior. Keeping it out of the endpoint
/// classifier avoids carrying this larger proof through the distance loop's
/// overwhelmingly common start/end decisions.
#[inline(never)]
fn classify_filtered_interior(
    segment: Segment,
    dx: f64,
    dy: f64,
    qx: f64,
    qy: f64,
    numerator: ProjectionDot,
) -> ClassifiedSegmentProjection {
    let denominator = ProjectionDot::new(dx, dx, dy, dy);
    if numerator.range_certified
        && denominator.range_certified
        && numerator.products_are_safe(qx, dx, qy, dy)
        && denominator.products_are_safe(dx, dx, dy, dy)
    {
        let fraction = numerator.value / denominator.value;
        if fraction.is_finite() && fraction > 0.0 && fraction < 1.0 && fraction.is_normal() {
            let foot = interpolate_segment_point(segment.start, segment.end, fraction);
            if !same_point(foot, segment.start) && !same_point(foot, segment.end) {
                let terms_do_not_cancel = numerator.terms_do_not_cancel(qx, dx, qy, dy)
                    && denominator.terms_do_not_cancel(dx, dx, dy, dy);
                return ClassifiedSegmentProjection::FilteredInterior(FilteredInteriorProjection {
                    fraction,
                    terms_do_not_cancel,
                });
            }
        }
    }
    ClassifiedSegmentProjection::Declined
}

/// The interval-filter decline is intentionally out of line: keeping limb
/// construction and exact ratio rounding out of the ordinary projection's
/// codegen is material in the distance inner loop.
#[cold]
#[inline(never)]
fn classify_segment_projection_exact(
    point: XY,
    segment: Segment,
) -> ExactClassifiedSegmentProjection {
    let terms = exact_projection_terms(point, segment);
    if terms.denominator.is_zero() {
        return ExactClassifiedSegmentProjection::Start;
    }
    if terms.numerator.sign() != ExactSign::Positive {
        return ExactClassifiedSegmentProjection::Start;
    }
    if terms.numerator.cmp(&terms.denominator) != Ordering::Less {
        return ExactClassifiedSegmentProjection::End;
    }
    ExactClassifiedSegmentProjection::Interior(terms)
}

fn projection_from_classification(
    point: XY,
    segment: Segment,
    classification: ClassifiedSegmentProjection,
) -> SegmentProjection {
    let (terms, historical_fraction) = match classification {
        ClassifiedSegmentProjection::Start => return SegmentProjection::Start,
        ClassifiedSegmentProjection::End => return SegmentProjection::End,
        ClassifiedSegmentProjection::FilteredInterior(filtered) if filtered.terms_do_not_cancel => {
            return SegmentProjection::Interior(filtered.fraction);
        },
        ClassifiedSegmentProjection::FilteredInterior(filtered) => {
            if let Some((fraction, foot)) = certified_projection(point, segment, filtered.fraction)
            {
                if fraction.to_bits() == filtered.fraction.to_bits() {
                    return SegmentProjection::Interior(filtered.fraction);
                }
                return SegmentProjection::CertifiedInterior(CertifiedSegmentProjection {
                    point,
                    segment,
                    fraction,
                    foot,
                });
            }
            (
                exact_projection_terms(point, segment),
                Some(filtered.fraction),
            )
        },
        ClassifiedSegmentProjection::Declined => {
            match classify_segment_projection_exact(point, segment) {
                ExactClassifiedSegmentProjection::Start => return SegmentProjection::Start,
                ExactClassifiedSegmentProjection::End => return SegmentProjection::End,
                ExactClassifiedSegmentProjection::Interior(terms) => (terms, None),
            }
        },
    };
    let fraction = round_ratio(&terms.numerator, &terms.denominator)
        .expect("an interior projection fraction is finite");
    if historical_fraction.is_some_and(|historical| historical.to_bits() == fraction.to_bits()) {
        return SegmentProjection::Interior(fraction);
    }
    SegmentProjection::ExactInterior(Box::new(ExactSegmentProjection {
        numerator: terms.numerator,
        denominator: terms.denominator,
        fraction,
    }))
}

fn exact_interior_projection(point: XY, segment: Segment) -> ExactSegmentProjection {
    let terms = exact_projection_terms(point, segment);
    debug_assert!(!terms.denominator.is_zero());
    debug_assert_eq!(terms.numerator.sign(), ExactSign::Positive);
    debug_assert!(terms.numerator.cmp(&terms.denominator).is_lt());
    let fraction = round_ratio(&terms.numerator, &terms.denominator)
        .expect("an interior projection fraction is finite");
    ExactSegmentProjection {
        numerator: terms.numerator,
        denominator: terms.denominator,
        fraction,
    }
}

struct ExactProjectionTerms {
    qx: ExactDyadic,
    qy: ExactDyadic,
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

fn exact_projection_terms(point: XY, segment: Segment) -> ExactProjectionTerms {
    let dx = projection_difference(segment.end.x, segment.start.x);
    let dy = projection_difference(segment.end.y, segment.start.y);
    let qx = projection_difference(point.x, segment.start.x);
    let qy = projection_difference(point.y, segment.start.y);
    let numerator = qx.product(&dx).add(qy.product(&dy));
    let denominator = dx.square().add(dy.square());
    ExactProjectionTerms {
        qx,
        qy,
        numerator,
        denominator,
    }
}

struct ExactSquaredDistance {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

impl ExactSquaredDistance {
    fn interior(terms: &ExactProjectionTerms) -> Self {
        let point_squared = terms.qx.square().add(terms.qy.square());
        Self {
            numerator: point_squared
                .product(&terms.denominator)
                .subtract(terms.numerator.square()),
            denominator: terms.denominator.clone(),
        }
    }

    fn point_pair(left: XY, right: XY) -> Self {
        let dx = projection_difference(left.x, right.x);
        let dy = projection_difference(left.y, right.y);
        Self {
            numerator: dx.square().add(dy.square()),
            denominator: ExactDyadic::from_f64(1.0),
        }
    }

    fn cmp(&self, other: &Self) -> Ordering {
        compare_ratios(
            &self.numerator,
            &self.denominator,
            &other.numerator,
            &other.denominator,
        )
    }

    fn rounded_trustworthy(&self) -> Option<f64> {
        let rounded = round_ratio(&self.numerator, &self.denominator).ok()?;
        if self.numerator.is_zero() {
            Some(0.0)
        } else {
            rounded.is_normal().then_some(rounded)
        }
    }
}

enum PointSegmentMetric {
    Squared {
        measurement_squared: f64,
        selection_squared: f64,
        filter: SquaredDistanceFilter,
        witness_kind: PointSegmentWitnessKind,
        projection: SelectionProjectionSource,
    },
    Projection(SegmentProjection),
}

#[derive(Clone)]
enum SelectionProjectionSource {
    Classified(ClassifiedSegmentProjection),
    Ready(SegmentProjection),
}

pub(super) struct PointSegmentSelection {
    pub(super) distance_squared: f64,
    filter: SquaredDistanceFilter,
    witness_kind: PointSegmentWitnessKind,
    projection: SelectionProjectionSource,
}

#[derive(Clone, Copy)]
enum PointSegmentWitnessKind {
    Start,
    Interior,
    End,
}

impl PointSegmentSelection {
    pub(super) fn projection(&self, point: XY, segment: Segment) -> SegmentProjection {
        match &self.projection {
            SelectionProjectionSource::Classified(classification) => {
                projection_from_classification(point, segment, *classification)
            },
            SelectionProjectionSource::Ready(projection) => projection.clone(),
        }
    }

    fn witness(&self, point: XY, segment: Segment) -> ExactDistanceWitness {
        match self.witness_kind {
            PointSegmentWitnessKind::Start => ExactDistanceWitness::PointPair(point, segment.start),
            PointSegmentWitnessKind::Interior => ExactDistanceWitness::ProjectedPointPair(
                point,
                self.projection(point, segment).interpolate_xy(segment),
            ),
            PointSegmentWitnessKind::End => ExactDistanceWitness::PointPair(point, segment.end),
        }
    }

    pub(super) fn distance_key(&self, point: XY, segment: Segment) -> SquaredDistanceKey {
        self.filter.bind(self.witness(point, segment))
    }

    pub(super) fn cmp(
        &self,
        point: XY,
        segment: Segment,
        incumbent: &SquaredDistanceKey,
    ) -> Ordering {
        self.filter
            .disjoint_cmp(incumbent.filter)
            .unwrap_or_else(|| {
                exact_witness_cmp_with_identity(self.witness(point, segment), incumbent.witness)
            })
    }
}

/// The single projection owner classifies every clamped parameter before a
/// metric consumes it. Ordinary certified interiors retain the historical
/// normal squared key; exact/ambiguous projections and untrustworthy squared
/// norms retain projection provenance for exact or distance-space rescue.
#[expect(
    clippy::inline_always,
    reason = "measured specialization reduced the ordinary distance ABBA ratio from 1.3043x to 1.224192x"
)]
#[inline(always)]
fn point_segment_metric(point: XY, segment: Segment) -> PointSegmentMetric {
    let classification = classify_segment_projection(point, segment);
    if matches!(&classification, ClassifiedSegmentProjection::Start) {
        return try_point_distance_squared(point, segment.start).map_or(
            PointSegmentMetric::Projection(SegmentProjection::Start),
            |squared| PointSegmentMetric::Squared {
                measurement_squared: squared,
                selection_squared: squared,
                filter: SquaredDistanceFilter::point_pair(squared),
                witness_kind: PointSegmentWitnessKind::Start,
                projection: SelectionProjectionSource::Classified(classification),
            },
        );
    }
    if matches!(&classification, ClassifiedSegmentProjection::End) {
        return try_point_distance_squared(point, segment.end).map_or(
            PointSegmentMetric::Projection(SegmentProjection::End),
            |squared| PointSegmentMetric::Squared {
                measurement_squared: squared,
                selection_squared: squared,
                filter: SquaredDistanceFilter::point_pair(squared),
                witness_kind: PointSegmentWitnessKind::End,
                projection: SelectionProjectionSource::Classified(classification),
            },
        );
    }
    match classification {
        ClassifiedSegmentProjection::FilteredInterior(filtered) => {
            // Measure in the segment-start frame. Reconstructing the foot in
            // world coordinates and then subtracting it from `point` loses the
            // small residual at large coordinate bases (ordinary UTM is
            // enough): `(point - start) - t * (end - start)` keeps that
            // cancellation in the well-conditioned local frame.
            let dx = segment.end.x - segment.start.x;
            let dy = segment.end.y - segment.start.y;
            let qx = point.x - segment.start.x;
            let qy = point.y - segment.start.y;
            let ex = qx - filtered.fraction * dx;
            let ey = qy - filtered.fraction * dy;
            let squared = ex * ex + ey * ey;
            if squared_norm_is_trustworthy(squared, ex == 0.0 && ey == 0.0) {
                if filtered.terms_do_not_cancel {
                    // Measurement owns the continuum residual above; nearest
                    // selection owns the emitted binary64 witness. Keep their
                    // filters separate because an unrepresentable foot can
                    // make those two distances differ in the last ulps.
                    let emitted =
                        interpolate_segment_point(segment.start, segment.end, filtered.fraction);
                    let selection_dx = point.x - emitted.x;
                    let selection_dy = point.y - emitted.y;
                    let selection_squared =
                        selection_dx * selection_dx + selection_dy * selection_dy;
                    PointSegmentMetric::Squared {
                        measurement_squared: squared,
                        selection_squared,
                        filter: if squared_norm_is_trustworthy(
                            selection_squared,
                            selection_dx == 0.0 && selection_dy == 0.0,
                        ) {
                            SquaredDistanceFilter::point_pair(selection_squared)
                        } else {
                            SquaredDistanceFilter::unbounded()
                        },
                        witness_kind: PointSegmentWitnessKind::Interior,
                        projection: SelectionProjectionSource::Classified(classification),
                    }
                } else {
                    let projection = projection_from_classification(point, segment, classification);
                    let emitted = projection.interpolate_xy(segment);
                    let selection_dx = point.x - emitted.x;
                    let selection_dy = point.y - emitted.y;
                    let selection_squared =
                        selection_dx * selection_dx + selection_dy * selection_dy;
                    PointSegmentMetric::Squared {
                        measurement_squared: squared,
                        selection_squared,
                        filter: if squared_norm_is_trustworthy(
                            selection_squared,
                            selection_dx == 0.0 && selection_dy == 0.0,
                        ) {
                            SquaredDistanceFilter::point_pair(selection_squared)
                        } else {
                            SquaredDistanceFilter::unbounded()
                        },
                        witness_kind: PointSegmentWitnessKind::Interior,
                        projection: SelectionProjectionSource::Ready(projection),
                    }
                }
            } else {
                PointSegmentMetric::Projection(projection_from_classification(
                    point,
                    segment,
                    ClassifiedSegmentProjection::FilteredInterior(filtered),
                ))
            }
        },
        ClassifiedSegmentProjection::Declined => point_segment_metric_exact(point, segment),
        ClassifiedSegmentProjection::Start | ClassifiedSegmentProjection::End => unreachable!(),
    }
}

/// Exact classification and squared-ratio rounding are a decline path. They
/// stay out of the ordinary metric's frame so a certified endpoint/interior
/// does not carry heap-backed dyadic state through the distance inner loop.
#[cold]
#[inline(never)]
fn point_segment_metric_exact(point: XY, segment: Segment) -> PointSegmentMetric {
    match classify_segment_projection_exact(point, segment) {
        ExactClassifiedSegmentProjection::Start => try_point_distance_squared(point, segment.start)
            .map_or(
                PointSegmentMetric::Projection(SegmentProjection::Start),
                |squared| PointSegmentMetric::Squared {
                    measurement_squared: squared,
                    selection_squared: squared,
                    filter: SquaredDistanceFilter::unbounded(),
                    witness_kind: PointSegmentWitnessKind::Start,
                    projection: SelectionProjectionSource::Ready(SegmentProjection::Start),
                },
            ),
        ExactClassifiedSegmentProjection::End => try_point_distance_squared(point, segment.end)
            .map_or(
                PointSegmentMetric::Projection(SegmentProjection::End),
                |squared| PointSegmentMetric::Squared {
                    measurement_squared: squared,
                    selection_squared: squared,
                    filter: SquaredDistanceFilter::unbounded(),
                    witness_kind: PointSegmentWitnessKind::End,
                    projection: SelectionProjectionSource::Ready(SegmentProjection::End),
                },
            ),
        ExactClassifiedSegmentProjection::Interior(terms) => {
            let squared = ExactSquaredDistance::interior(&terms);
            let rounded = squared.rounded_trustworthy();
            let fraction = round_ratio(&terms.numerator, &terms.denominator)
                .expect("an interior projection fraction is finite");
            let projection = SegmentProjection::ExactInterior(Box::new(ExactSegmentProjection {
                numerator: terms.numerator,
                denominator: terms.denominator,
                fraction,
            }));
            match rounded {
                Some(measurement_squared) => {
                    let emitted = projection.interpolate_xy(segment);
                    let selection_dx = point.x - emitted.x;
                    let selection_dy = point.y - emitted.y;
                    let selection_squared =
                        selection_dx * selection_dx + selection_dy * selection_dy;
                    PointSegmentMetric::Squared {
                        measurement_squared,
                        selection_squared,
                        filter: if squared_norm_is_trustworthy(
                            selection_squared,
                            selection_dx == 0.0 && selection_dy == 0.0,
                        ) {
                            SquaredDistanceFilter::point_pair(selection_squared)
                        } else {
                            SquaredDistanceFilter::unbounded()
                        },
                        witness_kind: PointSegmentWitnessKind::Interior,
                        projection: SelectionProjectionSource::Ready(projection),
                    }
                },
                None => PointSegmentMetric::Projection(projection),
            }
        },
    }
}

#[derive(Clone, Copy)]
pub(super) enum ExactDistanceWitness {
    PointPair(XY, XY),
    ProjectedPointPair(XY, XY),
}

/// Order-faithful key for one nearest-witness candidate.
///
/// Distance measures the continuum, while emitted nearest/shortest-line points
/// are faithful stored witnesses and candidate selection is exact. Their
/// stored-point distance may differ in the last ulps because the exact foot is
/// not representable. On the UTM regression, gometry's continuum distance is
/// correctly rounded to `0.7411226985789381` while its stored witness measures
/// `0.7411226987304141`; Shapely reports the same continuum value and a stored
/// witness distance of `0.7411226987502236`.
///
/// The emitted interior witness is not necessarily the globally closest
/// representable point: a 4,000-sample audit found 47 of 1,256 interior
/// projections with a closer diagonal one-ULP neighbour, but only by ~1e-18
/// relative, below distance resolution. Evidence ledger SHA-256:
/// `7618c21623b59ce1714251f4f2ea3abd607413185e143312e8b8ea7a6aa80685`.
/// The rounded squared value is therefore only an interval filter. Disjoint
/// intervals decide cheaply; every overlap goes to the stored-dyadic owner,
/// never to a bare floating-point `<`.
#[derive(Clone, Copy)]
pub(super) struct SquaredDistanceKey {
    filter: SquaredDistanceFilter,
    witness: ExactDistanceWitness,
}

#[derive(Clone, Copy)]
struct SquaredDistanceFilter {
    lo: f64,
    hi: f64,
}

impl SquaredDistanceFilter {
    /// A point-pair key has four error-bearing stages (coordinate residuals,
    /// their squares, and the sum). `gamma(4)` bounds their normal relative
    /// error; the final one-ulp widening absorbs the bound arithmetic itself.
    fn point_pair(squared: f64) -> Self {
        const UNIT_ROUNDOFF: f64 = f64::EPSILON / 2.0;
        const GAMMA_4: f64 = 4.0 * UNIT_ROUNDOFF / (1.0 - 4.0 * UNIT_ROUNDOFF);
        if !squared.is_finite() {
            return Self::unbounded();
        }
        let error = squared * GAMMA_4;
        let lo = (squared - error).next_down().max(0.0);
        let hi = (squared + error).next_up();
        if hi.is_finite() {
            Self { lo, hi }
        } else {
            Self::unbounded()
        }
    }

    const fn unbounded() -> Self {
        Self {
            lo: f64::NEG_INFINITY,
            hi: f64::INFINITY,
        }
    }

    const fn bind(self, witness: ExactDistanceWitness) -> SquaredDistanceKey {
        SquaredDistanceKey {
            filter: self,
            witness,
        }
    }

    fn disjoint_cmp(self, other: Self) -> Option<Ordering> {
        if self.hi < other.lo {
            Some(Ordering::Less)
        } else if other.hi < self.lo {
            Some(Ordering::Greater)
        } else {
            None
        }
    }
}

impl SquaredDistanceKey {
    pub(super) fn cmp(&self, other: &Self) -> Ordering {
        if self.filter.hi < other.filter.lo {
            return Ordering::Less;
        }
        if other.filter.hi < self.filter.lo {
            return Ordering::Greater;
        }
        exact_witness_cmp_with_identity(self.witness, other.witness)
    }

    pub(super) const fn upper_bound(&self) -> f64 {
        self.filter.hi
    }

    #[cfg(test)]
    fn contains_exact(self) -> bool {
        if !self.filter.lo.is_finite() || !self.filter.hi.is_finite() {
            return true;
        }
        let exact = self.witness.squared();
        let denominator = ExactDyadic::from_f64(1.0);
        let lo = ExactSquaredDistance {
            numerator: ExactDyadic::from_f64(self.filter.lo),
            denominator: denominator.clone(),
        };
        let hi = ExactSquaredDistance {
            numerator: ExactDyadic::from_f64(self.filter.hi),
            denominator,
        };
        !lo.cmp(&exact).is_gt() && !hi.cmp(&exact).is_lt()
    }

    #[cfg(test)]
    const fn is_bounded(self) -> bool {
        self.filter.lo.is_finite() && self.filter.hi.is_finite()
    }
}

pub(super) fn point_pair_distance_key(left: XY, right: XY) -> SquaredDistanceKey {
    let witness = ExactDistanceWitness::PointPair(left, right);
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let squared = dx * dx + dy * dy;
    if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
        SquaredDistanceFilter::point_pair(squared).bind(witness)
    } else {
        SquaredDistanceFilter::unbounded().bind(witness)
    }
}

pub(super) fn point_segment_distance_key(point: XY, segment: Segment) -> SquaredDistanceKey {
    point_segment_selection(point, segment).distance_key(point, segment)
}

pub(super) fn point_segment_selection(point: XY, segment: Segment) -> PointSegmentSelection {
    match point_segment_metric(point, segment) {
        PointSegmentMetric::Squared {
            selection_squared,
            filter,
            witness_kind,
            projection,
            ..
        } => PointSegmentSelection {
            distance_squared: selection_squared,
            filter,
            witness_kind,
            projection,
        },
        PointSegmentMetric::Projection(projection) => {
            let distance = point_distance(point, projection.interpolate_xy(segment));
            let squared = distance * distance;
            let witness_kind = if projection.is_start() {
                PointSegmentWitnessKind::Start
            } else if projection.is_end() {
                PointSegmentWitnessKind::End
            } else {
                PointSegmentWitnessKind::Interior
            };
            PointSegmentSelection {
                distance_squared: if squared.is_finite() {
                    squared
                } else {
                    f64::INFINITY
                },
                filter: SquaredDistanceFilter::unbounded(),
                witness_kind,
                projection: SelectionProjectionSource::Ready(projection),
            }
        },
    }
}

#[cfg(test)]
std::thread_local! {
    static EXACT_SELECTION_FALLBACKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl ExactDistanceWitness {
    const fn points(self) -> (XY, XY) {
        match self {
            Self::PointPair(left, right) | Self::ProjectedPointPair(left, right) => (left, right),
        }
    }

    const fn tie_rank(self) -> u8 {
        match self {
            // On an exact XY tie, retain an interior projection's ordinate
            // provenance rather than collapsing it to an endpoint. This is
            // selection metadata only; both variants compare the same emitted
            // stored XY coordinates below.
            Self::ProjectedPointPair(..) => 0,
            Self::PointPair(..) => 1,
        }
    }

    fn same_distance_as(self, other: Self) -> bool {
        let (left_a, left_b) = self.points();
        let (right_a, right_b) = other.points();
        (same_point(left_a, right_a) && same_point(left_b, right_b))
            || (same_point(left_a, right_b) && same_point(left_b, right_a))
    }

    fn squared(self) -> ExactSquaredDistance {
        let (left, right) = self.points();
        ExactSquaredDistance::point_pair(left, right)
    }
}

pub(super) fn exact_distance_witness_cmp(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Ordering {
    left.squared().cmp(&right.squared())
}

fn exact_witness_cmp_with_identity(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Ordering {
    if left.same_distance_as(right) {
        return left.tie_rank().cmp(&right.tie_rank());
    }
    #[cfg(test)]
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(count.get() + 1));
    exact_distance_witness_cmp(left, right).then_with(|| left.tie_rank().cmp(&right.tie_rank()))
}

/// Affinely interpolate between two finite points. A convex `fraction` in
/// `[0, 1]` always yields a finite point; far extrapolation (e.g. a
/// near-parallel `line_intersection`) can overflow, in which case we fall back
/// to the nearer endpoint rather than panic across the FFI boundary.
pub(super) fn interpolate_segment_point(
    start: impl Into<XY>,
    end: impl Into<XY>,
    fraction: f64,
) -> XY {
    let (start, end) = (start.into(), end.into());
    let x = interpolate_f64(start.x, end.x, fraction);
    let y = interpolate_f64(start.y, end.y, fraction);
    if x.is_finite() && y.is_finite() {
        XY::new(x, y)
    } else if fraction <= 0.5 {
        start
    } else {
        end
    }
}

/// Ordinate-carrying interpolation for the LRS lanes: every present
/// ordinate (X/Y/Z/M) interpolates; the engine's planar form is
/// [`interpolate_segment_point`].
pub(crate) fn lerp_point(start: Point, end: Point, fraction: f64) -> Point {
    Point::new_axes(
        interpolate_f64(start.x, end.x, fraction),
        interpolate_f64(start.y, end.y, fraction),
        ZOrdinate(interpolate_optional(start.z(), end.z(), fraction)),
        MOrdinate(interpolate_optional(start.m(), end.m(), fraction)),
    )
    .unwrap_or(if fraction <= 0.5 { start } else { end })
}

/// Affine interpolation `start*(1 - fraction) + end*fraction`, valid for any
/// `fraction` (callers such as `line_intersection` extrapolate past `[0, 1]`).
/// The convex form keeps the result bounded by `max(|start|, |end|)` for
/// `fraction` in `[0, 1]`, avoiding the `end - start` overflow class, and is
/// exact at the endpoints (`fraction` 0 → `start`, 1 → `end`).
/// Plain mul+add, NOT `mul_add`: scalar `mul_add` on the portable baseline
/// (no FMA target feature) is a libm `fma` CALL per ordinate — the dominant
/// cost of interpolation-heavy kernels. Fused rounding buys nothing here;
/// every documented property above holds for the unfused form.
pub(crate) fn interpolate_f64(start: f64, end: f64, fraction: f64) -> f64 {
    let value = start * (1.0 - fraction) + end * fraction;
    if value.is_finite() || !start.is_finite() || !end.is_finite() {
        return value;
    }
    // The same convex reconstruction in a power-of-two frame: `±1e308`
    // endpoints can have a perfectly finite interior point even though the
    // direct two products overflow with opposite signs.
    let scale = axis_pow2_scale(start.abs().max(end.abs()));
    (start * scale * (1.0 - fraction) + end * scale * fraction) / scale
}

/// Reconstruct the free axis at a known held-axis crossing without forming
/// `(at - start) / (end - start)` in world scale. `None` is deliberate: an
/// unresolved crossing must make a caller fall back, never manufacture a
/// non-finite vertex or an empty clipped result.
#[expect(
    clippy::float_cmp,
    reason = "a held axis exactly selects an already-stored endpoint before any interpolation"
)]
pub(crate) fn held_axis_interpolate(
    start_held: f64,
    start_free: f64,
    end_held: f64,
    end_free: f64,
    held: f64,
) -> Option<f64> {
    if held == start_held {
        return Some(start_free);
    }
    if held == end_held {
        return Some(end_free);
    }
    // Preserve the established ordinary finite arithmetic exactly.  The
    // normalized reconstruction below exists for an overflowed delta or a
    // non-finite raw interpolation, not to perturb unrelated stored-double
    // crossings by changing their rounding path.
    let numerator = held - start_held;
    let denominator = end_held - start_held;
    if numerator.is_finite() && denominator.is_finite() {
        let fraction = numerator / denominator;
        let value = start_free + fraction * (end_free - start_free);
        if fraction.is_finite() && (0.0..=1.0).contains(&fraction) && value.is_finite() {
            return Some(value);
        }
    }
    let scale = axis_pow2_scale(start_held.abs().max(end_held.abs()).max(held.abs()));
    let numerator = scaled_residual(held, start_held, scale);
    let denominator = scaled_residual(end_held, start_held, scale);
    let fraction = numerator / denominator;
    if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
        return None;
    }
    let value = interpolate_f64(start_free, end_free, fraction);
    value.is_finite().then_some(value)
}

pub(super) fn interpolate_optional(
    start: Option<f64>,
    end: Option<f64>,
    fraction: f64,
) -> Option<f64> {
    match (start, end) {
        (Some(start), Some(end)) => Some(interpolate_f64(start, end, fraction)),
        _ => None,
    }
}

/// Tolerance for deciding an output vertex lies on an input segment when
/// restoring Z/M (relative to the segment length; see `segment_ordinate_at`).
pub(super) const ON_SEGMENT_EPSILON: f64 = 1e-9;

/// Transversal crossing point of two segments, or `None` when they do not
/// meet (or are collinear — overlaps are the endpoint-overlap path's job).
///
/// **Existence is decided exactly** by the robust orientation predicates, so
/// floating-point conditioning can never drop a true crossing (nor invent
/// one). Placement then solves the parametric system in double-double
/// (error-free split-product) arithmetic, with an exact power-of-two rescale
/// when coordinate magnitudes would overflow products, and clamps into the
/// segment extent that existence already certified. The returned point is the
/// plain X/Y crossing; Z/M is restored later.
pub(super) fn segment_cross_point(a: Segment, b: Segment) -> Option<XY> {
    match segment_contact_exact(a, b) {
        Contact::Cross { point } | Contact::Touch { point } => Some(point),
        Contact::Disjoint | Contact::Collinear { .. } => None,
    }
}

fn segment_cross_point_with_orientations(
    a: Segment,
    b: Segment,
    orientations: PairOrientations,
) -> Option<XY> {
    // Existence: each segment's endpoints must straddle (or touch) the other's
    // line. Exact arithmetic — the only rounding-free way to make this call.
    if orientations.right_start == orientations.right_end {
        // Same strict side: no crossing. Both collinear: parallel overlap,
        // handled by the endpoint-overlap path.
        return None;
    }
    if orientations.left_start == orientations.left_end {
        return None;
    }
    // A crossing certified through a COLLINEAR endpoint is that endpoint,
    // bit-exactly: a straight segment meets the other's carrier line only
    // once, so the touching endpoint IS the intersection. Parametric
    // placement would land within an ulp of it and split coincident
    // strokes inconsistently across pairs (T-junction ulp-twins — the
    // erosion fold repro pinned in `winding_buffer_tests`).
    if orientations.right_start == Orientation::Collinear {
        return Some(b.start);
    }
    if orientations.right_end == Orientation::Collinear {
        return Some(b.end);
    }
    if orientations.left_start == Orientation::Collinear {
        return Some(a.start);
    }
    if orientations.left_end == Orientation::Collinear {
        return Some(a.end);
    }
    // Placement is solved parametrically ALONG the first segment, so it is
    // not symmetric in argument order at the last ULP. Noding computes the
    // same crossing once per side — canonicalize segment direction and pair
    // order so both sides place the bitwise-identical point, or the segment
    // graph grows ULP-twin nodes whose hairline slivers leak invalid rings
    // out of repair/polygonize.
    let canonical = |segment: Segment| {
        if (segment.start.x, segment.start.y) <= (segment.end.x, segment.end.y) {
            segment
        } else {
            Segment {
                start: segment.end,
                end: segment.start,
            }
        }
    };
    let (a, b) = (canonical(a), canonical(b));
    // A crossing can be an unrepresentable 2^-2000 along the long segment but
    // an ordinary 0.5 on the short one. The tuple is the stable tie-breaker.
    let a_span = segment_span_magnitude(a);
    let b_span = segment_span_magnitude(b);
    let a_tuple = (a.start.x, a.start.y, a.end.x, a.end.y);
    let b_tuple = (b.start.x, b.start.y, b.end.x, b.end.y);
    let span_order = a_span.total_cmp(&b_span);
    let (a, b) = if span_order.is_lt() || (span_order.is_eq() && a_tuple <= b_tuple) {
        (a, b)
    } else {
        (b, a)
    };
    // Preserve mixed-exponent raw products first; a coherent overflow or
    // underflow retries in the safe per-axis power-of-two frame.
    if let Some(point) = segment_cross_placement(a, b, 1.0, 1.0) {
        return Some(point);
    }
    let (x_shift, y_shift) = segment_pair_scale_shifts(a, b);
    segment_cross_placement(
        a,
        b,
        exact_power_of_two(x_shift),
        exact_power_of_two(y_shift),
    )
}

fn segment_span_magnitude(segment: Segment) -> f64 {
    (segment.end.x - segment.start.x)
        .abs()
        .max((segment.end.y - segment.start.y).abs())
}

/// The placement half of [`segment_cross_point`]: existence is already
/// certified, so the parametric solution is clamped into `[0, 1]` rather than
/// re-checked. `None` only when the products of `scale`-adjusted coordinates
/// are non-finite (which the shared safe-magnitude frame prevents for finite
/// segment pairs of a coherent scale).
fn segment_cross_placement(a: Segment, b: Segment, x_scale: f64, y_scale: f64) -> Option<XY> {
    // Differences carry their rounding tails (`two_sum`), so the placement is
    // computed on the exact float-endpoint geometry, not on pre-rounded
    // deltas. Per-axis power-of-two scales give every 2-D cross the same
    // positive x_scale*y_scale factor, preserving signs and fractions.
    let a_dx = dd_diff(x_scale * a.end.x, x_scale * a.start.x);
    let a_dy = dd_diff(y_scale * a.end.y, y_scale * a.start.y);
    let b_dx = dd_diff(x_scale * b.end.x, x_scale * b.start.x);
    let b_dy = dd_diff(y_scale * b.end.y, y_scale * b.start.y);
    let offset_x = dd_diff(x_scale * b.start.x, x_scale * a.start.x);
    let offset_y = dd_diff(y_scale * b.start.y, y_scale * a.start.y);
    let denominator = dd_cross(a_dx, b_dy, a_dy, b_dx);
    let numerator = dd_cross(offset_x, b_dy, offset_y, b_dx);
    if ![denominator.0, denominator.1, numerator.0, numerator.1]
        .into_iter()
        .all(f64::is_finite)
    {
        return None;
    }
    if denominator == (0.0, 0.0) {
        return None;
    }
    let along_a = dd_div(numerator, denominator);
    if !along_a.is_finite() {
        return None;
    }
    let along_a = along_a.clamp(0.0, 1.0);
    // Snap to an axis-aligned segment's fixed ordinate so a crossing on a
    // vertical/horizontal edge lands exactly on it (no `-1e-16` drift) — this
    // covers the common rectilinear polygon-boundary case; diagonals fall back
    // to the interpolated value.
    let x = if b_dx == (0.0, 0.0) {
        b.start.x
    } else if a_dx == (0.0, 0.0) {
        a.start.x
    } else {
        interpolate_f64(a.start.x, a.end.x, along_a)
    };
    let y = if b_dy == (0.0, 0.0) {
        b.start.y
    } else if a_dy == (0.0, 0.0) {
        a.start.y
    } else {
        interpolate_f64(a.start.y, a.end.y, along_a)
    };
    (x.is_finite() && y.is_finite()).then_some(XY::new(x, y))
}

pub(crate) fn point_on_segment(
    point: impl Into<XY>,
    start: impl Into<XY>,
    end: impl Into<XY>,
) -> bool {
    let (point, start, end) = (point.into(), start.into(), end.into());
    // Cheap closed-envelope reject first: orientation() is the robust adaptive
    // determinant and the compiler cannot reorder it past the && short-circuit.
    if point.x < start.x.min(end.x)
        || point.x > start.x.max(end.x)
        || point.y < start.y.min(end.y)
        || point.y > start.y.max(end.y)
    {
        return false;
    }
    orientation(start, end, point) == Orientation::Collinear
}

pub(crate) fn line_segments<C: Coordinates + ?Sized>(
    points: &C,
) -> impl Iterator<Item = Segment> + '_ {
    let mut columns = points
        .xy_columns()
        .map(|(xs, ys)| (xs.array_windows::<2>(), ys.array_windows::<2>()));
    let mut pairs = points.segment_pairs();
    std::iter::from_fn(move || {
        if let Some((xs, ys)) = &mut columns {
            let x = xs.next()?;
            let y = ys.next()?;
            return Some(Segment {
                start: XY::new(x[0], y[0]),
                end: XY::new(x[1], y[1]),
            });
        }
        pairs.next().map(|[start, end]| Segment {
            start: start.into(),
            end: end.into(),
        })
    })
}

/// Orientation-independent undirected key for two segment endpoints.
pub(super) fn undirected_edge_key(left: PointKey, right: PointKey) -> (PointKey, PointKey) {
    if (left.x, left.y) <= (right.x, right.y) {
        (left, right)
    } else {
        (right, left)
    }
}

/// Orientation-independent undirected key for an atomic segment.
pub(super) fn undirected_segment_edge_key(segment: Segment) -> (PointKey, PointKey) {
    undirected_edge_key(PointKey::new(segment.start), PointKey::new(segment.end))
}

pub(super) fn segments_intersect(left: Segment, right: Segment) -> bool {
    segment_contact(left, right) != SegmentContact::None
}

/// Closed-envelope disjointness — the cheap reject before the robust
/// orientation work in brute-force pair scans (the indexed paths already
/// prune by envelope).
pub(super) fn segment_envelopes_disjoint(left: Segment, right: Segment) -> bool {
    left.start.x.max(left.end.x) < right.start.x.min(right.end.x)
        || right.start.x.max(right.end.x) < left.start.x.min(left.end.x)
        || left.start.y.max(left.end.y) < right.start.y.min(right.end.y)
        || right.start.y.max(right.end.y) < left.start.y.min(left.end.y)
}

/// How a segment pair meets, strongest last: `Cross` is a TRANSVERSAL
/// crossing strictly interior to both segments (every endpoint strictly off
/// the other's carrier, both straddling), `Touch` is any other intersection
/// (endpoint contact or collinear overlap). The split is exact: with any
/// endpoint on the other's carrier, the carrier meets that segment only at
/// the endpoint (or the segments are fully collinear), so the intersection
/// — if it exists — involves an endpoint and the boundary-inclusive
/// membership checks decide it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SegmentContact {
    None,
    Touch,
    Cross,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PairOrientations {
    pub right_start: Orientation,
    pub right_end: Orientation,
    pub left_start: Orientation,
    pub left_end: Orientation,
}

impl PairOrientations {
    fn new(left: Segment, right: Segment) -> Self {
        Self {
            right_start: orientation(left.start, left.end, right.start),
            right_end: orientation(left.start, left.end, right.end),
            left_start: orientation(right.start, right.end, left.start),
            left_end: orientation(right.start, right.end, left.end),
        }
    }

    fn all_collinear(self) -> bool {
        self.right_start == Orientation::Collinear
            && self.right_end == Orientation::Collinear
            && self.left_start == Orientation::Collinear
            && self.left_end == Orientation::Collinear
    }

    fn has_collinear(self) -> bool {
        self.right_start == Orientation::Collinear
            || self.right_end == Orientation::Collinear
            || self.left_start == Orientation::Collinear
            || self.left_end == Orientation::Collinear
    }

    fn straddles_or_touches(self) -> bool {
        self.right_start != self.right_end && self.left_start != self.left_end
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct SharedSpan {
    pub same_direction: bool,
    pub start: XY,
    pub end: XY,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Contact {
    Disjoint,
    Cross { point: XY },
    Touch { point: XY },
    Collinear { span: SharedSpan },
}

/// Midpoint of the two segments' overlapping bounding extent.
///
/// A crossing certified by straddle lies in both segments, hence in both
/// bounding boxes, hence in their intersection. This is the tightest bounded
/// point available without a placement solve — used only where placement has
/// already declined.
fn segment_extent_overlap_midpoint(left: Segment, right: Segment) -> XY {
    let overlap = |a0: f64, a1: f64, b0: f64, b1: f64| {
        let low = a0.min(a1).max(b0.min(b1));
        let high = a0.max(a1).min(b0.max(b1));
        f64::midpoint(low, high)
    };
    XY::new(
        overlap(left.start.x, left.end.x, right.start.x, right.end.x),
        overlap(left.start.y, left.end.y, right.start.y, right.end.y),
    )
}

pub(super) fn segment_contact_with_orientations(
    left: Segment,
    right: Segment,
) -> (Contact, PairOrientations) {
    let orientations = PairOrientations::new(left, right);
    let contact = if orientations.all_collinear() {
        collinear_shared_span_exact(left, right)
            .map_or(Contact::Disjoint, |span| Contact::Collinear { span })
    } else if orientations.straddles_or_touches() {
        if orientations.has_collinear() {
            segment_cross_point_with_orientations(left, right, orientations)
                .map_or(Contact::Disjoint, |point| Contact::Touch { point })
        } else {
            // Existence is certified exactly by the straddle above, so a
            // failed placement must NOT become `Disjoint` — that would be a
            // false negative, the one error class these predicates exist to
            // prevent. Placement only declines on non-finite products, which
            // the power-of-two rescale frame rules out for finite input, so
            // the fallback is unreachable in practice; when it is taken, the
            // midpoint of the two segments' overlapping extent is a bounded
            // point inside the region that provably contains the crossing.
            // Never fall back to an operand endpoint: that can sit arbitrarily
            // far from the true crossing while looking like a real answer.
            Contact::Cross {
                point: segment_cross_point_with_orientations(left, right, orientations)
                    .unwrap_or_else(|| segment_extent_overlap_midpoint(left, right)),
            }
        }
    } else {
        Contact::Disjoint
    };
    (contact, orientations)
}

pub(super) fn segment_contact_exact(left: Segment, right: Segment) -> Contact {
    segment_contact_with_orientations(left, right).0
}

pub(super) fn segment_contact(left: Segment, right: Segment) -> SegmentContact {
    match segment_contact_exact(left, right) {
        Contact::Disjoint => SegmentContact::None,
        Contact::Cross { .. } => SegmentContact::Cross,
        Contact::Touch { .. } | Contact::Collinear { .. } => SegmentContact::Touch,
    }
}

fn collinear_shared_span_exact(left: Segment, right: Segment) -> Option<SharedSpan> {
    let left_degenerate = same_point(left.start, left.end);
    let right_degenerate = same_point(right.start, right.end);

    if left_degenerate && right_degenerate {
        // A zero-length span carries no meaningful direction; the endpoint
        // deltas are exactly zero, so the dot product is a well-defined `0.0`
        // here (no underflow) — reuse it for the degenerate agreement flag.
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return same_point(left.start, right.start).then_some(SharedSpan {
            same_direction,
            start: left.start,
            end: left.start,
        });
    }
    if left_degenerate {
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return point_in_segment_envelope(left.start, right).then_some(SharedSpan {
            same_direction,
            start: left.start,
            end: left.start,
        });
    }
    if right_degenerate {
        let same_direction = segment_direction_dot(left, right) > 0.0;
        return point_in_segment_envelope(right.start, left).then_some(SharedSpan {
            same_direction,
            start: right.start,
            end: right.start,
        });
    }

    let axis = if (left.end.x - left.start.x).abs() >= (left.end.y - left.start.y).abs() {
        Axis::X
    } else {
        Axis::Y
    };
    let left_forward = axis.value(left.start) <= axis.value(left.end);
    // Shared direction from endpoint ORDER on the dominant axis, NOT a
    // direction-vector dot product: for tiny (~1e-162) collinear segments the
    // products underflow to `0.0`, misreading two identical segments as
    // opposite-direction. Both segments are non-degenerate and collinear here,
    // so each has a strictly nonzero extent on the larger-extent axis and the
    // ordering comparison is exact.
    let same_direction = left_forward == (axis.value(right.start) <= axis.value(right.end));
    let along_left = |point: XY| {
        let value = axis.value(point);
        if left_forward { value } else { -value }
    };

    let left_start = along_left(left.start);
    let left_end = along_left(left.end);
    let right_start = along_left(right.start);
    let right_end = along_left(right.end);
    let right_min = right_start.min(right_end);
    let right_max = right_start.max(right_end);
    let overlap_start = left_start.max(right_min);
    let overlap_end = left_end.min(right_max);
    if overlap_start > overlap_end {
        return None;
    }

    Some(SharedSpan {
        same_direction,
        start: endpoint_at_collinear_order(left, right, along_left, overlap_start),
        end: endpoint_at_collinear_order(left, right, along_left, overlap_end),
    })
}

#[derive(Clone, Copy)]
enum Axis {
    X,
    Y,
}

impl Axis {
    const fn value(self, point: XY) -> f64 {
        match self {
            Self::X => point.x,
            Self::Y => point.y,
        }
    }
}

fn endpoint_at_collinear_order(
    left: Segment,
    right: Segment,
    along_left: impl Fn(XY) -> f64,
    target: f64,
) -> XY {
    [left.start, left.end, right.start, right.end]
        .into_iter()
        .find(|point| endpoint_order_key_matches(along_left(*point), target))
        .expect("collinear overlap boundary is one of the input endpoints")
}

#[expect(
    clippy::float_cmp,
    reason = "target is selected from the same exact endpoint keys"
)]
fn endpoint_order_key_matches(candidate: f64, target: f64) -> bool {
    // Exact key match: the target was selected from these endpoint keys.
    candidate == target
}

fn point_in_segment_envelope(point: XY, segment: Segment) -> bool {
    point.x >= segment.start.x.min(segment.end.x)
        && point.x <= segment.start.x.max(segment.end.x)
        && point.y >= segment.start.y.min(segment.end.y)
        && point.y <= segment.start.y.max(segment.end.y)
}

fn segment_direction_dot(left: Segment, right: Segment) -> f64 {
    let left_dx = left.end.x - left.start.x;
    let left_dy = left.end.y - left.start.y;
    let right_dx = right.end.x - right.start.x;
    let right_dy = right.end.y - right.start.y;
    left_dx * right_dx + left_dy * right_dy
}

/// Minimum of `metric` evaluated for each segment endpoint against the other
/// segment; `0.0` when the segments intersect. `metric` is monomorphized per
/// call site (plain or squared distance), so this stays allocation- and
/// indirection-free.
pub(super) fn shared_segment_part(left: Segment, right: Segment) -> Option<(bool, Vec<XY>)> {
    // The fused kernel already computes the EXACT collinear overlap span (its
    // endpoints are actual input coordinates, ordered along the segment). Reuse
    // it directly — a positive-length shared run is exactly an overlap whose two
    // endpoints differ by coordinate identity. No scale-dependent `1e-12` cutoff
    // (which misgraded a tiny-but-real shared edge as a point on large
    // coordinates) and no interpolation jitter.
    let Contact::Collinear { span } = segment_contact_exact(left, right) else {
        return None;
    };
    if same_point(span.start, span.end) {
        return None;
    }
    Some((span.same_direction, vec![span.start, span.end]))
}

pub(super) fn point_segment_distance(point: impl Into<XY>, segment: Segment) -> f64 {
    let point = point.into();
    match point_segment_metric(point, segment) {
        PointSegmentMetric::Squared {
            measurement_squared,
            ..
        } => measurement_squared.sqrt(),
        PointSegmentMetric::Projection(projection) => {
            point_distance(point, projection.interpolate_xy(segment))
        },
    }
}

/// A squared Euclidean residual is trustworthy only when it is a **normal**
/// positive number, or exactly zero with all-exact-zero deltas (true contact).
///
/// Positive subnormals are **not** faithful norms: `sqrt` of a subnormal square
/// quantizes the distance (e.g. separation `2.2e-162` squares to a subnormal
/// whose `sqrt` is ~`2.22e-162`, a +1% error that also flips `dwithin` and
/// breaks monotonicity). Full underflow to zero with nonzero deltas is the
/// sibling false-zero case. One rule for scalar, SIMD, and packed lanes.
pub(crate) fn squared_norm_is_trustworthy(squared: f64, all_deltas_zero: bool) -> bool {
    squared.is_normal() || (squared == 0.0 && all_deltas_zero)
}

/// Finalize a planar **distance** answer accumulated in squared space.
///
/// - `best_sq` is a normal positive → `sqrt` (ordinary mid-range, bit-stable)
/// - `best_sq` is zero or a positive subnormal → recompute in distance space
///   (true contact still returns 0; subnormal/false-zero cannot masquerade)
/// - non-finite → propagate (`+inf` = no candidate / overflow class)
///
/// Every planar distance answer that min-reduces squared candidates must
/// finish through this helper — never bare `squared.sqrt()`.
pub(crate) fn finish_planar_squared_min(
    best_sq: f64,
    recompute_distance: impl FnOnce() -> f64,
) -> f64 {
    if best_sq.is_normal() {
        best_sq.sqrt()
    } else if best_sq >= 0.0 && best_sq.is_finite() {
        // Zero or positive subnormal: recompute with an honest hypot form.
        recompute_distance()
    } else {
        best_sq
    }
}

/// Honest squared distance point→segment. `None` on overflow or false-zero
/// underflow (representable positive separation whose `ex²+ey²` collapses to
/// 0). Magnitude / zero-tests must not treat a bare `0.0` from
/// [`point_segment_distance_squared`] as contact — use this, or finish via
/// [`finish_planar_squared_min`].
pub(super) fn try_point_segment_distance_squared(
    point: impl Into<XY>,
    segment: Segment,
) -> Option<f64> {
    let point = point.into();
    match point_segment_metric(point, segment) {
        PointSegmentMetric::Squared {
            measurement_squared,
            ..
        } => Some(measurement_squared),
        PointSegmentMetric::Projection(_) => None,
    }
}

/// Squared distance from `point` to `segment` — self-contained MEASUREMENT.
/// Ordering / compare-min callers may use this; a returned `0.0` is **not**
/// a contact certificate under underflow (use [`try_point_segment_distance_squared`]
/// or [`finish_planar_squared_min`] for magnitude answers).
pub(super) fn point_segment_distance_squared(point: impl Into<XY>, segment: Segment) -> f64 {
    let point = point.into();
    match point_segment_metric(point, segment) {
        PointSegmentMetric::Squared {
            measurement_squared,
            ..
        } => measurement_squared,
        PointSegmentMetric::Projection(projection) => {
            let distance = point_distance(point, projection.interpolate_xy(segment));
            distance * distance
        },
    }
}

/// Guarded sqrt-form planar distance — ONE point-distance for the whole
/// project: one inline `sqrt` instead of a libm `hypot` call (~3× cheaper, the
/// dominant cost of prefix-length builds). The rescue covers both failure
/// classes — overflow to non-finite, and full underflow to `0.0` with a nonzero
/// delta — by falling back to `hypot`, so every input class gets a result
/// matching the overflow-safe form (lane rounding may differ by an ulp; lengths
/// are measurement arithmetic per the float policy). This is the scalar sibling
/// of the SIMD `line_length` kernel's sqrt-with-rescue lanes.
pub(crate) fn point_distance(left: impl Into<XY>, right: impl Into<XY>) -> f64 {
    let (left, right) = (left.into(), right.into());
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    // Plain mul+add, NOT `mul_add`: scalar `mul_add` on the portable
    // baseline (no FMA target feature) is a libm `fma` CALL — the very cost
    // class this function exists to remove. Fused rounding buys nothing a
    // measurement needs.
    let squared = dx * dx + dy * dy;
    if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
        squared.sqrt()
    } else {
        dx.hypot(dy)
    }
}

/// Honest squared Euclidean distance when squared space is faithful.
/// Returns `None` on overflow, positive-subnormal squares, or false-zero
/// underflow (distinct points whose `dx²+dy²` collapses to 0 or a subnormal)
/// — callers that need a correct magnitude or zero-test must use
/// [`point_distance`] / [`points_dwithin`].
pub(crate) fn try_point_distance_squared(left: impl Into<XY>, right: impl Into<XY>) -> Option<f64> {
    let (left, right) = (left.into(), right.into());
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let squared = dx * dx + dy * dy;
    squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0).then_some(squared)
}

pub(crate) fn point_distance_squared(left: impl Into<XY>, right: impl Into<XY>) -> f64 {
    let (left, right) = (left.into(), right.into());
    try_point_distance_squared(left, right).unwrap_or_else(|| {
        // Overflow: hypot then square when representable; underflow of
        // d*d for tiny d still yields 0 — ordering-only callers should
        // prefer nearest_pair_key-style compare helpers; magnitude
        // callers must use [`point_distance`] / [`points_dwithin`].
        let d = point_distance(left, right);
        let d2 = d * d;
        if d2.is_finite() { d2 } else { f64::INFINITY }
    })
}

/// Planar dwithin in distance space when squared space collapses.
/// Ordinary mid-range pairs keep the squared compare (bit-stable at the
/// boundary for honest squares); tiny separations and tiny limits use
/// [`point_distance`] so `dwithin(a,b,0)` is false for distinct points and
/// `dwithin(a,b,5e-201)` is false when distance is 1e-200.
pub(crate) fn points_dwithin(left: impl Into<XY>, right: impl Into<XY>, limit: f64) -> bool {
    let (left, right) = (left.into(), right.into());
    if !limit.is_finite() {
        // +inf admits everything finite; NaN / -inf never.
        return limit == f64::INFINITY;
    }
    if limit < 0.0 {
        return false;
    }
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    if dx == 0.0 && dy == 0.0 {
        return true;
    }
    if limit == 0.0 {
        // Distinct points are never within a hard zero radius.
        return false;
    }
    let d2 = dx * dx + dy * dy;
    let lim2 = limit * limit;
    // Both sides of the compare must be trustworthy normals — a false-zero
    // or positive-subnormal d²/limit² would quantize the boundary (e.g.
    // d=2.2e-162, limit=1.6e-162 wrongly returns true via subnormal squares).
    if d2.is_normal() && lim2.is_normal() {
        d2 <= lim2
    } else {
        point_distance(left, right) <= limit
    }
}

/// Total order of distances from `origin` to two points — overflow-safe
/// (squared space when finite, hypot otherwise). Used by noding cut sorts
/// that must not reorder under squared overflow.
pub(crate) fn distance_order_from_origin(origin: XY, left: XY, right: XY) -> std::cmp::Ordering {
    let key = |point: XY| {
        let dx = origin.x - point.x;
        let dy = origin.y - point.y;
        let squared = dx * dx + dy * dy;
        if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
            (true, squared)
        } else {
            (false, point_distance(origin, point))
        }
    };
    let left_key = key(left);
    let right_key = key(right);
    match (left_key.0, right_key.0) {
        (true, true) | (false, false) => left_key.1.total_cmp(&right_key.1),
        (true, false) => left_key.1.sqrt().total_cmp(&right_key.1),
        (false, true) => left_key.1.total_cmp(&right_key.1.sqrt()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Orientation {
    Collinear,
    Clockwise,
    CounterClockwise,
}

#[cfg(test)]
#[path = "segments_tests.rs"]
mod tests;
