#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![expect(
    clippy::self_named_module_files,
    reason = "the cohesive segment kernel predates its exact-arithmetic child module; a mechanical file move adds no ownership boundary"
)]
//! The shared segment-geometry kernel: nearest-point, segment intersection /
//! projection, and interpolation primitives used across distance, overlay,
//! linear-referencing, and predicate modules.

use super::*;

mod exact;

pub(crate) use exact::orientation;
pub(super) use exact::{CCW_ERRBOUND_A, orientation_xy, ray_crossing_is_right};
use exact::{dd_cross, dd_diff, dd_div, exact_power_of_two, segment_pair_scale_shifts};

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
            Some(candidate) => nearest.add(point, candidate),
            None => missing = true,
        },
    );
    right.for_each_point(
        |point| match nearest_point_with_segments(point, left, &left_pairs) {
            Some(candidate) => nearest.add(candidate, point),
            None => missing = true,
        },
    );
    if missing {
        return None;
    }
    for left_pair in &left_pairs {
        for right_pair in &right_pairs {
            let (left_point, right_point) = pair_nearest_points(*left_pair, *right_pair);
            nearest.add(left_point, right_point);
        }
    }

    nearest.points
}

/// [`segment_nearest_points`] over full-ordinate vertex pairs: the planar
/// kernel finds the XY witnesses, and each foot lifts back through its
/// host pair so Z/M interpolate.
fn pair_nearest_points(left: (Point, Point), right: (Point, Point)) -> (Point, Point) {
    let left_xy = Segment {
        start: left.0.xy(),
        end: left.1.xy(),
    };
    let right_xy = Segment {
        start: right.0.xy(),
        end: right.1.xy(),
    };
    let (foot_left, foot_right) = segment_nearest_points(left_xy, right_xy);
    (
        lift_onto_pair(foot_left, left, left_xy),
        lift_onto_pair(foot_right, right, right_xy),
    )
}

/// Lift one planar witness foot back onto its full-ordinate pair.
fn lift_onto_pair(foot: XY, pair: (Point, Point), segment: Segment) -> Point {
    lerp_point(pair.0, pair.1, segment_projection_fraction(foot, segment))
}

#[derive(Default)]
pub(super) struct NearestPoints {
    distance: f64,
    points: Option<(Point, Point)>,
}

impl NearestPoints {
    fn add(&mut self, left: Point, right: Point) {
        let distance = point_distance(left, right);
        if self.points.is_none() || distance < self.distance {
            self.distance = distance;
            self.points = Some((left, right));
        }
    }
}

/// Nearest point on `geometry` to `point`, with the geometry's segments
/// precomputed so batch callers hoist the segment gather out of their
/// per-vertex loop.
fn nearest_point_with_segments(
    point: Point,
    geometry: &Shape,
    pairs: &[(Point, Point)],
) -> Option<Point> {
    if geometry.contains_point(point) {
        return Some(point);
    }

    let mut nearest = None;
    let mut best = f64::INFINITY;
    for pair in pairs {
        let segment = Segment {
            start: pair.0.xy(),
            end: pair.1.xy(),
        };
        let projected = lift_onto_pair(nearest_point_on_segment(point, segment), *pair, segment);
        let distance = point_distance(point, projected);
        if distance < best {
            best = distance;
            nearest = Some(projected);
        }
    }
    geometry.for_each_point(|candidate| {
        let distance = point_distance(point, candidate);
        if distance < best {
            best = distance;
            nearest = Some(candidate);
        }
    });
    nearest
}

pub(super) fn clip_segment(
    start: impl Into<XY>,
    end: impl Into<XY>,
    rect: Bounds,
) -> Option<(XY, XY)> {
    let (start, end) = (start.into(), end.into());
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    let mut lower = 0.0;
    let mut upper = 1.0;
    for (edge, distance) in [
        (-dx, start.x - rect.minx()),
        (dx, rect.maxx() - start.x),
        (-dy, start.y - rect.miny()),
        (dy, rect.maxy() - start.y),
    ] {
        if edge == 0.0 {
            if distance < 0.0 {
                return None;
            }
            continue;
        }
        let ratio = distance / edge;
        if edge < 0.0 {
            if ratio > upper {
                return None;
            }
            lower = f64::max(lower, ratio);
        } else {
            if ratio < lower {
                return None;
            }
            upper = f64::min(upper, ratio);
        }
    }
    Some((
        interpolate_segment_point(start, end, lower),
        interpolate_segment_point(start, end, upper),
    ))
}

pub(super) fn segment_nearest_points(left: Segment, right: Segment) -> (XY, XY) {
    if segments_intersect(left, right) {
        let point = segment_intersection_point(left, right);
        return (point, point);
    }

    let left_start = nearest_point_on_segment(left.start, right);
    let left_end = nearest_point_on_segment(left.end, right);
    let right_start = nearest_point_on_segment(right.start, left);
    let right_end = nearest_point_on_segment(right.end, left);
    let candidates = [
        (left.start, left_start),
        (left.end, left_end),
        (right_start, right.start),
        (right_end, right.end),
    ];
    // Keep the best distance key next to the candidate so each challenger is
    // scored once (was re-evaluating `point_distance(best)` on every compare).
    // Guarded squared compare matches `point_distance` ordering for the common
    // finite/non-underflow case; overflow/underflow falls back to the full
    // distance so huge-coordinate ties stay bit-identical.
    let mut best = candidates[0];
    let mut best_key = nearest_pair_key(best.0, best.1);
    for &candidate in &candidates[1..] {
        let key = nearest_pair_key(candidate.0, candidate.1);
        if nearest_key_lt(key, best_key) {
            best = candidate;
            best_key = key;
        }
    }
    best
}

/// Ordering key for a nearest-pair candidate. When the squared form is finite
/// and not a false-zero underflow, the key is the squared distance (no sqrt);
/// otherwise it is the overflow-safe `point_distance` result. The `squared`
/// flag keeps the two key spaces from mixing under comparison.
#[derive(Clone, Copy)]
struct NearestPairKey {
    squared: bool,
    value: f64,
}

fn nearest_pair_key(left: XY, right: XY) -> NearestPairKey {
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let squared = dx * dx + dy * dy;
    if squared.is_finite() && (squared != 0.0 || (dx == 0.0 && dy == 0.0)) {
        NearestPairKey {
            squared: true,
            value: squared,
        }
    } else {
        NearestPairKey {
            squared: false,
            value: point_distance(left, right),
        }
    }
}

/// `true` when `challenger` is strictly closer than `incumbent` under
/// `point_distance` total-order semantics (first-wins on exact ties: strict lt).
fn nearest_key_lt(challenger: NearestPairKey, incumbent: NearestPairKey) -> bool {
    match (challenger.squared, incumbent.squared) {
        (true, true) | (false, false) => challenger.value.total_cmp(&incumbent.value).is_lt(),
        // Mixed key spaces: materialize both as real distances so ordering
        // matches the historical `point_distance` total_cmp.
        (true, false) => {
            let challenger_d = challenger.value.sqrt();
            challenger_d.total_cmp(&incumbent.value).is_lt()
        },
        (false, true) => {
            let incumbent_d = incumbent.value.sqrt();
            challenger.value.total_cmp(&incumbent_d).is_lt()
        },
    }
}

pub(super) fn segment_intersection_point(left: Segment, right: Segment) -> XY {
    for point in [left.start, left.end] {
        if point_on_segment(point, right.start, right.end) {
            return point;
        }
    }
    for point in [right.start, right.end] {
        if point_on_segment(point, left.start, left.end) {
            return point;
        }
    }

    let left_dx = left.end.x - left.start.x;
    let left_dy = left.end.y - left.start.y;
    let right_dx = right.end.x - right.start.x;
    let right_dy = right.end.y - right.start.y;
    let denominator = left_dx * right_dy - left_dy * right_dx;
    if denominator == 0.0 {
        return left.start;
    }
    let offset_x = right.start.x - left.start.x;
    let offset_y = right.start.y - left.start.y;
    let fraction = (offset_x * right_dy - offset_y * right_dx) / denominator;
    interpolate_segment_point(left.start, left.end, fraction)
}

pub(super) fn nearest_point_on_segment(point: impl Into<XY>, segment: Segment) -> XY {
    let point = point.into();
    interpolate_segment_point(
        segment.start,
        segment.end,
        segment_projection_fraction(point, segment),
    )
}

pub(super) fn segment_projection_fraction(point: impl Into<XY>, segment: Segment) -> f64 {
    let point = point.into();
    let dx = segment.end.x - segment.start.x;
    let dy = segment.end.y - segment.start.y;
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3, and the
    // projection fraction is measurement arithmetic (the clamp and the
    // overflow rescue below own the edge cases either way).
    let length2 = dx * dx + dy * dy;
    if length2 == 0.0 {
        return 0.0;
    }
    let offset_x = point.x - segment.start.x;
    let offset_y = point.y - segment.start.y;
    let fraction = (offset_x * dx + offset_y * dy) / length2;
    if fraction.is_finite() {
        return fraction.clamp(0.0, 1.0);
    }
    // Endpoints so far apart that `end - start` (or its square) overflowed to a
    // non-finite value and divided to NaN. The projection fraction is invariant
    // under a uniform coordinate scale, so recompute on coordinates shrunk to a
    // safe magnitude — exact, and overflow-free.
    segment_projection_fraction_scaled(point, segment)
}

/// Overflow-safe `segment_projection_fraction` for finite but extreme
/// coordinates: rescale every coordinate by a common factor (the fraction is
/// scale-invariant) so differences and their squares stay finite.
pub(super) fn segment_projection_fraction_scaled(point: impl Into<XY>, segment: Segment) -> f64 {
    let point = point.into();
    let magnitude = [
        segment.start.x,
        segment.start.y,
        segment.end.x,
        segment.end.y,
        point.x,
        point.y,
    ]
    .into_iter()
    .map(f64::abs)
    .fold(0.0_f64, f64::max);
    if magnitude == 0.0 {
        return 0.0;
    }
    let scale = SAFE_PROJECTION_MAGNITUDE / magnitude;
    let dx = segment.end.x * scale - segment.start.x * scale;
    let dy = segment.end.y * scale - segment.start.y * scale;
    let length2 = dx * dx + dy * dy;
    if length2 == 0.0 {
        return 0.0;
    }
    let offset_x = point.x * scale - segment.start.x * scale;
    let offset_y = point.y * scale - segment.start.y * scale;
    let fraction = (offset_x * dx + offset_y * dy) / length2;
    if fraction.is_finite() {
        fraction.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

/// Target magnitude for rescaled projection coordinates: small enough that the
/// squared length cannot overflow `f64` (`1e150^2 = 1e300 < f64::MAX`).
pub(super) const SAFE_PROJECTION_MAGNITUDE: f64 = 1e150;

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
pub(super) fn interpolate_f64(start: f64, end: f64, fraction: f64) -> f64 {
    start * (1.0 - fraction) + end * fraction
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

pub(super) fn point_on_segment(
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
            Contact::Cross {
                point: segment_cross_point_with_orientations(left, right, orientations)
                    .unwrap_or_else(|| segment_intersection_point(left, right)),
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
    point_distance(point, nearest_point_on_segment(point, segment))
}

/// Squared distance from `point` to `segment` — a self-contained
/// MEASUREMENT in plain ops: the projected-`Point` detour paid five libm
/// `fma` calls per case on the portable (pre-v3) baseline, and no geometry
/// placement leaves this function. The SIMD facet kernel computes the same
/// clamped-projection form in lanes; extreme coordinates rescue through the
/// scaled projection and, when a DELTA itself overflows (segment span or
/// probe offset past ~1.8e308 — where the fast error vector turns
/// non-finite even though the convex projection stays finite), through the
/// old projected-point form, so ungated callers (clearance, snap) measure
/// extreme operands exactly as before.
pub(super) fn point_segment_distance_squared(point: impl Into<XY>, segment: Segment) -> f64 {
    let point = point.into();
    let dx = segment.end.x - segment.start.x;
    let dy = segment.end.y - segment.start.y;
    let qx = point.x - segment.start.x;
    let qy = point.y - segment.start.y;
    let length2 = dx * dx + dy * dy;
    let fraction = if length2 == 0.0 {
        0.0
    } else {
        let fraction = ((qx * dx + qy * dy) / length2).clamp(0.0, 1.0);
        if fraction.is_finite() {
            fraction
        } else {
            segment_projection_fraction_scaled(point, segment)
        }
    };
    let ex = qx - fraction * dx;
    let ey = qy - fraction * dy;
    let squared = ex * ex + ey * ey;
    if squared.is_finite() {
        return squared;
    }
    point_distance_squared(point, nearest_point_on_segment(point, segment))
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
    if squared.is_finite() && (squared != 0.0 || (dx == 0.0 && dy == 0.0)) {
        squared.sqrt()
    } else {
        dx.hypot(dy)
    }
}

pub(crate) fn point_distance_squared(left: impl Into<XY>, right: impl Into<XY>) -> f64 {
    let (left, right) = (left.into(), right.into());
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3, and this
    // is pure measurement arithmetic.
    dx * dx + dy * dy
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
