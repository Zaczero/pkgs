use crate::geometry::*;

/// Lower-bound squared distance between two AABBs (0 when overlapping).
/// Plain mul+add, not `mul_add`: scalar `mul_add` on the portable baseline
/// is a libm `fma` CALL, and this bound runs once per visited node (14% of
/// tree-heavy dwithin in profile). Pruning bounds are measurements — both
/// forms carry the same 1-ulp approximation grade.
pub(crate) fn aabb_distance_squared(a: [f64; 4], b: [f64; 4]) -> f64 {
    let (dx, dy) = aabb_gaps(a, b);
    dx * dx + dy * dy
}

/// Lower-bound distance between two AABBs. Guarded sqrt: `hypot` is a
/// libm call per visited node; overflow falls back to it (underflow to
/// `0.0` only weakens a lower bound, which stays sound).
pub(crate) fn aabb_distance(a: [f64; 4], b: [f64; 4]) -> f64 {
    let (dx, dy) = aabb_gaps(a, b);
    let squared = dx * dx + dy * dy;
    if squared.is_finite() {
        squared.sqrt()
    } else {
        dx.hypot(dy)
    }
}

pub(crate) fn aabb_gaps(a: [f64; 4], b: [f64; 4]) -> (f64, f64) {
    (
        (a[0] - b[2]).max(b[0] - a[2]).max(0.0),
        (a[1] - b[3]).max(b[1] - a[3]).max(0.0),
    )
}

/// Upper-bound squared distance between two AABBs: no point pair across them
/// can be farther. Drives the `dwithin` early-TRUE (all pairs within limit).
/// Plain ops for the same libm-`fma` reason as [`aabb_distance_squared`].
pub(crate) fn aabb_max_distance_squared(a: [f64; 4], b: [f64; 4]) -> f64 {
    let dx = (a[2] - b[0]).max(b[2] - a[0]);
    let dy = (a[3] - b[1]).max(b[3] - a[1]);
    dx * dx + dy * dy
}

pub(crate) const fn point_aabb(x: f64, y: f64) -> [f64; 4] {
    [x, y, x, y]
}

pub(crate) const fn segment_aabb(segment: Segment) -> [f64; 4] {
    [
        segment.start.x.min(segment.end.x),
        segment.start.y.min(segment.end.y),
        segment.start.x.max(segment.end.x),
        segment.start.y.max(segment.end.y),
    ]
}

pub(crate) const fn union_aabb(left: [f64; 4], right: [f64; 4]) -> [f64; 4] {
    [
        left[0].min(right[0]),
        left[1].min(right[1]),
        left[2].max(right[2]),
        left[3].max(right[3]),
    ]
}

/// Whether two AABBs share any point (inclusive edges — touching segments
/// have touching boxes).
pub(crate) fn aabbs_overlap(a: [f64; 4], b: [f64; 4]) -> bool {
    a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]
}
