use crate::geometry::Segment;

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

/// Lower-bound distance between two AABBs after undoing independent positive
/// local-axis scales.  Each positive quotient is rounded down before the
/// final `hypot`, and that result is rounded down as well; zero gaps stay
/// exactly zero so underflow only weakens the bound.
pub(crate) fn aabb_distance_with_scales(
    a: [f64; 4],
    b: [f64; 4],
    scale_x: f64,
    scale_y: f64,
) -> f64 {
    let (gap_x, gap_y) = aabb_gaps(a, b);
    if gap_x == 0.0 && gap_y == 0.0 {
        return 0.0;
    }
    let scaled_gap = |gap: f64, scale: f64| {
        let quotient = gap / scale;
        if quotient > 0.0 {
            quotient.next_down()
        } else {
            0.0
        }
    };
    let distance = scaled_gap(gap_x, scale_x).hypot(scaled_gap(gap_y, scale_y));
    if distance > 0.0 {
        distance.next_down()
    } else {
        0.0
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaled_distance_rounds_down_from_positive_gaps() {
        let aabb = [2.0, 4.0, 3.0, 5.0];
        let probe = point_aabb(0.0, 0.0);
        let lower = aabb_distance_with_scales(aabb, probe, 2.0, 4.0);
        let corner_distance = (2.0_f64 / 2.0).hypot(4.0 / 4.0);
        assert!(lower > 0.0);
        assert!(lower < corner_distance);
    }

    #[test]
    fn scaled_distance_keeps_zero_gaps_at_zero() {
        let probe = point_aabb(0.0, 0.0);
        assert_eq!(
            aabb_distance_with_scales([-1.0, -1.0, 2.0, 1.0], probe, 3.0, 5.0).to_bits(),
            0.0_f64.to_bits()
        );
        assert!(aabb_distance_with_scales([2.0, -1.0, 3.0, 1.0], probe, 3.0, 5.0) > 0.0);
    }

    #[test]
    fn scaled_distance_underflow_only_weakens_a_positive_gap() {
        let tiny = f64::from_bits(1);
        let lower =
            aabb_distance_with_scales([tiny, 0.0, tiny, 0.0], point_aabb(0.0, 0.0), 1.0, 1.0);
        assert_eq!(lower.to_bits(), 0.0_f64.to_bits());
        assert!(lower <= tiny);
    }

    #[test]
    fn scaled_distance_stays_finite_when_unscaled_distance_overflows() {
        let lower = aabb_distance_with_scales(
            [f64::MAX, f64::MAX, f64::MAX, f64::MAX],
            point_aabb(0.0, 0.0),
            1.0,
            1.0,
        );
        assert_eq!(lower.to_bits(), f64::MAX.to_bits());

        let quotient_overflow = aabb_distance_with_scales(
            [f64::MAX, 0.0, f64::MAX, 0.0],
            point_aabb(0.0, 0.0),
            f64::MIN_POSITIVE,
            1.0,
        );
        assert_eq!(quotient_overflow.to_bits(), f64::MAX.next_down().to_bits());
    }
}
