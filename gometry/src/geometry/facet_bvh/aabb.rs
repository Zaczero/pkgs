use crate::geometry::Segment;

/// Squared distance estimate between two AABBs (0 when overlapping).
/// Plain mul+add, not `mul_add`: scalar `mul_add` on the portable baseline
/// is a libm `fma` CALL, and this bound runs once per visited node (14% of
/// tree-heavy dwithin in profile). Clearance pruning uses
/// [`aabb_distance_squared_lower`] instead of this rounded measurement.
pub(crate) fn aabb_distance_squared(a: [f64; 4], b: [f64; 4]) -> f64 {
    let (dx, dy) = aabb_gaps(a, b);
    dx * dx + dy * dy
}

/// Downward enclosure of squared distance between two AABBs.
///
/// Each positive subtraction, product, and sum is moved one representable
/// value toward negative infinity before it can be used to reject an exact
/// candidate. The bound may be loose at underflow or overflow; it never
/// overstates the source-space lower bound.
pub(crate) fn aabb_distance_squared_lower(a: [f64; 4], b: [f64; 4]) -> f64 {
    let downward_gap = |left: f64, right: f64| (left - right).next_down().max(0.0);
    let dx = downward_gap(a[0], b[2]).max(downward_gap(b[0], a[2]));
    let dy = downward_gap(a[1], b[3]).max(downward_gap(b[1], a[3]));
    let dx_squared = (dx * dx).next_down().max(0.0);
    let dy_squared = (dy * dy).next_down().max(0.0);
    (dx_squared + dy_squared).next_down().max(0.0)
}

/// Distance estimate between two AABBs. Ordinary squares use sqrt; overflow
/// and underflow use a scaled norm.
pub(crate) fn aabb_distance(a: [f64; 4], b: [f64; 4]) -> f64 {
    let (dx, dy) = aabb_gaps(a, b);
    let squared = dx * dx + dy * dy;
    if squared.is_normal() || (dx == 0.0 && dy == 0.0) {
        squared.sqrt()
    } else {
        aabb_distance_lower(a, b)
    }
}

/// Downward enclosure of distance between finite Cartesian boxes.
///
/// Lower every rounded operation, including gaps before normalization. Scaling
/// keeps gaps useful when their squares underflow or overflow. An overflowing
/// gap or product lowers to `f64::MAX`, still below the exact distance.
pub(crate) fn aabb_distance_lower(a: [f64; 4], b: [f64; 4]) -> f64 {
    let (dx, dy) = aabb_gaps(a, b);
    let dx = dx.next_down().max(0.0);
    let dy = dy.next_down().max(0.0);
    let scale = dx.max(dy);
    if scale == 0.0 {
        return 0.0;
    }
    let nx = (dx / scale).next_down().max(0.0);
    let ny = (dy / scale).next_down().max(0.0);
    let sum = ((nx * nx).next_down().max(0.0) + (ny * ny).next_down().max(0.0))
        .next_down()
        .max(0.0);
    (sum.sqrt().next_down().max(0.0) * scale)
        .next_down()
        .max(0.0)
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
    use crate::geometry::tessellation::exact::ExactDyadic;

    fn exact_squared_distance(left: [f64; 2], right: [f64; 2]) -> ExactDyadic {
        let dx = ExactDyadic::from_f64(left[0])
            .try_subtract(ExactDyadic::from_f64(right[0]))
            .unwrap();
        let dy = ExactDyadic::from_f64(left[1])
            .try_subtract(ExactDyadic::from_f64(right[1]))
            .unwrap();
        dx.try_product(&dx)
            .unwrap()
            .try_add(dy.try_product(&dy).unwrap())
            .unwrap()
    }

    #[test]
    fn distance_bound_retains_a_gap_whose_square_underflows() {
        let gap = f64::from_bits(323_u64 << 52);
        let bound = aabb_distance(point_aabb(0.0, 0.0), point_aabb(gap, 0.0));
        assert!(bound > 0.0);
        assert!(bound <= gap);
    }

    #[test]
    fn clearance_squared_bound_is_below_exact_source_square() {
        use crate::geometry::numerics::product::exact_power_of_two;
        let cases = [
            ([0.0, 0.0], [1.0, 2.0]),
            ([0.0, 0.0], [f64::from_bits(323_u64 << 52), 0.0]),
            ([0.0, 0.0], [
                exact_power_of_two(600),
                exact_power_of_two(599),
            ]),
            ([0.0, 0.0], [
                exact_power_of_two(-700),
                exact_power_of_two(-701),
            ]),
        ];
        for (left, right) in cases {
            let lower = aabb_distance_squared_lower(
                point_aabb(left[0], left[1]),
                point_aabb(right[0], right[1]),
            );
            let exact = exact_squared_distance(left, right);
            assert!(
                !ExactDyadic::from_f64(lower).cmp(&exact).is_gt(),
                "AABB lower bound {lower:?} exceeded exact squared distance {exact:?}",
            );
        }
    }

    #[test]
    fn clearance_squared_bound_survives_a_translated_rounded_up_square_sum() {
        use crate::geometry::numerics::product::exact_power_of_two;
        let origin = exact_power_of_two(60);
        let left = [origin, origin];
        let right = [
            origin + exact_power_of_two(40),
            origin + 3.0 * exact_power_of_two(12),
        ];
        let left_aabb = point_aabb(left[0], left[1]);
        let right_aabb = point_aabb(right[0], right[1]);
        let exact = exact_squared_distance(left, right);
        let rounded = aabb_distance_squared(left_aabb, right_aabb);

        assert!(
            ExactDyadic::from_f64(rounded).cmp(&exact).is_gt(),
            "this translated fixture must distinguish the rounded estimate",
        );
        let lower = aabb_distance_squared_lower(left_aabb, right_aabb);
        assert!(
            !ExactDyadic::from_f64(lower).cmp(&exact).is_gt(),
            "directed bound {lower:?} exceeded exact squared distance {exact:?}",
        );
    }

    #[rstest::rstest]
    #[case::overlap([0.0, -1.0, 3.0, 2.0], [1.0, 0.0, 4.0, 3.0], [0.0, 0.0], [0.0, 0.0])]
    #[case::touch_signed_zero([-1.0, -1.0, -0.0, -0.0], [0.0, 0.0, 1.0, 1.0], [-0.0, -0.0], [0.0, 0.0])]
    #[case::two_gaps([0.0, 0.0, 1.0, 2.0], [4.0, 6.0, 5.0, 7.0], [1.0, 2.0], [4.0, 6.0])]
    #[case::subnormal(point_aabb(0.0, 0.0), point_aabb(f64::from_bits(7), f64::from_bits(11)), [0.0, 0.0], [f64::from_bits(7), f64::from_bits(11)])]
    #[case::normal_boundary(point_aabb(0.0, 0.0), point_aabb(f64::MIN_POSITIVE, f64::MIN_POSITIVE.next_down()), [0.0, 0.0], [f64::MIN_POSITIVE, f64::MIN_POSITIVE.next_down()])]
    #[case::underflowed_square(point_aabb(0.0, 0.0), point_aabb(1e-200, 2e-200), [0.0, 0.0], [1e-200, 2e-200])]
    #[case::overflowed_square(point_aabb(0.0, 0.0), point_aabb(1e200, 2e200), [0.0, 0.0], [1e200, 2e200])]
    #[case::overflowed_gap(point_aabb(-f64::MAX, -f64::MAX), point_aabb(f64::MAX, f64::MAX), [-f64::MAX, -f64::MAX], [f64::MAX, f64::MAX])]
    #[case::translated(point_aabb(1e100, 1e100), point_aabb(1e100_f64.next_up(), 1e100_f64.next_up().next_up()), [1e100, 1e100], [1e100_f64.next_up(), 1e100_f64.next_up().next_up()])]
    fn nearest_bounds_do_not_exceed_exact_dyadic_box_distance(
        #[case] left: [f64; 4],
        #[case] right: [f64; 4],
        #[case] left_nearest: [f64; 2],
        #[case] right_nearest: [f64; 2],
    ) {
        let exact = exact_squared_distance(left_nearest, right_nearest);
        for (a, b) in [(left, right), (right, left)] {
            let lower_squared = aabb_distance_squared_lower(a, b);
            assert!(lower_squared >= 0.0);
            assert!(!ExactDyadic::from_f64(lower_squared).cmp(&exact).is_gt());
            let lower = aabb_distance_lower(a, b);
            assert!(lower >= 0.0);
            let dyadic = ExactDyadic::from_f64(lower);
            assert!(!dyadic.try_product(&dyadic).unwrap().cmp(&exact).is_gt());
        }
    }
}
