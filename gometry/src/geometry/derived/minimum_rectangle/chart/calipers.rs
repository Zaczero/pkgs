//! Retained-chart support selection.

use super::super::corners::minimum_rectangle_not_representable;
use super::super::score::{cmp_wide, wide_product};
use super::super::{
    Basis, Candidate, DOUBLE_LIMBS, ExactSum, LIMBS, Ordering, Result, Supports, TRIPLE_LIMBS,
};

pub(in crate::geometry::derived::minimum_rectangle) fn exact_chart_dot_delta(
    direction: (f64, f64),
    current: crate::geographic::ChartPointSite,
    next: crate::geographic::ChartPointSite,
    frame: crate::geographic::GeographicFrame,
) -> ExactSum {
    let axis = frame.longitude();
    let [current_x, next_x] = [current.longitude(), next.longitude()];
    let mut sum = ExactSum::zero();
    sum.add_product(direction.0, next_x.native().get());
    sum.add_scaled_product(direction.0, axis.period(), next_x.periods(), false);
    sum.subtract_product(direction.0, current_x.native().get());
    sum.add_scaled_product(direction.0, axis.period(), current_x.periods(), true);
    sum.add_product(direction.1, next.source_y());
    sum.subtract_product(direction.1, current.source_y());
    sum
}

pub(in crate::geometry::derived::minimum_rectangle) fn chart_projection_delta_order(
    direction: (f64, f64),
    current: crate::geographic::ChartPointSite,
    next: crate::geographic::ChartPointSite,
    frame: crate::geographic::GeographicFrame,
) -> Ordering {
    exact_chart_dot_delta(direction, current, next, frame).ordering()
}

pub(in crate::geometry::derived::minimum_rectangle) fn exact_chart_width(
    direction: (f64, f64),
    minimum: crate::geographic::ChartPointSite,
    maximum: crate::geographic::ChartPointSite,
    frame: crate::geographic::GeographicFrame,
) -> [u64; LIMBS] {
    let exact = exact_chart_dot_delta(direction, minimum, maximum, frame);
    let (sign, magnitude) = exact.signed_magnitude();
    debug_assert_eq!(sign, Ordering::Greater);
    magnitude
}

#[expect(
    clippy::indexing_slicing,
    reason = "the four caliper support indices are emitted by this admitted chart hull search"
)]
pub(in crate::geometry::derived::minimum_rectangle) fn chart_candidate_area_order(
    hull: &[crate::geographic::ChartPointSite],
    left: Candidate,
    right: Candidate,
    frame: crate::geographic::GeographicFrame,
) -> Ordering {
    let supports = left.supports;
    let left_a = exact_chart_width(
        left.basis.along,
        hull[supports.along_min],
        hull[supports.along_max],
        frame,
    );
    let left_o = exact_chart_width(
        left.basis.outward,
        hull[supports.outward_min],
        hull[supports.outward_max],
        frame,
    );
    let left_n = left.basis.norm_exact().signed_magnitude().1;
    let supports = right.supports;
    let right_a = exact_chart_width(
        right.basis.along,
        hull[supports.along_min],
        hull[supports.along_max],
        frame,
    );
    let right_o = exact_chart_width(
        right.basis.outward,
        hull[supports.outward_min],
        hull[supports.outward_max],
        frame,
    );
    let right_n = right.basis.norm_exact().signed_magnitude().1;
    let left_ao: [u64; DOUBLE_LIMBS] = wide_product(&left_a, &left_o);
    let right_ao: [u64; DOUBLE_LIMBS] = wide_product(&right_a, &right_o);
    let lhs: [u64; TRIPLE_LIMBS] = wide_product(&left_ao, &right_n);
    let rhs: [u64; TRIPLE_LIMBS] = wide_product(&right_ao, &left_n);
    cmp_wide(&lhs, &rhs)
}

#[expect(
    clippy::indexing_slicing,
    reason = "caliper indices are advanced only within this admitted chart hull"
)]
pub(in crate::geometry::derived::minimum_rectangle) fn exact_chart_argmax(
    hull: &[crate::geographic::ChartPointSite],
    direction: (f64, f64),
    frame: crate::geographic::GeographicFrame,
) -> usize {
    (1..hull.len()).fold(0, |best, index| {
        if chart_projection_delta_order(direction, hull[best], hull[index], frame).is_gt() {
            index
        } else {
            best
        }
    })
}

#[expect(
    clippy::indexing_slicing,
    reason = "the support pointer and its successor are bounded by the chart hull cycle"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the support pointer advances at most one chart hull length"
)]
pub(in crate::geometry::derived::minimum_rectangle) fn advance_chart_max(
    hull: &[crate::geographic::ChartPointSite],
    direction: (f64, f64),
    mut index: usize,
    frame: crate::geographic::GeographicFrame,
) -> usize {
    let count = hull.len();
    for _ in 0..count {
        let current = index % count;
        let next = (index + 1) % count;
        if chart_projection_delta_order(direction, hull[current], hull[next], frame).is_lt() {
            break;
        }
        index += 1;
    }
    index
}

#[expect(
    clippy::indexing_slicing,
    reason = "support pointer and its successor are positions in the nonempty chart hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the cyclic successor is bounded by the chart hull length"
)]
pub(in crate::geometry::derived::minimum_rectangle) fn chart_support_stops_here(
    hull: &[crate::geographic::ChartPointSite],
    direction: (f64, f64),
    index: usize,
    frame: crate::geographic::GeographicFrame,
) -> bool {
    let count = hull.len();
    let current = index % count;
    let next = (current + 1) % count;
    chart_projection_delta_order(direction, hull[current], hull[next], frame).is_lt()
}

#[expect(
    clippy::indexing_slicing,
    reason = "the caliper cycle and support indices are created from the admitted chart hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the hull contains at least three sites and each cyclic edge is in range"
)]
pub(in crate::geometry::derived::minimum_rectangle) fn select_minimum_candidate_chart(
    hull: &[crate::geographic::ChartPointSite],
    frame: crate::geographic::GeographicFrame,
) -> Result<Candidate> {
    let count = hull.len();
    let mut first_basis = None;
    for edge in 0..count {
        first_basis = Basis::from_chart_edge(hull[edge], hull[(edge + 1) % count], frame)?;
        if first_basis.is_some() {
            break;
        }
    }
    let first_basis = first_basis.ok_or_else(minimum_rectangle_not_representable)?;
    let outward_direction = (-first_basis.outward.0, -first_basis.outward.1);
    let initial_outward_min = exact_chart_argmax(hull, outward_direction, frame);
    let mut outward_min = advance_chart_max(hull, outward_direction, initial_outward_min, frame);
    let mut along_max = advance_chart_max(hull, first_basis.along, outward_min, frame);
    let mut outward_max = advance_chart_max(hull, first_basis.outward, along_max, frame);
    let mut along_min = advance_chart_max(
        hull,
        (-first_basis.along.0, -first_basis.along.1),
        outward_max,
        frame,
    );
    let mut candidate_outward_min = initial_outward_min;
    let mut best: Option<Candidate> = None;
    for edge in 0..count {
        let basis = Basis::from_chart_edge(hull[edge], hull[(edge + 1) % count], frame)?
            .ok_or_else(minimum_rectangle_not_representable)?;
        if edge != 0 {
            along_max = advance_chart_max(hull, basis.along, along_max, frame);
            outward_min = advance_chart_max(
                hull,
                (-basis.outward.0, -basis.outward.1),
                outward_min,
                frame,
            );
            candidate_outward_min = outward_min;
            outward_max = advance_chart_max(hull, basis.outward, outward_max, frame);
            along_min = advance_chart_max(hull, (-basis.along.0, -basis.along.1), along_min, frame);
        }
        debug_assert!(chart_support_stops_here(
            hull,
            basis.along,
            along_max,
            frame
        ));
        debug_assert!(chart_support_stops_here(
            hull,
            (-basis.outward.0, -basis.outward.1),
            outward_min,
            frame
        ));
        debug_assert!(chart_support_stops_here(
            hull,
            basis.outward,
            outward_max,
            frame
        ));
        debug_assert!(chart_support_stops_here(
            hull,
            (-basis.along.0, -basis.along.1),
            along_min,
            frame
        ));
        let candidate = Candidate {
            basis,
            supports: Supports {
                along_min: along_min % count,
                along_max: along_max % count,
                outward_min: candidate_outward_min % count,
                outward_max: outward_max % count,
            },
        };
        if best.is_none_or(|current| {
            chart_candidate_area_order(hull, candidate, current, frame).is_lt()
        }) {
            best = Some(candidate);
        }
    }
    Ok(best
        .take()
        .expect("a three-site chart hull has at least one edge candidate"))
}
