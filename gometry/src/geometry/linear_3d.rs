#![expect(
    clippy::many_single_char_names,
    reason = "the segment metric follows the standard closest-points notation"
)]

use crate::error::Result;
use crate::geometry::tessellation::exact::{ExactDyadic, round_sqrt_ratio};
use crate::geometry::{DdInterval, GeometryErrorKind, Ordering, dd_strict_cmp};

struct ExactSquaredDistance {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

fn exact_point(point: [f64; 3]) -> [ExactDyadic; 3] {
    point.map(ExactDyadic::from_f64)
}

fn exact_sub(left: &[ExactDyadic; 3], right: &[ExactDyadic; 3]) -> Result<[ExactDyadic; 3]> {
    Ok([
        left[0].try_clone()?.try_subtract(right[0].try_clone()?)?,
        left[1].try_clone()?.try_subtract(right[1].try_clone()?)?,
        left[2].try_clone()?.try_subtract(right[2].try_clone()?)?,
    ])
}

fn exact_dot(left: &[ExactDyadic; 3], right: &[ExactDyadic; 3]) -> Result<ExactDyadic> {
    let first = left[0].try_product(&right[0])?;
    let second = left[1].try_product(&right[1])?;
    let third = left[2].try_product(&right[2])?;
    first.try_add(second)?.try_add(third)
}

fn exact_squared_sum(values: &[ExactDyadic; 3]) -> Result<ExactDyadic> {
    let first = values[0].try_product(&values[0])?;
    let second = values[1].try_product(&values[1])?;
    let third = values[2].try_product(&values[2])?;
    first.try_add(second)?.try_add(third)
}

fn exact_point_pair_squared(
    left: &[ExactDyadic; 3],
    right: &[ExactDyadic; 3],
) -> Result<ExactSquaredDistance> {
    let difference = exact_sub(left, right)?;
    Ok(ExactSquaredDistance {
        numerator: exact_squared_sum(&difference)?,
        denominator: ExactDyadic::from_f64(1.0),
    })
}

fn exact_point_segment_squared(
    point: &[ExactDyadic; 3],
    start: &[ExactDyadic; 3],
    end: &[ExactDyadic; 3],
) -> Result<ExactSquaredDistance> {
    let direction = exact_sub(end, start)?;
    let offset = exact_sub(point, start)?;
    let denominator = exact_dot(&direction, &direction)?;
    let zero = ExactDyadic::from_f64(0.0);
    if denominator.is_zero() {
        return exact_point_pair_squared(point, start);
    }
    let numerator = exact_dot(&offset, &direction)?;
    if numerator.cmp(&zero) != Ordering::Greater {
        return exact_point_pair_squared(point, start);
    }
    if numerator.cmp(&denominator) != Ordering::Less {
        return exact_point_pair_squared(point, end);
    }
    let residual = [
        offset[0]
            .try_product(&denominator)?
            .try_subtract(direction[0].try_product(&numerator)?)?,
        offset[1]
            .try_product(&denominator)?
            .try_subtract(direction[1].try_product(&numerator)?)?,
        offset[2]
            .try_product(&denominator)?
            .try_subtract(direction[2].try_product(&numerator)?)?,
    ];
    Ok(ExactSquaredDistance {
        numerator: exact_squared_sum(&residual)?,
        denominator: denominator.try_product(&denominator)?,
    })
}

fn exact_min_squared(
    left: ExactSquaredDistance,
    right: ExactSquaredDistance,
) -> Result<ExactSquaredDistance> {
    let left_cross = left.numerator.try_product(&right.denominator)?;
    let right_cross = right.numerator.try_product(&left.denominator)?;
    Ok(if left_cross.cmp(&right_cross) == Ordering::Greater {
        right
    } else {
        left
    })
}

fn exact_boundary_squared(
    p1: &[ExactDyadic; 3],
    q1: &[ExactDyadic; 3],
    p2: &[ExactDyadic; 3],
    q2: &[ExactDyadic; 3],
) -> Result<ExactSquaredDistance> {
    let candidates = [
        exact_point_segment_squared(p1, p2, q2)?,
        exact_point_segment_squared(q1, p2, q2)?,
        exact_point_segment_squared(p2, p1, q1)?,
        exact_point_segment_squared(q2, p1, q1)?,
    ];
    let mut candidates = candidates.into_iter();
    let mut best = candidates.next().expect("four boundary candidates");
    for candidate in candidates {
        best = exact_min_squared(best, candidate)?;
        if best.numerator.is_zero() {
            break;
        }
    }
    Ok(best)
}

fn exact_segment_squared(
    p1: [f64; 3],
    q1: [f64; 3],
    p2: [f64; 3],
    q2: [f64; 3],
) -> Result<ExactSquaredDistance> {
    let p1 = exact_point(p1);
    let q1 = exact_point(q1);
    let p2 = exact_point(p2);
    let q2 = exact_point(q2);
    let d1 = exact_sub(&q1, &p1)?;
    let d2 = exact_sub(&q2, &p2)?;
    let r = exact_sub(&p1, &p2)?;
    let a = exact_dot(&d1, &d1)?;
    let e = exact_dot(&d2, &d2)?;
    let zero = ExactDyadic::from_f64(0.0);
    if a.is_zero() && e.is_zero() {
        return exact_point_pair_squared(&p1, &p2);
    }
    if a.is_zero() {
        return exact_point_segment_squared(&p1, &p2, &q2);
    }
    if e.is_zero() {
        return exact_point_segment_squared(&p2, &p1, &q1);
    }
    let b = exact_dot(&d1, &d2)?;
    let c = exact_dot(&d1, &r)?;
    let f = exact_dot(&d2, &r)?;
    let delta = a.try_product(&e)?.try_subtract(b.try_product(&b)?)?;
    debug_assert_ne!(
        delta.cmp(&zero),
        Ordering::Less,
        "exact Gram determinant must be nonnegative"
    );
    if delta.is_zero() {
        return exact_boundary_squared(&p1, &q1, &p2, &q2);
    }
    let ns = b.try_product(&f)?.try_subtract(c.try_product(&e)?)?;
    let nt = a.try_product(&f)?.try_subtract(b.try_product(&c)?)?;
    if ns.cmp(&zero) == Ordering::Greater
        && ns.cmp(&delta) == Ordering::Less
        && nt.cmp(&zero) == Ordering::Greater
        && nt.cmp(&delta) == Ordering::Less
    {
        let residual = [
            r[0].try_product(&delta)?
                .try_add(d1[0].try_product(&ns)?)?
                .try_subtract(d2[0].try_product(&nt)?)?,
            r[1].try_product(&delta)?
                .try_add(d1[1].try_product(&ns)?)?
                .try_subtract(d2[1].try_product(&nt)?)?,
            r[2].try_product(&delta)?
                .try_add(d1[2].try_product(&ns)?)?
                .try_subtract(d2[2].try_product(&nt)?)?,
        ];
        return Ok(ExactSquaredDistance {
            numerator: exact_squared_sum(&residual)?,
            denominator: delta.try_product(&delta)?,
        });
    }
    exact_boundary_squared(&p1, &q1, &p2, &q2)
}

fn exact_segment_distance_3d(
    p1: [f64; 3],
    q1: [f64; 3],
    p2: [f64; 3],
    q2: [f64; 3],
) -> Result<f64> {
    let squared = exact_segment_squared(p1, q1, p2, q2)?;
    round_sqrt_ratio(&squared.numerator, &squared.denominator)
}

fn dd_dot(left: &[DdInterval; 3], right: &[DdInterval; 3]) -> Option<DdInterval> {
    left[0]
        .mul(right[0])?
        .add(left[1].mul(right[1])?)?
        .add(left[2].mul(right[2])?)
}

fn dd_midpoint_square(value: f64, upward: bool) -> Option<DdInterval> {
    let neighbor = if upward {
        value.next_up()
    } else {
        value.next_down()
    };
    DdInterval::exact(neighbor)
        .add(DdInterval::exact(value))?
        .scale(0.5)?
        .square()
}

fn fast_interior_distance_3d(
    p1: [f64; 3],
    q1: [f64; 3],
    p2: [f64; 3],
    q2: [f64; 3],
) -> Option<f64> {
    let d1 = [
        DdInterval::difference(q1[0], p1[0])?,
        DdInterval::difference(q1[1], p1[1])?,
        DdInterval::difference(q1[2], p1[2])?,
    ];
    let d2 = [
        DdInterval::difference(q2[0], p2[0])?,
        DdInterval::difference(q2[1], p2[1])?,
        DdInterval::difference(q2[2], p2[2])?,
    ];
    let r = [
        DdInterval::difference(p1[0], p2[0])?,
        DdInterval::difference(p1[1], p2[1])?,
        DdInterval::difference(p1[2], p2[2])?,
    ];
    let a = dd_dot(&d1, &d1)?;
    let e = dd_dot(&d2, &d2)?;
    let b = dd_dot(&d1, &d2)?;
    let c = dd_dot(&d1, &r)?;
    let f = dd_dot(&d2, &r)?;
    let delta = a.mul(e)?.sub(b.square()?)?;
    let zero = DdInterval::exact(0.0);
    if dd_strict_cmp(delta, zero) != Some(Ordering::Greater) {
        return None;
    }
    let ns = b.mul(f)?.sub(c.mul(e)?)?;
    let nt = a.mul(f)?.sub(b.mul(c)?)?;
    for value in [ns, nt] {
        if dd_strict_cmp(value, zero) != Some(Ordering::Greater)
            || dd_strict_cmp(delta, value) != Some(Ordering::Greater)
        {
            return None;
        }
    }
    let residual = [
        r[0].mul(delta)?.add(d1[0].mul(ns)?)?.sub(d2[0].mul(nt)?)?,
        r[1].mul(delta)?.add(d1[1].mul(ns)?)?.sub(d2[1].mul(nt)?)?,
        r[2].mul(delta)?.add(d1[2].mul(ns)?)?.sub(d2[2].mul(nt)?)?,
    ];
    let squared = residual[0]
        .square()?
        .add(residual[1].square()?)?
        .add(residual[2].square()?)?;
    let denominator = delta.square()?;
    let (squared_head, squared_tail) = squared.approximate_pair();
    let (denominator_head, denominator_tail) = denominator.approximate_pair();
    let approximate_squared = (squared_head + squared_tail) / (denominator_head + denominator_tail);
    if !(approximate_squared.is_finite() && approximate_squared > 0.0) {
        return None;
    }
    let candidate = approximate_squared.sqrt();
    if !(candidate.is_finite() && candidate > 0.0) {
        return None;
    }
    let lower = dd_midpoint_square(candidate, false)?;
    let upper = dd_midpoint_square(candidate, true)?;
    let lower_cross = lower.mul(denominator)?;
    let upper_cross = upper.mul(denominator)?;
    (dd_strict_cmp(squared, lower_cross) == Some(Ordering::Greater)
        && dd_strict_cmp(upper_cross, squared) == Some(Ordering::Greater))
    .then_some(candidate)
}

/// Minimum Euclidean distance between two 3D segments. The DD lane is a
/// certified interior filter; all contacts, boundaries, ties, and uncertain
/// ranges use the exact source-coordinate candidate solver.
pub(crate) fn segment_segment_distance_3d(
    p1: [f64; 3],
    q1: [f64; 3],
    p2: [f64; 3],
    q2: [f64; 3],
) -> Result<f64> {
    if !p1
        .into_iter()
        .chain(q1)
        .chain(p2)
        .chain(q2)
        .all(f64::is_finite)
    {
        return Err(GeometryErrorKind::message(
            "3D segment distance requires finite coordinates",
        ));
    }
    if let Some(distance) = fast_interior_distance_3d(p1, q1, p2, q2) {
        return Ok(distance);
    }
    exact_segment_distance_3d(p1, q1, p2, q2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dd_filter_accepts_strict_interior_cases() {
        assert_eq!(
            fast_interior_distance_3d([0.0, 0.0, 0.0], [10.0, 0.0, 0.0], [5.0, -5.0, 1.0], [
                5.0, 5.0, 1.0
            ],)
            .expect("interior DD certificate"),
            1.0
        );
        assert_eq!(
            fast_interior_distance_3d([0.0, 0.0, 0.0], [10.0, 0.0, 0.0], [5.0, -4.0, -6.0], [
                5.0, 6.0, 4.0
            ],)
            .expect("non-dyadic interior DD certificate"),
            2.0_f64.sqrt()
        );
    }

    #[test]
    fn dd_filter_declines_contacts_and_outside_parameters() {
        assert!(
            fast_interior_distance_3d([0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.1, 0.0], [
                1.0, -0.1, 0.0
            ],)
            .is_none()
        );
        assert!(
            fast_interior_distance_3d([0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [2.0, 1.0, 0.0], [
                2.0, 2.0, 0.0
            ],)
            .is_none()
        );
    }

    #[test]
    fn exact_fallback_propagates_allocation_failures() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::{allocation_count, fail_allocations_after};

        let cases = [
            (
                ([0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.1, 0.0], [
                    1.0, -0.1, 0.0,
                ]),
                0.0,
            ),
            (
                ([0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 2.0, 0.0], [
                    1.0, 2.0, 0.0,
                ]),
                2.0,
            ),
        ];
        for (inputs, expected) in cases {
            let (result, allocations) = allocation_count(|| {
                segment_segment_distance_3d(inputs.0, inputs.1, inputs.2, inputs.3)
            });
            assert_eq!(result.expect("exact fallback result"), expected);
            assert!(allocations > 0);
            for prefix in 0..allocations {
                let result = fail_allocations_after(prefix, || {
                    segment_segment_distance_3d(inputs.0, inputs.1, inputs.2, inputs.3)
                });
                let error = result.expect_err("allocation failure must propagate");
                assert!(matches!(
                    error.kind(),
                    ErrorKind::Resource(ResourceError::Allocation)
                ));
            }
        }
    }
}
