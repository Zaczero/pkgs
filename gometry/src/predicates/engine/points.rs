#[cfg(all(test, debug_assertions))]
use super::point_predicate_dispatch;
use super::{
    Loc, PointMembership, Predicate, PredicateSpec, PreparedPlan, ShapeRow,
    cartesian_row_point_location,
};

pub(super) fn cartesian_point_predicate(
    spec: &PredicateSpec,
    left: &PreparedPlan,
    right: &PreparedPlan,
) -> crate::error::Result<Option<bool>> {
    #[cfg(all(test, debug_assertions))]
    point_predicate_dispatch();
    if matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
        return point_intersection_value(
            spec,
            ShapeRow::Shape(left.shape()).point(),
            ShapeRow::Shape(right.shape()).point(),
            |left_side, point| {
                (if left_side { left } else { right })
                    .cartesian_point_membership(point, PointMembership::BoundaryInclusive)
            },
        );
    }
    for (target, point_operand, point_is_left) in [(left, right, false), (right, left, true)] {
        if let Some(membership) = spec.predicate.point_membership(point_is_left)
            && let Some(point) = ShapeRow::Shape(point_operand.shape()).point()
        {
            return target
                .cartesian_point_membership(point, membership)
                .map(Some);
        }
    }
    Ok(None)
}

pub(super) fn geographic_point_predicate(
    spec: &PredicateSpec,
    left: super::GeographicPrepared<'_>,
    right: super::GeographicPrepared<'_>,
) -> crate::error::Result<Option<bool>> {
    for (target, point_operand, point_is_left) in [(left, right, false), (right, left, true)] {
        let Some(point) = ShapeRow::Shape(point_operand.plan().shape()).point() else {
            continue;
        };
        if matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
            return target
                .point_covers(point)
                .map(|intersects| Some(intersects == (spec.predicate == Predicate::Intersects)));
        }
        if let Some(membership) = spec.predicate.point_membership(point_is_left) {
            return target.point_membership(point, membership).map(Some);
        }
    }
    Ok(None)
}

pub(crate) fn row_point_intersection(
    spec: &PredicateSpec,
    left: ShapeRow<'_>,
    right: ShapeRow<'_>,
) -> crate::error::Result<Option<bool>> {
    if !matches!(spec.predicate, Predicate::Intersects | Predicate::Disjoint) {
        return Ok(None);
    }
    let target = if right.point().is_some() { left } else { right };
    if crate::array::packed_row_persistent!(target) {
        return Ok(None);
    }
    point_intersection_value(spec, left.point(), right.point(), |left_side, point| {
        Ok(
            cartesian_row_point_location(if left_side { left } else { right }, point, true)
                != Loc::Exterior,
        )
    })
}

fn point_intersection_value(
    spec: &PredicateSpec,
    left_point: Option<crate::geometry::Point>,
    right_point: Option<crate::geometry::Point>,
    mut covers: impl FnMut(bool, crate::geometry::Point) -> crate::error::Result<bool>,
) -> crate::error::Result<Option<bool>> {
    let intersects = match (left_point, right_point) {
        (Some(left), Some(right)) => crate::geometry::same_point(left, right),
        (Some(point), None) => covers(false, point)?,
        (None, Some(point)) => covers(true, point)?,
        (None, None) => return Ok(None),
    };
    Ok(Some(if spec.predicate == Predicate::Disjoint {
        !intersects
    } else {
        intersects
    }))
}
