use super::{
    CompiledPattern, GateOperand, Loc, Predicate, PredicateSpec, PreparedPlan,
    PreparedTopologyPair, Shape, ShapeRow, cartesian_point_predicate, predicate_matrix_value,
};

pub(crate) fn topology_scalar_pair(
    spec: &PredicateSpec,
    operands: &PreparedTopologyPair<'_>,
) -> crate::error::Result<bool> {
    let (left, right) = match *operands {
        PreparedTopologyPair::Cartesian { left, right } => {
            let (left_shape, right_shape) = (left.shape(), right.shape());
            if (matches!(left_shape, Shape::Point(_)) || matches!(right_shape, Shape::Point(_)))
                && let Some(answer) = cartesian_point_predicate(spec, left, right)?
            {
                return Ok(answer);
            }
            (
                GateOperand::cartesian(ShapeRow::Shape(left_shape), left.bounds()),
                GateOperand::cartesian(ShapeRow::Shape(right_shape), right.bounds()),
            )
        },
        PreparedTopologyPair::Geographic { left, right } => {
            if let Some(answer) = super::points::geographic_point_predicate(spec, left, right)? {
                return Ok(answer);
            }
            (
                GateOperand::from_prepared(&left),
                GateOperand::from_prepared(&right),
            )
        },
    };
    // The array, index and coverer paths all run the pre-topology gates before
    // admitting a pair to the relation; the scalar pair was the one lane that
    // did not, so `gm.intersects(a, b)` on two operands a hundred units apart
    // still built the exact pair topology to discover they share no point.
    if let Some(verdict) = spec.pre_topology(&left, &right) {
        return Ok(verdict);
    }
    topology_pair_after_gates(spec, operands)
}

pub(crate) fn topology_shapes_predicate(
    predicate: Predicate,
    left: &Shape,
    right: &Shape,
) -> crate::error::Result<bool> {
    let left = PreparedPlan::transient(left);
    let right = PreparedPlan::transient(right);
    topology_scalar_pair(
        &predicate.spec(),
        &PreparedTopologyPair::cartesian(&left, &right),
    )
}

/// Decide a pair after its caller's pre-topology gates have declined.
pub(crate) fn topology_pair_after_gates(
    spec: &PredicateSpec,
    operands: &PreparedTopologyPair<'_>,
) -> crate::error::Result<bool> {
    if let PreparedTopologyPair::Cartesian { left, right } = *operands {
        if let Some(verdict) = cartesian_point_predicate(spec, left, right)? {
            return Ok(verdict);
        }
        return crate::geometry::evaluate_cartesian_predicate(left, right, spec.predicate);
    }
    if let PreparedTopologyPair::Geographic { left, right } = *operands
        && let Some(answer) = super::points::geographic_point_predicate(spec, left, right)?
    {
        return Ok(answer);
    }
    Ok(predicate_matrix_value(spec, operands.relate()?))
}

/// A DE-9IM pattern over a scalar pair, decided as early as the evidence
/// allows: sound disjoint bounds settle every cell the bounds prove (a
/// curve's boundary cells stay open, since whether a closed line has a
/// boundary is a staging fact), and only a pair they leave open builds the
/// matrix.
pub(crate) fn topology_scalar_pattern(
    pattern: CompiledPattern,
    operands: &PreparedTopologyPair<'_>,
) -> crate::error::Result<bool> {
    match *operands {
        PreparedTopologyPair::Cartesian { left, right } => {
            crate::geometry::evaluate_cartesian_pattern(left, right, pattern)
        },
        PreparedTopologyPair::Geographic { left, right } => {
            let (left, right) = (
                GateOperand::from_prepared(&left),
                GateOperand::from_prepared(&right),
            );
            if let Some(verdict) = pattern_bounds_gate(pattern, &left, &right)? {
                return Ok(verdict);
            }
            Ok(operands.relate()?.matches(pattern))
        },
    }
}

/// What sound, disjoint bounds prove about a pattern: the four inner cells
/// are empty and each operand lies wholly in the other's exterior. Surface
/// dimensions are admitted only for a filled rectangle certificate; all other
/// areal support remains undecided here rather than inferred from kind.
pub(super) fn pattern_bounds_gate(
    pattern: CompiledPattern,
    left: &GateOperand<'_>,
    right: &GateOperand<'_>,
) -> crate::error::Result<Option<bool>> {
    use crate::geometry::{De9imInterval, Dimension, OperandShape, dimension};
    if !left.bounds_sound || !right.bounds_sound {
        return Ok(None);
    }
    let (Some(left_bounds), Some(right_bounds)) = (left.bounds, right.bounds) else {
        return Ok(None);
    };
    if left_bounds.intersects(right_bounds) {
        return Ok(None);
    }
    let shape_of = |operand: GateOperand<'_>| -> crate::error::Result<Option<OperandShape>> {
        let Some(interior) = operand.row.with_shape(dimension)? else {
            return Ok(None);
        };
        if interior == Dimension::Surface {
            let Some(bounds) = operand.bounds else {
                return Ok(None);
            };
            let is_filled_rectangle = operand.row.try_with_shape(|shape| {
                Ok::<_, crate::error::Error>(crate::geometry::is_axis_aligned_rectangle(
                    shape, bounds,
                ))
            })?;
            if !is_filled_rectangle {
                return Ok(None);
            }
        }
        Ok(Some(OperandShape {
            interior,
            boundary: match interior {
                Dimension::Surface => Some(Dimension::Curve),
                // Open or closed is a staging fact; leave the cell's bound.
                Dimension::Curve => Some(Dimension::Point),
                Dimension::Point => None,
            },
        }))
    };
    let (Some(a), Some(b)) = (shape_of(*left)?, shape_of(*right)?) else {
        return Ok(None);
    };
    let mut interval = De9imInterval::open(a, b);
    for (row, col) in [
        (Loc::Interior, Loc::Interior),
        (Loc::Interior, Loc::Boundary),
        (Loc::Boundary, Loc::Interior),
        (Loc::Boundary, Loc::Boundary),
    ] {
        interval.settle(row, col);
    }
    interval.witness(Loc::Interior, Loc::Exterior, a.interior);
    interval.settle(Loc::Interior, Loc::Exterior);
    interval.witness(Loc::Exterior, Loc::Interior, b.interior);
    interval.settle(Loc::Exterior, Loc::Interior);
    if a.interior == Dimension::Surface {
        interval.witness(Loc::Boundary, Loc::Exterior, Dimension::Curve);
        interval.settle(Loc::Boundary, Loc::Exterior);
    }
    if b.interior == Dimension::Surface {
        interval.witness(Loc::Exterior, Loc::Boundary, Dimension::Curve);
        interval.settle(Loc::Exterior, Loc::Boundary);
    }
    Ok(interval.pattern(pattern))
}
