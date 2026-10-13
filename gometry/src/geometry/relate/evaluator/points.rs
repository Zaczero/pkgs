use super::staging::row_certificates;
use super::*;

pub(super) fn operand_shape(row: ShapeRow<'_>, dimension: Option<Dimension>) -> OperandShape {
    let interior = dimension.unwrap_or(Dimension::Point);
    let boundary = if row.has_area_parts() {
        Some(Dimension::Curve)
    } else if interior == Dimension::Curve {
        Some(Dimension::Point)
    } else {
        None
    };
    OperandShape { interior, boundary }
}

pub(super) fn row_dimension(row: ShapeRow<'_>) -> Option<Dimension> {
    match row {
        ShapeRow::Handle(plan) => dimension(plan.shape()),
        ShapeRow::Shape(shape) => dimension(shape),
        ShapeRow::Point(_) => Some(Dimension::Point),
        ShapeRow::Packed(PackedRow::Points { .. }) => (!row.is_empty()).then_some(Dimension::Point),
        ShapeRow::Packed(PackedRow::Lines { .. }) => {
            let coordinates = row.line_coords()?;
            let mut nonzero = false;
            for [start, end] in coordinates.segment_pairs() {
                nonzero |= !same_point(start.xy(), end.xy());
            }
            if nonzero {
                Some(Dimension::Curve)
            } else if coordinates.coord_count() != 0 {
                Some(Dimension::Point)
            } else {
                None
            }
        },
        ShapeRow::Packed(PackedRow::Polygons { .. }) => {
            (!row.is_empty()).then_some(Dimension::Surface)
        },
    }
}

pub(super) fn settle_empty_operands(interval: &mut De9imInterval, empty: [bool; 2]) {
    for row in [Loc::Interior, Loc::Boundary] {
        if empty[0] {
            for col in LOCATIONS {
                interval.settle(row, col);
            }
        }
    }
    for col in [Loc::Interior, Loc::Boundary] {
        if empty[1] {
            for row in LOCATIONS {
                interval.settle(row, col);
            }
        }
    }
}

pub(super) fn settle_disjoint_interiors(interval: &mut De9imInterval) {
    for row in [Loc::Interior, Loc::Boundary] {
        for col in [Loc::Interior, Loc::Boundary] {
            interval.settle(row, col);
        }
    }
}

pub(super) fn settle_all(interval: &mut De9imInterval) {
    for row in LOCATIONS {
        for col in LOCATIONS {
            interval.settle(row, col);
        }
    }
}

pub(super) fn point_keys(row: ShapeRow<'_>) -> Vec<PointKey> {
    let mut points = Vec::new();
    row.for_each_point_only(|point| points.push(PointKey::new(point.xy())));
    points.sort_unstable();
    points.dedup();
    points
}

pub(super) const fn point_from_key(point: PointKey) -> Point {
    Point::new_raw_xy(f64::from_bits(point.x), f64::from_bits(point.y))
}

pub(super) fn point_only(row: ShapeRow<'_>, dimension: Option<Dimension>) -> bool {
    !row.has_area_parts() && dimension == Some(Dimension::Point)
}

pub(super) fn point_only_phase<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: &[Operand<'_>; 2],
    dimensions: [Option<Dimension>; 2],
    certificates: [SourceCertificates; 2],
    query: &Q,
) -> Result<Option<Q::Output>> {
    let [left, right] = operands;
    let [left_dimension, right_dimension] = dimensions;
    let [left_certificates, right_certificates] = certificates;
    let left_point_only = point_only(left.row, left_dimension);
    let right_point_only = point_only(right.row, right_dimension);
    if left_point_only && right_point_only {
        let left = point_keys(left.row);
        let right = point_keys(right.row);
        let mut left = left.into_iter().peekable();
        let mut right = right.into_iter().peekable();
        loop {
            match (left.peek().copied(), right.peek().copied()) {
                (Some(a), Some(b)) => match a.cmp(&b) {
                    Ordering::Less => {
                        interval.witness(Loc::Interior, Loc::Exterior, Dimension::Point);
                        left.next();
                    },
                    Ordering::Greater => {
                        interval.witness(Loc::Exterior, Loc::Interior, Dimension::Point);
                        right.next();
                    },
                    Ordering::Equal => {
                        interval.witness(Loc::Interior, Loc::Interior, Dimension::Point);
                        left.next();
                        right.next();
                    },
                },
                (Some(_), None) => {
                    interval.witness(Loc::Interior, Loc::Exterior, Dimension::Point);
                    left.next();
                },
                (None, Some(_)) => {
                    interval.witness(Loc::Exterior, Loc::Interior, Dimension::Point);
                    right.next();
                },
                (None, None) => break,
            }
            if let Some(answer) = query.verdict(interval) {
                return Ok(Some(answer));
            }
        }
        settle_all(interval);
        return Ok(query.verdict(interval));
    }

    let point_operand = if left_point_only {
        Some(matrix_predicates::Operand::Left)
    } else if right_point_only {
        Some(matrix_predicates::Operand::Right)
    } else {
        None
    };
    let Some(point_operand) = point_operand else {
        return Ok(None);
    };
    let (points, target, target_certificates) = match point_operand {
        matrix_predicates::Operand::Left => (left, right, right_certificates),
        matrix_predicates::Operand::Right => (right, left, left_certificates),
    };
    if SATURATE && let Some(point_query) = query.point_membership(point_operand) {
        if let Some(point) = points.row.point() {
            return target
                .point_membership(point, point_query.membership)
                .map(|matches| Some(point_query.result(matches)));
        }
        for key in point_keys(points.row) {
            if !target.point_membership(point_from_key(key), point_query.membership)? {
                return Ok(Some(point_query.result(false)));
            }
        }
        return Ok(Some(point_query.result(true)));
    }
    let mut target_area = target_certificates.hole_free_area;
    let answer = {
        let mut probe = |point| -> Result<Option<Q::Output>> {
            let target_location = target.locate_stored(point)?;
            let isolated = SourcePointLocation {
                location: Loc::Interior,
                area: false,
            };
            let (left_location, right_location) = match point_operand {
                matrix_predicates::Operand::Left => (isolated, target_location),
                matrix_predicates::Operand::Right => (target_location, isolated),
            };
            witness_point(
                interval,
                left_location,
                right_location,
                query.needs_dimensions(),
            );
            target_area |= target_location.area;
            Ok(query.verdict(interval))
        };
        if let Some(point) = points.row.point() {
            probe(point)?
        } else {
            let mut answer = None;
            for key in point_keys(points.row) {
                answer = probe(point_from_key(key))?;
                if answer.is_some() {
                    break;
                }
            }
            answer
        }
    };
    if let Some(answer) = answer {
        return Ok(Some(answer));
    }

    if !target_area {
        target_area = row_certificates(target.row)?.hole_free_area;
    }
    if target_area {
        let ((area_row, area_col), (boundary_row, boundary_col)) = match point_operand {
            matrix_predicates::Operand::Left => (
                (Loc::Exterior, Loc::Interior),
                (Loc::Exterior, Loc::Boundary),
            ),
            matrix_predicates::Operand::Right => (
                (Loc::Interior, Loc::Exterior),
                (Loc::Boundary, Loc::Exterior),
            ),
        };
        interval.witness(area_row, area_col, Dimension::Surface);
        interval.witness(boundary_row, boundary_col, Dimension::Curve);
        interval.settle(area_row, area_col);
        interval.settle(boundary_row, boundary_col);
        if let Some(answer) = query.verdict(interval) {
            return Ok(Some(answer));
        }
        // The point operand has no boundary or faces. All of its interior
        // points were sampled, and the area certificate settles the partner's
        // remaining faces and boundary outside those finitely many points.
        for row in LOCATIONS {
            for col in LOCATIONS {
                let point_cell_is_interior = match point_operand {
                    matrix_predicates::Operand::Left => row == Loc::Interior,
                    matrix_predicates::Operand::Right => col == Loc::Interior,
                };
                if point_cell_is_interior {
                    interval.settle(row, col);
                }
            }
        }
        return Ok(query.verdict(interval));
    }
    Ok(None)
}

pub(super) fn witness_point(
    interval: &mut De9imInterval,
    left: SourcePointLocation,
    right: SourcePointLocation,
    needs_dimensions: bool,
) {
    if left.location != Loc::Exterior || right.location != Loc::Exterior {
        interval.witness(left.location, right.location, Dimension::Point);
    }
    if left.area && needs_dimensions {
        interval.floor(matrix_predicates::Operand::Left, Dimension::Surface);
    }
    if right.area && needs_dimensions {
        interval.floor(matrix_predicates::Operand::Right, Dimension::Surface);
    }

    if left.area && right.location == Loc::Exterior {
        interval.witness(Loc::Interior, Loc::Exterior, Dimension::Surface);
        if left.location == Loc::Boundary {
            interval.witness(Loc::Boundary, Loc::Exterior, Dimension::Curve);
        }
    }
    if right.area && left.location == Loc::Exterior {
        interval.witness(Loc::Exterior, Loc::Interior, Dimension::Surface);
        if right.location == Loc::Boundary {
            interval.witness(Loc::Exterior, Loc::Boundary, Dimension::Curve);
        }
    }
    if left.area && right.area {
        if right.location == Loc::Interior {
            interval.witness(Loc::Interior, Loc::Interior, Dimension::Surface);
            if left.location == Loc::Boundary {
                interval.witness(Loc::Boundary, Loc::Interior, Dimension::Curve);
            }
        }
        if left.location == Loc::Interior {
            interval.witness(Loc::Interior, Loc::Interior, Dimension::Surface);
            if right.location == Loc::Boundary {
                interval.witness(Loc::Interior, Loc::Boundary, Dimension::Curve);
            }
        }
    }
}
