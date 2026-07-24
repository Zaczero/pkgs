use super::*;
use crate::geometry::*;

pub(crate) fn areal_relate_arrangement_oracle(
    left: &[Polygon],
    right: &[Polygon],
) -> Option<De9im> {
    let built = overlay::build_areal_arrangement(left, right);
    if !built.operand_present[0] || !built.operand_present[1] {
        return None;
    }
    let mut matrix = [b'F'; 9];
    matrix[8] = b'2';
    for &winding in &built.windings {
        let (a, b) = (winding[0] >= 1, winding[1] >= 1);
        if a && b {
            matrix[0] = b'2';
        }
        if a && !b {
            matrix[2] = b'2';
        }
        if !a && b {
            matrix[6] = b'2';
        }
    }
    // Boundary rows: classify each atomic edge piece, and track operand
    // incidence per vertex for the corner-touch (`BB = 0`) case.
    let mut incident_a = vec![false; built.arrangement.vertex_count()];
    let mut incident_b = vec![false; built.arrangement.vertex_count()];
    let mut shared_edge = false;
    built
        .arrangement
        .for_each_edge_piece(|multiplicity, from, to, left_face, _right_face| {
            let a_edge = multiplicity[0] != 0;
            let b_edge = multiplicity[1] != 0;
            if a_edge {
                incident_a[from as usize] = true;
                incident_a[to as usize] = true;
            }
            if b_edge {
                incident_b[from as usize] = true;
                incident_b[to as usize] = true;
            }
            match (a_edge, b_edge) {
                (true, true) => shared_edge = true,
                (true, false) => {
                    // The other operand's winding is constant across a
                    // one-operand edge — either side face reads it.
                    if built.windings[left_face as usize][1] >= 1 {
                        matrix[3] = b'1';
                    } else {
                        matrix[5] = b'1';
                    }
                },
                (false, true) => {
                    if built.windings[left_face as usize][0] >= 1 {
                        matrix[1] = b'1';
                    } else {
                        matrix[7] = b'1';
                    }
                },
                // Net-zero pieces (cancelled coincident boundary) bound
                // nothing — the winding doctrine for degenerate input.
                (false, false) => {},
            }
        });
    if shared_edge {
        matrix[4] = b'1';
    } else if incident_a.iter().zip(&incident_b).any(|(&a, &b)| a && b) {
        matrix[4] = b'0';
    }
    Some(De9im(matrix))
}

/// The native matrix for two shapes when BOTH are pure polygonal
/// (`Polygon` / `MultiPolygon`) — the lane behind `relate`,
/// `relate_pattern`, and the tangential-contact arms of the areal
/// predicates. Collections and mixed dimensions keep the geo fallback.
pub(crate) fn areal_relate_shapes(left: &Shape, right: &Shape) -> Option<De9im> {
    areal_relate_inner(left, right, relate_ng::AreaTesters::default())
}

/// [`areal_relate_shapes`] over cached operands: each side's prepared banded
/// raycaster turns the per-boundary-section membership probes from O(ring)
/// scans into O(band) lookups — the dominant cost on large polygons.
pub(crate) fn areal_relate_data(left: &ShapeData, right: &ShapeData) -> Option<De9im> {
    let (Some(left_rings), Some(right_rings)) = (left.staged_rings(), right.staged_rings()) else {
        return None;
    };
    let testers = relate_ng::AreaTesters {
        left: left.point_tester(),
        right: right.point_tester(),
    };
    match relate_ng::areal_relate_ng_staged(
        left_rings,
        right_rings,
        relate_ng::RelateGoal::Matrix,
        testers,
        left.is_simple_cached() && right.is_simple_cached(),
    ) {
        Some(relate_ng::RelateDecision::Matrix(matrix)) => Some(matrix),
        Some(relate_ng::RelateDecision::Pattern(_)) => unreachable!("matrix goal"),
        None => areal_relate_arrangement_oracle(
            polygon_parts(left.shape())?,
            polygon_parts(right.shape())?,
        ),
    }
}

pub(crate) fn areal_relate_inner(
    left: &Shape,
    right: &Shape,
    testers: relate_ng::AreaTesters<'_>,
) -> Option<De9im> {
    let left = polygon_parts(left)?;
    let right = polygon_parts(right)?;
    match relate_ng::areal_relate_ng(left, right, relate_ng::RelateGoal::Matrix, testers) {
        Some(relate_ng::RelateDecision::Matrix(matrix)) => Some(matrix),
        Some(relate_ng::RelateDecision::Pattern(_)) => unreachable!("matrix goal"),
        None => areal_relate_arrangement_oracle(left, right),
    }
}

pub(crate) fn areal_relate_pattern_shapes(
    left: &Shape,
    right: &Shape,
    pattern: relate_ng::CompiledPattern<'_>,
) -> Option<bool> {
    let left = polygon_parts(left)?;
    let right = polygon_parts(right)?;
    match relate_ng::areal_relate_ng(
        left,
        right,
        relate_ng::RelateGoal::Pattern(pattern),
        relate_ng::AreaTesters::default(),
    ) {
        Some(relate_ng::RelateDecision::Pattern(matches)) => Some(matches),
        Some(relate_ng::RelateDecision::Matrix(_)) => unreachable!("pattern goal"),
        None => areal_relate_arrangement_oracle(left, right).map(|matrix| pattern.matches(matrix)),
    }
}
