#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::relate::{
    De9im, LinealOperand, fully_covered, group_intervals_by_index, projection_interval,
};
use crate::geometry::{
    SEGMENT_INDEX_MIN_PAIRS, SegmentContact, Shape, for_each_bipartite_index_pair,
    point_on_segment, segment_contact, segment_envelopes_disjoint, shared_segment_part,
};

/// Native line × line DE-9IM from one classified contact scan.
///
/// - Collinear runs mark shared boundary intervals on both operands; the
///   residue of each operand's uncovered intervals is the open interior
///   analysis that decides `IE`/`EI` EXACTLY (the uncovered residue of a
///   closed-interval cover is open, hence 1-D or empty).
/// - Every other touch happens at a segment endpoint; each touch point
///   classifies by the operands' mod-2 boundary sets into `II`/`IB`/`BI`/`BB`
///   (all 0-dimensional — a line boundary is a finite point set, so those
///   entries are never `1`).
///
/// Boundary rows finish with direct membership: a boundary endpoint off
/// the other line entirely is `BE`/`EB`.
pub(crate) fn lineal_relate_shapes(left: &Shape, right: &Shape) -> Option<De9im> {
    let a = LinealOperand::from_shape(left)?;
    let b = LinealOperand::from_shape(right)?;
    let mut ii_point = false;
    let mut shared_run = false;
    let mut ib = false;
    let mut bi = false;
    let mut bb = false;
    // Covered t-intervals per segment, filled by collinear runs.
    let mut cover_a_flat = Vec::new();
    let mut cover_b_flat = Vec::new();
    {
        let mut visit = |a_index: usize, b_index: usize| {
            let (left, right) = (a.segments[a_index], b.segments[b_index]);
            match segment_contact(left, right) {
                SegmentContact::None => return,
                SegmentContact::Cross => {
                    ii_point = true;
                    return;
                },
                SegmentContact::Touch => {},
            }
            if let Some((_, run)) = shared_segment_part(left, right) {
                shared_run = true;
                let (a0, a1) = projection_interval(left, run[0], run[1]);
                cover_a_flat.push((a_index, a0, a1));
                let (b0, b1) = projection_interval(right, run[0], run[1]);
                cover_b_flat.push((b_index, b0, b1));
            }
            // Endpoint-involved contact points (run ends included — a
            // part endpoint inside a collinear run still grades the
            // boundary rows).
            for (point, host) in [
                (right.start, left),
                (right.end, left),
                (left.start, right),
                (left.end, right),
            ] {
                if point_on_segment(point, host.start, host.end) {
                    match (a.interior(point), b.interior(point)) {
                        (true, true) => ii_point = true,
                        (true, false) => ib = true,
                        (false, true) => bi = true,
                        (false, false) => bb = true,
                    }
                }
            }
        };
        // Envelope-pruned pair scan: the brute double loop below the
        // crossover, then the monotone-run bipartite sweep (same candidate
        // superset as the R-tree, but no per-call tree build — the dominant
        // cost on large smooth linework).
        if a.segments.len() * b.segments.len() < SEGMENT_INDEX_MIN_PAIRS {
            for a_index in 0..a.segments.len() {
                for b_index in 0..b.segments.len() {
                    if !segment_envelopes_disjoint(a.segments[a_index], b.segments[b_index]) {
                        visit(a_index, b_index);
                    }
                }
            }
        } else {
            for_each_bipartite_index_pair(&a.segments, &b.segments, &mut visit);
        }
    }
    let mut cover_a = group_intervals_by_index(cover_a_flat, a.segments.len());
    let mut cover_b = group_intervals_by_index(cover_b_flat, b.segments.len());
    let mut matrix = [b'F'; 9];
    matrix[8] = b'2';
    matrix[0] = if shared_run {
        b'1'
    } else if ii_point {
        b'0'
    } else {
        b'F'
    };
    if ib {
        matrix[1] = b'0';
    }
    if bi {
        matrix[3] = b'0';
    }
    if bb {
        matrix[4] = b'0';
    }
    if !fully_covered(&mut cover_a) {
        matrix[2] = b'1';
    }
    if !fully_covered(&mut cover_b) {
        matrix[6] = b'1';
    }
    if a.boundary.iter().any(|key| !b.covers(key.xy())) {
        matrix[5] = b'0';
    }
    if b.boundary.iter().any(|key| !a.covers(key.xy())) {
        matrix[7] = b'0';
    }
    Some(De9im(matrix))
}
