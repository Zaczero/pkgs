//! O(n + m) boundary-contact discovery for two single-ring convex shells.
//!
//! When [`try_collect`]'s gates pass, cross-operand crossings are found by a
//! two-pointer walk over x-monotone upper/lower chains instead of the
//! arrangement-style [`for_each_candidate_pair`] sweep. Touch, collinear
//! overlap, or any gate failure returns `None` so the caller falls back to the
//! general noding path.

use crate::geometry::predicates::shell_is_convex;
use crate::geometry::segments::{Contact, segment_contact_exact, segment_envelopes_disjoint};
use crate::geometry::topology::{Operand, OperandPool, OrientedRing};
use crate::geometry::{Bounds, Segment, XY, wrap_index};

/// Per-segment crossing cuts ready to merge into
/// [`super::clean_union::BoundaryContacts`].
pub(super) struct ConvexBoundaryScan {
    pub cuts: Vec<Vec<ConvexCut>>,
    pub has_cross: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ConvexCut {
    pub point: XY,
    pub cross: bool,
}

/// Discover transverse crossings between two convex hole-free shells.
///
/// `None` means fall back to the general segment sweep (ineligible input,
/// envelope miss, touch/collinear contact, or same-operand pairing).
pub(super) fn try_collect(pool: &OperandPool) -> Option<ConvexBoundaryScan> {
    let (left, right) = eligible_rings(pool)?;
    let n = pool.segments.len();
    let mut cuts: Vec<Vec<ConvexCut>> = vec![Vec::new(); n];
    let mut has_cross = false;

    let left_edges = ring_chains(left);
    let right_edges = ring_chains(right);

    for chain_a in &left_edges {
        for chain_b in &right_edges {
            match scan_chains(pool, chain_a, chain_b, &mut cuts) {
                ScanFlow::Ok(crossed) => has_cross |= crossed,
                ScanFlow::Bail => return None,
            }
        }
    }

    Some(ConvexBoundaryScan { cuts, has_cross })
}

enum ScanFlow {
    Ok(bool),
    Bail,
}

fn eligible_rings(pool: &OperandPool) -> Option<(&OrientedRing, &OrientedRing)> {
    if pool.rings.len() != 2 {
        return None;
    }
    let left = &pool.rings[0];
    let right = &pool.rings[1];
    if left.operand != Operand::Left
        || right.operand != Operand::Right
        || left.is_hole
        || right.is_hole
        || left.polygon != 0
        || right.polygon != 0
    {
        return None;
    }
    let a = left.points.as_ref();
    let b = right.points.as_ref();
    if a.len() < 3 || b.len() < 3 {
        return None;
    }
    if !shell_is_convex(a) || !shell_is_convex(b) {
        return None;
    }
    if !ring_envelopes_overlap(a, b) {
        return None;
    }
    Some((left, right))
}

fn ring_envelopes_overlap(a: &[XY], b: &[XY]) -> bool {
    ring_bounds(a).intersects(ring_bounds(b))
}

fn ring_bounds(ring: &[XY]) -> Bounds {
    Bounds::from_xy_iter(ring.iter().copied())
}

/// Upper and lower x-monotone edge-index chains for a CCW convex shell.
fn ring_chains(ring: &OrientedRing) -> [Vec<usize>; 2] {
    let points = ring.points.as_ref();
    let n = points.len();
    let leftmost = extremal_vertex(points, |left, right| {
        left.x
            .partial_cmp(&right.x)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.y
                    .partial_cmp(&right.y)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            == std::cmp::Ordering::Less
    });
    let rightmost = extremal_vertex(points, |left, right| {
        left.x
            .partial_cmp(&right.x)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.y
                    .partial_cmp(&right.y)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            == std::cmp::Ordering::Greater
    });
    if leftmost == rightmost {
        return [Vec::new(), Vec::new()];
    }
    let base = ring.segments.start;
    let upper = chain_edge_indices(n, leftmost, rightmost)
        .into_iter()
        .map(|edge| base + edge)
        .collect();
    let mut lower = chain_edge_indices(n, rightmost, leftmost)
        .into_iter()
        .map(|edge| base + edge)
        .collect::<Vec<_>>();
    lower.reverse();
    [upper, lower]
}

fn extremal_vertex(ring: &[XY], better: impl Fn(&XY, &XY) -> bool) -> usize {
    let mut index = 0_usize;
    for (candidate, point) in ring.iter().enumerate().skip(1) {
        if better(point, &ring[index]) {
            index = candidate;
        }
    }
    index
}

/// Directed edge indices along a CCW walk from `from` to `to` (exclusive of
/// `to`).
fn chain_edge_indices(n: usize, from: usize, to: usize) -> Vec<usize> {
    if from == to {
        return Vec::new();
    }
    let mut edges = Vec::new();
    let mut vertex = from;
    loop {
        edges.push(vertex);
        let next = wrap_index(vertex + 1, n);
        if next == to {
            break;
        }
        vertex = next;
    }
    edges
}

fn scan_chains(
    pool: &OperandPool,
    chain_a: &[usize],
    chain_b: &[usize],
    cuts: &mut [Vec<ConvexCut>],
) -> ScanFlow {
    let mut ia = 0_usize;
    let mut ib = 0_usize;
    let mut crossed = false;
    while ia < chain_a.len() && ib < chain_b.len() {
        let seg_a = pool.segments[chain_a[ia]];
        let seg_b = pool.segments[chain_b[ib]];
        let (a_minx, a_maxx) = segment_x_extent(seg_a);
        let (b_minx, b_maxx) = segment_x_extent(seg_b);
        if a_maxx < b_minx {
            ia += 1;
            continue;
        }
        if b_maxx < a_minx {
            ib += 1;
            continue;
        }
        if segment_envelopes_disjoint(seg_a, seg_b) {
            advance_by_xmax(a_maxx, b_maxx, &mut ia, &mut ib);
            continue;
        }
        if pool.operands[chain_a[ia]] == pool.operands[chain_b[ib]] {
            return ScanFlow::Bail;
        }
        match segment_contact_exact(seg_a, seg_b) {
            Contact::Disjoint => advance_by_xmax(a_maxx, b_maxx, &mut ia, &mut ib),
            Contact::Cross { point } => {
                push_cut(cuts, chain_a[ia], point, true);
                push_cut(cuts, chain_b[ib], point, true);
                crossed = true;
                advance_by_xmax(a_maxx, b_maxx, &mut ia, &mut ib);
            },
            Contact::Touch { .. } | Contact::Collinear { .. } => return ScanFlow::Bail,
        }
    }
    ScanFlow::Ok(crossed)
}

const fn segment_x_extent(segment: Segment) -> (f64, f64) {
    (
        segment.start.x.min(segment.end.x),
        segment.start.x.max(segment.end.x),
    )
}

fn advance_by_xmax(a_maxx: f64, b_maxx: f64, ia: &mut usize, ib: &mut usize) {
    if a_maxx < b_maxx {
        *ia += 1;
    } else if b_maxx < a_maxx {
        *ib += 1;
    } else {
        *ia += 1;
        *ib += 1;
    }
}

fn push_cut(cuts: &mut [Vec<ConvexCut>], segment: usize, point: XY, cross: bool) {
    cuts[segment].push(ConvexCut { point, cross });
}

#[cfg(test)]
mod tests {
    use super::super::clean_union::clean_overlay;
    use super::super::overlay::{OverlayOp, binary_areal_overlay, polygon_parts_to_shape};
    use super::super::segment_index::for_each_candidate_pair;
    use super::*;
    use crate::geometry::topology::build_operand_pool;
    use crate::{Point, Polygon, Ring};

    fn regular_ngon(cx: f64, cy: f64, radius: f64, n: usize) -> Polygon {
        let mut points: Vec<Point> = (0..n)
            .map(|i| {
                let angle = std::f64::consts::TAU * i as f64 / n as f64;
                Point::new_unchecked_xy(cx + radius * angle.cos(), cy + radius * angle.sin())
            })
            .collect();
        points.push(points[0]);
        Polygon::new(Ring::from_trusted_closed(points), Vec::new())
    }

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
        Polygon::new(
            Ring::from_trusted_closed(vec![
                Point::new_unchecked_xy(minx, miny),
                Point::new_unchecked_xy(maxx, miny),
                Point::new_unchecked_xy(maxx, maxy),
                Point::new_unchecked_xy(minx, maxy),
                Point::new_unchecked_xy(minx, miny),
            ]),
            Vec::new(),
        )
    }

    #[test]
    fn convex_fast_noding_matches_general_sweep() {
        // Circle protrudes past the box (true boundary crosses) but stays inset
        // enough that no discretized vertex lands on the box (touch would bail).
        let circle = regular_ngon(5.5, 5.5, 2.8, 48);
        let boxy = rectangle(3.0, 3.0, 9.0, 9.0);
        let pool = build_operand_pool(std::slice::from_ref(&circle), std::slice::from_ref(&boxy));
        let scan = try_collect(&pool).expect("convex circle/box should be eligible");
        assert!(scan.has_cross, "overlapping circle/box must cross");

        let mut sweep_cross = false;
        let mut clean = true;
        let visit = |left: usize, right: usize| -> std::ops::ControlFlow<()> {
            if pool.operands[left] == pool.operands[right] {
                return std::ops::ControlFlow::Continue(());
            }
            let (a_seg, b_seg) = (pool.segments[left], pool.segments[right]);
            if segment_envelopes_disjoint(a_seg, b_seg) {
                return std::ops::ControlFlow::Continue(());
            }
            match segment_contact_exact(a_seg, b_seg) {
                Contact::Cross { .. } => sweep_cross = true,
                Contact::Touch { .. } | Contact::Collinear { .. } => clean = false,
                Contact::Disjoint => {},
            }
            std::ops::ControlFlow::Continue(())
        };
        let _ = for_each_candidate_pair::<512>(&pool.segments, |index| pool.ring_of[index], visit);
        assert!(clean, "circle/box pair should stay transverse-clean");
        assert_eq!(scan.has_cross, sweep_cross);
    }

    #[test]
    fn convex_circle_box_overlay_ops_match_oracle() {
        let circle = regular_ngon(4.5, 4.5, 2.0, 32);
        let boxy = rectangle(3.0, 3.0, 7.0, 7.0);
        let a = [circle];
        let b = [boxy];
        for op in [
            OverlayOp::Union,
            OverlayOp::Intersection,
            OverlayOp::Difference,
        ] {
            let fast = clean_overlay(&a, &b, op).expect("convex transverse overlay");
            let exact = polygon_parts_to_shape(binary_areal_overlay(&a, &b, op));
            assert!(
                fast.equals(&exact),
                "convex circle/box {op:?} disagrees with oracle"
            );
        }
    }

    #[test]
    fn convex_pair_bails_on_shared_edge() {
        let a = rectangle(0.0, 0.0, 4.0, 4.0);
        let b = rectangle(4.0, 0.0, 8.0, 4.0);
        let pool = build_operand_pool(std::slice::from_ref(&a), std::slice::from_ref(&b));
        assert!(
            try_collect(&pool).is_none(),
            "touching shared edge must fall back to general sweep"
        );
    }
}
