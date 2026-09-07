//! O(n + m) boundary-contact discovery for two single-ring convex shells.
//!
//! When [`try_collect`]'s gates pass, cross-operand crossings are found by a
//! two-pointer walk over x-monotone upper/lower chains instead of the
//! arrangement-style [`for_each_candidate_pair`] sweep. Touch, collinear
//! overlap, or any gate failure returns `None` so the caller falls back to the
//! general noding path.

use crate::geometry::predicates::{
    RingClass, ring_classify_point, shell_is_convex, shell_is_strictly_convex,
};
use crate::geometry::segments::{Contact, segment_contact_exact, segment_envelopes_disjoint};
use crate::geometry::topology::{Operand, OperandPool, OrientedRing, build_operand_pool};
use crate::geometry::{Bounds, Polygon, Segment, XY, wrap_index};

/// Complete relation classes that can be derived for two strict convex
/// shells without constructing an arrangement. Any boundary touch, collinear
/// overlap, or uncertain admission returns `None` instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConvexRelation {
    Disjoint,
    ProperCrossing,
    LeftContainsRight,
    RightContainsLeft,
}

/// Classify two eligible convex shells using exact boundary contacts and
/// point membership. The existing convex contact walk owns segment pairing;
/// this function only turns its clean result into a relation class.
pub(super) fn try_classify(left: &Polygon, right: &Polygon) -> Option<ConvexRelation> {
    if !left.holes.is_empty()
        || !right.holes.is_empty()
        || !shell_is_strictly_convex(left.shell.coords())
        || !shell_is_strictly_convex(right.shell.coords())
    {
        return None;
    }
    let left_bounds = Bounds::from_coords(left.shell.coords())?;
    let right_bounds = Bounds::from_coords(right.shell.coords())?;
    if !left_bounds.intersects(right_bounds) {
        return Some(ConvexRelation::Disjoint);
    }

    let pool = build_operand_pool(std::slice::from_ref(left), std::slice::from_ref(right));
    let (left_ring, right_ring) = staged_shell_pair(&pool)?;
    if scan_rings(&pool, left_ring, right_ring, |_, _| {})? {
        return Some(ConvexRelation::ProperCrossing);
    }

    let left_class = ring_classify_point(left.shell.coords(), right.shell.first()?);
    let right_class = ring_classify_point(right.shell.coords(), left.shell.first()?);
    match (left_class, right_class) {
        (RingClass::Interior, RingClass::Exterior) => Some(ConvexRelation::LeftContainsRight),
        (RingClass::Exterior, RingClass::Interior) => Some(ConvexRelation::RightContainsLeft),
        (RingClass::Exterior, RingClass::Exterior) => Some(ConvexRelation::Disjoint),
        // A boundary result or both interiors contradicts the clean contact
        // certificate, so preserve the generic relation semantics.
        _ => None,
    }
}

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
    let has_cross = scan_rings(pool, left, right, |segment, point| {
        push_cut(&mut cuts, segment, point, true);
    })?;
    Some(ConvexBoundaryScan { cuts, has_cross })
}

fn scan_rings(
    pool: &OperandPool,
    left: &OrientedRing,
    right: &OrientedRing,
    mut on_cross: impl FnMut(usize, XY),
) -> Option<bool> {
    let mut has_cross = false;

    let left_edges = ring_chains(left);
    let right_edges = ring_chains(right);

    for chain_a in left_edges {
        for chain_b in right_edges {
            match scan_chains(pool, chain_a, chain_b, &mut on_cross) {
                ScanFlow::Ok(crossed) => has_cross |= crossed,
                ScanFlow::Bail => return None,
            }
        }
    }

    Some(has_cross)
}

enum ScanFlow {
    Ok(bool),
    Bail,
}

/// Total staged shell vertices above which the general segment sweep wins, so
/// this path declines the pair.
///
/// Both are linear in the crossing count, but this path walks two monotone
/// chains, a larger constant than the sweep's banded `n log n`. The advantage
/// therefore narrows with size and reverses. With materialized index chains,
/// measured on two overlapping convex rings (union, one host): 4.3x faster at
/// 16 total vertices, 1.7x at 256, 1.2x at 600, level near this bound, and
/// 1.2x slower at 2400.
const CONVEX_CONTACT_MAX_VERTICES: usize = 1000;

/// Two staged operand shells that the convex walk can address: one left and
/// one right, neither a hole, first polygon each side, under the vertex cap.
/// Overlay still has to prove envelope overlap and non-strict convexity;
/// classify already proved strict convexity and bounds on the source CoordSeq.
fn staged_shell_pair(pool: &OperandPool) -> Option<(&OrientedRing, &OrientedRing)> {
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
    let a = left.points.len();
    let b = right.points.len();
    if a < 3 || b < 3 || a + b > CONVEX_CONTACT_MAX_VERTICES {
        return None;
    }
    Some((left, right))
}

fn eligible_rings(pool: &OperandPool) -> Option<(&OrientedRing, &OrientedRing)> {
    let (left, right) = staged_shell_pair(pool)?;
    let a = left.points.as_ref();
    let b = right.points.as_ref();
    // Envelope before convexity: both are linear, but convexity spends an
    // exact orientation predicate per vertex, so a disjoint pair should not
    // pay for it.
    if !Bounds::from_xy_iter(a.iter().copied()).intersects(Bounds::from_xy_iter(b.iter().copied()))
    {
        return None;
    }
    if !shell_is_convex(a) || !shell_is_convex(b) {
        return None;
    }
    Some((left, right))
}

/// Visit-order range over CCW pool edges of one x-monotone chain.
///
/// `reverse` only reorders visit indices; each slot is still the CCW edge
/// `points[v] → points[wrap_index(v + 1, n)]` at `base + v`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ChainRange {
    base: usize,
    n: usize,
    origin: usize,
    len: usize,
    reverse: bool,
}

impl ChainRange {
    fn get(self, i: usize) -> Option<usize> {
        (i < self.len).then(|| {
            let j = if self.reverse { self.len - 1 - i } else { i };
            self.base + wrap_index(self.origin + j, self.n)
        })
    }
}

const fn ccw_len(from: usize, to: usize, n: usize) -> usize {
    if to >= from { to - from } else { n - from + to }
}

/// Upper and lower x-monotone edge-index chains for a CCW convex shell.
fn ring_chains(ring: &OrientedRing) -> [ChainRange; 2] {
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
    let base = ring.segments.start;
    [
        ChainRange {
            base,
            n,
            origin: leftmost,
            len: ccw_len(leftmost, rightmost, n),
            reverse: false,
        },
        ChainRange {
            base,
            n,
            origin: rightmost,
            len: ccw_len(rightmost, leftmost, n),
            reverse: true,
        },
    ]
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

fn scan_chains(
    pool: &OperandPool,
    chain_a: ChainRange,
    chain_b: ChainRange,
    on_cross: &mut impl FnMut(usize, XY),
) -> ScanFlow {
    let mut ia = 0_usize;
    let mut ib = 0_usize;
    let mut crossed = false;
    while let (Some(index_a), Some(index_b)) = (chain_a.get(ia), chain_b.get(ib)) {
        let seg_a = pool.segments[index_a];
        let seg_b = pool.segments[index_b];
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
        if !segment_envelopes_disjoint(seg_a, seg_b) {
            match segment_contact_exact(seg_a, seg_b) {
                Contact::Disjoint => {},
                Contact::Cross { point } => {
                    on_cross(index_a, point);
                    on_cross(index_b, point);
                    crossed = true;
                },
                Contact::Touch { .. } | Contact::Collinear { .. } => return ScanFlow::Bail,
            }
        }
        if a_maxx < b_maxx {
            ia += 1;
        } else if b_maxx < a_maxx {
            ib += 1;
        } else if skipped_pair_meets(pool, chain_a, ia + 1, chain_b, ib)
            || skipped_pair_meets(pool, chain_a, ia, chain_b, ib + 1)
        {
            return ScanFlow::Bail;
        } else {
            ia += 1;
            ib += 1;
        }
    }
    ScanFlow::Ok(crossed)
}

/// Whether either pair skipped by a coincident right-extreme advance actually
/// meets.
///
/// Both chains end their current segment at the same x, so advancing one
/// pointer would leave the other chain's successor untested and advancing both
/// leaves two pairs untested. Those pairs share only that single x, which for
/// two non-vertical segments is the shared endpoint the current pair already
/// reported — but a vertical edge occupies a whole y-range there, so the
/// contact is real and unreported. A single x admits no transversal crossing,
/// only a touch or a collinear overlap, and both of those end the walk anyway.
fn skipped_pair_meets(
    pool: &OperandPool,
    chain_a: ChainRange,
    ia: usize,
    chain_b: ChainRange,
    ib: usize,
) -> bool {
    let (Some(left), Some(right)) = (chain_a.get(ia), chain_b.get(ib)) else {
        return false;
    };
    let (seg_a, seg_b) = (pool.segments[left], pool.segments[right]);
    !segment_envelopes_disjoint(seg_a, seg_b)
        && !matches!(segment_contact_exact(seg_a, seg_b), Contact::Disjoint)
}

const fn segment_x_extent(segment: Segment) -> (f64, f64) {
    (
        segment.start.x.min(segment.end.x),
        segment.start.x.max(segment.end.x),
    )
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
                Point::new_raw_xy(cx + radius * angle.cos(), cy + radius * angle.sin())
            })
            .collect();
        points.push(points[0]);
        Polygon::new(Ring::from_closed(points), Vec::new())
    }

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
        Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(minx, miny),
                Point::new_raw_xy(maxx, miny),
                Point::new_raw_xy(maxx, maxy),
                Point::new_raw_xy(minx, maxy),
                Point::new_raw_xy(minx, miny),
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
        let _ = for_each_candidate_pair(&pool.segments, |index| pool.ring_of[index], visit);
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
        assert!(
            try_classify(&a, &b).is_none(),
            "shared-edge classify must fall back to the arrangement"
        );
    }

    fn collinear_bottom(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
        Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(minx, miny),
                Point::new_raw_xy(f64::midpoint(minx, maxx), miny),
                Point::new_raw_xy(maxx, miny),
                Point::new_raw_xy(maxx, maxy),
                Point::new_raw_xy(minx, maxy),
                Point::new_raw_xy(minx, miny),
            ]),
            Vec::new(),
        )
    }

    #[test]
    fn holes_decline_classify_and_overlay_fast_paths() {
        let shell = rectangle(0.0, 0.0, 4.0, 4.0);
        let hole = Ring::from_closed(vec![
            Point::new_raw_xy(1.0, 1.0),
            Point::new_raw_xy(2.0, 1.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(1.0, 1.0),
        ]);
        let holed = Polygon::new(shell.shell.clone(), vec![hole]);
        let other = rectangle(3.0, 3.0, 6.0, 6.0);
        assert!(
            try_classify(&holed, &other).is_none(),
            "a hole forbids the relate shortcut"
        );
        let pool = build_operand_pool(std::slice::from_ref(&holed), std::slice::from_ref(&other));
        assert!(
            try_collect(&pool).is_none(),
            "a hole forbids the overlay chain walk"
        );
    }

    #[test]
    fn collinear_shell_is_overlay_only() {
        let left = collinear_bottom(0.0, 0.0, 4.0, 4.0);
        let right = rectangle(2.0, 2.0, 6.0, 6.0);
        assert!(
            try_classify(&left, &right).is_none(),
            "collinear turns are not a strict convex relate class"
        );
        let pool = build_operand_pool(std::slice::from_ref(&left), std::slice::from_ref(&right));
        let scan = try_collect(&pool).expect("non-strict convex overlay may still walk");
        assert!(
            scan.has_cross,
            "overlapping collinear-bottom rectangles cross"
        );
    }

    /// Old `chain_edge_indices` walk: CCW `from` to `to` exclusive, then
    /// optional reverse. Independent of [`ChainRange::get`].
    fn iterative_ccw_then_maybe_reverse(
        n: usize,
        from: usize,
        to: usize,
        reverse: bool,
        base: usize,
    ) -> Vec<usize> {
        if from == to {
            return Vec::new();
        }
        let mut edges = Vec::new();
        let mut vertex = from;
        loop {
            edges.push(base + vertex);
            let next = wrap_index(vertex + 1, n);
            if next == to {
                break;
            }
            vertex = next;
        }
        if reverse {
            edges.reverse();
        }
        edges
    }

    fn range_sequence(range: ChainRange) -> Vec<usize> {
        (0..range.len)
            .map(|i| range.get(i).expect("i is inside len"))
            .collect()
    }

    fn assert_range_matches_oracle(n: usize, from: usize, to: usize, reverse: bool, base: usize) {
        let range = ChainRange {
            base,
            n,
            origin: from,
            len: ccw_len(from, to, n),
            reverse,
        };
        let oracle = iterative_ccw_then_maybe_reverse(n, from, to, reverse, base);
        assert_eq!(
            range.len,
            oracle.len(),
            "n={n} from={from} to={to} reverse={reverse} base={base}"
        );
        assert_eq!(
            range_sequence(range),
            oracle,
            "n={n} from={from} to={to} reverse={reverse} base={base}"
        );
        assert!(range.get(range.len).is_none());
        assert!(range.get(range.len.saturating_add(1)).is_none());
    }

    #[test]
    fn chain_range_matches_iterative_ccw_oracle() {
        for reverse in [false, true] {
            assert_range_matches_oracle(1, 0, 0, reverse, 0);
            assert_range_matches_oracle(4, 0, 0, reverse, 0);
            assert_range_matches_oracle(4, 0, 2, reverse, 0);
            assert_range_matches_oracle(4, 2, 0, reverse, 0);
            assert_range_matches_oracle(4, 3, 1, reverse, 7);
            assert_range_matches_oracle(5, 0, 1, reverse, 0);
            assert_range_matches_oracle(5, 1, 0, reverse, 0);
            assert_range_matches_oracle(3, 2, 1, reverse, 4);
            for from in 0..6 {
                for to in 0..6 {
                    assert_range_matches_oracle(6, from, to, reverse, 11);
                }
            }
        }
    }

    #[test]
    fn ring_chains_match_oracle_and_allocate_nothing() {
        let left = rectangle(0.0, 0.0, 4.0, 4.0);
        let right = rectangle(8.0, 0.0, 9.0, 1.0);
        let pool = build_operand_pool(std::slice::from_ref(&left), std::slice::from_ref(&right));
        assert!(pool.rings[1].segments.start > 0);
        for ring in &pool.rings {
            let n = ring.points.len();
            let points = ring.points.as_ref();
            let leftmost = extremal_vertex(points, |a, b| {
                a.x.partial_cmp(&b.x)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.y.partial_cmp(&b.y).unwrap_or(std::cmp::Ordering::Equal))
                    == std::cmp::Ordering::Less
            });
            let rightmost = extremal_vertex(points, |a, b| {
                a.x.partial_cmp(&b.x)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.y.partial_cmp(&b.y).unwrap_or(std::cmp::Ordering::Equal))
                    == std::cmp::Ordering::Greater
            });
            let [upper, lower] = ring_chains(ring);
            assert_eq!(
                range_sequence(upper),
                iterative_ccw_then_maybe_reverse(
                    n,
                    leftmost,
                    rightmost,
                    false,
                    ring.segments.start
                )
            );
            assert_eq!(
                range_sequence(lower),
                iterative_ccw_then_maybe_reverse(n, rightmost, leftmost, true, ring.segments.start)
            );
            if leftmost != rightmost {
                assert_eq!(upper.len + lower.len, n);
            }
        }
        let triangle = Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(3.0, 0.0),
                Point::new_raw_xy(1.0, 2.0),
                Point::new_raw_xy(0.0, 0.0),
            ]),
            Vec::new(),
        );
        let tri_pool = build_operand_pool(
            std::slice::from_ref(&triangle),
            std::slice::from_ref(&right),
        );
        let [upper, lower] = ring_chains(&tri_pool.rings[0]);
        assert_ne!(upper.len, lower.len, "triangle chains are asymmetric");
        let (_, allocations) = crate::test_support::allocation_count(|| {
            ring_chains(&pool.rings[0]);
            ring_chains(&pool.rings[1]);
            ring_chains(&tri_pool.rings[0]);
        });
        assert_eq!(allocations, 0, "ChainRange construction must not allocate");
    }
}
