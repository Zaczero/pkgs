use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::geometry::{
    Bounds, Coordinates, HashMap, HashMapExt as _, HashSet, IndexedSegment, LineworkChains, Point,
    PointKey, PointProbeUse, RingClass, Segment, Shape, ShapeData, TouchDirections,
    ValidationIssue, XY, convex_halfplanes_cover, face_interior_point, has_collection_operand,
    linework_contact, native_relate_data, native_relate_data_for, orient_ring, ring_classify_point,
    ring_label, same_point, segment_cross_point, segment_intersection_is_simple,
    segments_are_adjacent, segments_intersect, shared_segment_part, vertex_witness,
};

impl ShapeData {
    /// The relate-class predicates over CACHED bounds — the batch engine's
    /// per-pair lanes never re-scan coordinates for the box gates.
    pub fn contains_cached(&self, other: &Self) -> bool {
        self.contains_cached_for(
            other,
            PointProbeUse::OneShot(crate::geometry::vertex_witness_probe_count(other.shape())),
        )
    }

    pub fn covers_cached(&self, other: &Self) -> bool {
        self.covers_cached_for(
            other,
            PointProbeUse::OneShot(crate::geometry::vertex_witness_probe_count(other.shape())),
        )
    }

    pub(crate) fn contains_cached_for(&self, other: &Self, mode: PointProbeUse) -> bool {
        self.containment_cached::<false>(other, mode)
    }

    pub(crate) fn covers_cached_for(&self, other: &Self, mode: PointProbeUse) -> bool {
        self.containment_cached::<true>(other, mode)
    }

    fn containment_cached<const COVERS: bool>(&self, other: &Self, mode: PointProbeUse) -> bool {
        if has_collection_operand(self.shape(), other.shape()) {
            return if COVERS {
                native_relate_data_for(self, other, mode, PointProbeUse::OneShot(0)).is_covers()
            } else {
                native_relate_data_for(self, other, mode, PointProbeUse::OneShot(0)).is_contains()
            };
        }
        // Bounds containment is NECESSARY: a candidate poking out of the
        // container's box is never contained/covered. Refute the common
        // non-case for free BEFORE building any point-in-polygon index (the
        // `PointBatchTester` build dominated array contains over non-nesting pairs).
        match (self.bounds(), other.bounds()) {
            (Some(outer), Some(inner)) if !bounds_cover(outer, inner) => return false,
            (None, _) | (_, None) => return false,
            _ => {},
        }
        // Small convex hole-free containers (the index-refine hot case)
        // settle through pure halfplane signs. For covers, halfplane coverage
        // IS covers (both directions, any candidate dimension). For contains,
        // refutation is exact for any candidate; confirmation for areal ones
        // (positive area cannot hide inside the boundary curve); covered
        // NON-areal candidates keep the lane (boundary-contact subtleties).
        if let Shape::Polygon(container) = self.shape()
            && crate::geometry::uses_linear_plan_for_len(
                container.shell.coords().len().saturating_sub(1),
            )
            && let Some(ccw) = self.convex_shell()
            && let Shape::Polygon(container) = self.shape()
            && other.bounds().is_some()
        {
            let covered =
                convex_halfplanes_cover::<false, _>(container.shell.coords(), ccw, other.shape());
            if COVERS {
                return covered;
            }
            if !covered {
                return false;
            }
            if other.shape().has_area_parts() {
                return true;
            }
            return self.with_bounds_tail::<false, false>(other);
        }
        if let Some(tester) = self.point_tester_for(mode) {
            // Same uncovered-vertex refutation as the lane's witness, but
            // each probe rides the cached hierarchical tester.
            if vertex_witness(other.shape(), |point| !tester.covers_point(point)) {
                return false;
            }
            return self.with_bounds_tail::<COVERS, false>(other);
        }
        self.with_bounds_tail::<COVERS, true>(other)
    }

    fn with_bounds_tail<const COVERS: bool, const BUILD: bool>(&self, other: &Self) -> bool {
        let refute = || linework_contact(self.distance_parts(), other.distance_parts());
        if COVERS {
            self.shape().covers_with_bounds::<BUILD>(
                other.shape(),
                self.bounds(),
                other.bounds(),
                refute,
            )
        } else {
            self.shape().contains_with_bounds::<BUILD>(
                other.shape(),
                self.bounds(),
                other.bounds(),
                refute,
            )
        }
    }

    pub fn equals_cached(&self, other: &Self) -> bool {
        self.shape()
            .equals_with_bounds(other.shape(), self.bounds(), other.bounds())
    }

    pub fn touches_cached(&self, other: &Self) -> bool {
        if has_collection_operand(self.shape(), other.shape()) {
            return native_relate_data(self, other).is_touches();
        }
        self.shape()
            .touches_with_bounds(other.shape(), self.bounds(), other.bounds(), || {
                linework_contact(self.distance_parts(), other.distance_parts())
            })
    }

    pub fn crosses_cached(&self, other: &Self) -> bool {
        if has_collection_operand(self.shape(), other.shape()) {
            return native_relate_data(self, other).is_crosses_by_dimension();
        }
        self.shape()
            .crosses_with_bounds(other.shape(), self.bounds(), other.bounds(), || {
                linework_contact(self.distance_parts(), other.distance_parts())
            })
    }

    pub fn overlaps_cached(&self, other: &Self) -> bool {
        if has_collection_operand(self.shape(), other.shape()) {
            return native_relate_data(self, other).is_overlaps_by_dimension();
        }
        self.shape()
            .overlaps_with_bounds(other.shape(), self.bounds(), other.bounds(), || {
                linework_contact(self.distance_parts(), other.distance_parts())
            })
    }

    pub(crate) fn contains_properly_cached_for(&self, other: &Self, mode: PointProbeUse) -> bool {
        if has_collection_operand(self.shape(), other.shape()) {
            return native_relate_data_for(self, other, mode, PointProbeUse::OneShot(0))
                .is_contains_properly();
        }
        match (self.bounds(), other.bounds()) {
            (Some(outer), Some(inner)) if !bounds_cover(outer, inner) => return false,
            (None, _) | (_, None) => return false,
            _ => {},
        }
        if let Some(tester) = self.point_tester_for(mode)
            && vertex_witness(other.shape(), |point| !tester.covers_point(point))
        {
            return false;
        }
        self.shape().contains_properly_with_bounds(
            other.shape(),
            self.bounds(),
            other.bounds(),
            || linework_contact(self.distance_parts(), other.distance_parts()),
        )
    }
}

/// Whether `outer` covers `inner` (closed-box containment).
pub(crate) fn bounds_cover(outer: Bounds, inner: Bounds) -> bool {
    outer.minx() <= inner.minx()
        && outer.miny() <= inner.miny()
        && outer.maxx() >= inner.maxx()
        && outer.maxy() >= inner.maxy()
}

/// A point strictly interior to `ring`, preferring a cheap vertex that is
/// off `reference` (the classical hole-probe shortcut) and falling back to
/// the interior-point construction on a CCW copy.
pub(crate) fn ring_probe_point<C: Coordinates + ?Sized>(ring: &C, reference: &C) -> Point {
    if let Some(vertex) = ring
        .iter_coords()
        .find(|&vertex| ring_classify_point(reference, vertex) != RingClass::Boundary)
    {
        return vertex;
    }
    face_interior_point(
        &orient_ring(ring, false)
            .iter()
            .map(Point::xy)
            .collect::<Vec<_>>(),
    )
    .point()
}

/// Visit every intersecting segment pair exactly once (candidate sweep via
/// [`for_each_candidate_pair`]), stopping at the first issue.
pub(crate) fn visit_interacting_pairs(
    chains: &LineworkChains,
    mut visit: impl FnMut(&IndexedSegment, &IndexedSegment) -> Option<ValidationIssue>,
) -> Option<ValidationIssue> {
    let mut issue = None;
    let _ = chains.for_each_candidate_pair(|left, right| {
        let (left, right) = (&chains.at(left), &chains.at(right));
        // Chain-adjacent segments share an endpoint by construction, so
        // they always intersect — skip the orientation tests for the
        // dominant pair class of a valid ring.
        if (segments_are_adjacent(*left, *right) || segments_intersect(left.segment, right.segment))
            && let Some(found) = visit(left, right)
        {
            issue = Some(found);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    issue
}

/// Classify one intersecting pair: same-ring pairs use the simplicity
/// rule; cross-ring collinear overlaps and interior-interior crossings are
/// invalid; everything else is a touch, recorded with each side's local
/// directions for the wedge/connectivity post-pass.
pub(crate) fn classify_ring_pair(
    left: &IndexedSegment,
    right: &IndexedSegment,
    touches: &mut TouchDirections,
    describe: &impl Fn(usize, usize) -> String,
) -> Option<ValidationIssue> {
    if left.line == right.line {
        if segment_intersection_is_simple(*left, *right) {
            return None;
        }
        return Some(ValidationIssue::new(
            format!("{} has a self-intersection", ring_label(left.line)),
            pair_contact_point(left.segment, right.segment).map(XY::point),
            "$",
        ));
    }
    if let Some((_, part)) = shared_segment_part(left.segment, right.segment) {
        if part.len() >= 2 && !same_point(part[0], part[1]) {
            return Some(ValidationIssue::new(
                format!("{} on a line", describe(left.line, right.line)),
                Some(part[0].point()),
                "$",
            ));
        }
        record_touch(touches, left, right, part[0].point());
        return None;
    }
    let point = segment_cross_point(left.segment, right.segment)?;
    let left_interior =
        !same_point(point, left.segment.start) && !same_point(point, left.segment.end);
    let right_interior =
        !same_point(point, right.segment.start) && !same_point(point, right.segment.end);
    if left_interior && right_interior {
        return Some(ValidationIssue::new(
            describe(left.line, right.line),
            Some(point.point()),
            "$",
        ));
    }
    record_touch(touches, left, right, point.point());
    None
}

/// First contact point of an intersecting pair (witness extraction), or
/// `None` when the pair does not actually meet.
///
/// This used to answer `left.start` when `segment_cross_point` declined — a
/// point on neither segment, indistinguishable from a real witness.
pub(crate) fn pair_contact_point(left: Segment, right: Segment) -> Option<XY> {
    if let Some((_, part)) = shared_segment_part(left, right) {
        return Some(part[0]);
    }
    segment_cross_point(left, right)
}

/// Record the local directions both segments contribute at touch `point`.
pub(crate) fn record_touch(
    touches: &mut TouchDirections,
    left: &IndexedSegment,
    right: &IndexedSegment,
    point: Point,
) {
    let (first, second) = if left.line <= right.line {
        (left, right)
    } else {
        (right, left)
    };
    let entry = touches
        .entry((first.line, second.line))
        .or_default()
        .entry(PointKey::new(point))
        .or_default();
    push_touch_directions(&mut entry[0], first.segment, point.xy());
    push_touch_directions(&mut entry[1], second.segment, point.xy());
}

pub(crate) fn push_touch_directions(directions: &mut Vec<XY>, segment: Segment, point: XY) {
    for endpoint in [segment.start, segment.end] {
        if !same_point(endpoint, point)
            && !directions.iter().any(|known| same_point(*known, endpoint))
        {
            directions.push(endpoint);
        }
    }
}

/// Settle the collected touch nodes: a touch where the two rings' wedges
/// INTERLEAVE is a crossing through the shared point; the surviving plain
/// touches feed the rings-and-touch-points graph, where any cycle pinches
/// the interior apart (`connectivity`; GEOS's "interior is disconnected").
pub(crate) fn settle_touches(
    touches: &TouchDirections,
    ring_count: usize,
    connectivity: bool,
    describe: &impl Fn(usize, usize) -> String,
) -> Option<ValidationIssue> {
    let mut touch_nodes: HashMap<PointKey, usize> = HashMap::new();
    let mut graph = crate::collections::UnionFind::new(ring_count);
    let mut edges: HashSet<(usize, usize)> = HashSet::new();

    let mut ring_touches: Vec<_> = touches.iter().collect();
    ring_touches.sort_unstable_by_key(|(rings, _)| **rings);
    for (&(ring_a, ring_b), points) in ring_touches {
        let mut point_touches: Vec<_> = points.iter().collect();
        point_touches.sort_unstable_by_key(|(key, _)| **key);
        for (&key, directions) in point_touches {
            let point = XY::new(f64::from_bits(key.x), f64::from_bits(key.y));
            if wedges_interleave(&directions[0], &directions[1], point) {
                return Some(ValidationIssue::new(
                    describe(ring_a, ring_b),
                    Some(point.point()),
                    "$",
                ));
            }
            if !connectivity {
                continue;
            }
            // Touch points become graph nodes lazily, after the rings.
            let next_node = ring_count + touch_nodes.len();
            let node = *touch_nodes.entry(key).or_insert_with(|| {
                graph.push();
                next_node
            });
            for ring in [ring_a, ring_b] {
                if edges.insert((ring, node)) && graph.union(ring, node) {
                    return Some(ValidationIssue::new(
                        "interior is disconnected",
                        Some(point.point()),
                        "$",
                    ));
                }
            }
        }
    }
    None
}

/// Whether the two rings' local directions alternate around `point` — the
/// ring-through-vertex crossing no segment-pair test can see. Exactly two
/// circular label changes is a tangential touch; four is a transversal
/// crossing.
const MAX_WEDGE_SLOTS: usize = 16;

pub(crate) fn wedges_interleave(first: &[XY], second: &[XY], point: XY) -> bool {
    if first.len() < 2 || second.len() < 2 {
        return false;
    }
    let total = first.len() + second.len();
    let mut slots = [(0.0_f64, false); MAX_WEDGE_SLOTS];
    for (index, direction) in first.iter().enumerate() {
        slots[index] = (
            pseudo_angle(direction.x - point.x, direction.y - point.y),
            false,
        );
    }
    for (index, direction) in second.iter().enumerate() {
        slots[first.len() + index] = (
            pseudo_angle(direction.x - point.x, direction.y - point.y),
            true,
        );
    }
    if total > MAX_WEDGE_SLOTS {
        // Degenerate high-valence touch: fall back to heap (never observed in
        // validity scans, but keeps the lane total-correct).
        let mut heap: Vec<(f64, bool)> = Vec::with_capacity(total);
        for direction in first {
            heap.push((
                pseudo_angle(direction.x - point.x, direction.y - point.y),
                false,
            ));
        }
        for direction in second {
            heap.push((
                pseudo_angle(direction.x - point.x, direction.y - point.y),
                true,
            ));
        }
        heap.sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        let changes = heap
            .iter()
            .zip(heap.iter().cycle().skip(1))
            .take(heap.len())
            .filter(|(left, right)| left.1 != right.1)
            .count();
        return changes >= 4;
    }
    slots[..total]
        .sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    let changes = slots[..total]
        .iter()
        .zip(slots[..total].iter().cycle().skip(1))
        .take(total)
        .filter(|(left, right)| left.1 != right.1)
        .count();
    changes >= 4
}

/// Monotone angle substitute on `[0, 4)` (the "diamond angle"): cheap,
/// branch-light, and order-equivalent to `atan2` for sorting directions.
pub(in crate::geometry) fn pseudo_angle(dx: f64, dy: f64) -> f64 {
    // Scale first so extreme-but-finite components cannot overflow the
    // abs-sum denominator to inf (NaN angles reorder facets).
    let scale = dx.abs().max(dy.abs());
    if scale == 0.0 || !scale.is_finite() {
        return 0.0;
    }
    let (dx, dy) = (dx / scale, dy / scale);
    let denominator = dx.abs() + dy.abs();
    if denominator == 0.0 {
        return 0.0;
    }
    let core = dx / denominator;
    if dy >= 0.0 { 1.0 - core } else { 3.0 + core }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Polygon, Ring};

    fn indexed_polygon() -> ShapeData {
        let mut points: Vec<Point> = (0..64)
            .map(|index| {
                let angle = std::f64::consts::TAU * f64::from(index) / 64.0;
                Point::new_unchecked_xy(10.0 * angle.cos(), 10.0 * angle.sin())
            })
            .collect();
        points.push(points[0]);
        ShapeData::from(Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(points),
            Vec::new(),
        )))
    }

    #[test]
    fn contains_properly_bounds_rejection_does_not_build_tester() {
        let container = indexed_polygon();
        let candidate = ShapeData::from(Shape::LineString(LineSeq::from_trusted(CoordSeq::from(
            vec![
                Point::new_unchecked_xy(20.0, 0.0),
                Point::new_unchecked_xy(21.0, 0.0),
            ],
        ))));
        let cold = container.retained_heap_bytes();
        assert!(!container.contains_properly_cached_for(&candidate, PointProbeUse::AcrossCalls,));
        assert_eq!(container.retained_heap_bytes(), cold);
    }
}
