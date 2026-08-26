use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::geometry::{
    Coordinates, HashMap, HashMapExt as _, HashSet, IndexedSegment, LineworkChains, Point,
    PointKey, RingClass, Segment, TouchDirections, ValidationIssue, XY, face_interior_point,
    orient_ring, ring_classify_point, ring_label, same_point, segment_cross_point,
    segment_intersection_is_simple, segments_are_adjacent, segments_intersect, shared_segment_part,
};

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
        // Chain-adjacent segments share an endpoint by construction, so they
        // always intersect; skip orientation tests for this valid-ring case.
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

/// Classify one intersecting pair: same-ring pairs use the simplicity rule;
/// cross-ring collinear overlaps and interior-interior crossings are invalid;
/// everything else is a touch, recorded with each side's local directions.
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
            "self_intersection",
            format!("{} has a self-intersection", ring_label(left.line)),
            pair_contact_point(left.segment, right.segment).map(XY::point),
            "$",
        ));
    }
    if let Some((_, part)) = shared_segment_part(left.segment, right.segment) {
        if part.len() >= 2 && !same_point(part[0], part[1]) {
            return Some(ValidationIssue::new(
                "shared_linework",
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
            "ring_intersection",
            describe(left.line, right.line),
            Some(point.point()),
            "$",
        ));
    }
    record_touch(touches, left, right, point.point());
    None
}

/// First contact point of an intersecting pair, or `None` when the pair does
/// not actually meet.
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

/// Settle collected touch nodes. A wedge crossing is an invalid ring
/// crossing; the remaining touches feed the ring connectivity post-pass.
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
                    "ring_intersection",
                    describe(ring_a, ring_b),
                    Some(point.point()),
                    "$",
                ));
            }
            if !connectivity {
                continue;
            }
            let next_node = ring_count + touch_nodes.len();
            let node = *touch_nodes.entry(key).or_insert_with(|| {
                graph.push();
                next_node
            });
            for ring in [ring_a, ring_b] {
                if edges.insert((ring, node)) && graph.union(ring, node) {
                    return Some(ValidationIssue::new(
                        "interior_disconnected",
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

pub(in crate::geometry) fn pseudo_angle(dx: f64, dy: f64) -> f64 {
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
