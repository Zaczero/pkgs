use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::geometry::segments::crossing_point;
use crate::geometry::{
    Contact, Coordinates, HashMap, HashMapExt as _, HashSet, IndexedSegment, LineworkChains, Point,
    PointKey, RingClass, Segment, TouchDirections, ValidationIssue, XY, face_interior_point,
    orient_ring, ring_classify_point, ring_label, same_point, segment_contact_exact,
    segment_cross_point, segment_intersection_is_simple, segments_are_adjacent, segments_intersect,
    shared_segment_part,
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
    mut visit: impl FnMut(
        &IndexedSegment,
        &IndexedSegment,
    ) -> crate::error::Result<Option<ValidationIssue>>,
) -> crate::error::Result<Option<ValidationIssue>> {
    let mut issue = Ok(None);
    let _ = chains.for_each_candidate_pair(|left, right| {
        let (left, right) = (&chains.at(left), &chains.at(right));
        // Chain-adjacent segments share an endpoint by construction, so they
        // always intersect; skip orientation tests for this valid-ring case.
        if !(segments_are_adjacent(*left, *right)
            || segments_intersect(left.segment, right.segment))
        {
            return ControlFlow::Continue(());
        }
        match visit(left, right) {
            Ok(None) => ControlFlow::Continue(()),
            found => {
                issue = found;
                ControlFlow::Break(())
            },
        }
    })?;
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
) -> crate::error::Result<Option<ValidationIssue>> {
    classify_ring_pair_with_contacts(left, right, touches, &mut |_, _, _| Ok(()), describe)
}

pub(in crate::geometry) fn classify_ring_pair_with_contacts(
    left: &IndexedSegment,
    right: &IndexedSegment,
    touches: &mut TouchDirections,
    on_touch: &mut impl FnMut(&IndexedSegment, &IndexedSegment, XY) -> crate::error::Result<()>,
    describe: &impl Fn(usize, usize) -> String,
) -> crate::error::Result<Option<ValidationIssue>> {
    if left.line == right.line {
        if segment_intersection_is_simple(*left, *right) {
            return Ok(None);
        }
        return Ok(Some(ValidationIssue::new(
            "self_intersection",
            format!("{} has a self-intersection", ring_label(left.line)),
            pair_contact_point(left.segment, right.segment)?.map(XY::point),
            "$",
        )));
    }
    // A proper crossing is interior to both segments: the rings cross. Any
    // other contact is an endpoint of one on the other, a touch.
    match segment_contact_exact(left.segment, right.segment) {
        Contact::Cross => Ok(Some(ValidationIssue::new(
            "ring_intersection",
            describe(left.line, right.line),
            Some(crossing_point(left.segment, right.segment)?.point.point()),
            "$",
        ))),
        Contact::Touch { point } => {
            record_touch(touches, left, right, point.point());
            on_touch(left, right, point)?;
            Ok(None)
        },
        Contact::Collinear { span } if !same_point(span.start, span.end) => {
            Ok(Some(ValidationIssue::new(
                "shared_linework",
                format!("{} on a line", describe(left.line, right.line)),
                Some(span.start.point()),
                "$",
            )))
        },
        Contact::Collinear { span } => {
            on_touch(left, right, span.start)?;
            Ok(None)
        },
        Contact::Disjoint => Ok(None),
    }
}

/// First contact point of an intersecting pair, or `None` when the pair does
/// not actually meet.
pub(crate) fn pair_contact_point(
    left: Segment,
    right: Segment,
) -> crate::error::Result<Option<XY>> {
    if let Some(span) = shared_segment_part(left, right) {
        return Ok(Some(span.start));
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
    let [first_directions, second_directions] = entry;
    push_touch_directions(first_directions, first.segment, point.xy());
    push_touch_directions(second_directions, second.segment, point.xy());
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
        for (&key, [first_directions, second_directions]) in point_touches {
            let point = XY::new(f64::from_bits(key.x), f64::from_bits(key.y));
            if wedges_interleave(first_directions, second_directions, point) {
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
            let node = *touch_nodes.entry(key).or_insert_with(|| graph.push());
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
    let total = first.len().wrapping_add(second.len());
    let mut slots = [(0.0_f64, false); MAX_WEDGE_SLOTS];
    let mut heap;
    let directions = first
        .iter()
        .map(|direction| (direction, false))
        .chain(second.iter().map(|direction| (direction, true)))
        .map(|(direction, side)| {
            (
                pseudo_angle(direction.x - point.x, direction.y - point.y),
                side,
            )
        });
    let slots = if let Some(slots) = slots.get_mut(..total) {
        for (slot, direction) in slots.iter_mut().zip(directions) {
            *slot = direction;
        }
        slots
    } else {
        heap = Vec::with_capacity(total);
        heap.extend(directions);
        heap.as_mut_slice()
    };
    slots.sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    let changes = slots
        .iter()
        .zip(slots.iter().cycle().skip(1))
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

#[cfg(test)]
mod tests {
    use super::wedges_interleave;
    use crate::geometry::XY;

    #[test]
    fn ring_touch_directions_classify_small_and_large_fans() {
        let point = XY::new(0.0, 0.0);
        let left = [XY::new(-1.0, 1.0), XY::new(-1.0, -1.0)];
        for count in [2, 18] {
            let mut right: Vec<_> = (1..=count).map(|y| XY::new(1.0, f64::from(y))).collect();
            assert!(!wedges_interleave(&right, &left, point));
            assert!(!wedges_interleave(&left, &right, point));

            right.push(XY::new(-2.0, 0.0));
            assert!(wedges_interleave(&right, &left, point));
            assert!(wedges_interleave(&left, &right, point));
        }
    }
}
