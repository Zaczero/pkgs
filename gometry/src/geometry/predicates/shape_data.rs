use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::geometry::segments::crossing_point;
use crate::geometry::{
    Contact, Coordinates, HashMap, HashMapExt as _, HashSet, IndexedSegment, LineworkChains, Point,
    PointKey, RingClass, Segment, TouchDirections, ValidationIssue, XY, compare_rays,
    face_interior_point, orient_ring, ring_classify_point, ring_label, same_point,
    segment_contact_exact, segment_cross_point, segment_intersection_is_simple,
    segments_are_adjacent, segments_intersect, shared_segment_part,
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
    let _candidate_pairs = chains.for_each_candidate_pair(|left, right| {
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
    classify_ring_pair_with_contacts(
        left,
        right,
        touches,
        &mut |_, _, _| Ok(([true, true], [true, true])),
        &mut |_, _, _| Ok(()),
        describe,
    )
}

pub(in crate::geometry) fn classify_ring_pair_with_contacts(
    left: &IndexedSegment,
    right: &IndexedSegment,
    touches: &mut TouchDirections,
    include_touch_directions: &mut impl FnMut(
        &IndexedSegment,
        &IndexedSegment,
        XY,
    ) -> crate::error::Result<([bool; 2], [bool; 2])>,
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
    let contact = segment_contact_exact(left.segment, right.segment);
    match contact {
        Contact::Cross => Ok(Some(ValidationIssue::new(
            "ring_intersection",
            describe(left.line, right.line),
            Some(crossing_point(left.segment, right.segment)?.point.point()),
            "$",
        ))),
        Contact::Touch(endpoint) => {
            let point = endpoint.point(left.segment, right.segment);
            let (include_left, include_right) = include_touch_directions(left, right, point)?;
            record_touch_filtered(
                touches,
                left,
                right,
                point.point(),
                include_left,
                include_right,
            );
            on_touch(left, right, point)?;
            Ok(None)
        },
        Contact::Collinear => {
            let span = contact
                .shared_span(left.segment, right.segment)
                .expect("collinear contact has a shared span");
            if same_point(span.start, span.end) {
                on_touch(left, right, span.start)?;
                Ok(None)
            } else {
                Ok(Some(ValidationIssue::new(
                    "shared_linework",
                    format!("{} on a line", describe(left.line, right.line)),
                    Some(span.start.point()),
                    "$",
                )))
            }
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
pub(crate) fn record_touch_filtered(
    touches: &mut TouchDirections,
    left: &IndexedSegment,
    right: &IndexedSegment,
    point: Point,
    include_left: [bool; 2],
    include_right: [bool; 2],
) {
    if !include_left.contains(&true) && !include_right.contains(&true) {
        return;
    }
    let (first, second) = if left.line <= right.line {
        (left, right)
    } else {
        (right, left)
    };
    let (include_first, include_second) = if left.line <= right.line {
        (include_left, include_right)
    } else {
        (include_right, include_left)
    };
    let entry = touches
        .entry((first.line, second.line))
        .or_default()
        .entry(PointKey::new(point))
        .or_default();
    let [first_directions, second_directions] = entry;
    push_touch_directions(first_directions, first.segment, point.xy(), include_first);
    push_touch_directions(
        second_directions,
        second.segment,
        point.xy(),
        include_second,
    );
}

pub(crate) fn push_touch_directions(
    directions: &mut Vec<XY>,
    segment: Segment,
    point: XY,
    include: [bool; 2],
) {
    for (endpoint, include) in [segment.start, segment.end].into_iter().zip(include) {
        if include
            && !same_point(endpoint, point)
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
    let first_count = first.len();
    let mut slots = [(XY::new(0.0, 0.0), 0_usize); MAX_WEDGE_SLOTS];
    let mut heap;
    let directions = first
        .iter()
        .chain(second.iter())
        .enumerate()
        .map(|(index, direction)| (*direction, index));
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
    slots.sort_unstable_by(|left, right| {
        compare_rays(point, left.0, right.0).then_with(|| {
            (left.1 >= first_count)
                .cmp(&(right.1 >= first_count))
                .then_with(|| left.1.cmp(&right.1))
        })
    });
    fan_alternations(slots.iter().map(|(_, index)| *index >= first_count))
}

pub(crate) fn fan_alternations(labels: impl IntoIterator<Item = bool>) -> bool {
    let mut labels = labels.into_iter();
    let Some(first) = labels.next() else {
        return false;
    };
    let mut previous = first;
    let mut len = 1_usize;
    let mut changes = 0_usize;
    for current in labels {
        len = len.saturating_add(1);
        changes = changes.saturating_add(usize::from(current != previous));
        previous = current;
    }
    if len < 4 {
        return false;
    }
    changes = changes.saturating_add(usize::from(previous != first));
    changes >= 4
}

#[cfg(test)]
mod tests {
    use super::{classify_ring_pair, wedges_interleave};
    use crate::geometry::{IndexedSegment, Segment, TouchDirections, XY};

    fn indexed_segment(line: usize, start: XY, end: XY) -> IndexedSegment {
        IndexedSegment {
            segment: Segment { start, end },
            line,
            index: 0,
            count: 1,
            closed: false,
        }
    }

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

    #[test]
    fn one_ulp_distinct_noninterleaving_touch_rays_remain_valid() {
        let point = XY::new(0.0, 0.0);
        let below_one = 1.0_f64.next_down();
        let first = [XY::new(1.0, 1.0), XY::new(-1.0, -1.0)];
        let second = [XY::new(1.0, -1.0), XY::new(1.0, below_one)];
        let reversed_first = [XY::new(-1.0, -1.0), XY::new(1.0, 1.0)];
        let reversed_second = [XY::new(1.0, below_one), XY::new(1.0, -1.0)];

        assert!(!wedges_interleave(&first, &second, point));
        assert!(!wedges_interleave(&second, &first, point));
        assert!(!wedges_interleave(&reversed_first, &reversed_second, point));
        assert!(!wedges_interleave(&reversed_second, &reversed_first, point));
    }

    #[test]
    fn finite_extreme_touch_rays_keep_their_exact_crossing_order() {
        let max = f64::MAX;
        let point = XY::new(-max, 0.0);
        let first = [XY::new(max, 1.0), XY::new(max, 3.0)];
        let second = [XY::new(max, 2.0), XY::new(max, 4.0)];

        assert!(wedges_interleave(&first, &second, point));
    }

    #[test]
    fn same_ray_positive_overlap_is_rejected_before_touch_settlement() {
        let mut touches = TouchDirections::default();
        let issue = classify_ring_pair(
            &indexed_segment(0, XY::new(0.0, 0.0), XY::new(2.0, 0.0)),
            &indexed_segment(1, XY::new(0.0, 0.0), XY::new(1.0, 0.0)),
            &mut touches,
            &|_, _| "rings intersect".to_owned(),
        )
        .unwrap();

        assert_eq!(
            issue.expect("the rings share a segment").code,
            "shared_linework"
        );
        assert!(touches.is_empty());
    }

    #[test]
    fn point_sized_collinear_contact_is_not_shared_linework() {
        let mut touches = TouchDirections::default();
        let issue = classify_ring_pair(
            &indexed_segment(0, XY::new(-1.0, 0.0), XY::new(0.0, 0.0)),
            &indexed_segment(1, XY::new(0.0, 0.0), XY::new(1.0, 0.0)),
            &mut touches,
            &|_, _| "rings intersect".to_owned(),
        )
        .unwrap();

        assert!(issue.is_none());
    }

    #[test]
    fn t_junction_rays_on_one_side_are_noninterleaving() {
        let point = XY::new(0.0, 0.0);
        let through = [XY::new(-1.0, 0.0), XY::new(1.0, 0.0)];
        let endpoint = [XY::new(-1.0, 1.0), XY::new(1.0, 1.0)];

        assert!(!wedges_interleave(&through, &endpoint, point));
        assert!(!wedges_interleave(&endpoint, &through, point));
    }
}
