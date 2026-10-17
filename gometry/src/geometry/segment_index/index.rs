use ahash::HashSetExt as _;
use rstar::{AABB, PointDistance, RTreeNode, RTreeObject};

use crate::HeapSize;
use crate::collections::HashSet;
#[cfg(test)]
use crate::geometry::point_on_segment;
use crate::geometry::{
    BulkRTree, Point, PointKey, Segment, SquaredDistanceKey, clearance_aabb_filter,
    point_pair_distance_key, same_point,
};

/// One segment as an R-tree leaf, carrying its position in the build slice
/// (so pair-once sweeps can keep `ordinal > i` candidates).
pub(crate) struct SegmentEntry {
    pub segment: Segment,
    pub ordinal: usize,
    envelope: AABB<[f64; 2]>,
}

impl SegmentEntry {
    pub(super) fn new(segment: Segment, ordinal: usize) -> Self {
        Self {
            segment,
            ordinal,
            envelope: segment_envelope(segment),
        }
    }
}

impl RTreeObject for SegmentEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

pub(crate) fn segment_envelope(segment: Segment) -> AABB<[f64; 2]> {
    // Corners are ordered here — skip from_corners' second min/max pass.
    AABB::from_bounds(
        [
            segment.start.x.min(segment.end.x),
            segment.start.y.min(segment.end.y),
        ],
        [
            segment.start.x.max(segment.end.x),
            segment.start.y.max(segment.end.y),
        ],
    )
}

/// An R-tree over a segment set.
pub(crate) struct SegmentIndex {
    tree: BulkRTree<SegmentEntry>,
}

impl HeapSize for SegmentIndex {
    fn heap_bytes(&self) -> usize {
        super::bulk_rtree_heap_bytes(&self.tree)
    }
}

impl SegmentIndex {
    pub(crate) fn build(segments: &[Segment]) -> Self {
        Self::build_from_iter(segments.iter().copied())
    }

    /// Build straight from a segment iterator — entries are gathered once for
    /// the bulk load, with no caller-side `Vec<Segment>` staging.
    pub(crate) fn build_from_iter(segments: impl IntoIterator<Item = Segment>) -> Self {
        let entries = segments
            .into_iter()
            .enumerate()
            .map(|(ordinal, segment)| SegmentEntry::new(segment, ordinal))
            .collect();
        Self {
            tree: BulkRTree::bulk_load_with_params(entries),
        }
    }

    /// Indexed entries whose envelope intersects `query`'s — the noding /
    /// pair-sweep candidate set (each entry carries its build ordinal).
    pub(crate) fn intersecting_candidates(
        &self,
        query: Segment,
    ) -> impl Iterator<Item = &SegmentEntry> + '_ {
        self.tree
            .locate_in_envelope_intersecting(segment_envelope(query))
    }

    /// Whether a point lies on any indexed segment. The envelope query keeps
    /// the exact segment predicate on the small candidate set, so repeated
    /// point probes do not rescan the full linework.
    #[cfg(test)]
    pub(crate) fn covers_point(&self, point: Point) -> bool {
        let query = Segment {
            start: point.xy(),
            end: point.xy(),
        };
        self.intersecting_candidates(query)
            .any(|entry| point_on_segment(point, entry.segment.start, entry.segment.end))
    }
}

/// An R-tree over source point entries used by nearest-point and clearance queries.
pub(crate) struct PointSetIndex {
    tree: BulkRTree<PointEntry>,
}

struct ClearancePointNode<'a> {
    node: &'a RTreeNode<PointEntry>,
    lower_bound: SquaredDistanceKey,
}

pub(crate) struct ClearancePointQuery<'a> {
    tree: &'a BulkRTree<PointEntry>,
    stack: Vec<ClearancePointNode<'a>>,
}

#[cfg(test)]
std::thread_local! {
    pub(crate) static CLEARANCE_POINT_BUILD_SITES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_POINT_NODES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_POINT_LEAVES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_POINT_RECHECKS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// A point-index leaf retains its position in the full-ordinate source.
pub(crate) struct PointEntry {
    coords: [f64; 2],
    ordinal: usize,
}

impl RTreeObject for PointEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.coords)
    }
}

impl PointDistance for PointEntry {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        self.coords.distance_2(point)
    }
}

fn consider_clearance_neighbor(
    best: &mut Option<(usize, SquaredDistanceKey)>,
    point: Point,
    entry: &PointEntry,
) -> crate::error::Result<()> {
    let target = Point::new_raw_xy(entry.coords[0], entry.coords[1]);
    if same_point(point, target) {
        return Ok(());
    }
    let key = point_pair_distance_key(point.xy(), target.xy());
    let replace = match best.as_ref() {
        None => true,
        Some((ordinal, incumbent)) => {
            let order = key.cmp_distance(incumbent)?;
            order.is_lt() || order.is_eq() && entry.ordinal < *ordinal
        },
    };
    if replace {
        *best = Some((entry.ordinal, key));
    }
    Ok(())
}

fn clearance_node_envelope(node: &RTreeNode<PointEntry>) -> AABB<[f64; 2]> {
    match node {
        RTreeNode::Leaf(entry) => entry.envelope(),
        RTreeNode::Parent(parent) => parent.envelope(),
    }
}

fn nearer_clearance_limit<'a>(
    local: Option<&'a SquaredDistanceKey>,
    global: Option<&'a SquaredDistanceKey>,
) -> crate::error::Result<Option<&'a SquaredDistanceKey>> {
    match (local, global) {
        (None, None) => Ok(None),
        (Some(local), None) => Ok(Some(local)),
        (None, Some(global)) => Ok(Some(global)),
        (Some(local), Some(global)) => {
            if local.cmp_distance(global)?.is_lt() {
                Ok(Some(local))
            } else {
                Ok(Some(global))
            }
        },
    }
}

fn push_clearance_children<'a>(
    point: Point,
    children: &'a [RTreeNode<PointEntry>],
    incumbent: Option<&SquaredDistanceKey>,
    local: Option<&SquaredDistanceKey>,
    stack: &mut Vec<ClearancePointNode<'a>>,
) -> crate::error::Result<()> {
    let limit = nearer_clearance_limit(local, incumbent)?;
    let first_child = stack.len();
    for node in children {
        #[cfg(test)]
        match node {
            RTreeNode::Parent(_) => {
                CLEARANCE_POINT_NODES.with(|work| work.set(work.get().saturating_add(1)));
            },
            RTreeNode::Leaf(_) => {
                CLEARANCE_POINT_LEAVES.with(|work| work.set(work.get().saturating_add(1)));
            },
        }
        let envelope = clearance_node_envelope(node);
        let [lower_x, lower_y] = envelope.lower();
        let [upper_x, upper_y] = envelope.upper();
        let (witness, filter) =
            clearance_aabb_filter(point.xy(), [lower_x, lower_y, upper_x, upper_y]);
        if let Some(limit) = limit
            && filter.cmp_witness_key(witness, limit)?.is_gt()
        {
            continue;
        }
        stack.push(ClearancePointNode {
            node,
            lower_bound: filter.bind_witness(witness),
        });
    }

    let mut index = first_child.wrapping_add(1);
    while index < stack.len() {
        let mut current = index;
        while current > first_child {
            let current_bound = &stack
                .get(current)
                .expect("inserted clearance child remains in the stack")
                .lower_bound;
            let previous_bound = &stack
                .get(current.wrapping_sub(1))
                .expect("previous clearance child remains in the stack")
                .lower_bound;
            if !current_bound.cmp_distance(previous_bound)?.is_gt() {
                break;
            }
            stack.swap(current, current.wrapping_sub(1));
            current = current.wrapping_sub(1);
        }
        index = index.wrapping_add(1);
    }
    Ok(())
}

impl PointSetIndex {
    /// Build the clearance tree with the first source ordinal for each XY.
    pub(crate) fn build_clearance_with_ordinals(points: &[Point]) -> Self {
        let mut seen = HashSet::with_capacity(points.len());
        let entries: Vec<_> = points
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(ordinal, point)| {
                #[cfg(test)]
                CLEARANCE_POINT_BUILD_SITES.with(|work| {
                    work.set(work.get().saturating_add(1));
                });
                let unique =
                    point.x.is_nan() || point.y.is_nan() || seen.insert(PointKey::new(point));
                unique.then_some(PointEntry {
                    coords: [point.x, point.y],
                    ordinal,
                })
            })
            .collect();
        drop(seen);
        Self {
            tree: BulkRTree::bulk_load_with_params(entries),
        }
    }

    pub(crate) fn build_with_ordinals(points: impl IntoIterator<Item = Point>) -> Self {
        let entries = points
            .into_iter()
            .enumerate()
            .map(|(ordinal, point)| PointEntry {
                coords: [point.x, point.y],
                ordinal,
            })
            .collect();
        Self {
            tree: BulkRTree::bulk_load_with_params(entries),
        }
    }

    pub(crate) const fn clearance_query(&self) -> ClearancePointQuery<'_> {
        ClearancePointQuery {
            tree: &self.tree,
            stack: Vec::new(),
        }
    }

    pub(crate) fn nearest_ordinal(&self, point: Point) -> crate::error::Result<Option<usize>> {
        let mut best = None;
        let mut limit = f64::INFINITY;
        for (entry, distance) in self
            .tree
            .nearest_neighbor_iter_with_distance_2([point.x, point.y])
        {
            if distance.is_normal() && distance > limit {
                break;
            }
            let target = crate::geometry::XY::new(entry.coords[0], entry.coords[1]);
            let key = point_pair_distance_key(point.xy(), target);
            let replace = match best.as_ref() {
                None => true,
                Some((ordinal, incumbent)) => {
                    let order = key.cmp(incumbent)?;
                    order.is_lt() || order.is_eq() && entry.ordinal < *ordinal
                },
            };
            if replace {
                limit = key.upper_bound() * (1.0 + 4.0 * f64::EPSILON);
                best = Some((entry.ordinal, key));
            }
        }
        Ok(best.map(|(ordinal, _)| ordinal))
    }
}

impl ClearancePointQuery<'_> {
    /// Nearest distinct XY source that can improve `incumbent`, ordered by
    /// exact source distance and then by source ordinal. Node envelopes use the
    /// exact source-coordinate distance to their clamped closest point.
    pub(crate) fn nearest_clearance_neighbor(
        &mut self,
        point: Point,
        incumbent: Option<&SquaredDistanceKey>,
    ) -> crate::error::Result<Option<(usize, SquaredDistanceKey)>> {
        self.stack.clear();
        let tree = self.tree;
        push_clearance_children(
            point,
            tree.root().children(),
            incumbent,
            None,
            &mut self.stack,
        )?;
        let mut best: Option<(usize, SquaredDistanceKey)> = None;
        while let Some(node) = self.stack.pop() {
            let local = best.as_ref().map(|(_, key)| key);
            if let Some(limit) = nearer_clearance_limit(local, incumbent)? {
                #[cfg(test)]
                CLEARANCE_POINT_RECHECKS.with(|work| work.set(work.get().saturating_add(1)));
                if node.lower_bound.cmp_distance(limit)?.is_gt() {
                    continue;
                }
            }
            match node.node {
                RTreeNode::Leaf(entry) => {
                    consider_clearance_neighbor(&mut best, point, entry)?;
                },
                RTreeNode::Parent(parent) => {
                    let local = best.as_ref().map(|(_, key)| key);
                    push_clearance_children(
                        point,
                        parent.children(),
                        incumbent,
                        local,
                        &mut self.stack,
                    )?;
                },
            }
        }
        let Some((ordinal, key)) = best else {
            return Ok(None);
        };
        if let Some(incumbent) = incumbent
            && !key.cmp_distance(incumbent)?.is_lt()
        {
            return Ok(None);
        }
        Ok(Some((ordinal, key)))
    }
}

impl HeapSize for PointSetIndex {
    fn heap_bytes(&self) -> usize {
        super::bulk_rtree_heap_bytes(&self.tree)
    }
}

#[cfg(test)]
mod query_tests {
    use super::*;

    #[test]
    fn segment_index_covers_segment_interiors() {
        let segments = [
            Segment {
                start: crate::geometry::XY::new(0.0, 0.0),
                end: crate::geometry::XY::new(1.0, 0.0),
            },
            Segment {
                start: crate::geometry::XY::new(1.0, 0.0),
                end: crate::geometry::XY::new(1.0, 1.0),
            },
        ];
        let index = SegmentIndex::build(&segments);
        assert!(index.covers_point(Point::new_raw_xy(0.5, 0.0)));
    }

    #[test]
    fn chord_query_prunes_diagonal_envelope_amplification() {
        use crate::geometry::XY;
        use crate::geometry::segment_index::ChordIndex;

        let segments: Vec<_> = (0..1025)
            .map(|index| {
                let coordinate = f64::from(index);
                Segment {
                    start: XY::new(coordinate, coordinate + 32.0),
                    end: XY::new(coordinate + 0.25, coordinate + 32.0),
                }
            })
            .collect();
        let index = ChordIndex::build(&segments);
        let chord = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2048.0, 2048.0),
        };
        assert_eq!(
            SegmentIndex::build(&segments)
                .intersecting_candidates(chord)
                .count(),
            1025
        );
        let mut checked = 0;
        assert!(index.all_candidates(chord, |_| {
            checked += 1;
            true
        }));
        assert_eq!(checked, 0);
    }

    #[test]
    fn chord_query_retains_contacts_and_uncertain_boxes() {
        use crate::geometry::segment_index::ChordIndex;
        use crate::geometry::{XY, segments_intersect};

        let cases = [
            (
                Segment {
                    start: XY::new(0.0, 0.0),
                    end: XY::new(4.0, 4.0),
                },
                [
                    Segment {
                        start: XY::new(1.0, 1.0),
                        end: XY::new(3.0, 3.0),
                    },
                    Segment {
                        start: XY::new(0.0, 1.0),
                        end: XY::new(1.0, 0.0),
                    },
                    Segment {
                        start: XY::new(4.0, 4.0),
                        end: XY::new(5.0, 3.0),
                    },
                ],
            ),
            (
                Segment {
                    start: XY::new(-1e308, -1e308),
                    end: XY::new(1e308, 1e308),
                },
                [
                    Segment {
                        start: XY::new(-1.0, 1.0),
                        end: XY::new(1.0, -1.0),
                    },
                    Segment {
                        start: XY::new(0.0, 0.0),
                        end: XY::new(0.0, 0.0),
                    },
                    Segment {
                        start: XY::new(1e308, 1e308),
                        end: XY::new(1e308, 0.0),
                    },
                ],
            ),
            (
                Segment {
                    start: XY::new(0.0, 0.0),
                    end: XY::new(f64::from_bits(4), f64::from_bits(4)),
                },
                [
                    Segment {
                        start: XY::new(f64::from_bits(2), 0.0),
                        end: XY::new(f64::from_bits(2), f64::from_bits(4)),
                    },
                    Segment {
                        start: XY::new(0.0, 0.0),
                        end: XY::new(0.0, 0.0),
                    },
                    Segment {
                        start: XY::new(f64::from_bits(4), f64::from_bits(4)),
                        end: XY::new(f64::from_bits(4), 0.0),
                    },
                ],
            ),
        ];
        for (chord, segments) in cases {
            let segments: Vec<_> = segments.into_iter().cycle().take(1025).collect();
            let index = ChordIndex::build(&segments);
            let mut retained = vec![false; segments.len()];
            assert!(index.all_candidates(chord, |entry| {
                retained[entry.ordinal] = true;
                true
            }));
            for (ordinal, &segment) in segments.iter().enumerate() {
                assert!(segments_intersect(chord, segment));
                assert!(retained[ordinal], "{chord:?} against {segment:?}");
            }
        }
    }

    #[test]
    fn clearance_point_query_matches_exact_exhaustive_source_order() {
        use crate::geometry::numerics::product::exact_power_of_two;
        let cases = [
            vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, exact_power_of_two(-28)),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(-0.0, 0.0),
                Point::new_raw_xy(20.0, 20.0),
            ],
            vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(exact_power_of_two(-700), 0.0),
                Point::new_raw_xy(3.0 * exact_power_of_two(-700), 0.0),
                Point::new_raw_xy(100.0, 100.0),
            ],
            vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(exact_power_of_two(600), 0.0),
                Point::new_raw_xy(3.0 * exact_power_of_two(600), 0.0),
                Point::new_raw_xy(-(exact_power_of_two(600)), exact_power_of_two(600)),
            ],
        ];
        for points in cases {
            let index = PointSetIndex::build_clearance_with_ordinals(&points);
            let mut query = index.clearance_query();
            for (probe_ordinal, probe) in points.iter().copied().enumerate() {
                let mut expected: Option<(usize, SquaredDistanceKey)> = None;
                for (ordinal, target) in points.iter().copied().enumerate() {
                    if same_point(probe, target) {
                        continue;
                    }
                    let key = point_pair_distance_key(probe.xy(), target.xy());
                    let replace = match expected.as_ref() {
                        None => true,
                        Some((best_ordinal, best)) => {
                            let order = key
                                .cmp_distance(best)
                                .expect("finite test coordinates have comparable exact keys");
                            order.is_lt() || order.is_eq() && ordinal < *best_ordinal
                        },
                    };
                    if replace {
                        expected = Some((ordinal, key));
                    }
                }
                let actual = query.nearest_clearance_neighbor(probe, None).unwrap();
                assert_eq!(
                    actual.as_ref().map(|(ordinal, _)| *ordinal),
                    expected.as_ref().map(|(ordinal, _)| *ordinal),
                    "probe source ordinal {probe_ordinal}",
                );
                if let (Some((_, actual)), Some((_, expected))) = (actual, expected) {
                    assert!(
                        actual.cmp_distance(&expected).unwrap().is_eq(),
                        "probe source ordinal {probe_ordinal}",
                    );
                }
            }
        }
    }
}

// Prepared polygonal point membership lives in `geometry::point_location`
// (hierarchical Y-stabbing). This module keeps bipartite segment/point R-trees
// and the candidate-pair sweep.
