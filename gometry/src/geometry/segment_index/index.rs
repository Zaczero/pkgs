use rstar::{AABB, RTreeNode, RTreeObject};

use crate::HeapSize;
use crate::geometry::{
    BulkRTree, Ordering, Point, Segment, point_segment_distance_key,
    point_segment_distance_squared, same_point,
};

/// One segment as an R-tree leaf, carrying its position in the build slice
/// (so pair-once sweeps can keep `ordinal > i` candidates).
pub(crate) struct SegmentEntry {
    pub segment: Segment,
    pub ordinal: usize,
    envelope: AABB<[f64; 2]>,
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

impl SegmentIndex {
    pub(crate) fn build(segments: &[Segment]) -> Self {
        Self::build_from_iter(segments.iter().copied())
    }

    /// Build straight from a segment iterator — entries are gathered once for
    /// the bulk load, with no caller-side `Vec<Segment>` staging.
    pub(crate) fn build_from_iter(segments: impl IntoIterator<Item = Segment>) -> Self {
        Self {
            tree: BulkRTree::bulk_load_with_params(
                segments
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, segment)| SegmentEntry {
                        segment,
                        ordinal,
                        envelope: segment_envelope(segment),
                    })
                    .collect(),
            ),
        }
    }

    /// The realizing segment of the minimum [`point_segment_distance_squared`]
    /// from `point` to any indexed segment that improves on `best`, with its
    /// squared distance — or `None` when no candidate beats `best`. `keep`
    /// filters which computed candidate distances participate (minimum
    /// clearance skips zero/endpoint contacts); pruning uses the running
    /// accepted minimum, so filtered-out zeros never poison the bound.
    /// Equal rounded distances stay open long enough for the exact witness
    /// key, then segment ordinal, to select the same witness as a brute scan.
    /// Squared space — callers own the finite-range guarantee.
    pub(crate) fn nearest_segment_if(
        &self,
        point: Point,
        best: f64,
        keep: impl Fn(&Segment, f64) -> bool,
    ) -> Option<(Segment, f64)> {
        self.nearest_segment_ordinal_if(point, best, keep)
            .map(|(_, segment, distance)| (segment, distance))
    }

    pub(crate) fn nearest_segment_ordinal_if(
        &self,
        point: Point,
        mut best: f64,
        keep: impl Fn(&Segment, f64) -> bool,
    ) -> Option<(usize, Segment, f64)> {
        let query = [point.x, point.y];
        let mut witness = None;
        let mut stack: Vec<&RTreeNode<SegmentEntry>> = self.tree.root().children().iter().collect();
        while let Some(node) = stack.pop() {
            match node {
                RTreeNode::Parent(parent) => {
                    let lower_bound = parent.envelope().distance_2(&query);
                    if lower_bound < best || (witness.is_some() && lower_bound <= best) {
                        stack.extend(parent.children());
                    }
                },
                RTreeNode::Leaf(entry) => {
                    let lower_bound = entry.envelope.distance_2(&query);
                    if lower_bound < best || (witness.is_some() && lower_bound <= best) {
                        let distance = point_segment_distance_squared(point, entry.segment);
                        let replace = if distance < best {
                            true
                        } else if distance.total_cmp(&best).is_eq() {
                            witness.as_ref().is_some_and(|(ordinal, incumbent, _)| {
                                match point_segment_distance_key(point.xy(), entry.segment)
                                    .cmp(&point_segment_distance_key(point.xy(), *incumbent))
                                {
                                    Ordering::Less => true,
                                    Ordering::Equal => entry.ordinal < *ordinal,
                                    Ordering::Greater => false,
                                }
                            })
                        } else {
                            false
                        };
                        if replace && keep(&entry.segment, distance) {
                            best = distance;
                            witness = Some((entry.ordinal, entry.segment, distance));
                        }
                    }
                },
            }
        }
        witness
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
}

/// An R-tree over a point set (Hausdorff refinement, clearance candidates).
pub(crate) struct PointSetIndex {
    tree: BulkRTree<[f64; 2]>,
}

impl PointSetIndex {
    pub(crate) fn build(points: impl IntoIterator<Item = Point>) -> Self {
        Self {
            tree: BulkRTree::bulk_load_with_params(
                points.into_iter().map(|point| [point.x, point.y]).collect(),
            ),
        }
    }

    /// The nearest indexed point (INCLUDING a coincident one, distance 0) with
    /// its squared distance — the nearest-points/shortest-line query. `None`
    /// only for an empty set.
    pub(crate) fn nearest(&self, point: Point) -> Option<(Point, f64)> {
        self.tree
            .nearest_neighbor_with_distance_2([point.x, point.y])
            .map(|(coords, distance)| (Point::new_unchecked_xy(coords[0], coords[1]), distance))
    }

    /// The nearest indexed point that is not bit-identical to `point`, with
    /// its squared distance (`None` when none exists) — the
    /// minimum-clearance "nearest other vertex" query.
    ///
    /// Skips only **true** coordinate identity. A squared residual of 0 from
    /// underflow (distinct points whose `dx²+dy²` collapses) is still a
    /// clearance candidate — the finisher recomputes in distance space.
    pub(crate) fn nearest_other(&self, point: Point) -> Option<(Point, f64)> {
        self.tree
            .nearest_neighbor_iter_with_distance_2([point.x, point.y])
            .find(|&(coords, _distance)| {
                !same_point(point, Point::new_unchecked_xy(coords[0], coords[1]))
            })
            .map(|(coords, distance)| (Point::new_unchecked_xy(coords[0], coords[1]), distance))
    }
}

impl HeapSize for PointSetIndex {
    fn heap_bytes(&self) -> usize {
        fn node_bytes(node: &RTreeNode<[f64; 2]>) -> usize {
            match node {
                RTreeNode::Leaf(_) => std::mem::size_of::<RTreeNode<[f64; 2]>>(),
                RTreeNode::Parent(parent) => {
                    std::mem::size_of::<RTreeNode<[f64; 2]>>()
                        + parent.children().iter().map(node_bytes).sum::<usize>()
                },
            }
        }

        self.tree.root().children().iter().map(node_bytes).sum()
    }
}

// Prepared polygonal point membership lives in `geometry::point_location`
// (hierarchical Y-stabbing). This module keeps bipartite segment/point R-trees
// and the candidate-pair sweep.
