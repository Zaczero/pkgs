//! Geodesic segment BVH — branch-and-bound point probes over ellipsoidal
//! segments.
//!
//! Nodes carry one conservative auxiliary-sphere cap: an anchor point and a
//! true-metric reach covering every descendant segment. Traversal lower-bounds
//! a query point by `max(aux_bound(query, anchor) - reach, 0)`, then refines
//! only surviving leaves with the existing exact Karney/golden-section segment
//! kernel. The bound is purely pruning; exact distance and witness semantics
//! stay owned by `GeodesicMetric`.

use crate::HeapSize;
use crate::collections::HashMap;
use crate::geometry::{
    BvhCode, BvhNode, GeodesicMetric, GeodesicParts, GeodesicSegment, HashMapExt as _, Point,
};

const GEODESIC_LEAF_SEGMENTS: usize = 8;
const GEODESIC_BVH_MAX_POINT_VISITS: usize = 64;

pub(super) enum WitnessPointOutcome {
    LimitExceeded,
    Best(Option<GeodesicWitnessCandidate>),
}

#[derive(Clone, Copy)]
struct GeodesicLeaf {
    first: u32,
    len: u8,
}

#[derive(Clone, Copy)]
struct SegmentBuild {
    index: u32,
    centroid: [f64; 3],
}

pub(crate) struct GeodesicFacetBvh {
    anchors: Box<[Point]>,
    reaches: Box<[f64]>,
    codes: Box<[u32]>,
    leaves: Box<[GeodesicLeaf]>,
    segment_ids: Box<[u32]>,
}

impl GeodesicFacetBvh {
    pub(super) fn build(
        segments: &[GeodesicSegment],
        metric: &impl GeodesicMetric,
    ) -> Option<Self> {
        let segment_count = u32::try_from(segments.len()).ok()?;
        if segment_count == 0 || segment_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let max_leaf_count = segments.len();
        // Perfect binary tree over `n` leaves uses `2n-1` nodes, but near a
        // power of two the recursive partition often realises far fewer
        // internal nodes. Reserve capacity and shrink to the filled range
        // after build so we do not keep an ~8× over-allocation.
        let node_capacity = 2 * max_leaf_count - 1;
        let mut tree = Self {
            anchors: vec![Point::new_unchecked_xy(0.0, 0.0); node_capacity].into_boxed_slice(),
            reaches: vec![0.0; node_capacity].into_boxed_slice(),
            codes: vec![0; node_capacity].into_boxed_slice(),
            leaves: Box::default(),
            segment_ids: Box::default(),
        };
        let mut leaves = Vec::with_capacity(max_leaf_count);
        let mut segment_ids = Vec::with_capacity(segments.len());
        let mut aux_cache = HashMap::with_capacity(segments.len() + 1);
        let mut ids = segments
            .iter()
            .enumerate()
            .map(|(index, segment)| {
                let start_aux = *aux_cache
                    .entry(segment.start)
                    .or_insert_with(|| aux_unit(segment.start));
                let end_aux = *aux_cache
                    .entry(segment.end)
                    .or_insert_with(|| aux_unit(segment.end));
                SegmentBuild {
                    index: index as u32,
                    centroid: [
                        start_aux[0] + end_aux[0],
                        start_aux[1] + end_aux[1],
                        start_aux[2] + end_aux[2],
                    ],
                }
            })
            .collect::<Vec<_>>();
        let mut next = 1;
        tree.fill_node(
            0,
            &mut next,
            segments,
            &mut ids,
            &mut leaves,
            &mut segment_ids,
            metric,
        );
        // `next` is the count of realized nodes; drop unused tail capacity
        // (near power-of-two leaf counts this was ~8× over-reserved).
        if next < node_capacity {
            let mut anchors = tree.anchors.into_vec();
            let mut reaches = tree.reaches.into_vec();
            let mut codes = tree.codes.into_vec();
            anchors.truncate(next);
            reaches.truncate(next);
            codes.truncate(next);
            anchors.shrink_to_fit();
            reaches.shrink_to_fit();
            codes.shrink_to_fit();
            tree.anchors = anchors.into_boxed_slice();
            tree.reaches = reaches.into_boxed_slice();
            tree.codes = codes.into_boxed_slice();
        }
        tree.leaves = leaves.into_boxed_slice();
        tree.segment_ids = segment_ids.into_boxed_slice();
        Some(tree)
    }

    fn fill_node(
        &mut self,
        node: usize,
        next: &mut usize,
        segments: &[GeodesicSegment],
        ids: &mut [SegmentBuild],
        leaves: &mut Vec<GeodesicLeaf>,
        segment_ids: &mut Vec<u32>,
        metric: &impl GeodesicMetric,
    ) {
        if ids.len() <= GEODESIC_LEAF_SEGMENTS {
            ids.sort_unstable_by_key(|entry| entry.index);
            let first = segment_ids.len() as u32;
            segment_ids.extend(ids.iter().map(|entry| entry.index));
            let len = ids.len() as u8;
            let leaf_index = leaves.len() as u32;
            leaves.push(GeodesicLeaf { first, len });
            self.codes[node] = BvhCode::leaf(leaf_index as usize).raw();
            self.fill_cap(node, segments, ids, metric);
            return;
        }

        let axis = split_axis(ids);
        let mid = ids.len() / 2;
        ids.select_nth_unstable_by(mid, |a, b| {
            a.centroid[axis]
                .total_cmp(&b.centroid[axis])
                .then_with(|| a.index.cmp(&b.index))
        });
        let (lower, upper) = ids.split_at_mut(mid);
        let first_child = *next;
        *next += 2;
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_node(
            first_child,
            next,
            segments,
            lower,
            leaves,
            segment_ids,
            metric,
        );
        self.fill_node(
            first_child + 1,
            next,
            segments,
            upper,
            leaves,
            segment_ids,
            metric,
        );
        self.anchors[node] = self.anchors[first_child];
        self.reaches[node] = child_reach(self, node, first_child, metric).max(child_reach(
            self,
            node,
            first_child + 1,
            metric,
        ));
    }

    fn fill_cap(
        &mut self,
        node: usize,
        segments: &[GeodesicSegment],
        ids: &[SegmentBuild],
        metric: &impl GeodesicMetric,
    ) {
        let anchor = segments[ids[0].index as usize].start;
        let mut reach = 0.0_f64;
        for id in ids {
            let segment = segments[id.index as usize];
            reach = reach.max(metric.segment_length(anchor, segment.start) + segment.length);
        }
        self.anchors[node] = anchor;
        self.reaches[node] = reach;
    }

    pub(super) fn min_point_distance(
        &self,
        point: Point,
        parts: &GeodesicParts,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
        mut best: f64,
    ) -> Option<f64> {
        let mut visits = 0;
        stack.clear();
        stack.push(0);
        while let Some(node) = stack.pop() {
            visits += 1;
            if visits > GEODESIC_BVH_MAX_POINT_VISITS {
                return None;
            }
            let node = node as usize;
            if self.bound(point, node, metric) > best {
                continue;
            }
            match BvhCode::new(self.codes[node]).decode() {
                BvhNode::Leaf(leaf_index) => {
                    let leaf = self.leaves[leaf_index];
                    for edge_index in self.leaf_segment_ids(leaf) {
                        let edge_index = *edge_index as usize;
                        let segment = parts.segments[edge_index];
                        let bound = metric.point_distance_lower_bound(point, segment.start)
                            - segment.length;
                        if bound <= best {
                            best = best.min(metric.point_to_segment(point, segment, best));
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(point, first_child as u32, metric, stack);
                },
            }
        }
        for &target in &parts.point_only {
            if metric.point_distance_lower_bound(point, target) <= best {
                best = best.min(metric.segment_length(point, target));
            }
        }
        Some(best)
    }

    pub(super) fn witness_point(
        &self,
        vertex: Point,
        vertex_index: u32,
        parts: &GeodesicParts,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
        mut best: Option<GeodesicWitnessCandidate>,
        order_offset: usize,
        swapped: bool,
        width: usize,
    ) -> WitnessPointOutcome {
        let mut visits = 0;
        stack.clear();
        stack.push(0);
        while let Some(node) = stack.pop() {
            visits += 1;
            if visits > GEODESIC_BVH_MAX_POINT_VISITS {
                return WitnessPointOutcome::LimitExceeded;
            }
            let node = node as usize;
            let best_distance = best
                .as_ref()
                .map_or(f64::INFINITY, |candidate| candidate.distance);
            if self.bound(vertex, node, metric) > best_distance {
                continue;
            }
            match BvhCode::new(self.codes[node]).decode() {
                BvhNode::Leaf(leaf_index) => {
                    let leaf = self.leaves[leaf_index];
                    for edge_index in self.leaf_segment_ids(leaf) {
                        let edge_index = *edge_index as usize;
                        let segment = parts.segments[edge_index];
                        let order = order_offset + vertex_index as usize * width + edge_index;
                        let best_distance = witness_refine_limit(best.as_ref(), order);
                        let bound = metric.point_distance_lower_bound(vertex, segment.start)
                            - segment.length;
                        if bound > best_distance {
                            continue;
                        }
                        let witness = metric.point_segment_witness(vertex, segment, best_distance);
                        let candidate = GeodesicWitnessCandidate {
                            probe: vertex,
                            target: witness.foot,
                            distance: witness.distance,
                            order,
                            swapped,
                        };
                        if geodesic_witness_is_better(best.as_ref(), &candidate) {
                            best = Some(candidate);
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(vertex, first_child as u32, metric, stack);
                },
            }
        }
        if !parts.point_only.is_empty() {
            for (point_index, &point) in parts.point_only.iter().enumerate() {
                let order = order_offset
                    + vertex_index as usize * width
                    + parts.segments.len()
                    + point_index;
                let best_distance = witness_refine_limit(best.as_ref(), order);
                if metric.point_distance_lower_bound(vertex, point) > best_distance {
                    continue;
                }
                let distance = metric.segment_length(vertex, point);
                let candidate = GeodesicWitnessCandidate {
                    probe: vertex,
                    target: point,
                    distance,
                    order,
                    swapped,
                };
                if geodesic_witness_is_better(best.as_ref(), &candidate) {
                    best = Some(candidate);
                }
            }
        }
        WitnessPointOutcome::Best(best)
    }

    pub(super) fn dwithin_point(
        &self,
        point: Point,
        parts: &GeodesicParts,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
        limit: f64,
    ) -> Option<bool> {
        let mut visits = 0;
        stack.clear();
        stack.push(0);
        while let Some(node) = stack.pop() {
            visits += 1;
            if visits > GEODESIC_BVH_MAX_POINT_VISITS {
                return None;
            }
            let node = node as usize;
            if self.bound(point, node, metric) > limit {
                continue;
            }
            match BvhCode::new(self.codes[node]).decode() {
                BvhNode::Leaf(leaf_index) => {
                    let leaf = self.leaves[leaf_index];
                    for edge_index in self.leaf_segment_ids(leaf) {
                        let edge_index = *edge_index as usize;
                        let segment = parts.segments[edge_index];
                        let bound = metric.point_distance_lower_bound(point, segment.start)
                            - segment.length;
                        if bound <= limit && metric.point_to_segment(point, segment, limit) <= limit
                        {
                            return Some(true);
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    stack.push(first_child);
                    stack.push(first_child + 1);
                },
            }
        }
        Some(parts.point_only.iter().any(|&target| {
            metric.point_distance_lower_bound(point, target) <= limit
                && metric.segment_length(point, target) <= limit
        }))
    }

    fn push_nearer_last(
        &self,
        point: Point,
        first_child: u32,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
    ) {
        let left = first_child as usize;
        let right = left + 1;
        let left_bound = self.bound(point, left, metric);
        let right_bound = self.bound(point, right, metric);
        if left_bound < right_bound {
            stack.push(first_child + 1);
            stack.push(first_child);
        } else {
            stack.push(first_child);
            stack.push(first_child + 1);
        }
    }

    fn bound(&self, point: Point, node: usize, metric: &impl GeodesicMetric) -> f64 {
        (metric.point_distance_lower_bound(point, self.anchors[node]) - self.reaches[node]).max(0.0)
    }

    fn leaf_segment_ids(&self, leaf: GeodesicLeaf) -> &[u32] {
        &self.segment_ids[leaf.first as usize..leaf.first as usize + leaf.len as usize]
    }
}

crate::heapless!(GeodesicLeaf);

impl HeapSize for GeodesicFacetBvh {
    fn heap_bytes(&self) -> usize {
        self.anchors.heap_bytes()
            + self.reaches.heap_bytes()
            + self.codes.heap_bytes()
            + self.leaves.heap_bytes()
            + self.segment_ids.heap_bytes()
    }
}

fn child_reach(
    tree: &GeodesicFacetBvh,
    parent: usize,
    child: usize,
    metric: &impl GeodesicMetric,
) -> f64 {
    metric.segment_length(tree.anchors[parent], tree.anchors[child]) + tree.reaches[child]
}

fn split_axis(ids: &[SegmentBuild]) -> usize {
    let mut min = [f64::INFINITY; 3];
    let mut max = [f64::NEG_INFINITY; 3];
    for id in ids {
        for axis in 0..3 {
            min[axis] = min[axis].min(id.centroid[axis]);
            max[axis] = max[axis].max(id.centroid[axis]);
        }
    }
    let spans = [max[0] - min[0], max[1] - min[1], max[2] - min[2]];
    if spans[1] > spans[0] && spans[1] >= spans[2] {
        1
    } else if spans[2] > spans[0] && spans[2] > spans[1] {
        2
    } else {
        0
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct GeodesicWitnessCandidate {
    pub(super) probe: Point,
    pub(super) target: Point,
    pub(super) distance: f64,
    pub(super) order: usize,
    pub(super) swapped: bool,
}

pub(super) fn witness_refine_limit(best: Option<&GeodesicWitnessCandidate>, order: usize) -> f64 {
    best.map_or(f64::INFINITY, |candidate| {
        if order < candidate.order {
            f64::INFINITY
        } else {
            candidate.distance
        }
    })
}

#[expect(clippy::float_cmp, reason = "exact tie-break preserves brute order")]
pub(super) fn geodesic_witness_is_better(
    best: Option<&GeodesicWitnessCandidate>,
    candidate: &GeodesicWitnessCandidate,
) -> bool {
    best.is_none_or(|best| {
        candidate.distance < best.distance
            || (candidate.distance == best.distance && candidate.order < best.order)
    })
}

fn aux_unit(point: Point) -> [f64; 3] {
    let lon = point.x.to_radians();
    let lat = point.y.to_radians();
    // WGS84-ish reduced latitude is enough for a split heuristic; the
    // pruning proof uses the metric-provided lower bound, not this centroid.
    let beta = f64::atan((1.0 - 1.0 / 298.257_223_563) * lat.tan())
        .clamp(-std::f64::consts::FRAC_PI_2, std::f64::consts::FRAC_PI_2);
    [beta.cos() * lon.cos(), beta.cos() * lon.sin(), beta.sin()]
}
