//! Geodesic segment BVH — branch-and-bound point probes over ellipsoidal
//! segments.
//!
//! Nodes carry one conservative auxiliary-sphere cap: an anchor point and a
//! true-metric reach covering every descendant segment. Traversal lower-bounds
//! a query point by `max(aux_bound(query, anchor) - reach, 0)`, then refines
//! only surviving leaves with the numerical Karney segment kernel. The bound
//! is purely pruning; contact, minimum, and witness semantics stay owned by
//! `GeodesicMetric`.

use crate::HeapSize;
use crate::collections::HashMap;
use crate::error::Result;
use crate::geometry::{
    BvhCode, BvhNode, GeodesicMetric, GeodesicParts, GeodesicSegment, GeodesicSegmentQuery,
    GeodesicSegmentQueryResult, GeodesicSegmentScratch, GeodesicWitnessNeed, HashMapExt as _,
    Point,
};

const GEODESIC_LEAF_SEGMENTS: usize = 8;

fn geodesic_leaf_count(segment_count: usize) -> usize {
    if segment_count <= GEODESIC_LEAF_SEGMENTS {
        return 1;
    }
    let quotient = segment_count / GEODESIC_LEAF_SEGMENTS;
    let base = 1_usize << quotient.ilog2();
    // At `base * threshold` segments the balanced tree has `base` leaves;
    // it gains one leaf per segment until the next complete level.
    (base * 2).min(base + segment_count - GEODESIC_LEAF_SEGMENTS * base)
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

fn cached_auxiliary_unit(
    aux_cache: &mut HashMap<(u64, u64), [f64; 3]>,
    point: crate::geometry::XY,
    metric: &impl GeodesicMetric,
) -> [f64; 3] {
    let key = (point.x.to_bits(), point.y.to_bits());
    *aux_cache
        .entry(key)
        .or_insert_with(|| metric.auxiliary_unit(point))
}

impl GeodesicFacetBvh {
    pub(super) fn build(
        segments: &[GeodesicSegment],
        metric: &impl GeodesicMetric,
    ) -> Result<Option<Self>> {
        let Some(segment_count) = u32::try_from(segments.len()).ok() else {
            return Ok(None);
        };
        if segment_count == 0 || segment_count > BvhCode::LEAF_BIT / 2 {
            return Ok(None);
        }
        let leaf_count = geodesic_leaf_count(segments.len());
        let node_count = 2 * leaf_count - 1;
        let mut tree = Self {
            anchors: vec![Point::new_raw_xy(0.0, 0.0); node_count].into_boxed_slice(),
            reaches: vec![0.0; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
            leaves: vec![GeodesicLeaf { first: 0, len: 0 }; leaf_count].into_boxed_slice(),
            segment_ids: vec![0; segments.len()].into_boxed_slice(),
        };
        let mut aux_cache = HashMap::with_capacity(segments.len().wrapping_add(1));
        let mut ids = Vec::new();
        crate::try_reserve_vec(&mut ids, segments.len())?;
        for (index, segment) in segments.iter().enumerate() {
            let start_aux = cached_auxiliary_unit(&mut aux_cache, segment.start.xy(), metric);
            let end_aux = cached_auxiliary_unit(&mut aux_cache, segment.end.xy(), metric);
            // `ids` receives exactly one entry per segment after the exact
            // reservation above.
            ids.push(SegmentBuild {
                index: index as u32,
                centroid: [
                    start_aux[0] + end_aux[0],
                    start_aux[1] + end_aux[1],
                    start_aux[2] + end_aux[2],
                ],
            });
        }
        let mut next_leaf = 0;
        let mut next_segment_id = 0;
        let mut next = 1;
        tree.fill_node(
            0,
            &mut next,
            segments,
            &mut ids,
            &mut next_leaf,
            &mut next_segment_id,
            metric,
        );
        debug_assert_eq!(next, node_count);
        debug_assert_eq!(next_leaf, leaf_count);
        debug_assert_eq!(next_segment_id, segments.len());
        Ok(Some(tree))
    }

    fn fill_node(
        &mut self,
        node: usize,
        next: &mut usize,
        segments: &[GeodesicSegment],
        ids: &mut [SegmentBuild],
        next_leaf: &mut usize,
        next_segment_id: &mut usize,
        metric: &impl GeodesicMetric,
    ) {
        if ids.len() <= GEODESIC_LEAF_SEGMENTS {
            ids.sort_unstable_by_key(|entry| entry.index);
            let first = *next_segment_id as u32;
            for entry in ids.iter() {
                self.segment_ids[*next_segment_id] = entry.index;
                *next_segment_id += 1;
            }
            let len = ids.len() as u8;
            let leaf_index = *next_leaf as u32;
            self.leaves[*next_leaf] = GeodesicLeaf { first, len };
            *next_leaf += 1;
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
            next_leaf,
            next_segment_id,
            metric,
        );
        self.fill_node(
            first_child + 1,
            next,
            segments,
            upper,
            next_leaf,
            next_segment_id,
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
        segment_scratch: &mut GeodesicSegmentScratch,
        mut best: f64,
    ) -> Result<f64> {
        stack.clear();
        crate::try_push_vec(stack, 0)?;
        while let Some(node) = stack.pop() {
            let node = node as usize;
            if self.bound(point, node, metric)? > best {
                continue;
            }
            match BvhCode::new(self.codes[node]).decode() {
                BvhNode::Leaf(leaf_index) => {
                    let leaf = self.leaves[leaf_index];
                    for edge_index in self.leaf_segment_ids(leaf) {
                        let edge_index = *edge_index as usize;
                        let segment = parts.segments[edge_index];
                        let bound = metric.point_distance_lower_bound(point, segment.start)?
                            - segment.length;
                        if bound <= best {
                            let result = metric.point_segment_query(
                                point,
                                &segment,
                                GeodesicSegmentQuery::Minimum {
                                    cutoff: best,
                                    witness: GeodesicWitnessNeed::No,
                                },
                                segment_scratch,
                            )?;
                            if let GeodesicSegmentQueryResult::Attained { minimum, .. } = result {
                                best = best.min(minimum.distance);
                            }
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(point, first_child as u32, metric, stack)?;
                },
            }
        }
        for &target in &parts.point_only {
            if metric.point_distance_lower_bound(point, target)? <= best {
                best = best.min(metric.segment_length(point, target));
            }
        }
        Ok(best)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the BVH traversal keeps node order, segment scratch, witness order, and tie state explicit at one hot call boundary"
    )]
    pub(super) fn witness_point(
        &self,
        vertex: Point,
        vertex_index: u32,
        parts: &GeodesicParts,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
        segment_scratch: &mut GeodesicSegmentScratch,
        mut best: Option<GeodesicWitnessCandidate>,
        order_offset: usize,
        swapped: bool,
        width: usize,
    ) -> Result<Option<GeodesicWitnessCandidate>> {
        stack.clear();
        crate::try_push_vec(stack, 0)?;
        while let Some(node) = stack.pop() {
            let node = node as usize;
            let best_distance = best
                .as_ref()
                .map_or(f64::INFINITY, |candidate| candidate.distance);
            if self.bound(vertex, node, metric)? > best_distance {
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
                        let bound = metric.point_distance_lower_bound(vertex, segment.start)?
                            - segment.length;
                        if bound > best_distance {
                            continue;
                        }
                        let result = metric.point_segment_query(
                            vertex,
                            &segment,
                            GeodesicSegmentQuery::Minimum {
                                cutoff: best_distance,
                                witness: GeodesicWitnessNeed::Yes,
                            },
                            segment_scratch,
                        )?;
                        let GeodesicSegmentQueryResult::Attained {
                            minimum,
                            foot: Some(target),
                        } = result
                        else {
                            continue;
                        };
                        let candidate = GeodesicWitnessCandidate {
                            probe: vertex,
                            target,
                            distance: minimum.distance,
                            lower: minimum.lower,
                            evaluation_error: minimum.evaluation_error,
                            feature: GeodesicTargetFeature::Segment {
                                index: edge_index,
                                along: minimum.along,
                            },
                            order,
                            swapped,
                        };
                        if geodesic_witness_is_better(best.as_ref(), &candidate) {
                            best = Some(candidate);
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(vertex, first_child as u32, metric, stack)?;
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
                if metric.point_distance_lower_bound(vertex, point)? > best_distance {
                    continue;
                }
                let distance = metric.segment_length(vertex, point);
                let evaluation_error = metric.geodesic_numerical_policy().evaluation_allowance(1.0);
                let candidate = GeodesicWitnessCandidate {
                    probe: vertex,
                    target: point,
                    distance,
                    lower: (distance - evaluation_error).max(0.0),
                    evaluation_error,
                    feature: GeodesicTargetFeature::Point,
                    order,
                    swapped,
                };
                if geodesic_witness_is_better(best.as_ref(), &candidate) {
                    best = Some(candidate);
                }
            }
        }
        Ok(best)
    }

    pub(super) fn dwithin_point(
        &self,
        point: Point,
        parts: &GeodesicParts,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
        segment_scratch: &mut GeodesicSegmentScratch,
        limit: f64,
    ) -> Result<bool> {
        if limit.is_infinite() && limit.is_sign_positive() {
            return Ok(!parts.segments.is_empty() || !parts.point_only.is_empty());
        }
        stack.clear();
        crate::try_push_vec(stack, 0)?;
        while let Some(node) = stack.pop() {
            let node = node as usize;
            if self.bound(point, node, metric)? > limit {
                continue;
            }
            match BvhCode::new(self.codes[node]).decode() {
                BvhNode::Leaf(leaf_index) => {
                    let leaf = self.leaves[leaf_index];
                    for edge_index in self.leaf_segment_ids(leaf) {
                        let edge_index = *edge_index as usize;
                        let segment = parts.segments[edge_index];
                        if metric.point_distance_lower_bound(point, segment.start)? - segment.length
                            > limit
                        {
                            continue;
                        }
                        let result = metric.point_segment_query(
                            point,
                            &segment,
                            GeodesicSegmentQuery::Threshold {
                                limit,
                                witness: GeodesicWitnessNeed::No,
                            },
                            segment_scratch,
                        )?;
                        match result {
                            GeodesicSegmentQueryResult::Attained { .. } => return Ok(true),
                            GeodesicSegmentQueryResult::ThresholdOutside
                            | GeodesicSegmentQueryResult::Excluded => {},
                            GeodesicSegmentQueryResult::ThresholdResolvedOutside { minimum } => {
                                if minimum <= limit {
                                    return Ok(true);
                                }
                            },
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let first_child = first_child as u32;
                    crate::try_push_vec(stack, first_child)?;
                    crate::try_push_vec(stack, first_child + 1)?;
                },
            }
        }
        for &target in &parts.point_only {
            if metric.point_distance_lower_bound(point, target)? <= limit
                && metric.segment_length(point, target) <= limit
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn push_nearer_last(
        &self,
        point: Point,
        first_child: u32,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
    ) -> Result<()> {
        let left = first_child as usize;
        let right = left + 1;
        let left_bound = self.bound(point, left, metric)?;
        let right_bound = self.bound(point, right, metric)?;
        if left_bound < right_bound {
            crate::try_push_vec(stack, first_child + 1)?;
            crate::try_push_vec(stack, first_child)?;
        } else {
            crate::try_push_vec(stack, first_child)?;
            crate::try_push_vec(stack, first_child + 1)?;
        }
        Ok(())
    }

    fn bound(&self, point: Point, node: usize, metric: &impl GeodesicMetric) -> Result<f64> {
        Ok(
            (metric.point_distance_lower_bound(point, self.anchors[node])? - self.reaches[node])
                .max(0.0),
        )
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
pub(super) enum GeodesicTargetFeature {
    Segment { index: usize, along: f64 },
    Point,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct GeodesicWitnessCandidate {
    pub(super) probe: Point,
    pub(super) target: Point,
    pub(super) distance: f64,
    pub(super) lower: f64,
    pub(super) evaluation_error: f64,
    pub(super) feature: GeodesicTargetFeature,
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

#[cfg(test)]
mod tests {
    use super::geodesic_leaf_count;

    #[test]
    fn balanced_leaf_count_matches_split_boundaries() {
        for (segment_count, expected_leaves) in [
            (1, 1),
            (8, 1),
            (9, 2),
            (16, 2),
            (17, 3),
            (18, 4),
            (32, 4),
            (33, 5),
            (34, 6),
            (35, 7),
            (36, 8),
            (64, 8),
            (65, 9),
            (72, 16),
            (128, 16),
            (129, 17),
            (144, 32),
            (256, 32),
            (257, 33),
            (512, 64),
            (513, 65),
        ] {
            let leaves = geodesic_leaf_count(segment_count);
            assert_eq!(leaves, expected_leaves, "segments={segment_count}");
        }
    }
}
