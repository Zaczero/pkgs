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
use crate::error::Result;
use crate::geometry::{
    BvhCode, BvhNode, GeodesicMetric, GeodesicParts, GeodesicSegment, GeodesicSegmentQuery,
    GeodesicSegmentQueryResult, GeodesicSegmentScratch, GeodesicWitnessNeed, Point,
};

const GEODESIC_LEAF_SEGMENTS: usize = 8;

fn geodesic_leaf_count(segment_count: usize) -> usize {
    if segment_count <= GEODESIC_LEAF_SEGMENTS {
        return 1;
    }
    let quotient = segment_count / GEODESIC_LEAF_SEGMENTS;
    let base = 1_usize << quotient.ilog2();
    // At `base * threshold` segments the balanced tree has `base` leaves;
    // it gains one leaf per segment until the next complete level. `build`
    // admits at most `LEAF_BIT / 2` segments before calling this helper, so the
    // leaf formula and its node count are representable in `usize`.
    let complete_level = base.wrapping_mul(2);
    let partial_level = base
        .wrapping_add(segment_count)
        .wrapping_sub(GEODESIC_LEAF_SEGMENTS.wrapping_mul(base));
    complete_level.min(partial_level)
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

#[derive(Clone, Copy)]
struct CachedAuxiliaryUnit {
    key: (u64, u64),
    value: [f64; 3],
}

#[derive(Default)]
struct AuxiliaryUnitCache {
    /// Two slots reduce instructions by 0.22–1.43% versus the map in measured BVH builds.
    first: Option<CachedAuxiliaryUnit>,
    second: Option<CachedAuxiliaryUnit>,
    replace_first: bool,
}

impl AuxiliaryUnitCache {
    fn get_or_compute(
        &mut self,
        point: crate::geometry::XY,
        metric: &impl GeodesicMetric,
    ) -> [f64; 3] {
        let key = (point.x.to_bits(), point.y.to_bits());
        if let Some(cached) = self.first
            && cached.key == key
        {
            return cached.value;
        }
        if let Some(cached) = self.second
            && cached.key == key
        {
            return cached.value;
        }

        let cached = CachedAuxiliaryUnit {
            key,
            value: metric.auxiliary_unit(point),
        };
        if self.replace_first {
            self.first = Some(cached);
        } else {
            self.second = Some(cached);
        }
        self.replace_first = !self.replace_first;
        cached.value
    }
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
        let leaf_count = geodesic_leaf_count(segments.len());
        let node_count = leaf_count.wrapping_mul(2).wrapping_sub(1);
        let mut tree = Self {
            anchors: vec![Point::new_raw_xy(0.0, 0.0); node_count].into_boxed_slice(),
            reaches: vec![0.0; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
            leaves: vec![GeodesicLeaf { first: 0, len: 0 }; leaf_count].into_boxed_slice(),
            segment_ids: vec![0; segments.len()].into_boxed_slice(),
        };
        let mut aux_cache = AuxiliaryUnitCache::default();
        let mut ids = Vec::with_capacity(segments.len());
        for (index, segment) in segments.iter().enumerate() {
            let start_aux = aux_cache.get_or_compute(segment.start.xy(), metric);
            let end_aux = aux_cache.get_or_compute(segment.end.xy(), metric);
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
        Some(tree)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node, leaf, and segment IDs are generated together inside the admitted u32-sized BVH"
    )]
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
                *next_segment_id = (*next_segment_id).wrapping_add(1);
            }
            let len = ids.len() as u8;
            let leaf_index = *next_leaf as u32;
            self.leaves[*next_leaf] = GeodesicLeaf { first, len };
            *next_leaf = (*next_leaf).wrapping_add(1);
            self.codes[node] = BvhCode::leaf(leaf_index as usize).raw();
            self.fill_cap(node, segments, ids, metric);
            return;
        }

        let axis = split_axis(ids);
        let mid = ids.len() / 2;
        ids.select_nth_unstable_by(mid, |a, b| {
            axis.coordinate(a.centroid)
                .total_cmp(&axis.coordinate(b.centroid))
                .then_with(|| a.index.cmp(&b.index))
        });
        let (lower, upper) = ids.split_at_mut(mid);
        let first_child = *next;
        *next = (*next).wrapping_add(2);
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
            first_child.wrapping_add(1),
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
            first_child.wrapping_add(1),
            metric,
        ));
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "leaf segment IDs are selected from the same admitted segment slice used to build this BVH"
    )]
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

    #[expect(
        clippy::indexing_slicing,
        reason = "traversal node and leaf IDs are decoded from codes emitted by this immutable BVH builder"
    )]
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
        stack.push(0);
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
    #[expect(
        clippy::indexing_slicing,
        reason = "traversal node and leaf IDs are decoded from codes emitted by this immutable BVH builder"
    )]
    pub(super) fn witness_point(
        &self,
        vertex: Point,
        vertex_index: usize,
        parts: &GeodesicParts,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
        segment_scratch: &mut GeodesicSegmentScratch,
        mut best: Option<GeodesicWitnessCandidate>,
        order_offset: WitnessOrder,
        swapped: bool,
        width: u128,
    ) -> Result<Option<GeodesicWitnessCandidate>> {
        let row_order = WitnessOrder::after_rows(order_offset, vertex_index, width);
        stack.clear();
        stack.push(0);
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
                        let order = row_order.with_column(edge_index as u128);
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
                let order = row_order.with_column(WitnessOrder::point_feature(
                    parts.segments.len(),
                    point_index,
                ));
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

    #[expect(
        clippy::indexing_slicing,
        reason = "traversal node and leaf IDs are decoded from codes emitted by this immutable BVH builder"
    )]
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
        stack.push(0);
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
                    stack.push(first_child);
                    stack.push(first_child.wrapping_add(1));
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
        let right = left.wrapping_add(1);
        let left_bound = self.bound(point, left, metric)?;
        let right_bound = self.bound(point, right, metric)?;
        if left_bound < right_bound {
            stack.push(first_child.wrapping_add(1));
            stack.push(first_child);
        } else {
            stack.push(first_child);
            stack.push(first_child.wrapping_add(1));
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node IDs are rooted in this immutable BVH and its equal-length node columns"
    )]
    fn bound(&self, point: Point, node: usize, metric: &impl GeodesicMetric) -> Result<f64> {
        Ok(
            (metric.point_distance_lower_bound(point, self.anchors[node])? - self.reaches[node])
                .max(0.0),
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "leaf ranges are emitted by fill_node from the reserved segment-ID column"
    )]
    fn leaf_segment_ids(&self, leaf: GeodesicLeaf) -> &[u32] {
        let first = leaf.first as usize;
        let end = first.wrapping_add(usize::from(leaf.len));
        &self.segment_ids[first..end]
    }
}

crate::heapless!(GeodesicLeaf);

impl HeapSize for GeodesicFacetBvh {
    fn heap_bytes(&self) -> usize {
        self.anchors
            .heap_bytes()
            .saturating_add(self.reaches.heap_bytes())
            .saturating_add(self.codes.heap_bytes())
            .saturating_add(self.leaves.heap_bytes())
            .saturating_add(self.segment_ids.heap_bytes())
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "parent and adjacent child IDs are emitted by fill_node inside equal-length node columns"
)]
fn child_reach(
    tree: &GeodesicFacetBvh,
    parent: usize,
    child: usize,
    metric: &impl GeodesicMetric,
) -> f64 {
    metric.segment_length(tree.anchors[parent], tree.anchors[child]) + tree.reaches[child]
}

#[derive(Clone, Copy)]
enum AuxiliaryAxis {
    X,
    Y,
    Z,
}

impl AuxiliaryAxis {
    fn coordinate(self, point: [f64; 3]) -> f64 {
        match self {
            Self::X => point[0],
            Self::Y => point[1],
            Self::Z => point[2],
        }
    }
}

fn split_axis(ids: &[SegmentBuild]) -> AuxiliaryAxis {
    let mut min = [f64::INFINITY; 3];
    let mut max = [f64::NEG_INFINITY; 3];
    for id in ids {
        for ((minimum, maximum), coordinate) in min.iter_mut().zip(max.iter_mut()).zip(id.centroid)
        {
            *minimum = (*minimum).min(coordinate);
            *maximum = (*maximum).max(coordinate);
        }
    }
    let spans = [max[0] - min[0], max[1] - min[1], max[2] - min[2]];
    if spans[1] > spans[0] && spans[1] >= spans[2] {
        AuxiliaryAxis::Y
    } else if spans[2] > spans[0] && spans[2] > spans[1] {
        AuxiliaryAxis::Z
    } else {
        AuxiliaryAxis::X
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
    pub(super) order: WitnessOrder,
    pub(super) swapped: bool,
}

/// Exact row-major order among candidate witnesses.
///
/// Each stored point and segment occupies at least 16 bytes, and each vector is
/// bounded by `isize::MAX` bytes. On the supported 64-bit targets, one source
/// count is below 2^59, one combined target-feature width is below 2^60, and
/// both directed products together remain below 2^120.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct WitnessOrder(u128);

impl WitnessOrder {
    pub(super) const ZERO: Self = Self(0);

    pub(super) fn from_rank(rank: u128) -> Self {
        Self(rank)
    }

    pub(super) fn ordinal(index: usize) -> Self {
        Self(index as u128)
    }

    pub(super) fn feature_width(segments: usize, point_only: usize) -> u128 {
        (segments as u128).wrapping_add(point_only as u128)
    }

    pub(super) fn row_major(base: Self, row: usize, width: u128, column: u128) -> Self {
        Self::after_rows(base, row, width).with_column(column)
    }

    pub(super) fn after_rows(base: Self, rows: usize, width: u128) -> Self {
        Self(base.0.wrapping_add((rows as u128).wrapping_mul(width)))
    }

    pub(super) fn with_column(self, column: u128) -> Self {
        Self(self.0.wrapping_add(column))
    }

    pub(super) fn point_feature(segments: usize, point: usize) -> u128 {
        (segments as u128).wrapping_add(point as u128)
    }
}

pub(super) fn witness_refine_limit(
    best: Option<&GeodesicWitnessCandidate>,
    order: WitnessOrder,
) -> f64 {
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
    use super::{WitnessOrder, geodesic_leaf_count};

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

    #[test]
    fn witness_order_preserves_wide_forward_and_reverse_ranks() {
        let width = usize::MAX as u128;
        let last_forward =
            WitnessOrder::row_major(WitnessOrder::ZERO, 2, width, width.wrapping_sub(1));
        let first_reverse = WitnessOrder::row_major(
            WitnessOrder::after_rows(WitnessOrder::ZERO, 3, width),
            0,
            width,
            0,
        );
        let same_forward =
            WitnessOrder::row_major(WitnessOrder::ZERO, 2, width, width.wrapping_sub(1));

        assert!(last_forward.0 > width);
        assert!(last_forward < first_reverse);
        assert_eq!(same_forward, last_forward);
    }
}
