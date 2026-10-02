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
use crate::geometry::distance::ChartEdgePart;
use crate::geometry::{
    BvhCode, BvhNode, ChartBox, ChartMetric, GeodesicMetric, GeodesicParts, GeodesicSegment,
    GeodesicSegmentQuery, GeodesicSegmentQueryResult, GeodesicSegmentScratch, GeodesicWitnessNeed,
    Point,
};

const BVH_LEAF_SIZE: usize = 8;

fn balanced_bvh_leaf_count(entry_count: usize) -> usize {
    if entry_count <= BVH_LEAF_SIZE {
        return 1;
    }
    let quotient = entry_count / BVH_LEAF_SIZE;
    let base = 1_usize << quotient.ilog2();
    // At `base * BVH_LEAF_SIZE` entries the balanced tree has `base` leaves;
    // it gains one leaf per entry until the next complete level. Builders
    // admit at most `LEAF_BIT / 2` entries, so the leaf and node counts fit.
    let complete_level = base.wrapping_mul(2);
    let partial_level = base
        .wrapping_add(entry_count)
        .wrapping_sub(BVH_LEAF_SIZE.wrapping_mul(base));
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
        let leaf_count = balanced_bvh_leaf_count(segments.len());
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
        if ids.len() <= BVH_LEAF_SIZE {
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
    pub(super) fn witness_point(
        &self,
        point: Point,
        parts: &GeodesicParts,
        metric: &impl GeodesicMetric,
        stack: &mut Vec<u32>,
        segment_scratch: &mut GeodesicSegmentScratch,
    ) -> Result<Option<GeodesicWitnessCandidate>> {
        let mut best: Option<GeodesicWitnessCandidate> = None;
        stack.clear();
        stack.push(0);
        while let Some(node) = stack.pop() {
            let node = node as usize;
            let best_distance = best
                .as_ref()
                .map_or(f64::INFINITY, |candidate| candidate.distance);
            if self.bound(point, node, metric)? > best_distance {
                continue;
            }
            match BvhCode::new(self.codes[node]).decode() {
                BvhNode::Leaf(leaf_index) => {
                    let leaf = self.leaves[leaf_index];
                    for edge_index in self.leaf_segment_ids(leaf) {
                        let edge_index = *edge_index as usize;
                        let segment = parts.segments[edge_index];
                        let order = WitnessOrder::ordinal(edge_index);
                        let best_distance = witness_refine_limit(best.as_ref(), order);
                        let bound = metric.point_distance_lower_bound(point, segment.start)?
                            - segment.length;
                        if bound > best_distance {
                            continue;
                        }
                        let result = metric.point_segment_query(
                            point,
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
                            target,
                            distance: minimum.distance,
                            lower: minimum.lower,
                            evaluation_error: minimum.evaluation_error,
                            feature: GeodesicTargetFeature::Segment {
                                index: edge_index,
                                along: minimum.along,
                            },
                            order,
                        };
                        if geodesic_witness_is_better(best.as_ref(), &candidate) {
                            best = Some(candidate);
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    self.push_nearer_last(point, first_child as u32, metric, stack)?;
                },
            }
        }
        if !parts.point_only.is_empty() {
            for (point_index, &target) in parts.point_only.iter().enumerate() {
                let order = WitnessOrder::from_rank(WitnessOrder::point_feature(
                    parts.segments.len(),
                    point_index,
                ));
                let best_distance = witness_refine_limit(best.as_ref(), order);
                if metric.point_distance_lower_bound(point, target)? > best_distance {
                    continue;
                }
                let distance = metric.segment_length(point, target);
                let evaluation_error = metric.geodesic_numerical_policy().evaluation_allowance(1.0);
                let candidate = GeodesicWitnessCandidate {
                    target,
                    distance,
                    lower: (distance - evaluation_error).max(0.0),
                    evaluation_error,
                    feature: GeodesicTargetFeature::Point,
                    order,
                };
                if geodesic_witness_is_better(best.as_ref(), &candidate) {
                    best = Some(candidate);
                }
            }
        }
        Ok(best)
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
    const fn coordinate(self, point: [f64; 3]) -> f64 {
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
    pub(super) target: Point,
    pub(super) distance: f64,
    pub(super) lower: f64,
    pub(super) evaluation_error: f64,
    pub(super) feature: GeodesicTargetFeature,
    pub(super) order: WitnessOrder,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct WitnessOrder(u128);

impl WitnessOrder {
    pub(super) const fn from_rank(rank: u128) -> Self {
        Self(rank)
    }

    pub(super) const fn ordinal(index: usize) -> Self {
        Self(index as u128)
    }

    pub(super) const fn point_feature(segments: usize, point: usize) -> u128 {
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

#[derive(Clone, Copy)]
struct ChartEdgeBuild {
    index: u32,
    center: [f64; 2],
}

#[derive(Clone, Copy)]
struct ChartEdgeLeaf {
    first: u32,
    len: u8,
}
crate::heapless!(ChartEdgeLeaf);

/// Flat BVH over the chart boxes of one prepared geometry.
pub(crate) struct ChartEdgeBvh {
    boxes: Box<[ChartBox]>,
    codes: Box<[u32]>,
    leaves: Box<[ChartEdgeLeaf]>,
    edge_ids: Box<[u32]>,
}

impl ChartEdgeBvh {
    pub(crate) fn build(edges: &[ChartEdgePart]) -> Option<Self> {
        let edge_count = u32::try_from(edges.len()).ok()?;
        if edge_count == 0 || edge_count > BvhCode::LEAF_BIT / 2 {
            return None;
        }
        let leaf_count = balanced_bvh_leaf_count(edges.len());
        let node_count = leaf_count.checked_mul(2)?.checked_sub(1)?;
        let world_box = ChartBox {
            west: -180.0,
            south: -90.0,
            east: 180.0,
            north: 90.0,
        };
        let mut tree = Self {
            boxes: vec![world_box; node_count].into_boxed_slice(),
            codes: vec![0; node_count].into_boxed_slice(),
            leaves: vec![ChartEdgeLeaf { first: 0, len: 0 }; leaf_count].into_boxed_slice(),
            edge_ids: vec![0; edges.len()].into_boxed_slice(),
        };
        let mut entries = edges
            .iter()
            .enumerate()
            .map(|(index, edge)| ChartEdgeBuild {
                index: index as u32,
                center: chart_box_center(edge.bounds),
            })
            .collect::<Vec<_>>();
        let mut next_node = 1;
        let mut next_leaf = 0;
        let mut next_edge = 0;
        tree.fill_node(
            0,
            &mut next_node,
            &mut next_leaf,
            &mut next_edge,
            edges,
            &mut entries,
        );
        debug_assert_eq!(next_node, node_count);
        debug_assert_eq!(next_leaf, leaf_count);
        debug_assert_eq!(next_edge, edges.len());
        Some(tree)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node, leaf, and edge indexes are generated together within the admitted u32-sized chart BVH"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the admitted node count bounds adjacent child slots and cursor increments"
    )]
    fn fill_node(
        &mut self,
        node: usize,
        next_node: &mut usize,
        next_leaf: &mut usize,
        next_edge: &mut usize,
        edges: &[ChartEdgePart],
        entries: &mut [ChartEdgeBuild],
    ) {
        self.boxes[node] = chart_box_union(
            entries
                .iter()
                .map(|entry| edges[entry.index as usize].bounds),
        );
        if entries.len() <= BVH_LEAF_SIZE {
            entries.sort_unstable_by_key(|entry| entry.index);
            let first = *next_edge as u32;
            for entry in entries.iter() {
                self.edge_ids[*next_edge] = entry.index;
                *next_edge = (*next_edge).wrapping_add(1);
            }
            let leaf_index = *next_leaf as u32;
            self.leaves[*next_leaf] = ChartEdgeLeaf {
                first,
                len: entries.len() as u8,
            };
            *next_leaf = (*next_leaf).wrapping_add(1);
            self.codes[node] = BvhCode::leaf(leaf_index as usize).raw();
            return;
        }

        let axis = chart_split_axis(self.boxes[node]);
        let middle = entries.len() / 2;
        entries.select_nth_unstable_by(middle, |left, right| {
            left.center[axis]
                .total_cmp(&right.center[axis])
                .then_with(|| left.index.cmp(&right.index))
        });
        let (lower, upper) = entries.split_at_mut(middle);
        let first_child = *next_node;
        *next_node = (*next_node).wrapping_add(2);
        self.codes[node] = BvhCode::internal(first_child as u32).raw();
        self.fill_node(first_child, next_node, next_leaf, next_edge, edges, lower);
        self.fill_node(
            first_child + 1,
            next_node,
            next_leaf,
            next_edge,
            edges,
            upper,
        );
    }

    /// Visit candidate edges from the nearer chart boxes first. `limit` is
    /// inclusive for threshold queries and an improving upper bound for
    /// minima; the visitor may lower `best` or stop with a value.
    #[expect(
        clippy::indexing_slicing,
        reason = "Traversal IDs and leaf ranges are encoded by this immutable chart BVH builder"
    )]
    pub(crate) fn visit_point_candidates<T>(
        &self,
        edges: &[ChartEdgePart],
        point: Point,
        metric: &impl ChartMetric,
        best: &mut f64,
        limit: Option<f64>,
        stack: &mut Vec<(u32, f64)>,
        mut visit: impl FnMut(usize, f64, &mut f64) -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        stack.clear();
        let bound = metric.point_envelope_bound(point);
        let root_bound = bound(self.boxes[0]);
        if chart_bound_prunes(root_bound, *best, limit) {
            return Ok(None);
        }
        stack.push((0, root_bound));
        while let Some((node, lower_bound)) = stack.pop() {
            if chart_bound_prunes(lower_bound, *best, limit) {
                continue;
            }
            match BvhCode::new(self.codes[node as usize]).decode() {
                BvhNode::Leaf(leaf_index) => {
                    let leaf = self.leaves[leaf_index];
                    let Some(edge_ids) = self
                        .edge_ids
                        .get(leaf.first as usize..)
                        .and_then(|remaining| remaining.get(..leaf.len as usize))
                    else {
                        unreachable!("chart BVH leaf range belongs to its edge-id table")
                    };
                    for &edge_id in edge_ids {
                        let edge_index = edge_id as usize;
                        let edge_bound = bound(edges[edge_index].bounds);
                        if chart_bound_prunes(edge_bound, *best, limit) {
                            continue;
                        }
                        if let Some(result) = visit(edge_index, edge_bound, best)? {
                            return Ok(Some(result));
                        }
                    }
                },
                BvhNode::Internal(first_child) => {
                    let lower_child = first_child as u32;
                    let upper_child = lower_child.wrapping_add(1);
                    let lower_bound = bound(self.boxes[lower_child as usize]);
                    let upper_bound = bound(self.boxes[upper_child as usize]);
                    let lower_admitted = !chart_bound_prunes(lower_bound, *best, limit);
                    let upper_admitted = !chart_bound_prunes(upper_bound, *best, limit);
                    match (lower_admitted, upper_admitted) {
                        (true, true) if lower_bound <= upper_bound => {
                            stack.push((upper_child, upper_bound));
                            stack.push((lower_child, lower_bound));
                        },
                        (true, true) => {
                            stack.push((lower_child, lower_bound));
                            stack.push((upper_child, upper_bound));
                        },
                        (true, false) => stack.push((lower_child, lower_bound)),
                        (false, true) => stack.push((upper_child, upper_bound)),
                        (false, false) => {},
                    }
                },
            }
        }
        Ok(None)
    }
}

impl HeapSize for ChartEdgeBvh {
    fn heap_bytes(&self) -> usize {
        self.boxes
            .heap_bytes()
            .saturating_add(self.codes.heap_bytes())
            .saturating_add(self.leaves.heap_bytes())
            .saturating_add(self.edge_ids.heap_bytes())
    }
}

fn chart_bound_prunes(lower_bound: f64, best: f64, limit: Option<f64>) -> bool {
    limit.map_or_else(|| lower_bound > best.next_up(), |limit| lower_bound > limit)
}

#[expect(
    clippy::float_cmp,
    reason = "the exact (-180, 180) endpoints encode the canonical full-longitude sentinel"
)]
fn chart_box_union(boxes: impl Iterator<Item = ChartBox>) -> ChartBox {
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    let mut west = 180.0_f64;
    let mut east = -180.0_f64;
    let mut full_longitude = false;
    for bounds in boxes {
        south = south.min(bounds.south);
        north = north.max(bounds.north);
        if bounds.west > bounds.east || (bounds.west == -180.0 && bounds.east == 180.0) {
            full_longitude = true;
        } else {
            west = west.min(bounds.west);
            east = east.max(bounds.east);
        }
    }
    if full_longitude {
        ChartBox {
            west: -180.0,
            south,
            east: 180.0,
            north,
        }
    } else {
        ChartBox {
            west,
            south,
            east,
            north,
        }
    }
}

#[expect(
    clippy::float_cmp,
    reason = "the exact (-180, 180) endpoints encode the canonical full-longitude sentinel"
)]
fn chart_box_center(bounds: ChartBox) -> [f64; 2] {
    let longitude = if bounds.west == -180.0 && bounds.east == 180.0 {
        0.0
    } else if bounds.west <= bounds.east {
        f64::midpoint(bounds.west, bounds.east)
    } else {
        let sweep = (180.0 - bounds.west) + (bounds.east + 180.0);
        let unwrapped = bounds.west + sweep / 2.0;
        if unwrapped > 180.0 {
            unwrapped - 360.0
        } else {
            unwrapped
        }
    };
    [longitude, f64::midpoint(bounds.south, bounds.north)]
}

#[expect(
    clippy::float_cmp,
    reason = "the exact (-180, 180) endpoints encode the canonical full-longitude sentinel"
)]
fn chart_longitude_span(bounds: ChartBox) -> f64 {
    if bounds.west == -180.0 && bounds.east == 180.0 {
        360.0
    } else if bounds.west <= bounds.east {
        bounds.east - bounds.west
    } else {
        (180.0 - bounds.west) + (bounds.east + 180.0)
    }
}

fn chart_split_axis(bounds: ChartBox) -> usize {
    usize::from(chart_longitude_span(bounds) < bounds.north - bounds.south)
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
    use super::balanced_bvh_leaf_count;

    #[test]
    fn balanced_leaf_count_matches_split_boundaries() {
        for (segment_count, expected_leaves) in [
            (1, 1),
            (8, 1),
            (9, 2),
            (16, 2),
            (17, 3),
            (18, 4),
            (23, 4),
            (32, 4),
            (33, 5),
            (34, 6),
            (35, 7),
            (36, 8),
            (64, 8),
            (65, 9),
            (71, 15),
            (72, 16),
            (95, 16),
            (128, 16),
            (129, 17),
            (144, 32),
            (256, 32),
            (257, 33),
            (512, 64),
            (513, 65),
        ] {
            let leaves = balanced_bvh_leaf_count(segment_count);
            assert_eq!(leaves, expected_leaves, "segments={segment_count}");
        }
    }
}
