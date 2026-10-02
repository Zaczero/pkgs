use super::str_tree::{StaticStrTree, StrNode};
use crate::geometry::PreparedPlan;
use crate::py::index::{
    AABB, BinaryHeap, DistanceUnit, Frontier, FrontierNode, NearestCandidate, NearestOptions,
    NonNegative, PyGeometry, PyResult, PySpatialIndex, QueryDomain, Shape, aabb_box_distance_2,
    crs, index_envelopes, nearest_candidates_from_heap, pair_distance_geodesic,
    pair_distance_planar, push_nearest_candidate, resolve_metric, shape_pruner_point,
};

#[derive(Default)]
pub(crate) struct NearestVisited {
    values: Vec<usize>,
    touched: Vec<usize>,
    sparse: crate::collections::IndexMap<usize, usize>,
    unique: bool,
}

impl NearestVisited {
    fn reset(&mut self, len: usize, live_count: usize, entry_count: usize) {
        for idx in self.touched.drain(..) {
            *self
                .values
                .get_mut(idx)
                .expect("touched bitmap word remains allocated until reset") = 0;
        }
        let words = len.div_ceil(usize::BITS as usize);
        self.sparse.clear();
        self.unique = entry_count == live_count;
        // Dense bitmap initialization must be bounded by the live problem,
        // rather than the highest stable handle after a deletion history.
        self.values.resize(
            if !self.unique && words <= live_count {
                words
            } else {
                0
            },
            0,
        );
    }

    fn visit(&mut self, idx: usize) -> bool {
        // Every live row owns at least one cover entry. Equal row and entry
        // counts therefore prove that traversal cannot revisit a row alias.
        if self.unique {
            return true;
        }
        let word = idx.div_euclid(usize::BITS as usize);
        let shift = u32::try_from(idx.rem_euclid(usize::BITS as usize))
            .expect("bitmap shift is less than usize::BITS");
        let bit = 1_usize.wrapping_shl(shift);
        if self.values.is_empty() {
            let value = self.sparse.entry(word).or_default();
            let first = *value & bit == 0;
            *value |= bit;
            return first;
        }
        let word_value = self
            .values
            .get_mut(word)
            .expect("nearest handle belongs to the reset bitmap");
        if *word_value & bit != 0 {
            return false;
        }
        if *word_value == 0 {
            self.touched.push(word);
        }
        *word_value |= bit;
        true
    }
}

pub(super) trait NearestState {
    fn ceiling(&self, max_distance: Option<NonNegative>) -> Option<f64>;
    fn accept(&mut self, candidate: NearestCandidate);
    fn finish(self) -> Vec<NearestCandidate>;
}

pub(super) struct HeapNearest {
    nearest: BinaryHeap<NearestCandidate>,
    k: usize,
}

impl HeapNearest {
    pub(super) fn new(k: usize, rows: usize) -> Self {
        let nearest = BinaryHeap::with_capacity(k.min(rows));
        Self { nearest, k }
    }
}

impl NearestState for HeapNearest {
    fn ceiling(&self, max_distance: Option<NonNegative>) -> Option<f64> {
        let nearest = (self.nearest.len() == self.k)
            .then(|| self.nearest.peek().expect("full nearest heap").distance);
        combine_nearest_ceiling(nearest, max_distance)
    }

    fn accept(&mut self, candidate: NearestCandidate) {
        push_nearest_candidate(&mut self.nearest, self.k, candidate);
    }

    fn finish(self) -> Vec<NearestCandidate> {
        nearest_candidates_from_heap(self.nearest)
    }
}

#[derive(Default)]
pub(super) struct OneNearest {
    best: Option<NearestCandidate>,
}

impl NearestState for OneNearest {
    fn ceiling(&self, max_distance: Option<NonNegative>) -> Option<f64> {
        combine_nearest_ceiling(
            self.best.as_ref().map(|candidate| candidate.distance),
            max_distance,
        )
    }

    fn accept(&mut self, candidate: NearestCandidate) {
        if self
            .best
            .as_ref()
            .is_none_or(|current| candidate.cmp(current).is_lt())
        {
            self.best = Some(candidate);
        }
    }

    fn finish(self) -> Vec<NearestCandidate> {
        self.best.into_iter().collect()
    }
}

const fn combine_nearest_ceiling(
    nearest: Option<f64>,
    max_distance: Option<NonNegative>,
) -> Option<f64> {
    match (nearest, max_distance) {
        (Some(a), Some(b)) => Some(a.min(b.get())),
        (Some(a), None) => Some(a),
        (None, Some(a)) => Some(a.get()),
        (None, None) => None,
    }
}

pub(crate) struct LeafFrontierEntry {
    bound: f64,
    idx: usize,
}

struct HeapLeafPolicy<'a> {
    leaf_frontier: &'a mut Vec<LeafFrontierEntry>,
}

impl HeapLeafPolicy<'_> {
    fn visit_leaf<B>(
        &mut self,
        tree: &StaticStrTree,
        live: &[u8],
        node: &StrNode,
        ceiling: Option<f64>,
        bound: &B,
        frontier: &mut BinaryHeap<FrontierNode>,
    ) where
        B: Fn(&AABB<[f64; 2]>) -> f64,
    {
        self.leaf_frontier.clear();
        self.leaf_frontier.reserve(tree.leaf_entries(node).len());
        for entry in tree.leaf_entries(node).iter().filter(|entry| {
            *live
                .get(entry.idx)
                .expect("STR entry handle belongs to the index row table")
                != 0
        }) {
            self.leaf_frontier.push(LeafFrontierEntry {
                bound: bound(&entry.envelope),
                idx: entry.idx,
            });
        }
        for entry in self.leaf_frontier.drain(..) {
            if ceiling.is_some_and(|limit| entry.bound > limit) {
                // STR storage order is not bound order; do not discard later
                // entries in this leaf.
                continue;
            }
            frontier.push(FrontierNode {
                bound: entry.bound,
                node: Frontier::Entry(entry.idx),
            });
        }
    }
}

impl PySpatialIndex {
    pub(crate) fn nearest_one(
        &self,
        geometry: &PyGeometry,
        k: usize,
        unit: Option<DistanceUnit>,
        max_distance: Option<NonNegative>,
        options: NearestOptions,
    ) -> PyResult<Vec<NearestCandidate>> {
        self.ensure_query_compatible(geometry, "spatial index nearest")?;
        let metric = resolve_metric(
            self.metric_crs_str(geometry.crs_str()),
            unit,
            crate::dispatch::MetricOp::Nearest,
        )?;
        let query_plan = geometry.prepared_plan();
        let mut frontier = BinaryHeap::new();
        let mut leaf_frontier = Vec::new();
        let mut visited = NearestVisited::default();
        let resolved = crs::ResolvedMetric::from_model(&metric)?;
        let query_domain = if k > 0 {
            QueryDomain::for_query_frame(self, Some(&geometry.frame))?
        } else {
            QueryDomain::default()
        };
        query_domain.admit_shape(query_plan.shape(), query_plan.bounds(), None)?;
        let mut query_cache = || geometry.frame_cache();
        match &resolved {
            crs::ResolvedMetric::Planar { to_metre } => self.planar_nearest_core_ties(
                to_metre.get(),
                &query_plan,
                k,
                max_distance,
                options,
                &mut frontier,
                &mut leaf_frontier,
                &mut visited,
            ),
            crs::ResolvedMetric::Geodesic(geodesic) => self.geodesic_nearest_core_ties(
                &metric,
                geodesic,
                &query_plan,
                &mut query_cache,
                k,
                max_distance,
                options,
                &mut frontier,
                &mut leaf_frontier,
                &mut visited,
            ),
        }
    }

    pub(crate) fn planar_nearest_core_ties(
        &self,
        to_metre: f64,
        query: &PreparedPlan,
        k: usize,
        max_distance: Option<NonNegative>,
        options: NearestOptions,
        frontier: &mut BinaryHeap<FrontierNode>,
        leaf_frontier: &mut Vec<LeafFrontierEntry>,
        visited: &mut NearestVisited,
    ) -> PyResult<Vec<NearestCandidate>> {
        let candidates = self.planar_nearest_core(
            to_metre,
            query,
            k,
            max_distance,
            options.exclude_equal,
            frontier,
            leaf_frontier,
            visited,
        )?;
        if !options.include_ties || k == 0 || candidates.len() < k {
            return Ok(candidates);
        }
        let kth = candidates.last().expect("len >= k >= 1").distance;
        self.planar_nearest_core(
            to_metre,
            query,
            usize::MAX,
            Some(NonNegative::try_new("max_distance", kth)?),
            options.exclude_equal,
            frontier,
            leaf_frontier,
            visited,
        )
    }

    fn planar_nearest_core(
        &self,
        to_metre: f64,
        query: &PreparedPlan,
        k: usize,
        max_distance: Option<NonNegative>,
        exclusive: bool,
        frontier: &mut BinaryHeap<FrontierNode>,
        leaf_frontier: &mut Vec<LeafFrontierEntry>,
        visited: &mut NearestVisited,
    ) -> PyResult<Vec<NearestCandidate>> {
        let Some(query_bounds) = query.bounds().filter(|_| k > 0) else {
            return Ok(Vec::new());
        };
        let exclude = exclusive.then_some(query.shape());
        let Some(frame) = self.metadata.as_ref() else {
            return Ok(Vec::new());
        };
        // A part cover only tightens a lower bound: the query lies inside its
        // parts, so the nearest of them is no further than the whole box.
        let domain = crate::geometry::topology_domain(frame)?;
        let cover = index_envelopes(query.shape(), query_bounds, domain)?;
        let query_envelopes = cover.entries();
        let mut rows = self.rows.operation();
        let bound = |envelope: &AABB<[f64; 2]>| {
            query_envelopes
                .iter()
                .map(|query| aabb_box_distance_2(query, envelope))
                .fold(f64::INFINITY, f64::min)
                .sqrt()
                * to_metre
        };
        let exact = |idx| {
            if let Some(exclude) = exclude
                && rows.row(idx)?.with_shape(|other| *other == *exclude)?
            {
                return Ok(None);
            }
            let data = rows.prepared(idx)?;
            Ok(Some(pair_distance_planar(to_metre, &data, query)?))
        };
        if k == 1 {
            return self.bound_ordered_nearest_shared(
                bound,
                exact,
                OneNearest::default(),
                max_distance,
                frontier,
                leaf_frontier,
                visited,
            );
        }
        self.bound_ordered_nearest(
            bound,
            exact,
            k,
            max_distance,
            frontier,
            leaf_frontier,
            visited,
        )
    }

    /// `nearest_one` past the per-operation setup (frame check, metric) —
    /// the per-query engine the packed query lane drives with one shared
    /// metric resolution.
    /// [`nearest_core`](Self::nearest_core) with the all-ties extension:
    /// when `ties` and the k-th slot was filled, a second pass with the
    /// k-th distance as the ceiling (and no k cap) returns EVERY row tying
    /// it under exact comparison — shapely `all_matches` / geopandas
    /// `return_all` parity, opt-in.
    ///
    /// `frontier` is a batch-local spatial-index work heap: cleared and reused across
    /// array query rows (free-threading: never a receiver cache or lock).
    /// `leaf_frontier` is bounded scratch for one static leaf and is likewise
    /// reused across rows.
    pub(crate) fn geodesic_nearest_core_ties(
        &self,
        metric: &crs::MetricModel,
        resolved: &crs::GeodesicMetric,
        query: &PreparedPlan,
        query_cache: &mut impl FnMut() -> std::sync::Arc<crate::geometry::FrameDependentCaches>,
        k: usize,
        max_distance: Option<NonNegative>,
        options: NearestOptions,
        frontier: &mut BinaryHeap<FrontierNode>,
        leaf_frontier: &mut Vec<LeafFrontierEntry>,
        visited: &mut NearestVisited,
    ) -> PyResult<Vec<NearestCandidate>> {
        let candidates = self.geodesic_nearest_core(
            metric,
            resolved,
            query,
            query_cache,
            k,
            max_distance,
            options.exclude_equal,
            frontier,
            leaf_frontier,
            visited,
        )?;
        if !options.include_ties || k == 0 || candidates.len() < k {
            // Nothing was dropped at the k boundary — no ties to recover.
            return Ok(candidates);
        }
        let kth = candidates.last().expect("len >= k >= 1").distance;
        self.geodesic_nearest_core(
            metric,
            resolved,
            query,
            query_cache,
            usize::MAX,
            Some(NonNegative::try_new("max_distance", kth)?),
            options.exclude_equal,
            frontier,
            leaf_frontier,
            visited,
        )
    }

    pub(crate) fn geodesic_nearest_core(
        &self,
        metric: &crs::MetricModel,
        resolved: &crs::GeodesicMetric,
        query: &PreparedPlan,
        query_cache: &mut impl FnMut() -> std::sync::Arc<crate::geometry::FrameDependentCaches>,
        k: usize,
        max_distance: Option<NonNegative>,
        exclusive: bool,
        frontier: &mut BinaryHeap<FrontierNode>,
        leaf_frontier: &mut Vec<LeafFrontierEntry>,
        visited: &mut NearestVisited,
    ) -> PyResult<Vec<NearestCandidate>> {
        let Some(_query_bounds) = query.bounds().filter(|_| k > 0) else {
            return Ok(Vec::new());
        };
        let exclude = exclusive.then_some(query.shape());
        if let Some(pruner) = self.geodesic_pruner(metric, shape_pruner_point(query.shape())) {
            let query_cache_owner = query_cache();
            let mut rows = self.rows.operation();
            // Geodesic point query over an all-point index: prune with the
            // sound lower bound instead of Karney-evaluating every entry.
            let bound = |envelope: &AABB<[f64; 2]>| pruner.envelope_lower_bound(envelope);
            let exact = |idx| {
                if let Some(exclude) = exclude
                    && rows.row(idx)?.with_shape(|other| *other == *exclude)?
                {
                    return Ok(None);
                }
                let data = rows.prepared(idx)?;
                Ok(Some(pair_distance_geodesic(
                    resolved,
                    &data,
                    &*rows.frame_cache(idx)?,
                    query,
                    &query_cache_owner,
                )?))
            };
            if k == 1 {
                return self.bound_ordered_nearest_shared(
                    bound,
                    exact,
                    OneNearest::default(),
                    max_distance,
                    frontier,
                    leaf_frontier,
                    visited,
                );
            }
            return self.bound_ordered_nearest(
                bound,
                exact,
                k,
                max_distance,
                frontier,
                leaf_frontier,
                visited,
            );
        }
        // Geodesic metric without a sound envelope bound (non-point shapes
        // realize edge points outside their planar envelope): cap-ordered
        // scan when the per-row cap table holds, exact full scan otherwise.
        if let Some(result) =
            self.geodesic_capped_nearest(resolved, query, query_cache, k, max_distance, exclude)?
        {
            return Ok(result);
        }
        if k == 1 {
            return self.geodesic_full_scan_shared(
                OneNearest::default(),
                max_distance,
                exclude,
                visited,
                query_cache,
                resolved,
                query,
            );
        }
        let state = HeapNearest::new(k, self.__len__());
        self.geodesic_full_scan_shared(
            state,
            max_distance,
            exclude,
            visited,
            query_cache,
            resolved,
            query,
        )
    }

    fn geodesic_full_scan_shared<S: NearestState>(
        &self,
        mut state: S,
        max_distance: Option<NonNegative>,
        exclude: Option<&Shape>,
        visited: &mut NearestVisited,
        query_cache: &mut impl FnMut() -> std::sync::Arc<crate::geometry::FrameDependentCaches>,
        resolved: &crs::GeodesicMetric,
        query: &PreparedPlan,
    ) -> PyResult<Vec<NearestCandidate>> {
        self.reset_nearest_visited(visited);
        let query_cache_owner = query_cache();
        let mut rows = self.rows.operation();
        let mut exact = |idx| {
            if let Some(exclude) = exclude
                && rows.row(idx)?.with_shape(|other| *other == *exclude)?
            {
                return Ok(None);
            }
            let data = rows.prepared(idx)?;
            Ok(Some(pair_distance_geodesic(
                resolved,
                &data,
                &*rows.frame_cache(idx)?,
                query,
                &query_cache_owner,
            )?))
        };
        for entry in self.live_entries() {
            if !visited.visit(entry.idx) {
                continue;
            }
            self.consider_nearest_candidate(entry.idx, &mut exact, max_distance, &mut state)?;
        }
        Ok(state.finish())
    }

    /// Best-first nearest traversal over the bulk and immutable-run nodes:
    /// pop envelopes by ascending lower bound, descend parents,
    /// exact-evaluate leaves, and stop once the lower bound exceeds the
    /// relevant ceiling (the k-th best so far, or `max_distance`). Yields the
    /// identical k-nearest as a full scan for any `bound` that never exceeds
    /// the true distance.
    ///
    /// `frontier` is cleared and reused across rows on the array path — batch
    /// local only (no receiver cache, no lock).
    pub(crate) fn bound_ordered_nearest(
        &self,
        bound: impl Fn(&AABB<[f64; 2]>) -> f64,
        exact: impl FnMut(usize) -> PyResult<Option<f64>>,
        k: usize,
        max_distance: Option<NonNegative>,
        frontier: &mut BinaryHeap<FrontierNode>,
        leaf_frontier: &mut Vec<LeafFrontierEntry>,
        visited: &mut NearestVisited,
    ) -> PyResult<Vec<NearestCandidate>> {
        self.bound_ordered_nearest_shared(
            bound,
            exact,
            HeapNearest::new(k, self.__len__()),
            max_distance,
            frontier,
            leaf_frontier,
            visited,
        )
    }

    fn bound_ordered_nearest_shared<S: NearestState>(
        &self,
        bound: impl Fn(&AABB<[f64; 2]>) -> f64,
        mut exact: impl FnMut(usize) -> PyResult<Option<f64>>,
        mut state: S,
        max_distance: Option<NonNegative>,
        frontier: &mut BinaryHeap<FrontierNode>,
        leaf_frontier: &mut Vec<LeafFrontierEntry>,
        visited: &mut NearestVisited,
    ) -> PyResult<Vec<NearestCandidate>> {
        let mut leaf_policy = HeapLeafPolicy { leaf_frontier };
        self.reset_nearest_visited(visited);
        frontier.clear();
        let root_level = self.bulk.root_level();
        let root_count = self
            .bulk
            .roots()
            .len()
            .wrapping_add(self.overflow.root_count());
        frontier.reserve(root_count);
        for (index, root) in self.bulk.roots().iter().enumerate() {
            frontier.push(FrontierNode {
                bound: bound(&root.envelope),
                node: Frontier::Bulk(root_level, index as u32),
            });
        }
        for (run, root_level, index, root) in self.overflow.roots() {
            frontier.push(FrontierNode {
                bound: bound(&root.envelope),
                node: Frontier::Run(run, root_level, index),
            });
        }
        while let Some(node) = frontier.pop() {
            let ceiling = state.ceiling(max_distance);
            if ceiling.is_some_and(|limit| node.bound > limit) {
                break;
            }
            match node.node {
                Frontier::Bulk(level, index) => {
                    let bulk_node = self.bulk.node(level, index);
                    if level == 0 {
                        leaf_policy.visit_leaf(
                            &self.bulk,
                            &self.rows.live,
                            bulk_node,
                            ceiling,
                            &bound,
                            frontier,
                        );
                    } else {
                        let child_level = level.wrapping_sub(1);
                        for child in bulk_node.children.clone() {
                            frontier.push(FrontierNode {
                                bound: bound(&self.bulk.node(child_level, child).envelope),
                                node: Frontier::Bulk(child_level, child),
                            });
                        }
                    }
                },
                Frontier::Run(run, level, index) => {
                    let tree = self.overflow.run(run);
                    let run_node = tree.node(level, index);
                    if level == 0 {
                        leaf_policy.visit_leaf(
                            tree,
                            &self.rows.live,
                            run_node,
                            ceiling,
                            &bound,
                            frontier,
                        );
                    } else {
                        let child_level = level.wrapping_sub(1);
                        for child in run_node.children.clone() {
                            let child_node = tree.node(child_level, child);
                            frontier.push(FrontierNode {
                                bound: bound(&child_node.envelope),
                                node: Frontier::Run(run, child_level, child),
                            });
                        }
                    }
                },
                Frontier::Entry(entry_idx) => {
                    if !visited.visit(entry_idx) {
                        continue;
                    }
                    self.consider_nearest_candidate(
                        entry_idx,
                        &mut exact,
                        max_distance,
                        &mut state,
                    )?;
                },
            }
        }
        Ok(state.finish())
    }

    pub(super) fn consider_nearest_candidate<S: NearestState>(
        &self,
        entry_idx: usize,
        exact: &mut impl FnMut(usize) -> PyResult<Option<f64>>,
        max_distance: Option<NonNegative>,
        state: &mut S,
    ) -> PyResult<()> {
        let Some(distance) = exact(entry_idx)? else {
            return Ok(());
        };
        if max_distance.is_some_and(|max| distance > max.get()) {
            return Ok(());
        }
        state.accept(NearestCandidate::new(distance, entry_idx));
        Ok(())
    }

    fn reset_nearest_visited(&self, visited: &mut NearestVisited) {
        let entries = self
            .bulk
            .entry_count()
            .wrapping_sub(self.bulk_dead_entries)
            .wrapping_add(self.overflow.live_entry_count());
        visited.reset(self.rows.len(), self.rows.live_count(), entries);
    }
}

#[cfg(test)]
mod tests {

    use pyo3::Python;

    use super::{BinaryHeap, NearestOptions, NearestState as _, NearestVisited, OneNearest};
    use crate::crs::{ResolvedMetric, metric_model};
    use crate::geometry::{PreparedPlan, ShapeData};
    use crate::pair_distance_planar;
    use crate::py::index::{
        Frame, NearestCandidate, Point, PyGeometry, Shape, parse_max_distance, spatial_index,
    };
    use crate::test_support::allocation_count;

    #[test]
    fn sparse_nearest_visits_do_not_allocate_historical_bitmap_words() {
        let mut visited = NearestVisited::default();
        crate::test_support::assert_no_allocations(|| visited.reset(1 << 20, 16, 32));
        for handle in [0, 63, 64, (1 << 20) - 1] {
            assert!(visited.visit(handle));
            assert!(!visited.visit(handle));
        }
        visited.reset(1 << 20, 16, 32);
        assert!(visited.visit((1 << 20) - 1));
        visited.reset(128, 128, 256);
        assert!(visited.visit(64));
    }
    #[test]
    fn visited_reset_preserves_unique_visits_across_word_boundaries_and_sizes() {
        let mut visited = NearestVisited::default();
        for len in [129, 1, 0, 65, 257, 63] {
            visited.reset(len, len, len * 2);
            for row in (0..len).rev() {
                assert!(visited.visit(row));
                assert!(!visited.visit(row));
            }
            visited.reset(len, len, len * 2);
            for row in (0..len).step_by(7) {
                assert!(visited.visit(row));
            }
        }
    }

    #[test]
    fn nearest_max_distance_rejects_nan_at_parser() {
        Python::initialize();
        parse_max_distance(Some(f64::NAN)).unwrap_err();
    }

    #[test]
    fn one_nearest_empty_finish_does_not_allocate() {
        let (result, allocations) = allocation_count(|| OneNearest::default().finish());
        assert!(result.is_empty());
        assert_eq!(allocations, 0);
    }

    #[test]
    fn overflow_run_nearest_frontier_matches_exhaustive_distances() {
        Python::initialize();
        Python::attach(|_py| {
            let mut index = spatial_index(Vec::new()).expect("empty index");
            for handle in 0..128 {
                let geometry = PyGeometry::with_frame(
                    ShapeData::new(Shape::Point(Point::new_raw_xy(handle as f64, 0.0))),
                    Frame::None,
                );
                assert_eq!(index.insert_one(&geometry).expect("insert"), handle);
            }
            let query =
                PreparedPlan::new(ShapeData::new(Shape::Point(Point::new_raw_xy(63.25, 0.0))));
            let model = metric_model(None).expect("planar metric");
            let ResolvedMetric::Planar { to_metre } =
                ResolvedMetric::from_model(&model).expect("resolved planar metric")
            else {
                panic!("CRS-free metric must be planar");
            };
            let execute = || {
                let mut frontier = BinaryHeap::new();
                let mut leaf_frontier = Vec::new();
                let mut visited = NearestVisited::default();
                index.planar_nearest_core_ties(
                    to_metre.get(),
                    &query,
                    8,
                    None,
                    NearestOptions {
                        exclude_equal: false,
                        include_ties: false,
                    },
                    &mut frontier,
                    &mut leaf_frontier,
                    &mut visited,
                )
            };
            let mut rows = index.rows.operation();
            let mut expected: Vec<_> = index
                .live_handles_sorted()
                .into_iter()
                .map(|handle| {
                    let data = rows.prepared(handle).expect("prepared row");
                    NearestCandidate::new(
                        pair_distance_planar(to_metre.get(), &data, &query)
                            .expect("planar distance"),
                        handle,
                    )
                })
                .collect();
            expected.sort_unstable();
            expected.truncate(8);
            assert_eq!(execute().expect("nearest"), expected);
        });
    }
}
