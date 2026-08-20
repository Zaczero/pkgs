use crate::py::index::{
    AABB, BinaryHeap, DistanceUnit, Frontier, FrontierNode, NearestCandidate, NearestOptions,
    NonNegative, PyGeometry, PyResult, PySpatialIndex, RTreeNode, Shape, ShapeData,
    aabb_box_distance_2, crs, index_envelope, nearest_candidates_from_heap, pair_distance_resolved,
    push_nearest_candidate, resolve_metric, shape_pruner_point,
};

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
        let metric = resolve_metric(self.metric_crs_str(geometry.crs_str()), unit, "nearest")?;
        let mut frontier = BinaryHeap::new();
        self.nearest_core_ties(
            &metric,
            &geometry.shape,
            &geometry.frame_cache,
            k,
            max_distance,
            options,
            &mut frontier,
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
    /// `frontier` is a batch-local R-tree work heap: cleared and reused across
    /// array query rows (free-threading: never a receiver cache or lock).
    pub(crate) fn nearest_core_ties<'a>(
        &'a self,
        metric: &crs::MetricModel,
        query: &ShapeData,
        query_cache: &crate::geometry::FrameDependentCaches,
        k: usize,
        max_distance: Option<NonNegative>,
        options: NearestOptions,
        frontier: &mut BinaryHeap<FrontierNode<'a>>,
    ) -> PyResult<Vec<NearestCandidate>> {
        let candidates = self.nearest_core(
            metric,
            query,
            query_cache,
            k,
            max_distance,
            options.exclude_equal,
            frontier,
        )?;
        if !options.include_ties || k == 0 || candidates.len() < k {
            // Nothing was dropped at the k boundary — no ties to recover.
            return Ok(candidates);
        }
        let kth = candidates.last().expect("len >= k >= 1").distance;
        self.nearest_core(
            metric,
            query,
            query_cache,
            usize::MAX,
            Some(NonNegative::try_new("max_distance", kth)?),
            options.exclude_equal,
            frontier,
        )
    }

    pub(crate) fn nearest_core<'a>(
        &'a self,
        metric: &crs::MetricModel,
        query: &ShapeData,
        query_cache: &crate::geometry::FrameDependentCaches,
        k: usize,
        max_distance: Option<NonNegative>,
        exclusive: bool,
        frontier: &mut BinaryHeap<FrontierNode<'a>>,
    ) -> PyResult<Vec<NearestCandidate>> {
        let Some(query_bounds) = query.bounds().filter(|_| k > 0) else {
            return Ok(Vec::new());
        };
        let exclude = exclusive.then_some(query.shape());
        // Resolve the geodesic ellipsoid ONCE for the whole query (the per-row
        // distance kernel re-resolved the CRS for every candidate).
        let resolved = crs::ResolvedMetric::from_model(metric)?;
        if metric.has_coordinate_order() {
            // Planar metrics: the envelope box distance is a valid lower bound
            // in coordinate units (scaled to meters where needed), so one
            // bound-ordered traversal serves point and non-point queries.
            let query_envelope = index_envelope(query.shape(), query_bounds, self.geographic());
            let to_metre = metric.coordinate_scale();
            return self.bound_ordered_nearest(
                |envelope| aabb_box_distance_2(&query_envelope, envelope).sqrt() * to_metre,
                |idx| {
                    let data = self.rows.prepared_row(idx);
                    pair_distance_resolved(
                        &resolved,
                        &data,
                        &self.rows.frame_cache(idx),
                        query,
                        query_cache,
                    )
                },
                k,
                max_distance,
                exclude,
                frontier,
            );
        }
        if let Some(pruner) = self.geodesic_pruner(metric, shape_pruner_point(query.shape())) {
            // Geodesic point query over an all-point index: prune with the
            // sound lower bound instead of Karney-evaluating every entry.
            return self.bound_ordered_nearest(
                |envelope| pruner.envelope_lower_bound(envelope),
                |idx| {
                    let data = self.rows.prepared_row(idx);
                    pair_distance_resolved(
                        &resolved,
                        &data,
                        &self.rows.frame_cache(idx),
                        query,
                        query_cache,
                    )
                },
                k,
                max_distance,
                exclude,
                frontier,
            );
        }
        // Geodesic metric without a sound envelope bound (non-point shapes
        // realize edge points outside their planar envelope): cap-ordered
        // scan when the per-row cap table holds, exact full scan otherwise.
        if let Some(result) =
            self.geodesic_capped_nearest(metric, query, query_cache, k, max_distance, exclude)?
        {
            return Ok(result);
        }
        let mut nearest: BinaryHeap<NearestCandidate> =
            BinaryHeap::with_capacity(k.min(self.__len__()));
        for entry in self.live_entries() {
            let row = self.rows.row(entry.idx);
            if exclude.is_some_and(|shape| row.with_shape(|other| *other == *shape)) {
                continue;
            }
            let data = self.rows.prepared_row(entry.idx);
            let distance = pair_distance_resolved(
                &resolved,
                &data,
                &self.rows.frame_cache(entry.idx),
                query,
                query_cache,
            )?;
            if max_distance.is_some_and(|max| distance > max.get()) {
                continue;
            }
            push_nearest_candidate(&mut nearest, k, NearestCandidate::new(distance, entry.idx));
        }
        Ok(nearest_candidates_from_heap(nearest))
    }

    /// Best-first nearest traversal over the R-tree nodes: pop envelopes by
    /// ascending lower bound, descend parents, exact-evaluate leaves, and stop
    /// once the lower bound exceeds the relevant ceiling (the k-th best so
    /// far, or `max_distance`). Yields the identical k-nearest as a full scan
    /// for any `bound` that never exceeds the true distance.
    ///
    /// `frontier` is cleared and reused across rows on the array path — batch
    /// local only (no receiver cache, no lock).
    pub(crate) fn bound_ordered_nearest<'a>(
        &'a self,
        bound: impl Fn(&AABB<[f64; 2]>) -> f64,
        exact: impl Fn(usize) -> PyResult<f64>,
        k: usize,
        max_distance: Option<NonNegative>,
        exclude: Option<&Shape>,
        frontier: &mut BinaryHeap<FrontierNode<'a>>,
    ) -> PyResult<Vec<NearestCandidate>> {
        frontier.clear();
        let root_level = self.bulk.root_level();
        for (index, root) in self.bulk.roots().iter().enumerate() {
            frontier.push(FrontierNode {
                bound: bound(&root.envelope),
                node: Frontier::Bulk(root_level, index as u32),
            });
        }
        for child in self.overflow.root().children() {
            frontier.push(FrontierNode::overflow(&bound, child));
        }
        let mut nearest: BinaryHeap<NearestCandidate> =
            BinaryHeap::with_capacity(k.min(self.rows.len() + 1));
        while let Some(node) = frontier.pop() {
            let ceiling = match (nearest.len() == k).then(|| nearest.peek()) {
                Some(Some(worst)) => Some(worst.distance),
                _ => None,
            };
            let ceiling = match (ceiling, max_distance) {
                (Some(a), Some(b)) => Some(a.min(b.get())),
                (Some(a), None) => Some(a),
                (None, Some(a)) => Some(a.get()),
                (None, None) => None,
            };
            if ceiling.is_some_and(|limit| node.bound > limit) {
                break;
            }
            let entry_idx = match node.node {
                Frontier::Bulk(level, index) => {
                    let bulk_node = self.bulk.node(level, index);
                    if level == 0 {
                        for entry in self.bulk.leaf_entries(bulk_node) {
                            if self.bulk.is_live(entry.idx) {
                                frontier.push(FrontierNode {
                                    bound: bound(&entry.envelope),
                                    node: Frontier::Entry(entry.idx),
                                });
                            }
                        }
                    } else {
                        for child in bulk_node.children.clone() {
                            frontier.push(FrontierNode {
                                bound: bound(&self.bulk.node(level - 1, child).envelope),
                                node: Frontier::Bulk(level - 1, child),
                            });
                        }
                    }
                    continue;
                },
                Frontier::Overflow(RTreeNode::Parent(parent)) => {
                    for child in parent.children() {
                        frontier.push(FrontierNode::overflow(&bound, child));
                    }
                    continue;
                },
                Frontier::Overflow(RTreeNode::Leaf(entry)) => entry.idx,
                Frontier::Entry(idx) => idx,
            };
            if exclude.is_some_and(|shape| {
                self.rows
                    .row(entry_idx)
                    .with_shape(|other| *other == *shape)
            }) {
                continue;
            }
            let distance = exact(entry_idx)?;
            if max_distance.is_some_and(|max| distance > max.get()) {
                continue;
            }
            push_nearest_candidate(&mut nearest, k, NearestCandidate::new(distance, entry_idx));
        }
        Ok(nearest_candidates_from_heap(nearest))
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;

    use crate::py::index::parse_max_distance;

    #[test]
    fn nearest_max_distance_rejects_nan_at_parser() {
        Python::initialize();
        parse_max_distance(Some(f64::NAN)).unwrap_err();
    }
}
