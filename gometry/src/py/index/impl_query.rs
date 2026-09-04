use crate::array::ValidityMask;
use crate::geometry::{PreparedPlan, TopologyDomain};
use crate::predicates::engine::{GateOperand, PredicateSpec};
use crate::py::errors::{crs_mismatch_error, epoch_mismatch_error};
use crate::py::index::{
    AABB, Arc, BinaryHeap, Bounds, CapBound, Crs, DistanceUnit, Frame, GeodesicCapsCache,
    GeodesicPruner, GeodesicRowCaps, IndexEntry, IndexEnvelope, IndexPredicate, IndexRowOperation,
    NearestCandidate, NonNegative, Point, PointRows, Predicate, PreparedIndexQuery, PyGeometry,
    PyGeometryArray, PyResult, PySpatialIndex, RowMatches, Shape, ShapeData, ShapeRow,
    bounds_envelope, crs, crs_label, epoch_label, global_geographic_candidate_envelope,
    index_envelopes, nearest_candidates_from_heap, pair_distance_geodesic, pair_dwithin_geodesic,
    pair_dwithin_planar, point_from_bounds, push_nearest_candidate, retain_row, sort_row_ids,
    topology_scalar_pair,
};
use crate::{CsrBuilder, try_push_vec, try_reserve_heap};

impl PySpatialIndex {
    /// Every live entry — the STR bulk (row liveness skipped) then the
    /// dynamic overflow.
    pub(crate) fn live_entries(&self) -> impl Iterator<Item = &IndexEntry> {
        self.bulk
            .live_entries(&self.rows.live)
            .chain(self.overflow.iter())
    }

    /// Append every live handle whose envelope intersects `query`.
    pub(crate) fn collect_intersecting(
        &self,
        query: &AABB<[f64; 2]>,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let start = out.len();
        self.bulk
            .each_intersecting(&self.rows.live, query, |idx| try_push_vec(out, idx))?;
        for entry in self.overflow.locate_in_envelope_intersecting(*query) {
            try_push_vec(out, entry.idx)?;
        }
        dedup_row_ids(out, start);
        Ok(())
    }

    /// Append every live handle whose envelope is contained in `query`.
    pub(crate) fn collect_contained(
        &self,
        query: &AABB<[f64; 2]>,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let start = out.len();
        self.bulk
            .each_contained(&self.rows.live, query, |idx| try_push_vec(out, idx))?;
        for entry in self.overflow.locate_in_envelope(*query) {
            try_push_vec(out, entry.idx)?;
        }
        dedup_row_ids(out, start);
        Ok(())
    }

    /// Append every live handle whose envelope lies within squared distance
    /// `max_distance_2` of `point`.
    pub(crate) fn collect_within_distance_2(
        &self,
        point: [f64; 2],
        max_distance_2: f64,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let start = out.len();
        self.bulk
            .each_within_distance_2(&self.rows.live, point, max_distance_2, |idx| {
                try_push_vec(out, idx)
            })?;
        for entry in self.overflow.locate_within_distance(point, max_distance_2) {
            try_push_vec(out, entry.idx)?;
        }
        dedup_row_ids(out, start);
        Ok(())
    }

    /// Walk every unordered live pair `(i, j)` with `i < j` whose geometries
    /// satisfy the symmetric `predicate` — the self-join engine behind
    /// `self_join`.
    pub(crate) fn for_each_symmetric_pair(
        &self,
        predicate: IndexPredicate,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
        mut found: impl FnMut(usize, usize) -> PyResult<()>,
    ) -> PyResult<()> {
        let spec = match predicate {
            IndexPredicate::Topological(predicate) => Some(predicate.spec()),
            IndexPredicate::Dwithin => None,
        };
        let plan = PreparedIndexQuery::for_index(self, Some(predicate), distance, unit)?;
        let domain = self.domain()?;
        let resolved = match &plan {
            PreparedIndexQuery::Dwithin { resolved, .. } => Some(resolved),
            _ => None,
        };
        let radius = distance.map_or(0.0, NonNegative::get);
        let live = self.live_handles_sorted()?;
        let mut candidates = Vec::new();
        let mut products = self.rows.operation();
        for &i in &live {
            // Symmetric relation: keep only j > i *before* the exact refine,
            // so each unordered pair is refined exactly once.
            let row = products.row(i);
            let prepared = plan.row(self, row, crate::array::BoundsSeed::Unset);
            let (predicate, distance, metric) = plan.candidate_parts();
            candidates.clear();
            if let Some(spec) = spec {
                let query = products.prepared(i);
                if let Some(bounds) = query.bounds() {
                    self.collect_cover_candidates(
                        query.shape(),
                        bounds,
                        candidate_policy(&spec),
                        &mut candidates,
                    )?;
                }
            } else {
                self.candidate_ids_core(
                    prepared.bounds,
                    prepared.pruner_point,
                    prepared.cap,
                    predicate,
                    distance,
                    metric,
                    &mut candidates,
                )?;
            }
            candidates.retain(|&j| j > i);
            match (spec, resolved) {
                (Some(spec), _) => {
                    let query = products.prepared(i);
                    let query_operand = GateOperand::new(products.row(i), None, domain);
                    retain_row(&mut candidates, 0, |idx| {
                        if let Some(verdict) = spec.pre_topology(
                            query_operand,
                            GateOperand::new(products.row(idx), None, domain),
                        ) {
                            return Ok(verdict);
                        }
                        let data = products.prepared(idx);
                        Ok(topology_scalar_pair(
                            &spec,
                            crate::predicates::engine::PreparedTopologyPair::for_domain(
                                &query,
                                &products.frame_cache(i),
                                &data,
                                &products.frame_cache(idx),
                                domain,
                            ),
                        ))
                    })?;
                },
                (None, Some(resolved)) => {
                    let query = products.prepared(i);
                    match resolved {
                        crs::ResolvedMetric::Planar { to_metre } => {
                            retain_row(&mut candidates, 0, |idx| {
                                let data = products.prepared(idx);
                                pair_dwithin_planar(to_metre.get(), &data, &query, radius)
                                    .map_err(Into::into)
                            })?
                        },
                        crs::ResolvedMetric::Geodesic(metric) => {
                            let query_cache = products.frame_cache(i);
                            retain_row(&mut candidates, 0, |idx| {
                                let data = products.prepared(idx);
                                pair_dwithin_geodesic(
                                    metric,
                                    &data,
                                    &products.frame_cache(idx),
                                    &query,
                                    &query_cache,
                                    radius,
                                )
                                .map_err(Into::into)
                            })?;
                        },
                    }
                },
                (None, None) => unreachable!("dwithin self-join resolves its metric once"),
            }
            for &j in &candidates {
                found(i, j)?;
            }
        }
        Ok(())
    }

    /// Reject a query geometry whose CRS/epoch is operationally incompatible
    /// with the indexed frame. Axis-order-only differences (EPSG:4326 ↔
    /// OGC:CRS84) are accepted — query refinement compares coordinates in the
    /// shared lon/lat storage model. An empty index (`None`) accepts any query.
    pub(crate) fn ensure_query_compatible(
        &self,
        geometry: &PyGeometry,
        operation: &str,
    ) -> PyResult<()> {
        self.ensure_frame_compatible(geometry.crs_ref(), geometry.epoch(), operation)
    }

    /// Frame check for every index lane — query, nearest, candidates, join,
    /// and insert alike. Bulk lanes check ONCE per array instead of once per
    /// row.
    ///
    /// Insert used to demand exact string identity on the grounds that an
    /// indexed geometry is later returned carrying the index's single label,
    /// so admitting a differently-spelled CRS would relabel it. It does
    /// relabel it — but only ever between spellings of the *same* frame, since
    /// [`crs_operationally_equal`] rejects every genuine difference. Holding
    /// insert stricter than query just meant an index could answer questions
    /// about a geometry it refused to store.
    pub(crate) fn ensure_frame_compatible(
        &self,
        query_crs: Option<&Crs>,
        query_epoch: Option<f64>,
        operation: &str,
    ) -> PyResult<()> {
        let Some(frame) = &self.metadata else {
            return Ok(());
        };
        let crs_ok = match (frame.crs_ref(), query_crs) {
            (None, None) => true,
            (Some(index), Some(query)) => crate::crs_operationally_equal(index, query)?,
            _ => false,
        };
        if !crs_ok {
            return Err(crs_mismatch_error(
                format!(
                    "{operation} requires the geometry to share the index CRS; index is {}, got {}",
                    crs_label(frame.crs_str()),
                    crs_label(query_crs.map(|crs| &**crs)),
                ),
                frame.crs_str(),
                query_crs.map(Crs::as_str),
                None,
            ));
        }
        if query_epoch != frame.epoch() {
            return Err(epoch_mismatch_error(
                format!(
                    "{operation} requires the geometry to share the index coordinate epoch; index is {}, got {}",
                    epoch_label(frame.epoch()),
                    epoch_label(query_epoch),
                ),
                frame.epoch(),
                query_epoch,
                None,
            ));
        }
        Ok(())
    }

    /// CRS string for metric resolution after a successful frame check.
    ///
    /// Index metadata wins when present (frames already match). An empty
    /// index has no metadata and accepts any query CRS, so fall through to
    /// the caller's string. Shared by every distance-bearing index surface
    /// (`query`/`candidates`/`nearest`/`join`/`self_join`).
    pub(crate) fn metric_crs_str<'a>(&'a self, query_crs: Option<&'a str>) -> Option<&'a str> {
        self.metadata
            .as_ref()
            .and_then(Frame::crs_str)
            .or(query_crs)
    }

    /// The geodesic candidate pruner for `geometry`, when the sound lower
    /// bound applies: a geodesic metric, a point query, and an all-point index
    /// (planar envelopes bound exactly the realized point sets).
    /// The lazy per-row geodesic cap table — built once per `(row count,
    /// mutation generation, CRS runtime generation)` so inserts/removes and
    /// ellipsoid reconfiguration both invalidate; a cached `None` records a
    /// known-unsound frame (out-of-domain rows, non-oblate ellipsoid) so failed
    /// builds never repeat under the same key.
    pub(crate) fn geodesic_row_caps(
        &self,
        metric: &crs::MetricModel,
    ) -> PyResult<Option<Arc<GeodesicRowCaps>>> {
        let crs::MetricModel::Geodesic { crs, frame, state } = metric else {
            return Ok(None);
        };
        if *frame != crate::geographic::GeographicFrame::crs84() {
            return Ok(None);
        }
        self.geodesic_row_caps_at(crs, *frame, state)
    }

    fn geodesic_row_caps_at(
        &self,
        crs: &str,
        frame: crate::geographic::GeographicFrame,
        runtime_state: &crs::RuntimeState,
    ) -> PyResult<Option<Arc<GeodesicRowCaps>>> {
        if frame != crate::geographic::GeographicFrame::crs84() {
            return Ok(None);
        }
        let runtime_gen = runtime_state.generation;
        let mut cache = self
            .geodesic_caps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache.row_count == self.rows.len()
            && cache.generation == self.mutation_gen
            && cache.runtime_generation == runtime_gen
            && self.rows.len() > 0
        {
            return Ok(cache.caps.clone());
        }
        // Resolve the ellipsoid OUTSIDE the metric scope: `with_geodesic`
        // holds the thread-local handle cache borrow across its closure.
        let Ok(shape) = crs::ellipsoid_shape_at(crs, &runtime_state) else {
            *cache = GeodesicCapsCache {
                row_count: self.rows.len(),
                generation: self.mutation_gen,
                runtime_generation: runtime_gen,
                caps: None,
            };
            return Ok(None);
        };
        let mut products = self.rows.operation();
        let built = match crs::with_ellipsoid_metric_at(crs, &[], &runtime_state, |ellipsoid| {
            Ok(GeodesicRowCaps::from_live_handles(
                shape,
                self.rows.len(),
                self.live_entries().map(|entry| {
                    let (semi_major, flattening) = ellipsoid.ellipsoid_parameters();
                    (entry.idx, {
                        let data = products.prepared(entry.idx);
                        data.geodesic_cap_cached(
                            &products.frame_cache(entry.idx),
                            crs,
                            semi_major,
                            flattening,
                            ellipsoid,
                            &runtime_state,
                        )
                        .ok()
                        .flatten()
                    })
                }),
            ))
        }) {
            Ok(Ok(Some(caps))) => Some(Arc::new(caps)),
            Ok(Ok(None)) | Err(_) => None,
            Ok(Err(())) => {
                return Err(pyo3::exceptions::PyMemoryError::new_err(
                    "failed to allocate geodesic index bounds",
                ));
            },
        };
        *cache = GeodesicCapsCache {
            row_count: self.rows.len(),
            generation: self.mutation_gen,
            runtime_generation: runtime_gen,
            caps: built.clone(),
        };
        Ok(built)
    }

    /// Cap-ordered geodesic nearest over non-point rows: sound per-row
    /// lower bounds order the exact evaluations and stop the scan once no
    /// bound can beat the running k-th best (or `max_distance`). `None`
    /// when the cap table or the query cap cannot be built — the exact
    /// full scan owns those (including their domain errors).
    pub(crate) fn geodesic_capped_nearest(
        &self,
        metric: &crs::GeodesicMetric,
        query: &PreparedPlan,
        query_cache: &mut impl FnMut() -> Arc<crate::geometry::FrameDependentCaches>,
        k: usize,
        max_distance: Option<NonNegative>,
        exclude: Option<&Shape>,
    ) -> PyResult<Option<Vec<NearestCandidate>>> {
        let Some(caps) = self.geodesic_row_caps_at(&metric.crs, metric.frame, &metric.state)?
        else {
            return Ok(None);
        };
        let query_cache_owner = query_cache();
        let Some((anchor, reach)) =
            crs::with_ellipsoid_metric_at(&metric.crs, &[], &metric.state, |ellipsoid| {
                let (semi_major, flattening) = ellipsoid.ellipsoid_parameters();
                query.geodesic_cap_cached(
                    &query_cache_owner,
                    &metric.crs,
                    semi_major,
                    flattening,
                    ellipsoid,
                    &metric.state,
                )
            })
            .ok()
            .flatten()
        else {
            return Ok(None);
        };
        let Some(query_anchor) = caps.query_anchor(anchor) else {
            return Ok(None);
        };
        let mut order = Vec::new();
        for entry in self.live_entries() {
            try_push_vec(&mut order, CapBound {
                bound: caps.lower_bound(query_anchor, reach, entry.idx),
                idx: entry.idx,
            })?;
        }
        order.sort_unstable_by_key(|entry| entry.idx);
        order.dedup_by_key(|entry| entry.idx);
        let mut order = BinaryHeap::from(order);
        let mut nearest: BinaryHeap<NearestCandidate> = BinaryHeap::new();
        let mut products = self.rows.operation();
        try_reserve_heap(&mut nearest, k.min(self.rows.len().saturating_add(1)))
            .map_err(pyo3::PyErr::from)?;
        while let Some(CapBound { bound, idx }) = order.pop() {
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
            if ceiling.is_some_and(|limit| bound > limit) {
                break;
            }
            let row = products.row(idx);
            if exclude.is_some_and(|shape| row.with_shape(|other| *other == *shape)) {
                continue;
            }
            let data = products.prepared(idx);
            let distance = pair_distance_geodesic(
                metric,
                &data,
                &products.frame_cache(idx),
                query,
                &query_cache_owner,
            )?;
            if max_distance.is_some_and(|max| distance > max.get()) {
                continue;
            }
            push_nearest_candidate(&mut nearest, k, NearestCandidate::new(distance, idx))?;
        }
        Ok(Some(nearest_candidates_from_heap(nearest)))
    }

    pub(crate) fn geodesic_pruner(
        &self,
        metric: &crs::MetricModel,
        query: Option<Point>,
    ) -> Option<GeodesicPruner> {
        let crs::MetricModel::Geodesic { crs, frame, state } = metric else {
            return None;
        };
        if *frame != crate::geographic::GeographicFrame::crs84() {
            return None;
        }
        if self.non_prunable_live != 0 {
            return None;
        }
        GeodesicPruner::new_at(crs, query?, state)
    }

    /// The candidate walk for one ALREADY frame-checked query: its bounds,
    /// its point identity (the geodesic pruner's precondition), and the
    /// once-resolved distance model — the shared core of the scalar lane
    /// and the per-row array lane (which never stages a `PyGeometry`).
    pub(crate) fn candidate_ids_core(
        &self,
        query_bounds: Option<Bounds>,
        pruner_point: Option<Point>,
        query_cap: Option<(Point, f64)>,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        distance_model: Option<&crs::MetricModel>,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        out.clear();
        self.candidate_ids_core_append(
            query_bounds,
            pruner_point,
            query_cap,
            predicate,
            distance,
            distance_model,
            out,
        )
    }

    /// [`candidate_ids_core`](Self::candidate_ids_core) without clearing the
    /// destination, for CSR builders that append every query row directly into
    /// one flat values column.
    pub(crate) fn candidate_ids_core_append(
        &self,
        query_bounds: Option<Bounds>,
        pruner_point: Option<Point>,
        query_cap: Option<(Point, f64)>,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        distance_model: Option<&crs::MetricModel>,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let start = out.len();
        self.candidate_ids_core_append_inner(
            query_bounds,
            pruner_point,
            query_cap,
            predicate,
            distance,
            distance_model,
            out,
        )?;
        dedup_row_ids(out, start);
        Ok(())
    }

    fn candidate_ids_core_append_inner(
        &self,
        query_bounds: Option<Bounds>,
        pruner_point: Option<Point>,
        query_cap: Option<(Point, f64)>,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        distance_model: Option<&crs::MetricModel>,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let Some(query_bounds) = query_bounds else {
            return Ok(());
        };
        let coordinate_distance = distance.and_then(|distance| {
            distance_model
                .as_ref()
                .and_then(|model| model.coordinate_radius(distance.get()))
        });
        let point_distance_query = coordinate_distance
            .zip(point_from_bounds(query_bounds))
            .and_then(|(distance, point)| {
                let distance_2 = distance * distance;
                distance_2
                    .is_finite()
                    .then_some(([point.x, point.y], distance_2))
            });
        if let Some((query, distance_2)) = point_distance_query {
            self.collect_within_distance_2(query, distance_2, out)?;
            return Ok(());
        }
        if let Some(distance) = coordinate_distance {
            let query_envelope = bounds_envelope(query_bounds.expand(distance));
            self.collect_intersecting(&query_envelope, out)?;
            return Ok(());
        }
        if let Some(distance) = distance {
            let distance = distance.get();
            // Geodesic dwithin: a sound candidate window when the bound
            // applies, otherwise every entry is a candidate (exact distances
            // decide).
            let pruner = distance_model.and_then(|model| self.geodesic_pruner(model, pruner_point));
            if let Some(windows) = pruner
                .as_ref()
                .and_then(|pruner| pruner.dwithin_windows(distance))
            {
                // Windows never overlap (the wrap split is disjoint), so the
                // concatenation has no duplicates.
                for window in windows.iter() {
                    self.collect_intersecting(window, out)?;
                }
                return Ok(());
            }
            // Cap-table fallback — non-point queries and non-point rows
            // both get a SOUND row filter (a superset of every true dwithin
            // match) instead of the all-rows envelope.
            if let Some((anchor, reach)) = query_cap {
                let row_caps = match distance_model {
                    Some(model) => self.geodesic_row_caps(model)?,
                    None => None,
                };
                if let Some(caps) = row_caps
                    && let Some(query_anchor) = caps.query_anchor(anchor)
                {
                    for entry in self.live_entries().filter(|entry| {
                        caps.lower_bound(query_anchor, reach, entry.idx) <= distance
                    }) {
                        try_push_vec(out, entry.idx)?;
                    }
                    return Ok(());
                }
            }
            let query_envelope = global_geographic_candidate_envelope();
            self.collect_intersecting(&query_envelope, out)?;
            return Ok(());
        }
        let query_envelope = bounds_envelope(query_bounds);
        let containment = matches!(
            predicate,
            Some(IndexPredicate::Topological(predicate))
                if predicate.spec().index_envelope == Some(IndexEnvelope::ContainedInQuery)
        );
        if containment {
            self.collect_contained(&query_envelope, out)?;
        } else {
            self.collect_intersecting(&query_envelope, out)?;
        }
        Ok(())
    }

    /// Candidate handles, sorted (the public `candidates` contract).
    pub(crate) fn candidate_ids_sorted(
        &self,
        geometry: &PyGeometry,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Vec<usize>> {
        let plan = PreparedIndexQuery::for_geometry(self, geometry, None, distance, unit)?;
        let query_plan = geometry.plan_or_transient();
        let row = plan.row(
            self,
            ShapeRow::Handle(&query_plan),
            crate::array::BoundsSeed::Unset,
        );
        let mut ids = Vec::new();
        if distance.is_none() {
            if let Some(bounds) = query_plan.bounds() {
                self.collect_shape_candidates(query_plan.shape(), bounds, &mut ids)?;
            }
            sort_row_ids(&mut ids, self.rows.len());
            return Ok(ids);
        }
        let (predicate, distance, metric) = plan.candidate_parts();
        self.candidate_ids_core(
            row.bounds,
            row.pruner_point,
            row.cap,
            predicate,
            distance,
            metric,
            &mut ids,
        )?;
        sort_row_ids(&mut ids, self.rows.len());
        Ok(ids)
    }

    /// One ARRAY-row dwithin query against the index: the shared candidate
    /// core plus the pair kernel, all on stack handles (frame and metric
    /// resolved once per array by the caller). Shared by `query()`'s array
    /// lane and `join`'s left-row lane.
    pub(crate) fn planar_dwithin_query_row_matches(
        &self,
        row: ShapeRow<'_>,
        plan: &PreparedIndexQuery,
        matches: &mut Vec<usize>,
        seeded_bounds: crate::array::BoundsSeed,
    ) -> PyResult<()> {
        matches.clear();
        self.planar_dwithin_query_row_matches_append(row, plan, matches, seeded_bounds)
    }

    /// Append one dwithin result row directly to a flat CSR values column.
    pub(crate) fn planar_dwithin_query_row_matches_append(
        &self,
        row: ShapeRow<'_>,
        plan: &PreparedIndexQuery,
        matches: &mut Vec<usize>,
        seeded_bounds: crate::array::BoundsSeed,
    ) -> PyResult<()> {
        let row_start = matches.len();
        let prepared = plan.row(self, row, seeded_bounds);
        let (predicate, candidate_distance, candidate_metric) = plan.candidate_parts();
        self.candidate_ids_core_append(
            prepared.bounds,
            prepared.pruner_point,
            prepared.cap,
            predicate,
            candidate_distance,
            candidate_metric,
            matches,
        )?;
        let PreparedIndexQuery::Dwithin {
            resolved, distance, ..
        } = plan
        else {
            unreachable!("dwithin matcher is called only with a dwithin plan")
        };
        let query = crate::array::PreparedRow::transient_with_seed(row, seeded_bounds);
        let crs::ResolvedMetric::Planar { to_metre } = resolved else {
            unreachable!("planar dwithin runner receives a planar query")
        };
        let mut products = self.rows.operation();
        retain_row(matches, row_start, |idx| {
            let data = products.prepared(idx);
            pair_dwithin_planar(to_metre.get(), &data, &query, distance.get()).map_err(Into::into)
        })?;
        sort_row_ids(&mut matches[row_start..], self.rows.len());
        Ok(())
    }

    pub(crate) fn geodesic_dwithin_query_row_matches_append(
        &self,
        row: ShapeRow<'_>,
        metric: &crs::GeodesicMetric,
        query_cache: &mut impl FnMut() -> Arc<crate::geometry::FrameDependentCaches>,
        plan: &PreparedIndexQuery,
        matches: &mut Vec<usize>,
        seeded_bounds: crate::array::BoundsSeed,
    ) -> PyResult<()> {
        let row_start = matches.len();
        let prepared = plan.row(self, row, seeded_bounds);
        let (predicate, candidate_distance, candidate_metric) = plan.candidate_parts();
        self.candidate_ids_core_append(
            prepared.bounds,
            prepared.pruner_point,
            prepared.cap,
            predicate,
            candidate_distance,
            candidate_metric,
            matches,
        )?;
        let PreparedIndexQuery::Dwithin { distance, .. } = plan else {
            unreachable!("dwithin matcher is called only with a dwithin plan")
        };
        let query = crate::array::PreparedRow::transient_with_seed(row, seeded_bounds);
        let query_cache = query_cache();
        let mut products = self.rows.operation();
        retain_row(matches, row_start, |idx| {
            let data = products.prepared(idx);
            pair_dwithin_geodesic(
                metric,
                &data,
                &products.frame_cache(idx),
                &query,
                &query_cache,
                distance.get(),
            )
            .map_err(Into::into)
        })?;
        sort_row_ids(&mut matches[row_start..], self.rows.len());
        Ok(())
    }

    /// Refine packed point topological queries by indexed row. Candidate
    /// traversal remains point-driven, while every candidate is refined by
    /// the shared exact relation matrix.
    fn point_rows_join_matches(
        &self,
        array: &PyGeometryArray,
        points: &PointRows<'_>,
        missing: Option<&ValidityMask>,
        predicate: Predicate,
    ) -> PyResult<RowMatches> {
        let domain = self.domain()?;
        let mut builder =
            CsrBuilder::<usize>::with_capacity(0, points.len()).map_err(pyo3::PyErr::from)?;
        let mut queries = Vec::with_capacity(points.len());
        let mut query_rows = array.row_operation();
        for (row, point) in points.iter().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                let end = builder.len();
                builder.push_end(end).map_err(pyo3::PyErr::from)?;
                queries.push(None);
                continue;
            }
            let row_start = builder.len();
            self.collect_point_candidates(point, builder.values_mut())?;
            let end = builder.len();
            builder.push_end(end).map_err(pyo3::PyErr::from)?;
            queries.push((end > row_start).then(|| {
                (
                    PreparedPlan::new(ShapeData::new(Shape::Point(point))),
                    query_rows.frame_cache(row),
                )
            }));
        }
        let (mut ids, offsets) = builder.finish();
        if ids.is_empty() {
            return Ok(RowMatches { ids, offsets });
        }

        let mut candidates = crate::try_vec_with_capacity(ids.len())?;
        for row in 0..points.len() {
            for position in offsets[row] as usize..offsets[row + 1] as usize {
                try_push_vec(&mut candidates, (ids[position], row, position))?;
            }
        }
        // Refinement is indexed-row-local, while the tuple tie-breakers retain
        // the candidate's original read order within that row.
        candidates.sort_unstable_by_key(|candidate| *candidate);

        let spec = predicate.spec();
        let mut keep = crate::try_vec_with_capacity(ids.len())?;
        keep.resize(ids.len(), false);
        let mut index_products = self.rows.operation();
        let mut start = 0;
        while start < candidates.len() {
            let idx = candidates[start].0;
            let data = index_products.prepared(idx);
            let mut end = start;
            while end < candidates.len() && candidates[end].0 == idx {
                let (_, row, position) = candidates[end];
                let (query, query_cache) = queries[row]
                    .as_ref()
                    .expect("every grouped point row has a candidate plan");
                keep[position] = topology_scalar_pair(
                    &spec,
                    crate::predicates::engine::PreparedTopologyPair::for_domain(
                        query,
                        query_cache,
                        &data,
                        &index_products.frame_cache(idx),
                        domain,
                    ),
                );
                end += 1;
            }
            start = end;
        }

        let mut refined_ids = 0;
        let mut refined_offsets = CsrBuilder::<()>::new();
        for row in 0..points.len() {
            for position in offsets[row] as usize..offsets[row + 1] as usize {
                if keep[position] {
                    ids[refined_ids] = ids[position];
                    refined_ids += 1;
                }
            }
            refined_offsets
                .push_end(refined_ids)
                .map_err(pyo3::PyErr::from)?;
        }
        ids.truncate(refined_ids);
        let (_, refined_offsets) = refined_offsets.finish();
        Ok(RowMatches {
            ids,
            offsets: refined_offsets,
        })
    }

    /// Whether this index's shared frame is geographic. All rows and the query
    /// share this frame (`ensure_query_compatible`), so it is an index-level
    /// property used by the quotient-aware relation path.
    pub(crate) fn domain(&self) -> PyResult<TopologyDomain> {
        Ok(self
            .metadata
            .as_ref()
            .map(crate::geometry::topology_domain)
            .transpose()
            .map(|domain| domain.unwrap_or(TopologyDomain::Cartesian))?)
    }

    fn collect_point_candidates(&self, point: Point, out: &mut Vec<usize>) -> PyResult<()> {
        let shape = Shape::Point(point);
        let Some(bounds) = shape.bounds() else {
            return Ok(());
        };
        self.collect_shape_candidates(&shape, bounds, out)
    }

    /// Collect physical envelope hits and collapse periodic aliases to rows.
    pub(crate) fn collect_shape_candidates(
        &self,
        shape: &Shape,
        bounds: Bounds,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        self.collect_cover_candidates(shape, bounds, IndexEnvelope::Intersecting, out)
    }

    /// Candidate rows for a query shape under one envelope rule.
    ///
    /// A shape reaches the tree as a cover — the windows of its parts, and the
    /// seam windows a periodic frame splits it into — so one logical row can
    /// answer through several physical entries. Those aliases collapse here,
    /// leaving the caller a row id per match to refine exactly.
    fn collect_cover_candidates(
        &self,
        shape: &Shape,
        bounds: Bounds,
        policy: IndexEnvelope,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let start = out.len();
        let cover = index_envelopes(shape, bounds, self.domain()?)?;
        for envelope in cover.windows(policy) {
            match policy {
                IndexEnvelope::ContainedInQuery => self.collect_contained(envelope, out)?,
                IndexEnvelope::Intersecting => self.collect_intersecting(envelope, out)?,
            }
        }
        dedup_row_ids(out, start);
        Ok(())
    }

    fn refine_prepared(
        &self,
        geometry: &PyGeometry,
        candidates: Vec<usize>,
        plan: &PreparedIndexQuery,
    ) -> PyResult<Vec<usize>> {
        let mut matches = candidates;
        let query_plan = geometry.plan_or_transient();
        let domain = self.domain()?;
        let mut products = self.rows.operation();
        match plan {
            PreparedIndexQuery::Topological { predicate } => {
                let spec = predicate.spec();
                // `query OP item`: the query is the fixed left operand of the
                // shared batch engine (prepared/cached exactly like the
                // broadcast surfaces); candidate handles feed it directly.
                let query_operand = GateOperand::new(
                    ShapeRow::Shape(query_plan.shape()),
                    query_plan.bounds(),
                    domain,
                );
                retain_row(&mut matches, 0, |idx| {
                    if let Some(verdict) = spec.pre_topology(
                        query_operand,
                        GateOperand::new(products.row(idx), None, domain),
                    ) {
                        return Ok(verdict);
                    }
                    let data = products.prepared(idx);
                    Ok(topology_scalar_pair(
                        &spec,
                        crate::predicates::engine::PreparedTopologyPair::for_domain(
                            &query_plan,
                            &geometry.frame_cache(),
                            &data,
                            &products.frame_cache(idx),
                            domain,
                        ),
                    ))
                })?;
            },
            PreparedIndexQuery::Dwithin {
                resolved, distance, ..
            } => {
                let distance = distance.get();
                match resolved {
                    crs::ResolvedMetric::Planar { to_metre } => {
                        retain_row(&mut matches, 0, |idx| {
                            let data = products.prepared(idx);
                            pair_dwithin_planar(to_metre.get(), &data, &query_plan, distance)
                                .map_err(Into::into)
                        })?
                    },
                    crs::ResolvedMetric::Geodesic(metric) => retain_row(&mut matches, 0, |idx| {
                        let data = products.prepared(idx);
                        pair_dwithin_geodesic(
                            metric,
                            &data,
                            &products.frame_cache(idx),
                            &query_plan,
                            &geometry.frame_cache(),
                            distance,
                        )
                        .map_err(Into::into)
                    })?,
                }
            },
            PreparedIndexQuery::Candidates | PreparedIndexQuery::CandidatesDwithin { .. } => {
                unreachable!("exact refinement is called only with a predicate plan")
            },
        }
        sort_row_ids(&mut matches, self.rows.len());
        Ok(matches)
    }

    /// Candidate collection plus exact refine — the engine behind `query` and
    /// `join`.
    pub(crate) fn query_exact(
        &self,
        geometry: &PyGeometry,
        predicate: IndexPredicate,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Vec<usize>> {
        let plan =
            PreparedIndexQuery::for_geometry(self, geometry, Some(predicate), distance, unit)?;
        let query_plan = geometry.plan_or_transient();
        if let IndexPredicate::Topological(predicate) = predicate
            && distance.is_none()
        {
            return self.topological_matches(&query_plan, &geometry.frame_cache(), predicate);
        }
        let prepared = plan.row(
            self,
            ShapeRow::Handle(&query_plan),
            crate::array::BoundsSeed::Unset,
        );
        let mut candidates = Vec::new();
        let (candidate_predicate, candidate_distance, metric) = plan.candidate_parts();
        self.candidate_ids_core(
            prepared.bounds,
            prepared.pruner_point,
            prepared.cap,
            candidate_predicate,
            candidate_distance,
            metric,
            &mut candidates,
        )?;
        self.refine_prepared(geometry, candidates, &plan)
    }

    /// One topological query against a pre-checked SHAPE handle — the
    /// shared core of the scalar `query_exact` and the bulk array lane
    /// (which checks the frame ONCE and iterates storage rows with no
    /// per-row `PyGeometry` materialization). Distance-free by contract.
    pub(crate) fn topological_matches(
        &self,
        shape: &PreparedPlan,
        query_cache: &crate::geometry::FrameDependentCaches,
        predicate: Predicate,
    ) -> PyResult<Vec<usize>> {
        let mut matches = Vec::new();
        self.topological_matches_append(shape, query_cache, predicate, &mut matches)?;
        Ok(matches)
    }

    /// Append one topological result row directly into a flat CSR values
    /// column — the array `query` lane's one-pass builder.
    pub(crate) fn topological_matches_append(
        &self,
        shape: &PreparedPlan,
        query_cache: &crate::geometry::FrameDependentCaches,
        predicate: Predicate,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let row_start = out.len();
        let Some(query_bounds) = shape.bounds() else {
            return Ok(());
        };
        let spec = predicate.spec();
        let Some(frame) = self.metadata.as_ref() else {
            return Ok(());
        };
        self.collect_cover_candidates(shape.shape(), query_bounds, candidate_policy(&spec), out)?;
        let domain = crate::geometry::topology_domain(frame)?;
        // The query is fixed for every candidate, so read its gate facts once.
        // A candidate the gate settles is never prepared at all.
        let query_operand =
            GateOperand::new(ShapeRow::Shape(shape.shape()), Some(query_bounds), domain);
        let mut products = self.rows.operation();
        retain_row(out, row_start, |idx| {
            if let Some(verdict) = spec.pre_topology(
                query_operand,
                GateOperand::new(products.row(idx), None, domain),
            ) {
                return Ok(verdict);
            }
            let data = products.prepared(idx);
            Ok(topology_scalar_pair(
                &spec,
                crate::predicates::engine::PreparedTopologyPair::for_domain(
                    shape,
                    query_cache,
                    &data,
                    &products.frame_cache(idx),
                    domain,
                ),
            ))
        })?;
        Ok(())
    }

    /// Column-direct engine for packed point query arrays: one frame check and
    /// one metric/spec resolution for the whole column, then per-point
    /// envelope lookups refined with the shared relation matrix — no per-row
    /// `PyGeometry` materialization. `predicate=None` returns candidates only.
    pub(crate) fn point_rows_matches(
        &self,
        array: &PyGeometryArray,
        points: &PointRows<'_>,
        missing: Option<&ValidityMask>,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<RowMatches> {
        let plan = PreparedIndexQuery::for_array(self, array, predicate, distance, unit)?;
        if distance.is_none()
            && let Some(IndexPredicate::Topological(predicate)) = predicate
        {
            return self.point_rows_join_matches(array, points, missing, predicate);
        }
        if let PreparedIndexQuery::Dwithin {
            distance,
            metric,
            resolved,
        } = &plan
        {
            let mut query_rows = array.row_operation();
            return match resolved {
                crs::ResolvedMetric::Planar { to_metre } => self.point_rows_dwithin(
                    points,
                    missing,
                    *distance,
                    metric,
                    |_, query, ids, row_start, products| {
                        retain_row(ids, row_start, |idx| {
                            let data = products.prepared(idx);
                            pair_dwithin_planar(to_metre.get(), query, &data, distance.get())
                                .map_err(Into::into)
                        })
                    },
                ),
                crs::ResolvedMetric::Geodesic(geodesic) => self.point_rows_dwithin(
                    points,
                    missing,
                    *distance,
                    metric,
                    |row, query, ids, row_start, products| {
                        let query_cache = query_rows.frame_cache(row);
                        retain_row(ids, row_start, |idx| {
                            let data = products.prepared(idx);
                            pair_dwithin_geodesic(
                                geodesic,
                                query,
                                &query_cache,
                                &data,
                                &products.frame_cache(idx),
                                distance.get(),
                            )
                            .map_err(Into::into)
                        })
                    },
                ),
            };
        }
        let spec = match predicate {
            Some(IndexPredicate::Topological(predicate)) => Some(predicate.spec()),
            Some(IndexPredicate::Dwithin) | None => None,
        };
        let model = match &plan {
            PreparedIndexQuery::CandidatesDwithin { metric, .. }
            | PreparedIndexQuery::Dwithin { metric, .. } => Some(metric),
            _ => None,
        };
        let coordinate_distance =
            distance.and_then(|distance| model.and_then(|m| m.coordinate_radius(distance.get())));
        // Geodesic dwithin over an all-point index: the sound per-point
        // candidate window replaces the global scan.
        let (geodesic_crs, geodesic_state) = match (model, distance) {
            (Some(crs::MetricModel::Geodesic { crs, frame, state }), Some(_))
                if self.non_prunable_live == 0 && coordinate_distance.is_none() =>
            {
                if *frame == crate::geographic::GeographicFrame::crs84() {
                    (Some(crs.clone()), Some(state))
                } else {
                    (None, None)
                }
            },
            _ => (None, None),
        };
        // CSR-direct: every row appends into one flat ids buffer; candidate
        // collection orders the window and refinement compacts it in place —
        // no per-point `Vec`.
        let mut builder =
            CsrBuilder::<usize>::with_capacity(0, points.len()).map_err(pyo3::PyErr::from)?;
        // Point queries never cross; geographic rows are refined by the
        // quotient-aware matrix path below.
        let domain = self.domain()?;
        let mut query_rows = array.row_operation();
        let mut index_products = self.rows.operation();
        for (row, point) in points.iter().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                let end = builder.len();
                builder.push_end(end).map_err(pyo3::PyErr::from)?;
                continue;
            }
            let query = PreparedPlan::new(ShapeData::new(Shape::Point(point)));
            let row_start = builder.len();
            match (coordinate_distance, distance) {
                (Some(radius), _) => {
                    self.collect_within_distance_2(
                        [point.x, point.y],
                        radius * radius,
                        builder.values_mut(),
                    )?;
                },
                (None, Some(distance)) => {
                    let distance = distance.get();
                    match geodesic_crs
                        .as_deref()
                        .and_then(|crs| {
                            geodesic_state
                                .and_then(|state| GeodesicPruner::new_at(crs, point, state))
                        })
                        .and_then(|pruner| pruner.dwithin_windows(distance))
                    {
                        Some(windows) => {
                            for window in windows.iter() {
                                self.collect_intersecting(window, builder.values_mut())?;
                            }
                        },
                        None => {
                            for entry in self.live_entries() {
                                try_push_vec(builder.values_mut(), entry.idx)?;
                            }
                        },
                    }
                },
                (None, None) => {
                    self.collect_point_candidates(point, builder.values_mut())?;
                },
            }
            dedup_row_ids(builder.values_mut(), row_start);
            let mut ids = builder.values_mut();
            match (&spec, model, predicate) {
                (Some(spec), ..) => {
                    let query_operand =
                        GateOperand::new(ShapeRow::Shape(query.shape()), query.bounds(), domain);
                    let query_cache = query_rows.frame_cache(row);
                    retain_row(&mut ids, row_start, |idx| {
                        if let Some(verdict) = spec.pre_topology(
                            query_operand,
                            GateOperand::new(index_products.row(idx), None, domain),
                        ) {
                            return Ok(verdict);
                        }
                        let data = index_products.prepared(idx);
                        Ok(topology_scalar_pair(
                            spec,
                            crate::predicates::engine::PreparedTopologyPair::for_domain(
                                &query,
                                &query_cache,
                                &data,
                                &index_products.frame_cache(idx),
                                domain,
                            ),
                        ))
                    })?
                },
                (None, Some(_), Some(IndexPredicate::Dwithin)) => {
                    unreachable!("exact dwithin selects a typed point-row runner")
                },
                // Candidates-only: the envelope answer is the result.
                _ => {},
            }
            let end = builder.len();
            builder.push_end(end).map_err(pyo3::PyErr::from)?;
        }
        let (ids, offsets) = builder.finish();
        Ok(RowMatches { ids, offsets })
    }

    fn point_rows_dwithin(
        &self,
        points: &PointRows<'_>,
        missing: Option<&ValidityMask>,
        distance: NonNegative,
        model: &crs::MetricModel,
        mut refine: impl FnMut(
            usize,
            &PreparedPlan,
            &mut Vec<usize>,
            usize,
            &mut IndexRowOperation<'_>,
        ) -> PyResult<()>,
    ) -> PyResult<RowMatches> {
        let coordinate_distance = model.coordinate_radius(distance.get());
        let geodesic_pruner = match model {
            crs::MetricModel::Geodesic { crs, frame, state }
                if self.non_prunable_live == 0 && coordinate_distance.is_none() =>
            {
                (*frame == crate::geographic::GeographicFrame::crs84()).then_some((crs, state))
            },
            _ => None,
        };
        let mut builder =
            CsrBuilder::<usize>::with_capacity(0, points.len()).map_err(pyo3::PyErr::from)?;
        let mut products = self.rows.operation();
        for (row, point) in points.iter().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                let end = builder.len();
                builder.push_end(end).map_err(pyo3::PyErr::from)?;
                continue;
            }
            let query = PreparedPlan::new(ShapeData::new(Shape::Point(point)));
            let row_start = builder.len();
            if let Some(radius) = coordinate_distance {
                self.collect_within_distance_2(
                    [point.x, point.y],
                    radius * radius,
                    builder.values_mut(),
                )?;
            } else if let Some((crs, state)) = geodesic_pruner {
                if let Some(windows) = GeodesicPruner::new_at(crs, point, state)
                    .and_then(|pruner| pruner.dwithin_windows(distance.get()))
                {
                    for window in windows.iter() {
                        self.collect_intersecting(window, builder.values_mut())?;
                    }
                } else {
                    for entry in self.live_entries() {
                        try_push_vec(builder.values_mut(), entry.idx)?;
                    }
                }
            } else {
                for entry in self.live_entries() {
                    try_push_vec(builder.values_mut(), entry.idx)?;
                }
            }
            dedup_row_ids(builder.values_mut(), row_start);
            refine(row, &query, builder.values_mut(), row_start, &mut products)?;
            let end = builder.len();
            builder.push_end(end).map_err(pyo3::PyErr::from)?;
        }
        let (ids, offsets) = builder.finish();
        Ok(RowMatches { ids, offsets })
    }
}

/// The candidate lookup `spec` calls for. Admission rejects a predicate the
/// index cannot narrow, so every spec that reaches a query carries a rule.
fn candidate_policy(spec: &PredicateSpec) -> IndexEnvelope {
    spec.index_envelope
        .expect("the index rejects a predicate it cannot accelerate")
}

fn dedup_row_ids(ids: &mut Vec<usize>, start: usize) {
    ids[start..].sort_unstable();
    let mut write = start;
    for read in start..ids.len() {
        if write == start || ids[read] != ids[write - 1] {
            ids[write] = ids[read];
            write += 1;
        }
    }
    ids.truncate(write);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::BoundsSeed;
    use crate::geometry::{CoordSeq, LineSeq};
    use crate::py::index::spatial_index;

    #[test]
    fn dwithin_exact_refinement_preserves_unset_query_bounds() {
        let index = spatial_index(vec![PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(0.0, 0.0))),
            Frame::None,
        )])
        .unwrap();
        let array = PyGeometryArray::mixed_shapes(
            vec![Shape::LineString(
                LineSeq::try_new(CoordSeq::from_vecs(
                    vec![0.0, 1.0],
                    vec![0.0, 1.0],
                    None,
                    None,
                ))
                .expect("fixture line has two vertices"),
            )],
            Frame::None,
        );
        let plan = PreparedIndexQuery::for_array(
            &index,
            &array,
            Some(IndexPredicate::Dwithin),
            Some(NonNegative::try_new("distance", 0.0).unwrap()),
            None,
        )
        .unwrap();
        let mut matches = Vec::new();

        index
            .planar_dwithin_query_row_matches_append(
                array.storage().row(0),
                &plan,
                &mut matches,
                BoundsSeed::Unset,
            )
            .unwrap();

        assert_eq!(matches, [0]);
        assert_eq!(array.row_products.initialized_counts(), (0, 0, 0));
    }
}
