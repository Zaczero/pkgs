#[cfg(test)]
use std::cell::Cell;

use rstar::Envelope as _;

use crate::py::errors::{crs_mismatch_error, epoch_mismatch_error};
use crate::py::index::{
    AABB, Arc, BinaryHeap, Bounds, CapBound, Crs, DistanceUnit, Frame, GeodesicCapsCache,
    GeodesicPruner, GeodesicRowCaps, IndexEntry, IndexEnvelope, IndexPredicate, NearestCandidate,
    NonNegative, Point, PointRows, Predicate, PreparedIndexQuery, PyGeometry, PyGeometryArray,
    PyResult, PySpatialIndex, RTreeNode, RowMatches, Shape, ShapeData, ShapeRow, XY,
    bounds_envelope, collect_subtree_ids, convex_box_strictly_inside, crs, crs_label, epoch_label,
    global_geographic_candidate_envelope, index_envelope, nearest_candidates_from_heap,
    pair_distance_resolved, pair_dwithin_resolved, point_from_bounds, point_index_envelope,
    push_nearest_candidate, retain_row, scalar_vs_shapes, sort_row_ids, topology_scalar_pair,
};

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq)]
enum DwithinQueryBounds {
    NotObserved,
    Empty,
    Value(Bounds),
}

#[cfg(test)]
thread_local! {
    // The transient query handle is local to exact refinement, so this records
    // the boundary's cache state for the regression test below.
    static DWITHIN_QUERY_BOUNDS: Cell<DwithinQueryBounds> = const { Cell::new(DwithinQueryBounds::NotObserved) };
}

impl PySpatialIndex {
    /// Every live entry — the STR bulk (tombstones skipped) then the
    /// dynamic overflow.
    pub(crate) fn live_entries(&self) -> impl Iterator<Item = &IndexEntry> {
        self.bulk.live_entries().chain(self.overflow.iter())
    }

    /// Append every live handle whose envelope intersects `query`.
    pub(crate) fn collect_intersecting(&self, query: &AABB<[f64; 2]>, out: &mut Vec<usize>) {
        self.bulk.each_intersecting(query, |idx| out.push(idx));
        out.extend(
            self.overflow
                .locate_in_envelope_intersecting(*query)
                .map(|entry| entry.idx),
        );
    }

    /// Append every live handle whose envelope is contained in `query`.
    pub(crate) fn collect_contained(&self, query: &AABB<[f64; 2]>, out: &mut Vec<usize>) {
        self.bulk.each_contained(query, |idx| out.push(idx));
        out.extend(
            self.overflow
                .locate_in_envelope(*query)
                .map(|entry| entry.idx),
        );
    }

    /// Append every live handle whose envelope lies within squared distance
    /// `max_distance_2` of `point`.
    pub(crate) fn collect_within_distance_2(
        &self,
        point: [f64; 2],
        max_distance_2: f64,
        out: &mut Vec<usize>,
    ) {
        self.bulk
            .each_within_distance_2(point, max_distance_2, |idx| out.push(idx));
        out.extend(
            self.overflow
                .locate_within_distance(point, max_distance_2)
                .map(|entry| entry.idx),
        );
    }

    /// Walk every unordered live pair `(i, j)` with `i < j` whose geometries
    /// satisfy the symmetric `predicate` — the self-join engine behind
    /// `self_join`.
    pub(crate) fn for_each_symmetric_pair(
        &self,
        predicate: IndexPredicate,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
        mut found: impl FnMut(usize, usize),
    ) -> PyResult<()> {
        let spec = match predicate {
            IndexPredicate::Topological(predicate) => Some(predicate.spec()),
            IndexPredicate::Dwithin => None,
        };
        let plan = PreparedIndexQuery::for_index(self, Some(predicate), distance, unit)?;
        let resolved = match &plan {
            PreparedIndexQuery::Dwithin { resolved, .. } => Some(resolved),
            _ => None,
        };
        let radius = distance.map_or(0.0, NonNegative::get);
        let mut live: Vec<usize> = self.live_entries().map(|entry| entry.idx).collect();
        sort_row_ids(&mut live, self.rows.len());
        let mut candidates = Vec::new();
        for &i in &live {
            // Symmetric relation: keep only j > i *before* the exact refine,
            // so each unordered pair is refined exactly once.
            let row = self.rows.row(i);
            let prepared = plan.row(self, row, crate::array::BoundsSeed::Unset);
            let (predicate, distance, metric) = plan.candidate_parts();
            self.candidate_ids_core(
                prepared.bounds,
                prepared.pruner_point,
                prepared.cap,
                predicate,
                distance,
                metric,
                &mut candidates,
            );
            candidates.retain(|&j| j > i);
            match (spec, resolved) {
                (Some(spec), _) => {
                    let query = self.rows.prepared_row(i);
                    retain_row(&mut candidates, 0, |idx| {
                        let data = self.rows.prepared_row(idx);
                        Ok(topology_scalar_pair(
                            &spec,
                            &query,
                            &data,
                            self.geographic(),
                        ))
                    })?;
                },
                (None, Some(resolved)) => {
                    let query = self.rows.prepared_row(i);
                    let query_cache = self.rows.frame_cache(i);
                    retain_row(&mut candidates, 0, |idx| {
                        let data = self.rows.prepared_row(idx);
                        pair_dwithin_resolved(
                            resolved,
                            &data,
                            &self.rows.frame_cache(idx),
                            &query,
                            &query_cache,
                            radius,
                        )
                    })?;
                },
                (None, None) => unreachable!("dwithin self-join resolves its metric once"),
            }
            sort_row_ids(&mut candidates, self.rows.len());
            for &j in &candidates {
                found(i, j);
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
    ) -> Option<Arc<GeodesicRowCaps>> {
        let crs::MetricModel::Geodesic(crs) = metric else {
            return None;
        };
        let runtime_gen = crs::runtime_config_generation();
        let mut cache = self
            .geodesic_caps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache.row_count == self.rows.len()
            && cache.generation == self.mutation_gen
            && cache.runtime_generation == runtime_gen
            && self.rows.len() > 0
        {
            return cache.caps.clone();
        }
        // Resolve the ellipsoid OUTSIDE the metric scope: `with_geodesic`
        // holds the thread-local handle cache borrow across its closure.
        let Ok(shape) = crs::ellipsoid_shape(crs) else {
            *cache = GeodesicCapsCache {
                row_count: self.rows.len(),
                generation: self.mutation_gen,
                runtime_generation: runtime_gen,
                caps: None,
            };
            return None;
        };
        let built = crs::with_ellipsoid_metric(crs, &[], |ellipsoid| {
            Ok(GeodesicRowCaps::from_live_handles(
                shape,
                self.rows.len(),
                self.live_entries().map(|entry| {
                    let (semi_major, flattening) = ellipsoid.ellipsoid_parameters();
                    (entry.idx, {
                        let data = self.rows.prepared_row(entry.idx);
                        data.geodesic_cap_cached(
                            &self.rows.frame_cache(entry.idx),
                            crs,
                            semi_major,
                            flattening,
                            ellipsoid,
                        )
                    })
                }),
            ))
        })
        .ok()
        .flatten()
        .map(Arc::new);
        *cache = GeodesicCapsCache {
            row_count: self.rows.len(),
            generation: self.mutation_gen,
            runtime_generation: runtime_gen,
            caps: built.clone(),
        };
        built
    }

    /// Cap-ordered geodesic nearest over non-point rows: sound per-row
    /// lower bounds order the exact evaluations and stop the scan once no
    /// bound can beat the running k-th best (or `max_distance`). `None`
    /// when the cap table or the query cap cannot be built — the exact
    /// full scan owns those (including their domain errors).
    pub(crate) fn geodesic_capped_nearest(
        &self,
        metric: &crs::MetricModel,
        query: &ShapeData,
        query_cache: &crate::geometry::FrameDependentCaches,
        k: usize,
        max_distance: Option<NonNegative>,
        exclude: Option<&Shape>,
    ) -> PyResult<Option<Vec<NearestCandidate>>> {
        let Some(caps) = self.geodesic_row_caps(metric) else {
            return Ok(None);
        };
        let crs::MetricModel::Geodesic(crs) = metric else {
            return Ok(None);
        };
        let Some((anchor, reach)) = crs::with_ellipsoid_metric(crs, &[], |ellipsoid| {
            let (semi_major, flattening) = ellipsoid.ellipsoid_parameters();
            Ok(query.geodesic_cap_cached(query_cache, crs, semi_major, flattening, ellipsoid))
        })
        .ok()
        .flatten() else {
            return Ok(None);
        };
        let Some(query_anchor) = caps.query_anchor(anchor) else {
            return Ok(None);
        };
        let order: Vec<CapBound> = self
            .live_entries()
            .map(|entry| CapBound {
                bound: caps.lower_bound(query_anchor, reach, entry.idx),
                idx: entry.idx,
            })
            .collect();
        let mut order = BinaryHeap::from(order);
        let resolved = crs::ResolvedMetric::from_model(metric)?;
        let mut nearest: BinaryHeap<NearestCandidate> =
            BinaryHeap::with_capacity(k.min(self.rows.len() + 1));
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
            let row = self.rows.row(idx);
            if exclude.is_some_and(|shape| row.with_shape(|other| *other == *shape)) {
                continue;
            }
            let data = self.rows.prepared_row(idx);
            let distance = pair_distance_resolved(
                &resolved,
                &data,
                &self.rows.frame_cache(idx),
                query,
                query_cache,
            )?;
            if max_distance.is_some_and(|max| distance > max.get()) {
                continue;
            }
            push_nearest_candidate(&mut nearest, k, NearestCandidate::new(distance, idx));
        }
        Ok(Some(nearest_candidates_from_heap(nearest)))
    }

    pub(crate) fn geodesic_pruner(
        &self,
        metric: &crs::MetricModel,
        query: Option<Point>,
    ) -> Option<GeodesicPruner> {
        let crs::MetricModel::Geodesic(crs) = metric else {
            return None;
        };
        if self.non_prunable_live != 0 {
            return None;
        }
        GeodesicPruner::new(crs, query?)
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
    ) {
        out.clear();
        self.candidate_ids_core_append(
            query_bounds,
            pruner_point,
            query_cap,
            predicate,
            distance,
            distance_model,
            out,
        );
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
    ) {
        let Some(query_bounds) = query_bounds else {
            return;
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
            self.collect_within_distance_2(query, distance_2, out);
            return;
        }
        if let Some(distance) = coordinate_distance {
            let query_envelope = bounds_envelope(query_bounds.expand(distance));
            self.collect_intersecting(&query_envelope, out);
            return;
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
                    self.collect_intersecting(window, out);
                }
                return;
            }
            // Cap-table fallback — non-point queries and non-point rows
            // both get a SOUND row filter (a superset of every true dwithin
            // match) instead of the all-rows envelope.
            if let Some((anchor, reach)) = query_cap
                && let Some(caps) = distance_model.and_then(|model| self.geodesic_row_caps(model))
                && let Some(query_anchor) = caps.query_anchor(anchor)
            {
                out.extend(
                    self.live_entries()
                        .filter(|entry| {
                            caps.lower_bound(query_anchor, reach, entry.idx) <= distance
                        })
                        .map(|entry| entry.idx),
                );
                return;
            }
            let query_envelope = global_geographic_candidate_envelope();
            self.collect_intersecting(&query_envelope, out);
            return;
        }
        let query_envelope = bounds_envelope(query_bounds);
        let containment = matches!(
            predicate,
            Some(IndexPredicate::Topological(predicate))
                if predicate.spec().index_envelope == Some(IndexEnvelope::ContainedInQuery)
        );
        if containment {
            self.collect_contained(&query_envelope, out);
        } else {
            self.collect_intersecting(&query_envelope, out);
        }
    }

    /// Candidate handles, sorted (the public `candidates` contract).
    pub(crate) fn candidate_ids_sorted(
        &self,
        geometry: &PyGeometry,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Vec<usize>> {
        let plan = PreparedIndexQuery::for_geometry(self, geometry, None, distance, unit)?;
        let row = plan.row(
            self,
            ShapeRow::Handle(&geometry.shape),
            crate::array::BoundsSeed::Unset,
        );
        let mut ids = Vec::new();
        let (predicate, distance, metric) = plan.candidate_parts();
        self.candidate_ids_core(
            row.bounds,
            row.pruner_point,
            row.cap,
            predicate,
            distance,
            metric,
            &mut ids,
        );
        sort_row_ids(&mut ids, self.rows.len());
        Ok(ids)
    }

    /// One ARRAY-row dwithin query against the index: the shared candidate
    /// core plus the pair kernel, all on stack handles (frame and metric
    /// resolved once per array by the caller). Shared by `query()`'s array
    /// lane and `join`'s left-row lane.
    pub(crate) fn dwithin_query_row_matches(
        &self,
        row: ShapeRow<'_>,
        query_cache: &crate::geometry::FrameDependentCaches,
        plan: &PreparedIndexQuery,
        matches: &mut Vec<usize>,
        seeded_bounds: crate::array::BoundsSeed,
    ) -> PyResult<()> {
        matches.clear();
        self.dwithin_query_row_matches_append(row, query_cache, plan, matches, seeded_bounds)
    }

    /// Append one dwithin result row directly to a flat CSR values column.
    pub(crate) fn dwithin_query_row_matches_append(
        &self,
        row: ShapeRow<'_>,
        query_cache: &crate::geometry::FrameDependentCaches,
        plan: &PreparedIndexQuery,
        matches: &mut Vec<usize>,
        seeded_bounds: crate::array::BoundsSeed,
    ) -> PyResult<()> {
        let row_start = matches.len();
        let prepared = plan.row(self, row, seeded_bounds);
        let (predicate, candidate_distance, metric) = plan.candidate_parts();
        self.candidate_ids_core_append(
            prepared.bounds,
            prepared.pruner_point,
            prepared.cap,
            predicate,
            candidate_distance,
            metric,
            matches,
        );
        let PreparedIndexQuery::Dwithin {
            resolved, distance, ..
        } = plan
        else {
            unreachable!("dwithin matcher is called only with a dwithin plan")
        };
        let query = crate::array::PreparedRow::transient_with_seed(row, seeded_bounds);
        #[cfg(test)]
        DWITHIN_QUERY_BOUNDS.with(|bounds| {
            bounds.set(query.bounds().map_or(DwithinQueryBounds::Empty, |bounds| {
                DwithinQueryBounds::Value(bounds)
            }));
        });
        retain_row(matches, row_start, |idx| {
            let data = self.rows.prepared_row(idx);
            pair_dwithin_resolved(
                resolved,
                &data,
                &self.rows.frame_cache(idx),
                &query,
                query_cache,
                distance.get(),
            )
        })?;
        sort_row_ids(&mut matches[row_start..], self.rows.len());
        Ok(())
    }

    /// Whether this index's shared frame is a geographic CRS — the single
    /// source of truth that drives antimeridian split-normalization in every
    /// topological refine path (never hardcoded). All rows and the query share
    /// this frame (`ensure_query_compatible`), so it is an index-level property.
    pub(crate) fn geographic(&self) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(crate::geometry::is_geographic_frame)
    }

    fn refine_prepared(
        &self,
        geometry: &PyGeometry,
        candidates: Vec<usize>,
        plan: &PreparedIndexQuery,
    ) -> PyResult<Vec<usize>> {
        let mut matches = candidates;
        match plan {
            PreparedIndexQuery::Topological { predicate } => {
                let spec = predicate.spec();
                // `query OP item`: the query is the fixed left operand of the
                // shared batch engine (prepared/cached exactly like the
                // broadcast surfaces); candidate handles feed it directly.
                retain_row(&mut matches, 0, |idx| {
                    let data = self.rows.prepared_row(idx);
                    Ok(topology_scalar_pair(
                        &spec,
                        &geometry.shape,
                        &data,
                        self.geographic(),
                    ))
                })?;
            },
            PreparedIndexQuery::Dwithin {
                resolved, distance, ..
            } => {
                let distance = distance.get();
                retain_row(&mut matches, 0, |idx| {
                    let data = self.rows.prepared_row(idx);
                    pair_dwithin_resolved(
                        resolved,
                        &data,
                        &self.rows.frame_cache(idx),
                        &geometry.shape,
                        &geometry.frame_cache,
                        distance,
                    )
                })?;
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
        if let IndexPredicate::Topological(predicate) = predicate
            && distance.is_none()
        {
            return Ok(self.topological_matches(&geometry.shape, predicate));
        }
        let prepared = plan.row(
            self,
            ShapeRow::Handle(&geometry.shape),
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
        );
        self.refine_prepared(geometry, candidates, &plan)
    }

    /// One topological query against a pre-checked SHAPE handle — the
    /// shared core of the scalar `query_exact` and the bulk array lane
    /// (which checks the frame ONCE and iterates storage rows with no
    /// per-row `PyGeometry` materialization). Distance-free by contract.
    pub(crate) fn topological_matches(
        &self,
        shape: &ShapeData,
        predicate: Predicate,
    ) -> Vec<usize> {
        let mut matches = Vec::new();
        self.topological_matches_append(shape, predicate, &mut matches);
        matches
    }

    /// Append one topological result row directly into a flat CSR values
    /// column — the array `query` lane's one-pass builder.
    pub(crate) fn topological_matches_append(
        &self,
        shape: &ShapeData,
        predicate: Predicate,
        out: &mut Vec<usize>,
    ) {
        let row_start = out.len();
        if matches!(
            predicate,
            Predicate::Contains
                | Predicate::Covers
                | Predicate::ContainsProperly
                | Predicate::Intersects
        ) && let Some(matches) =
            self.convex_containment_matches(shape, predicate, self.geographic())
        {
            out.extend(matches);
            sort_row_ids(&mut out[row_start..], self.rows.len());
            return;
        }
        let Some(query_bounds) = shape.bounds() else {
            return;
        };
        let spec = predicate.spec();
        let query_envelope = index_envelope(shape.shape(), query_bounds, self.geographic());
        if spec.index_envelope == Some(IndexEnvelope::ContainedInQuery) {
            self.collect_contained(&query_envelope, out);
        } else {
            self.collect_intersecting(&query_envelope, out);
        }
        let rows = out[row_start..]
            .iter()
            .map(|&idx| (idx, self.rows.row(idx)));
        // Collect the mask first: `scalar_vs_shapes` may re-enter row handles
        // that would conflict with a simultaneous mutable retain over `out`.
        let mask = scalar_vs_shapes(&spec, shape, rows, true, false, None, self.geographic());
        let mut mask = mask.into_iter();
        retain_row(out, row_start, |_| Ok(mask.next().unwrap_or(false)))
            .expect("topological refinement is infallible");
        sort_row_ids(&mut out[row_start..], self.rows.len());
    }

    /// Ground-up convex-query descent for a small CONVEX hole-free query
    /// polygon: ONE tree pass in place of candidates + per-pair
    /// refinement. A node whose MBR corners all sit STRICTLY inside the
    /// query accepts its WHOLE subtree (everything beneath is strictly
    /// interior — settling `contains`, `covers`, `contains_properly`, and
    /// `intersects` at once, with no boundary or dimension subtleties);
    /// nodes missing the query envelope prune; only boundary-straddling
    /// leaves pay the exact per-pair lane (whose `spec` decides the
    /// predicate). `None` when the query does not qualify.
    pub(crate) fn convex_containment_matches(
        &self,
        scalar: &ShapeData,
        predicate: Predicate,
        geographic: bool,
    ) -> Option<Vec<usize>> {
        // The MBR-descent accepts whole subtrees via planar corner tests, which
        // are unsound for an antimeridian-crossing query (its planar interior
        // spans the false middle). Fall back to candidates + gated refine.
        if geographic && scalar.shape().crosses_antimeridian() {
            return None;
        }
        if let Shape::Polygon(polygon) = scalar.shape()
            && !crate::geometry::uses_linear_plan_for_len(
                polygon.shell.coords().len().saturating_sub(1),
            )
        {
            return None;
        }
        let ccw = scalar.convex_shell()?;
        let Shape::Polygon(container) = scalar.shape() else {
            return None;
        };
        let Some(query_bounds) = scalar.bounds() else {
            return Some(Vec::new());
        };
        // Hoist the shell into a flat slice once; axis-aligned rectangular
        // queries (window/tile lookups — the overwhelmingly common case)
        // degrade the corner test to four strict float comparisons.
        let coords = container.shell.coords();
        let shell: Vec<XY> = coords
            .xs()
            .iter()
            .zip(coords.ys())
            .map(|(&x, &y)| XY::new(x, y))
            .collect();
        // Bit equality is the intent: rectangle edges replicate exact
        // coordinates (boxes are built from their bounds).
        let rectangular = shell.array_windows::<2>().all(|[start, end]| {
            start.x.to_bits() == end.x.to_bits() || start.y.to_bits() == end.y.to_bits()
        });
        let query_envelope = bounds_envelope(query_bounds);
        let inside = |envelope: &AABB<[f64; 2]>| {
            let (lower, upper) = (envelope.lower(), envelope.upper());
            if rectangular {
                lower[0] > query_bounds.minx()
                    && lower[1] > query_bounds.miny()
                    && upper[0] < query_bounds.maxx()
                    && upper[1] < query_bounds.maxy()
            } else {
                convex_box_strictly_inside(shell.as_slice(), ccw, [
                    lower[0], lower[1], upper[0], upper[1],
                ])
            }
        };
        let spec = predicate.spec();
        let mut matches = Vec::new();
        let accept_entry = |entry: &IndexEntry, matches: &mut Vec<usize>| {
            if !entry.envelope.intersects(&query_envelope) {
                return;
            }
            if inside(&entry.envelope) || {
                let element = self.rows.prepared_row(entry.idx);
                topology_scalar_pair(&spec, scalar, &element, geographic)
            } {
                matches.push(entry.idx);
            }
        };
        // Bulk STR descent.
        let root_level = self.bulk.root_level();
        let mut stack: Vec<(usize, u32)> = (0..self.bulk.roots().len() as u32)
            .map(|node| (root_level, node))
            .collect();
        while let Some((level, index)) = stack.pop() {
            let node = self.bulk.node(level, index);
            if !node.envelope.intersects(&query_envelope) {
                continue;
            }
            if inside(&node.envelope) {
                self.bulk.collect_subtree(level, node, &mut matches);
                continue;
            }
            if level == 0 {
                for entry in self.bulk.leaf_entries(node) {
                    if self.bulk.is_live(entry.idx) {
                        accept_entry(entry, &mut matches);
                    }
                }
            } else {
                stack.extend(node.children.clone().map(|child| (level - 1, child)));
            }
        }
        // Overflow R-tree descent (usually empty).
        let mut overflow: Vec<&RTreeNode<IndexEntry>> =
            self.overflow.root().children().iter().collect();
        while let Some(node) = overflow.pop() {
            match node {
                RTreeNode::Parent(parent) => {
                    let envelope = parent.envelope();
                    if !envelope.intersects(&query_envelope) {
                        continue;
                    }
                    if inside(&envelope) {
                        collect_subtree_ids(parent, &mut matches);
                    } else {
                        overflow.extend(parent.children().iter());
                    }
                },
                RTreeNode::Leaf(entry) => accept_entry(entry, &mut matches),
            }
        }
        Some(matches)
    }

    /// Column-direct engine for packed point query arrays: one frame check and
    /// one metric/spec resolution for the whole column, then per-point
    /// envelope lookups refined with the predicate's pair kernel — no per-row
    /// `PyGeometry` materialization. `predicate=None` returns candidates only.
    pub(crate) fn point_rows_matches(
        &self,
        array: &PyGeometryArray,
        points: &PointRows<'_>,
        missing: Option<&[bool]>,
        predicate: Option<IndexPredicate>,
        distance: Option<NonNegative>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<RowMatches> {
        let plan = PreparedIndexQuery::for_array(self, array, predicate, distance, unit)?;
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
        let geodesic_crs = match (model, distance) {
            (Some(crs::MetricModel::Geodesic(crs)), Some(_))
                if self.non_prunable_live == 0 && coordinate_distance.is_none() =>
            {
                Some(crs.clone())
            },
            _ => None,
        };
        // CSR-direct: every row appends into one flat ids buffer (refine
        // compacts the row window in place, then the window sorts in
        // place) — no per-point `Vec`.
        let mut ids: Vec<usize> = Vec::new();
        let mut offsets: Vec<usize> = Vec::with_capacity(points.len() + 1);
        offsets.push(0);
        // Point queries never cross, but index rows can: split-normalize a
        // crossing item before the predicate (resolved once for the array).
        let geographic = self.geographic();
        for (row, point) in points.iter().enumerate() {
            if missing.is_some_and(|mask| mask[row]) {
                offsets.push(ids.len());
                continue;
            }
            let query = ShapeData::new(Shape::Point(point));
            let row_start = ids.len();
            match (coordinate_distance, distance) {
                (Some(radius), _) => {
                    self.collect_within_distance_2([point.x, point.y], radius * radius, &mut ids);
                },
                (None, Some(distance)) => {
                    let distance = distance.get();
                    match geodesic_crs
                        .as_deref()
                        .and_then(|crs| GeodesicPruner::new(crs, point))
                        .and_then(|pruner| pruner.dwithin_windows(distance))
                    {
                        Some(windows) => {
                            for window in windows.iter() {
                                self.collect_intersecting(window, &mut ids);
                            }
                        },
                        None => ids.extend(self.live_entries().map(|entry| entry.idx)),
                    }
                },
                (None, None) => {
                    self.collect_intersecting(&point_index_envelope(point, geographic), &mut ids);
                },
            }
            match (&spec, model, predicate) {
                (Some(spec), ..) => retain_row(&mut ids, row_start, |idx| {
                    let data = self.rows.prepared_row(idx);
                    Ok(topology_scalar_pair(spec, &query, &data, geographic))
                })?,
                (None, Some(_), Some(IndexPredicate::Dwithin)) => {
                    let PreparedIndexQuery::Dwithin {
                        distance: limit,
                        resolved,
                        ..
                    } = &plan
                    else {
                        unreachable!("dwithin predicate constructs a dwithin plan")
                    };
                    let limit = limit.get();
                    retain_row(&mut ids, row_start, |idx| {
                        let data = self.rows.prepared_row(idx);
                        pair_dwithin_resolved(
                            resolved,
                            &query,
                            &array.row_frame_cache(row),
                            &data,
                            &self.rows.frame_cache(idx),
                            limit,
                        )
                    })?;
                },
                // Candidates-only: the envelope answer is the result.
                _ => {},
            }
            sort_row_ids(&mut ids[row_start..], self.rows.len());
            offsets.push(ids.len());
        }
        Ok(RowMatches { ids, offsets })
    }
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
            ShapeData::new(Shape::Point(Point::new_unchecked_xy(0.0, 0.0))),
            Frame::None,
        )])
        .unwrap();
        let array = PyGeometryArray::mixed_shapes(
            vec![Shape::LineString(LineSeq::from_trusted(
                CoordSeq::from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None),
            ))],
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

        DWITHIN_QUERY_BOUNDS.with(|bounds| bounds.set(DwithinQueryBounds::NotObserved));
        index
            .dwithin_query_row_matches_append(
                array.storage().row(0),
                &array.row_frame_cache(0),
                &plan,
                &mut matches,
                BoundsSeed::Unset,
            )
            .unwrap();

        assert_eq!(matches, [0]);
        assert_eq!(
            DWITHIN_QUERY_BOUNDS.with(Cell::get),
            DwithinQueryBounds::Value(Bounds::new_unchecked(0.0, 0.0, 1.0, 1.0))
        );
    }
}
