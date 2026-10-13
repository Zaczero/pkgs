use super::{
    AABB, Bounds, DistanceUnit, DwithinDistance, GeodesicBoxPruner, IndexEntry, IndexEnvelope,
    IndexPredicate, Point, PredicateSpec, PreparedIndexQuery, PyGeometry, PyResult, PySpatialIndex,
    Shape, ShapeRow, bounds_envelope, crs, global_geographic_candidate_envelope, index_envelopes,
    point_from_bounds, sort_unique_row_ids,
};

impl PySpatialIndex {
    /// Every live entry — the STR bulk (row liveness skipped) then the
    /// immutable overflow runs.
    pub(crate) fn live_entries(&self) -> impl Iterator<Item = &IndexEntry> {
        self.bulk
            .live_entries(&self.rows.live_handles)
            .chain(self.overflow.live_entries(&self.rows.live_handles))
    }

    /// Append every live handle whose envelope intersects `query`.
    pub(crate) fn collect_intersecting(&self, query: &AABB<[f64; 2]>, out: &mut Vec<usize>) {
        self.bulk
            .append_intersecting(&self.rows.live_handles, query, out);
        self.overflow
            .append_intersecting(&self.rows.live_handles, query, out);
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
            .append_within_distance_2(&self.rows.live_handles, point, max_distance_2, out);
        self.overflow
            .append_within_distance_2(&self.rows.live_handles, point, max_distance_2, out);
    }

    /// Append the handles whose envelopes meet `bounds`, ascending and unique
    /// — the whole answer for a planar envelope query.
    pub(crate) fn envelope_candidates_append(&self, bounds: Bounds, out: &mut Vec<usize>) {
        let start = out.len();
        self.collect_intersecting(&bounds_envelope(bounds), out);
        self.finish_candidates(out, start);
    }

    /// Order the handles appended after `start` ascending, one per row — the
    /// form every candidate producer hands on. Retaining refinement keeps it.
    pub(super) fn finish_candidates(&self, out: &mut Vec<usize>, start: usize) {
        sort_unique_row_ids(out, start, self.rows.next_handle);
    }

    pub(super) fn append_live_handles(&self, out: &mut Vec<usize>) {
        self.rows.live_handles.append_ordered(out);
    }

    /// Candidate walk for one frame-checked query, shared by scalar and array
    /// lanes.
    pub(crate) fn candidate_ids_core(
        &self,
        query_bounds: Option<Bounds>,
        pruner_point: Option<Point>,
        box_pruner: Option<GeodesicBoxPruner>,
        predicate: Option<IndexPredicate>,
        distance: Option<DwithinDistance>,
        distance_model: Option<&crs::MetricModel>,
        out: &mut Vec<usize>,
    ) {
        out.clear();
        self.candidate_ids_core_append(
            query_bounds,
            pruner_point,
            box_pruner,
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
        box_pruner: Option<GeodesicBoxPruner>,
        _predicate: Option<IndexPredicate>,
        distance: Option<DwithinDistance>,
        distance_model: Option<&crs::MetricModel>,
        out: &mut Vec<usize>,
    ) {
        let start = out.len();
        self.candidate_ids_core_append_inner(
            query_bounds,
            pruner_point,
            box_pruner,
            distance,
            distance_model,
            out,
        );
        self.finish_candidates(out, start);
    }

    fn candidate_ids_core_append_inner(
        &self,
        query_bounds: Option<Bounds>,
        pruner_point: Option<Point>,
        box_pruner: Option<GeodesicBoxPruner>,
        distance: Option<DwithinDistance>,
        distance_model: Option<&crs::MetricModel>,
        out: &mut Vec<usize>,
    ) {
        let Some(query_bounds) = query_bounds else {
            return;
        };
        if distance.is_some_and(|distance| distance.get() == f64::INFINITY) {
            self.append_live_handles(out);
            return;
        }
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
            // Shape queries use their chart cover against each indexed row's
            // chart envelope. Invalid geodesic rows fail open so refinement
            // still reports the same coordinate-domain error.
            if self.non_prunable_live == 0
                && let Some(box_pruner) = box_pruner
            {
                for entry in self.live_entries() {
                    if box_pruner.lower_bound(&entry.envelope) <= distance {
                        out.push(entry.idx);
                    }
                }
                return;
            }
            let query_envelope = global_geographic_candidate_envelope();
            self.collect_intersecting(&query_envelope, out);
            return;
        }
        self.collect_intersecting(&bounds_envelope(query_bounds), out);
    }

    /// Candidate handles, sorted (the public `candidates` contract).
    pub(crate) fn candidate_ids_sorted(
        &self,
        geometry: &PyGeometry,
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Vec<usize>> {
        let plan = PreparedIndexQuery::for_geometry(self, geometry, None, distance, unit)?;
        let query_plan = geometry.prepared_plan();
        let row = plan.row(
            self,
            ShapeRow::Handle(&query_plan),
            crate::array::BoundsSeed::Unset,
            None,
        )?;
        let mut ids = Vec::new();
        if distance.is_none() {
            if let Some(bounds) = query_plan.bounds() {
                self.collect_shape_candidates(query_plan.shape(), bounds, &mut ids)?;
            }
            return Ok(ids);
        }
        let (predicate, distance, metric) = plan.candidate_parts();
        self.candidate_ids_core(
            row.bounds,
            row.pruner_point,
            row.box_pruner,
            predicate,
            distance,
            metric,
            &mut ids,
        );
        Ok(ids)
    }

    pub(super) fn collect_point_candidates(
        &self,
        point: Point,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
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
    pub(super) fn collect_cover_candidates(
        &self,
        shape: &Shape,
        bounds: Bounds,
        policy: IndexEnvelope,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let start = out.len();
        let cover = index_envelopes(shape, bounds, self.domain()?)?;
        for envelope in cover.windows(policy) {
            self.collect_intersecting(envelope, out);
        }
        self.finish_candidates(out, start);
        Ok(())
    }
}

/// The candidate lookup `spec` calls for. Admission rejects a predicate the
/// index cannot narrow, so every spec that reaches a query carries a rule.
pub(super) const fn candidate_policy(spec: &PredicateSpec) -> IndexEnvelope {
    spec.index_envelope
        .expect("the index rejects a predicate it cannot accelerate")
}
