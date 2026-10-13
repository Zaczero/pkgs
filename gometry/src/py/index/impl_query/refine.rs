use super::{
    DistanceUnit, DwithinDistance, GateOperand, IndexPredicate, Predicate, PreparedIndexQuery,
    PreparedPlan, PreparedTopologyPair, PyGeometry, PyResult, PySpatialIndex, QueryDomain,
    ShapeRow, TopologyDomain, admitted_query, candidate_policy, crs, geographic_keep,
    pair_dwithin_geodesic, pair_dwithin_planar, retain_row, row_kind_empty,
    topology_pair_after_gates,
};

impl PySpatialIndex {
    pub(super) fn refine_prepared(
        &self,
        geometry: &PyGeometry,
        candidates: Vec<usize>,
        plan: &PreparedIndexQuery,
    ) -> PyResult<Vec<usize>> {
        let mut matches = candidates;
        let query_plan = geometry.prepared_plan();
        let domain = self.domain()?;
        let mut products = self.rows.operation();
        match plan {
            PreparedIndexQuery::Topological { predicate, .. } => {
                let spec = predicate.spec();
                // `query OP item`: the query is the fixed left operand of the
                // shared batch engine (prepared/cached exactly like the
                // broadcast surfaces); candidate handles feed it directly.
                let query_cache = geometry.frame_cache();
                let mut query_prepared = None;
                retain_row(&mut matches, 0, |idx| match domain {
                    TopologyDomain::Cartesian => {
                        if let Some(verdict) = spec.pre_topology(
                            &GateOperand::cartesian(
                                ShapeRow::Shape(query_plan.shape()),
                                query_plan.bounds(),
                            ),
                            &GateOperand::cartesian(products.row(idx)?, None),
                        )? {
                            return Ok(verdict);
                        }
                        let data = products.prepared(idx)?;
                        Ok(topology_pair_after_gates(
                            &spec,
                            &PreparedTopologyPair::Cartesian {
                                left: &query_plan,
                                right: &data,
                            },
                        )?)
                    },
                    TopologyDomain::Geographic(frame) => {
                        if let Some(verdict) = spec.predicate.kind_gate(
                            row_kind_empty(ShapeRow::Shape(query_plan.shape())).0,
                            row_kind_empty(products.row(idx)?).0,
                        ) {
                            return Ok(verdict);
                        }
                        let query =
                            admitted_query(&mut query_prepared, &query_plan, &query_cache, frame)?;
                        let data = products.prepared(idx)?;
                        geographic_keep(
                            &spec,
                            &query,
                            &crate::geometry::GeographicPrepared::try_new(
                                &data,
                                &*products.frame_cache(idx)?,
                                frame,
                            )?,
                        )
                    },
                })?;
            },
            PreparedIndexQuery::Dwithin {
                resolved, distance, ..
            } => {
                let distance = distance.get();
                match resolved {
                    crs::ResolvedMetric::Planar { to_metre } => {
                        retain_row(&mut matches, 0, |idx| {
                            let data = products.prepared(idx)?;
                            pair_dwithin_planar(to_metre.get(), &data, &query_plan, distance)
                                .map_err(Into::into)
                        })?;
                    },
                    crs::ResolvedMetric::Geodesic(metric) => retain_row(&mut matches, 0, |idx| {
                        let data = products.prepared(idx)?;
                        pair_dwithin_geodesic(
                            metric,
                            &data,
                            &*products.frame_cache(idx)?,
                            &query_plan,
                            &geometry.frame_cache(),
                            distance,
                        )
                        .map_err(Into::into)
                    })?,
                }
            },
            PreparedIndexQuery::Candidates { .. }
            | PreparedIndexQuery::CandidatesDwithin { .. } => {
                unreachable!("exact refinement is called only with a predicate plan")
            },
        }
        Ok(matches)
    }

    /// Candidate collection plus exact refine — the engine behind `query` and
    /// `join`.
    pub(crate) fn query_exact(
        &self,
        geometry: &PyGeometry,
        predicate: IndexPredicate,
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<Vec<usize>> {
        let plan =
            PreparedIndexQuery::for_geometry(self, geometry, Some(predicate), distance, unit)?;
        let query_plan = geometry.prepared_plan();
        if let IndexPredicate::Topological(predicate) = predicate
            && distance.is_none()
        {
            return self.topological_matches(
                &query_plan,
                &geometry.frame_cache(),
                predicate,
                plan.query_domain(),
                None,
            );
        }
        let prepared = plan.row(
            self,
            ShapeRow::Handle(&query_plan),
            crate::array::BoundsSeed::Unset,
            None,
        )?;
        let mut candidates = Vec::new();
        let (candidate_predicate, candidate_distance, metric) = plan.candidate_parts();
        self.candidate_ids_core(
            prepared.bounds,
            prepared.pruner_point,
            prepared.box_pruner,
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
        shape: &PreparedPlan,
        query_cache: &crate::geometry::FrameDependentCaches,
        predicate: Predicate,
        query_domain: QueryDomain,
        logical_row: Option<usize>,
    ) -> PyResult<Vec<usize>> {
        let mut matches = Vec::new();
        self.topological_matches_append(
            shape,
            query_cache,
            predicate,
            query_domain,
            logical_row,
            &mut matches,
        )?;
        Ok(matches)
    }

    /// Append one topological result row directly into a flat CSR values
    /// column — the array `query` lane's one-pass builder.
    pub(crate) fn topological_matches_append(
        &self,
        shape: &PreparedPlan,
        query_cache: &crate::geometry::FrameDependentCaches,
        predicate: Predicate,
        query_domain: QueryDomain,
        logical_row: Option<usize>,
        out: &mut Vec<usize>,
    ) -> PyResult<()> {
        let row_start = out.len();
        let Some(query_bounds) = shape.bounds() else {
            return Ok(());
        };
        query_domain.admit_shape(shape.shape(), Some(query_bounds), logical_row)?;
        let spec = predicate.spec();
        let Some(frame) = self.metadata.as_ref() else {
            return Ok(());
        };
        self.collect_cover_candidates(shape.shape(), query_bounds, candidate_policy(&spec), out)?;
        let domain = crate::geometry::topology_domain(frame)?;
        // The query is fixed for every candidate, so read its gate facts once.
        // A candidate the gate settles is never prepared at all.
        let mut query_prepared = None;
        let mut products = self.rows.operation();
        retain_row(out, row_start, |idx| match domain {
            TopologyDomain::Cartesian => {
                if let Some(verdict) = spec.pre_topology(
                    &GateOperand::cartesian(ShapeRow::Shape(shape.shape()), Some(query_bounds)),
                    &GateOperand::cartesian(products.row(idx)?, None),
                )? {
                    return Ok(verdict);
                }
                let data = products.prepared(idx)?;
                Ok(topology_pair_after_gates(
                    &spec,
                    &PreparedTopologyPair::Cartesian {
                        left: shape,
                        right: &data,
                    },
                )?)
            },
            TopologyDomain::Geographic(frame) => {
                if let Some(verdict) = spec.predicate.kind_gate(
                    row_kind_empty(ShapeRow::Shape(shape.shape())).0,
                    row_kind_empty(products.row(idx)?).0,
                ) {
                    return Ok(verdict);
                }
                let query = admitted_query(&mut query_prepared, shape, query_cache, frame)?;
                let data = products.prepared(idx)?;
                geographic_keep(
                    &spec,
                    &query,
                    &crate::geometry::GeographicPrepared::try_new(
                        &data,
                        &*products.frame_cache(idx)?,
                        frame,
                    )?,
                )
            },
        })?;
        Ok(())
    }
}
