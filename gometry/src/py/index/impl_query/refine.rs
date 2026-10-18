use super::{
    BatchResult, DistanceUnit, DwithinDistance, IndexPredicate, Predicate, PreparedIndexQuery,
    PreparedPlan, PyGeometry, PyGeometryArray, PySpatialIndex, QueryDomain, ShapeRow,
    TopologyDomain, admitted_query, candidate_policy, crs, pair_dwithin_geodesic,
    pair_dwithin_planar, retain_row,
};
use crate::CsrBuilder;
use crate::py::vectors::Groups;

impl PySpatialIndex {
    pub(super) fn refine_prepared(
        &self,
        geometry: &PyGeometry,
        candidates: Vec<usize>,
        plan: &PreparedIndexQuery,
    ) -> BatchResult<Vec<usize>> {
        let mut matches = candidates;
        let query_plan = geometry.prepared_plan();
        let domain = self.domain()?;
        let mut products = self.rows.operation();
        match plan {
            PreparedIndexQuery::Topological { predicate, .. } => {
                let spec = predicate.spec();
                let query_cache = geometry.frame_cache();
                retain_topological(
                    &query_plan,
                    &query_cache,
                    &spec,
                    domain,
                    &mut products,
                    &mut matches,
                    0,
                )?;
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
    ) -> BatchResult<Vec<usize>> {
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
    ) -> BatchResult<Vec<usize>> {
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
    ) -> BatchResult<()> {
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
        let mut products = self.rows.operation();
        retain_topological(
            shape,
            query_cache,
            &spec,
            domain,
            &mut products,
            out,
            row_start,
        )?;
        Ok(())
    }
}

pub(super) fn retain_topological(
    query: &PreparedPlan,
    query_cache: &crate::geometry::FrameDependentCaches,
    spec: &super::PredicateSpec,
    domain: TopologyDomain,
    products: &mut super::IndexRowOperation<'_>,
    matches: &mut Vec<usize>,
    row_start: usize,
) -> BatchResult<()> {
    let evaluator = crate::predicates::engine::FixedPredicate::new(spec, query, true);
    let element_count = matches.len().wrapping_sub(row_start);
    let mut query_prepared = None;
    retain_row(matches, row_start, |idx| {
        let row = products.row(idx)?;
        match domain {
            TopologyDomain::Cartesian => {
                Ok(evaluator.cartesian(row, None, element_count, || products.prepared(idx))?)
            },
            TopologyDomain::Geographic(frame) => {
                if let Some(verdict) = evaluator.kind_gate(row) {
                    return Ok(verdict);
                }
                let query = admitted_query(&mut query_prepared, query, query_cache, frame)?;
                let data = products.prepared(idx)?;
                let cache = products.frame_cache(idx)?;
                Ok(evaluator.geographic(
                    query,
                    crate::geometry::GeographicPrepared::try_new(&data, &cache, frame)?,
                )?)
            },
        }
    })
}

impl PySpatialIndex {
    pub(crate) fn query_array(
        &self,
        array: &PyGeometryArray,
        predicate: IndexPredicate,
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> BatchResult<Groups> {
        if !array.is_compact_nullable()
            && let Some(points) = array.storage().point_rows()
        {
            let results = self.point_rows_matches(
                array,
                &points,
                array.missing(),
                Some(predicate),
                distance,
                unit,
            )?;
            return Ok(Groups::from_int64_csr(results.ids, results.offsets)?
                .with_row_missing(array.missing().cloned()));
        }
        let plan = PreparedIndexQuery::for_array(self, array, Some(predicate), distance, unit)?;
        if let IndexPredicate::Topological(topological) = predicate
            && distance.is_none()
        {
            // One frame check for the whole array; rows feed the
            // shape-level core with no per-row wrapper. Matches append
            // straight into the CSR values column (one pass).
            let missing = array.missing().cloned();
            let (ids, offsets) = {
                let row_count = array.len();
                let mut builder = CsrBuilder::<usize>::with_capacity(0, row_count);
                let mut rows = array.row_operation();
                for logical_row in rows.rows_with_missing(|| ()) {
                    if let crate::array::ArrayRow::Present(row) = logical_row {
                        let row_index = row.logical();
                        let shape = rows.prepared(row)?;
                        self.topological_matches_append(
                            &shape,
                            &rows.frame_cache(row_index),
                            topological,
                            plan.query_domain(),
                            Some(row_index),
                            builder.values_mut(),
                        )?;
                    }
                    let end = builder.len();
                    builder.push_end(end)?;
                }
                builder.finish()
            };
            return Ok(Groups::from_int64_csr(ids, offsets)?.with_row_missing(missing));
        }
        // dwithin array queries (the only distance-carrying lane left):
        // one frame check + one metric resolution; rows run the shared
        // candidate core and the pair kernel on stack handles — no
        // per-row PyGeometry staging.
        let missing = array.missing().cloned();
        let (ids, offsets) = {
            let row_count = array.len();
            let mut builder = CsrBuilder::<usize>::with_capacity(0, row_count);
            let PreparedIndexQuery::Dwithin { resolved, .. } = &plan else {
                unreachable!("dwithin array query constructs a dwithin plan")
            };
            match resolved {
                crate::crs::ResolvedMetric::Planar { .. } => {
                    let rows = array.row_operation();
                    for logical_row in rows.rows_with_missing(|| ()) {
                        if let crate::array::ArrayRow::Present(row) = logical_row {
                            let row_index = row.logical();
                            let row = row.row();
                            self.planar_dwithin_query_row_matches_append(
                                row,
                                &plan,
                                builder.values_mut(),
                                array.row_bounds_seed(row_index),
                                Some(row_index),
                            )?;
                        }
                        let end = builder.len();
                        builder.push_end(end)?;
                    }
                },
                crate::crs::ResolvedMetric::Geodesic(metric) => {
                    let mut rows = array.row_operation();
                    for logical_row in rows.rows_with_missing(|| ()) {
                        if let crate::array::ArrayRow::Present(row) = logical_row {
                            let row_index = row.logical();
                            let row = row.row();
                            let mut query_cache = || rows.frame_cache(row_index);
                            self.geodesic_dwithin_query_row_matches_append(
                                row,
                                metric,
                                &mut query_cache,
                                &plan,
                                builder.values_mut(),
                                array.row_bounds_seed(row_index),
                                Some(row_index),
                            )?;
                        }
                        let end = builder.len();
                        builder.push_end(end)?;
                    }
                },
            }
            builder.finish()
        };
        Ok(Groups::from_int64_csr(ids, offsets)?.with_row_missing(missing))
    }
}
