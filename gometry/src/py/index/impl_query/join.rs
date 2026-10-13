use super::{
    Bounds, CsrBuilder, DistanceUnit, DwithinDistance, GateOperand, IndexPredicate, PointRows,
    Predicate, PreparedIndexQuery, PreparedPlan, PreparedTopologyPair, PyGeometryArray, PyResult,
    PySpatialIndex, QueryDomain, RowMatches, Shape, ShapeData, ShapeRow, TopologyDomain,
    ValidityMask, admitted_query, candidate_policy, crs, csr_rows, geographic_keep,
    pair_dwithin_geodesic, pair_dwithin_planar, retain_row, row_kind_empty,
    topology_pair_after_gates, topology_scalar_pair,
};

impl PySpatialIndex {
    /// Visit each live row's matching higher handles for a symmetric predicate.
    pub(crate) fn for_each_symmetric_matches(
        &self,
        predicate: IndexPredicate,
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
        mut found: impl FnMut(usize, &[usize]),
    ) -> PyResult<()> {
        let spec = match predicate {
            IndexPredicate::Topological(predicate) => Some(predicate.spec()),
            IndexPredicate::Dwithin => None,
        };
        let plan = PreparedIndexQuery::for_index(self, Some(predicate), distance, unit)?;
        let domain = self.domain()?;
        let resolved = if let PreparedIndexQuery::Dwithin { resolved, .. } = &plan {
            Some(resolved)
        } else {
            None
        };
        let radius = distance.map_or(0.0, DwithinDistance::get);
        let mut candidates = Vec::new();
        let mut products = self.rows.operation();
        for i in self.rows.live_handles.iter() {
            // Symmetric relation: keep only j > i *before* the exact refine,
            // so each unordered pair is refined exactly once.
            let row = products.row(i)?;
            let prepared = plan.row(self, row, crate::array::BoundsSeed::Unset, None)?;
            let (predicate, distance, metric) = plan.candidate_parts();
            candidates.clear();
            if let Some(spec) = spec {
                let query = products.prepared(i)?;
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
                    prepared.box_pruner,
                    predicate,
                    distance,
                    metric,
                    &mut candidates,
                );
            }
            candidates.retain(|&j| j > i);
            match (spec, resolved) {
                (Some(spec), _) => {
                    let query = products.prepared(i)?;
                    let query_cache = products.frame_cache(i)?;
                    let mut query_prepared = None;
                    retain_row(&mut candidates, 0, |idx| match domain {
                        TopologyDomain::Cartesian => {
                            if let Some(verdict) = spec.pre_topology(
                                &GateOperand::cartesian(products.row(i)?, None),
                                &GateOperand::cartesian(products.row(idx)?, None),
                            )? {
                                return Ok(verdict);
                            }
                            let data = products.prepared(idx)?;
                            Ok(topology_pair_after_gates(
                                &spec,
                                &PreparedTopologyPair::Cartesian {
                                    left: &query,
                                    right: &data,
                                },
                            )?)
                        },
                        TopologyDomain::Geographic(frame) => {
                            if let Some(verdict) = spec.predicate.kind_gate(
                                row_kind_empty(products.row(i)?).0,
                                row_kind_empty(products.row(idx)?).0,
                            ) {
                                return Ok(verdict);
                            }
                            let query =
                                admitted_query(&mut query_prepared, &query, &query_cache, frame)?;
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
                (None, Some(resolved)) => {
                    let query = products.prepared(i)?;
                    match resolved {
                        crs::ResolvedMetric::Planar { to_metre } => {
                            retain_row(&mut candidates, 0, |idx| {
                                let data = products.prepared(idx)?;
                                pair_dwithin_planar(to_metre.get(), &data, &query, radius)
                                    .map_err(Into::into)
                            })?;
                        },
                        crs::ResolvedMetric::Geodesic(metric) => {
                            let query_cache = products.frame_cache(i)?;
                            retain_row(&mut candidates, 0, |idx| {
                                let data = products.prepared(idx)?;
                                pair_dwithin_geodesic(
                                    metric,
                                    &data,
                                    &*products.frame_cache(idx)?,
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
            found(i, &candidates);
        }
        Ok(())
    }

    /// Refine packed point topological queries by indexed row. Candidate
    /// traversal remains point-driven, while every candidate is refined by
    /// the shared exact relation matrix.
    pub(super) fn point_rows_join_matches(
        &self,
        array: &PyGeometryArray,
        points: &PointRows<'_>,
        missing: Option<&ValidityMask>,
        predicate: Predicate,
        query_domain: QueryDomain,
    ) -> PyResult<RowMatches> {
        let domain = self.domain()?;
        let mut builder = CsrBuilder::<usize>::with_capacity(0, points.len());
        let mut queries = Vec::with_capacity(points.len());
        let mut query_rows = array.row_operation();
        for (row, point) in points.iter().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                let end = builder.len();
                builder.push_end(end).map_err(pyo3::PyErr::from)?;
                queries.push(None);
                continue;
            }
            query_domain.admit_shape(
                &Shape::Point(point),
                Some(Bounds::from_point(point)),
                Some(row),
            )?;
            let row_start = builder.len();
            self.collect_point_candidates(point, builder.values_mut())?;
            let end = builder.len();
            builder.push_end(end).map_err(pyo3::PyErr::from)?;
            let query = (end > row_start).then(|| {
                (
                    PreparedPlan::new(ShapeData::new(Shape::Point(point))),
                    query_rows.frame_cache(row),
                )
            });
            queries.push(query);
        }
        let (mut ids, offsets) = builder.finish();
        if ids.is_empty() {
            return Ok(RowMatches { ids, offsets });
        }

        let mut candidates = Vec::with_capacity(ids.len());
        let mut position = 0_usize;
        for (row, row_ids) in csr_rows(&ids, &offsets).enumerate() {
            for &idx in row_ids {
                candidates.push((idx, row, position));
                position = position.wrapping_add(1);
            }
        }
        debug_assert_eq!(position, ids.len());
        // Refinement is indexed-row-local, while the tuple tie-breakers retain
        // the candidate's original read order within that row.
        candidates.sort_unstable_by_key(|candidate| *candidate);

        let spec = predicate.spec();
        let mut keep = vec![false; ids.len()];
        let mut query_proofs = if matches!(domain, TopologyDomain::Geographic(_)) {
            vec![None; queries.len()]
        } else {
            Vec::new()
        };
        let mut index_products = self.rows.operation();
        let mut candidates = candidates.iter().peekable();
        while let Some(first) = candidates.next() {
            let idx = first.0;
            let data = index_products.prepared(idx)?;
            let candidate_cache = index_products.frame_cache(idx)?;
            let mut candidate_proof = None;
            let mut refine = |candidate: &(usize, usize, usize)| -> PyResult<()> {
                let (_, row, position) = candidate;
                let (query, query_cache) = queries
                    .get(*row)
                    .and_then(Option::as_ref)
                    .expect("every grouped point row has a candidate plan");
                let keep_match = match domain {
                    TopologyDomain::Cartesian => topology_scalar_pair(
                        &spec,
                        &PreparedTopologyPair::try_for_domain(
                            query,
                            query_cache,
                            &data,
                            &*index_products.frame_cache(idx)?,
                            domain,
                        )?,
                    )?,
                    TopologyDomain::Geographic(frame) => {
                        if let Some(verdict) = spec.predicate.kind_gate(
                            crate::geometry::GeometryKind::Point,
                            row_kind_empty(ShapeRow::Shape(data.shape())).0,
                        ) {
                            verdict
                        } else {
                            let query_proof = query_proofs
                                .get_mut(*row)
                                .expect("geographic query proofs align with rows");
                            let query = admitted_query(query_proof, query, query_cache, frame)?;
                            let candidate = admitted_query(
                                &mut candidate_proof,
                                &data,
                                &candidate_cache,
                                frame,
                            )?;
                            geographic_keep(&spec, &query, &candidate)?
                        }
                    },
                };
                *keep
                    .get_mut(*position)
                    .expect("candidate position belongs to the CSR values") = keep_match;
                Ok(())
            };
            refine(first)?;
            while let Some(candidate) = candidates.next_if(|candidate| candidate.0 == idx) {
                refine(candidate)?;
            }
        }

        let mut refined_ids = 0_usize;
        let mut refined_offsets = CsrBuilder::<()>::with_capacity(0, points.len());
        let mut keep_rows = keep.iter().copied();
        for row_ids in csr_rows(&ids, &offsets) {
            let kept_in_row = keep_rows
                .by_ref()
                .take(row_ids.len())
                .filter(|&keep| keep)
                .count();
            refined_ids = refined_ids.wrapping_add(kept_in_row);
            refined_offsets
                .push_end(refined_ids)
                .map_err(pyo3::PyErr::from)?;
        }
        debug_assert_eq!(keep_rows.len(), 0);
        let mut keep_values = keep.into_iter();
        ids.retain(|_| {
            keep_values
                .next()
                .expect("keep mask aligns with CSR values")
        });
        debug_assert_eq!(keep_values.len(), 0);
        let (_, refined_offsets) = refined_offsets.finish();
        Ok(RowMatches {
            ids,
            offsets: refined_offsets,
        })
    }
}
