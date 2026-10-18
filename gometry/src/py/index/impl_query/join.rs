use super::{
    BatchResult, Bounds, CsrBuilder, DistanceUnit, DwithinDistance, IndexPredicate, PointRows,
    Predicate, PreparedIndexQuery, PreparedPlan, PyGeometryArray, PySpatialIndex, QueryDomain,
    RowMatches, Shape, ShapeData, ShapeRow, TopologyDomain, ValidityMask, admitted_query,
    candidate_policy, crs, csr_rows, pair_dwithin_geodesic, pair_dwithin_planar, retain_row,
};

impl PySpatialIndex {
    /// Collect each unordered matching pair once, in live-handle order.
    pub(crate) fn self_join_pairs(
        &self,
        predicate: IndexPredicate,
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> BatchResult<(Vec<i64>, Vec<i64>)> {
        let mut left = Vec::new();
        let mut right = Vec::new();
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
                    super::refine::retain_topological(
                        &query,
                        &query_cache,
                        &spec,
                        domain,
                        &mut products,
                        &mut candidates,
                        0,
                    )?;
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
            super::super::free_functions::append_join_matches(
                &mut left,
                &mut right,
                i,
                &candidates,
            );
        }
        Ok((left, right))
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
    ) -> BatchResult<RowMatches> {
        let domain = self.domain()?;
        let mut builder = CsrBuilder::<usize>::with_capacity(0, points.len());
        let mut queries = Vec::with_capacity(points.len());
        let mut query_rows = array.row_operation();
        for (row, point) in points.iter().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                let end = builder.len();
                builder.push_end(end)?;
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
            builder.push_end(end)?;
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
        for group in candidates.chunk_by(|left, right| left.0 == right.0) {
            let [(idx, ..), ..] = group else {
                unreachable!("chunk_by yields nonempty candidate groups");
            };
            let idx = *idx;
            let data = index_products.prepared(idx)?;
            let candidate_cache = index_products.frame_cache(idx)?;
            let mut candidate_proof = None;
            let evaluator = crate::predicates::engine::FixedPredicate::new(&spec, &data, false);
            for (_, row, position) in group {
                let (query, query_cache) = queries
                    .get(*row)
                    .and_then(Option::as_ref)
                    .expect("every grouped point row has a candidate plan");
                let keep_match = match domain {
                    TopologyDomain::Cartesian => evaluator.cartesian(
                        ShapeRow::Shape(query.shape()),
                        query.bounds(),
                        group.len(),
                        || Ok(query),
                    )?,
                    TopologyDomain::Geographic(frame) => {
                        if let Some(verdict) = evaluator.kind_gate(ShapeRow::Shape(query.shape())) {
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
                            evaluator.geographic(candidate, query)?
                        }
                    },
                };
                *keep
                    .get_mut(*position)
                    .expect("candidate position belongs to the CSR values") = keep_match;
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
            refined_offsets.push_end(refined_ids)?;
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
