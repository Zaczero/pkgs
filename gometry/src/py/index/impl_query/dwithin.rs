use super::{
    BatchResult, Bounds, CsrBuilder, DwithinDistance, GeodesicPruner, IndexRowOperation, PointRows,
    PreparedIndexQuery, PreparedPlan, PySpatialIndex, QueryDomain, RowMatches, Shape, ShapeRow,
    ValidityMask, crs, pair_dwithin_geodesic, pair_dwithin_planar, retain_row,
};
use crate::array::ArrayRowOperation;

impl PySpatialIndex {
    /// One ARRAY-row dwithin query against the index: the shared candidate
    /// core plus the pair kernel, all on stack handles (frame and metric
    /// resolved once per array by the caller). Shared by `query()`'s array
    /// lane and `join`'s left-row lane.
    pub(crate) fn planar_dwithin_query_row_matches<'a>(
        &self,
        row: ShapeRow<'a>,
        plan: &PreparedIndexQuery,
        matches: &mut Vec<usize>,
        seeded_bounds: crate::array::BoundsSeed,
        logical_row: usize,
        query_rows: &mut ArrayRowOperation<'a>,
    ) -> BatchResult<()> {
        matches.clear();
        self.planar_dwithin_query_row_matches_append(
            row,
            plan,
            matches,
            seeded_bounds,
            logical_row,
            query_rows,
        )
    }

    /// Append one dwithin result row directly to a flat CSR values column.
    pub(crate) fn planar_dwithin_query_row_matches_append<'a>(
        &self,
        row: ShapeRow<'a>,
        plan: &PreparedIndexQuery,
        matches: &mut Vec<usize>,
        seeded_bounds: crate::array::BoundsSeed,
        logical_row: usize,
        query_rows: &mut ArrayRowOperation<'a>,
    ) -> BatchResult<()> {
        let row_start = matches.len();
        let prepared = plan.row(self, row, seeded_bounds, Some(logical_row))?;
        let (predicate, candidate_distance, candidate_metric) = plan.candidate_parts();
        self.candidate_ids_core_append(
            prepared.bounds,
            prepared.pruner_point,
            prepared.box_pruner,
            predicate,
            candidate_distance,
            candidate_metric,
            matches,
        );
        if matches.len() == row_start {
            return Ok(());
        }
        let PreparedIndexQuery::Dwithin {
            resolved, distance, ..
        } = plan
        else {
            unreachable!("dwithin matcher is called only with a dwithin plan")
        };
        let query = query_rows.warm_prepared_at_with_seed(logical_row, row, seeded_bounds)?;
        let crs::ResolvedMetric::Planar { to_metre } = resolved else {
            unreachable!("planar dwithin runner receives a planar query")
        };
        let mut products = self.rows.operation();
        retain_row(matches, row_start, |idx| {
            let mut source = products.resolve(idx)?;
            let data = source.prepared()?;
            pair_dwithin_planar(to_metre.get(), &data, &query, distance.get()).map_err(Into::into)
        })?;
        Ok(())
    }

    pub(crate) fn geodesic_dwithin_query_row_matches_append<'a>(
        &self,
        row: ShapeRow<'a>,
        metric: &crs::GeodesicMetric,
        plan: &PreparedIndexQuery,
        matches: &mut Vec<usize>,
        seeded_bounds: crate::array::BoundsSeed,
        logical_row: usize,
        query_rows: &mut ArrayRowOperation<'a>,
    ) -> BatchResult<()> {
        let row_start = matches.len();
        let prepared = plan.row(self, row, seeded_bounds, Some(logical_row))?;
        let (predicate, candidate_distance, candidate_metric) = plan.candidate_parts();
        self.candidate_ids_core_append(
            prepared.bounds,
            prepared.pruner_point,
            prepared.box_pruner,
            predicate,
            candidate_distance,
            candidate_metric,
            matches,
        );
        if matches.len() == row_start {
            return Ok(());
        }
        let PreparedIndexQuery::Dwithin { distance, .. } = plan else {
            unreachable!("dwithin matcher is called only with a dwithin plan")
        };
        let query = query_rows.warm_prepared_at_with_seed(logical_row, row, seeded_bounds)?;
        let query_cache = query_rows.frame_cache(logical_row);
        let mut products = self.rows.operation();
        retain_row(matches, row_start, |idx| {
            let mut source = products.resolve(idx)?;
            let data = source.prepared()?;
            let candidate_cache = source.frame_cache();
            pair_dwithin_geodesic(
                metric,
                &data,
                &candidate_cache,
                &query,
                &query_cache,
                distance.get(),
            )
            .map_err(Into::into)
        })?;
        Ok(())
    }

    pub(super) fn point_rows_dwithin(
        &self,
        points: &PointRows<'_>,
        missing: Option<&ValidityMask>,
        distance: DwithinDistance,
        model: &crs::MetricModel,
        query_domain: QueryDomain,
        query_rows: &mut ArrayRowOperation<'_>,
        mut refine: impl FnMut(
            usize,
            &PreparedPlan,
            &mut Vec<usize>,
            usize,
            &mut IndexRowOperation<'_>,
            &mut ArrayRowOperation<'_>,
        ) -> BatchResult<()>,
    ) -> BatchResult<RowMatches> {
        let coordinate_distance = model.coordinate_radius(distance.get());
        let geodesic_pruner = match model {
            crs::MetricModel::Geodesic { crs, frame, state }
                if self.non_prunable_live == 0 && coordinate_distance.is_none() =>
            {
                (*frame == crate::geographic::GeographicFrame::crs84()).then_some((crs, state))
            },
            crs::MetricModel::Planar { .. } | crs::MetricModel::Geodesic { .. } => None,
        };
        let mut builder = CsrBuilder::<usize>::with_capacity(0, points.len());
        let mut products = self.rows.operation();
        for (row, point) in points.iter().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                let end = builder.len();
                builder.push_end(end)?;
                continue;
            }
            query_domain.admit_shape(
                &Shape::Point(point),
                Some(Bounds::from_point(point)),
                Some(row),
            )?;
            let row_start = builder.len();
            if distance.get() == f64::INFINITY {
                self.append_live_handles(builder.values_mut());
            } else if let Some(radius) = coordinate_distance {
                self.collect_within_distance_2(
                    [point.x, point.y],
                    radius * radius,
                    builder.values_mut(),
                );
            } else if let Some((crs, state)) = geodesic_pruner {
                if let Some(windows) = GeodesicPruner::new_at(crs, point, state)
                    .and_then(|pruner| pruner.dwithin_windows(distance.get()))
                {
                    for window in windows.iter() {
                        self.collect_intersecting(window, builder.values_mut());
                    }
                } else {
                    self.append_live_handles(builder.values_mut());
                }
            } else {
                self.append_live_handles(builder.values_mut());
            }
            self.finish_candidates(builder.values_mut(), row_start);
            if builder.len() != row_start {
                let query_bounds = crate::array::BoundsSeed::Value(Some(Bounds::from_point(point)));
                let query = query_rows.warm_prepared_at_with_seed(
                    row,
                    ShapeRow::Point(point),
                    query_bounds,
                )?;
                refine(
                    row,
                    &query,
                    builder.values_mut(),
                    row_start,
                    &mut products,
                    query_rows,
                )?;
            }
            let end = builder.len();
            builder.push_end(end)?;
        }
        let (ids, offsets) = builder.finish();
        Ok(RowMatches { ids, offsets })
    }
}
