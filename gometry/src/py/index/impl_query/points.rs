use super::{
    Arc, BatchResult, Bounds, CsrBuilder, DistanceUnit, DwithinDistance, GeodesicPruner,
    IndexPredicate, PointRows, PreparedIndexQuery, PyGeometryArray, PySpatialIndex, RowMatches,
    Shape, ValidityMask, crs, pair_dwithin_geodesic, pair_dwithin_planar, retain_row,
};

impl PySpatialIndex {
    /// Column-direct engine for packed point query arrays. Exact topology and
    /// dwithin use dedicated refinement runners; candidate-only queries stop
    /// after envelope collection without per-row `PyGeometry` materialization.
    pub(crate) fn point_rows_matches(
        &self,
        array: &PyGeometryArray,
        points: &PointRows<'_>,
        missing: Option<&ValidityMask>,
        predicate: Option<IndexPredicate>,
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> BatchResult<RowMatches> {
        let plan = PreparedIndexQuery::for_array(self, array, predicate, distance, unit)?;
        if distance.is_none()
            && let Some(IndexPredicate::Topological(predicate)) = predicate
        {
            return self.point_rows_join_matches(
                array,
                points,
                missing,
                predicate,
                plan.query_domain(),
            );
        }
        if let PreparedIndexQuery::Dwithin {
            distance,
            metric,
            resolved,
            ..
        } = &plan
        {
            let mut query_rows = array.row_operation();
            return match resolved {
                crs::ResolvedMetric::Planar { to_metre } => self.point_rows_dwithin(
                    points,
                    missing,
                    *distance,
                    metric,
                    plan.query_domain(),
                    &mut query_rows,
                    |_, query, ids, row_start, products, _query_rows| {
                        retain_row(ids, row_start, |idx| {
                            let mut source = products.resolve(idx)?;
                            let data = source.prepared()?;
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
                    plan.query_domain(),
                    &mut query_rows,
                    |row, query, ids, row_start, products, query_rows| {
                        let query_cache = query_rows.frame_cache(row);
                        retain_row(ids, row_start, |idx| {
                            let mut source = products.resolve(idx)?;
                            let data = source.prepared()?;
                            let candidate_cache = source.frame_cache();
                            pair_dwithin_geodesic(
                                geodesic,
                                query,
                                &query_cache,
                                &data,
                                &candidate_cache,
                                distance.get(),
                            )
                            .map_err(Into::into)
                        })
                    },
                ),
            };
        }
        let (candidate_distance, model) = match &plan {
            PreparedIndexQuery::Candidates { .. } => (None, None),
            PreparedIndexQuery::CandidatesDwithin {
                distance, metric, ..
            } => (Some(*distance), Some(metric)),
            PreparedIndexQuery::Topological { .. } | PreparedIndexQuery::Dwithin { .. } => {
                unreachable!("exact predicates use their dedicated point-row runners")
            },
        };
        let coordinate_distance = candidate_distance
            .and_then(|distance| model.and_then(|metric| metric.coordinate_radius(distance.get())));
        // Geodesic dwithin over an all-point index: the sound per-point
        // candidate window replaces the global scan.
        let (geodesic_crs, geodesic_state) = match (model, candidate_distance) {
            (Some(crs::MetricModel::Geodesic { crs, frame, state }), Some(_))
                if self.non_prunable_live == 0 && coordinate_distance.is_none() =>
            {
                if *frame == crate::geographic::GeographicFrame::crs84() {
                    (Some(Arc::clone(crs)), Some(state))
                } else {
                    (None, None)
                }
            },
            _ => (None, None),
        };
        // CSR-direct candidate collection appends, orders and deduplicates
        // each row in one flat ids buffer — no per-point `Vec`.
        let mut builder = CsrBuilder::<usize>::with_capacity(0, points.len());
        for (row, point) in points.iter().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row)) {
                let end = builder.len();
                builder.push_end(end)?;
                continue;
            }
            plan.query_domain().admit_shape(
                &Shape::Point(point),
                Some(Bounds::from_point(point)),
                Some(row),
            )?;
            let row_start = builder.len();
            match (coordinate_distance, candidate_distance) {
                (Some(radius), _) => {
                    if radius == f64::INFINITY {
                        self.append_live_handles(builder.values_mut());
                    } else {
                        self.collect_within_distance_2(
                            [point.x, point.y],
                            radius * radius,
                            builder.values_mut(),
                        );
                    }
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
                                self.collect_intersecting(window, builder.values_mut());
                            }
                        },
                        None => {
                            self.append_live_handles(builder.values_mut());
                        },
                    }
                },
                (None, None) => {
                    self.collect_point_candidates(point, builder.values_mut())?;
                },
            }
            self.finish_candidates(builder.values_mut(), row_start);
            let end = builder.len();
            builder.push_end(end)?;
        }
        let (ids, offsets) = builder.finish();
        Ok(RowMatches { ids, offsets })
    }
}
