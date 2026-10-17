use super::{
    Arc, Bounds, CsrBuilder, DistanceUnit, DwithinDistance, GeodesicPruner, IndexPredicate,
    PointRows, PreparedIndexQuery, PreparedPlan, PyGeometryArray, PyResult, PySpatialIndex,
    RowMatches, Shape, ShapeData, ValidityMask, crs, pair_dwithin_geodesic, pair_dwithin_planar,
    retain_row,
};

impl PySpatialIndex {
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
        distance: Option<DwithinDistance>,
        unit: Option<DistanceUnit>,
    ) -> PyResult<RowMatches> {
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
                    |_, query, ids, row_start, products| {
                        retain_row(ids, row_start, |idx| {
                            let data = products.prepared(idx)?;
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
                    |row, query, ids, row_start, products| {
                        let query_cache = query_rows.frame_cache(row);
                        retain_row(ids, row_start, |idx| {
                            let data = products.prepared(idx)?;
                            pair_dwithin_geodesic(
                                geodesic,
                                query,
                                &query_cache,
                                &data,
                                &*products.frame_cache(idx)?,
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
            PreparedIndexQuery::Candidates { .. } | PreparedIndexQuery::Topological { .. } => None,
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
                    (Some(Arc::clone(crs)), Some(state))
                } else {
                    (None, None)
                }
            },
            _ => (None, None),
        };
        // CSR-direct: every row appends into one flat ids buffer; candidate
        // collection orders the window and refinement compacts it in place —
        // no per-point `Vec`.
        let mut builder = CsrBuilder::<usize>::with_capacity(0, points.len());
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
            plan.query_domain().admit_shape(
                &Shape::Point(point),
                Some(Bounds::from_point(point)),
                Some(row),
            )?;
            let query = PreparedPlan::new(ShapeData::new(Shape::Point(point)));
            let row_start = builder.len();
            match (coordinate_distance, distance) {
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
            let ids = builder.values_mut();
            match (&spec, model, predicate) {
                (Some(spec), ..) => {
                    let query_cache = query_rows.frame_cache(row);
                    super::refine::retain_topological(
                        &query,
                        &query_cache,
                        spec,
                        domain,
                        &mut index_products,
                        ids,
                        row_start,
                    )?;
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
}
