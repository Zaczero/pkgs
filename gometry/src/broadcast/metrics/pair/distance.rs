use super::*;

pub(crate) fn pair_distance_planar(
    to_metre: f64,
    a: &PreparedPlan,
    b: &PreparedPlan,
) -> crate::error::Result<f64> {
    Ok(a.distance(b)? * to_metre)
}

pub(crate) fn pair_distance_geodesic(
    metric: &crs::GeodesicMetric,
    a: &PreparedPlan,
    a_cache: &FrameDependentCaches,
    b: &PreparedPlan,
    b_cache: &FrameDependentCaches,
) -> crate::error::Result<f64> {
    if a.shape().is_empty() || b.shape().is_empty() {
        return Ok(f64::INFINITY);
    }
    let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(
        &metric.geodesic,
        a,
        b,
        metric.frame,
        metric.state.generation,
    )?;
    crs::finite(
        a.geodesic_distance_cached(
            a_cache,
            b,
            b_cache,
            &metric.crs,
            semi_major,
            flattening,
            &ellipsoid,
        )?,
        crs::GeodesicCalculation::Distance,
    )
}

pub(crate) fn pair_dwithin_planar(
    to_metre: f64,
    a: &PreparedPlan,
    b: &PreparedPlan,
    distance: f64,
) -> crate::error::Result<bool> {
    let distance = if distance.is_infinite() && distance.is_sign_positive() {
        distance
    } else {
        NonNegative::try_new("distance", distance)?.get()
    };
    a.dwithin(b, distance / to_metre)
}

pub(crate) fn pair_dwithin_geodesic(
    metric: &crs::GeodesicMetric,
    a: &PreparedPlan,
    a_cache: &FrameDependentCaches,
    b: &PreparedPlan,
    b_cache: &FrameDependentCaches,
    distance: f64,
) -> crate::error::Result<bool> {
    let distance = if distance.is_infinite() && distance.is_sign_positive() {
        distance
    } else {
        NonNegative::try_new("distance", distance)?.get()
    };
    if a.shape().is_empty() || b.shape().is_empty() {
        return Ok(false);
    }
    if distance == f64::INFINITY {
        ensure_geodesic_shape(metric.frame, a.shape())?;
        ensure_geodesic_shape(metric.frame, b.shape())?;
        return Ok(true);
    }
    let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(
        &metric.geodesic,
        a,
        b,
        metric.frame,
        metric.state.generation,
    )?;
    a.geodesic_dwithin_cached(
        a_cache,
        b,
        b_cache,
        &metric.crs,
        semi_major,
        flattening,
        &ellipsoid,
        distance,
    )
}

/// Element-wise zero metric for array-vs-self when both operands share one
/// `Arc<GeometryArrayStorage>`.
#[expect(
    clippy::indexing_slicing,
    reason = "selected polygon ring ranges are admitted by the corresponding nested CSR offset columns"
)]
/// `metric_nearest_points` against an already-resolved metric. `None` when an
/// operand is empty — the surfaces map it to the output-type EMPTY sentinel
/// (`(POINT EMPTY, POINT EMPTY)` / `LINESTRING EMPTY`), mirroring distance's
/// `inf`.
pub(crate) fn metric_nearest_points_resolved(
    metric: &crs::ResolvedMetric,
    a: &PreparedPlan,
    a_cache: &FrameDependentCaches,
    b: &PreparedPlan,
    b_cache: &FrameDependentCaches,
    source_context: Option<&SourceArcContext<'_, '_, crs::GeodesicMetric>>,
) -> crate::error::Result<Option<(Point, Point)>> {
    if a.shape().is_empty() || b.shape().is_empty() {
        return Ok(None);
    }
    dispatch_resolved_metric(
        metric,
        |_| {
            source_context.map_or_else(
                || a.nearest_points(b),
                |source_context| a.nearest_points_with_source_context(b, source_context),
            )
        },
        |metric| {
            let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(
                &metric.geodesic,
                a,
                b,
                metric.frame,
                metric.state.generation,
            )?;
            a.geodesic_nearest_points_cached(
                a_cache,
                b,
                b_cache,
                &metric.crs,
                semi_major,
                flattening,
                &ellipsoid,
            )
        },
    )
}

/// CRS-aware shortest connecting line — `metric_nearest_points` as a two-point
/// `LineString` (degenerate when the geometries touch), or `LINESTRING EMPTY`
/// in the operands' common axes when a side is empty.
pub(crate) fn metric_shortest_line_resolved(
    metric: &crs::ResolvedMetric,
    a: &PreparedPlan,
    b: &PreparedPlan,
    domain: TopologyDomain,
    source_context: &SourceArcContext<'_, '_, crs::GeodesicMetric>,
) -> crate::error::Result<Shape> {
    let common = crate::geometry::common_axes(a.shape(), b.shape());
    let source_capture = SourceArcCapture::for_output_axes(common, source_context);
    match metric {
        crs::ResolvedMetric::Planar { .. } => {
            let pair = metric_nearest_points_resolved(
                metric,
                a,
                &FrameDependentCaches::default(),
                b,
                &FrameDependentCaches::default(),
                source_capture.context(),
            )?;
            match (pair.as_ref(), domain) {
                (Some((start, end)), TopologyDomain::Geographic(frame)) => {
                    crate::geometry::nearest_line_with_intent(
                        pair.as_ref(),
                        common,
                        frame,
                        ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [
                            start.x, end.x,
                        ])?,
                        &source_capture,
                    )
                },
                (_, TopologyDomain::Cartesian) => {
                    crate::geometry::nearest_line(pair.as_ref(), common)
                },
                (None, TopologyDomain::Geographic(_)) => {
                    crate::geometry::nearest_line(None, common)
                },
            }
        },
        crs::ResolvedMetric::Geodesic(metric) => {
            let (ellipsoid, semi_major, flattening) = prepare_geodesic_pair(
                &metric.geodesic,
                a,
                b,
                metric.frame,
                metric.state.generation,
            )?;
            let a_cache = FrameDependentCaches::default();
            let b_cache = FrameDependentCaches::default();
            match a.geodesic_nearest_line_cached(
                &a_cache,
                b,
                &b_cache,
                &metric.crs,
                semi_major,
                flattening,
                &ellipsoid,
            )? {
                Some((start, end, intent)) => crate::geometry::nearest_line_with_intent(
                    Some(&(start, end)),
                    common,
                    metric.frame,
                    ComputedEdgeIntent::retained_chart_witness(
                        metric.frame,
                        [start.x, end.x],
                        intent,
                    )?,
                    &source_capture,
                ),
                None => crate::geometry::nearest_line(None, common),
            }
        },
    }
}

#[cfg(test)]
mod dwithin_parity_tests {
    use super::*;
    use crate::geometry::{point_distance, points_dwithin};

    fn point(x: f64, y: f64) -> Point {
        Point::new_raw_xy(x, y)
    }

    fn scalar_point_dwithin(a: Point, b: Point, limit: f64) -> bool {
        Shape::Point(a).dwithin(&Shape::Point(b), limit).unwrap()
    }

    fn packed_planar_dwithin(a: Point, b: Point, limit: f64) -> bool {
        points_dwithin(a, b, limit)
    }

    fn shape_data_dwithin(a: Point, b: Point, limit: f64) -> bool {
        PreparedPlan::new(ShapeData::new(Shape::Point(a)))
            .dwithin(&PreparedPlan::new(ShapeData::new(Shape::Point(b))), limit)
            .unwrap()
    }

    #[test]
    fn planar_dwithin_scalar_packed_and_shape_data_agree() {
        let coords = [-1e6, -3.0, -1.0, 0.0, 1.0, 3.0, 4.0, 1e6];
        for &ax in &coords {
            for &ay in &coords {
                for &bx in &coords {
                    for &by in &coords {
                        let (a, b) = (point(ax, ay), point(bx, by));
                        let dist = point_distance(a, b);
                        let limits = [
                            0.0,
                            f64::MIN_POSITIVE,
                            dist * 0.5,
                            dist,
                            dist.next_down(),
                            dist.next_up(),
                            dist * 2.0,
                            f64::INFINITY,
                        ];
                        for limit in limits {
                            let scalar = scalar_point_dwithin(a, b, limit);
                            let packed = packed_planar_dwithin(a, b, limit);
                            let data = shape_data_dwithin(a, b, limit);
                            assert_eq!(
                                scalar, packed,
                                "scalar vs packed at ({ax},{ay})-({bx},{by}) limit={limit}"
                            );
                            assert_eq!(
                                scalar, data,
                                "scalar vs ShapeData at ({ax},{ay})-({bx},{by}) limit={limit}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod planar_nearest_source_ordinate_tests {
    use super::{metric_nearest_points_resolved, metric_shortest_line_resolved};
    use crate::crs::{GeodesicMetric, MetricModel, ResolvedMetric};
    use crate::geometry::{
        ChartMetric as _, FrameDependentCaches, LineSeq, Point, PreparedPlan, RootWorkCounters,
        Shape, SourceArcContext, TopologyDomain, reset_root_work_counters, root_work_counters,
    };

    const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";

    fn segmented_length(metric: &GeodesicMetric, start: Point, end: Point) -> f64 {
        (0..8)
            .zip(1..=8)
            .map(|(start_index, end_index)| {
                let start_parameter = f64::from(start_index) / 8.0;
                let end_parameter = f64::from(end_index) / 8.0;
                let start_point = Point::new_raw_xy(
                    start.x + (end.x - start.x) * start_parameter,
                    start.y + (end.y - start.y) * start_parameter,
                );
                let end_point = Point::new_raw_xy(
                    start.x + (end.x - start.x) * end_parameter,
                    start.y + (end.y - start.y) * end_parameter,
                );
                let edge = metric.chart_edge(start_point, end_point).unwrap();
                metric.chart_edge_length(&edge).unwrap()
            })
            .sum()
    }

    fn assert_native_samples_resolved_once(line: &LineSeq) {
        let expected_samples = line
            .as_coords()
            .iter()
            .count()
            .checked_sub(2)
            .expect("shortest-line output retains both witness endpoints");
        let work = root_work_counters();
        assert_eq!(work.native_chain_entries, 1);
        assert_eq!(work.source_arc_requests, expected_samples);
        assert_eq!(work.source_arc_resolutions, expected_samples);
        assert_eq!(work.root_requests, 0);
        assert_eq!(work.root_ranges, 0);
        assert_eq!(work.trace_rows_builders, 0);
        assert_eq!(work.request_tables, 0);
        assert_eq!(work.root_catalog_resolutions, 0);
    }

    #[test]
    fn planar_nearest_witness_keeps_xy_and_resolves_source_root_ordinates() {
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let metric_model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
        let metric = GeodesicMetric::from_model(&metric_model).unwrap();
        let frame = metric.frame;
        let source = Shape::LineString(
            LineSeq::segment(
                Point::new_axes(
                    0.0,
                    0.0,
                    crate::geometry::ZOrdinate(Some(10.0)),
                    crate::geometry::MOrdinate(Some(100.0)),
                )
                .unwrap(),
                Point::new_axes(
                    120.0,
                    60.0,
                    crate::geometry::ZOrdinate(Some(210.0)),
                    crate::geometry::MOrdinate(Some(500.0)),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let query = Shape::Point(
            Point::new_axes(
                60.0,
                31.0,
                crate::geometry::ZOrdinate(Some(7.0)),
                crate::geometry::MOrdinate(Some(9.0)),
            )
            .unwrap(),
        );
        let planar = ResolvedMetric::from_model(&MetricModel::COORDINATE).unwrap();
        let source_plan = PreparedPlan::new(source);
        let query_plan = PreparedPlan::new(query);
        let metric_access = || Ok(&metric);
        let source_context =
            SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));

        let (left, right) = metric_nearest_points_resolved(
            &planar,
            &source_plan,
            &FrameDependentCaches::default(),
            &query_plan,
            &FrameDependentCaches::default(),
            Some(&source_context),
        )
        .unwrap()
        .unwrap();

        assert_eq!((left.x, left.y), (60.4, 30.2));
        assert_eq!((right.x, right.y), (60.0, 31.0));
        let origin = Point::new_raw_xy(0.0, 0.0);
        let fraction = segmented_length(&metric, origin, Point::new_raw_xy(left.x, left.y))
            / segmented_length(&metric, origin, Point::new_raw_xy(120.0, 60.0));
        assert!((left.z().unwrap() - (10.0 + 200.0 * fraction)).abs() < 1.0e-9);
        assert!((left.m().unwrap() - (100.0 + 400.0 * fraction)).abs() < 1.0e-9);
        assert_eq!(right.z(), Some(7.0));
        assert_eq!(right.m(), Some(9.0));
    }

    #[test]
    fn shortest_line_samples_follow_the_retained_half_and_full_turn_chords() {
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
        let metric = GeodesicMetric::from_model(&model).unwrap();
        let frame = metric.frame;
        let geodesic = ResolvedMetric::from_model(&model).unwrap();
        let planar = ResolvedMetric::from_model(&MetricModel::COORDINATE).unwrap();
        let metric_access = || Ok(&metric);
        let source_context =
            SourceArcContext::new(TopologyDomain::Geographic(frame), Some(&metric_access));

        let half_turn_start = Point::new_axes(
            0.0,
            0.0,
            crate::geometry::ZOrdinate(Some(10.0)),
            crate::geometry::MOrdinate(Some(100.0)),
        )
        .unwrap();
        let half_turn_end = Point::new_axes(
            180.0,
            60.0,
            crate::geometry::ZOrdinate(Some(210.0)),
            crate::geometry::MOrdinate(Some(500.0)),
        )
        .unwrap();
        let left = PreparedPlan::new(Shape::Point(half_turn_start));
        let right = PreparedPlan::new(Shape::Point(half_turn_end));
        let origin = Point::new_raw_xy(0.0, 0.0);
        let half_turn_total = segmented_length(&metric, origin, Point::new_raw_xy(180.0, 60.0));
        for resolved in [&planar, &geodesic] {
            reset_root_work_counters();
            let Shape::LineString(line) = metric_shortest_line_resolved(
                resolved,
                &left,
                &right,
                TopologyDomain::Geographic(frame),
                &source_context,
            )
            .unwrap() else {
                panic!("shortest_line publishes a line");
            };
            assert_native_samples_resolved_once(&line);
            let sample = line
                .as_coords()
                .iter()
                .find(|point| {
                    point.x.abs().to_bits() == 90.0_f64.to_bits()
                        && point.y.to_bits() == 30.0_f64.to_bits()
                })
                .expect("the half-turn producer chord has a midpoint");
            let producer_midpoint = Point::new_raw_xy(90.0, 30.0);
            let fraction = segmented_length(&metric, origin, producer_midpoint) / half_turn_total;
            assert!((sample.z().unwrap() - (10.0 + 200.0 * fraction)).abs() < 1.0e-9);
            assert!((sample.m().unwrap() - (100.0 + 400.0 * fraction)).abs() < 1.0e-9);
        }

        let full_turn_start = Point::new_axes(
            0.0,
            1.0,
            crate::geometry::ZOrdinate(Some(10.0)),
            crate::geometry::MOrdinate(Some(100.0)),
        )
        .unwrap();
        let full_turn_end = Point::new_axes(
            360.0,
            60.0,
            crate::geometry::ZOrdinate(Some(210.0)),
            crate::geometry::MOrdinate(Some(500.0)),
        )
        .unwrap();
        let left = PreparedPlan::new(Shape::Point(full_turn_start));
        let right = PreparedPlan::new(Shape::Point(full_turn_end));
        let origin = Point::new_raw_xy(0.0, 1.0);
        let total = segmented_length(&metric, origin, Point::new_raw_xy(360.0, 60.0));
        reset_root_work_counters();
        let Shape::LineString(line) = metric_shortest_line_resolved(
            &planar,
            &left,
            &right,
            TopologyDomain::Geographic(frame),
            &source_context,
        )
        .unwrap() else {
            panic!("shortest_line publishes a line");
        };
        assert_native_samples_resolved_once(&line);
        for (index, parameter) in [0.25, 0.5, 0.75].into_iter().enumerate() {
            let y = 1.0 + 59.0 * parameter;
            let sample = line
                .as_coords()
                .iter()
                .find(|point| (point.y - y).abs() < 1.0e-12)
                .unwrap_or_else(|| panic!("missing full-turn sample at index {index}"));
            let producer_sample = Point::new_raw_xy(360.0 * parameter, 1.0 + 59.0 * parameter);
            let fraction = segmented_length(&metric, origin, producer_sample) / total;
            assert!((sample.z().unwrap() - (10.0 + 200.0 * fraction)).abs() < 1.0e-9);
            assert!((sample.m().unwrap() - (100.0 + 400.0 * fraction)).abs() < 1.0e-9);
        }
    }

    #[test]
    fn geographic_xy_planar_nearest_points_skip_source_root_capture() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let planar = ResolvedMetric::from_model(&MetricModel::COORDINATE).unwrap();
        let line = PreparedPlan::new(Shape::LineString(
            LineSeq::segment(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(120.0, 60.0)).unwrap(),
        ));
        let point = PreparedPlan::new(Shape::Point(Point::new_raw_xy(60.0, 31.0)));

        for (left, right, expected_left, expected_right) in [
            (&line, &point, (60.4, 30.2), (60.0, 31.0)),
            (&point, &line, (60.0, 31.0), (60.4, 30.2)),
        ] {
            reset_root_work_counters();
            let pair = metric_nearest_points_resolved(
                &planar,
                left,
                &FrameDependentCaches::default(),
                right,
                &FrameDependentCaches::default(),
                None,
            )
            .unwrap()
            .expect("nonempty XY nearest pair");
            assert!((pair.0.x - expected_left.0).abs() < 1.0e-12);
            assert!((pair.0.y - expected_left.1).abs() < 1.0e-12);
            assert!((pair.1.x - expected_right.0).abs() < 1.0e-12);
            assert!((pair.1.y - expected_right.1).abs() < 1.0e-12);
            assert_eq!(pair.0.axes(), crate::geometry::CoordinateAxes::XY);
            assert_eq!(pair.1.axes(), crate::geometry::CoordinateAxes::XY);
            assert_eq!(root_work_counters(), RootWorkCounters::default());
        }
    }

    #[test]
    fn geographic_xy_shortest_line_skips_source_root_capture() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let model = crate::crs::metric_model_at(Some(CRS), &state).unwrap();
        let metric = GeodesicMetric::from_model(&model).unwrap();
        let geodesic = ResolvedMetric::from_model(&model).unwrap();
        let planar = ResolvedMetric::from_model(&MetricModel::COORDINATE).unwrap();
        let metric_access = || Ok(&metric);
        let context = SourceArcContext::new(
            TopologyDomain::Geographic(metric.frame),
            Some(&metric_access),
        );
        let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(0.0, 0.0)));
        let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(180.0, 60.0)));

        for unit in [&planar, &geodesic] {
            reset_root_work_counters();
            let line = metric_shortest_line_resolved(
                unit,
                &left,
                &right,
                TopologyDomain::Geographic(metric.frame),
                &context,
            )
            .unwrap();
            assert_eq!(line.axes(), crate::geometry::CoordinateAxes::XY);
            assert_eq!(root_work_counters(), RootWorkCounters::default());
        }
    }
}

#[cfg(test)]
mod shortest_line_tests {
    use super::*;

    #[test]
    fn shortest_line_from_geodesic_point_witnesses_uses_its_stored_short_step() {
        let frame = GeographicFrame::crs84();
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let metric = crate::crs::ResolvedMetric::from_model(&model).unwrap();
        let source_context = SourceArcContext::cartesian();
        let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(179.8, 0.0)));
        let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(-179.8, 1.0)));
        let output = metric_shortest_line_resolved(
            &metric,
            &left,
            &right,
            TopologyDomain::Geographic(frame),
            &source_context,
        )
        .unwrap();
        let Shape::LineString(line) = output else {
            panic!("shortest_line publishes a line");
        };
        let coordinates = line.as_coords().iter().collect::<Vec<_>>();
        assert_eq!(coordinates.len(), 2);
        assert_eq!(coordinates[0].x, 179.8);
        assert_eq!(coordinates[1].x, -179.8);
        let axis = frame.longitude();
        let step = axis
            .chart_step(coordinates[0].x, coordinates[1].x)
            .unwrap()
            .delta(axis);
        assert!(step.abs() < 1.0, "seam connector step is {step}");
    }
    #[test]
    fn shortest_line_from_chart_edge_witness_retains_its_chart_step() {
        let frame = GeographicFrame::crs84();
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let metric = crate::crs::ResolvedMetric::from_model(&model).unwrap();
        let source_context = SourceArcContext::cartesian();
        let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(179.8, 0.0)));
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(-179.8, -1.0),
                    Point::new_raw_xy(-179.8, 1.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let right = PreparedPlan::new(line);
        let output = metric_shortest_line_resolved(
            &metric,
            &left,
            &right,
            TopologyDomain::Geographic(frame),
            &source_context,
        )
        .unwrap();
        let Shape::LineString(line) = output else {
            panic!("shortest_line publishes a line");
        };
        let coordinates = line.as_coords().iter().collect::<Vec<_>>();
        assert_eq!(coordinates.len(), 2);
        assert_eq!(coordinates[0].x, 179.8);
        assert_eq!(coordinates[1].x, -179.8);
        assert!(coordinates[1].y.abs() < 1e-12);
        let axis = frame.longitude();
        let step = axis
            .chart_step(coordinates[0].x, coordinates[1].x)
            .unwrap()
            .delta(axis);
        assert!(step.abs() < 1.0, "seam connector step is {step}");
    }
}
