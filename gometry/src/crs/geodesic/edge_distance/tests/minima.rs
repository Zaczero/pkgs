use super::*;

#[rstest::rstest]
#[case::short_equatorial_interior("short equatorial interior", Point::new_raw_xy(0.0, 0.001), Point::new_raw_xy(-0.005, 0.0), Point::new_raw_xy(0.005, 0.0), 0.4..=0.6)]
#[case::start_endpoint("start endpoint", Point::new_raw_xy(-3.0, 1.0), Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(2.0, 0.0), 0.0..=0.0)]
#[case::end_endpoint("end endpoint", Point::new_raw_xy(3.0, 1.0), Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(2.0, 0.0), 1.0..=1.0)]
#[case::long_oblique_interior("long oblique interior", Point::new_raw_xy(30.0, 22.0), Point::new_raw_xy(-70.0, 10.0), Point::new_raw_xy(80.0, 55.0), 0.01..=0.99)]
#[case::seam_interior("seam interior", Point::new_raw_xy(180.0, 20.0), Point::new_raw_xy(175.0, 10.0), Point::new_raw_xy(-175.0, 10.0), 0.4..=0.6)]
fn point_edge_fixed_minima_agree_with_dense_karney_and_golden_oracle(
    #[case] name: &'static str,
    #[case] point: Point,
    #[case] start: Point,
    #[case] end: Point,
    #[case] parameter_range: std::ops::RangeInclusive<f64>,
) {
    let frame = GeographicFrame::crs84();
    let metric = metric(&Geodesic::wgs84(), frame);

    let edge = metric.chart_edge(start, end).expect("admitted fixed edge");
    let actual = point_edge_minimum(&metric, point, start, end);
    let expected = dense_oracle(&metric, point, &edge);
    let gap = 4.0e-11 * metric.geodesic.a;
    assert!(
        actual.distance >= expected - metric.geodesic_numerical_policy().evaluation_allowance(2.0),
        "{name}: attained {} below oracle {expected}",
        actual.distance
    );
    assert!(
        actual.distance <= expected + gap,
        "{name}: attained {} exceeds oracle {expected} by more than {gap}",
        actual.distance
    );
    assert!(
        parameter_range.contains(&actual.parameter),
        "{name}: t={}",
        actual.parameter
    );
    assert!(
        !actual.budget_exhausted,
        "{name}: exhausted the 128-evaluation minimum budget"
    );
}
#[test]
fn near_antipodal_short_edge_uses_the_lipschitz_fallback() {
    let frame = GeographicFrame::crs84();
    let metric = metric(&Geodesic::wgs84(), frame);
    let point = Point::new_raw_xy(179.999, 0.001);
    let start = Point::new_raw_xy(0.0, 0.0);
    let end = Point::new_raw_xy(1.0e-10, 0.0);
    let edge = metric.chart_edge(start, end).unwrap();
    let minimum = point_edge_minimum(&metric, point, start, end);
    assert!(
        curvature_hessian_bound(&metric.geodesic, minimum.distance, minimum.distance).is_infinite()
    );
    let oracle = dense_oracle(&metric, point, &edge);
    assert!((minimum.distance - oracle).abs() <= 4.0e-11 * metric.geodesic.a);
    assert!(!minimum.budget_exhausted);
    assert!(minimum.evaluations <= MAX_POINT_EDGE_MINIMUM_EVALUATIONS);
}
#[test]
fn chart_parameter_trial_closes_the_near_edge_station_counterexample() {
    let frame = GeographicFrame::crs84();
    let metric = metric(&Geodesic::wgs84(), frame);
    let start = Point::new_raw_xy(0.0, 10.0);
    let end = Point::new_raw_xy(120.0, 60.0);
    // Independent 60-digit mpmath arclength inversion of the WGS 84 chart
    // edge gives station 6,004,860.750429569 m at this represented query.
    let point = Point::new_raw_xy(53.074_630_350_026_74, 32.114_429_312_511_14);
    let target_station = 6_004_860.750_429_569;

    let edge = metric.chart_edge(start, end).expect("admitted chart edge");
    let PointEdgeQueryResult::Minimum(minimum) = metric
        .point_edge_query(
            point,
            &edge,
            0.0,
            PointEdgeQuery::Locate {
                cutoff: f64::INFINITY,
            },
            &mut PointEdgeScratch::default(),
        )
        .expect("station minimum")
    else {
        panic!("unbounded station query must return a minimum");
    };
    let with_station = metric
        .chart_edge_length_to(&edge, minimum.parameter)
        .expect("trial station");

    // The precomputed allowance is 30.704 ns: two 4-epsilon length
    // inversions, a four-ULP physical coordinate envelope, four ULPs of
    // parameter propagated by the sampled edge speed, and two station ULPs.
    assert!((with_station - target_station).abs() <= 30.71e-9);
    assert!(minimum.distance <= metric.geodesic_numerical_policy().backend_floor());
    assert!(minimum.evaluations <= 3);
    assert!(!minimum.budget_exhausted);
}
#[rstest::rstest]
#[case::interior("interior", [-1.0, 1.0], [1.0, 1.0], [0.0, 0.0])]
#[case::endpoint("endpoint", [0.0, 1.0], [1.0, 2.0], [0.0, 0.0])]
#[case::flat("flat", [-1e-10, 1.0], [1e-10, 1.0], [0.0, 0.0])]
#[case::seam("seam", [175.0, 10.0], [-175.0, 10.0], [180.0, 20.0])]
#[case::polar("polar", [-45.0, 80.0], [45.0, 80.0], [0.0, 89.999_999_999])]
#[case::pole("pole", [-45.0, 60.0], [45.0, 60.0], [0.0, 90.0])]
#[case::degenerate("degenerate", [0.0, 1.0], [0.0, 1.0], [0.0, 0.0])]
#[case::contact("contact", [-1.0, 0.0], [1.0, 0.0], [0.0, 0.0])]
fn chart_witness_role_nearest_preserves_minimum_measurements(
    #[case] name: &'static str,
    #[case] start: [f64; 2],
    #[case] end: [f64; 2],
    #[case] point: [f64; 2],
) {
    let metric = metric(&Geodesic::wgs84(), GeographicFrame::crs84());

    let point = Point::new_raw_xy(point[0], point[1]);
    let edge = metric
        .chart_edge(
            Point::new_raw_xy(start[0], start[1]),
            Point::new_raw_xy(end[0], end[1]),
        )
        .unwrap();
    let query = |query| {
        metric
            .point_edge_query(point, &edge, 0.0, query, &mut PointEdgeScratch::default())
            .unwrap()
    };
    let PointEdgeQueryResult::Minimum(reference) = query(PointEdgeQuery::Minimum {
        cutoff: f64::INFINITY,
    }) else {
        panic!("{name}: unbounded minimum");
    };
    let allowance = metric.geodesic_numerical_policy().evaluation_allowance(2.0);
    for cutoff in [f64::INFINITY, reference.distance + allowance] {
        let PointEdgeQueryResult::Minimum(minimum) = query(PointEdgeQuery::Minimum { cutoff })
        else {
            panic!("{name}: admitted minimum");
        };
        let PointEdgeQueryResult::Minimum(nearest) = query(PointEdgeQuery::Nearest { cutoff })
        else {
            panic!("{name}: admitted nearest");
        };
        approx::assert_abs_diff_eq!(minimum.distance, nearest.distance, epsilon = allowance,);
        approx::assert_abs_diff_eq!(
            minimum.lower_bound,
            nearest.lower_bound,
            epsilon = allowance,
        );
        assert!(
            minimum.evaluations <= MAX_POINT_EDGE_MINIMUM_EVALUATIONS,
            "{name}"
        );
        assert!(
            nearest.evaluations <= MAX_POINT_EDGE_MINIMUM_EVALUATIONS,
            "{name}"
        );
        assert_eq!(minimum.budget_exhausted, nearest.budget_exhausted, "{name}");
        if name == "contact" {
            assert_eq!(minimum.distance, 0.0);
            assert_eq!(minimum.parameter, 0.5);
            assert_eq!(
                edge.witness_role(nearest.parameter),
                crate::geometry::WitnessRole::Interior
            );
            let PointEdgeQueryResult::Minimum(locate) = query(PointEdgeQuery::Locate { cutoff })
            else {
                panic!("contact: admitted locate");
            };
            assert_eq!(locate.distance, 0.0);
            assert_eq!(locate.parameter, 0.5);
            assert_eq!(locate.evaluations, 3);
        }
    }
}
#[test]
fn chart_edge_minimum_matches_independent_ellipsoid_quadrature() {
    let frame = GeographicFrame::crs84();
    let metric = metric(&Geodesic::wgs84(), frame);
    let start = Point::new_raw_xy(-70.0, 30.3);
    let end = Point::new_raw_xy(-68.0, 31.3);
    let point = Point::new_raw_xy(-68.0, 31.0);
    // Independent 70-digit auxiliary-sphere geodesic quadrature and
    // stationary-point shooting: no chart-edge search or length code.
    let expected_distance = 28_757.993_325_000_867;
    let minimum = point_edge_minimum(&metric, point, start, end);
    // Four-epsilon ellipsoid-scale arithmetic and four coordinate ULPs
    // propagated through WGS 84's local longitude/latitude scales.
    assert!(
        (minimum.distance - expected_distance).abs() <= 13.0e-9,
        "distance {} differs from the independent minimum",
        minimum.distance
    );
}
#[test]
fn point_edge_exact_degenerate_pole_sphere_and_prolate_families() {
    let frame = GeographicFrame::crs84();
    let wgs84 = metric(&Geodesic::wgs84(), frame);
    let point = Point::new_raw_xy(0.0, 30.0);
    let degenerate = point_edge_minimum(&wgs84, Point::new_raw_xy(2.0, 31.0), point, point);
    assert_eq!(degenerate.evaluations, 1);
    assert_eq!(degenerate.parameter, 0.0);

    let parallel_start = Point::new_raw_xy(-45.0, 80.0);
    let parallel_end = Point::new_raw_xy(45.0, 80.0);
    let pole = Point::new_raw_xy(0.0, 90.0);
    let pole_minimum = point_edge_minimum(&wgs84, pole, parallel_start, parallel_end);
    assert_eq!(pole_minimum.evaluations, 1);
    assert!(
        (pole_minimum.distance
            - oracle_distance(
                &wgs84,
                pole,
                &wgs84.chart_edge(parallel_start, parallel_end).unwrap(),
                0.0,
            ))
        .abs()
            <= wgs84.geodesic_numerical_policy().evaluation_allowance(2.0)
    );

    for (name, flattening, point, start, end) in [
        (
            "sphere long oblique edge",
            0.0,
            Point::new_raw_xy(30.0, 22.0),
            Point::new_raw_xy(-70.0, 10.0),
            Point::new_raw_xy(80.0, 55.0),
        ),
        (
            "sphere polar seam",
            0.0,
            Point::new_raw_xy(-179.0, 89.0),
            Point::new_raw_xy(170.0, 88.0),
            Point::new_raw_xy(-100.0, 87.0),
        ),
        (
            "prolate long oblique edge",
            -0.01,
            Point::new_raw_xy(30.0, 22.0),
            Point::new_raw_xy(-70.0, 10.0),
            Point::new_raw_xy(80.0, 55.0),
        ),
        (
            "prolate polar seam",
            -0.01,
            Point::new_raw_xy(-179.0, 89.0),
            Point::new_raw_xy(170.0, 88.0),
            Point::new_raw_xy(-100.0, 87.0),
        ),
    ] {
        let metric = metric(&Geodesic::new(6_371_000.0, flattening), frame);
        let edge = metric.chart_edge(start, end).unwrap();
        let minimum = point_edge_minimum(&metric, point, start, end);
        let oracle = dense_oracle(&metric, point, &edge);
        let gap = 4.0e-11 * metric.geodesic.a;
        assert!(
            (minimum.distance - oracle).abs() <= gap,
            "{name}: attained={} at t={} oracle={oracle} gap={gap}",
            minimum.distance,
            minimum.parameter
        );
    }
}
