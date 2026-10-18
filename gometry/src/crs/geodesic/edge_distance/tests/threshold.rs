use super::*;

#[test]
fn point_edge_cutoff_is_strict_and_threshold_is_inclusive() {
    let frame = GeographicFrame::crs84();
    let metric = metric(&Geodesic::wgs84(), frame);
    let point = Point::new_raw_xy(1.0, 0.0);
    let target = Point::new_raw_xy(0.0, 0.0);
    let edge = metric.chart_edge(target, target).unwrap();
    let minimum = point_edge_minimum(&metric, point, target, target);
    assert!((minimum.distance - 111_319.490_793_273_57).abs() <= 1.0e-9);
    let distance = minimum.distance;
    for (name, cutoff, expected_attainment) in [
        ("below", distance.next_down(), false),
        ("equal", distance, false),
        ("above", distance.next_up(), true),
    ] {
        let result = point_edge_query(
            &metric.geodesic,
            frame,
            point,
            &edge,
            0.0,
            PointEdgeQuery::Minimum { cutoff },
            GeodesicNumericalPolicy::new(metric.geodesic.a),
            &mut PointEdgeScratch::default(),
        )
        .unwrap();
        match result {
            PointEdgeQueryResult::Minimum(actual) => {
                assert!(expected_attainment, "{name}");
                assert_eq!(actual.distance, distance);
            },
            PointEdgeQueryResult::Excluded => assert!(!expected_attainment, "{name}"),
            other @ (PointEdgeQueryResult::Within { .. }
            | PointEdgeQueryResult::Outside { .. }
            | PointEdgeQueryResult::Unresolved { .. }) => {
                panic!("{name}: unexpected cutoff result {other:?}")
            },
        }
    }
    for (name, limit, expected_within) in [
        ("negative infinity", f64::NEG_INFINITY, false),
        ("negative", -1.0, false),
        ("below", distance.next_down(), false),
        ("equal", distance, true),
        ("above", distance.next_up(), true),
        ("positive infinity", f64::INFINITY, true),
    ] {
        let result = point_edge_query(
            &metric.geodesic,
            frame,
            point,
            &edge,
            0.0,
            PointEdgeQuery::Dwithin { limit },
            GeodesicNumericalPolicy::new(metric.geodesic.a),
            &mut PointEdgeScratch::default(),
        )
        .unwrap();
        assert_eq!(
            matches!(result, PointEdgeQueryResult::Within { .. }),
            expected_within,
            "{name}"
        );
    }
    let error = point_edge_query(
        &metric.geodesic,
        frame,
        point,
        &edge,
        0.0,
        PointEdgeQuery::Dwithin { limit: f64::NAN },
        GeodesicNumericalPolicy::new(metric.geodesic.a),
        &mut PointEdgeScratch::default(),
    )
    .expect_err("NaN is not an admitted threshold");
    assert!(
        error
            .to_string()
            .contains("geodesic dwithin limit is not finite")
    );
}
#[rstest::rstest]
#[case::delta_1_0e_12(1.0e-12)]
#[case::delta_1_0e_11(1.0e-11)]
#[case::delta_1_0e_9(1.0e-9)]
fn threshold_search_certifies_near_endpoint_minima_from_derivative_sign(#[case] delta: f64) {
    let frame = GeographicFrame::crs84();
    let metric = metric(&Geodesic::wgs84(), frame);
    let start = Point::new_raw_xy(0.0, 0.0);
    let end = Point::new_raw_xy(5.0, 0.0);
    let edge = metric.chart_edge(start, end).unwrap();

    let point = Point::new_raw_xy(5.0 + delta, 0.0);
    let endpoint_distance = metric.segment_length(point, end);
    assert!(endpoint_distance > 0.0, "{delta}");

    let outside = metric
        .point_edge_query(
            point,
            &edge,
            0.0,
            PointEdgeQuery::Dwithin {
                limit: endpoint_distance.next_down(),
            },
            &mut PointEdgeScratch::default(),
        )
        .unwrap();
    assert!(
        matches!(
            outside,
            PointEdgeQueryResult::Outside {
                lower_bound,
                evaluations: 2,
            } if lower_bound > endpoint_distance.next_down()
        ),
        "{delta}: {outside:?}"
    );

    let within = metric
        .point_edge_query(
            point,
            &edge,
            0.0,
            PointEdgeQuery::Dwithin {
                limit: endpoint_distance,
            },
            &mut PointEdgeScratch::default(),
        )
        .unwrap();
    assert!(
        matches!(within, PointEdgeQueryResult::Within { .. }),
        "{delta}: {within:?}"
    );
}
#[test]
fn exhausted_threshold_and_cutoff_straddles_preserve_unresolved_bounds() {
    let exhausted = PointEdgeMinimum {
        distance: 2.0,
        parameter: 0.5,
        lower_bound: 0.0,
        evaluations: MAX_POINT_EDGE_MINIMUM_EVALUATIONS,
        budget_exhausted: true,
    };
    let threshold = finalize_query(exhausted, f64::INFINITY, Some(1.0));
    assert!(
        matches!(threshold, PointEdgeQueryResult::Unresolved {
            lower_bound: 0.0,
            evaluations: MAX_POINT_EDGE_MINIMUM_EVALUATIONS,
        }),
        "lower=0, limit=1, best=2 must remain unresolved: {threshold:?}"
    );

    let minimum = finalize_query(exhausted, 1.0, None);
    assert!(
        matches!(
            minimum,
            PointEdgeQueryResult::Minimum(PointEdgeMinimum {
                distance: 2.0,
                lower_bound: 0.0,
                budget_exhausted: true,
                ..
            })
        ),
        "a sampled value above cutoff cannot exclude lower=0: {minimum:?}"
    );

    let outside = finalize_query(
        PointEdgeMinimum {
            lower_bound: 1.25,
            ..exhausted
        },
        f64::INFINITY,
        Some(1.0),
    );
    assert!(
        matches!(outside, PointEdgeQueryResult::Outside { lower_bound, .. } if lower_bound > 1.0)
    );

    let within = finalize_query(
        PointEdgeMinimum {
            distance: 0.5,
            lower_bound: 0.0,
            ..exhausted
        },
        f64::INFINITY,
        Some(1.0),
    );
    assert!(matches!(within, PointEdgeQueryResult::Within { .. }));
}
