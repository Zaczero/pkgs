use super::*;

#[test]
fn planar_locate_orders_segments_by_source_distance() {
    let points = [
        Point::new_raw_xy(-1.0, (-1.0_f64).next_up()),
        Point::new_raw_xy(1.0, 1.0),
        Point::new_raw_xy(0.5, 0.5),
    ];
    let line =
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap());
    assert_eq!(
        line.line_locate_point_streaming(points[2], true).unwrap(),
        1.0
    );
}
#[test]
fn planar_locate_selects_exact_nearest_when_distances_round_to_a_tie() {
    let shape = crate::io::parse_wkt(
        "MULTILINESTRING ((0.000000007450580596923828125 1, 1 1), (0 1, -1 1))",
    )
    .expect("valid multipart line");
    let probe = Point::new_raw_xy(0.0, 0.0);
    let prepared = crate::geometry::PreparedPlan::new(shape);
    let station = cached_planar_locate(&prepared, probe, false).expect("prepared nearest search");
    // The nearest endpoint is the start of segment 1. Segment 0 has
    // horizontal length 1 - 2^-27, which is exactly representable.
    assert_eq!(
        station.to_bits(),
        0x3FEF_FFFF_FC00_0000,
        "exact locate selects the second segment despite rounded-distance equality"
    );
    let query = crate::geometry::PreparedPlan::new(Shape::Point(probe));
    let (_, nearest) = query
        .nearest_points(&prepared)
        .expect("proximity query")
        .expect("nonempty nearest pair");
    assert_eq!(nearest.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(nearest.y.to_bits(), 1.0_f64.to_bits());
}
#[test]
fn planar_locate_retains_a_finite_station_when_query_distance_overflows() {
    use crate::geometry::point_pair_distance_key;
    let coordinates =
        CoordSeq::try_from_points(&[Point::new_raw_xy(1e308, 0.0), Point::new_raw_xy(1e308, 1.0)])
            .unwrap();
    let shape = Shape::LineString(LineSeq::try_new(coordinates.clone()).unwrap());
    let probe = Point::new_raw_xy(-1e308, 0.5);
    let prepared = crate::geometry::PreparedPlan::new(shape);
    assert_eq!(
        cached_planar_locate(&prepared, probe, false)
            .unwrap()
            .to_bits(),
        0.5_f64.to_bits(),
    );
    assert_eq!(
        locate_planar_route(std::iter::once(&coordinates), probe, false)
            .unwrap()
            .to_bits(),
        0.5_f64.to_bits(),
    );
    let old_point = Point::new_raw_xy(1e308, 0.0);
    let new_point = Point::new_raw_xy(1e308, 0.5);
    assert_eq!(
        point_distance(probe, old_point).to_bits(),
        f64::INFINITY.to_bits()
    );
    assert_eq!(
        point_distance(probe, new_point).to_bits(),
        f64::INFINITY.to_bits()
    );
    assert!(
        point_pair_distance_key(probe.xy(), new_point.xy())
            .cmp_distance(&point_pair_distance_key(probe.xy(), old_point.xy()))
            .unwrap()
            .is_lt()
    );
    assert_locate_interpolate_matches_proximity(&prepared, probe);
}
#[test]
fn planar_locate_prefers_interior_on_endpoint_interior_exact_tie() {
    let shape = crate::io::parse_wkt("MULTILINESTRING ((0 1, 1 2), (-1 1, 1 1))")
        .expect("valid multipart line");
    let probe = Point::new_raw_xy(0.0, 0.0);
    let prepared = crate::geometry::PreparedPlan::new(shape);
    assert_eq!(
        cached_planar_locate(&prepared, probe, false)
            .expect("exact nearest tie")
            .to_bits(),
        2.414_213_562_373_095_f64.to_bits(),
        "segment 1's interior station is the diagonal length sqrt(2) plus one"
    );
}
#[test]
fn planar_locate_interpolate_matches_proximity_away_from_part_boundaries() {
    for (wkt, probe) in [
        ("LINESTRING (0 0, 8 0)", Point::new_raw_xy(2.0, 1.0)),
        ("LINESTRING (0 0, 0 4, 4 4)", Point::new_raw_xy(2.0, 5.0)),
        (
            "MULTILINESTRING ((0 0, 2 0), (10 0, 14 0))",
            Point::new_raw_xy(12.0, 1.0),
        ),
    ] {
        let prepared = crate::geometry::PreparedPlan::new(crate::io::parse_wkt(wkt).unwrap());
        assert_locate_interpolate_matches_proximity(&prepared, probe);
    }
}
#[test]
fn planar_one_shot_locate_streams_without_route_allocations() {
    let coordinates =
        CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(8.0, 0.0)])
            .unwrap();
    let (station, allocations) = crate::test_support::allocation_count(|| {
        locate_planar_route(
            std::iter::once(&coordinates),
            Point::new_raw_xy(2.0, 1.0),
            true,
        )
        .unwrap()
    });
    assert_eq!(station.to_bits(), 0.25_f64.to_bits());
    assert_eq!(allocations, 0);
}
#[test]
fn planar_locate_ordinals_preserve_shared_multipart_occurrences() {
    use crate::geometry::{CoordinateAxes, MultiLineString, MultiMember};
    let repeated = LineSeq::try_new(
        CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(2.0, 0.0)])
            .unwrap(),
    )
    .unwrap();
    let final_line = LineSeq::try_new(
        CoordSeq::try_from_points(&[Point::new_raw_xy(10.0, 0.0), Point::new_raw_xy(14.0, 0.0)])
            .unwrap(),
    )
    .unwrap();
    let shape = Shape::MultiLineString(
        MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(repeated.clone()),
            MultiMember::Value(repeated),
            MultiMember::Empty,
            MultiMember::Value(final_line),
        ])
        .unwrap(),
    );
    let prepared = crate::geometry::PreparedPlan::new(shape);
    assert_eq!(
        cached_planar_locate(&prepared, Point::new_raw_xy(12.0, 1.0), false)
            .unwrap()
            .to_bits(),
        6.0_f64.to_bits(),
    );
    assert_eq!(
        prepared.distance_parts().unwrap().linework.segment_count(),
        3
    );
}
#[test]
fn prepared_planar_locate_matches_streaming_across_seeded_ties_zero_edges_and_empty_parts() {
    use crate::geometry::numerics::product::exact_power_of_two;
    use crate::geometry::{CoordinateAxes, MultiLineString, MultiMember};

    let line = |points: &[Point]| {
        LineSeq::try_new(CoordSeq::try_from_points(points).expect("valid test coordinates"))
            .expect("valid test line")
    };
    let tie_above = line(&[Point::new_raw_xy(-1.0, 1.0), Point::new_raw_xy(1.0, 1.0)]);
    let zero_at_probe = line(&[
        Point::new_raw_xy(100.0, 100.0),
        Point::new_raw_xy(100.0, 100.0),
    ]);
    let tie_below = line(&[Point::new_raw_xy(-1.0, -1.0), Point::new_raw_xy(1.0, -1.0)]);
    let near_zero_probe = line(&[
        Point::new_raw_xy(99.0, 99.0),
        Point::new_raw_xy(101.0, 99.0),
    ]);

    let mut state = 0x317_u64;
    let seeded_points: Vec<_> = (0_usize..=128)
        .map(|index| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let y = 1_000.0 + ((state >> 32) % 101) as f64 / 1_000.0;
            Point::new_raw_xy(1_000.0 + index as f64 * 4.0, y)
        })
        .collect();
    let repeated_vertex = *seeded_points
        .get(64)
        .expect("seeded line contains the repeated-vertex site");
    let mut seeded_points = seeded_points;
    seeded_points.insert(64, repeated_vertex);
    let seeded_coordinates =
        CoordSeq::try_from_points(&seeded_points).expect("valid seeded coordinates");
    let seeded_line = LineSeq::try_new(seeded_coordinates.clone()).expect("valid seeded line");
    let shape = Shape::MultiLineString(
        MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(tie_above),
            MultiMember::Empty,
            MultiMember::Value(zero_at_probe),
            MultiMember::Value(tie_below),
            MultiMember::Value(near_zero_probe),
            MultiMember::Empty,
            MultiMember::Value(line(&[
                Point::new_raw_xy(exact_power_of_two(-27), -9.0),
                Point::new_raw_xy(1.0, -9.0),
            ])),
            MultiMember::Value(line(&[
                Point::new_raw_xy(0.0, -9.0),
                Point::new_raw_xy(-1.0, -9.0),
            ])),
            MultiMember::Value(line(&[
                Point::new_raw_xy(0.0, -19.0),
                Point::new_raw_xy(1.0, -18.0),
            ])),
            MultiMember::Value(line(&[
                Point::new_raw_xy(-1.0, -19.0),
                Point::new_raw_xy(1.0, -19.0),
            ])),
            MultiMember::Value(seeded_line.clone()),
        ])
        .expect("valid multipart line"),
    );
    let prepared = crate::geometry::PreparedPlan::new(shape.clone());
    assert!(prepared.distance_parts().unwrap().bvh().is_some());
    let mut probes = vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(100.0, 100.0),
        Point::new_raw_xy(0.0, -10.0),
        Point::new_raw_xy(0.0, -20.0),
    ];
    for ordinal in [0, 7, 31, 63, 64, 127, 128] {
        let start = *seeded_points
            .get(ordinal)
            .expect("seeded segment starts in the line");
        let end = *seeded_points
            .get(ordinal.wrapping_add(1))
            .expect("seeded segment has a following vertex");
        probes.push(Point::new_raw_xy(
            start.x.midpoint(end.x),
            start.y.midpoint(end.y) + 0.25,
        ));
    }

    for probe in probes {
        for normalized in [false, true] {
            let expected =
                locate_planar_route(shape.linework().unwrap().into_iter(), probe, normalized)
                    .expect("one-shot streaming reference");
            assert_eq!(
                cached_planar_locate(&prepared, probe, normalized)
                    .expect("prepared nearest search")
                    .to_bits(),
                expected.to_bits(),
                "prepared locate differs from streaming at {probe:?}, normalized={normalized}"
            );
        }
    }
    assert_eq!(
        cached_planar_locate(&prepared, Point::new_raw_xy(0.0, 0.0), false)
            .expect("exact nearest tie")
            .to_bits(),
        1.0_f64.to_bits(),
        "the first source segment wins an exact distance tie across multipart EMPTY members"
    );
    assert_eq!(
        cached_planar_locate(&prepared, Point::new_raw_xy(100.0, 100.0), false)
            .expect("zero-length segment is not a locate candidate")
            .to_bits(),
        5.0_f64.to_bits(),
        "locate skips the coincident zero-length member and stationing crosses its prefix"
    );
    let seeded_shape = Shape::LineString(seeded_line);
    let seeded_prepared = crate::geometry::PreparedPlan::new(seeded_shape);
    for ordinal in [0, 7, 31, 63, 64, 127, 128] {
        let start = *seeded_points
            .get(ordinal)
            .expect("seeded segment starts in the line");
        let end = *seeded_points
            .get(ordinal.wrapping_add(1))
            .expect("seeded segment has a following vertex");
        let probe = Point::new_raw_xy(start.x.midpoint(end.x), start.y.midpoint(end.y) + 0.25);
        let expected = crate::line_locate_coordseq(
            &crate::crs::MetricModel::COORDINATE,
            &seeded_coordinates,
            probe,
            false,
        )
        .expect("packed one-shot reference");
        assert_eq!(
            cached_planar_locate(&seeded_prepared, probe, false)
                .expect("prepared nearest search")
                .to_bits(),
            expected.to_bits(),
            "prepared locate differs from packed streaming at {probe:?}"
        );
    }
}
#[test]
fn chart_locate_ordinals_preserve_zero_edges_and_shared_multipart_occurrences() {
    use geographiclib_rs::Geodesic;

    use crate::crs::EllipsoidMetric;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{MultiLineString, MultiMember};

    let chain = LineSeq::try_new(
        CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
        ])
        .unwrap(),
    )
    .unwrap();
    let shape = Shape::MultiLineString(
        MultiLineString::try_new(chain.axes(), vec![
            MultiMember::Value(chain.clone()),
            MultiMember::Value(chain),
        ])
        .unwrap(),
    );
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, GeographicFrame::crs84());
    let index = ChartLineIndex::build_chart(&shape, &metric).unwrap();
    assert_eq!(index.parts().edges.len(), 6);
    assert_eq!(index.prefix.len(), 7);
    assert_eq!(index.prefix[0].to_bits(), index.prefix[1].to_bits());
    assert_eq!(index.prefix[3].to_bits(), index.prefix[4].to_bits());
    let probe = Point::new_raw_xy(1.5, 0.0);
    let (ordinal, parameter) = point_chart_locate(probe, index.parts(), &metric, |part| {
        !part.edge.is_degenerate()
    })
    .unwrap()
    .unwrap();
    assert_eq!(ordinal, 2);
    assert!((parameter - 0.5).abs() < 1e-14);
    let station = index.locate_chart_point(probe, false, &metric).unwrap();
    assert!((station - 166_979.236_189_910_36).abs() < 4e-9);
    assert!((index.locate_chart_point(probe, true, &metric).unwrap() - 0.375).abs() < 2e-15);
}
#[test]
fn chart_bvh_locate_keeps_earliest_ordinal_when_a_later_leaf_is_visited_first() {
    use geographiclib_rs::Geodesic;

    use crate::crs::EllipsoidMetric;
    use crate::geographic::GeographicFrame;
    let shape = crate::io::parse_wkt(
        "MULTILINESTRING ((0 1, 2 1), (10 20, 11 20), (20 20, 21 20), \
             (30 20, 31 20), (40 20, 41 20), (-40 20, -39 20), \
             (-30 20, -29 20), (-20 20, -19 20), (-10 1, 0 1))",
    )
    .unwrap();
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, GeographicFrame::crs84());
    let index = ChartLineIndex::build_chart(&shape, &metric).unwrap();
    let probe = Point::new_raw_xy(0.0, 0.0);
    let mut best = f64::INFINITY;
    let mut visited = Vec::new();
    index
        .parts()
        .bvh()
        .unwrap()
        .visit_point_candidates(
            &index.parts().edges,
            probe,
            &metric,
            &mut best,
            None,
            &mut Vec::new(),
            |ordinal, _, _| {
                visited.push(ordinal);
                Ok::<_, crate::error::Error>(None::<()>)
            },
        )
        .unwrap();
    assert!(
        visited.iter().position(|&ordinal| ordinal == 8).unwrap()
            < visited.iter().position(|&ordinal| ordinal == 0).unwrap()
    );
    let (ordinal, _) = point_chart_locate(probe, index.parts(), &metric, |_| true)
        .unwrap()
        .unwrap();
    assert_eq!(ordinal, 0);
    assert_eq!(
        index
            .locate_chart_point(probe, false, &metric)
            .unwrap()
            .to_bits(),
        0.0_f64.to_bits()
    );
}
#[test]
fn cached_chart_locate_preserves_source_then_query_domain_error_precedence() {
    let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
    let metric = crate::crs::ResolvedMetric::from_model(&model).unwrap();
    let invalid = crate::geometry::PreparedPlan::new(
        crate::io::parse_wkt("LINESTRING (0 100, 1 100)").unwrap(),
    );
    let invalid_probe = Point::new_raw_xy(2.0, 101.0);
    let cache = crate::geometry::FrameDependentCaches::default();
    let source_error = crate::crs::ensure_geographic_domain_in_frame(
        crate::geographic::GeographicFrame::crs84(),
        invalid.shape(),
    )
    .unwrap_err()
    .to_string();
    assert_eq!(
        prepare_line_locate(&metric, &invalid, &cache, invalid_probe, false, false,)
            .map(|(_, located)| located)
            .unwrap_err()
            .to_string(),
        source_error,
    );
    let valid = crate::geometry::PreparedPlan::new(
        crate::io::parse_wkt("LINESTRING (0 10, 1 10)").unwrap(),
    );
    let query_error = crate::crs::ensure_geographic_lonlat_in_frame(
        crate::geographic::GeographicFrame::crs84(),
        invalid_probe.x,
        invalid_probe.y,
    )
    .unwrap_err()
    .to_string();
    for _ in 0..2 {
        assert_eq!(
            prepare_line_locate(&metric, &valid, &cache, invalid_probe, false, false,)
                .map(|(_, located)| located)
                .unwrap_err()
                .to_string(),
            query_error,
        );
    }
}
