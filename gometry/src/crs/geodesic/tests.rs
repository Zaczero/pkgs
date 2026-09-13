use geographiclib_rs::Geodesic;

use super::distance::{geodesic_frechet_dp, geodesic_frechet_dp_with_scratch};
use super::lower_bound::{LowerBoundKernel, exact_auxiliary_sphere_bound};
use super::segment::geodesic_point_segment_query;
use super::{
    EllipsoidMetric, GEODESIC_BACKEND_RELATIVE_FLOOR, GEODESIC_LINE_CACHE, geodesic_counters,
};
use crate::error::{ErrorKind, ResourceError, Result};
use crate::geographic::{AngularFactor, GeographicFrame};
use crate::geometry::{
    CoordSeq, Coordinates, FrameDependentCaches, FrechetDpScratch, GeodesicMetric, GeodesicSegment,
    GeodesicSegmentMinimum, GeodesicSegmentQuery, GeodesicSegmentQueryResult,
    GeodesicSegmentScratch, GeodesicThresholdCompletion, GeodesicWitnessNeed, LineSeq, Point,
    PreparedPlan, Shape, XY,
};

fn p(x: f64, y: f64) -> Point {
    Point::new_raw_xy(x, y)
}

#[derive(Clone, Copy)]
struct TestWitness {
    distance: f64,
    foot: Point,
    along: f64,
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "test helpers consume the owned geodesic segment snapshot"
)]
fn raw_minimum(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    cutoff: f64,
) -> GeodesicSegmentMinimum {
    let mut scratch = GeodesicSegmentScratch::default();
    match geodesic_point_segment_query(
        geodesic,
        point,
        segment,
        GeodesicSegmentQuery::Minimum {
            cutoff,
            witness: GeodesicWitnessNeed::No,
        },
        &mut scratch,
    )
    .expect("point-to-segment query should succeed")
    {
        GeodesicSegmentQueryResult::Attained {
            minimum,
            foot: None,
        } => minimum,
        GeodesicSegmentQueryResult::Attained { foot: Some(_), .. } => {
            panic!("distance query unexpectedly materialized a witness")
        },
        GeodesicSegmentQueryResult::Excluded { .. } => {
            panic!("unpruned test query was unexpectedly excluded")
        },
        GeodesicSegmentQueryResult::ThresholdOutside { .. }
        | GeodesicSegmentQueryResult::ThresholdResolvedOutside { .. }
        | GeodesicSegmentQueryResult::ThresholdUnresolved => {
            panic!("minimum query returned a threshold outcome")
        },
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "test helpers consume the owned geodesic segment snapshot"
)]
fn metric_witness(
    metric: &impl GeodesicMetric,
    point: Point,
    segment: GeodesicSegment,
    cutoff: f64,
) -> TestWitness {
    let mut scratch = GeodesicSegmentScratch::default();
    match metric
        .point_segment_query(
            point,
            segment,
            GeodesicSegmentQuery::Minimum {
                cutoff,
                witness: GeodesicWitnessNeed::Yes,
            },
            &mut scratch,
        )
        .expect("point-to-segment query should succeed")
    {
        GeodesicSegmentQueryResult::Attained {
            minimum,
            foot: Some(foot),
        } => TestWitness {
            distance: minimum.distance,
            foot,
            along: minimum.along,
        },
        GeodesicSegmentQueryResult::Attained { foot: None, .. } => {
            panic!("witness query did not materialize a foot")
        },
        GeodesicSegmentQueryResult::Excluded { .. } => {
            panic!("unpruned test query was unexpectedly excluded")
        },
        GeodesicSegmentQueryResult::ThresholdOutside { .. }
        | GeodesicSegmentQueryResult::ThresholdResolvedOutside { .. }
        | GeodesicSegmentQueryResult::ThresholdUnresolved => {
            panic!("minimum query returned a threshold outcome")
        },
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "test helpers consume the owned geodesic segment snapshot"
)]
fn metric_threshold(
    metric: &impl GeodesicMetric,
    point: Point,
    segment: GeodesicSegment,
    limit: f64,
) -> bool {
    let mut scratch = GeodesicSegmentScratch::default();
    matches!(
        metric
            .point_segment_query(
                point,
                segment,
                GeodesicSegmentQuery::Threshold {
                    limit,
                    completion: GeodesicThresholdCompletion::Complete,
                    witness: GeodesicWitnessNeed::No,
                },
                &mut scratch,
            )
            .expect("threshold query should succeed"),
        GeodesicSegmentQueryResult::Attained { .. }
    )
}

fn line(points: &[Point]) -> Shape {
    Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(points).unwrap()).expect("test line is valid"),
    )
}

fn multipoint(points: Vec<Point>) -> Shape {
    Shape::MultiPoint(points.into())
}

fn full_edges(shape: &Shape, metric: &impl GeodesicMetric) -> Vec<GeodesicSegment> {
    let mut edges = Vec::new();
    shape.for_each_vertex_pair(|start, end| {
        edges.push(
            metric
                .make_segment(start, end)
                .expect("unique test segment"),
        );
    });
    edges
}

fn point_only_points(shape: &Shape) -> Vec<Point> {
    let mut points = Vec::new();
    append_point_only(shape, &mut points);
    points
}

fn append_point_only(shape: &Shape, out: &mut Vec<Point>) {
    match shape {
        Shape::Point(point) => out.push(*point),
        Shape::MultiPoint(points) => {
            for point in points {
                out.push(point);
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                append_point_only(geometry, out);
            }
        },
        _ => {},
    }
}

#[test]
fn auxiliary_unit_uses_native_xy_conversion_and_raw_zero_semantics() {
    let geodesic = Geodesic::wgs84();
    let degree = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
    let grad = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
    let native = GeographicFrame::try_new(degree, grad, 2.0_f64.to_radians()).unwrap();
    let native_metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, native);
    let canonical_metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);

    let native_xy = XY::new(0.0, 50.0);
    let (longitude, latitude) = native.to_geodesic_xy(native_xy.x, native_xy.y).unwrap();
    assert_eq!(longitude, 2.0);
    assert!((latitude - 45.0).abs() <= 1e-12);
    assert_eq!(
        native_metric.auxiliary_unit(native_xy),
        canonical_metric.auxiliary_unit(XY::new(longitude, latitude))
    );

    let positive_latitude = canonical_metric.auxiliary_unit(XY::new(1.0, 0.0));
    let negative_latitude = canonical_metric.auxiliary_unit(XY::new(1.0, -0.0));
    assert_eq!(
        positive_latitude[0].to_bits(),
        negative_latitude[0].to_bits()
    );
    assert_eq!(
        positive_latitude[1].to_bits(),
        negative_latitude[1].to_bits()
    );
    assert_ne!(
        positive_latitude[2].to_bits(),
        negative_latitude[2].to_bits()
    );
}

#[test]
fn point_segment_witness_converts_native_frame_coordinates() {
    let geodesic = Geodesic::wgs84();
    let degree = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
    let grad = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
    let frame = GeographicFrame::try_new(degree, grad, 2.0_f64.to_radians()).unwrap();
    let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, frame);
    let segment = metric
        .make_segment(p(-1.0, 0.0), p(1.0, 0.0))
        .expect("native equatorial segment is unique");
    let witness = metric_witness(&metric, p(0.0, 1.0), segment, f64::INFINITY);
    let expected = EllipsoidMetric::for_crs84_geometry_test(&geodesic)
        .segment_length(p(2.0, 0.9), p(2.0, 0.0));

    assert!((witness.distance - expected).abs() <= 1e-6);
    assert!(witness.foot.x.abs() <= 1e-12);
    assert!(witness.foot.y.abs() <= 1e-12);
    assert!((witness.distance - metric.segment_length(p(0.0, 1.0), witness.foot)).abs() <= 1e-6);
}

#[test]
fn geodesic_point_segment_keeps_clamped_endpoint() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let point = p(-102.230_290_366_388_32, 77.320_571_012_858_38);
    let segment = metric
        .make_segment(
            p(71.410_681_476_172_64, -75.947_036_150_361_91),
            p(153.078_194_846_761_5, 71.124_469_524_788_08),
        )
        .expect("fixture segment is unique");
    let actual = raw_minimum(&geodesic, point, segment, f64::INFINITY);
    assert!((actual.distance - 2_809_179.208_891_459).abs() <= 1e-3);
    assert_eq!(actual.along.to_bits(), segment.length.to_bits());
}

#[test]
fn geodesic_line_cache_reuses_directed_segment_lines() {
    GEODESIC_LINE_CACHE.with(|cache| cache.borrow_mut().clear());
    geodesic_counters::reset();
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let segment = metric
        .make_segment(p(-1.0, 0.0), p(1.0, 0.0))
        .expect("unique test segment");
    let point = p(0.0, 1.0);

    let first = raw_minimum(&geodesic, point, segment, f64::INFINITY);
    let after_first = geodesic_counters::snapshot();
    let second = raw_minimum(&geodesic, point, segment, f64::INFINITY);
    let after_second = geodesic_counters::snapshot();

    assert_eq!(first.distance.to_bits(), second.distance.to_bits());
    assert_eq!(first.along.to_bits(), second.along.to_bits());
    assert_eq!(after_first.line_cache_misses, 1);
    assert_eq!(after_first.line_cache_hits, 0);
    assert_eq!(after_second.line_cache_misses, 1);
    assert_eq!(after_second.line_cache_hits, 1);
    GEODESIC_LINE_CACHE.with(|cache| assert_eq!(cache.borrow().len(), 1));
}

#[test]
fn geodesic_point_segment_distance_is_attained_at_refined_argument() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let segment_lengths = [0.25, 1.0, 5.0, 10.0, 30.0];
    let latitudes = [1e-9, 1e-7, 1e-5, 1e-4, 1e-2, 1.0, 20.0];

    for segment_length in segment_lengths {
        let segment = metric
            .make_segment(p(0.0, 0.0), p(segment_length, 0.0))
            .expect("equatorial segment is unique");
        let exhibited = p(segment_length / 2.0, 0.0);
        for latitude in latitudes {
            let query = p(segment_length / 2.0, latitude);
            let witness = metric_witness(&metric, query, segment, f64::INFINITY);
            let exhibited_distance = metric.segment_length(query, exhibited);
            let witness_distance = metric.segment_length(query, witness.foot);
            let rounding_floor =
                32.0 * f64::EPSILON * exhibited_distance.max(witness_distance).max(1.0);

            assert!(
                witness.distance <= exhibited_distance + rounding_floor,
                "segment {segment_length} latitude {latitude}: {} > {}",
                witness.distance,
                exhibited_distance
            );
            assert!(
                (witness.distance - witness_distance).abs() <= rounding_floor,
                "segment {segment_length} latitude {latitude}: reported {} is not attained by {}",
                witness.distance,
                witness_distance
            );
        }
    }
}

#[test]
fn geodesic_point_to_segment_threshold_is_inclusive() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let segment = metric
        .make_segment(p(-10.0, 0.0), p(10.0, 0.0))
        .expect("unique test segment");

    let on_segment = p(0.0, 0.0);
    let on_segment_distance = metric_witness(&metric, on_segment, segment, f64::INFINITY).distance;
    assert!(
        metric_threshold(&metric, on_segment, segment, 0.0),
        "sampled on-segment distance: {on_segment_distance}"
    );
    assert!(!metric_threshold(&metric, p(0.0, 1.0), segment, 0.0));
}

#[test]
fn spherical_segment_phase_selects_the_nearest_stationary_point() {
    let radius = 6_378_137.0;
    let geodesic = Geodesic::new(radius, 0.0);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let segment = metric
        .make_segment(p(-45.0, 0.0), p(45.0, 0.0))
        .expect("short spherical arc is unique");
    let interior = metric_witness(&metric, p(0.0, 10.0), segment, f64::INFINITY);
    let expected = metric.segment_length(p(0.0, 10.0), p(0.0, 0.0));

    assert!(interior.along > 0.0 && interior.along < segment.length);
    assert!((interior.distance - expected).abs() <= 1e-6);
    assert!(interior.foot.x.abs() <= 1e-12);
    assert!(interior.foot.y.abs() <= 1e-12);

    // For this geometry A and B are both positive and atan2(B, A) lies
    // inside the arc. The old +π phase lies outside it and falls back to an
    // endpoint, which is strictly farther than the interior foot.
    let to_unit = |longitude: f64, latitude: f64| {
        let (sin_lat, cos_lat) = latitude.to_radians().sin_cos();
        let (sin_lon, cos_lon) = longitude.to_radians().sin_cos();
        [cos_lat * cos_lon, cos_lat * sin_lon, sin_lat]
    };
    let start = to_unit(-45.0, 0.0);
    let tangent = [
        45.0_f64.to_radians().sin(),
        45.0_f64.to_radians().cos(),
        0.0,
    ];
    let query = to_unit(0.0, 10.0);
    let coefficient_a = start[0] * query[0] + start[1] * query[1] + start[2] * query[2];
    let coefficient_b = tangent[0] * query[0] + tangent[1] * query[1] + tangent[2] * query[2];
    let wrong_phase = coefficient_b.atan2(coefficient_a) + std::f64::consts::PI;
    assert!(wrong_phase > segment.length / radius);
    let endpoint_distance = metric
        .segment_length(p(0.0, 10.0), segment.start)
        .min(metric.segment_length(p(0.0, 10.0), segment.end));
    assert!(endpoint_distance > interior.distance);

    let endpoint = metric_witness(&metric, p(100.0, 10.0), segment, f64::INFINITY);
    assert_eq!(endpoint.along.to_bits(), segment.length.to_bits());
}

#[test]
fn near_geodesic_contact_is_not_tolerance_contact() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let segment = metric
        .make_segment(p(-1.0, 0.0), p(1.0, 0.0))
        .expect("equatorial segment is unique");
    for latitude in [1e-12, -1e-12] {
        let query = p(0.0, latitude);
        let witness = metric_witness(&metric, query, segment, f64::INFINITY);

        assert!(witness.distance > 0.0);
        assert!(!metric_threshold(&metric, query, segment, 0.0));
        assert!(witness.foot.x.abs() <= 1e-12);
        assert!(witness.foot.y.abs() <= 1e-12);
        assert_ne!(witness.foot.y.to_bits(), query.y.to_bits());
    }

    let reversed = metric
        .make_segment(p(1.0, 0.0), p(-1.0, 0.0))
        .expect("reversed equatorial segment is unique");
    let witness = metric_witness(&metric, p(0.0, -1e-12), reversed, f64::INFINITY);
    assert!(witness.along > 0.0 && witness.along < reversed.length);
    assert!(!metric_threshold(&metric, p(0.0, -1e-12), reversed, 0.0));
}

#[test]
fn typed_queries_keep_attainment_exclusion_and_probe_state_distinct() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let segment = metric
        .make_segment(p(-10.0, 0.0), p(10.0, 0.0))
        .expect("equatorial segment is unique");
    let query = p(0.0, 1.0);

    let mut scratch = GeodesicSegmentScratch::default();
    let result = metric
        .point_segment_query(
            query,
            segment,
            GeodesicSegmentQuery::Minimum {
                cutoff: f64::INFINITY,
                witness: GeodesicWitnessNeed::No,
            },
            &mut scratch,
        )
        .expect("minimum query should succeed");
    let GeodesicSegmentQueryResult::Attained {
        minimum,
        foot: None,
    } = result
    else {
        panic!("minimum query did not return an unmaterialized attained value");
    };
    assert!(minimum.distance > 0.0);

    let equal_cutoff = metric
        .point_segment_query(
            query,
            segment,
            GeodesicSegmentQuery::Minimum {
                cutoff: minimum.distance,
                witness: GeodesicWitnessNeed::No,
            },
            &mut scratch,
        )
        .expect("equal cutoff query should succeed");
    let GeodesicSegmentQueryResult::Excluded { lower_bound } = equal_cutoff else {
        panic!("equal cutoff must not be an attained strict improvement");
    };
    assert!(lower_bound >= minimum.distance);

    let equal_limit = metric
        .point_segment_query(
            query,
            segment,
            GeodesicSegmentQuery::Threshold {
                limit: minimum.distance,
                completion: GeodesicThresholdCompletion::Complete,
                witness: GeodesicWitnessNeed::No,
            },
            &mut scratch,
        )
        .expect("equal threshold query should succeed");
    assert!(matches!(
        equal_limit,
        GeodesicSegmentQueryResult::Attained { .. }
    ));

    let outside = metric
        .point_segment_query(
            query,
            segment,
            GeodesicSegmentQuery::Threshold {
                limit: 0.0,
                completion: GeodesicThresholdCompletion::Complete,
                witness: GeodesicWitnessNeed::No,
            },
            &mut scratch,
        )
        .expect("outside threshold query should succeed");
    let GeodesicSegmentQueryResult::ThresholdOutside { lower_bound } = outside else {
        panic!("zero threshold must be proven outside");
    };
    assert!(lower_bound > 0.0);

    let probe_segment = metric
        .make_segment(p(30.0, -20.0), p(40.0, -10.0))
        .expect("probe segment is unique");
    let unresolved = metric
        .point_segment_query(
            p(-120.0, 70.0),
            probe_segment,
            GeodesicSegmentQuery::Threshold {
                limit: 0.0,
                completion: GeodesicThresholdCompletion::Probe,
                witness: GeodesicWitnessNeed::No,
            },
            &mut scratch,
        )
        .expect("probe query should succeed");
    assert!(matches!(
        unresolved,
        GeodesicSegmentQueryResult::ThresholdUnresolved
    ));
}

#[test]
fn point_segment_query_reuses_one_prepared_line_and_closes_practically() {
    GEODESIC_LINE_CACHE.with(|cache| cache.borrow_mut().clear());
    geodesic_counters::reset();
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let segment = metric
        .make_segment(p(-20.0, -10.0), p(30.0, 20.0))
        .expect("test segment is unique");
    let witness = metric_witness(&metric, p(0.0, 5.0), segment, f64::INFINITY);
    let stats = geodesic_counters::snapshot();

    assert_eq!(stats.segment_queries, 1);
    assert_eq!(stats.line_cache_misses, 1);
    assert!(stats.segment_value_evaluations > 0);
    assert!(stats.segment_derivative_evaluations > 0);
    assert!(stats.segment_derivative_declines <= stats.segment_derivative_evaluations);
    assert!(stats.segment_interval_pops <= 128);
    assert!(stats.segment_interval_splits <= 128);
    assert!(stats.segment_convex_closures > 0);
    assert!(witness.along > 0.0 && witness.along < segment.length);
    assert!((witness.distance - metric.segment_length(p(0.0, 5.0), witness.foot)).abs() <= 1e-3);
}

#[test]
fn point_segment_query_propagates_scratch_allocation_failure() {
    use crate::test_support::fail_allocations_after;

    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let segment = metric
        .make_segment(p(-20.0, -10.0), p(30.0, 20.0))
        .expect("test segment is unique");
    let _ = metric_witness(&metric, p(0.0, 5.0), segment, f64::INFINITY);

    let mut scratch = GeodesicSegmentScratch::default();
    let result = fail_allocations_after(0, || {
        metric.point_segment_query(
            p(0.0, 5.0),
            segment,
            GeodesicSegmentQuery::Minimum {
                cutoff: f64::INFINITY,
                witness: GeodesicWitnessNeed::No,
            },
            &mut scratch,
        )
    });
    let Err(error) = result else {
        panic!("point-segment scratch allocation failure was swallowed");
    };
    assert!(matches!(
        error.kind(),
        ErrorKind::Resource(ResourceError::Allocation)
    ));
}

#[test]
fn prolate_boundary_point_segment_matches_dense_differential() {
    let geodesic = Geodesic::new(6_378_137.0, -0.02);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let cases = [
        ("seam", p(179.0, 0.0), p(-179.0, 0.0), p(179.5, 1.0)),
        ("pole", p(-45.0, 85.0), p(45.0, 85.0), p(0.0, 83.0)),
        (
            "equatorial_pole_plateau",
            p(-80.0, 0.0),
            p(80.0, 0.0),
            p(0.0, 90.0),
        ),
        ("near_cut", p(-80.0, 20.0), p(80.0, 20.0), p(0.0, -20.0)),
    ];

    for (name, start, end, query) in cases {
        let segment = metric
            .make_segment(start, end)
            .expect("test segment is unique");
        if name == "near_cut" {
            geodesic_counters::reset();
        }
        let witness = metric_witness(&metric, query, segment, f64::INFINITY);
        if name == "near_cut" {
            let stats = geodesic_counters::snapshot();
            assert!(stats.segment_interval_splits > 0);
            assert!(stats.segment_interval_pops >= stats.segment_interval_splits);
        }
        let mut dense = f64::INFINITY;
        for index in 0..=256 {
            let fraction = f64::from(index) / 256.0;
            dense =
                dense.min(metric.segment_length(query, metric.interpolate(start, end, fraction)));
        }
        assert!(
            witness.distance <= dense + 1e-3,
            "{name}: witness {} exceeded dense sample {}",
            witness.distance,
            dense
        );
        assert!(
            (witness.distance - metric.segment_length(query, witness.foot)).abs() <= 1e-3,
            "{name}: witness distance was not attained"
        );
        assert!(
            witness.along >= 0.0 && witness.along <= segment.length,
            "{name}: along-track witness escaped segment"
        );
    }
}

#[test]
fn point_segment_global_subdivision_matches_exact_reference() {
    let semi_major = 6_378_137.0;
    let geodesic = Geodesic::new(semi_major, 1.0 / 298.257_223_563);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let start = p(0.0, 0.0);
    let end = p(1.0, 1.0);
    let query = p(0.499_962_174_856_978_1, 0.500_019_548_801_697_6);
    // The nonzero flattening and diagonal segment bypass the exact equatorial,
    // meridian, and spherical families. The 10 µm normal offset also declines
    // the first derivative enclosure, forcing the general interval split.
    let segment = metric
        .make_segment(start, end)
        .expect("fixture segment is unique");

    let samples = (0..=256)
        .map(|index| {
            let fraction = f64::from(index) / 256.0;
            metric.segment_length(query, metric.interpolate(start, end, fraction))
        })
        .collect::<Vec<_>>();
    let local_minima = (1..256)
        .filter(|&index| samples[index] < samples[index - 1] && samples[index] < samples[index + 1])
        .count();
    assert!(
        local_minima > 0,
        "fixture must retain an interior local minimum"
    );

    // Reference values were generated from the fixed binary64 inputs above by
    // /tmp/opencode/segment_exact_nearcontact-normal10um.cpp, linked against the
    // pinned GeographicLib 2.7 GeodesicExact/GeodesicLineExact build recorded
    // in /tmp/opencode/geodesic-backend-profile/COMMANDS.txt.
    let reference_distance = 0.000_009_999_993_325_465_93;
    let reference_along = 78_449.784_145_670_39;
    let reference_foot = p(0.499_962_174_793_668_95, 0.500_019_548_865_860_5);
    let floor = GEODESIC_BACKEND_RELATIVE_FLOOR * semi_major;

    geodesic_counters::reset();
    let witness = metric_witness(&metric, query, segment, f64::INFINITY);
    let stats = geodesic_counters::snapshot();
    assert!(
        stats.segment_interval_splits > 0,
        "expected global subdivision, got {stats:?}"
    );
    assert!(stats.segment_interval_pops >= stats.segment_interval_splits);
    assert!(
        (witness.distance - reference_distance).abs() <= floor,
        "distance {} vs {reference_distance}, floor {floor}",
        witness.distance
    );
    assert!(
        (witness.along - reference_along).abs() <= floor,
        "along {} vs {reference_along}, floor {floor}; foot {:?} vs {reference_foot:?}",
        witness.along,
        witness.foot
    );
    assert!(
        metric.segment_length(witness.foot, reference_foot) <= floor,
        "foot {:?} vs {reference_foot:?}, floor {floor}",
        witness.foot
    );

    assert!(
        (witness.distance - metric.segment_length(query, witness.foot)).abs()
            <= 32.0 * f64::EPSILON * witness.distance.max(1.0)
    );
    assert!(witness.along >= 0.0 && witness.along <= segment.length);
}

#[test]
fn point_segment_global_frontier_keeps_multiple_local_minima() {
    let semi_major = 6_378_137.0;
    let geodesic = Geodesic::new(semi_major, -0.02);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let start = p(0.0, -20.0);
    let end = p(179.9, 10.0);
    let query = p(90.0, 0.0);
    let segment = metric
        .make_segment(start, end)
        .expect("fixture segment is unique");

    let samples = (0..=256)
        .map(|index| {
            let fraction = f64::from(index) / 256.0;
            metric.segment_length(query, metric.interpolate(start, end, fraction))
        })
        .collect::<Vec<_>>();
    let local_minima = (1..256)
        .filter(|&index| samples[index] < samples[index - 1] && samples[index] < samples[index + 1])
        .count();
    assert_eq!(local_minima, 2, "fixture must retain both local minima");

    // The global distance was generated from these fixed binary64 inputs by
    // /tmp/opencode/segment_exact_candidate_stored.cpp, linked against the
    // pinned GeographicLib 2.7 GeodesicExact/GeodesicLineExact build recorded
    // in /tmp/opencode/geodesic-backend-profile/COMMANDS.txt.
    let reference_distance = 9_975_904.231_800_672;
    let floor = GEODESIC_BACKEND_RELATIVE_FLOOR * semi_major;

    geodesic_counters::reset();
    let witness = metric_witness(&metric, query, segment, f64::INFINITY);
    let stats = geodesic_counters::snapshot();
    assert!(stats.segment_interval_splits > 1);
    assert!(stats.segment_interval_pops >= stats.segment_interval_splits);
    assert!((witness.distance - reference_distance).abs() <= floor);
    assert!(
        (witness.distance - metric.segment_length(query, witness.foot)).abs()
            <= 32.0 * f64::EPSILON * witness.distance
    );
    assert!(witness.along >= 0.0 && witness.along <= segment.length);

    let threshold = metric
        .point_segment_query(
            query,
            segment,
            GeodesicSegmentQuery::Threshold {
                limit: 10_000_000.0,
                completion: GeodesicThresholdCompletion::Complete,
                witness: GeodesicWitnessNeed::Yes,
            },
            &mut GeodesicSegmentScratch::default(),
        )
        .expect("complete threshold query should process the global frontier");
    let GeodesicSegmentQueryResult::Attained {
        minimum,
        foot: Some(foot),
    } = threshold
    else {
        panic!("the global minimum must satisfy the threshold");
    };
    assert!(minimum.distance <= 10_000_000.0);
    assert!(
        (minimum.distance - metric.segment_length(query, foot)).abs()
            <= 32.0 * f64::EPSILON * minimum.distance
    );
}

#[test]
fn geodesic_segment_intersection_uses_ellipsoidal_solver() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    assert!(
        metric
            .segments_cross(p(0.0, 0.0), p(10.0, 0.0), p(5.0, -1.0), p(5.0, 1.0))
            .expect("supported intersection")
    );
    let disjoint = metric
        .segments_cross(p(0.0, 0.0), p(10.0, 0.0), p(20.0, -1.0), p(20.0, 1.0))
        .expect("supported intersection");
    assert!(!disjoint);
    assert!(
        metric
            .segments_cross(p(0.0, 0.0), p(10.0, 0.0), p(5.0, 0.0), p(15.0, 0.0))
            .expect("supported intersection")
    );
}

#[test]
fn geodesic_segment_intersection_returns_on_curve_witnesses() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let first = metric
        .make_segment(p(-20.0, -10.0), p(30.0, 20.0))
        .expect("unique test segment");
    let second = metric
        .make_segment(p(-15.0, 20.0), p(25.0, -10.0))
        .expect("unique test segment");
    let (on_first, on_second) = metric
        .segment_intersection(first.start, first.end, second.start, second.end)
        .expect("supported intersection")
        .expect("segments cross");
    assert!(metric.segment_length(on_first, on_second) <= 1e-3);
    assert!(metric_witness(&metric, on_first, first, f64::INFINITY).distance <= 1e-3);
    assert!(metric_witness(&metric, on_second, second, f64::INFINITY).distance <= 1e-3);
}

#[test]
fn geodesic_segment_rejects_nonunique_shortest_endpoints() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    metric
        .make_segment(p(0.0, 10.0), p(180.0, -10.0))
        .unwrap_err();
    metric
        .make_segment(p(0.0, 90.0), p(180.0, -90.0))
        .unwrap_err();
    let sphere = Geodesic::new(6_378_137.0, 0.0);
    let sphere_metric = EllipsoidMetric::for_crs84_geometry_test(&sphere);
    sphere_metric
        .make_segment(p(0.0, 0.0), p(180.0, 0.0))
        .unwrap_err();
    let prolate = Geodesic::new(6_378_137.0, -0.01);
    let prolate_metric = EllipsoidMetric::for_crs84_geometry_test(&prolate);
    prolate_metric
        .make_segment(p(0.0, 10.0), p(180.0, -10.0))
        .unwrap_err();
}

#[test]
fn tabulated_lower_bound_never_exceeds_exact_auxiliary_bound_dense_sweep() {
    let cases = [
        ("wgs84", Geodesic::wgs84(), 0.005),
        ("k_0_9", Geodesic::new(6_378_137.0, 0.1), 0.1),
    ];
    let mut latitudes = (-360..=360)
        .map(|index| f64::from(index) * 0.25)
        .collect::<Vec<_>>();
    latitudes.extend([
        -90.0,
        -89.999_999,
        -89.999,
        -45.000_001,
        -0.000_001,
        0.0,
        0.000_001,
        45.000_001,
        89.999,
        89.999_999,
        90.0,
    ]);
    latitudes.sort_by(f64::total_cmp);
    latitudes.dedup_by(|left, right| left.to_bits() == right.to_bits());
    let deltas = [0.0, 1e-9, 1e-6, 0.1, 45.0, 179.999_999, 360.0, 720.0];
    let offsets = [0, 1, 2, 5, 17, 73, latitudes.len() / 2];

    for (name, geodesic, max_allowed_loss) in cases {
        let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
        let LowerBoundKernel::Tabulated(_) = LowerBoundKernel::for_geodesic(&geodesic) else {
            panic!("{name} should use the tabulated lower-bound kernel");
        };
        let mut max_loss = 0.0_f64;
        for (left_index, &lat1) in latitudes.iter().enumerate() {
            for offset in offsets {
                let lat2 = latitudes[(left_index + offset) % latitudes.len()];
                for delta_lon in deltas {
                    let a = p(-123.456_789, lat1);
                    let b = p(-123.456_789 + delta_lon, lat2);
                    let tabulated = metric.point_distance_lower_bound(a, b);
                    let exact = exact_auxiliary_sphere_bound(&geodesic, a, b);
                    assert!(
                        tabulated <= exact,
                        "{name}: tabulated {tabulated} exceeded exact {exact} for lat {lat1}/{lat2} dlon {delta_lon}"
                    );
                    max_loss = max_loss.max(exact - tabulated);
                }
            }
        }
        assert!(
            max_loss < max_allowed_loss,
            "{name}: max lower-bound loss {max_loss} >= {max_allowed_loss}"
        );
    }
}

#[test]
fn non_tabulated_flattening_uses_exact_oblate_lane() {
    let high_flattening = Geodesic::new(6_378_137.0, 0.2);
    let exact_metric = EllipsoidMetric::for_crs84_geometry_test(&high_flattening);
    let a = p(-12.0, -45.0);
    let b = p(78.0, 41.0);
    assert_eq!(
        exact_metric.point_distance_lower_bound(a, b).to_bits(),
        exact_auxiliary_sphere_bound(&high_flattening, a, b).to_bits()
    );
}

#[test]
fn disabled_lower_bound_is_zero_across_latitudes() {
    let geodesic = Geodesic::new(6_378_137.0, -0.01);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let cases = [
        ("equatorial", p(0.0, 0.0), p(1.0, 0.0)),
        ("southern_midlatitude", p(-12.0, -45.0), p(78.0, -41.0)),
        ("northern_midlatitude", p(-12.0, 45.0), p(78.0, 41.0)),
        ("polar", p(0.0, 90.0), p(180.0, 89.0)),
    ];
    for (name, a, b) in cases {
        assert_eq!(
            metric.point_distance_lower_bound(a, b).to_bits(),
            0.0_f64.to_bits(),
            "{name}: disabled lower bound"
        );
    }
}

fn brute_geodesic_distance_and_nearest_points(
    left: &Shape,
    right: &Shape,
    metric: &impl GeodesicMetric,
) -> Option<(f64, (Point, Point))> {
    let left_points = left.points_vec().unwrap();
    let right_points = right.points_vec().unwrap();
    if left_points.is_empty() || right_points.is_empty() {
        return None;
    }
    if left.intersects(right).unwrap() {
        return left
            .nearest_points(right)
            .unwrap()
            .map(|pair| (metric.segment_length(pair.0, pair.1), pair));
    }
    let left_edges = full_edges(left, metric);
    let right_edges = full_edges(right, metric);
    let left_point_only = point_only_points(left);
    let right_point_only = point_only_points(right);
    let mut best = f64::INFINITY;
    let mut pair = None;
    for &probe in &left_points {
        for &segment in &right_edges {
            let witness = metric_witness(metric, probe, segment, f64::INFINITY);
            if witness.distance < best {
                best = witness.distance;
                pair = Some((probe, witness.foot));
            }
        }
        for &target in &right_point_only {
            let distance = metric.segment_length(probe, target);
            if distance < best {
                best = distance;
                pair = Some((probe, target));
            }
        }
    }
    for &probe in &right_points {
        for &segment in &left_edges {
            let witness = metric_witness(metric, probe, segment, f64::INFINITY);
            if witness.distance < best {
                best = witness.distance;
                pair = Some((witness.foot, probe));
            }
        }
        for &target in &left_point_only {
            let distance = metric.segment_length(probe, target);
            if distance < best {
                best = distance;
                pair = Some((target, probe));
            }
        }
    }
    pair.map(|pair| (best, pair))
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn prolate_lower_bound_is_zero_and_cached_distance_paths_match_brute() -> Result<()> {
    let semi_major: f64 = 6_378_137.0;
    let flattening: f64 = -0.02;
    let semi_minor = semi_major * (1.0 - flattening);
    assert!((semi_minor - 6_505_699.74).abs() <= 1e-2);
    let geodesic = Geodesic::new(semi_major, flattening);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);

    let equator_a = p(0.0, 0.0);
    let equator_b = p(1.0, 0.0);
    assert!(
        (metric.segment_length(equator_a, equator_b) - semi_major * 1.0_f64.to_radians()).abs()
            < 1e-9
    );

    let left_table = [
        ("t0", p(-18.0, -3.0)),
        ("t1_7", p(-12.857_142_857_1, -2.218_168_517_5)),
        ("t2_7", p(-7.714_285_714_3, -2.025_072_087_8)),
        ("t3_7", p(-2.571_428_571_4, -2.566_116_260_9)),
        ("t4_7", p(2.571_428_571_4, -3.433_883_739_1)),
        ("t5_7", p(7.714_285_714_3, -3.974_927_912_2)),
        ("t6_7", p(12.857_142_857_1, -3.781_831_482_5)),
        ("t7_7", p(18.0, -3.0)),
        ("special", p(0.011, 0.0)),
    ];
    let left = multipoint(left_table.into_iter().map(|(_, point)| point).collect());
    let right_table = [
        ("south_endpoint", p(0.0, -2.5)),
        ("south_outer", p(0.0, -1.875)),
        ("south_inner", p(0.0, -1.25)),
        ("south_near", p(0.0, -0.625)),
        ("central_south", p(0.0, -0.125)),
        ("central_north", p(0.0, 0.625)),
        ("north_inner", p(0.0, 1.25)),
        ("north_outer", p(0.0, 1.875)),
        ("north_endpoint", p(0.0, 2.5)),
    ];
    let right = line(&right_table.map(|(_, point)| point));

    let (expected_distance, expected_nearest) =
        brute_geodesic_distance_and_nearest_points(&left, &right, &metric)
            .expect("non-empty shapes");
    let left_data = PreparedPlan::new(left).unwrap();
    let right_data = PreparedPlan::new(right).unwrap();
    let left_cache = FrameDependentCaches::default();
    let right_cache = FrameDependentCaches::default();
    let actual_distance = left_data.geodesic_distance_cached(
        &left_cache,
        &right_data,
        &right_cache,
        "PROLATE_TEST",
        semi_major,
        flattening,
        &metric,
    )?;
    assert!(
        (actual_distance - expected_distance).abs() <= 1e-9,
        "{actual_distance} != {expected_distance}"
    );

    for limit in [
        expected_distance * 0.5,
        expected_distance,
        f64::next_up(expected_distance),
    ] {
        let actual = left_data.geodesic_dwithin_cached(
            &left_cache,
            &right_data,
            &right_cache,
            "PROLATE_TEST",
            semi_major,
            flattening,
            &metric,
            limit,
        )?;
        assert_eq!(actual, expected_distance <= limit, "dwithin {limit}");
    }

    let actual_nearest = left_data
        .geodesic_nearest_points_cached(
            &left_cache,
            &right_data,
            &right_cache,
            "PROLATE_TEST",
            semi_major,
            flattening,
            &metric,
        )?
        .expect("non-empty shapes");
    assert_eq!(actual_nearest.0, expected_nearest.0);
    assert_eq!(actual_nearest.1, expected_nearest.1);
    Ok(())
}

/// The textbook FULL `O(n·m)` discrete geodesic-Fréchet DP — the oracle the
/// banded [`geodesic_frechet_dp`] must reproduce bit-for-bit.
fn geodesic_frechet_full_reference<S: Coordinates + ?Sized, L: Coordinates + ?Sized>(
    metric: &EllipsoidMetric<'_>,
    short: &S,
    long: &L,
) -> f64 {
    let width = short.coord_count();
    if width == 0 || long.coord_count() == 0 {
        return f64::INFINITY;
    }
    let distance = |p: Point, q: Point| metric.segment_length(p, q);
    let mut previous = vec![0.0_f64; width];
    let mut current = vec![0.0_f64; width];
    let mut long_points = long.iter_coords();
    let first_long = long_points.next().expect("non-empty linework");
    let mut running = 0.0_f64;
    for (cell, short_point) in std::iter::zip(&mut previous, short.iter_coords()) {
        running = running.max(distance(first_long, short_point));
        *cell = running;
    }
    for long_point in long_points {
        let mut first = true;
        let mut left_value = 0.0;
        for (short_index, short_point) in short.iter_coords().enumerate() {
            let edge = distance(long_point, short_point);
            let reach = if first {
                first = false;
                previous[0]
            } else {
                previous[short_index]
                    .min(previous[short_index - 1])
                    .min(left_value)
            };
            left_value = edge.max(reach);
            current[short_index] = left_value;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[width - 1]
}

#[test]
fn banded_geodesic_frechet_matches_full_dp_bit_for_bit() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let base = |n| {
        (0..n)
            .map(|i| {
                let i = f64::from(i);
                p(-30.0 + i, 5.0 * i.sin())
            })
            .collect::<Vec<_>>()
    };
    let perturb = |n, magnitude| {
        base(n)
            .into_iter()
            .enumerate()
            .map(|(i, point)| {
                let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
                p(point.x + sign * magnitude, point.y - sign * magnitude)
            })
            .collect::<Vec<_>>()
    };
    let check = |name, short: Vec<Point>, long: Vec<Point>| {
        let banded = geodesic_frechet_dp(&metric, short.as_slice(), long.as_slice());
        let reference = geodesic_frechet_full_reference(&metric, short.as_slice(), long.as_slice());
        assert_eq!(
            banded.to_bits(),
            reference.to_bits(),
            "{name}: banded {banded} != full {reference}"
        );
    };
    check("1x1_identical", base(1), base(1));
    check("1x5_base", base(1), base(5));
    check("5x1_base", base(5), base(1));
    check("14x14_near", base(14), perturb(14, 1e-9));
    check("7x13_moderate", base(7), perturb(13, 0.5));
    check("13x7_moderate", perturb(13, 0.5), base(7));
    check("15x14_far", base(15), perturb(14, 50.0));
    check("14x15_far", perturb(14, 50.0), base(15));
    let mut reversed = base(5);
    reversed.reverse();
    check("5x5_reversed", base(5), reversed);
}

#[test]
fn geodesic_frechet_reuses_supplied_rolling_workspace() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let short = (0..7)
        .map(|index| p(f64::from(index), f64::from(index).sin()))
        .collect::<Vec<_>>();
    let long = (0..11)
        .map(|index| p(f64::from(index), f64::from(index).cos()))
        .collect::<Vec<_>>();
    let mut scratch = FrechetDpScratch::default();

    let first = geodesic_frechet_dp_with_scratch(&metric, &short, &long, &mut scratch);
    let capacity = scratch.geodesic_capacity();
    let second = geodesic_frechet_dp_with_scratch(&metric, &short, &long, &mut scratch);

    assert!(capacity.0 >= short.len());
    assert!(capacity.1 >= short.len());
    assert_eq!(first.to_bits(), second.to_bits());
    assert_eq!(scratch.geodesic_capacity(), capacity);
}
