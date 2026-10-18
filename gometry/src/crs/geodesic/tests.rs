use geographiclib_rs::Geodesic;

use super::EllipsoidMetric;
use super::distance::{
    geodesic_frechet_dp, geodesic_frechet_dp_with_scratch, geodesic_point_distances,
};
use crate::crs::PointEdgeScratch;
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    ChartMetric as _, CoordSeq, Coordinates, FrameDependentCaches, FrechetDpScratch,
    GeodesicMetric as _, LineSeq, Point, PointEdgeQuery, PointEdgeQueryResult, PreparedPlan, Shape,
};

const MINIMUM_GAP_METERS: f64 = 4e-11 * 6_378_137.0;

fn p(x: f64, y: f64) -> Point {
    Point::new_raw_xy(x, y)
}

fn line(points: &[Point]) -> Shape {
    Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(points).unwrap()).expect("test line is valid"),
    )
}

fn multipoint(points: Vec<Point>) -> Shape {
    Shape::MultiPoint(points.into())
}

#[test]
fn geodesic_point_column_distance_rejects_invalid_latitude() {
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let frame = GeographicFrame::crs84();
    let xs = [0.0, 1.0, 2.0];
    let target = p(3.0, 0.0);

    let invalid_ys = [0.0, 95.0, 0.0];
    let error = geodesic_point_distances("EPSG:4326", &xs, &invalid_ys, target, frame, &state)
        .expect_err("invalid point-column latitude must be rejected");
    assert!(error.to_string().contains("invalid longitude/latitude"));
}

#[rstest::rstest]
#[case::geodesic_wgs84(Geodesic::wgs84())]
#[case::geodesic_new_6_378_137_0_0_0(Geodesic::new(6_378_137.0, 0.0))]
#[case::geodesic_new_6_378_137_0_0_02(Geodesic::new(6_378_137.0, -0.02))]
fn coincident_geodesic_points_have_zero_distance_across_inverse_grades(#[case] geodesic: Geodesic) {
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let frame = GeographicFrame::crs84();
    for (name, first, second) in [
        ("ordinary", p(0.0, 1.0), p(0.0, 1.0)),
        ("signed zero", p(-0.0, 0.0), p(0.0, -0.0)),
        ("seam", p(180.0, 1.0), p(-180.0, 1.0)),
        ("period", p(12.5, 1.0), p(372.5, 1.0)),
        ("north pole", p(-72.0, 90.0), p(123.0, 90.0)),
        ("south pole", p(-72.0, -90.0), p(123.0, -90.0)),
    ] {
        assert_eq!(metric.segment_length(first, second).to_bits(), 0, "{name}");
        let (distance, ..) = super::ellipsoid_metric::inverse_distance_azimuths(
            &geodesic, first.x, first.y, second.x, second.y,
        );
        assert_eq!(distance.to_bits(), 0, "{name}");
        let sample = metric
            .chart_point_distance_sample(first, second, 123_456.75, 0.37)
            .unwrap();
        assert_eq!(sample.distance.to_bits(), 0, "{name}");
        assert_eq!(sample.derivative.to_bits(), 0, "{name}");
        let route = super::route::checked_geodesic_route(
            &geodesic,
            frame.to_geodesic_point(first).unwrap(),
            frame.to_geodesic_point(second).unwrap(),
        )
        .unwrap();
        assert_eq!(route.length.to_bits(), 0, "{name}");
    }
    for second in [p(12.500_001, 1.000_001), p(12.5, 1.000_001), p(12.5, -1.0)] {
        let first = p(12.5, 1.0);
        let length = metric.segment_length(first, second);
        assert!(
            length > 0.0,
            "f={}: {first:?} to {second:?}: distance={length:?}",
            geodesic.f,
        );
        let (distance, ..) = super::ellipsoid_metric::inverse_distance_azimuths(
            &geodesic, first.x, first.y, second.x, second.y,
        );
        assert!(
            distance > 0.0,
            "f={}: {first:?} to {second:?}: joint distance={distance:?}",
            geodesic.f,
        );
    }
}

#[rstest::rstest]
#[case::wgs84("wgs84", Geodesic::wgs84())]
#[case::sphere("sphere", Geodesic::new(6_378_137.0, 0.0))]
#[case::prolate("prolate", Geodesic::new(6_378_137.0, -0.02))]
fn chart_point_sample_matches_backend_terminal_gradient(
    #[case] model: &'static str,
    #[case] geodesic: Geodesic,
) {
    let cases = [
        ("north_east", p(1.0, 5.0)),
        ("north_west", p(-1.0, 5.0)),
        ("south_east", p(1.0, -5.0)),
        ("south_west", p(-1.0, -5.0)),
        ("east_north", p(5.0, 1.0)),
        ("east_south", p(5.0, -1.0)),
        ("west_north", p(-5.0, 1.0)),
        ("west_south", p(-5.0, -1.0)),
        ("due_north", p(0.0, 5.0)),
        ("due_south", p(0.0, -5.0)),
        ("due_east", p(5.0, 0.0)),
        ("due_west", p(-5.0, 0.0)),
        ("north_pole", p(10.0, 90.0)),
        ("south_pole", p(-10.0, -90.0)),
        ("near_antipodal", p(179.999, 0.001)),
        ("coincident", p(0.0, 0.0)),
    ];

    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let target = p(0.0, 0.0);
    for (name, source) in cases {
        let (distance, _, terminal_azimuth) = super::ellipsoid_metric::inverse_distance_azimuths(
            &geodesic, target.x, target.y, source.x, source.y,
        );
        for speed in [0.0, 123_456.75] {
            let sample = metric
                .chart_point_distance_sample(source, target, speed, 0.37)
                .expect("admitted point sample");
            let derivative = if distance == 0.0 || speed == 0.0 {
                0.0
            } else {
                speed * (0.37 - terminal_azimuth.to_radians()).cos()
            };
            assert!(
                (sample.distance - distance).abs()
                    <= metric.geodesic_numerical_policy().evaluation_allowance(2.0),
                "{model}/{name}: distance {} != {distance}",
                sample.distance,
            );
            assert!(
                (sample.derivative - derivative).abs() <= 64.0 * f64::EPSILON * speed,
                "{model}/{name}: speed {speed} gradient {} != {derivative}",
                sample.derivative,
            );
        }
    }
}

#[test]
fn chart_lrs_index_rejects_half_period_edges() {
    let geodesic = Geodesic::new(6_378_137.0, 0.0);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let shape = PreparedPlan::new(line(&[p(0.0, 10.0), p(180.0, -10.0)]));
    let cache = FrameDependentCaches::default();
    let admission =
        crate::geometry::GeographicPrepared::try_new(&shape, &cache, metric.chart_frame()).unwrap();
    assert!(
        shape
            .chart_line_index(
                &cache, "TEST", geodesic.a, geodesic.f, 0, &metric, admission,
            )
            .is_err()
    );
}

#[test]
fn lower_bound_stays_below_backend_distance() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
    let check = |a: Point, b: Point| {
        let bound = metric
            .point_distance_lower_bound(a, b)
            .expect("lower-bound query");
        let distance = metric.segment_length(a, b);
        assert!(bound <= distance, "{a:?} {b:?}: {bound} > {distance}");
    };
    // Short meridional arcs across the equator are where the auxiliary-sphere
    // bound is tight to first order.
    for lon in [-180.0, 0.0, 179.5] {
        for half in [1.0e-12, 1.0e-6, 1.0, 10.0] {
            check(p(lon, -half), p(lon, half));
            check(p(lon, 0.0), p(lon, half));
            check(p(lon, -0.3 * half), p(lon, half));
        }
    }
    for (a, b) in [
        (p(-180.0, 0.0), p(180.0, 0.0)),
        (p(-179.5, 10.0), p(179.5, -10.0)),
        (p(15.0, 90.0), p(-120.0, 80.0)),
        (p(-15.0, -90.0), p(120.0, -80.0)),
        (p(0.0, 0.0), p(179.999, 0.001)),
        (p(-30.0, 45.0), p(125.0, -60.0)),
        (p(20.0, 10.0), p(20.0, 10.0)),
    ] {
        check(a, b);
    }
}

#[rstest::rstest]
#[case::equatorial("equatorial", p(0.0, 0.0), p(1.0, 0.0))]
#[case::southern_midlatitude("southern_midlatitude", p(-12.0, -45.0), p(78.0, -41.0))]
#[case::northern_midlatitude("northern_midlatitude", p(-12.0, 45.0), p(78.0, 41.0))]
#[case::polar("polar", p(0.0, 90.0), p(180.0, 89.0))]
fn disabled_lower_bound_is_zero_across_latitudes(
    #[case] name: &'static str,
    #[case] a: Point,
    #[case] b: Point,
) {
    let geodesic = Geodesic::new(6_378_137.0, -0.01);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);

    assert_eq!(
        metric
            .point_distance_lower_bound(a, b)
            .expect("disabled lower-bound query")
            .to_bits(),
        0.0_f64.to_bits(),
        "{name}: disabled lower bound"
    );
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test returns Result for fallible setup and uses normal assertion macros for its oracle"
)]
fn prolate_lower_bound_is_zero_and_cached_chart_paths_match_edge_queries() -> Result<()> {
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
        ("west", p(-18.0, -3.0)),
        ("east", p(18.0, -3.0)),
        ("nearest", p(0.011, 0.0)),
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

    let left_points = left.points_vec()?;
    let right_points = right.points_vec()?;
    let mut expected_distance = f64::INFINITY;
    let mut expected_nearest = None;
    let mut point_edge_scratch = PointEdgeScratch::default();
    for probe in left_points {
        for endpoints in right_points.windows(2) {
            let [start, end] = endpoints else {
                unreachable!("a two-point window has exactly two entries")
            };
            let edge = metric.chart_edge(*start, *end)?;
            let bounds = edge.bounds();
            let result = metric.point_edge_query(
                probe,
                &edge,
                metric.point_envelope_bound(probe)(bounds),
                PointEdgeQuery::Minimum {
                    cutoff: f64::INFINITY,
                },
                &mut point_edge_scratch,
            )?;
            let PointEdgeQueryResult::Minimum(minimum) = result else {
                unreachable!("an infinite cutoff admits every chart-edge minimum")
            };
            if minimum.distance < expected_distance {
                expected_distance = minimum.distance;
                expected_nearest = Some((
                    probe,
                    metric.chart_edge_witness(*start, *end, &edge, minimum.parameter, None)?,
                ));
            }
        }
    }
    let expected_nearest = expected_nearest.expect("non-empty points and chart edges");
    let left_data = PreparedPlan::new(left);
    let right_data = PreparedPlan::new(right);
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
        (actual_distance - expected_distance).abs()
            <= metric.geodesic_numerical_policy().evaluation_allowance(2.0),
        "{actual_distance} != {expected_distance}"
    );

    for limit in [
        expected_distance * 0.5,
        expected_distance + MINIMUM_GAP_METERS,
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
    let attained = metric.segment_length(actual_nearest.0, actual_nearest.1);
    assert!(
        attained <= expected_distance + MINIMUM_GAP_METERS,
        "cached witness distance {attained} exceeds the direct edge-query minimum \
         {expected_distance} by more than the Q8 gap {MINIMUM_GAP_METERS}"
    );
    Ok(())
}

/// The textbook FULL `O(n·m)` discrete geodesic-Fréchet DP — the oracle the
/// banded [`geodesic_frechet_dp`] must reproduce within backend accuracy.
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
                    .min(previous[short_index.wrapping_sub(1)])
                    .min(left_value)
            };
            left_value = edge.max(reach);
            current[short_index] = left_value;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[width.wrapping_sub(1)]
}

#[test]
fn banded_geodesic_frechet_matches_full_dp() {
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
        assert!(
            (banded - reference).abs()
                <= metric.geodesic_numerical_policy().evaluation_allowance(2.0),
            "{name}: banded {banded} != full {reference}",
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
    approx::assert_abs_diff_eq!(
        first,
        second,
        epsilon = metric.geodesic_numerical_policy().evaluation_allowance(2.0),
    );
    assert_eq!(scratch.geodesic_capacity(), capacity);
}
