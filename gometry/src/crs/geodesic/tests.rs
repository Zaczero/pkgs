use geographiclib_rs::Geodesic;

use super::distance::{geodesic_frechet_dp, geodesic_frechet_dp_with_scratch};
use super::lower_bound::{LowerBoundKernel, exact_auxiliary_sphere_bound};
use super::segment::geodesic_segment_minimum;
use super::{EllipsoidMetric, GEODESIC_LINE_CACHE, geodesic_counters};
use crate::error::Result;
use crate::geographic::{AngularFactor, GeographicFrame};
use crate::geometry::{
    CoordSeq, Coordinates, FrameDependentCaches, FrechetDpScratch, GeodesicMetric, GeodesicSegment,
    LineSeq, Point, PreparedPlan, Shape, XY,
};

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
    let actual = geodesic_segment_minimum(&geodesic, point, segment, f64::INFINITY)
        .expect("point-to-segment search should close");
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

    let first = geodesic_segment_minimum(&geodesic, point, segment, f64::INFINITY).unwrap();
    let after_first = geodesic_counters::snapshot();
    let second = geodesic_segment_minimum(&geodesic, point, segment, f64::INFINITY).unwrap();
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
            let witness = metric
                .point_segment_witness(query, segment, f64::INFINITY)
                .expect("point-to-segment refinement should close");
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
    let on_segment_distance = metric
        .point_to_segment(on_segment, segment, f64::INFINITY)
        .expect("point-to-segment search should close");
    assert!(
        metric
            .point_to_segment_within(on_segment, segment, 0.0)
            .expect("threshold check should close"),
        "sampled on-segment distance: {on_segment_distance}"
    );
    assert!(
        !metric
            .point_to_segment_within(p(0.0, 1.0), segment, 0.0)
            .expect("threshold check should close")
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
    assert!(
        metric
            .point_segment_witness(on_first, first, f64::INFINITY)
            .expect("point projection")
            .distance
            <= 1e-3
    );
    assert!(
        metric
            .point_segment_witness(on_second, second, f64::INFINITY)
            .expect("point projection")
            .distance
            <= 1e-3
    );
}

#[test]
fn geodesic_segment_intersection_accepts_flattening_boundaries() {
    for flattening in [-0.25, 0.2] {
        let geodesic = Geodesic::new(6_378_137.0, flattening);
        let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
        metric
            .segments_cross(p(0.0, 0.0), p(10.0, 0.0), p(5.0, -1.0), p(5.0, 1.0))
            .unwrap();
    }
    let outside = Geodesic::new(6_378_137.0, 0.200_000_000_1);
    let metric = EllipsoidMetric::for_crs84_geometry_test(&outside);
    metric
        .segments_cross(p(0.0, 0.0), p(10.0, 0.0), p(5.0, -1.0), p(5.0, 1.0))
        .unwrap_err();
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
            let witness = metric
                .point_segment_witness(probe, segment, best)
                .expect("point-to-segment proof should close");
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
            let witness = metric
                .point_segment_witness(probe, segment, best)
                .expect("point-to-segment proof should close");
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
    let semi_major = 6_378_137.0;
    let semi_minor = 7_000_000.0;
    let flattening = 1.0 - semi_minor / semi_major;
    assert!(flattening < 0.0);
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
