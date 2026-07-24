use geographiclib_rs::Geodesic;

use super::*;
use crate::error::Result;
use crate::geometry::{
    CoordSeq, Coordinates, FrameDependentCaches, GeodesicMetric, GeodesicSegment, LineSeq, Point,
    Shape, ShapeData,
};

fn p(x: f64, y: f64) -> Point {
    Point::new_unchecked_xy(x, y)
}

fn line(points: Vec<Point>) -> Shape {
    Shape::LineString(LineSeq::try_new(CoordSeq::from(points)).expect("test line is valid"))
}

fn multipoint(points: Vec<Point>) -> Shape {
    Shape::MultiPoint(points.into())
}

fn full_edges(shape: &Shape, metric: &impl GeodesicMetric) -> Vec<GeodesicSegment> {
    let mut edges = Vec::new();
    shape.for_each_vertex_pair(|start, end| edges.push(metric.make_segment(start, end)));
    edges
}

fn point_only_points(shape: &Shape) -> Vec<Point> {
    match shape {
        Shape::Point(point) => vec![*point],
        Shape::MultiPoint(points) => points.to_vec(),
        Shape::GeometryCollection(geometries) => {
            geometries.iter().flat_map(point_only_points).collect()
        },
        _ => Vec::new(),
    }
}

fn golden_oracle(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
) -> GeodesicSegmentMinimum {
    let a = endpoint_probe(geodesic, point, segment.start, segment.azimuth0);
    let b = endpoint_probe(geodesic, point, segment.end, segment.azimuth1);
    if !segment.length.is_finite() || segment.length == 0.0 {
        return nearer_endpoint(a.distance, b.distance, segment.length);
    }
    geodesic_segment_minimum_golden(
        geodesic,
        point,
        segment,
        nearer_endpoint(a.distance, b.distance, segment.length),
    )
}

#[test]
fn geodesic_newton_matches_golden_oracle() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_geodesic(&geodesic);
    let cases = [
        ("interior_foot", p(0.0, 1.0), p(-1.0, 0.0), p(1.0, 0.0)),
        ("foot_before_a", p(-1.0, 0.2), p(0.0, 0.0), p(1.0, 0.0)),
        ("foot_after_b", p(2.0, 0.2), p(0.0, 0.0), p(1.0, 0.0)),
        ("zero_length", p(2.0, 0.2), p(0.0, 0.0), p(0.0, 0.0)),
        ("pole_adjacent", p(0.0, 89.5), p(-45.0, 89.0), p(45.0, 89.0)),
        (
            "antimeridian_crossing",
            p(179.8, 0.5),
            p(179.0, -1.0),
            p(-179.0, 1.0),
        ),
        ("near_antipodal", p(179.8, 0.1), p(-1.0, 0.0), p(1.0, 0.0)),
        (
            "long_high_latitude",
            p(170.0, 78.0),
            p(-160.0, 75.0),
            p(160.0, 75.0),
        ),
    ];
    for (name, point, start, end) in cases {
        let segment = metric.make_segment(start, end);
        let actual = geodesic_segment_minimum(&geodesic, point, segment, f64::INFINITY);
        let expected = golden_oracle(&geodesic, point, segment);
        assert!(
            (actual.distance - expected.distance).abs() <= GOLDEN_SECTION_TOLERANCE_METRES,
            "{name} distance: actual {} expected {}",
            actual.distance,
            expected.distance
        );
        assert!(
            (actual.along - expected.along).abs() <= GOLDEN_SECTION_TOLERANCE_METRES,
            "{name} along: actual {} expected {}",
            actual.along,
            expected.along
        );
    }
}

#[test]
fn geodesic_newton_counter_typical_survivor_is_small() {
    geodesic_counters::reset();
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_geodesic(&geodesic);
    let segment = metric.make_segment(p(-1.0, 0.0), p(1.0, 0.0));
    let actual = geodesic_segment_minimum(&geodesic, p(0.0, 1.0), segment, f64::INFINITY);
    let counts = geodesic_counters::snapshot();
    assert!(!actual.used_golden_fallback);
    assert_eq!(counts.endpoint_inverses, 2);
    assert!(
        counts.newton_inverses <= 6,
        "newton survivor inverses: {}",
        counts.newton_inverses
    );
    assert_eq!(counts.fallback_golden_probes, 0);
}

#[test]
fn geodesic_newton_falls_back_near_conjugate() {
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_geodesic(&geodesic);
    let segment = metric.make_segment(p(180.0, -0.001), p(180.0, 0.001));
    let point = p(0.0, 0.0);
    let actual = geodesic_segment_minimum(&geodesic, point, segment, f64::INFINITY);
    let expected = golden_oracle(&geodesic, point, segment);
    assert!(
        actual.used_golden_fallback,
        "near-conjugate survivor should use golden fallback"
    );
    assert!((actual.distance - expected.distance).abs() <= GOLDEN_SECTION_TOLERANCE_METRES);
    assert!((actual.along - expected.along).abs() <= GOLDEN_SECTION_TOLERANCE_METRES);
}

#[test]
fn geodesic_line_cache_reuses_directed_segment_lines() {
    GEODESIC_LINE_CACHE.with(|cache| cache.borrow_mut().clear());
    geodesic_counters::reset();
    let geodesic = Geodesic::wgs84();
    let metric = EllipsoidMetric::for_geodesic(&geodesic);
    let segment = metric.make_segment(p(-1.0, 0.0), p(1.0, 0.0));
    let point = p(0.0, 1.0);

    let first = geodesic_segment_minimum(&geodesic, point, segment, f64::INFINITY);
    let after_first = geodesic_counters::snapshot();
    let second = geodesic_segment_minimum(&geodesic, point, segment, f64::INFINITY);
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
        let metric = EllipsoidMetric::for_geodesic(&geodesic);
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
fn non_tabulated_flattening_uses_exact_oblate_or_disabled_lane() {
    let prolate = Geodesic::new(6_378_137.0, -0.01);
    let prolate_metric = EllipsoidMetric::for_geodesic(&prolate);
    assert_eq!(
        prolate_metric
            .point_distance_lower_bound(p(0.0, 0.0), p(1.0, 0.0))
            .to_bits(),
        0.0_f64.to_bits()
    );

    let high_flattening = Geodesic::new(6_378_137.0, 0.2);
    let exact_metric = EllipsoidMetric::for_geodesic(&high_flattening);
    let a = p(-12.0, -45.0);
    let b = p(78.0, 41.0);
    assert_eq!(
        exact_metric.point_distance_lower_bound(a, b).to_bits(),
        exact_auxiliary_sphere_bound(&high_flattening, a, b).to_bits()
    );
}

fn brute_directed_geodesic_distance(
    probes: &[Point],
    target_edges: &[GeodesicSegment],
    target_points: &[Point],
    metric: &impl GeodesicMetric,
    mut best: f64,
) -> f64 {
    for &probe in probes {
        for &segment in target_edges {
            best = best.min(metric.point_to_segment(probe, segment, f64::INFINITY));
        }
        for &target in target_points {
            best = best.min(metric.segment_length(probe, target));
        }
    }
    best
}

fn brute_geodesic_distance(left: &Shape, right: &Shape, metric: &impl GeodesicMetric) -> f64 {
    let left_points = left.points_vec();
    let right_points = right.points_vec();
    if left_points.is_empty() || right_points.is_empty() {
        return f64::INFINITY;
    }
    if left.intersects(right) {
        return 0.0;
    }
    let left_edges = full_edges(left, metric);
    let right_edges = full_edges(right, metric);
    if left_edges.iter().any(|left| {
        right_edges
            .iter()
            .any(|right| metric.segments_cross(left.start, left.end, right.start, right.end))
    }) {
        return 0.0;
    }
    let left_point_only = point_only_points(left);
    let right_point_only = point_only_points(right);
    let mut best = f64::INFINITY;
    best = brute_directed_geodesic_distance(
        &left_points,
        &right_edges,
        &right_point_only,
        metric,
        best,
    );
    brute_directed_geodesic_distance(&right_points, &left_edges, &left_point_only, metric, best)
}

fn brute_geodesic_nearest_points(
    left: &Shape,
    right: &Shape,
    metric: &impl GeodesicMetric,
) -> Option<(Point, Point)> {
    let left_points = left.points_vec();
    let right_points = right.points_vec();
    if left_points.is_empty() || right_points.is_empty() {
        return None;
    }
    if left.intersects(right) {
        return left.nearest_points(right);
    }
    let left_edges = full_edges(left, metric);
    let right_edges = full_edges(right, metric);
    let left_point_only = point_only_points(left);
    let right_point_only = point_only_points(right);
    let mut best = f64::INFINITY;
    let mut pair = None;
    for &probe in &left_points {
        for &segment in &right_edges {
            let witness = metric.point_segment_witness(probe, segment, best);
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
            let witness = metric.point_segment_witness(probe, segment, best);
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
    pair
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "the prolate guard must return the exact no-prune sentinel"
)]
fn prolate_lower_bound_is_zero_and_cached_distance_paths_match_brute() -> Result<()> {
    let semi_major = 6_378_137.0;
    let semi_minor = 7_000_000.0;
    let flattening = 1.0 - semi_minor / semi_major;
    assert!(flattening < 0.0);
    let geodesic = Geodesic::new(semi_major, flattening);
    let metric = EllipsoidMetric::for_geodesic(&geodesic);

    let equator_a = p(0.0, 0.0);
    let equator_b = p(1.0, 0.0);
    assert_eq!(metric.point_distance_lower_bound(equator_a, equator_b), 0.0);
    assert!(
        (metric.segment_length(equator_a, equator_b) - semi_major * 1.0_f64.to_radians()).abs()
            < 1e-9
    );

    let left = multipoint(
        (0..96)
            .map(|index| {
                let t = f64::from(index) / 95.0;
                p(-18.0 + t * 36.0, -3.0 + (t * std::f64::consts::TAU).sin())
            })
            .chain([p(0.011, 0.0)])
            .collect(),
    );
    let right = line(
        (0..128)
            .map(|index| {
                let t = f64::from(index) / 127.0;
                p(0.0, -2.5 + t * 5.0)
            })
            .collect(),
    );

    let expected_distance = brute_geodesic_distance(&left, &right, &metric);
    let left_data = ShapeData::new(left.clone());
    let right_data = ShapeData::new(right.clone());
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

    let expected_nearest =
        brute_geodesic_nearest_points(&left, &right, &metric).expect("non-empty shapes");
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
    let metric = EllipsoidMetric::for_geodesic(&geodesic);
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
