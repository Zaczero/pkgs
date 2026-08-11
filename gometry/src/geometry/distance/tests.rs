use crate::geometry::distance::hausdorff::HAUSDORFF_SMALL_TARGET_MAX_VERTICES;
use crate::geometry::distance::*;
use crate::geometry::*;

pub(super) fn p(x: f64, y: f64) -> Point {
    Point::new_unchecked_xy(x, y)
}

#[test]
fn continuous_hausdorff_keeps_tiny_parallel_separation() {
    let separation = 1e-300;
    let left = line(vec![p(0.0, 0.0), p(2.0 * separation, 0.0)]);
    let right = line(vec![p(0.0, separation), p(2.0 * separation, separation)]);
    assert_eq!(
        left.hausdorff_distance(&right).to_bits(),
        separation.to_bits()
    );

    let base = 1e200_f64;
    let huge_separation = f64::from_bits(base.to_bits() + 16) - base;
    let left = line(vec![p(0.0, base), p(1.0, base)]);
    let right = line(vec![
        p(0.0, base + huge_separation),
        p(1.0, base + huge_separation),
    ]);
    assert_eq!(
        left.hausdorff_distance(&right).to_bits(),
        huge_separation.to_bits()
    );

    let left = line(vec![
        p(0.0, base),
        p(0.25, base),
        p(0.75, base),
        p(1.0, base),
    ]);
    let right = line(vec![
        p(0.0, base + huge_separation),
        p(0.25, base + huge_separation),
        p(0.75, base + huge_separation),
        p(1.0, base + huge_separation),
    ]);
    assert_eq!(
        left.hausdorff_distance(&right).to_bits(),
        huge_separation.to_bits()
    );
}

#[test]
fn point_segment_distance_divides_before_unscaling_tiny_area() {
    let end = 1e-200_f64;
    let midpoint = end / 2.0;
    let offset_y = f64::from_bits(midpoint.to_bits() + 1);
    let actual = shape_impl::robust_point_segment_distance(XY::new(midpoint, offset_y), Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(end, end),
    });
    // `Fraction` from these STORED doubles gives
    // d² = (offset_y - midpoint)² / 2 exactly. Its correctly-rounded square
    // root is 0x1.6a09e667f3bcdp-719; dividing by rounded sqrt(2) is one ulp low.
    let expected = f64::from_bits(1_370_959_738_765_786_061);
    assert!(actual > 0.0);
    assert_eq!(actual.to_bits(), expected.to_bits());
}

#[test]
fn continuous_hausdorff_preserves_underflowed_local_determinant() {
    let length = 2.0_f64.powi(996);
    let height = 2.0_f64.powi(396);
    let offset = 2.0_f64.powi(344);
    let baseline = line(vec![p(0.0, 0.0), p(length, height)]);
    let graph = line(vec![
        p(0.0, 0.0),
        p(height, 0.0),
        p(length, height + offset),
    ]);
    assert_eq!(
        baseline.hausdorff_distance(&graph).to_bits(),
        offset.to_bits()
    );
    assert_eq!(
        graph.hausdorff_distance(&baseline).to_bits(),
        offset.to_bits()
    );
}

#[test]
fn continuous_hausdorff_keeps_reciprocal_axis_bend_at_square_overflow_transition() {
    for exponent in [154, 155] {
        let length = 10.0_f64.powi(exponent);
        let height = 10.0_f64.powi(-exponent);
        let baseline = line(vec![p(0.0, 0.0), p(length, 0.0)]);
        let bent = line(vec![
            p(0.0, height),
            p(length / 2.0, 2.0 * height),
            p(length, height),
        ]);
        let expected = 2.0 * height;
        assert_eq!(
            baseline.hausdorff_distance(&bent).to_bits(),
            expected.to_bits(),
            "reciprocal-axis Hausdorff failed at e={exponent}"
        );
        assert_eq!(
            bent.hausdorff_distance(&baseline).to_bits(),
            expected.to_bits()
        );
    }
}

#[test]
fn continuous_hausdorff_finds_reciprocal_axis_gap_between_target_parts() {
    for exponent in [154, 200, 300] {
        let length = 10.0_f64.powi(exponent);
        let half_gap = 10.0_f64.powi(-exponent);
        let source = line(vec![p(-length, 0.0), p(length, 0.0)]);
        let target = multiline(vec![vec![p(-length, 0.0), p(-half_gap, 0.0)], vec![
            p(half_gap, 0.0),
            p(length, 0.0),
        ]]);
        assert_eq!(
            shape_impl::robust_point_segment_distance(XY::new(0.0, 0.0), Segment {
                start: XY::new(-length, 0.0),
                end: XY::new(-half_gap, 0.0),
            },)
            .to_bits(),
            half_gap.to_bits(),
        );
        assert_eq!(
            shape_impl::robust_directed_hausdorff(&source, &target).to_bits(),
            half_gap.to_bits(),
        );
        assert_eq!(
            source.hausdorff_distance(&target).to_bits(),
            half_gap.to_bits(),
            "reciprocal-axis target gap failed at e={exponent}"
        );
    }
}

#[test]
fn continuous_hausdorff_preserves_minor_axis_breakpoints_in_collections() {
    for exponent in [154, 200, 300] {
        let length = 10.0_f64.powi(exponent);
        let height = 10.0_f64.powi(-exponent);
        for swap in [false, true] {
            let xy = |x: f64, y: f64| if swap { p(y, x) } else { p(x, y) };
            let horizontal = line(vec![xy(-length, 10.0 * height), xy(length, 10.0 * height)]);
            let vertical = line(vec![xy(0.0, -height), xy(0.0, height)]);
            let lower = line(vec![xy(0.0, -height), xy(0.0, -height / 2.0)]);
            let upper = line(vec![xy(0.0, height / 2.0), xy(0.0, height)]);
            let source = Shape::GeometryCollection(vec![horizontal.clone(), vertical]);
            let target = Shape::GeometryCollection(vec![horizontal, lower, upper]);
            let expected = height / 2.0;
            assert!(
                source
                    .hausdorff_distance(&target)
                    .to_bits()
                    .abs_diff(expected.to_bits())
                    <= 4,
                "minor-axis collection gap failed at e={exponent}, swap={swap}"
            );
            assert!(
                target
                    .hausdorff_distance(&source)
                    .to_bits()
                    .abs_diff(expected.to_bits())
                    <= 4
            );
        }
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "test builder takes temporary shapes and returns their transformed value"
)]
fn with_z(shape: Shape, z: f64) -> Shape {
    shape.set_z(Some(z), true).expect("set z")
}

fn pzm(x: f64, y: f64, z: f64, m: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).expect("finite test point")
}

pub(super) fn line(points: Vec<Point>) -> Shape {
    Shape::LineString(LineSeq::try_new(CoordSeq::from(points)).expect("test line is valid"))
}

fn multiline(lines: Vec<Vec<Point>>) -> Shape {
    Shape::MultiLineString(
        lines
            .into_iter()
            .map(CoordSeq::from)
            .map(|line| LineSeq::try_new(line).expect("test line is valid"))
            .collect(),
    )
}

pub(super) fn multipoint(points: Vec<Point>) -> Shape {
    Shape::MultiPoint(points.into())
}

pub(super) fn polygon(points: Vec<Point>) -> Shape {
    Shape::Polygon(Polygon::new(Ring::from_trusted_closed(points), Vec::new()))
}

pub(super) fn dense_line(start_lon: f64, start_lat: f64, count: usize) -> Shape {
    line(
        (0..count)
            .map(|index| {
                let t = index as f64 / (count - 1) as f64;
                p(
                    start_lon + t * 4.0,
                    start_lat + (t * std::f64::consts::TAU).sin() * 0.25,
                )
            })
            .collect(),
    )
}

pub(super) fn dense_polygon(cx: f64, cy: f64, radius: f64, vertices: usize) -> Shape {
    let mut points = (0..vertices)
        .map(|index| {
            let angle = (index as f64 / vertices as f64) * std::f64::consts::TAU;
            p(cx + radius * angle.cos(), cy + radius * angle.sin())
        })
        .collect::<Vec<_>>();
    points.push(points[0]);
    polygon(points)
}

pub(super) fn dense_antimeridian_polygon() -> Shape {
    let mut points = Vec::new();
    for index in 0..35 {
        let t = f64::from(index) / 34.0;
        points.push(p(179.0, -4.0 + t * 8.0));
    }
    for index in 0..35 {
        let t = f64::from(index) / 34.0;
        points.push(p(-179.0, 4.0 - t * 8.0));
    }
    points.push(points[0]);
    polygon(points)
}

pub(super) fn dense_near_pole_polygon() -> Shape {
    let mut points = (0..72)
        .map(|index| {
            let angle = (f64::from(index) / 72.0) * std::f64::consts::TAU;
            p(angle.to_degrees() - 180.0, 87.0 + 1.5 * angle.sin())
        })
        .collect::<Vec<_>>();
    points.push(points[0]);
    polygon(points)
}

pub(super) fn dense_degenerate_line() -> Shape {
    line(std::iter::repeat_with(|| p(12.0, 3.0)).take(80).collect())
}

pub(super) fn dense_tie_lines() -> Shape {
    let mut lines = vec![vec![p(1.0, -2.0), p(1.0, 2.0)], vec![
        p(-1.0, -2.0),
        p(-1.0, 2.0),
    ]];
    for group in 0..32 {
        let lon = 20.0 + f64::from(group);
        lines.push(vec![p(lon, -2.0), p(lon, 2.0)]);
        lines.push(vec![p(-lon, -2.0), p(-lon, 2.0)]);
    }
    multiline(lines)
}

fn full_edges(shape: &Shape, metric: &impl GeodesicMetric) -> Vec<GeodesicSegment> {
    let mut edges = Vec::new();
    shape.for_each_vertex_pair(|start, end| edges.push(metric.make_segment(start, end)));
    edges
}

pub(super) fn brute_geodesic_distance(
    left: &Shape,
    right: &Shape,
    metric: &impl GeodesicMetric,
) -> f64 {
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
    let left_point_only = left.point_only_points();
    let right_point_only = right.point_only_points();
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

pub(super) fn assert_geodesic_close(name: &str, actual: f64, expected: f64) {
    if actual.is_infinite() || expected.is_infinite() {
        assert!(
            actual.is_infinite()
                && expected.is_infinite()
                && actual.is_sign_positive() == expected.is_sign_positive(),
            "{name}: actual {actual} expected {expected}"
        );
    } else {
        assert!(
            (actual - expected).abs() <= 1e-3,
            "{name}: actual {actual} expected {expected}"
        );
    }
}

pub(super) fn brute_geodesic_nearest_points(
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
    let left_point_only = left.point_only_points();
    let right_point_only = right.point_only_points();
    let mut best = f64::INFINITY;
    let mut pair: Option<(Point, Point)> = None;
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

pub(super) fn geodesic_nearest_oracle_cases() -> Vec<(&'static str, Shape, Shape)> {
    vec![
        (
            "point_line",
            Shape::Point(p(0.0, 0.0)),
            line(vec![
                pzm(1.0, -1.0, 10.0, 100.0),
                pzm(1.0, 1.0, 20.0, 200.0),
            ]),
        ),
        (
            "line_line",
            line(vec![p(-2.0, -1.0), p(2.0, -1.0)]),
            line(vec![pzm(-1.0, 1.0, 3.0, 30.0), pzm(1.0, 1.0, 4.0, 40.0)]),
        ),
        (
            "polygon_boundary",
            Shape::Point(p(4.0, 1.0)),
            polygon(vec![p(0.0, 0.0), p(2.0, 0.0), p(2.0, 2.0), p(0.0, 0.0)]),
        ),
        (
            "multipoint",
            multipoint(vec![p(-4.0, 0.0), p(4.0, 0.0), p(0.0, 3.0)]),
            line(vec![p(0.5, -1.0), p(0.5, 1.0)]),
        ),
        (
            "geometry_collection",
            Shape::GeometryCollection(vec![
                Shape::Point(p(-6.0, 1.0)),
                line(vec![p(-5.0, -1.0), p(-5.0, 1.0)]),
            ]),
            Shape::GeometryCollection(vec![
                multipoint(vec![p(6.0, 2.0), p(7.0, 3.0)]),
                line(vec![p(3.0, -1.0), p(3.0, 1.0)]),
            ]),
        ),
        (
            "degenerate_segment",
            line(vec![pzm(10.0, 0.0, 1.0, 2.0), pzm(10.0, 0.0, 1.0, 2.0)]),
            Shape::Point(p(11.0, 0.0)),
        ),
        (
            "antimeridian_spanning_segment",
            line(vec![p(179.5, 10.0), p(-179.5, 10.0)]),
            Shape::Point(p(179.8, 12.0)),
        ),
        (
            "near_pole",
            line(vec![p(-45.0, 88.5), p(45.0, 88.5)]),
            Shape::Point(p(0.0, 87.0)),
        ),
        (
            "exact_tie",
            Shape::Point(p(0.0, 0.0)),
            multipoint(vec![p(1.0, 0.0), p(-1.0, 0.0)]),
        ),
        (
            "interior_foot_exact_tie_bound_order_later_first",
            line(vec![p(0.0, 1.0), p(0.0, -1.0)]),
            line(vec![p(1.0, -2.0), p(1.0, 2.0)]),
        ),
        (
            "line_point_reverse_sweep_interior_winner",
            line(vec![p(0.0, -2.0), p(0.0, 2.0)]),
            Shape::Point(p(1.0, 0.0)),
        ),
        (
            "antimeridian_segment_pair_crossing",
            line(vec![p(179.0, 0.0), p(-179.0, 0.0)]),
            line(vec![p(180.0, -1.0), p(180.0, 1.0)]),
        ),
    ]
}

pub(super) fn geodesic_distance_oracle_cases() -> Vec<(&'static str, Shape, Shape)> {
    let mut cases = geodesic_nearest_oracle_cases();
    cases.extend([
        (
            "point_only_multipoint",
            multipoint(vec![p(-10.0, 2.0), p(-9.5, 2.5), p(-9.0, 3.0)]),
            multipoint(vec![p(12.0, -4.0), p(12.5, -3.5), p(13.0, -3.0)]),
        ),
        geodesic_global_cap_pruning_case(),
        geodesic_group_cap_pruning_case(),
    ]);
    cases
}

fn geodesic_global_cap_pruning_case() -> (&'static str, Shape, Shape) {
    let mut probes = Vec::new();
    for index in 0..64 {
        let offset = f64::from(index) * 0.0001;
        probes.push(p(-0.02 + offset, -0.01 + offset * 0.5));
    }
    for index in 0..96 {
        probes.push(p(
            70.0 + f64::from(index) * 0.02,
            35.0 + f64::from(index % 7) * 0.01,
        ));
    }
    (
        "global_cap_prunes_far_vertices",
        multipoint(probes),
        line(vec![p(0.01, -0.15), p(0.01, 0.15)]),
    )
}

fn geodesic_group_cap_pruning_case() -> (&'static str, Shape, Shape) {
    let near = (0..20)
        .map(|index| p(0.03, -0.04 + f64::from(index) * 0.004))
        .collect::<Vec<_>>();
    let mut lines = vec![near];
    for group in 0..8 {
        let lon = 20.0 + f64::from(group) * 12.0;
        let lat = -45.0 + f64::from(group) * 8.0;
        lines.push(
            (0..20)
                .map(|index| {
                    p(
                        lon + f64::from(index) * 0.01,
                        lat + f64::from(index) * 0.002,
                    )
                })
                .collect(),
        );
    }
    (
        "group_caps_prune_far_segment_groups",
        multipoint(vec![p(0.0, 0.0), p(0.002, -0.002), p(-0.002, 0.002)]),
        multiline(lines),
    )
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(super) fn assert_same_witness(
    name: &str,
    actual: Option<GeodesicWitnessCandidate>,
    expected: Option<GeodesicWitnessCandidate>,
) {
    match (actual, expected) {
        (Some(actual), Some(expected)) => {
            assert_eq!(actual.probe, expected.probe, "{name} witness probe");
            assert_eq!(actual.target, expected.target, "{name} witness target");
            assert_eq!(
                actual.distance, expected.distance,
                "{name} witness distance"
            );
            assert_eq!(actual.order, expected.order, "{name} witness order");
            assert_eq!(actual.swapped, expected.swapped, "{name} witness swapped");
        },
        (None, None) => {},
        (actual, expected) => panic!("{name} witness mismatch: {actual:?} != {expected:?}"),
    }
}

pub(super) fn geodesic_bvh_oracle_cases() -> Vec<(&'static str, Shape, Shape, bool)> {
    vec![
        (
            "point_inside_polygon",
            Shape::Point(p(0.0, 0.0)),
            dense_polygon(0.0, 0.0, 2.0, 96),
            true,
        ),
        (
            "point_outside_polygon",
            Shape::Point(p(5.0, 0.0)),
            dense_polygon(0.0, 0.0, 2.0, 96),
            true,
        ),
        (
            "point_on_boundary_polygon",
            Shape::Point(p(2.0, 0.0)),
            dense_polygon(0.0, 0.0, 2.0, 96),
            true,
        ),
        (
            "complex_line",
            multipoint(vec![p(-2.0, 1.0), p(2.0, -1.0), p(0.5, 1.5)]),
            dense_line(-2.0, 0.0, 96),
            true,
        ),
        (
            "multipoint_fallback",
            Shape::Point(p(0.0, 0.0)),
            multipoint(vec![p(1.0, 0.0), p(-1.0, 0.0), p(0.0, 1.0)]),
            false,
        ),
        (
            "antimeridian_polygon",
            Shape::Point(p(178.0, 0.0)),
            dense_antimeridian_polygon(),
            true,
        ),
        (
            "near_pole_polygon",
            Shape::Point(p(15.0, 85.0)),
            dense_near_pole_polygon(),
            true,
        ),
        (
            "degenerate",
            Shape::Point(p(12.5, 3.0)),
            dense_degenerate_line(),
            true,
        ),
        (
            "nearest_tie",
            Shape::Point(p(0.0, 0.0)),
            dense_tie_lines(),
            true,
        ),
    ]
}

fn brute_directed_hausdorff_squared(source: &Shape, target: &Shape) -> f64 {
    let target = HausdorffTarget::build(target);
    let mut best = 0.0_f64;
    source.for_each_point(|point| {
        best = best.max(target.distance_squared(point.xy()));
    });
    source.for_each_segment(|segment| {
        best = best.max(brute_max_on_segment_squared(segment, &target));
    });
    best
}

fn brute_max_on_segment_squared(source: Segment, target: &HausdorffTarget) -> f64 {
    let ax = source.start.x;
    let ay = source.start.y;
    let dx = source.end.x - ax;
    let dy = source.end.y - ay;
    if dx == 0.0 && dy == 0.0 {
        return target.distance_squared(source.start);
    }
    let steps = 256_usize;
    let mut best = 0.0_f64;
    for step in 0..=steps {
        let t = step as f64 / steps as f64;
        best = best.max(sample_hausdorff_on_segment(ax, ay, dx, dy, t, target));
    }
    best
}

fn assert_hausdorff_close(actual: f64, expected: f64) {
    if actual.is_infinite() || expected.is_infinite() {
        assert_eq!(actual.is_sign_positive(), expected.is_sign_positive());
        return;
    }
    assert!(
        (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0),
        "actual {actual} expected {expected}"
    );
}

#[test]
fn segment_aware_hausdorff_matches_brute_oracle_on_fixtures() {
    let parallel = (
        line(vec![p(0.0, 0.0), p(1.0, 1.0), p(2.0, 1.0)]),
        line(vec![p(0.0, 1.0), p(1.0, 2.0), p(2.0, 2.0)]),
    );
    let interior_peak = (
        line(vec![p(0.0, 0.0), p(10.0, 0.0)]),
        line(vec![p(0.0, 1.0), p(5.0, 8.0), p(10.0, 1.0)]),
    );
    let point_to_line = (
        Shape::Point(p(0.0, 0.0)),
        line(vec![p(0.0, 1.0), p(3.0, 1.0)]),
    );
    let polygon_boundary = (
        line(vec![p(-1.0, 0.5), p(4.0, 0.5)]),
        polygon(vec![p(0.0, 0.0), p(2.0, 0.0), p(2.0, 2.0), p(0.0, 0.0)]),
    );
    let multipoint = (
        multipoint(vec![p(0.0, 0.0), p(5.0, 5.0)]),
        line(vec![p(0.0, 1.0), p(10.0, 1.0)]),
    );
    for (left, right) in [
        parallel,
        interior_peak,
        point_to_line,
        polygon_boundary,
        multipoint,
    ] {
        let expected = brute_directed_hausdorff_squared(&left, &right)
            .max(brute_directed_hausdorff_squared(&right, &left))
            .sqrt();
        let actual = left.hausdorff_distance(&right);
        assert_hausdorff_close(actual, expected);
    }
}

#[test]
fn segment_aware_hausdorff_empty_and_point_semantics() {
    assert!(
        line(vec![p(0.0, 0.0), p(1.0, 0.0)])
            .hausdorff_distance(&Shape::empty_point())
            .is_infinite()
    );
    assert!(
        Shape::empty_point()
            .hausdorff_distance(&Shape::Point(p(0.0, 0.0)))
            .is_infinite()
    );
    assert_hausdorff_close(
        Shape::Point(p(0.0, 0.0)).hausdorff_distance(&Shape::Point(p(3.0, 4.0))),
        5.0,
    );
    let subnormal = 1.5e-162;
    assert_eq!(
        Shape::Point(p(0.0, 0.0))
            .hausdorff_distance(&Shape::Point(p(subnormal, 0.0)))
            .to_bits(),
        subnormal.to_bits(),
        "a point pair is a complete continuous Hausdorff problem; its squared distance may underflow"
    );
    assert_hausdorff_close(
        line(vec![p(0.0, 0.0), p(10.0, 0.0)]).hausdorff_distance(&line(vec![
            p(0.0, 1.0),
            p(5.0, 8.0),
            p(10.0, 1.0),
        ])),
        8.0,
    );
}

#[test]
fn backtracking_collinear_line_is_not_a_single_endpoint_segment() {
    let left = line(vec![p(0.0, 0.0), p(10.0, 0.0), p(-5.0, 0.0)]);
    let right = line(vec![p(-5.0, 1.0), p(0.0, 1.0)]);
    let expected = 101.0_f64.sqrt();
    assert_hausdorff_close(left.hausdorff_distance(&right), expected);
    assert_hausdorff_close(right.hausdorff_distance(&left), expected);
}

fn wiggly_line_columns(vertex_count: usize, y_offset: f64) -> (Vec<f64>, Vec<f64>) {
    let mut xs = Vec::with_capacity(vertex_count);
    let mut ys = Vec::with_capacity(vertex_count);
    for index in 0..vertex_count {
        let x = index as f64 * 0.01;
        xs.push(x);
        ys.push((x * 1.7).sin() + y_offset);
    }
    (xs, ys)
}

fn directed_hausdorff_distance_squared_columns_forced(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
    force_index: bool,
) -> f64 {
    let mut segments = Vec::with_capacity(right_xs.len().saturating_sub(1));
    for index in 1..right_xs.len() {
        segments.push(Segment {
            start: XY::new(right_xs[index - 1], right_ys[index - 1]),
            end: XY::new(right_xs[index], right_ys[index]),
        });
    }
    let target = HausdorffTarget::from_parts(&segments, Vec::new(), Some(force_index));
    directed_hausdorff_distance_squared_with_target_columns(left_xs, left_ys, &target)
}

#[test]
fn small_hausdorff_column_path_matches_forced_indexed() {
    for vertex_count in 2..=17 {
        let (left_xs, left_ys) = wiggly_line_columns(vertex_count, 0.0);
        let (right_xs, right_ys) = wiggly_line_columns(vertex_count, 0.3);
        let small = directed_hausdorff_distance_squared_columns_forced(
            &left_xs, &left_ys, &right_xs, &right_ys, false,
        );
        let indexed = directed_hausdorff_distance_squared_columns_forced(
            &left_xs, &left_ys, &right_xs, &right_ys, true,
        );
        assert_eq!(
            small.to_bits(),
            indexed.to_bits(),
            "vertex_count={vertex_count} small={small} indexed={indexed}"
        );
        let symmetric_small = small.max(directed_hausdorff_distance_squared_columns_forced(
            &right_xs, &right_ys, &left_xs, &left_ys, false,
        ));
        let symmetric_indexed = indexed.max(directed_hausdorff_distance_squared_columns_forced(
            &right_xs, &right_ys, &left_xs, &left_ys, true,
        ));
        assert_eq!(symmetric_small.to_bits(), symmetric_indexed.to_bits());
        let column = hausdorff_distance_line_columns(&left_xs, &left_ys, &right_xs, &right_ys);
        assert_eq!(column.to_bits(), symmetric_small.sqrt().to_bits());
        let right_segments = right_xs.len().saturating_sub(1);
        if vertex_count <= HAUSDORFF_SMALL_TARGET_MAX_VERTICES {
            assert!(!should_build_index(right_segments, 0));
        } else {
            assert!(should_build_index(right_segments, 0));
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "captured four-vertex oracle fixtures stay explicit for auditability"
)]
#[expect(
    clippy::unreadable_literal,
    reason = "captured oracle coordinates preserve their recorded decimal spelling"
)]
fn hausdorff_4v_exact_matches_bisection_oracle() {
    let (left_xs, left_ys) = wiggly_line_columns(4, 0.0);
    let (right_xs, right_ys) = wiggly_line_columns(4, 0.3);
    let exact = super::hausdorff_distance_4v_exact(&left_xs, &left_ys, &right_xs, &right_ys);
    let bisect = super::hausdorff_distance_4v_fused(&left_xs, &left_ys, &right_xs, &right_ys);
    assert_eq!(exact.to_bits(), bisect.to_bits());
    assert_hausdorff_close(exact, bisect);

    let check = |_name: &str,
                 left_xs: [f64; 4],
                 left_ys: [f64; 4],
                 right_xs: [f64; 4],
                 right_ys: [f64; 4]| {
        let exact = super::hausdorff_distance_4v_exact(&left_xs, &left_ys, &right_xs, &right_ys);
        let bisect = super::hausdorff_distance_4v_fused(&left_xs, &left_ys, &right_xs, &right_ys);
        assert_hausdorff_close(exact, bisect);
    };
    check(
        "identical",
        [-3.0, -2.0, -1.0, 0.0],
        [0.0; 4],
        [-3.0, -2.0, -1.0, 0.0],
        [0.0; 4],
    );
    check(
        "parallel",
        [-3.0, -2.0, -1.0, 0.0],
        [0.0; 4],
        [-3.0, -2.0, -1.0, 0.0],
        [1.0; 4],
    );
    check(
        "crossing_zigzags",
        [-4.0, -3.0, -2.0, -1.0],
        [0.0, 3.0, -3.0, 0.0],
        [-4.0, -3.0, -2.0, -1.0],
        [2.0, -2.0, 2.0, -2.0],
    );
    check(
        "interior_peak",
        [-4.0, -3.0, -2.0, -1.0],
        [0.0; 4],
        [-4.0, -3.0, -2.0, -1.0],
        [1.0, 4.0, 1.0, 1.0],
    );
    check(
        "target_switch",
        [-4.0, -1.0, 2.0, 5.0],
        [0.0, 3.0, -2.0, 1.0],
        [-3.0, 0.0, 3.0, 6.0],
        [2.0, -1.0, 4.0, 0.0],
    );
    check(
        "captured_0",
        [
            -757.7108432906007,
            -791.8065622895506,
            -619.838942685127,
            -430.1538168057226,
        ],
        [
            -479.3557691106936,
            -720.6152676885517,
            -675.6199672062928,
            -339.38461387049983,
        ],
        [
            -955.016858865278,
            -874.5267791381401,
            -30.843372231080025,
            -167.22624822687965,
        ],
        [
            -357.53845455067653,
            -477.5084293162238,
            -968.6316950173657,
            -969.7138555277404,
        ],
    );
    check(
        "captured_1",
        [
            -155.65479713390926,
            -781.0247209344582,
            -538.6284840150338,
            -111.1347808295709,
        ],
        [
            -154.51393536164278,
            -444.53912261979167,
            -985.5821402709889,
            -944.4459238891596,
        ],
        [
            -604.0307575846163,
            -54.99151327064999,
            -622.619189234129,
            -124.09888373783292,
        ],
        [
            -777.7836955566387,
            -802.0153790251388,
            -263.7478782012471,
            -519.4568497872579,
        ],
    );
}

#[test]
fn distance_3d_with_parts_matches_brute_oracle() {
    let long_line = with_z(dense_line(0.0, 0.0, 2_000), 0.0);
    let crossing_a = with_z(line(vec![p(0.0, 0.0), p(10.0, 10.0)]), 0.0);
    let crossing_b = with_z(line(vec![p(5.0, 0.0), p(5.0, 10.0)]), 0.0);
    let boundary = with_z(
        polygon(vec![
            p(0.0, 0.0),
            p(4.0, 0.0),
            p(4.0, 4.0),
            p(0.0, 4.0),
            p(0.0, 0.0),
        ]),
        0.0,
    );
    let cases = [
        (with_z(Shape::Point(p(0.0, 0.0)), 0.0), long_line),
        (crossing_a, crossing_b),
        (with_z(Shape::Point(p(1.0, 1.0)), 0.0), boundary),
        (
            with_z(Shape::Point(p(0.0, 0.0)), 0.0),
            with_z(Shape::Point(p(1e150, 1e150)), 0.0),
        ),
        (
            with_z(Shape::Point(p(1e-200, 1e-200)), 0.0),
            with_z(Shape::Point(p(0.0, 0.0)), 0.0),
        ),
        (
            with_z(multipoint(vec![p(0.0, 0.0), p(10.0, 0.0)]), 0.0),
            with_z(line(vec![p(5.0, 5.0), p(5.0, -5.0)]), 0.0),
        ),
    ];
    for (left_z, right_z) in cases {
        let left_parts = Distance3dParts::build(&left_z);
        let right_parts = Distance3dParts::build(&right_z);
        let brute = distance_3d_brute_parts(&left_parts, &right_parts);
        let pruned = distance_3d_with_parts(&left_parts, &right_parts);
        assert_eq!(
            pruned.to_bits(),
            brute.to_bits(),
            "left={left_z:?} right={right_z:?}"
        );
    }
}

#[test]
fn distance_3d_rejects_mixed_z_collection() {
    let mixed = Shape::GeometryCollection(vec![
        Shape::Point(p(0.0, 0.0)),
        with_z(line(vec![p(0.0, 0.0), p(1.0, 1.0)]), 0.0),
    ]);
    let target = with_z(Shape::Point(p(1.0, 1.0)), 1.0);
    mixed.distance_3d(&target).unwrap_err();
}

fn extreme_coord_line(segment_count: usize) -> Shape {
    // Just inside the squared-space gate so the facet BVH uses the SIMD
    // kernel — segment deltas stay below the per-pair overflow ceiling.
    let scale = 1e150;
    line(
        (0..=segment_count)
            .map(|index| {
                let t = index as f64 / segment_count as f64;
                p(-scale + t * 2.0 * scale, 0.0)
            })
            .collect(),
    )
}

fn brute_point_line_distance(point: Point, line: &Shape) -> f64 {
    let mut best = f64::INFINITY;
    line.for_each_segment(|segment| {
        best = best.min(point_segment_distance_squared(point.xy(), segment));
    });
    best.sqrt()
}

#[test]
fn facet_simd_extreme_coords_matches_scalar_oracle() {
    use crate::geometry::facet_bvh::BVH_MIN_INDEXED_SEGMENTS;

    let probe = p(0.0, 1.0);
    for segment_count in [BVH_MIN_INDEXED_SEGMENTS, BVH_MIN_INDEXED_SEGMENTS + 1] {
        let line = extreme_coord_line(segment_count);
        let expected = brute_point_line_distance(probe, &line);
        let actual = line.distance(&Shape::Point(probe));
        assert_eq!(
            actual.to_bits(),
            expected.to_bits(),
            "segment_count={segment_count} actual={actual} expected={expected}"
        );
        assert_eq!(
            1.0_f64.to_bits(),
            actual.to_bits(),
            "segment_count={segment_count}"
        );
    }
}

#[test]
fn facet_simd_extreme_hausdorff_matches_scalar_oracle() {
    use crate::geometry::facet_bvh::BVH_MIN_INDEXED_SEGMENTS;

    for segment_count in [BVH_MIN_INDEXED_SEGMENTS, BVH_MIN_INDEXED_SEGMENTS + 7] {
        let left = extreme_coord_line(segment_count);
        let right = left.translate(1e149, 0.0).expect("translate extreme line");
        let expected = brute_directed_hausdorff_squared(&left, &right)
            .max(brute_directed_hausdorff_squared(&right, &left))
            .sqrt();
        let actual = left.hausdorff_distance(&right);
        assert!(
            actual.is_finite() && expected.is_finite(),
            "segment_count={segment_count} actual={actual} expected={expected}"
        );
        assert_hausdorff_close(actual, expected);
    }
}
