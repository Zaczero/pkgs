use crate::geometry::distance::*;
use crate::geometry::*;

pub(super) fn p(x: f64, y: f64) -> Point {
    Point::new_raw_xy(x, y)
}

#[test]
fn vertex_point_sweep_restarts_probes_for_each_target_and_keeps_its_seed() {
    let target = PreparedPlan::new(crate::io::parse_wkt("MULTIPOINT ((100 0), (6 1))").unwrap());
    let targets = target.distance_parts().unwrap();
    for wkt in [
        "LINESTRING (0 0, 8 0, 6 2)",
        "MULTILINESTRING ((0 0, 8 0), (6 2, 6 2))",
        "MULTIPOINT ((0 0), (8 0), (6 2))",
    ] {
        let source = PreparedPlan::new(crate::io::parse_wkt(wkt).unwrap());
        let probes = source.distance_parts().unwrap();
        for (seed, expected) in [(f64::INFINITY, 1.0_f64), (0.25, 0.25)] {
            assert_eq!(
                min_parts_to_parts::<true>(probes, targets, seed)
                    .unwrap()
                    .to_bits(),
                expected.to_bits(),
                "squared vertex sweep for {wkt}, seed={seed}"
            );
            assert_eq!(
                min_parts_to_parts::<false>(probes, targets, seed)
                    .unwrap()
                    .to_bits(),
                expected.to_bits(),
                "distance vertex sweep for {wkt}, seed={seed}"
            );
        }
    }
    let empty = PreparedPlan::new(crate::io::parse_wkt("LINESTRING EMPTY").unwrap());
    assert_eq!(
        min_parts_to_parts::<true>(empty.distance_parts().unwrap(), targets, 17.0)
            .unwrap()
            .to_bits(),
        17.0_f64.to_bits(),
    );
    assert_eq!(
        min_parts_to_parts::<false>(empty.distance_parts().unwrap(), targets, 17.0)
            .unwrap()
            .to_bits(),
        17.0_f64.to_bits(),
    );
}

#[test]
fn continuous_hausdorff_keeps_tiny_parallel_separation() {
    let separation = 1e-300;
    let left = line(vec![p(0.0, 0.0), p(2.0 * separation, 0.0)]);
    let right = line(vec![p(0.0, separation), p(2.0 * separation, separation)]);
    let max_error = separation * 1e-12;
    assert!(
        (hausdorff_distance(&left, &right, Some(max_error), 1.0).unwrap() - separation).abs()
            <= max_error
    );

    let base = 1e200_f64;
    let huge_separation = f64::from_bits(base.to_bits() + 16) - base;
    let left = line(vec![p(0.0, base), p(1.0, base)]);
    let right = line(vec![
        p(0.0, base + huge_separation),
        p(1.0, base + huge_separation),
    ]);
    let max_error = huge_separation * 1e-12;
    assert!(
        (hausdorff_distance(&left, &right, Some(max_error), 1.0).unwrap() - huge_separation).abs()
            <= max_error
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
    assert!(
        (hausdorff_distance(&left, &right, Some(max_error), 1.0).unwrap() - huge_separation).abs()
            <= max_error
    );
}

#[test]
fn filled_hausdorff_support_matches_shared_wkb_expansion() {
    let square = polygon(vec![
        p(0.0, 0.0),
        p(1.0, 0.0),
        p(1.0, 1.0),
        p(0.0, 1.0),
        p(0.0, 0.0),
    ]);
    let target = Shape::Point(p(3.0, 3.0));
    for depth in [6, 10] {
        let mut shared = square.clone();
        for _ in 0..depth {
            shared = Shape::geometry_collection(vec![shared.clone(), shared]).unwrap();
        }
        let expanded = crate::io::parse_wkb(
            &crate::io::to_wkb(&shared, None, false).expect("shared source writes"),
        )
        .expect("unshared source expansion parses")
        .shape;

        assert_eq!(
            hausdorff_distance(&shared, &target, Some(1e-9), 1.0)
                .unwrap()
                .to_bits(),
            hausdorff_distance(&expanded, &target, Some(1e-9), 1.0)
                .unwrap()
                .to_bits(),
            "depth {depth}"
        );
    }
}

#[test]
fn point_segment_distance_divides_before_unscaling_tiny_area() {
    let end = 1e-200_f64;
    let midpoint = end / 2.0;
    let offset_y = f64::from_bits(midpoint.to_bits() + 1);
    let actual = point_segment_distance(XY::new(midpoint, offset_y), Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(end, end),
    })
    .unwrap();
    // `Fraction` from these STORED doubles gives
    // d² = (offset_y - midpoint)² / 2 exactly. Its correctly-rounded square
    // root is 0x1.6a09e667f3bcdp-719; dividing by rounded sqrt(2) is one ulp low.
    let expected = f64::from_bits(1_370_959_738_765_786_061);
    assert!(actual > 0.0);
    assert_eq!(actual.to_bits(), expected.to_bits());
}

#[test]
fn continuous_hausdorff_preserves_underflowed_local_determinant() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let length = exact_power_of_two(996);
    let height = exact_power_of_two(396);
    let offset = exact_power_of_two(344);
    let baseline = line(vec![p(0.0, 0.0), p(length, height)]);
    let graph = line(vec![
        p(0.0, 0.0),
        p(height, 0.0),
        p(length, height + offset),
    ]);
    // The endpoint witness itself exercises the underflowed determinant. A
    // broad bound keeps this test about preserving a positive exact witness
    // rather than paying hundreds of anisotropic subdivision levels.
    assert!(hausdorff_distance(&baseline, &graph, Some(length), 1.0).unwrap() > 0.0);
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
        let max_error = expected * 1e-12;
        assert!(
            (hausdorff_distance(&baseline, &bent, Some(max_error), 1.0).unwrap() - expected).abs()
                <= max_error,
            "reciprocal-axis Hausdorff failed at e={exponent}",
        );
        assert!(
            (hausdorff_distance(&bent, &baseline, Some(max_error), 1.0).unwrap() - expected).abs()
                <= max_error
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
            point_segment_distance(XY::new(0.0, 0.0), Segment {
                start: XY::new(-length, 0.0),
                end: XY::new(-half_gap, 0.0),
            },)
            .unwrap()
            .to_bits(),
            half_gap.to_bits(),
        );
        let max_error = half_gap * 1e-12;
        assert!(
            (hausdorff_distance(&source, &target, Some(max_error), 1.0).unwrap() - half_gap).abs()
                <= max_error,
            "reciprocal-axis target gap failed at e={exponent}",
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
            let source = Shape::geometry_collection(vec![horizontal.clone(), vertical])
                .expect("test collection is valid");
            let target = Shape::geometry_collection(vec![horizontal, lower, upper])
                .expect("test collection is valid");
            let expected = height / 2.0;
            let max_error = expected * 1e-12;
            assert!(
                (hausdorff_distance(&source, &target, Some(max_error), 1.0).unwrap() - expected)
                    .abs()
                    <= max_error,
                "minor-axis collection gap failed at e={exponent}, swap={swap}"
            );
            assert!(
                (hausdorff_distance(&target, &source, Some(max_error), 1.0).unwrap() - expected)
                    .abs()
                    <= max_error
            );
        }
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "test builder takes temporary shapes and returns their transformed value"
)]
fn with_z(shape: Shape, z: f64) -> Shape {
    shape.replace_z(Some(z), true).expect("replace Z")
}

fn pzm(x: f64, y: f64, z: f64, m: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).expect("finite test point")
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "test helpers take owned fixture vectors at the call site"
)]
pub(super) fn line(points: Vec<Point>) -> Shape {
    Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).expect("test line is valid"),
    )
}

fn multiline(lines: Vec<Vec<Point>>) -> Shape {
    Shape::MultiLineString(
        lines
            .into_iter()
            .map(|points| CoordSeq::try_from_points(&points).unwrap())
            .map(|line| LineSeq::try_new(line).expect("test line is valid"))
            .collect(),
    )
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "test helpers take owned fixture vectors at the call site"
)]
pub(super) fn multipoint(points: Vec<Point>) -> Shape {
    Shape::multi_point(
        crate::geometry::CoordinateAxes::XY,
        CoordSeq::try_from_points(&points).unwrap(),
    )
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "test helpers take owned fixture vectors at the call site"
)]
pub(super) fn polygon(points: Vec<Point>) -> Shape {
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

pub(super) fn dense_line(start_lon: f64, start_lat: f64, count: usize) -> Shape {
    line(
        (0..count)
            .map(|index| {
                let t = index as f64 / count.wrapping_sub(1) as f64;
                p(
                    start_lon + t * 4.0,
                    start_lat + (t * std::f64::consts::TAU).sin() * 0.25,
                )
            })
            .collect(),
    )
}

fn assert_hausdorff_close(actual: f64, expected: f64, error: f64) {
    assert!(actual.is_finite(), "{actual}");
    assert!(
        (actual - expected).abs() <= error,
        "actual {actual} expected {expected}"
    );
}

#[test]
fn continuous_hausdorff_on_subdivided_oblique_segments() {
    fn subdivided(from: (f64, f64), to: (f64, f64), count: usize) -> Shape {
        let step = |a: f64, b: f64, i: usize| a + (b - a) * (i as f64) / ((count - 1) as f64);
        line(
            (0..count)
                .map(|i| p(step(from.0, to.0, i), step(from.1, to.1, i)))
                .collect(),
        )
    }
    for count in [9, 17, 33] {
        let left = subdivided((0.0, 0.0), (10.0, 3.0), count);
        let right = subdivided((0.0, 1.0), (7.0, 9.0), count);
        // The far endpoint (7,9) projects inside the other segment:
        // perpendicular distance is |10*9-3*7| / sqrt(10²+3²).
        let expected = 69.0 / 109.0_f64.sqrt();
        assert_hausdorff_close(
            hausdorff_distance(&left, &right, Some(1e-9), 1.0).unwrap(),
            expected,
            1e-9,
        );
        assert_hausdorff_close(
            hausdorff_distance(&right, &left, Some(1e-9), 1.0).unwrap(),
            expected,
            1e-9,
        );
    }
}

#[test]
fn continuous_hausdorff_matches_known_fixtures() {
    for (left, right, expected) in [
        (
            line(vec![p(0.0, 0.0), p(1.0, 1.0), p(2.0, 1.0)]),
            line(vec![p(0.0, 1.0), p(1.0, 2.0), p(2.0, 2.0)]),
            1.0,
        ),
        (
            line(vec![p(0.0, 0.0), p(10.0, 0.0)]),
            line(vec![p(0.0, 1.0), p(5.0, 8.0), p(10.0, 1.0)]),
            8.0,
        ),
        (
            Shape::Point(p(0.0, 0.0)),
            line(vec![p(0.0, 1.0), p(3.0, 1.0)]),
            10.0_f64.sqrt(),
        ),
        (
            line(vec![p(-1.0, 0.5), p(4.0, 0.5)]),
            polygon(vec![p(0.0, 0.0), p(2.0, 0.0), p(2.0, 2.0), p(0.0, 0.0)]),
            2.0,
        ),
        (
            multipoint(vec![p(0.0, 0.0), p(5.0, 5.0)]),
            line(vec![p(0.0, 1.0), p(10.0, 1.0)]),
            41.0_f64.sqrt(),
        ),
    ] {
        assert_hausdorff_close(
            hausdorff_distance(&left, &right, Some(1e-9), 1.0).unwrap(),
            expected,
            1e-9,
        );
    }
}

#[test]
fn continuous_hausdorff_point_semantics() {
    let origin = Shape::Point(p(0.0, 0.0));
    assert!(
        (hausdorff_distance(&origin, &Shape::Point(p(3.0, 4.0)), Some(1e-9), 1.0).unwrap() - 5.0)
            .abs()
            <= 1e-9
    );
    let tiny = 1.5e-162;
    let max_error = tiny * 1e-12;
    assert!(
        (hausdorff_distance(&origin, &Shape::Point(p(tiny, 0.0)), Some(max_error), 1.0).unwrap()
            - tiny)
            .abs()
            <= max_error
    );
}

#[test]
fn point_targets_keep_interior_and_endpoint_maxima() {
    for (source, target, expected) in [
        (
            line(vec![p(0.0, 0.0), p(2.0, 0.0)]),
            multipoint(vec![p(0.0, 1.0), p(1.0, 1.0), p(2.0, 1.0)]),
            1.25_f64.sqrt(),
        ),
        (
            line(vec![p(0.0, 0.0), p(1.0, 0.0)]),
            multipoint(vec![p(0.0, 1.0), p(0.6, 0.8)]),
            1.0,
        ),
    ] {
        assert_hausdorff_close(
            hausdorff_distance(&source, &target, Some(1e-10), 1.0).unwrap(),
            expected,
            1e-10,
        );
    }
}

#[test]
fn backtracking_collinear_line_keeps_its_full_support() {
    let left = line(vec![p(0.0, 0.0), p(10.0, 0.0), p(-5.0, 0.0)]);
    let right = line(vec![p(-5.0, 1.0), p(0.0, 1.0)]);
    let expected = 101.0_f64.sqrt();
    assert_hausdorff_close(
        hausdorff_distance(&left, &right, Some(1e-9), 1.0).unwrap(),
        expected,
        1e-9,
    );
    assert_hausdorff_close(
        hausdorff_distance(&right, &left, Some(1e-9), 1.0).unwrap(),
        expected,
        1e-9,
    );
}

#[test]
fn packed_hausdorff_columns_preserve_translations_within_bound() {
    for count in [2, 3, 4, 9, 17] {
        let xs: Vec<_> = (0..count).map(f64::from).collect();
        let ys: Vec<_> = (0..count).map(|i| f64::from(i % 2)).collect();
        let shifted: Vec<_> = ys.iter().map(|y| y + 0.25).collect();
        let value =
            hausdorff_distance_line_columns(&xs, &ys, &xs, &shifted, Some(1e-9), 1.0).unwrap();
        assert_hausdorff_close(value, 0.25, 1e-9);
    }
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
            Shape::Point(pzm(0.0, 0.0, 0.0, 1e200)),
            Shape::Point(pzm(3.0, 4.0, 12.0, -1e200)),
        ),
        (
            with_z(multipoint(vec![p(0.0, 0.0), p(10.0, 0.0)]), 0.0),
            with_z(line(vec![p(5.0, 5.0), p(5.0, -5.0)]), 0.0),
        ),
    ];
    for (left_z, right_z) in cases {
        let left_parts = Distance3dParts::build(&left_z);
        let right_parts = Distance3dParts::build(&right_z);
        let brute =
            distance_3d_brute_parts(&left_parts, &right_parts).expect("exact brute distance");
        let pruned =
            distance_3d_with_parts(&left_parts, &right_parts).expect("exact pruned distance");
        assert_eq!(
            pruned.to_bits(),
            brute.to_bits(),
            "left={left_z:?} right={right_z:?}"
        );
    }
}

#[test]
fn distance_3d_rejects_mixed_z_collection() {
    let mixed = Shape::geometry_collection(vec![
        Shape::Point(p(0.0, 0.0)),
        with_z(line(vec![p(0.0, 0.0), p(1.0, 1.0)]), 0.0),
    ])
    .expect("test collection is valid");
    let target = with_z(Shape::Point(p(1.0, 1.0)), 1.0);
    mixed.distance_3d(&target).unwrap_err();
}

#[test]
fn distance_3d_distinguishes_shared_point_and_segment_runs() {
    let coords =
        CoordSeq::try_from_points(&[pzm(0.0, 0.0, 0.0, 1.0), pzm(10.0, 0.0, 0.0, 2.0)]).unwrap();
    let source = Shape::geometry_collection(vec![
        Shape::MultiPoint(MultiPoint::from_coordseq(coords.clone()).unwrap()),
        Shape::LineString(LineSeq::from_proven(coords)),
    ])
    .unwrap();
    let target = Shape::Point(pzm(5.0, 0.0, 0.0, 0.0));

    assert_eq!(source.distance_3d(&target).unwrap(), 0.0);
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
        best = best.min(point_segment_distance_squared(point.xy(), segment).unwrap());
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
        let actual = line.distance(&Shape::Point(probe)).unwrap();
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
fn continuous_hausdorff_keeps_extreme_collinear_translation() {
    for segment_count in [33, 65] {
        let left = extreme_coord_line(segment_count);
        let right = left.translate(1e149, 0.0).expect("translate extreme line");
        let expected = ((1e150_f64 + 1e149) - 1e150).max((-1e150_f64 + 1e149) + 1e150);
        let actual = hausdorff_distance(&left, &right, Some(1e137), 1.0).unwrap();
        assert_hausdorff_close(actual, expected, 1e137);
    }
}
