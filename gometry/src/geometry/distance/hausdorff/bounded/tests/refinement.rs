#[cfg(debug_assertions)]
struct Seed42Pcg64 {
    state: u128,
    increment: u128,
}

#[cfg(debug_assertions)]
impl Seed42Pcg64 {
    // NumPy default_rng(42)'s PCG64 state and 128/64 multiplier.
    const MULTIPLIER: u128 = 47_026_247_687_942_121_848_144_207_491_837_523_525;

    const fn new() -> Self {
        Self {
            state: 274_674_114_334_540_486_603_088_602_300_644_985_544,
            increment: 332_724_090_758_049_132_448_979_897_138_935_081_983,
        }
    }

    fn unit(&mut self) -> f64 {
        self.state = self
            .state
            .wrapping_mul(Self::MULTIPLIER)
            .wrapping_add(self.increment);
        let mixed = (self.state >> 64) as u64 ^ self.state as u64;
        let rotation = (self.state >> 122) as u32;
        (mixed.rotate_right(rotation) >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0)
    }
}

#[cfg(debug_assertions)]
fn seed42_radial_points(count: usize) -> Vec<(f64, f64)> {
    // Recreate the seed-42 sorted-angle/radius workload with native scalar trig.
    let mut generator = Seed42Pcg64::new();
    let mut angles = Vec::with_capacity(count);
    for _ in 0..count {
        angles.push(generator.unit() * std::f64::consts::TAU);
    }
    angles.sort_unstable_by(f64::total_cmp);
    let mut radii = Vec::with_capacity(count);
    for _ in 0..count {
        radii.push(0.8 + 0.2 * generator.unit());
    }
    angles
        .into_iter()
        .zip(radii)
        .map(|(angle, radius)| (angle.cos() * radius, angle.sin() * radius))
        .collect()
}

#[test]
fn extreme_exponent_spread_hausdorff_uses_bounded_refinement_work() {
    let length = 2.0_f64.powi(996);
    let height = 2.0_f64.powi(396);
    let offset = 2.0_f64.powi(344);
    let baseline = line(&[(0.0, 0.0), (length, height)]);
    let graph = line(&[(0.0, 0.0), (height, 0.0), (length, height + offset)]);
    let errors = [
        (
            "one ULP",
            Some(f64::from_bits(offset.to_bits() + 1) - offset),
        ),
        ("1e-12", Some(1e-12 * offset)),
        ("1e-6", Some(1e-6 * offset)),
        ("default", None),
    ];

    for (name, max_error) in errors {
        let (actual, work) = {
            let _guard = limit_refinement_work(10_000);
            let actual = distance(&baseline, &graph, max_error, 1.0).unwrap();
            (actual, refinement_work())
        };

        assert_eq!(actual, offset, "{name} max_error");
        assert!(
            work <= 8,
            "{name} max_error required {work} refinement steps"
        );
    }
}

#[test]
fn invalid_polygon_pair_stays_within_a_refinement_budget() {
    let left = crate::io::parse_wkt("POLYGON ((6 5, 4 6, 2 2, 6 6, 7 7, 7 8, 7 0, 6 5))").unwrap();
    let right = crate::io::parse_wkt(
        "POLYGON ((5 3, 0 2, 1 8, 1 0, 3 1, 5 3), \
         (8 2, 8 2, 1 6, 2 3, 2 1, 8 2))",
    )
    .unwrap();
    let limit = 256;
    let _guard = limit_refinement_work(limit);

    let result = distance(&left, &right, None, 1.0);
    let work = refinement_work();

    assert!(
        work <= limit,
        "invalid polygon Hausdorff used {work} refinements: {result:?}"
    );
    assert!(
        result.is_ok()
            || result.as_ref().is_err_and(|error| !matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(
                    crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge { .. }
                )
            )),
        "refinement budget error escaped instead of an answer or domain error: {result:?}"
    );
}

#[test]
fn hausdorff_f64_max_endpoint_rounds_to_max_before_refinement() {
    let point = Shape::Point(Point::new_raw_xy(-1.0, 1e-15));
    let line = line(&[(1e15, 0.0), (f64::MAX, 0.0)]);
    let _guard = limit_refinement_work(32);

    let actual = distance(&point, &line, None, 1.0).unwrap();

    assert_eq!(actual, f64::MAX);
    assert_eq!(refinement_work(), 0);
}

#[test]
fn hausdorff_unrepresentable_diagonal_fails_before_refinement() {
    let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let line = line(&[(1.0, 1.0), (f64::MAX, f64::MAX)]);
    let _guard = limit_refinement_work(32);

    let error = distance(&point, &line, None, 1.0).unwrap_err();

    assert!(
        matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(message))
                if message.as_ref()
                    == "hausdorff_distance cannot represent the effective accuracy"
        ),
        "{error}"
    );
    assert_eq!(refinement_work(), 0);
}

#[test]
fn ordinary_l_shape_refinement_work_stays_bounded() {
    let coordinates = [
        (0.0, 0.0),
        (9.0, 0.0),
        (9.0, 1.0),
        (4.0, 1.0),
        (4.0, 8.0),
        (0.0, 8.0),
        (0.0, 0.0),
    ];
    let source = polygon(&coordinates);
    let target = line(&coordinates);
    let limit = 1_000;
    let _guard = limit_refinement_work(limit);
    let result = distance(&source, &target, Some(1e-12), 1.0);
    let work = refinement_work();
    let actual = result.unwrap_or_else(|error| {
        panic!("ordinary L-shape exceeded {limit} refinements at {work}: {error}")
    });
    assert!(
        (actual - 2.0).abs() <= 1e-12,
        "ordinary L-shape returned {actual}, expected 2.0 within 1e-12"
    );
    assert!(
        work <= limit,
        "ordinary L-shape used {work} refinements, limit {limit}"
    );
}

#[test]
fn directed_adjacent_box_refinement_work_scales_with_boundary_spans() {
    for count in [8, 16, 32, 64] {
        let shape = adjacent_box_union(count);
        let tolerance = derived::scale_aware_default_tolerance(&shape);
        let (source, boundary) = adjacent_box_prepared_support(count);
        let limit = count * 64;
        let (result, work) = {
            let _guard = limit_refinement_work(limit);
            let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
            let mut context = SearchContext::new_directed(
                &accuracy,
                Rational::one(),
                Rational::from_f64(tolerance),
            );
            let result = run_directed(&source, &boundary, &mut context);
            let work = refinement_work();
            (result, work)
        };
        let result = result.unwrap();
        let radius = downward_sqrt(&result.center_squared).unwrap();
        assert_eq!(radius, 0.5);
        assert!(
            work <= limit,
            "adjacent boxes used {work} refinements, limit {limit}"
        );
    }
}

fn regular_polygon(count: usize, offset: (f64, f64)) -> Shape {
    let mut vertices: Vec<(f64, f64)> = (0..count)
        .map(|index| {
            let angle = std::f64::consts::TAU * index as f64 / count as f64;
            (
                100.0 * angle.cos() + offset.0,
                100.0 * angle.sin() + offset.1,
            )
        })
        .collect();
    let first = *vertices
        .first()
        .expect("regular polygon tests use a positive vertex count");
    vertices.push(first);
    polygon(&vertices)
}

#[test]
fn filled_translation_refinement_stays_near_the_boundary_maximum() {
    // A convex polygon against its translation by t is exactly |t| apart.
    // Boxes that cannot hold an interior maximum are discarded instead of
    // being refined until their corners fall below it; without that the
    // 64-gon needed over 13,000 refinements, and now about ten.
    for count in [4, 16, 64] {
        let source = regular_polygon(count, (0.0, 0.0));
        let target = regular_polygon(count, (1.0, 0.5));
        let _guard = limit_refinement_work(100);
        let value = distance(&source, &target, None, 1.0).unwrap();
        assert!(
            (value - 1.25_f64.sqrt()).abs() <= 1e-9 * value,
            "{count}: {value}"
        );
    }
}

#[test]
fn filled_interior_maximum_inside_a_target_hole_survives_box_discards() {
    // The hole is a 3-4-5 right triangle, so the square's farthest point
    // from the holed target is the hole's incenter at clearance 1. Its
    // three nearest edges form a closed cycle around it.
    let shell = [(2.0, 2.0), (8.0, 2.0), (8.0, 8.0), (2.0, 8.0), (2.0, 2.0)];
    let hole = [(3.0, 3.0), (3.0, 6.0), (7.0, 3.0), (3.0, 3.0)];
    let ring = |points: &[(f64, f64)]| {
        let points: Vec<_> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(&points).unwrap()).unwrap()
    };
    let holed = Shape::Polygon(crate::geometry::Polygon::new(ring(&shell), vec![ring(
        &hole,
    )]));
    let value = distance(&polygon(&shell), &holed, None, 1.0).unwrap();
    assert!((value - 1.0).abs() <= 1e-9, "{value}");
}

#[test]
fn ordinary_hausdorff_keeps_external_line_support_with_filled_area() {
    let area = polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]);
    let external = line(&[(10.0, 0.0), (12.0, 0.0)]);
    let mixed = Shape::geometry_collection(vec![area, external.clone()]).unwrap();
    let target = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    assert!((distance(&mixed, &target, Some(1e-9), 1.0).unwrap() - 12.0).abs() <= 1e-9);

    let mut line_budget = ExpansionBudget::new("hausdorff", "line support");
    let line_support = PreparedSupport::from_shape(&external, &mut line_budget).unwrap();
    assert!(line_support.area_locator.is_none());
    assert!(line_support.boundary_edges.is_empty());

    let mut budget = ExpansionBudget::new("hausdorff", "support");
    let prepared = PreparedSupport::from_shape(&mixed, &mut budget).unwrap();
    assert!(prepared.initial_patches().iter().any(|patch| {
        matches!(
            patch,
            SourcePatch::Span { span, .. }
                if prepared.spans[*span].source.start == XY::new(10.0, 0.0)
                    && prepared.spans[*span].source.end == XY::new(12.0, 0.0)
        )
    }));
}

#[test]
fn tiny_filled_support_uses_a_bounded_exact_boundary_scan() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let mut budget = ExpansionBudget::new("hausdorff", "tiny filled support");
    let support = PreparedSupport::from_shape(&shape, &mut budget).unwrap();
    assert!(support.area_locator.is_none());
    assert_eq!(support.boundary_edges.len(), 4);

    for ((x, y), expected) in [((2.0, 2.0), true), ((0.0, 2.0), true), ((5.0, 2.0), false)] {
        let point = ExactPoint::from_xy(XY::new(x, y));
        assert_eq!(support.contains_area(&point).unwrap(), expected);
    }

    let twice_wound =
        crate::io::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))").unwrap();
    let mut budget = ExpansionBudget::new("hausdorff", "twice-wound tiny support");
    let support = PreparedSupport::from_shape(&twice_wound, &mut budget).unwrap();
    assert!(support.area_locator.is_none());
    assert_eq!(support.boundary_edges.len(), 4);
    let point = ExactPoint::from_xy(XY::new(2.0, 2.0));
    let source = cartesian_exact_point_location_and_area(&twice_wound, &[
        ExactDyadic::from_f64(2.0),
        ExactDyadic::from_f64(2.0),
        ExactDyadic::one(),
    ])
    .unwrap();
    assert!(source.area);
    assert!(support.contains_area(&point).unwrap());

    let cancelled = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION (\
         POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0)),\
         LINESTRING (10 0,11 0))",
    )
    .unwrap();
    let mut budget = ExpansionBudget::new("hausdorff", "cancelled tiny support");
    let support = PreparedSupport::from_shape(&cancelled, &mut budget).unwrap();
    assert!(support.area_locator.is_none());
    assert!(support.boundary_edges.is_empty());
    assert!(!support.contains_area(&point).unwrap());
}

#[cfg(debug_assertions)]
#[test]
fn seed42_filled_membership_work_scales_with_slab_searches() {
    let mut totals = Vec::new();
    for count in [128, 256, 512, 1024, 2048, 4096, 8192] {
        let mut source_coordinates = seed42_radial_points(count);
        source_coordinates.push(source_coordinates[0]);
        if count == 128 {
            assert_eq!(
                source_coordinates[0].0.to_bits(),
                0.819_802_683_715_413_4_f64.to_bits()
            );
        }
        let target_coordinates: Vec<_> = source_coordinates
            .iter()
            .map(|&(x, y)| (x + 0.1, y + 0.1))
            .collect();
        let target = polygon(&target_coordinates);
        let mut budget = ExpansionBudget::new("hausdorff_distance", "test membership");
        let support = PreparedSupport::from_shape(&target, &mut budget).unwrap();
        let plan = crate::geometry::PreparedPlan::transient(&target);
        let tester = plan.prepare_point_queries(4).unwrap();

        // Q ~= n/log(n) keeps total work sublinear in the source size if
        // per-query candidate enumeration grows with the crossing set.
        let mut probes = Seed42Pcg64::new();
        let query_count = count.div_ceil(usize::try_from(count.ilog2()).unwrap());
        let mut probe_points = Vec::with_capacity(query_count);
        for _ in 0..query_count {
            probe_points.push((3.0 * probes.unit() - 1.5, 3.0 * probes.unit() - 1.5));
        }
        let streaming_oracle_stride = (query_count / 8).max(1);
        reset_query_comparisons();
        for (index, &(x, y)) in probe_points.iter().enumerate() {
            let point = ExactPoint::from_xy(XY::new(x, y));
            let actual = support.contains_area(&point).unwrap();
            let exact = [
                ExactDyadic::from_f64(x),
                ExactDyadic::from_f64(y),
                ExactDyadic::one(),
            ];
            let prepared = tester.locate_exact(&exact).unwrap();
            assert_eq!(actual, prepared.area, "seed42 n={count}, probe={index}");
            if index % streaming_oracle_stride == 0 {
                let streaming = cartesian_exact_point_location_and_area(&target, &exact).unwrap();
                assert_eq!(prepared, streaming, "seed42 n={count}, probe={index}");
            }
        }
        let comparisons = query_comparisons();
        let edge_tests = query_edge_tests();
        assert!(
            edge_tests > 0,
            "seed42 n={count} recorded no slab edge comparisons"
        );
        assert!(comparisons >= edge_tests);
        totals.push((count, comparisons, edge_tests));
        eprintln!(
            "seed42 n={count} area-locator comparisons={comparisons} edge-tests={edge_tests}"
        );
    }
    let (small_n, _, small_edge_tests) = totals[0];
    let (large_n, _, large_edge_tests) = *totals.last().unwrap();
    let exponent = (large_edge_tests as f64 / small_edge_tests as f64).ln()
        / (large_n as f64 / small_n as f64).ln();
    assert!(
        exponent <= 1.15,
        "seed-42 membership edge-test exponent {exponent:.3} exceeds 1.15: {totals:?}"
    );
}

#[test]
fn queue_enclosure_keeps_unbounded_patches_and_later_witnesses() {
    let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
    let mut context = SearchContext::new(&accuracy);
    let mut queue = PatchQueue::new(false);
    let patch = || {
        SourcePatch::Box(FloatBox {
            min_x: 0.0,
            max_x: 1.0,
            min_y: 0.0,
            max_y: 1.0,
        })
    };
    queue
        .push(PatchRecord {
            patch: patch(),
            upper: Some(Rational::one()),
        })
        .unwrap();
    context.update_lower(Rational::from_f64(4.0)).unwrap();
    let upper = queue_upper(&queue, &context).unwrap().unwrap();
    assert_ne!(
        upper.try_cmp(&Rational::from_f64(2.0)).unwrap(),
        Ordering::Less
    );
    queue
        .push(PatchRecord {
            patch: patch(),
            upper: None,
        })
        .unwrap();
    assert!(queue_upper(&queue, &context).unwrap().is_none());
}

#[test]
fn box_radius_covers_unrepresentable_midpoint_corners() {
    let source = PreparedSupport::from_line_columns(&[0.0], &[0.0]).unwrap();
    let patch = SourcePatch::Box(FloatBox {
        min_x: 1.0,
        max_x: 1.0_f64.next_up(),
        min_y: 0.0,
        max_y: f64::from_bits(1),
    });
    let (center, radius) = patch.center_and_radius_squared(&source).unwrap();
    for corner in patch.corners(&source).unwrap() {
        let dx = corner.x().try_subtract(center.x()).unwrap();
        let dy = corner.y().try_subtract(center.y()).unwrap();
        let squared = dx
            .try_square()
            .unwrap()
            .try_add(&dy.try_square().unwrap())
            .unwrap();
        assert_ne!(squared.try_cmp(&radius).unwrap(), Ordering::Greater);
    }
}

#[test]
fn segment_to_point_distance_attains_the_farthest_endpoint() {
    let source = line(&[(0.0, 0.0), (2.0, 0.0)]);
    let target = Shape::Point(Point::new_raw_xy(1.0, 1.0));
    let value = distance(&source, &target, Some(1e-6), 1.0).unwrap();
    assert!((value - 2.0_f64.sqrt()).abs() <= 1e-6);
}

#[test]
fn filled_square_to_point_distance_attains_the_farthest_corner() {
    let source = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let target = Shape::Point(Point::new_raw_xy(6.0, 2.0));
    let value = distance(&source, &target, Some(1e-3), 1.0).unwrap();
    assert!((value - 40.0_f64.sqrt()).abs() <= 1e-3);
}

#[test]
fn output_units_preserve_extreme_finite_distances() {
    for (coordinate, scale) in [
        (2.0_f64.powi(1023), 2.0_f64.powi(-1023)),
        (2.0_f64.powi(-1023), 2.0_f64.powi(1023)),
    ] {
        let left = Shape::Point(Point::new_raw_xy(-coordinate, 0.0));
        let right = Shape::Point(Point::new_raw_xy(coordinate, 0.0));
        assert_eq!(distance(&left, &right, Some(1e-9), scale).unwrap(), 2.0);
    }
}

#[test]
fn output_range_and_scaled_absolute_error_are_certified() {
    let huge = 2.0_f64.powi(1023);
    let left = Shape::Point(Point::new_raw_xy(-huge, 0.0));
    let right = Shape::Point(Point::new_raw_xy(huge, 0.0));
    distance(&left, &right, Some(1.0), 1.0).unwrap_err();

    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let large = Shape::Point(Point::new_raw_xy(1e200, 1e200));
    let expected = 2.0_f64.sqrt() * 1e200;
    let actual = distance(&origin, &large, None, 1.0).unwrap();
    assert!((actual - expected).abs() <= 1e-9 * expected);

    let vertices = [
        (0.0, 0.0),
        (9.0, 0.0),
        (9.0, 3.0),
        (4.0, 3.0),
        (4.0, 8.0),
        (0.0, 8.0),
        (0.0, 0.0),
    ];
    let expected = 100.0 * (7.0 - 24.0_f64.sqrt());
    let actual = distance(&polygon(&vertices), &line(&vertices), Some(0.01), 100.0).unwrap();
    assert!((actual - expected).abs() <= 0.01);
}

#[test]
fn overflowed_exact_point_distance_can_certify_maximum_with_large_error() {
    let left = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let right = Shape::Point(Point::new_raw_xy(f64::MAX, 0.0));
    let scale = 1.0_f64.next_up();
    let maximum = Rational::from_f64(f64::MAX);
    let scale_exact = Rational::from_f64(scale);
    let scaled_squared = maximum
        .try_square()
        .unwrap()
        .try_product(&scale_exact.try_square().unwrap())
        .unwrap();
    let exact_distance = maximum.try_product(&scale_exact).unwrap();
    let true_gap = exact_distance.try_subtract(&maximum).unwrap();
    let error = Rational::from_f64(6e292);
    assert_ne!(
        true_gap.try_cmp(&error).unwrap(),
        Ordering::Greater,
        "the exact point distance gap must fit the chosen error"
    );
    let target = maximum.try_add(&error).unwrap();
    assert_ne!(
        scaled_squared
            .try_cmp(&target.try_square().unwrap())
            .unwrap(),
        Ordering::Greater,
        "the exact squared distance must fit the chosen candidate allowance"
    );
    let interval = SqrtInterval::from_squared_with_error(&scaled_squared, Some(&error)).unwrap();
    let upper = interval
        .upper_rational()
        .expect("finite exact overflow upper");
    let initial_upper = scaled_squared.try_quotient(&maximum).unwrap();
    let initial_gap = initial_upper.try_subtract(&maximum).unwrap();
    assert_eq!(
        true_gap.try_cmp(&error).unwrap(),
        Ordering::Less,
        "this is the between-gaps case, not an exact-boundary tie"
    );
    assert_eq!(
        initial_gap.try_cmp(&error).unwrap(),
        Ordering::Greater,
        "q/MAX must be too loose for the chosen error"
    );
    let small_error = Rational::from_f64(3e292);
    assert_eq!(
        true_gap.try_cmp(&small_error).unwrap(),
        Ordering::Greater,
        "the negative control must be below the exact point-distance gap"
    );
    assert_ne!(
        upper.try_cmp(&initial_upper).unwrap(),
        Ordering::Greater,
        "Newton refinement must not loosen q/MAX"
    );
    assert_ne!(
        upper.try_cmp(&target).unwrap(),
        Ordering::Greater,
        "error-directed Newton refinement must fit MAX + error"
    );
    let sum = sum_upper(&interval, &SqrtInterval::singleton(1.0))
        .unwrap()
        .expect("overflow upper remains finite when summed");
    assert_eq!(
        sum.try_cmp(&upper.try_add(&Rational::one()).unwrap())
            .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        scaled_squared
            .try_cmp(&maximum.try_square().unwrap())
            .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        distance(&left, &right, Some(6e292), scale)
            .unwrap()
            .to_bits(),
        f64::MAX.to_bits()
    );
    assert_eq!(
        distance(&left, &right, None, scale).unwrap().to_bits(),
        f64::MAX.to_bits()
    );
    distance(&left, &right, Some(3e292), scale)
        .expect_err("an allowance below the exact max-to-upper gap must not certify f64::MAX");
}

#[test]
fn filled_interior_and_parallel_plateau_obey_the_error_bound() {
    for vertices in [vec![(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)], vec![
        (0.0, 0.0),
        (100.0, 0.0),
        (100.0, 2.0),
        (0.0, 2.0),
        (0.0, 0.0),
    ]] {
        let value = distance(&polygon(&vertices), &line(&vertices), Some(1e-6), 1.0).unwrap();
        assert!((value - 1.0).abs() <= 1e-6, "{value}");
    }
}

#[test]
fn default_relative_error_is_certified_without_blurring_zero() {
    const DEFAULT_RELATIVE_ERROR: f64 = 1e-9;

    let triangle = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
    let boundary = line(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
    let value = distance(&triangle, &boundary, None, 1.0).unwrap();
    assert!((value - 1.0).abs() <= DEFAULT_RELATIVE_ERROR);

    let diagonal = Shape::Point(Point::new_raw_xy(1.0, 1.0));
    let diagonal_value = distance(
        &Shape::Point(Point::new_raw_xy(0.0, 0.0)),
        &diagonal,
        None,
        1.0,
    )
    .unwrap();
    let expected = 2.0_f64.sqrt();
    assert!((diagonal_value - expected).abs() <= DEFAULT_RELATIVE_ERROR * expected);

    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let close = Shape::Point(Point::new_raw_xy(1e-200, 0.0));
    assert_eq!(distance(&origin, &origin, None, 1.0).unwrap(), 0.0);
    assert!(distance(&origin, &close, None, 1.0).unwrap() > 0.0);
}

#[test]
fn default_allowance_is_bounded_by_the_true_distance() {
    const DEFAULT_RELATIVE_ERROR: f64 = 1e-9;

    let accuracy = Accuracy::new(None, 1.0).unwrap();
    let squared = Rational::from_f64(2.0);
    let candidate = squared.round_sqrt().unwrap();
    let error = accuracy.absolute_error(&squared, candidate).unwrap();
    let relative = Rational::from_f64(DEFAULT_RELATIVE_ERROR);
    let relative_squared = relative
        .try_square()
        .unwrap()
        .try_product(&squared)
        .unwrap();
    assert_ne!(
        error
            .try_square()
            .unwrap()
            .try_cmp(&relative_squared)
            .unwrap(),
        Ordering::Greater,
    );
}

#[test]
fn default_relative_error_accepts_a_valid_minimum_subnormal_result() {
    let minimum_subnormal = f64::from_bits(1);
    let almost_one = 1.0_f64.next_down();
    let small_ordinate = 3.0 * 2.0_f64.powi(-28);
    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let nearby = Shape::Point(Point::new_raw_xy(almost_one, small_ordinate));

    assert_eq!(
        distance(&origin, &nearby, None, minimum_subnormal).unwrap(),
        minimum_subnormal
    );
}

#[test]
fn zero_distinguishes_equal_support_from_close_support() {
    let baseline = line(&[(0.0, 0.0), (2.0, 0.0)]);
    let segmented = line(&[(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)]);
    assert_eq!(
        distance(&baseline, &segmented, Some(1e-3), 1.0).unwrap(),
        0.0
    );
    let nearby = line(&[(0.0, 1e-9), (2.0, 1e-9)]);
    let value = distance(&baseline, &nearby, Some(1e-3), 1.0).unwrap();
    assert!(value > 0.0);
    assert!((value - 1e-9).abs() <= 1e-3);
    let vertices = [(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)];
    let value = distance(&polygon(&vertices), &line(&vertices), Some(2.0), 1.0).unwrap();
    assert!(value > 0.0);
    assert!((value - 1.0).abs() <= 2.0);
}

#[test]
fn identical_cancelled_polygon_support_is_undefined() {
    let ring = crate::geometry::Ring::from_closed(
        CoordSeq::try_from_xy(&[
            XY::new(0.0, 0.0),
            XY::new(1.0, 0.0),
            XY::new(2.0, 0.0),
            XY::new(0.0, 0.0),
        ])
        .unwrap(),
    )
    .unwrap();
    let shape = Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), vec![ring]));
    distance(&shape, &shape, Some(1e-6), 1.0).unwrap_err();
}

#[test]
fn invalid_accuracy_is_rejected_before_identity() {
    let point = Shape::Point(Point::new_raw_xy(3.0, 4.0));
    for error in [0.0, -1.0, f64::INFINITY, f64::NAN] {
        distance(&point, &point, Some(error), 1.0).unwrap_err();
    }
}
