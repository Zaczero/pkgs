#[test]
fn minimum_circle_radius_rounds_min_subnormal_support_exactly() {
    let u = f64::from_bits(1);
    let shape = Shape::MultiPoint(
        crate::geometry::MultiPoint::try_new(crate::geometry::CoordinateAxes::XY, vec![
            crate::geometry::MultiMember::Value(Point::new_raw_xy(0.0, 0.0)),
            crate::geometry::MultiMember::Value(Point::new_raw_xy(4.0 * u, 2.0 * u)),
        ])
        .expect("minimum-subnormal pair"),
    );
    assert_eq!(
        shape.minimum_bounding_radius().unwrap().to_bits(),
        2,
        "the pair's exact radius sqrt(5)u rounds to 2u"
    );
}

#[test]
fn disk_enclosure_interval_filter_respects_overlap_and_finiteness() {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let disk = smallest_enclosing_circle(&source)
        .expect("diameter support")
        .certified_disk(&source)
        .expect("source-certified unit disk");
    let coarse = DiskEnclosure {
        x: Interval {
            lo: -0.25,
            hi: 0.25,
        },
        y: Interval {
            lo: -0.25,
            hi: 0.25,
        },
        squared_radius: Interval::exact(1.0),
    };

    // This CCW edge leaves the unit disk outside: its clearance is only 0.9.
    // The interval lower bound overlaps the squared-radius upper bound and
    // therefore cannot certify enclosure.
    let unsafe_edge = (
        Point::new_raw_xy(-2.0, -0.9).xy(),
        Point::new_raw_xy(2.0, -0.9).xy(),
    );
    assert!(
        !coarse.proves_ccw_edge_encloses(unsafe_edge.0, unsafe_edge.1),
        "overlapping interval bounds must decline the unsafe half-plane"
    );
    assert!(
        !disk
            .ccw_edge_encloses(unsafe_edge.0, unsafe_edge.1)
            .expect("exact unsafe-edge decision"),
        "the exact unit disk is not enclosed by the 0.9-clearance edge"
    );

    let safe_edge = (
        Point::new_raw_xy(-2.0, -2.0).xy(),
        Point::new_raw_xy(2.0, -2.0).xy(),
    );
    assert!(coarse.proves_ccw_edge_encloses(safe_edge.0, safe_edge.1));
    assert!(
        disk.ccw_edge_encloses(safe_edge.0, safe_edge.1)
            .expect("exact safe-edge decision")
    );

    let nonfinite = DiskEnclosure {
        x: Interval {
            lo: f64::NEG_INFINITY,
            hi: f64::INFINITY,
        },
        y: Interval {
            lo: -0.25,
            hi: 0.25,
        },
        squared_radius: Interval::exact(1.0),
    };
    assert!(
        !nonfinite.proves_ccw_edge_encloses(safe_edge.0, safe_edge.1),
        "nonfinite enclosure endpoints must decline rather than prove"
    );
}

#[test]
fn minimum_circle_edge_certificate_rejects_an_inscribed_source_passing_ring() {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    let mut budget = ExpansionBudget::new("minimum_bounding_circle", "realization");
    let output = constructive::point_buffer(
        disk.center().expect("finite center"),
        1.0 + 1.0e-6,
        NonZeroU32::new(16).expect("non-zero"),
        &mut budget,
    )
    .expect("inscribed ring construction");
    let Shape::Polygon(polygon) = &output else {
        panic!("point buffer must emit a polygon");
    };
    let shell = polygon.rings().next().expect("shell");
    let vertices: Vec<_> = shell
        .coords()
        .iter_coords()
        .map(|point| point.xy())
        .collect();
    let open = &vertices[..vertices.len() - 1];
    assert!(
        open.iter().enumerate().all(|(index, &left)| {
            let right = open[(index + 1) % open.len()];
            source
                .iter()
                .all(|point| exact::orient2d(left, right, point.xy()) != ExactSign::Negative)
        }),
        "the old source-point-only check accepts this ring"
    );
    assert!(
        certify_minimum_circle_polygon(&output, &disk, NonZeroU32::new(16).expect("non-zero"))
            .is_err(),
        "the exact disk edge check must reject its inward chords"
    );
}

#[test]
fn minimum_circle_certificate_rejects_an_inward_vertex_mutation() {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    let valid = emit_minimum_bounding_circle(
        &disk,
        NonZeroU32::new(16).expect("non-zero"),
        &mut realization_budget(),
    )
    .expect("valid ring");
    let Shape::Polygon(polygon) = valid else {
        panic!("minimum circle must emit a polygon");
    };
    let shell = polygon.rings().next().expect("shell");
    let mut vertices: Vec<_> = shell.coords().iter_coords().collect();
    vertices[0] = Point::new_raw_xy(0.5, 0.0);
    let mutated = Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&vertices).expect("mutated ring")).unwrap(),
        Vec::new(),
    ));
    assert!(
        certify_minimum_circle_polygon(&mutated, &disk, NonZeroU32::new(16).expect("non-zero"))
            .is_err(),
        "an inward vertex must fail the exact disk certificate"
    );
}

#[test]
fn minimum_circle_certificate_does_not_assume_radius_denominator_is_center_weight_squared() {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    let two = ExactDyadic::from_f64(2.0);
    let equivalent = CertifiedDisk {
        center_x: disk.center_x.clone(),
        center_y: disk.center_y.clone(),
        center_w: disk.center_w.clone(),
        radius_squared_numerator: disk
            .radius_squared_numerator
            .try_product(&two)
            .expect("scaled numerator"),
        radius_squared_denominator: disk
            .radius_squared_denominator
            .try_product(&two)
            .expect("scaled denominator"),
        source_center: disk.source_center,
        exact_zero_radius: disk.exact_zero_radius,
    };
    let quadrants = NonZeroU32::new(16).expect("non-zero");
    let mut budget = ExpansionBudget::new("minimum_bounding_circle", "realization");
    let output = tangent_circle_polygon(
        equivalent.center().expect("finite center"),
        equivalent.radius().expect("finite radius"),
        quadrants,
        &mut budget,
    )
    .expect("equivalent disk polygon");
    certify_minimum_circle_polygon(&output, &equivalent, quadrants)
        .expect("equivalent S/T representation must certify");
}

#[rstest::rstest]
#[case::quadrants_1(1)]
#[case::quadrants_2(2)]
#[case::quadrants_16(16)]
fn minimum_circle_emission_keeps_the_requested_quadrant_count(#[case] quadrants: u32) {
    let source = [Point::new_raw_xy(-1.0, 0.0), Point::new_raw_xy(1.0, 0.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");

    let output = emit_minimum_bounding_circle(
        &disk,
        NonZeroU32::new(quadrants).expect("non-zero"),
        &mut realization_budget(),
    )
    .expect("representable tangent polygon");
    let Shape::Polygon(polygon) = output else {
        panic!("minimum circle must emit a polygon");
    };
    assert_eq!(
        polygon.rings().next().expect("shell").coords().len(),
        (quadrants as usize).wrapping_mul(4).wrapping_add(1)
    );
}

#[test]
fn minimum_circle_emission_retries_when_rounded_radius_is_below_tangent() {
    let source = [Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 5.0)];
    let circle = smallest_enclosing_circle(&source).expect("diameter support");
    let disk = circle.certified_disk(&source).expect("exact disk");
    let quadrants = NonZeroU32::new(16).expect("non-zero");
    let mut budget = ExpansionBudget::new("minimum_bounding_circle", "realization");
    let candidate = tangent_circle_polygon(
        disk.center().expect("finite center"),
        disk.radius().expect("finite radius"),
        quadrants,
        &mut budget,
    )
    .expect("candidate polygon");
    assert!(
        certify_minimum_circle_polygon(&candidate, &disk, quadrants).is_err(),
        "the rounded-down radius must expose an inward tangent edge"
    );
    let emitted = emit_minimum_bounding_circle(&disk, quadrants, &mut realization_budget())
        .expect("outward radius retry");
    certify_minimum_circle_polygon(&emitted, &disk, quadrants)
        .expect("retry must publish a certified ring");
}

#[test]
fn minimum_circle_polygon_admits_the_complete_requested_ring_before_emission() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let points = [
        Point::new_raw_xy(exact_power_of_two(60), exact_power_of_two(60)),
        Point::new_raw_xy(exact_power_of_two(60) + 1024.0, exact_power_of_two(60)),
        Point::new_raw_xy(
            exact_power_of_two(60) + 256.0,
            exact_power_of_two(60) + 768.0,
        ),
    ];
    let disk = smallest_enclosing_circle(&points)
        .expect("three sites")
        .certified_disk(&points)
        .expect("exact disk");
    let quadrants = NonZeroU32::new(16).expect("non-zero");
    let mut budget = realization_budget();
    budget
        .add(crate::geometry::GENERATED_ITEM_LIMIT - 4 * 16)
        .expect("one vertex less than the requested ring");
    let error = emit_minimum_bounding_circle(&disk, quadrants, &mut budget).unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "minimum_bounding_circle",
                parameter: "realization",
                ..
            }
        )
    ));
}

#[test]
fn minimum_circle_polygon_maps_nonfinite_source_to_unrepresentable_output() {
    let error = Shape::Point(Point::new_raw_xy(f64::INFINITY, 0.0))
        .minimum_bounding_circle()
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(crate::geometry::GeometryErrorKind::UnrepresentableOutput {
            operation: "minimum_bounding_circle",
            ..
        })
    ));
}

#[rstest::rstest]
#[case::acute_triangle(
    vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(4.0, 0.0),
        Point::new_raw_xy(1.0, 3.0),
    ],
    vec![XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(1.0, 3.0)],
)]
#[case::obtuse_triangle(
    vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(4.0, 0.0),
        Point::new_raw_xy(1.0, 0.5),
    ],
    vec![XY::new(0.0, 0.0), XY::new(4.0, 0.0)],
)]
#[case::collinear(
    vec![
        Point::new_raw_xy(-3.0, -3.0),
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(4.0, 4.0),
        Point::new_raw_xy(1.0, 1.0),
    ],
    vec![XY::new(-3.0, -3.0), XY::new(4.0, 4.0)],
)]
fn minimum_circle_support_matches_independent_boundary_cases(
    #[case] points: Vec<Point>,
    #[case] mut expected: Vec<XY>,
) {
    let circle = smallest_enclosing_circle(&points).unwrap();
    let (support, count) = circle.support_xy(&points);
    let mut actual = support[..count].to_vec();
    actual.sort_unstable_by(|left, right| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
    });
    expected.sort_unstable_by(|left, right| {
        left.x
            .total_cmp(&right.x)
            .then_with(|| left.y.total_cmp(&right.y))
    });
    assert_eq!(actual, expected);
}

#[test]
fn minimum_circle_work_counters_cover_normal_and_inverse_fixed_shuffle_orders() {
    // These thresholds characterize this fixed parabola fixture: they
    // document the old fixed-shuffle adversary without making a universal
    // speed claim about the randomized algorithm.
    let length = 64;
    let source: Vec<_> = (0..length)
        .map(|index| {
            let x = index as f64;
            Point::new_raw_xy(x, x * x)
        })
        .collect();
    let ordinary_order = shuffle_with_seed(length, 0xD1B5_4A32_D192_ED03);
    let mut ordinary_work = CircleWork::default();
    let ordinary =
        solve_enclosing_circle_order(&source, &ordinary_order, &mut ordinary_work).unwrap();

    let permutation = fixed_shuffle(length);
    let mut inverse_source = vec![Point::new_raw_xy(0.0, 0.0); length];
    for (after, &before) in permutation.iter().enumerate() {
        inverse_source[before] = source[after];
    }
    let mut adversarial_work = CircleWork::default();
    let adversarial =
        solve_enclosing_circle_order(&inverse_source, &permutation, &mut adversarial_work).unwrap();

    assert_eq!(
        ordinary
            .certified_disk(&source)
            .unwrap()
            .radius()
            .unwrap()
            .to_bits(),
        adversarial
            .certified_disk(&inverse_source)
            .unwrap()
            .radius()
            .unwrap()
            .to_bits()
    );
    assert!(ordinary_work.contains > 0);
    assert!(adversarial_work.contains > 0);
    assert!(ordinary_work.contains < length * length / 4);
    assert!(adversarial_work.contains > length * length / 4);
    assert!(ordinary_work.one_supports > 0);
    assert!(ordinary_work.two_supports > 0);

    let three_source = [
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(4.0, 0.0),
        Point::new_raw_xy(1.0, 3.0),
    ];
    let mut three_work = CircleWork::default();
    solve_enclosing_circle_order(&three_source, &[0, 1, 2], &mut three_work).unwrap();
    assert!(three_work.three_supports > 0);
}
