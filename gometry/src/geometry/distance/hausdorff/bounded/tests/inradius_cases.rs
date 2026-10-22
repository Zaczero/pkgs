fn directed_candidate_for(shape: &Shape, tolerance: f64, scale: f64) -> DirectedCandidate {
    let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
    let mut context = SearchContext::new_directed(
        &accuracy,
        Rational::from_f64(scale),
        Rational::from_f64(tolerance),
    );
    let source = PreparedSupport::from_shape_with_boundary(shape, &mut context.budget)
        .unwrap()
        .unwrap();
    let boundary = source.boundary_clone().unwrap();
    run_directed(&source, &boundary, &mut context).unwrap()
}

#[rstest::rstest]
#[case::extent_100_0(100.0)]
#[case::extent_1000_0(1000.0)]
#[case::extent_10_000_0(10_000.0)]
#[case::extent_100_000_0(100_000.0)]
#[case::extent_1_000_000_0(1_000_000.0)]
fn public_inscribed_circle_work_does_not_follow_aspect_ratio(#[case] extent: f64) {
    let shape = polygon(&[(0.0, 0.0), (extent, 1.0), (2.0 * extent, 0.0), (0.0, 0.0)]);
    let _guard = limit_refinement_work(32);
    reset_exact_span_evaluations();
    let circle = crate::geometry::CircleSolution::inscribed(&shape, None, 1.0).unwrap()
        .to_polygon(
            std::num::NonZeroU32::new(16).unwrap(),
            &mut ExpansionBudget::new("maximum_inscribed_circle", "quadrant_segments"),
        ).unwrap();
    assert!(matches!(circle, Shape::Polygon(_)));
    assert!(refinement_work() <= 32, "extent {extent}");
    assert!(exact_span_evaluations() <= 512, "extent {extent}");
}

#[rstest::rstest]
#[case::extent_10_0(10.0)]
#[case::extent_30_0(30.0)]
#[case::extent_100_0(100.0)]
#[case::extent_300_0(300.0)]
#[case::extent_1000_0(1000.0)]
#[case::extent_3000_0(3000.0)]
#[case::extent_10_000_0(10_000.0)]
fn shared_polygon_pole_search_is_bounded_on_thin_nonrectangular_support(#[case] extent: f64) {
    for shape in [
        polygon(&[(0.0, 0.0), (extent, 0.0), (0.0, 1.0), (0.0, 0.0)]),
        polygon(&[
            (0.0, 0.0),
            (extent, 0.0),
            (extent, 0.9),
            (0.0, 1.0),
            (0.0, 0.0),
        ]),
    ] {
        let _guard = limit_refinement_work(32);
        reset_exact_span_evaluations();
        let label = shape.polylabel(Some(0.001)).unwrap();
        let circle = crate::geometry::CircleSolution::inscribed(&shape, Some(0.001), 1.0).unwrap()
            .to_polygon(
                std::num::NonZeroU32::new(16).unwrap(),
                &mut ExpansionBudget::new("maximum_inscribed_circle", "quadrant_segments"),
            ).unwrap();
        assert!(matches!(label, Shape::Point(_)));
        assert!(matches!(circle, Shape::Polygon(_)));
        assert!(exact_span_evaluations() <= 512, "extent {extent}");
    }
}

#[test]
fn directed_inradius_retains_the_rounded_center_and_global_enclosure() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let result = directed_candidate_for(&shape, 1e-6, 1.0);
    assert_eq!(result.proof.witness_squared.round().unwrap(), 4.0);
    assert_eq!((result.center.x, result.center.y), (2.0, 2.0));
    assert_eq!(result.center_squared.round().unwrap(), 4.0);
    assert_eq!(result.proof.certified_squared.round().unwrap(), 4.0);
    assert_ne!(
        result
            .proof
            .upper
            .try_cmp(&Rational::from_f64(2.0 + 1e-6))
            .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        (
            result.proof.witness.x().round().unwrap(),
            result.proof.witness.y().round().unwrap()
        ),
        (2.0, 2.0)
    );
}

#[test]
fn directed_inradius_uses_a_stable_center_on_a_parallel_plateau() {
    let shape = polygon(&[(0.0, 0.0), (8.0, 0.0), (8.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let first = directed_candidate_for(&shape, 1e-9, 1.0);
    let second = directed_candidate_for(&shape, 1e-9, 1.0);
    assert_eq!(
        (first.center.x.to_bits(), first.center.y.to_bits()),
        (second.center.x.to_bits(), second.center.y.to_bits())
    );
    assert_eq!(
        (
            first.proof.witness.x().round().unwrap(),
            first.proof.witness.y().round().unwrap()
        ),
        (
            second.proof.witness.x().round().unwrap(),
            second.proof.witness.y().round().unwrap()
        )
    );
    assert_eq!(first.center.x, 4.0);
    assert_eq!(first.center.y, 2.0);
    assert_eq!(
        first
            .proof
            .upper
            .try_cmp(&Rational::from_f64(2.0 + 1e-9))
            .unwrap(),
        Ordering::Less
    );
}

#[test]
fn directed_inradius_selects_the_first_equal_multipart_winner() {
    let shape = crate::io::parse_wkt(
        "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),\
                       ((10 0,14 0,14 4,10 4,10 0)))",
    )
    .unwrap();
    let result = directed_candidate_for(&shape, 1e-9, 1.0);
    assert!(result.center.x < 4.0 && result.center.y > 0.0 && result.center.y < 4.0);
    assert_eq!(
        result
            .proof
            .upper
            .try_cmp(&Rational::from_f64(2.0 + 1e-9))
            .unwrap(),
        Ordering::Less
    );
}

#[test]
fn directed_inradius_accounts_for_rounding_an_exact_witness_center() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let shape = polygon(&[
        (1.0, 0.0),
        (exact_power_of_two(60), 0.0),
        (exact_power_of_two(60), 4.0),
        (1.0, 4.0),
        (1.0, 0.0),
    ]);
    let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
    let mut context = SearchContext::new_directed(&accuracy, Rational::one(), Rational::one());
    let source = PreparedSupport::from_shape_with_boundary(&shape, &mut context.budget)
        .unwrap()
        .unwrap();
    let target = source.boundary_clone().unwrap();
    let (witness, _) = SourcePatch::Box(source.area_bounds.unwrap())
        .center_and_radius_squared(&source)
        .unwrap();
    context
        .update_lower_with_witness(target.distance_squared(&witness).unwrap(), &witness)
        .unwrap();
    let result = directed_candidate(&source, &target, &Rational::from_f64(2.0), &context)
        .unwrap()
        .unwrap();
    let rounded = ExactPoint::from_xy(result.center.xy());
    assert!(
        result.proof.witness.x().try_cmp(rounded.x()).unwrap() != Ordering::Equal
            || result.proof.witness.y().try_cmp(rounded.y()).unwrap() != Ordering::Equal
    );
    assert_eq!(
        result
            .proof
            .certified_squared
            .try_cmp(&result.center_squared)
            .unwrap(),
        Ordering::Less
    );
}

#[test]
fn directed_inradius_checks_the_final_output_scale_before_stopping() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let scale_value = 0.1;
    let tolerance = 1e-12;
    let result = directed_candidate_for(&shape, tolerance, scale_value);
    let scale = Rational::from_f64(scale_value);
    let radius = downward_sqrt(
        &result
            .center_squared
            .try_product(&scale.try_square().unwrap())
            .unwrap(),
    )
    .unwrap();
    let upper = result.proof.upper.try_product(&scale).unwrap();
    let gap = upper.try_subtract(&Rational::from_f64(radius)).unwrap();
    assert_ne!(
        gap.try_cmp(&Rational::from_f64(tolerance)).unwrap(),
        Ordering::Greater
    );
    assert!(radius <= 0.2);
}

#[test]
fn boundary_clone_keeps_filled_source_separate_from_boundary_target() {
    let ring = |points: &[XY]| {
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap()).unwrap()
    };
    let shape = Shape::Polygon(crate::geometry::Polygon::new(
        ring(&[
            XY::new(0.0, 0.0),
            XY::new(4.0, 0.0),
            XY::new(4.0, 4.0),
            XY::new(0.0, 4.0),
            XY::new(0.0, 0.0),
        ]),
        vec![ring(&[
            XY::new(1.0, 1.0),
            XY::new(3.0, 1.0),
            XY::new(3.0, 3.0),
            XY::new(1.0, 3.0),
            XY::new(1.0, 1.0),
        ])],
    ));
    let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
    let mut context = SearchContext::new(&accuracy);
    let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
    let target = source.boundary_clone().unwrap();
    let center = ExactPoint::from_xy(XY::new(0.5, 0.5));
    assert!(!source.faces.is_empty());
    assert!(target.faces.is_empty());
    assert!(source.distance_squared(&center).unwrap().is_zero());
    assert_eq!(
        target.distance_squared(&center).unwrap().round().unwrap(),
        0.25
    );
}

#[test]
fn retained_clearance_disk_rejects_candidate_enclosing_a_hole() {
    let ring = |points: &[XY]| {
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap()).unwrap()
    };
    let shape = Shape::Polygon(crate::geometry::Polygon::new(
        ring(&[
            XY::new(-100.0, -100.0),
            XY::new(100.0, -100.0),
            XY::new(100.0, 100.0),
            XY::new(-100.0, 100.0),
            XY::new(-100.0, -100.0),
        ]),
        vec![ring(&[
            XY::new(-2.0, 8.0),
            XY::new(2.0, 8.0),
            XY::new(2.0, 10.0),
            XY::new(-2.0, 10.0),
            XY::new(-2.0, 8.0),
        ])],
    ));
    let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
    let mut context = SearchContext::new(&accuracy);
    let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
    let boundary = source.boundary_clone().unwrap();
    let valid = polygon(&[(3.0, 2.0), (2.0, 3.0), (1.0, 2.0), (2.0, 1.0), (3.0, 2.0)]);
    assert!(shape.covers(&valid).unwrap());
    assert!(
        crate::geometry::inscribed_candidate_is_contained(
                Point::new_raw_xy(2.0, 2.0),
                &boundary
                    .distance_squared(&ExactPoint::from_xy(XY::new(2.0, 2.0)))
                    .unwrap(),
                &valid,
            )
            .unwrap()
    );
    let hole_enclosing = polygon(&[
        (12.0, 0.0), (0.0, 12.0), (-12.0, 0.0), (0.0, -12.0), (12.0, 0.0),
    ]);
    hole_enclosing.for_each_point(|point| assert!(shape.covers_point(point)));
    assert!(!shape.covers(&hole_enclosing).unwrap());
    assert!(
        !crate::geometry::inscribed_candidate_is_contained(
                Point::new_raw_xy(0.0, 0.0),
                &boundary
                    .distance_squared(&ExactPoint::from_xy(XY::new(0.0, 0.0)))
                    .unwrap(),
                &hole_enclosing,
            )
            .unwrap()
    );
}

#[test]
fn boundary_target_cancels_shared_edges_of_filled_multipart_support() {
    let shape = crate::io::parse_wkt(
        "MULTIPOLYGON (((0 0,10 0,10 10,0 10,0 0),\
                        (20 20,22 20,22 22,20 22,20 20)),\
                       ((15 15,30 15,30 30,15 30,15 15)))",
    )
    .unwrap();
    let mut budget = ExpansionBudget::new("test", "support");
    let source = PreparedSupport::from_shape_with_boundary(&shape, &mut budget)
        .unwrap()
        .unwrap();
    let target = source.boundary_clone().unwrap();
    assert_eq!(target.spans.len(), 8);
    let result = directed_inradius_scaled(&shape, 1e-9, 1.0)
        .unwrap()
        .unwrap();
    assert_eq!((result.center.x, result.center.y), (22.5, 22.5));
    assert_eq!(downward_sqrt(&result.center_squared).unwrap(), 7.5);
    let center = ExactPoint::from_xy(result.center.xy());
    assert_eq!(
        target
            .distance_squared(&center)
            .unwrap()
            .try_cmp(&result.center_squared)
            .unwrap(),
        Ordering::Equal
    );
    assert!(source.contains_area(&center).unwrap());
}

#[test]
fn boundary_clone_drops_incidence_without_changing_the_solved_disk() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let source = PreparedSupport::from_shape_with_boundary(
        &shape, &mut ExpansionBudget::new("test", "support"),
    ).unwrap().unwrap();
    let boundary = source.boundary_clone().unwrap();
    assert!(
        boundary.boundary_spans.is_none() && boundary.boundary_points.is_none(),
        "boundary clone carries geometry only"
    );
    let circle = crate::geometry::CircleSolution::inscribed(&shape, Some(1e-3), 1.0).unwrap();
    assert_eq!(circle.radius(1.0).unwrap(), 2.0);
    assert_eq!(circle.center().unwrap(), Shape::Point(Point::new_raw_xy(2.0, 2.0)));
    let output = circle.to_polygon(
        std::num::NonZeroU32::new(16).unwrap(),
        &mut ExpansionBudget::new("maximum_inscribed_circle", "quadrant_segments"),
    ).unwrap();
    assert!(shape.covers(&output).unwrap());
}

#[test]
fn directed_inradius_ignores_collapsed_support_inside_positive_area() {
    let outer = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_xy(&[XY::new(1.0, 1.0), XY::new(3.0, 1.0), XY::new(1.0, 1.0)])
                .unwrap(),
        )
        .unwrap(),
    );
    let shape =
        Shape::geometry_collection(vec![outer.clone(), line, Shape::Point(Point::new_raw_xy(2.0, 2.0))])
            .unwrap();
    let result = directed_inradius_scaled(&shape, 1e-9, 1.0)
        .unwrap()
        .unwrap();
    assert_eq!(downward_sqrt(&result.center_squared).unwrap(), 2.0);
    assert!(outer.contains_point(result.center));
}

#[test]
fn directed_inradius_propagates_the_subdivision_budget() {
    let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)]);
    let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
    let mut context = SearchContext::new_directed(&accuracy, Rational::one(), Rational::one());
    let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
    let target = source.boundary_clone().unwrap();
    context.budget.add(GENERATED_ITEM_LIMIT).unwrap();
    let error = run_directed(&source, &target, &mut context).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(
            crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "maximum_inscribed_circle",
                parameter: "tolerance",
                ..
            }
        )
    ));
}

#[test]
fn multipart_area_boxes_and_mixed_points_use_only_source_witnesses() {
    let multipart = crate::io::parse_wkt(
        "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),\
                       ((3 3,5 3,5 5,3 5,3 3)))",
    )
    .unwrap();
    for ((x, y), expected_squared) in [((3.0, 1.0), 20.0), ((5.0, 3.0), 34.0)] {
        let target = Shape::Point(Point::new_raw_xy(x, y));
        let actual = distance(&multipart, &target, Some(1e-12), 1.0).unwrap();
        assert!((actual - f64::sqrt(expected_squared)).abs() <= 1e-12);
    }

    let mixed = Shape::geometry_collection(vec![
        line(&[(0.0, 0.0), (1.0, 0.0)]),
        Shape::Point(Point::new_raw_xy(10.0, 0.0)),
    ])
    .unwrap();
    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    assert!((distance(&mixed, &origin, Some(1e-12), 1.0).unwrap() - 10.0).abs() <= 1e-12);
}

#[test]
fn nested_island_target_fill_is_part_of_the_closed_support() {
    let source = crate::io::parse_wkt(
        "MULTILINESTRING (\
         (0 0,10 0,10 10,0 10,0 0),\
         (2 2,8 2,8 8,2 8,2 2))",
    )
    .unwrap();
    let target = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION (\
         POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,8 2,8 8,2 8,2 2)),\
         POLYGON ((4 4,6 4,6 6,4 6,4 4)))",
    )
    .unwrap();
    assert!((distance(&source, &target, Some(1e-12), 1.0).unwrap() - 3.0).abs() <= 1e-12);
}

#[test]
fn metric_support_consumption_preserves_multipart_faces_and_mixed_points() {
    for (wkt, inside, outside) in [
        (
            "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),\
                           ((2 0,4 0,4 2,2 2,2 0)))",
            [(1.0, 1.0), (3.0, 1.0), (2.0, 1.0)],
            [(5.0, 1.0), (2.0, 3.0), (-1.0, 1.0)],
        ),
        (
            "MULTIPOLYGON (((0 0,3 0,3 3,0 3,0 0)),\
                           ((2 0,5 0,5 3,2 3,2 0)))",
            [(1.0, 1.0), (2.5, 1.0), (4.0, 1.0)],
            [(6.0, 1.0), (2.5, 4.0), (-1.0, 1.0)],
        ),
    ] {
        let shape = crate::io::parse_wkt(wkt).unwrap();
        let mut budget = ExpansionBudget::new("test", "metric support");
        let support = PreparedSupport::from_shape(&shape, &mut budget).unwrap();
        assert!(!support.faces.is_empty(), "{wkt}");
        for (x, y) in inside {
            assert!(
                support
                    .contains_area(&ExactPoint::from_xy(XY::new(x, y)))
                    .unwrap(),
                "expected ({x}, {y}) inside {wkt}"
            );
        }
        for (x, y) in outside {
            assert!(
                !support
                    .contains_area(&ExactPoint::from_xy(XY::new(x, y)))
                    .unwrap(),
                "expected ({x}, {y}) outside {wkt}"
            );
        }
    }

    let shape = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         POINT (10 1))",
    )
    .unwrap();
    let mut budget = ExpansionBudget::new("test", "metric support");
    let support = PreparedSupport::from_shape(&shape, &mut budget).unwrap();
    assert_eq!(support.spans.len(), 4);
    assert_eq!(support.points.len(), 1);
    assert_eq!(support.faces.len(), 1);
    assert_eq!(support.faces[0].len(), 1);
    assert!(
        support
            .contains_area(&ExactPoint::from_xy(XY::new(2.0, 2.0)))
            .unwrap()
    );
    assert!(
        !support
            .contains_area(&ExactPoint::from_xy(XY::new(10.0, 1.0)))
            .unwrap()
    );
    assert!(support.points.iter().any(|point| {
        point
            .same_position(&ExactPoint::from_xy(XY::new(10.0, 1.0)))
            .unwrap()
    }));
}
