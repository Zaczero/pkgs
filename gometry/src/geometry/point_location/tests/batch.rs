#[test]
fn generic_point_tester_uses_exact_source_incidence() {
    let line = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(2.0, 2.0)]).unwrap(),
    ));
    let tester = PointBatchTester::new(&line).unwrap();

    assert!(tester.covers_point(Point::new_raw_xy(1.0, 1.0)));
    assert!(tester.covers_point(Point::new_raw_xy(0.0, 0.0)));
    assert!(!tester.covers_point(Point::new_raw_xy(1.0, 1.1)));
    assert!(!tester.contains_point(Point::new_raw_xy(0.0, 0.0)));
    assert!(tester.contains_point(Point::new_raw_xy(1.0, 1.0)));
}

#[rstest::rstest]
#[case::polygon_0_0_4_0_4_4_0_4_0_0_4_0_4_4_0_4_0_0(
    "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))",
    Point::new_raw_xy(2.0, 2.0),
    true
)]
#[case::polygon_0_0_2_0_1_0_0_0("POLYGON ((0 0,2 0,1 0,0 0))", Point::new_raw_xy(1.0, 0.0), true)]
#[case::polygon_0_0_4_0_4_4_0_4_0_0_8_8_9_8_9_9_8_9_8_8(
    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))",
    Point::new_raw_xy(2.0, 2.0),
    true
)]
fn indexed_point_tester_uses_composed_source_support(
    #[case] wkt: &'static str,
    #[case] point: Point,
    #[case] contains: bool,
) {
    let shape = crate::io::parse_wkt(wkt).unwrap();
    let tester = PointBatchTester::new(&shape).unwrap();
    assert_eq!(tester.contains_point(point), contains);
    assert_eq!(tester.covers_point(point), contains);
}

/// The tester's location must agree with the exact source scan on every
/// probe; its indexed off-boundary answer defers on every edge contact.
#[test]
fn indexed_locations_agree_with_the_exact_scan() {
    use crate::geometry::Coordinates as _;

    for wkt in [
        "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))",
        "POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))",
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 1,1 1,1 1))",
        "POLYGON ((0 0,4 4,4 0,0 4,0 0))",
        "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),((2 2,4 2,4 4,2 4,2 2)))",
        "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((1 1,3 1,3 3,1 3,1 1)))",
        "MULTIPOLYGON (((0 0,2 0,2 4,0 4,0 0)),((2 0,4 0,4 4,2 4,2 0)))",
        "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0),(0.5 0.5,1 0.5,1 1,0.5 1,0.5 0.5)),((2 0,4 0,4 4,2 4,2 0)))",
    ] {
        let shape = crate::io::parse_wkt(wkt).unwrap();
        let tester = PointBatchTester::new(&shape).unwrap();
        for (x, y) in [
            (-0.5, -0.5),
            (0.0, 0.0),
            (0.5, 0.5),
            (0.75, 0.75),
            (1.0, 1.0),
            (1.0, 2.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (2.0, 3.0),
            (2.0, 4.0),
            (3.0, 1.0),
            (3.5, 3.5),
            (4.0, 2.0),
            (4.0, 4.0),
            (4.5, 4.5),
            (-1.0, 0.0),
        ] {
            let point = Point::new_raw_xy(x, y);
            let exact = cartesian_point_location_and_area(&shape, point);
            assert_eq!(tester.location(point), exact.location, "{wkt} at {point:?}");
            assert_eq!(
                tester.location_and_area(point),
                exact,
                "area for {wkt} at {point:?}"
            );
            if let Some(location) = tester.location_off_boundary(point.x, point.y) {
                assert_eq!(location, exact.location, "{wkt} at {point:?}");
            } else {
                let mut on_edge = false;
                shape.for_each_segment_chain(|chain| {
                    on_edge |= chain
                        .segment_pairs()
                        .any(|[start, end]| crate::geometry::point_on_segment(point, start, end));
                });
                assert!(
                    on_edge || wkt.contains("1 1,1 1"),
                    "{wkt} deferred an off-boundary probe at {point:?}"
                );
            }
        }
    }
}

#[test]
fn indexed_rational_locations_match_streaming_labels_and_area() {
    let shape = dense_square();
    let tester = PointBatchTester::new(&shape).unwrap();
    assert!(tester.uses_indexed_plan());

    for point in [
        [
            ExactDyadic::from_f64(1.0),
            ExactDyadic::from_f64(1.0),
            ExactDyadic::from_f64(3.0),
        ],
        [
            ExactDyadic::from_f64(1.0),
            ExactDyadic::from_f64(0.0),
            ExactDyadic::from_f64(3.0),
        ],
        [
            ExactDyadic::from_f64(20.0),
            ExactDyadic::from_f64(10.0),
            ExactDyadic::from_f64(2.0),
        ],
        [
            ExactDyadic::from_f64(22.0),
            ExactDyadic::from_f64(10.0),
            ExactDyadic::from_f64(2.0),
        ],
    ] {
        let expected = cartesian_exact_point_location_and_area(&shape, &point).unwrap();
        assert_eq!(
            tester
                .location_and_area_query(ExactProbe::new(&point).source_query().unwrap())
                .unwrap(),
            expected
        );
        assert_eq!(
            cartesian_exact_point_location(&shape, &point).unwrap(),
            expected.location
        );
    }

    let point = [
        ExactDyadic::from_f64(1.0),
        ExactDyadic::from_f64(1.0),
        ExactDyadic::from_f64(3.0),
    ];
    let expected = cartesian_exact_point_location_and_area(&shape, &point).unwrap();
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    for _ in 0..4 {
        assert_eq!(plan.locate_exact(&point).unwrap(), expected);
    }
    assert!(plan.prepare_point_queries(4).unwrap().uses_indexed_plan());
}

#[test]
fn exact_offsets_use_indexed_edges_and_preserve_tangency() {
    let shape = dense_square();
    let point = [
        ExactDyadic::from_f64(1.0),
        ExactDyadic::from_f64(0.0),
        ExactDyadic::from_f64(3.0),
    ];
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    for _ in 0..4 {
        let _ = plan.locate_exact(&point).unwrap();
    }

    for (direction, expected_toward) in [
        (
            [ExactDyadic::from_f64(0.0), ExactDyadic::from_f64(1.0)],
            OccupiedToward::Yes,
        ),
        (
            [ExactDyadic::from_f64(0.0), ExactDyadic::from_f64(-1.0)],
            OccupiedToward::No,
        ),
        (
            [ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(0.0)],
            OccupiedToward::Tangent,
        ),
    ] {
        let indexed = plan.locate_exact_offset(&point, &direction).unwrap();
        let streaming =
            cartesian_exact_offset_location_and_area(&shape, &point, &direction).unwrap();
        assert_eq!(indexed, streaming);
        assert_eq!(
            cartesian_exact_offset_location(&shape, &point, &direction).unwrap(),
            streaming.location
        );
        assert_eq!(
            plan.occupied_toward_exact(&point, &direction).unwrap(),
            expected_toward
        );
    }
}

#[test]
fn exact_probe_locations_preserve_rationals_zero_and_subnormal_neighbours() {
    let shape = dense_square();
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    let rings = plan.prepare_point_queries(4).unwrap();
    assert!(rings.uses_indexed_plan());
    let subnormal = f64::from_bits(1);
    for (x, y, weight, location, area) in [
        (1.0, 1.0, 3.0, Loc::Interior, true),
        (1.0, 0.0, 3.0, Loc::Boundary, true),
        (-1.0, -1.0, -3.0, Loc::Interior, true),
        (30.0, 30.0, 3.0, Loc::Boundary, true),
        (-0.0, 0.0, 3.0, Loc::Boundary, true),
        (subnormal, 1.0, 3.0, Loc::Interior, true),
        (-subnormal, 1.0, 3.0, Loc::Exterior, false),
    ] {
        let point = [
            ExactDyadic::from_f64(x),
            ExactDyadic::from_f64(y),
            ExactDyadic::from_f64(weight),
        ];
        let probe = ExactProbe::new(&point);
        let expected = SourcePointLocation { location, area };
        assert_eq!(rings.locate_probe(&probe).unwrap(), expected);
        assert_eq!(
            cartesian_row_probe_location_and_area((&shape).into(), &probe,).unwrap(),
            expected,
        );
    }
}

#[test]
#[cfg(debug_assertions)]
fn homogeneous_slab_queries_do_not_admit_source_filters() {
    use crate::test_support::exact_operation_counts::measure;

    let mut members = vec![dense_square()];
    members.extend((0..32).map(|i| Shape::Point(Point::new_raw_xy(100.0 + f64::from(i), 100.0))));
    let shape = Shape::geometry_collection(members).unwrap();
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    let queries = plan.prepare_point_queries(usize::MAX).unwrap();
    assert!(queries.uses_slabs());
    let point = [
        ExactDyadic::one(),
        ExactDyadic::one(),
        ExactDyadic::from_f64(3.0),
    ];
    let probe = ExactProbe::new(&point);
    let direction = [ExactDyadic::one(), ExactDyadic::zero()];
    let ((), counts) = measure(|| {
        assert_eq!(queries.locate_probe(&probe).unwrap(), SourcePointLocation {
            location: Loc::Interior,
            area: true,
        });
        assert_eq!(
            queries.locate_probe_offset(&probe, &direction).unwrap(),
            SourcePointLocation {
                location: Loc::Interior,
                area: true,
            }
        );
    });
    assert_eq!(counts.ratio_roundings, 0);
    let (_, admission) = measure(|| probe.source_query().unwrap());
    assert_eq!(admission.ratio_roundings, 2);

    let outside = [
        ExactDyadic::from_f64(f64::MAX)
            .try_product(&ExactDyadic::from_f64(2.0))
            .unwrap(),
        ExactDyadic::one(),
        ExactDyadic::one(),
    ];
    let outside = ExactProbe::new(&outside);
    assert_eq!(
        queries.locate_probe(&outside).unwrap(),
        SourcePointLocation {
            location: Loc::Exterior,
            area: false,
        }
    );
    assert!(outside.source_query().is_err());
}

#[test]
fn exact_slabs_keep_twice_wound_nonzero_fill() {
    assert_exact_location_matches_source(
        "POLYGON ((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))",
        &[exact_xy(2.0, 2.0), exact_xy(0.0, 2.0), exact_xy(5.0, 2.0)],
        &[exact_direction(1.0, 0.0), exact_direction(0.0, 0.0)],
    );
}

#[test]
fn exact_slabs_union_oppositely_oriented_overlapping_members() {
    assert_exact_location_matches_source(
        "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),\
             ((2 0,2 4,6 4,6 0,2 0)))",
        &[exact_xy(1.0, 2.0), exact_xy(3.0, 2.0), exact_xy(5.0, 2.0)],
        &[exact_direction(1.0, 0.0), exact_direction(0.0, 1.0)],
    );
}

#[test]
fn exact_slabs_preserve_cancelled_spur_and_shell_hole_cancellation() {
    assert_exact_location_matches_source(
        "POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))",
        &[exact_xy(2.0, 2.0), exact_xy(-1.0, 0.0), exact_xy(0.0, 0.0)],
        &[exact_direction(0.0, 1.0), exact_direction(1.0, 0.0)],
    );
    for wkt in [
        "POLYGON ((0 0,4 0,4 4,0 4,0 0,0 4,4 4,4 0,0 0))",
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
        "POLYGON ((0 0,0 4,4 4,4 0,0 0),(0 0,0 4,4 4,4 0,0 0))",
    ] {
        assert_exact_location_matches_source(
            wkt,
            &[exact_xy(2.0, 2.0), exact_xy(0.0, 2.0), exact_xy(5.0, 2.0)],
            &[exact_direction(1.0, 0.0), exact_direction(0.0, 1.0)],
        );
    }
}

#[test]
fn exact_slabs_preserve_hole_interior_and_boundary_labels() {
    assert_exact_location_matches_source(
        "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,2 8,8 8,8 2,2 2))",
        &[
            exact_xy(1.0, 1.0),
            exact_xy(5.0, 5.0),
            exact_xy(2.0, 5.0),
            exact_xy(12.0, 5.0),
        ],
        &[
            exact_direction(1.0, 0.0),
            exact_direction(0.0, 1.0),
            exact_direction(-1.0, 0.0),
        ],
    );
}

#[test]
fn exact_slabs_keep_an_adjacent_multipolygon_shared_edge_interior() {
    let shape = crate::io::parse_wkt(
        "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),\
             ((2 0,4 0,4 2,2 2,2 0)))",
    )
    .unwrap();
    let query = exact_xy(2.0, 1.0);
    let (source, area) = exact_location_indexes(&shape);
    let expected = cartesian_exact_point_location_and_area(&shape, &query).unwrap();
    assert_eq!(expected, SourcePointLocation {
        location: Loc::Interior,
        area: true
    });
    assert_eq!(source.locate_exact(&query).unwrap(), expected);
    assert!(area.unwrap().locate_exact(&query).unwrap());
}

#[test]
fn exact_slabs_classify_crossing_nodes_horizontal_atoms_and_offsets() {
    assert_exact_location_matches_source(
        "POLYGON ((0 0,4 4,0 4,4 0,0 0))",
        &[
            exact_xy(2.0, 2.0),
            exact_xy(1.0, 3.0),
            exact_xy(3.0, 1.0),
            exact_xy(2.0, 0.0),
            exact_xy(2.0, 4.0),
        ],
        &[
            exact_direction(1.0, 0.0),
            exact_direction(0.0, 1.0),
            exact_direction(-1.0, 0.0),
            exact_direction(0.0, 0.0),
        ],
    );
}

#[test]
fn exact_slabs_locate_a_constructed_nondyadic_crossing_node() {
    let shape = crate::io::parse_wkt("POLYGON ((0 0,3 1,0 1,2 0,0 0))").unwrap();
    let query = [
        ExactDyadic::from_f64(6.0),
        ExactDyadic::from_f64(2.0),
        ExactDyadic::from_f64(5.0),
    ];
    let (source, _) = exact_location_indexes(&shape);
    let expected =
        cartesian_exact_point_location_and_area(&shape, &query).expect("exact crossing node");
    assert_eq!(expected.location, Loc::Boundary);
    assert_eq!(source.locate_exact(&query).unwrap(), expected);
    for direction in [
        exact_direction(1.0, 0.0),
        exact_direction(0.0, 1.0),
        exact_direction(-1.0, 0.0),
        exact_direction(0.0, -1.0),
    ] {
        assert_eq!(
            source.locate_exact_offset(&query, &direction).unwrap(),
            cartesian_exact_offset_location_and_area(&shape, &query, &direction).unwrap()
        );
    }
}

#[test]
fn exact_slabs_keep_collapsed_polygon_support_out_of_area() {
    assert_exact_location_matches_source(
        "POLYGON ((0 0,2 0,1 0,0 0))",
        &[exact_xy(1.0, 0.0), exact_xy(0.0, 0.0), exact_xy(3.0, 0.0)],
        &[
            exact_direction(1.0, 0.0),
            exact_direction(0.0, 1.0),
            exact_direction(0.0, 0.0),
        ],
    );
}

#[test]
fn exact_slabs_offset_isolated_nodes_into_their_host_face() {
    assert_exact_location_matches_source("POINT (1 1)", &[exact_xy(1.0, 1.0)], &[
        exact_direction(1.0, 0.0),
        exact_direction(0.0, 0.0),
    ]);
    assert_exact_location_matches_source(
        "GEOMETRYCOLLECTION (POINT (1 1),\
             POLYGON ((0 0,4 0,4 4,0 4,0 0)))",
        &[exact_xy(1.0, 1.0)],
        &[exact_direction(1.0, 0.0), exact_direction(0.0, 0.0)],
    );
}

#[test]
fn exact_homogeneous_boundary_queries_are_not_rounded() {
    let wkt = "POLYGON ((0 0,3 1,0 2,0 0))";
    let point = [
        ExactDyadic::from_f64(3.0),
        ExactDyadic::from_f64(1.0),
        ExactDyadic::from_f64(6.0),
    ];
    let negative_weight = [
        ExactDyadic::from_f64(-3.0),
        ExactDyadic::from_f64(-1.0),
        ExactDyadic::from_f64(-6.0),
    ];
    let shape = crate::io::parse_wkt(wkt).unwrap();
    let (source, area) = exact_location_indexes(&shape);
    for query in [&point, &negative_weight] {
        let expected = cartesian_exact_point_location_and_area(&shape, query).unwrap();
        assert_eq!(expected.location, Loc::Boundary);
        assert_eq!(source.locate_exact(query).unwrap(), expected);
        assert_eq!(
            area.as_ref().unwrap().locate_exact(query).unwrap(),
            expected.area
        );
        for direction in [
            exact_direction(0.0, 1.0),
            exact_direction(0.0, -1.0),
            exact_direction(1.0, 0.0),
            exact_direction(-1.0, 0.0),
        ] {
            assert_eq!(
                source.locate_exact_offset(query, &direction).unwrap(),
                cartesian_exact_offset_location_and_area(&shape, query, &direction).unwrap()
            );
        }
    }
    let rounded = [
        ExactDyadic::from_f64(0.5),
        ExactDyadic::from_f64(1.0 / 6.0),
        ExactDyadic::one(),
    ];
    assert_eq!(
        source.locate_exact(&rounded).unwrap().location,
        Loc::Exterior
    );
}

#[test]
fn prepared_low_ply_contacts_stay_on_bounded_ring_queries() {
    let shape = dense_square();
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    let queries = plan.prepare_point_queries(usize::MAX).unwrap();
    assert!(!queries.uses_slabs());
    assert!(queries.uses_indexed_plan());
    for point in [exact_xy(3.0, 0.0), exact_xy(10.0, 10.0), exact_xy(5.0, 5.0)] {
        assert_eq!(
            queries.locate_exact(&point).unwrap(),
            cartesian_exact_point_location_and_area(&shape, &point).unwrap()
        );
        for direction in [
            exact_direction(1.0, 0.0),
            exact_direction(0.0, 1.0),
            exact_direction(0.0, -1.0),
        ] {
            assert_eq!(
                queries.locate_exact_offset(&point, &direction).unwrap(),
                cartesian_exact_offset_location_and_area(&shape, &point, &direction).unwrap()
            );
        }
    }
}

#[test]
fn prepared_shared_collection_locations_preserve_endpoint_occurrence_parity() {
    let line = crate::io::parse_wkt(
        "LINESTRING (0 0,1 0,2 0,3 0,4 0,5 0,6 0,7 0,8 0,9 0,10 0,11 0,12 0,13 0,14 0,15 0,16 0)",
    )
    .unwrap();
    let mut shared = line.clone();
    for _ in 0..2 {
        shared = Shape::geometry_collection(vec![shared.clone(), shared]).unwrap();
    }
    assert!(shared.has_repeated_shared_owner());
    let plan = crate::geometry::PreparedPlan::transient(&shared);
    let queries = plan.prepare_point_queries(usize::MAX).unwrap();
    assert!(!queries.uses_slabs());
    let endpoint = exact_xy(0.0, 0.0);
    assert_eq!(
        cartesian_exact_point_location_and_area(&line, &endpoint)
            .unwrap()
            .location,
        Loc::Boundary,
    );
    let expected = SourcePointLocation {
        location: Loc::Interior,
        area: false,
    };
    assert_eq!(queries.locate_exact(&endpoint).unwrap(), expected);
    assert_eq!(
        queries.locate_stored(Point::new_raw_xy(0.0, 0.0)).unwrap(),
        expected
    );
    for point in [endpoint, [
        ExactDyadic::from_f64(1.0),
        ExactDyadic::zero(),
        ExactDyadic::from_f64(3.0),
    ]] {
        assert_eq!(
            queries.locate_exact(&point).unwrap(),
            cartesian_exact_point_location_and_area(&shared, &point).unwrap()
        );
        for direction in [
            exact_direction(1.0, 0.0),
            exact_direction(-1.0, 0.0),
            exact_direction(0.0, 1.0),
        ] {
            assert_eq!(
                queries.locate_exact_offset(&point, &direction).unwrap(),
                cartesian_exact_offset_location_and_area(&shared, &point, &direction).unwrap()
            );
        }
    }
}

#[test]
fn prepared_contact_certificate_includes_the_hole_hierarchy() {
    let shell = closed_ring(&[
        (0.0, 0.0),
        (200.0, 0.0),
        (200.0, 4.0),
        (0.0, 4.0),
        (0.0, 0.0),
    ]);
    let holes = (0..8)
        .map(|index| {
            let x = 2.0 + 3.0 * f64::from(index);
            closed_ring(&[(x, 1.0), (x + 1.0, 1.0), (x + 1.0, 3.0), (x, 3.0), (x, 1.0)])
        })
        .collect();
    let shape = Shape::geometry_collection(vec![
        Shape::Polygon(Polygon::new(shell, holes)),
        Shape::Point(Point::new_raw_xy(300.0, 0.0)),
    ])
    .unwrap();
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    let queries = plan.prepare_point_queries(usize::MAX).unwrap();
    assert!(queries.uses_slabs());
    for point in [exact_xy(2.0, 2.0), exact_xy(2.5, 2.0), exact_xy(1.0, 2.0)] {
        assert_eq!(
            queries.locate_exact(&point).unwrap(),
            cartesian_exact_point_location_and_area(&shape, &point).unwrap()
        );
    }
}

#[test]
fn query_forecasts_do_not_credit_demand_before_ring_work() {
    let shape = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(
            &(0..17)
                .map(|x| XY::new(f64::from(x), 1.0))
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    ));
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    let queries = plan.prepare_point_queries(127).unwrap();
    assert!(!queries.uses_slabs());
    // Forecasts guide this resolve only; they are not completed ring queries.
    assert!(!plan.prepare_point_queries(127).unwrap().uses_slabs());
    for _ in 0..256 {
        assert_eq!(
            queries.location(Point::new_raw_xy(0.0, 0.0)).unwrap(),
            Loc::Exterior
        );
    }
    let promoted = plan.prepare_point_queries(0).unwrap();
    assert!(promoted.uses_slabs());
    assert!(!queries.uses_slabs());
    assert_eq!(
        queries.location(Point::new_raw_xy(0.0, 0.0)).unwrap(),
        Loc::Exterior,
        "an admitted ring handle remains valid after later resolves promote"
    );
    assert_eq!(
        promoted.location(Point::new_raw_xy(0.0, 0.0)).unwrap(),
        Loc::Exterior
    );
}

#[test]
fn warm_scalar_membership_promotes_to_reused_slabs() {
    let shape = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(
            &(0..17)
                .map(|x| XY::new(f64::from(x), 1.0))
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    ));
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    let point = Point::new_raw_xy(8.5, 1.0);
    for _ in 0..256 {
        assert!(
            plan.cartesian_point_membership(point, PointMembership::Strict)
                .unwrap()
        );
    }
    assert!(plan.prepare_point_queries(0).unwrap().uses_slabs());
    for membership in [PointMembership::Strict, PointMembership::BoundaryInclusive] {
        assert!(plan.cartesian_point_membership(point, membership).unwrap());
    }
    assert!(plan.prepare_point_queries(0).unwrap().uses_slabs());
}

#[test]
#[cfg(debug_assertions)]
fn packed_comb_batches_use_logarithmic_slab_queries() {
    use crate::crs::frame::Frame;
    use crate::geometry::storage::ShapeRow;
    use crate::predicates::engine::{Predicate, scalar_vs_shapes};
    let points = crate::PyGeometryArray::from_shapes(
        vec![Shape::Point(Point::new_raw_xy(0.0, 0.0)); 8],
        Frame::default(),
    )
    .unwrap();
    let mut work = Vec::new();
    for teeth in [16, 32, 64] {
        let shape = Shape::geometry_collection(vec![
            sheared_comb(teeth),
            Shape::Point(Point::new_raw_xy(1000.0, 1000.0)),
        ])
        .unwrap();
        let plan = crate::geometry::PreparedPlan::transient(&shape);
        plan.prepare_point_queries(0x0001_0000).unwrap();
        reset_query_comparisons();
        let actual = scalar_vs_shapes(
            &Predicate::Contains.spec(),
            &plan,
            (0..8).map(|index| (index, Some(ShapeRow::Point(Point::new_raw_xy(0.0, 0.0))))),
            true,
            Some(&points),
            crate::geometry::TopologyDomain::Cartesian,
            &crate::geometry::FrameDependentCaches::default(),
        )
        .unwrap();
        assert_eq!(actual, vec![false; 8]);
        assert!(plan.prepare_point_queries(0).unwrap().uses_slabs());
        let comparisons = query_comparisons();
        assert!(comparisons > 0);
        assert!(
            comparisons < 8 * 64,
            "teeth={teeth}, comparisons={comparisons}"
        );
        work.push(comparisons);
    }
    assert!(work[2] < 3 * work[0], "fixed-query comb work: {work:?}");
}

#[test]
fn prepared_location_schedule_keeps_small_polygons_streaming() {
    let shape = Shape::Polygon(unit_square());
    let point = [
        ExactDyadic::from_f64(0.5),
        ExactDyadic::from_f64(0.5),
        ExactDyadic::one(),
    ];
    let plan = crate::geometry::PreparedPlan::transient(&shape);
    for _ in 0..4 {
        assert_eq!(plan.locate_exact(&point).unwrap().location, Loc::Interior);
    }
    assert!(!plan.prepare_point_queries(4).unwrap().uses_indexed_plan());
}
