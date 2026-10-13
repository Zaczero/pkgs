    #[test]
    #[cfg(debug_assertions)]
fn disjoint_nonpoint_pairs_skip_point_membership_dispatch() {
        use crate::test_support::binary_dispatch_counters;

        let left = PreparedPlan::new(rectangle(0.0, 0.0, 4.0, 4.0));
        let right = PreparedPlan::new(rectangle(10.0, 10.0, 14.0, 14.0));
        for predicate in [
            Predicate::Intersects,
            Predicate::Contains,
            Predicate::Within,
            Predicate::Touches,
        ] {
            binary_dispatch_counters::reset();
            assert!(
                !topology_scalar_pair(
                    &predicate.spec(),
                    &PreparedTopologyPair::cartesian(&left, &right),
                )
                .unwrap()
            );
            assert_eq!(
                binary_dispatch_counters::snapshot().point_predicate_dispatches,
                0
            );
        }
        let point = PreparedPlan::new(Shape::Point(Point::new_raw_xy(2.0, 2.0)));
        assert!(
            topology_scalar_pair(
                &Predicate::Contains.spec(),
                &PreparedTopologyPair::cartesian(&left, &point),
            )
            .unwrap()
        );
        assert!(binary_dispatch_counters::snapshot().point_predicate_dispatches > 0);
    }

    #[test]
    #[cfg(debug_assertions)]
fn scalar_point_membership_precedes_generic_topology_gates() {
        let polygon =
            crate::io::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))").unwrap();
        let target = PreparedPlan::transient(&polygon);
        for (x, y, strict, inclusive) in [
            (3.0, 3.0, true, true),
            (0.0, 3.0, false, true),
            (1.5, 1.5, false, false),
            (9.0, 8.0, false, false),
        ] {
            let point = Shape::Point(Point::new_raw_xy(x, y));
            let point = PreparedPlan::transient(&point);
            for (predicate, swapped, expected) in [
                (Predicate::Contains, false, strict),
                (Predicate::ContainsProperly, false, strict),
                (Predicate::Within, true, strict),
                (Predicate::Covers, false, inclusive),
                (Predicate::CoveredBy, true, inclusive),
                (Predicate::Intersects, false, inclusive),
                (Predicate::Intersects, true, inclusive),
                (Predicate::Disjoint, false, !inclusive),
                (Predicate::Disjoint, true, !inclusive),
            ] {
                let pair = if swapped {
                    PreparedTopologyPair::cartesian(&point, &target)
                } else {
                    PreparedTopologyPair::cartesian(&target, &point)
                };
                PRE_TOPOLOGY_CALLS.with(|calls| calls.set(0));
                assert_eq!(
                    topology_scalar_pair(&predicate.spec(), &pair).unwrap(),
                    expected
                );
                assert_eq!(PRE_TOPOLOGY_CALLS.with(std::cell::Cell::get), 0);
            }
        }
        PRE_TOPOLOGY_CALLS.with(|calls| calls.set(0));
        topology_scalar_pair(
            &Predicate::Intersects.spec(),
            &PreparedTopologyPair::cartesian(&target, &target),
        )
        .unwrap();
        assert!(PRE_TOPOLOGY_CALLS.with(std::cell::Cell::get) > 0);
    }

    #[test]
    #[cfg(debug_assertions)]
fn packed_point_batches_reuse_membership_without_generic_gates_or_source_scans() {
        let polygon = crate::io::parse_wkt(
            "POLYGON ((0 0,1 0,2 0,3 0,4 0,4 1,4 2,4 3,4 4,\
             3 4,2 4,1 4,0 4,0 3,0 2,0 1,0 0),\
             (1 1,2 1,2 2,1 2,1 1))",
        )
        .unwrap();
        let array = PyGeometryArray::from_shapes(
            [(9.0, 8.0), (3.0, 3.0), (0.0, 3.0), (1.5, 1.5), (2.0, 1.5)]
                .into_iter()
                .map(|(x, y)| Shape::Point(Point::new_raw_xy(x, y)))
                .collect(),
            Frame::None,
        )
        .unwrap();
        assert!(matches!(array.storage(), GeometryArrayStorage::Packed(_)));
        for (predicate, scalar_is_left, expected) in [
            (Predicate::Intersects, true, [
                false, true, true, false, true,
            ]),
            (Predicate::Disjoint, false, [
                true, false, false, true, false,
            ]),
            (Predicate::Contains, true, [
                false, true, false, false, false,
            ]),
            (Predicate::ContainsProperly, true, [
                false, true, false, false, false,
            ]),
            (Predicate::Within, false, [false, true, false, false, false]),
            (Predicate::Covers, true, [false, true, true, false, true]),
            (Predicate::CoveredBy, false, [
                false, true, true, false, true,
            ]),
        ] {
            let scalar = PreparedPlan::transient(&polygon);
            for _ in 0..2 {
                PRE_TOPOLOGY_CALLS.with(|calls| calls.set(0));
                let actual = scalar_vs_shapes(
                    &predicate.spec(),
                    &scalar,
                    array
                        .row_operation()
                        .rows_with_missing(|| ())
                        .map(|row| match row {
                            crate::array::ArrayRow::Missing { logical, .. } => (logical, None),
                            crate::array::ArrayRow::Present(row) => {
                                (row.logical(), Some(row.row()))
                            },
                        }),
                    scalar_is_left,
                    Some(&array),
                    TopologyDomain::Cartesian,
                    &crate::geometry::FrameDependentCaches::default(),
                )
                .unwrap();
                assert_eq!(actual, expected, "{predicate:?}");
                assert_eq!(
                    PRE_TOPOLOGY_CALLS.with(std::cell::Cell::get),
                    0,
                    "{predicate:?}"
                );
                assert_eq!(scalar.point_location_scans(), 0, "{predicate:?}");
            }
        }
    }

    #[test]
fn exterior_scalar_points_do_not_warm_a_membership_index() {
        let mut coordinates = (0..512)
            .map(|i| {
                let angle = std::f64::consts::TAU * f64::from(i) / 512.0;
                Point::new_raw_xy(5.0 * angle.cos(), 5.0 * angle.sin())
            })
            .collect::<Vec<_>>();
        coordinates.push(coordinates[0]);
        let polygon = Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&coordinates).unwrap()).unwrap(),
            vec![],
        ));
        let target = PreparedPlan::transient(&polygon);
        target.bounds().unwrap();
        let outside_shape = Shape::Point(Point::new_raw_xy(9.0, 8.0));
        let outside = PreparedPlan::transient(&outside_shape);
        let spec = Predicate::Intersects.spec();
        let ((), allocations) = crate::test_support::allocation_count(|| {
            for _ in 0..8 {
                assert!(
                    !topology_scalar_pair(
                        &spec,
                        &PreparedTopologyPair::cartesian(&target, &outside)
                    )
                    .unwrap()
                );
            }
        });
        assert_eq!(allocations, 0);
        let inside_shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let inside = PreparedPlan::transient(&inside_shape);
        let ((), control_allocations) = crate::test_support::allocation_count(|| {
            for _ in 0..8 {
                assert!(
                    topology_scalar_pair(&spec, &PreparedTopologyPair::cartesian(&target, &inside))
                        .unwrap()
                );
            }
        });
        assert!(control_allocations > 0);
    }

    #[test]
fn shared_boolean_point_predicates_match_unshared_wkb_expansion() {
        let cases = [
            (
                crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap(),
                Point::new_raw_xy(1.0, 1.0),
            ),
            (
                crate::io::parse_wkt("LINESTRING (0 0,2 0)").unwrap(),
                Point::new_raw_xy(0.0, 0.0),
            ),
            (
                crate::io::parse_wkt("POINT (1 1)").unwrap(),
                Point::new_raw_xy(1.0, 1.0),
            ),
            (
                crate::io::parse_wkt(
                    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
                )
                .unwrap(),
                Point::new_raw_xy(0.5, 0.5),
            ),
            (
                crate::io::parse_wkt(
                    "GEOMETRYCOLLECTION (POINT ZM (0 0 1 10), POINT ZM (0 0 2 20), POINT ZM (1 1 3 30))",
                )
                .unwrap(),
                Point::new_raw_xy(0.0, 0.0),
            ),
            (
                Shape::geometry_collection(vec![
                    crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap(),
                    crate::io::parse_wkt("LINESTRING (-1 1,3 1)").unwrap(),
                    crate::io::parse_wkt("POINT (1 1)").unwrap(),
                ])
                .unwrap(),
                Point::new_raw_xy(1.0, 1.0),
            ),
        ];

        for (base, query) in cases {
            let point = Shape::Point(query);
            for depth in [6, 10] {
                let shared = shared_collection(base.clone(), depth);
                let expanded = crate::io::parse_wkb(&wkb(&shared))
                    .expect("expanded WKB parses")
                    .shape;

                for predicate in [
                    Predicate::Intersects,
                    Predicate::Disjoint,
                    Predicate::Covers,
                ] {
                    assert_eq!(
                        cartesian_predicate(predicate, &shared, &point),
                        cartesian_predicate(predicate, &expanded, &point),
                        "{predicate:?}, depth {depth}"
                    );
                }
                assert_eq!(
                    cartesian_predicate(Predicate::CoveredBy, &point, &shared),
                    cartesian_predicate(Predicate::CoveredBy, &point, &expanded),
                    "CoveredBy, depth {depth}"
                );
            }
        }
    }

    #[test]
fn exact_location_predicates_keep_occurrence_boundary_parity() {
        let line = crate::io::parse_wkt("LINESTRING (0 0,1 0)").unwrap();
        let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let shared = shared_collection(line, 6);
        let expanded = crate::io::parse_wkb(&wkb(&shared))
            .expect("expanded WKB parses")
            .shape;

        for predicate in [Predicate::Contains, Predicate::Touches] {
            assert_eq!(
                cartesian_predicate(predicate, &shared, &point),
                cartesian_predicate(predicate, &expanded, &point),
                "{predicate:?}"
            );
        }
    }

    #[test]
fn depth_40_and_128_shared_point_intersects_complete() {
        let square = rectangle(0.0, 0.0, 1.0, 1.0);
        let point = Shape::Point(Point::new_raw_xy(0.5, 0.5));
        for depth in [40, 128] {
            let shared = shared_collection(square.clone(), depth);
            assert!(cartesian_predicate(Predicate::Intersects, &shared, &point));
        }
    }

    #[test]
fn shared_collection_point_batches_do_not_require_expanded_coordinate_counts() {
        let line = crate::io::parse_wkt("LINESTRING (0 0,2 0)").unwrap();
        let shared = shared_collection(line, 64);
        let point = Point::new_raw_xy(1.0, 0.0);
        assert!(crate::geometry::cartesian_point_covers(&shared, point));
        let array =
            PyGeometryArray::from_shapes(vec![Shape::Point(point); 8], Frame::None).unwrap();
        let scalar = PreparedPlan::transient(&shared);
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &scalar,
            (0..8).map(|index| (index, Some(ShapeRow::Point(point)))),
            true,
            Some(&array),
            TopologyDomain::Cartesian,
            &crate::geometry::FrameDependentCaches::default(),
        )
        .unwrap();
        assert_eq!(actual, vec![true; 8]);
    }
