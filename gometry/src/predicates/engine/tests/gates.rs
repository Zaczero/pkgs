    /// Sound disjoint bounds prove the four inner cells empty and each
    /// operand inside the other's exterior, and nothing else: whether an
    /// open line's boundary meets the other's exterior is a staging fact the
    /// bounds cannot see, so a pattern reading that cell must come back
    /// undecided rather than guessed.
    #[test]
fn disjoint_bounds_decide_a_pattern_only_where_they_prove_it() {
        let gate =
            |left: &crate::geometry::Shape, right: &crate::geometry::Shape, pattern: &str| {
                let (lp, rp) = (
                    PreparedPlan::transient(left),
                    PreparedPlan::transient(right),
                );
                let lo = GateOperand::cartesian(ShapeRow::Shape(lp.shape()), lp.bounds());
                let ro = GateOperand::cartesian(ShapeRow::Shape(rp.shape()), rp.bounds());
                super::pattern_bounds_gate(
                    crate::geometry::CompiledPattern::new(pattern).unwrap(),
                    &lo,
                    &ro,
                )
                .unwrap()
            };
        let (a, b) = (rectangle(0.0, 0.0, 1.0, 1.0), rectangle(3.0, 3.0, 4.0, 4.0));
        assert_eq!(
            gate(&a, &b, "FF2FF1212"),
            Some(true),
            "the full disjoint areal matrix"
        );
        assert_eq!(gate(&a, &b, "T********"), Some(false));
        let (l, m) = (line((0.0, 0.0), (1.0, 1.0)), line((5.0, 5.0), (6.0, 6.0)));
        assert_eq!(
            gate(&l, &m, "FF*FF****"),
            Some(true),
            "disjoint, wildcards on the open cells"
        );
        assert_eq!(
            gate(&l, &m, "FF1FF*1*2"),
            Some(true),
            "consistent on every settled cell, wildcards on the open ones"
        );
        assert_eq!(
            gate(&l, &m, "FF1FF0102"),
            None,
            "BE reads a boundary cell the bounds cannot settle"
        );
        assert_eq!(gate(&l, &m, "FF1FF*102"), None, "EB likewise");
        assert_eq!(
            gate(&a, &l, "FF2FF11F2"),
            None,
            "the line's EB cell is open even against an areal operand"
        );
        // Polygon against a line disjoint from it: the line's EB cell is the
        // one the bounds cannot settle, and every other cell is decided.
        {
            let cells: [&str; 9] = [
                "T********",
                "*T*******",
                "**2******",
                "***T*****",
                "****T****",
                "*****1***",
                "******1**",
                "*******T*",
                "********2",
            ];
            let open: Vec<usize> = cells
                .iter()
                .enumerate()
                .filter(|(_, c)| gate(&a, &m, c).is_none())
                .map(|(i, _)| i)
                .collect();
            assert_eq!(open, vec![7], "only EB may be open for polygon-vs-line");
        }
        assert_eq!(
            gate(&a, &m, "FF2FF11F2"),
            None,
            "the line's EB cell is open even against an areal operand"
        );
        assert_eq!(
            gate(&a, &m, "FF2FF11*2"),
            Some(true),
            "and settled everywhere else"
        );
        assert_eq!(
            gate(&a, &l, "FF2FF11*2"),
            None,
            "a line inside the square's box: bounds meet, the gate declines"
        );
        let touching = rectangle(1.0, 0.0, 2.0, 1.0);
        assert_eq!(
            gate(&a, &touching, "FF2FF1212"),
            None,
            "bounds meet: the gate declines"
        );
        let cancelled =
            crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))").unwrap();
        let remote = rectangle(4.0, 4.0, 5.0, 5.0);
        assert_eq!(
            gate(&cancelled, &remote, "**2******"),
            None,
            "declared polygon kind cannot prove an effective interior"
        );
        assert!(!cancelled.relate_pattern(&remote, "**2******").unwrap());
    }

    #[test]
fn disjoint_support_refutations_match_partition_matrices() {
        for case in FAR_APART_SUPPORT_CASES {
            let left = parse_wkt_shape(case.left);
            let right = parse_wkt_shape(case.right);
            let (left_plan, right_plan) = prepared_pair(&left, &right);
            let pair = PreparedTopologyPair::cartesian(&left_plan, &right_plan);
            let matrix = pair.relate().expect("fixture relation succeeds");
            if let Some(expected) = case.matrix {
                assert_eq!(matrix.text(), expected, "{} matrix", case.name);
            }

            for predicate in Predicate::ALL {
                let spec = predicate.spec();
                let without_pre_topology_gate = topology_pair_after_gates(&spec, &pair)
                    .expect("ungated Cartesian predicate succeeds");
                let with_pre_topology_gate =
                    topology_scalar_pair(&spec, &pair).expect("Cartesian predicate succeeds");
                assert_eq!(
                    with_pre_topology_gate, without_pre_topology_gate,
                    "{}: pre-topology gate changed {predicate:?}",
                    case.name,
                );
                assert_eq!(
                    with_pre_topology_gate,
                    predicate_matrix_value(&spec, matrix),
                    "{}: {predicate:?} disagrees with the exact matrix",
                    case.name,
                );
            }
        }
    }

    /// Two rectangles settle every predicate from their bounds. Each shell
    /// carries an extra collinear vertex, which the gate still certifies as a
    /// box but which sends the ungated evaluator through its general contact
    /// path instead of its own rectangle shortcut.
    #[test]
fn rectangle_gate_matches_the_general_evaluator_for_every_predicate() {
        let densified = |minx: f64, miny: f64, maxx: f64, maxy: f64| {
            parse_wkt_shape(&format!(
                "POLYGON (({minx} {miny},{} {miny},{maxx} {miny},{maxx} {maxy},{minx} {maxy},{minx} {miny}))",
                (minx + maxx) / 2.0
            ))
        };
        let base = densified(0.0, 0.0, 4.0, 4.0);
        let cases = [
            ("disjoint", densified(6.0, 0.0, 8.0, 4.0)),
            ("shared edge", densified(4.0, 0.0, 8.0, 4.0)),
            ("shared corner", densified(4.0, 4.0, 8.0, 8.0)),
            ("partial edge", densified(4.0, 1.0, 6.0, 3.0)),
            ("overlapping", densified(2.0, 2.0, 6.0, 6.0)),
            ("crossing strip", densified(1.0, -1.0, 3.0, 5.0)),
            ("strictly inside", densified(1.0, 1.0, 3.0, 3.0)),
            ("inside touching edge", densified(0.0, 1.0, 2.0, 3.0)),
            ("inside touching corner", densified(0.0, 0.0, 2.0, 2.0)),
            ("equal", densified(0.0, 0.0, 4.0, 4.0)),
            ("strictly containing", densified(-1.0, -1.0, 5.0, 5.0)),
        ];
        for (name, other) in &cases {
            for (left, right) in [(&base, other), (other, &base)] {
                let (left_plan, right_plan) = prepared_pair(left, right);
                let pair = PreparedTopologyPair::cartesian(&left_plan, &right_plan);
                let left_gate =
                    GateOperand::cartesian(ShapeRow::Shape(left_plan.shape()), left_plan.bounds());
                let right_gate =
                    GateOperand::cartesian(ShapeRow::Shape(right_plan.shape()), right_plan.bounds());
                for predicate in Predicate::ALL {
                    let spec = predicate.spec();
                    let gated = spec.pre_topology(&left_gate, &right_gate);
                    let exact = topology_pair_after_gates(&spec, &pair)
                        .expect("ungated Cartesian predicate succeeds");
                    assert_eq!(gated, Some(exact), "{name}: {predicate:?}");
                }

                let (left_cache, right_cache) = (
                    crate::geometry::FrameDependentCaches::default(),
                    crate::geometry::FrameDependentCaches::default(),
                );
                let left_geographic =
                    GeographicPrepared::try_new(&left_plan, &left_cache, degrees()).unwrap();
                let right_geographic =
                    GeographicPrepared::try_new(&right_plan, &right_cache, degrees()).unwrap();
                let geographic_pair =
                    PreparedTopologyPair::from_geographic(left_geographic, right_geographic);
                for predicate in Predicate::ALL {
                    let spec = predicate.spec();
                    let gated = spec
                        .pre_topology(
                            &GateOperand::from_prepared(&left_geographic),
                            &GateOperand::from_prepared(&right_geographic),
                        );
                    let exact = topology_pair_after_gates(&spec, &geographic_pair)
                        .expect("ungated geographic predicate succeeds");
                    assert_eq!(gated, Some(exact), "geographic {name}: {predicate:?}");
                }
            }
        }
    }

    #[test]
fn disjoint_bounds_refute_intersections_and_only_certified_equality() {
        let intersections = [
            Predicate::Intersects,
            Predicate::Contains,
            Predicate::ContainsProperly,
            Predicate::Within,
            Predicate::Covers,
            Predicate::CoveredBy,
            Predicate::Touches,
            Predicate::Crosses,
            Predicate::Overlaps,
        ];
        for case in FAR_APART_SUPPORT_CASES {
            let left = parse_wkt_shape(case.left);
            let right = parse_wkt_shape(case.right);
            let (left_plan, right_plan) = prepared_pair(&left, &right);
            let (left_gate, right_gate) = (
                GateOperand::cartesian(ShapeRow::Shape(left_plan.shape()), left_plan.bounds()),
                GateOperand::cartesian(ShapeRow::Shape(right_plan.shape()), right_plan.bounds()),
            );
            let Some((left_bounds, right_bounds)) = left_gate.bounds().zip(right_gate.bounds())
            else {
                for predicate in Predicate::ALL {
                    assert_eq!(
                        predicate.spec().bounds_gate(&left_gate, &right_gate),
                        None,
                        "{} has no two bounds",
                        case.name,
                    );
                }
                continue;
            };
            assert!(
                !left_bounds.intersects(right_bounds),
                "{} fixture bounds must be disjoint",
                case.name
            );
            for predicate in intersections {
                assert_eq!(
                    predicate.spec().bounds_gate(&left_gate, &right_gate),
                    Some(false),
                    "{}: {predicate:?} requires shared support",
                    case.name,
                );
            }
            assert_eq!(
                Predicate::Disjoint
                    .spec()
                    .bounds_gate(&left_gate, &right_gate),
                Some(true),
                "{}",
                case.name,
            );
            assert_eq!(
                Predicate::Equals
                    .spec()
                    .bounds_gate(&left_gate, &right_gate),
                case.both_supports_certified_nonempty.then_some(false),
                "{}",
                case.name,
            );
        }
    }

    #[test]
fn disjoint_contains_gate_skips_relation_evaluator_allocations() {
        let (left, right) = prepared_pair(
            &crate::io::parse_wkt("POLYGON ((0 0,10 0,2 8,0 0))").unwrap(),
            &crate::io::parse_wkt("POLYGON ((100 100,110 100,102 108,100 100))").unwrap(),
        );
        assert!(left.bounds().is_some());
        assert!(right.bounds().is_some());
        let pair = PreparedTopologyPair::cartesian(&left, &right);
        let spec = Predicate::Contains.spec();
        let (matrix, evaluator_allocations) =
            crate::test_support::allocation_count(|| pair.relate().expect("matrix succeeds"));
        assert!(
            evaluator_allocations > 0,
            "the general evaluator is the allocation control"
        );
        assert!(!predicate_matrix_value(&spec, matrix));

        let (answer, gate_allocations) = crate::test_support::allocation_count(|| {
            topology_scalar_pair(&spec, &pair).expect("Cartesian predicate succeeds")
        });
        assert!(!answer);
        assert_eq!(
            gate_allocations, 0,
            "disjoint bounds stop before witness evaluation"
        );
    }

    #[test]
fn geographic_crossing_rows_fail_open_the_bounds_gate() {
        let query = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
        let crossing = rectangle(170.0, 40.0, -170.0, 50.0);
        let cache = crate::geometry::FrameDependentCaches::default();
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, Some(ShapeRow::Shape(&crossing)))].into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(actual, [true]);

        let crossing_contains_query = scalar_vs_shapes(
            &Predicate::Contains.spec(),
            &query,
            [(0, Some(ShapeRow::Shape(&crossing)))].into_iter(),
            false,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(crossing_contains_query, [true]);
    }

    #[test]
fn prepared_intersects_rectangle_queries_use_exact_partition_and_respect_holes() {
        let outer = rectangle(0.0, 0.0, 10.0, 10.0);
        let Shape::Polygon(outer) = outer else {
            unreachable!("rectangle helper returns a polygon");
        };
        let hole = Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(3.0, 3.0),
                Point::new_raw_xy(7.0, 3.0),
                Point::new_raw_xy(7.0, 7.0),
                Point::new_raw_xy(3.0, 7.0),
                Point::new_raw_xy(3.0, 3.0),
            ])
            .unwrap(),
        )
        .unwrap();
        let scalar = PreparedPlan::new(Shape::Polygon(Polygon::new(outer.shell, vec![hole])));
        let cache = crate::geometry::FrameDependentCaches::default();
        let inside_hole = rectangle(4.0, 4.0, 6.0, 6.0);
        let inside_shell = rectangle(1.0, 1.0, 2.0, 2.0);
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &scalar,
            [
                (0, Some(ShapeRow::Shape(&inside_hole))),
                (1, Some(ShapeRow::Shape(&inside_shell))),
            ]
            .into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("rectangle queries admit");
        assert_eq!(actual, [false, true]);
        assert_eq!(scalar.geographic_exact_topology_builds(), 0);
        assert_eq!(scalar.geographic_exact_topology_accesses(), 0);
    }

    #[test]
fn geographic_crossing_packed_rows_fail_open_the_bounds_gate() {
        let query = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
        let coords = CoordSeq::try_from_points(&[
            Point::new_raw_xy(170.0, 40.0),
            Point::new_raw_xy(-170.0, 40.0),
            Point::new_raw_xy(-170.0, 50.0),
            Point::new_raw_xy(170.0, 50.0),
            Point::new_raw_xy(170.0, 40.0),
        ])
        .expect("test ring coordinates are valid");
        let ring_offsets =
            crate::geometry::CsrOffsetColumn::<crate::geometry::RingLevel>::try_new(vec![0, 5], 5)
                .expect("test ring offsets are valid");
        let row = ShapeRow::from_packed(crate::geometry::storage::PackedRow::polygon_window(
            0,
            &coords,
            &ring_offsets,
            0..1,
        ));
        let cache = crate::geometry::FrameDependentCaches::default();
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, Some(row))].into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(actual, [true]);
    }

    #[test]
fn geographic_invalid_latitude_is_not_a_bounds_false() {
        let query = PreparedPlan::new(rectangle(0.0, 0.0, 1.0, 1.0));
        let invalid = Shape::Point(Point::new_raw_xy(0.5, 91.0));
        let cache = crate::geometry::FrameDependentCaches::default();
        let error = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &query,
            [(0, Some(ShapeRow::Shape(&invalid)))].into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .unwrap_err();
        let message = error.1.to_string();
        assert!(
            message.contains("invalid longitude/latitude (0.5, 91)"),
            "{message}"
        );
    }
