fn partition_oracle(left: &Shape, right: &Shape) -> Result<De9im> {
    let rows = [ShapeRow::Shape(left), ShapeRow::Shape(right)];
    let inventory = ExactSourceInventory::from_rows_with_occurrences(rows.into_iter())?;
    let mut budget = ExpansionBudget::new("relate", "test exact partition oracle");
    let partition = inventory.partition(&mut budget)?;
    inventory.relate(&partition)
}

fn assert_locator_oracle_parity(name: &str, left: &Shape, right: &Shape) {
    let rows = [ShapeRow::Shape(left), ShapeRow::Shape(right)];
    let inventory = ExactSourceInventory::from_rows_with_occurrences(rows.into_iter()).unwrap();
    let mut budget = ExpansionBudget::new("relate", "test exact partition locator oracle");
    let partition = inventory.partition(&mut budget).unwrap();
    let (nodes, spans) = inventory.relation_location_samples(&partition).unwrap();
    let left_plan = PreparedPlan::transient(left);
    let right_plan = PreparedPlan::transient(right);

    for plan in [&left_plan, &right_plan] {
        for _ in 0..4 {
            plan.locate_stored(Point::new_raw_xy(0.0, 0.0)).unwrap();
        }
    }

    let directions = [
        [ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(0.0)],
        [ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(0.0)],
        [ExactDyadic::from_f64(0.0), ExactDyadic::from_f64(1.0)],
        [ExactDyadic::from_f64(0.0), ExactDyadic::from_f64(-1.0)],
        [ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(1.0)],
        [ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(-1.0)],
        [ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(1.0)],
        [ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(-1.0)],
    ];
    for (kind, samples) in [("node", &nodes), ("span", &spans)] {
        for (sample_index, (point, locations)) in samples.iter().enumerate() {
            let [left_location, right_location] = *locations;
            for (operand, plan, expected) in [
                (left, &left_plan, left_location),
                (right, &right_plan, right_location),
            ] {
                let row = ShapeRow::Shape(operand);
                let streamed = cartesian_row_exact_point_location_and_area(row, point).unwrap();
                assert_eq!(
                    streamed, expected,
                    "{name} {kind} {sample_index}: streaming point location"
                );
                let indexed = plan.locate_exact(point).unwrap();
                assert_eq!(
                    indexed, expected,
                    "{name} {kind} {sample_index}: prepared point location"
                );
                for direction in &directions {
                    let streamed =
                        cartesian_row_exact_offset_location_and_area(row, point, direction)
                            .unwrap();
                    let indexed = plan.locate_exact_offset(point, direction).unwrap();
                    assert_eq!(
                        indexed, streamed,
                        "{name} {kind} {sample_index}: indexed exact offset"
                    );
                    assert_eq!(
                        occupied_toward(indexed),
                        occupied_toward(streamed),
                        "{name} {kind} {sample_index}: occupied-toward result"
                    );
                }
            }
        }
    }
}

fn assert_full_oracle_and_controls(name: &str, left: &Shape, right: &Shape, oracle: De9im) {
    assert_direction_against_oracle(name, "forward", left, right, oracle);
    assert_direction_against_oracle(name, "reverse", right, left, oracle.transpose());
}

fn assert_direction_against_oracle(
    name: &str,
    direction: &str,
    left: &Shape,
    right: &Shape,
    oracle: De9im,
) {
    let left_plan = PreparedPlan::transient(left);
    let right_plan = PreparedPlan::transient(right);
    let pair = [
        Operand::from_plan(&left_plan),
        Operand::from_plan(&right_plan),
    ];
    let production = relate(&left_plan, &right_plan).unwrap();
    let saturation_off = evaluate_mode::<MatrixQuery, false>(&pair, &MatrixQuery).unwrap();
    let end_only =
        evaluate_mode::<EndOnly<MatrixQuery>, false>(&pair, &EndOnly(MatrixQuery)).unwrap();
    assert_eq!(
        (production, saturation_off, end_only),
        (oracle, oracle, oracle),
        "{name} {direction}: production/saturation-off/end-only matrices"
    );
    assert_eq!(
        evaluate_mode::<MatrixQuery, false>(
            &[
                Operand::from_row(ShapeRow::Shape(left)),
                Operand::from_row(ShapeRow::Shape(right)),
            ],
            &MatrixQuery,
        )
        .unwrap(),
        oracle,
        "{name} {direction}: borrowed ShapeRow matrix"
    );
    for predicate in Predicate::ALL {
        let expected = matrix_predicates::decided(predicate, oracle);
        assert_eq!(
            self::predicate(&left_plan, &right_plan, predicate).unwrap(),
            expected,
            "{name} {direction}: {predicate:?}: production query"
        );
        assert_eq!(
            evaluate_mode::<PredicateQuery, false>(&pair, &PredicateQuery(predicate)).unwrap(),
            expected,
            "{name} {direction}: {predicate:?}: saturation-off query"
        );
        assert_eq!(
            evaluate_mode::<EndOnly<PredicateQuery>, false>(
                &pair,
                &EndOnly(PredicateQuery(predicate)),
            )
            .unwrap(),
            expected,
            "{name} {direction}: {predicate:?}: end-only query"
        );
    }
    let mut patterns = vec![
        CompiledPattern::new(&oracle.text()).expect("matrix string is a pattern"),
        CompiledPattern::new("*********").expect("wildcard pattern is valid"),
    ];
    for cell in 0..9 {
        for literal in b"FT012" {
            let mut bytes = *b"*********";
            *bytes.get_mut(cell).expect("DE-9IM has nine cells") = *literal;
            patterns.push(
                CompiledPattern::new(std::str::from_utf8(&bytes).expect("ASCII pattern"))
                    .expect("single-cell pattern is valid"),
            );
        }
    }
    for pattern in [
        "T*T***T**",
        "T*****FF*",
        "F**F*****",
        "T*F**F***",
        "**T***T**",
        "FF*FF****",
        "F**0**F**",
        "1********",
        "2********",
        "********0",
        "****1****",
    ] {
        patterns.push(CompiledPattern::new(pattern).expect("fixed pattern is valid"));
    }
    for pattern in patterns {
        let expected = oracle.matches(pattern);
        assert_eq!(
            self::pattern(&left_plan, &right_plan, pattern).unwrap(),
            expected,
            "{name} {direction}: {pattern:?}: production query"
        );
        assert_eq!(
            evaluate_mode::<PatternQuery, false>(&pair, &PatternQuery(pattern)).unwrap(),
            expected,
            "{name} {direction}: {pattern:?}: saturation-off query"
        );
        assert_eq!(
            evaluate_mode::<EndOnly<PatternQuery>, false>(&pair, &EndOnly(PatternQuery(pattern)),)
                .unwrap(),
            expected,
            "{name} {direction}: {pattern:?}: end-only query"
        );
    }
}

fn run_curated_relation_corpus(locator_only: bool) {
    let cases = [
        ("empty polygons", "POLYGON EMPTY", "POLYGON EMPTY"),
        (
            "empty collection and line",
            "GEOMETRYCOLLECTION EMPTY",
            "LINESTRING (0 0,1 1)",
        ),
        (
            "empty line and polygon",
            "LINESTRING EMPTY",
            "POLYGON ((0 0,2 0,2 2,0 2,0 0))",
        ),
        (
            "equal points with different ZM",
            "POINT ZM (1 2 3 4)",
            "POINT ZM (1 2 30 40)",
        ),
        (
            "point sets partly equal",
            "MULTIPOINT ((0 0),(1 1),(2 2))",
            "MULTIPOINT ((1 1),(3 3))",
        ),
        (
            "point in area",
            "POINT (1 1)",
            "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
        ),
        (
            "point on area edge",
            "POINT (1 0)",
            "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
        ),
        (
            "point in hole",
            "POINT (5 5)",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
        ),
        (
            "crossing lines, non-dyadic node",
            "LINESTRING (0 0,3 3)",
            "LINESTRING (0 1,2 0)",
        ),
        (
            "vertex-through crossing",
            "LINESTRING (-1 0,0 0,1 0)",
            "LINESTRING (0 -1,0 1)",
        ),
        (
            "collinear partial overlap",
            "LINESTRING (0 0,4 0)",
            "LINESTRING (2 0,6 0)",
        ),
        (
            "collinear reversed overlap",
            "LINESTRING (0 0,4 0)",
            "LINESTRING (6 0,2 0)",
        ),
        (
            "collinear disjoint",
            "LINESTRING (0 0,1 0)",
            "LINESTRING (2 0,3 0)",
        ),
        (
            "closed line",
            "LINESTRING (0 0,4 0,4 4,0 0)",
            "LINESTRING (0 0,4 4,4 0,0 0)",
        ),
        ("collapsed line", "LINESTRING (7 7,7 7)", "POINT (7 7)"),
        (
            "line endpoint parity",
            "MULTILINESTRING ((0 0,1 0),(1 0,2 0),(2 0,0 0))",
            "POINT (1 0)",
        ),
        (
            "nested polygons",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0))",
            "POLYGON ((2 2,4 2,4 4,2 4,2 2))",
        ),
        (
            "polygon overlap",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0))",
            "POLYGON ((2 -1,5 -1,5 2,2 2,2 -1))",
        ),
        (
            "shared polygon edge",
            "POLYGON ((0 0,2 0,2 2,0 2,0 0))",
            "POLYGON ((2 0,4 0,4 2,2 2,2 0))",
        ),
        (
            "vertex-touching polygons",
            "POLYGON ((0 0,2 0,2 2,0 2,0 0))",
            "POLYGON ((2 2,4 2,4 4,2 4,2 2))",
        ),
        (
            "disjoint polygons",
            "POLYGON ((0 0,1 0,1 1,0 1,0 0))",
            "POLYGON ((3 3,4 3,4 4,3 4,3 3))",
        ),
        (
            "line crosses polygon",
            "LINESTRING (-1 1,4 1)",
            "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
        ),
        (
            "line inside polygon",
            "LINESTRING (1 1,2 2)",
            "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
        ),
        (
            "line on polygon edge",
            "LINESTRING (0 0,3 0)",
            "POLYGON ((0 0,3 0,3 3,0 3,0 0))",
        ),
        (
            "line through polygon hole",
            "LINESTRING (-1 5,11 5)",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
        ),
        (
            "hole exterior point",
            "POINT (11 11)",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(11 11,13 11,13 13,11 13,11 11))",
        ),
        (
            "hole touches shell",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(0 5,2 4,2 6,0 5))",
            "LINESTRING (-1 5,3 5)",
        ),
        (
            "bow-tie ring",
            "POLYGON ((0 0,4 4,0 4,4 0,0 0))",
            "POLYGON ((1 1,3 1,3 3,1 3,1 1))",
        ),
        (
            "spiked ring",
            "POLYGON ((0 0,4 0,4 4,2 2,4 4,0 4,0 0))",
            "POLYGON ((1 1,3 1,3 3,1 3,1 1))",
        ),
        (
            "collapsed polygon ring",
            "POLYGON ((0 0,2 0,0 0,2 0,0 0))",
            "LINESTRING (0 0,2 0)",
        ),
        (
            "mixed collection",
            "GEOMETRYCOLLECTION (POINT (1 1),LINESTRING (0 0,3 0),POLYGON ((0 0,2 0,2 2,0 2,0 0)))",
            "LINESTRING (1 0,3 0)",
        ),
        (
            "repeated members",
            "GEOMETRYCOLLECTION (LINESTRING (0 0,2 0),LINESTRING (2 0,0 0),POINT (1 0),POINT (1 0))",
            "LINESTRING (1 -1,1 1)",
        ),
        (
            "overlapping multipolygon members",
            "MULTIPOLYGON (((0 0,3 0,3 3,0 3,0 0)),((2 0,5 0,5 3,2 3,2 0)))",
            "POLYGON ((1 1,4 1,4 2,1 2,1 1))",
        ),
        (
            "three concurrent segments",
            "MULTILINESTRING ((-2 0,2 0),(0 -2,0 2),(-2 -2,2 2))",
            "LINESTRING (-2 2,2 -2)",
        ),
        (
            "shell equals hole",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
            "POLYGON ((1 1,2 1,2 2,1 2,1 1))",
        ),
        (
            "hole encloses shell",
            "POLYGON ((2 2,4 2,4 4,2 4,2 2),(0 0,10 0,10 10,0 10,0 0))",
            "POINT (3 3)",
        ),
        (
            "large coordinates",
            "LINESTRING (1e150 1e150,3e150 3e150)",
            "LINESTRING (1e150 2e150,2e150 1e150)",
        ),
        (
            "tiny coordinates",
            "POLYGON ((1e-150 1e-150,3e-150 1e-150,3e-150 3e-150,1e-150 3e-150,1e-150 1e-150))",
            "POLYGON ((2e-150 0.5e-150,4e-150 0.5e-150,4e-150 2e-150,2e-150 2e-150,2e-150 0.5e-150))",
        ),
        (
            "one-ulp-offset lines",
            "LINESTRING (0 0,1 1)",
            "LINESTRING (0 5e-324,1 1.0000000000000002)",
        ),
        (
            "duplicate source segment",
            "GEOMETRYCOLLECTION (LINESTRING (0 0,2 0),LINESTRING (2 0,0 0))",
            "LINESTRING (1 -1,1 1)",
        ),
    ];
    for (name, left_wkt, right_wkt) in cases {
        let left = crate::io::parse_wkt(left_wkt)
            .unwrap_or_else(|error| panic!("{name}: left WKT did not parse: {error}"));
        let right = crate::io::parse_wkt(right_wkt)
            .unwrap_or_else(|error| panic!("{name}: right WKT did not parse: {error}"));
        if locator_only {
            assert_locator_oracle_parity(name, &left, &right);
        } else {
            let oracle = partition_oracle(&left, &right)
                .unwrap_or_else(|error| panic!("{name}: exact partition failed: {error}"));
            assert_full_oracle_and_controls(name, &left, &right, oracle);
        }
    }
}
