#[test]
fn crossing_line_matrix_matches_partition_in_all_evaluator_modes() {
    let left = crate::io::parse_wkt("LINESTRING (-1 0,1 0)").unwrap();
    let right = crate::io::parse_wkt("LINESTRING (0 -1,0 1)").unwrap();
    let oracle = partition_oracle(&left, &right).unwrap();
    assert_eq!(oracle.text(), "0F1FF0102");

    let left_plan = PreparedPlan::transient(&left);
    let right_plan = PreparedPlan::transient(&right);
    let pair = [
        Operand::from_plan(&left_plan),
        Operand::from_plan(&right_plan),
    ];
    assert_eq!(relate(&left_plan, &right_plan).unwrap(), oracle);
    assert_eq!(
        evaluate_mode::<MatrixQuery, false>(&pair, &MatrixQuery).unwrap(),
        oracle
    );
    assert_eq!(
        evaluate_mode::<EndOnly<MatrixQuery>, false>(&pair, &EndOnly(MatrixQuery)).unwrap(),
        oracle
    );
    assert_eq!(
        evaluate_mode::<MatrixQuery, false>(
            &[
                Operand::from_row(ShapeRow::Shape(&left)),
                Operand::from_row(ShapeRow::Shape(&right)),
            ],
            &MatrixQuery,
        )
        .unwrap(),
        oracle
    );

    for predicate in Predicate::ALL {
        let expected = matrix_predicates::decided(predicate, oracle);
        assert_eq!(
            self::predicate(&left_plan, &right_plan, predicate).unwrap(),
            expected,
            "{predicate:?}"
        );
    }
    for pattern in ["0********", "F*1***0**", "*********", "T*****FF*"] {
        let pattern = CompiledPattern::new(pattern).expect("test pattern is valid");
        assert_eq!(
            self::pattern(&left_plan, &right_plan, pattern).unwrap(),
            oracle.matches(pattern),
            "{pattern:?}"
        );
    }
}

mod matrix_corpus {
    use super::*;
    relation_corpus!(assert_matrix_oracle_parity);
}

#[test]
fn named_counter_cases_match_exact_matrices_and_predicates() {
    let cases = [
        (
            "collapsed polygon overlaps line",
            "POLYGON ((0 0,2 2,4 4,0 0))",
            "LINESTRING (1 1,5 5)",
            Some("1010F0102"),
            Some((Predicate::Overlaps, true)),
        ),
        (
            "collapsed polygon crosses polygon",
            "POLYGON ((0 0,2 2,4 4,0 0))",
            "POLYGON ((1 3,3 1,3 3,1 3))",
            Some("101FF0212"),
            Some((Predicate::Crosses, true)),
        ),
        (
            "collapsed line meets a nonempty line",
            "LINESTRING (0 0,0 0)",
            "LINESTRING (-1 0,1 0)",
            Some("0FFFFF102"),
            Some((Predicate::Intersects, true)),
        ),
        (
            "external hole equals itself",
            "POLYGON ((0 0,2 0,2 2,0 2,0 0),(5 5,6 5,6 6,5 6,5 5))",
            "POLYGON ((0 0,2 0,2 2,0 2,0 0),(5 5,6 5,6 6,5 6,5 5))",
            Some("2FFF1FFF2"),
            Some((Predicate::Equals, true)),
        ),
        (
            "empty equals shell-equals-hole",
            "POLYGON EMPTY",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
            Some("FFFFFFFF2"),
            Some((Predicate::Equals, true)),
        ),
        (
            "shell-equals-hole against square",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
            "POLYGON ((-1 -1,5 -1,5 5,-1 5,-1 -1))",
            Some("FFFFFF212"),
            Some((Predicate::Equals, false)),
        ),
        (
            "spike equals itself",
            "POLYGON ((0 0,4 0,4 4,2 2,4 4,0 4,0 0))",
            "POLYGON ((0 0,4 0,4 4,2 2,4 4,0 4,0 0))",
            Some("2FFF1FFF2"),
            Some((Predicate::Equals, true)),
        ),
        (
            "bow-tie and polygonized lobes with an exterior point",
            "GEOMETRYCOLLECTION (POINT (2 5), POLYGON ((0 0,4 4,4 0,0 4,0 0)))",
            "GEOMETRYCOLLECTION (POINT (2 5), MULTIPOLYGON (((0 0,2 2,0 4,0 0)), ((2 2,4 0,4 4,2 2))))",
            Some("2FFF1FFF2"),
            Some((Predicate::Equals, true)),
        ),
        (
            "point inside hole",
            "POINT (5 5)",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
            None,
            None,
        ),
        (
            "point on hole edge",
            "POINT (3 5)",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
            None,
            None,
        ),
        (
            "point at touching-hole vertex",
            "POINT (4 4)",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2),(4 4,6 4,6 6,4 6,4 4))",
            None,
            None,
        ),
        (
            "monotone lines meet end to start",
            "LINESTRING (0 0,1 0,2 0,3 0,4 0)",
            "LINESTRING (4 0,5 0,6 0,7 0,8 0)",
            None,
            None,
        ),
        (
            "stored vertex and constructed crossing share one node",
            "GEOMETRYCOLLECTION (LINESTRING (-1 0,0 0,1 0),LINESTRING (-1 -1,1 1))",
            "LINESTRING (0 -1,0 1)",
            None,
            None,
        ),
        (
            "two holes touch at their first vertex",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2),(4 4,6 4,6 6,4 6,4 4))",
            "LINESTRING (3 3,5 5)",
            None,
            None,
        ),
        (
            "line interior in a mixed collection",
            "GEOMETRYCOLLECTION (POLYGON ((0 0,2 0,2 2,0 2,0 0)),LINESTRING (3 3,5 5))",
            "POINT (4 4)",
            None,
            None,
        ),
    ];
    for (name, left_wkt, right_wkt, expected_matrix, expected_predicate) in cases {
        let left = crate::io::parse_wkt(left_wkt)
            .unwrap_or_else(|error| panic!("{name}: left WKT did not parse: {error}"));
        let right = crate::io::parse_wkt(right_wkt)
            .unwrap_or_else(|error| panic!("{name}: right WKT did not parse: {error}"));
        let oracle = partition_oracle(&left, &right)
            .unwrap_or_else(|error| panic!("{name}: exact partition failed: {error}"));
        if let Some(expected) = expected_matrix {
            assert_eq!(oracle.text(), expected, "{name}: oracle matrix");
        }
        assert_locator_oracle_parity(name, &left, &right);
        assert_full_oracle_and_controls(name, &left, &right, oracle);
        if let Some((predicate, expected)) = expected_predicate {
            let left_plan = PreparedPlan::transient(&left);
            let right_plan = PreparedPlan::transient(&right);
            assert_eq!(
                self::predicate(&left_plan, &right_plan, predicate).unwrap(),
                expected,
                "{name}: {predicate:?}"
            );
        }
    }

    let mixed = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION (POLYGON ((0 0,2 0,2 2,0 2,0 0)),LINESTRING (3 3,5 5))",
    )
    .unwrap();
    let line_location = PreparedPlan::transient(&mixed)
        .locate_stored(Point::new_raw_xy(4.0, 4.0))
        .unwrap();
    assert_eq!(line_location.location, Loc::Interior);
    assert!(!line_location.area);
}
