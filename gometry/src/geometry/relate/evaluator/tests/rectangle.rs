#[test]
fn certified_rectangle_matrices_match_partition_across_axis_relations() {
    let axis_relations = [
        ((0.0, 1.0), (3.0, 4.0)),
        ((0.0, 1.0), (1.0, 2.0)),
        ((0.0, 2.0), (1.0, 3.0)),
        ((0.0, 3.0), (1.0, 2.0)),
        ((0.0, 3.0), (0.0, 3.0)),
        ((0.0, 3.0), (0.0, 2.0)),
        ((0.0, 3.0), (1.0, 3.0)),
        ((1.0, 2.0), (0.0, 3.0)),
        ((1.0, 3.0), (0.0, 2.0)),
        ((1.0, 2.0), (0.0, 1.0)),
        ((3.0, 4.0), (0.0, 1.0)),
        ((-0.0, 1.0), (0.0, 2.0)),
        ((-1.0, -0.0), (0.0, 1.0)),
    ];
    let rectangle = |x: (f64, f64), y: (f64, f64)| {
        Shape::Polygon(
            crate::geometry::rect_polygon(
                crate::geometry::Bounds::new(x.0, y.0, x.1, y.1).unwrap(),
            )
            .unwrap(),
        )
    };

    for (x_index, &(left_x, right_x)) in axis_relations.iter().enumerate() {
        for (y_index, &(left_y, right_y)) in axis_relations.iter().enumerate() {
            let left = rectangle(left_x, left_y);
            let right = rectangle(right_x, right_y);
            let left_plan = PreparedPlan::transient(&left);
            let right_plan = PreparedPlan::transient(&right);
            let oracle = partition_oracle(&left, &right).unwrap();
            let pair = [
                Operand::from_plan(&left_plan),
                Operand::from_plan(&right_plan),
            ];
            let name = format!("x relation {x_index}, y relation {y_index}");

            let bounds = [pair[0].bounds(), pair[1].bounds()];
            assert!(
                rectangle_relation(&pair, &bounds).is_some(),
                "{name}: rectangle proof"
            );
            assert_eq!(relate(&left_plan, &right_plan).unwrap(), oracle, "{name}");
            assert_eq!(
                evaluate_mode::<MatrixQuery, false>(&pair, &MatrixQuery).unwrap(),
                oracle,
                "{name}: saturation-off",
            );
            for predicate in Predicate::ALL {
                assert_eq!(
                    self::predicate(&left_plan, &right_plan, predicate).unwrap(),
                    matrix_predicates::decided(predicate, oracle),
                    "{name}: {predicate:?}",
                );
            }
            let matrix_pattern =
                CompiledPattern::new(&oracle.text()).expect("matrix text is a valid pattern");
            assert_eq!(
                self::pattern(&left_plan, &right_plan, matrix_pattern).unwrap(),
                oracle.matches(matrix_pattern),
                "{name}: exact matrix pattern",
            );
        }
    }
}

#[test]
fn noncanonical_rectangle_rows_fall_through_to_the_general_evaluator() {
    let box_shape = |minx, miny, maxx, maxy| {
        Shape::Polygon(
            crate::geometry::rect_polygon(
                crate::geometry::Bounds::new(minx, miny, maxx, maxy).unwrap(),
            )
            .unwrap(),
        )
    };
    let cases = [
        (
            "rectangle with a collinear side vertex",
            crate::io::parse_wkt("POLYGON ((0 0,1 0,2 0,2 2,0 2,0 0))").unwrap(),
            box_shape(1.0, 0.5, 3.0, 1.5),
        ),
        (
            "rectangle with a hole",
            crate::io::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))").unwrap(),
            box_shape(1.5, 1.5, 2.5, 2.5),
        ),
        (
            "two-part multipolygon",
            crate::io::parse_wkt("MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((2 0,3 0,3 1,2 1,2 0)))")
                .unwrap(),
            box_shape(0.5, 0.25, 2.5, 0.75),
        ),
        (
            "rectangle against a non-rectangle polygon",
            box_shape(0.0, 0.0, 2.0, 2.0),
            crate::io::parse_wkt("POLYGON ((1 -1,3 1,1 3,-1 1,1 -1))").unwrap(),
        ),
    ];

    for (name, left, right) in cases {
        let left_plan = PreparedPlan::transient(&left);
        let right_plan = PreparedPlan::transient(&right);
        let pair = [
            Operand::from_plan(&left_plan),
            Operand::from_plan(&right_plan),
        ];
        let bounds = [pair[0].bounds(), pair[1].bounds()];
        assert!(
            rectangle_relation(&pair, &bounds).is_none(),
            "{name}: expected general evaluator route",
        );
        let oracle = partition_oracle(&left, &right).unwrap();
        assert_eq!(relate(&left_plan, &right_plan).unwrap(), oracle, "{name}");
        for predicate in Predicate::ALL {
            assert_eq!(
                self::predicate(&left_plan, &right_plan, predicate).unwrap(),
                matrix_predicates::decided(predicate, oracle),
                "{name}: {predicate:?}",
            );
        }
    }
}
