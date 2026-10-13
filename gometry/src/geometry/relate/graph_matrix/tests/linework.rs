#[test]
fn oblique_polygon_line_contacts_keep_boundary_interior_points() {
    let polygon = Shape::Polygon(radial_polygon(97));
    for case in [22, 29] {
        let line = oscillating_line(case);
        assert_eq!(
            graph_relate_shapes(&polygon, &line).unwrap().text(),
            "1F20F1102",
            "radial case {case}: polygon/line"
        );
        assert_eq!(
            graph_relate_shapes(&line, &polygon).unwrap().text(),
            "101FF0212",
            "radial case {case}: line/polygon"
        );
        let polygon_plan = PreparedPlan::transient(&polygon);
        let line_plan = PreparedPlan::transient(&line);
        assert_eq!(
            graph_relate_data(&polygon_plan, &line_plan).unwrap().text(),
            "1F20F1102",
            "radial case {case}: prepared polygon/line"
        );
        assert_eq!(
            graph_relate_data(&line_plan, &polygon_plan).unwrap().text(),
            "101FF0212",
            "radial case {case}: prepared line/polygon"
        );
    }
}
#[test]
fn polygon_line_boundary_interior_remains_empty_without_contact() {
    let polygon = Shape::Polygon(rectangle(0.0, 0.0, 10.0, 10.0));
    for (name, line, expected, reverse_expected) in [
        (
            "inside",
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(1.0, 1.0),
                        Point::new_raw_xy(2.0, 2.0),
                        Point::new_raw_xy(3.0, 1.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
            "102FF1FF2",
            "1FF0FF212",
        ),
        (
            "outside",
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(-20.0, -20.0),
                        Point::new_raw_xy(-15.0, -15.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
            "FF2FF1102",
            "FF1FF0212",
        ),
        (
            "endpoint contact",
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(-1.0, 0.0),
                        Point::new_raw_xy(0.0, 0.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
            "FF2F01102",
            "FF1F00212",
        ),
    ] {
        assert_eq!(
            graph_relate_shapes(&polygon, &line).unwrap().text(),
            expected,
            "{name}: polygon/line"
        );
        assert_eq!(
            graph_relate_shapes(&line, &polygon).unwrap().text(),
            reverse_expected,
            "{name}: line/polygon"
        );
    }
}
#[test]
fn polygon_line_relation_covers_line_walk_and_boundary_cases() {
    let polygon = Shape::Polygon(rectangle(0.0, 0.0, 10.0, 10.0));
    for (name, line, expected) in [
        ("crossing", line(&[[-1.0, 5.0], [11.0, 5.0]]), "1F20F1102"),
        ("contained", line(&[[2.0, 2.0], [8.0, 8.0]]), "102FF1FF2"),
        (
            "outside",
            line(&[[-20.0, -20.0], [-15.0, -15.0]]),
            "FF2FF1102",
        ),
        (
            "tangent vertex",
            line(&[[-1.0, 1.0], [1.0, -1.0]]),
            "FF20F1102",
        ),
        (
            "partial collinear",
            line(&[[-1.0, 0.0], [11.0, 0.0]]),
            "FF21F1102",
        ),
        (
            "full boundary",
            line(&[[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [
                0.0, 0.0,
            ]]),
            "FF21FFFF2",
        ),
        (
            "boundary plus exterior tail",
            line(&[
                [0.0, 0.0],
                [5.0, 5.0],
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
                [11.0, 0.0],
            ]),
            "1F210F102",
        ),
    ] {
        assert_polygon_line_case(name, &polygon, &line, expected);
    }

    let donut = Shape::Polygon(donut());
    assert_polygon_line_case(
        "hole inside",
        &donut,
        &line(&[[4.0, 4.0], [6.0, 6.0]]),
        "FF2FF1102",
    );
    assert_polygon_line_case(
        "hole crossing",
        &donut,
        &line(&[[-1.0, 5.0], [11.0, 5.0]]),
        "1F20F1102",
    );

    let zm_polygon = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_vecs(
                vec![0.0, 10.0, 10.0, 0.0, 0.0],
                vec![0.0, 0.0, 10.0, 10.0, 0.0],
                Some(vec![1.0, 2.0, 3.0, 4.0, 1.0]),
                Some(vec![5.0, 6.0, 7.0, 8.0, 5.0]),
            )
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let zm_line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_vecs(
                vec![-1.0, 11.0],
                vec![5.0, 5.0],
                Some(vec![9.0, 10.0]),
                Some(vec![11.0, 12.0]),
            )
            .unwrap(),
        )
        .expect("Z/M line has two vertices"),
    );
    assert_polygon_line_case("Z/M axes", &zm_polygon, &zm_line, "1F20F1102");
}
#[test]
fn a_computed_crossing_node_keeps_its_boundary_provenance() {
    // The line crosses the star's edge (0 4)-(2 2) at (5/3, 7/3), a node
    // the pairing computes. Re-locating that rounded point read it as
    // interior; the edge provenance says boundary, and BI must be 0.
    let star = Shape::Polygon(polygon(&[
        [6.0, 3.0],
        [4.0, 4.0],
        [4.0, 6.0],
        [2.0, 4.0],
        [0.0, 4.0],
        [2.0, 2.0],
        [1.0, 0.0],
        [3.0, 2.0],
        [5.0, 1.0],
        [6.0, 3.0],
    ]));
    let probe = Shape::LineString(
        LineSeq::try_new(CoordSeq::try_from_xy(&[XY::new(1.0, 2.0), XY::new(3.0, 3.0)]).unwrap())
            .unwrap(),
    );
    assert_eq!(
        graph_relate_shapes(&star, &probe).unwrap().text(),
        "1020F1102"
    );
    assert_eq!(
        graph_relate_shapes_geographic(&star, &probe).text(),
        "1020F1102"
    );
}
#[test]
fn a_shared_segment_split_by_the_other_operand_keeps_both_owners() {
    // B doubles back over itself and its own noding splits the segment it
    // shares with A at the crossing; A's unsplit copy is then cut at the
    // same computed point. The pieces are one piece with two owners, and
    // the shared stretch is II at a curve.
    let line = |points: &[(f64, f64)]| {
        let xy: Vec<XY> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_xy(&xy).unwrap()).unwrap())
    };
    let a = line(&[(1.0, 1.0), (2.0, 0.0), (2.0, 0.0), (1.0, 0.0)]);
    let b = line(&[
        (1.0, 1.0),
        (2.0, 0.0),
        (2.0, 1.0),
        (0.0, 0.0),
        (2.0, 1.0),
        (1.0, 2.0),
    ]);
    assert_eq!(graph_relate_shapes(&a, &b).unwrap().text(), "1F1F00102");
    assert_eq!(graph_relate_shapes_geographic(&a, &b).text(), "1F1F00102");
}
#[test]
fn point_polygon_relation_matches_known_matrices() {
    let square = Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0));
    for (name, x, y, expected) in [
        ("inside", 2.0, 2.0, "0F2FF1FF2"),
        ("boundary", 4.0, 2.0, "FF20F1FF2"),
        ("vertex", 0.0, 0.0, "FF20F1FF2"),
        ("outside", 9.0, 9.0, "FF2FF10F2"),
    ] {
        let point = Shape::Point(crate::geometry::Point::new_raw_xy(x, y));
        let left = PreparedPlan::transient(&square);
        let right = PreparedPlan::transient(&point);
        let forward = graph_relate_data(&left, &right).unwrap();
        assert_eq!(forward.text(), expected, "{name} polygon-first");
        let reverse = graph_relate_data(&right, &left).unwrap();
        assert_eq!(
            reverse.text(),
            forward.transpose().text(),
            "{name} point-first is the transpose"
        );
    }
}
