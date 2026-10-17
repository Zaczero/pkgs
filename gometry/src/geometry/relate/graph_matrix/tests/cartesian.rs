#[test]
fn convex_pair_relations_match_known_matrices() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let ccw_outer = Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0));
    let cw_outer = Shape::Polygon(polygon(&[
        [0.0, 0.0],
        [0.0, 4.0],
        [4.0, 4.0],
        [4.0, 0.0],
        [0.0, 0.0],
    ]));
    let ccw_inner = Shape::Polygon(rectangle(1.0, 1.0, 2.0, 2.0));
    let cw_inner = Shape::Polygon(polygon(&[
        [1.0, 1.0],
        [1.0, 2.0],
        [2.0, 2.0],
        [2.0, 1.0],
        [1.0, 1.0],
    ]));
    let crossing = Shape::Polygon(rectangle(-1.0, 1.0, 5.0, 3.0));
    let disjoint = Shape::Polygon(rectangle(8.0, 0.0, 9.0, 1.0));
    let huge_crossing = Shape::Polygon(rectangle(1.5e150, 1.5e150, 2.5e150, 2.5e150));
    let huge_base = Shape::Polygon(rectangle(1.0e150, 1.0e150, 2.0e150, 2.0e150));
    let tiny_crossing = Shape::Polygon(rectangle(1.5e-150, 1.5e-150, 2.5e-150, 2.5e-150));
    let tiny_base = Shape::Polygon(rectangle(1.0e-150, 1.0e-150, 2.0e-150, 2.0e-150));
    let cancellation = Shape::Polygon(polygon(&[
        [0.0, 0.0],
        [1.0, 1.0 + exact_power_of_two(-27)],
        [1.0 - exact_power_of_two(-27), 1.0],
        [0.0, 0.0],
    ]));

    for (name, left, right, expected) in [
        ("ccw containment", &ccw_outer, &ccw_inner, "212FF1FF2"),
        ("cw containment", &cw_outer, &cw_inner, "212FF1FF2"),
        ("reverse containment", &cw_inner, &ccw_outer, "2FF1FF212"),
        ("proper crossing", &ccw_outer, &crossing, "212101212"),
        ("clockwise crossing", &cw_outer, &crossing, "212101212"),
        ("disjoint", &ccw_outer, &disjoint, "FF2FF1212"),
        ("huge crossing", &huge_base, &huge_crossing, "212101212"),
        ("tiny crossing", &tiny_base, &tiny_crossing, "212101212"),
        ("exact cancellation", &cancellation, &disjoint, "FF2FF1212"),
    ] {
        assert_relation_case(name, left, right, expected);
    }
}
#[test]
fn collection_boundary_contact_keeps_area_and_line_interior_distinct() {
    let left = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION(LINESTRING(3 0,3 2),POINT(0 0),LINESTRING(1 1,3 1),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((2 0,4 0,4 2,2 2,2 0)),LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))",
    )
    .unwrap();
    let right = crate::io::parse_wkt("GEOMETRYCOLLECTION(LINESTRING(2 1,3 1))").unwrap();
    let left_plan = PreparedPlan::transient(&left);
    let right_plan = PreparedPlan::transient(&right);
    assert_eq!(
        graph_relate_data(&left_plan, &right_plan).unwrap().text(),
        "102FF1FF2"
    );
}
#[test]
fn collapsed_linework_and_polygon_ring_keep_exact_lower_dimensions() {
    let collapsed =
        crate::io::parse_wkt("GEOMETRYCOLLECTION(POLYGON((0 0,1 0,0 0,1 0,0 0)))").unwrap();
    let line = crate::io::parse_wkt("LINESTRING(0 0,1 0)").unwrap();
    let left = PreparedPlan::transient(&collapsed);
    let right = PreparedPlan::transient(&line);
    assert_eq!(
        graph_relate_data(&left, &right).unwrap().text(),
        "1FFF0FFF2"
    );
}
#[test]
fn external_hole_boundary_is_not_polygon_support() {
    let polygon =
        crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0),(5 5,6 5,6 6,5 6,5 5))").unwrap();
    let point = Shape::Point(Point::new_raw_xy(5.0, 5.0));
    let left = PreparedPlan::transient(&polygon);
    let right = PreparedPlan::transient(&point);
    assert_eq!(
        graph_relate_data(&left, &right).unwrap().text(),
        "FF2FF10F2"
    );
}
#[test]
fn geographic_collapsed_shell_keeps_curve_support_outside_an_areal_hole() {
    let shell = polygon(&[[0.0, 0.0], [10.0, 0.0], [5.0, 0.0], [0.0, 0.0]]).shell;
    let hole = rectangle(2.0, -1.0, 8.0, 1.0).shell;
    let trimmed = Shape::Polygon(Polygon::new(shell, vec![hole]));
    let residual = line(&[[0.5, 0.0], [1.5, 0.0]]);
    assert!(graph_relate_shapes_geographic(&trimmed, &residual).is_intersects());
}
#[test]
fn geographic_areal_hole_removes_collapsed_shell_curve_support() {
    let shell = polygon(&[[0.0, 0.0], [10.0, 0.0], [5.0, 0.0], [0.0, 0.0]]).shell;
    let hole = rectangle(2.0, -1.0, 8.0, 1.0).shell;
    let trimmed = Shape::Polygon(Polygon::new(shell, vec![hole]));
    let inside_hole = line(&[[4.0, 0.0], [6.0, 0.0]]);
    assert!(graph_relate_shapes_geographic(&trimmed, &inside_hole).is_disjoint());
}
#[test]
fn geographic_areal_hole_trims_collapsed_shell_point_support() {
    let shell = polygon(&[[1.0, 1.0], [1.0, 1.0], [1.0, 1.0], [1.0, 1.0]]).shell;
    let hole = rectangle(1.0, 1.0, 3.0, 3.0).shell;
    let trimmed = Shape::Polygon(Polygon::new(shell, vec![hole]));
    let point = Shape::Point(Point::new_raw_xy(1.0, 1.0));
    assert!(graph_relate_shapes_geographic(&trimmed, &point).is_disjoint());
}
#[test]
fn ambiguous_and_unsupported_pair_relations_match_known_matrices() {
    let square = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
    let square4 = Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0));
    let far = Shape::Polygon(rectangle(8.0, 0.0, 9.0, 1.0));
    let equal = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
    let edge_touch = Shape::Polygon(rectangle(2.0, 0.0, 4.0, 2.0));
    let vertex_touch = Shape::Polygon(rectangle(2.0, 2.0, 4.0, 4.0));
    let collinear_overlap = Shape::Polygon(rectangle(1.0, 0.0, 3.0, 2.0));
    let hole = Shape::Polygon(donut());
    let concave = Shape::Polygon(polygon(&[
        [0.0, 0.0],
        [4.0, 0.0],
        [4.0, 1.0],
        [1.0, 1.0],
        [1.0, 4.0],
        [0.0, 4.0],
        [0.0, 0.0],
    ]));
    let invalid = Shape::Polygon(polygon(&[
        [0.0, 0.0],
        [3.0, 3.0],
        [0.0, 3.0],
        [3.0, 0.0],
        [0.0, 0.0],
    ]));
    let collapsed = Shape::Polygon(polygon(&[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 0.0]]));
    // Contained triangles whose apex sits on the square's right or left
    // edge. Both chains end a segment at that x, which is the case the
    // linear contact walk cannot settle from the current pair alone.
    let right_edge_apex =
        Shape::Polygon(polygon(&[[1.0, 1.0], [4.0, 2.0], [2.0, 3.0], [1.0, 1.0]]));
    let left_edge_apex = Shape::Polygon(polygon(&[[3.0, 1.0], [3.0, 3.0], [0.0, 2.0], [3.0, 1.0]]));
    let nonstrict = Shape::Polygon(polygon(&[
        [0.0, 0.0],
        [1.0, 0.0],
        [2.0, 0.0],
        [2.0, 2.0],
        [0.0, 2.0],
        [0.0, 0.0],
    ]));

    for (name, left, right, expected) in [
        ("equal", &square, &equal, "2FFF1FFF2"),
        ("edge touch", &square, &edge_touch, "FF2F11212"),
        ("vertex touch", &square, &vertex_touch, "FF2F01212"),
        (
            "collinear overlap",
            &square,
            &collinear_overlap,
            "212111212",
        ),
        ("hole", &hole, &square, "212F11FF2"),
        ("concave", &concave, &square, "212111212"),
        ("nonstrict", &nonstrict, &far, "FF2FF1212"),
        ("right edge apex", &square4, &right_edge_apex, "212F01FF2"),
        ("left edge apex", &square4, &left_edge_apex, "212F01FF2"),
        (
            "right edge apex reversed",
            &right_edge_apex,
            &square4,
            "2FF10F212",
        ),
        (
            "left edge apex reversed",
            &left_edge_apex,
            &square4,
            "2FF10F212",
        ),
    ] {
        assert_relation_case(name, left, right, expected);
    }
    let collapsed_left = PreparedPlan::transient(&collapsed);
    let square_right = PreparedPlan::transient(&square);
    assert_eq!(
        graph_relate_data(&collapsed_left, &square_right)
            .unwrap()
            .text(),
        "F1FF0F212"
    );
    let invalid_left = PreparedPlan::transient(&invalid);
    assert_eq!(
        graph_relate_data(&invalid_left, &square_right)
            .unwrap()
            .text(),
        "212111212"
    );
}
#[test]
fn two_unique_point_ring_relates_as_linework() {
    let a = Point::new_raw_xy(10.0, 10.0);
    let b = Point::new_raw_xy(11.0, 10.0);
    // `Ring::from_closed` is the internal transport constructor. The final
    // `a` is the stored closing duplicate for the raw A-B-A ring.
    let two_unique = Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&[a, b, a, a]).unwrap()).unwrap(),
        Vec::new(),
    ));
    let square = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
    let left = PreparedPlan::transient(&two_unique);
    let right = PreparedPlan::transient(&square);
    let forward = graph_relate_data(&left, &right).unwrap();
    assert_eq!(forward.text(), "FF1FF0212");

    let reverse_left = PreparedPlan::transient(&square);
    let reverse_right = PreparedPlan::transient(&two_unique);
    assert_eq!(
        graph_relate_data(&reverse_left, &reverse_right).unwrap().0,
        forward.transpose().0
    );
}
#[test]
fn strict_convexity_rejects_short_rings_but_permissive_convexity_remains() {
    let rings = [
        CoordSeq::try_from_points(&Vec::<Point>::new()).unwrap(),
        CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0)]).unwrap(),
        CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
        ])
        .unwrap(),
    ];
    for ring in rings {
        assert!(predicates::shell_is_convex(&ring));
        assert!(!predicates::shell_is_strictly_convex(&ring));
    }
}
#[test]
fn exact_areal_relation_transposes_for_curated_pairs() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let shell_with_hole = Shape::Polygon(donut());
    let cases = vec![
        (
            "disjoint",
            Shape::Polygon(rectangle(0.0, 0.0, 1.0, 1.0)),
            Shape::Polygon(rectangle(2.0, 0.0, 3.0, 1.0)),
        ),
        (
            "nested",
            Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0)),
            Shape::Polygon(rectangle(1.0, 1.0, 2.0, 2.0)),
        ),
        (
            "equal",
            Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
            Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
        ),
        (
            "overlap",
            Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
            Shape::Polygon(rectangle(1.0, 1.0, 3.0, 3.0)),
        ),
        (
            "shared edge",
            Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
            Shape::Polygon(rectangle(2.0, 0.0, 4.0, 2.0)),
        ),
        (
            "shared corner",
            Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
            Shape::Polygon(rectangle(2.0, 2.0, 4.0, 4.0)),
        ),
        (
            "transversal",
            Shape::Polygon(rectangle(-2.0, -1.0, 2.0, 1.0)),
            Shape::Polygon(polygon(&[
                [-1.0, -2.0],
                [1.0, -2.0],
                [1.0, 2.0],
                [-1.0, 2.0],
                [-1.0, -2.0],
            ])),
        ),
        (
            "inside hole",
            shell_with_hole.clone(),
            Shape::Polygon(rectangle(4.0, 4.0, 6.0, 6.0)),
        ),
        (
            "crosses hole",
            shell_with_hole,
            Shape::Polygon(rectangle(2.0, 2.0, 5.0, 5.0)),
        ),
        (
            "collinear overlap",
            Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
            Shape::Polygon(rectangle(1.0, 0.0, 3.0, 2.0)),
        ),
        (
            "t junction",
            Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
            Shape::Polygon(polygon(&[
                [2.0, -1.0],
                [4.0, -1.0],
                [4.0, 1.0],
                [2.0, 1.0],
                [2.0, 0.0],
                [2.0, -1.0],
            ])),
        ),
        (
            "repeated vertex",
            Shape::Polygon(polygon(&[
                [0.0, 0.0],
                [3.0, 0.0],
                [3.0, 3.0],
                [0.0, 3.0],
                [3.0, 0.0],
                [0.0, 0.0],
            ])),
            Shape::Polygon(rectangle(1.0, -1.0, 2.0, 4.0)),
        ),
        (
            "general position",
            Shape::Polygon(polygon(&[
                [0.1, 0.2],
                [3.7, 0.4],
                [2.9, 3.6],
                [-0.3, 2.1],
                [0.1, 0.2],
            ])),
            Shape::Polygon(polygon(&[[1.2, -0.4], [4.1, 1.3], [1.8, 4.2], [1.2, -0.4]])),
        ),
        (
            "near collinear",
            Shape::Polygon(polygon(&[
                [0.0, 0.0],
                [4.0, 1e-12],
                [4.0, 2.0],
                [0.0, 2.0],
                [0.0, 0.0],
            ])),
            Shape::Polygon(rectangle(1.0, -1.0, 3.0, 1.0)),
        ),
        (
            "exact sign near cancellation",
            Shape::Polygon(polygon(&[
                [0.0, 0.0],
                [1.0, 1.0 + exact_power_of_two(-27)],
                [1.0 - exact_power_of_two(-27), 1.0],
                [0.0, 0.0],
            ])),
            Shape::Polygon(rectangle(3.0, 0.0, 4.0, 1.0)),
        ),
        (
            "huge finite",
            Shape::Polygon(rectangle(1e150, 1e150, 2e150, 2e150)),
            Shape::Polygon(rectangle(1.5e150, 1.5e150, 2.5e150, 2.5e150)),
        ),
        (
            "tiny finite",
            Shape::Polygon(rectangle(1e-150, 1e-150, 2e-150, 2e-150)),
            Shape::Polygon(rectangle(1.5e-150, 1.5e-150, 2.5e-150, 2.5e-150)),
        ),
    ];
    for (name, left, right) in cases {
        assert_exact_relation_transposes(name, &left, &right);
    }

    let adjacent = Shape::MultiPolygon(
        vec![rectangle(0.0, 0.0, 2.0, 2.0), rectangle(2.0, 0.0, 4.0, 2.0)].into(),
    );
    let overlapping = Shape::MultiPolygon(
        vec![rectangle(0.0, 0.0, 3.0, 3.0), rectangle(1.0, 1.0, 4.0, 4.0)].into(),
    );
    let nested = Shape::MultiPolygon(
        vec![rectangle(0.0, 0.0, 5.0, 5.0), rectangle(1.0, 1.0, 2.0, 2.0)].into(),
    );
    let duplicate = Shape::MultiPolygon(
        vec![rectangle(0.0, 0.0, 3.0, 3.0), rectangle(0.0, 0.0, 3.0, 3.0)].into(),
    );
    let pinched = Shape::geometry_collection(vec![
        Shape::Polygon(polygon(&[[0.0, 0.0], [-2.0, -1.0], [-2.0, 1.0], [
            0.0, 0.0,
        ]])),
        Shape::Polygon(polygon(&[[0.0, 0.0], [2.0, 1.0], [2.0, -1.0], [0.0, 0.0]])),
    ])
    .expect("test collection is valid");
    let polygon_only_collection = Shape::geometry_collection(vec![
        Shape::Polygon(rectangle(10.0, 0.0, 12.0, 2.0)),
        Shape::Polygon(rectangle(12.0, 0.0, 14.0, 2.0)),
        Shape::Polygon(rectangle(20.0, 0.0, 23.0, 3.0)),
        Shape::Polygon(rectangle(21.0, 1.0, 24.0, 4.0)),
        Shape::Polygon(rectangle(30.0, 0.0, 35.0, 5.0)),
        Shape::Polygon(rectangle(31.0, 1.0, 32.0, 2.0)),
        Shape::Polygon(rectangle(40.0, 0.0, 43.0, 3.0)),
        Shape::Polygon(rectangle(40.0, 0.0, 43.0, 3.0)),
        Shape::Polygon(polygon(&[[50.0, 0.0], [48.0, -1.0], [48.0, 1.0], [
            50.0, 0.0,
        ]])),
        Shape::Polygon(polygon(&[[50.0, 0.0], [52.0, 1.0], [52.0, -1.0], [
            50.0, 0.0,
        ]])),
    ])
    .expect("test collection is valid");
    for (name, left, right) in [
        (
            "adjacent multipolygon",
            adjacent,
            Shape::Polygon(rectangle(1.0, 0.5, 3.0, 1.5)),
        ),
        (
            "overlapping multipolygon",
            overlapping,
            Shape::Polygon(rectangle(2.0, 0.0, 5.0, 3.0)),
        ),
        (
            "nested multipolygon",
            nested.clone(),
            Shape::Polygon(rectangle(1.25, 1.25, 1.75, 1.75)),
        ),
        (
            "covered member boundary",
            nested,
            Shape::Polygon(rectangle(1.25, 1.25, 1.75, 1.75)),
        ),
        (
            "duplicate multipolygon",
            duplicate,
            Shape::Polygon(rectangle(1.0, 1.0, 2.0, 2.0)),
        ),
        (
            "point pinched collection",
            pinched,
            Shape::Polygon(rectangle(-0.5, -0.5, 0.5, 0.5)),
        ),
        (
            "polygon-only collection",
            polygon_only_collection,
            Shape::Polygon(rectangle(11.0, -2.0, 51.0, 2.0)),
        ),
    ] {
        assert_exact_relation_transposes(name, &left, &right);
    }
}
#[test]
fn collapsed_cartesian_polygon_relation_uses_exact_partition() {
    let areal = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
    let collapsed = Shape::Polygon(polygon(&[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 0.0]]));
    assert_eq!(
        graph_relate_shapes(&collapsed, &areal).unwrap().text(),
        "F1FF0F212"
    );
}
#[test]
fn prepared_matrix_matches_shape_matrix_for_curated_pairs() {
    let pairs = [
        (
            Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0)),
            Shape::Polygon(rectangle(2.0, 2.0, 6.0, 6.0)),
        ),
        (
            Shape::Polygon(donut()),
            Shape::Point(Point::new_raw_xy(5.0, 5.0)),
        ),
        (
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(2.0, 2.0),
                    ])
                    .unwrap(),
                )
                .expect("line coordinate sequence must contain at least two vertices"),
            ),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 2.0),
                        Point::new_raw_xy(2.0, 0.0),
                    ])
                    .unwrap(),
                )
                .expect("line coordinate sequence must contain at least two vertices"),
            ),
        ),
    ];
    for (left, right) in pairs {
        let expected = graph_relate_shapes(&left, &right).unwrap();
        let left = PreparedPlan::new(left);
        let right = PreparedPlan::new(right);
        assert_eq!(
            graph_relate_data(&left, &right).unwrap().text(),
            expected.text()
        );
    }
}
#[test]
fn empty_graph_keeps_the_nonempty_operand_dimensions() {
    let empty = Shape::Empty(
        crate::geometry::GeometryKind::Polygon,
        crate::geometry::CoordinateAxes::XY,
    );
    let typed_empty = Shape::Empty(
        crate::geometry::GeometryKind::Polygon,
        crate::geometry::CoordinateAxes::XYZM,
    );
    let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 0.0)])
                .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );
    let polygon = Shape::Polygon(rectangle(0.0, 0.0, 1.0, 1.0));
    let mixed = Shape::geometry_collection(vec![
        Shape::Empty(
            crate::geometry::GeometryKind::Point,
            crate::geometry::CoordinateAxes::XYZ,
        ),
        line.clone(),
        polygon.clone(),
    ])
    .unwrap();
    for (empty, other, expected) in [
        (&empty, &point, "FFFFFF0F2"),
        (&empty, &line, "FFFFFF102"),
        (&empty, &polygon, "FFFFFF212"),
        (&empty, &mixed, "FFFFFF212"),
        (&typed_empty, &polygon, "FFFFFF212"),
    ] {
        assert_eq!(graph_relate_shapes(empty, other).unwrap().text(), expected);
        assert_eq!(
            graph_relate_shapes(other, empty).unwrap().text(),
            graph_relate_shapes(empty, other)
                .unwrap()
                .transpose()
                .text()
        );
    }
    assert_eq!(
        graph_relate_shapes(&empty, &empty).unwrap().text(),
        "FFFFFFFF2"
    );
}
#[test]
fn raw_geographic_bounds_guard_handles_shared_depth128_pole_members() {
    let frame = GeographicFrame::crs84();
    let disjoint_bounds = Bounds::new_raw(10.0, -1.0, 20.0, 1.0);
    let mut pole = Shape::Point(Point::new_raw_xy(0.0, 90.0));
    let mut ordinary = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    for _ in 0..128 {
        pole = Shape::geometry_collection(vec![pole.clone(), pole]).unwrap();
        ordinary = Shape::geometry_collection(vec![ordinary.clone(), ordinary]).unwrap();
    }

    assert!(!crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
        Some(&pole),
        disjoint_bounds,
        frame,
    ));
    assert!(crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
        Some(&ordinary),
        disjoint_bounds,
        frame,
    ));
}
