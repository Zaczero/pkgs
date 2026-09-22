use super::*;
use crate::geometry::{LineSeq, Point};

fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
    Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(minx, miny),
                Point::new_raw_xy(maxx, miny),
                Point::new_raw_xy(maxx, maxy),
                Point::new_raw_xy(minx, maxy),
                Point::new_raw_xy(minx, miny),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    )
}

fn donut() -> Polygon {
    Polygon::new(rectangle(0.0, 0.0, 10.0, 10.0).shell, vec![
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(3.0, 3.0),
                Point::new_raw_xy(7.0, 3.0),
                Point::new_raw_xy(7.0, 7.0),
                Point::new_raw_xy(3.0, 7.0),
                Point::new_raw_xy(3.0, 3.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    ])
}

fn polygon(points: &[[f64; 2]]) -> Polygon {
    Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&[x, y]| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    )
}

fn radial_polygon(vertex_count: usize) -> Polygon {
    let mut points: Vec<_> = (0..vertex_count)
        .map(|index| {
            let angle = 2.0 * std::f64::consts::PI * index as f64 / vertex_count as f64;
            let scale = 1.0 + 0.3 * (3.0 * angle).sin() + 0.08 * (7.0 * angle).cos();
            Point::new_raw_xy(10.0 * scale * angle.cos(), 10.0 * scale * angle.sin())
        })
        .collect();
    points.push(points[0]);
    Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    )
}

fn oscillating_line(case: usize) -> Shape {
    let phase = 0.31 * case as f64;
    Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-20.0, -12.0 + 24.0 * phase.sin()),
                Point::new_raw_xy(20.0, 12.0 * phase.cos()),
                Point::new_raw_xy(-18.0, 10.0 * (phase + 0.7).sin()),
                Point::new_raw_xy(18.0, -10.0 * (phase + 0.4).cos()),
            ])
            .unwrap(),
        )
        .expect("oscillating line has four vertices"),
    )
}

fn line(points: &[[f64; 2]]) -> Shape {
    Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&[x, y]| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .expect("line has at least two vertices"),
    )
}

fn assert_polygon_line_case(name: &str, polygon: &Shape, line: &Shape, expected: &str) {
    let polygon_plan = PreparedPlan::transient(polygon);
    let line_plan = PreparedPlan::transient(line);
    let forward = graph_relate_data(&polygon_plan, &line_plan).unwrap();
    assert_eq!(forward.text(), expected, "{name}: known matrix");
    assert_eq!(
        graph_relate_data(&line_plan, &polygon_plan).unwrap().0,
        forward.transpose().0,
        "{name}: transpose"
    );
    assert_eq!(
        graph_relate_shapes(polygon, line).unwrap().0,
        forward.0,
        "{name}: ordinary dispatch"
    );
    assert_eq!(
        graph_relate_shapes(line, polygon).unwrap().0,
        forward.transpose().0,
        "{name}: reverse ordinary dispatch"
    );
}

#[test]
fn geographic_near_vertical_box_keeps_planar_boundary_dimension() {
    let hexagon = Shape::Polygon(polygon(&[
        [1.0, 0.0],
        [0.500_000_000_000_000_1, 0.866_025_403_784_438_6],
        [-0.499_999_999_999_999_8, 0.866_025_403_784_438_7],
        [-1.0, 0.000_000_000_000_000_122_464_679_914_735_32],
        [-0.500_000_000_000_000_4, -0.866_025_403_784_438_4],
        [0.500_000_000_000_000_1, -0.866_025_403_784_438_6],
        [1.0, 0.0],
    ]));
    for (maxx, expected) in [
        (0.49, b"212101212"),
        (0.5, b"212101212"),
        (0.500_000_000_000_000_1, b"212101212"),
        (0.51, b"212101212"),
    ] {
        let rectangle = crate::geometry::geographic_bounds_to_shape(
            Bounds::new(-10.0, -10.0, maxx, 10.0).expect("ordered bounds"),
            crate::crs::info("EPSG:4326")
                .expect("known geographic CRS")
                .geographic_frame()
                .expect("geographic CRS"),
        )
        .expect("geographic rectangle");
        let geographic = graph_relate_shapes_geographic(&hexagon, &rectangle);
        assert_eq!(geographic.0, *expected, "maxx={maxx}");
        assert_eq!(
            graph_relate_shapes_geographic(&rectangle, &hexagon).0,
            geographic.transpose().0,
            "maxx={maxx}: transpose"
        );
        assert_eq!(
            graph_relate_shapes(&hexagon, &rectangle).unwrap().0,
            *expected,
            "maxx={maxx}: planar"
        );
    }
}

fn assert_areal_fast_path(name: &str, left: &Shape, right: &Shape) {
    let expected = graph_relate_shapes(left, right).unwrap();
    let fast = try_areal_relate(left, right)
        .expect("areal fast-path allocation")
        .expect("areal fast-path admission");
    assert_eq!(fast.0, expected.0, "{name}: forward");
    let reverse = try_areal_relate(right, left)
        .expect("reverse areal fast-path allocation")
        .expect("reverse areal fast-path admission");
    assert_eq!(reverse.0, expected.transpose().0, "{name}: reverse");
}

fn assert_relation_case(name: &str, left: &Shape, right: &Shape, expected: &str) {
    let left_plan = PreparedPlan::transient(left);
    let right_plan = PreparedPlan::transient(right);
    let forward = graph_relate_data(&left_plan, &right_plan).unwrap();
    assert_eq!(forward.text(), expected, "{name}: forward");
    assert_eq!(
        graph_relate_data(&right_plan, &left_plan).unwrap().0,
        forward.transpose().0,
        "{name}: transpose"
    );
}

#[test]
fn convex_pair_relations_match_known_matrices() {
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
        [1.0, 1.0 + 2.0_f64.powi(-27)],
        [1.0 - 2.0_f64.powi(-27), 1.0],
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
fn direct_areal_relation_matches_exact_relation() {
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
                [1.0, 1.0 + 2.0_f64.powi(-27)],
                [1.0 - 2.0_f64.powi(-27), 1.0],
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
        assert_areal_fast_path(name, &left, &right);
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
        assert_areal_fast_path(name, &left, &right);
    }
}

#[test]
fn direct_areal_relation_declines_unsupported_operands() {
    let areal = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(2.0, 2.0)])
                .unwrap(),
        )
        .expect("line has two vertices"),
    );
    let zero_length_edge = Shape::Polygon(polygon(&[
        [0.0, 0.0],
        [2.0, 0.0],
        [2.0, 0.0],
        [2.0, 2.0],
        [0.0, 2.0],
        [0.0, 0.0],
    ]));
    for (name, left, right) in [
        ("line member", &areal, &line),
        ("zero length edge", &zero_length_edge, &areal),
    ] {
        assert!(
            try_areal_relate(left, right)
                .expect("areal admission allocation")
                .is_none(),
            "{name}: admission"
        );
    }
    let collapsed = Shape::Polygon(polygon(&[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 0.0]]));
    let collapsed_left = PreparedPlan::transient(&collapsed);
    let areal_right = PreparedPlan::transient(&areal);
    assert_eq!(
        graph_relate_data(&collapsed_left, &areal_right)
            .unwrap()
            .text(),
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
    let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 0.0)])
                .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );
    let polygon = Shape::Polygon(rectangle(0.0, 0.0, 1.0, 1.0));
    for (other, expected) in [
        (&point, "FFFFFF0F2"),
        (&line, "FFFFFF102"),
        (&polygon, "FFFFFF212"),
    ] {
        assert_eq!(graph_relate_shapes(&empty, other).unwrap().text(), expected);
        assert_eq!(
            graph_relate_shapes(other, &empty).unwrap().text(),
            graph_relate_shapes(&empty, other)
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

fn areal_fold(
    left: &Shape,
    right: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Option<De9im>> {
    let sound = |shape: &Shape| {
        shape.bounds().is_some_and(|bounds| {
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(shape), bounds, frame)
        })
    };
    try_areal_relate_geographic(left, || sound(left), right, || sound(right))
}

fn geographic_graph_oracle(
    left: &Shape,
    right: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> De9im {
    let left = PreparedPlan::transient(left);
    let right = PreparedPlan::transient(right);
    let left_cache = crate::geometry::FrameDependentCaches::default();
    let right_cache = crate::geometry::FrameDependentCaches::default();
    let left = GeographicPrepared::try_new(&left, &left_cache, frame)
        .expect("fixture admits")
        .graph()
        .unwrap();
    let right = GeographicPrepared::try_new(&right, &right_cache, frame)
        .expect("fixture admits")
        .graph()
        .unwrap();
    graph_relate(&left, &right).unwrap()
}

fn assert_rectangle_cover_certificate(
    name: &str,
    source: &Shape,
    bounds: Bounds,
    frame: crate::geographic::GeographicFrame,
) {
    let graph = |shape: &Shape| {
        TopologyGraph::geographic(
            crate::geographic::ScalarGeographicView::try_new(frame, shape).expect("fixture admits"),
        )
        .unwrap()
    };
    let source_graph = graph(source);
    let candidate_graph = graph(&Shape::Polygon(rectangle(
        bounds.minx(),
        bounds.miny(),
        bounds.maxx(),
        bounds.maxy(),
    )));
    let expected = graph_relate(&source_graph, &candidate_graph)
        .unwrap()
        .is_covers();
    assert_eq!(
        source_graph.covers_rectangle(bounds),
        Some(expected),
        "{name}"
    );
}

#[test]
fn geographic_rectangle_cover_certificate_matches_graph_relation() {
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let outer = Shape::Polygon(rectangle(0.0, 0.0, 10.0, 10.0));
    let donut = Shape::Polygon(donut());
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 10.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let seam = Shape::Polygon(polygon(&[
        [170.0, -10.0],
        [-170.0, -10.0],
        [-170.0, 10.0],
        [170.0, 10.0],
        [170.0, -10.0],
    ]));
    let polar = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    for (name, source, bounds) in [
        (
            "geographic interior",
            &outer,
            Bounds::new_raw(2.0, 2.0, 8.0, 8.0),
        ),
        (
            "geographic boundary aligned",
            &outer,
            Bounds::new_raw(0.0, 2.0, 8.0, 8.0),
        ),
        (
            "geographic crossing",
            &outer,
            Bounds::new_raw(8.0, 2.0, 12.0, 8.0),
        ),
        (
            "geographic disjoint",
            &outer,
            Bounds::new_raw(12.0, 12.0, 14.0, 14.0),
        ),
        (
            "geographic line",
            &line,
            Bounds::new_raw(2.0, 2.0, 8.0, 8.0),
        ),
        (
            "geographic hole",
            &donut,
            Bounds::new_raw(4.0, 4.0, 6.0, 6.0),
        ),
        (
            "geographic hole crossing",
            &donut,
            Bounds::new_raw(2.0, 2.0, 5.0, 5.0),
        ),
        (
            "geographic seam interior",
            &seam,
            Bounds::new_raw(175.0, -5.0, 179.0, 5.0),
        ),
        (
            "geographic seam exterior",
            &seam,
            Bounds::new_raw(-5.0, -5.0, 5.0, 5.0),
        ),
    ] {
        assert_rectangle_cover_certificate(name, source, bounds, frame);
    }
    assert_rectangle_cover_certificate(
        "geographic polar interior",
        &polar,
        Bounds::new_raw(0.0, 81.0, 1.0, 82.0),
        frame,
    );
}

#[test]
fn rectangle_cover_certificate_rejects_an_exact_hole() {
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let graph = |shape: &Shape| {
        TopologyGraph::geographic(
            crate::geographic::ScalarGeographicView::try_new(frame, shape).expect("fixture admits"),
        )
        .unwrap()
    };
    let bounds = Bounds::new_raw(3.0, 3.0, 7.0, 7.0);
    let source = Shape::Polygon(donut());
    assert_eq!(graph(&source).covers_rectangle(bounds), Some(false));

    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[Point::new_raw_xy(4.0, 5.0), Point::new_raw_xy(6.0, 5.0)])
                .unwrap(),
        )
        .unwrap(),
    );
    let mixed = Shape::geometry_collection(vec![source, line]).unwrap();
    assert_eq!(graph(&mixed).covers_rectangle(bounds), Some(false));
}

#[test]
fn rectangle_cover_certificate_fails_open_for_an_adjacent_float_midpoint() {
    let minx: f64 = 1.0;
    let maxx = minx.next_up();
    let bounds = Bounds::new_raw(minx, 0.0, maxx, 1.0);
    assert_eq!(f64::midpoint(minx, maxx), minx);

    let source = Shape::Polygon(rectangle(minx, 0.0, maxx, 1.0));
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let geographic = TopologyGraph::geographic(
        crate::geographic::ScalarGeographicView::try_new(frame, &source).expect("fixture admits"),
    )
    .unwrap();
    assert_eq!(geographic.covers_rectangle(bounds), None);
}

#[test]
fn rectangle_cover_certificate_fails_open_at_geographic_singularities() {
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let source = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    let graph = TopologyGraph::geographic(
        crate::geographic::ScalarGeographicView::try_new(frame, &source).expect("fixture admits"),
    )
    .unwrap();
    for bounds in [
        Bounds::new_raw(-180.0, 81.0, -179.0, 85.0),
        Bounds::new_raw(0.0, 85.0, 10.0, 90.0),
        Bounds::new_raw(-180.0, -1.0, 180.0, 1.0),
    ] {
        assert_eq!(graph.covers_rectangle(bounds), None);
    }
}

#[test]
fn geographic_areal_relation_uses_the_safe_cartesian_fold() {
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let with_hole = Shape::Polygon(donut());
    let multipolygon = Shape::MultiPolygon(
        vec![
            rectangle(-10.0, -10.0, -6.0, -6.0),
            rectangle(-4.0, -10.0, 0.0, -6.0),
        ]
        .into(),
    );
    for (name, left, right) in [
        (
            "disjoint",
            Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
            Shape::Polygon(rectangle(-16.0, 0.0, -14.0, 2.0)),
        ),
        (
            "nested",
            Shape::Polygon(rectangle(-20.0, 0.0, -10.0, 10.0)),
            Shape::Polygon(rectangle(-18.0, 2.0, -16.0, 4.0)),
        ),
        (
            "overlapping",
            Shape::Polygon(rectangle(-20.0, 0.0, -16.0, 4.0)),
            Shape::Polygon(rectangle(-18.0, 2.0, -14.0, 6.0)),
        ),
        (
            "shared edge",
            Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
            Shape::Polygon(rectangle(-18.0, 0.0, -16.0, 2.0)),
        ),
        (
            "shared corner",
            Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
            Shape::Polygon(rectangle(-18.0, 2.0, -16.0, 4.0)),
        ),
        (
            "hole",
            with_hole,
            Shape::Polygon(rectangle(2.0, 2.0, 5.0, 5.0)),
        ),
        (
            "multipolygon",
            multipolygon,
            Shape::Polygon(rectangle(-8.0, -8.0, -2.0, -4.0)),
        ),
    ] {
        let expected = geographic_graph_oracle(&left, &right, frame);
        assert_eq!(
            areal_fold(&left, &right, frame)
                .expect("safe geographic areal allocation")
                .expect("safe geographic areal admission")
                .0,
            expected.0,
            "{name}: fold"
        );
        let left_plan = PreparedPlan::transient(&left);
        let right_plan = PreparedPlan::transient(&right);
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        assert_eq!(
            graph_relate_data_geographic(
                GeographicPrepared::try_new(&left_plan, &left_cache, frame)
                    .expect("fixture admits"),
                GeographicPrepared::try_new(&right_plan, &right_cache, frame)
                    .expect("fixture admits"),
            )
            .unwrap()
            .0,
            expected.0,
            "{name}: prepared dispatch"
        );
        assert_eq!(
            graph_relate_shapes_geographic(&left, &right).0,
            expected.0,
            "{name}: shape dispatch"
        );
    }
}

#[test]
fn geographic_areal_relation_declines_quotient_aliases() {
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let ordinary = Shape::Polygon(rectangle(10.0, 0.0, 12.0, 2.0));
    let seam = Shape::Polygon(polygon(&[
        [170.0, 0.0],
        [-170.0, 0.0],
        [-170.0, 10.0],
        [170.0, 10.0],
        [170.0, 0.0],
    ]));
    let pole = Shape::Polygon(rectangle(10.0, 80.0, 12.0, 90.0));
    let noncanonical = Shape::Polygon(rectangle(350.0, 0.0, 360.0, 2.0));
    for (name, left) in [
        ("seam", seam),
        ("pole", pole),
        ("longitude 360", noncanonical),
    ] {
        assert!(
            areal_fold(&left, &ordinary, frame)
                .expect("geographic admission allocation")
                .is_none(),
            "{name}: fold admission"
        );
        let expected = geographic_graph_oracle(&left, &ordinary, frame);
        let left_plan = PreparedPlan::transient(&left);
        let right_plan = PreparedPlan::transient(&ordinary);
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        assert_eq!(
            graph_relate_data_geographic(
                GeographicPrepared::try_new(&left_plan, &left_cache, frame)
                    .expect("fixture admits"),
                GeographicPrepared::try_new(&right_plan, &right_cache, frame)
                    .expect("fixture admits"),
            )
            .unwrap()
            .0,
            expected.0,
            "{name}: fallback"
        );
    }
}

#[test]
fn geographic_cap_point_matrices_are_stable() {
    let polygon = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(170.0, 80.0),
                Point::new_raw_xy(-170.0, 80.0),
                Point::new_raw_xy(-170.0, 90.0),
                Point::new_raw_xy(170.0, 90.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    for point in [
        Point::new_raw_xy(180.0, 85.0),
        Point::new_raw_xy(-180.0, 85.0),
        Point::new_raw_xy(179.999, 85.0),
        Point::new_raw_xy(-179.999, 85.0),
    ] {
        assert_eq!(
            graph_relate_shapes_geographic(&polygon, &Shape::Point(point)).text(),
            "0F2FF1FF2"
        );
    }
    for point in [
        Point::new_raw_xy(0.0, 90.0),
        Point::new_raw_xy(90.0, 90.0),
        Point::new_raw_xy(-90.0, 90.0),
    ] {
        assert_eq!(
            graph_relate_shapes_geographic(&polygon, &Shape::Point(point)).text(),
            "FF20F1FF2"
        );
    }
}

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
fn geographic_pole_line_intersects_cap_in_both_orders() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(160.0, 90.0),
                Point::new_raw_xy(180.0, 90.0),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );
    let cap = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    assert!(graph_relate_shapes_geographic(&line, &cap).is_intersects());
    assert!(graph_relate_shapes_geographic(&cap, &line).is_intersects());
}

#[test]
fn geographic_pole_cap_vertex_is_boundary() {
    let cap = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    let pole = Shape::Point(Point::new_raw_xy(0.0, 90.0));
    assert_eq!(
        graph_relate_shapes_geographic(&cap, &pole).text(),
        "FF20F1FF2"
    );
}

#[test]
fn geographic_split_crossing_keeps_boundary_point_relation() {
    let raw = Shape::Polygon(polygon(&[
        [170.0, 40.0],
        [-170.0, 40.0],
        [-170.0, 50.0],
        [170.0, 50.0],
        [170.0, 40.0],
    ]));
    let split = raw.split_antimeridian().expect("crossing polygon splits");
    let point = Shape::Point(Point::new_raw_xy(170.0, 40.0));
    let raw_point_relation = graph_relate_shapes_geographic(&raw, &point).text();
    let split_point_relation = graph_relate_shapes_geographic(&split, &point).text();
    assert_eq!(raw_point_relation, "FF20F1FF2");
    assert_eq!(split_point_relation, raw_point_relation);
    let probe = Shape::Polygon(rectangle(172.0, 42.0, 178.0, 48.0));
    let raw_probe_relation = graph_relate_shapes_geographic(&raw, &probe).text();
    let split_probe_relation = graph_relate_shapes_geographic(&split, &probe).text();
    assert_eq!(raw_probe_relation, "212FF1FF2");
    assert_eq!(split_probe_relation, raw_probe_relation);
}

#[test]
fn geographic_aggregate_contacts_use_quotient_face_locations() {
    let adjacent = Shape::geometry_collection(vec![
        Shape::Polygon(rectangle(0.0, 0.0, 1.0, 1.0)),
        Shape::Polygon(rectangle(1.0, 0.0, 2.0, 1.0)),
    ])
    .expect("test collection is valid");
    let pinch = Shape::geometry_collection(vec![
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(-2.0, -1.0),
                    Point::new_raw_xy(-2.0, 1.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )),
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(2.0, 1.0),
                    Point::new_raw_xy(2.0, -1.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )),
    ])
    .expect("test collection is valid");
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    for (geometry, point, expected) in [
        (&adjacent, Point::new_raw_xy(1.0, 0.5), "0F2FF1FF2"),
        (&adjacent, Point::new_raw_xy(1.0, 0.0), "FF20F1FF2"),
        (&pinch, Point::new_raw_xy(0.0, 0.0), "FF20F1FF2"),
    ] {
        let point = Shape::Point(point);
        assert_eq!(
            graph_relate_shapes_geographic(geometry, &point).text(),
            expected
        );
        let left = PreparedPlan::new(geometry.clone());
        let right = PreparedPlan::new(point);
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        assert_eq!(
            graph_relate_data_geographic(
                GeographicPrepared::try_new(&left, &left_cache, frame).expect("fixture admits"),
                GeographicPrepared::try_new(&right, &right_cache, frame).expect("fixture admits"),
            )
            .unwrap()
            .text(),
            expected
        );
    }
}

#[test]
fn geographic_grad_pole_aliases_match_after_native_latitude_admission() {
    let frame = crate::crs::info("EPSG:4807")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let mut raw_pole: f64 = 100.0;
    for _ in 0..7 {
        raw_pole = raw_pole.next_up();
    }
    assert_eq!(raw_pole.to_bits(), frame.pole().to_bits());
    let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(0.0, frame.pole())));
    let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(200.0, frame.pole())));
    let left_cache = crate::geometry::FrameDependentCaches::default();
    let right_cache = crate::geometry::FrameDependentCaches::default();
    assert!(
        graph_relate_data_geographic(
            GeographicPrepared::try_new(&left, &left_cache, frame).expect("fixture admits"),
            GeographicPrepared::try_new(&right, &right_cache, frame).expect("fixture admits"),
        )
        .unwrap()
        .is_intersects()
    );
}

#[test]
fn mixed_relation_node_locations_match_full_location_oracle() {
    let left = Shape::geometry_collection(vec![
        Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(-1.0, 1.0),
                    Point::new_raw_xy(1.0, 1.0),
                    Point::new_raw_xy(3.0, 1.0),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
        Shape::Point(Point::new_raw_xy(1.0, 1.0)),
    ])
    .expect("test collection is valid");
    let right = Shape::geometry_collection(vec![
        Shape::Polygon(rectangle(1.0, -1.0, 3.0, 1.0)),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(1.0, 1.0),
                    Point::new_raw_xy(1.0, 3.0),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
    ])
    .expect("test collection is valid");
    let geographic_left = Shape::geometry_collection(vec![
        Shape::Polygon(polygon(&[
            [170.0, 0.0],
            [-170.0, 0.0],
            [-170.0, 10.0],
            [170.0, 10.0],
            [170.0, 0.0],
        ])),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(175.0, 5.0),
                    Point::new_raw_xy(180.0, 5.0),
                    Point::new_raw_xy(-175.0, 5.0),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
        Shape::Point(Point::new_raw_xy(180.0, 5.0)),
    ])
    .expect("test collection is valid");
    let geographic_right = Shape::geometry_collection(vec![
        Shape::Polygon(rectangle(175.0, -5.0, 179.0, 5.0)),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(180.0, 5.0),
                    Point::new_raw_xy(180.0, 15.0),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
    ])
    .expect("test collection is valid");
    let frame = crate::crs::info("EPSG:4326")
        .expect("EPSG:4326 must be available")
        .geographic_frame()
        .expect("EPSG:4326 must have a geographic frame");
    let graph = |shape: &Shape| {
        TopologyGraph::geographic(
            crate::geographic::ScalarGeographicView::try_new(frame, shape).expect("fixture admits"),
        )
        .unwrap()
    };
    for (left, right) in [(&left, &right), (&geographic_left, &geographic_right)] {
        let left_graph = graph(left);
        let right_graph = graph(right);
        assert_eq!(
            graph_relate(&left_graph, &right_graph).unwrap().text(),
            graph_relate_with_full_node_locations(&left_graph, &right_graph).text(),
        );
    }
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
