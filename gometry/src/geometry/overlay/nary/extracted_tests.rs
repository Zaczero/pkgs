use super::super::test_support::{
    assert_overlay_matches_full_partition, assert_point_lane_matches_full_partition,
    assert_puntal_points_match_partition, nested_shared_set, point_part, shape_wkb, shapes_wkb,
    shared_set_case, subdivision_wkb, test_line, test_ring, test_square,
};
use crate::geometry::overlay::{OverlayOp, Strictness, overlay_all};
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryKind, LineSeq, Loc, MAX_NESTING, MOrdinate, Point, Polygon,
    Shape, TopologyDomain, ZOrdinate,
};

#[rstest::rstest]
#[case::interior("interior", Point::new_raw_xy(1.0, 1.0), true)]
#[case::shell_boundary("shell boundary", Point::new_raw_xy(0.0, 3.0), true)]
#[case::shell_vertex("shell vertex", Point::new_raw_xy(0.0, 0.0), true)]
#[case::hole_interior("hole interior", Point::new_raw_xy(3.0, 3.0), false)]
#[case::hole_boundary("hole boundary", Point::new_raw_xy(2.0, 3.0), true)]
#[case::exterior("exterior", Point::new_raw_xy(7.0, 3.0), false)]
fn point_overlays_match_full_partition_across_polygon_locations(
    #[case] name: &'static str,
    #[case] point: Point,
    #[case] covered: bool,
) {
    let carrier =
        crate::io::parse_wkt("POLYGON ((0 0,6 0,6 6,0 6,0 0),(2 2,4 2,4 4,2 4,2 2))").unwrap();

    let point_shape = Shape::Point(point);
    assert_puntal_points_match_partition(
        name,
        &point_shape,
        &carrier,
        OverlayOp::Intersection,
        Strictness::Lenient,
    );
    let intersection = overlay_all(
        &[&point_shape, &carrier],
        TopologyDomain::Cartesian,
        OverlayOp::Intersection,
        Strictness::Lenient,
    )
    .unwrap();
    assert_eq!(
        point_part(&intersection).is_some_and(|part| !part.is_empty()),
        covered,
        "{name}: {intersection:?}"
    );
    assert_puntal_points_match_partition(
        name,
        &point_shape,
        &carrier,
        OverlayOp::Difference,
        Strictness::Lenient,
    );
}

#[test]
fn point_set_overlays_match_full_partition_for_duplicates_and_ordinates() {
    let left = Shape::MultiPoint(
        vec![
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(30.0))).unwrap(),
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(30.0))).unwrap(),
            Point::new_axes(2.0, 2.0, ZOrdinate(Some(5.0)), MOrdinate(Some(50.0))).unwrap(),
        ]
        .into(),
    );
    let right = Shape::MultiPoint(
        vec![
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(30.0))).unwrap(),
            Point::new_axes(4.0, 4.0, ZOrdinate(Some(7.0)), MOrdinate(Some(70.0))).unwrap(),
        ]
        .into(),
    );
    for operation in [
        OverlayOp::Intersection,
        OverlayOp::Union,
        OverlayOp::Difference,
        OverlayOp::SymmetricDifference,
    ] {
        assert_puntal_points_match_partition(
            "XYZM point sets with duplicate coordinates",
            &left,
            &right,
            operation,
            Strictness::Lenient,
        );
    }

    let conflicting = Shape::MultiPoint(
        vec![
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(9.0)), MOrdinate(Some(30.0))).unwrap(),
            Point::new_axes(2.0, 2.0, ZOrdinate(Some(5.0)), MOrdinate(Some(50.0))).unwrap(),
        ]
        .into(),
    );
    for strictness in [Strictness::Strict, Strictness::Lenient] {
        assert_puntal_points_match_partition(
            "conflicting duplicate Z",
            &left,
            &conflicting,
            OverlayOp::Union,
            strictness,
        );
    }
}

#[test]
fn puntal_collections_and_empty_operands_match_full_partition_points() {
    let inner = Shape::geometry_collection(vec![
        Shape::MultiPoint(vec![Point::new_raw_xy(1.0, 1.0), Point::new_raw_xy(2.0, 3.0)].into()),
        Shape::Point(Point::new_raw_xy(5.0, 5.0)),
    ])
    .unwrap();
    let points = Shape::geometry_collection(vec![inner.clone(), inner]).unwrap();
    let carrier = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION (POLYGON ((0 0,6 0,6 6,0 6,0 0),(2 2,4 2,4 4,2 4,2 2)), LINESTRING (0 3,6 3))",
    )
    .unwrap();
    for operation in [
        OverlayOp::Intersection,
        OverlayOp::Union,
        OverlayOp::Difference,
        OverlayOp::SymmetricDifference,
    ] {
        assert_puntal_points_match_partition(
            "shared point collection against mixed carrier",
            &points,
            &carrier,
            operation,
            Strictness::Lenient,
        );
    }
    assert_puntal_points_match_partition(
        "shared point collection difference order",
        &carrier,
        &points,
        OverlayOp::Difference,
        Strictness::Lenient,
    );

    let empty_points = Shape::typed_empty(GeometryKind::MultiPoint, CoordinateAxes::XYZM);
    let empty_polygon = Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZM);
    for (left, right, operation) in [
        (&empty_points, &carrier, OverlayOp::Intersection),
        (&empty_points, &empty_polygon, OverlayOp::Intersection),
        (&empty_points, &carrier, OverlayOp::Difference),
        (&empty_points, &carrier, OverlayOp::Union),
        (&empty_points, &carrier, OverlayOp::SymmetricDifference),
    ] {
        assert_puntal_points_match_partition(
            "typed empty point operand",
            left,
            right,
            operation,
            Strictness::Lenient,
        );
    }
    let result = overlay_all(
        &[&empty_points, &empty_polygon],
        TopologyDomain::Cartesian,
        OverlayOp::Intersection,
        Strictness::Lenient,
    )
    .unwrap();
    assert_eq!(
        result,
        Shape::typed_empty(GeometryKind::Point, CoordinateAxes::XYZM)
    );
}

#[test]
fn point_overlays_preserve_the_nonpoint_operand_verbatim() {
    let polygon = crate::io::parse_wkt("POLYGON ((4 0,2 0,0 0,0 4,4 4,4 0))").unwrap();
    assert_ne!(
        shape_wkb(&polygon),
        shape_wkb(&polygon.normalize().unwrap())
    );
    let interior = Shape::Point(Point::new_raw_xy(1.0, 1.0));
    for operation in [
        OverlayOp::Union,
        OverlayOp::SymmetricDifference,
        OverlayOp::Difference,
    ] {
        let result = overlay_all(
            &[&polygon, &interior],
            TopologyDomain::Cartesian,
            operation,
            Strictness::Lenient,
        )
        .unwrap();
        assert_eq!(shape_wkb(&result), shape_wkb(&polygon), "{operation:?}");
    }

    let exterior = Shape::Point(Point::new_raw_xy(8.0, 1.0));
    for operation in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
        let result = overlay_all(
            &[&polygon, &exterior],
            TopologyDomain::Cartesian,
            operation,
            Strictness::Lenient,
        )
        .unwrap();
        let Shape::GeometryCollection(parts) = result else {
            panic!("outside point and polygon produce a mixed result: {result:?}");
        };
        let output_polygon = parts
            .iter()
            .find(|part| matches!(part, Shape::Polygon(_) | Shape::MultiPolygon(_)))
            .expect("polygon result part");
        assert_eq!(
            shape_wkb(output_polygon),
            shape_wkb(&polygon),
            "{operation:?}"
        );
    }

    // An absorbed point with conflicting Z does not re-resolve the stored
    // higher-dimensional operand that passes through the operation.
    let xyz_polygon = crate::io::parse_wkt(
        "POLYGON ZM ((4 0 14 140, 2 0 12 120, 0 0 10 100, 0 4 20 200, 4 4 18 180, 4 0 14 140))",
    )
    .unwrap();
    let conflicting_boundary = Shape::Point(
        Point::new_axes(2.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(120.0))).unwrap(),
    );
    for (left, right, operation) in [
        (&xyz_polygon, &conflicting_boundary, OverlayOp::Union),
        (
            &xyz_polygon,
            &conflicting_boundary,
            OverlayOp::SymmetricDifference,
        ),
        (&xyz_polygon, &conflicting_boundary, OverlayOp::Difference),
    ] {
        let result = overlay_all(
            &[left, right],
            TopologyDomain::Cartesian,
            operation,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(shape_wkb(&result), shape_wkb(&xyz_polygon), "{operation:?}");
    }

    let collection = Shape::geometry_collection(vec![polygon]).unwrap();
    for operation in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
        let result = overlay_all(
            &[&collection, &exterior],
            TopologyDomain::Cartesian,
            operation,
            Strictness::Lenient,
        )
        .unwrap();
        let Shape::GeometryCollection(parts) = result else {
            panic!("outside point and collection produce a mixed result: {result:?}");
        };
        assert!(
            parts
                .iter()
                .any(|part| shape_wkb(part) == shape_wkb(&collection)),
            "{operation:?}: the pass-through collection changed: {parts:?}"
        );
    }
}

#[test]
fn union_all_preserves_a_box_when_an_interior_point_is_absorbed() {
    let box_shape = crate::io::parse_wkt("POLYGON ((4 0,2 0,0 0,0 4,4 4,4 0))").unwrap();
    assert_ne!(
        shape_wkb(&box_shape),
        shape_wkb(&box_shape.normalize().unwrap())
    );
    let point = Shape::Point(Point::new_raw_xy(1.0, 1.0));

    let result = Shape::union_all(&[box_shape.clone(), point], Strictness::Lenient).unwrap();

    assert_eq!(shape_wkb(&result), shape_wkb(&box_shape));
}

#[test]
fn point_union_preserves_a_max_depth_collection_without_exceeding_the_limit() {
    let polygon = crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap();
    let mut collection = polygon.clone();
    for _ in 0..MAX_NESTING {
        collection = Shape::geometry_collection(vec![collection]).unwrap();
    }
    assert_eq!(collection.nesting_depth(), MAX_NESTING);

    let exterior = Shape::Point(Point::new_raw_xy(8.0, 1.0));
    let result = overlay_all(
        &[&collection, &exterior],
        TopologyDomain::Cartesian,
        OverlayOp::Union,
        Strictness::Lenient,
    )
    .unwrap();
    assert_eq!(result.nesting_depth(), MAX_NESTING);
    let Shape::GeometryCollection(parts) = &result else {
        panic!("outside point and maximum-depth collection produce a mixed result: {result:?}");
    };
    let mut parts = parts.iter();
    assert!(matches!(parts.next(), Some(Shape::Point(_))));
    let mut pass_through = parts.next().expect("collection pass-through member");
    assert!(parts.next().is_none());
    assert_eq!(pass_through.nesting_depth(), MAX_NESTING.saturating_sub(1));
    while let Shape::GeometryCollection(children) = pass_through {
        let mut members = children.iter();
        pass_through = members.next().expect("single nested collection member");
        assert!(members.next().is_none());
    }
    assert_eq!(shape_wkb(pass_through), shape_wkb(&polygon));
}

#[test]
fn line_polygon_rounding_keeps_disjoint_line_components_separate() {
    let line = crate::io::parse_wkt(
        "MULTILINESTRING ((0 0,1.1999999999999993 0.39999999999999974), \
         (0.5 -1,0.5 0.16666666666666666))",
    )
    .unwrap();
    let polygon = crate::io::parse_wkt("POLYGON ((-1 -2,1 -2,1 2,-2 3,-1 -2))").unwrap();
    for inputs in [[&line, &polygon], [&polygon, &line]] {
        let result = overlay_all(
            &inputs,
            TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        let Shape::MultiLineString(lines) = result else {
            panic!("intersection must retain the two line components");
        };
        assert_eq!(lines.len(), 2);
        let sloping = lines.first().unwrap().as_coords();
        let vertical = lines.last().unwrap().as_coords();
        assert_eq!(sloping.xs(), &[0.0, 1.0]);
        assert_eq!(vertical.xs(), &[0.5, 0.5]);
        assert_eq!(vertical.ys(), &[-1.0, 0.166_666_666_666_666_66]);
        // Halving these normal binary64 values is exact, so equality would be
        // the other line's endpoint lying on the published sloping chord.
        assert!(*sloping.ys().last().unwrap() * 0.5 > *vertical.ys().last().unwrap());
    }
}

#[test]
fn line_polygon_overlays_match_full_partition_wkb() {
    let square = test_square(0.0, 0.0, 4.0, 4.0);
    let holed = Shape::Polygon(Polygon::new(
        test_ring(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0), (0.0, 0.0)]),
        vec![test_ring(&[
            (3.0, 3.0),
            (5.0, 3.0),
            (5.0, 5.0),
            (3.0, 5.0),
            (3.0, 3.0),
        ])],
    ));
    let cases = [
        (
            "crossing with source vertices",
            square.clone(),
            test_line(&[(-1.0, 2.0), (1.0, 2.0), (3.0, 2.0), (5.0, 2.0)]),
        ),
        (
            "touch at polygon vertex",
            square.clone(),
            test_line(&[(-1.0, -1.0), (0.0, 0.0)]),
        ),
        (
            "collinear polygon edge",
            square.clone(),
            test_line(&[(-1.0, 0.0), (2.0, 0.0), (5.0, 0.0)]),
        ),
        (
            "line entirely inside",
            square.clone(),
            test_line(&[(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]),
        ),
        (
            "line entirely outside",
            square.clone(),
            test_line(&[(-3.0, 5.0), (-2.0, 6.0)]),
        ),
        (
            "line through a hole",
            holed,
            test_line(&[(-1.0, 4.0), (2.0, 4.0), (4.0, 4.0), (9.0, 4.0)]),
        ),
        (
            "nearest-rounded rational crossing",
            Shape::Polygon(Polygon::new(
                test_ring(&[(0.0, 0.0), (5.0, 0.0), (0.0, 5.0), (0.0, 0.0)]),
                Vec::new(),
            )),
            test_line(&[(-1.0, 1.0), (5.0, 4.0)]),
        ),
        (
            "collinear sloped boundary",
            Shape::Polygon(Polygon::new(
                test_ring(&[(0.0, 0.0), (5.0, 5.0), (0.0, 5.0), (0.0, 0.0)]),
                Vec::new(),
            )),
            test_line(&[(-1.0, -1.0), (2.5, 2.5), (6.0, 6.0)]),
        ),
        (
            "collapsed shell trimmed by an areal hole",
            Shape::Polygon(Polygon::new(
                test_ring(&[(0.0, 0.0), (10.0, 0.0), (5.0, 0.0), (0.0, 0.0)]),
                vec![test_ring(&[
                    (2.0, -1.0),
                    (8.0, -1.0),
                    (8.0, 1.0),
                    (2.0, 1.0),
                    (2.0, -1.0),
                ])],
            )),
            test_line(&[(4.0, 0.0), (6.0, 0.0)]),
        ),
        (
            "self-crossing line",
            square.clone(),
            test_line(&[(-1.0, 1.0), (5.0, 5.0), (-1.0, 5.0), (5.0, 1.0)]),
        ),
        (
            "reversed overlapping line segments",
            square.clone(),
            test_line(&[(-1.0, 2.0), (3.0, 2.0), (-1.0, 2.0), (5.0, 2.0)]),
        ),
        (
            "tangent vertex",
            square,
            test_line(&[(-1.0, 1.0), (1.0, -1.0)]),
        ),
        (
            "nonrectangular tangent vertex",
            Shape::Polygon(Polygon::new(
                test_ring(&[(0.0, 0.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)]),
                Vec::new(),
            )),
            test_line(&[(-1.0, 1.0), (1.0, -1.0)]),
        ),
    ];
    for (name, polygon, line) in cases {
        for (left, right) in [(&line, &polygon), (&polygon, &line)] {
            assert_overlay_matches_full_partition(
                name,
                left,
                right,
                OverlayOp::Intersection,
                Strictness::Lenient,
            );
        }
        assert_overlay_matches_full_partition(
            name,
            &line,
            &polygon,
            OverlayOp::Difference,
            Strictness::Lenient,
        );
    }

    let xyzm_polygon = crate::io::parse_wkt(
        "POLYGON ZM ((0 0 10 100, 4 0 10 100, 4 4 10 100, 0 4 10 100, 0 0 10 100))",
    )
    .unwrap();
    let xyzm_line = crate::io::parse_wkt("LINESTRING ZM (-1 2 1 11, 5 2 5 15)").unwrap();
    for strictness in [Strictness::Strict, Strictness::Lenient] {
        assert_overlay_matches_full_partition(
            "XYZM line crossing",
            &xyzm_line,
            &xyzm_polygon,
            OverlayOp::Intersection,
            strictness,
        );
        assert_overlay_matches_full_partition(
            "XYZM line difference",
            &xyzm_line,
            &xyzm_polygon,
            OverlayOp::Difference,
            strictness,
        );
    }

    let xyzm_overlapping_line =
        crate::io::parse_wkt("LINESTRING ZM (-1 2 0 10, 3 2 4 20, -1 2 8 30, 5 2 12 40)").unwrap();
    for strictness in [Strictness::Strict, Strictness::Lenient] {
        assert_overlay_matches_full_partition(
            "XYZM reversed and partial overlapping line intersection",
            &xyzm_overlapping_line,
            &xyzm_polygon,
            OverlayOp::Intersection,
            strictness,
        );
        assert_overlay_matches_full_partition(
            "XYZM reversed and partial overlapping line difference",
            &xyzm_overlapping_line,
            &xyzm_polygon,
            OverlayOp::Difference,
            strictness,
        );
    }
}

fn assert_degenerate_line_overlays(polygon_name: &str, polygon_wkt: &str) {
    let polygon =
        crate::io::parse_wkt(polygon_wkt).unwrap_or_else(|error| panic!("{polygon_name}: {error}"));
    let lines = [
        ("through", "LINESTRING(-1 2,5 2)"),
        ("along diagonal", "LINESTRING(-1 -1,5 5)"),
        ("along cancelled spur", "LINESTRING(2 3,2 8)"),
        ("along shell edge", "LINESTRING(-1 0,5 0)"),
    ];
    for (line_name, line_wkt) in lines {
        let line =
            crate::io::parse_wkt(line_wkt).unwrap_or_else(|error| panic!("{line_name}: {error}"));
        let name = format!("{polygon_name}, line {line_name}");
        for (left, right) in [(&line, &polygon), (&polygon, &line)] {
            assert_overlay_matches_full_partition(
                &name,
                left,
                right,
                OverlayOp::Intersection,
                Strictness::Lenient,
            );
        }
        assert_overlay_matches_full_partition(
            &name,
            &line,
            &polygon,
            OverlayOp::Difference,
            Strictness::Lenient,
        );
    }
}

fn assert_degenerate_point_overlays(polygon_name: &str, polygon_wkt: &str) {
    let polygon =
        crate::io::parse_wkt(polygon_wkt).unwrap_or_else(|error| panic!("{polygon_name}: {error}"));
    let points = [
        ("on cancelled spur", "POINT(2 5)"),
        ("on diagonal", "POINT(2 2)"),
        ("on touching endpoint", "POINT(0 0)"),
        ("in shell", "POINT(1 1)"),
        ("outside", "POINT(9 9)"),
        (
            "multipoint across support classes",
            "MULTIPOINT((2 5),(2 2),(9 9))",
        ),
    ];
    for (point_name, point_wkt) in points {
        let point =
            crate::io::parse_wkt(point_wkt).unwrap_or_else(|error| panic!("{point_name}: {error}"));
        let name = format!("{polygon_name}, point {point_name}");
        for (left, right) in [(&point, &polygon), (&polygon, &point)] {
            assert_point_lane_matches_full_partition(
                &name,
                left,
                right,
                OverlayOp::Intersection,
                Strictness::Lenient,
            );
        }
        assert_point_lane_matches_full_partition(
            &name,
            &point,
            &polygon,
            OverlayOp::Difference,
            Strictness::Lenient,
        );
        for operation in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
            for (left, right) in [(&point, &polygon), (&polygon, &point)] {
                assert_point_lane_matches_full_partition(
                    &name,
                    left,
                    right,
                    operation,
                    Strictness::Lenient,
                );
            }
        }
        assert_point_lane_matches_full_partition(
            &name,
            &polygon,
            &point,
            OverlayOp::Difference,
            Strictness::Lenient,
        );
    }
}

#[rstest::rstest]
#[case::degenerate_spike("spike", "POLYGON((0 0,4 0,4 4,2 4,2 7,2 4,0 4,0 0))")]
#[case::degenerate_collapsed("collapsed", "POLYGON((0 0,2 2,4 4,0 0))")]
#[case::degenerate_shell_equals_hole(
    "shell equals hole",
    "POLYGON((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))"
)]
#[case::degenerate_bow_tie("bow-tie", "POLYGON((0 0,4 4,4 0,0 4,0 0))")]
#[case::degenerate_twice_wound(
    "twice-wound square",
    "POLYGON((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))"
)]
#[case::degenerate_touching_hole(
    "hole touching shell",
    "POLYGON((0 0,6 0,6 6,0 6,0 0),(0 0,3 2,3 4,0 0))"
)]
#[case::degenerate_external_hole(
    "external hole",
    "POLYGON((0 0,4 0,4 4,0 4,0 0),(10 10,11 10,11 11,10 10))"
)]
#[case::degenerate_empty("empty", "POLYGON EMPTY")]
#[case::degenerate_collapsed_member(
    "multipolygon with collapsed member",
    "MULTIPOLYGON(((0 0,2 2,4 4,0 0)),((5 5,7 5,7 7,5 7,5 5)))"
)]
fn degenerate_overlays_match_full_partition_wkb(
    #[case] name: &str,
    #[case] polygon: &str,
    #[values(false, true)] point_operands: bool,
) {
    if point_operands {
        assert_degenerate_point_overlays(name, polygon);
    } else {
        assert_degenerate_line_overlays(name, polygon);
    }
}

#[test]
fn zero_length_line_difference_matches_full_partition_wkb() {
    let collapsed_line = test_line(&[(1.0, 1.0), (1.0, 1.0)]);
    assert_overlay_matches_full_partition(
        "zero-length line difference",
        &collapsed_line,
        &test_square(0.0, 0.0, 4.0, 4.0),
        OverlayOp::Difference,
        Strictness::Lenient,
    );
}

fn assert_shared_set_overlay_identity(name: &str, depth: usize) {
    let shared = nested_shared_set(shared_set_case(name), depth);
    let expanded = crate::io::parse_wkb(&shape_wkb(&shared))
        .expect("WKB round-trip expands shared members")
        .shape;

    let shared_parts = subdivision_wkb(&shared);
    assert_eq!(
        shared_parts,
        subdivision_wkb(&expanded),
        "{name}, depth {depth}"
    );

    let shared_union =
        Shape::union_all_topo(&[&shared], TopologyDomain::Cartesian, Strictness::Lenient)
            .expect("shared set union succeeds");
    let expanded_union =
        Shape::union_all_topo(&[&expanded], TopologyDomain::Cartesian, Strictness::Lenient)
            .expect("expanded set union succeeds");
    assert_eq!(
        shape_wkb(&shared_union),
        shape_wkb(&expanded_union),
        "{name}, depth {depth}"
    );

    assert_eq!(
        shape_wkb(
            &shared
                .node_with_context(
                    Strictness::Lenient,
                    &crate::geometry::SourceArcContext::cartesian(),
                )
                .unwrap(),
        ),
        shape_wkb(
            &expanded
                .node_with_context(
                    Strictness::Lenient,
                    &crate::geometry::SourceArcContext::cartesian(),
                )
                .unwrap(),
        ),
        "node: {name}, depth {depth}"
    );
    assert_eq!(
        shapes_wkb(
            &Shape::polygonize_all_with_context(
                &[&shared],
                false,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap(),
        ),
        shapes_wkb(
            &Shape::polygonize_all_with_context(
                &[&expanded],
                false,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap(),
        ),
        "polygonize_all: {name}, depth {depth}"
    );
    assert_eq!(
        shapes_wkb(
            &Shape::build_area_all_with_context(
                &[&shared],
                false,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap(),
        ),
        shapes_wkb(
            &Shape::build_area_all_with_context(
                &[&expanded],
                false,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap(),
        ),
        "build_area_all: {name}, depth {depth}"
    );

    let clip =
        crate::io::parse_wkt("POLYGON ((-0.5 -0.5,0.5 -0.5,0.5 0.5,-0.5 0.5,-0.5 -0.5))").unwrap();
    for operation in [
        OverlayOp::Union,
        OverlayOp::Intersection,
        OverlayOp::Difference,
        OverlayOp::SymmetricDifference,
    ] {
        assert_eq!(
            shape_wkb(
                &shared
                    .overlay(&clip, operation, Strictness::Lenient)
                    .unwrap()
            ),
            shape_wkb(
                &expanded
                    .overlay(&clip, operation, Strictness::Lenient)
                    .unwrap()
            ),
            "{operation:?}: {name}, depth {depth}"
        );
    }
}

#[rstest::rstest]
#[case::shared_polygon_set_ops_match_expansion_at_depth_2("polygon", 2)]
#[case::shared_line_set_ops_match_expansion_at_depth_2("line", 2)]
#[case::shared_point_set_ops_match_expansion_at_depth_2("point", 2)]
#[case::shared_holed_polygon_set_ops_match_expansion_at_depth_2("polygon with hole", 2)]
#[case::shared_zm_set_ops_match_expansion_at_depth_2("Z/M points sharing XY", 2)]
#[case::shared_mixed_set_ops_match_expansion_at_depth_2("mixed collection", 2)]
#[case::shared_polygon_set_ops_match_expansion_at_depth_3("polygon", 3)]
#[case::shared_line_set_ops_match_expansion_at_depth_3("line", 3)]
#[case::shared_point_set_ops_match_expansion_at_depth_3("point", 3)]
#[case::shared_holed_polygon_set_ops_match_expansion_at_depth_3("polygon with hole", 3)]
#[case::shared_zm_set_ops_match_expansion_at_depth_3("Z/M points sharing XY", 3)]
#[case::shared_mixed_set_ops_match_expansion_at_depth_3("mixed collection", 3)]
fn shared_set_overlay_identity_matches_expansion(#[case] name: &str, #[case] depth: usize) {
    assert_shared_set_overlay_identity(name, depth);
}

#[rstest::rstest]
#[case::depth_40(40)]
#[case::depth_128(128)]
fn depth_128_shared_set_overlay_finishes_with_correct_support(#[case] depth: usize) {
    let base = crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").unwrap();

    let mut shared = base;
    for _ in 0..depth {
        shared = Shape::geometry_collection(vec![shared.clone(), shared])
            .expect("shared collection depth is admitted");
    }

    let parts = shared
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .expect("deep subdivision succeeds");
    assert!(!parts.is_empty(), "depth {depth}");
    let area = Shape::union_all_topo(&parts, TopologyDomain::Cartesian, Strictness::Lenient)
        .expect("deep subdivision parts union");
    approx::assert_relative_eq!(area.area(), 1.0, max_relative = 64.0 * f64::EPSILON);

    let union = Shape::union_all_topo(&[&shared], TopologyDomain::Cartesian, Strictness::Lenient)
        .expect("deep union succeeds");
    approx::assert_relative_eq!(union.area(), 1.0, max_relative = 64.0 * f64::EPSILON);
}

use crate::geometry::test_support::polygon;

fn frame() -> crate::geographic::GeographicFrame {
    crate::geographic::GeographicFrame::crs84()
}

fn line(points: &[Point]) -> Shape {
    Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(points).unwrap()).unwrap())
}

fn polygon_with_axes(points: &[(f64, f64)], axes: CoordinateAxes, z: f64, m: f64) -> Shape {
    let points = points
        .iter()
        .map(|&(x, y)| {
            Point::new_axes(
                x,
                y,
                ZOrdinate(axes.has_z().then_some(z)),
                MOrdinate(axes.has_m().then_some(m)),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    Shape::Polygon(Polygon::new(
        crate::geometry::Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap())
            .unwrap(),
        Vec::new(),
    ))
}

fn exact_location(shape: &Shape, point: Point) -> Loc {
    exact_location_in_frame(shape, point, frame())
}

fn exact_location_in_frame(
    shape: &Shape,
    point: Point,
    frame: crate::geographic::GeographicFrame,
) -> Loc {
    let plan = crate::geometry::PreparedPlan::transient(shape);
    let cache = crate::geometry::FrameDependentCaches::default();
    let prepared = crate::geometry::GeographicPrepared::try_new(&plan, &cache, frame).unwrap();
    prepared
        .exact_topology()
        .unwrap()
        .locate_point(point)
        .unwrap()
        .geometry
}

fn assert_native_pole_path(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
    pole: crate::geographic::ChartPole,
) {
    let Shape::Polygon(polygon) = shape else {
        panic!("the native pole closure has one shell: {shape:?}");
    };
    let pole_latitude = frame.chart_pole_latitude(pole);
    let summary = crate::geographic::RingSummary::try_new(
        frame,
        &crate::geographic::GeographicRing::from_ring(&polygon.shell),
    )
    .unwrap();
    assert_eq!(summary.chart_turn, 0, "{shape:?}");
    let pole_longitudes: Vec<_> = polygon
        .shell
        .coords()
        .xs()
        .iter()
        .zip(polygon.shell.coords().ys())
        .filter_map(|(&longitude, &latitude)| {
            (latitude.to_bits() == pole_latitude.to_bits()).then_some(longitude)
        })
        .collect();
    let half = frame.longitude().half_period();
    let mut sorted_pole_longitudes = pole_longitudes.clone();
    sorted_pole_longitudes.sort_unstable_by(f64::total_cmp);
    assert_eq!(
        sorted_pole_longitudes,
        [-half, -half / 2.0, 0.0, half / 2.0, half],
        "{shape:?}"
    );
    assert!(
        pole_longitudes
            .windows(2)
            .all(|pair| (pair[1] - pair[0]).abs() < half),
        "pole path contains an ambiguous half-turn: {pole_longitudes:?}"
    );
}

#[test]
fn nary_global_q_preserves_seam_and_disconnected_area_rows() {
    let seam = polygon(&[
        (170.0, -10.0),
        (-170.0, -10.0),
        (-170.0, 10.0),
        (170.0, 10.0),
        (170.0, -10.0),
    ]);
    let distant = polygon(&[
        (30.0, 40.0),
        (32.0, 40.0),
        (32.0, 42.0),
        (30.0, 42.0),
        (30.0, 40.0),
    ]);
    let output = super::super::nary::overlay_all(
        &[&seam, &distant],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap();

    assert_eq!(
        exact_location(&output, Point::new_raw_xy(175.0, 0.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(-175.0, 0.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, 0.0)),
        Loc::Exterior
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(31.0, 41.0)),
        Loc::Interior
    );
}

#[test]
fn nary_intersection_keeps_a_pole_only_contact() {
    let left = polygon(&[(0.0, 90.0), (10.0, 80.0), (-10.0, 80.0), (0.0, 90.0)]);
    let right = polygon(&[(100.0, 90.0), (90.0, 80.0), (110.0, 80.0), (100.0, 90.0)]);
    let output = super::super::nary::overlay_all(
        &[&left, &right],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Intersection,
        Strictness::Strict,
    )
    .unwrap();
    assert!(matches!(output, Shape::Point(_)), "{output:?}");
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(-120.0, 90.0)),
        Loc::Interior
    );
}

#[test]
fn nary_difference_uses_global_face_rows_to_emit_a_hole() {
    let shell = polygon(&[
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (0.0, 0.0),
    ]);
    let first_hole = polygon(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0), (2.0, 2.0)]);
    let second_hole = polygon(&[(6.0, 6.0), (8.0, 6.0), (8.0, 8.0), (6.0, 8.0), (6.0, 6.0)]);
    let output = super::super::nary::overlay_all(
        &[&shell, &first_hole, &second_hole],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Difference,
        Strictness::Strict,
    )
    .unwrap();
    let Shape::Polygon(output) = output else {
        panic!("a nested polygon difference has one holed shell: {output:?}");
    };
    assert_eq!(output.holes.len(), 2);
    assert_eq!(
        exact_location(&Shape::Polygon(output.clone()), Point::new_raw_xy(1.0, 1.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&Shape::Polygon(output.clone()), Point::new_raw_xy(3.0, 3.0)),
        Loc::Exterior
    );
    assert_eq!(
        exact_location(&Shape::Polygon(output), Point::new_raw_xy(7.0, 7.0)),
        Loc::Exterior
    );
}

#[test]
fn nary_line_overlay_selects_exact_spans_for_union_intersection_and_difference() {
    let source = line(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(10.0, 0.0)]);
    let mask = line(&[Point::new_raw_xy(4.0, 0.0), Point::new_raw_xy(6.0, 0.0)]);

    let union = super::super::nary::overlay_all(
        &[&source, &mask],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        exact_location(&union, Point::new_raw_xy(2.0, 0.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&union, Point::new_raw_xy(5.0, 0.0)),
        Loc::Interior
    );

    let intersection = super::super::nary::overlay_all(
        &[&source, &mask],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Intersection,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        exact_location(&intersection, Point::new_raw_xy(5.0, 0.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&intersection, Point::new_raw_xy(2.0, 0.0)),
        Loc::Exterior
    );

    let difference = super::super::nary::overlay_all(
        &[&source, &mask],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Difference,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        exact_location(&difference, Point::new_raw_xy(2.0, 0.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&difference, Point::new_raw_xy(5.0, 0.0)),
        Loc::Exterior
    );
    assert_eq!(
        exact_location(&difference, Point::new_raw_xy(8.0, 0.0)),
        Loc::Interior
    );
}

#[test]
fn nary_point_overlay_selects_points_and_absorbs_them_in_area() {
    let area = polygon(&[
        (-10.0, -10.0),
        (10.0, -10.0),
        (10.0, 10.0),
        (-10.0, 10.0),
        (-10.0, -10.0),
    ]);
    let inside = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let outside = Shape::Point(Point::new_raw_xy(30.0, 30.0));

    let intersection = super::super::nary::overlay_all(
        &[&area, &inside],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Intersection,
        Strictness::Strict,
    )
    .unwrap();
    assert!(matches!(&intersection, Shape::Point(_)), "{intersection:?}");
    assert_eq!(
        exact_location(&intersection, Point::new_raw_xy(0.0, 0.0)),
        Loc::Interior
    );

    let empty = super::super::nary::overlay_all(
        &[&area, &outside],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Intersection,
        Strictness::Strict,
    )
    .unwrap();
    assert!(empty.is_empty());

    let union = super::super::nary::overlay_all(
        &[&area, &inside],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap();
    assert!(matches!(union, Shape::Polygon(_)), "{union:?}");

    let union = super::super::nary::overlay_all(
        &[&area, &outside],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        exact_location(&union, Point::new_raw_xy(30.0, 30.0)),
        Loc::Interior
    );
}

#[test]
fn nary_union_emits_a_horizontal_full_turn_ring_as_its_selected_cap() {
    let cap = polygon(&[
        (0.0, 80.0),
        (90.0, 80.0),
        (180.0, 80.0),
        (-90.0, 80.0),
        (0.0, 80.0),
    ]);
    let output = super::super::nary::overlay_all(
        &[&cap],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, 85.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, 70.0)),
        Loc::Exterior
    );
}

#[test]
fn nary_union_emits_a_full_longitude_axis_band_without_turn_fill() {
    let band = polygon(&[
        (-180.0, -10.0),
        (-90.0, -10.0),
        (0.0, -10.0),
        (90.0, -10.0),
        (180.0, -10.0),
        (180.0, 10.0),
        (90.0, 10.0),
        (0.0, 10.0),
        (-90.0, 10.0),
        (-180.0, 10.0),
        (-180.0, -10.0),
    ]);
    let output = super::super::nary::overlay_all(
        &[&band],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, 0.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, 20.0)),
        Loc::Exterior
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, 80.0)),
        Loc::Exterior
    );
}

#[test]
fn nary_union_emits_a_source_owned_whole_sphere() {
    let middle = polygon(&[
        (0.0, -80.0),
        (360.0, -80.0),
        (360.0, 80.0),
        (0.0, 80.0),
        (0.0, -80.0),
    ]);
    let south_cap = polygon(&[(0.0, -80.0), (120.0, -80.0), (-120.0, -80.0), (0.0, -80.0)]);
    let north_cap = polygon(&[(0.0, 80.0), (120.0, 80.0), (-120.0, 80.0), (0.0, 80.0)]);
    assert_eq!(
        exact_location(&middle, Point::new_raw_xy(0.0, 0.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&south_cap, Point::new_raw_xy(0.0, -85.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&north_cap, Point::new_raw_xy(0.0, 85.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&north_cap, Point::new_raw_xy(0.0, 89.0)),
        Loc::Interior
    );
    let source_union =
        Shape::geometry_collection(vec![south_cap.clone(), middle.clone(), north_cap.clone()])
            .unwrap();
    assert_eq!(
        exact_location(&source_union, Point::new_raw_xy(0.0, 89.0)),
        Loc::Interior
    );
    let one_row = super::super::nary::overlay_all(
        &[&source_union],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        exact_location(&one_row, Point::new_raw_xy(0.0, 89.0)),
        Loc::Interior,
        "{one_row:?}"
    );
    let output = super::super::nary::overlay_all(
        &[&south_cap, &middle, &north_cap],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap();
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, 0.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, 89.0)),
        Loc::Interior,
        "{output:?}"
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(0.0, -89.0)),
        Loc::Interior
    );
    assert_eq!(
        exact_location(&output, Point::new_raw_xy(40.0, 90.0)),
        Loc::Interior
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn t2_notched_intersection_assembles_both_winding_boundaries() {
    let source = polygon(&[
        (-180.0, 5.0),
        (-90.0, 5.0),
        (-10.0, 5.0),
        (-10.0, -10.0),
        (0.0, -10.0),
        (10.0, -10.0),
        (10.0, 5.0),
        (90.0, 5.0),
        (180.0, 5.0),
        (180.0, 90.0),
        (90.0, 90.0),
        (0.0, 90.0),
        (-90.0, 90.0),
        (-180.0, 90.0),
        (-180.0, 5.0),
    ]);
    let band = polygon(&[
        (-180.0, -30.0),
        (-60.0, -30.0),
        (60.0, -30.0),
        (-180.0, -30.0),
        (-180.0, 30.0),
        (60.0, 30.0),
        (-60.0, 30.0),
        (-180.0, 30.0),
        (-180.0, -30.0),
    ]);
    let output = super::super::nary::overlay_all(
        &[&source, &band],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Intersection,
        Strictness::Strict,
    )
    .expect("the admitted explicit cap intersects the native-latitude band");
    let Shape::Polygon(polygon) = &output else {
        panic!("the connected strip has one shell: {output:?}");
    };
    assert!(polygon.holes.is_empty());
    for (point, expected) in [
        ((0.0, -5.0), Loc::Interior),
        ((0.0, 20.0), Loc::Interior),
        ((50.0, 0.0), Loc::Exterior),
        ((0.0, 60.0), Loc::Exterior),
        ((0.0, 90.0), Loc::Exterior),
        ((0.0, -90.0), Loc::Exterior),
        ((50.0, 5.0), Loc::Boundary),
        ((50.0, 30.0), Loc::Boundary),
        ((180.0, 20.0), Loc::Interior),
        ((-180.0, 20.0), Loc::Interior),
    ] {
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(point.0, point.1)),
            expected,
            "point={point:?}"
        );
    }
    let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
    let resolved = crate::crs::ResolvedMetric::from_model(&model).unwrap();
    let crate::crs::ResolvedMetric::Geodesic(metric) = resolved else {
        unreachable!("EPSG:4326 resolves to a geodesic chart metric")
    };
    let parts = crate::geometry::ChartParts::build(&output, &metric).unwrap();
    assert!(
        parts.bvh().is_some(),
        "T2 strip with {} chart edges should enter ChartParts refinement",
        parts.edges.len()
    );
}

#[test]
fn t2_two_ambiguous_selected_cycles_emit_one_pole_free_strip() {
    let lower = polygon(&[
        (-180.0, -20.0),
        (-90.0, 10.0),
        (0.0, -20.0),
        (90.0, 10.0),
        (180.0, -20.0),
        (180.0, 90.0),
        (90.0, 90.0),
        (0.0, 90.0),
        (-90.0, 90.0),
        (-180.0, 90.0),
        (-180.0, -20.0),
    ]);
    let upper = polygon(&[
        (-180.0, -15.0),
        (-90.0, 15.0),
        (0.0, -15.0),
        (90.0, 15.0),
        (180.0, -15.0),
        (180.0, 90.0),
        (90.0, 90.0),
        (0.0, 90.0),
        (-90.0, 90.0),
        (-180.0, 90.0),
        (-180.0, -15.0),
    ]);
    let output = super::super::nary::overlay_all(
        &[&lower, &upper],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Difference,
        Strictness::Strict,
    )
    .expect("the difference retains the bounded strip between both caps");
    let Shape::Polygon(polygon) = &output else {
        panic!("the selected strip has one shell: {output:?}");
    };
    assert!(polygon.holes.is_empty());
    for point in [(0.0, -17.0), (90.0, 12.0), (-90.0, 12.0), (180.0, -17.0)] {
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(point.0, point.1)),
            Loc::Interior,
            "point={point:?}"
        );
    }
    for point in [(0.0, 0.0), (0.0, -30.0), (0.0, 90.0), (0.0, -90.0)] {
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(point.0, point.1)),
            Loc::Exterior,
            "point={point:?}"
        );
    }
}

#[rstest::rstest]
#[case::negative_latitude_5_0(-5.0, -85.0, 85.0)]
#[case::latitude_5_0(5.0, 85.0, -85.0)]
fn t2_equator_crossing_generated_boundary_closes_only_the_selected_pole(
    #[case] latitude: f64,
    #[case] selected: f64,
    #[case] opposite: f64,
) {
    let cap = polygon(&[
        (-180.0, latitude),
        (-90.0, latitude),
        (0.0, latitude),
        (90.0, latitude),
        (180.0, latitude),
        (-180.0, latitude),
    ]);
    let connector = polygon(&[
        (-10.0, -20.0),
        (10.0, -20.0),
        (10.0, 10.0),
        (-10.0, 10.0),
        (-10.0, -20.0),
    ]);
    let output = super::super::nary::overlay_all(
        &[&cap, &connector],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .expect("the selected cap and connector have a valid union");
    let pole = if latitude < 0.0 {
        crate::geographic::ChartPole::South
    } else {
        crate::geographic::ChartPole::North
    };
    assert_native_pole_path(&output, frame(), pole);
    for (point, expected) in [
        ((0.0, selected), Loc::Interior),
        ((0.0, 0.0), Loc::Interior),
        ((50.0, 0.0), Loc::Exterior),
        ((0.0, opposite), Loc::Exterior),
    ] {
        assert_eq!(
            exact_location(&output, Point::new_raw_xy(point.0, point.1)),
            expected,
            "boundary latitude={latitude}, point={point:?}"
        );
    }
}

#[rstest::rstest]
#[case::coordinateaxes_xyz(CoordinateAxes::XYZ)]
#[case::coordinateaxes_xym(CoordinateAxes::XYM)]
#[case::coordinateaxes_xyzm(CoordinateAxes::XYZM)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn t2_synthetic_pole_closure_drops_unsourceable_axes_before_publication(
    #[case] axes: CoordinateAxes,
) {
    let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
    let resolved = crate::crs::ResolvedMetric::from_model(&model).unwrap();
    let crate::crs::ResolvedMetric::Geodesic(metric) = resolved else {
        unreachable!("EPSG:4326 resolves to a geodesic chart metric")
    };
    let metric_access = || Ok(&metric);

    let cap = polygon_with_axes(
        &[
            (-180.0, -5.0),
            (-90.0, -5.0),
            (0.0, -5.0),
            (90.0, -5.0),
            (180.0, -5.0),
            (-180.0, -5.0),
        ],
        axes,
        7.0,
        70.0,
    );
    let connector = polygon_with_axes(
        &[
            (-10.0, -20.0),
            (10.0, -20.0),
            (10.0, 10.0),
            (-10.0, 10.0),
            (-10.0, -20.0),
        ],
        axes,
        11.0,
        110.0,
    );
    let inputs = [&cap, &connector];
    let strict = crate::geometry::overlay_all_with_metric(
        &inputs,
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Strict,
        &metric_access,
    );
    assert!(
        matches!(
            &strict,
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::OrdinateDropped(operation)
                    )
                    if operation.as_ref() == "union"
                ),
        ),
        "{axes:?}: {strict:?}"
    );
    let lenient = crate::geometry::overlay_all_with_metric(
        &inputs,
        TopologyDomain::Geographic(frame()),
        OverlayOp::Union,
        Strictness::Lenient,
        &metric_access,
    )
    .expect("lenient synthetic closures publish a consistent XY shape");
    assert_eq!(lenient.axes(), CoordinateAxes::XY);
}

#[rstest::rstest]
#[case::degrees(frame())]
#[case::radians({
    let radians = crate::geographic::AngularFactor::try_new("radian", 1.0).unwrap();
    crate::geographic::GeographicFrame::try_new(radians, radians, 0.0).unwrap()
})]
#[case::grads({
    let grads = crate::geographic::AngularFactor::try_new("grad", std::f64::consts::PI / 200.0)
        .unwrap();
    crate::geographic::GeographicFrame::try_new(grads, grads, 0.0).unwrap()
})]
fn t2_selected_strip_support_uses_each_native_angular_frame(
    #[case] frame: crate::geographic::GeographicFrame,
) {
    let half = frame.longitude().half_period();
    let pole = frame.pole();
    let source = polygon(&[
        (-half, 0.125 * pole),
        (-half / 2.0, 0.125 * pole),
        (-half / 16.0, 0.125 * pole),
        (-half / 16.0, -0.125 * pole),
        (0.0, -0.125 * pole),
        (half / 16.0, -0.125 * pole),
        (half / 16.0, 0.125 * pole),
        (half / 2.0, 0.125 * pole),
        (half, 0.125 * pole),
        (half, pole),
        (half / 2.0, pole),
        (0.0, pole),
        (-half / 2.0, pole),
        (-half, pole),
        (-half, 0.125 * pole),
    ]);
    let band = polygon(&[
        (-half, -0.25 * pole),
        (-half / 2.0, -0.25 * pole),
        (0.0, -0.25 * pole),
        (half / 2.0, -0.25 * pole),
        (-half, -0.25 * pole),
        (-half, 0.25 * pole),
        (half / 2.0, 0.25 * pole),
        (0.0, 0.25 * pole),
        (-half / 2.0, 0.25 * pole),
        (-half, 0.25 * pole),
        (-half, -0.25 * pole),
    ]);
    let output = super::super::nary::overlay_all(
        &[&source, &band],
        TopologyDomain::Geographic(frame),
        OverlayOp::Intersection,
        Strictness::Strict,
    )
    .unwrap_or_else(|error| panic!("Native frame {frame:?}: {error}"));
    for (point, expected) in [
        ((0.0, -0.0625 * pole), Loc::Interior),
        ((0.0, 0.125 * pole), Loc::Interior),
        ((half / 2.0, 0.0), Loc::Exterior),
        ((0.0, 0.5 * pole), Loc::Exterior),
        ((half, 0.1875 * pole), Loc::Interior),
        ((-half, 0.1875 * pole), Loc::Interior),
    ] {
        assert_eq!(
            exact_location_in_frame(&output, Point::new_raw_xy(point.0, point.1), frame),
            expected,
            "frame={frame:?}, point={point:?}"
        );
    }

    let cap_latitude = -0.0625 * pole;
    let cap = polygon(&[
        (-half, cap_latitude),
        (-half / 2.0, cap_latitude),
        (0.0, cap_latitude),
        (half / 2.0, cap_latitude),
        (half, cap_latitude),
        (-half, cap_latitude),
    ]);
    let connector_half = half / 18.0;
    let connector = polygon(&[
        (-connector_half, -0.25 * pole),
        (connector_half, -0.25 * pole),
        (connector_half, 0.125 * pole),
        (-connector_half, 0.125 * pole),
        (-connector_half, -0.25 * pole),
    ]);
    let cap_output = super::super::nary::overlay_all(
        &[&cap, &connector],
        TopologyDomain::Geographic(frame),
        OverlayOp::Union,
        Strictness::Strict,
    )
    .unwrap_or_else(|error| panic!("Native pole frame {frame:?}: {error}"));
    assert_native_pole_path(&cap_output, frame, crate::geographic::ChartPole::South);
    for (point, expected) in [
        ((0.0, -0.5 * pole), Loc::Interior),
        ((0.0, 0.0), Loc::Interior),
        ((half / 4.0, 0.0), Loc::Exterior),
        ((0.0, 0.5 * pole), Loc::Exterior),
    ] {
        assert_eq!(
            exact_location_in_frame(&cap_output, Point::new_raw_xy(point.0, point.1), frame),
            expected,
            "frame={frame:?}, point={point:?}"
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn nary_seam_intersection_carries_the_original_source_interval() {
    let crossing = line(&[
        Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(10.0))).unwrap(),
        Point::new_axes(-170.0, 0.0, ZOrdinate(Some(20.0)), MOrdinate(Some(30.0))).unwrap(),
    ]);
    let meridian = line(&[
        Point::new_raw_xy(180.0, -1.0),
        Point::new_raw_xy(180.0, 1.0),
    ]);
    let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
    let resolved = crate::crs::ResolvedMetric::from_model(&model).unwrap();
    let crate::crs::ResolvedMetric::Geodesic(metric) = resolved else {
        unreachable!("EPSG:4326 resolves to a geodesic chart metric")
    };
    let metric_access = || Ok(&metric);
    let output = crate::geometry::overlay_all_with_metric(
        &[&crossing, &meridian],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Intersection,
        Strictness::Strict,
        &metric_access,
    )
    .unwrap();
    let Shape::Point(point) = output else {
        panic!("a crossing of two lines is a point: {output:?}");
    };
    assert_eq!(point.z(), Some(10.0));
    assert_eq!(point.m(), Some(20.0));
    assert_eq!(point.y, 0.0);
}

#[test]
fn nary_emission_repairs_distinct_exact_crossings_that_round_together() {
    let sources = Shape::geometry_collection(vec![
        line(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(3.0, 1.0)]),
        line(&[Point::new_raw_xy(1.0, -1.0), Point::new_raw_xy(1.0, 1.0)]),
        line(&[
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(2.0, -1.0 / 3.0),
        ]),
    ])
    .unwrap();
    super::super::nary::overlay_all(
        &[&sources],
        TopologyDomain::Geographic(frame()),
        OverlayOp::Intersection,
        Strictness::Lenient,
    )
    .expect("adjacent floating-point coordinates separate the exact crossings");
}
