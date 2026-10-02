use crate::geometry::overlay::*;
use crate::geometry::*;

fn shared_set_case(name: &str) -> Shape {
    match name {
        "polygon" => crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap(),
        "line" => crate::io::parse_wkt("LINESTRING (0 0,2 0,2 1)").unwrap(),
        "point" => crate::io::parse_wkt("POINT (1 1)").unwrap(),
        "polygon with hole" => {
            crate::io::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))").unwrap()
        },
        "Z/M points sharing XY" => crate::io::parse_wkt(
            "GEOMETRYCOLLECTION (POINT ZM (0 0 1 10), POINT ZM (0 0 2 20), POINT ZM (1 1 3 30))",
        )
        .unwrap(),
        "mixed collection" => Shape::geometry_collection(vec![
            crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap(),
            crate::io::parse_wkt("LINESTRING (-1 1,3 1)").unwrap(),
            crate::io::parse_wkt("POINT (1 1)").unwrap(),
        ])
        .unwrap(),
        _ => unreachable!("known shared-set fixture"),
    }
}

fn nested_shared_set(shape: Shape, depth: usize) -> Shape {
    let mut shared = shape;
    for _ in 0..depth {
        shared = Shape::geometry_collection(vec![shared.clone(), shared])
            .expect("shared collection depth is admitted");
    }
    shared
}

fn shape_wkb(shape: &Shape) -> Vec<u8> {
    crate::io::to_wkb(shape, None, false).expect("shared-set result writes WKB")
}

fn test_line(points: &[(f64, f64)]) -> Shape {
    let points: Vec<_> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(&points).expect("test line coordinates are admitted"),
    ))
}

fn test_square(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
    let points = [
        XY::new(minx, miny),
        XY::new(maxx, miny),
        XY::new(maxx, maxy),
        XY::new(minx, maxy),
        XY::new(minx, miny),
    ];
    Shape::Polygon(Polygon::new(
        Ring::try_from_closed(
            CoordSeq::try_from_xy(&points).expect("test square coordinates are admitted"),
        )
        .expect("test square ring is closed"),
        Vec::new(),
    ))
}

fn test_ring(points: &[(f64, f64)]) -> Ring {
    let points: Vec<_> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    Ring::try_from_closed(
        CoordSeq::try_from_xy(&points).expect("test ring coordinates are admitted"),
    )
    .expect("test ring is closed")
}

fn point_part(shape: &Shape) -> Option<&Shape> {
    match shape {
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Empty(GeometryKind::Point | GeometryKind::MultiPoint, _) => Some(shape),
        Shape::GeometryCollection(collection) => collection.iter().find_map(point_part),
        Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => None,
    }
}

fn assert_puntal_points_match_partition(
    name: &str,
    left: &Shape,
    right: &Shape,
    operation: OverlayOp,
    strictness: Strictness,
) {
    let actual = overlay_all(
        &[left, right],
        TopologyDomain::Cartesian,
        operation,
        strictness,
    );
    let expected =
        super::nary::full_partition_cartesian_overlay(&[left, right], operation, strictness);
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            let actual_points = point_part(&actual).map(shape_wkb);
            let expected_points = point_part(&expected).map(shape_wkb);
            assert_eq!(
                actual_points, expected_points,
                "{name}: {actual:?} != {expected:?}"
            );
        },
        (Err(actual), Err(expected)) => {
            assert_eq!(actual.to_string(), expected.to_string(), "{name}");
        },
        (actual, expected) => {
            panic!("{name}: actual={actual:?}, full partition={expected:?}");
        },
    }
}

fn assert_overlay_matches_full_partition(
    name: &str,
    left: &Shape,
    right: &Shape,
    operation: OverlayOp,
    strictness: Strictness,
) {
    let actual = overlay_all(
        &[left, right],
        TopologyDomain::Cartesian,
        operation,
        strictness,
    );
    let expected =
        super::nary::full_partition_cartesian_overlay(&[left, right], operation, strictness);
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => {
            assert_eq!(
                shape_wkb(&actual),
                shape_wkb(&expected),
                "{name}: {actual:?} != {expected:?}"
            );
        },
        (Err(actual), Err(expected)) => {
            assert_eq!(actual.to_string(), expected.to_string(), "{name}");
        },
        (actual, expected) => {
            panic!("{name}: actual={actual:?}, full partition={expected:?}");
        },
    }
}

fn assert_point_lane_matches_full_partition(
    name: &str,
    left: &Shape,
    right: &Shape,
    operation: OverlayOp,
    strictness: Strictness,
) {
    let actual = overlay_all(
        &[left, right],
        TopologyDomain::Cartesian,
        operation,
        strictness,
    );
    let expected =
        super::nary::full_partition_cartesian_overlay(&[left, right], operation, strictness);
    let (actual, expected) = match (actual, expected) {
        (Ok(actual), Ok(expected)) => (actual, expected),
        (Err(actual), Err(expected)) => {
            assert_eq!(actual.to_string(), expected.to_string(), "{name}");
            return;
        },
        (actual, expected) => {
            panic!("{name}: actual={actual:?}, full partition={expected:?}");
        },
    };

    if operation == OverlayOp::Intersection
        || operation == OverlayOp::Difference && is_puntal_test_shape(left)
    {
        assert_eq!(
            shape_wkb(&actual),
            shape_wkb(&expected),
            "{name}: point-set output differs from full partition"
        );
        return;
    }

    assert!(
        actual.equals(&expected).unwrap(),
        "{name}: point-set result {actual:?} != {expected:?}"
    );

    let retained = match operation {
        OverlayOp::Difference if is_higher_dimensional_test_shape(left) => Some(left),
        OverlayOp::Union | OverlayOp::SymmetricDifference => {
            if is_higher_dimensional_test_shape(left) {
                Some(left)
            } else if is_higher_dimensional_test_shape(right) {
                Some(right)
            } else {
                None
            }
        },
        OverlayOp::Intersection | OverlayOp::Difference => None,
    };
    if let Some(retained) = retained.filter(|shape| !shape.is_empty()) {
        assert!(
            contains_verbatim_test_member(&actual, retained),
            "{name}: retained higher-dimensional operand was not passed through verbatim: {actual:?}"
        );
    }
}

fn is_puntal_test_shape(shape: &Shape) -> bool {
    shape.topological_dimension() == Dimension::Point
}

fn is_higher_dimensional_test_shape(shape: &Shape) -> bool {
    shape.topological_dimension() > Dimension::Point
}

fn contains_verbatim_test_member(shape: &Shape, retained: &Shape) -> bool {
    if shape_wkb(shape) == shape_wkb(retained) {
        return true;
    }
    matches!(shape, Shape::GeometryCollection(parts) if parts.iter().any(|part| contains_verbatim_test_member(part, retained)))
}

fn subdivision_wkb(shape: &Shape) -> Vec<Vec<u8>> {
    shape
        .subdivide(8, false)
        .expect("shared-set subdivision succeeds")
        .iter()
        .map(shape_wkb)
        .collect()
}

fn shapes_wkb(shapes: &[Shape]) -> Vec<Vec<u8>> {
    shapes.iter().map(shape_wkb).collect()
}

#[test]
fn point_overlays_match_full_partition_across_polygon_locations() {
    let carrier =
        crate::io::parse_wkt("POLYGON ((0 0,6 0,6 6,0 6,0 0),(2 2,4 2,4 4,2 4,2 2))").unwrap();
    let cases = [
        ("interior", Point::new_raw_xy(1.0, 1.0), true),
        ("shell boundary", Point::new_raw_xy(0.0, 3.0), true),
        ("shell vertex", Point::new_raw_xy(0.0, 0.0), true),
        ("hole interior", Point::new_raw_xy(3.0, 3.0), false),
        ("hole boundary", Point::new_raw_xy(2.0, 3.0), true),
        ("exterior", Point::new_raw_xy(7.0, 3.0), false),
    ];
    for (name, point, covered) in cases {
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

#[test]
fn degenerate_polygon_line_and_point_overlays_match_full_partition_wkb() {
    let polygons = [
        ("spike", "POLYGON((0 0,4 0,4 4,2 4,2 7,2 4,0 4,0 0))"),
        ("collapsed", "POLYGON((0 0,2 2,4 4,0 0))"),
        (
            "shell equals hole",
            "POLYGON((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
        ),
        ("bow-tie", "POLYGON((0 0,4 4,4 0,0 4,0 0))"),
        (
            "twice-wound square",
            "POLYGON((0 0,4 0,4 4,0 4,0 0,4 0,4 4,0 4,0 0))",
        ),
        (
            "hole touching shell",
            "POLYGON((0 0,6 0,6 6,0 6,0 0),(0 0,3 2,3 4,0 0))",
        ),
        (
            "external hole",
            "POLYGON((0 0,4 0,4 4,0 4,0 0),(10 10,11 10,11 11,10 10))",
        ),
        ("empty", "POLYGON EMPTY"),
        (
            "multipolygon with collapsed member",
            "MULTIPOLYGON(((0 0,2 2,4 4,0 0)),((5 5,7 5,7 7,5 7,5 5)))",
        ),
    ];
    let lines = [
        ("through", "LINESTRING(-1 2,5 2)"),
        ("along diagonal", "LINESTRING(-1 -1,5 5)"),
        ("along cancelled spur", "LINESTRING(2 3,2 8)"),
        ("along shell edge", "LINESTRING(-1 0,5 0)"),
    ];
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

    for (polygon_name, polygon_wkt) in polygons {
        let polygon = crate::io::parse_wkt(polygon_wkt)
            .unwrap_or_else(|error| panic!("{polygon_name}: {error}"));
        for (line_name, line_wkt) in lines {
            let line = crate::io::parse_wkt(line_wkt)
                .unwrap_or_else(|error| panic!("{line_name}: {error}"));
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

        for (point_name, point_wkt) in points {
            let point = crate::io::parse_wkt(point_wkt)
                .unwrap_or_else(|error| panic!("{point_name}: {error}"));
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
        shape_wkb(&shared.node(Strictness::Lenient).unwrap()),
        shape_wkb(&expanded.node(Strictness::Lenient).unwrap()),
        "node: {name}, depth {depth}"
    );
    assert_eq!(
        shapes_wkb(&Shape::polygonize_all(&[&shared], false).unwrap()),
        shapes_wkb(&Shape::polygonize_all(&[&expanded], false).unwrap()),
        "polygonize_all: {name}, depth {depth}"
    );
    assert_eq!(
        shapes_wkb(&Shape::build_area_all(&[&shared], false).unwrap()),
        shapes_wkb(&Shape::build_area_all(&[&expanded], false).unwrap()),
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

macro_rules! shared_set_overlay_identity_tests {
    ($($test:ident => ($name:literal, $depth:literal)),+ $(,)?) => {
        $(
            #[test]
            fn $test() {
                assert_shared_set_overlay_identity($name, $depth);
            }
        )+
    };
}

shared_set_overlay_identity_tests! {
    shared_polygon_set_ops_match_expansion_at_depth_6 => ("polygon", 6),
    shared_line_set_ops_match_expansion_at_depth_6 => ("line", 6),
    shared_point_set_ops_match_expansion_at_depth_6 => ("point", 6),
    shared_holed_polygon_set_ops_match_expansion_at_depth_6 => ("polygon with hole", 6),
    shared_zm_set_ops_match_expansion_at_depth_6 => ("Z/M points sharing XY", 6),
    shared_mixed_set_ops_match_expansion_at_depth_6 => ("mixed collection", 6),
    shared_polygon_set_ops_match_expansion_at_depth_10 => ("polygon", 10),
    shared_line_set_ops_match_expansion_at_depth_10 => ("line", 10),
    shared_point_set_ops_match_expansion_at_depth_10 => ("point", 10),
    shared_holed_polygon_set_ops_match_expansion_at_depth_10 => ("polygon with hole", 10),
    shared_zm_set_ops_match_expansion_at_depth_10 => ("Z/M points sharing XY", 10),
    shared_mixed_set_ops_match_expansion_at_depth_10 => ("mixed collection", 10),
}

#[test]
fn depth_128_shared_set_overlay_finishes_with_correct_support() {
    let base = crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").unwrap();
    for depth in [40, 128] {
        let mut shared = base.clone();
        for _ in 0..depth {
            shared = Shape::geometry_collection(vec![shared.clone(), shared])
                .expect("shared collection depth is admitted");
        }

        let parts = shared
            .subdivide(8, false)
            .expect("deep subdivision succeeds");
        assert!(!parts.is_empty(), "depth {depth}");
        let area = Shape::union_all_topo(&parts, TopologyDomain::Cartesian, Strictness::Lenient)
            .expect("deep subdivision parts union");
        assert_eq!(area.area(), 1.0, "depth {depth}");

        let union =
            Shape::union_all_topo(&[&shared], TopologyDomain::Cartesian, Strictness::Lenient)
                .expect("deep union succeeds");
        assert_eq!(union.area(), 1.0, "depth {depth}");
    }
}

#[test]
fn sourced_noder_orders_extreme_horizontal_cuts_and_keeps_owner() {
    for scale in [2.0_f64.powi(-600), 2.0_f64.powi(600)] {
        let source = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(4.0 * scale, 0.0),
        };
        let cutters = [3.0, 1.0, 2.0].map(|multiple| Segment {
            start: XY::new(multiple * scale, -scale),
            end: XY::new(multiple * scale, scale),
        });
        let (atomic, provenance) =
            self_node_segments_sourced(std::iter::once(source).chain(cutters)).unwrap();
        let source_atoms: Vec<_> = atomic
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.start.y == 0.0 && segment.end.y == 0.0)
            .collect();
        assert_eq!(source_atoms.len(), 4);
        assert_eq!(
            source_atoms
                .iter()
                .map(|(_, segment)| (segment.start.x, segment.end.x))
                .collect::<Vec<_>>(),
            vec![
                (0.0, scale),
                (scale, 2.0 * scale),
                (2.0 * scale, 3.0 * scale),
                (3.0 * scale, 4.0 * scale),
            ]
        );
        for (index, _) in source_atoms {
            assert_eq!(provenance.owners(index), &[(0, false)]);
        }
    }
}

mod binary_areal_overlay_tests {
    use super::*;

    /// The bounds-component partition is an optimization, so the only
    /// evidence it can give is agreement with the unpartitioned lane on
    /// inputs that actually split: several clusters of overlapping blobs
    /// with clear water between them, plus a lone empty input and a lone
    /// point so singleton components of every dimension are exercised.
    #[test]
    fn partitioned_union_matches_the_single_arrangement() {
        use crate::geometry::{Strictness, TopologyDomain};
        let blob = |cx: f64, cy: f64, r: f64| -> Shape {
            let points: Vec<(f64, f64)> = (0..12)
                .map(|i| {
                    let theta = f64::from(i) / 12.0 * std::f64::consts::TAU;
                    (cx + r * theta.cos(), cy + r * theta.sin())
                })
                .chain(std::iter::once((cx + r, cy)))
                .collect();
            Shape::Polygon(polygon(&points))
        };
        let mut inputs: Vec<Shape> = Vec::new();
        // Three clusters, each internally overlapping, far apart from each other.
        for (ox, oy) in [(0.0, 0.0), (100.0, 0.0), (0.0, 100.0)] {
            for k in 0..5 {
                inputs.push(blob(ox + 1.3 * f64::from(k), oy + 0.7 * f64::from(k), 1.5));
            }
        }
        inputs.push(Shape::Point(crate::geometry::Point::new_raw_xy(50.0, 50.0)));
        inputs.push(Shape::empty_polygon());
        for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
            let partitioned = super::nary::overlay_all_with_bounds_certificates(
                &inputs,
                TopologyDomain::Cartesian,
                op,
                Strictness::Lenient,
                true,
            )
            .unwrap();
            let single = super::nary::overlay_all_with_bounds_certificates(
                &inputs,
                TopologyDomain::Cartesian,
                op,
                Strictness::Lenient,
                false,
            )
            .unwrap();
            assert!(
                partitioned.equals(&single).unwrap(),
                "{op:?}: partitioned overlay differs from the single arrangement"
            );
        }
    }

    fn polygon(points: &[(f64, f64)]) -> Polygon {
        Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(
                    &points
                        .iter()
                        .map(|&(x, y)| Point::new(x, y).unwrap())
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )
    }

    fn square(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Shape {
        Shape::Polygon(polygon(&[
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]))
    }

    fn regular_polygon(cx: f64, cy: f64, radius: f64, vertices: usize) -> Shape {
        let mut points: Vec<Point> = (0..vertices)
            .map(|index| {
                let angle = std::f64::consts::TAU * index as f64 / vertices as f64;
                Point::new_raw_xy(cx + radius * angle.cos(), cy + radius * angle.sin())
            })
            .collect();
        points.push(points[0]);
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn zm_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
        let points = [
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]
        .into_iter()
        .map(|(x, y)| {
            Point::new_axes(
                x,
                y,
                ZOrdinate(Some(offset + x + y)),
                MOrdinate(Some(offset - x + y)),
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn m_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
        let points = [
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_axes(x, y, ZOrdinate(None), MOrdinate(Some(offset + x + y))))
        .collect::<Result<Vec<_>>>()
        .unwrap();
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn zm_line(start: (f64, f64), end: (f64, f64)) -> Shape {
        let points = [start, end]
            .into_iter()
            .map(|(x, y)| {
                Point::new_axes(
                    x,
                    y,
                    ZOrdinate(Some(x + y + 10.0)),
                    MOrdinate(Some(y - x + 10.0)),
                )
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap())
                .expect("test line is valid"),
        )
    }

    fn invalid_zm_bowtie() -> Shape {
        let points = [(0.0, 0.0), (2.0, 2.0), (0.0, 2.0), (2.0, 0.0), (0.0, 0.0)]
            .into_iter()
            .map(|(x, y)| {
                Point::new_axes(
                    x,
                    y,
                    ZOrdinate(Some(x + y + 10.0)),
                    MOrdinate(Some(y - x + 10.0)),
                )
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn z_square(minx: f64, miny: f64, maxx: f64, maxy: f64, offset: f64) -> Shape {
        let points = [
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_axes(x, y, ZOrdinate(Some(offset + x + y)), MOrdinate(None)))
        .collect::<Result<Vec<_>>>()
        .unwrap();
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn line_seq(points: &[(f64, f64)]) -> LineSeq {
        LineSeq::try_new(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| Point::new(x, y).unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .expect("test line is valid")
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        Shape::LineString(line_seq(points))
    }

    fn multiline(lines: &[&[(f64, f64)]]) -> Shape {
        Shape::MultiLineString(
            MultiLineString::try_new(
                CoordinateAxes::XY,
                lines
                    .iter()
                    .map(|points| MultiMember::Value(line_seq(points)))
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn disjoint_multiline(count: usize, offset: f64) -> Shape {
        Shape::MultiLineString(
            MultiLineString::try_new(
                CoordinateAxes::XY,
                (0..count)
                    .map(|index| {
                        let x = offset + index as f64 * 2.0;
                        MultiMember::Value(line_seq(&[(x, 0.0), (x, 1.0)]))
                    })
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn point(x: f64, y: f64) -> Shape {
        Shape::Point(Point::new(x, y).unwrap())
    }

    fn manual_left_fold(inputs: &[&Shape], op: OverlayOp) -> Shape {
        let (first, rest) = inputs.split_first().expect("non-empty test inputs");
        let mut accumulated = (*first).clone();
        for input in rest {
            accumulated = accumulated.overlay(input, op, Strictness::Strict).unwrap();
        }
        accumulated
    }

    fn assert_all_points_have_z(shape: &Shape) {
        shape.for_each_point(|point| {
            assert!(point.z().is_some(), "missing Z in {shape:?}");
        });
    }

    fn assert_topologically_equal(left: &Shape, right: &Shape) {
        let delta = left
            .symmetric_difference(right, Strictness::Strict)
            .unwrap();
        assert!(
            delta.is_empty(),
            "left={left:?}\nright={right:?}\ndelta={delta:?}"
        );
    }

    fn wkb(shape: &Shape) -> Vec<u8> {
        crate::io::to_wkb(shape, None, false).expect("overlay WKB")
    }

    fn assert_disjoint_certificate_matches_graph(inputs: &[&Shape], op: OverlayOp) {
        let certified = overlay_all(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        )
        .unwrap();
        let graph = super::super::nary::overlay_all_without_bounds_certificates(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            wkb(&certified),
            wkb(&graph),
            "{}: {certified:?} != {graph:?}",
            op.name()
        );
    }

    /// The overlay engine preserves INPUT VERTICES bit-exactly — the wart
    /// the `i_overlay` path had (it snapped every coordinate to its grid,
    /// perturbing untouched vertices in the 8th decimal).
    #[test]
    fn overlay_preserves_input_vertices_bit_exactly() {
        let left = polygon(&[
            (0.123_456_789_012_345_6, 0.987_654_321_098_765),
            (10.765_432_109_876_5, 0.123_456_789),
            (5.555_555_555_555_5, 9.999_999_999_99),
            (0.123_456_789_012_345_6, 0.987_654_321_098_765),
        ]);
        let right = polygon(&[
            (3.333_333_333_333_3, -1.111_111_111_111),
            (12.222_222_222_222_2, 4.444_444_444_444),
            (4.040_404_040_404, 8.080_808_080_808),
            (3.333_333_333_333_3, -1.111_111_111_111),
        ]);
        let union = binary_areal_overlay(
            std::slice::from_ref(&left),
            std::slice::from_ref(&right),
            OverlayOp::Union,
        )
        .unwrap();
        let output: Vec<Point> = union
            .iter()
            .flat_map(Polygon::rings)
            .flat_map(|ring| ring.iter_coords().collect::<Vec<_>>())
            .collect();
        for original in left.shell.coords().iter_coords() {
            assert!(
                output
                    .iter()
                    .any(|point| point.x.to_bits() == original.x.to_bits()
                        && point.y.to_bits() == original.y.to_bits()),
                "vertex {original:?} not preserved"
            );
        }
    }

    fn assert_disjoint_results_match(inputs: &[&Shape], op: OverlayOp) {
        let certified = overlay_all(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        );
        let forced = super::super::nary::overlay_all_without_bounds_certificates(
            inputs,
            crate::geometry::TopologyDomain::Cartesian,
            op,
            Strictness::Strict,
        );
        match (certified, forced) {
            (Ok(certified), Ok(forced)) => {
                assert_eq!(wkb(&certified), wkb(&forced), "{}", op.name());
            },
            (Err(certified), Err(forced)) => {
                assert_eq!(certified.to_string(), forced.to_string(), "{}", op.name());
            },
            (certified, forced) => panic!(
                "{} diverged: certified={certified:?}, forced={forced:?}",
                op.name()
            ),
        }
    }

    /// Every lower-dimensional contact of two areal operands is the same in
    /// both operand orders: shared boundary runs, isolated touches, a region
    /// plus a separate touch, nested with a shared side, identical operands.
    #[test]
    fn areal_intersection_contacts_are_operand_order_invariant() {
        let cases = [
            (
                "shared edge",
                square(0.0, 0.0, 2.0, 2.0),
                square(2.0, 0.0, 4.0, 2.0),
            ),
            (
                "corner touch",
                square(0.0, 0.0, 2.0, 2.0),
                square(2.0, 2.0, 4.0, 4.0),
            ),
            (
                "edge interior touch",
                square(0.0, 0.0, 2.0, 2.0),
                square(2.0, 1.0, 4.0, 3.0),
            ),
            (
                "two separate shared boundary runs",
                Shape::Polygon(polygon(&[
                    (0.0, 0.0),
                    (4.0, 0.0),
                    (4.0, 1.0),
                    (1.0, 1.0),
                    (1.0, 3.0),
                    (4.0, 3.0),
                    (4.0, 4.0),
                    (0.0, 4.0),
                    (0.0, 0.0),
                ])),
                Shape::Polygon(polygon(&[
                    (2.0, 1.0),
                    (6.0, 1.0),
                    (6.0, 3.0),
                    (2.0, 3.0),
                    (2.0, 1.0),
                ])),
            ),
            (
                "nested sharing a side",
                square(0.0, 0.0, 4.0, 4.0),
                square(0.0, 1.0, 2.0, 3.0),
            ),
            (
                "identical",
                square(0.0, 0.0, 2.0, 2.0),
                square(0.0, 0.0, 2.0, 2.0),
            ),
            (
                "disjoint",
                square(0.0, 0.0, 2.0, 2.0),
                square(3.0, 3.0, 4.0, 4.0),
            ),
            (
                "hole boundary touch",
                Shape::Polygon(Polygon::new(
                    polygon(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]).shell,
                    vec![
                        polygon(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0), (2.0, 2.0)])
                            .shell,
                    ],
                )),
                square(2.0, 2.0, 3.0, 3.0),
            ),
        ];
        for (name, left, right) in cases {
            let forward = intersection(&left, &right);
            let reverse = intersection(&right, &left);
            assert_eq!(wkb(&forward), wkb(&reverse), "{name}");
        }
    }

    #[test]
    fn areal_intersection_preserves_region_and_isolated_point() {
        let left = Shape::geometry_collection(vec![
            square(0.0, 0.0, 2.0, 2.0),
            square(4.0, 4.0, 6.0, 6.0),
        ])
        .unwrap();
        let right = square(1.0, 1.0, 4.0, 4.0);
        let expected = Shape::geometry_collection(vec![
            square(1.0, 1.0, 2.0, 2.0),
            Shape::Point(Point::new_raw_xy(4.0, 4.0)),
        ])
        .unwrap();
        for (left, right) in [(&left, &right), (&right, &left)] {
            let result = intersection(left, right);
            assert!(
                result.equals(&expected).unwrap(),
                "{result:?} != {expected:?}"
            );
        }
    }

    #[test]
    fn collapsed_shell_does_not_emit_an_areal_hole_as_linework() {
        let shell = polygon(&[(0.0, 0.0), (4.0, 0.0), (2.0, 0.0), (0.0, 0.0)]).shell;
        let hole = polygon(&[(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)]).shell;
        let source = Shape::Polygon(Polygon::new(shell, vec![hole]));
        let enclosing = square(-1.0, -1.0, 5.0, 5.0);
        let expected = line(&[(0.0, 0.0), (4.0, 0.0)]);
        for (left, right) in [(&source, &enclosing), (&enclosing, &source)] {
            let result = left.intersection(right, Strictness::Lenient).unwrap();
            assert!(
                result.equals(&expected).unwrap(),
                "{result:?} != {expected:?}"
            );
        }
    }

    #[test]
    fn areal_intersection_on_degenerate_boundaries() {
        let cases: &[(&str, &[(f64, f64)])] = &[
            ("collapsed ring", &[
                (0.0, 0.0),
                (2.0, 0.0),
                (4.0, 0.0),
                (0.0, 0.0),
            ]),
            ("retraced exterior spur", &[
                (0.0, 0.0),
                (2.0, 0.0),
                (4.0, 0.0),
                (2.0, 0.0),
                (2.0, 2.0),
                (0.0, 2.0),
                (0.0, 0.0),
            ]),
        ];
        for &(name, coordinates) in cases {
            let left = Shape::Polygon(polygon(coordinates));
            let right = square(3.0, -1.0, 5.0, 1.0);
            for (left, right) in [(&left, &right), (&right, &left)] {
                let result = intersection(left, right);
                if name == "collapsed ring" {
                    assert!(
                        result.equals(&line(&[(3.0, 0.0), (4.0, 0.0)])).unwrap(),
                        "{result:?}"
                    );
                }
                if name == "retraced exterior spur" {
                    // Opposite traversals cancel under the areal nonzero rule.
                    assert!(result.is_empty(), "{result:?}");
                }
            }
        }
    }

    #[test]
    fn areal_intersection_lane_preserves_zm_and_operand_order() {
        let left = zm_square(0.0, 0.0, 2.0, 2.0, 10.0);
        for right in [
            zm_square(1.0, 1.0, 3.0, 3.0, 20.0),
            zm_square(2.0, 0.0, 4.0, 2.0, 20.0),
            zm_square(2.0, 2.0, 4.0, 4.0, 20.0),
        ] {
            for strictness in [Strictness::Strict, Strictness::Lenient] {
                let fast = overlay_all(
                    &[&left, &right],
                    crate::geometry::TopologyDomain::Cartesian,
                    OverlayOp::Intersection,
                    strictness,
                );
                let reverse = overlay_all(
                    &[&right, &left],
                    crate::geometry::TopologyDomain::Cartesian,
                    OverlayOp::Intersection,
                    strictness,
                );
                match (fast, reverse) {
                    (Ok(fast), Ok(reverse)) => {
                        assert_eq!(wkb(&fast), wkb(&reverse), "{fast:?} != {reverse:?}");
                    },
                    (Err(fast), Err(reverse)) => {
                        assert_eq!(fast.to_string(), reverse.to_string());
                    },
                    (fast, reverse) => panic!("diverged: fast={fast:?} reverse={reverse:?}"),
                }
            }
        }
    }

    fn intersection(left: &Shape, right: &Shape) -> Shape {
        overlay_all(
            &[left, right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap()
    }

    /// Pinch resolution: corner-touching squares union into a VALID
    /// `MultiPolygon` of two simple lobes (never one figure-eight ring),
    /// and the maximal walk's split classifies a shell-touching hole
    /// correctly (the S2 dissolve lens — pinned by the cell-parity
    /// pytest); identical operands xor to nothing.
    #[test]
    fn pinched_overlays_resolve_to_simple_rings() {
        let lower = polygon(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]);
        let upper = polygon(&[(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0), (1.0, 1.0)]);
        let union = binary_areal_overlay(
            std::slice::from_ref(&lower),
            std::slice::from_ref(&upper),
            OverlayOp::Union,
        )
        .unwrap();
        assert_eq!(union.len(), 2, "{union:?}");
        assert!(union.iter().all(|part| part.holes.is_empty()));

        let xor = binary_areal_overlay(
            std::slice::from_ref(&lower),
            std::slice::from_ref(&lower),
            OverlayOp::SymmetricDifference,
        )
        .unwrap();
        assert!(xor.is_empty(), "{xor:?}");
    }

    #[test]
    fn intersection_all_typed_empty_uses_global_min_dimension() {
        let area = z_square(0.0, 0.0, 2.0, 2.0, 3.0);
        let later_disjoint_point = Shape::Point(
            Point::new_axes(10.0, 10.0, ZOrdinate(None), MOrdinate(Some(4.0))).unwrap(),
        );
        let result = overlay_all(
            &[&area, &later_disjoint_point],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(
            matches!(result, Shape::Empty(GeometryKind::Point, _)),
            "{result:?}"
        );
        assert!(result.has_z() && result.has_m(), "{result:?}");
    }

    #[test]
    fn intersection_refines_contained_and_disjoint_inputs() {
        let outer = square(0.0, 0.0, 10.0, 10.0);
        let inner = square(2.0, 2.0, 4.0, 4.0);
        let contained = outer
            .intersection(&inner, Strictness::Strict)
            .expect("contained intersection");
        assert_topologically_equal(&contained, &inner);

        let disjoint = outer
            .intersection(&square(20.0, 20.0, 22.0, 22.0), Strictness::Strict)
            .expect("disjoint intersection");
        assert!(disjoint.is_empty(), "{disjoint:?}");

        // The inner envelope is strictly inside the outer envelope, but the
        // inner polygon lies in the outer polygon's concave notch. A bounds
        // clone would return the wrong non-empty result here.
        let concave = Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (6.0, 10.0),
            (6.0, 4.0),
            (4.0, 4.0),
            (4.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]));
        let notch = square(4.5, 5.0, 5.5, 9.0);
        let refined = concave
            .intersection(&notch, Strictness::Strict)
            .expect("concave refinement");
        assert!(refined.is_empty(), "{refined:?}");
    }

    #[test]
    fn convex_intersection_fast_path_yields_a_polygon() {
        let left = square(0.0, 0.0, 4.0, 4.0);
        let right = Shape::Polygon(polygon(&[
            (2.0, -1.0),
            (5.0, 2.0),
            (2.0, 5.0),
            (-1.0, 2.0),
            (2.0, -1.0),
        ]));
        let fast = intersection(&left, &right);
        assert!(matches!(fast, Shape::Polygon(_)), "{fast:?}");
    }

    #[test]
    fn disjoint_convex_intersection_preserves_canonical_empty_fallback() {
        let cases = [
            (
                "overlapping envelopes",
                regular_polygon(0.0, 0.0, 1.0, 256),
                regular_polygon(1.5, 1.5, 1.0, 256),
            ),
            (
                "disjoint envelopes",
                regular_polygon(0.0, 0.0, 1.0, 256),
                regular_polygon(4.0, 0.0, 1.0, 256),
            ),
        ];
        for (name, left, right) in cases {
            // The first pair makes the candidate decline and must fall through
            // the original exact assembly chain. The second is settled before
            // dispatch by the bounds certificate. Both keep POLYGON EMPTY.
            let fast = intersection(&left, &right);
            assert!(
                matches!(
                    fast,
                    Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XY)
                ),
                "{name}: {fast:?}"
            );
        }
    }

    #[test]
    fn concave_intersection_keeps_the_line_the_areal_shortcut_drops() {
        let concave_left = polygon(&[
            (0.0, 0.0),
            (6.0, 0.0),
            (6.0, 1.0),
            (1.0, 1.0),
            (1.0, 5.0),
            (6.0, 5.0),
            (6.0, 6.0),
            (0.0, 6.0),
            (0.0, 0.0),
        ]);
        let concave_right = polygon(&[
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (5.0, 2.0),
            (5.0, 5.0),
            (0.0, 5.0),
            (0.0, 0.0),
        ]);
        // The area-only binary shortcut is not a valid mixed-dimension
        // intersection oracle: this concave pair also contributes a line.
        let unrestricted = polygon_parts_to_shape(
            binary_areal_overlay(
                std::slice::from_ref(&concave_left),
                std::slice::from_ref(&concave_right),
                OverlayOp::Intersection,
            )
            .unwrap(),
        );
        let concave_left_shape = Shape::Polygon(concave_left);
        let concave_right_shape = Shape::Polygon(concave_right);
        let exact = intersection(&concave_left_shape, &concave_right_shape);
        assert_ne!(wkb(&unrestricted), wkb(&exact));
    }

    #[test]
    fn convex_intersection_rejects_an_invalid_shell() {
        let invalid = Shape::Polygon(polygon(&[
            (0.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (4.0, 0.0),
            (0.0, 0.0),
        ]));
        let cutter = square(1.0, 1.0, 5.0, 5.0);
        let result = intersection(&invalid, &cutter);
        assert!(matches!(result, Shape::MultiPolygon(_)), "{result:?}");
        assert_eq!(result.area(), 4.5);
    }

    #[test]
    fn polygon_line_intersection_boundary_and_hole_cases() {
        let holed = Shape::Polygon(Polygon::new(
            polygon(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ])
            .shell,
            vec![
                Ring::from_closed(
                    CoordSeq::try_from_points(&[
                        Point::new(4.0, 4.0).unwrap(),
                        Point::new(6.0, 4.0).unwrap(),
                        Point::new(6.0, 6.0).unwrap(),
                        Point::new(4.0, 6.0).unwrap(),
                        Point::new(4.0, 4.0).unwrap(),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ],
        ));
        let multipart = Shape::MultiPolygon(
            vec![
                polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                polygon(&[(4.0, 0.0), (6.0, 0.0), (6.0, 2.0), (4.0, 2.0), (4.0, 0.0)]),
            ]
            .into(),
        );
        let cases = [
            (
                "crossing",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-1.0, 2.0), (5.0, 2.0)]),
            ),
            (
                "contained",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(1.0, 1.0), (3.0, 3.0)]),
            ),
            (
                "exterior",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-2.0, 5.0), (-1.0, 5.0)]),
            ),
            (
                "boundary-collinear",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-1.0, 0.0), (5.0, 0.0)]),
            ),
            (
                "vertex-only",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-1.0, -1.0), (0.0, 0.0)]),
            ),
            (
                "tangent",
                square(0.0, 0.0, 4.0, 4.0),
                line(&[(-1.0, 1.0), (1.0, -1.0)]),
            ),
            ("hole", holed.clone(), line(&[(1.0, 5.0), (9.0, 5.0)])),
            ("hole exterior", holed, line(&[(4.5, 5.0), (5.5, 5.0)])),
            ("multipart", multipart, line(&[(-1.0, 1.0), (7.0, 1.0)])),
        ];

        for (name, polygon, line) in cases {
            let result = intersection(&polygon, &line);
            match name {
                "crossing" | "contained" | "boundary-collinear" => {
                    assert!(matches!(result, Shape::LineString(_)), "{name}: {result:?}");
                },
                "exterior" | "hole exterior" => {
                    assert!(
                        matches!(result, Shape::Empty(GeometryKind::LineString, _)),
                        "{name}: {result:?}"
                    );
                },
                "vertex-only" | "tangent" => {
                    assert!(
                        matches!(result, Shape::Point(point) if point.x == 0.0 && point.y == 0.0),
                        "{name}: {result:?}"
                    );
                },
                "hole" | "multipart" => {
                    assert!(
                        matches!(result, Shape::MultiLineString(_)),
                        "{name}: {result:?}"
                    );
                },
                _ => unreachable!("enumerated case"),
            }
        }

        let reverse = intersection(
            &line(&[(-1.0, 2.0), (5.0, 2.0)]),
            &square(0.0, 0.0, 4.0, 4.0),
        );
        assert!(matches!(reverse, Shape::LineString(_)), "{reverse:?}");

        let multi_line = multiline(&[&[(-1.0, 1.0), (5.0, 1.0)], &[(1.0, 3.0), (3.0, 3.0)], &[
            (-2.0, 9.0),
            (-1.0, 9.0),
        ]]);
        let multi_result = intersection(&square(0.0, 0.0, 4.0, 4.0), &multi_line);
        assert!(
            matches!(multi_result, Shape::MultiLineString(_)),
            "{multi_result:?}"
        );

        let zm_result = intersection(
            &zm_square(0.0, 0.0, 4.0, 4.0, 10.0),
            &zm_line((-1.0, 2.0), (5.0, 2.0)),
        );
        assert!(zm_result.has_z() && zm_result.has_m(), "{zm_result:?}");
        assert_all_points_have_z(&zm_result);
        zm_result.for_each_point(|point| assert!(point.m().is_some(), "{zm_result:?}"));
    }

    #[test]
    fn rectangle_contained_collapsed_line_is_preserved_verbatim() {
        let polygon = square(0.0, 0.0, 4.0, 4.0);
        let collapsed = line(&[(1.0, 1.0), (1.0, 1.0)]);
        for (left, right) in [(&polygon, &collapsed), (&collapsed, &polygon)] {
            let result = left.intersection(right, Strictness::Strict).unwrap();
            assert_eq!(shape_wkb(&result), shape_wkb(&collapsed));
        }
    }

    #[test]
    fn polygon_intersection_absorbs_retained_boundary_lines() {
        let cases = [
            (
                "rounded shared boundary",
                polygon(&[(0.5, 0.5), (-2.0, 1.5), (-1.5, -1.5), (0.5, 0.5)]),
                polygon(&[(0.0, 1.0), (-1.0, 0.5), (0.0, 0.0), (0.0, 1.0)]),
                // The crossing is exactly (-1/3, 5/6); both ordinates are the
                // nearest binary64 to it.
                polygon(&[
                    (-1.0, 0.5),
                    (0.0, 0.0),
                    (0.0, 0.7),
                    (-0.333_333_333_333_333_3, 0.833_333_333_333_333_4),
                    (-1.0, 0.5),
                ]),
            ),
            (
                "integer shared boundary",
                polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
                polygon(&[(1.0, 0.0), (3.0, 0.0), (2.0, 2.0), (1.0, 0.0)]),
                polygon(&[(1.0, 0.0), (3.0, 0.0), (2.0, 2.0), (1.0, 0.0)]),
            ),
            (
                "integer collinear crossing",
                polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
                polygon(&[(1.0, 0.0), (3.0, 0.0), (5.0, 2.0), (3.0, 3.0), (1.0, 0.0)]),
                polygon(&[
                    (1.0, 0.0),
                    (3.0, 0.0),
                    (4.0, 1.0),
                    (4.0, 2.5),
                    (3.0, 3.0),
                    (1.0, 0.0),
                ]),
            ),
        ];

        for (name, left, right, expected) in cases {
            let left = Shape::Polygon(left);
            let right = Shape::Polygon(right);
            let expected = Shape::Polygon(expected);
            let result = intersection(&left, &right);
            assert_eq!(wkb(&result), wkb(&expected), "{name}: {result:?}");
            assert!(matches!(result, Shape::Polygon(_)), "{name}: {result:?}");
        }

        let square = square(0.0, 0.0, 4.0, 4.0);
        let partial_boundary = line(&[(1.0, 0.0), (3.0, 0.0)]);
        let result = intersection(&square, &partial_boundary);
        assert_eq!(wkb(&result), wkb(&partial_boundary), "{result:?}");
        assert!(matches!(result, Shape::LineString(_)), "{result:?}");
    }

    fn point_intersection(left: &Shape, right: &Shape, strictness: Strictness) -> Shape {
        overlay_all(
            &[left, right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            strictness,
        )
        .unwrap()
    }

    #[test]
    fn scalar_point_intersection_with_a_collapsed_line_is_the_point() {
        let collapsed = line(&[(1.0, 1.0), (1.0, 1.0)]);
        let collapsed_point = point(1.0, 1.0);
        for (left, right) in [
            (&collapsed_point, &collapsed),
            (&collapsed, &collapsed_point),
        ] {
            let inputs = vec![left, right];
            let result = overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(result, point(1.0, 1.0));
        }
    }

    #[test]
    fn scalar_point_intersection_reconciles_ordinates_in_both_orders() {
        let xy_line = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let z_line = zm_line((0.0, 0.0), (2.0, 0.0));
        let z_point = Shape::Point(
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(None)).unwrap(),
        );
        let matching_point = Shape::Point(
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(Some(9.0))).unwrap(),
        );
        let xy_point = point(1.0, 0.0);
        for (name, left, right, axes) in [
            ("point Z and line XY", z_point, xy_line, "XYZ"),
            ("point XY and line ZM", xy_point, z_line.clone(), "XYZM"),
        ] {
            for (left, right) in [(&left, &right), (&right, &left)] {
                let result = point_intersection(left, right, Strictness::Lenient);
                assert_eq!(result.coordinate_axes(), axes, "{name}: {result:?}");
            }
        }
        for (left, right) in [(&matching_point, &z_line), (&z_line, &matching_point)] {
            let result = point_intersection(left, right, Strictness::Strict);
            assert_eq!(result.coordinate_axes(), "XYZM", "{result:?}");
        }

        let conflicting = Shape::Point(
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(9.0))).unwrap(),
        );
        for (left, right) in [(&conflicting, &z_line), (&z_line, &conflicting)] {
            let result = point_intersection(left, right, Strictness::Lenient);
            assert_eq!(result.coordinate_axes(), "XYM", "{result:?}");
        }

        let z_polygon = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let boundary = point(2.0, 0.0);
        let boundary_result = point_intersection(&boundary, &z_polygon, Strictness::Lenient);
        assert_eq!(boundary_result.coordinate_axes(), "XYZ");
        let boundary_point = boundary_result.points_vec().unwrap()[0];
        assert_eq!(boundary_point.z(), Some(12.0));

        let interior_point = point(1.0, 1.0);
        let interior_result = point_intersection(&interior_point, &z_polygon, Strictness::Lenient);
        assert_eq!(interior_result.coordinate_axes(), "XY");
    }

    #[test]
    fn copied_point_intersection_requires_exact_segment_incidence() {
        let z_polygon = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let near_interior = point(1e-10, 2.0);
        let lenient = overlay_all(
            &[&near_interior, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert_eq!(lenient.coordinate_axes(), "XY", "{lenient:?}");
        let kept = lenient.points_vec().unwrap()[0];
        assert_eq!((kept.x, kept.y), (1e-10, 2.0));
        assert_eq!(kept.z(), None);

        let error = overlay_all(
            &[&near_interior, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap_err();
        assert!(
            matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(GeometryErrorKind::OrdinateDropped(operation))
                    if operation.as_ref() == "intersection"
            ),
            "{error}"
        );

        let boundary = point(2.0, 0.0);
        let boundary_result = overlay_all(
            &[&boundary, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(boundary_result.coordinate_axes(), "XYZ");
        assert_eq!(boundary_result.points_vec().unwrap()[0].z(), Some(12.0));

        let mixed = Shape::MultiPoint(
            vec![
                Point::new(1e-10, 2.0).unwrap(),
                Point::new(2.0, 0.0).unwrap(),
            ]
            .into(),
        );
        let mixed_lenient = overlay_all(
            &[&mixed, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert_eq!(mixed_lenient.coordinate_axes(), "XY", "{mixed_lenient:?}");
        assert_eq!(
            mixed_lenient
                .points_vec()
                .unwrap()
                .into_iter()
                .map(|point| (point.x, point.y, point.z()))
                .collect::<Vec<_>>(),
            vec![(1e-10, 2.0, None), (2.0, 0.0, None)]
        );
        let mixed_strict = overlay_all(
            &[&mixed, &z_polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap_err();
        assert!(
            mixed_strict
                .to_string()
                .contains("intersection cannot represent Z/M"),
            "{mixed_strict}"
        );
    }

    #[test]
    fn multipart_point_intersection_for_lineal_carriers() {
        let live = multiline(&[&[(0.0, 0.0), (2.0, 2.0)], &[(4.0, 0.0), (4.0, 2.0)]]);
        let multipoint = Shape::MultiPoint(
            vec![
                Point::new(4.0, 1.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
                Point::new(8.0, 8.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
            ]
            .into(),
        );
        for (left, right) in [(&multipoint, &live), (&live, &multipoint)] {
            let result = point_intersection(left, right, Strictness::Strict);
            assert!(matches!(result, Shape::MultiPoint(_)), "{result:?}");
            assert_eq!(
                result
                    .points_vec()
                    .unwrap()
                    .into_iter()
                    .map(|point| (point.x, point.y))
                    .collect::<Vec<_>>(),
                vec![(1.0, 1.0), (4.0, 1.0)]
            );
        }
    }

    #[test]
    fn multipart_point_intersection_for_polygonal_carriers() {
        let carrier = Shape::MultiPolygon(
            vec![
                polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                polygon(&[(4.0, 0.0), (6.0, 0.0), (6.0, 2.0), (4.0, 2.0), (4.0, 0.0)]),
            ]
            .into(),
        );
        let points = Shape::MultiPoint(
            vec![
                Point::new(5.0, 1.0).unwrap(),
                Point::new(0.0, 1.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
                Point::new(3.0, 1.0).unwrap(),
                Point::new(1.0, 1.0).unwrap(),
            ]
            .into(),
        );
        for (left, right) in [(&points, &carrier), (&carrier, &points)] {
            let result = point_intersection(left, right, Strictness::Strict);
            assert!(matches!(result, Shape::MultiPoint(_)), "{result:?}");
            assert_eq!(
                result
                    .points_vec()
                    .unwrap()
                    .into_iter()
                    .map(|point| (point.x, point.y))
                    .collect::<Vec<_>>(),
                vec![(0.0, 1.0), (1.0, 1.0), (5.0, 1.0)]
            );
        }
    }

    #[test]
    fn multipart_point_intersection_reconciles_ordinates() {
        let z_points = Shape::MultiPoint(
            vec![
                Point::new_axes(1.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(None)).unwrap(),
                Point::new_axes(1.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(None)).unwrap(),
            ]
            .into(),
        );
        let xy_line = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let lenient = point_intersection(&z_points, &xy_line, Strictness::Lenient);
        assert_eq!(lenient.coordinate_axes(), "XY");

        let points = Shape::MultiPoint(vec![Point::new(2.0, 0.0).unwrap()].into());
        let z_polygon = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let result = point_intersection(&points, &z_polygon, Strictness::Strict);
        assert_eq!(result.coordinate_axes(), "XYZ");
        assert_eq!(result.points_vec().unwrap()[0].z(), Some(12.0));
        let Shape::Polygon(polygon) = &z_polygon else {
            unreachable!("z_square returns a polygon");
        };
        let z_multipolygon = Shape::MultiPolygon(vec![polygon.clone()].into());
        let scalar = point(2.0, 0.0);
        let scalar_result = point_intersection(&scalar, &z_multipolygon, Strictness::Strict);
        assert_eq!(scalar_result.coordinate_axes(), "XYZ");
        assert_eq!(scalar_result.points_vec().unwrap()[0].z(), Some(12.0));

        let repeated_line = Shape::MultiLineString(
            MultiLineString::try_new(CoordinateAxes::XYZ, vec![MultiMember::Value(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_axes(0.0, 0.0, ZOrdinate(Some(1.0)), MOrdinate(None)).unwrap(),
                        Point::new_axes(0.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(None)).unwrap(),
                        Point::new_axes(2.0, 0.0, ZOrdinate(Some(2.0)), MOrdinate(None)).unwrap(),
                    ])
                    .unwrap(),
                )
                .expect("repeated line"),
            )])
            .unwrap(),
        );
        let repeated_point = Shape::MultiPoint(vec![Point::new(0.0, 0.0).unwrap()].into());
        let repeated_error = overlay_all(
            &[&repeated_point, &repeated_line],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap_err();
        assert!(repeated_error.to_string().contains("cannot represent Z/M"));
        let repeated_result = overlay_all(
            &[&repeated_point, &repeated_line],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert_eq!(repeated_result.coordinate_axes(), "XY");
    }

    #[test]
    fn intersection_all_matches_left_fold_for_permutations() {
        let shapes = [
            square(0.0, 0.0, 4.0, 4.0),
            square(1.0, -1.0, 5.0, 3.0),
            square(2.0, 1.0, 6.0, 5.0),
        ];
        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0], [2, 1, 0]] {
            let inputs = order
                .iter()
                .map(|&index| &shapes[index])
                .collect::<Vec<_>>();
            let canonical = overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                Strictness::Strict,
            )
            .unwrap();
            let folded = manual_left_fold(&inputs, OverlayOp::Intersection);
            assert_topologically_equal(&canonical, &folded);
        }
    }

    #[test]
    fn intersection_all_mixed_dimension_narrows_like_left_fold() {
        let area = square(0.0, 0.0, 4.0, 4.0);
        let crossing = line(&[(-1.0, 2.0), (5.0, 2.0)]);
        let marker = point(2.0, 2.0);
        let inputs = [&area, &crossing, &marker];
        let canonical = overlay_all(
            &inputs,
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        let folded = manual_left_fold(&inputs, OverlayOp::Intersection);
        assert!(matches!(canonical, Shape::Point(_)), "{canonical:?}");
        assert_topologically_equal(&canonical, &folded);
    }

    #[test]
    fn intersection_all_singleton_is_faithful_and_keeps_ordinates() {
        let input = Shape::geometry_collection(vec![
            Shape::Point(Point::new_axes(1.0, 2.0, ZOrdinate(Some(3.0)), MOrdinate(None)).unwrap()),
            Shape::Point(Point::new_axes(4.0, 5.0, ZOrdinate(None), MOrdinate(Some(6.0))).unwrap()),
        ])
        .expect("test collection is valid");
        // A single operand intersects to itself — faithful, so both policies
        // keep Z/M (force_2d is the way to flatten).
        for strictness in [Strictness::Lenient, Strictness::Strict] {
            let kept = overlay_all(
                &[&input],
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                strictness,
            )
            .unwrap();
            assert_eq!(kept, input);
            assert!(kept.has_z() && kept.has_m(), "{kept:?}");
        }
    }

    #[test]
    fn intersection_all_carries_z_once_from_original_inputs() {
        let first = z_square(0.0, 0.0, 4.0, 4.0, 10.0);
        let second = z_square(1.0, -1.0, 5.0, 3.0, 10.0);
        let third = z_square(2.0, 1.0, 6.0, 5.0, 10.0);
        let result = overlay_all(
            &[&first, &second, &third],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert!(result.has_z(), "{result:?}");
        assert_all_points_have_z(&result);
    }

    #[test]
    fn intersection_all_unresolved_survivor_degrades_under_auto_raises_under_strict() {
        let source = z_square(0.0, 0.0, 10.0, 10.0, 10.0);
        let interior = square(2.0, 2.0, 4.0, 4.0);
        // 'auto' (strict=false): an interior survivor vertex cannot source Z, so
        // the result degrades to 2D rather than raising.
        let auto = overlay_all(
            &[&source, &interior],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Lenient,
        )
        .unwrap();
        assert_topologically_equal(&auto, &interior);
        assert!(!auto.has_z() && !auto.has_m(), "{auto:?}");
        // 'error' (strict=true) raises on the unsourceable survivor.
        let error = overlay_all(
            &[&source, &interior],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        );
        assert!(error.is_err(), "{error:?}");
    }

    #[test]
    fn symmetric_difference_all_permutation_invariant_for_polygons() {
        let shapes = [
            square(0.0, 0.0, 3.0, 3.0),
            square(1.0, 1.0, 4.0, 4.0),
            square(2.0, -1.0, 5.0, 2.0),
        ];
        let baseline = overlay_all(
            &[&shapes[0], &shapes[1], &shapes[2]],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        for order in [[2, 1, 0], [1, 0, 2], [0, 2, 1]] {
            let inputs = order
                .iter()
                .map(|&index| &shapes[index])
                .collect::<Vec<_>>();
            let canonical = overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::SymmetricDifference,
                Strictness::Strict,
            )
            .unwrap();
            let folded = manual_left_fold(&inputs, OverlayOp::SymmetricDifference);
            assert_topologically_equal(&canonical, &baseline);
            assert_topologically_equal(&canonical, &folded);
        }
    }

    #[test]
    fn symmetric_difference_all_mixed_polygon_duplicates_leave_contained_line() {
        let polygon = square(0.0, 0.0, 4.0, 4.0);
        let contained_line = line(&[(1.0, 1.0), (3.0, 3.0)]);
        let result = overlay_all(
            &[&polygon, &polygon, &contained_line],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, contained_line);
    }

    #[test]
    fn symmetric_difference_all_mixed_collection_sees_non_empty_component_dimensions() {
        let polygon = square(0.0, 0.0, 4.0, 4.0);
        let contained_line = line(&[(1.0, 1.0), (3.0, 3.0)]);
        let collection =
            Shape::geometry_collection(vec![contained_line.clone(), Shape::empty_polygon()])
                .expect("test collection is valid");
        let result = overlay_all(
            &[&polygon, &polygon, &collection],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, contained_line);
    }

    #[test]
    fn mixed_symmetric_difference_keeps_line_support_outside_selected_area() {
        let left = crate::io::parse_wkt(
            "GEOMETRYCOLLECTION(LINESTRING(1 1,3 1),POLYGON((0 0,4 0,4 4,0 4,0 0)))",
        )
        .unwrap();
        let right = crate::io::parse_wkt("POLYGON((2 -1,6 -1,6 5,2 5,2 -1))").unwrap();
        let result = left
            .symmetric_difference(&right, Strictness::Strict)
            .unwrap();
        assert_eq!(
            crate::io::to_wkt(&result).unwrap(),
            "MULTIPOLYGON (((0 0, 2 0, 2 4, 0 4, 0 0)), ((2 -1, 6 -1, 6 5, 2 5, 2 4, 4 4, 4 0, 2 0, 2 -1)))"
        );
    }

    #[test]
    fn symmetric_difference_all_mixed_line_duplicates_leave_point() {
        let line = line(&[(-1.0, 0.0), (1.0, 0.0)]);
        let point_on_line = point(0.0, 0.0);
        let result = overlay_all(
            &[&line, &line, &point_on_line],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, point_on_line);
    }

    #[test]
    fn symmetric_difference_all_same_dimension_polygons_match_left_fold() {
        let shapes = [
            square(0.0, 0.0, 2.0, 2.0),
            square(0.0, 0.0, 2.0, 2.0),
            square(3.0, 3.0, 5.0, 5.0),
            square(3.0, 3.0, 5.0, 5.0),
        ];
        let inputs = [&shapes[0], &shapes[1], &shapes[2], &shapes[3]];
        let result = overlay_all(
            &inputs,
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        let folded = manual_left_fold(&inputs, OverlayOp::SymmetricDifference);
        assert_topologically_equal(&result, &folded);
        assert!(
            matches!(result, Shape::Empty(GeometryKind::Polygon, _)),
            "{result:?}"
        );
    }

    #[test]
    fn symmetric_difference_all_same_dimension_collections_are_canonical() {
        let duplicate =
            Shape::geometry_collection(vec![square(0.0, 0.0, 2.0, 2.0), Shape::empty_polygon()])
                .expect("test collection is valid");
        let survivor =
            Shape::geometry_collection(vec![square(3.0, 3.0, 5.0, 5.0), Shape::empty_polygon()])
                .expect("test collection is valid");
        let result = overlay_all(
            &[&duplicate, &duplicate, &survivor],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_topologically_equal(&result, &survivor);
    }

    #[test]
    fn symmetric_difference_all_duplicate_polygons_cancel_to_polygon_empty() {
        let polygon = square(0.0, 0.0, 2.0, 2.0);
        let result = overlay_all(
            &[&polygon, &polygon],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert!(
            matches!(result, Shape::Empty(GeometryKind::Polygon, _)),
            "{result:?}"
        );
    }

    #[test]
    fn symmetric_difference_all_duplicate_pair_cancels_before_survivor() {
        let duplicate = square(0.0, 0.0, 2.0, 2.0);
        let survivor = square(1.0, 1.0, 3.0, 3.0);
        let result = overlay_all(
            &[&duplicate, &duplicate, &survivor],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(result, survivor);
    }

    #[test]
    fn symmetric_difference_all_disjoint_inputs_match_union() {
        let shapes = [square(0.0, 0.0, 1.0, 1.0), square(3.0, 3.0, 4.0, 4.0)];
        let canonical = overlay_all(
            &[&shapes[0], &shapes[1]],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        let union = Shape::union_all(&shapes, Strictness::Strict).unwrap();
        assert_topologically_equal(&canonical, &union);
    }

    #[test]
    fn disjoint_bounds_certificates_match_joint_graph_assembly() {
        let polygons = [square(0.0, 0.0, 1.0, 1.0), square(3.0, 3.0, 4.0, 4.0)];
        let mixed = [
            square(0.0, 0.0, 1.0, 1.0),
            line(&[(3.0, 3.0), (4.0, 3.0)]),
            point(6.0, 6.0),
        ];
        let multipart = Shape::geometry_collection(vec![
            Shape::MultiPolygon(
                vec![polygon(&[
                    (0.0, 0.0),
                    (1.0, 0.0),
                    (1.0, 1.0),
                    (0.0, 1.0),
                    (0.0, 0.0),
                ])]
                .into(),
            ),
            Shape::MultiLineString(
                vec![
                    LineSeq::try_new(
                        CoordSeq::try_from_points(&[
                            Point::new(2.0, 2.0).unwrap(),
                            Point::new(3.0, 2.0).unwrap(),
                        ])
                        .unwrap(),
                    )
                    .unwrap(),
                ]
                .into(),
            ),
        ])
        .expect("test collection is valid");
        let remote = square(6.0, 6.0, 7.0, 7.0);
        let empty_z = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZ);

        for op in [
            OverlayOp::Union,
            OverlayOp::Difference,
            OverlayOp::SymmetricDifference,
        ] {
            assert_disjoint_certificate_matches_graph(&[&polygons[0], &polygons[1]], op);
            assert_disjoint_certificate_matches_graph(&[&mixed[0], &mixed[1], &mixed[2]], op);
            assert_disjoint_certificate_matches_graph(&[&multipart, &remote], op);
            assert_disjoint_results_match(&[&polygons[0], &empty_z, &polygons[1]], op);
        }
    }

    /// Three bounds components: an overlapping pair, an invalid bowtie with a
    /// square across it, and a remote square. Each component is partitioned
    /// alone, a difference keeps only its left operand's component, and an
    /// intersection whose bounds share no box builds nothing.
    #[test]
    fn bounds_components_partition_alone_and_match_one_arrangement() {
        use crate::geometry::overlay::exact_partition::{partition_builds, reset_partition_builds};
        let first = square(0.0, 0.0, 2.0, 2.0);
        let second = square(1.0, 1.0, 3.0, 3.0);
        let bowtie = Shape::Polygon(polygon(&[
            (10.0, 10.0),
            (12.0, 12.0),
            (10.0, 12.0),
            (12.0, 10.0),
            (10.0, 10.0),
        ]));
        let across = square(11.0, 11.0, 13.0, 13.0);
        let remote = square(20.0, 20.0, 21.0, 21.0);
        let inputs = [&first, &second, &bowtie, &across, &remote];
        for (op, builds) in [
            (OverlayOp::Union, 3),
            (OverlayOp::SymmetricDifference, 3),
            (OverlayOp::Difference, 1),
            (OverlayOp::Intersection, 0),
        ] {
            reset_partition_builds();
            let certified = overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(partition_builds(), builds, "{}", op.name());
            let joint = super::super::nary::overlay_all_without_bounds_certificates(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(wkb(&certified), wkb(&joint), "{}", op.name());
        }
    }

    /// Emission checks run where rounding moved a crossing, and nested
    /// operands, whose emitted nodes are all stored, skip them.
    #[test]
    fn emission_checks_run_only_where_rounding_moved_a_crossing() {
        use crate::geometry::overlay::exact_partition::{
            partition_metrics, reset_partition_builds,
        };
        let outer = Shape::Polygon(Polygon::new(
            test_ring(&[
                (0.0, 0.0),
                (2.0, 0.0),
                (4.0, 0.0),
                (4.0, 4.0),
                (0.0, 4.0),
                (0.0, 0.0),
            ]),
            Vec::new(),
        ));
        let inner = Shape::Polygon(Polygon::new(
            test_ring(&[
                (1.0, 1.0),
                (1.5, 1.0),
                (2.0, 1.0),
                (2.0, 2.0),
                (1.0, 2.0),
                (1.0, 1.0),
            ]),
            Vec::new(),
        ));
        for (right, moved) in [(inner, false), (regular_polygon(4.0, 4.0, 1.5, 7), true)] {
            reset_partition_builds();
            overlay_all(
                &[&outer, &right],
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                Strictness::Strict,
            )
            .unwrap();
            assert_eq!(partition_metrics().unmoved_emissions == 0, moved);
        }
    }

    #[test]
    fn disjoint_ordinate_axes_match_forced_graph_wkb() {
        let inputs = [
            (
                "XYZ",
                z_square(0.0, 0.0, 1.0, 1.0, 10.0),
                z_square(3.0, 3.0, 4.0, 4.0, 20.0),
            ),
            (
                "XYM",
                m_square(0.0, 0.0, 1.0, 1.0, 10.0),
                m_square(3.0, 3.0, 4.0, 4.0, 20.0),
            ),
            (
                "XYZM",
                zm_square(0.0, 0.0, 1.0, 1.0, 10.0),
                zm_square(3.0, 3.0, 4.0, 4.0, 20.0),
            ),
        ];
        for (axes, left, right) in inputs {
            assert_eq!(left.coordinate_axes(), axes);
            assert_eq!(right.coordinate_axes(), axes);
            for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
                assert_disjoint_certificate_matches_graph(&[&left, &right], op);
            }
        }
    }

    #[test]
    fn disjoint_invalid_zm_area_and_line_use_the_same_ordinate_path() {
        let polygon = invalid_zm_bowtie();
        let line = zm_line((10.0, 0.0), (11.0, 1.0));
        for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
            let certified = overlay_all(
                &[&polygon, &line],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .expect("certified mixed ZM overlay");
            let forced = super::super::nary::overlay_all_without_bounds_certificates(
                &[&polygon, &line],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .expect("forced mixed ZM overlay");
            assert_eq!(wkb(&certified), wkb(&forced), "{}", op.name());
        }
    }

    #[test]
    fn empty_ordinate_disjoint_paths_return_the_same_error() {
        let polygon = square(0.0, 0.0, 1.0, 1.0);
        let empty_z = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZ);
        let remote = square(3.0, 3.0, 4.0, 4.0);
        for op in [OverlayOp::Union, OverlayOp::SymmetricDifference] {
            let certified = overlay_all(
                &[&polygon, &empty_z, &remote],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            );
            let forced = super::super::nary::overlay_all_without_bounds_certificates(
                &[&polygon, &empty_z, &remote],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            );
            let (certified, forced) = (certified.unwrap_err(), forced.unwrap_err());
            assert_eq!(certified.to_string(), forced.to_string(), "{}", op.name());
            assert!(certified.to_string().contains("cannot represent Z/M"));
        }
    }

    #[test]
    fn symmetric_difference_all_carries_resolvable_ordinates() {
        let first = zm_square(0.0, 0.0, 1.0, 1.0, 10.0);
        let second = zm_square(3.0, 3.0, 4.0, 4.0, 20.0);
        // Disjoint symdiff is faithful — both policies carry Z/M.
        for strictness in [Strictness::Lenient, Strictness::Strict] {
            let kept = overlay_all(
                &[&first, &second],
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::SymmetricDifference,
                strictness,
            )
            .unwrap();
            assert!(kept.has_z() && kept.has_m(), "{kept:?}");
        }
        // Overlapping symdiff: seam crossings lie on the uniform-Z/M edges, so
        // they resolve and Z/M carries through under 'auto'.
        let overlap = zm_square(0.5, 0.5, 1.5, 1.5, 10.0);
        let carried = overlay_all(
            &[&first, &overlap],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Lenient,
        )
        .unwrap();
        assert!(carried.has_z() && carried.has_m(), "{carried:?}");
    }

    #[test]
    fn overlay_resolves_conflicting_z_and_equal_m_independently() {
        let point =
            |z, m| Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap();
        let left = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    point(1.0, 7.0),
                    Point::new_axes(2.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(9.0))).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let right = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    point(11.0, 7.0),
                    Point::new_axes(2.0, 0.0, ZOrdinate(Some(13.0)), MOrdinate(Some(9.0))).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );

        let result = left.union(&right, Strictness::Lenient).unwrap();
        assert_eq!(result.coordinate_axes(), "XYM");
        result.for_each_point(|point| {
            assert_eq!(point.m(), Some(if point.x == 0.0 { 7.0 } else { 9.0 }));
            assert!(point.z().is_none());
        });
    }

    #[test]
    fn overlay_preserves_signed_zero_and_drops_absent_difference_axis() {
        let left = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(0.0, 0.0, ZOrdinate(Some(-0.0)), MOrdinate(None)).unwrap(),
                    Point::new_axes(4.0, 0.0, ZOrdinate(Some(4.0)), MOrdinate(None)).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let xy = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new(0.0, 0.0).unwrap(),
                    Point::new(4.0, 0.0).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let intersection = left.intersection(&xy, Strictness::Lenient).unwrap();
        assert_eq!(intersection.coordinate_axes(), "XYZ");
        let first = intersection
            .points_vec()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(first.z().unwrap().to_bits(), (-0.0_f64).to_bits());
        assert!(first.m().is_none());

        let measured = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(2.0, -1.0, ZOrdinate(None), MOrdinate(Some(2.0))).unwrap(),
                    Point::new_axes(2.0, 1.0, ZOrdinate(None), MOrdinate(Some(4.0))).unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let difference = left.difference(&measured, Strictness::Lenient).unwrap();
        assert_eq!(difference.coordinate_axes(), "XYZ");
        difference.for_each_point(|point| assert!(point.m().is_none()));
    }

    #[test]
    fn empty_overlay_declares_union_axes_without_coordinates() {
        let z = z_square(0.0, 0.0, 1.0, 1.0, 1.0);
        let m = zm_square(2.0, 2.0, 3.0, 3.0, 1.0);
        let result = z.intersection(&m, Strictness::Lenient).unwrap();
        assert!(matches!(
            result,
            Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZM)
        ));
    }

    #[test]
    fn commutative_overlay_operand_reversal_keeps_axes_and_ordinates() {
        let left = zm_square(0.0, 0.0, 2.0, 2.0, 10.0);
        let right = zm_square(1.0, 1.0, 3.0, 3.0, 10.0);
        let signature = |shape: &Shape| {
            let mut points = shape
                .points_vec()
                .unwrap()
                .into_iter()
                .map(|point| {
                    (
                        point.x.to_bits(),
                        point.y.to_bits(),
                        point.z().map(f64::to_bits),
                        point.m().map(f64::to_bits),
                    )
                })
                .collect::<Vec<_>>();
            points.sort_unstable();
            points.dedup();
            (shape.coordinate_axes(), points)
        };
        for op in [
            OverlayOp::Union,
            OverlayOp::Intersection,
            OverlayOp::SymmetricDifference,
        ] {
            let forward = op.apply(&left, &right, Strictness::Lenient).unwrap();
            let reversed = op.apply(&right, &left, Strictness::Lenient).unwrap();
            assert_eq!(signature(&forward), signature(&reversed), "{}", op.name());
        }
    }

    #[test]
    fn line_merge_drops_only_conflicting_shared_node_axes() {
        let line = |z, m, end| {
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
                    Point::new_axes(end, 0.0, ZOrdinate(Some(z + 1.0)), MOrdinate(Some(m + 1.0)))
                        .unwrap(),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices")
        };
        let lines = MultiLineString::try_new(CoordinateAxes::XYZM, vec![
            MultiMember::Value(line(1.0, 10.0, 1.0)),
            MultiMember::Value(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_axes(1.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(11.0)))
                            .unwrap(),
                        Point::new_axes(2.0, 0.0, ZOrdinate(Some(100.0)), MOrdinate(Some(12.0)))
                            .unwrap(),
                    ])
                    .unwrap(),
                )
                .expect("fixture line has two vertices"),
            ),
        ])
        .unwrap();
        let merged = Shape::MultiLineString(lines).line_merge().unwrap();
        assert_eq!(merged.coordinate_axes(), "XYM");
        merged.for_each_point(|point| assert!(point.z().is_none()));
        merged.for_each_point(|point| assert!(point.m().is_some()));
    }

    const EVERY_OP: [OverlayOp; 4] = [
        OverlayOp::Union,
        OverlayOp::Intersection,
        OverlayOp::Difference,
        OverlayOp::SymmetricDifference,
    ];

    #[test]
    fn line_only_intersection_drops_contact_points_on_shared_edges() {
        let left = line(&[(0.0, 0.0), (4.0, 0.0)]);
        let right = line(&[(1.0, 0.0), (3.0, 0.0)]);
        let result = overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(result, Shape::LineString(_)), "{result:?}");
        assert_eq!(result.points_vec().unwrap().len(), 2, "{result:?}");
    }

    #[test]
    fn line_only_overlay_preserves_zm_and_operand_order() {
        let left = zm_line((0.0, 0.0), (2.0, 2.0));
        let right = zm_line((0.0, 2.0), (2.0, 0.0));
        for op in EVERY_OP {
            let forward = overlay_all(
                &[&left, &right],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap();
            let reverse = overlay_all(
                &[&right, &left],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap();
            if op != OverlayOp::Difference {
                assert_eq!(wkb(&forward), wkb(&reverse), "{}", op.name());
            }
            assert!(forward.has_z() && forward.has_m(), "{forward:?}");
            forward.for_each_point(|point| {
                assert!(point.z().is_some(), "{forward:?}");
                assert!(point.m().is_some(), "{forward:?}");
            });
        }
    }

    #[test]
    fn line_only_intersection_preserves_zm_for_overlapping_bounds_without_contact() {
        let cases = [(
            zm_line((0.0, 0.0), (4.0, 4.0)),
            zm_line((0.0, 1.0), (4.0, 5.0)),
        )];
        for (left, right) in cases {
            for strictness in [Strictness::Strict, Strictness::Lenient] {
                let fast = overlay_all(
                    &[&left, &right],
                    crate::geometry::TopologyDomain::Cartesian,
                    OverlayOp::Intersection,
                    strictness,
                )
                .unwrap();
                assert!(
                    matches!(
                        fast,
                        Shape::Empty(GeometryKind::LineString, CoordinateAxes::XYZM)
                    ),
                    "{fast:?}"
                );
            }
        }
    }

    #[test]
    fn line_only_support_includes_collapsed_points() {
        let line_shape = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let collapsed = line(&[(4.0, 4.0), (4.0, 4.0)]);
        let point_on_line = point(0.0, 0.0);
        let collapsed_union = overlay_all(
            &[&line_shape, &collapsed],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert_eq!(
            crate::io::to_wkt(&collapsed_union).unwrap(),
            "GEOMETRYCOLLECTION (POINT (4 4), LINESTRING (0 0, 2 0))"
        );
        let collapsed_intersection = overlay_all(
            &[&line_shape, &collapsed],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(
            collapsed_intersection,
            Shape::Empty(GeometryKind::LineString, _)
        ));

        let point_result = line_shape
            .intersection(&point_on_line, Strictness::Strict)
            .unwrap();
        assert_eq!(point_result, point_on_line);
    }

    #[test]
    fn disjoint_line_bounds_keep_certificate_before_provenance() {
        let count = 128;
        let left = disjoint_multiline(count, 0.0);
        let right = disjoint_multiline(count, 10_000.0);

        let union = overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Union,
            Strictness::Strict,
        )
        .unwrap();
        assert!(matches!(union, Shape::MultiLineString(_)), "{union:?}");
        assert_eq!(union.part_count(), count * 2);

        let intersection = overlay_all(
            &[&left, &right],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::Intersection,
            Strictness::Strict,
        )
        .unwrap();
        assert!(
            matches!(intersection, Shape::Empty(GeometryKind::LineString, _)),
            "{intersection:?}"
        );
    }

    #[test]
    fn aggregate_overlay_wkb_is_permutation_invariant() {
        let shapes = [
            square(0.0, 0.0, 3.0, 3.0),
            square(1.0, 1.0, 4.0, 4.0),
            square(2.0, -1.0, 5.0, 2.0),
        ];
        for op in [
            OverlayOp::Union,
            OverlayOp::Intersection,
            OverlayOp::SymmetricDifference,
        ] {
            let baseline = wkb(&overlay_all(
                &[&shapes[0], &shapes[1], &shapes[2]],
                crate::geometry::TopologyDomain::Cartesian,
                op,
                Strictness::Strict,
            )
            .unwrap());
            for order in [[2, 1, 0], [1, 0, 2], [0, 2, 1]] {
                let inputs = order
                    .iter()
                    .map(|&index| &shapes[index])
                    .collect::<Vec<_>>();
                assert_eq!(
                    wkb(&overlay_all(
                        &inputs,
                        crate::geometry::TopologyDomain::Cartesian,
                        op,
                        Strictness::Strict
                    )
                    .unwrap()),
                    baseline,
                    "{} order={order:?}",
                    op.name()
                );
            }
        }
    }

    #[test]
    fn mixed_symmetric_difference_is_associative() {
        let point = point(10.0, 10.0);
        let line = line(&[(1.0, 1.0), (3.0, 3.0)]);
        let area = square(0.0, 0.0, 4.0, 4.0);
        let direct = overlay_all(
            &[&point, &line, &area],
            crate::geometry::TopologyDomain::Cartesian,
            OverlayOp::SymmetricDifference,
            Strictness::Strict,
        )
        .unwrap();
        let left = point
            .symmetric_difference(&line, Strictness::Strict)
            .unwrap()
            .symmetric_difference(&area, Strictness::Strict)
            .unwrap();
        let right = point
            .symmetric_difference(
                &line
                    .symmetric_difference(&area, Strictness::Strict)
                    .unwrap(),
                Strictness::Strict,
            )
            .unwrap();
        assert_eq!(wkb(&direct), wkb(&left));
        assert_eq!(wkb(&direct), wkb(&right));
    }

    #[test]
    fn difference_assigns_contained_polygon_as_hole() {
        let result = square(0.0, 0.0, 4.0, 4.0)
            .difference(&square(1.0, 1.0, 3.0, 3.0), Strictness::Strict)
            .expect("contained polygon difference");
        let Shape::Polygon(polygon) = result else {
            panic!("expected holed polygon");
        };
        assert_eq!(polygon.holes.len(), 1);
    }
}

/// Every overlay lane leaves through one builder, so a node on a straight
/// run never survives to the output, whichever lane produced the ring.
#[test]
fn overlay_output_rings_carry_no_collinear_nodes() {
    let shape = |wkt: &str| crate::io::parse_wkt(wkt).unwrap();
    let wkt = |shape: &Shape| crate::io::to_wkt(shape).unwrap();
    let holed = shape("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))");
    let plain = shape("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))");
    let partner = shape("POLYGON ((1 1, 3 1, 3 4, 1 4, 1 1))");
    let strict = Strictness::Strict;
    assert_eq!(
        wkt(&plain.union(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
    );
    assert_eq!(
        wkt(&holed.union(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
    );
    assert_eq!(
        wkt(&holed.difference(&partner, strict).unwrap()),
        "POLYGON ((0 0, 4 0, 4 4, 3 4, 3 1, 1 1, 1 4, 0 4, 0 0))"
    );
}

mod rectangle_clip_tests {
    use super::*;
    use crate::geometry::segments::clip_segment_rect_interval;
    use crate::geometry::tessellation::exact::compare_ratios;

    fn bounds(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Bounds {
        Bounds::new(minx, miny, maxx, maxy).expect("finite ordered rectangle")
    }

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            )
            .expect("test coordinates form a sequence"),
        )
        .expect("test coordinates form a closed ring")
    }

    fn polygon(points: &[(f64, f64)]) -> Polygon {
        Polygon::new(ring(points), Vec::new())
    }

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
        polygon(&[
            (minx, miny),
            (maxx, miny),
            (maxx, maxy),
            (minx, maxy),
            (minx, miny),
        ])
    }

    fn compare_clip_results(
        name: &str,
        actual: Result<Shape>,
        expected: Result<Shape>,
        matching_errors_are_expected: bool,
    ) {
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(
                    shape_wkb(&actual),
                    shape_wkb(&expected),
                    "{name}: rectangle intersection {actual:?} differs from full partition {expected:?}"
                );
                assert!(
                    actual.validate().unwrap().is_none(),
                    "{name}: invalid rectangle clip output {actual:?}"
                );
            },
            (Err(actual), Err(expected)) if matching_errors_are_expected => {
                assert_eq!(
                    actual.to_string(),
                    expected.to_string(),
                    "{name}: local partition and exact overlay disagree on failure"
                );
            },
            (actual, expected) => panic!(
                "{name}: local partition returned {actual:?}, exact overlay returned {expected:?}"
            ),
        }
    }

    fn assert_clip_matches_oracle(name: &str, subjects: &[Polygon], bounds: Bounds) {
        let subject = Shape::narrow_polygons(CoordinateAxes::XY, subjects.to_vec());
        let rectangle_polygon = rect_polygon(bounds).expect("clip rectangle polygon");
        let rectangle = Shape::Polygon(rectangle_polygon.clone());
        let expected = super::nary::full_partition_cartesian_overlay(
            &[&subject, &rectangle],
            OverlayOp::Intersection,
            Strictness::Lenient,
        );
        let expected = expected.unwrap();
        let actual = super::intersect_rectangular_polygonal(
            &subject,
            &rectangle_polygon,
            bounds,
            crate::geometry::compare_shapes(&rectangle, &subject).is_lt(),
            Strictness::Lenient,
        )
        .unwrap()
        .unwrap_or_else(|| expected.clone());
        compare_clip_results(name, Ok(actual), Ok(expected), false);
    }

    fn assert_clip_oracle_failure_or_equal(name: &str, subjects: &[Polygon], bounds: Bounds) {
        let subject = Shape::narrow_polygons(CoordinateAxes::XY, subjects.to_vec());
        let rectangle = Shape::Polygon(rect_polygon(bounds).expect("clip rectangle polygon"));
        let inputs = [&subject, &rectangle];
        compare_clip_results(
            name,
            overlay_all(
                &inputs,
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Intersection,
                Strictness::Lenient,
            ),
            super::nary::full_partition_cartesian_overlay(
                &inputs,
                OverlayOp::Intersection,
                Strictness::Lenient,
            ),
            true,
        );
    }

    fn assert_public_clip_matches_intersection(
        name: &str,
        source_wkt: &str,
        rect: Bounds,
    ) -> Shape {
        let subject = crate::io::parse_wkt(source_wkt).expect("test geometry parses");
        let rectangle = Shape::Polygon(rect_polygon(rect).expect("clip rectangle polygon"));
        let actual = subject.clip_by_rect(rect, false);
        let expected = subject.intersection(&rectangle, Strictness::Lenient);
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(
                    shape_wkb(&actual),
                    shape_wkb(&expected),
                    "{name}: clip {actual:?} differs from intersection {expected:?}"
                );
                actual
            },
            (Err(actual), Err(expected)) => {
                assert_eq!(
                    actual.to_string(),
                    expected.to_string(),
                    "{name}: clip and intersection failed differently"
                );
                panic!("{name}: enumerated input must be accepted")
            },
            (actual, expected) => {
                panic!("{name}: clip returned {actual:?}, intersection returned {expected:?}")
            },
        }
    }

    #[test]
    fn clip_by_rect_matches_closed_box_intersection_on_contacts_and_degeneracies() {
        let rect = bounds(1.0, 1.0, 4.0, 4.0);
        let cases = [
            ("line-point-contact", "LINESTRING (5 3, 4 1)", rect),
            ("line-edge-contact", "LINESTRING (5 2, 4 2)", rect),
            ("line-corner-contact", "LINESTRING (5 5, 4 4)", rect),
            ("line-runs-along-edge", "LINESTRING (1 1, 4 1)", rect),
            (
                "multiline-mixed-contact",
                "MULTILINESTRING ((5 3, 4 1), (1 1, 4 1))",
                rect,
            ),
            (
                "multipolygon-point-contact-with-area",
                "MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))",
                rect,
            ),
            (
                "multipolygon-edge-contact-with-area",
                "MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)), ((4 2, 6 2, 6 3, 4 3, 4 2)))",
                rect,
            ),
            (
                "polygon-edge-contact",
                "POLYGON ((4 2, 6 2, 6 3, 4 3, 4 2))",
                rect,
            ),
            (
                "polygon-corner-contact",
                "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))",
                rect,
            ),
            (
                "polygon-runs-along-edge",
                "POLYGON ((0 1, 4 1, 4 3, 0 3, 0 1))",
                bounds(1.0, 1.0, 3.0, 4.0),
            ),
            (
                "holed-polygon-edge-contact",
                "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0), (4 2, 5 2, 5 4, 4 4, 4 2))",
                bounds(4.0, 1.0, 6.0, 5.0),
            ),
            (
                "holed-polygon-corner-contact",
                "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0), (3 3, 5 3, 5 5, 3 5, 3 3))",
                bounds(5.0, 5.0, 7.0, 7.0),
            ),
            (
                "zero-width-line",
                "LINESTRING (0 0, 4 4)",
                bounds(2.0, 1.0, 2.0, 3.0),
            ),
            (
                "zero-height-line",
                "LINESTRING (0 2, 4 2)",
                bounds(1.0, 2.0, 3.0, 2.0),
            ),
            (
                "zero-width-polygon",
                "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))",
                bounds(2.0, 0.0, 2.0, 4.0),
            ),
            (
                "zero-height-polygon",
                "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))",
                bounds(0.0, 2.0, 4.0, 2.0),
            ),
            (
                "spike",
                "POLYGON ((0 0, 4 0, 4 4, 2 4, 2 2, 3 2, 2 2, 2 4, 0 4, 0 0))",
                bounds(0.5, 0.5, 3.5, 3.5),
            ),
            (
                "collapsed-shell",
                "POLYGON ((0 0, 2 2, 4 4, 0 0))",
                bounds(-1.0, 1.0, 5.0, 3.0),
            ),
            (
                "shell-equals-hole",
                "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (0 0, 4 0, 4 4, 0 4, 0 0))",
                bounds(-1.0, -1.0, 5.0, 5.0),
            ),
            (
                "bow-tie",
                "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))",
                bounds(-1.0, -1.0, 3.0, 1.0),
            ),
            (
                "twice-wound",
                "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0, 4 0, 4 4, 0 4, 0 0))",
                bounds(-1.0, -1.0, 5.0, 5.0),
            ),
            (
                "hole-touching-shell",
                "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0), (0 2, 2 2, 2 4, 0 4, 0 2))",
                bounds(-1.0, -1.0, 7.0, 7.0),
            ),
            (
                "external-hole",
                "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (5 1, 6 1, 6 2, 5 2, 5 1))",
                bounds(-1.0, -1.0, 7.0, 7.0),
            ),
            ("empty", "POLYGON EMPTY", rect),
            (
                "collapsed-member-multipolygon",
                "MULTIPOLYGON (((0 0, 2 2, 4 4, 0 0)), ((5 0, 7 2, 9 4, 5 0)))",
                bounds(-1.0, -1.0, 10.0, 5.0),
            ),
            (
                "nearest-rounded-line-crossings",
                "LINESTRING (0 5, 5 2)",
                rect,
            ),
        ];
        for (name, source_wkt, rect) in cases {
            assert_public_clip_matches_intersection(name, source_wkt, rect);
        }

        let point_contact = assert_public_clip_matches_intersection(
            "known-line-point-contact",
            "LINESTRING (5 3, 4 1)",
            rect,
        );
        assert_eq!(
            shape_wkb(&point_contact),
            shape_wkb(&crate::io::parse_wkt("POINT (4 1)").unwrap())
        );
        let edge_contact = assert_public_clip_matches_intersection(
            "known-polygon-edge-contact",
            "POLYGON ((4 2, 6 2, 6 3, 4 3, 4 2))",
            rect,
        );
        assert_eq!(
            shape_wkb(&edge_contact),
            shape_wkb(&crate::io::parse_wkt("LINESTRING (4 2, 4 3)").unwrap())
        );
        let corner_contact = assert_public_clip_matches_intersection(
            "known-polygon-corner-contact",
            "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))",
            rect,
        );
        assert_eq!(
            shape_wkb(&corner_contact),
            shape_wkb(&crate::io::parse_wkt("POINT (4 4)").unwrap())
        );
        let mixed_point = assert_public_clip_matches_intersection(
            "known-mixed-point-and-area",
            "MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)), ((4 4, 6 4, 6 6, 4 6, 4 4)))",
            rect,
        );
        assert_eq!(
            shape_wkb(&mixed_point),
            shape_wkb(
                &crate::io::parse_wkt(
                    "GEOMETRYCOLLECTION (POINT (4 4), POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1)))"
                )
                .unwrap()
            )
        );
        let nearest_crossing = assert_public_clip_matches_intersection(
            "nearest-rounded-crossing",
            "LINESTRING (0 5, 5 2)",
            rect,
        );
        assert_eq!(
            shape_wkb(&nearest_crossing),
            shape_wkb(&crate::io::parse_wkt("LINESTRING (1.6666666666666667 4, 4 2.6)").unwrap())
        );
        let bow_tie = assert_public_clip_matches_intersection(
            "known-bow-tie-lobes",
            "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))",
            bounds(-1.0, -1.0, 3.0, 1.0),
        );
        assert_eq!(
            shape_wkb(&bow_tie),
            shape_wkb(
                &crate::io::parse_wkt(
                    "MULTIPOLYGON (((0 0, 1 1, 0 1, 0 0)), ((1 1, 2 0, 2 1, 1 1)))"
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn contained_valid_polygonal_intersection_preserves_the_operand_verbatim() {
        let bounds = bounds(-1.0, -1.0, 7.0, 5.0);
        for (name, source_wkt) in [
            (
                "noncanonical-shell-and-hole",
                "POLYGON ((4 0, 2 0, 0 0, 0 4, 4 4, 4 0), (1 1, 1 3, 3 3, 3 1, 1 1))",
            ),
            (
                "noncanonical-multipolygon-order",
                "MULTIPOLYGON (((4 0, 6 0, 6 2, 4 2, 4 0)), ((0 0, 2 0, 2 2, 0 2, 0 0)))",
            ),
        ] {
            let subject = crate::io::parse_wkt(source_wkt).unwrap();
            let expected = shape_wkb(&subject);
            let actual = assert_public_clip_matches_intersection(name, source_wkt, bounds);
            assert_eq!(
                shape_wkb(&actual),
                expected,
                "{name}: retained operand changed"
            );
            let rectangle = Shape::Polygon(rect_polygon(bounds).unwrap());
            let reversed = rectangle
                .intersection(&subject, Strictness::Lenient)
                .unwrap();
            assert_eq!(
                shape_wkb(&reversed),
                expected,
                "{name}: reversed operands changed the retained value"
            );
        }
    }

    #[test]
    fn contained_invalid_polygon_intersection_preserves_the_operand_verbatim() {
        let source_wkt = "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))";
        let subject = crate::io::parse_wkt(source_wkt).unwrap();
        let expected = shape_wkb(&subject);
        let actual = assert_public_clip_matches_intersection(
            "contained-bow-tie",
            source_wkt,
            bounds(-1.0, -1.0, 3.0, 3.0),
        );
        assert_eq!(shape_wkb(&actual), expected);
    }

    #[test]
    fn contained_zm_polygon_intersection_preserves_the_operand_verbatim() {
        let subject = crate::io::parse_wkt(
            "POLYGON ZM ((0 0 10 100, 2 0 12 120, 4 0 14 140, \
             4 4 18 180, 0 4 20 200, 0 0 10 100))",
        )
        .unwrap();
        let expected = shape_wkb(&subject);
        let bounds = bounds(0.0, 0.0, 4.0, 4.0);
        let rectangle = Shape::Polygon(rect_polygon(bounds).unwrap());

        let clipped = subject.clip_by_rect(bounds, false).unwrap();
        let intersection = subject
            .intersection(&rectangle, Strictness::Lenient)
            .unwrap();

        assert_eq!(shape_wkb(&clipped), expected);
        assert_eq!(shape_wkb(&intersection), expected);
    }

    #[test]
    fn contained_boundary_touching_polygon_intersection_preserves_the_operand() {
        let subject = crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 1, 2 2, 0 2, 0 0))").unwrap();
        let expected = shape_wkb(&subject);
        let bounds = bounds(-1.0, -1.0, 2.0, 3.0);
        let rectangle = Shape::Polygon(rect_polygon(bounds).unwrap());

        let clipped = subject.clip_by_rect(bounds, false).unwrap();
        let intersection = subject
            .intersection(&rectangle, Strictness::Lenient)
            .unwrap();

        assert_eq!(shape_wkb(&clipped), expected);
        assert_eq!(shape_wkb(&intersection), expected);
    }

    #[test]
    fn contained_point_touching_multipolygon_intersection_returns_input_verbatim() {
        let subject = crate::io::parse_wkt(
            "MULTIPOLYGON (((0 0, 2 0, 2 1, 2 2, 0 2, 0 0)), \
             ((2 1, 3 0, 4 1, 3 2, 2 1)))",
        )
        .unwrap();
        assert!(subject.validate().unwrap().is_none());
        let rectangle = bounds(-1.0, -1.0, 5.0, 3.0);
        let clipped = assert_public_clip_matches_intersection(
            "contained-point-touching-members",
            "MULTIPOLYGON (((0 0, 2 0, 2 1, 2 2, 0 2, 0 0)), \
             ((2 1, 3 0, 4 1, 3 2, 2 1)))",
            rectangle,
        );
        assert_eq!(
            shape_wkb(&clipped),
            shape_wkb(&subject),
            "contained touching members retain their original presentation"
        );
    }

    #[test]
    fn contained_rectangle_difference_returns_the_typed_empty() {
        let subject = crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))").unwrap();
        let rectangle = Shape::Polygon(rect_polygon(bounds(-1.0, -1.0, 3.0, 3.0)).unwrap());

        let difference = subject.difference(&rectangle, Strictness::Lenient).unwrap();

        assert_eq!(
            shape_wkb(&difference),
            shape_wkb(&crate::io::parse_wkt("POLYGON EMPTY").unwrap())
        );
    }

    #[test]
    fn rectangle_union_retains_the_covering_rectangle() {
        let subject = crate::io::parse_wkt("POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))").unwrap();
        let rectangle = Shape::Polygon(rect_polygon(bounds(0.0, 0.0, 4.0, 4.0)).unwrap());
        let expected = shape_wkb(&rectangle);

        let union = subject.union(&rectangle, Strictness::Lenient).unwrap();
        let reversed_union = rectangle.union(&subject, Strictness::Lenient).unwrap();

        assert_eq!(shape_wkb(&union), expected);
        assert_eq!(shape_wkb(&reversed_union), expected);
    }

    #[test]
    fn disjoint_rectangle_intersection_returns_the_typed_empty() {
        let subject = crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))").unwrap();
        let rectangle = Shape::Polygon(rect_polygon(bounds(5.0, 5.0, 6.0, 6.0)).unwrap());

        let intersection = subject
            .intersection(&rectangle, Strictness::Lenient)
            .unwrap();
        let reversed = rectangle
            .intersection(&subject, Strictness::Lenient)
            .unwrap();

        assert_eq!(
            shape_wkb(&intersection),
            shape_wkb(&crate::io::parse_wkt("POLYGON EMPTY").unwrap())
        );
        assert_eq!(shape_wkb(&reversed), shape_wkb(&intersection));
    }

    #[test]
    fn clip_by_rect_matches_full_intersection_for_the_country_fixture() {
        const COUNTRIES: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/osm_countries_0_1.geojson"
        ));

        let countries: serde_json::Value =
            serde_json::from_str(COUNTRIES).expect("country fixture parses");
        let features = countries["features"]
            .as_array()
            .expect("country fixture is a feature collection");
        assert_eq!(features.len(), 217);
        let bounds = bounds(-10.0, -10.0, 30.0, 50.0);
        let rectangle = Shape::Polygon(rect_polygon(bounds).expect("clip rectangle polygon"));
        for (index, feature) in features.iter().enumerate() {
            let code = feature["properties"]["tags"]["ISO3166-1:alpha2"]
                .as_str()
                .expect("country has an ISO code");
            let subject =
                crate::io::parse_geojson(&feature["geometry"]).expect("country geometry parses");
            let clipped = subject.clip_by_rect(bounds, false).unwrap();
            let intersection = subject
                .intersection(&rectangle, Strictness::Lenient)
                .unwrap();
            let full = super::nary::full_partition_cartesian_overlay(
                &[&subject, &rectangle],
                OverlayOp::Intersection,
                Strictness::Lenient,
            )
            .unwrap();
            let source_bounds = subject.bounds().expect("country has coordinates");
            let contained = source_bounds.minx() >= bounds.minx()
                && source_bounds.maxx() <= bounds.maxx()
                && source_bounds.miny() >= bounds.miny()
                && source_bounds.maxy() <= bounds.maxy();
            if contained {
                assert_eq!(
                    shape_wkb(&clipped),
                    shape_wkb(&subject),
                    "country {index} ({code}): contained clip changed the retained operand"
                );
                assert_eq!(
                    shape_wkb(&intersection),
                    shape_wkb(&subject),
                    "country {index} ({code}): intersection changed the retained operand"
                );
            } else {
                assert_eq!(
                    shape_wkb(&clipped),
                    shape_wkb(&intersection),
                    "country {index} ({code}): clip differs from intersection"
                );
                assert_eq!(
                    shape_wkb(&clipped),
                    shape_wkb(&full),
                    "country {index} ({code}): local clip differs from the full partition"
                );
            }
        }
    }

    fn regular_ellipse(cx: f64, cy: f64, rx: f64, ry: f64, count: usize, phase: f64) -> Polygon {
        let mut points = Vec::with_capacity(count.wrapping_add(1));
        for index in 0..count {
            let angle = phase + std::f64::consts::TAU * index as f64 / count as f64;
            points.push(Point::new_raw_xy(
                cx + rx * angle.cos(),
                cy + ry * angle.sin(),
            ));
        }
        points.push(points[0]);
        Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        )
    }

    #[test]
    fn rectangle_local_partition_matches_the_oracle_on_the_full_areal_matrix() {
        let square = rectangle(0.0, 0.0, 4.0, 4.0);
        let shell = ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let hole = ring(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)]);
        let holed = Polygon::new(shell, vec![hole]);
        let touching = [rectangle(0.0, 0.0, 2.0, 2.0), rectangle(2.0, 2.0, 4.0, 4.0)];
        let concave = polygon(&[
            (0.0, 0.0),
            (6.0, 0.0),
            (6.0, 6.0),
            (4.0, 6.0),
            (4.0, 2.0),
            (2.0, 2.0),
            (2.0, 6.0),
            (0.0, 6.0),
            (0.0, 0.0),
        ]);
        let spike = polygon(&[
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (2.0, 4.0),
            (2.0, 2.0),
            (3.0, 2.0),
            (2.0, 2.0),
            (2.0, 4.0),
            (0.0, 4.0),
            (0.0, 0.0),
        ]);
        let square_ring = ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let shell_equal_hole = Polygon::new(square_ring.clone(), vec![square_ring.clone()]);
        let touching_hole_shell = Polygon::new(
            ring(&[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0), (0.0, 0.0)]),
            vec![ring(&[
                (0.0, 2.0),
                (2.0, 2.0),
                (2.0, 4.0),
                (0.0, 4.0),
                (0.0, 2.0),
            ])],
        );
        let external_hole = Polygon::new(square_ring, vec![ring(&[
            (5.0, 1.0),
            (6.0, 1.0),
            (6.0, 2.0),
            (5.0, 2.0),
            (5.0, 1.0),
        ])]);

        let cases = [
            (
                "contained-window",
                vec![rectangle(-4.0, -3.0, 4.0, 3.0)],
                bounds(-1.0, -1.5, 1.25, 1.5),
            ),
            (
                "source-disjoint-from-window",
                vec![rectangle(0.0, 0.0, 1.0, 1.0)],
                bounds(5.0, 5.0, 6.0, 6.0),
            ),
            (
                "source-vertices-on-side-and-corner",
                vec![square.clone()],
                bounds(0.0, 1.0, 2.0, 4.0),
            ),
            (
                "coincident-shell-and-window",
                vec![square.clone()],
                bounds(0.0, 0.0, 4.0, 4.0),
            ),
            (
                "left-side-overlap",
                vec![square.clone()],
                bounds(0.0, 1.0, 2.0, 3.0),
            ),
            (
                "right-side-overlap",
                vec![square.clone()],
                bounds(2.0, 1.0, 4.0, 3.0),
            ),
            (
                "bottom-side-overlap",
                vec![square.clone()],
                bounds(1.0, 0.0, 3.0, 2.0),
            ),
            (
                "top-side-overlap",
                vec![square.clone()],
                bounds(1.0, 2.0, 3.0, 4.0),
            ),
            (
                "corner-only-contact",
                vec![square.clone()],
                bounds(4.0, 4.0, 5.0, 5.0),
            ),
            (
                "zero-width-window",
                vec![square.clone()],
                bounds(2.0, 0.0, 2.0, 4.0),
            ),
            (
                "zero-height-window",
                vec![square.clone()],
                bounds(0.0, 2.0, 4.0, 2.0),
            ),
            (
                "hole-crossed-by-window",
                vec![holed.clone()],
                bounds(3.0, 3.0, 7.0, 7.0),
            ),
            (
                "hole-contained-by-window",
                vec![holed],
                bounds(2.0, 2.0, 8.0, 8.0),
            ),
            (
                "multiple-shells",
                vec![square, rectangle(5.0, 1.0, 9.0, 5.0)],
                bounds(1.0, 1.0, 7.0, 3.0),
            ),
            (
                "point-touching-parts",
                touching.to_vec(),
                bounds(1.0, 1.0, 3.0, 3.0),
            ),
            (
                "disconnected-concave-result",
                vec![concave],
                bounds(0.5, 3.0, 5.5, 5.0),
            ),
            ("retraced-spike", vec![spike], bounds(0.5, 0.5, 3.5, 3.5)),
            (
                "collapsed-shell",
                vec![polygon(&[(0.0, 0.0), (2.0, 2.0), (4.0, 4.0), (0.0, 0.0)])],
                bounds(-1.0, 1.0, 5.0, 3.0),
            ),
            (
                "shell-equals-hole",
                vec![shell_equal_hole],
                bounds(-1.0, -1.0, 5.0, 5.0),
            ),
            (
                "bow-tie",
                vec![polygon(&[
                    (0.0, 0.0),
                    (2.0, 2.0),
                    (2.0, 0.0),
                    (0.0, 2.0),
                    (0.0, 0.0),
                ])],
                bounds(-1.0, -1.0, 3.0, 1.0),
            ),
            (
                "twice-wound",
                vec![polygon(&[
                    (0.0, 0.0),
                    (4.0, 0.0),
                    (4.0, 4.0),
                    (0.0, 4.0),
                    (0.0, 0.0),
                    (4.0, 0.0),
                    (4.0, 4.0),
                    (0.0, 4.0),
                    (0.0, 0.0),
                ])],
                bounds(-1.0, -1.0, 5.0, 5.0),
            ),
            (
                "hole-touching-shell",
                vec![touching_hole_shell],
                bounds(-1.0, -1.0, 7.0, 7.0),
            ),
            (
                "external-hole",
                vec![external_hole],
                bounds(-1.0, -1.0, 7.0, 7.0),
            ),
            ("empty-polygon", Vec::new(), bounds(1.0, 1.0, 4.0, 4.0)),
            (
                "collapsed-member-multipolygon",
                vec![
                    polygon(&[(0.0, 0.0), (2.0, 2.0), (4.0, 4.0), (0.0, 0.0)]),
                    polygon(&[(5.0, 0.0), (7.0, 2.0), (9.0, 4.0), (5.0, 0.0)]),
                ],
                bounds(-1.0, -1.0, 10.0, 5.0),
            ),
        ];
        for (name, polygons, rect) in cases {
            assert_clip_matches_oracle(name, &polygons, rect);
        }
    }

    #[test]
    fn named_convex_rings_match_the_rectangle_clip_oracle() {
        let cases = [
            (
                "pentagon-contained-window",
                regular_ellipse(-17.0, 12.0, 8.0, 4.0, 5, std::f64::consts::PI / 9.0),
                bounds(-17.2, 11.8, -16.8, 12.2),
            ),
            (
                "nine-vertex-crossing-window",
                regular_ellipse(14.0, -9.0, 11.0, 3.0, 9, std::f64::consts::PI / 7.0),
                bounds(18.0, -10.0, 24.0, -5.0),
            ),
            (
                "thirty-two-vertex-crossing-window",
                regular_ellipse(-40.0, -25.0, 16.0, 9.0, 32, std::f64::consts::PI / 13.0),
                bounds(-29.0, -27.0, -18.0, -23.0),
            ),
        ];
        for (name, polygon, rect) in cases {
            assert_clip_matches_oracle(name, &[polygon], rect);
        }
    }

    #[test]
    fn ellipse_and_close_rounded_crossings_match_the_oracle() {
        let ellipse = regular_ellipse(0.0, 0.0, 4.0, 3.0, 4096, 0.0);
        assert_clip_matches_oracle(
            "ellipse-4096-contained",
            &[ellipse],
            bounds(-1.0, -1.5, 1.25, 1.5),
        );

        let ulp = f64::from_bits(1.0_f64.to_bits().wrapping_add(1)) - 1.0;
        let quarter = polygon(&[(-1.0, 1.0), (7.0, 1.0 + ulp), (7.0, 0.0), (-1.0, 1.0)]);
        let half = polygon(&[(-1.0, 1.0), (3.0, 1.0 + ulp), (3.0, 0.0), (-1.0, 1.0)]);
        let quarter_interval = clip_segment_rect_interval(
            Segment {
                start: XY::new(-1.0, 1.0),
                end: XY::new(7.0, 1.0 + ulp),
            },
            bounds(1.0, 0.0, 2.0, 2.0),
        )
        .unwrap()
        .unwrap();
        let half_interval = clip_segment_rect_interval(
            Segment {
                start: XY::new(-1.0, 1.0),
                end: XY::new(3.0, 1.0 + ulp),
            },
            bounds(1.0, 0.0, 2.0, 2.0),
        )
        .unwrap()
        .unwrap();
        assert_eq!(quarter_interval.start.point.y.to_bits(), 1.0_f64.to_bits());
        assert_eq!(half_interval.start.point.y.to_bits(), 1.0_f64.to_bits());
        let [quarter_x, quarter_y, quarter_w] = *quarter_interval.start.exact.unwrap();
        let [half_x, half_y, half_w] = *half_interval.start.exact.unwrap();
        assert_eq!(
            compare_ratios(&quarter_x, &quarter_w, &half_x, &half_w).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert!(
            compare_ratios(&quarter_y, &quarter_w, &half_y, &half_w)
                .unwrap()
                .is_lt(),
            "the exact boundary crossings differ despite sharing one rounded coordinate"
        );
        assert_clip_oracle_failure_or_equal(
            "distinct-exact-crossings-one-rounded-coordinate",
            &[quarter, half],
            bounds(1.0, 0.0, 2.0, 2.0),
        );
    }

    #[test]
    fn rectangle_local_contact_guard_rejects_disjoint_source_edges() {
        let subject = rectangle(0.0, 0.0, 1.0, 1.0);
        assert_clip_matches_oracle(
            "disjoint-source-window",
            &[subject],
            bounds(5.0, 5.0, 6.0, 6.0),
        );
    }

    #[test]
    fn rectangle_clip_intersection_preserves_zm_carry() {
        let line_point =
            |x, z, m| Point::new_axes(x, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap();
        let source_line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    line_point(-1.0, 10.0, 100.0),
                    line_point(2.0, 40.0, 400.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let expected_line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    line_point(0.0, 20.0, 200.0),
                    line_point(1.0, 30.0, 300.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        assert!(
            source_line
                .clip_by_rect(bounds(0.0, -1.0, 1.0, 1.0), true)
                .unwrap()
                .equals_exact(&expected_line, 0.0, true, true)
        );

        let polygon = Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(
                    &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
                        .into_iter()
                        .map(|(x, y)| {
                            Point::new_axes(
                                x,
                                y,
                                ZOrdinate(Some(x + y + 7.0)),
                                MOrdinate(Some(x - y + 13.0)),
                            )
                            .unwrap()
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        );
        let rect = bounds(1.0, 0.5, 3.0, 3.5);
        let expected_xy = clip::polygon_parts_to_shape(
            binary_areal_overlay(
                std::slice::from_ref(&polygon),
                &[rect_polygon(rect).unwrap()],
                OverlayOp::Intersection,
            )
            .unwrap(),
        );
        let source = Shape::Polygon(polygon);
        let expected = carry_ordinates(expected_xy, &[&source], "clip_by_rect", false).unwrap();
        let actual = source.clip_by_rect(rect, false).unwrap();
        assert!(actual.equals_exact(&expected, 0.0, true, true));
        let _error = source.clip_by_rect(rect, true).unwrap_err();
    }
}
mod exact_geographic_nary_tests {
    use super::*;

    fn frame() -> crate::geographic::GeographicFrame {
        crate::geographic::GeographicFrame::crs84()
    }

    fn polygon(points: &[(f64, f64)]) -> Shape {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>();
        Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    fn line(points: &[Point]) -> Shape {
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(points).unwrap()).unwrap())
    }

    fn exact_location(shape: &Shape, point: Point) -> Loc {
        let frame = frame();
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
            (-180.0, -80.0),
            (-90.0, -80.0),
            (0.0, -80.0),
            (90.0, -80.0),
            (180.0, -80.0),
            (180.0, 80.0),
            (90.0, 80.0),
            (0.0, 80.0),
            (-90.0, 80.0),
            (-180.0, 80.0),
            (-180.0, -80.0),
        ]);
        let south_cap = polygon(&[
            (0.0, -80.0),
            (90.0, -80.0),
            (180.0, -80.0),
            (-90.0, -80.0),
            (0.0, -80.0),
        ]);
        let north_cap = polygon(&[
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
        ]);
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
    fn nary_seam_intersection_carries_the_original_source_interval() {
        let crossing = line(&[
            Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(10.0))).unwrap(),
            Point::new_axes(-170.0, 0.0, ZOrdinate(Some(20.0)), MOrdinate(Some(30.0))).unwrap(),
        ]);
        let meridian = line(&[
            Point::new_raw_xy(180.0, -1.0),
            Point::new_raw_xy(180.0, 1.0),
        ]);
        let output = super::super::nary::overlay_all(
            &[&crossing, &meridian],
            TopologyDomain::Geographic(frame()),
            OverlayOp::Intersection,
            Strictness::Strict,
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
}
