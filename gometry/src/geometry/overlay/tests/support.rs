use crate::geometry::overlay::{OverlayOp, Strictness, overlay_all};
use crate::geometry::{
    CoordSeq, Dimension, GeometryKind, LineSeq, Polygon, Ring, Shape, TopologyDomain, XY,
};

pub(in crate::geometry::overlay) fn shared_set_case(name: &str) -> Shape {
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

pub(in crate::geometry::overlay) fn nested_shared_set(shape: Shape, depth: usize) -> Shape {
    let mut shared = shape;
    for _ in 0..depth {
        shared = Shape::geometry_collection(vec![shared.clone(), shared])
            .expect("shared collection depth is admitted");
    }
    shared
}

pub(in crate::geometry::overlay) fn shape_wkb(shape: &Shape) -> Vec<u8> {
    crate::io::to_wkb(shape, None, false).expect("shared-set result writes WKB")
}

pub(in crate::geometry::overlay) fn test_line(points: &[(f64, f64)]) -> Shape {
    let points: Vec<_> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(&points).expect("test line coordinates are admitted"),
    ))
}

pub(in crate::geometry::overlay) fn test_square(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
) -> Shape {
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

pub(in crate::geometry::overlay) fn test_ring(points: &[(f64, f64)]) -> Ring {
    let points: Vec<_> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    Ring::try_from_closed(
        CoordSeq::try_from_xy(&points).expect("test ring coordinates are admitted"),
    )
    .expect("test ring is closed")
}

pub(in crate::geometry::overlay) fn point_part(shape: &Shape) -> Option<&Shape> {
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

pub(in crate::geometry::overlay) fn assert_puntal_points_match_partition(
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

pub(in crate::geometry::overlay) fn assert_overlay_matches_full_partition(
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

pub(in crate::geometry::overlay) fn assert_point_lane_matches_full_partition(
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

pub(in crate::geometry::overlay) fn is_puntal_test_shape(shape: &Shape) -> bool {
    shape.topological_dimension() == Dimension::Point
}

pub(in crate::geometry::overlay) fn is_higher_dimensional_test_shape(shape: &Shape) -> bool {
    shape.topological_dimension() > Dimension::Point
}

pub(in crate::geometry::overlay) fn contains_verbatim_test_member(
    shape: &Shape,
    retained: &Shape,
) -> bool {
    if shape_wkb(shape) == shape_wkb(retained) {
        return true;
    }
    matches!(shape, Shape::GeometryCollection(parts) if parts.iter().any(|part| contains_verbatim_test_member(part, retained)))
}

pub(in crate::geometry::overlay) fn subdivision_wkb(shape: &Shape) -> Vec<Vec<u8>> {
    shape
        .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
        .expect("shared-set subdivision succeeds")
        .iter()
        .map(shape_wkb)
        .collect()
}

pub(in crate::geometry::overlay) fn shapes_wkb(shapes: &[Shape]) -> Vec<Vec<u8>> {
    shapes.iter().map(shape_wkb).collect()
}

pub(in crate::geometry::overlay) fn chain_coords(points: &[(f64, f64)]) -> CoordSeq {
    let points: Vec<_> = points.iter().map(|&(x, y)| XY::new(x, y)).collect();
    CoordSeq::try_from_xy(&points).expect("chain test coordinates are admitted")
}

pub(in crate::geometry::overlay) type ChainCoordinateBits = (u64, u64, Option<u64>, Option<u64>);

pub(in crate::geometry::overlay) fn chain_coordinate_bits(
    coords: &CoordSeq,
) -> Vec<ChainCoordinateBits> {
    coords
        .iter()
        .map(|point| {
            (
                point.x.to_bits(),
                point.y.to_bits(),
                point.z().map(f64::to_bits),
                point.m().map(f64::to_bits),
            )
        })
        .collect()
}

pub(in crate::geometry::overlay) fn chains_coordinate_bits(
    chains: &[CoordSeq],
) -> Vec<Vec<ChainCoordinateBits>> {
    chains.iter().map(chain_coordinate_bits).collect()
}
