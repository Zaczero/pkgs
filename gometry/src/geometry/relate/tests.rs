use super::*;
use crate::geometry::{Point, Shape};
use crate::io::parse_wkt;

fn wkt(value: &str) -> Shape {
    parse_wkt(value).expect("test WKT parses")
}

#[test]
fn collapsed_collinear_polygon_collection_relate_is_native_lineal_support() {
    let left = wkt("GEOMETRYCOLLECTION(POLYGON((0 0,1 0,0 0,1 0,0 0)))");
    let right = wkt("LINESTRING(0 0,1 0)");

    assert_eq!(native_relate_shapes(&left, &right).text(), "1F2F0FFF2");
    assert_eq!(native_relate_shapes(&right, &left).text(), "1FFF0F2F2");
}

#[test]
fn collection_lineal_overlap_residual_is_native() {
    let left = wkt(
        "GEOMETRYCOLLECTION(POINT(3 1),POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),\
         POLYGON((1 1,3 1,3 3,1 3,1 1)),POINT(1 0))",
    );
    let right = wkt(
        "GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),POINT(1 0),POINT(3 1),\
         LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))",
    );

    assert_eq!(native_relate_shapes(&left, &right).text(), "0F20F1102");
    assert_eq!(native_relate_shapes(&right, &left).text(), "001FF0212");
}

#[test]
fn collection_boundary_vertices_grade_boundary_boundary_nodes() {
    let left = wkt(
        "GEOMETRYCOLLECTION(POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),\
         LINESTRING(1 1,3 1),POINT(1 1),POLYGON((2 0,4 0,4 2,2 2,2 0)),\
         LINESTRING(3 0,3 2))",
    );
    let right = wkt(
        "GEOMETRYCOLLECTION(POLYGON((0.5 0.5,1.5 0.5,1.5 1.5,0.5 1.5,0.5 0.5)),\
         LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))",
    );
    let node = Point::new_unchecked_xy(2.0, 0.0);
    let left_topology = RelateTopology::build(&left);
    let right_topology = RelateTopology::build(&right);

    assert!(left_topology.has_boundary_point(node));
    assert!(right_topology.has_boundary_point(node));
    assert_eq!(native_relate_shapes(&left, &right).text(), "102001212");
    assert_eq!(native_relate_shapes(&right, &left).text(), "102001212");
}

#[test]
fn collection_area_interior_hits_line_boundary_endpoint() {
    let left = wkt(
        "GEOMETRYCOLLECTION(POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),\
         POLYGON((2 0,4 0,4 2,2 2,2 0)),LINESTRING(2 1,3 1),POINT(1 0),\
         POLYGON((1 1,3 1,3 3,1 3,1 1)),LINESTRING(0 0,2 0),\
         LINESTRING(0.5 0,1.5 0))",
    );
    let right = wkt("LINESTRING(1 1,3 1)");
    let endpoint = Point::new_unchecked_xy(3.0, 1.0);
    let left_topology = RelateTopology::build(&left);
    let right_topology = RelateTopology::build(&right);

    assert!(matches!(
        left_topology.locate_point(endpoint),
        Loc::Interior
    ));
    assert!(right_topology.has_line_boundary_point(endpoint));
    assert_eq!(native_relate_shapes(&left, &right).text(), "102101FF2");
    assert_eq!(native_relate_shapes(&right, &left).text(), "11F00F212");
}

#[test]
fn line_boundary_inside_collection_interior_is_not_boundary_boundary() {
    let left = wkt(
        "GEOMETRYCOLLECTION(LINESTRING(3 0,3 2),POINT(0 0),LINESTRING(1 1,3 1),\
         POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((0 0,2 0,2 2,0 2,0 0)),\
         POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((2 0,4 0,4 2,2 2,2 0)),\
         LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))",
    );
    let right = wkt("GEOMETRYCOLLECTION(LINESTRING(2 1,3 1))");
    let left_topology = RelateTopology::build(&left);

    for point in [
        Point::new_unchecked_xy(2.0, 1.0),
        Point::new_unchecked_xy(3.0, 1.0),
    ] {
        assert!(matches!(left_topology.locate_point(point), Loc::Interior));
        assert!(!left_topology.has_boundary_point(point));
    }
    assert_eq!(native_relate_shapes(&left, &right).text(), "102FF1FF2");
    assert_eq!(native_relate_shapes(&right, &left).text(), "1FF0FF212");
}

#[test]
fn polygon_overlap_absorbs_collection_boundary_against_line_interior() {
    let left = wkt(
        "GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),LINESTRING(3 0,3 2),\
         POLYGON((0.5 0.5,1.5 0.5,1.5 1.5,0.5 1.5,0.5 0.5)),\
         POLYGON((1 1,3 1,3 3,1 3,1 1)),POINT(3 1),\
         POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((2 0,4 0,4 2,2 2,2 0)))",
    );
    let right = wkt("LINESTRING(3 0,3 2)");

    assert_eq!(native_relate_shapes(&left, &right).text(), "1F2F01FF2");
}

#[test]
fn overlapping_collection_line_endpoint_shares_boundary_with_line() {
    let left = wkt(
        "GEOMETRYCOLLECTION(LINESTRING(2 1,3 1),LINESTRING(3 0,3 2),\
         LINESTRING(1 1,3 1),POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),\
         POINT(1 1),LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))",
    );
    let right = wkt("LINESTRING(2 1,3 1)");
    let node = Point::new_unchecked_xy(2.0, 1.0);
    let left_topology = RelateTopology::build(&left);
    let right_topology = RelateTopology::build(&right);

    assert!(left_topology.has_line_boundary_point(node));
    assert!(right_topology.has_line_boundary_point(node));
    assert_eq!(
        mod2_relate(&left_topology, &right_topology).text(),
        "102F01FF2",
    );
    assert_eq!(native_relate_shapes(&left, &right).text(), "102F01FF2");
}

#[test]
fn mod2_relate_handles_lineal_collection_residuals() {
    let cases = [
        (
            "GEOMETRYCOLLECTION(LINESTRING(-1 1,3 1),LINESTRING(0.5 0,1.5 0),\
             POINT(1 0),LINESTRING(-1 1,3 1),POINT(1 1))",
            "POLYGON((2 0,4 0,4 2,2 2,2 0))",
            "101FFF212",
        ),
        (
            "GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),LINESTRING(0 0,2 0),\
             LINESTRING(0.5 0,1.5 0))",
            "POINT(3 1)",
            "FF1FFF0F2",
        ),
        (
            "GEOMETRYCOLLECTION(POLYGON((2 0,4 0,4 2,2 2,2 0)),POINT(1 1),\
             POINT(0 0),POINT(0 0),LINESTRING(2 1,3 1))",
            "GEOMETRYCOLLECTION(LINESTRING(3 0,3 2),LINESTRING(0 0,2 0),\
             LINESTRING(0.5 0,1.5 0))",
            "102F01102",
        ),
    ];
    for (left, right, expected) in cases {
        let left = wkt(left);
        let right = wkt(right);
        assert_eq!(native_relate_shapes(&left, &right).text(), expected);
    }
}
