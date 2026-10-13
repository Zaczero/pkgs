//! Shared point and polygon fixtures for tessellation tests.

use crate::error::ErrorKind;
use crate::geometry::{
    CdtRefinement, GeometryErrorKind, MOrdinate, Point, Polygon, Ring, Shape, ZOrdinate,
};

pub(super) fn xy(x: f64, y: f64) -> Point {
    Point::new(x, y).expect("finite test coordinate")
}
pub(super) fn xyz(x: f64, y: f64, z: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None)).expect("finite test coordinate")
}
pub(super) fn xym(x: f64, y: f64, m: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(None), MOrdinate(Some(m))).expect("finite test coordinate")
}
pub(super) fn polygon(shell: Vec<Point>, holes: Vec<Vec<Point>>) -> Polygon {
    Polygon::new(
        Ring::closed(shell).expect("valid test shell"),
        holes
            .into_iter()
            .map(|hole| Ring::closed(hole).expect("valid test hole"))
            .collect(),
    )
}
pub(super) fn assert_triangulation_error(polygon: Polygon) {
    let error = Shape::Polygon(polygon)
        .polygon_triangles()
        .expect_err("invalid polygon must fail earcut");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Triangulation(_))
    ));
}
pub(super) fn assert_constrained_triangulation_error(polygon: Polygon, expected: &str) {
    let error = Shape::Polygon(polygon)
        .constrained_delaunay_vertices(CdtRefinement::default())
        .expect_err("overlapping ring constraints must fail");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::Triangulation(message))
            if message.as_ref() == expected
    ));
}
pub(super) fn points(xy: &[(f64, f64)]) -> Vec<Point> {
    xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
}
