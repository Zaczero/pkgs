//! Shared exact-support fixtures for bounded Hausdorff tests.

use std::fmt::Write as _;

use super::*;

pub(super) fn line(vertices: &[(f64, f64)]) -> Shape {
    let points: Vec<_> = vertices
        .iter()
        .map(|&(x, y)| Point::new_raw_xy(x, y))
        .collect();
    Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap())
}

pub(super) fn polygon(vertices: &[(f64, f64)]) -> Shape {
    let points: Vec<_> = vertices.iter().map(|&(x, y)| XY::new(x, y)).collect();
    Shape::Polygon(crate::geometry::Polygon::new(
        crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}

pub(super) fn adjacent_box_union(count: usize) -> Shape {
    let mut wkt = String::from("MULTIPOLYGON (");
    for index in 0..count {
        if index != 0 {
            wkt.push(',');
        }
        write!(
            wkt,
            "(({} 0,{} 0,{} 1,{} 1,{} 0))",
            index,
            index.wrapping_add(1),
            index.wrapping_add(1),
            index,
            index
        )
        .unwrap();
    }
    wkt.push(')');
    crate::io::parse_wkt(&wkt).expect("valid adjacent-box union")
}

pub(super) fn adjacent_box_prepared_support(count: usize) -> (PreparedSupport, PreparedSupport) {
    let shape = adjacent_box_union(count);
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "adjacent boxes");
    let source = PreparedSupport::from_shape_with_boundary(&shape, &mut budget)
        .unwrap()
        .unwrap();
    let boundary = source.boundary_clone().unwrap();
    (source, boundary)
}
