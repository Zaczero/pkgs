use crate::geographic::{AngularFactor, GeographicFrame};
use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, Ring, Shape};

pub(in crate::geometry) fn frame() -> GeographicFrame {
    let degrees = AngularFactor::try_new("test degrees", std::f64::consts::PI / 180.0).unwrap();
    GeographicFrame::try_new(degrees, degrees, 0.0).unwrap()
}

pub(in crate::geometry) fn ring(points: &[(f64, f64)]) -> Ring {
    let points = points
        .iter()
        .map(|&(x, y)| Point::new_raw_xy(x, y))
        .collect::<Vec<_>>();
    Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap()
}

pub(in crate::geometry) fn line(points: &[(f64, f64)]) -> Shape {
    let points = points
        .iter()
        .map(|&(x, y)| Point::new_raw_xy(x, y))
        .collect::<Vec<_>>();
    Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap())
}

pub(in crate::geometry) fn polygon(points: &[(f64, f64)]) -> Shape {
    let points = points
        .iter()
        .map(|&(x, y)| Point::new_raw_xy(x, y))
        .collect::<Vec<_>>();
    Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        Vec::new(),
    ))
}
