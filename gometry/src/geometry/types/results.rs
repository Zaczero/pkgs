use crate::geometry::{Point, Shape};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ValidationIssue {
    pub code: &'static str,
    pub detail: String,
    pub location: Option<Point>,
    pub path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PolygonizeFull {
    pub polygons: Vec<Shape>,
    pub cuts: Vec<Vec<Shape>>,
    pub dangles: Vec<Vec<Shape>>,
    pub invalid_rings: Vec<Vec<Shape>>,
}
