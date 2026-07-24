#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

pub(crate) fn box_polygon(minx: f64, miny: f64, maxx: f64, maxy: f64) -> PyResult<Polygon> {
    Ok(Polygon::new(
        Ring::from_trusted_closed(vec![
            Point::new(minx, miny)?,
            Point::new(maxx, miny)?,
            Point::new(maxx, maxy)?,
            Point::new(minx, maxy)?,
            Point::new(minx, miny)?,
        ]),
        Vec::new(),
    ))
}

pub(crate) fn wrapped_box(west: f64, south: f64, east: f64, north: f64) -> PyResult<Shape> {
    // Bounds validated by caller (`box_` via `finite_coordinate_required`) — DbC.
    let mut width = east - west;
    if width < 0.0 {
        width += 360.0;
    }
    if width >= 360.0 {
        return Ok(Shape::Polygon(box_polygon(-180.0, south, 180.0, north)?));
    }
    if west <= east {
        return Ok(Shape::Polygon(box_polygon(west, south, east, north)?));
    }
    Ok(Shape::MultiPolygon(vec![
        box_polygon(west, south, 180.0, north)?,
        box_polygon(-180.0, south, east, north)?,
    ]))
}
