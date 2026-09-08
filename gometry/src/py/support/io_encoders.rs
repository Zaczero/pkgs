#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::{CoordSeqBuilder, CoordinateAxes};
use crate::py::support::{Point, Polygon, Ring, Shape};

pub(crate) fn box_polygon(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
) -> crate::error::Result<Polygon> {
    let mut coords = CoordSeqBuilder::try_with_capacity(CoordinateAxes::XY, 5)?;
    coords.try_push(Point::new(minx, miny))?;
    coords.try_push(Point::new(maxx, miny))?;
    coords.try_push(Point::new(maxx, maxy))?;
    coords.try_push(Point::new(minx, maxy))?;
    coords.try_push(Point::new(minx, miny))?;
    Polygon::new(Ring::from_closed(coords.finish()?), Vec::new())
}

pub(crate) fn wrapped_box(
    west: f64,
    south: f64,
    east: f64,
    north: f64,
) -> crate::error::Result<Shape> {
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
    Shape::multi_polygon(crate::geometry::CoordinateAxes::XY, vec![
        box_polygon(west, south, 180.0, north)?,
        box_polygon(-180.0, south, east, north)?,
    ])
}
