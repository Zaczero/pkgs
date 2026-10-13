use crate::geometry::{CoordSeqBuilder, CoordinateAxes};
use crate::py::support::{Point, Polygon, Ring};

pub(crate) fn box_polygon(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
) -> crate::error::Result<Polygon> {
    let mut coords = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, 5);
    coords.try_push(Point::new(minx, miny))?;
    coords.try_push(Point::new(maxx, miny))?;
    coords.try_push(Point::new(maxx, maxy))?;
    coords.try_push(Point::new(minx, maxy))?;
    coords.try_push(Point::new(minx, miny))?;
    Ok(Polygon::new(
        Ring::try_from_closed(coords.finish()?)?,
        Vec::new(),
    ))
}
