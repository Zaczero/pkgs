use crate::error::Result;
use crate::geometry::overlay::clip_polygonal_rectangle;
use crate::geometry::{Bounds, CoordinateAxes, Polygon, Shape, Strictness};

pub(crate) fn intersect_rectangular_polygonal(
    subject: &Shape,
    rectangle: &Polygon,
    bounds: Bounds,
    rectangle_first: bool,
    strictness: Strictness,
) -> Result<Option<Shape>> {
    let strict = strictness.is_strict();
    match subject {
        Shape::Polygon(polygon) => clip_polygonal_rectangle(
            std::slice::from_ref(polygon),
            rectangle,
            bounds,
            polygon.axes(),
            strict,
            rectangle_first,
        ),
        Shape::MultiPolygon(polygons) => clip_polygonal_rectangle(
            polygons.as_slice(),
            rectangle,
            bounds,
            polygons.axes(),
            strict,
            rectangle_first,
        ),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => Ok(None),
    }
}

/// Collapse a polygonal decomposition to its narrowest shape: `POLYGON EMPTY`
/// for none, the bare polygon for one, a multipolygon otherwise.
pub(crate) fn polygon_parts_to_shape(parts: Vec<Polygon>) -> Shape {
    Shape::narrow_polygons(CoordinateAxes::XY, parts)
}
