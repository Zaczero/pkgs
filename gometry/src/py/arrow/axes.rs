use crate::py::arrow::*;

pub(crate) fn homogeneous_geometry_axes(geometries: &[&PyGeometry]) -> Option<CoordinateAxes> {
    let mut axes = None;
    for geometry in geometries {
        if !accumulate_geometry_axes(geometry.shape.shape(), &mut axes) {
            return None;
        }
    }
    Some(axes.unwrap_or(CoordinateAxes::XY))
}

/// Fold a shape's coordinate axes into `axes`, returning `false` on the first
/// axes mismatch (the array is not packable into one fixed-layout `GeoArrow`
/// buffer). Checks each `CoordSeq`'s `axes()` -- `O(1)` per column since a
/// `CoordSeq` is uniform-axes by construction (`zs`/`ms` present for all
/// coordinates or none) -- instead of walking every vertex.
pub(crate) fn accumulate_geometry_axes(shape: &Shape, axes: &mut Option<CoordinateAxes>) -> bool {
    fn fold(axes: &mut Option<CoordinateAxes>, seq: CoordinateAxes) -> bool {
        *axes.get_or_insert(seq) == seq
    }
    match shape {
        Shape::Point(point) => fold(axes, CoordinateAxes::from_point(*point)),
        Shape::MultiPoint(coords) => fold(axes, coords.axes()),
        Shape::LineString(coords) => fold(axes, coords.axes()),
        Shape::MultiLineString(lines) => lines.iter().all(|coords| fold(axes, coords.axes())),
        Shape::Polygon(polygon) => polygon.rings().all(|coords| fold(axes, coords.axes())),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .all(|polygon| polygon.rings().all(|coords| fold(axes, coords.axes()))),
        Shape::GeometryCollection(geometries) => geometries
            .iter()
            .all(|geometry| accumulate_geometry_axes(geometry, axes)),
        // A typed empty declares real axes; fold them so a `POINT Z EMPTY`
        // column exports at Z dimension instead of collapsing to XY.
        Shape::Empty(_, empty_axes) => fold(axes, *empty_axes),
    }
}
