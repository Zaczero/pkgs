use super::*;

/// A polygon encloses no interior when every ring has zero signed area — it
/// is collinear, coincident, or otherwise degenerate, and is geometrically
/// indistinguishable from its boundary linework.
pub(crate) fn polygon_encloses_no_area(polygon: &Polygon) -> bool {
    polygon
        .rings()
        .all(|ring| ring_winding(ring).is_degenerate())
}

/// A degenerate (zero-area, hence collinear) ring as an OPEN polyline: the
/// closing vertex is dropped so the boundary buffers as a stadium around the
/// collinear span, not a retracing closed loop (which the winding engine
/// collapses to a thinner, wrong region). Rings are stored closed, so the
/// final vertex repeats the first; a coincident-point ring collapses to a
/// disk all the same.
pub(crate) fn ring_as_open_line(ring: &CoordSeq) -> CoordSeq {
    let n = ring.len();
    let take = if n >= 2 && ring.first() == ring.last() {
        n.wrapping_sub(1)
    } else {
        n
    };
    ring.view(CoordWindow::from_range(0..take, n))
}

fn ring_as_open_linework(ring: &CoordSeq) -> Result<Option<LineSeq>> {
    let line = ring_as_open_line(ring);
    match line.len() {
        0 => Ok(None),
        1 => {
            let point = line.point_at(0);
            Ok(Some(LineSeq::segment(point, point)?))
        },
        _ => Ok(Some(LineSeq::from_proven(line))),
    }
}

/// Reinterpret a shape's zero-area polygons as their boundary linework so the
/// winding buffer engine can resolve them (see [`buffer_with_style`]).
/// Non-degenerate polygons and non-polygonal parts pass through unchanged.
pub(crate) fn degenerate_polygonal_as_linework(shape: &Shape) -> Result<Shape> {
    Ok(match shape {
        Shape::Polygon(polygon) if polygon_encloses_no_area(polygon) => {
            let mut lines = Vec::with_capacity(polygon.rings().count());
            for ring in polygon.rings() {
                if let Some(line) = ring_as_open_linework(ring.coords())? {
                    lines.push(line);
                }
            }
            Shape::multi_line_string(polygon.shell.coords().axes(), lines)
        },
        Shape::MultiPolygon(polygons) => {
            let mut members = Vec::with_capacity(polygons.member_count());
            for member in polygons.members_with_values() {
                let shape = match member {
                    crate::geometry::MultiMember::Empty => {
                        Shape::typed_empty(GeometryKind::Polygon, polygons.axes())
                    },
                    crate::geometry::MultiMember::Value(polygon) => {
                        degenerate_polygonal_as_linework(&Shape::Polygon(polygon.clone()))?
                    },
                };
                members.push(shape);
            }
            Shape::geometry_collection(members)?
        },
        Shape::GeometryCollection(parts) => {
            Shape::GeometryCollection(parts.try_map_members(degenerate_polygonal_as_linework)?)
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::Empty(..) => shape.clone(),
    })
}
