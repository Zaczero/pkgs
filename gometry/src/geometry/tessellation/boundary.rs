use std::collections::hash_map::Entry;

use crate::collections::HashMap;
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates, GeometryKind, HashMapExt as _, LineSeq, Point, PointKey,
    Shape, same_point,
};

/// Narrow ring linework to its boundary shape: nothing → empty, one ring →
/// `LineString`, more → `MultiLineString`. Takes owned `CoordSeq`s so polygon
/// boundaries clone their coordinate columns once with no `Point` staging.
pub(crate) fn rings_to_boundary(mut rings: Vec<LineSeq>, empty_axes: CoordinateAxes) -> Shape {
    let axes = rings.first().map_or(empty_axes, |ring| ring.axes());
    match rings.len() {
        // An (empty) polygon's boundary is still linework: `MULTILINESTRING
        // EMPTY`, keeping the type contract stable instead of collapsing to an
        // untyped collection.
        0 => Shape::typed_empty(GeometryKind::MultiLineString, empty_axes),
        1 => Shape::LineString(rings.remove(0)),
        _ => Shape::multi_line_string(axes, rings),
    }
}

pub(crate) fn line_boundary<C: Coordinates + ?Sized>(
    points: &C,
    empty_axes: CoordinateAxes,
) -> crate::error::Result<Shape> {
    // A line's boundary is always a `MultiPoint` — empty when the line is
    // closed under the mod-2 rule — so the type contract is stable for
    // callers.
    match (points.first_coord(), points.last_coord()) {
        (Some(start), Some(end)) if points.coord_count() > 1 && !same_point(start, end) => Ok(
            Shape::multi_point(start.axes, CoordSeq::try_from_points(&[start, end])?),
        ),
        _ => Ok(Shape::typed_empty(GeometryKind::MultiPoint, empty_axes)),
    }
}

pub(crate) fn multiline_boundary<L: AsRef<CoordSeq>>(
    lines: &[L],
    empty_axes: CoordinateAxes,
) -> crate::error::Result<Shape> {
    // This is only an initial reservation; if the doubled hint does not fit,
    // normal collection growth still admits every published endpoint.
    let endpoint_capacity = lines.len().checked_mul(2).unwrap_or(lines.len());
    // First-seen order preserved in `endpoints`; the key map makes each
    // parity toggle O(1) (`PointKey` canonicalizes exactly like
    // `same_point` — ±0.0 only).
    let mut endpoints: Vec<(Point, bool)> = Vec::with_capacity(endpoint_capacity);
    let mut slots: HashMap<PointKey, usize> = HashMap::with_capacity(endpoint_capacity);
    for line in lines {
        let Shape::MultiPoint(boundary) = line_boundary(line.as_ref(), empty_axes)? else {
            continue;
        };
        for point in &boundary {
            match slots.entry(PointKey::new(point)) {
                Entry::Occupied(slot) => {
                    let endpoint = endpoints
                        .get_mut(*slot.get())
                        .expect("each endpoint slot names its inserted vector entry");
                    endpoint.1 = !endpoint.1;
                },
                Entry::Vacant(slot) => {
                    slot.insert(endpoints.len());
                    endpoints.push((point, true));
                },
            }
        }
    }
    // Odd-parity endpoints always come in pairs (each line toggles exactly
    // two), so this is a `MultiPoint` of 0, 2, 4, ... points.
    let points = endpoints
        .into_iter()
        .filter_map(|(point, odd)| odd.then_some(point))
        .collect::<Vec<_>>();
    Ok(Shape::multi_point(
        empty_axes,
        CoordSeq::try_from_points(&points)?,
    ))
}
