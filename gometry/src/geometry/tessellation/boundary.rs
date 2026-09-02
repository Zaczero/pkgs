use std::collections::hash_map::Entry;

use crate::collections::HashMap;
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates, GeometryKind, HashMapExt as _, LineSeq, Point, PointKey,
    Shape, same_point,
};

/// Narrow ring linework to its boundary shape, mirroring
/// [`line_parts_to_shape`]: nothing → empty, one ring → `LineString`, more →
/// `MultiLineString`. Takes owned `CoordSeq`s so polygon boundaries clone their
/// coordinate columns once with no `Point` staging.
pub(crate) fn rings_to_boundary(mut rings: Vec<CoordSeq>) -> Shape {
    match rings.len() {
        // An (empty) polygon's boundary is still linework: `MULTILINESTRING
        // EMPTY`, keeping the type contract stable instead of collapsing to an
        // untyped collection.
        0 => Shape::typed_empty(GeometryKind::MultiLineString, CoordinateAxes::XY),
        1 => Shape::LineString(
            LineSeq::try_new(rings.remove(0))
                .expect("line coordinate sequence must contain at least two vertices"),
        ),
        _ => Shape::MultiLineString(
            rings
                .into_iter()
                .map(|coords| {
                    LineSeq::try_new(coords).expect("ring boundary has at least two vertices")
                })
                .collect(),
        ),
    }
}

pub(crate) fn line_boundary<C: Coordinates + ?Sized>(points: &C) -> Shape {
    // A line's boundary is always a `MultiPoint` — empty when the line is
    // closed (mod-2 rule, matching GEOS/PostGIS) — so the type contract is
    // stable for callers.
    match (points.first_coord(), points.last_coord()) {
        (Some(start), Some(end)) if points.coord_count() > 1 && !same_point(start, end) => {
            Shape::MultiPoint(vec![start, end].into())
        },
        _ => Shape::typed_empty(GeometryKind::MultiPoint, CoordinateAxes::XY),
    }
}

pub(crate) fn multiline_boundary<L: AsRef<CoordSeq>>(lines: &[L]) -> Shape {
    // First-seen order preserved in `endpoints`; the key map makes each
    // parity toggle O(1) (`PointKey` canonicalizes exactly like
    // `same_point` — ±0.0 only).
    let mut endpoints: Vec<(Point, bool)> = Vec::with_capacity(lines.len() * 2);
    let mut slots: HashMap<PointKey, usize> = HashMap::with_capacity(lines.len() * 2);
    for line in lines {
        let Shape::MultiPoint(boundary) = line_boundary(line.as_ref()) else {
            continue;
        };
        for point in &boundary {
            match slots.entry(PointKey::new(point)) {
                Entry::Occupied(slot) => {
                    let odd = &mut endpoints[*slot.get()].1;
                    *odd = !*odd;
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
    if points.is_empty() {
        Shape::typed_empty(GeometryKind::MultiPoint, CoordinateAxes::XY)
    } else {
        Shape::MultiPoint(points.into())
    }
}
