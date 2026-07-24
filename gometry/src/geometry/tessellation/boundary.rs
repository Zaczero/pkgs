use std::collections::hash_map::Entry;

use super::*;
use crate::collections::HashMap;
use crate::geometry::PointKey;

/// Every linear chain (lines, rings) of every shape as XY rows, read
/// straight off the borrowed coordinate columns — the polygonize noding
/// input, with no intermediate `Vec<Point>` staging.
pub(crate) fn collect_xy_chains(shapes: &[&Shape]) -> Vec<Vec<XY>> {
    let mut lines: Vec<Vec<XY>> = Vec::new();
    for shape in shapes {
        shape.for_each_segment_chain(|chain| {
            lines.push(
                std::iter::zip(chain.xs(), chain.ys())
                    .map(|(&x, &y)| XY::new(x, y))
                    .collect(),
            );
        });
    }
    lines
}

/// Narrow ring linework to its boundary shape, mirroring
/// [`line_parts_to_shape`]: nothing → empty, one ring → `LineString`, more →
/// `MultiLineString`. Takes owned `CoordSeq`s so polygon boundaries clone their
/// coordinate columns once with no `Point` staging.
pub(crate) fn rings_to_boundary(mut rings: Vec<CoordSeq>) -> Shape {
    match rings.len() {
        // An (empty) polygon's boundary is still linework: `MULTILINESTRING
        // EMPTY`, keeping the type contract stable instead of collapsing to an
        // untyped collection.
        0 => Shape::MultiLineString(Vec::new()),
        1 => Shape::LineString(LineSeq::from_trusted(rings.remove(0))),
        _ => Shape::MultiLineString(rings.into_iter().map(LineSeq::from_trusted).collect()),
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
        _ => Shape::MultiPoint(Vec::<Point>::new().into()),
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
    Shape::MultiPoint(points.into())
}
