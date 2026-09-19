use crate::geometry::{CoordSeq, Coordinates as _, Ring, XY, closed_columns_winding};

/// Whether the source ring direction already keeps its interior on the left.
/// Consecutive duplicates do not affect winding, but still decide degeneracy.
pub(super) fn oriented_open_ring_direction(coords: &CoordSeq, is_hole: bool) -> Option<bool> {
    let (xs, ys) = (coords.xs(), coords.ys());
    (staged_open_ring_vertex_count(coords) >= Ring::MIN_VERTICES_OPEN).then(|| {
        closed_columns_winding(xs, ys, coords.coord_count().saturating_sub(1)).is_ccw() != is_hole
    })
}

fn staged_open_ring_vertex_count(coords: &CoordSeq) -> usize {
    let (xs, ys) = (coords.xs(), coords.ys());
    let mut first = None;
    let mut previous = None;
    let mut count = 0;
    for (&x, &y) in std::iter::zip(xs, ys) {
        let point = XY::new(x, y);
        if previous != Some(point) {
            first.get_or_insert(point);
            previous = Some(point);
            count += 1;
        }
    }
    if count > 1 && first == previous {
        count -= 1;
    }
    count
}
