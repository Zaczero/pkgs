use crate::geometry::{
    CoordSeq, Coordinates as _, Ring, XY, closed_columns_winding, open_xy_cycle_winding,
};

/// Open point list of a ring oriented so the operand interior lies to the
/// left of every directed edge. Degenerate rings are omitted.
pub(super) fn oriented_open_ring(coords: &CoordSeq, is_hole: bool) -> Option<Box<[XY]>> {
    let (xs, ys) = (coords.xs(), coords.ys());
    let pairs = coords.coord_count().saturating_sub(1);
    let column_ccw = closed_columns_winding(xs, ys, pairs).is_ccw();
    let staged_len = coords.coord_count();
    let mut points: Vec<XY> = std::iter::zip(xs, ys)
        .map(|(&x, &y)| XY::new(x, y))
        .collect();
    points.dedup();
    if points.len() > 1 && points[0] == points[points.len() - 1] {
        points.pop();
    }
    debug_assert_eq!(
        points.len(),
        staged_open_ring_vertex_count(coords),
        "shared staged-ring count"
    );
    if points.len() < Ring::MIN_VERTICES_OPEN {
        return None;
    }
    let ccw = if points.len() < staged_len.saturating_sub(1) {
        open_xy_cycle_winding(&points).is_ccw()
    } else {
        column_ccw
    };
    if ccw == is_hole {
        points.reverse();
    }
    Some(points.into_boxed_slice())
}

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
