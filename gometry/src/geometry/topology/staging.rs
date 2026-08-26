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
