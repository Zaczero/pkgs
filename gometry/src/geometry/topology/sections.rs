use crate::geometry::topology::{
    Cut, Operand, OperandPool, Segment, XY, polygon_rings_contain_interior,
};
use crate::geometry::{Bounds, point_on_segment};

pub(in crate::geometry) fn sort_dedup_cuts(segment: Segment, cuts: &mut Vec<Cut>) {
    if cuts.is_empty() {
        return;
    }
    cuts.sort_by(|left, right| compare_along_segment(segment, left.point, right.point));
    let mut write = 0;
    for read in 0..cuts.len() {
        if write > 0 && cuts[write - 1].key == cuts[read].key {
            cuts[write - 1].cross |= cuts[read].cross;
        } else {
            cuts[write] = cuts[read];
            write += 1;
        }
    }
    cuts.truncate(write);
}

pub(in crate::geometry) fn compare_along_segment(
    segment: Segment,
    left: XY,
    right: XY,
) -> std::cmp::Ordering {
    let dx = segment.end.x - segment.start.x;
    let dy = segment.end.y - segment.start.y;
    let x_dominant = dx.abs() >= dy.abs() && dx != 0.0;
    let primary = if x_dominant {
        compare_axis(left.x, right.x, dx > 0.0)
    } else {
        compare_axis(left.y, right.y, dy >= 0.0)
    };
    primary.then_with(|| {
        if x_dominant {
            compare_axis(left.y, right.y, dy >= 0.0)
        } else {
            compare_axis(left.x, right.x, dx >= 0.0)
        }
    })
}

fn compare_axis(left: f64, right: f64, forward: bool) -> std::cmp::Ordering {
    if forward {
        left.total_cmp(&right)
    } else {
        right.total_cmp(&left)
    }
}

pub(in crate::geometry) fn other_contains(pool: &OperandPool, operand: Operand, point: XY) -> bool {
    polygon_rings_contain_interior(
        pool.rings
            .iter()
            .filter(|ring| ring.operand == operand)
            .map(|ring| ring.points.as_ref()),
        point,
    )
}

pub(in crate::geometry) fn operand_covers_boundary(
    pool: &OperandPool,
    operand: Operand,
    point: XY,
) -> bool {
    pool.rings
        .iter()
        .filter(|ring| ring.operand == operand)
        .any(|ring| {
            let bounds = Bounds::from_xy_iter(ring.points.iter().copied());
            if !bounds.contains_xy(point) {
                return false;
            }
            pool.segments[ring.segments.clone()]
                .iter()
                .any(|segment| point_on_segment(point, segment.start, segment.end))
        })
}
