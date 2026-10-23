use super::super::earcut_bridges::BridgeArena;
use super::{
    EarcutCornerId, EarcutCycle, GeometryErrorKind, HoleList, Point, Result, RingVertexId,
};
#[cfg(test)]
use crate::geometry::{Orientation, orientation_xy, same_point};

pub(super) fn eliminate_holes_with_ids(
    ring: &mut Vec<Point>,
    ring_ids: &mut Vec<EarcutCornerId>,
    holes: &[Vec<Point>],
    hole_ids: &[Vec<EarcutCornerId>],
    next_occurrence: usize,
) -> Result<()> {
    if holes.is_empty() {
        return Ok(());
    }
    if ring.len() != ring_ids.len()
        || holes.len() != hole_ids.len()
        || holes
            .iter()
            .zip(hole_ids)
            .any(|(hole, ids)| hole.len() != ids.len())
    {
        return Err(GeometryErrorKind::Invalid(
            "earcut source identities do not match the rings".into(),
        )
        .into());
    }
    let holes = HoleList::new(holes);
    let mut order: Vec<_> = holes.ids().collect();
    order.sort_unstable_by(|&left, &right| {
        compare_hole_leftmost_xy_slope(holes.ring(left), holes.ring(right))
            .then_with(|| left.index().cmp(&right.index()))
    });
    let capacity = holes.rings.iter().fold(ring.len(), |count, hole| {
        count.wrapping_add(hole.len()).wrapping_add(2)
    });
    let mut arena = BridgeArena::new_with_ids(ring, ring_ids, capacity, next_occurrence);
    for (batch, hole_id) in order.into_iter().enumerate() {
        let hole_ring = holes.ring(hole_id);
        let start = leftmost_vertex(hole_ring);
        let hole_identity_ring = hole_ids
            .get(hole_id.index())
            .expect("HoleId belongs to the validated source hole table");
        let bridge = arena
            .find_bridge(EarcutCycle::new(hole_ring).point(start))
            .ok_or_else(|| {
                GeometryErrorKind::triangulation("failed to find a visible bridge for polygon hole")
            })?;
        let corners = hole_ring.iter().zip(hole_identity_ring);
        arena.splice_with_ids(
            bridge,
            corners
                .clone()
                .skip(start.0)
                .chain(corners.take(start.0))
                .map(|(&point, &identity)| (point, identity)),
            batch.wrapping_add(1),
        );
    }
    let (points, identities) = arena.into_points_and_ids();
    *ring = points;
    *ring_ids = identities;
    Ok(())
}

fn compare_hole_leftmost_xy_slope(left: &[Point], right: &[Point]) -> std::cmp::Ordering {
    let left_cycle = EarcutCycle::new(left);
    let right_cycle = EarcutCycle::new(right);
    let left_index = leftmost_vertex(left);
    let right_index = leftmost_vertex(right);
    let left_point = left_cycle.point(left_index);
    let right_point = right_cycle.point(right_index);
    left_point
        .x
        .total_cmp(&right_point.x)
        .then_with(|| left_point.y.total_cmp(&right_point.y))
        .then_with(|| {
            let left_next = left_cycle.point(left_cycle.next(left_index));
            let right_next = right_cycle.point(right_cycle.next(right_index));
            let left_dx = left_next.x - left_point.x;
            let right_dx = right_next.x - right_point.x;
            let left_slope = if left_dx == 0.0 {
                f64::INFINITY
            } else {
                (left_next.y - left_point.y) / left_dx
            };
            let right_slope = if right_dx == 0.0 {
                f64::INFINITY
            } else {
                (right_next.y - right_point.y) / right_dx
            };
            left_slope.total_cmp(&right_slope)
        })
}

fn leftmost_vertex(ring: &[Point]) -> RingVertexId {
    let cycle = EarcutCycle::new(ring);
    ring.iter()
        .enumerate()
        .skip(1)
        .fold(RingVertexId(0), |best, (index, &point)| {
            let current = cycle.point(best);
            if point
                .x
                .total_cmp(&current.x)
                .then_with(|| point.y.total_cmp(&current.y))
                == std::cmp::Ordering::Less
            {
                RingVertexId(index)
            } else {
                best
            }
        })
}

#[cfg(test)]
fn rotate_ring(ring: &[Point], start: RingVertexId) -> Vec<Point> {
    ring.iter()
        .skip(start.0)
        .chain(ring.iter().take(start.0))
        .copied()
        .collect()
}

#[expect(
    clippy::float_cmp,
    reason = "exact comparison is intentional (sentinel / degenerate / exact-literal check)"
)]
#[cfg(test)]
fn find_hole_bridge(ring: &[Point], hole: &[Point]) -> Option<RingVertexId> {
    let hole_point = *hole.first()?;
    let (hx, hy) = (hole_point.x, hole_point.y);
    let cycle = EarcutCycle::new(ring);
    let mut qx = f64::NEG_INFINITY;
    let mut m = None;

    for (index, a, b) in cycle.edges() {
        if same_point(hole_point, a) {
            return Some(index);
        }
        let next = cycle.next(index);
        if same_point(hole_point, b) {
            return Some(next);
        }
        if a.y >= hy && hy >= b.y && (a.y - b.y) != 0.0 {
            let x = a.x + (hy - a.y) * (b.x - a.x) / (b.y - a.y);
            if x <= hx && x > qx {
                qx = x;
                m = Some(if a.x < b.x { index } else { next });
                if x == hx {
                    return m;
                }
            }
        }
    }

    let mut m = m?;
    if hx == qx {
        return Some(m);
    }

    let (mx, my) = {
        let point = cycle.point(m);
        (point.x, point.y)
    };
    let mut tan_min = f64::INFINITY;
    for (p, pt) in cycle.vertices_from(m) {
        let inside = if hy < my {
            point_in_triangle(hx, hy, mx, my, qx, hy, pt.x, pt.y)
        } else {
            point_in_triangle(qx, hy, mx, my, hx, hy, pt.x, pt.y)
        };
        if hx >= pt.x && pt.x >= mx && hx != pt.x && inside {
            let tan = (hy - pt.y).abs() / (hx - pt.x);
            if locally_inside_for_bridge(&cycle, p, hole_point)
                && (tan < tan_min
                    || (tan == tan_min
                        && (pt.x > cycle.point(m).x
                            || (pt.x == cycle.point(m).x && hole_bridge_sector_ok(&cycle, m, p)))))
            {
                m = p;
                tan_min = tan;
            }
        }
    }
    Some(m)
}

pub(in crate::geometry::tessellation) fn point_in_triangle(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    cx: f64,
    cy: f64,
    px: f64,
    py: f64,
) -> bool {
    (cx - px) * (ay - py) >= (ax - px) * (cy - py)
        && (ax - px) * (by - py) >= (bx - px) * (ay - py)
        && (bx - px) * (cy - py) >= (cx - px) * (by - py)
}

#[cfg(test)]
fn locally_inside_for_bridge(cycle: &EarcutCycle<'_>, p: RingVertexId, target: Point) -> bool {
    let prev = cycle.point(cycle.previous(p));
    let curr = cycle.point(p);
    let next = cycle.point(cycle.next(p));
    let (tx, ty) = (target.x, target.y);
    if orientation_xy(prev.x, prev.y, curr.x, curr.y, next.x, next.y)
        == Orientation::CounterClockwise
    {
        orientation_xy(curr.x, curr.y, tx, ty, next.x, next.y) != Orientation::CounterClockwise
            && orientation_xy(curr.x, curr.y, prev.x, prev.y, tx, ty)
                != Orientation::CounterClockwise
    } else {
        orientation_xy(curr.x, curr.y, tx, ty, prev.x, prev.y) == Orientation::CounterClockwise
            || orientation_xy(curr.x, curr.y, next.x, next.y, tx, ty)
                == Orientation::CounterClockwise
    }
}

#[cfg(test)]
fn hole_bridge_sector_ok(cycle: &EarcutCycle<'_>, m: RingVertexId, p: RingVertexId) -> bool {
    let m_prev = cycle.point(cycle.previous(m));
    let m_curr = cycle.point(m);
    let m_next = cycle.point(cycle.next(m));
    let p_prev = cycle.point(cycle.previous(p));
    let p_next = cycle.point(cycle.next(p));
    orientation_xy(m_prev.x, m_prev.y, m_curr.x, m_curr.y, p_prev.x, p_prev.y)
        == Orientation::CounterClockwise
        && orientation_xy(p_next.x, p_next.y, m_curr.x, m_curr.y, m_next.x, m_next.y)
            == Orientation::CounterClockwise
}

#[cfg(test)]
fn splice_hole_bridge(ring: &mut Vec<Point>, bridge: RingVertexId, hole: &[Point]) {
    let bridge_point = EarcutCycle::new(ring).point(bridge);
    let hole_point = *hole.first().expect("validated holes are non-empty");
    let head_len = bridge.0.wrapping_add(1);
    let capacity = ring.len().wrapping_add(hole.len()).wrapping_add(2);
    let mut spliced = Vec::with_capacity(capacity);
    spliced.extend(ring.iter().take(head_len).copied());
    spliced.extend_from_slice(hole);
    spliced.push(hole_point);
    spliced.push(bridge_point);
    spliced.extend(ring.iter().skip(head_len).copied());
    *ring = spliced;
}

#[cfg(test)]
#[path = "bridges/tests.rs"]
mod tests;
