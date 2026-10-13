use std::cmp::Ordering;

#[cfg(test)]
use super::Ring;
use super::{
    ExactDyadic, ExactQuery, ExactSign, InfinitesimalQuery, Loc, PointQuery, QueryAxis, Result,
    RingKind, XY, point_on_segment, same_point, stored_orientation_sign,
};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RingProbe {
    pub(super) winding: i64,
    pub(super) incident: bool,
}

pub(super) struct Ray {
    pub(super) end: XY,
    pub(super) ring: Option<(usize, i64)>,
    pub(super) curve: bool,
    pub(super) collapsed: bool,
    pub(super) reverse: bool,
}

#[derive(Clone, Copy)]
pub(super) struct RayGroup {
    pub(super) start: usize,
    pub(super) end: usize,
}

pub(super) const fn line_location(
    endpoint_parity: bool,
    residual_collapsed_degree_odd: bool,
) -> Loc {
    let boundary = endpoint_parity ^ residual_collapsed_degree_odd;
    if boundary {
        Loc::Boundary
    } else {
        Loc::Interior
    }
}

pub(super) fn symbolic_orientation_from_sign(start: XY, end: XY, direct: ExactSign) -> ExactSign {
    if direct != ExactSign::Zero {
        return direct;
    }
    match start.y.total_cmp(&end.y) {
        Ordering::Less => ExactSign::Negative,
        Ordering::Greater => ExactSign::Positive,
        Ordering::Equal => match start.x.total_cmp(&end.x) {
            Ordering::Less => ExactSign::Positive,
            Ordering::Greater => ExactSign::Negative,
            Ordering::Equal => ExactSign::Zero,
        },
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "the exact ray events are finite and the following group bounds partition their sorted slice"
)]
pub(super) fn query_ray_groups<Q: PointQuery>(query: Q, rays: &[Ray]) -> Result<Vec<RayGroup>> {
    let mut groups = Vec::new();
    let mut start = 0_usize;
    while start < rays.len() {
        let mut end = start.wrapping_add(1);
        while end < rays.len()
            && compare_query_rays(query, rays[start].end, rays[end].end)? == Ordering::Equal
        {
            end = end.wrapping_add(1);
        }
        groups.push(RayGroup { start, end });
        start = end;
    }
    Ok(groups)
}

pub(super) fn compare_query_rays<Q: PointQuery>(query: Q, left: XY, right: XY) -> Result<Ordering> {
    let left_y = query.compare_axis(QueryAxis::Y, left.y)?;
    let right_y = query.compare_axis(QueryAxis::Y, right.y)?;
    let left_upper = left_y == Ordering::Less
        || (left_y == Ordering::Equal
            && query.compare_axis(QueryAxis::X, left.x)? == Ordering::Less);
    let right_upper = right_y == Ordering::Less
        || (right_y == Ordering::Equal
            && query.compare_axis(QueryAxis::X, right.x)? == Ordering::Less);
    if left_upper != right_upper {
        return Ok(right_upper.cmp(&left_upper));
    }
    Ok(match query.orientation(left, right)? {
        ExactSign::Positive => Ordering::Less,
        ExactSign::Negative => Ordering::Greater,
        ExactSign::Zero => Ordering::Equal,
    })
}

#[cfg(test)]
pub(super) fn ray_groups(query: XY, rays: &[Ray]) -> Vec<RayGroup> {
    let mut groups = Vec::new();
    let mut start = 0_usize;
    while start < rays.len() {
        let mut end = start.wrapping_add(1);
        while end < rays.len()
            && compare_rays(query, rays[start].end, rays[end].end) == Ordering::Equal
        {
            end = end.wrapping_add(1);
        }
        groups.push(RayGroup { start, end });
        start = end;
    }
    groups
}

#[expect(
    clippy::float_cmp,
    reason = "exact source-coordinate equality distinguishes contact from an open ray crossing"
)]
pub(in crate::geometry) fn compare_rays(query: XY, left: XY, right: XY) -> Ordering {
    let left_upper = left.y > query.y || (left.y == query.y && left.x > query.x);
    let right_upper = right.y > query.y || (right.y == query.y && right.x > query.x);
    if left_upper != right_upper {
        return right_upper.cmp(&left_upper);
    }
    match stored_orientation_sign(query.x, query.y, left.x, left.y, right.x, right.y) {
        ExactSign::Positive => Ordering::Less,
        ExactSign::Negative => Ordering::Greater,
        ExactSign::Zero => Ordering::Equal,
    }
}

/// Directed winding and exact source incidence for one stored ring.
#[cfg(test)]
pub(super) fn ring_probe_columns(xs: &[f64], ys: &[f64], point: XY) -> RingProbe {
    let mut winding = 0_i64;
    let mut incident = false;
    for_each_xy_segment(xs, ys, |start, end| match edge_winding(start, end, point) {
        Some(delta) => winding = winding.wrapping_add(delta),
        None => incident = true,
    });
    RingProbe { winding, incident }
}

/// One stored edge's contribution to the winding number at `query`: its
/// signed upward or downward crossing of the rightward ray, or `None` when
/// the query lies on the edge.
pub(crate) fn edge_winding(start: XY, end: XY, query: XY) -> Option<i64> {
    if same_point(start, end) {
        return Some(0);
    }
    if point_on_segment(query.point(), start, end) {
        return None;
    }
    let (direction, sign) = if start.y <= query.y && query.y < end.y {
        (1, ExactSign::Positive)
    } else if end.y <= query.y && query.y < start.y {
        (-1, ExactSign::Negative)
    } else {
        return Some(0);
    };
    let orientation = stored_orientation_sign(start.x, start.y, end.x, end.y, query.x, query.y);
    Some(if orientation == sign { direction } else { 0 })
}

pub(in crate::geometry) fn exact_edge_winding(
    start: XY,
    end: XY,
    point: &[ExactDyadic; 3],
) -> Result<Option<i64>> {
    edge_winding_for_query(start, end, ExactQuery::new(point)?)
}

pub(in crate::geometry) fn exact_offset_edge_winding(
    start: XY,
    end: XY,
    point: &[ExactDyadic; 3],
    direction: &[ExactDyadic; 2],
) -> Result<Option<i64>> {
    edge_winding_for_query(start, end, InfinitesimalQuery {
        point: ExactQuery::new(point)?,
        direction,
    })
}

fn edge_winding_for_query(start: XY, end: XY, query: impl PointQuery) -> Result<Option<i64>> {
    if same_point(start, end) {
        return Ok(Some(0));
    }
    let (x_start, x_end) = (
        query.compare_axis(QueryAxis::X, start.x)?,
        query.compare_axis(QueryAxis::X, end.x)?,
    );
    let (y_start, y_end) = (
        query.compare_axis(QueryAxis::Y, start.y)?,
        query.compare_axis(QueryAxis::Y, end.y)?,
    );
    let outside_x = (x_start == Ordering::Less && x_end == Ordering::Less)
        || (x_start == Ordering::Greater && x_end == Ordering::Greater);
    let outside_y = (y_start == Ordering::Less && y_end == Ordering::Less)
        || (y_start == Ordering::Greater && y_end == Ordering::Greater);
    let orientation = if !outside_x && !outside_y {
        let sign = query.orientation(start, end)?;
        if sign == ExactSign::Zero {
            return Ok(None);
        }
        Some(sign)
    } else {
        None
    };
    let direction = if y_start != Ordering::Less && y_end == Ordering::Less {
        1
    } else if y_end != Ordering::Less && y_start == Ordering::Less {
        -1
    } else {
        return Ok(Some(0));
    };
    let orientation = match orientation {
        Some(sign) => sign,
        None => query.orientation(start, end)?,
    };
    let expected = if direction > 0 {
        ExactSign::Positive
    } else {
        ExactSign::Negative
    };
    Ok(Some(if orientation == expected {
        direction
    } else {
        0
    }))
}

#[cfg(test)]
pub(super) fn formal_ring_winding(xs: &[f64], ys: &[f64], point: XY) -> i64 {
    let mut winding = 0_i64;
    for_each_xy_segment(xs, ys, |start, end| {
        if same_point(start, end) {
            return;
        }
        let start_above = start.y > point.y;
        let end_above = end.y > point.y;
        if start_above == end_above {
            return;
        }
        let orientation = symbolic_orientation(start, end, point);
        if !start_above && end_above && orientation == ExactSign::Positive {
            winding = winding.wrapping_add(1);
        } else if !end_above && start_above && orientation == ExactSign::Negative {
            winding = winding.wrapping_sub(1);
        }
    });
    winding
}

#[cfg(test)]
pub(super) fn symbolic_orientation(start: XY, end: XY, point: XY) -> ExactSign {
    let direct = stored_orientation_sign(start.x, start.y, end.x, end.y, point.x, point.y);
    if direct != ExactSign::Zero {
        return direct;
    }
    match start.y.total_cmp(&end.y) {
        Ordering::Less => ExactSign::Negative,
        Ordering::Greater => ExactSign::Positive,
        Ordering::Equal => match start.x.total_cmp(&end.x) {
            Ordering::Less => ExactSign::Positive,
            Ordering::Greater => ExactSign::Negative,
            Ordering::Equal => ExactSign::Zero,
        },
    }
}

#[cfg(test)]
pub(super) fn ring_kind(ring: &Ring) -> RingKind {
    ring_kind_columns(ring.xs(), ring.ys())
}

#[expect(
    clippy::float_cmp,
    reason = "exact source-coordinate equality distinguishes contact from an open ray crossing"
)]
pub(crate) fn ring_kind_columns(xs: &[f64], ys: &[f64]) -> RingKind {
    let Some((&ax, &ay)) = xs.first().zip(ys.first()) else {
        return RingKind::Collapsed;
    };
    let Some((&bx, &by)) = xs.iter().zip(ys).find(|(x, y)| **x != ax || **y != ay) else {
        return RingKind::Collapsed;
    };
    let collinear = xs
        .iter()
        .zip(ys)
        .all(|(&x, &y)| stored_orientation_sign(ax, ay, bx, by, x, y) == ExactSign::Zero);
    if collinear {
        RingKind::Collapsed
    } else {
        RingKind::Winding
    }
}

#[cfg(test)]
#[expect(
    clippy::float_cmp,
    reason = "exact source-coordinate equality distinguishes contact from an open ray crossing"
)]
pub(super) fn has_nonzero_segment(xs: &[f64], ys: &[f64]) -> bool {
    xs.array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .any(|([ax, bx], [ay, by])| *ax != *bx || *ay != *by)
}

#[cfg(test)]
pub(super) fn ring_has_point(xs: &[f64], ys: &[f64], point: XY) -> bool {
    let query = point.point();
    let mut hit = false;
    for_each_xy_segment(xs, ys, |start, end| {
        hit |= point_on_segment(query, start, end);
    });
    hit
}

#[cfg(test)]
pub(super) fn curve_has_point(xs: &[f64], ys: &[f64], point: XY) -> bool {
    ring_has_point(xs, ys, point)
}

#[cfg(test)]
pub(super) fn for_each_ring_segment(xs: &[f64], ys: &[f64], mut visit: impl FnMut(XY, XY)) {
    if xs.len() < 2 || ys.len() < 2 {
        return;
    }
    let (Some(&first_x), Some(&first_y), Some(&last_x), Some(&last_y)) =
        (xs.first(), ys.first(), xs.last(), ys.last())
    else {
        return;
    };
    let first = XY::new(first_x, first_y);
    let last = XY::new(last_x, last_y);
    for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        visit(XY::new(*ax, *ay), XY::new(*bx, *by));
    }
    if !same_point(first, last) {
        visit(last, first);
    }
}

#[cfg(test)]
pub(super) fn for_each_xy_segment(xs: &[f64], ys: &[f64], mut visit: impl FnMut(XY, XY)) {
    for ([ax, bx], [ay, by]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        visit(XY::new(*ax, *ay), XY::new(*bx, *by));
    }
}
