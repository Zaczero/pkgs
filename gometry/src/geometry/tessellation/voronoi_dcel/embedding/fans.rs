//! Rounded fan and ring orientation certificates.

use super::super::{
    EdgeId, ExactSign, ExactSubdivision, Ordering, Result, RingWinding, VertexId, XY, exact,
    fan_target, open_xy_cycle_winding, try_sort_by,
};

#[expect(
    clippy::float_cmp,
    reason = "certified Voronoi topology classifies exact frame and vertex identity before rounding"
)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn rounded_fan_angle_cmp(
    origin: XY,
    left: XY,
    right: XY,
) -> Option<Ordering> {
    let half = |target: XY| target.y > origin.y || (target.y == origin.y && target.x >= origin.x);
    match (half(left), half(right)) {
        (true, false) => Some(Ordering::Less),
        (false, true) => Some(Ordering::Greater),
        _ => match exact::orient2d(origin, left, right) {
            ExactSign::Positive => Some(Ordering::Less),
            ExactSign::Negative => Some(Ordering::Greater),
            ExactSign::Zero => None,
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FanSortMode {
    Exact,
    Rounded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FanSortOutcome {
    Sorted,
    AngularTie,
}

pub(super) fn sort_fan(
    vertex: VertexId,
    incident: &[EdgeId],
    mode: FanSortMode,
    subdivision: &ExactSubdivision,
    xy_of: &impl Fn(VertexId) -> Result<XY>,
    order: &mut Vec<EdgeId>,
) -> Result<FanSortOutcome> {
    let exact_origin = &subdivision.vertex(vertex).point;
    let rounded_origin = (mode == FanSortMode::Rounded)
        .then(|| xy_of(vertex))
        .transpose()?;
    let compare = |left: EdgeId, right: EdgeId| -> Result<Option<Ordering>> {
        let left = fan_target(subdivision, left, vertex);
        let right = fan_target(subdivision, right, vertex);
        if let Some(origin) = rounded_origin {
            Ok(rounded_fan_angle_cmp(origin, xy_of(left)?, xy_of(right)?))
        } else {
            exact::incidence::angle_ccw_cmp(
                exact_origin,
                &subdivision.vertex(left).point,
                &subdivision.vertex(right).point,
            )
            .map(Some)
        }
    };

    order.clear();
    order.reserve(incident.len());
    order.extend_from_slice(incident);
    try_sort_by(order, |&left, &right| {
        Ok(compare(left, right)?.unwrap_or(Ordering::Equal))
    })?;
    for &[left, right] in order.array_windows::<2>() {
        if compare(left, right)?.is_none() {
            return Ok(FanSortOutcome::AngularTie);
        }
    }
    if let Some((&first, rest)) = order.split_first()
        && let Some(&last) = rest.last()
        && compare(last, first)?.is_none()
    {
        return Ok(FanSortOutcome::AngularTie);
    }
    if let Some((least, _)) = order.iter().enumerate().min_by_key(|(_, edge)| *edge) {
        order.rotate_left(least);
    }
    Ok(FanSortOutcome::Sorted)
}

pub(super) fn rounded_ring_orientation_matches(
    vertices: &[VertexId],
    orientation: ExactSign,
    mut xy_of: impl FnMut(VertexId) -> Result<XY>,
) -> Result<bool> {
    let (_, open_vertices) = vertices
        .split_last()
        .expect("simple Voronoi rings are closed");
    let rounded = open_vertices
        .iter()
        .map(|&vertex| xy_of(vertex))
        .collect::<Result<Vec<_>>>()?;
    Ok(stored_xy_cycle_matches_orientation(&rounded, orientation))
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn stored_xy_cycle_matches_orientation(
    rounded: &[XY],
    orientation: ExactSign,
) -> bool {
    match open_xy_cycle_winding(rounded) {
        RingWinding::Clockwise => orientation == ExactSign::Negative,
        RingWinding::CounterClockwise => orientation == ExactSign::Positive,
        RingWinding::Degenerate => false,
    }
}
