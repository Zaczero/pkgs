use std::collections::BTreeSet;
use std::ops::ControlFlow;
#[cfg(test)]
use std::{cell::Cell, thread_local};

use super::points::ExactPoints;
use crate::error::{ResourceError, Result};
use crate::geometry::topology::{
    ExactChartPoint, exact_segments_intersect_with_work, point_on_exact_segment_with_work,
};
use crate::geometry::{
    Bounds, GeometryErrorKind, Work, XY, for_each_overlapping_bounds_cross_pair_with_work,
};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ContactCheckProgress {
    pub(super) bounds_attempted: usize,
    pub(super) bounds_completed: usize,
    pub(super) identity_attempted: usize,
    pub(super) identity_completed: usize,
}

#[cfg(test)]
pub(super) fn reset_contact_check_progress() {
    CONTACT_CHECK_PROGRESS.with(|progress| progress.set(ContactCheckProgress::default()));
}

#[cfg(test)]
pub(super) fn contact_check_progress() -> ContactCheckProgress {
    CONTACT_CHECK_PROGRESS.with(Cell::get)
}

#[cfg(test)]
fn record_bounds_attempt() {
    CONTACT_CHECK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.bounds_attempted = value.bounds_attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_bounds_complete() {
    CONTACT_CHECK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.bounds_completed = value.bounds_completed.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_identity_attempt() {
    CONTACT_CHECK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.identity_attempted = value.identity_attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_identity_complete() {
    CONTACT_CHECK_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.identity_completed = value.identity_completed.wrapping_add(1);
        progress.set(value);
    });
}

fn outward_bounds(point: &ExactChartPoint, work: &mut impl Work) -> Result<Bounds> {
    #[cfg(test)]
    record_bounds_attempt();
    let [min_x, min_y, max_x, max_y] = point.outward_bounds_with_work(work)?;
    #[cfg(test)]
    record_bounds_complete();
    Ok(Bounds::new_raw(min_x, min_y, max_x, max_y))
}

fn outward_segment_bounds(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<Bounds> {
    #[cfg(test)]
    record_bounds_attempt();
    let [start_min_x, start_min_y, start_max_x, start_max_y] =
        start.outward_bounds_with_work(work)?;
    let [end_min_x, end_min_y, end_max_x, end_max_y] = end.outward_bounds_with_work(work)?;
    #[cfg(test)]
    record_bounds_complete();
    Ok(Bounds::new_raw(
        start_min_x.min(end_min_x),
        start_min_y.min(end_min_y),
        start_max_x.max(end_max_x),
        start_max_y.max(end_max_y),
    ))
}

pub(super) fn point_edge_contacts_preserved(
    reference_points: &ExactPoints,
    candidate_points: &ExactPoints,
    isolated_vertices: &[u32],
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<bool> {
    fn contacts(
        points: &ExactPoints,
        isolated_vertices: &[u32],
        edge_nodes: &[[u32; 2]],
        work: &mut impl Work,
    ) -> Result<BTreeSet<(u32, usize)>> {
        work.charge_into_error(
            u64::try_from(isolated_vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut point_bounds = Vec::with_capacity(isolated_vertices.len());
        for &vertex in isolated_vertices {
            work.charge_into_error(1)?;
            let point = points.exact.get(vertex as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("isolated graph point is unknown".into())
            })?;
            point_bounds.push(outward_bounds(point, work)?);
        }
        work.charge_into_error(
            u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut edge_bounds = Vec::with_capacity(edge_nodes.len());
        for &[start, end] in edge_nodes {
            work.charge_into_error(1)?;
            edge_bounds.push(outward_segment_bounds(
                points.exact.get(start as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("point-contact edge start is unknown".into())
                })?,
                points.exact.get(end as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("point-contact edge end is unknown".into())
                })?,
                work,
            )?);
        }
        let mut result = BTreeSet::new();
        let completion = for_each_overlapping_bounds_cross_pair_with_work(
            &point_bounds,
            &edge_bounds,
            work,
            |point_index, edge_index, work| {
                work.charge_into_error(1)?;
                let Some(vertex) = isolated_vertices.get(point_index).copied() else {
                    return Err(GeometryErrorKind::Invalid(
                        "point-contact sweep returned an unknown point".into(),
                    )
                    .into());
                };
                let Some(edge) = edge_nodes.get(edge_index) else {
                    return Err(GeometryErrorKind::Invalid(
                        "point-contact sweep returned an unknown edge".into(),
                    )
                    .into());
                };
                let [start, end] = *edge;
                let (Some(point), Some(start), Some(end)) = (
                    points.exact.get(vertex as usize),
                    points.exact.get(start as usize),
                    points.exact.get(end as usize),
                ) else {
                    return Err(GeometryErrorKind::Invalid(
                        "point-contact graph row is outside its point inventory".into(),
                    )
                    .into());
                };
                if point_on_exact_segment_with_work(point, start, end, work)? {
                    work.charge_into_error(1)?;
                    result.insert((vertex, edge_index));
                }
                Ok(ControlFlow::<()>::Continue(()))
            },
        )?;
        debug_assert!(completion.is_continue());
        Ok(result)
    }

    Ok(
        contacts(reference_points, isolated_vertices, edge_nodes, work)?
            == contacts(candidate_points, isolated_vertices, edge_nodes, work)?,
    )
}

pub(super) fn edges_have_only_vertex_contacts(
    points: &ExactPoints,
    edge_nodes: &[[u32; 2]],
    work: &mut impl Work,
) -> Result<bool> {
    work.charge_into_error(
        u64::try_from(edge_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut bounds = Vec::with_capacity(edge_nodes.len());
    for &[start, end] in edge_nodes {
        work.charge_into_error(1)?;
        let start = points
            .exact
            .get(start as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("candidate edge start is unknown".into()))?;
        let end = points
            .exact
            .get(end as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("candidate edge end is unknown".into()))?;
        #[cfg(test)]
        record_identity_attempt();
        let same_position = start.try_eq_with_work(end, work)?;
        #[cfg(test)]
        record_identity_complete();
        if same_position {
            return Ok(false);
        }
        bounds.push(outward_segment_bounds(start, end, work)?);
    }
    let completion = for_each_overlapping_bounds_cross_pair_with_work(
        &bounds,
        &bounds,
        work,
        |left_index, right_index, work| {
            if left_index >= right_index {
                return Ok(ControlFlow::Continue(()));
            }
            work.charge_into_error(1)?;
            let (Some([left_start, left_end]), Some([right_start, right_end])) =
                (edge_nodes.get(left_index), edge_nodes.get(right_index))
            else {
                return Err(GeometryErrorKind::Invalid(
                    "candidate edge sweep returned an unknown edge".into(),
                )
                .into());
            };
            let (
                Some(left_start_point),
                Some(left_end_point),
                Some(right_start_point),
                Some(right_end_point),
            ) = (
                points.exact.get(*left_start as usize),
                points.exact.get(*left_end as usize),
                points.exact.get(*right_start as usize),
                points.exact.get(*right_end as usize),
            )
            else {
                return Err(GeometryErrorKind::Invalid(
                    "candidate edge endpoint is outside its point inventory".into(),
                )
                .into());
            };
            match exact_segments_intersect_with_work(
                left_start_point,
                left_end_point,
                right_start_point,
                right_end_point,
                work,
            ) {
                Err(error) => Err(error),
                Ok(false) => Ok(ControlFlow::Continue(())),
                Ok(true) => {
                    let shared = [*left_start, *left_end]
                        .into_iter()
                        .find(|vertex| [*right_start, *right_end].contains(vertex));
                    let Some(shared) = shared else {
                        return Ok(ControlFlow::Break(()));
                    };
                    let left_other = if *left_start == shared {
                        *left_end
                    } else {
                        *left_start
                    };
                    let right_other = if *right_start == shared {
                        *right_end
                    } else {
                        *right_start
                    };
                    let (Some(left_other_point), Some(right_other_point)) = (
                        points.exact.get(left_other as usize),
                        points.exact.get(right_other as usize),
                    ) else {
                        return Err(GeometryErrorKind::Invalid(
                            "candidate edge has no nonshared endpoint".into(),
                        )
                        .into());
                    };
                    let left_on_right = point_on_exact_segment_with_work(
                        left_other_point,
                        right_start_point,
                        right_end_point,
                        work,
                    );
                    let right_on_left = point_on_exact_segment_with_work(
                        right_other_point,
                        left_start_point,
                        left_end_point,
                        work,
                    );
                    match (left_on_right, right_on_left) {
                        (Ok(false), Ok(false)) => Ok(ControlFlow::Continue(())),
                        (Ok(_), Ok(_)) => Ok(ControlFlow::Break(())),
                        (Err(error), _) | (_, Err(error)) => Err(error),
                    }
                },
            }
        },
    )?;
    Ok(completion.is_continue())
}
