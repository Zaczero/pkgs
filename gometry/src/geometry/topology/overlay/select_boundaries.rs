use super::{
    BoundaryCycle, FaceUnion, GeometryErrorKind, NonZeroUsize, QDartId, ResourceError, Result, Work,
};

pub(in crate::geometry::topology) fn selected_area_boundaries(
    q: &super::super::q::ExactGeographicQ,
    selected_faces: &[bool],
    regions: &mut FaceUnion,
    work: &mut impl Work,
) -> Result<Vec<BoundaryCycle>> {
    if selected_faces.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    work.charge_into_error(
        u64::try_from(
            q.darts
                .len()
                .checked_mul(2)
                .and_then(|darts| darts.checked_add(q.nodes.len()))
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut boundary = vec![false; q.darts.len()];
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        work.charge_into_error(1)?;
        let Some([left, right]) = faces else {
            continue;
        };
        let left_selected = selected_faces
            .get(left.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown left face".into()))?;
        let right_selected = selected_faces
            .get(right.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown right face".into()))?;
        if left_selected == right_selected {
            continue;
        }
        let forward = q
            .span_darts
            .get(span_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a selected Q face boundary has no dart".into())
            })?;
        let dart = if left_selected {
            forward
        } else {
            QDartId(forward.0 ^ 1)
        };
        let slot = boundary.get_mut(dart.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("a Q face boundary dart is unknown".into())
        })?;
        *slot = true;
    }

    Ok(boundary_cycles_by_owner(
        q,
        |dart| {
            boundary
                .get(dart.0 as usize)
                .copied()
                .map(|selected| selected.then_some(0))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into()).into()
                })
        },
        regions,
        work,
    )?
    .into_iter()
    .map(|(_, cycle)| cycle)
    .collect())
}
pub(in crate::geometry::topology) fn coverage_area_boundaries(
    q: &super::super::q::ExactGeographicQ,
    face_owners: &[Option<usize>],
    regions: &mut FaceUnion,
    work: &mut impl Work,
) -> Result<Vec<(usize, BoundaryCycle)>> {
    if face_owners.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "coverage face owners do not match Q's global face table".into(),
        )
        .into());
    }
    let mut boundary = vec![None; q.darts.len()];
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        work.charge_into_error(1)?;
        let Some([left, right]) = faces else {
            continue;
        };
        let left_owner = face_owners.get(left.0 as usize).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span has an unknown left coverage owner".into())
        })?;
        let right_owner = face_owners.get(right.0 as usize).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q span has an unknown right coverage owner".into())
        })?;
        if left_owner == right_owner {
            continue;
        }
        let forward = q
            .span_darts
            .get(span_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a coverage Q boundary has no dart".into())
            })?;
        if let Some(row) = left_owner {
            set_coverage_boundary_owner(&mut boundary, forward, row)?;
        }
        if let Some(row) = right_owner {
            set_coverage_boundary_owner(&mut boundary, QDartId(forward.0 ^ 1), row)?;
        }
    }

    boundary_cycles_by_owner(
        q,
        |dart| {
            let owner = boundary.get(dart.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q boundary dart is unknown".into())
            })?;
            let owner = owner
                .map(|owner| {
                    owner
                        .get()
                        .checked_sub(1)
                        .ok_or(ResourceError::SizeOverflow)
                })
                .transpose()?;
            Ok(owner)
        },
        regions,
        work,
    )
}
pub(in crate::geometry::topology) fn set_coverage_boundary_owner(
    boundary: &mut [Option<NonZeroUsize>],
    dart: QDartId,
    row: usize,
) -> Result<()> {
    let encoded_row = row
        .checked_add(1)
        .and_then(NonZeroUsize::new)
        .ok_or(ResourceError::SizeOverflow)?;
    let slot = boundary
        .get_mut(dart.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage Q boundary dart is unknown".into()))?;
    if slot.replace(encoded_row).is_some() {
        return Err(GeometryErrorKind::Invalid(
            "a coverage Q boundary dart was selected more than once".into(),
        )
        .into());
    }
    Ok(())
}
pub(in crate::geometry::topology) fn boundary_cycles_by_owner(
    q: &super::super::q::ExactGeographicQ,
    mut owner_of: impl FnMut(QDartId) -> Result<Option<usize>>,
    regions: &mut FaceUnion,
    work: &mut impl Work,
) -> Result<Vec<(usize, BoundaryCycle)>> {
    work.charge_into_error(
        u64::try_from(
            q.darts
                .len()
                .checked_mul(2)
                .and_then(|darts| darts.checked_add(q.nodes.len()))
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut visited = vec![false; q.darts.len()];
    let mut positions = vec![None; q.nodes.len()];
    let mut walks = Vec::new();
    for seed in 0..q.darts.len() {
        work.charge_into_error(1)?;
        let start = QDartId(u32::try_from(seed).map_err(|_| ResourceError::SizeOverflow)?);
        let Some(owner) = owner_of(start)? else {
            continue;
        };
        let was_visited = visited.get(seed).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q overlay boundary has no visited state".into())
        })?;
        if was_visited {
            continue;
        }
        let mut current = start;
        let mut walk = Vec::new();
        loop {
            work.charge_into_error(1)?;
            let current_index = current.0 as usize;
            if visited.get(current_index).copied().unwrap_or(false) {
                if current == start {
                    break;
                }
                return Err(GeometryErrorKind::Invalid(
                    "Q overlay boundary walks intersect before closing".into(),
                )
                .into());
            }
            *visited.get_mut(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into())
            })? = true;
            let dart = *q.darts.get(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q overlay boundary dart is unknown".into())
            })?;
            *positions
                .get_mut(dart.from_node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q boundary starts at an unknown node".into())
                })? = Some(walk.len());
            walk.push(current);

            if let Some(begin) = positions.get(dart.to_node.0 as usize).copied().flatten() {
                let mut cycle = Vec::with_capacity(walk.len().saturating_sub(begin));
                for dart in walk.drain(begin..) {
                    work.charge_into_error(1)?;
                    let node = q
                        .darts
                        .get(dart.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q boundary cycle has an unknown dart".into(),
                            )
                        })?
                        .from_node;
                    *positions.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q boundary cycle has an unknown node".into())
                    })? = None;
                    cycle.push(dart);
                }
                if !cycle.is_empty() {
                    walks.push((owner, cycle));
                }
            }
            current = next_boundary_dart_by_owner(q, current, owner, &mut owner_of, work)?;
            if current == start {
                break;
            }
        }
        if !walk.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "Q overlay boundary did not decompose into closed cycles".into(),
            )
            .into());
        }
    }
    for (index, was_visited) in visited.into_iter().enumerate() {
        work.charge_into_error(1)?;
        let dart = QDartId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?);
        if owner_of(dart)?.is_some() && !was_visited {
            return Err(GeometryErrorKind::Invalid(
                "Q overlay left a selected boundary dart unvisited".into(),
            )
            .into());
        }
    }

    let mut cycles = Vec::with_capacity(walks.len());
    for (owner, darts) in walks {
        work.charge_into_error(1)?;
        let first = *darts.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q overlay emitted an empty boundary cycle".into())
        })?;
        let local = *q.dart_faces.get(first.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary cycle has no left face".into())
        })?;
        let face = *q
            .global_face_of_local
            .get(local.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle left face has no global ID".into())
            })?;
        let region = regions.selected_root(face, work)?;
        cycles.push((owner, BoundaryCycle { darts, region }));
    }
    Ok(cycles)
}
pub(in crate::geometry::topology) fn next_boundary_dart_by_owner(
    q: &super::super::q::ExactGeographicQ,
    dart_id: QDartId,
    owner: usize,
    owner_of: &mut impl FnMut(QDartId) -> Result<Option<usize>>,
    work: &mut impl Work,
) -> Result<QDartId> {
    let dart = q
        .darts
        .get(dart_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
    let twin = QDartId(dart_id.0 ^ 1);
    let twin_position = *q
        .dart_positions
        .get(twin.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary twin has no fan position".into()))?;
    let start = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node has no fan start".into()))?;
    let end = *q
        .outgoing_offsets
        .get(dart.to_node.0 as usize + 1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node has no fan end".into()))?;
    let length = end.checked_sub(start).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q boundary node has reversed fan offsets".into())
    })?;
    if length == 0 || twin_position >= length {
        return Err(
            GeometryErrorKind::Invalid("Q boundary twin is outside its node fan".into()).into(),
        );
    }
    let fan = q
        .outgoing
        .get(start..end)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node fan is incomplete".into()))?;
    let (before_twin, twin_and_after) = fan.split_at(twin_position);
    let (twin, after_twin) = twin_and_after.split_first().ok_or_else(|| {
        GeometryErrorKind::Invalid("Q boundary twin is outside its node fan".into())
    })?;
    for &candidate in std::iter::once(twin)
        .chain(before_twin.iter().rev())
        .chain(after_twin.iter().rev())
    {
        work.charge_into_error(1)?;
        if owner_of(candidate)? == Some(owner) {
            return Ok(candidate);
        }
    }
    Err(GeometryErrorKind::Invalid("a selected Q boundary dart has no successor".into()).into())
}
