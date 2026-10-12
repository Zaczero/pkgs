//! Geographic linework selection over one exact quotient chart.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "public producer routing starts when the shared F4 receiver lands"
    )
)]

use std::collections::VecDeque;

use super::exact::SourceRowId;
use super::q::{ExactGeographicTopology, GlobalFaceId, QDartId};
use crate::error::{ResourceError, Result};
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::{GeometryErrorKind, Work};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DiagnosticKind {
    Cut,
    Dangle,
    InvalidRing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NodedSpan {
    pub(super) span: usize,
    pub(super) owner: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DiagnosticSpan {
    pub(super) span: usize,
    pub(super) owner: usize,
    pub(super) row: SourceRowId,
    pub(super) kind: DiagnosticKind,
}

#[derive(Clone, Debug)]
pub(super) struct FaceBoundaryCycle {
    pub(super) face: GlobalFaceId,
    pub(super) darts: Vec<QDartId>,
}

pub(super) fn noded_spans(
    topology: &ExactGeographicTopology,
    work: &mut impl Work,
) -> Result<Vec<NodedSpan>> {
    let q = &topology.q;
    work.charge(u64::try_from(q.spans.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut output = Vec::with_capacity(q.spans.len());
    for (span_index, span) in q.spans.iter().enumerate() {
        work.charge(1)?;
        if span.nodes.first() == span.nodes.get(1) {
            continue;
        }
        if span.owners.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "a selected geographic linework span has no source owner".into(),
            )
            .into());
        }
        output.push(NodedSpan {
            span: span_index,
            owner: 0,
        });
    }
    Ok(output)
}

pub(super) fn minimal_face_selection(
    topology: &ExactGeographicTopology,
    work: &mut impl Work,
) -> Result<Vec<bool>> {
    let face_count = topology.q.global_face_count;
    work.charge(u64::try_from(face_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut selected = Vec::with_capacity(face_count);
    selected.extend((0..face_count).map(|face| face != 0));
    Ok(selected)
}

pub(super) fn face_cycle_hole_roles(
    topology: &ExactGeographicTopology,
    cycles: &[FaceBoundaryCycle],
    work: &mut impl Work,
) -> Result<Vec<bool>> {
    let q = &topology.q;
    let mut roles = Vec::with_capacity(cycles.len());
    for cycle in cycles {
        work.charge(1)?;
        let capacity = cycle
            .darts
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        work.charge(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut points = Vec::with_capacity(capacity);
        for &dart_id in &cycle.darts {
            work.charge(1)?;
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q linework cycle references an unknown dart".into())
            })?;
            let port = q.ports.get(dart.from_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q linework cycle references an unknown port".into())
            })?;
            points.push(port.point.try_clone_with_work(work)?);
        }
        let first = points.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q linework cycle has no chart vertices".into())
        })?;
        points.push(first.try_clone_with_work(work)?);
        roles.push(super::q_embedding::exact_q_ring_role_for_linework(
            topology.sources.frame,
            &points,
            work,
        )?);
    }
    Ok(roles)
}

/// Alternate parity only when crossing a face hole. Shared edges between
/// adjacent shell faces do not change fill depth.
pub(super) fn even_odd_face_selection(
    topology: &ExactGeographicTopology,
    cycles: &[FaceBoundaryCycle],
    hole_roles: &[bool],
    work: &mut impl Work,
) -> Result<Vec<bool>> {
    let q = &topology.q;
    let face_count = q.global_face_count;
    if cycles.len() != hole_roles.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q face cycles do not match their exact ring roles".into(),
        )
        .into());
    }
    work.charge(
        u64::try_from(
            face_count
                .checked_add(cycles.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut children = Vec::with_capacity(face_count);
    children.resize_with(face_count, Vec::<GlobalFaceId>::new);
    let mut parent = vec![None; face_count];
    for (cycle, &is_hole) in cycles.iter().zip(hole_roles) {
        work.charge(1)?;
        if !is_hole {
            continue;
        }
        if cycle.face.0 == 0 {
            return Err(GeometryErrorKind::Invalid(
                "the Q exterior face cannot own a selected hole cycle".into(),
            )
            .into());
        }
        let mut child_faces = Vec::with_capacity(cycle.darts.len());
        for &dart in &cycle.darts {
            work.charge(1)?;
            let opposite = dart_global_face(q, QDartId(dart.0 ^ 1))?;
            if opposite == cycle.face {
                continue;
            }
            if opposite.0 == 0 {
                return Err(GeometryErrorKind::Invalid(
                    "a geographic linework hole reaches the Q exterior face".into(),
                )
                .into());
            }
            child_faces.push(opposite);
        }
        if child_faces.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "a geographic linework hole has no child face".into(),
            )
            .into());
        }
        try_sort_by_with_work(&mut child_faces, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge(u64::try_from(child_faces.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        child_faces.dedup();
        for child in child_faces {
            work.charge(1)?;
            let parent_slot = parent.get_mut(child.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("a geographic linework child face is unknown".into())
            })?;
            if parent_slot.is_some_and(|previous| previous != cycle.face) {
                return Err(GeometryErrorKind::Invalid(
                    "a geographic linework face has multiple enclosing holes".into(),
                )
                .into());
            }
            if parent_slot.is_none() {
                *parent_slot = Some(cycle.face);
                children
                    .get_mut(cycle.face.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "a geographic linework parent face is unknown".into(),
                        )
                    })?
                    .push(child);
            }
        }
    }

    let mut depths = vec![None; face_count];
    *depths
        .get_mut(0)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q has no exterior face".into()))? = Some(-1_i64);
    let mut pending = VecDeque::new();
    for face in 1..face_count {
        work.charge(1)?;
        if parent.get(face).copied().flatten().is_none() {
            *depths.get_mut(face).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q linework root face is unknown".into())
            })? = Some(0);
            pending.push_back(GlobalFaceId(
                u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?,
            ));
        }
    }
    while let Some(host) = pending.pop_front() {
        work.charge(1)?;
        let host_depth = depths
            .get(host.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q face nesting depth is missing".into()))?;
        let child_depth = host_depth
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let hosted = children.get(host.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q linework parent face is unknown".into())
        })?;
        for &child in hosted {
            work.charge(1)?;
            let depth = depths.get_mut(child.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q linework child face is unknown".into())
            })?;
            match *depth {
                Some(previous) if previous != child_depth => {
                    return Err(GeometryErrorKind::Invalid(
                        "Q hole cycles give a face conflicting nesting depths".into(),
                    )
                    .into());
                },
                Some(_) => {},
                None => {
                    *depth = Some(child_depth);
                    pending.push_back(child);
                },
            }
        }
    }

    let mut selected = Vec::with_capacity(face_count);
    for depth in depths {
        work.charge(1)?;
        let depth = depth.ok_or_else(|| {
            GeometryErrorKind::Invalid("Q hole cycles do not reach every global face".into())
        })?;
        selected.push(depth.rem_euclid(2) == 0);
    }
    Ok(selected)
}

pub(super) fn minimal_face_boundaries(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    work: &mut impl Work,
) -> Result<Vec<FaceBoundaryCycle>> {
    let q = &topology.q;
    if selected_faces.len() != q.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "Q face selection does not match its global face table".into(),
        )
        .into());
    }
    work.charge(
        u64::try_from(
            q.darts
                .len()
                .checked_add(q.spans.len())
                .and_then(|count| count.checked_add(q.nodes.len()))
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut boundary = vec![false; q.darts.len()];
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        work.charge(1)?;
        let Some([left, right]) = faces else {
            continue;
        };
        if left == right {
            continue;
        }
        let forward = q
            .span_darts
            .get(span_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("a selected Q face has no boundary dart".into())
            })?;
        if selected_faces
            .get(left.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown left face".into()))?
            && left.0 != 0
        {
            *boundary
                .get_mut(forward.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))? =
                true;
        }
        if selected_faces
            .get(right.0 as usize)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown right face".into()))?
            && right.0 != 0
        {
            *boundary
                .get_mut((forward.0 ^ 1) as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary twin is unknown".into()))? =
                true;
        }
    }
    walk_minimal_face_boundaries(q, &boundary, selected_faces, work)
}

fn walk_minimal_face_boundaries(
    q: &super::q::ExactGeographicQ,
    boundary: &[bool],
    selected_faces: &[bool],
    work: &mut impl Work,
) -> Result<Vec<FaceBoundaryCycle>> {
    let mut visited = vec![false; q.darts.len()];
    let mut positions = vec![None; q.nodes.len()];
    let mut cycles = Vec::new();
    for (seed, &is_boundary) in boundary.iter().enumerate() {
        work.charge(1)?;
        if !is_boundary || visited.get(seed).copied().unwrap_or(false) {
            continue;
        }
        let start = QDartId(u32::try_from(seed).map_err(|_| ResourceError::SizeOverflow)?);
        let face = dart_global_face(q, start)?;
        if !selected_faces
            .get(face.0 as usize)
            .copied()
            .unwrap_or(false)
            || face.0 == 0
        {
            return Err(GeometryErrorKind::Invalid(
                "minimal-face boundary belongs to an unselected face".into(),
            )
            .into());
        }
        let mut current = start;
        let mut walk = Vec::new();
        loop {
            work.charge(1)?;
            let current_index = current.0 as usize;
            if visited.get(current_index).copied().unwrap_or(false) {
                if current == start {
                    break;
                }
                return Err(GeometryErrorKind::Invalid(
                    "minimal-face boundary walks intersect before closing".into(),
                )
                .into());
            }
            *visited.get_mut(current_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("minimal-face boundary dart is unknown".into())
            })? = true;
            if dart_global_face(q, current)? != face {
                return Err(GeometryErrorKind::Invalid(
                    "minimal-face boundary changed its left face".into(),
                )
                .into());
            }
            let dart = q
                .darts
                .get(current_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart is unknown".into()))?;
            *positions
                .get_mut(dart.from_node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary node is unknown".into()))? =
                Some(walk.len());
            walk.push(current);
            if let Some(begin) = positions.get(dart.to_node.0 as usize).copied().flatten() {
                let mut cycle = Vec::with_capacity(walk.len().saturating_sub(begin));
                for dart in walk.drain(begin..) {
                    work.charge(1)?;
                    let node = q
                        .darts
                        .get(dart.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "minimal-face cycle has an unknown dart".into(),
                            )
                        })?
                        .from_node;
                    *positions.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("minimal-face cycle node is unknown".into())
                    })? = None;
                    cycle.push(dart);
                }
                if !cycle.is_empty() {
                    cycles.push(FaceBoundaryCycle { face, darts: cycle });
                }
            }
            current = next_face_boundary_dart(q, boundary, current, face, work)?;
            if current == start {
                break;
            }
        }
        if !walk.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "minimal-face boundary did not decompose into closed cycles".into(),
            )
            .into());
        }
    }
    for (&selected, was_visited) in boundary.iter().zip(visited) {
        work.charge(1)?;
        if selected && !was_visited {
            return Err(GeometryErrorKind::Invalid(
                "Q left a minimal-face boundary dart unvisited".into(),
            )
            .into());
        }
    }
    Ok(cycles)
}

pub(super) fn diagnostic_spans(
    topology: &ExactGeographicTopology,
    work: &mut impl Work,
) -> Result<Vec<DiagnosticSpan>> {
    let q = &topology.q;
    work.charge(
        u64::try_from(
            q.spans
                .len()
                .checked_mul(2)
                .and_then(|count| count.checked_add(q.nodes.len()))
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut degree = vec![0_usize; q.nodes.len()];
    let mut incident = Vec::with_capacity(q.nodes.len());
    incident.resize_with(q.nodes.len(), Vec::<usize>::new);
    for (span_index, span) in q.spans.iter().enumerate() {
        work.charge(1)?;
        for node in span.nodes {
            let degree = degree.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has an unknown endpoint node".into())
            })?;
            *degree = degree.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            incident
                .get_mut(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span has an unknown endpoint node".into())
                })?
                .push(span_index);
        }
    }
    let mut pending = VecDeque::new();
    for (node, &degree) in degree.iter().enumerate() {
        work.charge(1)?;
        if degree == 1 {
            pending.push_back(node);
        }
    }
    let mut dangles = vec![false; q.spans.len()];
    while let Some(node) = pending.pop_front() {
        work.charge(1)?;
        if degree.get(node).copied() != Some(1) {
            continue;
        }
        let span_index = incident
            .get(node)
            .and_then(|spans| {
                spans
                    .iter()
                    .copied()
                    .find(|&span| dangles.get(span).is_some_and(|&removed| !removed))
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q dangle node has no remaining span".into())
            })?;
        *dangles
            .get_mut(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q dangle span is unknown".into()))? = true;
        let span = q
            .spans
            .get(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q dangle span is unknown".into()))?;
        for endpoint in span.nodes {
            work.charge(1)?;
            let degree = degree
                .get_mut(endpoint.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q dangle endpoint is unknown".into()))?;
            *degree = degree
                .checked_sub(1)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q dangle degree underflowed".into()))?;
            if *degree == 1 {
                pending.push_back(endpoint.0 as usize);
            }
        }
    }

    let mut output = Vec::new();
    for (span_index, span) in q.spans.iter().enumerate() {
        work.charge(1)?;
        let kind = if span.owners.len() > 1 {
            Some(DiagnosticKind::InvalidRing)
        } else if dangles.get(span_index).copied().unwrap_or(false) {
            Some(DiagnosticKind::Dangle)
        } else if q
            .span_global_faces
            .get(span_index)
            .copied()
            .flatten()
            .is_some_and(|[left, right]| left == right)
        {
            Some(DiagnosticKind::Cut)
        } else {
            None
        };
        let Some(kind) = kind else {
            continue;
        };
        let mut rows = Vec::<(SourceRowId, usize)>::with_capacity(span.owners.len());
        for (owner_index, owner) in span.owners.iter().enumerate() {
            work.charge(1)?;
            let edge = topology
                .sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q diagnostic owner has no source edge".into())
                })?;
            rows.push((edge.row, owner_index));
        }
        try_sort_by_with_work(&mut rows, work, |left, right, _work| Ok(left.cmp(right)))?;
        work.charge(u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        rows.dedup_by_key(|(row, _)| *row);
        for (row, owner) in rows {
            work.charge(1)?;
            output.push(DiagnosticSpan {
                span: span_index,
                owner,
                row,
                kind,
            });
        }
    }
    Ok(output)
}

fn next_face_boundary_dart(
    q: &super::q::ExactGeographicQ,
    selected: &[bool],
    dart_id: QDartId,
    face: GlobalFaceId,
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
        work.charge(1)?;
        if selected.get(candidate.0 as usize).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q boundary fan has an unknown dart".into())
        })? && dart_global_face(q, candidate)? == face
        {
            return Ok(candidate);
        }
    }
    Err(GeometryErrorKind::Invalid(
        "a selected minimal-face dart has no successor in its face".into(),
    )
    .into())
}

fn dart_global_face(q: &super::q::ExactGeographicQ, dart: QDartId) -> Result<GlobalFaceId> {
    let local = *q
        .dart_faces
        .get(dart.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q boundary dart has no local face".into()))?;
    q.global_face_of_local
        .get(local.0 as usize)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q local face has no global face".into()).into())
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::panic_in_result_fn,
        reason = "Result-returning tests use assertions for their behavioral checks"
    )]

    use std::collections::BTreeSet;

    use super::*;
    use crate::geographic::{GeographicFrame, GeographicInputs};
    use crate::geometry::topology::emission::RecipeWalkOwner;
    use crate::geometry::topology::exact::{SourceEdgeId, SourceEdgeKind};
    use crate::geometry::topology::overlay::{PolygonPlan, QEmissionPlan, QLineOrigin, QProducer};
    use crate::geometry::{Shape, Unmetered};

    fn topology(shapes: &[Shape]) -> Result<ExactGeographicTopology> {
        let frame = GeographicFrame::crs84();
        let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter())
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())
    }

    fn live_noded_span_count(topology: &ExactGeographicTopology) -> usize {
        topology
            .q
            .spans
            .iter()
            .filter(|span| span.nodes.first() != span.nodes.get(1))
            .count()
    }

    fn has_shared_line_node(topology: &ExactGeographicTopology) -> bool {
        topology.q.nodes.iter().any(|node| {
            let mut lines = vec![false; topology.sources.lines.len()];
            for span in &topology.q.spans {
                if span.nodes.contains(&node.id) {
                    for owner in &span.owners {
                        let Some(edge) = topology.sources.edges.get(owner.source_edge.0 as usize)
                        else {
                            continue;
                        };
                        if let SourceEdgeKind::Line { line } = edge.kind
                            && let Some(present) = lines.get_mut(line.0 as usize)
                        {
                            *present = true;
                        }
                    }
                }
            }
            lines.iter().filter(|&&present| present).count() > 1
        })
    }

    fn selected_face_count(plan: &QEmissionPlan) -> usize {
        plan.selected_face_roots
            .iter()
            .filter(|root| root.is_some())
            .count()
    }

    #[test]
    fn node_selection_uses_the_seam_junction_not_a_greenwich_crossing() -> Result<()> {
        let seam = crate::io::parse_wkt("MULTILINESTRING ((170 0, -170 0), (180 -1, 180 1))")?;
        let greenwich = crate::io::parse_wkt("MULTILINESTRING ((170 0, -170 0), (0 -1, 0 1))")?;
        let seam_topology = topology(&[seam])?;
        let greenwich_topology = topology(&[greenwich])?;
        assert!(has_shared_line_node(&seam_topology));
        assert!(!has_shared_line_node(&greenwich_topology));

        let mut work = Unmetered;
        let seam_plan = QEmissionPlan::build_node(&seam_topology, &mut work)?;
        let mut work = Unmetered;
        let greenwich_plan = QEmissionPlan::build_node(&greenwich_topology, &mut work)?;
        assert_eq!(seam_plan.producer, QProducer::Node);
        assert_eq!(
            seam_plan.line_paths.len(),
            live_noded_span_count(&seam_topology)
        );
        assert_eq!(
            greenwich_plan.line_paths.len(),
            live_noded_span_count(&greenwich_topology)
        );
        Ok(())
    }

    #[test]
    fn node_selection_keeps_all_directed_roots_on_a_shared_span() -> Result<()> {
        let first = crate::io::parse_wkt("LINESTRING (0 0, 2 0)")?;
        let second = crate::io::parse_wkt("LINESTRING (2 0, 0 0)")?;
        let topology = topology(&[first, second])?;
        let mut work = Unmetered;
        let plan = QEmissionPlan::build_node(&topology, &mut work)?;
        assert_eq!(plan.line_paths.len(), 1);

        let walk = plan
            .fragment_recipes
            .walks()
            .iter()
            .find(|walk| walk.owner == RecipeWalkOwner::Line(0))
            .ok_or_else(|| GeometryErrorKind::Invalid("shared span has no recipe walk".into()))?;
        let mut source_edges = BTreeSet::new();
        let mut forward = false;
        let mut reverse = false;
        for fragment in &walk.fragments {
            let recipe = plan
                .fragment_recipes
                .as_slice()
                .get(fragment.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("shared span recipe is unknown".into())
                })?;
            for range in &recipe.ranges {
                source_edges.insert(range.source_edge);
                forward |= range.range.start.is_zero() && range.range.end.is_one();
                reverse |= range.range.start.is_one() && range.range.end.is_zero();
            }
        }
        assert_eq!(
            source_edges,
            BTreeSet::from([SourceEdgeId(0), SourceEdgeId(1)])
        );
        assert!(forward && reverse);
        Ok(())
    }

    #[test]
    fn alias_closed_ring_selects_a_geographic_face_for_both_producers() -> Result<()> {
        let edges = [
            "LINESTRING (170 0, 180 0)",
            "LINESTRING (-180 0, -170 0)",
            "LINESTRING (-170 0, -170 10)",
            "LINESTRING (-170 10, -180 10)",
            "LINESTRING (180 10, 170 10)",
            "LINESTRING (170 10, 170 0)",
        ]
        .into_iter()
        .map(crate::io::parse_wkt)
        .collect::<Result<Vec<_>>>()?;
        let topology = topology(&edges)?;

        let mut work = Unmetered;
        let polygonize = QEmissionPlan::build_polygonize(&topology, false, &mut work)?;
        let mut work = Unmetered;
        let build_area = QEmissionPlan::build_area(&topology, &mut work)?;

        assert_eq!(polygonize.producer, QProducer::Polygonize);
        assert_eq!(build_area.producer, QProducer::BuildArea);
        assert_eq!(selected_face_count(&polygonize), 1);
        assert_eq!(selected_face_count(&build_area), 1);
        assert!(matches!(
            &polygonize.polygons,
            PolygonPlan::Regions { rings, .. } if !rings.is_empty()
        ));
        assert!(matches!(
            &build_area.polygons,
            PolygonPlan::Regions { rings, .. } if !rings.is_empty()
        ));
        Ok(())
    }

    #[test]
    fn polygonize_keeps_minimal_nested_faces_while_build_area_fills_even_odd() -> Result<()> {
        let linework = crate::io::parse_wkt(
            "MULTILINESTRING (\
             (0 0, 10 0, 10 10, 0 10, 0 0),\
             (3 3, 7 3, 7 7, 3 7, 3 3))",
        )?;
        let topology = topology(&[linework])?;

        let mut work = Unmetered;
        let polygonize = QEmissionPlan::build_polygonize(&topology, false, &mut work)?;
        let mut work = Unmetered;
        let build_area = QEmissionPlan::build_area(&topology, &mut work)?;

        assert_eq!(selected_face_count(&polygonize), 2);
        assert_eq!(selected_face_count(&build_area), 1);
        Ok(())
    }

    #[test]
    fn build_area_fills_adjacent_faces_at_one_component_depth() -> Result<()> {
        let linework = crate::io::parse_wkt(
            "MULTILINESTRING (\
             (0 0, 4 0, 4 4, 0 4, 0 0),\
             (0 2, 4 2))",
        )?;
        let topology = topology(&[linework])?;
        let mut work = Unmetered;
        let plan = QEmissionPlan::build_area(&topology, &mut work)?;
        assert_eq!(selected_face_count(&plan), 2);
        Ok(())
    }

    #[test]
    fn build_area_keeps_a_bridge_connected_inner_ring_as_a_hole_with_an_interior_chord()
    -> Result<()> {
        let linework = crate::io::parse_wkt(
            "MULTILINESTRING (\
             (0 0, 10 0, 10 10, 0 10, 0 0),\
             (3 3, 7 3, 7 7, 3 7, 3 3),\
             (0 5, 3 5),\
             (3 5, 7 5))",
        )?;
        let topology = topology(&[linework])?;
        assert_eq!(
            topology
                .q
                .component_hosts
                .iter()
                .filter(|host| host.is_some())
                .count(),
            1
        );
        let mut work = Unmetered;
        let plan = QEmissionPlan::build_area(&topology, &mut work)?;
        assert_eq!(selected_face_count(&plan), 1);
        Ok(())
    }

    #[test]
    fn polygonize_diagnostics_keep_shared_span_rows_and_roles() -> Result<()> {
        let first = crate::io::parse_wkt("LINESTRING (0 0, 2 0)")?;
        let second = crate::io::parse_wkt("LINESTRING (2 0, 0 0)")?;
        let topology = topology(&[first, second])?;
        let mut work = Unmetered;
        let plan = QEmissionPlan::build_polygonize(&topology, true, &mut work)?;

        assert_eq!(plan.producer, QProducer::Polygonize);
        assert_eq!(plan.line_paths.len(), 2);
        let rows = plan
            .line_paths
            .iter()
            .map(|line| line.source_row)
            .collect::<Vec<_>>();
        assert_eq!(rows, [Some(SourceRowId(0)), Some(SourceRowId(1))]);
        assert!(plan.line_paths.iter().all(|line| {
            line.origin == QLineOrigin::Diagnostic(DiagnosticKind::InvalidRing) && line.close
        }));
        Ok(())
    }
}
