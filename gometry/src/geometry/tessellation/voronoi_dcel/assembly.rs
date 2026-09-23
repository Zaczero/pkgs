//! Assembling cells and edges from the labelled subdivision.
//!
//! Selected faces become rings, rings become cell parts, and the separator
//! incidence is walked once to emit each shared edge exactly one time.

use super::{
    BoundaryFaceId, BoxIndex, EdgeId, ExactSign, ExactSubdivision, ExpansionBudget, Ordering,
    Result, SiteId, VertexId, budgeted_push, exact, rounded_box, selected_cell_label, try_sort_by,
    validate_exact_cycles, voronoi_error, walk_label,
};

pub(super) struct ExactSimpleRing {
    pub(super) face: BoundaryFaceId,
    pub(super) vertices: Vec<VertexId>,
    pub(super) edges: Vec<EdgeId>,
    pub(super) orientation: ExactSign,
}

pub(super) struct ExactCellPart {
    pub(super) owner: SiteId,
    pub(super) face: BoundaryFaceId,
    pub(super) shell: ExactSimpleRing,
    pub(super) holes: Vec<ExactSimpleRing>,
}

pub(super) struct ExactEdgeChain {
    pub(super) separator: [SiteId; 2],
    pub(super) vertices: Vec<VertexId>,
    pub(super) edges: Vec<EdgeId>,
}

pub(super) fn canonicalize_ring(vertices: &mut [VertexId], edges: &mut [EdgeId]) {
    let open = vertices.len() - 1;
    let least = (0..open)
        .min_by(|&left, &right| {
            (0..open)
                .map(|offset| {
                    vertices[(left + offset) % open].cmp(&vertices[(right + offset) % open])
                })
                .find(|order| order.is_ne())
                .unwrap_or(Ordering::Equal)
        })
        .unwrap();
    vertices[..open].rotate_left(least);
    vertices[open] = vertices[0];
    edges.rotate_left(least);
}

/// The simple ring through `vertices` (closed) and `edges`, canonically
/// rotated, with its exact orientation.
pub(super) fn simple_ring(
    subdivision: &ExactSubdivision,
    face: BoundaryFaceId,
    mut vertices: Vec<VertexId>,
    mut edges: Vec<EdgeId>,
) -> Result<ExactSimpleRing> {
    let orientation = exact::cycle_orientation(
        vertices[..vertices.len() - 1]
            .iter()
            .map(|vertex| &subdivision.vertices[vertex.0 as usize].point),
    )?;
    canonicalize_ring(&mut vertices, &mut edges);
    Ok(ExactSimpleRing {
        face,
        vertices,
        edges,
        orientation,
    })
}

pub(super) fn split_boundary_face(
    subdivision: &ExactSubdivision,
    boundary_face: BoundaryFaceId,
    result: &mut Vec<ExactSimpleRing>,
) -> Result<()> {
    let topology = &subdivision.topology;
    let walk = &topology.face_slots[topology.face_starts[boundary_face.0 as usize] as usize
        ..topology.face_starts[boundary_face.0 as usize + 1] as usize];
    let owner = |slot: u32| VertexId(topology.owners[slot as usize]);
    let edge = |slot: u32| EdgeId(subdivision.edge_of_slot[slot as usize]);
    let mut distinct = Vec::with_capacity(walk.len());
    distinct.extend(walk.iter().map(|&slot| owner(slot)));
    distinct.sort_unstable();
    if distinct.array_windows::<2>().all(|[a, b]| a != b) {
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        vertices.reserve(walk.len() + 1);
        edges.reserve(walk.len());
        vertices.extend(walk.iter().map(|&slot| owner(slot)));
        vertices.push(vertices[0]);
        edges.extend(walk.iter().map(|&slot| edge(slot)));
        result.push(simple_ring(subdivision, boundary_face, vertices, edges)?);
        return Ok(());
    }
    let mut path = Vec::new();
    let mut path_edges = Vec::new();
    path.reserve(walk.len());
    path_edges.reserve(walk.len());
    let mut positions = Vec::with_capacity(subdivision.vertices.len());
    positions.resize(subdivision.vertices.len(), None);
    for step in 0..=walk.len() {
        let vertex = owner(walk[step % walk.len()]);
        let incoming = (step > 0).then(|| edge(walk[(step - 1) % walk.len()]));
        if let Some(position) = positions[vertex.0 as usize] {
            let mut vertices = Vec::new();
            let mut edges = Vec::new();
            vertices.reserve(path.len() - position + 1);
            edges.reserve(path_edges.len() - position + 1);
            vertices.extend_from_slice(&path[position..]);
            vertices.push(vertex);
            edges.extend_from_slice(&path_edges[position..]);
            edges.push(incoming.expect("repeated boundary vertex has an incoming edge"));
            if vertices.len() < 4 {
                return Err(voronoi_error("pinch split produced a short exact ring"));
            }
            for removed in &path[position + 1..] {
                positions[removed.0 as usize] = None;
            }
            path.truncate(position + 1);
            path_edges.truncate(position);
            result.push(simple_ring(subdivision, boundary_face, vertices, edges)?);
        } else {
            positions[vertex.0 as usize] = Some(path.len());
            path.push(vertex);
            if let Some(edge) = incoming {
                path_edges.push(edge);
            }
        }
    }
    if path.len() != 1 || !path_edges.is_empty() {
        return Err(voronoi_error(
            "pinch split left a residual exact boundary path",
        ));
    }
    Ok(())
}

pub(super) fn assemble_owner_parts(
    subdivision: &ExactSubdivision,
    owner: SiteId,
    walks: impl IntoIterator<Item = BoundaryFaceId>,
    budget: &mut ExpansionBudget,
    parts: &mut Vec<ExactCellPart>,
) -> Result<()> {
    let mut rings = Vec::new();
    for orbit in walks {
        split_boundary_face(subdivision, orbit, &mut rings)?;
    }
    if rings.is_empty() {
        return Ok(());
    }
    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    for (index, ring) in rings.iter().enumerate() {
        match ring.orientation {
            ExactSign::Positive => positives.push(index),
            ExactSign::Negative => negatives.push(index),
            ExactSign::Zero => {},
        }
    }
    let mut ring_points = Vec::with_capacity(rings.len());
    for ring in &rings {
        let count = ring.vertices.len().saturating_sub(1);
        budget.add(count)?;
        let mut points = Vec::with_capacity(count);
        points.extend(
            ring.vertices[..count]
                .iter()
                .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone()),
        );
        ring_points.push(points);
    }
    let mut ring_boxes = Vec::with_capacity(rings.len());
    for points in &ring_points {
        ring_boxes.push(rounded_box(points)?);
    }
    let ring_index = BoxIndex::build(&ring_boxes)?;
    // A ring nests in another only if its first vertex lies in that ring's box.
    let first_vertex = |ring: usize| ring_points[ring][0].finite_rounding();
    // Whether `points` lie in the closed `cycle` with one strictly inside.
    let nested = |points: &[exact::ExactPoint], cycle: &[exact::ExactPoint]| -> Result<bool> {
        let mut inside = false;
        for point in points {
            match exact::point_in_cycle(cycle, point)? {
                exact::PointInCycle::Outside => return Ok(false),
                exact::PointInCycle::Inside => inside = true,
                exact::PointInCycle::Boundary => {},
            }
        }
        Ok(inside)
    };
    for &positive in &positives {
        ring_index.for_each_containing(first_vertex(positive)?, |other| {
            if other == positive {
                return Ok(());
            }
            budget.add(
                ring_points[positive]
                    .len()
                    .saturating_mul(ring_points[other].len()),
            )?;
            if nested(&ring_points[positive], &ring_points[other])? {
                return Err(voronoi_error(
                    "exact same-face ring forest has depth greater than one",
                ));
            }
            Ok(())
        })?;
    }
    let mut holes_by_shell = Vec::with_capacity(positives.len());
    holes_by_shell.resize_with(positives.len(), Vec::new);
    // The slot of each positive ring among `positives`.
    let mut shell_slot = Vec::with_capacity(rings.len());
    shell_slot.resize(rings.len(), usize::MAX);
    for (slot, &positive) in positives.iter().enumerate() {
        shell_slot[positive] = slot;
    }
    for &negative in &negatives {
        let mut candidates = Vec::new();
        ring_index.for_each_containing(first_vertex(negative)?, |other| {
            if shell_slot[other] == usize::MAX {
                return Ok(());
            }
            budget.add(
                ring_points[negative]
                    .len()
                    .saturating_mul(ring_points[other].len()),
            )?;
            if nested(&ring_points[negative], &ring_points[other])? {
                candidates.push(shell_slot[other]);
            }
            Ok(())
        })?;
        candidates.sort_unstable();
        if candidates.is_empty() {
            return Err(voronoi_error(
                "negative exact face ring has no same-face shell",
            ));
        }
        budget.add(
            candidates
                .iter()
                .map(|slot| ring_points[positives[*slot]].len())
                .sum::<usize>()
                .saturating_mul(2),
        )?;
        let mut candidates = candidates.into_iter();
        let mut parent = candidates.next().expect("nonempty ring candidates");
        for candidate in candidates {
            let parent_point =
                &subdivision.vertices[rings[positives[parent]].vertices[0].0 as usize].point;
            if exact::point_in_cycle(&ring_points[positives[candidate]], parent_point)?
                == exact::PointInCycle::Inside
            {
                continue;
            }
            let candidate_point =
                &subdivision.vertices[rings[positives[candidate]].vertices[0].0 as usize].point;
            if exact::point_in_cycle(&ring_points[positives[parent]], candidate_point)?
                != exact::PointInCycle::Inside
            {
                return Err(voronoi_error("exact face ring forest is non-laminar"));
            }
            parent = candidate;
        }
        holes_by_shell[parent].push(negative);
    }
    let mut taken = Vec::with_capacity(rings.len());
    taken.extend(rings.into_iter().map(Some));
    let mut rings = taken;
    for (slot, positive) in positives.into_iter().enumerate() {
        let shell = rings[positive].take().unwrap();
        let mut holes = Vec::with_capacity(holes_by_shell[slot].len());
        holes.extend(
            holes_by_shell[slot]
                .iter()
                .map(|&hole| rings[hole].take().unwrap()),
        );
        if walk_label(subdivision, shell.face).owner != Some(owner)
            || holes
                .iter()
                .any(|hole| walk_label(subdivision, hole.face).owner != Some(owner))
        {
            return Err(voronoi_error("exact ring escaped its owner group"));
        }
        let mut edge_ids: Vec<EdgeId> = Vec::with_capacity(
            shell.edges.len() + holes.iter().map(|hole| hole.edges.len()).sum::<usize>(),
        );
        edge_ids.extend(
            shell
                .edges
                .iter()
                .chain(holes.iter().flat_map(|hole| hole.edges.iter())),
        );
        edge_ids.sort_unstable();
        if edge_ids.array_windows::<2>().any(|[a, b]| a == b) {
            return Err(voronoi_error(
                "exact face shell and holes share an atomic edge",
            ));
        }
        let mut exact_rings = Vec::with_capacity(1 + holes.len());
        for ring in std::iter::once(&shell).chain(holes.iter()) {
            let mut points = Vec::with_capacity(ring.vertices.len() - 1);
            points.extend(
                ring.vertices[..ring.vertices.len() - 1]
                    .iter()
                    .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone()),
            );
            exact_rings.push(points);
        }
        let mut exact_ring_refs = Vec::with_capacity(exact_rings.len());
        exact_ring_refs.extend(exact_rings.iter().map(Vec::as_slice));
        validate_exact_cycles(&exact_ring_refs, 0, budget)?;
        budgeted_push(
            parts,
            ExactCellPart {
                owner,
                face: shell.face,
                shell,
                holes,
            },
            budget,
        )?;
    }
    Ok(())
}

pub(super) fn assemble_cells(
    subdivision: &ExactSubdivision,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ExactCellPart>> {
    let mut selected = Vec::with_capacity(subdivision.labels.len());
    for (index, label) in subdivision.labels.iter().enumerate() {
        if !selected_cell_label(*label) {
            continue;
        }
        let owner = label
            .owner
            .ok_or_else(|| voronoi_error("selected exact walk has no owner"))?;
        selected.push((owner, BoundaryFaceId(index as u32)));
    }
    selected.sort_unstable();
    let mut parts = Vec::new();
    let mut start = 0;
    while start < selected.len() {
        let owner = selected[start].0;
        let mut end = start + 1;
        while end < selected.len() && selected[end].0 == owner {
            end += 1;
        }
        assemble_owner_parts(
            subdivision,
            owner,
            selected[start..end].iter().map(|(_, face)| *face),
            budget,
            &mut parts,
        )?;
        start = end;
    }
    try_sort_by(&mut parts, |left, right| {
        Ok(left
            .owner
            .cmp(&right.owner)
            .then_with(|| left.shell.vertices.cmp(&right.shell.vertices)))
    })?;
    Ok(parts)
}

/// One separator group's atoms at each vertex, as `(vertex, slot, other end)`
/// sorted by vertex and then slot.
pub(super) type SeparatorIncidence = [(VertexId, usize, VertexId)];

pub(super) fn incident_at(incidence: &SeparatorIncidence, vertex: VertexId) -> &SeparatorIncidence {
    let start = incidence.partition_point(|&(known, ..)| known < vertex);
    let end = incidence.partition_point(|&(known, ..)| known <= vertex);
    &incidence[start..end]
}

pub(super) fn walk_separator_from(
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    incidence: &SeparatorIncidence,
    used: &mut [bool],
    start: VertexId,
    budget: &mut ExpansionBudget,
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut current = start;
    let mut vertices = Vec::new();
    let mut chain_edges = Vec::new();
    vertices.push(current);
    while let Some(&(_, slot, next)) = incident_at(incidence, current)
        .iter()
        .find(|&&(_, slot, _)| !used[slot])
    {
        used[slot] = true;
        chain_edges.push(group[slot].1);
        current = next;
        vertices.push(current);
    }
    if vertices.len() < 2 {
        return Err(voronoi_error("empty Voronoi edge chain"));
    }
    if vertices.first() == vertices.last() {
        return Err(voronoi_error("Voronoi separator forms a closed cycle"));
    }
    if vertices.iter().rev().lt(vertices.iter()) {
        vertices.reverse();
        chain_edges.reverse();
    }
    budget.add(vertices.len())?;
    result.push(ExactEdgeChain {
        separator,
        vertices,
        edges: chain_edges,
    });
    Ok(())
}

pub(super) fn assemble_separator_group(
    subdivision: &ExactSubdivision,
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    budget: &mut ExpansionBudget,
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut incidence = Vec::with_capacity(group.len().saturating_mul(2));
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        let [left, right] = subdivision.edges[edge_id.0 as usize].endpoints;
        incidence.extend([(left, slot, right), (right, slot, left)]);
    }
    incidence.sort_unstable();
    if incidence
        .array_windows::<3>()
        .any(|[first, _, third]| first.0 == third.0)
    {
        return Err(voronoi_error("Voronoi separator branches"));
    }
    let mut used = Vec::with_capacity(group.len());
    used.resize(group.len(), false);
    let end_of_path = |vertex: VertexId| incident_at(&incidence, vertex).len() == 1;
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let [left, right] = subdivision.edges[edge_id.0 as usize].endpoints;
        let start = if end_of_path(left) {
            left
        } else if end_of_path(right) {
            right
        } else {
            continue;
        };
        walk_separator_from(
            separator, group, &incidence, &mut used, start, budget, result,
        )?;
    }
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let start = subdivision.edges[edge_id.0 as usize].endpoints[0];
        walk_separator_from(
            separator, group, &incidence, &mut used, start, budget, result,
        )?;
    }
    Ok(())
}

pub(super) fn assemble_edges(
    subdivision: &ExactSubdivision,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ExactEdgeChain>> {
    let mut forward_slot = Vec::with_capacity(subdivision.edges.len());
    forward_slot.resize(subdivision.edges.len(), u32::MAX);
    for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
        let atomic = &subdivision.edges[edge as usize];
        if subdivision.topology.owners[slot] == atomic.endpoints[0].0 {
            forward_slot[edge as usize] = slot as u32;
        }
    }
    let mut selected = Vec::new();
    for (index, edge) in subdivision.edges.iter().enumerate() {
        let Some(separator) = edge.separator else {
            continue;
        };
        let slot = forward_slot[index] as usize;
        debug_assert_ne!(slot, u32::MAX as usize);
        let left = walk_label(
            subdivision,
            BoundaryFaceId(subdivision.topology.face_of[slot]),
        );
        let twin = subdivision.topology.twins[slot] as usize;
        let right = walk_label(
            subdivision,
            BoundaryFaceId(subdivision.topology.face_of[twin]),
        );
        if selected_cell_label(left) || selected_cell_label(right) {
            selected.push((separator, EdgeId(index as u32)));
        }
    }
    selected.sort_unstable();
    let mut result = Vec::new();
    let mut start = 0;
    while start < selected.len() {
        let separator = selected[start].0;
        let mut end = start + 1;
        while end < selected.len() && selected[end].0 == separator {
            end += 1;
        }
        assemble_separator_group(
            subdivision,
            separator,
            &selected[start..end],
            budget,
            &mut result,
        )?;
        start = end;
    }
    try_sort_by(&mut result, |left, right| {
        Ok(left
            .separator
            .cmp(&right.separator)
            .then_with(|| left.vertices.cmp(&right.vertices)))
    })?;
    Ok(result)
}
