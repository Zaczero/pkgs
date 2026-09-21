//! Labelling the exact subdivision.
//!
//! Once the scaffold embeds, each face carries a region label propagated
//! across its bounding transitions; a component grafted onto another inherits
//! the label of the face that contains it.

use super::{
    AtomicEdge, BoundaryFaceId, ExactSign, ExpansionBudget, IndexedTopology,
    IndexedTopologyScratch, NodedAtoms, Ordering, OwnerTransition, Result, Site, SiteAdjacency,
    SiteId, Transition, VecDeque, XY, exact, nearest_site_walk, try_reserve_vec, voronoi_error,
};

pub(super) struct ExactVertex {
    pub(super) point: exact::ExactPoint,
    pub(super) fixed_xy: Option<XY>,
}

pub(super) fn uf_root(parents: &mut [usize], mut node: usize) -> usize {
    while parents[node] != node {
        parents[node] = parents[parents[node]];
        node = parents[node];
    }
    node
}

pub(super) fn build_exact_subdivision(
    atoms: NodedAtoms,
    budget: &mut ExpansionBudget,
) -> Result<ExactSubdivision> {
    let vertices = atoms.vertices.len();
    let edges = atoms.edges.len();
    let slots = edges * 2;
    budget.add(slots)?;
    budget.add(slots)?;
    let mut topology = IndexedTopology::default();
    let mut scratch = IndexedTopologyScratch::default();
    topology.starts.reserve(vertices + 1);
    topology.targets.reserve(slots);
    topology.owners.reserve(slots);
    topology.twins.reserve(slots);
    topology.component_of.reserve(vertices);
    topology.face_of.reserve(slots);
    topology.face_starts.reserve(slots + 1);
    topology.face_slots.reserve(slots);
    scratch.component_stack.reserve(vertices);
    scratch.successor.reserve(slots);
    scratch.edge_slots.reserve(edges);
    let mut edge_of_slot = Vec::with_capacity(slots);
    let mut cursors = Vec::with_capacity(vertices);
    let mut exact_vertices = Vec::with_capacity(vertices);
    topology.starts.resize(vertices + 1, 0);
    for edge in &atoms.edges {
        topology.starts[edge.endpoints[0].0 as usize + 1] += 1;
        topology.starts[edge.endpoints[1].0 as usize + 1] += 1;
    }
    for index in 1..topology.starts.len() {
        topology.starts[index] += topology.starts[index - 1];
    }
    cursors.extend_from_slice(&topology.starts[..vertices]);
    topology.targets.resize(slots, 0);
    edge_of_slot.resize(slots, 0);
    for (edge, atom) in atoms.edges.iter().enumerate() {
        let edge = u32::try_from(edge).map_err(|_| voronoi_error("too many exact edges"))?;
        let [start, end] = atom.endpoints;
        for (origin, target) in [(start, end), (end, start)] {
            let slot = cursors[origin.0 as usize] as usize;
            topology.targets[slot] = target.0;
            edge_of_slot[slot] = edge;
            cursors[origin.0 as usize] += 1;
        }
    }
    for vertex in 0..vertices {
        let row = topology.starts[vertex] as usize..topology.starts[vertex + 1] as usize;
        if row.len() < 2 {
            return Err(voronoi_error("exact Voronoi subdivision contains a dangle"));
        }
        let origin = &atoms.vertices[vertex].point;
        for index in row.start + 1..row.end {
            let mut cursor = index;
            while cursor > row.start
                && exact::angle_ccw_cmp(
                    origin,
                    &atoms.vertices[topology.targets[cursor] as usize].point,
                    &atoms.vertices[topology.targets[cursor - 1] as usize].point,
                )? == Ordering::Less
            {
                topology.targets.swap(cursor, cursor - 1);
                edge_of_slot.swap(cursor, cursor - 1);
                cursor -= 1;
            }
        }
        for slot in row.start + 1..row.end {
            exact::angle_ccw_cmp(
                origin,
                &atoms.vertices[topology.targets[slot - 1] as usize].point,
                &atoms.vertices[topology.targets[slot] as usize].point,
            )
            .map_err(|_| voronoi_error("exact angular tie"))?;
        }
    }
    exact_vertices.extend(atoms.vertices.into_iter().map(|vertex| ExactVertex {
        point: vertex.point,
        fixed_xy: vertex.fixed_xy,
    }));
    topology.finish(&edge_of_slot, &mut scratch);
    for slot in 0..slots {
        if topology.face_of[slot] == topology.face_of[topology.twins[slot] as usize] {
            return Err(voronoi_error("exact Voronoi subdivision contains a bridge"));
        }
    }
    Ok(ExactSubdivision {
        vertices: exact_vertices,
        edges: atoms.edges,
        topology,
        edge_of_slot,
        labels: Vec::new(),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RegionLabel {
    pub(super) frame_winding: i8,
    pub(super) clip_winding: i32,
    pub(super) owner: Option<SiteId>,
}

pub(super) struct ExactSubdivision {
    pub(super) vertices: Vec<ExactVertex>,
    pub(super) edges: Vec<AtomicEdge>,
    pub(super) topology: IndexedTopology,
    pub(super) edge_of_slot: Vec<u32>,
    pub(super) labels: Vec<RegionLabel>,
}

pub(super) fn slot_transition(subdivision: &ExactSubdivision, slot: usize) -> Transition {
    let atomic = &subdivision.edges[subdivision.edge_of_slot[slot] as usize];
    if subdivision.topology.owners[slot] == atomic.endpoints[0].0 {
        atomic.forward
    } else {
        atomic.forward.reversed()
    }
}

pub(super) fn selected_cell_label(label: RegionLabel) -> bool {
    label.frame_winding == 1 && label.clip_winding == 1
}

pub(super) fn walk_label(subdivision: &ExactSubdivision, face: BoundaryFaceId) -> RegionLabel {
    subdivision.labels[face.0 as usize]
}

pub(super) fn validate_exact_subdivision(subdivision: &ExactSubdivision) -> Result<()> {
    let topology = &subdivision.topology;
    let boundary_faces = topology.face_starts.len().saturating_sub(1);
    if subdivision.labels.len() != boundary_faces {
        return Err(voronoi_error("one exact boundary face has no label"));
    }
    for edge in &subdivision.edges {
        if edge.clip_sources.array_windows::<2>().any(|[a, b]| a >= b)
            || (edge.forward.clip_delta != 0 && edge.clip_sources.is_empty())
        {
            return Err(voronoi_error(
                "exact atomic edge has incoherent clip provenance",
            ));
        }
    }
    for slot in 0..topology.targets.len() {
        let twin = topology.twins[slot] as usize;
        let edge = subdivision.edge_of_slot[slot] as usize;
        let left = BoundaryFaceId(topology.face_of[slot]);
        let right = BoundaryFaceId(topology.face_of[twin]);
        let transition = slot_transition(subdivision, slot);
        if topology.twins[twin] != slot as u32
            || topology.owners[slot] != topology.targets[twin]
            || topology.targets[slot] != topology.owners[twin]
            || subdivision.edge_of_slot[twin] as usize != edge
            || topology.component_of[topology.owners[slot] as usize]
                != topology.component_of[topology.owners[twin] as usize]
            || topology.predecessor_slot(topology.successor_slot(slot as u32)) != slot as u32
            || crossed_label(walk_label(subdivision, left), transition)?
                != walk_label(subdivision, right)
            || crossed_label(walk_label(subdivision, right), transition.reversed())?
                != walk_label(subdivision, left)
        {
            return Err(voronoi_error(
                "exact indexed topology invariants or twin labels disagree",
            ));
        }
    }
    for label in &subdivision.labels {
        if !matches!(label.frame_winding, 0 | 1)
            || !matches!(label.clip_winding, 0 | 1)
            || (label.frame_winding == 1 && label.owner.is_none())
        {
            return Err(voronoi_error(
                "exact boundary walk has an incoherent checked label",
            ));
        }
    }
    Ok(())
}

pub(super) fn crossed_label(left: RegionLabel, transition: Transition) -> Result<RegionLabel> {
    let owner = match transition.owner {
        OwnerTransition::Preserve => left.owner,
        OwnerTransition::Boundary {
            left: expected,
            right,
        } => {
            if left.owner != expected {
                return Err(voronoi_error(
                    "Voronoi owner transition disagrees with propagated face label",
                ));
            }
            right
        },
    };
    Ok(RegionLabel {
        frame_winding: left.frame_winding - transition.frame_delta,
        clip_winding: left.clip_winding - transition.clip_delta,
        owner,
    })
}

pub(super) fn assign_walk_label(
    labels: &mut [Option<RegionLabel>],
    queue: &mut VecDeque<BoundaryFaceId>,
    face: BoundaryFaceId,
    label: RegionLabel,
) -> Result<()> {
    match labels[face.0 as usize] {
        None => {
            labels[face.0 as usize] = Some(label);
            queue.push_back(face);
            Ok(())
        },
        Some(existing) if existing == label => Ok(()),
        Some(_) => Err(voronoi_error("inconsistent exact region-label propagation")),
    }
}

pub(super) fn propagate_walk_labels(
    subdivision: &ExactSubdivision,
    labels: &mut [Option<RegionLabel>],
    queue: &mut VecDeque<BoundaryFaceId>,
) -> Result<()> {
    let topology = &subdivision.topology;
    while let Some(left) = queue.pop_front() {
        let label = labels[left.0 as usize].expect("queued walk is labelled");
        let walk = &topology.face_slots[topology.face_starts[left.0 as usize] as usize
            ..topology.face_starts[left.0 as usize + 1] as usize];
        for &slot in walk {
            let transition = slot_transition(subdivision, slot as usize);
            let right = BoundaryFaceId(topology.face_of[topology.twins[slot as usize] as usize]);
            let right_label = crossed_label(label, transition)?;
            if crossed_label(right_label, transition.reversed())? != label {
                return Err(voronoi_error("twin label equations are not inverses"));
            }
            assign_walk_label(labels, queue, right, right_label)?;
        }
    }
    Ok(())
}

pub(super) fn graft_components(
    subdivision: &mut ExactSubdivision,
    sites: &[Site],
    adjacency: &SiteAdjacency,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let topology = &subdivision.topology;
    let boundary_faces = topology.face_starts.len() - 1;
    let components = topology
        .component_of
        .iter()
        .copied()
        .max()
        .map_or(0, |id| id as usize + 1);
    try_reserve_vec(&mut subdivision.labels, boundary_faces)?;
    let mut exterior = Vec::new();
    try_reserve_vec(&mut exterior, components)?;
    exterior.resize(components, None);
    let mut points = Vec::new();
    for face in 0..boundary_faces {
        let walk = &topology.face_slots
            [topology.face_starts[face] as usize..topology.face_starts[face + 1] as usize];
        budget.add(walk.len())?;
        points.clear();
        try_reserve_vec(&mut points, walk.len())?;
        points.extend(walk.iter().map(|&slot| {
            subdivision.vertices[topology.owners[slot as usize] as usize]
                .point
                .clone()
        }));
        let sign = exact::cycle_orientation(&points)?;
        let component = topology.component_of[topology.owners[walk[0] as usize] as usize] as usize;
        if sign == ExactSign::Negative
            && exterior[component]
                .replace(BoundaryFaceId(face as u32))
                .is_some()
        {
            return Err(voronoi_error(
                "edge component has more than one negative boundary face",
            ));
        }
    }
    if exterior.iter().any(Option::is_none) {
        return Err(voronoi_error(
            "edge component has no negative boundary face",
        ));
    }
    let frame_edge = subdivision
        .edges
        .iter()
        .position(|edge| edge.frame_side.is_some())
        .ok_or_else(|| voronoi_error("private frame is absent"))?;
    let frame_component =
        topology.component_of[subdivision.edges[frame_edge].endpoints[0].0 as usize] as usize;
    let mut labels = vec![None; boundary_faces];
    let mut queue = VecDeque::with_capacity(boundary_faces);
    let outside_frame = RegionLabel {
        frame_winding: 0,
        clip_winding: 0,
        owner: None,
    };
    assign_walk_label(
        &mut labels,
        &mut queue,
        exterior[frame_component].expect("component exterior was checked"),
        outside_frame,
    )?;
    propagate_walk_labels(subdivision, &mut labels, &mut queue)?;
    for outside in exterior.iter().copied().map(Option::unwrap) {
        if labels[outside.0 as usize].is_some() {
            continue;
        }
        let slot = topology.face_slots[topology.face_starts[outside.0 as usize] as usize] as usize;
        let edge = &subdivision.edges[subdivision.edge_of_slot[slot] as usize];
        if edge.clip_sources.is_empty() || edge.frame_side.is_some() {
            return Err(voronoi_error("unvisited exact component has no clip seed"));
        }
        let transition = slot_transition(subdivision, slot);
        if transition.frame_delta != 0 || transition.clip_delta.unsigned_abs() != 1 {
            return Err(voronoi_error(
                "unvisited exact component has no simple clip seed",
            ));
        }
        let start = &subdivision.vertices[topology.owners[slot] as usize].point;
        let end = &subdivision.vertices[topology.targets[slot] as usize].point;
        let owner = nearest_site_walk(&start.midpoint(end)?, sites, adjacency, SiteId(0), budget)?;
        let label = RegionLabel {
            frame_winding: 1,
            clip_winding: i32::from(transition.clip_delta > 0),
            owner: Some(owner),
        };
        assign_walk_label(&mut labels, &mut queue, outside, label)?;
        propagate_walk_labels(subdivision, &mut labels, &mut queue)?;
    }
    subdivision.labels.clear();
    for label in labels {
        subdivision.labels.push(
            label.ok_or_else(|| {
                voronoi_error("exact subdivision left an unlabelled boundary face")
            })?,
        );
    }
    validate_exact_subdivision(subdivision)
}
