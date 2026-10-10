//! Labelling the exact subdivision.
//!
//! Once the scaffold embeds, each face carries a region label propagated
//! across its bounding transitions; a component grafted onto another inherits
//! the label of the face that contains it.

use super::{
    AtomicEdge, BoundaryFaceId, EdgeId, ExactSign, IndexedTopology, IndexedTopologyScratch,
    NodedAtoms, Ordering, OwnerTransition, Result, Site, SiteAdjacency, SiteId, Transition,
    VecDeque, VertexId, XY, exact, nearest_site_walk, voronoi_error,
};

pub(super) struct ExactVertex {
    pub(super) point: exact::ExactPoint,
    pub(super) fixed_xy: Option<XY>,
}

#[expect(
    clippy::indexing_slicing,
    reason = "union-find nodes and parent links are dense indices from the same initialized parent column"
)]
pub(super) fn uf_root(parents: &mut [usize], mut node: usize) -> usize {
    while parents[node] != node {
        parents[node] = parents[parents[node]];
        node = parents[node];
    }
    node
}

#[expect(
    clippy::indexing_slicing,
    reason = "both roots are returned by uf_root over this same admitted parent column"
)]
pub(super) fn uf_union(parents: &mut [usize], left: usize, right: usize) -> bool {
    let left = uf_root(parents, left);
    let right = uf_root(parents, right);
    if left == right {
        false
    } else {
        parents[right] = left;
        true
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "admit_counts bounds the vertex, slot, and terminal-offset widths; scatter IDs come from the admitted endpoint columns, and insertion-sort predecessors stay inside the admitted row"
)]
pub(super) fn build_exact_subdivision(atoms: NodedAtoms) -> Result<ExactSubdivision> {
    let vertices = atoms.vertices.len();
    let edges = atoms.edges.len();
    let slots = IndexedTopology::admit_counts(vertices, edges)?;
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
    scratch.first_slot.reserve(edges);
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
    let subdivision = ExactSubdivision {
        vertices: exact_vertices,
        edges: atoms.edges,
        topology,
        edge_of_slot,
        labels: Vec::new(),
    };
    for slot in 0..slots {
        let dart = subdivision.dart(slot as u32);
        if dart.face == subdivision.dart(dart.twin).face {
            return Err(voronoi_error("exact Voronoi subdivision contains a bridge"));
        }
    }
    Ok(subdivision)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RegionLabel {
    pub(super) frame_winding: i8,
    pub(super) clip_winding: i32,
    pub(super) owner: Option<SiteId>,
}

struct FaceLabels {
    values: Vec<Option<RegionLabel>>,
}

impl FaceLabels {
    fn new(count: usize) -> Self {
        Self {
            values: vec![None; count],
        }
    }

    fn slot(&self, face: BoundaryFaceId) -> Option<&RegionLabel> {
        self.values.get(face.0 as usize).and_then(Option::as_ref)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "BoundaryFaceId is minted from the completed face CSR and this column has exactly one label slot per face"
    )]
    fn slot_mut(&mut self, face: BoundaryFaceId) -> &mut Option<RegionLabel> {
        &mut self.values[face.0 as usize]
    }

    fn required(&self, face: BoundaryFaceId) -> RegionLabel {
        self.slot(face)
            .copied()
            .expect("a queued boundary face has an assigned label")
    }

    fn into_values(self) -> Vec<Option<RegionLabel>> {
        self.values
    }
}

pub(super) struct ExactSubdivision {
    pub(super) vertices: Vec<ExactVertex>,
    pub(super) edges: Vec<AtomicEdge>,
    pub(super) topology: IndexedTopology,
    pub(super) edge_of_slot: Vec<u32>,
    pub(super) labels: Vec<RegionLabel>,
}

#[derive(Clone, Copy)]
pub(super) struct DartSlot {
    pub(super) origin: VertexId,
    pub(super) target: VertexId,
    pub(super) twin: u32,
    pub(super) face: BoundaryFaceId,
    pub(super) edge: EdgeId,
}

impl ExactSubdivision {
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "vertices are stored in exact lexicographic order, and binary search keeps its midpoint and lower-bound advance inside this admitted column"
    )]
    pub(super) fn vertex_at(&self, point: &exact::ExactPoint) -> Result<Option<VertexId>> {
        let (mut low, mut high) = (0, self.vertices.len());
        while low < high {
            let middle = low + (high - low) / 2;
            match self.vertices[middle].point.compare_lex(point)? {
                Ordering::Less => low = middle + 1,
                Ordering::Greater => high = middle,
                Ordering::Equal => return Ok(Some(VertexId(middle as u32))),
            }
        }
        Ok(None)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "VertexId values are minted from the lexicographically interned vertex column after IndexedTopology::admit_counts"
    )]
    pub(super) fn vertex(&self, id: VertexId) -> &ExactVertex {
        &self.vertices[id.0 as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "EdgeId values are minted from the admitted AtomicEdge column"
    )]
    pub(super) fn edge(&self, id: EdgeId) -> &AtomicEdge {
        &self.edges[id.0 as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "BoundaryFaceId values are assigned from the completed face_starts/labels rows"
    )]
    pub(super) fn label(&self, face: BoundaryFaceId) -> RegionLabel {
        self.labels[face.0 as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face_starts has one terminal offset, and each BoundaryFaceId comes from its completed CSR rows"
    )]
    pub(super) fn face_slots(&self, face: BoundaryFaceId) -> &[u32] {
        let face = face.0 as usize;
        let start = self.topology.face_starts[face] as usize;
        let end = self.topology.face_starts[face + 1] as usize;
        &self.topology.face_slots[start..end]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face_rows walks adjacent admitted offsets; each half-open slot row is published by IndexedTopology::finish"
    )]
    pub(super) fn face_rows(&self) -> impl Iterator<Item = (BoundaryFaceId, &[u32])> + '_ {
        self.topology
            .face_starts
            .array_windows::<2>()
            .enumerate()
            .map(move |(face, &[start, end])| {
                (
                    BoundaryFaceId(face as u32),
                    &self.topology.face_slots[start as usize..end as usize],
                )
            })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "slot IDs and edge_of_slot/topology columns are parallel outputs of the same admitted IndexedTopology::finish"
    )]
    pub(super) fn dart(&self, slot: u32) -> DartSlot {
        let slot = slot as usize;
        DartSlot {
            origin: VertexId(self.topology.owners[slot]),
            target: VertexId(self.topology.targets[slot]),
            twin: self.topology.twins[slot],
            face: BoundaryFaceId(self.topology.face_of[slot]),
            edge: EdgeId(self.edge_of_slot[slot]),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "component IDs are densely assigned by IndexedTopology::finish from the admitted vertex column"
    )]
    pub(super) fn component(&self, vertex: VertexId) -> usize {
        self.topology.component_of[vertex.0 as usize] as usize
    }
}

pub(super) fn slot_transition(subdivision: &ExactSubdivision, slot: u32) -> Transition {
    let dart = subdivision.dart(slot);
    let atomic = subdivision.edge(dart.edge);
    let [start, _] = atomic.endpoints;
    if dart.origin == start {
        atomic.forward
    } else {
        atomic.forward.reversed()
    }
}

pub(super) fn selected_cell_label(label: RegionLabel) -> bool {
    label.frame_winding == 1 && label.clip_winding == 1
}

pub(super) fn walk_label(subdivision: &ExactSubdivision, face: BoundaryFaceId) -> RegionLabel {
    subdivision.label(face)
}

pub(super) fn validate_exact_subdivision(subdivision: &ExactSubdivision) -> Result<()> {
    let topology = &subdivision.topology;
    let boundary_faces = topology.face_starts.array_windows::<2>().count();
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
    for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
        let slot = slot as u32;
        let dart = subdivision.dart(slot);
        let twin = subdivision.dart(dart.twin);
        let transition = slot_transition(subdivision, slot);
        if twin.twin != slot
            || dart.origin != twin.target
            || dart.target != twin.origin
            || dart.edge != EdgeId(edge)
            || subdivision.component(dart.origin) != subdivision.component(twin.origin)
            || topology.predecessor_slot(topology.successor_slot(slot)) != slot
            || crossed_label(walk_label(subdivision, dart.face), transition)?
                != walk_label(subdivision, twin.face)
            || crossed_label(walk_label(subdivision, twin.face), transition.reversed())?
                != walk_label(subdivision, dart.face)
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

#[expect(
    clippy::arithmetic_side_effects,
    reason = "validated transitions carry frame/clip deltas within the exact label domain, so the propagated 0-or-1 windings remain representable"
)]
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

fn assign_walk_label(
    labels: &mut FaceLabels,
    queue: &mut VecDeque<BoundaryFaceId>,
    face: BoundaryFaceId,
    label: RegionLabel,
) -> Result<()> {
    match labels.slot(face).copied() {
        None => {
            *labels.slot_mut(face) = Some(label);
            queue.push_back(face);
            Ok(())
        },
        Some(existing) if existing == label => Ok(()),
        Some(_) => Err(voronoi_error("inconsistent exact region-label propagation")),
    }
}

fn propagate_walk_labels(
    subdivision: &ExactSubdivision,
    labels: &mut FaceLabels,
    queue: &mut VecDeque<BoundaryFaceId>,
) -> Result<()> {
    while let Some(left) = queue.pop_front() {
        let label = labels.required(left);
        let walk = subdivision.face_slots(left);
        for &slot in walk {
            let transition = slot_transition(subdivision, slot);
            let right = subdivision.dart(subdivision.dart(slot).twin).face;
            let right_label = crossed_label(label, transition)?;
            if crossed_label(right_label, transition.reversed())? != label {
                return Err(voronoi_error("twin label equations are not inverses"));
            }
            assign_walk_label(labels, queue, right, right_label)?;
        }
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "component IDs are densely assigned within IndexedTopology::finish's admitted vertex count and exterior has one slot per component"
)]
pub(super) fn graft_components(
    subdivision: &mut ExactSubdivision,
    sites: &[Site],
    adjacency: &SiteAdjacency,
) -> Result<()> {
    let topology = &subdivision.topology;
    let boundary_faces = topology.face_starts.array_windows::<2>().count();
    let components = topology
        .component_of
        .iter()
        .copied()
        .max()
        .map_or(0, |id| id as usize + 1);
    subdivision.labels.reserve(boundary_faces);
    let mut exterior = Vec::with_capacity(components);
    exterior.resize(components, None);
    for (face, walk) in subdivision.face_rows() {
        let sign = exact::cycle_orientation(walk.iter().map(|&slot| {
            let dart = subdivision.dart(slot);
            &subdivision.vertex(dart.origin).point
        }))?;
        let first_slot = *walk
            .first()
            .expect("face rows produced by IndexedTopology contain at least one slot");
        let component = subdivision.component(subdivision.dart(first_slot).origin);
        if sign == ExactSign::Negative && exterior[component].replace(face).is_some() {
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
        .find(|edge| edge.frame_side.is_some())
        .ok_or_else(|| voronoi_error("private frame is absent"))?;
    let [frame_vertex, _] = frame_edge.endpoints;
    let frame_component = subdivision.component(frame_vertex);
    let mut labels = FaceLabels::new(boundary_faces);
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
        if labels.slot(outside).is_some() {
            continue;
        }
        let slot = *subdivision
            .face_slots(outside)
            .first()
            .expect("an exterior face came from a nonempty boundary walk");
        let dart = subdivision.dart(slot);
        let edge = subdivision.edge(dart.edge);
        if edge.clip_sources.is_empty() || edge.frame_side.is_some() {
            return Err(voronoi_error("unvisited exact component has no clip seed"));
        }
        let transition = slot_transition(subdivision, slot);
        if transition.frame_delta != 0 || transition.clip_delta.unsigned_abs() != 1 {
            return Err(voronoi_error(
                "unvisited exact component has no simple clip seed",
            ));
        }
        let start = &subdivision.vertex(dart.origin).point;
        let end = &subdivision.vertex(dart.target).point;
        let owner = nearest_site_walk(&start.midpoint(end)?, sites, adjacency, SiteId(0))?;
        let label = RegionLabel {
            frame_winding: 1,
            clip_winding: i32::from(transition.clip_delta > 0),
            owner: Some(owner),
        };
        assign_walk_label(&mut labels, &mut queue, outside, label)?;
        propagate_walk_labels(subdivision, &mut labels, &mut queue)?;
    }
    subdivision.labels.clear();
    for label in labels.into_values() {
        subdivision.labels.push(
            label.ok_or_else(|| {
                voronoi_error("exact subdivision left an unlabelled boundary face")
            })?,
        );
    }
    validate_exact_subdivision(subdivision)
}
