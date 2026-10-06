use super::walk::arrangement_faces;
use crate::error::{ResourceError, Result};
use crate::geometry::arrangement::{
    Arrangement, ArrangementSpares, TypedSpares, WindingWeight, arrangement_spares, build_csr,
    dedup_vertices_and_edges, order_single_loop_rows, positional_loop_cuts, positional_loop_pieces,
    sort_rows_counterclockwise, typed_spares,
};
use crate::geometry::{HashMap, PointKey, Segment, XY, arrangement};

impl<W: WindingWeight> Arrangement<W> {
    /// Build the test oracle from positional chart rings with per-ring weights.
    pub(crate) fn from_loops(
        segments: &[Segment],
        loop_ranges: &[(u32, u32)],
        weight_of_loop: impl Fn(usize) -> W,
    ) -> crate::error::Result<Option<Self>> {
        let n = segments.len();
        let Some((cuts, group_count)) = positional_loop_cuts(segments, loop_ranges)? else {
            return Ok(None);
        };
        let mut spares = arrangement_spares();
        let mut typed = typed_spares::<W>();
        let mut ids = spares.take_ids()?;
        let vertex_count = n
            .checked_add(group_count as usize)
            .ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(vertex_count).map_err(|_| ResourceError::SizeOverflow)?;
        let mut points = std::mem::take(&mut spares.points);
        points.clear();
        points.reserve(vertex_count);
        let (is_cut, pieces, anchor_ids) =
            positional_loop_pieces(segments, loop_ranges, &cuts, group_count, &mut points)?;
        debug_assert_eq!(points.len(), vertex_count);
        let mut starts = std::mem::take(&mut spares.topology.starts);
        starts.clear();
        starts.reserve(
            vertex_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        starts.push(0);
        let mut total = 0_u32;
        for &cut in &is_cut {
            total = total
                .checked_add(if cut { 4 } else { 2 })
                .ok_or(ResourceError::SizeOverflow)?;
            starts.push(total);
        }
        let half_edge_count = total as usize;
        debug_assert_eq!(
            half_edge_count,
            pieces
                .len()
                .checked_mul(2)
                .expect("piece count fits the checked half-edge total"),
        );
        let mut targets = std::mem::take(&mut spares.topology.targets);
        targets.clear();
        targets.reserve(half_edge_count);
        targets.resize(half_edge_count, 0);
        let mut edge_of_slot = std::mem::take(&mut spares.edge_of_slot);
        edge_of_slot.clear();
        edge_of_slot.reserve(half_edge_count);
        edge_of_slot.resize(half_edge_count, 0);
        let mut multiplicities = std::mem::take(&mut typed.multiplicities);
        multiplicities.clear();
        multiplicities.reserve(half_edge_count);
        multiplicities.resize(half_edge_count, W::UNSET);
        let mut filled = std::mem::take(&mut spares.filled);
        filled.clear();
        filled.reserve(vertex_count);
        filled.resize(vertex_count, 0);
        for (edge, &(from, to, loop_index)) in pieces.iter().enumerate() {
            let weight = weight_of_loop(loop_index as usize);
            let from_index = from as usize;
            let slot = starts[from_index].wrapping_add(filled[from_index]) as usize;
            filled[from_index] = filled[from_index].wrapping_add(1);
            targets[slot] = to;
            multiplicities[slot] = weight;
            edge_of_slot[slot] = edge as u32;
            let to_index = to as usize;
            let twin = starts[to_index].wrapping_add(filled[to_index]) as usize;
            filled[to_index] = filled[to_index].wrapping_add(1);
            targets[twin] = from;
            multiplicities[twin] = weight.neg();
            edge_of_slot[twin] = edge as u32;
        }
        if !order_single_loop_rows(
            &points,
            &starts,
            &mut targets,
            &mut multiplicities,
            &mut edge_of_slot,
        ) {
            spares.points = points;
            spares.topology.starts = starts;
            spares.topology.targets = targets;
            spares.edge_of_slot = edge_of_slot;
            spares.ids = Some(ids);
            spares.filled = filled;
            typed.multiplicities = multiplicities;
            return Ok(None);
        }
        // Only the loop ANCHORS are ever looked up (component probes map
        // anchors -> components); inserting all 40k vertices for K=2
        // lookups was pure waste. Duplicate keys already bailed.
        ids.reserve(anchor_ids.len());
        for &anchor in &anchor_ids {
            ids.insert(PointKey::new(points[anchor as usize]), anchor);
        }
        spares.filled = filled;
        spares.points = points;
        spares.topology.starts = starts;
        spares.topology.targets = targets;
        spares.edge_of_slot = edge_of_slot;
        typed.multiplicities = multiplicities;
        Ok(Some(Self::finish_topology(ids, spares, typed)?))
    }

    /// Build from noded directed segments, `weight_of(index)` giving each
    /// source segment's winding weight.
    pub(crate) fn weighted(
        segments: impl ExactSizeIterator<Item = Segment>,
        weight_of: impl Fn(usize) -> W,
    ) -> Result<Self> {
        Self::build(segments, weight_of)
    }

    pub(crate) fn build(
        segments: impl ExactSizeIterator<Item = Segment>,
        weight_of: impl Fn(usize) -> W,
    ) -> Result<Self> {
        let mut spares = arrangement_spares();
        let mut typed = typed_spares::<W>();
        let (ids, points, edges) = dedup_vertices_and_edges(
            segments,
            weight_of,
            spares.take_ids()?,
            std::mem::take(&mut spares.points),
            std::mem::take(&mut typed.edges),
        )?;
        let (starts, mut targets, mut multiplicities, mut edge_of_slot) = build_csr(
            points.len(),
            &edges,
            std::mem::take(&mut spares.topology.starts),
            std::mem::take(&mut spares.topology.targets),
            std::mem::take(&mut typed.multiplicities),
            std::mem::take(&mut spares.edge_of_slot),
        )?;
        typed.edges = edges;
        typed.edges.clear();
        sort_rows_counterclockwise(
            &points,
            &starts,
            &mut targets,
            &mut multiplicities,
            &mut edge_of_slot,
        );
        spares.points = points;
        spares.topology.starts = starts;
        spares.topology.targets = targets;
        spares.edge_of_slot = edge_of_slot;
        typed.multiplicities = multiplicities;
        Self::finish_topology(ids, spares, typed)
    }

    /// Post-CSR assembly shared by every constructor: connected components,
    /// the global face walk, and the face CSR. The topology columns are
    /// already final — everything here is derived.
    pub(crate) fn finish_topology(
        ids: HashMap<PointKey, u32>,
        mut core: crate::scratch_pool::ScratchLease<arrangement::ArrangementSpares>,
        typed: crate::scratch_pool::ScratchLease<TypedSpares<W>>,
    ) -> Result<Self> {
        let component_count = {
            let ArrangementSpares {
                topology,
                topology_scratch,
                edge_of_slot,
                ..
            } = &mut *core;
            let vertices = topology.starts.len().saturating_sub(1);
            let slots = topology.targets.len();
            topology.owners.reserve(slots);
            topology.twins.reserve(slots);
            topology.component_of.reserve(vertices);
            topology.face_of.reserve(slots);
            topology
                .face_starts
                .reserve(slots.checked_add(1).ok_or(ResourceError::SizeOverflow)?);
            topology.face_slots.reserve(slots);
            topology_scratch.component_stack.reserve(vertices);
            topology_scratch.successor.reserve(slots);
            topology_scratch.first_slot.reserve(slots / 2);
            topology.finish(edge_of_slot, topology_scratch)
        };
        core.edge_of_slot.clear();
        core.ids = Some(ids);
        let faces = arrangement_faces(&core.points, &core.topology);
        Ok(Self {
            core,
            typed,
            faces,
            component_count,
            thread_bound: std::marker::PhantomData,
        })
    }

    /// Winding number per face from one `seed` value per component
    /// (`seeds[component]` = the winding of that component's OUTER region),
    /// filled by BFS across twin half-edges: crossing `a -> b` from its
    /// left face onto its right face subtracts the half-edge's directed
    /// multiplicity.
    ///
    /// Precondition: the source segments form CLOSED cycles (every vertex
    /// has even degree). A dangling edge inside a face would create an
    /// extra zero-area walk that this seeding would mislabel with the
    /// component's outside winding.
    pub(crate) fn face_windings(&mut self, seeds: &[W]) -> Vec<W> {
        debug_assert_eq!(seeds.len(), self.component_count as usize);
        let mut windings = vec![W::UNSET; self.faces.len()];
        let mut queue = std::mem::take(&mut self.core.winding_queue);
        queue.clear();
        queue.reserve(self.faces.len());
        // Every component's outer face is its MOST NEGATIVE walk (it
        // encloses the whole component, so no other walk can be more
        // negative). Seeding anything else — in particular zero-area
        // degenerate faces from coincident strokes — would mislabel an
        // interior face with the outside winding and poison the fill.
        let mut outer = std::mem::take(&mut self.core.outer);
        outer.clear();
        outer.reserve(self.component_count as usize);
        outer.resize(self.component_count as usize, None);
        for (face_id, face) in self.faces.iter().enumerate() {
            let slot = &mut outer[face.component as usize];
            let better = slot.is_none_or(|current| {
                face.area_sign
                    .prefers_as_outer_face_than(self.faces[current as usize].area_sign)
            });
            if better {
                *slot = Some(face_id as u32);
            }
        }
        for (component, face_id) in outer.iter().enumerate() {
            if let Some(face_id) = face_id {
                windings[*face_id as usize] = seeds[component];
                queue.push_back(*face_id);
            }
        }
        while let Some(face_id) = queue.pop_front() {
            let winding = windings[face_id as usize];
            // The half-edge `a -> b` has its face on the LEFT; crossing to
            // the twin's face (the right side) subtracts its multiplicity.
            let range = self.face_starts[face_id as usize] as usize
                ..self.face_starts[face_id as usize + 1] as usize;
            for &slot in &self.face_slots[range] {
                let slot = slot as usize;
                let twin = self.twin_slot(slot);
                let neighbor = self.face_of[twin];
                if windings[neighbor as usize] != W::UNSET {
                    continue;
                }
                windings[neighbor as usize] = winding.sub(self.typed.multiplicities[slot]);
                queue.push_back(neighbor);
            }
        }
        // Faces the BFS could not reach belong to fully degenerate
        // sub-structures (zero-area coincident strokes); they enclose
        // nothing and take their component's outside winding.
        for (face_id, winding) in windings.iter_mut().enumerate() {
            if *winding == W::UNSET {
                *winding = seeds[self.faces[face_id].component as usize];
            }
        }
        self.core.outer = outer;
        self.core.winding_queue = queue;
        windings
    }

    pub(crate) fn component_probes_from_slots(
        &self,
        points: &mut Vec<XY>,
        probes: &mut Vec<Option<XY>>,
    ) {
        probes.clear();
        probes.reserve(self.component_count as usize);
        probes.resize(self.component_count as usize, None);
        for (vertex, &component) in self.component_of.iter().enumerate() {
            let slot = &mut probes[component as usize];
            if slot.is_none() {
                *slot = Some(self.points[vertex]);
            }
        }
        // Every component is seeded by at least one vertex above.
        points.clear();
        points.reserve(probes.len());
        points.extend(
            probes
                .iter()
                .copied()
                .map(|point| point.expect("each component has a probe vertex")),
        );
    }

    /// Component id of the loop whose first point is `point` (must be a
    /// vertex of the arrangement).
    pub(crate) fn component_of_point(&self, point: XY) -> u32 {
        let vertex = self
            .ids
            .as_ref()
            .expect("completed arrangement has vertex ids")[&PointKey::new(point)];
        self.component_of[vertex as usize]
    }

    pub(crate) fn twin_slot(&self, slot: usize) -> usize {
        self.twins[slot] as usize
    }
}
