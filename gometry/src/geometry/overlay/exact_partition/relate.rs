//! DE-9IM relate over an exact partition.
//!
//! The matrix is read off the partition's cells rather than recomputed:
//! each cell records which operands occupy it and which spans bound it, so
//! interior, boundary and exterior intersections follow from the cell facts.

use super::{
    ArealCells, CellFacts, ExactPartition, ExactSourceInventory, GlobalFaces, NodeId,
    PolygonOccupancy, location, node_source_range, ring_occupied, rings_occupied, select_operation,
};
use crate::error::ResourceError;
use crate::geometry::arrangement::IndexedTopology;
use crate::geometry::{De9im, Dimension, GeometryErrorKind, HashMap, Loc, Result, RingKind};
use crate::{try_push_vec, try_reserve_vec};

impl ExactSourceInventory {
    pub(in crate::geometry) fn relate(&self, partition: &ExactPartition<'_>) -> Result<De9im> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings()?;
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        let line_boundaries = self.node_source_flags(partition)?;
        let operands = self.operand_count();
        let mut matrix = De9im::empty_disjoint();
        for face in 0..global.count as usize {
            matrix.set_at_least(
                location(facts.face_membership[face * operands]),
                location(facts.face_membership[face * operands + 1]),
                Dimension::Surface,
            );
        }
        for span in 0..partition.spans.len() {
            let left_face = facts.span_faces[span][0] as usize;
            let right_face = facts.span_faces[span][1] as usize;
            let left =
                self.span_location_from_facts(0, span, left_face, right_face, &facts, operands);
            let right =
                self.span_location_from_facts(1, span, left_face, right_face, &facts, operands);
            if left != Loc::Exterior || right != Loc::Exterior {
                matrix.set_at_least(left, right, Dimension::Curve);
            }
        }
        for node in 0..partition.node_count() {
            let left = self.node_location_from_facts(
                0,
                NodeId(node as u32),
                &topology,
                &edges,
                &global,
                &facts,
                &line_boundaries,
                &facts.point_membership,
            );
            let right = self.node_location_from_facts(
                1,
                NodeId(node as u32),
                &topology,
                &edges,
                &global,
                &facts,
                &line_boundaries,
                &facts.point_membership,
            );
            if left != Loc::Exterior || right != Loc::Exterior {
                matrix.set_at_least(left, right, Dimension::Point);
            }
        }
        Ok(matrix)
    }

    pub(super) fn build_cell_facts(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        winding_rings: &[Option<u32>],
        odd: bool,
    ) -> Result<CellFacts> {
        let operands = self.operand_count();
        let face_cells = (global.count as usize)
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let span_cells = partition
            .spans
            .len()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let raw_cells = span_cells
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        let face_slots = self.global_face_slots(topology, edges, global)?;
        let span_faces = self.span_faces(topology, edges, global)?;
        let mut facts = CellFacts {
            face_membership: Vec::new(),
            span_raw: Vec::new(),
            span_curve: Vec::new(),
            span_support: Vec::new(),
            span_faces,
            point_membership: Vec::new(),
        };
        try_reserve_vec(&mut facts.face_membership, face_cells)?;
        facts.face_membership.resize(face_cells, false);
        try_reserve_vec(&mut facts.span_raw, raw_cells)?;
        facts.span_raw.resize(raw_cells, false);
        try_reserve_vec(&mut facts.span_curve, span_cells)?;
        facts.span_curve.resize(span_cells, false);
        try_reserve_vec(&mut facts.span_support, span_cells)?;
        facts.span_support.resize(span_cells, false);
        let node_cells = partition
            .node_count()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut facts.point_membership, node_cells)?;
        facts.point_membership.resize(node_cells, false);
        let mut shell_support = Vec::new();
        let mut hole_support = Vec::new();
        try_reserve_vec(&mut shell_support, self.polygon_count)?;
        try_reserve_vec(&mut hole_support, self.polygon_count)?;
        shell_support.resize(self.polygon_count, false);
        hole_support.resize(self.polygon_count, false);
        let mut touched_polygons = Vec::new();
        try_reserve_vec(&mut touched_polygons, partition.spans.len())?;
        let mut point_sources_by_node = Vec::new();
        try_reserve_vec(&mut point_sources_by_node, partition.point_sources.len())?;
        point_sources_by_node.extend_from_slice(&partition.point_sources);
        point_sources_by_node.sort_unstable_by_key(|(node, _)| node.0);
        let mut point_polygon_index =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        point_polygon_index
            .try_reserve(partition.point_sources.len())
            .map_err(crate::error::Error::from)?;
        let mut point_polygon_states = Vec::new();
        try_reserve_vec(&mut point_polygon_states, partition.point_sources.len())?;
        let mut point_shell_faces = Vec::new();
        for &(node, source) in &partition.point_sources {
            let source_index = source as usize;
            let Some(ring) = self.source_rings[source_index] else {
                continue;
            };
            if self.ring_holes[ring as usize]
                || self.source_ring_kinds[source_index] != Some(RingKind::Collapsed)
            {
                continue;
            }
            let polygon = self.ring_polygons[ring as usize];
            let state = if let Some(&state) = point_polygon_index.get(&(node, polygon)) {
                state
            } else {
                let state = u32::try_from(point_polygon_states.len())
                    .map_err(|_| ResourceError::SizeOverflow)?;
                point_polygon_index.insert((node, polygon), state);
                try_reserve_vec(&mut point_polygon_states, 1)?;
                try_push_vec(&mut point_polygon_states, (node, polygon, false))?;
                state
            };
            let range = topology.starts[node.0 as usize] as usize
                ..topology.starts[node.0 as usize + 1] as usize;
            if range.is_empty() {
                let component = topology.component_of[node.0 as usize] as usize;
                try_push_vec(
                    &mut point_shell_faces,
                    (global.component_hosts[component], state),
                )?;
            } else {
                for slot in range {
                    try_push_vec(
                        &mut point_shell_faces,
                        (global.walk_faces[topology.face_of[slot] as usize], state),
                    )?;
                }
            }
        }
        point_shell_faces.sort_unstable_by_key(|(face, _)| *face);
        let mut point_shell_cursor = 0;
        let mut occupancy = PolygonOccupancy::new(&self.polygon_rings, self.ring_count, odd)?;
        partition.visit_face_windings_with_hook(
            topology,
            edges,
            global,
            winding_rings,
            self.ring_count,
            &mut occupancy,
            |winding, ring, direction, occupancy| {
                occupancy.cross(winding, ring, direction);
                Ok(())
            },
            |face, winding, occupancy| {
                let face = face as usize;
                for operand in 0..operands {
                    let occupied = occupancy.operand_occupied(operand);
                    debug_assert_eq!(
                        occupied,
                        self.polygon_rings[operand]
                            .iter()
                            .any(|rings| rings_occupied(winding, rings.clone(), odd)),
                        "tracked occupancy diverged from the ring windings"
                    );
                    facts.face_membership[face * operands + operand] = occupied;
                }
                while point_shell_cursor < point_shell_faces.len()
                    && point_shell_faces[point_shell_cursor].0 == face as u32
                {
                    let (_, state_index) = point_shell_faces[point_shell_cursor];
                    let state = &mut point_polygon_states[state_index as usize];
                    let node = state.0;
                    let polygon = state.1 as usize;
                    let hole = self.polygon_ring_ranges[polygon]
                        .clone()
                        .skip(1)
                        .any(|hole| {
                            if self.ring_kinds[hole] == RingKind::Winding {
                                ring_occupied(winding[hole], odd)
                            } else {
                                self.collapsed_ring_at_node(
                                    partition,
                                    topology,
                                    edges,
                                    &point_sources_by_node,
                                    node,
                                    hole,
                                )
                            }
                        });
                    state.2 |= hole;
                    point_shell_cursor += 1;
                }
                let start = face_slots.0[face] as usize;
                let end = face_slots.0[face + 1] as usize;
                for &slot in &face_slots.1[start..end] {
                    let slot = slot as usize;
                    let span = edges[slot] as usize;
                    let side = usize::from(facts.span_faces[span][1] as usize == face);
                    for &(source, _) in partition.owners(span) {
                        let source = source as usize;
                        let Some(ring) = self.source_rings[source] else {
                            continue;
                        };
                        let occupied = match self.source_ring_kinds[source] {
                            Some(RingKind::Winding) => ring_occupied(winding[ring as usize], odd),
                            Some(RingKind::Collapsed) => true,
                            None => false,
                        };
                        if !occupied {
                            continue;
                        }
                        let polygon = self.ring_polygons[ring as usize] as usize;
                        if !shell_support[polygon] && !hole_support[polygon] {
                            try_reserve_vec(&mut touched_polygons, 1)?;
                            try_push_vec(&mut touched_polygons, polygon as u32)?;
                        }
                        if self.ring_holes[ring as usize] {
                            hole_support[polygon] = true;
                        } else {
                            shell_support[polygon] = true;
                        }
                    }
                    for &polygon in &touched_polygons {
                        let polygon = polygon as usize;
                        if shell_support[polygon] && !hole_support[polygon] {
                            hole_support[polygon] = self.polygon_ring_ranges[polygon]
                                .clone()
                                .skip(1)
                                .any(|hole| {
                                    self.ring_kinds[hole] == RingKind::Winding
                                        && ring_occupied(winding[hole], odd)
                                });
                        }
                        if shell_support[polygon] && !hole_support[polygon] {
                            let operand = self.polygon_operands[polygon] as usize;
                            facts.span_raw[(span * 2 + side) * operands + operand] = true;
                        }
                        shell_support[polygon] = false;
                        hole_support[polygon] = false;
                    }
                    touched_polygons.clear();
                }
                Ok(())
            },
        )?;
        for (node, polygon, hole) in point_polygon_states {
            if !hole {
                let operand = self.polygon_operands[polygon as usize] as usize;
                facts.point_membership[node.0 as usize * operands + operand] = true;
            }
        }
        if partition.point_nodes.len() != self.isolated_operands.len() {
            return Err(GeometryErrorKind::Invalid(
                "exact partition point sources lost their occurrence mapping".into(),
            )
            .into());
        }
        for (index, &node) in partition.point_nodes.iter().enumerate() {
            let operand = self.isolated_operands[index] as usize;
            facts.point_membership[node.0 as usize * operands + operand] = true;
        }
        for &(node, source) in &partition.point_sources {
            let source = source as usize;
            if self.source_rings[source].is_none() {
                let operand = self.source_operands[source] as usize;
                facts.point_membership[node.0 as usize * operands + operand] = true;
            }
        }

        for span in 0..partition.spans.len() {
            let left = facts.span_faces[span][0] as usize;
            let right = facts.span_faces[span][1] as usize;
            for operand in 0..operands {
                let index = span * operands + operand;
                facts.span_curve[index] = self.explicit_curve_support(partition, span, operand);
                facts.span_support[index] = facts.span_curve[index]
                    || facts.span_raw[(span * 2) * operands + operand]
                    || facts.span_raw[(span * 2 + 1) * operands + operand]
                    || facts.face_membership[left * operands + operand]
                    || facts.face_membership[right * operands + operand];
            }
        }
        Ok(facts)
    }

    pub(super) fn global_face_slots(
        &self,
        topology: &IndexedTopology,
        _edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<(Vec<u32>, Vec<u32>)> {
        let count = global.count as usize;
        let mut counts = Vec::new();
        try_reserve_vec(&mut counts, count)?;
        counts.resize(count, 0_u32);
        for &walk in &topology.face_of {
            let face = global.walk_faces[walk as usize] as usize;
            counts[face] = counts[face]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut starts: Vec<u32> = Vec::new();
        try_reserve_vec(
            &mut starts,
            count.checked_add(1).ok_or(ResourceError::SizeOverflow)?,
        )?;
        try_push_vec(&mut starts, 0)?;
        for count in counts {
            let next = starts
                .last()
                .copied()
                .unwrap_or(0_u32)
                .checked_add(count)
                .ok_or(ResourceError::SizeOverflow)?;
            try_push_vec(&mut starts, next)?;
        }
        let mut slots = Vec::new();
        try_reserve_vec(&mut slots, topology.face_of.len())?;
        slots.resize(topology.face_of.len(), 0);
        let mut cursor = Vec::new();
        try_reserve_vec(&mut cursor, count)?;
        cursor.extend_from_slice(&starts[..count]);
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let face = global.walk_faces[walk as usize] as usize;
            let index = cursor[face] as usize;
            slots[index] = u32::try_from(slot).map_err(|_| ResourceError::SizeOverflow)?;
            cursor[face] += 1;
        }
        Ok((starts, slots))
    }

    pub(super) fn span_faces(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<Vec<[u32; 2]>> {
        let mut faces = Vec::new();
        let face_capacity = edges.iter().copied().max().map_or(Ok(0_usize), |edge| {
            (edge as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)
        })?;
        try_reserve_vec(&mut faces, face_capacity)?;
        if !edges.is_empty() {
            faces.resize(face_capacity, [u32::MAX; 2]);
        }
        for (slot, &span) in edges.iter().enumerate() {
            let face = global.walk_faces[topology.face_of[slot] as usize];
            let twin = topology.twins[slot] as usize;
            let other = global.walk_faces[topology.face_of[twin] as usize];
            let pair = &mut faces[span as usize];
            if pair[0] == u32::MAX {
                *pair = [face, other];
            }
        }
        Ok(faces)
    }

    pub(super) fn explicit_curve_support(
        &self,
        partition: &ExactPartition<'_>,
        span: usize,
        operand: usize,
    ) -> bool {
        partition.owners(span).iter().any(|&(source, _)| {
            let source = source as usize;
            self.source_operands[source] as usize == operand && self.source_rings[source].is_none()
        })
    }

    pub(super) fn collapsed_ring_at_node(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        point_sources_by_node: &[(NodeId, u32)],
        node: NodeId,
        ring: usize,
    ) -> bool {
        let mut range = topology.starts[node.0 as usize] as usize
            ..topology.starts[node.0 as usize + 1] as usize;
        if range.any(|slot| {
            partition
                .owners(edges[slot] as usize)
                .iter()
                .any(|&(source, _)| {
                    self.source_rings[source as usize] == Some(ring as u32)
                        && self.source_ring_kinds[source as usize] == Some(RingKind::Collapsed)
                })
        }) {
            return true;
        }
        let mut range = node_source_range(point_sources_by_node, node);
        range.any(|index| {
            let source = point_sources_by_node[index].1 as usize;
            self.source_rings[source] == Some(ring as u32)
                && self.source_ring_kinds[source] == Some(RingKind::Collapsed)
        })
    }

    pub(super) fn node_source_flags(&self, partition: &ExactPartition<'_>) -> Result<Vec<bool>> {
        let operands = self.operand_count();
        let cells = partition
            .node_count()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut line_boundaries = Vec::new();
        try_reserve_vec(&mut line_boundaries, cells)?;
        line_boundaries.resize(cells, false);

        let mut stored = HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        stored
            .try_reserve(partition.node_count())
            .map_err(crate::error::Error::from)?;
        for index in 0..partition.node_count() {
            if let Some(key) = partition.stored_key(index) {
                stored.insert(key, index as u32);
            }
        }
        for &(operand, key) in &self.line_boundaries {
            let Some(&node) = stored.get(&key) else {
                continue;
            };
            let index = node as usize * operands + operand as usize;
            if let Some(value) = line_boundaries.get_mut(index) {
                *value = !*value;
            }
        }
        Ok(line_boundaries)
    }

    pub(super) fn span_location_from_facts(
        &self,
        operand: usize,
        span: usize,
        left_face: usize,
        right_face: usize,
        facts: &CellFacts,
        operands: usize,
    ) -> Loc {
        let left = facts.face_membership[left_face * operands + operand];
        let right = facts.face_membership[right_face * operands + operand];
        if left || right {
            return match (left, right) {
                (true, true) => Loc::Interior,
                _ => Loc::Boundary,
            };
        }
        if facts.span_support[span * operands + operand] {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    pub(super) fn node_location_from_facts(
        &self,
        operand: usize,
        node: NodeId,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        facts: &CellFacts,
        line_boundaries: &[bool],
        point_membership: &[bool],
    ) -> Loc {
        let operands = self.operand_count();
        let range = topology.starts[node.0 as usize] as usize
            ..topology.starts[node.0 as usize + 1] as usize;
        let (any_area, all_area) = if range.is_empty() {
            let component = topology.component_of[node.0 as usize] as usize;
            let face = global.component_hosts[component] as usize;
            let value = facts.face_membership[face * operands + operand];
            (value, value)
        } else {
            let mut any = false;
            let mut all = true;
            for slot in range.clone() {
                let face = global.walk_faces[topology.face_of[slot] as usize] as usize;
                let value = facts.face_membership[face * operands + operand];
                any |= value;
                all &= value;
            }
            (any, all)
        };
        if any_area {
            return if all_area {
                Loc::Interior
            } else {
                Loc::Boundary
            };
        }

        let mut line = false;
        let mut residual_degree = 0_usize;
        for slot in range {
            let span = edges[slot] as usize;
            let index = span * operands + operand;
            line |= facts.span_support[index];
            if facts.span_raw[span * 2 * operands + operand]
                || facts.span_raw[(span * 2 + 1) * operands + operand]
            {
                residual_degree += usize::from(!facts.span_curve[index]);
            }
        }
        let node_index = node.0 as usize * operands + operand;
        if line {
            return if line_boundaries[node_index] ^ (residual_degree % 2 == 1) {
                Loc::Boundary
            } else {
                Loc::Interior
            };
        }
        if point_membership[node_index] {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    pub(super) fn select_cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        operation: crate::geometry::OverlayOp,
        odd: bool,
    ) -> Result<ArealCells> {
        let winding_rings = self.winding_source_rings()?;
        self.select_cells_with_winding_rings(
            partition,
            topology,
            edges,
            global,
            operation,
            odd,
            &winding_rings,
        )
    }

    pub(super) fn select_cells_with_winding_rings(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        operation: crate::geometry::OverlayOp,
        odd: bool,
        winding_rings: &[Option<u32>],
    ) -> Result<ArealCells> {
        let operands = self.operand_count();
        let facts =
            self.build_cell_facts(partition, topology, edges, global, winding_rings, odd)?;
        let mut cells = ArealCells {
            faces: Vec::new(),
            spans: Vec::new(),
            nodes: Vec::new(),
        };
        try_reserve_vec(&mut cells.faces, global.count as usize)?;
        cells.faces.resize(global.count as usize, false);
        try_reserve_vec(&mut cells.spans, partition.spans.len())?;
        cells.spans.resize(partition.spans.len(), false);
        try_reserve_vec(&mut cells.nodes, partition.node_count())?;
        cells.nodes.resize(partition.node_count(), false);
        for face in 0..global.count as usize {
            cells.faces[face] = select_operation(
                operation,
                &facts.face_membership[face * operands..][..operands],
            );
        }

        let node_cells = partition
            .node_count()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut node_membership = Vec::new();
        try_reserve_vec(&mut node_membership, node_cells)?;
        node_membership.resize(node_cells, false);
        for span_index in 0..partition.spans.len() {
            let membership = &facts.span_support[span_index * operands..][..operands];
            cells.spans[span_index] = select_operation(operation, membership);
            if cells.spans[span_index] {
                let span = partition.spans[span_index];
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
            for operand in 0..operands {
                if facts.span_support[span_index * operands + operand] {
                    let span = partition.spans[span_index];
                    node_membership[span.start.0 as usize * operands + operand] = true;
                    node_membership[span.end.0 as usize * operands + operand] = true;
                }
            }
        }
        for (membership, &point) in node_membership.iter_mut().zip(&facts.point_membership) {
            *membership |= point;
        }
        for &node in &partition.isolated {
            let component = topology.component_of[node.0 as usize] as usize;
            let host = global.component_hosts[component] as usize;
            for operand in 0..operands {
                node_membership[node.0 as usize * operands + operand] |=
                    facts.face_membership[host * operands + operand];
            }
        }
        for node in 0..partition.node_count() {
            let range = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            if range.is_empty() {
                continue;
            }
            for operand in 0..operands {
                let any = range.clone().any(|slot| {
                    let face = global.walk_faces[topology.face_of[slot] as usize] as usize;
                    facts.face_membership[face * operands + operand]
                });
                node_membership[node * operands + operand] |= any;
            }
        }
        for node in 0..partition.node_count() {
            let membership = &node_membership[node * operands..][..operands];
            cells.nodes[node] |= select_operation(operation, membership);
        }
        Ok(cells)
    }

    pub(super) fn linework_source_rings(&self) -> Result<Vec<Option<u32>>> {
        let mut winding_rings = Vec::new();
        try_reserve_vec(&mut winding_rings, self.source_rings.len())?;
        for (&ring, &kind) in self.source_rings.iter().zip(&self.source_ring_kinds) {
            try_push_vec(
                &mut winding_rings,
                matches!(kind, Some(RingKind::Winding))
                    .then(|| ring.expect("a ring source always carries its ring identity")),
            )?;
        }
        Ok(winding_rings)
    }
}
