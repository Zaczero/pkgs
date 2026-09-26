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
use crate::geometry::{De9im, Dimension, HashMap, Loc, Result, RingKind};

impl ExactSourceInventory {
    pub(in crate::geometry) fn relate(&self, partition: &ExactPartition<'_>) -> Result<De9im> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        let operands = self.operand_count();
        let line_boundaries = self.node_source_flags(partition, facts.point_membership.len())?;
        let mut matrix = De9im::empty_disjoint();
        for membership in facts.face_membership.chunks_exact(operands) {
            let [left, right] = membership else {
                unreachable!("DE-9IM relation has exactly two operands")
            };
            matrix.set_at_least(location(*left), location(*right), Dimension::Surface);
        }
        for (span, &[left_face, right_face]) in facts.span_faces.iter().enumerate() {
            let (left_face, right_face) = (left_face as usize, right_face as usize);
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

    #[expect(
        clippy::indexing_slicing,
        reason = "source, node, face, and span IDs come from the same admitted inventory and topology; cell extents are allocated from their row counts"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "flattened cell offsets are bounded by the checked ID domains and their owning row extents"
    )]
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
        let face_cells = (global.count as usize).wrapping_mul(operands);
        let span_cells = partition.spans.len().wrapping_mul(operands);
        let face_slots = self.global_face_slots(topology, global);
        let span_faces = Self::span_faces(partition, topology, edges, global);
        let mut facts = CellFacts {
            face_membership: Vec::new(),
            span_raw: Vec::new(),
            span_curve: Vec::new(),
            span_support: Vec::new(),
            span_faces,
            point_membership: Vec::new(),
        };
        facts.face_membership.reserve(face_cells);
        facts.face_membership.resize(face_cells, false);
        facts.span_raw.reserve(span_cells);
        facts.span_raw.resize(span_cells, [false; 2]);
        facts.span_curve.reserve(span_cells);
        facts.span_curve.resize(span_cells, false);
        facts.span_support.reserve(span_cells);
        facts.span_support.resize(span_cells, false);
        let node_cells = partition.node_count().wrapping_mul(operands);
        facts.point_membership.reserve(node_cells);
        facts.point_membership.resize(node_cells, false);
        let mut shell_support = Vec::new();
        let mut hole_support = Vec::new();
        shell_support.reserve(self.polygon_count);
        hole_support.reserve(self.polygon_count);
        shell_support.resize(self.polygon_count, false);
        hole_support.resize(self.polygon_count, false);
        let mut touched_polygons = Vec::with_capacity(partition.spans.len());
        let mut point_sources_by_node = Vec::with_capacity(partition.point_sources.len());
        point_sources_by_node.extend_from_slice(&partition.point_sources);
        point_sources_by_node.sort_unstable_by_key(|(node, _)| node.0);
        let mut point_polygon_index =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        point_polygon_index.reserve(partition.point_sources.len());
        let mut point_polygon_states = Vec::with_capacity(partition.point_sources.len());
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
                point_polygon_states.push((node, polygon, false));
                state
            };
            let range = topology.starts[node.0 as usize] as usize
                ..topology.starts[node.0 as usize + 1] as usize;
            if range.is_empty() {
                let component = topology.component_of[node.0 as usize] as usize;
                point_shell_faces.push((global.component_hosts[component], state));
            } else {
                for slot in range {
                    point_shell_faces
                        .push((global.walk_faces[topology.face_of[slot] as usize], state));
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
                            touched_polygons.push(polygon as u32);
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
                            facts.span_raw[span * operands + operand][side] = true;
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
        for (node, _, operand) in self.isolated_point_occurrences(partition) {
            let operand = operand as usize;
            facts.point_membership[node.0 as usize * operands + operand] = true;
        }
        for &(node, source) in &partition.point_sources {
            let source = source as usize;
            if self.source_rings[source].is_none() {
                let operand = self.source_operands[source] as usize;
                facts.point_membership[node.0 as usize * operands + operand] = true;
            }
        }

        let span_rows = facts
            .span_curve
            .chunks_exact_mut(operands)
            .zip(facts.span_support.chunks_exact_mut(operands))
            .zip(facts.span_raw.chunks_exact(operands));
        for (span, (faces, ((curve_row, support_row), raw_row))) in
            facts.span_faces.iter().zip(span_rows).enumerate()
        {
            let [left, right] = *faces;
            let (left, right) = (left as usize, right as usize);
            for (operand, ((curve, support), raw)) in curve_row
                .iter_mut()
                .zip(support_row)
                .zip(raw_row)
                .enumerate()
            {
                let [left_raw, right_raw] = *raw;
                *curve = self.explicit_curve_support(partition, span, operand);
                *support = *curve
                    || left_raw
                    || right_raw
                    || facts.face_membership[left * operands + operand]
                    || facts.face_membership[right * operands + operand];
            }
        }
        Ok(facts)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "walk IDs come from global_faces and slot cursors address the exact topology CSR"
    )]
    pub(super) fn global_face_slots(
        &self,
        topology: &IndexedTopology,
        global: &GlobalFaces,
    ) -> (Vec<u32>, Vec<u32>) {
        let count = global.count as usize;
        let mut counts = vec![0_u32; count];
        for &walk in &topology.face_of {
            let face = global.walk_faces[walk as usize] as usize;
            counts[face] = counts[face].wrapping_add(1);
        }
        let mut starts = Vec::with_capacity(count.wrapping_add(1));
        let mut total = 0_u32;
        for count in &mut counts {
            let row_count = *count;
            starts.push(total);
            *count = total;
            total = total.wrapping_add(row_count);
        }
        starts.push(total);
        let mut cursor = counts;
        let mut slots = vec![0; topology.face_of.len()];
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let face = global.walk_faces[walk as usize] as usize;
            let index = cursor[face] as usize;
            slots[index] = slot as u32;
            cursor[face] = cursor[face].wrapping_add(1);
        }
        (starts, slots)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "local_topology pairs every slot with an admitted span ID and global walk face"
    )]
    pub(super) fn span_faces(
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Vec<[u32; 2]> {
        let mut faces = vec![[u32::MAX; 2]; partition.spans.len()];
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let span = edges[slot] as usize;
            let face = global.walk_faces[walk as usize];
            let twin = topology.twins[slot] as usize;
            let other = global.walk_faces[topology.face_of[twin] as usize];
            let pair = &mut faces[span];
            if pair[0] == u32::MAX {
                *pair = [face, other];
            }
        }
        faces
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span owner source IDs and their operand/ring columns come from the same inventory"
    )]
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

    #[expect(
        clippy::indexing_slicing,
        reason = "node, dart, source, and ring IDs are admitted by this partition's topology and provenance"
    )]
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

    #[expect(
        clippy::indexing_slicing,
        reason = "line boundary keys and operand IDs are produced by the same staged inventory and point-node partition"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the flattened node/operand index addresses the exact node-cell table sized by build_cell_facts"
    )]
    pub(super) fn node_source_flags(
        &self,
        partition: &ExactPartition<'_>,
        cells: usize,
    ) -> Result<Vec<bool>> {
        let operands = self.operand_count();
        let mut line_boundaries = Vec::with_capacity(cells);
        line_boundaries.resize(cells, false);

        let mut stored = HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        stored.reserve(partition.node_count());
        for index in 0..partition.node_count() {
            if let Some(key) = partition.stored_key(index) {
                stored.insert(key, index as u32);
            }
        }
        for &(operand, key) in &self.line_boundaries {
            let node = stored[&key];
            let index = node as usize * operands + operand as usize;
            line_boundaries[index] = !line_boundaries[index];
        }
        Ok(line_boundaries)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face and span IDs are admitted by the same exact partition and fact tables"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "flattened face and span cells are within the exact row-major table extents"
    )]
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

    #[expect(
        clippy::indexing_slicing,
        reason = "NodeId, CSR slot, span, host-face, and operand IDs are admitted by this partition and its row-major facts"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "flattened fact indices stay within their node/span rows; residual degree is only a mod-2 boundary count"
    )]
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
            let [left_raw, right_raw] = facts.span_raw[index];
            if left_raw || right_raw {
                residual_degree =
                    residual_degree.wrapping_add(usize::from(!facts.span_curve[index]));
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
        let winding_rings = self.winding_source_rings();
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

    #[expect(
        clippy::indexing_slicing,
        reason = "selected face/span/node IDs are the same IDs used to size the exact fact and cell tables"
    )]
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "remaining flattened indices are row offsets within those exact tables"
    )]
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
        cells.faces.reserve(global.count as usize);
        cells.faces.resize(global.count as usize, false);
        cells.spans.reserve(partition.spans.len());
        cells.spans.resize(partition.spans.len(), false);
        cells.nodes.reserve(partition.node_count());
        cells.nodes.resize(partition.node_count(), false);
        for (selected, membership) in cells
            .faces
            .iter_mut()
            .zip(facts.face_membership.chunks_exact(operands))
        {
            *selected = select_operation(operation, membership);
        }

        let mut node_membership = Vec::with_capacity(facts.point_membership.len());
        node_membership.extend_from_slice(&facts.point_membership);
        for ((selected, span), membership) in cells
            .spans
            .iter_mut()
            .zip(&partition.spans)
            .zip(facts.span_support.chunks_exact(operands))
        {
            *selected = select_operation(operation, membership);
            if *selected {
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
            for (operand, &present) in membership.iter().enumerate() {
                if present {
                    node_membership[span.start.0 as usize * operands + operand] = true;
                    node_membership[span.end.0 as usize * operands + operand] = true;
                }
            }
        }
        for &node in &partition.isolated {
            let component = topology.component_of[node.0 as usize] as usize;
            let host = global.component_hosts[component] as usize;
            for operand in 0..operands {
                node_membership[node.0 as usize * operands + operand] |=
                    facts.face_membership[host * operands + operand];
            }
        }
        for (node, (cell_node, membership)) in cells
            .nodes
            .iter_mut()
            .zip(node_membership.chunks_exact_mut(operands))
            .enumerate()
        {
            let range = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            if !range.is_empty() {
                for (operand, present) in membership.iter_mut().enumerate() {
                    let any = range.clone().any(|slot| {
                        let face = global.walk_faces[topology.face_of[slot] as usize] as usize;
                        facts.face_membership[face * operands + operand]
                    });
                    *present |= any;
                }
            }
            *cell_node |= select_operation(operation, membership);
        }
        Ok(cells)
    }

    pub(super) fn linework_source_rings(&self) -> Vec<Option<u32>> {
        self.source_rings
            .iter()
            .zip(&self.source_ring_kinds)
            .map(|(&ring, &kind)| {
                matches!(kind, Some(RingKind::Winding))
                    .then(|| ring.expect("a ring source always carries its ring identity"))
            })
            .collect()
    }
}
