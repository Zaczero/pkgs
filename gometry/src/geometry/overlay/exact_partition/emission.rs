//! Planning and emitting the faces of an exact arrangement.
//!
//! Once the partition knows its global faces, emission is a separate job:
//! pick the cells a stored-ring winding selects, plan the chords that bound
//! them, and round each face to a ring. The checks that rounding preserved
//! incidence, cyclic order and face identity are centralized in the sibling
//! `embedding` module.

use std::cmp::Ordering;
use std::ops::Range;

use super::embedding::{Embedding, Emission};
use super::rays::RayPoints;
use super::{
    ArealCells, Axis, ExactPartition, ExactSourceInventory, KeptNodes, NodeId, rings_occupied,
};
use crate::error::ResourceError;
use crate::geometry::arrangement::IndexedTopology;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::{point_on_segment, stored_cross_sign, stored_orientation_sign};
use crate::geometry::tessellation::exact::{ExactPoint, ExactRational};
use crate::geometry::{
    CoordSeq, GeometryErrorKind, Point, Polygon, Result, Ring, Unmetered, Work, XY, same_point,
};

pub(in crate::geometry) struct GlobalFaces {
    pub(in crate::geometry) walk_faces: Vec<u32>,
    pub(in crate::geometry) component_hosts: Vec<u32>,
    pub(in crate::geometry) count: u32,
}

pub(super) struct CellFacts {
    pub(super) face_membership: Vec<bool>,
    pub(super) span_raw: Vec<[bool; 2]>,
    pub(super) span_curve: Vec<bool>,
    pub(super) span_support: Vec<bool>,
    pub(super) span_faces: Vec<[u32; 2]>,
    pub(super) point_membership: Vec<bool>,
}

pub(super) struct PlannedChord {
    pub(super) sources: Range<usize>,
    pub(super) sides: [u32; 2],
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry) struct OverlaySourceLocation {
    pub(in crate::geometry) operand: u32,
    pub(in crate::geometry) ring: u32,
    pub(in crate::geometry) edge: u32,
}

#[derive(Clone, Debug)]
pub(in crate::geometry) struct OverlaySourceEdgeUse {
    pub(in crate::geometry) source_id: u32,
    pub(in crate::geometry) location: OverlaySourceLocation,
    pub(in crate::geometry) forward: bool,
    pub(in crate::geometry) start_parameter: ExactRational,
    pub(in crate::geometry) end_parameter: ExactRational,
}

pub(in crate::geometry) struct OverlayPolygonPartWithSources {
    pub(in crate::geometry) polygon: Polygon,
    pub(in crate::geometry) edge_sources: Vec<Vec<Vec<OverlaySourceEdgeUse>>>,
    pub(in crate::geometry) vertices: Vec<Vec<OverlayNodeReceipt>>,
}

#[derive(Clone)]
pub(in crate::geometry) struct OverlayNodeReceipt {
    pub(in crate::geometry) id: u32,
    pub(in crate::geometry) exact_point: ExactPoint,
    pub(in crate::geometry) assignment: XY,
}

pub(super) struct BoundaryPlan {
    pub(super) source_darts: Vec<u32>,
    /// Chords on the boundary between an occupied and an unoccupied areal
    /// face. These are the shell and hole cycles of the selected filled area.
    pub(super) walks: Vec<Vec<PlannedChord>>,
    /// Chords retained from lower-dimensional support with no occupied areal
    /// face on either side. They are residual lines, not areal boundary cycles.
    pub(super) lines: Vec<PlannedChord>,
    pub(super) points: Vec<(NodeId, u32)>,
    pub(super) face_quotient: Vec<u32>,
}

pub(super) struct NodeParts {
    pub(super) points: Vec<NodeId>,
    pub(super) lines: Vec<Vec<NodeId>>,
    pub(super) polygons: Vec<Vec<Vec<NodeId>>>,
}

pub(super) struct CoverageFaceState {
    pub(super) polygon_occupied: Vec<bool>,
    pub(super) row_polygon_counts: Vec<usize>,
    pub(super) active_positions: Vec<usize>,
    pub(super) active_rows: Vec<usize>,
}

impl CoverageFaceState {
    pub(super) fn new(polygon_count: usize, row_count: usize) -> Self {
        Self {
            polygon_occupied: vec![false; polygon_count],
            row_polygon_counts: vec![0; row_count],
            active_positions: vec![usize::MAX; row_count],
            active_rows: Vec::with_capacity(row_count),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "winding ring IDs, polygon owners, and operand rows are constructed together by the exact source inventory"
    )]
    pub(super) fn update_ring(
        &mut self,
        inventory: &ExactSourceInventory,
        winding: &mut [i64],
        ring: u32,
        delta: i64,
    ) {
        let ring_index = ring as usize;
        let before = winding[ring_index] != 0;
        winding[ring_index] = winding[ring_index].wrapping_add(delta);
        let after = winding[ring_index] != 0;
        if before == after {
            return;
        }
        let polygon = inventory.ring_polygons[ring_index] as usize;
        let occupied = rings_occupied(
            winding,
            inventory.polygon_ring_ranges[polygon].clone(),
            false,
        );
        if occupied == self.polygon_occupied[polygon] {
            return;
        }
        self.polygon_occupied[polygon] = occupied;
        let row = inventory.polygon_operands[polygon] as usize;
        if occupied {
            let count = &mut self.row_polygon_counts[row];
            let was_empty = *count == 0;
            *count = count.wrapping_add(1);
            if was_empty {
                let position = self.active_rows.len();
                self.active_rows.push(row);
                self.active_positions[row] = position;
            }
        } else {
            let count = &mut self.row_polygon_counts[row];
            *count = count.wrapping_sub(1);
            if *count == 0 {
                let position = self.active_positions[row];
                self.active_rows.swap_remove(position);
                if position < self.active_rows.len() {
                    let moved = self.active_rows[position];
                    self.active_positions[moved] = position;
                }
                self.active_positions[row] = usize::MAX;
            }
        }
    }
}

impl BoundaryPlan {
    fn chord_count(&self) -> usize {
        self.walks
            .iter()
            .map(Vec::len)
            .chain(std::iter::once(self.lines.len()))
            .fold(0, usize::wrapping_add)
    }

    fn area_chord_count(&self) -> usize {
        self.walks.iter().map(Vec::len).fold(0, usize::wrapping_add)
    }

    fn chord_is_preserved(
        &self,
        chord: &PlannedChord,
        edges: &[u32],
        preserved_spans: &[bool],
    ) -> Result<bool> {
        let mut role = None;
        for source_slot in chord.sources.clone() {
            let slot =
                self.source_darts.get(source_slot).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("source role dart is unknown".into())
                })? as usize;
            let span =
                edges.get(slot).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("source role slot is unknown".into())
                })? as usize;
            let preserved = preserved_spans
                .get(span)
                .copied()
                .ok_or_else(|| GeometryErrorKind::Invalid("source role span is unknown".into()))?;
            if role.is_some_and(|previous| previous != preserved) {
                return Err(GeometryErrorKind::Invalid(
                    "an emitted chord crosses a source-role boundary".into(),
                )
                .into());
            }
            role = Some(preserved);
        }
        role.ok_or_else(|| {
            GeometryErrorKind::Invalid("an emitted chord has no source spans".into()).into()
        })
    }

    fn chord_source_uses(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edge_of_slot: &[u32],
        chord: &PlannedChord,
        source_locations: &[OverlaySourceLocation],
        source_segments: &[crate::geometry::Segment],
    ) -> Result<Vec<OverlaySourceEdgeUse>> {
        let mut result = Vec::new();
        for source_index in chord.sources.clone() {
            let slot = self
                .source_darts
                .get(source_index)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("source edge-use dart is unknown".into())
                })? as usize;
            let span = *edge_of_slot.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("source edge-use span is unknown".into())
            })?;
            let output_start = NodeId(*topology.owners.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("source edge-use start is unknown".into())
            })?);
            let output_end = NodeId(*topology.targets.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("source edge-use end is unknown".into())
            })?);
            for &(source_id, _) in partition.provenance.owners(span as usize) {
                let segment = *source_segments.get(source_id as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source edge-use segment is unknown".into())
                })?;
                let start_parameter =
                    partition.parameter_on_source_segment(segment, output_start)?;
                let end_parameter = partition.parameter_on_source_segment(segment, output_end)?;
                let forward = start_parameter.try_cmp(&end_parameter)?.is_lt();
                let location = *source_locations.get(source_id as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source edge-use origin is unknown".into())
                })?;
                result.push(OverlaySourceEdgeUse {
                    source_id,
                    location,
                    forward,
                    start_parameter,
                    end_parameter,
                });
            }
        }
        Ok(result)
    }

    pub(super) fn polygonize_emission(
        &self,
        topology: &IndexedTopology,
        edge_of_slot: &[u32],
    ) -> Emission {
        debug_assert!(self.walks.is_empty());
        self.emission_with_areal_chords(topology, edge_of_slot, self.chord_count())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source ranges and slot IDs belong to this plan and the topology built from the same partition"
    )]
    fn emission_with_areal_chords(
        &self,
        topology: &IndexedTopology,
        edge_of_slot: &[u32],
        areal_chords: usize,
    ) -> Emission {
        let chords = self.chord_count();
        debug_assert!(areal_chords <= chords);
        let mut items = Vec::with_capacity(chords);
        let mut fan_spans = Vec::with_capacity(if areal_chords == 0 { 0 } else { chords });
        let mut faces = (areal_chords > 0).then(|| Vec::with_capacity(chords));
        for chord in self.walks.iter().flatten().chain(&self.lines) {
            let first = self.source_darts[chord.sources.start] as usize;
            let last = self.source_darts[chord.sources.end.wrapping_sub(1)] as usize;
            items.push([
                NodeId(topology.owners[first]),
                NodeId(topology.targets[last]),
            ]);
            if areal_chords > 0 {
                fan_spans.push([edge_of_slot[first], edge_of_slot[last]]);
            }
            if let Some(faces) = &mut faces {
                faces.push(chord.sides);
            }
        }
        items.reserve(self.points.len());
        if let Some(faces) = &mut faces {
            faces.reserve(self.points.len());
        }
        for &(node, host) in &self.points {
            items.push([node, node]);
            if let Some(faces) = &mut faces {
                faces.push([host, host]);
            }
        }
        debug_assert_eq!(items.len(), chords.wrapping_add(self.points.len()));
        Emission {
            items,
            chords,
            areal_chords,
            fan_spans,
            faces,
        }
    }

    pub(super) fn emission(&self, topology: &IndexedTopology, edge_of_slot: &[u32]) -> Emission {
        self.emission_with_areal_chords(topology, edge_of_slot, self.area_chord_count())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "chord endpoints and incidence nodes come from this boundary plan and its checked CSR topology"
    )]
    pub(super) fn line_walks(&self, topology: &IndexedTopology) -> Vec<Vec<(usize, bool)>> {
        if self.lines.is_empty() {
            return Vec::new();
        }
        let mut ends = Vec::with_capacity(self.lines.len());
        let node_count = topology.component_of.len();
        let mut incidence = Vec::with_capacity(node_count);
        incidence.resize(node_count, (0_usize, [0_usize; 2]));
        for (index, chord) in self.lines.iter().enumerate() {
            let pair = [
                topology.owners[self.source_darts[chord.sources.start] as usize] as usize,
                topology.targets[self.source_darts[chord.sources.end.wrapping_sub(1)] as usize]
                    as usize,
            ];
            for node in pair {
                let (degree, edges) = &mut incidence[node];
                if *degree < 2 {
                    edges[*degree] = index;
                }
                *degree = degree.wrapping_add(1);
            }
            ends.push(pair);
        }
        let mut used = Vec::with_capacity(self.lines.len());
        used.resize(self.lines.len(), false);
        let mut walks = Vec::new();
        // Open chains precede the remaining all-degree-two cycles.
        for cycles in [false, true] {
            for seed in 0..ends.len() {
                if used[seed] {
                    continue;
                }
                let start = ends[seed].iter().position(|&node| incidence[node].0 != 2);
                if !cycles && start.is_none() {
                    continue;
                }
                let mut from = ends[seed][start.unwrap_or(0)];
                let mut edge = seed;
                let mut walk = Vec::new();
                loop {
                    used[edge] = true;
                    let reversed = ends[edge][0] != from;
                    walk.push((edge, reversed));
                    let far = ends[edge][usize::from(!reversed)];
                    let (degree, adjacent) = incidence[far];
                    if degree != 2 {
                        break;
                    }
                    let next = adjacent[usize::from(adjacent[0] == edge)];
                    if used[next] {
                        break;
                    }
                    edge = next;
                    from = far;
                }
                walks.push(walk);
            }
        }
        walks
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ring owners and line-walk indices are produced from this same boundary plan; coordinate columns are emitted in walk order"
    )]
    pub(super) fn emit_xy_parts(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edge_of_slot: &[u32],
        operation: &'static str,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        let groups = self.polygon_walks(partition, topology)?;
        let line_walks = self.line_walks(topology);
        let emission = self.emission(topology, edge_of_slot);
        let emissions = [&emission];
        let embedding = partition.embed(&emissions, operation)?;
        let coordinates = embedding.coordinates();
        let mut remaining = emission.items.as_slice();
        let mut rings = Vec::with_capacity(self.walks.len());
        for walk in &self.walks {
            let (chords, rest) = remaining.split_at(walk.len());
            remaining = rest;
            rings.push(Some(closed_ring(
                chords
                    .iter()
                    .map(|chord| Embedding::xy_at(coordinates, chord[0])),
            )?));
        }
        let mut polygons = Vec::with_capacity(groups.len());
        for group in groups {
            let mut indices = group.into_iter();
            let shell = rings[indices.next().expect("polygon has a shell")]
                .take()
                .expect("each ring has one polygon owner");
            let mut holes = Vec::with_capacity(indices.len());
            for index in indices {
                holes.push(
                    rings[index]
                        .take()
                        .expect("each ring has one polygon owner"),
                );
            }
            polygons.push(Polygon::new(shell, holes));
        }
        drop(rings);
        let (chords, isolated) = remaining.split_at(self.lines.len());
        let mut lines = Vec::with_capacity(line_walks.len());
        for walk in line_walks {
            let mut xs = Vec::new();
            let mut ys = Vec::new();
            let coordinate_count = walk.len().wrapping_add(1);
            xs.reserve(coordinate_count);
            ys.reserve(coordinate_count);
            for (index, reversed) in walk {
                let [start_node, end_node] = chords[index];
                let (start, end) = if reversed {
                    (
                        Embedding::xy_at(coordinates, end_node),
                        Embedding::xy_at(coordinates, start_node),
                    )
                } else {
                    (
                        Embedding::xy_at(coordinates, start_node),
                        Embedding::xy_at(coordinates, end_node),
                    )
                };
                if xs.is_empty() {
                    xs.push(start.x);
                    ys.push(start.y);
                }
                xs.push(end.x);
                ys.push(end.y);
            }
            if xs.len() > 2
                && let (Some(first), Some(last)) =
                    (xs.first().zip(ys.first()), xs.last().zip(ys.last()))
                && same_point(XY::new(*first.0, *first.1), XY::new(*last.0, *last.1))
            {
                canonicalize_closed_xy(&mut xs, &mut ys);
                let open = xs.len().wrapping_sub(1);
                let next = (xs[1], ys[1]);
                let previous = (xs[open.wrapping_sub(1)], ys[open.wrapping_sub(1)]);
                if next > previous {
                    xs[1..open].reverse();
                    ys[1..open].reverse();
                    xs[open] = xs[0];
                    ys[open] = ys[0];
                }
            }
            if let (Some(first), Some(last)) =
                (xs.first().zip(ys.first()), xs.last().zip(ys.last()))
                && (last.0, last.1) < (first.0, first.1)
            {
                xs.reverse();
                ys.reverse();
            }
            lines.push(CoordSeq::try_from_vecs(xs, ys, None, None)?);
        }
        let mut points = Vec::with_capacity(isolated.len());
        for item in isolated {
            let point = Embedding::xy_at(coordinates, item[0]);
            points.push(Point::new_raw_xy(point.x, point.y));
        }
        Ok((points, lines, polygons))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "slot and span IDs are produced by this exact partition and its topology"
    )]
    pub(super) fn emit_xy_polygon_parts_with_source_roles(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edge_of_slot: &[u32],
        operation: &'static str,
        preserved_spans: &[bool],
        source_locations: &[OverlaySourceLocation],
        source_segments: &[crate::geometry::Segment],
    ) -> Result<Vec<OverlayPolygonPartWithSources>> {
        if preserved_spans.len() != partition.spans.len() {
            return Err(GeometryErrorKind::Invalid(
                "source edge roles do not match the exact partition".into(),
            )
            .into());
        }
        let groups = self.polygon_walks(partition, topology)?;
        let emission = self.emission(topology, edge_of_slot);
        let embedding = partition.embed(&[&emission], operation)?;
        let coordinates = embedding.coordinates();
        let mut remaining = emission.items.as_slice();
        let mut rings: Vec<
            Option<(
                Ring,
                Vec<Vec<OverlaySourceEdgeUse>>,
                Vec<OverlayNodeReceipt>,
            )>,
        > = Vec::with_capacity(self.walks.len());
        rings.resize_with(self.walks.len(), || None);
        for (ring_index, walk) in self.walks.iter().enumerate() {
            let (chords, rest) = remaining.split_at(walk.len());
            remaining = rest;
            let mut nodes: Vec<_> = chords.iter().map(|chord| chord[0]).collect();
            let mut points: Vec<_> = nodes
                .iter()
                .map(|&node| Embedding::xy_at(coordinates, node))
                .collect();
            let mut edge_sources = Vec::with_capacity(walk.len());
            for chord in walk {
                edge_sources.push(self.chord_source_uses(
                    partition,
                    topology,
                    edge_of_slot,
                    chord,
                    source_locations,
                    source_segments,
                )?);
            }
            let start = points
                .iter()
                .enumerate()
                .min_by(|(_, left), (_, right)| {
                    left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
                })
                .map_or(0, |(index, _)| index);
            points.rotate_left(start);
            nodes.rotate_left(start);
            edge_sources.rotate_left(start);
            let vertices = nodes
                .iter()
                .map(|&node| {
                    let point = partition.exact(node)?.into_owned();
                    Ok(OverlayNodeReceipt {
                        id: node.0,
                        exact_point: ExactPoint::from_homogeneous_dyadics(point),
                        assignment: Embedding::xy_at(coordinates, node),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let ring = closed_ring(points.into_iter())?;
            rings[ring_index] = Some((ring, edge_sources, vertices));
        }
        let mut polygons = Vec::with_capacity(groups.len());
        for group in groups {
            let mut indices = group.into_iter();
            let (shell, shell_sources, shell_vertices) = rings
                [indices.next().expect("polygon has a shell")]
            .take()
            .expect("each ring has one polygon owner");
            let mut holes_with_roles = Vec::with_capacity(indices.len());
            let mut edge_sources = Vec::with_capacity(indices.len().wrapping_add(1));
            let mut edge_vertices = Vec::with_capacity(indices.len().wrapping_add(1));
            edge_sources.push(shell_sources);
            edge_vertices.push(shell_vertices);
            for index in indices {
                let (hole, source_uses, vertices) = rings[index]
                    .take()
                    .expect("each ring has one polygon owner");
                holes_with_roles.push((hole, source_uses, vertices));
            }
            holes_with_roles.sort_unstable_by(|(left, ..), (right, ..)| {
                crate::geometry::compare_point_slices(left.coords(), right.coords())
            });
            let mut holes = Vec::with_capacity(holes_with_roles.len());
            for (hole, source_uses, vertices) in holes_with_roles {
                holes.push(hole);
                edge_sources.push(source_uses);
                edge_vertices.push(vertices);
            }
            polygons.push(OverlayPolygonPartWithSources {
                polygon: Polygon::new(shell, holes),
                edge_sources,
                vertices: edge_vertices,
            });
        }
        Ok(polygons)
    }

    pub(super) fn polygon_edge_roles(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        preserved_spans: &[bool],
    ) -> Result<Vec<Vec<Vec<bool>>>> {
        if preserved_spans.len() != partition.spans.len() {
            return Err(GeometryErrorKind::Invalid(
                "source edge roles do not match partition spans".into(),
            )
            .into());
        }
        let groups = self.polygon_walks(partition, topology)?;
        let mut ring_roles = Vec::with_capacity(self.walks.len());
        for walk in &self.walks {
            let mut roles = Vec::with_capacity(walk.len());
            let mut start_nodes = Vec::with_capacity(walk.len());
            for chord in walk {
                let preserved = self.chord_is_preserved(chord, edges, preserved_spans)?;
                roles.push(!preserved);
                let first_slot = self
                    .source_darts
                    .get(chord.sources.start)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("source role ring has no first dart".into())
                    })? as usize;
                let first_node = topology.owners.get(first_slot).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("source role ring node is unknown".into())
                })?;
                start_nodes.push(NodeId(first_node));
            }
            if start_nodes.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "source role ring has no boundary chords".into(),
                )
                .into());
            }
            let mut rotation = 0;
            for (index, &node) in start_nodes.iter().enumerate().skip(1) {
                let current = *start_nodes.get(rotation).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source role ring rotation is unknown".into())
                })?;
                if partition.compare_nodes(node, current)? == Ordering::Less {
                    rotation = index;
                }
            }
            roles.rotate_left(rotation);
            ring_roles.push(roles);
        }
        let mut polygon_roles = Vec::with_capacity(groups.len());
        for group in groups {
            let mut rings = Vec::with_capacity(group.len());
            for ring in group {
                rings.push(
                    ring_roles
                        .get(ring)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "source role plan references an unknown boundary ring".into(),
                            )
                        })?
                        .clone(),
                );
            }
            polygon_roles.push(rings);
        }
        Ok(polygon_roles)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "planned source ranges, walk owners, and node chords are IDs from this boundary plan and topology"
    )]
    pub(super) fn build_node_parts(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        emission: &Emission,
    ) -> Result<NodeParts> {
        let groups = self.polygon_walks(partition, topology)?;
        let mut remaining = emission.items.as_slice();
        let mut rings: Vec<Option<Vec<NodeId>>> = Vec::with_capacity(self.walks.len());
        rings.resize_with(self.walks.len(), || None);
        for (index, walk) in self.walks.iter().enumerate() {
            let (chords, rest) = remaining.split_at(walk.len());
            remaining = rest;
            rings[index] = Some(Self::ring_node_walk(partition, chords)?);
        }
        let mut polygons = Vec::with_capacity(groups.len());
        for group in groups {
            let mut indices = group.into_iter();
            let shell = rings[indices.next().expect("polygon has a shell")]
                .take()
                .expect("each ring has one polygon owner");
            let mut polygon_rings = Vec::new();
            let ring_capacity = indices.len().wrapping_add(1);
            polygon_rings.reserve(ring_capacity);
            polygon_rings.push(shell);
            for index in indices {
                polygon_rings.push(
                    rings[index]
                        .take()
                        .expect("each ring has one polygon owner"),
                );
            }
            polygons.push(polygon_rings);
        }
        drop(rings);

        let (chords, isolated) = remaining.split_at(self.lines.len());
        let line_walks = self.line_walks(topology);
        let mut lines = Vec::with_capacity(line_walks.len());
        for walk in line_walks {
            let mut nodes = Vec::new();
            let node_capacity = walk.len().wrapping_add(1);
            nodes.reserve(node_capacity);
            for (index, reversed) in walk {
                let [start, end] = chords[index];
                let (start, end) = if reversed { (end, start) } else { (start, end) };
                if nodes.is_empty() {
                    nodes.push(start);
                }
                nodes.push(end);
            }
            if nodes.len() > 2 && nodes.first() == nodes.last() {
                canonicalize_closed_nodes(partition, &mut nodes)?;
                let open = nodes.len().wrapping_sub(1);
                if partition.compare_nodes(nodes[1], nodes[open.wrapping_sub(1)])?
                    == Ordering::Greater
                {
                    nodes[1..open].reverse();
                    nodes[open] = nodes[0];
                }
            }
            if let (Some(&first), Some(&last)) = (nodes.first(), nodes.last())
                && partition.compare_nodes(last, first)? == Ordering::Less
            {
                nodes.reverse();
            }
            lines.push(nodes);
        }
        let mut points = Vec::with_capacity(isolated.len());
        points.extend(isolated.iter().map(|item| item[0]));
        Ok(NodeParts {
            points,
            lines,
            polygons,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "boundary walks are nonempty cycles whose source darts and face hosts come from this plan"
    )]
    pub(super) fn polygon_walks(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<Vec<Vec<usize>>> {
        let mut shells = Vec::with_capacity(self.face_quotient.len());
        shells.resize(self.face_quotient.len(), None);
        let mut holes = Vec::new();
        let mut polygons = Vec::new();
        for (index, walk) in self.walks.iter().enumerate() {
            let is_shell = walk_is_shell(
                partition,
                topology,
                walk.len(),
                |chord| self.source_darts[walk[chord].sources.start],
                |chord| self.source_darts[walk[chord].sources.end.wrapping_sub(1)],
            )?;
            // Each bounded occupied face has one outer cycle; its other
            // cycles bound holes, even when they touch that outer cycle.
            let host = walk[0].sides[0] as usize;
            debug_assert!(walk.iter().all(|chord| chord.sides[0] as usize == host));
            if is_shell {
                debug_assert!(shells[host].is_none());
                shells[host] = Some(polygons.len());
                let rings = vec![index];
                polygons.push(rings);
            } else {
                holes.push((host, index));
            }
        }
        for (host, hole) in holes {
            let rings = &mut polygons[shells[host].expect("bounded occupied face has a shell")];
            rings.push(hole);
        }
        Ok(polygons)
    }

    fn ring_node_walk(
        partition: &ExactPartition<'_>,
        chords: &[[NodeId; 2]],
    ) -> Result<Vec<NodeId>> {
        let mut nodes = Vec::with_capacity(chords.len().wrapping_add(1));
        nodes.extend(chords.iter().map(|chord| chord[0]));
        if let Some(&first) = nodes.first() {
            nodes.push(first);
            canonicalize_closed_nodes(partition, &mut nodes)?;
        }
        Ok(nodes)
    }

    pub(super) fn emit_boundary_node_parts(
        &self,
        partition: &ExactPartition<'_>,
        emission: &Emission,
    ) -> Result<NodeParts> {
        let mut remaining = emission.items.as_slice();
        let mut lines = Vec::with_capacity(self.walks.len());
        for walk in &self.walks {
            let (chords, rest) = remaining.split_at(walk.len());
            remaining = rest;
            lines.push(Self::ring_node_walk(partition, chords)?);
        }
        Ok(NodeParts {
            points: self.points.iter().map(|&(node, _)| node).collect(),
            lines,
            polygons: Vec::new(),
        })
    }
}

/// Every bounded face of the arrangement as its own polygon, in face order.
/// One plan that emits every span as its own chord certifies the rounded
/// arrangement once; each face's rings then read those certified node
/// coordinates instead of re-rounding the edges it shares with neighbors.
/// Dangling edges inside a face bound nothing and are neither certified nor
/// emitted.
pub(super) fn polygonize_emission(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
) -> Result<Emission> {
    let spans = polygonize_boundary_spans(partition, topology, edges, global);
    polygonize_emission_with_spans(partition, topology, edges, global, spans)
}

#[expect(
    clippy::indexing_slicing,
    reason = "polygonization slot and face IDs belong to this checked topology and exact partition"
)]
fn polygonize_boundary_spans(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
) -> Vec<bool> {
    let flags = |len: usize| vec![false; len];
    let mut spans = flags(partition.spans.len());
    for (slot, &span) in edges.iter().enumerate() {
        let face = global.walk_faces[topology.face_of[slot] as usize];
        let other = global.walk_faces[topology.face_of[topology.twins[slot] as usize] as usize];
        spans[span as usize] |= face != other;
    }
    spans
}

fn polygonize_emission_with_spans(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
    spans: Vec<bool>,
) -> Result<Emission> {
    let flags = |len: usize| vec![false; len];
    let lines = ArealCells {
        faces: flags(global.count as usize),
        spans,
        nodes: flags(partition.node_count()),
    };
    let plan = lines.emission_plan(partition, topology, edges, global)?;
    Ok(plan.polygonize_emission(topology, edges))
}

#[expect(
    clippy::indexing_slicing,
    reason = "diagnostic spans, side labels, and endpoint slots belong to this checked partition and topology"
)]
pub(super) fn polygonize_full_emission(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
    diagnostic_spans: &[bool],
) -> Result<Emission> {
    debug_assert_eq!(diagnostic_spans.len(), partition.spans.len());
    let boundary_spans = polygonize_boundary_spans(partition, topology, edges, global);
    let mut extra_spans = Vec::with_capacity(partition.spans.len());
    extra_spans.extend(
        diagnostic_spans
            .iter()
            .zip(&boundary_spans)
            .map(|(&diagnostic, &boundary)| diagnostic && !boundary),
    );
    let mut emission =
        polygonize_emission_with_spans(partition, topology, edges, global, boundary_spans)?;
    let area_chords = emission.chords;
    if let Some(faces) = &mut emission.faces {
        let mut side_labels = vec![[0; 2]; partition.spans.len()];
        for (slot, &span) in edges.iter().enumerate() {
            let span = span as usize;
            if topology.owners[slot] != partition.spans[span].start.0 {
                continue;
            }
            let twin = topology.twins[slot] as usize;
            side_labels[span] = [
                global.walk_faces[topology.face_of[slot] as usize],
                global.walk_faces[topology.face_of[twin] as usize],
            ];
        }
        for (span, &diagnostic) in extra_spans.iter().enumerate() {
            if !diagnostic {
                continue;
            }
            let endpoints = partition.spans[span];
            emission.items.push([endpoints.start, endpoints.end]);
            emission.fan_spans.push([span as u32, span as u32]);
            faces.push(side_labels[span]);
            emission.chords = emission.chords.wrapping_add(1);
        }
    } else {
        for (span, &diagnostic) in extra_spans.iter().enumerate() {
            if !diagnostic {
                continue;
            }
            let endpoints = partition.spans[span];
            emission.items.push([endpoints.start, endpoints.end]);
            emission.chords = emission.chords.wrapping_add(1);
        }
    }
    debug_assert_eq!(emission.areal_chords, area_chords);
    debug_assert_eq!(emission.items.len(), emission.chords);
    if let Some(faces) = &emission.faces {
        debug_assert_eq!(emission.fan_spans.len(), emission.chords);
        debug_assert_eq!(faces.len(), emission.chords);
    } else {
        debug_assert_eq!(emission.areal_chords, 0);
        debug_assert!(emission.fan_spans.is_empty());
    }
    Ok(emission)
}

#[expect(
    clippy::indexing_slicing,
    reason = "boundary darts and node IDs come from the same checked topology and embedding"
)]
pub(super) fn polygonize_node_groups(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    global: &GlobalFaces,
    embedding: &Embedding<'_>,
) -> Result<Vec<Vec<Vec<NodeId>>>> {
    let face_of = |slot: usize| global.walk_faces[topology.face_of[slot] as usize];
    let walks = global.boundary_walks(topology, |face, other| face != 0 && face != other);
    let mut shells = Vec::with_capacity(global.count as usize);
    shells.resize(global.count as usize, None);
    let mut polygons: Vec<(usize, Vec<Vec<NodeId>>)> = Vec::new();
    let mut holes = Vec::new();
    for walk in &walks {
        let first = *walk
            .first()
            .expect("a polygon boundary walk is a nonempty closed cycle");
        let face = face_of(first as usize) as usize;
        let is_shell = walk_is_shell(
            partition,
            topology,
            walk.len(),
            |step| walk[step],
            |step| walk[step],
        )?;
        let mut ring = walk
            .iter()
            .map(|&slot| NodeId(topology.owners[slot as usize]))
            .collect::<Vec<_>>();
        canonicalize_open_nodes_by_xy(embedding.coordinates(), &mut ring);
        if is_shell {
            debug_assert!(shells[face].is_none());
            shells[face] = Some(polygons.len());
            polygons.push((face, vec![ring]));
        } else {
            holes.push((face, ring));
        }
    }
    for (face, hole) in holes {
        polygons[shells[face].expect("bounded face has a shell")]
            .1
            .push(hole);
    }
    polygons.sort_unstable_by_key(|(face, _)| *face);
    Ok(polygons.into_iter().map(|(_, rings)| rings).collect())
}

fn canonicalize_open_nodes_by_xy(coordinates: &[XY], nodes: &mut [NodeId]) {
    let Some((&first, remaining)) = nodes.split_first() else {
        return;
    };
    let mut minimum = Embedding::xy_at(coordinates, first);
    let mut rotation = 0;
    for (index, &node) in remaining.iter().enumerate() {
        let point = Embedding::xy_at(coordinates, node);
        if point
            .x
            .total_cmp(&minimum.x)
            .then_with(|| point.y.total_cmp(&minimum.y))
            == Ordering::Less
        {
            minimum = point;
            rotation = index.wrapping_add(1);
        }
    }
    nodes.rotate_left(rotation);
}

/// Whether a closed boundary walk is the outer cycle of the face on its left,
/// judged at its exact lexicographically least vertex. `first(i)` and
/// `last(i)` are the first and last darts of the walk's `i`-th step.
pub(super) fn walk_is_shell(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    steps: usize,
    first: impl Fn(usize) -> u32,
    last: impl Fn(usize) -> u32,
) -> Result<bool> {
    let mut work = Unmetered;
    walk_is_shell_with_work(&partition.points, topology, steps, first, last, &mut work)
}

#[expect(
    clippy::indexing_slicing,
    reason = "walk callbacks return darts from the same nonempty closed walk in the checked topology"
)]
pub(in crate::geometry) fn walk_is_shell_with_work<R: RayPoints + ?Sized, W: Work + ?Sized>(
    points: &R,
    topology: &IndexedTopology,
    steps: usize,
    first: impl Fn(usize) -> u32,
    last: impl Fn(usize) -> u32,
    work: &mut W,
) -> Result<bool> {
    let mut minimum = 0;
    for candidate in 1..steps {
        work.charge(1)?;
        let candidate_node = topology.owners[first(candidate) as usize];
        let minimum_node = topology.owners[first(minimum) as usize];
        work.charge(1)?;
        let x_order = points.compare_axis(candidate_node, minimum_node, Axis::X)?;
        let order = if x_order.is_eq() {
            work.charge(1)?;
            points.compare_axis(candidate_node, minimum_node, Axis::Y)?
        } else {
            x_order
        };
        if order == Ordering::Less {
            minimum = candidate;
        }
    }
    let outgoing = first(minimum);
    let prior = if minimum == 0 {
        steps.wrapping_sub(1)
    } else {
        minimum.wrapping_sub(1)
    };
    let incoming = topology.twins[last(prior) as usize];
    let root = topology.owners[outgoing as usize];
    let mut upper = |slot: u32| -> Result<bool> {
        work.charge(1)?;
        Ok(points.compare_axis(topology.targets[slot as usize], root, Axis::Y)? != Ordering::Less)
    };
    let outgoing_upper = upper(outgoing)?;
    let incoming_upper = upper(incoming)?;
    // At the exact leftmost vertex both rays point right. The fan's angular
    // cut is +x, so a lower outgoing ray precedes an upper one.
    Ok(if outgoing_upper == incoming_upper {
        outgoing < incoming
    } else {
        !outgoing_upper
    })
}

pub(super) fn canonicalize_closed_nodes(
    partition: &ExactPartition<'_>,
    nodes: &mut [NodeId],
) -> Result<()> {
    let Some((closing, open)) = nodes.split_last_mut() else {
        return Ok(());
    };
    let Some(&first) = open.first() else {
        return Ok(());
    };
    let (mut start, mut minimum) = (0, first);
    for (index, &node) in open.iter().enumerate().skip(1) {
        if partition.compare_nodes(node, minimum)? == Ordering::Less {
            start = index;
            minimum = node;
        }
    }
    open.rotate_left(start);
    if let Some(&first) = open.first() {
        *closing = first;
    }
    Ok(())
}

/// A closed ring through `vertices`, rotated to start at its least vertex.
pub(super) fn closed_ring(vertices: impl ExactSizeIterator<Item = XY>) -> Result<Ring> {
    let count = vertices.len().wrapping_add(1);
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    xs.reserve(count);
    ys.reserve(count);
    let mut vertices = vertices;
    let first = vertices.next().expect("a ring has vertices");
    xs.push(first.x);
    ys.push(first.y);
    for vertex in vertices {
        xs.push(vertex.x);
        ys.push(vertex.y);
    }
    xs.push(first.x);
    ys.push(first.y);
    canonicalize_closed_xy(&mut xs, &mut ys);
    Ring::try_from_closed(CoordSeq::try_from_vecs(xs, ys, None, None)?)
}

pub(super) fn canonicalize_closed_xy(xs: &mut [f64], ys: &mut [f64]) {
    let Some((closing_x, open_x)) = xs.split_last_mut() else {
        return;
    };
    let Some((closing_y, open_y)) = ys.split_last_mut() else {
        return;
    };
    let (Some(&first_x), Some(&first_y)) = (open_x.first(), open_y.first()) else {
        return;
    };
    let mut minimum = (first_x, first_y);
    let mut start = 0;
    for (index, (&x, &y)) in open_x.iter().zip(open_y.iter()).enumerate().skip(1) {
        if x.total_cmp(&minimum.0)
            .then_with(|| y.total_cmp(&minimum.1))
            == Ordering::Less
        {
            minimum = (x, y);
            start = index;
        }
    }
    open_x.rotate_left(start);
    open_y.rotate_left(start);
    if let (Some(&first_x), Some(&first_y)) = (open_x.first(), open_y.first()) {
        *closing_x = first_x;
        *closing_y = first_y;
    }
}

pub(in crate::geometry) fn emitted_pair_preserves_incidence(
    left: [(NodeId, XY); 2],
    right: [(NodeId, XY); 2],
) -> bool {
    if left[0].0 != left[1].0
        && left
            .iter()
            .all(|&(node, _)| right.iter().any(|&(other, _)| node == other))
    {
        return false;
    }
    for (endpoints, other) in [(left, right), (right, left)] {
        for (node, point) in endpoints {
            if point_on_segment(point, other[0].1, other[1].1)
                && !other.iter().any(|&(other_node, _)| node == other_node)
            {
                return false;
            }
        }
    }
    let signs = |endpoints: [(NodeId, XY); 2], other: [(NodeId, XY); 2]| {
        other.map(|(_, point)| {
            stored_orientation_sign(
                endpoints[0].1.x,
                endpoints[0].1.y,
                endpoints[1].1.x,
                endpoints[1].1.y,
                point.x,
                point.y,
            )
        })
    };
    let straddles = |pair: [ExactSign; 2]| {
        pair[0] != ExactSign::Zero && pair[1] != ExactSign::Zero && pair[0] != pair[1]
    };
    !(straddles(signs(left, right)) && straddles(signs(right, left)))
}

impl GlobalFaces {
    #[cfg(test)]
    pub(super) fn boundary_plan(
        &self,
        lines: &[u32],
        topology: &IndexedTopology,
        edges: &[u32],
        partition: &ExactPartition<'_>,
        occupied: &[bool],
    ) -> Result<BoundaryPlan> {
        self.boundary_plan_with_protected(lines, topology, edges, partition, occupied, None, None)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span, dart, node, and face IDs come from the paired partition and topology; extra protection is length-checked at entry"
    )]
    pub(super) fn boundary_plan_with_protected(
        &self,
        lines: &[u32],
        topology: &IndexedTopology,
        edges: &[u32],
        partition: &ExactPartition<'_>,
        occupied: &[bool],
        extra_protected: Option<&[bool]>,
        preserved_spans: Option<&[bool]>,
    ) -> Result<BoundaryPlan> {
        if let Some(extra_protected) = extra_protected
            && extra_protected.len() != partition.node_count()
        {
            return Err(GeometryErrorKind::Invalid(
                "exact overlay protection does not match node count".into(),
            )
            .into());
        }
        if let Some(preserved_spans) = preserved_spans
            && preserved_spans.len() != partition.spans.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "source edge roles do not match partition spans".into(),
            )
            .into());
        }
        let mut walks =
            self.boundary_walks(topology, |face, other| occupied[face] && !occupied[other]);
        let mut retained = Vec::with_capacity(partition.spans.len());
        retained.resize(partition.spans.len(), false);
        for &span in lines {
            retained[span as usize] = true;
        }
        for walk in &walks {
            for &slot in walk {
                retained[edges[slot as usize] as usize] = true;
            }
        }
        let face_quotient = self.quotient(topology, edges, &retained);
        let mut protected = Vec::with_capacity(partition.node_count());
        protected.resize(partition.node_count(), false);
        match &partition.kept_nodes {
            KeptNodes::Selected => {},
            KeptNodes::All => protected.fill(true),
            KeptNodes::OffRings(on_ring) => {
                for (protect, &on_ring) in protected.iter_mut().zip(on_ring) {
                    *protect = !on_ring;
                }
            },
        }
        if let Some(extra_protected) = extra_protected {
            for (protected, &extra) in protected.iter_mut().zip(extra_protected) {
                *protected |= extra;
            }
        }
        if let Some(preserved_spans) = preserved_spans {
            for walk in &walks {
                let Some((&first, remaining)) = walk.split_first() else {
                    continue;
                };
                let mut previous = first;
                for &current in remaining.iter().chain(std::iter::once(&first)) {
                    let previous_span = edges[previous as usize] as usize;
                    let current_span = edges[current as usize] as usize;
                    if preserved_spans[previous_span] != preserved_spans[current_span] {
                        protected[topology.targets[previous as usize] as usize] = true;
                    }
                    previous = current;
                }
            }
        }
        for &node in &partition.point_nodes {
            protected[node.0 as usize] = true;
        }
        for &(node, _) in &partition.point_sources {
            protected[node.0 as usize] = true;
        }
        let sides = |slot: u32| {
            [slot, topology.twins[slot as usize]].map(|dart| {
                face_quotient[self.walk_faces[topology.face_of[dart as usize] as usize] as usize]
            })
        };
        let can_join_after_side_match = |node: usize, left: u32, right: u32| -> bool {
            let row =
                topology.starts[node] as usize..topology.starts[node.wrapping_add(1)] as usize;
            // A two-slot row contains only the incoming twin and its distinct
            // retained departure; there is no omitted branch in this row.
            if row.len() != 2 && row.filter(|&slot| retained[edges[slot] as usize]).count() != 2 {
                return false;
            }
            let source = |slot: u32| {
                partition.unique[partition.spans[edges[slot as usize] as usize].unique as usize]
            };
            let a = source(left);
            let b = source(right);
            stored_cross_sign(a.start, a.end, b.start, b.end) == ExactSign::Zero
        };
        let can_join = |left: u32, right: u32| -> bool {
            let node = topology.owners[right as usize] as usize;
            if protected[node] || sides(left) != sides(right) {
                return false;
            }
            can_join_after_side_match(node, left, right)
        };
        // Every selected boundary dart and retained line source is appended
        // once; chord joins only change grouping. Production line spans are
        // unique, while duplicate test inputs merely over-reserve this hint.
        let source_dart_capacity = walks
            .iter()
            .fold(lines.len(), |count, walk| count.wrapping_add(walk.len()));
        let mut source_darts = Vec::with_capacity(source_dart_capacity);
        let mut make_chord = |slot: u32| -> Result<PlannedChord> {
            let start = source_darts.len();
            source_darts.push(slot);
            Ok(PlannedChord {
                sources: start..start.wrapping_add(1),
                sides: sides(slot),
            })
        };
        let mut planned_walks = Vec::with_capacity(walks.len());
        for walk in &mut walks {
            // Start at a retained corner so a joined source range never wraps.
            for index in 0..walk.len() {
                let prior = if index == 0 {
                    walk.len().wrapping_sub(1)
                } else {
                    index.wrapping_sub(1)
                };
                if !can_join(walk[prior], walk[index]) {
                    walk.rotate_left(index);
                    break;
                }
            }
            let mut chords = Vec::with_capacity(walk.len());
            for (index, &slot) in walk.iter().enumerate() {
                let chord = make_chord(slot)?;
                if index == 0 {
                    chords.push(chord);
                    continue;
                }
                let previous: &mut PlannedChord = chords
                    .last_mut()
                    .expect("every later dart follows a planned chord");
                let node = topology.owners[slot as usize] as usize;
                // Earlier joins extended this chord only after its labels matched.
                let joins = if protected[node] || previous.sides != chord.sides {
                    false
                } else {
                    can_join_after_side_match(node, walk[index.wrapping_sub(1)], slot)
                };
                if joins {
                    previous.sources.end = chord.sources.end;
                } else {
                    chords.push(chord);
                }
            }
            planned_walks.push(chords);
        }
        drop(walks);
        let mut planned_lines = Vec::with_capacity(lines.len());
        if !lines.is_empty() {
            let mut span_slots = vec![0; partition.spans.len()];
            for (slot, &span) in edges.iter().enumerate() {
                if topology.owners[slot] == partition.spans[span as usize].start.0 {
                    span_slots[span as usize] = slot;
                }
            }
            let mut line_remaining = Vec::with_capacity(partition.spans.len());
            line_remaining.resize(partition.spans.len(), false);
            for &span in lines {
                line_remaining[span as usize] = true;
            }
            let continuation = |slot: u32, remaining: &[bool]| -> Result<Option<u32>> {
                let node = topology.targets[slot as usize] as usize;
                let twin = topology.twins[slot as usize];
                for next in topology.starts[node]..topology.starts[node.wrapping_add(1)] {
                    if next != twin
                        && remaining[edges[next as usize] as usize]
                        && can_join(slot, next)
                    {
                        return Ok(Some(next));
                    }
                }
                Ok(None)
            };
            for &span in lines {
                if !line_remaining[span as usize] {
                    continue;
                }
                let mut backwards = topology.twins[span_slots[span as usize]];
                while let Some(next) = continuation(backwards, &line_remaining)? {
                    backwards = next;
                }
                let mut slot = topology.twins[backwards as usize];
                let mut chord = make_chord(slot)?;
                loop {
                    line_remaining[edges[slot as usize] as usize] = false;
                    let Some(next) = continuation(slot, &line_remaining)? else {
                        break;
                    };
                    chord.sources.end = make_chord(next)?.sources.end;
                    slot = next;
                }
                planned_lines.push(chord);
            }
        }
        Ok(BoundaryPlan {
            source_darts,
            walks: planned_walks,
            lines: planned_lines,
            points: Vec::new(),
            face_quotient,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the node, CSR, component, walk-face, and global-face IDs come from this paired topology"
    )]
    pub(super) fn node_host_face(&self, topology: &IndexedTopology, node: NodeId) -> u32 {
        let index = node.0 as usize;
        let first = topology.starts[index] as usize;
        if topology.starts[index] == topology.starts[index + 1] {
            self.component_hosts[topology.component_of[index] as usize]
        } else {
            self.walk_faces[topology.face_of[first] as usize]
        }
    }

    /// Closed boundary walks over the darts whose global face `bounds` its
    /// twin's face: `bounds(face, twin_face)`. Each walk stays within one
    /// face, since the next dart clockwise from an incoming twin bounds the
    /// same face on its left.
    pub(super) fn boundary_walks(
        &self,
        topology: &IndexedTopology,
        bounds: impl Fn(usize, usize) -> bool,
    ) -> Vec<Vec<u32>> {
        let mut work = Unmetered;
        self.boundary_walks_with_work(topology, bounds, &mut work)
            .expect("global faces and indexed topology are paired by their owner")
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "selected darts and successor rows are generated from this checked topology's CSR and global face map"
    )]
    pub(in crate::geometry) fn boundary_walks_with_work<W: Work + ?Sized>(
        &self,
        topology: &IndexedTopology,
        bounds: impl Fn(usize, usize) -> bool,
        work: &mut W,
    ) -> Result<Vec<Vec<u32>>> {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(u8)]
        enum DartState {
            Omitted,
            Pending,
            Walked,
        }

        let slots = topology.targets.len();
        let walk_count = topology.face_starts.len().checked_sub(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph face starts have no sentinel".into())
        })?;
        if topology.face_of.len() != slots
            || topology.owners.len() != slots
            || topology.twins.len() != slots
            || self.walk_faces.len() != walk_count
            || topology.starts.len()
                != topology
                    .component_of
                    .len()
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?
        {
            return Err(GeometryErrorKind::Invalid(
                "graph boundary inputs do not share one indexed topology".into(),
            )
            .into());
        }
        work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;

        let mut state = Vec::with_capacity(topology.targets.len());
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            work.charge(1)?;
            let other = topology.face_of[topology.twins[slot] as usize];
            state.push(
                if bounds(
                    self.walk_faces[walk as usize] as usize,
                    self.walk_faces[other as usize] as usize,
                ) {
                    DartState::Pending
                } else {
                    DartState::Omitted
                },
            );
        }
        work.charge(
            u64::try_from(topology.component_of.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut positions = Vec::with_capacity(topology.component_of.len());
        positions.resize(topology.component_of.len(), None);
        let mut walks = Vec::new();
        for start in 0..state.len() {
            work.charge(1)?;
            if state[start] != DartState::Pending {
                continue;
            }
            let mut walk = Vec::new();
            let mut slot = start;
            loop {
                work.charge(1)?;
                debug_assert_eq!(state[slot], DartState::Pending);
                state[slot] = DartState::Walked;
                positions[topology.owners[slot] as usize] = Some(walk.len());
                work.charge(1)?;
                walk.push(slot as u32);
                let target = topology.targets[slot] as usize;
                // A tangent hole can share a boundary walk with its shell.
                // Cut closed cycles by exact identity before any rounding.
                if let Some(begin) = positions[target] {
                    let cycle_len = walk.len().checked_sub(begin).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "graph boundary cycle starts after its current walk".into(),
                        )
                    })?;
                    work.charge(
                        u64::try_from(cycle_len).map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    let mut cycle = Vec::with_capacity(cycle_len);
                    for dart in walk.drain(begin..) {
                        work.charge(1)?;
                        positions[topology.owners[dart as usize] as usize] = None;
                        cycle.push(dart);
                    }
                    work.charge(1)?;
                    walks.push(cycle);
                }
                let first = topology.starts[target] as usize;
                let end = topology.starts[target.wrapping_add(1)] as usize;
                let twin = topology.twins[slot] as usize;
                // Skip omitted wedges clockwise from the incoming twin.
                slot = twin;
                loop {
                    work.charge(1)?;
                    slot = if slot == first {
                        end.wrapping_sub(1)
                    } else {
                        slot.wrapping_sub(1)
                    };
                    if state[slot] != DartState::Omitted {
                        break;
                    }
                    debug_assert_ne!(slot, twin, "selected boundary has no successor");
                }
                if slot == start {
                    break;
                }
            }
            debug_assert!(walk.is_empty());
        }
        Ok(walks)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "dart, span, and face IDs are paired from the same checked topology and retained-span table"
    )]
    pub(super) fn quotient(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        retained: &[bool],
    ) -> Vec<u32> {
        let mut parents = Vec::with_capacity(self.count as usize);
        parents.extend(0..self.count);
        for (slot, &edge) in edges.iter().enumerate() {
            let twin = topology.twins[slot] as usize;
            if slot > twin || retained[edge as usize] {
                continue;
            }
            let mut left = self.walk_faces[topology.face_of[slot] as usize];
            let mut right = self.walk_faces[topology.face_of[twin] as usize];
            while parents[left as usize] != left {
                parents[left as usize] = parents[parents[left as usize] as usize];
                left = parents[left as usize];
            }
            while parents[right as usize] != right {
                parents[right as usize] = parents[parents[right as usize] as usize];
                right = parents[right as usize];
            }
            // Keep infinity's identity when its face absorbs a bounded face.
            parents[left.max(right) as usize] = left.min(right);
        }
        for face in 0..parents.len() {
            parents[face] = parents[parents[face] as usize];
        }
        parents
    }
}
