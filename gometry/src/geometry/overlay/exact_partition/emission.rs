//! Planning and emitting the faces of an exact arrangement.
//!
//! Once the partition knows its global faces, emission is a separate job:
//! pick the cells a stored-ring winding selects, plan the chords that bound
//! them, and round each face to a ring. The checks that rounding preserved
//! incidence, cyclic order and face identity live with the plan they verify.

use std::cmp::Ordering;
use std::ops::Range;

use super::rays::{EmittedPoints, shoot_left};
use super::{
    ArealCells, Axis, ExactPartition, ExactSourceInventory, KeptNodes, NodeId, rings_occupied,
};
use crate::error::ResourceError;
use crate::geometry::arrangement::{IndexedTopology, compare_departures};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::{point_on_segment, stored_orientation_sign};
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::{
    CoordSeq, GeometryErrorKind, Point, Polygon, Result, Ring, Segment, XY, same_point,
};

pub(super) struct GlobalFaces {
    pub(super) walk_faces: Vec<u32>,
    pub(super) component_hosts: Vec<u32>,
    pub(super) count: u32,
}

pub(super) struct CellFacts {
    pub(super) face_membership: Vec<bool>,
    pub(super) span_raw: Vec<bool>,
    pub(super) span_curve: Vec<bool>,
    pub(super) span_support: Vec<bool>,
    pub(super) span_faces: Vec<[u32; 2]>,
    pub(super) point_membership: Vec<bool>,
}

pub(super) struct PlannedChord {
    pub(super) sources: Range<usize>,
    pub(super) sides: [u32; 2],
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
    pub(super) rounded: Vec<(NodeId, XY)>,
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

    pub(super) fn update_ring(
        &mut self,
        inventory: &ExactSourceInventory,
        winding: &mut [i64],
        ring: u32,
        delta: i64,
    ) -> Result<()> {
        let ring_index = ring as usize;
        let before = winding[ring_index] != 0;
        winding[ring_index] += delta;
        let after = winding[ring_index] != 0;
        if before == after {
            return Ok(());
        }
        let polygon = *inventory.ring_polygons.get(ring_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact coverage winding references an unknown ring".into())
        })? as usize;
        let occupied = rings_occupied(
            winding,
            inventory.polygon_ring_ranges[polygon].clone(),
            false,
        );
        if occupied == self.polygon_occupied[polygon] {
            return Ok(());
        }
        self.polygon_occupied[polygon] = occupied;
        let row = inventory.polygon_operands[polygon] as usize;
        if occupied {
            let count = self.row_polygon_counts.get_mut(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact coverage polygon operand is out of range".into())
            })?;
            let was_empty = *count == 0;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if was_empty {
                let position = self.active_rows.len();
                self.active_rows.push(row);
                self.active_positions[row] = position;
            }
        } else {
            let count = self.row_polygon_counts.get_mut(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact coverage polygon operand is out of range".into())
            })?;
            *count = count.checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact coverage polygon occupancy underflow".into())
            })?;
            if *count == 0 {
                let position = self.active_positions[row];
                let last = self.active_rows.pop().ok_or_else(|| {
                    GeometryErrorKind::Invalid("exact coverage active rows are out of sync".into())
                })?;
                if position != self.active_rows.len() {
                    self.active_rows[position] = last;
                    self.active_positions[last] = position;
                }
                self.active_positions[row] = usize::MAX;
            }
        }
        Ok(())
    }
}

impl BoundaryPlan {
    pub(super) fn line_walks(&self, topology: &IndexedTopology) -> Vec<Vec<(usize, bool)>> {
        if self.lines.is_empty() {
            return Vec::new();
        }
        let mut ends = Vec::with_capacity(self.lines.len());
        let mut incidence = Vec::with_capacity(topology.starts.len() - 1);
        incidence.resize(topology.starts.len() - 1, (0_usize, [0_usize; 2]));
        for (index, chord) in self.lines.iter().enumerate() {
            let pair = [
                topology.owners[self.source_darts[chord.sources.start] as usize] as usize,
                topology.targets[self.source_darts[chord.sources.end - 1] as usize] as usize,
            ];
            for node in pair {
                let (degree, edges) = &mut incidence[node];
                if *degree < 2 {
                    edges[*degree] = index;
                }
                *degree += 1;
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

    pub(super) fn emit_xy_parts(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        let groups = self.polygon_walks(partition, topology)?;
        let line_walks = self.line_walks(topology);
        let (_, segments) = self.round_items(partition, topology)?;
        let mut remaining = segments.as_slice();
        let mut rings = Vec::with_capacity(self.walks.len());
        for walk in &self.walks {
            let (chords, rest) = remaining.split_at(walk.len());
            remaining = rest;
            rings.push(Some(closed_ring(chords.iter().map(|chord| chord.start))?));
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
            let coordinate_count = walk
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            xs.reserve(coordinate_count);
            ys.reserve(coordinate_count);
            for (index, reversed) in walk {
                let chord = chords[index];
                let (start, end) = if reversed {
                    (chord.end, chord.start)
                } else {
                    (chord.start, chord.end)
                };
                if xs.is_empty() {
                    xs.push(start.x);
                    ys.push(start.y);
                }
                xs.push(end.x);
                ys.push(end.y);
            }
            if xs.len() > 2
                && same_point(
                    XY::new(xs[0], ys[0]),
                    XY::new(xs[xs.len() - 1], ys[ys.len() - 1]),
                )
            {
                canonicalize_closed_xy(&mut xs, &mut ys);
                let open = xs.len() - 1;
                let next = (xs[1], ys[1]);
                let previous = (xs[open - 1], ys[open - 1]);
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
        lines.sort_unstable_by(crate::geometry::compare_point_slices);
        let mut points = Vec::with_capacity(isolated.len());
        for item in isolated {
            points.push(Point::new_raw_xy(item.start.x, item.start.y));
        }
        Ok((points, lines, polygons))
    }

    pub(super) fn emit_node_parts(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<NodeParts> {
        let (items, rounded_segments) = self.round_items(partition, topology)?;
        self.emit_node_parts_from_segments(partition, topology, &items, &rounded_segments, true)
    }

    pub(super) fn emit_node_parts_with_coordinates(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        coordinates: &[Option<XY>],
    ) -> Result<NodeParts> {
        let items = self.incident_items(topology);
        let mut rounded_segments = Vec::with_capacity(items.len());
        for &[start, end] in &items {
            let start = coordinates
                .get(start.0 as usize)
                .and_then(Option::as_ref)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact subdivision output lost a rounded start node".into(),
                    )
                })?;
            let end = coordinates
                .get(end.0 as usize)
                .and_then(Option::as_ref)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact subdivision output lost a rounded end node".into(),
                    )
                })?;
            rounded_segments.push(Segment { start, end });
        }
        self.check_emitted_fans(topology, &rounded_segments)?;
        self.check_global_faces(partition, &items, &rounded_segments)?;
        self.emit_node_parts_from_segments(partition, topology, &items, &rounded_segments, true)
    }

    /// A row's parts from nodes rounded once for every row, whose
    /// injectivity the caller certified across the whole coverage.
    pub(super) fn emit_node_parts_with_shared_rounding(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<NodeParts> {
        let items = self.incident_items(topology);
        let mut rounded_segments = Vec::with_capacity(items.len());
        rounded_segments.extend(items.iter().map(|&[start, end]| Segment {
            start: partition.node_xy(start),
            end: partition.node_xy(end),
        }));
        if partition.rounding_moves(&items) {
            self.check_emitted_fans(topology, &rounded_segments)?;
            self.check_global_faces(partition, &items, &rounded_segments)?;
        }
        self.emit_node_parts_from_segments(partition, topology, &items, &rounded_segments, false)
    }

    pub(super) fn emit_node_parts_from_segments(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        items: &[[NodeId; 2]],
        rounded_segments: &[Segment],
        retain_rounded: bool,
    ) -> Result<NodeParts> {
        let mut rounded = Vec::new();
        if retain_rounded {
            let rounded_capacity = items
                .len()
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?;
            rounded.reserve(rounded_capacity);
            for (&[start, end], segment) in items.iter().zip(rounded_segments) {
                rounded.push((start, segment.start));
                rounded.push((end, segment.end));
            }
        }
        let node_chord = |chord: &PlannedChord| {
            let first = self.source_darts[chord.sources.start] as usize;
            let last = self.source_darts[chord.sources.end - 1] as usize;
            [
                NodeId(topology.owners[first]),
                NodeId(topology.targets[last]),
            ]
        };

        let groups = self.polygon_walks(partition, topology)?;
        let mut rings: Vec<Option<Vec<NodeId>>> = Vec::with_capacity(self.walks.len());
        rings.resize_with(self.walks.len(), || None);
        for (index, walk) in self.walks.iter().enumerate() {
            let mut nodes = Vec::new();
            let node_capacity = walk
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            nodes.reserve(node_capacity);
            for chord in walk {
                nodes.push(node_chord(chord)[0]);
            }
            if let Some(&first) = nodes.first() {
                nodes.push(first);
                canonicalize_closed_nodes(partition, &mut nodes)?;
            }
            rings[index] = Some(nodes);
        }
        let mut polygons = Vec::with_capacity(groups.len());
        for group in groups {
            let mut indices = group.into_iter();
            let shell = rings[indices.next().expect("polygon has a shell")]
                .take()
                .expect("each ring has one polygon owner");
            let mut polygon_rings = Vec::new();
            let ring_capacity = indices
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
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

        let line_walks = self.line_walks(topology);
        let mut lines = Vec::with_capacity(line_walks.len());
        for walk in line_walks {
            let mut nodes = Vec::new();
            let node_capacity = walk
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            nodes.reserve(node_capacity);
            for (index, reversed) in walk {
                let [start, end] = node_chord(&self.lines[index]);
                let (start, end) = if reversed { (end, start) } else { (start, end) };
                if nodes.is_empty() {
                    nodes.push(start);
                }
                nodes.push(end);
            }
            if nodes.len() > 2 && nodes.first() == nodes.last() {
                canonicalize_closed_nodes(partition, &mut nodes)?;
                let open = nodes.len() - 1;
                if partition.compare_nodes(nodes[1], nodes[open - 1])? == Ordering::Greater {
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
        let mut points = Vec::with_capacity(self.points.len());
        points.extend(self.points.iter().map(|&(node, _)| node));
        Ok(NodeParts {
            points,
            lines,
            polygons,
            rounded,
        })
    }

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
                |chord| self.source_darts[walk[chord].sources.end - 1],
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

    pub(super) fn check_emitted_fans(
        &self,
        topology: &IndexedTopology,
        segments: &[Segment],
    ) -> Result<()> {
        let mut darts = Vec::new();
        for chord in self.walks.iter().flatten().chain(&self.lines) {
            let first = self.source_darts[chord.sources.start];
            let last = self.source_darts[chord.sources.end - 1];
            darts.push([first, topology.twins[last as usize]]);
        }
        debug_assert_eq!(segments.len(), darts.len() + self.points.len());
        ExactPartition::check_emitted_fans(topology, &darts, &segments[..darts.len()])
    }

    pub(super) fn round_items(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<(Vec<[NodeId; 2]>, Vec<Segment>)> {
        let items = self.incident_items(topology);
        let segments = partition.round_incident_items(&items)?;
        if partition.rounding_moves(&items) {
            self.check_emitted_fans(topology, &segments)?;
            self.check_global_faces(partition, &items, &segments)?;
        }
        Ok((items, segments))
    }

    // Incidence and cyclic order fix each connected embedding up to its exterior.
    // Fixing that face and the hosts of disconnected components fixes the global faces.
    #[expect(
        clippy::float_cmp,
        reason = "exact rounded coordinates are ordered by their stored binary64 values to keep emitted topology deterministic"
    )]
    pub(super) fn check_global_faces(
        &self,
        partition: &ExactPartition<'_>,
        items: &[[NodeId; 2]],
        segments: &[Segment],
    ) -> Result<()> {
        let mut sides = Vec::with_capacity(items.len());
        for chord in self.walks.iter().flatten().chain(&self.lines) {
            sides.push(chord.sides);
        }
        for &(_, host) in &self.points {
            sides.push([host, host]);
        }
        debug_assert_eq!(sides.len(), items.len());
        debug_assert_eq!(segments.len(), items.len());
        let node_count = partition.node_count();
        // Every node at its emitted coordinate, as stored doubles.
        let mut coordinates = Vec::with_capacity(node_count);
        coordinates.extend((0..node_count as u32).map(|node| partition.node_xy(NodeId(node))));
        let mut incident_starts = vec![0_u32; node_count + 1];
        let mut parents = Vec::with_capacity(node_count);
        parents.extend(0..node_count as u32);
        for (&[start, end], segment) in items.iter().zip(segments) {
            coordinates[start.0 as usize] = segment.start;
            coordinates[end.0 as usize] = segment.end;
            incident_starts[start.0 as usize + 1] += 1;
            if end != start {
                incident_starts[end.0 as usize + 1] += 1;
            }
            let mut left = start.0;
            let mut right = end.0;
            while parents[left as usize] != left {
                parents[left as usize] = parents[parents[left as usize] as usize];
                left = parents[left as usize];
            }
            while parents[right as usize] != right {
                parents[right as usize] = parents[parents[right as usize] as usize];
                right = parents[right as usize];
            }
            parents[left.max(right) as usize] = left.min(right);
        }
        for node in 0..node_count {
            incident_starts[node + 1] += incident_starts[node];
        }
        let mut incident = vec![0_u32; incident_starts[node_count] as usize];
        {
            let mut cursor = Vec::with_capacity(node_count);
            cursor.extend_from_slice(&incident_starts[..node_count]);
            for (index, &[start, end]) in items.iter().enumerate() {
                for node in [start, end].into_iter().take(1 + usize::from(end != start)) {
                    incident[cursor[node.0 as usize] as usize] = index as u32;
                    cursor[node.0 as usize] += 1;
                }
            }
        }
        // Each component's root is its lexicographically least emitted node.
        let mut roots: Vec<Option<(usize, XY)>> = Vec::with_capacity(node_count);
        roots.resize(node_count, None);
        for node in 0..node_count {
            if incident_starts[node] == incident_starts[node + 1] {
                continue;
            }
            let mut root = node as u32;
            while parents[root as usize] != root {
                root = parents[root as usize];
            }
            let point = coordinates[node];
            let entry = &mut roots[root as usize];
            if entry.is_none_or(|(_, prior): (usize, XY)| {
                point.x < prior.x || (point.x == prior.x && point.y < prior.y)
            }) {
                *entry = Some((node, point));
            }
        }
        // The face each component's own plan puts around it: its root's
        // exterior wedge, or the host of an isolated point.
        drop(parents);
        let mut queries = Vec::new();
        let mut exteriors = Vec::new();
        for &(node, point) in roots.iter().flatten() {
            let mut exterior = None;
            let mut chosen = None;
            let mut has_upper = false;
            let range = incident_starts[node] as usize..incident_starts[node + 1] as usize;
            for &index in &incident[range] {
                let index = index as usize;
                let [start, end] = items[index];
                let (target, target_point, face) = if start.0 as usize == node {
                    (end.0, segments[index].end, sides[index][0])
                } else {
                    (start.0, segments[index].start, sides[index][1])
                };
                if target as usize == node {
                    exterior = Some(face);
                    continue;
                }
                let upper = target_point.y >= point.y;
                if chosen.is_none()
                    || (upper && !has_upper)
                    || (upper == has_upper && {
                        let (prior_target, prior_point) = chosen
                            .expect("a selected departure exists before comparing the next one");
                        compare_departures(point, prior_point, prior_target, target_point, target)
                            == Ordering::Less
                    })
                {
                    chosen = Some((target, target_point));
                    exterior = Some(face);
                    has_upper = upper;
                }
            }
            queries.push(node as u32);
            exteriors.push(exterior.expect("a component root has an incident emitted edge"));
        }
        drop(roots);
        drop(incident);
        drop(incident_starts);
        // The emitted embedding keeps the exact incidence and fans, so the
        // face just east of the first chord a leftward ray from a root meets
        // is the face holding that component.
        let hits = shoot_left(
            &EmittedPoints(&coordinates),
            items.len(),
            |index| [items[index][0].0, items[index][1].0],
            &queries,
        )?;
        for ((&node, &exterior), hit) in queries.iter().zip(&exteriors).zip(hits) {
            let host = hit.map_or(0, |chord| {
                let chord = chord as usize;
                let segment = segments[chord];
                if segment.start.y < segment.end.y {
                    sides[chord][1]
                } else {
                    sides[chord][0]
                }
            });
            if host != exterior {
                return Err(GeometryErrorKind::ExactGlobalFace { node }.into());
            }
        }
        Ok(())
    }

    pub(super) fn incident_items(&self, topology: &IndexedTopology) -> Vec<[NodeId; 2]> {
        let mut items = Vec::new();
        for chord in self.walks.iter().flatten().chain(&self.lines) {
            let first = self.source_darts[chord.sources.start] as usize;
            let last = self.source_darts[chord.sources.end - 1] as usize;
            items.push([
                NodeId(topology.owners[first]),
                NodeId(topology.targets[last]),
            ]);
        }
        items.reserve(self.points.len());
        for &(node, _) in &self.points {
            items.push([node, node]);
        }
        items
    }
}

/// Every bounded face of the arrangement as its own polygon, in face order.
/// One plan that emits every span as its own chord certifies the rounded
/// arrangement once; each face's rings then read those certified node
/// coordinates instead of re-rounding the edges it shares with neighbors.
/// Dangling edges inside a face bound nothing and are neither certified nor
/// emitted.
pub(super) fn polygonize_faces(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
) -> Result<Vec<Polygon>> {
    let face_of = |slot: usize| global.walk_faces[topology.face_of[slot] as usize];
    let flags = |len: usize| vec![false; len];
    let mut spans = flags(partition.spans.len());
    for (slot, &span) in edges.iter().enumerate() {
        spans[span as usize] |= face_of(slot) != face_of(topology.twins[slot] as usize);
    }
    let lines = ArealCells {
        faces: flags(global.count as usize),
        spans,
        nodes: flags(partition.node_count()),
    };
    lines
        .emission_plan(partition, topology, edges, global)?
        .round_items(partition, topology)?;
    let walks = global.boundary_walks(topology, |face, other| face != 0 && face != other);
    let mut shells = Vec::with_capacity(global.count as usize);
    shells.resize(global.count as usize, None);
    let mut polygons: Vec<(usize, Ring, Vec<Ring>)> = Vec::new();
    let mut holes = Vec::new();
    for walk in &walks {
        let face = face_of(walk[0] as usize) as usize;
        let ring = closed_ring(
            walk.iter()
                .map(|&slot| partition.node_xy(NodeId(topology.owners[slot as usize]))),
        )?;
        if walk_is_shell(
            partition,
            topology,
            walk.len(),
            |step| walk[step],
            |step| walk[step],
        )? {
            debug_assert!(shells[face].is_none());
            shells[face] = Some(polygons.len());
            polygons.push((face, ring, Vec::new()));
        } else {
            holes.push((face, ring));
        }
    }
    drop(walks);
    for (face, hole) in holes {
        let polygon = shells[face].expect("bounded face has a shell");
        polygons[polygon].2.push(hole);
    }
    drop(shells);
    polygons.sort_unstable_by_key(|&(face, ..)| face);
    let mut output = Vec::with_capacity(polygons.len());
    for (_, shell, holes) in polygons {
        output.push(Polygon::new(shell, holes));
    }
    Ok(output)
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
    let mut minimum = 0;
    for candidate in 1..steps {
        if partition.compare_nodes(
            NodeId(topology.owners[first(candidate) as usize]),
            NodeId(topology.owners[first(minimum) as usize]),
        )? == Ordering::Less
        {
            minimum = candidate;
        }
    }
    let outgoing = first(minimum);
    let prior = if minimum == 0 { steps - 1 } else { minimum - 1 };
    let incoming = topology.twins[last(prior) as usize];
    let root = NodeId(topology.owners[outgoing as usize]);
    let upper = |slot: u32| -> Result<bool> {
        Ok(
            partition.compare_axis(NodeId(topology.targets[slot as usize]), root, Axis::Y)?
                != Ordering::Less,
        )
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
    let open = nodes.len().saturating_sub(1);
    if open == 0 {
        return Ok(());
    }
    let mut start = 0;
    for index in 1..open {
        if partition.compare_nodes(nodes[index], nodes[start])? == Ordering::Less {
            start = index;
        }
    }
    nodes[..open].rotate_left(start);
    nodes[open] = nodes[0];
    Ok(())
}

/// A closed ring through `vertices`, rotated to start at its least vertex.
pub(super) fn closed_ring(vertices: impl ExactSizeIterator<Item = XY> + Clone) -> Result<Ring> {
    let count = vertices
        .len()
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    xs.reserve(count);
    ys.reserve(count);
    let first = vertices.clone().next().expect("a ring has vertices");
    for vertex in vertices.chain([first]) {
        xs.push(vertex.x);
        ys.push(vertex.y);
    }
    canonicalize_closed_xy(&mut xs, &mut ys);
    Ring::try_from_closed(CoordSeq::try_from_vecs(xs, ys, None, None)?)
}

pub(super) fn canonicalize_closed_xy(xs: &mut [f64], ys: &mut [f64]) {
    let open = xs.len().saturating_sub(1);
    if open == 0 {
        return;
    }
    let start = (0..open)
        .min_by(|&left, &right| {
            xs[left]
                .total_cmp(&xs[right])
                .then_with(|| ys[left].total_cmp(&ys[right]))
        })
        .unwrap_or(0);
    xs[..open].rotate_left(start);
    ys[..open].rotate_left(start);
    xs[open] = xs[0];
    ys[open] = ys[0];
}

pub(super) fn emitted_pair_preserves_incidence(
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
        self.boundary_plan_with_protected(lines, topology, edges, partition, occupied, None)
    }

    pub(super) fn boundary_plan_with_protected(
        &self,
        lines: &[u32],
        topology: &IndexedTopology,
        edges: &[u32],
        partition: &ExactPartition<'_>,
        occupied: &[bool],
        extra_protected: Option<&[bool]>,
    ) -> Result<BoundaryPlan> {
        if let Some(extra_protected) = extra_protected
            && extra_protected.len() != partition.node_count()
        {
            return Err(GeometryErrorKind::Invalid(
                "exact overlay protection does not match node count".into(),
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
        let can_join = |left: u32, right: u32| -> Result<bool> {
            let node = topology.owners[right as usize] as usize;
            if protected[node] || sides(left) != sides(right) {
                return Ok(false);
            }
            let row = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            if row.filter(|&slot| retained[edges[slot] as usize]).count() != 2 {
                return Ok(false);
            }
            let source = |slot: u32| {
                partition.unique[partition.spans[edges[slot as usize] as usize].unique as usize]
            };
            let a = source(left);
            let b = source(right);
            let difference =
                |end, start| ExactDyadic::from_f64(end).try_subtract(ExactDyadic::from_f64(start));
            let ax = difference(a.end.x, a.start.x)?;
            let ay = difference(a.end.y, a.start.y)?;
            let bx = difference(b.end.x, b.start.x)?;
            let by = difference(b.end.y, b.start.y)?;
            Ok(ax
                .try_product(&by)?
                .try_subtract(ay.try_product(&bx)?)?
                .is_zero())
        };
        let mut span_slots = vec![0; partition.spans.len()];
        for (slot, &span) in edges.iter().enumerate() {
            if topology.owners[slot] == partition.spans[span as usize].start.0 {
                span_slots[span as usize] = slot;
            }
        }
        let mut source_darts = Vec::new();
        let mut make_chord = |slot: u32| -> Result<PlannedChord> {
            let start = source_darts.len();
            source_darts.push(slot);
            Ok(PlannedChord {
                sources: start..start + 1,
                sides: sides(slot),
            })
        };
        let mut planned_walks = Vec::with_capacity(walks.len());
        for walk in &mut walks {
            // Start at a retained corner so a joined source range never wraps.
            for index in 0..walk.len() {
                let prior = if index == 0 {
                    walk.len() - 1
                } else {
                    index - 1
                };
                if !can_join(walk[prior], walk[index])? {
                    walk.rotate_left(index);
                    break;
                }
            }
            let mut chords = Vec::with_capacity(walk.len());
            for (index, &slot) in walk.iter().enumerate() {
                let chord = make_chord(slot)?;
                if index > 0 && can_join(walk[index - 1], slot)? {
                    let previous: &mut PlannedChord = chords
                        .last_mut()
                        .expect("a joined chord has a preceding planned chord");
                    previous.sources.end = chord.sources.end;
                } else {
                    chords.push(chord);
                }
            }
            planned_walks.push(chords);
        }
        drop(walks);
        let mut planned_lines = Vec::with_capacity(lines.len());
        let mut line_remaining = Vec::with_capacity(partition.spans.len());
        line_remaining.resize(partition.spans.len(), false);
        for &span in lines {
            line_remaining[span as usize] = true;
        }
        let continuation = |slot: u32, remaining: &[bool]| -> Result<Option<u32>> {
            let node = topology.targets[slot as usize] as usize;
            let twin = topology.twins[slot as usize];
            for next in topology.starts[node]..topology.starts[node + 1] {
                if next != twin && remaining[edges[next as usize] as usize] && can_join(slot, next)?
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
        Ok(BoundaryPlan {
            source_darts,
            walks: planned_walks,
            lines: planned_lines,
            points: Vec::new(),
            face_quotient,
        })
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
        let mut selected = Vec::with_capacity(topology.targets.len());
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let other = topology.face_of[topology.twins[slot] as usize];
            selected.push(bounds(
                self.walk_faces[walk as usize] as usize,
                self.walk_faces[other as usize] as usize,
            ));
        }
        let mut visited = Vec::with_capacity(selected.len());
        visited.resize(selected.len(), false);
        let mut positions = Vec::with_capacity(topology.starts.len() - 1);
        positions.resize(topology.starts.len() - 1, None);
        let mut walks = Vec::new();
        for start in 0..selected.len() {
            if !selected[start] || visited[start] {
                continue;
            }
            let mut walk = Vec::new();
            let mut slot = start;
            loop {
                debug_assert!(!visited[slot]);
                visited[slot] = true;
                positions[topology.owners[slot] as usize] = Some(walk.len());
                walk.push(slot as u32);
                let target = topology.targets[slot] as usize;
                // A tangent hole can share a boundary walk with its shell.
                // Cut closed cycles by exact identity before any rounding.
                if let Some(begin) = positions[target] {
                    let mut cycle = Vec::with_capacity(walk.len() - begin);
                    for dart in walk.drain(begin..) {
                        positions[topology.owners[dart as usize] as usize] = None;
                        cycle.push(dart);
                    }
                    walks.push(cycle);
                }
                let first = topology.starts[target] as usize;
                let end = topology.starts[target + 1] as usize;
                let twin = topology.twins[slot] as usize;
                // Skip omitted wedges clockwise from the incoming twin.
                slot = twin;
                loop {
                    slot = if slot == first { end - 1 } else { slot - 1 };
                    if selected[slot] {
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
        walks
    }

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
