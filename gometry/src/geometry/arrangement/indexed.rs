use std::cmp::Ordering;

use crate::error::{ResourceError, Result};
use crate::geometry::segments::try_sort_by_with_work;
use crate::geometry::{GeometryErrorKind, Unmetered, Work};

/// Geometry-independent CSR topology. Rows are supplied in their final cyclic
/// order; this type only derives reciprocal slots, components, and boundary
/// walks from that combinatorial embedding.
#[derive(Default)]
pub(crate) struct IndexedTopology {
    pub(crate) starts: Vec<u32>,
    pub(crate) targets: Vec<u32>,
    pub(crate) owners: Vec<u32>,
    pub(crate) twins: Vec<u32>,
    pub(crate) component_of: Vec<u32>,
    pub(crate) face_of: Vec<u32>,
    pub(crate) face_starts: Vec<u32>,
    pub(crate) face_slots: Vec<u32>,
}

#[derive(Default)]
pub(crate) struct IndexedTopologyScratch {
    pub(crate) component_stack: Vec<u32>,
    pub(crate) successor: Vec<u32>,
    pub(crate) first_slot: Vec<u32>,
}

pub(crate) fn indexed_topology_from_edges_with_work<W, C>(
    node_count: usize,
    edge_nodes: &[[u32; 2]],
    work: &mut W,
    compare_departures: C,
) -> Result<(IndexedTopology, Vec<u32>)>
where
    W: Work,
    C: FnMut(u32, usize, usize, &mut W) -> Result<Ordering>,
{
    indexed_topology_from_edge_fn_with_work(
        node_count,
        edge_nodes.len(),
        |edge| {
            edge_nodes.get(edge).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("indexed graph edge is unknown".into()).into()
            })
        },
        work,
        compare_departures,
    )
}

/// Return each undirected input edge's forward and reverse departure slots.
///
/// Slot IDs use the endpoint order supplied for that edge, even though the
/// topology stores each departure in its owner vertex's cyclic row.
pub(crate) fn indexed_edge_slots_with_work<W: Work>(
    edge_nodes: &[[u32; 2]],
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    work: &mut W,
) -> Result<Vec<[u32; 2]>> {
    let slot_count = edge_nodes
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    if edge_of_slot.len() != slot_count
        || topology.owners.len() != slot_count
        || topology.targets.len() != slot_count
        || topology.twins.len() != slot_count
    {
        return Err(GeometryErrorKind::Invalid(
            "indexed edge-slot inputs do not share one topology".into(),
        )
        .into());
    }
    work.charge(u64::try_from(slot_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut slot_of_edge = vec![[u32::MAX; 2]; edge_nodes.len()];
    for (slot_index, &edge) in edge_of_slot.iter().enumerate() {
        work.charge(1)?;
        let endpoints = edge_nodes.get(edge as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed slot references an unknown edge".into())
        })?;
        let origin = *topology
            .owners
            .get(slot_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("indexed slot has no origin".into()))?;
        let target = *topology
            .targets
            .get(slot_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("indexed slot has no target".into()))?;
        let direction = if [origin, target] == *endpoints {
            0
        } else if [target, origin] == *endpoints {
            1
        } else {
            return Err(GeometryErrorKind::Invalid(
                "indexed slot endpoints disagree with their edge".into(),
            )
            .into());
        };
        let row = slot_of_edge.get_mut(edge as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed slot edge is outside its inverse map".into())
        })?;
        let slot = row.get_mut(direction).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed slot direction is unknown".into())
        })?;
        if *slot != u32::MAX {
            return Err(GeometryErrorKind::Invalid(
                "indexed edge has multiple slots in one direction".into(),
            )
            .into());
        }
        *slot = u32::try_from(slot_index).map_err(|_| ResourceError::SizeOverflow)?;
    }
    for row in &slot_of_edge {
        work.charge(2)?;
        if row.contains(&u32::MAX) {
            return Err(GeometryErrorKind::Invalid(
                "indexed edge is missing a directed slot".into(),
            )
            .into());
        }
    }
    Ok(slot_of_edge)
}

pub(crate) fn indexed_topology_from_edge_fn_with_work<W, E, C>(
    node_count: usize,
    edge_count: usize,
    mut edge_nodes: E,
    work: &mut W,
    mut compare_departures: C,
) -> Result<(IndexedTopology, Vec<u32>)>
where
    W: Work,
    E: FnMut(usize) -> Result<[u32; 2]>,
    C: FnMut(u32, usize, usize, &mut W) -> Result<Ordering>,
{
    let slots = IndexedTopology::admit_counts(node_count, edge_count)?;
    let start_capacity = node_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(start_capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut topology = IndexedTopology::default();
    topology.starts.resize(start_capacity, 0);
    for edge in 0..edge_count {
        work.charge(1)?;
        let [start, end] = edge_nodes(edge)?;
        for node in [start, end] {
            work.charge(1)?;
            if node as usize >= node_count || start == end {
                return Err(GeometryErrorKind::Invalid(
                    "indexed graph edge has an invalid endpoint pair".into(),
                )
                .into());
            }
            let offset = (node as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let degree = topology.starts.get_mut(offset).ok_or_else(|| {
                GeometryErrorKind::Invalid("indexed graph endpoint is outside its CSR".into())
            })?;
            *degree = degree.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for offset in 1..start_capacity {
        work.charge(1)?;
        let previous = *topology.starts.get(offset - 1).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph CSR prefix is incomplete".into())
        })?;
        let degree = *topology.starts.get(offset).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph CSR degree is unknown".into())
        })?;
        *topology.starts.get_mut(offset).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph CSR prefix is incomplete".into())
        })? = previous
            .checked_add(degree)
            .ok_or(ResourceError::SizeOverflow)?;
    }
    if topology.starts.last().copied()
        != Some(u32::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)
    {
        return Err(GeometryErrorKind::Invalid(
            "indexed graph degree total differs from its admitted edge count".into(),
        )
        .into());
    }

    work.charge(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut cursor = Vec::with_capacity(node_count);
    cursor.extend(topology.starts.iter().take(node_count).copied());
    work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut edge_of_slot = vec![0_u32; slots];
    for edge in 0..edge_count {
        work.charge(1)?;
        let [start, end] = edge_nodes(edge)?;
        for node in [start, end] {
            work.charge(1)?;
            let position = cursor.get_mut(node as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("indexed graph endpoint is outside its CSR".into())
            })?;
            let slot = usize::try_from(*position).map_err(|_| ResourceError::SizeOverflow)?;
            *edge_of_slot.get_mut(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("indexed graph endpoint slot is unknown".into())
            })? = u32::try_from(edge).map_err(|_| ResourceError::SizeOverflow)?;
            *position = position.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }

    for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
        work.charge(1)?;
        let row = edge_of_slot
            .get_mut(start as usize..end as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("indexed graph departure row is incomplete".into())
            })?;
        try_sort_by_with_work(row, work, |left, right, work| {
            compare_departures(node as u32, *left as usize, *right as usize, work)
        })?;
        for pair in row.array_windows::<2>() {
            work.charge(1)?;
            let [left, right] = pair;
            if compare_departures(node as u32, *left as usize, *right as usize, work)?.is_eq() {
                return Err(GeometryErrorKind::Invalid(
                    "distinct indexed graph edges share a departure ray".into(),
                )
                .into());
            }
        }
    }

    work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
    topology.targets.reserve(slots);
    for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
        for &edge in edge_of_slot
            .get(start as usize..end as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("indexed graph departure row is incomplete".into())
            })?
        {
            work.charge(1)?;
            let [left, right] = edge_nodes(edge as usize)?;
            let target = if left as usize == node {
                right
            } else if right as usize == node {
                left
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "indexed graph departure is not incident to its row".into(),
                )
                .into());
            };
            topology.targets.push(target);
        }
    }

    let face_start_capacity = slots.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
    topology.owners.reserve(slots);
    work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
    topology.twins.reserve(slots);
    work.charge(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    topology.component_of.reserve(node_count);
    work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
    topology.face_of.reserve(slots);
    work.charge(u64::try_from(face_start_capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    topology.face_starts.reserve(face_start_capacity);
    work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
    topology.face_slots.reserve(slots);

    let mut scratch = IndexedTopologyScratch::default();
    work.charge(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    scratch.component_stack.reserve(node_count);
    work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
    scratch.successor.reserve(slots);
    work.charge(u64::try_from(edge_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    scratch.first_slot.reserve(edge_count);
    topology.finish_with_work(&edge_of_slot, &mut scratch, work)?;
    Ok((topology, edge_of_slot))
}

impl IndexedTopology {
    pub(crate) fn admit_counts(
        vertex_count: usize,
        edge_count: usize,
    ) -> crate::error::Result<usize> {
        let slots = edge_count
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if vertex_count > u32::MAX as usize || slots > u32::MAX as usize {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        }
        Ok(slots)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "slot, target, and reciprocal twin IDs belong to this admitted CSR topology"
    )]
    pub(crate) fn successor_slot(&self, slot: u32) -> u32 {
        let slot = slot as usize;
        let target = self.targets[slot] as usize;
        let start = self.starts[target] as usize;
        let end = self.starts[target + 1] as usize;
        let reverse = self.twins[slot] as usize;
        if reverse == start {
            end.wrapping_sub(1) as u32
        } else {
            reverse.wrapping_sub(1) as u32
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "slot and origin IDs belong to this admitted CSR topology"
    )]
    pub(crate) fn predecessor_slot(&self, slot: u32) -> u32 {
        let slot = slot as usize;
        let origin = self.owners[slot] as usize;
        let start = self.starts[origin] as usize;
        let end = self.starts[origin + 1] as usize;
        let next = if slot + 1 == end { start } else { slot + 1 };
        self.twins[next]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "edge, vertex, twin, successor, and face slots are derived from the admitted CSR columns built here"
    )]
    #[expect(
        clippy::expect_used,
        reason = "the Unmetered specialization cannot fail its Work charge"
    )]
    pub(crate) fn finish(
        &mut self,
        edge_of_slot: &[u32],
        scratch: &mut IndexedTopologyScratch,
    ) -> u32 {
        let mut work = Unmetered;
        self.finish_with_work(edge_of_slot, scratch, &mut work)
            .expect("unmetered indexed topology finalization cannot fail")
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "edge, vertex, twin, successor, and face slots are derived from the admitted CSR columns built here"
    )]
    pub(crate) fn finish_with_work<W: Work>(
        &mut self,
        edge_of_slot: &[u32],
        scratch: &mut IndexedTopologyScratch,
        work: &mut W,
    ) -> Result<u32> {
        let vertices = self.starts.len().saturating_sub(1);
        let slots = self.targets.len();
        debug_assert_eq!(self.starts.first(), Some(&0));
        debug_assert_eq!(self.starts.last().copied(), Some(slots as u32));
        debug_assert_eq!(edge_of_slot.len(), slots);
        debug_assert_eq!(slots % 2, 0);

        debug_assert!(self.owners.capacity() >= slots);
        debug_assert!(self.twins.capacity() >= slots);
        debug_assert!(self.component_of.capacity() >= vertices);
        debug_assert!(self.face_of.capacity() >= slots);
        debug_assert!(self.face_starts.capacity() > slots);
        debug_assert!(self.face_slots.capacity() >= slots);
        debug_assert!(scratch.component_stack.capacity() >= vertices);
        debug_assert!(scratch.successor.capacity() >= slots);
        debug_assert!(scratch.first_slot.capacity() >= slots / 2);
        work.charge(u64::try_from(vertices).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.owners.clear();
        for vertex in 0..vertices {
            work.charge(1)?;
            let row = self.starts[vertex] as usize..self.starts[vertex.wrapping_add(1)] as usize;
            debug_assert!(row.end <= slots);
            self.owners
                .extend(std::iter::repeat_n(vertex as u32, row.len()));
        }

        work.charge(u64::try_from(slots / 2).map_err(|_| ResourceError::SizeOverflow)?)?;
        scratch.first_slot.clear();
        scratch.first_slot.resize(slots / 2, u32::MAX);
        work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.twins.clear();
        self.twins.resize(slots, u32::MAX);
        let mut paired_edges = 0_usize;
        for (slot, &edge) in edge_of_slot.iter().enumerate() {
            work.charge(1)?;
            let first_slot = &mut scratch.first_slot[edge as usize];
            if *first_slot == u32::MAX {
                *first_slot = slot as u32;
            } else {
                let first = *first_slot as usize;
                debug_assert_ne!(first, slot);
                debug_assert_eq!(self.twins[first], u32::MAX);
                debug_assert_eq!(self.owners[first], self.targets[slot]);
                debug_assert_eq!(self.targets[first], self.owners[slot]);
                self.twins[first] = slot as u32;
                self.twins[slot] = first as u32;
                paired_edges = paired_edges.wrapping_add(1);
            }
        }
        debug_assert_eq!(paired_edges, slots / 2);

        work.charge(u64::try_from(vertices).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.component_of.clear();
        self.component_of.resize(vertices, u32::MAX);
        scratch.component_stack.clear();
        let mut component_count = 0;
        for seed in 0..vertices {
            work.charge(1)?;
            if self.component_of[seed] != u32::MAX {
                continue;
            }
            self.component_of[seed] = component_count;
            scratch.component_stack.push(seed as u32);
            while let Some(vertex) = scratch.component_stack.pop() {
                work.charge(1)?;
                let row = self.starts[vertex as usize] as usize
                    ..self.starts[vertex as usize + 1] as usize;
                for &neighbor in &self.targets[row] {
                    work.charge(1)?;
                    let neighbor = neighbor as usize;
                    if self.component_of[neighbor] == u32::MAX {
                        self.component_of[neighbor] = component_count;
                        scratch.component_stack.push(neighbor as u32);
                    }
                }
            }
            component_count = component_count.wrapping_add(1);
        }

        work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
        scratch.successor.clear();
        scratch.successor.resize(slots, u32::MAX);
        for &[start, end] in self.starts.array_windows::<2>() {
            work.charge(1)?;
            let row = start as usize..end as usize;
            let mut previous = end.wrapping_sub(1);
            for slot in row {
                work.charge(1)?;
                scratch.successor[self.twins[slot] as usize] = previous;
                previous = slot as u32;
            }
        }
        work.charge(u64::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?)?;
        self.face_of.clear();
        self.face_of.resize(slots, u32::MAX);
        self.face_starts.clear();
        work.charge(1)?;
        self.face_starts.push(0);
        self.face_slots.clear();
        for seed in 0..slots {
            work.charge(1)?;
            if self.face_of[seed] != u32::MAX {
                continue;
            }
            let face = self.face_starts.len().wrapping_sub(1) as u32;
            let mut slot = seed;
            for _ in 0..slots {
                work.charge(1)?;
                debug_assert_eq!(self.face_of[slot], u32::MAX);
                self.face_of[slot] = face;
                self.face_slots.push(slot as u32);
                slot = scratch.successor[slot] as usize;
                if slot == seed {
                    break;
                }
            }
            debug_assert_eq!(slot, seed);
            work.charge(1)?;
            self.face_starts.push(self.face_slots.len() as u32);
        }
        Ok(component_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reserved_topology(
        starts: &[u32],
        targets: &[u32],
    ) -> (IndexedTopology, IndexedTopologyScratch) {
        let mut topology = IndexedTopology {
            starts: starts.to_vec(),
            targets: targets.to_vec(),
            ..Default::default()
        };
        let vertices = starts
            .len()
            .checked_sub(1)
            .expect("CSR starts include their initial sentinel");
        let slots = targets.len();
        topology.owners.reserve(slots);
        topology.twins.reserve(slots);
        topology.component_of.reserve(vertices);
        topology.face_of.reserve(slots);
        topology.face_starts.reserve(
            slots
                .checked_add(1)
                .expect("face starts include their terminal sentinel"),
        );
        topology.face_slots.reserve(slots);
        let mut scratch = IndexedTopologyScratch::default();
        scratch.component_stack.reserve(vertices);
        scratch.successor.reserve(slots);
        scratch.first_slot.reserve(slots / 2);
        (topology, scratch)
    }

    fn finish(starts: &[u32], targets: &[u32], edges: &[u32]) -> (IndexedTopology, u32) {
        let (mut topology, mut scratch) = reserved_topology(starts, targets);
        let component_count = topology.finish(edges, &mut scratch);
        (topology, component_count)
    }

    fn assert_same_topology(
        actual: &IndexedTopology,
        actual_components: u32,
        expected: &IndexedTopology,
        expected_components: u32,
    ) {
        assert_eq!(actual.starts, expected.starts);
        assert_eq!(actual.targets, expected.targets);
        assert_eq!(actual.owners, expected.owners);
        assert_eq!(actual.twins, expected.twins);
        assert_eq!(actual.component_of, expected.component_of);
        assert_eq!(actual.face_of, expected.face_of);
        assert_eq!(actual.face_starts, expected.face_starts);
        assert_eq!(actual.face_slots, expected.face_slots);
        assert_eq!(actual_components, expected_components);
    }

    #[test]
    fn indexes_parallel_disconnected_and_isolated_vertices() {
        // Vertices 0 and 1 have two distinct reciprocal edges. Vertices 2--4
        // form an independent triangle; vertex 5 is intentionally isolated.
        let (topology, component_count) = finish(
            &[0, 2, 4, 6, 8, 10, 10],
            &[1, 1, 0, 0, 3, 4, 2, 4, 2, 3],
            &[0, 1, 0, 1, 2, 4, 2, 3, 4, 3],
        );
        assert_eq!(component_count, 3);
        assert_eq!(topology.owners, [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]);
        assert_eq!(topology.twins, [2, 3, 0, 1, 6, 8, 4, 9, 5, 7]);
        assert_eq!(topology.component_of, [0, 0, 1, 1, 1, 2]);
        assert_eq!(topology.face_of, [0, 1, 1, 0, 2, 3, 3, 2, 2, 3]);
        assert_eq!(topology.face_starts, [0, 2, 4, 7, 10]);
        assert_eq!(topology.face_slots, [0, 3, 1, 2, 4, 7, 8, 5, 9, 6]);
        for (slot, &twin) in topology.twins.iter().enumerate() {
            assert_eq!(topology.twins[twin as usize], slot as u32);
            assert_eq!(topology.owners[slot], topology.targets[twin as usize]);
        }
    }

    #[test]
    fn finish_keeps_vertex_ids_across_an_empty_row() {
        let (topology, component_count) = finish(&[0, 1, 1, 2], &[2, 0], &[0, 0]);

        assert_eq!(component_count, 2);
        assert_eq!(topology.owners, [0, 2]);
        assert_eq!(topology.twins, [1, 0]);
        assert_eq!(topology.component_of, [0, 1, 0]);
        assert_eq!(topology.face_of, [0, 0]);
        assert_eq!(topology.face_starts, [0, 2]);
        assert_eq!(topology.face_slots, [0, 1]);
    }

    #[test]
    fn finish_handles_zero_edges_with_no_vertices() {
        let (topology, component_count) = finish(&[0], &[], &[]);

        assert_eq!(component_count, 0);
        assert!(topology.owners.is_empty());
        assert!(topology.twins.is_empty());
        assert!(topology.component_of.is_empty());
        assert!(topology.face_of.is_empty());
        assert_eq!(topology.face_starts, [0]);
        assert!(topology.face_slots.is_empty());
    }

    #[test]
    fn finish_handles_zero_edges_with_isolated_vertices() {
        let (topology, component_count) = finish(&[0, 0, 0, 0], &[], &[]);

        assert_eq!(component_count, 3);
        assert!(topology.owners.is_empty());
        assert!(topology.twins.is_empty());
        assert_eq!(topology.component_of, [0, 1, 2]);
        assert!(topology.face_of.is_empty());
        assert_eq!(topology.face_starts, [0]);
        assert!(topology.face_slots.is_empty());
    }

    #[test]
    fn finish_reuse_matches_a_fresh_topology_after_shrinking() {
        let larger_starts = [0, 2, 4, 6, 8, 10, 10];
        let larger_targets = [1, 1, 0, 0, 3, 4, 2, 4, 2, 3];
        let larger_edges = [0, 1, 0, 1, 2, 4, 2, 3, 4, 3];
        let (mut reused, mut scratch) = reserved_topology(&larger_starts, &larger_targets);
        assert_eq!(reused.finish(&larger_edges, &mut scratch), 3);

        reused.starts.clear();
        reused.starts.extend([0, 1, 1, 2]);
        reused.targets.clear();
        reused.targets.extend([2, 0]);
        let reused_components = reused.finish(&[0, 0], &mut scratch);
        let (fresh, fresh_components) = finish(&[0, 1, 1, 2], &[2, 0], &[0, 0]);

        assert_same_topology(&reused, reused_components, &fresh, fresh_components);
        assert_eq!(reused.owners, [0, 2]);
        assert_eq!(reused.twins, [1, 0]);
        assert_eq!(reused.component_of, [0, 1, 0]);
    }

    #[test]
    fn count_admission_checks_vertex_sentinel_and_terminal_slot_widths() {
        let max_count = u32::MAX as usize;
        assert_eq!(
            IndexedTopology::admit_counts(max_count, (u32::MAX / 2) as usize).unwrap(),
            max_count - 1
        );
        let beyond_vertex_domain = max_count.checked_add(1).unwrap();
        assert!(matches!(
            IndexedTopology::admit_counts(beyond_vertex_domain, 0)
                .unwrap_err()
                .kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
        assert!(matches!(
            IndexedTopology::admit_counts(0, (u32::MAX / 2) as usize + 1)
                .unwrap_err()
                .kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
    }
}
