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

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.starts.clear();
        self.targets.clear();
        self.owners.clear();
        self.twins.clear();
        self.component_of.clear();
        self.face_of.clear();
        self.face_starts.clear();
        self.face_slots.clear();
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "edge, vertex, twin, successor, and face slots are derived from the admitted CSR columns built here"
    )]
    pub(crate) fn finish(
        &mut self,
        edge_of_slot: &[u32],
        scratch: &mut IndexedTopologyScratch,
    ) -> u32 {
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
        self.owners.clear();
        for vertex in 0..vertices {
            let row = self.starts[vertex] as usize..self.starts[vertex.wrapping_add(1)] as usize;
            debug_assert!(row.end <= slots);
            self.owners
                .extend(std::iter::repeat_n(vertex as u32, row.len()));
        }

        scratch.first_slot.clear();
        scratch.first_slot.resize(slots / 2, u32::MAX);
        self.twins.clear();
        self.twins.resize(slots, u32::MAX);
        let mut paired_edges = 0_usize;
        for (slot, &edge) in edge_of_slot.iter().enumerate() {
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

        self.component_of.clear();
        self.component_of.resize(vertices, u32::MAX);
        scratch.component_stack.clear();
        let mut component_count = 0;
        for seed in 0..vertices {
            if self.component_of[seed] != u32::MAX {
                continue;
            }
            self.component_of[seed] = component_count;
            scratch.component_stack.push(seed as u32);
            while let Some(vertex) = scratch.component_stack.pop() {
                let row = self.starts[vertex as usize] as usize
                    ..self.starts[vertex as usize + 1] as usize;
                for &neighbor in &self.targets[row] {
                    let neighbor = neighbor as usize;
                    if self.component_of[neighbor] == u32::MAX {
                        self.component_of[neighbor] = component_count;
                        scratch.component_stack.push(neighbor as u32);
                    }
                }
            }
            component_count = component_count.wrapping_add(1);
        }

        scratch.successor.clear();
        scratch.successor.resize(slots, u32::MAX);
        for &[start, end] in self.starts.array_windows::<2>() {
            let row = start as usize..end as usize;
            let mut previous = end.wrapping_sub(1);
            for slot in row {
                scratch.successor[self.twins[slot] as usize] = previous;
                previous = slot as u32;
            }
        }
        self.face_of.clear();
        self.face_of.resize(slots, u32::MAX);
        self.face_starts.clear();
        self.face_starts.push(0);
        self.face_slots.clear();
        for seed in 0..slots {
            if self.face_of[seed] != u32::MAX {
                continue;
            }
            let face = self.face_starts.len().wrapping_sub(1) as u32;
            let mut slot = seed;
            for _ in 0..slots {
                debug_assert_eq!(self.face_of[slot], u32::MAX);
                self.face_of[slot] = face;
                self.face_slots.push(slot as u32);
                slot = scratch.successor[slot] as usize;
                if slot == seed {
                    break;
                }
            }
            debug_assert_eq!(slot, seed);
            self.face_starts.push(self.face_slots.len() as u32);
        }
        component_count
    }
}

#[cfg(test)]
impl IndexedTopologyScratch {
    pub(crate) fn clear(&mut self) {
        self.component_stack.clear();
        self.successor.clear();
        self.first_slot.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finish(starts: &[u32], targets: &[u32], edges: &[u32]) -> IndexedTopology {
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
        topology.finish(edges, &mut scratch);
        topology
    }

    #[test]
    fn indexes_parallel_disconnected_and_isolated_vertices() {
        // Vertices 0 and 1 have two distinct reciprocal edges. Vertices 2--4
        // form an independent triangle; vertex 5 is intentionally isolated.
        let topology = finish(
            &[0, 2, 4, 6, 8, 10, 10],
            &[1, 1, 0, 0, 3, 4, 2, 4, 2, 3],
            &[0, 1, 0, 1, 2, 4, 2, 3, 4, 3],
        );
        assert_eq!(topology.component_of, [0, 0, 1, 1, 1, 2]);
        assert_eq!(topology.twins, [2, 3, 0, 1, 6, 8, 4, 9, 5, 7]);
        assert_eq!(topology.face_starts.first(), Some(&0));
        assert_eq!(topology.face_starts.last(), Some(&10));
        for (slot, &twin) in topology.twins.iter().enumerate() {
            assert_eq!(topology.twins[twin as usize], slot as u32);
            assert_eq!(topology.owners[slot], topology.targets[twin as usize]);
        }
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
