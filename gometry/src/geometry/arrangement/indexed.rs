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
    pub(crate) face_cursor: Vec<u32>,
    pub(crate) successor: Vec<u32>,
    pub(crate) edge_slots: Vec<[u32; 2]>,
}

impl IndexedTopology {
    pub(crate) fn successor_slot(&self, slot: u32) -> u32 {
        let slot = slot as usize;
        let target = self.targets[slot] as usize;
        let start = self.starts[target] as usize;
        let end = self.starts[target + 1] as usize;
        let reverse = self.twins[slot] as usize;
        if reverse == start {
            (end - 1) as u32
        } else {
            (reverse - 1) as u32
        }
    }

    pub(crate) fn predecessor_slot(&self, slot: u32) -> u32 {
        let slot = slot as usize;
        let origin = self.owners[slot] as usize;
        let start = self.starts[origin] as usize;
        let end = self.starts[origin + 1] as usize;
        let next = if slot + 1 == end { start } else { slot + 1 };
        self.twins[next]
    }

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
        debug_assert!(scratch.edge_slots.capacity() >= slots / 2);
        self.owners.clear();
        for vertex in 0..vertices {
            let row = self.starts[vertex] as usize..self.starts[vertex + 1] as usize;
            debug_assert!(row.end <= slots);
            self.owners
                .extend(std::iter::repeat_n(vertex as u32, row.len()));
        }

        scratch.edge_slots.clear();
        scratch.edge_slots.resize(slots / 2, [u32::MAX; 2]);
        for (slot, &edge) in edge_of_slot.iter().enumerate() {
            let pair = &mut scratch.edge_slots[edge as usize];
            if pair[0] == u32::MAX {
                pair[0] = slot as u32;
            } else {
                pair[1] = slot as u32;
            }
        }
        self.twins.clear();
        self.twins.resize(slots, u32::MAX);
        for &[left, right] in &scratch.edge_slots {
            debug_assert_ne!(left, u32::MAX);
            debug_assert_ne!(right, u32::MAX);
            debug_assert_eq!(self.owners[left as usize], self.targets[right as usize]);
            debug_assert_eq!(self.targets[left as usize], self.owners[right as usize]);
            self.twins[left as usize] = right;
            self.twins[right as usize] = left;
        }

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
            component_count += 1;
        }

        scratch.successor.clear();
        scratch.successor.resize(slots, u32::MAX);
        for (slot, successor) in scratch.successor.iter_mut().enumerate() {
            *successor = self.successor_slot(slot as u32);
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
            let face = (self.face_starts.len() - 1) as u32;
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

impl IndexedTopologyScratch {
    pub(crate) fn clear(&mut self) {
        self.component_stack.clear();
        self.face_cursor.clear();
        self.successor.clear();
        self.edge_slots.clear();
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
        let vertices = starts.len() - 1;
        let slots = targets.len();
        topology.owners.reserve(slots);
        topology.twins.reserve(slots);
        topology.component_of.reserve(vertices);
        topology.face_of.reserve(slots);
        topology.face_starts.reserve(slots + 1);
        topology.face_slots.reserve(slots);
        let mut scratch = IndexedTopologyScratch::default();
        scratch.component_stack.reserve(vertices);
        scratch.successor.reserve(slots);
        scratch.edge_slots.reserve(slots / 2);
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
}
