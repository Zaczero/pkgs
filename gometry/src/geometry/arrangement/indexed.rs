use crate::collections::UnionFind;

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
    pub(crate) component_id_of_root: Vec<u32>,
    pub(crate) face_cursor: Vec<u32>,
    pub(crate) successor: Vec<u32>,
    pub(crate) edge_slots: Vec<[u32; 2]>,
}

impl IndexedTopology {
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

    pub(crate) fn finish(&mut self, edge_of_slot: &[u32], scratch: &mut IndexedTopologyScratch) -> u32 {
        let vertices = self.starts.len().saturating_sub(1);
        let slots = self.targets.len();
        debug_assert_eq!(self.starts.first(), Some(&0));
        debug_assert_eq!(self.starts.last().copied(), Some(slots as u32));
        debug_assert_eq!(edge_of_slot.len(), slots);
        debug_assert_eq!(slots % 2, 0);

        self.owners.clear();
        self.owners.reserve(slots);
        for vertex in 0..vertices {
            let row = self.starts[vertex] as usize..self.starts[vertex + 1] as usize;
            debug_assert!(row.end <= slots);
            self.owners.extend(std::iter::repeat_n(vertex as u32, row.len()));
        }

        scratch.edge_slots.clear();
        scratch.edge_slots.resize(slots / 2, [u32::MAX; 2]);
        for (slot, &edge) in edge_of_slot.iter().enumerate() {
            let pair = &mut scratch.edge_slots[edge as usize];
            if pair[0] == u32::MAX { pair[0] = slot as u32; } else { pair[1] = slot as u32; }
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

        let mut components = UnionFind::new(vertices);
        for slot in 0..slots { components.union(self.owners[slot] as usize, self.targets[slot] as usize); }
        scratch.component_id_of_root.clear();
        scratch.component_id_of_root.resize(vertices, u32::MAX);
        self.component_of.clear();
        self.component_of.reserve(vertices);
        let mut component_count = 0;
        for vertex in 0..vertices {
            let id = &mut scratch.component_id_of_root[components.find(vertex)];
            if *id == u32::MAX { *id = component_count; component_count += 1; }
            self.component_of.push(*id);
        }

        scratch.successor.clear();
        scratch.successor.resize(slots, u32::MAX);
        for slot in 0..slots {
            let target = self.targets[slot] as usize;
            let row = self.starts[target] as usize..self.starts[target + 1] as usize;
            let reverse = self.twins[slot] as usize;
            let position = reverse - row.start;
            scratch.successor[slot] = (row.start + (position + row.len() - 1) % row.len()) as u32;
        }
        self.face_of.clear();
        self.face_of.resize(slots, u32::MAX);
        self.face_starts.clear();
        self.face_starts.push(0);
        self.face_slots.clear();
        self.face_slots.reserve(slots);
        for seed in 0..slots {
            if self.face_of[seed] != u32::MAX { continue; }
            let face = (self.face_starts.len() - 1) as u32;
            let mut slot = seed;
            for _ in 0..slots {
                debug_assert_eq!(self.face_of[slot], u32::MAX);
                self.face_of[slot] = face;
                self.face_slots.push(slot as u32);
                slot = scratch.successor[slot] as usize;
                if slot == seed { break; }
            }
            debug_assert_eq!(slot, seed);
            self.face_starts.push(self.face_slots.len() as u32);
        }
        component_count
    }
}

impl IndexedTopologyScratch {
    pub(crate) fn clear(&mut self) {
        self.component_id_of_root.clear();
        self.face_cursor.clear();
        self.successor.clear();
        self.edge_slots.clear();
    }
}
