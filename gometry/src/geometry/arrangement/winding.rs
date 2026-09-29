use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::{AreaSign, HashMap, PointKey, XY};
use crate::scratch_pool::{
    RetainScratch, ScratchLease, ScratchPool, flat_vec_retained_bytes, hash_map_retained_bytes,
};
/// Per-half-edge winding weight: `i32` for single-selection consumers
/// (buffers, coverage, dissolve), `[i32; 2]` for binary overlays (one
/// winding per operand). The trait keeps the columns and the BFS fill
/// monomorphized — no boxing, no per-edge branching.
pub(crate) trait WindingWeight: Copy + Eq + Sized + 'static {
    /// BFS sentinel — a value real fills can never produce.
    const UNSET: Self;
    fn add(self, other: Self) -> Self;
    fn sub(self, other: Self) -> Self;
    fn neg(self) -> Self;
    fn typed_spares() -> ScratchLease<TypedSpares<Self>>;
}

impl WindingWeight for i32 {
    const UNSET: Self = Self::MIN;

    fn add(self, other: Self) -> Self {
        self + other
    }

    fn sub(self, other: Self) -> Self {
        self - other
    }

    fn neg(self) -> Self {
        -self
    }

    fn typed_spares() -> ScratchLease<TypedSpares<Self>> {
        ScratchPool::checkout(&I32_TYPED_SPARES)
    }
}

impl WindingWeight for [i32; 2] {
    const UNSET: Self = [i32::MIN; 2];

    fn add(self, other: Self) -> Self {
        [self[0] + other[0], self[1] + other[1]]
    }

    fn sub(self, other: Self) -> Self {
        [self[0] - other[0], self[1] - other[1]]
    }

    fn neg(self) -> Self {
        [-self[0], -self[1]]
    }

    fn typed_spares() -> ScratchLease<TypedSpares<Self>> {
        ScratchPool::checkout(&PAIR_TYPED_SPARES)
    }
}

/// One arrangement face's exact area sign and component id.
pub(crate) struct Face {
    pub area_sign: AreaSign,
    pub component: u32,
}

/// Reusable geometry-independent buffers for the geographic graph test oracle.
/// Each thread keeps one spare set; a nested or concurrent build allocates a
/// fresh set while the pooled one is checked out.
#[derive(Default)]
pub(crate) struct ArrangementSpares {
    pub(crate) points: Vec<XY>,
    pub(crate) topology: IndexedTopology,
    pub(crate) topology_scratch: IndexedTopologyScratch,
    pub(crate) edge_of_slot: Vec<u32>,
    pub(crate) ids: Option<HashMap<PointKey, u32>>,
    pub(crate) filled: Vec<u32>,
    pub(crate) outer: Vec<Option<u32>>,
    pub(crate) winding_queue: std::collections::VecDeque<u32>,
}

impl RetainScratch for ArrangementSpares {
    fn clear_for_reuse(&mut self) {
        self.points.clear();
        self.topology.clear();
        self.topology_scratch.clear();
        self.edge_of_slot.clear();
        if let Some(ids) = &mut self.ids {
            ids.clear();
        }
        self.filled.clear();
        self.outer.clear();
        self.winding_queue.clear();
    }

    fn retained_bytes(&self) -> usize {
        flat_vec_retained_bytes(&self.points)
            .saturating_add(flat_vec_retained_bytes(&self.topology.starts))
            .saturating_add(flat_vec_retained_bytes(&self.topology.targets))
            .saturating_add(flat_vec_retained_bytes(&self.topology.owners))
            .saturating_add(flat_vec_retained_bytes(&self.topology.twins))
            .saturating_add(flat_vec_retained_bytes(&self.topology.component_of))
            .saturating_add(flat_vec_retained_bytes(&self.topology.face_of))
            .saturating_add(flat_vec_retained_bytes(&self.topology.face_starts))
            .saturating_add(flat_vec_retained_bytes(&self.topology.face_slots))
            .saturating_add(flat_vec_retained_bytes(&self.edge_of_slot))
            .saturating_add(self.ids.as_ref().map_or(0, hash_map_retained_bytes))
            .saturating_add(flat_vec_retained_bytes(&self.filled))
            .saturating_add(flat_vec_retained_bytes(
                &self.topology_scratch.component_stack,
            ))
            .saturating_add(flat_vec_retained_bytes(&self.topology_scratch.successor))
            .saturating_add(flat_vec_retained_bytes(&self.topology_scratch.edge_slots))
            .saturating_add(flat_vec_retained_bytes(&self.outer))
            .saturating_add(
                self.winding_queue
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
    }
}

impl ArrangementSpares {
    pub(crate) fn take_ids(&mut self) -> crate::error::Result<HashMap<PointKey, u32>> {
        match self.ids.take() {
            Some(mut ids) => {
                ids.clear();
                Ok(ids)
            },
            None => Ok(HashMap::with_hasher(
                crate::collections::collection_hash_state()?.clone(),
            )),
        }
    }
}

/// Weight-typed buffers for the geographic graph test oracle, pooled per `W`.
/// Its weights are `i32` or `[i32; 2]`.
pub(crate) struct TypedSpares<W> {
    pub(crate) edges: Vec<(u32, u32, W)>,
    pub(crate) multiplicities: Vec<W>,
}

impl<W> Default for TypedSpares<W> {
    fn default() -> Self {
        Self {
            edges: Vec::new(),
            multiplicities: Vec::new(),
        }
    }
}

impl<W: 'static> RetainScratch for TypedSpares<W> {
    fn clear_for_reuse(&mut self) {
        self.edges.clear();
        self.multiplicities.clear();
    }

    fn retained_bytes(&self) -> usize {
        flat_vec_retained_bytes(&self.edges)
            .saturating_add(flat_vec_retained_bytes(&self.multiplicities))
    }
}

thread_local! {
    static I32_TYPED_SPARES: ScratchPool<TypedSpares<i32>> =
        ScratchPool::new(TypedSpares::default());
    static PAIR_TYPED_SPARES: ScratchPool<TypedSpares<[i32; 2]>> =
        ScratchPool::new(TypedSpares::default());
}

pub(crate) fn typed_spares<W: WindingWeight>() -> ScratchLease<TypedSpares<W>> {
    W::typed_spares()
}

thread_local! {
    static SPARES: ScratchPool<ArrangementSpares> = ScratchPool::new(ArrangementSpares::default());
}

pub(crate) fn arrangement_spares() -> ScratchLease<ArrangementSpares> {
    ScratchPool::checkout(&SPARES)
}

/// The arrangement over one noded segment set.
pub(crate) struct Arrangement<W: WindingWeight = i32> {
    pub(crate) core: ScratchLease<ArrangementSpares>,
    /// CSR adjacency: neighbors of vertex `v` are
    /// `targets[starts[v]..starts[v+1]]`, CCW by departure angle. `owners`
    /// is the source vertex and `twins` is the opposite half-edge slot for
    /// every adjacency slot.
    /// Net directed multiplicity per half-edge slot: each source segment's
    /// weight added running with the half-edge, subtracted against.
    pub(crate) typed: ScratchLease<TypedSpares<W>>,
    /// Face id per half-edge slot.
    /// CSR: the half-edge slots of face `f` are
    /// `face_slots[face_starts[f]..face_starts[f + 1]]`.
    pub(crate) faces: Vec<Face>,
    /// Vertex id by coordinate key (the construction dedup map, kept for
    /// consumer lookups).
    /// Component id per vertex.
    pub(crate) component_count: u32,
    pub(crate) thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl<W: WindingWeight> std::ops::Deref for Arrangement<W> {
    type Target = ArrangementSpares;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl std::ops::Deref for ArrangementSpares {
    type Target = IndexedTopology;

    fn deref(&self) -> &Self::Target {
        &self.topology
    }
}

impl std::ops::DerefMut for ArrangementSpares {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.topology
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scratch_pool::SCRATCH_RETAIN_BYTES;

    fn over_budget_capacity(element_size: usize) -> usize {
        SCRATCH_RETAIN_BYTES
            .checked_div(element_size)
            .expect("a vector element has nonzero size")
            .checked_add(1)
            .expect("the scratch retention test capacity fits usize")
    }

    fn clear_core_pool() {
        {
            let mut scratch = arrangement_spares();
            scratch
                .points
                .reserve_exact(over_budget_capacity(size_of::<XY>()));
        }
        assert_eq!(arrangement_spares().points.capacity(), 0);
    }

    fn clear_typed_pool<W: WindingWeight>() {
        typed_spares::<W>()
            .multiplicities
            .reserve_exact(over_budget_capacity(size_of::<W>()));
        assert_eq!(typed_spares::<W>().multiplicities.capacity(), 0);
    }

    fn assert_lane_evicted<T: RetainScratch + 'static>(
        lane: &str,
        element_size: usize,
        clear_pool: impl Fn(),
        checkout: impl Fn() -> ScratchLease<T>,
        reserve: impl FnOnce(&mut T, usize),
        capacity: impl FnOnce(&T) -> usize,
    ) {
        clear_pool();
        {
            let mut scratch = checkout();
            reserve(&mut scratch, over_budget_capacity(element_size));
        }
        let scratch = checkout();
        assert_eq!(capacity(&scratch), 0, "{lane} capacity was retained");
    }

    #[test]
    fn core_pooled_buffers_evict_when_each_lane_exceeds_budget() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        let u32_size = size_of::<u32>();
        assert_lane_evicted(
            "topology.starts",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology.starts.reserve_exact(capacity),
            |scratch| scratch.topology.starts.capacity(),
        );
        assert_lane_evicted(
            "topology.targets",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology.targets.reserve_exact(capacity),
            |scratch| scratch.topology.targets.capacity(),
        );
        assert_lane_evicted(
            "topology.owners",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology.owners.reserve_exact(capacity),
            |scratch| scratch.topology.owners.capacity(),
        );
        assert_lane_evicted(
            "topology.twins",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology.twins.reserve_exact(capacity),
            |scratch| scratch.topology.twins.capacity(),
        );
        assert_lane_evicted(
            "topology.component_of",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology.component_of.reserve_exact(capacity),
            |scratch| scratch.topology.component_of.capacity(),
        );
        assert_lane_evicted(
            "topology.face_of",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology.face_of.reserve_exact(capacity),
            |scratch| scratch.topology.face_of.capacity(),
        );
        assert_lane_evicted(
            "topology.face_starts",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology.face_starts.reserve_exact(capacity),
            |scratch| scratch.topology.face_starts.capacity(),
        );
        assert_lane_evicted(
            "topology.face_slots",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology.face_slots.reserve_exact(capacity),
            |scratch| scratch.topology.face_slots.capacity(),
        );
        assert_lane_evicted(
            "topology_scratch.component_stack",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| {
                scratch
                    .topology_scratch
                    .component_stack
                    .reserve_exact(capacity);
            },
            |scratch| scratch.topology_scratch.component_stack.capacity(),
        );
        assert_lane_evicted(
            "topology_scratch.successor",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology_scratch.successor.reserve_exact(capacity),
            |scratch| scratch.topology_scratch.successor.capacity(),
        );
        assert_lane_evicted(
            "topology_scratch.edge_slots",
            size_of::<[u32; 2]>(),
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.topology_scratch.edge_slots.reserve_exact(capacity),
            |scratch| scratch.topology_scratch.edge_slots.capacity(),
        );
        assert_lane_evicted(
            "edge_of_slot",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.edge_of_slot.reserve_exact(capacity),
            |scratch| scratch.edge_of_slot.capacity(),
        );
        assert_lane_evicted(
            "filled",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.filled.reserve_exact(capacity),
            |scratch| scratch.filled.capacity(),
        );
        assert_lane_evicted(
            "outer",
            size_of::<Option<u32>>(),
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.outer.reserve_exact(capacity),
            |scratch| scratch.outer.capacity(),
        );
        assert_lane_evicted(
            "winding_queue",
            u32_size,
            clear_core_pool,
            arrangement_spares,
            |scratch, capacity| scratch.winding_queue.reserve(capacity),
            |scratch| scratch.winding_queue.capacity(),
        );
    }

    #[test]
    fn typed_edge_buffers_evict_when_each_lane_exceeds_budget() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        assert_lane_evicted(
            "typed i32 edges",
            size_of::<(u32, u32, i32)>(),
            clear_typed_pool::<i32>,
            typed_spares::<i32>,
            |scratch, capacity| scratch.edges.reserve_exact(capacity),
            |scratch| scratch.edges.capacity(),
        );
        assert_lane_evicted(
            "typed pair edges",
            size_of::<(u32, u32, [i32; 2])>(),
            clear_typed_pool::<[i32; 2]>,
            typed_spares::<[i32; 2]>,
            |scratch, capacity| scratch.edges.reserve_exact(capacity),
            |scratch| scratch.edges.capacity(),
        );
    }

    #[test]
    fn each_arrangement_owner_reuses_medium_and_drops_huge() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        clear_core_pool();
        assert_eq!(arrangement_spares().points.capacity(), 0);
        {
            let mut core = arrangement_spares();
            core.points.reserve_exact(64);
        }
        let ((), allocations) = crate::test_support::allocation_count(|| {
            arrangement_spares().points.reserve_exact(64);
        });
        assert_eq!(allocations, 0);

        for pair in [false, true] {
            if pair {
                let mut scratch = typed_spares::<[i32; 2]>();
                scratch.edges.reserve_exact(64);
                drop(scratch);
                let ((), allocations) = crate::test_support::allocation_count(|| {
                    typed_spares::<[i32; 2]>().edges.reserve_exact(64);
                });
                assert_eq!(allocations, 0);
            } else {
                let mut scratch = typed_spares::<i32>();
                scratch.edges.reserve_exact(64);
                drop(scratch);
                let ((), allocations) = crate::test_support::allocation_count(|| {
                    typed_spares::<i32>().edges.reserve_exact(64);
                });
                assert_eq!(allocations, 0);
            }
        }

        clear_core_pool();
        assert_eq!(arrangement_spares().points.capacity(), 0);
        let ((), allocations) = crate::test_support::allocation_count(|| {
            arrangement_spares().points.reserve_exact(64);
        });
        assert_eq!(allocations, 1);
        let ((), allocations) = crate::test_support::allocation_count(|| {
            arrangement_spares().points.reserve_exact(64);
        });
        assert_eq!(allocations, 0);

        {
            let mut scratch = typed_spares::<i32>();
            scratch
                .multiplicities
                .reserve_exact(SCRATCH_RETAIN_BYTES / size_of::<i32>() + 1);
        }
        assert_eq!(typed_spares::<i32>().multiplicities.capacity(), 0);

        {
            let mut scratch = typed_spares::<[i32; 2]>();
            scratch
                .multiplicities
                .reserve_exact(SCRATCH_RETAIN_BYTES / size_of::<[i32; 2]>() + 1);
        }
        assert_eq!(typed_spares::<[i32; 2]>().multiplicities.capacity(), 0);
    }

    #[test]
    fn map_alone_can_exceed_the_core_budget() {
        let _budget = crate::scratch_pool::RetainBudgetGuard::new();
        clear_core_pool();
        {
            let mut scratch = arrangement_spares();
            let entries = SCRATCH_RETAIN_BYTES
                / (size_of::<PointKey>() + size_of::<u32>() + size_of::<usize>())
                + 1;
            let mut ids = scratch.take_ids().unwrap();
            ids.reserve(entries);
            assert!(hash_map_retained_bytes(&ids) > SCRATCH_RETAIN_BYTES);
            scratch.ids = Some(ids);
        }
        assert!(arrangement_spares().ids.is_none());
    }
}
