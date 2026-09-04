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

/// One face of the arrangement: its boundary ring (closed, first point
/// repeated), exact face decision (sign-only for winding consumers, signed-area
/// magnitude when rings are retained), and component id.
pub(crate) struct Face {
    /// Materialized only by [`Arrangement::new_with_rings`] — empty under
    /// every other constructor (winding consumers walk boundaries through
    /// [`Arrangement::region_rings`] instead).
    pub ring: Vec<XY>,
    pub area_sign: AreaSign,
    pub component: u32,
}

/// Reusable W-independent construction buffers: the bulk lanes build one
/// arrangement PER ROW, and these vectors plus the vertex-dedup map were
/// the row's dominant allocations. One spare set per thread; a nested or
/// concurrent construction simply allocates fresh (the pool is empty
/// while a set is checked out) and the last drop restocks it.
#[derive(Default)]
pub(crate) struct ArrangementSpares {
    pub(crate) points: Vec<XY>,
    pub(crate) starts: Vec<u32>,
    pub(crate) targets: Vec<u32>,
    pub(crate) owners: Vec<u32>,
    pub(crate) twins: Vec<u32>,
    pub(crate) face_of: Vec<u32>,
    pub(crate) face_starts: Vec<u32>,
    pub(crate) face_slots: Vec<u32>,
    pub(crate) ids: HashMap<PointKey, u32>,
    pub(crate) component_of: Vec<u32>,
    pub(crate) filled: Vec<u32>,
    pub(crate) component_id_of_root: Vec<u32>,
    pub(crate) face_cursor: Vec<u32>,
    pub(crate) outer: Vec<Option<u32>>,
    pub(crate) winding_queue: std::collections::VecDeque<u32>,
}

impl RetainScratch for ArrangementSpares {
    fn clear_for_reuse(&mut self) {
        self.points.clear();
        self.starts.clear();
        self.targets.clear();
        self.owners.clear();
        self.twins.clear();
        self.face_of.clear();
        self.face_starts.clear();
        self.face_slots.clear();
        self.ids.clear();
        self.component_of.clear();
        self.filled.clear();
        self.component_id_of_root.clear();
        self.face_cursor.clear();
        self.outer.clear();
        self.winding_queue.clear();
    }

    fn retained_bytes(&self) -> usize {
        flat_vec_retained_bytes(&self.points)
            .saturating_add(flat_vec_retained_bytes(&self.starts))
            .saturating_add(flat_vec_retained_bytes(&self.targets))
            .saturating_add(flat_vec_retained_bytes(&self.owners))
            .saturating_add(flat_vec_retained_bytes(&self.twins))
            .saturating_add(flat_vec_retained_bytes(&self.face_of))
            .saturating_add(flat_vec_retained_bytes(&self.face_starts))
            .saturating_add(flat_vec_retained_bytes(&self.face_slots))
            .saturating_add(hash_map_retained_bytes(&self.ids))
            .saturating_add(flat_vec_retained_bytes(&self.component_of))
            .saturating_add(flat_vec_retained_bytes(&self.filled))
            .saturating_add(flat_vec_retained_bytes(&self.component_id_of_root))
            .saturating_add(flat_vec_retained_bytes(&self.face_cursor))
            .saturating_add(flat_vec_retained_bytes(&self.outer))
            .saturating_add(
                self.winding_queue
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
    }
}

/// Reusable `region_rings` scratch (separate from the construction set:
/// a consumer may walk regions on a still-alive arrangement).
#[derive(Default)]
pub(crate) struct RegionSpares {
    pub(crate) used: Vec<bool>,
    pub(crate) position_of: Vec<(u32, u32)>,
    pub(crate) ring: Vec<XY>,
    pub(crate) ring_ids: Vec<u32>,
    pub(crate) path: Vec<XY>,
    pub(crate) path_ids: Vec<u32>,
}

impl RetainScratch for RegionSpares {
    fn clear_for_reuse(&mut self) {
        self.used.clear();
        self.position_of.clear();
        self.ring.clear();
        self.ring_ids.clear();
        self.path.clear();
        self.path_ids.clear();
    }

    fn retained_bytes(&self) -> usize {
        flat_vec_retained_bytes(&self.used)
            .saturating_add(flat_vec_retained_bytes(&self.position_of))
            .saturating_add(flat_vec_retained_bytes(&self.ring))
            .saturating_add(flat_vec_retained_bytes(&self.ring_ids))
            .saturating_add(flat_vec_retained_bytes(&self.path))
            .saturating_add(flat_vec_retained_bytes(&self.path_ids))
    }
}

thread_local! {
    static REGION_SPARES: ScratchPool<RegionSpares> = ScratchPool::new(RegionSpares::default());
}

/// Weight-typed construction buffers, pooled per `W` (the two real
/// instantiations are `i32` and `[i32; 2]`): edges and multiplicities
/// were the remaining per-row allocations after the W-independent set.
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

pub(crate) fn region_spares() -> ScratchLease<RegionSpares> {
    ScratchPool::checkout(&REGION_SPARES)
}

/// Construction-only reciprocal-edge index. It is separate from the core
/// lease so its capacity returns to the pool as soon as topology is finished.
#[derive(Default)]
pub(crate) struct TwinSlotSpares {
    pub(crate) by_endpoints: HashMap<(u32, u32), u32>,
}

impl RetainScratch for TwinSlotSpares {
    fn clear_for_reuse(&mut self) {
        self.by_endpoints.clear();
    }

    fn retained_bytes(&self) -> usize {
        hash_map_retained_bytes(&self.by_endpoints)
    }
}

thread_local! {
    static TWIN_SLOT_SPARES: ScratchPool<TwinSlotSpares> = ScratchPool::new(TwinSlotSpares::default());
}

pub(crate) fn twin_slot_spares() -> ScratchLease<TwinSlotSpares> {
    ScratchPool::checkout(&TWIN_SLOT_SPARES)
}

/// The six topology columns every constructor must produce — bundled so
/// [`Arrangement::finish_topology`] has one obvious signature.
pub(crate) struct Columns<W> {
    pub(crate) points: Vec<XY>,
    pub(crate) starts: Vec<u32>,
    pub(crate) targets: Vec<u32>,
    pub(crate) owners: Vec<u32>,
    pub(crate) twins: Vec<u32>,
    pub(crate) multiplicities: Vec<W>,
}

/// The pooled buffers [`Arrangement::finish_topology`] consumes.
pub(crate) struct FinishSpares {
    pub(crate) component_of: Vec<u32>,
    pub(crate) face_of: Vec<u32>,
    pub(crate) face_starts: Vec<u32>,
    pub(crate) face_slots: Vec<u32>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scratch_pool::SCRATCH_RETAIN_BYTES;

    fn clear_core_pool() {
        let mut scratch = arrangement_spares();
        scratch
            .points
            .reserve_exact(SCRATCH_RETAIN_BYTES / size_of::<XY>() + 1);
    }

    use std::mem::size_of;

    #[test]
    fn core_accounting_includes_every_vector_and_map() {
        let mut scratch = ArrangementSpares::default();
        scratch.points.reserve_exact(3);
        scratch.starts.reserve_exact(5);
        scratch.targets.reserve_exact(7);
        scratch.owners.reserve_exact(11);
        scratch.twins.reserve_exact(12);
        scratch.face_of.reserve_exact(13);
        scratch.face_starts.reserve_exact(17);
        scratch.face_slots.reserve_exact(19);
        scratch.component_of.reserve_exact(23);
        scratch.filled.reserve_exact(29);
        scratch.component_id_of_root.reserve_exact(31);
        scratch.face_cursor.reserve_exact(37);
        scratch.outer.reserve_exact(41);
        scratch.winding_queue.reserve_exact(43);
        scratch.ids.reserve(29);
        assert_eq!(
            scratch.retained_bytes(),
            flat_vec_retained_bytes(&scratch.points)
                .saturating_add(flat_vec_retained_bytes(&scratch.starts))
                .saturating_add(flat_vec_retained_bytes(&scratch.targets))
                .saturating_add(flat_vec_retained_bytes(&scratch.owners))
                .saturating_add(flat_vec_retained_bytes(&scratch.twins))
                .saturating_add(flat_vec_retained_bytes(&scratch.face_of))
                .saturating_add(flat_vec_retained_bytes(&scratch.face_starts))
                .saturating_add(flat_vec_retained_bytes(&scratch.face_slots))
                .saturating_add(hash_map_retained_bytes(&scratch.ids))
                .saturating_add(flat_vec_retained_bytes(&scratch.component_of))
                .saturating_add(flat_vec_retained_bytes(&scratch.filled))
                .saturating_add(flat_vec_retained_bytes(&scratch.component_id_of_root))
                .saturating_add(flat_vec_retained_bytes(&scratch.face_cursor))
                .saturating_add(flat_vec_retained_bytes(&scratch.outer))
                .saturating_add(
                    scratch
                        .winding_queue
                        .capacity()
                        .saturating_mul(std::mem::size_of::<u32>()),
                )
        );
    }

    #[test]
    fn each_arrangement_owner_reuses_medium_and_drops_huge() {
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

        {
            let mut region = region_spares();
            region.ring.reserve_exact(64);
        }
        let ((), allocations) = crate::test_support::allocation_count(|| {
            region_spares().ring.reserve_exact(64);
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
            let mut region = region_spares();
            region
                .ring
                .reserve_exact(SCRATCH_RETAIN_BYTES / size_of::<XY>() + 1);
        }
        assert_eq!(region_spares().ring.capacity(), 0);

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
        clear_core_pool();
        {
            let mut scratch = arrangement_spares();
            let entries = SCRATCH_RETAIN_BYTES
                / (size_of::<PointKey>() + size_of::<u32>() + size_of::<usize>())
                + 1;
            scratch.ids.reserve(entries);
            assert!(hash_map_retained_bytes(&scratch.ids) > SCRATCH_RETAIN_BYTES);
        }
        assert_eq!(arrangement_spares().ids.capacity(), 0);
    }
}
