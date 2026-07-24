#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::*;
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
    fn take_typed_spares() -> TypedSpares<Self>;
    fn restore_typed_spares(spares: TypedSpares<Self>);
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

    fn take_typed_spares() -> TypedSpares<Self> {
        I32_TYPED_SPARES.with(std::cell::RefCell::take)
    }

    fn restore_typed_spares(spares: TypedSpares<Self>) {
        I32_TYPED_SPARES.with(|cell| *cell.borrow_mut() = spares);
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

    fn take_typed_spares() -> TypedSpares<Self> {
        PAIR_TYPED_SPARES.with(std::cell::RefCell::take)
    }

    fn restore_typed_spares(spares: TypedSpares<Self>) {
        PAIR_TYPED_SPARES.with(|cell| *cell.borrow_mut() = spares);
    }
}

/// One face of the arrangement: its boundary ring (closed, first point
/// repeated), exact signed-area decision (orientation + magnitude for
/// outer-face seeding), and component id.
pub(crate) struct Face {
    /// Materialized only by [`Arrangement::new_with_rings`] — empty under
    /// every other constructor (winding consumers walk boundaries through
    /// [`Arrangement::region_rings`] instead).
    pub ring: Vec<XY>,
    pub decision_area: RingDecisionArea,
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
    pub(crate) face_of: Vec<u32>,
    pub(crate) face_starts: Vec<u32>,
    pub(crate) face_slots: Vec<u32>,
    pub(crate) ids: HashMap<PointKey, u32>,
    pub(crate) component_of: Vec<u32>,
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

thread_local! {
    static REGION_SPARES: std::cell::RefCell<RegionSpares> =
        std::cell::RefCell::new(RegionSpares::default());
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

thread_local! {
    static I32_TYPED_SPARES: std::cell::RefCell<TypedSpares<i32>> =
        std::cell::RefCell::new(TypedSpares::default());
    static PAIR_TYPED_SPARES: std::cell::RefCell<TypedSpares<[i32; 2]>> =
        std::cell::RefCell::new(TypedSpares::default());
}

pub(crate) fn take_typed_spares<W: WindingWeight>() -> TypedSpares<W> {
    W::take_typed_spares()
}

pub(crate) fn restore_typed_spares<W: WindingWeight>(spares: TypedSpares<W>) {
    W::restore_typed_spares(spares);
}

thread_local! {
    static SPARES: std::cell::RefCell<ArrangementSpares> =
        std::cell::RefCell::new(ArrangementSpares::default());
}

pub(crate) fn arrangement_spares_with<R>(
    f: impl FnOnce(&std::cell::RefCell<ArrangementSpares>) -> R,
) -> R {
    SPARES.with(f)
}

pub(crate) fn region_spares_with<R>(f: impl FnOnce(&std::cell::RefCell<RegionSpares>) -> R) -> R {
    REGION_SPARES.with(f)
}

/// The five topology columns every constructor must produce — bundled so
/// [`Arrangement::finish_topology`] has one obvious signature.
pub(crate) struct Columns<W> {
    pub(crate) points: Vec<XY>,
    pub(crate) starts: Vec<u32>,
    pub(crate) targets: Vec<u32>,
    pub(crate) owners: Vec<u32>,
    pub(crate) multiplicities: Vec<W>,
}

/// The pooled buffers [`Arrangement::finish_topology`] consumes.
pub(crate) struct FinishSpares {
    pub(crate) component_of: Vec<u32>,
    pub(crate) face_of: Vec<u32>,
    pub(crate) face_starts: Vec<u32>,
    pub(crate) face_slots: Vec<u32>,
}

impl<W: WindingWeight> Drop for Arrangement<W> {
    fn drop(&mut self) {
        // Return the W-independent buffers for the next row. Capacity is
        // what we recycle; contents are stale by definition.
        SPARES.with(|cell| {
            let mut spares = cell.borrow_mut();
            spares.points = std::mem::take(&mut self.points);
            spares.starts = std::mem::take(&mut self.starts);
            spares.targets = std::mem::take(&mut self.targets);
            spares.owners = std::mem::take(&mut self.owners);
            spares.face_of = std::mem::take(&mut self.face_of);
            spares.face_starts = std::mem::take(&mut self.face_starts);
            spares.face_slots = std::mem::take(&mut self.face_slots);
            spares.ids = std::mem::take(&mut self.ids);
            spares.component_of = std::mem::take(&mut self.component_of);
        });
        let mut multiplicities = std::mem::take(&mut self.multiplicities);
        multiplicities.clear();
        let edges = take_typed_spares::<W>().edges;
        restore_typed_spares::<W>(TypedSpares {
            edges,
            multiplicities,
        });
    }
}

/// The arrangement over one noded segment set.
pub(crate) struct Arrangement<W: WindingWeight = i32> {
    pub(crate) points: Vec<XY>,
    /// CSR adjacency: neighbors of vertex `v` are
    /// `targets[starts[v]..starts[v+1]]`, CCW by departure angle.
    pub(crate) starts: Vec<u32>,
    pub(crate) targets: Vec<u32>,
    /// Source vertex per half-edge slot (the CSR row owner).
    pub(crate) owners: Vec<u32>,
    /// Net directed multiplicity per half-edge slot: each source segment's
    /// weight added running with the half-edge, subtracted against.
    pub(crate) multiplicities: Vec<W>,
    /// Face id per half-edge slot.
    pub(crate) face_of: Vec<u32>,
    /// CSR: the half-edge slots of face `f` are
    /// `face_slots[face_starts[f]..face_starts[f + 1]]`.
    pub(crate) face_starts: Vec<u32>,
    pub(crate) face_slots: Vec<u32>,
    pub(crate) faces: Vec<Face>,
    /// Vertex id by coordinate key (the construction dedup map, kept for
    /// consumer lookups).
    pub(crate) ids: HashMap<PointKey, u32>,
    /// Component id per vertex.
    pub(crate) component_of: Vec<u32>,
    pub(crate) component_count: u32,
}
