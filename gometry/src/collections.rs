//! Native collection builders, capacity planning, and shared hash policy.
//!
//! [`HashMap`]/[`HashSet`] use `ahash` (a fast, high-quality non-cryptographic
//! hasher) as the default for all crate-owned maps/sets, including public-path
//! grouping helpers such as `GeometryArray.dissolve`. The `…Ext` traits bring
//! `new`/`with_capacity`, so call sites read exactly like `std::collections`.
//!
//! **Hasher decision:** the dominant key is
//! [`PointKey`](crate::geometry::PointKey) (`{ x: u64, y: u64 }` — two
//! canonicalized f64 bit-words). Identity hashing (`nohash-hasher`) is
//! structurally impossible for a two-word key; `ahash` is the correct choice
//! for `PointKey`, `(PointKey, PointKey)`, tuple, `TypeId`, and other
//! multi-word struct keys. [`IndexHasher`] covers the other shape: one dense
//! integer the crate produced itself, such as a row-product chunk index. Those
//! keys need spreading, not strength, so it multiplies rather than hashing — a
//! `RandomState` SipHash of the index cost about a tenth of a scalar predicate
//! call. H3 `CellIndex` stays on `ahash`: it is a foreign `NonZero` wrapper.

use std::convert::Infallible;
use std::hash::{BuildHasher as _, BuildHasherDefault, Hash, Hasher};
use std::ops::ControlFlow;
use std::sync::{LazyLock, OnceLock};

use crate::work::Work;

pub(crate) mod csr;

pub(crate) use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};

/// Hasher for a dense integer key the crate produced itself.
///
/// One odd-constant multiply spreads the index across the table's control
/// bytes. Deliberately not collision-resistant: a key an outside caller can
/// choose belongs in an `ahash` map instead.
#[derive(Clone, Copy, Default)]
pub(crate) struct IndexHasher(u64);

/// Golden-ratio odd constant; the multiply carries low bits into the high ones
/// the table reads for its control byte.
const INDEX_HASH_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;

impl Hasher for IndexHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(INDEX_HASH_MULTIPLIER);
        }
    }

    fn write_usize(&mut self, value: usize) {
        self.0 = (value as u64).wrapping_mul(INDEX_HASH_MULTIPLIER);
    }
}

/// [`std::collections::HashMap`] keyed by a crate-produced integer.
pub(crate) type IndexMap<K, V> = std::collections::HashMap<K, V, BuildHasherDefault<IndexHasher>>;

static COLLECTION_HASH_STATE: OnceLock<ahash::RandomState> = OnceLock::new();

pub(crate) fn collection_hash_state() -> crate::error::Result<&'static ahash::RandomState> {
    collection_hash_state_with(&COLLECTION_HASH_STATE, getrandom::fill)
}

/// Construct a crate-owned hash map with the fallible process hash state and
/// no capacity reservation.
pub(crate) fn try_hash_map<K, V>() -> crate::error::Result<HashMap<K, V>>
where
    K: Eq + Hash,
{
    Ok(HashMap::with_hasher(collection_hash_state()?.clone()))
}

/// Construct a crate-owned hash set with the fallible process hash state and
/// no capacity reservation.
pub(crate) fn try_hash_set<T>() -> crate::error::Result<HashSet<T>>
where
    T: Eq + Hash,
{
    Ok(HashSet::with_hasher(collection_hash_state()?.clone()))
}

/// Construct a crate-owned hash map with the fallible process hash state and
/// reserve its validated element count before the caller inserts anything.
pub(crate) fn try_hash_map_with_capacity<K, V>(
    capacity: usize,
) -> crate::error::Result<HashMap<K, V>>
where
    K: Eq + Hash,
{
    let mut map = HashMap::with_hasher(collection_hash_state()?.clone());
    map.reserve(capacity);
    Ok(map)
}

/// Construct a crate-owned hash set with the fallible process hash state and
/// reserve its validated element count before the caller inserts anything.
pub(crate) fn try_hash_set_with_capacity<T>(capacity: usize) -> crate::error::Result<HashSet<T>>
where
    T: Eq + Hash,
{
    let mut set = HashSet::with_hasher(collection_hash_state()?.clone());
    set.reserve(capacity);
    Ok(set)
}

fn collection_hash_state_with(
    state: &OnceLock<ahash::RandomState>,
    fill: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>,
) -> crate::error::Result<&ahash::RandomState> {
    state.get_or_try_init(|| {
        let mut bytes = [[0_u8; 8]; 4];
        fill(bytes.as_flattened_mut()).map_err(|_| crate::error::ResourceError::Entropy)?;
        let [first, second, third, fourth] = bytes;
        Ok(ahash::RandomState::with_seeds(
            u64::from_ne_bytes(first),
            u64::from_ne_bytes(second),
            u64::from_ne_bytes(third),
            u64::from_ne_bytes(fourth),
        ))
    })
}

/// One process-stable `ahash` state for Python `__hash__` implementations.
/// Python requires only intra-process consistency (str hashes vary per run
/// too), and `ahash` walks large payloads — array hashes visit every
/// coordinate — far faster than `DefaultHasher`'s `SipHash`.
static PY_HASH_STATE: LazyLock<ahash::RandomState> = LazyLock::new(ahash::RandomState::new);

/// One-shot hash for a Python `__hash__` implementation.
pub(crate) fn python_hash<T: std::hash::Hash + ?Sized>(value: &T) -> u64 {
    PY_HASH_STATE.hash_one(value)
}

/// Streaming hasher for Python `__hash__` implementations that fold many
/// pieces (e.g. every row of an array); same state as [`python_hash`].
pub(crate) fn python_hasher() -> impl std::hash::Hasher {
    PY_HASH_STATE.build_hasher()
}

/// Minimal union-find for cycle detection over small node sets (the
/// validity ring-touch graph, clustering components): path-halving find,
/// rank-union. `union` reports whether the two nodes were ALREADY
/// connected — adding such an edge closes a cycle.
pub(crate) struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl UnionFind {
    pub(crate) fn new(nodes: usize) -> Self {
        Self {
            parent: (0..nodes).collect(),
            rank: vec![0; nodes],
        }
    }

    pub(crate) fn new_with_work<W: Work>(nodes: usize, work: &mut W) -> crate::error::Result<Self> {
        work.charge_product_into_error(nodes, 2)?;
        Ok(Self::new(nodes))
    }

    pub(crate) fn find_with_work<W: Work>(
        &mut self,
        node: usize,
        work: &mut W,
    ) -> crate::error::Result<usize> {
        let mut charge = |ticks| match work.charge_into_error(ticks) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        };
        match self.find_with_charge(node, &mut charge) {
            ControlFlow::Continue(root) => Ok(root),
            ControlFlow::Break(error) => Err(error),
        }
    }

    fn find_with_charge<E>(
        &mut self,
        mut node: usize,
        charge: &mut impl FnMut(u64) -> ControlFlow<E, ()>,
    ) -> ControlFlow<E, usize> {
        loop {
            charge(1)?;
            let parent = *self.parent.get(node).expect("union-find node exists");
            if parent == node {
                return ControlFlow::Continue(node);
            }
            let grandparent = *self.parent.get(parent).expect("union-find parent exists");
            charge(1)?;
            *self.parent.get_mut(node).expect("union-find node exists") = grandparent;
            node = grandparent;
        }
    }

    /// Append one fresh singleton node and return its id.
    pub(crate) fn push(&mut self) -> usize {
        let node = self.parent.len();
        self.parent.push(node);
        self.rank.push(0);
        node
    }

    /// Join `a` and `b`; `true` when they were ALREADY connected — adding
    /// such an edge closes a cycle.
    pub(crate) fn union(&mut self, a: usize, b: usize) -> bool {
        let mut charge = |_: u64| ControlFlow::<Infallible, ()>::Continue(());
        match self.union_with_charge(a, b, &mut charge) {
            ControlFlow::Continue(already_connected) => already_connected,
            ControlFlow::Break(never) => match never {},
        }
    }

    /// Join `a` and `b` while charging path-halving visits and root updates
    /// before the corresponding union-find progress.
    pub(crate) fn union_with_work<W: Work>(
        &mut self,
        a: usize,
        b: usize,
        work: &mut W,
    ) -> crate::error::Result<bool> {
        let mut charge = |ticks| match work.charge_into_error(ticks) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        };
        match self.union_with_charge(a, b, &mut charge) {
            ControlFlow::Continue(already_connected) => Ok(already_connected),
            ControlFlow::Break(error) => Err(error),
        }
    }

    fn union_with_charge<E>(
        &mut self,
        a: usize,
        b: usize,
        charge: &mut impl FnMut(u64) -> ControlFlow<E, ()>,
    ) -> ControlFlow<E, bool> {
        let root_a = self.find_with_charge(a, charge)?;
        let root_b = self.find_with_charge(b, charge)?;
        charge(1)?;
        if root_a == root_b {
            return ControlFlow::Continue(true);
        }
        let rank_a = *self.rank.get(root_a).expect("union-find root exists");
        let rank_b = *self.rank.get(root_b).expect("union-find root exists");
        let (big, small) = if rank_a >= rank_b {
            (root_a, root_b)
        } else {
            (root_b, root_a)
        };
        charge(if rank_a == rank_b { 2 } else { 1 })?;
        *self.parent.get_mut(small).expect("union-find root exists") = big;
        if rank_a == rank_b {
            // Rank r requires at least 2^r nodes, so rank is below usize::BITS.
            *self.rank.get_mut(big).expect("union-find root exists") = rank_a.wrapping_add(1);
        }
        ControlFlow::Continue(false)
    }
}

/// Sort the row ids in `ids[start..]` ascending and keep one of each.
///
/// A tail holding at least one id per 256 of `universe` goes through a bitmap
/// in `O(universe / 64 + n)`, which also drops repeats; a sparser one is
/// sorted by comparison.
pub(crate) fn sort_unique_row_ids(ids: &mut Vec<usize>, start: usize, universe: usize) {
    let tail = ids
        .get_mut(start..)
        .expect("row-id tail starts within output");
    if tail.len() < 2 {
        return;
    }
    let unique = if tail.len().saturating_mul(256) < universe {
        tail.sort_unstable();
        tail.partition_dedup().0.len()
    } else {
        let mut bitmap = vec![0_u64; universe.div_ceil(64)];
        for &id in tail.iter() {
            *bitmap.get_mut(id / 64).expect("row id is within universe") |= 1 << (id % 64);
        }
        let mut output = tail.iter_mut();
        for (word_index, mut word) in bitmap.into_iter().enumerate() {
            while word != 0 {
                *output.next().expect("unique rows fit in the input tail") = word_index
                    .wrapping_mul(64)
                    .wrapping_add(word.trailing_zeros() as usize);
                word &= word.wrapping_sub(1);
            }
        }
        let remaining = output.len();
        tail.len().wrapping_sub(remaining)
    };
    ids.truncate(start.wrapping_add(unique));
}

#[cfg(test)]
mod row_id_sort_tests {
    use crate::error::{ErrorKind, ResourceError};
    use crate::work::Work;

    struct TickLimit {
        limit: u64,
        spent: u64,
    }

    impl Work for TickLimit {
        type Error = crate::error::Error;

        fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
            let next = self
                .spent
                .checked_add(ticks)
                .ok_or(ResourceError::SizeOverflow)?;
            if next > self.limit {
                return Err(ResourceError::SizeOverflow.into());
            }
            self.spent = next;
            Ok(())
        }

        fn charge_product(
            &mut self,
            count: usize,
            ticks_per_item: usize,
        ) -> crate::error::Result<()> {
            self.charge(crate::work::work_product_ticks(count, ticks_per_item)?)
        }
    }

    #[test]
    fn union_find_detects_cycles_after_merging_and_appending_components() {
        let mut graph = super::UnionFind::new(6);
        for (a, b) in [(0, 1), (2, 3), (4, 5), (0, 2), (4, 0)] {
            assert!(!graph.union(a, b));
        }
        assert!(graph.union(1, 5));
        let appended = graph.push();
        assert!(!graph.union(appended, 3));
        assert!(graph.union(5, appended));
        assert!(graph.union(appended, appended));
    }

    #[test]
    fn synthetic_work_error_precedes_union_find_path_halving() {
        let mut graph = super::UnionFind::new(4);
        assert!(!graph.union(0, 1));
        assert!(!graph.union(2, 3));
        assert!(!graph.union(0, 2));
        assert_eq!(graph.parent, [0, 0, 0, 2]);

        let mut work = TickLimit { limit: 1, spent: 0 };
        let error = graph.find_with_work(3, &mut work).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
        assert_eq!(work.spent, 1);
        assert_eq!(graph.parent, [0, 0, 0, 2]);
    }

    #[test]
    fn synthetic_work_error_precedes_union_find_parent_and_rank_updates() {
        let mut graph = super::UnionFind::new(2);
        let mut work = TickLimit { limit: 3, spent: 0 };
        let error = graph.union_with_work(0, 1, &mut work).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
        assert_eq!(work.spent, 3);
        assert_eq!(graph.parent, [0, 1]);
        assert_eq!(graph.rank, [0, 0]);
    }

    #[test]
    fn cold_collection_hash_state_needs_no_rust_heap_allocation() {
        let ((), allocations) = crate::test_support::allocation_count(|| {
            let state = super::collection_hash_state().unwrap();
            let map = super::HashMap::<u64, u64>::with_hasher(state.clone());
            assert_eq!(map.capacity(), 0);
        });
        assert_eq!(allocations, 0);
    }

    #[test]
    fn hash_table_try_reserve_reports_unrepresentable_layouts() {
        let mut map = super::try_hash_map::<u64, u64>().unwrap();
        assert!(map.try_reserve(usize::MAX).is_err());

        let mut set = super::try_hash_set::<u64>().unwrap();
        assert!(set.try_reserve(usize::MAX).is_err());
    }

    #[test]
    fn collection_entropy_failure_remains_retryable_and_maps_to_os_error() {
        use crate::error::{ErrorKind, ResourceError};
        pyo3::Python::initialize();
        let state = std::sync::OnceLock::new();
        let error =
            super::collection_hash_state_with(&state, |_| Err(getrandom::Error::UNSUPPORTED))
                .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Entropy)
        ));
        assert!(state.get().is_none());
        pyo3::Python::attach(|py| {
            let error: pyo3::PyErr = error.into();
            assert!(error.is_instance_of::<pyo3::exceptions::PyOSError>(py));
        });
        let first = super::collection_hash_state_with(&state, getrandom::fill).unwrap();
        let reused =
            super::collection_hash_state_with(&state, |_| panic!("initialized entropy reused"))
                .unwrap();
        assert!(std::ptr::eq(first, reused));
    }

    #[test]
    fn row_id_tails_sort_and_drop_repeats_on_both_paths() {
        let mut sparse = vec![42, 9, 1, 7, 9, 3, 1];
        super::sort_unique_row_ids(&mut sparse, 1, 10_000);
        assert_eq!(sparse, [42, 1, 3, 7, 9]);

        let mut dense = vec![511, 4, 255, 1023, 64, 4, 65, 0, 511];
        super::sort_unique_row_ids(&mut dense, 0, 1024);
        assert_eq!(dense, [0, 4, 64, 65, 255, 511, 1023]);
    }
}

/// Initial reservation bound for advisory Rust iterator and serde size hints.
pub(crate) const MAX_RESERVE_HINT: usize = 1 << 12; // 4096 elements

/// Reserve from an untrusted **hint** without letting the hint affect
/// validity. The reservation is clamped to both the advisory bound and `T`'s
/// remaining addressable layout.
pub(crate) fn reserve_hint<T>(vec: &mut Vec<T>, additional: usize) {
    let element_size = std::mem::size_of::<T>();
    let max_capacity = (isize::MAX as usize)
        .checked_div(element_size)
        .unwrap_or(usize::MAX);
    let additional = additional
        .min(MAX_RESERVE_HINT)
        .min(max_capacity.saturating_sub(vec.len()));
    vec.reserve(additional);
}

#[cfg(test)]
mod capacity_tests {
    use super::{MAX_RESERVE_HINT, reserve_hint};

    #[test]
    fn reserve_hint_clamps_instead_of_rejecting() {
        let mut v: Vec<u8> = Vec::new();
        reserve_hint(&mut v, MAX_RESERVE_HINT + 1);
        assert!(v.capacity() <= MAX_RESERVE_HINT);
        assert!(v.capacity() > 0);
    }
}
