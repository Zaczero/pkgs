//! Project-wide hash collections — one place that picks the hasher so every map
//! and set in the crate is consistent.
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

use std::hash::{BuildHasher as _, BuildHasherDefault, Hash, Hasher};
use std::sync::{LazyLock, OnceLock};

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
/// reserve its validated element count before the caller inserts anything.
pub(crate) fn try_hash_map_with_capacity<K, V>(
    capacity: usize,
) -> crate::error::Result<HashMap<K, V>>
where
    K: Eq + Hash,
{
    let mut map = HashMap::with_hasher(collection_hash_state()?.clone());
    map.try_reserve(capacity)?;
    Ok(map)
}

/// Construct a crate-owned hash set with the fallible process hash state and
/// reserve its validated element count before the caller inserts anything.
pub(crate) fn try_hash_set_with_capacity<T>(capacity: usize) -> crate::error::Result<HashSet<T>>
where
    T: Eq + Hash,
{
    let mut set = HashSet::with_hasher(collection_hash_state()?.clone());
    set.try_reserve(capacity)?;
    Ok(set)
}

fn collection_hash_state_with(
    state: &OnceLock<ahash::RandomState>,
    fill: impl FnOnce(&mut [u8]) -> Result<(), getrandom::Error>,
) -> crate::error::Result<&ahash::RandomState> {
    state.get_or_try_init(|| {
        let mut bytes = [0_u8; 32];
        fill(&mut bytes).map_err(|_| crate::error::ResourceError::Entropy)?;
        let Some(&[first, second, third, fourth]) = bytes.as_chunks::<8>().0.first_chunk::<4>()
        else {
            return Err(crate::error::ResourceError::Entropy.into());
        };
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
/// size-union. `union` reports whether the two nodes were ALREADY
/// connected — adding such an edge closes a cycle.
pub(crate) struct UnionFind {
    parent: Vec<usize>,
    size: Vec<u32>,
}

impl UnionFind {
    pub(crate) fn new(nodes: usize) -> Self {
        Self {
            parent: (0..nodes).collect(),
            size: vec![1; nodes],
        }
    }

    pub(crate) fn find(&mut self, mut node: usize) -> usize {
        while self.parent[node] != node {
            self.parent[node] = self.parent[self.parent[node]];
            node = self.parent[node];
        }
        node
    }

    /// Append one fresh singleton node, returning nothing (its id is the
    /// previous node count).
    pub(crate) fn push(&mut self) {
        self.parent.push(self.parent.len());
        self.size.push(1);
    }

    /// Join `a` and `b`; `true` when they were ALREADY connected — adding
    /// such an edge closes a cycle.
    pub(crate) fn union(&mut self, a: usize, b: usize) -> bool {
        let (root_a, root_b) = (self.find(a), self.find(b));
        if root_a == root_b {
            return true;
        }
        let (big, small) = if self.size[root_a] >= self.size[root_b] {
            (root_a, root_b)
        } else {
            (root_b, root_a)
        };
        self.parent[small] = big;
        self.size[big] += self.size[small];
        false
    }
}

/// Sort the row ids in `ids[start..]` ascending and keep one of each.
///
/// A tail holding at least one id per 256 of `universe` goes through a bitmap
/// in `O(universe / 64 + n)`, which also drops repeats; a sparser one is
/// sorted by comparison.
pub(crate) fn sort_unique_row_ids(ids: &mut Vec<usize>, start: usize, universe: usize) {
    let tail = &mut ids[start..];
    if tail.len() < 2 {
        return;
    }
    let unique = if tail.len().saturating_mul(256) < universe {
        tail.sort_unstable();
        tail.partition_dedup().0.len()
    } else {
        let mut bitmap = vec![0_u64; universe.div_ceil(64)];
        for &id in tail.iter() {
            bitmap[id / 64] |= 1 << (id % 64);
        }
        let mut write = 0;
        for (word_index, mut word) in bitmap.into_iter().enumerate() {
            while word != 0 {
                tail[write] = word_index * 64 + word.trailing_zeros() as usize;
                write += 1;
                word &= word - 1;
            }
        }
        write
    };
    ids.truncate(start + unique);
}

#[cfg(test)]
mod row_id_sort_tests {

    #[test]
    fn cold_collection_hash_state_needs_no_rust_heap_allocation() {
        crate::test_support::fail_allocations_after(0, || {
            let state = super::collection_hash_state().unwrap();
            let map = super::HashMap::<u64, u64>::with_hasher(state.clone());
            assert_eq!(map.capacity(), 0);
        });
    }

    #[test]
    fn collection_entropy_failure_remains_retryable_and_maps_to_os_error() {
        use crate::error::{ErrorKind, ResourceError};
        use crate::test_support::fail_allocations_after;

        pyo3::Python::initialize();
        let state = std::sync::OnceLock::new();
        let error = fail_allocations_after(0, || {
            super::collection_hash_state_with(&state, |_| Err(getrandom::Error::UNSUPPORTED))
        })
        .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Entropy)
        ));
        assert!(state.get().is_none());
        pyo3::Python::attach(|py| {
            let error: pyo3::PyErr = fail_allocations_after(0, || error.into());
            assert!(error.is_instance_of::<pyo3::exceptions::PyOSError>(py));
        });
        let first = fail_allocations_after(0, || {
            super::collection_hash_state_with(&state, getrandom::fill)
        })
        .unwrap();
        let reused =
            super::collection_hash_state_with(&state, |_| panic!("initialized entropy reused"))
                .unwrap();
        assert!(std::ptr::eq(first, reused));
    }

    #[test]
    fn fallible_hash_collections_return_allocation_errors_and_retry() {
        use crate::error::{ErrorKind, ResourceError};

        let map = crate::test_support::fail_allocations_after(0, || {
            super::try_hash_map_with_capacity::<u64, u64>(8)
        });
        assert!(matches!(
            map.unwrap_err().kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        let set = crate::test_support::fail_allocations_after(0, || {
            super::try_hash_set_with_capacity::<u64>(8)
        });
        assert!(matches!(
            set.unwrap_err().kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        super::try_hash_map_with_capacity::<u64, u64>(8).unwrap();
        super::try_hash_set_with_capacity::<u64>(8).unwrap();
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
