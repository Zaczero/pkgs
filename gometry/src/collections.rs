#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
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
//! multi-word struct keys. `nohash` would only apply to a single-integer key
//! (none exists today; H3 `CellIndex` is a foreign `NonZero` wrapper without an
//! `IsEnabled` impl).

use std::hash::BuildHasher;
use std::sync::LazyLock;

pub(crate) use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};

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

/// Sort unique row indices in `0..universe` in ascending order.
///
/// Uses a bitmap pass when the id set is dense enough (`O(universe/64 + n)`),
/// otherwise falls back to `sort_unstable`.
pub(crate) fn sort_row_ids(ids: &mut [usize], universe: usize) {
    if ids.len().saturating_mul(256) < universe {
        ids.sort_unstable();
        return;
    }
    let mut bitmap = vec![0_u64; universe.div_ceil(64)];
    for &id in ids.iter() {
        bitmap[id / 64] |= 1 << (id % 64);
    }
    // Ids are unique, so the bitmap pass writes back exactly `ids.len()`
    // values — sorting works in place on any slice (CSR row windows too).
    let mut write = 0;
    for (word_index, &word) in bitmap.iter().enumerate() {
        let mut word = word;
        while word != 0 {
            ids[write] = word_index * 64 + word.trailing_zeros() as usize;
            write += 1;
            word &= word - 1;
        }
    }
    debug_assert_eq!(write, ids.len());
}
