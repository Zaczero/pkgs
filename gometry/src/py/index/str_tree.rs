//! Immutable STR-packed envelope tree — the bulk half of the spatial index.
//!
//! Built once with the sort-tile-recursive layout (one global x-sort, one
//! y-sort per strip, flat level arrays — no per-node heap graph). Liveness is
//! owned by the index row table; post-build inserts live in its small dynamic
//! overflow tree. Node fields are `pub(super)` — the nearest
//! frontier and the convex descent in `mod.rs` walk levels directly.

use rstar::{AABB, Envelope as _};

use crate::HeapSize;
use crate::py::index::IndexEntry;

/// STR node fan-out. 16 keeps the tree shallow (100k rows ≈ 6.7k nodes over
/// 4 levels) while each node's children still scan as one cache line burst.
pub(super) const NODE_CAP: usize = 16;

/// One packed node: its children occupy `children` in the level below
/// (level 0 nodes index the entry array instead).
#[derive(Clone, Debug)]
pub(super) struct StrNode {
    pub(super) envelope: AABB<[f64; 2]>,
    pub(super) children: std::ops::Range<u32>,
}

/// The immutable bulk tree. Each `entries[*].idx` is a stable build handle in
/// `0..initial_len()`; rows with no entry (skipped empties) still occupy a
/// handle in the row liveness table, so `remove` never searches the tree.
#[derive(Clone, Debug, Default)]
pub(super) struct StaticStrTree {
    /// STR-ordered entries (leaf children).
    entries: Vec<IndexEntry>,
    /// `levels[0]` = leaves; the last level is the root layer (length <=
    /// `NODE_CAP` — descents seed from it directly).
    levels: Vec<Vec<StrNode>>,
    initial_len: usize,
}

impl StaticStrTree {
    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(super) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    /// Bulk-load with the STR layout. `total_rows` is the input row count —
    /// each `entry.idx` is a stable handle in `0..total_rows`. `total_rows`
    /// is the tree/overflow handle boundary and spans skipped rows too.
    pub(super) fn build(entries: Vec<IndexEntry>, total_rows: usize) -> crate::error::Result<Self> {
        Self::build_with_len(entries, total_rows)
    }

    /// Build a dense bulk tree whose every input row has an entry. Packed
    /// arrays prove this at admission.
    pub(super) fn build_dense(entries: Vec<IndexEntry>) -> crate::error::Result<Self> {
        let total_rows = entries
            .iter()
            .map(|entry| entry.idx)
            .max()
            .map_or(0, |idx| idx + 1);
        Self::build_with_len(entries, total_rows)
    }

    fn build_with_len(
        mut entries: Vec<IndexEntry>,
        total_rows: usize,
    ) -> crate::error::Result<Self> {
        let entry_count = entries.len();
        let mut tree = Self {
            entries: Vec::new(),
            levels: Vec::new(),
            initial_len: total_rows,
        };
        if entry_count == 0 {
            return Ok(tree);
        }
        // Sort-tile-recursive: x-sort globally, slice into ~sqrt(leaves)
        // vertical strips, y-sort each strip; consecutive NODE_CAP runs
        // become leaves and every upper level re-chunks the one below
        // (already spatially ordered).
        let leaf_count = entry_count.div_ceil(NODE_CAP);
        let root = leaf_count.isqrt();
        let strips = root + usize::from(root * root != leaf_count);
        let strip_len = entry_count.div_ceil(strips).max(NODE_CAP);
        // Ordering by the coordinate SUM is ordering by the midpoint (the
        // halving is monotone), one add per key. At +-1e308 the sum can reach
        // +-inf, which `total_cmp` still orders deterministically — extreme
        // entries then tie at the ends, degrading only partition balance for
        // absurd-coordinate data, never query correctness (envelopes stay
        // exact; see `str_centers_stay_ordered_for_huge_finite_envelopes`).
        let center = |entry: &IndexEntry, axis: usize| {
            entry.envelope.lower()[axis] + entry.envelope.upper()[axis]
        };
        entries.sort_unstable_by(|a, b| center(a, 0).total_cmp(&center(b, 0)));
        for strip in entries.chunks_mut(strip_len) {
            strip.sort_unstable_by(|a, b| center(a, 1).total_cmp(&center(b, 1)));
        }
        let mut level = Vec::new();
        crate::boundary::try_reserve_vec(&mut level, leaf_count)?;
        for (node, chunk) in entries.chunks(NODE_CAP).enumerate() {
            crate::boundary::try_push_vec(&mut level, StrNode {
                envelope: chunk
                    .iter()
                    .map(|entry| entry.envelope)
                    .reduce(|a, b| a.merged(&b))
                    .expect("chunks are non-empty"),
                children: range_at(node, chunk.len()),
            })?;
        }
        tree.entries = entries;
        while level.len() > NODE_CAP {
            let next_count = level.len().div_ceil(NODE_CAP);
            let mut next = Vec::new();
            crate::boundary::try_reserve_vec(&mut next, next_count)?;
            for (node, chunk) in level.chunks(NODE_CAP).enumerate() {
                crate::boundary::try_push_vec(&mut next, StrNode {
                    envelope: chunk
                        .iter()
                        .map(|child| child.envelope)
                        .reduce(|a, b| a.merged(&b))
                        .expect("chunks are non-empty"),
                    children: range_at(node, chunk.len()),
                })?;
            }
            crate::boundary::try_push_vec(&mut tree.levels, level)?;
            level = next;
        }
        crate::boundary::try_push_vec(&mut tree.levels, level)?;
        Ok(tree)
    }

    /// Handles `0..initial_len()` belong to this tree; later handles are the
    /// index's overflow inserts.
    pub(super) const fn initial_len(&self) -> usize {
        self.initial_len
    }

    /// Every live entry, in STR storage order.
    pub(super) fn live_entries<'a>(
        &'a self,
        live: &'a [u8],
    ) -> impl Iterator<Item = &'a IndexEntry> {
        self.entries.iter().filter(|entry| live[entry.idx] != 0)
    }

    /// The root layer (empty tree → empty slice) — descent seeds.
    pub(super) fn roots(&self) -> &[StrNode] {
        self.levels.last().map_or(&[], Vec::as_slice)
    }

    /// The level index of [`StaticStrTree::roots`].
    pub(super) const fn root_level(&self) -> usize {
        self.levels.len().saturating_sub(1)
    }

    pub(super) fn node(&self, level: usize, index: u32) -> &StrNode {
        &self.levels[level][index as usize]
    }

    /// The level-0-children entries of a LEAF node.
    pub(super) fn leaf_entries(&self, node: &StrNode) -> &[IndexEntry] {
        &self.entries[node.children.start as usize..node.children.end as usize]
    }

    /// Live handles whose envelope intersects `query`.
    pub(super) fn each_intersecting(
        &self,
        live: &[u8],
        query: &AABB<[f64; 2]>,
        mut visit: impl FnMut(usize) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        self.descend(
            live,
            |envelope| intersects(envelope, query),
            |entry| {
                if intersects(&entry.envelope, query) {
                    visit(entry.idx)?;
                }
                Ok(())
            },
        )
    }

    /// Live handles whose envelope is fully contained in `query`.
    pub(super) fn each_contained(
        &self,
        live: &[u8],
        query: &AABB<[f64; 2]>,
        mut visit: impl FnMut(usize) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        self.descend(
            live,
            |envelope| intersects(envelope, query),
            |entry| {
                if contains_envelope(query, &entry.envelope) {
                    visit(entry.idx)?;
                }
                Ok(())
            },
        )
    }

    /// Live handles whose envelope lies within squared distance
    /// `max_distance_2` of `point` (rstar `locate_within_distance`
    /// semantics: the entry's envelope distance decides).
    pub(super) fn each_within_distance_2(
        &self,
        live: &[u8],
        point: [f64; 2],
        max_distance_2: f64,
        mut visit: impl FnMut(usize) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        self.descend(
            live,
            |envelope| distance_2(envelope, &point) <= max_distance_2,
            |entry| {
                if distance_2(&entry.envelope, &point) <= max_distance_2 {
                    visit(entry.idx)?;
                }
                Ok(())
            },
        )
    }

    /// Explicit-stack descent: `enter` prunes nodes, `leaf` sees every live
    /// entry of surviving leaves.
    ///
    /// The traversal stack is a recycled thread-local: a BATCHED query (`join`,
    /// `query(array)`, the spatial-join refine loop) runs this thousands of
    /// times back-to-back, so a fresh `Vec` per descent is measurable waste
    /// (~560 instr/query). Descents never nest (the `leaf` visitor only records
    /// ids), so one buffer per thread serves every call.
    fn descend(
        &self,
        live: &[u8],
        enter: impl Fn(&AABB<[f64; 2]>) -> bool,
        mut leaf: impl FnMut(&IndexEntry) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        thread_local! {
            static STACK: std::cell::RefCell<Vec<(usize, u32)>> =
                const { std::cell::RefCell::new(Vec::new()) };
        }
        STACK.with_borrow_mut(|stack| {
            stack.clear();
            let root_level = self.root_level();
            crate::try_reserve_vec(stack, self.roots().len())?;
            for node in 0..self.roots().len() as u32 {
                if enter(&self.node(root_level, node).envelope) {
                    crate::try_push_vec(stack, (root_level, node))?;
                }
            }
            while let Some((level, index)) = stack.pop() {
                let node = self.node(level, index);
                if level == 0 {
                    for entry in self.leaf_entries(node) {
                        if live[entry.idx] != 0 {
                            leaf(entry)?;
                        }
                    }
                    continue;
                }
                for child in node.children.clone() {
                    if enter(&self.node(level - 1, child).envelope) {
                        crate::try_push_vec(stack, (level - 1, child))?;
                    }
                }
            }
            Ok(())
        })
    }
}

#[inline]
fn intersects(left: &AABB<[f64; 2]>, right: &AABB<[f64; 2]>) -> bool {
    let left_lower = left.lower();
    let left_upper = left.upper();
    let right_lower = right.lower();
    let right_upper = right.upper();
    left_lower[0] <= right_upper[0]
        && left_lower[1] <= right_upper[1]
        && left_upper[0] >= right_lower[0]
        && left_upper[1] >= right_lower[1]
}

#[inline]
fn contains_envelope(outer: &AABB<[f64; 2]>, inner: &AABB<[f64; 2]>) -> bool {
    let outer_lower = outer.lower();
    let outer_upper = outer.upper();
    let inner_lower = inner.lower();
    let inner_upper = inner.upper();
    outer_lower[0] <= inner_lower[0]
        && outer_lower[1] <= inner_lower[1]
        && outer_upper[0] >= inner_upper[0]
        && outer_upper[1] >= inner_upper[1]
}

#[inline]
fn distance_2(envelope: &AABB<[f64; 2]>, point: &[f64; 2]) -> f64 {
    let lower = envelope.lower();
    let upper = envelope.upper();
    let dx = if point[0] < lower[0] {
        lower[0] - point[0]
    } else if point[0] > upper[0] {
        point[0] - upper[0]
    } else {
        0.0
    };
    let dy = if point[1] < lower[1] {
        lower[1] - point[1]
    } else if point[1] > upper[1] {
        point[1] - upper[1]
    } else {
        0.0
    };
    dx * dx + dy * dy
}

crate::heapless!(IndexEntry, StrNode);

impl HeapSize for StaticStrTree {
    fn heap_bytes(&self) -> usize {
        self.entries.heap_bytes() + self.levels.heap_bytes()
    }
}

/// The half-open child range of chunk `node` (every chunk before it is
/// full).
const fn range_at(node: usize, len: usize) -> std::ops::Range<u32> {
    let start = (node * NODE_CAP) as u32;
    start..start + len as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(idx: usize, lo: [f64; 2], hi: [f64; 2]) -> IndexEntry {
        IndexEntry {
            idx,
            envelope: AABB::from_corners(lo, hi),
        }
    }

    fn fixture(count: usize) -> Vec<IndexEntry> {
        // Deterministic scatter with varied extents.
        (0..count)
            .map(|idx| {
                let x = ((idx * 73) % 311) as f64;
                let y = ((idx * 131) % 257) as f64;
                let w = ((idx * 7) % 11) as f64 * 0.5;
                entry(idx, [x, y], [x + w, y + w * 0.5])
            })
            .collect()
    }

    fn brute<'a>(
        entries: &'a [IndexEntry],
        removed: &'a [usize],
        keep: impl Fn(&IndexEntry) -> bool + 'a,
    ) -> Vec<usize> {
        let mut ids: Vec<usize> = entries
            .iter()
            .filter(|entry| !removed.contains(&entry.idx) && keep(entry))
            .map(|entry| entry.idx)
            .collect();
        ids.sort_unstable();
        ids
    }

    fn sorted(mut ids: Vec<usize>) -> Vec<usize> {
        ids.sort_unstable();
        ids
    }

    #[test]
    fn brute_force_parity_across_sizes_and_tombstones() {
        for count in [0, 1, 15, 16, 17, 100, 1000] {
            let entries = fixture(count);
            let tree = StaticStrTree::build(entries.clone(), entries.len()).expect("tree");
            let removed: Vec<usize> = (0..count).filter(|idx| idx % 7 == 3).collect();
            let mut live = vec![1_u8; count];
            for &handle in &removed {
                live[handle] = 0;
            }
            assert_eq!(
                sorted(tree.live_entries(&live).map(|entry| entry.idx).collect()),
                brute(&entries, &removed, |_| true)
            );
            for query in [
                AABB::from_corners([50.0, 40.0], [180.0, 200.0]),
                AABB::from_corners([0.0, 0.0], [311.0, 257.0]),
                AABB::from_corners([400.0, 400.0], [500.0, 500.0]),
                AABB::from_point([73.0, 131.0]),
            ] {
                let mut hits = Vec::new();
                tree.each_intersecting(&live, &query, |idx| {
                    hits.push(idx);
                    Ok(())
                })
                .unwrap();
                assert_eq!(
                    sorted(hits),
                    brute(&entries, &removed, |entry| entry
                        .envelope
                        .intersects(&query))
                );
                let mut inside = Vec::new();
                tree.each_contained(&live, &query, |idx| {
                    inside.push(idx);
                    Ok(())
                })
                .unwrap();
                assert_eq!(
                    sorted(inside),
                    brute(&entries, &removed, |entry| query
                        .contains_envelope(&entry.envelope))
                );
            }
            let point = [120.0, 90.0];
            for radius_2 in [0.0, 100.0, 10_000.0] {
                let mut near = Vec::new();
                tree.each_within_distance_2(&live, point, radius_2, |idx| {
                    near.push(idx);
                    Ok(())
                })
                .unwrap();
                assert_eq!(
                    sorted(near),
                    brute(&entries, &removed, |entry| entry
                        .envelope
                        .distance_2(&point)
                        <= radius_2)
                );
            }
        }
    }

    #[test]
    fn str_centers_stay_ordered_for_huge_finite_envelopes() {
        // The sum sort key may hit +-inf at +-1e308; queries must stay exact
        // regardless (envelopes are never approximated, only the partition).
        let negative = entry(0, [-1.0e308, -1.0e308], [-9.0e307, -9.0e307]);
        let positive = entry(1, [9.0e307, 9.0e307], [1.0e308, 1.0e308]);

        let tree = StaticStrTree::build(vec![positive, negative], 2).expect("tree");
        let mut negative_hits = Vec::new();
        tree.each_intersecting(
            &[1, 1],
            &AABB::from_corners([-1.0e308, -1.0e308], [-9.0e307, -9.0e307]),
            |idx| {
                negative_hits.push(idx);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(negative_hits, [0]);
    }
}
