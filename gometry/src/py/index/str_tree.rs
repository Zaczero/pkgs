//! Immutable STR-packed envelope tree — the bulk half of the spatial index.
//!
//! Built once with the sort-tile-recursive layout (one global x-sort, one
//! y-sort per strip, flat level arrays — no per-node heap graph), queried
//! with explicit-stack descents, and "mutated" only by tombstoning: removed
//! handles flip a bitmap bit, post-build inserts live in the index's small
//! dynamic overflow tree. Node fields are `pub(super)` — the nearest
//! frontier and the convex descent in `mod.rs` walk levels directly.

use rstar::{AABB, Envelope};

use super::IndexEntry;
use crate::HeapSize;

/// STR node fan-out. 16 keeps the tree shallow (100k rows ≈ 6.7k nodes over
/// 4 levels) while each node's children still scan as one cache line burst.
const NODE_CAP: usize = 16;

/// One packed node: its children occupy `children` in the level below
/// (level 0 nodes index the entry array instead).
#[derive(Clone, Debug)]
pub(super) struct StrNode {
    pub(super) envelope: AABB<[f64; 2]>,
    pub(super) children: std::ops::Range<u32>,
}

/// The immutable bulk tree. Each `entries[*].idx` is a stable build handle in
/// `0..initial_len()`; rows with no entry (skipped empties) are pre-tombstoned
/// but still occupy a handle, so tombstones index by HANDLE — `remove` never
/// searches the tree.
#[derive(Clone, Debug, Default)]
pub(super) struct StaticStrTree {
    /// STR-ordered entries (leaf children).
    entries: Vec<IndexEntry>,
    /// `levels[0]` = leaves; the last level is the root layer (length <=
    /// `NODE_CAP` — descents seed from it directly).
    levels: Vec<Vec<StrNode>>,
    removed: Vec<bool>,
    live: usize,
}

impl StaticStrTree {
    pub(super) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    /// Bulk-load with the STR layout. `total_rows` is the input row count —
    /// each `entry.idx` is a stable handle in `0..total_rows`. Rows with no
    /// entry (e.g. skipped empty geometries) are pre-tombstoned, so `removed`
    /// stays sized to `total_rows`: that length is also the tree/overflow handle
    /// boundary ([`initial_len`](Self::initial_len)) and the index for
    /// `removed[entry.idx]`, both of which must span every input row, not just
    /// the indexed ones.
    pub(super) fn build(mut entries: Vec<IndexEntry>, total_rows: usize) -> Self {
        let entry_count = entries.len();
        let mut removed = vec![true; total_rows];
        for entry in &entries {
            removed[entry.idx] = false;
        }
        let mut tree = Self {
            entries: Vec::new(),
            levels: Vec::new(),
            removed,
            live: entry_count,
        };
        if entry_count == 0 {
            return tree;
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
        let mut level: Vec<StrNode> = entries
            .chunks(NODE_CAP)
            .enumerate()
            .map(|(node, chunk)| StrNode {
                envelope: chunk
                    .iter()
                    .map(|entry| entry.envelope)
                    .reduce(|a, b| a.merged(&b))
                    .expect("chunks are non-empty"),
                children: range_at(node, chunk.len()),
            })
            .collect();
        tree.entries = entries;
        while level.len() > NODE_CAP {
            let next: Vec<StrNode> = level
                .chunks(NODE_CAP)
                .enumerate()
                .map(|(node, chunk)| StrNode {
                    envelope: chunk
                        .iter()
                        .map(|child| child.envelope)
                        .reduce(|a, b| a.merged(&b))
                        .expect("chunks are non-empty"),
                    children: range_at(node, chunk.len()),
                })
                .collect();
            tree.levels.push(level);
            level = next;
        }
        tree.levels.push(level);
        tree
    }

    /// Handles `0..initial_len()` belong to this tree; later handles are the
    /// index's overflow inserts.
    pub(super) const fn initial_len(&self) -> usize {
        self.removed.len()
    }

    pub(super) const fn live(&self) -> usize {
        self.live
    }

    /// Tombstone a build handle. `true` when it was live.
    pub(super) fn tombstone(&mut self, handle: usize) -> bool {
        let slot = &mut self.removed[handle];
        if *slot {
            return false;
        }
        *slot = true;
        self.live -= 1;
        true
    }

    /// Every live entry, in STR storage order.
    pub(super) fn live_entries(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries.iter().filter(|entry| !self.removed[entry.idx])
    }

    pub(super) fn is_live(&self, handle: usize) -> bool {
        !self.removed[handle]
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

    /// Every live handle beneath `node` (the convex descent's whole-subtree
    /// accept).
    pub(super) fn collect_subtree(&self, level: usize, node: &StrNode, out: &mut Vec<usize>) {
        if level == 0 {
            out.extend(self.leaf_entries(node).iter().filter_map(|entry| {
                let live = !self.removed[entry.idx];
                live.then_some(entry.idx)
            }));
            return;
        }
        let range = node.children.start as usize..node.children.end as usize;
        for child in &self.levels[level - 1][range] {
            self.collect_subtree(level - 1, child, out);
        }
    }

    /// Live handles whose envelope intersects `query`.
    pub(super) fn each_intersecting(&self, query: &AABB<[f64; 2]>, mut visit: impl FnMut(usize)) {
        self.descend(
            |envelope| envelope.intersects(query),
            |entry| {
                if entry.envelope.intersects(query) {
                    visit(entry.idx);
                }
            },
        );
    }

    /// Live handles whose envelope is fully contained in `query`.
    pub(super) fn each_contained(&self, query: &AABB<[f64; 2]>, mut visit: impl FnMut(usize)) {
        self.descend(
            |envelope| envelope.intersects(query),
            |entry| {
                if query.contains_envelope(&entry.envelope) {
                    visit(entry.idx);
                }
            },
        );
    }

    /// Live handles whose envelope lies within squared distance
    /// `max_distance_2` of `point` (rstar `locate_within_distance`
    /// semantics: the entry's envelope distance decides).
    pub(super) fn each_within_distance_2(
        &self,
        point: [f64; 2],
        max_distance_2: f64,
        mut visit: impl FnMut(usize),
    ) {
        self.descend(
            |envelope| envelope.distance_2(&point) <= max_distance_2,
            |entry| {
                if entry.envelope.distance_2(&point) <= max_distance_2 {
                    visit(entry.idx);
                }
            },
        );
    }

    /// Explicit-stack descent: `enter` prunes nodes, `leaf` sees every live
    /// entry of surviving leaves.
    ///
    /// The traversal stack is a recycled thread-local: a BATCHED query (`join`,
    /// `query(array)`, the spatial-join refine loop) runs this thousands of
    /// times back-to-back, so a fresh `Vec` per descent is measurable waste
    /// (~560 instr/query). Descents never nest (the `leaf` visitor only records
    /// ids), so one buffer per thread serves every call.
    fn descend(&self, enter: impl Fn(&AABB<[f64; 2]>) -> bool, mut leaf: impl FnMut(&IndexEntry)) {
        thread_local! {
            static STACK: std::cell::RefCell<Vec<(usize, u32)>> =
                const { std::cell::RefCell::new(Vec::new()) };
        }
        STACK.with_borrow_mut(|stack| {
            stack.clear();
            let root_level = self.root_level();
            stack.extend(
                (0..self.roots().len() as u32)
                    .filter(|&node| enter(&self.node(root_level, node).envelope))
                    .map(|node| (root_level, node)),
            );
            while let Some((level, index)) = stack.pop() {
                let node = self.node(level, index);
                if level == 0 {
                    for entry in self.leaf_entries(node) {
                        if !self.removed[entry.idx] {
                            leaf(entry);
                        }
                    }
                    continue;
                }
                for child in node.children.clone() {
                    if enter(&self.node(level - 1, child).envelope) {
                        stack.push((level - 1, child));
                    }
                }
            }
        });
    }
}

crate::heapless!(IndexEntry, StrNode);

impl HeapSize for StaticStrTree {
    fn heap_bytes(&self) -> usize {
        self.entries.heap_bytes() + self.levels.heap_bytes() + self.removed.heap_bytes()
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
            let mut tree = StaticStrTree::build(entries.clone(), entries.len());
            let removed: Vec<usize> = (0..count).filter(|idx| idx % 7 == 3).collect();
            for &handle in &removed {
                assert!(tree.tombstone(handle));
                assert!(!tree.tombstone(handle), "double tombstone must be false");
            }
            assert_eq!(tree.live(), count - removed.len());
            assert_eq!(
                sorted(tree.live_entries().map(|entry| entry.idx).collect()),
                brute(&entries, &removed, |_| true)
            );
            for query in [
                AABB::from_corners([50.0, 40.0], [180.0, 200.0]),
                AABB::from_corners([0.0, 0.0], [311.0, 257.0]),
                AABB::from_corners([400.0, 400.0], [500.0, 500.0]),
                AABB::from_point([73.0, 131.0]),
            ] {
                let mut hits = Vec::new();
                tree.each_intersecting(&query, |idx| hits.push(idx));
                assert_eq!(
                    sorted(hits),
                    brute(&entries, &removed, |entry| entry
                        .envelope
                        .intersects(&query))
                );
                let mut inside = Vec::new();
                tree.each_contained(&query, |idx| inside.push(idx));
                assert_eq!(
                    sorted(inside),
                    brute(&entries, &removed, |entry| query
                        .contains_envelope(&entry.envelope))
                );
            }
            let point = [120.0, 90.0];
            for radius_2 in [0.0, 100.0, 10_000.0] {
                let mut near = Vec::new();
                tree.each_within_distance_2(point, radius_2, |idx| near.push(idx));
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

        let tree = StaticStrTree::build(vec![positive, negative], 2);
        let mut negative_hits = Vec::new();
        tree.each_intersecting(
            &AABB::from_corners([-1.0e308, -1.0e308], [-9.0e307, -9.0e307]),
            |idx| negative_hits.push(idx),
        );
        assert_eq!(negative_hits, [0]);
    }
}
