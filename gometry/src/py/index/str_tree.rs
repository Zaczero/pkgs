//! Immutable STR-packed envelope trees for both halves of the spatial index.
//!
//! Built with the sort-tile-recursive layout (one global x-sort, one y-sort
//! per strip, flat level arrays — no per-node heap graph). Liveness is owned
//! by the index row table. Post-build inserts live in a binary forest of these
//! immutable runs, so rebuilding work is carried only by binary carries rather
//! than by one tree rebuilt for every mutation.

use rstar::{AABB, Envelope as _};

use crate::HeapSize;
use crate::py::index::IndexEntry;

#[cfg(test)]
thread_local! {
    static BUILD_WORK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

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
#[derive(Debug, Default)]
pub(super) struct StaticStrTree {
    /// STR-ordered entries (leaf children).
    entries: Vec<IndexEntry>,
    /// `levels[0]` = leaves; the last level is the root layer (length <=
    /// `NODE_CAP` — descents seed from it directly).
    levels: Vec<Vec<StrNode>>,
    initial_len: usize,
}

impl StaticStrTree {
    #[cfg(test)]
    pub(super) fn reset_build_work() {
        BUILD_WORK.with(|work| work.set(0));
    }

    #[cfg(test)]
    pub(super) fn build_work() -> usize {
        BUILD_WORK.with(std::cell::Cell::get)
    }

    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(super) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    /// Clone the immutable tree through the crate's fallible allocation
    /// boundary. The clone keeps the logical entries and levels but reserves
    /// only their initialized lengths, not incidental source spare capacity.
    pub(super) fn try_clone(&self) -> crate::error::Result<Self> {
        let mut entries = Vec::new();
        entries.try_reserve_exact(self.entries.len())?;
        for &entry in &self.entries {
            crate::try_push_vec(&mut entries, entry)?;
        }

        let mut levels = Vec::new();
        levels.try_reserve_exact(self.levels.len())?;
        for level in &self.levels {
            let mut copied = Vec::new();
            copied.try_reserve_exact(level.len())?;
            for node in level {
                crate::try_push_vec(&mut copied, node.clone())?;
            }
            crate::try_push_vec(&mut levels, copied)?;
        }

        Ok(Self {
            entries,
            levels,
            initial_len: self.initial_len,
        })
    }

    /// Bulk-load with the STR layout. `total_rows` is the input row count —
    /// each `entry.idx` is a stable handle in `0..total_rows`. `total_rows`
    /// is the tree/overflow handle boundary and spans skipped rows too.
    pub(super) fn build(entries: Vec<IndexEntry>, total_rows: usize) -> crate::error::Result<Self> {
        Self::build_with_len(entries, total_rows)
    }

    fn build_with_len(
        mut entries: Vec<IndexEntry>,
        total_rows: usize,
    ) -> crate::error::Result<Self> {
        let entry_count = entries.len();
        #[cfg(test)]
        BUILD_WORK.with(|work| work.set(work.get().saturating_add(entry_count)));
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

    /// Number of physical envelope entries in this immutable run.
    pub(super) const fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Entries in this run's STR storage order.
    ///
    /// The forest copies these entries only while preparing a binary carry or
    /// a compaction. Keeping the copy surface here avoids exposing the tree's
    /// level representation to its owner.
    pub(super) fn entries(&self) -> &[IndexEntry] {
        &self.entries
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

/// An immutable-run forest for post-build index entries.
///
/// `runs[rank]`, when occupied, contains exactly `2.pow(rank)` physical
/// envelope entries. The row table remains the sole liveness authority:
/// `live_entries` and `dead_entries` count physical entries, while
/// `entry_counts` records how many envelopes each never-reused overflow handle
/// contributed.
#[derive(Debug)]
pub(super) struct StaticStrForest {
    runs: Vec<Option<StaticStrTree>>,
    handle_start: usize,
    entry_counts: Vec<usize>,
    total_entries: usize,
    live_entries: usize,
    dead_entries: usize,
}

impl StaticStrForest {
    pub(super) const fn new(handle_start: usize) -> Self {
        Self {
            runs: Vec::new(),
            handle_start,
            entry_counts: Vec::new(),
            total_entries: 0,
            live_entries: 0,
            dead_entries: 0,
        }
    }

    #[expect(
        clippy::same_name_method,
        reason = "the inherent operation deliberately shares the domain vocabulary of its trait contract"
    )]
    pub(super) fn heap_bytes(&self) -> usize {
        HeapSize::heap_bytes(self)
    }

    /// Fallible deep clone of the immutable runs and their physical-entry
    /// accounting. No geodesic cache belongs here; it remains an index-level
    /// cache and is intentionally rebuilt empty by [`PySpatialIndex`].
    pub(super) fn try_clone(&self) -> crate::error::Result<Self> {
        let mut runs = Vec::new();
        runs.try_reserve_exact(self.runs.len())?;
        for run in &self.runs {
            let copied = run.as_ref().map(StaticStrTree::try_clone).transpose()?;
            crate::try_push_vec(&mut runs, copied)?;
        }

        let mut entry_counts = Vec::new();
        entry_counts.try_reserve_exact(self.entry_counts.len())?;
        for &count in &self.entry_counts {
            crate::try_push_vec(&mut entry_counts, count)?;
        }

        Ok(Self {
            runs,
            handle_start: self.handle_start,
            entry_counts,
            total_entries: self.total_entries,
            live_entries: self.live_entries,
            dead_entries: self.dead_entries,
        })
    }

    #[cfg(test)]
    pub(super) const fn total_entries(&self) -> usize {
        self.total_entries
    }

    #[cfg(test)]
    pub(super) const fn live_entry_count(&self) -> usize {
        self.live_entries
    }

    #[cfg(test)]
    pub(super) const fn dead_entry_count(&self) -> usize {
        self.dead_entries
    }

    /// Number of occupied binary ranks. This is deliberately not a
    /// configurable compaction or fan-out threshold.
    #[cfg(test)]
    pub(super) fn occupied_run_count(&self) -> usize {
        self.runs.iter().filter(|run| run.is_some()).count()
    }

    /// Every occupied run with its exact binary rank.
    pub(super) fn occupied_runs(&self) -> impl Iterator<Item = (usize, &StaticStrTree)> {
        self.runs
            .iter()
            .enumerate()
            .filter_map(|(rank, run)| run.as_ref().map(|run| (rank, run)))
    }

    pub(super) fn root_count(&self) -> usize {
        self.occupied_runs().map(|(_, run)| run.roots().len()).sum()
    }

    /// Roots of every occupied run, tagged with the run rank and level for a
    /// shared best-first frontier.
    pub(super) fn roots(&self) -> impl Iterator<Item = (usize, usize, u32, &StrNode)> + '_ {
        self.occupied_runs().flat_map(|(run, tree)| {
            let level = tree.root_level();
            tree.roots()
                .iter()
                .enumerate()
                .map(move |(index, node)| (run, level, index as u32, node))
        })
    }

    pub(super) fn run(&self, rank: usize) -> &StaticStrTree {
        self.runs
            .get(rank)
            .and_then(Option::as_ref)
            .expect("occupied forest rank")
    }

    /// Every live physical entry in run order. Periodic envelope aliases are
    /// intentionally retained; row-level callers deduplicate them where the
    /// query contract returns logical handles.
    pub(super) fn live_entries<'a>(
        &'a self,
        live: &'a [u8],
    ) -> impl Iterator<Item = &'a IndexEntry> {
        self.runs
            .iter()
            .filter_map(Option::as_ref)
            .flat_map(move |run| run.live_entries(live))
    }

    pub(super) fn each_intersecting(
        &self,
        live: &[u8],
        query: &AABB<[f64; 2]>,
        mut visit: impl FnMut(usize) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        for run in self.runs.iter().filter_map(Option::as_ref) {
            run.each_intersecting(live, query, &mut visit)?;
        }
        Ok(())
    }

    pub(super) fn each_within_distance_2(
        &self,
        live: &[u8],
        point: [f64; 2],
        max_distance_2: f64,
        mut visit: impl FnMut(usize) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        for run in self.runs.iter().filter_map(Option::as_ref) {
            run.each_within_distance_2(live, point, max_distance_2, &mut visit)?;
        }
        Ok(())
    }

    /// Fallibly reserve the persistent per-overflow-row count column. No
    /// logical state changes if the allocator rejects this request.
    pub(super) fn try_reserve_entry_counts(
        &mut self,
        additional: usize,
    ) -> crate::error::Result<()> {
        self.entry_counts.try_reserve(additional)?;
        Ok(())
    }

    pub(super) fn entry_count_for_handle(&self, handle: usize) -> usize {
        debug_assert!(handle >= self.handle_start);
        self.entry_counts
            .get(handle.saturating_sub(self.handle_start))
            .copied()
            .unwrap_or(0)
    }

    /// Prepare the binary forest after adding `entries`, without touching this
    /// forest. The returned runs are complete enough to publish by a
    /// non-fallible move once all other mutation-owned reserves succeed.
    pub(super) fn prepare_insert(
        &self,
        entries: &[IndexEntry],
        total_rows: usize,
    ) -> crate::error::Result<PreparedForest> {
        let total_entries = self
            .total_entries
            .checked_add(entries.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if entries.is_empty() {
            return Ok(PreparedForest {
                runs: Vec::new(),
                consumed: Vec::new(),
                total_entries,
            });
        }

        let rank_count = required_rank_count(self.runs.len(), total_entries);
        let mut runs = Vec::new();
        runs.try_reserve(rank_count)?;
        runs.resize_with(rank_count, || None);
        let mut consumed = Vec::new();
        consumed.try_reserve(rank_count)?;
        consumed.resize(rank_count, false);

        let mut offset = 0;
        while offset < entries.len() {
            let chunk_len = highest_power_of_two(entries.len() - offset);
            let chunk = copy_entries(&entries[offset..offset + chunk_len])?;
            let mut carry = StaticStrTree::build(chunk, total_rows)?;
            let mut rank = chunk_len.trailing_zeros() as usize;
            offset += chunk_len;

            loop {
                debug_assert!(rank < runs.len());
                if let Some(staged) = runs[rank].take() {
                    carry = merge_trees(&staged, &carry, total_rows)?;
                    rank += 1;
                    continue;
                }
                if rank < self.runs.len()
                    && !consumed[rank]
                    && let Some(existing) = self.runs[rank].as_ref()
                {
                    consumed[rank] = true;
                    carry = merge_trees(existing, &carry, total_rows)?;
                    rank += 1;
                    continue;
                }
                runs[rank] = Some(carry);
                break;
            }
        }

        Ok(PreparedForest {
            runs,
            consumed,
            total_entries,
        })
    }

    /// Publish a previously prepared insertion. Every operation after this
    /// point is a move, drop, or infallible push into capacity reserved by the
    /// caller.
    pub(super) fn commit_insert(&mut self, prepared: PreparedForest, entry_counts: &[usize]) {
        let added_entries = prepared.total_entries - self.total_entries;
        debug_assert_eq!(entry_counts.iter().copied().sum::<usize>(), added_entries);
        if !prepared.runs.is_empty() {
            let old_runs = std::mem::take(&mut self.runs);
            let mut runs = prepared.runs;
            debug_assert!(old_runs.len() <= runs.len());
            for (rank, old) in old_runs.into_iter().enumerate() {
                if runs[rank].is_none() && !prepared.consumed[rank] {
                    runs[rank] = old;
                } else {
                    debug_assert!(old.is_none() || prepared.consumed[rank]);
                }
            }
            while runs.last().is_some_and(Option::is_none) {
                runs.pop();
            }
            self.runs = runs;
        }
        for &count in entry_counts {
            self.entry_counts.push(count);
        }
        self.total_entries = prepared.total_entries;
        self.live_entries += added_entries;
        debug_assert_eq!(self.total_entries, self.live_entries + self.dead_entries);
    }

    pub(super) const fn should_compact(&self, removed_entries: usize) -> bool {
        let live_after = self
            .live_entries
            .checked_sub(removed_entries)
            .expect("removed overflow entries are live");
        self.dead_entries
            .checked_add(removed_entries)
            .expect("overflow entry counters fit usize")
            > live_after
    }

    pub(super) fn mark_removed(&mut self, removed_entries: usize) {
        debug_assert!(removed_entries <= self.live_entries);
        self.live_entries -= removed_entries;
        self.dead_entries += removed_entries;
        debug_assert_eq!(self.total_entries, self.live_entries + self.dead_entries);
    }

    /// Prepare a fresh forest containing every currently live entry except
    /// `removed_handle`. The row tombstone is intentionally not published
    /// until this fallible construction succeeds.
    pub(super) fn prepare_compaction(
        &self,
        live: &[u8],
        removed_handle: usize,
        total_rows: usize,
        removed_entries: usize,
    ) -> crate::error::Result<PreparedForest> {
        debug_assert!(removed_entries <= self.live_entries);
        let remaining = self.live_entries - removed_entries;
        let mut entries = Vec::new();
        entries.try_reserve(remaining)?;
        for entry in self.live_entries(live) {
            if entry.idx != removed_handle {
                crate::try_push_vec(&mut entries, *entry)?;
            }
        }
        debug_assert_eq!(entries.len(), remaining);
        let empty = Self::new(self.handle_start);
        empty.prepare_insert(&entries, total_rows)
    }

    pub(super) fn commit_compaction(&mut self, prepared: PreparedForest) {
        let _old_runs = std::mem::replace(&mut self.runs, prepared.runs);
        self.total_entries = prepared.total_entries;
        self.live_entries = prepared.total_entries;
        self.dead_entries = 0;
        debug_assert_eq!(self.total_entries, self.live_entries + self.dead_entries);
    }
}

/// A fully constructed candidate run set. `runs` may contain `None` slots
/// where an unchanged old run is filled during insertion publication.
pub(super) struct PreparedForest {
    runs: Vec<Option<StaticStrTree>>,
    consumed: Vec<bool>,
    total_entries: usize,
}

impl HeapSize for StaticStrForest {
    fn heap_bytes(&self) -> usize {
        self.runs.heap_bytes() + self.entry_counts.heap_bytes()
    }
}

fn required_rank_count(existing: usize, total_entries: usize) -> usize {
    let highest_rank = (usize::BITS - 1 - total_entries.leading_zeros()) as usize;
    existing.max(highest_rank + 1)
}

fn highest_power_of_two(value: usize) -> usize {
    debug_assert!(value != 0);
    1_usize << (usize::BITS - 1 - value.leading_zeros())
}

fn copy_entries(entries: &[IndexEntry]) -> crate::error::Result<Vec<IndexEntry>> {
    let mut copied = Vec::new();
    copied.try_reserve(entries.len())?;
    for &entry in entries {
        crate::try_push_vec(&mut copied, entry)?;
    }
    Ok(copied)
}

fn merge_trees(
    left: &StaticStrTree,
    right: &StaticStrTree,
    total_rows: usize,
) -> crate::error::Result<StaticStrTree> {
    let count = left
        .entry_count()
        .checked_add(right.entry_count())
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut entries = Vec::new();
    entries.try_reserve(count)?;
    for &entry in left.entries().iter().chain(right.entries()) {
        crate::try_push_vec(&mut entries, entry)?;
    }
    StaticStrTree::build(entries, total_rows)
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

    fn forest_entries(start: usize, count: usize) -> Vec<IndexEntry> {
        (0..count)
            .map(|offset| entry(start + offset, [offset as f64, 0.0], [offset as f64, 0.0]))
            .collect()
    }

    fn commit_forest_batch(forest: &mut StaticStrForest, entries: &[IndexEntry], counts: &[usize]) {
        let prepared = forest
            .prepare_insert(entries, 128)
            .expect("prepared forest");
        forest
            .try_reserve_entry_counts(counts.len())
            .expect("entry-count reserve");
        forest.commit_insert(prepared, counts);
    }

    #[test]
    fn binary_forest_keeps_exact_occupied_weights_for_arbitrary_batches() {
        let mut forest = StaticStrForest::new(10);
        let entries = forest_entries(10, 13);
        let counts = vec![1; entries.len()];
        commit_forest_batch(&mut forest, &entries, &counts);

        let runs: Vec<_> = forest
            .occupied_runs()
            .map(|(rank, tree)| (rank, tree.entry_count()))
            .collect();
        assert_eq!(runs, [(0, 1), (2, 4), (3, 8)]);
        assert_eq!(forest.total_entries(), 13);
        assert_eq!(forest.live_entry_count(), 13);
        assert_eq!(forest.dead_entry_count(), 0);
        assert_eq!(forest.occupied_run_count(), 3);
    }

    #[test]
    fn binary_forest_carries_across_preoccupied_ranks_without_losing_entries() {
        let mut forest = StaticStrForest::new(20);
        let first = forest_entries(20, 7);
        commit_forest_batch(&mut forest, &first, &[1; 7]);
        let second = forest_entries(27, 9);
        commit_forest_batch(&mut forest, &second, &[1; 9]);

        let weights: Vec<_> = forest
            .occupied_runs()
            .map(|(rank, tree)| (rank, tree.entry_count()))
            .collect();
        assert_eq!(weights, [(4, 16)]);
        assert_eq!(forest.total_entries(), 16);
        let live = vec![1_u8; 36];
        let ids: Vec<_> = forest.live_entries(&live).map(|entry| entry.idx).collect();
        assert_eq!(ids.len(), 16);
        assert!(ids.into_iter().all(|idx| (20..36).contains(&idx)));
    }

    #[test]
    fn binary_forest_small_batch_matrix_preserves_every_entry() {
        for initial_count in 0..16 {
            for batch_count in 0..16 {
                let mut forest = StaticStrForest::new(50);
                let initial = forest_entries(50, initial_count);
                commit_forest_batch(&mut forest, &initial, &vec![1; initial_count]);
                let batch = forest_entries(50 + initial_count, batch_count);
                commit_forest_batch(&mut forest, &batch, &vec![1; batch_count]);

                let total = initial_count + batch_count;
                let live = vec![1_u8; 50 + total];
                let mut ids: Vec<_> = forest.live_entries(&live).map(|entry| entry.idx).collect();
                ids.sort_unstable();
                let expected: Vec<_> = (50..50 + total).collect();
                assert_eq!(ids, expected);
                let weights: usize = forest
                    .occupied_runs()
                    .map(|(rank, tree)| {
                        assert_eq!(tree.entry_count(), 1_usize << rank);
                        tree.entry_count()
                    })
                    .sum();
                assert_eq!(weights, total);
                assert_eq!(
                    forest.occupied_run_count(),
                    (0..usize::BITS as usize)
                        .filter(|rank| total & (1_usize << rank) != 0)
                        .count()
                );
            }
        }
    }

    #[test]
    fn forest_spatial_traversal_matches_exhaustive_physical_oracle() {
        let mut forest = StaticStrForest::new(60);
        let entries = forest_entries(60, 37);
        commit_forest_batch(&mut forest, &entries, &[1; 37]);
        let mut live = vec![1_u8; 97];
        for handle in [63, 74, 91] {
            live[handle] = 0;
        }
        forest.mark_removed(3);

        let intersects_queries = [
            AABB::from_corners([0.0, 0.0], [8.0, 0.0]),
            AABB::from_corners([16.0, 0.0], [27.0, 0.0]),
            AABB::from_corners([100.0, 100.0], [110.0, 110.0]),
        ];
        for query in intersects_queries {
            let mut actual = Vec::new();
            forest
                .each_intersecting(&live, &query, |handle| {
                    actual.push(handle);
                    Ok(())
                })
                .expect("forest intersection traversal");
            actual.sort_unstable();
            let mut expected: Vec<_> = entries
                .iter()
                .filter(|entry| live[entry.idx] != 0 && intersects(&entry.envelope, &query))
                .map(|entry| entry.idx)
                .collect();
            expected.sort_unstable();
            assert_eq!(actual, expected);
        }

        for (point, radius_2) in [([2.0, 0.0], 4.0), ([29.0, 0.0], 1.0), ([100.0, 0.0], 1.0)] {
            let mut actual = Vec::new();
            forest
                .each_within_distance_2(&live, point, radius_2, |handle| {
                    actual.push(handle);
                    Ok(())
                })
                .expect("forest distance traversal");
            actual.sort_unstable();
            let mut expected: Vec<_> = entries
                .iter()
                .filter(|entry| {
                    live[entry.idx] != 0 && distance_2(&entry.envelope, &point) <= radius_2
                })
                .map(|entry| entry.idx)
                .collect();
            expected.sort_unstable();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn compaction_rebuilds_only_live_entries_and_resets_dead_count() {
        let mut forest = StaticStrForest::new(30);
        let entries = forest_entries(30, 8);
        commit_forest_batch(&mut forest, &entries, &[1; 8]);
        let mut live = vec![1_u8; 38];
        live[30] = 0;
        live[31] = 0;
        live[32] = 0;
        live[33] = 0;
        forest.mark_removed(4);
        assert!(forest.should_compact(1));
        let prepared = forest
            .prepare_compaction(&live, 34, 38, 1)
            .expect("compaction");
        forest.commit_compaction(prepared);
        assert_eq!(forest.total_entries(), 3);
        assert_eq!(forest.live_entry_count(), 3);
        assert_eq!(forest.dead_entry_count(), 0);
        assert!(forest.live_entries(&live).all(|entry| entry.idx != 34));
    }

    #[test]
    fn scalar_insert_remove_work_is_not_quadratic() {
        let count = 256;
        let mut forest = StaticStrForest::new(40);
        let mut live = vec![0_u8; 40 + count];
        StaticStrTree::reset_build_work();
        for (handle, row) in live.iter_mut().enumerate().skip(40).take(count) {
            let entries = [entry(handle, [handle as f64, 0.0], [handle as f64, 0.0])];
            commit_forest_batch(&mut forest, &entries, &[1]);
            *row = 1;
        }
        let mut handle = 40;
        while handle < 40 + count {
            if forest.should_compact(1) {
                let prepared = forest
                    .prepare_compaction(&live, handle, live.len(), 1)
                    .expect("compaction");
                live[handle] = 0;
                forest.commit_compaction(prepared);
            } else {
                live[handle] = 0;
                forest.mark_removed(1);
            }
            handle += 1;
        }
        assert_eq!(forest.live_entry_count(), 0);
        assert_eq!(forest.dead_entry_count(), 0);
        let work = StaticStrTree::build_work();
        // A full accumulated rebuild per scalar insert would process
        // count * (count + 1) / 2 entries. Binary carries and geometric
        // tombstone compactions stay far below that quadratic work.
        assert!(work * 4 < count * count, "entry rebuild work: {work}");
    }
}
