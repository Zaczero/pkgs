//! Immutable STR-packed envelope trees for both halves of the spatial index.
//!
//! Built with the sort-tile-recursive layout (one global x-sort, one y-sort
//! per strip, flat level arrays — no per-node heap graph). Liveness is owned
//! by the index row table. Post-build inserts live in a binary forest of these
//! immutable runs, so rebuilding work is carried only by binary carries rather
//! than by one tree rebuilt for every mutation.

use std::sync::Arc;

use rstar::{AABB, Envelope as _};

use crate::HeapSize;
use crate::py::index::IndexEntry;
use crate::py::index::live_handles::LiveHandles;

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
/// handle in the live-handle directory, so `remove` never searches the tree.
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

    /// Bulk-load with the STR layout. `total_rows` is the input row count —
    /// each `entry.idx` is a stable handle in `0..total_rows`. `total_rows`
    /// is the tree/overflow handle boundary and spans skipped rows too.
    pub(super) fn build(
        mut entries: Vec<IndexEntry>,
        total_rows: usize,
    ) -> crate::error::Result<Arc<Self>> {
        let entry_count = entries.len();
        #[cfg(test)]
        BUILD_WORK.with(|work| work.set(work.get().saturating_add(entry_count)));
        let mut tree = Self {
            entries: Vec::new(),
            levels: Vec::new(),
            initial_len: total_rows,
        };
        if entry_count == 0 {
            return Ok(Arc::new(tree));
        }
        if u32::try_from(entry_count).is_err() {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        }
        // Sort-tile-recursive: x-sort globally, slice into ~sqrt(leaves)
        // vertical strips, y-sort each strip; consecutive NODE_CAP runs
        // become leaves and every upper level re-chunks the one below
        // (already spatially ordered).
        let leaf_count = entry_count.div_ceil(NODE_CAP);
        let mut level_count = 1_usize;
        let mut level_len = leaf_count;
        while level_len > NODE_CAP {
            level_len = level_len.div_ceil(NODE_CAP);
            level_count = level_count.wrapping_add(1);
        }
        let root = leaf_count.isqrt();
        let strips = root.wrapping_add(usize::from(root.wrapping_mul(root) != leaf_count));
        let strip_len = entry_count.div_ceil(strips).max(NODE_CAP);
        // Ordering by the coordinate SUM is ordering by the midpoint (the
        // halving is monotone), one add per key. At +-1e308 the sum can reach
        // +-inf, which `total_cmp` still orders deterministically — extreme
        // entries then tie at the ends, degrading only partition balance for
        // absurd-coordinate data, never query correctness (envelopes stay
        // exact; see `str_centers_stay_ordered_for_huge_finite_envelopes`).
        let center = |entry: &IndexEntry, axis: usize| {
            let lower = entry.envelope.lower();
            let upper = entry.envelope.upper();
            *lower
                .get(axis)
                .expect("index envelopes have exactly two axes")
                + *upper
                    .get(axis)
                    .expect("index envelopes have exactly two axes")
        };
        entries.sort_unstable_by(|a, b| center(a, 0).total_cmp(&center(b, 0)));
        for strip in entries.chunks_mut(strip_len) {
            strip.sort_unstable_by(|a, b| center(a, 1).total_cmp(&center(b, 1)));
        }
        let mut level = Vec::with_capacity(leaf_count);
        for (node, chunk) in entries.chunks(NODE_CAP).enumerate() {
            level.push(StrNode {
                envelope: chunk
                    .iter()
                    .map(|entry| entry.envelope)
                    .reduce(|a, b| a.merged(&b))
                    .expect("chunks are non-empty"),
                children: range_at(node, chunk.len()),
            });
        }
        let mut levels = Vec::with_capacity(level_count);
        while level.len() > NODE_CAP {
            let next_count = level.len().div_ceil(NODE_CAP);
            let mut next = Vec::with_capacity(next_count);
            for (node, chunk) in level.chunks(NODE_CAP).enumerate() {
                next.push(StrNode {
                    envelope: chunk
                        .iter()
                        .map(|child| child.envelope)
                        .reduce(|a, b| a.merged(&b))
                        .expect("chunks are non-empty"),
                    children: range_at(node, chunk.len()),
                });
            }
            levels.push(level);
            level = next;
        }
        levels.push(level);
        tree.entries = entries;
        tree.levels = levels;
        Ok(Arc::new(tree))
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

    pub(super) fn prepare_compaction(
        &self,
        live: &LiveHandles,
        removed_handle: usize,
        remaining: usize,
    ) -> crate::error::Result<Arc<Self>> {
        let mut entries = LiveEntriesExcept {
            entries: self.live_entries(live),
            removed_handle,
            remaining,
        };
        let retained = entries.by_ref().collect();
        #[cfg(debug_assertions)]
        entries.debug_assert_exhausted();
        Self::build(retained, self.initial_len)
    }

    /// Every live entry, in STR storage order.
    pub(super) fn live_entries<'a>(
        &'a self,
        live: &'a LiveHandles,
    ) -> impl Iterator<Item = &'a IndexEntry> {
        self.entries.iter().filter(|entry| live.contains(entry.idx))
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
        self.levels
            .get(level)
            .and_then(|nodes| nodes.get(index as usize))
            .expect("frontier node was admitted from this tree's root or child range")
    }

    /// The level-0-children entries of a LEAF node.
    pub(super) fn leaf_entries(&self, node: &StrNode) -> &[IndexEntry] {
        let range = node.children.start as usize..node.children.end as usize;
        self.entries
            .get(range)
            .expect("leaf child range was admitted when the tree was built")
    }

    /// Append the live handles whose envelope intersects `query`.
    pub(super) fn append_intersecting(
        &self,
        live: &LiveHandles,
        query: &AABB<[f64; 2]>,
        out: &mut Vec<usize>,
    ) {
        self.descend(
            |envelope| intersects(envelope, query),
            |entries| {
                append_live(entries, live, out, |entry| {
                    intersects(&entry.envelope, query)
                });
            },
        );
    }

    /// Append the live handles whose envelope lies within squared distance
    /// `max_distance_2` of `point` (rstar `locate_within_distance` semantics:
    /// the entry's envelope distance decides).
    pub(super) fn append_within_distance_2(
        &self,
        live: &LiveHandles,
        point: [f64; 2],
        max_distance_2: f64,
        out: &mut Vec<usize>,
    ) {
        self.descend(
            |envelope| distance_2(envelope, point) <= max_distance_2,
            |entries| {
                append_live(entries, live, out, |entry| {
                    distance_2(&entry.envelope, point) <= max_distance_2
                });
            },
        );
    }

    /// Depth-first descent: `enter` prunes nodes and `leaf` receives the
    /// entries of every surviving leaf. The `u32` child ranges bound the
    /// height, so the recursion is at most a handful of frames deep.
    fn descend(
        &self,
        enter: impl Fn(&AABB<[f64; 2]>) -> bool,
        mut leaf: impl FnMut(&[IndexEntry]),
    ) {
        let level = self.root_level();
        for root in self.roots() {
            if enter(&root.envelope) {
                self.visit(level, root, &enter, &mut leaf);
            }
        }
    }

    fn visit(
        &self,
        level: usize,
        node: &StrNode,
        enter: &impl Fn(&AABB<[f64; 2]>) -> bool,
        leaf: &mut impl FnMut(&[IndexEntry]),
    ) {
        if level == 0 {
            leaf(self.leaf_entries(node));
            return;
        }
        let child_level = level.wrapping_sub(1);
        let children = self
            .levels
            .get(child_level)
            .expect("non-leaf node has a child level")
            .get(node.children.start as usize..node.children.end as usize)
            .expect("node child range was admitted when the tree was built");
        for child in children {
            if enter(&child.envelope) {
                self.visit(child_level, child, enter, leaf);
            }
        }
    }
}

/// Append the handle of each live entry of one leaf that `keep` accepts. A
/// leaf holds at most [`NODE_CAP`] entries, so one reservation covers the
/// whole leaf and the pushes stay in capacity.
fn append_live(
    entries: &[IndexEntry],
    live: &LiveHandles,
    out: &mut Vec<usize>,
    keep: impl Fn(&IndexEntry) -> bool,
) {
    out.reserve(entries.len());
    for entry in entries {
        if live.contains(entry.idx) && keep(entry) {
            out.push(entry.idx);
        }
    }
}

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

fn distance_2(envelope: &AABB<[f64; 2]>, point: [f64; 2]) -> f64 {
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
        self.entries
            .heap_bytes()
            .saturating_add(self.levels.heap_bytes())
    }
}

/// An immutable-run forest for post-build index entries.
///
/// `runs[rank]`, when occupied, contains exactly `2.pow(rank)` physical
/// envelope entries. The row table remains the sole liveness authority:
/// `live_entries` and `dead_entries` count physical entries.
#[derive(Clone, Debug)]
pub(super) struct StaticStrForest {
    runs: Vec<Option<Arc<StaticStrTree>>>,
    total_entries: usize,
    live_entries: usize,
    dead_entries: usize,
}

impl StaticStrForest {
    pub(super) const fn new() -> Self {
        Self {
            runs: Vec::new(),
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

    #[cfg(test)]
    pub(super) const fn total_entries(&self) -> usize {
        self.total_entries
    }

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
            .filter_map(|(rank, run)| run.as_deref().map(|run| (rank, run)))
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
        live: &'a LiveHandles,
    ) -> impl Iterator<Item = &'a IndexEntry> {
        self.runs
            .iter()
            .filter_map(Option::as_ref)
            .flat_map(move |run| run.live_entries(live))
    }

    pub(super) fn append_intersecting(
        &self,
        live: &LiveHandles,
        query: &AABB<[f64; 2]>,
        out: &mut Vec<usize>,
    ) {
        for run in self.runs.iter().filter_map(Option::as_ref) {
            run.append_intersecting(live, query, out);
        }
    }

    pub(super) fn append_within_distance_2(
        &self,
        live: &LiveHandles,
        point: [f64; 2],
        max_distance_2: f64,
        out: &mut Vec<usize>,
    ) {
        for run in self.runs.iter().filter_map(Option::as_ref) {
            run.append_within_distance_2(live, point, max_distance_2, out);
        }
    }

    /// Prepare the binary forest after adding `entries`, without touching this
    /// forest. The returned runs are complete enough to publish by a
    /// non-fallible move once all other mutation-owned reserves succeed.
    pub(super) fn prepare_insert(
        &self,
        entries: &[IndexEntry],
        total_rows: usize,
    ) -> crate::error::Result<PreparedForest> {
        self.prepare_insert_from(entries.iter().copied(), total_rows)
    }

    /// The exact incoming length determines the binary chunk partition.
    fn prepare_insert_from(
        &self,
        mut entries: impl ExactSizeIterator<Item = IndexEntry>,
        total_rows: usize,
    ) -> crate::error::Result<PreparedForest> {
        let added_entries = entries.len();
        let total_entries = self
            .total_entries
            .checked_add(added_entries)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if added_entries == 0 {
            return Ok(PreparedForest {
                runs: Vec::new(),
                consumed: Vec::new(),
                total_entries,
            });
        }

        let rank_count = required_rank_count(self.runs.len(), total_entries);
        let mut pending: Vec<Option<Vec<IndexEntry>>> = Vec::with_capacity(rank_count);
        pending.resize_with(rank_count, || None);
        let mut consumed = vec![false; rank_count];

        let mut offset = 0;
        while offset < added_entries {
            let chunk_len = highest_power_of_two(added_entries.wrapping_sub(offset));
            let mut carry = Vec::with_capacity(chunk_len);
            carry.extend((&mut entries).take(chunk_len));
            debug_assert_eq!(carry.len(), chunk_len);
            let mut rank = chunk_len.trailing_zeros() as usize;
            offset = offset.wrapping_add(chunk_len);

            loop {
                debug_assert!(rank < pending.len());
                let staged_slot = pending
                    .get_mut(rank)
                    .expect("carry rank is within the prepared forest");
                if let Some(staged) = staged_slot.take() {
                    carry.reserve(staged.len());
                    carry.extend_from_slice(&staged);
                    rank = rank.wrapping_add(1);
                    continue;
                }
                if rank < self.runs.len()
                    && !*consumed
                        .get(rank)
                        .expect("occupied carry rank has a consumed flag")
                    && let Some(existing) = self.runs.get(rank).and_then(Option::as_ref)
                {
                    *consumed
                        .get_mut(rank)
                        .expect("occupied carry rank has a consumed flag") = true;
                    carry.reserve(existing.entry_count());
                    carry.extend_from_slice(existing.entries());
                    rank = rank.wrapping_add(1);
                    continue;
                }
                *pending
                    .get_mut(rank)
                    .expect("carry rank is within the prepared forest") = Some(carry);
                break;
            }
        }

        let mut runs = Vec::with_capacity(rank_count);
        for entries in pending {
            runs.push(
                entries
                    .map(|entries| StaticStrTree::build(entries, total_rows))
                    .transpose()?,
            );
        }
        debug_assert_eq!(entries.len(), 0);
        Ok(PreparedForest {
            runs,
            consumed,
            total_entries,
        })
    }

    /// Publish a previously prepared insertion. Every operation after this
    /// point is a move, drop, or infallible push into capacity reserved by the
    /// caller.
    pub(super) fn commit_insert(&mut self, prepared: PreparedForest) {
        debug_assert!(prepared.total_entries >= self.total_entries);
        let added_entries = prepared.total_entries.wrapping_sub(self.total_entries);
        if !prepared.runs.is_empty() {
            let old_runs = std::mem::take(&mut self.runs);
            let mut runs = prepared.runs;
            debug_assert!(old_runs.len() <= runs.len());
            let existing_count = old_runs.len();
            let runs_prefix = runs
                .get_mut(..existing_count)
                .expect("prepared forest retains every prior rank");
            let consumed_prefix = prepared
                .consumed
                .get(..existing_count)
                .expect("prepared forest retains every prior rank flag");
            for ((run, &consumed), old) in runs_prefix.iter_mut().zip(consumed_prefix).zip(old_runs)
            {
                if run.is_none() && !consumed {
                    *run = old;
                } else {
                    debug_assert!(old.is_none() || consumed);
                }
            }
            while runs.last().is_some_and(Option::is_none) {
                runs.pop();
            }
            self.runs = runs;
        }
        self.total_entries = prepared.total_entries;
        self.live_entries = self.live_entries.wrapping_add(added_entries);
        debug_assert_eq!(
            self.total_entries,
            self.live_entries.wrapping_add(self.dead_entries)
        );
    }

    pub(super) const fn should_compact(&self, removed_entries: usize) -> bool {
        debug_assert!(removed_entries <= self.live_entries);
        let live_after = self.live_entries.wrapping_sub(removed_entries);
        let dead_after = self.dead_entries.wrapping_add(removed_entries);
        debug_assert!(self.total_entries == live_after.wrapping_add(dead_after));
        compaction_due(self.total_entries, dead_after)
    }

    pub(super) fn mark_removed(&mut self, removed_entries: usize) {
        debug_assert!(removed_entries <= self.live_entries);
        self.live_entries = self.live_entries.wrapping_sub(removed_entries);
        self.dead_entries = self.dead_entries.wrapping_add(removed_entries);
        debug_assert_eq!(
            self.total_entries,
            self.live_entries.wrapping_add(self.dead_entries)
        );
    }

    /// Prepare a fresh forest containing every currently live entry except
    /// `removed_handle`. Row removal is not published until this fallible
    /// construction succeeds.
    pub(super) fn prepare_compaction(
        &self,
        live: &LiveHandles,
        removed_handle: usize,
        total_rows: usize,
        removed_entries: usize,
    ) -> crate::error::Result<PreparedForest> {
        debug_assert!(removed_entries <= self.live_entries);
        let remaining = self.live_entries.wrapping_sub(removed_entries);
        let mut entries = LiveEntriesExcept {
            entries: self.live_entries(live),
            removed_handle,
            remaining,
        };
        let empty = Self::new();
        let prepared = empty.prepare_insert_from(&mut entries, total_rows)?;
        #[cfg(debug_assertions)]
        entries.debug_assert_exhausted();
        Ok(prepared)
    }

    pub(super) fn commit_compaction(&mut self, prepared: PreparedForest) {
        let _old_runs = std::mem::replace(&mut self.runs, prepared.runs);
        self.total_entries = prepared.total_entries;
        self.live_entries = prepared.total_entries;
        self.dead_entries = 0;
        debug_assert_eq!(
            self.total_entries,
            self.live_entries.wrapping_add(self.dead_entries)
        );
    }
}

/// Retaining at most one tombstone per live entry bounds every physical-entry
/// traversal by twice its live size. Rebuilds occur only after enough removals
/// to pay for a fresh STR sort.
pub(super) const fn compaction_due(total_entries: usize, dead_after: usize) -> bool {
    dead_after > total_entries / 2
}

struct LiveEntriesExcept<I> {
    entries: I,
    removed_handle: usize,
    remaining: usize,
}

impl<'a, I> Iterator for LiveEntriesExcept<I>
where
    I: Iterator<Item = &'a IndexEntry>,
{
    type Item = IndexEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let entry = self.entries.find(|entry| entry.idx != self.removed_handle);
        debug_assert!(
            entry.is_some(),
            "live forest entries ended before its physical-entry count"
        );
        let entry = entry?;
        self.remaining = self.remaining.wrapping_sub(1);
        Some(*entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a, I> ExactSizeIterator for LiveEntriesExcept<I>
where
    I: Iterator<Item = &'a IndexEntry>,
{
    fn len(&self) -> usize {
        self.remaining
    }
}

impl<'a, I> LiveEntriesExcept<I>
where
    I: Iterator<Item = &'a IndexEntry>,
{
    #[cfg(debug_assertions)]
    fn debug_assert_exhausted(&mut self) {
        debug_assert_eq!(self.remaining, 0);
        let removed_handle = self.removed_handle;
        debug_assert!(
            self.entries.all(|entry| entry.idx == removed_handle),
            "live forest iterator left an unremoved physical entry"
        );
    }
}

/// A fully constructed candidate run set. `runs` may contain `None` slots
/// where an unchanged old run is filled during insertion publication.
pub(super) struct PreparedForest {
    runs: Vec<Option<Arc<StaticStrTree>>>,
    consumed: Vec<bool>,
    total_entries: usize,
}

impl HeapSize for StaticStrForest {
    fn heap_bytes(&self) -> usize {
        self.runs.heap_bytes()
    }
}

fn required_rank_count(existing: usize, total_entries: usize) -> usize {
    debug_assert!(total_entries != 0);
    let highest_rank = total_entries.ilog2() as usize;
    existing.max(highest_rank.wrapping_add(1))
}

fn highest_power_of_two(value: usize) -> usize {
    debug_assert!(value != 0);
    1_usize.wrapping_shl(value.ilog2())
}

/// The half-open child range of chunk `node` (every chunk before it is
/// full).
///
/// `StaticStrTree::build` admits the physical entry count as `u32` before
/// reaching this constructor, and every caller's chunk lies within that range.
const fn range_at(node: usize, len: usize) -> std::ops::Range<u32> {
    let start = node.wrapping_mul(NODE_CAP) as u32;
    start..start.wrapping_add(len as u32)
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
                let x = (idx.wrapping_mul(73) % 311) as f64;
                let y = (idx.wrapping_mul(131) % 257) as f64;
                let w = (idx.wrapping_mul(7) % 11) as f64 * 0.5;
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
            let live =
                LiveHandles::from_sorted((0..count).filter(|handle| !removed.contains(handle)));
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
                tree.append_intersecting(&live, &query, &mut hits);
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
                tree.append_within_distance_2(&live, point, radius_2, &mut near);
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
        tree.append_intersecting(
            &LiveHandles::from_sorted(0..2),
            &AABB::from_corners([-1.0e308, -1.0e308], [-9.0e307, -9.0e307]),
            &mut negative_hits,
        );
        assert_eq!(negative_hits, [0]);
    }

    fn forest_entries(start: usize, count: usize) -> Vec<IndexEntry> {
        (0..count)
            .map(|offset| {
                entry(start.wrapping_add(offset), [offset as f64, 0.0], [
                    offset as f64,
                    0.0,
                ])
            })
            .collect()
    }

    fn commit_forest_batch(forest: &mut StaticStrForest, entries: &[IndexEntry]) {
        let prepared = forest
            .prepare_insert(entries, 128)
            .expect("prepared forest");
        forest.commit_insert(prepared);
    }

    #[test]
    fn binary_forest_keeps_exact_occupied_weights_for_arbitrary_batches() {
        let mut forest = StaticStrForest::new();
        let entries = forest_entries(10, 13);
        commit_forest_batch(&mut forest, &entries);

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
        let mut forest = StaticStrForest::new();
        let first = forest_entries(20, 7);
        commit_forest_batch(&mut forest, &first);
        let second = forest_entries(27, 9);
        commit_forest_batch(&mut forest, &second);

        let weights: Vec<_> = forest
            .occupied_runs()
            .map(|(rank, tree)| (rank, tree.entry_count()))
            .collect();
        assert_eq!(weights, [(4, 16)]);
        assert_eq!(forest.total_entries(), 16);
        let live = LiveHandles::from_sorted(0..36);
        let ids: Vec<_> = forest.live_entries(&live).map(|entry| entry.idx).collect();
        assert_eq!(ids.len(), 16);
        assert!(ids.into_iter().all(|idx| (20..36).contains(&idx)));
    }

    #[test]
    fn binary_forest_small_batch_matrix_preserves_every_entry() {
        for initial_count in 0..16 {
            for batch_count in 0..16 {
                let mut forest = StaticStrForest::new();
                let initial = forest_entries(50, initial_count);
                commit_forest_batch(&mut forest, &initial);
                let batch = forest_entries(50 + initial_count, batch_count);
                commit_forest_batch(&mut forest, &batch);

                let total = initial_count + batch_count;
                let live = LiveHandles::from_sorted(0..50 + total);
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
        let mut forest = StaticStrForest::new();
        let entries = forest_entries(60, 37);
        commit_forest_batch(&mut forest, &entries);
        let live =
            LiveHandles::from_sorted((0..97).filter(|handle| ![63, 74, 91].contains(handle)));
        forest.mark_removed(3);

        let intersects_queries = [
            AABB::from_corners([0.0, 0.0], [8.0, 0.0]),
            AABB::from_corners([16.0, 0.0], [27.0, 0.0]),
            AABB::from_corners([100.0, 100.0], [110.0, 110.0]),
        ];
        for query in intersects_queries {
            let mut actual = Vec::new();
            forest.append_intersecting(&live, &query, &mut actual);
            actual.sort_unstable();
            let mut expected: Vec<_> = entries
                .iter()
                .filter(|entry| live.contains(entry.idx) && intersects(&entry.envelope, &query))
                .map(|entry| entry.idx)
                .collect();
            expected.sort_unstable();
            assert_eq!(actual, expected);
        }

        for (point, radius_2) in [([2.0, 0.0], 4.0), ([29.0, 0.0], 1.0), ([100.0, 0.0], 1.0)] {
            let mut actual = Vec::new();
            forest.append_within_distance_2(&live, point, radius_2, &mut actual);
            actual.sort_unstable();
            let mut expected: Vec<_> = entries
                .iter()
                .filter(|entry| {
                    live.contains(entry.idx) && distance_2(&entry.envelope, point) <= radius_2
                })
                .map(|entry| entry.idx)
                .collect();
            expected.sort_unstable();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn compaction_rebuilds_only_live_entries_and_resets_dead_count() {
        let mut forest = StaticStrForest::new();
        let entries = forest_entries(30, 8);
        commit_forest_batch(&mut forest, &entries);
        let live = LiveHandles::from_sorted(34..38);
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
        let mut forest = StaticStrForest::new();
        let mut live = LiveHandles::default();
        StaticStrTree::reset_build_work();
        for handle in 40..40 + count {
            let entries = [entry(handle, [handle as f64, 0.0], [handle as f64, 0.0])];
            commit_forest_batch(&mut forest, &entries);
            live.insert(handle);
        }
        let mut handle = 40;
        while handle < 40 + count {
            if forest.should_compact(1) {
                let prepared = forest
                    .prepare_compaction(&live, handle, 40 + count, 1)
                    .expect("compaction");
                assert!(live.remove(handle));
                forest.commit_compaction(prepared);
            } else {
                assert!(live.remove(handle));
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
