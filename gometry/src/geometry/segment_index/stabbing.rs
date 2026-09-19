//! Private Y-stabbing index for prepared point membership.
//!
//! One structure, three representations chosen by a **checked sizing pass**:
//! - **Linear** — tiny collections (full scan with per-item Y filter).
//! - **DenseBands** — CSR edge→band replication only when total references stay
//!   proportional to the item count (checked arithmetic; never silent `u32` wrap).
//! - **IntervalTree** — every item stored once; O(N) memory and O(log N + k)
//!   stabbing. Safe harbor for tall-edge / many-item cases that would blow up
//!   dense band replication.
//!
//! Exact membership predicates stay outside this module; the index only reports
//! Y-overlapping candidates.

use crate::HeapSize;
use crate::error::Result;

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct EdgeId(u32);

impl EdgeId {
    pub(crate) const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

/// Maximum items handled by a flat linear scan.
const LINEAR_MAX: usize = 32;

/// Whether a collection uses the flat plan rather than an indexed plan.
pub(crate) const fn uses_linear_plan_for_len(len: usize) -> bool {
    len <= LINEAR_MAX
}

/// Dense bands are admitted only when total band references stay within
/// `DENSE_REF_FACTOR * n` (and fit in `u32` CSR storage). Sized so ordinary
/// short-span hole sets (≈10 bands/item) stay dense; full-height tall edges
/// still trip the factor and fall back to the interval tree.
const DENSE_REF_FACTOR: usize = 16;

/// Sentinel child / root for the flat interval tree.
const NIL: u32 = u32::MAX;

/// One node of a centered (median-of-sorted-lo) interval tree.
#[derive(Clone, Debug)]
struct ITreeNode {
    item: u32,
    lo: f64,
    hi: f64,
    /// Max `hi` in this subtree (inclusive).
    max_hi: f64,
    left: u32,
    right: u32,
}

/// Representation chosen by the sizing pass.
#[derive(Clone, Debug)]
enum YPlan {
    Linear,
    DenseBands {
        min_y: f64,
        inv_band_height: f64,
        band_count: std::num::NonZeroUsize,
        band_offsets: Vec<u32>,
        /// Indices into [`YStabbingIndex::items`].
        band_items: Vec<u32>,
    },
    IntervalTree {
        nodes: Vec<ITreeNode>,
        root: u32,
    },
}

/// Packed Y-stabbing index over owned items.
///
/// Query reports every item whose closed Y-interval contains the probe Y.
/// Callers apply any further X/Y bounds checks and exact predicates.
#[derive(Clone, Debug)]
pub(crate) struct YStabbingIndex<T> {
    items: Vec<T>,
    /// Closed Y-span per item — kept for Linear filtering and build bookkeeping.
    spans: Vec<(f64, f64)>,
    /// Closed X-span per item, stored beside the Y-span so a point query can
    /// reject a candidate without touching the item. Overlapping Y-bands are
    /// the common case, not the exception — a row of boxes, a country's
    /// islands, a coverage — so point queries apply this second coordinate
    /// filter after the fallible Y plan.
    x_spans: Vec<(f64, f64)>,
    plan: YPlan,
}

impl<T> YStabbingIndex<T> {
    #[cfg(test)]
    pub(crate) const fn uses_indexed_plan(&self) -> bool {
        !matches!(self.plan, YPlan::Linear)
    }
    /// Build with a checked sizing pass selecting linear / dense bands / tree.
    /// `envelope` returns the item's closed `(min_x, max_x, min_y, max_y)`; an
    /// index whose domain has no usable X extent (a periodic longitude, say)
    /// passes an infinite X-span and owns that filter itself.
    pub(crate) fn build(
        items: Vec<T>,
        mut envelope: impl FnMut(&T) -> (f64, f64, f64, f64),
    ) -> Result<Self> {
        let mut spans = Vec::new();
        let mut x_spans = Vec::new();
        crate::try_reserve_vec(&mut spans, items.len())?;
        crate::try_reserve_vec(&mut x_spans, items.len())?;
        for item in &items {
            let (min_x, max_x, min_y, max_y) = envelope(item);
            spans.push((min_y, max_y));
            x_spans.push((min_x, max_x));
        }
        let plan = choose_plan(&spans)?;
        Ok(Self {
            items,
            spans,
            x_spans,
            plan,
        })
    }

    /// Visit every item whose Y-interval contains `y` (closed).
    ///
    /// Stops early when `visit` returns [`ControlFlow::Break`].
    pub(crate) fn for_each_at_y<B>(
        &self,
        y: f64,
        mut visit: impl FnMut(&T) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        use std::ops::ControlFlow;
        match &self.plan {
            YPlan::Linear => {
                for (item, &(lo, hi)) in self.items.iter().zip(self.spans.iter()) {
                    if lo <= y
                        && y <= hi
                        && let ControlFlow::Break(b) = visit(item)
                    {
                        return Some(b);
                    }
                }
                None
            },
            YPlan::DenseBands {
                min_y,
                inv_band_height,
                band_count,
                band_offsets,
                band_items,
            } => {
                let band = band_of(y, *min_y, *inv_band_height, band_count.get());
                let start = band_offsets[band] as usize;
                let end = band_offsets[band + 1] as usize;
                // Band placement is a conservative superset of closed Y-spans.
                // Re-filter to the closed interval so non-edge callers (holes /
                // parts) never see a non-covering candidate. Edge raycasts
                // still pay a cheap compare before the exact straddle test.
                for &idx in &band_items[start..end] {
                    let (lo, hi) = self.spans[idx as usize];
                    if lo <= y
                        && y <= hi
                        && let ControlFlow::Break(b) = visit(&self.items[idx as usize])
                    {
                        return Some(b);
                    }
                }
                None
            },
            YPlan::IntervalTree { nodes, root } => {
                query_itree(nodes, *root, y, &mut |idx| visit(&self.items[idx as usize]))
            },
        }
    }

    /// Visit every item whose closed envelope contains `(x, y)`.
    ///
    /// Stops early when `visit` returns [`ControlFlow::Break`].
    pub(crate) fn for_each_at_point<B>(
        &self,
        x: f64,
        y: f64,
        mut visit: impl FnMut(&T) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        self.for_each_at_point_index(x, y, |index| visit(&self.items[index]))
    }

    /// Visit every item index whose closed envelope contains `(x, y)`.
    ///
    /// This keeps the index's callback lifetime from constraining callers that
    /// need to borrow a view from the selected item after the traversal.
    pub(crate) fn for_each_at_point_index<B>(
        &self,
        x: f64,
        y: f64,
        mut visit: impl FnMut(usize) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        use std::ops::ControlFlow;
        self.for_each_at_y_indexed(y, |item_index| {
            let (min_x, max_x) = self.x_spans[item_index];
            if x < min_x || x > max_x {
                return ControlFlow::Continue(());
            }
            visit(item_index)
        })
    }

    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        self.items.get(index)
    }

    pub(crate) fn items(&self) -> &[T] {
        &self.items
    }

    /// The Y walk, handing the caller each surviving item's index so a filter
    /// can run before the item itself is touched.
    fn for_each_at_y_indexed<B>(
        &self,
        y: f64,
        mut visit: impl FnMut(usize) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        use std::ops::ControlFlow;
        match &self.plan {
            YPlan::Linear => {
                for (index, &(lo, hi)) in self.spans.iter().enumerate() {
                    if lo <= y
                        && y <= hi
                        && let ControlFlow::Break(b) = visit(index)
                    {
                        return Some(b);
                    }
                }
                None
            },
            YPlan::DenseBands {
                min_y,
                inv_band_height,
                band_count,
                band_offsets,
                band_items,
            } => {
                let band = band_of(y, *min_y, *inv_band_height, band_count.get());
                let start = band_offsets[band] as usize;
                let end = band_offsets[band + 1] as usize;
                for &idx in &band_items[start..end] {
                    let (lo, hi) = self.spans[idx as usize];
                    if lo <= y
                        && y <= hi
                        && let ControlFlow::Break(b) = visit(idx as usize)
                    {
                        return Some(b);
                    }
                }
                None
            },
            YPlan::IntervalTree { nodes, root } => {
                query_itree(nodes, *root, y, &mut |idx| visit(idx as usize))
            },
        }
    }

    /// Visit every item (no Y filter) — useful for tiny Linear collections
    /// where the caller applies a tighter XY bounds gate.
    pub(crate) fn for_each(&self, mut visit: impl FnMut(&T)) {
        for item in &self.items {
            visit(item);
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.items.len()
    }

    /// Which representation the sizing pass selected (tests / diagnostics).
    #[cfg(test)]
    pub(crate) const fn plan_kind(&self) -> &'static str {
        match &self.plan {
            YPlan::Linear => "linear",
            YPlan::DenseBands { .. } => "dense",
            YPlan::IntervalTree { .. } => "tree",
        }
    }
}

/// Specialized Y-stabbing over ring edge ids — no `items` indirection.
/// Dense CSR stores edge ids directly (the prior `RingRaycaster` layout).
#[derive(Clone, Debug)]
pub(crate) struct EdgeYIndex {
    edge_count: usize,
    plan: YPlan,
}

impl EdgeYIndex {
    #[cfg(test)]
    pub(crate) const fn uses_indexed_plan(&self) -> bool {
        !matches!(self.plan, YPlan::Linear)
    }
    /// Build from parallel ring Y columns (`edge i` spans `ys[i]..ys[i+1]`).
    /// Items in the shared plan are edge ids (`0..edge_count`).
    pub(crate) fn build(ys: &[f64]) -> Result<Self> {
        let edge_count = ys.len().saturating_sub(1);
        let mut spans = Vec::new();
        crate::try_reserve_vec(&mut spans, edge_count)?;
        spans.extend((0..edge_count).map(|e| {
            let (a, b) = (ys[e], ys[e + 1]);
            (a.min(b), a.max(b))
        }));
        // Dense band_items are item indices; with items[i]=i they are edge ids.
        Ok(Self {
            edge_count,
            plan: choose_plan(&spans)?,
        })
    }

    /// Build over an open ring, including its closing edge from the last point
    /// back to the first. Chart rings use this representation so seam lifting
    /// does not need to repeat the first coordinate.
    pub(crate) fn build_closed(ys: &[f64]) -> Result<Self> {
        let edge_count = ys.len();
        let mut spans = Vec::new();
        crate::try_reserve_vec(&mut spans, edge_count)?;
        spans.extend((0..edge_count).map(|edge| {
            let next = if edge + 1 < edge_count { edge + 1 } else { 0 };
            let (a, b) = (ys[edge], ys[next]);
            (a.min(b), a.max(b))
        }));
        Ok(Self {
            edge_count,
            plan: choose_plan(&spans)?,
        })
    }

    /// Borrow the dense CSR candidates for probe `y` without exposing the
    /// coordinate columns owned by the caller.
    pub(crate) fn dense_band_edges(&self, y: f64) -> Option<&[EdgeId]> {
        match &self.plan {
            YPlan::DenseBands {
                min_y,
                inv_band_height,
                band_count,
                band_offsets,
                band_items,
            } => {
                let range =
                    dense_band_range(y, *min_y, *inv_band_height, band_count.get(), band_offsets);
                // SAFETY: EdgeId is a transparent u32 wrapper and the slice is
                // only interpreted as edge ids from this index.
                Some(unsafe { &*(&raw const band_items[range] as *const [EdgeId]) })
            },
            _ => None,
        }
    }

    /// Visit candidate edge ids for probe `y`.
    ///
    /// Dense bands walk the same CSR range as [`Self::dense_band_edges`].
    /// Stops early when `visit` returns [`ControlFlow::Break`].
    pub(crate) fn for_each_edge<B>(
        &self,
        y: f64,
        mut visit: impl FnMut(EdgeId) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        use std::ops::ControlFlow;
        match &self.plan {
            YPlan::Linear => {
                for edge in 0..self.edge_count as u32 {
                    if let ControlFlow::Break(b) = visit(EdgeId(edge)) {
                        return Some(b);
                    }
                }
                None
            },
            YPlan::DenseBands {
                min_y,
                inv_band_height,
                band_count,
                band_offsets,
                band_items,
            } => {
                let range =
                    dense_band_range(y, *min_y, *inv_band_height, band_count.get(), band_offsets);
                for &id in &band_items[range] {
                    if let ControlFlow::Break(b) = visit(EdgeId(id)) {
                        return Some(b);
                    }
                }
                None
            },
            YPlan::IntervalTree { nodes, root } => {
                query_itree(nodes, *root, y, &mut |edge| visit(EdgeId(edge)))
            },
        }
    }
}

impl HeapSize for EdgeYIndex {
    fn heap_bytes(&self) -> usize {
        match &self.plan {
            YPlan::Linear => 0,
            YPlan::DenseBands {
                band_offsets,
                band_items,
                ..
            } => band_offsets.heap_bytes() + band_items.heap_bytes(),
            YPlan::IntervalTree { nodes, .. } => nodes.heap_bytes(),
        }
    }
}

impl<T: HeapSize> HeapSize for YStabbingIndex<T> {
    fn heap_bytes(&self) -> usize {
        self.items.heap_bytes()
            + self.spans.heap_bytes()
            + self.x_spans.heap_bytes()
            + match &self.plan {
                YPlan::Linear => 0,
                YPlan::DenseBands {
                    band_offsets,
                    band_items,
                    ..
                } => band_offsets.heap_bytes() + band_items.heap_bytes(),
                YPlan::IntervalTree { nodes, .. } => nodes.heap_bytes(),
            }
    }
}

impl HeapSize for ITreeNode {
    fn heap_bytes(&self) -> usize {
        0
    }
}

fn band_of(y: f64, min_y: f64, inv_band_height: f64, band_count: usize) -> usize {
    // Float→usize saturates out-of-range; negatives become 0.
    (((y - min_y) * inv_band_height) as usize).min(band_count.saturating_sub(1))
}

fn dense_band_range(
    y: f64,
    min_y: f64,
    inv_band_height: f64,
    band_count: usize,
    offsets: &[u32],
) -> std::ops::Range<usize> {
    let band = band_of(y, min_y, inv_band_height, band_count);
    offsets[band] as usize..offsets[band + 1] as usize
}

/// Checked dense-band sizing. Returns `Some((min_y, inv, band_count, total_refs))`
/// only when references stay proportional and fit `u32` CSR storage.
fn try_dense_sizing(spans: &[(f64, f64)]) -> Option<(f64, f64, std::num::NonZeroUsize, usize)> {
    let n = spans.len();
    if n == 0 || n > u32::MAX as usize {
        return None;
    }
    let (min_y, max_y) = spans
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), &(a, b)| {
            (lo.min(a), hi.max(b))
        });
    if !min_y.is_finite() || !max_y.is_finite() {
        return None;
    }
    let band_count = n;
    let span = (max_y - min_y).max(f64::MIN_POSITIVE);
    let inv_band_height = band_count as f64 / span;
    let max_refs = DENSE_REF_FACTOR.saturating_mul(n);
    let mut total_refs: usize = 0;
    for &(lo, hi) in spans {
        let b0 = band_of(lo, min_y, inv_band_height, band_count);
        let b1 = band_of(hi, min_y, inv_band_height, band_count);
        let span_bands = b1 - b0 + 1;
        total_refs = total_refs.checked_add(span_bands)?;
        if total_refs > max_refs {
            return None;
        }
    }
    // CSR offsets and item ids are `u32`.
    if total_refs > u32::MAX as usize {
        return None;
    }
    Some((
        min_y,
        inv_band_height,
        std::num::NonZeroUsize::new(band_count)?,
        total_refs,
    ))
}

fn choose_plan(spans: &[(f64, f64)]) -> Result<YPlan> {
    let n = spans.len();
    if n > u32::MAX as usize {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    if n <= LINEAR_MAX {
        return Ok(YPlan::Linear);
    }
    if let Some((min_y, inv_band_height, band_count, total_refs)) = try_dense_sizing(spans) {
        return build_dense(spans, min_y, inv_band_height, band_count, total_refs);
    }
    build_itree(spans)
}

fn build_dense(
    spans: &[(f64, f64)],
    min_y: f64,
    inv_band_height: f64,
    band_count: std::num::NonZeroUsize,
    total_refs: usize,
) -> Result<YPlan> {
    let n = spans.len();
    let band_count_value = band_count.get();
    let offset_count = band_count_value
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    // Two-pass CSR with checked counts (sizing already proved fit).
    let mut band_offsets = Vec::new();
    crate::try_reserve_vec(&mut band_offsets, offset_count)?;
    band_offsets.resize(offset_count, 0_u32);
    for &(lo, hi) in spans {
        let b0 = band_of(lo, min_y, inv_band_height, band_count_value);
        let b1 = band_of(hi, min_y, inv_band_height, band_count_value);
        for band in b0..=b1 {
            // Sizing proved no wrap; debug_assert for the contract.
            debug_assert!(band_offsets[band + 1] < u32::MAX);
            band_offsets[band + 1] += 1;
        }
    }
    for band in 0..band_count_value {
        band_offsets[band + 1] = band_offsets[band + 1]
            .checked_add(band_offsets[band])
            .expect("dense band offsets checked at sizing");
    }
    debug_assert_eq!(band_offsets[band_count_value] as usize, total_refs);
    let mut cursor = Vec::new();
    crate::try_reserve_vec(&mut cursor, band_offsets.len())?;
    cursor.extend_from_slice(&band_offsets);
    let mut band_items = Vec::new();
    crate::try_reserve_vec(&mut band_items, total_refs)?;
    band_items.resize(total_refs, 0_u32);
    for (idx, &(lo, hi)) in spans.iter().enumerate() {
        let b0 = band_of(lo, min_y, inv_band_height, band_count_value);
        let b1 = band_of(hi, min_y, inv_band_height, band_count_value);
        let edge = idx as u32;
        for c in &mut cursor[b0..=b1] {
            let slot = *c as usize;
            band_items[slot] = edge;
            *c += 1;
        }
    }
    let _ = n;
    Ok(YPlan::DenseBands {
        min_y,
        inv_band_height,
        band_count,
        band_offsets,
        band_items,
    })
}

fn build_itree(spans: &[(f64, f64)]) -> Result<YPlan> {
    let n = spans.len();
    if n == 0 {
        return Ok(YPlan::IntervalTree {
            nodes: Vec::new(),
            root: NIL,
        });
    }
    // Indices sorted by lo (then hi, then id for stability).
    let mut order = Vec::new();
    crate::try_reserve_vec(&mut order, n)?;
    order.extend(0..n as u32);
    order.sort_unstable_by(|&a, &b| {
        let (alo, ahi) = spans[a as usize];
        let (blo, bhi) = spans[b as usize];
        alo.total_cmp(&blo)
            .then_with(|| ahi.total_cmp(&bhi))
            .then_with(|| a.cmp(&b))
    });
    let mut nodes = Vec::new();
    crate::try_reserve_vec(&mut nodes, n)?;
    let root = build_itree_node(&mut order, spans, &mut nodes);
    Ok(YPlan::IntervalTree { nodes, root })
}

/// Median-of-sorted-lo recursive build. Returns node index (`NIL` if empty).
fn build_itree_node(order: &mut [u32], spans: &[(f64, f64)], nodes: &mut Vec<ITreeNode>) -> u32 {
    if order.is_empty() {
        return NIL;
    }
    let mid = order.len() / 2;
    let item = order[mid];
    let (lo, hi) = spans[item as usize];
    // Split without allocating: recurse on left, then right halves.
    let left = build_itree_node(&mut order[..mid], spans, nodes);
    let right = build_itree_node(&mut order[mid + 1..], spans, nodes);
    let mut max_hi = hi;
    if left != NIL {
        max_hi = max_hi.max(nodes[left as usize].max_hi);
    }
    if right != NIL {
        max_hi = max_hi.max(nodes[right as usize].max_hi);
    }
    let id = nodes.len() as u32;
    nodes.push(ITreeNode {
        item,
        lo,
        hi,
        max_hi,
        left,
        right,
    });
    id
}

/// Allocation-free, early-terminating interval-tree stabbing.
///
/// Recursion depth is bounded by the median-of-sorted-lo construction (tree
/// height O(log n)); a hit returning [`ControlFlow::Break`] aborts descent
/// immediately — the old `Vec` stack could not stop mid-query and paid one
/// heap alloc/free per probe.
fn query_itree<B>(
    nodes: &[ITreeNode],
    root: u32,
    y: f64,
    visit: &mut impl FnMut(u32) -> std::ops::ControlFlow<B>,
) -> Option<B> {
    if root == NIL {
        return None;
    }
    query_itree_node(nodes, root, y, visit)
}

fn query_itree_node<B>(
    nodes: &[ITreeNode],
    idx: u32,
    y: f64,
    visit: &mut impl FnMut(u32) -> std::ops::ControlFlow<B>,
) -> Option<B> {
    use std::ops::ControlFlow;
    let node = &nodes[idx as usize];
    if node.max_hi < y {
        return None;
    }
    // Left first (lower lo values) — same visit order as the prior stack walk.
    if node.left != NIL
        && let Some(b) = query_itree_node(nodes, node.left, y, visit)
    {
        return Some(b);
    }
    if node.lo > y {
        // This interval and every right-child interval have lo > y.
        return None;
    }
    if y <= node.hi
        && let ControlFlow::Break(b) = visit(node.item)
    {
        return Some(b);
    }
    if node.right != NIL {
        return query_itree_node(nodes, node.right, y, visit);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_at(index: &YStabbingIndex<u32>, y: f64) -> Vec<u32> {
        let mut out = Vec::new();
        let _ = index.for_each_at_y(y, |&item| {
            out.push(item);
            std::ops::ControlFlow::<()>::Continue(())
        });
        out.sort_unstable();
        out
    }

    #[test]
    fn linear_for_tiny_collections() {
        let items: Vec<u32> = (0..8).collect();
        let index = YStabbingIndex::build(items, |&i| {
            let y = f64::from(i);
            (f64::NEG_INFINITY, f64::INFINITY, y, y + 0.5)
        })
        .unwrap();
        assert_eq!(index.plan_kind(), "linear");
        assert_eq!(collect_at(&index, 3.25), vec![3]);
        assert!(collect_at(&index, 100.0).is_empty());
    }

    #[test]
    fn dense_for_short_edges() {
        // Many short non-overlapping Y spans → proportional dense bands.
        let n = 128_u32;
        let items: Vec<u32> = (0..n).collect();
        let index = YStabbingIndex::build(items, |&i| {
            let y = f64::from(i);
            (f64::NEG_INFINITY, f64::INFINITY, y, y + 0.5)
        })
        .unwrap();
        assert_eq!(index.plan_kind(), "dense");
        assert_eq!(collect_at(&index, 40.25), vec![40]);
        // Endpoint-closed.
        assert_eq!(collect_at(&index, 40.0), vec![40]);
        assert_eq!(collect_at(&index, 40.5), vec![40]);
    }

    #[test]
    fn tree_exact_hit_early_exits_via_control_flow_break() {
        // Tall edges → interval tree. An exact hit must stop after the first
        // matching item and not visit the rest of the set.
        let n = 64_u32;
        let items: Vec<u32> = (0..n).collect();
        let index =
            YStabbingIndex::build(items, |_| (f64::NEG_INFINITY, f64::INFINITY, 0.0, 1000.0))
                .unwrap();
        assert_eq!(index.plan_kind(), "tree");
        let mut visits = 0_u32;
        let broken = index.for_each_at_y(500.0, |&item| {
            visits += 1;
            if item == 0 {
                std::ops::ControlFlow::Break(item)
            } else {
                std::ops::ControlFlow::Continue(())
            }
        });
        assert_eq!(broken, Some(0));
        assert_eq!(
            visits, 1,
            "ControlFlow::Break must stop after the exact hit"
        );
    }

    #[test]
    fn tall_edges_fall_back_to_interval_tree() {
        // Every item spans the full Y range → dense refs = n * n, not proportional.
        let n = 64_u32;
        let items: Vec<u32> = (0..n).collect();
        let index =
            YStabbingIndex::build(items, |_| (f64::NEG_INFINITY, f64::INFINITY, 0.0, 1000.0))
                .unwrap();
        assert_eq!(index.plan_kind(), "tree");
        let hit = collect_at(&index, 500.0);
        assert_eq!(hit.len(), n as usize);
        // Memory: items once + tree nodes once — O(n), not O(n²).
        let bytes = index.heap_bytes();
        let per_item = bytes / n as usize;
        assert!(
            per_item < 256,
            "tall-edge storage not proportional: {bytes} bytes for {n} items ({per_item}/item)"
        );
    }

    #[test]
    fn interval_tree_reports_only_covering_intervals() {
        // n > LINEAR_MAX so we leave the linear path; short spans may still
        // pick dense bands — either representation must be exact.
        let n = 64_u32;
        let items: Vec<u32> = (0..n).collect();
        let index = YStabbingIndex::build(items, |&i| {
            // Item i covers [i, i+10]
            let y = f64::from(i);
            (f64::NEG_INFINITY, f64::INFINITY, y, y + 10.0)
        })
        .unwrap();
        let hit = collect_at(&index, 20.0);
        // Items with lo <= 20 <= hi → i in [10, 20]
        let expected: Vec<u32> = (10..=20).collect();
        assert_eq!(hit, expected);
    }

    #[test]
    fn dense_refuses_u32_wrap_class_amplification() {
        // Simulate many bands with full-span items: sizing must not admit dense.
        let n = 1000_u32;
        let items: Vec<u32> = (0..n).collect();
        let index =
            YStabbingIndex::build(items, |_| (f64::NEG_INFINITY, f64::INFINITY, 0.0, 1.0)).unwrap();
        assert_eq!(index.plan_kind(), "tree");
        // Construction stays O(n).
        let bytes = index.heap_bytes();
        assert!(bytes < 1000 * 200, "expected O(n) heap, got {bytes}");
    }

    fn collect_edge_ids(index: &EdgeYIndex, y: f64) -> Vec<u32> {
        let mut out = Vec::new();
        let _ = index.for_each_edge(y, |edge| {
            out.push(edge.as_usize() as u32);
            std::ops::ControlFlow::<()>::Continue(())
        });
        out
    }

    #[test]
    fn edge_y_dense_for_each_edge_matches_band_slice_and_stops_on_break() {
        let ys: Vec<f64> = (0..=128).map(f64::from).collect();
        let index = EdgeYIndex::build(&ys).unwrap();
        assert!(index.uses_indexed_plan());
        let y = 40.25;
        let slice = index
            .dense_band_edges(y)
            .expect("short unit-span edges use dense bands");
        assert_eq!(
            collect_edge_ids(&index, y),
            slice
                .iter()
                .map(|edge| edge.as_usize() as u32)
                .collect::<Vec<_>>()
        );

        let mut visits = 0_usize;
        let broken = index.for_each_edge(y, |edge| {
            visits += 1;
            std::ops::ControlFlow::Break(edge)
        });
        assert_eq!(visits, 1);
        assert_eq!(
            broken.map(EdgeId::as_usize),
            slice.first().map(|edge| edge.as_usize())
        );
    }

    #[test]
    fn edge_y_dense_for_each_edge_matches_slice_below_above_and_on_band_edge() {
        let ys: Vec<f64> = (0..=128).map(f64::from).collect();
        let index = EdgeYIndex::build(&ys).unwrap();
        let slice = index
            .dense_band_edges(40.0)
            .expect("short unit-span edges use dense bands");
        for y in [-1.0, 0.0, 40.0, 128.0, 200.0] {
            let from_slice = index
                .dense_band_edges(y)
                .expect("dense plan")
                .iter()
                .map(|edge| edge.as_usize() as u32)
                .collect::<Vec<_>>();
            assert_eq!(collect_edge_ids(&index, y), from_slice, "y={y}");
        }
        assert!(!slice.is_empty());
    }

    #[test]
    fn heap_bytes_includes_x_spans_on_linear_plan() {
        let items: Vec<u32> = (0..8).collect();
        let index = YStabbingIndex::build(items, |&i| {
            let y = f64::from(i);
            (0.0, 1.0, y, y + 0.5)
        })
        .unwrap();
        assert_eq!(index.plan_kind(), "linear");
        assert_eq!(
            index.heap_bytes(),
            index.items.heap_bytes() + index.spans.heap_bytes() + index.x_spans.heap_bytes()
        );
        assert_eq!(
            index.x_spans.heap_bytes(),
            8 * std::mem::size_of::<(f64, f64)>()
        );
    }

    #[test]
    fn interval_plan_keeps_x_filter_after_y_indexing() {
        let n = 64_u32;
        let finite = YStabbingIndex::build((0..n).collect(), |&i| {
            let x = f64::from(i);
            (x, x + 1.0, 0.0, 1000.0)
        })
        .unwrap();
        let infinite = YStabbingIndex::build((0..n).collect(), |_| {
            (f64::NEG_INFINITY, f64::INFINITY, 0.0, 1000.0)
        })
        .unwrap();
        assert_eq!(finite.plan_kind(), "tree");
        assert_eq!(infinite.plan_kind(), "tree");
        let mut finite_hits = Vec::new();
        finite.for_each_at_point(0.5, 500.0, |item| {
            finite_hits.push(*item);
            std::ops::ControlFlow::<()>::Continue(())
        });
        assert_eq!(finite_hits, [0]);
        let mut infinite_hits = Vec::new();
        infinite.for_each_at_point(0.5, 500.0, |item| {
            infinite_hits.push(*item);
            std::ops::ControlFlow::<()>::Continue(())
        });
        assert_eq!(infinite_hits.len(), n as usize);
        assert_eq!(infinite_hits.first(), Some(&0));
        assert_eq!(infinite_hits.last(), Some(&(n - 1)));
    }
}
