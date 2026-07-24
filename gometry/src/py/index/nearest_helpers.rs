#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::NonNegative;
use crate::py::index::*;
use crate::py::vectors::Groups;
/// A frontier node of the best-first nearest traversal — a bulk STR node
/// (level, index), an overflow R-tree node, or an evaluated-bound entry —
/// with its envelope's distance lower bound, min-ordered by that bound.
pub(crate) enum Frontier<'a> {
    Bulk(usize, u32),
    Overflow(&'a RTreeNode<IndexEntry>),
    Entry(usize),
}

pub(crate) struct FrontierNode<'a> {
    pub(crate) bound: f64,
    pub(crate) node: Frontier<'a>,
}

impl<'a> FrontierNode<'a> {
    pub(crate) fn overflow(
        bound: &impl Fn(&AABB<[f64; 2]>) -> f64,
        node: &'a RTreeNode<IndexEntry>,
    ) -> Self {
        let envelope = match node {
            RTreeNode::Parent(parent) => parent.envelope(),
            RTreeNode::Leaf(entry) => entry.envelope,
        };
        Self {
            bound: bound(&envelope),
            node: Frontier::Overflow(node),
        }
    }
}

impl PartialEq for FrontierNode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.bound.total_cmp(&other.bound).is_eq()
    }
}

impl Eq for FrontierNode<'_> {}

impl PartialOrd for FrontierNode<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FrontierNode<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed: BinaryHeap is a max-heap, the traversal needs min-bound
        // first.
        other.bound.total_cmp(&self.bound)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NearestCandidate {
    pub(crate) distance: f64,
    pub(crate) idx: usize,
}

/// One row in a cap-lower-bound nearest scan, min-ordered by bound so a
/// `BinaryHeap` can stop without globally sorting rows that will never be
/// exact-evaluated.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CapBound {
    pub(crate) bound: f64,
    pub(crate) idx: usize,
}

impl PartialEq for CapBound {
    fn eq(&self, other: &Self) -> bool {
        self.bound.total_cmp(&other.bound).is_eq() && self.idx == other.idx
    }
}

impl Eq for CapBound {}

impl PartialOrd for CapBound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CapBound {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .bound
            .total_cmp(&self.bound)
            .then_with(|| other.idx.cmp(&self.idx))
    }
}

impl NearestCandidate {
    pub(crate) const fn new(distance: f64, idx: usize) -> Self {
        Self { distance, idx }
    }
}

impl PartialEq for NearestCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance.total_cmp(&other.distance).is_eq() && self.idx == other.idx
    }
}

impl Eq for NearestCandidate {}

impl PartialOrd for NearestCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NearestCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.idx.cmp(&other.idx))
    }
}

pub(crate) fn push_nearest_candidate(
    nearest: &mut BinaryHeap<NearestCandidate>,
    k: usize,
    candidate: NearestCandidate,
) {
    if nearest.len() < k {
        nearest.push(candidate);
    } else if nearest
        .peek()
        .is_some_and(|worst| candidate.cmp(worst).is_lt())
    {
        nearest.pop();
        nearest.push(candidate);
    }
}

/// Squared box-to-box distance between two axis-aligned envelopes (0 when they
/// overlap). A lower bound on the distance between any geometries they contain.
pub(crate) fn aabb_box_distance_2(a: &AABB<[f64; 2]>, b: &AABB<[f64; 2]>) -> f64 {
    let (a_lower, a_upper) = (a.lower(), a.upper());
    let (b_lower, b_upper) = (b.lower(), b.upper());
    let dx = (a_lower[0] - b_upper[0])
        .max(b_lower[0] - a_upper[0])
        .max(0.0);
    let dy = (a_lower[1] - b_upper[1])
        .max(b_lower[1] - a_upper[1])
        .max(0.0);
    dx * dx + dy * dy
}

pub(crate) fn nearest_candidates_from_heap(
    nearest: BinaryHeap<NearestCandidate>,
) -> Vec<NearestCandidate> {
    let mut nearest = nearest.into_vec();
    nearest.sort_unstable();
    nearest
}

/// Validate the optional `max_distance` ceiling for a nearest query.
pub(crate) fn parse_max_distance(max_distance: Option<f64>) -> PyResult<Option<NonNegative>> {
    match max_distance {
        Some(distance) => Ok(Some(NonNegative::try_new("max_distance", distance)?)),
        None => Ok(None),
    }
}

/// One CSR batch of per-query matches: a flat ids buffer plus row
/// offsets (`rows()` yields each sorted window) — the zero-per-row-alloc
/// hand-off into `Groups`/join columns.
pub(crate) struct RowMatches {
    pub(crate) ids: Vec<usize>,
    pub(crate) offsets: Vec<usize>,
}

impl RowMatches {
    pub(crate) fn rows(&self) -> impl Iterator<Item = &[usize]> {
        self.offsets
            .array_windows::<2>()
            .map(|[start, end]| &self.ids[*start..*end])
    }
}

/// Compact `ids[row_start..]` in place to the entries `keep` accepts —
/// the per-row exact-refine pass of the CSR builders.
pub(crate) fn retain_row(
    ids: &mut Vec<usize>,
    row_start: usize,
    mut keep: impl FnMut(usize) -> PyResult<bool>,
) -> PyResult<()> {
    let mut write = row_start;
    for read in row_start..ids.len() {
        let idx = ids[read];
        if keep(idx)? {
            ids[write] = idx;
            write += 1;
        }
    }
    ids.truncate(write);
    Ok(())
}

/// Column-form array nearest: CSR ids, plus one flat distances column
/// parallel to `matches.values` when asked (the rows share the CSR offsets).
pub(crate) fn format_nearest_rows(
    py: Python<'_>,
    ids: Vec<usize>,
    offsets: Vec<usize>,
    distances: Vec<f64>,
    return_distance: bool,
) -> PyResult<Py<PyAny>> {
    let matches = Groups::from_int64_csr(ids, offsets)?;
    if return_distance {
        let distances = float64_array(py, distances)?;
        return (matches, distances).into_py_any(py);
    }
    matches.into_py_any(py)
}

pub(crate) fn format_nearest(
    py: Python<'_>,
    candidates: Vec<NearestCandidate>,
    return_distance: bool,
) -> PyResult<Py<PyAny>> {
    let mut ids = Vec::with_capacity(candidates.len());
    let mut distances = Vec::with_capacity(usize::from(return_distance) * candidates.len());
    for candidate in candidates {
        ids.push(candidate.idx as i64);
        if return_distance {
            distances.push(candidate.distance);
        }
    }
    let ids = int64_array(py, ids)?;
    if return_distance {
        let distances = float64_array(py, distances)?;
        return (ids, distances).into_py_any(py);
    }
    ids.into_py_any(py)
}

/// Every leaf id beneath `parent`, in tree order.
pub(crate) fn collect_subtree_ids(parent: &ParentNode<IndexEntry>, out: &mut Vec<usize>) {
    for child in parent.children() {
        match child {
            RTreeNode::Parent(parent) => collect_subtree_ids(parent, out),
            RTreeNode::Leaf(entry) => out.push(entry.idx),
        }
    }
}

impl RTreeObject for IndexEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

impl PointDistance for IndexEntry {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        self.envelope.distance_2(point)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_bound_heap_pops_smallest_bound_then_row() {
        let mut heap = BinaryHeap::from(vec![
            CapBound { bound: 2.0, idx: 0 },
            CapBound { bound: 1.0, idx: 4 },
            CapBound { bound: 1.0, idx: 2 },
        ]);
        assert_eq!(
            heap.pop().map(|entry| (entry.bound, entry.idx)),
            Some((1.0, 2))
        );
        assert_eq!(
            heap.pop().map(|entry| (entry.bound, entry.idx)),
            Some((1.0, 4))
        );
        assert_eq!(
            heap.pop().map(|entry| (entry.bound, entry.idx)),
            Some((2.0, 0))
        );
    }
}
