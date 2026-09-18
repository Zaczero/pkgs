use pyo3::IntoPyObjectExt as _;

use crate::NonNegative;
use crate::py::index::{
    AABB, BinaryHeap, Ordering, Py, PyAny, PyErr, PyResult, Python, float64_array, int64_array,
};
use crate::py::vectors::Groups;
/// A frontier node of the best-first nearest traversal — a bulk STR node, an
/// immutable overflow-run node, or an evaluated-bound entry — with its
/// envelope's distance lower bound, min-ordered by that bound.
pub(crate) enum Frontier {
    Bulk(usize, u32),
    Run(usize, usize, u32),
    Entry(usize),
}

pub(crate) struct FrontierNode {
    pub(crate) bound: f64,
    pub(crate) node: Frontier,
}

impl PartialEq for FrontierNode {
    fn eq(&self, other: &Self) -> bool {
        self.bound.total_cmp(&other.bound).is_eq()
    }
}

impl Eq for FrontierNode {}

impl PartialOrd for FrontierNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FrontierNode {
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
) -> PyResult<()> {
    if nearest.len() < k {
        crate::try_push_heap(nearest, candidate).map_err(PyErr::from)?;
    } else if let Some(mut worst) = nearest.peek_mut() {
        // Replace-in-place via peek_mut (one sift) instead of pop+push (two).
        // Kept only when the public nearest row improves beyond noise.
        if candidate.cmp(&*worst).is_lt() {
            *worst = candidate;
        }
    }
    Ok(())
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
    pub(crate) offsets: Vec<i64>,
}

impl RowMatches {
    pub(crate) fn rows(&self) -> impl Iterator<Item = &[usize]> {
        self.offsets
            .array_windows::<2>()
            .map(|[start, end]| &self.ids[*start as usize..*end as usize])
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
    offsets: Vec<i64>,
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
    let mut ids = crate::try_vec_with_capacity(candidates.len())?;
    let mut distances = if return_distance {
        crate::try_vec_with_capacity(candidates.len())?
    } else {
        Vec::new()
    };
    for candidate in candidates {
        crate::try_push(&mut ids, candidate.idx as i64)?;
        if return_distance {
            crate::try_push(&mut distances, candidate.distance)?;
        }
    }
    let ids = int64_array(py, ids)?;
    if return_distance {
        let distances = float64_array(py, distances)?;
        return (ids, distances).into_py_any(py);
    }
    ids.into_py_any(py)
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
