use pyo3::IntoPyObjectExt as _;

use crate::NonNegative;
use crate::geometry::storage::ValidityMask;
use crate::py::index::{
    AABB, BinaryHeap, Ordering, Py, PyAny, PyResult, Python, float64_array, int64_array,
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
    } else if let Some(mut worst) = nearest.peek_mut() {
        // Replace-in-place via peek_mut (one sift) instead of pop+push (two).
        // Kept only when the public nearest row improves beyond noise.
        if candidate.cmp(&*worst).is_lt() {
            *worst = candidate;
        }
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
    pub(crate) offsets: Vec<i64>,
}

impl RowMatches {
    pub(crate) fn rows(&self) -> impl Iterator<Item = &[usize]> {
        csr_rows(&self.ids, &self.offsets)
    }
}

pub(crate) fn csr_rows<'a>(
    ids: &'a [usize],
    offsets: &'a [i64],
) -> impl Iterator<Item = &'a [usize]> + 'a {
    offsets.array_windows::<2>().map(move |[start, end]| {
        let start = usize::try_from(*start).expect("CSR offsets are nonnegative");
        let end = usize::try_from(*end).expect("CSR offsets are nonnegative");
        ids.get(start..end)
            .expect("CSR builder offsets bound the match values")
    })
}

/// Compact `ids[row_start..]` in place to the entries `keep` accepts —
/// the per-row exact-refine pass of the CSR builders.
pub(crate) fn retain_row(
    ids: &mut Vec<usize>,
    row_start: usize,
    mut keep: impl FnMut(usize) -> PyResult<bool>,
) -> PyResult<()> {
    debug_assert!(row_start <= ids.len());
    let failed = std::cell::Cell::new(false);
    let mut failure = None;
    let mut removed = ids.extract_if(row_start.., |idx| {
        if failed.get() {
            return false;
        }
        match keep(*idx) {
            Ok(keep) => !keep,
            Err(error) => {
                failed.set(true);
                failure = Some(error);
                // This is temporary candidate storage and the caller discards
                // it on error. Removing this row makes extract_if stop before
                // evaluating any later candidate.
                true
            },
        }
    });
    while !failed.get() && removed.next().is_some() {}
    drop(removed);
    if let Some(error) = failure {
        return Err(error);
    }
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
    missing: Option<ValidityMask>,
) -> PyResult<Py<PyAny>> {
    let matches = Groups::from_int64_csr(ids, offsets)?.with_row_missing(missing);
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
    let mut distances = return_distance.then(|| Vec::with_capacity(candidates.len()));
    for candidate in candidates {
        ids.push(candidate.idx as i64);
        if let Some(distances) = &mut distances {
            distances.push(candidate.distance);
        }
    }
    let ids = int64_array(py, ids)?;
    if let Some(distances) = distances {
        let distances = float64_array(py, distances)?;
        return (ids, distances).into_py_any(py);
    }
    ids.into_py_any(py)
}

#[cfg(test)]
mod tests {
    use pyo3::types::{PyAnyMethods as _, PyStringMethods as _};

    use super::*;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn retain_row_stops_after_the_first_refinement_error() {
        crate::test_support::initialize_python();
        let (message, visited) = pyo3::Python::attach(|py| {
            let mut ids = vec![0, 1, 2, 3];
            let mut visited = Vec::new();
            let error = retain_row(&mut ids, 1, |id| {
                visited.push(id);
                if id == 2 {
                    Err(pyo3::exceptions::PyValueError::new_err(
                        "first refinement error",
                    ))
                } else {
                    Ok(id % 2 == 0)
                }
            })
            .expect_err("the second candidate fails");
            let message = error
                .value(py)
                .str()
                .expect("PyValueError has a string representation")
                .to_str()
                .expect("error message is valid UTF-8")
                .to_owned();
            (message, visited)
        });
        assert_eq!(message, "first refinement error");
        assert_eq!(visited, [1, 2]);
    }
}
