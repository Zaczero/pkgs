use pyo3::IntoPyObjectExt as _;

use crate::NonNegative;
use crate::py::index::{AABB, BinaryHeap, Ordering, Py, PyAny, PyResult, Python, float64_array};
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
pub(crate) fn parse_max_distance(
    max_distance: Option<f64>,
) -> crate::error::Result<Option<NonNegative>> {
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
    mut keep: impl FnMut(usize) -> crate::error::BatchResult<bool>,
) -> crate::error::BatchResult<()> {
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

pub(crate) fn index_ids(ids: Vec<usize>) -> Vec<i64> {
    ids.into_iter().map(|handle| handle as i64).collect()
}

pub(crate) fn nearest_columns(
    candidates: Vec<NearestCandidate>,
    return_distance: bool,
) -> (Vec<i64>, Option<Vec<f64>>) {
    let mut ids = Vec::with_capacity(candidates.len());
    let mut distances = return_distance.then(|| Vec::with_capacity(candidates.len()));
    for candidate in candidates {
        ids.push(candidate.idx as i64);
        if let Some(distances) = &mut distances {
            distances.push(candidate.distance);
        }
    }
    (ids, distances)
}

pub(crate) fn format_nearest(
    py: Python<'_>,
    ids: Py<PyAny>,
    distances: Option<Vec<f64>>,
) -> PyResult<Py<PyAny>> {
    if let Some(distances) = distances {
        let distances = float64_array(py, distances)?;
        return (ids, distances).into_py_any(py);
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_row_stops_after_the_first_refinement_error() {
        let mut ids = vec![0, 1, 2, 3];
        let mut visited = Vec::new();
        let error = retain_row(&mut ids, 1, |id| {
            visited.push(id);
            if id == 2 {
                Err(crate::geometry::GeometryErrorKind::message("first refinement error").into())
            } else {
                Ok(id % 2 == 0)
            }
        })
        .expect_err("the second candidate fails");
        let crate::error::BatchError::Batch(error) = error else {
            panic!("scalar refinement error has no row annotation");
        };
        assert_eq!(error.to_string(), "first refinement error");
        assert_eq!(visited, [1, 2]);
    }
}
