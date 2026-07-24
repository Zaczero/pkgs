//! Lazy handle iterator over a `SpatialIndex`, kept beside its pymethods.

use super::*;

/// Lazy ascending-handle iterator over a live index. It scans the sparse handle
/// table on demand instead of collecting and sorting the live entries before
/// the first result. Mutation invalidates iteration, matching mapping iterator
/// semantics and keeping ``__length_hint__`` exact.
#[pyclass(name = "SpatialIndexIterator", module = "gometry")]
pub(crate) struct PySpatialIndexIter {
    pub(super) source: Py<PySpatialIndex>,
    pub(super) next_handle: usize,
    pub(super) remaining: usize,
    pub(super) generation: u64,
}

#[pymethods]
impl PySpatialIndexIter {
    const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    const fn __length_hint__(&self) -> usize {
        self.remaining
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<usize>> {
        let index = self.source.borrow(py);
        if index.mutation_gen != self.generation {
            return Err(PyRuntimeError::new_err(
                "spatial index changed during iteration",
            ));
        }
        while self.next_handle < index.rows.len() {
            let handle = self.next_handle;
            self.next_handle += 1;
            if index.is_live_handle(handle) {
                self.remaining -= 1;
                return Ok(Some(handle));
            }
        }
        self.remaining = 0;
        Ok(None)
    }
}
