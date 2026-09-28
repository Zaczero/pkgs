//! Lazy handle iterator over a `SpatialIndex`, kept beside its pymethods.

use pyo3::exceptions::PyRuntimeError;
use pyo3::{Py, PyRef, PyResult, Python, pyclass, pymethods};

use crate::py::index::PySpatialIndex;

/// Lazy ascending-handle iterator over a live index. It scans the sparse handle
/// table on demand instead of collecting and sorting the live entries before
/// the first result. Mutation invalidates iteration, matching mapping iterator
/// semantics and keeping ``__length_hint__`` exact.
#[pyclass(name = "SpatialIndexIterator", module = "gometry", immutable_type)]
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

    fn __sizeof__(&self, py: Python<'_>) -> usize {
        size_of_val(self).saturating_add(self.source.borrow(py).__sizeof__())
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<usize>> {
        let index = self.source.borrow(py);
        if index.mutation_gen != self.generation {
            return Err(PyRuntimeError::new_err(
                "spatial index changed during iteration",
            ));
        }
        let live_rows = index
            .rows
            .live
            .get(self.next_handle..)
            .expect("iterator cursor stays within the stable handle table");
        if let Some((offset, _)) = live_rows.iter().enumerate().find(|(_, live)| **live != 0) {
            let handle = self.next_handle.wrapping_add(offset);
            self.next_handle = handle.wrapping_add(1);
            self.remaining = self.remaining.wrapping_sub(1);
            return Ok(Some(handle));
        }
        self.next_handle = index.rows.len();
        self.remaining = 0;
        Ok(None)
    }
}
