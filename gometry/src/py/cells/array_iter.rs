//! Iterator surface for `CellArray`.

use pyo3::prelude::*;

use crate::py::cells::PyCellArray;
use crate::py::row::RowIterState;

/// Lazy iterator over a `CellArray`'s typed cells (both directions).
#[pyclass(name = "CellArrayIterator", module = "gometry", frozen, immutable_type)]
pub(crate) struct PyCellArrayIter {
    source: PyCellArray,
    state: RowIterState,
}

impl PyCellArrayIter {
    pub(super) const fn new(source: PyCellArray, reverse: bool) -> Self {
        Self {
            source,
            state: RowIterState::new(reverse),
        }
    }
}

row_iter_pymethods! {
    impl PyCellArrayIter {
        source: PyCellArray,
    }
}
