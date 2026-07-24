use super::*;
use crate::py::row::RowIterState;

/// Lazy element iterator for `GeometryArray` (both directions): one typed
/// scalar per `next`, so iterating a packed point array never materializes
/// the whole wrapper list up front.
#[pyclass(name = "GeometryArrayIterator", module = "gometry", frozen)]
pub struct PyGeometryArrayIter {
    pub(crate) source: PyGeometryArray,
    pub(crate) state: RowIterState,
}

impl PyGeometryArrayIter {
    pub(crate) const fn new(source: PyGeometryArray, reverse: bool) -> Self {
        Self {
            source,
            state: RowIterState::new(reverse),
        }
    }
}

row_iter_pymethods! {
    impl PyGeometryArrayIter {
        source: PyGeometryArray,
    }
}
