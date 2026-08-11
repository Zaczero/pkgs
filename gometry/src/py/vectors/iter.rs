//! Lazy row iterator for `Groups`, kept beside the cohesive vector module.

use crate::py::vectors::{Groups, RowIterState, pyclass};

/// Lazy row iterator for [`Groups`].
#[pyclass(name = "GroupsIterator", module = "gometry", frozen, immutable_type)]
pub(crate) struct GroupsIter {
    source: Groups,
    state: RowIterState,
}

impl GroupsIter {
    pub(super) const fn new(source: Groups, reverse: bool) -> Self {
        Self {
            source,
            state: RowIterState::new(reverse),
        }
    }
}

row_iter_pymethods! {
    impl GroupsIter {
        source: Groups,
    }
}
