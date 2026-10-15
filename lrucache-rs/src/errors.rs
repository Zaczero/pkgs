use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use thiserror::Error;

use crate::store::MAX_CAPACITY;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CacheError {
    #[error("maxsize must be a positive integer")]
    MaxsizeMustBePositive,
    #[error("maxsize must be at most {MAX_CAPACITY}")]
    MaxsizeTooLarge,
}

impl CacheError {
    pub(crate) fn into_pyerr(self) -> PyErr {
        PyValueError::new_err(self.to_string())
    }
}
