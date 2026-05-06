use pyo3::PyErr;
use pyo3::exceptions::PyValueError;
use thiserror::Error;

use crate::constants::MAX_ZIDS_AT_ONCE;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid count: expected at most {MAX_ZIDS_AT_ONCE}, got {attempted}")]
    TooManyZIDsAtOnce { attempted: usize },
}

impl Error {
    pub(crate) fn into_pyerr(self) -> PyErr {
        PyValueError::new_err(self.to_string())
    }
}
