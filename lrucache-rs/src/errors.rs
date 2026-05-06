use pyo3::PyErr;
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use thiserror::Error;

use crate::store::MAX_CAPACITY;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CacheError {
    #[error("maxsize must be a positive integer")]
    MaxsizeMustBePositive,
    #[error("maxsize must be at most {MAX_CAPACITY}")]
    MaxsizeTooLarge,
    #[error("popitem(): cache is empty")]
    PopitemEmpty,
}

impl CacheError {
    pub fn into_pyerr(self) -> PyErr {
        match self {
            Self::PopitemEmpty => PyKeyError::new_err(self.to_string()),
            Self::MaxsizeMustBePositive | Self::MaxsizeTooLarge => {
                PyValueError::new_err(self.to_string())
            },
        }
    }
}

pub fn missing_key(key: &Bound<'_, PyAny>) -> PyErr {
    let repr = key
        .repr()
        .map_or_else(|_| String::from("key not found"), |s| s.to_string());
    PyKeyError::new_err(repr)
}
