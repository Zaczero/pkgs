use pyo3::PyErr;
use pyo3::exceptions::PyValueError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid RGB buffer length: expected {expected}, got {got}")]
    InvalidRGBBufferLength { expected: usize, got: usize },
    #[error("Invalid {axis} component count: expected 1..=9, got {got}")]
    InvalidComponentCount { axis: &'static str, got: u8 },
    #[error("Invalid blurhash: malformed at index {index}")]
    BlurhashMalformed { index: usize },
    #[error("Invalid blurhash length: expected {expected}, got {got}")]
    BlurhashLengthMismatch { expected: usize, got: usize },
}

impl Error {
    pub(crate) fn into_pyerr(self) -> PyErr {
        PyValueError::new_err(self.to_string())
    }
}
