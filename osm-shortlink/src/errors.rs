use pyo3::PyErr;
use pyo3::exceptions::PyValueError;
use thiserror::Error;

use crate::codec::MAX_ZOOM;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum DecodeError {
    #[error("Invalid shortlink: too short")]
    TooShort,
    #[error("Invalid shortlink: too long")]
    TooLong,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum EncodeError {
    #[error("Invalid zoom: must be between 0 and {MAX_ZOOM}, got {zoom}")]
    ZoomOutOfRange { zoom: u8 },
}

impl DecodeError {
    pub(crate) fn into_pyerr(self) -> PyErr {
        PyValueError::new_err(self.to_string())
    }
}

impl EncodeError {
    pub(crate) fn into_pyerr(self) -> PyErr {
        PyValueError::new_err(self.to_string())
    }
}
