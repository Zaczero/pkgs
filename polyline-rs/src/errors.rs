use pyo3::PyErr;
use pyo3::exceptions::PyValueError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid coordinate: expected 2 values at index {index}")]
    CoordinateMustContain2Values { index: usize },
    #[error("Invalid polyline: byte 0x{byte:02x} at index {index} is outside encoded range")]
    InvalidPolylineByte { index: usize, byte: u8 },
    #[error("Invalid polyline: unterminated value starting at index {index}")]
    UnterminatedPolylineValue { index: usize },
    #[error("Invalid polyline: value starting at index {index} exceeds 32 bits")]
    PolylineValueOverflow { index: usize },
    #[error("Invalid polyline: missing coordinate value at index {index}")]
    IncompletePolylineCoordinate { index: usize },
    #[error("Invalid polyline: accumulated coordinate overflows at index {index}")]
    CoordinateOverflow { index: usize },
}

impl Error {
    pub(crate) fn into_pyerr(self) -> PyErr {
        PyValueError::new_err(self.to_string())
    }
}
