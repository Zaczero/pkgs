use pyo3::PyErr;
use pyo3::exceptions::PyValueError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum PolylineError {
    #[error(
        "Invalid precision {precision}: scale and inverse must be finite and nonzero and decoded i32 coordinates must remain finite"
    )]
    PrecisionOutOfRange { precision: i32 },
    #[error("Invalid coordinate: expected 2 values at index {index}")]
    CoordinateMustContain2Values { index: usize },
    #[error(
        "Invalid coordinate at index {index}: {coordinate} is not finite or its rounded scaled value does not fit i32"
    )]
    CoordinateOutOfRange {
        index: usize,
        coordinate: &'static str,
    },
    #[error("Invalid coordinate at index {index}: {coordinate} delta does not fit i32")]
    CoordinateDeltaOutOfRange {
        index: usize,
        coordinate: &'static str,
    },
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

impl PolylineError {
    pub(crate) fn into_pyerr(self) -> PyErr {
        PyValueError::new_err(self.to_string())
    }
}
