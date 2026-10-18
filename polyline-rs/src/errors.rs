use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum PolylineError {
    #[error(
        "precision {precision} must produce a finite, nonzero scale and inverse and finite decoded coordinates"
    )]
    PrecisionOutOfRange { precision: i32 },
    #[error("coordinate at index {index} must contain at least 2 values")]
    CoordinateMustContain2Values { index: usize },
    #[error(
        "{coordinate} at index {index} must be finite and fit in signed 32 bits after scaling and rounding"
    )]
    CoordinateOutOfRange {
        index: usize,
        coordinate: &'static str,
    },
    #[error("{coordinate} delta at index {index} must fit in signed 32 bits")]
    CoordinateDeltaOutOfRange {
        index: usize,
        coordinate: &'static str,
    },
    #[error("polyline byte 0x{byte:02x} at index {index} is outside the encoded range")]
    InvalidPolylineByte { index: usize, byte: u8 },
    #[error("polyline has an unterminated value starting at index {index}")]
    UnterminatedPolylineValue { index: usize },
    #[error("polyline value starting at index {index} exceeds 32 bits")]
    PolylineValueOverflow { index: usize },
    #[error("polyline is missing a coordinate value at index {index}")]
    IncompletePolylineCoordinate { index: usize },
    #[error("polyline accumulated coordinate overflows at index {index}")]
    CoordinateOverflow { index: usize },
}
