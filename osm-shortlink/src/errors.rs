use thiserror::Error;

use crate::codec::MAX_ZOOM;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum DecodeError {
    #[error("shortlink is too short")]
    TooShort,
    #[error("shortlink is too long")]
    TooLong,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum EncodeError {
    #[error("zoom must be between 0 and {MAX_ZOOM}, got {zoom}")]
    ZoomOutOfRange { zoom: u8 },
    #[error("{coordinate} must be finite")]
    NonFiniteCoordinate { coordinate: &'static str },
}
