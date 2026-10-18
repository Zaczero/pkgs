use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum BlurhashError {
    #[error("width and height must be positive; got {width}x{height}")]
    InvalidImageDimensions { width: usize, height: usize },
    #[error("image dimensions {width}x{height} exceed the supported storage size")]
    ImageDimensionsTooLarge { width: usize, height: usize },
    #[error("RGB buffer must contain {expected} bytes; got {got}")]
    InvalidRGBBufferLength { expected: usize, got: usize },
    #[error("{axis} component count must be between 1 and 9; got {got}")]
    InvalidComponentCount { axis: &'static str, got: u8 },
    #[error("blurhash contains an invalid value at index {index}")]
    BlurhashMalformed { index: usize },
    #[error("blurhash must contain {expected} bytes; got {got}")]
    BlurhashLengthMismatch { expected: usize, got: usize },
    #[error("punch must be finite and nonnegative")]
    InvalidPunch,
}
