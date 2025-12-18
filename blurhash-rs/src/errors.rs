use std::borrow::Cow;
use std::fmt;

#[derive(Debug)]
pub(crate) enum Error {
    InvalidRGBBufferLength { expected: usize, got: usize },
    BlurhashMalformed { index: usize },
    BlurhashLengthMismatch { expected: usize, got: usize },
}

impl Error {
    pub(crate) fn message(&self) -> Cow<'static, str> {
        match self {
            Self::InvalidRGBBufferLength { expected, got } => Cow::Owned(format!(
                "Invalid RGB buffer length (expected {expected}, got {got})"
            )),
            Self::BlurhashMalformed { index } => {
                Cow::Owned(format!("Invalid blurhash: malformed at index {index}"))
            }
            Self::BlurhashLengthMismatch { expected, got } => Cow::Owned(format!(
                "Invalid blurhash: length mismatch (expected {expected}, got {got})"
            )),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_ref())
    }
}
