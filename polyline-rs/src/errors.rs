use std::borrow::Cow;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    CoordinateMustContain2Values { index: usize },
    InvalidPolylineByte { index: usize, byte: u8 },
    UnterminatedPolylineValue { index: usize },
    PolylineValueOverflow { index: usize },
    IncompletePolylineCoordinate { index: usize },
    CoordinateOverflow { index: usize },
}

impl Error {
    pub(crate) fn message(&self) -> Cow<'static, str> {
        match self {
            Self::CoordinateMustContain2Values { index } => Cow::Owned(format!(
                "Invalid coordinate: expected 2 values at index {index}"
            )),
            Self::InvalidPolylineByte { index, byte } => Cow::Owned(format!(
                "Invalid polyline: byte 0x{byte:02x} at index {index} is outside encoded range"
            )),
            Self::UnterminatedPolylineValue { index } => Cow::Owned(format!(
                "Invalid polyline: unterminated value starting at index {index}"
            )),
            Self::PolylineValueOverflow { index } => Cow::Owned(format!(
                "Invalid polyline: value starting at index {index} exceeds 32 bits"
            )),
            Self::IncompletePolylineCoordinate { index } => Cow::Owned(format!(
                "Invalid polyline: missing coordinate value at index {index}"
            )),
            Self::CoordinateOverflow { index } => Cow::Owned(format!(
                "Invalid polyline: accumulated coordinate overflows at index {index}"
            )),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_ref())
    }
}
