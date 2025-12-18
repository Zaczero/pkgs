use std::borrow::Cow;
use std::fmt;

#[derive(Debug)]
pub(crate) enum Error {
    InvalidSecretChar { index: usize },
    InvalidSecretType,
    DigitsOutOfRange { digits: u8 },
    StepSecondsMustBeNonZero,
    InvalidAlgorithm,
    TimeAndTimeWindowBothSet,
}

impl Error {
    pub(crate) fn message(&self) -> Cow<'static, str> {
        match self {
            Self::InvalidSecretChar { index } => {
                Cow::Owned(format!("Invalid secret: invalid base32 at index {index}"))
            }
            Self::InvalidSecretType => {
                Cow::Borrowed("Invalid secret: must be bytes or base32 string")
            }
            Self::DigitsOutOfRange { digits } => {
                Cow::Owned(format!("digits must be in 1..=9 (got {digits})"))
            }
            Self::StepSecondsMustBeNonZero => Cow::Borrowed("step_seconds must be non-zero"),
            Self::InvalidAlgorithm => {
                Cow::Borrowed("algorithm must be one of: sha1, sha256, sha512")
            }
            Self::TimeAndTimeWindowBothSet => {
                Cow::Borrowed("time and time_window cannot both be set")
            }
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message().as_ref())
    }
}
