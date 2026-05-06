use pyo3::PyErr;
use pyo3::exceptions::PyValueError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid secret: invalid Base32 character at index {index}")]
    InvalidSecretChar { index: usize },
    #[error("Invalid secret: must be bytes or Base32 string")]
    InvalidSecretType,
    #[error("Invalid digits: expected 1..=9, got {digits}")]
    DigitsOutOfRange { digits: u8 },
    #[error("Invalid step_seconds: must be non-zero")]
    StepSecondsMustBeNonZero,
    #[error("Invalid algorithm: expected sha1, sha256, or sha512")]
    InvalidAlgorithm,
    #[error("Invalid arguments: time and time_window cannot both be set")]
    TimeAndTimeWindowBothSet,
}

impl Error {
    pub(crate) fn into_pyerr(self) -> PyErr {
        PyValueError::new_err(self.to_string())
    }
}

pub(crate) fn validate_digits(digits: u8) -> Result<(), Error> {
    if (1..=9).contains(&digits) {
        Ok(())
    } else {
        Err(Error::DigitsOutOfRange { digits })
    }
}
