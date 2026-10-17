use pyo3::PyErr;
use pyo3::exceptions::{PyOverflowError, PyValueError};
use thiserror::Error;

use crate::constants::MAX_ZIDS_AT_ONCE;

#[derive(Debug, Error)]
pub(crate) enum ZidError {
    #[error("Invalid count: expected at most {MAX_ZIDS_AT_ONCE}, got {attempted}")]
    TooManyZIDsAtOnce { attempted: usize },
    #[error("ZID's 47-bit millisecond timestamp is exhausted")]
    TimestampExhausted,
}

impl ZidError {
    pub(crate) fn into_pyerr(self) -> PyErr {
        match self {
            Self::TooManyZIDsAtOnce { .. } => PyValueError::new_err(self.to_string()),
            Self::TimestampExhausted => PyOverflowError::new_err(self.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use pyo3::Python;
    use pyo3::exceptions::{PyOverflowError, PyValueError};

    use super::ZidError;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn public_exception_classes_distinguish_horizon_and_count() {
        Python::initialize();
        Python::attach(|py| {
            assert!(
                ZidError::TimestampExhausted
                    .into_pyerr()
                    .is_instance_of::<PyOverflowError>(py)
            );
            assert!(
                ZidError::TooManyZIDsAtOnce {
                    attempted: 0x0001_0001,
                }
                .into_pyerr()
                .is_instance_of::<PyValueError>(py)
            );
        });
    }
}
