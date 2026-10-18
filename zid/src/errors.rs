use thiserror::Error;

use crate::constants::MAX_ZIDS_AT_ONCE;

#[derive(Debug, Error)]
pub(crate) enum ZidError {
    #[error("n must be at most {MAX_ZIDS_AT_ONCE}, got {attempted}")]
    TooManyZIDsAtOnce { attempted: usize },
    #[error("ZID's 47-bit millisecond timestamp is exhausted")]
    TimestampExhausted,
}
