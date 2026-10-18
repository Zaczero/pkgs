use thiserror::Error;

use crate::store::MAX_CAPACITY;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CacheError {
    #[error("maxsize must be a positive integer")]
    MaxsizeMustBePositive,
    #[error("maxsize must be at most {MAX_CAPACITY}")]
    MaxsizeTooLarge,
}
