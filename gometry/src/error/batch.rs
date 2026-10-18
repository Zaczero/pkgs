use super::Error;

#[derive(Debug)]
pub(crate) enum BatchError {
    Batch(Error),
    Row { row: usize, error: Error },
}

pub(crate) type BatchResult<T> = Result<T, BatchError>;

impl From<Error> for BatchError {
    fn from(error: Error) -> Self {
        Self::Batch(error)
    }
}

impl From<(usize, Error)> for BatchError {
    fn from((row, error): (usize, Error)) -> Self {
        Self::Row { row, error }
    }
}
