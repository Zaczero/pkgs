//! The crate-wide error type: one pointer-sized handle over the domain error
//! kinds.
//!
//! Core modules return [`Result`]; the Python boundary converts exactly once
//! (`impl From<Error> for PyErr` in `crate::py::errors`, which also owns the
//! exception classes). Domain vocabulary stays in the owning module —
//! `GeometryErrorKind` in `geometry`, `CrsError` in `crs`, `IoError` in `io`,
//! `FrameError` in `metadata` — and composes
//! here. Because the whole kind sits behind one `Box`, variants carry their
//! fields directly (no per-variant payload boxing), `Result<T, Error>` stays
//! register-friendly on hot per-row paths, and the allocation happens only on
//! the error path.

use thiserror::Error as ThisError;

use crate::FrameError;
use crate::crs::CrsError;
use crate::geometry::GeometryErrorKind;
use crate::io::IoError;

/// Crate-wide result alias. The error parameter defaults to [`Error`] so core
/// signatures read `Result<T>`, while explicit forms (`Result<T, PyErr>`)
/// still work under the same import.
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// The crate error: a boxed [`ErrorKind`], so the type is pointer-sized and
/// large messages are heap-allocated only when an error actually occurs.
// The `std::io::Error` naming precedent: the crate error IS named `Error`.
#[expect(clippy::error_impl_error)]
#[derive(Debug, ThisError)]
#[error(transparent)]
#[repr(transparent)]
pub struct Error(Box<ErrorKind>);

const _: () = assert!(size_of::<Error>() == size_of::<Box<ErrorKind>>());

/// The domain error kinds. Adding a kind forces a deliberate Python exception
/// classification in `py::errors` (the conversion match has no wildcard arm).
#[derive(Debug, ThisError)]
pub(crate) enum ErrorKind {
    #[error(transparent)]
    Geometry(#[from] GeometryErrorKind),
    #[error(transparent)]
    Crs(#[from] CrsError),
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Frame(#[from] FrameError),
}

impl Error {
    pub(crate) fn kind(&self) -> &ErrorKind {
        &self.0
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self(Box::new(kind))
    }
}

impl From<GeometryErrorKind> for Error {
    fn from(error: GeometryErrorKind) -> Self {
        ErrorKind::Geometry(error).into()
    }
}

impl From<CrsError> for Error {
    fn from(error: CrsError) -> Self {
        ErrorKind::Crs(error).into()
    }
}

impl From<IoError> for Error {
    fn from(error: IoError) -> Self {
        ErrorKind::Io(error).into()
    }
}

impl From<FrameError> for Error {
    fn from(error: FrameError) -> Self {
        ErrorKind::Frame(error).into()
    }
}

/// One pointer, period: the boxed kind is what keeps `Result<T, Error>`
/// register-returnable on hot per-row paths. If this trips, something put a
/// field on `Error` itself — move it into a kind variant instead.
const _: () = assert!(std::mem::size_of::<Error>() == std::mem::size_of::<usize>());
