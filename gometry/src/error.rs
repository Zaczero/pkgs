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
#[expect(
    clippy::error_impl_error,
    reason = "the crate's public error type deliberately follows the standard library Error naming convention"
)]
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

    pub(crate) fn with_parse_position(mut self, position: usize) -> Self {
        if let ErrorKind::Io(error) = self.0.as_mut() {
            error.set_position_if_missing(position);
        }
        self
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

/// A `ParseError` format tag (which codec rejected the input) — so the
/// point/grid codecs match the WKT/WKB/GeoJSON contract. PyErr decoration lives
/// in `crate::py::errors`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParseFormat {
    Wkt,
    Wkb,
    GeoJson,
    GeoArrow,
    GeoParquet,
    H3,
    S2,
    Geohash,
    Tile,
    Quadkey,
    Polyline,
    PlusCode,
    OsmShortlink,
    Pickle,
}

impl ParseFormat {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Wkt => "wkt",
            Self::Wkb => "wkb",
            Self::GeoJson => "geojson",
            Self::GeoArrow => "geoarrow",
            Self::GeoParquet => "geoparquet",
            Self::H3 => "h3",
            Self::S2 => "s2",
            Self::Geohash => "geohash",
            Self::Tile => "tile",
            Self::Quadkey => "quadkey",
            Self::Polyline => "polyline",
            Self::PlusCode => "pluscode",
            Self::OsmShortlink => "osm_shortlink",
            Self::Pickle => "pickle",
        }
    }

    pub(crate) const fn display(self) -> &'static str {
        match self {
            Self::Wkt => "WKT",
            Self::Wkb => "WKB",
            Self::GeoJson => "GeoJSON",
            Self::GeoArrow => "GeoArrow",
            Self::GeoParquet => "GeoParquet",
            Self::H3 => "H3",
            Self::S2 => "S2",
            Self::Geohash => "geohash",
            Self::Tile => "tile",
            Self::Quadkey => "quadkey",
            Self::Polyline => "polyline",
            Self::PlusCode => "plus code",
            Self::OsmShortlink => "osm shortlink",
            Self::Pickle => "pickle",
        }
    }
}
