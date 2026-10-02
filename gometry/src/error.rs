//! The crate-wide error type with allocation-free resource failure reporting.
//!
//! Core modules return [`Result`]; the Python boundary converts exactly once
//! (`impl From<Error> for PyErr` in `crate::py::errors`, which also owns the
//! exception classes). Domain vocabulary stays in the owning module —
//! `GeometryErrorKind` in `geometry`, `CrsError` in `crs`, `IoError` in `io`,
//! `FrameError` in `metadata` — and composes
//! here. Domain payloads are boxed; entropy failure and size overflow use
//! static kinds.

use thiserror::Error as ThisError;

use crate::FrameError;
use crate::crs::CrsError;
use crate::geometry::GeometryErrorKind;
use crate::io::IoError;

/// Entropy acquisition and representable resource-size failures.
#[derive(Debug, ThisError)]
pub(crate) enum ResourceError {
    #[error("operating system entropy acquisition failed")]
    Entropy,
    #[error("resource size arithmetic overflowed")]
    SizeOverflow,
    #[error("{0}")]
    SizeOverflowContext(&'static str),
}

/// Crate-wide result alias. The error parameter defaults to [`Error`] so core
/// signatures read `Result<T>`, while explicit forms (`Result<T, PyErr>`)
/// still work under the same import.
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// The crate error, with boxed domain details and allocation-free resource errors.
// The `std::io::Error` naming precedent: the crate error IS named `Error`.
#[expect(
    clippy::error_impl_error,
    reason = "the crate's public error type deliberately follows the standard library Error naming convention"
)]
#[derive(Debug, ThisError)]
#[error(transparent)]
#[repr(transparent)]
pub struct Error(ErrorStorage);

#[derive(Debug, ThisError)]
enum ErrorStorage {
    #[error(transparent)]
    Owned(Box<ErrorKind>),
    #[error(transparent)]
    Static(&'static ErrorKind),
}

static ENTROPY_ERROR: ErrorKind = ErrorKind::Resource(ResourceError::Entropy);
static SIZE_OVERFLOW_ERROR: ErrorKind = ErrorKind::Resource(ResourceError::SizeOverflow);
static GEODESIC_EXPONENT_OVERFLOW_ERROR: ErrorKind =
    ErrorKind::Crs(CrsError::GeodesicExponentOverflow);

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
    #[error(transparent)]
    Resource(#[from] ResourceError),
}

impl Error {
    pub(crate) fn kind(&self) -> &ErrorKind {
        match &self.0 {
            ErrorStorage::Owned(kind) => kind,
            ErrorStorage::Static(kind) => kind,
        }
    }

    pub(crate) fn with_parse_position(mut self, position: usize) -> Self {
        if let ErrorStorage::Owned(kind) = &mut self.0
            && let ErrorKind::Io(error) = kind.as_mut()
        {
            error.set_position_if_missing(position);
        }
        self
    }

    pub(crate) fn with_parse_position_offset(mut self, offset: usize) -> Self {
        if let ErrorStorage::Owned(kind) = &mut self.0
            && let ErrorKind::Io(error) = kind.as_mut()
        {
            error.offset_position(offset);
        }
        self
    }

    pub(crate) fn with_parse_path(mut self, path: &str) -> Self {
        if let ErrorStorage::Owned(kind) = &mut self.0
            && let ErrorKind::Io(error) = kind.as_mut()
        {
            error.set_path_if_missing(path);
        }
        self
    }

    pub(crate) fn annotate_crs_display(
        &mut self,
        definition: &str,
        display_name: Option<&'static str>,
    ) {
        let Some(display_name) = display_name else {
            return;
        };
        if let ErrorStorage::Owned(kind) = &mut self.0
            && let ErrorKind::Crs(error) = kind.as_mut()
        {
            error.annotate_display(definition, display_name);
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self(match kind {
            ErrorKind::Resource(ResourceError::Entropy) => ErrorStorage::Static(&ENTROPY_ERROR),
            ErrorKind::Resource(ResourceError::SizeOverflow) => {
                ErrorStorage::Static(&SIZE_OVERFLOW_ERROR)
            },
            ErrorKind::Crs(CrsError::GeodesicExponentOverflow) => {
                ErrorStorage::Static(&GEODESIC_EXPONENT_OVERFLOW_ERROR)
            },
            kind @ (ErrorKind::Geometry(_)
            | ErrorKind::Crs(_)
            | ErrorKind::Io(_)
            | ErrorKind::Frame(_)
            | ErrorKind::Resource(ResourceError::SizeOverflowContext(_))) => {
                ErrorStorage::Owned(Box::new(kind))
            },
        })
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

impl From<ResourceError> for Error {
    fn from(error: ResourceError) -> Self {
        ErrorKind::Resource(error).into()
    }
}

/// A `ParseError` format tag identifying the malformed serialized, grid-cell,
/// or point-code input. PyErr decoration lives in `crate::py::errors`.
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
    pub(crate) const ALL: [Self; 14] = [
        Self::Wkt,
        Self::Wkb,
        Self::GeoJson,
        Self::GeoArrow,
        Self::GeoParquet,
        Self::H3,
        Self::S2,
        Self::Geohash,
        Self::Tile,
        Self::Quadkey,
        Self::Polyline,
        Self::PlusCode,
        Self::OsmShortlink,
        Self::Pickle,
    ];

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
