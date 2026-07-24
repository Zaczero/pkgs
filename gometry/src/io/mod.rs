#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Geometry serialization codecs: WKT, WKB, and GeoJSON.

use smol_str::SmolStr;
use thiserror::Error;

use crate::crs;
use crate::error::{Error as CrateError, Result};
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates, EmptyKind, GeometryErrorKind, LineSeq, Point, Polygon,
    Ring, Shape,
};
use crate::py::errors::ParseFormat;

const WKB_MULTIPOINT: u32 = 4;
const WKB_MULTILINESTRING: u32 = 5;
const WKB_MULTIPOLYGON: u32 = 6;
const WKB_GEOMETRYCOLLECTION: u32 = 7;
const WKB_Z_OFFSET: u32 = 1000;
const WKB_M_OFFSET: u32 = 2000;
const WKB_ZM_OFFSET: u32 = 3000;
const EWKB_Z_FLAG: u32 = 0x8000_0000;
const EWKB_M_FLAG: u32 = 0x4000_0000;
const EWKB_SRID_FLAG: u32 = 0x2000_0000;

/// Maximum nesting accepted by recursive geometry decoders. This matches
/// serde_json's default recursion boundary while keeping WKT, WKB, and Python
/// mapping ingestion from exhausting the native stack on adversarial input.
pub(crate) const MAX_PARSE_DEPTH: usize = 128;

/// A parse failure for serialized geometry input. Surfaces as
/// `gometry.ParseError`. Constructors return the crate-wide
/// [`Error`](CrateError) so call sites build the boxed error in one step.
#[derive(Debug, Error)]
#[error("invalid {}: {detail}", format.display())]
pub struct IoError {
    format: ParseFormat,
    detail: Box<str>,
}

impl IoError {
    /// The format label for the `ParseError.format` attribute.
    pub const fn format_label(&self) -> &'static str {
        self.format.label()
    }

    pub(crate) fn parse(format: ParseFormat, detail: impl Into<Box<str>>) -> CrateError {
        Self {
            format,
            detail: detail.into(),
        }
        .into()
    }

    pub fn wkt(detail: impl Into<Box<str>>) -> CrateError {
        Self::parse(ParseFormat::Wkt, detail)
    }

    pub fn wkb(detail: impl Into<Box<str>>) -> CrateError {
        Self::parse(ParseFormat::Wkb, detail)
    }

    pub fn geojson(detail: impl Into<Box<str>>) -> CrateError {
        Self::parse(ParseFormat::GeoJson, detail)
    }
}

/// Whether an SRID should be written for an extended text/binary format.
/// EWKT/EWKB carry integer SRIDs, which only exist for EPSG-authority CRSs.
fn extended_srid_code(
    crs: Option<&str>,
    include_srid: bool,
    format_label: &'static str,
) -> Result<Option<u32>> {
    if !include_srid {
        return Ok(None);
    }
    crs.map_or(Ok(None), |crs| {
        crs::parse_epsg(crs).map(Some).ok_or_else(|| {
            crs::CrsError::message(format!(
                "{format_label} SRID requires an EPSG-authority CRS, but {crs:?} has no EPSG code; \
                 set include_srid=False or reproject to an EPSG CRS"
            ))
        })
    })
}

/// Map an EWKT/EWKB SRID integer to a CRS.
///
/// PostGIS reserves SRID 0 as unknown/unspecified — it becomes CRS-free
/// (``None``), never a false ``EPSG:0``. Every nonzero code is resolved
/// through the canonical PROJ-backed CRS parser so invalid codes fail at
/// parse time rather than later on first introspection.
pub(crate) fn crs_from_srid(code: u32) -> Result<Option<smol_str::SmolStr>> {
    if code == 0 {
        return Ok(None);
    }
    Ok(Some(crs::canonicalize(&format!("EPSG:{code}"))?))
}

#[derive(Clone, Copy)]
struct WkbAxes {
    z: bool,
    m: bool,
}

/// The seven supported geometry kinds, shared by the WKT and WKB grammars:
/// one enum drives keyword parsing, type-code encoding, and header writing.
#[derive(Clone, Copy)]
enum IoGeometryKind {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    GeometryCollection,
}

pub(crate) struct WkbGeometry {
    pub shape: Shape,
    pub crs: Option<SmolStr>,
}

struct WktHeader<'a> {
    geometry_type: IoGeometryKind,
    axes: CoordinateAxes,
    /// True when a Z / M / ZM dimensional tag was present on the type keyword.
    /// Untagged members of a tagged GeometryCollection inherit the outer axes;
    /// an explicit conflicting tag is rejected.
    axes_explicit: bool,
    body: Option<&'a str>,
}

pub(crate) enum GeoJsonInput {
    Geometry(Shape),
    /// One entry per feature; ``None`` is RFC 7946's null geometry (a valid
    /// feature with no geometry — a missing row downstream).
    FeatureCollection(Vec<Option<Shape>>),
}

/// Parsed `GeoJSON` text geometry / FeatureCollection shapes.
///
/// Nested legacy ``crs`` members are reconciled by walking the raw JSON value
/// (see [`collect_geojson_legacy_crs`]) — the typed text decoder does not
/// retain them.
pub(crate) struct GeoJsonTextParse {
    pub input: GeoJsonInput,
}

fn parse_content(format: ParseFormat, error: CrateError) -> CrateError {
    match error.kind() {
        crate::error::ErrorKind::Geometry(_) => IoError::parse(format, error.to_string()),
        _ => error,
    }
}

mod geojson;
mod wkb;
mod wkt;

pub(crate) use geojson::*;
pub(crate) use wkb::*;
pub(crate) use wkt::*;
