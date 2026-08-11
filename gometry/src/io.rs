//! Geometry serialization codecs: WKT, WKB, and GeoJSON.

use thiserror::Error;

use crate::crs;
use crate::error::{Error as CrateError, ParseFormat, Result};
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates, EmptyKind, GeometryErrorKind, LineSeq, Point, Polygon,
    Ring, Shape,
};

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
    position: Option<usize>,
}

impl IoError {
    /// The format label for the `ParseError.format` attribute.
    pub const fn format_label(&self) -> &'static str {
        self.format.label()
    }

    pub const fn position(&self) -> Option<usize> {
        self.position
    }

    pub(crate) const fn set_position_if_missing(&mut self, position: usize) {
        if self.position.is_none() {
            self.position = Some(position);
        }
    }

    pub(crate) fn parse(format: ParseFormat, detail: impl Into<Box<str>>) -> CrateError {
        Self {
            format,
            detail: detail.into(),
            position: None,
        }
        .into()
    }

    pub(crate) fn parse_at(
        format: ParseFormat,
        position: usize,
        detail: impl Into<Box<str>>,
    ) -> CrateError {
        Self {
            format,
            detail: detail.into(),
            position: Some(position),
        }
        .into()
    }

    pub(crate) fn wkb_at(position: usize, detail: impl Into<Box<str>>) -> CrateError {
        Self::parse_at(ParseFormat::Wkb, position, detail)
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any detail conversion; a named generic is not part of its parse-error API"
    )]
    pub fn wkt(detail: impl Into<Box<str>>) -> CrateError {
        Self::parse(ParseFormat::Wkt, detail)
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any detail conversion; a named generic is not part of its parse-error API"
    )]
    pub fn wkb(detail: impl Into<Box<str>>) -> CrateError {
        Self::parse(ParseFormat::Wkb, detail)
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the factory accepts any detail conversion; a named generic is not part of its parse-error API"
    )]
    pub fn geojson(detail: impl Into<Box<str>>) -> CrateError {
        Self::parse(ParseFormat::GeoJson, detail)
    }
}

/// Exact PostGIS wire-alias table for non-EPSG CRS that still have a
/// conventional integer SRID on the EWKB/EWKT wire. Deliberately **not**
/// general PROJ equivalence — only these two canonical OGC lon/lat spellings.
///
/// Round-trip identity loss is intentional: `from_wkb(crs84.to_wkb(include_srid=True)).crs`
/// is `EPSG:4326`. Documented as a PostGIS serialization alias, not universal
/// CRS identity (GDAL is more conservative).
const fn wire_srid_alias(crs: &str) -> Option<u32> {
    match crs.as_bytes() {
        b"OGC:CRS84" => Some(4326),
        b"OGC:CRS84h" => Some(4979),
        _ => None,
    }
}

/// Whether `explicit` may restore a wire-alias CRS over an embedded EPSG SRID
/// that is exactly that alias's code (e.g. `crs='OGC:CRS84'` over SRID 4326).
/// A genuine conflict (different code) still raises at the decode guard.
pub(crate) fn is_wire_alias_restore(explicit: &str, embedded: &str) -> bool {
    wire_srid_alias(explicit).is_some_and(|code| crs::parse_epsg(embedded) == Some(code))
}

/// Resolve the integer SRID for an extended text/binary format, or `None` when
/// `include_srid` is false.
///
/// EWKT/EWKB carry integer SRIDs: EPSG-authority codes plus the two exact
/// PostGIS wire aliases (`OGC:CRS84` → 4326, `OGC:CRS84h` → 4979). No SRID is
/// inferred from general CRS equivalence.
///
/// `include_srid=True` is never a silent no-op: a CRS-free geometry and a CRS
/// outside that table both raise (callers must clear the flag or attach an
/// EPSG/alias CRS).
fn extended_srid_code(
    crs: Option<&str>,
    include_srid: bool,
    format_label: &'static str,
) -> Result<Option<u32>> {
    if !include_srid {
        return Ok(None);
    }
    let Some(crs) = crs else {
        return Err(crs::CrsError::message(format!(
            "{format_label} include_srid=True requires an EPSG-authority CRS, but the geometry is \
             CRS-free; set include_srid=False or attach an EPSG CRS"
        )));
    };
    if let Some(code) = crs::parse_epsg(crs) {
        return Ok(Some(code));
    }
    if let Some(code) = wire_srid_alias(crs) {
        return Ok(Some(code));
    }
    Err(crs::CrsError::message(format!(
        "{format_label} SRID requires an EPSG-authority CRS, but {crs:?} has no EPSG code; \
         set include_srid=False or reproject to an EPSG CRS"
    )))
}

/// Untrusted polygon-ring admission shared by WKT, WKB, pickle, and GeoArrow.
///
/// Thin alias of [`Ring::closed_coordseq`] — `Ring` is the single admission
/// owner so constructors and serialized ingresses cannot diverge. GeoJSON keeps
/// the stricter RFC 7946 rule (explicit close only) in its own reader. Trusted
/// transforms of an already-admitted ring still use [`Ring::from_trusted_closed`].
pub(crate) fn admit_closed_ring(coords: CoordSeq) -> Result<Ring> {
    Ring::closed_coordseq(coords)
}

/// Reject multipart layouts that would invent missing Z/M under a union axes
/// header when writing WKT/WKB.
pub(crate) fn require_serializable_axes(shape: &Shape) -> Result<()> {
    match shape {
        Shape::MultiLineString(lines) => {
            multi_linestring_output_axes(lines)?;
        },
        Shape::MultiPolygon(polygons) => {
            multi_polygon_output_axes(polygons)?;
        },
        Shape::Polygon(polygon) => {
            polygon_output_axes(polygon)?;
        },
        Shape::GeometryCollection(members) => {
            for member in members {
                require_serializable_axes(member)?;
            }
        },
        _ => {},
    }
    Ok(())
}

/// Axes of every line member of a MultiLineString, **including typed empties**.
///
/// Empty `LineSeq` values still carry declared axes (`LINESTRING Z EMPTY` is a
/// zero-length XYZ sequence). Skipping them let `XY` + `LINESTRING Z EMPTY`
/// admit under a Z union header and then invent `z=0` on write — empty members
/// participate in axis homogeneity exactly like non-empty ones.
pub(crate) fn multi_linestring_output_axes(lines: &[LineSeq]) -> Result<CoordinateAxes> {
    let mut axes: Option<CoordinateAxes> = None;
    for line in lines {
        let member = line.axes();
        match axes {
            None => axes = Some(member),
            Some(prev) if prev == member => {},
            Some(prev) => {
                return Err(GeometryErrorKind::message(format!(
                    "MultiLineString members must share one coordinate axes layout for serialization; \
                     got {} and {} — promote with force_3d/set_m or split the members",
                    prev.as_str(),
                    member.as_str(),
                )));
            },
        }
    }
    Ok(axes.unwrap_or(CoordinateAxes::XY))
}

/// Axes of a single polygon's rings (must be homogeneous).
pub(crate) fn polygon_output_axes(polygon: &Polygon) -> Result<CoordinateAxes> {
    let mut axes: Option<CoordinateAxes> = None;
    for ring in polygon.rings() {
        let member = ring.axes();
        match axes {
            None => axes = Some(member),
            Some(prev) if prev == member => {},
            Some(prev) => {
                return Err(GeometryErrorKind::message(format!(
                    "Polygon rings must share one coordinate axes layout for serialization; \
                     got {} and {} — promote with force_3d/set_m first",
                    prev.as_str(),
                    member.as_str(),
                )));
            },
        }
    }
    Ok(axes.unwrap_or(CoordinateAxes::XY))
}

/// Axes of every polygon member of a MultiPolygon (per-polygon ring union;
/// members must still agree with each other so writers never invent Z/M).
pub(crate) fn multi_polygon_output_axes(polygons: &[Polygon]) -> Result<CoordinateAxes> {
    let mut axes: Option<CoordinateAxes> = None;
    for polygon in polygons {
        let member = polygon_output_axes(polygon)?;
        match axes {
            None => axes = Some(member),
            Some(prev) if prev == member => {},
            Some(prev) => {
                return Err(GeometryErrorKind::message(format!(
                    "MultiPolygon members must share one coordinate axes layout for serialization; \
                     got {} and {} — promote with force_3d/set_m or split the members",
                    prev.as_str(),
                    member.as_str(),
                )));
            },
        }
    }
    Ok(axes.unwrap_or(CoordinateAxes::XY))
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

/// Resolve an optional normalized EWKB/EWKT SRID (`None` = CRS-free).
pub(crate) fn crs_from_optional_srid(srid: Option<u32>) -> Result<Option<smol_str::SmolStr>> {
    srid.map_or(Ok(None), crs_from_srid)
}

/// Resolve each distinct nonzero EWKB/EWKT SRID at most once per batch.
///
/// Uniform multi-row ingest (the common case) hits the one-slot cache on every
/// row after the first, so bulk `from_wkb` / EWKT / Arrow WKB no longer pay
/// `format!("EPSG:{code}")` + canonicalize per row. A second distinct code
/// still resolves (and fails SharedCrs / conflict guards with the same
/// messages) without re-canonicalizing the first.
#[derive(Default)]
pub(crate) struct SridCrsCache {
    last: Option<(u32, Option<smol_str::SmolStr>)>,
}

impl SridCrsCache {
    /// Resolve `srid` through [`crs_from_srid`], reusing the previous result
    /// when the code repeats.
    pub(crate) fn resolve(&mut self, srid: Option<u32>) -> Result<Option<smol_str::SmolStr>> {
        let Some(code) = srid else {
            return Ok(None);
        };
        if let Some((cached, crs)) = &self.last
            && *cached == code
        {
            return Ok(crs.clone());
        }
        let crs = crs_from_srid(code)?;
        self.last = Some((code, crs.clone()));
        Ok(crs)
    }
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

/// Decoded WKB/EWKB payload: shape plus the normalized embedded SRID.
///
/// `srid` is `None` for plain WKB and for PostGIS SRID 0 (unknown). Nonzero
/// codes stay numeric until a consumer resolves them through
/// [`crs_from_srid`] / [`SridCrsCache`] so bulk ingest can canonicalize each
/// distinct code once.
pub(crate) struct WkbGeometry {
    pub shape: Shape,
    pub srid: Option<u32>,
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
/// Legacy ``crs`` declarations are captured during the typed text probe (same
/// order as [`collect_geojson_legacy_crs`]) so callers need no second full
/// `serde_json::Value` parse of the text.
pub(crate) struct GeoJsonTextParse {
    pub input: GeoJsonInput,
    /// Legacy ``crs`` members in reconciliation order (own, then nested).
    pub legacy_crs: Vec<serde_json::Value>,
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

#[cfg(test)]
pub(crate) use geojson::to_geojson_string_with_z;
pub(crate) use geojson::{
    DefiningMembers, LegacyGeoJsonCrsPolicy, collect_geojson_legacy_crs, geojson_output_shape,
    parse_geojson, parse_geojson_text, reconcile_legacy_geojson_crs,
    reject_rfc7946_cross_type_members, reject_rfc7946_value_object, to_geojson_string,
};
pub(crate) use wkb::{
    WkbCoordArena, parse_wkb, parse_wkb_batch, to_wkb, to_wkb_len, wkb_len, write_wkb_into,
    write_wkb_to,
};
pub(crate) use wkt::{
    WktDimension, WktNumberFormat, parse_wkt, to_wkt, to_wkt_display, to_wkt_preview,
    to_wkt_with_dimension,
};
