//! Geometry serialization codecs: WKT, WKB, and GeoJSON.

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;

use thiserror::Error;

use crate::crs;
use crate::error::{Error as CrateError, ParseFormat, Result};
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates, GeometryErrorKind, GeometryKind, LineSeq, Point,
    Polygon, Ring, Shape, ShapeCountQuery, ShapeOwnerKey, SharedShapeOwners,
};

const WKB_Z_OFFSET: u32 = 1000;
const WKB_M_OFFSET: u32 = 2000;
const WKB_ZM_OFFSET: u32 = 3000;
const EWKB_Z_FLAG: u32 = 0x8000_0000;
const EWKB_M_FLAG: u32 = 0x4000_0000;
const EWKB_SRID_FLAG: u32 = 0x2000_0000;

/// The serialization readers use the same admission limit as `Shape`.
pub(crate) use crate::geometry::MAX_NESTING;

/// A parse failure for serialized geometry input. Surfaces as
/// `gometry.ParseError`. Constructors return the crate-wide
/// [`Error`](CrateError) so call sites build the boxed error in one step.
#[derive(Debug, Error)]
#[error("invalid {}: {detail}", format.display())]
pub struct IoError {
    format: ParseFormat,
    detail: Box<str>,
    position: Option<usize>,
    path: Option<Box<str>>,
}

impl IoError {
    /// The format label for the `ParseError.format` attribute.
    pub const fn format_label(&self) -> &'static str {
        self.format.label()
    }

    /// Polyline historically reports its decoder detail without the generic
    /// ``invalid polyline:`` prefix; preserve that public error text after its
    /// parser moved to the crate-error boundary.
    pub(crate) fn display_message(&self) -> String {
        if self.format == ParseFormat::Polyline {
            self.detail.to_string()
        } else {
            self.to_string()
        }
    }

    pub const fn position(&self) -> Option<usize> {
        self.position
    }

    pub(crate) fn set_path_if_missing(&mut self, path: &str) {
        if self.path.is_none() {
            self.path = Some(path.into());
        }
    }

    pub(crate) fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    pub(crate) const fn set_position_if_missing(&mut self, position: usize) {
        if self.position.is_none() {
            self.position = Some(position);
        }
    }

    pub(crate) const fn offset_position(&mut self, offset: usize) {
        if let Some(position) = self.position.as_mut() {
            *position = position.wrapping_add(offset);
        }
    }

    pub(crate) fn parse(format: ParseFormat, detail: impl Into<Box<str>>) -> CrateError {
        Self {
            format,
            detail: detail.into(),
            position: None,
            path: None,
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
            path: None,
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
pub(crate) fn extended_srid_code(
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
/// transforms of an already-admitted ring still use [`Ring::from_closed`].
pub(crate) fn admit_closed_ring(coords: CoordSeq) -> Result<Ring> {
    Ring::closed_coordseq(coords)
}

#[derive(Clone, Copy)]
enum BoundedCount {
    Within(usize),
    Exceeds,
}

const WKT_MIN_KEYWORD_BYTES: usize = 5;
const WKT_MIN_XY_POSITION_BYTES: usize = 3;
const WKT_AXIS_TAG_MAX_BYTES: usize = 3;
pub(crate) const GEOJSON_POSITION_UPPER_BYTES: usize = 80;
const GEOJSON_OBJECT_FRAMING_UPPER_BYTES: usize = 64;
const GEOJSON_ARRAY_FRAMING_UPPER_BYTES: usize = 3;

impl BoundedCount {
    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Within(left), Self::Within(right)) => left
                .checked_add(right)
                .filter(|count| isize::try_from(*count).is_ok())
                .map_or(Self::Exceeds, Self::Within),
            _ => Self::Exceeds,
        }
    }

    const fn get(self) -> Option<usize> {
        match self {
            Self::Within(count) => Some(count),
            Self::Exceeds => None,
        }
    }
}

/// Shape and visible-coordinate occurrences admitted for a full text output.
#[derive(Clone, Copy)]
pub(crate) struct TextShapeSummary {
    shapes: BoundedCount,
    coordinates: BoundedCount,
    structures: BoundedCount,
    /// Fixed WKT keywords, delimiters, and separators beyond the 5-byte
    /// keyword and 3-byte XY-position minima; float spelling stays in writer.
    wkt_extra: BoundedCount,
}

impl TextShapeSummary {
    fn lower_bound(
        self,
        bytes_per_shape: usize,
        bytes_per_coordinate: usize,
        extra: BoundedCount,
        limit: usize,
    ) -> TextWidthBound {
        let Some(shape_bytes) = self
            .shapes
            .get()
            .and_then(|count| count.checked_mul(bytes_per_shape))
        else {
            return TextWidthBound::Exceeds { limit };
        };
        let Some(coordinate_bytes) = self
            .coordinates
            .get()
            .and_then(|count| count.checked_mul(bytes_per_coordinate))
        else {
            return TextWidthBound::Exceeds { limit };
        };
        let Some(length) = shape_bytes
            .checked_add(coordinate_bytes)
            .and_then(|length| length.checked_add(extra.get()?))
            .filter(|length| *length <= limit)
        else {
            return TextWidthBound::Exceeds { limit };
        };
        TextWidthBound::Within {
            lower_bound: length,
            limit,
        }
    }

    pub(crate) fn wkt_lower_bound(self, limit: usize) -> TextWidthBound {
        self.lower_bound(
            WKT_MIN_KEYWORD_BYTES,
            WKT_MIN_XY_POSITION_BYTES,
            self.wkt_extra,
            limit,
        )
    }

    pub(crate) fn geojson_lower_bound(self, limit: usize) -> TextWidthBound {
        // A Point EMPTY object is the smallest GeoJSON geometry object (33
        // bytes). Each emitted position replaces its `[]` coordinate value by
        // at least `[0,0]`, adding three bytes without double-counting brackets.
        self.lower_bound(33, 3, BoundedCount::Within(0), limit)
    }

    pub(crate) fn wkt_upper_bound(
        self,
        dimensions: usize,
        maximum_ordinate_bytes: usize,
    ) -> Option<usize> {
        // The summary keeps exact fixed syntax in `wkt_extra`; each shape
        // contributes its base keyword and at most a three-byte axis tag.
        let shapes = self.shapes.get()?;
        let coordinates = self.coordinates.get()?;
        let keyword_and_axis_bytes = shapes
            .checked_mul(WKT_MIN_KEYWORD_BYTES)?
            .checked_add(shapes.checked_mul(WKT_AXIS_TAG_MAX_BYTES)?)?;
        let ordinate_width = dimensions
            .checked_mul(maximum_ordinate_bytes)?
            .checked_add(dimensions.checked_sub(1)?)?;
        let coordinate_bytes = coordinates.checked_mul(ordinate_width)?;
        keyword_and_axis_bytes
            .checked_add(self.wkt_extra.get()?)?
            .checked_add(coordinate_bytes)
    }

    pub(crate) fn geojson_upper_bound(self) -> Option<usize> {
        let shapes = self.shapes.get()?;
        let coordinates = self.coordinates.get()?;
        let structures = self.structures.get()?;
        // XYZ positions fit in 80 bytes including their own delimiters; the
        // extra coordinate byte bounds their list separators. A geometry
        // object needs under 64 bytes for its framing, and each non-position
        // array needs two brackets plus at most one separator per child.
        let position_bytes = coordinates.checked_mul(GEOJSON_POSITION_UPPER_BYTES)?;
        let position_separators = coordinates;
        let object_and_collection_separators = shapes
            .checked_mul(GEOJSON_OBJECT_FRAMING_UPPER_BYTES)?
            .checked_add(shapes)?;
        let array_delimiters_and_separators =
            structures.checked_mul(GEOJSON_ARRAY_FRAMING_UPPER_BYTES)?;
        position_bytes
            .checked_add(position_separators)?
            .checked_add(object_and_collection_separators)?
            .checked_add(array_delimiters_and_separators)
    }

    pub(crate) const fn coordinate_count(self) -> Option<usize> {
        self.coordinates.get()
    }

    pub(crate) const fn structure_count(self) -> Option<usize> {
        self.structures.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TextWidthBound {
    Within { lower_bound: usize, limit: usize },
    Exceeds { limit: usize },
}

impl TextWidthBound {
    pub(crate) fn require_within(self) -> Result<()> {
        match self {
            Self::Within { .. } => Ok(()),
            Self::Exceeds { .. } => Err(crate::error::ResourceError::SizeOverflow.into()),
        }
    }
}

#[derive(Default)]
struct TextShapeSummaryMemo {
    collections: Option<HashMap<ShapeOwnerKey, TextShapeSummary>>,
}

impl TextShapeSummaryMemo {
    fn collection(&self, owner: ShapeOwnerKey) -> Option<TextShapeSummary> {
        self.collections
            .as_ref()
            .and_then(|collections| collections.get(&owner))
            .copied()
    }

    fn insert_collection(&mut self, owner: ShapeOwnerKey, summary: TextShapeSummary) {
        self.collections
            .get_or_insert_with(HashMap::new)
            .insert(owner, summary);
    }
}

#[derive(Clone, Copy)]
enum TextCodec {
    Wkt,
    GeoJson,
}

/// Count text-output occurrences while summarizing each shared collection
/// owner once. Cached summaries are still added for every member edge.
fn text_shape_summary(shape: &Shape, codec: TextCodec) -> TextShapeSummary {
    fn summarize(
        shape: &Shape,
        codec: TextCodec,
        memo: &mut TextShapeSummaryMemo,
    ) -> TextShapeSummary {
        if let Shape::GeometryCollection(collection) = shape {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some(summary) = owner.and_then(|owner| memo.collection(owner)) {
                return summary;
            }
            let mut summary = TextShapeSummary {
                shapes: BoundedCount::Within(1),
                coordinates: BoundedCount::Within(0),
                structures: BoundedCount::Within(1),
                wkt_extra: text_wkt_collection_extra(collection.len(), codec)
                    .map_or(BoundedCount::Exceeds, BoundedCount::Within),
            };
            for member in collection {
                let child = summarize(member, codec, memo);
                summary.shapes = summary.shapes.add(child.shapes);
                summary.coordinates = summary.coordinates.add(child.coordinates);
                summary.structures = summary.structures.add(child.structures);
                summary.wkt_extra = summary.wkt_extra.add(child.wkt_extra);
            }
            if let Some(owner) = owner {
                memo.insert_collection(owner, summary);
            }
            return summary;
        }
        TextShapeSummary {
            shapes: BoundedCount::Within(1),
            coordinates: ShapeCountQuery::default()
                .coordinate_count(shape)
                .filter(|count| isize::try_from(*count).is_ok())
                .map_or(BoundedCount::Exceeds, BoundedCount::Within),
            structures: text_structure_count_hint(shape)
                .filter(|count| isize::try_from(*count).is_ok())
                .map_or(BoundedCount::Exceeds, BoundedCount::Within),
            wkt_extra: match codec {
                TextCodec::Wkt => text_wkt_shape_extra(shape)
                    .filter(|count| isize::try_from(*count).is_ok())
                    .map_or(BoundedCount::Exceeds, BoundedCount::Within),
                TextCodec::GeoJson => BoundedCount::Within(0),
            },
        }
    }

    summarize(shape, codec, &mut TextShapeSummaryMemo::default())
}

pub(crate) fn wkt_text_summary(shape: &Shape) -> TextShapeSummary {
    text_shape_summary(shape, TextCodec::Wkt)
}

pub(crate) fn geojson_text_summary(shape: &Shape) -> TextShapeSummary {
    text_shape_summary(shape, TextCodec::GeoJson)
}

fn text_structure_count_hint(shape: &Shape) -> Option<usize> {
    match shape {
        Shape::MultiPoint(points) => points.member_count().checked_add(1),
        Shape::MultiLineString(lines) => lines.member_count().checked_add(1),
        Shape::Polygon(polygon) => polygon.holes.len().checked_add(2),
        Shape::MultiPolygon(polygons) => {
            let mut total = polygons.member_count().checked_add(1)?;
            for polygon in polygons.iter() {
                total = total.checked_add(polygon.holes.len().checked_add(1)?)?;
            }
            Some(total)
        },
        Shape::GeometryCollection(_) => unreachable!("collections are summarized recursively"),
        Shape::Point(_) | Shape::LineString(_) | Shape::Empty(..) => Some(1),
    }
}

fn text_wkt_collection_extra(member_count: usize, codec: TextCodec) -> Option<usize> {
    if matches!(codec, TextCodec::GeoJson) {
        return Some(0);
    }
    let type_extra = "GEOMETRYCOLLECTION"
        .len()
        .checked_sub(WKT_MIN_KEYWORD_BYTES)?;
    let separators = member_count.saturating_sub(1).checked_mul(2)?;
    type_extra.checked_add(3)?.checked_add(separators)
}

fn text_wkt_shape_extra(shape: &Shape) -> Option<usize> {
    let type_extra = shape
        .geometry_type()
        .len()
        .checked_sub(WKT_MIN_KEYWORD_BYTES)?;
    match shape {
        Shape::Point(_) => type_extra.checked_add(3),
        Shape::MultiPoint(points) => {
            let mut extra = type_extra
                .checked_add(3)?
                .checked_add(points.member_count().saturating_sub(1).checked_mul(2)?)?;
            for member in points.members() {
                extra = extra.checked_add(match member {
                    crate::geometry::MultiMember::Empty => 5,
                    crate::geometry::MultiMember::Value(_) => 2,
                })?;
            }
            Some(extra)
        },
        Shape::LineString(line) => type_extra
            .checked_add(3)?
            .checked_add(line.len().saturating_sub(1).checked_mul(2)?),
        Shape::MultiLineString(lines) => {
            let mut extra = type_extra
                .checked_add(3)?
                .checked_add(lines.member_count().saturating_sub(1).checked_mul(2)?)?;
            for member in lines.members_with_values() {
                extra = extra.checked_add(match member {
                    crate::geometry::MultiMember::Empty => 5,
                    crate::geometry::MultiMember::Value(line) => {
                        2_usize.checked_add(line.len().saturating_sub(1).checked_mul(2)?)?
                    },
                })?;
            }
            Some(extra)
        },
        Shape::Polygon(polygon) => polygon_wkt_ring_extra(polygon)
            .and_then(|rings| type_extra.checked_add(3)?.checked_add(rings)),
        Shape::MultiPolygon(polygons) => {
            let mut extra = type_extra
                .checked_add(3)?
                .checked_add(polygons.member_count().saturating_sub(1).checked_mul(2)?)?;
            for member in polygons.members_with_values() {
                extra = extra.checked_add(match member {
                    crate::geometry::MultiMember::Empty => 5,
                    crate::geometry::MultiMember::Value(polygon) => {
                        2_usize.checked_add(polygon_wkt_ring_extra(polygon)?)?
                    },
                })?;
            }
            Some(extra)
        },
        Shape::GeometryCollection(_) => unreachable!("collections are summarized recursively"),
        Shape::Empty(..) => type_extra.checked_add(6),
    }
}

fn polygon_wkt_ring_extra(polygon: &Polygon) -> Option<usize> {
    let mut extra = 2_usize.checked_add(polygon.shell.len().saturating_sub(1).checked_mul(2)?)?;
    for hole in polygon.holes.iter() {
        extra = extra
            .checked_add(4_usize.checked_add(hole.len().saturating_sub(1).checked_mul(2)?)?)?;
    }
    Some(extra)
}

/// A string sink that rejects an append before its byte length exceeds the
/// representable Rust string length.
pub(crate) struct CheckedText {
    value: String,
    remaining: usize,
}

impl CheckedText {
    #[expect(
        clippy::disallowed_methods,
        reason = "Rendered-size estimates can substantially exceed the actual text length."
    )]
    pub(crate) fn with_capacity_hint(capacity: usize, limit: usize) -> Self {
        let limit = limit.min(isize::MAX.unsigned_abs());
        let capacity = capacity.min(limit);
        let mut value = String::new();
        if isize::try_from(capacity).is_ok() {
            let _reservation = value.try_reserve(capacity);
        }
        Self {
            value,
            remaining: limit,
        }
    }

    pub(crate) fn push_str(&mut self, value: &str) -> Result<()> {
        self.check_append(value.len())?;
        self.value.push_str(value);
        Ok(())
    }

    pub(crate) fn push(&mut self, value: char) -> Result<()> {
        self.check_append(value.len_utf8())?;
        self.value.push(value);
        Ok(())
    }

    pub(crate) fn write_formatted(&mut self, arguments: fmt::Arguments<'_>) -> Result<()> {
        fmt::write(self, arguments).map_err(|_| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) const fn len(&self) -> usize {
        self.value.len()
    }

    #[cfg(test)]
    pub(crate) fn as_str(&self) -> &str {
        &self.value
    }

    pub(crate) fn into_string(self) -> String {
        self.value
    }

    fn check_append(&mut self, bytes: usize) -> Result<()> {
        self.remaining = self
            .remaining
            .checked_sub(bytes)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    }
}

impl fmt::Write for CheckedText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let Some(remaining) = self.remaining.checked_sub(value.len()) else {
            return Err(fmt::Error);
        };
        self.remaining = remaining;
        self.value.push_str(value);
        Ok(())
    }
}

/// Reject polygon layouts that would invent missing Z/M under a union axes
/// header when admitting or writing a multipart geometry.
pub(crate) fn require_serializable_axes(shape: &Shape) -> Result<()> {
    require_serializable_axes_inner(shape, &mut SharedShapeOwners::for_collections())
}

fn require_serializable_axes_inner(shape: &Shape, owners: &mut SharedShapeOwners) -> Result<()> {
    match shape {
        Shape::Polygon(polygon) => {
            polygon_output_axes(polygon)?;
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                polygon_output_axes(polygon)?;
            }
        },
        Shape::GeometryCollection(members) => {
            if !owners.first_collection(members.as_slice(), members.members_are_shared()) {
                return Ok(());
            }
            for member in members {
                require_serializable_axes_inner(member, owners)?;
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => {},
    }
    Ok(())
}

/// Axes of a single polygon's rings (must be homogeneous).
pub(crate) fn polygon_output_axes(polygon: &Polygon) -> Result<CoordinateAxes> {
    polygon_output_axes_with(polygon, |_, detail| GeometryErrorKind::message(detail))
}

/// The one ring-axis traversal behind serialization and input admission.
///
/// The mismatch callback owns the error boundary: serializers retain the
/// existing geometry error, while GeoJSON can attach the offending ring path
/// before the generic parser wrapper fills in any missing context.
pub(crate) fn polygon_output_axes_with<F>(
    polygon: &Polygon,
    mut on_mismatch: F,
) -> Result<CoordinateAxes, CrateError>
where
    F: FnMut(usize, Box<str>) -> CrateError,
{
    let mut axes: Option<CoordinateAxes> = None;
    for (index, ring) in polygon.rings().enumerate() {
        let member = ring.axes();
        match axes {
            None => axes = Some(member),
            Some(prev) if prev == member => {},
            Some(prev) => {
                return Err(on_mismatch(
                    index,
                    format!(
                        "Polygon rings must share one coordinate axes layout for serialization; \
                         got {} and {} — promote with force_3d/with_coordinates(m=...) first",
                        prev.as_str(),
                        member.as_str(),
                    )
                    .into_boxed_str(),
                ));
            },
        }
    }
    Ok(axes.unwrap_or(CoordinateAxes::XY))
}

/// Normalize an EWKB/EWKT SRID at its wire boundary. PostGIS reserves SRID 0
/// as unknown/unspecified, so it becomes `None`.
pub(crate) const fn normalize_wire_srid(srid: Option<u32>) -> Option<NonZeroU32> {
    match srid {
        Some(code) => NonZeroU32::new(code),
        None => None,
    }
}

/// Map a nonzero EWKT/EWKB SRID to a CRS.
///
/// Every code is resolved through the canonical PROJ-backed CRS parser so
/// invalid codes fail at parse time rather than later on first introspection.
pub(crate) fn crs_from_srid(code: NonZeroU32) -> Result<smol_str::SmolStr> {
    crs::definition(&format!("EPSG:{}", code.get()))
}

/// Resolve an optional normalized EWKB/EWKT SRID (`None` = CRS-free).
pub(crate) fn crs_from_optional_srid(
    srid: Option<NonZeroU32>,
) -> Result<Option<smol_str::SmolStr>> {
    srid.map(crs_from_srid).transpose()
}

/// Reuse the most recently resolved nonzero EWKB/EWKT SRID.
///
/// Per-row explicit/storage CRS checks reuse the cached result for adjacent
/// repeated codes. Without those checks, a uniform batch resolves its shared
/// SRID once when the frame is finished. A non-adjacent repeat is resolved
/// again after another code replaces this one-slot cache.
#[derive(Default)]
pub(crate) struct SridCrsCache {
    /// Uniform explicit-CRS WKB uses 2.06× more instructions without this cache.
    last: Option<(NonZeroU32, smol_str::SmolStr)>,
}

impl SridCrsCache {
    /// Resolve a nonzero `srid` through [`crs_from_srid`], reusing the previous
    /// result when the code repeats.
    pub(crate) fn resolve(&mut self, code: NonZeroU32) -> Result<smol_str::SmolStr> {
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

/// Decoded WKB/EWKB payload: shape plus the normalized embedded SRID.
///
/// `srid` is `None` for plain WKB and for PostGIS SRID 0 (unknown). Nonzero
/// codes stay numeric until a consumer resolves them through [`crs_from_srid`];
/// bulk admission defers resolution until a fallback check or final frame
/// resolution needs the definition.
pub(crate) struct WkbGeometry {
    pub shape: Shape,
    pub srid: Option<NonZeroU32>,
}

fn parse_content(format: ParseFormat, error: CrateError) -> CrateError {
    match error.kind() {
        crate::error::ErrorKind::Geometry(_) => IoError::parse(format, error.to_string()),
        crate::error::ErrorKind::Crs(_)
        | crate::error::ErrorKind::Io(_)
        | crate::error::ErrorKind::Frame(_)
        | crate::error::ErrorKind::Resource(_) => error,
    }
}

mod geojson;
mod wkb;
mod wkt;

#[cfg(test)]
pub(crate) use geojson::parse_geojson;
#[cfg(test)]
pub(crate) use geojson::to_geojson_string;
pub(crate) use geojson::{
    DefiningMembers, GEOJSON_M_ERROR, GeoJsonCrs, GeoJsonMemberKind, LegacyGeoJsonCrsPolicy,
    geojson_interface_error, parse_geojson_at, reject_rfc7946_cross_type_members,
    reject_rfc7946_value_object, to_geojson_string_with_metric, validate_geojson_bbox,
    validate_geojson_bbox_member, validate_geojson_feature_id, validate_geojson_feature_members,
    validate_geojson_feature_properties,
};
pub(crate) use wkb::{parse_wkb, to_wkb, wkb_len, write_wkb_to_srid};
pub(crate) use wkt::{
    WktNumberFormat, classify_wkt_type_token, parse_wkt, to_wkt, to_wkt_display, to_wkt_preview,
    to_wkt_with_srid,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{MOrdinate, MultiMember, MultiPolygon, ZOrdinate};

    fn ring(axes: CoordinateAxes) -> Ring {
        let point = |x: f64, y: f64, z: Option<f64>| {
            Point::new_axes(x, y, ZOrdinate(z), MOrdinate(None)).expect("finite test point")
        };
        let points = [
            point(0.0, 0.0, axes.has_z().then_some(1.0)),
            point(2.0, 0.0, axes.has_z().then_some(1.0)),
            point(0.0, 2.0, axes.has_z().then_some(1.0)),
            point(0.0, 0.0, axes.has_z().then_some(1.0)),
        ];
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap()
    }

    fn doubled_collection(depth: usize) -> Shape {
        let mut shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        for _ in 0..depth {
            let child = shape.clone();
            shape = Shape::geometry_collection(vec![child.clone(), child])
                .expect("collection remains within its nesting limit");
        }
        shape
    }

    #[test]
    fn text_lower_bounds_reject_depth_58_doubled_geometry() {
        let shape = doubled_collection(58);
        let limit = isize::MAX.unsigned_abs();
        assert!(matches!(
            wkt_text_summary(&shape).wkt_lower_bound(limit),
            TextWidthBound::Exceeds { limit: got } if got == limit
        ));
        assert!(matches!(
            geojson_text_summary(&shape).geojson_lower_bound(limit),
            TextWidthBound::Exceeds { limit: got } if got == limit
        ));
        let error = to_wkt_with_srid(&shape, Some("not a CRS"), true)
            .expect_err("WKT body width fails before SRID prefix validation");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
    }

    #[test]
    fn geojson_lower_bound_fits_a_point_document() {
        let shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let bound = geojson_text_summary(&shape).geojson_lower_bound(isize::MAX.unsigned_abs());
        assert!(matches!(bound, TextWidthBound::Within { .. }));
        let lower_bound = match bound {
            TextWidthBound::Within { lower_bound, .. } => lower_bound,
            TextWidthBound::Exceeds { .. } => 0,
        };
        let output = to_geojson_string(&shape, crate::geometry::TopologyDomain::Cartesian)
            .expect("Point GeoJSON writes");
        assert!(lower_bound <= output.len());
    }

    #[rstest::rstest]
    #[case::point_0_0("POINT (0 0)")]
    #[case::point_empty("POINT EMPTY")]
    #[case::multipoint_empty_0_0("MULTIPOINT (EMPTY, (0 0))")]
    #[case::linestring_0_0_1_1("LINESTRING (0 0, 1 1)")]
    #[case::multilinestring_empty_0_0_1_1("MULTILINESTRING (EMPTY, (0 0, 1 1))")]
    #[case::polygon_0_0_2_0_2_2_0_0_0_5_0_5_1_0_5_1_1_0_5_0_5(
        "POLYGON ((0 0, 2 0, 2 2, 0 0), (0.5 0.5, 1 0.5, 1 1, 0.5 0.5))"
    )]
    #[case::multipolygon_empty_0_0_2_0_2_2_0_0("MULTIPOLYGON (EMPTY, ((0 0, 2 0, 2 2, 0 0)))")]
    #[case::geometrycollection_point_0_0_geometrycollection_point_empty(
        "GEOMETRYCOLLECTION (POINT (0 0), GEOMETRYCOLLECTION (POINT EMPTY))"
    )]
    fn wkt_syntax_lower_bound_fits_each_geometry_kind(#[case] source: &'static str) {
        let shape = parse_wkt(source).expect("fixture WKT parses");
        let output = to_wkt(&shape).expect("fixture geometry serializes");
        let bound = wkt_text_summary(&shape).wkt_lower_bound(isize::MAX.unsigned_abs());
        assert!(matches!(bound, TextWidthBound::Within { .. }), "{source}");
        let lower_bound = match bound {
            TextWidthBound::Within { lower_bound, .. } => lower_bound,
            TextWidthBound::Exceeds { .. } => 0,
        };
        assert!(
            lower_bound <= output.len(),
            "lower bound {lower_bound} exceeds output length {} for {source}",
            output.len()
        );
    }

    #[test]
    fn serializable_axes_reject_mixed_multipolygon_rings() {
        let polygon = Polygon::new(ring(CoordinateAxes::XYZ), vec![ring(CoordinateAxes::XY)]);
        let shape = Shape::MultiPolygon(
            MultiPolygon::try_new(CoordinateAxes::XYZ, vec![MultiMember::Value(polygon)])
                .expect("polygon aggregate axes match"),
        );
        let error = require_serializable_axes(&shape).expect_err("mixed rings must be rejected");
        assert!(
            error
                .to_string()
                .contains("Polygon rings must share one coordinate axes layout")
        );
    }

    #[test]
    fn checked_text_rejects_an_append_before_crossing_its_limit() {
        let mut out = CheckedText {
            value: String::new(),
            remaining: 3,
        };
        out.push_str("abc").expect("initial bytes fit");
        let error = out
            .push('d')
            .expect_err("the fourth byte exceeds the limit");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
        assert_eq!(out.as_str(), "abc");

        let mut out = CheckedText {
            value: String::new(),
            remaining: 3,
        };
        let error = out
            .write_formatted(format_args!("abcd"))
            .expect_err("formatted bytes obey the same limit");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
        ));
        assert_eq!(out.as_str(), "");
    }
}
