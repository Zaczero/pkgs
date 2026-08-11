use std::borrow::Cow;
use std::fmt;

use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::geometry::{CoordSeqBuilder, MOrdinate, ZOrdinate};
use crate::io::{
    CoordSeq, CoordinateAxes, Coordinates, EmptyKind, GeoJsonInput, GeoJsonTextParse,
    GeometryErrorKind, IoError, IoGeometryKind, LineSeq, ParseFormat, Point, Polygon, Result, Ring,
    Shape, crs, parse_content,
};

#[cfg(test)]
pub(crate) fn to_geojson_string_with_z(shape: &Shape, include_z: bool) -> String {
    // The writer-parity fixtures are CRS-free planar shapes: only ring winding
    // is canonicalized (no geographic domain check or antimeridian cut).
    if include_z {
        to_geojson_string::<true>(shape, false)
    } else {
        to_geojson_string::<false>(shape, false)
    }
    .expect("test geometry serializes to GeoJSON")
}

/// Serialize a geometry to an RFC 7946 GeoJSON string.
///
/// GeoJSON coordinates are WGS84 longitude/latitude by specification
/// (RFC 7946 §4). When `geographic` (the geometry carries a WGS84 frame),
/// the writer enforces the standard's geographic rules: positions are
/// validated against the lon/lat domain (§4), antimeridian-crossing parts are
/// cut into seam-following multiparts (§3.1.9), and polygon rings are emitted
/// with right-hand winding — exterior counterclockwise, holes clockwise
/// (§3.1.6). A CRS-free geometry is untagged/planar: only the ring-winding
/// representation is canonicalized, since domain and antimeridian are
/// geographic semantics that do not apply to planar coordinates.
pub(crate) fn to_geojson_string<const INCLUDE_Z: bool>(
    shape: &Shape,
    geographic: bool,
) -> Result<String> {
    let prepared = geojson_output_shape(shape, geographic)?;
    let mut out = String::with_capacity(32 + prepared.coord_count() * 24);
    write_geojson_to_impl::<INCLUDE_Z>(&mut out, &prepared);
    Ok(out)
}

/// The serialization-only view of `shape` normalized for RFC 7946 output.
///
/// Never mutates the source: a geographic geometry is split at the
/// antimeridian (a no-op that returns an owned clone for non-crossing input),
/// and any polygonal geometry is oriented to right-hand winding. Non-polygonal
/// planar geometry borrows through unchanged.
pub(crate) fn geojson_output_shape(shape: &Shape, geographic: bool) -> Result<Cow<'_, Shape>> {
    let prepared: Cow<'_, Shape> = if geographic {
        validate_geojson_domain(shape)?;
        Cow::Owned(shape.split_antimeridian()?)
    } else {
        Cow::Borrowed(shape)
    };
    // RFC 7946 §3.1.6 mandates right-hand ring winding on output; only
    // polygonal geometry carries rings, so everything else borrows through
    // without a copy. `orient_polygons` reverses a ring only when its winding
    // is wrong, so a geometry already in canonical form is not perturbed.
    if matches!(
        prepared.as_ref(),
        Shape::Polygon(_) | Shape::MultiPolygon(_) | Shape::GeometryCollection(_)
    ) {
        Ok(Cow::Owned(prepared.orient_polygons(false)))
    } else {
        Ok(prepared)
    }
}

/// Position outside the WGS84 lon/lat domain, as `(coordinate_label, value)`.
fn geojson_domain_violation(x: f64, y: f64) -> Option<(&'static str, f64)> {
    if !(-180.0..=180.0).contains(&x) {
        Some(("longitude", x))
    } else if !(-90.0..=90.0).contains(&y) {
        Some(("latitude", y))
    } else {
        None
    }
}

/// Reject any position outside the WGS84 domain before serializing a
/// geographic geometry (RFC 7946 §4). Out-of-range longitudes are an
/// unwrapped-coordinate mistake, not an antimeridian crossing (those stay in
/// `[-180, 180]` and are cut) — so the writer fails loudly rather than
/// emitting non-conforming output.
fn validate_geojson_domain(shape: &Shape) -> Result<()> {
    let mut violation = None;
    shape.for_each_point(|point| {
        if violation.is_none() {
            violation = geojson_domain_violation(point.x, point.y);
        }
    });
    if let Some((label, value)) = violation {
        return Err(GeometryErrorKind::message(format!(
            "GeoJSON {label} {value} is outside the WGS84 domain (longitude \
             [-180, 180], latitude [-90, 90]); wrap or split \
             antimeridian-crossing geometry before serializing"
        )));
    }
    Ok(())
}

fn write_geojson_to_impl<const INCLUDE_Z: bool>(out: &mut String, shape: &Shape) {
    // `type` is emitted FIRST, matching RFC 7946's examples and every mainstream
    // producer (shapely, GDAL); key order is cosmetic to a parser but conventional
    // for humans and lenient consumers.
    match shape {
        Shape::Point(point) => {
            out.push_str(r#"{"type":"Point","coordinates":"#);
            write_geojson_position::<INCLUDE_Z>(out, *point);
            out.push('}');
        },
        Shape::MultiPoint(points) => {
            out.push_str(r#"{"type":"MultiPoint","coordinates":"#);
            write_geojson_positions::<INCLUDE_Z, _>(out, points);
            out.push('}');
        },
        Shape::LineString(points) => {
            out.push_str(r#"{"type":"LineString","coordinates":"#);
            write_geojson_positions::<INCLUDE_Z, _>(out, points);
            out.push('}');
        },
        Shape::MultiLineString(lines) => {
            out.push_str(r#"{"type":"MultiLineString","coordinates":"#);
            write_geojson_lines::<INCLUDE_Z, _>(out, lines.iter().map(LineSeq::as_coords));
            out.push('}');
        },
        Shape::Polygon(polygon) => {
            out.push_str(r#"{"type":"Polygon","coordinates":"#);
            write_geojson_polygon::<INCLUDE_Z>(out, polygon);
            out.push('}');
        },
        Shape::MultiPolygon(polygons) => {
            out.push_str(r#"{"type":"MultiPolygon","coordinates":"#);
            write_geojson_polygons::<INCLUDE_Z>(out, polygons);
            out.push('}');
        },
        Shape::GeometryCollection(geometries) => {
            out.push_str(r#"{"type":"GeometryCollection","geometries":["#);
            for (idx, geometry) in geometries.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_geojson_to_impl::<INCLUDE_Z>(out, geometry);
            }
            out.push_str("]}");
        },
        // GeoJSON has no dimensional-empty form; every typed empty flattens
        // to its kind with an empty coordinates/geometries member.
        Shape::Empty(EmptyKind::GeometryCollection, _) => {
            out.push_str(r#"{"type":"GeometryCollection","geometries":[]}"#);
        },
        Shape::Empty(kind, _) => {
            out.push_str(r#"{"type":""#);
            out.push_str(kind.geometry_type());
            out.push_str(r#"","coordinates":[]}"#);
        },
    }
}

fn write_geojson_position<const INCLUDE_Z: bool>(out: &mut String, point: Point) {
    out.push('[');
    write_geojson_number(out, point.x);
    out.push(',');
    write_geojson_number(out, point.y);
    if INCLUDE_Z && let Some(z) = point.z() {
        out.push(',');
        write_geojson_number(out, z);
    }
    out.push(']');
}

fn write_geojson_positions<const INCLUDE_Z: bool, C: Coordinates + ?Sized>(
    out: &mut String,
    points: &C,
) {
    out.push('[');
    for (idx, point) in points.iter_coords().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        write_geojson_position::<INCLUDE_Z>(out, point);
    }
    out.push(']');
}

fn write_geojson_lines<'a, const INCLUDE_Z: bool, I>(out: &mut String, lines: I)
where
    I: IntoIterator<Item = &'a CoordSeq>,
{
    out.push('[');
    for (idx, line) in lines.into_iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        write_geojson_positions::<INCLUDE_Z, _>(out, line);
    }
    out.push(']');
}

fn write_geojson_polygon<const INCLUDE_Z: bool>(out: &mut String, polygon: &Polygon) {
    out.push('[');
    write_geojson_positions::<INCLUDE_Z, _>(out, &polygon.shell);
    for hole in polygon.holes.iter() {
        out.push(',');
        write_geojson_positions::<INCLUDE_Z, _>(out, hole);
    }
    out.push(']');
}

fn write_geojson_polygons<const INCLUDE_Z: bool>(out: &mut String, polygons: &[Polygon]) {
    out.push('[');
    for (idx, polygon) in polygons.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        write_geojson_polygon::<INCLUDE_Z>(out, polygon);
    }
    out.push(']');
}

fn write_geojson_number(out: &mut String, value: f64) {
    let mut buffer = zmij::Buffer::new();
    out.push_str(buffer.format_finite(value));
}

/// Content violations discovered while deserializing (non-finite ordinates,
/// short lines) surface as parse errors: the caller's failure domain is "this
/// serialized input is bad", with the structural rule as the detail.
pub(crate) fn parse_geojson(value: &Value) -> Result<Shape> {
    parse_geojson_inner(value).map_err(|error| parse_content(ParseFormat::GeoJson, error))
}

pub(crate) fn parse_geojson_text(text: &str) -> Result<GeoJsonTextParse> {
    let probe = parse_geojson_probe(text)?;
    if let Some(kind) = direct_coordinate_kind(&probe.geometry_type) {
        // RFC 7946 §7.1 before the direct coordinate decode.
        // coordinate_members count may exceed one (last-wins); presence is
        // enough for §7.1 — merge into the probe's member flags.
        let mut members = probe.members;
        if probe.coordinate_members > 0 {
            members.set(DefiningMembers::COORDINATES);
        }
        reject_rfc7946_cross_type_members(&probe.geometry_type, members)?;
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let object = DirectGeoJsonSeed {
            kind,
            coordinate_members: probe.coordinate_members,
        }
        .deserialize(&mut deserializer)
        .map_err(|error| IoError::geojson(error.to_string()))?;
        deserializer
            .end()
            .map_err(|error| IoError::geojson(error.to_string()))?;
        return Ok(GeoJsonTextParse {
            input: GeoJsonInput::Geometry(object.shape),
            legacy_crs: probe.legacy_crs,
        });
    }
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let object = GeoJsonObject::deserialize(&mut deserializer)
        .map_err(|error| IoError::geojson(error.to_string()))?;
    deserializer
        .end()
        .map_err(|error| IoError::geojson(error.to_string()))?;
    let input = geojson_object_to_input(object)?;
    Ok(GeoJsonTextParse {
        input,
        legacy_crs: probe.legacy_crs,
    })
}

/// The first pass records the final ``type``, how many duplicate
/// ``coordinates`` members exist, and every legacy ``crs`` declaration in the
/// same order as [`collect_geojson_legacy_crs`] (own first, then nested by
/// type). Coordinates / properties payloads are skipped via `IgnoredAny` so
/// the probe never builds a coordinate tree. The second pass then deserializes
/// the LAST coordinates member straight into its concrete geometry columns
/// while skipping shadowed duplicates, preserving serde_json's map semantics
/// and arbitrary object-key order.
///
/// Final-member policy also applies to ``type`` (R18): a non-string intermediate
/// ``type`` does not abort if a later string ``type`` wins — matching the
/// bytes/dict frontends that collapse via ``serde_json::Value`` last-wins.
struct GeoJsonProbe {
    geometry_type: String,
    coordinate_members: usize,
    /// Defining members that must be excluded for the final type (RFC 7946 §7.1).
    members: DefiningMembers,
    /// Legacy ``crs`` members (own then nested), reconciliation order.
    legacy_crs: Vec<Value>,
}

fn parse_geojson_probe(text: &str) -> Result<GeoJsonProbe> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let probe = deserializer
        .deserialize_map(GeoJsonProbeVisitor)
        .map_err(|error| IoError::geojson(error.to_string()))?;
    deserializer
        .end()
        .map_err(|error| IoError::geojson(error.to_string()))?;
    Ok(probe)
}

struct GeoJsonProbeVisitor;

impl<'de> Visitor<'de> for GeoJsonProbeVisitor {
    type Value = GeoJsonProbe;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GeoJSON object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut geometry_type: Option<Value> = None;
        let mut coordinate_members = 0;
        let mut members = DefiningMembers::default();
        // Own `crs` last-wins (serde_json map semantics); nested lists are
        // collected as their containers are visited, then assembled below in
        // collect_geojson_legacy_crs order (own first, then children).
        let mut own_crs: Option<Value> = None;
        let mut nested_from_geometry: Vec<Value> = Vec::new();
        let mut nested_from_features: Vec<Value> = Vec::new();
        let mut nested_from_geometries: Vec<Value> = Vec::new();
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            match key.as_ref() {
                "type" => geometry_type = Some(map.next_value()?),
                "crs" => own_crs = Some(map.next_value()?),
                "coordinates" => {
                    coordinate_members += 1;
                    members.set(DefiningMembers::COORDINATES);
                    let _ = map.next_value::<IgnoredAny>()?;
                },
                "geometries" => {
                    members.set(DefiningMembers::GEOMETRIES);
                    nested_from_geometries = map.next_value_seed(LegacyCrsArraySeed)?;
                },
                "geometry" => {
                    members.set(DefiningMembers::GEOMETRY);
                    nested_from_geometry = map.next_value_seed(LegacyCrsObjectSeed)?;
                },
                "properties" => {
                    members.set(DefiningMembers::PROPERTIES);
                    let _ = map.next_value::<IgnoredAny>()?;
                },
                "features" => {
                    members.set(DefiningMembers::FEATURES);
                    nested_from_features = map.next_value_seed(LegacyCrsArraySeed)?;
                },
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                },
            }
        }
        let geometry_type = match geometry_type {
            Some(Value::String(s)) => s,
            Some(_) => {
                return Err(de::Error::custom(
                    "invalid type: expected a string for GeoJSON type",
                ));
            },
            None => {
                return Err(de::Error::custom("GeoJSON geometry requires a type"));
            },
        };
        let legacy_crs = assemble_legacy_crs(
            &geometry_type,
            own_crs,
            nested_from_geometry,
            nested_from_features,
            nested_from_geometries,
        );
        Ok(GeoJsonProbe {
            geometry_type,
            coordinate_members,
            members,
            legacy_crs,
        })
    }
}

/// Assemble declarations in [`collect_geojson_legacy_crs`] order: own `crs`
/// first (if any), then children selected by the final `type`.
fn assemble_legacy_crs(
    geometry_type: &str,
    own_crs: Option<Value>,
    nested_from_geometry: Vec<Value>,
    nested_from_features: Vec<Value>,
    nested_from_geometries: Vec<Value>,
) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(crs) = own_crs {
        out.push(crs);
    }
    match geometry_type {
        "Feature" => out.extend(nested_from_geometry),
        "FeatureCollection" => out.extend(nested_from_features),
        "GeometryCollection" => out.extend(nested_from_geometries),
        _ => {},
    }
    out
}

/// Nested GeoJSON object: collect legacy CRS only (skip coordinate payloads).
struct LegacyCrsObjectSeed;

impl<'de> DeserializeSeed<'de> for LegacyCrsObjectSeed {
    type Value = Vec<Value>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(LegacyCrsObjectVisitor)
    }
}

struct LegacyCrsObjectVisitor;

impl<'de> Visitor<'de> for LegacyCrsObjectVisitor {
    type Value = Vec<Value>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GeoJSON object (legacy CRS walk)")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut geometry_type: Option<String> = None;
        let mut own_crs: Option<Value> = None;
        let mut nested_from_geometry = Vec::new();
        let mut nested_from_features = Vec::new();
        let mut nested_from_geometries = Vec::new();
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            match key.as_ref() {
                "type" => {
                    // Last-wins string type; non-string intermediate ignored
                    // if a later string wins (same as the top-level probe).
                    if let Value::String(s) = map.next_value::<Value>()? {
                        geometry_type = Some(s);
                    }
                },
                "crs" => own_crs = Some(map.next_value()?),
                "geometry" => nested_from_geometry = map.next_value_seed(LegacyCrsObjectSeed)?,
                "features" => nested_from_features = map.next_value_seed(LegacyCrsArraySeed)?,
                "geometries" => nested_from_geometries = map.next_value_seed(LegacyCrsArraySeed)?,
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                },
            }
        }
        Ok(assemble_legacy_crs(
            geometry_type.as_deref().unwrap_or(""),
            own_crs,
            nested_from_geometry,
            nested_from_features,
            nested_from_geometries,
        ))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }

    fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_bool<E>(self, _: bool) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }

    fn visit_i64<E>(self, _: i64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }

    fn visit_u64<E>(self, _: u64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }

    fn visit_f64<E>(self, _: f64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }

    fn visit_str<E>(self, _: &str) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Vec::new())
    }
}

/// Array of nested GeoJSON objects (features / geometries): concatenate each
/// child's CRS list in array order.
struct LegacyCrsArraySeed;

impl<'de> DeserializeSeed<'de> for LegacyCrsArraySeed {
    type Value = Vec<Value>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(LegacyCrsArrayVisitor)
    }
}

struct LegacyCrsArrayVisitor;

impl<'de> Visitor<'de> for LegacyCrsArrayVisitor {
    type Value = Vec<Value>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GeoJSON array (legacy CRS walk)")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut out = Vec::new();
        while let Some(child) = seq.next_element_seed(LegacyCrsObjectSeed)? {
            out.extend(child);
        }
        Ok(out)
    }
}

fn direct_coordinate_kind(value: &str) -> Option<IoGeometryKind> {
    match value {
        "Point" => Some(IoGeometryKind::Point),
        "MultiPoint" => Some(IoGeometryKind::MultiPoint),
        "LineString" => Some(IoGeometryKind::LineString),
        "MultiLineString" => Some(IoGeometryKind::MultiLineString),
        "Polygon" => Some(IoGeometryKind::Polygon),
        "MultiPolygon" => Some(IoGeometryKind::MultiPolygon),
        _ => None,
    }
}

struct DirectGeoJsonObject {
    shape: Shape,
}

struct DirectGeoJsonSeed {
    kind: IoGeometryKind,
    coordinate_members: usize,
}

impl<'de> DeserializeSeed<'de> for DirectGeoJsonSeed {
    type Value = DirectGeoJsonObject;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(DirectGeoJsonObjectVisitor {
            kind: self.kind,
            coordinate_members: self.coordinate_members,
        })
    }
}

struct DirectGeoJsonObjectVisitor {
    kind: IoGeometryKind,
    coordinate_members: usize,
}

impl<'de> Visitor<'de> for DirectGeoJsonObjectVisitor {
    type Value = DirectGeoJsonObject;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GeoJSON geometry object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut remaining_coordinates = self.coordinate_members;
        let mut shape = None;
        while let Some(key) = map.next_key::<Cow<'de, str>>()? {
            match key.as_ref() {
                "coordinates" => {
                    remaining_coordinates = remaining_coordinates.saturating_sub(1);
                    if remaining_coordinates == 0 {
                        shape = Some(map.next_value_seed(ShapeCoordinatesSeed(self.kind))?);
                    } else {
                        let _ = map.next_value::<IgnoredAny>()?;
                    }
                },
                // Legacy ``crs`` is reconciled by walking the raw JSON value.
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                },
            }
        }
        Ok(DirectGeoJsonObject {
            shape: shape
                .ok_or_else(|| de::Error::custom("GeoJSON geometry requires coordinates"))?,
        })
    }
}

struct CoordinateNumberSeed(&'static str);

impl<'de> DeserializeSeed<'de> for CoordinateNumberSeed {
    type Value = f64;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(CoordinateNumberVisitor(self.0))
    }
}

struct CoordinateNumberVisitor(&'static str);

impl Visitor<'_> for CoordinateNumberVisitor {
    type Value = f64;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        value
            .is_finite()
            .then_some(value)
            .ok_or_else(|| E::custom(self.0))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        i64_to_exact_f64(value).map_err(E::custom)
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        u64_to_exact_f64(value).map_err(E::custom)
    }
}

#[path = "geojson_seeds.rs"]
mod geojson_seeds;
use geojson_seeds::ShapeCoordinatesSeed;

#[path = "geojson_object.rs"]
mod geojson_object;
// Crate-visible for from_features defining-member flags + shared §7.1 check.
pub(crate) use geojson_object::{
    DefiningMembers, i64_to_exact_f64, json_number_to_f64, reject_rfc7946_cross_type_members,
    u64_to_exact_f64,
};
use geojson_object::{
    FeatureGeometry, GeoJsonObject, JsonCoordinates, defining_members_from_object,
};

fn geojson_object_to_input(object: GeoJsonObject) -> Result<GeoJsonInput> {
    reject_rfc7946_cross_type_members(
        &object.geometry_type,
        defining_members_from_object(&object),
    )?;
    match object.geometry_type.as_str() {
        "FeatureCollection" => {
            let features = object
                .features
                .ok_or_else(|| IoError::geojson("GeoJSON FeatureCollection requires features"))?;
            Ok(GeoJsonInput::FeatureCollection(
                features
                    .into_iter()
                    .map(|feature| feature.geometry)
                    .collect(),
            ))
        },
        // Top-level Feature: unwrap the geometry slot once (geometry-only decode
        // below rejects nested Feature-in-geometry).
        "Feature" => feature_geometry(object.geometry)?
            .map(GeoJsonInput::Geometry)
            .ok_or_else(|| {
                IoError::geojson(
                    "Feature has null geometry (an unlocated feature); parse the \
                     FeatureCollection with from_geojson/from_features, where null \
                     geometries become missing rows",
                )
            }),
        _ => geojson_object_to_shape(object).map(GeoJsonInput::Geometry),
    }
}

/// Geometry-slot decoder: RFC 7946 Geometry objects only — never a Feature or
/// FeatureCollection. Nested GeometryCollection members use this same rule.
fn geojson_object_to_shape(object: GeoJsonObject) -> Result<Shape> {
    reject_rfc7946_cross_type_members(
        &object.geometry_type,
        defining_members_from_object(&object),
    )?;
    let coordinates = || {
        object
            .coordinates
            .as_ref()
            .ok_or_else(|| IoError::geojson("GeoJSON geometry requires coordinates"))
    };
    match object.geometry_type.as_str() {
        "Feature" => Err(IoError::geojson(
            "GeoJSON geometry must be a Geometry object, not a Feature",
        )),
        "FeatureCollection" => Err(IoError::geojson(
            "GeoJSON FeatureCollection is a feature set, not one geometry; \
             use from_geojson (returns a GeometryArray) or from_features",
        )),
        "GeometryCollection" => Ok(Shape::GeometryCollection(
            object
                .geometries
                .ok_or_else(|| IoError::geojson("GeometryCollection requires geometries"))?
                .into_iter()
                .map(|geometry| geometry.0)
                .collect(),
        )),
        "Point" => {
            let coords = coordinates()?;
            if coords.is_empty_array() {
                Ok(Shape::empty_point())
            } else {
                Ok(Shape::Point(geojson_point(coords)?))
            }
        },
        "MultiPoint" => Ok(Shape::MultiPoint(geojson_points(coordinates()?)?)),
        "LineString" => {
            let points = geojson_points(coordinates()?)?;
            Ok(Shape::LineString(LineSeq::try_new(points)?))
        },
        "MultiLineString" => Ok(Shape::MultiLineString(
            geojson_lines(coordinates()?)?
                .into_iter()
                .map(LineSeq::try_new)
                .collect::<Result<_>>()?,
        )),
        "Polygon" => {
            let coords = coordinates()?;
            if coords.is_empty_array() {
                Ok(Shape::empty_polygon())
            } else {
                Ok(Shape::Polygon(geojson_polygon(coords)?))
            }
        },
        "MultiPolygon" => Ok(Shape::MultiPolygon(geojson_polygons(coordinates()?)?)),
        value => Err(IoError::geojson(format!(
            "unsupported GeoJSON geometry type {value:?}"
        ))),
    }
}

fn feature_geometry(geometry: FeatureGeometry) -> Result<Option<Shape>> {
    match geometry {
        FeatureGeometry::Missing => Err(IoError::geojson("GeoJSON Feature requires geometry")),
        // RFC 7946 §3.2: an unlocated feature has geometry: null — a valid
        // feature that becomes a missing row.
        FeatureGeometry::Null => Ok(None),
        FeatureGeometry::Geometry(geometry) => Ok(Some(geometry.0)),
    }
}

trait GeoJsonCoords: Sized {
    fn as_number(&self, message: &'static str) -> Result<f64>;
    fn as_array(&self, message: &'static str) -> Result<&[Self]>;
    fn is_empty_array(&self) -> bool;
}

impl GeoJsonCoords for JsonCoordinates {
    fn is_empty_array(&self) -> bool {
        matches!(self, Self::Array(values) if values.is_empty())
    }

    fn as_array(&self, message: &'static str) -> Result<&[Self]> {
        match self {
            Self::Array(values) => Ok(values),
            Self::Number(_) => Err(IoError::geojson(message)),
        }
    }

    fn as_number(&self, message: &'static str) -> Result<f64> {
        match self {
            Self::Number(value) => Ok(*value),
            Self::Array(_) => Err(IoError::geojson(message)),
        }
    }
}

impl GeoJsonCoords for Value {
    fn is_empty_array(&self) -> bool {
        matches!(self, Self::Array(values) if values.is_empty())
    }

    fn as_array(&self, message: &'static str) -> Result<&[Self]> {
        match self {
            Self::Array(values) => Ok(values.as_slice()),
            _ => Err(IoError::geojson(message)),
        }
    }

    fn as_number(&self, message: &'static str) -> Result<f64> {
        match self {
            Self::Number(number) => json_number_to_f64(number),
            _ => Err(IoError::geojson(message)),
        }
    }
}

fn parse_point<C: GeoJsonCoords>(value: &C) -> Result<Point> {
    let pair = value.as_array("coordinate must be an array")?;
    if pair.len() < 2 {
        return Err(IoError::geojson("coordinate requires x and y"));
    }
    if pair.len() > 3 {
        return Err(IoError::geojson(
            "GeoJSON coordinates support XY or XYZ only; M coordinates are not supported",
        ));
    }
    let z = pair
        .get(2)
        .map(|value| value.as_number("z coordinate must be numeric"))
        .transpose()?;
    let x = pair[0].as_number("x coordinate must be numeric")?;
    let y = pair[1].as_number("y coordinate must be numeric")?;
    // RFC 7946 §4 fixes GeoJSON coordinates to the WGS84 lon/lat domain: a
    // parsed document claiming a position outside it is malformed, not planar
    // data to trust silently.
    if let Some((label, value)) = geojson_domain_violation(x, y) {
        return Err(IoError::geojson(format!(
            "GeoJSON {label} {value} is outside the WGS84 domain (longitude \
             [-180, 180], latitude [-90, 90])"
        )));
    }
    Point::new_axes(x, y, ZOrdinate(z), MOrdinate(None))
}

/// Error text for within-sequence mixed XY/XYZ (and siblings). Shared by the
/// Value decoder and the seeded text decoder so frontends stay in lockstep.
pub(super) const fn geojson_mixed_axes_message() -> &'static str {
    "GeoJSON coordinate sequences must be dimensionally uniform \
     (every position in one sequence shares the same axes); \
     this one mixes XY and XYZ positions"
}

fn parse_points<C: GeoJsonCoords>(value: &C) -> Result<CoordSeq> {
    let positions = value.as_array("coordinates must be an array")?;
    let Some((first, rest)) = positions.split_first() else {
        return Ok(CoordSeq::empty(CoordinateAxes::XY));
    };
    let first = parse_point(first)?;
    let axes = CoordinateAxes::from_point(first);
    let mut points = CoordSeqBuilder::with_capacity(axes, positions.len());
    points.push(first);
    for position in rest {
        let point = parse_point(position)?;
        // See `geojson_mixed_axes_message`: no NaN-fill promotion — non-finite
        // coordinates are illegal and 0 would invent elevation.
        if CoordinateAxes::from_point(point) != axes {
            return Err(IoError::geojson(geojson_mixed_axes_message()));
        }
        points.push(point);
    }
    points.finish()
}

fn parse_lines<C: GeoJsonCoords>(value: &C) -> Result<Vec<CoordSeq>> {
    value
        .as_array("coordinates must be an array")?
        .iter()
        .map(parse_points)
        .collect()
}

fn parse_polygon<C: GeoJsonCoords>(value: &C) -> Result<Polygon> {
    let rings = value.as_array("Polygon coordinates must be an array")?;
    let Some(shell) = rings.first() else {
        return Err(IoError::geojson("Polygon requires a shell"));
    };
    Ok(Polygon::new(
        parse_geojson_ring(shell)?,
        rings[1..]
            .iter()
            .map(parse_geojson_ring)
            .collect::<Result<_>>()?,
    ))
}

fn parse_geojson_ring<C: GeoJsonCoords>(value: &C) -> Result<Ring> {
    let points = parse_points(value)?;
    if points.len() < Ring::MIN_VERTICES_CLOSED {
        return Err(IoError::geojson(format!(
            "Polygon ring requires at least {} coordinates",
            Ring::MIN_VERTICES_CLOSED
        )));
    }
    let first = points.point_at(0);
    let last = points.point_at(points.len() - 1);
    // RFC 7946 §3.1.6: first and last position values must be identical on
    // every active ordinate (X/Y/Z/M), not just the planar XY pair.
    if !crate::geometry::same_active_position(first, last) {
        return Err(IoError::geojson("Polygon ring must be explicitly closed"));
    }
    Ok(Ring::from_trusted_closed(points))
}

fn parse_polygons<C: GeoJsonCoords>(value: &C) -> Result<Vec<Polygon>> {
    value
        .as_array("MultiPolygon coordinates must be an array")?
        .iter()
        .map(parse_polygon)
        .collect()
}

fn geojson_point(value: &JsonCoordinates) -> Result<Point> {
    parse_point(value)
}

fn geojson_points(value: &JsonCoordinates) -> Result<CoordSeq> {
    parse_points(value)
}

fn geojson_lines(value: &JsonCoordinates) -> Result<Vec<CoordSeq>> {
    parse_lines(value)
}

fn geojson_polygon(value: &JsonCoordinates) -> Result<Polygon> {
    parse_polygon(value)
}

fn geojson_polygons(value: &JsonCoordinates) -> Result<Vec<Polygon>> {
    parse_polygons(value)
}

/// Top-level legacy ``crs`` member on a `GeoJSON` object, if present.
pub(crate) fn geojson_legacy_crs(value: &Value) -> Option<&Value> {
    value.as_object()?.get("crs")
}

/// Collect every legacy ``crs`` declaration accepted in a GeoJSON tree:
/// the object itself, Feature geometry slots, FeatureCollection members, and
/// GeometryCollection children. Callers reconcile each entry against ``crs=``.
pub(crate) fn collect_geojson_legacy_crs<'a>(value: &'a Value, out: &mut Vec<&'a Value>) {
    let Some(object) = value.as_object() else {
        return;
    };
    if let Some(crs) = geojson_legacy_crs(value) {
        out.push(crs);
    }
    match object.get("type").and_then(Value::as_str) {
        Some("Feature") => {
            if let Some(geometry) = object.get("geometry") {
                collect_geojson_legacy_crs(geometry, out);
            }
        },
        Some("FeatureCollection") => {
            if let Some(features) = object.get("features").and_then(Value::as_array) {
                for feature in features {
                    collect_geojson_legacy_crs(feature, out);
                }
            }
        },
        Some("GeometryCollection") => {
            if let Some(geometries) = object.get("geometries").and_then(Value::as_array) {
                for geometry in geometries {
                    collect_geojson_legacy_crs(geometry, out);
                }
            }
        },
        _ => {},
    }
}

/// Policy for reconciling nested legacy GeoJSON ``crs`` declarations against
/// the caller's ``crs=``.
///
/// * [`Adopt`](LegacyGeoJsonCrsPolicy::Adopt) — used by `GeometryArray` /
///   `require`: when the target is omitted, the first embedded declaration is
///   adopted and every other declaration must match it semantically; when a
///   target is set, every declaration must match that target.
/// * [`Fixed`](LegacyGeoJsonCrsPolicy::Fixed) — used by `from_geojson` /
///   `from_features`: the target (default WGS84, or explicit ``None``) is the
///   frame; every declaration must match it (or raise when target is
///   ``None`` and any declaration is present). Omitted never adopts.
#[derive(Clone, Copy, Debug)]
pub(crate) enum LegacyGeoJsonCrsPolicy<'a> {
    Adopt(Option<&'a str>),
    Fixed(Option<&'a str>),
}

/// Reconcile a collected set of legacy GeoJSON ``crs`` declarations under
/// `policy`. Matching uses
/// [`crs::same`](crate::crs::same) with
/// [`CrsComparison::IgnoreAxisOrder`](crate::crs::CrsComparison::IgnoreAxisOrder)
/// so ``OGC:CRS84`` and ``EPSG:4326`` agree. Returns the resolved canonical
/// CRS string (or ``None`` for CRS-free).
pub(crate) fn reconcile_legacy_geojson_crs(
    declarations: &[&Value],
    policy: LegacyGeoJsonCrsPolicy<'_>,
) -> Result<Option<String>> {
    match policy {
        LegacyGeoJsonCrsPolicy::Adopt(Some(target))
        | LegacyGeoJsonCrsPolicy::Fixed(Some(target)) => {
            for legacy in declarations {
                validate_legacy_geojson_crs(Some(legacy), Some(target))?;
            }
            Ok(Some(target.to_owned()))
        },
        LegacyGeoJsonCrsPolicy::Adopt(None) => {
            if declarations.is_empty() {
                return Ok(None);
            }
            let Some(first) = parse_legacy_geojson_crs(declarations[0])? else {
                return Err(IoError::geojson(
                    "legacy GeoJSON CRS member is not supported",
                ));
            };
            for legacy in declarations.iter().skip(1) {
                validate_legacy_geojson_crs(Some(legacy), Some(first.as_str()))?;
            }
            Ok(Some(first))
        },
        LegacyGeoJsonCrsPolicy::Fixed(None) => {
            if let Some(legacy) = declarations.first() {
                // Any declaration conflicts with an explicit CRS-free frame.
                validate_legacy_geojson_crs(Some(legacy), None)?;
            }
            Ok(None)
        },
    }
}

/// Parse a pre-RFC-7946 ``crs`` member to a canonical CRS identifier.
pub(crate) fn parse_legacy_geojson_crs(value: &Value) -> Result<Option<String>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if object.get("type").and_then(Value::as_str) != Some("name") {
        return Ok(None);
    }
    let Some(name) = object
        .get("properties")
        .and_then(|properties| properties.get("name"))
        .and_then(Value::as_str)
    else {
        return Ok(None);
    };
    let authority = match name {
        "urn:ogc:def:crs:OGC:1.3:CRS84" | "OGC:CRS84" => "OGC:CRS84",
        "urn:ogc:def:crs:EPSG::4326" => "EPSG:4326",
        other => other,
    };
    Ok(Some(crs::canonicalize(authority)?.to_string()))
}

/// Reconcile a legacy ``crs`` member with the caller's ``crs=`` frame.
///
/// RFC 7946 made top-level ``crs`` obsolete — coordinates are WGS84 — so a
/// legacy CRS84/EPSG:4326 declaration is ignored when it matches the default
/// frame. Anything else must agree with ``crs=`` or parsing fails loudly.
/// Matching is semantic ([`CrsComparison::IgnoreAxisOrder`]), never string
/// equality — the sole comparator for legacy GeoJSON CRS declarations.
pub(crate) fn validate_legacy_geojson_crs(
    legacy: Option<&Value>,
    target: Option<&str>,
) -> Result<()> {
    let Some(legacy) = legacy else {
        return Ok(());
    };
    let Some(target) = target else {
        return Err(IoError::geojson(
            "legacy GeoJSON CRS member conflicts with crs=None",
        ));
    };
    let Some(legacy_canonical) = parse_legacy_geojson_crs(legacy)? else {
        return Err(IoError::geojson(
            "legacy GeoJSON CRS member is not supported",
        ));
    };
    if !crs::same(
        &legacy_canonical,
        target,
        crate::crs::CrsComparison::IgnoreAxisOrder,
    )? {
        return Err(IoError::geojson(format!(
            "legacy GeoJSON CRS member ({legacy_canonical}) conflicts with crs={target}"
        )));
    }
    Ok(())
}

/// Apply RFC 7946 §7.1 exclusions for a JSON object Value (dict/bytes path).
pub(crate) fn reject_rfc7946_value_object(object: &serde_json::Map<String, Value>) -> Result<()> {
    let geometry_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| IoError::geojson("GeoJSON geometry requires a type"))?;
    let mut members = DefiningMembers::default();
    if object.contains_key("coordinates") {
        members.set(DefiningMembers::COORDINATES);
    }
    if object.contains_key("geometries") {
        members.set(DefiningMembers::GEOMETRIES);
    }
    if object.contains_key("geometry") {
        members.set(DefiningMembers::GEOMETRY);
    }
    if object.contains_key("properties") {
        members.set(DefiningMembers::PROPERTIES);
    }
    if object.contains_key("features") {
        members.set(DefiningMembers::FEATURES);
    }
    reject_rfc7946_cross_type_members(geometry_type, members)
}

/// Geometry-slot `Value` decoder: Geometry objects only (RFC 7946 §3.1).
/// A Feature or FeatureCollection in a geometry slot is always an error —
/// top-level Feature unwrap lives in document/context decoders, not here.
fn parse_geojson_inner(value: &Value) -> Result<Shape> {
    let object = value
        .as_object()
        .ok_or_else(|| IoError::geojson("GeoJSON geometry must be an object"))?;
    // §7.1 before kind dispatch so Point+properties / GC+coordinates reject.
    reject_rfc7946_value_object(object)?;
    let geometry_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| IoError::geojson("GeoJSON geometry requires a type"))?;
    if geometry_type == "Feature" {
        return Err(IoError::geojson(
            "GeoJSON geometry must be a Geometry object, not a Feature",
        ));
    }
    if geometry_type == "FeatureCollection" {
        // A FeatureCollection is a set of rows, not one geometry: it decodes
        // to a GeometryArray via `from_geojson`/`from_features`, never
        // silently collapses into a GeometryCollection.
        return Err(IoError::geojson(
            "GeoJSON FeatureCollection is a feature set, not one geometry; \
             use from_geojson (returns a GeometryArray) or from_features",
        ));
    }
    let coordinates = || {
        object
            .get("coordinates")
            .ok_or_else(|| IoError::geojson("GeoJSON geometry requires coordinates"))
    };
    match geometry_type {
        "GeometryCollection" => Ok(Shape::GeometryCollection(
            object
                .get("geometries")
                .and_then(Value::as_array)
                .ok_or_else(|| IoError::geojson("GeometryCollection requires geometries"))?
                .iter()
                .map(parse_geojson)
                .collect::<Result<_>>()?,
        )),
        "Point" => {
            let coords = coordinates()?;
            if coords.as_array().is_some_and(Vec::is_empty) {
                Ok(Shape::empty_point())
            } else {
                Ok(Shape::Point(json_point(coords)?))
            }
        },
        "MultiPoint" => Ok(Shape::MultiPoint(json_points(coordinates()?)?)),
        "LineString" => {
            let points = json_points(coordinates()?)?;
            Ok(Shape::LineString(LineSeq::try_new(points)?))
        },
        "MultiLineString" => Ok(Shape::MultiLineString(
            json_lines(coordinates()?)?
                .into_iter()
                .map(LineSeq::try_new)
                .collect::<Result<_>>()?,
        )),
        "Polygon" => {
            let coords = coordinates()?;
            if coords.as_array().is_some_and(Vec::is_empty) {
                Ok(Shape::empty_polygon())
            } else {
                Ok(Shape::Polygon(json_polygon(coords)?))
            }
        },
        "MultiPolygon" => Ok(Shape::MultiPolygon(json_polygons(coordinates()?)?)),
        value => Err(IoError::geojson(format!(
            "unsupported GeoJSON geometry type {value:?}"
        ))),
    }
}

fn json_point(value: &Value) -> Result<Point> {
    parse_point(value)
}

fn json_points(value: &Value) -> Result<CoordSeq> {
    parse_points(value)
}

fn json_lines(value: &Value) -> Result<Vec<CoordSeq>> {
    parse_lines(value)
}

fn json_polygon(value: &Value) -> Result<Polygon> {
    parse_polygon(value)
}

fn json_polygons(value: &Value) -> Result<Vec<Polygon>> {
    parse_polygons(value)
}

#[cfg(test)]
#[path = "geojson_tests.rs"]
mod conformance_tests;
