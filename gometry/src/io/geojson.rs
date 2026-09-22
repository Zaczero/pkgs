use std::borrow::Cow;
use std::fmt::Write as _;

use serde_json::Value;

use crate::boundary::metadata;
use crate::geometry::{
    CoordSeqBuilder, MOrdinate, MultiLineString, MultiMember, MultiPoint, MultiPolygon,
    TopologyDomain, ZOrdinate,
};
use crate::io::{
    CoordSeq, CoordinateAxes, Coordinates, GeometryErrorKind, GeometryKind, IoError, LineSeq,
    ParseFormat, Point, Polygon, Result, Ring, Shape, crs, parse_content, text_structure_count,
};

#[cfg(test)]
pub(crate) fn to_geojson_string_with_z(shape: &Shape, include_z: bool) -> String {
    // Writer-parity fixtures select the low-level Cartesian domain explicitly;
    // public CRS-free inputs use `geojson_domain` and therefore select WGS84.
    if include_z {
        to_geojson_string::<true>(shape, TopologyDomain::Cartesian)
    } else {
        to_geojson_string::<false>(shape, TopologyDomain::Cartesian)
    }
    .expect("test geometry serializes to GeoJSON")
}

/// Serialize a geometry to an RFC 7946 GeoJSON string.
///
/// GeoJSON coordinates are WGS84 longitude/latitude by specification
/// (RFC 7946 §4). When the domain is geographic,
/// the writer enforces the standard's geographic rules: positions are
/// validated against the lon/lat domain (§4), antimeridian-crossing parts are
/// cut into seam-following multiparts (§3.1.9), and polygon rings are emitted
/// with right-hand winding — exterior counterclockwise, holes clockwise
/// (§3.1.6). Public CRS-free geometries explicitly assume WGS84 coordinates
/// and select this geographic domain; callers with projected or other planar
/// semantics must transform or choose another format.
pub(crate) fn to_geojson_string<const INCLUDE_Z: bool>(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<String> {
    validate_geojson_value::<INCLUDE_Z>(shape)?;
    crate::io::require_serializable_axes(shape)?;
    let prepared = geojson_output_shape(shape, domain)?;
    // Three zmij-formatted ordinates and their delimiters fit within 80 bytes.
    let coordinate_bytes = prepared.coord_count().wrapping_mul(80);
    let structural_bytes = text_structure_count(&prepared).wrapping_mul(128);
    let capacity = coordinate_bytes.wrapping_add(structural_bytes);
    let mut out = String::with_capacity(capacity);
    write_geojson_to_impl::<INCLUDE_Z>(&mut out, &prepared);
    Ok(out)
}

/// Select the RFC 7946 domain for a public GeoJSON writer.
///
/// GeoJSON has no CRS member in its current specification. An absent CRS is
/// therefore an explicit WGS84-coordinate assumption, not permission to emit
/// arbitrary Cartesian coordinates. Tagged non-WGS84 values retain their
/// existing low-level domain selection; the Python boundary rejects them
/// before serialization.
pub(crate) fn geojson_domain(crs: Option<&str>) -> Result<TopologyDomain> {
    Ok(match crs {
        None => TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
        Some(crs) if crate::crs::is_wgs84_lonlat(crs) => {
            TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84())
        },
        Some(crs) => crate::crs::info(crs)?
            .geographic_frame()
            .map_or(TopologyDomain::Cartesian, TopologyDomain::Geographic),
    })
}

pub(crate) fn validate_geojson_value<const INCLUDE_Z: bool>(shape: &Shape) -> Result<()> {
    match shape {
        Shape::Empty(_, axes) if *axes != CoordinateAxes::XY => {
            Err(GeometryErrorKind::OrdinateDropped("GeoJSON empty geometry".into()).into())
        },
        shape if shape.has_m() => {
            Err(GeometryErrorKind::OrdinateDropped("GeoJSON M ordinate".into()).into())
        },
        shape if !INCLUDE_Z && shape.has_z() => {
            Err(GeometryErrorKind::OrdinateDropped("GeoJSON Z ordinate".into()).into())
        },
        Shape::MultiPoint(points) => {
            if points.has_empty_members() {
                return Err(GeometryErrorKind::OrdinateDropped(
                    "GeoJSON empty MultiPoint member".into(),
                )
                .into());
            }
            Ok(())
        },
        Shape::MultiLineString(lines) => {
            if lines.has_empty_members() {
                return Err(GeometryErrorKind::OrdinateDropped(
                    "GeoJSON empty MultiLineString member".into(),
                )
                .into());
            }
            Ok(())
        },
        Shape::MultiPolygon(polygons) => {
            if polygons.has_empty_members() {
                return Err(GeometryErrorKind::OrdinateDropped(
                    "GeoJSON empty MultiPolygon member".into(),
                )
                .into());
            }
            Ok(())
        },
        Shape::GeometryCollection(geometries) => geometries
            .iter()
            .try_for_each(validate_geojson_value::<INCLUDE_Z>),
        _ => Ok(()),
    }
}
/// The serialization-only view of `shape` normalized for RFC 7946 output.
///
/// Never mutates the source: a geographic geometry is split at the
/// antimeridian (a no-op that returns an owned clone for non-crossing input),
/// and any polygonal geometry is oriented to right-hand winding. Non-polygonal
/// planar geometry borrows through unchanged.
pub(crate) fn geojson_output_shape(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<Cow<'_, Shape>> {
    let prepared: Cow<'_, Shape> = match domain {
        TopologyDomain::Cartesian => Cow::Borrowed(shape),
        TopologyDomain::Geographic(frame) => {
            validate_geojson_domain(shape)?;
            Cow::Owned(shape.split_antimeridian_in_frame(frame)?)
        },
    };
    // RFC 7946 §3.1.6 mandates right-hand ring winding on output; only
    // polygonal geometry carries rings, so everything else borrows through
    // without a copy. `orient_polygons` reverses a ring only when its winding
    // is wrong, so a geometry already in canonical form is not perturbed.
    if matches!(
        prepared.as_ref(),
        Shape::Polygon(_) | Shape::MultiPolygon(_) | Shape::GeometryCollection(_)
    ) {
        Ok(Cow::Owned(prepared.as_ref().orient_polygons(false)?))
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
        Shape::Empty(GeometryKind::GeometryCollection, _) => {
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
    // The shortest formatter canonicalizes both zero spellings to ``0``, which
    // drops the sign bit. Spell it with a fraction: readers that infer number
    // types parse bare ``-0`` as an integer zero and lose the sign again.
    if value.to_bits() == (-0.0_f64).to_bits() {
        out.push_str("-0.0");
        return;
    }
    let mut buffer = zmij::Buffer::new();
    out.push_str(buffer.format_finite(value));
}

/// Content violations discovered while deserializing (non-finite ordinates,
/// short lines) surface as parse errors: the caller's failure domain is "this
/// serialized input is bad", with the structural rule as the detail.
#[cfg(test)]
pub(crate) fn parse_geojson(value: &Value) -> Result<Shape> {
    parse_geojson_at(value, "$")
}

/// Parse one geometry while retaining its JSON structural path. All Python
/// carriers normalize to `Value` before calling this owner, so text, mappings,
/// and `__geo_interface__` report the same location for the same malformed
/// geometry.
pub(crate) fn parse_geojson_at(value: &Value, path: &str) -> Result<Shape> {
    let path = JsonPath::Root(path);
    parse_geojson_inner(value, &path)
}

#[path = "geojson_object.rs"]
mod geojson_object;
// Crate-visible for from_features defining-member flags + shared §7.1 check.
pub(crate) use geojson_object::{
    DefiningMembers, json_number_to_f64, reject_rfc7946_cross_type_members,
};

fn json_number(value: &Value, message: &'static str) -> Result<f64> {
    match value {
        Value::Number(number) => json_number_to_f64(number),
        _ => Err(IoError::geojson(message)),
    }
}

fn json_is_empty_array(value: &Value) -> bool {
    value.as_array().is_some_and(Vec::is_empty)
}

fn parse_point(value: &Value, path: &JsonPath<'_>) -> Result<Point> {
    let pair = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("coordinate must be an array"))
        .map_err(|error| error.with_parse_path(&path.render()))?;
    let Some([raw_x, raw_y]) = pair.first_chunk::<2>() else {
        return Err(IoError::geojson("coordinate requires x and y").with_parse_path(&path.render()));
    };
    if pair.len() > 3 {
        return Err(IoError::geojson(
            "GeoJSON coordinates support XY or XYZ only; M coordinates are not supported",
        )
        .with_parse_path(&path.render()));
    }
    let z = pair
        .get(2)
        .map(|value| {
            json_number(value, "z coordinate must be numeric")
                .map_err(|error| error.with_parse_path(&path.index(2).render()))
        })
        .transpose()?;
    let x = json_number(raw_x, "x coordinate must be numeric")
        .map_err(|error| error.with_parse_path(&path.index(0).render()))?;
    let y = json_number(raw_y, "y coordinate must be numeric")
        .map_err(|error| error.with_parse_path(&path.index(1).render()))?;
    // RFC 7946 §4 fixes GeoJSON coordinates to the WGS84 lon/lat domain: a
    // parsed document claiming a position outside it is malformed, not planar
    // data to trust silently.
    if let Some((label, value)) = geojson_domain_violation(x, y) {
        return Err(IoError::geojson(format!(
            "GeoJSON {label} {value} is outside the WGS84 domain (longitude \
             [-180, 180], latitude [-90, 90])"
        ))
        .with_parse_path(&path.render()));
    }
    Point::new_axes(x, y, ZOrdinate(z), MOrdinate(None))
        .map_err(|error| error.with_parse_path(&path.render()))
}

/// Error text for within-sequence mixed XY/XYZ (and siblings). Shared by the
/// Value decoder so every GeoJSON frontend stays in lockstep.
pub(super) const fn geojson_mixed_axes_message() -> &'static str {
    "GeoJSON coordinate sequences must be dimensionally uniform \
     (every position in one sequence shares the same axes); \
     this one mixes XY and XYZ positions"
}

fn parse_points(value: &Value, path: &JsonPath<'_>) -> Result<CoordSeq> {
    let positions = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("coordinates must be an array"))
        .map_err(|error| error.with_parse_path(&path.render()))?;
    let Some((first, rest)) = positions.split_first() else {
        return Ok(CoordSeq::empty(CoordinateAxes::XY));
    };
    let first = parse_point(first, &path.index(0))?;
    let axes = CoordinateAxes::from_point(first);
    let mut points = CoordSeqBuilder::with_capacity(axes, positions.len());
    points.push(first);
    for (index, position) in rest.iter().enumerate() {
        let position_path = path.index(index.wrapping_add(1));
        let point = parse_point(position, &position_path)?;
        // See `geojson_mixed_axes_message`: no NaN-fill promotion — non-finite
        // coordinates are illegal and 0 would invent elevation.
        if CoordinateAxes::from_point(point) != axes {
            return Err(IoError::geojson(geojson_mixed_axes_message())
                .with_parse_path(&position_path.render()));
        }
        points.push(point);
    }
    points.finish()
}

fn parse_polygon(value: &Value, path: &JsonPath<'_>) -> Result<Polygon> {
    let rings = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("Polygon coordinates must be an array"))
        .map_err(|error| error.with_parse_path(&path.render()))?;
    let Some((shell, remaining)) = rings.split_first() else {
        return Err(IoError::geojson("Polygon requires a shell").with_parse_path(&path.render()));
    };
    let shell = parse_geojson_ring(shell, &path.index(0))?;
    let mut holes = Vec::with_capacity(remaining.len());
    for (index, ring) in remaining.iter().enumerate() {
        holes.push(parse_geojson_ring(
            ring,
            &path.index(index.wrapping_add(1)),
        )?);
    }
    let polygon = Polygon::new(shell, holes);
    crate::io::polygon_output_axes_with(&polygon, |index, detail| {
        IoError::geojson(detail).with_parse_path(&path.index(index).render())
    })?;
    Ok(polygon)
}

fn parse_geojson_ring(value: &Value, path: &JsonPath<'_>) -> Result<Ring> {
    let points = parse_points(value, path)?;
    let Some(ring) = Ring::from_closed(points) else {
        return Err(IoError::geojson(format!(
            "Polygon ring requires at least {} coordinates",
            Ring::MIN_VERTICES_CLOSED
        ))
        .with_parse_path(&path.render()));
    };
    let coords = ring.coords();
    let first = coords.point_at(0);
    let last = coords.point_at(coords.len().wrapping_sub(1));
    // RFC 7946 §3.1.6: first and last position values must be identical on
    // every active ordinate (X/Y/Z/M), not just the planar XY pair.
    if !crate::geometry::same_active_position(first, last) {
        return Err(IoError::geojson("Polygon ring must be explicitly closed")
            .with_parse_path(&path.render()));
    }
    Ok(ring)
}

fn parse_geojson_multipoint(value: &Value, path: &JsonPath<'_>) -> Result<MultiPoint> {
    let positions = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("MultiPoint coordinates must be an array"))
        .map_err(|error| error.with_parse_path(&path.render()))?;
    let mut axes = None;
    let mut members = Vec::with_capacity(positions.len());
    for (index, position) in positions.iter().enumerate() {
        if json_is_empty_array(position) {
            members.push(MultiMember::Empty);
            continue;
        }
        let point = parse_point(position, &path.index(index))?;
        let point_axes = CoordinateAxes::from_point(point);
        if *axes.get_or_insert(point_axes) != point_axes {
            return Err(IoError::geojson(geojson_mixed_axes_message())
                .with_parse_path(&path.index(index).render()));
        }
        members.push(MultiMember::Value(point));
    }
    MultiPoint::try_new(axes.unwrap_or(CoordinateAxes::XY), members)
}

fn parse_geojson_multilinestring(value: &Value, path: &JsonPath<'_>) -> Result<MultiLineString> {
    let lines = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("MultiLineString coordinates must be an array"))
        .map_err(|error| error.with_parse_path(&path.render()))?;
    let mut axes = None;
    let mut members = Vec::with_capacity(lines.len());
    for (index, line) in lines.iter().enumerate() {
        let line_path = path.index(index);
        let points = parse_points(line, &line_path)?;
        if points.is_empty() {
            members.push(MultiMember::Empty);
            continue;
        }
        let line_axes = points.axes();
        if *axes.get_or_insert(line_axes) != line_axes {
            return Err(
                IoError::geojson(geojson_mixed_axes_message()).with_parse_path(&line_path.render())
            );
        }
        members.push(MultiMember::Value(
            LineSeq::try_new(points).map_err(|error| error.with_parse_path(&line_path.render()))?,
        ));
    }
    MultiLineString::try_new(axes.unwrap_or(CoordinateAxes::XY), members)
}

fn parse_geojson_multipolygon(value: &Value, path: &JsonPath<'_>) -> Result<MultiPolygon> {
    let polygons = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("MultiPolygon coordinates must be an array"))
        .map_err(|error| error.with_parse_path(&path.render()))?;
    let mut axes = None;
    let mut members = Vec::with_capacity(polygons.len());
    for (index, polygon) in polygons.iter().enumerate() {
        let polygon_path = path.index(index);
        if json_is_empty_array(polygon) {
            members.push(MultiMember::Empty);
            continue;
        }
        let polygon = parse_polygon(polygon, &polygon_path)?;
        let polygon_axes = polygon.shell.coords().axes();
        if *axes.get_or_insert(polygon_axes) != polygon_axes {
            return Err(IoError::geojson(geojson_mixed_axes_message())
                .with_parse_path(&polygon_path.render()));
        }
        members.push(MultiMember::Value(polygon));
    }
    MultiPolygon::try_new(axes.unwrap_or(CoordinateAxes::XY), members)
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

pub(crate) struct GeoJsonCrs<'a> {
    policy: LegacyGeoJsonCrsPolicy<'a>,
    adopted: Option<String>,
}

impl<'a> GeoJsonCrs<'a> {
    pub(crate) const fn new(policy: LegacyGeoJsonCrsPolicy<'a>) -> Self {
        Self {
            policy,
            adopted: None,
        }
    }

    pub(crate) fn reconcile_declaration(&mut self, legacy: &Value) -> Result<()> {
        match self.policy {
            LegacyGeoJsonCrsPolicy::Fixed(target) => {
                validate_legacy_geojson_crs(Some(legacy), target)
            },
            LegacyGeoJsonCrsPolicy::Adopt(target) => {
                if let Some(target) = target.or(self.adopted.as_deref()) {
                    validate_legacy_geojson_crs(Some(legacy), Some(target))
                } else {
                    self.adopted = Some(parse_legacy_geojson_crs(legacy)?.ok_or_else(|| {
                        IoError::geojson("legacy GeoJSON CRS member is not supported")
                    })?);
                    Ok(())
                }
            },
        }
    }

    pub(crate) fn reconcile_tree(&mut self, value: &Value) -> Result<()> {
        let Some(object) = value.as_object() else {
            return Ok(());
        };
        if let Some(legacy) = object.get("crs") {
            self.reconcile_declaration(legacy)?;
        }
        match object.get("type").and_then(Value::as_str) {
            Some("Feature") => {
                if let Some(geometry) = object.get("geometry") {
                    self.reconcile_tree(geometry)?;
                }
            },
            Some(kind @ ("FeatureCollection" | "GeometryCollection")) => {
                let key = if kind == "FeatureCollection" {
                    "features"
                } else {
                    "geometries"
                };
                if let Some(children) = object.get(key).and_then(Value::as_array) {
                    for child in children {
                        self.reconcile_tree(child)?;
                    }
                }
            },
            _ => {},
        }
        Ok(())
    }

    pub(crate) fn into_crs(self) -> Option<String> {
        match self.policy {
            LegacyGeoJsonCrsPolicy::Adopt(target) => target.map(str::to_owned).or(self.adopted),
            LegacyGeoJsonCrsPolicy::Fixed(target) => target.map(str::to_owned),
        }
    }
}

/// Parse a pre-RFC-7946 ``crs`` member to a normalized CRS definition.
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
    Ok(Some(crs::definition(authority)?.to_string()))
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
    let Some(legacy_definition) = parse_legacy_geojson_crs(legacy)? else {
        return Err(IoError::geojson(
            "legacy GeoJSON CRS member is not supported",
        ));
    };
    if !metadata::crs_labels_operationally_equal(&legacy_definition, target)? {
        return Err(IoError::geojson(format!(
            "legacy GeoJSON CRS member ({legacy_definition}) conflicts with crs={target}"
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

/// A geometry member's position. Successful parses retain only this borrowed
/// stack; the user-facing string is materialized when an error needs it.
enum JsonPath<'a> {
    Root(&'a str),
    Key { parent: &'a Self, key: &'static str },
    Index { parent: &'a Self, index: usize },
}

impl JsonPath<'_> {
    const fn key(&self, key: &'static str) -> JsonPath<'_> {
        JsonPath::Key { parent: self, key }
    }

    const fn index(&self, index: usize) -> JsonPath<'_> {
        JsonPath::Index {
            parent: self,
            index,
        }
    }

    fn render(&self) -> String {
        let mut rendered = String::with_capacity(self.rendered_len());
        self.write_to(&mut rendered);
        rendered
    }

    fn rendered_len(&self) -> usize {
        match self {
            Self::Root(path) => path.len(),
            Self::Key { parent, key } => parent
                .rendered_len()
                .wrapping_add(1)
                .wrapping_add(key.len()),
            Self::Index { parent, index } => parent
                .rendered_len()
                .wrapping_add(3)
                .wrapping_add(index.checked_ilog10().unwrap_or(0) as usize),
        }
    }

    fn write_to(&self, rendered: &mut String) {
        match self {
            Self::Root(path) => rendered.push_str(path),
            Self::Key { parent, key } => {
                parent.write_to(rendered);
                rendered.push('.');
                rendered.push_str(key);
            },
            Self::Index { parent, index } => {
                parent.write_to(rendered);
                write!(rendered, "[{index}]").expect("writing to String cannot fail");
            },
        }
    }
}

/// Geometry-slot `Value` decoder: Geometry objects only (RFC 7946 §3.1).
/// A Feature or FeatureCollection in a geometry slot is always an error —
/// top-level Feature unwrap lives in document/context decoders, not here.
fn parse_geojson_inner(value: &Value, path: &JsonPath<'_>) -> Result<Shape> {
    parse_geojson_inner_impl(value, path, 0)
        .map_err(|error| parse_content(ParseFormat::GeoJson, error))
        .map_err(|error| error.with_parse_path(&path.render()))
}

fn parse_geojson_inner_impl(value: &Value, path: &JsonPath<'_>, depth: usize) -> Result<Shape> {
    if depth > crate::io::MAX_NESTING {
        return Err(IoError::geojson(format!(
            "GeoJSON geometry nesting exceeds the limit of {}",
            crate::io::MAX_NESTING
        )));
    }
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
    let coordinates_path = path.key("coordinates");
    let coordinates = || {
        object.get("coordinates").ok_or_else(|| {
            IoError::geojson("GeoJSON geometry requires coordinates")
                .with_parse_path(&coordinates_path.render())
        })
    };
    match geometry_type {
        "GeometryCollection" => {
            let geometries_path = path.key("geometries");
            let geometries = object
                .get("geometries")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    IoError::geojson("GeometryCollection requires geometries")
                        .with_parse_path(&geometries_path.render())
                })?;
            let mut parsed = Vec::with_capacity(geometries.len());
            for (index, geometry) in geometries.iter().enumerate() {
                parsed.push(parse_geojson_inner_impl(
                    geometry,
                    &geometries_path.index(index),
                    depth.wrapping_add(1),
                )?);
            }
            Ok(Shape::geometry_collection(parsed)?)
        },
        "Point" => {
            let coords = coordinates()?;
            if coords.as_array().is_some_and(Vec::is_empty) {
                Ok(Shape::empty_point())
            } else {
                Ok(Shape::Point(parse_point(coords, &coordinates_path)?))
            }
        },
        "MultiPoint" => {
            let coords = coordinates()?;
            if coords.as_array().is_some_and(Vec::is_empty) {
                Ok(Shape::typed_empty(
                    GeometryKind::MultiPoint,
                    CoordinateAxes::XY,
                ))
            } else {
                Ok(Shape::MultiPoint(parse_geojson_multipoint(
                    coords,
                    &coordinates_path,
                )?))
            }
        },
        "LineString" => {
            let points = parse_points(coordinates()?, &coordinates_path)?;
            if points.is_empty() {
                Ok(Shape::typed_empty(GeometryKind::LineString, points.axes()))
            } else {
                Ok(Shape::LineString(LineSeq::try_new(points).map_err(
                    |error| error.with_parse_path(&coordinates_path.render()),
                )?))
            }
        },
        "MultiLineString" => {
            let coords = coordinates()?;
            if coords.as_array().is_some_and(Vec::is_empty) {
                Ok(Shape::typed_empty(
                    GeometryKind::MultiLineString,
                    CoordinateAxes::XY,
                ))
            } else {
                Ok(Shape::MultiLineString(
                    parse_geojson_multilinestring(coords, &coordinates_path)
                        .map_err(|error| error.with_parse_path(&coordinates_path.render()))?,
                ))
            }
        },
        "Polygon" => {
            let coords = coordinates()?;
            if coords.as_array().is_some_and(Vec::is_empty) {
                Ok(Shape::empty_polygon())
            } else {
                Ok(Shape::Polygon(parse_polygon(coords, &coordinates_path)?))
            }
        },
        "MultiPolygon" => {
            let coords = coordinates()?;
            if coords.as_array().is_some_and(Vec::is_empty) {
                Ok(Shape::typed_empty(
                    GeometryKind::MultiPolygon,
                    CoordinateAxes::XY,
                ))
            } else {
                Ok(Shape::MultiPolygon(
                    parse_geojson_multipolygon(coords, &coordinates_path)
                        .map_err(|error| error.with_parse_path(&coordinates_path.render()))?,
                ))
            }
        },
        value => Err(IoError::geojson(format!(
            "unsupported GeoJSON geometry type {value:?}"
        ))),
    }
}

#[cfg(test)]
#[path = "geojson_tests.rs"]
mod conformance_tests;
