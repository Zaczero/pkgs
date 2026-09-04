use crate::boundary::metadata;
use std::borrow::Cow;

use serde_json::Value;

use crate::geometry::{
    CoordSeqBuilder, MOrdinate, MultiLineString, MultiMember, MultiPoint, MultiPolygon,
    TopologyDomain, ZOrdinate,
};
use crate::io::{
    CoordSeq, CoordinateAxes, Coordinates, GeometryErrorKind, GeometryKind, IoError, LineSeq,
    ParseFormat, Point, Polygon, Result, Ring, Shape, crs, parse_content,
};

#[cfg(test)]
pub(crate) fn to_geojson_string_with_z(shape: &Shape, include_z: bool) -> String {
    // The writer-parity fixtures are CRS-free planar shapes: only ring winding
    // is canonicalized (no geographic domain check or antimeridian cut).
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
/// (RFC 7946 §4). When the domain is geographic (the geometry carries a WGS84
/// frame),
/// the writer enforces the standard's geographic rules: positions are
/// validated against the lon/lat domain (§4), antimeridian-crossing parts are
/// cut into seam-following multiparts (§3.1.9), and polygon rings are emitted
/// with right-hand winding — exterior counterclockwise, holes clockwise
/// (§3.1.6). A CRS-free geometry is untagged/planar: only the ring-winding
/// representation is canonicalized, since domain and antimeridian are
/// geographic semantics that do not apply to planar coordinates.
pub(crate) fn to_geojson_string<const INCLUDE_Z: bool>(
    shape: &Shape,
    domain: TopologyDomain,
) -> Result<String> {
    validate_geojson_value::<INCLUDE_Z>(shape)?;
    let prepared = geojson_output_shape(shape, domain)?;
    let coordinate_bytes = prepared
        .coord_count()
        .checked_mul(4096)
        .ok_or_else(|| IoError::geojson("GeoJSON coordinate byte length overflows usize"))?;
    let structural_bytes = geojson_structure_count(&prepared)?
        .checked_mul(128)
        .ok_or_else(|| IoError::geojson("GeoJSON structural byte length overflows usize"))?;
    let capacity = coordinate_bytes
        .checked_add(structural_bytes)
        .ok_or_else(|| IoError::geojson("GeoJSON output length overflows usize"))?;
    let mut out = String::new();
    out.try_reserve(capacity)
        .map_err(|_| crate::error::ResourceError::Allocation)?;
    write_geojson_to_impl::<INCLUDE_Z>(&mut out, &prepared);
    Ok(out)
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
fn geojson_structure_count(shape: &Shape) -> Result<usize> {
    match shape {
        Shape::MultiLineString(lines) => lines
            .len()
            .checked_add(1)
            .ok_or_else(|| IoError::geojson("GeoJSON structural count overflows usize")),
        Shape::Polygon(polygon) => polygon
            .holes
            .len()
            .checked_add(1)
            .ok_or_else(|| IoError::geojson("GeoJSON structural count overflows usize")),
        Shape::MultiPolygon(polygons) => polygons.iter().try_fold(1_usize, |total, polygon| {
            let polygon_count = polygon
                .holes
                .len()
                .checked_add(1)
                .ok_or_else(|| IoError::geojson("GeoJSON structural count overflows usize"))?;
            total
                .checked_add(polygon_count)
                .ok_or_else(|| IoError::geojson("GeoJSON structural count overflows usize"))
        }),
        Shape::GeometryCollection(geometries) => {
            geometries.iter().try_fold(1_usize, |total, geometry| {
                let geometry_count = geojson_structure_count(geometry)?;
                total
                    .checked_add(geometry_count)
                    .ok_or_else(|| IoError::geojson("GeoJSON structural count overflows usize"))
            })
        },
        _ => Ok(1),
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
    let path = JsonPath::from(path);
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

fn parse_point(value: &Value, path: &JsonPath) -> Result<Point> {
    let pair = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("coordinate must be an array"))
        .map_err(|error| error.with_parse_path(path.as_str()))?;
    if pair.len() < 2 {
        return Err(IoError::geojson("coordinate requires x and y").with_parse_path(path.as_str()));
    }
    if pair.len() > 3 {
        return Err(IoError::geojson(
            "GeoJSON coordinates support XY or XYZ only; M coordinates are not supported",
        )
        .with_parse_path(path.as_str()));
    }
    let z = pair
        .get(2)
        .map(|value| {
            json_number(value, "z coordinate must be numeric")
                .map_err(|error| error.with_parse_path(&path.index(2).0))
        })
        .transpose()?;
    let x = json_number(&pair[0], "x coordinate must be numeric")
        .map_err(|error| error.with_parse_path(&path.index(0).0))?;
    let y = json_number(&pair[1], "y coordinate must be numeric")
        .map_err(|error| error.with_parse_path(&path.index(1).0))?;
    // RFC 7946 §4 fixes GeoJSON coordinates to the WGS84 lon/lat domain: a
    // parsed document claiming a position outside it is malformed, not planar
    // data to trust silently.
    if let Some((label, value)) = geojson_domain_violation(x, y) {
        return Err(IoError::geojson(format!(
            "GeoJSON {label} {value} is outside the WGS84 domain (longitude \
             [-180, 180], latitude [-90, 90])"
        ))
        .with_parse_path(path.as_str()));
    }
    Point::new_axes(x, y, ZOrdinate(z), MOrdinate(None))
        .map_err(|error| error.with_parse_path(path.as_str()))
}

/// Error text for within-sequence mixed XY/XYZ (and siblings). Shared by the
/// Value decoder so every GeoJSON frontend stays in lockstep.
pub(super) const fn geojson_mixed_axes_message() -> &'static str {
    "GeoJSON coordinate sequences must be dimensionally uniform \
     (every position in one sequence shares the same axes); \
     this one mixes XY and XYZ positions"
}

fn parse_points(value: &Value, path: &JsonPath) -> Result<CoordSeq> {
    let positions = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("coordinates must be an array"))
        .map_err(|error| error.with_parse_path(path.as_str()))?;
    let Some((first, rest)) = positions.split_first() else {
        return Ok(CoordSeq::empty(CoordinateAxes::XY));
    };
    let first = parse_point(first, &path.index(0))?;
    let axes = CoordinateAxes::from_point(first);
    let mut points = CoordSeqBuilder::try_with_capacity(axes, positions.len())?;
    points.push(first);
    for (index, position) in rest.iter().enumerate() {
        let point = parse_point(position, &path.index(index + 1))?;
        // See `geojson_mixed_axes_message`: no NaN-fill promotion — non-finite
        // coordinates are illegal and 0 would invent elevation.
        if CoordinateAxes::from_point(point) != axes {
            return Err(IoError::geojson(geojson_mixed_axes_message())
                .with_parse_path(&path.index(index + 1).0));
        }
        points.push(point);
    }
    points.finish()
}

fn parse_polygon(value: &Value, path: &JsonPath) -> Result<Polygon> {
    let rings = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("Polygon coordinates must be an array"))
        .map_err(|error| error.with_parse_path(path.as_str()))?;
    let Some(shell) = rings.first() else {
        return Err(IoError::geojson("Polygon requires a shell").with_parse_path(path.as_str()));
    };
    let shell = parse_geojson_ring(shell, &path.index(0))?;
    let mut holes = Vec::new();
    crate::boundary::try_reserve_vec(&mut holes, rings.len().saturating_sub(1))?;
    for (index, ring) in rings.iter().enumerate().skip(1) {
        crate::boundary::try_push_vec(&mut holes, parse_geojson_ring(ring, &path.index(index))?)?;
    }
    Ok(Polygon::new(shell, holes))
}

fn parse_geojson_ring(value: &Value, path: &JsonPath) -> Result<Ring> {
    let points = parse_points(value, path)?;
    if points.len() < Ring::MIN_VERTICES_CLOSED {
        return Err(IoError::geojson(format!(
            "Polygon ring requires at least {} coordinates",
            Ring::MIN_VERTICES_CLOSED
        ))
        .with_parse_path(path.as_str()));
    }
    let first = points.point_at(0);
    let last = points.point_at(points.len() - 1);
    // RFC 7946 §3.1.6: first and last position values must be identical on
    // every active ordinate (X/Y/Z/M), not just the planar XY pair.
    if !crate::geometry::same_active_position(first, last) {
        return Err(IoError::geojson("Polygon ring must be explicitly closed")
            .with_parse_path(path.as_str()));
    }
    Ok(Ring::from_closed(points))
}

fn parse_geojson_multipoint(value: &Value, path: &JsonPath) -> Result<MultiPoint> {
    let positions = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("MultiPoint coordinates must be an array"))
        .map_err(|error| error.with_parse_path(path.as_str()))?;
    let mut axes = None;
    let mut members = Vec::new();
    crate::boundary::try_reserve_vec(&mut members, positions.len())?;
    for (index, position) in positions.iter().enumerate() {
        if json_is_empty_array(position) {
            crate::boundary::try_push_vec(&mut members, MultiMember::Empty)?;
            continue;
        }
        let point = parse_point(position, &path.index(index))?;
        let point_axes = CoordinateAxes::from_point(point);
        match axes {
            None => axes = Some(point_axes),
            Some(expected) if expected != point_axes => {
                return Err(IoError::geojson(geojson_mixed_axes_message())
                    .with_parse_path(&path.index(index).0));
            },
            Some(_) => {},
        }
        crate::boundary::try_push_vec(&mut members, MultiMember::Value(point))?;
    }
    MultiPoint::try_new(axes.unwrap_or(CoordinateAxes::XY), members)
}

fn parse_geojson_multilinestring(value: &Value, path: &JsonPath) -> Result<MultiLineString> {
    let lines = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("MultiLineString coordinates must be an array"))
        .map_err(|error| error.with_parse_path(path.as_str()))?;
    let mut axes = None;
    let mut members = Vec::new();
    crate::boundary::try_reserve_vec(&mut members, lines.len())?;
    for (index, line) in lines.iter().enumerate() {
        let line_path = path.index(index);
        let points = parse_points(line, &line_path)?;
        if points.is_empty() {
            crate::boundary::try_push_vec(&mut members, MultiMember::Empty)?;
            continue;
        }
        let line_axes = points.axes();
        match axes {
            None => axes = Some(line_axes),
            Some(expected) if expected != line_axes => {
                return Err(IoError::geojson(geojson_mixed_axes_message())
                    .with_parse_path(line_path.as_str()));
            },
            Some(_) => {},
        }
        crate::boundary::try_push_vec(
            &mut members,
            MultiMember::Value(
                LineSeq::try_new(points)
                    .map_err(|error| error.with_parse_path(line_path.as_str()))?,
            ),
        )?;
    }
    MultiLineString::try_new(axes.unwrap_or(CoordinateAxes::XY), members)
}

fn parse_geojson_multipolygon(value: &Value, path: &JsonPath) -> Result<MultiPolygon> {
    let polygons = value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| IoError::geojson("MultiPolygon coordinates must be an array"))
        .map_err(|error| error.with_parse_path(path.as_str()))?;
    let mut axes = None;
    let mut members = Vec::new();
    crate::boundary::try_reserve_vec(&mut members, polygons.len())?;
    for (index, polygon) in polygons.iter().enumerate() {
        let polygon_path = path.index(index);
        if json_is_empty_array(polygon) {
            crate::boundary::try_push_vec(&mut members, MultiMember::Empty)?;
            continue;
        }
        let polygon = parse_polygon(polygon, &polygon_path)?;
        let polygon_axes = polygon.shell.coords().axes();
        match axes {
            None => axes = Some(polygon_axes),
            Some(expected) if expected != polygon_axes => {
                return Err(IoError::geojson(geojson_mixed_axes_message())
                    .with_parse_path(polygon_path.as_str()));
            },
            Some(_) => {},
        }
        crate::boundary::try_push_vec(&mut members, MultiMember::Value(polygon))?;
    }
    MultiPolygon::try_new(axes.unwrap_or(CoordinateAxes::XY), members)
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
    if !metadata::crs_labels_operationally_equal(&legacy_canonical, target)? {
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
#[derive(Clone)]
struct JsonPath(String);

impl JsonPath {
    fn from(path: &str) -> Self {
        Self(path.to_owned())
    }

    fn key(&self, key: &str) -> Self {
        Self(format!("{}.{}", self.0, key))
    }

    fn index(&self, index: usize) -> Self {
        Self(format!("{}[{}]", self.0, index))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

fn parse_geojson_inner(value: &Value, path: &JsonPath) -> Result<Shape> {
    parse_geojson_inner_impl(value, path, 0)
        .map_err(|error| parse_content(ParseFormat::GeoJson, error))
        .map_err(|error| error.with_parse_path(path.as_str()))
}

fn parse_geojson_inner_impl(value: &Value, path: &JsonPath, depth: usize) -> Result<Shape> {
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
                .with_parse_path(coordinates_path.as_str())
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
                        .with_parse_path(geometries_path.as_str())
                })?;
            let mut parsed = Vec::new();
            crate::boundary::try_reserve_vec(&mut parsed, geometries.len())?;
            for (index, geometry) in geometries.iter().enumerate() {
                crate::boundary::try_push_vec(
                    &mut parsed,
                    parse_geojson_inner_impl(
                        geometry,
                        &geometries_path.index(index),
                        depth.saturating_add(1),
                    )?,
                )?;
            }
            Ok(Shape::geometry_collection(parsed)?)
        },
        "Point" => {
            let coords = coordinates()?;
            if coords.as_array().is_some_and(Vec::is_empty) {
                Ok(Shape::empty_point())
            } else {
                Ok(Shape::Point(parse_point(coords, &path.key("coordinates"))?))
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
                    &path.key("coordinates"),
                )?))
            }
        },
        "LineString" => {
            let points = parse_points(coordinates()?, &path.key("coordinates"))?;
            if points.is_empty() {
                Ok(Shape::typed_empty(GeometryKind::LineString, points.axes()))
            } else {
                Ok(Shape::LineString(LineSeq::try_new(points).map_err(
                    |error| error.with_parse_path(coordinates_path.as_str()),
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
                        .map_err(|error| error.with_parse_path(coordinates_path.as_str()))?,
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
                        .map_err(|error| error.with_parse_path(coordinates_path.as_str()))?,
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
