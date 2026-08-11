#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Google Encoded Polyline codec: `from_polyline`/`to_polyline`.
//!
//! The classic compact lat/lon line encoding (Google's Encoded Polyline
//! Algorithm Format): scale to `10^precision`, delta-encode, zig-zag the
//! sign, and emit 5-bit chunks offset by 63. Rounding follows the reference
//! `JavaScript` `Math.round` (half toward +infinity) so encodings are
//! byte-identical with the canonical implementations. Coordinates are
//! WGS84 latitude/longitude by definition — and the format stores LAT
//! first, while gometry is always `(x, y)` = `(lon, lat)`.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::geometry::{CoordSeq, CoordSeqBuilder, CoordinateAxes, LineSeq, Point};
use crate::py::errors::ParseError;
use crate::py::functions::bulk_rows::{StreamedRow, stream_bulk};
use crate::{
    CRSError, Coordinates, Crs, GeometryError, PyGeometry, PyGeometryArray, PyTypeError, Shape,
    Typed, Wgs84DefaultCrs, coordinate_epoch_option, geometry_type_err, guard_epoch_frame,
    is_wgs84_family_crs, note_array_row,
};

/// Decimal digits kept per ordinate; 5 is the classic default, 6 the
/// high-resolution variant (OSRM/Valhalla). `10^11` still fits the scaled
/// domain in `i64` with headroom.
pub(crate) fn polyline_precision_factor(precision: i32) -> PyResult<f64> {
    if !(0..=11).contains(&precision) {
        return Err(GeometryError::new_err(format!(
            "from_polyline/to_polyline precision must be between 0 and 11, got {precision}"
        )));
    }
    Ok(10_f64.powi(precision))
}

/// Scale one ordinate like the reference implementation: `Math.round`
/// (half toward +infinity), not Rust's half-away-from-zero.
fn scale(value: f64, factor: f64) -> i64 {
    (value * factor + 0.5).floor() as i64
}

/// Append one signed value as zig-zag 5-bit chunks offset by 63.
fn encode_value(value: i64, out: &mut String) {
    let mut bits = value << 1;
    if value < 0 {
        bits = !bits;
    }
    while bits >= 0x20 {
        out.push(char::from((0x20 | (bits & 0x1F)) as u8 + 63));
        bits >>= 5;
    }
    out.push(char::from(bits as u8 + 63));
}

fn encode_xy(
    x: f64,
    y: f64,
    factor: f64,
    out: &mut String,
    previous: &mut (i64, i64),
) -> PyResult<()> {
    // The format is WGS84 by definition: out-of-domain coordinates
    // would encode as silent nonsense.
    if !(crate::boundary::geographic::MIN_LONGITUDE..=crate::boundary::geographic::MAX_LONGITUDE)
        .contains(&x)
        || !(crate::boundary::geographic::MIN_LATITUDE..=crate::boundary::geographic::MAX_LATITUDE)
            .contains(&y)
    {
        return Err(crate::py::errors::InvalidGeometryError::new_err(format!(
            "to_polyline coordinate (lon {x}, lat {y}) is outside the longitude/latitude \
             domain",
        )));
    }
    let lat = scale(y, factor);
    let lon = scale(x, factor);
    encode_value(lat - previous.0, out);
    encode_value(lon - previous.1, out);
    *previous = (lat, lon);
    Ok(())
}

fn encode_line<C: Coordinates + ?Sized>(points: &C, factor: f64) -> PyResult<String> {
    let mut out = String::with_capacity(points.coord_count() * 8);
    let mut previous = (0_i64, 0_i64);
    for point in points.iter_coords() {
        encode_xy(point.x, point.y, factor, &mut out, &mut previous)?;
    }
    Ok(out)
}

fn encode_point(point: Point, factor: f64) -> PyResult<String> {
    // Construct the encoding directly — no Point→Vec→CoordSeq detour.
    let mut out = String::with_capacity(8);
    let mut previous = (0_i64, 0_i64);
    encode_xy(point.x, point.y, factor, &mut out, &mut previous)?;
    Ok(out)
}

fn encode_shape(shape: &Shape, factor: f64) -> PyResult<String> {
    match shape {
        Shape::LineString(points) => encode_line(points, factor),
        Shape::Point(point) => encode_point(*point, factor),
        _ => Err(geometry_type_err(
            "to_polyline requires a LineString or Point",
        )),
    }
}

/// Decode polyline bytes straight into SoA columns (no intermediate
/// `Vec<Point>` staging). Returns the sealed coordinate sequence.
fn decode_line(data: &str, factor: f64) -> PyResult<CoordSeq> {
    // Polyline vertices are always XY lon/lat. Size hint from the encoded
    // length (~5 chars/ordinate worst-case) keeps growth rare without
    // over-reading.
    let capacity = (data.len() / 6).max(2);
    let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, capacity);
    let mut bytes = data.bytes();
    let (mut lat, mut lon) = (0_i64, 0_i64);
    // Hoist constant domain bounds once for the whole stream.
    let lat_bound = (90.0 * factor) as i64;
    let lon_bound = (180.0 * factor) as i64;
    let out_of_domain =
        || ParseError::new_err("polyline coordinate is outside the longitude/latitude domain");
    loop {
        let Some(delta_lat) = decode_value(&mut bytes, data)? else {
            break;
        };
        let Some(delta_lon) = decode_value(&mut bytes, data)? else {
            return Err(ParseError::new_err(
                "polyline data ended between a latitude and its longitude",
            ));
        };
        // The accumulated scaled coordinates must stay in the WGS84 domain
        // — the output is tagged OGC:CRS84, so a wrapped or absurd stream
        // is malformed input, not data.
        lat = lat.checked_add(delta_lat).ok_or_else(out_of_domain)?;
        lon = lon.checked_add(delta_lon).ok_or_else(out_of_domain)?;
        if lat.abs() > lat_bound || lon.abs() > lon_bound {
            return Err(out_of_domain());
        }
        let x = lon as f64 / factor;
        let y = lat as f64 / factor;
        // Point::new validates finiteness; polyline domain gates above keep
        // values finite, so the builder path matches the prior error surface.
        Point::new(x, y).map_err(|error| ParseError::new_err(error.to_string()))?;
        builder.push_xyzm(x, y, None, None);
    }
    Ok(builder.finish_infallible())
}

fn shape_from_decoded_polyline(coords: CoordSeq) -> Shape {
    match coords.len() {
        0 => Shape::LineString(LineSeq::empty(CoordinateAxes::XY)),
        1 => Shape::Point(coords.point_at(0)),
        _ => {
            Shape::LineString(LineSeq::try_new(coords).expect("polyline has two or more vertices"))
        },
    }
}

/// Python-style single-quoted char for parse messages (`'\x01'`, not Rust
/// `Debug`'s `'\u{1}'`).
fn python_char_repr(byte: u8) -> String {
    match byte {
        b'\'' => r"'\''".to_owned(),
        b'\\' => r"'\\'".to_owned(),
        b'\n' => r"'\n'".to_owned(),
        b'\r' => r"'\r'".to_owned(),
        b'\t' => r"'\t'".to_owned(),
        0x20..=0x7E => format!("'{}'", char::from(byte)),
        _ => format!("'\\x{byte:02x}'"),
    }
}

/// Double-quoted string with Python-style `\xNN` for non-printables, matching
/// sibling codec messages that echo the offending input.
fn python_str_repr(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for byte in value.bytes() {
        match byte {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7E => out.push(char::from(byte)),
            _ => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\x{byte:02x}");
            },
        }
    }
    out.push('"');
    out
}

/// One zig-zag value from the byte stream; `None` at clean end-of-input.
fn decode_value(bytes: &mut std::str::Bytes<'_>, source: &str) -> PyResult<Option<i64>> {
    let mut bits = 0_i64;
    let mut shift = 0_u32;
    loop {
        // Reject before applying: a chunk at bit 60+ could wrap the
        // accumulator through the sign bit.
        if shift >= 60 {
            return Err(ParseError::new_err("polyline value overflows"));
        }
        let Some(byte) = bytes.next() else {
            return if shift == 0 {
                Ok(None)
            } else {
                Err(ParseError::new_err("polyline data ends mid-value"))
            };
        };
        let Some(chunk) = byte.checked_sub(63).filter(|chunk| *chunk < 0x40) else {
            return Err(ParseError::new_err(format!(
                "invalid polyline character {} in {}",
                python_char_repr(byte),
                python_str_repr(source),
            )));
        };
        bits |= i64::from(chunk & 0x1F) << shift;
        shift += 5;
        if chunk < 0x20 {
            let value = if bits & 1 != 0 {
                !(bits >> 1)
            } else {
                bits >> 1
            };
            return Ok(Some(value));
        }
    }
}

/// `to_polyline` is WGS84 by definition: a bare-coordinate geometry is
/// trusted to be lon/lat; only the shared WGS84-family frames (same set as
/// GeoJSON: EPSG:4326/4979 and OGC:CRS84/h) are accepted without reproject.
fn require_polyline_crs(crs: Option<&str>) -> PyResult<()> {
    if is_wgs84_family_crs(crs) {
        Ok(())
    } else {
        Err(CRSError::new_err(
            "to_polyline requires WGS84 longitude/latitude coordinates (EPSG:4326, \
             EPSG:4979, OGC:CRS84, or OGC:CRS84h; polylines are WGS84 by definition); \
             use to_crs(...) first",
        ))
    }
}

/// Encode every present row of an array (each must be a `LineString` or `Point`).
pub(crate) fn present_polylines_of(
    array: &PyGeometryArray,
    factor: f64,
) -> PyResult<Vec<(usize, String)>> {
    require_polyline_crs(array.crs_str())?;
    array
        .present_shape_rows()
        .map(|(row, shape)| {
            let encode = || -> PyResult<String> {
                if shape.has_z() || shape.has_m() {
                    return Err(crate::geometry::GeometryErrorKind::ordinate_dropped(
                        "to_polyline",
                    )
                    .into());
                }
                encode_shape(shape.as_ref(), factor)
            };
            encode()
                .map(|encoded| (row, encoded))
                .map_err(|err| note_array_row(err, row))
        })
        .collect()
}

/// Encode one geometry (must be a `LineString` or `Point`).
pub(crate) fn polyline_of(geometry: &PyGeometry, factor: f64) -> PyResult<String> {
    require_polyline_crs(geometry.crs_str())?;
    if geometry.shape.has_z() || geometry.shape.has_m() {
        return Err(crate::geometry::GeometryErrorKind::ordinate_dropped("to_polyline").into());
    }
    encode_shape(geometry.shape.shape(), factor)
}

/// Decode polyline text into ``LineString``/``Point`` geometries.
///
/// The Google Encoded Polyline Algorithm Format — the compact lat/lon
/// route encoding used by Google Maps, OSRM, and Valhalla. Accepts one
/// string (returns a ``Point`` for one coordinate, otherwise a ``LineString``)
/// or an iterable of strings/``None`` rows (returns a `GeometryArray` with
/// missing rows). Polylines are WGS84 by definition, so results carry OGC:CRS84
/// unless ``crs=None`` explicitly requests a CRS-free result;
/// ``epoch`` restores coordinate-epoch metadata on round-trip.
///
/// Parameters
/// ----------
/// data : str or iterable of str
///     Encoded polyline text.
/// precision : int, default 5
///     Decimal digits encoded per ordinate (``0`` to ``11``); 5 is the
///     classic default, 6 the high-resolution variant.
/// crs : str or int or None, default 'OGC:CRS84'
///     Frame for the decoded longitude/latitude coordinates. Only WGS84
///     longitude/latitude CRS are valid; pass ``None`` for CRS-free output.
/// epoch : float, optional
///     Coordinate epoch (decimal year) to attach as frame metadata.
///
/// Returns
/// -------
/// Point, LineString, or GeometryArray
///     A ``Point`` when the encoding has one coordinate, a ``LineString``
///     for two or more, or a `GeometryArray` for iterable input.
///
/// Raises
/// ------
/// ParseError
///     If the text is not valid polyline data.
/// GeometryError
///     If ``precision`` is out of range.
/// CRSError
///     If ``epoch`` is set without ``crs``, or ``crs`` is not recognized.
///
/// See Also
/// --------
/// Geometry.to_polyline : Encode a LineString or Point as a polyline.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.from_polyline('_p~iF~ps|U_ulLnnqC_mqNvxq`@').to_wkt()
/// 'LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252)'
#[pyfunction]
#[pyo3(
    signature = (data, *, precision = 5, crs = Wgs84DefaultCrs::Default, epoch = None),
    text_signature = "(data, *, precision=5, crs='OGC:CRS84', epoch=None)"
)]
pub(crate) fn from_polyline(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    precision: i32,
    crs: Wgs84DefaultCrs,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let factor = polyline_precision_factor(precision)?;
    let crs = crs.into_crs();
    require_polyline_crs(crs.as_ref().map(Crs::as_str))?;
    let epoch = coordinate_epoch_option("epoch", epoch)?;
    guard_epoch_frame(epoch, crs.as_ref())?;
    let decode_tagged = |text: &str| -> PyResult<Shape> {
        let points = decode_line(text, factor).map_err(|err| {
            crate::py::errors::tag_parse_format(err, crate::error::ParseFormat::Polyline)
        })?;
        Ok(shape_from_decoded_polyline(points))
    };
    if let Ok(text) = data.cast::<pyo3::types::PyString>() {
        return Ok(Typed(PyGeometry::with_epoch(
            decode_tagged(text.to_cow()?.as_ref())?,
            crs,
            epoch,
        ))
        .into_pyobject(py)?
        .unbind());
    }
    let error = || PyTypeError::new_err("expected polyline string or iterable of strings");
    let iter = data.try_iter().map_err(|_| error())?;
    let array = stream_bulk(iter, crs, epoch, None, |item, rows| {
        let text = item
            .cast::<pyo3::types::PyString>()
            .map_err(|_| error())?
            .to_cow()?;
        rows.try_push(decode_tagged(text.as_ref())?)?;
        Ok(StreamedRow::Present(None))
    })?;
    Ok(array.into_pyobject(py)?.unbind().into())
}
