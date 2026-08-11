use pyo3::types::{PyDict, PyFloat, PyList, PyString};

use crate::py::support::{
    Bound, CRSError, Frame, GeometryError, Py, PyAny, PyAnyMethods as _, PyBool,
    PyBoolMethods as _, PyDictMethods as _, PyFloatMethods as _, PyInt, PyListMethods as _,
    PyOnceLock, PyResult, PyStringMethods as _, PyTuple, PyTupleMethods as _, PyTypeMethods as _,
    Python, Shape, Value,
};

/// Borrow `value` as a dict when it is one, or convert any other mapping
/// (`Mapping`, `MappingProxyType`, keys()-only ducks — anything with ``keys``)
/// into a fresh `PyDict`. `None` when the value is not mapping-shaped.
///
/// Enumeration follows the **mapping protocol** (same keys ``dict(value)``
/// uses): iterate ``value.keys()``, then ``value[key]`` — never treat the
/// object as a sequence (which over-rejects keys()-only mappings with
/// ``KeyError: 0``).
///
/// ``__len__`` is advisory only (ingress carrier 3): it may size a reservation
/// hint but never becomes a hard cap or exact iteration count. Progress is
/// bounded by the **N4** seen-set: a key that *repeats* is rejected immediately
/// (a valid mapping has distinct keys), which terminates ``itertools.repeat``
/// key streams without promoting ``__len__`` into authority.
///
/// Exact ``dict`` inputs take a shallow ``copy()`` so callers never share
/// identity with the returned mapping (feature properties contract).
pub(crate) fn mapping_as_dict<'py>(
    value: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    if let Ok(dict) = value.cast::<PyDict>() {
        return Ok(Some(dict.copy()?));
    }
    let Some(keys_attr) = value.getattr_opt(pyo3::intern!(value.py(), "keys"))? else {
        return Ok(None);
    };
    // Bare non-callable ``keys`` is not mapping-shaped. Call keys() only here
    // (the conversion owner) so probes never reacquire/consume the stream; a
    // raising keys() is a protocol error, not "not a mapping".
    if !keys_attr.is_callable() {
        return Ok(None);
    }
    let keys = keys_attr.call0()?;

    let py = value.py();
    // `__len__` is advisory only — not consulted as an iteration ceiling.
    let dict = PyDict::new(py);
    // try_iter uses PyObject_GetIter: accepts ``__iter__`` *and* legacy
    // ``__getitem__``-sequence keys (same as builtin dict(mapping)).
    for key in keys.try_iter()? {
        let key = key?;
        // Distinct-key invariant: reject repeats before set_item overwrites.
        if dict.contains(&key)? {
            return Err(GeometryError::new_err("mapping has duplicate key"));
        }
        let item = value.get_item(&key)?;
        dict.set_item(key, item)?;
    }
    Ok(Some(dict))
}

/// True when *value* is mapping-shaped (exact ``dict`` or callable ``keys``).
///
/// Does **not** call or enumerate ``keys()`` — classification probes must not
/// consume a one-shot key stream or swallow a raising ``keys()`` as "not a
/// mapping". The conversion owner ([`mapping_as_dict`]) is the only caller.
pub(crate) fn is_mapping_like(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.cast::<PyDict>().is_ok() {
        return Ok(true);
    }
    let Some(keys_attr) = value.getattr_opt(pyo3::intern!(value.py(), "keys"))? else {
        return Ok(false);
    };
    Ok(keys_attr.is_callable())
}

/// Scalar-vs-iterable classification that retains a successfully acquired
/// iterator (no double-acquire) and propagates genuine protocol errors from
/// ``__iter__`` / ``__getitem__``.
pub(crate) enum ScalarOrIterator<'py> {
    Scalar,
    Iterator(Bound<'py, pyo3::types::PyIterator>),
}

/// Classify *value* as a GeoJSON scalar document (string / bytes / mapping /
/// ``__geo_interface__``) or a retained row iterator for bulk ingestion.
///
/// Mapping classification uses [`is_mapping_like`] (no ``keys()`` call). When
/// the value is not a known scalar shape, the iterator is acquired once;
/// protocol exceptions propagate rather than being re-read as "scalar".
pub(crate) fn classify_scalar_or_iterator<'py>(
    value: &Bound<'py, PyAny>,
) -> PyResult<ScalarOrIterator<'py>> {
    if value.cast::<PyString>().is_ok()
        || is_py_bytes_or_bytearray(value)
        || is_mapping_like(value)?
        || value
            .getattr_opt(pyo3::intern!(value.py(), "__geo_interface__"))?
            .is_some()
    {
        return Ok(ScalarOrIterator::Scalar);
    }
    Ok(ScalarOrIterator::Iterator(value.try_iter()?))
}

#[derive(Clone, Copy)]
enum JsonProjection {
    Full,
    /// Geometry-shaped keys only (plus nested legacy ``crs``), so feature
    /// properties stay out of the geometry coerce path.
    Geometry,
}

/// Convert a Python `int` into a JSON number.
///
/// - Values that fit in `i64`/`u64` stay integer-shaped `Number`s (PROJJSON
///   codes, feature properties, GeoParquet frame metadata must not grow a
///   trailing `.0`).
/// - Larger magnitudes are admitted only when exactly representable as
///   binary64 (same rule as TEXT coordinate integer tokens); non-exact
///   integers raise rather than silently round.
///
/// Coordinate exactness for the i64/u64 range is enforced later by
/// [`crate::io::json_number_to_f64`] / the text visitors — one shared rule.
fn py_int_to_json_number(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if let Ok(i) = value.extract::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(u) = value.extract::<u64>() {
        return Ok(Value::Number(u.into()));
    }
    // Magnitude outside i64/u64: platform float conversion, then require
    // int(float) == value so only exact binary64 integers pass.
    let float: f64 = value
        .call_method0(pyo3::intern!(value.py(), "__float__"))?
        .extract()?;
    if !float.is_finite() {
        return Err(crate::io::IoError::geojson(format!(
            "GeoJSON coordinate {} is not exactly representable as f64",
            value.str()?
        ))
        .into());
    }
    let as_int = value
        .py()
        .import(pyo3::intern!(value.py(), "builtins"))?
        .getattr(pyo3::intern!(value.py(), "int"))?
        .call1((float,))?;
    if value.eq(&as_int)? {
        serde_json::Number::from_f64(float)
            .map(Value::Number)
            .ok_or_else(|| GeometryError::new_err("JSON number must be finite"))
    } else {
        Err(crate::io::IoError::geojson(format!(
            "GeoJSON coordinate {} is not exactly representable as f64",
            value.str()?
        ))
        .into())
    }
}

/// Recursively convert a Python object to `serde_json::Value` without a
/// `json.dumps` round-trip. `projection` lets geometry-only ingestion skip
/// feature properties while sharing the same bounded recursive walker.
fn py_to_json_value_inner(
    value: &Bound<'_, PyAny>,
    projection: JsonProjection,
    depth: usize,
) -> PyResult<Value> {
    if depth >= crate::io::MAX_PARSE_DEPTH {
        return Err(GeometryError::new_err(format!(
            "JSON nesting exceeds the limit of {}",
            crate::io::MAX_PARSE_DEPTH
        )));
    }
    if value.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = value.cast::<PyBool>() {
        return Ok(Value::Bool(b.is_true()));
    }
    if let Ok(s) = value.cast::<PyString>() {
        return Ok(Value::String(s.to_string()));
    }
    // F3: any mapping-shaped object at nested positions (UserDict,
    // MappingProxyType, keys()-only ducks) — copy once via the keystone, then
    // walk the retained dict. Exact PyDict takes the shallow-clone fast path.
    if let Some(dict) = mapping_as_dict(value)? {
        let mut map = serde_json::Map::new();
        for (key, val) in dict.iter() {
            let key_str = key.extract::<String>()?;
            let child_projection = match projection {
                JsonProjection::Full => Some(JsonProjection::Full),
                // Keep legacy ``crs`` at every nesting level (Feature geometry,
                // FeatureCollection members, GeometryCollection children) so
                // reconciliation can see every declaration — not only the root.
                JsonProjection::Geometry if key_str == "crs" => Some(JsonProjection::Full),
                JsonProjection::Geometry
                    if matches!(
                        key_str.as_str(),
                        "type" | "coordinates" | "geometries" | "geometry" | "features"
                    ) =>
                {
                    Some(JsonProjection::Geometry)
                },
                // Presence-only stub for ``properties``: RFC 7946 §7.1 forbids
                // it on Geometry objects, but Geometry projection must not
                // materialize property values (opaque / non-JSON side data).
                JsonProjection::Geometry if key_str == "properties" => {
                    map.insert(key_str.clone(), Value::Null);
                    None
                },
                JsonProjection::Geometry => None,
            };
            if let Some(child_projection) = child_projection {
                map.insert(
                    key_str,
                    py_to_json_value_inner(&val, child_projection, depth + 1)?,
                );
            }
        }
        return Ok(Value::Object(map));
    }
    if let Ok(list) = value.cast::<PyList>() {
        return list
            .iter()
            .map(|item| py_to_json_value_inner(&item, projection, depth + 1))
            .collect::<PyResult<_>>()
            .map(Value::Array);
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return tuple
            .iter()
            .map(|item| py_to_json_value_inner(&item, projection, depth + 1))
            .collect::<PyResult<_>>()
            .map(Value::Array);
    }
    // Integers: keep i64/u64 as integer JSON numbers (CRS/PROJJSON codes, etc.);
    // large exact PyLongs become finite binary64; non-exact large ints raise.
    // Coordinate exactness for the i64/u64 range is shared with the TEXT path
    // via json_number_to_f64 / visit_i64/u64.
    if value.cast::<PyInt>().is_ok() && value.cast::<PyBool>().is_err() {
        return py_int_to_json_number(value);
    }
    if let Ok(f) = value.cast::<PyFloat>() {
        let f = f.value();
        return serde_json::Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(|| GeometryError::new_err("JSON number must be finite"));
    }
    Err(GeometryError::new_err(format!(
        "unsupported JSON value type {}",
        value.get_type().name()?
    )))
}

pub(crate) fn py_to_json_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    py_to_json_value_inner(value, JsonProjection::Full, 0)
}

pub(crate) fn parse_geojson_slice(bytes: &[u8]) -> PyResult<Value> {
    Ok(serde_json::from_slice(bytes)
        .map_err(|error| crate::io::IoError::geojson(error.to_string()))?)
}

pub(crate) fn is_py_bytes_or_bytearray(value: &Bound<'_, PyAny>) -> bool {
    // Broad one-byte buffer detection: bytes/bytearray/memoryview plus any
    // buffer-protocol exporter with itemsize 1 (signed or unsigned) —
    // ``array.array('B'|'b')``, ``memoryview(...).cast('b')``, etc.
    crate::is_one_byte_buffer(value)
}

pub(crate) fn parse_geojson_bytes_like(value: &Bound<'_, PyAny>) -> PyResult<Option<Value>> {
    if !crate::is_one_byte_buffer(value) {
        return Ok(None);
    }
    Ok(Some(crate::with_one_byte_buffer(
        value,
        parse_geojson_slice,
    )?))
}

fn parse_geojson_mapping(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if let Some(dict) = mapping_as_dict(value)? {
        return py_to_json_value(&dict);
    }
    // Exotic mapping types that are not dict-shaped: fall back to `json.dumps`.
    let text = json_dumps(value.py())?
        .call1((value,))?
        .extract::<String>()?;
    Ok(serde_json::from_str(&text)
        .map_err(|error| crate::io::IoError::geojson(error.to_string()))?)
}

pub(crate) fn parse_geojson_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    parse_geojson_input(value, parse_geojson_mapping)
}

fn parse_geojson_input(
    value: &Bound<'_, PyAny>,
    parse_mapping: impl Fn(&Bound<'_, PyAny>) -> PyResult<Value>,
) -> PyResult<Value> {
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(serde_json::from_str(text.to_cow()?.as_ref())
            .map_err(|error| crate::io::IoError::geojson(error.to_string()))?);
    }
    if let Some(parsed) = parse_geojson_bytes_like(value)? {
        return Ok(parsed);
    }
    if let Some(interface) = value.getattr_opt(pyo3::intern!(value.py(), "__geo_interface__"))? {
        return parse_mapping(&interface);
    }
    parse_mapping(value)
}

fn parse_geojson_geometry_mapping(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if let Some(dict) = mapping_as_dict(value)? {
        return py_to_json_value_inner(&dict, JsonProjection::Geometry, 0);
    }
    let text = json_dumps(value.py())?
        .call1((value,))?
        .extract::<String>()?;
    Ok(serde_json::from_str(&text)
        .map_err(|error| crate::io::IoError::geojson(error.to_string()))?)
}

pub(crate) fn parse_geojson_geometry_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    parse_geojson_input(value, parse_geojson_geometry_mapping)
}

pub(crate) fn json_dumps(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    static JSON_DUMPS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    JSON_DUMPS.import(py, "json", "dumps")
}

/// How a GeoJSON `Value` selects its geometry payload before parsing.
///
/// * [`GeometryLike`](GeoJsonDecodeContext::GeometryLike) — bare Geometry or
///   one top-level Feature; a null Feature geometry is an error.
/// * [`NullableRow`](GeoJsonDecodeContext::NullableRow) — bare Geometry or
///   Feature; a null Feature geometry yields ``None`` (missing row).
/// * [`GeometrySlot`](GeoJsonDecodeContext::GeometrySlot) — Geometry only;
///   Feature / FeatureCollection are rejected by the geometry-slot decoder.
#[derive(Clone, Copy, Debug)]
pub(crate) enum GeoJsonDecodeContext {
    GeometryLike,
    NullableRow,
    GeometrySlot,
}

/// One context-aware Shape+Frame coercer for foreign GeoJSON values.
///
/// Collects every nested legacy ``crs`` declaration from the still-wrapped
/// value, resolves them through
/// [`reconcile_legacy_geojson_crs`](crate::io::reconcile_legacy_geojson_crs),
/// selects the shape payload per `context`, parses via `io::parse_geojson`,
/// and returns `(Shape, Frame::new(resolved_crs, epoch))`. This is the sole
/// Shape+Frame owner for GeoJSON coerce paths — there is no second validation
/// loop or frame-attachment helper.
pub(crate) fn coerce_geojson_geometry_value(
    value: &Value,
    context: GeoJsonDecodeContext,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
    epoch: Option<f64>,
) -> PyResult<Option<(Shape, Frame)>> {
    let mut declarations = Vec::new();
    crate::io::collect_geojson_legacy_crs(value, &mut declarations);
    let resolved = crate::io::reconcile_legacy_geojson_crs(&declarations, policy)?;
    let Some(geometry_value) = select_geojson_geometry_value(value, context)? else {
        return Ok(None);
    };
    let shape = crate::io::parse_geojson(geometry_value)?;
    let frame = Frame::new(resolved.map(crate::crs_arc), epoch)?;
    Ok(Some((shape, frame)))
}

fn select_geojson_geometry_value(
    value: &Value,
    context: GeoJsonDecodeContext,
) -> PyResult<Option<&Value>> {
    match context {
        GeoJsonDecodeContext::GeometrySlot => Ok(Some(value)),
        GeoJsonDecodeContext::GeometryLike | GeoJsonDecodeContext::NullableRow => {
            let Some(object) = value.as_object() else {
                return Ok(Some(value));
            };
            if object.get("type").and_then(Value::as_str) != Some("Feature") {
                return Ok(Some(value));
            }
            // Top-level Feature: enforce §7.1 before unwrapping geometry.
            crate::io::reject_rfc7946_value_object(object)?;
            let geometry = object
                .get("geometry")
                .ok_or_else(|| crate::io::IoError::geojson("GeoJSON Feature requires geometry"))?;
            if geometry.is_null() {
                return match context {
                    GeoJsonDecodeContext::NullableRow => Ok(None),
                    GeoJsonDecodeContext::GeometryLike => Err(crate::io::IoError::geojson(
                        "Feature has null geometry (an unlocated feature); parse the \
                         FeatureCollection with from_geojson/from_features, where null \
                         geometries become missing rows",
                    )
                    .into()),
                    GeoJsonDecodeContext::GeometrySlot => unreachable!("matched above"),
                };
            }
            Ok(Some(geometry))
        },
    }
}

/// True when `crs` is absent or a WGS84 lon/lat family identifier accepted by
/// GeoJSON, polyline, and related codecs (EPSG:4326/4979 and OGC:CRS84/h).
pub(crate) fn is_wgs84_family_crs(crs: Option<&str>) -> bool {
    matches!(
        crs,
        None | Some("EPSG:4326" | "EPSG:4979" | "OGC:CRS84" | "OGC:CRS84h")
    )
}

/// `GeoJSON` is WGS84 by specification (RFC 7946): refuse to serialize
/// coordinates declared in any other frame — raw projected output would be
/// read as longitude/latitude downstream. CRS-free input is trusted as-is.
pub(crate) fn require_geojson_crs(crs: Option<&str>) -> PyResult<()> {
    if is_wgs84_family_crs(crs) {
        Ok(())
    } else {
        Err(CRSError::new_err(format!(
            "GeoJSON is WGS84 by specification (RFC 7946); got CRS {:?} — \
             reproject with to_crs('OGC:CRS84') or to_crs(4326) first",
            crs.expect("non-family CRS is Some")
        )))
    }
}
