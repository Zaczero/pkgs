use pyo3::PyErr;
use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::types::{PyDict, PyFloat, PyList, PyString};

use crate::py::errors::parse_error;
use crate::py::support::{
    Bound, CRSError, Frame, GeometryError, Py, PyAny, PyAnyMethods as _, PyBool,
    PyBoolMethods as _, PyDictMethods as _, PyFloatMethods as _, PyInt, PyListMethods as _,
    PyOnceLock, PyResult, PyStringMethods as _, PyTuple, PyTupleMethods as _, PyTypeMethods as _,
    Python, Shape, Value,
};
use crate::py::wire_crs::has_ewkt_srid_prefix;

/// Select WKT before trying GeoJSON so malformed WKT-looking strings retain
/// the WKT error format instead of being reported as JSON failures.
pub(crate) fn is_wkt_string(text: &str) -> bool {
    let trimmed = text.trim();
    if has_ewkt_srid_prefix(text) {
        return true;
    }
    let keyword_len = trimmed
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .unwrap_or(trimmed.len());
    trimmed
        .get(..keyword_len)
        .and_then(crate::io::classify_wkt_type_token)
        .is_some()
}

/// Borrow `value` as a dict when it is one, or convert any other mapping
/// (`Mapping`, `MappingProxyType`, keys()-only ducks — anything with ``keys``)
/// into a fresh `PyDict`. `None` when the value is not mapping-shaped.
///
/// Enumeration follows the **mapping protocol** (same keys ``dict(value)``
/// uses): iterate ``value.keys()``, then ``value[key]`` — never treat the
/// object as a sequence (which over-rejects keys()-only mappings with
/// ``KeyError: 0``).
///
/// Duplicate keys are rejected instead of silently overwriting earlier values.
///
/// Exact ``dict`` inputs take a shallow ``copy()`` so callers never share
/// identity with the returned mapping (feature properties contract).
pub(crate) fn mapping_as_dict<'py>(
    value: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    if let Ok(dict) = value.cast_exact::<PyDict>() {
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
    let dict = PyDict::new(py);
    // try_iter uses PyObject_GetIter: accepts ``__iter__`` *and* legacy
    // ``__getitem__``-sequence keys (same as builtin dict(mapping)).
    for (index, key) in keys.try_iter()?.enumerate() {
        crate::check_python_signals(value.py(), index)?;
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
    if value.cast_exact::<PyDict>().is_ok() {
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
        || is_py_bytes_or_bytearray(value)?
        || is_mapping_like(value)?
        || value
            .getattr_opt(pyo3::intern!(value.py(), "__geo_interface__"))?
            .is_some()
    {
        return Ok(ScalarOrIterator::Scalar);
    }
    Ok(ScalarOrIterator::Iterator(value.try_iter()?))
}

/// Concrete containers can be snapshotted before one detached batch parse.
/// Everything else remains a general iterator and must retain fail-fast row
/// consumption semantics.
pub(crate) fn is_concrete_batch(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.cast::<PyList>().is_ok() || value.cast::<PyTuple>().is_ok() {
        return Ok(true);
    }
    let value_type = value.get_type();
    Ok(value_type.name()? == "ndarray"
        && value_type.getattr("__module__")?.extract::<&str>()? == "numpy")
}

#[derive(Clone, Copy)]
enum JsonProjection {
    Full,
    GeoJsonFull,
    /// Geometry-shaped keys only (plus nested legacy ``crs``), so feature
    /// metadata is checked without being used to construct the geometry.
    Geometry,
    /// JSON data whose numeric values are not geometry coordinates. Large
    /// Python integers remain JSON numbers even when binary64 would round them.
    Data,
}

/// Conversion failures with semantic information retained for callers that
/// need to classify a JSON boundary without inspecting its display text.
pub(crate) enum JsonValueError {
    NonFinite,
    NonStringKey,
    NestingLimit(usize),
    FeatureDataNestingLimit,
    UnsupportedType(String),
    Python(PyErr),
}

impl From<PyErr> for JsonValueError {
    fn from(error: PyErr) -> Self {
        Self::Python(error)
    }
}

impl JsonValueError {
    fn into_py_err(self) -> PyErr {
        match self {
            Self::NonFinite => GeometryError::new_err("JSON number must be finite"),
            Self::NonStringKey => PyTypeError::new_err("JSON object keys must be strings"),
            Self::NestingLimit(max_depth) => PyErr::from(crate::io::IoError::geojson(format!(
                "GeoJSON JSON nesting exceeds the structural limit of {max_depth}"
            ))),
            Self::FeatureDataNestingLimit => parse_error(
                format!(
                    "GeoJSON Feature side data exceeds nesting depth {}",
                    crate::geometry::MAX_NESTING
                ),
                crate::error::ParseFormat::GeoJson,
            ),
            Self::UnsupportedType(name) => {
                GeometryError::new_err(format!("unsupported JSON value type {name}"))
            },
            Self::Python(error) => error,
        }
    }

    fn into_geojson_py_err(self) -> PyErr {
        match self {
            Self::NonFinite => crate::io::IoError::geojson("JSON number must be finite").into(),
            Self::NonStringKey => {
                crate::io::IoError::geojson("JSON object keys must be strings").into()
            },
            Self::NestingLimit(max_depth) => crate::io::IoError::geojson(format!(
                "GeoJSON JSON nesting exceeds the structural limit of {max_depth}"
            ))
            .into(),
            Self::FeatureDataNestingLimit => parse_error(
                format!(
                    "GeoJSON Feature side data exceeds nesting depth {}",
                    crate::geometry::MAX_NESTING
                ),
                crate::error::ParseFormat::GeoJson,
            ),
            Self::UnsupportedType(name) => {
                crate::io::IoError::geojson(format!("unsupported JSON value type {name}")).into()
            },
            Self::Python(error) => error,
        }
    }
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
/// Coordinate exactness for the i64/u64 range is enforced later by the shared
/// Value decoder via [`crate::io::json_number_to_f64`].
fn py_int_to_json_number(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    if let Ok(i) = value.extract::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(u) = value.extract::<u64>() {
        return Ok(Value::Number(u.into()));
    }
    // Magnitude outside i64/u64: platform float conversion, then require
    // int(float) == value so only exact binary64 integers pass.
    let float = json_integer_float(value)?;
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

fn py_int_to_json_data_number(value: &Bound<'_, PyAny>) -> Result<Value, JsonValueError> {
    if let Ok(i) = value.extract::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(u) = value.extract::<u64>() {
        return Ok(Value::Number(u.into()));
    }
    let float = json_integer_float(value)?;
    serde_json::Number::from_f64(float)
        .map(Value::Number)
        .ok_or(JsonValueError::NonFinite)
}

fn json_integer_float(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    let float = match value.call_method0(pyo3::intern!(value.py(), "__float__")) {
        Ok(float) => float,
        Err(error) if error.is_instance_of::<PyOverflowError>(value.py()) => {
            return Err(
                crate::io::IoError::geojson("JSON number is outside the supported range").into(),
            );
        },
        Err(error) => return Err(error),
    };
    float.extract()
}

fn python_json_type_summary(
    value: &Bound<'_, PyAny>,
    depth: usize,
) -> Result<Value, JsonValueError> {
    validate_python_json_value_inner(value, depth, MAX_JSON_NESTING, true)?;
    if value.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(value) = value.cast::<PyBool>() {
        return Ok(Value::Bool(value.is_true()));
    }
    if value.cast::<PyString>().is_ok() {
        return Ok(Value::String(String::new()));
    }
    if value.cast::<PyInt>().is_ok() {
        return Ok(Value::Number(0.into()));
    }
    if value.cast::<PyFloat>().is_ok() {
        return Ok(Value::Number(0.into()));
    }
    if mapping_as_dict(value)?.is_some() {
        return Ok(Value::Object(serde_json::Map::new()));
    }
    if value.cast::<PyList>().is_ok() || value.cast::<PyTuple>().is_ok() {
        return Ok(Value::Array(Vec::new()));
    }
    Err(JsonValueError::UnsupportedType(
        value.get_type().name()?.to_cow()?.into_owned(),
    ))
}

fn py_to_json_value_mapping(
    dict: &Bound<'_, PyDict>,
    projection: JsonProjection,
    depth: usize,
) -> Result<Value, JsonValueError> {
    let mut map = serde_json::Map::new();
    for (key, val) in dict.iter() {
        let key = key
            .cast::<PyString>()
            .map_err(|_| JsonValueError::NonStringKey)?;
        let key_str = key.to_cow()?.into_owned();
        if matches!(projection, JsonProjection::Geometry)
            && matches!(key_str.as_str(), "properties" | "id")
        {
            map.insert(
                key_str,
                python_json_type_summary(&val, depth.wrapping_add(1))?,
            );
            continue;
        }
        if matches!(projection, JsonProjection::Geometry)
            && !matches!(
                key_str.as_str(),
                "crs" | "type" | "coordinates" | "geometries" | "geometry" | "features" | "bbox"
            )
        {
            validate_python_json_value_inner(&val, depth.wrapping_add(1), MAX_JSON_NESTING, true)?;
            continue;
        }
        let child_projection = match projection {
            JsonProjection::Full | JsonProjection::GeoJsonFull | JsonProjection::Data => projection,
            // Keep legacy ``crs`` at every nesting level (Feature geometry,
            // FeatureCollection members, GeometryCollection children) so
            // reconciliation can see every declaration — not only the root.
            JsonProjection::Geometry if key_str == "crs" => JsonProjection::GeoJsonFull,
            JsonProjection::Geometry
                if matches!(
                    key_str.as_str(),
                    "type" | "coordinates" | "geometries" | "geometry" | "features"
                ) =>
            {
                JsonProjection::Geometry
            },
            JsonProjection::Geometry => JsonProjection::Data,
        };
        let converted = py_to_json_value_inner(&val, child_projection, depth.wrapping_add(1))?;
        map.insert(key_str, converted);
    }
    Ok(Value::Object(map))
}

/// Recursively convert a Python object to `serde_json::Value` without a
/// `json.dumps` round-trip. `projection` retains geometry fields while
/// validating discarded Feature metadata and extension members as JSON.
fn py_to_json_value_inner(
    value: &Bound<'_, PyAny>,
    projection: JsonProjection,
    depth: usize,
) -> Result<Value, JsonValueError> {
    let max_depth = match projection {
        JsonProjection::Full => crate::geometry::MAX_NESTING,
        JsonProjection::GeoJsonFull | JsonProjection::Geometry | JsonProjection::Data => {
            MAX_JSON_NESTING
        },
    };
    if depth > max_depth {
        if matches!(projection, JsonProjection::Full) {
            return Err(JsonValueError::Python(PyErr::from(
                crate::io::IoError::geojson(format!(
                    "JSON nesting exceeds the limit of {max_depth}"
                )),
            )));
        }
        return Err(JsonValueError::NestingLimit(max_depth));
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
        return py_to_json_value_mapping(&dict, projection, depth);
    }
    if let Ok(list) = value.cast::<PyList>() {
        let mut values = Vec::with_capacity(list.len());
        for item in list.iter() {
            values.push(py_to_json_value_inner(
                &item,
                projection,
                depth.wrapping_add(1),
            )?);
        }
        return Ok(Value::Array(values));
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        let mut values = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            values.push(py_to_json_value_inner(
                &item,
                projection,
                depth.wrapping_add(1),
            )?);
        }
        return Ok(Value::Array(values));
    }
    // Coordinate integers must be exactly representable as binary64. Metadata
    // uses the Data projection and only needs to retain the JSON number category.
    if value.cast::<PyInt>().is_ok() && value.cast::<PyBool>().is_err() {
        return if matches!(projection, JsonProjection::Data) {
            Ok(py_int_to_json_data_number(value)?)
        } else {
            Ok(py_int_to_json_number(value)?)
        };
    }
    if let Ok(f) = value.cast::<PyFloat>() {
        let f = f.value();
        return serde_json::Number::from_f64(f)
            .map(Value::Number)
            .ok_or(JsonValueError::NonFinite);
    }
    Err(JsonValueError::UnsupportedType(
        value.get_type().name()?.to_cow()?.into_owned(),
    ))
}

pub(crate) fn py_to_json_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    py_to_json_value_inner(value, JsonProjection::Full, 0).map_err(JsonValueError::into_py_err)
}

pub(crate) fn py_to_geojson_json_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    py_to_json_value_inner(value, JsonProjection::GeoJsonFull, 0)
        .map_err(JsonValueError::into_geojson_py_err)
}

pub(crate) fn py_to_geojson_data_value(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    py_to_json_value_inner(value, JsonProjection::Data, 0)
        .map_err(JsonValueError::into_geojson_py_err)
}

fn validate_python_json_value_inner(
    value: &Bound<'_, PyAny>,
    depth: usize,
    depth_limit: usize,
    check_number_range: bool,
) -> Result<(), JsonValueError> {
    if depth > depth_limit {
        return Err(if depth_limit == crate::geometry::MAX_NESTING {
            JsonValueError::FeatureDataNestingLimit
        } else {
            JsonValueError::NestingLimit(depth_limit)
        });
    }
    if value.is_none() || value.cast::<PyBool>().is_ok() || value.cast::<PyString>().is_ok() {
        return Ok(());
    }
    if let Some(dict) = mapping_as_dict(value)? {
        for (key, item) in dict.iter() {
            if key.cast::<PyString>().is_err() {
                return Err(JsonValueError::NonStringKey);
            }
            validate_python_json_value_inner(
                &item,
                depth.wrapping_add(1),
                depth_limit,
                check_number_range,
            )?;
        }
        return Ok(());
    }
    if let Ok(list) = value.cast::<PyList>() {
        for item in list.iter() {
            validate_python_json_value_inner(
                &item,
                depth.wrapping_add(1),
                depth_limit,
                check_number_range,
            )?;
        }
        return Ok(());
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        for item in tuple.iter() {
            validate_python_json_value_inner(
                &item,
                depth.wrapping_add(1),
                depth_limit,
                check_number_range,
            )?;
        }
        return Ok(());
    }
    if value.cast::<PyInt>().is_ok() {
        if check_number_range
            && value.extract::<i64>().is_err()
            && value.extract::<u64>().is_err()
            && !json_integer_float(value)?.is_finite()
        {
            return Err(JsonValueError::NonFinite);
        }
        return Ok(());
    }
    if let Ok(number) = value.cast::<PyFloat>() {
        return if number.value().is_finite() {
            Ok(())
        } else {
            Err(JsonValueError::NonFinite)
        };
    }
    Err(JsonValueError::UnsupportedType(
        value.get_type().name()?.to_cow()?.into_owned(),
    ))
}

pub(crate) fn validate_geojson_json_value(
    value: &Bound<'_, PyAny>,
    depth_limit: usize,
) -> PyResult<()> {
    validate_python_json_value_inner(value, 0, depth_limit, true)
        .map_err(JsonValueError::into_geojson_py_err)
}

pub(crate) fn validate_geojson_feature_side_data(value: &Bound<'_, PyAny>) -> PyResult<()> {
    validate_python_json_value_inner(value, 0, crate::geometry::MAX_NESTING, false)
        .map_err(JsonValueError::into_geojson_py_err)
}

pub(crate) fn py_to_json_value_classified(
    value: &Bound<'_, PyAny>,
) -> Result<Value, JsonValueError> {
    py_to_json_value_inner(value, JsonProjection::Full, 0)
}

// Each GeometryCollection contributes an object and a `geometries` array to
// JSON nesting, so serde_json's own 128-frame guard is below the geometry cap.
// Keep a finite lexical envelope before disabling that parser guard.
pub(crate) const MAX_JSON_NESTING: usize = crate::geometry::MAX_NESTING * 2 + 16;

#[expect(
    clippy::indexing_slicing,
    reason = "the JSON scanner calls this predicate only for an in-range byte offset"
)]
fn is_non_finite_json_number(bytes: &[u8], index: usize) -> bool {
    let at_value_boundary = index == 0
        || bytes
            .get(index.wrapping_sub(1))
            .is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'[' | b',' | b':'));
    if !at_value_boundary {
        return false;
    }
    let remaining = &bytes[index..];
    let token_len = if remaining.starts_with(b"-Infinity") {
        b"-Infinity".len()
    } else if remaining.starts_with(b"Infinity") {
        b"Infinity".len()
    } else if remaining.starts_with(b"NaN") {
        b"NaN".len()
    } else {
        return false;
    };
    bytes
        .get(index.wrapping_add(token_len))
        .is_none_or(|byte| byte.is_ascii_whitespace() || matches!(byte, b',' | b']' | b'}'))
}

fn check_json_nesting(bytes: &[u8]) -> crate::error::Result<()> {
    let mut depth = 0_usize;
    let mut escaped = false;
    let mut in_string = false;
    for (index, &byte) in bytes.iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        if is_non_finite_json_number(bytes, index) {
            return Err(crate::io::IoError::geojson("JSON number must be finite"));
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth.wrapping_add(1);
                if depth > MAX_JSON_NESTING {
                    return Err(crate::io::IoError::geojson(format!(
                        "GeoJSON JSON nesting exceeds the structural limit of {MAX_JSON_NESTING}"
                    )));
                }
            },
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {},
        }
    }
    Ok(())
}

pub(crate) fn deserialize_json<'de, T>(bytes: &'de [u8]) -> crate::error::Result<T>
where
    T: serde::Deserialize<'de>,
{
    check_json_nesting(bytes)?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    deserializer.disable_recursion_limit();
    let value = T::deserialize(&mut deserializer).map_err(geojson_json_error)?;
    deserializer.end().map_err(geojson_json_error)?;
    Ok(value)
}

fn geojson_json_error(error: serde_json::Error) -> crate::error::Error {
    let detail = error.to_string();
    if detail.starts_with("number out of range at line ") {
        crate::io::IoError::geojson("JSON number is outside the supported range")
    } else {
        crate::io::IoError::geojson(detail)
    }
}

fn parse_json_text(text: &str) -> crate::error::Result<Value> {
    deserialize_json(text.as_bytes())
}

fn parse_json_bytes(bytes: &[u8]) -> crate::error::Result<Value> {
    deserialize_json(bytes)
}

pub(crate) fn parse_geojson_text(text: &str) -> crate::error::Result<Value> {
    parse_json_text(text)
}

pub(crate) fn parse_geojson_slice(bytes: &[u8]) -> crate::error::Result<Value> {
    parse_json_bytes(bytes)
}

pub(crate) fn is_py_bytes_or_bytearray(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    // Broad one-byte buffer detection: bytes/bytearray/memoryview plus any
    // buffer-protocol exporter with itemsize 1 (signed or unsigned) —
    // ``array.array('B'|'b')``, ``memoryview(...).cast('b')``, etc.
    crate::is_one_byte_buffer(value)
}

pub(crate) fn parse_geojson_bytes_like(value: &Bound<'_, PyAny>) -> PyResult<Option<Value>> {
    if !crate::is_one_byte_buffer(value)? {
        return Ok(None);
    }
    Ok(Some(crate::with_one_byte_buffer(value, |bytes| {
        parse_geojson_slice(bytes).map_err(pyo3::PyErr::from)
    })?))
}

fn parse_geojson_input(
    value: &Bound<'_, PyAny>,
    parse_mapping: impl Fn(&Bound<'_, PyAny>) -> PyResult<Value>,
) -> PyResult<Value> {
    if let Ok(text) = value.cast::<PyString>() {
        return Ok(parse_json_text(text.to_cow()?.as_ref())?);
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
        return py_to_json_value_mapping(&dict, JsonProjection::Geometry, 0)
            .map_err(JsonValueError::into_geojson_py_err);
    }
    let text = json_dumps(value.py())?
        .call1((value,))?
        .extract::<String>()?;
    Ok(parse_json_text(&text)?)
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
#[derive(Clone, Copy, Debug)]
pub(crate) enum GeoJsonDecodeContext {
    GeometryLike,
    NullableRow,
}

/// One context-aware Shape+Frame coercer for foreign GeoJSON values.
///
/// Reconciles nested legacy ``crs`` declarations from the still-wrapped
/// value through [`GeoJsonCrs`](crate::io::GeoJsonCrs),
/// selects the shape payload per `context`, parses via `io::parse_geojson_at`,
/// and returns `(Shape, Frame::new(resolved_crs, epoch))`. This is the sole
/// Shape+Frame owner for GeoJSON coerce paths — there is no second validation
/// loop or frame-attachment helper.
pub(crate) fn coerce_geojson_geometry_value(
    value: &Value,
    context: GeoJsonDecodeContext,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
    epoch: Option<f64>,
) -> crate::error::Result<Option<(Shape, Frame)>> {
    coerce_geojson_geometry_value_at(value, context, policy, epoch, "$")
}

pub(crate) fn coerce_geojson_geometry_value_at(
    value: &Value,
    context: GeoJsonDecodeContext,
    policy: crate::io::LegacyGeoJsonCrsPolicy<'_>,
    epoch: Option<f64>,
    base_path: &str,
) -> crate::error::Result<Option<(Shape, Frame)>> {
    let mut declarations = crate::io::GeoJsonCrs::new(policy);
    declarations.reconcile_tree(value)?;
    let resolved = declarations.into_crs();
    let Some((geometry_value, path)) = select_geojson_geometry_value(value, context, base_path)?
    else {
        return Ok(None);
    };
    let shape = crate::io::parse_geojson_at(geometry_value, &path)?;
    let frame = Frame::new(resolved.map(crate::crs_arc), epoch)?;
    Ok(Some((shape, frame)))
}

fn select_geojson_geometry_value<'a>(
    value: &'a Value,
    context: GeoJsonDecodeContext,
    base_path: &str,
) -> crate::error::Result<Option<(&'a Value, String)>> {
    match context {
        GeoJsonDecodeContext::GeometryLike | GeoJsonDecodeContext::NullableRow => {
            let Some(object) = value.as_object() else {
                return Ok(Some((value, base_path.to_owned())));
            };
            if object.get("type").and_then(Value::as_str) != Some("Feature") {
                return Ok(Some((value, base_path.to_owned())));
            }
            // Top-level Feature: enforce §7.1 before unwrapping geometry.
            crate::io::reject_rfc7946_value_object(object)
                .map_err(|error| error.with_parse_path(base_path))?;
            crate::io::validate_geojson_feature_members(object)?;
            let geometry_path = format!("{base_path}.geometry");
            let geometry = object.get("geometry").ok_or_else(|| {
                crate::io::IoError::geojson("GeoJSON Feature requires geometry")
                    .with_parse_path(base_path)
            })?;
            if geometry.is_null() {
                return match context {
                    GeoJsonDecodeContext::NullableRow => Ok(None),
                    GeoJsonDecodeContext::GeometryLike => Err(crate::io::IoError::geojson(
                        "Feature has null geometry (an unlocated feature); parse the \
                         FeatureCollection with from_geojson/from_features, where null \
                         geometries become missing rows",
                    )
                    .with_parse_path(&geometry_path)),
                };
            }
            Ok(Some((geometry, geometry_path)))
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
/// read as longitude/latitude downstream. CRS-free input is treated as WGS84
/// coordinates and validated by the writer.
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
