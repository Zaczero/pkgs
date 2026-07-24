#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

pub(crate) fn arrow_storage_array(
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<ArrowStorage> {
    if crate::py::arrow_c::is_native_arrow_array(value) {
        return native_arrow_storage_array(value);
    }
    let value_type = value.getattr("type")?;
    // Keystone: ExtensionType and field metadata are reconciled in ONE place.
    let type_source = type_extension_source(&value_type)?;
    let field_source = match field {
        Some(field) => field_extension_source(field)?,
        None => None,
    };
    let from_extension_type = type_source.is_some() && extension_name_present(&value_type)?;
    if let Some((extension_name, metadata)) =
        reconcile_extension_sources(type_source, field_source)?
    {
        // Present extension name must classify or error — never fall through.
        let Some(encoding) = GeometryEncoding::from_extension_name(&extension_name) else {
            return Err(PyTypeError::new_err(GeometryEncoding::EXPECTED_EXTENSION));
        };
        // ExtensionType roots expose `.storage`; field-metadata-only arrays are storage.
        let storage = if from_extension_type {
            value.getattr("storage")?
        } else {
            storage_or_self(value)?
        };
        let storage_type = storage.getattr("type")?;
        // Exact encoding-shape gate (also rejects LargeList at each legitimate
        // list level). Do NOT recursively walk the whole type tree first —
        // pathological list nesting would stack-overflow (P01); geometry
        // encodings have trivial depth and are classified exactly below.
        ensure_pyarrow_encoding_storage(pa, &storage_type, encoding)?;
        let wkb_offset_width = if matches!(encoding, GeometryEncoding::Wkb) {
            wkb_offset_width(
                arrow_type_is_large_binary(pa, &storage_type)?,
                arrow_type_is_binary_view(pa, &storage_type)?,
            )
        } else {
            WkbOffsetWidth::Int32
        };
        let (crs, epoch) = parse_geoarrow_extension_metadata(&metadata)?;
        return Ok(ArrowStorage {
            storage: storage.unbind(),
            crs,
            epoch,
            encoding,
            wkb_offset_width,
        });
    }

    let types = pa.getattr("types")?;
    let is_binary = types
        .call_method1("is_binary", (&value_type,))?
        .extract::<bool>()?;
    let is_large_binary = arrow_type_is_large_binary(pa, &value_type)?;
    let is_binary_view = arrow_type_is_binary_view(pa, &value_type)?;
    if is_binary || is_large_binary || is_binary_view {
        return Ok(ArrowStorage {
            storage: value.clone().unbind(),
            crs: None,
            epoch: None,
            encoding: GeometryEncoding::Wkb,
            wkb_offset_width: wkb_offset_width(is_large_binary, is_binary_view),
        });
    }
    Err(PyTypeError::new_err(
        "expected a geoarrow point, multipoint, linestring, multilinestring, polygon, multipolygon, WKB, binary, or large_binary Arrow array",
    ))
}

pub(crate) fn arrow_value_frame(
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Option<String>, Option<f64>)> {
    let value_type = value.getattr("type")?;
    arrow_type_frame(pa, &value_type, field)
}

pub(crate) fn arrow_type_frame(
    pa: &Bound<'_, PyModule>,
    value_type: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Option<String>, Option<f64>)> {
    // Empty/zero-chunk shares the non-empty encoding classifier: admit only
    // supported extension+storage pairs (or bare binary WKB). Same reconciliation
    // keystone as non-empty import (ExtensionType vs field metadata).
    let type_source = type_extension_source(value_type)?;
    let field_source = match field {
        Some(field) => field_extension_source(field)?,
        None => None,
    };
    let from_extension_type = type_source.is_some() && extension_name_present(value_type)?;
    if let Some((extension_name, metadata)) =
        reconcile_extension_sources(type_source, field_source)?
    {
        let Some(encoding) = GeometryEncoding::from_extension_name(&extension_name) else {
            return Err(PyTypeError::new_err(GeometryEncoding::EXPECTED_EXTENSION));
        };
        // ExtensionType: validate storage_type; field-metadata-only: type is storage.
        let storage_type = if from_extension_type {
            match value_type.getattr("storage_type") {
                Ok(storage_type) => storage_type,
                Err(err)
                    if err
                        .is_instance_of::<pyo3::exceptions::PyAttributeError>(value_type.py()) =>
                {
                    value_type.clone()
                },
                Err(err) => return Err(err),
            }
        } else {
            value_type.clone()
        };
        // Exact shape classification only — see arrow_storage_array (P01).
        ensure_pyarrow_encoding_storage(pa, &storage_type, encoding)?;
        return parse_geoarrow_extension_metadata(&metadata);
    }

    let types = pa.getattr("types")?;
    let is_binary = types
        .call_method1("is_binary", (value_type,))?
        .extract::<bool>()?;
    // Zero-chunk BinaryView must match non-empty acceptance (WKB storage).
    if is_binary
        || arrow_type_is_large_binary(pa, value_type)?
        || arrow_type_is_binary_view(pa, value_type)?
    {
        return Ok((None, None));
    }
    Err(PyTypeError::new_err(
        "expected a geoarrow point, multipoint, linestring, multilinestring, polygon, multipolygon, WKB, binary, or large_binary Arrow array",
    ))
}

/// Empty/non-empty shared storage shape gate for PyArrow types.
fn ensure_pyarrow_encoding_storage(
    pa: &Bound<'_, PyModule>,
    storage_type: &Bound<'_, PyAny>,
    encoding: GeometryEncoding,
) -> PyResult<()> {
    let types = pa.getattr("types")?;
    match encoding {
        GeometryEncoding::Wkb => {
            let ok = types
                .call_method1("is_binary", (storage_type,))?
                .extract::<bool>()?
                || arrow_type_is_large_binary(pa, storage_type)?
                || arrow_type_is_binary_view(pa, storage_type)?;
            if !ok {
                return Err(PyTypeError::new_err(
                    "geoarrow.wkb storage must be binary, large_binary, or binary_view",
                ));
            }
            Ok(())
        },
        GeometryEncoding::Point => ensure_pyarrow_point_struct(pa, storage_type),
        GeometryEncoding::MultiPoint | GeometryEncoding::LineString => {
            ensure_pyarrow_list(pa, storage_type)?;
            let inner = storage_type.getattr("value_type")?;
            ensure_pyarrow_point_struct(pa, &inner)
        },
        GeometryEncoding::MultiLineString | GeometryEncoding::Polygon => {
            ensure_pyarrow_list(pa, storage_type)?;
            let inner = storage_type.getattr("value_type")?;
            ensure_pyarrow_list(pa, &inner)?;
            let point = inner.getattr("value_type")?;
            ensure_pyarrow_point_struct(pa, &point)
        },
        GeometryEncoding::MultiPolygon => {
            ensure_pyarrow_list(pa, storage_type)?;
            let l1 = storage_type.getattr("value_type")?;
            ensure_pyarrow_list(pa, &l1)?;
            let l2 = l1.getattr("value_type")?;
            ensure_pyarrow_list(pa, &l2)?;
            let point = l2.getattr("value_type")?;
            ensure_pyarrow_point_struct(pa, &point)
        },
    }
}

fn ensure_pyarrow_list(pa: &Bound<'_, PyModule>, value_type: &Bound<'_, PyAny>) -> PyResult<()> {
    if arrow_type_is_large_list(pa, value_type)? {
        return Err(PyTypeError::new_err(
            crate::py::arrow_c::LARGE_LIST_UNSUPPORTED,
        ));
    }
    let types = pa.getattr("types")?;
    let is_list = if types.hasattr("is_list")? {
        types
            .call_method1("is_list", (value_type,))?
            .extract::<bool>()?
    } else {
        false
    };
    if !is_list {
        return Err(PyTypeError::new_err(
            "geoarrow list geometry storage must be list (+l) with i32 offsets",
        ));
    }
    Ok(())
}

fn ensure_pyarrow_point_struct(
    pa: &Bound<'_, PyModule>,
    value_type: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let types = pa.getattr("types")?;
    let is_struct = if types.hasattr("is_struct")? {
        types
            .call_method1("is_struct", (value_type,))?
            .extract::<bool>()?
    } else {
        false
    };
    if !is_struct {
        return Err(PyTypeError::new_err(
            "geoarrow point storage must be a struct of float64 ordinates",
        ));
    }
    // Mandatory shared classifier: exact x/y, at most z/m, float64 leaves only.
    let names: Vec<String> = value_type.getattr("names")?.extract()?;
    let fields_attr = value_type.getattr("fields")?;
    if fields_attr.len()? != names.len() {
        return Err(PyTypeError::new_err(
            "geoarrow point struct field names and fields length disagree",
        ));
    }
    let mut fields = Vec::with_capacity(names.len());
    for (index, name) in names.iter().enumerate() {
        let field_type = fields_attr.get_item(index)?.getattr("type")?;
        let is_float64 = pyarrow_type_is_float64(&types, &field_type)?;
        fields.push((name.as_str(), is_float64));
    }
    crate::py::geoarrow::classify_geoarrow_ordinates(fields).map_err(PyTypeError::new_err)?;
    Ok(())
}

fn pyarrow_type_is_float64(
    types: &Bound<'_, PyAny>,
    field_type: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    if types.hasattr("is_float64")? {
        return types
            .call_method1("is_float64", (field_type,))?
            .extract::<bool>();
    }
    // Fallback: Arrow format token `g` is float64.
    Ok(field_type
        .getattr("format")
        .ok()
        .and_then(|f| f.extract::<String>().ok())
        .is_some_and(|f| f == "g"))
}

pub(crate) fn arrow_extension_metadata(
    value_type: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<(String, Vec<u8>)>> {
    let type_source = type_extension_source(value_type)?;
    let field_source = match field {
        Some(field) => field_extension_source(field)?,
        None => None,
    };
    reconcile_extension_sources(type_source, field_source)
}

/// True when the type exposes a present ExtensionType `extension_name`.
///
/// Missing attribute → not an ExtensionType. A property that raises for any
/// reason other than AttributeError propagates (never swallowed as "absent").
fn extension_name_present(value_type: &Bound<'_, PyAny>) -> PyResult<bool> {
    match value_type.getattr("extension_name") {
        Ok(name) => Ok(name.extract::<Option<String>>()?.is_some()),
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyAttributeError>(value_type.py()) => {
            Ok(false)
        },
        Err(err) => Err(err),
    }
}

/// Storage array for field-metadata-only geometry columns: prefer `.storage`
/// when present; otherwise the value itself is storage.
///
/// - `AttributeError` → not an ExtensionType root (use `value`)
/// - native non-extension `_NativeArrowArray.storage` raises `TypeError`
///   ("not an extension array") with the same meaning
/// - any other exception propagates
fn storage_or_self<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    match value.getattr("storage") {
        Ok(storage) => Ok(storage),
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyAttributeError>(value.py()) => {
            Ok(value.clone())
        },
        Err(err)
            if crate::py::arrow_c::is_native_arrow_array(value)
                && err.is_instance_of::<pyo3::exceptions::PyTypeError>(value.py()) =>
        {
            Ok(value.clone())
        },
        Err(err) => Err(err),
    }
}

/// Type-level GeoArrow extension source (ExtensionType API and/or type metadata).
fn type_extension_source(value_type: &Bound<'_, PyAny>) -> PyResult<Option<(String, Vec<u8>)>> {
    // PyArrow ExtensionType: extension_name + __arrow_ext_serialize__.
    if let Ok(extension_name) = value_type.getattr("extension_name")
        && let Some(extension_name) = extension_name.extract::<Option<String>>()?
    {
        let metadata = if let Ok(serialize) = value_type.getattr("__arrow_ext_serialize__") {
            serialize.call0()?.extract::<Vec<u8>>()?
        } else {
            metadata_bytes(value_type, "ARROW:extension:metadata")?.unwrap_or_default()
        };
        return Ok(Some((extension_name, metadata)));
    }
    if let Some(extension_name) = arrow_extension_name_attr(value_type)? {
        let metadata = if let Ok(serialize) = value_type.getattr("__arrow_ext_serialize__") {
            serialize.call0()?.extract::<Vec<u8>>()?
        } else {
            metadata_bytes(value_type, "ARROW:extension:metadata")?.unwrap_or_default()
        };
        return Ok(Some((extension_name, metadata)));
    }
    if let Some(extension_name) = metadata_string(value_type, "ARROW:extension:name")?
        .or(metadata_string(value_type, "__arrow_ext_name__")?)
    {
        let metadata = metadata_bytes(value_type, "ARROW:extension:metadata")?.unwrap_or_default();
        return Ok(Some((extension_name, metadata)));
    }
    Ok(None)
}

/// Field-level `ARROW:extension:*` metadata source (schema field only).
fn field_extension_source(field: &Bound<'_, PyAny>) -> PyResult<Option<(String, Vec<u8>)>> {
    if let Some(extension_name) = metadata_string(field, "ARROW:extension:name")?
        .or(metadata_string(field, "__arrow_ext_name__")?)
    {
        let metadata = metadata_bytes(field, "ARROW:extension:metadata")?.unwrap_or_default();
        return Ok(Some((extension_name, metadata)));
    }
    Ok(None)
}

/// Keystone: reconcile ExtensionType and field extension metadata.
///
/// A single source wins. When both are present, encoding name and semantic
/// frame (CRS + epoch after parse/normalize) must agree — ambiguity is an
/// error, never a silent pick. Identical sources prefer the type-level bytes.
pub(crate) fn reconcile_extension_sources(
    type_source: Option<(String, Vec<u8>)>,
    field_source: Option<(String, Vec<u8>)>,
) -> PyResult<Option<(String, Vec<u8>)>> {
    match (type_source, field_source) {
        (None, None) => Ok(None),
        (Some(only), None) | (None, Some(only)) => Ok(Some(only)),
        (Some(type_src), Some(field_src)) => {
            if type_src.0 != field_src.0 {
                return Err(geoarrow_parse_error(format!(
                    "conflicting GeoArrow extension names: type declares '{}' but field metadata declares '{}'",
                    type_src.0, field_src.0
                )));
            }
            let type_frame = parse_geoarrow_extension_metadata(&type_src.1)?;
            let field_frame = parse_geoarrow_extension_metadata(&field_src.1)?;
            if type_frame.0 != field_frame.0 || !extension_epochs_match(type_frame.1, field_frame.1)
            {
                return Err(geoarrow_parse_error(
                    "conflicting GeoArrow extension metadata between ExtensionType and field",
                ));
            }
            Ok(Some(type_src))
        },
    }
}

fn extension_epochs_match(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => crate::boundary::metadata::epochs_equal(left, right),
        _ => false,
    }
}

pub(crate) fn arrow_extension_name_attr(value_type: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    let Ok(attr) = value_type.getattr("__arrow_ext_name__") else {
        return Ok(None);
    };
    if attr.is_callable() {
        attr.call0()?.extract().map(Some)
    } else {
        attr.extract().map(Some)
    }
}

pub(crate) fn metadata_string(owner: &Bound<'_, PyAny>, key: &str) -> PyResult<Option<String>> {
    let Some(value) = metadata_value(owner, key)? else {
        return Ok(None);
    };
    if let Ok(bytes) = value.extract::<Vec<u8>>() {
        if key == "ARROW:extension:name" || key == "__arrow_ext_name__" {
            return decode_extension_name(bytes).map(Some);
        }
        return String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| geoarrow_parse_error("Arrow extension metadata key is not UTF-8"));
    }
    value.extract::<String>().map(Some)
}

/// Decode the reserved Arrow extension-name value consistently at every
/// ingress boundary.  A present-but-malformed name is never equivalent to an
/// absent name: doing so would silently admit it as bare WKB.
pub(crate) fn decode_extension_name(raw: Vec<u8>) -> PyResult<String> {
    String::from_utf8(raw)
        .map_err(|_| geoarrow_parse_error("Arrow extension name metadata is not UTF-8"))
}

pub(crate) fn metadata_bytes(owner: &Bound<'_, PyAny>, key: &str) -> PyResult<Option<Vec<u8>>> {
    let Some(value) = metadata_value(owner, key)? else {
        return Ok(None);
    };
    if let Ok(bytes) = value.extract::<Vec<u8>>() {
        return Ok(Some(bytes));
    }
    value
        .extract::<String>()
        .map(|value| Some(value.into_bytes()))
}

pub(crate) fn metadata_value<'py>(
    owner: &Bound<'py, PyAny>,
    key: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let Ok(metadata) = owner.getattr("metadata") else {
        return Ok(None);
    };
    if metadata.is_none() {
        return Ok(None);
    }
    let bytes_key = PyBytes::new(owner.py(), key.as_bytes());
    let value = metadata.call_method1("get", (bytes_key,))?;
    if !value.is_none() {
        return Ok(Some(value));
    }
    let value = metadata.call_method1("get", (key,))?;
    if value.is_none() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

/// The `edges` member: planar is the only supported interpretation — a
/// declared spherical-family value is real metadata that would be silently
/// misread as planar, so it rejects rather than degrades.
fn parse_geoarrow_edges(object: &serde_json::Map<String, Value>) -> PyResult<EdgeModel> {
    match object.get("edges") {
        None => Ok(EdgeModel::Planar),
        Some(Value::String(value)) if value == "planar" => Ok(EdgeModel::Planar),
        Some(Value::String(value))
            if matches!(
                value.as_str(),
                "spherical" | "vincenty" | "thomas" | "andoyer" | "karney"
            ) =>
        {
            Err(geoarrow_parse_error(format!(
                "invalid GeoArrow extension metadata: edges {value:?} are unsupported; only planar edges are accepted"
            )))
        },
        Some(value) => Err(geoarrow_parse_error(format!(
            "invalid GeoArrow extension metadata: unknown edges value {value}"
        ))),
    }
}

/// The `epoch` member: a present value must be numeric — anything else is
/// malformed metadata, never "epoch absent".
fn parse_geoarrow_epoch(object: &serde_json::Map<String, Value>) -> PyResult<Option<f64>> {
    match object.get("epoch") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => Ok(Some(number.as_f64().ok_or_else(|| {
            geoarrow_parse_error("invalid GeoArrow extension metadata: epoch must be finite")
        })?)),
        Some(_) => Err(geoarrow_parse_error(
            "invalid GeoArrow extension metadata: epoch must be a number",
        )),
    }
}

pub(crate) fn parse_geoarrow_extension_metadata(
    metadata: &[u8],
) -> PyResult<(Option<String>, Option<f64>)> {
    if metadata.is_empty() {
        return Ok((None, None));
    }
    let value = serde_json::from_slice::<Value>(metadata).map_err(|error| {
        geoarrow_parse_error(format!("invalid GeoArrow extension metadata: {error}"))
    })?;
    let Value::Object(object) = value else {
        return Err(geoarrow_parse_error(
            "invalid GeoArrow extension metadata: expected a JSON object",
        ));
    };
    let crs_type = match object.get("crs_type") {
        None => None,
        Some(Value::String(value))
            if matches!(
                value.as_str(),
                "projjson" | "wkt2:2019" | "authority_code" | "srid"
            ) =>
        {
            Some(value.as_str())
        },
        Some(value) => {
            return Err(geoarrow_parse_error(format!(
                "invalid GeoArrow extension metadata: unknown crs_type {value}"
            )));
        },
    };
    let _edges = parse_geoarrow_edges(&object)?;
    // The CRS is a PROJJSON object (this encoder's output and the GeoArrow
    // recommendation) or an authority string from other producers; both
    // canonicalize through the CRS engine. PROJJSON declaring an identity
    // canonicalizes to its authority:code rather than the full definition.
    let crs = match object.get("crs") {
        Some(Value::String(_)) if crs_type == Some("projjson") => {
            return Err(geoarrow_parse_error(
                "invalid GeoArrow extension metadata: crs_type projjson requires an object crs",
            ));
        },
        Some(Value::String(_)) if crs_type == Some("srid") => {
            return Err(geoarrow_parse_error(
                "invalid GeoArrow extension metadata: crs_type srid requires out-of-band CRS resolution",
            ));
        },
        Some(Value::String(crs)) => Some(normalize_geoarrow_crs(crs)?),
        Some(Value::Object(_))
            if matches!(crs_type, Some("wkt2:2019" | "authority_code" | "srid")) =>
        {
            return Err(geoarrow_parse_error(format!(
                "invalid GeoArrow extension metadata: crs_type {} requires a string crs",
                crs_type.expect("matched a present crs_type")
            )));
        },
        Some(crs @ Value::Object(object)) => {
            let identity = object.get("id").and_then(|id| {
                let authority = id.get("authority")?.as_str()?;
                let code = id.get("code")?;
                let code = code
                    .as_i64()
                    .map(|value| value.to_string())
                    .or_else(|| code.as_str().map(ToOwned::to_owned))?;
                Some(format!("{authority}:{code}"))
            });
            let reference = identity.unwrap_or_else(|| crs.to_string());
            Some(normalize_geoarrow_crs(&reference)?)
        },
        Some(Value::Null) | None if crs_type.is_none() => None,
        Some(Value::Null) | None => {
            return Err(geoarrow_parse_error(
                "invalid GeoArrow extension metadata: crs_type requires crs",
            ));
        },
        Some(_) => {
            return Err(geoarrow_parse_error(
                "invalid GeoArrow extension metadata: crs must be a string or object",
            ));
        },
    };
    let epoch = parse_geoarrow_epoch(&object)?;
    let epoch = crate::deserialized_epoch(epoch, crs.as_deref()).map_err(|error| {
        Python::attach(|py| {
            geoarrow_parse_error(format!(
                "invalid GeoArrow extension metadata: {}",
                error.value(py)
            ))
        })
    })?;
    Ok((crs, epoch))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EdgeModel {
    Planar,
}

pub(crate) fn geoarrow_parse_error(message: impl Into<String>) -> PyErr {
    parse_error(message, ParseFormat::GeoArrow)
}

pub(crate) fn normalize_geoarrow_crs(reference: &str) -> PyResult<String> {
    crate::crs::normalize(reference).map_err(|error| {
        geoarrow_parse_error(format!("invalid GeoArrow extension metadata: {error}"))
    })
}

/// Private Python entry: parse GeoArrow extension metadata bytes → (crs, epoch).
#[pyfunction]
#[pyo3(name = "_parse_geoarrow_extension_metadata")]
pub(crate) fn py_parse_geoarrow_extension_metadata(
    metadata: &[u8],
) -> PyResult<(Option<String>, Option<f64>)> {
    parse_geoarrow_extension_metadata(metadata)
}

/// Private Python entry: parse one GeoParquet column metadata object for the
/// shared CRS/epoch/edges frame (defaults missing CRS to OGC:CRS84; CRS must
/// be a PROJJSON object or null when present).
#[pyfunction]
#[pyo3(name = "_parse_geoparquet_column_frame", signature = (metadata, column_name))]
pub(crate) fn py_parse_geoparquet_column_frame(
    metadata: &[u8],
    column_name: &str,
) -> PyResult<(Option<String>, Option<f64>)> {
    parse_geoparquet_column_frame(metadata, column_name)
}

fn geoparquet_column_error(column_name: &str, detail: impl std::fmt::Display) -> PyErr {
    crate::py::errors::parse_error(
        format!(
            "malformed GeoParquet geo metadata: invalid GeoParquet column '{column_name}': {detail}"
        ),
        crate::py::errors::ParseFormat::GeoParquet,
    )
}

/// GeoParquet column-level CRS/epoch/edges boundary (frame only — encoding,
/// geometry_types, orientation, and bbox stay in the Python reader).
pub(crate) fn parse_geoparquet_column_frame(
    metadata: &[u8],
    column_name: &str,
) -> PyResult<(Option<String>, Option<f64>)> {
    let value = if metadata.is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_slice::<Value>(metadata).map_err(|error| {
            geoparquet_column_error(column_name, format!("invalid JSON: {error}"))
        })?
    };
    let Value::Object(object) = value else {
        return Err(geoparquet_column_error(
            column_name,
            "metadata must be a JSON object",
        ));
    };

    // edges: planar only; spherical is real unsupported metadata; other tokens unknown.
    match object.get("edges") {
        None => {},
        Some(Value::String(value)) if value == "planar" => {},
        Some(Value::String(value)) if value == "spherical" => {
            return Err(geoparquet_column_error(
                column_name,
                format!("edges '{value}' are unsupported; only planar edges are accepted"),
            ));
        },
        Some(Value::String(value)) => {
            return Err(geoparquet_column_error(
                column_name,
                format!("edges has an unknown value '{value}'"),
            ));
        },
        Some(value) => {
            return Err(geoparquet_column_error(
                column_name,
                format!("edges has an unknown value {value}"),
            ));
        },
    }

    // CRS: absent → OGC:CRS84; present null → CRS-free; present object → PROJJSON;
    // present string is rejected (GeoParquet 1.x requires PROJJSON or null).
    let crs = match object.get("crs") {
        None => Some(normalize_geoparquet_crs("OGC:CRS84", column_name)?),
        Some(Value::Null) => None,
        Some(crs @ Value::Object(projjson)) => {
            let identity = projjson.get("id").and_then(|id| {
                let authority = id.get("authority")?.as_str()?;
                let code = id.get("code")?;
                let code = code
                    .as_i64()
                    .map(|value| value.to_string())
                    .or_else(|| code.as_str().map(ToOwned::to_owned))?;
                Some(format!("{authority}:{code}"))
            });
            let reference = identity.unwrap_or_else(|| crs.to_string());
            Some(normalize_geoparquet_crs(&reference, column_name)?)
        },
        Some(_) => {
            return Err(geoparquet_column_error(
                column_name,
                "crs must be a PROJJSON object or null",
            ));
        },
    };

    let epoch = match object.get("epoch") {
        None | Some(Value::Null) => None,
        Some(Value::Number(number)) => Some(
            number
                .as_f64()
                .ok_or_else(|| geoparquet_column_error(column_name, "epoch must be finite"))?,
        ),
        Some(_) => {
            return Err(geoparquet_column_error(
                column_name,
                "epoch must be a number",
            ));
        },
    };
    let epoch = match epoch {
        None => None,
        Some(value) if value.is_finite() => Some(if value == 0.0 { 0.0 } else { value }),
        Some(_) => {
            return Err(geoparquet_column_error(column_name, "epoch must be finite"));
        },
    };
    if epoch.is_some() && crs.is_none() {
        return Err(geoparquet_column_error(column_name, "epoch requires crs"));
    }
    Ok((crs, epoch))
}

fn normalize_geoparquet_crs(reference: &str, column_name: &str) -> PyResult<String> {
    crate::crs::normalize(reference)
        .map_err(|error| geoparquet_column_error(column_name, format!("{error}")))
}

pub(crate) fn arrow_extension_meta(
    value_type: &Bound<'_, PyAny>,
) -> PyResult<(Option<String>, Option<f64>)> {
    let metadata = value_type
        .call_method0("__arrow_ext_serialize__")?
        .extract::<Vec<u8>>()?;
    parse_geoarrow_extension_metadata(&metadata)
}

pub(crate) fn arrow_type_is_large_binary(
    pa: &Bound<'_, PyModule>,
    value_type: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    if let Ok(format) = value_type.getattr("format")
        && let Ok(format) = format.extract::<String>()
    {
        return Ok(format == "Z");
    }
    pa.getattr("types")?
        .call_method1("is_large_binary", (value_type,))?
        .extract()
}

fn arrow_type_is_large_list(
    pa: &Bound<'_, PyModule>,
    value_type: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    if let Ok(format) = value_type.getattr("format")
        && let Ok(format) = format.extract::<String>()
    {
        // Same token check as reject_large_list_format / ArrowFormat::parse.
        return Ok(format == "+L");
    }
    let types = pa.getattr("types")?;
    if types.hasattr("is_large_list")? {
        return types
            .call_method1("is_large_list", (value_type,))?
            .extract();
    }
    // Fallback: type name / class name for older pyarrow.
    if let Ok(name) = value_type.get_type().name() {
        return Ok(name == "LargeListType");
    }
    Ok(false)
}

fn arrow_type_is_binary_view(
    pa: &Bound<'_, PyModule>,
    value_type: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    if let Ok(format) = value_type.getattr("format")
        && let Ok(format) = format.extract::<String>()
    {
        return Ok(format == "vz");
    }
    let types = pa.getattr("types")?;
    if !types.hasattr("is_binary_view")? {
        return Ok(false);
    }
    types
        .call_method1("is_binary_view", (value_type,))?
        .extract()
}

pub(crate) fn native_arrow_storage_array(value: &Bound<'_, PyAny>) -> PyResult<ArrowStorage> {
    // Prefer the shared schema classifier (same admission as stream/empty)
    // before any coordinate buffer is read.
    if let Some(storage) = crate::py::arrow_c::native_arrow_storage_via_classifier(value)? {
        return Ok(storage);
    }
    let value_type = value.getattr("type")?;
    if let Ok(extension_name) = value_type.getattr("extension_name")
        && let Some(extension_name) = extension_name.extract::<Option<String>>()?
    {
        // Present name must classify or error — never fall through.
        let Some(encoding) = GeometryEncoding::from_extension_name(&extension_name) else {
            return Err(PyTypeError::new_err(GeometryEncoding::EXPECTED_EXTENSION));
        };
        let storage = value.getattr("storage")?;
        crate::py::arrow_c::validate_native_encoding_storage(&storage, encoding)?;
        let wkb_offset_width = if matches!(encoding, GeometryEncoding::Wkb) {
            wkb_offset_width(
                crate::py::arrow_c::native_schema_format_is_large_binary(&storage)?,
                crate::py::arrow_c::native_schema_format_is_binary_view(&storage)?,
            )
        } else {
            WkbOffsetWidth::Int32
        };
        let (crs, epoch) = arrow_extension_meta(&value_type)?;
        return Ok(ArrowStorage {
            storage: storage.unbind(),
            crs,
            epoch,
            encoding,
            wkb_offset_width,
        });
    }
    if let Some((extension_name, metadata)) = arrow_extension_metadata(&value_type, None)? {
        let Some(encoding) = GeometryEncoding::from_extension_name(&extension_name) else {
            return Err(PyTypeError::new_err(GeometryEncoding::EXPECTED_EXTENSION));
        };
        let storage = storage_or_self(value)?;
        crate::py::arrow_c::validate_native_encoding_storage(&storage, encoding)?;
        let wkb_offset_width = if matches!(encoding, GeometryEncoding::Wkb) {
            wkb_offset_width(
                crate::py::arrow_c::native_schema_format_is_large_binary(&storage)?,
                crate::py::arrow_c::native_schema_format_is_binary_view(&storage)?,
            )
        } else {
            WkbOffsetWidth::Int32
        };
        let (crs, epoch) = parse_geoarrow_extension_metadata(&metadata)?;
        return Ok(ArrowStorage {
            storage: storage.unbind(),
            crs,
            epoch,
            encoding,
            wkb_offset_width,
        });
    }
    if crate::py::arrow_c::native_schema_format_is_binary(value)? {
        let is_large_binary = crate::py::arrow_c::native_schema_format_is_large_binary(value)?;
        let is_binary_view = crate::py::arrow_c::native_schema_format_is_binary_view(value)?;
        return Ok(ArrowStorage {
            storage: value.clone().unbind(),
            crs: None,
            epoch: None,
            encoding: GeometryEncoding::Wkb,
            wkb_offset_width: wkb_offset_width(is_large_binary, is_binary_view),
        });
    }
    Err(PyTypeError::new_err(
        "expected a geoarrow point, multipoint, linestring, multilinestring, polygon, multipolygon, WKB, binary, or large_binary Arrow array",
    ))
}

const fn wkb_offset_width(is_large_binary: bool, is_binary_view: bool) -> WkbOffsetWidth {
    if is_binary_view {
        WkbOffsetWidth::View
    } else if is_large_binary {
        WkbOffsetWidth::Int64
    } else {
        WkbOffsetWidth::Int32
    }
}

/// Build [`ArrowStorage`] from a native geometry array already classified by
/// the shared stream/empty path (encoding + frame known from the schema plan).
///
/// For extension roots, prefers the array itself as storage (native extension
/// arrays return `self` from `.storage`); for bare binary WKB, the value is
/// storage. CRS/epoch come from the classifier so table field metadata is not
/// lost when the native type previously read only the struct root.
pub(crate) fn arrow_storage_from_native_geometry(
    value: &Bound<'_, PyAny>,
    encoding: GeometryEncoding,
    wkb_offset_width: WkbOffsetWidth,
    crs: Option<String>,
    epoch: Option<f64>,
) -> PyResult<ArrowStorage> {
    // Nested geoarrow or WKB: use `.storage` when present (extension root), else self.
    let storage = storage_or_self(value)?.unbind();
    Ok(ArrowStorage {
        storage,
        crs,
        epoch,
        encoding,
        wkb_offset_width,
    })
}
