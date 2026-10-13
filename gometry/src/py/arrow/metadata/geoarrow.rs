use super::*;
use crate::py::geoarrow;

/// True when the type exposes a present ExtensionType `extension_name`.
///
/// Missing attribute → not an ExtensionType. A property that raises for any
/// reason other than AttributeError propagates (never swallowed as "absent").
pub(super) fn extension_name_present(value_type: &Bound<'_, PyAny>) -> PyResult<bool> {
    match value_type.getattr("extension_name") {
        Ok(name) => Ok(name.extract::<Option<String>>()?.is_some()),
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyAttributeError>(value_type.py()) => {
            Ok(false)
        },
        Err(err) => Err(err),
    }
}

/// Type-level GeoArrow extension source (ExtensionType API and/or type metadata).
pub(super) fn type_extension_source(
    value_type: &Bound<'_, PyAny>,
) -> PyResult<Option<(String, Vec<u8>)>> {
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
pub(super) fn field_extension_source(
    field: &Bound<'_, PyAny>,
) -> PyResult<Option<(String, Vec<u8>)>> {
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
            let encoding = GeometryEncoding::from_extension_name(&type_src.0);
            let type_frame = parse_geoarrow_extension_metadata_details(&type_src.1, encoding)?;
            let field_frame = parse_geoarrow_extension_metadata_details(&field_src.1, encoding)?;
            if !optional_crs_labels_operationally_equal(
                type_frame.crs.as_deref(),
                field_frame.crs.as_deref(),
            )? || !optional_epochs_equal(type_frame.epoch, field_frame.epoch)
            {
                return Err(geoarrow_parse_error(
                    "conflicting GeoArrow extension metadata between ExtensionType and field",
                ));
            }
            if let (Some(type_definition), Some(field_definition)) = (
                type_frame.crs_definition.as_deref(),
                field_frame.crs_definition.as_deref(),
            ) && type_definition != field_definition
            {
                return Err(geoarrow_parse_error(
                    "conflicting gometry:crs_definition values between ExtensionType and field",
                ));
            }
            let selected =
                if type_frame.crs_definition.is_none() && field_frame.crs_definition.is_some() {
                    field_src
                } else {
                    type_src
                };
            Ok(Some(selected))
        },
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

/// The `edges` member: planar is the only supported interpretation for
/// non-point geometry — a declared spherical-family value would be silently
/// misread as planar, so it rejects rather than degrades. On **point**
/// encodings edge semantics are vacuous (no edges), so spherical-family
/// values are accepted and ignored.
fn parse_geoarrow_edges(
    object: &serde_json::Map<String, Value>,
    encoding: Option<GeometryEncoding>,
) -> PyResult<EdgeModel> {
    match object.get("edges") {
        None => Ok(EdgeModel::Planar),
        Some(Value::String(value)) if value == "planar" => Ok(EdgeModel::Planar),
        Some(Value::String(value))
            if matches!(
                value.as_str(),
                "spherical" | "vincenty" | "thomas" | "andoyer" | "karney"
            ) =>
        {
            if matches!(encoding, Some(GeometryEncoding::Point)) {
                // Point geometry has no edges — accept and treat as planar.
                Ok(EdgeModel::Planar)
            } else {
                Err(geoarrow_parse_error(format!(
                    "invalid GeoArrow extension metadata: edges {value:?} are unsupported; only planar edges are accepted"
                )))
            }
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
    parse_geoarrow_extension_metadata_for(metadata, None)
}

/// Like [`parse_geoarrow_extension_metadata`], with encoding context so
/// vacuous `edges` on point geometry can be accepted.
pub(crate) fn parse_geoarrow_extension_metadata_for(
    metadata: &[u8],
    encoding: Option<GeometryEncoding>,
) -> PyResult<(Option<String>, Option<f64>)> {
    let parsed = parse_geoarrow_extension_metadata_details(metadata, encoding)?;
    Ok((parsed.crs, parsed.epoch))
}

#[derive(Clone, Debug)]
struct ParsedGeoArrowExtensionMetadata {
    crs: Option<String>,
    epoch: Option<f64>,
    crs_definition: Option<String>,
}

fn parse_geoarrow_extension_metadata_details(
    metadata: &[u8],
    encoding: Option<GeometryEncoding>,
) -> PyResult<ParsedGeoArrowExtensionMetadata> {
    if metadata.is_empty() {
        return Ok(ParsedGeoArrowExtensionMetadata {
            crs: None,
            epoch: None,
            crs_definition: None,
        });
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
    let _edges = parse_geoarrow_edges(&object, encoding)?;
    // The CRS is a PROJJSON object (this encoder's output and the GeoArrow
    // recommendation) or an authority string from other producers; both
    // resolve through the CRS engine while retaining the full authored
    // PROJJSON object; an embedded ID is metadata, not a replacement label.
    let standard_crs = match object.get("crs") {
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
        Some(crs @ Value::Object(_)) => Some(normalize_geoarrow_crs(&crs.to_string())?),
        // GeoArrow forbids explicit ``{"crs": null}`` (use absent key for
        // CRS-free). GeoParquet keeps its own omitted-vs-null contract.
        Some(Value::Null) => {
            return Err(geoarrow_parse_error(
                "invalid GeoArrow extension metadata: crs must not be null (omit the key for CRS-free)",
            ));
        },
        None if crs_type.is_none() => None,
        None => {
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
    let crs_definition = match object.get(geoarrow::CRS_DEFINITION_METADATA_KEY) {
        None => None,
        Some(Value::String(definition)) => {
            let Some(standard_crs) = standard_crs.as_deref() else {
                return Err(geoarrow_parse_error(format!(
                    "invalid GeoArrow extension metadata: {} requires a standard crs",
                    geoarrow::CRS_DEFINITION_METADATA_KEY
                )));
            };
            normalize_geoarrow_crs(definition)?;
            if !optional_crs_labels_operationally_equal(Some(standard_crs), Some(definition))? {
                return Err(geoarrow_parse_error(format!(
                    "invalid GeoArrow extension metadata: {} conflicts with standard GeoArrow CRS",
                    geoarrow::CRS_DEFINITION_METADATA_KEY
                )));
            }
            Some(definition.clone())
        },
        Some(_) => {
            return Err(geoarrow_parse_error(format!(
                "invalid GeoArrow extension metadata: {} must be a string",
                geoarrow::CRS_DEFINITION_METADATA_KEY
            )));
        },
    };
    let crs = crs_definition.clone().or(standard_crs);
    let epoch = parse_geoarrow_epoch(&object)?;
    let epoch = crate::deserialized_epoch(epoch, crs.as_deref()).map_err(|error| {
        Python::attach(|py| {
            geoarrow_parse_error(format!(
                "invalid GeoArrow extension metadata: {}",
                error.value(py)
            ))
        })
    })?;
    Ok(ParsedGeoArrowExtensionMetadata {
        crs,
        epoch,
        crs_definition,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EdgeModel {
    Planar,
}

pub(crate) fn geoarrow_parse_error(message: impl Into<String>) -> PyErr {
    parse_error(message, ParseFormat::GeoArrow)
}

pub(crate) fn normalize_geoarrow_crs(reference: &str) -> PyResult<String> {
    crate::crs::normalize(reference)
        .map_err(|error| {
            geoarrow_parse_error(format!("invalid GeoArrow extension metadata: {error}"))
        })
        .map(std::borrow::Cow::into_owned)
}

/// Private Python entry: parse GeoArrow extension metadata bytes → (stored CRS label, epoch).
#[pyfunction]
#[pyo3(name = "_parse_geoarrow_extension_metadata")]
pub(crate) fn py_parse_geoarrow_extension_metadata(
    metadata: &[u8],
) -> PyResult<(Option<String>, Option<f64>)> {
    parse_geoarrow_extension_metadata(metadata)
}
