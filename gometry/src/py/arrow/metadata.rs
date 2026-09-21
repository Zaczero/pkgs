use pyo3::types::{PyBytes, PyTuple, PyTupleMethods as _};
use pyo3::{IntoPyObject as _, pyclass, pyfunction, pymethods};
use serde_json::Value;

use crate::boundary::metadata::{optional_crs_labels_operationally_equal, optional_epochs_equal};
use crate::py::arrow::{
    Bound, GeometryEncoding, Py, PyAny, PyAnyMethods as _, PyErr, PyModule, PyResult, PyTypeError,
    PyTypeMethods as _, Python, WkbOffsetWidth,
};
use crate::py::arrow_c::NativeArrowClassification;
use crate::py::errors::{GeometryError, ParseFormat, parse_error};
use crate::py::support::JsonValueError;
use crate::py::{arrow, errors, geoarrow, support};

/// One native admission result for a GeoParquet geometry column.
///
/// The record is produced from the file schema before any materialized column
/// is decoded.  Its encoding, physical nesting, ordinate fields, and embedded
/// frame are immutable facts carried through the Python adapter; decoding must
/// not infer them again from a post-read PyArrow value.
#[pyclass(
    name = "_GeoParquetPhysicalLayout",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
pub(crate) struct GeoParquetPhysicalLayout {
    encoding: GeometryEncoding,
    wkb_offset_width: WkbOffsetWidth,
    has_extension: bool,
    embedded_crs: Option<String>,
    embedded_epoch: Option<f64>,
    list_depth: Option<u8>,
    has_z: bool,
    has_m: bool,
}

impl GeoParquetPhysicalLayout {
    pub(crate) fn classification(
        &self,
        crs: Option<String>,
        epoch: Option<f64>,
    ) -> NativeArrowClassification {
        NativeArrowClassification::new(self.encoding, self.wkb_offset_width, crs, epoch)
    }

    const fn native(
        encoding: GeometryEncoding,
        has_extension: bool,
        embedded_crs: Option<String>,
        embedded_epoch: Option<f64>,
        list_depth: u8,
        has_z: bool,
        has_m: bool,
    ) -> Self {
        Self {
            encoding,
            wkb_offset_width: WkbOffsetWidth::Int32,
            has_extension,
            embedded_crs,
            embedded_epoch,
            list_depth: Some(list_depth),
            has_z,
            has_m,
        }
    }

    const fn wkb(
        has_extension: bool,
        embedded_crs: Option<String>,
        embedded_epoch: Option<f64>,
        wkb_offset_width: WkbOffsetWidth,
    ) -> Self {
        Self {
            encoding: GeometryEncoding::Wkb,
            wkb_offset_width,
            has_extension,
            embedded_crs,
            embedded_epoch,
            list_depth: None,
            has_z: false,
            has_m: false,
        }
    }
}

#[pymethods]
impl GeoParquetPhysicalLayout {
    /// Canonical GeoParquet encoding label admitted for the field.
    ///
    /// Returns
    /// -------
    /// str
    ///     ``'WKB'`` for binary storage, or the lower-case native kind label:
    ///     ``'point'``, ``'multipoint'``, ``'linestring'``,
    ///     ``'multilinestring'``, ``'polygon'``, or ``'multipolygon'``.
    #[getter]
    const fn encoding(&self) -> &'static str {
        match self.encoding {
            GeometryEncoding::Point => "point",
            GeometryEncoding::MultiPoint => "multipoint",
            GeometryEncoding::LineString => "linestring",
            GeometryEncoding::MultiLineString => "multilinestring",
            GeometryEncoding::Polygon => "polygon",
            GeometryEncoding::MultiPolygon => "multipolygon",
            GeometryEncoding::Wkb => "WKB",
        }
    }

    /// Whether the field carried reconciled GeoArrow extension metadata.
    ///
    /// Returns
    /// -------
    /// bool
    ///     ``True`` when extension metadata was found on the Arrow type or
    ///     field and reconciled; ``False`` for plain or untyped storage.
    #[getter]
    const fn has_extension(&self) -> bool {
        self.has_extension
    }

    /// Normalized CRS from the field's own GeoArrow extension metadata.
    ///
    /// Returns
    /// -------
    /// str or None
    ///     The embedded CRS label when the extension declares one; ``None``
    ///     when the extension metadata omits a CRS. This is not the caller's
    ///     GeoParquet column declaration.
    #[getter]
    fn embedded_crs(&self) -> Option<String> {
        self.embedded_crs.clone()
    }

    /// Coordinate epoch from the field's own GeoArrow extension metadata.
    ///
    /// Returns
    /// -------
    /// float or None
    ///     A finite decimal year when embedded metadata declares an epoch;
    ///     ``None`` when it omits one. This is not the caller's declaration.
    #[getter]
    const fn embedded_epoch(&self) -> Option<f64> {
        self.embedded_epoch
    }

    /// Native GeoArrow list levels above coordinate storage.
    ///
    /// Returns
    /// -------
    /// int or None
    ///     The number of nested list levels above the coordinate struct or
    ///     interleaved coordinate list; ``None`` for WKB storage.
    #[getter]
    const fn list_depth(&self) -> Option<u8> {
        self.list_depth
    }

    /// Whether admitted native coordinate storage contains a Z ordinate field.
    ///
    /// Returns
    /// -------
    /// bool
    ///     ``True`` when the terminal native coordinate storage has a Z field;
    ///     ``False`` for XY storage and WKB, whose dimensions live in payloads.
    #[getter]
    const fn has_z(&self) -> bool {
        self.has_z
    }

    /// Whether admitted native coordinate storage contains an M ordinate field.
    ///
    /// Returns
    /// -------
    /// bool
    ///     ``True`` when the terminal native coordinate storage has an M field;
    ///     ``False`` for XY/Z storage and WKB, whose dimensions live in payloads.
    #[getter]
    const fn has_m(&self) -> bool {
        self.has_m
    }

    /// WKB binary offset representation selected for admitted storage.
    ///
    /// Returns
    /// -------
    /// str
    ///     ``'int32'`` for ``Binary``, ``'int64'`` for ``LargeBinary``, or
    ///     ``'view'`` for ``BinaryView``. Native encodings report ``'int32'``.
    #[getter]
    const fn wkb_offset_width(&self) -> &'static str {
        match self.wkb_offset_width {
            WkbOffsetWidth::Int32 => "int32",
            WkbOffsetWidth::Int64 => "int64",
            WkbOffsetWidth::View => "view",
        }
    }

    /// Retain the historical private `(has_extension, crs, epoch)` unpacking
    /// contract while exposing the full record through named properties.
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = (
            self.has_extension,
            self.embedded_crs.clone(),
            self.embedded_epoch,
        )
            .into_pyobject(py)?;
        values.call_method0("__iter__").map(Bound::unbind)
    }
}

/// Typed failure categories at the GeoParquet physical-layout boundary.
///
/// The Python projection deliberately retains its historical exception
/// classes: layout failures are `GeometryError`, malformed or contradictory
/// metadata is `ParseError(format="geoparquet")`, and native allocation
/// failures remain `MemoryError`.
#[derive(Debug)]
enum GeoParquetAdmissionError {
    Layout(String),
    Metadata(String),
    Resource(crate::error::Error),
}

impl GeoParquetAdmissionError {
    fn layout_from_py(error: PyErr) -> Self {
        Python::attach(|py| {
            if error.is_instance_of::<pyo3::exceptions::PyMemoryError>(py) {
                Self::Resource(crate::error::ResourceError::Allocation.into())
            } else {
                Self::Layout(error.value(py).to_string())
            }
        })
    }

    fn into_py_err(self) -> PyErr {
        match self {
            Self::Layout(detail) => geoparquet_layout_error(detail),
            Self::Metadata(detail) => geoparquet_meta_error(detail),
            Self::Resource(error) => geoparquet_resource_error(error),
        }
    }
}

impl From<PyErr> for GeoParquetAdmissionError {
    fn from(error: PyErr) -> Self {
        Python::attach(|py| {
            if error.is_instance_of::<pyo3::exceptions::PyMemoryError>(py) {
                Self::Resource(crate::error::ResourceError::Allocation.into())
            } else {
                Self::Metadata(error.value(py).to_string())
            }
        })
    }
}

impl From<crate::error::Error> for GeoParquetAdmissionError {
    fn from(error: crate::error::Error) -> Self {
        if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
            Self::Resource(error)
        } else {
            Self::Metadata(error.to_string())
        }
    }
}

pub(crate) fn arrow_classification(
    pa: &Bound<'_, PyModule>,
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<NativeArrowClassification> {
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
        // ExtensionType roots expose storage_type; field-metadata-only arrays
        // already are physical storage. Classification is type-only so it can
        // happen before the selected value's Arrow-C protocol is consumed.
        let storage_type = if from_extension_type {
            value_type.getattr("storage_type")?
        } else {
            value_type.clone()
        };
        // Exact encoding-shape gate (list and large_list at each legitimate
        // list level; interleaved FixedSizeList coords). Do NOT recursively
        // walk the whole type tree first — pathological list nesting would
        // stack-overflow (P01); geometry encodings have trivial depth and are
        // classified exactly below.
        ensure_pyarrow_encoding_storage(pa, &storage_type, encoding)?;
        let wkb_offset_width = if matches!(encoding, GeometryEncoding::Wkb) {
            wkb_offset_width(
                arrow_type_is_large_binary(pa, &storage_type)?,
                arrow_type_is_binary_view(pa, &storage_type)?,
            )
        } else {
            WkbOffsetWidth::Int32
        };
        let (crs, epoch) = parse_geoarrow_extension_metadata_for(&metadata, Some(encoding))?;
        return Ok(NativeArrowClassification::new(
            encoding,
            wkb_offset_width,
            crs,
            epoch,
        ));
    }

    let types = pa.getattr("types")?;
    let is_binary = types
        .call_method1("is_binary", (&value_type,))?
        .extract::<bool>()?;
    let is_large_binary = arrow_type_is_large_binary(pa, &value_type)?;
    let is_binary_view = arrow_type_is_binary_view(pa, &value_type)?;
    if is_binary || is_large_binary || is_binary_view {
        return Ok(NativeArrowClassification::new(
            GeometryEncoding::Wkb,
            wkb_offset_width(is_large_binary, is_binary_view),
            None,
            None,
        ));
    }
    Err(PyTypeError::new_err(
        "expected a geoarrow point, multipoint, linestring, multilinestring, polygon, multipolygon, WKB, binary, or large_binary Arrow array",
    ))
}

pub(crate) fn arrow_classification_for_value(
    value: &Bound<'_, PyAny>,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<NativeArrowClassification> {
    let pa = arrow::pyarrow_module(value.py())?;
    arrow_classification(pa, value, field)
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
    // GeoArrow SHOULD accept LargeList (i64 offsets); List (i32) remains the
    // common producer form. ListView is not admitted.
    if arrow_type_is_large_list(pa, value_type)? {
        return Ok(());
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
            "geoarrow list geometry storage must be list (+l) or large_list (+L)",
        ));
    }
    Ok(())
}

fn ensure_pyarrow_point_struct(
    pa: &Bound<'_, PyModule>,
    value_type: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let types = pa.getattr("types")?;
    // Interleaved coordinates: FixedSizeList<float64>[n] with n in {2,3,4}
    // and field name "xy" / "xyz" / "xym" / "xyzm" (GeoArrow format §Coordinate).
    if arrow_type_is_fixed_size_list(pa, value_type)? {
        return ensure_pyarrow_interleaved_coords(pa, value_type);
    }
    let is_struct = if types.hasattr("is_struct")? {
        types
            .call_method1("is_struct", (value_type,))?
            .extract::<bool>()?
    } else {
        false
    };
    if !is_struct {
        return Err(PyTypeError::new_err(
            "geoarrow point storage must be a struct of float64 ordinates or fixed_size_list interleaved coordinates",
        ));
    }
    // Mandatory shared classifier: exact x/y, at most z/m, float64 leaves only.
    let names_value = value_type.getattr("names")?;
    let names = crate::collect_py_strings(&names_value, "GeoArrow field names")?;
    let fields_attr = value_type.getattr("fields")?;
    if fields_attr.len()? != names.len() {
        return Err(PyTypeError::new_err(
            "geoarrow point struct field names and fields length disagree",
        ));
    }
    let mut fields = crate::try_vec_with_capacity(names.len())?;
    for (index, name) in names.iter().enumerate() {
        let field_type = fields_attr.get_item(index)?.getattr("type")?;
        let is_float64 = pyarrow_type_is_float64(&types, &field_type)?;
        crate::try_push(&mut fields, (name.as_str(), is_float64))?;
    }
    geoarrow::classify_geoarrow_ordinates(fields).map_err(PyTypeError::new_err)?;
    Ok(())
}

/// Validate GeoArrow interleaved coordinate storage (`FixedSizeList<f64>[n]`).
fn ensure_pyarrow_interleaved_coords(
    pa: &Bound<'_, PyModule>,
    value_type: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let types = pa.getattr("types")?;
    let list_size: i32 = value_type.getattr("list_size")?.extract()?;
    if !(2..=4).contains(&list_size) {
        return Err(PyTypeError::new_err(
            "geoarrow interleaved coordinates require fixed_size_list of length 2, 3, or 4",
        ));
    }
    let value_field_type = value_type.getattr("value_type")?;
    if !pyarrow_type_is_float64(&types, &value_field_type)? {
        return Err(PyTypeError::new_err(
            "geoarrow interleaved coordinates require float64 value type",
        ));
    }
    // Field name of the FixedSizeList encodes dimensions when present
    // ("xy"/"xyz"/"xym"/"xyzm"); size alone is accepted when the name is
    // absent or the generic "item" pyarrow default. Non-canonical names for
    // any size (including size 3) are rejected — a size-3 field named
    // "garbage"/"xy"/"xyzm" must not silently decode as XYZ.
    if let Ok(name) = value_type
        .getattr("value_field")
        .and_then(|f| f.getattr("name"))
        .and_then(|n| n.extract::<String>())
    {
        if name.is_empty() || name == "item" {
            return Ok(());
        }
        let allowed: &[&str] = match list_size {
            2 => &["xy"],
            // size 3 is XYZ or XYM only (GeoArrow interleaved field names).
            3 => &["xyz", "xym"],
            4 => &["xyzm"],
            _ => unreachable!("list_size gated above"),
        };
        if !allowed.contains(&name.as_str()) {
            return Err(PyTypeError::new_err(format!(
                "geoarrow interleaved fixed_size_list[{list_size}] field name must be one of {allowed:?} (or default 'item'), got {name:?}"
            )));
        }
    }
    Ok(())
}

fn arrow_type_is_fixed_size_list(
    pa: &Bound<'_, PyModule>,
    value_type: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    if let Ok(format) = value_type.getattr("format")
        && let Ok(format) = format.extract::<String>()
    {
        // FixedSizeList format is `+w:N`.
        if format.starts_with("+w:") {
            return Ok(true);
        }
    }
    let types = pa.getattr("types")?;
    if types.hasattr("is_fixed_size_list")? {
        return types
            .call_method1("is_fixed_size_list", (value_type,))?
            .extract();
    }
    if let Ok(name) = value_type.get_type().name() {
        return Ok(name == "FixedSizeListType");
    }
    Ok(false)
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
            let encoding = GeometryEncoding::from_extension_name(&type_src.0);
            let type_frame = parse_geoarrow_extension_metadata_for(&type_src.1, encoding)?;
            let field_frame = parse_geoarrow_extension_metadata_for(&field_src.1, encoding)?;
            if !optional_crs_labels_operationally_equal(
                type_frame.0.as_deref(),
                field_frame.0.as_deref(),
            )? || !optional_epochs_equal(type_frame.1, field_frame.1)
            {
                return Err(geoarrow_parse_error(
                    "conflicting GeoArrow extension metadata between ExtensionType and field",
                ));
            }
            Ok(Some(type_src))
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
    let _edges = parse_geoarrow_edges(&object, encoding)?;
    // The CRS is a PROJJSON object (this encoder's output and the GeoArrow
    // recommendation) or an authority string from other producers; both
    // resolve through the CRS engine while retaining the full authored
    // PROJJSON object; an embedded ID is metadata, not a replacement label.
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

/// Private Python entry: parse one already-decoded GeoParquet column mapping for
/// the shared CRS/epoch/edges frame (defaults missing CRS to OGC:CRS84; CRS must
/// be a PROJJSON object or null when present).
#[pyfunction]
#[pyo3(name = "_parse_geoparquet_column_frame", signature = (metadata, column_name))]
pub(crate) fn py_parse_geoparquet_column_frame(
    metadata: &Bound<'_, PyAny>,
    column_name: &str,
) -> PyResult<GeoParquetColumnFrame> {
    let value = support::py_to_json_value_classified(metadata).map_err(|error| {
        match error {
            JsonValueError::NonFinite => GeoParquetAdmissionError::Metadata(format!(
                "invalid GeoParquet column '{column_name}': epoch must be finite"
            )),
            JsonValueError::Python(error) => GeoParquetAdmissionError::from(error),
        }
        .into_py_err()
    })?;
    parse_geoparquet_column_frame_value_typed(&value, column_name)
        .map_err(GeoParquetAdmissionError::into_py_err)
}

/// Admit GeoParquet geometry storage against a declared encoding.
///
/// Dictionary-wrapped WKB is accepted. ExtensionType and Field metadata are
/// reconciled together (name + frame) so raw-field frame metadata is never
/// discarded. Returns one immutable ``_GeoParquetPhysicalLayout`` record whose
/// fields are consumed by decoding without re-inspecting the materialized
/// value.
#[pyfunction]
#[pyo3(
    name = "_admit_geoparquet_geometry_storage",
    signature = (arrow_type, encoding, column_name, field = None)
)]
pub(crate) fn py_admit_geoparquet_geometry_storage(
    py: Python<'_>,
    arrow_type: &Bound<'_, PyAny>,
    encoding: &str,
    column_name: &str,
    field: Option<&Bound<'_, PyAny>>,
) -> PyResult<GeoParquetPhysicalLayout> {
    admit_geoparquet_geometry_storage(py, arrow_type, encoding, column_name, field)
        .map_err(GeoParquetAdmissionError::into_py_err)
}

fn geoparquet_meta_error(detail: impl std::fmt::Display) -> PyErr {
    let error = errors::parse_error(
        format!("malformed GeoParquet geo metadata: {detail}"),
        crate::error::ParseFormat::GeoParquet,
    );
    Python::attach(|py| {
        let _ = error.value(py).setattr("category", "metadata");
    });
    error
}

fn geoparquet_layout_error(detail: impl std::fmt::Display) -> PyErr {
    let error = GeometryError::new_err(detail.to_string());
    Python::attach(|py| {
        let _ = error.value(py).setattr("category", "layout");
    });
    error
}

fn geoparquet_resource_error(error: crate::error::Error) -> PyErr {
    let error: PyErr = error.into();
    Python::attach(|py| {
        let _ = error.value(py).setattr("category", "resource");
    });
    error
}

/// GeoParquet field/storage admission: declared encoding vs physical type.
///
/// The record's embedded frame is absent when no extension is present (plain
/// binary WKB or untyped native list storage whose depth matches the
/// declaration).
///
/// Extension sources are collected **before** physical unwrap, from (1) the
/// column/array type, (2) the schema field's type (ExtensionType after
/// registration), and (3) field-level ``ARROW:extension:*`` metadata. PyArrow
/// may rewrite the column to bare dictionary under ``read_dictionary`` and
/// clear field metadata after extension registration; the schema field's type
/// still carries the frame. Physical storage peels **extension first, then
/// dictionary** (Arrow: extension is the outer logical type; dictionary is a
/// physical encoding of the storage).
fn admit_geoparquet_geometry_storage(
    py: Python<'_>,
    arrow_type: &Bound<'_, PyAny>,
    encoding: &str,
    column_name: &str,
    field: Option<&Bound<'_, PyAny>>,
) -> Result<GeoParquetPhysicalLayout, GeoParquetAdmissionError> {
    let Some(declared) = GeometryEncoding::from_geoparquet_encoding(encoding) else {
        return Err(GeoParquetAdmissionError::Metadata(format!(
            "unsupported GeoParquet geometry encoding: {encoding:?}"
        )));
    };
    let pa = arrow::pyarrow_module(py)?;
    // Extension name/metadata decode errors (e.g. non-UTF-8) and source
    // conflicts surface as GeoParquet metadata — one boundary class for the
    // reader (Python previously used `_metadata_error` / format=geoparquet).
    let as_geoparquet = GeoParquetAdmissionError::from;

    // Frame sources before any physical peel (see module comment above).
    let column_type_source = type_extension_source(arrow_type).map_err(as_geoparquet)?;
    let (field_type_source, field_meta_source) = match field {
        Some(field) => {
            let field_type = field
                .getattr("type")
                .map_err(GeoParquetAdmissionError::from)?;
            (
                type_extension_source(&field_type).map_err(as_geoparquet)?,
                field_extension_source(field).map_err(as_geoparquet)?,
            )
        },
        None => (None, None),
    };
    // Reconcile name+frame across all sources; any disagreement is an error.
    let reconciled = reconcile_extension_sources(column_type_source, field_type_source)
        .map_err(as_geoparquet)?;
    let reconciled =
        reconcile_extension_sources(reconciled, field_meta_source).map_err(as_geoparquet)?;

    // Physical storage: extension wrapper first, then dictionary encoding.
    let storage_type = unwrap_extension_then_dictionary(pa, arrow_type)
        .map_err(GeoParquetAdmissionError::layout_from_py)?;

    match declared {
        GeometryEncoding::Wkb => {
            let is_binary = arrow_type_is_binary_family(pa, &storage_type)
                .map_err(GeoParquetAdmissionError::layout_from_py)?;
            let is_large_binary = arrow_type_is_large_binary(pa, &storage_type)
                .map_err(GeoParquetAdmissionError::layout_from_py)?;
            let is_binary_view = arrow_type_is_binary_view(pa, &storage_type)
                .map_err(GeoParquetAdmissionError::layout_from_py)?;
            if let Some((extension_name, metadata)) = reconciled {
                if !is_binary {
                    return Err(GeoParquetAdmissionError::Layout(format!(
                        "column '{column_name}' encoding 'WKB' requires Binary, \
                         LargeBinary, BinaryView, or geoarrow.wkb storage \
                         (dictionary-encoded binary is allowed)"
                    )));
                }
                if extension_name != "geoarrow.wkb" {
                    return Err(GeoParquetAdmissionError::Metadata(format!(
                        "column '{column_name}' encoding 'WKB' conflicts with \
                         embedded Arrow extension '{extension_name}'"
                    )));
                }
                // Frame parse failures stay on the GeoParquet boundary class.
                let (crs, epoch) =
                    parse_geoarrow_extension_metadata(&metadata).map_err(as_geoparquet)?;
                return Ok(GeoParquetPhysicalLayout::wkb(
                    true,
                    crs,
                    epoch,
                    wkb_offset_width(is_large_binary, is_binary_view),
                ));
            }
            if !is_binary {
                return Err(GeoParquetAdmissionError::Layout(format!(
                    "column '{column_name}' encoding 'WKB' requires Binary, \
                     LargeBinary, BinaryView, or geoarrow.wkb storage \
                     (dictionary-encoded binary is allowed)"
                )));
            }
            Ok(GeoParquetPhysicalLayout::wkb(
                false,
                None,
                None,
                wkb_offset_width(is_large_binary, is_binary_view),
            ))
        },
        native => {
            let expected_name = native.extension_name();
            let physical = geoarrow_native_layout(pa, &storage_type)
                .map_err(GeoParquetAdmissionError::layout_from_py)?;
            if let Some((extension_name, metadata)) = reconciled {
                if extension_name != expected_name {
                    // Match Python !r quoting so pinned tests keep working.
                    return Err(GeoParquetAdmissionError::Metadata(format!(
                        "native GeoParquet encoding '{encoding}' conflicts with \
                         embedded Arrow extension '{extension_name}'"
                    )));
                }
                // Frame parse failures stay on the GeoParquet boundary class.
                let (crs, epoch) =
                    parse_geoarrow_extension_metadata(&metadata).map_err(as_geoparquet)?;
                let Some(physical) = physical else {
                    return Err(GeoParquetAdmissionError::Layout(format!(
                        "native GeoParquet encoding '{encoding}' does not match storage layout"
                    )));
                };
                return Ok(GeoParquetPhysicalLayout::native(
                    native, true, crs, epoch, physical.0, physical.1, physical.2,
                ));
            }
            // No extension: declared encoding + exact list depth + xy struct.
            let expected_depth = native.list_depth().expect("native encoding has list depth");
            let Some(physical) = physical else {
                return Err(GeoParquetAdmissionError::Layout(format!(
                    "native GeoParquet encoding '{encoding}' does not match \
                     storage layout (expected list depth {expected_depth})"
                )));
            };
            if physical.0 != expected_depth {
                return Err(GeoParquetAdmissionError::Layout(format!(
                    "native GeoParquet encoding '{encoding}' does not match \
                     storage layout (expected list depth {expected_depth}, \
                     got {:?})",
                    Some(physical.0)
                )));
            }
            Ok(GeoParquetPhysicalLayout::native(
                native, false, None, None, physical.0, physical.1, physical.2,
            ))
        },
    }
}

/// Binary / LargeBinary / BinaryView physical storage (post extension+dict peel).
fn arrow_type_is_binary_family(
    pa: &Bound<'_, PyModule>,
    arrow_type: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    let types = pa.getattr("types")?;
    Ok(types
        .call_method1("is_binary", (arrow_type,))?
        .extract::<bool>()?
        || arrow_type_is_large_binary(pa, arrow_type)?
        || arrow_type_is_binary_view(pa, arrow_type)?)
}

/// Peel ExtensionType (`storage_type`) first, then dictionary (`value_type`).
///
/// Arrow order: extension is the outer logical type; dictionary is a physical
/// encoding of the storage. `extension<dictionary<binary>>` therefore unwraps
/// to binary. Bare `dictionary<binary>` and plain binary are unchanged.
fn unwrap_extension_then_dictionary<'py>(
    pa: &Bound<'py, PyModule>,
    arrow_type: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut current = arrow_type.clone();
    // ExtensionType exposes storage_type; only peel when this is actually an
    // extension root (extension_name / type-level extension source present).
    if type_extension_source(&current)?.is_some()
        && let Ok(storage_type) = current.getattr("storage_type")
    {
        current = storage_type;
    }
    unwrap_dictionary_type(pa, &current)
}

fn unwrap_dictionary_type<'py>(
    pa: &Bound<'py, PyModule>,
    arrow_type: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let types = pa.getattr("types")?;
    let mut current = arrow_type.clone();
    while types
        .call_method1("is_dictionary", (&current,))?
        .extract::<bool>()?
    {
        current = current.getattr("value_type")?;
    }
    Ok(current)
}

/// Native GeoArrow physical facts: list nesting and terminal ordinate fields.
///
/// This is the only GeoParquet physical-shape walk.  The returned tuple is
/// carried in [`GeoParquetPhysicalLayout`] instead of being recomputed during
/// rewrapping or decoded-content validation.
fn geoarrow_native_layout(
    pa: &Bound<'_, PyModule>,
    arrow_type: &Bound<'_, PyAny>,
) -> PyResult<Option<(u8, bool, bool)>> {
    let types = pa.getattr("types")?;
    let mut depth: u8 = 0;
    let mut current = arrow_type.clone();
    loop {
        let is_list = types
            .call_method1("is_list", (&current,))?
            .extract::<bool>()?
            || arrow_type_is_large_list(pa, &current)?
            || {
                if types.hasattr("is_list_view")? {
                    types
                        .call_method1("is_list_view", (&current,))?
                        .extract::<bool>()?
                } else {
                    false
                }
            };
        if !is_list {
            break;
        }
        depth = depth.saturating_add(1);
        current = current.getattr("value_type")?;
    }
    // Terminal coordinate type: separated struct or interleaved FixedSizeList.
    if arrow_type_is_fixed_size_list(pa, &current)? {
        ensure_pyarrow_interleaved_coords(pa, &current)?;
        let list_size: i32 = current.getattr("list_size")?.extract()?;
        let (has_z, has_m) = if list_size == 2 {
            (false, false)
        } else if list_size == 4 {
            (true, true)
        } else {
            let name = current
                .getattr("value_field")
                .ok()
                .and_then(|field| field.getattr("name").ok())
                .and_then(|name| name.extract::<String>().ok());
            (
                name.as_deref() != Some("xym"),
                name.as_deref() == Some("xym"),
            )
        };
        return Ok(Some((depth, has_z, has_m)));
    }
    let is_struct = if types.hasattr("is_struct")? {
        types
            .call_method1("is_struct", (&current,))?
            .extract::<bool>()?
    } else {
        false
    };
    if !is_struct {
        return Ok(None);
    }
    let names: Vec<String> = if let Ok(names) = current.getattr("names") {
        crate::collect_py_strings(&names, "GeoArrow field names")?
    } else {
        // Fall back to field-index name access (matches Python helper).
        let num_fields = current.getattr("num_fields")?.extract::<usize>()?;
        let mut names = crate::try_vec_with_capacity(num_fields)?;
        for index in 0..num_fields {
            crate::try_push(
                &mut names,
                current.get_item(index)?.getattr("name")?.extract()?,
            )?;
        }
        names
    };
    let fields = current.getattr("fields")?;
    if fields.len()? != names.len() {
        return Ok(None);
    }
    let mut ordinate_fields = crate::try_vec_with_capacity(names.len())?;
    for (index, name) in names.iter().enumerate() {
        let field_type = fields.get_item(index)?.getattr("type")?;
        let is_float64 = pyarrow_type_is_float64(&types, &field_type)?;
        crate::try_push(&mut ordinate_fields, (name.as_str(), is_float64))?;
    }
    let axes = geoarrow::classify_geoarrow_ordinates(ordinate_fields).ok();
    Ok(axes.map(|axes| (depth, axes.has_z, axes.has_m)))
}

/// GeoParquet column-level CRS/epoch/edges boundary (frame only — encoding,
/// geometry_types, orientation, and bbox stay in the Python reader).
#[pyclass(
    name = "_GeoParquetColumnFrame",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GeoParquetColumnFrame {
    pub(crate) crs: Option<String>,
    pub(crate) epoch: Option<f64>,
    /// Retain key presence independently from its decoded value.  In
    /// particular, an omitted epoch and an explicit JSON null are not the same
    /// declaration even though both decode to `None`.
    pub(crate) crs_present: bool,
    pub(crate) epoch_present: bool,
}

#[pymethods]
impl GeoParquetColumnFrame {
    /// Normalized CRS declared by GeoParquet column metadata.
    ///
    /// Returns
    /// -------
    /// str or None
    ///     ``'OGC:CRS84'`` when the ``crs`` key is absent, ``None`` for an
    ///     explicit JSON null, or the normalized CRS from a PROJJSON object.
    #[getter]
    fn crs(&self) -> Option<String> {
        self.crs.clone()
    }

    /// Coordinate epoch declared by GeoParquet column metadata.
    ///
    /// Returns
    /// -------
    /// float or None
    ///     A finite decimal year when ``epoch`` is numeric; ``None`` when the
    ///     key is absent or explicitly null. A non-null epoch requires a CRS.
    #[getter]
    const fn epoch(&self) -> Option<f64> {
        self.epoch
    }

    /// Whether GeoParquet column metadata contained a ``crs`` key.
    ///
    /// Returns
    /// -------
    /// bool
    ///     ``False`` means the key was absent and the CRS was defaulted to
    ///     ``'OGC:CRS84'``; ``True`` includes an explicit JSON null.
    #[getter]
    const fn crs_present(&self) -> bool {
        self.crs_present
    }

    /// Whether GeoParquet column metadata contained an ``epoch`` key.
    ///
    /// Returns
    /// -------
    /// bool
    ///     ``False`` means the epoch was omitted; ``True`` includes an
    ///     explicit JSON null, which is distinct from omission.
    #[getter]
    const fn epoch_present(&self) -> bool {
        self.epoch_present
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = (self.crs.clone(), self.epoch).into_pyobject(py)?;
        values.call_method0("__iter__").map(Bound::unbind)
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let Ok(other) = other.cast::<PyTuple>() else {
            return Ok(false);
        };
        if other.len() != 2 {
            return Ok(false);
        }
        Ok(other.get_item(0)?.extract::<Option<String>>()? == self.crs
            && other.get_item(1)?.extract::<Option<f64>>()? == self.epoch)
    }
}

fn parse_geoparquet_column_frame_value_typed(
    value: &Value,
    column_name: &str,
) -> Result<GeoParquetColumnFrame, GeoParquetAdmissionError> {
    let column_error = |detail: String| {
        GeoParquetAdmissionError::Metadata(format!(
            "invalid GeoParquet column '{column_name}': {detail}"
        ))
    };
    let Value::Object(object) = value else {
        return Err(column_error("metadata must be a JSON object".to_owned()));
    };

    // edges: planar only; spherical is real unsupported metadata; other tokens unknown.
    match object.get("edges") {
        None => {},
        Some(Value::String(value)) if value == "planar" => {},
        Some(Value::String(value)) if value == "spherical" => {
            return Err(column_error(format!(
                "edges '{value}' are unsupported; only planar edges are accepted"
            )));
        },
        Some(Value::String(value)) => {
            return Err(column_error(format!(
                "edges has an unknown value '{value}'"
            )));
        },
        Some(value) => {
            return Err(column_error(format!("edges has an unknown value {value}")));
        },
    }

    // CRS: absent → OGC:CRS84; present null → CRS-free; present object → PROJJSON;
    // present string is rejected (GeoParquet 1.x requires PROJJSON or null).
    let crs_present = object.contains_key("crs");
    let crs = match object.get("crs") {
        None => Some(normalize_geoparquet_crs_typed("OGC:CRS84", column_name)?),
        Some(Value::Null) => None,
        Some(crs @ Value::Object(_)) => Some(normalize_geoparquet_crs_typed(
            &crs.to_string(),
            column_name,
        )?),
        Some(_) => {
            return Err(column_error(
                "crs must be a PROJJSON object or null".to_owned(),
            ));
        },
    };

    let epoch_present = object.contains_key("epoch");
    let epoch = match object.get("epoch") {
        None | Some(Value::Null) => None,
        Some(Value::Number(number)) => Some(
            number
                .as_f64()
                .ok_or_else(|| column_error("epoch must be finite".to_owned()))?,
        ),
        Some(_) => {
            return Err(column_error("epoch must be a number".to_owned()));
        },
    };
    let epoch = match epoch {
        None => None,
        Some(value) if value.is_finite() => Some(if value == 0.0 { 0.0 } else { value }),
        Some(_) => {
            return Err(column_error("epoch must be finite".to_owned()));
        },
    };
    if epoch.is_some() && crs.is_none() {
        return Err(column_error("epoch requires crs".to_owned()));
    }
    Ok(GeoParquetColumnFrame {
        crs,
        epoch,
        crs_present,
        epoch_present,
    })
}

fn normalize_geoparquet_crs_typed(
    reference: &str,
    column_name: &str,
) -> Result<String, GeoParquetAdmissionError> {
    crate::crs::normalize(reference).map_err(|error| {
        GeoParquetAdmissionError::Metadata(format!(
            "invalid GeoParquet column '{column_name}': {error}"
        ))
    })
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
        // Match the native ArrowFormat token for LargeList.
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

const fn wkb_offset_width(is_large_binary: bool, is_binary_view: bool) -> WkbOffsetWidth {
    if is_binary_view {
        WkbOffsetWidth::View
    } else if is_large_binary {
        WkbOffsetWidth::Int64
    } else {
        WkbOffsetWidth::Int32
    }
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::PyMemoryError;
    use pyo3::types::PyAnyMethods as _;

    use super::GeoParquetAdmissionError;
    use crate::py::errors::{GeometryError, ParseError};

    #[test]
    fn geoparquet_error_categories_keep_python_projections_distinct() {
        crate::test_support::initialize_python();
        pyo3::Python::attach(|py| {
            let layout = GeoParquetAdmissionError::Layout("wrong storage".to_owned()).into_py_err();
            assert!(layout.is_instance_of::<GeometryError>(py));
            assert_eq!(
                layout
                    .value(py)
                    .getattr("category")
                    .expect("layout category")
                    .extract::<&str>()
                    .expect("layout category string"),
                "layout"
            );

            let metadata =
                GeoParquetAdmissionError::Metadata("bad declaration".to_owned()).into_py_err();
            assert!(metadata.is_instance_of::<ParseError>(py));
            assert_eq!(
                metadata
                    .value(py)
                    .getattr("category")
                    .expect("metadata category")
                    .extract::<&str>()
                    .expect("metadata category string"),
                "metadata"
            );
            assert_eq!(
                metadata
                    .value(py)
                    .getattr("format")
                    .expect("metadata format")
                    .extract::<&str>()
                    .expect("metadata format string"),
                "geoparquet"
            );

            let resource =
                GeoParquetAdmissionError::Resource(crate::error::ResourceError::Allocation.into())
                    .into_py_err();
            assert!(resource.is_instance_of::<PyMemoryError>(py));
            assert_eq!(
                resource
                    .value(py)
                    .getattr("category")
                    .expect("resource category")
                    .extract::<&str>()
                    .expect("resource category string"),
                "resource"
            );
        });
    }
}
