use super::*;
use crate::py::geoarrow;

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
    let mut fields = Vec::with_capacity(names.len());
    for (index, name) in names.iter().enumerate() {
        let field_type = fields_attr.get_item(index)?.getattr("type")?;
        let is_float64 = pyarrow_type_is_float64(&types, &field_type)?;
        fields.push((name.as_str(), is_float64));
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

/// Binary / LargeBinary / BinaryView physical storage (post extension+dict peel).
pub(super) fn arrow_type_is_binary_family(
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
pub(super) fn unwrap_extension_then_dictionary<'py>(
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
pub(super) fn geoarrow_native_layout(
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
    let names: Vec<String> =
        if let Some(names) = current.getattr_opt(pyo3::intern!(current.py(), "names"))? {
            crate::collect_py_strings(&names, "GeoArrow field names")?
        } else {
            // Fall back to field-index name access (matches Python helper).
            let num_fields = current.getattr("num_fields")?.extract::<usize>()?;
            let mut names = Vec::with_capacity(num_fields);
            for index in 0..num_fields {
                names.push(current.get_item(index)?.getattr("name")?.extract()?);
            }
            names
        };
    let fields = current.getattr("fields")?;
    if fields.len()? != names.len() {
        return Ok(None);
    }
    let mut ordinate_fields = Vec::with_capacity(names.len());
    for (index, name) in names.iter().enumerate() {
        let field_type = fields.get_item(index)?.getattr("type")?;
        let is_float64 = pyarrow_type_is_float64(&types, &field_type)?;
        ordinate_fields.push((name.as_str(), is_float64));
    }
    let axes = geoarrow::classify_geoarrow_ordinates(ordinate_fields).ok();
    Ok(axes.map(|axes| (depth, axes.has_z, axes.has_m)))
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

pub(super) fn arrow_type_is_binary_view(
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

pub(super) const fn wkb_offset_width(
    is_large_binary: bool,
    is_binary_view: bool,
) -> WkbOffsetWidth {
    if is_binary_view {
        WkbOffsetWidth::View
    } else if is_large_binary {
        WkbOffsetWidth::Int64
    } else {
        WkbOffsetWidth::Int32
    }
}
