use super::*;
use crate::py::geoarrow;

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

    /// Stored CRS label from the field's own GeoArrow extension metadata.
    ///
    /// Returns
    /// -------
    /// str or None
    ///     The exact gometry CRS definition when its namespaced metadata is
    ///     present and agrees with standard CRS metadata; otherwise the
    ///     normalized standard label. ``None`` means the extension omits CRS.
    ///     This is not the caller's GeoParquet column declaration.
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
}

/// Typed failure categories at the GeoParquet physical-layout boundary.
///
/// Layout failures are `GeometryError`; malformed or contradictory metadata is
/// `ParseError(format="geoparquet")`. Python allocation errors retain their
/// original exception objects.
#[derive(Debug)]
pub(super) enum GeoParquetAdmissionError {
    Layout(String),
    Metadata(String),
    Resource(crate::error::Error),
    PythonResource(PyErr),
}

impl GeoParquetAdmissionError {
    pub(super) fn layout_from_py(error: PyErr) -> Self {
        Python::attach(|py| {
            if error.is_instance_of::<pyo3::exceptions::PyMemoryError>(py) {
                Self::PythonResource(error)
            } else {
                Self::Layout(error.value(py).to_string())
            }
        })
    }

    pub(super) fn into_py_err(self) -> PyErr {
        match self {
            Self::Layout(detail) => geoparquet_layout_error(detail),
            Self::Metadata(detail) => geoparquet_meta_error(detail),
            Self::Resource(error) => geoparquet_resource_error(error.into()),
            Self::PythonResource(error) => geoparquet_resource_error(error),
        }
    }
}

impl From<PyErr> for GeoParquetAdmissionError {
    fn from(error: PyErr) -> Self {
        Python::attach(|py| {
            if error.is_instance_of::<pyo3::exceptions::PyMemoryError>(py) {
                Self::PythonResource(error)
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
            JsonValueError::NonStringKey => GeoParquetAdmissionError::from(PyTypeError::new_err(
                "JSON object keys must be strings",
            )),
            JsonValueError::NestingLimit(max_depth) => {
                GeoParquetAdmissionError::from(PyErr::from(crate::io::IoError::geojson(format!(
                    "GeoJSON JSON nesting exceeds the structural limit of {max_depth}"
                ))))
            },
            JsonValueError::FeatureDataNestingLimit => {
                GeoParquetAdmissionError::from(PyErr::from(crate::io::IoError::geojson(format!(
                    "GeoJSON Feature side data exceeds nesting depth {}",
                    crate::geometry::MAX_NESTING
                ))))
            },
            JsonValueError::UnsupportedType(name) => GeoParquetAdmissionError::from(
                GeometryError::new_err(format!("unsupported JSON value type {name}")),
            ),
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
        drop(error.value(py).setattr("category", "metadata"));
    });
    error
}

fn geoparquet_layout_error(detail: impl std::fmt::Display) -> PyErr {
    let error = GeometryError::new_err(detail.to_string());
    Python::attach(|py| {
        drop(error.value(py).setattr("category", "layout"));
    });
    error
}

fn geoparquet_resource_error(error: PyErr) -> PyErr {
    Python::attach(|py| {
        drop(error.value(py).setattr("category", "resource"));
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
        native @ (GeometryEncoding::Point
        | GeometryEncoding::MultiPoint
        | GeometryEncoding::LineString
        | GeometryEncoding::MultiLineString
        | GeometryEncoding::Polygon
        | GeometryEncoding::MultiPolygon) => {
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
    /// CRS label declared by GeoParquet column metadata.
    ///
    /// Returns
    /// -------
    /// str or None
    ///     The exact gometry CRS definition when its namespaced metadata is
    ///     present and agrees with standard metadata; otherwise
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
    let standard_crs = match object.get("crs") {
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
    let crs_definition = match object.get(geoarrow::CRS_DEFINITION_METADATA_KEY) {
        None => None,
        Some(Value::String(definition)) => {
            let Some(standard_crs) = standard_crs.as_deref() else {
                return Err(column_error(format!(
                    "{} requires a standard CRS",
                    geoarrow::CRS_DEFINITION_METADATA_KEY
                )));
            };
            normalize_geoparquet_crs_typed(definition, column_name)?;
            if !optional_crs_labels_operationally_equal(Some(standard_crs), Some(definition))
                .map_err(|error| column_error(error.to_string()))?
            {
                return Err(column_error(format!(
                    "{} conflicts with standard GeoParquet CRS",
                    geoarrow::CRS_DEFINITION_METADATA_KEY
                )));
            }
            Some(definition.clone())
        },
        Some(_) => {
            return Err(column_error(format!(
                "{} must be a string",
                geoarrow::CRS_DEFINITION_METADATA_KEY
            )));
        },
    };
    let crs = crs_definition.or(standard_crs);

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
    crate::crs::normalize(reference)
        .map_err(|error| {
            GeoParquetAdmissionError::Metadata(format!(
                "invalid GeoParquet column '{column_name}': {error}"
            ))
        })
        .map(std::borrow::Cow::into_owned)
}
