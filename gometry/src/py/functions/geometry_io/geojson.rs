#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::collections::BTreeMap;
use std::fmt;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyFloat, PyInt, PyList, PyModule, PyString};
use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::array::MissingMask;
use crate::py::functions::bulk_rows::{StreamedRow, stream_bulk};
use crate::{
    CoordinateAxes, Crs, Frame, GeoJsonDecodeContext, Polygon, PyGeometry, PyGeometryArray,
    ScalarOrIterator, Shape, Typed, Wgs84DefaultCrs, classify_scalar_or_iterator,
    coerce_geojson_geometry_value, coordinate_epoch_option, exact_geometry, exact_geometry_array,
    guard_epoch_frame, io, is_py_bytes_or_bytearray, mapping_as_dict, parse_geojson_geometry_value,
    py_bool, py_to_json_value, require_geojson_crs, with_one_byte_buffer,
};

/// Parse `geojson` from a string or mapping.
///
/// A geometry or ``Feature`` decodes to a ``Geometry`` (Feature properties
/// are dropped — see `from_features` to keep them); a ``FeatureCollection``
/// is a feature set, so it decodes to a ``GeometryArray`` with one geometry
/// per feature.
///
/// Coordinate sequences are **axis-uniform**: every position in one sequence
/// (LineString, MultiPoint member list, ring, …) must share the same axes
/// (all XY or all XYZ). RFC 7946 makes the third ordinate optional per
/// position, but gometry's coordinate model requires finite values on every
/// active ordinate and rejects non-finite coordinates, so mixed XY/XYZ within
/// one sequence is a ``ParseError`` rather than a silent 0-elevation fill.
/// Distinct members of a ``GeometryCollection`` may still differ in axes.
///
/// Parameters
/// ----------
/// data : str or mapping
///     A `geojson` string or mapping (Feature/FeatureCollection ok).
///
/// crs : str or int, default 'OGC:CRS84'
///     CRS to attach. `geojson` coordinates are WGS84 by specification, so
///     the default declares OGC:CRS84 (lon/lat, matching GeoParquet);
///     pass ``None`` for a CRS-free geometry or ``crs=4326`` for EPSG:4326.
///     Only the WGS84 family (``EPSG:4326``, ``EPSG:4979``, ``OGC:CRS84``,
///     ``OGC:CRS84h``) is accepted — reproject first for other CRS. A legacy
///     top-level ``crs`` member (pre-RFC 7946) is ignored when it agrees with
///     ``crs=`` and raises on conflict or unsupported declarations.
///
/// epoch : float, optional
///     Coordinate epoch (decimal year) to attach as frame metadata.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     The decoded geometry, or one geometry per feature for a
///     ``FeatureCollection``.
///
/// Raises
/// ------
/// ParseError
///     If the `geojson` is malformed or an unsupported type, a coordinate
///     sequence mixes axes (XY with XYZ), a coordinate integer is not exactly
///     representable as binary64, or a legacy ``crs`` member is unsupported or
///     conflicts with ``crs`` (``format`` is ``"geojson"``).
/// InvalidGeometryError
///     If a position is outside the WGS84 lon/lat domain or a ring fails
///     structural ring admission.
/// CRSError
///     If ``crs`` is not a recognized CRS or is outside the WGS84 family, or
///     ``epoch`` is set with ``crs=None``.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Notes
/// -----
/// Finite decimals parse as correctly-rounded binary64 (bit-exact round-trip
/// with ``to_geojson``). Integer tokens and Python ``int`` values are admitted
/// only when exactly representable as ``float``; non-exact integers raise
/// rather than silently rounding. Text and mapping inputs share this rule.
///
/// See Also
/// --------
/// Geometry.to_geojson : Serialize a geometry to GeoJSON.
/// from_features : Keep per-feature properties and ids.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.from_geojson('{"type": "Point", "coordinates": [1, 2]}').to_wkt()
/// 'POINT (1 2)'
#[pyfunction]
#[pyo3(
    signature = (data, *, crs = Wgs84DefaultCrs::Default, epoch = None),
    text_signature = "(data, *, crs='OGC:CRS84', epoch=None)"
)]
pub(crate) fn from_geojson(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    crs: Wgs84DefaultCrs,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    if exact_geometry(data).is_some() || exact_geometry_array(data).is_some() {
        return Err(PyTypeError::new_err(
            "from_geojson expects foreign GeoJSON data; a Geometry or GeometryArray is already decoded",
        ));
    }
    let crs = crs.into_crs();
    require_geojson_crs(crs.as_ref().map(Crs::as_str))?;
    let parsed_epoch = coordinate_epoch_option("epoch", epoch)?;
    guard_epoch_frame(parsed_epoch, crs.as_ref())?;
    // One classifier: retain the acquired iterator (no double try_iter) and
    // propagate protocol errors instead of misclassifying them as scalar docs.
    // Build Frame only after classification — the vector path never needs it.
    if let ScalarOrIterator::Iterator(iter) = classify_scalar_or_iterator(data)? {
        let crs_for_rows = crs.clone();
        let array = stream_bulk(iter, crs, parsed_epoch, None, move |data, rows| {
            if let Some(shape) = parse_geojson_shape_input(data, crs_for_rows.as_ref())? {
                rows.try_push(shape)?;
                Ok(StreamedRow::Present(None))
            } else {
                Ok(StreamedRow::Missing)
            }
        })?;
        return Ok(array.into_pyobject(py)?.unbind().into());
    }
    let frame = Frame::new(crs.clone(), parsed_epoch)?;
    let fixed = io::LegacyGeoJsonCrsPolicy::Fixed(crs.as_deref());
    if let Ok(text) = data.cast::<pyo3::types::PyString>() {
        let text = text.to_cow()?;
        // Legacy ``crs`` is captured during the typed text probe (own then
        // nested, same order as collect_geojson_legacy_crs) — no second full
        // Value parse of the document just to walk CRS members.
        let parsed = io::parse_geojson_text(text.as_ref())?;
        let declarations: Vec<&Value> = parsed.legacy_crs.iter().collect();
        io::reconcile_legacy_geojson_crs(&declarations, fixed)?;
        return match parsed.input {
            io::GeoJsonInput::Geometry(shape) => Ok(Typed(PyGeometry::with_frame(shape, frame))
                .into_pyobject(py)?
                .unbind()),
            io::GeoJsonInput::FeatureCollection(features) => {
                let (shapes, mask) = feature_shapes_and_missing(features);
                let array = PyGeometryArray::from_shapes(shapes, frame).with_missing_mask(mask);
                Ok(array.into_pyobject(py)?.unbind().into())
            },
        };
    }
    let parsed = parse_geojson_geometry_value(data)?;
    if let Some(features) = geojson_feature_collection(&parsed)? {
        let mut declarations = Vec::new();
        io::collect_geojson_legacy_crs(&parsed, &mut declarations);
        io::reconcile_legacy_geojson_crs(&declarations, fixed)?;
        let (shapes, mask) = feature_value_shapes_and_missing(features)?;
        let array = PyGeometryArray::from_shapes(shapes, frame).with_missing_mask(mask);
        return Ok(array.into_pyobject(py)?.unbind().into());
    }
    // Scalar Geometry or Feature: one shared coercer (GeometryLike + Fixed).
    // Nested Feature-in-geometry-slot is rejected by the geometry-slot parse;
    // a null Feature geometry is rejected as an unlocated scalar Feature.
    let Some((shape, coerced_frame)) = coerce_geojson_geometry_value(
        &parsed,
        GeoJsonDecodeContext::GeometryLike,
        fixed,
        parsed_epoch,
    )?
    else {
        return Err(io::IoError::geojson(
            "GeoJSON Feature geometry is null; use from_features or a FeatureCollection for missing rows",
        )
        .into());
    };
    debug_assert_eq!(coerced_frame.crs_ref(), frame.crs_ref());
    debug_assert_eq!(coerced_frame.epoch(), frame.epoch());
    let _ = frame;
    Ok(Typed(PyGeometry::with_frame(shape, coerced_frame))
        .into_pyobject(py)?
        .unbind())
}

/// Parse GeoJSON features into geometries plus parallel properties and ids.
///
/// Accepts a ``FeatureCollection``/``Feature`` mapping, JSON text of one
/// (``str`` or UTF-8 bytes/buffer), or an iterable of ``Feature`` mappings.
/// Unlike `from_geojson`, per-feature ``properties`` and ``id`` values are
/// preserved. Missing ``properties`` normalize to ``{}``; an explicit JSON
/// null remains ``None``.
///
/// Parameters
/// ----------
/// features : str, bytes, mapping, or iterable of mapping
///     A ``FeatureCollection``/``Feature`` mapping, JSON text of one, or an
///     iterable of ``Feature`` mappings.
/// crs : str or int, default 'OGC:CRS84'
///     CRS to attach. GeoJSON coordinates are WGS84 by specification, so the
///     default declares OGC:CRS84 (lon/lat); pass ``None`` for CRS-free
///     geometries or ``crs=4326`` for EPSG:4326.
/// epoch : float, optional
///     Coordinate epoch (decimal year) to attach as frame metadata.
///
/// Returns
/// -------
/// Features
///     A ``Features`` record with one row per
///     feature. Null geometries are represented by missing array rows.
///
/// Raises
/// ------
/// ParseError
///     If the input is not a Feature/FeatureCollection/iterable, a feature is
///     malformed, a geometry cannot be parsed, or a legacy ``crs`` member is
///     unsupported or conflicts with ``crs`` (``format`` is ``"GeoJSON"``).
/// InvalidGeometryError
///     If a position is outside the WGS84 lon/lat domain or a ring fails
///     structural ring admission.
/// CRSError
///     If ``crs`` is not recognized or is outside the WGS84 family, or
///     ``epoch`` is set with ``crs=None``.
/// GeometryError
///     If ``epoch`` is not a finite decimal year.
///
/// Notes
/// -----
/// Text input is decoded in Rust. Coordinate numbers follow the same admission
/// as ``from_geojson``: correctly-rounded binary64 floats, and integers only
/// when exactly representable as ``float`` (non-exact integers raise). Object
/// keys in text input are returned sorted; mapping input keeps key order and
/// opaque property values.
///
/// See Also
/// --------
/// from_geojson : Decode geometry while dropping feature side data.
/// to_feature_collection : Encode geometry and aligned side data.
#[pyfunction]
#[pyo3(
    signature = (features, *, crs = Wgs84DefaultCrs::Default, epoch = None),
    text_signature = "(features, *, crs='OGC:CRS84', epoch=None)"
)]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> feats = gm.from_features([{
/// ...     'type': 'Feature',
/// ...     'geometry': {'type': 'Point', 'coordinates': [1.0, 2.0]},
/// ...     'properties': {'a': 1},
/// ... }])
/// >>> feats.geometries.to_wkt()
/// ['POINT (1 2)']
pub(crate) fn from_features(
    py: Python<'_>,
    features: &Bound<'_, PyAny>,
    crs: Wgs84DefaultCrs,
    epoch: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let crs = crs.into_crs();
    require_geojson_crs(crs.as_ref().map(Crs::as_str))?;
    let epoch = coordinate_epoch_option("epoch", epoch)?;
    guard_epoch_frame(epoch, crs.as_ref())?;
    if let Ok(text) = features.cast::<PyString>() {
        return parse_features_text(py, text.to_cow()?.as_ref(), crs, epoch);
    }
    if is_py_bytes_or_bytearray(features) {
        // Same one-byte buffer path as from_geojson / from_wkb: signed and
        // unsigned itemsize-1 exporters (bytes/bytearray/memoryview/array.array
        // 'B'|'b'). Do not re-fork an unsigned-only PyBuffer::<u8> lane.
        return with_one_byte_buffer(features, |bytes| {
            let text = std::str::from_utf8(bytes).map_err(invalid_feature_json_utf8)?;
            parse_features_text(py, text, crs, epoch)
        });
    }

    let batch = python_feature_rows(py, features)?;
    // Reconcile each row's legacy CRS against the resolved frame as we go —
    // no parallel declaration vector or error-history ballast.
    let policy = io::LegacyGeoJsonCrsPolicy::Fixed(crs.as_deref());
    for legacy in &batch.legacy_crs {
        let value = py_to_json_value(legacy.bind(py))?;
        io::reconcile_legacy_geojson_crs(&[&value], policy)?;
    }
    build_features(py, batch.rows, crs, epoch)
}

fn invalid_feature_json_utf8(error: std::str::Utf8Error) -> PyErr {
    io::IoError::geojson(format!("invalid GeoJSON feature JSON: {error}")).into()
}

fn parse_features_text(
    py: Python<'_>,
    text: &str,
    crs: Option<Crs>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    // Single streaming pass: FeatureTopMeta / FeatureRowsMeta walk the JSON
    // once via serde visitors, capturing top-level, per-feature, and nested
    // geometry legacy `crs` members (including Unicode-escaped keys such as
    // `c\u0072s`) as they appear. There is no whole-document prefilter and no
    // second `serde_json::from_str` for legacy-CRS validation.
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let (rows_meta, collection_legacy) = if text.trim_start().starts_with('{') {
        FeatureTopMeta::deserialize(&mut deserializer)
            .map_err(invalid_feature_json)?
            .into_rows()?
    } else {
        match FeatureRowsMeta::deserialize(&mut deserializer).map_err(invalid_feature_json)? {
            FeatureRowsMeta::Rows(rows) => (rows, None),
            FeatureRowsMeta::NotIterable => {
                return Err(feature_parse_error(
                    "expected a Feature, FeatureCollection, or iterable of Feature mappings",
                ));
            },
        }
    };
    deserializer.end().map_err(invalid_feature_json)?;

    let policy = io::LegacyGeoJsonCrsPolicy::Fixed(crs.as_deref());
    // Collection-level legacy CRS first, then per-feature nested declarations
    // during row parse — no parallel accumulated declaration vector.
    if let Some(legacy) = collection_legacy.as_ref() {
        io::reconcile_legacy_geojson_crs(&[legacy], policy)?;
    }
    let mut rows = Vec::with_capacity(rows_meta.len());
    for meta in rows_meta {
        let (row, nested) = feature_row_from_meta(meta)?;
        for legacy in &nested {
            io::reconcile_legacy_geojson_crs(&[legacy], policy)?;
        }
        rows.push(row);
    }
    build_features(py, rows, crs, epoch)
}

fn invalid_feature_json(error: serde_json::Error) -> PyErr {
    io::IoError::geojson(format!("invalid GeoJSON feature JSON: {error}")).into()
}

enum FeatureGeometryInput {
    Null,
    Json(Value),
    Python(Py<PyAny>),
}

enum FeatureSideData {
    /// Text-side properties and ids stay as validated JSON text until this
    /// boundary converts their leaves into Python objects. In particular, a
    /// JSON integer is never rounded through `serde_json::Value` before it
    /// becomes Python's arbitrary-precision `int`.
    Json(Option<Box<RawValue>>),
    Python(Py<PyAny>),
}

struct FeatureRow {
    geometry: FeatureGeometryInput,
    properties: FeatureSideData,
    id: FeatureSideData,
}

fn feature_row_from_meta(feature: FeatureRowMeta) -> PyResult<(FeatureRow, Vec<Value>)> {
    let FeatureRowMeta::Mapping {
        kind,
        geometry,
        properties,
        id,
        legacy_crs,
    } = feature
    else {
        return Err(feature_parse_error("each feature must be a mapping"));
    };
    if kind.as_deref() != Some("Feature") {
        return Err(feature_parse_error(
            "each feature must have type \"Feature\"",
        ));
    }
    let mut nested_legacy_crs = legacy_crs.into_iter().collect::<Vec<_>>();
    let geometry = match geometry {
        FeatureGeometryValue::Missing => {
            return Err(feature_parse_error("each feature must have a geometry"));
        },
        FeatureGeometryValue::Null => FeatureGeometryInput::Null,
        FeatureGeometryValue::Value(value) => {
            let mut nested = Vec::new();
            io::collect_geojson_legacy_crs(&value, &mut nested);
            nested_legacy_crs.extend(nested.into_iter().cloned());
            FeatureGeometryInput::Json(value)
        },
    };
    Ok((
        FeatureRow {
            geometry,
            properties: FeatureSideData::Json(properties),
            id: FeatureSideData::Json(id),
        },
        nested_legacy_crs,
    ))
}

struct PythonFeatureRows {
    rows: Vec<FeatureRow>,
    legacy_crs: Vec<Py<PyAny>>,
}

fn python_feature_rows(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<PythonFeatureRows> {
    // Keystone first (not abc.Mapping): keys()-only ducks that dict() accepts
    // must enter the Feature/FeatureCollection branch without an ABC gate.
    let (items, collection_legacy_crs) = if let Some(data) = mapping_as_dict(data)? {
        match python_mapping_kind(&data)? {
            Some(PythonFeatureKind::Collection) => {
                // RFC 7946 §7.1: FeatureCollection must not carry Geometry/Feature
                // defining members (parity with from_geojson / JSON frontends).
                let mut members = io::DefiningMembers::default();
                if data.contains(pyo3::intern!(py, "coordinates"))? {
                    members.set(io::DefiningMembers::COORDINATES);
                }
                if data.contains(pyo3::intern!(py, "geometries"))? {
                    members.set(io::DefiningMembers::GEOMETRIES);
                }
                if data.contains(pyo3::intern!(py, "geometry"))? {
                    members.set(io::DefiningMembers::GEOMETRY);
                }
                if data.contains(pyo3::intern!(py, "properties"))? {
                    members.set(io::DefiningMembers::PROPERTIES);
                }
                io::reject_rfc7946_cross_type_members("FeatureCollection", members)?;
                let features = data
                    .get_item(pyo3::intern!(py, "features"))?
                    .ok_or_else(|| feature_parse_error("feature collection requires features"))?;
                // Fallible growth (D10): infinite feature lists → MemoryError,
                // never hang/abort via `Vec::collect`. Non-iterable TypeError
                // stays the historical ParseError message.
                let items = crate::collect_py_iter(&features, Ok).map_err(|err| {
                    if err.is_instance_of::<pyo3::exceptions::PyMemoryError>(py) {
                        err
                    } else {
                        feature_parse_error("feature collection features must be iterable")
                    }
                })?;
                (
                    items,
                    data.get_item(pyo3::intern!(py, "crs"))?.map(Bound::unbind),
                )
            },
            Some(PythonFeatureKind::Feature) => (vec![data.into_any()], None),
            _ => {
                return Err(feature_parse_error(
                    "expected a Feature, FeatureCollection, or iterable of Feature mappings",
                ));
            },
        }
    } else {
        let items = crate::collect_py_iter(data, Ok).map_err(|err| {
            if err.is_instance_of::<pyo3::exceptions::PyMemoryError>(py) {
                err
            } else {
                feature_parse_error(
                    "expected a Feature, FeatureCollection, or iterable of Feature mappings",
                )
            }
        })?;
        (items, None)
    };
    let mut legacy_crs = collection_legacy_crs.into_iter().collect::<Vec<_>>();
    let rows = items
        .iter()
        .map(|item| {
            let (row, row_legacy_crs) = python_feature_row(py, item)?;
            legacy_crs.extend(row_legacy_crs);
            Ok(row)
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PythonFeatureRows { rows, legacy_crs })
}

#[derive(Clone, Copy)]
enum PythonFeatureKind {
    Feature,
    Collection,
}

fn python_mapping_kind(mapping: &Bound<'_, PyDict>) -> PyResult<Option<PythonFeatureKind>> {
    let Some(value) = mapping.get_item(pyo3::intern!(mapping.py(), "type"))? else {
        return Ok(None);
    };
    let Ok(value) = value.cast::<PyString>() else {
        return Ok(None);
    };
    Ok(match value.to_cow()?.as_ref() {
        "Feature" => Some(PythonFeatureKind::Feature),
        "FeatureCollection" => Some(PythonFeatureKind::Collection),
        _ => None,
    })
}

fn python_feature_row(
    py: Python<'_>,
    feature: &Bound<'_, PyAny>,
) -> PyResult<(FeatureRow, Option<Py<PyAny>>)> {
    // keys()+seen copier (N4): accept keys()-only ducks, not only abc.Mapping.
    let feature = mapping_as_dict(feature)?
        .ok_or_else(|| feature_parse_error("each feature must be a mapping"))?;
    if !matches!(
        python_mapping_kind(&feature)?,
        Some(PythonFeatureKind::Feature)
    ) {
        return Err(feature_parse_error(
            "each feature must have type \"Feature\"",
        ));
    }
    // RFC 7946 §7.1: same defining-member exclusions as from_geojson.
    let mut members = io::DefiningMembers::default();
    if feature.contains(pyo3::intern!(py, "coordinates"))? {
        members.set(io::DefiningMembers::COORDINATES);
    }
    if feature.contains(pyo3::intern!(py, "geometries"))? {
        members.set(io::DefiningMembers::GEOMETRIES);
    }
    if feature.contains(pyo3::intern!(py, "features"))? {
        members.set(io::DefiningMembers::FEATURES);
    }
    io::reject_rfc7946_cross_type_members("Feature", members)?;
    let geometry = feature
        .get_item(pyo3::intern!(py, "geometry"))?
        .ok_or_else(|| feature_parse_error("each feature must have a geometry"))?;
    let geometry = if geometry.is_none() {
        FeatureGeometryInput::Null
    } else {
        FeatureGeometryInput::Python(geometry.unbind())
    };
    let properties =
        python_feature_properties(py, feature.get_item(pyo3::intern!(py, "properties"))?)?;
    let id = python_feature_id(py, feature.get_item(pyo3::intern!(py, "id"))?)?;
    let legacy_crs = feature
        .get_item(pyo3::intern!(py, "crs"))?
        .map(Bound::unbind);
    Ok((
        FeatureRow {
            geometry,
            properties: FeatureSideData::Python(properties),
            id: FeatureSideData::Python(id),
        },
        legacy_crs,
    ))
}

fn python_feature_properties(
    py: Python<'_>,
    value: Option<Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let Some(value) = value else {
        return Ok(PyDict::new(py).unbind().into());
    };
    if value.is_none() {
        return Ok(py.None());
    }
    // Shared keys()+seen copier (N4): accepts keys()-only ducks and rejects
    // repeated-key streams immediately — never dict.update object-iteration.
    let Some(output) = mapping_as_dict(&value)? else {
        return Err(feature_parse_error(
            "feature properties must be a mapping or None",
        ));
    };
    for key in output.keys().iter() {
        if !key.is_instance_of::<PyString>() {
            return Err(feature_parse_error(
                "feature properties keys must be strings",
            ));
        }
    }
    Ok(output.unbind().into())
}

fn python_feature_id(py: Python<'_>, value: Option<Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(py.None());
    };
    let valid = !value.is_instance_of::<PyBool>()
        && (value.is_instance_of::<PyString>()
            || value.is_instance_of::<PyInt>()
            || value
                .cast::<PyFloat>()
                .is_ok_and(|number| number.value().is_finite()));
    if !valid {
        return Err(feature_parse_error(
            "feature id must be a string or finite number",
        ));
    }
    Ok(value.unbind())
}

fn build_features(
    py: Python<'_>,
    rows: Vec<FeatureRow>,
    crs: Option<Crs>,
    epoch: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let row_count = rows.len();
    let mut shapes = Vec::with_capacity(row_count);
    let mut missing_rows = Vec::new();
    let mut properties = Vec::with_capacity(rows.len());
    let mut ids = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.into_iter().enumerate() {
        match row.geometry {
            FeatureGeometryInput::Null => {
                missing_rows.push(row_index);
            },
            FeatureGeometryInput::Json(value) => shapes.push(io::parse_geojson(&value)?),
            FeatureGeometryInput::Python(value) => {
                // Geometry slot only — nested Feature must not unwrap (object/
                // text/bytes parity). CRS was already reconciled for the batch.
                let parsed = parse_geojson_geometry_value(value.bind(py))?;
                let Some((shape, _)) = coerce_geojson_geometry_value(
                    &parsed,
                    GeoJsonDecodeContext::GeometrySlot,
                    io::LegacyGeoJsonCrsPolicy::Fixed(crs.as_deref()),
                    None,
                )?
                else {
                    return Err(feature_parse_error("each feature must have a geometry"));
                };
                shapes.push(shape);
            },
        }
        properties.push(match row.properties {
            FeatureSideData::Json(value) => feature_properties_raw_py(py, value.as_deref())?,
            FeatureSideData::Python(value) => value,
        });
        ids.push(match row.id {
            FeatureSideData::Json(value) => feature_id_raw_py(py, value.as_deref())?,
            FeatureSideData::Python(value) => value,
        });
    }
    // Final-order with kind-preserving placeholders (no scatter).
    let geometries = {
        let array = if missing_rows.is_empty() {
            PyGeometryArray::from_shapes(shapes, Frame::new(crs, epoch)?)
        } else {
            let placeholder = homogeneous_missing_placeholder(shapes.iter());
            let mut full = Vec::with_capacity(row_count);
            let mut present = shapes.into_iter();
            let mut miss = missing_rows.iter().copied().peekable();
            for row in 0..row_count {
                if miss.peek() == Some(&row) {
                    miss.next();
                    full.push(placeholder.clone());
                } else {
                    full.push(present.next().expect("present count matches"));
                }
            }
            PyGeometryArray::from_shapes(full, Frame::new(crs, epoch)?)
                .with_missing_mask(crate::array::sparse_missing_mask(row_count, &missing_rows))
        };
        array.into_pyobject(py)?.into_any()
    };
    let properties = pyo3::types::PyTuple::new(py, properties)?;
    let ids = pyo3::types::PyTuple::new(py, ids)?;
    Ok(crate::py::support::features_type(py)?
        .call1((geometries, properties, ids))?
        .unbind())
}

struct FeatureTopMeta {
    kind: Option<String>,
    features: Option<FeatureRowsMeta>,
    geometry: FeatureGeometryValue,
    properties: Option<Box<RawValue>>,
    id: Option<Box<RawValue>>,
    legacy_crs: Option<Value>,
}

impl FeatureTopMeta {
    fn into_rows(self) -> PyResult<(Vec<FeatureRowMeta>, Option<Value>)> {
        match self.kind.as_deref() {
            Some("FeatureCollection") => match self.features {
                Some(FeatureRowsMeta::Rows(rows)) => Ok((rows, self.legacy_crs)),
                Some(FeatureRowsMeta::NotIterable) => Err(feature_parse_error(
                    "feature collection features must be iterable",
                )),
                None => Err(feature_parse_error("feature collection requires features")),
            },
            Some("Feature") => Ok((
                vec![FeatureRowMeta::Mapping {
                    kind: self.kind,
                    geometry: self.geometry,
                    properties: self.properties,
                    id: self.id,
                    legacy_crs: self.legacy_crs,
                }],
                None,
            )),
            _ => Err(feature_parse_error(
                "expected a Feature, FeatureCollection, or iterable of Feature mappings",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for FeatureTopMeta {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(FeatureTopMetaVisitor)
    }
}

struct FeatureTopMetaVisitor;

impl<'de> Visitor<'de> for FeatureTopMetaVisitor {
    type Value = FeatureTopMeta;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GeoJSON Feature or FeatureCollection object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut kind = None;
        let mut features = None;
        let mut geometry = FeatureGeometryValue::Missing;
        let mut properties = None;
        let mut id = None;
        let mut legacy_crs = None;
        let mut has_coordinates = false;
        let mut has_geometries = false;
        let mut has_geometry = false;
        let mut has_properties = false;
        let mut has_features = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => {
                    kind = map.next_value::<Value>()?.as_str().map(str::to_owned);
                },
                "features" => {
                    has_features = true;
                    features = Some(map.next_value()?);
                },
                "geometry" => {
                    has_geometry = true;
                    geometry = map.next_value()?;
                },
                "properties" => {
                    has_properties = true;
                    properties = Some(map.next_value()?);
                },
                "id" => id = Some(map.next_value()?),
                "crs" => legacy_crs = Some(map.next_value()?),
                "coordinates" => {
                    has_coordinates = true;
                    let _ = map.next_value::<IgnoredAny>()?;
                },
                "geometries" => {
                    has_geometries = true;
                    let _ = map.next_value::<IgnoredAny>()?;
                },
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                },
            }
        }
        // RFC 7946 §7.1 before kind dispatch — same exclusions as from_geojson.
        let mut members = io::DefiningMembers::default();
        if has_coordinates {
            members.set(io::DefiningMembers::COORDINATES);
        }
        if has_geometries {
            members.set(io::DefiningMembers::GEOMETRIES);
        }
        if has_geometry {
            members.set(io::DefiningMembers::GEOMETRY);
        }
        if has_properties {
            members.set(io::DefiningMembers::PROPERTIES);
        }
        if has_features {
            members.set(io::DefiningMembers::FEATURES);
        }
        match kind.as_deref() {
            Some("Feature") => {
                io::reject_rfc7946_cross_type_members("Feature", members)
                    .map_err(de::Error::custom)?;
            },
            Some("FeatureCollection") => {
                io::reject_rfc7946_cross_type_members("FeatureCollection", members)
                    .map_err(de::Error::custom)?;
            },
            _ => {},
        }
        Ok(FeatureTopMeta {
            kind,
            features,
            geometry,
            properties,
            id,
            legacy_crs,
        })
    }
}

enum FeatureRowsMeta {
    Rows(Vec<FeatureRowMeta>),
    NotIterable,
}

impl<'de> Deserialize<'de> for FeatureRowsMeta {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(FeatureRowsMetaVisitor)
    }
}

struct FeatureRowsMetaVisitor;

macro_rules! reject_feature_scalar_values {
    ($rejected:path) => {
        fn visit_bool<E>(self, _value: bool) -> std::result::Result<Self::Value, E> {
            Ok($rejected)
        }

        fn visit_i64<E>(self, _value: i64) -> std::result::Result<Self::Value, E> {
            Ok($rejected)
        }

        fn visit_u64<E>(self, _value: u64) -> std::result::Result<Self::Value, E> {
            Ok($rejected)
        }

        fn visit_f64<E>(self, _value: f64) -> std::result::Result<Self::Value, E> {
            Ok($rejected)
        }

        fn visit_str<E>(self, _value: &str) -> std::result::Result<Self::Value, E> {
            Ok($rejected)
        }

        fn visit_string<E>(self, _value: String) -> std::result::Result<Self::Value, E> {
            Ok($rejected)
        }

        fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
            Ok($rejected)
        }
    };
}

impl<'de> Visitor<'de> for FeatureRowsMetaVisitor {
    type Value = FeatureRowsMeta;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a feature sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut rows = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(row) = seq.next_element()? {
            rows.push(row);
        }
        Ok(FeatureRowsMeta::Rows(rows))
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some((_key, _value)) = map.next_entry::<IgnoredAny, IgnoredAny>()? {}
        Ok(FeatureRowsMeta::NotIterable)
    }

    reject_feature_scalar_values!(FeatureRowsMeta::NotIterable);
}

enum FeatureRowMeta {
    Mapping {
        kind: Option<String>,
        geometry: FeatureGeometryValue,
        properties: Option<Box<RawValue>>,
        id: Option<Box<RawValue>>,
        legacy_crs: Option<Value>,
    },
    NotMapping,
}

impl<'de> Deserialize<'de> for FeatureRowMeta {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(FeatureRowMetaVisitor)
    }
}

struct FeatureRowMetaVisitor;

impl<'de> Visitor<'de> for FeatureRowMetaVisitor {
    type Value = FeatureRowMeta;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GeoJSON Feature object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut kind = None;
        let mut geometry = FeatureGeometryValue::Missing;
        let mut properties = None;
        let mut id = None;
        let mut legacy_crs = None;
        let mut has_coordinates = false;
        let mut has_geometries = false;
        let mut has_features = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => {
                    kind = map.next_value::<Value>()?.as_str().map(str::to_owned);
                },
                "geometry" => geometry = map.next_value()?,
                "properties" => properties = Some(map.next_value()?),
                "id" => id = Some(map.next_value()?),
                "crs" => legacy_crs = Some(map.next_value()?),
                "coordinates" => {
                    has_coordinates = true;
                    let _ = map.next_value::<IgnoredAny>()?;
                },
                "geometries" => {
                    has_geometries = true;
                    let _ = map.next_value::<IgnoredAny>()?;
                },
                "features" => {
                    has_features = true;
                    let _ = map.next_value::<IgnoredAny>()?;
                },
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                },
            }
        }
        // Nested Feature rows (iterable / FeatureCollection.features) share
        // the same §7.1 validator as a top-level Feature and from_geojson.
        if kind.as_deref() == Some("Feature") {
            let mut members = io::DefiningMembers::default();
            if has_coordinates {
                members.set(io::DefiningMembers::COORDINATES);
            }
            if has_geometries {
                members.set(io::DefiningMembers::GEOMETRIES);
            }
            if has_features {
                members.set(io::DefiningMembers::FEATURES);
            }
            io::reject_rfc7946_cross_type_members("Feature", members).map_err(de::Error::custom)?;
        }
        Ok(FeatureRowMeta::Mapping {
            kind,
            geometry,
            properties,
            id,
            legacy_crs,
        })
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(_value) = seq.next_element::<IgnoredAny>()? {}
        Ok(FeatureRowMeta::NotMapping)
    }

    reject_feature_scalar_values!(FeatureRowMeta::NotMapping);
}

enum FeatureGeometryValue {
    Missing,
    Null,
    Value(Value),
}

impl<'de> Deserialize<'de> for FeatureGeometryValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        if value.is_null() {
            Ok(Self::Null)
        } else {
            Ok(Self::Value(value))
        }
    }
}

/// Split parsed features into **final-order** shapes (missing → kind-preserving
/// placeholder when present rows share one packable kind) plus missing mask.
fn feature_shapes_and_missing(features: Vec<Option<Shape>>) -> (Vec<Shape>, Option<MissingMask>) {
    let row_count = features.len();
    let mut missing_rows = Vec::new();
    let placeholder = homogeneous_missing_placeholder(features.iter().filter_map(|s| s.as_ref()));
    let mut shapes = Vec::with_capacity(row_count);
    for (row, shape) in features.into_iter().enumerate() {
        if let Some(shape) = shape {
            shapes.push(shape);
        } else {
            missing_rows.push(row);
            shapes.push(placeholder.clone());
        }
    }
    (
        shapes,
        crate::array::sparse_missing_mask(row_count, &missing_rows),
    )
}

/// When every present shape is the same packable kind/axes, return a
/// kind-preserving missing placeholder so final-order arrays stay packed
/// (D2 GeoArrow native). Otherwise the generic NaN point placeholder.
fn homogeneous_missing_placeholder<'a>(present: impl Iterator<Item = &'a Shape>) -> Shape {
    let mut first: Option<&Shape> = None;
    for shape in present {
        match first {
            None => first = Some(shape),
            Some(prev) if !same_packable_kind(prev, shape) => {
                return PyGeometryArray::missing_placeholder();
            },
            Some(_) => {},
        }
    }
    match first {
        Some(Shape::Point(p)) => {
            let axes = CoordinateAxes::from_point(*p);
            let z = axes.has_z().then_some(f64::NAN);
            let m = axes.has_m().then_some(f64::NAN);
            Shape::Point(crate::geometry::Point::new_unchecked_axes(
                f64::NAN,
                f64::NAN,
                crate::geometry::ZOrdinate(z),
                crate::geometry::MOrdinate(m),
            ))
        },
        Some(Shape::LineString(line)) => {
            let axes = line.axes();
            let mut b = crate::geometry::CoordSeqBuilder::with_capacity(axes, 2);
            let pt = crate::geometry::Point::new_unchecked_axes(
                f64::NAN,
                f64::NAN,
                crate::geometry::ZOrdinate(axes.has_z().then_some(f64::NAN)),
                crate::geometry::MOrdinate(axes.has_m().then_some(f64::NAN)),
            );
            b.push(pt);
            b.push(pt);
            Shape::LineString(crate::geometry::LineSeq::from_trusted(
                b.finish_infallible(),
            ))
        },
        Some(Shape::Polygon(poly)) => {
            // Kind-preserving missing placeholder for hole-bearing polygons too
            // (D2): packable storage keeps mask/offsets; do not demote to WKB.
            let axes = poly.shell.coords().axes();
            let mut b = crate::geometry::CoordSeqBuilder::with_capacity(axes, 4);
            let pt = crate::geometry::Point::new_unchecked_axes(
                f64::NAN,
                f64::NAN,
                crate::geometry::ZOrdinate(axes.has_z().then_some(f64::NAN)),
                crate::geometry::MOrdinate(axes.has_m().then_some(f64::NAN)),
            );
            for _ in 0..4 {
                b.push(pt);
            }
            // Empty hole list on the placeholder; present rows keep their holes.
            // Packed admission tolerates missing shells as NaN rings.
            Shape::Polygon(Polygon::new(
                crate::geometry::Ring::from_trusted_closed(b.finish_infallible()),
                Vec::new(),
            ))
        },
        _ => PyGeometryArray::missing_placeholder(),
    }
}

fn same_packable_kind(a: &Shape, b: &Shape) -> bool {
    match (a, b) {
        (Shape::Point(pa), Shape::Point(pb)) => {
            CoordinateAxes::from_point(*pa) == CoordinateAxes::from_point(*pb)
        },
        (Shape::LineString(la), Shape::LineString(lb)) => la.axes() == lb.axes(),
        (Shape::Polygon(pa), Shape::Polygon(pb)) => {
            // Same axes — holes may differ per row; packed polygon storage
            // admits variable hole counts via CSR offsets.
            pa.shell.coords().axes() == pb.shell.coords().axes()
        },
        _ => false,
    }
}

fn feature_properties_raw_py(py: Python<'_>, value: Option<&RawValue>) -> PyResult<Py<PyAny>> {
    match value {
        None => Ok(PyDict::new(py).unbind().into()),
        Some(value) if value.get().trim() == "null" => Ok(py.None()),
        Some(value) if value.get().trim_start().starts_with('{') => raw_json_to_py(py, value),
        Some(_) => Err(feature_parse_error(
            "feature properties must be a mapping or None",
        )),
    }
}

fn feature_id_raw_py(py: Python<'_>, value: Option<&RawValue>) -> PyResult<Py<PyAny>> {
    let Some(value) = value else {
        return Ok(py.None());
    };
    let text = value.get().trim();
    if text == "null" {
        return Ok(py.None());
    }
    let result = raw_json_to_py(py, value)?;
    let result = result.bind(py);
    if result.is_instance_of::<PyString>()
        || (!result.is_instance_of::<PyBool>() && result.is_instance_of::<PyInt>())
        || result
            .cast::<PyFloat>()
            .is_ok_and(|number| number.value().is_finite())
    {
        Ok(result.clone().unbind())
    } else {
        Err(feature_parse_error(
            "feature id must be a string or finite number",
        ))
    }
}

/// Convert a validated RawValue into Python without routing integer lexemes
/// through binary64. This walks opaque Feature side data exactly once: the old
/// RawValue recursion reparsed and retained every container tail, turning a
/// valid depth-125 one-megabyte leaf into O(depth × payload) work.
fn raw_json_to_py(py: Python<'_>, value: &RawValue) -> PyResult<Py<PyAny>> {
    let mut parser = RawJsonToPy {
        py,
        text: value.get(),
        position: 0,
    };
    let result = parser.parse_value(0)?;
    parser.skip_whitespace();
    if parser.position != parser.text.len() {
        return Err(parser.invalid("trailing data"));
    }
    Ok(result)
}

/// A compact parser for side data that serde has already admitted as JSON.
/// `RawValue` preserves arbitrary-size integer lexemes; this cursor preserves
/// that property while avoiding recursive whole-subtree deserialization.
struct RawJsonToPy<'py, 'text> {
    py: Python<'py>,
    text: &'text str,
    position: usize,
}

impl RawJsonToPy<'_, '_> {
    fn invalid(&self, detail: &str) -> PyErr {
        crate::py::errors::parse_error(
            format!("invalid GeoJSON Feature side data: {detail}"),
            crate::error::ParseFormat::GeoJson,
        )
    }

    fn skip_whitespace(&mut self) {
        while self
            .text
            .as_bytes()
            .get(self.position)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.position += 1;
        }
    }

    fn current(&self) -> Option<u8> {
        self.text.as_bytes().get(self.position).copied()
    }

    fn consume(&mut self, byte: u8) -> PyResult<()> {
        if self.current() != Some(byte) {
            return Err(self.invalid("unexpected token"));
        }
        self.position += 1;
        Ok(())
    }

    fn parse_value(&mut self, depth: usize) -> PyResult<Py<PyAny>> {
        if depth >= crate::io::MAX_PARSE_DEPTH {
            return Err(crate::py::errors::parse_error(
                "GeoJSON Feature side data exceeds nesting depth 128",
                crate::error::ParseFormat::GeoJson,
            ));
        }
        self.skip_whitespace();
        match self.current() {
            Some(b'{') => self.parse_object(depth),
            Some(b'[') => self.parse_array(depth),
            Some(b'"') => Ok(self.parse_string()?.into_pyobject(self.py)?.unbind().into()),
            Some(b'n') => {
                self.parse_literal("null")?;
                Ok(self.py.None())
            },
            Some(b't') => {
                self.parse_literal("true")?;
                Ok(py_bool(self.py, true))
            },
            Some(b'f') => {
                self.parse_literal("false")?;
                Ok(py_bool(self.py, false))
            },
            Some(b'-' | b'0'..=b'9') => self.parse_number(),
            _ => Err(self.invalid("expected a JSON value")),
        }
    }

    fn parse_object(&mut self, depth: usize) -> PyResult<Py<PyAny>> {
        self.consume(b'{')?;
        self.skip_whitespace();
        let mut values = BTreeMap::<String, Py<PyAny>>::new();
        if self.current() != Some(b'}') {
            loop {
                self.skip_whitespace();
                if self.current() != Some(b'"') {
                    return Err(self.invalid("object key is not a string"));
                }
                let key = self.parse_string()?;
                self.skip_whitespace();
                self.consume(b':')?;
                values.insert(key, self.parse_value(depth + 1)?);
                self.skip_whitespace();
                match self.current() {
                    Some(b',') => self.position += 1,
                    Some(b'}') => break,
                    _ => return Err(self.invalid("object is missing a separator")),
                }
            }
        }
        self.consume(b'}')?;
        let dict = PyDict::new(self.py);
        // Preserve the established key-sorted dict order without materializing
        // nested raw JSON tails.
        for (key, value) in values {
            dict.set_item(key, value)?;
        }
        Ok(dict.unbind().into())
    }

    fn parse_array(&mut self, depth: usize) -> PyResult<Py<PyAny>> {
        self.consume(b'[')?;
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.current() != Some(b']') {
            loop {
                values.push(self.parse_value(depth + 1)?);
                self.skip_whitespace();
                match self.current() {
                    Some(b',') => self.position += 1,
                    Some(b']') => break,
                    _ => return Err(self.invalid("array is missing a separator")),
                }
            }
        }
        self.consume(b']')?;
        Ok(PyList::new(self.py, values)?.unbind().into())
    }

    fn parse_literal(&mut self, literal: &str) -> PyResult<()> {
        let end = self
            .position
            .checked_add(literal.len())
            .ok_or_else(|| self.invalid("literal length overflows"))?;
        if self.text.get(self.position..end) != Some(literal) {
            return Err(self.invalid("invalid JSON literal"));
        }
        self.position = end;
        Ok(())
    }

    fn parse_string(&mut self) -> PyResult<String> {
        let start = self.position;
        self.consume(b'"')?;
        while let Some(byte) = self.current() {
            self.position += 1;
            match byte {
                b'"' => {
                    return serde_json::from_str(
                        self.text
                            .get(start..self.position)
                            .ok_or_else(|| self.invalid("invalid JSON string boundary"))?,
                    )
                    .map_err(invalid_feature_json);
                },
                b'\\' => {
                    if self.current().is_none() {
                        return Err(self.invalid("unterminated string escape"));
                    }
                    self.position += 1;
                },
                _ => {},
            }
        }
        Err(self.invalid("unterminated string"))
    }

    fn parse_number(&mut self) -> PyResult<Py<PyAny>> {
        let start = self.position;
        if self.current() == Some(b'-') {
            self.position += 1;
        }
        match self.current() {
            Some(b'0') => self.position += 1,
            Some(b'1'..=b'9') => {
                self.position += 1;
                while matches!(self.current(), Some(b'0'..=b'9')) {
                    self.position += 1;
                }
            },
            _ => return Err(self.invalid("invalid JSON number")),
        }
        let mut integer = true;
        if self.current() == Some(b'.') {
            integer = false;
            self.position += 1;
            let fraction_start = self.position;
            while matches!(self.current(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
            if self.position == fraction_start {
                return Err(self.invalid("invalid JSON fraction"));
            }
        }
        if matches!(self.current(), Some(b'e' | b'E')) {
            integer = false;
            self.position += 1;
            if matches!(self.current(), Some(b'+' | b'-')) {
                self.position += 1;
            }
            let exponent_start = self.position;
            while matches!(self.current(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
            if self.position == exponent_start {
                return Err(self.invalid("invalid JSON exponent"));
            }
        }
        let text = self
            .text
            .get(start..self.position)
            .ok_or_else(|| self.invalid("invalid JSON number boundary"))?;
        if integer {
            return PyModule::import(self.py, "builtins")?
                .getattr("int")?
                .call1((text,))
                .map(Bound::unbind);
        }
        let value: f64 = serde_json::from_str(text).map_err(invalid_feature_json)?;
        Ok(value.into_pyobject(self.py)?.unbind().into())
    }
}

fn feature_parse_error(message: &'static str) -> PyErr {
    crate::py::errors::parse_error(message, crate::error::ParseFormat::GeoJson)
}

fn parse_geojson_shape_input(
    data: &Bound<'_, PyAny>,
    crs: Option<&Crs>,
) -> PyResult<Option<Shape>> {
    if exact_geometry(data).is_some() || exact_geometry_array(data).is_some() {
        return Err(PyTypeError::new_err(
            "from_geojson expects foreign GeoJSON data; a Geometry or GeometryArray is already decoded",
        ));
    }
    let fixed = io::LegacyGeoJsonCrsPolicy::Fixed(crs.map(AsRef::as_ref));
    if let Ok(text) = data.cast::<pyo3::types::PyString>() {
        let text = text.to_cow()?;
        if let Ok(parsed) = io::parse_geojson_text(text.as_ref()) {
            let declarations: Vec<&Value> = parsed.legacy_crs.iter().collect();
            io::reconcile_legacy_geojson_crs(&declarations, fixed)?;
            return match parsed.input {
                io::GeoJsonInput::Geometry(shape) => Ok(Some(shape)),
                io::GeoJsonInput::FeatureCollection(_) => Err(io::IoError::geojson(
                    "GeoJSON FeatureCollection is a feature set, not one geometry; use from_geojson (returns a GeometryArray) or from_features",
                )
                .into()),
            };
        }
        // A null-geometry Feature is the one valid scalar-row form that the
        // geometry-only text parser deliberately rejects. Retain the value
        // fallback for that missing-row case (and for identical diagnostics on
        // malformed input); ordinary geometry strings stay on the direct
        // column decoder above.
        let parsed: Value = serde_json::from_str(text.as_ref())
            .map_err(|error| io::IoError::geojson(error.to_string()))?;
        return Ok(coerce_geojson_geometry_value(
            &parsed,
            GeoJsonDecodeContext::NullableRow,
            fixed,
            None,
        )?
        .map(|(shape, _)| shape));
    }
    let parsed = parse_geojson_geometry_value(data)?;
    Ok(
        coerce_geojson_geometry_value(&parsed, GeoJsonDecodeContext::NullableRow, fixed, None)?
            .map(|(shape, _)| shape),
    )
}

/// The features of a `GeoJSON` ``FeatureCollection``, or `None` for any other
/// `GeoJSON` value (a feature set decodes to rows, not to one geometry).
fn geojson_feature_collection(value: &Value) -> PyResult<Option<&Vec<Value>>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    if object.get("type").and_then(Value::as_str) != Some("FeatureCollection") {
        return Ok(None);
    }
    // RFC 7946 §7.1: FeatureCollection must not carry Geometry/Feature members.
    io::reject_rfc7946_value_object(object)?;
    object
        .get("features")
        .and_then(Value::as_array)
        .map(Some)
        .ok_or_else(|| io::IoError::geojson("GeoJSON FeatureCollection requires features").into())
}

/// FeatureCollection member decoder: each element MUST be a Feature (RFC 7946
/// §3.3) — identical for dict / str / bytes frontends.
fn geojson_feature_collection_member(value: &Value) -> PyResult<Option<Shape>> {
    let Some(object) = value.as_object() else {
        return Err(
            io::IoError::geojson("GeoJSON FeatureCollection features must be Features").into(),
        );
    };
    if object.get("type").and_then(Value::as_str) != Some("Feature") {
        return Err(
            io::IoError::geojson("GeoJSON FeatureCollection features must be Features").into(),
        );
    }
    io::reject_rfc7946_value_object(object)?;
    let geometry = object
        .get("geometry")
        .ok_or_else(|| io::IoError::geojson("GeoJSON Feature requires geometry"))?;
    if geometry.is_null() {
        Ok(None)
    } else {
        Ok(Some(io::parse_geojson(geometry)?))
    }
}

fn feature_value_shapes_and_missing(
    features: &[Value],
) -> PyResult<(Vec<Shape>, Option<MissingMask>)> {
    let mut present = Vec::with_capacity(features.len());
    let mut slots: Vec<Option<Shape>> = Vec::with_capacity(features.len());
    for feature in features {
        let shape = geojson_feature_collection_member(feature)?;
        if let Some(s) = &shape {
            present.push(s.clone());
        }
        slots.push(shape);
    }
    let placeholder = homogeneous_missing_placeholder(present.iter());
    let mut shapes = Vec::with_capacity(slots.len());
    let mut missing_rows = Vec::new();
    for (row, slot) in slots.into_iter().enumerate() {
        if let Some(shape) = slot {
            shapes.push(shape);
        } else {
            missing_rows.push(row);
            shapes.push(placeholder.clone());
        }
    }
    Ok((
        shapes,
        crate::array::sparse_missing_mask(features.len(), &missing_rows),
    ))
}
