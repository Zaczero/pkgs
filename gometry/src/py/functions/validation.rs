//! Validation-at-the-boundary free functions: `require` and `repair`.
//!
//! `require` parses an input and enforces a geometry contract (kind/dimension/
//! CRS); `repair` fixes invalid geometries. Both delegate to the crate-root and
//! `geometry` validation kernels via `use super::*`.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::py::errors::crs_mismatch_error;
use crate::py::wire_crs::{guard_embedded_crs_conflict, prefer_wire_alias_crs, split_ewkt_srid};
use crate::{
    CoordinateAxes, Crs, Frame, FrameAdoption, FrameEdit, GeoJsonDecodeContext, HeapSize,
    InvalidGeometryError, PyGeometry, PyGeometryArray, Typed, ValidationIssue,
    coerce_geojson_geometry_value, crs_arc, crs_label, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, geometry, guard_epoch_frame, io, is_mapping_like,
    is_one_byte_buffer, is_wkt_string, parse_crs, parse_geojson_geometry_value,
    parse_geojson_slice, parse_wkb_geometry, with_one_byte_buffer,
};

/// Parse and require a geometry contract at an input boundary.
///
/// Parameters
/// ----------
/// value : geometry-like or iterable of geometry-like
///     One geometry, a `GeometryArray`, or an iterable. Foreign scalar inputs
///     may be WKT, WKB, GeoJSON mappings/text, or ``__geo_interface__`` objects.
/// crs : str or int, optional
///     CRS as an EPSG code or authority/WKT string to attach.
/// axes : {'XY', 'XYZ', 'XYM', 'XYZM'}, optional
///     If given, require the geometry's coordinate axes to match exactly,
///     otherwise raise.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     The validated input. Iterables return a `GeometryArray`.
///
/// Raises
/// ------
/// CRSError
///     If ``crs`` is not a recognized CRS.
/// ParseError
///     If foreign GeoJSON is malformed, or its legacy ``crs`` member is
///     unsupported or conflicts with ``crs`` (``format`` is ``"GeoJSON"``).
/// CRSMismatchError
///     If an already-decoded / native / non-GeoJSON geometry's CRS differs
///     from ``crs``.
/// InvalidGeometryError
///     If the geometry is invalid, or its axes differ from ``axes``.
///     Geographic antimeridian crossings are validated after topology
///     normalization; projected and CRS-free geometry remains planar.
///
/// See Also
/// --------
/// Geometry.validate : Structured validity report.
/// Geometry.repair : Fix the geometry.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.require(gm.Point(1, 2, crs=4326), crs=4326).to_wkt()
/// 'POINT (1 2)'
#[pyfunction]
#[pyo3(signature = (value, *, crs = None, axes = None))]
pub(crate) fn require(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    crs: Option<&Bound<'_, PyAny>>,
    axes: Option<CoordinateAxes>,
) -> PyResult<Py<PyAny>> {
    // Parse the expected CRS once for every lane (scalar, array, foreign
    // iterable) so row parsers never re-canonicalize the same text.
    let expected = parse_crs(crs)?;
    if let Some(geometry) = exact_geometry(value) {
        let geometry = require_geometry_contract(geometry.clone(), expected.as_ref(), axes)?;
        return Ok(Typed(geometry).into_pyobject(py)?.into_any().unbind());
    }
    if let Some(array) = exact_geometry_array(value) {
        let array = require_array_contract(array.clone(), expected.as_ref(), axes)?;
        return Ok(array.into_pyobject(py)?.into_any().unbind());
    }
    if is_require_scalar(value)? {
        let geometry = require_geometry_contract(
            parse_require_input(value, expected.as_ref())?,
            expected.as_ref(),
            axes,
        )?;
        return Ok(Typed(geometry).into_pyobject(py)?.into_any().unbind());
    }

    // Fallible growth (D10): materialization raises MemoryError rather than
    // aborting on OOM. Bounding a genuinely infinite stream is the caller's
    // responsibility. Stream native geometries into the array constructor —
    // no intermediate PyList of Typed wrappers.
    let raw_items = crate::collect_py_iter(value, Ok)?;
    let len = raw_items.len();
    let mut has_missing = false;
    let mut mask = crate::try_vec_with_capacity(len)?;
    for item in &raw_items {
        let missing = item.is_none();
        has_missing |= missing;
        mask.push(missing);
    }
    let mut present = crate::try_vec_with_capacity(len)?;
    for (row, item) in raw_items.into_iter().enumerate() {
        if item.is_none() {
            continue;
        }
        let geometry = if let Some(geometry) = exact_geometry(&item) {
            geometry.clone()
        } else {
            parse_require_input(&item, expected.as_ref())
                .map_err(|error| crate::note_array_row(error, row))?
        };
        present.push(geometry);
    }
    let frame = Frame::resolve_items(
        &mut present,
        FrameAdoption {
            crs: expected.clone(),
            epoch: None,
        },
        "GeometryArray",
    )?;
    let array = if has_missing {
        let mut present_items = present.into_iter();
        let mut items = crate::try_vec_with_capacity(len)?;
        for &missing in &mask {
            let item = if missing {
                PyGeometry::with_frame(PyGeometryArray::missing_placeholder(), frame.clone())
            } else {
                present_items
                    .next()
                    .expect("present item count derived from input mask")
            };
            items.push(item);
        }
        PyGeometryArray::pack_or_mixed(items, frame)?
            .with_missing_mask(crate::array::MissingMask::from_vec(len, mask))
    } else {
        PyGeometryArray::pack_or_mixed(present, frame)?
    };
    let array = require_array_contract(array, expected.as_ref(), axes)?;
    Ok(array.into_pyobject(py)?.into_any().unbind())
}

fn is_require_scalar(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(value.cast::<pyo3::types::PyString>().is_ok()
        || is_one_byte_buffer(value)
        || is_mapping_like(value)?
        || value
            .getattr_opt(pyo3::intern!(value.py(), "__geo_interface__"))?
            .is_some())
}

/// Whether a `require(crs=)` contract needs the value retagged to `expected`,
/// or `Err` when the value is in a genuinely different frame.
///
/// `require` *attaches* rather than merely asserting — it already labels
/// CRS-free input — so a value already in the requested frame under a
/// different spelling is retagged to the label the caller asked for. That is
/// the same rule `GeometryArray(values, crs=...)` applies, and it keeps
/// `require` from rejecting a pair that every predicate and metric accepts.
fn require_crs_retag(actual: Option<&Crs>, expected: Option<&Crs>) -> PyResult<bool> {
    let Some(expected) = expected else {
        return Ok(false);
    };
    match actual {
        None => Ok(true),
        Some(actual) if actual == expected => Ok(false),
        Some(actual) if crate::crs_operationally_equal(actual, expected)? => Ok(true),
        Some(actual) => Err(crs_mismatch_error(
            format!(
                "expected CRS {}, got {}",
                crs_label(Some(expected.as_str())),
                crs_label(Some(actual.as_str())),
            ),
            Some(expected.as_str()),
            Some(actual.as_str()),
            None,
        )),
    }
}

fn require_geometry_contract(
    mut geometry: PyGeometry,
    expected: Option<&Crs>,
    axes: Option<CoordinateAxes>,
) -> PyResult<PyGeometry> {
    if require_crs_retag(geometry.crs_ref(), expected)? {
        geometry.set_crs_keep_epoch(expected.cloned());
    }
    if let Some(axes) = axes
        && geometry.shape.coordinate_axes() != axes.as_str()
    {
        return Err(InvalidGeometryError::new_err(format!(
            "expected axes {:?}, got {:?}",
            axes.as_str(),
            geometry.shape.coordinate_axes(),
        )));
    }
    if let Some(issue) = geometry::validate_data_in_frame(
        &geometry.shape,
        geometry::is_geographic_frame(&geometry.frame),
    ) {
        return Err(InvalidGeometryError::new_err(issue.reason));
    }
    Ok(geometry)
}

fn require_array_contract(
    mut array: PyGeometryArray,
    expected: Option<&Crs>,
    axes: Option<CoordinateAxes>,
) -> PyResult<PyGeometryArray> {
    if require_crs_retag(array.crs_ref(), expected)? {
        // Attach the pre-parsed CRS without re-parsing a Bound token.
        // `overwrite` is safe here: `require_crs_retag` only asks for a retag
        // once the existing label is absent or names the very same frame.
        let frame = FrameEdit::SetCrs {
            crs: expected.cloned(),
            overwrite: true,
        }
        .apply(&array.frame)?;
        array =
            PyGeometryArray::from_storage_arc(std::sync::Arc::clone(array.storage_arc()), frame)
                .with_missing_mask(array.missing().cloned());
    }
    if let Some(axes) = axes
        && array.uniform_axes() != Some(axes)
    {
        let mut seen: Vec<&'static str> = Vec::new();
        for (_, shape) in array.present_shape_rows() {
            let row_axes = shape.axes().as_str();
            if !seen.contains(&row_axes) {
                seen.push(row_axes);
            }
        }
        let got = match seen.as_slice() {
            [] | [_] => format!("{:?}", array.coordinate_axes().unwrap_or("mixed")),
            many => format!("mixed ({})", many.join(", ")),
        };
        return Err(InvalidGeometryError::new_err(format!(
            "expected axes {:?}, got {got}",
            axes.as_str(),
        )));
    }
    let geographic = geometry::is_geographic_frame(&array.frame);
    if let Some((row, issue)) = array.present_shape_rows().find_map(|(row, shape)| {
        geometry::validate_shape_in_frame(&shape, geographic).map(|issue| (row, issue))
    }) {
        return Err(InvalidGeometryError::new_err(format!(
            "geometry {row} is invalid: {}",
            issue.reason,
        )));
    }
    Ok(array)
}

fn parse_require_wkt(text: &str, fallback: Option<&Crs>) -> PyResult<PyGeometry> {
    let (body, srid) = split_ewkt_srid(text)?;
    let embedded = io::crs_from_optional_srid(srid)?;
    guard_embedded_crs_conflict(embedded.as_deref(), fallback.map(Crs::as_str), "EWKT SRID")?;
    let crs = prefer_wire_alias_crs(embedded.map(crs_arc), fallback).or_else(|| fallback.cloned());
    guard_epoch_frame(None, crs.as_ref())?;
    Ok(PyGeometry::with_epoch(io::parse_wkt(body)?, crs, None))
}

fn parse_require_geojson_value(
    value: &serde_json::Value,
    target: Option<&Crs>,
) -> PyResult<PyGeometry> {
    let Some((shape, frame)) = coerce_geojson_geometry_value(
        value,
        GeoJsonDecodeContext::GeometryLike,
        crate::io::LegacyGeoJsonCrsPolicy::Adopt(target.map(Crs::as_str)),
        None,
    )?
    else {
        return Err(crate::io::IoError::geojson(
            "Feature has null geometry (an unlocated feature); parse the \
             FeatureCollection with from_geojson/from_features, where null \
             geometries become missing rows",
        )
        .into());
    };
    Ok(PyGeometry::with_frame(shape, frame))
}

fn parse_require_bytes_slice(data: &[u8], fallback: Option<&Crs>) -> PyResult<PyGeometry> {
    if matches!(
        data.iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace()),
        Some(b'{' | b'[')
    ) {
        let parsed = parse_geojson_slice(data)?;
        return parse_require_geojson_value(&parsed, fallback);
    }
    let mut geometry = parse_wkb_geometry(data)?;
    guard_embedded_crs_conflict(geometry.crs_str(), fallback.map(Crs::as_str), "EWKB SRID")?;
    let resolved =
        prefer_wire_alias_crs(geometry.crs_ref().cloned(), fallback).or_else(|| fallback.cloned());
    geometry.set_crs_keep_epoch(resolved);
    Ok(geometry)
}

fn parse_require_buffer(data: &Bound<'_, PyAny>, fallback: Option<&Crs>) -> PyResult<PyGeometry> {
    with_one_byte_buffer(data, |bytes| parse_require_bytes_slice(bytes, fallback))
}

fn parse_require_input(geom: &Bound<'_, PyAny>, fallback: Option<&Crs>) -> PyResult<PyGeometry> {
    if let Ok(text) = geom.cast::<pyo3::types::PyString>() {
        let text = text.to_cow()?;
        if is_wkt_string(text.as_ref()) {
            return parse_require_wkt(text.as_ref(), fallback);
        }
        // GeoJSON text: same shared coercer as mapping / interface / bytes.
        let parsed: serde_json::Value = serde_json::from_str(text.as_ref())
            .map_err(|error| crate::io::IoError::geojson(error.to_string()))?;
        return parse_require_geojson_value(&parsed, fallback);
    }
    if is_one_byte_buffer(geom) {
        return parse_require_buffer(geom, fallback);
    }
    let parsed = parse_geojson_geometry_value(geom)?;
    parse_require_geojson_value(&parsed, fallback)
}

/// A structured geometry-validity verdict.
///
/// Returned by ``geom.validate()``: truthy when the geometry is valid;
/// otherwise ``report.reason`` names the OGC violation, ``report.location``
/// pinpoints it, and ``report.path`` addresses the offending part.
/// Validation reports describe invalid geometries and their reasons.
#[pyclass(
    name = "ValidationReport",
    module = "gometry",
    frozen,
    immutable_type,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyValidationReport {
    pub geometry: PyGeometry,
    pub issue: Option<ValidationIssue>,
}

type ValidationReportPickleArgs = (Typed,);

/// Rebuild a pickled `ValidationReport` from its geometry only (internal;
/// see ``ValidationReport.__reduce__``).
///
/// The verdict is always re-derived via the same path as ``Geometry.validate``
/// — forged `.valid` / nonfinite diagnostic locations in the payload are never
/// trusted.
#[pyfunction]
pub(crate) fn _unpickle_validation_report(
    geometry: &Bound<'_, PyAny>,
) -> PyResult<PyValidationReport> {
    let geometry = exact_geometry(geometry)
        .ok_or_else(expected_geometry_or_array)?
        .clone();
    Ok(geometry.validate_impl())
}

// --- ValidationReport #[pymethods] (moved from crate root) ---

frozen_pymethods! {
impl PyValidationReport {
    /// ``case ValidationReport(False, reason)`` destructures the verdict.
    #[classattr]
    const fn __match_args__() -> (&'static str, &'static str, &'static str, &'static str) {
        ("valid", "reason", "location", "path")
    }

    /// Whether the geometry is valid (also the report's truth value).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    pub const fn valid(&self) -> bool {
        self.issue.is_none()
    }

    /// Human-readable reason for the first validity problem, or ``None``.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    pub fn reason(&self) -> Option<String> {
        self.issue.as_ref().map(|issue| issue.reason.clone())
    }

    /// ``(x, y)`` location of the first problem, when known.
    ///
    /// Returns
    /// -------
    /// tuple or None
    #[getter]
    pub fn location(&self) -> Option<(f64, f64)> {
        self.issue
            .as_ref()
            .and_then(|issue| issue.location)
            .map(|point| (point.x, point.y))
    }

    /// Structural path to the first problem (e.g. ``'$.shell'``), when known.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    pub fn path(&self) -> Option<String> {
        self.issue.as_ref().and_then(|issue| issue.path.clone())
    }

    /// ``sys.getsizeof`` support: the report plus the retained geometry
    /// coordinate payload and any validation issue strings.
    pub fn __sizeof__(&self) -> usize {
        self.total_size()
    }

    pub const fn __bool__(&self) -> bool {
        self.valid()
    }

    /// Reports are immutable values: equal when they describe the same
    /// geometry with the same verdict. Other types defer.
    pub fn __eq__(&self, other: &Self) -> bool {
        let (left, right) = (&self.geometry, &other.geometry);
        left.crs_ref() == right.crs_ref()
            && left.epoch() == right.epoch()
            && left.shape == right.shape
            && self.issue == other.issue
    }

    /// Hash consistent with `__eq__`.
    pub fn __hash__(&self) -> u64 {
        crate::collections::python_hash(&(
            self.geometry.crs_ref(),
            self.geometry.epoch().map(f64::to_bits),
            &self.geometry.shape,
            &self.issue,
        ))
    }

    /// Pickle support: serialize the geometry only; the verdict is recomputed
    /// on unpickle (never trusts derived state in the payload).
    pub fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, ValidationReportPickleArgs)> {
        Ok((
            crate::gometry_lib_module(py)?
                .getattr(pyo3::intern!(py, "_unpickle_validation_report"))?
                .unbind(),
            (Typed(self.geometry.clone()),),
        ))
    }

    pub fn __repr__(&self) -> String {
        self.issue.as_ref().map_or_else(
            || "<ValidationReport valid>".to_owned(),
            |issue| format!("<ValidationReport invalid reason={:?}>", issue.reason),
        )
    }
}
}

impl HeapSize for PyValidationReport {
    fn heap_bytes(&self) -> usize {
        self.geometry.shape.shape().coordinate_bytes() + self.issue.heap_bytes()
    }
}

impl HeapSize for ValidationIssue {
    fn heap_bytes(&self) -> usize {
        self.reason.heap_bytes() + self.path.heap_bytes()
    }
}
