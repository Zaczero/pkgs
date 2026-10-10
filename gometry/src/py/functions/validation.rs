//! Validation-at-the-boundary free functions: `require` and `repair`.
//!
//! `require` parses an input and enforces a geometry contract (kind/dimension/
//! CRS); `repair` fixes invalid geometries. Both delegate to the crate-root and
//! `geometry` validation kernels via `use super::*`.

use pyo3::prelude::*;

use crate::geometry::storage::GeometryArrayStorage;
use crate::geometry::types::ShapeHeapMemo;
use crate::py::errors::crs_mismatch_error;
use crate::py::functions::bulk_rows::{StreamedRow, stream_bulk};
use crate::py::sizeof::python_sizeof;
use crate::py::wire_crs::{rebase_ewkt_parse_error, resolve_wire_crs, split_ewkt_srid};
use crate::py::{errors, support, vectors};
use crate::{
    CoordinateAxes, Crs, FrameEdit, GeoJsonDecodeContext, HeapSize, PyGeometry, PyGeometryArray,
    ScalarOrIterator, Typed, ValidationIssue, classify_scalar_or_iterator,
    coerce_geojson_geometry_value, crs_arc, crs_label, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, geometry, guard_epoch_frame, io, is_one_byte_buffer, is_wkt_string,
    parse_crs, parse_geojson_geometry_value, parse_geojson_slice, parse_wkb_geometry,
    with_one_byte_buffer,
};

/// Parse and require a geometry contract at an input boundary.
///
/// Parameters
/// ----------
/// value : geometry-like or iterable of geometry-like
///     One geometry, a `GeometryArray`, a geometry-valued `Groups`, or an
///     iterable. Foreign scalar inputs may be WKT, WKB, GeoJSON mappings/text,
///     or ``__geo_interface__`` objects. Geometry-valued groups are flattened
///     in group order and return a `GeometryArray`.
/// crs : CrsInput, optional
///     Expected CRS for the result. CRS-free coordinates are stamped with
///     this CRS; coordinates are never transformed. An existing CRS that is
///     operationally equivalent is relabelled to this requested spelling,
///     while a conflicting CRS raises ``CRSMismatchError``. When omitted or
///     ``None``, no target is required and an existing CRS is preserved.
///     Accepted forms are an EPSG integer, authority/WKT/PROJ string,
///     ``(authority, code)`` pair, PROJJSON/CF mapping, or an object exposing
///     ``to_wkt()`` such as ``gometry.CRS`` or ``pyproj.CRS``. Coordinate
///     epochs are preserved and revalidated against the resulting CRS.
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
///     unsupported or conflicts with ``crs`` (``format`` is ``"geojson"``).
/// CRSMismatchError
///     If an already-decoded / native / non-GeoJSON geometry's CRS conflicts
///     with ``crs``. EWKT/EWKB embedded SRID conflicts use this error too.
/// InvalidGeometryError
///     If the geometry is invalid, or its axes differ from ``axes``.
///     Geographic validity uses exact quotient topology, so longitude aliases
///     and seam cuts do not depend on stored spelling. Out-of-domain latitude
///     is invalid. For quotient-topology issues, the location is a canonical
///     coordinate of the issue's contact set, chosen deterministically by
///     minimum latitude then longitude.
///     Projected and CRS-free geometry remains planar.
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
        let geometry =
            require_geometry_contract(geometry.clone(), expected.as_ref(), axes, None, Some(py))?;
        return Ok(Typed(geometry).into_pyobject(py)?.into_any().unbind());
    }
    if let Some(array) = exact_geometry_array(value) {
        let array = require_array_contract(array.clone(), expected.as_ref(), axes, Some(py))?;
        return Ok(array.into_pyobject(py)?.into_any().unbind());
    }
    if let Ok(groups) = value.cast::<vectors::Groups>()
        && let Some(array) = groups.get().geometry_values_view()?
    {
        let array = require_array_contract(array, expected.as_ref(), axes, Some(py))?;
        return Ok(array.into_pyobject(py)?.into_any().unbind());
    }
    match classify_scalar_or_iterator(value)? {
        ScalarOrIterator::Scalar => {
            let geometry = require_geometry_contract(
                parse_require_input(value, expected.as_ref())?,
                expected.as_ref(),
                axes,
                None,
                None,
            )?;
            Ok(Typed(geometry).into_pyobject(py)?.into_any().unbind())
        },
        ScalarOrIterator::Iterator(iter) => {
            let expected_for_rows = expected.clone();
            let array = stream_bulk(
                value,
                iter,
                expected,
                None,
                Some("GeometryArray"),
                move |item, rows, row| {
                    let geometry = if let Some(geometry) = exact_geometry(item) {
                        geometry.clone()
                    } else {
                        parse_require_input(item, expected_for_rows.as_ref())?
                    };
                    let geometry = require_geometry_contract(
                        geometry,
                        expected_for_rows.as_ref(),
                        axes,
                        Some(row),
                        None,
                    )?;
                    rows.try_push(geometry.shape.shape().clone())?;
                    Ok(StreamedRow {
                        crs: geometry.crs_ref().cloned(),
                        epoch: geometry.epoch(),
                    })
                },
            )?;
            Ok(array.into_pyobject(py)?.into_any().unbind())
        },
    }
}

/// Whether a `require(crs=)` contract needs the value retagged to `expected`,
/// or `Err` when the value is in a genuinely different frame.
///
/// `require` *attaches* rather than merely asserting — it already labels
/// CRS-free input — so a value already in the requested frame under a
/// different spelling is retagged to the label the caller asked for. That is
/// the same rule `GeometryArray(values, crs=...)` applies, and it keeps
/// `require` from rejecting a pair that every predicate and metric accepts.
fn require_crs_retag(
    actual: Option<&Crs>,
    expected: Option<&Crs>,
    row: Option<usize>,
) -> PyResult<bool> {
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
            row,
        )),
    }
}

fn require_geometry_contract(
    mut geometry: PyGeometry,
    expected: Option<&Crs>,
    axes: Option<CoordinateAxes>,
    row: Option<usize>,
    py: Option<Python<'_>>,
) -> PyResult<PyGeometry> {
    if require_crs_retag(geometry.crs_ref(), expected, row)? {
        geometry.set_crs_keep_epoch(expected.cloned())?;
    }
    if let Some(axes) = axes
        && geometry.shape.coordinate_axes() != axes.as_str()
    {
        let issue = ValidationIssue::new(
            "coordinate_axes_mismatch",
            format!(
                "expected axes {:?}, got {:?}",
                axes.as_str(),
                geometry.shape.coordinate_axes(),
            ),
            None,
            "$",
        );
        return Err(errors::invalid_geometry_issue(&issue, row));
    }
    let plan = geometry.prepared_plan();
    let domain = geometry::topology_domain(&geometry.frame)?;
    let issue = match py {
        Some(py) => py.detach(move || geometry::validate_data_in_frame(&plan, domain)),
        None => geometry::validate_data_in_frame(&plan, domain),
    }?;
    if let Some(issue) = issue {
        return Err(errors::invalid_geometry_issue(&issue, row));
    }
    Ok(geometry)
}

fn require_array_contract(
    mut array: PyGeometryArray,
    expected: Option<&Crs>,
    axes: Option<CoordinateAxes>,
    py: Option<Python<'_>>,
) -> PyResult<PyGeometryArray> {
    if require_crs_retag(array.crs_ref(), expected, None)? {
        // Attach the pre-parsed CRS without re-parsing a Bound token.
        // `overwrite` is safe here: `require_crs_retag` only asks for a retag
        // once the existing label is absent or names the very same frame.
        let frame = FrameEdit::SetCrs {
            crs: expected.cloned(),
            overwrite: true,
        }
        .apply(&array.frame)?;
        array = array.retag_frame(frame);
    }
    if let Some(axes) = axes {
        let axes_diagnostic = match array.storage() {
            GeometryArrayStorage::Packed(packed) => {
                let actual = packed.axes();
                (actual != axes).then(|| {
                    let row = array
                        .row_operation()
                        .present_rows()
                        .next()
                        .map(|row| row.logical());
                    (format!("{:?}", actual.as_str()), row)
                })
            },
            GeometryArrayStorage::Mixed(_) => {
                let mut seen: Vec<CoordinateAxes> = Vec::new();
                let mut first_mismatch = None;
                for row in array.row_operation().present_rows() {
                    let row_axes = row.row().axes();
                    if !seen.contains(&row_axes) {
                        seen.push(row_axes);
                    }
                    if first_mismatch.is_none() && row_axes != axes {
                        first_mismatch = Some(row.logical());
                    }
                }
                let mismatch =
                    first_mismatch.is_some() || (seen.is_empty() && axes != CoordinateAxes::XY);
                mismatch.then(|| {
                    let got = match seen.as_slice() {
                        [] => format!("{:?}", CoordinateAxes::XY.as_str()),
                        [one] => format!("{:?}", one.as_str()),
                        many => format!(
                            "mixed ({})",
                            many.iter()
                                .map(|axes| axes.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    };
                    (got, first_mismatch)
                })
            },
        };
        if let Some((got, row)) = axes_diagnostic {
            let issue = ValidationIssue::new(
                "coordinate_axes_mismatch",
                format!("expected axes {:?}, got {got}", axes.as_str()),
                None,
                "$",
            );
            return Err(errors::invalid_geometry_issue(&issue, row));
        }
    }
    let domain = geometry::topology_domain(&array.frame)?;
    let first_issue = |array: &PyGeometryArray| -> crate::error::Result<_> {
        for row in array.row_operation().present_rows() {
            let logical = row.logical();
            if let Some(issue) = row
                .row()
                .try_with_shape(|shape| geometry::validate_shape_in_frame(shape, domain))?
            {
                return Ok(Some((logical, issue)));
            }
        }
        Ok(None)
    };
    if let Some(py) = py {
        let (array, issue) = py.detach(move || {
            let issue = first_issue(&array)?;
            Ok::<_, crate::error::Error>((array, issue))
        })?;
        if let Some((row, issue)) = issue {
            return Err(errors::invalid_geometry_issue(&issue, Some(row)));
        }
        return Ok(array);
    }
    if let Some((row, issue)) = first_issue(&array)? {
        return Err(errors::invalid_geometry_issue(&issue, Some(row)));
    }
    Ok(array)
}

fn parse_require_wkt(text: &str, fallback: Option<&Crs>) -> PyResult<PyGeometry> {
    let (body, srid) = split_ewkt_srid(text)?;
    let embedded = io::crs_from_optional_srid(srid)?;
    let crs = resolve_wire_crs(embedded.map(crs_arc), fallback, "EWKT SRID")?;
    guard_epoch_frame(None, crs.as_ref())?;
    Ok(PyGeometry::with_epoch(
        io::parse_wkt(body).map_err(|error| rebase_ewkt_parse_error(error, text, body))?,
        crs,
        None,
    )?)
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
    let resolved = resolve_wire_crs(geometry.crs_ref().cloned(), fallback, "EWKB SRID")?;
    geometry.set_crs_keep_epoch(resolved)?;
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
        let parsed = support::parse_geojson_text(text.as_ref())?;
        return parse_require_geojson_value(&parsed, fallback);
    }
    if is_one_byte_buffer(geom)? {
        return parse_require_buffer(geom, fallback);
    }
    let parsed = parse_geojson_geometry_value(geom)?;
    parse_require_geojson_value(&parsed, fallback)
}

/// A structured geometry-validity verdict.
///
/// Returned by ``geom.validate()``: truthy when the geometry is valid;
/// otherwise ``report.reason`` names the OGC violation, ``report.location``
/// pinpoints it, ``report.path`` addresses the offending part, and
/// ``report.code``/``report.detail`` provide stable machine-readable and
/// human-readable issue data.
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

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled `ValidationReport` from its geometry only (internal;
/// see ``ValidationReport.__reduce__``).
///
/// The verdict is always re-derived via the same path as ``Geometry.validate``
/// — forged `.valid` / nonfinite diagnostic locations in the payload are never
/// trusted.
#[pyfunction]
pub(crate) fn _unpickle_validation_report_v1(
    py: Python<'_>,
    geometry: &Bound<'_, PyAny>,
) -> PyResult<PyValidationReport> {
    let geometry = exact_geometry(geometry)
        .ok_or_else(expected_geometry_or_array)?
        .clone();
    geometry.validate_impl(py)
}

// --- ValidationReport #[pymethods] (moved from crate root) ---

frozen_pymethods! {
impl PyValidationReport {
    /// ``case ValidationReport(False, reason)`` destructures the verdict.
    #[classattr]
    const fn __match_args__() -> (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
    ) {
        ("valid", "reason", "location", "path", "code", "detail")
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
        self.issue.as_ref().map(|issue| issue.detail.clone())
    }

    /// Stable machine-readable validity code, or ``None`` for a valid report.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    pub fn code(&self) -> Option<&'static str> {
        self.issue.as_ref().map(|issue| issue.code)
    }

    /// Detail for the first validity issue, or ``None`` for a valid report.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    pub fn detail(&self) -> Option<String> {
        self.issue.as_ref().map(|issue| issue.detail.clone())
    }

    /// ``(x, y)`` location of the first problem, when known.
    ///
    /// For quotient-topology issues, the location is a canonical coordinate of
    /// the issue's contact set, chosen deterministically by minimum latitude
    /// then longitude.
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

    /// ``sys.getsizeof`` support: Python's allocation base plus the retained geometry
    /// coordinate payload and any validation issue strings.
    ///
    /// Raises
    /// ------
    /// OverflowError
    ///     If the reported size exceeds Python's ``Py_ssize_t`` range.
    pub fn __sizeof__(slf: &Bound<'_, Self>) -> PyResult<isize> {
        python_sizeof(slf.py(), slf.as_any(), slf.borrow().heap_bytes())
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
                .getattr(pyo3::intern!(py, "_unpickle_validation_report_v1"))?
                .unbind(),
            (Typed(self.geometry.clone()),),
        ))
    }

    pub fn __repr__(&self) -> String {
        self.issue.as_ref().map_or_else(
            || "<ValidationReport valid>".to_owned(),
            |issue| format!("<ValidationReport invalid reason={:?}>", issue.detail),
        )
    }
}
}

impl HeapSize for PyValidationReport {
    fn heap_bytes(&self) -> usize {
        ShapeHeapMemo::default()
            .coordinate_bytes(self.geometry.shape.shape())
            .saturating_add(self.issue.heap_bytes())
    }
}

impl HeapSize for ValidationIssue {
    fn heap_bytes(&self) -> usize {
        self.detail
            .heap_bytes()
            .saturating_add(self.path.heap_bytes())
    }
}
