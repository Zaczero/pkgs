//! Binary-operation broadcasts layered on the [`crate::dispatch`] spine.
//!
//! `broadcast2`/`array_binary_*`/`geometry_binary*` bind the spine's resolver
//! policies so callers get a slim signature, and this module owns the genuinely
//! broadcast-specific logic: prepared predicate kernels, CRS-aware
//! distance/dwithin broadcasts, and shared classifiers/helpers. Reaches the
//! crate-root model and sibling modules via `use super::*`; re-exported at the
//! crate root.
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::geometry::storage;
use crate::{
    Arc, Bound, Bounds, CoordinateAxes, CoordinateInput, F64Param, Frame, GeoJsonDecodeContext,
    GeometryArrayStorage, GeometryError, GeometryErrorKind, GeometryKind, Point, PreparedPlan, Py,
    PyBool, PyErr, PyGeometry, PyGeometryArray, PyResult, PyTypeError, Python, Shape, ShapeData,
    broadcast_coordinate_input, coerce_geojson_geometry_value, coordinate_input_with_expected,
    coordinate_sequence_len_hint, crs, crs_arc, float64_array, is_mapping_like, is_one_byte_buffer,
    parse_geojson_geometry_value, point_distance, scalar_vs_shapes,
};
mod element;
pub(crate) use element::BulkElement;

use crate::error::Result;
use crate::predicates::PredicateSpec;
use crate::py::numpy::bool_array;

mod input;
pub(crate) use input::{
    GeometryInput, classify_input, classify_required, coerce_geometry, exact_geometry,
    exact_geometry_array, expected_geometry_or_array, expected_geometry_or_array_for,
    parse_wkb_geometry, validate_paired_arrays,
};
mod metrics;
pub(crate) use metrics::{
    DistanceUnit, LonLatPointsError, SimilarityOp, array_crs_distances, array_crs_dwithin,
    array_crs_dwithin_per_element, array_crs_frechet_metric_per_densify, array_crs_metric_float,
    lonlat_points, lonlat_shape, metric_maximum_inscribed_radius, metric_minimum_clearance,
    metric_minimum_clearance_line, metric_nearest_points_resolved, metric_shortest_line_resolved,
    pair_distance_geodesic, pair_distance_planar, pair_dwithin_geodesic, pair_dwithin_planar,
    resolve_metric, resolve_metric_3d, resolve_metric_at,
};
mod predicates;
pub(crate) use predicates::{
    bool_array_mask_missing, ensure_same_len, predicate_pairwise_arrays, predicate_scalar_vs_array,
    relate_pattern_broadcast, relate_string_broadcast, splitter_points_from_array, xy_predicate,
};

/// The single engine behind the binary value-returning broadcast families
/// (bool / f64 / String and their fallible variants).
///
/// Resolves both operands
/// over the four scalar×array shapes, checks CRS/epoch metadata ONCE per
/// operand pair-set under the GIL (arrays are uniform-frame by construction),
/// then applies `op` per pair inside `py.detach` — rows stream allocation-free
/// off a clone of the storage `Arc`. Infallible callers return `Ok`; metric
/// errors flow through `GeometryErrorKind` (converted to `PyErr` after the GIL is
/// reacquired, since `PyErr` is not `Send`).
pub(crate) fn broadcast2<R>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    op: impl Fn(&PreparedPlan, &PreparedPlan) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    R: Send + BulkElement,
{
    crate::dispatch::dispatch_broadcast2(py, left, right, op_name, op)
}

/// Binary broadcast for geometry-returning ops: a scalar yields the matching
/// `Typed` leaf, any array operand yields a `GeometryArray`.
pub(crate) fn broadcast2_geometry<K>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    kernel: K,
) -> PyResult<Py<PyAny>>
where
    K: Fn(&PreparedPlan, &PreparedPlan, &crate::dispatch::CachelessRowContext<'_>) -> Result<Shape>
        + Send
        + Sync,
{
    crate::dispatch::dispatch_binary_geometry(
        py,
        left,
        right,
        op_name,
        crate::dispatch::MetricResolver::GeographicCarry,
        move |left, right, context| kernel(left, right, context),
    )
}

/// Enumerated fallible collection for per-element array kernels: a failure
/// rides out with its row so [`rows_err`] can attach the
/// "while processing array element {row}" note at the `PyErr` boundary.
/// One spelling for every per-element loop — bare `collect::<Result<..>>`
/// in an array lane loses the row and is a bug.
pub(crate) trait CollectRows<T> {
    fn collect_rows(self) -> Result<Vec<T>, (usize, crate::error::Error)>;
}

impl<T, I: Iterator<Item = Result<T, crate::error::Error>>> CollectRows<T> for I {
    fn collect_rows(self) -> Result<Vec<T>, (usize, crate::error::Error)> {
        let mut out = Vec::new();
        for (row, result) in self.enumerate() {
            match result {
                Ok(value) => out.push(value),
                Err(error) => return Err((row, error)),
            }
        }
        Ok(out)
    }
}

/// The boundary adapter for [`CollectRows`]: convert the failing row's
/// error and attach its note.
pub(crate) fn rows_err((row, error): (usize, crate::error::Error)) -> PyErr {
    crate::note_array_row(error.into(), row)
}

pub(crate) fn mask_missing<T: Copy>(
    values: &mut [T],
    missing: Option<&storage::ValidityMask>,
    sentinel: T,
) {
    if let Some(mask) = missing {
        for (value, is_missing) in values.iter_mut().zip(mask.iter()) {
            if is_missing {
                *value = sentinel;
            }
        }
    }
}

pub(crate) fn detach_collect_f64(
    py: Python<'_>,
    f: impl FnOnce() -> Result<Vec<f64>, (usize, crate::error::Error)> + Send,
) -> PyResult<Py<PyAny>> {
    float64_array(py, py.detach(f).map_err(rows_err)?)
}

/// Whether a linear-referencing error is instead answered with an empty point,
/// an empty line, or NaN. Empty linework has no positions to reference, so
/// every lane answers it. Missing or non-monotonic M is the caller's error for
/// one geometry, but one row's data in an array, which must not abort the batch.
/// Wrong-kind and parameter errors always raise.
pub(crate) fn linref_error_degrades(
    error: &crate::error::Error,
    lane: crate::dispatch::Lane,
) -> bool {
    use crate::geometry::GeometryErrorKind as G;
    matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(G::EmptyLinework)
    ) || (lane.is_array()
        && matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(G::MissingMeasure | G::NonMonotonicMeasure)
        ))
}

fn degrade_linref<T>(
    result: Result<T>,
    lane: crate::dispatch::Lane,
    empty: impl FnOnce() -> T,
) -> Result<T> {
    match result {
        Err(error) if linref_error_degrades(&error, lane) => Ok(empty()),
        result => result,
    }
}

pub(crate) fn degrade_linref_float(
    result: Result<f64>,
    lane: crate::dispatch::Lane,
) -> Result<f64> {
    degrade_linref(result, lane, || f64::NAN)
}

pub(crate) fn degrade_linref_point(
    result: Result<crate::geometry::Shape>,
    lane: crate::dispatch::Lane,
) -> Result<crate::geometry::Shape> {
    degrade_linref(result, lane, crate::geometry::Shape::empty_point)
}

pub(crate) fn degrade_linref_linestring(
    result: Result<crate::geometry::Shape>,
    lane: crate::dispatch::Lane,
) -> Result<crate::geometry::Shape> {
    degrade_linref(result, lane, || {
        crate::geometry::Shape::typed_empty(
            crate::geometry::GeometryKind::LineString,
            crate::geometry::CoordinateAxes::XY,
        )
    })
}

/// Value-returning binary broadcast whose `op` also receives the ROW INDEX —
/// the hook for a per-element [`F64Param`] magnitude on a value op (e.g.
/// `equals_exact(other, tolerance=[...])`). Mirrors the scalar/array reference
/// broadcast shape of [`dispatch_binary_array_left`].
pub(crate) fn array_binary_values_indexed<R>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    op: impl Fn(&Shape, &Shape, usize) -> Result<R> + Send + Sync,
) -> PyResult<Vec<R>>
where
    R: Send + BulkElement,
{
    crate::dispatch::dispatch_binary_array_left(
        py,
        left,
        right,
        op_name,
        crate::dispatch::MetricResolver::None,
        move |left, right, ctx| op(left.shape(), right.shape(), ctx.lane.row()),
    )
}

pub(crate) fn py_bool(py: Python<'_>, value: bool) -> Py<PyAny> {
    PyBool::new(py, value).to_owned().unbind().into()
}

pub(crate) fn py_bool_or_not_implemented<T>(
    py: Python<'_>,
    value: Option<T>,
    predicate: impl FnOnce(T) -> bool,
) -> Py<PyAny> {
    value.map_or_else(
        || py.NotImplemented(),
        |value| py_bool(py, predicate(value)),
    )
}
