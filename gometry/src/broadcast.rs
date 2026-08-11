//! Binary-operation broadcasts layered on the [`crate::dispatch`] spine.
//!
//! `broadcast2`/`array_binary_*`/`geometry_binary*` bind the spine's defaults
//! (fast-path/resolver) so callers get a slim signature, and this module owns
//! the genuinely broadcast-specific logic: the prepared + point-fast-path
//! predicate optimizations, the CRS-aware distance/dwithin broadcasts, and the
//! small shared classifiers/helpers. Reaches the crate-root model and sibling
//! modules via `use super::*`; re-exported at the crate root.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{
    Arc, Bound, Bounds, CoordSeq, CoordinateAxes, CoordinateInput, Crs, EmptyKind, F64Param, Frame,
    GeoJsonDecodeContext, GeometryArrayStorage, GeometryError, GeometryErrorKind, Point, Py,
    PyBool, PyErr, PyGeometry, PyGeometryArray, PyResult, PyTypeError, Python, Shape, ShapeData,
    ShapeRow, broadcast_coordinate_input, coerce_geojson_geometry_value,
    coordinate_input_with_expected, coordinate_sequence_len_hint, crs, crs_arc, float64_array,
    geometry_type_err, is_mapping_like, is_one_byte_buffer, parse_geojson_geometry_value,
    point_distance, scalar_vs_shapes, validate_lonlat_shape,
};
mod element;
pub(crate) use element::BulkElement;

use crate::error::Result;
use crate::geometry::{LineSeq, PointBatchTester};
use crate::predicates::{PredicateSpec, point_batch};
use crate::py::numpy::bool_array;

mod input;
pub(crate) use input::{
    GeometryInput, classify_input, classify_required, coerce_geometry, exact_geometry,
    exact_geometry_array, expected_geometry_or_array, expected_geometry_or_array_for,
    paired_arrays, paired_arrays_len, parse_wkb_geometry,
};
mod metrics;
pub(crate) use metrics::{
    DistanceUnit, OptionalDensifyParam, array_crs_distances, array_crs_dwithin,
    array_crs_dwithin_per_element, array_crs_dwithin_scalar, array_crs_metric_float,
    array_crs_similarity_metric_per_densify, crs_aware_dwithin,
    crs_metric_binary_geometry_broadcast, lonlat_shape, lonlat_shape_under,
    metric_frechet_densified, metric_hausdorff_densified, metric_maximum_inscribed_radius,
    metric_minimum_clearance, metric_minimum_clearance_line, metric_nearest_points,
    metric_shortest_line, pair_distance_resolved, pair_distance_resolved_result,
    pair_dwithin_resolved, pair_dwithin_resolved_result, require_point, resolve_metric,
    resolve_metric_3d, validate_densify,
};
mod predicates;
pub(crate) use predicates::{
    bool_array_mask_missing, ensure_same_len, multipoint_splitter_from_array, predicate_broadcast,
    predicate_pairwise_arrays, predicate_scalar_vs_array, relate_pattern_broadcast,
    relate_string_broadcast, xy_predicate, xy_predicate_geometry, xy_predicate_values,
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
    op: impl Fn(&Shape, &Shape) -> Result<R> + Send + Sync,
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
    K: Fn(&ShapeData, &ShapeData) -> Result<Shape> + Send + Sync,
{
    crate::dispatch::dispatch_binary_geometry(
        py,
        left,
        right,
        op_name,
        &crate::dispatch::OverlayFastPath,
        move |left, right, _ctx| kernel(left, right),
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
        let (low, _) = self.size_hint();
        let mut out = Vec::with_capacity(low);
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
    missing: Option<&crate::array::MissingMask>,
    sentinel: T,
) {
    if let Some(mask) = missing {
        for (value, &is_missing) in values.iter_mut().zip(mask.iter()) {
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
    crate::py::numpy::float64_array(py, py.detach(f).map_err(rows_err)?)
}

/// True if `error` is a per-row line-data condition that the ARRAY linref ops
/// degrade to a sentinel (empty line / missing or non-monotonic M) rather than
/// aborting the batch. Wrong-kind and parameter errors are NOT degradable.
pub(crate) fn is_degradable_line_row(error: &crate::error::Error) -> bool {
    use crate::geometry::GeometryErrorKind as G;
    matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(
            G::EmptyLinework | G::MissingMeasure | G::NonMonotonicMeasure
        )
    )
}

pub(crate) fn degrade_linref_float(result: Result<f64>) -> Result<f64> {
    result.or_else(|error| {
        if is_degradable_line_row(&error) {
            Ok(f64::NAN)
        } else {
            Err(error)
        }
    })
}

pub(crate) fn degrade_linref_point(
    result: Result<crate::geometry::Shape>,
) -> Result<crate::geometry::Shape> {
    result.or_else(|error| {
        if is_degradable_line_row(&error) {
            Ok(crate::geometry::Shape::empty_point())
        } else {
            Err(error)
        }
    })
}

pub(crate) fn degrade_linref_linestring(
    result: Result<crate::geometry::Shape>,
) -> Result<crate::geometry::Shape> {
    result.or_else(|error| {
        if is_degradable_line_row(&error) {
            Ok(crate::geometry::Shape::LineString(
                crate::geometry::LineSeq::empty(crate::geometry::CoordinateAxes::XY),
            ))
        } else {
            Err(error)
        }
    })
}

pub(crate) fn degrade_linref_point_between(
    result: Result<crate::geometry::Point>,
) -> Result<crate::geometry::Shape> {
    match result {
        Ok(point) => Ok(crate::geometry::Shape::Point(point)),
        Err(error) if is_degradable_line_row(&error) => Ok(crate::geometry::Shape::empty_point()),
        Err(error) => Err(error),
    }
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

/// Geometry-returning pairwise
/// `kernel` over `left` × `right` producing a `GeometryArray`, DETACHED
/// like the value form (frames resolved once; pure shape work without the
/// GIL).
pub(crate) fn array_binary_geometry<K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    kernel: K,
) -> PyResult<PyGeometryArray>
where
    K: Fn(&ShapeData, &ShapeData) -> Result<Shape> + Send + Sync,
{
    let right = classify_required(right)?;
    crate::dispatch::geometry_kernel_over_array(
        py,
        left,
        right,
        op_name,
        crate::dispatch::MetricResolver::None,
        move |left, right, _ctx| kernel(left, right),
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

/// Collect polygonal rings as bare `Shape::LineString` values (no scalar
/// `PyGeometry` staging). Used by free `gm.rings`.
pub(crate) fn push_ring_shapes(shape: &Shape, rings: &mut Vec<Shape>) -> PyResult<()> {
    if !matches!(
        *shape,
        Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::Empty(EmptyKind::Polygon | EmptyKind::MultiPolygon, _)
    ) {
        return Err(crate::py::errors::geometry_type_err(
            "rings requires Polygon or MultiPolygon geometries",
        ));
    }
    shape.for_each_ring(|ring| {
        rings.push(Shape::LineString(LineSeq::from_trusted(ring.clone())));
    });
    Ok(())
}
