use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::boundary::metadata::Frame;
use crate::broadcast::{
    BulkElement, CollectRows as _, GeometryInput, classify_input, expected_geometry_or_array,
    rows_err,
};
use crate::dispatch::metric::{Lane, MetricScratch, OpCtx};
use crate::dispatch::operation::Operation;
use crate::dispatch::packed::{
    PackedUnary, try_unary_packed_array, try_unary_packed_bool, try_unary_packed_bounds,
    try_unary_packed_f64,
};
use crate::error::Result;
use crate::geometry::{Shape, is_geographic_frame};
use crate::{DistanceUnit, PyGeometry, PyGeometryArray, Typed};

/// Scalar geometry lane: resolve metric once, run one kernel, return the bulk element.
pub(crate) fn unary_scalar<R: BulkElement>(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    unit: Option<DistanceUnit>,
    kernel: impl FnOnce(&crate::ShapeData, &OpCtx<'_>) -> Result<R> + Send,
) -> PyResult<R> {
    let op_name = op.name();
    let resolver = op.resolver_with_unit(unit);
    let frame = geometry.frame.clone();
    let geographic = is_geographic_frame(&frame);
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, op_name, &mut metric_scratch)?;
    let ctx = OpCtx {
        frame: &frame,
        geographic,
        metric,
        lane: Lane::Scalar,
        left_frame_cache: Some(&geometry.frame_cache),
        right_frame_cache: None,
    };
    py.detach(move || kernel(geometry.shape.as_ref(), &ctx))
        .map_err(PyErr::from)
}

/// How the per-row unary fallback materializes a [`ShapeData`] handle.
///
/// Pure topology transforms only need the bare shape; persisting prepared
/// state (or frame caches) on every row is a memory leak against the
/// caller's intent. Validity/simplicity and other genuine `ShapeData`
/// consumers keep the prepared/array-cached path. Distance-basis linref
/// needs a frame cache without necessarily needing prepared persistence.
#[derive(Clone, Copy)]
enum UnaryRowMode {
    /// Transient stack handle — no prepared-slot or frame-cache write.
    ShapeOnly,
    /// Persist large/mixed prepared handles into the array cache; still no
    /// eager frame-cache acquisition.
    Prepared,
    /// Transient `ShapeData` + per-row frame cache (distance-basis linref).
    ShapeWithFrameCache,
}

/// Array geometry lane: resolve metric once, try packed fast paths, else per-row kernel.
pub(crate) fn unary_array<R: BulkElement>(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    unit: Option<DistanceUnit>,
    packed: Option<&PackedUnary>,
    kernel: impl Fn(&crate::ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let op_name = op.name();
    let resolver = op.resolver_with_unit(unit);
    if let Some(result) = try_unary_packed_f64(py, array, op, unit, op_name) {
        return result;
    }
    if let Some(result) = try_unary_packed_bool(py, array, op) {
        return result;
    }
    if op == Operation::Bounds
        && let Some(result) = try_unary_packed_bounds(py, array)
    {
        return result;
    }
    if let Some(result) = try_unary_packed_array(py, array, op, resolver, packed) {
        return Ok(result?.into_pyobject(py)?.unbind().into());
    }
    // Bool/fact kernels (is_valid/is_simple/…) amortize prepared state across
    // repeats; distance-basis linref needs a frame cache; pure geometry
    // transforms use the Shape-only lane.
    let mode = unary_row_mode(op);
    let (values, output_frame) = run_unary_rows(py, array, resolver, op_name, mode, kernel)?;
    R::bulk_into_py_masked(values, py, &output_frame, array.missing())
}

const fn unary_row_mode(op: Operation) -> UnaryRowMode {
    match op {
        Operation::IsValid
        | Operation::IsSimple
        | Operation::IsRing
        | Operation::Bounds
        | Operation::ClipByRect => UnaryRowMode::Prepared,
        // Distance-basis LRS kernels read `ctx.left_frame_cache`; M-basis
        // siblings ignore it. Acquiring only for these ops keeps simplify /
        // reverse / etc. cache-free.
        Operation::LineInterpolate | Operation::LineSubstring | Operation::LineLocate => {
            UnaryRowMode::ShapeWithFrameCache
        },
        _ => UnaryRowMode::ShapeOnly,
    }
}

/// The per-row unary lane shared by every array return type: resolve the
/// metric once, run the kernel over each row inside one ``py.detach``.
fn run_unary_rows<T: BulkElement>(
    py: Python<'_>,
    array: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    mode: UnaryRowMode,
    kernel: impl Fn(&crate::ShapeData, &OpCtx<'_>) -> Result<T> + Send + Sync,
) -> PyResult<(Vec<T>, Frame)> {
    let frame = array.frame.clone();
    let output_frame = frame.clone();
    let geographic = is_geographic_frame(&frame);
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, op_name, &mut metric_scratch)?;
    let array = array.clone();
    let values = py
        .detach(move || {
            let frame = &frame;
            array
                .storage()
                .iter_rows()
                .enumerate()
                .map(|(row, shape_row)| {
                    if array.is_row_missing(row) {
                        // Missing rows never reach the kernel: they yield the
                        // element's missing value (NaN / false / placeholder).
                        return Ok(T::missing_value());
                    }
                    let row_cache;
                    let left_frame_cache = match mode {
                        UnaryRowMode::ShapeWithFrameCache => {
                            row_cache = array.row_frame_cache(row);
                            Some(row_cache.as_ref())
                        },
                        UnaryRowMode::ShapeOnly | UnaryRowMode::Prepared => None,
                    };
                    let ctx = OpCtx {
                        frame,
                        geographic,
                        metric,
                        lane: Lane::Array(row),
                        left_frame_cache,
                        right_frame_cache: None,
                    };
                    match mode {
                        UnaryRowMode::ShapeOnly | UnaryRowMode::ShapeWithFrameCache => {
                            // Transient handle — never write prepared slots.
                            shape_row.with_data(|data| kernel(data, &ctx))
                        },
                        UnaryRowMode::Prepared => {
                            array.with_row_data(row, shape_row, |data| kernel(data, &ctx))
                        },
                    }
                })
                .collect_rows()
        })
        .map_err(rows_err)?;
    Ok((values, output_frame))
}

/// Scalar geometry method returning a typed leaf geometry.
pub(crate) fn unary_scalar_shape(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    unit: Option<DistanceUnit>,
    kernel: impl FnOnce(&crate::ShapeData, &OpCtx<'_>) -> Result<Shape> + Send,
) -> PyResult<Typed> {
    let frame = geometry.frame.clone();
    let shape = unary_scalar(py, geometry, op, unit, kernel)?;
    Ok(Typed(PyGeometry::with_frame(shape, frame)))
}

/// Array geometry method returning a `GeometryArray` — natively, without the
/// Python-object round-trip (`Py<PyAny>` -> downcast -> borrow -> clone) the
/// generic lane would pay.
///
/// Geometry-returning unaries: prefer pure-`Shape` fallback (no prepared
/// persistence). Distance-basis linref still acquires a frame cache via
/// [`unary_row_mode`].
pub(crate) fn unary_array_shapes(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    unit: Option<DistanceUnit>,
    packed: Option<&PackedUnary>,
    kernel: impl Fn(&crate::ShapeData, &OpCtx<'_>) -> Result<Shape> + Send + Sync,
) -> PyResult<PyGeometryArray> {
    let resolver = op.resolver_with_unit(unit);
    if let Some(result) = try_unary_packed_array(py, array, op, resolver, packed) {
        return result;
    }
    let (shapes, frame) =
        run_unary_rows(py, array, resolver, op.name(), unary_row_mode(op), kernel)?;
    Ok(PyGeometryArray::from_shapes(shapes, frame).with_missing_mask(array.missing().cloned()))
}

/// Geometry-returning array lane for constructive operations whose generated
/// work must be bounded across the entire logical array.  It intentionally
/// stays serial inside one detached closure: the mutable budget is the owner
/// shared by rows, while the kernel threads it into all nested parts/rings.
pub(crate) fn unary_array_shapes_budgeted(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    unit: Option<DistanceUnit>,
    parameter: &'static str,
    mut kernel: impl FnMut(
        &crate::ShapeData,
        &OpCtx<'_>,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<Shape>
    + Send,
) -> PyResult<PyGeometryArray> {
    let resolver = op.resolver_with_unit(unit);
    let frame = array.frame.clone();
    let output_frame = frame.clone();
    let geographic = is_geographic_frame(&frame);
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, op.name(), &mut metric_scratch)?;
    let missing = array.missing().cloned();
    let array = array.clone();
    let shapes = py
        .detach(move || {
            let mut budget = crate::geometry::ExpansionBudget::new(op.name(), parameter);
            array
                .storage()
                .iter_rows()
                .enumerate()
                .map(|(row, shape_row)| {
                    if array.is_row_missing(row) {
                        return Ok(Shape::empty_point());
                    }
                    let ctx = OpCtx {
                        frame: &frame,
                        geographic,
                        metric,
                        lane: Lane::Array(row),
                        left_frame_cache: None,
                        right_frame_cache: None,
                    };
                    shape_row.with_data(|data| kernel(data, &ctx, &mut budget))
                })
                .collect_rows()
        })
        .map_err(rows_err)?;
    Ok(PyGeometryArray::from_shapes(shapes, output_frame).with_missing_mask(missing))
}

/// Unified unary scalar/array dispatch: classify once, resolve metric once,
/// try packed-column fast paths on the array lane, then run one kernel per row
/// inside a single ``py.detach``, bulk-convert via [`BulkElement`].
pub(crate) fn dispatch_unary<R: BulkElement>(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    op: Operation,
    unit: Option<DistanceUnit>,
    packed: Option<&PackedUnary>,
    kernel: impl Fn(&crate::ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    use GeometryInput::{Many, One};
    let Some(input) = classify_input(value) else {
        return Err(expected_geometry_or_array());
    };
    match input {
        One(geometry) => {
            let frame = geometry.frame.clone();
            let result = unary_scalar(py, geometry, op, unit, |data, ctx| kernel(data, ctx))?;
            R::into_py(result, py, &frame)
        },
        Many(array) => unary_array(py, array, op, unit, packed, kernel),
    }
}
