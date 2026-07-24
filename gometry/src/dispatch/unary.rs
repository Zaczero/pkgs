use pyo3::prelude::*;
use pyo3::types::PyAny;

use super::element::BulkElement;
use super::metric::{Lane, MetricScratch, OpCtx};
use super::operation::Operation;
use super::packed::{
    PackedUnary, try_unary_packed_array, try_unary_packed_bool, try_unary_packed_bounds,
    try_unary_packed_f64,
};
use crate::boundary::metadata::Frame;
use crate::broadcast::{
    CollectRows, GeometryInput, classify_input, expected_geometry_or_array, rows_err,
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
    if let Some(result) = try_unary_packed_array(py, array, op, packed) {
        return Ok(result?.into_pyobject(py)?.unbind().into());
    }
    let (values, output_frame) = run_unary_rows(py, array, resolver, op_name, kernel)?;
    R::bulk_into_py_masked(values, py, &output_frame, array.missing())
}

/// The per-row unary lane shared by every array return type: resolve the
/// metric once, run the kernel over each row inside one ``py.detach``.
fn run_unary_rows<T: BulkElement>(
    py: Python<'_>,
    array: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    op_name: &str,
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
                    let row_cache = array.row_frame_cache(row);
                    let ctx = OpCtx {
                        frame,
                        geographic,
                        metric,
                        lane: Lane::Array(row),
                        left_frame_cache: Some(&row_cache),
                        right_frame_cache: None,
                    };
                    array.with_row_data(row, shape_row, |data| kernel(data, &ctx))
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
pub(crate) fn unary_array_shapes(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    unit: Option<DistanceUnit>,
    packed: Option<&PackedUnary>,
    kernel: impl Fn(&crate::ShapeData, &OpCtx<'_>) -> Result<Shape> + Send + Sync,
) -> PyResult<PyGeometryArray> {
    if let Some(result) = try_unary_packed_array(py, array, op, packed) {
        return result;
    }
    let resolver = op.resolver_with_unit(unit);
    let (shapes, frame) = run_unary_rows(py, array, resolver, op.name(), kernel)?;
    Ok(PyGeometryArray::from_shapes(shapes, frame).with_missing_mask(array.missing().cloned()))
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
