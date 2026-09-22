use pyo3::prelude::*;

use crate::array::{ArrayRow, ArrayRowOperation};
use crate::boundary::metadata::Frame;
use crate::broadcast::{
    BulkElement, CollectRows as _, GeometryInput, classify_input, expected_geometry_or_array,
    rows_err,
};
use crate::dispatch::metric::{
    CachelessRowContext, Lane, LinearReferenceRow, MetricCtx, MetricScratch,
};
use crate::dispatch::operation::{MetricOp, Operation};
use crate::dispatch::packed::{
    PackedLinearReference, PackedUnary, try_packed_linear_reference, try_unary_packed_array,
    try_unary_packed_bool, try_unary_packed_bounds, try_unary_packed_f64,
};
use crate::error::Result;
use crate::geometry::{PreparedPlan, Shape, TopologyDomain};
use crate::{DistanceUnit, PyGeometry, PyGeometryArray, Typed};

fn execute_unary_scalar<R: BulkElement>(
    py: Python<'_>,
    geometry: &PyGeometry,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: impl FnOnce(&PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send,
) -> Result<R> {
    let ctx = CachelessRowContext {
        frame: &geometry.frame,
        domain,
        metric,
        lane: Lane::Scalar,
    };
    let plan = geometry.plan_or_transient()?;
    py.detach(move || kernel(&plan, &ctx))
}

/// Scalar geometry lane: resolve metric once, run one kernel, return the bulk element.
pub(crate) fn unary_scalar<R: BulkElement>(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    unit: Option<DistanceUnit>,
    kernel: impl FnOnce(&PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send,
) -> PyResult<R> {
    let resolver = op.resolver_with_unit(unit);
    let domain = crate::geometry::topology_domain(&geometry.frame)?;
    let mut metric_scratch = MetricScratch::default();
    let result = {
        let metric = match resolver.resolve_ctx(&geometry.frame, &mut metric_scratch) {
            Ok(metric) => metric,
            Err(error) => {
                drop(metric_scratch);
                return Err(error);
            },
        };
        execute_unary_scalar(py, geometry, domain, metric, kernel)
    };
    drop(metric_scratch);
    result.map_err(PyErr::from)
}

/// How the per-row unary fallback materializes a [`PreparedPlan`] handle.
///
/// Pure topology transforms only need the bare shape; persisting prepared
/// state (or frame caches) on every row is a memory leak against the
/// caller's intent. Validity/simplicity and other genuine prepared-plan
/// consumers keep the prepared/array-cached path. Distance-basis linref
/// needs a frame cache without necessarily needing prepared persistence.
#[derive(Clone, Copy)]
enum UnaryRowMode {
    /// Transient stack handle — no prepared-slot or frame-cache write.
    ShapeOnly,
    /// Persist large/mixed prepared handles into the array cache; still no
    /// eager frame-cache acquisition.
    Prepared,
}

/// Array geometry lane: resolve metric once, try packed fast paths, else per-row kernel.
pub(crate) fn unary_array<R: BulkElement>(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    unit: Option<DistanceUnit>,
    packed: Option<&PackedUnary>,
    kernel: impl Fn(&PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let resolver = op.resolver_with_unit(unit);
    if let Some(result) = try_unary_packed_f64(py, array, op, unit) {
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
    let (values, output_frame) =
        run_unary_rows(py, array, resolver, mode, move |data, ctx, _rows| {
            kernel(data, ctx)
        })?;
    R::bulk_into_py_masked(values, py, &output_frame, array.missing())
}

const fn unary_row_mode(op: Operation) -> UnaryRowMode {
    match op {
        Operation::IsValid
        | Operation::IsSimple
        | Operation::IsRing
        | Operation::Bounds
        | Operation::ClipByRect
        | Operation::Metric(MetricOp::Area | MetricOp::LineInterpolate | MetricOp::LineSubstring) => {
            UnaryRowMode::Prepared
        },
        _ => UnaryRowMode::ShapeOnly,
    }
}

fn execute_unary_rows<T: BulkElement>(
    py: Python<'_>,
    array: PyGeometryArray,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    mode: UnaryRowMode,
    kernel: impl Fn(&PreparedPlan, &CachelessRowContext<'_>, &mut ArrayRowOperation<'_>) -> Result<T>
    + Send
    + Sync,
) -> std::result::Result<Vec<T>, (usize, crate::error::Error)> {
    py.detach(move || {
        let mut rows = array.row_operation();
        rows.rows_with_missing(T::missing_value)
            .map(|row| match row {
                ArrayRow::Missing { value, .. } => Ok(value),
                ArrayRow::Present(present) => {
                    let row_index = present.logical();
                    let shape_row = present.row();
                    let ctx = CachelessRowContext {
                        frame: &array.frame,
                        domain,
                        metric,
                        lane: Lane::Array(row_index),
                    };
                    match mode {
                        UnaryRowMode::ShapeOnly => {
                            // Transient handle — never write prepared slots.
                            let data = crate::array::PreparedRow::transient(shape_row)?;
                            kernel(&data, &ctx, &mut rows)
                        },
                        UnaryRowMode::Prepared => {
                            let data = rows.prepared_at(row_index, shape_row)?;
                            kernel(&data, &ctx, &mut rows)
                        },
                    }
                },
            })
            .collect_rows()
    })
}

/// The per-row unary lane shared by every array return type: resolve the
/// metric once, run the kernel over each row inside one ``py.detach``.
fn run_unary_rows<T: BulkElement>(
    py: Python<'_>,
    array: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    mode: UnaryRowMode,
    kernel: impl Fn(&PreparedPlan, &CachelessRowContext<'_>, &mut ArrayRowOperation<'_>) -> Result<T>
    + Send
    + Sync,
) -> PyResult<(Vec<T>, Frame)> {
    let domain = crate::geometry::topology_domain(&array.frame)?;
    let mut metric_scratch = MetricScratch::default();
    let result = {
        let metric = match resolver.resolve_ctx(&array.frame, &mut metric_scratch) {
            Ok(metric) => metric,
            Err(error) => {
                drop(metric_scratch);
                return Err(error);
            },
        };
        execute_unary_rows(py, array.clone(), domain, metric, mode, kernel)
    };
    drop(metric_scratch);
    let values = result.map_err(rows_err)?;
    Ok((values, array.frame.clone()))
}

/// Scalar geometry method returning a typed leaf geometry.
pub(crate) fn unary_scalar_shape(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    unit: Option<DistanceUnit>,
    kernel: impl FnOnce(&PreparedPlan, &CachelessRowContext<'_>) -> Result<Shape> + Send,
) -> PyResult<Typed> {
    let frame = geometry.frame.clone();
    let shape = unary_scalar(py, geometry, op, unit, kernel)?;
    Ok(Typed(PyGeometry::with_frame(shape, frame)))
}

pub(crate) fn unary_scalar_shape_admitted(
    py: Python<'_>,
    geometry: &PyGeometry,
    domain: TopologyDomain,
    kernel: impl FnOnce(&PreparedPlan, &CachelessRowContext<'_>) -> Result<Shape> + Send,
) -> PyResult<Typed> {
    let shape =
        execute_unary_scalar(py, geometry, domain, MetricCtx::None, kernel).map_err(PyErr::from)?;
    Ok(Typed(PyGeometry::with_frame(shape, geometry.frame.clone())))
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
    kernel: impl Fn(&PreparedPlan, &CachelessRowContext<'_>) -> Result<Shape> + Send + Sync,
) -> PyResult<PyGeometryArray> {
    let resolver = op.resolver_with_unit(unit);
    if let Some(result) = try_unary_packed_array(py, array, op, resolver, packed) {
        return result;
    }
    if let Some(PackedUnary::ClipByRect { domain, .. }) = packed {
        let shapes = execute_unary_rows(
            py,
            array.clone(),
            *domain,
            MetricCtx::None,
            unary_row_mode(op),
            move |data, ctx, _rows| kernel(data, ctx),
        )
        .map_err(rows_err)?;
        return Ok(PyGeometryArray::from_shapes(shapes, array.frame.clone())?
            .with_missing_mask(array.missing().cloned())?);
    }
    let (shapes, frame) = run_unary_rows(
        py,
        array,
        resolver,
        unary_row_mode(op),
        move |data, ctx, _rows| kernel(data, ctx),
    )?;
    Ok(PyGeometryArray::from_shapes(shapes, frame)?.with_missing_mask(array.missing().cloned())?)
}

pub(crate) fn unary_scalar_linref_shape(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    resolver: super::metric::MetricResolver,
    kernel: impl FnOnce(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<Shape> + Send,
) -> PyResult<Typed> {
    let frame = geometry.frame.clone();
    let domain = crate::geometry::topology_domain(&frame)?;
    let mut scratch = MetricScratch::default();
    let result = {
        let metric = match resolver.resolve_ctx(&frame, &mut scratch) {
            Ok(metric) => metric,
            Err(error) => {
                drop(scratch);
                return Err(error);
            },
        };
        let model = metric.require_model(
            op.metric_op()
                .expect("linear-reference operation must carry metric identity"),
        );
        let model = match model {
            Ok(model) => model,
            Err(error) => {
                drop(scratch);
                return Err(PyErr::from(error));
            },
        };
        (|| -> Result<_> {
            let row = LinearReferenceRow {
                op: CachelessRowContext {
                    frame: &frame,
                    domain,
                    metric,
                    lane: Lane::Scalar,
                },
                model,
                cache: &*geometry.frame_cache(),
            };
            let plan = geometry.plan_or_transient()?;
            py.detach(move || kernel(&plan, &row))
        })()
    };
    drop(scratch);
    let shape = result?;
    Ok(Typed(PyGeometry::with_frame(shape, frame)))
}

pub(crate) fn unary_array_linref_shapes(
    py: Python<'_>,
    array: &PyGeometryArray,
    packed: &PackedLinearReference,
    kernel: impl Fn(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<Shape> + Send + Sync,
) -> PyResult<PyGeometryArray> {
    if let Some(result) = try_packed_linear_reference(py, array, packed) {
        return result;
    }
    let model = match packed {
        PackedLinearReference::InterpolatePoint { metric, .. }
        | PackedLinearReference::Substring { metric, .. } => metric,
    };
    let frame = array.frame.clone();
    let output_frame = frame.clone();
    let domain = crate::geometry::topology_domain(&frame)?;
    let output_missing = array.missing().cloned();
    let array = array.clone();
    let result = py.detach(move || {
        let mut rows = array.row_operation();
        rows.rows_with_missing(Shape::empty_point)
            .map(|row| {
                let ArrayRow::Present(present) = row else {
                    return Ok(Shape::empty_point());
                };
                let row_index = present.logical();
                let shape = present.row();
                let data = crate::array::PreparedRow::transient(shape)?;
                let cache = rows.frame_cache(row_index);
                let row = LinearReferenceRow {
                    op: CachelessRowContext {
                        frame: &frame,
                        domain,
                        metric: MetricCtx::None,
                        lane: Lane::Array(row_index),
                    },
                    model,
                    cache: &cache,
                };
                kernel(&data, &row)
            })
            .collect_rows()
    });
    let shapes = match result {
        Ok(shapes) => shapes,
        Err(error) => {
            drop(output_missing);
            return Err(rows_err(error));
        },
    };
    Ok(PyGeometryArray::from_shapes(shapes, output_frame)?.with_missing_mask(output_missing)?)
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
        &PreparedPlan,
        &CachelessRowContext<'_>,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<Shape>
    + Send,
) -> PyResult<PyGeometryArray> {
    let resolver = op.resolver_with_unit(unit);
    let frame = array.frame.clone();
    let output_frame = frame.clone();
    let domain = crate::geometry::topology_domain(&frame)?;
    let mut metric_scratch = MetricScratch::default();
    let missing = array.missing().cloned();
    let array = array.clone();
    let result = {
        let metric = match resolver.resolve_ctx(&frame, &mut metric_scratch) {
            Ok(metric) => metric,
            Err(error) => {
                drop(metric_scratch);
                return Err(error);
            },
        };
        py.detach(move || {
            let mut rows = array.row_operation();
            rows.rows_with_missing(Shape::empty_point)
                .map(|row| {
                    let ArrayRow::Present(present) = row else {
                        return Ok(Shape::empty_point());
                    };
                    let row = present.logical();
                    let shape_row = present.row();
                    let ctx = CachelessRowContext {
                        frame: &frame,
                        domain,
                        metric,
                        lane: Lane::Array(row),
                    };
                    let data = rows.prepared_at(row, shape_row)?;
                    let mut budget = crate::geometry::ExpansionBudget::new(op.name(), parameter);
                    kernel(&data, &ctx, &mut budget)
                })
                .collect_rows()
        })
    };
    drop(metric_scratch);
    let shapes = match result {
        Ok(shapes) => shapes,
        Err(error) => {
            drop(missing);
            return Err(rows_err(error));
        },
    };
    Ok(PyGeometryArray::from_shapes(shapes, output_frame)?.with_missing_mask(missing)?)
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
    kernel: impl Fn(&PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
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
