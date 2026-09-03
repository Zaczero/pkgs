#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::array::{ArrayRow, ArrayRowOperation, GeometryArrayStorage, ShapeRow, ValidityMask};
use crate::boundary::metadata::Frame;
use crate::broadcast::{
    BulkElement, CollectRows as _, GeometryInput, classify_required, paired_arrays,
    paired_arrays_len, rows_err,
};
use crate::crs::{MetricModel, ResolvedMetric};
use crate::dispatch::MetricOp;
use crate::dispatch::metric::{
    CachelessRowContext, GeodesicBinaryRow, Lane, MetricCtx, MetricScratch,
};
use crate::error::Result;
use crate::geometry::{PreparedPlan, Shape, ShapeData, TopologyDomain};
use crate::{PyGeometry, PyGeometryArray, Typed};

type MaskedRows<R> = (Vec<R>, Option<ValidityMask>);
type FramedMaskedRows<R> = (Vec<R>, Option<ValidityMask>, Frame);

#[derive(Clone, Copy)]
enum ArrayOperandSide {
    Left,
    Right,
}

/// Optional binary array-lane fast path: run BEFORE the generic per-row kernel
/// when packed-column or other batch engines apply.
pub(crate) trait BinaryArrayFastPath<R>: Send + Sync {
    fn try_dispatch(
        &self,
        py: Python<'_>,
        left: &PyGeometryArray,
        right: GeometryInput<'_>,
        op_name: &str,
        frame: &Frame,
        model: Option<&MetricModel>,
        metric: Option<&ResolvedMetric>,
    ) -> Option<PyResult<Py<PyAny>>>;
}

/// No-op fast path — generic per-row lane only.
pub(crate) struct NoBinaryFastPath;

impl<R: BulkElement> BinaryArrayFastPath<R> for NoBinaryFastPath {
    fn try_dispatch(
        &self,
        _py: Python<'_>,
        _left: &PyGeometryArray,
        _right: GeometryInput<'_>,
        _op_name: &str,
        _frame: &Frame,
        _model: Option<&MetricModel>,
        _metric: Option<&ResolvedMetric>,
    ) -> Option<PyResult<Py<PyAny>>> {
        None
    }
}

fn run_fast_path<R, F>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: GeometryInput<'_>,
    op_name: &str,
    frame: &Frame,
    resolver: super::metric::MetricResolver,
    fast_path: &F,
) -> Option<PyResult<Py<PyAny>>>
where
    R: BulkElement,
    F: BinaryArrayFastPath<R>,
{
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(frame, &mut metric_scratch).ok();
    fast_path.try_dispatch(
        py,
        left,
        right,
        op_name,
        frame,
        metric.and_then(MetricCtx::model),
        metric.and_then(MetricCtx::resolved),
    )
}

fn run_array_rows<R, K>(
    py: Python<'_>,
    array: &PyGeometryArray,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> PyResult<Vec<R>>
where
    R: BulkElement,
    K: for<'row, 'ctx> Fn(
            usize,
            ShapeRow<'row>,
            &CachelessRowContext<'ctx>,
            &mut ArrayRowOperation<'row>,
        ) -> Result<R>
        + Send
        + Sync,
{
    let array = array.clone();
    py.detach(move || {
        let frame = &frame;
        let mut rows = array.row_operation();
        rows.rows_with_missing(R::missing_value)
            .map(|row| match row {
                ArrayRow::Missing { value, .. } => Ok(value),
                ArrayRow::Present(present) => {
                    let row_index = present.logical();
                    let shape_row = present.row();
                    let ctx = CachelessRowContext {
                        frame,
                        domain,
                        metric,
                        lane: Lane::Array(row_index),
                    };
                    kernel(row_index, shape_row, &ctx, &mut rows)
                },
            })
            .collect_rows()
    })
    .map_err(rows_err)
}

fn run_paired_rows<R, K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    _rows: (Arc<GeometryArrayStorage>, Arc<GeometryArrayStorage>),
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> PyResult<MaskedRows<R>>
where
    R: BulkElement,
    K: for<'left, 'right, 'ctx> Fn(
            usize,
            ShapeRow<'left>,
            ShapeRow<'right>,
            &CachelessRowContext<'ctx>,
            &mut ArrayRowOperation<'left>,
            &mut ArrayRowOperation<'right>,
        ) -> Result<R>
        + Send
        + Sync,
{
    let left_array = left.clone();
    let right_array = right.clone();
    let missing = crate::array::missing::union_pair(left.missing(), right.missing());
    let values = py
        .detach(move || {
            let frame = &frame;
            let mut left_rows = left_array.row_operation();
            let mut right_rows = right_array.row_operation();
            left_rows
                .rows_with_missing(R::missing_value)
                .zip(right_rows.rows_with_missing(R::missing_value))
                .map(|(left, right)| match (left, right) {
                    (ArrayRow::Present(left), ArrayRow::Present(right)) => {
                        let row_index = left.logical();
                        let left_row = left.row();
                        let right_row = right.row();
                        let ctx = CachelessRowContext {
                            frame,
                            domain,
                            metric,
                            lane: Lane::Array(row_index),
                        };
                        kernel(
                            row_index,
                            left_row,
                            right_row,
                            &ctx,
                            &mut left_rows,
                            &mut right_rows,
                        )
                    },
                    (ArrayRow::Missing { value, .. }, _) | (_, ArrayRow::Missing { value, .. }) => {
                        Ok(value)
                    },
                })
                .collect_rows()
        })
        .map_err(rows_err)?;
    Ok((values, missing))
}

fn run_binary_fixed_array_rows<R, K>(
    py: Python<'_>,
    array: &PyGeometryArray,
    fixed: &PyGeometry,
    side: ArrayOperandSide,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    kernel: K,
) -> PyResult<(Vec<R>, Frame)>
where
    R: BulkElement,
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
    let (frame, domain) = match side {
        ArrayOperandSide::Left => binary_frame(
            (array.crs_ref(), array.epoch()),
            (fixed.crs_ref(), fixed.epoch()),
            op_name,
        )?,
        ArrayOperandSide::Right => binary_frame(
            (fixed.crs_ref(), fixed.epoch()),
            (array.crs_ref(), array.epoch()),
            op_name,
        )?,
    };
    let output_frame = frame.clone();
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, &mut metric_scratch)?;
    let fixed_plan = fixed.plan_or_transient();
    // Metric/predicate binary array×scalar: keep prepared handles (`&PreparedPlan`);
    // frame caches are derived from MetricCtx. Never demote these kernels to
    // bare `&Shape` (Deref coercion would drop prepared engines).
    let values = run_array_rows(
        py,
        array,
        &frame,
        domain,
        metric,
        move |row, array_row, ctx, products| {
            let array_data = products.prepared_at(row, array_row);
            match side {
                ArrayOperandSide::Left => kernel(&array_data, &fixed_plan, ctx),
                ArrayOperandSide::Right => kernel(&fixed_plan, &array_data, ctx),
            }
        },
    )?;
    Ok((values, output_frame))
}

fn run_binary_array_array_rows<R, K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    kernel: K,
) -> PyResult<FramedMaskedRows<R>>
where
    R: BulkElement,
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
    let (lefts, rights) = paired_arrays(left, right, op_name)?;
    let (frame, domain) = binary_frame(
        (left.crs_ref(), left.epoch()),
        (right.crs_ref(), right.epoch()),
        op_name,
    )?;
    let output_frame = frame.clone();
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, &mut metric_scratch)?;
    // Prepared handles; frame caches derived from MetricCtx.
    let (values, missing) = run_paired_rows(
        py,
        left,
        right,
        (lefts, rights),
        &frame,
        domain,
        metric,
        move |row, left_row, right_row, ctx, left_products, right_products| {
            let left_data = left_products.prepared_at(row, left_row);
            let right_data = right_products.prepared_at(row, right_row);
            kernel(&left_data, &right_data, ctx)
        },
    )?;
    Ok((values, missing, output_frame))
}

/// Binary scalar/array dispatch (four scalar×array shapes).
///
/// Resolves the metric once via ``op.resolver()``, checks frame compatibility
/// once per operand pair-set, runs the kernel inside one ``py.detach``, uses
/// caching prepared rows for array lanes, and bulk-converts via
/// [`BulkElement`].
pub(crate) fn dispatch_binary<R, F>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    resolver: super::metric::MetricResolver,
    fast_path: &F,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    R: BulkElement,
    F: BinaryArrayFastPath<R>,
{
    use GeometryInput::{Many, One};
    let (left_in, right_in) = (classify_required(left)?, classify_required(right)?);

    match (left_in, right_in) {
        (One(left), One(right)) => {
            dispatch_binary_scalar_scalar(py, left, right, resolver, op_name, kernel)
        },
        (One(left), Many(right)) => {
            if let Some(result) = run_fast_path::<R, _>(
                py,
                right,
                GeometryInput::One(left),
                op_name,
                &right.frame,
                resolver,
                fast_path,
            ) {
                return result;
            }
            dispatch_binary_scalar_array(py, left, right, resolver, op_name, kernel)
        },
        (Many(left), One(right)) => {
            if let Some(result) = run_fast_path::<R, _>(
                py,
                left,
                GeometryInput::One(right),
                op_name,
                &left.frame,
                resolver,
                fast_path,
            ) {
                return result;
            }
            dispatch_binary_array_scalar(py, left, right, resolver, op_name, kernel)
        },
        (Many(left), Many(right)) => {
            if let Some(result) = run_fast_path::<R, _>(
                py,
                left,
                GeometryInput::Many(right),
                op_name,
                &left.frame,
                resolver,
                fast_path,
            ) {
                return result;
            }
            dispatch_binary_array_array(py, left, right, resolver, op_name, kernel)
        },
    }
}

pub(crate) fn dispatch_typed_metric_binary<R>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op: MetricOp,
    resolver: super::metric::MetricResolver,
    planar: impl Fn(f64, &PreparedPlan, &PreparedPlan) -> Result<R> + Send + Sync,
    geodesic: impl Fn(&PreparedPlan, &PreparedPlan, &GeodesicBinaryRow<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    R: BulkElement,
{
    let op_name = op.name();
    use GeometryInput::{Many, One};
    let (left_input, right_input) = (classify_required(left)?, classify_required(right)?);
    let (left_crs, left_epoch) = match left_input {
        One(value) => (value.crs_ref(), value.epoch()),
        Many(value) => (value.crs_ref(), value.epoch()),
    };
    let (right_crs, right_epoch) = match right_input {
        One(value) => (value.crs_ref(), value.epoch()),
        Many(value) => (value.crs_ref(), value.epoch()),
    };
    let (frame, _domain) = binary_frame((left_crs, left_epoch), (right_crs, right_epoch), op_name)?;
    let mut scratch = MetricScratch::default();
    let metric_ctx = resolver.resolve_ctx(&frame, &mut scratch)?;
    let resolved = metric_ctx.require_resolved(op)?;

    match (left_input, right_input, resolved) {
        (One(left), One(right), crate::crs::ResolvedMetric::Planar { to_metre }) => {
            let left = left.plan_or_transient();
            let right = right.plan_or_transient();
            let value = py.detach(|| planar(to_metre.get(), &left, &right))?;
            R::into_py(value, py, &frame)
        },
        (One(left), One(right), crate::crs::ResolvedMetric::Geodesic(metric)) => {
            let left_plan = left.plan_or_transient();
            let right_plan = right.plan_or_transient();
            let row = GeodesicBinaryRow {
                metric,
                left_cache: &left.frame_cache(),
                right_cache: &right.frame_cache(),
            };
            let value = py.detach(|| geodesic(&left_plan, &right_plan, &row))?;
            R::into_py(value, py, &frame)
        },
        (Many(array), One(fixed), crate::crs::ResolvedMetric::Planar { to_metre })
        | (One(fixed), Many(array), crate::crs::ResolvedMetric::Planar { to_metre }) => {
            let scalar_left = matches!(left_input, One(_));
            let fixed = fixed.plan_or_transient();
            let output_missing = array.missing().cloned();
            let array = array.clone();
            let values = py
                .detach(move || {
                    let mut rows = array.row_operation();
                    rows.rows_with_missing(R::missing_value)
                        .map(|logical_row| {
                            let ArrayRow::Present(row) = logical_row else {
                                if let ArrayRow::Missing { value, .. } = logical_row {
                                    return Ok(value);
                                }
                                unreachable!();
                            };
                            let data = rows.prepared(row);
                            if scalar_left {
                                planar(to_metre.get(), &fixed, &data)
                            } else {
                                planar(to_metre.get(), &data, &fixed)
                            }
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?;
            R::bulk_into_py_masked(values, py, &frame, output_missing.as_ref())
        },
        (Many(array), One(fixed), crate::crs::ResolvedMetric::Geodesic(metric))
        | (One(fixed), Many(array), crate::crs::ResolvedMetric::Geodesic(metric)) => {
            let scalar_left = matches!(left_input, One(_));
            let fixed_plan = fixed.plan_or_transient();
            let fixed_cache = fixed.frame_cache();
            let output_missing = array.missing().cloned();
            let array = array.clone();
            let values = py
                .detach(move || {
                    let mut rows = array.row_operation();
                    rows.rows_with_missing(R::missing_value)
                        .map(|logical_row| {
                            let ArrayRow::Present(row) = logical_row else {
                                if let ArrayRow::Missing { value, .. } = logical_row {
                                    return Ok(value);
                                }
                                unreachable!();
                            };
                            let row_index = row.logical();
                            let data = rows.prepared(row);
                            let row_cache = rows.frame_cache(row_index);
                            let row = if scalar_left {
                                GeodesicBinaryRow {
                                    metric,
                                    left_cache: &fixed_cache,
                                    right_cache: &row_cache,
                                }
                            } else {
                                GeodesicBinaryRow {
                                    metric,
                                    left_cache: &row_cache,
                                    right_cache: &fixed_cache,
                                }
                            };
                            if scalar_left {
                                geodesic(&fixed_plan, &data, &row)
                            } else {
                                geodesic(&data, &fixed_plan, &row)
                            }
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?;
            R::bulk_into_py_masked(values, py, &frame, output_missing.as_ref())
        },
        (Many(left), Many(right), crate::crs::ResolvedMetric::Planar { to_metre }) => {
            paired_arrays(left, right, op_name)?;
            let missing = crate::array::missing::union_pair(left.missing(), right.missing());
            let output_missing = missing;
            let left = left.clone();
            let right = right.clone();
            let values = py
                .detach(move || {
                    let mut left_rows = left.row_operation();
                    let mut right_rows = right.row_operation();
                    left_rows
                        .rows_with_missing(R::missing_value)
                        .zip(right_rows.rows_with_missing(R::missing_value))
                        .map(|(left_row, right_row)| match (left_row, right_row) {
                            (ArrayRow::Present(a), ArrayRow::Present(b)) => {
                                let a = left_rows.prepared(a);
                                let b = right_rows.prepared(b);
                                planar(to_metre.get(), &a, &b)
                            },
                            (ArrayRow::Missing { value, .. }, _)
                            | (_, ArrayRow::Missing { value, .. }) => Ok(value),
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?;
            R::bulk_into_py_masked(values, py, &frame, output_missing.as_ref())
        },
        (Many(left), Many(right), crate::crs::ResolvedMetric::Geodesic(metric)) => {
            paired_arrays(left, right, op_name)?;
            let missing = crate::array::missing::union_pair(left.missing(), right.missing());
            let output_missing = missing;
            let left = left.clone();
            let right = right.clone();
            let values = py
                .detach(move || {
                    let mut left_rows = left.row_operation();
                    let mut right_rows = right.row_operation();
                    left_rows
                        .rows_with_missing(R::missing_value)
                        .zip(right_rows.rows_with_missing(R::missing_value))
                        .map(|(left_row, right_row)| match (left_row, right_row) {
                            (ArrayRow::Present(a), ArrayRow::Present(b)) => {
                                let row_index = a.logical();
                                let a = left_rows.prepared(a);
                                let b = right_rows.prepared(b);
                                let left_cache = left_rows.frame_cache(row_index);
                                let right_cache = right_rows.frame_cache(row_index);
                                let row = GeodesicBinaryRow {
                                    metric,
                                    left_cache: &left_cache,
                                    right_cache: &right_cache,
                                };
                                geodesic(&a, &b, &row)
                            },
                            (ArrayRow::Missing { value, .. }, _)
                            | (_, ArrayRow::Missing { value, .. }) => Ok(value),
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?;
            R::bulk_into_py_masked(values, py, &frame, output_missing.as_ref())
        },
    }
}

/// Shared binary prologue: the ONE frame-compatibility + geographic
/// resolution all five scalar/array shape handlers start from.
fn binary_frame(
    left: (Option<&crate::Crs>, Option<f64>),
    right: (Option<&crate::Crs>, Option<f64>),
    op_name: &str,
) -> PyResult<(Frame, TopologyDomain)> {
    let frame = Frame::compatible_parts(left.0, left.1, right.0, right.1, op_name)?;
    let domain = crate::geometry::topology_domain(&frame)?;
    Ok((frame, domain))
}

fn dispatch_binary_scalar_scalar<R: BulkElement>(
    py: Python<'_>,
    left: &PyGeometry,
    right: &PyGeometry,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let (frame, domain) = binary_frame(
        (left.crs_ref(), left.epoch()),
        (right.crs_ref(), right.epoch()),
        op_name,
    )?;
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, &mut metric_scratch)?;
    let ctx = CachelessRowContext {
        frame: &frame,
        domain,
        metric,
        lane: Lane::Scalar,
    };
    let left_plan = left.plan_or_transient();
    let right_plan = right.plan_or_transient();
    let result = py.detach(move || kernel(&left_plan, &right_plan, &ctx))?;
    R::into_py(result, py, &frame)
}

fn dispatch_binary_scalar_array<R: BulkElement>(
    py: Python<'_>,
    left: &PyGeometry,
    right: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let (values, output_frame) = run_binary_fixed_array_rows(
        py,
        right,
        left,
        ArrayOperandSide::Right,
        resolver,
        op_name,
        kernel,
    )?;
    R::bulk_into_py_masked(values, py, &output_frame, right.missing())
}

fn dispatch_binary_array_scalar<R: BulkElement>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometry,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let (values, output_frame) = run_binary_fixed_array_rows(
        py,
        left,
        right,
        ArrayOperandSide::Left,
        resolver,
        op_name,
        kernel,
    )?;
    R::bulk_into_py_masked(values, py, &output_frame, left.missing())
}

fn dispatch_binary_array_array<R: BulkElement>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let (values, missing, output_frame) =
        run_binary_array_array_rows(py, left, right, resolver, op_name, kernel)?;
    R::bulk_into_py_masked(values, py, &output_frame, missing.as_ref())
}

/// Array-left binary dispatch for ``GeometryArray`` methods (scalar or
/// equal-length array ``right`` operand).
pub(crate) fn dispatch_binary_array_left<R>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    resolver: super::metric::MetricResolver,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Vec<R>>
where
    R: BulkElement,
{
    use GeometryInput::{Many, One};
    match classify_required(right)? {
        One(right) => {
            let (values, _) = run_binary_fixed_array_rows(
                py,
                left,
                right,
                ArrayOperandSide::Left,
                resolver,
                op_name,
                kernel,
            )?;
            Ok(values)
        },
        Many(right) => {
            let (values, ..) =
                run_binary_array_array_rows(py, left, right, resolver, op_name, kernel)?;
            Ok(values)
        },
    }
}

/// Split one operand when the resolved frame is geographic and the shape
/// crosses ±180, reusing the handle otherwise.
fn split_operand_arc_for_topology(
    domain: TopologyDomain,
    geometry: &PyGeometry,
) -> Result<Arc<PreparedPlan>> {
    let plan = geometry.plan_or_transient();
    if let TopologyDomain::Geographic(frame) = domain
        && crate::geometry::crosses_antimeridian_in_frame(plan.shape(), frame)
    {
        Ok(Arc::new(PreparedPlan::new(ShapeData::from(
            plan.shape().split_antimeridian_in_frame(frame)?,
        ))))
    } else {
        Ok(plan)
    }
}

/// The shared antimeridian gate for geometry-returning binary lanes.
fn invoke_topology_geometry_kernel<K>(
    domain: TopologyDomain,
    left: &PreparedPlan,
    right: &PreparedPlan,
    kernel: &K,
) -> Result<Shape>
where
    K: Fn(&PreparedPlan, &PreparedPlan) -> Result<Shape>,
{
    let (left_crosses, right_crosses) = match domain {
        TopologyDomain::Cartesian => (false, false),
        TopologyDomain::Geographic(frame) => (
            crate::geometry::crosses_antimeridian_in_frame(left.shape(), frame),
            crate::geometry::crosses_antimeridian_in_frame(right.shape(), frame),
        ),
    };
    if left_crosses || right_crosses {
        let TopologyDomain::Geographic(frame) = domain else {
            unreachable!("Cartesian topology cannot cross the antimeridian")
        };
        let left_split = if left_crosses {
            Some(PreparedPlan::new(ShapeData::from(
                left.shape().split_antimeridian_in_frame(frame)?,
            )))
        } else {
            None
        };
        let right_split = if right_crosses {
            Some(PreparedPlan::new(ShapeData::from(
                right.shape().split_antimeridian_in_frame(frame)?,
            )))
        } else {
            None
        };
        kernel(
            left_split.as_ref().unwrap_or(left),
            right_split.as_ref().unwrap_or(right),
        )
    } else {
        kernel(left, right)
    }
}

/// Geometry-returning binary dispatch with topology split.
#[expect(
    clippy::too_many_lines,
    reason = "cohesive four-shape geometry dispatch; splitting obscures the algorithm"
)]
pub(crate) fn dispatch_binary_geometry<F>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    fast_path: &F,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<Shape>
    + Send
    + Sync,
) -> PyResult<Py<PyAny>>
where
    F: BinaryArrayFastPath<Shape>,
{
    use GeometryInput::{Many, One};
    let (left_in, right_in) = (classify_required(left)?, classify_required(right)?);
    let resolver = super::metric::MetricResolver::None;

    match (left_in, right_in) {
        (One(left), One(right)) => {
            let (frame, domain) = binary_frame(
                (left.crs_ref(), left.epoch()),
                (right.crs_ref(), right.epoch()),
                op_name,
            )?;
            let mut metric_scratch = MetricScratch::default();
            let metric = resolver.resolve_ctx(&frame, &mut metric_scratch)?;
            let ctx = CachelessRowContext {
                frame: &frame,
                domain,
                metric,
                lane: Lane::Scalar,
            };
            let left_shape = left.plan_or_transient();
            let right_shape = right.plan_or_transient();
            let shape = py.detach(move || {
                invoke_topology_geometry_kernel(domain, &left_shape, &right_shape, &|l, r| {
                    kernel(l, r, &ctx)
                })
            })?;
            Ok(Typed(PyGeometry::with_frame(shape, frame))
                .into_pyobject(py)?
                .unbind())
        },
        (One(left), Many(right)) => {
            let (frame, domain) = binary_frame(
                (left.crs_ref(), left.epoch()),
                (right.crs_ref(), right.epoch()),
                op_name,
            )?;
            if let Some(result) = fast_path.try_dispatch(
                py,
                right,
                GeometryInput::One(left),
                op_name,
                &frame,
                None,
                None,
            ) {
                return result;
            }
            let output_frame = frame.clone();
            let mut metric_scratch = MetricScratch::default();
            let metric = resolver.resolve_ctx(&frame, &mut metric_scratch)?;
            let left_fixed = split_operand_arc_for_topology(domain, left)?;
            // Overlay rows use transient plans only; MetricCtx is None so
            // no frame-dependent cache is retained.
            let shapes = run_array_rows(
                py,
                right,
                &frame,
                domain,
                metric,
                move |row, right_row, ctx, products| {
                    let known_bounds = right_row.quick_bounds();
                    let right_data = products.warm_prepared_at(row, right_row, known_bounds);
                    invoke_topology_geometry_kernel(domain, &left_fixed, &right_data, &|l, r| {
                        kernel(l, r, ctx)
                    })
                },
            )?;
            Shape::bulk_into_py_masked(shapes, py, &output_frame, right.missing())
        },
        (Many(left), right_in) => {
            let (right_crs, right_epoch) = match right_in {
                GeometryInput::One(right) => (right.crs_ref(), right.epoch()),
                GeometryInput::Many(right) => (right.crs_ref(), right.epoch()),
            };
            let frame = Frame::compatible_parts(
                left.crs_ref(),
                left.epoch(),
                right_crs,
                right_epoch,
                op_name,
            )?;
            if let Some(result) = match right_in {
                GeometryInput::One(right) => fast_path.try_dispatch(
                    py,
                    left,
                    GeometryInput::One(right),
                    op_name,
                    &frame,
                    None,
                    None,
                ),
                GeometryInput::Many(right) => fast_path.try_dispatch(
                    py,
                    left,
                    GeometryInput::Many(right),
                    op_name,
                    &frame,
                    None,
                    None,
                ),
            } {
                return result;
            }
            let array = geometry_kernel_over_array(py, left, right_in, op_name, resolver, kernel)?;
            Ok(array.into_pyobject(py)?.unbind().into())
        },
    }
}

pub(crate) fn geometry_kernel_over_array<K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: GeometryInput<'_>,
    op_name: &str,
    resolver: super::metric::MetricResolver,
    kernel: K,
) -> PyResult<PyGeometryArray>
where
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<Shape> + Send + Sync,
{
    use GeometryInput::{Many, One};
    let (right_crs, right_epoch) = match right {
        One(right) => (right.crs_ref(), right.epoch()),
        Many(right) => (right.crs_ref(), right.epoch()),
    };
    let frame = Frame::compatible_parts(
        left.crs_ref(),
        left.epoch(),
        right_crs,
        right_epoch,
        op_name,
    )?;
    let output_crs = frame.crs_owned();
    let output_epoch = frame.epoch();
    let row_box = |bounds: &Option<crate::array::ElementBounds>,
                   row: usize|
     -> Option<crate::geometry::Bounds> {
        bounds.as_ref().and_then(|b| b.get(row).copied()).flatten()
    };
    let domain = crate::geometry::topology_domain(&frame)?;
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, &mut metric_scratch)?;
    // Prefer already-warm bounds caches, but do NOT force-initialize them:
    // a pure overlay of two packed point arrays must not retain multi-MiB
    // element-bounds sidecars on the inputs. Cold rows use `quick_bounds`.
    let left_bounds = left.bounds_cache.get().cloned().flatten();
    let (shapes, output_missing) = match right {
        One(right) => {
            let right_shape = split_operand_arc_for_topology(domain, right)?;
            // Pure topology overlay: MetricCtx is None → no frame caches.
            let shapes = run_array_rows(
                py,
                left,
                &frame,
                domain,
                metric,
                move |row, left_row, ctx, products| {
                    let left_box = row_box(&left_bounds, row).or_else(|| left_row.quick_bounds());
                    let left_data = products.warm_prepared_at(row, left_row, left_box);
                    invoke_topology_geometry_kernel(domain, &left_data, &right_shape, &|l, r| {
                        kernel(l, r, ctx)
                    })
                },
            )?;
            (shapes, left.missing().cloned())
        },
        Many(right) => {
            let rows = paired_arrays_len(left, right)?;
            let right_bounds = right.bounds_cache.get().cloned().flatten();
            run_paired_rows(
                py,
                left,
                right,
                rows,
                &frame,
                domain,
                metric,
                move |row, left_row, right_row, ctx, left_products, right_products| {
                    let left_box = row_box(&left_bounds, row).or_else(|| left_row.quick_bounds());
                    let right_box =
                        row_box(&right_bounds, row).or_else(|| right_row.quick_bounds());
                    let left_data = left_products.warm_prepared_at(row, left_row, left_box);
                    let right_data = right_products.warm_prepared_at(row, right_row, right_box);
                    invoke_topology_geometry_kernel(domain, &left_data, &right_data, &|l, r| {
                        kernel(l, r, ctx)
                    })
                },
            )?
        },
    };
    Ok(
        PyGeometryArray::from_shapes(shapes, Frame::new(output_crs, output_epoch)?)
            .with_missing_mask(output_missing),
    )
}
