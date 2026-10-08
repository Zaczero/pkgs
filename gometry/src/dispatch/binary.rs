#![expect(
    clippy::similar_names,
    reason = "left and right operands carry the same set of derived names"
)]
use std::sync::Arc;

use pyo3::prelude::*;

use crate::array::{ArrayRow, ArrayRowOperation};
use crate::broadcast::{
    BulkElement, CollectRows as _, GeometryInput, classify_required, rows_err,
    validate_paired_arrays,
};
use crate::crs::frame::Frame;
use crate::crs::{MetricModel, ResolvedMetric};
use crate::dispatch::MetricOp;
use crate::dispatch::metric::{
    CachelessRowContext, GeodesicBinaryRow, Lane, MetricCtx, MetricResolver, MetricScratch,
};
use crate::error::Result;
use crate::geometry::storage::{ShapeRow, ValidityMask, validity as missing};
use crate::geometry::{PreparedPlan, Shape, ShapeData, TopologyDomain};
#[cfg(test)]
use crate::test_support::binary_dispatch_counters;
use crate::{PyGeometry, PyGeometryArray, Typed};

type FramedMaskedRows<R> = (Vec<R>, Option<ValidityMask>, Frame);

/// Keep raw row errors until the metric scratch owner has been released.
enum RawMetricBinary<R> {
    Scalar(Result<R>),
    Array(
        Result<Vec<R>, (usize, crate::error::Error)>,
        Option<ValidityMask>,
    ),
}

#[derive(Clone, Copy)]
enum ArrayOperandSide {
    Left,
    Right,
}

/// Classified operands and their one common frame. Admission owns the
/// structural checks so every execution lane receives the same metadata and
/// never has to classify or resolve the pair again.
pub(crate) struct BinaryAdmission<'a> {
    pub(crate) left: GeometryInput<'a>,
    pub(crate) right: GeometryInput<'a>,
    pub(crate) frame: Frame,
    pub(crate) domain: TopologyDomain,
}

pub(crate) fn admit_binary<'a>(
    left: &'a Bound<'_, PyAny>,
    right: &'a Bound<'_, PyAny>,
    op_name: &str,
) -> PyResult<BinaryAdmission<'a>> {
    let left = classify_required(left)?;
    let right = classify_required(right)?;
    admit_binary_inputs(left, right, op_name)
}

pub(crate) fn admit_binary_inputs<'a>(
    left: GeometryInput<'a>,
    right: GeometryInput<'a>,
    op_name: &str,
) -> PyResult<BinaryAdmission<'a>> {
    let (frame, domain) = match (left, right) {
        (GeometryInput::Many(left), GeometryInput::Many(right)) => {
            let frame = validate_paired_arrays(left, right, op_name)?;
            let domain = crate::geometry::topology_domain(&frame)?;
            (frame, domain)
        },
        (GeometryInput::One(left), GeometryInput::One(right)) => binary_frame(
            (left.crs_ref(), left.epoch()),
            (right.crs_ref(), right.epoch()),
            op_name,
        )?,
        (GeometryInput::One(left), GeometryInput::Many(right)) => binary_frame(
            (left.crs_ref(), left.epoch()),
            (right.crs_ref(), right.epoch()),
            op_name,
        )?,
        (GeometryInput::Many(left), GeometryInput::One(right)) => binary_frame(
            (left.crs_ref(), left.epoch()),
            (right.crs_ref(), right.epoch()),
            op_name,
        )?,
    };
    Ok(BinaryAdmission {
        left,
        right,
        frame,
        domain,
    })
}

/// Optional binary array-lane fast path: run BEFORE the generic per-row kernel
/// when packed-column or other batch engines apply.
pub(crate) trait BinaryArrayFastPath<R>: Send + Sync {
    /// Decide whether this path is worth entering without acquiring metric
    /// scratch. The decision may inspect only already-admitted inputs and
    /// metadata; it must not execute a Python callback.
    fn is_eligible(
        &self,
        left: &PyGeometryArray,
        right: GeometryInput<'_>,
        op_name: &str,
        frame: &Frame,
    ) -> bool;

    fn try_dispatch(
        &self,
        py: Python<'_>,
        left: &PyGeometryArray,
        right: GeometryInput<'_>,
        op_name: &str,
        frame: &Frame,
        domain: TopologyDomain,
        missing: Option<&ValidityMask>,
        model: Option<&MetricModel>,
        metric: Option<&ResolvedMetric>,
    ) -> Option<PyResult<Py<PyAny>>>;
}

/// No-op fast path — generic per-row lane only.
pub(crate) struct NoBinaryFastPath;

impl<R: BulkElement> BinaryArrayFastPath<R> for NoBinaryFastPath {
    fn is_eligible(
        &self,
        _left: &PyGeometryArray,
        _right: GeometryInput<'_>,
        _op_name: &str,
        _frame: &Frame,
    ) -> bool {
        false
    }

    fn try_dispatch(
        &self,
        _py: Python<'_>,
        _left: &PyGeometryArray,
        _right: GeometryInput<'_>,
        _op_name: &str,
        _frame: &Frame,
        _domain: TopologyDomain,
        _missing: Option<&ValidityMask>,
        _model: Option<&MetricModel>,
        _metric: Option<&ResolvedMetric>,
    ) -> Option<PyResult<Py<PyAny>>> {
        None
    }
}

fn run_array_rows<R, K>(
    py: Python<'_>,
    array: &PyGeometryArray,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> Result<Vec<R>, (usize, crate::error::Error)>
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
}

fn run_paired_rows<R, K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> Result<Vec<R>, (usize, crate::error::Error)>
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
    py.detach(move || {
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
}

fn run_binary_fixed_array_rows<R, K>(
    py: Python<'_>,
    array: &PyGeometryArray,
    fixed: &PyGeometry,
    side: ArrayOperandSide,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> Result<(Vec<R>, Frame), (usize, crate::error::Error)>
where
    R: BulkElement,
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
    let output_frame = frame.clone();
    let fixed_plan = fixed.prepared_plan();
    // Metric/predicate binary array×scalar: keep prepared handles (`&PreparedPlan`);
    // frame caches are derived from MetricCtx. Never demote these kernels to
    // bare `&Shape` (Deref coercion would drop prepared engines).
    run_array_rows(
        py,
        array,
        frame,
        domain,
        metric,
        move |row, array_row, ctx, products| {
            let array_data = products.prepared_at(row, array_row)?;
            match side {
                ArrayOperandSide::Left => kernel(&array_data, &fixed_plan, ctx),
                ArrayOperandSide::Right => kernel(&fixed_plan, &array_data, ctx),
            }
        },
    )
    .map(|values| (values, output_frame))
}

fn run_binary_array_array_rows<R, K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    missing: Option<ValidityMask>,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> Result<FramedMaskedRows<R>, (usize, crate::error::Error)>
where
    R: BulkElement,
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
    let output_frame = frame.clone();
    // Prepared handles; frame caches derived from MetricCtx.
    let values = run_paired_rows(
        py,
        left,
        right,
        frame,
        domain,
        metric,
        move |row, left_row, right_row, ctx, left_products, right_products| {
            let left_data = left_products.prepared_at(row, left_row)?;
            let right_data = right_products.prepared_at(row, right_row)?;
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
    let admission = admit_binary(left, right, op_name)?;
    dispatch_binary_admitted(py, &admission, op_name, resolver, fast_path, kernel)
}

pub(crate) fn dispatch_binary_admitted<R, F>(
    py: Python<'_>,
    admission: &BinaryAdmission<'_>,
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
    match (admission.left, admission.right) {
        (One(left), One(right)) => {
            let mut scratch = MetricScratch::default();
            let result = {
                let metric = resolver.resolve_ctx(&admission.frame, &mut scratch)?;
                run_binary_scalar_scalar(
                    py,
                    left,
                    right,
                    &admission.frame,
                    admission.domain,
                    metric,
                    kernel,
                )
            };
            drop(scratch);
            let value = result.map_err(PyErr::from)?;
            R::into_py(value, py, &admission.frame)
        },
        (One(left), Many(right)) => dispatch_binary_array_admitted(
            py,
            admission,
            right,
            One(left),
            ArrayOperandSide::Right,
            op_name,
            resolver,
            fast_path,
            kernel,
        ),
        (Many(left), One(right)) => dispatch_binary_array_admitted(
            py,
            admission,
            left,
            One(right),
            ArrayOperandSide::Left,
            op_name,
            resolver,
            fast_path,
            kernel,
        ),
        (Many(left), Many(right)) => dispatch_binary_array_admitted(
            py,
            admission,
            left,
            Many(right),
            ArrayOperandSide::Left,
            op_name,
            resolver,
            fast_path,
            kernel,
        ),
    }
}

fn dispatch_binary_array_admitted<R, F>(
    py: Python<'_>,
    admission: &BinaryAdmission<'_>,
    array: &PyGeometryArray,
    other: GeometryInput<'_>,
    side: ArrayOperandSide,
    op_name: &str,
    resolver: super::metric::MetricResolver,
    fast_path: &F,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    R: BulkElement,
    F: BinaryArrayFastPath<R>,
{
    let eligible = fast_path.is_eligible(array, other, op_name, &admission.frame);
    let pair_missing = match other {
        GeometryInput::Many(right) => missing::union_many([array.missing(), right.missing()]),
        GeometryInput::One(_) => None,
    };
    let mut scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&admission.frame, &mut scratch)?;

    if eligible {
        #[cfg(test)]
        binary_dispatch_counters::fast_path_attempt();
        let fast_result = fast_path.try_dispatch(
            py,
            array,
            other,
            op_name,
            &admission.frame,
            admission.domain,
            pair_missing.as_ref(),
            metric.model(),
            metric.resolved(),
        );
        if let Some(result) = fast_result {
            #[cfg(test)]
            binary_dispatch_counters::fast_path_taken();
            drop(scratch);
            return result;
        }
        #[cfg(test)]
        binary_dispatch_counters::fast_path_declined();
    }

    #[cfg(test)]
    binary_dispatch_counters::general_lane();
    let result = match other {
        GeometryInput::One(fixed) => run_binary_fixed_array_rows(
            py,
            array,
            fixed,
            side,
            &admission.frame,
            admission.domain,
            metric,
            kernel,
        )
        .map(|(values, _)| (values, None, admission.frame.clone())),
        GeometryInput::Many(right) => run_binary_array_array_rows(
            py,
            array,
            right,
            pair_missing,
            &admission.frame,
            admission.domain,
            metric,
            kernel,
        ),
    };
    drop(scratch);
    let (values, missing, frame) = result.map_err(rows_err)?;
    R::bulk_into_py_masked(values, py, &frame, missing.as_ref())
}

pub(crate) fn dispatch_typed_metric_binary<R>(
    py: Python<'_>,
    admission: &BinaryAdmission<'_>,
    op: MetricOp,
    resolver: super::metric::MetricResolver,
    planar: impl Fn(f64, &PreparedPlan, &PreparedPlan) -> Result<R> + Send + Sync,
    geodesic: impl Fn(&PreparedPlan, &PreparedPlan, &GeodesicBinaryRow<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    R: BulkElement,
{
    use GeometryInput::{Many, One};
    let left_input = admission.left;
    let right_input = admission.right;
    let frame = &admission.frame;
    let mut scratch = MetricScratch::default();
    let raw = {
        let metric_ctx = match resolver.resolve_ctx(frame, &mut scratch) {
            Ok(metric_ctx) => metric_ctx,
            Err(error) => {
                drop(scratch);
                return Err(error);
            },
        };
        let resolved = match metric_ctx.require_resolved(op) {
            Ok(resolved) => resolved,
            Err(error) => {
                drop(scratch);
                return Err(PyErr::from(error));
            },
        };

        match (left_input, right_input, resolved) {
            (One(left), One(right), crate::crs::ResolvedMetric::Planar { to_metre }) => {
                let left = left.prepared_plan();
                let right = right.prepared_plan();
                RawMetricBinary::Scalar(py.detach(|| planar(to_metre.get(), &left, &right)))
            },
            (One(left), One(right), crate::crs::ResolvedMetric::Geodesic(metric)) => {
                let left_plan = left.prepared_plan();
                let right_plan = right.prepared_plan();
                let row = GeodesicBinaryRow {
                    metric,
                    left_cache: &left.frame_cache(),
                    right_cache: &right.frame_cache(),
                };
                RawMetricBinary::Scalar(py.detach(|| geodesic(&left_plan, &right_plan, &row)))
            },
            (Many(array), One(fixed), crate::crs::ResolvedMetric::Planar { to_metre })
            | (One(fixed), Many(array), crate::crs::ResolvedMetric::Planar { to_metre }) => {
                let scalar_left = matches!(left_input, One(_));
                let fixed = fixed.prepared_plan();
                let output_missing = array.missing().cloned();
                let array = array.clone();
                RawMetricBinary::Array(
                    py.detach(move || {
                        let mut rows = array.row_operation();
                        rows.rows_with_missing(R::missing_value)
                            .map(|logical_row| {
                                let ArrayRow::Present(row) = logical_row else {
                                    if let ArrayRow::Missing { value, .. } = logical_row {
                                        return Ok(value);
                                    }
                                    unreachable!();
                                };
                                let data = rows.prepared(row)?;
                                if scalar_left {
                                    planar(to_metre.get(), &fixed, &data)
                                } else {
                                    planar(to_metre.get(), &data, &fixed)
                                }
                            })
                            .collect_rows()
                    }),
                    output_missing,
                )
            },
            (Many(array), One(fixed), crate::crs::ResolvedMetric::Geodesic(metric))
            | (One(fixed), Many(array), crate::crs::ResolvedMetric::Geodesic(metric)) => {
                let scalar_left = matches!(left_input, One(_));
                let fixed_plan = fixed.prepared_plan();
                let fixed_cache = fixed.frame_cache();
                let output_missing = array.missing().cloned();
                let array = array.clone();
                RawMetricBinary::Array(
                    py.detach(move || {
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
                                let data = rows.prepared(row)?;
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
                    }),
                    output_missing,
                )
            },
            (Many(left), Many(right), crate::crs::ResolvedMetric::Planar { to_metre }) => {
                let missing = missing::union_many([left.missing(), right.missing()]);
                let left = left.clone();
                let right = right.clone();
                RawMetricBinary::Array(
                    py.detach(move || {
                        let mut left_rows = left.row_operation();
                        let mut right_rows = right.row_operation();
                        left_rows
                            .rows_with_missing(R::missing_value)
                            .zip(right_rows.rows_with_missing(R::missing_value))
                            .map(|(left_row, right_row)| match (left_row, right_row) {
                                (ArrayRow::Present(a), ArrayRow::Present(b)) => {
                                    let a = left_rows.prepared(a)?;
                                    let b = right_rows.prepared(b)?;
                                    planar(to_metre.get(), &a, &b)
                                },
                                (ArrayRow::Missing { value, .. }, _)
                                | (_, ArrayRow::Missing { value, .. }) => Ok(value),
                            })
                            .collect_rows()
                    }),
                    missing,
                )
            },
            (Many(left), Many(right), crate::crs::ResolvedMetric::Geodesic(metric)) => {
                let missing = missing::union_many([left.missing(), right.missing()]);
                let left = left.clone();
                let right = right.clone();
                RawMetricBinary::Array(
                    py.detach(move || {
                        let mut left_rows = left.row_operation();
                        let mut right_rows = right.row_operation();
                        left_rows
                            .rows_with_missing(R::missing_value)
                            .zip(right_rows.rows_with_missing(R::missing_value))
                            .map(|(left_row, right_row)| match (left_row, right_row) {
                                (ArrayRow::Present(a), ArrayRow::Present(b)) => {
                                    let row_index = a.logical();
                                    let a = left_rows.prepared(a)?;
                                    let b = right_rows.prepared(b)?;
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
                    }),
                    missing,
                )
            },
        }
    };
    drop(scratch);
    match raw {
        RawMetricBinary::Scalar(result) => R::into_py(result.map_err(PyErr::from)?, py, frame),
        RawMetricBinary::Array(result, missing) => {
            let values = match result {
                Ok(values) => values,
                Err(error) => {
                    drop(missing);
                    return Err(rows_err(error));
                },
            };
            R::bulk_into_py_masked(values, py, frame, missing.as_ref())
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
    #[cfg(test)]
    binary_dispatch_counters::frame_resolution();
    let frame = Frame::compatible_parts(left.0, left.1, right.0, right.1, op_name)?;
    let domain = crate::geometry::topology_domain(&frame)?;
    Ok((frame, domain))
}

fn run_binary_scalar_scalar<R: Send>(
    py: Python<'_>,
    left: &PyGeometry,
    right: &PyGeometry,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> Result<R> {
    let ctx = CachelessRowContext {
        frame,
        domain,
        metric,
        lane: Lane::Scalar,
    };
    let left_plan = left.prepared_plan();
    let right_plan = right.prepared_plan();
    py.detach(move || kernel(&left_plan, &right_plan, &ctx))
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
    let admission = admit_binary_inputs(
        GeometryInput::Many(left),
        classify_required(right)?,
        op_name,
    )?;
    let missing = match admission.right {
        One(_) => None,
        Many(right) => missing::union_many([left.missing(), right.missing()]),
    };
    let mut scratch = MetricScratch::default();
    let result = {
        let metric = resolver.resolve_ctx(&admission.frame, &mut scratch)?;
        #[cfg(test)]
        binary_dispatch_counters::general_lane();
        match admission.right {
            One(right) => run_binary_fixed_array_rows(
                py,
                left,
                right,
                ArrayOperandSide::Left,
                &admission.frame,
                admission.domain,
                metric,
                kernel,
            )
            .map(|(values, _)| values),
            Many(right) => run_binary_array_array_rows(
                py,
                left,
                right,
                missing,
                &admission.frame,
                admission.domain,
                metric,
                kernel,
            )
            .map(|(values, ..)| values),
        }
    };
    drop(scratch);
    result.map_err(rows_err)
}

/// Split one operand when the resolved frame is geographic and the shape
/// crosses ±180, reusing the handle otherwise.
fn split_operand_arc_for_topology(
    domain: TopologyDomain,
    geometry: &PyGeometry,
    metric: MetricCtx<'_>,
) -> Result<Arc<PreparedPlan>> {
    let plan = geometry.prepared_plan();
    if let TopologyDomain::Geographic(frame) = domain
        && crate::geometry::crosses_antimeridian_in_frame(plan.shape(), frame)
    {
        let metric_access = || {
            metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic topology split source ordinates require the operation's CRS metric",
                )
            })
        };
        Ok(Arc::new(PreparedPlan::new(ShapeData::from(
            plan.shape()
                .split_antimeridian_in_frame_with_metric(frame, Some(&metric_access))?,
        ))))
    } else {
        Ok(plan)
    }
}

/// The shared antimeridian gate for geometry-returning binary lanes.
fn invoke_topology_geometry_kernel<K>(
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
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
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic topology split source ordinates require the operation's CRS metric",
                    )
                })
            };
            Some(PreparedPlan::new(ShapeData::from(
                left.shape()
                    .split_antimeridian_in_frame_with_metric(frame, Some(&metric_access))?,
            )))
        } else {
            None
        };
        let right_split = if right_crosses {
            let metric_access = || {
                metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic topology split source ordinates require the operation's CRS metric",
                    )
                })
            };
            Some(PreparedPlan::new(ShapeData::from(
                right
                    .shape()
                    .split_antimeridian_in_frame_with_metric(frame, Some(&metric_access))?,
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
pub(crate) fn dispatch_binary_geometry<F>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    resolver: MetricResolver,
    fast_path: &F,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<Shape>
    + Send
    + Sync,
) -> PyResult<Py<PyAny>>
where
    F: BinaryArrayFastPath<Shape>,
{
    let admission = admit_binary(left, right, op_name)?;
    dispatch_binary_geometry_admitted_with_resolver(
        py, &admission, op_name, resolver, fast_path, kernel,
    )
}

pub(crate) fn dispatch_binary_geometry_admitted_with_resolver<F>(
    py: Python<'_>,
    admission: &BinaryAdmission<'_>,
    op_name: &str,
    resolver: MetricResolver,
    fast_path: &F,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<Shape>
    + Send
    + Sync,
) -> PyResult<Py<PyAny>>
where
    F: BinaryArrayFastPath<Shape>,
{
    use GeometryInput::{Many, One};

    match (admission.left, admission.right) {
        (One(left), One(right)) => {
            let frame = admission.frame.clone();
            let mut metric_scratch = MetricScratch::default();
            let result = {
                let (domain, metric) =
                    match resolver.resolve_topology_ctx(&frame, &mut metric_scratch) {
                        Ok(context) => context,
                        Err(error) => {
                            drop(metric_scratch);
                            return Err(error);
                        },
                    };
                let ctx = CachelessRowContext {
                    frame: &frame,
                    domain,
                    metric,
                    lane: Lane::Scalar,
                };
                let left_shape = left.prepared_plan();
                let right_shape = right.prepared_plan();
                invoke_topology_geometry_kernel(
                    domain,
                    metric,
                    &left_shape,
                    &right_shape,
                    &|l, r| kernel(l, r, &ctx),
                )
            };
            drop(metric_scratch);
            let shape = result?;
            Ok(Typed(PyGeometry::with_frame(shape, frame))
                .into_pyobject(py)?
                .unbind())
        },
        (One(left), Many(right)) => {
            let frame = admission.frame.clone();
            let admission_domain = admission.domain;
            if fast_path.is_eligible(right, GeometryInput::One(left), op_name, &frame)
                && let Some(result) = fast_path.try_dispatch(
                    py,
                    right,
                    GeometryInput::One(left),
                    op_name,
                    &frame,
                    admission_domain,
                    None,
                    None,
                    None,
                )
            {
                return result;
            }
            let output_frame = frame.clone();
            let mut metric_scratch = MetricScratch::default();
            let result = {
                let (domain, metric) =
                    match resolver.resolve_topology_ctx(&frame, &mut metric_scratch) {
                        Ok(context) => context,
                        Err(error) => {
                            drop(metric_scratch);
                            return Err(error);
                        },
                    };
                let left_fixed = match split_operand_arc_for_topology(domain, left, metric) {
                    Ok(left_fixed) => left_fixed,
                    Err(error) => {
                        drop(metric_scratch);
                        return Err(PyErr::from(error));
                    },
                };
                // Overlay rows use transient plans and retain no frame cache;
                // geographic carry keeps its lazy metric scratch call-local.
                run_array_rows(
                    py,
                    right,
                    &frame,
                    domain,
                    metric,
                    move |row, right_row, ctx, products| {
                        let known_bounds = right_row.quick_bounds();
                        let right_data = products.warm_prepared_at(row, right_row, known_bounds)?;
                        invoke_topology_geometry_kernel(
                            domain,
                            metric,
                            &left_fixed,
                            &right_data,
                            &|l, r| kernel(l, r, ctx),
                        )
                    },
                )
            };
            drop(metric_scratch);
            let shapes = result.map_err(rows_err)?;
            Shape::bulk_into_py_masked(shapes, py, &output_frame, right.missing())
        },
        (Many(left), right_in) => {
            let frame = admission.frame.clone();
            let admission_domain = admission.domain;
            if fast_path.is_eligible(left, right_in, op_name, &frame)
                && let Some(result) = fast_path.try_dispatch(
                    py,
                    left,
                    right_in,
                    op_name,
                    &frame,
                    admission_domain,
                    None,
                    None,
                    None,
                )
            {
                return result;
            }
            let mut scratch = MetricScratch::default();
            let result = {
                let (domain, metric) = resolver.resolve_topology_ctx(&frame, &mut scratch)?;
                geometry_kernel_over_array_admitted(
                    py, left, right_in, &frame, domain, metric, kernel,
                )
            };
            drop(scratch);
            let array = result?;
            Ok(array.into_pyobject(py)?.unbind().into())
        },
    }
}

fn geometry_kernel_over_array_admitted<K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: GeometryInput<'_>,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> PyResult<PyGeometryArray>
where
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<Shape> + Send + Sync,
{
    use GeometryInput::{Many, One};
    let row_box = |bounds: &Option<crate::array::ElementBounds>,
                   row: usize|
     -> Option<crate::geometry::Bounds> {
        bounds.as_ref().and_then(|b| b.get(row).copied()).flatten()
    };
    // Prefer already-warm bounds caches, but do NOT force-initialize them:
    // a pure overlay of two packed point arrays must not retain multi-MiB
    // element-bounds sidecars on the inputs. Cold rows use `quick_bounds`.
    let left_bounds = left.existing_element_bounds();
    let result = match right {
        One(right) => {
            let right_shape = match split_operand_arc_for_topology(domain, right, metric) {
                Ok(right_shape) => right_shape,
                Err(error) => return Err(PyErr::from(error)),
            };
            // Transient row plans retain no frame caches; any geographic carry
            // metric scratch belongs only to this operation.
            run_array_rows(
                py,
                left,
                frame,
                domain,
                metric,
                move |row, left_row, ctx, products| {
                    let left_box = row_box(&left_bounds, row).or_else(|| left_row.quick_bounds());
                    let left_data = products.warm_prepared_at(row, left_row, left_box)?;
                    invoke_topology_geometry_kernel(
                        domain,
                        metric,
                        &left_data,
                        &right_shape,
                        &|l, r| kernel(l, r, ctx),
                    )
                },
            )
        },
        Many(right) => {
            let right_bounds = right.existing_element_bounds();
            run_paired_rows(
                py,
                left,
                right,
                frame,
                domain,
                metric,
                move |row, left_row, right_row, ctx, left_products, right_products| {
                    let left_box = row_box(&left_bounds, row).or_else(|| left_row.quick_bounds());
                    let right_box =
                        row_box(&right_bounds, row).or_else(|| right_row.quick_bounds());
                    let left_data = left_products.warm_prepared_at(row, left_row, left_box)?;
                    let right_data = right_products.warm_prepared_at(row, right_row, right_box)?;
                    invoke_topology_geometry_kernel(
                        domain,
                        metric,
                        &left_data,
                        &right_data,
                        &|l, r| kernel(l, r, ctx),
                    )
                },
            )
        },
    };
    let shapes = result.map_err(rows_err)?;
    let output_missing = match right {
        One(_) => left.missing().cloned(),
        Many(right) => missing::union_many([left.missing(), right.missing()]),
    };
    Ok(PyGeometryArray::from_shapes(shapes, frame.clone())?.with_missing_mask(output_missing)?)
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
    let admission = admit_binary_inputs(GeometryInput::Many(left), right, op_name)?;
    let mut scratch = MetricScratch::default();
    let result = {
        let metric = resolver.resolve_ctx(&admission.frame, &mut scratch)?;
        geometry_kernel_over_array_admitted(
            py,
            left,
            admission.right,
            &admission.frame,
            admission.domain,
            metric,
            kernel,
        )
    };
    drop(scratch);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crs::RUNTIME_STATE_TEST_LOCK;
    use crate::dispatch::kernels;
    use crate::geometry::Point;
    use crate::py::errors::CRSError;
    use crate::py::numpy::bool_array;
    use crate::test_support::binary_dispatch_counters::Snapshot;

    struct RejectingFastPath;

    impl BinaryArrayFastPath<bool> for RejectingFastPath {
        fn is_eligible(
            &self,
            _left: &PyGeometryArray,
            _right: GeometryInput<'_>,
            _op_name: &str,
            _frame: &Frame,
        ) -> bool {
            true
        }

        fn try_dispatch(
            &self,
            _py: Python<'_>,
            _left: &PyGeometryArray,
            _right: GeometryInput<'_>,
            _op_name: &str,
            _frame: &Frame,
            _domain: TopologyDomain,
            _missing: Option<&ValidityMask>,
            _model: Option<&MetricModel>,
            _metric: Option<&ResolvedMetric>,
        ) -> Option<PyResult<Py<PyAny>>> {
            panic!("fast path must not run after metric resolution fails");
        }
    }

    struct TakingFastPath;

    impl BinaryArrayFastPath<bool> for TakingFastPath {
        fn is_eligible(
            &self,
            _left: &PyGeometryArray,
            _right: GeometryInput<'_>,
            _op_name: &str,
            _frame: &Frame,
        ) -> bool {
            true
        }

        fn try_dispatch(
            &self,
            py: Python<'_>,
            _left: &PyGeometryArray,
            _right: GeometryInput<'_>,
            _op_name: &str,
            _frame: &Frame,
            _domain: TopologyDomain,
            _missing: Option<&ValidityMask>,
            _model: Option<&MetricModel>,
            _metric: Option<&ResolvedMetric>,
        ) -> Option<PyResult<Py<PyAny>>> {
            Some(bool_array(py, vec![true]))
        }
    }

    struct DecliningFastPath;

    impl BinaryArrayFastPath<bool> for DecliningFastPath {
        fn is_eligible(
            &self,
            _left: &PyGeometryArray,
            _right: GeometryInput<'_>,
            _op_name: &str,
            _frame: &Frame,
        ) -> bool {
            true
        }

        fn try_dispatch(
            &self,
            _py: Python<'_>,
            _left: &PyGeometryArray,
            _right: GeometryInput<'_>,
            _op_name: &str,
            _frame: &Frame,
            _domain: TopologyDomain,
            _missing: Option<&ValidityMask>,
            _model: Option<&MetricModel>,
            _metric: Option<&ResolvedMetric>,
        ) -> Option<PyResult<Py<PyAny>>> {
            None
        }
    }

    fn metric_resolver() -> MetricResolver {
        MetricResolver::Metric {
            op: MetricOp::Distance,
            unit: None,
        }
    }

    fn point_array_pair<'py>(
        py: Python<'py>,
        frame: &Frame,
    ) -> (Bound<'py, PyAny>, Bound<'py, PyAny>) {
        let left = PyGeometryArray::from_shapes(
            vec![Shape::Point(Point::new_raw_xy(0.0, 0.0))],
            frame.clone(),
        )
        .unwrap();
        let right = PyGeometryArray::from_shapes(
            vec![Shape::Point(Point::new_raw_xy(1.0, 0.0))],
            frame.clone(),
        )
        .unwrap();
        (
            Bound::new(py, left).unwrap().into_any(),
            Bound::new(py, right).unwrap().into_any(),
        )
    }

    #[test]
    fn fast_path_admission_counters_are_single_pass_when_taken() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let (left, right) = point_array_pair(py, &Frame::Crs("EPSG:3857".into()));
            binary_dispatch_counters::reset();
            let result = dispatch_binary(
                py,
                &left,
                &right,
                "distance",
                metric_resolver(),
                &TakingFastPath,
                |_left, _right, _ctx| Ok(false),
            )
            .expect("taken fast path")
            .bind(py)
            .extract::<Vec<bool>>()
            .expect("fast path bool array");
            assert_eq!(result, [true]);
            assert_eq!(binary_dispatch_counters::snapshot(), Snapshot {
                classifications: 2,
                paired_array_validations: 1,
                frame_resolutions: 1,
                metric_context_acquisitions: 1,
                runtime_state_snapshots: 2,
                fast_path_attempts: 1,
                fast_path_taken: 1,
                fast_path_declines: 0,
                general_lane_runs: 0,
                #[cfg(debug_assertions)]
                point_predicate_dispatches: 0,
            });
        });
    }

    #[test]
    fn disabled_fast_path_preserves_values_in_the_general_array_lane() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let (left, right) = point_array_pair(py, &Frame::Crs("EPSG:3857".into()));
            binary_dispatch_counters::reset();
            let result = dispatch_binary(
                py,
                &left,
                &right,
                "distance",
                metric_resolver(),
                &DecliningFastPath,
                |left, right, ctx| {
                    assert!(ctx.metric.resolved().is_some());
                    Ok(left.shape() != right.shape())
                },
            )
            .expect("general lane after fast-path decline");
            assert_eq!(
                result
                    .bind(py)
                    .extract::<Vec<bool>>()
                    .expect("general array bool"),
                vec![true]
            );
            assert_eq!(binary_dispatch_counters::snapshot(), Snapshot {
                classifications: 2,
                paired_array_validations: 1,
                frame_resolutions: 1,
                metric_context_acquisitions: 1,
                runtime_state_snapshots: 2,
                fast_path_attempts: 1,
                fast_path_taken: 0,
                fast_path_declines: 1,
                general_lane_runs: 1,
                #[cfg(debug_assertions)]
                point_predicate_dispatches: 0,
            });
        });
    }

    #[test]
    fn three_d_no_fast_lane_resolves_once_without_attempting_a_fast_path() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let frame = Frame::Crs("EPSG:3857".into());
            let left_shape = crate::io::parse_wkt("POINT Z (0 0 0)").unwrap();
            let right_shape = crate::io::parse_wkt("POINT Z (0 0 3)").unwrap();
            let array = PyGeometryArray::from_shapes(vec![left_shape], frame.clone()).unwrap();
            let right = PyGeometry::with_frame(right_shape, frame);
            let left = Bound::new(py, array).unwrap().into_any();
            let right = Bound::new(py, right).unwrap().into_any();
            binary_dispatch_counters::reset();
            let result = dispatch_binary(
                py,
                &left,
                &right,
                "distance_3d",
                MetricResolver::Metric3d {
                    op: MetricOp::Distance3d,
                    unit: None,
                },
                &NoBinaryFastPath,
                kernels::binary_distance_3d,
            )
            .expect("3D general lane");
            assert_eq!(
                result
                    .bind(py)
                    .extract::<Vec<f64>>()
                    .expect("3D array result"),
                vec![3.0]
            );
            assert_eq!(binary_dispatch_counters::snapshot(), Snapshot {
                classifications: 2,
                paired_array_validations: 0,
                frame_resolutions: 1,
                metric_context_acquisitions: 1,
                runtime_state_snapshots: 2,
                fast_path_attempts: 0,
                fast_path_taken: 0,
                fast_path_declines: 0,
                general_lane_runs: 1,
                #[cfg(debug_assertions)]
                point_predicate_dispatches: 0,
            });
        });
    }

    #[test]
    fn declined_fast_path_preserves_the_general_error_after_context_release() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let (left, right) = point_array_pair(py, &Frame::Crs("EPSG:3857".into()));
            binary_dispatch_counters::reset();
            let error = dispatch_binary(
                py,
                &left,
                &right,
                "distance",
                metric_resolver(),
                &DecliningFastPath,
                |_left, _right, _ctx| {
                    Err(crate::geometry::GeometryErrorKind::message(
                        "general lane error",
                    ))
                },
            )
            .expect_err("the disabled fast lane must expose the general error");
            assert!(error.to_string().contains("general lane error"));
            let counters = binary_dispatch_counters::snapshot();
            assert_eq!(counters.classifications, 2);
            assert_eq!(counters.paired_array_validations, 1);
            assert_eq!(counters.frame_resolutions, 1);
            assert_eq!(counters.metric_context_acquisitions, 1);
            assert_eq!(counters.runtime_state_snapshots, 2);
            assert_eq!(counters.fast_path_attempts, 1);
            assert_eq!(counters.fast_path_taken, 0);
            assert_eq!(counters.fast_path_declines, 1);
            assert_eq!(counters.general_lane_runs, 1);
        });
    }

    #[test]
    fn metric_array_admission_resolves_the_specialized_lane_once() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let (left, right) = point_array_pair(py, &Frame::Crs("EPSG:3857".into()));
            binary_dispatch_counters::reset();
            let result = crate::dispatch::dispatch_distance(py, &left, &right, None)
                .expect("specialized metric array lane");
            assert_eq!(
                result
                    .bind(py)
                    .extract::<Vec<f64>>()
                    .expect("distance array"),
                vec![1.0]
            );
            let counters = binary_dispatch_counters::snapshot();
            assert_eq!(counters.classifications, 2);
            assert_eq!(counters.paired_array_validations, 1);
            assert_eq!(counters.frame_resolutions, 1);
            assert_eq!(counters.metric_context_acquisitions, 1);
            assert_eq!(counters.runtime_state_snapshots, 2);
            assert_eq!(counters.fast_path_attempts, 0);
            assert_eq!(counters.fast_path_taken, 0);
            assert_eq!(counters.fast_path_declines, 0);
            assert_eq!(counters.general_lane_runs, 0);
        });
    }

    #[test]
    fn fast_path_returns_metric_resolution_error_before_dispatch() {
        Python::initialize();
        Python::attach(|py| {
            let frame = Frame::Crs("EPSG:4326".into());
            let array = PyGeometryArray::from_shapes(
                vec![Shape::Point(Point::new_raw_xy(0.0, 0.0))],
                frame.clone(),
            )
            .unwrap();
            let right = PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(1.0, 0.0)), frame);
            let array = Bound::new(py, array).unwrap().into_any();
            let right = Bound::new(py, right).unwrap().into_any();
            let result = dispatch_binary(
                py,
                &array,
                &right,
                "distance_3d",
                MetricResolver::Metric3d {
                    op: MetricOp::Distance3d,
                    unit: None,
                },
                &RejectingFastPath,
                |_left, _right, _ctx| Ok(false),
            );
            let Err(error) = result else {
                panic!("metric resolution failure must be returned by the fast-path gate");
            };
            assert!(error.is_instance_of::<CRSError>(py));
        });
    }
}
