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
use crate::dispatch::MetricOp;
use crate::dispatch::metric::{
    CachelessRowContext, GeodesicBinaryRow, Lane, MetricCtx, MetricResolver, MetricScratch,
};
use crate::error::Result;
use crate::geometry::publication::{GeometryOutput, PublicationRows};
use crate::geometry::storage::{ShapeRow, ValidityMask, validity as missing};
use crate::geometry::{GeometryErrorKind, PreparedPlan, Shape, ShapeData, TopologyDomain};
#[cfg(test)]
use crate::test_support::binary_dispatch_counters;
use crate::{PyGeometry, PyGeometryArray, Typed};

struct FramedRows<R> {
    values: Vec<R>,
    missing: Option<ValidityMask>,
    frame: Frame,
}

impl<R> FramedRows<R> {
    fn fixed_array(values: Vec<R>, array: &PyGeometryArray, frame: &Frame) -> Self {
        Self {
            values,
            missing: array.missing().cloned(),
            frame: frame.clone(),
        }
    }

    fn paired_arrays(
        values: Vec<R>,
        left: &PyGeometryArray,
        right: &PyGeometryArray,
        frame: &Frame,
    ) -> Self {
        Self {
            values,
            missing: missing::union_many([left.missing(), right.missing()]),
            frame: frame.clone(),
        }
    }
}

impl<R: BulkElement> FramedRows<R> {
    fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        R::bulk_into_py_masked(self.values, py, &self.frame, self.missing.as_ref())
    }
}

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

fn run_array_shape_rows<R, K>(
    py: Python<'_>,
    array: &PyGeometryArray,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> PyResult<PyGeometryArray>
where
    R: Into<GeometryOutput>,
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
    let output_frame = frame.clone();
    let result = py.detach(move || {
        let mut rows = array.row_operation();
        let mut output = PublicationRows::with_capacity(array.len());
        for row in rows.rows_with_missing(Shape::empty_point) {
            let (logical, result) = match row {
                ArrayRow::Missing { logical, .. } => (logical, output.try_push_missing()),
                ArrayRow::Present(present) => {
                    let row_index = present.logical();
                    let shape_row = present.row();
                    let ctx = CachelessRowContext {
                        frame,
                        domain,
                        metric,
                        lane: Lane::Array(row_index),
                    };
                    let result = kernel(row_index, shape_row, &ctx, &mut rows)
                        .and_then(|output_row| output.try_push(output_row.into()));
                    (row_index, result)
                },
            };
            result.map_err(|error| (Some(logical), error))?;
        }
        output.finish(output_frame).map_err(|error| (None, error))
    });
    result.map_err(map_array_row_error)
}

fn run_paired_shape_rows<R, K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> PyResult<PyGeometryArray>
where
    R: Into<GeometryOutput>,
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
    let output_frame = frame.clone();
    let result = py.detach(move || {
        let mut left_rows = left_array.row_operation();
        let mut right_rows = right_array.row_operation();
        let mut output = PublicationRows::with_capacity(left_array.len());
        let left_iter = left_rows.rows_with_missing(Shape::empty_point);
        let right_iter = right_rows.rows_with_missing(Shape::empty_point);
        for (left, right) in left_iter.zip(right_iter) {
            let (logical, result) = match (left, right) {
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
                    let result = kernel(
                        row_index,
                        left_row,
                        right_row,
                        &ctx,
                        &mut left_rows,
                        &mut right_rows,
                    )
                    .and_then(|output_row| output.try_push(output_row.into()));
                    (row_index, result)
                },
                (ArrayRow::Missing { logical, .. }, _) | (_, ArrayRow::Missing { logical, .. }) => {
                    (logical, output.try_push_missing())
                },
            };
            result.map_err(|error| (Some(logical), error))?;
        }
        output.finish(output_frame).map_err(|error| (None, error))
    });
    result.map_err(map_array_row_error)
}

fn map_array_row_error((row, error): (Option<usize>, crate::error::Error)) -> PyErr {
    match row {
        Some(row) => rows_err((row, error)),
        None => PyErr::from(error),
    }
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
) -> Result<FramedRows<R>, (usize, crate::error::Error)>
where
    R: BulkElement,
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
    let values =
        run_binary_fixed_array_values(py, array, fixed, side, frame, domain, metric, kernel)?;
    Ok(FramedRows::fixed_array(values, array, frame))
}

fn run_binary_fixed_array_values<R, K>(
    py: Python<'_>,
    array: &PyGeometryArray,
    fixed: &PyGeometry,
    side: ArrayOperandSide,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> Result<Vec<R>, (usize, crate::error::Error)>
where
    R: BulkElement,
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
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
}

fn run_binary_array_array_rows<R, K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    frame: &Frame,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: K,
) -> Result<FramedRows<R>, (usize, crate::error::Error)>
where
    R: BulkElement,
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
    let values = run_binary_array_array_values(py, left, right, frame, domain, metric, kernel)?;
    Ok(FramedRows::paired_arrays(values, left, right, frame))
}

fn run_binary_array_array_values<R, K>(
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
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
    // Prepared handles; frame caches derived from MetricCtx.
    run_paired_rows(
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
    )
}

/// Binary scalar/array dispatch (four scalar×array shapes).
///
/// Resolves the metric once via ``op.resolver()``, checks frame compatibility
/// once per operand pair-set, runs the kernel inside one ``py.detach``, uses
/// caching prepared rows for array lanes, and bulk-converts via
/// [`BulkElement`].
pub(crate) fn dispatch_binary<R>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    resolver: super::metric::MetricResolver,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    R: BulkElement,
{
    let left = crate::normalize_geometry_batch(py, left)?;
    let right = crate::normalize_geometry_batch(py, right)?;
    let admission = admit_binary(&left, &right, op_name)?;
    dispatch_binary_admitted(py, &admission, resolver, kernel)
}

pub(crate) fn dispatch_binary_admitted<R>(
    py: Python<'_>,
    admission: &BinaryAdmission<'_>,
    resolver: super::metric::MetricResolver,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    R: BulkElement,
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
            resolver,
            kernel,
        ),
        (Many(left), One(right)) => dispatch_binary_array_admitted(
            py,
            admission,
            left,
            One(right),
            ArrayOperandSide::Left,
            resolver,
            kernel,
        ),
        (Many(left), Many(right)) => dispatch_binary_array_admitted(
            py,
            admission,
            left,
            Many(right),
            ArrayOperandSide::Left,
            resolver,
            kernel,
        ),
    }
}

fn dispatch_binary_array_admitted<R>(
    py: Python<'_>,
    admission: &BinaryAdmission<'_>,
    array: &PyGeometryArray,
    other: GeometryInput<'_>,
    side: ArrayOperandSide,
    resolver: super::metric::MetricResolver,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    R: BulkElement,
{
    let mut scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&admission.frame, &mut scratch)?;

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
        ),
        GeometryInput::Many(right) => run_binary_array_array_rows(
            py,
            array,
            right,
            &admission.frame,
            admission.domain,
            metric,
            kernel,
        ),
    };
    drop(scratch);
    result.map_err(rows_err)?.into_py(py)
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
    let mut scratch = MetricScratch::default();
    let result = {
        let metric = resolver.resolve_ctx(&admission.frame, &mut scratch)?;
        #[cfg(test)]
        binary_dispatch_counters::general_lane();
        match admission.right {
            One(right) => run_binary_fixed_array_values(
                py,
                left,
                right,
                ArrayOperandSide::Left,
                &admission.frame,
                admission.domain,
                metric,
                kernel,
            ),
            Many(right) => run_binary_array_array_values(
                py,
                left,
                right,
                &admission.frame,
                admission.domain,
                metric,
                kernel,
            ),
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
fn invoke_topology_geometry_kernel<R, K>(
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    left: &PreparedPlan,
    right: &PreparedPlan,
    split_geographic_inputs: bool,
    kernel: &K,
) -> Result<R>
where
    K: Fn(&PreparedPlan, &PreparedPlan) -> Result<R>,
{
    if !split_geographic_inputs {
        return kernel(left, right);
    }
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

fn geometry_output_shape(output: GeometryOutput) -> Result<Shape> {
    match output {
        GeometryOutput::Shape(shape) => Ok(shape),
        GeometryOutput::Pending(_) => Err(GeometryErrorKind::Invalid(
            "a pending array publication cannot enter a scalar geometry result".into(),
        )
        .into()),
    }
}

/// Geometry-returning binary dispatch with optional geographic input splitting.
pub(crate) fn dispatch_binary_geometry<R: Into<GeometryOutput>>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    resolver: MetricResolver,
    split_geographic_inputs: bool,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let left = crate::normalize_geometry_batch(py, left)?;
    let right = crate::normalize_geometry_batch(py, right)?;
    let admission = admit_binary(&left, &right, op_name)?;
    dispatch_binary_geometry_admitted_with_resolver(
        py,
        &admission,
        resolver,
        split_geographic_inputs,
        kernel,
    )
}

pub(crate) fn dispatch_binary_geometry_admitted_with_resolver<R: Into<GeometryOutput>>(
    py: Python<'_>,
    admission: &BinaryAdmission<'_>,
    resolver: MetricResolver,
    split_geographic_inputs: bool,
    kernel: impl Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
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
                    split_geographic_inputs,
                    &|l, r| kernel(l, r, &ctx),
                )
                .and_then(|output| geometry_output_shape(output.into()))
            };
            drop(metric_scratch);
            let shape = result?;
            Ok(Typed(PyGeometry::with_frame(shape, frame))
                .into_pyobject(py)?
                .unbind())
        },
        (One(left), Many(right)) => {
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
                let left_fixed = if split_geographic_inputs {
                    match split_operand_arc_for_topology(domain, left, metric) {
                        Ok(left_fixed) => left_fixed,
                        Err(error) => {
                            drop(metric_scratch);
                            return Err(PyErr::from(error));
                        },
                    }
                } else {
                    left.prepared_plan()
                };
                // Overlay rows use transient plans and retain no frame cache;
                // geographic carry keeps its lazy metric scratch call-local.
                run_array_shape_rows(
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
                            split_geographic_inputs,
                            &|l, r| kernel(l, r, ctx),
                        )
                    },
                )
            };
            drop(metric_scratch);
            let output = result?;
            Ok(output.into_pyobject(py)?.unbind().into())
        },
        (Many(left), right_in) => {
            let frame = admission.frame.clone();
            let mut scratch = MetricScratch::default();
            let result = {
                let (domain, metric) = resolver.resolve_topology_ctx(&frame, &mut scratch)?;
                geometry_kernel_over_array_admitted(
                    py,
                    left,
                    right_in,
                    domain,
                    metric,
                    split_geographic_inputs,
                    kernel,
                )
            };
            drop(scratch);
            let array = result?;
            Ok(array.into_pyobject(py)?.unbind().into())
        },
    }
}

fn geometry_kernel_over_array_admitted<R: Into<GeometryOutput>, K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: GeometryInput<'_>,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    split_geographic_inputs: bool,
    kernel: K,
) -> PyResult<PyGeometryArray>
where
    K: Fn(&PreparedPlan, &PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
{
    use GeometryInput::{Many, One};
    let frame = &left.frame;
    let row_box = |bounds: &Option<crate::array::ElementBounds>,
                   row: usize|
     -> Option<crate::geometry::Bounds> {
        bounds.as_ref().and_then(|b| b.get(row).copied()).flatten()
    };
    // Prefer already-warm bounds caches, but do NOT force-initialize them:
    // a pure overlay of two packed point arrays must not retain multi-MiB
    // element-bounds sidecars on the inputs. Cold rows use `quick_bounds`.
    let left_bounds = left.existing_element_bounds();

    match right {
        One(right) => {
            let right_shape = if split_geographic_inputs {
                match split_operand_arc_for_topology(domain, right, metric) {
                    Ok(right_shape) => right_shape,
                    Err(error) => return Err(PyErr::from(error)),
                }
            } else {
                right.prepared_plan()
            };
            // Transient row plans retain no frame caches; any geographic carry
            // metric scratch belongs only to this operation.
            run_array_shape_rows(
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
                        split_geographic_inputs,
                        &|l, r| kernel(l, r, ctx),
                    )
                },
            )
        },
        Many(right) => {
            let right_bounds = right.existing_element_bounds();
            run_paired_shape_rows(
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
                        split_geographic_inputs,
                        &|l, r| kernel(l, r, ctx),
                    )
                },
            )
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
    let admission = admit_binary_inputs(GeometryInput::Many(left), right, op_name)?;
    let mut scratch = MetricScratch::default();
    let result = {
        let metric = resolver.resolve_ctx(&admission.frame, &mut scratch)?;
        geometry_kernel_over_array_admitted(
            py,
            left,
            admission.right,
            admission.domain,
            metric,
            true,
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
    use crate::geometry::storage::{GeometryArrayStorage, PackedStorage};
    use crate::py::errors::CRSError;
    use crate::test_support::binary_dispatch_counters::Snapshot;

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

    fn nullable_measured_lines(missing: [bool; 3]) -> PyGeometryArray {
        let present = PyGeometryArray::from_shapes(
            vec![
                crate::io::parse_wkt("LINESTRING M (0 0 10, 1 0 20, 2 0 30)")
                    .expect("first measured line"),
                crate::io::parse_wkt("LINESTRING M (3 0 40, 4 0 50, 5 0 60)")
                    .expect("second measured line"),
            ],
            Frame::None,
        )
        .expect("homogeneous lines pack");
        PyGeometryArray::scatter_present_rows(
            &present,
            ValidityMask::from_vec(3, missing.to_vec()).expect("mask"),
        )
        .expect("nullable lines admit")
    }

    fn assert_packed_measured_lines(array: &PyGeometryArray, offsets: &[i32], measures: &[f64]) {
        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords,
            offsets: actual_offsets,
            ..
        }) = array.storage()
        else {
            panic!("homogeneous nullable lines stay packed");
        };
        assert_eq!(coords.ms(), Some(measures));
        assert_eq!(actual_offsets.as_slice(), offsets);
    }

    #[test]
    fn binary_geometry_scalar_array_stream_keeps_nullable_lines_packed() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let fixed = PyGeometry::with_frame(
                crate::io::parse_wkt("LINESTRING M (10 0 70, 11 0 80, 12 0 90)")
                    .expect("fixed measured line"),
                Frame::None,
            );
            let array = nullable_measured_lines([false, true, false]);
            let left = Bound::new(py, fixed).unwrap().into_any();
            let right = Bound::new(py, array).unwrap().into_any();
            let result = dispatch_binary_geometry(
                py,
                &left,
                &right,
                "test binary geometry",
                MetricResolver::None,
                true,
                |left, _, _| Ok(left.shape().clone()),
            )
            .expect("scalar/array geometry stream");
            let result = result.bind(py);
            let array = crate::broadcast::exact_geometry_array(result)
                .expect("geometry dispatch returns GeometryArray");

            assert_packed_measured_lines(array, &[0, 3, 3, 6], &[
                70.0, 80.0, 90.0, 70.0, 80.0, 90.0,
            ]);
            assert!(array.is_row_missing(1));
        });
    }

    #[test]
    fn binary_geometry_array_scalar_stream_keeps_nullable_lines_packed() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let array = nullable_measured_lines([false, true, false]);
            let fixed = PyGeometry::with_frame(
                crate::io::parse_wkt("LINESTRING M (10 0 70, 11 0 80, 12 0 90)")
                    .expect("fixed measured line"),
                Frame::None,
            );
            let output = geometry_kernel_over_array(
                py,
                &array,
                GeometryInput::One(&fixed),
                "test binary geometry",
                MetricResolver::None,
                |left, _, _| Ok(left.shape().clone()),
            )
            .expect("array/scalar geometry stream");

            assert_packed_measured_lines(&output, &[0, 3, 3, 6], &[
                10.0, 20.0, 30.0, 40.0, 50.0, 60.0,
            ]);
            assert!(output.is_row_missing(1));
        });
    }

    #[test]
    fn binary_geometry_array_pair_stream_unions_missing_rows() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let left = nullable_measured_lines([false, true, false]);
            let right = nullable_measured_lines([true, false, false]);
            let output = geometry_kernel_over_array(
                py,
                &left,
                GeometryInput::Many(&right),
                "test binary geometry",
                MetricResolver::None,
                |left, _, _| Ok(left.shape().clone()),
            )
            .expect("array-pair geometry stream");

            assert_packed_measured_lines(&output, &[0, 0, 0, 3], &[40.0, 50.0, 60.0]);
            assert!(output.is_row_missing(0));
            assert!(output.is_row_missing(1));
            assert!(!output.is_row_missing(2));
        });
    }

    #[test]
    fn binary_array_rows_preserve_values_and_admission_counts() {
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
                |left, right, ctx| {
                    let _resolved = ctx.metric.require_resolved(MetricOp::Distance)?;
                    Ok(left.shape() != right.shape())
                },
            )
            .expect("binary array row lane");
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
                general_lane_runs: 1,
                #[cfg(debug_assertions)]
                point_predicate_dispatches: 0,
            });
        });
    }

    #[test]
    fn three_d_binary_array_resolves_metric_once() {
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
                kernels::binary_distance_3d,
            )
            .expect("3D binary array lane");
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
                general_lane_runs: 1,
                #[cfg(debug_assertions)]
                point_predicate_dispatches: 0,
            });
        });
    }

    #[test]
    fn binary_array_kernel_error_is_propagated() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let (left, right) = point_array_pair(py, &Frame::Crs("EPSG:3857".into()));
            binary_dispatch_counters::reset();
            let error = dispatch_binary::<bool>(
                py,
                &left,
                &right,
                "distance",
                metric_resolver(),
                |_left, _right, _ctx| {
                    Err(crate::geometry::GeometryErrorKind::message(
                        "general lane error",
                    ))
                },
            )
            .expect_err("the array kernel error must be returned");
            assert!(error.to_string().contains("general lane error"));
            let counters = binary_dispatch_counters::snapshot();
            assert_eq!(counters.classifications, 2);
            assert_eq!(counters.paired_array_validations, 1);
            assert_eq!(counters.frame_resolutions, 1);
            assert_eq!(counters.metric_context_acquisitions, 1);
            assert_eq!(counters.runtime_state_snapshots, 2);
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
            assert_eq!(counters.general_lane_runs, 0);
        });
    }

    #[test]
    fn metric_resolution_error_precedes_binary_kernel() {
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
            let result = dispatch_binary::<bool>(
                py,
                &array,
                &right,
                "distance_3d",
                MetricResolver::Metric3d {
                    op: MetricOp::Distance3d,
                    unit: None,
                },
                |_left, _right, _ctx| panic!("metric resolution must fail before the row kernel"),
            );
            let Err(error) = result else {
                panic!("the metric resolver must reject 2D inputs before row execution");
            };
            assert!(error.is_instance_of::<CRSError>(py));
        });
    }
}
