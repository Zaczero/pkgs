#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::array::{GeometryArrayStorage, MissingMask, ShapeRow};
use crate::boundary::metadata::Frame;
use crate::broadcast::{
    BulkElement, CollectRows as _, GeometryInput, classify_required, paired_arrays,
    paired_arrays_len, rows_err,
};
use crate::crs::{MetricModel, ResolvedMetric};
use crate::dispatch::metric::{Lane, MetricCtx, MetricScratch, OpCtx};
use crate::error::Result;
use crate::geometry::{Dimension, Shape, ShapeData, is_geographic_frame};
use crate::{PyGeometry, PyGeometryArray, Typed};

type MaskedRows<R> = (Vec<R>, Option<MissingMask>);
type FramedMaskedRows<R> = (Vec<R>, Option<MissingMask>, Frame);

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
    let metric = resolver
        .resolve_ctx(frame, op_name, &mut metric_scratch)
        .ok();
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

/// Whether the per-row binary fallback materializes frame-dependent caches.
///
/// Derived from [`MetricCtx`]: only `Metric` kernels (distance/dwithin/LRS)
/// inspect cached fields. `None` and `Antimeridian` need no frame cache —
/// acquiring an empty `FrameDependentCaches` Arc per row is retained memory
/// with no payoff.
#[derive(Clone, Copy)]
enum FrameCachePolicy {
    None,
    Eager,
}

impl FrameCachePolicy {
    const fn from_metric(metric: MetricCtx<'_>) -> Self {
        match metric {
            MetricCtx::Metric { .. } => Self::Eager,
            MetricCtx::None | MetricCtx::Antimeridian(_) => Self::None,
        }
    }
}

fn run_array_rows<R, K>(
    py: Python<'_>,
    array: &PyGeometryArray,
    frame: &Frame,
    geographic: bool,
    metric: MetricCtx<'_>,
    kernel: K,
) -> PyResult<Vec<R>>
where
    R: BulkElement,
    K: for<'row, 'ctx> Fn(usize, ShapeRow<'row>, &OpCtx<'ctx>) -> Result<R> + Send + Sync,
{
    let array = array.clone();
    let missing = array.missing().cloned();
    let frame_caches = FrameCachePolicy::from_metric(metric);
    py.detach(move || {
        let frame = &frame;
        array
            .storage()
            .iter_rows()
            .enumerate()
            .map(|(row, shape_row)| {
                if missing.as_ref().is_some_and(|mask| mask[row]) {
                    return Ok(R::missing_value());
                }
                // Demand-driven: only MetricCtx::Metric asks for frame caches.
                let row_cache;
                let left_frame_cache = match frame_caches {
                    FrameCachePolicy::Eager => {
                        row_cache = array.row_frame_cache(row);
                        Some(row_cache.as_ref())
                    },
                    FrameCachePolicy::None => None,
                };
                let ctx = OpCtx {
                    frame,
                    geographic,
                    metric,
                    lane: Lane::Array(row),
                    left_frame_cache,
                    right_frame_cache: None,
                };
                kernel(row, shape_row, &ctx)
            })
            .collect_rows()
    })
    .map_err(rows_err)
}

fn run_paired_rows<R, K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: &PyGeometryArray,
    rows: (Arc<GeometryArrayStorage>, Arc<GeometryArrayStorage>),
    frame: &Frame,
    geographic: bool,
    metric: MetricCtx<'_>,
    kernel: K,
) -> PyResult<MaskedRows<R>>
where
    R: BulkElement,
    K: for<'left, 'right, 'ctx> Fn(
            usize,
            ShapeRow<'left>,
            ShapeRow<'right>,
            &OpCtx<'ctx>,
        ) -> Result<R>
        + Send
        + Sync,
{
    let (lefts, rights) = rows;
    let left_array = left.clone();
    let right_array = right.clone();
    let missing = crate::array::missing::union_pair(left.missing(), right.missing());
    let row_missing = missing.clone();
    let frame_caches = FrameCachePolicy::from_metric(metric);
    let values = py
        .detach(move || {
            let frame = &frame;
            lefts
                .iter_rows()
                .zip(rights.iter_rows())
                .enumerate()
                .map(|(row, (left_row, right_row))| {
                    if row_missing.as_ref().is_some_and(|mask| mask[row]) {
                        return Ok(R::missing_value());
                    }
                    let left_owned;
                    let right_owned;
                    let (left_frame_cache, right_frame_cache) = match frame_caches {
                        FrameCachePolicy::Eager => {
                            left_owned = left_array.row_frame_cache(row);
                            right_owned = right_array.row_frame_cache(row);
                            (Some(left_owned.as_ref()), Some(right_owned.as_ref()))
                        },
                        FrameCachePolicy::None => (None, None),
                    };
                    let ctx = OpCtx {
                        frame,
                        geographic,
                        metric,
                        lane: Lane::Array(row),
                        left_frame_cache,
                        right_frame_cache,
                    };
                    kernel(row, left_row, right_row, &ctx)
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
    K: Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
{
    let (frame, geographic) = match side {
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
    let metric = resolver.resolve_ctx(&frame, op_name, &mut metric_scratch)?;
    let fixed_shape = Arc::clone(&fixed.shape);
    let fixed_cache = Arc::clone(&fixed.frame_cache);
    let array_handle = array.clone();
    // Metric/predicate binary array×scalar: keep prepared handles (`&ShapeData`);
    // frame caches are derived from MetricCtx. Never demote these kernels to
    // bare `&Shape` (Deref coercion would drop prepared engines).
    let values = run_array_rows(
        py,
        array,
        &frame,
        geographic,
        metric,
        move |row, array_row, ctx| {
            let array_data = array_handle.prepared_row(row, array_row);
            let ordered = OpCtx {
                frame: ctx.frame,
                geographic: ctx.geographic,
                metric: ctx.metric,
                lane: ctx.lane,
                left_frame_cache: match side {
                    ArrayOperandSide::Left => ctx.left_frame_cache,
                    ArrayOperandSide::Right => Some(&fixed_cache),
                },
                right_frame_cache: match side {
                    ArrayOperandSide::Left => Some(&fixed_cache),
                    ArrayOperandSide::Right => ctx.left_frame_cache,
                },
            };
            match side {
                ArrayOperandSide::Left => kernel(&array_data, &fixed_shape, &ordered),
                ArrayOperandSide::Right => kernel(&fixed_shape, &array_data, &ordered),
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
    K: Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
{
    let (lefts, rights) = paired_arrays(left, right, op_name)?;
    let (frame, geographic) = binary_frame(
        (left.crs_ref(), left.epoch()),
        (right.crs_ref(), right.epoch()),
        op_name,
    )?;
    let output_frame = frame.clone();
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, op_name, &mut metric_scratch)?;
    let left_array = left.clone();
    let right_array = right.clone();
    // Prepared handles; frame caches derived from MetricCtx.
    let (values, missing) = run_paired_rows(
        py,
        left,
        right,
        (lefts, rights),
        &frame,
        geographic,
        metric,
        move |row, left_row, right_row, ctx| {
            let left_data = left_array.prepared_row(row, left_row);
            let right_data = right_array.prepared_row(row, right_row);
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
    kernel: impl Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
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

/// Shared binary prologue: the ONE frame-compatibility + geographic
/// resolution all five scalar/array shape handlers start from.
fn binary_frame(
    left: (Option<&crate::Crs>, Option<f64>),
    right: (Option<&crate::Crs>, Option<f64>),
    op_name: &str,
) -> PyResult<(Frame, bool)> {
    let frame = Frame::compatible_parts(left.0, left.1, right.0, right.1, op_name)?;
    let geographic = is_geographic_frame(&frame);
    Ok((frame, geographic))
}

fn dispatch_binary_scalar_scalar<R: BulkElement>(
    py: Python<'_>,
    left: &PyGeometry,
    right: &PyGeometry,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    kernel: impl Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let (frame, geographic) = binary_frame(
        (left.crs_ref(), left.epoch()),
        (right.crs_ref(), right.epoch()),
        op_name,
    )?;
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, op_name, &mut metric_scratch)?;
    let ctx = OpCtx {
        frame: &frame,
        geographic,
        metric,
        lane: Lane::Scalar,
        left_frame_cache: Some(&left.frame_cache),
        right_frame_cache: Some(&right.frame_cache),
    };
    let left_shape = Arc::clone(&left.shape);
    let right_shape = Arc::clone(&right.shape);
    let result = py.detach(move || kernel(&left_shape, &right_shape, &ctx))?;
    R::into_py(result, py, &frame)
}

fn dispatch_binary_scalar_array<R: BulkElement>(
    py: Python<'_>,
    left: &PyGeometry,
    right: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    op_name: &str,
    kernel: impl Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
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
    kernel: impl Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
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
    kernel: impl Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
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
    kernel: impl Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<R> + Send + Sync,
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
    geographic: bool,
    shape: &Arc<ShapeData>,
) -> Result<Arc<ShapeData>> {
    if geographic && shape.shape().crosses_antimeridian() {
        Ok(Arc::new(ShapeData::from(
            shape.shape().split_antimeridian()?,
        )))
    } else {
        Ok(Arc::clone(shape))
    }
}

/// The shared antimeridian gate for geometry-returning binary lanes.
fn invoke_topology_geometry_kernel<K>(
    geographic: bool,
    left: &ShapeData,
    right: &ShapeData,
    kernel: &K,
) -> Result<Shape>
where
    K: Fn(&ShapeData, &ShapeData) -> Result<Shape>,
{
    let left_crosses = geographic && left.crosses_antimeridian();
    let right_crosses = geographic && right.crosses_antimeridian();
    if left_crosses || right_crosses {
        let left = if left_crosses {
            ShapeData::from(left.shape().split_antimeridian()?)
        } else {
            ShapeData::new(left.shape().clone())
        };
        let right = if right_crosses {
            ShapeData::from(right.shape().split_antimeridian()?)
        } else {
            ShapeData::new(right.shape().clone())
        };
        kernel(&left, &right)
    } else {
        kernel(left, right)
    }
}

/// Bounds-disjoint overlay shortcut (union/intersection/symmetric_difference).
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum DisjointShortcut {
    Empty,
    Combine,
}

impl DisjointShortcut {
    pub(crate) fn for_op(op_name: &str) -> Option<Self> {
        if op_name == crate::OverlayOp::Intersection.name() {
            Some(Self::Empty)
        } else if op_name == crate::OverlayOp::Union.name()
            || op_name == crate::OverlayOp::SymmetricDifference.name()
        {
            Some(Self::Combine)
        } else {
            None
        }
    }

    fn resolve(
        self,
        left: Option<crate::geometry::Bounds>,
        right: Option<crate::geometry::Bounds>,
        min_dim: impl FnOnce() -> Dimension,
        combine: impl FnOnce() -> Shape,
    ) -> Option<Shape> {
        let (left, right) = (left?, right?);
        if left.intersects(right) {
            return None;
        }
        Some(match self {
            Self::Empty => crate::geometry::empty_shape_for_dimension(min_dim()),
            Self::Combine => combine(),
        })
    }
}

/// Geometry-returning binary dispatch with topology split + overlay disjoint shortcut.
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
    kernel: impl Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<Shape> + Send + Sync,
) -> PyResult<Py<PyAny>>
where
    F: BinaryArrayFastPath<Shape>,
{
    use GeometryInput::{Many, One};
    let (left_in, right_in) = (classify_required(left)?, classify_required(right)?);
    let resolver = super::metric::MetricResolver::None;

    match (left_in, right_in) {
        (One(left), One(right)) => {
            let (frame, geographic) = binary_frame(
                (left.crs_ref(), left.epoch()),
                (right.crs_ref(), right.epoch()),
                op_name,
            )?;
            let mut metric_scratch = MetricScratch::default();
            let metric = resolver.resolve_ctx(&frame, op_name, &mut metric_scratch)?;
            let ctx = OpCtx {
                frame: &frame,
                geographic,
                metric,
                lane: Lane::Scalar,
                left_frame_cache: Some(&left.frame_cache),
                right_frame_cache: Some(&right.frame_cache),
            };
            let left_shape = Arc::clone(&left.shape);
            let right_shape = Arc::clone(&right.shape);
            let shape = py.detach(move || {
                invoke_topology_geometry_kernel(geographic, &left_shape, &right_shape, &|l, r| {
                    kernel(l, r, &ctx)
                })
            })?;
            Ok(Typed(PyGeometry::with_frame(shape, frame))
                .into_pyobject(py)?
                .unbind())
        },
        (One(left), Many(right)) => {
            let (frame, geographic) = binary_frame(
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
            let metric = resolver.resolve_ctx(&frame, op_name, &mut metric_scratch)?;
            let left_fixed = split_operand_arc_for_topology(geographic, &left.shape)?;
            // Overlay: transient ShapeData only (no prepared-slot persist;
            // MetricCtx is None so no frame caches). Contained shortcuts still
            // see a ShapeData handle.
            let shapes = run_array_rows(
                py,
                right,
                &frame,
                geographic,
                metric,
                move |row, right_row, ctx| {
                    let known_bounds = right_row.quick_bounds();
                    let right_data =
                        right.warm_prepared_row_or_transient(row, right_row, known_bounds);
                    invoke_topology_geometry_kernel(
                        geographic,
                        &left_fixed,
                        &right_data,
                        &|l, r| kernel(l, r, ctx),
                    )
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

#[expect(
    clippy::too_many_lines,
    reason = "cohesive geometry array runner: one branch owns scalar-right shortcut, one owns pair shortcut"
)]
pub(crate) fn geometry_kernel_over_array<K>(
    py: Python<'_>,
    left: &PyGeometryArray,
    right: GeometryInput<'_>,
    op_name: &str,
    resolver: super::metric::MetricResolver,
    kernel: K,
) -> PyResult<PyGeometryArray>
where
    K: Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> Result<Shape> + Send + Sync,
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
    let disjoint_shortcut = DisjointShortcut::for_op(op_name);
    let row_box = |bounds: &Option<crate::array::ElementBounds>,
                   row: usize|
     -> Option<crate::geometry::Bounds> {
        bounds.as_ref().and_then(|b| b.get(row).copied()).flatten()
    };
    let geographic = is_geographic_frame(&frame);
    let mut metric_scratch = MetricScratch::default();
    let metric = resolver.resolve_ctx(&frame, op_name, &mut metric_scratch)?;
    // Prefer already-warm bounds caches, but do NOT force-initialize them:
    // a pure overlay of two packed point arrays must not retain multi-MiB
    // element-bounds sidecars on the inputs. Cold rows use `quick_bounds`.
    let left_bounds = left.bounds_cache.get().cloned().flatten();
    let (shapes, output_missing) = match right {
        One(right) => {
            let right_shape = split_operand_arc_for_topology(geographic, &right.shape)?;
            let right_bounds = right_shape.bounds();
            let right_dim = right_shape.shape().topological_dimension();
            // Pure topology overlay: MetricCtx is None → no frame caches.
            let shapes = run_array_rows(
                py,
                left,
                &frame,
                geographic,
                metric,
                move |row, left_row, ctx| {
                    let left_box = row_box(&left_bounds, row).or_else(|| left_row.quick_bounds());
                    if let Some(result) = disjoint_shortcut.and_then(|shortcut| {
                        shortcut.resolve(
                            left_box,
                            right_bounds,
                            || left_row.topological_dimension().min(right_dim),
                            || {
                                left_row.with_shape(|left| {
                                    crate::geometry::disjoint_overlay_combine(
                                        left,
                                        right_shape.shape(),
                                    )
                                })
                            },
                        )
                    }) {
                        return Ok(result);
                    }
                    let left_data = left.warm_prepared_row_or_transient(row, left_row, left_box);
                    invoke_topology_geometry_kernel(
                        geographic,
                        &left_data,
                        &right_shape,
                        &|l, r| kernel(l, r, ctx),
                    )
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
                geographic,
                metric,
                move |row, left_row, right_row, ctx| {
                    let left_box = row_box(&left_bounds, row).or_else(|| left_row.quick_bounds());
                    let right_box =
                        row_box(&right_bounds, row).or_else(|| right_row.quick_bounds());
                    if let Some(result) = disjoint_shortcut.and_then(|shortcut| {
                        shortcut.resolve(
                            left_box,
                            right_box,
                            || {
                                left_row
                                    .topological_dimension()
                                    .min(right_row.topological_dimension())
                            },
                            || {
                                left_row.with_shape(|left| {
                                    right_row.with_shape(|right| {
                                        crate::geometry::disjoint_overlay_combine(left, right)
                                    })
                                })
                            },
                        )
                    }) {
                        return Ok(result);
                    }
                    let left_data = left.warm_prepared_row_or_transient(row, left_row, left_box);
                    let right_data =
                        right.warm_prepared_row_or_transient(row, right_row, right_box);
                    invoke_topology_geometry_kernel(geographic, &left_data, &right_data, &|l, r| {
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
