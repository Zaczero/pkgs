use pyo3::prelude::*;

use crate::array::{ArrayRow, ArrayRowOperation, StreamingShapes};
use crate::broadcast::{
    BulkElement, CollectRows as _, GeometryInput, classify_input, expected_geometry_or_array,
    rows_err,
};
use crate::crs::frame::Frame;
use crate::dispatch::metric::{
    CachelessRowContext, Lane, LinearReferenceRow, MetricCtx, MetricResolver, MetricScratch,
};
use crate::dispatch::operation::{MetricOp, Operation};
use crate::dispatch::packed::{
    PackedLinearReference, PackedUnary, try_packed_linear_reference, try_unary_packed_array,
    try_unary_packed_bool, try_unary_packed_bounds, try_unary_packed_f64,
};
use crate::error::Result;
use crate::geometry::publication::{GeometryOutput, PublicationRows};
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
    let plan = geometry.prepared_plan();
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
    let mut metric_scratch =
        (!matches!(resolver, MetricResolver::None)).then(MetricScratch::default);
    let result = {
        let context_result = metric_scratch.as_mut().map_or_else(
            || {
                crate::geometry::topology_domain(&geometry.frame)
                    .map(|domain| (domain, MetricCtx::None))
                    .map_err(PyErr::from)
            },
            |scratch| resolver.resolve_topology_ctx(&geometry.frame, scratch),
        );
        let (domain, metric) = match context_result {
            Ok(context) => context,
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
        | Operation::Metric(MetricOp::Area | MetricOp::LineInterpolate | MetricOp::LineSubstring) => {
            UnaryRowMode::Prepared
        },
        Operation::AffineTransform
        | Operation::Boundary
        | Operation::BuildArea
        | Operation::Centroid
        | Operation::ClipByRect
        | Operation::ConvexHull
        | Operation::CrossesAntimeridian
        | Operation::Difference
        | Operation::Envelope
        | Operation::Force2d
        | Operation::Force3d
        | Operation::Intersection
        | Operation::IsCcw
        | Operation::IsClosed
        | Operation::IsConvex
        | Operation::IsEmpty
        | Operation::MinimumRotatedRectangle
        | Operation::Node
        | Operation::Normalize
        | Operation::OrientPolygons
        | Operation::PointOnSurface
        | Operation::Quantize
        | Operation::RemoveRepeatedPoints
        | Operation::Repair
        | Operation::Reverse
        | Operation::Rotate
        | Operation::Scale
        | Operation::Skew
        | Operation::Snap
        | Operation::Simplify
        | Operation::Smooth
        | Operation::SnapToGrid
        | Operation::SplitAntimeridian
        | Operation::SwapXy
        | Operation::SymmetricDifference
        | Operation::Translate
        | Operation::Union
        | Operation::UniquePoints
        | Operation::Metric(
            MetricOp::Bearing
            | MetricOp::Buffer
            | MetricOp::ConcaveHull
            | MetricOp::CrossTrackDistance
            | MetricOp::Destination
            | MetricOp::Distance
            | MetricOp::Distance3d
            | MetricOp::Dwithin
            | MetricOp::FrechetDistance
            | MetricOp::HausdorffDistance
            | MetricOp::InterpolateM
            | MetricOp::Length
            | MetricOp::Length3d
            | MetricOp::LineLocate
            | MetricOp::MaximumInscribedCircle
            | MetricOp::MaximumInscribedRadius
            | MetricOp::MinimumBoundingCircle
            | MetricOp::MinimumBoundingRadius
            | MetricOp::MinimumClearance
            | MetricOp::MinimumClearanceLine
            | MetricOp::Nearest
            | MetricOp::NearestPoints
            | MetricOp::OffsetCurve
            | MetricOp::PointBetween
            | MetricOp::PointBetweenRhumb
            | MetricOp::Polylabel
            | MetricOp::RhumbBearing
            | MetricOp::RhumbDestination
            | MetricOp::RhumbDistance
            | MetricOp::Segmentize
            | MetricOp::ShortestLine
            | MetricOp::SpatialIndexDistanceQuery,
        )
        | Operation::Predicate(_) => UnaryRowMode::ShapeOnly,
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

fn execute_unary_shape_rows<R: Into<GeometryOutput>>(
    py: Python<'_>,
    array: PyGeometryArray,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    mode: UnaryRowMode,
    kernel: impl Fn(&PreparedPlan, &CachelessRowContext<'_>, &mut ArrayRowOperation<'_>) -> Result<R>
    + Send
    + Sync,
) -> std::result::Result<PublicationRows, (usize, crate::error::Error)> {
    py.detach(move || {
        let mut rows = array.row_operation();
        let mut output = PublicationRows::with_capacity(array.len());
        for row in rows.rows_with_missing(Shape::empty_point) {
            let (logical, result) = match row {
                ArrayRow::Missing { logical, .. } => (logical, output.try_push_missing()),
                ArrayRow::Present(present) => {
                    let row_index = present.logical();
                    let shape_row = present.row();
                    let ctx = CachelessRowContext {
                        frame: &array.frame,
                        domain,
                        metric,
                        lane: Lane::Array(row_index),
                    };
                    let data = match mode {
                        UnaryRowMode::ShapeOnly => crate::array::PreparedRow::transient(shape_row),
                        UnaryRowMode::Prepared => rows.prepared_at(row_index, shape_row),
                    };
                    let result = match data {
                        Ok(data) => kernel(&data, &ctx, &mut rows)
                            .and_then(|value| output.try_push(value.into())),
                        Err(error) => Err(error),
                    };
                    (row_index, result)
                },
            };
            result.map_err(|error| (logical, error))?;
        }
        Ok(output)
    })
}

fn run_unary_shape_rows<R: Into<GeometryOutput>>(
    py: Python<'_>,
    array: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    mode: UnaryRowMode,
    kernel: impl Fn(&PreparedPlan, &CachelessRowContext<'_>, &mut ArrayRowOperation<'_>) -> Result<R>
    + Send
    + Sync,
) -> PyResult<PyGeometryArray> {
    let frame = array.frame.clone();
    let rows = match resolver {
        MetricResolver::None => {
            let domain = crate::geometry::topology_domain(&frame)?;
            execute_unary_shape_rows(py, array.clone(), domain, MetricCtx::None, mode, kernel)
                .map_err(rows_err)?
        },
        resolver @ (MetricResolver::Antimeridian
        | MetricResolver::Metric { .. }
        | MetricResolver::Metric3d { .. }
        | MetricResolver::GeographicCarry) => {
            let mut metric_scratch = MetricScratch::default();
            let result = match resolver.resolve_topology_ctx(&frame, &mut metric_scratch) {
                Ok((domain, metric)) => {
                    execute_unary_shape_rows(py, array.clone(), domain, metric, mode, kernel)
                },
                Err(error) => {
                    drop(metric_scratch);
                    return Err(error);
                },
            };
            drop(metric_scratch);
            result.map_err(rows_err)?
        },
    };
    rows.finish(frame).map_err(PyErr::from)
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
    match resolver {
        MetricResolver::None => {
            let domain = crate::geometry::topology_domain(&array.frame)?;
            let values =
                execute_unary_rows(py, array.clone(), domain, MetricCtx::None, mode, kernel)
                    .map_err(rows_err)?;
            Ok((values, array.frame.clone()))
        },
        resolver @ (MetricResolver::Antimeridian
        | MetricResolver::Metric { .. }
        | MetricResolver::Metric3d { .. }
        | MetricResolver::GeographicCarry) => {
            let mut metric_scratch = MetricScratch::default();
            let result = match resolver.resolve_topology_ctx(&array.frame, &mut metric_scratch) {
                Ok((domain, metric)) => {
                    execute_unary_rows(py, array.clone(), domain, metric, mode, kernel)
                },
                Err(error) => {
                    drop(metric_scratch);
                    return Err(error);
                },
            };
            drop(metric_scratch);
            let values = result.map_err(rows_err)?;
            Ok((values, array.frame.clone()))
        },
    }
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

/// Array geometry method returning a `GeometryArray` — natively, without the
/// Python-object round-trip (`Py<PyAny>` -> downcast -> borrow -> clone) the
/// generic lane would pay.
///
/// Geometry-returning unaries: prefer pure-`Shape` fallback (no prepared
/// persistence). Distance-basis linref still acquires a frame cache via
/// [`unary_row_mode`].
pub(crate) fn unary_array_shapes<R: Into<GeometryOutput>>(
    py: Python<'_>,
    array: &PyGeometryArray,
    op: Operation,
    unit: Option<DistanceUnit>,
    packed: Option<&PackedUnary>,
    kernel: impl Fn(&PreparedPlan, &CachelessRowContext<'_>) -> Result<R> + Send + Sync,
) -> PyResult<PyGeometryArray> {
    let resolver = op.resolver_with_unit(unit);
    if let Some(result) = try_unary_packed_array(py, array, op, resolver, packed) {
        return result;
    }
    run_unary_shape_rows(
        py,
        array,
        resolver,
        unary_row_mode(op),
        move |data, ctx, _rows| kernel(data, ctx),
    )
}

fn execute_scalar_linref<R: Send>(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    resolver: super::metric::MetricResolver,
    kernel: impl FnOnce(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<R> + Send,
) -> PyResult<R> {
    let frame = &geometry.frame;
    let mut scratch = MetricScratch::default();
    let result = {
        let metric = match resolver.resolve_ctx(frame, &mut scratch) {
            Ok(metric) => metric,
            Err(error) => {
                drop(scratch);
                return Err(error);
            },
        };
        let resolved_metric = match metric.require_resolved(
            op.metric_op()
                .expect("linear-reference operation must carry metric identity"),
        ) {
            Ok(metric) => metric,
            Err(error) => {
                drop(scratch);
                return Err(PyErr::from(error));
            },
        };
        let row = LinearReferenceRow {
            lane: Lane::Scalar,
            metric: resolved_metric,
            cache: &geometry.frame_cache(),
        };
        let plan = geometry.prepared_plan();
        py.detach(move || kernel(&plan, &row))
    };
    drop(scratch);
    Ok(result?)
}

pub(crate) fn unary_scalar_linref_shape(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    resolver: super::metric::MetricResolver,
    kernel: impl FnOnce(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<Shape> + Send,
) -> PyResult<Typed> {
    let frame = geometry.frame.clone();
    let shape = execute_scalar_linref(py, geometry, op, resolver, kernel)?;
    Ok(Typed(PyGeometry::with_frame(shape, frame)))
}

/// Evaluate several stations on one scalar geometry while sharing its
/// prepared plan and metric context. Each station remains on the scalar lane,
/// so invalid M support keeps the same error behavior as one scalar call.
pub(crate) fn unary_scalar_linref_column(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    resolver: super::metric::MetricResolver,
    stations: Vec<f64>,
    kernel: impl Fn(&PreparedPlan, &LinearReferenceRow<'_>, f64) -> Result<Shape> + Send,
) -> PyResult<PyGeometryArray> {
    let frame = geometry.frame.clone();
    let shapes = execute_scalar_linref(py, geometry, op, resolver, move |plan, row| {
        stations
            .into_iter()
            .map(|station| kernel(plan, row, station))
            .collect::<Result<Vec<_>>>()
    })?;
    Ok(PyGeometryArray::from_shapes(shapes, frame)?)
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
    unary_array_linref_with_model(py, array, model, kernel)
}

pub(crate) fn unary_array_linref_with_model(
    py: Python<'_>,
    array: &PyGeometryArray,
    model: &crate::crs::MetricModel,
    kernel: impl Fn(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<Shape> + Send + Sync,
) -> PyResult<PyGeometryArray> {
    let frame = array.frame.clone();
    let array = array.clone();
    let result = py.detach(move || {
        let mut rows = array.row_operation();
        let mut output = StreamingShapes::with_capacity(0, array.len());
        let mut resolved = None;
        for row in rows.rows_with_missing(Shape::empty_point) {
            let (logical, result) = match row {
                ArrayRow::Missing { logical, .. } => (logical, output.try_push_missing()),
                ArrayRow::Present(present) => {
                    let row_index = present.logical();
                    let shape = present.row();
                    let result = crate::array::PreparedRow::transient(shape)
                        .and_then(|data| {
                            let cache = rows.frame_cache(row_index);
                            if let crate::crs::MetricModel::Geodesic { frame, .. } = model {
                                crate::geometry::GeographicPrepared::try_new(
                                    &data, &cache, *frame,
                                )?;
                            }
                            if resolved.is_none() {
                                resolved = Some(crate::crs::ResolvedMetric::from_model(model)?);
                            }
                            let row = LinearReferenceRow {
                                lane: Lane::Array(row_index),
                                metric: resolved
                                    .as_ref()
                                    .expect("a present linear-reference row resolves its metric"),
                                cache: &cache,
                            };
                            kernel(&data, &row)
                        })
                        .and_then(|shape| output.try_push(shape));
                    (row_index, result)
                },
            };
            result.map_err(|error| (logical, error))?;
        }
        Ok(output)
    });
    let output = result.map_err(rows_err)?;
    output.finish(frame).map_err(PyErr::from)
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
    let mut metric_scratch =
        (!matches!(resolver, MetricResolver::None)).then(MetricScratch::default);
    let context = metric_scratch.as_mut().map_or_else(
        || {
            crate::geometry::topology_domain(&frame)
                .map(|domain| (domain, MetricCtx::None))
                .map_err(PyErr::from)
        },
        |scratch| resolver.resolve_topology_ctx(&frame, scratch),
    );
    let (domain, metric) = match context {
        Ok(context) => context,
        Err(error) => {
            drop(metric_scratch);
            return Err(error);
        },
    };
    let array = array.clone();
    let result = {
        py.detach(move || {
            let mut rows = array.row_operation();
            let mut output = StreamingShapes::with_capacity(0, array.len());
            for row in rows.rows_with_missing(Shape::empty_point) {
                let (logical, result) = match row {
                    ArrayRow::Missing { logical, .. } => (logical, output.try_push_missing()),
                    ArrayRow::Present(present) => {
                        let row = present.logical();
                        let shape_row = present.row();
                        let ctx = CachelessRowContext {
                            frame: &frame,
                            domain,
                            metric,
                            lane: Lane::Array(row),
                        };
                        let result = match rows.prepared_at(row, shape_row) {
                            Ok(data) => {
                                let mut budget =
                                    crate::geometry::ExpansionBudget::new(op.name(), parameter);
                                kernel(&data, &ctx, &mut budget)
                                    .and_then(|shape| output.try_push(shape))
                            },
                            Err(error) => Err(error),
                        };
                        (row, result)
                    },
                };
                result.map_err(|error| (logical, error))?;
            }
            Ok(output)
        })
    };
    drop(metric_scratch);
    let output = result.map_err(rows_err)?;
    output.finish(output_frame).map_err(PyErr::from)
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

#[cfg(test)]
mod shape_streaming_tests {
    use super::*;
    use crate::array::LogicalRow;
    use crate::geometry::storage::{GeometryArrayStorage, PackedStorage, ValidityMask};

    fn nullable_measured_lines() -> PyGeometryArray {
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
            ValidityMask::from_vec(3, vec![false, true, false]).expect("mask"),
        )
        .expect("nullable lines admit")
    }

    #[test]
    fn unary_shape_stream_keeps_nullable_measured_lines_packed() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let array = nullable_measured_lines();
            let output =
                unary_array_shapes(py, &array, Operation::Translate, None, None, |plan, _| {
                    Ok(plan.shape().clone())
                })
                .expect("unary shape stream");

            let GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords, offsets, ..
            }) = output.storage()
            else {
                panic!("homogeneous nullable lines stay packed");
            };
            assert_eq!(coords.ms(), Some(&[10.0, 20.0, 30.0, 40.0, 50.0, 60.0][..]));
            assert_eq!(offsets.as_slice(), &[0, 3, 3, 6]);
            assert!(output.is_row_missing(1));
        });
    }

    #[test]
    fn unary_linref_stream_keeps_nullable_measured_lines_packed() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let array = nullable_measured_lines();
            let output = unary_array_linref_with_model(
                py,
                &array,
                &crate::crs::MetricModel::COORDINATE,
                |plan, _| Ok(plan.shape().clone()),
            )
            .expect("linear-reference shape stream");

            let GeometryArrayStorage::Packed(PackedStorage::Lines {
                coords, offsets, ..
            }) = output.storage()
            else {
                panic!("homogeneous nullable linear-reference lines stay packed");
            };
            assert_eq!(coords.ms(), Some(&[10.0, 20.0, 30.0, 40.0, 50.0, 60.0][..]));
            assert_eq!(offsets.as_slice(), &[0, 3, 3, 6]);
            assert!(output.is_row_missing(1));
        });
    }

    #[test]
    fn budgeted_shape_stream_keeps_nullable_points_packed() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let present = PyGeometryArray::from_shapes(
                vec![
                    Shape::Point(crate::geometry::Point::new_raw_xy(1.0, 2.0)),
                    Shape::Point(crate::geometry::Point::new_raw_xy(3.0, 4.0)),
                ],
                Frame::None,
            )
            .expect("homogeneous points pack");
            let array = PyGeometryArray::scatter_present_rows(
                &present,
                ValidityMask::from_vec(3, vec![false, true, false]).expect("mask"),
            )
            .expect("nullable points admit");
            let output = unary_array_shapes_budgeted(
                py,
                &array,
                Operation::Translate,
                None,
                "test expansion",
                |plan, _, _| Ok(plan.shape().clone()),
            )
            .expect("budgeted shape stream");

            let GeometryArrayStorage::Packed(PackedStorage::Points { coords, .. }) =
                output.storage()
            else {
                panic!("homogeneous nullable points stay packed");
            };
            assert_eq!(coords.xs(), [1.0, 3.0]);
            assert_eq!(coords.ys(), [2.0, 4.0]);
            assert!(output.is_row_missing(1));
        });
    }

    #[test]
    fn unary_shape_stream_preserves_present_typed_empty_results() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let present = PyGeometryArray::from_shapes(
                vec![
                    Shape::Point(crate::geometry::Point::new_raw_xy(1.0, 2.0)),
                    Shape::Point(crate::geometry::Point::new_raw_xy(3.0, 4.0)),
                ],
                Frame::None,
            )
            .expect("homogeneous points pack");
            let array = PyGeometryArray::scatter_present_rows(
                &present,
                ValidityMask::from_vec(3, vec![false, true, false]).expect("mask"),
            )
            .expect("nullable points admit");
            let empty = Shape::typed_empty(
                crate::geometry::GeometryKind::Polygon,
                crate::geometry::CoordinateAxes::XY,
            );
            let output =
                unary_array_shapes(py, &array, Operation::Translate, None, None, |_, _| {
                    Ok(empty.clone())
                })
                .expect("typed-empty shape stream");

            assert!(matches!(output.storage(), GeometryArrayStorage::Mixed(_)));
            let Some(LogicalRow::Present { row, .. }) = output.logical_row(0) else {
                panic!("present empty polygon remains present");
            };
            assert_eq!(row.with_shape(Clone::clone).unwrap(), empty);
            assert!(matches!(
                output.logical_row(1),
                Some(LogicalRow::Missing { .. })
            ));
        });
    }

    #[test]
    fn unary_shape_stream_reports_first_error_with_logical_row() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let present = PyGeometryArray::from_shapes(
                vec![
                    Shape::Point(crate::geometry::Point::new_raw_xy(1.0, 2.0)),
                    crate::io::parse_wkt("LINESTRING (0 0, 1 1)").expect("first line"),
                    crate::io::parse_wkt("LINESTRING (2 2, 3 3)").expect("second line"),
                ],
                Frame::None,
            )
            .expect("mixed geometries admit");
            let array = PyGeometryArray::scatter_present_rows(
                &present,
                ValidityMask::from_vec(4, vec![false, true, false, false]).expect("mask"),
            )
            .expect("nullable mixed geometries admit");
            let error =
                unary_array_shapes(py, &array, Operation::Translate, None, None, |plan, _| {
                    match plan.shape() {
                        Shape::LineString(_) => Err(crate::geometry::GeometryErrorKind::message(
                            "row kernel failure",
                        )),
                        Shape::Point(_)
                        | Shape::MultiPoint(_)
                        | Shape::MultiLineString(_)
                        | Shape::Polygon(_)
                        | Shape::MultiPolygon(_)
                        | Shape::GeometryCollection(_)
                        | Shape::Empty(..) => Ok(plan.shape().clone()),
                    }
                })
                .expect_err("the first row error is propagated");
            let notes = error
                .value(py)
                .getattr("__notes__")
                .expect("row error note")
                .extract::<Vec<String>>()
                .expect("row error notes are strings");

            assert_eq!(notes, ["while processing array element 2"]);
        });
    }
}
