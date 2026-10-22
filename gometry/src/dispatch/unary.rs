use pyo3::prelude::*;

use crate::array::{ArrayRow, ArrayRowOperation, StreamingShapes};
use crate::broadcast::{
    BulkElement, CollectRows as _, GeometryInput, classify_input, expected_geometry_or_array,
    rows_err,
};
use crate::crs::frame::Frame;
use crate::dispatch::metric::{
    Lane, LinearReferenceRow, MetricCtx, MetricResolver, MetricRowContext, MetricScratch,
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
    geometry: &PyGeometry,
    plan: &PreparedPlan,
    domain: TopologyDomain,
    metric: MetricCtx<'_>,
    kernel: impl FnOnce(&PreparedPlan, &MetricRowContext<'_>) -> Result<R> + pyo3::marker::Ungil,
) -> Result<R> {
    let ctx = MetricRowContext {
        frame: &geometry.frame,
        domain,
        metric,
        lane: Lane::Scalar,
        frame_caches: None,
    };
    kernel(plan, &ctx)
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "only proven bounded predicate kernels execute attached; other operations keep their native boundary"
)]
fn scalar_unary_is_bounded(op: Operation, plan: &PreparedPlan, domain: TopologyDomain) -> bool {
    use Shape::{
        Empty, GeometryCollection, LineString, MultiLineString, MultiPoint, Point, Polygon,
    };
    let point_or_empty = matches!(plan.shape(), Point(_) | Empty(..));
    match op {
        Operation::IsEmpty => matches!(
            plan.shape(),
            Point(_) | Empty(..) | LineString(_) | MultiPoint(_)
        ),
        Operation::IsClosed => !matches!(plan.shape(), MultiLineString(_)),
        Operation::IsCcw => match plan.shape() {
            LineString(line) => line.len() < 4 || !plan.shape().is_closed(),
            _ => true,
        },
        Operation::IsConvex => !matches!(plan.shape(), Polygon(_)),
        Operation::IsRing => {
            point_or_empty
                || (!domain.is_geographic()
                    && (!matches!(plan.shape(), LineString(_)) || !plan.shape().is_closed()))
        },
        Operation::IsSimple => {
            point_or_empty
                || (!domain.is_geographic()
                    && (matches!(plan.shape(), GeometryCollection(_))
                        || plan.existing_simplicity().is_some()))
        },
        Operation::IsValid => {
            point_or_empty
                || if domain.is_geographic() {
                    plan.existing_structure_issue().is_some()
                } else {
                    plan.existing_validity().is_some()
                }
        },
        Operation::CrossesAntimeridian => {
            matches!(plan.shape(), Point(_) | Empty(..) | MultiPoint(_))
        },
        _ => false,
    }
}

/// Scalar geometry lane: resolve metric once, run one kernel, return the bulk element.
pub(crate) fn unary_scalar<R: BulkElement>(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    unit: Option<DistanceUnit>,
    kernel: impl FnOnce(&PreparedPlan, &MetricRowContext<'_>) -> Result<R> + pyo3::marker::Ungil,
) -> PyResult<R> {
    let resolver = op.resolver_with_unit(unit);
    let plan = geometry.prepared_plan();
    if matches!(resolver, MetricResolver::None) {
        let potentially_bounded = scalar_unary_is_bounded(op, &plan, TopologyDomain::Cartesian)
            || (op == Operation::IsValid && plan.existing_structure_issue().is_some());
        if potentially_bounded {
            let crs = geometry.crs_str();
            if crs.is_none_or(crate::crs::is_wgs84_lonlat) {
                let domain = crate::geometry::topology_domain(&geometry.frame)?;
                if scalar_unary_is_bounded(op, &plan, domain) {
                    return Ok(execute_unary_scalar(
                        geometry,
                        &plan,
                        domain,
                        MetricCtx::None,
                        kernel,
                    )?);
                }
                return Ok(py.detach(|| {
                    execute_unary_scalar(geometry, &plan, domain, MetricCtx::None, kernel)
                })?);
            }
            let state = crate::crs::runtime_state_snapshot()?;
            if crate::crs::thread_caches_are_current(&state)
                && let Some(info) = crate::crs::info_if_cached(crs.expect("tagged frame"), &state)?
            {
                let domain = info
                    .geographic_frame()
                    .map_or(TopologyDomain::Cartesian, TopologyDomain::Geographic);
                if scalar_unary_is_bounded(op, &plan, domain) {
                    return Ok(execute_unary_scalar(
                        geometry,
                        &plan,
                        domain,
                        MetricCtx::None,
                        kernel,
                    )?);
                }
                return Ok(py.detach(|| {
                    execute_unary_scalar(geometry, &plan, domain, MetricCtx::None, kernel)
                })?);
            }
            return Ok(py.detach(|| {
                let domain = crate::geometry::topology_domain_at(&geometry.frame, Some(&state))?;
                execute_unary_scalar(geometry, &plan, domain, MetricCtx::None, kernel)
            })?);
        }
    }
    let run = || {
        let mut scratch = MetricScratch::default();
        let (domain, metric) = resolver.resolve_topology_ctx(&geometry.frame, &mut scratch)?;
        execute_unary_scalar(geometry, &plan, domain, metric, kernel)
    };
    if matches!(resolver, MetricResolver::Antimeridian)
        && geometry.crs_str().is_none()
        && scalar_unary_is_bounded(op, &plan, TopologyDomain::Cartesian)
    {
        return Ok(run()?);
    }
    Ok(py.detach(run)?)
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
    kernel: impl Fn(&PreparedPlan, &MetricRowContext<'_>) -> Result<R> + pyo3::marker::Ungil,
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
            | MetricOp::SamplePoints
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
    kernel: impl Fn(&PreparedPlan, &MetricRowContext<'_>, &mut ArrayRowOperation<'_>) -> Result<T>
    + pyo3::marker::Ungil,
) -> std::result::Result<Vec<T>, (usize, crate::error::Error)> {
    py.detach(move || {
        let mut rows = array.row_operation();
        rows.rows_with_missing(T::missing_value)
            .map(|row| match row {
                ArrayRow::Missing { value, .. } => Ok(value),
                ArrayRow::Present(present) => {
                    let row_index = present.logical();
                    let shape_row = present.row();
                    let ctx = MetricRowContext {
                        frame: &array.frame,
                        domain,
                        metric,
                        lane: Lane::Array(row_index),
                        frame_caches: None,
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
    kernel: impl Fn(&PreparedPlan, &MetricRowContext<'_>, &mut ArrayRowOperation<'_>) -> Result<R>
    + pyo3::marker::Ungil,
) -> crate::error::BatchResult<PyGeometryArray> {
    py.detach(move || {
        let mut rows = array.row_operation();
        let mut output = PublicationRows::with_capacity(array.len());
        for row in rows.rows_with_missing(Shape::empty_point) {
            let (logical, result) = match row {
                ArrayRow::Missing { logical, .. } => (logical, output.try_push_missing()),
                ArrayRow::Present(present) => {
                    let row_index = present.logical();
                    let shape_row = present.row();
                    let ctx = MetricRowContext {
                        frame: &array.frame,
                        domain,
                        metric,
                        lane: Lane::Array(row_index),
                        frame_caches: None,
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
            result.map_err(|error| crate::error::BatchError::Row {
                row: logical,
                error,
            })?;
        }
        drop(rows);
        output
            .finish(array.frame.clone())
            .map_err(crate::error::BatchError::from)
    })
}

fn run_unary_shape_rows<R: Into<GeometryOutput>>(
    py: Python<'_>,
    array: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    mode: UnaryRowMode,
    kernel: impl Fn(&PreparedPlan, &MetricRowContext<'_>, &mut ArrayRowOperation<'_>) -> Result<R>
    + pyo3::marker::Ungil,
) -> PyResult<PyGeometryArray> {
    let frame = array.frame.clone();
    let output = match resolver {
        MetricResolver::None => {
            let domain = crate::geometry::topology_domain(&frame)?;
            execute_unary_shape_rows(py, array.clone(), domain, MetricCtx::None, mode, kernel)
                .map_err(PyErr::from)?
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
                    return Err(error.into());
                },
            };
            drop(metric_scratch);
            result.map_err(PyErr::from)?
        },
    };
    Ok(output)
}

/// The per-row unary lane shared by every array return type: resolve the
/// metric once, run the kernel over each row inside one ``py.detach``.
fn run_unary_rows<T: BulkElement>(
    py: Python<'_>,
    array: &PyGeometryArray,
    resolver: super::metric::MetricResolver,
    mode: UnaryRowMode,
    kernel: impl Fn(&PreparedPlan, &MetricRowContext<'_>, &mut ArrayRowOperation<'_>) -> Result<T>
    + pyo3::marker::Ungil,
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
                    return Err(error.into());
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
    kernel: impl FnOnce(&PreparedPlan, &MetricRowContext<'_>) -> Result<Shape> + pyo3::marker::Ungil,
) -> PyResult<Typed> {
    if op == Operation::SplitAntimeridian {
        return unary_scalar_split_antimeridian(py, geometry);
    }
    let frame = geometry.frame.clone();
    let shape = unary_scalar(py, geometry, op, unit, kernel)?;
    Ok(Typed(PyGeometry::with_frame(shape, frame)))
}

fn split_drawing(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<crate::geometry::SplitDrawing> {
    let frame = crate::geometry::antimeridian_frame(ctx.frame, ctx.domain, "split_antimeridian")?;
    if ctx.frame.crs_ref().is_none() {
        return data.shape().split_antimeridian_captured_in_crs84();
    }
    let metric_access = || {
        ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic antimeridian source ordinates require the operation's CRS metric",
            )
        })
    };
    data.shape()
        .split_antimeridian_captured_in_frame_with_metric(frame, Some(&metric_access))
}

fn unary_scalar_split_antimeridian(py: Python<'_>, geometry: &PyGeometry) -> PyResult<Typed> {
    let frame = geometry.frame.clone();
    let shape = unary_scalar(
        py,
        geometry,
        Operation::SplitAntimeridian,
        None,
        |data, ctx| {
            let drawing = split_drawing(data, ctx)?;
            let finished = drawing.finish_scalar(ctx.frame.clone())?;
            let (shape, _) = finished.into_scalar()?;
            Ok(shape)
        },
    )?;
    Ok(Typed(PyGeometry::with_frame(shape, frame)))
}

fn unary_array_split_antimeridian(
    py: Python<'_>,
    array: &PyGeometryArray,
) -> PyResult<PyGeometryArray> {
    let frame = array.frame.clone();
    let mut metric_scratch = MetricScratch::default();
    let (domain, metric) = Operation::SplitAntimeridian
        .resolver_with_unit(None)
        .resolve_topology_ctx(&frame, &mut metric_scratch)?;
    let array = array.clone();
    let output_frame = frame.clone();
    let finished = py
        .detach(
            move || -> std::result::Result<_, (usize, crate::error::Error)> {
                let mut publication = PublicationRows::with_producer_drawing_capacity(array.len());
                let rows = array.row_operation();
                for row in rows.rows_with_missing(Shape::empty_point) {
                    match row {
                        ArrayRow::Missing { logical, .. } => {
                            publication
                                .try_push_missing()
                                .map_err(|error| (logical, error))?;
                        },
                        ArrayRow::Present(present) => {
                            let logical = present.logical();
                            let data = crate::array::PreparedRow::transient(present.row())
                                .map_err(|error| (logical, error))?;
                            let ctx = MetricRowContext {
                                frame: &array.frame,
                                domain,
                                metric,
                                lane: Lane::Array(logical),
                                frame_caches: None,
                            };
                            let drawing =
                                split_drawing(&data, &ctx).map_err(|error| (logical, error))?;
                            drawing
                                .append_to(&mut publication, logical)
                                .map_err(|error| (logical, error))?;
                        },
                    }
                }
                publication
                    .finish_geometry(output_frame)
                    .map_err(|error| (0, error))
            },
        )
        .map_err(rows_err);
    drop(metric_scratch);
    PyGeometryArray::from_finished(finished?).map_err(PyErr::from)
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
    kernel: impl Fn(&PreparedPlan, &MetricRowContext<'_>) -> Result<R> + pyo3::marker::Ungil,
) -> PyResult<PyGeometryArray> {
    if op == Operation::SplitAntimeridian {
        return unary_array_split_antimeridian(py, array);
    }
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

fn execute_scalar_linref<R: pyo3::marker::Ungil>(
    py: Python<'_>,
    geometry: &PyGeometry,
    op: Operation,
    resolver: super::metric::MetricResolver,
    kernel: impl FnOnce(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<R> + pyo3::marker::Ungil,
) -> PyResult<R> {
    let frame = &geometry.frame;
    let mut scratch = MetricScratch::default();
    let result = {
        let metric = match resolver.resolve_ctx(frame, &mut scratch) {
            Ok(metric) => metric,
            Err(error) => {
                drop(scratch);
                return Err(error.into());
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
    kernel: impl FnOnce(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<Shape> + pyo3::marker::Ungil,
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
    kernel: impl Fn(&PreparedPlan, &LinearReferenceRow<'_>, f64) -> Result<Shape> + pyo3::marker::Ungil,
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
    kernel: impl Fn(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<Shape> + pyo3::marker::Ungil,
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
    kernel: impl Fn(&PreparedPlan, &LinearReferenceRow<'_>) -> Result<Shape> + pyo3::marker::Ungil,
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
        &MetricRowContext<'_>,
        &mut crate::geometry::ExpansionBudget,
    ) -> Result<Shape>
    + pyo3::marker::Ungil,
) -> PyResult<PyGeometryArray> {
    let resolver = op.resolver_with_unit(unit);
    let frame = array.frame.clone();
    let output_frame = frame.clone();
    let mut metric_scratch =
        (!matches!(resolver, MetricResolver::None)).then(MetricScratch::default);
    let context = metric_scratch.as_mut().map_or_else(
        || crate::geometry::topology_domain(&frame).map(|domain| (domain, MetricCtx::None)),
        |scratch| resolver.resolve_topology_ctx(&frame, scratch),
    );
    let (domain, metric) = match context {
        Ok(context) => context,
        Err(error) => {
            drop(metric_scratch);
            return Err(error.into());
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
                        let ctx = MetricRowContext {
                            frame: &frame,
                            domain,
                            metric,
                            lane: Lane::Array(row),
                            frame_caches: None,
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
    kernel: impl Fn(&PreparedPlan, &MetricRowContext<'_>) -> Result<R> + pyo3::marker::Ungil,
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
mod scalar_boundary_tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::crs::frame::crs_arc_static;
    use crate::dispatch::kernels;
    use crate::test_support::{boundary_probe, initialize_python, python_is_attached};

    type Kernel = fn(&PreparedPlan, &MetricRowContext<'_>) -> Result<bool>;

    fn make_geometry(wkt: &str, frame: Frame) -> PyGeometry {
        PyGeometry::with_frame(crate::io::parse_wkt(wkt).unwrap(), frame)
    }

    #[expect(
        clippy::panic_in_result_fn,
        reason = "the attachment assertion must fail the test even when the tested kernel returns an expected error"
    )]
    fn predicate(
        py: Python<'_>,
        geometry: &PyGeometry,
        op: Operation,
        kernel: Kernel,
        expected_attached: bool,
    ) -> PyResult<bool> {
        let attached = AtomicBool::new(!expected_attached);
        let result = unary_scalar(py, geometry, op, None, |plan, ctx| {
            attached.store(python_is_attached(), Ordering::Relaxed);
            kernel(plan, ctx)
        });
        assert_eq!(attached.load(Ordering::Relaxed), expected_attached);
        result
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn structural_and_endpoint_predicates_use_attached_kernels() {
        initialize_python();
        Python::attach(|py| {
            for (wkt, op, kernel, expected) in [
                (
                    "LINESTRING (0 0, 1 1, 0 0)",
                    Operation::IsClosed,
                    kernels::unary_is_closed as Kernel,
                    true,
                ),
                (
                    "GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1))",
                    Operation::IsCcw,
                    kernels::unary_is_ccw as Kernel,
                    false,
                ),
                (
                    "MULTIPOINT ((1 2), (3 4))",
                    Operation::IsConvex,
                    kernels::unary_is_convex as Kernel,
                    false,
                ),
                (
                    "LINESTRING (0 0, 1 1)",
                    Operation::IsRing,
                    kernels::unary_is_ring as Kernel,
                    false,
                ),
                (
                    "POINT (1 2)",
                    Operation::IsValid,
                    kernels::unary_is_valid as Kernel,
                    true,
                ),
            ] {
                let geometry = make_geometry(wkt, Frame::None);
                assert_eq!(
                    predicate(py, &geometry, op, kernel, true).unwrap(),
                    expected,
                    "{wkt}"
                );
            }
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn cartesian_validity_and_simplicity_detach_cold_but_not_cached_kernels() {
        initialize_python();
        Python::attach(|py| {
            for (wkt, op, kernel, expected) in [
                (
                    "POLYGON ((0 0, 2 0, 2 2, 0 0))",
                    Operation::IsValid,
                    kernels::unary_is_valid as Kernel,
                    true,
                ),
                (
                    "POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))",
                    Operation::IsValid,
                    kernels::unary_is_valid as Kernel,
                    false,
                ),
                (
                    "LINESTRING (0 0, 2 2, 0 2, 2 0)",
                    Operation::IsSimple,
                    kernels::unary_is_simple as Kernel,
                    false,
                ),
            ] {
                let geometry = make_geometry(wkt, Frame::None);
                let plan = geometry.prepared_plan();
                assert_eq!(plan.existing_validity(), None);
                assert_eq!(plan.existing_simplicity(), None);
                assert_eq!(
                    predicate(py, &geometry, op, kernel, false).unwrap(),
                    expected
                );
                assert_eq!(
                    predicate(py, &geometry, op, kernel, true).unwrap(),
                    expected
                );
            }
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn geographic_admission_remains_detached_with_warm_topology() {
        initialize_python();
        Python::attach(|py| {
            let geometry = make_geometry(
                "LINESTRING (180 10, 170 20, 175 30, -180 10)",
                Frame::Crs(crs_arc_static("EPSG:4326")),
            );
            for _ in 0..2 {
                assert!(
                    predicate(
                        py,
                        &geometry,
                        Operation::IsRing,
                        kernels::unary_is_ring,
                        false
                    )
                    .unwrap()
                );
                assert!(
                    predicate(
                        py,
                        &geometry,
                        Operation::IsSimple,
                        kernels::unary_is_simple,
                        false
                    )
                    .unwrap()
                );
                assert!(
                    predicate(
                        py,
                        &geometry,
                        Operation::IsValid,
                        kernels::unary_is_valid,
                        false
                    )
                    .unwrap()
                );
            }
            let invalid = make_geometry(
                "LINESTRING (0 91, 1 92)",
                Frame::Crs(crs_arc_static("EPSG:4326")),
            );
            predicate(
                py,
                &invalid,
                Operation::IsSimple,
                kernels::unary_is_simple,
                false,
            )
            .unwrap_err();
            predicate(
                py,
                &invalid,
                Operation::IsRing,
                kernels::unary_is_ring,
                false,
            )
            .unwrap_err();
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn projected_metadata_miss_and_generation_change_share_the_native_boundary() {
        initialize_python();
        Python::attach(|py| {
            crate::crs::reset_runtime_config().unwrap();
            let geometry = make_geometry("POINT (1 2)", Frame::Crs(crs_arc_static("EPSG:3857")));
            for _ in 0..2 {
                let (result, events) = boundary_probe::capture(|| {
                    predicate(
                        py,
                        &geometry,
                        Operation::IsValid,
                        kernels::unary_is_valid,
                        false,
                    )
                });
                assert!(result.unwrap());
                assert_eq!(events, [("topology domain", false)]);
                let (result, events) = boundary_probe::capture(|| {
                    predicate(
                        py,
                        &geometry,
                        Operation::IsValid,
                        kernels::unary_is_valid,
                        true,
                    )
                });
                assert!(result.unwrap());
                assert!(
                    events.is_empty(),
                    "cache hit must not repeat domain resolution: {events:?}"
                );
                crate::crs::reset_runtime_config().unwrap();
            }
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn invalid_context_precedes_even_a_constant_predicate_kernel() {
        initialize_python();
        Python::attach(|py| {
            let geometry = make_geometry("POINT (1 2)", Frame::Crs("EPSG:not-a-code".into()));
            let called = AtomicBool::new(false);
            let result = unary_scalar(py, &geometry, Operation::IsCcw, None, |plan, ctx| {
                called.store(true, Ordering::Relaxed);
                kernels::unary_is_ccw(plan, ctx)
            });
            result.unwrap_err();
            assert!(!called.load(Ordering::Relaxed));
        });
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
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
