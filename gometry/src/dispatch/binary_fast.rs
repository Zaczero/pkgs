#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::broadcast::{
    GeometryInput, array_crs_distances, array_crs_dwithin, classify_input,
    predicate_pairwise_arrays, predicate_scalar_vs_array,
};
use crate::crs::{MetricModel, ResolvedMetric};
use crate::dispatch::binary::{
    BinaryArrayFastPath, NoBinaryFastPath, dispatch_binary, dispatch_binary_geometry,
};
use crate::dispatch::kernels;
use crate::dispatch::metric::OpCtx;
use crate::dispatch::operation::Operation;
use crate::geometry::{ShapeData, is_geographic_frame};
use crate::predicates::engine::{Predicate, topology_scalar_pair};
use crate::{DistanceUnit, NonNegative, OverlayOp};

/// Predicate array-lane fast paths (point SIMD, bounds gates, prepared scans).
pub(crate) struct PredicateFastPath {
    predicate: Predicate,
    scalar_is_left: bool,
}

impl PredicateFastPath {
    fn new(predicate: Predicate, left: &Bound<'_, PyAny>) -> Self {
        Self {
            predicate,
            scalar_is_left: matches!(classify_input(left), Some(GeometryInput::One(_))),
        }
    }
}

impl BinaryArrayFastPath<bool> for PredicateFastPath {
    fn try_dispatch(
        &self,
        py: Python<'_>,
        array: &crate::PyGeometryArray,
        other: GeometryInput<'_>,
        _op_name: &str,
        _frame: &crate::boundary::metadata::Frame,
        _model: Option<&MetricModel>,
        _metric: Option<&ResolvedMetric>,
    ) -> Option<PyResult<Py<PyAny>>> {
        let spec = self.predicate.spec();
        Some(match other {
            GeometryInput::One(scalar) => {
                predicate_scalar_vs_array(py, &spec, scalar, array, self.scalar_is_left)
            },
            GeometryInput::Many(right) => predicate_pairwise_arrays(py, &spec, array, right),
        })
    }
}

/// Overlay set-ops use the disjoint-shortcut path in [`super::binary::geometry_kernel_over_array`].
pub(crate) struct OverlayFastPath;

impl BinaryArrayFastPath<crate::geometry::Shape> for OverlayFastPath {
    fn try_dispatch(
        &self,
        _py: Python<'_>,
        _left: &crate::PyGeometryArray,
        _right: GeometryInput<'_>,
        _op_name: &str,
        _frame: &crate::boundary::metadata::Frame,
        _model: Option<&MetricModel>,
        _metric: Option<&ResolvedMetric>,
    ) -> Option<PyResult<Py<PyAny>>> {
        None
    }
}

/// Predicate broadcast through the spine (fast paths in [`PredicateFastPath`]).
pub(crate) fn dispatch_predicate(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    predicate: Predicate,
) -> PyResult<Py<PyAny>> {
    let spec = predicate.spec();
    let operation = Operation::Predicate(predicate);
    dispatch_binary(
        py,
        left,
        right,
        operation.name(),
        super::MetricResolver::None,
        &PredicateFastPath::new(predicate, left),
        move |left, right, ctx| {
            debug_assert!(
                is_geographic_frame(ctx.frame) == ctx.geographic,
                "geographic flag matches the resolved frame"
            );
            Ok(topology_scalar_pair(&spec, left, right, ctx.geographic))
        },
    )
}

/// Geometry overlay through the spine (disjoint shortcut in ``geometry_kernel_over_array``).
pub(crate) fn dispatch_overlay(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op: OverlayOp,
) -> PyResult<Py<PyAny>> {
    let strictness = crate::geometry::Strictness::Lenient;
    let operation = Operation::from_overlay(op);
    dispatch_binary_geometry(
        py,
        left,
        right,
        operation.name(),
        &OverlayFastPath,
        move |left, right, ctx| kernels::binary_overlay(left, right, ctx, op, strictness),
    )
}

/// Value-returning ``broadcast2`` shim onto the spine.
pub(crate) fn dispatch_broadcast2<R>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op_name: &str,
    op: impl Fn(&crate::geometry::Shape, &crate::geometry::Shape) -> crate::error::Result<R>
    + Send
    + Sync,
) -> PyResult<Py<PyAny>>
where
    R: Send + crate::broadcast::BulkElement,
{
    dispatch_binary(
        py,
        left,
        right,
        op_name,
        super::MetricResolver::None,
        &NoBinaryFastPath,
        move |left, right, _ctx| op(left.shape(), right.shape()),
    )
}

fn dispatch_metric_binary<R>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op: Operation,
    unit: Option<DistanceUnit>,
    scalar_kernel: impl Fn(&ShapeData, &ShapeData, &OpCtx<'_>) -> crate::error::Result<R>
    + Copy
    + Send
    + Sync,
    array_lane: impl Fn(
        Python<'_>,
        &crate::PyGeometryArray,
        &Bound<'_, PyAny>,
        &'static str,
        Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>>
where
    R: Send + crate::broadcast::BulkElement,
{
    let op_name = op.name();
    let left_in = classify_input(left);
    let right_in = classify_input(right);
    let has_missing = matches!(&left_in, Some(GeometryInput::Many(array)) if array.has_missing())
        || matches!(&right_in, Some(GeometryInput::Many(array)) if array.has_missing());
    if has_missing
        || matches!(
            (&left_in, &right_in),
            (Some(GeometryInput::One(_)), Some(GeometryInput::One(_)))
        )
    {
        return dispatch_binary(
            py,
            left,
            right,
            op_name,
            op.resolver_with_unit(unit),
            &NoBinaryFastPath,
            scalar_kernel,
        );
    }
    match (left_in, right_in) {
        (Some(GeometryInput::Many(array)), _) => array_lane(py, array, right, op_name, unit),
        (_, Some(GeometryInput::Many(array))) => array_lane(py, array, left, op_name, unit),
        (None, _) => Err(crate::broadcast::expected_geometry_or_array_for(left)),
        (_, None) => Err(crate::broadcast::expected_geometry_or_array_for(right)),
        (Some(GeometryInput::One(_)), Some(GeometryInput::One(_))) => unreachable!(),
    }
}

/// Distance free-function dispatch through the spine.
pub(crate) fn dispatch_distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    dispatch_metric_binary(
        py,
        left,
        right,
        Operation::Distance,
        unit,
        kernels::binary_distance,
        array_crs_distances,
    )
}

/// ``dwithin`` free-function dispatch through the spine.
pub(crate) fn dispatch_dwithin(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    distance: f64,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let distance = NonNegative::try_new("distance", distance)?;
    dispatch_metric_binary(
        py,
        left,
        right,
        Operation::Dwithin,
        unit,
        move |left, right, ctx| kernels::binary_dwithin(left, right, ctx, distance.get()),
        move |py, array, other, op_name, unit| {
            array_crs_dwithin(py, array, other, distance.get(), op_name, unit)
        },
    )
}

/// ``distance_3d`` free-function dispatch through the spine.
pub(crate) fn dispatch_distance_3d(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let op = Operation::Distance3d;
    dispatch_binary(
        py,
        left,
        right,
        op.name(),
        op.resolver().with_unit(unit),
        &NoBinaryFastPath,
        kernels::binary_distance_3d,
    )
}

/// Hausdorff-distance free-function dispatch through the spine.
pub(crate) fn dispatch_hausdorff_distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    dispatch_similarity_metric(
        py,
        left,
        right,
        densify,
        unit,
        Operation::HausdorffDistance,
        kernels::binary_hausdorff_distance,
        crate::metric_hausdorff_densified,
        |left_xs, left_ys, right_xs, right_ys| {
            Ok(crate::geometry::hausdorff_distance_line_columns(
                left_xs, left_ys, right_xs, right_ys,
            ))
        },
    )
}

/// Fréchet-distance free-function dispatch through the spine.
pub(crate) fn dispatch_frechet_distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    dispatch_similarity_metric(
        py,
        left,
        right,
        densify,
        unit,
        Operation::FrechetDistance,
        kernels::binary_frechet_distance,
        crate::metric_frechet_densified,
        |left_xs, left_ys, right_xs, right_ys| {
            Ok(crate::geometry::frechet_distance_line_columns(
                left_xs, left_ys, right_xs, right_ys,
            ))
        },
    )
}

fn dispatch_similarity_metric<ScalarKernel, PerDensifyKernel, PackedLineKernel>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
    op: Operation,
    scalar_kernel: ScalarKernel,
    per_densify_kernel: PerDensifyKernel,
    packed_line_kernel: PackedLineKernel,
) -> PyResult<Py<PyAny>>
where
    ScalarKernel: Fn(&ShapeData, &ShapeData, &OpCtx<'_>, Option<f64>) -> crate::error::Result<f64>
        + Copy
        + Send
        + Sync,
    PerDensifyKernel: Fn(
            &MetricModel,
            &crate::geometry::Shape,
            &crate::geometry::Shape,
            Option<f64>,
        ) -> crate::error::Result<f64>
        + Copy
        + Send
        + Sync,
    PackedLineKernel:
        Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<f64> + Copy + Send + Sync,
{
    let left_array = crate::broadcast::exact_geometry_array(left);
    let right_array = crate::broadcast::exact_geometry_array(right);
    let Some((len, side)) = similarity_array_side(left_array, right_array) else {
        let densify = parse_scalar_densify(densify)?;
        return dispatch_binary(
            py,
            left,
            right,
            op.name(),
            op.resolver_with_unit(unit),
            &NoBinaryFastPath,
            move |left, right, ctx| scalar_kernel(left, right, ctx, densify),
        );
    };
    let densify = crate::OptionalDensifyParam::parse(densify, len)?;
    dispatch_similarity_array_lane(
        py,
        side,
        left,
        right,
        &densify,
        unit,
        op.name(),
        per_densify_kernel,
        move |py, array, other, densify, unit| {
            similarity_array_lane(
                py,
                array,
                other,
                densify,
                unit,
                op,
                densify.is_none().then_some(packed_line_kernel),
                per_densify_kernel,
            )
        },
    )
}

fn parse_scalar_densify(densify: Option<&Bound<'_, PyAny>>) -> PyResult<Option<f64>> {
    let Some(value) = densify else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    let param = crate::F64Param::parse(value, "densify", 1)?;
    crate::validate_densify(Some(param.get(0)))
}

#[derive(Clone, Copy)]
enum SimilarityArraySide<'a> {
    Left(&'a crate::PyGeometryArray),
    Right(&'a crate::PyGeometryArray),
    Both(&'a crate::PyGeometryArray, &'a crate::PyGeometryArray),
}

fn similarity_array_side<'a>(
    left: Option<&'a crate::PyGeometryArray>,
    right: Option<&'a crate::PyGeometryArray>,
) -> Option<(usize, SimilarityArraySide<'a>)> {
    match (left, right) {
        (Some(left), Some(right)) => {
            Some((left.storage().len(), SimilarityArraySide::Both(left, right)))
        },
        (Some(left), None) => Some((left.storage().len(), SimilarityArraySide::Left(left))),
        (None, Some(right)) => Some((right.storage().len(), SimilarityArraySide::Right(right))),
        (None, None) => None,
    }
}

/// Shared array lane for the densify-aware similarity metrics (Hausdorff and
/// Fréchet). Both differ only by their operation name, the per-element
/// densified kernel, and the scalar lane that runs the packed column kernels —
/// everything else (per-element vs scalar branch, the array-operand flip when
/// the array is the right operand) is identical.
fn dispatch_similarity_array_lane(
    py: Python<'_>,
    side: SimilarityArraySide<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: &crate::OptionalDensifyParam,
    unit: Option<DistanceUnit>,
    op_name: &str,
    per_densify_kernel: impl Fn(
        &MetricModel,
        &crate::geometry::Shape,
        &crate::geometry::Shape,
        Option<f64>,
    ) -> crate::error::Result<f64>
    + Send
    + Sync,
    scalar_lane: impl Fn(
        Python<'_>,
        &crate::PyGeometryArray,
        &Bound<'_, PyAny>,
        Option<f64>,
        Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    if densify.is_per_element() {
        return match side {
            SimilarityArraySide::Left(array) => {
                crate::broadcast::array_crs_similarity_metric_per_densify(
                    py,
                    array,
                    right,
                    op_name,
                    unit,
                    densify,
                    per_densify_kernel,
                )
            },
            SimilarityArraySide::Both(array, other) => {
                let _ = other.storage().len();
                crate::broadcast::array_crs_similarity_metric_per_densify(
                    py,
                    array,
                    right,
                    op_name,
                    unit,
                    densify,
                    per_densify_kernel,
                )
            },
            SimilarityArraySide::Right(array) => {
                crate::broadcast::array_crs_similarity_metric_per_densify(
                    py,
                    array,
                    left,
                    op_name,
                    unit,
                    densify,
                    per_densify_kernel,
                )
            },
        };
    }
    let scalar_densify = densify.as_scalar_densify();
    match side {
        SimilarityArraySide::Left(array) => scalar_lane(py, array, right, scalar_densify, unit),
        SimilarityArraySide::Both(array, other) => {
            let _ = other.storage().len();
            scalar_lane(py, array, right, scalar_densify, unit)
        },
        SimilarityArraySide::Right(array) => scalar_lane(py, array, left, scalar_densify, unit),
    }
}

fn similarity_array_lane<P, K>(
    py: Python<'_>,
    array: &crate::PyGeometryArray,
    other: &Bound<'_, PyAny>,
    densify: Option<f64>,
    unit: Option<DistanceUnit>,
    op: Operation,
    packed_planar_lines: Option<P>,
    kernel: K,
) -> PyResult<Py<PyAny>>
where
    P: Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<f64> + Send + Sync,
    K: Fn(
            &MetricModel,
            &crate::geometry::Shape,
            &crate::geometry::Shape,
            Option<f64>,
        ) -> crate::error::Result<f64>
        + Send
        + Sync,
{
    crate::broadcast::array_crs_metric_float(
        py,
        array,
        other,
        op.name(),
        unit,
        packed_planar_lines,
        move |model, a, b| kernel(model, a, b, densify),
    )
}
