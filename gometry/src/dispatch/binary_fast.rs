#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::broadcast::{
    FrechetMode, GeometryInput, HausdorffMode, SimilarityMode, SimilarityOp, array_crs_distances,
    array_crs_dwithin, array_crs_metric_float, classify_input, predicate_pairwise_arrays,
    predicate_scalar_vs_array,
};
use crate::crs::{MetricModel, ResolvedMetric};
use crate::dispatch::binary::{
    BinaryArrayFastPath, NoBinaryFastPath, dispatch_binary, dispatch_binary_geometry,
};
use crate::dispatch::kernels;
use crate::dispatch::metric::CachelessRowContext;
use crate::dispatch::operation::{MetricOp, Operation};
use crate::geometry::{GeometryKind, PreparedPlan};
use crate::predicates::engine::Predicate;
use crate::{DistanceUnit, NonNegative, OverlayOp};

/// Predicate array-lane fast path using the shared batch engine.
pub(crate) struct PredicateFastPath {
    predicate: Predicate,
    scalar_is_left: bool,
}

impl PredicateFastPath {
    fn new(predicate: Predicate, left: &Bound<'_, PyAny>) -> Self {
        let scalar_is_left = matches!(classify_input(left), Some(GeometryInput::One(_)));
        Self {
            predicate,
            scalar_is_left,
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

/// Overlay set-ops use the shared geometry kernel over array rows.
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
    let operation = Operation::Predicate(predicate);
    if let (Some(GeometryInput::One(left)), Some(GeometryInput::One(right))) =
        (classify_input(left), classify_input(right))
    {
        left.frame.compatible(&right.frame, operation.name())?;
        let left_plan = left.plan_or_transient()?;
        let right_plan = right.plan_or_transient()?;
        let left_cache = left.frame_cache()?;
        let right_cache = right.frame_cache()?;
        let domain = crate::geometry::topology_domain(&left.frame)?;
        let spec = predicate.spec();
        let value = py.detach(move || {
            if let Some(verdict) = spec.predicate.kind_gate(
                GeometryKind::of(left_plan.shape()),
                GeometryKind::of(right_plan.shape()),
            ) {
                return Ok(verdict);
            }
            crate::predicates::engine::PreparedTopologyPair::try_for_domain(
                &left_plan,
                &left_cache,
                &right_plan,
                &right_cache,
                domain,
            )
            .and_then(|operands| crate::predicates::engine::topology_scalar_pair(&spec, operands))
        })?;
        return Ok(crate::broadcast::py_bool(py, value));
    }
    dispatch_binary(
        py,
        left,
        right,
        operation.name(),
        super::MetricResolver::None,
        &PredicateFastPath::new(predicate, left),
        |_left, _right, _ctx| unreachable!("predicate array fast path handles every array lane"),
    )
}

/// Geometry overlay through the shared geometry kernel.
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
    op: impl Fn(&PreparedPlan, &PreparedPlan) -> crate::error::Result<R> + Send + Sync,
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
        move |left, right, _ctx| op(left, right),
    )
}

fn dispatch_metric_binary<R>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    op: MetricOp,
    unit: Option<DistanceUnit>,
    planar_kernel: impl Fn(f64, &PreparedPlan, &PreparedPlan) -> crate::error::Result<R>
    + Copy
    + Send
    + Sync,
    geodesic_kernel: impl Fn(
        &PreparedPlan,
        &PreparedPlan,
        &crate::dispatch::metric::GeodesicBinaryRow<'_>,
    ) -> crate::error::Result<R>
    + Copy
    + Send
    + Sync,
    array_lane: impl Fn(
        Python<'_>,
        &crate::PyGeometryArray,
        &Bound<'_, PyAny>,
        MetricOp,
        Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>>
where
    R: Send + crate::broadcast::BulkElement,
{
    let operation = Operation::Metric(op);
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
        return super::dispatch_typed_metric_binary(
            py,
            left,
            right,
            op,
            operation.resolver_with_unit(unit),
            planar_kernel,
            geodesic_kernel,
        );
    }
    match (left_in, right_in) {
        (Some(GeometryInput::Many(array)), _) => array_lane(py, array, right, op, unit),
        (_, Some(GeometryInput::Many(array))) => array_lane(py, array, left, op, unit),
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
        MetricOp::Distance,
        unit,
        crate::broadcast::pair_distance_planar,
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
        MetricOp::Dwithin,
        unit,
        move |scale, left, right| {
            crate::broadcast::pair_dwithin_planar(scale, left, right, distance.get())
        },
        move |left, right, ctx| kernels::binary_dwithin(left, right, ctx, distance.get()),
        move |py, array, other, op, unit| {
            array_crs_dwithin(py, array, other, distance.get(), op, unit)
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
    let op = Operation::Metric(MetricOp::Distance3d);
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
    dispatch_similarity_metric::<HausdorffMode, _, _, _>(
        py,
        left,
        right,
        densify,
        unit,
        SimilarityOp::Hausdorff { densify: None },
        kernels::binary_hausdorff_distance,
        crate::metric_hausdorff_densified,
        crate::geometry::hausdorff_distance_line_columns,
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
    dispatch_similarity_metric::<FrechetMode, _, _, _>(
        py,
        left,
        right,
        densify,
        unit,
        SimilarityOp::Frechet { densify: None },
        kernels::binary_frechet_distance,
        crate::metric_frechet_densified,
        |left_xs, left_ys, right_xs, right_ys| {
            Ok(crate::geometry::frechet_distance_line_columns(
                left_xs, left_ys, right_xs, right_ys,
            ))
        },
    )
}

fn dispatch_similarity_metric<M, ScalarKernel, PerDensifyKernel, PackedLineKernel>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
    op: SimilarityOp,
    scalar_kernel: ScalarKernel,
    per_densify_kernel: PerDensifyKernel,
    packed_line_kernel: PackedLineKernel,
) -> PyResult<Py<PyAny>>
where
    M: SimilarityMode,
    ScalarKernel: Fn(
            &PreparedPlan,
            &PreparedPlan,
            &CachelessRowContext<'_>,
            Option<f64>,
        ) -> crate::error::Result<f64>
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
        let operation = op.with_densify(parse_scalar_densify(densify)?);
        return dispatch_binary(
            py,
            left,
            right,
            Operation::Metric(operation.metric_op()).name(),
            Operation::Metric(operation.metric_op()).resolver_with_unit(unit),
            &NoBinaryFastPath,
            move |left, right, ctx| scalar_kernel(left, right, ctx, operation.densify()),
        );
    };
    let densify = crate::OptionalDensifyParam::parse(densify, len)?;
    let operation = op.with_densify(densify.as_scalar_densify());
    dispatch_similarity_array_lane::<M>(
        py,
        side,
        left,
        right,
        &densify,
        unit,
        operation,
        per_densify_kernel,
        move |py, array, other, operation| {
            similarity_array_lane::<M, _, _>(
                py,
                array,
                other,
                unit,
                operation,
                operation.densify().is_none().then_some(packed_line_kernel),
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
        (Some(left), Some(right)) => Some((left.len(), SimilarityArraySide::Both(left, right))),
        (Some(left), None) => Some((left.len(), SimilarityArraySide::Left(left))),
        (None, Some(right)) => Some((right.len(), SimilarityArraySide::Right(right))),
        (None, None) => None,
    }
}

/// Shared array lane for the densify-aware similarity metrics (Hausdorff and
/// Fréchet). Both differ only by their operation name, the per-element
/// densified kernel, and the scalar lane that runs the packed column kernels —
/// everything else (per-element vs scalar branch, the array-operand flip when
/// the array is the right operand) is identical.
fn dispatch_similarity_array_lane<M: SimilarityMode>(
    py: Python<'_>,
    side: SimilarityArraySide<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: &crate::OptionalDensifyParam,
    unit: Option<DistanceUnit>,
    operation: SimilarityOp,
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
        SimilarityOp,
    ) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    if densify.is_per_element() {
        return match side {
            SimilarityArraySide::Left(array) => {
                crate::broadcast::array_crs_similarity_metric_per_densify(
                    py,
                    array,
                    right,
                    operation.metric_op(),
                    unit,
                    densify,
                    per_densify_kernel,
                )
            },
            SimilarityArraySide::Both(array, other) => {
                let _ = other.len();
                crate::broadcast::array_crs_similarity_metric_per_densify(
                    py,
                    array,
                    right,
                    operation.metric_op(),
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
                    operation.metric_op(),
                    unit,
                    densify,
                    per_densify_kernel,
                )
            },
        };
    }
    match side {
        SimilarityArraySide::Left(array) => scalar_lane(py, array, right, operation),
        SimilarityArraySide::Both(array, other) => {
            let _ = other.len();
            scalar_lane(py, array, right, operation)
        },
        SimilarityArraySide::Right(array) => scalar_lane(py, array, left, operation),
    }
}

fn similarity_array_lane<M, P, K>(
    py: Python<'_>,
    array: &crate::PyGeometryArray,
    other: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
    operation: SimilarityOp,
    packed_planar_lines: Option<P>,
    kernel: K,
) -> PyResult<Py<PyAny>>
where
    M: SimilarityMode,
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
    array_crs_metric_float::<M, _, _>(
        py,
        array,
        other,
        operation,
        unit,
        packed_planar_lines,
        move |model, a, b| kernel(model, a, b, operation.densify()),
    )
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use crate::PyGeometry;
    use crate::boundary::{Frame, crs_arc_static};
    use crate::geometry::{Point, Shape};
    use crate::predicates::engine::Predicate;
    use crate::py::errors::InvalidGeometryError;

    #[test]
    fn dispatch_predicate_point_crosses_invalid_latitude_is_false() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
                .expect("EPSG:4326 is a valid geographic frame");
            let invalid = Bound::new(
                py,
                PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(0.5, 91.0)), frame.clone())
                    .unwrap(),
            )
            .expect("invalid-latitude point binds")
            .into_any();
            let other = Bound::new(
                py,
                PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(0.0, 0.0)), frame).unwrap(),
            )
            .expect("origin point binds")
            .into_any();
            for predicate in [Predicate::Crosses, Predicate::Overlaps] {
                let value = super::dispatch_predicate(py, &invalid, &other, predicate)
                    .unwrap_or_else(|error| {
                        panic!("{predicate:?} must kind-gate before admission: {error}")
                    });
                let actual = value
                    .bind(py)
                    .extract::<bool>()
                    .expect("named predicate returns a bool");
                assert!(!actual, "{predicate:?} of two points is false");
            }
        });
    }

    #[test]
    fn dispatch_predicate_point_intersects_invalid_latitude_is_lonlat_error() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
                .expect("EPSG:4326 is a valid geographic frame");
            let invalid = Bound::new(
                py,
                PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(0.5, 91.0)), frame.clone())
                    .unwrap(),
            )
            .expect("invalid-latitude point binds")
            .into_any();
            let other = Bound::new(
                py,
                PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(0.0, 0.0)), frame).unwrap(),
            )
            .expect("origin point binds")
            .into_any();
            let error = super::dispatch_predicate(py, &invalid, &other, Predicate::Intersects)
                .expect_err("intersects must admit and fail on invalid latitude");
            assert!(error.is_instance_of::<InvalidGeometryError>(py), "{error}");
            let message = error.to_string();
            assert!(
                message.contains("invalid longitude/latitude (0.5, 91)"),
                "{message}"
            );
            assert!(!message.contains("geographic shape"), "{message}");
        });
    }
}
