use pyo3::prelude::*;

use crate::broadcast::{
    GeometryInput, OverlayOutput, SimilarityOp, array_crs_distances, array_crs_dwithin,
    classify_required, predicate_pairwise_arrays, predicate_scalar_vs_array,
};
use crate::crs::{MetricModel, ResolvedMetric};
use crate::dispatch::binary::{
    BinaryArrayFastPath, NoBinaryFastPath, dispatch_binary, dispatch_binary_geometry,
};
use crate::dispatch::kernels;
use crate::dispatch::metric::GeodesicBinaryRow;
use crate::dispatch::operation::{MetricOp, Operation};
use crate::geometry::{GeometryKind, PreparedPlan, TopologyDomain, storage};
use crate::predicates::engine;
use crate::predicates::engine::Predicate;
use crate::{DistanceUnit, DwithinDistance, Frame, OverlayOp};

/// Predicate array-lane fast path using the shared batch engine.
pub(crate) struct PredicateFastPath {
    predicate: Predicate,
    scalar_is_left: bool,
}

impl PredicateFastPath {
    const fn new(predicate: Predicate, scalar_is_left: bool) -> Self {
        Self {
            predicate,
            scalar_is_left,
        }
    }
}

impl BinaryArrayFastPath<bool> for PredicateFastPath {
    fn is_eligible(
        &self,
        _left: &crate::PyGeometryArray,
        _right: GeometryInput<'_>,
        _op_name: &str,
        _frame: &Frame,
    ) -> bool {
        true
    }

    fn try_dispatch(
        &self,
        py: Python<'_>,
        array: &crate::PyGeometryArray,
        other: GeometryInput<'_>,
        _op_name: &str,
        frame: &Frame,
        domain: crate::geometry::TopologyDomain,
        missing: Option<&storage::ValidityMask>,
        _model: Option<&MetricModel>,
        _metric: Option<&ResolvedMetric>,
    ) -> Option<PyResult<Py<PyAny>>> {
        let spec = self.predicate.spec();
        Some(match other {
            GeometryInput::One(scalar) => predicate_scalar_vs_array(
                py,
                &spec,
                scalar,
                array,
                self.scalar_is_left,
                frame,
                domain,
            ),
            GeometryInput::Many(right) => {
                predicate_pairwise_arrays(py, &spec, array, right, frame, domain, missing)
            },
        })
    }
}

/// Overlay set-ops use the shared geometry kernel over array rows.
pub(crate) struct OverlayFastPath;

impl BinaryArrayFastPath<OverlayOutput> for OverlayFastPath {
    fn is_eligible(
        &self,
        _left: &crate::PyGeometryArray,
        _right: GeometryInput<'_>,
        _op_name: &str,
        _frame: &Frame,
    ) -> bool {
        false
    }

    fn try_dispatch(
        &self,
        _py: Python<'_>,
        _left: &crate::PyGeometryArray,
        _right: GeometryInput<'_>,
        _op_name: &str,
        _frame: &Frame,
        _domain: crate::geometry::TopologyDomain,
        _missing: Option<&storage::ValidityMask>,
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
    let admission = super::binary::admit_binary(left, right, operation.name())?;
    if let (GeometryInput::One(left), GeometryInput::One(right)) = (admission.left, admission.right)
    {
        let spec = predicate.spec();
        // The kind gate reads the operand kinds alone, so it answers before a
        // prepared plan or a frame product is resolved.
        if let Some(verdict) = spec.predicate.kind_gate(
            GeometryKind::of(left.shape.shape()),
            GeometryKind::of(right.shape.shape()),
        ) {
            return Ok(crate::broadcast::py_bool(py, verdict));
        }
        let left_plan = left.prepared_plan();
        let right_plan = right.prepared_plan();
        // Only geographic topology reads frame products; resolving them for a
        // planar pair allocates a row-product chunk per operand that the
        // Cartesian arm never looks at.
        let value = match admission.domain {
            TopologyDomain::Cartesian => py.detach(move || {
                let operands = engine::PreparedTopologyPair::cartesian(&left_plan, &right_plan);
                engine::topology_scalar_pair(&spec, &operands)
            }),
            TopologyDomain::Geographic(frame) => {
                let left_cache = left.frame_cache();
                let right_cache = right.frame_cache();
                py.detach(move || {
                    let operands = engine::PreparedTopologyPair::try_geographic(
                        &left_plan,
                        &left_cache,
                        &right_plan,
                        &right_cache,
                        frame,
                    )?;
                    engine::topology_scalar_pair(&spec, &operands)
                })
            },
        }?;
        return Ok(crate::broadcast::py_bool(py, value));
    }
    let scalar_is_left = matches!(admission.left, GeometryInput::One(_));
    super::binary::dispatch_binary_admitted(
        py,
        &admission,
        operation.name(),
        super::MetricResolver::None,
        &PredicateFastPath::new(predicate, scalar_is_left),
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
        operation.resolver(),
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
        &GeodesicBinaryRow<'_>,
    ) -> crate::error::Result<R>
    + Copy
    + Send
    + Sync,
    array_lane: impl for<'a> Fn(
        Python<'_>,
        &'a crate::PyGeometryArray,
        GeometryInput<'a>,
        &Frame,
        MetricOp,
        Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>>
where
    R: Send + crate::broadcast::BulkElement,
{
    let admission = super::binary::admit_binary(left, right, op.name())?;
    dispatch_metric_binary_admitted(
        py,
        &admission,
        op,
        unit,
        planar_kernel,
        geodesic_kernel,
        array_lane,
    )
}

fn dispatch_metric_binary_admitted<R>(
    py: Python<'_>,
    admission: &super::binary::BinaryAdmission<'_>,
    op: MetricOp,
    unit: Option<DistanceUnit>,
    planar_kernel: impl Fn(f64, &PreparedPlan, &PreparedPlan) -> crate::error::Result<R>
    + Copy
    + Send
    + Sync,
    geodesic_kernel: impl Fn(
        &PreparedPlan,
        &PreparedPlan,
        &GeodesicBinaryRow<'_>,
    ) -> crate::error::Result<R>
    + Copy
    + Send
    + Sync,
    array_lane: impl for<'a> Fn(
        Python<'_>,
        &'a crate::PyGeometryArray,
        GeometryInput<'a>,
        &Frame,
        MetricOp,
        Option<DistanceUnit>,
    ) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>>
where
    R: Send + crate::broadcast::BulkElement,
{
    let operation = Operation::Metric(op);
    let has_missing = matches!(&admission.left, GeometryInput::Many(array) if array.has_missing())
        || matches!(&admission.right, GeometryInput::Many(array) if array.has_missing());
    if (has_missing && matches!(admission.domain, TopologyDomain::Geographic(_)))
        || matches!(
            (&admission.left, &admission.right),
            (GeometryInput::One(_), GeometryInput::One(_))
        )
    {
        return super::dispatch_typed_metric_binary(
            py,
            admission,
            op,
            operation.resolver_with_unit(unit),
            planar_kernel,
            geodesic_kernel,
        );
    }
    match (admission.left, admission.right) {
        (GeometryInput::Many(array), other) | (other, GeometryInput::Many(array)) => {
            array_lane(py, array, other, &admission.frame, op, unit)
        },
        (GeometryInput::One(_), GeometryInput::One(_)) => unreachable!(),
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

pub(crate) fn dispatch_dwithin_admitted(
    py: Python<'_>,
    admission: &super::binary::BinaryAdmission<'_>,
    distance: DwithinDistance,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    dispatch_metric_binary_admitted(
        py,
        admission,
        MetricOp::Dwithin,
        unit,
        move |scale, left, right| {
            crate::broadcast::pair_dwithin_planar(scale, left, right, distance.get())
        },
        move |left, right, ctx| kernels::binary_dwithin(left, right, ctx, distance.get()),
        move |py, array, other, frame, op, unit| {
            array_crs_dwithin(py, array, other, frame, distance.get(), op, unit)
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
    max_error: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    dispatch_similarity_metric(py, left, right, unit, |_| {
        SimilarityOp::hausdorff(max_error)
    })
}

/// Fréchet-distance free-function dispatch through the spine.
pub(crate) fn dispatch_frechet_distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    dispatch_similarity_metric(py, left, right, unit, |len| {
        SimilarityOp::frechet_with_densify(densify, len)
    })
}

fn dispatch_similarity_metric(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
    make_operation: impl Fn(usize) -> PyResult<SimilarityOp>,
) -> PyResult<Py<PyAny>> {
    let left_array = crate::broadcast::exact_geometry_array(left);
    let right_array = crate::broadcast::exact_geometry_array(right);
    let array_len = match (left_array, right_array) {
        (Some(array), _) | (None, Some(array)) => Some(array.len()),
        (None, None) => None,
    };
    let Some(len) = array_len else {
        let operation = make_operation(1)?;
        let metric_op = operation.metric_op();
        let left_input = classify_required(left)?;
        let right_input = classify_required(right)?;
        let admission =
            super::binary::admit_binary_inputs(left_input, right_input, metric_op.name())?;
        return super::binary::dispatch_binary_admitted(
            py,
            &admission,
            Operation::Metric(metric_op).name(),
            Operation::Metric(metric_op).resolver_with_unit(unit),
            &NoBinaryFastPath,
            move |left, right, ctx| operation.eval_scalar(left, right, ctx),
        );
    };
    let operation = make_operation(len)?;
    let metric_op = operation.metric_op();
    let left_input = classify_required(left)?;
    let right_input = classify_required(right)?;
    let admission = super::binary::admit_binary_inputs(left_input, right_input, metric_op.name())?;
    dispatch_similarity_array_lane(py, &admission, operation, unit)
}

/// Shared array lane for the similarity metrics. Fréchet's per-element
/// densification and the scalar lane's packed column kernels are the only
/// operation-specific branches; the array-operand flip is common.
fn dispatch_similarity_array_lane(
    py: Python<'_>,
    admission: &super::binary::BinaryAdmission<'_>,
    operation: SimilarityOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let (array, other) = match (admission.left, admission.right) {
        (GeometryInput::Many(array), other) | (other, GeometryInput::Many(array)) => (array, other),
        (GeometryInput::One(_), GeometryInput::One(_)) => unreachable!(),
    };
    if operation.is_per_element() {
        return crate::broadcast::array_crs_frechet_metric_per_densify(
            py,
            array,
            other,
            &admission.frame,
            operation,
            unit,
        );
    }
    similarity_array_lane(py, array, other, &admission.frame, operation, unit)
}

fn similarity_array_lane(
    py: Python<'_>,
    array: &crate::PyGeometryArray,
    other: GeometryInput<'_>,
    frame: &Frame,
    operation: SimilarityOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    crate::broadcast::array_crs_metric_float(py, array, other, frame, operation, unit)
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use crate::PyGeometry;
    use crate::array::PyGeometryArray;
    use crate::crs::frame::{Frame, crs_arc_static};
    use crate::geometry::storage::{
        GeometryArrayStorage, ValidityMask, reset_shape_iter_materializations,
        shape_iter_materializations,
    };
    use crate::geometry::{LineSeq, Point, Shape};
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
                PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(0.5, 91.0)), frame.clone()),
            )
            .expect("invalid-latitude point binds")
            .into_any();
            let other = Bound::new(
                py,
                PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(0.0, 0.0)), frame),
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
                PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(0.5, 91.0)), frame.clone()),
            )
            .expect("invalid-latitude point binds")
            .into_any();
            let other = Bound::new(
                py,
                PyGeometry::with_frame(Shape::Point(Point::new_raw_xy(0.0, 0.0)), frame),
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

    #[test]
    fn geographic_array_overlay_keeps_the_bound_shape_sink_mixed() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let frame = Frame::new(Some(crs_arc_static("EPSG:4326")), None)
                .expect("EPSG:4326 is a valid geographic frame");
            let polygon = crate::io::parse_wkt(
                "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))",
            )
            .expect("the geographic polygon parses");
            let left = PyGeometryArray::from_shapes(vec![polygon.clone()], frame.clone())
                .expect("the input array is admitted");
            let right = PyGeometry::with_frame(polygon, frame);
            let left = Bound::new(py, left).unwrap().into_any();
            let right = Bound::new(py, right).unwrap().into_any();

            let result = super::dispatch_overlay(py, &left, &right, crate::OverlayOp::Union)
                .expect("the geographic overlay completes");
            let output = result
                .bind(py)
                .cast::<PyGeometryArray>()
                .expect("an array operand returns a GeometryArray")
                .get();
            assert!(
                matches!(output.storage(), GeometryArrayStorage::Mixed(_)),
                "the bound geographic Shape sink must not be packed after acceptance"
            );
        });
    }

    #[test]
    fn similarity_many_skips_shape_materialization_for_missing_rows() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
            let line = Shape::LineString(
                LineSeq::try_new(
                    crate::geometry::CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(1.0, 1.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            );
            let left = PyGeometryArray::from_shapes(vec![point.clone(), line.clone()], Frame::None)
                .unwrap()
                .with_missing_mask(Some(
                    ValidityMask::from_vec(2, vec![false, true]).expect("one missing row"),
                ))
                .unwrap();
            let right = PyGeometryArray::from_shapes(vec![point, line], Frame::None).unwrap();
            let left = Bound::new(py, left).unwrap().into_any();
            let right = Bound::new(py, right).unwrap().into_any();

            reset_shape_iter_materializations();
            let max_error = 1e-12_f64.into_pyobject(py).unwrap().into_any();
            let result =
                super::dispatch_hausdorff_distance(py, &left, &right, Some(&max_error), None)
                    .expect("masked pair is a valid Hausdorff broadcast");
            let values = result
                .bind(py)
                .extract::<Vec<f64>>()
                .expect("Hausdorff returns a float array");

            assert_eq!(values[0], 0.0);
            assert!(values[1].is_nan());
            assert_eq!(
                shape_iter_materializations(),
                0,
                "missing rows must not enter the shape-materializing iterator"
            );
        });
    }
}
