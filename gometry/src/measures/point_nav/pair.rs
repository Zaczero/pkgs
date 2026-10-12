use pyo3::IntoPyObjectExt as _;

use super::adapters::{
    geometry_point, input_crs_str, input_frame, is_missing, point_operand, reject_rhumb_unit,
    require_geographic_model, row_zero,
};
use super::kernels::{
    bearing_degrees, distance_rhumb_points, interpolate_point, interpolate_point_on_geodesic,
    interpolate_point_planar, rhumb_bearing_points, rhumb_interpolate_point, to_geodesic_point,
};
use super::rows::{
    DistanceRow, PointBinaryF64Kernel, PointBinaryGeometryKernel, PointOperand,
    PointRowSource as _, PreparedPointOperand, distance_rows, finish_point_nav_coords,
    point_binary_f64_eval, point_binary_f64_rows_dispatch, point_binary_geometry_coords_dispatch,
    point_binary_geometry_eval, point_nav_missing_point_with_axes, point_operand_input,
    row_missing, uniform_point_binary_geometry_axes_dispatch, union_missing_masks,
};
use super::{
    ArrayRows, Bound, CoordSeq, CoordSeqBuilder, CoordinateAxes, DistanceUnit, Frame,
    GeometryInput, HasM, HasZ, MetricOp, NavigationPath, Point, Py, PyAny, PyGeometry,
    PyGeometryArray, PyResult, Python, Shape, ValidityMask, classify_input, crs, ensure_same_len,
    expected_geometry_or_array, float64_array, parameter_input_with_expected, pyfunction,
    resolve_metric,
};
use crate::IntoPyObject as _;

fn point_bearing(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    let from = point_operand(geometry, MetricOp::Bearing)?;
    let to = point_operand(other, MetricOp::Bearing)?;
    geometry
        .frame
        .compatible(&other.frame, MetricOp::Bearing.name())?;
    let model = resolve_metric(geometry.crs_str(), None, MetricOp::Bearing)?;
    let (Some(from), Some(to)) = (from, to) else {
        return Ok(f64::NAN);
    };
    point_binary_f64_eval(BearingKernel, &model, from, to)
}

fn point_distance_rhumb(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    const OP: MetricOp = MetricOp::RhumbDistance;
    let from = point_operand(geometry, OP)?;
    let to = point_operand(other, OP)?;
    geometry.frame.compatible(&other.frame, OP.name())?;
    let model = require_geographic_model(geometry.crs_str(), OP)?;
    let (Some(from), Some(to)) = (from, to) else {
        return Ok(f64::INFINITY);
    };
    point_binary_f64_eval(RhumbDistanceKernel, &model, from, to)
}

fn point_rhumb_bearing(geometry: &PyGeometry, other: &PyGeometry) -> PyResult<f64> {
    const OP: MetricOp = MetricOp::RhumbBearing;
    let from = point_operand(geometry, OP)?;
    let to = point_operand(other, OP)?;
    geometry.frame.compatible(&other.frame, OP.name())?;
    let model = require_geographic_model(geometry.crs_str(), OP)?;
    let (Some(from), Some(to)) = (from, to) else {
        return Ok(f64::NAN);
    };
    point_binary_f64_eval(RhumbBearingKernel, &model, from, to)
}

fn point_rhumb_between(
    geometry: &PyGeometry,
    other: &PyGeometry,
    distance: f64,
    normalized: bool,
) -> PyResult<Shape> {
    const OP: MetricOp = MetricOp::PointBetweenRhumb;
    let from = point_operand(geometry, OP)?;
    let to = point_operand(other, OP)?;
    geometry.frame.compatible(&other.frame, OP.name())?;
    let model = require_geographic_model(geometry.crs_str(), OP)?;
    let (Some(from), Some(to)) = (from, to) else {
        return Ok(Shape::empty_point());
    };
    let kernel = RhumbInterpolatePointKernel {
        distance: &[distance],
        normalized,
    };
    point_binary_geometry_eval(
        kernel,
        &model,
        distance_rows(kernel.distances(), 1)
            .next()
            .expect("scalar point-between input has one distance"),
        from,
        to,
    )
}

fn point_interpolate(
    geometry: &PyGeometry,
    other: &PyGeometry,
    distance: f64,
    normalized: bool,
    unit: Option<DistanceUnit>,
) -> PyResult<Shape> {
    const OP: MetricOp = MetricOp::PointBetween;
    let from = point_operand(geometry, OP)?;
    let to = point_operand(other, OP)?;
    geometry.frame.compatible(&other.frame, OP.name())?;
    let model = resolve_metric(geometry.crs_str(), unit, OP)?;
    let (Some(from), Some(to)) = (from, to) else {
        return Ok(Shape::empty_point());
    };
    interpolate_point(&model, from, to, distance, normalized)
}

trait PointPairRowsSink {
    type Value;

    fn run(
        self,
        left: PreparedPointOperand<'_>,
        right: PreparedPointOperand<'_>,
    ) -> PyResult<Self::Value>;
}

fn dispatch_point_pair_rows<S: PointPairRowsSink>(
    left: PointOperand<'_>,
    right: PointOperand<'_>,
    sink: S,
) -> PyResult<S::Value> {
    sink.run(left.prepare(), right.prepare())
}

#[derive(Clone, Copy)]
struct BearingKernel;

impl PointBinaryF64Kernel for BearingKernel {
    fn eval_planar(self, _to_metre: f64, from: Point, to: Point) -> PyResult<f64> {
        Ok(bearing_degrees(to.x - from.x, to.y - from.y))
    }

    fn eval_geodesic(self, metric: &crs::GeodesicMetric, from: Point, to: Point) -> PyResult<f64> {
        let from = to_geodesic_point(metric.frame, from)?;
        let to = to_geodesic_point(metric.frame, to)?;
        Ok(crs::geodesic_bearing(
            &metric.geodesic,
            from.x,
            from.y,
            to.x,
            to.y,
        )?)
    }
}

#[derive(Clone, Copy)]
struct RhumbDistanceKernel;

impl PointBinaryF64Kernel for RhumbDistanceKernel {
    fn eval_planar(self, _to_metre: f64, _left: Point, _right: Point) -> PyResult<f64> {
        unreachable!("rhumb setup admits only a geographic CRS")
    }

    fn eval_geodesic(
        self,
        metric: &crs::GeodesicMetric,
        left: Point,
        right: Point,
    ) -> PyResult<f64> {
        distance_rhumb_points(&metric.crs, metric.frame, left, right, &metric.state)
    }
}

#[derive(Clone, Copy)]
struct RhumbBearingKernel;

impl PointBinaryF64Kernel for RhumbBearingKernel {
    fn eval_planar(self, _to_metre: f64, _left: Point, _right: Point) -> PyResult<f64> {
        unreachable!("rhumb setup admits only a geographic CRS")
    }

    fn eval_geodesic(
        self,
        metric: &crs::GeodesicMetric,
        left: Point,
        right: Point,
    ) -> PyResult<f64> {
        rhumb_bearing_points(&metric.crs, metric.frame, left, right, &metric.state)
    }
}

#[derive(Clone, Copy)]
struct InterpolatePointKernel<'a> {
    distance: &'a [f64],
    normalized: bool,
}

#[derive(Clone, Copy)]
struct RhumbInterpolatePointKernel<'a> {
    distance: &'a [f64],
    normalized: bool,
}

impl PointBinaryGeometryKernel for RhumbInterpolatePointKernel<'_> {
    fn distances(&self) -> &[f64] {
        self.distance
    }

    fn eval_planar(
        self,
        _to_metre: f64,
        _row: DistanceRow,
        _left: Point,
        _right: Point,
    ) -> PyResult<Shape> {
        unreachable!("rhumb setup admits only a geographic CRS")
    }

    fn eval_geodesic(
        self,
        metric: &crs::GeodesicMetric,
        row: DistanceRow,
        left: Point,
        right: Point,
    ) -> PyResult<Shape> {
        rhumb_interpolate_point(
            &metric.crs,
            metric.frame,
            left,
            right,
            row.distance,
            self.normalized,
            &metric.state,
        )
    }

    fn output_axes(self, left: Point, right: Point) -> CoordinateAxes {
        CoordinateAxes::new(
            HasZ(left.axes.has_z() && right.axes.has_z()),
            HasM(left.axes.has_m() && right.axes.has_m()),
        )
    }
}

impl PointBinaryGeometryKernel for InterpolatePointKernel<'_> {
    fn distances(&self) -> &[f64] {
        self.distance
    }

    fn eval_planar(
        self,
        to_metre: f64,
        row: DistanceRow,
        left: Point,
        right: Point,
    ) -> PyResult<Shape> {
        Ok(interpolate_point_planar(
            to_metre,
            left,
            right,
            row.distance,
            self.normalized,
        ))
    }

    fn eval_geodesic(
        self,
        metric: &crs::GeodesicMetric,
        row: DistanceRow,
        left: Point,
        right: Point,
    ) -> PyResult<Shape> {
        Ok(Shape::Point(interpolate_point_on_geodesic(
            &metric.geodesic,
            metric.frame,
            left,
            right,
            row.distance,
            self.normalized,
        )?))
    }

    fn output_axes(self, left: Point, right: Point) -> CoordinateAxes {
        CoordinateAxes::new(
            HasZ(left.axes.has_z() && right.axes.has_z()),
            HasM(left.axes.has_m() && right.axes.has_m()),
        )
    }

    fn coords_rows_geodesic<const HAS_MISSING: bool, P>(
        self,
        metric: &crs::GeodesicMetric,
        len: usize,
        axes: CoordinateAxes,
        missing: Option<&ValidityMask>,
        mut points: P,
    ) -> PyResult<CoordSeq>
    where
        P: FnMut(usize) -> (Point, Point),
    {
        let mut builder = CoordSeqBuilder::with_capacity(axes, len);
        for row in distance_rows(self.distance, len) {
            let logical = row.logical.get();
            if row_missing::<HAS_MISSING>(missing, logical) {
                builder.push(point_nav_missing_point_with_axes(axes));
                continue;
            }
            let (left, right) = points(logical);
            // Fused inverse+direct into coordinate columns — no Shape::Point stage.
            let point = interpolate_point_on_geodesic(
                &metric.geodesic,
                metric.frame,
                left,
                right,
                row.distance,
                self.normalized,
            )
            .map_err(|err| crate::note_array_row(err.into(), logical))?;
            builder.push(point);
        }
        finish_point_nav_coords(builder, missing)
    }
}

fn owned_row_input(input: GeometryInput<'_>) -> PointBinaryRowInput {
    match input {
        GeometryInput::One(geometry) => PointBinaryRowInput::One(geometry.clone()),
        GeometryInput::Many(array) => PointBinaryRowInput::Many(array.clone()),
    }
}

#[derive(Clone, Copy)]
enum PointBinaryExecution<'a> {
    Scalar {
        left: &'a PyGeometry,
        right: &'a PyGeometry,
    },
    Rows(usize),
}

struct PointBinaryPlan<'a> {
    left: GeometryInput<'a>,
    right: GeometryInput<'a>,
    execution: PointBinaryExecution<'a>,
    output_frame: Frame,
    missing: Option<ValidityMask>,
}

impl<'a> PointBinaryPlan<'a> {
    fn new(
        left: &'a Bound<'_, PyAny>,
        right: &'a Bound<'_, PyAny>,
        operation: MetricOp,
    ) -> PyResult<Self> {
        let (Some(left), Some(right)) = (classify_input(left), classify_input(right)) else {
            return Err(expected_geometry_or_array());
        };
        let execution = match (left, right) {
            (GeometryInput::One(left), GeometryInput::One(right)) => {
                PointBinaryExecution::Scalar { left, right }
            },
            (GeometryInput::One(_), GeometryInput::Many(array))
            | (GeometryInput::Many(array), GeometryInput::One(_)) => {
                PointBinaryExecution::Rows(array.len())
            },
            (GeometryInput::Many(left), GeometryInput::Many(right)) => {
                // Length wins over frame compatibility for strict array broadcasting.
                ensure_same_len(left.len(), right.len())?;
                PointBinaryExecution::Rows(left.len())
            },
        };
        let (left_crs, left_epoch) = input_frame(&left);
        let (right_crs, right_epoch) = input_frame(&right);
        let output_frame = Frame::compatible_parts(
            left_crs,
            left_epoch,
            right_crs,
            right_epoch,
            operation.name(),
        )?;
        let missing = union_missing_masks([left, right]);
        Ok(Self {
            left,
            right,
            execution,
            output_frame,
            missing,
        })
    }

    fn crs_str(&self) -> Option<&str> {
        input_crs_str(&self.left)
    }

    fn point_operands(&self) -> Option<(PointOperand<'a>, PointOperand<'a>)> {
        Some((
            point_operand_input(self.left)?,
            point_operand_input(self.right)?,
        ))
    }

    fn owned_rows(&self) -> PointBinaryRows {
        PointBinaryRows {
            left: owned_row_input(self.left),
            right: owned_row_input(self.right),
        }
    }
}

enum PointBinaryRowInput {
    One(PyGeometry),
    Many(PyGeometryArray),
}

struct PointBinaryRows {
    left: PointBinaryRowInput,
    right: PointBinaryRowInput,
}

impl PointBinaryRows {
    fn evaluate<T>(
        &self,
        row: usize,
        operation: impl FnOnce(&PyGeometry, &PyGeometry) -> PyResult<T>,
    ) -> PyResult<T> {
        match (&self.left, &self.right) {
            (PointBinaryRowInput::One(left), PointBinaryRowInput::One(right)) => {
                operation(left, right)
            },
            (PointBinaryRowInput::One(left), PointBinaryRowInput::Many(array)) => {
                let geometry = array.geometry_at(row).map_err(crate::PyErr::from)?;
                operation(left, &geometry)
            },
            (PointBinaryRowInput::Many(array), PointBinaryRowInput::One(right)) => {
                let geometry = array.geometry_at(row).map_err(crate::PyErr::from)?;
                operation(&geometry, right)
            },
            (PointBinaryRowInput::Many(left), PointBinaryRowInput::Many(right)) => {
                let left_geometry = left.geometry_at(row).map_err(crate::PyErr::from)?;
                let right_geometry = right.geometry_at(row).map_err(crate::PyErr::from)?;
                operation(&left_geometry, &right_geometry)
            },
        }
    }
}

struct PointBinaryNumericRowsSink<'py, K> {
    py: Python<'py>,
    model: crs::MetricModel,
    len: usize,
    missing: Option<ValidityMask>,
    kernel: K,
}

impl<K: PointBinaryF64Kernel> PointPairRowsSink for PointBinaryNumericRowsSink<'_, K> {
    type Value = Py<PyAny>;

    fn run(
        self,
        left: PreparedPointOperand<'_>,
        right: PreparedPointOperand<'_>,
    ) -> PyResult<Self::Value> {
        let Self {
            py,
            model,
            len,
            missing,
            kernel,
        } = self;
        float64_array(
            py,
            py.detach(move || {
                point_binary_f64_rows_dispatch(kernel, &model, len, missing.as_ref(), |row| {
                    (left.point_at(row), right.point_at(row))
                })
            })?,
        )
    }
}

struct PointBinaryGeometryRowsSink<'py, 'plan, 'input, K, F> {
    py: Python<'py>,
    plan: &'plan PointBinaryPlan<'input>,
    model: crs::MetricModel,
    len: usize,
    kernel: K,
    fallback: F,
}

impl<K, F> PointPairRowsSink for PointBinaryGeometryRowsSink<'_, '_, '_, K, F>
where
    K: PointBinaryGeometryKernel,
    F: Fn(&PyGeometry, &PyGeometry, DistanceRow) -> PyResult<Shape> + Copy + Send + Sync,
{
    type Value = Py<PyAny>;

    fn run(
        self,
        left: PreparedPointOperand<'_>,
        right: PreparedPointOperand<'_>,
    ) -> PyResult<Self::Value> {
        let Self {
            py,
            plan,
            model,
            len,
            kernel,
            fallback,
        } = self;
        if len == 0 {
            return Ok(
                PyGeometryArray::from_shapes(Vec::new(), plan.output_frame.clone())?
                    .into_pyobject(py)?
                    .unbind()
                    .into(),
            );
        }
        let Some(axes) = uniform_point_binary_geometry_axes_dispatch(
            kernel,
            len,
            plan.missing.as_ref(),
            |row| (left.point_at(row), right.point_at(row)),
        ) else {
            return point_binary_geometry_fallback(py, plan, len, kernel, fallback);
        };
        let missing = plan.missing.clone();
        let missing_for_rows = missing.clone();
        let coords = py.detach(move || {
            point_binary_geometry_coords_dispatch(
                kernel,
                &model,
                len,
                axes,
                missing_for_rows.as_ref(),
                |row| (left.point_at(row), right.point_at(row)),
            )
        })?;
        Ok(
            PyGeometryArray::packed_points(coords, plan.output_frame.clone())?
                .with_missing_mask(missing)?
                .into_pyobject(py)?
                .unbind()
                .into(),
        )
    }
}

fn point_binary_f64(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    operation: MetricOp,
    setup: impl Fn(Option<&str>) -> PyResult<crs::MetricModel> + Copy + Send + Sync,
    point_op: impl PointBinaryF64Kernel,
    fallback: impl Fn(&PyGeometry, &PyGeometry) -> PyResult<f64> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let plan = PointBinaryPlan::new(left, right, operation)?;
    point_binary_numeric_sink(py, &plan, setup, point_op, fallback)
}

fn point_binary_numeric_sink(
    py: Python<'_>,
    plan: &PointBinaryPlan<'_>,
    setup: impl Fn(Option<&str>) -> PyResult<crs::MetricModel> + Copy + Send + Sync,
    point_op: impl PointBinaryF64Kernel,
    fallback: impl Fn(&PyGeometry, &PyGeometry) -> PyResult<f64> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let len = match plan.execution {
        PointBinaryExecution::Scalar { left, right } => {
            return match (geometry_point(left), geometry_point(right)) {
                (Some(left_point), Some(right_point)) => {
                    let model = setup(plan.crs_str())?;
                    point_binary_f64_eval(point_op, &model, left_point, right_point)?
                        .into_py_any(py)
                },
                _ => fallback(left, right)?.into_py_any(py),
            };
        },
        PointBinaryExecution::Rows(len) => len,
    };

    if let Some((left, right)) = plan.point_operands() {
        let model = setup(plan.crs_str()).map_err(row_zero)?;
        return dispatch_point_pair_rows(left, right, PointBinaryNumericRowsSink {
            py,
            model,
            len,
            missing: plan.missing.clone(),
            kernel: point_op,
        });
    }

    let rows = plan.owned_rows();
    let missing = plan.missing.clone();
    float64_array(
        py,
        py.detach(move || {
            (0..len)
                .map(|row| {
                    if is_missing(missing.as_ref(), row) {
                        return Ok(f64::NAN);
                    }
                    rows.evaluate(row, fallback)
                        .map_err(|err| crate::note_array_row(err, row))
                })
                .collect::<PyResult<Vec<_>>>()
        })?,
    )
}

fn point_binary_geometry_sink(
    py: Python<'_>,
    plan: &PointBinaryPlan<'_>,
    setup: impl Fn(Option<&str>) -> PyResult<crs::MetricModel> + Copy + Send + Sync,
    point_op: impl PointBinaryGeometryKernel,
    fallback: impl Fn(&PyGeometry, &PyGeometry, DistanceRow) -> PyResult<Shape> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let len = match plan.execution {
        PointBinaryExecution::Scalar { left, right } => {
            let shape = if let (Some(left_point), Some(right_point)) =
                (geometry_point(left), geometry_point(right))
            {
                let model = setup(plan.crs_str())?;
                point_binary_geometry_eval(
                    point_op,
                    &model,
                    distance_rows(point_op.distances(), 1)
                        .next()
                        .expect("scalar point-between input has one distance"),
                    left_point,
                    right_point,
                )?
            } else {
                fallback(
                    left,
                    right,
                    distance_rows(point_op.distances(), 1)
                        .next()
                        .expect("scalar point-between input has one distance"),
                )?
            };
            return Ok(left.typed_shape(shape).into_pyobject(py)?.unbind());
        },
        PointBinaryExecution::Rows(len) => len,
    };

    if let Some((left, right)) = plan.point_operands() {
        let model = setup(plan.crs_str()).map_err(row_zero)?;
        return dispatch_point_pair_rows(left, right, PointBinaryGeometryRowsSink {
            py,
            plan,
            model,
            len,
            kernel: point_op,
            fallback,
        });
    }

    point_binary_geometry_fallback(py, plan, len, point_op, fallback)
}

fn point_binary_geometry_fallback(
    py: Python<'_>,
    plan: &PointBinaryPlan<'_>,
    len: usize,
    kernel: impl PointBinaryGeometryKernel,
    fallback: impl Fn(&PyGeometry, &PyGeometry, DistanceRow) -> PyResult<Shape> + Copy + Send + Sync,
) -> PyResult<Py<PyAny>> {
    let rows = plan.owned_rows();
    let missing = plan.missing.clone();
    let missing_for_rows = missing.clone();
    let shapes = py.detach(move || {
        distance_rows(kernel.distances(), len)
            .map(|row| {
                let logical = row.logical.get();
                if is_missing(missing_for_rows.as_ref(), logical) {
                    return Ok(ArrayRows::missing_placeholder());
                }
                rows.evaluate(logical, |left, right| fallback(left, right, row))
                    .map_err(|err| crate::note_array_row(err, logical))
            })
            .collect::<PyResult<Vec<_>>>()
    })?;
    Ok(
        PyGeometryArray::from_shapes(shapes, plan.output_frame.clone())?
            .with_missing_mask(missing)?
            .into_pyobject(py)?
            .unbind()
            .into(),
    )
}
/// Initial bearing from one point to another, in degrees clockwise from north
/// (``0..360``).
///
/// Geodesic by default on a geographic CRS (on the CRS ellipsoid); grid
/// azimuth on a projected or CRS-free point. ``path='rhumb'`` selects the
/// constant compass course on a geographic CRS. An empty point yields ``nan``.
///
/// Parameters
/// ----------
/// left, right : Point or GeometryArray
///     Origin and destination; must share a CRS.
/// path : {'geodesic', 'rhumb'}, default 'geodesic'
///     Route model. Rhumb bearings require a geographic CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     One bearing per input pair.
///
/// Raises
/// ------
/// GeometryTypeError
///     If either operand is not a `Point`.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// CRSError
///     If a coordinate is outside the longitude/latitude domain.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.bearing(gm.Point(0, 0), gm.Point(1, 0))
/// 90.0
#[pyfunction]
#[pyo3(
    signature = (left, right, *, path = NavigationPath::Geodesic),
    text_signature = "(left, right, *, path='geodesic')"
)]
pub(crate) fn bearing(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    path: NavigationPath,
) -> PyResult<Py<PyAny>> {
    match path {
        NavigationPath::Geodesic => point_binary_f64(
            py,
            left,
            right,
            MetricOp::Bearing,
            |crs| Ok(crs::metric_model(crs)?),
            BearingKernel,
            point_bearing,
        ),
        NavigationPath::Rhumb => point_binary_f64(
            py,
            left,
            right,
            MetricOp::RhumbBearing,
            |crs| require_geographic_model(crs, MetricOp::RhumbBearing),
            RhumbBearingKernel,
            point_rhumb_bearing,
        ),
    }
}

/// Return a point interpolated from ``left`` towards ``right``.
///
/// CRS-aware like every metric: geodesic on a geographic CRS, planar on a
/// projected CRS, and raw coordinate units when CRS-free. ``path='rhumb'`` follows
/// the constant-bearing track on a geographic CRS. Z/M interpolate. An empty
/// endpoint yields ``POINT EMPTY``.
///
/// Parameters
/// ----------
/// left, right : Point or GeometryArray
///     Endpoints; must share a CRS.
/// distance : float or sequence of float
///     Finite distance(s) from ``left`` (or fractions if ``normalized``).
///     Values outside the segment clamp to its endpoints.
/// normalized : bool, default False
///     Treat ``distance`` as a fraction of the total distance.
/// path : {'geodesic', 'rhumb'}, default 'geodesic'
///     Route model. Rhumb paths require a geographic CRS and use meters.
#[doc = unit_parameter_doc!()]
///     ``path='rhumb'`` uses meters and does not accept a unit override.
///
/// Returns
/// -------
/// Point or GeometryArray
///     One interpolated point per input pair.
///
/// Raises
/// ------
/// GeometryTypeError
///     If either operand is not a `Point`.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``distance`` is non-finite.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.point_between(gm.Point(0, 0), gm.Point(2, 0), 0.5).to_wkt()
/// 'POINT (0.5 0)'
#[pyfunction]
#[pyo3(
    signature = (left, right, distance, *, normalized = false, path = NavigationPath::Geodesic, unit = None),
    text_signature = "(left, right, distance, *, normalized=False, path='geodesic', unit=None)"
)]
pub(crate) fn point_between(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    normalized: bool,
    path: NavigationPath,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let mut plan = PointBinaryPlan::new(left, right, MetricOp::PointBetween)?;
    let receiver_len = match plan.execution {
        PointBinaryExecution::Scalar { .. } => None,
        PointBinaryExecution::Rows(len) => Some(len),
    };
    let mut distance =
        parameter_input_with_expected(py, distance, "distance", receiver_len, &|| {
            "distance must be finite".into()
        })?;
    match plan.execution {
        PointBinaryExecution::Scalar { .. } if !distance.scalar => {
            plan.execution = PointBinaryExecution::Rows(distance.values.len());
        },
        PointBinaryExecution::Rows(len) => {
            crate::broadcast_coordinate_input(&mut distance, len, "distance")?;
        },
        PointBinaryExecution::Scalar { .. } => {},
    }
    let distances = distance.values.as_slice();
    match path {
        NavigationPath::Geodesic => point_binary_geometry_sink(
            py,
            &plan,
            move |crs| resolve_metric(crs, unit, MetricOp::PointBetween),
            InterpolatePointKernel {
                distance: distances,
                normalized,
            },
            move |left, right, row| point_interpolate(left, right, row.distance, normalized, unit),
        ),
        NavigationPath::Rhumb => {
            reject_rhumb_unit(unit, MetricOp::PointBetween)?;
            point_binary_geometry_sink(
                py,
                &plan,
                |crs| require_geographic_model(crs, MetricOp::PointBetweenRhumb),
                RhumbInterpolatePointKernel {
                    distance: distances,
                    normalized,
                },
                move |left, right, row| point_rhumb_between(left, right, row.distance, normalized),
            )
        },
    }
}

pub(crate) fn distance_rhumb(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    reject_rhumb_unit(unit, MetricOp::RhumbDistance)?;
    let left = crate::normalize_geometry_batch(py, left)?;
    let right = crate::normalize_geometry_batch(py, right)?;
    point_binary_f64(
        py,
        &left,
        &right,
        MetricOp::RhumbDistance,
        |crs| require_geographic_model(crs, MetricOp::RhumbDistance),
        RhumbDistanceKernel,
        point_distance_rhumb,
    )
}
