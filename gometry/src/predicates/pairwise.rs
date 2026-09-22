use pyo3::IntoPyObjectExt as _;
use pyo3::types::PyAny;

use crate::array::missing::union_many;
use crate::{
    Bound, CollectRows as _, DistanceUnit, GeometryInput, IntoPyObject as _, Py, PyGeometry,
    PyResult, Python, classify_required, crs, exact_geometry_array,
    fixed_geometry_array_nearest_points_admitted, fixed_geometry_array_shortest_line_geodesic,
    metric_nearest_points_resolved, metric_shortest_line_resolved, nearest_point_columns_masked,
    pyfunction, resolve_metric, validate_dwithin_distance, validate_dwithin_distance_arg,
};

/// Return the shortest connecting line between two geometries (CRS-aware).
///
/// ``nearest_points`` as a ``LineString`` — degenerate when the geometries
/// touch. Planar and projected metrics accept any geometry pair, not only
/// points, and the witness is the true closest approach, so an endpoint may
/// land on an edge interior rather than a vertex. Geographic metrics support
/// point and linework operands only; filled polygon interiors require
/// ``unit='planar'`` or a projected CRS. An empty operand yields
/// ``LINESTRING EMPTY`` (the output-type sentinel, like ``distance``'s ``inf``).
/// Output ordinates are the operands' common axes (an XYZM line versus an XY
/// point gives an XY line). See `nearest_points` (the endpoints) and `distance`
/// (the length).
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (native angular units as Cartesian
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     One connecting ``LineString`` per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// CRSError
///     If a geographic operand includes a filled polygon interior.
///
/// See Also
/// --------
/// distance : Minimum pairwise distance.
/// nearest_points : Closest-pair witness points.
/// dwithin : Within-distance test.
/// hausdorff_distance : Hausdorff (set-to-set) similarity distance.
/// frechet_distance : Curve-to-curve similarity (linestrings).
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> square = gm.box(0, 0, 1, 1)
/// >>> gm.shortest_line(square, gm.Point(3, 0.5)).to_wkt()
/// 'LINESTRING (1 0.5, 3 0.5)'
/// >>> # The near point can fall mid-edge, not on a vertex:
/// >>> a = gm.from_wkt('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')
/// >>> b = gm.from_wkt('POLYGON ((4 1, 5 0, 6 1, 5 2, 4 1))')
/// >>> gm.shortest_line(a, b).to_wkt()
/// 'LINESTRING (2 1, 4 1)'
#[pyfunction]
#[pyo3(signature = (left, right, *, unit = None))]
pub(crate) fn shortest_line(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let operation = crate::dispatch::MetricOp::ShortestLine;
        let admission = crate::dispatch::admit_binary(left, right, operation.name())?;
        let model = resolve_metric(admission.frame.crs_str(), unit, operation)?;
        match (admission.left, admission.right) {
            (GeometryInput::One(geometry), GeometryInput::Many(array)) => {
                // A crossing scalar can't share one prepared cache across the
                // seam — fall through to the split-normalizing broadcast below
                // for it.
                if let crs::MetricModel::Geodesic { crs, frame, state } = &model
                    && !crate::geometry::crosses_antimeridian_in_frame(
                        geometry.shape.shape(),
                        *frame,
                    )
                {
                    let result = fixed_geometry_array_shortest_line_geodesic(
                        py, geometry, array, true, crs, *frame, state,
                    )?;
                    return Ok(result.into_pyobject(py)?.unbind().into());
                }
                let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
                crate::dispatch::dispatch_binary_geometry_admitted(
                    py,
                    &admission,
                    operation.name(),
                    &crate::dispatch::OverlayFastPath,
                    move |left, right, _ctx| metric_shortest_line_resolved(&resolved, left, right),
                )
            },
            (GeometryInput::Many(array), GeometryInput::One(geometry)) => {
                if let crs::MetricModel::Geodesic { crs, frame, state } = &model {
                    let result = fixed_geometry_array_shortest_line_geodesic(
                        py, geometry, array, false, crs, *frame, state,
                    )?;
                    return Ok(result.into_pyobject(py)?.unbind().into());
                }
                let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
                crate::dispatch::dispatch_binary_geometry_admitted(
                    py,
                    &admission,
                    operation.name(),
                    &crate::dispatch::OverlayFastPath,
                    move |left, right, _ctx| metric_shortest_line_resolved(&resolved, left, right),
                )
            },
            (GeometryInput::One(_), GeometryInput::One(_))
            | (GeometryInput::Many(_), GeometryInput::Many(_)) => {
                let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
                crate::dispatch::dispatch_binary_geometry_admitted(
                    py,
                    &admission,
                    operation.name(),
                    &crate::dispatch::OverlayFastPath,
                    move |left, right, _ctx| metric_shortest_line_resolved(&resolved, left, right),
                )
            },
        }
    })
}

/// Compute the minimum distance between ``left`` and ``right``.
///
/// CRS-aware: geodesic meters on a geographic CRS, native linear units on a
/// projected CRS, coordinate units when CRS-free. Geographic distance supports
/// point and linework operands only; filled polygon interiors require
/// ``unit='planar'`` or a projected CRS.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (native angular units as Cartesian
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     One result per input pair.
///
/// Raises
/// ------
/// CRSError
///     If the CRS lacks linear axis units for a metric result, or a geographic
///     operand includes a filled polygon interior.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``unit='meters'`` is requested for a CRS-free geometry.
///
/// See Also
/// --------
/// dwithin : Within-distance test.
/// nearest_points : Closest-pair witness points.
/// shortest_line : Connecting line between closest points.
/// hausdorff_distance : Hausdorff (set-to-set) similarity distance.
/// frechet_distance : Curve-to-curve similarity (linestrings).
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.distance(gm.Point(0, 0), gm.Point(3, 4))
/// 5.0
/// >>> a, b = gm.Point(13.0, 52.0, crs=4326), gm.Point(13.1, 52.0, crs=4326)
/// >>> round(gm.distance(a, b))  # meters
/// 6868
#[pyfunction]
#[pyo3(signature = (left, right, *, unit = None))]
pub(crate) fn distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_distance(py, left, right, unit)
}

/// Compute the minimum 3D (Euclidean) distance between two geometries,
/// measured for their CRS.
///
/// Distance is over linework: points/multipoints as degenerate segments,
/// polygons via their boundary rings. Part of the 3D Euclidean family with
/// ``length_3d`` only — area and perimeter are inherently 2D (XY footprint)
/// and have no 3D form; the 3D extent is ``bounds`` with ``min_z``/``max_z``.
///
/// A projected CRS gives native linear units and a CRS-free geometry gives
/// coordinate units — the same defaults as the 2D ``distance``. A geographic
/// CRS raises under every ``unit``: a Euclidean norm cannot combine degrees
/// with meter heights. Every vertex on both operands must carry a Z ordinate.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
/// unit : {'planar', 'meters'} or None, default None
///     ``None`` follows the CRS, exactly like ``distance``. ``'planar'``
///     forces raw coordinate units; ``'meters'`` forces SI meters and raises
///     without a CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     One result per input pair.
///
/// Raises
/// ------
/// CRSError
///     If the CRS lacks linear axis units for a metric result.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``unit='meters'`` is requested for a CRS-free geometry.
/// InvalidGeometryError
///     If either operand lacks a Z ordinate on every vertex.
///
/// See Also
/// --------
/// distance : The 2D sibling, with the same ``unit`` override.
/// length_3d : Total 3D length under the same metric.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.distance_3d(gm.from_wkt('POINT Z (0 0 0)'), gm.from_wkt('POINT Z (0 0 3)'))
/// 3.0
#[pyfunction]
#[pyo3(signature = (left, right, *, unit = None))]
pub(crate) fn distance_3d(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_distance_3d(py, left, right, unit)
}

/// Compute the continuous Hausdorff (set-to-set) distance between ``left``
/// and ``right``.
///
/// CRS-aware: geodesic meters on a geographic CRS, native linear units on a
/// projected CRS, coordinate units when CRS-free. Geographic Hausdorff
/// distance supports point and linework operands only; filled polygon
/// interiors require ``unit='planar'`` or a projected CRS. Planar results use
/// a certified relative bound by default or a caller-supplied absolute bound.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
/// max_error : float, optional
///     Positive finite scalar absolute error bound in final result units.
///     Planar results default to a certified relative error of at most
///     ``1e-9``. Omit it for the numerical geodesic Hausdorff path; geodesic
///     results do not provide this certification contract.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (native angular units as Cartesian
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     One result per input pair. A planar result is zero exactly when the
///     operands have equal closed support; unequal positive distances remain
///     positive even when smaller than the effective error bound. An empty
///     operand yields ``inf``, as in ``distance``.
///
/// Raises
/// ------
/// CRSError
///     If the CRS lacks linear axis units for a metric result, or a geographic
///     operand includes a filled polygon interior.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``max_error`` is not a positive finite scalar or is supplied for a
///     geodesic result; if ``unit='meters'`` is requested for a CRS-free
///     geometry; or if a finite planar result cannot be certified within
///     representable output and the available generated-work or memory
///     resources.
///
/// See Also
/// --------
/// distance : Minimum pairwise distance.
/// frechet_distance : Curve-to-curve similarity (linestrings).
/// dwithin : Within-distance test.
/// nearest_points : Closest-pair witness points.
/// shortest_line : Connecting line between closest points.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> a = gm.LineString([(0, 0), (10, 0)])
/// >>> b = gm.LineString([(0, 1), (10, 3)])
/// >>> abs(gm.hausdorff_distance(a, b, max_error=1e-12) - 3.0) <= 1e-12
/// True
#[pyfunction]
#[pyo3(signature = (left, right, *, max_error = None, unit = None))]
pub(crate) fn hausdorff_distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    max_error: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_hausdorff_distance(py, left, right, max_error, unit)
}

/// Discrete Fréchet distance between two linestrings.
///
/// Measured for the CRS like ``distance``: geodesic meters on a geographic
/// CRS, native units on a projected one, coordinate units when CRS-free.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     The two linestrings.
/// densify : float, optional
///     Subdivide every segment into pieces no longer than this
///     fraction of its length (in ``(0, 1]``) before measuring,
///     tightening the discrete vertex metric toward the continuous
///     one. Omitted measures vertices only.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (native angular units as Cartesian
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     One result per input pair.
///
/// Raises
/// ------
/// GeometryError
///     If ``densify`` is outside ``(0, 1]``.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// InvalidGeometryError
///     If the linework is empty.
/// GeometryTypeError
///     If either geometry is not a single line.
///
/// See Also
/// --------
/// distance : Minimum pairwise distance.
/// hausdorff_distance : Hausdorff (set-to-set) similarity distance.
/// dwithin : Within-distance test.
/// nearest_points : Closest-pair witness points.
/// shortest_line : Connecting line between closest points.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> a = gm.LineString([(0, 0), (10, 0)])
/// >>> b = gm.LineString([(0, 1), (10, 1)])
/// >>> gm.frechet_distance(a, b)
/// 1.0
#[pyfunction]
#[pyo3(signature = (left, right, *, densify = None, unit = None))]
pub(crate) fn frechet_distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_frechet_distance(py, left, right, densify, unit)
}

/// Return the closest pair of points between two geometries.
///
/// Realizes the same minimizer as ``distance``: the geodesically closest pair
/// on a geographic CRS, the planar-closest pair on a projected one, and the
/// coordinate-space closest pair when CRS-free. Planar and projected metrics
/// accept any geometry pair, not only points (the witness on a polygon/line may
/// be an edge-interior point). Geographic metrics support point and linework
/// operands only; filled polygon interiors require ``unit='planar'`` or a
/// projected CRS. The connecting line is ``shortest_line``. An empty operand yields
/// ``(POINT EMPTY, POINT EMPTY)``, and the pair is dropped to the operands'
/// common axes so it matches ``shortest_line``'s endpoints exactly.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch. Unpack order is point on ``left``, then on ``right``.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (native angular units as Cartesian
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// tuple of Point or tuple of GeometryArray
///     A ``(left_point, right_point)`` pair for scalar inputs, or a
///     ``(left, right)`` pair of parallel witness point columns when either
///     operand is a `GeometryArray`.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// CRSError
///     If a geographic operand includes a filled polygon interior.
///
/// See Also
/// --------
/// distance : Minimum pairwise distance.
/// shortest_line : Connecting line between closest points.
/// dwithin : Within-distance test.
/// hausdorff_distance : Hausdorff (set-to-set) similarity distance.
/// frechet_distance : Curve-to-curve similarity (linestrings).
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> square = gm.box(0, 0, 1, 1)
/// >>> a, b = gm.nearest_points(square, gm.Point(3, 0.5))
/// >>> (a.to_wkt(), b.to_wkt())
/// ('POINT (1 0.5)', 'POINT (3 0.5)')
#[pyfunction]
#[pyo3(signature = (left, right, *, unit = None))]
pub(crate) fn nearest_points(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let admission = crate::dispatch::admit_binary(
        left,
        right,
        crate::dispatch::MetricOp::NearestPoints.name(),
    )?;
    let model = resolve_metric(
        admission.frame.crs_str(),
        unit,
        crate::dispatch::MetricOp::NearestPoints,
    )?;
    match (admission.left, admission.right) {
        (GeometryInput::One(left), GeometryInput::One(right)) => {
            let common = crate::geometry::common_axes(left.shape.shape(), right.shape.shape());
            let left_plan = left.plan_or_transient()?;
            let right_plan = right.plan_or_transient()?;
            let left_cache = crate::geometry::FrameDependentCaches::default();
            let right_cache = crate::geometry::FrameDependentCaches::default();
            let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
            let pair = py.detach(move || {
                metric_nearest_points_resolved(
                    &resolved,
                    &left_plan,
                    &left_cache,
                    &right_plan,
                    &right_cache,
                )
            })?;
            let (left_shape, right_shape) =
                crate::geometry::nearest_point_pair(pair.as_ref(), common);
            (
                crate::Typed(PyGeometry::with_frame(left_shape, left.frame.clone()))
                    .into_pyobject(py)?
                    .unbind(),
                crate::Typed(PyGeometry::with_frame(right_shape, right.frame.clone()))
                    .into_pyobject(py)?
                    .unbind(),
            )
                .into_py_any(py)
        },
        (GeometryInput::One(fixed), GeometryInput::Many(array)) => {
            fixed_geometry_array_nearest_points_admitted(py, fixed, array, true, model)?
                .into_py_any(py)
        },
        (GeometryInput::Many(array), GeometryInput::One(fixed)) => {
            fixed_geometry_array_nearest_points_admitted(py, fixed, array, false, model)?
                .into_py_any(py)
        },
        (GeometryInput::Many(left), GeometryInput::Many(right)) => {
            let missing = union_many([left.missing(), right.missing()])?;
            let left = left.clone();
            let right = right.clone();
            let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
            let pairs = py
                .detach(move || {
                    let mut left_rows = left.row_operation();
                    let mut right_rows = right.row_operation();
                    left_rows
                        .rows_with_missing(|| {
                            (
                                None::<(crate::geometry::Point, crate::geometry::Point)>,
                                crate::geometry::CoordinateAxes::XY,
                            )
                        })
                        .zip(right_rows.rows_with_missing(|| {
                            (
                                None::<(crate::geometry::Point, crate::geometry::Point)>,
                                crate::geometry::CoordinateAxes::XY,
                            )
                        }))
                        .map(|(left, right)| {
                            let (
                                crate::array::ArrayRow::Present(left),
                                crate::array::ArrayRow::Present(right),
                            ) = (left, right)
                            else {
                                return Ok((None, crate::geometry::CoordinateAxes::XY));
                            };
                            let row = left.logical();
                            let left = left_rows.prepared(left)?;
                            let right = right_rows.prepared(right)?;
                            let left_cache = left_rows.frame_cache(row)?;
                            let right_cache = right_rows.frame_cache(row)?;
                            let common = crate::geometry::common_axes(left.shape(), right.shape());
                            metric_nearest_points_resolved(
                                &resolved,
                                &left,
                                &left_cache,
                                &right,
                                &right_cache,
                            )
                            .map(|pair| (pair, common))
                        })
                        .collect_rows()
                })
                .map_err(crate::broadcast::rows_err)?;
            nearest_point_columns_masked(pairs, admission.frame, missing)
                .and_then(|value| value.into_py_any(py))
        },
    }
}

/// Test whether ``left`` and ``right`` are within ``distance`` of each other.
///
/// Geographic metrics support point and linework operands only; filled polygon
/// interiors require ``unit='planar'`` or a projected CRS.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
/// distance : float or sequence of float
///     CRS-aware distance threshold: geodesic meters on a geographic CRS,
///     native units on a projected one, coordinate units otherwise. A scalar
///     applies to every pair; with an array operand, an iterable supplies one
///     threshold per result row. Positive infinity is accepted.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (native angular units as Cartesian
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     One result per input pair. Array results retain one boolean per input
///     row; missing rows and present empty geometries are ``False`` rather
///     than being dropped. Positive infinity matches only admitted non-empty
///     operands.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``distance`` is negative, NaN, or negative infinity, or
///     ``unit='meters'`` is requested for a CRS-free geometry. Positive
///     infinity is accepted and matches every pair of admitted non-empty
///     operands.
/// CRSError
///     If a geographic operand includes a filled polygon interior.
///
/// See Also
/// --------
/// distance : Minimum pairwise distance.
/// nearest_points : Closest-pair witness points.
/// shortest_line : Connecting line between closest points.
/// hausdorff_distance : Hausdorff (set-to-set) similarity distance.
/// frechet_distance : Curve-to-curve similarity (linestrings).
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> a, b = gm.Point(13.0, 52.0, crs=4326), gm.Point(13.1, 52.0, crs=4326)
/// >>> gm.dwithin(a, b, 7000)
/// True
#[pyfunction]
#[pyo3(signature = (left, right, distance, *, unit = None))]
pub(crate) fn dwithin(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let left_array = exact_geometry_array(left);
    let right_array = exact_geometry_array(right);
    // The per-element distance lane needs an array operand (its result is one
    // bool per row); the broadcast length comes from that operand.
    let Some(len) = (match (left_array, right_array) {
        (Some(array), _) | (None, Some(array)) => Some(array.len()),
        (None, None) => None,
    }) else {
        // Two scalars: a single bool, so the distance must be scalar too.
        let distance = validate_dwithin_distance_arg(distance)?;
        let left_input = classify_required(left)?;
        let right_input = classify_required(right)?;
        let admission = crate::dispatch::admit_binary_inputs(left_input, right_input, "dwithin")?;
        return crate::dispatch::dispatch_dwithin_admitted(py, &admission, distance, unit);
    };
    let distance = crate::F64Param::parse_raw(distance, "distance", len)?;
    distance.try_validate(|value| validate_dwithin_distance(value).map(|_| ()))?;
    let left_input = classify_required(left)?;
    let right_input = classify_required(right)?;
    let admission = crate::dispatch::admit_binary_inputs(left_input, right_input, "dwithin")?;
    let Some(scalar) = distance.as_scalar() else {
        // `dwithin` is symmetric, so route the ARRAY operand as the primary and
        // the other operand (scalar or array) as the broadcast partner.
        return match (admission.left, admission.right) {
            (GeometryInput::Many(array), other) | (other, GeometryInput::Many(array)) => {
                crate::broadcast::array_crs_dwithin_per_element(
                    py,
                    array,
                    other,
                    &admission.frame,
                    &distance,
                    crate::dispatch::MetricOp::Dwithin,
                    unit,
                )
            },
            (GeometryInput::One(_), GeometryInput::One(_)) => {
                unreachable!("the length came from an array operand")
            },
        };
    };
    crate::dispatch::dispatch_dwithin_admitted(
        py,
        &admission,
        validate_dwithin_distance(scalar)?,
        unit,
    )
}
