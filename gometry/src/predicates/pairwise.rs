use pyo3::IntoPyObjectExt as _;
use pyo3::types::PyAny;

use crate::{
    Arc, Bound, CollectRows as _, DistanceUnit, Frame, IntoPyObject as _, Py, PyGeometryArray,
    PyResult, Python, crs, crs_metric_binary_geometry_broadcast, exact_geometry,
    exact_geometry_array, expected_geometry_or_array, fixed_geometry_array_nearest_points,
    metric_shortest_line, py_nearest_points, pyfunction, resolve_metric, rows_err,
    validate_distance_arg,
};

/// Return the shortest connecting line between two geometries (CRS-aware).
///
/// ``nearest_points`` as a ``LineString`` — degenerate when the geometries
/// touch. Accepts any geometry pair (point/line/polygon/multi/collection), not
/// only points, and the witness is the true closest approach, so an endpoint
/// may land on an edge interior rather than a vertex. An empty operand yields
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
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
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
        if let (Some(geometry), Some(array)) = (exact_geometry(left), exact_geometry_array(right)) {
            Frame::compatible_parts(
                geometry.crs_ref(),
                geometry.epoch(),
                array.crs_ref(),
                array.epoch(),
                "shortest_line",
            )?;
            let model = resolve_metric(geometry.crs_str(), unit, "shortest_line")?;
            // A crossing scalar can't share one prepared cache across the seam —
            // fall through to the split-normalizing broadcast below for it.
            if let crs::MetricModel::Geodesic(crs) = model
                && !geometry.shape.shape().crosses_antimeridian()
                && !array.has_missing()
            {
                let scalar = Arc::clone(&geometry.shape);
                let scalar_cache = Arc::clone(&geometry.frame_cache);
                let array = array.clone();
                let rows = py.detach(move || {
                    crs::with_resolved_ellipsoid_metric(&crs, &[scalar.shape()], |crs, metric| {
                        let (semi_major, flattening) = metric.ellipsoid_parameters();
                        scalar.prepare_geodesic_parts(
                            &scalar_cache,
                            crs,
                            semi_major,
                            flattening,
                            metric,
                        )?;
                        Ok(array
                            .storage()
                            .iter_rows()
                            .enumerate()
                            .map(|(row_index, row)| {
                                let element = array.prepared_row(row_index, row);
                                let element_cache = array.row_frame_cache(row_index);
                                Ok(crate::geometry::nearest_line(
                                    scalar.geodesic_nearest_points_cached_split(
                                        &scalar_cache,
                                        &element,
                                        &element_cache,
                                        crs,
                                        semi_major,
                                        flattening,
                                        metric,
                                    )?,
                                    crate::geometry::common_axes(scalar.shape(), element.shape()),
                                ))
                            })
                            .collect_rows())
                    })
                })?;
                let result =
                    PyGeometryArray::from_shapes(rows.map_err(rows_err)?, geometry.frame.clone());
                return Ok(result.into_pyobject(py)?.unbind().into());
            }
        }
        if let Some(array) = exact_geometry_array(left) {
            let model = resolve_metric(array.crs_str(), unit, "shortest_line")?;
            if let crs::MetricModel::Geodesic(_) = model {
                let result = array.shortest_line(right, unit)?;
                return Ok(result.into_pyobject(py)?.unbind().into());
            }
        }
        crs_metric_binary_geometry_broadcast(
            py,
            left,
            right,
            "shortest_line",
            unit,
            metric_shortest_line,
        )
    })
}

/// Compute the minimum distance between ``left`` and ``right``.
///
/// CRS-aware: geodesic meters on a geographic CRS, native linear units on a
/// projected CRS, coordinate units when CRS-free.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
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
///     If the CRS lacks linear axis units for a metric result.
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
/// and have no 3D form; ``bounds_3d`` gives the 3D extent (there is no
/// ``envelope_3d``).
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
#[pyfunction]
#[pyo3(signature = (left, right, *, unit = None))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.distance_3d(gm.from_wkt('POINT Z (0 0 0)'), gm.from_wkt('POINT Z (0 0 3)'))
/// 3.0
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
/// projected CRS, coordinate units when CRS-free.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
/// densify : float, optional
///     Subdivide every segment into pieces no longer than this fraction of
///     its length (in ``(0, 1]``) before measuring. The metric remains the
///     continuous Hausdorff distance; gometry does not offer a discrete,
///     vertex-only Hausdorff variant.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
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
///     If the CRS lacks linear axis units for a metric result.
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``densify`` is outside ``(0, 1]``, or ``unit='meters'`` is
///     requested for a CRS-free geometry.
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
/// >>> gm.hausdorff_distance(a, b)
/// 3.0
#[pyfunction]
#[pyo3(signature = (left, right, *, densify = None, unit = None))]
pub(crate) fn hausdorff_distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    densify: Option<&Bound<'_, PyAny>>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_hausdorff_distance(py, left, right, densify, unit)
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
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
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
/// coordinate-space closest pair when CRS-free. Accepts any geometry pair, not
/// only points (the witness on a polygon/line may be an edge-interior point);
/// the connecting line is ``shortest_line``. An empty operand yields
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
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
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
    if let Some(geometry) = exact_geometry(left) {
        if let Some(right_geometry) = exact_geometry(right) {
            return Ok(py_nearest_points(geometry, right_geometry, unit)?
                .into_pyobject(py)?
                .unbind()
                .into());
        }
        if let Some(right_array) = exact_geometry_array(right) {
            return fixed_geometry_array_nearest_points(py, geometry, right_array, true, unit)?
                .into_py_any(py);
        }
    }
    if let Some(array) = exact_geometry_array(left) {
        return array.nearest_points(py, right, unit)?.into_py_any(py);
    }
    Err(expected_geometry_or_array())
}

/// Test whether ``left`` and ``right`` are within ``distance`` of each other.
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
///     threshold per result row.
/// unit : {'planar', 'meters'}, default None
///     Omitted follows the CRS: geodesic meters on a geographic CRS, native
///     units on a projected one, coordinate units without a CRS.
///     ``'planar'`` forces raw coordinate units (degrees-as-Cartesian on a
///     geographic CRS — only for deliberate coordinate-space math);
///     ``'meters'`` forces the CRS metric and raises without a CRS.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     One result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``distance`` is negative or non-finite, or ``unit='meters'`` is
///     requested for a CRS-free geometry.
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
    if let Ok(prepared) = left.cast::<crate::PyPreparedGeometry>() {
        return Python::attach(|py| {
            let geometry = prepared.get().geometry.clone().into_pyobject(py)?;
            dwithin(py, geometry.as_any(), right, distance, unit)
        });
    }
    if let Ok(prepared) = right.cast::<crate::PyPreparedGeometry>() {
        return Python::attach(|py| {
            let geometry = prepared.get().geometry.clone().into_pyobject(py)?;
            dwithin(py, left, geometry.as_any(), distance, unit)
        });
    }
    // The per-element distance lane needs an array operand (its result is one
    // bool per row); the broadcast length comes from that operand.
    let left_array = crate::broadcast::exact_geometry_array(left);
    let right_array = crate::broadcast::exact_geometry_array(right);
    let Some(len) = left_array
        .or(right_array)
        .map(|array| array.storage().len())
    else {
        // Two scalars: a single bool, so the distance must be scalar too.
        let distance = validate_distance_arg(distance)?.get();
        return crate::dispatch::dispatch_dwithin(py, left, right, distance, unit);
    };
    let distance = crate::F64Param::parse(distance, "distance", len)?;
    distance.try_validate(|value| crate::validate_distance(value).map(|_| ()))?;
    let Some(scalar) = distance.as_scalar() else {
        // `dwithin` is symmetric, so route the ARRAY operand as the primary and
        // the other operand (scalar or array) as the broadcast partner.
        return match (left_array, right_array) {
            (Some(array), _) => crate::broadcast::array_crs_dwithin_per_element(
                py, array, right, &distance, "dwithin", unit,
            ),
            (None, Some(array)) => crate::broadcast::array_crs_dwithin_per_element(
                py, array, left, &distance, "dwithin", unit,
            ),
            (None, None) => unreachable!("the length came from an array operand"),
        };
    };
    crate::dispatch::dispatch_dwithin(py, left, right, scalar, unit)
}
