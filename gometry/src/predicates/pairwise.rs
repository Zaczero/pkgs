use pyo3::IntoPyObjectExt as _;
use pyo3::types::PyAny;

use crate::geometry::publication::GeometryOutput;
use crate::geometry::storage::validity::union_many;
use crate::{
    Bound, CollectRows as _, DistanceUnit, GeometryInput, IntoPyObject as _, NavigationPath, Py,
    PyErr, PyGeometry, PyResult, Python, classify_required, crs, exact_geometry_array,
    fixed_geometry_array_nearest_points_admitted, fixed_geometry_array_shortest_line_geodesic,
    metric_nearest_points_resolved, metric_shortest_line_resolved, nearest_point_columns_masked,
    pyfunction, resolve_metric, validate_dwithin_distance, validate_dwithin_distance_arg,
};

fn shortest_line_array_output(
    metric: &crate::crs::ResolvedMetric,
    left: &crate::geometry::PreparedPlan,
    right: &crate::geometry::PreparedPlan,
    ctx: &crate::dispatch::CachelessRowContext<'_>,
    source_context: &crate::geometry::SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
) -> crate::error::Result<GeometryOutput> {
    let output = metric_shortest_line_resolved(metric, left, right, ctx.domain, source_context)?;
    match ctx.domain {
        crate::geometry::TopologyDomain::Cartesian => Ok(GeometryOutput::Shape(output)),
        crate::geometry::TopologyDomain::Geographic(_) => Ok(GeometryOutput::Pending(
            crate::geometry::geographic_line_pending_row(
                output,
                ctx.frame,
                ctx.lane.row(),
                "shortest_line",
            )?,
        )),
    }
}

/// Return the shortest connecting line between two geometries (CRS-aware).
///
/// ``nearest_points`` as a connecting ``LineString`` — degenerate when the
/// geometries touch. Planar and projected metrics accept any geometry pair, not
/// only points, and the witness is the closest approach, so an endpoint may
/// land on an edge interior rather than a vertex. On a geographic CRS,
/// point-to-point separation uses ellipsoidal geodesics. Shape edges are
/// straight in the CRS-native periodic longitude/latitude chart: admitted
/// ordinary edges use their shortest periodic step, while an explicitly stored
/// full-period circle edge retains its full turn. See the [CRS guide's
/// polar-ring and full-turn-edge rules](../../guide/crs.md#polar-rings-and-full-turn-edges).
/// Filled polygon interiors participate.
/// The returned line follows the chart-edge model. With the CRS-aware
/// geographic metric, its ellipsoidal length is no shorter than the witness
/// separation in exact geometry, subject to binary64 rounding. If
/// ``unit='planar'`` overrides the metric, measure the line with the same
/// ``unit`` when comparing lengths. Publication may add vertices to preserve
/// the computed chart edge; do not assume the line always stores exactly two
/// coordinates. An empty operand yields
/// ``LINESTRING EMPTY`` (the output-type sentinel, like
/// ``distance``'s ``inf``). Output ordinates are the operands' common axes (an
/// XYZM line versus an XY point gives an XY line). See ``nearest_points`` for
/// the witnesses and ``distance`` for their metric separation.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Scalar and sequence operands broadcast pairwise; sequences must have
///     equal length and compatible CRS/epoch frames.
#[doc = unit_parameter_doc!()]
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
        let operation = crate::dispatch::MetricOp::ShortestLine;
        let left = crate::normalize_geometry_batch(py, left)?;
        let right = crate::normalize_geometry_batch(py, right)?;
        let admission = crate::dispatch::admit_binary(&left, &right, operation.name())?;
        let model = resolve_metric(admission.frame.crs_str(), unit, operation)?;
        match (admission.left, admission.right) {
            (GeometryInput::One(geometry), GeometryInput::Many(array)) => {
                if let crs::MetricModel::Geodesic { crs, state, .. } = &model {
                    let result = fixed_geometry_array_shortest_line_geodesic(
                        py, geometry, array, true, crs, state,
                    )?;
                    return Ok(result.into_pyobject(py)?.unbind().into());
                }
                let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
                crate::dispatch::dispatch_binary_geometry_admitted_with_resolver(
                    py,
                    &admission,
                    crate::dispatch::MetricResolver::GeographicCarry,
                    move |left, right, ctx| {
                        ctx.with_source_arc_context(|source_context| {
                            shortest_line_array_output(&resolved, left, right, ctx, source_context)
                        })
                    },
                )
            },
            (GeometryInput::Many(array), GeometryInput::One(geometry)) => {
                if let crs::MetricModel::Geodesic { crs, state, .. } = &model {
                    let result = fixed_geometry_array_shortest_line_geodesic(
                        py, geometry, array, false, crs, state,
                    )?;
                    return Ok(result.into_pyobject(py)?.unbind().into());
                }
                let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
                crate::dispatch::dispatch_binary_geometry_admitted_with_resolver(
                    py,
                    &admission,
                    crate::dispatch::MetricResolver::GeographicCarry,
                    move |left, right, ctx| {
                        ctx.with_source_arc_context(|source_context| {
                            shortest_line_array_output(&resolved, left, right, ctx, source_context)
                        })
                    },
                )
            },
            (GeometryInput::One(left), GeometryInput::One(right)) => {
                let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
                let left_plan = left.prepared_plan();
                let right_plan = right.prepared_plan();
                let domain = admission.domain;
                let source_frame = admission.frame.clone();
                let shape = py.detach(move || {
                    crate::broadcast::with_source_arc_context(
                        &resolved,
                        &source_frame,
                        domain,
                        |source_context| {
                            metric_shortest_line_resolved(
                                &resolved,
                                &left_plan,
                                &right_plan,
                                domain,
                                source_context,
                            )
                        },
                    )
                })?;
                let shape = if domain.is_geographic() {
                    crate::geometry::finish_geographic_line(shape, admission.frame.clone())
                        .map_err(PyErr::from)?
                } else {
                    shape
                };
                crate::Typed(PyGeometry::with_frame(shape, admission.frame)).into_py_any(py)
            },
            (GeometryInput::Many(_), GeometryInput::Many(_)) => {
                let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
                crate::dispatch::dispatch_binary_geometry_admitted_with_resolver(
                    py,
                    &admission,
                    crate::dispatch::MetricResolver::GeographicCarry,
                    move |left, right, ctx| {
                        ctx.with_source_arc_context(|source_context| {
                            shortest_line_array_output(&resolved, left, right, ctx, source_context)
                        })
                    },
                )
            },
        }
    })
}

/// Compute the minimum distance between ``left`` and ``right``.
///
/// CRS-aware: geodesic meters on a geographic CRS, native linear units on a
/// projected CRS, raw coordinate units when CRS-free. On a geographic CRS,
/// point-to-point distances use ellipsoidal geodesics, and shape edges are
/// straight in the CRS's longitude/latitude chart with short-way longitude
/// across the antimeridian. Filled polygon interiors participate. For two
/// points, ``path='rhumb'`` measures the constant-bearing route in meters on a
/// geographic CRS.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Scalar and sequence operands broadcast pairwise; sequences must have
///     equal length and compatible CRS/epoch frames.
/// path : {'geodesic', 'rhumb'}, default varies by call form
///     Route for point operands. Omit for geodesic distance; rhumb distance
///     requires ``path='rhumb'`` and a geographic CRS and uses meters.
#[doc = unit_parameter_doc!()]
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
/// GeometryTypeError
///     If ``path='rhumb'`` is used with non-point operands.
/// CRSError
///     If ``path='rhumb'`` is used without a geographic CRS.
/// GeometryError
///     If ``unit='meters'`` is requested for a CRS-free geometry or a unit
///     override is supplied with ``path='rhumb'``.
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
#[pyo3(
    signature = (left, right, *, path = NavigationPath::Geodesic, unit = None),
    text_signature = "(left, right, *, path='geodesic', unit=None)"
)]
pub(crate) fn distance(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    path: NavigationPath,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    match path {
        NavigationPath::Geodesic => crate::dispatch::dispatch_distance(py, left, right, unit),
        NavigationPath::Rhumb => crate::measures::distance_rhumb(py, left, right, unit),
    }
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
/// raw coordinate units — the same defaults as the 2D ``distance``. A geographic
/// CRS raises under every ``unit``: a Euclidean norm cannot combine degrees
/// with meter heights. Every vertex on both operands must carry a Z ordinate.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Scalar and sequence operands broadcast pairwise; sequences must have
///     equal length and compatible CRS/epoch frames.
#[doc = unit_parameter_doc!()]
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
/// projected CRS, raw coordinate units when CRS-free. Geographic Hausdorff
/// ranges continuously over straight chart edges and supports point and
/// linework operands only; filled polygon interiors require ``unit='planar'``
/// or a projected CRS. Planar results use a certified relative bound by
/// default or a caller-supplied absolute bound.
///
/// Planar polygons are filled sets, including their interiors. For example,
/// a square ``[-2, 2] by [-2, 2]`` and the same square with a diamond hole at
/// ``(0, 1), (1, 0), (0, -1), (-1, 0)`` have filled-set Hausdorff distance
/// ``1 / sqrt(2)``, whereas their boundary Hausdorff distance is ``1.5``.
/// Shapely's discrete and densified Hausdorff forms measure boundaries instead.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Scalar and sequence operands broadcast pairwise; sequences must have
///     equal length and compatible CRS/epoch frames.
/// max_error : float, optional
///     Positive finite scalar absolute error bound in final result units.
///     Planar results default to a certified relative error of at most
///     ``1e-9``. Omit it for geographic Hausdorff, which uses a fixed
///     radius-scaled algorithmic gap rather than a caller-requested bound.
#[doc = unit_parameter_doc!()]
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
///     operand includes a filled polygon interior, or the geographic source
///     frontier cannot close its numerical gap before binary64 parameter
///     refinement stops.
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
/// CRS, native linear units on a projected CRS, raw coordinate units when CRS-free.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     The line operands; scalar and sequence inputs broadcast pairwise.
/// densify : float, optional
///     Subdivide every segment into pieces no longer than this
///     fraction of its length (in ``(0, 1]``) before measuring,
///     tightening the discrete vertex metric toward the continuous
///     one. Omitted measures vertices only.
#[doc = unit_parameter_doc!()]
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
/// Realizes the same minimizer as ``distance``: point-to-point distance is
/// geodesic on a geographic CRS, planar on a projected one, and in coordinate
/// space when CRS-free. Geographic shape edges are straight in the CRS's
/// longitude/latitude chart, using short-way longitude across the antimeridian;
/// filled polygon interiors participate. Planar and projected metrics accept
/// any geometry pair, not only points (the witness on a polygon/line may be an
/// edge-interior point). The connecting line is ``shortest_line``. An empty operand yields
/// ``(POINT EMPTY, POINT EMPTY)``, and the pair is dropped to the operands'
/// common axes so it matches ``shortest_line``'s endpoints exactly.
/// Equal-distance witnesses prefer an interior projection over a point or
/// endpoint, then source order. Geographic metrics apply this order only to
/// equal evaluated backend distances; analytic branches retain their available
/// candidate. This does not assert an exact geodesic minimum.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Scalar and sequence operands broadcast pairwise; sequences must have
///     equal length and compatible CRS/epoch frames. Unpack order is point on
///     ``left``, then on ``right``.
#[doc = unit_parameter_doc!()]
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
    let left = crate::normalize_geometry_batch(py, left)?;
    let right = crate::normalize_geometry_batch(py, right)?;
    let admission = crate::dispatch::admit_binary(
        &left,
        &right,
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
            let left_plan = left.prepared_plan();
            let right_plan = right.prepared_plan();
            let left_cache = left.frame_cache();
            let right_cache = right.frame_cache();
            let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
            let pair = if common.has_z() || common.has_m() {
                let source_frame = admission.frame.clone();
                let domain = admission.domain;
                py.detach(move || {
                    crate::broadcast::with_source_arc_context(
                        &resolved,
                        &source_frame,
                        domain,
                        |source_context| {
                            metric_nearest_points_resolved(
                                &resolved,
                                &left_plan,
                                &left_cache,
                                &right_plan,
                                &right_cache,
                                Some(source_context),
                            )
                        },
                    )
                })?
            } else {
                py.detach(move || {
                    metric_nearest_points_resolved(
                        &resolved,
                        &left_plan,
                        &left_cache,
                        &right_plan,
                        &right_cache,
                        None,
                    )
                })?
            };
            let (left_shape, right_shape) =
                crate::geometry::nearest_point_pair(pair.as_ref(), common);
            let (left_shape, right_shape) = if admission.domain.is_geographic() {
                (
                    crate::geometry::finish_geographic_nearest_point(
                        left_shape,
                        left.frame.clone(),
                    )
                    .map_err(PyErr::from)?,
                    crate::geometry::finish_geographic_nearest_point(
                        right_shape,
                        right.frame.clone(),
                    )
                    .map_err(PyErr::from)?,
                )
            } else {
                (left_shape, right_shape)
            };
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
            fixed_geometry_array_nearest_points_admitted(
                py,
                fixed,
                array,
                true,
                model,
                admission.domain,
            )?
            .into_py_any(py)
        },
        (GeometryInput::Many(array), GeometryInput::One(fixed)) => {
            fixed_geometry_array_nearest_points_admitted(
                py,
                fixed,
                array,
                false,
                model,
                admission.domain,
            )?
            .into_py_any(py)
        },
        (GeometryInput::Many(left), GeometryInput::Many(right)) => {
            let missing = union_many([left.missing(), right.missing()]);
            let left = left.clone();
            let right = right.clone();
            let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
            let source_frame = admission.frame.clone();
            let domain = admission.domain;
            let pairs = py
                .detach(move || {
                    crate::broadcast::with_source_arc_context(
                        &resolved,
                        &source_frame,
                        domain,
                        |source_context| {
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
                                    let left_cache = left_rows.frame_cache(row);
                                    let right_cache = right_rows.frame_cache(row);
                                    let common =
                                        crate::geometry::common_axes(left.shape(), right.shape());
                                    metric_nearest_points_resolved(
                                        &resolved,
                                        &left,
                                        &left_cache,
                                        &right,
                                        &right_cache,
                                        (common.has_z() || common.has_m())
                                            .then_some(source_context),
                                    )
                                    .map(|pair| (pair, common))
                                })
                                .collect_rows()
                        },
                    )
                })
                .map_err(crate::broadcast::rows_err)?;
            nearest_point_columns_masked(pairs, admission.frame, missing, admission.domain)
                .and_then(|value| value.into_py_any(py))
        },
    }
}

/// Test whether ``left`` and ``right`` are within ``distance`` of each other.
///
/// On a geographic CRS, point-to-point distances use ellipsoidal geodesics, and
/// shape edges are straight in the CRS's longitude/latitude chart with
/// short-way longitude across the antimeridian. Filled polygon interiors
/// participate.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Scalar and sequence operands broadcast pairwise; sequences must have
///     equal length and compatible CRS/epoch frames.
/// distance : float or sequence of float
///     CRS-aware distance threshold: geodesic meters on a geographic CRS,
///     native linear units on a projected CRS, raw coordinate units when
///     CRS-free. A scalar
///     applies to every pair; with an array operand, an iterable supplies one
///     threshold per result row. Positive infinity is accepted.
#[doc = unit_parameter_doc!()]
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
///     If geographic point-edge search exhausts its work allowance (a
///     binary64 midpoint-depth factor times the current live frontier, subject
///     to a 16,384-evaluation ceiling) or parameter precision ends without
///     certifying the threshold.
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
    let left = crate::normalize_geometry_batch(py, left)?;
    let right = crate::normalize_geometry_batch(py, right)?;
    let left_array = exact_geometry_array(&left);
    let right_array = exact_geometry_array(&right);
    // The per-element distance lane needs an array operand (its result is one
    // bool per row); the broadcast length comes from that operand.
    let Some(len) = (match (left_array, right_array) {
        (Some(array), _) | (None, Some(array)) => Some(array.len()),
        (None, None) => None,
    }) else {
        // Two scalars: a single bool, so the distance must be scalar too.
        let distance = validate_dwithin_distance_arg(distance)?;
        let left_input = classify_required(&left)?;
        let right_input = classify_required(&right)?;
        let admission = crate::dispatch::admit_binary_inputs(left_input, right_input, "dwithin")?;
        return crate::dispatch::dispatch_dwithin_admitted(py, &admission, distance, unit);
    };
    let distance = crate::F64Param::parse_raw(distance, "distance", len)?;
    distance.try_validate(|value| validate_dwithin_distance(value).map(|_| ()))?;
    let left_input = classify_required(&left)?;
    let right_input = classify_required(&right)?;
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
