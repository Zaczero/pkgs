use pyo3::prelude::*;

use crate::dispatch::{MetricCtx, MetricOp, MetricResolver, kernels};
use crate::geometry::publication::GeometryOutput;
use crate::{DistanceUnit, PyGeometry, PyGeometryArray, finite_f64_required};

pub(crate) fn bounds_array(py: Python<'_>, values: &PyGeometryArray) -> PyResult<Py<PyAny>> {
    crate::dispatch::unary_array(
        py,
        values,
        crate::dispatch::Operation::Bounds,
        None,
        None,
        kernels::unary_bounds,
    )
}

/// Compute area in CRS-natural units or with a ``unit`` override.
///
/// For a ``GeometryCollection``, overlapping members count once, as the area
/// of their union. Parts of a valid ``MultiPolygon`` cannot overlap.
///
/// For geographic geometries, area is the ellipsoidal area of the region whose
/// edges are straight in longitude/latitude.
///
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of geometry-like values or None
///     Input geometry, array, or iterable materialized as a row-preserving
///     array. Missing rows return ``nan``.
#[doc = unit_parameter_doc!()]
///
/// Returns
/// -------
/// float or numpy.ndarray
///     Scalar area or one value per row.
///
/// Raises
/// ------
/// CRSError
///     If the CRS lacks linear axis units for a metric result.
/// GeometryError
///     If ``unit='meters'`` is requested for a CRS-free geometry.
///
/// See Also
/// --------
/// area : CRS-natural property form.
/// length : Length/perimeter with the same ``unit`` override.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.area(gm.box(0, 0, 2, 2), unit='planar')
/// 4.0
#[pyfunction]
#[pyo3(signature = (geom, *, unit = None))]
pub(crate) fn area(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    measure_natural_or_override(
        py,
        geom,
        unit,
        crate::dispatch::Operation::Metric(MetricOp::Area),
        kernels::unary_area,
    )
}

pub(crate) fn area_natural_scalar(py: Python<'_>, geom: &PyGeometry) -> PyResult<f64> {
    crate::dispatch::unary_scalar(
        py,
        geom,
        crate::dispatch::Operation::Metric(MetricOp::Area),
        None,
        kernels::unary_area,
    )
}

pub(crate) fn area_natural_array(py: Python<'_>, values: &PyGeometryArray) -> PyResult<Py<PyAny>> {
    crate::dispatch::unary_array(
        py,
        values,
        crate::dispatch::Operation::Metric(MetricOp::Area),
        None,
        None,
        kernels::unary_area,
    )
}

/// The body every `unit=`-override measure free function shares: a scalar or
/// `GeometryArray` dispatches straight through, while a raw iterable is
/// materialized into one array first (the capability that earns these free
/// functions a place beside their property forms).
fn measure_natural_or_override(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
    op: crate::dispatch::Operation,
    kernel: crate::dispatch::UnaryMeasureKernel,
) -> PyResult<Py<PyAny>> {
    if crate::broadcast::classify_input(geom).is_some() {
        return crate::dispatch::dispatch_unary(py, geom, op, unit, None, kernel);
    }
    let array = PyGeometryArray::new(geom, None, None)?;
    crate::dispatch::unary_array(py, &array, op, unit, None, kernel)
}

/// Compute length in CRS-natural units or with a ``unit`` override.
///
/// For a ``GeometryCollection``, length sums every member's length or
/// perimeter, including overlapping members.
///
/// For geographic geometries, length and perimeter are measured along edges
/// straight in longitude/latitude on the CRS ellipsoid.
///
/// Geographic polygon perimeter removes chart-cut copies only: within one
/// ring, antiparallel coincident arcs at different longitude lifts cancel;
/// on the canonical seam meridian, opposite-interior copies cancel across all
/// rings, including a ring with itself. Same-side seam duplicates, off-seam
/// same-lift retraces, and every other stored overlap remain in the perimeter.
/// This seam convention is tied to the canonical seam: rotating the longitude
/// origin can change which arcs are seam cuts.
///
/// Arc identity uses exact canonical longitudes and admitted latitudes; no
/// tolerance is applied. Between the two seam spellings, the longitude step is
/// degenerate; differing latitudes still trace a meridian arc. An explicit
/// one-period edge between non-seam longitudes follows the full circle. Planar
/// and projected perimeter rules are unchanged.
///
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of geometry-like values or None
///     Input geometry, array, or iterable materialized as a row-preserving
///     array. Missing rows return ``nan``.
#[doc = unit_parameter_doc!()]
///
/// Returns
/// -------
/// float or numpy.ndarray
///     Scalar length or one value per row.
///
/// Raises
/// ------
/// CRSError
///     If the CRS lacks linear axis units for a metric result.
/// GeometryError
///     If ``unit='meters'`` is requested for a CRS-free geometry.
///
/// See Also
/// --------
/// length : CRS-natural property form.
/// area : Area with the same ``unit`` override.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.length(gm.LineString([(0, 0), (3, 4)]), unit='planar')
/// 5.0
#[pyfunction]
#[pyo3(signature = (geom, *, unit = None))]
pub(crate) fn length(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    measure_natural_or_override(
        py,
        geom,
        unit,
        crate::dispatch::Operation::Metric(MetricOp::Length),
        kernels::unary_length,
    )
}

/// Compute 3D length in CRS-natural units or with a ``unit`` override.
///
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of geometry-like values or None
///     Input geometry, array, or iterable materialized as a row-preserving
///     array. Missing rows return ``nan``.
#[doc = unit_parameter_doc!()]
///     Geographic CRSs are not supported by 3D Euclidean metrics.
///
/// Returns
/// -------
/// float or numpy.ndarray
///     Scalar 3D length or one value per row.
///
/// Raises
/// ------
/// CRSError
///     If the CRS lacks linear axis units for a metric result.
/// GeometryError
///     If ``unit='meters'`` is requested for a CRS-free geometry.
/// InvalidGeometryError
///     If a scalar geometry lacks Z on every vertex.
///
/// See Also
/// --------
/// length_3d : CRS-natural property form.
/// length : The 2D sibling, with the same ``unit`` override.
/// distance_3d : Pairwise 3D distance under the same metric.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.length_3d(gm.from_wkt('LINESTRING Z (0 0 0, 3 4 0)'), unit='planar')
/// 5.0
#[pyfunction]
#[pyo3(signature = (geom, *, unit = None))]
pub(crate) fn length_3d(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    measure_natural_or_override(
        py,
        geom,
        unit,
        crate::dispatch::Operation::Metric(MetricOp::Length3d),
        kernels::unary_length_3d,
    )
}

pub(crate) fn length_natural_scalar(py: Python<'_>, geom: &PyGeometry) -> PyResult<f64> {
    crate::dispatch::unary_scalar(
        py,
        geom,
        crate::dispatch::Operation::Metric(MetricOp::Length),
        None,
        kernels::unary_length,
    )
}

pub(crate) fn length_natural_array(
    py: Python<'_>,
    values: &PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::unary_array(
        py,
        values,
        crate::dispatch::Operation::Metric(MetricOp::Length),
        None,
        None,
        kernels::unary_length,
    )
}

fn snap_metric_resolver(
    geometry: &Bound<'_, PyAny>,
    geometry_array: Option<&PyGeometryArray>,
) -> MetricResolver {
    let has_ordinates = geometry_array.map_or_else(
        || {
            crate::exact_geometry(geometry).is_none_or(|geometry| {
                let axes = geometry.shape.axes();
                axes.has_z() || axes.has_m()
            })
        },
        |array| array.has_z() || array.has_m(),
    );
    if has_ordinates {
        MetricResolver::GeographicCarry
    } else {
        MetricResolver::None
    }
}

fn snap_prepared(
    left: &crate::geometry::PreparedPlan,
    right: &crate::geometry::PreparedPlan,
    tolerance: f64,
    context: &crate::dispatch::CachelessRowContext<'_>,
) -> crate::error::Result<crate::geometry::Shape> {
    let source = left.shape();
    match context.metric {
        MetricCtx::GeographicCarry { .. } => {
            let metric_access = || {
                context.metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic snap source ordinates require the operation's CRS metric",
                    )
                })
            };
            let source_context =
                crate::geometry::SourceArcContext::new(context.domain, Some(&metric_access));
            source.snap_with_context(right.shape(), tolerance, &source_context)
        },
        MetricCtx::None => source.snap(right.shape(), tolerance),
        MetricCtx::Antimeridian | MetricCtx::Metric { .. } => Err(crate::crs::CrsError::invalid(
            "internal dispatch error: snap requires no metric or geographic carry",
        )),
    }
}

fn snap_output(
    left: &crate::geometry::PreparedPlan,
    right: &crate::geometry::PreparedPlan,
    tolerance: f64,
    context: &crate::dispatch::CachelessRowContext<'_>,
) -> crate::error::Result<GeometryOutput> {
    let output = snap_prepared(left, right, tolerance, context)?;
    match context.domain {
        crate::geometry::TopologyDomain::Cartesian => Ok(GeometryOutput::Shape(output)),
        crate::geometry::TopologyDomain::Geographic(_) if context.lane.is_array() => Ok(
            GeometryOutput::Pending(crate::geometry::geographic_snap_pending_row(
                output,
                context.frame,
                context.lane.row(),
                "snap",
            )?),
        ),
        crate::geometry::TopologyDomain::Geographic(_) => Ok(GeometryOutput::Shape(
            crate::geometry::finish_geographic_snap(output, context.frame.clone())?,
        )),
    }
}

/// Snap vertices of ``geom`` onto ``reference`` within ``tolerance``.
///
/// Each source vertex snaps independently to its nearest reference vertex at
/// or below ``tolerance``; if none is in range, it stays unchanged. An exact
/// computed-distance tie goes to the reference vertex with the smaller X,
/// then Y, using IEEE total order. Multiple source vertices may snap to the
/// same reference vertex.
///
/// On each line segment, every reference vertex whose projection lies in the
/// open segment, whose segment distance is at or below ``tolerance``, and
/// whose distance from both endpoints is greater than ``tolerance`` is
/// inserted at its reference X/Y. Insertions follow segment order; equal
/// projections are ordered by reference X/Y. Snapped source vertices retain
/// their own Z/M. Inserted geographic vertices keep the reference X/Y and use
/// the source edge's ellipsoidal arclength at the exact projection parameter;
/// Cartesian vertices interpolate along the source segment. Consecutive line
/// coordinates with identical X/Y are emitted once,
/// keeping the first. If that leaves only one coordinate, the snapped start
/// and end are retained as two endpoints, which may share X/Y. Closed rings
/// are reclosed.
///
/// Snapping may collapse polygon rings: collapsed holes are dropped and a
/// collapsed shell produces an empty polygon. Snapping does not validate or
/// repair topology, so a snapped polygon may be invalid.
///
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of Geometry or None
///     Geometry whose vertices are moved. Missing rows remain missing.
/// reference : Geometry, GeometryArray, or iterable of Geometry or None
///     Target geometry to snap onto.
/// tolerance : float or sequence of float
///     Maximum finite non-negative snap distance in coordinate space:
///     geographic CRSs use native angular units, projected CRSs use native
///     linear units, and CRS-free geometries use raw coordinate units.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     Snapped result(s), with missing rows preserved.
///
/// Raises
/// ------
/// CRSMismatchError
///     If operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``tolerance`` is invalid.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.snap(gm.Point(0.01, 0), gm.Point(0, 0), 0.1).to_wkt()
/// 'POINT (0 0)'
#[pyfunction]
pub(crate) fn snap(
    geom: &Bound<'_, PyAny>,
    reference: &Bound<'_, PyAny>,
    tolerance: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let geom_array = crate::broadcast::exact_geometry_array(geom);
    let ref_array = crate::broadcast::exact_geometry_array(reference);
    let resolver = snap_metric_resolver(geom, geom_array);
    let Some(len) = geom_array.or(ref_array).map(PyGeometryArray::len) else {
        let tolerance = finite_f64_required("tolerance", tolerance)?;
        return Python::attach(|py| {
            crate::dispatch::dispatch_binary_geometry(
                py,
                geom,
                reference,
                crate::dispatch::Operation::Snap.name(),
                resolver,
                true,
                move |left, right, context| snap_output(left, right, tolerance, context),
            )
        });
    };
    let tolerance = crate::F64Param::parse(tolerance, "tolerance", len)?;
    Python::attach(|py| {
        crate::dispatch::dispatch_binary_geometry(
            py,
            geom,
            reference,
            "snap",
            resolver,
            true,
            move |left, right, context| {
                snap_output(left, right, tolerance.get(context.lane.row()), context)
            },
        )
    })
}
