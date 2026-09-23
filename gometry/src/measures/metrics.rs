use pyo3::prelude::*;

use crate::dispatch::{MetricOp, kernels};
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
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of geometry-like values
///     Input geometry, array, or iterable materialized as an array.
/// unit : {'planar', 'meters'} or None, default None
///     ``None`` follows the geometry's CRS, exactly like ``geom.area``.
///     ``'planar'`` forces raw coordinate units; ``'meters'`` forces the CRS
///     metric and raises without a CRS.
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
    match crate::GeometryValues::parse(geom)? {
        crate::GeometryValues::One(_) | crate::GeometryValues::Array(_) => {
            crate::dispatch::dispatch_unary(py, geom, op, unit, None, kernel)
        },
        crate::GeometryValues::Collected(mut items) => {
            let frame = crate::Frame::resolve_items(
                &mut items,
                crate::FrameAdoption {
                    crs: None,
                    epoch: None,
                },
                "GeometryArray",
            )?;
            let array = PyGeometryArray::pack_or_mixed(items, frame)?;
            crate::dispatch::unary_array(py, &array, op, unit, None, kernel)
        },
    }
}

/// Compute length in CRS-natural units or with a ``unit`` override.
///
/// Parameters
/// ----------
/// geom : Geometry, GeometryArray, or iterable of geometry-like values
///     Input geometry, array, or iterable materialized as an array.
/// unit : {'planar', 'meters'} or None, default None
///     ``None`` follows the geometry's CRS, exactly like ``geom.length``.
///     ``'planar'`` forces raw coordinate units; ``'meters'`` forces the CRS
///     metric and raises without a CRS.
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
/// geom : Geometry, GeometryArray, or iterable of geometry-like values
///     Input geometry, array, or iterable materialized as an array.
/// unit : {'planar', 'meters'} or None, default None
///     ``None`` follows the geometry's CRS, exactly like ``geom.length_3d``.
///     ``'planar'`` forces raw coordinate units; ``'meters'`` forces the CRS
///     metric and raises without a CRS.
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

/// Snap vertices of ``geom`` onto ``reference`` within ``tolerance``.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Geometry whose vertices are moved.
/// reference : Geometry or GeometryArray
///     Target geometry to snap onto.
/// tolerance : float or sequence of float
///     Maximum snap distance in coordinate units.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     Snapped result(s).
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
    let Some(len) = geom_array.or(ref_array).map(PyGeometryArray::len) else {
        let tolerance = finite_f64_required("tolerance", tolerance)?;
        return Python::attach(|py| {
            crate::dispatch::dispatch_binary(
                py,
                geom,
                reference,
                crate::dispatch::Operation::Snap.name(),
                crate::dispatch::MetricResolver::None,
                &crate::dispatch::NoBinaryFastPath,
                move |left, right, _ctx| left.shape().snap(right.shape(), tolerance),
            )
        });
    };
    let tolerance = crate::F64Param::parse(tolerance, "tolerance", len)?;
    Python::attach(|py| {
        let Some(scalar) = tolerance.as_scalar() else {
            use crate::broadcast::{GeometryInput, classify_required};
            let left_in = classify_required(geom)?;
            if let GeometryInput::Many(left) = left_in {
                let right = classify_required(reference)?;
                let array = crate::dispatch::geometry_kernel_over_array(
                    py,
                    left,
                    right,
                    "snap",
                    crate::dispatch::MetricResolver::None,
                    move |left, right, ctx| {
                        left.shape()
                            .snap(right.shape(), tolerance.get(ctx.lane.row()))
                    },
                )?;
                return Ok(array.into_pyobject(py)?.unbind().into());
            }
            return crate::dispatch::dispatch_binary_geometry(
                py,
                geom,
                reference,
                "snap",
                &crate::dispatch::NoBinaryFastPath,
                move |left, right, ctx| {
                    left.shape()
                        .snap(right.shape(), tolerance.get(ctx.lane.row()))
                },
            );
        };
        crate::dispatch::dispatch_binary(
            py,
            geom,
            reference,
            crate::dispatch::Operation::Snap.name(),
            crate::dispatch::MetricResolver::None,
            &crate::dispatch::NoBinaryFastPath,
            move |left, right, _ctx| left.shape().snap(right.shape(), scalar),
        )
    })
}
