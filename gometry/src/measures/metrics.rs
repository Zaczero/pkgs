#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use super::super::*;

/// Axis-aligned bounds as a matrix (see `GeometryArray.bounds`).
///
/// Parameters
/// ----------
/// values : iterable of Geometry or GeometryArray
///     Input geometry collection.
///
/// Returns
/// -------
/// numpy.ndarray
///     ``(minx, miny, maxx, maxy)`` per row.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.bounds(gm.GeometryArray([gm.box(0, 0, 2, 3)])).tolist()
/// [[0.0, 0.0, 2.0, 3.0]]
pub(crate) fn bounds(py: Python<'_>, values: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if exact_geometry(values).is_some() {
        return Err(PyTypeError::new_err(
            "gm.bounds(values) is bulk-only; use geom.bounds for a single Geometry",
        ));
    }
    if exact_geometry_array(values).is_none() {
        let array = PyGeometryArray::new(values, None, None)?;
        return bounds_array(py, &array);
    }
    crate::dispatch::dispatch_unary(
        py,
        values,
        crate::dispatch::Operation::Bounds,
        None,
        None,
        crate::dispatch::kernels::unary_bounds,
    )
}

pub(crate) fn bounds_array(py: Python<'_>, values: &PyGeometryArray) -> PyResult<Py<PyAny>> {
    crate::dispatch::unary_array(
        py,
        values,
        crate::dispatch::Operation::Bounds,
        None,
        None,
        crate::dispatch::kernels::unary_bounds,
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
#[pyfunction]
#[pyo3(signature = (geom, *, unit = None))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.area(gm.box(0, 0, 2, 2), unit='planar')
/// 4.0
pub(crate) fn area(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    area_natural_or_override(py, geom, unit)
}

pub(crate) fn area_natural_scalar(py: Python<'_>, geom: &PyGeometry) -> PyResult<f64> {
    crate::dispatch::unary_scalar(
        py,
        geom,
        crate::dispatch::Operation::Area,
        None,
        crate::dispatch::kernels::unary_area,
    )
}

pub(crate) fn area_natural_array(py: Python<'_>, values: &PyGeometryArray) -> PyResult<Py<PyAny>> {
    crate::dispatch::unary_array(
        py,
        values,
        crate::dispatch::Operation::Area,
        None,
        None,
        crate::dispatch::kernels::unary_area,
    )
}

fn area_natural_or_override(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    match crate::GeometryValues::parse(geom)? {
        crate::GeometryValues::One(_) | crate::GeometryValues::Array(_) => {
            crate::dispatch::dispatch_unary(
                py,
                geom,
                crate::dispatch::Operation::Area,
                unit,
                None,
                crate::dispatch::kernels::unary_area,
            )
        },
        crate::GeometryValues::Collected(items) => {
            let mut items = items.into_items();
            let frame = crate::Frame::resolve_items(
                &mut items,
                crate::FrameAdoption {
                    crs: None,
                    epoch: None,
                },
                "GeometryArray",
            )?;
            let array = PyGeometryArray::pack_or_mixed(items, frame);
            crate::dispatch::unary_array(
                py,
                &array,
                crate::dispatch::Operation::Area,
                unit,
                None,
                crate::dispatch::kernels::unary_area,
            )
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
#[pyfunction]
#[pyo3(signature = (geom, *, unit = None))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.length(gm.LineString([(0, 0), (3, 4)]), unit='planar')
/// 5.0
pub(crate) fn length(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    length_natural_or_override(py, geom, unit)
}

pub(crate) fn length_natural_scalar(py: Python<'_>, geom: &PyGeometry) -> PyResult<f64> {
    crate::dispatch::unary_scalar(
        py,
        geom,
        crate::dispatch::Operation::Length,
        None,
        crate::dispatch::kernels::unary_length,
    )
}

pub(crate) fn length_natural_array(
    py: Python<'_>,
    values: &PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::unary_array(
        py,
        values,
        crate::dispatch::Operation::Length,
        None,
        None,
        crate::dispatch::kernels::unary_length,
    )
}

fn length_natural_or_override(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    match crate::GeometryValues::parse(geom)? {
        crate::GeometryValues::One(_) | crate::GeometryValues::Array(_) => {
            crate::dispatch::dispatch_unary(
                py,
                geom,
                crate::dispatch::Operation::Length,
                unit,
                None,
                crate::dispatch::kernels::unary_length,
            )
        },
        crate::GeometryValues::Collected(items) => {
            let mut items = items.into_items();
            let frame = crate::Frame::resolve_items(
                &mut items,
                crate::FrameAdoption {
                    crs: None,
                    epoch: None,
                },
                "GeometryArray",
            )?;
            let array = PyGeometryArray::pack_or_mixed(items, frame);
            crate::dispatch::unary_array(
                py,
                &array,
                crate::dispatch::Operation::Length,
                unit,
                None,
                crate::dispatch::kernels::unary_length,
            )
        },
    }
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
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.snap(gm.Point(0.01, 0), gm.Point(0, 0), 0.1).to_wkt()
/// 'POINT (0 0)'
pub(crate) fn snap(
    geom: &Bound<'_, PyAny>,
    reference: &Bound<'_, PyAny>,
    tolerance: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let geom_array = crate::broadcast::exact_geometry_array(geom);
    let ref_array = crate::broadcast::exact_geometry_array(reference);
    let Some(len) = geom_array.or(ref_array).map(|array| array.storage().len()) else {
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
