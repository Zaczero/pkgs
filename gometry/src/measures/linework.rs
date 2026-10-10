use pyo3::prelude::*;

use crate::{
    Frame, PyGeometry, broadcast2_geometry, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, splitter_points_from_array,
};

/// Shared paths between two lineal geometries.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Two lineal geometries.
///
/// Returns
/// -------
/// Geometry or GeometryArray
///     The shared linework.
///
/// Raises
/// ------
/// CRSMismatchError
///     If operands' CRS or coordinate-epoch metadata differ.
/// GeometryTypeError
///     If either operand is not lineal.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.shared_paths(gm.LineString([(0, 0), (2, 0)]), gm.LineString([(1, 0), (3, 0)])).to_wkt()
/// 'GEOMETRYCOLLECTION (MULTILINESTRING ((1 0, 2 0)), MULTILINESTRING EMPTY)'
#[pyfunction]
pub(crate) fn shared_paths(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        broadcast2_geometry(py, left, right, "shared_paths", |left, right, ctx| {
            let metric_access = || {
                ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic shared_paths source ordinates require the operation's CRS metric",
                    )
                })
            };
            let context = crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access));
            left.shape()
                .shared_paths_with_context(right.shape(), &context)
        })
    })
}

/// Split lineal geometry by point splitter(s).
///
/// Each piece retains source vertices, including consecutive equal-X/Y
/// vertices whose active Z/M ordinates differ.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Lineal geometry to split.
/// splitter : Geometry or GeometryArray
///     Point or multipoint cutter.
/// tolerance : float, keyword-only, optional
///     Coordinate-space distance within which a splitter point counts as
///     on the line and near-equal cut offsets collapse: native angular units
///     on geographic CRSs, native linear units on projected CRSs, and raw
///     coordinate units when CRS-free. The default ``0.0``
///     is exact topological membership (a point splits only when it lies
///     exactly on the linework) with identity deduplication.
///
/// Returns
/// -------
/// GeometryArray or Groups[GeometryArray]
///     A scalar line returns all split parts in one `GeometryArray`, even when
///     its cutter is an array of points. A line array returns one `Groups` row
///     per source line; an array cutter is paired row by row. Missing line or
///     cutter rows produce empty groups.
///
/// Raises
/// ------
/// GeometryError
///     If ``tolerance`` is negative or non-finite.
/// CRSMismatchError
///     If operands' CRS or coordinate-epoch metadata differ.
/// GeometryTypeError
///     If ``geom`` is not lineal.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.split(gm.LineString([(0, 0), (2, 0)]), gm.Point(1, 0)).to_wkt()
/// ['LINESTRING (0 0, 1 0)', 'LINESTRING (1 0, 2 0)']
/// >>> pieces = gm.split(gm.GeometryArray([gm.LineString([(0, 0), (2, 0)])]), gm.Point(1, 0))
/// >>> pieces.offsets.tolist()
/// [0, 2]
#[pyfunction]
#[pyo3(signature = (geom, splitter, *, tolerance = 0.0))]
pub(crate) fn split(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    splitter: &Bound<'_, PyAny>,
    tolerance: f64,
) -> PyResult<Py<PyAny>> {
    let tolerance = crate::NonNegative::try_new("tolerance", tolerance)?.get();
    if let Some(geometry) = exact_geometry(geom) {
        if let Some(splitter) = exact_geometry(splitter) {
            return Ok(geometry
                .split(py, splitter, tolerance)?
                .into_pyobject(py)?
                .unbind()
                .into());
        }
        if let Some(splitters) = exact_geometry_array(splitter) {
            Frame::compatible_parts(
                geometry.crs_ref(),
                geometry.epoch(),
                splitters.crs_ref(),
                splitters.epoch(),
                "split",
            )?;
            let splitter_points = splitter_points_from_array(splitters)?;
            return PyGeometry::split_shape_detached(
                py,
                std::sync::Arc::clone(&geometry.shape),
                splitter_points,
                geometry.frame.clone(),
                tolerance,
            )?
            .into_pyobject(py)
            .map(|array| array.unbind().into());
        }
    }
    if let Some(array) = exact_geometry_array(geom) {
        return Ok(array
            .split(py, splitter, tolerance)?
            .into_pyobject(py)?
            .unbind()
            .into());
    }
    Err(expected_geometry_or_array())
}
