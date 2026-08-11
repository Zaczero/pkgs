use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{
    Frame, PyGeometryArray, broadcast2_geometry, exact_geometry, exact_geometry_array,
    expected_geometry_or_array, multipoint_splitter_from_array,
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
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.shared_paths(gm.LineString([(0, 0), (2, 0)]), gm.LineString([(1, 0), (3, 0)])).to_wkt()
/// 'GEOMETRYCOLLECTION (MULTILINESTRING ((1 0, 2 0)), MULTILINESTRING EMPTY)'
pub(crate) fn shared_paths(
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        broadcast2_geometry(py, left, right, "shared_paths", |left, right| {
            left.shape().shared_paths(right.shape())
        })
    })
}

/// Split lineal geometry by point splitter(s).
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     Lineal geometry to split.
/// splitter : Geometry or GeometryArray
///     Point or multipoint cutter.
/// tolerance : float, keyword-only, optional
///     Coordinate-space distance within which a splitter point counts as
///     on the line and near-equal cut offsets collapse. The default ``0.0``
///     is exact topological membership (a point splits only when it lies
///     exactly on the linework) with identity deduplication.
///
/// Returns
/// -------
/// GeometryArray
///     Split parts.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If ``tolerance`` is negative or non-finite.
/// CRSMismatchError
///     If operands' CRS or coordinate-epoch metadata differ.
/// GeometryTypeError
///     If ``geom`` is not lineal.
#[pyfunction]
#[pyo3(signature = (geom, splitter, *, tolerance = 0.0))]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.split(gm.LineString([(0, 0), (2, 0)]), gm.Point(1, 0)).to_wkt()
/// ['LINESTRING (0 0, 1 0)', 'LINESTRING (1 0, 2 0)']
pub(crate) fn split(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    splitter: &Bound<'_, PyAny>,
    tolerance: f64,
) -> PyResult<PyGeometryArray> {
    let tolerance = crate::NonNegative::try_new("tolerance", tolerance)?.get();
    if let Some(geometry) = exact_geometry(geom) {
        if let Some(splitter) = exact_geometry(splitter) {
            return geometry.split(splitter, tolerance);
        }
        if let Some(splitters) = exact_geometry_array(splitter) {
            Frame::compatible_parts(
                geometry.crs_ref(),
                geometry.epoch(),
                splitters.crs_ref(),
                splitters.epoch(),
                "split",
            )?;
            let splitter_shape = multipoint_splitter_from_array(splitters)?;
            return Ok(PyGeometryArray::pack_or_mixed(
                geometry
                    .shape
                    .split(&splitter_shape, tolerance)?
                    .into_iter()
                    .map(|shape| geometry.with_shape(shape))
                    .collect(),
                geometry.frame.clone(),
            ));
        }
    }
    if let Some(array) = exact_geometry_array(geom) {
        return array.split(py, splitter, tolerance);
    }
    Err(expected_geometry_or_array())
}
