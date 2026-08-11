use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::{Predicate, predicate_broadcast, xy_predicate};
/// Test whether ``left`` contains ``right``.
///
/// Returns ``True`` if no points of ``right`` lie outside ``left`` and at least
/// one interior point of ``right`` lies in the interior of ``left``.
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Container (``left``) and candidate (``right``). Scalar and
///     ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
///     epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// within : Inverse relation.
/// covers : Boundary-inclusive containment.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> square = gm.box(0, 0, 2, 2)
/// >>> gm.contains(square, gm.Point(1, 1))
/// True
/// >>> gm.contains(square, gm.Point(2, 1))  # boundary: not contained
/// False
#[pyfunction]
pub(crate) fn contains(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Contains)
}

/// Test whether ``left`` contains ``right`` with no boundary contact.
///
/// Like ``contains``, but ``right`` must lie entirely in the interior of
/// ``left`` — touching the boundary of ``left`` anywhere fails (DE-9IM
/// ``T**FF*FF*``). Evaluated in the coordinate plane; geographic inputs crossing the antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Container (``left``) and candidate (``right``). Scalar and
///     ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
///     epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// contains : Boundary contact allowed.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.contains_properly(gm.box(0, 0, 2, 2), gm.Point(1, 1))
/// True
pub(crate) fn contains_properly(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::ContainsProperly)
}

/// Test whether ``left`` lies within ``right``; inverse of ``contains``.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Candidate (``left``) and container (``right``). Scalar and
///     ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
///     epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// contains : Inverse relation (container first).
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.within(gm.Point(1, 1), gm.box(0, 0, 2, 2))
/// True
#[pyfunction]
pub(crate) fn within(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Within)
}

/// Test whether ``left`` covers ``right``: every point of ``right`` lies in
/// ``left``.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Container (``left``) and candidate (``right``). Scalar and
///     ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
///     epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// covered_by : Inverse relation.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> square = gm.box(0, 0, 2, 2)
/// >>> gm.covers(square, gm.Point(2, 1))  # boundary counts
/// True
#[pyfunction]
pub(crate) fn covers(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Covers)
}

/// Test whether ``left`` is covered by ``right``; inverse of ``covers``.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Candidate (``left``) and container (``right``). Scalar and
///     ``GeometryArray`` broadcast pairwise; must share CRS and coordinate
///     epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// covers : Inverse relation.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.covered_by(gm.Point(2, 1), gm.box(0, 0, 2, 2))  # boundary counts
/// True
#[pyfunction]
pub(crate) fn covered_by(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::CoveredBy)
}

/// Test whether a geometry contains each ``(x, y)`` point (vectorized).
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     The geometry row(s) to test against.
/// x, y : float or sequence of float
///     Finite coordinates in ``geom``'s CRS. Geographic antimeridian seams
///     and poles use the same topology as point-geometry predicates.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     A single bool for scalar geometry and coordinates, otherwise one result
///     per broadcast row.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If ``x``/``y`` are non-finite or differ in length.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.contains_xy(gm.box(0, 0, 2, 2), 1, 1)
/// True
pub(crate) fn contains_xy(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    xy_predicate(py, geom, x, y, false)
}

/// Test whether a geometry intersects each ``(x, y)`` point (vectorized).
///
/// Boundary-inclusive (unlike ``contains_xy``), and skips building point
/// geometries.
///
/// Parameters
/// ----------
/// geom : Geometry or GeometryArray
///     The geometry row(s) to test against.
/// x, y : float or sequence of float
///     Finite coordinates in ``geom``'s CRS. Geographic antimeridian seams
///     and poles use the same topology as point-geometry predicates.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     A single bool for scalar geometry and coordinates, otherwise one result
///     per broadcast row.
///
/// Raises
/// ------
/// InvalidGeometryError
///     If ``x``/``y`` are non-finite or differ in length.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.intersects_xy(gm.box(0, 0, 2, 2), 3, 3)
/// False
pub(crate) fn intersects_xy(
    py: Python<'_>,
    geom: &Bound<'_, PyAny>,
    x: &Bound<'_, PyAny>,
    y: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    xy_predicate(py, geom, x, y, true)
}

/// Test whether ``left`` and ``right`` share any point.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the two geometries share any point; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// disjoint : Logical negation.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.intersects(gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3))
/// True
#[pyfunction]
pub(crate) fn intersects(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Intersects)
}

/// Test whether ``left`` and ``right`` share no point; negation of
/// ``intersects``.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// See Also
/// --------
/// intersects : Logical negation.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.disjoint(gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3))
/// True
pub(crate) fn disjoint(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Disjoint)
}

/// Test whether ``left`` and ``right`` touch only at boundaries.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.touches(gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1))
/// True
#[pyfunction]
pub(crate) fn touches(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Touches)
}

/// Test whether ``left`` and ``right`` cross (interiors meet with lower
/// dimension).
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> rising = gm.LineString([(0, 0), (2, 2)])
/// >>> falling = gm.LineString([(0, 2), (2, 0)])
/// >>> gm.crosses(rising, falling)
/// True
#[pyfunction]
pub(crate) fn crosses(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Crosses)
}

/// Test whether ``left`` and ``right`` overlap (same dimension, partial
/// interior overlap).
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the relation holds; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.overlaps(gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3))
/// True
#[pyfunction]
pub(crate) fn overlaps(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Overlaps)
}
