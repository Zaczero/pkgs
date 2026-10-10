use pyo3::prelude::*;

use crate::{Predicate, xy_predicate};
/// Test whether ``left`` contains ``right`` or contains the point ``(x, y)``.
///
/// Returns ``True`` if no point of ``right`` lies outside ``left`` and the
/// interiors of the two geometries share a point.
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left : Geometry, GeometryArray, or iterable of Geometry or None
///     Container geometry. Array and iterable rows retain their logical
///     positions.
/// right : Geometry, GeometryArray, iterable of Geometry or None, optional
///     Candidate geometry; scalar and sequence operands broadcast pairwise,
///     and two sequences must have equal lengths and compatible frames.
/// x, y : float or sequence of float, keyword-only
///     Raw coordinates in ``left``'s CRS. Supply both instead of ``right``;
///     provide exactly one input mode.
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
/// InvalidGeometryError
///     If ``x`` or ``y`` contains a non-finite coordinate, or the coordinate
///     columns have incompatible lengths.
/// TypeError
///     If ``right`` is combined with ``x`` or ``y``, or if only one coordinate
///     keyword is supplied.
///
/// See Also
/// --------
/// within : Inverse relation.
/// covers : Boundary-inclusive containment.
/// intersects : Boundary-inclusive point test.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> square = gm.box(0, 0, 2, 2)
/// >>> gm.contains(square, gm.Point(1, 1))
/// True
/// >>> gm.contains(square, gm.Point(2, 1))  # boundary: not contained
/// False
/// >>> gm.contains(square, x=1, y=1)
/// True
#[pyfunction]
#[pyo3(signature = (left, right = None, *, x = None, y = None))]
pub(crate) fn contains(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    predicate_or_coordinates(py, left, right, x, y, Predicate::Contains)
}

/// Test whether ``left`` contains ``right`` with no boundary contact.
///
/// Like ``contains``, but ``right`` must lie entirely in the interior of
/// ``left`` — touching the boundary of ``left`` anywhere fails (DE-9IM
/// ``T**FF*FF*``). Evaluated from the exact DE-9IM relation matrix; geographic
/// inputs use the longitude and pole quotient.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Container and candidate rows. Scalar and sequence operands broadcast
///     pairwise; sequences must have equal length and compatible CRS/epoch.
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
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.contains_properly(gm.box(0, 0, 2, 2), gm.Point(1, 1))
/// True
#[pyfunction]
pub(crate) fn contains_properly(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_predicate(py, left, right, Predicate::ContainsProperly)
}

/// Test whether ``left`` lies within ``right``; inverse of ``contains``.
///
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Candidate and container rows. Scalar and sequence operands broadcast
///     pairwise; sequences must have equal length and compatible CRS/epoch.
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
    crate::dispatch::dispatch_predicate(py, left, right, Predicate::Within)
}

/// Test whether ``left`` covers ``right``: every point of ``right`` lies in
/// ``left``.
///
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Container and candidate rows. Scalar and sequence operands broadcast
///     pairwise; sequences must have equal length and compatible CRS/epoch.
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
    crate::dispatch::dispatch_predicate(py, left, right, Predicate::Covers)
}

/// Test whether ``left`` is covered by ``right``; inverse of ``covers``.
///
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Candidate and container rows. Scalar and sequence operands broadcast
///     pairwise; sequences must have equal length and compatible CRS/epoch.
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
    crate::dispatch::dispatch_predicate(py, left, right, Predicate::CoveredBy)
}

/// Test whether ``left`` and ``right`` share any point, or whether ``left``
/// intersects the point ``(x, y)``.
///
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left : Geometry, GeometryArray, or iterable of Geometry or None
///     Geometry being tested. Array and iterable rows retain their logical
///     positions.
/// right : Geometry, GeometryArray, iterable of Geometry or None, optional
///     Geometry compared with ``left``. Scalar and sequence operands broadcast
///     pairwise, and two sequences must have equal lengths and compatible frames.
/// x, y : float or sequence of float, keyword-only
///     Raw coordinates in ``left``'s CRS. Supply both instead of ``right``;
///     provide exactly one input mode.
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
/// InvalidGeometryError
///     If ``x`` or ``y`` contains a non-finite coordinate, or the coordinate
///     columns have incompatible lengths.
/// TypeError
///     If ``right`` is combined with ``x`` or ``y``, or if only one coordinate
///     keyword is supplied.
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
/// >>> gm.intersects(gm.box(0, 0, 2, 2), x=2, y=1)
/// True
#[pyfunction]
#[pyo3(signature = (left, right = None, *, x = None, y = None))]
pub(crate) fn intersects(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    predicate_or_coordinates(py, left, right, x, y, Predicate::Intersects)
}

fn predicate_or_coordinates(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: Option<&Bound<'_, PyAny>>,
    x: Option<&Bound<'_, PyAny>>,
    y: Option<&Bound<'_, PyAny>>,
    predicate: Predicate,
) -> PyResult<Py<PyAny>> {
    match (right, x, y) {
        (Some(right), None, None) => {
            crate::dispatch::dispatch_predicate(py, left, right, predicate)
        },
        (None, Some(x), Some(y)) => {
            let left = crate::normalize_geometry_batch(py, left)?;
            xy_predicate(py, &left, x, y, matches!(predicate, Predicate::Intersects))
        },
        _ => Err(pyo3::exceptions::PyTypeError::new_err(
            "provide either right or both x and y; geometry and coordinate inputs cannot be mixed",
        )),
    }
}

/// Test whether ``left`` and ``right`` share no point; negation of
/// ``intersects``.
///
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Operands broadcast pairwise; sequences must have equal lengths and
///     compatible CRS/epoch frames.
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
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.disjoint(gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3))
/// True
#[pyfunction]
pub(crate) fn disjoint(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::dispatch_predicate(py, left, right, Predicate::Disjoint)
}

/// Test whether ``left`` and ``right`` touch only at boundaries.
///
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Operands broadcast pairwise; sequences must have equal lengths and
///     compatible CRS/epoch frames.
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
    crate::dispatch::dispatch_predicate(py, left, right, Predicate::Touches)
}

/// Test whether ``left`` and ``right`` cross.
///
/// The dimension pair determines the relation. A line crosses a polygon when
/// part of the line interior is inside the polygon and part is outside. Two
/// curves cross when their interiors meet at a point.
///
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Operands broadcast pairwise; sequences must have equal lengths and
///     compatible CRS/epoch frames.
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
    crate::dispatch::dispatch_predicate(py, left, right, Predicate::Crosses)
}

/// Test whether ``left`` and ``right`` overlap (same dimension, partial
/// interior overlap).
///
/// Evaluated from the exact DE-9IM relation matrix; geographic inputs use the
/// longitude and pole quotient.
///
/// Parameters
/// ----------
/// left, right : Geometry, GeometryArray, or iterable of Geometry or None
///     Operands broadcast pairwise; sequences must have equal lengths and
///     compatible CRS/epoch frames.
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
    crate::dispatch::dispatch_predicate(py, left, right, Predicate::Overlaps)
}
