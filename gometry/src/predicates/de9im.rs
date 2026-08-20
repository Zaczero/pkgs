#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObject as _;
use pyo3::types::PyAny;

use crate::{
    Bound, DefaultedF64Input, Predicate, Py, PyResult, Python, broadcast2, finite_f64_required,
    predicate_broadcast, pyfunction, relate_pattern_broadcast, relate_string_broadcast,
    validate_equals_exact_tolerance,
};
/// Compute the DE-9IM intersection matrix string for ``left`` and
/// ``right``.
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
/// str or list of str
///     The nine-character DE-9IM pattern; one per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.relate(gm.box(0, 0, 2, 2), gm.Point(1, 1))
/// '0F2FF1FF2'
#[pyfunction]
pub(crate) fn relate(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    relate_string_broadcast(py, left, right)
}

/// Test whether two geometries' DE-9IM matrix matches a pattern.
///
/// Evaluated in the coordinate plane; geographic inputs crossing the
/// antimeridian are split-normalized first.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     Scalar and ``GeometryArray`` broadcast pairwise; must share CRS and
///     coordinate epoch.
/// pattern : str
///     A DE-9IM pattern string (``T``/``F``/``*``/``0``/``1``/``2`` per cell).
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the matrix matches; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``pattern`` is not a 9-character DE-9IM pattern.
#[pyfunction]
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.relate_pattern(gm.box(0, 0, 1, 1), gm.box(0.5, 0.5, 1.5, 1.5), 'T*T***T**')
/// True
pub(crate) fn relate_pattern(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    pattern: &str,
) -> PyResult<Py<PyAny>> {
    relate_pattern_broadcast(py, left, right, pattern)
}

/// Test whether ``left`` and ``right`` are spatially equal.
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
///     Whether the two geometries are spatially equal; one result per
///     input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> a = gm.from_wkt('LINESTRING (0 0, 2 2)')
/// >>> b = gm.from_wkt('LINESTRING (2 2, 1 1, 0 0)')
/// >>> gm.equals(a, b)  # same point set, different vertices
/// True
#[pyfunction]
pub(crate) fn equals(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    predicate_broadcast(py, left, right, Predicate::Equals)
}

/// Test full value identity elementwise: the vectorized ``==``.
///
/// Two geometries are identical when they share the same CRS, coordinate
/// epoch, geometry kind, and every active ordinate bit-for-bit in the same
/// vertex order — exactly the scalar ``left == right``. A frame (CRS/epoch)
/// difference is an *unequal value*, never an error, so mixed-frame data can
/// be compared safely. Use `equals` for the order-independent topological
/// test, or `equals_exact` for tolerance-based coordinate comparison.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     The two operands (scalar/array broadcasting).
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the values are identical; one result per input pair.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.equals_identical(gm.Point(1, 2), gm.Point(1, 2))
/// True
/// >>> gm.equals_identical(gm.Point(1, 2), gm.Point(1, 2, crs=4326))
/// False
#[pyfunction]
pub(crate) fn equals_identical(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObjectExt as _;

    let left_frame = identity_frame(left);
    let right_frame = identity_frame(right);
    if let (Some(left_frame), Some(right_frame)) = (left_frame, right_frame)
        && left_frame != right_frame
    {
        // Value semantics: differing frames compare unequal (scalar `==`
        // parity) — but operand SHAPES still follow binary-op rules.
        return match (
            crate::broadcast::exact_geometry_array(left),
            crate::broadcast::exact_geometry_array(right),
        ) {
            (Some(l), Some(r)) => {
                crate::broadcast::ensure_same_len(l.storage().len(), r.storage().len())?;
                crate::py::numpy::false_bool_array(py, l.storage().len())
            },
            (Some(l), None) => crate::py::numpy::false_bool_array(py, l.storage().len()),
            (None, Some(r)) => crate::py::numpy::false_bool_array(py, r.storage().len()),
            (None, None) => crate::broadcast::py_bool(py, false).into_py_any(py),
        };
    }
    broadcast2(py, left, right, "equals_identical", |a, b| Ok(a == b))
}

/// The value frame (CRS + epoch) of a geometry-typed operand; `None` when the
/// operand is not a geometry (the broadcast machinery raises its standard
/// TypeError downstream).
fn identity_frame<'a>(
    value: &'a Bound<'_, PyAny>,
) -> Option<(Option<&'a crate::Crs>, Option<f64>)> {
    if let Some(geometry) = crate::broadcast::exact_geometry(value) {
        return Some((geometry.crs_ref(), geometry.epoch()));
    }
    if let Some(array) = crate::broadcast::exact_geometry_array(value) {
        return Some((array.crs_ref(), array.epoch()));
    }
    None
}

/// Test coordinate equality within ``tolerance``, optionally comparing Z/M.
///
/// Two geometries are equal when they share the same structure and every paired
/// ordinate agrees to within ``tolerance`` (``|left - right| <= tolerance``).
/// ``tolerance=0.0`` is exact. Like every binary operation, both operands must
/// share one CRS/epoch frame; use `equals_identical` (the vectorized ``==``)
/// for full value identity including the frame, or `equals` for an
/// order-independent topological test.
///
/// Parameters
/// ----------
/// left, right : Geometry or GeometryArray
///     The two operands (scalar/array broadcasting).
/// tolerance : float or sequence of float, default 0.0
///     Maximum permitted per-ordinate difference — a scalar applies to every
///     pair, or pass one value per geometry.
/// include_z, include_m : bool, default True
///     Whether the Z and M ordinates participate in the comparison.
///
/// Returns
/// -------
/// bool or numpy.ndarray
///     Whether the coordinates match; one result per input pair.
///
/// Raises
/// ------
/// CRSMismatchError
///     If the operands' CRS or coordinate-epoch metadata differ.
/// GeometryError
///     If ``tolerance`` is negative or non-finite.
///
/// Examples
/// --------
/// >>> import gometry as gm
/// >>> gm.equals_exact(gm.Point(1, 1), gm.Point(1, 1.0000001), 1e-6)
/// True
#[pyfunction]
#[pyo3(
    signature = (left, right, tolerance = DefaultedF64Input::Default(0.0), *, include_z = true, include_m = true),
    text_signature = "(left, right, tolerance=0.0, *, include_z=True, include_m=True)"
)]
pub(crate) fn equals_exact(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    tolerance: DefaultedF64Input,
    include_z: bool,
    include_m: bool,
) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObjectExt as _;
    // Prepared handles are predicate operands; exact equality does not use
    // their cache, but must still compare the wrapped geometries.
    let left_prepared = left.cast::<crate::PyPreparedGeometry>().ok();
    let right_prepared = right.cast::<crate::PyPreparedGeometry>().ok();
    let left_geometry = match left_prepared {
        Some(prepared) => Some(prepared.get().geometry.clone().into_pyobject(py)?),
        None => None,
    };
    let right_geometry = match right_prepared {
        Some(prepared) => Some(prepared.get().geometry.clone().into_pyobject(py)?),
        None => None,
    };
    let left = left_geometry
        .as_ref()
        .map_or(left, |geometry| geometry.as_any());
    let right = right_geometry
        .as_ref()
        .map_or(right, |geometry| geometry.as_any());
    // Delegate any array operand to the `GeometryArray::equals_exact` method —
    // it owns the packed-polygon/line SIMD fast paths and the scalar-or-array
    // tolerance lane. `equals_exact` is symmetric, so a scalar-left/array-right
    // call routes the array as the primary operand. Two scalars are the single
    // pair below.
    if let Some(array) = crate::broadcast::exact_geometry_array(left) {
        return array
            .equals_exact(py, right, tolerance, include_z, include_m)?
            .into_py_any(py);
    }
    if let Some(array) = crate::broadcast::exact_geometry_array(right) {
        return array
            .equals_exact(py, left, tolerance, include_z, include_m)?
            .into_py_any(py);
    }
    let tolerance = match tolerance {
        DefaultedF64Input::Default(value) => value,
        DefaultedF64Input::Supplied(value) => finite_f64_required("tolerance", value.bind(py))?,
    };
    let tolerance = validate_equals_exact_tolerance(tolerance)?.get();
    match (include_z, include_m) {
        (false, false) => equals_exact_broadcast::<false, false>(py, left, right, tolerance),
        (true, false) => equals_exact_broadcast::<true, false>(py, left, right, tolerance),
        (false, true) => equals_exact_broadcast::<false, true>(py, left, right, tolerance),
        (true, true) => equals_exact_broadcast::<true, true>(py, left, right, tolerance),
    }
}

fn equals_exact_broadcast<const Z: bool, const M: bool>(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    tolerance: f64,
) -> PyResult<Py<PyAny>> {
    broadcast2(py, left, right, "equals_exact", move |a, b| {
        Ok(a.equals_exact_impl::<Z, M>(b, tolerance))
    })
}
