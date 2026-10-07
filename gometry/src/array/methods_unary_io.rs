use crate::array::{Bound, Py, PyAny, PyGeometryArray, PyResult, Python, SpatialCurve, pymethods};
use crate::py::numpy::uint64_array;

#[pymethods]
impl PyGeometryArray {
    /// Space-filling-curve key of each geometry's bbox center.
    /// Discretizes centers into a ``2^level x 2^level`` grid over ``bounds``
    /// and returns distances along the selected curve.
    ///
    /// Parameters
    /// ----------
    /// curve : {'hilbert', 'morton'}, default hilbert
    ///     ``hilbert`` prioritizes locality; ``morton`` uses Z-order.
    ///
    /// level : int, default 16
    ///     Grid order (``1`` to ``32``); 16 matches GeoPandas/DuckDB.
    ///
    /// bounds : tuple of float, optional
    ///     Ordered ``(minx, miny, maxx, maxy)`` rectangle for keying. When
    ///     omitted, uses the present rows' minimum and maximum stored
    ///     coordinates without periodic wrapping. Geographic ``total_bounds``
    ///     may wrap, so it is not necessarily a curve frame. Keys compare
    ///     across geometries only against a *shared* frame — pass the same
    ///     ``bounds`` when keying separate geometries.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One result per row. Empty and missing rows use ``uint64.max``
    ///     and therefore sort last.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If ``level`` or ``bounds`` is invalid.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> keys = gm.GeometryArray([gm.Point(0, 0), gm.Point(10, 10)]).spatial_key(bounds=(0, 0, 10, 10))
    /// >>> bool(keys[0] != keys[1])
    /// True
    #[pyo3(signature = (*, curve = SpatialCurve::Hilbert, level = 16, bounds = None), text_signature = "($self, *, curve='hilbert', level=16, bounds=None)")]
    pub fn spatial_key(
        &self,
        py: Python<'_>,
        curve: SpatialCurve,
        #[pyo3(from_py_with = crate::strict_i64)] level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        uint64_array(py, self.curve_keys(level, bounds, curve.into())?)
    }
}
