use super::*;

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
    ///     The frame ``(minx, miny, maxx, maxy)``; the array's total bounds
    ///     when omitted. Keys compare across geometries only against a *shared*
    ///     frame — pass the same ``bounds`` when keying separate geometries.
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
        level: i64,
        bounds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        self.spatial_key_impl(py, curve, level, bounds)
    }
}
