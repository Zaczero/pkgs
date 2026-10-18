use crate::py::crs::{
    Bound, Py, PyAny, PyCrs, PyDict, PyResult, Python, crs, crs_factors, crs_geoid_models,
    crs_non_deprecated, crs_same, pymethods,
};
#[pymethods]
impl PyCrs {
    /// Whether this CRS is geographic (lon/lat on an ellipsoid).
    ///
    /// For a compound CRS this is ``True`` when **any component** is
    /// geographic (not root-kind only) — e.g. ``EPSG:9707`` (WGS 84 + height).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_geographic(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_geographic())
    }

    /// Whether this CRS is projected (planar, metric).
    ///
    /// For a compound CRS this is ``True`` when **any component** is
    /// projected (not root-kind only).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_projected(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_projected())
    }

    /// Whether this CRS is vertical (height/depth only).
    ///
    /// For a compound CRS this is ``True`` when **any component** is
    /// vertical (not root-kind only).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_vertical(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_vertical())
    }

    /// Whether this CRS is geocentric (earth-centered Cartesian).
    ///
    /// For a compound CRS this is ``True`` when **any component** is
    /// geocentric (not root-kind only).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_geocentric(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_geocentric())
    }

    /// Whether this CRS is compound (horizontal + vertical).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_compound(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_compound())
    }

    /// Whether this CRS is engineering (local/non-geodetic).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_engineering(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_engineering())
    }

    /// Whether this CRS is a bound CRS (carries a datum transform).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_bound(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_bound())
    }

    /// Whether this CRS is flagged deprecated by its authority.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_deprecated(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_deprecated())
    }

    /// Whether this CRS is derived from a base CRS.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_derived(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.cached_info(py)?.is_derived)
    }

    /// CRS kind as a snake_case token (``"geographic_2d"``, ``"geographic_3d"``,
    /// ``"projected"``, ``"geocentric"``, ``"vertical"``, ``"compound"``,
    /// ``"bound"``, ``"other"``, ...).
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn kind(&self, py: Python<'_>) -> PyResult<&'static str> {
        Ok(self.cached_info(py)?.kind)
    }

    /// Human-readable CRS name, if known.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    fn name(&self, py: Python<'_>) -> PyResult<Option<String>> {
        Ok(self.cached_info(py)?.name.clone())
    }

    /// Registry authority (e.g. ``"EPSG"``), if identified.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    fn authority(&self, py: Python<'_>) -> PyResult<Option<String>> {
        Ok(self.cached_info(py)?.authority.clone())
    }

    /// Authority code (e.g. ``"4326"``), if identified.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    fn code(&self, py: Python<'_>) -> PyResult<Option<String>> {
        Ok(self.cached_info(py)?.code.clone())
    }

    /// Stored definition string for this CRS.
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn definition(&self) -> String {
        self.definition.to_string()
    }

    /// Axis roles in CRS order (lowercase, e.g. ``["lat", "lon"]`` for a
    /// lat/lon CRS; tokens ``"lat"``/``"lon"``/``"x"``/``"y"``/``"z"``/
    /// ``"height"``/``"other"``). For the raw PROJ abbreviations use ``axes``.
    ///
    /// Returns
    /// -------
    /// list of str
    #[getter]
    fn axis_order(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let info = self.cached_info(py)?;
        Ok(info.axis_order.iter().map(|a| (*a).to_owned()).collect())
    }

    /// Name of the celestial body (usually ``"Earth"``), if known.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    fn celestial_body(&self, py: Python<'_>) -> PyResult<Option<String>> {
        Ok(self.cached_info(py)?.celestial_body.clone())
    }

    /// Coordinate-system axes as a list of dicts.
    ///
    /// Returns
    /// -------
    /// list of CrsAxisInfo
    #[getter]
    fn axes(&self, py: Python<'_>) -> PyResult<Vec<crs::AxisInfo>> {
        Ok(self.cached_info(py)?.axes.clone())
    }

    /// Area of use as a ``{west, south, east, north, name}`` dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsAreaOfUse or None
    #[getter]
    fn area_of_use(&self, py: Python<'_>) -> PyResult<Option<crs::AreaOfUse>> {
        Ok(self.cached_info(py)?.area_of_use.clone())
    }

    /// Reference ellipsoid as a dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsEllipsoidInfo or None
    #[getter]
    fn ellipsoid(&self, py: Python<'_>) -> PyResult<Option<crs::EllipsoidInfo>> {
        Ok(self.cached_info(py)?.ellipsoid.clone())
    }

    /// Geodetic datum as a dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsDatumInfo or None
    #[getter]
    fn datum(&self, py: Python<'_>) -> PyResult<Option<crs::DatumInfo>> {
        Ok(self.cached_info(py)?.datum.clone())
    }

    /// Prime meridian as a dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsPrimeMeridianInfo or None
    #[getter]
    fn prime_meridian(&self, py: Python<'_>) -> PyResult<Option<crs::PrimeMeridianInfo>> {
        Ok(self.cached_info(py)?.prime_meridian.clone())
    }

    /// Underlying geodetic CRS as an authority-object dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsAuthorityObject or None
    #[getter]
    fn geodetic_crs(&self, py: Python<'_>) -> PyResult<Option<crs::AuthorityObjectInfo>> {
        Ok(self.cached_info(py)?.geodetic_crs.clone())
    }

    /// Full raw PROJ description as a dict (escape hatch).
    ///
    /// Returns
    /// -------
    /// CrsInfo
    #[getter]
    fn info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        self.cached_info_dict(py)
    }

    /// Test whether this CRS describes the same system as ``other``.
    ///
    /// Parameters
    /// ----------
    /// other : CRS-like
    ///     The other CRS-like value to compare against.
    ///
    /// mode : str
    ///     One of ``'ignore_axis_order'``
    ///     (same but axis-swapped CRS count as equal), or ``'exact'``
    ///     (strict, detail-identical match).
    ///
    /// Returns
    /// -------
    /// bool
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).same_as(gm.CRS(4326), mode='exact')
    /// True
    #[pyo3(
        signature = (other, *, mode),
        text_signature = "($self, other, *, mode)"
    )]
    fn same_as(
        slf: &Bound<'_, Self>,
        other: &Bound<'_, PyAny>,
        mode: crs::CrsComparison,
    ) -> PyResult<bool> {
        crs_same(slf.as_any(), other, mode)
    }

    /// Candidate authority matches for this CRS, best first.
    ///
    /// Parameters
    /// ----------
    /// authority : str, optional
    ///     Restrict matches to this authority.
    ///
    /// Returns
    /// -------
    /// list of CrsIdentifyCandidate
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If identification fails.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).identify()[0]['code']
    /// '4326'
    #[pyo3(signature = (*, authority = None))]
    fn identify(
        slf: &Bound<'_, Self>,
        authority: Option<&str>,
    ) -> PyResult<Vec<crs::IdentifyCandidate>> {
        Ok(slf.borrow().cached_identify(slf.py(), authority)?)
    }

    /// Non-deprecated authority objects equivalent to this CRS.
    ///
    /// Returns
    /// -------
    /// list of CrsAuthorityObject
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).non_deprecated
    /// []
    #[getter]
    fn non_deprecated(slf: &Bound<'_, Self>) -> PyResult<Vec<crs::AuthorityObjectInfo>> {
        crs_non_deprecated(slf.as_any())
    }

    /// Geoid model names available for this CRS.
    ///
    /// Returns
    /// -------
    /// list of str
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).geoid_models
    /// []
    #[getter]
    fn geoid_models(slf: &Bound<'_, Self>) -> PyResult<Vec<String>> {
        crs_geoid_models(slf.as_any())
    }

    /// Map-projection factors (scale, distortion, Tissot, …) at a lon/lat.
    ///
    /// For a **projected** CRS the evaluation point is geographic lon/lat on
    /// the base ellipsoid and the returned scales/distortions describe the
    /// projection at that location. For a pure geographic CRS the factors are
    /// near-identity (meridional/parallel scale ≈ 1).
    ///
    /// Parameters
    /// ----------
    /// lon : float or sequence of float
    ///     Longitude of the evaluation point(s), degrees unless
    ///     ``radians=True``.
    ///
    /// lat : float or sequence of float
    ///     Latitude of the evaluation point(s), degrees unless
    ///     ``radians=True``.
    ///
    /// radians : bool, optional
    ///     Interpret angular inputs as radians (default ``False``).
    ///
    /// Returns
    /// -------
    /// dict
    ///     Scalar call: mapping of factor names to floats
    ///     (``meridional_scale``, ``parallel_scale``, ``areal_scale``,
    ///     ``angular_distortion``, ``meridian_parallel_angle``,
    ///     ``meridian_convergence``, ``tissot_semimajor``,
    ///     ``tissot_semiminor``, ``dx_dlam``, ``dx_dphi``, ``dy_dlam``,
    ///     ``dy_dphi``). Array call: same keys with float arrays.
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If ``lon``/``lat`` are non-finite or differ in length.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> f = gm.CRS(3857).factors(0.0, 0.0)
    /// >>> round(f['meridional_scale'], 5)
    /// 1.0
    #[pyo3(signature = (lon, lat, *, radians = false))]
    fn factors(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        lon: &Bound<'_, PyAny>,
        lat: &Bound<'_, PyAny>,
        radians: bool,
    ) -> PyResult<Py<PyDict>> {
        crs_factors(py, slf.as_any(), lon, lat, radians)
    }
}
