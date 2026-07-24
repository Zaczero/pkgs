use crate::py::crs::*;
#[pymethods]
impl PyCrs {
    /// Whether this CRS is geographic (lon/lat on an ellipsoid).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_geographic(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_geographic())
    }

    /// Whether this CRS is projected (planar, metric).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_projected(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_projected())
    }

    /// Whether this CRS is vertical (height/depth only).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_vertical(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_vertical())
    }

    /// Whether this CRS is geocentric (earth-centered Cartesian).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_geocentric(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_geocentric())
    }

    /// Whether this CRS is compound (horizontal + vertical).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_compound(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_compound())
    }

    /// Whether this CRS is engineering (local/non-geodetic).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_engineering(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_engineering())
    }

    /// Whether this CRS is a bound CRS (carries a datum transform).
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_bound(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_bound())
    }

    /// Whether this CRS is flagged deprecated by its authority.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_deprecated(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_deprecated())
    }

    /// Whether this CRS is derived from a base CRS.
    ///
    /// Returns
    /// -------
    /// bool
    #[getter]
    fn is_derived(&self) -> PyResult<bool> {
        Ok(self.cached_info()?.is_derived)
    }

    /// CRS kind as a snake_case token (``"geographic_2d"``, ``"geographic_3d"``,
    /// ``"projected"``, ``"geocentric"``, ``"vertical"``, ``"compound"``,
    /// ``"bound"``, ``"other"``, ...).
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn kind(&self) -> PyResult<&'static str> {
        Ok(self.cached_info()?.kind)
    }

    /// Human-readable CRS name, if known.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    fn name(&self) -> PyResult<Option<String>> {
        Ok(self.cached_info()?.name.clone())
    }

    /// Registry authority (e.g. ``"EPSG"``), if identified.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    fn authority(&self) -> PyResult<Option<String>> {
        Ok(self.cached_info()?.authority.clone())
    }

    /// Authority code (e.g. ``"4326"``), if identified.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    fn code(&self) -> PyResult<Option<String>> {
        Ok(self.cached_info()?.code.clone())
    }

    /// Canonical identifier string for this CRS.
    ///
    /// Returns
    /// -------
    /// str
    #[getter]
    fn canonical(&self) -> String {
        self.canonical.to_string()
    }

    /// Axis roles in CRS order (lowercase, e.g. ``["lat", "lon"]`` for a
    /// lat/lon CRS; tokens ``"lat"``/``"lon"``/``"x"``/``"y"``/``"z"``/
    /// ``"height"``/``"other"``). For the raw PROJ abbreviations use ``axes``.
    ///
    /// Returns
    /// -------
    /// list of str
    #[getter]
    fn axis_order(&self) -> PyResult<Vec<String>> {
        Ok(self
            .cached_info()?
            .axis_order
            .iter()
            .map(|a| (*a).to_owned())
            .collect())
    }

    /// Name of the celestial body (usually ``"Earth"``), if known.
    ///
    /// Returns
    /// -------
    /// str or None
    #[getter]
    fn celestial_body(&self) -> PyResult<Option<String>> {
        Ok(self.cached_info()?.celestial_body.clone())
    }

    /// Coordinate-system axes as a list of dicts.
    ///
    /// Returns
    /// -------
    /// list of CrsAxisInfo
    #[getter]
    fn axes(&self) -> PyResult<Vec<crs::AxisInfo>> {
        Ok(self.cached_info()?.axes.clone())
    }

    /// Area of use as a ``{west, south, east, north, name}`` dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsAreaOfUse or None
    #[getter]
    fn area_of_use(&self) -> PyResult<Option<crs::AreaOfUse>> {
        Ok(self.cached_info()?.area_of_use.clone())
    }

    /// Reference ellipsoid as a dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsEllipsoidInfo or None
    #[getter]
    fn ellipsoid(&self) -> PyResult<Option<crs::EllipsoidInfo>> {
        Ok(self.cached_info()?.ellipsoid.clone())
    }

    /// Geodetic datum as a dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsDatumInfo or None
    #[getter]
    fn datum(&self) -> PyResult<Option<crs::DatumInfo>> {
        Ok(self.cached_info()?.datum.clone())
    }

    /// Prime meridian as a dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsPrimeMeridianInfo or None
    #[getter]
    fn prime_meridian(&self) -> PyResult<Option<crs::PrimeMeridianInfo>> {
        Ok(self.cached_info()?.prime_meridian.clone())
    }

    /// Underlying geodetic CRS as an authority-object dict, or ``None``.
    ///
    /// Returns
    /// -------
    /// CrsAuthorityObject or None
    #[getter]
    fn geodetic_crs(&self) -> PyResult<Option<crs::AuthorityObjectInfo>> {
        Ok(self.cached_info()?.geodetic_crs.clone())
    }

    /// Full raw PROJ description as a dict (escape hatch).
    ///
    /// Returns
    /// -------
    /// CrsInfo
    #[getter]
    fn info(&self) -> PyResult<crs::CrsInfo> {
        Ok(self.cached_info()?.clone())
    }
}
