use crate::py::crs::*;
#[pymethods]
impl PyCrs {
    /// EPSG integer code, or ``None`` if it cannot be determined.
    ///
    /// Parameters
    /// ----------
    /// min_confidence : int, optional
    ///     Minimum identification confidence (0-100, default 70).
    ///
    /// Returns
    /// -------
    /// int or None
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If ``min_confidence`` is outside ``0``-``100``.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_epsg()
    /// 4326
    #[pyo3(signature = (*, min_confidence = 70), text_signature = "($self, *, min_confidence=70)")]
    fn to_epsg(slf: &Bound<'_, Self>, min_confidence: u8) -> PyResult<Option<i32>> {
        crs_to_epsg(slf.as_any(), min_confidence)
    }

    /// ``(authority, code)`` pair, or ``None`` if it cannot be determined.
    ///
    /// Parameters
    /// ----------
    /// authority : str, optional
    ///     Restrict identification to this authority.
    ///
    /// min_confidence : int, optional
    ///     Minimum identification confidence (0-100, default 70).
    ///
    /// Returns
    /// -------
    /// tuple or None
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If ``min_confidence`` is outside ``0``-``100``.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_authority()
    /// ('EPSG', '4326')
    #[pyo3(signature = (*, authority = None, min_confidence = 70), text_signature = "($self, *, authority=None, min_confidence=70)")]
    fn to_authority(
        slf: &Bound<'_, Self>,
        authority: Option<&str>,
        min_confidence: u8,
    ) -> PyResult<Option<(String, String)>> {
        crs_to_authority(slf.as_any(), authority, min_confidence)
    }

    /// Serialize this CRS to WKT.
    ///
    /// Parameters
    /// ----------
    /// version : str, optional
    ///     WKT dialect (default ``"WKT2_2019"``).
    ///
    /// pretty : bool, optional
    ///     Indent the output (default ``False``).
    ///
    /// output_axis : str, default "auto"
    ///     Axis-output policy (``"auto"``, ``"yes"``, ``"no"``).
    ///
    /// strict : bool, optional
    ///     Fail on lossy output (default ``True``).
    ///
    /// indentation_width : int, default 4
    ///     Spaces per indent level when ``pretty``.
    ///
    /// Returns
    /// -------
    /// str
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If export fails or a formatting option is invalid.
    #[pyo3(
        signature = (*, version = crs::WktVersion::Wkt2_2019, pretty = false, output_axis = crs::WktAxisRule::Auto, strict = true, indentation_width = 4),
        text_signature = "($self, *, version='WKT2_2019', pretty=False, output_axis='auto', strict=True, indentation_width=4)"
    )]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_wkt().startswith('GEOGCRS')
    /// True
    fn to_wkt(
        slf: &Bound<'_, Self>,
        version: crs::WktVersion,
        pretty: bool,
        output_axis: crs::WktAxisRule,
        strict: bool,
        indentation_width: i128,
    ) -> PyResult<String> {
        crs_to_wkt(
            slf.as_any(),
            version,
            pretty,
            output_axis,
            strict,
            indentation_width,
        )
    }

    /// Serialize this CRS to a PROJ string.
    ///
    /// Parameters
    /// ----------
    /// version : int, optional
    ///     PROJ string version (4 or 5).
    ///
    /// pretty : bool, optional
    ///     Indent the output (default ``False``).
    ///
    /// approximate_tmerc : bool, optional
    ///     Use the approximate transverse-Mercator formulation.
    ///
    /// indentation_width : int, default 2
    ///     Spaces per indent level when ``pretty``.
    ///
    /// max_line_length : int, optional
    ///     Wrap lines at this width.
    ///
    /// Returns
    /// -------
    /// str
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If export fails or a formatting option is invalid.
    #[pyo3(signature = (*, version = 5, pretty = false, approximate_tmerc = false, indentation_width = 2, max_line_length = 80))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> '+proj=longlat' in gm.CRS(4326).to_proj()
    /// True
    fn to_proj(
        slf: &Bound<'_, Self>,
        version: i64,
        pretty: bool,
        approximate_tmerc: bool,
        indentation_width: i64,
        max_line_length: i64,
    ) -> PyResult<String> {
        crs_to_proj(
            slf.as_any(),
            version,
            pretty,
            approximate_tmerc,
            indentation_width,
            max_line_length,
        )
    }

    /// Serialize this CRS to a PROJJSON string.
    ///
    /// Parameters
    /// ----------
    /// pretty : bool, optional
    ///     Indent the output (default ``False``).
    ///
    /// indentation_width : int, default 2
    ///     Spaces per indent level when ``pretty``.
    ///
    /// Returns
    /// -------
    /// str
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If export fails or a formatting option is invalid.
    #[pyo3(signature = (*, pretty = false, indentation_width = 2))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> 'GeographicCRS' in gm.CRS(4326).to_projjson()
    /// True
    fn to_projjson(
        slf: &Bound<'_, Self>,
        pretty: bool,
        indentation_width: i128,
    ) -> PyResult<String> {
        crs_to_projjson(slf.as_any(), pretty, indentation_width)
    }

    /// Serialize this CRS to a PROJJSON ``dict``.
    ///
    /// Returns
    /// -------
    /// dict[str, object]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_projjson_dict()['type']
    /// 'GeographicCRS'
    fn to_projjson_dict(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyDict>> {
        crs_to_projjson_dict(py, slf.as_any())
    }

    /// CF (Climate and Forecast) grid-mapping attributes as a dict.
    ///
    /// Parameters
    /// ----------
    /// wkt_version : str, optional
    ///     WKT dialect embedded in ``crs_wkt`` (default ``"WKT2_2019"``).
    ///
    /// Returns
    /// -------
    /// CrsCfInfo
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If export fails.
    #[pyo3(
        signature = (*, wkt_version = crs::WktVersion::Wkt2_2019),
        text_signature = "($self, *, wkt_version='WKT2_2019')"
    )]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_cf().get('grid_mapping_name')
    /// 'latitude_longitude'
    fn to_cf(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        wkt_version: crs::WktVersion,
    ) -> PyResult<Py<PyDict>> {
        crs_to_cf(py, slf.as_any(), wkt_version)
    }

    /// Return the 2D (horizontal) form of this CRS.
    ///
    /// Converts the CRS definition by removing the ellipsoidal-height axis;
    /// it does not add or remove Z ordinates on geometries — use
    /// `force_2d`/`force_3d` or `set_z` for that.
    ///
    /// Parameters
    /// ----------
    /// name : str, optional
    ///     Name for the derived CRS.
    ///
    /// Returns
    /// -------
    /// CRS
    #[pyo3(signature = (*, name = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4979).to_2d().to_epsg()
    /// 4326
    fn to_2d(slf: &Bound<'_, Self>, name: Option<&str>) -> PyResult<Self> {
        Ok(Self::from_canonical(crs_arc(crs::to_2d(
            &slf.get().canonical,
            name,
        )?)))
    }

    /// Return the 3D form of this CRS (adds an ellipsoidal height axis).
    ///
    /// Converts the CRS definition only; it does not add or remove Z
    /// ordinates on geometries — use `force_2d`/`force_3d` or `set_z` for
    /// that.
    ///
    /// Parameters
    /// ----------
    /// name : str, optional
    ///     Name for the derived CRS.
    ///
    /// Returns
    /// -------
    /// CRS
    #[pyo3(signature = (*, name = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_3d().to_epsg()
    /// 4979
    fn to_3d(slf: &Bound<'_, Self>, name: Option<&str>) -> PyResult<Self> {
        Ok(Self::from_canonical(crs_arc(crs::to_3d(
            &slf.get().canonical,
            name,
        )?)))
    }
}
