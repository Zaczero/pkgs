use pyo3::IntoPyObject as _;

use crate::py::crs::{
    Bound, GeometryError, MinConfidence, Py, PyAny, PyCrs, PyDict, PyResult, Python, crs, crs_arc,
    crs_projjson_mapping, crs_to_authority, crs_to_cf, crs_to_epsg, crs_to_proj, crs_to_projjson,
    crs_to_wkt, pymethods,
};
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
    #[pyo3(signature = (*, min_confidence = MinConfidence::DEFAULT), text_signature = "($self, *, min_confidence=70)")]
    fn to_epsg(slf: &Bound<'_, Self>, min_confidence: MinConfidence) -> PyResult<Option<i32>> {
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
    #[pyo3(signature = (*, authority = None, min_confidence = MinConfidence::DEFAULT), text_signature = "($self, *, authority=None, min_confidence=70)")]
    fn to_authority(
        slf: &Bound<'_, Self>,
        authority: Option<&str>,
        min_confidence: MinConfidence,
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
    ///     Indent the WKT output (default ``False``).
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
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_wkt().startswith('GEOGCRS')
    /// True
    #[pyo3(
        signature = (*, version = crs::WktVersion::Wkt2_2019, pretty = false, output_axis = crs::WktAxisRule::Auto, strict = true, indentation_width = 4),
        text_signature = "($self, *, version='WKT2_2019', pretty=False, output_axis='auto', strict=True, indentation_width=4)"
    )]
    fn to_wkt(
        slf: &Bound<'_, Self>,
        version: crs::WktVersion,
        pretty: bool,
        output_axis: crs::WktAxisRule,
        strict: bool,
        #[pyo3(from_py_with = crate::strict_i128)] indentation_width: i128,
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
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> '+proj=longlat' in gm.CRS(4326).to_proj()
    /// True
    #[pyo3(signature = (*, version = 5, pretty = false, approximate_tmerc = false, indentation_width = 2, max_line_length = 80))]
    fn to_proj(
        slf: &Bound<'_, Self>,
        #[pyo3(from_py_with = crate::strict_i64)] version: i64,
        pretty: bool,
        approximate_tmerc: bool,
        #[pyo3(from_py_with = crate::strict_i64)] indentation_width: i64,
        #[pyo3(from_py_with = crate::strict_i64)] max_line_length: i64,
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

    /// Export this CRS as PROJJSON text or a fresh Python mapping.
    ///
    /// Parameters
    /// ----------
    /// as_dict : bool
    ///     Return a Python mapping; the JSON-text form omits ``as_dict`` and
    ///     the mapping form requires ``as_dict=True``.
    ///
    /// pretty : bool, optional
    ///     Indent JSON text (default ``False``). Only applies when
    ///     ``as_dict=False``.
    ///
    /// indentation_width : int
    ///     Spaces per indent level for pretty JSON text; available only when
    ///     ``as_dict=False``.
    ///
    /// Returns
    /// -------
    /// str or dict[str, object]
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If export fails or a formatting option is invalid.
    /// GeometryError
    ///     If formatting is requested with ``as_dict=True``.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_projjson(as_dict=True)['type']
    /// 'GeographicCRS'
    #[pyo3(signature = (*, as_dict = false, pretty = false, indentation_width = 2))]
    fn to_projjson(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        as_dict: bool,
        pretty: bool,
        #[pyo3(from_py_with = crate::strict_i128)] indentation_width: i128,
    ) -> PyResult<Py<PyAny>> {
        if as_dict {
            if pretty || indentation_width != 2 {
                return Err(GeometryError::new_err(
                    "pretty and nondefault indentation_width cannot be used with as_dict=True",
                ));
            }
            return Ok(crs_projjson_mapping(py, slf.as_any())?.into_any());
        }
        Ok(crs_to_projjson(slf.as_any(), pretty, indentation_width)?
            .into_pyobject(py)?
            .into_any()
            .unbind())
    }

    /// CF (Climate and Forecast) grid-mapping attributes as a dict.
    ///
    /// Projected ``units`` is ``m``, ``ft``, or ``us-ft`` when every horizontal
    /// axis admits the same token. Otherwise the reconstructible grid mapping
    /// and ``units`` are omitted; ``crs_wkt`` still carries the full CRS.
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
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_cf().get('grid_mapping_name')
    /// 'latitude_longitude'
    #[pyo3(
        signature = (*, wkt_version = crs::WktVersion::Wkt2_2019),
        text_signature = "($self, *, wkt_version='WKT2_2019')"
    )]
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
    /// `force_2d`/`force_3d` or `with_coordinates(z=...)` for that.
    ///
    /// Parameters
    /// ----------
    /// name : str, optional
    ///     Name for the derived CRS.
    ///
    /// Returns
    /// -------
    /// CRS
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4979).to_2d().to_epsg()
    /// 4326
    #[pyo3(signature = (*, name = None))]
    fn to_2d(slf: &Bound<'_, Self>, name: Option<&str>) -> PyResult<Self> {
        Ok(Self::from_definition(crs_arc(
            super::functions::export_crs(
                slf.py(),
                slf.get().definition.clone(),
                crs::CrsExportKind::To2d {
                    name: name.map(str::to_owned),
                },
            )?,
        )))
    }

    /// Return the 3D form of this CRS (adds an ellipsoidal height axis).
    ///
    /// Converts the CRS definition only; it does not add or remove Z
    /// ordinates on geometries — use `force_2d`/`force_3d` or `with_coordinates(z=...)` for
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
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).to_3d().to_epsg()
    /// 4979
    #[pyo3(signature = (*, name = None))]
    fn to_3d(slf: &Bound<'_, Self>, name: Option<&str>) -> PyResult<Self> {
        Ok(Self::from_definition(crs_arc(
            super::functions::export_crs(
                slf.py(),
                slf.get().definition.clone(),
                crs::CrsExportKind::To3d {
                    name: name.map(str::to_owned),
                },
            )?,
        )))
    }
}
