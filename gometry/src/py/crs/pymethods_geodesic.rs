use super::*;
#[pymethods]
impl PyCrs {
    /// Test whether this CRS describes the same system as ``other``.
    ///
    /// Parameters
    /// ----------
    /// other : str or int or CRS
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
    #[pyo3(
        signature = (other, *, mode),
        text_signature = "($self, other, *, mode)"
    )]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).same_as(gm.CRS(4326), mode='exact')
    /// True
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
    #[pyo3(signature = (*, authority = None))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.CRS(4326).identify()[0]['code']
    /// '4326'
    fn identify(
        slf: &Bound<'_, Self>,
        authority: Option<&str>,
    ) -> PyResult<Vec<crs::IdentifyCandidate>> {
        crs_identify(slf.as_any(), authority)
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
    /// >>> gm.CRS(4326).non_deprecated()
    /// []
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
    /// >>> gm.CRS(4326).geoid_models()
    /// []
    fn geoid_models(slf: &Bound<'_, Self>) -> PyResult<Vec<String>> {
        crs_geoid_models(slf.as_any())
    }

    /// Map-projection factors (scale, distortion, ...) at a point.
    ///
    /// Parameters
    /// ----------
    /// lon : float
    ///     Longitude (or easting) of the evaluation point.
    ///
    /// lat : float
    ///     Latitude (or northing) of the evaluation point.
    ///
    /// radians : bool, optional
    ///     Interpret angular inputs as radians (default ``False``).
    ///
    /// Returns
    /// -------
    /// CrsProjectionFactors or CrsProjectionFactorsBatch
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If ``lon``/``lat`` are non-finite or differ in length.
    #[pyo3(signature = (lon, lat, *, radians = false))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> round(gm.CRS(4326).factors(-122.4, 37.8)['meridional_scale'], 5)
    /// 1.00294
    fn factors(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        lon: &Bound<'_, PyAny>,
        lat: &Bound<'_, PyAny>,
        radians: bool,
    ) -> PyResult<Py<PyDict>> {
        crs_factors(py, slf.as_any(), lon, lat, radians)
    }

    /// Compute the geodesic inverse solution between two points on this CRS's ellipsoid.
    ///
    /// Parameters
    /// ----------
    /// lon1, lat1 : float
    ///     First point.
    ///
    /// lon2, lat2 : float
    ///     Second point.
    ///
    /// z1, z2 : float, optional
    ///     Heights for a 3D (slant) distance.
    ///
    /// radians : bool, optional
    ///     Interpret angular inputs as radians (default ``False``).
    ///
    /// Returns
    /// -------
    /// CrsGeodesicInfo or CrsGeodesicBatchInfo
    ///
    /// Raises
    /// ------
    /// InvalidGeometryError
    ///     If coordinate columns are non-finite, differ in length, or only
    ///     one of ``z1``/``z2`` is given.
    /// GeometryError
    ///     If the CRS does not expose a usable ellipsoid for geodesic use.
    #[pyo3(signature = (lon1, lat1, lon2, lat2, z1 = None, z2 = None, *, radians = false))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> round(gm.CRS(4326).geodesic(-122.4, 37.8, -122.3, 37.9)['distance'])
    /// 14165
    fn geodesic(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        lon1: &Bound<'_, PyAny>,
        lat1: &Bound<'_, PyAny>,
        lon2: &Bound<'_, PyAny>,
        lat2: &Bound<'_, PyAny>,
        z1: Option<&Bound<'_, PyAny>>,
        z2: Option<&Bound<'_, PyAny>>,
        radians: bool,
    ) -> PyResult<Py<PyDict>> {
        crs_geodesic(
            py,
            slf.as_any(),
            lon1,
            lat1,
            lon2,
            lat2,
            z1,
            z2,
            crs::AngleUnit::of(radians),
        )
    }

    /// Compute the geodesic direct solution: project a point along an azimuth.
    ///
    /// Parameters
    /// ----------
    /// lon, lat : float
    ///     Start point.
    ///
    /// azimuth : float
    ///     Forward azimuth in degrees (or radians if ``radians``).
    ///
    /// distance : float
    ///     Geodesic distance in meters.
    ///
    /// radians : bool, optional
    ///     Interpret/return angular values as radians (default ``False``).
    ///
    /// Returns
    /// -------
    /// CrsGeodesicDirectInfo or CrsGeodesicDirectBatchInfo
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the value is not a recognized CRS.
    /// InvalidGeometryError
    ///     If coordinate columns are non-finite or differ in length.
    #[pyo3(signature = (lon, lat, azimuth, distance, *, radians = false))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> d = gm.CRS(4326).geodesic_direct(-122.4, 37.8, 45.0, 1000.0)
    /// >>> (round(d['longitude'], 5), round(d['latitude'], 5))
    /// (-122.39197, 37.80637)
    fn geodesic_direct(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        lon: &Bound<'_, PyAny>,
        lat: &Bound<'_, PyAny>,
        azimuth: &Bound<'_, PyAny>,
        distance: &Bound<'_, PyAny>,
        radians: bool,
    ) -> PyResult<Py<PyDict>> {
        crs_geodesic_direct(
            py,
            slf.as_any(),
            lon,
            lat,
            azimuth,
            distance,
            crs::AngleUnit::of(radians),
        )
    }

    /// Interpolate a point a given distance along the geodesic between two
    /// points.
    ///
    /// Parameters
    /// ----------
    /// lon1, lat1, lon2, lat2 : float
    ///     Endpoints of the geodesic.
    ///
    /// distance : float
    ///     Distance from the first point, in meters (or a fraction if
    ///     ``normalized``).
    ///
    /// normalized : bool, default False
    ///     Treat ``distance`` as a fraction of the total length (default
    ///     ``False``).
    ///
    /// radians : bool, optional
    ///     Interpret/return angular values as radians (default ``False``).
    ///
    /// Returns
    /// -------
    /// CrsGeodesicInterpolateInfo or CrsGeodesicInterpolateBatchInfo
    ///
    /// Raises
    /// ------
    /// CRSError
    ///     If the value is not a recognized CRS.
    /// InvalidGeometryError
    ///     If coordinate columns are non-finite or differ in length.
    #[pyo3(signature = (lon1, lat1, lon2, lat2, distance, *, normalized = false, radians = false))]
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> mid = gm.CRS(4326).geodesic_interpolate(
    /// ...     -122.4, 37.8, -122.3, 37.9, 0.5, normalized=True)
    /// >>> (round(mid['longitude'], 5), round(mid['latitude'], 5))
    /// (-122.35003, 37.85001)
    fn geodesic_interpolate(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        lon1: &Bound<'_, PyAny>,
        lat1: &Bound<'_, PyAny>,
        lon2: &Bound<'_, PyAny>,
        lat2: &Bound<'_, PyAny>,
        distance: &Bound<'_, PyAny>,
        normalized: bool,
        radians: bool,
    ) -> PyResult<Py<PyDict>> {
        crs_geodesic_interpolate(
            py,
            slf.as_any(),
            lon1,
            lat1,
            lon2,
            lat2,
            distance,
            crs::DistanceMode::of(normalized),
            crs::AngleUnit::of(radians),
        )
    }
}
