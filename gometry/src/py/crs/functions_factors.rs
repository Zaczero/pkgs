use crate::py::crs::{
    Bound, Py, PyAny, PyDict, PyDictMethods as _, PyResult, Python, broadcast_coordinate_group,
    crs, crs_normalize,
};
use crate::py::numpy::float64_array;

/// Map projection factors (scale, distortion) at coordinates.
///
/// Parameters
/// ----------
/// target : str or int
///     Projected CRS as an EPSG code or authority/WKT string.
/// lon, lat : float or sequence of float
///     Geodetic coordinates, scalar or batch (degrees unless ``radians``).
/// radians : bool, default False
///     Whether ``lon``/``lat`` are in radians.
///
/// Returns
/// -------
/// dict
pub(crate) fn crs_factors(
    py: Python<'_>,
    target: &Bound<'_, PyAny>,
    lon: &Bound<'_, PyAny>,
    lat: &Bound<'_, PyAny>,
    radians: bool,
) -> PyResult<Py<PyDict>> {
    let target = crs_normalize(target)?;
    let mut expected = crate::coordinate_sequence_len_hint(lon, "lon")?;
    if expected.is_none() {
        expected = crate::coordinate_sequence_len_hint(lat, "lat")?;
    }
    let mut lon = crate::coordinate_input_with_expected(py, lon, "lon", expected, &|| {
        "lon must be finite".into()
    })?;
    let expected = expected.or_else(|| (!lon.scalar).then_some(lon.values.len()));
    let mut lat = crate::coordinate_input_with_expected(py, lat, "lat", expected, &|| {
        "lat must be finite".into()
    })?;
    let scalar = lon.scalar && lat.scalar;
    broadcast_coordinate_group([(&mut lon, "lon"), (&mut lat, "lat")], "lon and lat")?;
    if scalar {
        let lon = lon.values[0];
        let lat = lat.values[0];
        let factors = py.detach(move || crs::factors(&target, lon, lat, radians))?;
        return projection_factors_to_py(py, &factors);
    }
    let lons = lon.values;
    let lats = lat.values;
    let factors = py.detach(move || crs::factor_columns(&target, &lons, &lats, radians))?;
    projection_factors_batch_to_py(py, factors)
}

/// The 12 projection-factor fields written once for the scalar dict. Batch
/// results move columns explicitly below so ownership stays clear without an
/// unused table accessor.
type ProjectionFactorField = (&'static str, fn(&crs::ProjectionFactors) -> f64);
static PROJECTION_FACTOR_FIELDS: [ProjectionFactorField; 12] = [
    ("meridional_scale", |f| f.meridional_scale),
    ("parallel_scale", |f| f.parallel_scale),
    ("areal_scale", |f| f.areal_scale),
    ("angular_distortion", |f| f.angular_distortion),
    ("meridian_parallel_angle", |f| f.meridian_parallel_angle),
    ("meridian_convergence", |f| f.meridian_convergence),
    ("tissot_semimajor", |f| f.tissot_semimajor),
    ("tissot_semiminor", |f| f.tissot_semiminor),
    ("dx_dlam", |f| f.dx_dlam),
    ("dx_dphi", |f| f.dx_dphi),
    ("dy_dlam", |f| f.dy_dlam),
    ("dy_dphi", |f| f.dy_dphi),
];

pub(crate) fn projection_factors_to_py(
    py: Python<'_>,
    factors: &crs::ProjectionFactors,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for (key, scalar) in &PROJECTION_FACTOR_FIELDS {
        dict.set_item(key, scalar(factors))?;
    }
    Ok(dict.unbind())
}

pub(crate) fn projection_factors_batch_to_py(
    py: Python<'_>,
    factors: crs::ProjectionFactorColumns,
) -> PyResult<Py<PyDict>> {
    let crs::ProjectionFactorColumns {
        meridional_scale,
        parallel_scale,
        areal_scale,
        angular_distortion,
        meridian_parallel_angle,
        meridian_convergence,
        tissot_semimajor,
        tissot_semiminor,
        dx_dlam,
        dx_dphi,
        dy_dlam,
        dy_dphi,
    } = factors;
    let dict = PyDict::new(py);
    for (key, values) in [
        ("meridional_scale", meridional_scale),
        ("parallel_scale", parallel_scale),
        ("areal_scale", areal_scale),
        ("angular_distortion", angular_distortion),
        ("meridian_parallel_angle", meridian_parallel_angle),
        ("meridian_convergence", meridian_convergence),
        ("tissot_semimajor", tissot_semimajor),
        ("tissot_semiminor", tissot_semiminor),
        ("dx_dlam", dx_dlam),
        ("dx_dphi", dx_dphi),
        ("dy_dlam", dy_dlam),
        ("dy_dphi", dy_dphi),
    ] {
        dict.set_item(key, float64_array(py, values)?)?;
    }
    Ok(dict.unbind())
}
