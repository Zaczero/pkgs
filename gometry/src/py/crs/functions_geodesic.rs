use crate::py::crs::{
    Bound, CoordinateInput, Py, PyAny, PyAnyMethods as _, PyDict, PyDictMethods as _, PyResult,
    Python, broadcast_coordinate_group, crs, crs_normalize, parameter_input_with_expected,
};
use crate::py::errors::InvalidGeometryError;
use crate::py::numpy::{float64_array, optional_float64_array};
/// Export a CRS to CF (Climate & Forecast) grid-mapping attributes.
///
/// Parameters
/// ----------
/// crs : str or int
///     CRS as an EPSG code or authority/WKT string.
///
/// Returns
/// -------
/// dict
pub(crate) fn crs_to_cf(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    wkt_version: crs::WktVersion,
) -> PyResult<Py<PyDict>> {
    let crs = crs_normalize(value)?;
    let dict = PyDict::new(py);
    for (key, value) in crs::to_cf(&crs, wkt_version)? {
        match value {
            crs::CfValue::String(value) => dict.set_item(key, value)?,
            crs::CfValue::Float(value) => dict.set_item(key, value)?,
            crs::CfValue::FloatList(value) => dict.set_item(key, value)?,
        }
    }
    Ok(dict.unbind())
}

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

/// One geodesic result field: a plain or optional float accessor.
enum GeodesicField<T> {
    F64(fn(&T) -> f64),
    OptionalF64(fn(&T) -> Option<f64>),
}

/// Build a geodesic result dict from a field table: the scalar form sets one
/// value per key, the batch form one list per key. One table per operation
/// guarantees the scalar and batch key sets stay identical.
fn geodesic_rows_to_dict<T>(
    py: Python<'_>,
    items: &[T],
    scalar: bool,
    fields: &[(&str, GeodesicField<T>)],
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    for (key, field) in fields {
        match field {
            GeodesicField::F64(get) => {
                if scalar {
                    dict.set_item(key, get(&items[0]))?;
                } else {
                    // Bulk results are zero-copy buffer columns, not one
                    // boxed float per row (the same contract as the CRS
                    // transform lanes).
                    let mut values = crate::try_vec_with_capacity(items.len())?;
                    for item in items {
                        crate::try_push(&mut values, get(item))?;
                    }
                    let column = float64_array(py, values)?;
                    dict.set_item(key, column)?;
                }
            },
            GeodesicField::OptionalF64(get) => {
                if scalar {
                    dict.set_item(key, get(&items[0]))?;
                } else {
                    let column = optional_float64_array(py, items.iter().map(get))?;
                    dict.set_item(key, column)?;
                }
            },
        }
    }
    Ok(dict.unbind())
}

fn geodesic_direct_columns_to_dict(
    py: Python<'_>,
    columns: crs::GeodesicDirectColumns,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("longitude", float64_array(py, columns.longitude)?)?;
    dict.set_item("latitude", float64_array(py, columns.latitude)?)?;
    dict.set_item("final_azimuth", float64_array(py, columns.final_azimuth)?)?;
    Ok(dict.unbind())
}

static GEODESIC_INVERSE_FIELDS: [(&str, GeodesicField<crs::GeodesicInverseInfo>); 4] = [
    ("distance", GeodesicField::F64(|i| i.distance)),
    ("distance_3d", GeodesicField::OptionalF64(|i| i.distance_3d)),
    ("forward_azimuth", GeodesicField::F64(|i| i.forward_azimuth)),
    ("reverse_azimuth", GeodesicField::F64(|i| i.reverse_azimuth)),
];

static GEODESIC_DIRECT_FIELDS: [(&str, GeodesicField<crs::GeodesicDirectInfo>); 3] = [
    ("longitude", GeodesicField::F64(|i| i.longitude)),
    ("latitude", GeodesicField::F64(|i| i.latitude)),
    ("final_azimuth", GeodesicField::F64(|i| i.final_azimuth)),
];

static GEODESIC_INTERPOLATE_FIELDS: [(&str, GeodesicField<crs::GeodesicInterpolateInfo>); 4] = [
    ("longitude", GeodesicField::F64(|i| i.longitude)),
    ("latitude", GeodesicField::F64(|i| i.latitude)),
    ("final_azimuth", GeodesicField::F64(|i| i.final_azimuth)),
    ("distance", GeodesicField::F64(|i| i.distance)),
];

/// Scalar geodesic-inverse fast path: returns Some(dict) only when every
/// coordinate is a plain finite float (and z1/z2 are both absent or both finite
/// floats), calling the scalar kernel directly. Returns None to fall back to
/// the vectorized/broadcast path for array or non-float inputs.
pub(crate) fn crs_geodesic_scalar_fast_path(
    py: Python<'_>,
    crs: &str,
    lon1: &Bound<'_, PyAny>,
    lat1: &Bound<'_, PyAny>,
    lon2: &Bound<'_, PyAny>,
    lat2: &Bound<'_, PyAny>,
    z1: Option<&Bound<'_, PyAny>>,
    z2: Option<&Bound<'_, PyAny>>,
    angle_unit: crs::AngleUnit,
) -> PyResult<Option<Py<PyDict>>> {
    // Only probe built-in scalar values here. Calling `__float__` to decide
    // whether to use the fast path would call a stateful provider again when
    // the general broadcast path is selected.
    let plain = |value: &Bound<'_, PyAny>| {
        value.cast::<pyo3::types::PyInt>().is_ok() || value.cast::<pyo3::types::PyFloat>().is_ok()
    };
    if ![lon1, lat1, lon2, lat2].into_iter().all(plain)
        || z1.is_some_and(|value| !value.is_none() && !plain(value))
        || z2.is_some_and(|value| !value.is_none() && !plain(value))
    {
        return Ok(None);
    }
    let z1_scalar = match z1 {
        Some(value) if !value.is_none() => Some(crate::finite_coordinate_required("z1", value)?),
        _ => None,
    };
    let z2_scalar = match z2 {
        Some(value) if !value.is_none() => Some(crate::finite_coordinate_required("z2", value)?),
        _ => None,
    };
    let z_pair = match (z1_scalar, z2_scalar) {
        (None, None) => Some((None, None)),
        (Some(a), Some(b)) => Some((Some(a), Some(b))),
        _ => None,
    };
    let Some((z1f, z2f)) = z_pair else {
        return Ok(None);
    };
    let lon1f = crate::finite_coordinate_required("lon1", lon1)?;
    let lat1f = crate::finite_coordinate_required("lat1", lat1)?;
    let lon2f = crate::finite_coordinate_required("lon2", lon2)?;
    let lat2f = crate::finite_coordinate_required("lat2", lat2)?;
    let info =
        py.detach(|| crs::geodesic_inverse(crs, lon1f, lat1f, lon2f, lat2f, z1f, z2f, angle_unit))?;
    geodesic_rows_to_dict(
        py,
        std::slice::from_ref(&info),
        true,
        &GEODESIC_INVERSE_FIELDS,
    )
    .map(Some)
}

pub(crate) fn crs_geodesic_direct_scalar_fast_path(
    py: Python<'_>,
    crs: &str,
    lon: &Bound<'_, PyAny>,
    lat: &Bound<'_, PyAny>,
    azimuth: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    angle_unit: crs::AngleUnit,
) -> PyResult<Option<Py<PyDict>>> {
    let plain = |value: &Bound<'_, PyAny>| {
        value.cast::<pyo3::types::PyInt>().is_ok() || value.cast::<pyo3::types::PyFloat>().is_ok()
    };
    if ![lon, lat, azimuth, distance].into_iter().all(plain) {
        return Ok(None);
    }
    let lon = crate::finite_coordinate_required("lon", lon)?;
    let lat = crate::finite_coordinate_required("lat", lat)?;
    let azimuth = crate::finite_f64_required("azimuth", azimuth)?;
    let distance = crate::finite_f64_required("distance", distance)?;
    let info = py.detach(|| crs::geodesic_direct(crs, lon, lat, azimuth, distance, angle_unit))?;
    geodesic_rows_to_dict(
        py,
        std::slice::from_ref(&info),
        true,
        &GEODESIC_DIRECT_FIELDS,
    )
    .map(Some)
}

/// Geodesic distance/azimuths between coordinates on a CRS ellipsoid.
///
/// Parameters
/// ----------
/// crs : str or int
///     CRS as an EPSG code or authority/WKT string.
///
/// lon1, lat1, lon2, lat2 : float
///     The two endpoint coordinates.
///
/// Returns
/// -------
/// dict
pub(crate) fn crs_geodesic(
    py: Python<'_>,
    crs: &Bound<'_, PyAny>,
    lon1: &Bound<'_, PyAny>,
    lat1: &Bound<'_, PyAny>,
    lon2: &Bound<'_, PyAny>,
    lat2: &Bound<'_, PyAny>,
    z1: Option<&Bound<'_, PyAny>>,
    z2: Option<&Bound<'_, PyAny>>,
    angle_unit: crs::AngleUnit,
) -> PyResult<Py<PyDict>> {
    let crs = crs_normalize(crs)?;
    // Scalar fast path: plain floats with matching optional z bypass the batch
    // machinery (six per-call `Vec` allocations, broadcasting, and a result
    // `Vec`) and call the scalar kernel directly. This is the common call shape.
    if let Some(dict) =
        crs_geodesic_scalar_fast_path(py, &crs, lon1, lat1, lon2, lat2, z1, z2, angle_unit)?
    {
        return Ok(dict);
    }
    let mut expected = None;
    for (value, name) in [
        (lon1, "lon1"),
        (lat1, "lat1"),
        (lon2, "lon2"),
        (lat2, "lat2"),
    ] {
        if expected.is_none() {
            expected = crate::coordinate_sequence_len_hint(value, name)?;
        }
    }
    for (value, name) in [(z1, "z1"), (z2, "z2")] {
        if expected.is_none()
            && let Some(value) = value
            && !value.is_none()
        {
            expected = crate::coordinate_sequence_len_hint(value, name)?;
        }
    }
    let mut lon1 = geodesic_coordinate_input_with_expected(py, lon1, "lon1", expected)?;
    let expected = expected.or_else(|| (!lon1.scalar).then_some(lon1.values.len()));
    let mut lat1 = geodesic_coordinate_input_with_expected(py, lat1, "lat1", expected)?;
    let expected = expected.or_else(|| (!lat1.scalar).then_some(lat1.values.len()));
    let mut lon2 = geodesic_coordinate_input_with_expected(py, lon2, "lon2", expected)?;
    let expected = expected.or_else(|| (!lon2.scalar).then_some(lon2.values.len()));
    let mut lat2 = geodesic_coordinate_input_with_expected(py, lat2, "lat2", expected)?;
    let expected = expected.or_else(|| (!lat2.scalar).then_some(lat2.values.len()));
    let z1 = optional_geodesic_coordinate_input_with_expected(py, z1, "z1", expected)?;
    let expected = expected.or_else(|| {
        z1.as_ref()
            .filter(|value| !value.scalar)
            .map(|value| value.values.len())
    });
    let z2 = optional_geodesic_coordinate_input_with_expected(py, z2, "z2", expected)?;
    if z1.is_some() != z2.is_some() {
        return Err(InvalidGeometryError::new_err(
            "geodesic height distance requires both z1 and z2",
        ));
    }
    let mut z1 = z1;
    let mut z2 = z2;
    let scalar = lon1.scalar
        && lat1.scalar
        && lon2.scalar
        && lat2.scalar
        && z1.as_ref().is_none_or(|value| value.scalar)
        && z2.as_ref().is_none_or(|value| value.scalar);
    if let (Some(z1), Some(z2)) = (&mut z1, &mut z2) {
        broadcast_coordinate_group(
            [
                (&mut lon1, "lon1"),
                (&mut lat1, "lat1"),
                (&mut lon2, "lon2"),
                (&mut lat2, "lat2"),
                (z1, "z1"),
                (z2, "z2"),
            ],
            "lon1, lat1, lon2, lat2, z1, and z2",
        )?;
    } else {
        broadcast_coordinate_group(
            [
                (&mut lon1, "lon1"),
                (&mut lat1, "lat1"),
                (&mut lon2, "lon2"),
                (&mut lat2, "lat2"),
            ],
            "lon1, lat1, lon2, and lat2",
        )?;
    }
    let items = py.detach(|| {
        crs::geodesic_inverses(
            &crs,
            &lon1.values,
            &lat1.values,
            &lon2.values,
            &lat2.values,
            z1.as_ref().map(|value| value.values.as_slice()),
            z2.as_ref().map(|value| value.values.as_slice()),
            angle_unit,
        )
    })?;
    geodesic_rows_to_dict(py, &items, scalar, &GEODESIC_INVERSE_FIELDS)
}

/// Direct geodesic problem on a CRS ellipsoid.
///
/// Parameters
/// ----------
/// crs : str or int
///     CRS as an EPSG code or authority/WKT string.
/// lon, lat : float or sequence of float
///     Start point, scalar or batch (degrees unless ``radians``).
/// azimuth, distance : float or sequence of float
///     Forward azimuth(s) in degrees and distance(s) in meters.
///
/// Returns
/// -------
/// coordinate
pub(crate) fn crs_geodesic_direct(
    py: Python<'_>,
    crs: &Bound<'_, PyAny>,
    lon: &Bound<'_, PyAny>,
    lat: &Bound<'_, PyAny>,
    azimuth: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    angle_unit: crs::AngleUnit,
) -> PyResult<Py<PyDict>> {
    let crs = crs_normalize(crs)?;
    if let Some(dict) =
        crs_geodesic_direct_scalar_fast_path(py, &crs, lon, lat, azimuth, distance, angle_unit)?
    {
        return Ok(dict);
    }
    let mut expected = None;
    for (value, name) in [
        (lon, "lon"),
        (lat, "lat"),
        (azimuth, "azimuth"),
        (distance, "distance"),
    ] {
        if expected.is_none() {
            expected = crate::coordinate_sequence_len_hint(value, name)?;
        }
    }
    let mut lon = geodesic_coordinate_input_with_expected(py, lon, "lon", expected)?;
    let expected = expected.or_else(|| (!lon.scalar).then_some(lon.values.len()));
    let mut lat = geodesic_coordinate_input_with_expected(py, lat, "lat", expected)?;
    let expected = expected.or_else(|| (!lat.scalar).then_some(lat.values.len()));
    let mut azimuth = parameter_input_with_expected(py, azimuth, "azimuth", expected, &|| {
        "azimuth must be finite".into()
    })?;
    let expected = expected.or_else(|| (!azimuth.scalar).then_some(azimuth.values.len()));
    let mut distance = parameter_input_with_expected(py, distance, "distance", expected, &|| {
        "distance must be finite".into()
    })?;
    let scalar = lon.scalar && lat.scalar && azimuth.scalar && distance.scalar;
    broadcast_coordinate_group(
        [
            (&mut lon, "lon"),
            (&mut lat, "lat"),
            (&mut azimuth, "azimuth"),
            (&mut distance, "distance"),
        ],
        "lon, lat, azimuth, and distance",
    )?;
    if scalar {
        let item = py.detach(|| {
            crs::geodesic_direct(
                &crs,
                lon.values[0],
                lat.values[0],
                azimuth.values[0],
                distance.values[0],
                angle_unit,
            )
        })?;
        return geodesic_rows_to_dict(
            py,
            std::slice::from_ref(&item),
            true,
            &GEODESIC_DIRECT_FIELDS,
        );
    }
    let columns = py.detach(|| {
        crs::geodesic_direct_columns(
            &crs,
            &lon.values,
            &lat.values,
            &azimuth.values,
            &distance.values,
            angle_unit,
        )
    })?;
    geodesic_direct_columns_to_dict(py, columns)
}

/// Interpolate along a geodesic on a CRS ellipsoid.
///
/// Parameters
/// ----------
/// crs : str or int
///     CRS as an EPSG code or authority/WKT string.
///
/// lon1, lat1, lon2, lat2 : float
///     The two endpoint coordinates.
///
/// distance : float
///     Distance from the first point (``[0, 1]`` fraction if `normalized`).
///
/// normalized : bool, default False
///     Treat `distance` as a fraction of the total geodesic length.
///
/// Returns
/// -------
/// coordinate
pub(crate) fn crs_geodesic_interpolate(
    py: Python<'_>,
    crs: &Bound<'_, PyAny>,
    lon1: &Bound<'_, PyAny>,
    lat1: &Bound<'_, PyAny>,
    lon2: &Bound<'_, PyAny>,
    lat2: &Bound<'_, PyAny>,
    distance: &Bound<'_, PyAny>,
    distance_mode: crs::DistanceMode,
    angle_unit: crs::AngleUnit,
) -> PyResult<Py<PyDict>> {
    let crs = crs_normalize(crs)?;
    let mut expected = None;
    for (value, name) in [
        (lon1, "lon1"),
        (lat1, "lat1"),
        (lon2, "lon2"),
        (lat2, "lat2"),
        (distance, "distance"),
    ] {
        if expected.is_none() {
            expected = crate::coordinate_sequence_len_hint(value, name)?;
        }
    }
    let mut lon1 = geodesic_coordinate_input_with_expected(py, lon1, "lon1", expected)?;
    let expected = expected.or_else(|| (!lon1.scalar).then_some(lon1.values.len()));
    let mut lat1 = geodesic_coordinate_input_with_expected(py, lat1, "lat1", expected)?;
    let expected = expected.or_else(|| (!lat1.scalar).then_some(lat1.values.len()));
    let mut lon2 = geodesic_coordinate_input_with_expected(py, lon2, "lon2", expected)?;
    let expected = expected.or_else(|| (!lon2.scalar).then_some(lon2.values.len()));
    let mut lat2 = geodesic_coordinate_input_with_expected(py, lat2, "lat2", expected)?;
    let expected = expected.or_else(|| (!lat2.scalar).then_some(lat2.values.len()));
    let mut distance = parameter_input_with_expected(py, distance, "distance", expected, &|| {
        "distance must be finite".into()
    })?;
    let scalar = lon1.scalar && lat1.scalar && lon2.scalar && lat2.scalar && distance.scalar;
    broadcast_coordinate_group(
        [
            (&mut lon1, "lon1"),
            (&mut lat1, "lat1"),
            (&mut lon2, "lon2"),
            (&mut lat2, "lat2"),
            (&mut distance, "distance"),
        ],
        "lon1, lat1, lon2, lat2, and distance",
    )?;
    let items = py.detach(|| {
        crs::geodesic_interpolates(
            &crs,
            &lon1.values,
            &lat1.values,
            &lon2.values,
            &lat2.values,
            &distance.values,
            distance_mode,
            angle_unit,
        )
    })?;
    geodesic_rows_to_dict(py, &items, scalar, &GEODESIC_INTERPOLATE_FIELDS)
}

pub(crate) fn geodesic_coordinate_input_with_expected(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    name: &str,
    expected: Option<usize>,
) -> PyResult<CoordinateInput> {
    crate::coordinate_input_with_expected(py, value, name, expected, &|| {
        format!("{name} must be finite")
    })
}

pub(crate) fn optional_geodesic_coordinate_input_with_expected(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
    expected: Option<usize>,
) -> PyResult<Option<CoordinateInput>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    geodesic_coordinate_input_with_expected(py, value, name, expected).map(Some)
}

#[cfg(test)]
mod allocation_tests {
    use pyo3::exceptions::PyMemoryError;
    use pyo3::prelude::*;
    use pyo3::types::PyString;

    use super::*;
    use crate::crs::RUNTIME_STATE_TEST_LOCK;
    use crate::error::{ErrorKind, ResourceError};
    use crate::test_support::{allocation_count, fail_allocations_after};

    #[expect(
        clippy::panic_in_result_fn,
        reason = "the helper uses assertions to classify every injected allocation failure"
    )]
    fn assert_bulk_public_publication_allocations<T, F, P>(
        name: &str,
        assemble: F,
        publish: P,
    ) -> PyResult<()>
    where
        F: Fn() -> crate::error::Result<T>,
        P: Fn(T) -> PyResult<Py<PyDict>>,
    {
        // Fault injection covers the Rust-owned bulk result. Publication is
        // performed after the guard is dropped because NumPy owns its C-side
        // allocation; the public Python suite exercises that boundary.
        drop(publish(assemble().map_err(PyErr::from)?)?);
        let (_, allocations) = allocation_count(&assemble);
        assert!(allocations > 0, "{name} did not allocate native output");
        for allowed in 0..allocations {
            let result = fail_allocations_after(allowed, &assemble);
            let Err(error) = result else {
                panic!("{name} prefix {allowed} unexpectedly succeeded");
            };
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
            drop(publish(assemble().map_err(PyErr::from)?)?);
        }
        Ok(())
    }

    #[test]
    #[expect(
        clippy::panic_in_result_fn,
        clippy::needless_borrows_for_generic_args,
        reason = "the test classifies every injected normalization failure and retries the same closure"
    )]
    fn crs_normalization_allocation_failures_return_memory_error() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
            let value = PyString::new(py, "EPSG:4326");
            let run = || crs_normalize(value.as_any());
            drop(run()?);
            let (_, allocations) = allocation_count(&run);
            assert!(allocations > 0, "CRS normalization did not allocate");
            for allowed in 0..allocations {
                let result = fail_allocations_after(allowed, &run);
                let Err(error) = result else {
                    panic!("CRS normalization prefix {allowed} unexpectedly succeeded");
                };
                assert!(error.is_instance_of::<PyMemoryError>(py));
                drop(error);
                drop(run()?);
            }
            Ok(())
        })
    }

    #[test]
    fn public_geodesic_column_operations_propagate_allocation_failures() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
            assert_bulk_public_publication_allocations(
                "inverse",
                || {
                    crs::geodesic_inverses(
                        "EPSG:4326",
                        &[-73.0, -72.0, -71.0],
                        &[41.0, 42.0, 43.0],
                        &[-74.0, -73.0, -72.0],
                        &[42.0, 43.0, 44.0],
                        None,
                        None,
                        crs::AngleUnit::Degrees,
                    )
                },
                |items| geodesic_rows_to_dict(py, &items, false, &GEODESIC_INVERSE_FIELDS),
            )?;

            assert_bulk_public_publication_allocations(
                "direct",
                || {
                    crs::geodesic_direct_columns(
                        "EPSG:4326",
                        &[-73.0, -72.0, -71.0],
                        &[41.0, 42.0, 43.0],
                        &[45.0, 90.0, 135.0],
                        &[1_000.0, 2_000.0, 3_000.0],
                        crs::AngleUnit::Degrees,
                    )
                },
                |columns| geodesic_direct_columns_to_dict(py, columns),
            )?;

            assert_bulk_public_publication_allocations(
                "interpolate",
                || {
                    crs::geodesic_interpolates(
                        "EPSG:4326",
                        &[-73.0, -72.0, -71.0],
                        &[41.0, 42.0, 43.0],
                        &[-74.0, -73.0, -72.0],
                        &[42.0, 43.0, 44.0],
                        &[0.0, 1_000.0, -500.0],
                        crs::DistanceMode::Absolute,
                        crs::AngleUnit::Degrees,
                    )
                },
                |items| geodesic_rows_to_dict(py, &items, false, &GEODESIC_INTERPOLATE_FIELDS),
            )
        })
    }
}
