use crate::py::crs::*;
// ---- CRS/CF input parsing (relocated from the crate root; consumed here and
// by the crate-central `parse_crs`) ----

pub(super) fn crs_epsg_code(value: &Bound<'_, PyAny>) -> PyResult<Option<u32>> {
    if value.cast_exact::<PyBool>().is_ok() || value.cast::<PyInt>().is_err() {
        return Ok(None);
    }
    let code = value
        .extract::<i64>()
        .map_err(|_| CRSError::new_err("EPSG code is too large"))?;
    if code < 0 {
        return Err(CRSError::new_err("EPSG code must be non-negative"));
    }
    u32::try_from(code)
        .map(Some)
        .map_err(|_| CRSError::new_err("EPSG code is too large"))
}

pub(super) fn crs_text_from_object(
    value: &Bound<'_, PyAny>,
    depth: u8,
) -> PyResult<Option<String>> {
    let py = value.py();
    let mut to_authority_returned_none = false;
    let mut to_epsg_returned_none = false;
    if let Some(method) = value.getattr_opt(pyo3::intern!(py, "to_authority"))? {
        let authority = method.call0()?;
        if !authority.is_none() {
            return crs_text_from_authority_pair(&authority, "CRS object to_authority() return")?
                .map(Some)
                .ok_or_else(|| {
                    PyTypeError::new_err(
                        "CRS object to_authority() must return a two-item authority tuple or list",
                    )
                });
        }
        to_authority_returned_none = true;
    }
    if let Some(method) = value.getattr_opt(pyo3::intern!(py, "to_epsg"))? {
        let epsg = method.call0()?;
        if !epsg.is_none() {
            let Some(code) = crs_epsg_code(&epsg)? else {
                return Err(PyTypeError::new_err(
                    "CRS object to_epsg() must return an integer EPSG code or None",
                ));
            };
            return Ok(Some(format!("EPSG:{code}")));
        }
        to_epsg_returned_none = true;
    }
    for (method, attr) in [
        ("to_wkt", value.getattr_opt(pyo3::intern!(py, "to_wkt"))?),
        ("to_json", value.getattr_opt(pyo3::intern!(py, "to_json"))?),
    ] {
        if let Some(attr) = attr {
            let text = attr.call0()?.extract::<String>().map_err(|_| {
                PyTypeError::new_err(format!("CRS object {method}() must return a string"))
            })?;
            if text.trim().is_empty() {
                return Err(CRSError::new_err(format!(
                    "CRS object {method}() returned an empty string"
                )));
            }
            return Ok(Some(text));
        }
    }
    if let Some(text) = crs_text_from_holder(value, depth)? {
        return Ok(Some(text));
    }
    if to_authority_returned_none {
        return Err(CRSError::new_err("CRS object to_authority() returned None"));
    }
    if to_epsg_returned_none {
        return Err(CRSError::new_err("CRS object to_epsg() returned None"));
    }
    Ok(None)
}

pub(super) fn crs_text_from_holder(
    value: &Bound<'_, PyAny>,
    depth: u8,
) -> PyResult<Option<String>> {
    let py = value.py();
    for nested in [
        value.getattr_opt(pyo3::intern!(py, "crs"))?,
        value.getattr_opt(pyo3::intern!(py, "srs"))?,
    ]
    .into_iter()
    .flatten()
    {
        if nested.is_none() {
            continue;
        }
        if let Some(crs) = parse_crs_inner(&nested, depth + 1)? {
            return Ok(Some(crs.to_string()));
        }
    }
    Ok(None)
}

pub(super) fn crs_text_from_authority_pair(
    value: &Bound<'_, PyAny>,
    label: &str,
) -> PyResult<Option<String>> {
    if let Ok(tuple) = value.cast::<PyTuple>() {
        if tuple.len() != 2 {
            return Err(CRSError::new_err(format!(
                "{label} must contain exactly authority and code"
            )));
        }
        let authority = crs_authority_tuple_part(&tuple.get_item(0)?, label, "authority")?;
        let code = crs_authority_tuple_code(&tuple.get_item(1)?, label)?;
        return Ok(Some(format!("{authority}:{code}")));
    }
    if let Ok(list) = value.cast::<PyList>() {
        if list.len() != 2 {
            return Err(CRSError::new_err(format!(
                "{label} must contain exactly authority and code"
            )));
        }
        let authority = crs_authority_tuple_part(&list.get_item(0)?, label, "authority")?;
        let code = crs_authority_tuple_code(&list.get_item(1)?, label)?;
        return Ok(Some(format!("{authority}:{code}")));
    }
    Ok(None)
}

pub(super) fn crs_authority_tuple_part(
    value: &Bound<'_, PyAny>,
    label: &str,
    field: &str,
) -> PyResult<String> {
    let text = value
        .extract::<String>()
        .map_err(|_| PyTypeError::new_err(format!("{label} {field} must be a string")))?;
    let text = text.trim();
    if text.is_empty() {
        return Err(CRSError::new_err(format!(
            "{label} {field} must be non-empty"
        )));
    }
    Ok(text.to_owned())
}

pub(super) fn crs_authority_tuple_code(value: &Bound<'_, PyAny>, label: &str) -> PyResult<String> {
    if value.cast_exact::<PyBool>().is_err() && value.cast::<PyInt>().is_ok() {
        let code = value
            .extract::<i64>()
            .map_err(|_| CRSError::new_err(format!("{label} code is too large")))?;
        if code < 0 {
            return Err(CRSError::new_err(format!(
                "{label} code must be non-negative"
            )));
        }
        return Ok(code.to_string());
    }
    crs_authority_tuple_part(value, label, "code")
}

pub(super) fn crs_text_from_dict(dict: &Bound<'_, PyDict>) -> PyResult<String> {
    if let Some(text) = dict_string(dict, "crs_wkt")? {
        if text.trim().is_empty() {
            return Err(CRSError::new_err(
                "crs_wkt CRS dictionary value must be non-empty",
            ));
        }
        return Ok(text);
    }
    if let Some(text) = dict_string(dict, "spatial_ref")? {
        if text.trim().is_empty() {
            return Err(CRSError::new_err(
                "spatial_ref CRS dictionary value must be non-empty",
            ));
        }
        return Ok(text);
    }
    if let Some(grid_mapping) = dict_string(dict, "grid_mapping_name")? {
        return cf_grid_mapping_to_proj(dict, &grid_mapping);
    }
    serde_json::to_string(&crate::py_to_json_value(dict)?).map_err(|error| {
        CRSError::new_err(format!("CRS dictionary is not JSON-serializable: {error}"))
    })
}

pub(super) fn dict_string(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<String>> {
    dict.get_item(key)?
        .map(|value| value.extract::<String>())
        .transpose()
}

pub(super) fn dict_f64(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<f64>> {
    dict.get_item(key)?
        .map(|value| finite_f64_required(key, &value))
        .transpose()
}

pub(super) fn required_cf_f64(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<f64> {
    dict_f64(dict, key)?
        .ok_or_else(|| CRSError::new_err(format!("CF CRS dictionary requires {key}")))
}

pub(super) fn cf_float_or_first(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<f64>> {
    if let Some(value) = dict.get_item(key)? {
        let values = coordinate_input(dict.py(), &value, key)?;
        return Ok(values.values.first().copied());
    }
    Ok(None)
}

/// Parse CF `standard_parallel` as one or two values; reject empty and extras.
pub(super) fn cf_standard_parallels(dict: &Bound<'_, PyDict>) -> PyResult<(f64, Option<f64>)> {
    let Some(value) = dict.get_item("standard_parallel")? else {
        return Err(CRSError::new_err(
            "CF CRS dictionary requires standard_parallel",
        ));
    };
    let values = coordinate_input(dict.py(), &value, "standard_parallel")?;
    match values.values.as_slice() {
        [first] => Ok((*first, None)),
        [first, second] => Ok((*first, Some(*second))),
        [] => Err(CRSError::new_err(
            "CF CRS dictionary requires standard_parallel",
        )),
        _ => Err(CRSError::new_err(
            "CF CRS dictionary standard_parallel must have 1 or 2 values",
        )),
    }
}

pub(super) fn cf_grid_mapping_to_proj(
    dict: &Bound<'_, PyDict>,
    grid_mapping: &str,
) -> PyResult<String> {
    if !matches!(
        grid_mapping,
        "latitude_longitude"
            | "transverse_mercator"
            | "lambert_azimuthal_equal_area"
            | "lambert_conformal_conic"
            | "mercator"
            | "polar_stereographic"
            | "lambert_cylindrical_equal_area"
    ) {
        return Err(CRSError::new_err(format!(
            "unsupported CF grid_mapping_name: {grid_mapping}"
        )));
    }
    let semi_major = required_cf_f64(dict, "semi_major_axis")?;
    let ellipsoid = if let Some(inverse_flattening) = dict_f64(dict, "inverse_flattening")? {
        format!("+a={semi_major} +rf={inverse_flattening}")
    } else {
        format!(
            "+a={semi_major} +b={}",
            required_cf_f64(dict, "semi_minor_axis")?
        )
    };
    let prime_meridian = dict_f64(dict, "longitude_of_prime_meridian")?
        .filter(|value| *value != 0.0)
        .map(|value| format!(" +pm={value}"))
        .unwrap_or_default();
    let base = format!("{ellipsoid}{prime_meridian}");
    let false_easting = dict_f64(dict, "false_easting")?.unwrap_or(0.0);
    let false_northing = dict_f64(dict, "false_northing")?.unwrap_or(0.0);

    match grid_mapping {
        "latitude_longitude" => {
            if (semi_major - 6_378_137.0).abs() <= f64::EPSILON
                && dict_f64(dict, "inverse_flattening")?
                    .is_some_and(|value| (value - 298.257_223_563).abs() <= 1e-12)
            {
                Ok("OGC:CRS84".to_owned())
            } else {
                Ok(format!("+proj=longlat {base} +type=crs"))
            }
        },
        "transverse_mercator" => Ok(format!(
            "+proj=tmerc +lat_0={} +lon_0={} +k_0={} +x_0={false_easting} +y_0={false_northing} {base} +units=m +type=crs",
            dict_f64(dict, "latitude_of_projection_origin")?.unwrap_or(0.0),
            required_cf_f64(dict, "longitude_of_central_meridian")?,
            required_cf_f64(dict, "scale_factor_at_central_meridian")?,
        )),
        "lambert_azimuthal_equal_area" => Ok(format!(
            "+proj=laea +lat_0={} +lon_0={} +x_0={false_easting} +y_0={false_northing} {base} +units=m +type=crs",
            required_cf_f64(dict, "latitude_of_projection_origin")?,
            required_cf_f64(dict, "longitude_of_projection_origin")?,
        )),
        "lambert_conformal_conic" => {
            let (lat_1, lat_2) = cf_standard_parallels(dict)?;
            let lat_2 = lat_2
                .map(|value| format!(" +lat_2={value}"))
                .unwrap_or_default();
            Ok(format!(
                "+proj=lcc +lat_0={} +lon_0={} +lat_1={lat_1}{lat_2} +x_0={false_easting} +y_0={false_northing} {base} +units=m +type=crs",
                dict_f64(dict, "latitude_of_projection_origin")?.unwrap_or(lat_1),
                required_cf_f64(dict, "longitude_of_central_meridian")?,
            ))
        },
        "mercator" => {
            let scale = dict_f64(dict, "scale_factor_at_projection_origin")?
                .map(|value| format!(" +k_0={value}"))
                .unwrap_or_default();
            let lat_ts = cf_float_or_first(dict, "standard_parallel")?
                .map(|value| format!(" +lat_ts={value}"))
                .unwrap_or_default();
            Ok(format!(
                "+proj=merc +lat_0={} +lon_0={}{scale}{lat_ts} +x_0={false_easting} +y_0={false_northing} {base} +units=m +type=crs",
                dict_f64(dict, "latitude_of_projection_origin")?.unwrap_or(0.0),
                dict_f64(dict, "longitude_of_projection_origin")?.unwrap_or(0.0),
            ))
        },
        "polar_stereographic" => {
            // Parse standard_parallel once (optional; default 90°).
            let lat_ts = cf_float_or_first(dict, "standard_parallel")?.unwrap_or(90.0);
            let lat_0 = dict_f64(dict, "latitude_of_projection_origin")?
                .unwrap_or_else(|| if lat_ts < 0.0 { -90.0 } else { 90.0 });
            Ok(format!(
                "+proj=stere +lat_0={lat_0} +lat_ts={lat_ts} +lon_0={} +x_0={false_easting} +y_0={false_northing} {base} +units=m +type=crs",
                required_cf_f64(dict, "straight_vertical_longitude_from_pole")?,
            ))
        },
        "lambert_cylindrical_equal_area" => Ok(format!(
            "+proj=cea +lat_ts={} +lon_0={} +x_0={false_easting} +y_0={false_northing} {base} +units=m +type=crs",
            required_cf_f64(dict, "standard_parallel")?,
            dict_f64(dict, "longitude_of_central_meridian")?.unwrap_or(0.0),
        )),
        _ => unreachable!("unsupported CF grid mappings are rejected before conversion"),
    }
}

/// Maximum nesting depth for CRS-holder objects (`.crs` / `.srs` indirection).
const MAX_CRS_HOLDER_DEPTH: u8 = 4;

pub(crate) fn parse_crs_inner(value: &Bound<'_, PyAny>, depth: u8) -> PyResult<Option<Crs>> {
    if depth > MAX_CRS_HOLDER_DEPTH {
        return Err(CRSError::new_err("CRS holder nesting is too deep"));
    }
    if value.is_none() {
        return Ok(None);
    }
    // Fast path for plain `str` CRS inputs (the common scalar call shape): a
    // string is never an authority tuple, mapping, or CRS-holder object, so skip
    // the type-dispatch ladder and its per-call attribute probes.
    if let Ok(text) = value.cast_exact::<pyo3::types::PyString>() {
        return Ok(Some(crs_arc(crs::canonicalize(&text.to_cow()?)?)));
    }
    // A `CRS` object already holds its canonical identifier — reuse the Arc.
    if let Ok(crs_obj) = value.cast::<PyCrs>() {
        return Ok(Some(crs_obj.get().canonical.clone()));
    }
    if let Some(code) = crs_epsg_code(value)? {
        return Ok(Some(crs_arc(crs::canonicalize(&format!("EPSG:{code}"))?)));
    }
    if let Some(text) = crs_text_from_authority_pair(value, "CRS authority tuple")? {
        return Ok(Some(crs_arc(crs::canonicalize(&text)?)));
    }
    if let Some(dict) = crate::mapping_as_dict(value)? {
        return Ok(Some(crs_arc(crs::canonicalize(&crs_text_from_dict(
            &dict,
        )?)?)));
    }
    if let Some(text) = crs_text_from_object(value, depth)? {
        return Ok(Some(crs_arc(crs::canonicalize(&text)?)));
    }
    let text = value.extract::<String>().map_err(|_| {
        PyTypeError::new_err(
            "CRS input must be an authority string, authority tuple, integer EPSG code, PROJJSON dictionary, CF dictionary, CRS-holder object, or object with to_wkt()/to_json()/to_authority()/to_epsg()",
        )
    })?;
    Ok(Some(crs_arc(crs::canonicalize(&text)?)))
}
