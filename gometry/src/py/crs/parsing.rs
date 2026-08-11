use crate::py::crs::{
    Bound, CRSError, Crs, PyAny, PyAnyMethods as _, PyBool, PyCrs, PyDict, PyDictMethods as _,
    PyInt, PyList, PyListMethods as _, PyResult, PyStringMethods as _, PyTuple,
    PyTupleMethods as _, PyTypeError, coordinate_input, crs, crs_arc, finite_f64_required,
};
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

fn cf_ellipsoid(dict: &Bound<'_, PyDict>) -> PyResult<(f64, Option<f64>, String)> {
    let earth_radius = dict_f64(dict, "earth_radius")?;
    let semi_major_axis = dict_f64(dict, "semi_major_axis")?;
    let semi_minor_axis = dict_f64(dict, "semi_minor_axis")?;
    let inverse_flattening = dict_f64(dict, "inverse_flattening")?;
    let (semi_major, ellipsoid) = match (
        earth_radius,
        semi_major_axis,
        semi_minor_axis,
        inverse_flattening,
    ) {
        (Some(radius), None, None, None) => (radius, format!("+a={radius} +b={radius}")),
        (Some(_), ..) => {
            return Err(CRSError::new_err(
                "CF CRS dictionary earth_radius cannot be combined with another ellipsoid descriptor",
            ));
        },
        (None, Some(semi_major), Some(semi_minor), None) => {
            (semi_major, format!("+a={semi_major} +b={semi_minor}"))
        },
        (None, Some(semi_major), Some(semi_minor), Some(inverse_flattening)) => {
            let implied_semi_minor = cf_semi_minor_axis(semi_major, inverse_flattening)?;
            let tolerance = 8.0 * f64::EPSILON * semi_minor.abs().max(implied_semi_minor.abs());
            if (semi_minor - implied_semi_minor).abs() > tolerance {
                return Err(CRSError::new_err(
                    "CF CRS dictionary semi_minor_axis and inverse_flattening are contradictory ellipsoid descriptors",
                ));
            }
            (semi_major, format!("+a={semi_major} +b={semi_minor}"))
        },
        (None, Some(semi_major), None, Some(0.0)) => {
            (semi_major, format!("+a={semi_major} +b={semi_major}"))
        },
        (None, Some(semi_major), None, Some(inverse_flattening)) if inverse_flattening > 0.0 => (
            semi_major,
            format!("+a={semi_major} +rf={inverse_flattening}"),
        ),
        (None, Some(_), None, Some(_)) => {
            return Err(CRSError::new_err(
                "CF CRS dictionary inverse_flattening must be zero or positive",
            ));
        },
        (None, Some(_), None, None) => {
            return Err(CRSError::new_err(
                "CF CRS dictionary requires semi_minor_axis or inverse_flattening",
            ));
        },
        (None, None, ..) => {
            return Err(CRSError::new_err(
                "CF CRS dictionary requires earth_radius or semi_major_axis",
            ));
        },
    };
    Ok((semi_major, inverse_flattening, ellipsoid))
}

fn cf_semi_minor_axis(semi_major: f64, inverse_flattening: f64) -> PyResult<f64> {
    if inverse_flattening == 0.0 {
        Ok(semi_major)
    } else if inverse_flattening > 0.0 {
        Ok(semi_major * (1.0 - inverse_flattening.recip()))
    } else {
        Err(CRSError::new_err(
            "CF CRS dictionary inverse_flattening must be zero or positive",
        ))
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
    let (semi_major, inverse_flattening, ellipsoid) = cf_ellipsoid(dict)?;
    let prime_meridian = dict_f64(dict, "longitude_of_prime_meridian")?
        .filter(|value| *value != 0.0)
        .map(|value| format!(" +pm={value}"))
        .unwrap_or_default();
    let base = format!("{ellipsoid}{prime_meridian}");

    if grid_mapping == "latitude_longitude" {
        return if (semi_major - 6_378_137.0).abs() <= f64::EPSILON
            && inverse_flattening.is_some_and(|value| (value - 298.257_223_563).abs() <= 1e-12)
        {
            Ok("OGC:CRS84".to_owned())
        } else {
            Ok(format!("+proj=longlat {base} +type=crs"))
        };
    }

    // CF false_easting/northing are in projected native units; PROJ +x_0/+y_0
    // are metres even when +units=us-ft / +units=ft (EPSG/PROJ pattern).
    let (units, to_metre) = cf_projected_units(dict)?;
    let false_easting = dict_f64(dict, "false_easting")?.unwrap_or(0.0) * to_metre;
    let false_northing = dict_f64(dict, "false_northing")?.unwrap_or(0.0) * to_metre;

    match grid_mapping {
        "transverse_mercator" => Ok(format!(
            "+proj=tmerc +lat_0={} +lon_0={} +k_0={} +x_0={false_easting} +y_0={false_northing} {base} +units={units} +type=crs",
            dict_f64(dict, "latitude_of_projection_origin")?.unwrap_or(0.0),
            required_cf_f64(dict, "longitude_of_central_meridian")?,
            required_cf_f64(dict, "scale_factor_at_central_meridian")?,
        )),
        "lambert_azimuthal_equal_area" => Ok(format!(
            "+proj=laea +lat_0={} +lon_0={} +x_0={false_easting} +y_0={false_northing} {base} +units={units} +type=crs",
            required_cf_f64(dict, "latitude_of_projection_origin")?,
            required_cf_f64(dict, "longitude_of_projection_origin")?,
        )),
        "lambert_conformal_conic" => {
            let (lat_1, lat_2) = cf_standard_parallels(dict)?;
            let lat_2 = lat_2
                .map(|value| format!(" +lat_2={value}"))
                .unwrap_or_default();
            Ok(format!(
                "+proj=lcc +lat_0={} +lon_0={} +lat_1={lat_1}{lat_2} +x_0={false_easting} +y_0={false_northing} {base} +units={units} +type=crs",
                dict_f64(dict, "latitude_of_projection_origin")?.unwrap_or(lat_1),
                required_cf_f64(dict, "longitude_of_central_meridian")?,
            ))
        },
        "mercator" => cf_mercator_proj(dict, &base, units, false_easting, false_northing),
        "polar_stereographic" => {
            cf_polar_stereographic_proj(dict, &base, units, false_easting, false_northing)
        },
        "lambert_cylindrical_equal_area" => Ok(format!(
            "+proj=cea +lat_ts={} +lon_0={} +x_0={false_easting} +y_0={false_northing} {base} +units={units} +type=crs",
            required_cf_f64(dict, "standard_parallel")?,
            dict_f64(dict, "longitude_of_central_meridian")?.unwrap_or(0.0),
        )),
        _ => unreachable!("unsupported CF grid mappings are rejected before conversion"),
    }
}

fn cf_mercator_proj(
    dict: &Bound<'_, PyDict>,
    base: &str,
    units: &str,
    false_easting: f64,
    false_northing: f64,
) -> PyResult<String> {
    let scale = dict_f64(dict, "scale_factor_at_projection_origin")?
        .map(|value| format!(" +k_0={value}"))
        .unwrap_or_default();
    let lat_ts = cf_optional_standard_parallel(dict)?
        .map(|value| format!(" +lat_ts={value}"))
        .unwrap_or_default();
    Ok(format!(
        "+proj=merc +lat_0={} +lon_0={}{scale}{lat_ts} +x_0={false_easting} +y_0={false_northing} {base} +units={units} +type=crs",
        dict_f64(dict, "latitude_of_projection_origin")?.unwrap_or(0.0),
        dict_f64(dict, "longitude_of_projection_origin")?.unwrap_or(0.0),
    ))
}

/// Polar stereo by defining parameters (CF/EPSG), not method names:
///   Variant A: `scale_factor_at_projection_origin` → `+k_0`
///   Variant B: `standard_parallel` → `+lat_ts`
fn cf_polar_stereographic_proj(
    dict: &Bound<'_, PyDict>,
    base: &str,
    units: &str,
    false_easting: f64,
    false_northing: f64,
) -> PyResult<String> {
    let lon_0 = required_cf_f64(dict, "straight_vertical_longitude_from_pole")?;
    if let Some(k0) = dict_f64(dict, "scale_factor_at_projection_origin")? {
        let lat_0 = dict_f64(dict, "latitude_of_projection_origin")?.unwrap_or(90.0);
        // Reject empty/extra standard_parallel if the key is present.
        if dict.get_item("standard_parallel")?.is_some() {
            let _ = cf_optional_standard_parallel(dict)?;
        }
        return Ok(format!(
            "+proj=stere +lat_0={lat_0} +lon_0={lon_0} +k_0={k0} +x_0={false_easting} +y_0={false_northing} {base} +units={units} +type=crs"
        ));
    }
    let lat_ts = cf_optional_standard_parallel(dict)?.unwrap_or(90.0);
    let lat_0 = dict_f64(dict, "latitude_of_projection_origin")?
        .unwrap_or_else(|| if lat_ts < 0.0 { -90.0 } else { 90.0 });
    Ok(format!(
        "+proj=stere +lat_0={lat_0} +lat_ts={lat_ts} +lon_0={lon_0} +x_0={false_easting} +y_0={false_northing} {base} +units={units} +type=crs"
    ))
}

/// Optional `standard_parallel` for polar stereo / mercator: absent → `None`;
/// present → exactly one finite value (empty list / extras raise).
pub(super) fn cf_optional_standard_parallel(dict: &Bound<'_, PyDict>) -> PyResult<Option<f64>> {
    let Some(value) = dict.get_item("standard_parallel")? else {
        return Ok(None);
    };
    let values = coordinate_input(dict.py(), &value, "standard_parallel")?;
    match values.values.as_slice() {
        [first] => Ok(Some(*first)),
        [] => Err(CRSError::new_err(
            "CF CRS dictionary standard_parallel must not be empty",
        )),
        _ => Err(CRSError::new_err(
            "CF CRS dictionary standard_parallel must have exactly 1 value for this projection",
        )),
    }
}

/// Projected linear units from admitted CF metadata (`units` / `proj_units`).
///
/// Returns `(+units= token, metres per native unit)` so false easting/northing
/// can be converted to the metre-valued `+x_0`/`+y_0` PROJ expects even when
/// the projected CRS is foot-based. Defaults to metres when omitted.
fn cf_projected_units(dict: &Bound<'_, PyDict>) -> PyResult<(&'static str, f64)> {
    let Some(text) = dict_string(dict, "units")?.or(dict_string(dict, "proj_units")?) else {
        return Ok(("m", 1.0));
    };
    match text.as_str() {
        "m" | "metre" | "meter" | "metres" | "meters" => Ok(("m", 1.0)),
        "ft" | "foot" | "feet" => Ok(("ft", 0.3048)),
        "us-ft" | "US survey foot" | "us_survey_foot" | "ftUS" => {
            Ok(("us-ft", 0.304_800_609_601_219))
        },
        other => Err(CRSError::new_err(format!(
            "unsupported CF projected units {other:?}; expected m, ft, or us-ft"
        ))),
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
