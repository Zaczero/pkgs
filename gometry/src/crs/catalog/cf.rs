use crate::crs::{
    CfValue, CrsCoordinateOperationInfo, CrsError, CrsInfo, DEGREE_TO_RADIAN,
    OperationParameterInfo, PrimeMeridianInfo,
};
use crate::error::Result;

pub(crate) fn is_transverse_mercator(operation: &CrsCoordinateOperationInfo) -> bool {
    is_operation_method(operation, "9807", "Transverse Mercator")
}

pub(crate) fn is_operation_method(
    operation: &CrsCoordinateOperationInfo,
    code: &str,
    name: &str,
) -> bool {
    operation.method.as_ref().is_some_and(|method| {
        method.code.as_deref() == Some(code) || method.name.as_deref() == Some(name)
    })
}

/// CF numeric quantity kind. The schema names the target unit; PROJ metadata
/// supplies only the conversion factor from the stored value.
#[derive(Clone, Copy)]
pub(crate) enum CfNumericKind {
    /// CF latitudes and longitudes, in degrees.
    Angle,
    /// False easting/northing, in the projected CRS axis unit.
    Length,
    /// Scale factors, unity.
    Scale,
}

#[derive(Clone, Copy)]
struct CfNumericExport<'a> {
    crs: &'a str,
    axis_to_metre: Option<f64>,
}

impl CfNumericExport<'_> {
    fn convert_parameter(
        &self,
        parameter: &OperationParameterInfo,
        kind: CfNumericKind,
    ) -> Result<f64> {
        let what = parameter
            .name
            .as_deref()
            .or(parameter.code.as_deref())
            .unwrap_or("parameter");
        cf_numeric_value(
            self.crs,
            parameter.value,
            parameter.unit_conversion_factor,
            kind,
            self.axis_to_metre,
            what,
        )
    }
}

fn cf_numeric_target_factor(
    crs: &str,
    kind: CfNumericKind,
    axis_to_metre: Option<f64>,
    what: &str,
) -> Result<f64> {
    match kind {
        CfNumericKind::Angle => Ok(DEGREE_TO_RADIAN),
        CfNumericKind::Scale => Ok(1.0),
        CfNumericKind::Length => {
            let Some(factor) = axis_to_metre else {
                return Err(CrsError::export(
                    crs,
                    "CF",
                    format!(
                        "cannot export {what} as CF: projected linear parameters require a projected axis unit"
                    ),
                ));
            };
            valid_factor(crs, factor, "projected axis")
        },
    }
}

fn valid_factor(crs: &str, factor: f64, what: &str) -> Result<f64> {
    if factor.is_finite() && factor > 0.0 {
        Ok(factor)
    } else {
        Err(CrsError::export(
            crs,
            "CF",
            format!(
                "cannot export {what} as CF: unit conversion factor must be finite and positive"
            ),
        ))
    }
}

#[expect(
    clippy::float_cmp,
    reason = "equal conversion factors are the same unit: skip scaling rather than approximating a no-op"
)]
fn cf_numeric_value(
    crs: &str,
    value: f64,
    source_factor: f64,
    kind: CfNumericKind,
    axis_to_metre: Option<f64>,
    what: &str,
) -> Result<f64> {
    if !value.is_finite() {
        return Err(CrsError::export(
            crs,
            "CF",
            format!("cannot export {what} as CF: value must be finite"),
        ));
    }
    let source_factor = valid_factor(crs, source_factor, what)?;
    let target_factor = cf_numeric_target_factor(crs, kind, axis_to_metre, what)?;
    if source_factor == target_factor {
        return Ok(value);
    }
    let converted = value * (source_factor / target_factor);
    if converted.is_finite() {
        Ok(converted)
    } else {
        Err(CrsError::export(
            crs,
            "CF",
            format!("cannot export {what} as CF: unit conversion overflowed"),
        ))
    }
}

/// Metres per unit of the first projected axis, or `None` for non-projected CRS.
///
/// Errors when the first two axes disagree: CF has one linear `units` token.
#[expect(
    clippy::float_cmp,
    reason = "CF has one linear units token: the two axes either declare the same conversion factor or they are anisotropic"
)]
pub(crate) fn cf_projected_axis_to_metre(crs: &str, info: &CrsInfo) -> Result<Option<f64>> {
    if info.kind != "projected" {
        return Ok(None);
    }
    let Some(first) = info.axes.first() else {
        return Ok(None);
    };
    let factor = valid_factor(crs, first.unit_conversion_factor, "projected axis")?;
    if let Some(second) = info.axes.get(1)
        && second.unit_conversion_factor != factor
    {
        return Err(CrsError::export(
            crs,
            "CF",
            format!(
                "projected axes have unequal unit conversion factors ({factor} vs {}); CF cannot represent anisotropic projected axes",
                second.unit_conversion_factor
            ),
        ));
    }
    Ok(Some(factor))
}

pub(crate) fn cf_prime_meridian_longitude_degrees(
    crs: &str,
    prime_meridian: &PrimeMeridianInfo,
) -> Result<f64> {
    cf_numeric_value(
        crs,
        prime_meridian.longitude,
        prime_meridian.unit_conversion_factor,
        CfNumericKind::Angle,
        None,
        "longitude_of_prime_meridian",
    )
}

fn add_cf_parameters(
    export: CfNumericExport<'_>,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    mappings: &[CfParameterMapping],
) -> Result<()> {
    for parameter in parameters {
        let Some(mapping) = cf_projection_parameter_mapping(parameter, mappings) else {
            continue;
        };
        items.push((
            mapping.cf_key,
            CfValue::Float(export.convert_parameter(parameter, mapping.kind)?),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct CfParameterMapping {
    pub codes: &'static [&'static str],
    pub names: &'static [&'static str],
    pub cf_key: &'static str,
    pub kind: CfNumericKind,
}

pub(crate) fn add_cf_projection_parameters(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    mappings: &[CfParameterMapping],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    add_cf_parameters(
        CfNumericExport { crs, axis_to_metre },
        items,
        parameters,
        mappings,
    )
}

fn cf_projection_parameter_mapping(
    parameter: &OperationParameterInfo,
    mappings: &[CfParameterMapping],
) -> Option<CfParameterMapping> {
    let code = parameter.code.as_deref();
    let name = parameter.name.as_deref();
    mappings.iter().copied().find(|mapping| {
        code.is_some_and(|code| mapping.codes.contains(&code))
            || name.is_some_and(|name| mapping.names.contains(&name))
    })
}

pub(crate) const CF_TRANSVERSE_MERCATOR_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8801"],
        names: &["Latitude of natural origin"],
        cf_key: "latitude_of_projection_origin",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_central_meridian",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8805"],
        names: &["Scale factor at natural origin"],
        cf_key: "scale_factor_at_central_meridian",
        kind: CfNumericKind::Scale,
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
        kind: CfNumericKind::Length,
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
        kind: CfNumericKind::Length,
    },
];

const CF_LAMBERT_AZIMUTHAL_EQUAL_AREA_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8801"],
        names: &["Latitude of natural origin"],
        cf_key: "latitude_of_projection_origin",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_projection_origin",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
        kind: CfNumericKind::Length,
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
        kind: CfNumericKind::Length,
    },
];

const CF_MERCATOR_FRAME: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8801"],
        names: &["Latitude of natural origin"],
        cf_key: "latitude_of_projection_origin",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_projection_origin",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
        kind: CfNumericKind::Length,
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
        kind: CfNumericKind::Length,
    },
];

const CF_MERCATOR_A_DEFINING: &[CfParameterMapping] = &[CfParameterMapping {
    codes: &["8805"],
    names: &["Scale factor at natural origin"],
    cf_key: "scale_factor_at_projection_origin",
    kind: CfNumericKind::Scale,
}];

const CF_MERCATOR_B_DEFINING: &[CfParameterMapping] = &[CfParameterMapping {
    codes: &["8823"],
    names: &["Latitude of 1st standard parallel"],
    cf_key: "standard_parallel",
    kind: CfNumericKind::Angle,
}];

const CF_POLAR_STEREOGRAPHIC_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8801"],
        names: &["Latitude of natural origin"],
        cf_key: "latitude_of_projection_origin",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "straight_vertical_longitude_from_pole",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8805"],
        names: &["Scale factor at natural origin"],
        cf_key: "scale_factor_at_projection_origin",
        kind: CfNumericKind::Scale,
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
        kind: CfNumericKind::Length,
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
        kind: CfNumericKind::Length,
    },
];

const CF_LAMBERT_CYLINDRICAL_EQUAL_AREA_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8823"],
        names: &["Latitude of 1st standard parallel"],
        cf_key: "standard_parallel",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_central_meridian",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
        kind: CfNumericKind::Length,
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
        kind: CfNumericKind::Length,
    },
];

const CF_LAMBERT_CONFORMAL_CONIC_1SP_FRAME: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_central_meridian",
        kind: CfNumericKind::Angle,
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
        kind: CfNumericKind::Length,
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
        kind: CfNumericKind::Length,
    },
];

fn parameter_matches(parameter: &OperationParameterInfo, code: &str, name: &str) -> bool {
    parameter.code.as_deref() == Some(code) || parameter.name.as_deref() == Some(name)
}

fn unique_matching_parameter<'a>(
    parameters: &'a [OperationParameterInfo],
    code: &str,
    name: &str,
) -> Option<&'a OperationParameterInfo> {
    let mut found = None;
    for parameter in parameters {
        if !parameter_matches(parameter, code, name) {
            continue;
        }
        if found.is_some() {
            return None;
        }
        found = Some(parameter);
    }
    found
}

fn is_lcc_1sp_natural_origin_latitude(parameter: &OperationParameterInfo) -> bool {
    parameter_matches(parameter, "8801", "Latitude of natural origin")
}

#[expect(
    clippy::float_cmp,
    reason = "stored unity scale is the exact 8805 identity (value 1 with conversion 1), not an approximate scale"
)]
fn cf_lcc_1sp_stored_unity_scale(parameters: &[OperationParameterInfo]) -> bool {
    unique_matching_parameter(parameters, "8805", "Scale factor at natural origin")
        .is_some_and(|parameter| parameter.value == 1.0 && parameter.unit_conversion_factor == 1.0)
}

/// Admit CF 1SP Lambert conformal when origin, meridian, and unity scale are unique.
///
/// Exactly one 8801, one 8802, and one stored-unity 8805 (code or name). Missing
/// or duplicate origin/meridian, or a non-unity scale, fail closed.
pub(crate) fn cf_lcc_1sp_admits_mapping(parameters: &[OperationParameterInfo]) -> bool {
    unique_matching_parameter(parameters, "8801", "Latitude of natural origin").is_some()
        && unique_matching_parameter(parameters, "8802", "Longitude of natural origin").is_some()
        && cf_lcc_1sp_stored_unity_scale(parameters)
}

/// True when Mercator variant B carries any 8805 scale (code or name).
pub(crate) fn cf_mercator_b_has_scale(parameters: &[OperationParameterInfo]) -> bool {
    parameters
        .iter()
        .any(|parameter| parameter_matches(parameter, "8805", "Scale factor at natural origin"))
}

pub(crate) fn add_cf_lambert_conformal_conic_1sp_parameters(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    let export = CfNumericExport { crs, axis_to_metre };
    let mut latitude_of_origin = None;
    for parameter in parameters {
        if is_lcc_1sp_natural_origin_latitude(parameter) {
            latitude_of_origin = Some(export.convert_parameter(parameter, CfNumericKind::Angle)?);
        }
    }
    if let Some(lat0) = latitude_of_origin {
        items.push(("latitude_of_projection_origin", CfValue::Float(lat0)));
    }
    add_cf_projection_parameters(
        crs,
        items,
        parameters,
        CF_LAMBERT_CONFORMAL_CONIC_1SP_FRAME,
        axis_to_metre,
    )?;
    if let Some(lat0) = latitude_of_origin {
        items.push(("standard_parallel", CfValue::Float(lat0)));
    }
    Ok(())
}

pub(crate) fn add_cf_lambert_azimuthal_equal_area_parameters(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    add_cf_projection_parameters(
        crs,
        items,
        parameters,
        CF_LAMBERT_AZIMUTHAL_EQUAL_AREA_PARAMETERS,
        axis_to_metre,
    )
}

pub(crate) fn add_cf_lambert_conformal_conic_parameters(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    let export = CfNumericExport { crs, axis_to_metre };
    let mut standard_parallel = Vec::new();
    for parameter in parameters {
        match parameter.code.as_deref() {
            Some("8821") => items.push((
                "latitude_of_projection_origin",
                CfValue::Float(export.convert_parameter(parameter, CfNumericKind::Angle)?),
            )),
            Some("8822") => items.push((
                "longitude_of_central_meridian",
                CfValue::Float(export.convert_parameter(parameter, CfNumericKind::Angle)?),
            )),
            Some("8823" | "8824") => {
                standard_parallel.push(export.convert_parameter(parameter, CfNumericKind::Angle)?);
            },
            Some("8826") => items.push((
                "false_easting",
                CfValue::Float(export.convert_parameter(parameter, CfNumericKind::Length)?),
            )),
            Some("8827") => items.push((
                "false_northing",
                CfValue::Float(export.convert_parameter(parameter, CfNumericKind::Length)?),
            )),
            _ => match parameter.name.as_deref() {
                Some("Latitude of false origin") => items.push((
                    "latitude_of_projection_origin",
                    CfValue::Float(export.convert_parameter(parameter, CfNumericKind::Angle)?),
                )),
                Some("Longitude of false origin") => items.push((
                    "longitude_of_central_meridian",
                    CfValue::Float(export.convert_parameter(parameter, CfNumericKind::Angle)?),
                )),
                Some("Latitude of 1st standard parallel" | "Latitude of 2nd standard parallel") => {
                    standard_parallel
                        .push(export.convert_parameter(parameter, CfNumericKind::Angle)?);
                },
                Some("Easting at false origin" | "False easting") => {
                    items.push((
                        "false_easting",
                        CfValue::Float(export.convert_parameter(parameter, CfNumericKind::Length)?),
                    ));
                },
                Some("Northing at false origin" | "False northing") => {
                    items.push((
                        "false_northing",
                        CfValue::Float(export.convert_parameter(parameter, CfNumericKind::Length)?),
                    ));
                },
                _ => {},
            },
        }
    }
    if standard_parallel.len() == 1 {
        items.push(("standard_parallel", CfValue::Float(standard_parallel[0])));
    } else if !standard_parallel.is_empty() {
        items.push(("standard_parallel", CfValue::FloatList(standard_parallel)));
    }
    Ok(())
}

fn add_cf_mercator_with_defining(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    defining: &[CfParameterMapping],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    add_cf_projection_parameters(crs, items, parameters, CF_MERCATOR_FRAME, axis_to_metre)?;
    add_cf_projection_parameters(crs, items, parameters, defining, axis_to_metre)
}

pub(crate) fn add_cf_mercator_variant_a_parameters(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    add_cf_mercator_with_defining(
        crs,
        items,
        parameters,
        CF_MERCATOR_A_DEFINING,
        axis_to_metre,
    )
}

pub(crate) fn add_cf_mercator_variant_b_parameters(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    add_cf_mercator_with_defining(
        crs,
        items,
        parameters,
        CF_MERCATOR_B_DEFINING,
        axis_to_metre,
    )
}

pub(crate) fn add_cf_polar_stereographic_parameters(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    let export = CfNumericExport { crs, axis_to_metre };
    let mut standard_parallel = None;
    let mut straight_vertical = None;
    add_cf_projection_parameters(
        crs,
        items,
        parameters,
        CF_POLAR_STEREOGRAPHIC_PARAMETERS,
        axis_to_metre,
    )?;
    for parameter in parameters {
        match parameter.code.as_deref() {
            Some("8832") => {
                standard_parallel =
                    Some(export.convert_parameter(parameter, CfNumericKind::Angle)?);
            },
            Some("8833") => {
                straight_vertical =
                    Some(export.convert_parameter(parameter, CfNumericKind::Angle)?);
            },
            _ => match parameter.name.as_deref() {
                Some("Latitude of standard parallel") => {
                    standard_parallel =
                        Some(export.convert_parameter(parameter, CfNumericKind::Angle)?);
                },
                Some("Longitude of origin") => {
                    straight_vertical =
                        Some(export.convert_parameter(parameter, CfNumericKind::Angle)?);
                },
                _ => {},
            },
        }
    }
    if let Some(value) = standard_parallel {
        items.push(("standard_parallel", CfValue::Float(value)));
        items.push((
            "latitude_of_projection_origin",
            CfValue::Float(if value < 0.0 { -90.0 } else { 90.0 }),
        ));
    }
    if let Some(value) = straight_vertical {
        items.push((
            "straight_vertical_longitude_from_pole",
            CfValue::Float(value),
        ));
    }
    Ok(())
}

pub(crate) fn add_cf_lambert_cylindrical_equal_area_parameters(
    crs: &str,
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    axis_to_metre: Option<f64>,
) -> Result<()> {
    add_cf_projection_parameters(
        crs,
        items,
        parameters,
        CF_LAMBERT_CYLINDRICAL_EQUAL_AREA_PARAMETERS,
        axis_to_metre,
    )
}

#[cfg(test)]
mod lcc_1sp_scale_tests {
    use super::{
        cf_lcc_1sp_admits_mapping, cf_lcc_1sp_stored_unity_scale, cf_mercator_b_has_scale,
    };
    use crate::crs::OperationParameterInfo;

    fn param(
        code: Option<&str>,
        name: Option<&str>,
        value: f64,
        factor: f64,
    ) -> OperationParameterInfo {
        OperationParameterInfo {
            name: name.map(str::to_owned),
            authority: Some("EPSG".to_owned()),
            code: code.map(str::to_owned),
            value,
            value_string: None,
            unit_conversion_factor: factor,
            unit_name: None,
            unit_authority: None,
            unit_code: None,
            unit_category: None,
        }
    }

    fn scale(value: f64, factor: f64) -> OperationParameterInfo {
        param(
            Some("8805"),
            Some("Scale factor at natural origin"),
            value,
            factor,
        )
    }

    #[test]
    fn stored_unity_admits() {
        assert!(cf_lcc_1sp_stored_unity_scale(&[scale(1.0, 1.0)]));
        assert!(cf_lcc_1sp_stored_unity_scale(&[param(
            None,
            Some("Scale factor at natural origin"),
            1.0,
            1.0,
        )]));
        assert!(cf_lcc_1sp_stored_unity_scale(&[param(
            Some("8805"),
            None,
            1.0,
            1.0,
        )]));
    }

    #[test]
    fn missing_duplicate_and_non_unity_fail_closed() {
        assert!(!cf_lcc_1sp_stored_unity_scale(&[]));
        assert!(!cf_lcc_1sp_stored_unity_scale(&[param(
            Some("8801"),
            Some("Latitude of natural origin"),
            18.0,
            1.0,
        )]));
        assert!(!cf_lcc_1sp_stored_unity_scale(&[
            scale(1.0, 1.0),
            param(Some("8805"), None, 1.0, 1.0),
        ]));
        assert!(!cf_lcc_1sp_stored_unity_scale(&[
            scale(1.0, 1.0),
            param(None, Some("Scale factor at natural origin"), 1.0, 1.0),
        ]));
        assert!(!cf_lcc_1sp_stored_unity_scale(&[scale(0.999_877_499, 1.0)]));
        assert!(!cf_lcc_1sp_stored_unity_scale(&[scale(0.5, 2.0)]));
        assert!(!cf_lcc_1sp_stored_unity_scale(&[scale(
            1.0,
            1.0_f64.next_up()
        )]));
        assert!(!cf_lcc_1sp_stored_unity_scale(&[scale(
            1.0_f64.next_up(),
            1.0
        )]));
        assert!(!cf_lcc_1sp_stored_unity_scale(&[scale(
            1.0_f64.next_down(),
            1.0
        )]));
    }

    fn origin() -> OperationParameterInfo {
        param(Some("8801"), Some("Latitude of natural origin"), 40.0, 1.0)
    }

    fn meridian() -> OperationParameterInfo {
        param(
            Some("8802"),
            Some("Longitude of natural origin"),
            -100.0,
            1.0,
        )
    }

    #[test]
    fn admits_mapping_requires_unique_origin_and_meridian() {
        assert!(cf_lcc_1sp_admits_mapping(&[
            origin(),
            meridian(),
            scale(1.0, 1.0)
        ]));
        assert!(!cf_lcc_1sp_admits_mapping(&[meridian(), scale(1.0, 1.0)]));
        assert!(!cf_lcc_1sp_admits_mapping(&[origin(), scale(1.0, 1.0)]));
        assert!(!cf_lcc_1sp_admits_mapping(&[
            origin(),
            origin(),
            meridian(),
            scale(1.0, 1.0)
        ]));
        assert!(!cf_lcc_1sp_admits_mapping(&[
            origin(),
            meridian(),
            meridian(),
            scale(1.0, 1.0)
        ]));
        assert!(cf_lcc_1sp_admits_mapping(&[
            param(None, Some("Latitude of natural origin"), 18.0, 1.0),
            param(None, Some("Longitude of natural origin"), -77.0, 1.0),
            param(None, Some("Scale factor at natural origin"), 1.0, 1.0),
        ]));
    }

    #[test]
    fn mercator_b_detects_any_scale() {
        assert!(!cf_mercator_b_has_scale(&[origin(), meridian()]));
        assert!(cf_mercator_b_has_scale(&[scale(1.0, 1.0)]));
        assert!(cf_mercator_b_has_scale(&[scale(0.8, 1.0)]));
        assert!(cf_mercator_b_has_scale(&[param(
            None,
            Some("Scale factor at natural origin"),
            0.8,
            1.0
        )]));
    }
}
