use crate::crs::{CfValue, CrsCoordinateOperationInfo, OperationParameterInfo};

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

pub(crate) fn add_cf_parameters(
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    key: impl Fn(&OperationParameterInfo) -> Option<&'static str>,
) {
    for parameter in parameters {
        let Some(key) = key(parameter) else {
            continue;
        };
        items.push((key, CfValue::Float(parameter.value)));
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CfParameterMapping {
    pub codes: &'static [&'static str],
    pub names: &'static [&'static str],
    pub cf_key: &'static str,
}

pub(crate) fn add_cf_projection_parameters(
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
    mappings: &[CfParameterMapping],
) {
    add_cf_parameters(items, parameters, |parameter| {
        cf_projection_parameter_key(parameter, mappings)
    });
}

pub(crate) fn cf_projection_parameter_key(
    parameter: &OperationParameterInfo,
    mappings: &[CfParameterMapping],
) -> Option<&'static str> {
    let code = parameter.code.as_deref();
    let name = parameter.name.as_deref();
    mappings.iter().find_map(|mapping| {
        (code.is_some_and(|code| mapping.codes.contains(&code))
            || name.is_some_and(|name| mapping.names.contains(&name)))
        .then_some(mapping.cf_key)
    })
}

pub(crate) const CF_TRANSVERSE_MERCATOR_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8801"],
        names: &["Latitude of natural origin"],
        cf_key: "latitude_of_projection_origin",
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_central_meridian",
    },
    CfParameterMapping {
        codes: &["8805"],
        names: &["Scale factor at natural origin"],
        cf_key: "scale_factor_at_central_meridian",
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
    },
];

const CF_LAMBERT_AZIMUTHAL_EQUAL_AREA_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8801"],
        names: &["Latitude of natural origin"],
        cf_key: "latitude_of_projection_origin",
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_projection_origin",
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
    },
];

const CF_MERCATOR_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8801"],
        names: &["Latitude of natural origin"],
        cf_key: "latitude_of_projection_origin",
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_projection_origin",
    },
    CfParameterMapping {
        codes: &["8805"],
        names: &["Scale factor at natural origin"],
        cf_key: "scale_factor_at_projection_origin",
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
    },
];

const CF_POLAR_STEREOGRAPHIC_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8805"],
        names: &["Scale factor at natural origin"],
        cf_key: "scale_factor_at_projection_origin",
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
    },
];

const CF_LAMBERT_CYLINDRICAL_EQUAL_AREA_PARAMETERS: &[CfParameterMapping] = &[
    CfParameterMapping {
        codes: &["8823"],
        names: &["Latitude of 1st standard parallel"],
        cf_key: "standard_parallel",
    },
    CfParameterMapping {
        codes: &["8802"],
        names: &["Longitude of natural origin"],
        cf_key: "longitude_of_central_meridian",
    },
    CfParameterMapping {
        codes: &["8806"],
        names: &["False easting"],
        cf_key: "false_easting",
    },
    CfParameterMapping {
        codes: &["8807"],
        names: &["False northing"],
        cf_key: "false_northing",
    },
];

pub(crate) fn add_cf_lambert_azimuthal_equal_area_parameters(
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
) {
    add_cf_projection_parameters(
        items,
        parameters,
        CF_LAMBERT_AZIMUTHAL_EQUAL_AREA_PARAMETERS,
    );
}

pub(crate) fn add_cf_lambert_conformal_conic_parameters(
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
) {
    let mut standard_parallel = Vec::new();
    for parameter in parameters {
        match parameter.code.as_deref() {
            Some("8821") => items.push((
                "latitude_of_projection_origin",
                CfValue::Float(parameter.value),
            )),
            Some("8822") => items.push((
                "longitude_of_central_meridian",
                CfValue::Float(parameter.value),
            )),
            Some("8823" | "8824") => standard_parallel.push(parameter.value),
            Some("8826") => items.push(("false_easting", CfValue::Float(parameter.value))),
            Some("8827") => items.push(("false_northing", CfValue::Float(parameter.value))),
            _ => match parameter.name.as_deref() {
                Some("Latitude of false origin") => items.push((
                    "latitude_of_projection_origin",
                    CfValue::Float(parameter.value),
                )),
                Some("Longitude of false origin") => items.push((
                    "longitude_of_central_meridian",
                    CfValue::Float(parameter.value),
                )),
                Some("Latitude of 1st standard parallel" | "Latitude of 2nd standard parallel") => {
                    standard_parallel.push(parameter.value);
                },
                Some("Easting at false origin" | "False easting") => {
                    items.push(("false_easting", CfValue::Float(parameter.value)));
                },
                Some("Northing at false origin" | "False northing") => {
                    items.push(("false_northing", CfValue::Float(parameter.value)));
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
}

pub(crate) fn add_cf_mercator_parameters(
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
) {
    add_cf_projection_parameters(items, parameters, CF_MERCATOR_PARAMETERS);
}

pub(crate) fn add_cf_polar_stereographic_parameters(
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
) {
    let mut standard_parallel = None;
    let mut straight_vertical = None;
    add_cf_projection_parameters(items, parameters, CF_POLAR_STEREOGRAPHIC_PARAMETERS);
    for parameter in parameters {
        match parameter.code.as_deref() {
            Some("8832") => standard_parallel = Some(parameter.value),
            Some("8833") => straight_vertical = Some(parameter.value),
            _ => match parameter.name.as_deref() {
                Some("Latitude of standard parallel") => standard_parallel = Some(parameter.value),
                Some("Longitude of origin") => straight_vertical = Some(parameter.value),
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
}

pub(crate) fn add_cf_lambert_cylindrical_equal_area_parameters(
    items: &mut Vec<(&'static str, CfValue)>,
    parameters: &[OperationParameterInfo],
) {
    add_cf_projection_parameters(
        items,
        parameters,
        CF_LAMBERT_CYLINDRICAL_EQUAL_AREA_PARAMETERS,
    );
}
