use std::ptr;

use crate::crs::introspect::crs_coordinate_operation_info_from_pj;
use crate::crs::{
    CrsCoordinateOperationInfo, GridInfo, MethodInfo, OperationParameterInfo, OwnedPj, ProjContext,
};

pub(crate) fn method_info(context: &ProjContext, operation: &OwnedPj) -> Option<MethodInfo> {
    let mut name = ptr::null();
    let mut authority = ptr::null();
    let mut code = ptr::null();
    // SAFETY: DOC-H. Typed live context/operation; OUT slots exclusive locals;
    // returned C strings are object-lifetime and copied immediately.
    let ok = unsafe {
        proj_sys::proj_coordoperation_get_method_info(
            context.as_ptr(),
            operation.as_ptr(),
            &raw mut name,
            &raw mut authority,
            &raw mut code,
        )
    };
    (ok != 0).then(|| MethodInfo {
        name: proj_c_string!(name),
        authority: proj_c_string!(authority),
        code: proj_c_string!(code),
    })
}

pub(crate) fn operation_parameters(
    context: &ProjContext,
    operation: &OwnedPj,
) -> Vec<OperationParameterInfo> {
    // SAFETY: DOC-H. Typed live context/operation on creating thread.
    let count = unsafe {
        proj_sys::proj_coordoperation_get_param_count(context.as_ptr(), operation.as_ptr())
    };
    if count <= 0 {
        return Vec::new();
    }
    let mut parameters = Vec::with_capacity(count as usize);
    for index in 0..count {
        let mut name = ptr::null();
        let mut authority = ptr::null();
        let mut code = ptr::null();
        let mut value = 0.0;
        let mut value_string = ptr::null();
        let mut unit_conversion_factor = 0.0;
        let mut unit_name = ptr::null();
        let mut unit_authority = ptr::null();
        let mut unit_code = ptr::null();
        let mut unit_category = ptr::null();
        // SAFETY: DOC-H. Index in 0..count; OUT slots exclusive; strings copied.
        let ok = unsafe {
            proj_sys::proj_coordoperation_get_param(
                context.as_ptr(),
                operation.as_ptr(),
                index,
                &raw mut name,
                &raw mut authority,
                &raw mut code,
                &raw mut value,
                &raw mut value_string,
                &raw mut unit_conversion_factor,
                &raw mut unit_name,
                &raw mut unit_authority,
                &raw mut unit_code,
                &raw mut unit_category,
            )
        };
        if ok != 0 {
            parameters.push(OperationParameterInfo {
                name: proj_c_string!(name),
                authority: proj_c_string!(authority),
                code: proj_c_string!(code),
                value,
                value_string: proj_c_string!(value_string),
                unit_conversion_factor,
                unit_name: proj_c_string!(unit_name),
                unit_authority: proj_c_string!(unit_authority),
                unit_code: proj_c_string!(unit_code),
                unit_category: proj_c_string!(unit_category),
            });
        }
    }
    parameters
}

pub(crate) fn grids(context: &ProjContext, operation: &OwnedPj) -> Vec<GridInfo> {
    // SAFETY: DOC-H. Typed live context/operation.
    let count = unsafe {
        proj_sys::proj_coordoperation_get_grid_used_count(context.as_ptr(), operation.as_ptr())
    };
    if count <= 0 {
        return Vec::new();
    }
    let mut grids = Vec::with_capacity(count as usize);
    for index in 0..count {
        let mut short_name = ptr::null();
        let mut full_name = ptr::null();
        let mut package_name = ptr::null();
        let mut url = ptr::null();
        let mut direct_download = 0;
        let mut open_license = 0;
        let mut available = 0;
        // SAFETY: DOC-H. Index in 0..count; OUT slots exclusive; strings copied.
        let ok = unsafe {
            proj_sys::proj_coordoperation_get_grid_used(
                context.as_ptr(),
                operation.as_ptr(),
                index,
                &raw mut short_name,
                &raw mut full_name,
                &raw mut package_name,
                &raw mut url,
                &raw mut direct_download,
                &raw mut open_license,
                &raw mut available,
            )
        };
        if ok != 0 {
            grids.push(GridInfo {
                short_name: proj_c_string!(short_name),
                full_name: proj_c_string!(full_name),
                package_name: proj_c_string!(package_name),
                available: available != 0,
            });
        }
    }
    grids
}

pub(crate) fn operation_steps(
    context: &ProjContext,
    operation: &OwnedPj,
) -> Vec<CrsCoordinateOperationInfo> {
    // SAFETY: DOC-H. Typed owners; non-concatenated ops report zero steps.
    let count = unsafe {
        proj_sys::proj_concatoperation_get_step_count(context.as_ptr(), operation.as_ptr())
    };
    if count <= 0 {
        return Vec::new();
    }
    let mut steps = Vec::with_capacity(count as usize);
    for index in 0..count {
        // SAFETY: DOC-H. Index in 0..count; returns uniquely owned step or null.
        let step = unsafe {
            proj_sys::proj_concatoperation_get_step(context.as_ptr(), operation.as_ptr(), index)
        };
        // SAFETY: non-null returns are uniquely owned; Drop after metadata copy.
        let Some(step) = (unsafe { OwnedPj::try_from_owned(step) }) else {
            continue;
        };
        steps.push(crs_coordinate_operation_info_from_pj(context, &step));
    }
    steps
}

pub(crate) const fn crs_type_name(type_: proj_sys::PJ_TYPE) -> &'static str {
    match type_ {
        proj_sys::PJ_TYPE_PJ_TYPE_ELLIPSOID => "ellipsoid",
        proj_sys::PJ_TYPE_PJ_TYPE_PRIME_MERIDIAN => "prime_meridian",
        proj_sys::PJ_TYPE_PJ_TYPE_GEODETIC_REFERENCE_FRAME => "geodetic_reference_frame",
        proj_sys::PJ_TYPE_PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME => {
            "dynamic_geodetic_reference_frame"
        },
        proj_sys::PJ_TYPE_PJ_TYPE_VERTICAL_REFERENCE_FRAME => "vertical_reference_frame",
        proj_sys::PJ_TYPE_PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME => {
            "dynamic_vertical_reference_frame"
        },
        proj_sys::PJ_TYPE_PJ_TYPE_DATUM_ENSEMBLE => "datum_ensemble",
        proj_sys::PJ_TYPE_PJ_TYPE_GEOGRAPHIC_2D_CRS => "geographic_2d",
        proj_sys::PJ_TYPE_PJ_TYPE_GEOGRAPHIC_3D_CRS => "geographic_3d",
        proj_sys::PJ_TYPE_PJ_TYPE_GEOGRAPHIC_CRS => "geographic",
        proj_sys::PJ_TYPE_PJ_TYPE_GEOCENTRIC_CRS => "geocentric",
        proj_sys::PJ_TYPE_PJ_TYPE_PROJECTED_CRS
        | proj_sys::PJ_TYPE_PJ_TYPE_DERIVED_PROJECTED_CRS => "projected",
        proj_sys::PJ_TYPE_PJ_TYPE_VERTICAL_CRS => "vertical",
        proj_sys::PJ_TYPE_PJ_TYPE_COMPOUND_CRS => "compound",
        proj_sys::PJ_TYPE_PJ_TYPE_TEMPORAL_CRS => "temporal",
        proj_sys::PJ_TYPE_PJ_TYPE_ENGINEERING_CRS => "engineering",
        proj_sys::PJ_TYPE_PJ_TYPE_BOUND_CRS => "bound",
        proj_sys::PJ_TYPE_PJ_TYPE_CONVERSION => "conversion",
        proj_sys::PJ_TYPE_PJ_TYPE_TRANSFORMATION => "transformation",
        proj_sys::PJ_TYPE_PJ_TYPE_CONCATENATED_OPERATION => "concatenated_operation",
        proj_sys::PJ_TYPE_PJ_TYPE_OTHER_COORDINATE_OPERATION => "other_coordinate_operation",
        proj_sys::PJ_TYPE_PJ_TYPE_TEMPORAL_DATUM => "temporal_datum",
        proj_sys::PJ_TYPE_PJ_TYPE_ENGINEERING_DATUM => "engineering_datum",
        proj_sys::PJ_TYPE_PJ_TYPE_PARAMETRIC_DATUM => "parametric_datum",
        proj_sys::PJ_TYPE_PJ_TYPE_OTHER_CRS => "other",
        _ => "unknown",
    }
}
