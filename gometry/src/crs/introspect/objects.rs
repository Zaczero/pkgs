use std::ffi::CString;
use std::ptr;

use crate::crs::introspect::{
    area_of_use, crs_type_name, grids, method_info, operation_parameters, operation_steps,
};
use crate::crs::{
    AuthorityObjectInfo, CrsCoordinateOperationInfo, CrsError, DatumInfo, EllipsoidInfo,
    OperationInfo, OwnedPj, PrimeMeridianInfo, ProjContext, copy_proj_c_string,
    proj_context_error_message,
};
use crate::error::Result;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Confidence(u8);

impl Confidence {
    pub(crate) fn try_new(name: &'static str, value: u8) -> Result<Self> {
        if value <= 100 {
            Ok(Self(value))
        } else {
            Err(CrsError::invalid(format!(
                "{name} must be between 0 and 100, got {value}"
            )))
        }
    }

    pub(crate) const fn get(self) -> u8 {
        self.0
    }
}

impl<'py> pyo3::IntoPyObject<'py> for Confidence {
    type Target = pyo3::types::PyInt;
    type Output = pyo3::Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(
        self,
        py: pyo3::Python<'py>,
    ) -> std::result::Result<Self::Output, Self::Error> {
        pyo3::IntoPyObject::into_pyobject(self.get(), py)
    }
}

pub(crate) fn finite_nonnegative(value: f64) -> Option<f64> {
    (value.is_finite() && value >= 0.0).then_some(value)
}

pub(crate) const fn validate_min_confidence(value: Confidence) {
    let _ = value;
}

pub(crate) fn create_crs_transform_object(
    context: &ProjContext,
    definition: &CString,
    crs: &str,
    epoch: Option<f64>,
) -> Result<OwnedPj> {
    // SAFETY: DOC-H. Typed live context; definition is a live NUL-terminated
    // CString; non-null return is uniquely owned. Creating-thread confined.
    let raw = unsafe { proj_sys::proj_create(context.as_ptr(), definition.as_ptr()) };
    // SAFETY: non-null return is uniquely owned by the caller per PROJ.
    let Some(object) = (unsafe { OwnedPj::try_from_owned(raw) }) else {
        return Err(CrsError::crs_create(
            crs,
            proj_context_error_message(context),
        ));
    };
    if let Some(epoch) = epoch {
        // SAFETY: DOC-H. Typed context + owned object; returns owned metadata or null.
        let metadata = unsafe {
            proj_sys::proj_coordinate_metadata_create(context.as_ptr(), object.as_ptr(), epoch)
        };
        // SAFETY: non-null return is uniquely owned.
        let Some(metadata) = (unsafe { OwnedPj::try_from_owned(metadata) }) else {
            return Err(CrsError::crs_create(
                crs,
                proj_context_error_message(context),
            ));
        };
        Ok(metadata)
    } else {
        Ok(object)
    }
}

pub(crate) fn operation_info_from_pj(
    context: &ProjContext,
    operation: &OwnedPj,
    source: String,
    target: String,
    source_epoch: Option<f64>,
    target_epoch: Option<f64>,
) -> OperationInfo {
    // SAFETY: DOC-H. Typed live context/operation on creating thread. Returned
    // info struct contains borrowed C strings copied immediately. No Python
    // callback.
    unsafe {
        let info = proj_sys::proj_pj_info(operation.as_ptr());
        OperationInfo {
            name: copy_proj_c_string(info.id),
            definition: copy_proj_c_string(info.definition),
            description: copy_proj_c_string(info.description),
            accuracy: finite_nonnegative(info.accuracy),
            has_inverse: info.has_inverse != 0,
            has_ballpark_transformation: proj_sys::proj_coordoperation_has_ballpark_transformation(
                context.as_ptr(),
                operation.as_ptr(),
            ) != 0,
            requires_coordinate_epoch:
                proj_sys::proj_coordoperation_requires_per_coordinate_input_time(
                    context.as_ptr(),
                    operation.as_ptr(),
                ) != 0,
            instantiable: proj_sys::proj_coordoperation_is_instantiable(
                context.as_ptr(),
                operation.as_ptr(),
            ) != 0,
            method: method_info(context, operation),
            parameters: operation_parameters(context, operation),
            grids: grids(context, operation),
            steps: operation_steps(context, operation),
            area_of_use: area_of_use(context, operation),
            source,
            target,
            source_epoch,
            target_epoch,
        }
    }
}

pub(crate) fn exported_owned_crs(
    context: &ProjContext,
    object: Option<OwnedPj>,
    crs: String,
    format: &'static str,
) -> Result<String> {
    let Some(object) = object else {
        return Err(CrsError::export(
            crs,
            format,
            proj_context_error_message(context),
        ));
    };
    let authority = id_authority(&object).map(|(authority, code)| format!("{authority}:{code}"));
    // SAFETY: DOC-H. Typed owners; PROJ returns object-lifetime WKT string.
    let wkt = unsafe {
        copy_proj_c_string(proj_sys::proj_as_wkt(
            context.as_ptr(),
            object.as_ptr(),
            proj_sys::PJ_WKT_TYPE_PJ_WKT2_2019,
            ptr::null(),
        ))
    };
    // `object` drops here after strings are copied (OwnedPj Drop).
    drop(object);
    if let Some(authority) = authority {
        return Ok(authority);
    }
    wkt.ok_or_else(|| CrsError::export(crs, format, proj_context_error_message(context)))
}

/// Split an optional `(authority, code)` pair into the two optional columns
/// the result DTOs carry.
pub(crate) fn split_authority(pair: Option<(String, String)>) -> (Option<String>, Option<String>) {
    pair.map_or((None, None), |(authority, code)| {
        (Some(authority), Some(code))
    })
}

pub(crate) fn id_authority(object: &OwnedPj) -> Option<(String, String)> {
    // SAFETY: DOC-H. Typed live object; index zero is the primary authority
    // identifier when present; strings copied immediately.
    unsafe {
        let auth = copy_proj_c_string(proj_sys::proj_get_id_auth_name(object.as_ptr(), 0))?;
        let code = copy_proj_c_string(proj_sys::proj_get_id_code(object.as_ptr(), 0))?;
        Some((auth, code))
    }
}

pub(crate) fn authority_object_info(
    context: &ProjContext,
    object: &OwnedPj,
) -> AuthorityObjectInfo {
    // SAFETY: DOC-H. Typed live object for immediate metadata inspection.
    unsafe {
        let authority = copy_proj_c_string(proj_sys::proj_get_id_auth_name(object.as_ptr(), 0));
        let code = copy_proj_c_string(proj_sys::proj_get_id_code(object.as_ptr(), 0));
        let crs = match (&authority, &code) {
            (Some(authority), Some(code)) => format!("{authority}:{code}"),
            _ => copy_proj_c_string(proj_sys::proj_get_name(object.as_ptr()))
                .unwrap_or_else(|| "unknown".to_owned()),
        };
        AuthorityObjectInfo {
            crs,
            authority,
            code,
            name: copy_proj_c_string(proj_sys::proj_get_name(object.as_ptr())),
            kind: crs_type_name(proj_sys::proj_get_type(object.as_ptr())),
            deprecated: proj_sys::proj_is_deprecated(object.as_ptr()) != 0,
            area_of_use: area_of_use(context, object),
        }
    }
}

pub(crate) fn owned_authority_object_info(
    context: &ProjContext,
    object: Option<OwnedPj>,
) -> Option<AuthorityObjectInfo> {
    let object = object?;
    let info = authority_object_info(context, &object);
    // Drop destroys the owned PJ after metadata copy.
    drop(object);
    Some(info)
}

pub(crate) fn sub_crs_infos(context: &ProjContext, crs: &OwnedPj) -> Vec<AuthorityObjectInfo> {
    // Dynamic length: walk until PROJ returns null. A fixed 32-cap silently
    // truncated compound trees (never acceptable); there is no practical
    // compound with hundreds of components, but growth is fallible via Vec.
    let mut items = Vec::new();
    let mut index = 0_i32;
    loop {
        // SAFETY: DOC-H. Typed owners; returns uniquely owned sub-CRS or null.
        let object =
            unsafe { proj_sys::proj_crs_get_sub_crs(context.as_ptr(), crs.as_ptr(), index) };
        // SAFETY: non-null returns are uniquely owned.
        let object = unsafe { OwnedPj::try_from_owned(object) };
        let Some(info) = owned_authority_object_info(context, object) else {
            break;
        };
        items.push(info);
        index = index.saturating_add(1);
    }
    items
}

pub(crate) fn owned_crs_coordinate_operation_info(
    context: &ProjContext,
    operation: Option<OwnedPj>,
) -> Option<CrsCoordinateOperationInfo> {
    let operation = operation?;
    let info = crs_coordinate_operation_info_from_pj(context, &operation);
    drop(operation);
    Some(info)
}

pub(crate) fn crs_coordinate_operation_info_from_pj(
    context: &ProjContext,
    operation: &OwnedPj,
) -> CrsCoordinateOperationInfo {
    // SAFETY: DOC-H. Typed live operation; returned C strings copied immediately.
    unsafe {
        let pj = proj_sys::proj_pj_info(operation.as_ptr());
        let id = copy_proj_c_string(pj.id);
        let description = copy_proj_c_string(pj.description);
        CrsCoordinateOperationInfo {
            name: description.clone().or(id),
            definition: copy_proj_c_string(pj.definition),
            description,
            accuracy: finite_nonnegative(pj.accuracy),
            has_inverse: pj.has_inverse != 0,
            has_ballpark_transformation: proj_sys::proj_coordoperation_has_ballpark_transformation(
                context.as_ptr(),
                operation.as_ptr(),
            ) != 0,
            requires_coordinate_epoch:
                proj_sys::proj_coordoperation_requires_per_coordinate_input_time(
                    context.as_ptr(),
                    operation.as_ptr(),
                ) != 0,
            instantiable: proj_sys::proj_coordoperation_is_instantiable(
                context.as_ptr(),
                operation.as_ptr(),
            ) != 0,
            method: method_info(context, operation),
            parameters: operation_parameters(context, operation),
            grids: grids(context, operation),
            steps: operation_steps(context, operation),
            area_of_use: area_of_use(context, operation),
        }
    }
}

pub(crate) fn datum_info(context: &ProjContext, crs: &OwnedPj) -> Option<DatumInfo> {
    // SAFETY: DOC-H. Typed owners; returns uniquely owned ensemble/datum or null.
    let ensemble = unsafe { proj_sys::proj_crs_get_datum_ensemble(context.as_ptr(), crs.as_ptr()) };
    // SAFETY: non-null is uniquely owned; Drop after metadata.
    if let Some(ensemble) = unsafe { OwnedPj::try_from_owned(ensemble) } {
        return Some(datum_ensemble_info(context, &ensemble));
    }
    // SAFETY: DOC-H. Typed owners; returns uniquely owned datum or null.
    let datum = unsafe { proj_sys::proj_crs_get_datum(context.as_ptr(), crs.as_ptr()) };
    // SAFETY: non-null is uniquely owned.
    let datum = unsafe { OwnedPj::try_from_owned(datum)? };
    Some(datum_object_info(context, &datum))
}

pub(crate) fn datum_ensemble_info(context: &ProjContext, ensemble: &OwnedPj) -> DatumInfo {
    // SAFETY: DOC-H. Typed live ensemble on creating thread.
    let count = unsafe {
        proj_sys::proj_datum_ensemble_get_member_count(context.as_ptr(), ensemble.as_ptr())
    };
    let mut members = Vec::with_capacity(count.max(0) as usize);
    for index in 0..count {
        // SAFETY: DOC-H. Index in range; returns uniquely owned member or null.
        let member = unsafe {
            proj_sys::proj_datum_ensemble_get_member(context.as_ptr(), ensemble.as_ptr(), index)
        };
        // SAFETY: non-null is uniquely owned; Drop after copy.
        let Some(member) = (unsafe { OwnedPj::try_from_owned(member) }) else {
            continue;
        };
        members.push(datum_object_info(context, &member));
    }
    let (authority, code) = split_authority(id_authority(ensemble));
    // SAFETY: DOC-H. Ensemble is a live typed owner for the duration of these calls.
    let (type_, accuracy, name) = unsafe {
        (
            proj_sys::proj_get_type(ensemble.as_ptr()),
            proj_sys::proj_datum_ensemble_get_accuracy(context.as_ptr(), ensemble.as_ptr()),
            copy_proj_c_string(proj_sys::proj_get_name(ensemble.as_ptr())),
        )
    };
    DatumInfo {
        name,
        authority,
        code,
        kind: crs_type_name(type_),
        frame_reference_epoch: None,
        ensemble_accuracy: finite_nonnegative(accuracy),
        ensemble_members: members,
    }
}

pub(crate) fn datum_object_info(context: &ProjContext, datum: &OwnedPj) -> DatumInfo {
    // SAFETY: DOC-H. Typed live datum on creating thread.
    let (type_, name, epoch) = unsafe {
        let type_ = proj_sys::proj_get_type(datum.as_ptr());
        let name = copy_proj_c_string(proj_sys::proj_get_name(datum.as_ptr()));
        let epoch = matches!(
            type_,
            proj_sys::PJ_TYPE_PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME
                | proj_sys::PJ_TYPE_PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME
        )
        .then(|| {
            proj_sys::proj_dynamic_datum_get_frame_reference_epoch(context.as_ptr(), datum.as_ptr())
        });
        (type_, name, epoch)
    };
    let (authority, code) = split_authority(id_authority(datum));
    DatumInfo {
        name,
        authority,
        code,
        kind: crs_type_name(type_),
        frame_reference_epoch: epoch.and_then(finite_nonnegative),
        ensemble_accuracy: None,
        ensemble_members: Vec::new(),
    }
}

pub(crate) fn ellipsoid_info(context: &ProjContext, object: &OwnedPj) -> Option<EllipsoidInfo> {
    // SAFETY: DOC-H. Typed owners; returns uniquely owned ellipsoid or null.
    let ellipsoid = unsafe { proj_sys::proj_get_ellipsoid(context.as_ptr(), object.as_ptr()) };
    // SAFETY: non-null is uniquely owned; Drop after parameter copy.
    let ellipsoid = unsafe { OwnedPj::try_from_owned(ellipsoid)? };
    let mut semi_major_metre = f64::NAN;
    let mut semi_minor_metre = f64::NAN;
    let mut is_semi_minor_computed = 0;
    let mut inverse_flattening = f64::NAN;
    // SAFETY: DOC-H. Live owned ellipsoid; OUT slots exclusive locals.
    let ok = unsafe {
        proj_sys::proj_ellipsoid_get_parameters(
            context.as_ptr(),
            ellipsoid.as_ptr(),
            &raw mut semi_major_metre,
            &raw mut semi_minor_metre,
            &raw mut is_semi_minor_computed,
            &raw mut inverse_flattening,
        )
    };
    (ok != 0).then(|| {
        let name = proj_c_string!(proj_sys::proj_get_name(ellipsoid.as_ptr()));
        EllipsoidInfo {
            name,
            semi_major_metre,
            semi_minor_metre,
            inverse_flattening,
            is_semi_minor_computed: is_semi_minor_computed != 0,
        }
    })
}

pub(crate) fn prime_meridian_info(
    context: &ProjContext,
    object: &OwnedPj,
) -> Option<PrimeMeridianInfo> {
    // SAFETY: DOC-H. Typed owners; returns uniquely owned PM or null.
    let prime_meridian =
        unsafe { proj_sys::proj_get_prime_meridian(context.as_ptr(), object.as_ptr()) };
    // SAFETY: non-null is uniquely owned; Drop after copy.
    let prime_meridian = unsafe { OwnedPj::try_from_owned(prime_meridian)? };
    let mut longitude = f64::NAN;
    let mut unit_conversion_factor = f64::NAN;
    let mut unit_name = ptr::null();
    // SAFETY: DOC-H. Live owned PM; OUT slots exclusive locals.
    let ok = unsafe {
        proj_sys::proj_prime_meridian_get_parameters(
            context.as_ptr(),
            prime_meridian.as_ptr(),
            &raw mut longitude,
            &raw mut unit_conversion_factor,
            &raw mut unit_name,
        )
    };
    (ok != 0).then(|| {
        let name = proj_c_string!(proj_sys::proj_get_name(prime_meridian.as_ptr()));
        PrimeMeridianInfo {
            name,
            longitude,
            unit_name: proj_c_string!(unit_name),
            unit_conversion_factor,
        }
    })
}
