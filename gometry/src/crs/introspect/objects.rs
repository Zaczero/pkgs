use super::*;
use crate::crs::*;
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
    context: *mut proj_sys::PJ_CONTEXT,
    definition: *const c_char,
    crs: &str,
    epoch: Option<f64>,
) -> Result<OwnedPj> {
    // SAFETY: definition is a valid C string for the duration of the call, and a
    // non-null returned object is owned by the guard below.
    let raw = unsafe { proj_sys::proj_create(context, definition) };
    if raw.is_null() {
        return Err(CrsError::crs_create(
            crs,
            proj_context_error_message(context),
        ));
    }
    // SAFETY: `raw` was just checked non-null and is owned by this guard.
    let object = unsafe { OwnedPj::from_owned(raw) };
    if let Some(epoch) = epoch {
        // SAFETY: context/object are valid. PROJ returns owned metadata or null.
        let metadata =
            unsafe { proj_sys::proj_coordinate_metadata_create(context, object.as_ptr(), epoch) };
        if metadata.is_null() {
            return Err(CrsError::crs_create(
                crs,
                proj_context_error_message(context),
            ));
        }
        // SAFETY: `metadata` was just checked non-null and is owned by this guard.
        Ok(unsafe { OwnedPj::from_owned(metadata) })
    } else {
        Ok(object)
    }
}

pub(crate) fn operation_info_from_pj(
    context: *mut proj_sys::PJ_CONTEXT,
    operation: *mut proj_sys::PJ,
    source: String,
    target: String,
    source_epoch: Option<f64>,
    target_epoch: Option<f64>,
) -> OperationInfo {
    // SAFETY: operation is a valid PROJ coordinate operation. The returned info
    // struct contains borrowed C strings copied immediately.
    unsafe {
        let info = proj_sys::proj_pj_info(operation);
        OperationInfo {
            name: string_from_ptr(info.id),
            definition: string_from_ptr(info.definition),
            description: string_from_ptr(info.description),
            accuracy: finite_nonnegative(info.accuracy),
            has_inverse: info.has_inverse != 0,
            has_ballpark_transformation: proj_sys::proj_coordoperation_has_ballpark_transformation(
                context, operation,
            ) != 0,
            requires_coordinate_epoch:
                proj_sys::proj_coordoperation_requires_per_coordinate_input_time(context, operation)
                    != 0,
            instantiable: proj_sys::proj_coordoperation_is_instantiable(context, operation) != 0,
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
    context: *mut proj_sys::PJ_CONTEXT,
    object: *mut proj_sys::PJ,
    crs: String,
    format: &'static str,
) -> Result<String> {
    if object.is_null() {
        return Err(CrsError::export(
            crs,
            format,
            proj_context_error_message(context),
        ));
    }
    let authority = id_authority(object).map(|(authority, code)| format!("{authority}:{code}"));
    // SAFETY: object/context are valid; PROJ returns an object-lifetime string.
    let wkt = unsafe {
        string_from_ptr(proj_sys::proj_as_wkt(
            context,
            object,
            proj_sys::PJ_WKT_TYPE_PJ_WKT2_2019,
            ptr::null(),
        ))
    };
    // SAFETY: object was returned by PROJ for this call and is no longer used
    // after all metadata/export strings have been copied.
    unsafe {
        proj_sys::proj_destroy(object);
    }
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

pub(crate) fn id_authority(object: *const proj_sys::PJ) -> Option<(String, String)> {
    // SAFETY: object is a valid PROJ object and index zero is the primary
    // authority identifier when present.
    unsafe {
        let auth = string_from_ptr(proj_sys::proj_get_id_auth_name(object, 0))?;
        let code = string_from_ptr(proj_sys::proj_get_id_code(object, 0))?;
        Some((auth, code))
    }
}

pub(crate) fn authority_object_info(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *const proj_sys::PJ,
) -> AuthorityObjectInfo {
    // SAFETY: object is valid for immediate metadata inspection.
    unsafe {
        let authority = string_from_ptr(proj_sys::proj_get_id_auth_name(object, 0));
        let code = string_from_ptr(proj_sys::proj_get_id_code(object, 0));
        let crs = match (&authority, &code) {
            (Some(authority), Some(code)) => format!("{authority}:{code}"),
            _ => string_from_ptr(proj_sys::proj_get_name(object))
                .unwrap_or_else(|| "unknown".to_owned()),
        };
        AuthorityObjectInfo {
            crs,
            authority,
            code,
            name: string_from_ptr(proj_sys::proj_get_name(object)),
            kind: crs_type_name(proj_sys::proj_get_type(object)),
            deprecated: proj_sys::proj_is_deprecated(object) != 0,
            area_of_use: area_of_use(context, object),
        }
    }
}

pub(crate) fn owned_authority_object_info(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *mut proj_sys::PJ,
) -> Option<AuthorityObjectInfo> {
    if object.is_null() {
        return None;
    }
    let info = authority_object_info(context, object);
    // SAFETY: object is owned by the caller and no longer used after metadata is
    // copied into the Rust value.
    unsafe {
        proj_sys::proj_destroy(object);
    }
    Some(info)
}

pub(crate) fn sub_crs_infos(
    context: *mut proj_sys::PJ_CONTEXT,
    crs: *const proj_sys::PJ,
) -> Vec<AuthorityObjectInfo> {
    let mut items = Vec::new();
    for index in 0..32 {
        // SAFETY: crs is a valid CRS object. PROJ returns an owned sub-CRS
        // object or null when the index is out of range/not applicable.
        let object = unsafe { proj_sys::proj_crs_get_sub_crs(context, crs, index) };
        let Some(info) = owned_authority_object_info(context, object) else {
            break;
        };
        items.push(info);
    }
    items
}

pub(crate) fn owned_crs_coordinate_operation_info(
    context: *mut proj_sys::PJ_CONTEXT,
    operation: *mut proj_sys::PJ,
) -> Option<CrsCoordinateOperationInfo> {
    if operation.is_null() {
        return None;
    }
    let info = crs_coordinate_operation_info_from_pj(context, operation);
    // SAFETY: operation is owned by this helper and metadata has been copied.
    unsafe {
        proj_sys::proj_destroy(operation);
    }
    Some(info)
}

pub(crate) fn crs_coordinate_operation_info_from_pj(
    context: *mut proj_sys::PJ_CONTEXT,
    operation: *mut proj_sys::PJ,
) -> CrsCoordinateOperationInfo {
    // SAFETY: operation is a valid PROJ coordinate operation. Returned C strings
    // are copied immediately into Rust-owned values.
    unsafe {
        let pj = proj_sys::proj_pj_info(operation);
        let id = string_from_ptr(pj.id);
        let description = string_from_ptr(pj.description);
        CrsCoordinateOperationInfo {
            name: description.clone().or(id),
            definition: string_from_ptr(pj.definition),
            description,
            accuracy: finite_nonnegative(pj.accuracy),
            has_inverse: pj.has_inverse != 0,
            has_ballpark_transformation: proj_sys::proj_coordoperation_has_ballpark_transformation(
                context, operation,
            ) != 0,
            requires_coordinate_epoch:
                proj_sys::proj_coordoperation_requires_per_coordinate_input_time(context, operation)
                    != 0,
            instantiable: proj_sys::proj_coordoperation_is_instantiable(context, operation) != 0,
            method: method_info(context, operation),
            parameters: operation_parameters(context, operation),
            grids: grids(context, operation),
            steps: operation_steps(context, operation),
            area_of_use: area_of_use(context, operation),
        }
    }
}

pub(crate) fn datum_info(
    context: *mut proj_sys::PJ_CONTEXT,
    crs: *const proj_sys::PJ,
) -> Option<DatumInfo> {
    // SAFETY: crs is a valid PROJ CRS object. PROJ returns owned objects or null.
    unsafe {
        let ensemble = proj_sys::proj_crs_get_datum_ensemble(context, crs);
        if !ensemble.is_null() {
            let info = datum_ensemble_info(context, ensemble);
            proj_sys::proj_destroy(ensemble);
            return Some(info);
        }
        let datum = proj_sys::proj_crs_get_datum(context, crs);
        if datum.is_null() {
            return None;
        }
        let info = datum_object_info(context, datum);
        proj_sys::proj_destroy(datum);
        Some(info)
    }
}

pub(crate) fn datum_ensemble_info(
    context: *mut proj_sys::PJ_CONTEXT,
    ensemble: *const proj_sys::PJ,
) -> DatumInfo {
    // SAFETY: ensemble is a valid PROJ datum ensemble object.
    unsafe {
        let count = proj_sys::proj_datum_ensemble_get_member_count(context, ensemble);
        let mut members = Vec::with_capacity(count.max(0) as usize);
        for index in 0..count {
            let member = proj_sys::proj_datum_ensemble_get_member(context, ensemble, index);
            if member.is_null() {
                continue;
            }
            members.push(datum_object_info(context, member));
            proj_sys::proj_destroy(member);
        }
        let (authority, code) = split_authority(id_authority(ensemble));
        DatumInfo {
            name: string_from_ptr(proj_sys::proj_get_name(ensemble)),
            authority,
            code,
            kind: crs_type_name(proj_sys::proj_get_type(ensemble)),
            frame_reference_epoch: None,
            ensemble_accuracy: finite_nonnegative(proj_sys::proj_datum_ensemble_get_accuracy(
                context, ensemble,
            )),
            ensemble_members: members,
        }
    }
}

pub(crate) fn datum_object_info(
    context: *mut proj_sys::PJ_CONTEXT,
    datum: *const proj_sys::PJ,
) -> DatumInfo {
    // SAFETY: datum is a valid PROJ datum object.
    unsafe {
        let type_ = proj_sys::proj_get_type(datum);
        let frame_reference_epoch = if matches!(
            type_,
            proj_sys::PJ_TYPE_PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME
                | proj_sys::PJ_TYPE_PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME
        ) {
            finite_nonnegative(proj_sys::proj_dynamic_datum_get_frame_reference_epoch(
                context, datum,
            ))
        } else {
            None
        };
        let (authority, code) = split_authority(id_authority(datum));
        DatumInfo {
            name: string_from_ptr(proj_sys::proj_get_name(datum)),
            authority,
            code,
            kind: crs_type_name(type_),
            frame_reference_epoch,
            ensemble_accuracy: None,
            ensemble_members: Vec::new(),
        }
    }
}

pub(crate) fn ellipsoid_info(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *const proj_sys::PJ,
) -> Option<EllipsoidInfo> {
    // SAFETY: object is valid and PROJ returns an owned ellipsoid object or null.
    unsafe {
        let ellipsoid = proj_sys::proj_get_ellipsoid(context, object);
        if ellipsoid.is_null() {
            return None;
        }
        let mut semi_major_metre = f64::NAN;
        let mut semi_minor_metre = f64::NAN;
        let mut is_semi_minor_computed = 0;
        let mut inverse_flattening = f64::NAN;
        let ok = proj_sys::proj_ellipsoid_get_parameters(
            context,
            ellipsoid,
            &raw mut semi_major_metre,
            &raw mut semi_minor_metre,
            &raw mut is_semi_minor_computed,
            &raw mut inverse_flattening,
        );
        let info = (ok != 0).then(|| EllipsoidInfo {
            name: string_from_ptr(proj_sys::proj_get_name(ellipsoid)),
            semi_major_metre,
            semi_minor_metre,
            inverse_flattening,
            is_semi_minor_computed: is_semi_minor_computed != 0,
        });
        proj_sys::proj_destroy(ellipsoid);
        info
    }
}

pub(crate) fn prime_meridian_info(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *const proj_sys::PJ,
) -> Option<PrimeMeridianInfo> {
    // SAFETY: object is valid and PROJ returns an owned prime meridian object or
    // null.
    unsafe {
        let prime_meridian = proj_sys::proj_get_prime_meridian(context, object);
        if prime_meridian.is_null() {
            return None;
        }
        let mut longitude = f64::NAN;
        let mut unit_conversion_factor = f64::NAN;
        let mut unit_name = ptr::null();
        let ok = proj_sys::proj_prime_meridian_get_parameters(
            context,
            prime_meridian,
            &raw mut longitude,
            &raw mut unit_conversion_factor,
            &raw mut unit_name,
        );
        let info = (ok != 0).then(|| PrimeMeridianInfo {
            name: string_from_ptr(proj_sys::proj_get_name(prime_meridian)),
            longitude,
            unit_name: string_from_ptr(unit_name),
            unit_conversion_factor,
        });
        proj_sys::proj_destroy(prime_meridian);
        info
    }
}
