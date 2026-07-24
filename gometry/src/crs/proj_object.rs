//! The libPROJ object wrapper (`impl ProjObject`) — CRS/operation
//! introspection over a cached `PJ` handle.
//!
//! Reaches the FFI primitives, caches, and export/error helpers in the parent
//! `crs` module via `use super::*`.

use super::*;
use crate::error::Result;

impl ProjObject {
    pub(super) fn new(crs: &str) -> Result<Self> {
        let definition = cstring(crs)?;
        let context =
            ProjContext::new().map_err(|error| CrsError::crs_create(crs, error.to_string()))?;
        // SAFETY: definition is a valid C string for the duration of the call.
        let object = unsafe { proj_sys::proj_create(context.as_ptr(), definition.as_ptr()) };
        if object.is_null() {
            let mut message = proj_context_error_message(context.as_ptr());
            // PROJ 9.8 reports an unclassified lookup error for a bare token
            // that is not a CRS definition, while older builds returned the
            // more precise wrong-syntax diagnostic.  Keep the public error
            // contract stable without hiding unresolved authority references
            // (for example, `EPSG:999999`) behind a syntax error.
            if message == "PROJ could not resolve CRS" && !crs.contains(':') {
                "Invalid PROJ string syntax".clone_into(&mut message);
            }
            return Err(CrsError::crs_create(crs, message));
        }
        Ok(Self {
            // SAFETY: `object` was just checked non-null and is owned by this guard.
            object: unsafe { OwnedPj::from_owned(object) },
            context,
        })
    }

    pub(super) fn factors(
        &self,
        longitude: f64,
        latitude: f64,
        radians: bool,
    ) -> Result<ProjectionFactors> {
        let (lambda, phi) = if radians {
            (longitude, latitude)
        } else {
            (longitude.to_radians(), latitude.to_radians())
        };
        // SAFETY: the PROJ object is owned by self and the coordinate is passed
        // by value. PROJ returns projection factors by value.
        unsafe {
            let object = self.object.as_ptr();
            proj_sys::proj_errno_reset(object);
            let raw = proj_sys::proj_factors(object, proj_sys::proj_coord(lambda, phi, 0.0, 0.0));
            let error = proj_sys::proj_errno(object);
            let factors = ProjectionFactors::from_raw(raw, radians);
            if error != 0 || !factors.is_finite() {
                return Err(CrsError::invalid(proj_transform_error(error)));
            }
            Ok(factors)
        }
    }

    pub(super) fn factor_columns(
        &self,
        longitudes: &[f64],
        latitudes: &[f64],
        radians: bool,
    ) -> Result<ProjectionFactorColumns> {
        let mut columns = ProjectionFactorColumns::with_capacity(longitudes.len());
        // SAFETY: the PROJ object is owned by self and each coordinate is passed
        // by value. PROJ returns projection factors by value.
        unsafe {
            let object = self.object.as_ptr();
            proj_sys::proj_errno_reset(object);
            for (&longitude, &latitude) in longitudes.iter().zip(latitudes) {
                let (lambda, phi) = if radians {
                    (longitude, latitude)
                } else {
                    (longitude.to_radians(), latitude.to_radians())
                };
                let raw =
                    proj_sys::proj_factors(object, proj_sys::proj_coord(lambda, phi, 0.0, 0.0));
                let factors = ProjectionFactors::from_raw(raw, radians);
                if !factors.is_finite() {
                    let error = proj_sys::proj_errno(object);
                    return Err(CrsError::invalid(proj_transform_error(error)));
                }
                columns.push(factors);
            }
            let error = proj_sys::proj_errno(object);
            if error != 0 {
                return Err(CrsError::invalid(proj_transform_error(error)));
            }
        }
        Ok(columns)
    }

    pub(super) fn info(&self, normalized: String) -> CrsInfo {
        // SAFETY: the inspected pointer is owned by self and remains valid for the
        // duration of each PROJ metadata call.
        unsafe {
            let context = self.context.as_ptr();
            let object = self.object.as_ptr();
            let type_ = proj_sys::proj_get_type(object);
            let coordinate_system = coordinate_system_type(context, object);
            let axes = axes(context, object);
            let (axes, axis_order) = if axes.is_empty()
                && type_ == proj_sys::PJ_TYPE_PJ_TYPE_COMPOUND_CRS
                && let Some((compound_axes, compound_axis_order)) =
                    compound_axis_metadata(context, object)
            {
                (compound_axes, compound_axis_order)
            } else {
                let axis_order = axes
                    .iter()
                    .map(|axis| axis_role(coordinate_system, axis))
                    .collect::<Vec<_>>();
                (axes, axis_order)
            };
            let (authority, code) = split_authority(id_authority(object));
            CrsInfo {
                crs: normalized,
                name: string_from_ptr(proj_sys::proj_get_name(object)),
                authority,
                code,
                kind: crs_type_name(type_),
                is_derived: proj_sys::proj_crs_is_derived(context, object) != 0,
                deprecated: proj_sys::proj_is_deprecated(object) != 0,
                remarks: string_from_ptr(proj_sys::proj_get_remarks(object)),
                scope: string_from_ptr(proj_sys::proj_get_scope(object)),
                coordinate_system,
                axis_order,
                celestial_body: string_from_ptr(proj_sys::proj_get_celestial_body_name(
                    context, object,
                )),
                has_point_motion_operation: proj_sys::proj_crs_has_point_motion_operation(
                    context, object,
                ) != 0,
                area_of_use: area_of_use(context, object),
                axes,
                domains: domain_infos(context, object),
                sub_crs: sub_crs_infos(context, object),
                source_crs: owned_authority_object_info(
                    context,
                    proj_sys::proj_get_source_crs(context, object),
                ),
                target_crs: owned_authority_object_info(
                    context,
                    proj_sys::proj_get_target_crs(context, object),
                ),
                coordinate_operation: owned_crs_coordinate_operation_info(
                    context,
                    proj_sys::proj_crs_get_coordoperation(context, object),
                ),
                geodetic_crs: owned_authority_object_info(
                    context,
                    proj_sys::proj_crs_get_geodetic_crs(context, object),
                ),
                horizontal_datum: owned_authority_object_info(
                    context,
                    proj_sys::proj_crs_get_horizontal_datum(context, object),
                ),
                datum: datum_info(context, object),
                ellipsoid: ellipsoid_info(context, object),
                prime_meridian: prime_meridian_info(context, object),
            }
        }
    }

    pub(super) fn to_wkt_with_options(
        &self,
        crs: String,
        version: proj_sys::PJ_WKT_TYPE,
        options: &CrsWktOptions,
    ) -> Result<String> {
        let c_options = options.to_c_options()?;
        let option_ptrs = c_option_ptrs(&c_options);
        let context = self.context.as_ptr();
        let object = self.object.as_ptr();
        // SAFETY: self.object is a valid PROJ object. option_ptrs is
        // null-terminated and points to C strings that live for the call.
        let value =
            unsafe { proj_sys::proj_as_wkt(context, object, version, option_ptrs.as_ptr()) };
        string_from_ptr(value)
            .ok_or_else(|| CrsError::export(crs, "WKT", proj_context_error_message(context)))
    }

    pub(super) fn to_projjson(&self, crs: String, options: &CrsProjJsonOptions) -> Result<String> {
        let c_options = options.to_c_options()?;
        let option_ptrs = c_option_ptrs(&c_options);
        let context = self.context.as_ptr();
        let object = self.object.as_ptr();
        // SAFETY: self.object is a valid PROJ object. option_ptrs is
        // null-terminated and points to C strings that live for the call.
        let value = unsafe { proj_sys::proj_as_projjson(context, object, option_ptrs.as_ptr()) };
        string_from_ptr(value)
            .ok_or_else(|| CrsError::export(crs, "PROJJSON", proj_context_error_message(context)))
    }

    pub(super) fn to_proj_string(
        &self,
        crs: String,
        version: proj_sys::PJ_PROJ_STRING_TYPE,
        options: &CrsProjOptions,
    ) -> Result<String> {
        let c_options = options.to_c_options()?;
        let option_ptrs = c_option_ptrs(&c_options);
        let context = self.context.as_ptr();
        let object = self.object.as_ptr();
        // SAFETY: self.object is a valid PROJ object. option_ptrs is
        // null-terminated and points to C strings that live for the call.
        let value = unsafe {
            proj_sys::proj_as_proj_string(context, object, version, option_ptrs.as_ptr())
        };
        string_from_ptr(value).ok_or_else(|| {
            CrsError::export(crs, "PROJ string", proj_context_error_message(context))
        })
    }

    pub(super) fn is_equivalent_to(
        &self,
        other: &Self,
        criterion: proj_sys::PJ_COMPARISON_CRITERION,
    ) -> bool {
        // SAFETY: both PROJ object pointers and self.context are valid for the
        // duration of the call.
        unsafe {
            let context = self.context.as_ptr();
            let object = self.object.as_ptr();
            let other_object = other.object.as_ptr();
            proj_sys::proj_is_equivalent_to_with_ctx(context, object, other_object, criterion) != 0
        }
    }

    pub(super) fn identify(
        &self,
        authority: Option<&str>,
        crs: String,
    ) -> Result<Vec<IdentifyCandidate>> {
        let authority = authority.map(cstring).transpose()?;
        let mut confidence = ptr::null_mut();
        // SAFETY: self.object and context are valid. authority, when present, is a
        // valid C string for the duration of the call. Returned list/confidence are
        // destroyed before returning.
        unsafe {
            let context = self.context.as_ptr();
            let object = self.object.as_ptr();
            let list = proj_sys::proj_identify(
                context,
                object,
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                ptr::null(),
                &raw mut confidence,
            );
            if list.is_null() {
                return Err(CrsError::identify(crs, proj_context_error_message(context)));
            }
            let list = ProjObjList::from_owned(list);
            let count = list.count();
            // SAFETY: `proj_identify` transfers `confidence`; when non-null it
            // contains one initialized entry per object reported by `list`.
            let confidence = if confidence.is_null() {
                ProjIntList::empty()
            } else {
                ProjIntList::from_owned(confidence, count.max(0) as usize)
            };
            let mut candidates = Vec::with_capacity(count.max(0) as usize);
            for index in 0..count {
                let Some(object) = list.get(context, index) else {
                    continue;
                };
                let object = object.as_ptr();
                let authority = string_from_ptr(proj_sys::proj_get_id_auth_name(object, 0));
                let code = string_from_ptr(proj_sys::proj_get_id_code(object, 0));
                let candidate_crs = match (&authority, &code) {
                    (Some(authority), Some(code)) => format!("{authority}:{code}"),
                    _ => string_from_ptr(proj_sys::proj_get_name(object))
                        .unwrap_or_else(|| "unknown".to_owned()),
                };
                let confidence = if let Some(value) = confidence.get(index as usize) {
                    let value = u8::try_from(value).map_err(|_| {
                        CrsError::invalid(format!(
                            "confidence must be between 0 and 100, got {value}"
                        ))
                    })?;
                    Confidence::try_new("confidence", value)?
                } else {
                    Confidence::try_new("confidence", 0)?
                };
                candidates.push(IdentifyCandidate {
                    crs: candidate_crs,
                    name: string_from_ptr(proj_sys::proj_get_name(object)),
                    authority,
                    code,
                    confidence,
                });
            }
            Ok(candidates)
        }
    }

    pub(super) fn to_2d(&self, crs: String, name: Option<&str>) -> Result<String> {
        let name = optional_c_string(name)?;
        let name_ptr = name.as_ref().map_or(ptr::null(), |value| value.as_ptr());
        let context = self.context.as_ptr();
        let object = self.object.as_ptr();
        // SAFETY: self.object/context are valid PROJ handles. name_ptr is either
        // null or a C string that lives for the duration of the call.
        let object = unsafe { proj_sys::proj_crs_demote_to_2D(context, name_ptr, object) };
        exported_owned_crs(context, object, crs, "2D CRS")
    }

    pub(super) fn to_3d(&self, crs: String, name: Option<&str>) -> Result<String> {
        let name = optional_c_string(name)?;
        let name_ptr = name.as_ref().map_or(ptr::null(), |value| value.as_ptr());
        let context = self.context.as_ptr();
        let object = self.object.as_ptr();
        // SAFETY: self.object/context are valid PROJ handles. name_ptr is either
        // null or a C string that lives for the duration of the call.
        let object = unsafe { proj_sys::proj_crs_promote_to_3D(context, name_ptr, object) };
        exported_owned_crs(context, object, crs, "3D CRS")
    }

    pub(super) fn non_deprecated(&self) -> Result<Vec<AuthorityObjectInfo>> {
        // SAFETY: self.object and context are valid. The returned list and each
        // independently owned object fetched from it are destroyed before return.
        unsafe {
            let context = self.context.as_ptr();
            let object = self.object.as_ptr();
            let list = proj_sys::proj_get_non_deprecated(context, object);
            if list.is_null() {
                let error = proj_sys::proj_context_errno(context);
                if error == 0 {
                    return Ok(Vec::new());
                }
                return Err(CrsError::invalid(proj_context_error_message(context)));
            }
            let list = ProjObjList::from_owned(list);
            let count = list.count();
            let mut items = Vec::with_capacity(count.max(0) as usize);
            for index in 0..count {
                if let Some(object) = list.get(context, index) {
                    items.push(authority_object_info(context, object.as_ptr()));
                }
            }
            Ok(items)
        }
    }
}
