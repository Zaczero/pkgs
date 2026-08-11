//! The libPROJ object wrapper (`impl ProjObject`) — CRS/operation
//! introspection over a cached `PJ` handle.
//!
//! Reaches the FFI primitives, caches, and export/error helpers in the parent
//! `crs` module via `use super::*`.

use crate::crs::{
    AngularUnit, AuthorityObjectInfo, Confidence, CrsError, CrsInfo, CrsProjJsonOptions,
    CrsProjOptions, CrsWktOptions, IdentifyCandidate, OwnedPj, ProjContext, ProjIntList,
    ProjObjList, ProjObject, ProjectionFactorColumns, ProjectionFactors, area_of_use,
    authority_object_info, axes, axis_role, c_option_ptrs, compound_axis_metadata,
    coordinate_system_type, copy_proj_c_string, crs_type_name, cstring, datum_info, domain_infos,
    ellipsoid_info, exported_owned_crs, id_authority, optional_c_string,
    owned_authority_object_info, owned_crs_coordinate_operation_info, prime_meridian_info,
    proj_context_error_message, proj_transform_error, ptr, split_authority, sub_crs_infos,
};
use crate::error::Result;

impl ProjObject {
    pub(super) fn new(crs: &str) -> Result<Self> {
        let definition = cstring(crs)?;
        let context =
            ProjContext::new().map_err(|error| CrsError::crs_create(crs, error.to_string()))?;
        // SAFETY: DOC-H. Typed live context; definition is a live CString;
        // returns uniquely owned PJ or null.
        let object = unsafe { proj_sys::proj_create(context.as_ptr(), definition.as_ptr()) };
        // SAFETY: non-null return is uniquely owned.
        let Some(object) = (unsafe { OwnedPj::try_from_owned(object) }) else {
            let mut message = proj_context_error_message(&context);
            // PROJ 9.8 reports an unclassified lookup error for a bare token
            // that is not a CRS definition, while older builds returned the
            // more precise wrong-syntax diagnostic.  Keep the public error
            // contract stable without hiding unresolved authority references
            // (for example, `EPSG:999999`) behind a syntax error.
            if message == "PROJ could not resolve CRS" && !crs.contains(':') {
                "Invalid PROJ string syntax".clone_into(&mut message);
            }
            return Err(CrsError::crs_create(crs, message));
        };
        Ok(Self { object, context })
    }

    pub(super) fn factors(
        &self,
        longitude: f64,
        latitude: f64,
        unit: AngularUnit,
    ) -> Result<ProjectionFactors> {
        let (lambda, phi) = if unit.is_radians() {
            (longitude, latitude)
        } else {
            (longitude.to_radians(), latitude.to_radians())
        };
        // SAFETY: DOC-H. Typed self owns the PJ on the creating thread;
        // coordinate is by value; PROJ returns factors by value. No Python.
        unsafe {
            let object = self.object.as_ptr();
            proj_sys::proj_errno_reset(object);
            let raw = proj_sys::proj_factors(object, proj_sys::proj_coord(lambda, phi, 0.0, 0.0));
            let error = proj_sys::proj_errno(object);
            let factors = ProjectionFactors::from_raw(raw, unit.is_radians());
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
        unit: AngularUnit,
    ) -> Result<ProjectionFactorColumns> {
        let mut columns = ProjectionFactorColumns::with_capacity(longitudes.len());
        // SAFETY: DOC-H. Typed self owns the PJ for the entire Rust-owned input
        // loop; coordinates by value; no Python call inside the loop.
        unsafe {
            let object = self.object.as_ptr();
            proj_sys::proj_errno_reset(object);
            for (&longitude, &latitude) in longitudes.iter().zip(latitudes) {
                let (lambda, phi) = if unit.is_radians() {
                    (longitude, latitude)
                } else {
                    (longitude.to_radians(), latitude.to_radians())
                };
                let raw =
                    proj_sys::proj_factors(object, proj_sys::proj_coord(lambda, phi, 0.0, 0.0));
                let factors = ProjectionFactors::from_raw(raw, unit.is_radians());
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
        // DOC-H block: context/object derive from `&self` typed owners on the
        // creating thread. Returned owned children are adopted into OwnedPj
        // immediately; strings are copied while the parent lives. PROJ invokes
        // no Python callback.
        let context = &self.context;
        let object = &self.object;
        // SAFETY: DOC-H. Typed live object on creating thread.
        let type_ = unsafe { proj_sys::proj_get_type(object.as_ptr()) };
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
        // SAFETY: DOC-H. Typed live context/object for all metadata FFI below;
        // child PJ returns are uniquely owned and adopted immediately; C strings
        // are object-lifetime and copied via copy_proj_c_string.
        let (
            is_derived,
            deprecated,
            has_pmo,
            name,
            remarks,
            scope,
            celestial_body,
            source_crs,
            target_crs,
            coord_op,
            geodetic_crs,
            horizontal_datum,
        ) = unsafe {
            (
                proj_sys::proj_crs_is_derived(context.as_ptr(), object.as_ptr()) != 0,
                proj_sys::proj_is_deprecated(object.as_ptr()) != 0,
                proj_sys::proj_crs_has_point_motion_operation(context.as_ptr(), object.as_ptr())
                    != 0,
                copy_proj_c_string(proj_sys::proj_get_name(object.as_ptr())),
                copy_proj_c_string(proj_sys::proj_get_remarks(object.as_ptr())),
                copy_proj_c_string(proj_sys::proj_get_scope(object.as_ptr())),
                copy_proj_c_string(proj_sys::proj_get_celestial_body_name(
                    context.as_ptr(),
                    object.as_ptr(),
                )),
                OwnedPj::try_from_owned(proj_sys::proj_get_source_crs(
                    context.as_ptr(),
                    object.as_ptr(),
                )),
                OwnedPj::try_from_owned(proj_sys::proj_get_target_crs(
                    context.as_ptr(),
                    object.as_ptr(),
                )),
                OwnedPj::try_from_owned(proj_sys::proj_crs_get_coordoperation(
                    context.as_ptr(),
                    object.as_ptr(),
                )),
                OwnedPj::try_from_owned(proj_sys::proj_crs_get_geodetic_crs(
                    context.as_ptr(),
                    object.as_ptr(),
                )),
                OwnedPj::try_from_owned(proj_sys::proj_crs_get_horizontal_datum(
                    context.as_ptr(),
                    object.as_ptr(),
                )),
            )
        };
        CrsInfo {
            crs: normalized,
            name,
            authority,
            code,
            kind: crs_type_name(type_),
            is_derived,
            deprecated,
            remarks,
            scope,
            coordinate_system,
            axis_order,
            celestial_body,
            has_point_motion_operation: has_pmo,
            area_of_use: area_of_use(context, object),
            axes,
            domains: domain_infos(context, object),
            sub_crs: sub_crs_infos(context, object),
            source_crs: owned_authority_object_info(context, source_crs),
            target_crs: owned_authority_object_info(context, target_crs),
            coordinate_operation: owned_crs_coordinate_operation_info(context, coord_op),
            geodetic_crs: owned_authority_object_info(context, geodetic_crs),
            horizontal_datum: owned_authority_object_info(context, horizontal_datum),
            datum: datum_info(context, object),
            ellipsoid: ellipsoid_info(context, object),
            prime_meridian: prime_meridian_info(context, object),
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
        // SAFETY: DOC-H. Typed owners; option_ptrs null-terminated and points
        // to C strings that live for the call; returned string copied immediately.
        let value = unsafe {
            proj_sys::proj_as_wkt(
                self.context.as_ptr(),
                self.object.as_ptr(),
                version,
                option_ptrs.as_ptr(),
            )
        };
        proj_c_string!(value)
            .ok_or_else(|| CrsError::export(crs, "WKT", proj_context_error_message(&self.context)))
    }

    pub(super) fn to_projjson(&self, crs: String, options: &CrsProjJsonOptions) -> Result<String> {
        let c_options = options.to_c_options()?;
        let option_ptrs = c_option_ptrs(&c_options);
        // SAFETY: DOC-H. Same proof as WKT export.
        let value = unsafe {
            proj_sys::proj_as_projjson(
                self.context.as_ptr(),
                self.object.as_ptr(),
                option_ptrs.as_ptr(),
            )
        };
        proj_c_string!(value).ok_or_else(|| {
            CrsError::export(crs, "PROJJSON", proj_context_error_message(&self.context))
        })
    }

    pub(super) fn to_proj_string(
        &self,
        crs: String,
        version: proj_sys::PJ_PROJ_STRING_TYPE,
        options: &CrsProjOptions,
    ) -> Result<String> {
        let c_options = options.to_c_options()?;
        let option_ptrs = c_option_ptrs(&c_options);
        // SAFETY: DOC-H. Same proof as WKT export.
        let value = unsafe {
            proj_sys::proj_as_proj_string(
                self.context.as_ptr(),
                self.object.as_ptr(),
                version,
                option_ptrs.as_ptr(),
            )
        };
        proj_c_string!(value).ok_or_else(|| {
            CrsError::export(
                crs,
                "PROJ string",
                proj_context_error_message(&self.context),
            )
        })
    }

    pub(super) fn is_equivalent_to(
        &self,
        other: &Self,
        criterion: proj_sys::PJ_COMPARISON_CRITERION,
    ) -> bool {
        // SAFETY: DOC-H. Both typed objects and self context are live on the
        // current thread for the synchronous comparison.
        unsafe {
            proj_sys::proj_is_equivalent_to_with_ctx(
                self.context.as_ptr(),
                self.object.as_ptr(),
                other.object.as_ptr(),
                criterion,
            ) != 0
        }
    }

    /// Normalize axis order for visualization (lon/lat or easting/northing)
    /// and replace the owned PJ, keeping the creating-thread context.
    ///
    /// Used by the operational CRS-compatibility predicate before
    /// `PJ_COMP_EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS` comparison.
    pub(super) fn into_normalized_for_visualization(mut self) -> Result<Self> {
        // SAFETY: DOC-H. Typed live context + object on the creating thread;
        // returns a uniquely owned normalized PJ or null.
        let normalized = unsafe {
            proj_sys::proj_normalize_for_visualization(self.context.as_ptr(), self.object.as_ptr())
        };
        // SAFETY: non-null return is uniquely owned.
        let Some(object) = (unsafe { OwnedPj::try_from_owned(normalized) }) else {
            return Err(CrsError::message(
                "PROJ could not normalize CRS for visualization axis order".to_owned(),
            ));
        };
        // Drop the pre-normalization PJ via assignment; context stays put.
        self.object = object;
        Ok(self)
    }

    pub(super) fn identify(
        &self,
        authority: Option<&str>,
        crs: String,
    ) -> Result<Vec<IdentifyCandidate>> {
        let authority = authority.map(cstring).transpose()?;
        let mut confidence = ptr::null_mut();
        // SAFETY: DOC-H + OUT + O + LIST(n). Typed receiver and authority live;
        // list/confidence are immediately RAII-wrapped; strings copied before
        // item drop. Creating-thread confined; no Python callback.
        let list = unsafe {
            proj_sys::proj_identify(
                self.context.as_ptr(),
                self.object.as_ptr(),
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                ptr::null(),
                &raw mut confidence,
            )
        };
        // SAFETY: non-null list is uniquely owned.
        let Some(list) = (unsafe { ProjObjList::try_from_owned(list) }) else {
            return Err(CrsError::identify(
                crs,
                proj_context_error_message(&self.context),
            ));
        };
        let count = list.count();
        // SAFETY: `proj_identify` transfers `confidence`; when non-null it
        // contains one initialized entry per object reported by `list`.
        let confidence = unsafe { ProjIntList::try_from_owned(confidence, count.max(0) as usize) };
        let mut candidates = Vec::with_capacity(count.max(0) as usize);
        for index in 0..count {
            let Some(object) = list.get(&self.context, index) else {
                continue;
            };
            let (authority, code) = match id_authority(&object) {
                Some((a, c)) => (Some(a), Some(c)),
                None => (None, None),
            };
            let name = proj_c_string!(proj_sys::proj_get_name(object.as_ptr()));
            let candidate_crs = match (&authority, &code) {
                (Some(authority), Some(code)) => format!("{authority}:{code}"),
                _ => name.clone().unwrap_or_else(|| "unknown".to_owned()),
            };
            let confidence = if let Some(value) = confidence.get(index as usize) {
                let value = u8::try_from(value).map_err(|_| {
                    CrsError::invalid(format!("confidence must be between 0 and 100, got {value}"))
                })?;
                Confidence::try_new("confidence", value)?
            } else {
                Confidence::try_new("confidence", 0)?
            };
            candidates.push(IdentifyCandidate {
                crs: candidate_crs,
                name,
                authority,
                code,
                confidence,
            });
        }
        Ok(candidates)
    }

    pub(super) fn to_2d(&self, crs: String, name: Option<&str>) -> Result<String> {
        let name = optional_c_string(name)?;
        let name_ptr = name.as_ref().map_or(ptr::null(), |value| value.as_ptr());
        // SAFETY: DOC-H. Typed receiver; name_ptr null or live CString; returns
        // uniquely owned demoted CRS or null.
        let object = unsafe {
            proj_sys::proj_crs_demote_to_2D(self.context.as_ptr(), name_ptr, self.object.as_ptr())
        };
        // SAFETY: non-null is uniquely owned.
        let object = unsafe { OwnedPj::try_from_owned(object) };
        exported_owned_crs(&self.context, object, crs, "2D CRS")
    }

    pub(super) fn to_3d(&self, crs: String, name: Option<&str>) -> Result<String> {
        let name = optional_c_string(name)?;
        let name_ptr = name.as_ref().map_or(ptr::null(), |value| value.as_ptr());
        // SAFETY: DOC-H. Same proof as demotion.
        let object = unsafe {
            proj_sys::proj_crs_promote_to_3D(self.context.as_ptr(), name_ptr, self.object.as_ptr())
        };
        // SAFETY: non-null is uniquely owned.
        let object = unsafe { OwnedPj::try_from_owned(object) };
        exported_owned_crs(&self.context, object, crs, "3D CRS")
    }

    pub(super) fn non_deprecated(&self) -> Result<Vec<AuthorityObjectInfo>> {
        // SAFETY: DOC-H. Typed receiver; returns uniquely owned list or null.
        let list = unsafe {
            proj_sys::proj_get_non_deprecated(self.context.as_ptr(), self.object.as_ptr())
        };
        // SAFETY: non-null is uniquely owned.
        let Some(list) = (unsafe { ProjObjList::try_from_owned(list) }) else {
            if self.context.errno() == 0 {
                return Ok(Vec::new());
            }
            return Err(CrsError::invalid(proj_context_error_message(&self.context)));
        };
        let count = list.count();
        let mut items = Vec::with_capacity(count.max(0) as usize);
        for index in 0..count {
            if let Some(object) = list.get(&self.context, index) {
                items.push(authority_object_info(&self.context, &object));
            }
        }
        Ok(items)
    }
}
