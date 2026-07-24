use std::ptr;

use crate::crs::*;
use crate::error::Result;

pub(crate) fn authorities() -> Result<Vec<String>> {
    with_proj_context(|context| {
        // SAFETY: context is valid and PROJ returns a null-terminated string list
        // owned by the caller.
        string_list_from_ptr(context, unsafe {
            proj_sys::proj_get_authorities_from_database(context)
        })
    })?
}

pub(crate) fn geoid_models(value: &str) -> Result<Vec<String>> {
    let crs_info = info(value)?;
    let (Some(authority), Some(code)) = (crs_info.authority.clone(), crs_info.code.clone()) else {
        return Err(CrsError::invalid(
            "geoid model lookup requires a CRS with an authority code".to_owned(),
        ));
    };
    let authority = cstring(authority)?;
    let code = cstring(code)?;
    with_proj_context(|context| {
        // SAFETY: context and authority/code strings are valid. Null options use
        // PROJ defaults. PROJ returns a caller-owned null-terminated string list.
        string_list_from_ptr(context, unsafe {
            proj_sys::proj_get_geoid_models_from_database(
                context,
                authority.as_ptr(),
                code.as_ptr(),
                ptr::null(),
            )
        })
    })?
}

pub(crate) fn codes(
    authority: &str,
    kind: Option<&str>,
    allow_deprecated: bool,
) -> Result<Vec<String>> {
    if authority.is_empty() {
        return Err(CrsError::invalid(
            "authority must be a non-empty string".to_owned(),
        ));
    }
    let authority = cstring(authority)?;
    let type_ = CrsObjectKind::parse(kind)?.to_proj();
    with_proj_context(|context| {
        // SAFETY: context and authority are valid for the duration of the call,
        // and PROJ returns a caller-owned null-terminated string list.
        string_list_from_ptr(context, unsafe {
            proj_sys::proj_get_codes_from_database(
                context,
                authority.as_ptr(),
                type_,
                i32::from(allow_deprecated),
            )
        })
    })?
}

pub(crate) fn catalog(
    authority: Option<&str>,
    options: &CrsCatalogOptions,
) -> Result<Vec<CrsCatalogInfo>> {
    ensure_thread_caches_current();
    options.validate()?;
    let authority = authority
        .filter(|value| !value.is_empty())
        .unwrap_or("EPSG")
        .to_owned();
    CRS_CATALOG_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_CATALOG_CACHE_CAPACITY,
            |item| {
                item.authority.as_deref() == Some(authority.as_str()) && item.options == *options
            },
            || {
                Ok(CachedCrsCatalog {
                    authority: Some(authority.clone()),
                    options: options.clone(),
                    items: catalog_uncached(Some(authority.as_str()), options)?,
                })
            },
        )?;
        Ok(cache[index].items.clone())
    })
}

fn crs_catalog_infos_from_list(
    list: *mut *mut proj_sys::PROJ_CRS_INFO,
    count: i32,
) -> Vec<CrsCatalogInfo> {
    let mut items = Vec::with_capacity(count.max(0) as usize);
    for index in 0..count {
        // SAFETY: PROJ returned count entries.
        let info = unsafe { *list.add(index as usize) };
        if info.is_null() {
            continue;
        }
        // SAFETY: info points to PROJ_CRS_INFO owned by list until destroy.
        let info = unsafe { &*info };
        let authority = string_from_ptr(info.auth_name);
        let code = string_from_ptr(info.code);
        let crs = match (&authority, &code) {
            (Some(authority), Some(code)) => format!("{authority}:{code}"),
            _ => string_from_ptr(info.name).unwrap_or_else(|| "unknown".to_owned()),
        };
        items.push(CrsCatalogInfo {
            crs,
            authority,
            code,
            name: string_from_ptr(info.name),
            kind: crs_type_name(info.type_),
            deprecated: info.deprecated != 0,
            area_of_use: (info.bbox_valid != 0).then(|| AreaOfUse {
                west: info.west_lon_degree,
                south: info.south_lat_degree,
                east: info.east_lon_degree,
                north: info.north_lat_degree,
                name: string_from_ptr(info.area_name),
            }),
            projection_method_name: string_from_ptr(info.projection_method_name),
            celestial_body: string_from_ptr(info.celestial_body_name),
        });
    }
    items
}

fn celestial_body_infos_from_list(
    list: *mut *mut proj_sys::PROJ_CELESTIAL_BODY_INFO,
    count: i32,
) -> Vec<CelestialBodyInfo> {
    let mut items = Vec::with_capacity(count.max(0) as usize);
    for index in 0..count {
        // SAFETY: PROJ returned count entries.
        let info = unsafe { *list.add(index as usize) };
        if info.is_null() {
            continue;
        }
        // SAFETY: info points to PROJ_CELESTIAL_BODY_INFO owned by list until destroy.
        let info = unsafe { &*info };
        items.push(CelestialBodyInfo {
            authority: string_from_ptr(info.auth_name),
            name: string_from_ptr(info.name),
        });
    }
    items
}

fn catalog_uncached(
    authority: Option<&str>,
    options: &CrsCatalogOptions,
) -> Result<Vec<CrsCatalogInfo>> {
    let authority = authority.map(cstring).transpose()?;
    with_proj_context(|context| {
        let params = ProjCrsListParameters::new(options)?;
        let mut count = 0;
        // SAFETY: context, optional authority, and params are valid for this call.
        // PROJ returns a caller-owned list with count entries.
        let list = unsafe {
            proj_sys::proj_get_crs_info_list_from_database(
                context,
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                params.as_ptr(),
                &raw mut count,
            )
        };
        if list.is_null() {
            // SAFETY: context is valid for this immediate PROJ error inspection.
            let error = unsafe { proj_sys::proj_context_errno(context) };
            if error == 0 {
                return Ok(Vec::new());
            }
            return Err(CrsError::invalid(proj_context_error_message(context)));
        }
        let items = crs_catalog_infos_from_list(list, count);
        // SAFETY: list is owned by caller and destroyed once here.
        unsafe {
            proj_sys::proj_crs_info_list_destroy(list);
        }
        Ok(items)
    })?
}

pub(crate) fn celestial_bodies(authority: Option<&str>) -> Result<Vec<CelestialBodyInfo>> {
    if matches!(authority, Some("")) {
        return Err(CrsError::invalid(
            "celestial body authority must be a non-empty string".to_owned(),
        ));
    }
    let authority = authority.map(cstring).transpose()?;
    with_proj_context(|context| {
        let mut count = 0;
        // SAFETY: context and optional authority are valid for this call. PROJ
        // returns a caller-owned list with count entries.
        let list = unsafe {
            proj_sys::proj_get_celestial_body_list_from_database(
                context,
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                &raw mut count,
            )
        };
        if list.is_null() {
            // SAFETY: context is valid for this immediate PROJ error inspection.
            let error = unsafe { proj_sys::proj_context_errno(context) };
            if error == 0 {
                return Ok(Vec::new());
            }
            return Err(CrsError::invalid(proj_context_error_message(context)));
        }
        let items = celestial_body_infos_from_list(list, count);
        // SAFETY: list is owned by caller and destroyed once here.
        unsafe {
            proj_sys::proj_celestial_body_list_destroy(list);
        }
        Ok(items)
    })?
}
