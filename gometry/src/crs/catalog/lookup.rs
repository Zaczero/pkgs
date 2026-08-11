use std::ptr;
use std::ptr::NonNull;

use crate::crs::{
    CRS_CATALOG_CACHE, CRS_CATALOG_CACHE_CAPACITY, CRS_CELESTIAL_BODIES_CACHE,
    CRS_CELESTIAL_BODIES_CACHE_CAPACITY, CachedCelestialBodies, CachedCrsCatalog,
    CelestialBodyInfo, CrsCatalogInfo, CrsCatalogOptions, CrsError, CrsObjectKind,
    OwnedCelestialBodyList, OwnedCrsInfoList, OwnedProjStringList, ProjCrsListParameters, cstring,
    ensure_thread_caches_current, info, lru_resolve, proj_context_error_message,
    take_proj_string_list, with_proj_context,
};
use crate::error::Result;

pub(crate) fn authorities() -> Result<Vec<String>> {
    with_proj_context(|context| {
        // SAFETY: DOC-H. Typed live context; PROJ returns a uniquely owned
        // null-terminated string list (or null); adopt immediately.
        let list = unsafe {
            OwnedProjStringList::try_from_owned(proj_sys::proj_get_authorities_from_database(
                context.as_ptr(),
            ))
        };
        take_proj_string_list(context, list)
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
        // SAFETY: DOC-H. Typed context; authority/code CStrings live for the
        // call; null options use PROJ defaults; uniquely owned list adopted.
        let list = unsafe {
            OwnedProjStringList::try_from_owned(proj_sys::proj_get_geoid_models_from_database(
                context.as_ptr(),
                authority.as_ptr(),
                code.as_ptr(),
                ptr::null(),
            ))
        };
        take_proj_string_list(context, list)
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
        // SAFETY: DOC-H. Typed context; authority CString live for the call;
        // uniquely owned string list adopted immediately.
        let list = unsafe {
            OwnedProjStringList::try_from_owned(proj_sys::proj_get_codes_from_database(
                context.as_ptr(),
                authority.as_ptr(),
                type_,
                i32::from(allow_deprecated),
            ))
        };
        take_proj_string_list(context, list)
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

fn catalog_uncached(
    authority: Option<&str>,
    options: &CrsCatalogOptions,
) -> Result<Vec<CrsCatalogInfo>> {
    let authority = authority.map(cstring).transpose()?;
    with_proj_context(|context| {
        let params = ProjCrsListParameters::new(options)?;
        let mut count = 0;
        // SAFETY: DOC-H. Typed context/params; optional authority CString live;
        // OUT count exclusive local; returns caller-owned list with count entries.
        let list = unsafe {
            proj_sys::proj_get_crs_info_list_from_database(
                context.as_ptr(),
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                params.as_ptr(),
                &raw mut count,
            )
        };
        let Some(list) = NonNull::new(list) else {
            if context.errno() == 0 {
                return Ok(Vec::new());
            }
            return Err(CrsError::invalid(proj_context_error_message(context)));
        };
        // SAFETY: non-null list + reported count; unique ownership to Drop guard.
        let list = unsafe { OwnedCrsInfoList::from_owned(list, count) };
        Ok(list.into_catalog_infos())
    })?
}

pub(crate) fn celestial_bodies(authority: Option<&str>) -> Result<Vec<CelestialBodyInfo>> {
    if matches!(authority, Some("")) {
        return Err(CrsError::invalid(
            "celestial body authority must be a non-empty string".to_owned(),
        ));
    }
    ensure_thread_caches_current();
    let authority_owned = authority.map(str::to_owned);
    CRS_CELESTIAL_BODIES_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_CELESTIAL_BODIES_CACHE_CAPACITY,
            |item| item.authority.as_deref() == authority_owned.as_deref(),
            || {
                Ok(CachedCelestialBodies {
                    authority: authority_owned.clone(),
                    items: celestial_bodies_uncached(authority)?.into(),
                })
            },
        )?;
        Ok(cache[index].items.to_vec())
    })
}

fn celestial_bodies_uncached(authority: Option<&str>) -> Result<Vec<CelestialBodyInfo>> {
    let authority = authority.map(cstring).transpose()?;
    with_proj_context(|context| {
        let mut count = 0;
        // SAFETY: DOC-H. Typed context; optional authority live; OUT count
        // exclusive; returns caller-owned list with count entries.
        let list = unsafe {
            proj_sys::proj_get_celestial_body_list_from_database(
                context.as_ptr(),
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                &raw mut count,
            )
        };
        let Some(list) = NonNull::new(list) else {
            if context.errno() == 0 {
                return Ok(Vec::new());
            }
            return Err(CrsError::invalid(proj_context_error_message(context)));
        };
        // SAFETY: non-null list + count; unique ownership to Drop guard.
        let list = unsafe { OwnedCelestialBodyList::from_owned(list, count) };
        Ok(list.into_infos())
    })?
}
