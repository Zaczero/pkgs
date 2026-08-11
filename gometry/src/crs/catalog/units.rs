use std::ptr;

use crate::crs::catalog::catalog;
use crate::crs::{
    CRS_UNITS_CACHE, CRS_UNITS_CACHE_CAPACITY, CachedUnits, CrsCatalogInfo, CrsCatalogOptions,
    CrsError, CrsObjectKind, OwnedUnitList, UnitInfo, UtmCatalogOptions, cstring,
    ensure_thread_caches_current, lru_resolve, proj_context_error_message, with_proj_context,
};
use crate::error::Result;
use crate::text::str_contains_ignore_ascii_case;

pub(crate) fn utm_zones(options: &UtmCatalogOptions) -> Result<Vec<CrsCatalogInfo>> {
    options.validate()?;
    let catalog_options = CrsCatalogOptions {
        kind: Some(CrsObjectKind::ProjectedCrs),
        area: options.area,
        contains_area: options.contains_area,
        allow_deprecated: options.allow_deprecated,
        celestial_body: Some("Earth".to_owned()),
    };
    let datum_name = options.datum_name.as_deref();
    let mut items = catalog(Some("EPSG"), &catalog_options)?
        .into_iter()
        .filter(|info| is_utm_crs_info(info, datum_name))
        .collect::<Vec<_>>();
    items.sort_unstable_by(|left, right| {
        left.code
            .as_deref()
            .unwrap_or_default()
            .cmp(right.code.as_deref().unwrap_or_default())
    });
    Ok(items)
}

fn is_utm_crs_info(info: &CrsCatalogInfo, datum_name: Option<&str>) -> bool {
    if info.kind != "projected" {
        return false;
    }
    if info.projection_method_name.as_deref() != Some("Transverse Mercator") {
        return false;
    }
    let Some(name) = &info.name else {
        return false;
    };
    if !name.contains(" / UTM zone ") {
        return false;
    }
    datum_name.is_none_or(|datum_name| str_contains_ignore_ascii_case(name, datum_name))
}

pub(crate) fn unit_info(authority: &str, code: &str) -> Result<UnitInfo> {
    if authority.is_empty() {
        return Err(CrsError::invalid(
            "unit authority must be a non-empty string".to_owned(),
        ));
    }
    if code.is_empty() {
        return Err(CrsError::invalid(
            "unit code must be a non-empty string".to_owned(),
        ));
    }
    let authority = cstring(authority)?;
    let code = cstring(code)?;
    with_proj_context(|context| {
        let mut name = ptr::null();
        let mut conversion_factor = 0.0;
        let mut category = ptr::null();
        // SAFETY: DOC-H. Typed context; authority/code CStrings live; OUT slots
        // exclusive locals; returned strings copied immediately.
        let found = unsafe {
            proj_sys::proj_uom_get_info_from_database(
                context.as_ptr(),
                authority.as_ptr(),
                code.as_ptr(),
                &raw mut name,
                &raw mut conversion_factor,
                &raw mut category,
            )
        };
        if found == 0 {
            return Err(CrsError::invalid(format!(
                "unknown PROJ unit {}:{}",
                authority.to_string_lossy(),
                code.to_string_lossy()
            )));
        }
        Ok(UnitInfo {
            authority: Some(authority.to_string_lossy().into_owned()),
            code: Some(code.to_string_lossy().into_owned()),
            name: proj_c_string!(name),
            category: proj_c_string!(category),
            conversion_factor,
            proj_short_name: None,
        })
    })?
}

pub(crate) fn units(
    authority: &str,
    category: Option<&str>,
    allow_deprecated: bool,
) -> Result<Vec<UnitInfo>> {
    if authority.is_empty() {
        return Err(CrsError::invalid(
            "unit authority must be a non-empty string".to_owned(),
        ));
    }
    ensure_thread_caches_current();
    let authority_owned = authority.to_owned();
    let category_owned = category.map(str::to_owned);
    CRS_UNITS_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_UNITS_CACHE_CAPACITY,
            |item| {
                item.authority == authority_owned
                    && item.category.as_deref() == category_owned.as_deref()
                    && item.allow_deprecated == allow_deprecated
            },
            || {
                Ok(CachedUnits {
                    authority: authority_owned.clone(),
                    category: category_owned.clone(),
                    allow_deprecated,
                    items: units_uncached(authority, category, allow_deprecated)?.into(),
                })
            },
        )?;
        Ok(cache[index].items.to_vec())
    })
}

fn units_uncached(
    authority: &str,
    category: Option<&str>,
    allow_deprecated: bool,
) -> Result<Vec<UnitInfo>> {
    use std::ptr::NonNull;

    let authority = cstring(authority)?;
    let category = category.map(cstring).transpose()?;
    with_proj_context(|context| {
        let mut count = 0;
        // SAFETY: DOC-H. Typed context; authority/category live; OUT count
        // exclusive; returns caller-owned list with count entries.
        let list = unsafe {
            proj_sys::proj_get_units_from_database(
                context.as_ptr(),
                authority.as_ptr(),
                category
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                i32::from(allow_deprecated),
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
        let list = unsafe { OwnedUnitList::from_owned(list, count) };
        Ok(list.into_units())
    })?
}
