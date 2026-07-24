use std::ptr;

use super::*;
use crate::crs::*;
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
        // SAFETY: context/authority/code are valid for this call; output pointers
        // reference initialized local storage and returned strings are copied.
        let found = unsafe {
            proj_sys::proj_uom_get_info_from_database(
                context,
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
            name: string_from_ptr(name),
            category: string_from_ptr(category),
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
    let authority = cstring(authority)?;
    let category = category.map(cstring).transpose()?;
    with_proj_context(|context| {
        let mut count = 0;
        // SAFETY: context/authority/category are valid for the call. PROJ returns
        // a caller-owned null-terminated-ish list with count entries.
        let list = unsafe {
            proj_sys::proj_get_units_from_database(
                context,
                authority.as_ptr(),
                category
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                i32::from(allow_deprecated),
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
        let mut units = Vec::with_capacity(count.max(0) as usize);
        for index in 0..count {
            // SAFETY: PROJ returned count entries.
            let info = unsafe { *list.add(index as usize) };
            if info.is_null() {
                continue;
            }
            // SAFETY: info points to a PROJ_UNIT_INFO owned by list until destroy.
            let info = unsafe { &*info };
            units.push(UnitInfo {
                authority: string_from_ptr(info.auth_name),
                code: string_from_ptr(info.code),
                name: string_from_ptr(info.name),
                category: string_from_ptr(info.category),
                conversion_factor: info.conv_factor,
                proj_short_name: string_from_ptr(info.proj_short_name),
            });
        }
        // SAFETY: list is owned by caller and destroyed once here.
        unsafe {
            proj_sys::proj_unit_list_destroy(list);
        }
        Ok(units)
    })?
}
