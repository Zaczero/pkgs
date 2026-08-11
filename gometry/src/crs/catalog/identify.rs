use std::ptr;

use crate::crs::catalog::{
    CF_TRANSVERSE_MERCATOR_PARAMETERS, add_cf_lambert_azimuthal_equal_area_parameters,
    add_cf_lambert_conformal_conic_parameters, add_cf_lambert_cylindrical_equal_area_parameters,
    add_cf_mercator_parameters, add_cf_polar_stereographic_parameters,
    add_cf_projection_parameters, cached_export, crs_to_2d_export, crs_to_3d_export,
    is_operation_method, is_transverse_mercator, to_wkt_with_options,
};
use crate::crs::{
    AuthorityObjectInfo, CRS_AUTHORITY_MATCH_CACHE, CRS_AUTHORITY_MATCH_CACHE_CAPACITY,
    CRS_SEARCH_CACHE, CRS_SEARCH_CACHE_CAPACITY, CachedAuthorityObjects, CachedCrsAuthorityMatches,
    CachedCrsSearch, CfValue, Confidence, CrsError, CrsExportKind, CrsInfo, CrsObjectKind,
    CrsSearchOptions, CrsWktOptions, IdentifyCandidate, NON_DEPRECATED_CACHE,
    NON_DEPRECATED_CACHE_CAPACITY, ProjObjList, ProjObject, WktVersion, authority_object_info,
    cstring, ensure_thread_caches_current, info, lru_resolve, normalize,
    proj_context_error_message, validate_min_confidence, with_proj_context,
};
use crate::error::Result;

/// Map PROJ axis unit metadata to a CF/`+units=` token for projected CRS.
fn cf_proj_units_token(unit_name: &str, to_metre: f64) -> String {
    let lower = unit_name.to_ascii_lowercase();
    if lower.contains("us survey")
        || lower.contains("u.s. survey")
        || (to_metre - 0.304_800_609_601_219).abs() < 1e-12
    {
        "us-ft".to_owned()
    } else if lower.contains("foot") || lower.contains("feet") || (to_metre - 0.3048).abs() < 1e-9 {
        "ft".to_owned()
    } else if (to_metre - 1.0).abs() < 1e-12 || lower.contains("metr") {
        "m".to_owned()
    } else {
        // Unknown linear unit: emit the native name so parse can reject or map.
        unit_name.to_owned()
    }
}

pub(crate) fn identify(value: &str, authority: Option<&str>) -> Result<Vec<IdentifyCandidate>> {
    let normalized = normalize(value)?;
    ProjObject::new(&normalized)?.identify(authority, normalized)
}

pub(crate) fn to_authority(
    value: &str,
    authority: Option<&str>,
    min_confidence: Confidence,
) -> Result<Option<(String, String)>> {
    Ok(authority_matches(value, authority, min_confidence)?
        .into_iter()
        .find_map(|candidate| match (candidate.authority, candidate.code) {
            (Some(authority), Some(code)) => Some((authority, code)),
            _ => None,
        }))
}

pub(crate) fn authority_matches(
    value: &str,
    authority: Option<&str>,
    min_confidence: Confidence,
) -> Result<Vec<IdentifyCandidate>> {
    validate_min_confidence(min_confidence);
    ensure_thread_caches_current();
    let normalized = normalize(value)?;
    let authority = authority.map(str::to_owned);
    CRS_AUTHORITY_MATCH_CACHE.with(|items| {
        let mut cache = items.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_AUTHORITY_MATCH_CACHE_CAPACITY,
            |item| {
                item.crs == normalized
                    && item.authority == authority
                    && item.min_confidence == min_confidence
            },
            || {
                let candidates = ProjObject::new(&normalized)?
                    .identify(authority.as_deref(), normalized.clone())?;
                let items: Vec<_> = candidates
                    .into_iter()
                    .filter(|candidate| {
                        candidate.confidence >= min_confidence
                            && candidate.authority.is_some()
                            && candidate.code.is_some()
                    })
                    .collect();
                Ok(CachedCrsAuthorityMatches {
                    crs: normalized.clone(),
                    authority: authority.clone(),
                    min_confidence,
                    items,
                })
            },
        )?;
        Ok(cache[index].items.clone())
    })
}

pub(crate) fn to_epsg(value: &str, min_confidence: Confidence) -> Result<Option<i32>> {
    let Some((_, code)) = to_authority(value, Some("EPSG"), min_confidence)? else {
        return Ok(None);
    };
    Ok(code.parse().ok())
}

pub(crate) fn to_2d(value: &str, name: Option<&str>) -> Result<String> {
    cached_export(
        value,
        || CrsExportKind::To2d {
            name: name.map(str::to_owned),
        },
        crs_to_2d_export,
    )
}

pub(crate) fn to_3d(value: &str, name: Option<&str>) -> Result<String> {
    cached_export(
        value,
        || CrsExportKind::To3d {
            name: name.map(str::to_owned),
        },
        crs_to_3d_export,
    )
}

pub(crate) fn to_cf(value: &str, wkt_version: WktVersion) -> Result<Vec<(&'static str, CfValue)>> {
    let normalized = normalize(value)?;
    let info = info(&normalized)?;
    // Compound CRSs are not a single CF grid_mapping; reject rather than
    // silently truncating to the horizontal ellipsoid metadata alone.
    if info.is_compound() {
        return Err(CrsError::invalid(format!(
            "CRS.to_cf() does not support compound CRS {normalized}; convert components separately"
        )));
    }
    let wkt = to_wkt_with_options(&normalized, wkt_version, &CrsWktOptions::default())?;
    let mut items = Vec::with_capacity(16);
    items.push(("crs_wkt", CfValue::String(wkt)));
    if let Some(ellipsoid) = &info.ellipsoid {
        items.push((
            "semi_major_axis",
            CfValue::Float(ellipsoid.semi_major_metre),
        ));
        items.push((
            "semi_minor_axis",
            CfValue::Float(ellipsoid.semi_minor_metre),
        ));
        items.push((
            "inverse_flattening",
            CfValue::Float(ellipsoid.inverse_flattening),
        ));
        if let Some(name) = &ellipsoid.name {
            items.push(("reference_ellipsoid_name", CfValue::String(name.clone())));
        }
    }
    if let Some(prime_meridian) = &info.prime_meridian {
        items.push((
            "longitude_of_prime_meridian",
            CfValue::Float(prime_meridian.longitude),
        ));
        if let Some(name) = &prime_meridian.name {
            items.push(("prime_meridian_name", CfValue::String(name.clone())));
        }
    }
    push_cf_crs_names_and_units(&mut items, &info);
    if let Some(name) = info.datum.as_ref().and_then(|datum| datum.name.clone()) {
        items.push(("horizontal_datum_name", CfValue::String(name)));
    }
    push_cf_grid_mapping(&mut items, &info);
    Ok(items)
}

fn push_cf_crs_names_and_units(items: &mut Vec<(&'static str, CfValue)>, info: &CrsInfo) {
    if matches!(info.kind, "geographic" | "geographic_2d" | "geographic_3d") {
        if let Some(name) = &info.name {
            items.push(("geographic_crs_name", CfValue::String(name.clone())));
        }
        return;
    }
    if info.kind != "projected" {
        return;
    }
    if let Some(name) = info.geodetic_crs.as_ref().and_then(|crs| crs.name.clone()) {
        items.push(("geographic_crs_name", CfValue::String(name)));
    }
    if let Some(name) = &info.name {
        items.push(("projected_crs_name", CfValue::String(name.clone())));
    }
    // Native horizontal linear unit — CF parse must not hard-code metres.
    if let Some(axis) = info.axes.first()
        && let Some(unit_name) = axis.unit_name.as_deref()
    {
        items.push((
            "units",
            CfValue::String(cf_proj_units_token(unit_name, axis.unit_conversion_factor)),
        ));
    }
}

fn push_cf_grid_mapping(items: &mut Vec<(&'static str, CfValue)>, info: &CrsInfo) {
    if matches!(info.kind, "geographic" | "geographic_2d" | "geographic_3d") {
        items.push((
            "grid_mapping_name",
            CfValue::String("latitude_longitude".to_owned()),
        ));
        return;
    }
    let Some(operation) = &info.coordinate_operation else {
        return;
    };
    if is_transverse_mercator(operation) {
        items.push((
            "grid_mapping_name",
            CfValue::String("transverse_mercator".to_owned()),
        ));
        add_cf_projection_parameters(
            items,
            &operation.parameters,
            CF_TRANSVERSE_MERCATOR_PARAMETERS,
        );
    } else if is_operation_method(operation, "9820", "Lambert Azimuthal Equal Area") {
        items.push((
            "grid_mapping_name",
            CfValue::String("lambert_azimuthal_equal_area".to_owned()),
        ));
        add_cf_lambert_azimuthal_equal_area_parameters(items, &operation.parameters);
    } else if is_operation_method(operation, "9802", "Lambert Conic Conformal (2SP)") {
        items.push((
            "grid_mapping_name",
            CfValue::String("lambert_conformal_conic".to_owned()),
        ));
        add_cf_lambert_conformal_conic_parameters(items, &operation.parameters);
    } else if is_operation_method(operation, "9804", "Mercator (variant A)") {
        items.push(("grid_mapping_name", CfValue::String("mercator".to_owned())));
        add_cf_mercator_parameters(items, &operation.parameters);
    } else if is_operation_method(operation, "9829", "Polar Stereographic (variant B)")
        || is_operation_method(operation, "9810", "Polar Stereographic (variant A)")
    {
        items.push((
            "grid_mapping_name",
            CfValue::String("polar_stereographic".to_owned()),
        ));
        add_cf_polar_stereographic_parameters(items, &operation.parameters);
    } else if is_operation_method(operation, "9835", "Lambert Cylindrical Equal Area") {
        items.push((
            "grid_mapping_name",
            CfValue::String("lambert_cylindrical_equal_area".to_owned()),
        ));
        add_cf_lambert_cylindrical_equal_area_parameters(items, &operation.parameters);
    }
}

pub(crate) fn non_deprecated(value: &str) -> Result<Vec<AuthorityObjectInfo>> {
    ensure_thread_caches_current();
    let normalized = normalize(value)?;
    NON_DEPRECATED_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            NON_DEPRECATED_CACHE_CAPACITY,
            |item| item.crs == normalized,
            || {
                let items = ProjObject::new(&normalized)?.non_deprecated()?;
                Ok(CachedAuthorityObjects {
                    crs: normalized.clone(),
                    items,
                })
            },
        )?;
        Ok(cache[index].items.clone())
    })
}

pub(crate) fn search(name: &str, options: &CrsSearchOptions) -> Result<Vec<AuthorityObjectInfo>> {
    ensure_thread_caches_current();
    options.validate()?;
    let name = name.trim();
    if name.is_empty() {
        return Err(CrsError::invalid(
            "CRS search name must be a non-empty string".to_owned(),
        ));
    }
    CRS_SEARCH_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_SEARCH_CACHE_CAPACITY,
            |item| item.name == name && item.options == *options,
            || {
                Ok(CachedCrsSearch {
                    name: name.to_owned(),
                    options: options.clone(),
                    items: search_uncached(name, options)?,
                })
            },
        )?;
        Ok(cache[index].items.clone())
    })
}

fn search_uncached(name: &str, options: &CrsSearchOptions) -> Result<Vec<AuthorityObjectInfo>> {
    let name = cstring(name)?;
    let authority = options.authority.as_deref().map(cstring).transpose()?;
    let types = options
        .kind
        .map(CrsObjectKind::to_proj)
        .into_iter()
        .collect::<Vec<_>>();
    with_proj_context(|context| {
        // SAFETY: DOC-H. Typed context; strings and optional type slice live for
        // the call; returns caller-owned object list or null.
        let list = unsafe {
            proj_sys::proj_create_from_name(
                context.as_ptr(),
                authority
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                name.as_ptr(),
                if types.is_empty() {
                    ptr::null()
                } else {
                    types.as_ptr()
                },
                types.len(),
                i32::from(options.approximate),
                options.limit.get(),
                ptr::null(),
            )
        };
        // SAFETY: non-null returns are uniquely owned by the caller.
        let Some(list) = (unsafe { ProjObjList::try_from_owned(list) }) else {
            if context.errno() == 0 {
                return Ok(Vec::new());
            }
            return Err(CrsError::invalid(proj_context_error_message(context)));
        };
        let count = list.count();
        let mut items = Vec::with_capacity(count.max(0) as usize);
        for index in 0..count {
            if let Some(object) = list.get(context, index) {
                items.push(authority_object_info(context, &object));
            }
        }
        Ok(items)
    })?
}
