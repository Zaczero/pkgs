#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! CRS string normalization (`normalize`/`canonicalize`) and global runtime
//! configuration (PROJ search paths and local grid directories).
//!
//! `&str`/`RuntimeConfig` API; the raw-pointer context mutators stay private in
//! the parent `crs` module, reached via `use super::*`; re-exported at `crs`.

use smol_str::SmolStr;

use super::*;
use crate::error::Result;

pub(crate) fn normalize(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if let Some((authority, code)) = trimmed.split_once(':')
        && authority.eq_ignore_ascii_case("EPSG")
    {
        let code = code
            .parse::<u32>()
            .map_err(|_| CrsError::invalid(value.to_owned()))?;
        return Ok(format!("EPSG:{code}"));
    }
    match trimmed {
        "CRS84" => Ok("OGC:CRS84".to_owned()),
        _ => Ok(trimmed.to_owned()),
    }
}

pub(crate) fn normalize_pair(source: &str, target: &str) -> Result<(String, String)> {
    let source = normalize(source)?;
    let target = normalize(target)?;
    Ok((source, target))
}

pub(crate) fn coordinate_identity_crs(source: &str, target: &str) -> bool {
    normalize(source).is_ok_and(|source| normalize(target).is_ok_and(|target| source == target))
        || is_literal_wgs84_lonlat_identity(source) && is_literal_wgs84_lonlat_identity(target)
        || is_literal_web_mercator_identity(source) && is_literal_web_mercator_identity(target)
}

fn is_literal_wgs84_lonlat_identity(value: &str) -> bool {
    matches!(
        normalize(value).as_deref(),
        Ok("EPSG:4326" | "EPSG:4979" | "OGC:CRS84")
    ) || value == "urn:ogc:def:crs:OGC:1.3:CRS84"
}

fn is_literal_web_mercator_identity(value: &str) -> bool {
    matches!(parse_epsg(value), Some(3857 | 900_913 | 3785 | 102_100))
}

pub(crate) fn cstring(value: impl Into<Vec<u8>>) -> Result<std::ffi::CString> {
    std::ffi::CString::new(value).map_err(|error| CrsError::invalid(error.to_string()))
}

/// WKT/ProjJSON carry caller-authored conversion parameters that PROJ's `info`
/// may canonicalize to a database EPSG record — keep the definition string for
/// transform/admission so in_core never shadows mismatched params.
fn projected_operation_params_match(lhs: &str, authority_crs: &str) -> Result<bool> {
    let lhs_info = info(lhs)?;
    let rhs_info = info(authority_crs)?;
    if !lhs_info.is_projected() || !rhs_info.is_projected() {
        return Ok(true);
    }
    let Some(lhs_op) = lhs_info.coordinate_operation.as_ref() else {
        return Ok(true);
    };
    let Some(rhs_op) = rhs_info.coordinate_operation.as_ref() else {
        return Ok(false);
    };
    if lhs_op.method.as_ref().map(|m| m.code.as_deref())
        != rhs_op.method.as_ref().map(|m| m.code.as_deref())
    {
        return Ok(false);
    }
    let mut rhs_params: std::collections::BTreeMap<String, f64> = std::collections::BTreeMap::new();
    for param in &rhs_op.parameters {
        let Some(code) = param.code.as_ref() else {
            continue;
        };
        rhs_params.insert(code.clone(), param.value * param.unit_conversion_factor);
    }
    for param in &lhs_op.parameters {
        let Some(code) = param.code.as_ref() else {
            if param.value != 0.0 {
                return Ok(false);
            }
            continue;
        };
        let lhs_value = param.value * param.unit_conversion_factor;
        match rhs_params.get(code) {
            Some(rhs_value) if (lhs_value - rhs_value).abs() <= 1e-9 => {},
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn crs_definition_must_stay_verbatim(normalized: &str) -> bool {
    let trimmed = normalized.trim_start();
    // Only projected ProjJSON carries caller-authored conversion parameters
    // that PROJ `info` may canonicalize away. WKT and geographic definitions
    // may still collapse to authority codes.
    trimmed.starts_with('{')
        && (trimmed.contains("\"type\":\"ProjectedCRS\"")
            || trimmed.contains("\"type\": \"ProjectedCRS\""))
}

pub(crate) fn canonicalize(value: &str) -> Result<SmolStr> {
    let normalized = normalize(value)?;
    // Canonicalization repeats heavily across scalar CRS calls (e.g. the geodesic
    // inverse loop). Cache the small input->canonical string mapping so the hot
    // path avoids cloning the heavy `CrsInfo` struct on every call.
    ensure_thread_caches_current();
    CRS_CANONICAL_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_CANONICAL_CACHE_CAPACITY,
            |(key, _)| *key == normalized,
            || {
                let crs_info = info(&normalized)?;
                let canonical = if crs_definition_must_stay_verbatim(&normalized) {
                    normalized.clone()
                } else if parse_epsg(&normalized).is_some() {
                    // Keep the caller's EPSG spelling (e.g. EPSG:102100 must not
                    // rewrite to ESRI:102100 — literal/admission gates key on it).
                    normalized.clone()
                } else if let (Some(authority), Some(code)) =
                    (crs_info.authority.clone(), crs_info.code.clone())
                {
                    let authority_crs = format!("{authority}:{code}");
                    if crs_info.is_projected()
                        && !projected_operation_params_match(&normalized, &authority_crs)?
                    {
                        normalized.clone()
                    } else {
                        authority_crs
                    }
                } else if let Some((authority, code)) = to_authority(
                    &normalized,
                    None,
                    Confidence::try_new("min_confidence", 70)?,
                )? {
                    format!("{authority}:{code}")
                } else {
                    normalized.clone()
                };
                Ok((normalized.clone(), SmolStr::from(canonical)))
            },
        )?;
        Ok(cache[index].1.clone())
    })
}

pub(crate) fn runtime_config() -> Result<RuntimeConfig> {
    runtime_config_snapshot()
}

pub(crate) fn configure_runtime(
    search_paths: Option<Vec<String>>,
    user_writable_directory: Option<String>,
) -> Result<RuntimeConfig> {
    if let Some(paths) = &search_paths {
        if paths.iter().any(String::is_empty) {
            return Err(CrsError::invalid(
                "search_paths entries must be non-empty strings".to_owned(),
            ));
        }
        for path in paths {
            cstring(path.as_str())?;
        }
    }
    validate_runtime_path(
        "user_writable_directory",
        user_writable_directory.as_deref(),
    )?;
    let mut config = runtime_config_lock()
        .write()
        .map_err(|_| CrsError::invalid("CRS runtime config lock is poisoned".to_owned()))?;
    if let Some(search_paths) = search_paths {
        config.search_paths = Some(search_paths);
    }
    if let Some(user_writable_directory) = user_writable_directory {
        config.user_writable_directory = Some(user_writable_directory);
    }
    let updated = config.clone();
    bump_runtime_config_generation();
    drop(config);
    ensure_thread_caches_current();
    Ok(updated)
}

fn validate_runtime_path(name: &str, value: Option<&str>) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() {
        return Err(CrsError::invalid(format!(
            "{name} must be a non-empty string"
        )));
    }
    cstring(value)?;
    Ok(())
}

pub(crate) fn clear_cache() {
    bump_runtime_config_generation();
    ensure_thread_caches_current();
}

pub(crate) fn reset_runtime_config() -> Result<RuntimeConfig> {
    let mut config = runtime_config_lock()
        .write()
        .map_err(|_| CrsError::invalid("CRS runtime config lock is poisoned".to_owned()))?;
    *config = RuntimeConfig::default();
    let updated = config.clone();
    bump_runtime_config_generation();
    drop(config);
    ensure_thread_caches_current();
    Ok(updated)
}

pub(crate) static PROJ_CONFIG: OnceLock<RwLock<RuntimeConfig>> = OnceLock::new();
pub(crate) static PROJ_CONFIG_GENERATION: AtomicU64 = AtomicU64::new(0);

pub(crate) fn runtime_config_lock() -> &'static RwLock<RuntimeConfig> {
    PROJ_CONFIG.get_or_init(|| RwLock::new(RuntimeConfig::default()))
}

pub(crate) fn runtime_config_snapshot() -> Result<RuntimeConfig> {
    runtime_config_lock()
        .read()
        .map_err(|_| CrsError::invalid("CRS runtime config lock is poisoned".to_owned()))
        .map(|config| config.clone())
}

pub(crate) fn bump_runtime_config_generation() {
    PROJ_CONFIG_GENERATION.fetch_add(1, Ordering::AcqRel);
}

pub(crate) fn runtime_config_generation() -> u64 {
    PROJ_CONFIG_GENERATION.load(Ordering::Acquire)
}

pub(super) fn create_proj_context() -> Result<std::ptr::NonNull<proj_sys::PJ_CONTEXT>> {
    let config = runtime_config_snapshot()?;
    // SAFETY: PROJ context creation returns an owned context pointer or null.
    let context = std::ptr::NonNull::new(unsafe { proj_sys::proj_context_create() })
        .ok_or_else(|| CrsError::invalid("PROJ context creation returned null".to_owned()))?;
    if let Err(error) = apply_runtime_config(context, &config) {
        // SAFETY: context is owned on this path and destroyed exactly once.
        unsafe {
            proj_sys::proj_context_destroy(context.as_ptr());
        }
        return Err(error);
    }
    Ok(context)
}

pub(super) fn apply_runtime_config(
    context: std::ptr::NonNull<proj_sys::PJ_CONTEXT>,
    config: &RuntimeConfig,
) -> Result<()> {
    // SAFETY: context is a valid PROJ context and these setters only mutate
    // context-local PROJ behavior.
    unsafe {
        let context = context.as_ptr();
        proj_sys::proj_log_level(context, proj_sys::PJ_LOG_LEVEL_PJ_LOG_NONE);
        if let Some(search_paths) = &config.search_paths {
            let values = search_paths
                .iter()
                .map(|path| cstring(path.as_str()))
                .collect::<Result<Vec<_>>>()?;
            let pointers = values
                .iter()
                .map(|value| value.as_ptr())
                .collect::<Vec<_>>();
            proj_sys::proj_context_set_search_paths(
                context,
                pointers.len() as i32,
                pointers.as_ptr(),
            );
        }
        if let Some(directory) = &config.user_writable_directory {
            let directory = cstring(directory.as_str())?;
            proj_sys::proj_context_set_user_writable_directory(context, directory.as_ptr(), 0);
        }
    }
    Ok(())
}
