//! CRS string normalization (`normalize`/`definition`) and global runtime
//! configuration (PROJ search paths and local grid directories).
//!
//! `&str`/`RuntimeConfig` API; the raw-pointer context mutators stay private in
//! the parent `crs` module, reached via `use super::*`; re-exported at `crs`.

use std::sync::Arc;

use smol_str::SmolStr;

use crate::crs::context::ProjDiagnostic;
use crate::crs::{
    CStr, CrsError, OnceLock, ProjContext, RuntimeConfig, RuntimeState, RwLock, c_char,
    ensure_thread_caches_current, info_with_state, parse_epsg,
};
use crate::error::Result;
#[cfg(test)]
use crate::test_support::binary_dispatch_counters::runtime_state_snapshot as count_runtime_state_snapshot;

const MAX_PROJ_DIAGNOSTIC_BYTES: usize = 4096;

unsafe extern "C" fn capture_proj_diagnostic(
    app_data: *mut std::ffi::c_void,
    _level: i32,
    message: *const c_char,
) {
    if message.is_null() {
        return;
    }
    // SAFETY: PROJ invokes its registered callback synchronously with a live,
    // NUL-terminated message. We copy it before returning and retain no pointer.
    let bytes = unsafe { CStr::from_ptr(message) }.to_bytes();
    if !bytes.starts_with(b"Attempt to use coordinate operation ")
        || !bytes
            .windows(b" is not available.".len())
            .any(|window| window == b" is not available.")
    {
        return;
    }
    let end = bytes.len().min(MAX_PROJ_DIAGNOSTIC_BYTES);
    let message = String::from_utf8_lossy(&bytes[..end]).into_owned();
    if app_data.is_null() {
        return;
    }
    // SAFETY: `app_data` is the stable boxed diagnostic slot owned by the live
    // context that registered this callback. PROJ invokes it synchronously.
    let diagnostic = unsafe { &*(app_data.cast::<ProjDiagnostic>()) };
    diagnostic.store(message);
}

const unsafe extern "C" fn bare_resource(
    _context: *mut proj_sys::PJ_CONTEXT,
    name: *const c_char,
    _user_data: *mut std::ffi::c_void,
) -> *const c_char {
    name
}

pub(crate) fn normalize(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if let Some((authority, code)) = trimmed.split_once(':')
        && authority.eq_ignore_ascii_case("EPSG")
    {
        let code = code.parse::<u32>().map_err(|_| {
            CrsError::invalid(
                "invalid EPSG code: expected an unsigned 32-bit decimal code".to_owned(),
            )
        })?;
        let mut digit_count = 1;
        let mut remaining = code;
        while remaining >= 10 {
            digit_count += 1;
            remaining /= 10;
        }
        let mut normalized = String::new();
        normalized
            .try_reserve_exact("EPSG:".len() + digit_count)
            .map_err(crate::error::Error::from)?;
        std::fmt::write(&mut normalized, format_args!("EPSG:{code}"))
            .map_err(|_| crate::error::ResourceError::Allocation)?;
        return Ok(normalized);
    }
    match trimmed {
        "CRS84" => crate::boundary::try_string_from_str("OGC:CRS84")
            .map_err(|()| crate::error::ResourceError::Allocation.into()),
        _ => crate::boundary::try_string_from_str(trimmed)
            .map_err(|()| crate::error::ResourceError::Allocation.into()),
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

/// Validate a CRS through PROJ while retaining the caller's normalized
/// definition. Authority IDs discovered in an authored WKT/PROJJSON object are
/// metadata only; they never replace the definition used for later operations.
pub(crate) fn definition(value: &str) -> Result<SmolStr> {
    let normalized = normalize(value)?;
    let state = runtime_state_snapshot()?;
    // Keep the existing eager-admission contract: malformed or unavailable
    // definitions fail at the public constructor, while the stored value stays
    // exactly the normalized caller input.
    let _ = info_with_state(&normalized, &state)?;
    Ok(SmolStr::from(normalized))
}

pub(crate) fn runtime_config() -> Result<RuntimeConfig> {
    Ok((*runtime_state_snapshot()?.config).clone())
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
    let mut state = runtime_state_lock()
        .write()
        .map_err(|_| CrsError::invalid("CRS runtime config lock is poisoned".to_owned()))?;
    let mut config = (*state.config).clone();
    if let Some(search_paths) = search_paths {
        config.search_paths = Some(search_paths);
    }
    if let Some(user_writable_directory) = user_writable_directory {
        config.user_writable_directory = Some(user_writable_directory);
    }
    if config == *state.config {
        return Ok(config);
    }
    let updated = Arc::new(RuntimeState {
        generation: state.generation.wrapping_add(1),
        config: Arc::new(config),
    });
    *state = Arc::clone(&updated);
    drop(state);
    ensure_thread_caches_current(&updated);
    Ok((*updated.config).clone())
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
    let updated = {
        let mut state = runtime_state_lock()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let updated = Arc::new(RuntimeState {
            generation: state.generation.wrapping_add(1),
            config: Arc::clone(&state.config),
        });
        *state = Arc::clone(&updated);
        updated
    };
    ensure_thread_caches_current(&updated);
}

pub(crate) fn reset_runtime_config() -> Result<RuntimeConfig> {
    let mut state = runtime_state_lock()
        .write()
        .map_err(|_| CrsError::invalid("CRS runtime config lock is poisoned".to_owned()))?;
    let updated = Arc::new(RuntimeState {
        generation: state.generation.wrapping_add(1),
        config: Arc::new(RuntimeConfig::default()),
    });
    *state = Arc::clone(&updated);
    drop(state);
    ensure_thread_caches_current(&updated);
    Ok((*updated.config).clone())
}

pub(crate) static PROJ_STATE: OnceLock<RwLock<Arc<RuntimeState>>> = OnceLock::new();

pub(crate) fn runtime_state_lock() -> &'static RwLock<Arc<RuntimeState>> {
    PROJ_STATE.get_or_init(|| {
        RwLock::new(Arc::new(RuntimeState {
            generation: 0,
            config: Arc::new(RuntimeConfig::default()),
        }))
    })
}

pub(crate) fn runtime_state_snapshot() -> Result<Arc<RuntimeState>> {
    #[cfg(test)]
    count_runtime_state_snapshot();
    runtime_state_lock()
        .read()
        .map_err(|_| CrsError::invalid("CRS runtime config lock is poisoned".to_owned()))
        .map(|state| Arc::clone(&state))
}

/// Apply process runtime search-path / writable-directory settings to a live
/// typed context. Callers wrap the context in [`ProjContext`] *before* this
/// so error/panic paths never need a manual `proj_context_destroy`.
pub(super) fn apply_runtime_config(context: &ProjContext, config: &RuntimeConfig) -> Result<()> {
    // SAFETY: DOC-H. Typed live context on the creating thread. PROJ copies
    // search paths / directory into context-owned storage; CString/pointer
    // vectors need only outlive each call. The bare finder returns the
    // caller-owned resource name, which PROJ consumes synchronously.
    unsafe {
        proj_sys::proj_log_func(
            context.as_ptr(),
            context.diagnostic_ptr(),
            Some(capture_proj_diagnostic),
        );
        // Missing-best-grid diagnostics are emitted at DEBUG. TRACE would add
        // unrelated per-step chatter; TELL is a query sentinel, not a level.
        proj_sys::proj_log_level(context.as_ptr(), proj_sys::PJ_LOG_LEVEL_PJ_LOG_DEBUG);
        let values = config
            .search_paths
            .clone()
            .unwrap_or_default()
            // PROJ's writable cache is also a valid grid search location. It
            // must be installed in the context search list, not merely set as
            // the download/cache destination, so caller-supplied grids are
            // actually discoverable without a process-global environment var.
            .into_iter()
            .chain(config.user_writable_directory.clone())
            .map(cstring)
            .collect::<Result<Vec<_>>>()?;
        if values.is_empty() {
            proj_sys::proj_context_set_file_finder(
                context.as_ptr(),
                Some(bare_resource),
                std::ptr::null_mut(),
            );
        } else {
            proj_sys::proj_context_set_file_finder(context.as_ptr(), None, std::ptr::null_mut());
            let pointers = values
                .iter()
                .map(|value| value.as_ptr())
                .collect::<Vec<_>>();
            proj_sys::proj_context_set_search_paths(
                context.as_ptr(),
                pointers.len() as i32,
                pointers.as_ptr(),
            );
        }
        if let Some(directory) = &config.user_writable_directory {
            let directory = cstring(directory.as_str())?;
            proj_sys::proj_context_set_user_writable_directory(
                context.as_ptr(),
                directory.as_ptr(),
                0,
            );
        }
    }
    Ok(())
}
