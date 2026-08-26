//! CRS string normalization (`normalize`/`canonicalize`) and global runtime
//! configuration (PROJ search paths and local grid directories).
//!
//! `&str`/`RuntimeConfig` API; the raw-pointer context mutators stay private in
//! the parent `crs` module, reached via `use super::*`; re-exported at `crs`.

use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::sync::Arc;

use smol_str::SmolStr;

use crate::crs::context::ProjDiagnostic;
use crate::crs::{
    AtomicU64, CRS_CANONICAL_CACHE, CRS_CANONICAL_CACHE_CAPACITY, CStr, Confidence, CrsError,
    OnceLock, Ordering, ProjContext, RuntimeConfig, RuntimeState, RwLock, c_char,
    ensure_thread_caches_current, info_with_state, lru_resolve, parse_epsg,
    to_authority_with_state,
};
use crate::error::Result;

const PROJ_INI: &str = "[general]\nnetwork = off\ncdn_endpoint = https://cdn.proj.org\ncache_enabled = on\ncache_size_MB = 300\ncache_ttl_sec = 86400\ntmerc_default_algo = poder_engsager\n";
const MAX_PROJ_DIAGNOSTIC_BYTES: usize = 4096;

static PROJ_CONFIG_DIRECTORY: OnceLock<std::result::Result<std::path::PathBuf, String>> =
    OnceLock::new();
static PROJ_CONFIG_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn proj_config_directory() -> Result<&'static std::path::Path> {
    match PROJ_CONFIG_DIRECTORY.get_or_init(|| {
        let temp = std::env::temp_dir();
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).map_err(|error| error.to_string())?;
        let nonce = u128::from_ne_bytes(random);
        for _ in 0..128 {
            let sequence = PROJ_CONFIG_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let directory = temp.join(format!(
                "gometry-proj-{}-{nonce:032x}-{sequence}",
                std::process::id()
            ));
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            builder.mode(0o700);
            match builder.create(&directory) {
                Ok(()) => {
                    let ini = directory.join("proj.ini");
                    let mut options = std::fs::OpenOptions::new();
                    options.write(true).create_new(true);
                    #[cfg(unix)]
                    options.mode(0o600);
                    let mut file = options.open(ini).map_err(|error| error.to_string())?;
                    file.write_all(PROJ_INI.as_bytes())
                        .map_err(|error| error.to_string())?;
                    return Ok(directory);
                },
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {},
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("could not allocate a private temporary directory".to_owned())
    }) {
        Ok(directory) => Ok(directory.as_path()),
        Err(error) => Err(CrsError::invalid(format!(
            "could not create gometry PROJ configuration: {error}"
        ))),
    }
}

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
fn projected_operation_params_match(
    lhs: &str,
    authority_crs: &str,
    state: &RuntimeState,
) -> Result<bool> {
    let lhs_info = info_with_state(lhs, state)?;
    let rhs_info = info_with_state(authority_crs, state)?;
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
    let state = runtime_state_snapshot()?;
    ensure_thread_caches_current(&state);
    CRS_CANONICAL_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_CANONICAL_CACHE_CAPACITY,
            |(key, _)| *key == normalized,
            || {
                let crs_info = info_with_state(&normalized, &state)?;
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
                        && !projected_operation_params_match(&normalized, &authority_crs, &state)?
                    {
                        normalized.clone()
                    } else {
                        authority_crs
                    }
                } else if let Some((authority, code)) = to_authority_with_state(
                    &normalized,
                    None,
                    Confidence::try_new("min_confidence", 70)?,
                    &state,
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
    // search paths / directory into context-owned storage; temporary
    // CString/pointer vectors need only outlive each call. PROJ invokes no
    // Python callback.
    unsafe {
        proj_sys::proj_log_func(
            context.as_ptr(),
            context.diagnostic_ptr(),
            Some(capture_proj_diagnostic),
        );
        // Missing-best-grid diagnostics are emitted at DEBUG. TRACE would add
        // unrelated per-step chatter; TELL is a query sentinel, not a level.
        proj_sys::proj_log_level(context.as_ptr(), proj_sys::PJ_LOG_LEVEL_PJ_LOG_DEBUG);
        let config_directory = proj_config_directory()?;
        let values = std::iter::once(config_directory.to_string_lossy().into_owned())
            .chain(config.search_paths.clone().unwrap_or_default())
            // PROJ's writable cache is also a valid grid search location. It
            // must be installed in the context search list, not merely set as
            // the download/cache destination, so caller-supplied grids are
            // actually discoverable without a process-global environment var.
            .chain(config.user_writable_directory.clone())
            .map(cstring)
            .collect::<Result<Vec<_>>>()?;
        let pointers = values
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        proj_sys::proj_context_set_search_paths(
            context.as_ptr(),
            pointers.len() as i32,
            pointers.as_ptr(),
        );
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
