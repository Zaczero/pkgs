//! CRS introspection public API: `info`/`axis_order`/`factors`/`factor_columns`
//! and operation lookup (`operation_info`/`operation_info_at`).
//!
//! `&str`-keyed queries that drive the FFI object/cache helpers (which stay
//! private in the parent `crs` module) via `use super::*`; re-exported at
//! `crs`.

use crate::crs::{
    CRS_FACTOR_CACHE, CRS_FACTOR_CACHE_CAPACITY, CRS_INFO_CACHE, CRS_INFO_CACHE_CAPACITY, CString,
    CachedCrsInfo, CachedProjectionFactorsObject, CrsError, CrsInfo, CrsProjOptions, DatumInfo,
    EngineInfo, OperationInfo, OwnedPj, ProjContext, ProjObject, ProjectionFactorColumns,
    ProjectionFactors, TransformOptions, c_option_ptrs, copy_proj_c_string,
    create_crs_transform_object, cstring, ensure_thread_caches_current, lru_resolve, normalize,
    normalize_pair, proj_context_error_message, with_proj_context, with_proj_diagnostic_pipeline,
    with_proj_pipeline,
};
use crate::error::Result;
use crate::geometry::column_all_finite;

#[derive(Clone, Copy)]
pub(crate) enum AngularUnit {
    Degrees,
    Radians,
}

impl AngularUnit {
    const fn from_radians(radians: bool) -> Self {
        if radians {
            Self::Radians
        } else {
            Self::Degrees
        }
    }

    pub(super) const fn is_radians(self) -> bool {
        matches!(self, Self::Radians)
    }
}

pub(crate) fn info(value: &str) -> Result<std::sync::Arc<CrsInfo>> {
    ensure_thread_caches_current();
    let normalized = normalize(value)?;
    CRS_INFO_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_INFO_CACHE_CAPACITY,
            |item| item.crs == normalized,
            || {
                let info = ProjObject::new(&normalized)?.info(normalized.clone());
                Ok(CachedCrsInfo {
                    crs: normalized.clone(),
                    info: std::sync::Arc::new(info),
                })
            },
        )?;
        // Cheap refcount clone — `CrsInfo` is immutable per CRS.
        Ok(std::sync::Arc::clone(&cache[index].info))
    })
}

/// Whether one datum is a backend-admissible dynamic reference frame. PROJ
/// marks actual dynamic frames with a reference epoch; an ensemble merely
/// containing dynamic realizations does not inherit that property. WGS 84's
/// EPSG ensemble is the deliberate exception: PROJ accepts coordinate epochs
/// for it and its derived CRSs.
fn datum_is_dynamic(datum: &DatumInfo) -> bool {
    datum.frame_reference_epoch.is_some()
        || (datum.authority.as_deref() == Some("EPSG") && datum.code.as_deref() == Some("6326"))
}

/// Whether a CRS admits a coordinate epoch in PROJ — the question behind the
/// epoch-through-`to_crs` policy. One cached `info` lookup per distinct CRS.
///
/// Recurses through compound sub-CRSs and datum ensembles: a compound whose
/// horizontal component is dynamic (e.g. EPSG:9707 = WGS 84 + height) is
/// dynamic even when the compound object's own datum slot is empty.
pub(crate) fn is_dynamic(value: &str) -> Result<bool> {
    is_dynamic_info(info(value)?.as_ref())
}

fn is_dynamic_info(crs: &CrsInfo) -> Result<bool> {
    if crs.datum.as_ref().is_some_and(datum_is_dynamic) {
        return Ok(true);
    }
    // Compound components only — never re-resolve a datum/ensemble authority
    // code as a CRS (EPSG:6326 is a datum ensemble, not a constructible CRS).
    for sub in &crs.sub_crs {
        if !sub.crs.is_empty() && is_dynamic(&sub.crs)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn factors(
    target: &str,
    longitude: f64,
    latitude: f64,
    radians: bool,
) -> Result<ProjectionFactors> {
    let mut factors = factors_batch(
        target,
        &[longitude],
        &[latitude],
        AngularUnit::from_radians(radians),
    )?;
    Ok(factors.remove(0))
}

pub(crate) fn factors_batch(
    target: &str,
    longitudes: &[f64],
    latitudes: &[f64],
    unit: AngularUnit,
) -> Result<Vec<ProjectionFactors>> {
    if longitudes.len() != latitudes.len() {
        return Err(CrsError::invalid(
            "factor coordinates must have the same length".to_owned(),
        ));
    }
    if !column_all_finite(longitudes) || !column_all_finite(latitudes) {
        return Err(CrsError::invalid(
            "factor coordinates must be finite".to_owned(),
        ));
    }
    ensure_thread_caches_current();
    let target = normalize(target)?;
    CRS_FACTOR_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_FACTOR_CACHE_CAPACITY,
            |item| item.crs == target,
            || projection_factors_object(&target),
        )?;
        longitudes
            .iter()
            .zip(latitudes)
            .map(|(&longitude, &latitude)| cache[index].object.factors(longitude, latitude, unit))
            .collect()
    })
}

pub(crate) fn ensure_same_geodesic_len<const N: usize>(
    inputs: [(usize, &str); N],
    description: &str,
) -> Result<()> {
    let Some((expected, _)) = inputs.first() else {
        return Ok(());
    };
    if inputs.iter().all(|(len, _)| len == expected) {
        Ok(())
    } else {
        Err(CrsError::invalid(format!(
            "geodesic inputs {description} must have the same length"
        )))
    }
}

pub(crate) fn factor_columns(
    target: &str,
    longitudes: &[f64],
    latitudes: &[f64],
    radians: bool,
) -> Result<ProjectionFactorColumns> {
    let unit = AngularUnit::from_radians(radians);
    if longitudes.len() != latitudes.len() {
        return Err(CrsError::invalid(
            "factor coordinates must have the same length".to_owned(),
        ));
    }
    if !column_all_finite(longitudes) || !column_all_finite(latitudes) {
        return Err(CrsError::invalid(
            "factor coordinates must be finite".to_owned(),
        ));
    }
    ensure_thread_caches_current();
    let target = normalize(target)?;
    CRS_FACTOR_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_FACTOR_CACHE_CAPACITY,
            |item| item.crs == target,
            || projection_factors_object(&target),
        )?;
        cache[index]
            .object
            .factor_columns(longitudes, latitudes, unit)
    })
}

fn projection_factors_object(target: &str) -> Result<CachedProjectionFactorsObject> {
    let target_definition = cstring(target)?;
    let context =
        ProjContext::new().map_err(|error| CrsError::crs_create(target, error.to_string()))?;
    let target_object = create_crs_transform_object(&context, &target_definition, target, None)?;
    let mut projection = projection_string_for_factors(&context, &target_object, target)?;
    if let Some(stripped) = projection.strip_suffix(" +type=crs") {
        projection.truncate(stripped.len());
    }
    let projection_definition = cstring(projection.as_str())?;
    let projection_object =
        create_crs_transform_object(&context, &projection_definition, &projection, None)?;
    Ok(CachedProjectionFactorsObject {
        crs: target.to_owned(),
        object: ProjObject {
            object: projection_object,
            context,
        },
    })
}

fn projection_string_for_factors(
    context: &ProjContext,
    object: &OwnedPj,
    target: &str,
) -> Result<String> {
    let options = CrsProjOptions::default();
    let c_options = options.to_c_options()?;
    let option_ptrs = c_option_ptrs(&c_options);
    // SAFETY: DOC-H. Typed live context/object; option_ptrs null-terminated and
    // points to C strings that live for this call.
    let value = unsafe {
        proj_sys::proj_as_proj_string(
            context.as_ptr(),
            object.as_ptr(),
            proj_sys::PJ_PROJ_STRING_TYPE_PJ_PROJ_5,
            option_ptrs.as_ptr(),
        )
    };
    proj_c_string!(value)
        .ok_or_else(|| CrsError::export(target, "PROJ string", proj_context_error_message(context)))
}
pub(crate) fn operation_info(
    source: &str,
    target: &str,
    options: &TransformOptions,
) -> Result<OperationInfo> {
    let (source, target) = normalize_pair(source, target)?;
    let source_info = source.clone();
    let target_info = target.clone();
    with_proj_pipeline(&source, &target, options, |pipeline| {
        Ok(pipeline.operation_info(
            source_info,
            target_info,
            options.source_epoch,
            options.target_epoch,
        ))
    })
}

pub(crate) fn operation_info_at(
    source: &str,
    target: &str,
    x: f64,
    y: f64,
    zt: crate::ZtValues,
    options: &TransformOptions,
) -> Result<OperationInfo> {
    let finite_zt = match &zt {
        crate::Zt::None => true,
        crate::Zt::Z(z) => z.is_finite(),
        crate::Zt::T(t) => t.is_finite(),
        crate::Zt::Zt { z, t } => z.is_finite() && t.is_finite(),
    };
    if !x.is_finite() || !y.is_finite() || !finite_zt {
        return Err(CrsError::invalid(
            "operation_at coordinates must be finite".to_owned(),
        ));
    }
    let (z, t) = match zt {
        crate::Zt::None => (None, None),
        crate::Zt::Z(z) => (Some(z), None),
        crate::Zt::T(t) => (None, Some(t)),
        crate::Zt::Zt { z, t } => (Some(z), Some(t)),
    };
    options.validate()?;
    if options.only_best == Some(true) || options.force_over {
        return operation_info(source, target, options);
    }
    let (source, target) = normalize_pair(source, target)?;
    let source_info = source.clone();
    let target_info = target.clone();
    with_proj_diagnostic_pipeline(&source, &target, options, |pipeline| {
        pipeline.operation_info_at(
            source_info,
            target_info,
            options.source_epoch,
            options.target_epoch,
            x,
            y,
            z,
            t,
        )
    })
}

/// Copy path entries from a `proj_info()` snapshot under [`PROJ_INFO_LOCK`].
///
/// # Safety
///
/// Call only while holding the lock that serialized `proj_info()` and while
/// the returned `paths` pointers remain valid (before any concurrent
/// `proj_info()` can free them). `path_count` is the count reported by that
/// same call.
unsafe fn proj_info_paths_locked(info: &proj_sys::PJ_INFO) -> Vec<String> {
    if info.paths.is_null() || info.path_count == 0 {
        return Vec::new();
    }
    let mut paths = Vec::with_capacity(info.path_count);
    for index in 0..info.path_count {
        // SAFETY: LIST(path_count) under the lock that owns the snapshot.
        let path = unsafe { *info.paths.add(index) };
        if let Some(path) = proj_c_string!(path) {
            paths.push(path);
        }
    }
    paths
}

pub(crate) const DATABASE_METADATA_KEYS: [&str; 10] = [
    "DATABASE.LAYOUT.VERSION.MAJOR",
    "DATABASE.LAYOUT.VERSION.MINOR",
    "EPSG.VERSION",
    "EPSG.DATE",
    "ESRI.VERSION",
    "ESRI.DATE",
    "IGNF.SOURCE",
    "IGNF.VERSION",
    "IGNF.DATE",
    "PROJ.VERSION",
];

pub(super) fn database_metadata(context: &ProjContext) -> Vec<(String, String)> {
    DATABASE_METADATA_KEYS
        .iter()
        .filter_map(|key| {
            let key_c = CString::new(*key).ok()?;
            // SAFETY: DOC-H. Typed live context; key_c is a live CString for the
            // call; returned string is context-lifetime and copied immediately.
            let value = unsafe {
                proj_sys::proj_context_get_database_metadata(context.as_ptr(), key_c.as_ptr())
            };
            proj_c_string!(value).map(|value| ((*key).to_owned(), value))
        })
        .collect()
}

pub(crate) fn engine_info() -> Result<EngineInfo> {
    // `proj_info()` returns pointers into PROJ's mutable global metadata
    // storage. Concurrent `proj_info()` rewrites/frees that backing after the
    // call returns (reproduced on free-threaded CPython 3.14t: corrupt search
    // paths and empty version strings). Own every string *immediately* under a
    // process-wide lock, before any other PROJ/context work can run.
    let snapshot = owned_proj_info_snapshot();
    with_proj_context(|context| {
        // SAFETY: DOC-H. Typed live context; returned paths/directory are
        // context-lifetime C strings copied immediately.
        let (database_path, user_writable_directory) = unsafe {
            (
                copy_proj_c_string(proj_sys::proj_context_get_database_path(context.as_ptr())),
                copy_proj_c_string(proj_sys::proj_context_get_user_writable_directory(
                    context.as_ptr(),
                    0,
                )),
            )
        };
        EngineInfo {
            backend: "proj-sys/libPROJ",
            bundled_proj: true,
            version: snapshot.version,
            release: snapshot.release,
            major: snapshot.major,
            minor: snapshot.minor,
            patch: snapshot.patch,
            search_path: snapshot.search_path,
            paths: snapshot.paths,
            database_path,
            database_metadata: database_metadata(context),
            user_writable_directory,
        }
    })
}

/// Owned snapshot of every `proj_info()` string field.
///
/// Captured under [`PROJ_INFO_LOCK`] so concurrent callers cannot free/rewrite
/// the global backing mid-copy.
struct OwnedProjInfo {
    version: Option<String>,
    release: Option<String>,
    major: i32,
    minor: i32,
    patch: i32,
    search_path: Option<String>,
    paths: Vec<String>,
}

/// Serializes `proj_info()` + all string copies. PROJ releases its own mutex
/// before returning, so without this lock two free-threaded callers race on
/// the mutable global string backing.
static PROJ_INFO_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn owned_proj_info_snapshot() -> OwnedProjInfo {
    let _guard = PROJ_INFO_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // SAFETY: held under PROJ_INFO_LOCK; all returned C-string pointers are
    // copied into owned Rust storage before the lock is released, so no other
    // thread can rewrite/free the global backing while we read them.
    let info = unsafe { proj_sys::proj_info() };
    OwnedProjInfo {
        version: proj_c_string!(info.version),
        release: proj_c_string!(info.release),
        major: info.major,
        minor: info.minor,
        patch: info.patch,
        search_path: proj_c_string!(info.searchpath),
        // SAFETY: still under PROJ_INFO_LOCK; paths remain live until unlock.
        paths: unsafe { proj_info_paths_locked(&info) },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The epoch-through-`to_crs` classification: WGS84's explicitly handled
    /// ensemble and dynamic frames (ITRF2014) admit an epoch; a static frame
    /// and a merely dynamic-member ensemble (ETRS89) do not.
    #[test]
    fn dynamic_classification_matches_frame_physics() {
        for (crs, expected) in [
            ("EPSG:4326", true),  // WGS 84 ensemble of dynamic realizations
            ("EPSG:9000", true),  // ITRF2014 (dynamic reference frame)
            ("EPSG:2180", false), // ETRF2000 Poland (static geodetic frame)
            ("EPSG:4258", false), // ETRS89 ensemble is not itself dynamic
            ("EPSG:3857", true),  // Web Mercator on the WGS84 ensemble
        ] {
            assert_eq!(
                is_dynamic(crs).unwrap(),
                expected,
                "dynamic classification for {crs}",
            );
        }
    }
}
