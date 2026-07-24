#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! CRS introspection public API: `info`/`axis_order`/`factors`/`factor_columns`
//! and operation lookup (`operation_info`/`operation_info_at`).
//!
//! `&str`-keyed queries that drive the FFI object/cache helpers (which stay
//! private in the parent `crs` module) via `use super::*`; re-exported at
//! `crs`.

use super::*;
use crate::error::Result;
use crate::geometry::column_all_finite;

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
pub(crate) fn is_dynamic(value: &str) -> Result<bool> {
    Ok(info(value)?.datum.as_ref().is_some_and(datum_is_dynamic))
}

pub(crate) fn factors(
    target: &str,
    longitude: f64,
    latitude: f64,
    radians: bool,
) -> Result<ProjectionFactors> {
    let mut factors = factors_batch(target, &[longitude], &[latitude], radians)?;
    Ok(factors.remove(0))
}

pub(crate) fn factors_batch(
    target: &str,
    longitudes: &[f64],
    latitudes: &[f64],
    radians: bool,
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
            .map(|(&longitude, &latitude)| {
                cache[index].object.factors(longitude, latitude, radians)
            })
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
            .factor_columns(longitudes, latitudes, radians)
    })
}

fn projection_factors_object(target: &str) -> Result<CachedProjectionFactorsObject> {
    let target_definition = cstring(target)?;
    let context =
        ProjContext::new().map_err(|error| CrsError::crs_create(target, error.to_string()))?;
    let target_object =
        create_crs_transform_object(context.as_ptr(), target_definition.as_ptr(), target, None)?;
    let mut projection =
        projection_string_for_factors(context.as_ptr(), target_object.as_ptr(), target)?;
    if let Some(stripped) = projection.strip_suffix(" +type=crs") {
        projection.truncate(stripped.len());
    }
    let projection_definition = cstring(projection.as_str())?;
    let projection_object = create_crs_transform_object(
        context.as_ptr(),
        projection_definition.as_ptr(),
        &projection,
        None,
    )?;
    Ok(CachedProjectionFactorsObject {
        crs: target.to_owned(),
        object: ProjObject {
            object: projection_object,
            context,
        },
    })
}

fn projection_string_for_factors(
    context: *mut proj_sys::PJ_CONTEXT,
    object: *mut proj_sys::PJ,
    target: &str,
) -> Result<String> {
    let options = CrsProjOptions::default();
    let c_options = options.to_c_options()?;
    let option_ptrs = c_option_ptrs(&c_options);
    // SAFETY: `context` and `object` are live PROJ handles owned by RAII
    // guards in the caller; `option_ptrs` is null-terminated and points to C
    // strings that live for this call.
    let value = unsafe {
        proj_sys::proj_as_proj_string(
            context,
            object,
            proj_sys::PJ_PROJ_STRING_TYPE_PJ_PROJ_5,
            option_ptrs.as_ptr(),
        )
    };
    string_from_ptr(value)
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

pub(super) fn proj_info_paths(info: proj_sys::PJ_INFO) -> Vec<String> {
    if info.paths.is_null() || info.path_count == 0 {
        return Vec::new();
    }
    let mut paths = Vec::with_capacity(info.path_count);
    for index in 0..info.path_count {
        // SAFETY: PROJ reports path_count elements in the paths array.
        let path = unsafe { *info.paths.add(index) };
        if let Some(path) = string_from_ptr(path) {
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

pub(super) fn database_metadata(context: *mut proj_sys::PJ_CONTEXT) -> Vec<(String, String)> {
    DATABASE_METADATA_KEYS
        .iter()
        .filter_map(|key| {
            let key_c = CString::new(*key).ok()?;
            // SAFETY: context is valid and key_c is a valid C string for the call.
            let value =
                unsafe { proj_sys::proj_context_get_database_metadata(context, key_c.as_ptr()) };
            string_from_ptr(value).map(|value| ((*key).to_owned(), value))
        })
        .collect()
}

pub(crate) fn engine_info() -> Result<EngineInfo> {
    // SAFETY: proj_info returns a value struct with static/bound PROJ-owned strings
    // copied immediately by string_from_ptr.
    let info = unsafe { proj_sys::proj_info() };
    with_proj_context(|context| {
        // SAFETY: context is valid for these metadata calls.
        let (database_path, user_writable_directory) = unsafe {
            (
                string_from_ptr(proj_sys::proj_context_get_database_path(context)),
                string_from_ptr(proj_sys::proj_context_get_user_writable_directory(
                    context, 0,
                )),
            )
        };
        EngineInfo {
            backend: "proj-sys/libPROJ",
            bundled_proj: true,
            version: string_from_ptr(info.version),
            release: string_from_ptr(info.release),
            major: info.major,
            minor: info.minor,
            patch: info.patch,
            search_path: string_from_ptr(info.searchpath),
            paths: proj_info_paths(info),
            database_path,
            database_metadata: database_metadata(context),
            user_writable_directory,
        }
    })
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
