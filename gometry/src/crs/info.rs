//! CRS introspection public API: `info`/`axis_order`/`factors`/`factor_columns`
//! and operation lookup (`operation_info`/`operation_info_at`).
//!
//! `&str`-keyed queries that drive the FFI object/cache helpers (which stay
//! private in the parent `crs` module) via `use super::*`; re-exported at
//! `crs`.

use crate::crs::{
    CRS_FACTOR_CACHE, CRS_FACTOR_CACHE_CAPACITY, CRS_INFO_CACHE, CRS_INFO_CACHE_CAPACITY, CString,
    CachedCrsInfo, CachedProjectionFactorsObject, CrsError, CrsInfo, CrsProjOptions, EngineInfo,
    OperationInfo, OwnedPj, ProjContext, ProjObject, ProjectionFactorColumns, ProjectionFactors,
    RuntimeState, TransformExecution, TransformOptions, c_option_ptrs, copy_proj_c_string,
    create_crs_transform_object, cstring, ensure_thread_caches_current, lru_resolve, normalize,
    normalize_pair, proj_context_error_message, runtime_state_snapshot, with_proj_context_at,
    with_proj_diagnostic_pipeline_at, with_proj_pipeline_at,
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
    let state = runtime_state_snapshot()?;
    info_with_state(value, &state)
}

pub(crate) fn info_with_state(
    value: &str,
    state: &RuntimeState,
) -> Result<std::sync::Arc<CrsInfo>> {
    ensure_thread_caches_current(state);
    let normalized = normalize(value)?;
    CRS_INFO_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_INFO_CACHE_CAPACITY,
            |item| item.crs == normalized,
            || {
                let info =
                    ProjObject::new_with_state(&normalized, state)?.info(normalized.clone())?;
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

/// Geometry `Frame` epoch policy: the cached `admits_geometry_epoch` fact
/// built with the CRS info snapshot. Dynamic PJ types and the EPSG:6326
/// identity admit an epoch; ensemble members do not confer it.
pub(crate) fn is_dynamic(value: &str) -> Result<bool> {
    let state = runtime_state_snapshot()?;
    is_dynamic_with_state(value, &state)
}

pub(crate) fn is_dynamic_with_state(value: &str, state: &RuntimeState) -> Result<bool> {
    Ok(info_with_state(value, state)?.admits_geometry_epoch)
}

pub(crate) fn factors(
    target: &str,
    longitude: f64,
    latitude: f64,
    radians: bool,
) -> Result<ProjectionFactors> {
    let state = runtime_state_snapshot()?;
    factors_at(target, longitude, latitude, radians, &state)
}

pub(crate) fn factors_at(
    target: &str,
    longitude: f64,
    latitude: f64,
    radians: bool,
    state: &RuntimeState,
) -> Result<ProjectionFactors> {
    let mut factors = factors_batch_with_state(
        target,
        &[longitude],
        &[latitude],
        AngularUnit::from_radians(radians),
        state,
    )?;
    Ok(factors.remove(0))
}

fn factors_batch_with_state(
    target: &str,
    longitudes: &[f64],
    latitudes: &[f64],
    unit: AngularUnit,
    state: &RuntimeState,
) -> Result<Vec<ProjectionFactors>> {
    ensure_thread_caches_current(state);
    let target = normalize(target)?;
    CRS_FACTOR_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_FACTOR_CACHE_CAPACITY,
            |item| item.crs == target,
            || projection_factors_object(&target, state),
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
    let state = runtime_state_snapshot()?;
    factor_columns_with_state(target, longitudes, latitudes, radians, &state)
}

fn factor_columns_with_state(
    target: &str,
    longitudes: &[f64],
    latitudes: &[f64],
    radians: bool,
    state: &RuntimeState,
) -> Result<ProjectionFactorColumns> {
    let unit = AngularUnit::from_radians(radians);
    ensure_thread_caches_current(state);
    let target = normalize(target)?;
    CRS_FACTOR_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_FACTOR_CACHE_CAPACITY,
            |item| item.crs == target,
            || projection_factors_object(&target, state),
        )?;
        cache[index]
            .object
            .factor_columns(longitudes, latitudes, unit)
    })
}

fn projection_factors_object(
    target: &str,
    state: &RuntimeState,
) -> Result<CachedProjectionFactorsObject> {
    let target_definition = cstring(target)?;
    let context = ProjContext::new_with_state(state)
        .map_err(|error| CrsError::crs_create(target, error.to_string()))?;
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
pub(crate) fn operation_info_with_state(
    source: &str,
    target: &str,
    options: &TransformOptions,
    state: &RuntimeState,
) -> Result<OperationInfo> {
    let (source, target) = normalize_pair(source, target)?;
    let source_info = source.clone();
    let target_info = target.clone();
    let execution = TransformExecution::default();
    with_proj_pipeline_at(&source, &target, options, state, &execution, |pipeline| {
        let info = pipeline.operation_info(
            source_info,
            target_info,
            options.source_epoch,
            options.target_epoch,
        );
        execution.record_operation(info.clone());
        Ok(info)
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
    let state = runtime_state_snapshot()?;
    operation_info_at_with_state(source, target, x, y, zt, options, &state)
}

pub(crate) fn operation_info_at_with_state(
    source: &str,
    target: &str,
    x: f64,
    y: f64,
    zt: crate::ZtValues,
    options: &TransformOptions,
    state: &RuntimeState,
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
        return operation_info_with_state(source, target, options, state);
    }
    let (source, target) = normalize_pair(source, target)?;
    let source_info = source.clone();
    let target_info = target.clone();
    let execution = TransformExecution::default();
    with_proj_diagnostic_pipeline_at(&source, &target, options, state, &execution, |pipeline| {
        let info = pipeline.operation_info_at(
            source_info,
            target_info,
            options.source_epoch,
            options.target_epoch,
            x,
            y,
            z,
            t,
            &execution,
        )?;
        Ok(execution.operation().unwrap_or(info))
    })
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

pub(crate) fn engine_info_with_state(state: &RuntimeState) -> Result<EngineInfo> {
    // `proj_info()` returns pointers into PROJ's mutable global metadata
    // storage. Concurrent `proj_info()` rewrites/frees that backing after the
    // call returns (reproduced on free-threaded CPython 3.14t: corrupt search
    // paths and empty version strings). Own every string *immediately* under a
    // process-wide lock, before any other PROJ/context work can run.
    let snapshot = owned_proj_info_snapshot();
    with_proj_context_at(state, |context| {
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
            paths: state.config.search_paths.clone().unwrap_or_default(),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crs::RUNTIME_STATE_TEST_LOCK;
    use crate::geographic::Latitude;

    fn assert_close(actual: f64, expected: f64, tolerance: f64) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "{actual} != {expected}"
        );
    }

    fn ulp_distance(left: f64, right: f64) -> u64 {
        left.to_bits().abs_diff(right.to_bits())
    }

    #[test]
    fn geographic_frames_follow_axis_roles_units_and_prime_meridian() {
        let epsg = info("EPSG:4326").unwrap();
        let crs84 = info("OGC:CRS84").unwrap();
        let epsg_frame = epsg.geographic_frame().unwrap();
        let crs84_frame = crs84.geographic_frame().unwrap();
        assert_eq!(epsg.axis_order, ["lat", "lon"]);
        assert_eq!(crs84.axis_order, ["lon", "lat"]);
        assert_eq!(epsg_frame, crs84_frame);
        assert_eq!(epsg_frame.longitude().period(), 360.0);
        assert_eq!(epsg_frame.pole(), 90.0);

        // EPSG:4807 is NTF (Paris): both axes use grads while its prime
        // meridian is independently encoded as 2.5969213 grads.
        let paris = info("EPSG:4807").unwrap();
        let frame = paris.geographic_frame().unwrap();
        let grad_to_radian = paris.axes[0].unit_conversion_factor;
        let prime = paris.prime_meridian.as_ref().unwrap();
        assert_close(
            frame.longitude().period(),
            400.0,
            8.0 * f64::EPSILON * 400.0,
        );
        assert_close(frame.pole(), 100.0, 8.0 * f64::EPSILON * 100.0);
        assert_eq!(frame.longitude().unit().radians_per_unit(), grad_to_radian);
        assert_eq!(frame.latitude_unit().radians_per_unit(), grad_to_radian);
        assert_eq!(
            frame.prime_meridian_radians(),
            prime.longitude * prime.unit_conversion_factor
        );
        assert_eq!(frame.longitude().canonicalize(400.0).unwrap().get(), 0.0);
        // Independent worked conversion: 2.5969213 grad * pi/200.
        assert_close(
            frame.prime_meridian_radians(),
            2.596_921_3 * std::f64::consts::PI / 200.0,
            2.0 * f64::EPSILON,
        );
    }

    #[test]
    fn geographic_frames_cover_compound_and_exclude_nonperiodic_crs() {
        let horizontal = info("EPSG:4326").unwrap().geographic_frame().unwrap();
        assert_eq!(
            info("EPSG:9707").unwrap().geographic_frame(),
            Some(horizontal)
        );
        for crs in ["EPSG:3857", "EPSG:4978"] {
            assert_eq!(info(crs).unwrap().geographic_frame(), None, "{crs}");
        }
        let bound =
            info("+proj=longlat +ellps=clrk80ign +pm=paris +towgs84=0,0,0 +type=crs").unwrap();
        assert_ne!(
            bound.geographic_frame().unwrap().prime_meridian_radians(),
            0.0
        );
    }

    #[test]
    fn geographic_frame_is_retained_by_the_info_cache() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let first = info("EPSG:4807").unwrap();
        let second = info("EPSG:4807").unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &second));
        assert_eq!(first.geographic_frame(), second.geographic_frame());

        let current = runtime_state_snapshot().unwrap();
        let next_generation = RuntimeState {
            generation: current.generation.checked_add(1).unwrap(),
            config: std::sync::Arc::clone(&current.config),
        };
        let refreshed = info_with_state("EPSG:4807", &next_generation).unwrap();
        assert!(!std::sync::Arc::ptr_eq(&first, &refreshed));
        assert_eq!(first.geographic_frame(), refreshed.geographic_frame());
    }

    #[test]
    fn geographic_frame_reads_axis_factors_independently() {
        let wkt = r#"GEOGCRS["Mixed angular units",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],PRIMEM["Paris",2.5969213,ANGLEUNIT["grad",0.015707963267948967]],CS[ellipsoidal,2],AXIS["latitude",north,ORDER[1],ANGLEUNIT["grad",0.015707963267948967]],AXIS["longitude",east,ORDER[2],ANGLEUNIT["radian",1]]]"#;
        let metadata = info(wkt).unwrap();
        let frame = metadata.geographic_frame().unwrap();
        assert_eq!(frame.longitude().unit().radians_per_unit(), 1.0);
        assert_eq!(
            frame.latitude_unit().radians_per_unit(),
            0.015_707_963_267_948_967
        );
        assert_eq!(frame.longitude().period(), std::f64::consts::TAU);
        assert_close(frame.pole(), 100.0, 4.0 * f64::EPSILON * 100.0);
    }

    #[test]
    fn malformed_geographic_axes_are_an_error_not_nongeographic() {
        let wkt = r#"GEOGCRS["No longitude",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.017453292519943295]],CS[ellipsoidal,2],AXIS["latitude one",north,ORDER[1],ANGLEUNIT["degree",0.017453292519943295]],AXIS["latitude two",north,ORDER[2],ANGLEUNIT["degree",0.017453292519943295]]]"#;
        let error = info(wkt).unwrap_err().to_string();
        assert!(
            error.contains("duplicate horizontal angular axes")
                || error.contains("longitude and latitude angular axes")
                || error.contains("cannot create CRS"),
            "{error}",
        );
    }

    #[test]
    fn metadata_pole_round_trips_require_one_exterior_ulp_allowance() {
        let radian_wkt = r#"GEOGCRS["Radian text round trip",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0,ANGLEUNIT["radian",1]],CS[ellipsoidal,2],AXIS["latitude",north,ORDER[1],ANGLEUNIT["radian",1]],AXIS["longitude",east,ORDER[2],ANGLEUNIT["radian",1]]]"#;
        let grad_wkt = r#"GEOGCRS["Grad text round trip",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],PRIMEM["Paris",2.5969213,ANGLEUNIT["grad",0.015707963267948967]],CS[ellipsoidal,2],AXIS["latitude",north,ORDER[1],ANGLEUNIT["grad",0.015707963267948967]],AXIS["longitude",east,ORDER[2],ANGLEUNIT["grad",0.015707963267948967]]]"#;
        let cases = [
            ("degree database", "EPSG:4326", 90.0, 0),
            ("radian text", radian_wkt, std::f64::consts::FRAC_PI_2, 0),
            ("grad Paris database", "EPSG:4807", 100.0, 7),
            ("grad Paris text", grad_wkt, 100.0, 1),
        ];
        for (name, crs, nominal, expected_interior_distance) in cases {
            let pole = info(crs).unwrap().geographic_frame().unwrap().pole();
            assert_eq!(
                ulp_distance(pole, nominal),
                expected_interior_distance,
                "{name}"
            );
            assert_eq!(
                ulp_distance(-pole, -nominal),
                expected_interior_distance,
                "{name}"
            );
            let exterior_distance = if nominal > pole {
                ulp_distance(pole, nominal)
            } else {
                0
            };
            assert!(exterior_distance <= 1, "{name} exceeded the one-ULP policy");
            assert_eq!(
                Latitude::try_new(info(crs).unwrap().geographic_frame().unwrap(), nominal)
                    .unwrap()
                    .get(),
                if exterior_distance == 0 {
                    nominal
                } else {
                    pole
                },
                "{name}",
            );
            let negative_exterior_distance = if -nominal < -pole {
                ulp_distance(-pole, -nominal)
            } else {
                0
            };
            assert!(
                negative_exterior_distance <= 1,
                "{name} exceeded the negative one-ULP policy"
            );
            assert_eq!(
                Latitude::try_new(info(crs).unwrap().geographic_frame().unwrap(), -nominal)
                    .unwrap()
                    .get(),
                if negative_exterior_distance == 0 {
                    -nominal
                } else {
                    -pole
                },
                "{name}",
            );
        }
    }

    /// Geometry epoch policy: WGS84's 6326 identity and dynamic frames admit
    /// an epoch; a static frame and a merely dynamic-member ensemble do not.
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
