//! Thread-local CRS cache machinery — the cached-entry types, the
//! move-to-back LRU caches with their capacities, generation-based
//! invalidation, `cache_info`, and the `with_proj_pipeline`/
//! `with_proj_operation` cached accessors. Reached via `use super::*`.

use smol_str::SmolStr;

use crate::crs::geodesic::AuthalicSeries;
use crate::crs::{
    AuthorityObjectInfo, CacheBucketInfo, CacheInfo, CelestialBodyInfo, Confidence, CrsCatalogInfo,
    CrsCatalogOptions, CrsComparison, CrsError, CrsInfo, CrsNameMatchOptions, CrsProjJsonOptions,
    CrsProjOptions, CrsWktOptions, Geodesic, IdentifyCandidate, OperationInfo, ProjObject,
    ProjPipeline, ProjStringVersion, RefCell, RuntimeState, TransformExecution, TransformFailure,
    TransformOptions, UnitInfo, WktVersion, preserve_resource_error,
};
#[cfg(test)]
use crate::crs::{runtime, runtime_state_snapshot};
use crate::error::Result;

thread_local! {
    static TRANSFORM_OBSERVATION: std::cell::Cell<(Option<&'static str>, usize)> = const {
        std::cell::Cell::new((None, 0))
    };
}

pub(super) fn record_transform_engine(in_core: bool) {
    TRANSFORM_OBSERVATION.with(|observation| {
        let (_, invocations) = observation.get();
        observation.set((
            Some(if in_core { "in_core" } else { "proj" }),
            invocations.saturating_add(1),
        ));
    });
}

pub(super) fn begin_transform_observation() {
    TRANSFORM_OBSERVATION.with(|observation| {
        let (_, invocations) = observation.get();
        observation.set((None, invocations));
    });
}

pub(super) struct CachedProjPipeline {
    pub source: SmolStr,
    pub target: SmolStr,
    pub options: TransformOptions,
    pub pipeline: ProjPipeline,
}

pub(super) struct CachedProjOperation {
    pub definition: SmolStr,
    pub operation: ProjPipeline,
}

pub(super) struct CachedCrsInfo {
    pub definition: String,
    /// `Arc` so [`info`] hands out cheap refcount clones — a `CrsInfo` is a
    /// heavy immutable struct (datum, axes, authority) and was being DEEP
    /// cloned on every lookup (~half a geodesic-distance batch's profile).
    pub info: std::sync::Arc<CrsInfo>,
}

pub(super) struct CachedCrsCatalog {
    pub authority: Option<String>,
    pub options: CrsCatalogOptions,
    pub items: Vec<CrsCatalogInfo>,
}

pub(super) struct CachedUnits {
    pub authority: String,
    pub category: Option<String>,
    pub allow_deprecated: bool,
    /// Shared so warm hits clone an `Arc`, not the owned unit list.
    pub items: std::sync::Arc<[UnitInfo]>,
}

pub(super) struct CachedCelestialBodies {
    pub authority: Option<String>,
    /// Shared so warm hits clone an `Arc`, not the owned body list.
    pub items: std::sync::Arc<[CelestialBodyInfo]>,
}

pub(super) struct CachedAuthorityObjects {
    pub definition: SmolStr,
    pub items: Vec<AuthorityObjectInfo>,
}

pub(super) struct CachedCrsAuthorityMatches {
    pub definition: SmolStr,
    pub authority: Option<String>,
    pub min_confidence: Confidence,
    pub items: Vec<IdentifyCandidate>,
}

pub(super) struct CachedCrsNameMatch {
    pub name: String,
    pub options: CrsNameMatchOptions,
    pub items: Vec<String>,
}

pub(super) struct CachedCrsOperations {
    pub source: SmolStr,
    pub target: SmolStr,
    pub options: TransformOptions,
    pub items: Vec<OperationInfo>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct CrsComparisonKey {
    /// Ordered first CRS definition (`min(left, right)` by `str` order).
    pub left: SmolStr,
    /// Ordered second CRS definition (`max(left, right)` by `str` order).
    pub right: SmolStr,
    /// Gometry comparison mode — not a raw PROJ criterion — so normalize+compare
    /// and strict paths cannot collide under the same cache key.
    pub mode: CrsComparison,
}

pub(super) struct CachedCrsComparison {
    pub key: CrsComparisonKey,
    pub value: bool,
}

pub(super) struct CachedProjectionFactorsObject {
    pub definition: SmolStr,
    pub object: ProjObject,
}

pub(super) struct CachedGeodesicObject {
    pub definition: String,
    pub object: Geodesic,
    pub authalic_series: AuthalicSeries,
}

pub(in crate::crs) struct CachedRhumbEllipsoid {
    pub definition: SmolStr,
    pub ellipsoid: super::rhumb::RhumbEllipsoid,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CrsExportKey {
    pub definition: SmolStr,
    pub kind: CrsExportKind,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum CrsExportKind {
    Wkt {
        version: WktVersion,
        options: CrsWktOptions,
    },
    Proj {
        version: ProjStringVersion,
        options: CrsProjOptions,
    },
    ProjJson {
        options: CrsProjJsonOptions,
    },
    To2d {
        name: Option<String>,
    },
    To3d {
        name: Option<String>,
    },
}

pub(super) struct CachedCrsExport {
    pub key: CrsExportKey,
    pub value: String,
}

/// Declare one thread-local CRS LRU cache and register it for clear/report.
///
/// `reported` caches surface in [`cache_info`]; `internal` caches are cleared
/// on generation bumps but omitted from the public bucket list.
macro_rules! crs_cache {
    (
        @expand
        thread_local: { $($thread_local:tt)* }
        capacities: { $($capacities:tt)* }
        clear: { $($clear:tt)* }
        buckets: [ $($buckets:tt)* ]
        rest: []
    ) => {
        thread_local! {
            $($thread_local)*
            pub static CRS_CACHE_GENERATION: RefCell<u64> = const { RefCell::new(0) };
        }
        $($capacities)*
        pub(crate) fn clear_thread_caches() {
            $($clear)*
            TRANSFORM_OBSERVATION.with(|observation| observation.set((None, 0)));
        }
        fn crs_reported_cache_buckets() -> Vec<CacheBucketInfo> {
            vec![ $($buckets)* ]
        }
    };
    (
        @expand
        thread_local: { $($thread_local:tt)* }
        capacities: { $($capacities:tt)* }
        clear: { $($clear:tt)* }
        buckets: [ $($buckets:tt)* ]
        rest: [
            reported $name:ident, $cap_name:ident, $ty:ty, $capacity:literal, $label:literal;
            $($rest:tt)*
        ]
    ) => {
        crs_cache! {
            @expand
            thread_local: {
                $($thread_local)*
                pub static $name: RefCell<Vec<$ty>> = const { RefCell::new(Vec::new()) };
            }
            capacities: {
                $($capacities)*
                pub(crate) const $cap_name: usize = $capacity;
            }
            clear: {
                $($clear)*
                $name.with(|cache| cache.borrow_mut().clear());
            }
            buckets: [
                $($buckets)*
                CacheBucketInfo {
                    name: $label,
                    entries: $name.with(|cache| cache.borrow().len()),
                    capacity: $cap_name,
                },
            ]
            rest: [ $($rest)* ]
        }
    };
    (
        @expand
        thread_local: { $($thread_local:tt)* }
        capacities: { $($capacities:tt)* }
        clear: { $($clear:tt)* }
        buckets: [ $($buckets:tt)* ]
        rest: [
            internal $name:ident, $cap_name:ident, $ty:ty, $capacity:literal;
            $($rest:tt)*
        ]
    ) => {
        crs_cache! {
            @expand
            thread_local: {
                $($thread_local)*
                pub static $name: RefCell<Vec<$ty>> = const { RefCell::new(Vec::new()) };
            }
            capacities: {
                $($capacities)*
                pub(crate) const $cap_name: usize = $capacity;
            }
            clear: {
                $($clear)*
                $name.with(|cache| cache.borrow_mut().clear());
            }
            buckets: [ $($buckets)* ]
            rest: [ $($rest)* ]
        }
    };
    ( $( $decl:tt )+ ) => {
        crs_cache! {
            @expand
            thread_local: {}
            capacities: {}
            clear: {}
            buckets: []
            rest: [ $($decl)+ ]
        }
    };
}

crs_cache! {
    reported PROJ_CACHE, PROJ_CACHE_CAPACITY, CachedProjPipeline, 256, "proj_pipeline";
    reported PROJ_DIAGNOSTIC_CACHE, PROJ_DIAGNOSTIC_CACHE_CAPACITY, CachedProjPipeline, 256, "proj_diagnostic_pipeline";
    reported PROJ_OPERATION_CACHE, PROJ_OPERATION_CACHE_CAPACITY, CachedProjOperation, 16, "proj_operation";
    reported CRS_INFO_CACHE, CRS_INFO_CACHE_CAPACITY, CachedCrsInfo, 256, "crs_info";
    reported CRS_CATALOG_CACHE, CRS_CATALOG_CACHE_CAPACITY, CachedCrsCatalog, 8, "crs_catalog";
    reported CRS_UNITS_CACHE, CRS_UNITS_CACHE_CAPACITY, CachedUnits, 16, "crs_units";
    reported CRS_CELESTIAL_BODIES_CACHE, CRS_CELESTIAL_BODIES_CACHE_CAPACITY, CachedCelestialBodies, 8, "crs_celestial_bodies";
    reported NON_DEPRECATED_CACHE, NON_DEPRECATED_CACHE_CAPACITY, CachedAuthorityObjects, 16, "crs_non_deprecated";
    reported CRS_AUTHORITY_MATCH_CACHE, CRS_AUTHORITY_MATCH_CACHE_CAPACITY, CachedCrsAuthorityMatches, 32, "crs_authority_matches";
    reported CRS_NAME_MATCH_CACHE, CRS_NAME_MATCH_CACHE_CAPACITY, CachedCrsNameMatch, 16, "crs_catalog_name_matches";
    reported CRS_OPERATIONS_CACHE, CRS_OPERATIONS_CACHE_CAPACITY, CachedCrsOperations, 256, "crs_operations";
    reported CRS_COMPARISON_CACHE, CRS_COMPARISON_CACHE_CAPACITY, CachedCrsComparison, 32, "crs_comparison";
    reported CRS_FACTOR_CACHE, CRS_FACTOR_CACHE_CAPACITY, CachedProjectionFactorsObject, 256, "crs_factors";
    reported CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY, CachedGeodesicObject, 256, "crs_geodesic";
    internal CRS_RHUMB_CACHE, CRS_RHUMB_CACHE_CAPACITY, CachedRhumbEllipsoid, 256;
    internal CRS_IN_CORE_DESCRIPTOR_CACHE, CRS_IN_CORE_DESCRIPTOR_CACHE_CAPACITY, super::in_core::CachedInCoreDescriptor, 256;
    reported CRS_EXPORT_CACHE, CRS_EXPORT_CACHE_CAPACITY, CachedCrsExport, 32, "crs_export";
}

pub(super) fn ensure_thread_caches_current(state: &RuntimeState) {
    let current = state.generation;
    CRS_CACHE_GENERATION.with(|generation| {
        let mut generation = generation.borrow_mut();
        if *generation != current {
            clear_thread_caches();
            *generation = current;
        }
    });
}

pub(crate) fn thread_caches_are_current(state: &RuntimeState) -> bool {
    CRS_CACHE_GENERATION.with(|generation| *generation.borrow() == state.generation)
}

#[cfg(test)]
pub(crate) fn cache_info() -> CacheInfo {
    let state = runtime_state_snapshot().unwrap_or_else(|_| {
        runtime::runtime_state_lock()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    });
    cache_info_with_state(&state)
}

pub(crate) fn cache_info_with_state(state: &RuntimeState) -> CacheInfo {
    ensure_thread_caches_current(state);
    let buckets = crs_reported_cache_buckets();
    let (last_transform_engine, transform_invocations) =
        TRANSFORM_OBSERVATION.with(std::cell::Cell::get);
    CacheInfo {
        generation: state.generation,
        total_entries: buckets.iter().map(|bucket| bucket.entries).sum(),
        total_capacity: buckets.iter().map(|bucket| bucket.capacity).sum(),
        buckets,
        last_transform_engine,
        transform_invocations,
    }
}

/// Move a matching entry to the LRU tail and report whether it was present.
pub(crate) fn lru_promote<E>(cache: &mut Vec<E>, matches: impl Fn(&E) -> bool) -> bool {
    let Some(index) = cache.iter().position(matches) else {
        return false;
    };
    if index.wrapping_add(1) != cache.len() {
        let entry = cache.remove(index);
        cache.push(entry);
    }
    true
}

/// Insert a new entry at the MRU tail, evicting the least-recently-used entry
/// when the cache is full.
pub(crate) fn lru_insert<E>(cache: &mut Vec<E>, capacity: usize, entry: E) -> &E {
    if cache.len() == capacity {
        cache.remove(0);
    }
    cache.push(entry);
    cache.last().expect("an inserted LRU entry is the MRU tail")
}

/// Resolve an entry in a thread-local LRU `Vec`, returning the MRU entry.
///
/// The match closure borrows the key, so the hit path clones no key. CRS caches
/// whose miss construction consults another cache can use `lru_promote` and
/// `lru_insert` around that work without holding this cache's borrow.
pub(crate) fn lru_resolve<E>(
    cache: &mut Vec<E>,
    capacity: usize,
    matches: impl Fn(&E) -> bool,
    make: impl FnOnce() -> Result<E>,
) -> Result<&E> {
    if lru_promote(cache, matches) {
        return Ok(cache.last().expect("a promoted LRU entry is the MRU tail"));
    }
    let entry = make()?;
    Ok(lru_insert(cache, capacity, entry))
}

pub(super) fn with_proj_pipeline_at<R>(
    source: &str,
    target: &str,
    options: &TransformOptions,
    state: &RuntimeState,
    execution: &TransformExecution,
    transform: impl FnOnce(&ProjPipeline) -> Result<R>,
) -> Result<R> {
    with_proj_pipeline_indexed_at(source, target, options, state, execution, |pipeline| {
        transform(pipeline).map_err(TransformFailure::from)
    })
    .map_err(|failure| failure.error)
}

pub(super) fn with_proj_pipeline_indexed_at<R>(
    source: &str,
    target: &str,
    options: &TransformOptions,
    state: &RuntimeState,
    execution: &TransformExecution,
    transform: impl FnOnce(&ProjPipeline) -> std::result::Result<R, TransformFailure>,
) -> std::result::Result<R, TransformFailure> {
    ensure_thread_caches_current(state);
    PROJ_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let (diagnostics_before, result) = {
            let pipeline = lru_resolve(
                &mut cache,
                PROJ_CACHE_CAPACITY,
                |item| item.source == source && item.target == target && item.options == *options,
                || {
                    options.validate()?;
                    Ok(CachedProjPipeline {
                        source: source.into(),
                        target: target.into(),
                        options: options.clone(),
                        pipeline: ProjPipeline::new_with_state(source, target, options, state)?,
                    })
                },
            )?;
            let diagnostics_before = execution.diagnostic_count();
            let result = transform(&pipeline.pipeline);
            (diagnostics_before, result)
        };
        if execution.diagnostic_count() > diagnostics_before {
            // PROJ disables this diagnostic after the first attempted
            // coordinate on a PJ. Retaining that PJ would make a later call in
            // another region repeat the first grid name. Evict only degraded
            // pipelines so the next call gets a coordinate-correct selection;
            // a batch still makes one FFI call and one failed attempt total.
            cache.pop().expect("the degraded pipeline is the MRU tail");
            result.map_err(|failure| {
                let message = execution.diagnostic().unwrap_or_default();
                let error = if message.is_empty() {
                    preserve_resource_error(failure.error, |error| {
                        CrsError::transform(source, target, error.to_string())
                    })
                } else {
                    preserve_resource_error(failure.error, |error| {
                        CrsError::transform(source, target, format!("{error}; {message}"))
                    })
                };
                TransformFailure {
                    error,
                    coordinate: failure.coordinate,
                }
            })
        } else {
            result.map_err(|failure| {
                let coordinate = failure.coordinate;
                let error = preserve_resource_error(failure.error, |error| {
                    CrsError::transform(source, target, error.to_string())
                });
                TransformFailure { error, coordinate }
            })
        }
    })
}

/// Resolve the cached PROJ pipeline used only by diagnostic observers
/// (`operation_at` and round-trip error reporting). Keeping it separate from
/// the execution cache prevents diagnostic calls from evicting hot transforms,
/// while one resolver guarantees identical keying, validation, and errors for
/// every diagnostic surface.
#[cfg(test)]
pub(super) fn with_proj_diagnostic_pipeline<R>(
    source: &str,
    target: &str,
    options: &TransformOptions,
    inspect: impl FnOnce(&ProjPipeline) -> Result<R>,
) -> Result<R> {
    let state = runtime_state_snapshot()?;
    let execution = TransformExecution::default();
    with_proj_diagnostic_pipeline_at(source, target, options, &state, &execution, inspect)
}

pub(super) fn with_proj_diagnostic_pipeline_at<R>(
    source: &str,
    target: &str,
    options: &TransformOptions,
    state: &RuntimeState,
    execution: &TransformExecution,
    inspect: impl FnOnce(&ProjPipeline) -> Result<R>,
) -> Result<R> {
    ensure_thread_caches_current(state);
    options.validate()?;
    PROJ_DIAGNOSTIC_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let (diagnostics_before, result) = {
            let pipeline = lru_resolve(
                &mut cache,
                PROJ_DIAGNOSTIC_CACHE_CAPACITY,
                |item| item.source == source && item.target == target && item.options == *options,
                || {
                    Ok(CachedProjPipeline {
                        source: source.into(),
                        target: target.into(),
                        options: options.clone(),
                        pipeline: ProjPipeline::new_with_state(source, target, options, state)?,
                    })
                },
            )?;
            let diagnostics_before = execution.diagnostic_count();
            let result = inspect(&pipeline.pipeline);
            (diagnostics_before, result)
        };
        if execution.diagnostic_count() > diagnostics_before {
            cache
                .pop()
                .expect("the diagnostic pipeline is the MRU tail");
        }
        result.map_err(|error| {
            preserve_resource_error(error, |error| {
                CrsError::transform(source, target, error.to_string())
            })
        })
    })
}

pub(super) fn with_proj_operation_at<R>(
    definition: &str,
    state: &RuntimeState,
    execution: &TransformExecution,
    transform: impl FnOnce(&ProjPipeline) -> Result<R>,
) -> Result<R> {
    ensure_thread_caches_current(state);
    PROJ_OPERATION_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let (diagnostics_before, result) = {
            let operation = lru_resolve(
                &mut cache,
                PROJ_OPERATION_CACHE_CAPACITY,
                |item| item.definition == definition,
                || {
                    Ok(CachedProjOperation {
                        definition: definition.into(),
                        operation: ProjPipeline::from_definition_with_state(definition, state)?,
                    })
                },
            )?;
            let diagnostics_before = execution.diagnostic_count();
            let result = transform(&operation.operation);
            (diagnostics_before, result)
        };
        if execution.diagnostic_count() > diagnostics_before {
            cache.pop().expect("the degraded operation is the MRU tail");
        }
        result.map_err(|error| {
            preserve_resource_error(error, |error| {
                CrsError::transform("coordinates", definition, error.to_string())
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;
    use crate::crs::catalog::{
        authority_matches, catalog_name_matches_with_state, catalog_with_state,
    };
    use crate::crs::geodesic::with_geodesic_at;
    use crate::crs::operations::operations_info;
    use crate::crs::{
        ConfigUpdate, CrsObjectKind, ProjDirection, RUNTIME_STATE_TEST_LOCK, TransformPlan,
        WGS84_A, apply_operation, clear_cache, configure_runtime, distance_rhumb_crs, factors,
        info, non_deprecated, operation_info_at, reset_runtime_config, same, to_wkt_with_options,
        transform_coordinates,
    };

    fn resolve_geodesic(crs: &str) {
        let state = runtime_state_snapshot().unwrap();
        with_geodesic_at(crs, &state, |_| Ok(())).unwrap();
    }

    fn transform_known_point(
        pipeline: &ProjPipeline,
        execution: &TransformExecution,
    ) -> (f64, f64) {
        let mut x = [1.0];
        let mut y = [2.0];
        pipeline
            .transform_lanes(
                &mut x,
                &mut y,
                None,
                None,
                ProjDirection::Forward,
                execution,
            )
            .unwrap();
        (x[0], y[0])
    }

    fn thread_cache_lengths() -> [usize; 17] {
        [
            PROJ_CACHE.with(|cache| cache.borrow().len()),
            PROJ_DIAGNOSTIC_CACHE.with(|cache| cache.borrow().len()),
            PROJ_OPERATION_CACHE.with(|cache| cache.borrow().len()),
            CRS_INFO_CACHE.with(|cache| cache.borrow().len()),
            CRS_CATALOG_CACHE.with(|cache| cache.borrow().len()),
            CRS_UNITS_CACHE.with(|cache| cache.borrow().len()),
            CRS_CELESTIAL_BODIES_CACHE.with(|cache| cache.borrow().len()),
            NON_DEPRECATED_CACHE.with(|cache| cache.borrow().len()),
            CRS_AUTHORITY_MATCH_CACHE.with(|cache| cache.borrow().len()),
            CRS_NAME_MATCH_CACHE.with(|cache| cache.borrow().len()),
            CRS_OPERATIONS_CACHE.with(|cache| cache.borrow().len()),
            CRS_COMPARISON_CACHE.with(|cache| cache.borrow().len()),
            CRS_FACTOR_CACHE.with(|cache| cache.borrow().len()),
            CRS_GEODESIC_CACHE.with(|cache| cache.borrow().len()),
            CRS_RHUMB_CACHE.with(|cache| cache.borrow().len()),
            CRS_IN_CORE_DESCRIPTOR_CACHE.with(|cache| cache.borrow().len()),
            CRS_EXPORT_CACHE.with(|cache| cache.borrow().len()),
        ]
    }

    fn assert_thread_caches_are_empty() {
        assert_eq!(thread_cache_lengths(), [0; 17]);
    }

    fn assert_some_thread_cache_is_warm() {
        assert!(thread_cache_lengths().iter().any(|length| *length > 0));
    }

    fn warm_representative_thread_caches() {
        let options = TransformOptions::default();
        let state = runtime_state_snapshot().unwrap();
        let catalog_options = CrsCatalogOptions {
            kind: Some(CrsObjectKind::Geographic2dCrs),
            area: None,
            contains_area: false,
            allow_deprecated: false,
            celestial_body: Some("Earth".to_owned()),
        };
        let name_match_options = CrsNameMatchOptions {
            authority: Some("EPSG".to_owned()),
            kind: Some(CrsObjectKind::Geographic2dCrs),
            approximate: false,
        };

        info("EPSG:4326").unwrap();
        catalog_with_state(Some("EPSG"), &catalog_options, &state).unwrap();
        non_deprecated("EPSG:4326").unwrap();
        authority_matches(
            "EPSG:4326",
            Some("EPSG"),
            Confidence::try_new("min_confidence", 70).unwrap(),
        )
        .unwrap();
        catalog_name_matches_with_state("WGS 84", &name_match_options, &state).unwrap();
        operations_info("EPSG:4267", "EPSG:4326", &options).unwrap();
        same("EPSG:4326", "OGC:CRS84", CrsComparison::IgnoreAxisOrder).unwrap();
        factors("EPSG:3857", -73.0, 41.0, false).unwrap();
        resolve_geodesic("EPSG:4326");
        distance_rhumb_crs("EPSG:4326", -73.0, 41.0, -72.0, 42.0).unwrap();
        to_wkt_with_options(
            "EPSG:4326",
            WktVersion::Wkt2_2019,
            &CrsWktOptions::default(),
        )
        .unwrap();
        operation_info_at(
            "EPSG:4267",
            "EPSG:4326",
            -73.0,
            41.0,
            crate::Zt::None,
            &options,
        )
        .unwrap();

        let mut x = [-73.0];
        let mut y = [41.0];
        transform_coordinates(
            "EPSG:4267",
            "EPSG:4326",
            &mut x,
            &mut y,
            crate::Zt::None,
            options,
        )
        .unwrap();

        let mut x = [1.0];
        let mut y = [2.0];
        apply_operation(
            "+proj=pipeline +step +proj=affine +xoff=1 +yoff=2",
            ProjDirection::Forward,
            &mut x,
            &mut y,
            crate::Zt::None,
        )
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn runtime_config_changes_clear_current_thread_caches() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        reset_runtime_config().unwrap();
        assert_thread_caches_are_empty();

        warm_representative_thread_caches();
        assert_some_thread_cache_is_warm();

        let warm_generation = runtime_state_snapshot().unwrap().generation;
        configure_runtime(ConfigUpdate::Keep, ConfigUpdate::Keep).unwrap();
        assert_eq!(
            runtime_state_snapshot().unwrap().generation,
            warm_generation
        );
        assert_some_thread_cache_is_warm();

        clear_cache();
        assert_thread_caches_are_empty();
        assert_eq!(
            runtime_state_snapshot().unwrap().generation,
            warm_generation.wrapping_add(1)
        );

        warm_representative_thread_caches();
        assert_some_thread_cache_is_warm();

        configure_runtime(
            ConfigUpdate::Set(Some(vec!["/tmp/gometry-crs-old".to_owned()])),
            ConfigUpdate::Keep,
        )
        .unwrap();
        assert_thread_caches_are_empty();

        reset_runtime_config().unwrap();
        assert_thread_caches_are_empty();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn equal_runtime_config_preserves_generation_and_thread_cache_products() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        reset_runtime_config().unwrap();
        info("EPSG:4326").unwrap();
        let before = cache_info();
        assert!(before.total_entries > 0);

        let config = configure_runtime(ConfigUpdate::Keep, ConfigUpdate::Keep).unwrap();
        let after = cache_info();

        assert_eq!(config.search_paths, None);
        assert_eq!(config.user_writable_directory, None);
        assert_eq!(after.generation, before.generation);
        assert_eq!(after.total_entries, before.total_entries);
        assert_some_thread_cache_is_warm();

        reset_runtime_config().unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn live_pipeline_survives_other_thread_runtime_invalidation() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        reset_runtime_config().unwrap();
        let old_state = runtime_state_snapshot().unwrap();
        let options = TransformOptions::default();
        let ready = Arc::new(Barrier::new(2));
        let configured = Arc::new(Barrier::new(2));
        let cleared = Arc::new(Barrier::new(2));
        let reset = Arc::new(Barrier::new(2));
        let new_pipeline_live = Arc::new(Barrier::new(2));

        let worker_ready = Arc::clone(&ready);
        let worker_configured = Arc::clone(&configured);
        let worker_cleared = Arc::clone(&cleared);
        let worker_reset = Arc::clone(&reset);
        let worker_new_pipeline_live = Arc::clone(&new_pipeline_live);
        let worker_options = options.clone();
        let worker_state = Arc::clone(&old_state);
        let worker = std::thread::spawn(move || {
            let execution = TransformExecution::default();
            with_proj_pipeline_at(
                "EPSG:4326",
                "EPSG:3857",
                &worker_options,
                &worker_state,
                &execution,
                |pipeline| {
                    let initial = transform_known_point(pipeline, &execution);
                    worker_ready.wait();

                    worker_configured.wait();
                    let after_configure = transform_known_point(pipeline, &execution);

                    worker_cleared.wait();
                    let after_clear = transform_known_point(pipeline, &execution);

                    worker_reset.wait();
                    worker_new_pipeline_live.wait();
                    let after_reset = transform_known_point(pipeline, &execution);

                    Ok([initial, after_configure, after_clear, after_reset])
                },
            )
            .unwrap()
        });

        ready.wait();
        configure_runtime(
            ConfigUpdate::Set(Some(vec!["/tmp/gometry-crs-live-new".to_owned()])),
            ConfigUpdate::Keep,
        )
        .unwrap();
        configured.wait();

        clear_cache();
        cleared.wait();

        reset_runtime_config().unwrap();
        let new_state = runtime_state_snapshot().unwrap();
        assert_eq!(new_state.generation, old_state.generation.wrapping_add(3));
        reset.wait();

        let new_execution = TransformExecution::default();
        let new_result = with_proj_pipeline_at(
            "EPSG:4326",
            "EPSG:3857",
            &options,
            &new_state,
            &new_execution,
            |pipeline| {
                new_pipeline_live.wait();
                Ok(transform_known_point(pipeline, &new_execution))
            },
        )
        .unwrap();
        let old_results = worker.join().unwrap();
        let expected = (111_319.490_793_273_58, 222_684.208_505_544_55);
        for result in old_results {
            assert!((result.0 - expected.0).abs() < 1e-6);
            assert!((result.1 - expected.1).abs() < 1e-6);
        }
        assert!((new_result.0 - expected.0).abs() < 1e-6);
        assert!((new_result.1 - expected.1).abs() < 1e-6);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn held_runtime_state_survives_mutation_during_context_creation() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        reset_runtime_config().unwrap();
        configure_runtime(
            ConfigUpdate::Set(Some(vec!["/tmp/gometry-crs-old".to_owned()])),
            ConfigUpdate::Keep,
        )
        .unwrap();
        let expected = runtime_state_snapshot().unwrap();
        let entered = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let observed = Arc::new(std::sync::Mutex::new(None));
        let worker_entered = Arc::clone(&entered);
        let worker_resume = Arc::clone(&resume);
        let worker_observed = Arc::clone(&observed);
        let worker = std::thread::spawn(move || {
            super::super::context::install_context_creation_state_hook(
                worker_entered,
                worker_resume,
                worker_observed,
            );
            operations_info("EPSG:4267", "EPSG:4326", &TransformOptions::default())
        });
        entered.wait();
        configure_runtime(
            ConfigUpdate::Set(Some(vec!["/tmp/gometry-crs-new".to_owned()])),
            ConfigUpdate::Keep,
        )
        .unwrap();
        resume.wait();

        worker
            .join()
            .expect("context creation must not panic")
            .unwrap();
        assert_eq!(
            *observed.lock().unwrap(),
            Some((expected.generation, expected.config.search_paths.clone()))
        );
        let current = runtime_state_snapshot().unwrap();
        assert_eq!(current.generation, expected.generation + 1);
        assert_eq!(
            current.config.search_paths.as_deref(),
            Some(["/tmp/gometry-crs-new".to_owned()].as_slice())
        );
        reset_runtime_config().unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn held_runtime_state_survives_mutation_during_catalog_observation() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        reset_runtime_config().unwrap();
        configure_runtime(
            ConfigUpdate::Set(Some(vec!["/tmp/gometry-crs-old".to_owned()])),
            ConfigUpdate::Keep,
        )
        .unwrap();
        let expected = runtime_state_snapshot().unwrap();
        let entered = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let observed = Arc::new(std::sync::Mutex::new(None));
        let worker_entered = Arc::clone(&entered);
        let worker_resume = Arc::clone(&resume);
        let worker_observed = Arc::clone(&observed);
        let worker_expected = Arc::clone(&expected);
        let worker = std::thread::spawn(move || {
            super::super::context::install_context_creation_state_hook(
                worker_entered,
                worker_resume,
                worker_observed,
            );
            let options = CrsCatalogOptions {
                kind: Some(CrsObjectKind::Geographic2dCrs),
                area: None,
                contains_area: false,
                allow_deprecated: false,
                celestial_body: Some("Earth".to_owned()),
            };
            catalog_with_state(Some("EPSG"), &options, &worker_expected)
        });
        entered.wait();
        configure_runtime(
            ConfigUpdate::Set(Some(vec!["/tmp/gometry-crs-new".to_owned()])),
            ConfigUpdate::Keep,
        )
        .unwrap();
        resume.wait();
        worker
            .join()
            .expect("catalog observation must not panic")
            .unwrap();
        assert_eq!(
            *observed.lock().unwrap(),
            Some((expected.generation, expected.config.search_paths.clone()))
        );
        reset_runtime_config().unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn factor_cache_promotes_hits_before_eviction() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        clear_cache();
        let factor_crs =
            |offset: usize| format!("+proj=merc +datum=WGS84 +x_0={offset} +y_0=0 +type=crs");
        for offset in 0..CRS_FACTOR_CACHE_CAPACITY {
            factors(&factor_crs(offset), 0.0, 0.0, false).unwrap();
        }
        let first = factor_crs(0);
        let second = factor_crs(1);
        factors(&first, 0.0, 0.0, false).unwrap();
        factors(&factor_crs(CRS_FACTOR_CACHE_CAPACITY), 0.0, 0.0, false).unwrap();

        CRS_FACTOR_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert_eq!(cache.len(), CRS_FACTOR_CACHE_CAPACITY);
            assert!(cache.iter().any(|item| item.definition == first));
            assert!(!cache.iter().any(|item| item.definition == second));
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn diagnostic_pipeline_resolver_reuses_the_same_key() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        clear_cache();
        let options = TransformOptions::default();
        for _ in 0..2 {
            with_proj_diagnostic_pipeline("EPSG:4326", "EPSG:3857", &options, |_| Ok(())).unwrap();
        }
        PROJ_DIAGNOSTIC_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert_eq!(cache.len(), 1);
            assert_eq!(cache[0].source, "EPSG:4326");
            assert_eq!(cache[0].target, "EPSG:3857");
        });
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn reusable_degraded_plan_reprobes_after_each_diagnostic() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        clear_cache();
        let plan = TransformPlan::new("EPSG:4267", "EPSG:4326").unwrap();
        for (index, (x, y)) in [(-104.0, 40.0), (-100.0, 55.0)].into_iter().enumerate() {
            let mut x = [x];
            let mut y = [y];
            plan.transform_coordinates(&mut x, &mut y, crate::Zt::None)
                .unwrap();
            assert_eq!(plan.execution().diagnostic_count(), index + 1);
            PROJ_CACHE.with(|cache| assert!(cache.borrow().is_empty()));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn unequal_epochs_do_not_select_identity_plan() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let state = runtime_state_snapshot().unwrap();
        let options = TransformOptions {
            source_epoch: Some(2010.0),
            target_epoch: Some(2020.0),
            ..TransformOptions::default()
        };
        let plan =
            TransformPlan::new_with_options_at("EPSG:7789", "EPSG:7789", options, state).unwrap();
        assert!(!plan.is_identity());
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn geodesic_cache_promotes_hits_before_eviction() {
        let _guard = RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        clear_cache();
        let crs_values: Vec<String> = (0..=CRS_GEODESIC_CACHE_CAPACITY)
            .map(|offset| {
                format!(
                    "+proj=longlat +a={} +rf=298.257223563 +type=crs",
                    WGS84_A + offset as f64
                )
            })
            .collect();

        for crs in crs_values.iter().take(CRS_GEODESIC_CACHE_CAPACITY) {
            resolve_geodesic(crs);
        }
        let first = crs_values[0].clone();
        let second = crs_values[1].clone();
        resolve_geodesic(&first);
        resolve_geodesic(&crs_values[CRS_GEODESIC_CACHE_CAPACITY]);

        CRS_GEODESIC_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert_eq!(cache.len(), CRS_GEODESIC_CACHE_CAPACITY);
            assert!(cache.iter().any(|item| item.definition == first));
            assert!(!cache.iter().any(|item| item.definition == second));
        });
    }
}
