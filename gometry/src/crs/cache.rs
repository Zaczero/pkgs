#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Thread-local CRS cache machinery — the cached-entry types, the 14
//! move-to-back LRU caches with their capacities, generation-based
//! invalidation, `cache_info`, and the `with_proj_pipeline`/
//! `with_proj_operation` cached accessors. Reached via `use super::*`.

use super::*;
use crate::error::Result;

pub(super) struct CachedProjPipeline {
    pub source: String,
    pub target: String,
    pub options: TransformOptions,
    pub pipeline: ProjPipeline,
}

pub(super) struct CachedProjOperation {
    pub definition: String,
    pub operation: ProjPipeline,
}

pub(super) struct CachedCrsInfo {
    pub crs: String,
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

pub(super) struct CachedAuthorityObjects {
    pub crs: String,
    pub items: Vec<AuthorityObjectInfo>,
}

pub(super) struct CachedCrsAuthorityMatches {
    pub crs: String,
    pub authority: Option<String>,
    pub min_confidence: Confidence,
    pub items: Vec<IdentifyCandidate>,
}

pub(super) struct CachedCrsSearch {
    pub name: String,
    pub options: CrsSearchOptions,
    pub items: Vec<AuthorityObjectInfo>,
}

pub(super) struct CachedCrsOperations {
    pub source: String,
    pub target: String,
    pub options: TransformOptions,
    pub items: Vec<OperationInfo>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct CrsComparisonKey {
    pub left: String,
    pub right: String,
    pub criterion: proj_sys::PJ_COMPARISON_CRITERION,
}

pub(super) struct CachedCrsComparison {
    pub key: CrsComparisonKey,
    pub value: bool,
}

pub(super) struct CachedProjectionFactorsObject {
    pub crs: String,
    pub object: ProjObject,
}

pub(super) struct CachedGeodesicObject {
    pub crs: String,
    pub object: Geodesic,
}

pub(in crate::crs) struct CachedRhumbEllipsoid {
    pub crs: String,
    pub ellipsoid: super::rhumb::RhumbEllipsoid,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CrsExportKey {
    pub crs: String,
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
    reported NON_DEPRECATED_CACHE, NON_DEPRECATED_CACHE_CAPACITY, CachedAuthorityObjects, 16, "crs_non_deprecated";
    reported CRS_AUTHORITY_MATCH_CACHE, CRS_AUTHORITY_MATCH_CACHE_CAPACITY, CachedCrsAuthorityMatches, 32, "crs_authority_matches";
    reported CRS_SEARCH_CACHE, CRS_SEARCH_CACHE_CAPACITY, CachedCrsSearch, 16, "crs_search";
    reported CRS_OPERATIONS_CACHE, CRS_OPERATIONS_CACHE_CAPACITY, CachedCrsOperations, 256, "crs_operations";
    reported CRS_COMPARISON_CACHE, CRS_COMPARISON_CACHE_CAPACITY, CachedCrsComparison, 32, "crs_comparison";
    reported CRS_FACTOR_CACHE, CRS_FACTOR_CACHE_CAPACITY, CachedProjectionFactorsObject, 256, "crs_factors";
    reported CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY, CachedGeodesicObject, 256, "crs_geodesic";
    internal CRS_RHUMB_CACHE, CRS_RHUMB_CACHE_CAPACITY, CachedRhumbEllipsoid, 256;
    internal CRS_CANONICAL_CACHE, CRS_CANONICAL_CACHE_CAPACITY, (String, smol_str::SmolStr), 256;
    internal CRS_IN_CORE_DESCRIPTOR_CACHE, CRS_IN_CORE_DESCRIPTOR_CACHE_CAPACITY, super::in_core::CachedInCoreDescriptor, 256;
    reported CRS_EXPORT_CACHE, CRS_EXPORT_CACHE_CAPACITY, CachedCrsExport, 32, "crs_export";
}

pub(super) fn ensure_thread_caches_current() {
    let current = runtime_config_generation();
    CRS_CACHE_GENERATION.with(|generation| {
        let mut generation = generation.borrow_mut();
        if *generation != current {
            clear_thread_caches();
            *generation = current;
        }
    });
}

pub(crate) fn cache_info() -> CacheInfo {
    ensure_thread_caches_current();
    let buckets = crs_reported_cache_buckets();
    CacheInfo {
        generation: runtime_config_generation(),
        total_entries: buckets.iter().map(|bucket| bucket.entries).sum(),
        total_capacity: buckets.iter().map(|bucket| bucket.capacity).sum(),
        buckets,
    }
}

/// Resolve an entry in a thread-local LRU `Vec`, returning its index.
///
/// The entry matching `matches` is moved to the back (most-recently-used); on a
/// miss the front entry is evicted when the cache is at `capacity` and `make()`
/// is pushed. The match closure borrows the key, so the hot hit path clones no
/// key — the single source of truth for the move-to-back caching all 14 CRS
/// thread-local caches share (instead of re-implementing the dance per cache).
pub(crate) fn lru_resolve<E>(
    cache: &mut Vec<E>,
    capacity: usize,
    matches: impl Fn(&E) -> bool,
    make: impl FnOnce() -> Result<E>,
) -> Result<usize> {
    if let Some(index) = cache.iter().position(&matches) {
        if index + 1 == cache.len() {
            return Ok(index);
        }
        let entry = cache.remove(index);
        cache.push(entry);
        return Ok(cache.len() - 1);
    }
    if cache.len() == capacity {
        cache.remove(0);
    }
    cache.push(make()?);
    Ok(cache.len() - 1)
}

pub(super) fn with_proj_pipeline<R>(
    source: &str,
    target: &str,
    options: &TransformOptions,
    transform: impl FnOnce(&ProjPipeline) -> Result<R>,
) -> Result<R> {
    ensure_thread_caches_current();
    PROJ_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            PROJ_CACHE_CAPACITY,
            |item| item.source == source && item.target == target && item.options == *options,
            || {
                options.validate()?;
                Ok(CachedProjPipeline {
                    source: source.to_owned(),
                    target: target.to_owned(),
                    options: options.clone(),
                    pipeline: ProjPipeline::new(source, target, options)?,
                })
            },
        )?;
        transform(&cache[index].pipeline)
            .map_err(|error| CrsError::transform(source, target, error.to_string()))
    })
}

/// Resolve the cached PROJ pipeline used only by diagnostic observers
/// (`operation_at` and round-trip error reporting). Keeping it separate from
/// the execution cache prevents diagnostic calls from evicting hot transforms,
/// while one resolver guarantees identical keying, validation, and errors for
/// every diagnostic surface.
pub(super) fn with_proj_diagnostic_pipeline<R>(
    source: &str,
    target: &str,
    options: &TransformOptions,
    inspect: impl FnOnce(&ProjPipeline) -> Result<R>,
) -> Result<R> {
    ensure_thread_caches_current();
    options.validate()?;
    PROJ_DIAGNOSTIC_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            PROJ_DIAGNOSTIC_CACHE_CAPACITY,
            |item| item.source == source && item.target == target && item.options == *options,
            || {
                Ok(CachedProjPipeline {
                    source: source.to_owned(),
                    target: target.to_owned(),
                    options: options.clone(),
                    pipeline: ProjPipeline::new(source, target, options)?,
                })
            },
        )?;
        inspect(&cache[index].pipeline)
            .map_err(|error| CrsError::transform(source, target, error.to_string()))
    })
}

pub(super) fn with_proj_operation<R>(
    definition: &str,
    transform: impl FnOnce(&ProjPipeline) -> Result<R>,
) -> Result<R> {
    ensure_thread_caches_current();
    PROJ_OPERATION_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            PROJ_OPERATION_CACHE_CAPACITY,
            |item| item.definition == definition,
            || {
                Ok(CachedProjOperation {
                    definition: definition.to_owned(),
                    operation: ProjPipeline::from_definition(definition)?,
                })
            },
        )?;
        transform(&cache[index].operation)
            .map_err(|error| CrsError::transform("coordinates", definition, error.to_string()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thread_cache_lengths() -> [usize; 16] {
        [
            PROJ_CACHE.with(|cache| cache.borrow().len()),
            PROJ_DIAGNOSTIC_CACHE.with(|cache| cache.borrow().len()),
            PROJ_OPERATION_CACHE.with(|cache| cache.borrow().len()),
            CRS_INFO_CACHE.with(|cache| cache.borrow().len()),
            CRS_CATALOG_CACHE.with(|cache| cache.borrow().len()),
            NON_DEPRECATED_CACHE.with(|cache| cache.borrow().len()),
            CRS_AUTHORITY_MATCH_CACHE.with(|cache| cache.borrow().len()),
            CRS_SEARCH_CACHE.with(|cache| cache.borrow().len()),
            CRS_OPERATIONS_CACHE.with(|cache| cache.borrow().len()),
            CRS_COMPARISON_CACHE.with(|cache| cache.borrow().len()),
            CRS_FACTOR_CACHE.with(|cache| cache.borrow().len()),
            CRS_GEODESIC_CACHE.with(|cache| cache.borrow().len()),
            CRS_RHUMB_CACHE.with(|cache| cache.borrow().len()),
            CRS_CANONICAL_CACHE.with(|cache| cache.borrow().len()),
            CRS_IN_CORE_DESCRIPTOR_CACHE.with(|cache| cache.borrow().len()),
            CRS_EXPORT_CACHE.with(|cache| cache.borrow().len()),
        ]
    }

    fn assert_thread_caches_are_empty() {
        assert_eq!(thread_cache_lengths(), [0; 16]);
    }

    fn assert_some_thread_cache_is_warm() {
        assert!(thread_cache_lengths().iter().any(|length| *length > 0));
    }

    fn warm_representative_thread_caches() {
        let options = TransformOptions::default();
        let catalog_options = CrsCatalogOptions {
            kind: Some(CrsObjectKind::Geographic2dCrs),
            area: None,
            contains_area: false,
            allow_deprecated: false,
            celestial_body: Some("Earth".to_owned()),
        };
        let search_options = CrsSearchOptions {
            authority: Some("EPSG".to_owned()),
            kind: Some(CrsObjectKind::Geographic2dCrs),
            approximate: false,
            limit: std::num::NonZeroUsize::new(5).unwrap(),
        };

        info("EPSG:4326").unwrap();
        catalog(Some("EPSG"), &catalog_options).unwrap();
        non_deprecated("EPSG:4326").unwrap();
        authority_matches(
            "EPSG:4326",
            Some("EPSG"),
            Confidence::try_new("min_confidence", 70).unwrap(),
        )
        .unwrap();
        search("WGS 84", &search_options).unwrap();
        operations_info("EPSG:4267", "EPSG:4326", &options).unwrap();
        same("EPSG:4326", "OGC:CRS84", CrsComparison::IgnoreAxisOrder).unwrap();
        factors("EPSG:3857", -73.0, 41.0, false).unwrap();
        geodesic_inverse("EPSG:4326", -73.0, 41.0, -72.0, 42.0, None, None, false).unwrap();
        rhumb_distance_crs("EPSG:4326", -73.0, 41.0, -72.0, 42.0).unwrap();
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
    fn runtime_config_changes_clear_current_thread_caches() {
        reset_runtime_config().unwrap();
        assert_thread_caches_are_empty();

        warm_representative_thread_caches();
        assert_some_thread_cache_is_warm();

        configure_runtime(None, None).unwrap();
        assert_thread_caches_are_empty();

        warm_representative_thread_caches();
        assert_some_thread_cache_is_warm();

        reset_runtime_config().unwrap();
        assert_thread_caches_are_empty();
    }

    #[test]
    fn factor_cache_promotes_hits_before_eviction() {
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
            assert!(cache.iter().any(|item| item.crs == first));
            assert!(!cache.iter().any(|item| item.crs == second));
        });
    }

    #[test]
    fn diagnostic_pipeline_resolver_reuses_the_same_key() {
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
    fn geodesic_cache_promotes_hits_before_eviction() {
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
            geodesic_inverse(crs, -73.0, 41.0, -74.0, 42.0, None, None, false).unwrap();
        }
        let first = crs_values[0].clone();
        let second = crs_values[1].clone();
        geodesic_inverse(&first, -73.0, 41.0, -74.0, 42.0, None, None, false).unwrap();
        geodesic_inverse(
            &crs_values[CRS_GEODESIC_CACHE_CAPACITY],
            -73.0,
            41.0,
            -74.0,
            42.0,
            None,
            None,
            false,
        )
        .unwrap();

        CRS_GEODESIC_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert_eq!(cache.len(), CRS_GEODESIC_CACHE_CAPACITY);
            assert!(cache.iter().any(|item| item.crs == first));
            assert!(!cache.iter().any(|item| item.crs == second));
        });
    }

    #[test]
    fn geodesic_batch_kernels_reject_mismatched_lengths() {
        let inverse = geodesic_inverses(
            "EPSG:4326",
            &[-73.0, -72.0],
            &[41.0],
            &[-74.0, -75.0],
            &[42.0, 43.0],
            None,
            None,
            false,
        )
        .unwrap_err();
        assert!(inverse.to_string().contains("must have the same length"));

        let inverse_z = geodesic_inverses(
            "EPSG:4326",
            &[-73.0, -72.0],
            &[41.0, 42.0],
            &[-74.0, -75.0],
            &[42.0, 43.0],
            Some(&[0.0]),
            Some(&[1.0, 2.0]),
            false,
        )
        .unwrap_err();
        assert!(inverse_z.to_string().contains("must have the same length"));

        let direct = geodesic_direct_columns(
            "EPSG:4326",
            &[-73.0, -72.0],
            &[41.0],
            &[45.0],
            &[1_000.0],
            false,
        )
        .unwrap_err();
        assert!(direct.to_string().contains("must have the same length"));

        let interpolate = geodesic_interpolates(
            "EPSG:4326",
            &[-73.0, -72.0],
            &[41.0],
            &[-74.0, -75.0],
            &[42.0, 43.0],
            &[0.5, 0.5],
            true,
            false,
        )
        .unwrap_err();
        assert!(
            interpolate
                .to_string()
                .contains("must have the same length")
        );
    }
}
