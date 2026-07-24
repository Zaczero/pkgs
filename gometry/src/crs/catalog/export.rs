#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::crs::*;
use crate::error::Result;

pub(crate) fn proj_operations() -> Vec<ProjOperationCatalogInfo> {
    PROJ_OPERATION_CATALOG
        .get_or_init(proj_operations_uncached)
        .clone()
}

fn proj_operations_uncached() -> Vec<ProjOperationCatalogInfo> {
    proj_static_list(
        || {
            // SAFETY: PROJ returns a process-static null-terminated array.
            unsafe { proj_sys::proj_list_operations() }
        },
        |info| {
            let id = string_from_ptr(info.id)?;
            Some(ProjOperationCatalogInfo {
                id,
                description: first_static_string_from_ptr(info.descr),
            })
        },
    )
}

fn proj_static_list<T, R>(
    list_fn: impl FnOnce() -> *const T,
    mut map_info: impl FnMut(&T) -> Option<R>,
) -> Vec<R> {
    let mut items = Vec::new();
    let mut current = list_fn();
    while !current.is_null() {
        // SAFETY: current is within PROJ's process-static null-terminated
        // array. Each mapped field is copied before returning to Rust/Python.
        let info = unsafe { &*current };
        let Some(item) = map_info(info) else {
            break;
        };
        items.push(item);
        // SAFETY: advancing one element within PROJ's null-terminated array.
        current = unsafe { current.add(1) };
    }
    items
}

pub(crate) fn ellipsoids() -> Vec<EllipsoidCatalogInfo> {
    ELLIPSOID_CATALOG.get_or_init(ellipsoids_uncached).clone()
}

fn ellipsoids_uncached() -> Vec<EllipsoidCatalogInfo> {
    proj_static_list(
        || {
            // SAFETY: PROJ returns a process-static null-terminated array.
            unsafe { proj_sys::proj_list_ellps() }
        },
        |info| {
            let id = string_from_ptr(info.id)?;
            Some(EllipsoidCatalogInfo {
                id,
                semi_major: string_from_ptr(info.major),
                definition: string_from_ptr(info.ell),
                name: string_from_ptr(info.name),
            })
        },
    )
}

pub(crate) fn prime_meridians() -> Vec<PrimeMeridianCatalogInfo> {
    PRIME_MERIDIAN_CATALOG
        .get_or_init(prime_meridians_uncached)
        .clone()
}

fn prime_meridians_uncached() -> Vec<PrimeMeridianCatalogInfo> {
    proj_static_list(
        || {
            // SAFETY: PROJ returns a process-static null-terminated array.
            unsafe { proj_sys::proj_list_prime_meridians() }
        },
        |info| {
            let id = string_from_ptr(info.id)?;
            Some(PrimeMeridianCatalogInfo {
                id,
                definition: string_from_ptr(info.defn),
            })
        },
    )
}

pub(crate) fn to_wkt_with_options(
    value: &str,
    version: WktVersion,
    options: &CrsWktOptions,
) -> Result<String> {
    cached_export(
        value,
        || CrsExportKind::Wkt {
            version,
            options: options.clone(),
        },
        wkt_export,
    )
}

pub(crate) fn to_projjson(value: &str) -> Result<String> {
    to_projjson_with_options(value, &CrsProjJsonOptions::default())
}

pub(crate) fn to_projjson_with_options(
    value: &str,
    options: &CrsProjJsonOptions,
) -> Result<String> {
    cached_export(
        value,
        || CrsExportKind::ProjJson {
            options: options.clone(),
        },
        projjson_export,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ProjStringVersion {
    V4 = 4,
    V5 = 5,
}

impl std::hash::Hash for ProjStringVersion {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (*self as u8).hash(state);
    }
}

pub(crate) fn to_proj(
    value: &str,
    version: ProjStringVersion,
    options: &CrsProjOptions,
) -> Result<String> {
    cached_export(
        value,
        || CrsExportKind::Proj {
            version,
            options: options.clone(),
        },
        proj_string_export,
    )
}

pub(crate) fn cached_export(
    value: &str,
    kind: impl FnOnce() -> CrsExportKind,
    produce: impl FnOnce(&CrsExportKey) -> Result<String>,
) -> Result<String> {
    let normalized = normalize(value)?;
    let key = CrsExportKey {
        crs: normalized,
        kind: kind(),
    };
    cached_crs_export(&key, produce)
}

fn wkt_export(key: &CrsExportKey) -> Result<String> {
    let CrsExportKind::Wkt { version, options } = &key.kind else {
        unreachable!("WKT cache key");
    };
    ProjObject::new(&key.crs)?.to_wkt_with_options(key.crs.clone(), version.to_proj(), options)
}

fn projjson_export(key: &CrsExportKey) -> Result<String> {
    let CrsExportKind::ProjJson { options } = &key.kind else {
        unreachable!("PROJJSON export cache key has PROJJSON options")
    };
    ProjObject::new(&key.crs)?.to_projjson(key.crs.clone(), options)
}

fn proj_string_export(key: &CrsExportKey) -> Result<String> {
    let CrsExportKind::Proj { version, options } = &key.kind else {
        unreachable!("PROJ string cache key");
    };
    ProjObject::new(&key.crs)?.to_proj_string(key.crs.clone(), version.to_proj(), options)
}

pub(crate) fn crs_to_2d_export(key: &CrsExportKey) -> Result<String> {
    let CrsExportKind::To2d { name } = &key.kind else {
        unreachable!("2D CRS cache key");
    };
    ProjObject::new(&key.crs)?.to_2d(key.crs.clone(), name.as_deref())
}

pub(crate) fn crs_to_3d_export(key: &CrsExportKey) -> Result<String> {
    let CrsExportKind::To3d { name } = &key.kind else {
        unreachable!("3D CRS cache key");
    };
    ProjObject::new(&key.crs)?.to_3d(key.crs.clone(), name.as_deref())
}

pub(crate) fn same(left: &str, right: &str, comparison: CrsComparison) -> Result<bool> {
    ensure_thread_caches_current();
    let left = normalize(left)?;
    let right = normalize(right)?;
    let criterion = comparison.criterion();
    let key = CrsComparisonKey {
        left,
        right,
        criterion,
    };
    CRS_COMPARISON_CACHE.with(|items| {
        let mut cache = items.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_COMPARISON_CACHE_CAPACITY,
            |item| item.key == key,
            || {
                let left = ProjObject::new(&key.left)?;
                let right = ProjObject::new(&key.right)?;
                let value = left.is_equivalent_to(&right, key.criterion);
                Ok(CachedCrsComparison {
                    key: key.clone(),
                    value,
                })
            },
        )?;
        Ok(cache[index].value)
    })
}

pub(crate) fn cached_crs_export<F>(key: &CrsExportKey, produce: F) -> Result<String>
where
    F: FnOnce(&CrsExportKey) -> Result<String>,
{
    ensure_thread_caches_current();
    CRS_EXPORT_CACHE.with(|items| {
        let mut cache = items.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_EXPORT_CACHE_CAPACITY,
            |item| &item.key == key,
            || {
                let value = produce(key)?;
                Ok(CachedCrsExport {
                    key: key.clone(),
                    value,
                })
            },
        )?;
        Ok(cache[index].value.clone())
    })
}

pub(crate) static PROJ_OPERATION_CATALOG: OnceLock<Vec<ProjOperationCatalogInfo>> = OnceLock::new();
pub(crate) static ELLIPSOID_CATALOG: OnceLock<Vec<EllipsoidCatalogInfo>> = OnceLock::new();
pub(crate) static PRIME_MERIDIAN_CATALOG: OnceLock<Vec<PrimeMeridianCatalogInfo>> = OnceLock::new();
