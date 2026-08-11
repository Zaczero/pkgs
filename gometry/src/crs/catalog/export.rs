use crate::crs::{
    CRS_COMPARISON_CACHE, CRS_COMPARISON_CACHE_CAPACITY, CRS_EXPORT_CACHE,
    CRS_EXPORT_CACHE_CAPACITY, CachedCrsComparison, CachedCrsExport, CrsComparison,
    CrsComparisonKey, CrsExportKey, CrsExportKind, CrsInfo, CrsProjJsonOptions, CrsProjOptions,
    CrsWktOptions, EllipsoidCatalogInfo, OnceLock, PrimeMeridianCatalogInfo, ProjObject,
    ProjOperationCatalogInfo, WktVersion, ensure_thread_caches_current,
    first_static_string_from_ptr, info, lru_resolve, normalize,
};
use crate::error::Result;

pub(crate) fn proj_operations() -> Vec<ProjOperationCatalogInfo> {
    PROJ_OPERATION_CATALOG
        .get_or_init(proj_operations_uncached)
        .clone()
}

fn proj_operations_uncached() -> Vec<ProjOperationCatalogInfo> {
    let mut items = Vec::new();
    // SAFETY: DOC-STATIC. PROJ returns an immutable process-static array of
    // PJ_OPERATIONS records terminated by a sentinel whose `id` field is null
    // (PROJ loops `for (p = proj_list_operations(); p->id; ++p)`). The mapper
    // reads `id` first and returns None at the sentinel; pointer arithmetic
    // advances only after a non-sentinel record.
    let mut current = unsafe { proj_sys::proj_list_operations() };
    while !current.is_null() {
        // SAFETY: current points at a process-static record; id is read first.
        let info = unsafe { &*current };
        let Some(id) = proj_c_string!(info.id) else {
            break;
        };
        items.push(ProjOperationCatalogInfo {
            id,
            // SAFETY: descr is process-static char** (or null on sentinel).
            description: unsafe { first_static_string_from_ptr(info.descr) },
        });
        // SAFETY: advance one record within the static sentinel-terminated array.
        current = unsafe { current.add(1) };
    }
    items
}

pub(crate) fn ellipsoids() -> Vec<EllipsoidCatalogInfo> {
    ELLIPSOID_CATALOG.get_or_init(ellipsoids_uncached).clone()
}

fn ellipsoids_uncached() -> Vec<EllipsoidCatalogInfo> {
    let mut items = Vec::new();
    // SAFETY: DOC-STATIC. Process-static ellipsoid records terminated by a
    // sentinel whose `id` is null (not a pointer-null-terminated array).
    let mut current = unsafe { proj_sys::proj_list_ellps() };
    while !current.is_null() {
        // SAFETY: current points at a process-static record; id is read first.
        let info = unsafe { &*current };
        let Some(id) = proj_c_string!(info.id) else {
            break;
        };
        items.push(EllipsoidCatalogInfo {
            id,
            semi_major: proj_c_string!(info.major),
            definition: proj_c_string!(info.ell),
            name: proj_c_string!(info.name),
        });
        // SAFETY: advance one record within the static sentinel-terminated array.
        current = unsafe { current.add(1) };
    }
    items
}

pub(crate) fn prime_meridians() -> Vec<PrimeMeridianCatalogInfo> {
    PRIME_MERIDIAN_CATALOG
        .get_or_init(prime_meridians_uncached)
        .clone()
}

fn prime_meridians_uncached() -> Vec<PrimeMeridianCatalogInfo> {
    let mut items = Vec::new();
    // SAFETY: DOC-STATIC. Process-static prime-meridian records terminated by a
    // sentinel whose `id` is null (not a pointer-null-terminated array).
    let mut current = unsafe { proj_sys::proj_list_prime_meridians() };
    while !current.is_null() {
        // SAFETY: current points at a process-static record; id is read first.
        let info = unsafe { &*current };
        let Some(id) = proj_c_string!(info.id) else {
            break;
        };
        items.push(PrimeMeridianCatalogInfo {
            id,
            definition: proj_c_string!(info.defn),
        });
        // SAFETY: advance one record within the static sentinel-terminated array.
        current = unsafe { current.add(1) };
    }
    items
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
    // Order the pair so (A,B) and (B,A) share one cache entry (comparison is
    // commutative for both exact and ignore-axis-order modes).
    let (left, right) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    let key = CrsComparisonKey {
        left,
        right,
        mode: comparison,
    };
    CRS_COMPARISON_CACHE.with(|items| {
        let mut cache = items.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_COMPARISON_CACHE_CAPACITY,
            |item| item.key == key,
            || {
                let value = crs_same_uncached(&key.left, &key.right, key.mode)?;
                Ok(CachedCrsComparison {
                    key: key.clone(),
                    value,
                })
            },
        )?;
        Ok(cache[index].value)
    })
}

/// Single horizontal geographic or projected CRS (top-level kind only).
/// Compound, vertical, engineering, bound, and geocentric stay exact and are
/// not admitted into the axis-order-agnostic path.
fn is_single_horizontal_crs(info: &CrsInfo) -> bool {
    matches!(
        info.kind,
        "geographic_2d" | "geographic_3d" | "geographic" | "projected"
    )
}

/// Uncached CRS comparison. `IgnoreAxisOrder` is the operational coordinate
/// predicate: single horizontal geographic/projected CRS of equal axis count,
/// each `proj_normalize_for_visualization`'d, then compared with
/// `PJ_COMP_EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS`. `Exact` stays a strict PROJ
/// equivalence. Both negative and positive results are cached by the caller.
fn crs_same_uncached(left: &str, right: &str, mode: CrsComparison) -> Result<bool> {
    if left == right {
        return Ok(true);
    }
    match mode {
        CrsComparison::Exact => {
            let left = ProjObject::new(left)?;
            let right = ProjObject::new(right)?;
            Ok(left.is_equivalent_to(&right, mode.criterion()))
        },
        CrsComparison::IgnoreAxisOrder => {
            let left_info = info(left)?;
            let right_info = info(right)?;
            if !is_single_horizontal_crs(&left_info) || !is_single_horizontal_crs(&right_info) {
                // Compound / vertical / engineering / bound / mixed kinds: no
                // axis-order relaxation — only the string-equality short path
                // above can accept them.
                return Ok(false);
            }
            if left_info.axes.len() != right_info.axes.len() {
                return Ok(false);
            }
            let left = ProjObject::new(left)?.into_normalized_for_visualization()?;
            let right = ProjObject::new(right)?.into_normalized_for_visualization()?;
            Ok(left.is_equivalent_to(&right, mode.criterion()))
        },
    }
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
