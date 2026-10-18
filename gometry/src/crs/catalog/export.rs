use crate::crs::{
    CRS_COMPARISON_CACHE, CRS_COMPARISON_CACHE_CAPACITY, CRS_EXPORT_CACHE,
    CRS_EXPORT_CACHE_CAPACITY, CachedCrsComparison, CachedCrsExport, CrsComparison,
    CrsComparisonKey, CrsExportKey, CrsExportKind, CrsInfo, CrsProjJsonOptions, CrsWktOptions,
    EllipsoidCatalogInfo, OnceLock, PrimeMeridianCatalogInfo, ProjObject, ProjOperationCatalogInfo,
    RuntimeState, WktVersion, ensure_thread_caches_current, first_static_string_from_ptr,
    info_with_state, lru_insert, lru_promote, normalize, runtime_state_snapshot,
};
use crate::error::Result;

pub(crate) fn proj_operations() -> &'static [ProjOperationCatalogInfo] {
    PROJ_OPERATION_CATALOG.get_or_init(proj_operations_uncached)
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

pub(crate) fn ellipsoids() -> &'static [EllipsoidCatalogInfo] {
    ELLIPSOID_CATALOG.get_or_init(ellipsoids_uncached)
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

pub(crate) fn prime_meridians() -> &'static [PrimeMeridianCatalogInfo] {
    PRIME_MERIDIAN_CATALOG.get_or_init(prime_meridians_uncached)
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

#[cfg(test)]
pub(crate) fn to_wkt_with_options(
    value: &str,
    version: WktVersion,
    options: &CrsWktOptions,
) -> Result<String> {
    let state = runtime_state_snapshot()?;
    to_wkt_with_options_at(value, version, options, &state)
}

pub(crate) fn to_wkt_with_options_at(
    value: &str,
    version: WktVersion,
    options: &CrsWktOptions,
    state: &RuntimeState,
) -> Result<String> {
    cached_export(
        value,
        || CrsExportKind::Wkt {
            version,
            options: options.clone(),
        },
        state,
    )
}

pub(crate) fn to_projjson(value: &str) -> Result<String> {
    to_projjson_with_options(value, &CrsProjJsonOptions::default())
}

pub(crate) fn to_projjson_with_options(
    value: &str,
    options: &CrsProjJsonOptions,
) -> Result<String> {
    let state = runtime_state_snapshot()?;
    to_projjson_with_options_at(value, options, &state)
}

pub(crate) fn to_projjson_with_options_at(
    value: &str,
    options: &CrsProjJsonOptions,
    state: &RuntimeState,
) -> Result<String> {
    cached_export(
        value,
        || CrsExportKind::ProjJson {
            options: options.clone(),
        },
        state,
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

fn cached_export(
    value: &str,
    kind: impl FnOnce() -> CrsExportKind,
    state: &RuntimeState,
) -> Result<String> {
    let normalized = normalize(value)?;
    let key = CrsExportKey {
        definition: normalized.into(),
        kind: kind(),
    };
    key.resolve(state)
}

impl CrsExportKey {
    /// Copy an existing export without invoking PROJ.
    pub(crate) fn cached(&self, state: &RuntimeState) -> Option<String> {
        if !crate::crs::thread_caches_are_current(state) {
            return None;
        }
        CRS_EXPORT_CACHE.with(|items| {
            let mut cache = items.borrow_mut();
            lru_promote(&mut cache, |item| item.key == *self)
                .then(|| cache.last().expect("promoted export entry").value.clone())
        })
    }

    pub(crate) fn resolve(&self, state: &RuntimeState) -> Result<String> {
        ensure_thread_caches_current(state);
        if let Some(value) = self.cached(state) {
            return Ok(value);
        }
        let object = ProjObject::new_with_state(&self.definition, state)?;
        let value = match &self.kind {
            CrsExportKind::Wkt { version, options } => {
                object.to_wkt_with_options(&self.definition, version.to_proj(), options)?
            },
            CrsExportKind::ProjJson { options } => object.to_projjson(&self.definition, options)?,
            CrsExportKind::Proj { version, options } => {
                object.to_proj_string(&self.definition, version.to_proj(), options)?
            },
            CrsExportKind::To2d { name } => object.to_2d(&self.definition, name.as_deref())?,
            CrsExportKind::To3d { name } => object.to_3d(&self.definition, name.as_deref())?,
        };
        CRS_EXPORT_CACHE.with(|items| {
            lru_insert(
                &mut items.borrow_mut(),
                CRS_EXPORT_CACHE_CAPACITY,
                CachedCrsExport {
                    key: self.clone(),
                    value: value.clone(),
                },
            );
        });
        Ok(value)
    }
}

pub(crate) fn same(left: &str, right: &str, comparison: CrsComparison) -> Result<bool> {
    let state = runtime_state_snapshot()?;
    same_with_state(left, right, comparison, &state)
}

pub(crate) fn same_with_state(
    left: &str,
    right: &str,
    comparison: CrsComparison,
    state: &RuntimeState,
) -> Result<bool> {
    if let Some(value) = same_if_cached(left, right, comparison, state)? {
        return Ok(value);
    }
    let left = normalize(left)?;
    let right = normalize(right)?;
    let (left, right) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    let value = crs_same_uncached(&left, &right, comparison, state)?;
    ensure_thread_caches_current(state);
    let key = CrsComparisonKey {
        left: left.into(),
        right: right.into(),
        mode: comparison,
    };
    CRS_COMPARISON_CACHE.with(|items| {
        let mut cache = items.borrow_mut();
        Ok(lru_insert(
            &mut cache,
            CRS_COMPARISON_CACHE_CAPACITY,
            CachedCrsComparison { key, value },
        )
        .value)
    })
}

/// Structural identities and memoized comparisons need no PROJ execution.
pub(crate) fn same_if_cached(
    left: &str,
    right: &str,
    comparison: CrsComparison,
    state: &RuntimeState,
) -> Result<Option<bool>> {
    let left = normalize(left)?;
    let right = normalize(right)?;
    // Order the pair so (A,B) and (B,A) share one cache entry (comparison is
    // commutative for both exact and ignore-axis-order modes).
    let (left, right) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    if left == right {
        return Ok(Some(true));
    }
    if !crate::crs::thread_caches_are_current(state) {
        return Ok(None);
    }
    Ok(CRS_COMPARISON_CACHE.with(|items| {
        let mut cache = items.borrow_mut();
        lru_promote(&mut cache, |item| {
            item.key.left.as_str() == left.as_ref()
                && item.key.right.as_str() == right.as_ref()
                && item.key.mode == comparison
        })
        .then(|| {
            cache
                .last()
                .expect("a promoted CRS comparison is the MRU tail")
                .value
        })
    }))
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
fn crs_same_uncached(
    left: &str,
    right: &str,
    mode: CrsComparison,
    state: &RuntimeState,
) -> Result<bool> {
    if left == right {
        return Ok(true);
    }
    match mode {
        CrsComparison::Exact => {
            let left = ProjObject::new_with_state(left, state)?;
            let right = ProjObject::new_with_state(right, state)?;
            Ok(left.is_equivalent_to(&right, mode.criterion()))
        },
        CrsComparison::IgnoreAxisOrder => {
            let left_info = info_with_state(left, state)?;
            let right_info = info_with_state(right, state)?;
            if !is_single_horizontal_crs(&left_info) || !is_single_horizontal_crs(&right_info) {
                // Compound / vertical / engineering / bound / mixed kinds: no
                // axis-order relaxation — only the string-equality short path
                // above can accept them.
                return Ok(false);
            }
            if left_info.axes.len() != right_info.axes.len() {
                return Ok(false);
            }
            let left =
                ProjObject::new_with_state(left, state)?.into_normalized_for_visualization()?;
            let right =
                ProjObject::new_with_state(right, state)?.into_normalized_for_visualization()?;
            Ok(left.is_equivalent_to(&right, mode.criterion()))
        },
    }
}

pub(crate) static PROJ_OPERATION_CATALOG: OnceLock<Vec<ProjOperationCatalogInfo>> = OnceLock::new();
pub(crate) static ELLIPSOID_CATALOG: OnceLock<Vec<EllipsoidCatalogInfo>> = OnceLock::new();
pub(crate) static PRIME_MERIDIAN_CATALOG: OnceLock<Vec<PrimeMeridianCatalogInfo>> = OnceLock::new();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn export_lookup_leaves_stale_native_products_for_the_resolver() {
        let state = runtime_state_snapshot().expect("runtime state");
        ensure_thread_caches_current(&state);
        let key = CrsExportKey {
            definition: "EPSG:4326".into(),
            kind: CrsExportKind::Wkt {
                version: WktVersion::Wkt2_2019,
                options: CrsWktOptions::default(),
            },
        };
        let expected = key.resolve(&state).expect("WKT export");
        assert!(expected.starts_with("GEOGCRS["));
        assert_eq!(key.cached(&state).as_deref(), Some(expected.as_str()));
        let next = RuntimeState {
            generation: state.generation.wrapping_add(1),
            config: std::sync::Arc::clone(&state.config),
        };
        assert!(key.cached(&next).is_none());
        assert_eq!(key.cached(&state).as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn warm_comparison_of_an_authored_crs_allocates_nothing() {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK
            .lock()
            .expect("runtime test lock");
        let state = runtime_state_snapshot().expect("runtime snapshot");
        let definition = "+proj=longlat +datum=WGS84 +no_defs +type=crs";
        assert!(
            same_with_state(definition, definition, CrsComparison::Exact, &state)
                .expect("first comparison")
        );
        let (same, allocations) = crate::test_support::allocation_count(|| {
            same_with_state(definition, definition, CrsComparison::Exact, &state)
        });
        assert!(same.expect("cached comparison"));
        assert_eq!(allocations, 0);
    }
}
