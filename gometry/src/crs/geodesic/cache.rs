use geographiclib_rs::Geodesic;

use crate::crs::geodesic::{
    CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY, CachedGeodesicObject, CrsError,
    ensure_thread_caches_current, lru_resolve,
};
use crate::crs::{RuntimeState, info_with_state, runtime_state_snapshot};
use crate::error::Result;

fn cached_geodesic_object(crs: &str, state: &RuntimeState) -> Result<CachedGeodesicObject> {
    // Reuse the cached CRS introspection (`info` is LRU-backed) instead of a
    // fresh `ProjObject` FFI parse — the ellipsoid is identical and this CRS was
    // already resolved to reach the geodesic path.
    let crs_info = info_with_state(crs, state)?;
    let ellipsoid = crs_info.ellipsoid.as_ref().ok_or_else(|| {
        CrsError::invalid(format!(
            "{crs} does not expose an ellipsoid for geodesic use"
        ))
    })?;
    let flattening =
        if ellipsoid.inverse_flattening.is_finite() && ellipsoid.inverse_flattening != 0.0 {
            1.0 / ellipsoid.inverse_flattening
        } else if ellipsoid.semi_major_metre != 0.0 {
            (ellipsoid.semi_major_metre - ellipsoid.semi_minor_metre) / ellipsoid.semi_major_metre
        } else {
            return Err(CrsError::invalid(format!(
                "{crs} exposes an invalid ellipsoid for geodesic use"
            )));
        };
    if !ellipsoid.semi_major_metre.is_finite()
        || !ellipsoid.semi_minor_metre.is_finite()
        || !flattening.is_finite()
    {
        return Err(CrsError::invalid(format!(
            "{crs} exposes an invalid ellipsoid for geodesic use"
        )));
    }
    Ok(CachedGeodesicObject {
        crs: crs.to_owned(),
        object: Geodesic::new(ellipsoid.semi_major_metre, flattening),
    })
}

pub(crate) fn with_geodesic_cache<T, E>(
    crs: &str,
    map_err: impl Fn(crate::error::Error) -> E,
    f: impl FnOnce(&Geodesic) -> Result<T, E>,
) -> Result<T, E> {
    let state = runtime_state_snapshot().map_err(&map_err)?;
    with_geodesic_cache_at(crs, &state, map_err, f)
}

pub(crate) fn with_geodesic_cache_at<T, E>(
    crs: &str,
    state: &RuntimeState,
    map_err: impl Fn(crate::error::Error) -> E,
    f: impl FnOnce(&Geodesic) -> Result<T, E>,
) -> Result<T, E> {
    ensure_thread_caches_current(state);
    // Build a miss outside the cache borrow. `cached_geodesic_object` uses the
    // CRS-info cache, and a runtime-generation change during that work may
    // invalidate every thread-local cache, including this one.
    let hit = CRS_GEODESIC_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let Some(index) = cache.iter().position(|item| item.crs == crs) else {
            return false;
        };
        if index + 1 != cache.len() {
            let entry = cache.remove(index);
            cache.push(entry);
        }
        true
    });
    if hit {
        return CRS_GEODESIC_CACHE.with(|cache| {
            let cache = cache.borrow();
            f(&cache[cache.len() - 1].object)
        });
    }

    let entry = cached_geodesic_object(crs, state).map_err(&map_err)?;
    ensure_thread_caches_current(state);
    CRS_GEODESIC_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_GEODESIC_CACHE_CAPACITY,
            |item| item.crs == crs,
            || Ok(entry),
        )
        .map_err(&map_err)?;
        f(&cache[index].object)
    })
}

pub(crate) fn with_geodesic<T>(crs: &str, f: impl FnOnce(&Geodesic) -> Result<T>) -> Result<T> {
    with_geodesic_cache(crs, |error| error, f)
}

pub(crate) fn with_geodesic_at<T>(
    crs: &str,
    state: &RuntimeState,
    f: impl FnOnce(&Geodesic) -> Result<T>,
) -> Result<T> {
    with_geodesic_cache_at(crs, state, |error| error, f)
}
