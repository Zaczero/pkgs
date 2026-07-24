use geographiclib_rs::Geodesic;

use super::*;
use crate::error::Result;

fn cached_geodesic_object(crs: &str) -> Result<CachedGeodesicObject> {
    // Reuse the cached CRS introspection (`info` is LRU-backed) instead of a
    // fresh `ProjObject` FFI parse — the ellipsoid is identical and this CRS was
    // already resolved to reach the geodesic path.
    let crs_info = info(crs)?;
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
    ensure_thread_caches_current();
    CRS_GEODESIC_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_GEODESIC_CACHE_CAPACITY,
            |item| item.crs == crs,
            || cached_geodesic_object(crs),
        )
        .map_err(&map_err)?;
        f(&cache[index].object)
    })
}

pub(crate) fn with_geodesic<T>(crs: &str, f: impl FnOnce(&Geodesic) -> Result<T>) -> Result<T> {
    with_geodesic_cache(crs, |error| error, f)
}
