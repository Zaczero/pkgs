use geographiclib_rs::Geodesic;

use crate::crs::geodesic::{
    CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY, CachedGeodesicObject, CrsError,
    ensure_thread_caches_current, lru_resolve,
};
use crate::crs::{RuntimeState, info_with_state};
use crate::error::Result;

/// Largest flattening admitted by the pinned order-six binary64 series.
///
/// GeographicLib describes the series as accurate for `|f| < 0.02` and gives
/// an approximate error table at the boundary. The released geodesic layer
/// keeps the selected ordinary domain explicit rather than accepting shapes
/// whose primitive error grows with flattening.
pub(crate) const GEODESIC_SERIES_MAX_FLATTENING: f64 = 0.02;

fn validate_geodesic_ellipsoid(
    crs: &str,
    semi_major: f64,
    semi_minor: f64,
    flattening: f64,
) -> Result<()> {
    if !(semi_major.is_finite() && semi_major > 0.0) {
        return Err(CrsError::invalid(format!(
            "{crs} exposes an invalid geodesic ellipsoid: semi-major axis must be positive and finite"
        )));
    }
    if !semi_minor.is_finite() || semi_minor <= 0.0 {
        return Err(CrsError::invalid(format!(
            "{crs} exposes an invalid geodesic ellipsoid: semi-minor axis must be positive and finite"
        )));
    }
    if !flattening.is_finite() {
        return Err(CrsError::invalid(format!(
            "{crs} geodesic operations require a finite flattening in the supported binary64 series domain |f| <= 0.02"
        )));
    }
    let semi_minor_from_flattening = semi_major * (1.0 - flattening);
    if !(semi_minor_from_flattening.is_finite() && semi_minor_from_flattening > 0.0) {
        return Err(CrsError::invalid(format!(
            "{crs} exposes an invalid geodesic ellipsoid: b=a(1-f) must be positive and finite (a={semi_major}, f={flattening})"
        )));
    }
    if flattening.abs() > GEODESIC_SERIES_MAX_FLATTENING {
        return Err(CrsError::invalid(format!(
            "{crs} geodesic operations support only the order-six binary64 series domain |f| <= 0.02; exact elliptic support is not shipped (got f={flattening})"
        )));
    }
    Ok(())
}

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
    validate_geodesic_ellipsoid(
        crs,
        ellipsoid.semi_major_metre,
        ellipsoid.semi_minor_metre,
        flattening,
    )?;
    Ok(CachedGeodesicObject {
        definition: crs.to_owned(),
        object: Geodesic::new(ellipsoid.semi_major_metre, flattening),
    })
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
        let Some(index) = cache.iter().position(|item| item.definition == crs) else {
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
            |item| item.definition == crs,
            || Ok(entry),
        )
        .map_err(&map_err)?;
        f(&cache[index].object)
    })
}

pub(crate) fn with_geodesic_at<T>(
    crs: &str,
    state: &RuntimeState,
    f: impl FnOnce(&Geodesic) -> Result<T>,
) -> Result<T> {
    with_geodesic_cache_at(crs, state, |error| error, f)
}

#[cfg(test)]
mod tests {
    use super::{GEODESIC_SERIES_MAX_FLATTENING, validate_geodesic_ellipsoid};

    #[test]
    fn series_domain_admits_sphere_and_both_boundaries() {
        for flattening in [
            -GEODESIC_SERIES_MAX_FLATTENING,
            0.0,
            GEODESIC_SERIES_MAX_FLATTENING,
        ] {
            let semi_minor = 6_378_137.0 * (1.0 - flattening);
            validate_geodesic_ellipsoid("test", 6_378_137.0, semi_minor, flattening)
                .expect("series boundary is admitted");
        }
    }

    #[test]
    fn series_domain_rejects_adjacent_and_invalid_ellipsoids() {
        for flattening in [
            f64::next_up(GEODESIC_SERIES_MAX_FLATTENING),
            f64::next_down(-GEODESIC_SERIES_MAX_FLATTENING),
        ] {
            let error = validate_geodesic_ellipsoid(
                "test",
                6_378_137.0,
                6_378_137.0 * (1.0 - flattening),
                flattening,
            )
            .expect_err("adjacent flattening must be rejected");
            assert!(error.to_string().contains("|f| <= 0.02"));
            assert!(error.to_string().contains("binary64"));
        }
        validate_geodesic_ellipsoid("test", 0.0, 1.0, 0.0)
            .expect_err("zero semi-major axis must be rejected");
        validate_geodesic_ellipsoid("test", 1.0, 0.0, 0.0)
            .expect_err("zero semi-minor axis must be rejected");
        validate_geodesic_ellipsoid("test", 1.0, 1.0, f64::NAN)
            .expect_err("non-finite flattening must be rejected");
    }
}
