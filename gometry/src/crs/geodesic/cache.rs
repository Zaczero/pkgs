use geographiclib_rs::Geodesic;

use crate::crs::geodesic::{
    CRS_GEODESIC_CACHE, CRS_GEODESIC_CACHE_CAPACITY, CachedGeodesicObject, CrsError,
    ensure_thread_caches_current, lru_insert, lru_promote,
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

fn admit_geodesic_ellipsoid(
    crs: &str,
    semi_major: f64,
    semi_minor: f64,
    flattening: f64,
) -> Result<Geodesic> {
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
    let geodesic = Geodesic::new(semi_major, flattening);
    // The constructor squares both axes before forming the authalic radius;
    // chart area uses half a² and the backend's full area uses 4πc².
    let semi_major_squared = semi_major * semi_major;
    let semi_minor_squared = geodesic._b * geodesic._b;
    if ![
        semi_major_squared,
        semi_minor_squared,
        semi_major_squared / 2.0,
        geodesic._c2,
        geodesic.area(),
    ]
    .into_iter()
    .all(f64::is_normal)
    {
        return Err(CrsError::invalid(format!(
            "{crs} geodesic ellipsoid axes are outside the representable binary64 range: squared axes, half the squared semi-major axis, authalic radius squared and full surface area must be finite normal values in [{:.17e}, {:.17e}] (a={semi_major:.17e}, b={:.17e}, f={flattening})",
            f64::MIN_POSITIVE,
            f64::MAX,
            geodesic._b,
        )));
    }
    Ok(geodesic)
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
    let object = admit_geodesic_ellipsoid(
        crs,
        ellipsoid.semi_major_metre,
        ellipsoid.semi_minor_metre,
        flattening,
    )?;
    Ok(CachedGeodesicObject {
        definition: crs.to_owned(),
        authalic_series: super::AuthalicSeries::new(&object),
        object,
    })
}

pub(crate) fn with_geodesic_cache_at<T, E>(
    crs: &str,
    state: &RuntimeState,
    map_err: impl Fn(crate::error::Error) -> E,
    f: impl FnOnce(&Geodesic) -> Result<T, E>,
) -> Result<T, E> {
    with_geodesic_object_cache_at(crs, state, map_err, |entry| f(&entry.object))
}

pub(crate) fn with_geodesic_chart_cache_at<T, E>(
    crs: &str,
    state: &RuntimeState,
    map_err: impl Fn(crate::error::Error) -> E,
    f: impl FnOnce(&Geodesic, &super::AuthalicSeries) -> Result<T, E>,
) -> Result<T, E> {
    with_geodesic_object_cache_at(crs, state, map_err, |entry| {
        f(&entry.object, &entry.authalic_series)
    })
}

fn with_geodesic_object_cache_at<T, E>(
    crs: &str,
    state: &RuntimeState,
    map_err: impl Fn(crate::error::Error) -> E,
    f: impl FnOnce(&CachedGeodesicObject) -> Result<T, E>,
) -> Result<T, E> {
    ensure_thread_caches_current(state);
    // Build a miss outside the cache borrow. `cached_geodesic_object` uses the
    // CRS-info cache, and a runtime-generation change during that work may
    // invalidate every thread-local cache, including this one.
    let hit = CRS_GEODESIC_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        lru_promote(&mut cache, |item| item.definition == crs)
    });
    if hit {
        return CRS_GEODESIC_CACHE.with(|cache| {
            let cache = cache.borrow();
            f(&cache
                .last()
                .expect("a promoted geodesic object is the MRU tail"))
        });
    }

    let entry = cached_geodesic_object(crs, state).map_err(&map_err)?;
    ensure_thread_caches_current(state);
    // The miss path resolves cached CRS info and builds the ellipsoid's geodesic
    // and authalic series; it cannot re-enter this cache bucket.
    CRS_GEODESIC_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let entry = lru_insert(&mut cache, CRS_GEODESIC_CACHE_CAPACITY, entry);
        f(&entry)
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
    use super::{GEODESIC_SERIES_MAX_FLATTENING, admit_geodesic_ellipsoid};

    #[test]
    fn series_domain_admits_sphere_and_both_boundaries() {
        for flattening in [
            -GEODESIC_SERIES_MAX_FLATTENING,
            0.0,
            GEODESIC_SERIES_MAX_FLATTENING,
        ] {
            let semi_minor = 6_378_137.0 * (1.0 - flattening);
            admit_geodesic_ellipsoid("test", 6_378_137.0, semi_minor, flattening)
                .expect("series boundary is admitted");
        }
    }

    #[test]
    fn series_domain_rejects_adjacent_and_invalid_ellipsoids() {
        for flattening in [
            f64::next_up(GEODESIC_SERIES_MAX_FLATTENING),
            f64::next_down(-GEODESIC_SERIES_MAX_FLATTENING),
        ] {
            let error = admit_geodesic_ellipsoid(
                "test",
                6_378_137.0,
                6_378_137.0 * (1.0 - flattening),
                flattening,
            )
            .expect_err("adjacent flattening must be rejected");
            assert!(error.to_string().contains("|f| <= 0.02"));
            assert!(error.to_string().contains("binary64"));
        }
        admit_geodesic_ellipsoid("test", 0.0, 1.0, 0.0)
            .expect_err("zero semi-major axis must be rejected");
        admit_geodesic_ellipsoid("test", 1.0, 0.0, 0.0)
            .expect_err("zero semi-minor axis must be rejected");
        admit_geodesic_ellipsoid("test", 1.0, 1.0, f64::NAN)
            .expect_err("non-finite flattening must be rejected");
    }

    #[test]
    fn axis_arithmetic_boundaries_admit_scaled_physical_distances() {
        for axis in [
            f64::from_bits(0x2006_A09E_667F_3BCD),
            f64::from_bits(0x5FD2_0DD7_5042_9B6D),
        ] {
            let crs = format!("+proj=longlat +a={axis:.17e} +b={axis:.17e} +type=crs");
            let model = crate::crs::metric_model(Some(&crs)).unwrap();
            let metric = crate::crs::GeodesicMetric::from_model(&model).unwrap();
            let distance = crate::crs::geodesic_point_pair_distance(
                &metric.geodesic,
                crate::geometry::Point::new_raw_xy(0.0, 0.0),
                crate::geometry::Point::new_raw_xy(90.0, 0.0),
                metric.frame,
                metric.state.generation,
            )
            .unwrap();
            assert!(distance.is_finite() && distance > 0.0);
            assert!((distance / axis - std::f64::consts::FRAC_PI_2).abs() <= 4.0 * f64::EPSILON);
        }
    }

    #[test]
    fn axis_arithmetic_boundaries_reject_adjacent_unrepresentable_areas() {
        for axis in [
            f64::from_bits(0x2006_A09E_667F_3BCC),
            f64::from_bits(0x5FD2_0DD7_5042_9B6E),
        ] {
            let crs = format!("+proj=longlat +a={axis:.17e} +b={axis:.17e} +type=crs");
            let model = crate::crs::metric_model(Some(&crs)).unwrap();
            let Err(error) = crate::crs::GeodesicMetric::from_model(&model) else {
                panic!("an adjacent axis outside the arithmetic range is admitted");
            };
            let message = error.to_string();
            assert!(
                message.contains("outside the representable binary64 range"),
                "{message}"
            );
            assert!(message.contains("finite normal values"), "{message}");
        }
    }
}
