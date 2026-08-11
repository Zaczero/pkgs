//! Geodesic lower-bound pruning for the spatial index.
//!
//! A sound, tight lower bound on the ellipsoidal geodesic distance between a
//! query point and any point inside a lon/lat envelope, so geographic
//! nearest/`dwithin` queries prune the R-tree instead of Karney-evaluating
//! every entry.
//!
//! **Soundness.** Karney's auxiliary-sphere construction gives the geodesic
//! length as `s = b · ∫√(1 + k²sin²σ′) dσ′ ≥ b · σ`, where `b` is the
//! semi-minor axis, `σ` the arc on the auxiliary sphere between the points'
//! *reduced* latitudes (`tan β = (1−f) tan φ`), and the ellipsoidal longitude
//! difference never exceeds the auxiliary-sphere one (`|Δλ| ≤ |Δω|`). The
//! central angle grows with the longitude gap, so evaluating it with the raw
//! `Δλ` under-estimates `σ` — hence `b · σ(β₁, β₂, Δλ) ≤ s` for every oblate
//! ellipsoid. The equatorial north–south case is exact
//! (`b · (1−f) ·` … `= M(0) · dφ`), and the bound is everywhere within `b/a`
//! of the true distance. Verified against the Karney oracle over random,
//! equatorial-micro-delta, polar, antimeridian, and near-antipodal pairs in
//! `tests/test_index.py`.
//!
//! Pruning applies only when both realized point sets are exactly bounded by
//! their envelopes — a **point** query against an **all-point** index (the
//! canonical geographic KNN/radius workload). Non-point geodesic shapes
//! realize edge points on geodesic arcs that can bulge outside their planar
//! envelope, so those queries keep the exact full scan.

use rstar::AABB;

use crate::geometry::Point;
use crate::{HeapSize, crs};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Degrees(pub f64);

#[derive(Clone, Copy, Debug)]
pub(crate) struct Radians(pub f64);

crate::heapless!(Degrees, Radians);

pub(crate) struct DwithinWindows {
    first: AABB<[f64; 2]>,
    second: Option<AABB<[f64; 2]>>,
}

impl DwithinWindows {
    const fn one(first: AABB<[f64; 2]>) -> Self {
        Self {
            first,
            second: None,
        }
    }

    const fn two(first: AABB<[f64; 2]>, second: AABB<[f64; 2]>) -> Self {
        Self {
            first,
            second: Some(second),
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &AABB<[f64; 2]>> {
        std::iter::once(&self.first).chain(self.second.iter())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        1 + usize::from(self.second.is_some())
    }
}

/// Reduced (parametric) latitude in radians: `tan β = (1−f) tan φ`.
fn reduced_latitude(lat: Degrees, one_minus_f: f64) -> Radians {
    let phi = lat.0.to_radians();
    // ±90° maps to ±π/2 (tan overflows but atan saturates correctly).
    Radians(
        f64::atan(one_minus_f * phi.tan())
            .clamp(-std::f64::consts::FRAC_PI_2, std::f64::consts::FRAC_PI_2),
    )
}

/// Geodetic latitude (degrees) back from a reduced latitude.
fn geodetic_latitude(beta: Radians, one_minus_f: f64) -> Degrees {
    Degrees(
        f64::atan(beta.0.tan() / one_minus_f)
            .to_degrees()
            .clamp(-90.0, 90.0),
    )
}

/// Haversine central angle between two auxiliary-sphere points.
fn central_angle(beta1: Radians, beta2: Radians, delta_lon: Radians) -> Radians {
    let sin_lat = ((beta2.0 - beta1.0) / 2.0).sin();
    let sin_lon = (delta_lon.0 / 2.0).sin();
    // Plain ops: scalar `mul_add` is a libm call below x86-64-v3, and the
    // clamped haversine bound gains nothing from fused rounding (sin/cos
    // already carry ~1 ulp).
    let h = (sin_lon * sin_lon * beta1.0.cos() * beta2.0.cos() + sin_lat * sin_lat).clamp(0.0, 1.0);
    Radians(2.0 * h.sqrt().asin())
}

/// Geodesic distance lower bounds from one fixed query point, on one CRS
/// ellipsoid.
pub(crate) struct GeodesicPruner {
    /// Semi-minor axis (meters): the auxiliary-sphere radius of the bound.
    minor_radius: f64,
    one_minus_f: f64,
    /// Query point on the auxiliary sphere (radians).
    beta: Radians,
    lon: Radians,
}

impl GeodesicPruner {
    /// Build a pruner for `point` on the `crs` ellipsoid.
    ///
    /// `None` when the
    /// ellipsoid is not oblate (the bound's proof needs `0 ≤ f < 1`) — callers
    /// fall back to the exact full scan.
    pub(crate) fn new(crs: &str, point: Point) -> Option<Self> {
        // Out-of-domain coordinates fall back to the exact path so they raise
        // the same domain error every other geodesic spelling raises, instead
        // of being silently pruned to an empty window.
        if !(-180.0..=180.0).contains(&point.x) || !(-90.0..=90.0).contains(&point.y) {
            return None;
        }
        let shape = crs::ellipsoid_shape(crs).ok()?;
        let major = shape.semi_major.get();
        let flattening = shape.flattening;
        let one_minus_f = 1.0 - flattening;
        Some(Self {
            minor_radius: major * one_minus_f,
            one_minus_f,
            beta: reduced_latitude(Degrees(point.y), one_minus_f),
            lon: Radians(point.x.to_radians()),
        })
    }

    /// Shortest angular longitude gap to `lon_degrees`, wrap-aware.
    fn lon_gap(&self, lon: Degrees) -> Radians {
        // Both longitudes are validated lonlat, so the delta is within one
        // period — the conditional subtract is EXACT (float `%` would be a
        // soft `fmod` call on the x86-64-v2 baseline, per KNN node visit),
        // keeping the pruning bound sound.
        let delta = (lon.0.to_radians() - self.lon.0).abs();
        let gap = if delta >= std::f64::consts::TAU {
            delta - std::f64::consts::TAU
        } else {
            delta
        };
        Radians(gap.min(std::f64::consts::TAU - gap))
    }

    /// Central angle from the query to the meridian arc at `lon_degrees`
    /// spanning reduced latitudes `[beta_lo, beta_hi]`.
    fn to_meridian_arc(&self, lon: Degrees, beta_lo: Radians, beta_hi: Radians) -> Radians {
        let theta = self.lon_gap(lon);
        let mut best = central_angle(self.beta, beta_lo, theta)
            .0
            .min(central_angle(self.beta, beta_hi, theta).0);
        // Cross-track candidate: the perpendicular foot on the meridian's
        // great circle, valid when it lands inside the arc's latitude span.
        if theta.0 < std::f64::consts::FRAC_PI_2 && self.beta.0.abs() < std::f64::consts::FRAC_PI_2
        {
            let foot = Radians(f64::atan2(self.beta.0.tan(), theta.0.cos()));
            if (beta_lo.0..=beta_hi.0).contains(&foot.0) {
                let cross_track = (theta.0.sin().abs() * self.beta.0.cos())
                    .clamp(0.0, 1.0)
                    .asin();
                best = best.min(cross_track);
            }
        }
        Radians(best)
    }

    /// Lower bound (meters) on the geodesic distance from the query point to
    /// any point inside the lon/lat envelope `env`.
    pub(crate) fn envelope_lower_bound(&self, env: &AABB<[f64; 2]>) -> f64 {
        let (lower, upper) = (env.lower(), env.upper());
        let (lon_lo, lat_lo) = lower.into();
        let (lon_hi, lat_hi) = upper.into();
        let beta_lo = reduced_latitude(Degrees(lat_lo), self.one_minus_f);
        let beta_hi = reduced_latitude(Degrees(lat_hi), self.one_minus_f);
        // Degenerate/wide longitude spans collapse to the latitude gap.
        let lon_degrees = Degrees(self.lon.0.to_degrees());
        let sigma = if lon_hi - lon_lo >= 360.0 || (lon_lo..=lon_hi).contains(&lon_degrees.0) {
            Radians((self.beta.0 - self.beta.0.clamp(beta_lo.0, beta_hi.0)).abs())
        } else {
            Radians(
                self.to_meridian_arc(Degrees(lon_lo), beta_lo, beta_hi)
                    .0
                    .min(self.to_meridian_arc(Degrees(lon_hi), beta_lo, beta_hi).0),
            )
        };
        self.minor_radius * sigma.0
    }

    /// The planar lon/lat window(s) guaranteed to contain every point within
    /// `distance` meters of the query: one envelope, two when the longitude
    /// window wraps the antimeridian, or `None` when the cap covers the
    /// whole world (callers keep the global scan).
    pub(crate) fn dwithin_windows(&self, distance: f64) -> Option<DwithinWindows> {
        // s ≤ distance ⟹ σ ≤ distance / b (the bound, inverted).
        let alpha = distance / self.minor_radius;
        if alpha >= std::f64::consts::PI {
            return None;
        }
        let beta_lo = Radians(self.beta.0 - alpha);
        let beta_hi = Radians(self.beta.0 + alpha);
        if beta_lo.0 <= -std::f64::consts::FRAC_PI_2 || beta_hi.0 >= std::f64::consts::FRAC_PI_2 {
            // The cap reaches a pole: every longitude qualifies.
            let lat_lo = geodetic_latitude(
                Radians(beta_lo.0.max(-std::f64::consts::FRAC_PI_2)),
                self.one_minus_f,
            );
            let lat_hi = geodetic_latitude(
                Radians(beta_hi.0.min(std::f64::consts::FRAC_PI_2)),
                self.one_minus_f,
            );
            return Some(DwithinWindows::one(AABB::from_corners(
                [-180.0, lat_lo.0],
                [180.0, lat_hi.0],
            )));
        }
        let lat_lo = geodetic_latitude(beta_lo, self.one_minus_f);
        let lat_hi = geodetic_latitude(beta_hi, self.one_minus_f);
        // Spherical-cap rect bound: max longitude deviation where the
        // meridian is tangent to the cap.
        let half_width = (alpha.sin() / self.beta.0.cos())
            .clamp(-1.0, 1.0)
            .asin()
            .to_degrees();
        let lon = self.lon.0.to_degrees();
        let (lon_lo, lon_hi) = (lon - half_width, lon + half_width);
        // Inclusive wrap checks: a window touching ±180 must also cover the
        // seam's other spelling (an entry at lon 180 matches a query at -180).
        if lon_lo <= -180.0 {
            return Some(DwithinWindows::two(
                AABB::from_corners([-180.0, lat_lo.0], [lon_hi, lat_hi.0]),
                AABB::from_corners([(lon_lo + 360.0).min(180.0), lat_lo.0], [180.0, lat_hi.0]),
            ));
        }
        if lon_hi >= 180.0 {
            return Some(DwithinWindows::two(
                AABB::from_corners([lon_lo, lat_lo.0], [180.0, lat_hi.0]),
                AABB::from_corners([-180.0, lat_lo.0], [(lon_hi - 360.0).max(-180.0), lat_hi.0]),
            ));
        }
        Some(DwithinWindows::one(AABB::from_corners(
            [lon_lo, lat_lo.0],
            [lon_hi, lat_hi.0],
        )))
    }
}

#[cfg(test)]
mod tests {
    use geographiclib_rs::{Geodesic, InverseGeodesic as _};
    use rstar::AABB;

    use super::*;

    fn karney(geodesic: &Geodesic, a: (f64, f64), b: (f64, f64)) -> f64 {
        // Distance-only capability: the 4-tuple impl also computes both
        // azimuths, which the pruner discards.
        let distance: f64 = geodesic.inverse(a.1, a.0, b.1, b.0);
        distance
    }

    fn pruner(lon: f64, lat: f64) -> GeodesicPruner {
        GeodesicPruner::new("EPSG:4326", Point::new(lon, lat).expect("finite"))
            .expect("WGS84 is oblate and the point is in domain")
    }

    /// The envelope bound never exceeds the Karney distance to any sampled
    /// point of the rect — including the adversarial equatorial micro-delta,
    /// seam, and polar cases.
    #[test]
    fn envelope_lower_bound_is_sound_against_sampled_karney_distances() {
        let wgs84 = Geodesic::wgs84();
        let cases: &[((f64, f64), [f64; 4])] = &[
            ((0.0, 0.0), [10.0, -1.0, 12.0, 1.0]),
            ((0.0, 1e-5), [-1.0, -1e-4, 1.0, 0.0]), // equatorial micro-delta
            ((179.8, 10.0), [-180.0, 9.8, -179.7, 10.2]), // across the seam
            ((45.0, 89.8), [-180.0, 89.9, 180.0, 90.0]), // polar cap rect
            ((0.0, -89.8), [-10.0, -90.0, 10.0, -89.9]),
            ((0.0, 0.0), [-180.0, -1.0, 180.0, 1.0]), // full-longitude band
            ((-90.0, -45.0), [-89.0, -46.0, -88.0, -44.0]),
        ];
        for &((lon, lat), [lon_lo, lat_lo, lon_hi, lat_hi]) in cases {
            let bound = pruner(lon, lat)
                .envelope_lower_bound(&AABB::from_corners([lon_lo, lat_lo], [lon_hi, lat_hi]));
            assert!(bound.is_finite() && bound >= 0.0);
            for i in 0..=20 {
                for j in 0..=20 {
                    let sample = (
                        lon_lo + (lon_hi - lon_lo) * f64::from(i) / 20.0,
                        lat_lo + (lat_hi - lat_lo) * f64::from(j) / 20.0,
                    );
                    let exact = karney(&wgs84, (lon, lat), sample);
                    assert!(
                        bound <= exact + 1e-6,
                        "unsound: query ({lon}, {lat}) rect sample {sample:?}: \
                         bound {bound} > karney {exact}"
                    );
                }
            }
        }
    }

    /// Reduced and geodetic latitudes are inverses across the domain.
    #[test]
    fn latitude_conversions_round_trip() {
        let one_minus_f = 1.0 - 1.0 / 298.257_223_563;
        for lat in [-90.0, -45.0, -1e-9, 0.0, 30.0, 89.999, 90.0] {
            let beta = reduced_latitude(Degrees(lat), one_minus_f);
            let back = geodetic_latitude(beta, one_minus_f);
            assert!(
                (back.0 - lat).abs() < 1e-9,
                "{lat} -> {} -> {}",
                beta.0,
                back.0
            );
        }
    }

    /// A zero-radius window at the seam covers both spellings of ±180.
    #[test]
    fn seam_window_includes_both_longitude_spellings() {
        let windows = pruner(-180.0, 0.0)
            .dwithin_windows(0.0)
            .expect("zero radius is a window, not the world");
        let covers = |lon: f64| {
            windows
                .iter()
                .any(|window| (window.lower()[0]..=window.upper()[0]).contains(&lon))
        };
        assert!(covers(-180.0) && covers(180.0));
    }

    /// A radius that reaches a pole widens to the full longitude range.
    #[test]
    fn pole_reaching_window_spans_all_longitudes() {
        let windows = pruner(0.0, 89.0)
            .dwithin_windows(500_000.0)
            .expect("regional radius");
        assert_eq!(windows.len(), 1);
        let window = windows.iter().next().expect("one window");
        assert!((window.lower()[0] - -180.0).abs() < f64::EPSILON);
        assert!((window.upper()[0] - 180.0).abs() < f64::EPSILON);
    }

    /// Every sampled point within `distance` of the query lies inside at
    /// least one returned window (or the windows are `None` = whole world).
    #[test]
    fn dwithin_windows_contain_every_point_within_distance() {
        let wgs84 = Geodesic::wgs84();
        let cases: &[((f64, f64), f64)] = &[
            ((179.9, 0.0), 50_000.0), // wraps the seam
            ((-179.999, -45.0), 1_000.0),
            ((0.0, 89.8), 50_000.0), // reaches the pole
            ((0.0, 0.0), 0.0),
            ((10.0, 20.0), 2_000_000.0),
        ];
        for &((lon, lat), distance) in cases {
            let Some(windows) = pruner(lon, lat).dwithin_windows(distance) else {
                continue; // whole-world fallback is trivially sound
            };
            for i in 0..=40 {
                for j in 0..=20 {
                    let sample = (
                        -180.0 + 360.0 * f64::from(i) / 40.0,
                        -90.0 + 180.0 * f64::from(j) / 20.0,
                    );
                    if karney(&wgs84, (lon, lat), sample) > distance {
                        continue;
                    }
                    let covered = windows.iter().any(|window| {
                        let (lower, upper) = (window.lower(), window.upper());
                        (lower[0]..=upper[0]).contains(&sample.0)
                            && (lower[1]..=upper[1]).contains(&sample.1)
                    });
                    assert!(
                        covered,
                        "window miss: query ({lon}, {lat}) d={distance} sample {sample:?}"
                    );
                }
            }
        }
    }
}

/// Per-row auxiliary-sphere caps for the index's one CRS frame: each row
/// stores an anchor (its first vertex, reduced) plus a PROVEN upper bound
/// on the row's angular reach from it in meters (`σ ≤ s/b` turns
/// `s(anchor, segment.start) + length` into a bound that covers bowed
/// geodesic edges — which can leave their lon/lat envelope, the reason the
/// planar R-tree bound is unsound here). Gives a sound lower bound on the
/// geodesic distance from any query cap to any point of the row.
#[derive(Debug)]
pub(crate) struct GeodesicRowCaps {
    minor_radius: f64,
    one_minus_f: f64,
    /// `(β, λ radians)` anchor per row.
    anchors: Vec<(Radians, Radians)>,
    /// Aux reach upper bound (meters) per row.
    reaches: Vec<f64>,
}

impl GeodesicRowCaps {
    /// Assemble from per-row `(anchor, reach_meters)` pairs for the given
    /// `(semi_major, flattening)` ellipsoid (resolved by the CALLER —
    /// `ellipsoid_shape` re-enters the geodesic handle cache and must not
    /// run inside a metric scope). `None` outside the bound's proof domain
    /// (callers fall back to the exact full scan).
    /// Per-handle caps for live index rows only. Tombstoned slots keep
    /// placeholders — `lower_bound` is only called for live handles.
    pub(crate) fn from_live_handles(
        shape: crs::EllipsoidShape,
        row_count: usize,
        live: impl Iterator<Item = (usize, Option<(Point, f64)>)>,
    ) -> Option<Self> {
        let major = shape.semi_major.get();
        let flattening = shape.flattening;
        let one_minus_f = 1.0 - flattening;
        let mut anchors = vec![(Radians(0.0), Radians(0.0)); row_count];
        let mut reaches = vec![0.0; row_count];
        for (handle, row) in live {
            let (anchor, reach) = row?;
            if !(-180.0..=180.0).contains(&anchor.x) || !(-90.0..=90.0).contains(&anchor.y) {
                // Out-of-domain rows must reach the exact path so they raise
                // its domain error instead of being silently pruned.
                return None;
            }
            anchors[handle] = (
                reduced_latitude(Degrees(anchor.y), one_minus_f),
                Radians(anchor.x.to_radians()),
            );
            reaches[handle] = reach;
        }
        Some(Self {
            minor_radius: major * one_minus_f,
            one_minus_f,
            anchors,
            reaches,
        })
    }

    /// The query side of the bound: a reduced anchor for `point`. `None`
    /// out of domain (exact path owns the error).
    pub(crate) fn query_anchor(&self, point: Point) -> Option<(Radians, Radians)> {
        if !(-180.0..=180.0).contains(&point.x) || !(-90.0..=90.0).contains(&point.y) {
            return None;
        }
        Some((
            reduced_latitude(Degrees(point.y), self.one_minus_f),
            Radians(point.x.to_radians()),
        ))
    }

    /// Sound lower bound (meters) on the geodesic distance between any
    /// point within `query_reach` of the query anchor and any point of row
    /// `handle`.
    pub(crate) fn lower_bound(
        &self,
        query: (Radians, Radians),
        query_reach: f64,
        handle: usize,
    ) -> f64 {
        let (beta, lon) = self.anchors[handle];
        let delta = (query.1.0 - lon.0).abs();
        let gap = if delta >= std::f64::consts::TAU {
            delta - std::f64::consts::TAU
        } else {
            delta
        };
        let gap = Radians(gap.min(std::f64::consts::TAU - gap));
        let sigma = central_angle(query.0, beta, gap);
        (self.minor_radius * sigma.0 - self.reaches[handle] - query_reach).max(0.0)
    }
}

impl HeapSize for GeodesicRowCaps {
    fn heap_bytes(&self) -> usize {
        self.anchors.heap_bytes() + self.reaches.heap_bytes()
    }
}
