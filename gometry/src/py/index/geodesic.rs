//! Geodesic lower-bound pruning for the spatial index.
//!
//! Point queries use a point-to-envelope bound; shape queries use a
//! chart-box-to-chart-box bound over the shared geographic index cover.
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
//! Chart-edge shapes stay within their shared chart envelopes, so either bound
//! can prune rows of any shape type. Unsupported frames and invalid raw
//! geodesic coordinates fail open to exact refinement.

use rstar::AABB;

use crate::crs;
use crate::crs::{GeodesicBoxEnvelopeBound, GeodesicChartBox, GeodesicEnvelopeBound};
use crate::geometry::{ChartBox, Point};

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
    const fn len(&self) -> usize {
        match self.second {
            Some(_) => 2,
            None => 1,
        }
    }
}

/// Reduced (parametric) latitude in radians: `tan β = (1−f) tan φ`.
#[cfg(test)]
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

/// Geodesic distance lower bounds from one fixed query point, on one CRS
/// ellipsoid.
pub(crate) struct GeodesicPruner {
    envelope: GeodesicEnvelopeBound,
}

impl GeodesicPruner {
    /// Build a pruner for `point` on the `crs` ellipsoid.
    ///
    /// `None` when the
    /// ellipsoid is not oblate (the bound's proof needs `0 ≤ f < 1`) — callers
    /// fall back to the exact full scan.
    pub(crate) fn new_at(
        crs: &str,
        point: Point,
        state: &crate::crs::RuntimeState,
    ) -> Option<Self> {
        // Out-of-domain coordinates fall back to the exact path so they raise
        // the same domain error every other geodesic spelling raises, instead
        // of being silently pruned to an empty window.
        if !(-180.0..=180.0).contains(&point.x) || !(-90.0..=90.0).contains(&point.y) {
            return None;
        }
        let shape = crs::ellipsoid_shape_at(crs, state).ok()?;
        let major = shape.semi_major.get();
        let flattening = shape.flattening;
        Some(Self {
            envelope: GeodesicEnvelopeBound::new(
                major,
                flattening,
                Point::new_raw_xy(point.x, point.y),
            ),
        })
    }

    /// Lower bound (meters) on the geodesic distance from the query point to
    /// any point inside the lon/lat envelope `env`.
    pub(crate) fn envelope_lower_bound(&self, env: &AABB<[f64; 2]>) -> f64 {
        let (lower, upper) = (env.lower(), env.upper());
        let (lon_lo, lat_lo) = lower.into();
        let (lon_hi, lat_hi) = upper.into();
        self.envelope.lower_bound(ChartBox {
            west: lon_lo,
            south: lat_lo,
            east: lon_hi,
            north: lat_hi,
        })
    }

    /// The planar lon/lat window(s) guaranteed to contain every point within
    /// `distance` meters of the query: one envelope, two when the longitude
    /// window wraps the antimeridian, or `None` when the cap covers the
    /// whole world (callers keep the global scan).
    pub(crate) fn dwithin_windows(&self, distance: f64) -> Option<DwithinWindows> {
        // s ≤ distance ⟹ σ ≤ distance / b (the bound, inverted).
        let alpha = (distance + self.envelope.backend_floor()) / self.envelope.minor_radius();
        if alpha >= std::f64::consts::PI {
            return None;
        }
        let beta_lo = Radians(self.envelope.beta() - alpha);
        let beta_hi = Radians(self.envelope.beta() + alpha);
        if beta_lo.0 <= -std::f64::consts::FRAC_PI_2 || beta_hi.0 >= std::f64::consts::FRAC_PI_2 {
            // The cap reaches a pole: every longitude qualifies.
            let lat_lo = geodetic_latitude(
                Radians(beta_lo.0.max(-std::f64::consts::FRAC_PI_2)),
                self.envelope.one_minus_f(),
            );
            let lat_hi = geodetic_latitude(
                Radians(beta_hi.0.min(std::f64::consts::FRAC_PI_2)),
                self.envelope.one_minus_f(),
            );
            return Some(DwithinWindows::one(AABB::from_corners(
                [-180.0, lat_lo.0],
                [180.0, lat_hi.0],
            )));
        }
        let lat_lo = geodetic_latitude(beta_lo, self.envelope.one_minus_f());
        let lat_hi = geodetic_latitude(beta_hi, self.envelope.one_minus_f());
        // Spherical-cap rect bound: max longitude deviation where the
        // meridian is tangent to the cap.
        let half_width = (alpha.sin() / self.envelope.beta().cos())
            .clamp(-1.0, 1.0)
            .asin()
            .to_degrees();
        let lon = self.envelope.longitude().to_degrees();
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

/// Geodesic lower bounds from a chart-box cover to one index envelope.
pub(crate) struct GeodesicBoxPruner {
    bound: GeodesicBoxEnvelopeBound,
    query: Box<[GeodesicChartBox]>,
}

impl GeodesicBoxPruner {
    pub(crate) fn new(semi_major: f64, flattening: f64, query: &[AABB<[f64; 2]>]) -> Self {
        let bound = GeodesicBoxEnvelopeBound::new(semi_major, flattening);
        let query = query
            .iter()
            .map(|envelope| bound.prepare(chart_box_from_aabb(envelope)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { bound, query }
    }

    pub(crate) fn lower_bound(&self, envelope: &AABB<[f64; 2]>) -> f64 {
        let target = self.bound.prepare(chart_box_from_aabb(envelope));
        let mut lower = f64::INFINITY;
        for &query in &self.query {
            lower = lower.min(self.bound.lower_bound_prepared(query, target));
            if lower == 0.0 {
                return 0.0;
            }
        }
        if lower.is_finite() { lower } else { 0.0 }
    }
}

fn chart_box_from_aabb(envelope: &AABB<[f64; 2]>) -> ChartBox {
    let [west, south] = envelope.lower();
    let [east, north] = envelope.upper();
    ChartBox {
        west,
        south,
        east,
        north,
    }
}

pub(crate) enum GeodesicIndexPruner {
    Point(GeodesicPruner),
    Boxes(GeodesicBoxPruner),
}

impl GeodesicIndexPruner {
    pub(crate) fn lower_bound(&self, envelope: &AABB<[f64; 2]>) -> f64 {
        match self {
            Self::Point(pruner) => pruner.envelope_lower_bound(envelope),
            Self::Boxes(pruner) => pruner.lower_bound(envelope),
        }
    }
}

#[cfg(test)]
mod tests {
    use geographiclib_rs::{Geodesic, InverseGeodesic as _};

    use super::*;

    fn karney(geodesic: &Geodesic, a: (f64, f64), b: (f64, f64)) -> f64 {
        // Distance-only capability: the 4-tuple impl also computes both
        // azimuths, which the pruner discards.
        let distance: f64 = geodesic.inverse(a.1, a.0, b.1, b.0);
        distance
    }

    fn pruner(lon: f64, lat: f64) -> GeodesicPruner {
        let state = crs::runtime_state_snapshot().expect("CRS runtime state is readable");
        GeodesicPruner::new_at("EPSG:4326", Point::new(lon, lat).expect("finite"), &state)
            .expect("WGS84 is oblate and the point is in domain")
    }

    /// The envelope bound never exceeds the Karney distance to any sampled
    /// point of the rect — including the adversarial equatorial micro-delta,
    /// seam, and polar cases.
    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
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
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
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
