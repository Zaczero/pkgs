//! Shared auxiliary-sphere lower bound from a geographic point to a chart box.

use crate::geometry::{ChartBox, GeodesicNumericalPolicy, Point};

/// Reusable point-to-box lower bound for one geographic query point.
pub(crate) struct GeodesicEnvelopeBound {
    minor_radius: f64,
    backend_floor: f64,
    one_minus_f: f64,
    beta: f64,
    longitude: f64,
}

impl GeodesicEnvelopeBound {
    pub(crate) fn new(semi_major: f64, flattening: f64, point: Point) -> Self {
        let one_minus_f = 1.0 - flattening;
        Self {
            minor_radius: semi_major * one_minus_f,
            backend_floor: GeodesicNumericalPolicy::new(semi_major).backend_floor(),
            one_minus_f,
            beta: reduced_latitude(point.y, one_minus_f),
            longitude: point.x.to_radians(),
        }
    }

    pub(crate) const fn minor_radius(&self) -> f64 {
        self.minor_radius
    }

    pub(crate) const fn backend_floor(&self) -> f64 {
        self.backend_floor
    }

    pub(crate) const fn one_minus_f(&self) -> f64 {
        self.one_minus_f
    }

    pub(crate) const fn beta(&self) -> f64 {
        self.beta
    }

    pub(crate) const fn longitude(&self) -> f64 {
        self.longitude
    }

    /// Lower bound (meters) on the distance to every point in `bounds`.
    ///
    /// The prolate case deliberately returns zero: `b·σ` is not a lower
    /// bound there. `ChartBox` keeps a seam-crossing longitude interval as
    /// `west > east`, and a full-longitude band as `[-180, 180]`.
    pub(crate) fn lower_bound(&self, bounds: ChartBox) -> f64 {
        if self.one_minus_f > 1.0 {
            return 0.0;
        }

        let beta_south = reduced_latitude(bounds.south, self.one_minus_f);
        let beta_north = reduced_latitude(bounds.north, self.one_minus_f);
        let query_longitude = self.longitude.to_degrees();
        let full_longitude = bounds.west == -180.0 && bounds.east == 180.0;
        let sigma = if full_longitude || longitude_contains(bounds, query_longitude) {
            (self.beta - self.beta.clamp(beta_south, beta_north)).abs()
        } else {
            longitude_windows(bounds)
                .into_iter()
                .flatten()
                .map(|[west, east]| {
                    self.to_meridian_arc(west, beta_south, beta_north)
                        .min(self.to_meridian_arc(east, beta_south, beta_north))
                })
                .fold(f64::INFINITY, f64::min)
        };
        (self.minor_radius * sigma - self.backend_floor).max(0.0)
    }

    fn lon_gap(&self, longitude: f64) -> f64 {
        // Both longitudes are canonical degrees, so one conditional period
        // correction is exact and avoids a floating remainder in each visit.
        let delta = (longitude.to_radians() - self.longitude).abs();
        let gap = if delta >= std::f64::consts::TAU {
            delta - std::f64::consts::TAU
        } else {
            delta
        };
        gap.min(std::f64::consts::TAU - gap)
    }

    fn to_meridian_arc(&self, longitude: f64, beta_south: f64, beta_north: f64) -> f64 {
        let delta = self.lon_gap(longitude);
        let mut best = central_angle(self.beta, beta_south, delta)
            .min(central_angle(self.beta, beta_north, delta));
        if delta < std::f64::consts::FRAC_PI_2 && self.beta.abs() < std::f64::consts::FRAC_PI_2 {
            let foot = (self.beta.tan() / delta.cos()).atan();
            if (beta_south..=beta_north).contains(&foot) {
                let cross_track = (delta.sin().abs() * self.beta.cos()).clamp(0.0, 1.0).asin();
                best = best.min(cross_track);
            }
        }
        best
    }
}

fn reduced_latitude(latitude_degrees: f64, one_minus_f: f64) -> f64 {
    let phi = latitude_degrees.to_radians();
    (one_minus_f * phi.tan())
        .atan()
        .clamp(-std::f64::consts::FRAC_PI_2, std::f64::consts::FRAC_PI_2)
}

fn central_angle(beta1: f64, beta2: f64, delta_lon: f64) -> f64 {
    let sin_lat = ((beta2 - beta1) / 2.0).sin();
    let sin_lon = (delta_lon / 2.0).sin();
    let haversine =
        (sin_lon * sin_lon * beta1.cos() * beta2.cos() + sin_lat * sin_lat).clamp(0.0, 1.0);
    2.0 * haversine.sqrt().asin()
}

fn longitude_contains(bounds: ChartBox, longitude: f64) -> bool {
    if bounds.west <= bounds.east {
        (bounds.west..=bounds.east).contains(&longitude)
    } else {
        longitude >= bounds.west || longitude <= bounds.east
    }
}

fn longitude_windows(bounds: ChartBox) -> [Option<[f64; 2]>; 2] {
    if bounds.west <= bounds.east {
        [Some([bounds.west, bounds.east]), None]
    } else {
        [Some([bounds.west, 180.0]), Some([-180.0, bounds.east])]
    }
}
