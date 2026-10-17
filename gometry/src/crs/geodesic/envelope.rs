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
    #[expect(
        clippy::float_cmp,
        reason = "full longitude uses its exact canonical sentinel bounds"
    )]
    pub(crate) fn lower_bound(&self, bounds: ChartBox) -> f64 {
        if self.one_minus_f > 1.0 {
            return 0.0;
        }

        let (beta_south, beta_north) = reduced_latitude_bounds(bounds, self.one_minus_f);
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

/// Auxiliary-sphere lower bound between two chart rectangles.
///
/// Each rectangle maps to a patch on the unit auxiliary sphere. The patch is
/// enclosed by an outward-rounded Cartesian box; the gap between those boxes
/// lower-bounds the chord between any two patch points. Converting that chord
/// to a central angle and multiplying by the semi-minor axis gives an
/// oblate-ellipsoid geodesic lower bound.
pub(crate) struct GeodesicBoxEnvelopeBound {
    minor_radius: f64,
    backend_floor: f64,
    one_minus_f: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct GeodesicChartBox(UnitBox);

#[derive(Clone, Copy)]
struct UnitBox {
    x: (f64, f64),
    y: (f64, f64),
    z: (f64, f64),
}

impl GeodesicBoxEnvelopeBound {
    pub(crate) fn new(semi_major: f64, flattening: f64) -> Self {
        let policy = GeodesicNumericalPolicy::new(semi_major);
        let one_minus_f = 1.0 - flattening;
        Self {
            minor_radius: semi_major * one_minus_f,
            backend_floor: policy.backend_floor(),
            one_minus_f,
        }
    }

    /// Lower bound (meters) between every point in the two chart boxes.
    ///
    /// Prolate ellipsoids use zero because the auxiliary-sphere inequality
    /// does not provide a lower bound there.
    #[cfg(test)]
    pub(crate) fn lower_bound(&self, first: ChartBox, second: ChartBox) -> f64 {
        if self.one_minus_f > 1.0 {
            return 0.0;
        }
        self.lower_bound_prepared(self.prepare(first), self.prepare(second))
    }

    pub(crate) fn prepare(&self, bounds: ChartBox) -> GeodesicChartBox {
        GeodesicChartBox(unit_box(bounds, self.one_minus_f))
    }

    pub(crate) fn lower_bound_prepared(
        &self,
        first: GeodesicChartBox,
        second: GeodesicChartBox,
    ) -> f64 {
        if self.one_minus_f > 1.0 {
            return 0.0;
        }
        let first = first.0;
        let second = second.0;
        let chord = component_gap(first.x, second.x)
            .hypot(component_gap(first.y, second.y))
            .hypot(component_gap(first.z, second.z))
            .min(2.0);
        let angle = 2.0 * (chord / 2.0).asin();
        (self.minor_radius * angle - self.backend_floor).max(0.0)
    }
}

fn unit_box(bounds: ChartBox, one_minus_f: f64) -> UnitBox {
    let (beta_south, beta_north) = reduced_latitude_bounds(bounds, one_minus_f);
    let abs_min = if beta_south <= 0.0 && beta_north >= 0.0 {
        0.0
    } else {
        beta_south.abs().min(beta_north.abs())
    };
    let cos_latitude = (beta_south.abs().max(beta_north.abs()).cos(), abs_min.cos());
    let mut longitude = longitude_windows(bounds).into_iter().flatten();
    let first = longitude.next().expect("chart box has longitude coverage");
    let first_sine = sin_range(first);
    let first_cosine = cos_range(first);
    let (sin_longitude, cos_longitude) =
        longitude
            .next()
            .map_or((first_sine, first_cosine), |second| {
                (
                    range_union(first_sine, sin_range(second)),
                    range_union(first_cosine, cos_range(second)),
                )
            });
    UnitBox {
        x: outward(product_range(cos_longitude, cos_latitude)),
        y: outward(product_range(sin_longitude, cos_latitude)),
        z: outward((beta_south.sin(), beta_north.sin())),
    }
}

fn sin_range([west, east]: [f64; 2]) -> (f64, f64) {
    trig_range(west, east, false)
}

fn cos_range([west, east]: [f64; 2]) -> (f64, f64) {
    trig_range(west, east, true)
}

fn trig_range(west: f64, east: f64, cosine: bool) -> (f64, f64) {
    if east - west >= 360.0 {
        return (-1.0, 1.0);
    }
    let to_radians = f64::to_radians;
    let start = if cosine {
        to_radians(west).cos()
    } else {
        to_radians(west).sin()
    };
    let end = if cosine {
        to_radians(east).cos()
    } else {
        to_radians(east).sin()
    };
    let mut lower = start.min(end);
    let mut upper = start.max(end);
    let (minimum_phase, maximum_phase) = if cosine { (180.0, 0.0) } else { (-90.0, 90.0) };
    if contains_periodic_phase(west, east, minimum_phase) {
        lower = -1.0;
    }
    if contains_periodic_phase(west, east, maximum_phase) {
        upper = 1.0;
    }
    (lower, upper)
}

fn contains_periodic_phase(west: f64, east: f64, phase: f64) -> bool {
    let first = ((west - phase) / 360.0).ceil() * 360.0 + phase;
    first <= east
}

const fn range_union(first: (f64, f64), second: (f64, f64)) -> (f64, f64) {
    (first.0.min(second.0), first.1.max(second.1))
}

fn product_range(first: (f64, f64), second: (f64, f64)) -> (f64, f64) {
    let products = [
        first.0 * second.0,
        first.0 * second.1,
        first.1 * second.0,
        first.1 * second.1,
    ];
    let mut lower = f64::INFINITY;
    let mut upper = f64::NEG_INFINITY;
    for product in products {
        lower = lower.min(product);
        upper = upper.max(product);
    }
    (lower, upper)
}

fn outward((lower, upper): (f64, f64)) -> (f64, f64) {
    const ROUNDING_PAD: f64 = 64.0 * f64::EPSILON;
    (
        (lower - ROUNDING_PAD).max(-1.0),
        (upper + ROUNDING_PAD).min(1.0),
    )
}

fn component_gap(first: (f64, f64), second: (f64, f64)) -> f64 {
    if first.1 < second.0 {
        second.0 - first.1
    } else if second.1 < first.0 {
        first.0 - second.1
    } else {
        0.0
    }
}

fn reduced_latitude_bounds(bounds: ChartBox, one_minus_f: f64) -> (f64, f64) {
    let south = reduced_latitude(bounds.south, one_minus_f);
    let north = reduced_latitude(bounds.north, one_minus_f);
    // Independent transcendental evaluations can reverse equal or adjacent inputs.
    (south.min(north), south.max(north))
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

#[cfg(test)]
mod tests {
    use geographiclib_rs::{Geodesic, InverseGeodesic as _};

    use super::*;

    fn box_(west: f64, south: f64, east: f64, north: f64) -> ChartBox {
        ChartBox {
            west,
            south,
            east,
            north,
        }
    }

    fn samples(bounds: ChartBox) -> Vec<(f64, f64)> {
        let mut points = Vec::new();
        for [west, east] in longitude_windows(bounds).into_iter().flatten() {
            for longitude_index in 0..=2 {
                let fraction = f64::from(longitude_index) / 2.0;
                let longitude = west + (east - west) * fraction;
                for latitude_index in 0..=2 {
                    let fraction = f64::from(latitude_index) / 2.0;
                    let latitude = bounds.south + (bounds.north - bounds.south) * fraction;
                    points.push((longitude, latitude));
                }
            }
        }
        points
    }

    fn sampled_distance(geodesic: &Geodesic, first: ChartBox, second: ChartBox) -> f64 {
        let first_points = samples(first);
        let second_points = samples(second);
        let mut minimum = f64::INFINITY;
        for &(first_longitude, first_latitude) in &first_points {
            for &(second_longitude, second_latitude) in &second_points {
                let distance: f64 = geodesic.inverse(
                    first_latitude,
                    first_longitude,
                    second_latitude,
                    second_longitude,
                );
                minimum = minimum.min(distance);
            }
        }
        minimum
    }

    #[test]
    fn flat_latitude_envelopes_keep_ordered_bounds_below_attained_distances() {
        let geodesic = Geodesic::wgs84();
        let query = Point::new_raw_xy(0.0, 0.0);
        let bound = GeodesicEnvelopeBound::new(geodesic.a, geodesic.f, query);
        for latitude in [-80.0_f64, -1.0, 1.0, 80.0] {
            for north in [latitude, latitude.next_up()] {
                for longitude in [0.0, 10.0] {
                    let bounds = box_(longitude, latitude, longitude, north);
                    let lower = bound.lower_bound(bounds);
                    let attained: f64 = geodesic.inverse(0.0, 0.0, latitude, longitude);
                    assert!(lower.is_finite());
                    assert!(lower >= 0.0);
                    assert!(lower <= attained, "{bounds:?}: {lower} > {attained}");
                }
            }
        }
    }

    #[test]
    fn chart_box_lower_bound_stays_below_sampled_ellipsoidal_distances() {
        let geodesic = Geodesic::wgs84();
        let bound = GeodesicBoxEnvelopeBound::new(geodesic.a, geodesic.f);
        let cases = [
            (box_(-1.0, -1.0, 1.0, 1.0), box_(9.0, -1.0, 11.0, 1.0)),
            (box_(0.0, 0.0, 1.0, 1.0), box_(0.0, 3.0, 1.0, 4.0)),
            (
                box_(179.0, -1.0, 180.0, 1.0),
                box_(-180.0, -1.0, -179.0, 1.0),
            ),
            (box_(179.0, -1.0, -179.0, 1.0), box_(-1.0, -1.0, 1.0, 1.0)),
            (box_(-180.0, 80.0, 180.0, 90.0), box_(-1.0, 0.0, 1.0, 1.0)),
            (box_(-180.0, -1.0, 180.0, 1.0), box_(20.0, 10.0, 21.0, 11.0)),
            (box_(0.0, 80.0, 1.0, 81.0), box_(10.0, 80.0, 11.0, 81.0)),
            (box_(-10.0, -5.0, 10.0, 5.0), box_(9.0, 4.0, 11.0, 6.0)),
            (box_(0.0, 0.0, 0.0, 0.0), box_(90.0, 0.0, 90.0, 0.0)),
            (
                box_(0.0, 0.0, 0.0, 0.0),
                box_(179.999, 0.001, 179.999, 0.001),
            ),
            (
                box_(179.999, 80.0, 179.999, 80.0),
                box_(-179.999, 80.0, -179.999, 80.0),
            ),
        ];
        for (first, second) in cases {
            let lower_bound = bound.lower_bound(first, second);
            let sampled = sampled_distance(&geodesic, first, second);
            assert!(
                lower_bound <= sampled + 1e-6,
                "bound {lower_bound} exceeds sampled distance {sampled}: {first:?} / {second:?}"
            );
        }
    }

    #[test]
    fn chart_box_lower_bound_is_useful_and_keeps_overlaps_at_zero() {
        let geodesic = Geodesic::wgs84();
        let bound = GeodesicBoxEnvelopeBound::new(geodesic.a, geodesic.f);
        assert!(bound.lower_bound(box_(0.0, -1.0, 1.0, 1.0), box_(10.0, -1.0, 11.0, 1.0)) > 0.0);
        assert_eq!(
            bound.lower_bound(box_(0.0, -1.0, 2.0, 1.0), box_(1.0, 0.0, 3.0, 2.0)),
            0.0
        );
        assert_eq!(
            bound.lower_bound(box_(180.0, 0.0, 180.0, 0.0), box_(-180.0, 0.0, -180.0, 0.0)),
            0.0
        );
    }

    #[test]
    fn prolate_chart_box_lower_bound_fails_open() {
        let bound = GeodesicBoxEnvelopeBound::new(6_378_137.0, -1.0 / 300.0);
        assert_eq!(
            bound.lower_bound(box_(0.0, 0.0, 1.0, 1.0), box_(10.0, 0.0, 11.0, 1.0)),
            0.0
        );
    }
}
