//! Shared chart-edge geometry, measures, and arclength parametrization.
//!
//! Longitude direction comes from `PeriodicAxis::shortest_edge`; latitude stays
//! in native units until pole-referenced colatitude is formed.

use geographiclib_rs::Geodesic;

use crate::error::Result;
use crate::geographic::{
    CircularInterval, EdgeDirection, GeographicFrame, Latitude, LongitudeWindows, SeamIdentity,
};
use crate::geometry::types::ChartMetric;
use crate::geometry::{
    ChartBox, GeometryErrorKind, MOrdinate, Point, PointEdgeQuery, PointEdgeQueryResult, ZOrdinate,
    interpolate_optional_ordinate,
};

const GL4_NODES: [f64; 4] = [
    0.069_431_844_202_973_71,
    0.330_009_478_207_571_87,
    0.669_990_521_792_428_1,
    0.930_568_155_797_026_2,
];
const GL4_WEIGHTS: [f64; 4] = [
    0.173_927_422_568_726_93,
    0.326_072_577_431_273_07,
    0.326_072_577_431_273_07,
    0.173_927_422_568_726_93,
];

const GK15_NODES: [f64; 8] = [
    0.991_455_371_120_812_6,
    0.949_107_912_342_758_5,
    0.864_864_423_359_769_1,
    0.741_531_185_599_394_5,
    0.586_087_235_467_691_1,
    0.405_845_151_377_397_2,
    0.207_784_955_007_898_48,
    0.0,
];
const GK15_WEIGHTS: [f64; 8] = [
    0.022_935_322_010_529_224,
    0.063_092_092_629_978_55,
    0.104_790_010_322_250_18,
    0.140_653_259_715_525_92,
    0.169_004_726_639_267_9,
    0.190_350_578_064_785_42,
    0.204_432_940_075_298_89,
    0.209_482_141_084_727_82,
];
const G7_WEIGHTS: [f64; 4] = [
    0.129_484_966_168_869_7,
    0.279_705_391_489_276_67,
    0.381_830_050_505_118_9,
    0.417_959_183_673_469_4,
];

const MAX_GK15_EVALUATIONS: usize = 5_000;
const RELATIVE_LENGTH_ERROR: f64 = 4.0 * f64::EPSILON;
const ONE_DEGREE: f64 = std::f64::consts::PI / 180.0;
const MAX_AUTHALIC_HARMONICS: usize = 9;
const AUTHALIC_TAIL_RELATIVE: f64 = 1.0e-17;

#[derive(Clone, Copy, Debug)]
/// Truncated authalic-q sine series cached with its ellipsoid's geodesic.
pub(crate) struct AuthalicSeries {
    coefficients: [f64; MAX_AUTHALIC_HARMONICS],
    harmonics: usize,
    polar_q: f64,
}

impl AuthalicSeries {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the validated flattening domain bounds harmonics to nine and the largest binomial row to sixty-nine"
    )]
    pub(crate) fn new(geodesic: &Geodesic) -> Self {
        let e2 = geodesic.f * (2.0 - geodesic.f);
        let ratio = (e2 / 4.0).abs();
        let mut harmonics: usize = 1;
        while ratio.powi(harmonics as i32) >= AUTHALIC_TAIL_RELATIVE {
            harmonics = harmonics.wrapping_add(1);
        }

        let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
        for (k, coefficient) in coefficients.iter_mut().enumerate().take(harmonics) {
            let mut sum = 0.0;
            let mut scaled_power = (e2 / 4.0).powi(k as i32);
            for j in k..3 * harmonics + 8 {
                let ratio = (2 * j + 2) as f64 / (2 * j + 1) as f64;
                sum += scaled_power * ratio * binomial(2 * j + 1, j - k);
                scaled_power *= e2 / 4.0;
            }
            *coefficient = (1.0 - e2) * sum * if k % 2 == 0 { 1.0 } else { -1.0 };
        }

        let mut polar_q = 0.0;
        let mut correction = 0.0;
        for (k, &coefficient) in coefficients.iter().take(harmonics).enumerate() {
            let value = if k % 2 == 0 {
                coefficient
            } else {
                -coefficient
            };
            add_compensated(&mut polar_q, &mut correction, value);
        }
        Self {
            coefficients,
            harmonics,
            polar_q,
        }
    }

    pub(crate) fn area_factor(self, semi_major: f64) -> f64 {
        semi_major * semi_major / 2.0
    }

    pub(crate) fn surface_area(self, semi_major: f64) -> f64 {
        2.0 * std::f64::consts::PI * semi_major * semi_major * self.polar_q
    }
}

fn binomial(n: usize, k: usize) -> f64 {
    let k = k.min(n - k);
    let mut coefficient = 1.0;
    for index in 1..=k {
        coefficient *= (n + 1 - index) as f64 / index as f64;
    }
    coefficient
}

fn add_compensated(sum: &mut f64, correction: &mut f64, value: f64) {
    let adjusted = value - *correction;
    let next = *sum + adjusted;
    *correction = (next - *sum) - adjusted;
    *sum = next;
}

fn one_minus_sinc_small(value: f64) -> f64 {
    let squared = value * value;
    squared / 6.0
        * (1.0
            - squared / 20.0
                * (1.0
                    - squared / 42.0
                        * (1.0
                            - squared / 72.0 * (1.0 - squared / 110.0 * (1.0 - squared / 156.0)))))
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartEdge {
    frame: GeographicFrame,
    meridian: Option<MeridianCoordinates>,
    seam_meridian: bool,
    start_longitude: f64,
    end_longitude: f64,
    start_latitude: f64,
    end_latitude: f64,
    latitude_delta: f64,
    longitude_delta_native: f64,
    longitude_delta: f64,
    latitude_unit: f64,
    pole: f64,
}

#[derive(Clone, Copy, Debug)]
struct MeridianCoordinates {
    longitude: f64,
    start_latitude: f64,
    end_latitude: f64,
}

impl ChartEdge {
    pub(crate) fn new(start: Point, end: Point, frame: GeographicFrame) -> Result<Self> {
        let axis = frame.longitude();
        let longitude_delta_native = match axis
            .shortest_edge(start.x, end.x)
            .expect("chart-edge coordinates were admitted before measurement")
        {
            EdgeDirection::Minor(delta) => delta,
            EdgeDirection::HalfTurn(direction) => {
                return Err(GeometryErrorKind::parameter(
                    "longitude",
                    EdgeDirection::HalfTurn(direction).delta(axis),
                    "edge spans exactly half the longitude period; add a vertex",
                ));
            },
        };
        let start_latitude = Latitude::from_admitted(frame, start.y).get();
        let end_latitude = Latitude::from_admitted(frame, end.y).get();
        let latitude_delta = end_latitude - start_latitude;
        let seam_meridian = is_seam_longitude(axis, start.x) && is_seam_longitude(axis, end.x);
        let meridian = if longitude_delta_native == 0.0 && latitude_delta != 0.0 {
            let (longitude, start_latitude) = frame
                .to_geodesic_xy(start.x, start_latitude)
                .expect("chart-edge coordinates were admitted before measurement");
            let (_, end_latitude) = frame
                .to_geodesic_xy(end.x, end_latitude)
                .expect("chart-edge coordinates were admitted before measurement");
            Some(MeridianCoordinates {
                longitude,
                start_latitude,
                end_latitude,
            })
        } else {
            None
        };

        Ok(Self {
            frame,
            meridian,
            seam_meridian,
            start_longitude: start.x,
            end_longitude: end.x,
            start_latitude,
            end_latitude,
            latitude_delta,
            longitude_delta_native,
            longitude_delta: longitude_delta_native * axis.unit().radians_per_unit(),
            latitude_unit: frame.latitude_unit().radians_per_unit(),
            pole: frame.pole(),
        })
    }

    pub(super) fn geodetic_position(self, parameter: f64) -> Result<Point> {
        let point = self.position(parameter);
        let (longitude, latitude) = self
            .frame
            .to_geodesic_xy(point.x, point.y)
            .expect("an admitted chart-edge position stays in its geographic frame");
        Ok(Point::new_raw_xy(longitude, latitude))
    }

    pub(super) fn bounds(self) -> ChartBox {
        let (start_longitude, start_latitude) = self
            .frame
            .to_geodesic_xy(self.start_longitude, self.start_latitude)
            .expect("chart-edge endpoints were admitted");
        let (end_longitude, end_latitude) = self
            .frame
            .to_geodesic_xy(self.end_longitude, self.end_latitude)
            .expect("chart-edge endpoints were admitted");
        let south = start_latitude.min(end_latitude).next_down().max(-90.0);
        let north = start_latitude.max(end_latitude).next_up().min(90.0);
        let axis = GeographicFrame::crs84().longitude();
        let interval = if start_latitude.abs() == 90.0 || end_latitude.abs() == 90.0 {
            CircularInterval::Full
        } else {
            match axis
                .shortest_edge(start_longitude, end_longitude)
                .expect("canonical chart-edge endpoints are admitted")
            {
                EdgeDirection::Minor(delta) => {
                    let west = if delta < 0.0 {
                        end_longitude
                    } else {
                        start_longitude
                    };
                    let sweep = delta.abs() + 64.0 * f64::EPSILON * 360.0;
                    CircularInterval::try_arc(axis, west, sweep)
                        .expect("a chart edge spans less than half a longitude period")
                },
                EdgeDirection::HalfTurn(_) => CircularInterval::Full,
            }
        };
        let (west, east) = match interval.windows(axis) {
            Some(LongitudeWindows::One([west, east])) => (west, east),
            Some(LongitudeWindows::Two([west, _], [_, east])) => (west, east),
            Some(LongitudeWindows::Full) | None => (-180.0, 180.0),
        };
        ChartBox {
            west,
            south,
            east,
            north,
        }
    }

    pub(super) fn is_geodesically_degenerate(self, geodesic: &Geodesic) -> bool {
        self.derivatives(0.5, geodesic).speed == 0.0
    }

    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "the azimuth derivative is the 2D cross product divided by squared speed"
    )]
    pub(super) fn derivatives(self, parameter: f64, geodesic: &Geodesic) -> ChartEdgeDerivatives {
        let e2 = geodesic.f * (2.0 - geodesic.f);
        let dphi = self.latitude_delta * self.latitude_unit;
        let dlambda = self.longitude_delta;
        let (sin_latitude, cos_latitude) = self.latitude_sincos(parameter);
        let w2 = 1.0 - e2 * sin_latitude * sin_latitude;
        let w = w2.sqrt();
        let prime_vertical = geodesic.a / w;
        let meridian = geodesic.a * (1.0 - e2) / (w2 * w);
        let east = prime_vertical * cos_latitude * dlambda;
        let north = meridian * dphi;
        let speed = east.hypot(north);
        if speed == 0.0 {
            return ChartEdgeDerivatives {
                speed,
                acceleration: 0.0,
                azimuth: 0.0,
                geodesic_curvature: 0.0,
            };
        }
        let east_derivative = -meridian * sin_latitude * dphi * dlambda;
        let north_derivative = dphi * dphi * 3.0 * e2 * sin_latitude * cos_latitude * meridian / w2;
        let azimuth = east.atan2(north);
        let azimuth_derivative =
            (east_derivative * north - east * north_derivative) / (speed * speed);
        let geodesic_curvature = (azimuth_derivative - sin_latitude * dlambda) / speed;
        ChartEdgeDerivatives {
            speed,
            acceleration: east_derivative.hypot(north_derivative),
            azimuth,
            geodesic_curvature,
        }
    }

    pub(super) fn sampled_derivative_bounds(
        self,
        geodesic: &Geodesic,
    ) -> ChartEdgeDerivativeBounds {
        const SAMPLE_PARAMETERS: [f64; 9] = [0.0, 0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1.0];
        let mut speed = 0.0_f64;
        let mut acceleration = 0.0_f64;
        let mut curvature = 0.0_f64;
        for parameter in SAMPLE_PARAMETERS {
            let derivative = self.derivatives(parameter, geodesic);
            speed = speed.max(derivative.speed);
            acceleration = acceleration.max(derivative.acceleration);
            curvature = curvature.max(derivative.geodesic_curvature.abs());
        }
        ChartEdgeDerivativeBounds {
            speed: speed * 1.05,
            acceleration: acceleration * 1.05,
            curvature: curvature * 1.05,
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "the exact endpoint parameters reuse the edge's already-computed length"
    )]
    pub(super) fn length_to(self, parameter: f64, geodesic: &Geodesic) -> Result<f64> {
        if parameter == 0.0 {
            return Ok(0.0);
        }
        if parameter == 1.0 {
            return self.length(geodesic);
        }
        self.integrate(0.0, parameter, geodesic)
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact minimizer endpoint parameters preserve input coordinates and ordinates"
    )]
    pub(super) fn witness(
        self,
        start: Point,
        end: Point,
        parameter: f64,
        geodesic: &Geodesic,
    ) -> Result<Point> {
        if parameter == 0.0 {
            return Ok(start);
        }
        if parameter == 1.0 {
            return Ok(end);
        }
        let total = self.length(geodesic)?;
        let fraction = if total == 0.0 {
            0.0
        } else {
            (self.length_to(parameter, geodesic)? / total).clamp(0.0, 1.0)
        };
        let position = self.position(parameter);
        let longitude = self
            .frame
            .longitude()
            .canonicalize(position.x)
            .expect("an admitted chart-edge witness has finite longitude");
        Ok(Point::new_axes(
            longitude.get(),
            position.y,
            ZOrdinate(interpolate_optional_ordinate(start.z(), end.z(), fraction)),
            MOrdinate(interpolate_optional_ordinate(start.m(), end.m(), fraction)),
        )
        .expect("an admitted chart-edge witness is finite"))
    }

    pub(super) fn seam_span(&self) -> Option<[f64; 2]> {
        self.seam_meridian
            .then_some([self.start_latitude, self.end_latitude])
    }

    pub(crate) fn length(&self, geodesic: &Geodesic) -> Result<f64> {
        if self.latitude_delta == 0.0 {
            let colatitude = self.pole - self.start_latitude.abs();
            let theta = colatitude * self.latitude_unit;
            let cos_latitude = theta.sin();
            let sin_latitude = theta.cos().copysign(self.start_latitude);
            let w = (1.0 - geodesic.f * (2.0 - geodesic.f) * sin_latitude * sin_latitude).sqrt();
            let result = geodesic.a / w * cos_latitude * self.longitude_delta.abs();
            return self.finite_length(result);
        }

        if let Some(meridian) = self.meridian {
            return self.finite_length(super::inverse_distance(
                geodesic,
                meridian.longitude,
                meridian.start_latitude,
                meridian.longitude,
                meridian.end_latitude,
            ));
        }

        self.integrate_full(geodesic)
    }

    pub(crate) fn is_parallel(&self) -> bool {
        self.latitude_delta == 0.0
    }

    /// Ellipsoidal arclength from the stored start to parameter `t`.
    pub(crate) fn arclength(
        &self,
        t: f64,
        geodesic: &Geodesic,
        frame: GeographicFrame,
    ) -> Result<f64> {
        debug_assert!((0.0..=1.0).contains(&t));
        if t <= 0.0 {
            return Ok(0.0);
        }
        if t >= 1.0 {
            return self.length(geodesic);
        }
        if self.latitude_delta == 0.0 {
            return self.finite_length(self.length(geodesic)? * t);
        }
        if let Some(meridian) = self.meridian {
            let position = self.position(t);
            let (_, latitude) = frame
                .to_geodesic_xy(position.x, position.y)
                .expect("chart-edge position belongs to its admitted frame");
            return self.finite_length(super::inverse_distance(
                geodesic,
                meridian.longitude,
                meridian.start_latitude,
                meridian.longitude,
                latitude,
            ));
        }
        self.integrate(0.0, t, geodesic)
    }

    /// Parameter whose accumulated ellipsoidal arclength is `distance`.
    pub(crate) fn locate_t(
        &self,
        distance: f64,
        length: f64,
        geodesic: &Geodesic,
        frame: GeographicFrame,
    ) -> Result<f64> {
        if distance <= 0.0 || length == 0.0 {
            return Ok(0.0);
        }
        if distance >= length {
            return Ok(1.0);
        }
        if self.latitude_delta == 0.0 {
            return Ok(distance / length);
        }

        let dphi = self.latitude_delta * self.latitude_unit;
        let e2 = geodesic.f * (2.0 - geodesic.f);
        let mut lower = 0.0;
        let mut upper = 1.0;
        let mut parameter = distance / length;
        let mut best = (parameter, f64::INFINITY);
        let tolerance = RELATIVE_LENGTH_ERROR * length;

        for _ in 0..64 {
            let arclength = self.arclength(parameter, geodesic, frame)?;
            let residual = arclength - distance;
            let error = residual.abs();
            if error < best.1 {
                best = (parameter, error);
            }
            if error <= tolerance {
                return Ok(parameter);
            }
            if residual > 0.0 {
                upper = parameter;
            } else {
                lower = parameter;
            }

            let speed = self.speed(parameter, geodesic.a, e2, dphi);
            let newton = parameter - residual / speed;
            let midpoint = f64::midpoint(lower, upper);
            parameter = if newton > lower && newton < upper {
                newton
            } else {
                midpoint
            };
            if parameter == lower || parameter == upper {
                break;
            }
        }

        Ok(best.0)
    }

    /// Position on the chart edge in its native angular frame.
    pub(crate) fn position(&self, t: f64) -> crate::geometry::Point {
        let longitude = self.start_longitude + t * self.longitude_delta_native;
        let latitude = crate::geometry::interpolate_subdivision_ordinate(
            self.start_latitude,
            self.end_latitude,
            t,
        );
        crate::geometry::Point::new_raw_xy(longitude, latitude)
    }

    fn integrate(&self, start: f64, end: f64, geodesic: &Geodesic) -> Result<f64> {
        self.integrate_interval::<false>(start, end, geodesic)
    }

    fn integrate_full(&self, geodesic: &Geodesic) -> Result<f64> {
        self.integrate_interval::<true>(0.0, 1.0, geodesic)
    }

    #[expect(
        clippy::float_cmp,
        reason = "only an exactly zero-width partial interval has zero arclength"
    )]
    fn integrate_interval<const FULL: bool>(
        &self,
        start: f64,
        end: f64,
        geodesic: &Geodesic,
    ) -> Result<f64> {
        if !FULL && start == end {
            return Ok(0.0);
        }
        let e2 = geodesic.f * (2.0 - geodesic.f);
        let dphi = self.latitude_delta * self.latitude_unit;
        if self.admits_gl4(dphi) {
            let width = if FULL { 1.0 } else { end - start };
            let value = std::iter::zip(GL4_NODES, GL4_WEIGHTS)
                .map(|(node, weight)| {
                    let parameter = if FULL { node } else { start + width * node };
                    weight * self.speed(parameter, geodesic.a, e2, dphi)
                })
                .sum::<f64>()
                * width;
            return self.finite_length(value);
        }

        let mut evaluations = 15;
        let (initial, initial_error) = self.gk15(start, end, geodesic.a, e2, dphi);
        let tolerance = RELATIVE_LENGTH_ERROR * initial.abs();
        if initial_error <= tolerance {
            return self.finite_length(initial);
        }

        let mut total = initial;
        let mut total_error = initial_error;
        let mut intervals = vec![Interval {
            start,
            end,
            value: initial,
            error: initial_error,
        }];
        while total_error > tolerance {
            let largest = intervals
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| left.error.total_cmp(&right.error))
                .map(|(index, _)| index)
                .expect("an unconverged quadrature has an interval");
            let parent = intervals.swap_remove(largest);
            if evaluations + 30 > MAX_GK15_EVALUATIONS {
                return Err(GeometryErrorKind::message(
                    "chart-edge length quadrature did not converge within 5,000 evaluations",
                ));
            }
            let middle = f64::midpoint(parent.start, parent.end);
            let (left, left_error) = self.gk15(parent.start, middle, geodesic.a, e2, dphi);
            let (right, right_error) = self.gk15(middle, parent.end, geodesic.a, e2, dphi);
            evaluations += 30;
            total += (left + right) - parent.value;
            total_error = (total_error - parent.error).max(0.0) + left_error + right_error;
            intervals.push(Interval {
                start: parent.start,
                end: middle,
                value: left,
                error: left_error,
            });
            intervals.push(Interval {
                start: middle,
                end: parent.end,
                value: right,
                error: right_error,
            });
        }
        self.finite_length(total)
    }

    /// Mean pole-referenced authalic integral multiplied by the chart longitude step.
    pub(crate) fn authalic_area_term(&self, series: &AuthalicSeries, north: bool) -> f64 {
        let direction = if north { 1.0 } else { -1.0 };
        let theta0 = (self.pole - direction * self.start_latitude) * self.latitude_unit;
        let half_delta = -direction * self.latitude_delta * self.latitude_unit / 2.0;
        let midpoint = theta0 + half_delta;
        let (sin_mid, cos_mid) = (midpoint / 2.0).sin_cos();
        let (sin_delta, cos_delta) = half_delta.sin_cos();
        let sin_mid_step = 2.0 * sin_mid * cos_mid;
        let cos_mid_step = cos_mid * cos_mid - sin_mid * sin_mid;
        let sin_delta_step = 2.0 * sin_delta * cos_delta;
        let cos_delta_step = cos_delta * cos_delta - sin_delta * sin_delta;
        let mut sin_mid_harmonic = sin_mid;
        let mut cos_mid_harmonic = cos_mid;
        let mut sin_delta_harmonic = sin_delta;
        let mut cos_delta_harmonic = cos_delta;
        let mut mean = 0.0;
        for (k, &coefficient) in series
            .coefficients
            .iter()
            .take(series.harmonics)
            .enumerate()
        {
            let y = (2 * k + 1) as f64 * half_delta;
            let (sinc_y, one_minus_sinc_y) = if y.abs() < 0.5 {
                let one_minus_sinc_y = one_minus_sinc_small(y);
                (1.0 - one_minus_sinc_y, one_minus_sinc_y)
            } else {
                let sinc_y = sin_delta_harmonic / y;
                (sinc_y, 1.0 - sinc_y)
            };
            let mean_deficit =
                one_minus_sinc_y + sinc_y * 2.0 * sin_mid_harmonic * sin_mid_harmonic;
            let signed_coefficient = if k % 2 == 0 {
                coefficient
            } else {
                -coefficient
            };
            mean += signed_coefficient * mean_deficit;

            let next_sin_mid = sin_mid_harmonic * cos_mid_step + cos_mid_harmonic * sin_mid_step;
            let next_cos_mid = cos_mid_harmonic * cos_mid_step - sin_mid_harmonic * sin_mid_step;
            sin_mid_harmonic = next_sin_mid;
            cos_mid_harmonic = next_cos_mid;

            let next_sin_delta =
                sin_delta_harmonic * cos_delta_step + cos_delta_harmonic * sin_delta_step;
            let next_cos_delta =
                cos_delta_harmonic * cos_delta_step - sin_delta_harmonic * sin_delta_step;
            sin_delta_harmonic = next_sin_delta;
            cos_delta_harmonic = next_cos_delta;
        }
        self.longitude_delta * mean
    }

    fn finite_length(&self, length: f64) -> Result<f64> {
        if length.is_finite() && length >= 0.0 {
            Ok(length)
        } else {
            Err(GeometryErrorKind::message(
                "chart-edge length is not representable in binary64 metres",
            ))
        }
    }

    fn admits_gl4(&self, dphi: f64) -> bool {
        let latitude0 = self.start_latitude;
        let latitude1 = self.end_latitude;
        let colatitude0 = (self.pole - latitude0.abs()) * self.latitude_unit;
        let colatitude1 = (self.pole - latitude1.abs()) * self.latitude_unit;
        let crosses_equator =
            (latitude0 <= 0.0 && latitude1 >= 0.0) || (latitude1 <= 0.0 && latitude0 >= 0.0);
        let max_cos_latitude = if crosses_equator {
            1.0
        } else {
            colatitude0.max(colatitude1).sin()
        };
        let extent = dphi
            .abs()
            .max(self.longitude_delta.abs() * max_cos_latitude);
        let colatitude = colatitude0.min(colatitude1);
        extent <= ONE_DEGREE && extent / colatitude <= 0.1
    }

    fn speed(&self, t: f64, semi_major: f64, e2: f64, dphi: f64) -> f64 {
        let (sin_latitude, cos_latitude) = self.latitude_sincos(t);
        let w2 = 1.0 - e2 * sin_latitude * sin_latitude;
        let w = w2.sqrt();
        let prime_vertical = semi_major / w;
        let meridian = semi_major * (1.0 - e2) / (w2 * w);
        (meridian * dphi).hypot(prime_vertical * cos_latitude * self.longitude_delta)
    }

    fn latitude_sincos(&self, t: f64) -> (f64, f64) {
        let latitude0 = self.start_latitude;
        let latitude1 = self.end_latitude;
        let latitude_delta = self.latitude_delta;
        let sign0 = latitude0.signum().copysign(1.0);
        let sign1 = latitude1.signum().copysign(1.0);
        let crosses_equator =
            (latitude0 < 0.0 && latitude1 > 0.0) || (latitude1 < 0.0 && latitude0 > 0.0);
        let (anchor, parameter, sign) = if crosses_equator {
            let crossing = -latitude0 / latitude_delta;
            if t <= crossing {
                (latitude0, t, sign0)
            } else {
                (latitude1, t - 1.0, sign1)
            }
        } else {
            (latitude0, t, sign0)
        };
        let anchor_colatitude = self.pole - sign * anchor;
        let colatitude =
            (anchor_colatitude - sign * parameter * latitude_delta) * self.latitude_unit;
        let (sin_colatitude, cos_colatitude) = colatitude.sin_cos();
        (sign * cos_colatitude, sin_colatitude)
    }

    fn gk15(&self, start: f64, end: f64, semi_major: f64, e2: f64, dphi: f64) -> (f64, f64) {
        let center = f64::midpoint(start, end);
        let half = (end - start) / 2.0;
        let center_value = self.speed(center, semi_major, e2, dphi);
        let mut kronrod = center_value * GK15_WEIGHTS[7];
        let mut gauss = center_value * G7_WEIGHTS[3];
        for index in 0..7 {
            let offset = half * GK15_NODES[index];
            let pair = self.speed(center - offset, semi_major, e2, dphi)
                + self.speed(center + offset, semi_major, e2, dphi);
            kronrod += GK15_WEIGHTS[index] * pair;
            if index % 2 == 1 {
                gauss += G7_WEIGHTS[index / 2] * pair;
            }
        }
        let value = kronrod * half;
        (value, ((kronrod - gauss) * half).abs())
    }
}

#[expect(
    clippy::float_cmp,
    reason = "only an exact seam spelling is the topology-owned seam identity"
)]
fn is_seam_longitude(axis: crate::geographic::PeriodicAxis, value: f64) -> bool {
    let magnitude = value.abs();
    if magnitude < axis.half_period() {
        false
    } else if magnitude == axis.half_period() {
        true
    } else {
        axis.canonicalize(value)
            .expect("chart-edge longitude was admitted")
            .identity(axis)
            == SeamIdentity::Seam
    }
}

impl ChartMetric for super::GeodesicMetric {
    type Edge = ChartEdge;
    type PointEdgeScratch = super::edge_distance::PointEdgeScratch;

    fn chart_frame(&self) -> GeographicFrame {
        self.frame
    }

    fn chart_edge(&self, start: Point, end: Point) -> Result<Self::Edge> {
        ChartEdge::new(start, end, self.frame)
    }

    fn chart_edge_length(&self, edge: &Self::Edge) -> Result<f64> {
        edge.length(&self.geodesic)
    }

    fn chart_edge_bounds(&self, edge: &Self::Edge) -> ChartBox {
        edge.bounds()
    }

    fn chart_edge_witness(
        &self,
        start: Point,
        end: Point,
        edge: &Self::Edge,
        parameter: f64,
    ) -> Result<Point> {
        edge.witness(start, end, parameter, &self.geodesic)
    }

    fn point_edge_query(
        &self,
        point: Point,
        edge: &Self::Edge,
        envelope_lower: f64,
        query: PointEdgeQuery,
        scratch: &mut Self::PointEdgeScratch,
    ) -> Result<PointEdgeQueryResult> {
        super::edge_distance::point_edge_query(
            &self.geodesic,
            self.frame,
            point,
            edge,
            envelope_lower,
            query,
            crate::geometry::GeodesicNumericalPolicy::new(self.geodesic.a),
            scratch,
        )
    }

    fn point_envelope_bound(&self, point: Point) -> impl Fn(ChartBox) -> f64 {
        let point = self
            .frame
            .to_geodesic_point(point)
            .expect("the query point was admitted before envelope pruning");
        let bound =
            super::envelope::GeodesicEnvelopeBound::new(self.geodesic.a, self.geodesic.f, point);
        move |bounds| bound.lower_bound(bounds)
    }

    fn chart_area_edge_term(&self, edge: &Self::Edge, north: bool) -> f64 {
        edge.authalic_area_term(&self.authalic_series, north)
    }

    fn chart_edge_seam_span(&self, edge: &Self::Edge) -> Option<[f64; 2]> {
        edge.seam_span()
    }

    fn chart_seam_span_length(&self, south: f64, north: f64) -> Result<f64> {
        let half_period = self.frame.longitude().half_period();
        let edge = ChartEdge::new(
            Point::new_raw_xy(half_period, south),
            Point::new_raw_xy(-half_period, north),
            self.frame,
        )?;
        edge.length(&self.geodesic)
    }

    fn chart_area_factor(&self) -> f64 {
        self.authalic_series.area_factor(self.geodesic.a)
    }

    fn chart_surface_area(&self) -> f64 {
        self.authalic_series.surface_area(self.geodesic.a)
    }
}

#[derive(Clone, Copy)]
pub(super) struct ChartEdgeDerivatives {
    pub(super) speed: f64,
    pub(super) acceleration: f64,
    pub(super) azimuth: f64,
    pub(super) geodesic_curvature: f64,
}

#[derive(Clone, Copy)]
pub(super) struct ChartEdgeDerivativeBounds {
    pub(super) speed: f64,
    pub(super) acceleration: f64,
    pub(super) curvature: f64,
}

#[derive(Clone, Copy)]
struct Interval {
    start: f64,
    end: f64,
    value: f64,
    error: f64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use geographiclib_rs::Geodesic;

    use super::super::GeodesicMetric;
    use super::{AuthalicSeries, ChartEdge, ONE_DEGREE};
    use crate::geographic::{AngularFactor, EdgeDirection, GeographicFrame, PeriodicAxis};
    use crate::geometry::types::ChartMetric;
    use crate::geometry::{GeometryErrorKind, Point};

    fn degrees() -> GeographicFrame {
        GeographicFrame::crs84()
    }

    fn grads() -> GeographicFrame {
        let factor = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
        GeographicFrame::try_new(factor, factor, 0.0).unwrap()
    }

    fn radians() -> GeographicFrame {
        let factor = AngularFactor::try_new("radian", 1.0).unwrap();
        GeographicFrame::try_new(factor, factor, 0.0).unwrap()
    }

    fn wgs84() -> Geodesic {
        Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563)
    }

    fn edge_length(
        frame: GeographicFrame,
        start: (f64, f64),
        end: (f64, f64),
    ) -> crate::error::Result<f64> {
        ChartEdge::new(
            Point::new_raw_xy(start.0, start.1),
            Point::new_raw_xy(end.0, end.1),
            frame,
        )?
        .length(&wgs84())
    }

    #[test]
    fn chart_edge_matches_mpmath_length_literals() {
        let cases = [
            (
                "general ellipsoidal edge",
                edge_length(degrees(), (-10.0, 35.0), (10.0, 48.0)).unwrap(),
                2_205_762.711_444_021_484_697_706_486_400_154_249_267_664_244_430_293_772,
            ),
            (
                "near-pole chart edge",
                edge_length(degrees(), (0.0, 89.9999), (100.0, 89.99995)).unwrap(),
                15.684_865_577_474_261_319_323_114_261_000_244_174_390_118_724_580_350_24,
            ),
            (
                "parallel closed form",
                edge_length(degrees(), (0.0, 60.0), (90.0, 60.0)).unwrap(),
                5_022_000.141_519_251_583_393_985_617_689_302_237_201_497_525_118_898_693,
            ),
            (
                "meridian closed form",
                edge_length(degrees(), (15.0, -20.0), (15.0, 70.0)).unwrap(),
                9_981_346.981_941_827_186_123_967_737_420_675_801_600_371_943_871_469_972,
            ),
        ];
        for (label, actual, expected) in cases {
            let relative_error = (actual - expected).abs() / expected;
            assert!(
                relative_error <= 1.0e-15,
                "{label}: actual={actual:.17}, expected={expected:.17}, relative_error={relative_error:.3e}"
            );
        }
    }

    #[test]
    fn chart_edge_partial_arclength_and_inverse_match_mpmath() {
        let geodesic = wgs84();
        let edge = ChartEdge::new(
            Point::new_raw_xy(0.0, 10.0),
            Point::new_raw_xy(120.0, 60.0),
            degrees(),
        )
        .unwrap();
        let expected_partial = 6_723_143.426_641_406_156_391_42;
        let expected_half_length = 6_004_860.750_429_569_135_504_388;
        let partial = edge.arclength(0.5, &geodesic, degrees()).unwrap();
        assert!(
            (partial - expected_partial).abs() <= 1.0e-8,
            "partial={partial:.17}, expected={expected_partial:.17}"
        );

        let length = edge.length(&geodesic).unwrap();
        let parameter = edge
            .locate_t(expected_half_length, length, &geodesic, degrees())
            .unwrap();
        assert!(
            (parameter - 0.442_288_586_250_222_77).abs() <= 2.0e-15,
            "parameter={parameter:.17}"
        );
        assert!(
            (edge.arclength(parameter, &geodesic, degrees()).unwrap() - expected_half_length).abs()
                <= 1.0e-8
        );
        let point = edge.position(parameter);
        assert!((point.x - 53.074_630_350_026_73).abs() <= 3.0e-14);
        assert!((point.y - 32.114_429_312_511_14).abs() <= 3.0e-14);
    }

    #[test]
    fn chart_edge_parallel_inversion_is_exact_and_seam_positions_follow_the_lifted_chart() {
        let geodesic = wgs84();
        let parallel = ChartEdge::new(
            Point::new_raw_xy(0.0, 60.0),
            Point::new_raw_xy(90.0, 60.0),
            degrees(),
        )
        .unwrap();
        assert!(parallel.is_parallel());
        let length = parallel.length(&geodesic).unwrap();
        assert_eq!(
            parallel
                .locate_t(length / 2.0, length, &geodesic, degrees())
                .unwrap(),
            0.5
        );
        assert_eq!(parallel.position(0.5).x, 45.0);
        assert_eq!(parallel.position(0.5).y, 60.0);

        let seam = ChartEdge::new(
            Point::new_raw_xy(170.0, 10.0),
            Point::new_raw_xy(-170.0, 10.0),
            degrees(),
        )
        .unwrap();
        // The lifted edge runs 170°→190°; retain +180° rather than the
        // equivalent canonical spelling -180° so inserted vertices stay on
        // the source edge's chart.
        assert_eq!(seam.position(0.5).x, 180.0);
        assert_eq!(seam.position(0.5).y, 10.0);

        let aliased_seam = ChartEdge::new(
            Point::new_raw_xy(530.0, 10.0),
            Point::new_raw_xy(190.0, 10.0),
            degrees(),
        )
        .unwrap();
        let midpoint = aliased_seam.position(0.5).x;
        assert_eq!(midpoint, 540.0);
        let axis = degrees().longitude();
        for (start, middle, end) in [(170.0, 180.0, -170.0), (530.0, 540.0, 190.0)] {
            for (from, to) in [(start, middle), (middle, end)] {
                assert!(axis.full_turn_delta(from, to).is_none());
                assert_eq!(axis.shortest_edge(from, to).unwrap().delta(axis), 10.0);
            }
        }
    }

    #[test]
    fn chart_edge_length_is_invariant_between_degree_grad_and_radian_frames() {
        let degree_length = edge_length(degrees(), (20.0, 35.0), (40.0, 48.0)).unwrap();
        let grad_length = edge_length(
            grads(),
            (20.0 * 10.0 / 9.0, 35.0 * 10.0 / 9.0),
            (40.0 * 10.0 / 9.0, 48.0 * 10.0 / 9.0),
        )
        .unwrap();
        let radian_length = edge_length(
            radians(),
            (20.0_f64.to_radians(), 35.0_f64.to_radians()),
            (40.0_f64.to_radians(), 48.0_f64.to_radians()),
        )
        .unwrap();

        for actual in [grad_length, radian_length] {
            assert!(
                (actual - degree_length).abs() <= 2.0e-9,
                "frame mismatch: degree={degree_length:.17}, actual={actual:.17}"
            );
        }
    }

    #[test]
    fn chart_edge_colatitude_uses_native_subtraction_before_conversion() {
        for frame in [degrees(), grads(), radians()] {
            let latitude = frame.pole().next_down();
            let edge = ChartEdge::new(
                Point::new_raw_xy(0.0, latitude),
                Point::new_raw_xy(0.25, latitude),
                frame,
            )
            .unwrap();
            let expected =
                (frame.pole() - latitude.abs()) * frame.latitude_unit().radians_per_unit();
            let actual = (edge.pole - edge.start_latitude.abs()) * edge.latitude_unit;
            assert_eq!(actual.to_bits(), expected.to_bits());
        }
    }

    #[test]
    fn chart_edge_rejects_exact_half_periods_with_an_actionable_geometry_error() {
        for frame in [degrees(), grads(), radians()] {
            let error = ChartEdge::new(
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(frame.longitude().half_period(), 10.0),
                frame,
            )
            .unwrap_err();
            assert!(
                matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        GeometryErrorKind::Parameter { message, .. }
                    ) if message.as_ref() == "edge spans exactly half the longitude period; add a vertex"
                ),
                "{error}"
            );
        }
    }

    #[test]
    fn chart_edge_reports_only_meridians_on_the_periodic_seam() {
        let seam = ChartEdge::new(
            Point::new_raw_xy(180.0, 70.0),
            Point::new_raw_xy(-180.0, 80.0),
            degrees(),
        )
        .unwrap();
        assert_eq!(seam.seam_span(), Some([70.0, 80.0]));

        let ordinary = ChartEdge::new(
            Point::new_raw_xy(0.0, 70.0),
            Point::new_raw_xy(0.0, 80.0),
            degrees(),
        )
        .unwrap();
        assert_eq!(ordinary.seam_span(), None);
    }

    #[test]
    fn chart_metric_trait_constructs_and_measures_a_chart_edge() {
        let metric = GeodesicMetric {
            crs: Arc::new("EPSG:4326".to_owned()),
            geodesic: Box::new(wgs84()),
            authalic_series: AuthalicSeries::new(&wgs84()),
            frame: degrees(),
            state: crate::crs::runtime_state_snapshot().unwrap(),
        };
        let edge = metric
            .chart_edge(Point::new_raw_xy(0.0, 60.0), Point::new_raw_xy(90.0, 60.0))
            .unwrap();
        let measured = metric.chart_edge_length(&edge).unwrap();
        let expected = 5_022_000.141_519_251_583_393_985_617_689_302_237_201_497_525_118_898_693;
        assert!((measured - expected).abs() <= 2.0e-9);
    }

    #[test]
    fn chart_edge_unwrapping_matches_topology_for_seams_ties_and_random_pairs() {
        let frames = [degrees(), grads(), radians()];
        let mut state = 0x4D59_5DF4_D0F3_3173_u64;
        for frame in frames {
            let axis: PeriodicAxis = frame.longitude();
            let half = axis.half_period();
            let period = axis.period();
            for (from, to) in [
                (0.0, half.next_down()),
                (0.0, half),
                (0.0, half.next_up()),
                (0.0, -half.next_down()),
                (0.0, -half),
                (0.0, -half.next_up()),
                (-period, period),
                (2.0 * period, 3.0 * period),
                (-3.0 * period, -2.0 * period),
            ] {
                assert_topology_delta(frame, from, to);
            }
            for index in 0..100_000 {
                let from = pseudo_random(&mut state) * axis.period() * 12.0;
                let to = if index % 4 == 0 {
                    let seam = axis.half_period();
                    match index % 8 {
                        0 => seam.next_down(),
                        4 => -seam.next_down(),
                        _ => pseudo_random(&mut state) * axis.period() * 12.0,
                    }
                } else {
                    pseudo_random(&mut state) * axis.period() * 12.0
                };
                assert_topology_delta(frame, from, to);
            }
        }
    }

    fn assert_topology_delta(frame: GeographicFrame, from: f64, to: f64) {
        let axis = frame.longitude();
        let direction = axis.shortest_edge(from, to).unwrap();
        let EdgeDirection::Minor(delta) = direction else {
            assert!(
                ChartEdge::new(
                    Point::new_raw_xy(from, 0.0),
                    Point::new_raw_xy(to, 0.0),
                    frame
                )
                .is_err(),
                "half-period edge was accepted: frame={frame:?}, from={from:?}, to={to:?}"
            );
            return;
        };
        let edge = ChartEdge::new(
            Point::new_raw_xy(from, 0.0),
            Point::new_raw_xy(to, 0.0),
            frame,
        )
        .unwrap();
        assert_eq!(
            edge.longitude_delta.to_bits(),
            (delta * axis.unit().radians_per_unit()).to_bits(),
            "frame={frame:?}, from={from:?}, to={to:?}"
        );

        let from_canonical = axis.canonicalize(from).unwrap().get();
        let to_canonical = axis.canonicalize(to).unwrap().get();
        let expected_transit = if delta > 0.0 && to_canonical < from_canonical {
            1
        } else if delta < 0.0 && to_canonical > from_canonical {
            -1
        } else {
            0
        };
        assert_eq!(
            axis.seam_transit(from, to).unwrap(),
            expected_transit,
            "frame={frame:?}, from={from:?}, to={to:?}"
        );
    }

    #[test]
    fn gauss_legendre_admission_is_bounded_to_one_degree_and_near_pole_ratio() {
        let edge = ChartEdge::new(
            Point::new_raw_xy(0.0, 45.0),
            Point::new_raw_xy(0.01, 45.01),
            degrees(),
        )
        .unwrap();
        assert!(edge.admits_gl4(edge.latitude_delta * edge.latitude_unit));
        assert!(ONE_DEGREE > edge.latitude_delta * edge.latitude_unit);
        let polar = ChartEdge::new(
            Point::new_raw_xy(0.0, 89.99),
            Point::new_raw_xy(0.9, 89.991),
            degrees(),
        )
        .unwrap();
        assert!(!polar.admits_gl4(polar.latitude_delta * polar.latitude_unit));
    }

    fn pseudo_random(state: &mut u64) -> f64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        (*state >> 11) as f64 * (1.0 / ((1_u64 << 53) as f64)) - 0.5
    }
}
