//! Shared chart-edge geometry, measures, and arclength parametrization.
//!
//! Longitude direction and explicit-period circles come from
//! `PeriodicAxis::chart_step`; latitude stays in native units until
//! pole-referenced colatitude is formed.

use geographiclib_rs::Geodesic;

use crate::error::Result;
use crate::geographic::{
    ChartStep, CircularInterval, EdgeDirection, GeographicFrame, Latitude, LongitudeWindows,
    RetainedChartDisplacement,
};
use crate::geometry::exact_arith::{
    retained_delta_scaled, retained_source_interval_initial_piece_count,
    retained_source_interval_reads_as, retained_source_sheet_site,
};
use crate::geometry::types::ChartMetric;
use crate::geometry::{
    ChartBox, ChartEdgeDifferential, ChartEdgeDifferentialBounds, ChartPointDistanceSample,
    CoordinateAxes, GeographicAreaDensity, GeometryErrorKind, HasM, HasZ, MAX_AUTHALIC_HARMONICS,
    MOrdinate, Point, PointEdgeQuery, PointEdgeQueryResult, Segment, SegmentProjection, Unmetered,
    Work, XY, ZOrdinate, segment_projection,
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

const GK15_CENTER_KRONROD_WEIGHT: f64 = 0.209_482_141_084_727_82;
const G7_CENTER_WEIGHT: f64 = 0.417_959_183_673_469_4;
const GK15_NONCENTRAL_RULE: [(f64, f64, Option<f64>); 7] = [
    (0.991_455_371_120_812_6, 0.022_935_322_010_529_224, None),
    (
        0.949_107_912_342_758_5,
        0.063_092_092_629_978_55,
        Some(0.129_484_966_168_869_7),
    ),
    (0.864_864_423_359_769_1, 0.104_790_010_322_250_18, None),
    (
        0.741_531_185_599_394_5,
        0.140_653_259_715_525_92,
        Some(0.279_705_391_489_276_67),
    ),
    (0.586_087_235_467_691_1, 0.169_004_726_639_267_9, None),
    (
        0.405_845_151_377_397_2,
        0.190_350_578_064_785_42,
        Some(0.381_830_050_505_118_9),
    ),
    (0.207_784_955_007_898_48, 0.204_432_940_075_298_89, None),
];

const MAX_GK15_EVALUATIONS: usize = 5_000;
const RELATIVE_LENGTH_ERROR: f64 = 4.0 * f64::EPSILON;
const ONE_DEGREE: f64 = std::f64::consts::PI / 180.0;
const AUTHALIC_TAIL_RELATIVE: f64 = 1.0e-17;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QuadratureTestProgress {
    pub(crate) seeds: u32,
    pub(crate) scans: u32,
    pub(crate) removals: u32,
    pub(crate) child_pairs: u32,
    pub(crate) growth_requests: u32,
    pub(crate) growths: u32,
    pub(crate) refinements: u32,
    pub(crate) current_refinements: u32,
    pub(crate) max_refinements: u32,
}

#[cfg(test)]
std::thread_local! {
    static QUADRATURE_TEST_PROGRESS: std::cell::Cell<QuadratureTestProgress> =
        const { std::cell::Cell::new(QuadratureTestProgress {
            seeds: 0,
            scans: 0,
            removals: 0,
            child_pairs: 0,
            growth_requests: 0,
            growths: 0,
            refinements: 0,
            current_refinements: 0,
            max_refinements: 0,
        }) };
}

#[cfg(test)]
fn update_test_progress(update: impl FnOnce(&mut QuadratureTestProgress)) {
    QUADRATURE_TEST_PROGRESS.with(|progress| {
        let mut current = progress.get();
        update(&mut current);
        progress.set(current);
    });
}

#[cfg(test)]
pub(crate) fn reset_quadrature_test_progress() {
    QUADRATURE_TEST_PROGRESS.set(QuadratureTestProgress::default());
}

#[cfg(test)]
pub(crate) fn quadrature_test_progress() -> QuadratureTestProgress {
    QUADRATURE_TEST_PROGRESS.get()
}

#[derive(Clone, Copy, Debug)]
/// Truncated authalic-q sine series cached with its ellipsoid's geodesic.
pub(crate) struct AuthalicSeries {
    coefficients: [f64; MAX_AUTHALIC_HARMONICS],
    harmonics: usize,
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
        #[expect(
            clippy::while_float,
            reason = "the retained sine-series order ends at the specified relative tail threshold"
        )]
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

        Self {
            coefficients,
            harmonics,
        }
    }

    pub(crate) fn area_density(
        self,
        geodesic: &Geodesic,
        reference_colatitude: f64,
        reference_pole: crate::geographic::GeodeticPole,
    ) -> GeographicAreaDensity {
        GeographicAreaDensity::new(
            self.coefficients,
            self.harmonics,
            geodesic.a,
            reference_colatitude,
            reference_pole,
        )
    }
}

fn binomial(n: usize, k: usize) -> f64 {
    let k = k.min(n.wrapping_sub(k));
    let mut coefficient = 1.0;
    for index in 1..=k {
        coefficient *= n.wrapping_add(1).wrapping_sub(index) as f64 / index as f64;
    }
    coefficient
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartEdge {
    frame: GeographicFrame,
    meridian: Option<MeridianCoordinates>,
    longitude: RetainedChartDisplacement,
    start_latitude: f64,
    end_latitude: f64,
    latitude_delta: f64,
    longitude_delta_native: f64,
    longitude_delta: f64,
    latitude_unit: f64,
    /// Native chart-pole magnitude; geodetic north/south are frame conversions.
    chart_pole: f64,
}

#[derive(Clone, Copy, Debug)]
struct MeridianCoordinates {
    longitude: f64,
    start_latitude: f64,
    end_latitude: f64,
}

impl ChartEdge {
    #[expect(
        clippy::float_cmp,
        reason = "exact chart deltas and the native chart-pole magnitude determine physical degeneracy"
    )]
    pub(crate) fn is_degenerate(&self) -> bool {
        self.latitude_delta == 0.0
            && (self.longitude_delta_native == 0.0 || self.start_latitude.abs() == self.chart_pole)
    }

    pub(crate) fn witness_role(&self, parameter: f64) -> crate::geometry::WitnessRole {
        if !self.is_degenerate() && parameter > 0.0 && parameter < 1.0 {
            crate::geometry::WitnessRole::Interior
        } else {
            crate::geometry::WitnessRole::Point
        }
    }

    pub(crate) fn new(start: Point, end: Point, frame: GeographicFrame) -> Result<Self> {
        let axis = frame.longitude();
        let connection = axis
            .chart_connection(start.x, end.x)
            .expect("chart-edge coordinates were admitted before measurement");
        if let ChartStep::Shortest(EdgeDirection::HalfTurn(direction)) = connection.step {
            return Err(GeometryErrorKind::parameter(
                "longitude",
                EdgeDirection::HalfTurn(direction).delta(axis),
                "edge spans exactly half the longitude period; add a vertex",
            ));
        }
        Ok(Self::from_retained_admitted(
            start,
            end,
            frame,
            connection.retained(),
        ))
    }

    /// Build a metric edge from an already selected source or producer branch.
    /// Unlike `new`, this does not classify the branch from rounded endpoints.
    pub(crate) fn from_retained(
        start: Point,
        end: Point,
        frame: GeographicFrame,
        longitude: RetainedChartDisplacement,
    ) -> Result<Self> {
        let axis = frame.longitude();
        let admitted_start = axis
            .canonicalize(start.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let admitted_end = axis
            .canonicalize(end.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        if admitted_start != longitude.endpoints()[0] || admitted_end != longitude.endpoints()[1] {
            return Err(GeometryErrorKind::Invalid(
                "retained chart edge endpoints do not match its source records".into(),
            )
            .into());
        }
        Ok(Self::from_retained_admitted(start, end, frame, longitude))
    }

    fn from_retained_admitted(
        start: Point,
        end: Point,
        frame: GeographicFrame,
        longitude: RetainedChartDisplacement,
    ) -> Self {
        let axis = frame.longitude();
        let canonical_start = longitude.endpoints()[0];
        let canonical_end = longitude.endpoints()[1];
        let longitude_delta_native = longitude.delta_native(axis);
        let start_latitude = Latitude::from_admitted(frame, start.y).get();
        let end_latitude = Latitude::from_admitted(frame, end.y).get();
        let latitude_delta = end_latitude - start_latitude;
        let meridian = (longitude_delta_native == 0.0 && latitude_delta != 0.0).then(|| {
            let (longitude, start_latitude) = frame
                .to_geodesic_xy(canonical_start.get(), start_latitude)
                .expect("chart-edge coordinates were admitted before measurement");
            let (_, end_latitude) = frame
                .to_geodesic_xy(canonical_end.get(), end_latitude)
                .expect("chart-edge coordinates were admitted before measurement");
            MeridianCoordinates {
                longitude,
                start_latitude,
                end_latitude,
            }
        });

        Self {
            frame,
            meridian,
            longitude,
            start_latitude,
            end_latitude,
            latitude_delta,
            longitude_delta_native,
            longitude_delta: retained_longitude_radians(
                longitude,
                axis,
                frame.longitude_orientation_sign(),
                longitude_delta_native,
            ),
            latitude_unit: frame.latitude_radians_per_unit() * frame.latitude_orientation_sign(),
            chart_pole: frame.pole().abs(),
        }
    }

    pub(super) fn geodetic_position(self, parameter: f64) -> Result<Point> {
        let axis = self.frame.longitude();
        let longitude = self.retained_longitude();
        let site: crate::geographic::RetainedLongitudeSite =
            axis.native_site_at(&longitude, parameter)?;
        let native = *site.native();
        let latitude = crate::geometry::interpolate_subdivision_ordinate(
            self.start_latitude,
            self.end_latitude,
            parameter,
        );
        let (longitude, latitude) = self
            .frame
            .to_geodesic_xy(native.get(), latitude)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        Ok(Point::new_raw_xy(longitude, latitude))
    }

    const fn retained_longitude(self) -> RetainedChartDisplacement {
        self.longitude
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact poles and explicit circle edges require a full-longitude bound"
    )]
    pub(crate) fn bounds(self) -> ChartBox {
        let [source_start, source_end] = *self.longitude.endpoints();
        let (start_longitude, start_latitude) = self
            .frame
            .to_geodesic_xy(source_start.get(), self.start_latitude)
            .expect("chart-edge endpoints were admitted");
        let (end_longitude, end_latitude) = self
            .frame
            .to_geodesic_xy(source_end.get(), self.end_latitude)
            .expect("chart-edge endpoints were admitted");
        let south = start_latitude.min(end_latitude).next_down().max(-90.0);
        let north = start_latitude.max(end_latitude).next_up().min(90.0);
        let axis = GeographicFrame::crs84().longitude();
        // `ChartEdge::new` stores the selected chart-step delta. A Circle step
        // spans one full period; shortest steps never exceed half a period.
        // Canonicalizing both endpoints would erase that distinction.
        let circle = self.longitude_delta_native.abs() > self.frame.longitude().half_period();
        let longitude_delta = self.longitude_delta.to_degrees();
        let interval = if start_latitude.abs() == 90.0 || end_latitude.abs() == 90.0 || circle {
            CircularInterval::Full
        } else {
            let (west, east) = if longitude_delta < 0.0 {
                (end_longitude, start_longitude)
            } else {
                (start_longitude, end_longitude)
            };
            CircularInterval::from_endpoints(
                axis.canonicalize(west)
                    .expect("chart-edge endpoint is an admitted longitude"),
                axis.canonicalize(east)
                    .expect("chart-edge endpoint is an admitted longitude"),
            )
        };
        let (west, east) = match interval.windows(axis) {
            Some(
                LongitudeWindows::One([west, east]) | LongitudeWindows::Two([west, _], [_, east]),
            ) => [west, east].into(),
            Some(LongitudeWindows::Full) | None => (-180.0, 180.0),
        };
        ChartBox {
            west,
            south,
            east,
            north,
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "the exact pole and parallel are analytic constant-distance families"
    )]
    pub(super) fn parallel_pole_distance_upper_bound(
        self,
        geodesic: &Geodesic,
        frame: GeographicFrame,
        target: Point,
    ) -> Option<f64> {
        if self.latitude_delta != 0.0 {
            return None;
        }
        let latitude = frame
            .to_geodesic_latitude(target.y)
            .expect("the target witness was admitted before chart distance");
        if latitude.abs() != 90.0 {
            return None;
        }
        let target = frame
            .to_geodesic_point(target)
            .expect("the target witness was admitted before chart distance");
        let source = self.geodetic_position(0.0).ok()?;
        let distance = super::ellipsoid_metric::inverse_distance(
            geodesic, target.x, target.y, source.x, source.y,
        );
        distance.is_finite().then_some(distance)
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
        endpoint_and_midpoint: &[ChartEdgeDerivatives; 3],
    ) -> ChartEdgeDerivativeBounds {
        const SAMPLE_PARAMETERS: [f64; 6] = [0.125, 0.25, 0.375, 0.625, 0.75, 0.875];
        let sample_parameters: &[f64] = if self.latitude_delta == 0.0 {
            &[]
        } else {
            &SAMPLE_PARAMETERS
        };
        let mut speed = 0.0_f64;
        let mut acceleration = 0.0_f64;
        let mut curvature = 0.0_f64;
        for derivative in endpoint_and_midpoint.iter().copied().chain(
            sample_parameters
                .iter()
                .map(|&parameter| self.derivatives(parameter, geodesic)),
        ) {
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

    pub(super) fn witness(
        self,
        start: Point,
        end: Point,
        parameter: f64,
        total_length: Option<f64>,
        geodesic: &Geodesic,
    ) -> Result<Point> {
        self.witness_with_work(
            start,
            end,
            parameter,
            total_length,
            geodesic,
            &mut Unmetered,
        )
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact minimizer endpoint parameters preserve input coordinates and ordinates"
    )]
    fn witness_with_work(
        self,
        start: Point,
        end: Point,
        parameter: f64,
        total_length: Option<f64>,
        geodesic: &Geodesic,
        work: &mut impl Work,
    ) -> Result<Point> {
        if parameter == 0.0 {
            return Ok(start);
        }
        if parameter == 1.0 {
            return Ok(end);
        }
        let axes = CoordinateAxes::new(
            HasZ(start.z().is_some() && end.z().is_some()),
            HasM(start.m().is_some() && end.m().is_some()),
        );
        let fraction = if axes.has_z() || axes.has_m() {
            let total = total_length.map_or_else(|| self.length_with_work(geodesic, work), Ok)?;
            Some(self.arclength_fraction_with_work(parameter, Some(total), geodesic, work)?)
        } else {
            None
        };
        self.witness_interior(start, end, parameter, axes, fraction)
    }

    fn witness_interior(
        self,
        start: Point,
        end: Point,
        parameter: f64,
        axes: CoordinateAxes,
        fraction: Option<f64>,
    ) -> Result<Point> {
        let position = self.chart_position(parameter)?;
        let (z, m) = fraction.map_or((ZOrdinate(None), MOrdinate(None)), |fraction| {
            crate::geometry::source_ordinate_values_at_fraction(start, end, axes, fraction)
        });
        let point = Point::new_axes(position.x, position.y, z, m)
            .expect("an admitted chart-edge witness is finite");
        Ok(self
            .frame
            .canonical_computed_point(point)
            .expect("an admitted chart-edge witness is finite"))
    }

    pub(crate) fn length(&self, geodesic: &Geodesic) -> Result<f64> {
        self.length_with_work(geodesic, &mut Unmetered)
    }

    pub(crate) fn length_with_work(
        &self,
        geodesic: &Geodesic,
        work: &mut impl Work,
    ) -> Result<f64> {
        if self.latitude_delta == 0.0 {
            work.charge(1)?;
            return self.finite_length(self.parallel_length(geodesic));
        }

        if let Some(meridian) = self.meridian {
            work.charge(1)?;
            return self.finite_length(super::inverse_distance(
                geodesic,
                meridian.longitude,
                meridian.start_latitude,
                meridian.longitude,
                meridian.end_latitude,
            ));
        }

        self.integrate_full_with_work(geodesic, work)
    }

    /// Shared source-edge arclength fraction for both carried ordinates.
    pub(crate) fn arclength_fraction(
        &self,
        parameter: f64,
        total_length: Option<f64>,
        geodesic: &Geodesic,
    ) -> Result<f64> {
        self.arclength_fraction_with_work(parameter, total_length, geodesic, &mut Unmetered)
    }

    pub(crate) fn arclength_fraction_with_work(
        &self,
        parameter: f64,
        total_length: Option<f64>,
        geodesic: &Geodesic,
        work: &mut impl Work,
    ) -> Result<f64> {
        if parameter <= 0.0 {
            return Ok(0.0);
        }
        if parameter >= 1.0 {
            return Ok(1.0);
        }
        let total = total_length.map_or_else(|| self.length_with_work(geodesic, work), Ok)?;
        if total == 0.0 {
            return Ok(0.0);
        }
        Ok(
            (self.arclength_with_work(parameter, Some(total), geodesic, work)? / total)
                .clamp(0.0, 1.0),
        )
    }

    pub(crate) fn is_parallel(&self) -> bool {
        self.latitude_delta == 0.0
    }

    fn parallel_length(&self, geodesic: &Geodesic) -> f64 {
        let colatitude = self.chart_pole - self.start_latitude.abs();
        let theta = colatitude * self.latitude_unit.abs();
        let cos_latitude = theta.sin();
        let physical_latitude = self.start_latitude * self.frame.latitude_orientation_sign();
        let sin_latitude = theta.cos().copysign(physical_latitude);
        let w = (1.0 - geodesic.f * (2.0 - geodesic.f) * sin_latitude * sin_latitude).sqrt();
        geodesic.a / w * cos_latitude * self.longitude_delta.abs()
    }

    /// Ellipsoidal arclength from the stored start to parameter `t`.
    pub(crate) fn arclength(&self, t: f64, geodesic: &Geodesic) -> Result<f64> {
        self.arclength_with_work(t, None, geodesic, &mut Unmetered)
    }

    pub(crate) fn arclength_with_work(
        &self,
        t: f64,
        total_length: Option<f64>,
        geodesic: &Geodesic,
        work: &mut impl Work,
    ) -> Result<f64> {
        debug_assert!((0.0..=1.0).contains(&t));
        if t <= 0.0 {
            return Ok(0.0);
        }
        if t >= 1.0 {
            return total_length.map_or_else(|| self.length_with_work(geodesic, work), Ok);
        }
        if self.latitude_delta == 0.0 {
            let total = total_length.map_or_else(|| self.length_with_work(geodesic, work), Ok)?;
            return self.finite_length(total * t);
        }
        if let Some(meridian) = self.meridian {
            work.charge(1)?;
            let position = self.geodetic_position(t)?;
            return self.finite_length(super::inverse_distance(
                geodesic,
                meridian.longitude,
                meridian.start_latitude,
                meridian.longitude,
                position.y,
            ));
        }
        self.integrate_with_work(0.0, t, geodesic, work)
    }

    /// Parameter whose accumulated ellipsoidal arclength is `distance`.
    pub(crate) fn locate_t(&self, distance: f64, length: f64, geodesic: &Geodesic) -> Result<f64> {
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
            let arclength = self.arclength(parameter, geodesic)?;
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
            #[expect(
                clippy::float_cmp,
                reason = "an iterate equal to a bracket endpoint cannot advance past adjacent binary64 values"
            )]
            if parameter == lower || parameter == upper {
                break;
            }
        }

        Ok(best.0)
    }

    /// Intended position on the chart edge before canonical point emission.
    pub(crate) fn chart_position(&self, t: f64) -> Result<crate::geometry::Point> {
        let axis = self.frame.longitude();
        let longitude = self.retained_longitude();
        let site: crate::geographic::RetainedLongitudeSite = axis.native_site_at(&longitude, t)?;
        let longitude = axis.lifted_site_longitude(site)?;
        let latitude = crate::geometry::interpolate_subdivision_ordinate(
            self.start_latitude,
            self.end_latitude,
            t,
        );
        Ok(crate::geometry::Point::new_raw_xy(longitude, latitude))
    }

    /// Publish one point in the original source sheet while keeping the
    /// relative retained branch authoritative for the metric curve.
    #[expect(
        clippy::float_cmp,
        reason = "exact endpoints and constant longitude preserve authentic source spellings"
    )]
    pub(crate) fn source_sheet_point(
        &self,
        source_start: Point,
        source_end: Point,
        parameter: f64,
        canonical_point: Point,
    ) -> Result<Point> {
        if parameter == 0.0 {
            return Ok(source_start);
        }
        if parameter == 1.0 {
            return Ok(source_end);
        }
        let retained = self.retained_longitude();
        let longitude = if source_start.x == source_end.x && retained.periods() == 0 {
            source_start.x
        } else {
            retained_source_sheet_site(source_start.x, retained, self.frame.longitude(), parameter)?
        };
        Ok(canonical_point.with_xy_raw(longitude, canonical_point.y))
    }

    pub(crate) fn source_interval_reads_correctly(
        &self,
        from_parameter: f64,
        to_parameter: f64,
        start: Point,
        end: Point,
    ) -> Result<bool> {
        let readback = self
            .frame
            .longitude()
            .chart_step(start.x, end.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        retained_source_interval_reads_as(
            self.retained_longitude(),
            self.frame,
            [self.start_latitude, self.end_latitude],
            from_parameter,
            to_parameter,
            readback,
            start,
            end,
        )
    }

    pub(crate) fn source_interval_initial_piece_count(&self, from: f64, to: f64) -> Result<usize> {
        retained_source_interval_initial_piece_count(
            self.retained_longitude(),
            self.frame.longitude(),
            from,
            to,
        )
    }

    /// Publish and verify the source-sheet chain over one retained parameter
    /// interval. The exact edge branch and parameter endpoints remain the
    /// certificate while output is subdivided until each stored edge reads
    /// along that branch.
    #[expect(
        clippy::float_cmp,
        reason = "source parameters identify exact binary64 endpoints and splits"
    )]
    pub(crate) fn publish_source_interval(
        &self,
        source_start: Point,
        source_end: Point,
        from: f64,
        to: f64,
        mut canonical_at: impl FnMut(f64) -> Result<Point>,
        mut emit: impl FnMut(f64, Point, bool) -> Result<()>,
        operation: &'static str,
    ) -> Result<()> {
        if !(from.is_finite()
            && to.is_finite()
            && (0.0..=1.0).contains(&from)
            && (0.0..=1.0).contains(&to)
            && from <= to)
        {
            return Err(GeometryErrorKind::unrepresentable_output(operation));
        }

        let mut point_at = |parameter: f64| {
            if parameter == 0.0 {
                Ok(source_start)
            } else if parameter == 1.0 {
                Ok(source_end)
            } else {
                self.source_sheet_point(
                    source_start,
                    source_end,
                    parameter,
                    canonical_at(parameter)?,
                )
            }
        };

        let mut previous_parameter = from;
        let mut previous = point_at(from)?;
        emit(from, previous, false)?;
        let end = point_at(to)?;
        if self.source_interval_reads_correctly(from, to, previous, end)? {
            return emit(to, end, false);
        }
        if previous.x == end.x {
            let axis = self.frame.longitude();
            let retained = self.retained_longitude();
            let start_site = retained_source_sheet_site(source_start.x, retained, axis, from)?;
            let end_site = retained_source_sheet_site(source_start.x, retained, axis, to)?;
            // RN is monotone over the exact affine source longitude. When both
            // written endpoints are those rounded sites and are identical, no
            // interior parameter can produce a readable nonzero longitude.
            if start_site.to_bits() == previous.x.to_bits() && end_site.to_bits() == end.x.to_bits()
            {
                return Err(GeometryErrorKind::unrepresentable_output(operation));
            }
        }

        let pieces = self.source_interval_initial_piece_count(from, to)?;
        for step in 1..=pieces {
            let parameter = if step == pieces {
                to
            } else {
                crate::geometry::interpolate_subdivision_ordinate(
                    from,
                    to,
                    step as f64 / pieces as f64,
                )
            };
            if parameter <= previous_parameter || (step < pieces && parameter >= to) {
                return Err(GeometryErrorKind::unrepresentable_output(operation));
            }
            let point = if parameter == to {
                end
            } else {
                point_at(parameter)?
            };
            self.publish_source_piece(
                previous_parameter,
                parameter,
                previous,
                point,
                to,
                &mut point_at,
                &mut emit,
                operation,
            )?;
            previous_parameter = parameter;
            previous = point;
        }
        Ok(())
    }

    #[expect(
        clippy::float_cmp,
        reason = "source parameters identify exact binary64 progress and longitude collapse"
    )]
    fn publish_source_piece(
        &self,
        from: f64,
        to: f64,
        start: Point,
        end: Point,
        planned_end: f64,
        point_at: &mut impl FnMut(f64) -> Result<Point>,
        emit: &mut impl FnMut(f64, Point, bool) -> Result<()>,
        operation: &'static str,
    ) -> Result<()> {
        if self.source_interval_reads_correctly(from, to, start, end)? {
            return emit(to, end, to != planned_end);
        }
        if from > 0.0 && to < 1.0 && start.x == end.x {
            return Err(GeometryErrorKind::unrepresentable_output(operation));
        }
        let mut pending = vec![(from, to, start, end)];
        while let Some((from, to, start, end)) = pending.pop() {
            if self.source_interval_reads_correctly(from, to, start, end)? {
                emit(to, end, to != planned_end)?;
                continue;
            }
            let middle = f64::midpoint(from, to);
            if middle == from || middle == to {
                return Err(GeometryErrorKind::unrepresentable_output(operation));
            }
            let middle_point = point_at(middle)?;
            pending.push((middle, to, middle_point, end));
            pending.push((from, middle, start, middle_point));
        }
        Ok(())
    }

    /// Coordinate-plane projection proposal expressed in this edge's lifted
    /// chart. The minimizer still evaluates its geodesic objective before the
    /// parameter can affect the attained result.
    pub(super) fn projection_parameter(self, point: Point) -> Result<f64> {
        Ok(self.project_source_chart(point)?.0.fraction())
    }

    /// Project through the retained source branch, returning the local query
    /// and segment for callers that must test source membership as well.
    /// Coordinates in that local chart are rounded `f64`s, so an exact contact
    /// predicate certifies only this represented segment, not exact membership
    /// in the original source geometry.
    pub(crate) fn project_source_chart(
        self,
        point: Point,
    ) -> Result<(SegmentProjection, XY, Segment)> {
        let axis = self.frame.longitude();
        // The start alias folds a full-turn edge onto one endpoint. The stored
        // chart-step delta makes the midpoint the reference for its full lifted span.
        let longitude = self.retained_longitude();
        let midpoint: crate::geographic::RetainedLongitudeSite = axis
            .native_site_at(&longitude, 0.5)
            .map_err(|error| crate::crs::CrsError::message(error.to_string()))?;
        let midpoint_longitude = axis
            .lifted_site_longitude(midpoint)
            .map_err(|error| crate::crs::CrsError::message(error.to_string()))?;
        let lifted_longitude = axis
            .nearest_longitude_alias(midpoint_longitude, point.x)
            .map_err(|error| crate::crs::CrsError::message(error.to_string()))?;
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(self.longitude_delta_native, self.latitude_delta),
        };
        let start_longitude = self.longitude.endpoints()[0].get();
        let query = XY::new(
            lifted_longitude - start_longitude,
            point.y - self.start_latitude,
        );
        Ok((segment_projection(query, segment)?, query, segment))
    }

    pub(super) fn integrate(&self, start: f64, end: f64, geodesic: &Geodesic) -> Result<f64> {
        self.integrate_with_work(start, end, geodesic, &mut Unmetered)
    }

    pub(crate) fn integrate_with_work(
        &self,
        start: f64,
        end: f64,
        geodesic: &Geodesic,
        work: &mut impl Work,
    ) -> Result<f64> {
        self.integrate_interval::<false>(start, end, geodesic, work)
    }

    fn integrate_full_with_work(&self, geodesic: &Geodesic, work: &mut impl Work) -> Result<f64> {
        self.integrate_interval::<true>(0.0, 1.0, geodesic, work)
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
        work: &mut impl Work,
    ) -> Result<f64> {
        if !FULL && start == end {
            return Ok(0.0);
        }
        let e2 = geodesic.f * (2.0 - geodesic.f);
        let dphi = self.latitude_delta * self.latitude_unit;
        if self.admits_gl4(dphi) {
            work.charge(GL4_NODES.len() as u64)?;
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

        let mut evaluations = 15_usize;
        work.charge(15)?;
        let (initial, initial_error) = self.gk15(start, end, geodesic.a, e2, dphi);
        let tolerance = RELATIVE_LENGTH_ERROR * initial.abs();
        if initial_error <= tolerance {
            return self.finite_length(initial);
        }

        let mut total = initial;
        let mut total_error = initial_error;
        work.charge(2)?;
        let mut intervals = vec![Interval {
            start,
            end,
            value: initial,
            error: initial_error,
        }];
        #[cfg(test)]
        update_test_progress(|progress| {
            progress.seeds = progress.seeds.wrapping_add(1);
            progress.current_refinements = 0;
        });
        #[expect(
            clippy::while_float,
            reason = "adaptive quadrature refines until its computed error meets the numerical tolerance"
        )]
        while total_error > tolerance {
            work.charge(intervals.len() as u64)?;
            let largest = intervals
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| left.error.total_cmp(&right.error))
                .map(|(index, _)| index)
                .expect("an unconverged quadrature has an interval");
            #[cfg(test)]
            update_test_progress(|progress| {
                progress.scans = progress.scans.wrapping_add(1);
            });
            let removal_work = if largest < intervals.len().saturating_sub(1) {
                2
            } else {
                1
            };
            work.charge(removal_work)?;
            let parent = intervals.swap_remove(largest);
            #[cfg(test)]
            update_test_progress(|progress| {
                progress.removals = progress.removals.wrapping_add(1);
            });
            if evaluations.wrapping_add(30) > MAX_GK15_EVALUATIONS {
                return Err(GeometryErrorKind::message(
                    "chart-edge length quadrature did not converge within 5,000 evaluations",
                ));
            }
            work.charge(30)?;
            let middle = f64::midpoint(parent.start, parent.end);
            let (left, left_error) = self.gk15(parent.start, middle, geodesic.a, e2, dphi);
            let (right, right_error) = self.gk15(middle, parent.end, geodesic.a, e2, dphi);
            #[cfg(test)]
            update_test_progress(|progress| {
                progress.child_pairs = progress.child_pairs.wrapping_add(1);
            });

            let needs_growth = intervals.capacity().saturating_sub(intervals.len()) < 2;
            #[cfg(test)]
            if needs_growth {
                update_test_progress(|progress| {
                    progress.growth_requests = progress.growth_requests.wrapping_add(1);
                });
            }
            // Three scalar updates, two interval writes, and—when reserve
            // relocates storage—two requested slots plus every live record.
            // The 5,000-sample cap keeps this billing sum below 174.
            let refinement_work = if needs_growth {
                5_u64.wrapping_add(2).wrapping_add(intervals.len() as u64)
            } else {
                5
            };
            work.charge(refinement_work)?;
            if needs_growth {
                intervals.reserve(2);
                #[cfg(test)]
                update_test_progress(|progress| {
                    progress.growths = progress.growths.wrapping_add(1);
                });
            }

            evaluations = evaluations.wrapping_add(30);
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
            #[cfg(test)]
            update_test_progress(|progress| {
                progress.refinements = progress.refinements.wrapping_add(1);
                progress.current_refinements = progress.current_refinements.wrapping_add(1);
                progress.max_refinements =
                    progress.max_refinements.max(progress.current_refinements);
            });
        }
        self.finite_length(total)
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
        let colatitude0 = (self.chart_pole - latitude0.abs()) * self.latitude_unit.abs();
        let colatitude1 = (self.chart_pole - latitude1.abs()) * self.latitude_unit.abs();
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
        let orientation = self.frame.latitude_orientation_sign();
        let latitude0 = self.start_latitude * orientation;
        let latitude1 = self.end_latitude * orientation;
        let latitude_delta = self.latitude_delta * orientation;
        let pole = self.chart_pole;
        let latitude_unit = self.latitude_unit * orientation;
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
        let anchor_colatitude = pole - sign * anchor;
        let colatitude = (anchor_colatitude - sign * parameter * latitude_delta) * latitude_unit;
        let (sin_colatitude, cos_colatitude) = colatitude.sin_cos();
        (sign * cos_colatitude, sin_colatitude)
    }

    fn gk15(&self, start: f64, end: f64, semi_major: f64, e2: f64, dphi: f64) -> (f64, f64) {
        let center = f64::midpoint(start, end);
        let half = (end - start) / 2.0;
        let center_value = self.speed(center, semi_major, e2, dphi);
        let mut kronrod = center_value * GK15_CENTER_KRONROD_WEIGHT;
        let mut gauss = center_value * G7_CENTER_WEIGHT;
        for &(node, kronrod_weight, gauss_weight) in &GK15_NONCENTRAL_RULE {
            let offset = half * node;
            let pair = self.speed(center - offset, semi_major, e2, dphi)
                + self.speed(center + offset, semi_major, e2, dphi);
            kronrod += kronrod_weight * pair;
            if let Some(gauss_weight) = gauss_weight {
                gauss += gauss_weight * pair;
            }
        }
        let value = kronrod * half;
        (value, ((kronrod - gauss) * half).abs())
    }
}

pub(super) fn chart_edge_differential(
    edge: &ChartEdge,
    geodesic: &Geodesic,
    parameter: f64,
) -> ChartEdgeDifferential {
    let differential = edge.derivatives(parameter, geodesic);
    ChartEdgeDifferential {
        speed: differential.speed,
        azimuth: differential.azimuth,
    }
}

pub(super) fn chart_edge_differential_bounds(
    edge: &ChartEdge,
    geodesic: &Geodesic,
) -> ChartEdgeDifferentialBounds {
    let start = edge.derivatives(0.0, geodesic);
    let samples = if edge.is_parallel() {
        [start; 3]
    } else {
        [
            start,
            edge.derivatives(0.5, geodesic),
            edge.derivatives(1.0, geodesic),
        ]
    };
    let bounds = edge.sampled_derivative_bounds(geodesic, &samples);
    let lipschitz_speed = if edge.is_parallel() {
        // A parallel has constant speed; retain the radius-relative arithmetic
        // allowance without the sampled-variation margin used by the Taylor bound.
        let allowance = crate::geometry::GeodesicNumericalPolicy::new(geodesic.a)
            .evaluation_allowance(4.0)
            * edge.longitude_delta.abs();
        bounds
            .speed
            .min((edge.parallel_length(geodesic) + allowance).next_up())
    } else {
        bounds.speed
    };
    ChartEdgeDifferentialBounds {
        speed: bounds.speed,
        lipschitz_speed,
        acceleration: bounds.acceleration,
        curvature: bounds.curvature,
    }
}

fn retained_longitude_radians(
    longitude: RetainedChartDisplacement,
    axis: crate::geographic::PeriodicAxis,
    orientation: f64,
    native_delta: f64,
) -> f64 {
    let radians_per_unit = axis.radians_per_unit();
    let ordinary = native_delta * radians_per_unit * orientation;
    if ordinary.is_finite() {
        return ordinary;
    }
    retained_delta_scaled(
        longitude.endpoints()[0].get(),
        longitude.endpoints()[1].get(),
        axis.period(),
        longitude.periods(),
        radians_per_unit * orientation,
    )
}

impl ChartMetric for super::GeodesicMetric {
    type PointEdgeScratch = super::edge_distance::PointEdgeScratch;

    fn chart_frame(&self) -> GeographicFrame {
        self.frame
    }

    fn chart_edge(&self, start: Point, end: Point) -> Result<ChartEdge> {
        ChartEdge::new(start, end, self.frame)
    }

    fn chart_edge_length(&self, edge: &ChartEdge) -> Result<f64> {
        edge.length(&self.geodesic)
    }

    fn chart_edge_length_with_work<W: Work>(&self, edge: &ChartEdge, work: &mut W) -> Result<f64> {
        edge.length_with_work(&self.geodesic, work)
    }

    fn chart_edge_length_to(&self, edge: &ChartEdge, parameter: f64) -> Result<f64> {
        edge.arclength(parameter, &self.geodesic)
    }

    fn chart_edge_arclength_fraction(
        &self,
        edge: &ChartEdge,
        parameter: f64,
        total_length: Option<f64>,
    ) -> Result<f64> {
        edge.arclength_fraction(parameter, total_length, &self.geodesic)
    }

    fn chart_edge_arclength_fraction_with_work<W: Work>(
        &self,
        edge: &ChartEdge,
        parameter: f64,
        total_length: Option<f64>,
        work: &mut W,
    ) -> Result<f64> {
        edge.arclength_fraction_with_work(parameter, total_length, &self.geodesic, work)
    }

    fn chart_edge_locate_t(&self, edge: &ChartEdge, distance: f64, length: f64) -> Result<f64> {
        edge.locate_t(distance, length, &self.geodesic)
    }

    fn chart_edge_interval_length(&self, edge: &ChartEdge, start: f64, end: f64) -> Result<f64> {
        edge.integrate(start, end, &self.geodesic)
    }

    fn chart_edge_witness(
        &self,
        start: Point,
        end: Point,
        edge: &ChartEdge,
        parameter: f64,
        length: Option<f64>,
    ) -> Result<Point> {
        edge.witness(start, end, parameter, length, &self.geodesic)
    }

    fn chart_edge_differential(&self, edge: &ChartEdge, parameter: f64) -> ChartEdgeDifferential {
        chart_edge_differential(edge, &self.geodesic, parameter)
    }

    fn chart_edge_differential_bounds(&self, edge: &ChartEdge) -> ChartEdgeDifferentialBounds {
        chart_edge_differential_bounds(edge, &self.geodesic)
    }

    fn chart_edge_pole_distance_upper_bound(&self, edge: &ChartEdge, target: Point) -> Option<f64> {
        edge.parallel_pole_distance_upper_bound(&self.geodesic, self.frame, target)
    }

    fn point_edge_query(
        &self,
        point: Point,
        edge: &ChartEdge,
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

    fn chart_point_distance_sample(
        &self,
        source: Point,
        target: Point,
        source_speed: f64,
        source_azimuth: f64,
    ) -> Result<ChartPointDistanceSample> {
        super::ellipsoid_metric::chart_point_distance_sample(
            &self.geodesic,
            self.frame,
            source,
            target,
            source_speed,
            source_azimuth,
        )
    }

    fn geodesic_distance_hessian_upper_bound(&self, lower_distance: f64) -> f64 {
        super::edge_distance::curvature_hessian_bound(&self.geodesic, lower_distance)
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

    fn chart_area_density(
        &self,
        reference_colatitude: f64,
        reference_pole: crate::geographic::GeodeticPole,
    ) -> GeographicAreaDensity {
        self.authalic_series
            .area_density(&self.geodesic, reference_colatitude, reference_pole)
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
    use crate::crs::geodesic::geographic_metric_frame_at;
    use crate::geographic::{
        AngularFactor, ChartStep, EdgeDirection, GeographicAxisOrientation, GeographicFrame,
    };
    use crate::geometry::exact_arith::{
        retained_source_interval_initial_piece_count, retained_source_sheet_site,
    };
    use crate::geometry::types::{ChartMetric as _, GeodesicMetric as _};
    use crate::geometry::{GeometryErrorKind, MOrdinate, Point, Work, ZOrdinate};

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

    fn south_axis_degrees() -> GeographicFrame {
        let factor = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
        GeographicFrame::try_new_with_orientation(
            factor,
            factor,
            0.0,
            GeographicAxisOrientation::from_axis_directions(true, false),
        )
        .unwrap()
    }

    fn wgs84() -> Geodesic {
        Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563)
    }

    #[derive(Default)]
    struct CountingWork(u64);

    impl Work for CountingWork {
        fn charge(&mut self, ticks: u64) -> crate::error::Result<()> {
            self.0 = self.0.wrapping_add(ticks);
            Ok(())
        }
    }

    #[test]
    fn xy_witness_skips_arclength_but_ordinate_witnesses_compute_it() {
        let edge = ChartEdge::new(
            Point::new_raw_xy(0.0, 10.0),
            Point::new_raw_xy(20.0, 30.0),
            degrees(),
        )
        .unwrap();
        let geodesic = wgs84();

        let mut work = CountingWork::default();
        edge.witness_with_work(
            Point::new_raw_xy(0.0, 10.0),
            Point::new_raw_xy(20.0, 30.0),
            0.4,
            None,
            &geodesic,
            &mut work,
        )
        .unwrap();
        assert_eq!(work.0, 0);

        for (start, end) in [
            (
                Point::new_axes(0.0, 10.0, ZOrdinate(Some(2.0)), MOrdinate(None)).unwrap(),
                Point::new_axes(20.0, 30.0, ZOrdinate(Some(7.0)), MOrdinate(None)).unwrap(),
            ),
            (
                Point::new_axes(0.0, 10.0, ZOrdinate(None), MOrdinate(Some(2.0))).unwrap(),
                Point::new_axes(20.0, 30.0, ZOrdinate(None), MOrdinate(Some(7.0))).unwrap(),
            ),
            (
                Point::new_axes(0.0, 10.0, ZOrdinate(Some(2.0)), MOrdinate(Some(12.0))).unwrap(),
                Point::new_axes(20.0, 30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(17.0))).unwrap(),
            ),
        ] {
            let mut work = CountingWork::default();
            let witness = edge
                .witness_with_work(start, end, 0.4, None, &geodesic, &mut work)
                .unwrap();
            assert!(work.0 > 0);
            assert_eq!(witness.z().is_some(), start.z().is_some());
            assert_eq!(witness.m().is_some(), start.m().is_some());
        }
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
                2_205_762.711_444_021_7,
            ),
            (
                "near-pole chart edge",
                edge_length(degrees(), (0.0, 89.9999), (100.0, 89.99995)).unwrap(),
                15.684_865_577_474_26,
            ),
            (
                "parallel closed form",
                edge_length(degrees(), (0.0, 60.0), (90.0, 60.0)).unwrap(),
                5_022_000.141_519_251,
            ),
            (
                "meridian closed form",
                edge_length(degrees(), (15.0, -20.0), (15.0, 70.0)).unwrap(),
                9_981_346.981_941_827,
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
        let expected_partial = 6_723_143.426_641_406_5;
        let expected_half_length = 6_004_860.750_429_569;
        let partial = edge.arclength(0.5, &geodesic).unwrap();
        assert!(
            (partial - expected_partial).abs() <= 1.0e-8,
            "partial={partial:.17}, expected={expected_partial:.17}"
        );

        let length = edge.length(&geodesic).unwrap();
        let parameter = edge
            .locate_t(expected_half_length, length, &geodesic)
            .unwrap();
        assert!(
            (parameter - 0.442_288_586_250_222_77).abs() <= 2.0e-15,
            "parameter={parameter:.17}"
        );
        assert!(
            (edge.arclength(parameter, &geodesic).unwrap() - expected_half_length).abs() <= 1.0e-8
        );
        let point = edge.chart_position(parameter).unwrap();
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
            parallel.locate_t(length / 2.0, length, &geodesic).unwrap(),
            0.5
        );
        assert_eq!(parallel.chart_position(0.5).unwrap().x, 45.0);
        assert_eq!(parallel.chart_position(0.5).unwrap().y, 60.0);

        let seam = ChartEdge::new(
            Point::new_raw_xy(170.0, 10.0),
            Point::new_raw_xy(-170.0, 10.0),
            degrees(),
        )
        .unwrap();
        // The lifted edge runs 170°→190°; retain +180° rather than the
        // equivalent canonical spelling -180° so inserted vertices stay on
        // the source edge's chart.
        assert_eq!(seam.chart_position(0.5).unwrap().x, 180.0);
        assert_eq!(seam.chart_position(0.5).unwrap().y, 10.0);

        let aliased_seam = ChartEdge::new(
            Point::new_raw_xy(530.0, 10.0),
            Point::new_raw_xy(190.0, 10.0),
            degrees(),
        )
        .unwrap();
        let midpoint = aliased_seam.chart_position(0.5).unwrap().x;
        assert_eq!(midpoint, 180.0);
        let axis = degrees().longitude();
        for (start, middle, end) in [(170.0, 180.0, -170.0), (530.0, 540.0, 190.0)] {
            for (from, to) in [(start, middle), (middle, end)] {
                assert!(axis.full_turn_delta(from, to).is_none());
                assert_eq!(
                    axis.shortest_alias_step(from, to).unwrap().delta(axis),
                    10.0
                );
            }
        }
    }

    #[test]
    fn chart_edge_bounds_contain_conditioned_positions_across_native_frames() {
        let degree = degrees();
        let grad_unit = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
        let radian_unit = AngularFactor::try_new("radian", 1.0).unwrap();
        let orientation = GeographicAxisOrientation::from_axis_directions(true, false);
        let frames = [
            degree,
            GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap(),
            GeographicFrame::try_new(radian_unit, radian_unit, 0.0).unwrap(),
            GeographicFrame::try_new(grad_unit, grad_unit, 2.0_f64.to_radians()).unwrap(),
            GeographicFrame::try_new_with_orientation(grad_unit, grad_unit, 0.0, orientation)
                .unwrap(),
        ];
        for frame in frames {
            let axis = frame.longitude();
            let start = Point::new_raw_xy(axis.degrees_to_native(170.0), frame.pole() * 0.6);
            let end = Point::new_raw_xy(axis.degrees_to_native(-170.0), frame.pole() * 0.9);
            let edge = ChartEdge::new(start, end, frame).unwrap();
            let bounds = edge.bounds();
            for sample in 0..=64 {
                let parameter = f64::from(sample) / 64.0;
                let chart = edge.chart_position(parameter).unwrap();
                let (longitude, latitude) = frame.to_geodesic_xy(chart.x, chart.y).unwrap();
                let longitude = degrees().longitude().canonicalize(longitude).unwrap().get();
                let contains_longitude = if bounds.west <= bounds.east {
                    (bounds.west..=bounds.east).contains(&longitude)
                } else {
                    longitude >= bounds.west || longitude <= bounds.east
                };
                assert!(
                    contains_longitude && (bounds.south..=bounds.north).contains(&latitude),
                    "frame={frame:?}, t={parameter}, point=({longitude}, {latitude}), bounds={bounds:?}"
                );
            }
        }
    }

    #[test]
    fn huge_source_alias_has_a_conservative_metric_box_and_authentic_endpoint_distance() {
        const CRS: &str = "EPSG:4326";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            let start = Point::new_raw_xy(1.0e18, 0.0);
            let end = Point::new_raw_xy(0.0, 0.0);
            let query = Point::new_raw_xy(48.0, 0.0);
            let edge = metric.chart_edge(start, end)?;
            let bounds = edge.bounds();
            let lower = metric.point_envelope_bound(query)(bounds);
            let endpoint_distance = metric.segment_length(query, end);
            let independently_known_equatorial_distance = 6_378_137.0 * 48.0_f64.to_radians();

            assert!(lower > 0.0);
            assert!(lower <= endpoint_distance);
            assert!((endpoint_distance - independently_known_equatorial_distance).abs() <= 1.0e-8);
            for parameter in [0.0, 0.25, 0.5, 0.75, 1.0] {
                let target = edge.geodetic_position(parameter)?;
                assert!(lower <= metric.segment_length(query, target));
            }
            assert_eq!(edge.chart_position(1.0)?.x, 0.0);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn retained_producer_branches_keep_long_half_and_multiple_turn_intent() {
        let axis = degrees().longitude();
        let cases = [
            ("positive major", 0.0, 250.0, 1, 0.5, 125.0),
            ("positive half", 0.0, 180.0, 1, 0.25, 45.0),
            (
                "positive half opposite spelling",
                0.0,
                -180.0,
                1,
                0.25,
                45.0,
            ),
            ("negative half", 0.0, -180.0, 0, 0.25, -45.0),
            ("positive two turns", 0.0, 720.0, 2, 0.25, 180.0),
            ("negative two turns", 0.0, -720.0, -2, 0.25, -180.0),
        ];
        for (name, start_x, end_x, periods, parameter, expected_x) in cases {
            let longitude = axis
                .retained_native_endpoints(start_x, end_x, periods)
                .unwrap();
            let edge = ChartEdge::from_retained(
                Point::new_raw_xy(start_x, 0.0),
                Point::new_raw_xy(end_x, 40.0),
                degrees(),
                longitude,
            )
            .unwrap();
            let point = edge.chart_position(parameter).unwrap();
            let source_point = edge
                .source_sheet_point(
                    Point::new_raw_xy(start_x, 0.0),
                    Point::new_raw_xy(end_x, 40.0),
                    parameter,
                    point,
                )
                .unwrap();
            assert!(
                (point.x - expected_x).abs() <= 2.0e-14,
                "{name}: actual={}, expected={expected_x}",
                point.x
            );
            assert!(
                (source_point.x - expected_x).abs() <= 2.0e-14,
                "{name}: source sheet={}, expected={expected_x}",
                source_point.x
            );
            let mut chain = Vec::new();
            edge.publish_source_interval(
                Point::new_raw_xy(start_x, 0.0),
                Point::new_raw_xy(end_x, 40.0),
                0.0,
                1.0,
                |parameter| edge.chart_position(parameter),
                |_, point, _| {
                    chain.push(point.x);
                    Ok(())
                },
                "line_substring",
            )
            .unwrap();
            let expected_chain: &[f64] = match name {
                "positive major" => &[0.0, 125.0, 250.0],
                "positive half" => &[0.0, 90.0, 180.0],
                "positive half opposite spelling" => &[0.0, 90.0, -180.0],
                "negative half" => &[0.0, -90.0, -180.0],
                "positive two turns" => {
                    &[0.0, 90.0, 180.0, 270.0, 360.0, 450.0, 540.0, 630.0, 720.0]
                },
                "negative two turns" => &[
                    0.0, -90.0, -180.0, -270.0, -360.0, -450.0, -540.0, -630.0, -720.0,
                ],
                _ => unreachable!("the branch table names every retained edge"),
            };
            assert_eq!(chain, expected_chain, "{name}: published branch chain");
        }

        let positive_half = axis.retained_native_endpoints(0.0, -180.0, 1).unwrap();
        assert_eq!(positive_half.periods(), 1);
        let negative_half = axis.retained_native_endpoints(0.0, -180.0, 0).unwrap();
        assert_eq!(negative_half.periods(), 0);
    }

    #[test]
    fn retained_source_connection_preserves_relative_lift_and_public_half_turn_policy() {
        let frame = degrees();
        let axis = frame.longitude();
        let connection = axis.chart_connection(530.0, 190.0).unwrap();
        assert_eq!(connection.source_lift, 1);
        assert_eq!(connection.step.delta(axis), 20.0);
        let edge = ChartEdge::from_retained(
            Point::new_raw_xy(530.0, 10.0),
            Point::new_raw_xy(190.0, 20.0),
            frame,
            connection.retained(),
        )
        .unwrap();
        assert_eq!(edge.chart_position(0.5).unwrap().x, 180.0);
        assert_eq!(edge.chart_position(1.0).unwrap().x, 190.0);
        drop(
            ChartEdge::new(
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(180.0, 20.0),
                frame,
            )
            .unwrap_err(),
        );
    }

    #[test]
    fn source_sheet_publication_uses_rust_admitted_frame_periods_and_branches() {
        let grad_unit = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
        let radian_unit = AngularFactor::try_new("radian", 1.0).unwrap();
        let west_radian = GeographicFrame::try_new_with_orientation(
            radian_unit,
            radian_unit,
            0.0,
            GeographicAxisOrientation::from_axis_directions(false, true),
        )
        .unwrap();
        let south_grad = GeographicFrame::try_new_with_orientation(
            grad_unit,
            grad_unit,
            0.0,
            GeographicAxisOrientation::from_axis_directions(true, false),
        )
        .unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let proj_radian = geographic_metric_frame_at(
            "GEOGCS[\"WGS 84 rad\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"radian\",1],AXIS[\"Longitude\",EAST],AXIS[\"Latitude\",NORTH]]",
            &state,
        )
        .unwrap();
        let proj_grad = geographic_metric_frame_at("EPSG:4807", &state).unwrap();
        let cases = [
            ("degrees", degrees(), 360.0, 10.0, 2.0),
            ("radians", radians(), std::f64::consts::TAU, 0.0, 0.5),
            (
                "PROJ radian CRS",
                proj_radian,
                std::f64::consts::TAU,
                0.0,
                0.5,
            ),
            ("EPSG:4807 grad CRS", proj_grad, 400.0, 5.0, 2.0),
            ("west radians", west_radian, std::f64::consts::TAU, 0.0, 0.5),
            ("south grads", south_grad, 400.0, 5.0, 2.0),
        ];
        for (name, frame, expected_period, base, step) in cases {
            let axis = frame.longitude();
            assert_eq!(
                axis.period().to_bits(),
                expected_period.to_bits(),
                "{name} frame period"
            );
            for turns in [-2.0, -1.0, 1.0, 2.0] {
                let source_start = base + turns * axis.period();
                let source_end = source_start + step;
                let start = Point::new_raw_xy(source_start, 0.0);
                let end = Point::new_raw_xy(source_end, 0.0);
                let admitted = axis.chart_connection(source_start, source_end).unwrap();
                let edge = ChartEdge::new(start, end, frame).unwrap();
                assert_eq!(
                    edge.retained_longitude().endpoints(),
                    &[admitted.start, admitted.end],
                    "{name}, turns={turns}: admitted canonical endpoints"
                );
                assert_eq!(
                    edge.retained_longitude().periods(),
                    i64::from(admitted.source_lift),
                    "{name}, turns={turns}: admitted relative lift"
                );
                let canonical = edge.chart_position(0.5).unwrap();
                let published = edge.source_sheet_point(start, end, 0.5, canonical).unwrap();
                let expected = source_start + step / 2.0;
                assert_eq!(
                    published.x.to_bits(),
                    expected.to_bits(),
                    "{name}, turns={turns}"
                );
            }
        }
    }

    #[test]
    fn source_interval_readback_accepts_correctly_rounded_endpoint_differences() {
        let start = Point::new_raw_xy(10.0, 60.0);
        let end = Point::new_raw_xy(12.0, 60.0);
        let edge = ChartEdge::new(start, end, degrees()).unwrap();
        let from = 0.25;
        let to = 0.75_f64.next_down();
        let from_point = edge
            .source_sheet_point(start, end, from, edge.chart_position(from).unwrap())
            .unwrap();
        let to_point = edge
            .source_sheet_point(start, end, to, edge.chart_position(to).unwrap())
            .unwrap();

        assert_eq!(from_point.x, 10.5);
        assert_eq!(to_point.x, 11.5);
        assert_eq!((to_point.x - from_point.x).to_bits(), 1.0_f64.to_bits());
        let exact_interval_delta = 2.0 * (to - from);
        assert_eq!(exact_interval_delta.to_bits(), 0x3FEF_FFFF_FFFF_FFFE);
        assert_ne!(
            (to_point.x - from_point.x).to_bits(),
            exact_interval_delta.to_bits()
        );
        assert!(
            edge.source_interval_reads_correctly(from, to, from_point, to_point)
                .unwrap()
        );
    }

    #[test]
    fn source_interval_readback_accepts_monotone_latitude_quantization_with_longitude_motion() {
        for (direction, end_latitude) in [
            ("increasing latitude", 10.0_f64.next_up()),
            ("decreasing latitude", 10.0_f64.next_down()),
        ] {
            let start = Point::new_raw_xy(0.0, 10.0);
            let end = Point::new_raw_xy(10.0, end_latitude);
            let edge = ChartEdge::new(start, end, degrees()).unwrap();
            let from = 0.125;
            let to = 0.25;
            let from_point = edge
                .source_sheet_point(start, end, from, edge.chart_position(from).unwrap())
                .unwrap();
            let to_point = edge
                .source_sheet_point(start, end, to, edge.chart_position(to).unwrap())
                .unwrap();

            assert_eq!(
                (from_point.x, from_point.y, to_point.x, to_point.y),
                (1.25, 10.0, 2.5, 10.0),
                "{direction}"
            );
            assert!(
                edge.source_interval_reads_correctly(from, to, from_point, to_point)
                    .unwrap(),
                "{direction}: exact latitude movement may round monotonically to equality"
            );
        }
    }

    #[test]
    fn source_interval_readback_rejects_a_nonzero_full_xy_collapse() {
        let start = Point::new_raw_xy(0.0, 10.0);
        let end = Point::new_raw_xy(0.0, 12.0);
        let edge = ChartEdge::new(start, end, degrees()).unwrap();
        let from = 0.5_f64;
        let to = from.next_up();
        let from_point = edge
            .source_sheet_point(start, end, from, edge.chart_position(from).unwrap())
            .unwrap();
        let to_point = edge
            .source_sheet_point(start, end, to, edge.chart_position(to).unwrap())
            .unwrap();

        // The exact sites are 11 and 11 + 2^-52. The latter is below half an
        // ULP at 11 and rounds to 11, so both emitted XY endpoints are (0, 11)
        // despite the nonzero source interval.
        assert_eq!((from_point.x, from_point.y), (0.0, 11.0));
        assert_eq!((to_point.x, to_point.y), (0.0, 11.0));
        assert!(
            edge.source_interval_reads_correctly(from, from, from_point, from_point)
                .unwrap(),
            "an exact zero-width interval remains a readable point"
        );
        assert!(
            !edge
                .source_interval_reads_correctly(from, to, from_point, to_point)
                .unwrap(),
            "a nonzero native-latitude interval cannot read back as one point"
        );

        let error = edge
            .publish_source_interval(
                start,
                end,
                from,
                to,
                |parameter| edge.chart_position(parameter),
                |_, _, _| Ok(()),
                "line_substring",
            )
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "line_substring",
                ..
            })
        ));
    }

    #[test]
    fn source_interval_readback_keeps_pole_degeneracy_distinct_from_rounded_motion() {
        let from = 0.5_f64;
        let to = from.next_up();
        let pole_start = Point::new_raw_xy(10.0, 90.0);
        let pole_end = Point::new_raw_xy(11.0, 90.0);
        let pole = ChartEdge::new(pole_start, pole_end, degrees()).unwrap();
        let pole_from = pole
            .source_sheet_point(
                pole_start,
                pole_end,
                from,
                pole.chart_position(from).unwrap(),
            )
            .unwrap();
        let pole_to = pole
            .source_sheet_point(pole_start, pole_end, to, pole.chart_position(to).unwrap())
            .unwrap();
        assert_eq!((pole_from.x, pole_from.y), (pole_to.x, pole_to.y));
        assert!(
            pole.source_interval_reads_correctly(from, to, pole_from, pole_to)
                .unwrap(),
            "a retained interval at one typed pole is physically constant"
        );

        let near_pole_end = Point::new_raw_xy(11.0, 89.5);
        let near_pole = ChartEdge::new(pole_start, near_pole_end, degrees()).unwrap();
        let near_from = near_pole
            .source_sheet_point(
                pole_start,
                near_pole_end,
                from,
                near_pole.chart_position(from).unwrap(),
            )
            .unwrap();
        let near_to = near_pole
            .source_sheet_point(
                pole_start,
                near_pole_end,
                to,
                near_pole.chart_position(to).unwrap(),
            )
            .unwrap();
        assert_eq!((near_from.x, near_from.y), (near_to.x, near_to.y));
        assert!(
            !near_pole
                .source_interval_reads_correctly(from, to, near_from, near_to)
                .unwrap(),
            "near-pole metric smallness does not make a nonzero interval constant"
        );
    }

    #[test]
    fn source_sheet_publication_keeps_readable_circles_and_rejects_collapsed_intervals() {
        let meridian_start = Point::new_raw_xy(370.0, -30.0);
        let meridian_end = Point::new_raw_xy(370.0, 70.0);
        let meridian = ChartEdge::new(meridian_start, meridian_end, degrees()).unwrap();
        assert_eq!(
            meridian
                .source_interval_initial_piece_count(0.25, 0.75)
                .unwrap(),
            1
        );
        let meridian_from = meridian
            .source_sheet_point(
                meridian_start,
                meridian_end,
                0.25,
                meridian.chart_position(0.25).unwrap(),
            )
            .unwrap();
        let meridian_to = meridian
            .source_sheet_point(
                meridian_start,
                meridian_end,
                0.75,
                meridian.chart_position(0.75).unwrap(),
            )
            .unwrap();
        assert_eq!(meridian_from.x.to_bits(), 370.0_f64.to_bits());
        assert_eq!(meridian_to.x.to_bits(), 370.0_f64.to_bits());
        assert!(
            meridian
                .source_interval_reads_correctly(0.25, 0.75, meridian_from, meridian_to)
                .unwrap()
        );

        let circle_start = Point::new_raw_xy(10.0, 0.0);
        let circle_end = Point::new_raw_xy(370.0, 0.0);
        let circle = ChartEdge::new(circle_start, circle_end, degrees()).unwrap();
        assert_eq!(circle.retained_longitude().periods(), 1);
        assert_eq!(
            circle
                .source_interval_initial_piece_count(0.0, 1.0)
                .unwrap(),
            2
        );
        assert!(
            circle
                .source_interval_reads_correctly(0.0, 1.0, circle_start, circle_end)
                .unwrap()
        );
        let mut authentic_circle = Vec::new();
        circle
            .publish_source_interval(
                circle_start,
                circle_end,
                0.0,
                1.0,
                |parameter| circle.chart_position(parameter),
                |parameter, point, refined| {
                    authentic_circle.push((parameter, point.x, refined));
                    Ok(())
                },
                "line_substring",
            )
            .unwrap();
        assert_eq!(authentic_circle, [(0.0, 10.0, false), (1.0, 370.0, false)]);
        let negative_circle_end = Point::new_raw_xy(-350.0, 0.0);
        let negative_circle = ChartEdge::new(circle_start, negative_circle_end, degrees()).unwrap();
        assert_eq!(negative_circle.retained_longitude().periods(), -1);
        let mut authentic_negative_circle = Vec::new();
        negative_circle
            .publish_source_interval(
                circle_start,
                negative_circle_end,
                0.0,
                1.0,
                |parameter| negative_circle.chart_position(parameter),
                |parameter, point, refined| {
                    authentic_negative_circle.push((parameter, point.x, refined));
                    Ok(())
                },
                "line_substring",
            )
            .unwrap();
        assert_eq!(authentic_negative_circle, [
            (0.0, 10.0, false),
            (1.0, -350.0, false)
        ]);
        assert_eq!(
            circle
                .source_interval_initial_piece_count(0.25, 1.0)
                .unwrap(),
            2
        );
        let canonical = circle.chart_position(0.25).unwrap();
        let published = circle
            .source_sheet_point(circle_start, circle_end, 0.25, canonical)
            .unwrap();
        assert_eq!(published.x, 100.0);
        let retained_circle = ChartEdge::from_retained(
            circle_start,
            circle_start,
            degrees(),
            degrees()
                .longitude()
                .retained_native_endpoints(10.0, 10.0, 1)
                .unwrap(),
        )
        .unwrap();
        let retained_midpoint = retained_circle
            .source_sheet_point(
                circle_start,
                circle_start,
                0.5,
                retained_circle.chart_position(0.5).unwrap(),
            )
            .unwrap();
        assert_eq!(retained_midpoint.x, 190.0);
        assert!(
            !retained_circle
                .source_interval_reads_correctly(0.0, 1.0, circle_start, circle_start)
                .unwrap(),
            "equal raw endpoints do not read an explicit retained full turn"
        );
        assert!(
            !retained_circle
                .source_interval_reads_correctly(0.0, 0.5, circle_start, retained_midpoint)
                .unwrap(),
            "a generated exact half-turn must be refined before publication"
        );
        assert!(
            !retained_circle
                .source_interval_reads_correctly(0.5, 1.0, retained_midpoint, circle_start)
                .unwrap(),
            "the authentic final spelling selects the opposite half-turn"
        );
        let mut expanded_circle = Vec::new();
        retained_circle
            .publish_source_interval(
                circle_start,
                circle_start,
                0.0,
                1.0,
                |parameter| retained_circle.chart_position(parameter),
                |parameter, point, refined| {
                    expanded_circle.push((parameter, point.x, refined));
                    Ok(())
                },
                "line_substring",
            )
            .unwrap();
        assert_eq!(expanded_circle, [
            (0.0, 10.0, false),
            (0.25, 100.0, true),
            (0.5, 190.0, true),
            (0.75, 280.0, true),
            (1.0, 10.0, false),
        ]);
        let retained_negative_circle = ChartEdge::from_retained(
            circle_start,
            circle_start,
            degrees(),
            degrees()
                .longitude()
                .retained_native_endpoints(10.0, 10.0, -1)
                .unwrap(),
        )
        .unwrap();
        let mut expanded_negative_circle = Vec::new();
        retained_negative_circle
            .publish_source_interval(
                circle_start,
                circle_start,
                0.0,
                1.0,
                |parameter| retained_negative_circle.chart_position(parameter),
                |parameter, point, refined| {
                    expanded_negative_circle.push((parameter, point.x, refined));
                    Ok(())
                },
                "line_substring",
            )
            .unwrap();
        assert_eq!(expanded_negative_circle, [
            (0.0, 10.0, false),
            (0.25, -80.0, true),
            (0.5, -170.0, true),
            (0.75, -260.0, true),
            (1.0, 10.0, false),
        ]);

        let large_start = Point::new_raw_xy(360.0 * 2.0_f64.powi(44), 0.0);
        let large_end = Point::new_raw_xy(large_start.x + 5.0, 0.0);
        let large_edge = ChartEdge::new(large_start, large_end, degrees()).unwrap();
        let large_site = retained_source_sheet_site(
            large_start.x,
            large_edge.retained_longitude(),
            degrees().longitude(),
            0.1,
        )
        .unwrap();
        assert_eq!(
            large_site.to_bits(),
            (large_start.x + 1.0).to_bits(),
            "one rounding follows the exact value just above the half-ULP tie"
        );
        let canonical = large_edge.chart_position(0.1).unwrap();
        assert_eq!(canonical.x, 0.5);
        let published = large_edge
            .source_sheet_point(large_start, large_end, 0.1, canonical)
            .unwrap();
        assert_eq!(published.x.to_bits(), large_site.to_bits());
        assert_ne!(
            published.x.to_bits(),
            (large_start.x + canonical.x).to_bits(),
            "single rounding differs from translating a rounded canonical site"
        );

        let huge_start = Point::new_raw_xy(1.0e22, 0.0);
        let huge_end = Point::new_raw_xy(1.0e22_f64.next_up(), 0.0);
        let huge = ChartEdge::new(huge_start, huge_end, degrees()).unwrap();
        let source_sheet = retained_source_sheet_site(
            huge_start.x,
            huge.retained_longitude(),
            degrees().longitude(),
            0.5,
        )
        .unwrap();
        assert_eq!(source_sheet.to_bits(), huge_start.x.to_bits());
        let _ = retained_source_interval_initial_piece_count(
            huge.retained_longitude(),
            degrees().longitude(),
            0.0,
            0.5,
        )
        .unwrap();
        let canonical = huge.chart_position(0.5).unwrap();
        let collapsed = huge
            .source_sheet_point(huge_start, huge_end, 0.5, canonical)
            .unwrap();
        assert_eq!(collapsed.x.to_bits(), huge_start.x.to_bits());
        assert_eq!(
            degrees()
                .longitude()
                .canonicalize(canonical.x)
                .unwrap()
                .get(),
            -4.0
        );
        assert!(
            !huge
                .source_interval_reads_correctly(0.0, 0.5, huge_start, collapsed)
                .unwrap(),
            "the nonzero first-half source interval collapsed to one longitude"
        );
        let error = huge
            .publish_source_interval(
                huge_start,
                huge_end,
                0.0,
                0.5,
                |parameter| huge.chart_position(parameter),
                |_, _, _| Ok(()),
                "line_substring",
            )
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                operation: "line_substring",
                ..
            })
        ));
    }

    #[test]
    fn conditioned_position_retains_small_endpoint_after_large_longitude_delta() {
        let start = Point::new_raw_xy(100.0, 10.0);
        let end = Point::new_raw_xy(1.0e-20, 20.0);
        let edge = ChartEdge::new(start, end, degrees()).unwrap();
        assert_eq!(
            edge.chart_position(1.0).unwrap().x.to_bits(),
            end.x.to_bits()
        );
        assert_eq!(edge.witness(start, end, 1.0, None, &wgs84()).unwrap(), end);
    }

    #[test]
    fn conditioned_position_does_not_lift_a_computed_site_from_a_huge_source_alias() {
        let edge = ChartEdge::new(
            Point::new_raw_xy(1.0e18, 0.0),
            Point::new_raw_xy(0.0, 0.0),
            degrees(),
        )
        .unwrap();
        assert_eq!(edge.chart_position(1.0).unwrap().x, 0.0);
        assert_eq!(edge.geodetic_position(1.0).unwrap().x, 0.0);
    }

    #[test]
    fn chart_edge_fraction_uses_the_actual_non_wgs84_ellipsoid() {
        const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
            assert_eq!(metric.ellipsoid_parameters(), (7_000_000.0, 1.0 / 150.0));
            let frame = metric.frame();
            let start = Point::new_axes(0.0, -30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(101.0)))?;
            let end = Point::new_axes(0.0, 70.0, ZOrdinate(Some(47.0)), MOrdinate(Some(701.0)))?;
            let longitude = frame
                .longitude()
                .chart_connection(start.x, end.x)
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?
                .retained();
            let edge = ChartEdge::from_retained(start, end, frame, longitude)?;
            let total = metric.chart_edge_length(&edge)?;
            let fraction = metric.chart_edge_arclength_fraction(&edge, 0.25, Some(total))?;
            assert!((fraction - 0.249_093_482_943_736_33).abs() <= 2.0e-15);
            let z = crate::geometry::interpolate_f64(7.0, 47.0, fraction);
            let m = crate::geometry::interpolate_f64(101.0, 701.0, fraction);
            assert!((z - 16.963_739_317_749_453).abs() <= 2.0e-13);
            assert!((m - 250.456_089_766_241_8).abs() <= 2.0e-12);
            Ok(())
        })
        .unwrap();
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
        for frame in [degrees(), grads(), radians(), south_axis_degrees()] {
            let latitude = frame.pole().next_down();
            let edge = ChartEdge::new(
                Point::new_raw_xy(0.0, latitude),
                Point::new_raw_xy(0.25, latitude),
                frame,
            )
            .unwrap();
            let expected = (frame.pole() - latitude.abs()) * frame.latitude_radians_per_unit();
            let actual = (edge.chart_pole - edge.start_latitude.abs()) * edge.latitude_unit.abs();
            assert_eq!(actual.to_bits(), expected.to_bits());
        }
    }

    #[test]
    fn chart_pole_degeneracy_uses_native_axis_magnitude_on_reversed_latitude_axis() {
        let frame = south_axis_degrees();
        let edge = ChartEdge::new(
            Point::new_raw_xy(0.0, frame.pole()),
            Point::new_raw_xy(20.0, frame.pole()),
            frame,
        )
        .unwrap();
        assert_eq!(edge.chart_pole, 90.0);
        assert!(edge.is_degenerate());
        assert_eq!(edge.geodetic_position(0.5).unwrap().y, -90.0);
    }

    #[test]
    fn reversed_native_latitude_uses_the_same_physical_meridian_measure() {
        let geodesic = wgs84();
        let edge = ChartEdge::new(
            Point::new_raw_xy(15.0, 20.0),
            Point::new_raw_xy(15.0, 40.0),
            south_axis_degrees(),
        )
        .unwrap();
        let expected = super::super::inverse_distance(&geodesic, 15.0, -20.0, 15.0, -40.0);
        assert!((edge.length(&geodesic).unwrap() - expected).abs() <= 1.0e-8);
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
    fn chart_edge_witness_uses_canonical_point_emission_and_arclength_axes() {
        let start =
            Point::new_axes(170.0, 10.0, ZOrdinate(Some(2.0)), MOrdinate(Some(-0.0))).unwrap();
        let end =
            Point::new_axes(-170.0, 10.0, ZOrdinate(Some(6.0)), MOrdinate(Some(8.0))).unwrap();
        let edge = ChartEdge::new(start, end, degrees()).unwrap();
        let witness = edge.witness(start, end, 0.5, None, &wgs84()).unwrap();

        assert_eq!(witness.x, -180.0);
        assert_eq!(witness.y, 10.0);
        assert_eq!(witness.z(), Some(4.0));
        assert_eq!(witness.m(), Some(4.0));
        assert_eq!(edge.chart_position(0.5).unwrap().x, 180.0);
    }

    #[test]
    fn explicit_full_turn_circle_has_full_longitude_bounds() {
        let circle = ChartEdge::new(
            Point::new_raw_xy(10.0, 60.0),
            Point::new_raw_xy(370.0, 60.0),
            degrees(),
        )
        .unwrap();
        let bounds = circle.bounds();
        assert_eq!(bounds.west.to_bits(), (-180.0_f64).to_bits());
        assert_eq!(bounds.east.to_bits(), 180.0_f64.to_bits());
    }

    #[test]
    fn chart_edge_bounds_keep_authentic_longitude_endpoints() {
        let start = 0.1;
        let end = 0.2;
        let edge = ChartEdge::new(
            Point::new_raw_xy(start, 10.0),
            Point::new_raw_xy(end, 10.0),
            degrees(),
        )
        .unwrap();
        let bounds = edge.bounds();
        assert_eq!(bounds.west.to_bits(), start.to_bits());
        assert_eq!(bounds.east.to_bits(), end.to_bits());
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
        let expected = 5_022_000.141_519_251;
        assert!((measured - expected).abs() <= 2.0e-9);
    }

    #[test]
    fn chart_edge_unwrapping_matches_topology_for_seams_ties_and_random_pairs() {
        let frames = [degrees(), grads(), radians()];
        let mut state = 0x4D59_5DF4_D0F3_3173_u64;
        for frame in frames {
            let axis = frame.longitude();
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
        let step = axis.chart_step(from, to).unwrap();
        let delta = step.delta(axis);
        if matches!(step, ChartStep::Shortest(EdgeDirection::HalfTurn(_))) {
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
        }
        let edge = ChartEdge::new(
            Point::new_raw_xy(from, 0.0),
            Point::new_raw_xy(to, 0.0),
            frame,
        )
        .unwrap();
        assert_eq!(
            edge.longitude_delta.to_bits(),
            (delta * axis.radians_per_unit() * frame.longitude_orientation_sign()).to_bits(),
            "frame={frame:?}, from={from:?}, to={to:?}"
        );

        let expected_transit = step.transit(axis, from, to).unwrap();
        assert_eq!(
            axis.chart_transit(from, to).unwrap(),
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
