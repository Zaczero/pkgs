use geographiclib_rs::Geodesic;

use super::{ChartEdge, GeometryErrorKind, Result, Unmetered, Work};

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
pub(super) const ONE_DEGREE: f64 = std::f64::consts::PI / 180.0;

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

struct Interval {
    start: f64,
    end: f64,
    value: f64,
    error: f64,
}

impl ChartEdge {
    pub(crate) fn length(&self, geodesic: &Geodesic) -> Result<f64> {
        self.length_with_work(geodesic, &mut Unmetered)
    }

    pub(crate) fn length_with_work(
        &self,
        geodesic: &Geodesic,
        work: &mut impl Work,
    ) -> Result<f64> {
        if self.latitude_delta == 0.0 {
            work.charge_into_error(1)?;
            return self.finite_length(self.parallel_length(geodesic));
        }

        if let Some(meridian) = self.meridian {
            work.charge_into_error(1)?;
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
            work.charge_into_error(1)?;
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

    pub(in crate::crs::geodesic) fn integrate(
        &self,
        start: f64,
        end: f64,
        geodesic: &Geodesic,
    ) -> Result<f64> {
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
            work.charge_into_error(GL4_NODES.len() as u64)?;
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
        work.charge_into_error(15)?;
        let (initial, initial_error) = self.gk15(start, end, geodesic.a, e2, dphi);
        let tolerance = RELATIVE_LENGTH_ERROR * initial.abs();
        if initial_error <= tolerance {
            return self.finite_length(initial);
        }

        let mut total = initial;
        let mut total_error = initial_error;
        work.charge_into_error(2)?;
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
            work.charge_into_error(intervals.len() as u64)?;
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
            work.charge_into_error(removal_work)?;
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
            work.charge_into_error(30)?;
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
            work.charge_into_error(refinement_work)?;
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

    pub(super) fn admits_gl4(&self, dphi: f64) -> bool {
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

    pub(super) fn latitude_sincos(&self, t: f64) -> (f64, f64) {
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
