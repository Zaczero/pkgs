//! Geographic polygon area from original-coordinate chart moments.

use crate::error::Result;
use crate::geographic::{
    ChartPole, ChartStep, GeodeticPole, GeographicFrame, GeographicRing, RingSummary,
};
use crate::geometry::exact_arith::{two_product, two_sum};
use crate::geometry::tessellation::exact::{ExactDyadic, outward_ratio_bounds, round_ratio};
use crate::geometry::{
    ChartMetric, Coordinates, ExactFactorialArea, GeometryErrorKind, Polygon, Ring,
    canonicalize_zero,
};
use crate::scratch_pool::{RetainScratch, ScratchPool, flat_vec_retained_bytes};

pub(crate) const MAX_AUTHALIC_HARMONICS: usize = 9;
const INITIAL_TAYLOR_ORDER: usize = 4;
const LATITUDE_SCALE: f64 = 0.125;
const MIN_SUBNORMAL: f64 = f64::from_bits(1);

#[derive(Clone, Copy)]
struct Harmonic {
    coefficient: f64,
    order: f64,
    sine: f64,
    cosine: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct GeographicAreaDensity {
    harmonics: [Harmonic; MAX_AUTHALIC_HARMONICS],
    count: usize,
    semi_major: f64,
    reference_pole: GeodeticPole,
}

impl GeographicAreaDensity {
    pub(crate) fn new(
        coefficients: [f64; MAX_AUTHALIC_HARMONICS],
        count: usize,
        semi_major: f64,
        reference_colatitude: f64,
        reference_pole: GeodeticPole,
    ) -> Self {
        debug_assert!(count <= MAX_AUTHALIC_HARMONICS);
        let mut harmonics = [Harmonic {
            coefficient: 0.0,
            order: 1.0,
            sine: 0.0,
            cosine: 1.0,
        }; MAX_AUTHALIC_HARMONICS];
        for (index, harmonic) in harmonics.iter_mut().enumerate().take(count) {
            let order = (2 * index + 1) as f64;
            let angle = order * reference_colatitude;
            let (sine, cosine) = angle.sin_cos();
            *harmonic = Harmonic {
                coefficient: if index % 2 == 0 {
                    coefficients[index]
                } else {
                    -coefficients[index]
                },
                order,
                sine,
                cosine,
            };
        }
        Self {
            harmonics,
            count,
            semi_major,
            reference_pole,
        }
    }

    fn fill_coefficients(self, order: usize, output: &mut Vec<AreaCoefficient>) -> Result<()> {
        output.clear();
        output.reserve(order.saturating_add(1));
        let mut even = [0.0; MAX_AUTHALIC_HARMONICS];
        let mut odd = [0.0; MAX_AUTHALIC_HARMONICS];
        let mut frequencies = [1.0; MAX_AUTHALIC_HARMONICS];
        let mut maximum_frequency_squared = 1.0_f64;
        let mut phase_bound = 1.0_f64;
        let derivative_direction = match self.reference_pole {
            GeodeticPole::North => -1.0,
            GeodeticPole::South => 1.0,
        };
        for index in 0..self.count {
            let harmonic = self.harmonics[index];
            let weighted = harmonic.coefficient * harmonic.order * 0.5;
            let frequency = derivative_direction * harmonic.order * (1.0 / LATITUDE_SCALE);
            even[index] = weighted * harmonic.sine;
            odd[index] = weighted * frequency * harmonic.cosine;
            frequencies[index] = (harmonic.order * (1.0 / LATITUDE_SCALE)).powi(2);
            maximum_frequency_squared = maximum_frequency_squared.max(frequencies[index]);
            phase_bound = phase_bound
                .max(harmonic.sine.abs())
                .max(harmonic.cosine.abs());
        }
        let mut even_underflow = upper_mul(
            MIN_SUBNORMAL,
            upper_mul(
                4.0 * self.count as f64,
                upper_mul(maximum_frequency_squared, phase_bound),
            ),
        );
        let mut odd_underflow = upper_mul(
            MIN_SUBNORMAL,
            upper_mul(
                8.0 * self.count as f64,
                upper_mul(maximum_frequency_squared, phase_bound),
            ),
        );
        for degree in 0..=order {
            let mut value = 0.0;
            let mut absolute = 0.0;
            for index in 0..self.count {
                let term = if degree % 2 == 0 {
                    even[index]
                } else {
                    odd[index]
                };
                value += term;
                absolute = upper_add(absolute, term.abs());
            }
            let seed_operations = if degree % 2 == 0 { 3 } else { 4 };
            let operations = (seed_operations + 2 * (degree / 2) + self.count) as f64;
            let relative = (operations / (9_007_199_254_740_992.0 - 2.0 * operations)).next_up();
            let underflow = if degree % 2 == 0 {
                even_underflow
            } else {
                odd_underflow
            };
            let error = upper_add(
                upper_mul(relative, absolute),
                upper_add(underflow, upper_mul(MIN_SUBNORMAL, self.count as f64)),
            );
            output.push(AreaCoefficient { value, error });
            if degree + 2 > order {
                continue;
            }
            let left_denominator = (degree + 1) as u64;
            let right_denominator = (degree + 2) as u64;
            let denominator = left_denominator
                .checked_mul(right_denominator)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            for index in 0..self.count {
                let value = if degree % 2 == 0 {
                    &mut even[index]
                } else {
                    &mut odd[index]
                };
                *value = (-*value * frequencies[index]) / denominator as f64;
            }
            let underflow = if degree % 2 == 0 {
                &mut even_underflow
            } else {
                &mut odd_underflow
            };
            *underflow = upper_add(
                upper_mul(
                    upper_div_u64(maximum_frequency_squared, denominator)
                        .next_up()
                        .next_up(),
                    *underflow,
                ),
                upper_mul(MIN_SUBNORMAL, 2.0 * self.count as f64),
            );
        }
        Ok(())
    }

    fn fill_exact_derivatives(self, order: usize, output: &mut Vec<ExactDyadic>) -> Result<()> {
        output.clear();
        output.reserve(order.saturating_add(1));
        let mut even: [ExactDyadic; MAX_AUTHALIC_HARMONICS] =
            std::array::from_fn(|_| ExactDyadic::zero());
        let mut odd: [ExactDyadic; MAX_AUTHALIC_HARMONICS] =
            std::array::from_fn(|_| ExactDyadic::zero());
        let mut frequency_squared: [ExactDyadic; MAX_AUTHALIC_HARMONICS] =
            std::array::from_fn(|_| ExactDyadic::zero());
        let derivative_direction = match self.reference_pole {
            GeodeticPole::North => -1.0,
            GeodeticPole::South => 1.0,
        };
        for index in 0..self.count {
            let harmonic = self.harmonics[index];
            let order = ExactDyadic::from_f64(harmonic.order);
            let weighted = ExactDyadic::from_f64(harmonic.coefficient)
                .try_product(&order)?
                .try_product(&ExactDyadic::from_f64(0.5))?;
            let frequency = ExactDyadic::from_f64(harmonic.order * (1.0 / LATITUDE_SCALE));
            even[index] = weighted.try_product(&ExactDyadic::from_f64(harmonic.sine))?;
            odd[index] = weighted
                .try_product(
                    &frequency
                        .clone()
                        .try_product(&ExactDyadic::from_f64(derivative_direction))?,
                )?
                .try_product(&ExactDyadic::from_f64(harmonic.cosine))?;
            frequency_squared[index] = frequency.try_product(&frequency)?;
        }

        for degree in 0..=order {
            let mut sum = ExactDyadic::zero();
            for index in 0..self.count {
                let coefficient = if degree % 2 == 0 {
                    &even[index]
                } else {
                    &odd[index]
                };
                sum = sum.try_add(coefficient.clone())?;
            }
            output.push(sum);
            for index in 0..self.count {
                let derivative = if degree % 2 == 0 {
                    &mut even[index]
                } else {
                    &mut odd[index]
                };
                *derivative = derivative
                    .clone()
                    .try_product(&frequency_squared[index].clone().neg())?;
            }
        }
        Ok(())
    }

    fn area_scale(self, x_exponent: i32) -> Result<ExactDyadic> {
        let scale_exponent = x_exponent
            .checked_neg()
            .and_then(|exponent| exponent.checked_add(3))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let scale = 2.0_f64.powi(scale_exponent);
        ExactDyadic::from_f64(self.semi_major)
            .try_product(&ExactDyadic::from_f64(self.semi_major))?
            .try_product(&ExactDyadic::from_f64(scale))
            .map_err(Into::into)
    }
}

#[derive(Clone, Copy)]
struct AreaCoefficient {
    value: f64,
    error: f64,
}

#[derive(Clone, Copy, Default)]
struct Bounded {
    value: f64,
    error: f64,
}

impl Bounded {
    const fn exact(value: f64) -> Self {
        Self { value, error: 0.0 }
    }

    fn add(self, other: Self) -> Self {
        let (value, residual) = two_sum(self.value, other.value);
        Self {
            value,
            error: upper_add(
                upper_add(self.error, other.error),
                upper_add(
                    residual.abs(),
                    if value.is_finite() {
                        MIN_SUBNORMAL
                    } else {
                        f64::INFINITY
                    },
                ),
            ),
        }
    }

    fn sub(self, other: Self) -> Self {
        self.add(Self {
            value: -other.value,
            error: other.error,
        })
    }

    fn mul(self, other: Self) -> Self {
        let (value, residual) = two_product(self.value, other.value);
        let round = if value.is_finite() {
            upper_add(residual.abs(), MIN_SUBNORMAL)
        } else {
            f64::INFINITY
        };
        let input = upper_add(
            upper_add(
                upper_mul(self.value.abs(), other.error),
                upper_mul(other.value.abs(), self.error),
            ),
            upper_mul(self.error, other.error),
        );
        Self {
            value,
            error: upper_add(input, round),
        }
    }

    fn div_u64(self, denominator: u64) -> Self {
        let denominator = denominator as f64;
        let value = self.value / denominator;
        let error = upper_add(
            upper_div_u64(self.error, denominator as u64),
            rounding_error(value),
        );
        Self { value, error }
    }
}

#[derive(Default)]
struct BoundedSum {
    head: f64,
    low: f64,
    lost: f64,
    input_error: f64,
    failed: bool,
}

impl BoundedSum {
    fn push(&mut self, value: Bounded) {
        if self.failed {
            return;
        }
        if !value.value.is_finite() || !value.error.is_finite() || value.error < 0.0 {
            self.failed = true;
            return;
        }
        if !(self.head + value.value).is_finite() {
            self.failed = true;
            return;
        }
        let (head, remainder) = two_sum(self.head, value.value);
        if !(self.low + remainder).is_finite() {
            self.failed = true;
            return;
        }
        let (low, discarded) = two_sum(self.low, remainder);
        self.input_error = upper_add(self.input_error, value.error);
        self.lost = upper_add(self.lost, discarded.abs());
        self.head = head;
        self.low = low;
        self.failed = !self.input_error.is_finite() || !self.lost.is_finite();
    }

    fn value(&self) -> f64 {
        if self.failed {
            f64::NAN
        } else {
            self.head + self.low
        }
    }

    fn error(&self) -> f64 {
        if self.failed || !(self.head + self.low).is_finite() {
            return f64::INFINITY;
        }
        upper_add(
            self.input_error,
            upper_add(self.lost, two_sum(self.head, self.low).1.abs()),
        )
    }

    fn bounded(&self) -> Bounded {
        Bounded {
            value: self.value(),
            error: self.error(),
        }
    }
}

#[derive(Default)]
struct GeographicAreaScratch {
    coefficients: Vec<AreaCoefficient>,
    exact_coefficients: Vec<ExactDyadic>,
    ring_moments: Vec<BoundedSum>,
    polygon_moments: Vec<BoundedSum>,
    exact_ring_moments: Vec<ExactDyadic>,
    exact_polygon_moments: Vec<ExactDyadic>,
}

impl RetainScratch for GeographicAreaScratch {
    fn clear_for_reuse(&mut self) {
        self.coefficients.clear();
        self.exact_coefficients.clear();
        self.ring_moments.clear();
        self.polygon_moments.clear();
        self.exact_ring_moments.clear();
        self.exact_polygon_moments.clear();
    }

    fn retained_bytes(&self) -> usize {
        flat_vec_retained_bytes(&self.coefficients)
            .saturating_add(flat_vec_retained_bytes(&self.exact_coefficients))
            .saturating_add(flat_vec_retained_bytes(&self.ring_moments))
            .saturating_add(flat_vec_retained_bytes(&self.polygon_moments))
            .saturating_add(flat_vec_retained_bytes(&self.exact_ring_moments))
            .saturating_add(flat_vec_retained_bytes(&self.exact_polygon_moments))
    }
}

thread_local! {
    static GEOGRAPHIC_AREA_SCRATCH: ScratchPool<GeographicAreaScratch> =
        const { ScratchPool::new(GeographicAreaScratch {
            coefficients: Vec::new(),
            exact_coefficients: Vec::new(),
            ring_moments: Vec::new(),
            polygon_moments: Vec::new(),
            exact_ring_moments: Vec::new(),
            exact_polygon_moments: Vec::new(),
        }) };
}

pub(crate) fn polygon_area(
    metric: &impl ChartMetric,
    polygon: &Polygon,
    frame: GeographicFrame,
) -> Result<f64> {
    // Shell and holes share one basis so signed source moments can cancel
    // before ring magnitudes or the final ellipsoid scale are rounded.
    let first = polygon.shell.nth_coord(0);
    let axis = frame.longitude();
    let x_origin = axis
        .chart_connection(first.x, polygon.shell.nth_coord(1).x)
        .expect("area geometry longitude was admitted")
        .start
        .get();
    let y_origin = first.y;
    let edges = polygon.rings().try_fold(0_usize, |count, ring| {
        count
            .checked_add(
                ring.coord_count()
                    .checked_sub(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
            )
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let exponent_bits = ceil_log2(
        edges
            .checked_add(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?,
    );
    let x_exponent = i32::try_from(
        exponent_bits
            .checked_add(3)
            .ok_or(crate::error::ResourceError::SizeOverflow)?,
    )
    .map_err(|_| crate::error::ResourceError::SizeOverflow)?
    .wrapping_neg();
    let longitude_scale = 2.0_f64.powi(x_exponent);
    let longitude_coordinate_scale =
        Bounded::exact(axis.radians_per_unit() * frame.longitude_orientation_sign())
            .mul(Bounded::exact(longitude_scale));
    let latitude_coordinate_scale =
        Bounded::exact(frame.latitude_radians_per_unit() * frame.latitude_orientation_sign())
            .mul(Bounded::exact(LATITUDE_SCALE));
    let reference_pole = if first.y * frame.latitude_orientation_sign() >= 0.0 {
        GeodeticPole::North
    } else {
        GeodeticPole::South
    };
    let pole_direction = match reference_pole {
        GeodeticPole::North => 1.0,
        GeodeticPole::South => -1.0,
    };
    let reference_colatitude = exact_difference_product(
        frame.native_latitude_pole(reference_pole),
        y_origin,
        frame.latitude_radians_per_unit() * frame.latitude_orientation_sign() * pole_direction,
    )?;
    let density = metric.chart_area_density(reference_colatitude.value, reference_pole);
    let exact_scale = density.area_scale(x_exponent)?;
    let scale_bound = area_scale_bound(&density, x_exponent)?;
    let mut scratch = ScratchPool::checkout(&GEOGRAPHIC_AREA_SCRATCH);
    let mut order = INITIAL_TAYLOR_ORDER;
    let mut exact_mode = false;
    let mut tail = None;
    let mut ceiling = None;
    loop {
        let mut predicted_order = None;
        if !exact_mode {
            density.fill_coefficients(order, &mut scratch.coefficients)?;
            let fast = {
                let GeographicAreaScratch {
                    coefficients,
                    ring_moments,
                    polygon_moments,
                    ..
                } = &mut *scratch;
                resize_moments(ring_moments, coefficients.len());
                resize_moments(polygon_moments, coefficients.len());
                accumulate_fast(
                    polygon,
                    frame,
                    x_origin,
                    y_origin,
                    longitude_coordinate_scale,
                    latitude_coordinate_scale,
                    coefficients,
                    ring_moments,
                    polygon_moments,
                )?
            };
            let fast_valid = [
                fast.integral.value,
                fast.integral.error,
                fast.source_error,
                fast.dot_error,
                fast.contraction_budget,
                fast.variation,
                fast.maximum_latitude,
                fast.coefficient_error,
            ]
            .into_iter()
            .all(f64::is_finite)
                && fast.integral.error >= 0.0
                && fast.source_error >= 0.0
                && fast.dot_error >= 0.0
                && fast.contraction_budget >= 0.0
                && fast.variation >= 0.0
                && fast.maximum_latitude >= 0.0
                && fast.coefficient_error >= 0.0;
            if fast_valid {
                if tail.is_none() {
                    let variation = PositiveBound::from_f64(fast.variation)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    let maximum_latitude = PositiveBound::from_f64(fast.maximum_latitude)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    let state = TaylorTail::new(&density, variation, maximum_latitude)?;
                    ceiling = Some(state.sufficient_order(scale_bound, INITIAL_TAYLOR_ORDER)?);
                    tail = Some(state);
                }
                let taylor_error = tail
                    .as_mut()
                    .expect("the finite fast source established its tail state")
                    .bound_at(scratch.coefficients.len())?;
                let norm = PositiveBound::from_f64(fast.contraction_budget)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                if let Some((area, scale_error)) =
                    scale_area_fast(fast.integral.value, &density, x_exponent)
                    && !fast.sign_uncertain
                {
                    let arithmetic_error =
                        positive_sum([fast.source_error, fast.dot_error, fast.coefficient_error])?;
                    let total_error = arithmetic_error
                        .add_upper(scale_error)
                        .and_then(|value| value.add_upper(taylor_error))
                        .ok_or(crate::error::ResourceError::SizeOverflow)?;
                    if accept_work_budget(total_error, norm, &exact_scale, order)? {
                        return Ok(area);
                    }
                    if !accept_work_budget(arithmetic_error, norm, &exact_scale, order)? {
                        exact_mode = true;
                    } else if order == INITIAL_TAYLOR_ORDER {
                        let fixed_error = arithmetic_error
                            .add_upper(scale_error)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        let first_trial = order
                            .checked_add(1)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        let last_trial = order
                            .checked_mul(2)
                            .ok_or(crate::error::ResourceError::SizeOverflow)?
                            .min(ceiling.expect("the fast source established its ceiling"));
                        for trial in first_trial..=last_trial {
                            let omitted_degree = trial
                                .checked_add(1)
                                .ok_or(crate::error::ResourceError::SizeOverflow)?;
                            let trial_tail = tail
                                .as_mut()
                                .expect("the finite fast source established its tail state")
                                .bound_at(omitted_degree)?;
                            let trial_error = fixed_error
                                .add_upper(trial_tail)
                                .ok_or(crate::error::ResourceError::SizeOverflow)?;
                            if accept_work_budget(trial_error, norm, &exact_scale, trial)? {
                                predicted_order = Some(trial);
                                break;
                            }
                        }
                    }
                } else {
                    exact_mode = true;
                }
            } else {
                exact_mode = true;
            }
        }

        if exact_mode {
            if order == INITIAL_TAYLOR_ORDER && reversed_source_zero(polygon, frame) {
                return Ok(0.0);
            }
            density.fill_exact_derivatives(order, &mut scratch.exact_coefficients)?;
            let (area, source_bounds) = {
                let GeographicAreaScratch {
                    exact_coefficients,
                    exact_ring_moments,
                    exact_polygon_moments,
                    ..
                } = &mut *scratch;
                resize_exact(exact_ring_moments, exact_coefficients.len());
                resize_exact(exact_polygon_moments, exact_coefficients.len());
                accumulate_exact(
                    polygon,
                    frame,
                    x_origin,
                    y_origin,
                    longitude_scale,
                    exact_coefficients,
                    exact_ring_moments,
                    exact_polygon_moments,
                )?
            };
            if tail.is_none() {
                let variation = positive_exact_bound(&source_bounds.variation)?;
                let maximum_latitude = positive_exact_bound(&source_bounds.maximum_latitude)?;
                let state = TaylorTail::new(&density, variation, maximum_latitude)?;
                ceiling = Some(state.sufficient_order(scale_bound, INITIAL_TAYLOR_ORDER)?);
                tail = Some(state);
            }
            let taylor_error = tail
                .as_mut()
                .expect("the exact source established its tail state")
                .bound_at(scratch.exact_coefficients.len())?;
            let norm = ExactFactorialArea::absolute_contraction(
                &scratch.exact_coefficients,
                &scratch.exact_polygon_moments,
            )?;
            let operation_count = operation_count(order)?;
            let gamma_accept = norm
                .compare_dyadic_gamma(&taylor_error.to_exact()?, operation_count)?
                .is_le();
            let physical_error = exact_scale.try_product(&taylor_error.to_exact()?)?;
            let lattice = ExactDyadic::try_from_u128(false, 1, -1075)?;
            if gamma_accept || physical_error.cmp(&lattice).is_le() {
                return Ok(canonicalize_zero(area.scaled_round(&exact_scale)?));
            }
            if order >= ceiling.expect("the exact source established its ceiling") {
                return Err(GeometryErrorKind::message(
                    "geographic area exact Taylor refinement exceeded its derived ceiling",
                ));
            }
        }

        let limit = ceiling.ok_or_else(|| {
            GeometryErrorKind::message("geographic area refinement has no finite progress bound")
        })?;
        if order >= limit {
            exact_mode = true;
            continue;
        }
        let doubled = order
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let next_order = order
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        order = predicted_order
            .unwrap_or(doubled)
            .max(next_order)
            .min(limit);
    }
}

fn reversed_source_zero(polygon: &Polygon, frame: GeographicFrame) -> bool {
    polygon.rings().all(|ring| {
        let count = ring.coord_count();
        if count < 2 {
            return false;
        }
        for index in 0..count / 2 {
            let left = ring.nth_coord(index);
            let right = ring.nth_coord(count.wrapping_sub(1).wrapping_sub(index));
            #[expect(
                clippy::float_cmp,
                reason = "reversal authentication compares stored coordinates exactly; signed zero remains equal"
            )]
            let source_vertices_differ = left.x != right.x || left.y != right.y;
            if source_vertices_differ {
                return false;
            }
        }
        let axis = frame.longitude();
        for index in 0..count.wrapping_sub(1).div_ceil(2) {
            let a = ring.nth_coord(index);
            let b = ring.nth_coord(index.wrapping_add(1));
            let c = ring.nth_coord(count.wrapping_sub(2).wrapping_sub(index));
            let d = ring.nth_coord(count.wrapping_sub(1).wrapping_sub(index));
            let Ok(forward) = axis.chart_connection(a.x, b.x) else {
                return false;
            };
            let Ok(reverse) = axis.chart_connection(c.x, d.x) else {
                return false;
            };
            if forward.start != reverse.end
                || forward.end != reverse.start
                || i64::from(forward.source_lift)
                    != i64::from(reverse.source_lift).wrapping_neg()
                || matches!(
                    forward.step,
                    ChartStep::Shortest(crate::geographic::EdgeDirection::HalfTurn(_))
                )
            {
                return false;
            }
        }
        true
    })
}

fn positive_sum<const N: usize>(values: [f64; N]) -> Result<PositiveBound> {
    let mut sum = PositiveBound::ZERO;
    for value in values {
        let value =
            PositiveBound::from_f64(value).ok_or(crate::error::ResourceError::SizeOverflow)?;
        sum = sum
            .add_upper(value)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    Ok(sum)
}

fn positive_exact_bound(value: &ExactDyadic) -> Result<PositiveBound> {
    if value.is_zero() {
        return Ok(PositiveBound::ZERO);
    }
    let (_, upper) = outward_ratio_bounds(value, &ExactDyadic::one())?;
    PositiveBound::from_f64(upper).ok_or_else(|| {
        GeometryErrorKind::message("geographic source bound is outside the proven range")
    })
}

struct FastArea {
    integral: Bounded,
    source_error: f64,
    dot_error: f64,
    contraction_budget: f64,
    variation: f64,
    maximum_latitude: f64,
    coefficient_error: f64,
    sign_uncertain: bool,
}

fn accumulate_fast(
    polygon: &Polygon,
    frame: GeographicFrame,
    x_origin: f64,
    y_origin: f64,
    longitude_coordinate_scale: Bounded,
    latitude_coordinate_scale: Bounded,
    coefficients: &[AreaCoefficient],
    ring_moments: &mut [BoundedSum],
    polygon_moments: &mut [BoundedSum],
) -> Result<FastArea> {
    for moment in ring_moments.iter_mut().chain(polygon_moments.iter_mut()) {
        *moment = BoundedSum::default();
    }
    let mut variation = 0.0;
    let mut maximum_latitude = 0.0_f64;
    let mut sign_uncertain = false;
    for (ring_index, ring) in polygon.rings().enumerate() {
        let mut row_certificate = RowCertificate::default();
        if !rectangle_moments(
            ring,
            frame,
            y_origin,
            longitude_coordinate_scale,
            latitude_coordinate_scale,
            ring_moments,
            &mut variation,
            &mut maximum_latitude,
        )? {
            let mut chart_turn = 0_i64;
            let mut previous = ring.nth_coord(0);
            let first_connection = frame
                .longitude()
                .chart_connection(previous.x, ring.nth_coord(1).x)
                .expect("area geometry longitude edge was admitted");
            let previous_canonical = first_connection.start;
            let mut previous_x = scaled_x(
                frame,
                previous_canonical.get(),
                chart_turn,
                x_origin,
                longitude_coordinate_scale,
            );
            let mut previous_z = scaled_y(previous.y, y_origin, latitude_coordinate_scale);
            for index in 1..ring.coord_count() {
                let next = ring.nth_coord(index);
                let connection = frame
                    .longitude()
                    .chart_connection(previous.x, next.x)
                    .expect("area geometry longitude edge was admitted");
                if let ChartStep::Shortest(crate::geographic::EdgeDirection::HalfTurn(direction)) =
                    connection.step
                {
                    return Err(GeometryErrorKind::parameter(
                        "longitude",
                        crate::geographic::EdgeDirection::HalfTurn(direction)
                            .delta(frame.longitude()),
                        "edge spans exactly half the longitude period; add a vertex",
                    ));
                }
                chart_turn = chart_turn
                    .checked_add(i64::from(connection.source_lift))
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                let next_canonical = connection.end;
                let next_x = scaled_x(
                    frame,
                    next_canonical.get(),
                    chart_turn,
                    x_origin,
                    longitude_coordinate_scale,
                );
                let next_z = scaled_y(next.y, y_origin, latitude_coordinate_scale);
                let delta_x = scaled_x(
                    frame,
                    connection.end.get(),
                    i64::from(connection.source_lift),
                    connection.start.get(),
                    longitude_coordinate_scale,
                );
                let delta_z = scaled_y(next.y, previous.y, latitude_coordinate_scale);
                add_fast_determinant(
                    previous_x.mul(delta_z).sub(previous_z.mul(delta_x)),
                    previous_z,
                    next_z,
                    ring_moments,
                    &mut variation,
                    &mut maximum_latitude,
                    &mut row_certificate,
                )?;
                previous = next;
                previous_x = next_x;
                previous_z = next_z;
            }
            if chart_turn != 0 {
                let summary = ring_summary(ring, frame);
                debug_assert_eq!(chart_turn, summary.chart_turn);
                // Close the integration chain at the selected pole; these edges are
                // not emitted or added to the stored ring.
                let pole = frame.chart_pole_latitude(selected_pole(summary));
                let pole_z = scaled_y(pole, y_origin, latitude_coordinate_scale);
                let start_canonical = first_connection.start;
                let start_x = scaled_x(
                    frame,
                    start_canonical.get(),
                    0,
                    x_origin,
                    longitude_coordinate_scale,
                );
                add_fast_edge(
                    previous_x,
                    previous_z,
                    previous_x,
                    pole_z,
                    ring_moments,
                    &mut variation,
                    &mut maximum_latitude,
                    &mut row_certificate,
                )?;
                add_fast_edge(
                    previous_x,
                    pole_z,
                    start_x,
                    pole_z,
                    ring_moments,
                    &mut variation,
                    &mut maximum_latitude,
                    &mut row_certificate,
                )?;
                add_fast_edge(
                    start_x,
                    pole_z,
                    start_x,
                    previous_z,
                    ring_moments,
                    &mut variation,
                    &mut maximum_latitude,
                    &mut row_certificate,
                )?;
            }
        }
        row_certificate.charge(ring_moments);
        let sign = ring_sign_fast(ring_moments, coefficients).unwrap_or_else(|| {
            sign_uncertain = true;
            if contract_fast(ring_moments, coefficients)
                .0
                .value
                .is_sign_negative()
            {
                -1.0
            } else {
                1.0
            }
        });
        if sign == 0.0 {
            for moment in ring_moments.iter_mut() {
                *moment = BoundedSum::default();
            }
            continue;
        }
        let polarity = if ring_index == 0 { 1.0 } else { -1.0 };
        for (polygon_moment, ring_moment) in polygon_moments.iter_mut().zip(ring_moments.iter()) {
            let value = Bounded {
                value: ring_moment.value() * (sign * polarity),
                error: ring_moment.error(),
            };
            polygon_moment.push(value);
        }
        for moment in ring_moments.iter_mut() {
            *moment = BoundedSum::default();
        }
    }
    let (integral, source_error, dot_error) = contract_fast(polygon_moments, coefficients);
    let coefficient_error = coefficient_dot_error(polygon_moments, coefficients);
    let contraction_budget = contraction_work_budget(polygon_moments, coefficients);
    Ok(FastArea {
        integral,
        source_error,
        dot_error,
        contraction_budget,
        variation,
        maximum_latitude,
        coefficient_error,
        sign_uncertain,
    })
}

fn rectangle_moments(
    ring: &Ring,
    frame: GeographicFrame,
    y_origin: f64,
    longitude_scale: Bounded,
    latitude_scale: Bounded,
    moments: &mut [BoundedSum],
    variation: &mut f64,
    maximum_latitude: &mut f64,
) -> Result<bool> {
    if ring.coord_count() != 5 {
        return Ok(false);
    }
    let first = ring.nth_coord(0);
    let second = ring.nth_coord(1);
    let third = ring.nth_coord(2);
    let fourth = ring.nth_coord(3);
    let closing = ring.nth_coord(4);
    #[expect(
        clippy::float_cmp,
        reason = "rectangle closure compares stored coordinates exactly; signed zero remains equal"
    )]
    let closure_differs = first.x != closing.x || first.y != closing.y;
    if closure_differs {
        return Ok(false);
    }
    #[expect(
        clippy::float_cmp,
        reason = "axis-aligned rectangle recognition requires exact native-coordinate equality"
    )]
    let (left, right, reverse_left, reverse_right, top, polarity, verticals) =
        if first.y == second.y && second.x == third.x && third.y == fourth.y && fourth.x == first.x
        {
            (first.x, second.x, third.x, fourth.x, third.y, 1.0, [
                (second.x, third.x),
                (fourth.x, closing.x),
            ])
        } else if first.x == second.x
            && second.y == third.y
            && third.x == fourth.x
            && fourth.y == first.y
        {
            (second.x, third.x, fourth.x, first.x, second.y, -1.0, [
                (first.x, second.x),
                (third.x, fourth.x),
            ])
        } else {
            return Ok(false);
        };
    let axis = frame.longitude();
    let forward = axis
        .chart_connection(left, right)
        .expect("rectangle longitude source was admitted");
    let reverse = axis
        .chart_connection(reverse_left, reverse_right)
        .expect("rectangle longitude source was admitted");
    for (from, to) in verticals {
        let connection = axis
            .chart_connection(from, to)
            .expect("rectangle longitude source was admitted");
        if connection.start != connection.end || connection.source_lift != 0 {
            return Ok(false);
        }
    }
    if forward.start != reverse.end
        || forward.end != reverse.start
        || i64::from(forward.source_lift) != i64::from(reverse.source_lift).wrapping_neg()
        || matches!(
            forward.step,
            ChartStep::Shortest(crate::geographic::EdgeDirection::HalfTurn(_))
        )
    {
        return Ok(false);
    }

    let width = scaled_x(
        frame,
        forward.end.get(),
        i64::from(forward.source_lift),
        forward.start.get(),
        longitude_scale,
    );
    let height = scaled_y(top, first.y, latitude_scale);
    let determinant = width.mul(height).mul(Bounded::exact(polarity));
    let z0 = scaled_y(first.y, y_origin, latitude_scale);
    let z1 = scaled_y(top, y_origin, latitude_scale);
    let radius = upper_add(z0.value.abs(), z0.error).max(upper_add(z1.value.abs(), z1.error));
    if !radius.is_finite() || radius > 0.5 {
        return Ok(false);
    }

    *variation = upper_add(
        *variation,
        upper_add(determinant.value.abs(), determinant.error),
    );
    *maximum_latitude = (*maximum_latitude).max(radius);
    let input_error = upper_add(z0.error, z1.error);
    let mut symmetric_power = 1.0;
    let mut z1_power = z1.value;
    let mut radius_power = 1.0;
    let mut previous_radius_power = 1.0;
    let count = moments.len();
    for (degree, moment) in moments.iter_mut().enumerate() {
        let degree_value = degree as f64;
        let factor = degree_value + 2.0;
        let operation_count = 2.0 * degree_value + 2.0;
        let gamma = (operation_count / (9_007_199_254_740_992.0 - operation_count)).next_up();
        let absolute_h = upper_mul(degree_value + 1.0, radius_power);
        let sensitivity = if degree == 0 {
            0.0
        } else {
            upper_mul(
                degree_value * (degree_value + 1.0) * 0.5,
                previous_radius_power,
            )
        };
        let error = upper_add(
            upper_mul(
                factor,
                upper_add(
                    upper_mul(
                        upper_add(determinant.error, upper_mul(gamma, determinant.value.abs())),
                        absolute_h,
                    ),
                    upper_add(
                        upper_mul(determinant.value.abs(), upper_mul(sensitivity, input_error)),
                        upper_mul(
                            MIN_SUBNORMAL,
                            upper_add(1.0, upper_mul(4.0 * degree_value, determinant.value.abs())),
                        ),
                    ),
                ),
            ),
            MIN_SUBNORMAL,
        );
        moment.push(Bounded {
            value: determinant.value * symmetric_power * factor,
            error,
        });
        if degree.wrapping_add(1) < count {
            symmetric_power = z0.value * symmetric_power + z1_power;
            z1_power *= z1.value;
            previous_radius_power = radius_power;
            radius_power = upper_mul(radius_power, radius);
        }
    }
    Ok(true)
}

fn add_fast_edge(
    x0: Bounded,
    z0: Bounded,
    x1: Bounded,
    z1: Bounded,
    moments: &mut [BoundedSum],
    variation: &mut f64,
    maximum_latitude: &mut f64,
    row_certificate: &mut RowCertificate,
) -> Result<()> {
    let determinant = cross(x0, z0, x1, z1);
    add_fast_determinant(
        determinant,
        z0,
        z1,
        moments,
        variation,
        maximum_latitude,
        row_certificate,
    )
}

#[derive(Default)]
struct RowCertificate {
    determinant_magnitude: f64,
    determinant_error: f64,
    weighted_latitude_error: f64,
    radius: f64,
    rows: usize,
}

impl RowCertificate {
    fn charge(&self, moments: &mut [BoundedSum]) {
        if self.rows == 0 {
            return;
        }
        let mut radius_power = 1.0;
        let mut previous_radius_power = 1.0;
        for (degree, moment) in moments.iter_mut().enumerate() {
            let k = degree as f64;
            let n = 2.0 * k + 1.0;
            let gamma = (n / (9_007_199_254_740_992.0 - n)).next_up();
            let absolute_h = upper_mul(k + 1.0, radius_power);
            let sensitivity = if degree == 0 {
                0.0
            } else {
                upper_mul(k * (k + 1.0) * 0.5, previous_radius_power)
            };
            let error = upper_add(
                upper_mul(
                    upper_add(
                        self.determinant_error,
                        upper_mul(gamma, self.determinant_magnitude),
                    ),
                    absolute_h,
                ),
                upper_add(
                    upper_mul(sensitivity, self.weighted_latitude_error),
                    upper_mul(
                        MIN_SUBNORMAL,
                        upper_add(
                            (self.rows as f64).next_up(),
                            upper_mul(4.0 * k, self.determinant_magnitude),
                        ),
                    ),
                ),
            );
            moment.input_error = upper_add(moment.input_error, error);
            previous_radius_power = radius_power;
            radius_power = upper_mul(radius_power, self.radius);
        }
    }
}

fn add_fast_determinant(
    determinant: Bounded,
    z0: Bounded,
    z1: Bounded,
    moments: &mut [BoundedSum],
    variation: &mut f64,
    maximum_latitude: &mut f64,
    row_certificate: &mut RowCertificate,
) -> Result<()> {
    *variation = upper_add(
        *variation,
        upper_mul(upper_add(determinant.value.abs(), determinant.error), 0.5),
    );
    let radius = upper_add(z0.value.abs(), z0.error).max(upper_add(z1.value.abs(), z1.error));
    *maximum_latitude = (*maximum_latitude).max(radius);
    if !radius.is_finite() || radius > 0.5 {
        for moment in moments {
            moment.push(Bounded {
                value: f64::NAN,
                error: f64::INFINITY,
            });
        }
        return Ok(());
    }
    let input_error = upper_add(z0.error, z1.error);
    row_certificate.determinant_magnitude = upper_add(
        row_certificate.determinant_magnitude,
        determinant.value.abs(),
    );
    row_certificate.determinant_error =
        upper_add(row_certificate.determinant_error, determinant.error);
    row_certificate.weighted_latitude_error = upper_add(
        row_certificate.weighted_latitude_error,
        upper_mul(determinant.value.abs(), input_error),
    );
    row_certificate.radius = row_certificate.radius.max(radius);
    row_certificate.rows = row_certificate
        .rows
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let moment_count = moments.len();
    let mut h = 1.0;
    let mut z1_power = z1.value;
    for (degree, moment) in moments.iter_mut().enumerate() {
        moment.push(Bounded::exact(determinant.value * h));
        if degree.wrapping_add(1) < moment_count {
            h = z0.value * h + z1_power;
            z1_power *= z1.value;
        }
    }
    Ok(())
}

fn cross(x0: Bounded, z0: Bounded, x1: Bounded, z1: Bounded) -> Bounded {
    x0.mul(z1).sub(x1.mul(z0))
}

fn ring_sign_fast(moments: &[BoundedSum], coefficients: &[AreaCoefficient]) -> Option<f64> {
    let (area, source_error, dot_error) = contract_fast(moments, coefficients);
    let error = upper_add(
        upper_add(source_error, dot_error),
        coefficient_dot_error(moments, coefficients),
    );
    if !area.value.is_finite() || !error.is_finite() || area.value.abs() <= error {
        None
    } else {
        Some(area.value.signum())
    }
}

fn coefficient_dot_error(moments: &[BoundedSum], coefficients: &[AreaCoefficient]) -> f64 {
    let mut error = 0.0;
    for (degree, (moment, coefficient)) in moments.iter().zip(coefficients).enumerate() {
        let denominator =
            (degree.wrapping_add(1) as u64).saturating_mul(degree.wrapping_add(2) as u64);
        error = upper_add(
            error,
            upper_div_u64(
                upper_mul(
                    coefficient.error,
                    upper_add(moment.value().abs(), moment.error()),
                ),
                denominator,
            ),
        );
    }
    error
}

fn contract_fast(moments: &[BoundedSum], coefficients: &[AreaCoefficient]) -> (Bounded, f64, f64) {
    let mut sum = BoundedSum::default();
    let mut source_error = 0.0;
    for (degree, (moment, coefficient)) in moments.iter().zip(coefficients).enumerate() {
        let denominator =
            (degree.wrapping_add(1) as u64).saturating_mul(degree.wrapping_add(2) as u64);
        let moment_value = moment.value();
        let moment_error = moment.error();
        let term = Bounded::exact(coefficient.value)
            .mul(Bounded::exact(moment_value))
            .div_u64(denominator);
        sum.push(term);
        source_error = upper_add(
            source_error,
            upper_div_u64(
                upper_mul(coefficient.value.abs(), moment_error),
                denominator,
            ),
        );
    }
    (sum.bounded(), source_error, sum.error())
}

fn contraction_work_budget(moments: &[BoundedSum], coefficients: &[AreaCoefficient]) -> f64 {
    let mut norm = 0.0;
    for (degree, (moment, coefficient)) in moments.iter().zip(coefficients).enumerate() {
        let denominator =
            (degree.wrapping_add(1) as u64).saturating_mul(degree.wrapping_add(2) as u64);
        norm = upper_add(
            norm,
            upper_div_u64(
                upper_mul(
                    upper_add(coefficient.value.abs(), coefficient.error),
                    upper_add(moment.value().abs(), moment.error()),
                ),
                denominator,
            ),
        );
    }
    norm
}

fn accumulate_exact(
    polygon: &Polygon,
    frame: GeographicFrame,
    x_origin: f64,
    y_origin: f64,
    longitude_scale: f64,
    coefficients: &[ExactDyadic],
    ring_moments: &mut [ExactDyadic],
    polygon_moments: &mut [ExactDyadic],
) -> Result<(ExactFactorialArea, ExactSourceBounds)> {
    use crate::geometry::exact_arith::ExactSign;

    for moment in ring_moments.iter_mut().chain(polygon_moments.iter_mut()) {
        *moment = ExactDyadic::zero();
    }
    let mut bounds = ExactSourceBounds::default();
    for (ring_index, ring) in polygon.rings().enumerate() {
        let mut chart_turn = 0_i64;
        let mut previous = ring.nth_coord(0);
        let first_connection = frame
            .longitude()
            .chart_connection(previous.x, ring.nth_coord(1).x)
            .expect("area geometry longitude edge was admitted");
        let previous_canonical = first_connection.start;
        let mut previous_x = exact_scaled_x(
            frame,
            previous_canonical.get(),
            chart_turn,
            x_origin,
            longitude_scale,
        )?;
        let mut previous_z = exact_scaled_y(frame, previous.y, y_origin)?;
        for index in 1..ring.coord_count() {
            let next = ring.nth_coord(index);
            let connection = frame
                .longitude()
                .chart_connection(previous.x, next.x)
                .expect("area geometry longitude edge was admitted");
            if let ChartStep::Shortest(crate::geographic::EdgeDirection::HalfTurn(direction)) =
                connection.step
            {
                return Err(GeometryErrorKind::parameter(
                    "longitude",
                    crate::geographic::EdgeDirection::HalfTurn(direction).delta(frame.longitude()),
                    "edge spans exactly half the longitude period; add a vertex",
                ));
            }
            chart_turn = chart_turn
                .checked_add(i64::from(connection.source_lift))
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let next_canonical = connection.end;
            let next_x = exact_scaled_x(
                frame,
                next_canonical.get(),
                chart_turn,
                x_origin,
                longitude_scale,
            )?;
            let next_z = exact_scaled_y(frame, next.y, y_origin)?;
            add_exact_edge(
                &previous_x,
                &previous_z,
                &next_x,
                &next_z,
                ring_moments,
                &mut bounds,
            )?;
            previous = next;
            previous_x = next_x;
            previous_z = next_z;
        }
        if chart_turn != 0 {
            let summary = ring_summary(ring, frame);
            debug_assert_eq!(chart_turn, summary.chart_turn);
            let pole = frame.chart_pole_latitude(selected_pole(summary));
            let pole_z = exact_scaled_y(frame, pole, y_origin)?;
            let start_canonical = first_connection.start;
            let start_x =
                exact_scaled_x(frame, start_canonical.get(), 0, x_origin, longitude_scale)?;
            add_exact_edge(
                &previous_x,
                &previous_z,
                &previous_x,
                &pole_z,
                ring_moments,
                &mut bounds,
            )?;
            add_exact_edge(
                &previous_x,
                &pole_z,
                &start_x,
                &pole_z,
                ring_moments,
                &mut bounds,
            )?;
            add_exact_edge(
                &start_x,
                &pole_z,
                &start_x,
                &previous_z,
                ring_moments,
                &mut bounds,
            )?;
        }
        let ring_area = ExactFactorialArea::from_derivatives(coefficients, ring_moments)?;
        let sign = match ring_area.sign() {
            ExactSign::Negative => -1_i64,
            ExactSign::Zero => 0,
            ExactSign::Positive => 1,
        };
        let polygon_sign = if ring_index == 0 {
            sign
        } else {
            sign.wrapping_neg()
        };
        if polygon_sign != 0 {
            for (destination, source) in polygon_moments.iter_mut().zip(ring_moments.iter()) {
                let term = if polygon_sign < 0 {
                    source.clone().neg()
                } else {
                    source.clone()
                };
                *destination = destination.clone().try_add(term)?;
            }
        }
        ring_moments.fill(ExactDyadic::zero());
    }
    Ok((
        ExactFactorialArea::from_derivatives(coefficients, polygon_moments)?,
        bounds,
    ))
}

struct ExactSourceBounds {
    variation: ExactDyadic,
    maximum_latitude: ExactDyadic,
}

impl Default for ExactSourceBounds {
    fn default() -> Self {
        Self {
            variation: ExactDyadic::zero(),
            maximum_latitude: ExactDyadic::zero(),
        }
    }
}

fn add_exact_edge(
    x0: &ExactDyadic,
    z0: &ExactDyadic,
    x1: &ExactDyadic,
    z1: &ExactDyadic,
    moments: &mut [ExactDyadic],
    bounds: &mut ExactSourceBounds,
) -> Result<()> {
    // Green-theorem fan integration gives z^k the symmetric divided
    // difference H_k and denominator (k + 1)(k + 2).
    let delta_x = x1.clone().try_subtract(x0.clone())?;
    let delta_z = z1.clone().try_subtract(z0.clone())?;
    let determinant = x0
        .try_product(&delta_z)?
        .try_subtract(z0.try_product(&delta_x)?)?;
    let half_determinant = determinant
        .clone()
        .abs()
        .try_product(&ExactDyadic::from_f64(0.5))?;
    bounds.variation = bounds.variation.clone().try_add(half_determinant)?;
    for latitude in [z0, z1] {
        let latitude = latitude.clone().abs();
        if latitude.cmp(&bounds.maximum_latitude).is_gt() {
            bounds.maximum_latitude = latitude;
        }
    }
    let mut z1_power = ExactDyadic::one();
    let mut h = ExactDyadic::one();
    for moment in moments.iter_mut() {
        *moment = moment.clone().try_add(determinant.try_product(&h)?)?;
        z1_power = z1_power.try_product(z1)?;
        h = z0.try_product(&h)?.try_add(z1_power.clone())?;
    }
    Ok(())
}

fn exact_scaled_x(
    frame: GeographicFrame,
    canonical_x: f64,
    turn: i64,
    origin: f64,
    scale: f64,
) -> Result<ExactDyadic> {
    // Preserve canonical source endpoints and ChartStep's integral lift; never
    // rebuild a lifted endpoint from a rounded floating-point edge width.
    let mut lifted = ExactDyadic::from_f64(canonical_x);
    if turn != 0 {
        let offset = ExactDyadic::from_f64(frame.longitude().period())
            .try_product(&ExactDyadic::from_i64(turn))?;
        lifted = lifted.try_add(offset)?;
    }
    lifted
        .try_subtract(ExactDyadic::from_f64(origin))?
        .try_product(&ExactDyadic::from_f64(
            frame.longitude().radians_per_unit() * frame.longitude_orientation_sign(),
        ))?
        .try_product(&ExactDyadic::from_f64(scale))
}

fn exact_scaled_y(frame: GeographicFrame, latitude: f64, origin: f64) -> Result<ExactDyadic> {
    ExactDyadic::from_f64(latitude)
        .try_subtract(ExactDyadic::from_f64(origin))?
        .try_product(&ExactDyadic::from_f64(
            frame.latitude_radians_per_unit() * frame.latitude_orientation_sign(),
        ))?
        .try_product(&ExactDyadic::from_f64(LATITUDE_SCALE))
}

fn exact_difference_product(left: f64, right: f64, scale: f64) -> Result<Bounded> {
    let exact = ExactDyadic::from_f64(left)
        .try_subtract(ExactDyadic::from_f64(right))?
        .try_product(&ExactDyadic::from_f64(scale))?;
    exact_to_bounded(&exact)
}

fn scaled_x(
    frame: GeographicFrame,
    canonical_x: f64,
    turn: i64,
    origin: f64,
    coordinate_scale: Bounded,
) -> Bounded {
    let turn_error = if turn.unsigned_abs() <= (1_u64 << 53) {
        0.0
    } else {
        f64::INFINITY
    };
    let mut lifted = Bounded::exact(canonical_x);
    if turn != 0 {
        lifted = lifted.add(Bounded::exact(frame.longitude().period()).mul(Bounded {
            value: turn as f64,
            error: turn_error,
        }));
    }
    lifted.sub(Bounded::exact(origin)).mul(coordinate_scale)
}

fn scaled_y(latitude: f64, origin: f64, coordinate_scale: Bounded) -> Bounded {
    Bounded::exact(latitude)
        .sub(Bounded::exact(origin))
        .mul(coordinate_scale)
}

fn exact_to_bounded(value: &ExactDyadic) -> Result<Bounded> {
    let rounded = round_ratio(value, &ExactDyadic::one())?;
    let (lower, upper) = outward_ratio_bounds(value, &ExactDyadic::one())?;
    let error = (rounded - lower).abs().max((upper - rounded).abs());
    Ok(Bounded {
        value: rounded,
        error,
    })
}

fn ring_summary(ring: &Ring, frame: GeographicFrame) -> RingSummary {
    RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
        .expect("geographic area input was admitted before measurement")
}

fn selected_pole(summary: RingSummary) -> ChartPole {
    summary
        .bounded_pole()
        .expect("geographic area admission rejects ambiguous full-turn pole selection")
}

fn resize_moments(values: &mut Vec<BoundedSum>, count: usize) {
    values.clear();
    values.resize_with(count, BoundedSum::default);
}

fn resize_exact(values: &mut Vec<ExactDyadic>, count: usize) {
    values.clear();
    values.resize_with(count, ExactDyadic::zero);
}

fn ceil_log2(value: usize) -> u32 {
    debug_assert!(value > 0);
    value.wrapping_sub(1).bit_width()
}

fn scale_area_fast(
    integral: f64,
    density: &GeographicAreaDensity,
    x_exponent: i32,
) -> Option<(f64, PositiveBound)> {
    if !integral.is_normal() {
        return None;
    }
    let scale_exponent = x_exponent.checked_neg()?.checked_add(3)?;
    let power = 2.0_f64.powi(scale_exponent);
    if !power.is_normal() {
        return None;
    }
    let scale = Bounded::exact(density.semi_major)
        .mul(Bounded::exact(density.semi_major))
        .mul(Bounded::exact(power));
    if !scale.value.is_normal() {
        return None;
    }
    if !scale.error.is_finite() || scale.error >= scale.value {
        return None;
    }
    let lower_scale = (scale.value - scale.error).next_down();
    if !lower_scale.is_normal() {
        return None;
    }
    let relative_scale_error = PositiveBound::from_f64(scale.error)?
        .divide_by_upper(PositiveBound::from_f64(lower_scale)?)?;
    let normalized_scale_error =
        relative_scale_error.multiply_upper(PositiveBound::from_f64(integral.abs())?)?;
    let area = scale.value * integral;
    if !area.is_finite() || !area.is_normal() {
        return None;
    }
    Some((canonicalize_zero(area), normalized_scale_error))
}

fn area_scale_bound(density: &GeographicAreaDensity, x_exponent: i32) -> Result<PositiveBound> {
    let scale_exponent = x_exponent
        .checked_neg()
        .and_then(|exponent| exponent.checked_add(3))
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let semi_major = PositiveBound::from_f64(density.semi_major)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    semi_major
        .multiply_upper(semi_major)
        .and_then(|value| value.scale_power_of_two(scale_exponent))
        .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
}

fn operation_count(order: usize) -> Result<u64> {
    let count = order
        .checked_add(1)
        .and_then(|degree_count| degree_count.checked_mul(3))
        .and_then(|count| count.checked_add(3))
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let count = u64::try_from(count).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    if count >= (1_u64 << 53) {
        return Err(crate::error::ResourceError::SizeOverflow.into());
    }
    Ok(count)
}

fn accept_work_budget(
    error: PositiveBound,
    norm: PositiveBound,
    scale: &ExactDyadic,
    order: usize,
) -> Result<bool> {
    let operation_count = operation_count(order)?;
    let error = error.to_exact()?;
    let norm = norm.to_exact()?;
    if ExactFactorialArea::dyadic_within_gamma(&error, &norm, operation_count)? {
        return Ok(true);
    }
    let physical_error = scale.try_product(&error)?;
    let half_minimum_subnormal = ExactDyadic::try_from_u128(false, 1, -1075)?;
    Ok(physical_error.cmp(&half_minimum_subnormal).is_le())
}

fn rounding_error(value: f64) -> f64 {
    if value.is_finite() {
        upper_add(value.abs() * f64::EPSILON, MIN_SUBNORMAL)
    } else {
        f64::INFINITY
    }
}

fn upper_add(left: f64, right: f64) -> f64 {
    if left == 0.0 {
        return right;
    }
    if right == 0.0 {
        return left;
    }
    let sum = left + right;
    if sum.is_finite() {
        sum.next_up()
    } else {
        f64::INFINITY
    }
}

fn upper_mul(left: f64, right: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        return 0.0;
    }
    let product = left * right;
    if product.is_finite() {
        product.next_up()
    } else {
        f64::INFINITY
    }
}

fn upper_div_u64(value: f64, denominator: u64) -> f64 {
    if value == 0.0 {
        return 0.0;
    }
    let quotient = value / denominator as f64;
    if quotient.is_finite() {
        quotient.next_up()
    } else {
        f64::INFINITY
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PositiveBound {
    mantissa: f64,
    exponent: i32,
}

impl PositiveBound {
    const ZERO: Self = Self {
        mantissa: 0.0,
        exponent: 0,
    };

    fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() || value < 0.0 {
            return None;
        }
        if value == 0.0 {
            return Some(Self::ZERO);
        }
        let bits = value.to_bits();
        let fraction = bits & ((1_u64 << 52) - 1);
        let encoded_exponent = ((bits >> 52) & 0x7FF) as i32;
        if encoded_exponent != 0 {
            return Some(Self {
                mantissa: f64::from_bits((1023_u64 << 52) | fraction),
                exponent: encoded_exponent.wrapping_sub(1023),
            });
        }
        let leading = fraction.ilog2();
        let significand = fraction << 52_u32.wrapping_sub(leading);
        Some(Self {
            mantissa: f64::from_bits((1023_u64 << 52) | (significand & ((1_u64 << 52) - 1))),
            exponent: (leading as i32).wrapping_sub(1074),
        })
    }

    fn from_parts(mantissa: f64, exponent: i32) -> Option<Self> {
        if mantissa == 0.0 {
            return Some(Self::ZERO);
        }
        let normalized = Self::from_f64(mantissa)?;
        Some(Self {
            exponent: normalized.exponent.checked_add(exponent)?,
            ..normalized
        })
    }

    fn is_zero(self) -> bool {
        self.mantissa == 0.0
    }

    fn multiply_upper(self, other: Self) -> Option<Self> {
        if self.is_zero() || other.is_zero() {
            return Some(Self::ZERO);
        }
        Self::from_parts(
            (self.mantissa * other.mantissa).next_up(),
            self.exponent.checked_add(other.exponent)?,
        )
    }

    fn add_upper(self, other: Self) -> Option<Self> {
        if self.is_zero() {
            return Some(other);
        }
        if other.is_zero() {
            return Some(self);
        }
        let (large, small) = if self.exponent >= other.exponent {
            (self, other)
        } else {
            (other, self)
        };
        let difference = small.exponent.checked_sub(large.exponent)?;
        if difference < -1074 {
            return Self::from_parts(large.mantissa.next_up(), large.exponent);
        }
        let aligned = small.mantissa * 2.0_f64.powi(difference);
        if aligned == 0.0 {
            return Self::from_parts(large.mantissa.next_up(), large.exponent);
        }
        Self::from_parts((large.mantissa + aligned).next_up(), large.exponent)
    }

    fn divide_upper(self, denominator: u64) -> Option<Self> {
        if denominator == 0 {
            return None;
        }
        if self.is_zero() {
            return Some(Self::ZERO);
        }
        Self::from_parts(
            (self.mantissa / denominator as f64).next_up(),
            self.exponent,
        )
    }

    fn power_upper(self, mut exponent: usize) -> Option<Self> {
        let mut result = Self::from_f64(1.0)?;
        let mut base = self;
        while exponent != 0 {
            if exponent & 1 != 0 {
                result = result.multiply_upper(base)?;
            }
            exponent >>= 1;
            if exponent != 0 {
                base = base.multiply_upper(base)?;
            }
        }
        Some(result)
    }

    fn divide_by_upper(self, denominator: Self) -> Option<Self> {
        if denominator.is_zero() {
            return None;
        }
        if self.is_zero() {
            return Some(Self::ZERO);
        }
        Self::from_parts(
            (self.mantissa / denominator.mantissa).next_up(),
            self.exponent.checked_sub(denominator.exponent)?,
        )
    }

    fn scale_power_of_two(self, exponent: i32) -> Option<Self> {
        if self.is_zero() {
            return Some(self);
        }
        Some(Self {
            mantissa: self.mantissa,
            exponent: self.exponent.checked_add(exponent)?,
        })
    }

    fn maximum(self, other: Self) -> Self {
        if self.is_zero() {
            return other;
        }
        if other.is_zero() {
            return self;
        }
        if self.exponent > other.exponent
            || (self.exponent == other.exponent && self.mantissa >= other.mantissa)
        {
            self
        } else {
            other
        }
    }

    fn ceil_log2(self) -> Option<i32> {
        if self.is_zero() {
            return Some(i32::MIN);
        }
        self.exponent.checked_add(i32::from(self.mantissa > 1.0))
    }

    fn ceil_to_usize(self) -> Option<usize> {
        if self.is_zero() {
            return Some(0);
        }
        if self.exponent < 0 {
            let value = self.mantissa * 2.0_f64.powi(self.exponent);
            return Some(value.ceil() as usize);
        }
        let exponent = u32::try_from(self.exponent).ok()?;
        if exponent >= usize::BITS {
            return None;
        }
        let value = self.mantissa * 2.0_f64.powi(self.exponent);
        if !value.is_finite() || value.ceil() > usize::MAX as f64 {
            return None;
        }
        Some(value.ceil() as usize)
    }

    fn to_exact(self) -> Result<ExactDyadic> {
        if self.is_zero() {
            return Ok(ExactDyadic::zero());
        }
        let mut exact = ExactDyadic::from_f64(self.mantissa);
        exact.exponent = exact
            .exponent
            .checked_add(i64::from(self.exponent))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(exact)
    }
}

#[derive(Clone)]
struct TaylorTail {
    amplitude: [PositiveBound; MAX_AUTHALIC_HARMONICS],
    argument: [PositiveBound; MAX_AUTHALIC_HARMONICS],
    term: [PositiveBound; MAX_AUTHALIC_HARMONICS],
    count: usize,
    degree: usize,
    variation: PositiveBound,
}

impl TaylorTail {
    fn new(
        density: &GeographicAreaDensity,
        variation: PositiveBound,
        maximum_latitude: PositiveBound,
    ) -> Result<Self> {
        let mut state = Self {
            amplitude: [PositiveBound::ZERO; MAX_AUTHALIC_HARMONICS],
            argument: [PositiveBound::ZERO; MAX_AUTHALIC_HARMONICS],
            term: [PositiveBound::ZERO; MAX_AUTHALIC_HARMONICS],
            count: density.count,
            degree: 1,
            variation,
        };
        for (((harmonic, argument_slot), amplitude_slot), term_slot) in density
            .harmonics
            .iter()
            .zip(&mut state.argument)
            .zip(&mut state.amplitude)
            .zip(&mut state.term)
            .take(density.count)
        {
            let sine_cosine =
                PositiveBound::from_f64(upper_add(harmonic.sine.abs(), harmonic.cosine.abs()))
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let amplitude = PositiveBound::from_f64(harmonic.coefficient.abs())
                .and_then(|value| value.multiply_upper(PositiveBound::from_f64(harmonic.order)?))
                .and_then(|value| value.multiply_upper(PositiveBound::from_f64(0.5)?))
                .and_then(|value| value.multiply_upper(sine_cosine))
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let argument = maximum_latitude
                .multiply_upper(
                    PositiveBound::from_f64(harmonic.order * (1.0 / LATITUDE_SCALE))
                        .ok_or(crate::error::ResourceError::SizeOverflow)?,
                )
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            *argument_slot = argument;
            *amplitude_slot = amplitude;
            *term_slot = amplitude
                .multiply_upper(variation)
                .and_then(|term| term.multiply_upper(argument))
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        Ok(state)
    }

    fn bound_at(&mut self, omitted_degree: usize) -> Result<PositiveBound> {
        while self.degree < omitted_degree {
            let next_degree = self
                .degree
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            for (term, argument) in self.term.iter_mut().zip(&self.argument).take(self.count) {
                *term = (*term)
                    .multiply_upper(*argument)
                    .and_then(|term| term.divide_upper(next_degree as u64))
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            }
            self.degree = next_degree;
        }
        let mut sum = PositiveBound::ZERO;
        for term in self.term.iter().take(self.count) {
            sum = sum
                .add_upper(*term)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        Ok(sum)
    }

    fn sufficient_order(&self, scale: PositiveBound, initial_order: usize) -> Result<usize> {
        let maximum_argument = self
            .argument
            .iter()
            .take(self.count)
            .copied()
            .fold(PositiveBound::ZERO, PositiveBound::maximum);
        let mut amplitude_sum = PositiveBound::ZERO;
        for amplitude in self.amplitude.iter().take(self.count) {
            amplitude_sum = amplitude_sum
                .add_upper(*amplitude)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        let bound = scale
            .multiply_upper(self.variation)
            .and_then(|value| value.multiply_upper(amplitude_sum))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if bound.is_zero() || maximum_argument.is_zero() {
            return Ok(initial_order);
        }
        let twelve_r = maximum_argument
            .multiply_upper(
                PositiveBound::from_f64(12.0).ok_or(crate::error::ResourceError::SizeOverflow)?,
            )
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let first_ceiling = twelve_r
            .ceil_to_usize()
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let b = bound
            .ceil_log2()
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let factorial_ceiling = b
            .checked_add(1077)
            .ok_or(crate::error::ResourceError::SizeOverflow)?
            .div_euclid(2)
            .max(1);
        let omitted_degree = first_ceiling.max(
            usize::try_from(factorial_ceiling)
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        );
        let ratio = maximum_argument
            .multiply_upper(
                PositiveBound::from_f64(3.0).ok_or(crate::error::ResourceError::SizeOverflow)?,
            )
            .and_then(|value| value.divide_upper(omitted_degree as u64))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let geometric_tail = bound
            .multiply_upper(
                ratio
                    .power_upper(omitted_degree)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
            )
            .ok_or(crate::error::ResourceError::SizeOverflow)?
            .to_exact()?;
        let lattice = ExactDyadic::try_from_u128(false, 1, -1075)?;
        if geometric_tail.cmp(&lattice).is_gt() {
            return Err(GeometryErrorKind::message(format!(
                "geographic area Taylor ceiling failed: m={omitted_degree}, B={bound:?}, r={maximum_argument:?}, ratio={ratio:?}, tail={geometric_tail:?}"
            )));
        }
        Ok(omitted_degree.saturating_sub(1).max(initial_order))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::exact_arith::ExactSign;
    include!("geographic_area/row_certificate_tests.rs");
    include!("geographic_area/coefficient_certificate_tests.rs");
    include!("geographic_area/rectangle_moment_tests.rs");
    include!("geographic_area/source_current_tests.rs");

    fn assert_receipt_bound_covers(terms: &[(f64, f64)]) {
        let mut sum = BoundedSum::default();
        let mut exact_center = ExactDyadic::zero();
        let mut exact_error = ExactDyadic::zero();
        for &(value, input_error) in terms {
            sum.push(Bounded {
                value,
                error: input_error,
            });
            exact_center = exact_center.try_add(ExactDyadic::from_f64(value)).unwrap();
            exact_error = exact_error
                .try_add(ExactDyadic::from_f64(input_error))
                .unwrap();
        }
        let rounded = ExactDyadic::from_f64(sum.value());
        for endpoint in [
            exact_center
                .clone()
                .try_subtract(exact_error.clone())
                .unwrap(),
            exact_center.try_add(exact_error).unwrap(),
        ] {
            let difference = endpoint.try_subtract(rounded.clone()).unwrap();
            let (lower, upper) = outward_ratio_bounds(&difference, &ExactDyadic::one()).unwrap();
            let deviation = lower.abs().max(upper.abs());
            assert!(
                deviation <= sum.error(),
                "sum-receipt deviation={deviation:.17e}, bound={:.17e}, terms={terms:?}",
                sum.error()
            );
        }
    }

    #[test]
    fn positive_bound_decomposes_binary64_extrema_exactly() {
        let cases = [
            (1_u64, 0x3FF0_0000_0000_0000_u64, -1074),
            (0x000F_FFFF_FFFF_FFFF, 0x3FFF_FFFF_FFFF_FFFE, -1023),
            (0x0010_0000_0000_0000, 0x3FF0_0000_0000_0000, -1022),
            (0x7FEF_FFFF_FFFF_FFFF, 0x3FFF_FFFF_FFFF_FFFF, 1023),
        ];
        for (input_bits, mantissa_bits, exponent) in cases {
            let bound = PositiveBound::from_f64(f64::from_bits(input_bits)).unwrap();
            assert_eq!(bound.mantissa.to_bits(), mantissa_bits);
            assert_eq!(bound.exponent, exponent);
        }

        assert!(PositiveBound::from_f64(-1.0).is_none());
        assert!(PositiveBound::from_f64(f64::INFINITY).is_none());
        assert!(PositiveBound::from_f64(f64::NAN).is_none());
        assert!(PositiveBound::from_f64(-0.0).unwrap().is_zero());
    }

    #[test]
    fn positive_bound_parts_keep_exact_normalization_and_exponent_failures() {
        for (mantissa, exponent, expected_mantissa, expected_exponent) in [
            (8.0, -3, 1.0_f64.to_bits(), 0),
            (0.25, 2, 1.0_f64.to_bits(), 0),
            (MIN_SUBNORMAL, 1074, 1.0_f64.to_bits(), 0),
            (f64::MAX, -1023, 0x3FFF_FFFF_FFFF_FFFF, 0),
        ] {
            let bound = PositiveBound::from_parts(mantissa, exponent).unwrap();
            assert_eq!(bound.mantissa.to_bits(), expected_mantissa);
            assert_eq!(bound.exponent, expected_exponent);
        }

        assert!(PositiveBound::from_parts(-0.0, i32::MAX).unwrap().is_zero());
        assert!(PositiveBound::from_parts(2.0, i32::MAX).is_none());
        assert!(PositiveBound::from_parts(0.5, i32::MIN).is_none());
        assert!(PositiveBound::from_parts(-1.0, 0).is_none());
        assert!(PositiveBound::from_parts(f64::INFINITY, 0).is_none());
    }

    #[test]
    fn positive_bound_upward_arithmetic_covers_exact_subnormal_targets() {
        let one = PositiveBound::from_f64(1.0).unwrap();
        let one_product = one.multiply_upper(one).unwrap().to_exact().unwrap();
        assert!(one_product.cmp(&ExactDyadic::one()).is_gt());

        let smallest = PositiveBound::from_f64(MIN_SUBNORMAL).unwrap();
        let tiny_product = smallest
            .multiply_upper(smallest)
            .unwrap()
            .to_exact()
            .unwrap();
        let exact_tiny_product = ExactDyadic::try_from_u128(false, 1, -2148).unwrap();
        assert!(tiny_product.cmp(&exact_tiny_product).is_gt());

        let one_plus_smallest = one.add_upper(smallest).unwrap().to_exact().unwrap();
        let exact_sum = ExactDyadic::one()
            .try_add(ExactDyadic::from_f64(MIN_SUBNORMAL))
            .unwrap();
        assert!(one_plus_smallest.cmp(&exact_sum).is_gt());
    }

    #[test]
    fn area_scaling_preserves_exact_values_and_nonfinite_receipts() {
        let frame = GeographicFrame::crs84();
        let x = scaled_x(frame, 20.0, 1, 10.0, Bounded::exact(0.125));
        assert_eq!(x.value.to_bits(), 46.25_f64.to_bits());
        assert!(x.error.is_finite() && x.error >= 0.0);

        let y = scaled_y(17.0, 5.0, Bounded::exact(0.125));
        assert_eq!(y.value.to_bits(), 1.5_f64.to_bits());
        let reversed_y = scaled_y(17.0, 5.0, Bounded::exact(-0.125));
        assert_eq!(reversed_y.value.to_bits(), (-1.5_f64).to_bits());

        let inexact_turn = scaled_x(frame, 20.0, (1_i64 << 53) + 1, 0.0, Bounded::exact(1.0));
        assert!(inexact_turn.error.is_infinite());

        let underflowed_y = scaled_y(MIN_SUBNORMAL, 0.0, Bounded::exact(0.5));
        assert_eq!(underflowed_y.value.to_bits(), 0);
        let exact_half_subnormal = ExactDyadic::try_from_u128(false, 1, -1075).unwrap();
        let half_subnormal_residual = exact_half_subnormal
            .try_subtract(ExactDyadic::from_f64(underflowed_y.value))
            .unwrap()
            .abs();
        assert!(
            half_subnormal_residual
                .cmp(&ExactDyadic::from_f64(underflowed_y.error))
                .is_le()
        );

        let overflowing_y = scaled_y(f64::MAX, -f64::MAX, Bounded::exact(2.0));
        assert!(!overflowing_y.value.is_finite());
        assert!(overflowing_y.error.is_infinite());

        let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
        coefficients[0] = 1.0;
        let density = GeographicAreaDensity::new(coefficients, 1, 1.0, 0.0, GeodeticPole::North);
        let (positive_area, scale_error) = scale_area_fast(1.0, &density, -3).unwrap();
        assert_eq!(positive_area.to_bits(), 64.0_f64.to_bits());
        assert!(!scale_error.is_zero());
        let exact_scale_bound = area_scale_bound(&density, -3).unwrap().to_exact().unwrap();
        assert!(exact_scale_bound.cmp(&ExactDyadic::from_i64(64)).is_ge());

        let (negative_area, _) = scale_area_fast(-1.0, &density, -3).unwrap();
        assert_eq!(negative_area.to_bits(), (-64.0_f64).to_bits());
    }

    #[test]
    fn ceil_log2_matches_positive_integer_width_boundaries() {
        for (value, expected) in [
            (1, 0),
            (2, 1),
            (3, 2),
            (4, 2),
            (5, 3),
            (1_usize << (usize::BITS - 1), usize::BITS - 1),
            (usize::MAX, usize::BITS),
        ] {
            assert_eq!(ceil_log2(value), expected);
        }
    }

    #[test]
    fn eft_receipts_cover_cancellation_and_subnormal_inputs() {
        assert_receipt_bound_covers(&[(1.0e16, 0.0), (1.0, 2.0 * f64::EPSILON), (-1.0e16, 0.0)]);

        let mut mixed_scale = Vec::with_capacity(162);
        for exponent in -40..=40 {
            let value = 2.0_f64.powi(exponent);
            mixed_scale.push((value, value * f64::EPSILON));
            mixed_scale.push((-value, 0.0));
        }
        mixed_scale.extend(std::iter::repeat_n((MIN_SUBNORMAL, MIN_SUBNORMAL), 32));
        assert_receipt_bound_covers(&mixed_scale);

        let mut overflowing = BoundedSum::default();
        overflowing.push(Bounded::exact(1.0e308));
        overflowing.push(Bounded::exact(1.0e308));
        overflowing.push(Bounded::exact(-1.0e308));
        assert!(overflowing.error().is_infinite());
        assert!(overflowing.value().is_nan());
    }

    #[test]
    fn exact_reversed_source_has_zero_moments_but_positive_tail() {
        let points = [
            (0.0, 0.0),
            (1.0, 0.0),
            (1.0, 1.0 / 16.0),
            (0.0, 1.0 / 16.0),
            (0.0, 0.0),
            (0.0, 1.0 / 16.0),
            (1.0, 1.0 / 16.0),
            (1.0, 0.0),
            (0.0, 0.0),
        ];
        let mut moments = vec![ExactDyadic::zero(); 12];
        let mut source_bounds = ExactSourceBounds::default();
        for edge in points.windows(2) {
            let [(x0, z0), (x1, z1)] = edge else {
                unreachable!("each source edge has two endpoints");
            };
            add_exact_edge(
                &ExactDyadic::from_f64(*x0),
                &ExactDyadic::from_f64(*z0),
                &ExactDyadic::from_f64(*x1),
                &ExactDyadic::from_f64(*z1),
                &mut moments,
                &mut source_bounds,
            )
            .unwrap();
        }
        assert!(moments.iter().all(ExactDyadic::is_zero));
        assert_eq!(source_bounds.variation.sign(), ExactSign::Positive);

        let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
        coefficients[0] = 1.0;
        let density = GeographicAreaDensity::new(
            coefficients,
            1,
            6_378_137.0,
            std::f64::consts::FRAC_PI_2,
            GeodeticPole::North,
        );
        let scale = density.area_scale(0).unwrap();
        let scale_bound = area_scale_bound(&density, 0).unwrap();
        let mut tail = TaylorTail::new(
            &density,
            positive_exact_bound(&source_bounds.variation).unwrap(),
            positive_exact_bound(&source_bounds.maximum_latitude).unwrap(),
        )
        .unwrap();
        let ceiling = tail
            .sufficient_order(scale_bound, INITIAL_TAYLOR_ORDER)
            .unwrap();
        let first_tail = tail.bound_at(INITIAL_TAYLOR_ORDER + 1).unwrap();
        assert!(!first_tail.is_zero());

        let mut accepted_order = None;
        for order in INITIAL_TAYLOR_ORDER..=ceiling {
            let error = tail.bound_at(order + 1).unwrap();
            if accept_work_budget(error, PositiveBound::ZERO, &scale, order).unwrap() {
                accepted_order = Some(order);
                break;
            }
        }
        assert!(accepted_order.is_some());
        assert!(accepted_order.unwrap() <= ceiling);
    }

    #[test]
    fn taylor_tail_bounds_an_independent_single_harmonic_remainder() {
        let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
        coefficients[0] = 1.0;
        let density = GeographicAreaDensity::new(coefficients, 1, 1.0, 0.0, GeodeticPole::North);
        let order = 4;
        let latitude = 1.0 / 16.0;
        let mut polynomial = Vec::new();
        density.fill_coefficients(order, &mut polynomial).unwrap();
        let mut approximation = 0.0;
        let mut power = 1.0;
        for coefficient in &polynomial {
            approximation += coefficient.value * power;
            power *= latitude;
        }
        let exact_field = -0.5 * (8.0 * latitude).sin();
        let independently_evaluated_remainder = (exact_field - approximation).abs();
        let mut tail = TaylorTail::new(
            &density,
            PositiveBound::from_f64(1.0).unwrap(),
            PositiveBound::from_f64(latitude).unwrap(),
        )
        .unwrap();
        let upper = tail.bound_at(order + 1).unwrap().to_exact().unwrap();
        assert!(
            upper
                .cmp(&ExactDyadic::from_f64(independently_evaluated_remainder))
                .is_ge()
        );
    }

    #[test]
    fn zero_moment_prefix_does_not_hide_a_nonzero_later_moment() {
        let mut points = vec![(0.0, 0.0)];
        for (index, weight) in [1.0, 4.0, 6.0, 4.0, 1.0].into_iter().enumerate() {
            let low = index as f64 / 8.0;
            let high = low + 1.0 / 256.0;
            let width = weight / 256.0;
            let mut rectangle = vec![
                (0.0, low),
                (width, low),
                (width, high),
                (0.0, high),
                (0.0, low),
            ];
            if index % 2 != 0 {
                rectangle.reverse();
            }
            points.extend(rectangle);
            points.push((0.0, 0.0));
        }
        let mut moments = vec![ExactDyadic::zero(); 5];
        let mut source_bounds = ExactSourceBounds::default();
        for edge in points.windows(2) {
            let [(x0, z0), (x1, z1)] = edge else {
                unreachable!("each source edge has two endpoints");
            };
            add_exact_edge(
                &ExactDyadic::from_f64(*x0),
                &ExactDyadic::from_f64(*z0),
                &ExactDyadic::from_f64(*x1),
                &ExactDyadic::from_f64(*z1),
                &mut moments,
                &mut source_bounds,
            )
            .unwrap();
        }
        assert!(moments[..4].iter().all(ExactDyadic::is_zero));
        assert_eq!(
            round_ratio(&moments[4], &ExactDyadic::one()).unwrap(),
            45.0 / 16_777_216.0
        );
        assert_eq!(source_bounds.variation.sign(), ExactSign::Positive);
    }

    #[test]
    fn fast_and_exact_contractions_include_the_last_active_degree() {
        let mut moments = Vec::with_capacity(3);
        for value in [3.0, 4.0, 5.0] {
            let mut moment = BoundedSum::default();
            moment.push(Bounded::exact(value));
            moments.push(moment);
        }
        let coefficients = [2.0, 6.0, 12.0].map(|value| AreaCoefficient { value, error: 0.0 });
        let (integral, ..) = contract_fast(&moments, &coefficients);
        assert_eq!(integral.value, 12.0);

        let mut last_only_fast_moments = Vec::with_capacity(3);
        for value in [0.0, 0.0, 5.0] {
            let mut moment = BoundedSum::default();
            moment.push(Bounded::exact(value));
            last_only_fast_moments.push(moment);
        }
        let last_only_fast_coefficients =
            [0.0, 0.0, 12.0].map(|value| AreaCoefficient { value, error: 0.0 });
        let (last_only_fast, ..) =
            contract_fast(&last_only_fast_moments, &last_only_fast_coefficients);
        assert_eq!(last_only_fast.value, 5.0);

        let exact_moments = [3.0, 4.0, 5.0].map(ExactDyadic::from_f64);
        let exact_derivatives = [2.0, 6.0, 24.0].map(ExactDyadic::from_f64);
        let exact = ExactFactorialArea::from_derivatives(&exact_derivatives, &exact_moments)
            .unwrap()
            .scaled_round(&ExactDyadic::one())
            .unwrap();
        assert_eq!(exact, 12.0);

        let last_only_moments = [
            ExactDyadic::zero(),
            ExactDyadic::zero(),
            ExactDyadic::from_f64(5.0),
        ];
        let last_only_derivatives = [
            ExactDyadic::zero(),
            ExactDyadic::zero(),
            ExactDyadic::from_f64(24.0),
        ];
        let last_only =
            ExactFactorialArea::from_derivatives(&last_only_derivatives, &last_only_moments)
                .unwrap()
                .scaled_round(&ExactDyadic::one())
                .unwrap();
        assert_eq!(last_only, 5.0);
    }

    #[test]
    fn bounded_edge_moments_match_literal_low_degree_divided_differences() {
        let mut moments = std::iter::repeat_with(BoundedSum::default)
            .take(3)
            .collect::<Vec<_>>();
        let mut variation = 0.0;
        let mut maximum_latitude = 0.0;
        let mut row_certificate = RowCertificate::default();
        add_fast_determinant(
            Bounded::exact(3.0 / 32.0),
            Bounded::exact(1.0 / 4.0),
            Bounded::exact(1.0 / 8.0),
            &mut moments,
            &mut variation,
            &mut maximum_latitude,
            &mut row_certificate,
        )
        .unwrap();

        for (moment, expected) in moments.iter().zip([3.0 / 32.0, 9.0 / 256.0, 21.0 / 2048.0]) {
            assert_eq!(moment.value(), expected);
            let residual = ExactDyadic::from_f64(expected)
                .try_subtract(ExactDyadic::from_f64(moment.value()))
                .unwrap()
                .abs();
            assert!(residual.cmp(&ExactDyadic::from_f64(moment.error())).is_le());
        }
    }

    #[test]
    fn exact_factorial_contraction_retains_scaled_terms_below_binary64_factors() {
        const DEGREE: usize = 1025;
        let mut harmonics = [Harmonic {
            coefficient: 0.0,
            order: 1.0,
            sine: 0.0,
            cosine: 0.0,
        }; MAX_AUTHALIC_HARMONICS];
        harmonics[8] = Harmonic {
            coefficient: 2.0_f64.powi(-64),
            order: 17.0,
            sine: 0.0,
            cosine: 1.0,
        };
        let density = GeographicAreaDensity {
            harmonics,
            count: MAX_AUTHALIC_HARMONICS,
            semi_major: 1.0,
            reference_pole: GeodeticPole::North,
        };

        let mut fast_coefficients = Vec::new();
        density
            .fill_coefficients(DEGREE, &mut fast_coefficients)
            .unwrap();
        assert_eq!(fast_coefficients[DEGREE].value, 0.0);

        let mut exact_derivatives = Vec::new();
        density
            .fill_exact_derivatives(DEGREE, &mut exact_derivatives)
            .unwrap();
        let determinant = ExactDyadic::try_from_u128(false, 3, -8).unwrap();
        let radius = ExactDyadic::try_from_u128(false, 3, -3).unwrap();
        let mut exact_moment = determinant;
        for _ in 0..DEGREE {
            exact_moment = exact_moment.try_product(&radius).unwrap();
        }
        let mut fast_moment = 3.0 / 256.0;
        for _ in 0..DEGREE {
            fast_moment *= 3.0 / 8.0;
        }
        assert_eq!(fast_moment, 0.0);

        let mut moments = vec![ExactDyadic::zero(); DEGREE + 1];
        moments[DEGREE] = exact_moment;
        let mut derivatives = vec![ExactDyadic::zero(); DEGREE + 1];
        derivatives[DEGREE] = exact_derivatives[DEGREE].clone();
        let area = ExactFactorialArea::from_derivatives(&derivatives, &moments).unwrap();
        let scale = ExactDyadic::try_from_u128(false, 1, 2055).unwrap();
        let actual = area.scaled_round(&scale).unwrap();
        let expected = -6.936_318_504_392_211e-301;
        assert!(actual.is_normal() && actual < 0.0);
        assert!((actual / expected - 1.0).abs() <= 2.0e-15);
    }

    #[test]
    fn exact_publication_handles_scale_overflow_underflow_and_subnormal_quantization() {
        let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
        coefficients[0] = 1.0;

        let large_axis = 1.0e155;
        let large_density = GeographicAreaDensity::new(
            coefficients,
            1,
            large_axis,
            std::f64::consts::FRAC_PI_2,
            GeodeticPole::North,
        );
        assert!(!(large_axis * large_axis).is_finite());
        assert!(scale_area_fast(1.0e-308, &large_density, -6).is_none());
        let two = ExactDyadic::from_i64(2);
        let large_derivatives = [ExactDyadic::from_f64(1.0e-308).try_product(&two).unwrap()];
        let unit_moment = [ExactDyadic::one()];
        let normalized_area =
            ExactFactorialArea::from_derivatives(&large_derivatives, &unit_moment).unwrap();
        let actual = normalized_area
            .scaled_round(&large_density.area_scale(-6).unwrap())
            .unwrap();
        let expected = 51_199.999_999_999_99;
        assert!(actual.is_finite());
        assert!((actual / expected - 1.0).abs() <= 2.0e-15);

        let small_axis = 2.0_f64.powi(-538);
        let small_density = GeographicAreaDensity::new(
            coefficients,
            1,
            small_axis,
            std::f64::consts::FRAC_PI_2,
            GeodeticPole::North,
        );
        assert_eq!(small_axis * small_axis, 0.0);
        assert!(scale_area_fast(f64::MAX, &small_density, -6).is_none());
        let large_integral_derivatives = [ExactDyadic::try_from_u128(false, 1, 1074).unwrap()];
        let normalized_area =
            ExactFactorialArea::from_derivatives(&large_integral_derivatives, &unit_moment)
                .unwrap();
        assert_eq!(
            normalized_area
                .scaled_round(&small_density.area_scale(-6).unwrap())
                .unwrap(),
            64.0
        );

        let subnormal_integral = ExactDyadic::try_from_u128(false, 3, -1076).unwrap();
        let subnormal_derivatives = [ExactDyadic::try_from_u128(false, 3, -1075).unwrap()];
        let subnormal =
            ExactFactorialArea::from_derivatives(&subnormal_derivatives, &unit_moment).unwrap();
        let rounded = subnormal.scaled_round(&ExactDyadic::one()).unwrap();
        assert_eq!(rounded.to_bits(), 1);
        let quantization = ExactDyadic::from_f64(rounded)
            .try_subtract(subnormal_integral)
            .unwrap()
            .abs();
        let norm =
            ExactFactorialArea::absolute_contraction(&subnormal_derivatives, &unit_moment).unwrap();
        assert!(
            !norm
                .compare_dyadic_gamma(&quantization, 18)
                .unwrap()
                .is_le()
        );
    }
}
