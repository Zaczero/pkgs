//! Geographic polygon area from original-coordinate chart moments.

use crate::error::Result;
use crate::geographic::{
    ChartPole, ChartStep, GeodeticPole, GeographicFrame, GeographicRing, RingSummary,
};
use crate::geometry::exact_arith::{two_product, two_sum};
use crate::geometry::tessellation::exact::{ExactDyadic, outward_ratio_bounds, round_ratio};
use crate::geometry::{
    ChartMetric, Coordinates as _, ExactFactorialArea, GeometryErrorKind, Polygon, Ring,
    canonicalize_zero,
};
use crate::scratch_pool::{RetainScratch, ScratchPool, flat_vec_retained_bytes};

mod bounds;
mod density;
mod exact;
mod fast;

use self::bounds::{
    Bounded, BoundedSum, PositiveBound, accept_work_budget, area_scale_bound, ceil_log2,
    exact_difference_product, operation_count, positive_exact_bound, positive_sum, scale_area_fast,
    scaled_x, scaled_y, upper_add, upper_div_u64, upper_mul,
};
use self::density::{
    AreaCoefficient, INITIAL_TAYLOR_ORDER, LATITUDE_SCALE, MIN_SUBNORMAL, TaylorTail,
};
pub(crate) use self::density::{GeographicAreaDensity, MAX_AUTHALIC_HARMONICS};
use self::exact::accumulate_exact;
use self::fast::accumulate_fast;

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

#[cfg(test)]
mod tests;
