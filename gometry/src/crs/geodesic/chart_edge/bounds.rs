use std::cmp::Ordering;

use geographiclib_rs::Geodesic;

use super::ChartEdge;
use crate::geographic::GeographicFrame;
use crate::geometry::interval::{
    HALF_PI_LOWER, HALF_PI_UPPER, Interval as CertifiedInterval, PI_LOWER, PI_UPPER,
};
use crate::geometry::numerics::product_sum::ExactSum;
use crate::geometry::{GeodesicNumericalPolicy, Point};

#[derive(Clone, Copy)]
struct EllipsoidMetricBounds {
    meridian_min: f64,
    meridian_max: f64,
    prime_vertical_min: f64,
    prime_vertical_max: f64,
    meridian_derivative_max: f64,
}

fn ellipsoid_metric_bounds(geodesic: &Geodesic) -> Option<EllipsoidMetricBounds> {
    let one = CertifiedInterval::exact(1.0);
    let flattening = CertifiedInterval::exact(geodesic.f);
    let eccentricity_squared = flattening.mul(CertifiedInterval::exact(2.0).sub(flattening));
    let one_minus_eccentricity_squared = one.sub(eccentricity_squared);
    let denominator_min = one_minus_eccentricity_squared.lower().min(1.0);
    let denominator_max = one_minus_eccentricity_squared.upper().max(1.0);
    if !(denominator_min.is_finite() && denominator_min > 0.0) {
        return None;
    }
    let semi_major = CertifiedInterval::exact(geodesic.a);
    let min_denominator = CertifiedInterval::exact(denominator_min);
    let max_denominator = CertifiedInterval::exact(denominator_max);
    let min_root = min_denominator.sqrt();
    let max_root = max_denominator.sqrt();
    let prime_vertical_min = semi_major.div(max_root).lower();
    let prime_vertical_max = semi_major.div(min_root).upper();
    let meridian_min = semi_major
        .mul(one_minus_eccentricity_squared)
        .div(max_denominator.mul(max_root))
        .lower();
    let meridian_max = semi_major
        .mul(one_minus_eccentricity_squared)
        .div(min_denominator.mul(min_root))
        .upper();
    let minimum_denominator_fifth_half = min_denominator.mul(min_denominator).mul(min_root);
    let meridian_derivative_max = CertifiedInterval::exact(3.0)
        .mul(semi_major)
        .mul(CertifiedInterval::exact(
            one_minus_eccentricity_squared.upper(),
        ))
        .mul(CertifiedInterval::exact(
            eccentricity_squared.max_abs_upper(),
        ))
        .div(CertifiedInterval::exact(2.0).mul(minimum_denominator_fifth_half))
        .upper();
    (meridian_min.is_finite()
        && meridian_min > 0.0
        && meridian_max.is_finite()
        && prime_vertical_min.is_finite()
        && prime_vertical_min > 0.0
        && prime_vertical_max.is_finite()
        && meridian_derivative_max.is_finite())
    .then_some(EllipsoidMetricBounds {
        meridian_min,
        meridian_max,
        prime_vertical_min,
        prime_vertical_max,
        meridian_derivative_max,
    })
}

fn exact_sum_interval(sum: &ExactSum) -> Option<CertifiedInterval> {
    let rounded = sum.round_to_binary64();
    if rounded == f64::INFINITY {
        return Some(CertifiedInterval::from_bounds(f64::MAX, f64::INFINITY));
    }
    if rounded == f64::NEG_INFINITY {
        return Some(CertifiedInterval::from_bounds(f64::NEG_INFINITY, -f64::MAX));
    }
    if rounded.is_nan() {
        return None;
    }
    let mut remainder = sum.clone();
    remainder.subtract_product(rounded, 1.0);
    Some(match remainder.ordering() {
        Ordering::Equal => CertifiedInterval::exact(rounded),
        Ordering::Greater => CertifiedInterval::from_bounds(rounded, rounded.next_up()),
        Ordering::Less => CertifiedInterval::from_bounds(rounded.next_down(), rounded),
    })
}

fn affine_latitude(edge: &ChartEdge, parameter: f64) -> Option<CertifiedInterval> {
    let mut value = ExactSum::zero();
    value.add_product(edge.start_latitude, 1.0);
    value.add_product(edge.end_latitude, parameter);
    value.subtract_product(edge.start_latitude, parameter);
    exact_sum_interval(&value)
}

fn chart_parallel_radius_upper(
    edge: &ChartEdge,
    parameter_interval: [f64; 2],
    prime_vertical_max: f64,
) -> Option<f64> {
    let latitude_scale = CertifiedInterval::exact(
        edge.frame.latitude_radians_per_unit() * edge.frame.latitude_orientation_sign(),
    );
    let left = affine_latitude(edge, parameter_interval[0])?.mul(latitude_scale);
    let right = affine_latitude(edge, parameter_interval[1])?.mul(latitude_scale);
    let latitude_range = CertifiedInterval::from_bounds(
        left.lower().min(right.lower()),
        left.upper().max(right.upper()),
    );
    let latitude_magnitude_lower = if latitude_range.contains_zero() {
        0.0
    } else {
        latitude_range
            .lower()
            .abs()
            .min(latitude_range.upper().abs())
            .next_down()
            .max(0.0)
    };
    // Across this affine latitude range, the largest parallel radius is at its
    // smallest absolute latitude; cos(phi) is bounded by its colatitude.
    let colatitude_upper = CertifiedInterval::from_bounds(HALF_PI_LOWER, HALF_PI_UPPER)
        .sub(CertifiedInterval::exact(latitude_magnitude_lower))
        .upper()
        .max(0.0);
    let cosine_upper = 1.0_f64.min(colatitude_upper).next_up();
    let radius_upper = CertifiedInterval::exact(prime_vertical_max)
        .mul(CertifiedInterval::exact(cosine_upper))
        .upper();
    radius_upper.is_finite().then_some(radius_upper)
}

fn relative_latitude(
    source: &ChartEdge,
    target: &ChartEdge,
    source_parameter: f64,
    target_parameter: f64,
) -> Option<CertifiedInterval> {
    let mut difference = ExactSum::zero();
    difference.add_product(target.start_latitude, 1.0);
    difference.add_product(target.end_latitude, target_parameter);
    difference.subtract_product(target.start_latitude, target_parameter);
    difference.subtract_product(source.start_latitude, 1.0);
    difference.subtract_product(source.end_latitude, source_parameter);
    difference.add_product(source.start_latitude, source_parameter);
    exact_sum_interval(&difference)
}

fn longitude_sheet_shift(source: &ChartEdge, target: &ChartEdge) -> i64 {
    let axis = source.frame.longitude();
    let source_start = source.longitude.endpoints()[0].get();
    let target_start = target.longitude.endpoints()[0].get();
    let mut difference = ExactSum::zero();
    difference.add_product(target_start, 1.0);
    difference.subtract_product(source_start, 1.0);

    let mut above_positive_half = difference.clone();
    above_positive_half.subtract_product(axis.half_period(), 1.0);
    if above_positive_half.ordering() == Ordering::Greater {
        return -1;
    }
    let mut below_negative_half = difference;
    below_negative_half.add_product(axis.half_period(), 1.0);
    if below_negative_half.ordering() == Ordering::Less {
        return 1;
    }
    0
}

fn relative_longitude(
    source: &ChartEdge,
    target: &ChartEdge,
    source_parameter: f64,
    target_parameter: f64,
    target_sheet_shift: i64,
) -> Option<CertifiedInterval> {
    let period = source.frame.longitude().period();
    let [source_start, source_end] = *source.longitude.endpoints();
    let [target_start, target_end] = *target.longitude.endpoints();
    let mut difference = ExactSum::zero();
    difference.add_product(target_start.get(), 1.0);
    difference.subtract_product(source_start.get(), 1.0);
    difference.add_scaled_product(period, 1.0, target_sheet_shift, false);
    difference.add_product(target_end.get(), target_parameter);
    difference.subtract_product(target_start.get(), target_parameter);
    difference.add_scaled_product(period, target_parameter, target.longitude.periods(), false);
    difference.subtract_product(source_end.get(), source_parameter);
    difference.add_product(source_start.get(), source_parameter);
    difference.add_scaled_product(period, source_parameter, source.longitude.periods(), true);
    exact_sum_interval(&difference)
}

fn add_latitude_rate(sum: &mut ExactSum, edge: &ChartEdge, start: f64, end: f64, subtract: bool) {
    add_or_subtract_product(sum, edge.end_latitude, end, subtract);
    add_or_subtract_product(sum, edge.end_latitude, start, !subtract);
    add_or_subtract_product(sum, edge.start_latitude, end, !subtract);
    add_or_subtract_product(sum, edge.start_latitude, start, subtract);
}

fn add_longitude_rate(
    sum: &mut ExactSum,
    edge: &ChartEdge,
    period: f64,
    start: f64,
    end: f64,
    subtract: bool,
) {
    let [native_start, native_end] = *edge.longitude.endpoints();
    add_or_subtract_product(sum, native_end.get(), end, subtract);
    add_or_subtract_product(sum, native_end.get(), start, !subtract);
    add_or_subtract_product(sum, native_start.get(), end, !subtract);
    add_or_subtract_product(sum, native_start.get(), start, subtract);
    sum.add_scaled_product(period, end, edge.longitude.periods(), subtract);
    sum.add_scaled_product(period, start, edge.longitude.periods(), !subtract);
}

fn add_or_subtract_product(sum: &mut ExactSum, left: f64, right: f64, subtract: bool) {
    if subtract {
        sum.subtract_product(left, right);
    } else {
        sum.add_product(left, right);
    }
}

fn chart_position_rounding_error(
    edge: &ChartEdge,
    parameter: f64,
    point: Point,
    bounds: EllipsoidMetricBounds,
) -> Option<f64> {
    let mut longitude_residual = ExactSum::zero();
    let [start, end] = *edge.longitude.endpoints();
    longitude_residual.add_product(start.get(), 1.0);
    longitude_residual.add_product(end.get(), parameter);
    longitude_residual.subtract_product(start.get(), parameter);
    longitude_residual.add_scaled_product(
        edge.frame.longitude().period(),
        parameter,
        edge.longitude.periods(),
        false,
    );
    longitude_residual.subtract_product(point.x, 1.0);
    let longitude_error = exact_sum_interval(&longitude_residual)?.max_abs_upper();

    let mut latitude_residual = ExactSum::zero();
    latitude_residual.add_product(edge.start_latitude, 1.0);
    latitude_residual.add_product(edge.end_latitude, parameter);
    latitude_residual.subtract_product(edge.start_latitude, parameter);
    latitude_residual.subtract_product(point.y, 1.0);
    let latitude_error = exact_sum_interval(&latitude_residual)?.max_abs_upper();

    let longitude_error = CertifiedInterval::exact(longitude_error)
        .mul(CertifiedInterval::exact(
            edge.frame.longitude().radians_per_unit(),
        ))
        .upper();
    let latitude_error = CertifiedInterval::exact(latitude_error)
        .mul(CertifiedInterval::exact(
            edge.frame.latitude_radians_per_unit(),
        ))
        .upper();
    let position_error = CertifiedInterval::exact(bounds.prime_vertical_max)
        .mul(CertifiedInterval::exact(longitude_error))
        .add(
            CertifiedInterval::exact(bounds.meridian_max)
                .mul(CertifiedInterval::exact(latitude_error)),
        )
        .upper();
    position_error.is_finite().then_some(position_error)
}

pub(super) fn upper_product(left: f64, right: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        0.0
    } else {
        CertifiedInterval::exact(left)
            .mul(CertifiedInterval::exact(right))
            .upper()
    }
}

pub(super) fn lower_product(left: f64, right: f64) -> f64 {
    if left == 0.0 || right == 0.0 {
        0.0
    } else {
        CertifiedInterval::exact(left)
            .mul(CertifiedInterval::exact(right))
            .lower()
    }
}

pub(super) fn upper_ratio(numerator: f64, denominator: f64) -> f64 {
    if numerator == 0.0 {
        0.0
    } else {
        CertifiedInterval::exact(numerator)
            .div(CertifiedInterval::exact(denominator))
            .upper()
    }
}

pub(super) fn upper_sum(left: f64, right: f64) -> f64 {
    if left == 0.0 && right == 0.0 {
        0.0
    } else {
        CertifiedInterval::exact(left)
            .add(CertifiedInterval::exact(right))
            .upper()
    }
}

pub(super) fn upper_hypot(left: f64, right: f64) -> f64 {
    if left == 0.0 && right == 0.0 {
        return 0.0;
    }
    let left = CertifiedInterval::exact(left).mul(CertifiedInterval::exact(left));
    let right = CertifiedInterval::exact(right).mul(CertifiedInterval::exact(right));
    left.add(right).sqrt().upper()
}

impl ChartEdge {
    #[expect(
        clippy::float_cmp,
        reason = "an exact pole witness has a monotone distance along affine latitude"
    )]
    pub(in crate::crs::geodesic) fn pole_distance_upper_bound(
        self,
        geodesic: &Geodesic,
        frame: GeographicFrame,
        source_interval: [f64; 2],
        target: Point,
    ) -> Option<f64> {
        let latitude = frame
            .to_geodesic_latitude(target.y)
            .expect("the target witness was admitted before chart distance");
        if latitude.abs() != 90.0 {
            return None;
        }
        let target = frame
            .to_geodesic_point(target)
            .expect("the target witness was admitted before chart distance");
        let source_start = self.geodetic_position(source_interval[0]).ok()?;
        let source_end = self.geodetic_position(source_interval[1]).ok()?;
        // Distance to the fixed pole is monotone in signed latitude, so its
        // maximum over an affine interval occurs at an endpoint.
        let start_distance = super::ellipsoid_metric::inverse_distance(
            geodesic,
            target.x,
            target.y,
            source_start.x,
            source_start.y,
        );
        let end_distance = super::ellipsoid_metric::inverse_distance(
            geodesic,
            target.x,
            target.y,
            source_end.x,
            source_end.y,
        );
        let distance = start_distance.max(end_distance);
        distance.is_finite().then_some(distance)
    }

    pub(crate) fn moving_correspondence_upper(
        &self,
        target: &Self,
        source_interval: [f64; 2],
        target_interval: [f64; 2],
        source_points: &[Point; 2],
        target_points: &[Point; 2],
        endpoint_distances: [f64; 2],
        geodesic: &Geodesic,
    ) -> Option<f64> {
        if self.frame != target.frame {
            return None;
        }
        let [source_left, source_right] = source_interval;
        let [target_left, target_right] = target_interval;
        if !(source_left.is_finite()
            && source_right.is_finite()
            && target_left.is_finite()
            && target_right.is_finite()
            && source_left >= 0.0
            && source_left < source_right
            && source_right <= 1.0
            && target_left.is_finite()
            && target_right.is_finite()
            && (0.0..=1.0).contains(&target_left)
            && (0.0..=1.0).contains(&target_right)
            && endpoint_distances
                .iter()
                .all(|distance| distance.is_finite()))
        {
            return None;
        }

        let bounds = ellipsoid_metric_bounds(geodesic)?;
        let longitude_axis = self.frame.longitude();
        let longitude_sheet = longitude_sheet_shift(self, target);

        let latitude_left = relative_latitude(self, target, source_left, target_left)?;
        let latitude_right = relative_latitude(self, target, source_right, target_right)?;
        let longitude_left =
            relative_longitude(self, target, source_left, target_left, longitude_sheet)?;
        let longitude_right =
            relative_longitude(self, target, source_right, target_right, longitude_sheet)?;
        let latitude_scale = CertifiedInterval::exact(
            self.frame.latitude_radians_per_unit() * self.frame.latitude_orientation_sign(),
        );
        let longitude_scale = CertifiedInterval::exact(
            longitude_axis.radians_per_unit() * self.frame.longitude_orientation_sign(),
        );
        let latitude_difference = latitude_left
            .mul(latitude_scale)
            .max_abs_upper()
            .max(latitude_right.mul(latitude_scale).max_abs_upper());
        let longitude_difference = longitude_left
            .mul(longitude_scale)
            .max_abs_upper()
            .max(longitude_right.mul(longitude_scale).max_abs_upper());
        let source_parallel_radius_upper =
            chart_parallel_radius_upper(self, source_interval, bounds.prime_vertical_max)
                .unwrap_or(bounds.prime_vertical_max);
        let target_parallel_radius_upper =
            chart_parallel_radius_upper(target, target_interval, bounds.prime_vertical_max)
                .unwrap_or(bounds.prime_vertical_max);
        let path_parallel_radius_upper =
            source_parallel_radius_upper.min(target_parallel_radius_upper);

        let distance_path = CertifiedInterval::exact(bounds.meridian_max)
            .mul(CertifiedInterval::exact(latitude_difference))
            .add(
                CertifiedInterval::exact(path_parallel_radius_upper)
                    .mul(CertifiedInterval::exact(longitude_difference)),
            );
        if !distance_path.upper().is_finite() {
            return None;
        }

        let source_left_latitude = affine_latitude(self, source_left)?
            .mul(latitude_scale)
            .max_abs_upper();
        let source_right_latitude = affine_latitude(self, source_right)?
            .mul(latitude_scale)
            .max_abs_upper();
        let target_left_latitude = affine_latitude(target, target_left)?
            .mul(latitude_scale)
            .max_abs_upper();
        let target_right_latitude = affine_latitude(target, target_right)?
            .mul(latitude_scale)
            .max_abs_upper();
        let endpoint_latitude = source_left_latitude
            .max(source_right_latitude)
            .max(target_left_latitude)
            .max(target_right_latitude);
        let latitude_strip = CertifiedInterval::exact(endpoint_latitude)
            .add(distance_path.div(CertifiedInterval::exact(bounds.meridian_min)));
        let half_pi = CertifiedInterval::from_bounds(HALF_PI_LOWER, HALF_PI_UPPER);
        let colatitude_lower = half_pi.sub(latitude_strip).lower().max(0.0);
        if colatitude_lower == 0.0 {
            return None;
        }
        let sine_lower = CertifiedInterval::exact(2.0)
            .mul(CertifiedInterval::exact(colatitude_lower))
            .div(CertifiedInterval::from_bounds(PI_LOWER, PI_UPPER))
            .lower()
            .max(0.0);
        let radius_lower = CertifiedInterval::exact(bounds.prime_vertical_min)
            .mul(CertifiedInterval::exact(sine_lower))
            .lower();
        if !(radius_lower.is_finite() && radius_lower > 0.0) {
            return None;
        }
        let deformation = CertifiedInterval::exact(latitude_strip.upper().min(1.0))
            .div(CertifiedInterval::exact(radius_lower))
            .upper();

        let mut source_latitude_rate = ExactSum::zero();
        add_latitude_rate(
            &mut source_latitude_rate,
            self,
            source_left,
            source_right,
            false,
        );
        let source_latitude_rate = exact_sum_interval(&source_latitude_rate)?;

        let mut target_latitude_rate = ExactSum::zero();
        add_latitude_rate(
            &mut target_latitude_rate,
            target,
            target_left,
            target_right,
            false,
        );
        let target_latitude_rate = exact_sum_interval(&target_latitude_rate)?;

        let mut relative_latitude_rate = ExactSum::zero();
        add_latitude_rate(
            &mut relative_latitude_rate,
            target,
            target_left,
            target_right,
            false,
        );
        add_latitude_rate(
            &mut relative_latitude_rate,
            self,
            source_left,
            source_right,
            true,
        );
        let relative_latitude_rate = exact_sum_interval(&relative_latitude_rate)?
            .mul(latitude_scale)
            .max_abs_upper();

        let source_latitude_rate = source_latitude_rate.mul(latitude_scale).max_abs_upper();
        let target_latitude_rate = target_latitude_rate.mul(latitude_scale).max_abs_upper();

        let mut relative_longitude_rate = ExactSum::zero();
        add_longitude_rate(
            &mut relative_longitude_rate,
            target,
            longitude_axis.period(),
            target_left,
            target_right,
            false,
        );
        add_longitude_rate(
            &mut relative_longitude_rate,
            self,
            longitude_axis.period(),
            source_left,
            source_right,
            true,
        );
        let relative_longitude_rate = exact_sum_interval(&relative_longitude_rate)?
            .mul(longitude_scale)
            .max_abs_upper();

        let source_longitude_motion = CertifiedInterval::exact(target_parallel_radius_upper)
            .mul(CertifiedInterval::exact(relative_longitude_rate))
            .upper();
        let target_longitude_motion = CertifiedInterval::exact(source_parallel_radius_upper)
            .mul(CertifiedInterval::exact(relative_longitude_rate))
            .upper();
        let common_metric_deformation = CertifiedInterval::exact(bounds.meridian_derivative_max)
            .mul(CertifiedInterval::exact(latitude_difference));
        let relative_meridional = CertifiedInterval::exact(bounds.meridian_max)
            .mul(CertifiedInterval::exact(relative_latitude_rate));
        let source_metric_deformation =
            common_metric_deformation.mul(CertifiedInterval::exact(source_latitude_rate));
        let target_metric_deformation =
            common_metric_deformation.mul(CertifiedInterval::exact(target_latitude_rate));
        let source_meridional = relative_meridional.add(source_metric_deformation);
        let target_meridional = relative_meridional.add(target_metric_deformation);
        let source_common_motion = CertifiedInterval::exact(bounds.meridian_max)
            .mul(CertifiedInterval::exact(source_latitude_rate))
            .mul(CertifiedInterval::exact(deformation))
            .mul(distance_path);
        let target_common_motion = CertifiedInterval::exact(bounds.meridian_max)
            .mul(CertifiedInterval::exact(target_latitude_rate))
            .mul(CertifiedInterval::exact(deformation))
            .mul(distance_path);
        let source_variation = CertifiedInterval::exact(source_longitude_motion)
            .mul(CertifiedInterval::exact(source_longitude_motion))
            .add(source_meridional.mul(source_meridional))
            .sqrt()
            .add(source_common_motion)
            .upper();
        let target_variation = CertifiedInterval::exact(target_longitude_motion)
            .mul(CertifiedInterval::exact(target_longitude_motion))
            .add(target_meridional.mul(target_meridional))
            .sqrt()
            .add(target_common_motion)
            .upper();
        let variation = source_variation.min(target_variation);
        if !variation.is_finite() {
            return None;
        }

        let allowance = GeodesicNumericalPolicy::new(geodesic.a)
            .evaluation_allowance(4.0)
            .next_up();
        let source_left_error =
            chart_position_rounding_error(self, source_left, source_points[0], bounds)?;
        let source_right_error =
            chart_position_rounding_error(self, source_right, source_points[1], bounds)?;
        let target_left_error =
            chart_position_rounding_error(target, target_left, target_points[0], bounds)?;
        let target_right_error =
            chart_position_rounding_error(target, target_right, target_points[1], bounds)?;
        let left_distance = CertifiedInterval::exact(endpoint_distances[0])
            .add(CertifiedInterval::exact(allowance))
            .add(CertifiedInterval::exact(source_left_error))
            .add(CertifiedInterval::exact(target_left_error))
            .upper();
        let right_distance = CertifiedInterval::exact(endpoint_distances[1])
            .add(CertifiedInterval::exact(allowance))
            .add(CertifiedInterval::exact(source_right_error))
            .add(CertifiedInterval::exact(target_right_error))
            .upper();
        // The two endpoint Lipschitz cones peak at their crossing or an interval endpoint.
        let cone_crossing = CertifiedInterval::exact(left_distance)
            .add(CertifiedInterval::exact(right_distance))
            .add(CertifiedInterval::exact(variation))
            .div(CertifiedInterval::exact(2.0))
            .upper();
        let upper = left_distance.max(right_distance).max(cone_crossing);
        upper.is_finite().then_some(upper)
    }

    pub(super) fn east_speed_upper(self, n_max: f64, longitude_rate: f64) -> f64 {
        let parallel_radius =
            chart_parallel_radius_upper(&self, [0.0, 1.0], n_max).unwrap_or(n_max);
        upper_product(parallel_radius, longitude_rate)
    }
}
