use std::num::NonZeroUsize;

use super::frame::{
    LocalCrsDefinition, is_east_north_metre_axes, is_projected_metre_cartesian_axes,
    local_projection_parameter,
};
use crate::crs::in_core::{ParamUnit, polar_certificate_terms, tmerc_certificate_terms};
use crate::crs::{ChartEdge, CrsError, RuntimeState, info_with_state};
use crate::error::Result;
use crate::geographic::{ChartStep, EdgeDirection, GeodeticPole, GeographicFrame, Latitude};
use crate::geometry::{CoordSeq, Point, line_segments};

// These simple decimal upper bounds follow by squaring against the exact
// constants: 1.09² > 32/27 and 0.385² > 4/27.
const GAUSSIAN_TANGENTIAL_CURVATURE: f64 = 1.09;
const GAUSSIAN_LATITUDE_ACCELERATION: f64 = 0.385;
const FAST_TMER_BETA: f64 = 0.125;

#[derive(Clone, Copy, Debug)]
enum ProjectionCertificate {
    Tmerc(TmercCertificate),
    Ups(UpsCertificate),
}

#[derive(Clone, Copy, Debug)]
struct EllipsoidRadii {
    a: f64,
    e2_lower: f64,
    e2_upper: f64,
    one_es_lower: f64,
    one_es_upper: f64,
    n_max: f64,
    m_max: f64,
    min_radius: f64,
}

#[derive(Clone, Copy, Debug)]
struct TmercCertificate {
    radius_lower: f64,
    radius_upper: f64,
    p_min: f64,
    p_max: f64,
    p2_max: f64,
    gtu: [f64; 6],
    c1_fast: f64,
    c2_fast: f64,
}

#[derive(Clone, Copy, Debug)]
struct UpsCertificate {
    radius_lower: f64,
    radius_upper: f64,
    pole: GeodeticPole,
    p_min: f64,
    p_max: f64,
    p2_max: f64,
}

#[derive(Clone, Copy)]
struct CertificateEdge {
    longitude_delta_native: f64,
    longitude_from_central_meridian: f64,
    longitude_delta: f64,
    latitude0: f64,
    latitude1: f64,
    is_selected_pole_parallel: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct LocalProjectionCertificate {
    frame: GeographicFrame,
    central_meridian: f64,
    radii: EllipsoidRadii,
    projection: ProjectionCertificate,
}

impl LocalProjectionCertificate {
    /// `local` comes from `estimate_local_crs_at`, whose return path validates
    /// the resolved datum-preserving operation before a certificate can exist.
    pub(super) fn from_selected_frame(
        local: &LocalCrsDefinition,
        geodesic: &geographiclib_rs::Geodesic,
        frame: GeographicFrame,
        state: &RuntimeState,
    ) -> Result<Self> {
        let info = info_with_state(&local.definition, state)?;
        if !is_east_north_metre_axes(&info) && !is_projected_metre_cartesian_axes(&info) {
            return Err(CrsError::invalid(
                "local projection certificate requires metre Cartesian axes",
            ));
        }
        let operation = info
            .coordinate_operation
            .as_ref()
            .ok_or_else(|| CrsError::invalid("local projection has no coordinate operation"))?;
        let method_info = operation
            .method
            .as_ref()
            .ok_or_else(|| CrsError::invalid("local projection has no EPSG method code"))?;
        if method_info.authority.as_deref() != Some("EPSG") {
            return Err(CrsError::invalid(
                "local projection certificate requires an EPSG projection method",
            ));
        }
        let method = method_info
            .code
            .as_deref()
            .ok_or_else(|| CrsError::invalid("local projection has no EPSG method code"))?;
        let latitude_of_origin = local_projection_parameter(operation, "8801", ParamUnit::Degrees)
            .ok_or_else(|| CrsError::invalid("local projection has no latitude of origin"))?;
        let longitude_of_origin = local_projection_parameter(operation, "8802", ParamUnit::Degrees)
            .ok_or_else(|| CrsError::invalid("local projection has no longitude of origin"))?;
        let k0 = local_projection_parameter(operation, "8805", ParamUnit::Unity)
            .ok_or_else(|| CrsError::invalid("local projection has no central scale factor"))?;
        let central_meridian = frame
            .point_from_geodesic(Point::new_raw_xy(longitude_of_origin, 0.0))
            .map_err(|error| {
                CrsError::invalid(format!(
                    "local projection origin is outside the source chart: {error}"
                ))
            })?
            .x;
        let radii = certificate_math::ellipsoid_radii(geodesic.a, geodesic.f)?;
        let projection = match method {
            "9807" => {
                let terms = tmerc_certificate_terms(geodesic.a, geodesic.f, k0);
                let (
                    latitude_derivative_minimum,
                    latitude_derivative_maximum,
                    latitude_second_derivative_maximum,
                ) = certificate_math::tmerc_latitude_derivative_bounds(&terms.cbg);
                if !(latitude_derivative_minimum.is_finite()
                    && latitude_derivative_minimum > 0.0
                    && latitude_derivative_maximum.is_finite()
                    && latitude_second_derivative_maximum.is_finite())
                {
                    return Err(CrsError::invalid(
                        "local transverse Mercator latitude map cannot be certified",
                    ));
                }
                let (c1_fast, c2_fast) = certificate_math::correction_derivative_bounds(
                    &terms.gtu,
                    FAST_TMER_BETA,
                    FAST_TMER_BETA,
                );
                ProjectionCertificate::Tmerc(TmercCertificate {
                    radius_lower: certificate_math::multiply_down(geodesic.a, terms.qn),
                    radius_upper: certificate_math::multiply_up(geodesic.a, terms.qn),
                    p_min: latitude_derivative_minimum,
                    p_max: latitude_derivative_maximum,
                    p2_max: latitude_second_derivative_maximum,
                    gtu: terms.gtu,
                    c1_fast,
                    c2_fast,
                })
            },
            "9810" => {
                let terms = polar_certificate_terms(geodesic.a, geodesic.f, latitude_of_origin, k0)
                    .ok_or_else(|| {
                        CrsError::invalid("local UPS parameters do not match polar stereographic")
                    })?;
                let (
                    latitude_derivative_minimum,
                    latitude_derivative_maximum,
                    latitude_second_derivative_maximum,
                ) = certificate_math::ups_latitude_derivative_bounds(terms.e);
                if !(latitude_derivative_minimum.is_finite()
                    && latitude_derivative_minimum > 0.0
                    && latitude_derivative_maximum.is_finite()
                    && latitude_second_derivative_maximum.is_finite())
                {
                    return Err(CrsError::invalid(
                        "local polar stereographic latitude map cannot be certified",
                    ));
                }
                ProjectionCertificate::Ups(UpsCertificate {
                    radius_lower: certificate_math::multiply_down(terms.a, terms.akm1) / 2.0,
                    radius_upper: certificate_math::multiply_up(terms.a, terms.akm1) / 2.0,
                    pole: terms.pole,
                    p_min: latitude_derivative_minimum,
                    p_max: latitude_derivative_maximum,
                    p2_max: latitude_second_derivative_maximum,
                })
            },
            _ => {
                return Err(CrsError::invalid(
                    "local projection certificate supports only Transverse Mercator and UPS",
                ));
            },
        };
        Ok(Self {
            frame,
            central_meridian,
            radii,
            projection,
        })
    }

    fn chart_edge(&self, start: Point, end: Point) -> Result<CertificateEdge> {
        let axis = self.frame.longitude();
        let longitude_delta_native = match axis
            .chart_step(start.x, end.x)
            .expect("screen coordinates were admitted before local projection")
        {
            ChartStep::Shortest(EdgeDirection::Minor(delta)) => delta,
            step @ ChartStep::Circle(_) => step.delta(axis),
            ChartStep::Shortest(EdgeDirection::HalfTurn(direction)) => {
                // Preserve ChartEdge's public-facing exact-half-turn error.
                ChartEdge::new(start, end, self.frame)?;
                unreachable!("ChartEdge rejects an exact half-turn: {direction:?}")
            },
        };
        let latitude0_native = Latitude::from_admitted(self.frame, start.y).get();
        let latitude1_native = Latitude::from_admitted(self.frame, end.y).get();
        let latitude_unit =
            self.frame.latitude_radians_per_unit() * self.frame.latitude_orientation_sign();
        let longitude_from_central_meridian = axis
            .shortest_alias_step(self.central_meridian, start.x)
            .expect("screen coordinates were admitted before local projection")
            .delta(axis)
            * axis.radians_per_unit()
            * self.frame.longitude_orientation_sign();
        let selected_pole = match self.projection {
            ProjectionCertificate::Tmerc(_) => None,
            ProjectionCertificate::Ups(certificate) => {
                Some(self.frame.native_latitude_pole(certificate.pole))
            },
        };
        Ok(CertificateEdge {
            longitude_delta_native,
            longitude_from_central_meridian,
            longitude_delta: longitude_delta_native
                * axis.radians_per_unit()
                * self.frame.longitude_orientation_sign(),
            latitude0: latitude0_native * latitude_unit,
            latitude1: latitude1_native * latitude_unit,
            is_selected_pole_parallel: selected_pole.is_some_and(|pole| {
                start.y.to_bits() == pole.to_bits() && end.y.to_bits() == pole.to_bits()
            }),
        })
    }

    fn is_straight_chart_edge(&self, edge: CertificateEdge) -> bool {
        match self.projection {
            ProjectionCertificate::Tmerc(_) => {
                (edge.longitude_delta_native == 0.0 && edge.longitude_from_central_meridian == 0.0)
                    || (edge.latitude0 == 0.0 && edge.latitude1 == 0.0)
            },
            ProjectionCertificate::Ups(_) => {
                edge.longitude_delta_native == 0.0 || edge.is_selected_pole_parallel
            },
        }
    }

    fn cheap_bound(&self, edge: CertificateEdge) -> f64 {
        match self.projection {
            ProjectionCertificate::Tmerc(certificate) => self.cheap_tmerc_bound(
                &certificate,
                edge.longitude_from_central_meridian,
                edge.longitude_delta,
                edge.latitude0,
                edge.latitude1,
            ),
            ProjectionCertificate::Ups(certificate) => self.cheap_ups_bound(
                certificate,
                edge.longitude_delta,
                edge.latitude0,
                edge.latitude1,
            ),
        }
    }

    /// Arithmetic-only screen for one source-chart edge.
    ///
    /// It performs no projection, geodesic integration, or allocation. The
    /// caller can scan a geometry and return its original source sequence
    /// immediately when every edge is safe.
    pub(crate) fn edge_needs_refinement(
        &self,
        start: Point,
        end: Point,
        budget: f64,
    ) -> Result<bool> {
        let edge = self.chart_edge(start, end)?;
        if self.is_straight_chart_edge(edge) {
            return Ok(false);
        }
        Ok(self
            .cheap_bound(edge)
            .partial_cmp(&budget)
            .is_none_or(std::cmp::Ordering::is_gt))
    }

    /// Return whether any stored chart edge needs refinement, without
    /// allocating a plan or a replacement coordinate sequence.
    pub(crate) fn has_edges_to_refine(&self, points: &CoordSeq, budget: f64) -> Result<bool> {
        for segment in line_segments(points) {
            if self.edge_needs_refinement(
                Point::new_raw_xy(segment.start.x, segment.start.y),
                Point::new_raw_xy(segment.end.x, segment.end.y),
                budget,
            )? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return the certified piece count for one stored chart edge.
    pub(crate) fn piece_count(
        &self,
        start: Point,
        end: Point,
        budget: f64,
    ) -> Result<NonZeroUsize> {
        let edge = self.chart_edge(start, end)?;
        if self.is_straight_chart_edge(edge) || self.cheap_bound(edge) <= budget {
            return Ok(NonZeroUsize::MIN);
        }

        let precise_bound = match self.projection {
            ProjectionCertificate::Tmerc(certificate) => self.precise_tmerc_bound(
                &certificate,
                edge.longitude_from_central_meridian,
                edge.longitude_delta,
                edge.latitude0,
                edge.latitude1,
            ),
            ProjectionCertificate::Ups(certificate) => self.precise_ups_bound(
                certificate,
                edge.longitude_delta,
                edge.latitude0,
                edge.latitude1,
            ),
        };
        Ok(certificate_math::piece_count_from_bound(
            precise_bound,
            budget,
        ))
    }

    fn cheap_tmerc_bound(
        &self,
        certificate: &TmercCertificate,
        longitude_from_central_meridian: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        certificate_math::cheap_tmerc_bound(
            &self.radii,
            certificate,
            longitude_from_central_meridian,
            longitude_delta,
            latitude0,
            latitude1,
        )
    }

    fn cheap_ups_bound(
        &self,
        certificate: UpsCertificate,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        certificate_math::cheap_ups_bound(
            &self.radii,
            certificate,
            longitude_delta,
            latitude0,
            latitude1,
        )
    }

    fn precise_tmerc_bound(
        &self,
        certificate: &TmercCertificate,
        longitude_from_central_meridian: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        certificate_math::precise_tmerc_bound(
            &self.radii,
            certificate,
            longitude_from_central_meridian,
            longitude_delta,
            latitude0,
            latitude1,
        )
    }

    fn precise_ups_bound(
        &self,
        certificate: UpsCertificate,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        certificate_math::precise_ups_bound(
            &self.radii,
            certificate,
            longitude_delta,
            latitude0,
            latitude1,
        )
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    clippy::float_cmp,
    reason = "these operations compute the outward-rounded analytic local-projection certificate"
)]
mod certificate_math {
    use super::{
        CrsError, EllipsoidRadii, FAST_TMER_BETA, GAUSSIAN_LATITUDE_ACCELERATION,
        GAUSSIAN_TANGENTIAL_CURVATURE, GeodeticPole, NonZeroUsize, Result, TmercCertificate,
        UpsCertificate,
    };

    #[derive(Clone, Copy)]
    struct IntervalRadii {
        near_abs_lower: f64,
        n_max: f64,
        m_max: f64,
        min_radius: f64,
    }

    fn round_up(value: f64) -> f64 {
        if value == 0.0 || !value.is_finite() {
            value
        } else {
            value.next_up()
        }
    }

    fn round_down(value: f64) -> f64 {
        if value == 0.0 || !value.is_finite() {
            value
        } else {
            value.next_down()
        }
    }

    fn add_up(left: f64, right: f64) -> f64 {
        round_up(left + right)
    }

    fn add_down(left: f64, right: f64) -> f64 {
        round_down(left + right)
    }

    fn subtract_up(left: f64, right: f64) -> f64 {
        round_up(left - right)
    }

    fn subtract_down(left: f64, right: f64) -> f64 {
        round_down(left - right)
    }

    pub(super) fn multiply_up(left: f64, right: f64) -> f64 {
        round_up(left * right)
    }

    pub(super) fn multiply_down(left: f64, right: f64) -> f64 {
        round_down(left * right)
    }

    fn divide_up(numerator: f64, denominator: f64) -> f64 {
        round_up(numerator / denominator)
    }

    fn divide_down(numerator: f64, denominator: f64) -> f64 {
        round_down(numerator / denominator)
    }

    fn multiply_interval(
        left_lower: f64,
        left_upper: f64,
        right_lower: f64,
        right_upper: f64,
    ) -> (f64, f64) {
        let lower = multiply_down(left_lower, right_lower)
            .min(multiply_down(left_lower, right_upper))
            .min(multiply_down(left_upper, right_lower))
            .min(multiply_down(left_upper, right_upper));
        let upper = multiply_up(left_lower, right_lower)
            .max(multiply_up(left_lower, right_upper))
            .max(multiply_up(left_upper, right_lower))
            .max(multiply_up(left_upper, right_upper));
        (lower, upper)
    }

    pub(super) fn ellipsoid_radii(a: f64, f: f64) -> Result<EllipsoidRadii> {
        if !(a.is_finite() && a > 0.0 && f.is_finite() && f < 1.0) {
            return Err(CrsError::invalid(
                "local projection certificate requires a supported ellipsoid",
            ));
        }
        let flattening_complement_lower = subtract_down(2.0, f);
        let flattening_complement_upper = subtract_up(2.0, f);
        let (e2_lower, e2_upper) = if f >= 0.0 {
            (
                multiply_down(f, flattening_complement_lower),
                multiply_up(f, flattening_complement_upper),
            )
        } else {
            (
                multiply_down(f, flattening_complement_upper),
                multiply_up(f, flattening_complement_lower),
            )
        };
        let one_es_lower = subtract_down(1.0, e2_upper);
        let one_es_upper = subtract_up(1.0, e2_lower);
        if one_es_lower <= 0.0 {
            return Err(CrsError::invalid(
                "local projection certificate requires positive ellipsoid curvature radii",
            ));
        }

        let (e2_product_lower, e2_product_upper) = multiply_interval(e2_lower, e2_upper, 0.0, 1.0);
        let d_min = subtract_down(1.0, e2_product_upper);
        let d_max = subtract_up(1.0, e2_product_lower);
        if d_min <= 0.0 {
            return Err(CrsError::invalid(
                "local projection certificate requires positive ellipsoid curvature radii",
            ));
        }
        // For D > 0, sqrt(D) >= min(D, 1) and D^(3/2) >= min(D², 1).
        let n_denominator_lower = d_min.min(1.0);
        let m_denominator_lower = multiply_down(d_min, d_min).min(1.0);
        let n_max = divide_up(a, n_denominator_lower);
        let m_max = divide_up(multiply_up(a, one_es_upper), m_denominator_lower);
        let n_denominator_upper = d_max.max(1.0);
        let m_denominator_upper = multiply_up(d_max, d_max).max(1.0);
        let n_min = divide_down(a, n_denominator_upper);
        let m_min = divide_down(multiply_down(a, one_es_lower), m_denominator_upper);
        Ok(EllipsoidRadii {
            a,
            e2_lower,
            e2_upper,
            one_es_lower,
            one_es_upper,
            n_max,
            m_max,
            min_radius: n_min.min(m_min),
        })
    }

    pub(super) fn tmerc_latitude_derivative_bounds(coefficients: &[f64; 6]) -> (f64, f64, f64) {
        let mut first_derivative = 0.0;
        let mut second_derivative = 0.0;
        for (index, coefficient) in coefficients.iter().enumerate() {
            let harmonic = 2.0 * f64::from(u32::try_from(index + 1).expect("six TM coefficients"));
            let magnitude = coefficient.abs();
            first_derivative = add_up(first_derivative, multiply_up(harmonic, magnitude));
            second_derivative = add_up(
                second_derivative,
                multiply_up(multiply_up(harmonic, harmonic), magnitude),
            );
        }
        (
            subtract_down(1.0, first_derivative),
            add_up(1.0, first_derivative),
            second_derivative,
        )
    }

    pub(super) fn ups_latitude_derivative_bounds(e: f64) -> (f64, f64, f64) {
        let e_upper = round_up(e);
        let eccentricity_squared_upper = multiply_up(e_upper, e_upper);
        let one_es_lower = subtract_down(1.0, eccentricity_squared_upper);
        let h = divide_up(eccentricity_squared_upper, one_es_lower);
        let one_minus_h = subtract_down(1.0, h);
        let one_minus_two_h = subtract_down(1.0, multiply_up(2.0, h));
        if one_es_lower <= 0.0 || one_minus_h <= 0.0 || one_minus_two_h <= 0.0 {
            return (0.0, f64::INFINITY, f64::INFINITY);
        }
        // atanh(e) <= e/(1-e²), and exp(x) <= 1/(1-x) for 0 <= x < 1.
        let exp_h_upper = divide_up(1.0, one_minus_h);
        let exp_two_h_upper = divide_up(1.0, one_minus_two_h);
        let first = multiply_up(h, exp_two_h_upper);
        let second = divide_up(multiply_up(3.0, eccentricity_squared_upper), one_es_lower);
        let p_max = exp_h_upper;
        let derivative_minimum = divide_down(one_es_lower, exp_h_upper);
        let second_derivative_maximum = multiply_up(p_max, add_up(first, second));
        (derivative_minimum, p_max, second_derivative_maximum)
    }

    pub(super) fn correction_derivative_bounds(
        coefficients: &[f64; 6],
        beta_lower: f64,
        beta_upper: f64,
    ) -> (f64, f64) {
        if !(0.0..1.0).contains(&beta_upper) {
            return (f64::INFINITY, f64::INFINITY);
        }
        let beta_lower = beta_lower.clamp(0.0, beta_upper);
        let beta_squared_lower = multiply_down(beta_lower, beta_lower).max(0.0);
        let beta_squared_upper = multiply_up(beta_upper, beta_upper);
        let cosh_numerator_lower = add_down(1.0, beta_squared_lower);
        let cosh_numerator_upper = add_up(1.0, beta_squared_upper);
        let cosh_denominator_lower = subtract_down(1.0, beta_squared_upper);
        let cosh_denominator_upper = subtract_up(1.0, beta_squared_lower);
        if cosh_denominator_lower <= 0.0 {
            return (f64::INFINITY, f64::INFINITY);
        }
        let cosh_two_eta_lower = divide_down(cosh_numerator_lower, cosh_denominator_upper).max(1.0);
        let cosh_two_eta_upper = divide_up(cosh_numerator_upper, cosh_denominator_lower);
        let mut previous_lower = 1.0;
        let mut previous_upper = 1.0;
        let mut current_lower = cosh_two_eta_lower;
        let mut current_upper = cosh_two_eta_upper;
        let mut c1_upper = 0.0;
        let mut c2_upper = 0.0;

        for (index, coefficient) in coefficients.iter().enumerate() {
            let harmonic = 2.0 * f64::from(u32::try_from(index + 1).expect("six TM coefficients"));
            let magnitude = coefficient.abs();
            let weighted = multiply_up(multiply_up(harmonic, magnitude), current_upper);
            c1_upper = add_up(c1_upper, weighted);
            c2_upper = add_up(c2_upper, multiply_up(harmonic, weighted));

            if index + 1 < coefficients.len() {
                let next_upper = subtract_up(
                    multiply_up(multiply_up(2.0, cosh_two_eta_upper), current_upper),
                    previous_lower,
                );
                let next_lower = subtract_down(
                    multiply_down(multiply_down(2.0, cosh_two_eta_lower), current_lower),
                    previous_upper,
                )
                .max(1.0);
                previous_lower = current_lower;
                previous_upper = current_upper;
                current_lower = next_lower;
                current_upper = next_upper;
            }
        }
        (c1_upper, c2_upper)
    }

    fn latitude_absolute_range(latitude0: f64, latitude1: f64) -> (f64, f64) {
        let lower = round_down(latitude0.min(latitude1));
        let upper = round_up(latitude0.max(latitude1));
        let nearest = if lower <= 0.0 && upper >= 0.0 {
            0.0
        } else {
            round_down(if lower > 0.0 { lower } else { upper.abs() }).max(0.0)
        };
        let farthest =
            round_up(lower.abs().max(upper.abs())).min(round_up(std::f64::consts::FRAC_PI_2));
        (nearest, farthest)
    }

    fn ellipsoid_interval_radii(
        radii: &EllipsoidRadii,
        latitude0: f64,
        latitude1: f64,
    ) -> IntervalRadii {
        let (near_abs_lower, far_abs_upper) = latitude_absolute_range(latitude0, latitude1);
        let sin_near_lower = sine_lower(near_abs_lower);
        let sin_far_upper = sine_upper(far_abs_upper);
        let squared_sine_near_lower = multiply_down(sin_near_lower, sin_near_lower).max(0.0);
        let squared_sine_far_upper = multiply_up(sin_far_upper, sin_far_upper).min(1.0);
        let (e2_product_lower, e2_product_upper) = multiply_interval(
            radii.e2_lower,
            radii.e2_upper,
            squared_sine_near_lower,
            squared_sine_far_upper,
        );
        let d_min = subtract_down(1.0, e2_product_upper);
        let d_max = subtract_up(1.0, e2_product_lower);
        if d_min <= 0.0 || d_max <= 0.0 {
            return IntervalRadii {
                near_abs_lower,
                n_max: f64::INFINITY,
                m_max: f64::INFINITY,
                min_radius: 0.0,
            };
        }
        let n_denominator_lower = d_min.min(1.0);
        let m_denominator_lower = multiply_down(d_min, d_min).min(1.0);
        let n_max = divide_up(radii.a, n_denominator_lower);
        let m_max = divide_up(
            multiply_up(radii.a, radii.one_es_upper),
            m_denominator_lower,
        );
        let n_denominator_upper = d_max.max(1.0);
        let m_denominator_upper = multiply_up(d_max, d_max).max(1.0);
        let n_min = divide_down(radii.a, n_denominator_upper);
        let m_min = divide_down(
            multiply_down(radii.a, radii.one_es_lower),
            m_denominator_upper,
        );
        IntervalRadii {
            near_abs_lower,
            n_max,
            m_max,
            min_radius: n_min.min(m_min),
        }
    }

    fn absolute_difference_upper(left: f64, right: f64) -> f64 {
        if left == right {
            return 0.0;
        }
        let left_lower = round_down(left);
        let left_upper = round_up(left);
        let right_lower = round_down(right);
        let right_upper = round_up(right);
        round_up(
            (left_upper - right_lower)
                .abs()
                .max((right_upper - left_lower).abs()),
        )
    }

    fn cosine_upper(angle_lower: f64) -> f64 {
        // On [0, π/2], the alternating Taylor polynomial through x⁴/4!
        // is an upper bound for cos(x).
        let square_lower = multiply_down(angle_lower, angle_lower).max(0.0);
        let square_upper = multiply_up(angle_lower, angle_lower).max(0.0);
        let fourth_upper = multiply_up(square_upper, square_upper);
        add_up(
            subtract_up(1.0, divide_down(square_lower, 2.0)),
            divide_up(fourth_upper, 24.0),
        )
        .min(1.0)
    }

    fn sine_lower(angle_lower: f64) -> f64 {
        // sin(x) >= x − x³/3! on [0, π/2], by the alternating series.
        let square_upper = multiply_up(angle_lower, angle_lower);
        subtract_down(
            angle_lower,
            divide_up(multiply_up(angle_lower, square_upper), 6.0),
        )
        .max(0.0)
    }

    fn sine_upper(angle_upper: f64) -> f64 {
        if angle_upper >= round_down(std::f64::consts::FRAC_PI_2) {
            return 1.0;
        }
        let square_lower = multiply_down(angle_upper, angle_upper).max(0.0);
        let square_upper = multiply_up(angle_upper, angle_upper).max(0.0);
        let cube_lower = multiply_down(square_lower, angle_upper);
        let fourth_upper = multiply_up(square_upper, square_upper);
        let fifth_upper = multiply_up(fourth_upper, angle_upper);
        add_up(
            subtract_up(angle_upper, divide_down(cube_lower, 6.0)),
            divide_up(fifth_upper, 120.0),
        )
        .min(1.0)
    }

    fn tangent_upper(angle_upper: f64) -> f64 {
        let pole_distance_lower =
            subtract_down(round_down(std::f64::consts::FRAC_PI_2), angle_upper);
        if pole_distance_lower <= 0.0 {
            return f64::INFINITY;
        }
        let reciprocal_colatitude = divide_up(1.0, pole_distance_lower);
        if angle_upper >= round_down(std::f64::consts::FRAC_PI_2) {
            return reciprocal_colatitude;
        }

        let square_upper = multiply_up(angle_upper, angle_upper);
        let square_lower = multiply_down(angle_upper, angle_upper).max(0.0);
        let fourth_lower = multiply_down(square_lower, square_lower);
        let fourth_upper = multiply_up(square_upper, square_upper);
        let sixth_upper = multiply_up(fourth_upper, square_upper);
        let cosine_lower = add_down(
            subtract_down(1.0, divide_up(square_upper, 2.0)),
            divide_down(fourth_lower, 24.0),
        );
        let cosine_lower = subtract_down(cosine_lower, divide_up(sixth_upper, 720.0));
        if cosine_lower <= 0.0 {
            return reciprocal_colatitude;
        }
        divide_up(sine_upper(angle_upper), cosine_lower).min(reciprocal_colatitude)
    }

    fn source_length_upper_squared(
        radii: &EllipsoidRadii,
        latitude0: f64,
        latitude1: f64,
        longitude_delta: f64,
        coarse: bool,
    ) -> f64 {
        let interval = ellipsoid_interval_radii(radii, latitude0, latitude1);
        let cosine = if coarse {
            round_up(round_up(std::f64::consts::FRAC_PI_2) - interval.near_abs_lower).min(1.0)
        } else {
            cosine_upper(interval.near_abs_lower)
        };
        let n_max = if coarse { radii.n_max } else { interval.n_max };
        let m_max = if coarse { radii.m_max } else { interval.m_max };
        let east = multiply_up(multiply_up(n_max, cosine), round_up(longitude_delta.abs()));
        let north = multiply_up(m_max, absolute_difference_upper(latitude0, latitude1));
        add_up(multiply_up(east, east), multiply_up(north, north))
    }

    fn gaussian_latitude_endpoint(phi: f64, p_min: f64, p_max: f64) -> (f64, f64) {
        let absolute_lower = round_down(phi.abs()).max(0.0);
        let absolute_upper = round_up(phi.abs()).min(round_up(std::f64::consts::FRAC_PI_2));
        let pole_lower = round_down(std::f64::consts::FRAC_PI_2);
        let pole_upper = round_up(std::f64::consts::FRAC_PI_2);
        let delta_lower = subtract_down(pole_lower, absolute_upper).max(0.0);
        let delta_upper = subtract_up(pole_upper, absolute_lower).max(0.0);
        let lower = multiply_down(p_min, absolute_lower)
            .max(subtract_down(pole_lower, multiply_up(p_max, delta_upper)))
            .max(0.0);
        let upper = multiply_up(p_max, absolute_upper)
            .min(subtract_up(pole_upper, multiply_down(p_min, delta_lower)))
            .min(pole_upper);
        if phi < 0.0 {
            (round_down(-upper), round_up(-lower))
        } else {
            (lower, upper)
        }
    }

    fn gaussian_latitude_interval(
        latitude0: f64,
        latitude1: f64,
        p_min: f64,
        p_max: f64,
    ) -> (f64, f64) {
        let (lower0, upper0) = gaussian_latitude_endpoint(latitude0, p_min, p_max);
        let (lower1, upper1) = gaussian_latitude_endpoint(latitude1, p_min, p_max);
        (lower0.min(lower1), upper0.max(upper1))
    }

    fn absolute_interval_extrema(lower: f64, upper: f64) -> (f64, f64) {
        let near = if lower <= 0.0 && upper >= 0.0 {
            0.0
        } else {
            round_down(lower.abs().min(upper.abs())).max(0.0)
        };
        let far = round_up(lower.abs().max(upper.abs())).min(round_up(std::f64::consts::FRAC_PI_2));
        (near, far)
    }

    fn lambda_absolute_upper(longitude0: f64, longitude_delta: f64) -> f64 {
        let lambda0_lower = round_down(longitude0);
        let lambda0_upper = round_up(longitude0);
        let delta_lower = round_down(longitude_delta);
        let delta_upper = round_up(longitude_delta);
        let lambda1_lower = add_down(lambda0_lower, delta_lower);
        let lambda1_upper = add_up(lambda0_upper, delta_upper);
        round_up(
            lambda0_lower
                .abs()
                .max(lambda0_upper.abs())
                .max(lambda1_lower.abs())
                .max(lambda1_upper.abs()),
        )
    }

    fn maximum_absolute_sine(longitude0: f64, longitude_delta: f64) -> f64 {
        // |sin(x)| <= min(1, |x|), so the interval needs no trigonometric call.
        lambda_absolute_upper(longitude0, longitude_delta).min(1.0)
    }

    pub(super) fn piece_count_from_bound(bound: f64, budget: f64) -> NonZeroUsize {
        if bound <= budget {
            return NonZeroUsize::MIN;
        }
        if budget <= 0.0 {
            return NonZeroUsize::new(crate::geometry::GENERATED_ITEM_LIMIT + 2)
                .expect("the generated-item limit is positive");
        }
        let ratio = divide_up(bound, budget);
        let maximum_pieces = crate::geometry::GENERATED_ITEM_LIMIT + 1;
        if !ratio.is_finite() || ratio > (maximum_pieces as u64).pow(2) as f64 {
            return NonZeroUsize::new(maximum_pieces + 1)
                .expect("the generated-item limit is positive");
        }
        let mut lower = 1_usize;
        let mut upper = maximum_pieces;
        while lower < upper {
            let middle = lower.wrapping_add(upper.wrapping_sub(lower) / 2);
            let square = (middle as u64).pow(2) as f64;
            if square >= ratio {
                upper = middle;
            } else {
                lower = middle.wrapping_add(1);
            }
        }
        let pieces = lower;
        NonZeroUsize::new(pieces).expect("the analytic piece count is positive")
    }

    pub(super) fn cheap_tmerc_bound(
        radii: &EllipsoidRadii,
        certificate: &TmercCertificate,
        longitude0: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        let (near_abs, far_abs) = latitude_absolute_range(latitude0, latitude1);
        let pi_half_upper = round_up(std::f64::consts::FRAC_PI_2);
        let delta_near_upper = subtract_up(pi_half_upper, near_abs).max(0.0);
        let delta_far_lower =
            subtract_down(round_down(std::f64::consts::FRAC_PI_2), far_abs).max(0.0);
        if delta_far_lower == 0.0 {
            return f64::INFINITY;
        }
        let cos_chi_upper = multiply_up(certificate.p_max, delta_near_upper).min(1.0);
        let sin_lambda_upper = lambda_absolute_upper(longitude0, longitude_delta).min(1.0);
        let beta = multiply_up(cos_chi_upper, sin_lambda_upper);
        let beta2 = multiply_up(beta, beta);
        let spherical_denominator = subtract_down(1.0, beta2);
        if spherical_denominator <= 0.0 {
            return f64::INFINITY;
        }
        let (c1, c2) = if beta <= FAST_TMER_BETA {
            (certificate.c1_fast, certificate.c2_fast)
        } else {
            correction_derivative_bounds(&certificate.gtu, 0.0, beta)
        };
        let minimum_scale = subtract_down(1.0, c1);
        if minimum_scale <= 0.0 {
            return f64::INFINITY;
        }
        let latitude_curvature = add_up(
            divide_up(
                GAUSSIAN_TANGENTIAL_CURVATURE,
                multiply_down(certificate.p_min, delta_far_lower),
            ),
            divide_up(
                multiply_up(GAUSSIAN_LATITUDE_ACCELERATION, certificate.p2_max),
                multiply_down(certificate.p_min, certificate.p_min),
            ),
        );
        let logarithmic_gradient = divide_up(beta, spherical_denominator);
        let correction_gradient = divide_up(c2, minimum_scale);
        let curvature_numerator = add_up(
            add_up(latitude_curvature, logarithmic_gradient),
            correction_gradient,
        );
        let curvature = divide_up(
            curvature_numerator,
            multiply_down(certificate.radius_lower, minimum_scale),
        );
        let image_scale = divide_up(
            multiply_up(
                multiply_up(certificate.radius_upper, add_up(1.0, c1)),
                certificate.p_max,
            ),
            multiply_down(radii.min_radius, spherical_denominator),
        );
        let coefficient = divide_up(
            multiply_up(curvature, multiply_up(image_scale, image_scale)),
            8.0,
        );
        let cosine_latitude_upper = round_up(delta_near_upper).min(1.0);
        let east_weight = multiply_up(radii.n_max, cosine_latitude_upper);
        let east = multiply_up(east_weight, round_up(longitude_delta.abs()));
        let north = multiply_up(radii.m_max, absolute_difference_upper(latitude0, latitude1));
        add_up(
            multiply_up(coefficient, multiply_up(east, east)),
            multiply_up(coefficient, multiply_up(north, north)),
        )
    }

    pub(super) fn precise_tmerc_bound(
        radii: &EllipsoidRadii,
        certificate: &TmercCertificate,
        longitude0: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        let Some((curvature, image_scale)) = tmerc_curvature_and_scale(
            radii,
            certificate,
            longitude0,
            longitude_delta,
            latitude0,
            latitude1,
        ) else {
            return f64::INFINITY;
        };
        let length2 =
            source_length_upper_squared(radii, latitude0, latitude1, longitude_delta, false);
        divide_up(
            multiply_up(
                multiply_up(curvature, multiply_up(image_scale, image_scale)),
                length2,
            ),
            8.0,
        )
    }

    fn tmerc_curvature_and_scale(
        radii: &EllipsoidRadii,
        certificate: &TmercCertificate,
        longitude0: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> Option<(f64, f64)> {
        let (chi_lower, chi_upper) =
            gaussian_latitude_interval(latitude0, latitude1, certificate.p_min, certificate.p_max);
        let (chi_abs_lower, chi_abs_upper) = absolute_interval_extrema(chi_lower, chi_upper);
        let cosine_chi_upper = cosine_upper(chi_abs_lower);
        let longitude_sine_upper = maximum_absolute_sine(longitude0, longitude_delta);
        let beta = multiply_up(cosine_chi_upper, longitude_sine_upper);
        let beta2 = multiply_up(beta, beta);
        let spherical_denominator = subtract_down(1.0, beta2);
        if beta >= 1.0 || spherical_denominator <= 0.0 {
            return None;
        }
        let (c1, c2) = correction_derivative_bounds(&certificate.gtu, 0.0, beta);
        let minimum_scale = subtract_down(1.0, c1);
        if minimum_scale <= 0.0 {
            return None;
        }
        let curvature_sphere = longitude_curvature(
            certificate.p_min,
            certificate.p2_max,
            longitude_delta,
            latitude0,
            latitude1,
            chi_abs_upper,
        );
        let logarithmic_gradient = divide_up(beta, spherical_denominator);
        let correction_gradient = divide_up(c2, minimum_scale);
        let curvature = divide_up(
            add_up(
                add_up(curvature_sphere, logarithmic_gradient),
                correction_gradient,
            ),
            multiply_down(certificate.radius_lower, minimum_scale),
        );
        let image_scale = divide_up(
            multiply_up(
                multiply_up(certificate.radius_upper, add_up(1.0, c1)),
                certificate.p_max,
            ),
            multiply_down(radii.min_radius, spherical_denominator),
        );
        Some((curvature, image_scale))
    }

    pub(super) fn cheap_ups_bound(
        radii: &EllipsoidRadii,
        certificate: UpsCertificate,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        let (latitude0, latitude1) = toward_pole(certificate.pole, latitude0, latitude1);
        let (chi_lower, _chi_upper) =
            gaussian_latitude_interval(latitude0, latitude1, certificate.p_min, certificate.p_max);
        let (scale_upper, gradient_upper) = if chi_lower >= 0.0 {
            let delta_north_upper = subtract_up(round_up(std::f64::consts::FRAC_PI_2), chi_lower);
            let half_delta = divide_up(delta_north_upper, 2.0);
            let cosine_lower =
                subtract_down(1.0, divide_up(multiply_up(half_delta, half_delta), 2.0));
            if cosine_lower <= 0.0 {
                return f64::INFINITY;
            }
            (
                divide_up(1.0, multiply_down(cosine_lower, cosine_lower)),
                divide_up(half_delta, cosine_lower),
            )
        } else {
            let antipode_colatitude_lower =
                add_down(round_down(std::f64::consts::FRAC_PI_2), chi_lower);
            if antipode_colatitude_lower <= 0.0 {
                return f64::INFINITY;
            }
            (
                divide_up(
                    multiply_up(std::f64::consts::PI, std::f64::consts::PI),
                    multiply_down(antipode_colatitude_lower, antipode_colatitude_lower),
                ),
                divide_up(2.0, antipode_colatitude_lower),
            )
        };
        let (near_abs, far_abs) = latitude_absolute_range(latitude0, latitude1);
        let delta_far_lower =
            subtract_down(round_down(std::f64::consts::FRAC_PI_2), far_abs).max(0.0);
        if delta_far_lower == 0.0 {
            return f64::INFINITY;
        }
        let curvature_sphere = add_up(
            divide_up(
                GAUSSIAN_TANGENTIAL_CURVATURE,
                multiply_down(certificate.p_min, delta_far_lower),
            ),
            divide_up(
                multiply_up(GAUSSIAN_LATITUDE_ACCELERATION, certificate.p2_max),
                multiply_down(certificate.p_min, certificate.p_min),
            ),
        );
        let curvature = divide_up(
            add_up(curvature_sphere, gradient_upper),
            certificate.radius_lower,
        );
        let image_scale = divide_up(
            multiply_up(
                multiply_up(certificate.radius_upper, scale_upper),
                certificate.p_max,
            ),
            radii.min_radius,
        );
        let coefficient = divide_up(
            multiply_up(curvature, multiply_up(image_scale, image_scale)),
            8.0,
        );
        let cosine_latitude_upper =
            round_up(round_up(std::f64::consts::FRAC_PI_2) - near_abs).min(1.0);
        let east = multiply_up(
            multiply_up(
                multiply_up(radii.n_max, cosine_latitude_upper),
                round_up(longitude_delta.abs()),
            ),
            1.0,
        );
        let north = multiply_up(radii.m_max, absolute_difference_upper(latitude0, latitude1));
        add_up(
            multiply_up(coefficient, multiply_up(east, east)),
            multiply_up(coefficient, multiply_up(north, north)),
        )
    }

    pub(super) fn precise_ups_bound(
        radii: &EllipsoidRadii,
        certificate: UpsCertificate,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
    ) -> f64 {
        let (latitude0, latitude1) = toward_pole(certificate.pole, latitude0, latitude1);
        let (chi_lower, chi_upper) =
            gaussian_latitude_interval(latitude0, latitude1, certificate.p_min, certificate.p_max);
        if chi_lower < 0.0 {
            return f64::INFINITY;
        }
        let denominator_lower = add_down(1.0, sine_lower(chi_lower));
        let denominator_upper = add_up(1.0, sine_upper(chi_upper));
        if denominator_lower <= 0.0 {
            return f64::INFINITY;
        }
        let scale_min_lower = divide_down(2.0, denominator_upper);
        let scale_max_upper = divide_up(2.0, denominator_lower);
        let gradient_upper = divide_up(cosine_upper(chi_lower), denominator_lower);
        let (_, chi_abs_upper) = absolute_interval_extrema(chi_lower, chi_upper);
        let (_, far_abs) = latitude_absolute_range(latitude0, latitude1);
        let delta_far_lower =
            subtract_down(round_down(std::f64::consts::FRAC_PI_2), far_abs).max(0.0);
        if delta_far_lower == 0.0 {
            return f64::INFINITY;
        }
        let curvature_sphere = longitude_curvature(
            certificate.p_min,
            certificate.p2_max,
            longitude_delta,
            latitude0,
            latitude1,
            chi_abs_upper,
        );
        let curvature = divide_up(
            add_up(curvature_sphere, gradient_upper),
            multiply_down(certificate.radius_lower, scale_min_lower),
        );
        let image_scale = divide_up(
            multiply_up(
                multiply_up(certificate.radius_upper, scale_max_upper),
                certificate.p_max,
            ),
            ellipsoid_interval_radii(radii, latitude0, latitude1).min_radius,
        );
        let length2 =
            source_length_upper_squared(radii, latitude0, latitude1, longitude_delta, false);
        divide_up(
            multiply_up(
                multiply_up(curvature, multiply_up(image_scale, image_scale)),
                length2,
            ),
            8.0,
        )
    }

    fn toward_pole(pole: GeodeticPole, latitude0: f64, latitude1: f64) -> (f64, f64) {
        match pole {
            GeodeticPole::North => (latitude0, latitude1),
            GeodeticPole::South => (-latitude0, -latitude1),
        }
    }

    fn longitude_curvature(
        p_min: f64,
        p2_max: f64,
        longitude_delta: f64,
        latitude0: f64,
        latitude1: f64,
        chi_abs_upper: f64,
    ) -> f64 {
        if longitude_delta == 0.0 {
            return 0.0;
        }
        let tangent_upper = tangent_upper(chi_abs_upper);
        if !tangent_upper.is_finite() {
            return f64::INFINITY;
        }
        if latitude0 == latitude1 {
            return tangent_upper;
        }
        let v = absolute_difference_upper(latitude0, latitude1);
        let tangential = multiply_up(GAUSSIAN_TANGENTIAL_CURVATURE, tangent_upper);
        let orientation = divide_up(
            multiply_up(2.0, round_up(longitude_delta.abs())),
            multiply_down(p_min, v),
        );
        add_up(
            tangential.min(orientation),
            divide_up(
                multiply_up(GAUSSIAN_LATITUDE_ACCELERATION, p2_max),
                multiply_down(p_min, p_min),
            ),
        )
    }

    #[cfg(test)]
    mod tests {
        use super::super::{LocalCrsDefinition, LocalProjectionCertificate, ProjectionCertificate};
        use super::*;

        #[test]
        fn long_parallel_certificate_bounds_offset_endpoint_direction() {
            let state = crate::crs::runtime_state_snapshot().unwrap();
            let frame = crate::crs::info_with_state("EPSG:4326", &state)
                .unwrap()
                .geographic_frame()
                .unwrap();
            let geodesic = crate::crs::geodesic_for_crs_at("EPSG:4326", &state).unwrap();
            let local = LocalCrsDefinition {
                definition: "EPSG:32631".to_owned(),
                display_name: None,
                projected_pole: None,
            };
            let certificate =
                LocalProjectionCertificate::from_selected_frame(&local, &geodesic, frame, &state)
                    .unwrap();
            let ProjectionCertificate::Tmerc(projection) = certificate.projection else {
                panic!("UTM uses Transverse Mercator");
            };
            let west = -0.463_147_336_714_631_5;
            let east = 6.463_147_336_714_631_5;
            let corners = [
                crate::geometry::Point::new_raw_xy(west, 39.0),
                crate::geometry::Point::new_raw_xy(east, 39.0),
                crate::geometry::Point::new_raw_xy(east, 39.2),
                crate::geometry::Point::new_raw_xy(west, 39.2),
                crate::geometry::Point::new_raw_xy(west, 39.0),
            ];
            for &[start, end] in corners.array_windows::<2>() {
                let edge = certificate.chart_edge(start, end).unwrap();
                let (curvature, image_scale) = tmerc_curvature_and_scale(
                    &certificate.radii,
                    &projection,
                    edge.longitude_from_central_meridian,
                    edge.longitude_delta,
                    edge.latitude0,
                    edge.latitude1,
                )
                .unwrap();
                // These analytic envelopes bound the independent public oracle:
                // K*l bounds tangent rotation, and K*l²/8 bounds chord error.
                assert!((0.0..=2.0e-7).contains(&curvature));
                assert!(image_scale <= 1.02);
                if start.y == end.y {
                    assert!(curvature >= 1.0e-7);
                    let length2 = source_length_upper_squared(
                        &certificate.radii,
                        edge.latitude0,
                        edge.latitude1,
                        edge.longitude_delta,
                        false,
                    );
                    for budget in [0.1, 1.0] {
                        let count =
                            certificate.piece_count(start, end, budget).unwrap().get() as f64;
                        let image_piece_length = image_scale * length2.sqrt() / count;
                        assert!(image_piece_length <= (8.0 * budget / 1.0e-7).sqrt());
                    }
                }
            }
        }
    }
}
