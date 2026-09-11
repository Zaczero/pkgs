//! Certified strict-interior inverse on an oblate ellipsoid.
//!
//! The inverse is parameterised by the initial azimuth on the auxiliary
//! sphere.  A candidate only seeds the search; every accepted endpoint is
//! replayed through the exact-ingress arithmetic and the positive square-root
//! crossing formulas.  In particular, the root is not selected from a
//! stationary endpoint with an arbitrary sign of `cos(sigma2)`.

use geographiclib_rs::{Geodesic, geodesic_capability as caps};

use super::taylor::{TaylorOptions, TaylorPhaseInterval, TaylorSource, TaylorSurface};
use super::{
    CertifiedBinary64, Degree, Dyadic, Enclosure, ExactBinary64, PrecisionContext, RoundedBinary64,
    Sign, evaluate_graded_with_moments_enclosed, numerical_indeterminacy_error, pi_bracket,
    principal_longitude_difference, sin_cos_degrees, sin_cos_radians_enclosure,
};
use crate::crs::CrsError;
use crate::error::Result;

const MAX_ROOT_STEPS: usize = 4_096;
const MAX_BOUNDARY_PROBES: usize = 256;
const MAX_PRECISION_REFINEMENTS: usize = 32;

/// A strict-interior point retained at exact binary64 ingress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OblatePoint {
    longitude: Degree,
    latitude: Degree,
}

impl OblatePoint {
    pub(crate) fn from_f64(longitude: f64, latitude: f64) -> Result<Self> {
        if !longitude.is_finite()
            || !latitude.is_finite()
            || !(-180.0..=180.0).contains(&longitude)
            || !(-90.0..=90.0).contains(&latitude)
        {
            return Err(CrsError::message(
                "certified oblate geodesic coordinates must be finite longitude in [-180, 180] \
                 and latitude in [-90, 90]",
            ));
        }
        Ok(Self {
            longitude: Degree::from_f64(longitude)?,
            latitude: Degree::from_f64(latitude)?,
        })
    }

    pub(crate) fn from_bits(longitude: u64, latitude: u64) -> Result<Self> {
        Self::from_f64(f64::from_bits(longitude), f64::from_bits(latitude))
    }

    pub(crate) const fn longitude_bits(self) -> u64 {
        self.longitude.bits()
    }

    pub(crate) const fn latitude_bits(self) -> u64 {
        self.latitude.bits()
    }
}

/// Exact source for an oblate ellipsoid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OblateModel {
    semi_major: ExactBinary64,
    flattening: ExactBinary64,
}

impl OblateModel {
    pub(crate) fn from_f64(semi_major: f64, flattening: f64) -> Result<Self> {
        let semi_major = ExactBinary64::from_f64(semi_major)?;
        let flattening = ExactBinary64::from_f64(flattening)?;
        if semi_major.to_f64() <= 0.0 || !(0.0 < flattening.to_f64() && flattening.to_f64() < 1.0) {
            return Err(CrsError::message(
                "certified oblate geodesic model requires positive semi-major radius and \
                 flattening in (0, 1)",
            ));
        }
        Ok(Self {
            semi_major,
            flattening,
        })
    }

    pub(crate) fn from_bits(semi_major: u64, flattening: u64) -> Result<Self> {
        Self::from_f64(f64::from_bits(semi_major), f64::from_bits(flattening))
    }

    pub(crate) const fn semi_major_bits(self) -> u64 {
        self.semi_major.bits()
    }

    pub(crate) const fn flattening_bits(self) -> u64 {
        self.flattening.bits()
    }
}

/// Complete exact source material for one oblate inverse attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OblateInput {
    start: OblatePoint,
    end: OblatePoint,
    model: OblateModel,
}

impl OblateInput {
    pub(crate) fn from_f64(
        start: (f64, f64),
        end: (f64, f64),
        semi_major: f64,
        flattening: f64,
    ) -> Result<Self> {
        Ok(Self {
            start: OblatePoint::from_f64(start.0, start.1)?,
            end: OblatePoint::from_f64(end.0, end.1)?,
            model: OblateModel::from_f64(semi_major, flattening)?,
        })
    }

    pub(crate) fn from_bits(
        start: (u64, u64),
        end: (u64, u64),
        semi_major: u64,
        flattening: u64,
    ) -> Result<Self> {
        Ok(Self {
            start: OblatePoint::from_bits(start.0, start.1)?,
            end: OblatePoint::from_bits(end.0, end.1)?,
            model: OblateModel::from_bits(semi_major, flattening)?,
        })
    }

    pub(crate) const fn start(self) -> OblatePoint {
        self.start
    }

    pub(crate) const fn end(self) -> OblatePoint {
        self.end
    }

    pub(crate) const fn model(self) -> OblateModel {
        self.model
    }
}

/// The canonical longitude orientation retained by the branch identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OblateLongitudeDirection {
    East,
    West,
}

/// Canonical transformations performed before the auxiliary-sphere solve.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OblateCanonicalization {
    ReducedLatitudesAndPositivePrincipalLongitude(OblateLongitudeDirection),
}

/// The strict interior branch certified by this module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OblateBranch {
    FirstForwardCrossing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OblateMultiplicity {
    UniqueMinimizer,
}

impl OblateBranch {
    pub(crate) const fn multiplicity(self) -> OblateMultiplicity {
        match self {
            Self::FirstForwardCrossing => OblateMultiplicity::UniqueMinimizer,
        }
    }
}

/// Certified identity of a selected oblate branch.
///
/// `root` is the initial-azimuth bracket.  The source coordinates, model and
/// canonicalization are retained because rounded output cannot reconstruct the
/// auxiliary-sphere curve.
#[derive(Debug)]
pub(crate) struct OblateBranchIdentity {
    input: OblateInput,
    canonicalization: OblateCanonicalization,
    branch: OblateBranch,
    root: Enclosure,
    theta: Enclosure,
}

impl OblateBranchIdentity {
    pub(crate) const fn input(&self) -> OblateInput {
        self.input
    }

    pub(crate) const fn canonicalization(&self) -> OblateCanonicalization {
        self.canonicalization
    }

    pub(crate) const fn branch(&self) -> OblateBranch {
        self.branch
    }

    pub(crate) const fn root(&self) -> &Enclosure {
        &self.root
    }

    pub(crate) const fn theta(&self) -> &Enclosure {
        &self.theta
    }
}

/// Certified strict-interior oblate inverse result.
#[derive(Debug)]
pub(crate) struct OblateGeodesic {
    identity: OblateBranchIdentity,
    length: CertifiedBinary64,
    azimuth_radians: CertifiedBinary64,
    reduced_length: CertifiedBinary64,
}

impl OblateGeodesic {
    pub(crate) const fn identity(&self) -> &OblateBranchIdentity {
        &self.identity
    }

    pub(crate) const fn branch(&self) -> OblateBranch {
        self.identity.branch
    }

    pub(crate) const fn multiplicity(&self) -> OblateMultiplicity {
        self.identity.branch.multiplicity()
    }

    pub(crate) const fn length(&self) -> &CertifiedBinary64 {
        &self.length
    }

    pub(crate) const fn azimuth_radians(&self) -> &CertifiedBinary64 {
        &self.azimuth_radians
    }

    pub(crate) const fn reduced_length(&self) -> &CertifiedBinary64 {
        &self.reduced_length
    }
}

/// Certify the minimizing strict-canonical oblate inverse.
///
/// This entry point deliberately declines all boundary and noncanonical
/// cases.  Numerical exhaustion is retried at a larger precision and remains
/// [`ErrorKind::NumericalIndeterminacy`](crate::error::ErrorKind::NumericalIndeterminacy)
/// when the caller's policy is exhausted.
pub(crate) fn classify_oblate_geodesic(
    input: OblateInput,
    context: &mut PrecisionContext,
) -> Result<OblateGeodesic> {
    for _ in 0..=MAX_PRECISION_REFINEMENTS {
        match classify_attempt(input, context) {
            Ok(result) => return Ok(result),
            Err(error) if is_numerical(&error) => {
                if context.refine().is_err() {
                    return Err(numerical_indeterminacy_error());
                }
            },
            Err(error) => return Err(error),
        }
    }
    Err(numerical_indeterminacy_error())
}

fn classify_attempt(input: OblateInput, context: &mut PrecisionContext) -> Result<OblateGeodesic> {
    let canonical = canonical_data(input, context)?;
    let root = find_root(&canonical, context)?;
    publish_result(&canonical, &root, context)
}

#[derive(Debug)]
struct CanonicalData {
    input: OblateInput,
    direction: OblateLongitudeDirection,
    d: Dyadic,
    eccentricity: Dyadic,
    longitude: Enclosure,
    u: Enclosure,
    cb1: Enclosure,
    w: Enclosure,
    cb2: Enclosure,
    candidate_longitude_degrees: f64,
}

fn canonical_data(input: OblateInput, context: &mut PrecisionContext) -> Result<CanonicalData> {
    let start_latitude = input.start.latitude.exact(context)?;
    let end_latitude = input.end.latitude.exact(context)?;
    let zero = Dyadic::zero();
    let ninety = Dyadic::from_i128(90, context)?;
    let negative_ninety = ninety.try_clone(context)?.negate();
    if start_latitude >= zero
        || start_latitude <= negative_ninety
        || end_latitude <= start_latitude
        || end_latitude >= start_latitude.negate()
    {
        return Err(CrsError::message(
            "certified oblate inverse requires strict canonical latitudes \
             phi1 < 0, phi1 < phi2 < -phi1",
        ));
    }

    let principal_longitude =
        principal_longitude_difference(input.start.longitude, input.end.longitude, context)?;
    let direction = match principal_longitude.sign() {
        Sign::Positive => OblateLongitudeDirection::East,
        Sign::Negative => OblateLongitudeDirection::West,
        Sign::Zero => {
            return Err(CrsError::message(
                "certified oblate inverse declines zero principal longitude",
            ));
        },
    };
    let magnitude = principal_longitude.abs(context)?;
    if magnitude >= Dyadic::from_i128(180, context)? {
        return Err(CrsError::message(
            "certified oblate inverse requires principal longitude magnitude below 180 degrees",
        ));
    }

    let semi_major = input.model.semi_major.exact(context)?;
    if semi_major.sign() != Sign::Positive {
        return Err(CrsError::message(
            "certified oblate inverse requires a positive semi-major radius",
        ));
    }
    let flattening = input.model.flattening.exact(context)?;
    if flattening.sign() != Sign::Positive {
        return Err(CrsError::message(
            "certified oblate inverse requires flattening strictly above zero",
        ));
    }
    let one = Dyadic::one(context)?;
    if flattening >= one {
        return Err(CrsError::message(
            "certified oblate inverse requires flattening strictly below one",
        ));
    }
    let d = one.sub(&flattening, context)?;
    let eccentricity = flattening.mul(&two_minus(&flattening, context)?, context)?;

    let start = reduced_latitude(input.start.latitude, &d, context)?;
    let end = reduced_latitude(input.end.latitude, &d, context)?;
    if start.1.lo().sign() != Sign::Positive || end.1.lo().sign() != Sign::Positive {
        return Err(numerical_indeterminacy_error());
    }

    let longitude = radians_from_degrees(&magnitude, context)?;
    let candidate_longitude_degrees = rounded_f64(&magnitude, context)?;

    Ok(CanonicalData {
        input,
        direction,
        d,
        eccentricity,
        longitude,
        u: start.0,
        cb1: start.1,
        w: end.0,
        cb2: end.1,
        candidate_longitude_degrees,
    })
}

fn two_minus(value: &Dyadic, context: &mut PrecisionContext) -> Result<Dyadic> {
    Dyadic::two(context)?.sub(value, context)
}

fn reduced_latitude(
    latitude: Degree,
    d: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<(Enclosure, Enclosure)> {
    let trigonometry = sin_cos_degrees(latitude, context)?;
    if trigonometry.cos().lo().sign() != Sign::Positive {
        return Err(numerical_indeterminacy_error());
    }
    let d_interval = Enclosure::exact(d.try_clone(context)?, context)?;
    let sine_squared = trigonometry.sin().mul(trigonometry.sin(), context)?;
    let cosine_squared = trigonometry.cos().mul(trigonometry.cos(), context)?;
    let d_squared = d_interval.mul(&d_interval, context)?;
    let norm_squared = cosine_squared.add(&d_squared.mul(&sine_squared, context)?, context)?;
    let norm = norm_squared.sqrt(context)?;
    if norm.lo().sign() != Sign::Positive {
        return Err(numerical_indeterminacy_error());
    }
    let sine = d_interval
        .mul(trigonometry.sin(), context)?
        .div(&norm, context)?;
    let cosine = trigonometry.cos().div(&norm, context)?;
    Ok((sine, cosine))
}

fn radians_from_degrees(degrees: &Dyadic, context: &mut PrecisionContext) -> Result<Enclosure> {
    let degree_value = Enclosure::exact(degrees.try_clone(context)?, context)?;
    let pi = pi_bracket(context)?;
    let one_eighty = Enclosure::exact(Dyadic::from_i128(180, context)?, context)?;
    degree_value.mul(&pi, context)?.div(&one_eighty, context)
}

fn rounded_f64(value: &Dyadic, context: &mut PrecisionContext) -> Result<f64> {
    match value.round_binary64(context)? {
        RoundedBinary64::Finite(bits) => Ok(f64::from_bits(bits)),
        RoundedBinary64::Overflow(_) => Err(numerical_indeterminacy_error()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SampleSign {
    Negative,
    Positive,
    Zero,
    Infeasible,
}

#[derive(Debug)]
struct SampleEvaluation {
    sign: SampleSign,
    v_sign: Option<Sign>,
}

#[derive(Debug)]
struct Crossing {
    v: Enclosure,
    c: Enclosure,
    square_root_r: Enclosure,
    theta: Enclosure,
    omega: Enclosure,
    sigma1: Enclosure,
    amplitude: Enclosure,
}

fn find_root(data: &CanonicalData, context: &mut PrecisionContext) -> Result<Enclosure> {
    let candidate = candidate_alpha(data, context)?;
    let candidate_evaluation = sample_at(data, &candidate, context)?;
    if candidate_evaluation.sign == SampleSign::Infeasible {
        return Err(numerical_indeterminacy_error());
    }
    let v_sign = candidate_evaluation
        .v_sign
        .ok_or_else(numerical_indeterminacy_error)?;
    if candidate_evaluation.sign == SampleSign::Zero {
        context.restart_attempt();
        return Enclosure::exact(candidate, context);
    }

    let search_upper = candidate_evaluation.sign == SampleSign::Negative;
    if let Some(bracket) = local_bracket(data, &candidate, search_upper, context)? {
        return bisect_root(data, bracket.0, bracket.1, context);
    }

    let (lower, upper) = match (v_sign, candidate_evaluation.sign) {
        (Sign::Positive, SampleSign::Positive) => (Dyadic::zero(), candidate),
        (Sign::Negative, SampleSign::Negative) => {
            let upper = upper_pi_probe(data, &candidate, context)?;
            (candidate, upper)
        },
        (Sign::Positive, SampleSign::Negative) => {
            find_first_component_bracket(data, candidate, context)?
        },
        (Sign::Negative, SampleSign::Positive) => {
            find_second_component_bracket(data, candidate, context)?
        },
        _ => return Err(numerical_indeterminacy_error()),
    };
    bisect_root(data, lower, upper, context)
}

/// Search a small, certified neighbourhood of the ordinary candidate before
/// falling back to the full feasible component.  The candidate only supplies
/// the centre; the returned endpoints are accepted only after this module
/// proves opposite residual signs.
fn local_bracket(
    data: &CanonicalData,
    candidate: &Dyadic,
    search_upper: bool,
    context: &mut PrecisionContext,
) -> Result<Option<(Dyadic, Dyadic)>> {
    // The first probe is several binary64 ulps wide at radian scale.  The
    // geometric expansion remains bounded and the final bisection still
    // proves the publication cell from its own residuals.
    context.restart_attempt();
    let upper_bound = if search_upper {
        Some(pi_bracket(context)?.lo().try_clone(context)?)
    } else {
        None
    };
    for exponent in [48_i64, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8] {
        context.restart_attempt();
        let offset = Dyadic::one(context)?.scale_pow2(-exponent, context)?;
        let probe = if search_upper {
            candidate.add_scratch(&offset, context)?
        } else {
            candidate.sub_scratch(&offset, context)?
        };
        if (!search_upper && probe.sign() != Sign::Positive)
            || (search_upper && upper_bound.as_ref().is_some_and(|bound| probe >= *bound))
        {
            continue;
        }
        let sample = sample_at(data, &probe, context)?;
        if sample.sign == SampleSign::Infeasible {
            continue;
        }
        match (search_upper, sample.sign) {
            (true, SampleSign::Positive) => {
                return Ok(Some((candidate.try_clone(context)?, probe)));
            },
            (false, SampleSign::Negative) => {
                return Ok(Some((probe, candidate.try_clone(context)?)));
            },
            _ => {},
        }
    }
    Ok(None)
}

fn candidate_alpha(data: &CanonicalData, context: &mut PrecisionContext) -> Result<Dyadic> {
    context.restart_attempt();
    // This call only chooses a component of the search and an initial point.
    // Its azimuth, distance, and branch decision never enter the certificate.
    let flattening = rounded_f64(&data.input.model.flattening.exact(context)?, context)?;
    let geodesic = Geodesic::new(1.0, flattening);
    let (_, _, azimuth, ..) = geodesic._gen_inverse_azi(
        data.input
            .start
            .latitude
            .exact(context)?
            .round_binary64(context)
            .and_then(|rounded| match rounded {
                RoundedBinary64::Finite(bits) => Ok(f64::from_bits(bits)),
                RoundedBinary64::Overflow(_) => Err(numerical_indeterminacy_error()),
            })?,
        0.0,
        data.input
            .end
            .latitude
            .exact(context)?
            .round_binary64(context)
            .and_then(|rounded| match rounded {
                RoundedBinary64::Finite(bits) => Ok(f64::from_bits(bits)),
                RoundedBinary64::Overflow(_) => Err(numerical_indeterminacy_error()),
            })?,
        data.candidate_longitude_degrees,
        caps::AZIMUTH,
    );
    if !azimuth.is_finite() || !(0.0 < azimuth && azimuth < 180.0) {
        return Err(numerical_indeterminacy_error());
    }
    let radians = azimuth.to_radians();
    ExactBinary64::from_f64(radians)?.exact(context)
}

fn upper_pi_probe(
    data: &CanonicalData,
    candidate: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    context.restart_attempt();
    let pi = pi_bracket(context)?;
    let upper = pi.lo().try_clone(context)?;
    if upper <= *candidate {
        return Err(numerical_indeterminacy_error());
    }
    let sample = sample_at(data, &upper, context)?;
    if sample.sign != SampleSign::Positive {
        return Err(numerical_indeterminacy_error());
    }
    Ok(upper)
}

fn find_first_component_bracket(
    data: &CanonicalData,
    candidate: Dyadic,
    context: &mut PrecisionContext,
) -> Result<(Dyadic, Dyadic)> {
    context.restart_attempt();
    let pi = pi_bracket(context)?;
    let target = pi.lo().scale_pow2(-1, context)?;
    if target <= candidate {
        return Err(numerical_indeterminacy_error());
    }
    for step in 1..=MAX_BOUNDARY_PROBES {
        context.restart_attempt();
        let distance = target.sub_scratch(&candidate, context)?;
        let offset = distance.scale_pow2(
            -i64::try_from(step).map_err(|_| numerical_indeterminacy_error())?,
            context,
        )?;
        let probe = target.sub_scratch(&offset, context)?;
        let sample = sample_at(data, &probe, context)?;
        match sample.sign {
            SampleSign::Positive => return Ok((candidate, probe)),
            SampleSign::Zero => {
                let other = probe.try_clone(context)?;
                return Ok((probe, other));
            },
            SampleSign::Infeasible => return Err(numerical_indeterminacy_error()),
            SampleSign::Negative => {},
        }
    }
    Err(numerical_indeterminacy_error())
}

fn find_second_component_bracket(
    data: &CanonicalData,
    candidate: Dyadic,
    context: &mut PrecisionContext,
) -> Result<(Dyadic, Dyadic)> {
    context.restart_attempt();
    let pi = pi_bracket(context)?;
    let target = pi.hi().scale_pow2(-1, context)?;
    if candidate <= target {
        return Err(numerical_indeterminacy_error());
    }
    for step in 1..=MAX_BOUNDARY_PROBES {
        context.restart_attempt();
        let distance = candidate.sub_scratch(&target, context)?;
        let offset = distance.scale_pow2(
            -i64::try_from(step).map_err(|_| numerical_indeterminacy_error())?,
            context,
        )?;
        let probe = target.add_scratch(&offset, context)?;
        let sample = sample_at(data, &probe, context)?;
        match sample.sign {
            SampleSign::Negative => return Ok((probe, candidate)),
            SampleSign::Zero => {
                let other = probe.try_clone(context)?;
                return Ok((other, probe));
            },
            SampleSign::Infeasible => return Err(numerical_indeterminacy_error()),
            SampleSign::Positive => {},
        }
    }
    Err(numerical_indeterminacy_error())
}

fn bisect_root(
    data: &CanonicalData,
    mut lower: Dyadic,
    mut upper: Dyadic,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let target_bits = context.active_precision_bits().saturating_sub(32).min(80);
    let target = Dyadic::one(context)?.scale_pow2(-i64::from(target_bits), context)?;
    for _ in 0..MAX_ROOT_STEPS {
        context.restart_attempt();
        let width = upper.sub_scratch(&lower, context)?;
        if width <= target {
            return Enclosure::new(lower, upper).ok_or_else(numerical_indeterminacy_error);
        }
        let midpoint = lower
            .add_scratch(&upper, context)?
            .scale_pow2(-1, context)?;
        let sample = sample_at(data, &midpoint, context)?;
        match sample.sign {
            SampleSign::Negative => lower = midpoint,
            SampleSign::Positive => upper = midpoint,
            SampleSign::Zero => return Enclosure::exact(midpoint, context),
            SampleSign::Infeasible => return Err(numerical_indeterminacy_error()),
        }
    }
    Err(numerical_indeterminacy_error())
}

fn sample_at(
    data: &CanonicalData,
    alpha: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<SampleEvaluation> {
    context.restart_attempt();
    let alpha = Enclosure::exact(alpha.try_clone(context)?, context)?;
    let Some(crossing) = crossing(data, &alpha, context)? else {
        return Ok(SampleEvaluation {
            sign: SampleSign::Infeasible,
            v_sign: None,
        });
    };
    let source = TaylorSource::from_enclosed_tokens(
        TaylorSurface::Oblate,
        data.input.model.flattening,
        crossing.amplitude.try_clone(context)?,
        crossing.sigma1.try_clone(context)?,
        zero_phase_interval()?,
    );
    let options = integration_options(context, false)?;
    let integrals =
        evaluate_graded_with_moments_enclosed(&source, &crossing.theta, options, context)?;
    let correction = Enclosure::exact(data.eccentricity.try_clone(context)?, context)?
        .mul(&crossing.c, context)?
        .mul(integrals.one_plus_reciprocal(), context)?;
    let residual = crossing
        .omega
        .sub(&correction, context)?
        .sub(&data.longitude, context)?;
    let zero = Dyadic::zero();
    let sign = if residual.hi() < &zero {
        SampleSign::Negative
    } else if residual.lo() > &zero {
        SampleSign::Positive
    } else if residual.lo().is_zero() && residual.hi().is_zero() {
        SampleSign::Zero
    } else {
        return Err(numerical_indeterminacy_error());
    };
    Ok(SampleEvaluation {
        sign,
        v_sign: crossing.v.sign(),
    })
}

fn zero_phase_interval() -> Result<TaylorPhaseInterval> {
    TaylorPhaseInterval::from_f64(0.0, 0.0)
}

fn crossing(
    data: &CanonicalData,
    alpha: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<Option<Crossing>> {
    let alpha_trigonometry = sin_cos_radians_enclosure(alpha, context)?;
    let v = data.cb1.mul(alpha_trigonometry.cos(), context)?;
    let c = data.cb1.mul(alpha_trigonometry.sin(), context)?;
    let u_squared = data.u.mul(&data.u, context)?;
    let v_squared = v.mul(&v, context)?;
    let d = u_squared.add(&v_squared, context)?;
    if d.sign() != Some(Sign::Positive) {
        return Err(numerical_indeterminacy_error());
    }
    let w_squared = data.w.mul(&data.w, context)?;
    let r = d.sub(&w_squared, context)?;
    if r.hi() < &Dyadic::zero() {
        return Ok(None);
    }
    if r.lo().sign() != Sign::Positive {
        return Err(numerical_indeterminacy_error());
    }
    let square_root_r = r.sqrt(context)?;
    let cos_numerator = data
        .u
        .mul(&data.w, context)?
        .add(&v.mul(&square_root_r, context)?, context)?;
    let sin_numerator = v
        .mul(&data.w, context)?
        .sub(&data.u.mul(&square_root_r, context)?, context)?;
    if sin_numerator.sign() != Some(Sign::Positive) {
        return Err(numerical_indeterminacy_error());
    }
    let theta = positive_angle(&cos_numerator, &sin_numerator, context)?;
    let pi = pi_bracket(context)?;
    if theta.lo().sign() != Sign::Positive || theta.hi() >= pi.lo() {
        return Err(numerical_indeterminacy_error());
    }

    let c_squared = c.mul(&c, context)?;
    let omega_x = v.mul(&square_root_r, context)?.add(
        &data.u.mul(&data.w, context)?.mul(&c_squared, context)?,
        context,
    )?;
    let omega_y = c.mul(&sin_numerator, context)?;
    if omega_y.sign() != Some(Sign::Positive) {
        return Err(numerical_indeterminacy_error());
    }
    let omega = positive_angle(&omega_x, &omega_y, context)?;

    let sigma1 = signed_angle(&v, &data.u, context)?;
    let amplitude = d.sqrt(context)?;
    let one = Dyadic::one(context)?;
    // `D = u² + v² = 1 - c²` for the exact auxiliary-sphere values, so
    // `A = sqrt(D)` is known to be at most one.  Tightening an outward
    // rounding overshoot to that proved bound is not a numerical snap.
    let amplitude = if amplitude.hi() > &one {
        if amplitude.lo() > &one {
            return Err(numerical_indeterminacy_error());
        }
        Enclosure::new(amplitude.lo().try_clone(context)?, one)
            .ok_or_else(numerical_indeterminacy_error)?
    } else {
        amplitude
    };
    if amplitude.lo().sign() != Sign::Positive {
        return Err(numerical_indeterminacy_error());
    }
    Ok(Some(Crossing {
        v,
        c,
        square_root_r,
        theta,
        omega,
        sigma1,
        amplitude,
    }))
}

fn positive_angle(
    x: &Enclosure,
    y: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    if y.sign() != Some(Sign::Positive) {
        return Err(numerical_indeterminacy_error());
    }
    angle_rectangle(x, y, context)
}

fn signed_angle(x: &Enclosure, y: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    if y.sign() != Some(Sign::Negative) {
        return Err(numerical_indeterminacy_error());
    }
    angle_rectangle(x, y, context)
}

fn angle_rectangle(
    x: &Enclosure,
    y: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let y_sign = y.sign().ok_or_else(numerical_indeterminacy_error)?;
    let mut result: Option<Enclosure> = None;
    for x_value in [x.lo(), x.hi()] {
        for y_value in [y.lo(), y.hi()] {
            let x_sign = x_value.sign();
            let y_sign = y_value.sign();
            if x_sign == Sign::Zero && y_sign == Sign::Zero {
                return Err(numerical_indeterminacy_error());
            }
            let angle = angle_point(x_value, y_value, context)?;
            result = Some(match result {
                Some(current) => current.hull(&angle, context)?,
                None => angle,
            });
        }
    }
    if x.lo() <= &Dyadic::zero() && x.hi() >= &Dyadic::zero() {
        let pi = pi_bracket(context)?;
        let axis = pi.scale_pow2(-1, context)?;
        let axis = if y_sign == Sign::Negative {
            Enclosure::new(
                axis.hi().try_clone(context)?.negate(),
                axis.lo().try_clone(context)?.negate(),
            )
            .ok_or_else(numerical_indeterminacy_error)?
        } else {
            axis
        };
        result = Some(match result {
            Some(current) => current.hull(&axis, context)?,
            None => axis,
        });
    }
    result.ok_or_else(numerical_indeterminacy_error)
}

fn angle_point(x: &Dyadic, y: &Dyadic, context: &mut PrecisionContext) -> Result<Enclosure> {
    let x_sign = x.sign();
    let y_sign = y.sign();
    if x_sign == Sign::Zero {
        let pi = pi_bracket(context)?;
        let half = pi.scale_pow2(-1, context)?;
        return if y_sign == Sign::Positive {
            Ok(half)
        } else if y_sign == Sign::Negative {
            Enclosure::new(
                half.hi().try_clone(context)?.negate(),
                half.lo().try_clone(context)?.negate(),
            )
            .ok_or_else(numerical_indeterminacy_error)
        } else {
            Err(numerical_indeterminacy_error())
        };
    }
    if y_sign == Sign::Zero {
        if x_sign == Sign::Positive {
            return Enclosure::exact(Dyadic::zero(), context);
        }
        let pi = pi_bracket(context)?;
        return if x_sign == Sign::Negative {
            Enclosure::new(
                pi.hi().try_clone(context)?.negate(),
                pi.lo().try_clone(context)?.negate(),
            )
            .ok_or_else(numerical_indeterminacy_error)
        } else {
            Err(numerical_indeterminacy_error())
        };
    }
    let x_abs = x.abs(context)?;
    let y_abs = y.abs(context)?;
    let ratio =
        Enclosure::exact(y_abs, context)?.div(&Enclosure::exact(x_abs, context)?, context)?;
    let acute = atan_nonnegative(&ratio, context)?;
    let pi = pi_bracket(context)?;
    match (x_sign, y_sign) {
        (Sign::Positive, Sign::Positive) => Ok(acute),
        (Sign::Positive, Sign::Negative) => negate_enclosure(&acute, context),
        (Sign::Negative, Sign::Positive) => pi.sub(&acute, context),
        (Sign::Negative, Sign::Negative) => {
            let negative_pi = Enclosure::new(
                pi.hi().try_clone(context)?.negate(),
                pi.lo().try_clone(context)?.negate(),
            )
            .ok_or_else(numerical_indeterminacy_error)?;
            negative_pi.add(&acute, context)
        },
        _ => Err(numerical_indeterminacy_error()),
    }
}

const MAX_ATAN_REDUCTION_DEPTH: usize = 64;
const MAX_ATAN_TERMS: usize = 8_192;

fn atan_nonnegative(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    if value.lo().sign() == Sign::Negative {
        return Err(numerical_indeterminacy_error());
    }
    atan_nonnegative_at_depth(value, context, 0)
}

fn atan_nonnegative_at_depth(
    value: &Enclosure,
    context: &mut PrecisionContext,
    depth: usize,
) -> Result<Enclosure> {
    if depth >= MAX_ATAN_REDUCTION_DEPTH {
        return Err(numerical_indeterminacy_error());
    }
    let one = Dyadic::one(context)?;
    let half = one.scale_pow2(-1, context)?;
    if value.hi() <= &half {
        return atan_series(value, context);
    }
    if value.lo() >= &one {
        if value.hi() <= &one {
            return pi_bracket(context)?.scale_pow2(-2, context);
        }
        let reciprocal = Enclosure::exact(one.try_clone(context)?, context)?.div(value, context)?;
        let small = atan_nonnegative_at_depth(&reciprocal, context, depth + 1)?;
        return pi_bracket(context)?
            .scale_pow2(-1, context)?
            .sub(&small, context);
    }
    if value.lo() >= &half && value.hi() <= &one {
        let numerator = value.sub(
            &Enclosure::exact(one.try_clone(context)?, context)?,
            context,
        )?;
        let denominator = value.add(
            &Enclosure::exact(one.try_clone(context)?, context)?,
            context,
        )?;
        let reduced = numerator.div(&denominator, context)?;
        let remainder = atan_series_signed(&reduced, context)?;
        return pi_bracket(context)?
            .scale_pow2(-2, context)?
            .add(&remainder, context);
    }
    if value.hi() > &one {
        let lower = Enclosure::new(value.lo().try_clone(context)?, one.try_clone(context)?)
            .ok_or_else(numerical_indeterminacy_error)?;
        let upper = Enclosure::new(one.try_clone(context)?, value.hi().try_clone(context)?)
            .ok_or_else(numerical_indeterminacy_error)?;
        return atan_nonnegative_at_depth(&lower, context, depth + 1)?.hull(
            &atan_nonnegative_at_depth(&upper, context, depth + 1)?,
            context,
        );
    }
    let lower = Enclosure::new(value.lo().try_clone(context)?, half.try_clone(context)?)
        .ok_or_else(numerical_indeterminacy_error)?;
    let upper = Enclosure::new(half.try_clone(context)?, value.hi().try_clone(context)?)
        .ok_or_else(numerical_indeterminacy_error)?;
    atan_nonnegative_at_depth(&lower, context, depth + 1)?.hull(
        &atan_nonnegative_at_depth(&upper, context, depth + 1)?,
        context,
    )
}

fn atan_series(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    atan_series_signed(value, context)
}

fn atan_series_signed(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    let target_bits = context.active_precision_bits().min(256);
    let target = Dyadic::one(context)?.scale_pow2(-i64::from(target_bits), context)?;
    let square = value.mul(value, context)?;
    let mut power = value.try_clone(context)?;
    let mut sum = Enclosure::exact(Dyadic::zero(), context)?;
    for index in 0..MAX_ATAN_TERMS {
        context.consume_fuel(1)?;
        let odd = index
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(numerical_indeterminacy_error)?;
        let denominator = Dyadic::from_u64(
            u64::try_from(odd).map_err(|_| numerical_indeterminacy_error())?,
            context,
        )?;
        let term = power.div(
            &Enclosure::exact(denominator.try_clone(context)?, context)?,
            context,
        )?;
        sum = if index % 2 == 0 {
            sum.add(&term, context)?
        } else {
            sum.sub(&term, context)?
        };

        let next_index = index
            .checked_add(1)
            .ok_or_else(numerical_indeterminacy_error)?;
        power = power.mul(&square, context)?;
        let next_odd = next_index
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(numerical_indeterminacy_error)?;
        let next_denominator = Dyadic::from_u64(
            u64::try_from(next_odd).map_err(|_| numerical_indeterminacy_error())?,
            context,
        )?;
        let radius = upper_abs_quotient(&power, &next_denominator, context)?;
        if radius <= target {
            return widen_symmetric(&sum, &radius, context);
        }
    }
    Err(numerical_indeterminacy_error())
}

fn upper_abs_quotient(
    value: &Enclosure,
    denominator: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    let lower = if value.lo().sign() == Sign::Negative {
        value.lo().try_clone(context)?.negate()
    } else {
        value.lo().try_clone(context)?
    };
    let upper = if value.hi().sign() == Sign::Negative {
        value.hi().try_clone(context)?.negate()
    } else {
        value.hi().try_clone(context)?
    };
    let maximum = if lower >= upper { lower } else { upper };
    maximum
        .divide(denominator, context)?
        .hi()
        .try_clone(context)
}

fn widen_symmetric(
    value: &Enclosure,
    radius: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    Enclosure::new(
        radius.try_clone(context)?.negate(),
        radius.try_clone(context)?,
    )
    .ok_or_else(numerical_indeterminacy_error)
    .and_then(|tail| value.add(&tail, context))
}

fn integration_options(
    context: &mut PrecisionContext,
    include_reduced_length: bool,
) -> Result<TaylorOptions> {
    let bits = context.active_precision_bits();
    let target_limit = if include_reduced_length { 64 } else { 80 };
    let target_bits = bits.saturating_sub(32).min(target_limit);
    let target = binary64_power_of_two(
        -i32::try_from(target_bits).map_err(|_| numerical_indeterminacy_error())?,
    );
    TaylorOptions::new(target, 128, 4_096, 64, include_reduced_length)
}

fn binary64_power_of_two(exponent: i32) -> f64 {
    debug_assert!((-1074..=0).contains(&exponent));
    let negative = exponent.unsigned_abs();
    if negative <= 1_022 {
        let encoded = u64::try_from(1_023_u32 - negative).unwrap_or(0);
        f64::from_bits(encoded << 52)
    } else {
        f64::from_bits(1_u64 << (1_074 - negative))
    }
}

fn publish_result(
    data: &CanonicalData,
    root: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<OblateGeodesic> {
    context.restart_attempt();
    let crossing = crossing(data, root, context)?.ok_or_else(numerical_indeterminacy_error)?;
    let source = TaylorSource::from_enclosed_tokens(
        TaylorSurface::Oblate,
        data.input.model.flattening,
        crossing.amplitude.try_clone(context)?,
        crossing.sigma1.try_clone(context)?,
        zero_phase_interval()?,
    );
    let options = integration_options(context, true)?;
    let integrals =
        evaluate_graded_with_moments_enclosed(&source, &crossing.theta, options, context)?;

    let semi_major = Enclosure::exact(data.input.model.semi_major.exact(context)?, context)?;
    let length = semi_major.mul(integrals.q(), context)?;
    let length = CertifiedBinary64::from_enclosure(length, context)?;

    let azimuth = if data.direction == OblateLongitudeDirection::West {
        negate_enclosure(root, context)?
    } else {
        root.try_clone(context)?
    };
    let azimuth = CertifiedBinary64::from_enclosure(azimuth, context)?;

    let reduced = reduced_length(data, &crossing, integrals.weighted_reciprocal(), context)?;
    let reduced = semi_major.mul(&reduced, context)?;
    let reduced = CertifiedBinary64::from_enclosure(reduced, context)?;

    let root_identity = root.try_clone(context)?;
    let theta_identity = crossing.theta.try_clone(context)?;
    Ok(OblateGeodesic {
        identity: OblateBranchIdentity {
            input: data.input,
            canonicalization: OblateCanonicalization::ReducedLatitudesAndPositivePrincipalLongitude(
                data.direction,
            ),
            branch: OblateBranch::FirstForwardCrossing,
            root: root_identity,
            theta: theta_identity,
        },
        length,
        azimuth_radians: azimuth,
        reduced_length: reduced,
    })
}

fn reduced_length(
    data: &CanonicalData,
    crossing: &Crossing,
    weighted: Option<&Enclosure>,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let weighted = weighted.ok_or_else(numerical_indeterminacy_error)?;
    let amplitude = &crossing.amplitude;
    let sigma1_sine = data.u.div(amplitude, context)?;
    let sigma1_cosine = crossing.v.div(amplitude, context)?;
    let sigma2_sine = data.w.div(amplitude, context)?;
    let sigma2_cosine = crossing.square_root_r.div(amplitude, context)?;
    let q1 = endpoint_q(&data.d, &data.eccentricity, &data.u, context)?;
    let q2 = endpoint_q(&data.d, &data.eccentricity, &data.w, context)?;
    let k12 =
        Enclosure::exact(data.eccentricity.try_clone(context)?, context)?.mul(weighted, context)?;
    let first = q2
        .mul(&sigma1_cosine, context)?
        .mul(&sigma2_sine, context)?;
    let second = q1
        .mul(&sigma1_sine, context)?
        .mul(&sigma2_cosine, context)?;
    let third = sigma1_cosine
        .mul(&sigma2_cosine, context)?
        .mul(&k12, context)?;
    let result = first.sub(&second, context)?.sub(&third, context)?;
    if result.hi() < &Dyadic::zero() {
        return Err(CrsError::message(
            "certified oblate first crossing produced a negative reduced length",
        ));
    }
    if result.lo().sign() != Sign::Positive {
        return Err(numerical_indeterminacy_error());
    }
    Ok(result)
}

fn endpoint_q(
    d: &Dyadic,
    eccentricity: &Dyadic,
    z: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let d = Enclosure::exact(d.try_clone(context)?, context)?;
    let d_squared = d.mul(&d, context)?;
    let z_squared = z.mul(z, context)?;
    let eccentricity = Enclosure::exact(eccentricity.try_clone(context)?, context)?;
    d_squared
        .add(&eccentricity.mul(&z_squared, context)?, context)?
        .sqrt(context)
}

fn negate_enclosure(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    Enclosure::new(
        value.hi().try_clone(context)?.negate(),
        value.lo().try_clone(context)?.negate(),
    )
    .ok_or_else(numerical_indeterminacy_error)
}

fn is_numerical(error: &crate::error::Error) -> bool {
    matches!(
        error.kind(),
        crate::error::ErrorKind::NumericalIndeterminacy
    )
}

#[cfg(test)]
mod tests {
    use super::super::RoundingCell;
    use super::*;
    use crate::crs::CrsError;
    use crate::error::{ErrorKind, ResourceError};
    use crate::test_support::fail_allocations_after;

    fn test_context() -> PrecisionContext {
        PrecisionContext::new(super::super::PrecisionPolicy::new(
            128,
            256,
            128_000_000,
            256 * 1024 * 1024,
        ))
        .expect("oblate test policy")
    }

    #[test]
    fn karney_longitude_negative_control_selects_the_first_crossing() {
        let input = OblateInput::from_f64(
            (0.0, -30.0),
            (179.8, 29.9),
            6_378_137.0,
            1.0 / 298.257_223_563,
        )
        .expect("strict input");
        let mut context = test_context();
        let result = classify_oblate_geodesic(input, &mut context).expect("oblate inverse");
        assert_eq!(result.branch(), OblateBranch::FirstForwardCrossing);
        assert_eq!(result.multiplicity(), OblateMultiplicity::UniqueMinimizer);
        assert!(result.identity().theta().lo() > &Dyadic::zero());
        let mut pi_context = test_context();
        let pi = pi_bracket(&mut pi_context).expect("pi bracket");
        assert!(
            result.identity().theta().hi() < pi.lo(),
            "the selected endpoint is before the later post-half-turn stationary branch"
        );
        assert!(result.length().value().is_finite());
        assert!(result.azimuth_radians().value().is_finite());
        assert!(result.reduced_length().value().is_finite());
        assert!(matches!(
            result.identity().canonicalization(),
            OblateCanonicalization::ReducedLatitudesAndPositivePrincipalLongitude(
                OblateLongitudeDirection::East
            )
        ));
        assert!(matches!(
            result.length().enclosure().rounding_cell(&mut context),
            Ok(RoundingCell::Unique(_))
        ));
    }

    #[test]
    fn strict_canonical_inverse_retains_west_orientation_and_source_bits() {
        let start = (0.0_f64.to_bits(), (-30.0_f64).to_bits());
        let end = ((-179.8_f64).to_bits(), 29.9_f64.to_bits());
        let model = (
            6_378_137.0_f64.to_bits(),
            (1.0 / 298.257_223_563_f64).to_bits(),
        );
        let input = OblateInput::from_bits(start, end, model.0, model.1).expect("strict input");
        let mut context = test_context();
        let result = classify_oblate_geodesic(input, &mut context).expect("west inverse");

        assert_eq!(result.identity().input(), input);
        assert_eq!(input.start().longitude_bits(), start.0);
        assert_eq!(input.start().latitude_bits(), start.1);
        assert_eq!(input.end().longitude_bits(), end.0);
        assert_eq!(input.end().latitude_bits(), end.1);
        assert_eq!(input.model().semi_major_bits(), model.0);
        assert_eq!(input.model().flattening_bits(), model.1);
        assert!(matches!(
            result.identity().canonicalization(),
            OblateCanonicalization::ReducedLatitudesAndPositivePrincipalLongitude(
                OblateLongitudeDirection::West
            )
        ));
        assert!(result.azimuth_radians().value() < 0.0);
    }

    #[test]
    fn strict_canonical_inverse_also_certifies_the_first_component() {
        let input = OblateInput::from_f64(
            (0.0, -30.0),
            (100.0, 20.0),
            6_378_137.0,
            1.0 / 298.257_223_563,
        )
        .expect("strict input");
        let mut context = test_context();
        let result =
            classify_oblate_geodesic(input, &mut context).expect("first component inverse");
        assert!(result.azimuth_radians().value() > 0.0);
        assert!(result.azimuth_radians().value() < std::f64::consts::FRAC_PI_2);
        let mut pi_context = test_context();
        assert!(
            result.identity().theta().hi() < pi_bracket(&mut pi_context).expect("pi bracket").lo()
        );
    }

    #[test]
    fn strict_canonical_inverse_handles_a_small_nonzero_longitude() {
        let input = OblateInput::from_f64(
            (0.0, -30.0),
            (0.001, 20.0),
            6_378_137.0,
            1.0 / 298.257_223_563,
        )
        .expect("strict input");
        let mut context = test_context();
        let result =
            classify_oblate_geodesic(input, &mut context).expect("small longitude inverse");
        assert!(result.azimuth_radians().value() > 0.0);
        assert!(result.length().value().is_finite());
    }

    #[test]
    fn exact_angle_boundaries_terminate_without_repeating_a_reduction() {
        let mut context = PrecisionContext::new(super::super::PrecisionPolicy::new(
            128,
            128,
            128_000_000,
            256 * 1024 * 1024,
        ))
        .expect("angle policy");
        let one = Dyadic::one(&mut context).expect("one");
        let zero = Dyadic::zero();
        for (x, y) in [
            (
                one.try_clone(&mut context).expect("x"),
                one.try_clone(&mut context).expect("y"),
            ),
            (one.try_clone(&mut context).expect("x"), zero),
        ] {
            let angle = angle_point(&x, &y, &mut context).expect("boundary angle");
            assert!(angle.lo() <= angle.hi());
        }
    }

    #[test]
    fn strict_inverse_declines_boundary_dispatch_explicitly() {
        let input = OblateInput::from_f64(
            (0.0, -30.0),
            (30.0, -30.0),
            6_378_137.0,
            1.0 / 298.257_223_563,
        )
        .expect("boundary-shaped input");
        let mut context = test_context();
        let error = classify_oblate_geodesic(input, &mut context)
            .expect_err("same-latitude boundary must be declined");
        assert!(matches!(error.kind(), ErrorKind::Crs(CrsError::Message(_))));
        assert!(error.to_string().contains("strict canonical latitudes"));
    }

    #[test]
    fn numerical_indeterminacy_and_allocation_failure_remain_distinct() {
        let input = OblateInput::from_f64(
            (0.0, -30.0),
            (30.0, 20.0),
            6_378_137.0,
            1.0 / 298.257_223_563,
        )
        .expect("strict input");
        let mut numerical_context = PrecisionContext::new(super::super::PrecisionPolicy::new(
            128,
            128,
            0,
            256 * 1024 * 1024,
        ))
        .expect("numerical policy");
        let numerical = classify_oblate_geodesic(input, &mut numerical_context)
            .expect_err("zero arithmetic fuel must be numerical indeterminacy");
        assert!(matches!(
            numerical.kind(),
            ErrorKind::NumericalIndeterminacy
        ));

        let allocation = fail_allocations_after(0, || {
            let mut context = test_context();
            classify_oblate_geodesic(input, &mut context)
        })
        .expect_err("allocator rejection");
        assert!(matches!(
            allocation.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }
}
