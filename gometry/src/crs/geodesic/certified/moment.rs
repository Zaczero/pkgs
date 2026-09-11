//! Certified small-flattening moment integration.
//!
//! The common oblate and prolate radicands have the same normalized form
//! once the signed flattening is retained:
//!
//! ```text
//! q² = 1 + lambda * u,
//! lambda = -f * (2 - f),
//! u = c² + A² cos²(sigma + t).
//! ```
//!
//! For `|lambda| <= 1/16`, `0 <= u <= 1` puts the power series for
//! `sqrt(1+x)`, `(1+x)^(-1/2)`, and `1/(1+sqrt(1+x))` inside their common
//! disk of convergence.  Every coefficient has absolute value at most one,
//! so the uncomputed terms have the explicit pointwise bound
//! `|lambda|^(N + 1) / (1 - |lambda|)`.  This module only publishes a result
//! after that bound and the directed arithmetic enclosure both fit the
//! requested target.  Inputs outside the admitted disk return `None` so the
//! complete graded Taylor evaluator can take over.
//!
//! In flattening terms, the admitted oblate range is
//! `0 <= f <= 1 - sqrt(15)/4`, and the admitted prolate range is
//! `1 - sqrt(17)/4 <= f <= 0`.  The source phase must be in `[-4, 4]`
//! radians and the nonnegative terminal phase in `[0, 4]` radians; these
//! bounds are inherited from the certified trigonometric evaluator and from
//! the exact nonnegative integration parameter.

use super::taylor::{
    TaylorIntegrals, TaylorOptions, TaylorSource, TaylorSurface, TaylorTailBounds,
    evaluate as evaluate_taylor, evaluate_enclosed as evaluate_taylor_enclosed,
};
use super::{
    Dyadic, Enclosure, Error, ErrorKind, PrecisionContext, Sign, numerical_indeterminacy,
    sin_cos_radians,
};
use crate::crs::CrsError;
use crate::error::{ResourceError, Result};

/// The dimensionless perturbation radius used by the coefficient proof.
///
/// The geometric series in the tail proof needs `r < 1`; `1/16` leaves a
/// large margin while still covering ordinary Earth-like flattenings and
/// makes the admissibility boundary a simple exact dyadic.
const MAX_LAMBDA_EXPONENT: i64 = -4;
const MAX_MOMENT_DEGREE: usize = 16;

/// Evaluate the ordinary-flattening moment path.
///
/// `Ok(None)` is a deliberate fast-path decline: the source phase or
/// flattening is outside the proved domain.  Numerical budget exhaustion is
/// returned as `NumericalIndeterminacy`, allowing the dispatch wrapper to
/// retry the complete evaluator.  Allocator failures remain resource errors.
pub(crate) fn evaluate(
    source: &TaylorSource,
    options: TaylorOptions,
    context: &mut PrecisionContext,
) -> Result<Option<TaylorIntegrals>> {
    let lower = source.terminal().lower().exact(context)?;
    let upper = source.terminal().upper().exact(context)?;
    evaluate_endpoints(source, lower, upper, options, context)
}

/// Evaluate with a terminal phase enclosure rebuilt at the active precision.
pub(crate) fn evaluate_enclosed(
    source: &TaylorSource,
    terminal: &Enclosure,
    options: TaylorOptions,
    context: &mut PrecisionContext,
) -> Result<Option<TaylorIntegrals>> {
    let lower = terminal.lo().try_clone(context)?;
    let upper = terminal.hi().try_clone(context)?;
    evaluate_endpoints(source, lower, upper, options, context)
}

fn evaluate_endpoints(
    source: &TaylorSource,
    lower: Dyadic,
    upper: Dyadic,
    options: TaylorOptions,
    context: &mut PrecisionContext,
) -> Result<Option<TaylorIntegrals>> {
    let Some(input) = prepare(source, lower, upper, options, context)? else {
        return Ok(None);
    };

    if options.max_panels() == 0 {
        return Err(numerical_indeterminacy());
    }

    let max_degree = options.max_degree().min(MAX_MOMENT_DEGREE);
    let (degree, tail) = choose_degree(
        &input.total_width,
        &input.lambda_abs,
        &input.target,
        max_degree,
        context,
    )?;
    let moments = cosine_moments(&input, degree, options.include_reduced_length(), context)?;
    let (q, reciprocal, weighted) = integrate_series(
        &input,
        &moments,
        degree,
        options.include_reduced_length(),
        context,
    )?;

    let tail_interval = symmetric_tail(&tail, context)?;
    let mut q = q.add(&tail_interval, context)?;
    let mut reciprocal = reciprocal.add(&tail_interval, context)?;
    let mut weighted = weighted
        .map(|value| value.add(&tail_interval, context))
        .transpose()?;

    if !input.strip.is_zero() {
        let two = Dyadic::two(context)?;
        let one = Dyadic::one(context)?;
        // The admitted disk gives 15/16 <= q² <= 17/16.  Thus q < 2,
        // 1/(1+q) < 1, and the weighted z²/q integrand is < 2.
        let q_strip = positive_interval(upper_product(&input.strip, &two, context)?)?;
        let reciprocal_strip = positive_interval(upper_product(&input.strip, &one, context)?)?;
        q = q.add(&q_strip, context)?;
        reciprocal = reciprocal.add(&reciprocal_strip, context)?;
        if let Some(value) = &mut weighted {
            let weighted_strip = positive_interval(upper_product(&input.strip, &two, context)?)?;
            *value = value.add(&weighted_strip, context)?;
        }
    }

    if !within_target(&q, &input.target, context)?
        || !within_target(&reciprocal, &input.target, context)?
        || weighted
            .as_ref()
            .map(|value| within_target(value, &input.target, context))
            .transpose()?
            .is_some_and(|within| !within)
    {
        return Err(numerical_indeterminacy());
    }

    let tails = TaylorTailBounds::from_parts(
        tail.try_clone(context)?,
        tail.try_clone(context)?,
        weighted
            .as_ref()
            .map(|_| tail.try_clone(context))
            .transpose()?,
    );
    let panels = usize::from(!input.total_width.is_zero());
    Ok(Some(TaylorIntegrals::from_parts(
        q, reciprocal, weighted, tails, panels, degree, 0,
    )))
}

/// Try moments and replay the source through the complete evaluator when the
/// optimization declines or exhausts its numerical budget.
pub(crate) fn evaluate_with_fallback(
    source: &TaylorSource,
    options: TaylorOptions,
    context: &mut PrecisionContext,
) -> Result<TaylorIntegrals> {
    match evaluate(source, options, context) {
        Ok(Some(result)) => Ok(result),
        Ok(None) => {
            context.restart_attempt();
            evaluate_taylor(source, options, context)
        },
        Err(error) if is_numerical(&error) => {
            context.restart_attempt();
            evaluate_taylor(source, options, context)
        },
        Err(error) => Err(error),
    }
}

/// Enclosed-terminal counterpart of [`evaluate_with_fallback`].
pub(crate) fn evaluate_enclosed_with_fallback(
    source: &TaylorSource,
    terminal: &Enclosure,
    options: TaylorOptions,
    context: &mut PrecisionContext,
) -> Result<TaylorIntegrals> {
    match evaluate_enclosed(source, terminal, options, context) {
        Ok(Some(result)) => Ok(result),
        Ok(None) => {
            context.restart_attempt();
            evaluate_taylor_enclosed(source, terminal, options, context)
        },
        Err(error) if is_numerical(&error) => {
            context.restart_attempt();
            evaluate_taylor_enclosed(source, terminal, options, context)
        },
        Err(error) => Err(error),
    }
}

fn is_numerical(error: &Error) -> bool {
    matches!(error.kind(), ErrorKind::NumericalIndeterminacy)
}

struct Prepared {
    surface: TaylorSurface,
    phase: Enclosure,
    total_width: Dyadic,
    strip: Dyadic,
    amplitude_squared: Enclosure,
    complementary_squared: Enclosure,
    lambda: Enclosure,
    lambda_abs: Dyadic,
    target: Dyadic,
}

fn prepare(
    source: &TaylorSource,
    lower: Dyadic,
    upper: Dyadic,
    options: TaylorOptions,
    context: &mut PrecisionContext,
) -> Result<Option<Prepared>> {
    if options.max_degree() % 2 != 0 {
        return Err(CrsError::message("graded Taylor degree must be even").into());
    }

    let target = options.target().exact(context)?;
    if target.sign() != Sign::Positive {
        return Err(CrsError::message(
            "graded Taylor integration requires a positive target width",
        )
        .into());
    }

    let phase = source.initial_phase_enclosure(context)?;
    let four = Dyadic::from_i128(4, context)?;
    let negative_four = four.try_clone(context)?.negate();
    if phase.lo() < &negative_four || phase.hi() > &four || upper < negative_four || upper > four {
        return Ok(None);
    }
    if lower > upper {
        return Err(CrsError::message("graded Taylor terminal phase interval is reversed").into());
    }

    // The moment path integrates the nonnegative terminal parameter exactly.
    // An interval crossing zero is still valid for the complete evaluator, but
    // its uncertain strip is intentionally left to that evaluator.
    let zero = Dyadic::zero();
    if lower < zero || upper < zero {
        return Ok(None);
    }
    let total_width = lower.try_clone(context)?;
    let strip = upper.sub_scratch(&total_width, context)?;

    let flattening = source.flattening().exact(context)?;
    let amplitude = source.amplitude_enclosure(context)?;
    if amplitude.lo().sign() == Sign::Negative {
        return Err(CrsError::message("graded Taylor amplitude must be nonnegative").into());
    }

    let one = Dyadic::one(context)?;
    let two = Dyadic::two(context)?;
    let d = one.sub_scratch(&flattening, context)?;
    if d.sign() != Sign::Positive {
        return Err(
            CrsError::message("graded Taylor ellipsoid requires a positive polar scale").into(),
        );
    }
    match source.surface() {
        TaylorSurface::Oblate if flattening.sign() == Sign::Negative => {
            return Err(CrsError::message(
                "oblate graded Taylor input requires nonnegative flattening",
            )
            .into());
        },
        TaylorSurface::Prolate if flattening.sign() == Sign::Positive => {
            return Err(CrsError::message(
                "prolate graded Taylor input requires nonpositive flattening",
            )
            .into());
        },
        _ => {},
    }

    let amplitude_squared = amplitude.mul(&amplitude, context)?;
    if amplitude_squared.hi() > &one {
        return Err(CrsError::message("graded Taylor amplitude must not exceed one").into());
    }
    let one_interval = Enclosure::exact(one.try_clone(context)?, context)?;
    let complementary_squared = one_interval.sub(&amplitude_squared, context)?;
    if complementary_squared.lo().sign() == Sign::Negative
        || amplitude_squared.lo().sign() == Sign::Negative
        || amplitude_squared.hi() > &one
    {
        return Err(numerical_indeterminacy());
    }

    // Both surfaces reduce to lambda = -f * (2-f).  The selected sine or
    // cosine changes only which coordinate supplies the weighted integral.
    let raw_lambda = flattening.mul_scratch(&two.sub_scratch(&flattening, context)?, context)?;
    let lambda_exact = raw_lambda.negate();
    let lambda_abs_exact = lambda_exact.abs(context)?;
    let maximum = one.scale_pow2(MAX_LAMBDA_EXPONENT, context)?;
    if lambda_abs_exact > maximum {
        return Ok(None);
    }
    let lambda = rounded_enclosure(&lambda_exact, context)?;
    let lambda_abs = maximum_abs(&lambda, context)?;

    Ok(Some(Prepared {
        surface: source.surface(),
        phase,
        total_width,
        strip,
        amplitude_squared,
        complementary_squared,
        lambda,
        lambda_abs,
        target,
    }))
}

fn choose_degree(
    total_width: &Dyadic,
    lambda_abs: &Dyadic,
    target: &Dyadic,
    max_degree: usize,
    context: &mut PrecisionContext,
) -> Result<(usize, Dyadic)> {
    if total_width.is_zero() || lambda_abs.is_zero() {
        return Ok((0, Dyadic::zero()));
    }

    let one = Dyadic::one(context)?;
    let target_budget = target.scale_pow2(-2, context)?;
    let lambda_interval = Enclosure::exact(lambda_abs.try_clone(context)?, context)?;
    let one_interval = Enclosure::exact(one.try_clone(context)?, context)?;
    let denominator = one_interval.sub(&lambda_interval, context)?;
    if denominator.lo().sign() != Sign::Positive {
        return Err(numerical_indeterminacy());
    }

    let mut power = lambda_interval.try_clone(context)?;
    for degree in 0..=max_degree {
        context.consume_fuel(1)?;
        let quotient = power.div(&denominator, context)?;
        let width = Enclosure::exact(total_width.try_clone(context)?, context)?;
        let tail = width.mul(&quotient, context)?;
        let tail = tail.hi().try_clone(context)?;
        if tail <= target_budget {
            return Ok((degree, tail));
        }
        if degree == max_degree {
            break;
        }
        power = power.mul(&lambda_interval, context)?;
    }
    Err(numerical_indeterminacy())
}

struct MomentIntegrals {
    ordinary: Vec<Enclosure>,
    weighted: Option<Vec<Enclosure>>,
}

fn cosine_moments(
    input: &Prepared,
    degree: usize,
    include_weighted: bool,
    context: &mut PrecisionContext,
) -> Result<MomentIntegrals> {
    let cosine = cosine_power_integrals(
        &input.phase,
        &input.total_width,
        degree + usize::from(include_weighted),
        context,
    )?;
    let amplitude_powers = powers(&input.amplitude_squared, degree, context)?;
    let complement_powers = powers(&input.complementary_squared, degree, context)?;
    let mut ordinary = Vec::new();
    context.try_reserve_items(&mut ordinary, degree + 1)?;
    let mut weighted = if include_weighted {
        let mut values = Vec::new();
        context.try_reserve_items(&mut values, degree + 1)?;
        Some(values)
    } else {
        None
    };

    for n in 0..=degree {
        context.consume_fuel(1)?;
        let mut value = Enclosure::exact(Dyadic::zero(), context)?;
        let mut cosine_value = Enclosure::exact(Dyadic::zero(), context)?;
        for j in 0..=n {
            context.consume_fuel(1)?;
            let coefficient =
                Enclosure::exact(Dyadic::from_u64(binomial(n, j)?, context)?, context)?;
            let coefficient = coefficient
                .mul(&complement_powers[n - j], context)?
                .mul(&amplitude_powers[j], context)?;
            value = value.add(&coefficient.mul(&cosine[j], context)?, context)?;
            if include_weighted {
                cosine_value =
                    cosine_value.add(&coefficient.mul(&cosine[j + 1], context)?, context)?;
            }
        }
        ordinary.push(value.try_clone(context)?);
        if let Some(values) = &mut weighted {
            let value = match input.surface {
                TaylorSurface::Oblate => input
                    .amplitude_squared
                    .mul(&value.sub(&cosine_value, context)?, context)?,
                TaylorSurface::Prolate => input.amplitude_squared.mul(&cosine_value, context)?,
            };
            values.push(value);
        }
    }

    Ok(MomentIntegrals { ordinary, weighted })
}

fn integrate_series(
    input: &Prepared,
    moments: &MomentIntegrals,
    degree: usize,
    include_weighted: bool,
    context: &mut PrecisionContext,
) -> Result<(Enclosure, Enclosure, Option<Enclosure>)> {
    let one = Enclosure::exact(Dyadic::one(context)?, context)?;
    let mut sqrt_coefficient = one.try_clone(context)?;
    let mut reciprocal_coefficient = one.try_clone(context)?;
    let mut lambda_power = one.try_clone(context)?;
    let mut q = Enclosure::exact(Dyadic::zero(), context)?;
    let mut reciprocal = Enclosure::exact(Dyadic::zero(), context)?;
    let mut weighted = include_weighted
        .then(|| Enclosure::exact(Dyadic::zero(), context))
        .transpose()?;

    for n in 0..=degree {
        context.consume_fuel(1)?;
        if n != 0 {
            lambda_power = lambda_power.mul(&input.lambda, context)?;
            sqrt_coefficient = sqrt_coefficient.mul(
                &rational(3_i128 - 2 * n as i128, (2 * n) as u64, context)?,
                context,
            )?;
            reciprocal_coefficient = reciprocal_coefficient.mul(
                &rational(1_i128 - 2 * n as i128, (2 * n) as u64, context)?,
                context,
            )?;
        }

        let q_coefficient = sqrt_coefficient.mul(&lambda_power, context)?;
        q = q.add(&q_coefficient.mul(&moments.ordinary[n], context)?, context)?;

        let reciprocal_coefficient_n = reciprocal_coefficient.mul(&lambda_power, context)?;
        let next_sqrt_coefficient = sqrt_coefficient.mul(
            &rational(3_i128 - 2 * (n as i128 + 1), 2 * (n as u64 + 1), context)?,
            context,
        )?;
        reciprocal = reciprocal.add(
            &next_sqrt_coefficient
                .mul(&lambda_power, context)?
                .mul(&moments.ordinary[n], context)?,
            context,
        )?;

        if include_weighted {
            if let (Some(value), Some(weighted_moments)) = (&mut weighted, &moments.weighted) {
                *value = value.add(
                    &reciprocal_coefficient_n.mul(&weighted_moments[n], context)?,
                    context,
                )?;
            }
        }
    }

    Ok((q, reciprocal, weighted))
}

fn cosine_power_integrals(
    phase: &Enclosure,
    total_width: &Dyadic,
    maximum_power: usize,
    context: &mut PrecisionContext,
) -> Result<Vec<Enclosure>> {
    let mut result = Vec::new();
    context.try_reserve_items(&mut result, maximum_power + 1)?;
    let zero = Enclosure::exact(Dyadic::zero(), context)?;
    if total_width.is_zero() {
        for _ in 0..=maximum_power {
            result.push(zero.try_clone(context)?);
        }
        return Ok(result);
    }

    let start = super::sin_cos_radians_enclosure(phase, context)?;
    let offset = sin_cos_radians(total_width, context)?;
    let end = add_angles(&start, &offset, context)?;
    let mut previous = Enclosure::exact(total_width.try_clone(context)?, context)?;
    result.push(previous.try_clone(context)?);

    let one = Enclosure::exact(Dyadic::one(context)?, context)?;
    let start_square = start.cos().mul(start.cos(), context)?;
    let end_square = end.cos().mul(end.cos(), context)?;
    let mut start_even = one.try_clone(context)?;
    let mut end_even = one;

    for index in 1..=maximum_power {
        context.consume_fuel(1)?;
        let start_odd = start_even.mul(start.cos(), context)?;
        let end_odd = end_even.mul(end.cos(), context)?;
        let start_boundary = start.sin().mul(&start_odd, context)?;
        let end_boundary = end.sin().mul(&end_odd, context)?;
        let boundary = end_boundary.sub(&start_boundary, context)?;
        let denominator = (2 * index) as u64;
        let current = boundary
            .mul(&rational(1, denominator, context)?, context)?
            .add(
                &previous.mul(
                    &rational((2 * index - 1) as i128, denominator, context)?,
                    context,
                )?,
                context,
            )?;
        result.push(current.try_clone(context)?);
        previous = current;
        start_even = start_even.mul(&start_square, context)?;
        end_even = end_even.mul(&end_square, context)?;
    }
    Ok(result)
}

fn add_angles(
    start: &super::SineCosine,
    offset: &super::SineCosine,
    context: &mut PrecisionContext,
) -> Result<super::SineCosine> {
    let sine = start
        .sin()
        .mul(offset.cos(), context)?
        .add(&start.cos().mul(offset.sin(), context)?, context)?;
    let cosine = start
        .cos()
        .mul(offset.cos(), context)?
        .sub(&start.sin().mul(offset.sin(), context)?, context)?;
    Ok(super::SineCosine::from_parts(sine, cosine))
}

fn powers(
    base: &Enclosure,
    maximum_power: usize,
    context: &mut PrecisionContext,
) -> Result<Vec<Enclosure>> {
    let mut result = Vec::new();
    context.try_reserve_items(&mut result, maximum_power + 1)?;
    result.push(Enclosure::exact(Dyadic::one(context)?, context)?);
    for _ in 0..maximum_power {
        context.consume_fuel(1)?;
        let next = result
            .last()
            .ok_or_else(numerical_indeterminacy)?
            .mul(base, context)?;
        result.push(next);
    }
    Ok(result)
}

fn binomial(n: usize, k: usize) -> Result<u64> {
    let mut value = 1_u128;
    for index in 1..=k {
        value = value
            .checked_mul(u128::from((n - k + index) as u64))
            .ok_or(ResourceError::SizeOverflow)?
            / u128::from(index as u64);
    }
    u64::try_from(value).map_err(|_| ResourceError::SizeOverflow.into())
}

fn rational(
    numerator: i128,
    denominator: u64,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    if denominator == 0 {
        return Err(CrsError::message("certified moment denominator is zero").into());
    }
    let numerator = Dyadic::from_i128(numerator, context)?;
    let denominator = Dyadic::from_u64(denominator, context)?;
    numerator.divide(&denominator, context)
}

fn rounded_enclosure(value: &Dyadic, context: &mut PrecisionContext) -> Result<Enclosure> {
    let lower = value.round_outward(true, context)?;
    let upper = value.round_outward(false, context)?;
    Enclosure::new(lower, upper).ok_or_else(numerical_indeterminacy)
}

fn maximum_abs(value: &Enclosure, context: &mut PrecisionContext) -> Result<Dyadic> {
    if value.lo().sign() == Sign::Negative {
        let negative = value.lo().try_clone(context)?.negate();
        if value.hi().sign() == Sign::Positive && value.hi() > &negative {
            return value.hi().try_clone(context);
        }
        return Ok(negative);
    }
    value.hi().try_clone(context)
}

fn symmetric_tail(tail: &Dyadic, context: &mut PrecisionContext) -> Result<Enclosure> {
    Enclosure::new(tail.try_clone(context)?.negate(), tail.try_clone(context)?)
        .ok_or_else(numerical_indeterminacy)
}

fn positive_interval(value: Dyadic) -> Result<Enclosure> {
    Enclosure::new(Dyadic::zero(), value).ok_or_else(numerical_indeterminacy)
}

fn upper_product(left: &Dyadic, right: &Dyadic, context: &mut PrecisionContext) -> Result<Dyadic> {
    left.mul_scratch(right, context)?
        .round_outward(false, context)
}

fn within_target(
    value: &Enclosure,
    target: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<bool> {
    Ok(value.hi().sub_scratch(value.lo(), context)? <= *target)
}
