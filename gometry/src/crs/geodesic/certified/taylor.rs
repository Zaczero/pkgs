//! Certified graded Taylor integration for the complete ellipsoid formulas.
//!
//! The evaluator deliberately accepts one source amplitude and derives its
//! complementary square from `1 - A²`.  There is therefore no public
//! construction of two independently rounded amplitudes that could leave the
//! auxiliary-sphere family.  Every panel is expanded in the scaled variable
//! `x = (t - t0) / h`; coefficient arithmetic is outward and the omitted
//! analytic tail is bounded by the common complex disk argument.

use super::{
    Dyadic, Enclosure, ExactBinary64, PrecisionContext, Sign, SineCosine, numerical_indeterminacy,
};
use crate::crs::CrsError;
use crate::error::Result;

const DEFAULT_MAX_DEGREE: usize = 32;
const DEFAULT_MAX_PANELS: usize = 2_048;
const DEFAULT_MAX_DEPTH: usize = 58;

/// Select the common radicand form.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaylorSurface {
    Oblate,
    Prolate,
}

/// A terminal phase bracket retained from the source calculation.
///
/// The endpoints are source binary64 values rather than rounded dyadics from
/// an earlier attempt.  Replaying this token pair at a larger precision is
/// consequently the only way to narrow input uncertainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TaylorPhaseInterval {
    lower: ExactBinary64,
    upper: ExactBinary64,
}

impl TaylorPhaseInterval {
    pub(crate) fn from_f64(lower: f64, upper: f64) -> Result<Self> {
        Ok(Self {
            lower: ExactBinary64::from_f64(lower)?,
            upper: ExactBinary64::from_f64(upper)?,
        })
    }

    pub(crate) const fn from_tokens(lower: ExactBinary64, upper: ExactBinary64) -> Self {
        Self { lower, upper }
    }

    pub(super) const fn lower(&self) -> ExactBinary64 {
        self.lower
    }

    pub(super) const fn upper(&self) -> ExactBinary64 {
        self.upper
    }
}

/// Original-source inputs for one feasible auxiliary-sphere family.
///
/// `amplitude` is `A`; its complementary square `c² = 1 - A²` is derived
/// inside the evaluator.  This is intentionally not a `(A, c)` pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TaylorSource {
    surface: TaylorSurface,
    flattening: ExactBinary64,
    amplitude: ExactBinary64,
    initial_phase: ExactBinary64,
    terminal: TaylorPhaseInterval,
}

impl TaylorSource {
    pub(crate) fn from_f64(
        surface: TaylorSurface,
        flattening: f64,
        amplitude: f64,
        initial_phase: f64,
        terminal: TaylorPhaseInterval,
    ) -> Result<Self> {
        Ok(Self {
            surface,
            flattening: ExactBinary64::from_f64(flattening)?,
            amplitude: ExactBinary64::from_f64(amplitude)?,
            initial_phase: ExactBinary64::from_f64(initial_phase)?,
            terminal,
        })
    }

    pub(crate) const fn from_tokens(
        surface: TaylorSurface,
        flattening: ExactBinary64,
        amplitude: ExactBinary64,
        initial_phase: ExactBinary64,
        terminal: TaylorPhaseInterval,
    ) -> Self {
        Self {
            surface,
            flattening,
            amplitude,
            initial_phase,
            terminal,
        }
    }

    pub(super) const fn surface(&self) -> TaylorSurface {
        self.surface
    }

    pub(super) const fn flattening(&self) -> ExactBinary64 {
        self.flattening
    }

    pub(super) const fn amplitude(&self) -> ExactBinary64 {
        self.amplitude
    }

    pub(super) const fn initial_phase(&self) -> ExactBinary64 {
        self.initial_phase
    }

    pub(super) const fn terminal(&self) -> &TaylorPhaseInterval {
        &self.terminal
    }
}

/// Explicit resource and accuracy limits for one Taylor attempt.
///
/// The panel and depth limits are availability budgets, not a decidability
/// theorem.  Exhausting any of them returns numerical indeterminacy so the
/// caller can replay the original source at a larger budget or precision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TaylorOptions {
    target: ExactBinary64,
    max_degree: usize,
    max_panels: usize,
    max_depth: usize,
    include_reduced_length: bool,
}

impl TaylorOptions {
    pub(crate) fn new(
        target: f64,
        max_degree: usize,
        max_panels: usize,
        max_depth: usize,
        include_reduced_length: bool,
    ) -> Result<Self> {
        Ok(Self {
            target: ExactBinary64::from_f64(target)?,
            max_degree,
            max_panels,
            max_depth,
            include_reduced_length,
        })
    }

    pub(crate) fn standard(target: f64, include_reduced_length: bool) -> Result<Self> {
        Self::new(
            target,
            DEFAULT_MAX_DEGREE,
            DEFAULT_MAX_PANELS,
            DEFAULT_MAX_DEPTH,
            include_reduced_length,
        )
    }

    pub(super) const fn target(&self) -> ExactBinary64 {
        self.target
    }

    pub(super) const fn max_degree(&self) -> usize {
        self.max_degree
    }

    pub(super) const fn max_panels(&self) -> usize {
        self.max_panels
    }

    pub(super) const fn include_reduced_length(&self) -> bool {
        self.include_reduced_length
    }
}

/// Certified integrals returned by one graded attempt.
#[derive(Debug)]
pub(crate) struct TaylorIntegrals {
    q: Enclosure,
    reciprocal: Enclosure,
    weighted_reciprocal: Option<Enclosure>,
    tails: TaylorTailBounds,
    panels: usize,
    degree: usize,
    depth: usize,
}

impl TaylorIntegrals {
    pub(super) fn from_parts(
        q: Enclosure,
        reciprocal: Enclosure,
        weighted_reciprocal: Option<Enclosure>,
        tails: TaylorTailBounds,
        panels: usize,
        degree: usize,
        depth: usize,
    ) -> Self {
        Self {
            q,
            reciprocal,
            weighted_reciprocal,
            tails,
            panels,
            degree,
            depth,
        }
    }

    pub(crate) const fn q(&self) -> &Enclosure {
        &self.q
    }

    /// Integral of `1 / (1 + q)`.
    pub(crate) const fn one_plus_reciprocal(&self) -> &Enclosure {
        &self.reciprocal
    }

    pub(crate) const fn weighted_reciprocal(&self) -> Option<&Enclosure> {
        self.weighted_reciprocal.as_ref()
    }

    pub(crate) const fn tails(&self) -> &TaylorTailBounds {
        &self.tails
    }

    pub(crate) const fn panels(&self) -> usize {
        self.panels
    }

    pub(crate) const fn degree(&self) -> usize {
        self.degree
    }

    pub(crate) const fn depth(&self) -> usize {
        self.depth
    }
}

/// Positive upper bounds on the analytic truncation tails.
#[derive(Debug)]
pub(crate) struct TaylorTailBounds {
    q: Dyadic,
    reciprocal: Dyadic,
    weighted_reciprocal: Option<Dyadic>,
}

impl TaylorTailBounds {
    pub(super) fn from_parts(
        q: Dyadic,
        reciprocal: Dyadic,
        weighted_reciprocal: Option<Dyadic>,
    ) -> Self {
        Self {
            q,
            reciprocal,
            weighted_reciprocal,
        }
    }

    pub(crate) const fn q(&self) -> &Dyadic {
        &self.q
    }

    pub(crate) const fn one_plus_reciprocal(&self) -> &Dyadic {
        &self.reciprocal
    }

    pub(crate) const fn weighted_reciprocal(&self) -> Option<&Dyadic> {
        self.weighted_reciprocal.as_ref()
    }
}

/// Evaluate all requested integrals over the terminal phase bracket.
pub(crate) fn evaluate(
    source: &TaylorSource,
    options: TaylorOptions,
    context: &mut PrecisionContext,
) -> Result<TaylorIntegrals> {
    let lower = source.terminal.lower.exact(context)?;
    let upper = source.terminal.upper.exact(context)?;
    evaluate_with_endpoints(source, options, context, lower, upper)
}

/// Evaluate with a certified terminal phase enclosure rebuilt by the caller
/// from the original source at the current precision attempt.
pub(crate) fn evaluate_enclosed(
    source: &TaylorSource,
    terminal: &Enclosure,
    options: TaylorOptions,
    context: &mut PrecisionContext,
) -> Result<TaylorIntegrals> {
    let lower = terminal.lo().try_clone(context)?;
    let upper = terminal.hi().try_clone(context)?;
    evaluate_with_endpoints(source, options, context, lower, upper)
}

fn evaluate_with_endpoints(
    source: &TaylorSource,
    options: TaylorOptions,
    context: &mut PrecisionContext,
    lower: Dyadic,
    upper: Dyadic,
) -> Result<TaylorIntegrals> {
    validate_options(&options)?;

    let target = source_target(&options, context)?;
    if target.sign() != Sign::Positive {
        return Err(CrsError::message(
            "graded Taylor integration requires a positive target width",
        )
        .into());
    }

    let parameters = Parameters::from_source(source, context)?;
    let phase = source.initial_phase.exact(context)?;
    validate_phase_inputs(&phase, &lower, &upper, context)?;

    let zero = Dyadic::zero();
    let fixed_end = if lower < zero { Dyadic::zero() } else { lower };
    if upper < fixed_end {
        return Err(CrsError::message("graded Taylor terminal phase interval is reversed").into());
    }
    let total_width = fixed_end.try_clone(context)?;
    let initial = super::sin_cos_radians(&phase, context)?;
    let target_for_result = target.try_clone(context)?;

    let mut evaluator = Evaluator {
        context,
        parameters,
        initial,
        target,
        total_width,
        options,
        panels: 0,
        degree: 0,
        depth: 0,
    };

    if !fixed_end.is_zero() {
        let panel_capacity = evaluator.parameters.radius.mul_scratch(
            &Dyadic::from_u64(
                u64::try_from(options.max_panels).map_err(|_| numerical_indeterminacy())?,
                evaluator.context,
            )?,
            evaluator.context,
        )?;
        if panel_capacity < fixed_end {
            return Err(numerical_indeterminacy());
        }
    }

    let mut accumulation = if fixed_end.is_zero() {
        Accumulation::zero(evaluator.context)?
    } else {
        evaluator.integrate_interval(&zero, &fixed_end, 0)?
    };

    let strip = upper.sub_scratch(&fixed_end, evaluator.context)?;
    if !strip.is_zero() {
        let q_strip = positive_interval(upper_product(
            &strip,
            &evaluator.parameters.strip_q,
            evaluator.context,
        )?)?;
        let reciprocal_strip = positive_interval(upper_product(
            &strip,
            &evaluator.parameters.strip_reciprocal,
            evaluator.context,
        )?)?;
        accumulation.q = accumulation.q.add(&q_strip, evaluator.context)?;
        accumulation.reciprocal = accumulation
            .reciprocal
            .add(&reciprocal_strip, evaluator.context)?;

        if let Some(weighted) = &mut accumulation.weighted_reciprocal {
            let bound = upper_product(
                &strip,
                &evaluator.parameters.strip_weighted_reciprocal,
                evaluator.context,
            )?;
            *weighted = weighted.add(&positive_interval(bound)?, evaluator.context)?;
        }
    }

    let weighted = if options.include_reduced_length {
        accumulation.weighted_reciprocal
    } else {
        None
    };
    let result = TaylorIntegrals {
        q: accumulation.q,
        reciprocal: accumulation.reciprocal,
        weighted_reciprocal: weighted,
        tails: TaylorTailBounds {
            q: accumulation.tail_q,
            reciprocal: accumulation.tail_reciprocal,
            weighted_reciprocal: if options.include_reduced_length {
                accumulation.tail_weighted_reciprocal
            } else {
                None
            },
        },
        panels: evaluator.panels,
        degree: evaluator.degree,
        depth: evaluator.depth,
    };
    let within_q = within_target(&result.q, &target_for_result, evaluator.context)?;
    let within_reciprocal =
        within_target(&result.reciprocal, &target_for_result, evaluator.context)?;
    let within_weighted = result
        .weighted_reciprocal
        .as_ref()
        .map(|value| within_target(value, &target_for_result, evaluator.context))
        .transpose()?
        .unwrap_or(true);
    if !within_q || !within_reciprocal || !within_weighted {
        return Err(numerical_indeterminacy());
    }
    Ok(result)
}

fn validate_options(options: &TaylorOptions) -> Result<()> {
    if options.max_degree % 2 != 0 {
        return Err(CrsError::message("graded Taylor degree must be even").into());
    }
    Ok(())
}

fn source_target(options: &TaylorOptions, context: &mut PrecisionContext) -> Result<Dyadic> {
    options.target.exact(context)
}

fn validate_phase_inputs(
    phase: &Dyadic,
    lower: &Dyadic,
    upper: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<()> {
    let four = Dyadic::from_i128(4, context)?;
    let negative_four = four.try_clone(context)?.negate();
    if phase < &negative_four || phase > &four {
        return Err(CrsError::message("graded Taylor phase must be in [-4, 4] radians").into());
    }
    if upper < &negative_four || upper > &four {
        return Err(
            CrsError::message("graded Taylor terminal phase must be in [-4, 4] radians").into(),
        );
    }
    if lower > upper {
        return Err(CrsError::message("graded Taylor terminal phase interval is reversed").into());
    }
    Ok(())
}

struct Parameters {
    surface: TaylorSurface,
    amplitude_squared: Enclosure,
    mu: Enclosure,
    k: Enclosure,
    radius: Dyadic,
    q_majorant: Dyadic,
    one_plus_reciprocal_majorant: Dyadic,
    weighted_reciprocal_majorant: Dyadic,
    strip_q: Dyadic,
    strip_reciprocal: Dyadic,
    strip_weighted_reciprocal: Dyadic,
}

impl Parameters {
    fn from_source(source: &TaylorSource, context: &mut PrecisionContext) -> Result<Self> {
        let one = Dyadic::one(context)?;
        let two = Dyadic::two(context)?;
        let flattening = source.flattening.exact(context)?;
        let amplitude = source.amplitude.exact(context)?;
        if amplitude.sign() == Sign::Negative {
            return Err(CrsError::message("graded Taylor amplitude must be nonnegative").into());
        }
        let amplitude_squared_exact = amplitude.mul_scratch(&amplitude, context)?;
        if amplitude_squared_exact > one {
            return Err(CrsError::message("graded Taylor amplitude must not exceed one").into());
        }
        let complementary_squared_exact = one.sub_scratch(&amplitude_squared_exact, context)?;
        let amplitude_squared = rounded_enclosure(&amplitude_squared_exact, context)?;

        let d_exact = match source.surface {
            TaylorSurface::Oblate => {
                if flattening.sign() == Sign::Negative {
                    return Err(CrsError::message(
                        "oblate graded Taylor input requires nonnegative flattening",
                    )
                    .into());
                }
                one.sub_scratch(&flattening, context)?
            },
            TaylorSurface::Prolate => {
                if flattening.sign() == Sign::Positive {
                    return Err(CrsError::message(
                        "prolate graded Taylor input requires nonpositive flattening",
                    )
                    .into());
                }
                one.sub_scratch(&flattening, context)?
            },
        };
        if d_exact.sign() != Sign::Positive {
            return Err(CrsError::message(
                "graded Taylor ellipsoid requires a positive polar scale",
            )
            .into());
        }
        let d = rounded_enclosure(&d_exact, context)?;

        let (mu_exact, k_exact) = match source.surface {
            TaylorSurface::Oblate => {
                let eccentricity =
                    flattening.mul_scratch(&two.sub_scratch(&flattening, context)?, context)?;
                (
                    d_exact.mul_scratch(&d_exact, context)?,
                    eccentricity.mul_scratch(&amplitude_squared_exact, context)?,
                )
            },
            TaylorSurface::Prolate => {
                let negative_flattening = flattening.try_clone(context)?.negate();
                let h = negative_flattening
                    .mul_scratch(&two.sub_scratch(&flattening, context)?, context)?;
                let complement_term = h.mul_scratch(&complementary_squared_exact, context)?;
                (
                    one.add_scratch(&complement_term, context)?,
                    h.mul_scratch(&amplitude_squared_exact, context)?,
                )
            },
        };
        let mu = rounded_enclosure(&mu_exact, context)?;
        let k = rounded_enclosure(&k_exact, context)?;

        // These are common real-path bounds for every feasible auxiliary
        // phase.  The prolate lower bound is sqrt(mu), while d is the upper
        // bound because A² + c² was retained before rounding.
        let q_lower = match source.surface {
            TaylorSurface::Oblate => d.try_clone(context)?,
            TaylorSurface::Prolate => mu.sqrt(context)?,
        };
        let q_upper = match source.surface {
            TaylorSurface::Oblate => Enclosure::exact(one.try_clone(context)?, context)?,
            TaylorSurface::Prolate => d.try_clone(context)?,
        };
        let q_lower_lo = q_lower.lo().try_clone(context)?;
        if q_lower_lo.sign() != Sign::Positive {
            return Err(numerical_indeterminacy());
        }

        let q_majorant = match source.surface {
            TaylorSurface::Oblate => one.scale_pow2(1, context)?,
            TaylorSurface::Prolate => q_upper.hi().scale_pow2(1, context)?,
        };
        // Cauchy majorants on the proved disk: 2*qU, 2/qL, and 1 for
        // q, 1/q, and 1/(1+q), respectively.  The weighted majorant is
        // 4*(2/qL) = 8/qL because |z| <= 2 on the same disk.
        let reciprocal_majorant = one
            .divide(&q_lower_lo, context)?
            .hi
            .scale_pow2(1, context)?;
        let one_plus_reciprocal_majorant = one.try_clone(context)?;
        let weighted_reciprocal_majorant = reciprocal_majorant.scale_pow2(2, context)?;

        let (strip_q, strip_reciprocal, strip_weighted_reciprocal) = match source.surface {
            TaylorSurface::Oblate => {
                let denominator = one.add_scratch(d.lo(), context)?;
                let reciprocal = one.divide(&denominator, context)?.hi;
                let weighted = one.divide(d.lo(), context)?.hi;
                (one.try_clone(context)?, reciprocal, weighted)
            },
            TaylorSurface::Prolate => (
                d.hi().try_clone(context)?,
                one.scale_pow2(-1, context)?,
                one.try_clone(context)?,
            ),
        };

        let radius = if k_exact.is_zero() {
            one.scale_pow2(-2, context)?
        } else {
            let sqrt_k = k.sqrt(context)?;
            let denominator = sqrt_k.hi().scale_pow2(4, context)?;
            let candidate = q_lower_lo.divide(&denominator, context)?.lo;
            let quarter = one.scale_pow2(-2, context)?;
            if candidate < quarter {
                candidate
            } else {
                quarter
            }
        };
        if radius.is_zero() {
            return Err(numerical_indeterminacy());
        }

        Ok(Self {
            surface: source.surface,
            amplitude_squared,
            mu,
            k,
            radius,
            q_majorant,
            one_plus_reciprocal_majorant,
            weighted_reciprocal_majorant,
            strip_q,
            strip_reciprocal,
            strip_weighted_reciprocal,
        })
    }
}

struct Evaluator<'a> {
    context: &'a mut PrecisionContext,
    parameters: Parameters,
    initial: SineCosine,
    target: Dyadic,
    total_width: Dyadic,
    options: TaylorOptions,
    panels: usize,
    degree: usize,
    depth: usize,
}

impl Evaluator<'_> {
    fn integrate_interval(
        &mut self,
        lower: &Dyadic,
        upper: &Dyadic,
        depth: usize,
    ) -> Result<Accumulation> {
        let width = upper.sub_scratch(lower, self.context)?;
        if width.is_zero() {
            return Accumulation::zero(self.context);
        }
        if width <= self.parameters.radius {
            self.context.consume_fuel(1)?;
            if self.panels >= self.options.max_panels {
                return Err(numerical_indeterminacy());
            }
            self.panels += 1;
            self.depth = self.depth.max(depth);
            let panel_target =
                target_for_width(&self.target, &width, &self.total_width, self.context)?;
            return self.integrate_panel(lower, upper, &width, &panel_target);
        }
        if depth >= self.options.max_depth {
            return Err(numerical_indeterminacy());
        }
        self.context.consume_fuel(1)?;
        let midpoint = lower
            .add_scratch(upper, self.context)?
            .scale_pow2(-1, self.context)?;
        let left = self.integrate_interval(lower, &midpoint, depth + 1)?;
        let right = self.integrate_interval(&midpoint, upper, depth + 1)?;
        left.add(right, self.context)
    }

    fn integrate_panel(
        &mut self,
        lower: &Dyadic,
        upper: &Dyadic,
        width: &Dyadic,
        panel_target: &Dyadic,
    ) -> Result<Accumulation> {
        let half_width = width.scale_pow2(-1, self.context)?;
        let center = lower
            .add_scratch(upper, self.context)?
            .scale_pow2(-1, self.context)?;
        let mut degree = 0;
        loop {
            self.context.consume_fuel(1)?;
            let panel = self.panel_at(&center, &half_width, degree)?;
            let tails = self.tail_bounds(&half_width, degree)?;
            let weighted_ok = !self.options.include_reduced_length
                || tails.2.as_ref().is_some_and(|value| value <= panel_target);
            if tails.0 <= *panel_target && tails.1 <= *panel_target && weighted_ok {
                self.degree = self.degree.max(degree);
                return panel.with_tails(tails, self.context);
            }
            if degree >= self.options.max_degree {
                return Err(numerical_indeterminacy());
            }
            degree = degree.checked_add(2).ok_or_else(numerical_indeterminacy)?;
        }
    }

    fn panel_at(
        &mut self,
        center: &Dyadic,
        half_width: &Dyadic,
        degree: usize,
    ) -> Result<PanelIntegrals> {
        let offset = center.try_clone(self.context)?;
        let offset_trig = super::sin_cos_radians(&offset, self.context)?;
        let (sine, cosine) = phase_coefficients(
            &self.initial,
            &offset_trig,
            half_width,
            degree,
            self.context,
        )?;
        let selected = match self.parameters.surface {
            TaylorSurface::Oblate => &sine,
            TaylorSurface::Prolate => &cosine,
        };
        let sine_square = square_coefficients(selected, degree, self.context)?;
        let q_coefficients = self.q_coefficients(&sine_square, degree)?;
        let reciprocal_coefficients =
            reciprocal_coefficients(&q_coefficients, degree, self.context)?;
        let one_plus_q_coefficients =
            one_plus_reciprocal_coefficients(&q_coefficients, degree, self.context)?;
        let q_integral = integrate_coefficients(&q_coefficients, half_width, degree, self.context)?;
        let reciprocal_integral =
            integrate_coefficients(&one_plus_q_coefficients, half_width, degree, self.context)?;
        let weighted_integral = if self.options.include_reduced_length {
            let mut z_square = Vec::new();
            self.context.try_reserve_items(&mut z_square, degree + 1)?;
            for coefficient in &sine_square {
                z_square.push(coefficient.mul(&self.parameters.amplitude_squared, self.context)?);
            }
            let weighted_coefficients =
                multiply_series(&z_square, &reciprocal_coefficients, degree, self.context)?;
            Some(integrate_coefficients(
                &weighted_coefficients,
                half_width,
                degree,
                self.context,
            )?)
        } else {
            None
        };
        Ok(PanelIntegrals {
            q: q_integral,
            reciprocal: reciprocal_integral,
            weighted_reciprocal: weighted_integral,
        })
    }

    fn q_coefficients(
        &mut self,
        sine_square: &[Enclosure],
        degree: usize,
    ) -> Result<Vec<Enclosure>> {
        let mut q_squared = Vec::new();
        self.context.try_reserve_items(&mut q_squared, degree + 1)?;
        for (index, coefficient) in sine_square.iter().enumerate() {
            let term = coefficient.mul(&self.parameters.k, self.context)?;
            if index == 0 {
                q_squared.push(self.parameters.mu.add(&term, self.context)?);
            } else {
                q_squared.push(term);
            }
        }

        let mut q = Vec::new();
        self.context.try_reserve_items(&mut q, degree + 1)?;
        q.push(q_squared[0].sqrt(self.context)?);
        let denominator = q[0].scale_pow2(1, self.context)?;
        for index in 1..=degree {
            let mut sum = Enclosure::exact(Dyadic::zero(), self.context)?;
            for left in 1..index {
                let term = q[left].mul(&q[index - left], self.context)?;
                sum = sum.add(&term, self.context)?;
            }
            let numerator = q_squared[index].sub(&sum, self.context)?;
            q.push(numerator.div(&denominator, self.context)?);
        }
        Ok(q)
    }

    fn tail_bounds(
        &mut self,
        half_width: &Dyadic,
        degree: usize,
    ) -> Result<(Dyadic, Dyadic, Option<Dyadic>)> {
        let q = tail_bound(
            half_width,
            &self.parameters.radius,
            &self.parameters.q_majorant,
            degree,
            self.context,
        )?;
        let reciprocal = tail_bound(
            half_width,
            &self.parameters.radius,
            &self.parameters.one_plus_reciprocal_majorant,
            degree,
            self.context,
        )?;
        let weighted = if self.options.include_reduced_length {
            Some(tail_bound(
                half_width,
                &self.parameters.radius,
                &self.parameters.weighted_reciprocal_majorant,
                degree,
                self.context,
            )?)
        } else {
            None
        };
        Ok((q, reciprocal, weighted))
    }
}

#[derive(Debug)]
struct PanelIntegrals {
    q: Enclosure,
    reciprocal: Enclosure,
    weighted_reciprocal: Option<Enclosure>,
}

impl PanelIntegrals {
    fn with_tails(
        self,
        tails: (Dyadic, Dyadic, Option<Dyadic>),
        context: &mut PrecisionContext,
    ) -> Result<Accumulation> {
        Ok(Accumulation {
            q: widen_symmetrically(self.q, tails.0.try_clone(context)?, context)?,
            reciprocal: widen_symmetrically(self.reciprocal, tails.1.try_clone(context)?, context)?,
            weighted_reciprocal: self
                .weighted_reciprocal
                .map(|value| {
                    tails
                        .2
                        .as_ref()
                        .ok_or_else(numerical_indeterminacy)
                        .and_then(|tail| {
                            widen_symmetrically(value, tail.try_clone(context)?, context)
                        })
                })
                .transpose()?,
            tail_q: tails.0,
            tail_reciprocal: tails.1,
            tail_weighted_reciprocal: tails.2,
        })
    }
}

#[derive(Debug)]
struct Accumulation {
    q: Enclosure,
    reciprocal: Enclosure,
    weighted_reciprocal: Option<Enclosure>,
    tail_q: Dyadic,
    tail_reciprocal: Dyadic,
    tail_weighted_reciprocal: Option<Dyadic>,
}

impl Accumulation {
    fn zero(context: &mut PrecisionContext) -> Result<Self> {
        let zero = Enclosure::exact(Dyadic::zero(), context)?;
        Ok(Self {
            q: zero.try_clone(context)?,
            reciprocal: zero.try_clone(context)?,
            weighted_reciprocal: Some(zero),
            tail_q: Dyadic::zero(),
            tail_reciprocal: Dyadic::zero(),
            tail_weighted_reciprocal: Some(Dyadic::zero()),
        })
    }

    fn add(mut self, right: Self, context: &mut PrecisionContext) -> Result<Self> {
        self.q = self.q.add(&right.q, context)?;
        self.reciprocal = self.reciprocal.add(&right.reciprocal, context)?;
        self.weighted_reciprocal = match (self.weighted_reciprocal, right.weighted_reciprocal) {
            (Some(left), Some(right)) => Some(left.add(&right, context)?),
            (None, None) => None,
            _ => return Err(numerical_indeterminacy()),
        };
        self.tail_q = self
            .tail_q
            .add_scratch(&right.tail_q, context)?
            .round_outward(false, context)?;
        self.tail_reciprocal = self
            .tail_reciprocal
            .add_scratch(&right.tail_reciprocal, context)?
            .round_outward(false, context)?;
        self.tail_weighted_reciprocal = match (
            self.tail_weighted_reciprocal,
            right.tail_weighted_reciprocal,
        ) {
            (Some(left), Some(right)) => Some(
                left.add_scratch(&right, context)?
                    .round_outward(false, context)?,
            ),
            (None, None) => None,
            _ => return Err(numerical_indeterminacy()),
        };
        Ok(self)
    }
}

fn phase_coefficients(
    initial: &SineCosine,
    offset: &SineCosine,
    half_width: &Dyadic,
    degree: usize,
    context: &mut PrecisionContext,
) -> Result<(Vec<Enclosure>, Vec<Enclosure>)> {
    let (sine_h, cosine_h) = phase_series(half_width, degree, context)?;
    let mut sine = Vec::new();
    let mut cosine = Vec::new();
    context.try_reserve_items(&mut sine, degree + 1)?;
    context.try_reserve_items(&mut cosine, degree + 1)?;
    for index in 0..=degree {
        let shifted_sine = offset
            .sin()
            .mul(&cosine_h[index], context)?
            .add(&offset.cos().mul(&sine_h[index], context)?, context)?;
        let shifted_cosine = offset
            .cos()
            .mul(&cosine_h[index], context)?
            .sub(&offset.sin().mul(&sine_h[index], context)?, context)?;
        // `offset` is the panel displacement from the initial phase.  The
        // initial pair is retained separately so the same panel engine serves
        // `sin(sigma+t)` and `cos(sigma+t)` without evaluating a potentially
        // out-of-range absolute phase.
        sine.push(
            initial
                .sin()
                .mul(&shifted_cosine, context)?
                .add(&initial.cos().mul(&shifted_sine, context)?, context)?,
        );
        cosine.push(
            initial
                .cos()
                .mul(&shifted_cosine, context)?
                .sub(&initial.sin().mul(&shifted_sine, context)?, context)?,
        );
    }
    Ok((sine, cosine))
}

fn phase_series(
    half_width: &Dyadic,
    degree: usize,
    context: &mut PrecisionContext,
) -> Result<(Vec<Enclosure>, Vec<Enclosure>)> {
    let mut sine = Vec::new();
    let mut cosine = Vec::new();
    context.try_reserve_items(&mut sine, degree + 1)?;
    context.try_reserve_items(&mut cosine, degree + 1)?;
    sine.push(Enclosure::exact(Dyadic::zero(), context)?);
    cosine.push(Enclosure::exact(Dyadic::one(context)?, context)?);
    let width = Enclosure::exact(half_width.try_clone(context)?, context)?;
    for index in 1..=degree {
        let denominator = Enclosure::exact(Dyadic::from_u64(index as u64, context)?, context)?;
        let sine_term = cosine[index - 1]
            .mul(&width, context)?
            .div(&denominator, context)?;
        let cosine_term = sine[index - 1]
            .mul(&width, context)?
            .div(&denominator, context)?;
        sine.push(sine_term);
        cosine.push(negate_enclosure(cosine_term, context)?);
    }
    Ok((sine, cosine))
}

fn square_coefficients(
    coefficients: &[Enclosure],
    degree: usize,
    context: &mut PrecisionContext,
) -> Result<Vec<Enclosure>> {
    multiply_series(coefficients, coefficients, degree, context)
}

fn multiply_series(
    left: &[Enclosure],
    right: &[Enclosure],
    degree: usize,
    context: &mut PrecisionContext,
) -> Result<Vec<Enclosure>> {
    let mut result = Vec::new();
    context.try_reserve_items(&mut result, degree + 1)?;
    for index in 0..=degree {
        let mut sum = Enclosure::exact(Dyadic::zero(), context)?;
        for left_index in 0..=index {
            let right_index = index - left_index;
            let term = left[left_index].mul(&right[right_index], context)?;
            sum = sum.add(&term, context)?;
        }
        result.push(sum);
    }
    Ok(result)
}

fn reciprocal_coefficients(
    q: &[Enclosure],
    degree: usize,
    context: &mut PrecisionContext,
) -> Result<Vec<Enclosure>> {
    let mut reciprocal = Vec::new();
    context.try_reserve_items(&mut reciprocal, degree + 1)?;
    let one = Enclosure::exact(Dyadic::one(context)?, context)?;
    reciprocal.push(one.div(&q[0], context)?);
    for index in 1..=degree {
        let mut sum = Enclosure::exact(Dyadic::zero(), context)?;
        for left in 1..=index {
            let term = q[left].mul(&reciprocal[index - left], context)?;
            sum = sum.add(&term, context)?;
        }
        reciprocal.push(negate_enclosure(sum, context)?.div(&q[0], context)?);
    }
    Ok(reciprocal)
}

fn one_plus_reciprocal_coefficients(
    q: &[Enclosure],
    degree: usize,
    context: &mut PrecisionContext,
) -> Result<Vec<Enclosure>> {
    let mut reciprocal = Vec::new();
    context.try_reserve_items(&mut reciprocal, degree + 1)?;
    let one = Enclosure::exact(Dyadic::one(context)?, context)?;
    let denominator = one.add(&q[0], context)?;
    reciprocal.push(one.div(&denominator, context)?);
    for index in 1..=degree {
        let mut sum = Enclosure::exact(Dyadic::zero(), context)?;
        for left in 1..=index {
            let term = q[left].mul(&reciprocal[index - left], context)?;
            sum = sum.add(&term, context)?;
        }
        reciprocal.push(negate_enclosure(sum, context)?.div(&denominator, context)?);
    }
    Ok(reciprocal)
}

fn integrate_coefficients(
    coefficients: &[Enclosure],
    half_width: &Dyadic,
    degree: usize,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let mut result = Enclosure::exact(Dyadic::zero(), context)?;
    let doubled = half_width.scale_pow2(1, context)?;
    let numerator = Enclosure::exact(doubled, context)?;
    for index in (0..=degree).step_by(2) {
        let denominator =
            Enclosure::exact(Dyadic::from_u64((index + 1) as u64, context)?, context)?;
        let factor = numerator.div(&denominator, context)?;
        result = result.add(&coefficients[index].mul(&factor, context)?, context)?;
    }
    Ok(result)
}

fn tail_bound(
    half_width: &Dyadic,
    radius: &Dyadic,
    majorant: &Dyadic,
    degree: usize,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    let ratio = half_width.divide(radius, context)?.hi.try_clone(context)?;
    let ratio_square = upper_product(&ratio, &ratio, context)?;
    let one = Dyadic::one(context)?;
    if ratio_square >= one {
        return Err(numerical_indeterminacy());
    }
    let denominator_factor = one
        .sub_scratch(&ratio_square, context)?
        .round_outward(true, context)?;
    if denominator_factor.is_zero() || denominator_factor.sign() != Sign::Positive {
        return Err(numerical_indeterminacy());
    }
    let mut power = one.try_clone(context)?;
    let power_count = degree.checked_add(2).ok_or_else(numerical_indeterminacy)?;
    for _ in 0..power_count {
        power = upper_product(&power, &ratio, context)?;
    }
    let mut numerator = upper_product(half_width, majorant, context)?;
    numerator = numerator.scale_pow2(1, context)?;
    numerator = upper_product(&numerator, &power, context)?;
    let degree_denominator = Dyadic::from_u64(
        u64::try_from(degree.checked_add(3).ok_or_else(numerical_indeterminacy)?)
            .map_err(|_| numerical_indeterminacy())?,
        context,
    )?;
    let denominator = lower_product(&degree_denominator, &denominator_factor, context)?;
    if denominator.is_zero() || denominator.sign() != Sign::Positive {
        return Err(numerical_indeterminacy());
    }
    numerator
        .divide(&denominator, context)?
        .hi
        .try_clone(context)
}

fn target_for_width(
    target: &Dyadic,
    width: &Dyadic,
    total_width: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    let numerator = target.mul_scratch(width, context)?;
    numerator
        .divide(total_width, context)?
        .lo
        .try_clone(context)
}

fn upper_product(left: &Dyadic, right: &Dyadic, context: &mut PrecisionContext) -> Result<Dyadic> {
    left.mul_scratch(right, context)?
        .round_outward(false, context)
}

fn lower_product(left: &Dyadic, right: &Dyadic, context: &mut PrecisionContext) -> Result<Dyadic> {
    left.mul_scratch(right, context)?
        .round_outward(true, context)
}

fn rounded_enclosure(value: &Dyadic, context: &mut PrecisionContext) -> Result<Enclosure> {
    let lower = value.round_outward(true, context)?;
    let upper = value.round_outward(false, context)?;
    Enclosure::new(lower, upper).ok_or_else(numerical_indeterminacy)
}

fn positive_interval(upper: Dyadic) -> Result<Enclosure> {
    Enclosure::new(Dyadic::zero(), upper).ok_or_else(numerical_indeterminacy)
}

fn widen_symmetrically(
    value: Enclosure,
    radius: Dyadic,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let negative = radius.try_clone(context)?.negate();
    let tail = Enclosure::new(negative, radius).ok_or_else(numerical_indeterminacy)?;
    value.add(&tail, context)
}

fn negate_enclosure(value: Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    Enclosure::new(
        value.hi().try_clone(context)?.negate(),
        value.lo().try_clone(context)?.negate(),
    )
    .ok_or_else(numerical_indeterminacy)
}

fn within_target(
    value: &Enclosure,
    target: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<bool> {
    let width = value.hi().sub_scratch(value.lo(), context)?;
    Ok(width <= *target)
}
