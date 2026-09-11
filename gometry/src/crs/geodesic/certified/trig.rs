use super::{Degree, Dyadic, Enclosure, PrecisionContext, Sign};
use crate::crs::CrsError;
use crate::error::Result;

#[derive(Debug)]
pub(crate) struct SineCosine {
    sin: Enclosure,
    cos: Enclosure,
}

impl SineCosine {
    pub(super) const fn from_parts(sin: Enclosure, cos: Enclosure) -> Self {
        Self { sin, cos }
    }

    pub(crate) const fn sin(&self) -> &Enclosure {
        &self.sin
    }

    pub(crate) const fn cos(&self) -> &Enclosure {
        &self.cos
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VectorSector {
    NegativeOuter,
    NegativeInner,
    PositiveInner,
    PositiveOuter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AngleBracketStatus {
    Certified,
    Undecidable,
}

#[derive(Debug)]
pub(crate) struct AngleBracket {
    enclosure: Enclosure,
    status: AngleBracketStatus,
}

impl AngleBracket {
    pub(crate) const fn enclosure(&self) -> &Enclosure {
        &self.enclosure
    }

    pub(crate) const fn status(&self) -> AngleBracketStatus {
        self.status
    }

    pub(crate) const fn is_certified(&self) -> bool {
        matches!(self.status, AngleBracketStatus::Certified)
    }
}

/// Compute pi from `16 atan(1/5) - 4 atan(1/239)`.  Each atan series is the
/// alternating rational series; its next term supplies the explicit symmetric
/// tail enclosure used for termination.
pub(crate) fn pi_bracket(context: &mut PrecisionContext) -> Result<Enclosure> {
    let first = atan_inverse_integer(5, context)?.scale_pow2(4, context)?;
    let second = atan_inverse_integer(239, context)?.scale_pow2(2, context)?;
    first.sub(&second, context)
}

fn atan_inverse_integer(denominator: u64, context: &mut PrecisionContext) -> Result<Enclosure> {
    let one = Dyadic::one(context)?;
    let denominator = Dyadic::from_u64(denominator, context)?;
    let denominator_square = denominator.mul_scratch(&denominator, context)?;
    let target = target_tolerance(context)?;
    let mut power = denominator.try_clone(context)?;
    let mut sum = Enclosure::exact(Dyadic::zero(), context)?;
    let mut term_index = 0_u64;

    loop {
        context.consume_fuel(1)?;
        let odd = odd_factor(term_index, context)?;
        let term_denominator = power.mul_scratch(&odd, context)?;
        let term = one.divide(&term_denominator, context)?;
        sum = if term_index % 2 == 0 {
            sum.add(&term, context)?
        } else {
            sum.sub(&term, context)?
        };

        let next_index = term_index
            .checked_add(1)
            .ok_or_else(super::numerical_indeterminacy_error)?;
        power = power.mul_scratch(&denominator_square, context)?;
        let next_odd = odd_factor(next_index, context)?;
        let next_denominator = power.mul_scratch(&next_odd, context)?;
        let next_term = one.divide(&next_denominator, context)?;
        if next_term.hi() <= &target {
            return widen_by_symmetric_tail(&sum, next_term.hi(), context);
        }
        term_index = next_index;
    }
}

fn odd_factor(index: u64, context: &mut PrecisionContext) -> Result<Dyadic> {
    let doubled = index
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(super::numerical_indeterminacy_error)?;
    Dyadic::from_u64(doubled, context)
}

fn target_tolerance(context: &mut PrecisionContext) -> Result<Dyadic> {
    let precision = i64::from(context.active_precision_bits());
    Dyadic::one(context)?.scale_pow2(-precision, context)
}

fn widen_by_symmetric_tail(
    value: &Enclosure,
    radius: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let positive = radius.try_clone(context)?;
    let negative = positive.try_clone(context)?.negate();
    let tail =
        Enclosure::new(negative, positive).ok_or_else(super::numerical_indeterminacy_error)?;
    value.add(&tail, context)
}

/// Sine and cosine for a bounded exact radian argument.  The fixed exact bound
/// four contains the certified outer enclosure of every admitted `[-pi, pi]`
/// argument; no quadrant boundary is used as a branch or fallback.
pub(crate) fn sin_cos_radians(
    value: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<SineCosine> {
    if value.is_zero() {
        return exact_sine_cosine(Dyadic::zero(), Dyadic::one(context)?, context);
    }
    let value = Enclosure::exact(value.try_clone(context)?, context)?;
    sin_cos_radians_enclosure(&value, context)
}

pub(crate) fn sin_cos_radians_enclosure(
    value: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<SineCosine> {
    let bound = Dyadic::from_i128(4, context)?;
    let negative_bound = bound.try_clone(context)?.negate();
    if value.lo() < &negative_bound || value.hi() > &bound {
        return Err(CrsError::message(
            "certified radian sine and cosine require an argument in [-4, 4]",
        ));
    }
    sine_cosine_bounded(value, context)
}

/// Evaluate signed Taylor polynomials directly on the entire bounded input.
/// For a sine polynomial through degree `2k + 1`, the degree `2k + 2`
/// coefficient is zero, so the real Lagrange remainder is bounded by
/// `r^(2k + 3) / (2k + 3)!`.  The cosine remainder is bounded analogously by
/// `r^(2k + 2) / (2k + 2)!`.  These derivative bounds do not rely on term
/// monotonicity or on the sign of the input.
fn sine_cosine_bounded(value: &Enclosure, context: &mut PrecisionContext) -> Result<SineCosine> {
    let one = Dyadic::one(context)?;
    let radius = maximum_abs(value, context)?;
    let bound = Dyadic::from_i128(4, context)?;
    if radius > bound {
        return Err(super::numerical_indeterminacy_error());
    }
    let target = target_tolerance(context)?;
    let square = value.mul(value, context)?;
    let radius_square = upper_product(&radius, &radius, context)?;

    let mut sine_sum = Enclosure::exact(Dyadic::zero(), context)?;
    let mut cosine_sum = Enclosure::exact(Dyadic::zero(), context)?;
    let mut sine_power = value.try_clone(context)?;
    let mut cosine_power = Enclosure::exact(one.try_clone(context)?, context)?;
    let mut radius_sine_power = radius.try_clone(context)?;
    let mut radius_cosine_power = one.try_clone(context)?;
    let mut factorial_even = one.try_clone(context)?;
    let mut factorial_odd = one;
    let mut term_index = 0_u64;

    loop {
        context.consume_fuel(1)?;
        let even_denominator = Enclosure::exact(factorial_even.try_clone(context)?, context)?;
        let odd_denominator = Enclosure::exact(factorial_odd.try_clone(context)?, context)?;
        let sine_term = sine_power.div(&odd_denominator, context)?;
        let cosine_term = cosine_power.div(&even_denominator, context)?;
        sine_sum = if term_index % 2 == 0 {
            sine_sum.add(&sine_term, context)?
        } else {
            sine_sum.sub(&sine_term, context)?
        };
        cosine_sum = if term_index % 2 == 0 {
            cosine_sum.add(&cosine_term, context)?
        } else {
            cosine_sum.sub(&cosine_term, context)?
        };

        let base = term_index
            .checked_mul(2)
            .ok_or_else(super::numerical_indeterminacy_error)?;
        let first = Dyadic::from_u64(
            base.checked_add(1)
                .ok_or_else(super::numerical_indeterminacy_error)?,
            context,
        )?;
        let second = Dyadic::from_u64(
            base.checked_add(2)
                .ok_or_else(super::numerical_indeterminacy_error)?,
            context,
        )?;
        let third = Dyadic::from_u64(
            base.checked_add(3)
                .ok_or_else(super::numerical_indeterminacy_error)?,
            context,
        )?;
        factorial_even = factorial_even
            .mul_scratch(&first, context)?
            .mul_scratch(&second, context)?;
        factorial_odd = factorial_odd
            .mul_scratch(&second, context)?
            .mul_scratch(&third, context)?;
        sine_power = sine_power.mul(&square, context)?;
        cosine_power = cosine_power.mul(&square, context)?;
        radius_sine_power = upper_product(&radius_sine_power, &radius_square, context)?;
        radius_cosine_power = upper_product(&radius_cosine_power, &radius_square, context)?;

        let sine_remainder = upper_quotient(&radius_sine_power, &factorial_odd, context)?;
        let cosine_remainder = upper_quotient(&radius_cosine_power, &factorial_even, context)?;
        if sine_remainder <= target && cosine_remainder <= target {
            return Ok(SineCosine {
                sin: widen_by_symmetric_tail(&sine_sum, &sine_remainder, context)?,
                cos: widen_by_symmetric_tail(&cosine_sum, &cosine_remainder, context)?,
            });
        }
        term_index = term_index
            .checked_add(1)
            .ok_or_else(super::numerical_indeterminacy_error)?;
    }
}

fn upper_quotient(
    numerator: &Dyadic,
    denominator: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    let quotient = numerator.divide(denominator, context)?;
    quotient.hi().try_clone(context)
}

fn upper_product(left: &Dyadic, right: &Dyadic, context: &mut PrecisionContext) -> Result<Dyadic> {
    let product = left.mul_scratch(right, context)?;
    product.round_outward(false, context)
}

fn maximum_abs(value: &Enclosure, context: &mut PrecisionContext) -> Result<Dyadic> {
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
    if lower >= upper { Ok(lower) } else { Ok(upper) }
}

fn exact_sine_cosine(
    sine: Dyadic,
    cosine: Dyadic,
    context: &mut PrecisionContext,
) -> Result<SineCosine> {
    Ok(SineCosine {
        sin: Enclosure::exact(sine, context)?,
        cos: Enclosure::exact(cosine, context)?,
    })
}

fn negate_enclosure(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    Ok(Enclosure::new(
        value.hi().try_clone(context)?.negate(),
        value.lo().try_clone(context)?.negate(),
    )
    .ok_or_else(super::numerical_indeterminacy_error)?)
}

/// Degree ingress performs exact principal reduction before multiplying by pi.
/// Cardinal degrees bypass the series and publish exact zero/one values.
pub(crate) fn sin_cos_degrees(value: Degree, context: &mut PrecisionContext) -> Result<SineCosine> {
    let reduced = super::principal_longitude(&value.exact(context)?, context)?;
    let zero = Dyadic::zero();
    let one = Dyadic::one(context)?;
    let negative_one = one.try_clone(context)?.negate();
    let ninety = Dyadic::from_i128(90, context)?;
    let negative_ninety = ninety.try_clone(context)?.negate();
    let negative_180 = Dyadic::from_i128(-180, context)?;
    if reduced == zero {
        return exact_sine_cosine(zero, one, context);
    }
    if reduced == ninety {
        return exact_sine_cosine(one, zero, context);
    }
    if reduced == negative_ninety {
        return exact_sine_cosine(negative_one, zero, context);
    }
    if reduced == negative_180 {
        return exact_sine_cosine(zero, negative_one, context);
    }

    let negative = reduced.sign() == Sign::Negative;
    let mut magnitude = reduced.try_clone(context)?;
    if negative {
        magnitude = magnitude.negate();
    }
    let mut reflected = false;
    if magnitude > ninety {
        let one_80 = Dyadic::from_i128(180, context)?;
        magnitude = one_80.sub(&magnitude, context)?;
        reflected = true;
    }
    let pi = pi_bracket(context)?;
    let degrees = Enclosure::exact(Dyadic::from_i128(180, context)?, context)?;
    let radians = Enclosure::exact(magnitude, context)?
        .mul(&pi, context)?
        .div(&degrees, context)?;
    let mut result = sin_cos_radians_enclosure(&radians, context)?;
    if negative {
        result.sin = negate_enclosure(&result.sin, context)?;
    }
    if reflected {
        result.cos = negate_enclosure(&result.cos, context)?;
    }
    Ok(result)
}

/// Bracket the principal vector angle in a caller-provided quadrant.  Two
/// separated dyadic quarter probes keep a central root from making a single
/// midpoint comparison the permanent stopping point.
pub(crate) fn vector_angle_bracket(
    x: &Dyadic,
    y: &Dyadic,
    sector: VectorSector,
    context: &mut PrecisionContext,
) -> Result<AngleBracket> {
    if x.is_zero() && y.is_zero() {
        return Err(CrsError::message(
            "certified vector angle requires a nonzero vector",
        ));
    }
    validate_vector_sector(x, y, sector)?;
    if y.is_zero() && x.sign() == Sign::Positive {
        return Ok(AngleBracket {
            enclosure: Enclosure::exact(Dyadic::zero(), context)?,
            status: AngleBracketStatus::Certified,
        });
    }
    let pi = pi_bracket(context)?;
    let (mut lower, mut upper) = sector_bounds(&pi, sector, context)?;
    let x_value = Enclosure::exact(x.try_clone(context)?, context)?;
    let y_value = Enclosure::exact(y.try_clone(context)?, context)?;
    if let Some(axis) = axis_angle(x, y, sector, &pi, context)? {
        return Ok(axis);
    }
    let target = target_tolerance(context)?;

    loop {
        let width = upper.sub_scratch(&lower, context)?;
        if width <= target {
            return Ok(AngleBracket {
                enclosure: Enclosure::new(lower, upper)
                    .ok_or_else(super::numerical_indeterminacy_error)?,
                status: AngleBracketStatus::Certified,
            });
        }
        context.consume_fuel(1)?;
        let span = upper.sub_scratch(&lower, context)?;
        let quarter = span.scale_pow2(-2, context)?;
        let first = lower.add_scratch(&quarter, context)?;
        let second = upper.sub_scratch(&quarter, context)?;
        let mut proposed_lower = lower.try_clone(context)?;
        let mut proposed_upper = upper.try_clone(context)?;
        let mut changed = false;

        for probe in [first, second] {
            let candidate_angle = Enclosure::exact(probe.try_clone(context)?, context)?;
            let candidate = sin_cos_radians_enclosure(&candidate_angle, context)?;
            let cross = x_value
                .mul(candidate.sin(), context)?
                .sub(&y_value.mul(candidate.cos(), context)?, context)?;
            let dot = x_value
                .mul(candidate.cos(), context)?
                .add(&y_value.mul(candidate.sin(), context)?, context)?;
            match (cross.sign(), dot.sign()) {
                (Some(Sign::Positive), Some(Sign::Positive)) if probe < proposed_upper => {
                    proposed_upper = probe;
                    changed = true;
                },
                (Some(Sign::Negative), Some(Sign::Positive)) if probe > proposed_lower => {
                    proposed_lower = probe;
                    changed = true;
                },
                (Some(Sign::Zero), Some(Sign::Positive)) => {
                    return Ok(AngleBracket {
                        enclosure: Enclosure::exact(probe, context)?,
                        status: AngleBracketStatus::Certified,
                    });
                },
                _ => {},
            }
        }
        if !changed || proposed_lower >= proposed_upper {
            return Ok(AngleBracket {
                enclosure: Enclosure::new(lower, upper)
                    .ok_or_else(super::numerical_indeterminacy_error)?,
                status: AngleBracketStatus::Undecidable,
            });
        }
        lower = proposed_lower;
        upper = proposed_upper;
    }
}

fn validate_vector_sector(x: &Dyadic, y: &Dyadic, sector: VectorSector) -> Result<()> {
    let valid = match sector {
        VectorSector::NegativeOuter => x.sign() != Sign::Positive && y.sign() != Sign::Positive,
        VectorSector::NegativeInner => x.sign() != Sign::Negative && y.sign() != Sign::Positive,
        VectorSector::PositiveInner => x.sign() != Sign::Negative && y.sign() != Sign::Negative,
        VectorSector::PositiveOuter => x.sign() != Sign::Positive && y.sign() != Sign::Negative,
    };
    if valid {
        Ok(())
    } else {
        Err(CrsError::message(
            "vector components do not satisfy the supplied certified angle sector",
        ))
    }
}

fn sector_bounds(
    pi: &Enclosure,
    sector: VectorSector,
    context: &mut PrecisionContext,
) -> Result<(Dyadic, Dyadic)> {
    let half = pi.scale_pow2(-1, context)?;
    let negative_half = negate_enclosure(&half, context)?;
    let negative_pi = negate_enclosure(pi, context)?;
    match sector {
        VectorSector::NegativeOuter => Ok((
            negative_pi.lo().try_clone(context)?,
            negative_half.hi().try_clone(context)?,
        )),
        VectorSector::NegativeInner => Ok((negative_half.lo().try_clone(context)?, Dyadic::zero())),
        VectorSector::PositiveInner => Ok((Dyadic::zero(), half.hi().try_clone(context)?)),
        VectorSector::PositiveOuter => {
            Ok((half.lo().try_clone(context)?, pi.hi().try_clone(context)?))
        },
    }
}

fn axis_angle(
    x: &Dyadic,
    y: &Dyadic,
    sector: VectorSector,
    pi: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<Option<AngleBracket>> {
    let angle = if y.is_zero() && x.sign() == Sign::Positive {
        Some(Enclosure::exact(Dyadic::zero(), context)?)
    } else if y.is_zero() && x.sign() == Sign::Negative {
        match sector {
            VectorSector::PositiveOuter => Some(pi.try_clone(context)?),
            VectorSector::NegativeOuter => Some(negate_enclosure(pi, context)?),
            VectorSector::NegativeInner | VectorSector::PositiveInner => None,
        }
    } else if x.is_zero() && y.sign() == Sign::Positive {
        Some(pi.scale_pow2(-1, context)?)
    } else if x.is_zero() && y.sign() == Sign::Negative {
        Some(negate_enclosure(&pi.scale_pow2(-1, context)?, context)?)
    } else {
        None
    };
    Ok(angle.map(|enclosure| AngleBracket {
        enclosure,
        status: AngleBracketStatus::Certified,
    }))
}

#[cfg(test)]
mod tests {
    use super::super::PrecisionPolicy;
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::test_support::fail_allocations_after;

    fn context_at(bits: u32) -> PrecisionContext {
        PrecisionContext::new(PrecisionPolicy::new(
            bits,
            bits,
            128_000_000,
            256 * 1024 * 1024,
        ))
        .expect("test policy")
    }

    fn assert_contains(value: &Enclosure, expected: &Dyadic) {
        assert!(value.lo() <= expected);
        assert!(value.hi() >= expected);
    }

    fn width(value: &Enclosure, context: &mut PrecisionContext) -> Dyadic {
        value
            .hi()
            .sub_scratch(value.lo(), context)
            .expect("enclosure width")
    }

    #[test]
    fn machin_pi_bracket_contains_independent_rational_bounds() {
        let mut context = context_at(128);
        let pi = pi_bracket(&mut context).expect("Machin pi");
        let lower_numerator = Dyadic::from_i128(333, &mut context).expect("lower numerator");
        let lower_denominator = Dyadic::from_i128(106, &mut context).expect("lower denominator");
        let upper_numerator = Dyadic::from_i128(355, &mut context).expect("upper numerator");
        let upper_denominator = Dyadic::from_i128(113, &mut context).expect("upper denominator");
        let lower_product = pi
            .lo()
            .mul_scratch(&lower_denominator, &mut context)
            .expect("lower rational product");
        let upper_product = pi
            .hi()
            .mul_scratch(&upper_denominator, &mut context)
            .expect("upper rational product");
        assert!(lower_product > lower_numerator);
        assert!(upper_product < upper_numerator);
        assert!(pi.lo() < pi.hi());
    }

    #[test]
    fn degree_sincos_publishes_cardinals_and_strict_minimum_subnormal_sine() {
        let mut context = context_at(128);
        let zero = sin_cos_degrees(Degree::from_f64(0.0).expect("zero"), &mut context)
            .expect("zero sincos");
        assert_eq!(zero.sin().lo().sign(), Sign::Zero);
        assert_eq!(zero.sin().hi().sign(), Sign::Zero);
        assert_eq!(zero.cos().lo().significand_u128(), Some(1));
        let quarter = sin_cos_degrees(Degree::from_f64(90.0).expect("quarter"), &mut context)
            .expect("quarter sincos");
        assert_eq!(quarter.sin().lo().significand_u128(), Some(1));
        assert_eq!(quarter.cos().lo().sign(), Sign::Zero);
        let half = sin_cos_degrees(Degree::from_f64(180.0).expect("half"), &mut context)
            .expect("half sincos");
        assert_eq!(half.sin().lo().sign(), Sign::Zero);
        assert_eq!(half.cos().sign(), Some(Sign::Negative));
        let second = sin_cos_degrees(Degree::from_f64(120.0).expect("second"), &mut context)
            .expect("second sincos");
        assert_eq!(second.sin().sign(), Some(Sign::Positive));
        assert_eq!(second.cos().sign(), Some(Sign::Negative));
        let third = sin_cos_degrees(Degree::from_f64(-120.0).expect("third"), &mut context)
            .expect("third sincos");
        assert_eq!(third.sin().sign(), Some(Sign::Negative));
        assert_eq!(third.cos().sign(), Some(Sign::Negative));

        let tiny = sin_cos_degrees(
            Degree::from_bits(1).expect("minimum subnormal degree"),
            &mut context,
        )
        .expect("tiny degree sincos");
        assert!(tiny.sin().lo() > &Dyadic::zero());
        assert!(tiny.cos().lo() > &Dyadic::zero());
    }

    #[test]
    fn radian_sincos_has_independent_one_sided_rational_oracle() {
        let mut context = context_at(128);
        let one = Dyadic::from_i128(1, &mut context).expect("one");
        let result = sin_cos_radians(&one, &mut context).expect("radian sincos");
        let sin_lower = Dyadic::from_i128(21, &mut context).expect("sine lower numerator");
        let sin_lower_denominator =
            Dyadic::from_i128(25, &mut context).expect("sine lower denominator");
        let sin_upper = Dyadic::from_i128(17, &mut context).expect("sine upper numerator");
        let sin_upper_denominator =
            Dyadic::from_i128(20, &mut context).expect("sine upper denominator");
        let cos_lower = Dyadic::from_i128(27, &mut context).expect("cosine lower numerator");
        let cos_lower_denominator =
            Dyadic::from_i128(50, &mut context).expect("cosine lower denominator");
        let cos_upper = Dyadic::from_i128(11, &mut context).expect("cosine upper numerator");
        let cos_upper_denominator =
            Dyadic::from_i128(20, &mut context).expect("cosine upper denominator");
        assert!(
            result
                .sin()
                .lo()
                .mul_scratch(&sin_lower_denominator, &mut context)
                .expect("sine lower product")
                > sin_lower
        );
        assert!(
            result
                .sin()
                .hi()
                .mul_scratch(&sin_upper_denominator, &mut context)
                .expect("sine upper product")
                < sin_upper
        );
        assert!(
            result
                .cos()
                .lo()
                .mul_scratch(&cos_lower_denominator, &mut context)
                .expect("cosine lower product")
                > cos_lower
        );
        assert!(
            result
                .cos()
                .hi()
                .mul_scratch(&cos_upper_denominator, &mut context)
                .expect("cosine upper product")
                < cos_upper
        );
        for precision in [64, 256] {
            let mut context = context_at(precision);
            let one = Dyadic::from_i128(1, &mut context).expect("one at selected precision");
            let result = sin_cos_radians(&one, &mut context).expect("selected precision sincos");
            assert!(result.sin().lo() > &Dyadic::zero());
            assert!(result.cos().lo() > &Dyadic::zero());
            assert!(result.sin().hi() < &Dyadic::from_i128(1, &mut context).expect("one bound"));
            assert!(result.cos().hi() < &Dyadic::from_i128(1, &mut context).expect("one bound"));
        }
    }

    #[test]
    fn bounded_radian_crossings_narrow_at_64_128_and_256_bits() {
        let mut widths_45 = Vec::new();
        let mut widths_half_pi = Vec::new();
        let mut widths_zero = Vec::new();
        for precision in [64, 128, 256] {
            let mut context = context_at(precision);
            let result = sin_cos_degrees(Degree::from_f64(45.0).expect("45 degrees"), &mut context)
                .expect("45-degree sincos");
            widths_45.push(width(result.sin(), &mut context));
            let pi = pi_bracket(&mut context).expect("pi");
            let half = pi.scale_pow2(-1, &mut context).expect("half pi");
            let half_result = sin_cos_radians_enclosure(&half, &mut context).expect("half sincos");
            widths_half_pi.push(width(half_result.cos(), &mut context));
            let epsilon = Dyadic::one(&mut context)
                .expect("one")
                .scale_pow2(-i64::from(precision), &mut context)
                .expect("zero radius");
            let zero_crossing = Enclosure::new(
                epsilon
                    .try_clone(&mut context)
                    .expect("zero lower")
                    .negate(),
                epsilon,
            )
            .expect("zero crossing");
            let zero_result = sin_cos_radians_enclosure(&zero_crossing, &mut context)
                .expect("zero-crossing sincos");
            widths_zero.push(width(zero_result.sin(), &mut context));
            assert!(zero_result.sin().lo() <= &Dyadic::zero());
            assert!(zero_result.sin().hi() >= &Dyadic::zero());
        }
        assert!(widths_45[0] > widths_45[1] && widths_45[1] > widths_45[2]);
        assert!(widths_half_pi[0] > widths_half_pi[1] && widths_half_pi[1] > widths_half_pi[2]);
        assert!(widths_zero[0] > widths_zero[1] && widths_zero[1] > widths_zero[2]);
    }

    #[test]
    fn vector_angle_brackets_narrow_diagonal_and_both_quadrants() {
        let mut context = context_at(64);
        let one = Dyadic::one(&mut context).expect("one");
        let diagonal = vector_angle_bracket(&one, &one, VectorSector::PositiveInner, &mut context)
            .expect("diagonal angle");
        let pi = pi_bracket(&mut context).expect("pi");
        let half = pi.scale_pow2(-1, &mut context).expect("half pi");
        let initial_width = half.hi().try_clone(&mut context).expect("initial upper");
        assert!(diagonal.enclosure().hi() < &initial_width);
        assert_eq!(diagonal.status(), AngleBracketStatus::Undecidable);
        let mut replay_context = context_at(128);
        let replay_one = Dyadic::one(&mut replay_context).expect("replay one");
        let replay = vector_angle_bracket(
            &replay_one,
            &replay_one,
            VectorSector::PositiveInner,
            &mut replay_context,
        )
        .expect("replayed diagonal angle");
        assert!(
            width(diagonal.enclosure(), &mut context)
                > width(replay.enclosure(), &mut replay_context)
        );
        let two = Dyadic::from_i128(2, &mut context).expect("two");
        let first = vector_angle_bracket(&one, &two, VectorSector::PositiveInner, &mut context)
            .expect("first quadrant angle");
        let second = vector_angle_bracket(
            &one.negate(),
            &two,
            VectorSector::PositiveOuter,
            &mut context,
        )
        .expect("second quadrant angle");
        assert!(first.enclosure().lo() < first.enclosure().hi());
        assert!(second.enclosure().lo() < second.enclosure().hi());
        assert!(first.enclosure().hi() < half.hi());
        assert!(second.enclosure().lo() > half.lo());
    }

    #[test]
    fn low_budgets_and_allocation_failures_keep_distinct_errors() {
        let mut low_fuel =
            PrecisionContext::new(PrecisionPolicy::new(64, 64, 0, 1024)).expect("low fuel policy");
        let error = pi_bracket(&mut low_fuel).expect_err("fuel exhaustion");
        assert!(matches!(error.kind(), ErrorKind::NumericalIndeterminacy));
        let mut low_workspace = PrecisionContext::new(PrecisionPolicy::new(64, 64, 100_000, 0))
            .expect("low workspace policy");
        let error = pi_bracket(&mut low_workspace).expect_err("workspace exhaustion");
        assert!(matches!(error.kind(), ErrorKind::NumericalIndeterminacy));
        let allocation_error = fail_allocations_after(0, || {
            let mut context = context_at(128);
            pi_bracket(&mut context)
        })
        .expect_err("allocator rejection");
        assert!(matches!(
            allocation_error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        let mut context = context_at(128);
        let value = Dyadic::from_i128(1, &mut context).expect("one");
        let mut zero_fuel =
            PrecisionContext::new(PrecisionPolicy::new(64, 64, 0, 1024)).expect("zero fuel retry");
        let error = sin_cos_radians(&value, &mut zero_fuel).expect_err("trigonometric fuel");
        assert!(matches!(error.kind(), ErrorKind::NumericalIndeterminacy));
    }

    #[test]
    fn composition_replays_ingress_at_64_128_and_256_bits() {
        let mut product_widths = Vec::new();
        let mut square_widths = Vec::new();
        for precision in [64, 128, 256] {
            let mut context = context_at(precision);
            let one_source = super::super::ExactBinary64::from_f64(1.0).expect("one source");
            let three_source = super::super::ExactBinary64::from_f64(3.0).expect("three source");
            let one = one_source.exact(&mut context).expect("one");
            let three = three_source.exact(&mut context).expect("three");
            let third = one.divide(&three, &mut context).expect("third");
            let three_interval = Enclosure::exact(three, &mut context).expect("three interval");
            let product = third
                .mul(&three_interval, &mut context)
                .expect("third product");
            assert_contains(&product, &one);
            product_widths.push(width(&product, &mut context));
            let two = Dyadic::from_i128(2, &mut context).expect("two");
            let root = two.sqrt(&mut context).expect("root");
            let square = root.mul(&root, &mut context).expect("root square");
            assert_contains(&square, &two);
            square_widths.push(width(&square, &mut context));
            let mut chain = third;
            for _ in 0..16 {
                chain = chain.add(&three_interval, &mut context).expect("chain add");
                chain = chain.mul(&root, &mut context).expect("chain multiply");
                assert!(chain.lo().significand_bits() <= precision as usize);
                assert!(chain.hi().significand_bits() <= precision as usize);
            }
        }
        assert!(product_widths[0] > product_widths[1] && product_widths[1] > product_widths[2]);
        assert!(square_widths[0] > square_widths[1] && square_widths[1] > square_widths[2]);
    }
}
