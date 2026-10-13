//! Certified square-root intervals and their test instrumentation.

#[cfg(test)]
use super::SqrtStats;
use super::span_bounds::square_f64;
use super::{ExactDyadic, FloatSqrtBracket, Ordering, Rational, Result, SqrtInterval};

#[cfg(test)]
pub(super) fn dyadic_as_f64(value: &ExactDyadic) -> Result<f64> {
    Rational::from_dyadic(value.clone()).round()
}

#[cfg(test)]
thread_local! {
    static SQRT_STATS: std::cell::Cell<SqrtStats> =
        const { std::cell::Cell::new(SqrtStats {
            fast_admissions: 0,
            fallbacks: 0,
        }) };
    static FORCE_EXACT_SQRT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn exact_sqrt_path_forced() -> bool {
    FORCE_EXACT_SQRT.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(super) fn record_sqrt_fast_admission() {
    SQRT_STATS.with(|stats| {
        let mut value = stats.get();
        value.fast_admissions = value.fast_admissions.wrapping_add(1);
        stats.set(value);
    });
}

#[cfg(test)]
pub(super) fn record_sqrt_fallback() {
    SQRT_STATS.with(|stats| {
        let mut value = stats.get();
        value.fallbacks = value.fallbacks.wrapping_add(1);
        stats.set(value);
    });
}

#[cfg(test)]
pub(super) fn reset_sqrt_stats() {
    SQRT_STATS.with(|stats| stats.set(SqrtStats::default()));
}

#[cfg(test)]
pub(super) fn sqrt_stats() -> SqrtStats {
    SQRT_STATS.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(super) struct ExactSqrtPathGuard(bool);

#[cfg(test)]
impl Drop for ExactSqrtPathGuard {
    fn drop(&mut self) {
        FORCE_EXACT_SQRT.with(|forced| forced.set(self.0));
    }
}

#[cfg(test)]
pub(super) fn force_exact_sqrt_path(force: bool) -> ExactSqrtPathGuard {
    ExactSqrtPathGuard(FORCE_EXACT_SQRT.with(|forced| forced.replace(force)))
}

pub(super) const OVERFLOW_NEWTON_STEPS: usize = 4;

pub(super) fn overflow_newton_step(squared: &Rational, upper: &Rational) -> Result<Rational> {
    let reciprocal = squared.try_quotient(upper)?;
    upper
        .try_add(&reciprocal)?
        .try_quotient(&Rational::from_f64(2.0))
}

impl FloatSqrtBracket {
    pub(super) fn into_interval(
        self,
        squared: &Rational,
        error: Option<&Rational>,
    ) -> Result<SqrtInterval> {
        match self.upper {
            None => SqrtInterval::overflow(squared, error),
            Some(upper) if self.lower.to_bits() == upper.to_bits() => {
                Ok(SqrtInterval::singleton(self.lower))
            },
            Some(upper) => Ok(SqrtInterval::adjacent(self.lower, upper)),
        }
    }

    pub(super) fn nearest_candidate(self, squared: &Rational) -> Result<Option<f64>> {
        let Some(upper) = self.upper else {
            debug_assert_eq!(self.lower.to_bits(), f64::MAX.to_bits());
            return Ok(Some(self.lower));
        };
        if self.lower.to_bits() == upper.to_bits() {
            return Ok(Some(self.lower));
        }
        let midpoint = ExactDyadic::from_f64(self.lower)
            .try_add(ExactDyadic::from_f64(upper))?
            .try_product(&ExactDyadic::from_f64(0.5))?;
        let midpoint_squared = Rational::from_dyadic(midpoint.try_product(&midpoint)?);
        Ok(Some(match squared.try_cmp(&midpoint_squared)? {
            Ordering::Less => self.lower,
            Ordering::Equal if self.lower.to_bits() & 1 == 0 => self.lower,
            Ordering::Greater | Ordering::Equal => upper,
        }))
    }
}

impl SqrtInterval {
    #[cfg(test)]
    pub(super) fn from_squared(squared: &Rational) -> Result<Self> {
        Self::from_squared_with_error(squared, None)
    }

    pub(super) fn from_squared_with_error(
        squared: &Rational,
        error: Option<&Rational>,
    ) -> Result<Self> {
        #[cfg(test)]
        if exact_sqrt_path_forced() {
            record_sqrt_fallback();
            return Self::from_squared_exact_with_error(squared, error);
        }

        if let Some(bracket) = Self::from_squared_fast(squared)? {
            #[cfg(test)]
            record_sqrt_fast_admission();
            return bracket.into_interval(squared, error);
        }

        #[cfg(test)]
        record_sqrt_fallback();
        Self::from_squared_exact_with_error(squared, error)
    }

    /// Seed a bracket from the rounded squared value, then certify each
    /// endpoint against the original exact squared value. A platform sqrt
    /// estimate that is not close enough simply takes the exact fallback.
    pub(super) fn from_squared_fast(squared: &Rational) -> Result<Option<FloatSqrtBracket>> {
        const MAX_ULP_STEPS: usize = 4;
        let rounded_squared = match squared.round() {
            Ok(value) => value,
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::ExactCoordinateOutOfRange
                    )
                ) =>
            {
                return Ok(None);
            },
            Err(error) => return Err(error),
        };
        if !(rounded_squared.is_finite() && rounded_squared > 0.0) {
            return Ok(None);
        }

        let proposal = rounded_squared.sqrt();
        if !(proposal.is_finite() && proposal > 0.0) {
            return Ok(None);
        }

        let mut order = squared.try_cmp(&square_f64(proposal)?)?;
        if order == Ordering::Equal {
            return Ok(Some(FloatSqrtBracket {
                lower: proposal,
                upper: Some(proposal),
            }));
        }

        match order {
            Ordering::Less => {
                let mut upper = proposal;
                for _ in 0..MAX_ULP_STEPS {
                    let lower = upper.next_down();
                    order = squared.try_cmp(&square_f64(lower)?)?;
                    match order {
                        Ordering::Greater => {
                            return Ok(Some(FloatSqrtBracket {
                                lower,
                                upper: Some(upper),
                            }));
                        },
                        Ordering::Equal => {
                            return Ok(Some(FloatSqrtBracket {
                                lower,
                                upper: Some(lower),
                            }));
                        },
                        Ordering::Less => upper = lower,
                    }
                }
            },
            Ordering::Greater => {
                let mut lower = proposal;
                for _ in 0..MAX_ULP_STEPS {
                    if lower.to_bits() == f64::MAX.to_bits() {
                        return Ok(Some(FloatSqrtBracket { lower, upper: None }));
                    }
                    let upper = lower.next_up();
                    order = squared.try_cmp(&square_f64(upper)?)?;
                    match order {
                        Ordering::Less => {
                            return Ok(Some(FloatSqrtBracket {
                                lower,
                                upper: Some(upper),
                            }));
                        },
                        Ordering::Equal => {
                            return Ok(Some(FloatSqrtBracket {
                                lower: upper,
                                upper: Some(upper),
                            }));
                        },
                        Ordering::Greater => lower = upper,
                    }
                }
            },
            Ordering::Equal => unreachable!("the equal square was returned above"),
        }
        Ok(None)
    }

    pub(super) fn from_squared_exact_with_error(
        squared: &Rational,
        error: Option<&Rational>,
    ) -> Result<Self> {
        if squared.try_cmp(&square_f64(f64::MAX)?)? == Ordering::Greater {
            return Self::overflow(squared, error);
        }
        let value = squared.clone();
        let rounded = value.round_sqrt()?;
        if rounded.is_infinite() {
            return Self::overflow(squared, error);
        }
        match squared.try_cmp(&square_f64(rounded)?)? {
            Ordering::Equal => Ok(Self::singleton(rounded)),
            // The predecessor of +0 is negative in total binary64 order,
            // but a squared-distance bracket must stay in the nonnegative root domain.
            Ordering::Less if rounded == 0.0 => Ok(Self::adjacent(0.0, f64::from_bits(1))),
            Ordering::Less => Ok(Self::adjacent(rounded.next_down(), rounded)),
            Ordering::Greater => {
                if rounded.to_bits() == f64::MAX.to_bits() {
                    Self::overflow(squared, error)
                } else {
                    Ok(Self::adjacent(rounded, rounded.next_up()))
                }
            },
        }
    }

    pub(super) fn candidate_from_squared(squared: &Rational) -> Result<Option<f64>> {
        #[cfg(test)]
        if exact_sqrt_path_forced() {
            record_sqrt_fallback();
            return Self::complete_candidate(squared);
        }

        if let Some(bracket) = Self::from_squared_fast(squared)? {
            #[cfg(test)]
            record_sqrt_fast_admission();
            return bracket.nearest_candidate(squared);
        }

        #[cfg(test)]
        record_sqrt_fallback();
        Self::complete_candidate(squared)
    }

    pub(super) fn complete_candidate(squared: &Rational) -> Result<Option<f64>> {
        if squared.try_cmp(&square_f64(f64::MAX)?)? == Ordering::Greater {
            return Ok(Some(f64::MAX));
        }
        let value = squared.clone().round_sqrt()?;
        Ok(value.is_finite().then_some(value))
    }

    pub(super) fn singleton(value: f64) -> Self {
        Self {
            lower: ExactDyadic::from_f64(value),
            upper: Some(ExactDyadic::from_f64(value)),
            overflow_upper: None,
        }
    }

    pub(super) fn adjacent(lower: f64, upper: f64) -> Self {
        debug_assert!(lower.is_finite() && upper.is_finite());
        debug_assert_eq!(upper.to_bits().checked_sub(lower.to_bits()), Some(1));
        Self {
            lower: ExactDyadic::from_f64(lower),
            upper: Some(ExactDyadic::from_f64(upper)),
            overflow_upper: None,
        }
    }

    pub(super) fn overflow(squared: &Rational, error: Option<&Rational>) -> Result<Self> {
        let maximum = Rational::from_f64(f64::MAX);
        let mut upper = squared.try_quotient(&maximum)?;
        let target = error.map(|error| maximum.try_add(error)).transpose()?;
        let target_squared = target.as_ref().map(Rational::try_square).transpose()?;
        for _ in 0..OVERFLOW_NEWTON_STEPS {
            upper = overflow_newton_step(squared, &upper)?;
            if let Some(target) = target.as_ref()
                && matches!(upper.try_cmp(target)?, Ordering::Less | Ordering::Equal)
            {
                break;
            }
        }
        if let (Some(target), Some(target_squared)) = (target, target_squared)
            && squared.try_cmp(&target_squared)? != Ordering::Greater
            && upper.try_cmp(&target)? == Ordering::Greater
        {
            upper = target;
        }
        Ok(Self {
            lower: ExactDyadic::from_f64(f64::MAX),
            upper: None,
            overflow_upper: Some(upper),
        })
    }

    pub(super) fn refine(&mut self, squared: &Rational, error: &Rational) -> Result<()> {
        let Some(mut upper) = self.upper.take() else {
            return Ok(());
        };
        loop {
            let width = Rational::from_dyadic(upper.clone().try_subtract(self.lower.clone())?);
            if width.try_cmp(error)? != Ordering::Greater {
                self.upper = Some(upper);
                return Ok(());
            }

            let midpoint = self
                .lower
                .clone()
                .try_add(upper.clone())?
                .try_product(&ExactDyadic::from_f64(0.5))?;
            let midpoint_rational = Rational::from_dyadic(midpoint.clone());
            let midpoint_squared = midpoint_rational.try_square()?;

            match squared.try_cmp(&midpoint_squared)? {
                Ordering::Equal => {
                    self.lower = midpoint.clone();
                    upper = midpoint;
                },
                Ordering::Greater => {
                    self.lower = midpoint;
                },
                Ordering::Less => {
                    upper = midpoint;
                },
            }
        }
    }

    pub(super) fn upper_rational(&self) -> Option<Rational> {
        if let Some(value) = &self.upper {
            return Some(Rational::from_dyadic(value.clone()));
        }
        self.overflow_upper.clone()
    }
}

pub(super) fn min_rational(current: &mut Option<Rational>, candidate: Rational) -> Result<()> {
    let replace = match current {
        None => true,
        Some(value) => candidate.try_cmp(value)? == Ordering::Less,
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}

pub(super) fn max_rational(current: &mut Option<Rational>, candidate: Rational) -> Result<()> {
    let replace = match current {
        None => true,
        Some(value) => candidate.try_cmp(value)? == Ordering::Greater,
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}
