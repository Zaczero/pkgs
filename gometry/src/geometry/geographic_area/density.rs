use super::{
    ExactDyadic, GeodeticPole, GeometryErrorKind, PositiveBound, Result, upper_add, upper_div_u64,
    upper_mul,
};

pub(crate) const MAX_AUTHALIC_HARMONICS: usize = 9;

pub(super) const INITIAL_TAYLOR_ORDER: usize = 4;

pub(super) const LATITUDE_SCALE: f64 = 0.125;

pub(super) const MIN_SUBNORMAL: f64 = f64::from_bits(1);

#[derive(Clone, Copy)]
pub(super) struct Harmonic {
    pub(super) coefficient: f64,
    pub(super) order: f64,
    pub(super) sine: f64,
    pub(super) cosine: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct GeographicAreaDensity {
    pub(super) harmonics: [Harmonic; MAX_AUTHALIC_HARMONICS],
    pub(super) count: usize,
    pub(super) semi_major: f64,
    pub(super) reference_pole: GeodeticPole,
}

impl GeographicAreaDensity {
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "AuthalicSeries::area_density transfers this fixed coefficient array into owned harmonic state"
    )]
    pub(crate) fn new(
        coefficients: [f64; MAX_AUTHALIC_HARMONICS],
        count: usize,
        semi_major: f64,
        reference_colatitude: f64,
        reference_pole: GeodeticPole,
    ) -> Self {
        let mut harmonics = [Harmonic {
            coefficient: 0.0,
            order: 1.0,
            sine: 0.0,
            cosine: 1.0,
        }; MAX_AUTHALIC_HARMONICS];
        let (active_harmonics, _) = harmonics.split_at_mut(count);
        let (coefficients, _) = coefficients.split_at(count);
        for (index, (harmonic, coefficient)) in
            active_harmonics.iter_mut().zip(coefficients).enumerate()
        {
            // The checked active prefix has at most nine harmonics (odd orders 1 through 17).
            let order = index.wrapping_mul(2).wrapping_add(1) as f64;
            let angle = order * reference_colatitude;
            let (sine, cosine) = angle.sin_cos();
            *harmonic = Harmonic {
                coefficient: if index % 2 == 0 {
                    *coefficient
                } else {
                    -*coefficient
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

    pub(super) fn fill_coefficients(
        self,
        order: usize,
        output: &mut Vec<AreaCoefficient>,
    ) -> Result<()> {
        output.clear();
        output.reserve(order.saturating_add(1));
        // A successful nonzero-element reserve bounds order+1 below the Vec allocation limit;
        // degree+2 and the at-most-13 operation-count adjustment therefore cannot wrap.
        let mut even = [0.0; MAX_AUTHALIC_HARMONICS];
        let mut odd = [0.0; MAX_AUTHALIC_HARMONICS];
        let mut frequencies = [1.0; MAX_AUTHALIC_HARMONICS];
        let (harmonics, _) = self.harmonics.split_at(self.count);
        let (even, _) = even.split_at_mut(self.count);
        let (odd, _) = odd.split_at_mut(self.count);
        let (frequencies, _) = frequencies.split_at_mut(self.count);
        let mut maximum_frequency_squared = 1.0_f64;
        let mut phase_bound = 1.0_f64;
        let derivative_direction = match self.reference_pole {
            GeodeticPole::North => -1.0,
            GeodeticPole::South => 1.0,
        };
        for (harmonic, (even, (odd, frequency_squared))) in harmonics.iter().zip(
            even.iter_mut()
                .zip(odd.iter_mut().zip(frequencies.iter_mut())),
        ) {
            let weighted = harmonic.coefficient * harmonic.order * 0.5;
            let frequency = derivative_direction * harmonic.order * (1.0 / LATITUDE_SCALE);
            *even = weighted * harmonic.sine;
            *odd = weighted * frequency * harmonic.cosine;
            *frequency_squared = (harmonic.order * (1.0 / LATITUDE_SCALE)).powi(2);
            maximum_frequency_squared = maximum_frequency_squared.max(*frequency_squared);
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
            let selected = if degree % 2 == 0 { &*even } else { &*odd };
            for &term in selected {
                value += term;
                absolute = upper_add(absolute, term.abs());
            }
            let seed_operations: usize = if degree % 2 == 0 { 3 } else { 4 };
            let operations = seed_operations
                .wrapping_add((degree / 2).wrapping_mul(2))
                .wrapping_add(self.count) as f64;
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
            if degree.wrapping_add(2) > order {
                continue;
            }
            let left_denominator = degree.wrapping_add(1) as u64;
            let right_denominator = degree.wrapping_add(2) as u64;
            // Each factor fits, but their product is a separate observable size bound.
            let denominator = left_denominator
                .checked_mul(right_denominator)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let selected = if degree % 2 == 0 {
                &mut *even
            } else {
                &mut *odd
            };
            for (value, frequency) in selected.iter_mut().zip(frequencies.iter()) {
                *value = (-*value * *frequency) / denominator as f64;
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

    pub(super) fn fill_exact_derivatives(
        self,
        order: usize,
        output: &mut Vec<ExactDyadic>,
    ) -> Result<()> {
        output.clear();
        output.reserve(order.saturating_add(1));
        let mut even: [ExactDyadic; MAX_AUTHALIC_HARMONICS] =
            std::array::from_fn(|_| ExactDyadic::zero());
        let mut odd: [ExactDyadic; MAX_AUTHALIC_HARMONICS] =
            std::array::from_fn(|_| ExactDyadic::zero());
        let mut frequency_squared: [ExactDyadic; MAX_AUTHALIC_HARMONICS] =
            std::array::from_fn(|_| ExactDyadic::zero());
        let (harmonics, _) = self.harmonics.split_at(self.count);
        let (even, _) = even.split_at_mut(self.count);
        let (odd, _) = odd.split_at_mut(self.count);
        let (frequency_squared, _) = frequency_squared.split_at_mut(self.count);
        let derivative_direction = match self.reference_pole {
            GeodeticPole::North => -1.0,
            GeodeticPole::South => 1.0,
        };
        for (harmonic, (even, (odd, frequency_squared))) in harmonics.iter().zip(
            even.iter_mut()
                .zip(odd.iter_mut().zip(frequency_squared.iter_mut())),
        ) {
            let order = ExactDyadic::from_f64(harmonic.order);
            let weighted = ExactDyadic::from_f64(harmonic.coefficient)
                .try_product(&order)?
                .try_product(&ExactDyadic::from_f64(0.5))?;
            let frequency = ExactDyadic::from_f64(harmonic.order * (1.0 / LATITUDE_SCALE));
            *even = weighted.try_product(&ExactDyadic::from_f64(harmonic.sine))?;
            *odd = weighted
                .try_product(&frequency.try_product(&ExactDyadic::from_f64(derivative_direction))?)?
                .try_product(&ExactDyadic::from_f64(harmonic.cosine))?;
            *frequency_squared = frequency.try_product(&frequency)?;
        }

        for degree in 0..=order {
            let mut sum = ExactDyadic::zero();
            let selected = if degree % 2 == 0 { &*even } else { &*odd };
            for coefficient in selected {
                sum = sum.try_add(coefficient.clone())?;
            }
            output.push(sum);
            let selected = if degree % 2 == 0 {
                &mut *even
            } else {
                &mut *odd
            };
            for (derivative, frequency_squared) in selected.iter_mut().zip(frequency_squared.iter())
            {
                *derivative = derivative
                    .clone()
                    .try_product(&frequency_squared.clone().neg())?;
            }
        }
        Ok(())
    }

    pub(super) fn area_scale(self, x_exponent: i32) -> Result<ExactDyadic> {
        let scale_exponent = x_exponent
            .checked_neg()
            .and_then(|exponent| exponent.checked_add(3))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let scale = 2.0_f64.powi(scale_exponent);
        ExactDyadic::from_f64(self.semi_major)
            .try_product(&ExactDyadic::from_f64(self.semi_major))?
            .try_product(&ExactDyadic::from_f64(scale))
    }
}

#[derive(Clone, Copy)]
pub(super) struct AreaCoefficient {
    pub(super) value: f64,
    pub(super) error: f64,
}

#[derive(Clone)]
pub(super) struct TaylorTail {
    amplitude: [PositiveBound; MAX_AUTHALIC_HARMONICS],
    argument: [PositiveBound; MAX_AUTHALIC_HARMONICS],
    term: [PositiveBound; MAX_AUTHALIC_HARMONICS],
    count: usize,
    degree: usize,
    variation: PositiveBound,
}

impl TaylorTail {
    pub(super) fn new(
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

    pub(super) fn bound_at(&mut self, omitted_degree: usize) -> Result<PositiveBound> {
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

    pub(super) fn sufficient_order(
        &self,
        scale: PositiveBound,
        initial_order: usize,
    ) -> Result<usize> {
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
