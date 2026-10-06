#[test]
fn coefficient_row_enclosure_covers_exact_derivative_numerators() {
    for phase in [0.0, 0.7, std::f64::consts::FRAC_PI_2] {
        for pole in [GeodeticPole::North, GeodeticPole::South] {
            let mut seeds = [0.0; MAX_AUTHALIC_HARMONICS];
            seeds[0] = 1.0;
            seeds[1] = -0.01;
            seeds[8] = 2.0_f64.powi(-64);
            let density = GeographicAreaDensity::new(seeds, 9, 1.0, phase, pole);
            let mut ordinary = Vec::new();
            let mut exact = Vec::new();
            density.fill_coefficients(32, &mut ordinary).unwrap();
            density.fill_exact_derivatives(32, &mut exact).unwrap();
            let mut factorial = ExactDyadic::one();
            for (degree, (coefficient, derivative)) in ordinary.iter().zip(&exact).enumerate() {
                let value = ExactDyadic::from_f64(coefficient.value)
                    .try_product(&factorial)
                    .unwrap();
                let difference = value.try_subtract(derivative.clone()).unwrap().abs();
                let error = ExactDyadic::from_f64(coefficient.error)
                    .try_product(&factorial)
                    .unwrap();
                assert!(
                    difference.cmp(&error).is_le(),
                    "degree={degree}, phase={phase}, pole={pole:?}"
                );
                factorial = factorial
                    .try_product(&ExactDyadic::from_i64((degree + 1) as i64))
                    .unwrap();
            }
        }
    }
}

#[test]
fn coefficient_underflow_receipt_survives_frequency_amplification() {
    let zero = Harmonic {
        coefficient: 0.0,
        order: 1.0,
        sine: 0.0,
        cosine: 0.0,
    };
    let mut harmonics = [zero; MAX_AUTHALIC_HARMONICS];
    harmonics[0] = Harmonic {
        coefficient: 3.0 * MIN_SUBNORMAL,
        order: 17.0,
        sine: 1.0,
        cosine: 0.0,
    };
    let density = GeographicAreaDensity {
        harmonics,
        count: 1,
        semi_major: 1.0,
        reference_pole: GeodeticPole::North,
    };
    let mut ordinary = Vec::new();
    let mut exact = Vec::new();
    density.fill_coefficients(2, &mut ordinary).unwrap();
    density.fill_exact_derivatives(2, &mut exact).unwrap();
    let factorial = ExactDyadic::from_i64(2);
    let residual = ExactDyadic::from_f64(ordinary[2].value)
        .try_product(&factorial)
        .unwrap()
        .try_subtract(exact[2].clone())
        .unwrap()
        .abs();
    let radius = ExactDyadic::from_f64(ordinary[2].error)
        .try_product(&factorial)
        .unwrap();
    assert!(
        residual.cmp(&radius).is_le(),
        "coefficient underflow error was not propagated"
    );
}
