#[test]
fn coefficient_row_enclosure_covers_exact_derivative_numerators() {
    use crate::geometry::numerics::product::exact_power_of_two;
    for phase in [0.0, 0.7, std::f64::consts::FRAC_PI_2] {
        for pole in [GeodeticPole::North, GeodeticPole::South] {
            let mut seeds = [0.0; MAX_AUTHALIC_HARMONICS];
            seeds[0] = 1.0;
            seeds[1] = -0.01;
            seeds[8] = exact_power_of_two(-64);
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
fn coefficient_rows_pin_active_prefix_parity_and_reused_output_buffers() {
    let coefficients = [2.0, 6.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 64.0];
    let cases: [(usize, GeodeticPole, [i64; 4], [f64; 3]); 6] = [
        (0, GeodeticPole::North, [0, 0, 0, 0], [0.0, 0.0, 0.0]),
        (0, GeodeticPole::South, [0, 0, 0, 0], [0.0, 0.0, 0.0]),
        (1, GeodeticPole::North, [0, -8, 0, 512], [0.0, -8.0, 0.0]),
        (1, GeodeticPole::South, [0, 8, 0, -512], [0.0, 8.0, 0.0]),
        (2, GeodeticPole::North, [0, 208, 0, -123_904], [
            0.0, 208.0, 0.0,
        ]),
        (2, GeodeticPole::South, [0, -208, 0, 123_904], [
            0.0, -208.0, 0.0,
        ]),
    ];
    let factorials = [1_i64, 1, 2, 6];
    let mut ordinary = vec![
        AreaCoefficient {
            value: 17.0,
            error: 19.0,
        };
        7
    ];
    let mut exact = vec![ExactDyadic::from_i64(23); 7];

    for (count, pole, expected_derivatives, expected_values) in cases {
        let density = GeographicAreaDensity::new(coefficients, count, 1.0, 0.0, pole);
        density.fill_coefficients(3, &mut ordinary).unwrap();
        density.fill_exact_derivatives(3, &mut exact).unwrap();
        assert_eq!(ordinary.len(), 4, "count={count}, pole={pole:?}");
        assert_eq!(exact.len(), 4, "count={count}, pole={pole:?}");

        for (coefficient, expected) in ordinary.iter().zip(expected_values) {
            assert_eq!(
                coefficient.value.to_bits(),
                expected.to_bits(),
                "ordinary value count={count}, pole={pole:?}",
            );
        }
        for (degree, ((coefficient, derivative), (expected_derivative, factorial))) in ordinary
            .iter()
            .zip(exact.iter())
            .zip(expected_derivatives.into_iter().zip(factorials))
            .enumerate()
        {
            let expected_numerator = ExactDyadic::from_i64(expected_derivative);
            assert_eq!(
                derivative.cmp(&expected_numerator),
                std::cmp::Ordering::Equal,
                "exact derivative count={count}, pole={pole:?}, degree={degree}",
            );
            assert!(
                coefficient.error.is_finite() && coefficient.error >= 0.0,
                "invalid receipt count={count}, pole={pole:?}, degree={degree}",
            );
            let factorial = ExactDyadic::from_i64(factorial);
            let ordinary_numerator = ExactDyadic::from_f64(coefficient.value)
                .try_product(&factorial)
                .unwrap();
            let residual = ordinary_numerator
                .try_subtract(expected_numerator)
                .unwrap()
                .abs();
            let receipt = ExactDyadic::from_f64(coefficient.error)
                .try_product(&factorial)
                .unwrap();
            assert!(
                residual.cmp(&receipt).is_le(),
                "ordinary receipt count={count}, pole={pole:?}, degree={degree}",
            );
        }
    }
}

#[test]
fn coefficient_fills_ignore_nonzero_inactive_harmonic_lane() {
    let inactive = Harmonic {
        coefficient: 64.0,
        order: 17.0,
        sine: 0.75,
        cosine: -1.25,
    };
    let unit_cosine = Harmonic {
        coefficient: 2.0,
        order: 1.0,
        sine: 0.0,
        cosine: 1.0,
    };
    let zero = Harmonic {
        coefficient: 0.0,
        order: 1.0,
        sine: 0.0,
        cosine: 1.0,
    };
    let density = GeographicAreaDensity {
        harmonics: [
            unit_cosine,
            zero,
            zero,
            zero,
            zero,
            zero,
            zero,
            zero,
            inactive,
        ],
        count: 1,
        semi_major: 1.0,
        reference_pole: GeodeticPole::North,
    };
    let mut ordinary = Vec::new();
    let mut exact = Vec::new();
    density.fill_coefficients(3, &mut ordinary).unwrap();
    density.fill_exact_derivatives(3, &mut exact).unwrap();

    assert_eq!(ordinary.len(), 4);
    for (coefficient, expected) in ordinary.iter().zip([0.0_f64, -8.0, 0.0]) {
        assert_eq!(coefficient.value.to_bits(), expected.to_bits());
    }
    for (derivative, expected) in exact.iter().zip([0_i64, -8, 0, 512]) {
        assert_eq!(
            derivative.cmp(&ExactDyadic::from_i64(expected)),
            std::cmp::Ordering::Equal,
        );
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
