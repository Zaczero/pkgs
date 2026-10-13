#[test]
fn taylor_tail_bounds_an_independent_single_harmonic_remainder() {
    let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
    coefficients[0] = 1.0;
    let density = GeographicAreaDensity::new(coefficients, 1, 1.0, 0.0, GeodeticPole::North);
    let order = 4;
    let latitude = 1.0 / 16.0;
    let mut polynomial = Vec::new();
    density.fill_coefficients(order, &mut polynomial).unwrap();
    let mut approximation = 0.0;
    let mut power = 1.0;
    for coefficient in &polynomial {
        approximation += coefficient.value * power;
        power *= latitude;
    }
    let exact_field = -0.5 * (8.0 * latitude).sin();
    let independently_evaluated_remainder = (exact_field - approximation).abs();
    let mut tail = TaylorTail::new(
        &density,
        PositiveBound::from_f64(1.0).unwrap(),
        PositiveBound::from_f64(latitude).unwrap(),
    )
    .unwrap();
    let upper = tail.bound_at(order + 1).unwrap().to_exact().unwrap();
    assert!(
        upper
            .cmp(&ExactDyadic::from_f64(independently_evaluated_remainder))
            .is_ge()
    );
}

include!("../coefficient_certificate_tests.rs");
