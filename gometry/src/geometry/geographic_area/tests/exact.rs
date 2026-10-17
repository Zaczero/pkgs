#[test]
fn exact_reversed_source_has_zero_moments_but_positive_tail() {
    let points = [
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0 / 16.0),
        (0.0, 1.0 / 16.0),
        (0.0, 0.0),
        (0.0, 1.0 / 16.0),
        (1.0, 1.0 / 16.0),
        (1.0, 0.0),
        (0.0, 0.0),
    ];
    let mut moments = vec![ExactDyadic::zero(); 12];
    let mut source_bounds = ExactSourceBounds::default();
    for edge in points.windows(2) {
        let [(x0, z0), (x1, z1)] = edge else {
            unreachable!("each source edge has two endpoints");
        };
        add_exact_edge(
            &ExactDyadic::from_f64(*x0),
            &ExactDyadic::from_f64(*z0),
            &ExactDyadic::from_f64(*x1),
            &ExactDyadic::from_f64(*z1),
            &mut moments,
            &mut source_bounds,
        )
        .unwrap();
    }
    assert!(moments.iter().all(ExactDyadic::is_zero));
    assert_eq!(source_bounds.variation.sign(), ExactSign::Positive);

    let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
    coefficients[0] = 1.0;
    let density = GeographicAreaDensity::new(
        coefficients,
        1,
        6_378_137.0,
        std::f64::consts::FRAC_PI_2,
        GeodeticPole::North,
    );
    let scale = density.area_scale(0).unwrap();
    let scale_bound = area_scale_bound(&density, 0).unwrap();
    let mut tail = TaylorTail::new(
        &density,
        positive_exact_bound(&source_bounds.variation).unwrap(),
        positive_exact_bound(&source_bounds.maximum_latitude).unwrap(),
    )
    .unwrap();
    let ceiling = tail
        .sufficient_order(scale_bound, INITIAL_TAYLOR_ORDER)
        .unwrap();
    let first_tail = tail.bound_at(INITIAL_TAYLOR_ORDER + 1).unwrap();
    assert!(!first_tail.is_zero());

    let mut accepted_order = None;
    for order in INITIAL_TAYLOR_ORDER..=ceiling {
        let error = tail.bound_at(order + 1).unwrap();
        if accept_work_budget(error, PositiveBound::ZERO, &scale, order).unwrap() {
            accepted_order = Some(order);
            break;
        }
    }
    assert!(accepted_order.is_some());
    assert!(accepted_order.unwrap() <= ceiling);
}

#[test]
fn zero_moment_prefix_does_not_hide_a_nonzero_later_moment() {
    let mut points = vec![(0.0, 0.0)];
    for (index, weight) in [1.0, 4.0, 6.0, 4.0, 1.0].into_iter().enumerate() {
        let low = index as f64 / 8.0;
        let high = low + 1.0 / 256.0;
        let width = weight / 256.0;
        let mut rectangle = vec![
            (0.0, low),
            (width, low),
            (width, high),
            (0.0, high),
            (0.0, low),
        ];
        if index % 2 != 0 {
            rectangle.reverse();
        }
        points.extend(rectangle);
        points.push((0.0, 0.0));
    }
    let mut moments = vec![ExactDyadic::zero(); 5];
    let mut source_bounds = ExactSourceBounds::default();
    for edge in points.windows(2) {
        let [(x0, z0), (x1, z1)] = edge else {
            unreachable!("each source edge has two endpoints");
        };
        add_exact_edge(
            &ExactDyadic::from_f64(*x0),
            &ExactDyadic::from_f64(*z0),
            &ExactDyadic::from_f64(*x1),
            &ExactDyadic::from_f64(*z1),
            &mut moments,
            &mut source_bounds,
        )
        .unwrap();
    }
    assert!(moments[..4].iter().all(ExactDyadic::is_zero));
    assert_eq!(
        round_ratio(&moments[4], &ExactDyadic::one()).unwrap(),
        45.0 / 16_777_216.0
    );
    assert_eq!(source_bounds.variation.sign(), ExactSign::Positive);
}

#[test]
fn fast_and_exact_contractions_include_the_last_active_degree() {
    let mut moments = Vec::with_capacity(3);
    for value in [3.0, 4.0, 5.0] {
        let mut moment = BoundedSum::default();
        moment.push(Bounded::exact(value));
        moments.push(moment);
    }
    let coefficients = [2.0, 6.0, 12.0].map(|value| AreaCoefficient { value, error: 0.0 });
    let (integral, ..) = contract_fast(&moments, &coefficients);
    assert_eq!(integral.value, 12.0);

    let mut last_only_fast_moments = Vec::with_capacity(3);
    for value in [0.0, 0.0, 5.0] {
        let mut moment = BoundedSum::default();
        moment.push(Bounded::exact(value));
        last_only_fast_moments.push(moment);
    }
    let last_only_fast_coefficients =
        [0.0, 0.0, 12.0].map(|value| AreaCoefficient { value, error: 0.0 });
    let (last_only_fast, ..) =
        contract_fast(&last_only_fast_moments, &last_only_fast_coefficients);
    assert_eq!(last_only_fast.value, 5.0);

    let exact_moments = [3.0, 4.0, 5.0].map(ExactDyadic::from_f64);
    let exact_derivatives = [2.0, 6.0, 24.0].map(ExactDyadic::from_f64);
    let exact = ExactFactorialArea::from_derivatives(&exact_derivatives, &exact_moments)
        .unwrap()
        .scaled_round(&ExactDyadic::one())
        .unwrap();
    assert_eq!(exact, 12.0);

    let last_only_moments = [
        ExactDyadic::zero(),
        ExactDyadic::zero(),
        ExactDyadic::from_f64(5.0),
    ];
    let last_only_derivatives = [
        ExactDyadic::zero(),
        ExactDyadic::zero(),
        ExactDyadic::from_f64(24.0),
    ];
    let last_only =
        ExactFactorialArea::from_derivatives(&last_only_derivatives, &last_only_moments)
            .unwrap()
            .scaled_round(&ExactDyadic::one())
            .unwrap();
    assert_eq!(last_only, 5.0);
}

#[test]
fn exact_factorial_contraction_retains_scaled_terms_below_binary64_factors() {
    use crate::geometry::numerics::product::exact_power_of_two;
    const DEGREE: usize = 1025;
    let mut harmonics = [Harmonic {
        coefficient: 0.0,
        order: 1.0,
        sine: 0.0,
        cosine: 0.0,
    }; MAX_AUTHALIC_HARMONICS];
    harmonics[8] = Harmonic {
        coefficient: exact_power_of_two(-64),
        order: 17.0,
        sine: 0.0,
        cosine: 1.0,
    };
    let density = GeographicAreaDensity {
        harmonics,
        count: MAX_AUTHALIC_HARMONICS,
        semi_major: 1.0,
        reference_pole: GeodeticPole::North,
    };

    let mut fast_coefficients = Vec::new();
    density
        .fill_coefficients(DEGREE, &mut fast_coefficients)
        .unwrap();
    assert_eq!(fast_coefficients[DEGREE].value, 0.0);

    let mut exact_derivatives = Vec::new();
    density
        .fill_exact_derivatives(DEGREE, &mut exact_derivatives)
        .unwrap();
    let determinant = ExactDyadic::try_from_u128(false, 3, -8).unwrap();
    let radius = ExactDyadic::try_from_u128(false, 3, -3).unwrap();
    let mut exact_moment = determinant;
    for _ in 0..DEGREE {
        exact_moment = exact_moment.try_product(&radius).unwrap();
    }
    let mut fast_moment = 3.0 / 256.0;
    for _ in 0..DEGREE {
        fast_moment *= 3.0 / 8.0;
    }
    assert_eq!(fast_moment, 0.0);

    let mut moments = vec![ExactDyadic::zero(); DEGREE + 1];
    moments[DEGREE] = exact_moment;
    let mut derivatives = vec![ExactDyadic::zero(); DEGREE + 1];
    derivatives[DEGREE] = exact_derivatives[DEGREE].clone();
    let area = ExactFactorialArea::from_derivatives(&derivatives, &moments).unwrap();
    let scale = ExactDyadic::try_from_u128(false, 1, 2055).unwrap();
    let actual = area.scaled_round(&scale).unwrap();
    let expected = -6.936_318_504_392_211e-301;
    assert!(actual.is_normal() && actual < 0.0);
    assert!((actual / expected - 1.0).abs() <= 2.0e-15);
}

#[test]
fn exact_publication_handles_scale_overflow_underflow_and_subnormal_quantization() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
    coefficients[0] = 1.0;

    let large_axis = 1.0e155;
    let large_density = GeographicAreaDensity::new(
        coefficients,
        1,
        large_axis,
        std::f64::consts::FRAC_PI_2,
        GeodeticPole::North,
    );
    assert!(!(large_axis * large_axis).is_finite());
    assert!(scale_area_fast(1.0e-308, &large_density, -6).is_none());
    let two = ExactDyadic::from_i64(2);
    let large_derivatives = [ExactDyadic::from_f64(1.0e-308).try_product(&two).unwrap()];
    let unit_moment = [ExactDyadic::one()];
    let normalized_area =
        ExactFactorialArea::from_derivatives(&large_derivatives, &unit_moment).unwrap();
    let actual = normalized_area
        .scaled_round(&large_density.area_scale(-6).unwrap())
        .unwrap();
    let expected = 51_199.999_999_999_99;
    assert!(actual.is_finite());
    assert!((actual / expected - 1.0).abs() <= 2.0e-15);

    let small_axis = exact_power_of_two(-538);
    let small_density = GeographicAreaDensity::new(
        coefficients,
        1,
        small_axis,
        std::f64::consts::FRAC_PI_2,
        GeodeticPole::North,
    );
    assert_eq!(small_axis * small_axis, 0.0);
    assert!(scale_area_fast(f64::MAX, &small_density, -6).is_none());
    let large_integral_derivatives = [ExactDyadic::try_from_u128(false, 1, 1074).unwrap()];
    let normalized_area =
        ExactFactorialArea::from_derivatives(&large_integral_derivatives, &unit_moment)
            .unwrap();
    assert_eq!(
        normalized_area
            .scaled_round(&small_density.area_scale(-6).unwrap())
            .unwrap(),
        64.0
    );

    let subnormal_integral = ExactDyadic::try_from_u128(false, 3, -1076).unwrap();
    let subnormal_derivatives = [ExactDyadic::try_from_u128(false, 3, -1075).unwrap()];
    let subnormal =
        ExactFactorialArea::from_derivatives(&subnormal_derivatives, &unit_moment).unwrap();
    let rounded = subnormal.scaled_round(&ExactDyadic::one()).unwrap();
    assert_eq!(rounded.to_bits(), 1);
    let quantization = ExactDyadic::from_f64(rounded)
        .try_subtract(subnormal_integral)
        .unwrap()
        .abs();
    let norm =
        ExactFactorialArea::absolute_contraction(&subnormal_derivatives, &unit_moment).unwrap();
    assert!(
        !norm
            .compare_dyadic_gamma(&quantization, 18)
            .unwrap()
            .is_le()
    );
}
