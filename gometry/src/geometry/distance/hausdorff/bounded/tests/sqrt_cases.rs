fn assert_sqrt_certificate(name: &str, squared: &Rational) -> SqrtInterval {
    let interval = SqrtInterval::from_squared(squared).expect("sqrt interval");
    let lower = Rational::from_dyadic(interval.lower.clone())
        .try_square()
        .expect("lower square");
    assert_ne!(
        lower.try_cmp(squared).expect("lower comparison"),
        Ordering::Greater,
        "{name}: lower endpoint is above the exact root"
    );
    let lower_value = dyadic_as_f64(&interval.lower).expect("lower binary64");
    if let Some(upper) = &interval.upper {
        let upper_squared = Rational::from_dyadic(upper.clone())
            .try_square()
            .expect("upper square");
        assert_ne!(
            upper_squared.try_cmp(squared).expect("upper comparison"),
            Ordering::Less,
            "{name}: upper endpoint is below the exact root"
        );
        let upper_value = dyadic_as_f64(upper).expect("upper binary64");
        if lower_value.to_bits() != upper_value.to_bits() {
            assert_eq!(
                lower_value.to_bits().checked_add(1),
                Some(upper_value.to_bits()),
                "{name}: finite endpoints must be adjacent"
            );
        }
    } else {
        assert_eq!(
            lower_value.to_bits(),
            f64::MAX.to_bits(),
            "{name}: an overflow interval must start at f64::MAX"
        );
        let upper = interval
            .upper_rational()
            .expect("finite exact overflow upper");
        let upper_squared = upper.try_square().expect("overflow upper square");
        assert_ne!(
            upper_squared
                .try_cmp(squared)
                .expect("overflow upper comparison"),
            Ordering::Less,
            "{name}: exact overflow upper is below the root"
        );
    }
    interval
}

#[test]
fn verified_sqrt_brackets_cover_irrational_exact_and_boundary_values() {
    reset_sqrt_stats();
    let minimum_subnormal = f64::from_bits(1);
    let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
    let minimum_normal = f64::from_bits(0x0010_0000_0000_0000);
    let maximum = f64::MAX;
    let below_minimum = ExactDyadic::from_f64(minimum_subnormal)
        .try_product(&ExactDyadic::from_f64(0.5))
        .unwrap();
    let underflowing_squared = squared(1.5e-162);
    let cases = [
        ("irrational two", Rational::from_f64(2.0)),
        ("exact square", squared(3.0)),
        (
            "normal rounding midpoint",
            squared_midpoint(1.0, 1.0_f64.next_up()),
        ),
        (
            "minimum subnormal rounding midpoint",
            squared_midpoint(minimum_subnormal, f64::from_bits(2)),
        ),
        (
            "subnormal normal rounding midpoint",
            squared_midpoint(largest_subnormal, minimum_normal),
        ),
        ("minimum subnormal root", squared(minimum_subnormal)),
        ("minimum normal root", squared(minimum_normal)),
        ("rounded squared underflow", underflowing_squared),
        (
            "root below minimum subnormal",
            Rational::from_dyadic(below_minimum).try_square().unwrap(),
        ),
        ("maximum square", squared(maximum)),
        (
            "root just beyond maximum",
            Rational::from_dyadic(
                ExactDyadic::from_f64(maximum)
                    .try_add(ExactDyadic::from_f64(2.0_f64.powi(969)))
                    .unwrap(),
            )
            .try_square()
            .unwrap(),
        ),
        (
            "root at overflow rounding midpoint",
            Rational::from_dyadic(
                ExactDyadic::from_f64(maximum)
                    .try_add(ExactDyadic::from_f64(2.0_f64.powi(970)))
                    .unwrap(),
            )
            .try_square()
            .unwrap(),
        ),
    ];
    for (name, value) in &cases {
        assert_sqrt_certificate(name, value);
    }
    assert!(
        sqrt_stats().fast_admissions > 0,
        "ordinary cases should admit the verified candidate"
    );
    assert!(
        sqrt_stats().fallbacks > 0,
        "underflow and overflow cases should use the exact fallback"
    );
}

#[test]
fn endpoint_verification_rejects_a_reversed_sqrt_comparison() {
    reset_sqrt_stats();
    let squared = Rational::from_f64(2.0);
    let proposal = 2.0_f64.sqrt();
    assert_eq!(
        squared
            .try_cmp(&square_f64(proposal).expect("proposal square"))
            .unwrap(),
        Ordering::Less,
        "the deliberately chosen libm proposal is above the exact root"
    );
    assert_sqrt_certificate("reversed-comparison negative control", &squared);
    assert_eq!(sqrt_stats().fast_admissions, 1);
}

#[test]
fn verified_sqrt_candidate_matches_complete_rounding_at_extrema() {
    let minimum_subnormal = f64::from_bits(1);
    let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
    let minimum_normal = f64::from_bits(0x0010_0000_0000_0000);
    let maximum = f64::MAX;
    let below_minimum = ExactDyadic::from_f64(minimum_subnormal)
        .try_product(&ExactDyadic::from_f64(0.5))
        .unwrap();
    let cases = [
        Rational::zero(),
        Rational::from_f64(2.0),
        Rational::from_f64(3.0),
        Rational::from_f64(minimum_subnormal),
        Rational::from_f64(minimum_normal),
        Rational::from_f64(maximum),
        squared(minimum_subnormal),
        squared(minimum_normal),
        squared_midpoint(1.0, 1.0_f64.next_up()),
        squared_midpoint(minimum_subnormal, f64::from_bits(2)),
        squared_midpoint(largest_subnormal, minimum_normal),
        Rational::from_dyadic(below_minimum).try_square().unwrap(),
        squared(maximum),
        Rational::from_dyadic(
            ExactDyadic::from_f64(maximum)
                .try_add(ExactDyadic::from_f64(2.0_f64.powi(969)))
                .unwrap(),
        )
        .try_square()
        .unwrap(),
        Rational::from_dyadic(
            ExactDyadic::from_f64(maximum)
                .try_add(ExactDyadic::from_f64(2.0_f64.powi(970)))
                .unwrap(),
        )
        .try_square()
        .unwrap(),
    ];
    for value in &cases {
        let actual = SqrtInterval::candidate_from_squared(value).unwrap();
        let expected = value.round_sqrt().unwrap();
        if expected.is_infinite() {
            assert_eq!(
                actual.map(f64::to_bits),
                Some(f64::MAX.to_bits()),
                "a finite tolerance candidate must retain f64::MAX above its range"
            );
        } else {
            assert_eq!(
                actual.map(f64::to_bits),
                Some(expected.to_bits()),
                "candidate differs from complete exact rounding"
            );
        }
    }
}

#[test]
fn fast_and_complete_sqrt_paths_have_identical_bounded_results() {
    let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    let diagonal = Shape::Point(Point::new_raw_xy(1.0, 1.0));
    let source_line = line(&[(0.0, 0.0), (2.0, 0.0)]);
    let target_line = line(&[(0.0, 1.0), (2.0, 1.0)]);
    let triangle = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
    let tiny = Shape::Point(Point::new_raw_xy(1.5e-162, 0.0));
    let length = 10.0_f64.powi(155);
    let height = 10.0_f64.powi(-155);
    let reciprocal_baseline = line(&[(0.0, 0.0), (length, 0.0)]);
    let reciprocal_bend = line(&[
        (0.0, height),
        (length / 2.0, 2.0 * height),
        (length, height),
    ]);
    let cases = [
        (&origin, &diagonal, 1e-6, 1.0),
        (&source_line, &target_line, 1e-6, 1.0),
        (&triangle, &source_line, 1e-6, 1.0),
        (&origin, &tiny, 1e-174, 1.0),
        (
            &reciprocal_baseline,
            &reciprocal_bend,
            2.0 * height * 1e-12,
            1.0,
        ),
    ];

    reset_sqrt_stats();
    let fast: Vec<_> = cases
        .iter()
        .map(|(left, right, error, scale)| {
            let _path = force_exact_sqrt_path(false);
            distance(*left, *right, Some(*error), *scale).expect("fast bounded result")
        })
        .collect();
    let fast_stats = sqrt_stats();
    assert!(fast_stats.fast_admissions > 0);
    assert!(fast_stats.fallbacks > 0);

    reset_sqrt_stats();
    let complete: Vec<_> = cases
        .iter()
        .map(|(left, right, error, scale)| {
            let _path = force_exact_sqrt_path(true);
            distance(*left, *right, Some(*error), *scale).expect("complete bounded result")
        })
        .collect();
    let complete_stats = sqrt_stats();
    assert_eq!(complete_stats.fast_admissions, 0);
    assert!(complete_stats.fallbacks > 0);
    assert_eq!(
        fast.iter().map(|value| value.to_bits()).collect::<Vec<_>>(),
        complete
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>()
    );
}

#[test]
fn overflow_newton_steps_remain_exact_upper_bounds() {
    let maximum = Rational::from_f64(f64::MAX);
    let scale = Rational::from_f64(1.0_f64.next_up());
    let squared = maximum
        .try_square()
        .unwrap()
        .try_product(&scale.try_square().unwrap())
        .unwrap();
    let mut upper = squared.try_quotient(&maximum).unwrap();
    for step in 0..=OVERFLOW_NEWTON_STEPS {
        assert_ne!(
            upper.try_square().unwrap().try_cmp(&squared).unwrap(),
            Ordering::Less,
            "Newton upper step {step} fell below the exact squared value"
        );
        if step != OVERFLOW_NEWTON_STEPS {
            let next = overflow_newton_step(&squared, &upper).unwrap();
            assert_ne!(
                next.try_cmp(&upper).unwrap(),
                Ordering::Greater,
                "Newton step {step} must not loosen the upper"
            );
            upper = next;
        }
    }
}
