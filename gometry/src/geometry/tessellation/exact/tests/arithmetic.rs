use crate::work::Unmetered;
#[test]
fn rational_integrality_includes_the_dyadic_numerator_exponent() {
    assert!(ExactRational::zero().is_integer());
    assert!(ExactRational::from_f64(-12.0).is_integer());
    assert!(!ExactRational::from_f64(0.5).is_integer());
    assert!(!ExactRational::from_f64(-1.5).is_integer());
}

#[test]
fn work_product_matches_the_exact_nineteen_limb_result() {
    let left = ExactDyadic::from_parts(false, vec![u64::MAX; 9], 0);
    let right = ExactDyadic::from_parts(false, vec![u64::MAX; 10], 0);
    let expected = [
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        u64::MAX,
        u64::MAX - 1,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        u64::MAX,
    ];

    let ordinary = left.try_product(&right).unwrap();
    let paid = left.try_product_with_work(&right, &mut Unmetered).unwrap();

    assert_eq!(paid, ordinary);
    assert_eq!(paid.limbs.as_slice(), expected);
}

#[test]
fn work_division_matches_the_known_algorithm_d_addback_result() {
    const TOP: u64 = 1 << 63;
    let dividend = [0, 0, TOP, TOP - 1];
    let divisor = [1, 0, TOP];
    let expected_quotient = [u64::MAX - 1];
    let expected_remainder = [2, u64::MAX, TOP - 1];

    let ordinary = ExactDyadic::div_rem_unsigned(&dividend, &divisor);
    let paid =
        ExactDyadic::div_rem_unsigned_with_work(&dividend, &divisor, &mut Unmetered).unwrap();

    assert_eq!(paid, ordinary);
    assert_eq!(paid.0, expected_quotient);
    assert_eq!(paid.1, expected_remainder);
}

#[test]
fn work_gcd_matches_the_inline_and_heap_euclid_results() {
    const PRIME: u64 = 0xFFFF_FFFF_FFFF_FFC5;
    let prime = u128::from(PRIME);
    let small = |value, exponent| ExactDyadic::try_from_u128(false, value, exponent).unwrap();
    let left = small(prime.wrapping_mul(3), 5);
    let right = small(prime.wrapping_mul(5), -3);
    let expected = ExactDyadic::from_parts(false, vec![PRIME], -3);

    let ordinary_inline = left.gcd(&right);
    let paid_inline = left.gcd_with_work(&right, &mut Unmetered).unwrap();
    assert_eq!(paid_inline, ordinary_inline);
    assert_eq!(paid_inline, expected);

    let wide = left
        .try_product(&ExactDyadic::try_from_u128(false, prime, 0).unwrap())
        .unwrap();
    let ordinary_heap = wide.gcd(&right);
    let paid_heap = wide.gcd_with_work(&right, &mut Unmetered).unwrap();
    assert_eq!(paid_heap, ordinary_heap);
    assert_eq!(paid_heap, expected);
}

#[test]
fn work_rational_normalization_and_identity_match_canonical_parts() {
    let numerator = ExactDyadic::from_f64(615.0);
    let denominator = ExactDyadic::from_f64(9.0);
    let expected_numerator = ExactDyadic::from_f64(205.0);
    let expected_denominator = ExactDyadic::from_f64(3.0);

    let ordinary = ExactRational::new(numerator.clone(), denominator.clone()).unwrap();
    let paid = ExactRational::new_with_work(numerator, denominator, &mut Unmetered).unwrap();
    let cloned = paid.try_clone_with_work(&mut Unmetered).unwrap();

    assert_eq!(paid.numerator, expected_numerator);
    assert_eq!(paid.denominator, expected_denominator);
    assert!(cloned.try_eq_with_work(&ordinary, &mut Unmetered).unwrap());
    assert_eq!(
        paid.try_cmp_with_work(&ordinary, &mut Unmetered).unwrap(),
        Ordering::Equal
    );
}

#[test]
fn work_ratio_rounding_matches_one_third_and_an_even_midpoint() {
    let numerator = ExactDyadic::one();
    let denominator = ExactDyadic::from_f64(3.0);
    let ordinary = round_ratio(&numerator, &denominator).unwrap();
    let paid = round_ratio_with_work(&numerator, &denominator, &mut Unmetered).unwrap();
    assert_eq!(paid.to_bits(), 0x3FD5_5555_5555_5555);
    assert_eq!(paid.to_bits(), ordinary.to_bits());

    let midpoint_numerator = ExactDyadic::try_from_u128(false, (1_u128 << 53) + 1, 0).unwrap();
    let midpoint_denominator = ExactDyadic::from_parts(false, vec![1], 53);
    let ordinary = round_ratio(&midpoint_numerator, &midpoint_denominator).unwrap();
    let paid =
        round_ratio_with_work(&midpoint_numerator, &midpoint_denominator, &mut Unmetered).unwrap();
    assert_eq!(paid, 1.0);
    assert_eq!(paid.to_bits(), ordinary.to_bits());
}

#[rstest::rstest]
#[case::shared_odd_denominator([1, 7], [2, 7], [3, 7], [-1, 7], Ordering::Less)]
#[case::reduced_sum([1, 9], [2, 9], [1, 3], [-1, 9], Ordering::Less)]
#[case::opposite_signs([-5, 7], [2, 7], [-3, 7], [-1, 1], Ordering::Less)]
#[case::cancellation([5, 7], [-5, 7], [0, 1], [10, 7], Ordering::Greater)]
#[case::negative_denominator([1, -7], [2, -7], [-3, 7], [1, 7], Ordering::Greater)]
#[case::equivalent_ratios([1, 3], [2, 6], [2, 3], [0, 1], Ordering::Equal)]
#[case::distinct_denominators([1, 3], [2, 7], [13, 21], [1, 21], Ordering::Greater)]
fn rational_arithmetic_preserves_reduced_values(
    #[case] left: [i64; 2],
    #[case] right: [i64; 2],
    #[case] sum: [i64; 2],
    #[case] difference: [i64; 2],
    #[case] order: Ordering,
) {
    let ratio = |[numerator, denominator]: [i64; 2]| {
        ExactRational::new(
            ExactDyadic::from_i64(numerator),
            ExactDyadic::from_i64(denominator),
        )
        .unwrap()
    };
    let left = ratio(left);
    let right = ratio(right);
    assert_eq!(left.try_add(&right).unwrap(), ratio(sum));
    assert_eq!(left.try_subtract(&right).unwrap(), ratio(difference));
    assert_eq!(left.try_cmp(&right).unwrap(), order);
}

fn dyadic_orientation_sign(a: XY, b: XY, c: XY) -> ExactSign {
    let difference =
        |left: f64, right: f64| ExactDyadic::from_f64(left).subtract(ExactDyadic::from_f64(right));
    difference(b.x, a.x)
        .product(&difference(c.y, a.y))
        .subtract(difference(b.y, a.y).product(&difference(c.x, a.x)))
        .sign()
}

#[test]
fn long_division_reconstructs_the_dividend() {
    const TOP: u64 = 1 << 63;
    let value = |limbs: &[u64]| {
        let mut value = ExactDyadic {
            negative: false,
            limbs: CompactLimbs::from_vec(limbs.to_vec()),
            exponent: 0,
        };
        value.try_trim().unwrap();
        value
    };
    let cases: [(&[u64], &[u64]); 9] = [
        // One-digit divisor.
        (&[u64::MAX, u64::MAX, 7], &[3]),
        // Divisor already normalized: no shift.
        (&[5, 0, TOP | 9], &[1, TOP]),
        // The first digit estimate overshoots; one addback corrects it.
        (&[0, 0, TOP, TOP - 1], &[1, 0, TOP]),
        // An estimate two too large, which only the second-digit test fixes.
        (&[u64::MAX - 1, 0, TOP - 1, 1], &[u64::MAX, TOP + 1]),
        // Estimates at the digit boundary.
        (&[u64::MAX, u64::MAX, u64::MAX, u64::MAX], &[u64::MAX, 1]),
        (&[0, 0, 0, 1], &[1, 1]),
        (&[3, 0, 0, 0, 0x1234], &[u64::MAX, u64::MAX, 0x10]),
        // Equal magnitudes and a dividend below the divisor.
        (&[9, 8, 7], &[9, 8, 7]),
        (&[1, 2], &[1, 2, 3]),
    ];
    for (dividend, divisor) in cases {
        let (quotient, remainder) = ExactDyadic::div_rem_unsigned(dividend, divisor);
        assert_eq!(
            ExactDyadic::compare_unsigned(&remainder, divisor),
            Ordering::Less,
            "{dividend:?} / {divisor:?}"
        );
        let rebuilt = value(&quotient)
            .try_product(&value(divisor))
            .unwrap()
            .try_add(value(&remainder))
            .unwrap();
        assert_eq!(
            rebuilt.cmp(&value(dividend)),
            Ordering::Equal,
            "{dividend:?} / {divisor:?}"
        );
    }
}

#[rstest::rstest]
#[case::value_0_5(0.5, ExactDyadic::zero())]
#[case::negative_value_0_5(-0.5, ExactDyadic::zero())]
#[case::value_0_75(0.75, ExactDyadic::one())]
#[case::negative_value_0_75(-0.75, ExactDyadic::one().neg())]
fn nearest_integer_dyadic_quotient_trims_aligned_words_and_rounds_ties_to_zero(
    #[case] value: f64,
    #[case] expected: ExactDyadic,
) {
    let divisor = ExactDyadic::from_f64(1.0);

    assert_eq!(
        ExactDyadic::from_f64(value)
            .nearest_integer_quotient_ties_to_zero(&divisor)
            .unwrap(),
        expected
    );
}

#[test]
fn power_of_two_denominators_reduce_as_the_gcd_would() {
    let small =
        |value: u128, exponent: i64| ExactDyadic::try_from_u128(false, value, exponent).unwrap();
    let prime = ExactDyadic::try_from_u128(false, 0xFFFF_FFFF_FFFF_FFC5, 0).unwrap();
    let numerators = [
        small(1, 0),
        small(1, -4),
        small(3, 0),
        small(3, 7),
        small(3, -9),
        small(5, 2).neg(),
        prime
            .try_product(&prime)
            .unwrap()
            .try_product(&small(1, -2))
            .unwrap(),
    ];
    for numerator in &numerators {
        for exponent in [-6, -1, 0, 1, 6] {
            let denominator = small(1, exponent);
            let common = numerator.gcd(&denominator);
            let expected = (
                numerator.try_exact_quotient(&common).unwrap(),
                denominator.try_exact_quotient(&common).unwrap(),
            );
            let reduced = ExactRational::new(numerator.clone(), denominator).unwrap();
            assert_eq!(
                (reduced.numerator, reduced.denominator),
                expected,
                "{numerator:?} / 2^{exponent}"
            );
        }
    }
}

#[test]
fn rational_equality_and_hash_use_ratio_value() {
    let from_dyadic = ExactRational::from_f64(307.5);
    let from_quotient =
        ExactRational::new(ExactDyadic::from_f64(615.0), ExactDyadic::from_f64(2.0)).unwrap();
    assert_eq!(
        from_dyadic.try_cmp(&from_quotient).unwrap(),
        Ordering::Equal
    );
    assert_eq!(from_dyadic, from_quotient);
    let mut values = std::collections::HashSet::new();
    assert!(values.insert(from_dyadic));
    assert!(!values.insert(from_quotient));
}

#[test]
fn gcd_and_exact_quotient_agree_across_the_inline_and_heap_paths() {
    // The largest prime below 2^64, so every product crosses into a second
    // or third limb.
    let prime = ExactDyadic::try_from_u128(false, 0xFFFF_FFFF_FFFF_FFC5, 0).unwrap();
    let small =
        |value: u128, exponent: i64| ExactDyadic::try_from_u128(false, value, exponent).unwrap();
    let left = prime.try_product(&small(3, 5)).unwrap();
    let right = prime.try_product(&small(5, -3)).unwrap();
    let gcd = left.gcd(&right);
    assert_eq!(
        gcd.cmp(&prime.try_product(&small(1, -3)).unwrap()),
        Ordering::Equal
    );
    assert_eq!(
        left.try_exact_quotient(&gcd).unwrap().cmp(&small(3, 8)),
        Ordering::Equal
    );
    // A three-limb operand takes the long-division path to the same gcd.
    let wide = left.try_product(&prime).unwrap();
    assert_eq!(wide.gcd(&right).cmp(&gcd), Ordering::Equal);
    assert_eq!(
        wide.try_exact_quotient(&gcd)
            .unwrap()
            .cmp(&prime.try_product(&small(3, 8)).unwrap()),
        Ordering::Equal
    );
    right.try_exact_quotient(&small(3, 0)).unwrap_err();
    let negative = small(15, 0).neg();
    assert_eq!(
        negative
            .try_exact_quotient(&small(5, 0))
            .unwrap()
            .cmp(&small(3, 0).neg()),
        Ordering::Equal
    );
}

#[test]
fn predicate_filters_preserve_signs_across_exponent_ranges() {
    use crate::geometry::numerics::product::exact_power_of_two;
    for exponent in [-600, -451, -450, -201, -200, 0, 199, 200, 449, 450, 600] {
        let scale = exact_power_of_two(exponent);
        let origin = XY::new(0.0, 0.0);
        let east = XY::new(scale, 0.0);
        let north = XY::new(0.0, scale);
        let inside = XY::new(scale * 0.25, scale * 0.25);
        assert_eq!(super::orient2d(origin, east, north), ExactSign::Positive);
        assert_eq!(
            super::incircle(origin, east, north, inside).unwrap(),
            ExactSign::Positive
        );
        if let Some(sign) = super::orient2d_float(origin, east, north) {
            assert_eq!(sign, ExactSign::Positive, "orientation exponent {exponent}");
        }
        if let Some(sign) = super::incircle_float(origin, east, north, inside) {
            assert_eq!(sign, ExactSign::Positive, "incircle exponent {exponent}");
        }
    }
}

#[test]
fn source_parameter_keeps_the_exact_ratio_after_rounding() {
    let start = XY::new(0.25, 1.000_000_000_000_000_2);
    let end = XY::new(0.25, 1.25);
    let midpoint = ExactRational::zero()
        .try_midpoint(&ExactRational::one())
        .expect("midpoint parameter");
    let rounded = midpoint
        .round_segment(start, end)
        .expect("rounded source point");
    let actual = ExactRational::from_segment(start, end, rounded).expect("source parameter");
    assert_ne!(
        actual
            .try_cmp(&midpoint)
            .expect("exact parameter comparison"),
        Ordering::Equal
    );
    let next = ExactRational::zero()
        .try_midpoint(&actual)
        .expect("child midpoint parameter");
    assert_eq!(
        next.round_segment(start, end)
            .expect("rounded child source point"),
        XY::new(0.25, 1.0625)
    );
}

#[test]
fn incircle_cocircular_underflow_positive() {
    check_cocircular_underflow(-270);
}

#[test]
fn incircle_cocircular_underflow_negative() {
    check_cocircular_underflow(-271);
}

#[test]
fn incircle_cocircular_squares_underflow_to_opposite_wrong_signs() {
    use crate::geometry::numerics::product::exact_power_of_two;
    for scale in [
        3.0 * exact_power_of_two(-270),
        13.0 * exact_power_of_two(-272),
    ] {
        let [a, b, c, d] = [
            XY::new(0.0, 0.0),
            XY::new(scale, 0.0),
            XY::new(scale, scale),
            XY::new(0.0, scale),
        ];
        assert_eq!(super::incircle_dyadic(a, b, c, d).unwrap(), ExactSign::Zero);
        if let Some(sign) = super::incircle_float(a, b, c, d) {
            assert_eq!(sign, ExactSign::Zero, "scale {scale:?}");
        }
        assert_eq!(super::incircle(a, b, c, d).unwrap(), ExactSign::Zero);
    }
}

#[test]
fn orient2d_uses_the_shared_underflow_ray_resolver_for_all_permutations() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let mu = f64::from_bits(1);
    let a = XY::new(1.0, -4096.0 * mu);
    let b = XY::new(-(exact_power_of_two(-12)), 6144.0 * mu);
    let p = XY::new(f64::from_bits(0x3FD9_9733_3333_3333), 2048.0 * mu);
    for ((left, middle, right), expected) in [
        ((a, b, p), ExactSign::Positive),
        ((a, p, b), ExactSign::Negative),
        ((b, a, p), ExactSign::Negative),
        ((b, p, a), ExactSign::Positive),
        ((p, a, b), ExactSign::Positive),
        ((p, b, a), ExactSign::Negative),
    ] {
        assert_eq!(super::orient2d(left, middle, right), expected);
        assert_eq!(
            stored_orientation_sign(left.x, left.y, middle.x, middle.y, right.x, right.y),
            expected
        );
    }
}

/// A decided stage-A result must agree with the independent exact sign.
/// Cases that decline are still checked through the complete predicate;
/// decline is conservative and is not itself a contract here.
#[test]
fn filtered_predicates_never_contradict_exact_signs() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let mu = f64::from_bits(1);
    let orientation_cases = [
        (
            "unit ccw",
            XY::new(0.0, 0.0),
            XY::new(1.0, 0.0),
            XY::new(0.0, 1.0),
            ExactSign::Positive,
        ),
        (
            "unit cw",
            XY::new(0.0, 0.0),
            XY::new(1.0, 0.0),
            XY::new(0.0, -1.0),
            ExactSign::Negative,
        ),
        (
            "unit collinear",
            XY::new(0.0, 0.0),
            XY::new(1.0, 1.0),
            XY::new(2.0, 2.0),
            ExactSign::Zero,
        ),
        (
            "one ulp",
            XY::new(0.0, 1.0),
            XY::new(1.0, f64::from_bits(0x3FF0_0000_0000_0001)),
            XY::new(2.0, 1.0),
            ExactSign::Negative,
        ),
        (
            "subnormal triangle",
            XY::new(0.0, 0.0),
            XY::new(mu, 0.0),
            XY::new(0.0, mu),
            ExactSign::Positive,
        ),
        (
            "underflow ray",
            XY::new(1.0, f64::from_bits(0x8000_0000_0000_1000)),
            XY::new(-(exact_power_of_two(-12)), f64::from_bits(0x1800)),
            XY::new(f64::from_bits(0x3FD9_9733_3333_3333), f64::from_bits(0x800)),
            ExactSign::Positive,
        ),
        (
            "overflow differences",
            XY::new(-f64::MAX, -f64::MAX),
            XY::new(f64::MAX, f64::MAX),
            XY::new(f64::MAX, -f64::MAX),
            ExactSign::Negative,
        ),
        (
            "lattice exponent spread",
            XY::new(1.0, 1.0),
            XY::new(exact_power_of_two(30), 0.0),
            XY::new(0.0, 3.0),
            ExactSign::Positive,
        ),
    ];
    for &(name, a, b, c, expected) in &orientation_cases {
        assert_eq!(dyadic_orientation_sign(a, b, c), expected, "{name}: dyadic");
        assert_eq!(
            super::orient2d(a, b, c),
            expected,
            "{name}: complete predicate"
        );
        if let Some(sign) = super::orient2d_float(a, b, c) {
            assert_eq!(sign, expected, "{name}: float filter");
        }
        if let Some(sign) = super::orient2d_lattice(a, b, c) {
            assert_eq!(sign, expected, "{name}: lattice filter");
        }
    }

    let scale = exact_power_of_two(-270);
    let incircle_cases = [
        (
            "unit inside",
            XY::new(0.0, 0.0),
            XY::new(1.0, 0.0),
            XY::new(0.0, 1.0),
            XY::new(0.25, 0.25),
            ExactSign::Positive,
        ),
        (
            "unit outside",
            XY::new(0.0, 0.0),
            XY::new(1.0, 0.0),
            XY::new(0.0, 1.0),
            XY::new(2.0, 2.0),
            ExactSign::Negative,
        ),
        (
            "unit cocircular",
            XY::new(0.0, 0.0),
            XY::new(1.0, 0.0),
            XY::new(1.0, 1.0),
            XY::new(0.0, 1.0),
            ExactSign::Zero,
        ),
        (
            "tiny cocircular",
            XY::new(5.0 * scale, 0.0),
            XY::new(3.0 * scale, 4.0 * scale),
            XY::new(0.0, 5.0 * scale),
            XY::new(-3.0 * scale, 4.0 * scale),
            ExactSign::Zero,
        ),
        (
            "extreme overflow",
            XY::new(-1e308, 0.0),
            XY::new(0.0, 1e308),
            XY::new(1e308, 0.0),
            XY::new(0.0, 0.0),
            ExactSign::Negative,
        ),
        (
            "half lattice inside",
            XY::new(0.0, 0.0),
            XY::new(2.0, 0.0),
            XY::new(0.0, 2.0),
            XY::new(0.5, 0.5),
            ExactSign::Positive,
        ),
        (
            "non-lattice",
            XY::new(0.1, 0.2),
            XY::new(1.0, 0.0),
            XY::new(1.0, 1.0),
            XY::new(0.0, 1.0),
            ExactSign::Negative,
        ),
    ];
    for &(name, a, b, c, d, expected) in &incircle_cases {
        assert_eq!(
            super::incircle_dyadic(a, b, c, d).unwrap(),
            expected,
            "{name}: dyadic"
        );
        assert_eq!(
            super::incircle(a, b, c, d).unwrap(),
            expected,
            "{name}: complete predicate"
        );
        if let Some(sign) = super::incircle_float(a, b, c, d) {
            assert_eq!(sign, expected, "{name}: float filter");
        }
        if let Some(sign) = super::incircle_lattice(a, b, c, d) {
            assert_eq!(sign, expected, "{name}: lattice filter");
        }
    }
}

#[test]
fn exact_signs_cover_full_binary64_range() {
    assert_eq!(
        orient2d(
            XY::new(0.0, 0.0),
            XY::new(f64::MAX, 0.0),
            XY::new(0.0, f64::from_bits(1))
        ),
        ExactSign::Positive
    );
    assert_eq!(
        incircle(
            XY::new(-1e308, 0.0),
            XY::new(0.0, 1e308),
            XY::new(1e308, 0.0),
            XY::new(0.0, 0.0)
        )
        .unwrap(),
        ExactSign::Negative
    );
}

#[rstest::rstest]
#[case::exactdyadic_zero(ExactDyadic::zero())]
#[case::exactdyadic_one(ExactDyadic::one())]
fn ratio_rounding_rejects_zero_denominators(#[case] numerator: ExactDyadic) {
    let error = round_ratio(&numerator, &ExactDyadic::zero()).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactRatioZeroDenominator)
    ));
}

#[test]
fn ratio_rounding_is_ties_to_even_and_handles_extremes() {
    let one = ExactDyadic::one();
    let halfway = ExactDyadic::from_f64(1.0)
        .add(ExactDyadic::from_f64(f64::from_bits(1.0_f64.to_bits() + 1)));
    let two = ExactDyadic::from_f64(2.0);
    assert_eq!(
        round_ratio(&halfway, &two).unwrap().to_bits(),
        1.0_f64.to_bits()
    );
    assert_eq!(
        round_ratio(&one, &ExactDyadic::from_parts(false, vec![1], 1074))
            .unwrap()
            .to_bits(),
        1
    );
    assert_eq!(
        round_ratio(&ExactDyadic::from_f64(f64::MAX), &one)
            .unwrap()
            .to_bits(),
        f64::MAX.to_bits()
    );
    let below_overflow =
        ExactDyadic::from_f64(f64::MAX).add(ExactDyadic::from_parts(false, vec![1], 969));
    let overflow_tie =
        ExactDyadic::from_f64(f64::MAX).add(ExactDyadic::from_parts(false, vec![1], 970));
    assert_eq!(
        round_ratio(&below_overflow, &one).unwrap().to_bits(),
        f64::MAX.to_bits()
    );
    round_ratio(&overflow_tie, &one).unwrap_err();
}

#[test]
fn dyadics_are_odd_low_canonical_and_exponent_capacity_is_live() {
    let values = [
        ExactDyadic::from_f64(2.0),
        ExactDyadic::from_f64(f64::from_bits(1)),
        ExactDyadic::from_f64(1.5).add(ExactDyadic::from_f64(0.5)),
        ExactDyadic::from_f64(8.0).subtract(ExactDyadic::from_f64(2.0)),
        ExactDyadic::from_f64(6.0).product(&ExactDyadic::from_f64(10.0)),
    ];
    assert!(
        values
            .iter()
            .all(|value| value.is_zero() || value.limbs[0] & 1 == 1)
    );

    let beyond_i32 = ExactDyadic::from_parts(false, vec![1], i64::from(i32::MAX) + 1);
    assert_eq!(beyond_i32.square().exponent, 2 * (i64::from(i32::MAX) + 1));
}

#[test]
fn inline_and_spilled_dyadics_have_one_numeric_identity() {
    let inline = ExactDyadic::from_parts(false, vec![1, 3], -211);
    let spilled = ExactDyadic {
        negative: false,
        limbs: CompactLimbs::Heap(vec![1, 3]),
        exponent: -211,
    };
    assert_eq!(inline, spilled);
    assert_eq!(inline.cmp(&spilled), Ordering::Equal);
    assert_eq!(inline.cmp_magnitude(&spilled), Ordering::Equal);
    assert_eq!(
        round_ratio(&inline, &ExactDyadic::one()).unwrap().to_bits(),
        round_ratio(&spilled, &ExactDyadic::one())
            .unwrap()
            .to_bits()
    );
}

#[test]
fn extreme_exponents_compare_without_scanning_implicit_zero_limbs() {
    let high = ExactDyadic::from_parts(false, vec![1], i64::MAX);
    let low = ExactDyadic::from_parts(false, vec![1], i64::MIN);
    assert_eq!(high.cmp(&low), Ordering::Greater);
    assert_eq!(low.cmp_magnitude(&high), Ordering::Less);
    assert_eq!(high.clone().neg().cmp(&low.clone().neg()), Ordering::Less);

    let higher_at_the_same_top_bit =
        ExactDyadic::from_parts(false, vec![1, 1 << 63], i64::MAX - 127);
    assert_eq!(high.cmp(&higher_at_the_same_top_bit), Ordering::Less);
    assert_eq!(high.cmp_magnitude_scaled(&low, i64::MAX), Ordering::Greater);
    assert_eq!(low.cmp_magnitude_scaled(&high, i64::MAX), Ordering::Less);
    let extended = ExactDyadic::from_parts(false, vec![1, 1], i64::MAX);
    assert_eq!(extended.cmp_magnitude_scaled(&high, 64), Ordering::Greater);
}

#[test]
fn exact_dyadic_arithmetic_rejects_unrepresentable_exponents() {
    let high = ExactDyadic::from_parts(false, vec![1], i64::MAX);
    let two = ExactDyadic::from_parts(false, vec![1], 1);
    for result in [
        high.try_product(&two),
        ExactDyadic::try_add_refs(&high, &high),
        ExactDyadic::try_from_u128(false, 2, i64::MAX),
    ] {
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::Resource(ResourceError::SizeOverflow)
        ));
    }
    assert!(matches!(
        ExactRational::new(high, ExactDyadic::from_parts(false, vec![1], i64::MIN))
            .unwrap_err()
            .kind(),
        ErrorKind::Resource(ResourceError::SizeOverflow)
    ));
    assert!(matches!(
        ExactRational::new(
            ExactDyadic::from_parts(false, vec![1], i64::MIN),
            ExactDyadic::from_parts(false, vec![1], i64::MAX)
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Resource(ResourceError::SizeOverflow)
    ));

    let mut untrimmed = ExactDyadic {
        negative: false,
        limbs: CompactLimbs::from_vec(vec![0, 1]),
        exponent: i64::MAX,
    };
    assert!(matches!(
        untrimmed.try_trim().unwrap_err().kind(),
        ErrorKind::Resource(ResourceError::SizeOverflow)
    ));
    assert_eq!(untrimmed.exponent, i64::MAX);
    assert_eq!(untrimmed.limbs.as_slice(), &[0, 1]);
}

#[test]
fn ordinary_one_limb_try_arithmetic_does_not_allocate() {
    use crate::test_support::allocation_count;

    let (_, allocations) = allocation_count(|| {
        let left = ExactDyadic::from_f64(3.0);
        let right = ExactDyadic::from_f64(5.0);
        let sum = left.try_add(right).unwrap();
        sum.try_product(&ExactDyadic::one()).unwrap()
    });
    assert_eq!(allocations, 0);
}

#[test]
fn exact_sum_spills_only_when_an_inline_result_carries() {
    use crate::test_support::allocation_count;

    let left = ExactDyadic::from_parts(false, vec![u64::MAX, u64::MAX - 1], 0);
    let no_carry = ExactDyadic::from_parts(false, vec![1], 0);
    let (sum, allocations) = allocation_count(|| left.clone().try_add(no_carry.clone()).unwrap());
    assert_eq!(allocations, 0);
    assert_eq!(sum, ExactDyadic::from_parts(false, vec![u64::MAX], 64));

    let carry = ExactDyadic::from_parts(false, vec![1, 1], 0);
    let (sum, allocations) = allocation_count(|| left.clone().try_add(carry.clone()).unwrap());
    assert_eq!(allocations, 1);
    assert_eq!(sum, ExactDyadic::from_parts(false, vec![1], 128));
}

#[test]
fn trimmed_heap_product_clones_without_a_second_allocation() {
    use crate::test_support::allocation_count;

    let left = ExactDyadic::from_parts(false, vec![1, 1], 0);
    let right = ExactDyadic::from_parts(false, vec![3], 0);
    let (product, allocations) = allocation_count(|| left.try_product(&right).unwrap());
    assert_eq!(allocations, 1);
    assert_eq!(product, ExactDyadic::from_parts(false, vec![3, 3], 0));

    let (clone, allocations) = allocation_count(|| product.clone());
    assert_eq!(allocations, 0);
    assert_eq!(clone, product);
}

#[test]
fn exact_rational_arithmetic_reduces_and_rounds() {
    let third = ExactRational::new(ExactDyadic::one(), ExactDyadic::from_f64(3.0)).unwrap();
    let sixth = ExactRational::new(ExactDyadic::one(), ExactDyadic::from_f64(6.0)).unwrap();
    let half = third.try_add(&sixth).unwrap();
    assert_eq!(half.numerator, ExactDyadic::one());
    assert_eq!(half.denominator, ExactDyadic::from_f64(2.0));
    assert_eq!(half.sign(), ExactSign::Positive);
    assert!(!half.is_zero());
    assert_eq!(half.round().unwrap().to_bits(), 0.5_f64.to_bits());
    assert_eq!(
        half.outward_bounds().unwrap(),
        (0.5_f64, 0.5_f64),
        "an exactly representable ratio has a singleton outward interval"
    );
    assert_eq!(
        half.try_square().unwrap().round().unwrap().to_bits(),
        0.25_f64.to_bits()
    );

    let zero = half.try_subtract(&half).unwrap();
    assert!(zero.is_zero());
    assert_eq!(zero, ExactRational::zero());
}

#[test]
fn denominator_one_rational_arithmetic_avoids_cross_product_allocations() {
    use crate::test_support::allocation_count;

    let exponent = i64::MIN + 1024;
    let positive = ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1, 3], exponent));
    let negative = ExactRational::from_dyadic(ExactDyadic::from_parts(true, vec![1, 3], exponent));
    let zero = ExactRational::zero();
    let expected_subtraction =
        ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1, 3], exponent + 1));

    let (sum, allocations) = allocation_count(|| positive.try_add(&negative));
    assert_eq!(allocations, 0);
    assert_eq!(sum.unwrap(), zero);

    let (difference, allocations) = allocation_count(|| positive.try_subtract(&negative));
    assert_eq!(allocations, 0);
    assert_eq!(difference.unwrap(), expected_subtraction);

    let product_left =
        ExactRational::from_dyadic(ExactDyadic::from_parts(true, vec![3], i64::MAX / 4));
    let product_right =
        ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![5], i64::MAX / 4));
    let expected_product =
        ExactRational::from_dyadic(ExactDyadic::from_parts(true, vec![15], (i64::MAX / 4) * 2));
    let (product, allocations) = allocation_count(|| product_left.try_product(&product_right));
    assert_eq!(allocations, 0);
    assert_eq!(product.unwrap(), expected_product);

    let square_value =
        ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![3], i64::MIN / 4));
    let expected_square =
        ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![9], (i64::MIN / 4) * 2));
    let (square, allocations) = allocation_count(|| square_value.try_square());
    assert_eq!(allocations, 0);
    assert_eq!(square.unwrap(), expected_square);
}

#[test]
fn denominator_one_rational_heap_results_allocate_once() {
    use crate::test_support::allocation_count;

    let left =
        ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![u64::MAX, u64::MAX], 0));
    let right = ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1, 1], 0));
    let expected = ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1, 1], 64));

    let (result, allocations) = allocation_count(|| left.try_add(&right));
    assert_eq!(allocations, 1);
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn exact_rational_quotient_rejects_zero_and_negative_squares() {
    let error = ExactRational::one()
        .try_quotient(&ExactRational::zero())
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactRatioZeroDenominator)
    ));

    let error = ExactRational::from_f64(-1.0).round_sqrt().unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));
}

#[rstest::rstest]
#[case::numerator_1_0(1.0, -1.0, -1.0)]
#[case::negative_numerator_1_0(-1.0, 1.0, -1.0)]
#[case::negative_numerator_1_0_12330c75(-1.0, -1.0, 1.0)]
#[case::numerator_8_0(8.0, -1.0, -8.0)]
#[case::negative_numerator_8_0(-8.0, 1.0, -8.0)]
#[case::negative_numerator_8_0_40d79552(-8.0, -1.0, 8.0)]
#[case::numerator_1_0_c8d7e2d7(1.0, 8.0, 0.125)]
#[case::negative_numerator_1_0_ef217ccf(-1.0, 8.0, -0.125)]
#[case::numerator_1_0_eb946231(1.0, -8.0, -0.125)]
#[case::negative_numerator_1_0_a6b3a544(-1.0, -8.0, 0.125)]
#[case::numerator_1_0_28974f2e(1.0, 0.125, 8.0)]
#[case::negative_numerator_1_0_fbf0260b(-1.0, -0.125, 8.0)]
fn exact_dyadic_power_two_quotients_preserve_signs(
    #[case] numerator: f64,
    #[case] divisor: f64,
    #[case] expected: f64,
) {
    let actual = ExactDyadic::from_f64(numerator)
        .try_exact_quotient(&ExactDyadic::from_f64(divisor))
        .unwrap();
    assert_eq!(
        actual,
        ExactDyadic::from_f64(expected),
        "{numerator:?} / {divisor:?}"
    );
}

#[test]
fn exact_dyadic_power_two_quotients_preserve_extreme_exponents() {
    let upper = ExactDyadic::from_parts(false, vec![1], i64::MAX);
    let upper_divisor = ExactDyadic::from_parts(true, vec![1], i64::MAX - 3);
    assert_eq!(
        upper.try_exact_quotient(&upper_divisor).unwrap(),
        ExactDyadic::from_parts(true, vec![1], 3)
    );

    let lower = ExactDyadic::from_parts(true, vec![1], i64::MIN + 3);
    let lower_divisor = ExactDyadic::from_parts(false, vec![1], i64::MIN);
    assert_eq!(
        lower.try_exact_quotient(&lower_divisor).unwrap(),
        ExactDyadic::from_parts(true, vec![1], 3)
    );

    let ratio = ExactRational::from_dyadic(upper);
    let divisor = ExactRational::from_dyadic(ExactDyadic::from_parts(false, vec![1], i64::MAX - 3));
    let quotient = ratio.try_quotient(&divisor).unwrap();
    assert_eq!(
        quotient,
        ExactRational::new(
            ExactDyadic::from_parts(false, vec![1], 3),
            ExactDyadic::one()
        )
        .unwrap()
    );
}

#[rstest::rstest]
#[case::exactdyadic_one(ExactDyadic::one(), ExactDyadic::from_f64(3.0))]
#[case::exactdyadic_from_f64_f64_from_bits_1(
    ExactDyadic::from_f64(f64::from_bits(1)),
    ExactDyadic::from_f64(2.0)
)]
#[case::exactdyadic_from_f64_f64_max(ExactDyadic::from_f64(f64::MAX), ExactDyadic::from_f64(3.0))]
#[case::exactdyadic_from_f64_1_0(ExactDyadic::from_f64(-1.0), ExactDyadic::from_f64(10.0))]
fn certified_ratio_proposal_matches_complete_binary_search(
    #[case] numerator: ExactDyadic,
    #[case] denominator: ExactDyadic,
) {
    assert_eq!(
        round_ratio(&numerator, &denominator).unwrap().to_bits(),
        round_ratio_binary(&numerator, &denominator)
            .unwrap()
            .to_bits(),
    );
}

#[test]
fn exact_zero_ratio_is_canonical_positive_zero() {
    let zero = ExactDyadic::zero();
    let negative_denominator = ExactDyadic::from_f64(-3.0);
    assert_eq!(
        round_ratio(&zero, &negative_denominator).unwrap().to_bits(),
        0.0_f64.to_bits()
    );
}

#[test]
fn exact_singleton_and_equal_magnitude_brackets_preserve_sign() {
    let numerator = ExactDyadic::from_f64(3.0);
    let denominator = ExactDyadic::from_f64(-3.0);
    assert_eq!(round_ratio(&numerator, &denominator).unwrap(), -1.0);
    assert_eq!(round_ratio(&denominator, &numerator).unwrap(), -1.0);
    assert_eq!(round_ratio(&numerator.neg(), &denominator).unwrap(), 1.0);

    let half = ExactDyadic::from_f64(1.0);
    let two = ExactDyadic::from_f64(2.0);
    assert_eq!(round_ratio(&half, &two).unwrap(), 0.5);
    assert_eq!(round_ratio(&half.neg(), &two).unwrap(), -0.5);
}

#[test]
fn single_limb_fast_paths_match_the_general_kernels() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let values = [
        0.1,
        -0.3,
        1.0,
        -1.0,
        1.5e300,
        -2.0e-300,
        3.0,
        12345.678,
        -0.000_123,
        f64::from_bits(1),
        exact_power_of_two(62),
        -(exact_power_of_two(63)) - 2048.0,
    ];
    for &left in &values {
        for &right in &values {
            let (l, r) = (ExactDyadic::from_f64(left), ExactDyadic::from_f64(right));
            assert_eq!(
                l.try_product(&r).unwrap(),
                l.product(&r),
                "{left} * {right}"
            );
            assert_eq!(
                l.clone().try_add(r.clone()).unwrap(),
                l.clone().add(r.clone()),
                "{left} + {right}"
            );
            assert_eq!(
                l.cmp(&r),
                left.partial_cmp(&right).unwrap(),
                "{left} <> {right}"
            );
            let (lp, rp) = (l.product(&l), r.product(&l).product(&r));
            let difference = lp.clone().subtract(rp.clone()).sign();
            assert_eq!(
                lp.cmp(&rp),
                difference.as_i8().cmp(&0),
                "{left}^2 <> {left}{right}^2"
            );
            assert_eq!(
                l.clone().try_subtract(r.clone()).unwrap(),
                l.subtract(r),
                "{left} - {right}"
            );
        }
    }
}

#[test]
fn filtered_ratio_order_matches_the_exact_products() {
    use crate::geometry::numerics::product::exact_power_of_two;
    type Ratio = (ExactDyadic, ExactDyadic);
    let value = |x: f64| ExactDyadic::from_f64(x);
    let tail = |exponent| ExactDyadic::from_parts(false, vec![1], exponent);
    let near_one = value(1.0).add(tail(-100));
    let under_two = value(2.0).subtract(tail(-60));
    let cases: [(Ratio, Ratio, bool); 12] = [
        ((value(1.0), value(3.0)), (value(2.0), value(6.0)), false),
        ((value(-1.0), value(3.0)), (value(1.0), value(-3.0)), false),
        (
            (value(1.0), value(1.0)),
            (value(1.0_f64.next_up()), value(1.0)),
            false,
        ),
        ((near_one, value(1.0)), (value(1.0), value(1.0)), false),
        ((under_two, value(1.0)), (value(1.0), value(0.5)), false),
        ((value(1.0), value(3.0)), (value(1.0), value(2.0)), true),
        (
            (value(exact_power_of_two(1000)), value(1.0)),
            (value(exact_power_of_two(-1000)), value(1.0)),
            true,
        ),
        ((value(-5.0), value(2.0)), (value(3.0), value(7.0)), true),
        (
            (ExactDyadic::zero(), value(-4.0)),
            (value(-1.0), value(9.0)),
            true,
        ),
        (
            (ExactDyadic::zero(), value(3.0)),
            (ExactDyadic::zero(), value(-8.0)),
            true,
        ),
        ((value(-7.0), value(-2.0)), (value(3.0), value(1.0)), true),
        ((value(-3.0), value(1.0)), (value(-7.0), value(2.0)), true),
    ];
    for ((an, ad), (bn, bd), decided) in cases {
        let exact = exact_ratio_order(&an, &ad, &bn, &bd).unwrap();
        let filtered = ratio_order_filter(&an, &ad, &bn, &bd);
        assert!(
            filtered.is_none_or(|order| order == exact),
            "{an:?}/{ad:?} vs {bn:?}/{bd:?}"
        );
        assert_eq!(
            filtered.is_some(),
            decided,
            "{an:?}/{ad:?} vs {bn:?}/{bd:?}"
        );
        assert_eq!(compare_ratios(&an, &ad, &bn, &bd).unwrap(), exact);
        assert_eq!(compare_ratios(&bn, &bd, &an, &ad).unwrap(), exact.reverse());
    }
}

#[test]
fn ratio_bounds_contain_adversarial_exact_ratios() {
    fn assert_contains(name: &str, numerator: &ExactDyadic, denominator: &ExactDyadic) {
        assert_bounds_contain(name, numerator, denominator, outward_ratio_bounds);
    }

    fn assert_bounds_contain(
        name: &str,
        numerator: &ExactDyadic,
        denominator: &ExactDyadic,
        bounds: fn(&ExactDyadic, &ExactDyadic) -> Result<(f64, f64)>,
    ) {
        let (lower, upper) = bounds(numerator, denominator).unwrap();
        assert!(
            lower <= upper,
            "{name}: invalid interval [{lower:?}, {upper:?}]"
        );
        let one = ExactDyadic::one();
        let lower_exact = ExactDyadic::from_f64(lower);
        let upper_exact = ExactDyadic::from_f64(upper);
        assert_ne!(
            compare_ratios(numerator, denominator, &lower_exact, &one).unwrap(),
            Ordering::Less,
            "{name}: ratio below lower bound"
        );
        assert_ne!(
            compare_ratios(numerator, denominator, &upper_exact, &one).unwrap(),
            Ordering::Greater,
            "{name}: ratio above upper bound"
        );
    }

    let one = ExactDyadic::one();
    let half_ulp_at_one = ExactDyadic::from_parts(false, vec![1], -53);
    let below_tie = one.clone().subtract(half_ulp_at_one.clone());
    let above_tie = one.clone().add(half_ulp_at_one);
    let below_minimum_subnormal = ExactDyadic::from_parts(false, vec![1], -1075);
    let minimum_subnormal = ExactDyadic::from_f64(f64::from_bits(1));

    for (name, numerator, denominator) in [
        (
            "negative nonrepresentable",
            ExactDyadic::from_f64(-1.0),
            ExactDyadic::from_f64(3.0),
        ),
        (
            "negative exactly representable",
            ExactDyadic::from_f64(-1.5),
            one.clone(),
        ),
        (
            "negative zero",
            ExactDyadic::zero(),
            ExactDyadic::from_f64(-3.0),
        ),
        (
            "positive exactly representable",
            ExactDyadic::from_f64(1.5),
            one.clone(),
        ),
        ("below one midpoint", below_tie, one.clone()),
        ("above one midpoint", above_tie, one.clone()),
        ("minimum subnormal", minimum_subnormal, one.clone()),
        (
            "below minimum subnormal",
            below_minimum_subnormal,
            one.clone(),
        ),
        (
            "bits below the leading window",
            ExactDyadic::from_parts(false, vec![u64::MAX, u64::MAX, u64::MAX, 1], -150),
            ExactDyadic::from_parts(false, vec![1, 0, 3], 17),
        ),
        (
            "negative multi-limb over multi-limb",
            ExactDyadic::from_parts(true, vec![0x8000_0000_0000_0001, 7, 1], -64),
            ExactDyadic::from_parts(false, vec![u64::MAX, 5], 3),
        ),
        (
            "just under a power of two",
            one.clone()
                .add(one.clone())
                .subtract(ExactDyadic::from_parts(false, vec![1], -120)),
            one.clone(),
        ),
        (
            "just above a power of two",
            one.add(ExactDyadic::from_parts(false, vec![1], -120)),
            ExactDyadic::from_parts(false, vec![1, 1], -70),
        ),
    ] {
        assert_contains(name, &numerator, &denominator);
    }
}

#[test]
fn adjacent_cells_and_midpoint_ties_keep_nearest_even_policy() {
    let one_f64: f64 = 1.0;
    let one_up_f64 = one_f64.next_up();
    let one_up_twice_f64 = one_up_f64.next_up();
    let one = ExactDyadic::from_f64(one_f64);
    let one_up = ExactDyadic::from_f64(one_up_f64);
    let midpoint = one
        .clone()
        .add(one_up.clone())
        .product(&ExactDyadic::from_f64(0.5));
    let quarter_gap = one_up
        .clone()
        .subtract(one)
        .product(&ExactDyadic::from_f64(0.25));
    assert_eq!(
        round_ratio(
            &midpoint.clone().subtract(quarter_gap.clone()),
            &ExactDyadic::one()
        )
        .unwrap()
        .to_bits(),
        1.0_f64.to_bits()
    );
    assert_eq!(
        round_ratio(&midpoint.clone().add(quarter_gap), &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        one_up_f64.to_bits()
    );
    assert_eq!(
        round_ratio(&midpoint, &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        1.0_f64.to_bits()
    );
    assert_eq!(
        round_ratio(&midpoint.neg(), &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        (-1.0_f64).to_bits()
    );

    let odd_lower = one_up;
    let even_upper_f64 = one_up_twice_f64;
    let even_upper = ExactDyadic::from_f64(even_upper_f64);
    let odd_midpoint = odd_lower
        .add(even_upper)
        .product(&ExactDyadic::from_f64(0.5));
    assert_eq!(
        round_ratio(&odd_midpoint, &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        even_upper_f64.to_bits()
    );
    assert_eq!(
        round_ratio(&odd_midpoint.neg(), &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        (-even_upper_f64).to_bits()
    );
}

#[test]
fn ratio_rounding_preserves_signs_subnormals_and_power_boundaries() {
    let min_subnormal = ExactDyadic::from_f64(f64::from_bits(1));
    assert_eq!(
        round_ratio(&min_subnormal, &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        1
    );
    let below_minimum = ExactDyadic::from_parts(false, vec![1], -1075);
    assert_eq!(
        round_ratio(&below_minimum, &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        0
    );
    assert_eq!(
        round_ratio(&below_minimum.neg(), &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        (-0.0_f64).to_bits()
    );

    let predecessor = 1.0_f64.next_down();
    let lower_midpoint = ExactDyadic::from_f64(predecessor)
        .add(ExactDyadic::from_f64(1.0))
        .product(&ExactDyadic::from_f64(0.5));
    assert_eq!(
        round_ratio(&lower_midpoint, &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        1.0_f64.to_bits()
    );

    for (numerator, denominator) in [(5.0, 3.0), (-5.0, 3.0), (5.0, -3.0), (-5.0, -3.0)] {
        let numerator = ExactDyadic::from_f64(numerator);
        let denominator = ExactDyadic::from_f64(denominator);
        assert_eq!(
            round_ratio(&numerator, &denominator).unwrap().to_bits(),
            round_ratio_binary(&numerator, &denominator)
                .unwrap()
                .to_bits()
        );
    }
}

#[test]
fn ratio_rounding_matches_literal_integer_boundary_answers() {
    // Integer significands encode the exact boundaries without using the
    // arithmetic or midpoint construction whose rounding this test checks.
    let cases = [
        ("below half-minsubnormal", 3, -1077, Some(0)),
        ("half-minsubnormal", 1, -1075, Some(0)),
        ("above half-minsubnormal", 5, -1077, Some(1)),
        (
            "below normal-transition midpoint",
            0x003F_FFFF_FFFF_FFFD,
            -1076,
            Some(0x000F_FFFF_FFFF_FFFF),
        ),
        (
            "normal-transition midpoint",
            0x001F_FFFF_FFFF_FFFF,
            -1075,
            Some(0x0010_0000_0000_0000),
        ),
        (
            "above normal-transition midpoint",
            0x003F_FFFF_FFFF_FFFF,
            -1076,
            Some(0x0010_0000_0000_0000),
        ),
        (
            "below even-lower midpoint",
            0x0040_0000_0000_0001,
            -54,
            Some(0x3FF0_0000_0000_0000),
        ),
        (
            "even-lower midpoint",
            0x0020_0000_0000_0001,
            -53,
            Some(0x3FF0_0000_0000_0000),
        ),
        (
            "above even-lower midpoint",
            0x0040_0000_0000_0003,
            -54,
            Some(0x3FF0_0000_0000_0001),
        ),
        (
            "below odd-lower midpoint",
            0x0040_0000_0000_0005,
            -54,
            Some(0x3FF0_0000_0000_0001),
        ),
        (
            "odd-lower midpoint",
            0x0020_0000_0000_0003,
            -53,
            Some(0x3FF0_0000_0000_0002),
        ),
        (
            "above odd-lower midpoint",
            0x0040_0000_0000_0007,
            -54,
            Some(0x3FF0_0000_0000_0002),
        ),
        (
            "below overflow midpoint",
            0x007F_FFFF_FFFF_FFFD,
            969,
            Some(0x7FEF_FFFF_FFFF_FFFF),
        ),
        ("overflow midpoint", 0x003F_FFFF_FFFF_FFFF, 970, None),
        ("above overflow midpoint", 0x007F_FFFF_FFFF_FFFF, 969, None),
    ];
    for (name, significand, exponent, expected_bits) in cases {
        for shift in [-65_i64, 0, 65] {
            for negative in [false, true] {
                let numerator = ExactDyadic::from_parts(
                    negative,
                    vec![significand],
                    i64::from(exponent).checked_add(shift).unwrap(),
                );
                let denominator = ExactDyadic::from_parts(false, vec![1], shift);
                let actual = round_ratio(&numerator, &denominator);
                match expected_bits {
                    Some(bits) => {
                        let sign = if negative { 0x8000_0000_0000_0000 } else { 0 };
                        assert_eq!(
                            actual.unwrap().to_bits(),
                            bits | sign,
                            "{name}, negative={negative}, exponent shift={shift}"
                        );
                    },
                    None => {
                        assert!(
                            matches!(
                                actual.unwrap_err().kind(),
                                crate::error::ErrorKind::Geometry(
                                    GeometryErrorKind::ExactCoordinateOutOfRange
                                )
                            ),
                            "{name}, negative={negative}, exponent shift={shift}"
                        );
                    },
                }
            }
        }
    }
    assert_eq!(
        round_ratio(
            &ExactDyadic::zero(),
            &ExactDyadic::from_parts(true, vec![3], -65)
        )
        .unwrap()
        .to_bits(),
        0
    );
}

#[test]
fn ratio_rounding_matches_binary_reference_on_curated_bit_patterns() {
    let patterns = [
        0x0000_0000_0000_0000,
        0x0000_0000_0000_0001,
        0x000F_FFFF_FFFF_FFFF,
        0x0010_0000_0000_0000,
        0x3FE0_0000_0000_0000,
        0x3FF0_0000_0000_0000,
        0x3FF0_0000_0000_0001,
        0x4000_0000_0000_0000,
        0x7FE0_0000_0000_0000,
        0x7FEF_FFFF_FFFF_FFFF,
    ];
    for &numerator_bits in &patterns {
        for &denominator_bits in &patterns {
            for (numerator_bits, denominator_bits) in [
                (numerator_bits, denominator_bits),
                (numerator_bits | (1_u64 << 63), denominator_bits),
                (numerator_bits, denominator_bits | (1_u64 << 63)),
            ] {
                let numerator = ExactDyadic::from_f64(f64::from_bits(numerator_bits));
                let denominator = ExactDyadic::from_f64(f64::from_bits(denominator_bits));
                if denominator.is_zero() {
                    continue;
                }
                let actual = round_ratio(&numerator, &denominator);
                let reference = round_ratio_binary(&numerator, &denominator);
                match (actual, reference) {
                    (Ok(actual), Ok(reference)) => {
                        assert_eq!(actual.to_bits(), reference.to_bits());
                    },
                    (Err(_), Err(_)) => {},
                    (actual, reference) => {
                        panic!("fast and binary ratio paths disagree: {actual:?} vs {reference:?}");
                    },
                }
            }
        }
    }
}

#[rstest::rstest]
#[case::exactdyadic_from_f64_f64_from_bits_1(
    ExactDyadic::from_f64(f64::from_bits(1)),
    ExactDyadic::one()
)]
#[case::exactdyadic_from_f64_f64_max(ExactDyadic::from_f64(f64::MAX), ExactDyadic::one())]
#[case::exactdyadic_one(ExactDyadic::one(), ExactDyadic::from_f64(f64::MAX))]
#[case::exactdyadic_from_parts_false_vec_1_u64_max_3_7_1_211(ExactDyadic::from_parts(false, vec![1, u64::MAX, 3, 7, 1], -211), ExactDyadic::from_f64(3.0))]
#[case::exactdyadic_from_parts_true_vec_1_0_0_0_u64_max_900(ExactDyadic::from_parts(true, vec![1, 0, 0, 0, u64::MAX], 900), ExactDyadic::one())]
fn ratio_intervals_contain_extreme_exact_values(
    #[case] numerator: ExactDyadic,
    #[case] denominator: ExactDyadic,
) {
    verify_interval(
        &numerator,
        &denominator,
        ratio_interval(&numerator, &denominator),
    );
}

#[test]
fn filtered_predicates_agree_with_exact_and_cover_subnormals() {
    let tiny = f64::from_bits(1);
    let a = XY::new(0.0, 0.0);
    let b = XY::new(tiny, 0.0);
    let c = XY::new(1.0e-300, 1.0e-300);
    assert_eq!(orient2d(a, b, c), ExactSign::Positive);

    let ordinary = [
        XY::new(-3.0, 2.0),
        XY::new(5.0, -1.0),
        XY::new(4.0, 7.0),
        XY::new(-2.0, -6.0),
    ];
    assert_eq!(
        stored_orientation_sign(
            ordinary[0].x,
            ordinary[0].y,
            ordinary[1].x,
            ordinary[1].y,
            ordinary[2].x,
            ordinary[2].y,
        ),
        ExactSign::Positive
    );
    let filtered = incircle_filtered(ordinary[0], ordinary[1], ordinary[2], ordinary[3])
        .expect("ordinary incircle should hit the interval filter");
    assert_eq!(
        filtered,
        incircle_dyadic(ordinary[0], ordinary[1], ordinary[2], ordinary[3]).unwrap()
    );
}
