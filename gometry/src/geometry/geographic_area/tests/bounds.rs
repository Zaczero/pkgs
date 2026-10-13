#[test]
fn positive_bound_decomposes_binary64_extrema_exactly() {
    let cases = [
        (1_u64, 0x3FF0_0000_0000_0000_u64, -1074),
        (0x000F_FFFF_FFFF_FFFF, 0x3FFF_FFFF_FFFF_FFFE, -1023),
        (0x0010_0000_0000_0000, 0x3FF0_0000_0000_0000, -1022),
        (0x7FEF_FFFF_FFFF_FFFF, 0x3FFF_FFFF_FFFF_FFFF, 1023),
    ];
    for (input_bits, mantissa_bits, exponent) in cases {
        let bound = PositiveBound::from_f64(f64::from_bits(input_bits)).unwrap();
        assert_eq!(bound.mantissa.to_bits(), mantissa_bits);
        assert_eq!(bound.exponent, exponent);
    }

    assert!(PositiveBound::from_f64(-1.0).is_none());
    assert!(PositiveBound::from_f64(f64::INFINITY).is_none());
    assert!(PositiveBound::from_f64(f64::NAN).is_none());
    assert!(PositiveBound::from_f64(-0.0).unwrap().is_zero());
}

#[test]
fn positive_bound_parts_keep_exact_normalization_and_exponent_failures() {
    for (mantissa, exponent, expected_mantissa, expected_exponent) in [
        (8.0, -3, 1.0_f64.to_bits(), 0),
        (0.25, 2, 1.0_f64.to_bits(), 0),
        (MIN_SUBNORMAL, 1074, 1.0_f64.to_bits(), 0),
        (f64::MAX, -1023, 0x3FFF_FFFF_FFFF_FFFF, 0),
    ] {
        let bound = PositiveBound::from_parts(mantissa, exponent).unwrap();
        assert_eq!(bound.mantissa.to_bits(), expected_mantissa);
        assert_eq!(bound.exponent, expected_exponent);
    }

    assert!(PositiveBound::from_parts(-0.0, i32::MAX).unwrap().is_zero());
    assert!(PositiveBound::from_parts(2.0, i32::MAX).is_none());
    assert!(PositiveBound::from_parts(0.5, i32::MIN).is_none());
    assert!(PositiveBound::from_parts(-1.0, 0).is_none());
    assert!(PositiveBound::from_parts(f64::INFINITY, 0).is_none());
}

#[test]
fn positive_bound_upward_arithmetic_covers_exact_subnormal_targets() {
    let one = PositiveBound::from_f64(1.0).unwrap();
    let one_product = one.multiply_upper(one).unwrap().to_exact().unwrap();
    assert!(one_product.cmp(&ExactDyadic::one()).is_gt());

    let smallest = PositiveBound::from_f64(MIN_SUBNORMAL).unwrap();
    let tiny_product = smallest
        .multiply_upper(smallest)
        .unwrap()
        .to_exact()
        .unwrap();
    let exact_tiny_product = ExactDyadic::try_from_u128(false, 1, -2148).unwrap();
    assert!(tiny_product.cmp(&exact_tiny_product).is_gt());

    let one_plus_smallest = one.add_upper(smallest).unwrap().to_exact().unwrap();
    let exact_sum = ExactDyadic::one()
        .try_add(ExactDyadic::from_f64(MIN_SUBNORMAL))
        .unwrap();
    assert!(one_plus_smallest.cmp(&exact_sum).is_gt());
}

#[test]
fn area_scaling_preserves_exact_values_and_nonfinite_receipts() {
    let frame = GeographicFrame::crs84();
    let x = scaled_x(frame, 20.0, 1, 10.0, Bounded::exact(0.125));
    assert_eq!(x.value.to_bits(), 46.25_f64.to_bits());
    assert!(x.error.is_finite() && x.error >= 0.0);

    let y = scaled_y(17.0, 5.0, Bounded::exact(0.125));
    assert_eq!(y.value.to_bits(), 1.5_f64.to_bits());
    let reversed_y = scaled_y(17.0, 5.0, Bounded::exact(-0.125));
    assert_eq!(reversed_y.value.to_bits(), (-1.5_f64).to_bits());

    let inexact_turn = scaled_x(frame, 20.0, (1_i64 << 53) + 1, 0.0, Bounded::exact(1.0));
    assert!(inexact_turn.error.is_infinite());

    let underflowed_y = scaled_y(MIN_SUBNORMAL, 0.0, Bounded::exact(0.5));
    assert_eq!(underflowed_y.value.to_bits(), 0);
    let exact_half_subnormal = ExactDyadic::try_from_u128(false, 1, -1075).unwrap();
    let half_subnormal_residual = exact_half_subnormal
        .try_subtract(ExactDyadic::from_f64(underflowed_y.value))
        .unwrap()
        .abs();
    assert!(
        half_subnormal_residual
            .cmp(&ExactDyadic::from_f64(underflowed_y.error))
            .is_le()
    );

    let overflowing_y = scaled_y(f64::MAX, -f64::MAX, Bounded::exact(2.0));
    assert!(!overflowing_y.value.is_finite());
    assert!(overflowing_y.error.is_infinite());

    let mut coefficients = [0.0; MAX_AUTHALIC_HARMONICS];
    coefficients[0] = 1.0;
    let density = GeographicAreaDensity::new(coefficients, 1, 1.0, 0.0, GeodeticPole::North);
    let (positive_area, scale_error) = scale_area_fast(1.0, &density, -3).unwrap();
    assert_eq!(positive_area.to_bits(), 64.0_f64.to_bits());
    assert!(!scale_error.is_zero());
    let exact_scale_bound = area_scale_bound(&density, -3).unwrap().to_exact().unwrap();
    assert!(exact_scale_bound.cmp(&ExactDyadic::from_i64(64)).is_ge());

    let (negative_area, _) = scale_area_fast(-1.0, &density, -3).unwrap();
    assert_eq!(negative_area.to_bits(), (-64.0_f64).to_bits());
}

#[test]
fn ceil_log2_matches_positive_integer_width_boundaries() {
    for (value, expected) in [
        (1, 0),
        (2, 1),
        (3, 2),
        (4, 2),
        (5, 3),
        (1_usize << (usize::BITS - 1), usize::BITS - 1),
        (usize::MAX, usize::BITS),
    ] {
        assert_eq!(ceil_log2(value), expected);
    }
}

#[test]
fn eft_receipts_cover_cancellation_and_subnormal_inputs() {
    assert_receipt_bound_covers(&[(1.0e16, 0.0), (1.0, 2.0 * f64::EPSILON), (-1.0e16, 0.0)]);

    let mut mixed_scale = Vec::with_capacity(162);
    for exponent in -40..=40 {
        let value = 2.0_f64.powi(exponent);
        mixed_scale.push((value, value * f64::EPSILON));
        mixed_scale.push((-value, 0.0));
    }
    mixed_scale.extend(std::iter::repeat_n((MIN_SUBNORMAL, MIN_SUBNORMAL), 32));
    assert_receipt_bound_covers(&mixed_scale);

    let mut overflowing = BoundedSum::default();
    overflowing.push(Bounded::exact(1.0e308));
    overflowing.push(Bounded::exact(1.0e308));
    overflowing.push(Bounded::exact(-1.0e308));
    assert!(overflowing.error().is_infinite());
    assert!(overflowing.value().is_nan());
}
