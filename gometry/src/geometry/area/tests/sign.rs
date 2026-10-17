#[test]
fn certified_ring_sign_matches_reference_on_slivers_and_generated_cycles() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let tiny = f64::from_bits(1);
    let sliver = exact_power_of_two(-54);
    let translated = exact_power_of_two(-54);
    for (xs, ys) in [
        (
            vec![
                -1.446_158_859_221_779_4e-110,
                -2.594_428_477_756_554e-110,
                -2.307_361_073_122_860_3e-110,
                -1.446_158_859_221_779_4e-110,
            ],
            vec![
                2.371_948_131_117_221_8e-110,
                -1.487_324_694_703_768_7e-110,
                -5.225_064_882_485_208e-111,
                2.371_948_131_117_221_8e-110,
            ],
        ),
        (vec![0.0, 1.0, 1.0 - exact_power_of_two(-27), 0.0], vec![
            0.0,
            1.0 + exact_power_of_two(-27),
            1.0,
            0.0,
        ]),
        (vec![1.0, 0.0, sliver, 1.0], vec![0.0, 1.0, 1.0, 0.0]),
        (vec![1.0e7, 1.0e7 + 1.0, 1.0e7 + 1.0, 1.0e7, 1.0e7], vec![
            1.0e7,
            1.0e7,
            1.0e7 + translated,
            1.0e7 + translated,
            1.0e7,
        ]),
        (vec![0.0, tiny, 0.0, 0.0], vec![0.0, 0.0, tiny, 0.0]),
        (vec![f64::MAX, -f64::MAX, 0.0, f64::MAX], vec![
            1.0, 1.0, 0.0, 1.0,
        ]),
    ] {
        assert_ring_sign_matches_reference(&xs, &ys);
    }

    let mut seed = 0x67E1_8A29_0D43_B5CF_u64;
    let mut next_coordinate = || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        f64::from(u32::try_from(seed >> 32).expect("upper word fits u32") % 31) - 15.0
    };
    for ring_index in 0..32 {
        let mut xs = Vec::with_capacity(25);
        let mut ys = Vec::with_capacity(25);
        for vertex in 0..24 {
            let x = next_coordinate();
            let y = if ring_index % 4 == 0 {
                f64::from(u32::try_from(vertex).expect("24 vertices fit u32")) - 12.0
            } else {
                next_coordinate()
            };
            xs.push(x);
            ys.push(y);
        }
        if ring_index % 3 == 0 {
            let duplicate_x = *xs.get(4).expect("24 generated vertices");
            let duplicate_y = *ys.get(4).expect("24 generated vertices");
            *xs.get_mut(5).expect("24 generated vertices") = duplicate_x;
            *ys.get_mut(5).expect("24 generated vertices") = duplicate_y;
            let collinear_y = *ys.get(12).expect("24 generated vertices");
            *ys.get_mut(13).expect("24 generated vertices") = collinear_y;
        }
        let first_x = *xs.first().expect("24 generated vertices");
        let first_y = *ys.first().expect("24 generated vertices");
        xs.push(first_x);
        ys.push(first_y);
        assert_ring_sign_matches_reference(&xs, &ys);
    }
}

#[test]
fn near_collinear_ring_preserves_original_product_sum() {
    // Exact doubled area is -485 / 2^56; translating to rounded
    // coordinates erased the only noncollinear vertex.
    let xs = [
        19.34375,
        -30.765_625,
        -18.265_625,
        0.140_625,
        -3.109_375,
        30.359_375,
        19.34375,
    ];
    let ys = [
        59.03125,
        -91.296_875,
        -53.796_875,
        1.421_875_000_000_000_4,
        -8.328_125,
        92.078_125,
        59.03125,
    ];
    assert_eq!(
        exact_ring_area_sign_columns(&xs, &ys, 6),
        AreaSign::Negative
    );
}

#[test]
fn ring_sign_retains_product_tails_and_subnormal_products() {
    use crate::geometry::numerics::product::exact_power_of_two;
    for epsilon in [exact_power_of_two(-27), exact_power_of_two(-28)] {
        let xs = [0.0, 1.0, 1.0 - epsilon, 0.0];
        let ys = [0.0, 1.0 + epsilon, 1.0, 0.0];
        assert_eq!(
            exact_ring_area_sign_columns(&xs, &ys, 3),
            AreaSign::Positive
        );
    }
    let tiny = f64::from_bits(1);
    assert_eq!(
        exact_ring_area_sign_columns(&[0.0, tiny, 0.0, 0.0], &[0.0, 0.0, tiny, 0.0], 3),
        AreaSign::Positive
    );
}

#[test]
fn translated_thin_triangle_retains_clockwise_sign() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let tiny = exact_power_of_two(-54);
    let xs = [1.0, 0.0, tiny, 1.0];
    let ys = [0.0, 1.0, 1.0, 0.0];
    assert_eq!(
        exact_ring_area_sign_columns(&xs, &ys, 3),
        AreaSign::Negative
    );
}

#[test]
fn ring_sign_defers_underflowed_products_to_exact_sum() {
    use crate::geometry::numerics::product::exact_power_of_two;
    for exponent in [-537, -600] {
        let tiny = exact_power_of_two(exponent);
        assert_eq!(
            exact_ring_area_sign_columns(&[0.0, tiny, 0.0, 0.0], &[0.0, 0.0, tiny, 0.0], 3),
            AreaSign::Positive,
            "2^{exponent} triangle"
        );
    }
}

#[test]
fn ring_sign_defers_overflowing_differences_and_sums_to_exact_sum() {
    let huge = f64::MAX;
    // Both products are finite, but MAX - -MAX is not.
    assert_eq!(
        exact_ring_area_sign_columns(&[huge, -huge, 0.0, huge], &[1.0, 1.0, 0.0, 1.0], 3),
        AreaSign::Positive
    );
    // Each directed cross product is finite, but their sequential sum is not.
    assert_eq!(
        exact_ring_area_sign_columns(
            &[huge, huge, 0.0, 0.0, huge],
            &[0.0, 1.0, 1.0, 0.0, 0.0],
            4
        ),
        AreaSign::Positive
    );
}

#[test]
fn rounded_filter_defers_residual_product_and_sum_overflow_to_exact_sum() {
    let huge = f64::MAX;
    // The second residual is -MAX - MAX, which is nonfinite.
    assert_eq!(
        exact_ring_area_sign_columns(&[huge, -huge, huge, huge], &[0.0, 0.0, 1.0, 0.0], 3),
        AreaSign::Negative
    );
    // Residual products overflow before their cross difference is formed.
    assert_eq!(
        exact_ring_area_sign_columns(
            &[0.0, huge, huge, 0.0, 0.0],
            &[0.0, 0.0, huge, huge, 0.0],
            4
        ),
        AreaSign::Positive
    );
    // Two finite MAX cross terms overflow the running translated sum.
    assert_eq!(
        exact_ring_area_sign_columns(
            &[0.0, huge, huge, 0.0, 0.0],
            &[0.0, 0.0, 1.0, 1.0, 0.0],
            4
        ),
        AreaSign::Positive
    );
}

#[test]
fn rounded_residual_error_bound_covers_stored_dyadic_subtractions() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let tiny = f64::from_bits(1);
    // 2^-54 is lost when subtracting from one; its known exact residual
    // error must remain inside the residual-relative enclosure.
    let rounded = exact_power_of_two(-54) - 1.0;
    assert!(exact_power_of_two(-54) <= rounded_residual_error_bound(rounded.abs()));
    let subnormal = 2.0 * tiny - 5.0 * tiny;
    assert_eq!(subnormal, -3.0 * tiny);
    assert!(rounded_residual_error_bound(subnormal.abs()) >= tiny);
    let signed_zero = -0.0_f64 - 0.0;
    assert_eq!(signed_zero, 0.0);
    assert!(rounded_residual_error_bound(signed_zero.abs()) >= tiny);
}

#[test]
fn closed_column_zero_one_and_two_edge_cycles_are_zero() {
    assert_eq!(
        exact_ring_area_sign_columns(&[0.0], &[0.0], 0),
        AreaSign::Zero
    );
    assert_eq!(
        exact_ring_area_sign_columns(&[1.0, 1.0], &[-1.0, -1.0], 1),
        AreaSign::Zero
    );
    assert_eq!(
        exact_ring_area_sign_columns(&[0.0, 1.0, 0.0], &[0.0, 0.0, 0.0], 2),
        AreaSign::Zero
    );
}

#[test]
fn rounded_translated_rings_preserve_both_orientations() {
    let offset = 1.0e7;
    let positive_xs = [offset, offset + 1.0, offset, offset];
    let positive_ys = [offset, offset, offset + 1.0, offset];
    let negative_xs = [offset, offset, offset + 1.0, offset];
    let negative_ys = [offset, offset + 1.0, offset, offset];
    assert_eq!(
        exact_ring_area_sign_columns(&positive_xs, &positive_ys, 3),
        AreaSign::Positive
    );
    assert_eq!(
        exact_ring_area_sign_columns(&negative_xs, &negative_ys, 3),
        AreaSign::Negative
    );
}

#[test]
fn open_thin_triangle_retains_clockwise_sign() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let points = [
        XY::new(1.0, 0.0),
        XY::new(0.0, 1.0),
        XY::new(exact_power_of_two(-54), 1.0),
    ];
    assert_eq!(open_xy_cycle_sign(&points), AreaSign::Negative);
    let reversed = [points[2], points[1], points[0]];
    assert_eq!(open_xy_cycle_sign(&reversed), AreaSign::Positive);
    let point_cycle = [
        Point::new_raw_xy(1.0, 0.0),
        Point::new_raw_xy(0.0, 1.0),
        Point::new_raw_xy(exact_power_of_two(-54), 1.0),
    ];
    assert_eq!(exact_open_point_sign(&point_cycle), AreaSign::Negative);
}

/// Raw products for these rings overflow, so the scalar certificate must
/// decline and leave their orientation to the exact sum.
#[test]
fn rings_spanning_the_float_range_keep_their_orientation() {
    let huge = f64::MAX;
    // A counter-clockwise triangle and its mirror, both far wider than any
    // finite subtraction can express.
    let ccw_xs = [-huge, huge, 0.0, -huge];
    let ccw_ys = [-huge, -huge, huge, -huge];
    let cw_xs = [-huge, 0.0, huge, -huge];
    let cw_ys = [-huge, huge, -huge, -huge];
    assert_eq!(
        exact_ring_area_sign_columns(&ccw_xs, &ccw_ys, 3),
        AreaSign::Positive
    );
    assert_eq!(
        exact_ring_area_sign_columns(&cw_xs, &cw_ys, 3),
        AreaSign::Negative
    );
}
