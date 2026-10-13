#[test]
fn nonzero_ratio_is_invariant_under_projective_sign_change() {
    let numerator = ExactDyadic::from_f64(5.0);
    let denominator = ExactDyadic::from_f64(3.0);
    let expected = 0x3FFA_AAAA_AAAA_AAAB;
    assert_eq!(
        round_ratio(&numerator, &denominator).unwrap().to_bits(),
        expected
    );
    assert_eq!(
        round_ratio(&numerator.neg(), &denominator.neg())
            .unwrap()
            .to_bits(),
        expected
    );
}

#[test]
fn nonzero_negative_ratio_underflow_retains_negative_zero() {
    let tiny_negative = ExactDyadic::from_parts(true, vec![1], -1075);
    assert_eq!(
        round_ratio(&tiny_negative, &ExactDyadic::one())
            .unwrap()
            .to_bits(),
        (-0.0_f64).to_bits()
    );
}

#[test]
fn line_intersection_rounding_is_projective_sign_invariant() {
    let x_axis = ExactLine::through_points(
        &ExactPoint::from_xy(XY::new(0.0, 0.0)),
        &ExactPoint::from_xy(XY::new(1.0, 0.0)),
    )
    .unwrap();
    let vertical = ExactLine::through_points(
        &ExactPoint::from_xy(XY::new(0.5, -1.0)),
        &ExactPoint::from_xy(XY::new(0.5, 1.0)),
    )
    .unwrap();
    let forward = line_intersection(&x_axis, &vertical)
        .unwrap()
        .round_nearest_even()
        .unwrap();
    let reversed = line_intersection(&vertical, &x_axis)
        .unwrap()
        .round_nearest_even()
        .unwrap();
    assert_eq!(
        (forward.x.to_bits(), forward.y.to_bits()),
        (0x3FE0_0000_0000_0000, 0x0000_0000_0000_0000)
    );
    assert_eq!(
        (reversed.x.to_bits(), reversed.y.to_bits()),
        (forward.x.to_bits(), forward.y.to_bits())
    );
}

#[test]
fn near_fixture_circumcenter_is_correctly_rounded() {
    let center = circumcenter(
        XY::new(22.0, -61.0),
        XY::new(-22.0, 61.0),
        XY::new(-62.0, f64::from_bits(19.0_f64.to_bits() + 1)),
    )
    .unwrap()
    .round_nearest_even()
    .unwrap();
    assert_eq!(
        center,
        XY::new(-1.224_017_584_342_978_8e-15, -4.414_489_648_450_088e-16)
    );
}

#[test]
fn certified_materialization_preserves_exact_homogeneous_coordinates() {
    let center = circumcenter(XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(0.0, 2.0)).unwrap();
    assert_eq!(
        compare_ratios(
            center.x().unwrap(),
            center.w().unwrap(),
            &ExactDyadic::from_f64(2.0),
            &ExactDyadic::one(),
        )
        .unwrap(),
        Ordering::Equal,
    );
    assert_eq!(
        compare_ratios(
            center.y().unwrap(),
            center.w().unwrap(),
            &ExactDyadic::from_f64(1.0),
            &ExactDyadic::one(),
        )
        .unwrap(),
        Ordering::Equal,
    );

    let hit = match ExactPoint::certified_bisector_hit([XY::new(0.0, 0.0), XY::new(4.0, 2.0)], [
        XY::new(-10.0, 7.0),
        XY::new(10.0, 7.0),
    ]) {
        FrameHit::Point(point) => point,
        FrameHit::Declined => panic!("expected a certified bisector hit, got a declined hit"),
        FrameHit::Parallel => panic!("expected a certified bisector hit, got a parallel bisector"),
    };
    assert_eq!(
        compare_ratios(
            hit.x().unwrap(),
            hit.w().unwrap(),
            &ExactDyadic::from_f64(-1.0),
            &ExactDyadic::one(),
        )
        .unwrap(),
        Ordering::Equal,
    );
    assert_eq!(
        compare_ratios(
            hit.y().unwrap(),
            hit.w().unwrap(),
            &ExactDyadic::from_f64(7.0),
            &ExactDyadic::one(),
        )
        .unwrap(),
        Ordering::Equal,
    );
}

#[test]
fn distinct_exact_vertices_can_collide_after_rounding() {
    let one = ExactDyadic::one();
    let base =
        ExactPoint::homogeneous(ExactDyadic::from_f64(1.0), ExactDyadic::zero(), one.clone());
    let shifted = ExactPoint::homogeneous(
        ExactDyadic::from_f64(1.0).add(ExactDyadic::from_parts(false, vec![1], -1074)),
        ExactDyadic::zero(),
        one,
    );
    assert_ne!(base.x().unwrap(), shifted.x().unwrap());
    assert_eq!(
        base.round_nearest_even().unwrap(),
        shifted.round_nearest_even().unwrap()
    );
}

#[test]
fn homogeneous_products_and_ratio_order_are_exact() {
    let x_axis = ExactLine([ExactDyadic::zero(), ExactDyadic::one(), ExactDyadic::zero()]);
    let vertical = ExactLine([
        ExactDyadic::one(),
        ExactDyadic::zero(),
        ExactDyadic::from_f64(-0.5),
    ]);
    let intersection = line_intersection(&x_axis, &vertical).unwrap();
    assert_eq!(
        intersection.round_nearest_even().unwrap(),
        XY::new(0.5, 0.0)
    );
    assert_eq!(
        signed_line_product(&vertical, &intersection).unwrap(),
        ExactSign::Zero
    );
    let reversed = line_intersection(&vertical, &x_axis).unwrap();
    let above = ExactLine::through_points(
        &ExactPoint::from_xy(XY::new(0.0, 1.0)),
        &ExactPoint::from_xy(XY::new(1.0, 1.0)),
    )
    .unwrap();
    assert_eq!(
        signed_line_product(&above, &intersection).unwrap(),
        signed_line_product(&above, &reversed).unwrap()
    );
    assert_eq!(
        compare_ratios(
            &ExactDyadic::one(),
            &ExactDyadic::from_f64(3.0),
            &ExactDyadic::one(),
            &ExactDyadic::from_f64(2.0),
        )
        .unwrap(),
        Ordering::Less
    );
}

#[test]
fn squared_distance_cmp_point_exact_returns_tie_on_site_bisector() {
    let origin = ExactPoint::from_xy(XY::new(1.0, 3.0));
    assert_eq!(
        squared_distance_cmp_point_exact(&origin, XY::new(0.0, 0.0), XY::new(2.0, 0.0)).unwrap(),
        Ordering::Equal
    );
}

#[test]
fn squared_distance_cmp_point_exact_accounts_for_negative_projective_weight() {
    let origin = ExactPoint::homogeneous(
        ExactDyadic::from_f64(-1.0),
        ExactDyadic::from_f64(-1.0),
        ExactDyadic::from_f64(-1.0),
    );
    assert_eq!(
        squared_distance_cmp_point_exact(&origin, XY::new(0.0, 0.0), XY::new(3.0, 0.0)).unwrap(),
        Ordering::Less
    );
}

#[test]
fn squared_distance_cmp_point_exact_orders_full_binary64_site_spread() {
    let origin = ExactPoint::from_xy(XY::new(1.0, 0.0));
    assert_eq!(
        squared_distance_cmp_point_exact(
            &origin,
            XY::new(f64::MAX, 0.0),
            XY::new(f64::from_bits(1), 0.0),
        )
        .unwrap(),
        Ordering::Greater
    );
}

#[test]
fn squared_distance_cmp_point_exact_orders_subnormal_site() {
    let origin = ExactPoint::from_xy(XY::new(0.0, 0.0));
    assert_eq!(
        squared_distance_cmp_point_exact(
            &origin,
            XY::new(f64::from_bits(1), 0.0),
            XY::new(0.0, 0.0),
        )
        .unwrap(),
        Ordering::Greater
    );
}

#[test]
fn squared_distance_cmp_point_exact_orders_midpoint_origin() {
    let first = ExactPoint::from_xy(XY::new(-4.0, 2.0));
    let second = ExactPoint::from_xy(XY::new(6.0, 4.0));
    let origin = first.midpoint(&second).unwrap();
    assert_eq!(
        squared_distance_cmp_point_exact(&origin, XY::new(0.0, 0.0), XY::new(4.0, 0.0)).unwrap(),
        Ordering::Less
    );
}
