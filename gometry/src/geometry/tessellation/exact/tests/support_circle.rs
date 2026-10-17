#[test]
fn minimum_circle_support_predicates_use_exact_diameter_and_incircle_signs() {
    let diameter = [XY::new(-1.0, 0.0), XY::new(1.0, 0.0)];
    assert_eq!(
        diameter_contains_filtered(diameter[0], diameter[1], diameter[0]),
        None
    );
    assert_eq!(
        diameter_contains_filtered(diameter[0], diameter[1], XY::new(0.0, 0.5)),
        Some(true)
    );
    assert!(try_circle_contains(&diameter, XY::new(0.0, 1.0)).unwrap());
    assert!(!try_circle_contains(&diameter, XY::new(0.0, 1.0 + 2.5e-13)).unwrap());

    let acute = [XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(1.0, 3.0)];
    assert!(try_circle_contains(&acute, XY::new(2.0, 1.0)).unwrap());
    assert!(!try_circle_contains(&acute, XY::new(5.0, 5.0)).unwrap());
    assert!(try_certify_support(&acute, acute.into_iter()).unwrap());

    let obtuse = [XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(1.0, 0.5)];
    assert!(!try_certify_support(&obtuse, obtuse.into_iter()).unwrap());
}

#[test]
fn minimum_circle_rounds_exact_midpoint_and_circumradius_without_recomputing() {
    let pair = [XY::new(0.0, 0.0), XY::new(4.0, 0.0)];
    let (center, radius, zero) = round_support_circle(&pair).unwrap();
    assert_eq!(center, XY::new(2.0, 0.0));
    assert_eq!(radius.to_bits(), 2.0_f64.to_bits());
    assert!(!zero);

    let triangle = [XY::new(0.0, 0.0), XY::new(4.0, 0.0), XY::new(0.0, 2.0)];
    let (center, radius, zero) = round_support_circle(&triangle).unwrap();
    assert_eq!(center, XY::new(2.0, 1.0));
    assert_eq!(radius.to_bits(), 5.0_f64.sqrt().to_bits());
    assert!(!zero);

    let singleton = [XY::new(-0.0, 3.0)];
    let (center, radius, zero) = round_support_circle(&singleton).unwrap();
    assert_eq!(center, XY::new(0.0, 3.0));
    assert_eq!(radius.to_bits(), 0.0_f64.to_bits());
    assert!(zero);
}

#[test]
fn minimum_circle_diameter_filter_handles_mixed_exponents_without_allocation() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let scale = exact_power_of_two(200);
    let support = [
        XY::new(-scale, -scale),
        XY::new(scale, -scale + exact_power_of_two(148)),
    ];
    let point = XY::new(0.0, 0.0);
    let (result, allocations) =
        crate::test_support::allocation_count(|| try_circle_contains(&support, point));
    assert!(result.unwrap());
    assert_eq!(
        allocations, 0,
        "the certified interval path is allocation-free"
    );
}

#[test]
fn minimum_circle_three_support_rounding_handles_mixed_exponents() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let scale = exact_power_of_two(200);
    let tiny = exact_power_of_two(-200);
    let support = [
        XY::new(-scale, 0.0),
        XY::new(scale, 0.0),
        XY::new(0.0, tiny),
    ];
    round_support_circle(&support).expect("mixed-exponent circumcircle is representable");
}
