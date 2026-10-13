fn assert_receipt_bound_covers(terms: &[(f64, f64)]) {
    let mut sum = BoundedSum::default();
    let mut exact_center = ExactDyadic::zero();
    let mut exact_error = ExactDyadic::zero();
    for &(value, input_error) in terms {
        sum.push(Bounded {
            value,
            error: input_error,
        });
        exact_center = exact_center.try_add(ExactDyadic::from_f64(value)).unwrap();
        exact_error = exact_error
            .try_add(ExactDyadic::from_f64(input_error))
            .unwrap();
    }
    let rounded = ExactDyadic::from_f64(sum.value());
    for endpoint in [
        exact_center
            .clone()
            .try_subtract(exact_error.clone())
            .unwrap(),
        exact_center.try_add(exact_error).unwrap(),
    ] {
        let difference = endpoint.try_subtract(rounded.clone()).unwrap();
        let (lower, upper) = outward_ratio_bounds(&difference, &ExactDyadic::one()).unwrap();
        let deviation = lower.abs().max(upper.abs());
        assert!(
            deviation <= sum.error(),
            "sum-receipt deviation={deviation:.17e}, bound={:.17e}, terms={terms:?}",
            sum.error()
        );
    }
}
