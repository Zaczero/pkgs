#[test]
fn bounded_edge_moments_match_literal_low_degree_divided_differences() {
    let mut moments = std::iter::repeat_with(BoundedSum::default)
        .take(3)
        .collect::<Vec<_>>();
    let mut variation = 0.0;
    let mut maximum_latitude = 0.0;
    let mut row_certificate = RowCertificate::default();
    add_fast_determinant(
        Bounded::exact(3.0 / 32.0),
        Bounded::exact(1.0 / 4.0),
        Bounded::exact(1.0 / 8.0),
        &mut moments,
        &mut variation,
        &mut maximum_latitude,
        &mut row_certificate,
    )
    .unwrap();

    for (moment, expected) in moments.iter().zip([3.0 / 32.0, 9.0 / 256.0, 21.0 / 2048.0]) {
        assert_eq!(moment.value(), expected);
        let residual = ExactDyadic::from_f64(expected)
            .try_subtract(ExactDyadic::from_f64(moment.value()))
            .unwrap()
            .abs();
        assert!(residual.cmp(&ExactDyadic::from_f64(moment.error())).is_le());
    }
}

include!("../row_certificate_tests.rs");
include!("../rectangle_moment_tests.rs");
