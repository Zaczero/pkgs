#[rstest::rstest]
#[case::z0_0_0(0.0, 0.001, 1.0e-20)]
#[case::z0_0_04(0.04, 0.040_001, 1.0e-18)]
#[case::negative_z0_0_04(-0.04, 0.04, 1.0e-18)]
#[case::z0_0_04_9807c7e6(0.04, 0.040_001, 0.001)]
#[case::negative_z0_0_5(-0.5, 0.5, 0.0)]
#[case::z0_1_0e_160(1.0e-160, -1.0e-160, 0.0)]
#[case::f64_from_bits_1(f64::from_bits(1), 0.0, 0.0)]
fn aggregate_moment_row_enclosure_covers_source_and_endpoint_error(
    #[case] z0: f64,
    #[case] z1: f64,
    #[case] latitude_error: f64,
) {
    let mut moments: Vec<BoundedSum> = std::iter::repeat_with(BoundedSum::default)
        .take(33)
        .collect();
    let mut variation = 0.0;
    let mut radius = 0.0;
    let mut certificate = RowCertificate::default();
    add_fast_determinant(
        Bounded {
            value: 0.03125,
            error: 1.0e-19,
        },
        Bounded {
            value: z0,
            error: latitude_error,
        },
        Bounded {
            value: z1,
            error: latitude_error,
        },
        &mut moments,
        &mut variation,
        &mut radius,
        &mut certificate,
    )
    .unwrap();
    certificate.charge(&mut moments);

    for (left, right, determinant) in [
        (
            ExactDyadic::from_f64(z0),
            ExactDyadic::from_f64(z1),
            ExactDyadic::from_f64(0.03125),
        ),
        (
            ExactDyadic::from_f64(z0)
                .try_add(ExactDyadic::from_f64(latitude_error))
                .unwrap(),
            ExactDyadic::from_f64(z1)
                .try_subtract(ExactDyadic::from_f64(latitude_error))
                .unwrap(),
            ExactDyadic::from_f64(0.03125)
                .try_add(ExactDyadic::from_f64(1.0e-19))
                .unwrap(),
        ),
    ] {
        let mut symmetric_power = ExactDyadic::one();
        let mut right_power = right.clone();
        for (degree, moment) in moments.iter().enumerate() {
            let exact = determinant.try_product(&symmetric_power).unwrap();
            let residual = exact
                .try_subtract(ExactDyadic::from_f64(moment.value()))
                .unwrap()
                .abs();
            assert!(
                residual.cmp(&ExactDyadic::from_f64(moment.error())).is_le(),
                "z0={z0}, z1={z1}, degree={degree}"
            );
            symmetric_power = left
                .try_product(&symmetric_power)
                .unwrap()
                .try_add(right_power.clone())
                .unwrap();
            right_power = right_power.try_product(&right).unwrap();
        }
    }
}
