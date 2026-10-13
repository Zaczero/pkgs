#[test]
fn rational_area_accumulator_rounds_selected_fraction_once() {
    let exact = |value| ExactDyadic::from_f64(value);
    let mut area = ExactRationalArea::new();
    area.add_homogeneous_cross(&[exact(0.0), exact(0.0), exact(1.0)], &[
        exact(1.0),
        exact(0.0),
        exact(1.0),
    ])
    .expect("first edge");
    area.add_homogeneous_cross(&[exact(1.0), exact(0.0), exact(1.0)], &[
        exact(0.0),
        exact(1.0),
        exact(1.0),
    ])
    .expect("second edge");
    area.add_homogeneous_cross(&[exact(0.0), exact(1.0), exact(1.0)], &[
        exact(0.0),
        exact(0.0),
        exact(1.0),
    ])
    .expect("third edge");
    assert_eq!(area.round_abs_half().expect("rounded area"), 0.5);

    let mut rational = ExactRationalArea::new();
    rational
        .add_fraction(exact(1.0), exact(3.0))
        .expect("third");
    rational
        .add_fraction(exact(1.0), exact(6.0))
        .expect("sixth");
    assert_eq!(rational.round_abs_half().expect("rounded rational"), 0.25);
}
