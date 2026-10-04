use super::*;

#[test]
fn homogeneous_point_segment_distance_preserves_half_subnormal() {
    let step = f64::from_bits(1);
    let midpoint = ExactPoint::from_homogeneous([
        ExactDyadic::from_f64(step),
        ExactDyadic::zero(),
        ExactDyadic::from_f64(2.0),
    ])
    .unwrap();
    let boundary = Segment {
        start: XY::new(0.0, -1.0),
        end: XY::new(0.0, 1.0),
    };
    let half_step = ExactDyadic::from_f64(step)
        .try_product(&ExactDyadic::from_f64(0.5))
        .unwrap();
    let expected = Rational::from_dyadic(half_step.try_product(&half_step).unwrap());

    assert_eq!(
        point_segment_distance_squared_exact(&midpoint, boundary)
            .unwrap()
            .try_cmp(&expected)
            .unwrap(),
        Ordering::Equal
    );
    assert!(
        point_segment_distance_squared_exact(
            &ExactPoint::from_homogeneous([
                ExactDyadic::zero(),
                ExactDyadic::zero(),
                ExactDyadic::from_f64(2.0),
            ])
            .unwrap(),
            boundary,
        )
        .unwrap()
        .is_zero()
    );
}
