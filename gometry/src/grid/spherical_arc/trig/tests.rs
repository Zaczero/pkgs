use super::super::h3_lattice::{
    CertifiedCircularBoundsRad, CertifiedDegreeWindows, CertifiedLongitudeDegrees,
    CertifiedLongitudeRad, DegreeWindowResult,
};
use super::super::interval::verified_sqrt_enclosure;
use super::super::{CertSign, LongitudeSpan};
use super::*;

#[rstest::rstest]
#[case::negative_value_0_7(-0.7)]
#[case::negative_value_0_2(-0.2)]
#[case::value_0_0(0.0)]
#[case::value_0_2(0.2)]
#[case::value_0_7(0.7)]
#[case::value_1_0(1.0)]
#[case::negative_value_1_0(-1.0)]
fn trig_encloses_fast_path_without_libm_in_the_carrier(#[case] value: f64) {
    let (sin, cos) = sin_cos(Bound::exact(value).unwrap()).unwrap();
    assert!(sin.bound().unwrap().lo <= value.sin());
    assert!(sin.bound().unwrap().hi >= value.sin());
    assert!(cos.bound().unwrap().lo <= value.cos());
    assert!(cos.bound().unwrap().hi >= value.cos());
}

#[test]
fn trig_rejects_enclosed_quadrant_boundary() {
    let boundary = Bound::new(HALF_PI_LO, HALF_PI_HI).unwrap();
    assert!(sin_cos(boundary).is_none());
}

#[test]
fn trig_fallback_keeps_an_outward_pi_over_four_enclosure() {
    let input = degrees_to_radians_bound(45.0).unwrap();
    let (sin, cos) =
        sin_cos(input).expect("the degree conversion only spans an artificial pi/4 cut");
    let value = (PI_LO + PI_HI) * 0.125;
    assert!(sin.bound().unwrap().lo <= value.sin());
    assert!(sin.bound().unwrap().hi >= value.sin());
    assert!(cos.bound().unwrap().lo <= value.cos());
    assert!(cos.bound().unwrap().hi >= value.cos());
}

#[test]
fn square_root_requires_outward_multiplication_witnesses() {
    let four = Bound::exact(4.0).unwrap();
    assert!(verified_sqrt_enclosure(four, 2.0_f64.next_up(), 3.0).is_none());
    assert!(verified_sqrt_enclosure(four, 1.0, 2.0_f64.next_down()).is_none());

    let root = Bound::new(2.0, 9.0).unwrap().sqrt().unwrap();
    assert!(root.lo <= 2.0_f64.sqrt() && 3.0_f64.sqrt() <= root.hi);
    assert_eq!(Bound::exact(0.0).unwrap().sqrt(), Bound::exact(0.0));
}

#[test]
fn bounds_are_outward_and_division_refuses_zero() {
    let one = Bound::exact(1.0).unwrap();
    let three = Bound::exact(3.0).unwrap();
    let quotient = one.div(three).unwrap();
    assert!(quotient.lo < 1.0 / 3.0 && quotient.hi > 1.0 / 3.0);
    assert!(one.div(Bound::new(-1.0, 1.0).unwrap()).is_none());
}

#[test]
fn trig_rejects_an_enclosed_period_boundary_but_keeps_exact_zero() {
    assert!(sin_cos(degrees_to_radians_bound(360.0).unwrap()).is_none());
    let (sin, cos) = sin_cos(Bound::exact(0.0).unwrap()).expect("structural zero");
    assert!(sin.bound().unwrap().contains_zero());
    assert!(cos.bound().unwrap().lo <= 1.0 && cos.bound().unwrap().hi >= 1.0);
}

#[test]
fn directed_longitude_refuses_a_lower_pi_boundary_enclosure() {
    // `-PI_LO` is the stored-double upper bound of -π.  Treating it as
    // strictly inside would let a half-circle establish a directed sheet.
    let lower_boundary = Bound::exact(-PI_LO).unwrap();
    assert!(!strict_minor_longitude_delta(lower_boundary));
    assert!(strict_minor_longitude_delta(Bound::exact(0.0).unwrap()));
}

#[test]
fn degree_windows_are_outward_and_refuse_an_ambiguous_seam() {
    let radians = |degrees| degrees_to_radians_bound(degrees).unwrap();
    let split = CertifiedCircularBoundsRad {
        latitude: radians(10.0),
        longitude: CertifiedLongitudeRad::Span(
            LongitudeSpan::ordered(radians(170.0), radians(190.0)).unwrap(),
        ),
    };
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude: CertifiedLongitudeDegrees::Two([west, east]),
    }) = split.to_degree_windows()
    else {
        panic!("a strictly ordered +180 crossing has two certified windows");
    };
    assert!(latitude.lo <= 10.0 && 10.0 <= latitude.hi);
    assert!(west.lo <= 170.0 && 180.0 <= west.hi);
    assert!(east.lo <= -180.0 && -170.0 <= east.hi);

    let seam = CertifiedCircularBoundsRad {
        latitude: Bound::exact(0.0).unwrap(),
        longitude: CertifiedLongitudeRad::Span(
            LongitudeSpan::ordered(radians(180.0), radians(180.0)).unwrap(),
        ),
    };
    assert_eq!(seam.to_degree_windows(), DegreeWindowResult::Boundary);
}

#[test]
fn ball_sign_never_decides_through_zero() {
    assert_eq!(Ball::new(1.0, 0.25).unwrap().sign(), CertSign::Positive);
    assert_eq!(Ball::new(-1.0, 0.25).unwrap().sign(), CertSign::Negative);
    assert_eq!(Ball::new(0.0, 0.25).unwrap().sign(), CertSign::Uncertain);
}
