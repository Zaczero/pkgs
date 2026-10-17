//! Tests for source projection and its exact-ratio witness.

use super::super::test_support::reverse_segment;
use super::segment_projection;
use crate::geometry::tessellation::exact::{ExactDyadic, compare_ratios};
use crate::geometry::{MOrdinate, Point, Segment, XY, ZOrdinate};

fn assert_projection_ratio(
    projection: &super::SegmentProjection,
    numerator: f64,
    denominator: f64,
) {
    let expected_numerator = ExactDyadic::from_f64(numerator);
    let expected_denominator = ExactDyadic::from_f64(denominator);
    match projection {
        super::SegmentProjection::CertifiedInterior(certified) => {
            let exact = certified.exact().unwrap();
            assert_eq!(
                compare_ratios(
                    &exact.numerator,
                    &exact.denominator,
                    &expected_numerator,
                    &expected_denominator,
                )
                .unwrap(),
                std::cmp::Ordering::Equal,
            );
        },
        super::SegmentProjection::ExactInterior(exact) => {
            assert_eq!(
                compare_ratios(
                    &exact.numerator,
                    &exact.denominator,
                    &expected_numerator,
                    &expected_denominator,
                )
                .unwrap(),
                std::cmp::Ordering::Equal,
            );
        },
        other @ (super::SegmentProjection::Start
        | super::SegmentProjection::Interior(_)
        | super::SegmentProjection::End) => {
            panic!("expected a source-bound interior, got {other:?}")
        },
    }
}

#[test]
fn cancelling_projection_retains_exact_ratio_on_reversed_supports() {
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(13.0, 13.0),
    };
    let point = XY::new(20.0, -6.0);
    let projection = segment_projection(point, segment).unwrap();
    assert!(projection.uses_exact_ratio());
    assert_projection_ratio(&projection, 7.0, 13.0);
    assert_eq!(
        projection.interpolate_xy(segment).unwrap(),
        XY::new(7.0, 7.0)
    );
    assert!(
        projection
            .cmp_along(&super::SegmentProjection::Start)
            .unwrap()
            .is_gt()
    );
    assert!(
        projection
            .cmp_along(&super::SegmentProjection::End)
            .unwrap()
            .is_lt()
    );

    let reversed = reverse_segment(segment);
    let reversed_projection = segment_projection(point, reversed).unwrap();
    assert!(reversed_projection.uses_exact_ratio());
    assert_projection_ratio(&reversed_projection, 6.0, 13.0);
    assert_eq!(
        reversed_projection.interpolate_xy(reversed).unwrap(),
        XY::new(7.0, 7.0)
    );
    assert!(projection.cmp_along(&reversed_projection).unwrap().is_gt());
}

#[test]
fn exact_interior_ratio_that_rounds_to_zero_keeps_xy_and_ordinates() {
    let minimum = f64::from_bits(1);
    let extent = f64::from_bits(0x7FE0_0000_0000_0000);
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(extent, 0.0),
    };
    let point = XY::new(minimum, 1.0);
    let projection = segment_projection(point, segment).unwrap();
    assert!(projection.uses_exact_ratio());
    assert!(!projection.is_start() && !projection.is_end());
    assert_eq!(projection.fraction().to_bits(), 0.0_f64.to_bits());
    assert!(
        projection
            .cmp_along(&super::SegmentProjection::Start)
            .unwrap()
            .is_gt()
    );
    assert!(
        projection
            .cmp_along(&super::SegmentProjection::End)
            .unwrap()
            .is_lt()
    );
    assert_eq!(
        projection.interpolate_xy(segment).unwrap(),
        XY::new(minimum, 0.0)
    );

    let start = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(0.0)));
    let end = Point::new_raw_axes(
        extent,
        0.0,
        ZOrdinate(Some(extent)),
        MOrdinate(Some(extent)),
    );
    let lifted = projection.interpolate_point(start, end).unwrap();
    assert_eq!(lifted.x.to_bits(), minimum.to_bits());
    assert_eq!(lifted.z().unwrap().to_bits(), minimum.to_bits());
    assert_eq!(lifted.m().unwrap().to_bits(), minimum.to_bits());
}

#[test]
fn exact_interior_ratio_that_rounds_to_one_keeps_large_ordinates_interior() {
    let previous = 1.0_f64.next_down();
    let large = f64::from_bits(0x7FE0_0000_0000_0000);
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(1.0, 1.0),
    };
    let projection = segment_projection(XY::new(1.0, previous), segment).unwrap();
    assert!(projection.uses_exact_ratio());
    assert!(!projection.is_start() && !projection.is_end());
    assert_eq!(projection.fraction().to_bits(), 1.0_f64.to_bits());
    assert_eq!(projection.interpolate_xy(segment).unwrap(), segment.end);

    let start = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(-large)), MOrdinate(Some(-large)));
    let end = Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(large)), MOrdinate(Some(large)));
    let lifted = projection.interpolate_point(start, end).unwrap();
    let expected = large.next_down();
    assert_eq!(lifted.z().unwrap().to_bits(), expected.to_bits());
    assert_eq!(lifted.m().unwrap().to_bits(), expected.to_bits());
    assert_ne!(lifted.z().unwrap().to_bits(), end.z().unwrap().to_bits());
    assert_ne!(lifted.m().unwrap().to_bits(), end.m().unwrap().to_bits());
}

#[test]
fn exact_clamp_classification_wins_around_one_plus_two_to_the_minus_fifty_four() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let half_ulp = exact_power_of_two(-53);
    let segment = Segment {
        start: XY::new(0.0, -half_ulp),
        end: XY::new(1.0, 1.0 - half_ulp),
    };
    let point = XY::new(1.0, 1.0);
    let terms = super::exact_projection_terms(point, segment).unwrap();
    let expected_numerator = ExactDyadic::from_f64(1.0)
        .try_add(ExactDyadic::from_f64(exact_power_of_two(-54)))
        .unwrap();
    assert_eq!(
        compare_ratios(
            &terms.numerator,
            &terms.denominator,
            &expected_numerator,
            &ExactDyadic::from_f64(1.0),
        )
        .unwrap(),
        std::cmp::Ordering::Equal
    );
    assert!(matches!(
        super::classify_segment_projection_exact(point, segment).unwrap(),
        super::ExactClassifiedSegmentProjection::End
    ));
    let projection = segment_projection(point, segment).unwrap();
    assert!(projection.is_end());
    assert_eq!(
        projection.interpolate_xy(segment).unwrap().y.to_bits(),
        segment.end.y.to_bits()
    );
}

#[test]
fn endpoint_projection_preserves_source_signed_zero_bits() {
    let segment = Segment {
        start: XY::new(-0.0, 0.0),
        end: XY::new(1.0, 1.0),
    };
    let projection = segment_projection(XY::new(-0.0, 0.0), segment).unwrap();
    assert!(projection.is_start());
    let foot = projection.interpolate_xy(segment).unwrap();
    assert_eq!(foot.x.to_bits(), (-0.0_f64).to_bits());
    let start = Point::new_raw_axes(-0.0, 0.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(3.0)));
    let end = Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(9.0)), MOrdinate(Some(7.0)));
    let lifted = projection.interpolate_point(start, end).unwrap();
    assert_eq!(lifted.x.to_bits(), start.x.to_bits());
    assert_eq!(lifted.z().unwrap().to_bits(), start.z().unwrap().to_bits());
    assert_eq!(lifted.m().unwrap().to_bits(), start.m().unwrap().to_bits());
}

#[cfg(test)]
mod exact_projection_allocation_tests {
    use super::segment_projection;
    use super::support::assert_allocation_count;
    use crate::geometry::{MOrdinate, Point, Segment, XY, ZOrdinate};
    use crate::test_support::allocation_count;

    #[test]
    fn endpoint_exact_classification_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 0.0),
        };
        let point = XY::new(0.0, 0.0);
        assert_allocation_count(0, || {
            super::super::classify_segment_projection_exact(point, segment).map(|_| ())
        });
    }

    #[test]
    fn historical_equal_exact_projection_retains_ratio_on_publication() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(
                f64::from_bits(0x2013_6323_7586_2748),
                f64::from_bits(0x2000_FBF1_9953_AE0A),
            ),
        };
        let point = XY::new(
            f64::from_bits(0xA018_FBE4_C3D7_9F2E),
            f64::from_bits(0x2039_6423_3D01_E367),
        );
        assert_allocation_count(6, || {
            segment_projection(point, segment).map(|projection| {
                assert!(matches!(
                    projection,
                    super::super::SegmentProjection::ExactInterior(inner)
                        if inner.emitted_foot.is_some()
                ));
            })
        });
    }

    #[test]
    fn ordinary_projection_path_stays_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        };
        let ((), allocations) = allocation_count(|| {
            let projection = segment_projection(XY::new(1.0, 1.0), segment).unwrap();
            assert!(!projection.uses_exact_ratio());
            projection.interpolate_xy(segment).unwrap();
        });
        assert_eq!(allocations, 0);
    }

    #[test]
    fn interior_exact_terms_are_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        };
        let point = XY::new(1.0, 1.0);
        assert_allocation_count(0, || {
            super::super::exact_projection_terms(point, segment).map(|_| ())
        });
    }

    #[test]
    fn interior_exact_classification_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let point = XY::new(1.0, 1.0_f64.next_down());
        assert_allocation_count(0, || {
            super::super::classify_segment_projection_exact(point, segment).map(|classification| {
                assert!(matches!(
                    classification,
                    super::super::ExactClassifiedSegmentProjection::Interior(_)
                ));
            })
        });
    }

    #[test]
    fn retained_exact_ratio_publication_allocates_one_owner() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let point = XY::new(1.0, 1.0_f64.next_down());
        let selection = super::super::point_segment_selection(point, segment).unwrap();
        assert!(
            selection
                .projection(point, segment)
                .unwrap()
                .uses_exact_ratio()
        );
        assert_allocation_count(1, || {
            selection.projection(point, segment).map(|projection| {
                assert!(projection.uses_exact_ratio());
            })
        });
    }

    #[test]
    fn exact_ratio_interpolation_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let projection = segment_projection(XY::new(1.0, 1.0_f64.next_down()), segment).unwrap();
        let start = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(-1.0)), MOrdinate(Some(-3.0)));
        let end = Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(5.0)), MOrdinate(Some(9.0)));
        let expected = projection.interpolate_point(start, end).unwrap();
        assert_allocation_count(0, || projection.interpolate_point(start, end).map(|_| ()));
        assert_eq!(
            projection.interpolate_point(start, end).unwrap(),
            expected,
            "repeated interpolation preserves the emitted point"
        );
    }

    #[test]
    fn historical_equal_exact_projection_does_not_rebuild_ratio_on_interpolation() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(
                f64::from_bits(0x2013_6323_7586_2748),
                f64::from_bits(0x2000_FBF1_9953_AE0A),
            ),
        };
        let point = XY::new(
            f64::from_bits(0xA018_FBE4_C3D7_9F2E),
            f64::from_bits(0x2039_6423_3D01_E367),
        );
        let retained = segment_projection(point, segment).unwrap();
        let exact = super::super::exact_interior_projection(point, segment).unwrap();
        let exact = super::super::SegmentProjection::ExactInterior(Box::new(exact));
        let start = Point::new_raw_xy(0.0, 0.0);
        let end = Point::new_raw_axes(
            segment.end.x,
            segment.end.y,
            ZOrdinate(Some(13.0)),
            MOrdinate(Some(29.0)),
        );

        let (retained_result, retained_allocations) =
            allocation_count(|| retained.interpolate_point(start, end));
        let (exact_result, exact_allocations) =
            allocation_count(|| exact.interpolate_point(start, end));
        assert_eq!(retained_result.unwrap(), exact_result.unwrap());
        assert_eq!(
            retained_allocations, exact_allocations,
            "an exact owner with an emitted-foot override must not rebuild N/D"
        );
    }

    #[test]
    fn certified_ratio_interpolation_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(13.0, 13.0),
        };
        let projection = segment_projection(XY::new(20.0, -6.0), segment).unwrap();
        assert!(matches!(
            projection,
            super::super::SegmentProjection::CertifiedInterior(_)
        ));
        let start = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(1.0)), MOrdinate(Some(3.0)));
        let end = Point::new_raw_axes(13.0, 13.0, ZOrdinate(Some(5.0)), MOrdinate(Some(9.0)));
        assert_allocation_count(0, || projection.interpolate_point(start, end).map(|_| ()));
    }

    #[test]
    fn exact_ratio_xy_interpolation_is_allocation_free() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(1.0, 1.0),
        };
        let projection = segment_projection(XY::new(1.0, 1.0_f64.next_down()), segment).unwrap();
        assert_allocation_count(0, || projection.interpolate_xy(segment).map(|_| ()));
    }

    #[test]
    fn extreme_exact_projection_is_allocation_free() {
        let segment = Segment {
            start: XY::new(1e200, 0.0),
            end: XY::new(1e200 + 1.0, 0.0),
        };
        let point = XY::new(1e200 + 0.5, 1.0);
        assert_allocation_count(0, || segment_projection(point, segment).map(|_| ()));
    }
}

mod support;
