use super::*;

#[test]
fn ring_summary_uses_chart_turn_and_keeps_axis_bands() {
    let positive = ring(vec![0.0, 120.0, 240.0], vec![80.0; 3]);
    let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&positive)).unwrap();
    assert_eq!(summary.chart_turn, 1);
    assert_eq!(summary.turn(), RingTurn::Full(TurnDirection::Positive));
    assert!(!summary.exact_axis_band);
    assert_eq!(summary.bounded_pole(), Some(ChartPole::North));
    assert_eq!(
        summary.domain,
        RingDomain::FullLongitude(RingLatitudeSpan::Ordinary)
    );

    let negative = ring(vec![0.0, -120.0, -240.0], vec![-80.0; 3]);
    let negative = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&negative)).unwrap();
    assert_eq!(negative.chart_turn, -1);
    assert_eq!(negative.turn(), RingTurn::Full(TurnDirection::Negative));
    assert_eq!(negative.bounded_pole(), Some(ChartPole::South));

    let retraced = ring(vec![0.0, 120.0, 0.0, 0.0], vec![0.0; 4]);
    assert_eq!(
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&retraced))
            .unwrap()
            .turn(),
        RingTurn::None
    );

    // D3: the closing 360→0 edge is an explicit reverse circle. It cancels the
    // three short eastward edges, so this stored ring is chart-closed.
    let period_closed = ring(vec![0.0, 120.0, 240.0, 360.0], vec![60.0; 4]);
    let period_closed =
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&period_closed)).unwrap();
    assert_eq!(period_closed.chart_turn, 0);
    assert_eq!(period_closed.turn(), RingTurn::None);
    assert_eq!(period_closed.full_turn_pole(), None);
}
#[test]
fn raw_bounds_certify_only_strictly_ordinary_nonpolar_chart_support() {
    let frame = degrees();
    let bounds = |minx, miny, maxx, maxy| crate::geometry::Bounds::new_raw(minx, miny, maxx, maxy);

    assert!(frame.bounds_certify_ordinary_support(bounds(-10.0, -20.0, 10.0, 20.0)));
    assert!(frame.longitude_bounds_certify_no_seam(-10.0, 10.0));
    assert!(!frame.longitude_bounds_certify_no_seam(10.0, -10.0));
    assert!(!frame.bounds_certify_ordinary_support(bounds(10.0, -1.0, -10.0, 1.0)));
    assert!(!frame.bounds_certify_ordinary_support(bounds(-10.0, 10.0, 10.0, -10.0)));
    assert!(
        !frame.longitude_bounds_certify_no_seam(179.0, 181.0),
        "a narrow alias interval can still cross the canonical seam"
    );
    assert!(!frame.bounds_certify_ordinary_support(bounds(179.0, -1.0, 181.0, 1.0)));
    assert!(!frame.bounds_certify_ordinary_support(bounds(-180.0, -1.0, -170.0, 1.0)));
    assert!(!frame.bounds_certify_ordinary_support(bounds(0.0, -1.0, 180.0, 1.0)));
    assert!(!frame.bounds_certify_ordinary_support(bounds(-90.0, -1.0, 90.0, 1.0)));
    assert!(!frame.bounds_certify_ordinary_support(bounds(-1.0, -90.0, 1.0, 10.0)));
    assert!(!frame.bounds_certify_ordinary_support(bounds(-1.0, -10.0, 1.0, 90.0)));

    let grads = GeographicFrame::try_new(
        factor(std::f64::consts::TAU / 400.0),
        factor(std::f64::consts::TAU / 400.0),
        0.0,
    )
    .unwrap();
    assert!(grads.longitude_bounds_certify_no_seam(-20.0, 20.0));
    assert!(!grads.longitude_bounds_certify_no_seam(199.0, 201.0));
}
#[test]
fn explicit_period_edge_keeps_the_ambiguous_ring_admission_gate() {
    use crate::geometry::{Polygon, Shape};

    let shape = Shape::Polygon(Polygon::new(
        ring(vec![0.0, 360.0, 300.0, 0.0], vec![
            -10.0, -10.0, 10.0, -10.0,
        ]),
        Vec::new(),
    ));

    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();
    assert_eq!(error.location.ring, Some(0));
    assert_eq!(error.location.coordinate, 0);
    assert!(matches!(
        error.source,
        GeographicError::AmbiguousFullTurnRing { .. }
    ));
}
#[test]
fn every_ambiguous_nonzero_turn_is_rejected_in_both_directions() {
    use crate::geometry::{Polygon, Shape};

    let forward = [
        (0.0, 20.0),
        (120.0, -20.0),
        (240.0, 20.0),
        (360.0, -20.0),
        (480.0, 20.0),
        (600.0, -20.0),
        (0.0, 20.0),
    ];
    let reverse = [
        (0.0, 20.0),
        (600.0, -20.0),
        (480.0, 20.0),
        (360.0, -20.0),
        (240.0, 20.0),
        (120.0, -20.0),
        (0.0, 20.0),
    ];

    for (points, expected_turn) in [(&forward, 2), (&reverse, -2)] {
        let ring = ring_from_points(points);
        let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&ring)).unwrap();
        assert_eq!(summary.chart_turn, expected_turn);
        assert_eq!(summary.full_turn_pole(), Some(FullTurnPole::Ambiguous));

        let shape = Shape::Polygon(Polygon::new(ring, Vec::new()));
        let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();
        assert_eq!(error.location.ring, Some(0));
        assert_eq!(error.location.coordinate, 0);
        assert_bits(error.native.x, 0.0);
        assert_bits(error.native.y, 20.0);
        assert!(matches!(
            error.source,
            GeographicError::AmbiguousFullTurnRing { south, north }
                if south.to_bits() == (-20.0_f64).to_bits()
                    && north.to_bits() == 20.0_f64.to_bits()
        ));
    }
}
#[test]
fn full_turn_pole_uses_the_closed_hemisphere_rule_for_every_nonzero_turn() {
    let xs = vec![-180.0, -90.0, 0.0, 90.0, 180.0];
    let north = ring(xs.clone(), vec![0.0, 5.0, 5.0, 5.0, 0.0]);
    let north = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&north)).unwrap();
    assert_eq!(north.chart_turn, 1);
    assert_eq!(
        north.full_turn_pole(),
        Some(FullTurnPole::Bounded(ChartPole::North))
    );

    let south = ring(xs.clone(), vec![0.0, -5.0, -5.0, -5.0, 0.0]);
    let south = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&south)).unwrap();
    assert_eq!(south.chart_turn, 1);
    assert_eq!(
        south.full_turn_pole(),
        Some(FullTurnPole::Bounded(ChartPole::South))
    );

    let all_equator = ring(xs.clone(), vec![0.0; 5]);
    let all_equator =
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&all_equator)).unwrap();
    assert_eq!(all_equator.chart_turn, 1);
    assert_eq!(all_equator.full_turn_pole(), Some(FullTurnPole::Ambiguous));

    let straddling = ring(xs, vec![-5.0, 5.0, 5.0, 5.0, -5.0]);
    let straddling =
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&straddling)).unwrap();
    assert_eq!(straddling.chart_turn, 1);
    assert_eq!(straddling.full_turn_pole(), Some(FullTurnPole::Ambiguous));

    let multiple_turns = ring(vec![0.0, 120.0, 240.0, 360.0, 480.0, 600.0, 720.0], vec![
        1.0;
        7
    ]);
    let multiple_turns =
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&multiple_turns)).unwrap();
    assert_eq!(multiple_turns.chart_turn, 2);
    assert_eq!(
        multiple_turns.full_turn_pole(),
        Some(FullTurnPole::Bounded(ChartPole::North))
    );
}
#[rstest::rstest]
#[case::degrees(degrees())]
#[case::frame_1_0(frame(1.0))]
#[case::frame_std_f64_consts_tau_400_0(frame(std::f64::consts::TAU / 400.0))]
fn exact_axis_boxes_are_world_bands_without_fabricating_turns(#[case] frame: GeographicFrame) {
    let half = frame.longitude().half_period();
    let summary = axis_box(frame, -half, half, -frame.pole() / 2.0, frame.pole() / 2.0);
    assert_eq!(summary.turn(), RingTurn::None);
    assert!(summary.exact_axis_band);
    assert_eq!(
        summary.domain,
        RingDomain::FullLongitude(RingLatitudeSpan::Ordinary)
    );
    let reverse = axis_box(frame, half, -half, -frame.pole(), frame.pole());
    assert_eq!(reverse.turn(), RingTurn::None);
    assert!(reverse.exact_axis_band);
    assert_eq!(
        reverse.domain,
        RingDomain::FullLongitude(RingLatitudeSpan::PoleToPole)
    );

    let inside = axis_box(
        frame,
        -half,
        half.next_down(),
        -frame.pole() / 2.0,
        frame.pole() / 2.0,
    );
    assert_eq!(inside.domain, RingDomain::Ordinary);
}
#[test]
fn canonical_axis_band_spelling_retains_both_complete_parallel_turns() {
    let source = ring(
        vec![
            -180.0, -60.0, 60.0, -180.0, -180.0, 60.0, -60.0, -180.0, -180.0,
        ],
        vec![70.0, 70.0, 70.0, 70.0, 80.0, 80.0, 80.0, 80.0, 70.0],
    );
    for source in [source.clone(), source.reversed().unwrap()] {
        let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&source)).unwrap();
        assert_eq!(summary.chart_turn, 0);
        assert!(summary.exact_axis_band);
        assert_eq!(
            summary.domain,
            RingDomain::FullLongitude(RingLatitudeSpan::Ordinary)
        );
    }
}
#[rstest::rstest]
#[case::disabled(false)]
#[case::enabled(true)]
fn historical_parallel_levels_reject_interior_cancellation_independent_of_start(
    #[case] reverse: bool,
) {
    let frame = degrees();
    let counterring = [
        (0.0, 10.0),
        (360.0, 10.0),
        (360.0, 15.0),
        (0.0, 15.0),
        (0.0, 20.0),
        (360.0, 20.0),
        (360.0, 0.0),
        (0.0, 0.0),
        (0.0, 10.0),
    ];

    for offset in 0..counterring.len().wrapping_sub(1) {
        let source = rotated_ring(&counterring, offset, reverse);
        let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&source)).unwrap();
        let fused = admitted_ring_summary(frame, &source);

        assert_eq!(summary, fused);
        assert_eq!(summary.chart_turn, 0);
        assert!(!summary.exact_axis_band);
        assert_eq!(summary.domain, RingDomain::Ordinary);
    }
}
#[test]
fn historical_parallel_levels_accept_north_first_bands_and_ignore_zero_edges() {
    let frame = degrees();
    let north_first = [
        (0.0, 20.0),
        (120.0, 20.0),
        (240.0, 20.0),
        (360.0, 20.0),
        (360.0, 10.0),
        (240.0, 10.0),
        (120.0, 10.0),
        (0.0, 10.0),
        (0.0, 20.0),
    ];
    for reverse in [false, true] {
        for offset in 0..north_first.len() - 1 {
            let source = rotated_ring(&north_first, offset, reverse);
            let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&source)).unwrap();
            assert_eq!(summary, admitted_ring_summary(frame, &source));
            assert_eq!(summary.chart_turn, 0);
            assert!(summary.exact_axis_band);
            assert!(matches!(summary.domain, RingDomain::FullLongitude(_)));
        }
    }

    let zero_edge = ring_from_points(&[
        (-180.0, 20.0),
        (-60.0, 20.0),
        (60.0, 20.0),
        (180.0, 20.0),
        (-180.0, 20.0),
        (-180.0, 10.0),
        (60.0, 10.0),
        (-60.0, 10.0),
        (-180.0, 10.0),
        (-180.0, 20.0),
    ]);
    let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&zero_edge)).unwrap();
    assert_eq!(summary, admitted_ring_summary(frame, &zero_edge));
    assert!(summary.exact_axis_band);
}
#[test]
fn retraced_single_parallel_does_not_form_an_axis_band() {
    let frame = degrees();
    let retraced = ring_from_points(&[
        (0.0, 20.0),
        (360.0, 20.0),
        (0.0, 20.0),
        (0.0, 10.0),
        (0.0, 20.0),
    ]);
    let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&retraced)).unwrap();

    assert_eq!(summary, admitted_ring_summary(frame, &retraced));
    assert_eq!(summary.chart_turn, 0);
    assert!(!summary.exact_axis_band);
    assert_eq!(summary.domain, RingDomain::Ordinary);
}
#[test]
fn seam_zigzag_parallels_do_not_fabricate_an_axis_band() {
    let source = ring(
        vec![
            -180.0, 170.0, -170.0, 180.0, 180.0, -170.0, 170.0, -180.0, -180.0,
        ],
        vec![70.0, 70.0, 70.0, 70.0, 80.0, 80.0, 80.0, 80.0, 70.0],
    );
    let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&source)).unwrap();
    assert!(!summary.exact_axis_band);
    assert_eq!(summary.domain, RingDomain::Ordinary);
}
#[test]
fn unwrapped_axis_boxes_are_exact_and_wide_diagonals_are_not_bands() {
    let summary = axis_box(degrees(), 720.0, 1080.0, -10.0, 10.0);
    assert_eq!(
        summary.domain,
        RingDomain::FullLongitude(RingLatitudeSpan::Ordinary)
    );

    let huge_west = 1.0e15;
    let huge_east = huge_west + 360.0;
    assert_eq!(huge_east - huge_west, 360.0);
    assert!(matches!(
        axis_box(degrees(), huge_west, huge_east, -10.0, 10.0).domain,
        RingDomain::FullLongitude(_)
    ));
    assert_eq!(
        axis_box(degrees(), -f64::MAX, f64::MAX, -10.0, 10.0).domain,
        RingDomain::Ordinary
    );

    let diagonal = ring(vec![-180.0, 180.0, -180.0, 180.0, -180.0], vec![
        -10.0, 10.0, 10.0, -10.0, -10.0,
    ]);
    assert_eq!(
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&diagonal))
            .unwrap()
            .domain,
        RingDomain::SeamOnly
    );
}
#[test]
fn seam_spelled_axis_ring_is_not_an_explicit_world_band() {
    let seam_only = ring(vec![-180.0, 180.0, 180.0, -180.0, -180.0], vec![
        -85.0, -85.0, 85.0, 85.0, -85.0,
    ]);
    let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&seam_only)).unwrap();

    assert_eq!(summary.turn(), RingTurn::None);
    assert!(!summary.exact_axis_band);
    assert_eq!(summary.domain, RingDomain::SeamOnly);
}
