use super::view::RingLatitudeSpan;
use super::*;

fn factor(value: f64) -> AngularFactor {
    AngularFactor::try_new("unit", value).unwrap()
}
fn frame(value: f64) -> GeographicFrame {
    GeographicFrame::try_new(factor(value), factor(value), 0.0).unwrap()
}
fn degrees() -> GeographicFrame {
    frame(std::f64::consts::PI / 180.0)
}

#[test]
fn chart_and_geodetic_poles_remain_distinct_for_southward_latitude_axes() {
    let unit = factor(std::f64::consts::PI / 180.0);
    let frame = GeographicFrame::try_new_with_orientation(
        unit,
        unit,
        0.0,
        GeographicAxisOrientation::from_axis_directions(true, false),
    )
    .unwrap();
    assert_eq!(frame.chart_pole_latitude(ChartPole::North), 90.0);
    assert_eq!(frame.chart_pole_latitude(ChartPole::South), -90.0);
    assert_eq!(frame.native_latitude_pole(GeodeticPole::North), -90.0);
    assert_eq!(frame.native_latitude_pole(GeodeticPole::South), 90.0);
}

fn assert_bits(value: f64, expected: f64) {
    assert_eq!(value.to_bits(), expected.to_bits());
}

fn xy_seq(xs: Vec<f64>, ys: Vec<f64>) -> crate::geometry::CoordSeq {
    crate::geometry::CoordSeq::try_from_vecs(xs, ys, None, None).unwrap()
}

fn ring(xs: Vec<f64>, ys: Vec<f64>) -> crate::geometry::Ring {
    crate::geometry::Ring::closed_coordseq(xy_seq(xs, ys)).unwrap()
}

fn nearby_grad_factor_frame() -> GeographicFrame {
    let factor = factor(std::f64::consts::TAU / 400.0_f64.next_up());
    GeographicFrame::try_new(factor, factor, 0.0).unwrap()
}

#[test]
fn scalar_view_borrows_literal_payload_and_reduces_only_on_access() {
    let point = crate::geometry::Point::new(540.0, 45.0).unwrap();
    let shape = crate::geometry::Shape::Point(point);
    let view = ScalarGeographicView::try_new(degrees(), &shape).unwrap();
    assert!(std::ptr::eq(view.shape(), &raw const shape));
    let mut seen = None;
    view.for_each_coordinate(|coordinate| {
        seen = Some(coordinate);
    });
    let coordinate = seen.unwrap();
    assert_bits(coordinate.native_xy().x, 540.0);
    assert_bits(coordinate.longitude().get(), -180.0);
    assert_eq!(coordinate.identity(degrees()), SeamIdentity::Seam);
}

#[test]
fn pole_identity_supersedes_longitude() {
    let paris = GeographicFrame::try_new(
        factor(std::f64::consts::PI / 180.0),
        factor(std::f64::consts::PI / 180.0),
        2.0_f64.to_radians(),
    )
    .unwrap();
    let shape = crate::geometry::Shape::Point(crate::geometry::Point::new(1234.0, 90.0).unwrap());
    let view = ScalarGeographicView::try_new(paris, &shape).unwrap();
    view.for_each_coordinate(|coordinate| {
        assert_eq!(coordinate.identity(paris), SeamIdentity::Pole);
    });
}

#[test]
fn native_periodic_seam_detection_uses_the_frame_half_period() {
    let grads = GeographicFrame::try_new(
        factor(std::f64::consts::TAU / 400.0),
        factor(std::f64::consts::TAU / 400.0),
        0.0,
    )
    .unwrap();
    let crossing = crate::geometry::Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(vec![150.0, 250.0], vec![0.0, 0.0])).unwrap(),
    );
    let non_crossing = crate::geometry::Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(vec![0.0, 300.0], vec![0.0, 0.0])).unwrap(),
    );
    let seam_endpoint = crate::geometry::Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(vec![200.0, 199.5], vec![0.0, 0.0])).unwrap(),
    );

    assert!(crate::geometry::crosses_antimeridian_in_frame(
        &crossing, grads
    ));
    assert!(!crate::geometry::crosses_antimeridian_in_frame(
        &non_crossing,
        grads
    ));
    assert!(!crate::geometry::crosses_antimeridian_in_frame(
        &seam_endpoint,
        grads
    ));
}

#[test]
fn ordinary_chart_certificate_matches_selected_period_and_edge_contracts() {
    let nearby_grads = nearby_grad_factor_frame();
    let frames = [
        degrees(),
        frame(1.0),
        frame(std::f64::consts::TAU / 400.0),
        nearby_grads,
    ];
    for frame in frames {
        let axis = frame.longitude();
        let half = axis.half_period();
        assert!(!axis.ordinary_chart_edge(-half / 2.0, half / 2.0));
        assert!(axis.ordinary_chart_edge(-half.next_down(), 0.0));
        assert!(!axis.ordinary_chart_edge(-half, half));
        assert!(!axis.ordinary_chart_edge(0.0, axis.period()));
        assert!(!axis.ordinary_chart_edge(-half.next_down(), half.next_down()));
        assert_eq!(axis.chart_transit(-half / 2.0, half / 2.0).unwrap(), 0);
    }

    let axis = degrees().longitude();
    let from = -90.0;
    let to = 90.0_f64.next_up();
    assert_eq!(to - from, 180.0);
    assert!(!axis.ordinary_chart_edge(from, to));
    let connection = axis.chart_connection(from, to).unwrap();
    assert_eq!(connection.source_lift, -1);
    assert_eq!(connection.seam_transit(), -1);

    for axis in [
        nearby_grads.longitude(),
        frame(std::f64::consts::TAU / 400.0).longitude(),
    ] {
        assert_bits(axis.period(), 400.0);
        assert_bits(axis.half_period(), 200.0);
        assert_eq!(axis.ordinary_longitude_bound(), 200.0);
        let nominal_half = axis.canonicalize(200.0).unwrap();
        assert_bits(nominal_half.get(), -200.0);
        assert_eq!(nominal_half.identity(axis), SeamIdentity::Seam);
    }
    assert_eq!(
        axis.canonicalize(-0.0).unwrap().get().to_bits(),
        0.0_f64.to_bits()
    );
}

#[test]
fn latitude_admission_is_one_exterior_ulp_and_contextual() {
    let pole = degrees().pole();
    let admitted =
        crate::geometry::Shape::Point(crate::geometry::Point::new(0.0, pole.next_up()).unwrap());
    let view = ScalarGeographicView::try_new(degrees(), &admitted).unwrap();
    view.for_each_coordinate(|coordinate| {
        assert_bits(coordinate.latitude().get(), pole);
        assert_bits(coordinate.native_xy().y, pole.next_up());
    });

    let rejected = crate::geometry::Shape::Point(
        crate::geometry::Point::new(0.0, pole.next_up().next_up()).unwrap(),
    );
    let error = ScalarGeographicView::try_new(degrees(), &rejected).unwrap_err();
    assert_eq!(error.location.coordinate, 0);
    assert_bits(error.native.x, 0.0);
    assert_bits(error.native.y, pole.next_up().next_up());
    assert!(matches!(
        error.source,
        GeographicError::LatitudeOutOfRange { .. }
    ));
}

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

#[test]
fn coordinate_domain_errors_precede_pending_ambiguous_ring_gates() {
    let shell = ring(vec![-180.0, -90.0, 0.0, 90.0, 180.0], vec![
        -5.0, 5.0, 5.0, 5.0, -5.0,
    ]);
    let hole = ring(vec![10.0, 20.0, 20.0, 10.0], vec![
        100.0, 100.0, 101.0, 100.0,
    ]);
    let shape = crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(shell, vec![hole]));

    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();
    assert_eq!(error.location.ring, Some(1));
    assert!(matches!(
        error.source,
        GeographicError::LatitudeOutOfRange { .. }
    ));
}

fn axis_box(frame: GeographicFrame, west: f64, east: f64, south: f64, north: f64) -> RingSummary {
    let middle = f64::midpoint(west, east);
    let ring = ring(vec![west, middle, east, east, middle, west, west], vec![
        south, south, south, north, north, north, south,
    ]);
    RingSummary::try_new(frame, &GeographicRing::from_ring(&ring)).unwrap()
}

#[test]
fn exact_axis_boxes_are_world_bands_without_fabricating_turns() {
    for frame in [degrees(), frame(1.0), frame(std::f64::consts::TAU / 400.0)] {
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

#[test]
fn scalar_errors_preserve_nested_logical_members_and_ring_indices() {
    use crate::geometry::{CoordinateAxes, MultiMember, MultiPolygon, Polygon, Shape, SharedSlice};

    let shell = ring(vec![0.0, 2.0, 2.0, 0.0, 0.0], vec![0.0, 0.0, 2.0, 2.0, 0.0]);
    let bad_hole = ring(vec![0.5, 1.5, 1.5, 0.5, 0.5], vec![
        0.5, 0.5, 91.0, 1.5, 0.5,
    ]);
    let polygon = Polygon {
        shell,
        holes: SharedSlice::from_vec(vec![bad_hole]),
    };
    let multipart = MultiPolygon::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(polygon),
    ])
    .unwrap();
    let nested = Shape::geometry_collection(vec![
        Shape::geometry_collection(vec![Shape::MultiPolygon(multipart)])
            .expect("test collection is valid"),
    ])
    .expect("test collection is valid");
    let error = ScalarGeographicView::try_new(degrees(), &nested).unwrap_err();
    assert_eq!(error.location.members.as_slice(), [0, 0, 1]);
    assert_eq!(error.location.ring, Some(1));
    assert_eq!(error.location.coordinate, 2);
    assert_bits(error.native.x, 1.5);
    assert_bits(error.native.y, 91.0);
}

#[test]
fn shared_dag_domain_error_keeps_the_first_member_path() {
    use crate::geometry::Shape;

    let invalid = Shape::geometry_collection(vec![
        Shape::Point(crate::geometry::Point::new_raw_xy(10.0, 20.0)),
        Shape::Point(crate::geometry::Point::new_raw_xy(f64::NAN, 30.0)),
        Shape::Point(crate::geometry::Point::new_raw_xy(0.0, 91.0)),
    ])
    .unwrap();
    let shape = Shape::geometry_collection(vec![invalid.clone(), invalid]).unwrap();

    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();

    assert_eq!(error.location.members.as_slice(), [0, 1]);
    assert_eq!(error.location.coordinate, 0);
    assert!(error.native.x.is_nan());
    assert_bits(error.native.y, 30.0);
}

fn deeply_nested_line(coordinate_count: usize) -> crate::geometry::Shape {
    use crate::geometry::Shape;

    let line = Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(
            (0..coordinate_count).map(|index| index as f64).collect(),
            vec![0.0; coordinate_count],
        ))
        .unwrap(),
    );
    (0..5).fold(line, |shape, _| {
        Shape::geometry_collection(vec![shape]).expect("test collection is valid")
    })
}

#[test]
fn valid_deeply_nested_validation_does_not_allocate_per_coordinate() {
    let one = deeply_nested_line(2);
    let many = deeply_nested_line(128);
    let (_, one_allocations) = crate::test_support::allocation_count(|| {
        ScalarGeographicView::try_new(degrees(), &one).unwrap()
    });
    let (_, many_allocations) = crate::test_support::allocation_count(|| {
        ScalarGeographicView::try_new(degrees(), &many).unwrap()
    });
    assert_eq!(one_allocations, 1);
    assert_eq!(many_allocations, one_allocations);
}

#[test]
fn admitted_walk_keeps_nested_empty_order_wrap_and_ulp() {
    use crate::geometry::{
        CoordinateAxes, GeometryKind, LineSeq, MOrdinate, MultiMember, MultiPoint, Point, Shape,
        ZOrdinate,
    };

    let pole = degrees().pole();
    let wrapped = Point::new_axes(540.0, 45.0, ZOrdinate(Some(3.0)), MOrdinate(Some(4.0))).unwrap();
    let ulp = Point::new(0.0, pole.next_up()).unwrap();
    let points = MultiPoint::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Empty,
        MultiMember::Value(wrapped),
        MultiMember::Empty,
    ])
    .unwrap();
    let stored = points.point_at(0);
    assert_eq!(stored.z(), Some(3.0));
    assert_eq!(stored.m(), Some(4.0));
    let line =
        Shape::LineString(LineSeq::try_new(xy_seq(vec![10.0, 20.0], vec![1.0, 2.0])).unwrap());
    let shape = Shape::geometry_collection(vec![
        Shape::Empty(GeometryKind::Point, CoordinateAxes::XY),
        Shape::MultiPoint(points),
        Shape::Point(ulp),
        line,
    ])
    .unwrap();
    let view = ScalarGeographicView::try_new(degrees(), &shape).unwrap();
    let mut seen = Vec::new();
    view.for_each_coordinate(|coordinate| seen.push(coordinate));
    assert_eq!(seen.len(), 4);
    assert_bits(seen[0].native_xy().x, 540.0);
    assert_bits(seen[0].longitude().get(), -180.0);
    assert_bits(seen[1].latitude().get(), pole);
    assert_bits(seen[1].native_xy().y, pole.next_up());
    assert_bits(seen[2].native_xy().x, 10.0);
    assert_bits(seen[2].native_xy().y, 1.0);
    assert_bits(seen[3].native_xy().x, 20.0);
    assert_bits(seen[3].native_xy().y, 2.0);
}

#[test]
fn nested_invalid_coordinate_reports_longitude_before_latitude() {
    use crate::geometry::{CoordinateAxes, MultiMember, MultiPolygon, Polygon, Shape, SharedSlice};

    let shell = ring(vec![0.0, 2.0, 2.0, 0.0, 0.0], vec![0.0, 0.0, 2.0, 2.0, 0.0]);
    let bad_hole = ring(vec![0.5, 1.5, f64::NAN, 0.5, 0.5], vec![
        0.5, 0.5, 91.0, 1.5, 0.5,
    ]);
    let polygon = Polygon {
        shell,
        holes: SharedSlice::from_vec(vec![bad_hole]),
    };
    let multipart = MultiPolygon::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(polygon),
    ])
    .unwrap();
    let nested = Shape::geometry_collection(vec![
        Shape::geometry_collection(vec![Shape::MultiPolygon(multipart)])
            .expect("test collection is valid"),
    ])
    .expect("test collection is valid");
    let error = ScalarGeographicView::try_new(degrees(), &nested).unwrap_err();
    assert_eq!(error.location.members.as_slice(), [0, 0, 1]);
    assert_eq!(error.location.ring, Some(1));
    assert_eq!(error.location.coordinate, 2);
    assert!(error.native.x.is_nan());
    assert_bits(error.native.y, 91.0);
    assert!(matches!(
        error.source,
        GeographicError::NonFinite {
            name: "longitude",
            value,
        } if value.is_nan()
    ));
}

#[test]
fn domain_admission_reports_first_native_xy() {
    let shape = crate::geometry::Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(vec![0.0, 10.0], vec![95.0, 91.0])).unwrap(),
    );
    let error = crate::crs::ensure_geographic_domain_in_frame(degrees(), &shape).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("invalid longitude/latitude (0, 95)"),
        "{message}"
    );
    assert!(!message.contains("(10, 91)"), "{message}");
}

#[test]
fn frames_derive_degree_grad_and_radian_axes() {
    let degree = degrees();
    assert_eq!(degree.longitude().period(), 360.0);
    assert_eq!(degree.pole(), 90.0);
    let grad = frame(std::f64::consts::TAU / 400.0);
    assert_eq!(grad.longitude().period(), 400.0);
    assert_eq!(grad.pole(), 100.0);
    let radian = frame(1.0);
    assert_eq!(radian.longitude().period(), std::f64::consts::TAU);
    assert_eq!(radian.pole(), std::f64::consts::FRAC_PI_2);
    assert_eq!(
        degree.longitude().unit().radians_per_unit(),
        std::f64::consts::PI / 180.0
    );
    assert_eq!(
        grad.longitude().radians_per_unit(),
        std::f64::consts::TAU / 400.0
    );
    assert_eq!(
        grad.latitude_radians_per_unit(),
        std::f64::consts::TAU / 400.0
    );
    let adjacent_grad = frame((std::f64::consts::TAU / 400.0).next_up());
    assert_eq!(adjacent_grad.longitude().period(), 400.0);
    assert_eq!(adjacent_grad.pole(), 100.0);
    assert_eq!(grad, adjacent_grad);
    assert_eq!(
        degree.latitude_unit().radians_per_unit(),
        std::f64::consts::PI / 180.0
    );
    assert_eq!(degree.prime_meridian_radians(), 0.0);
    AngularFactor::try_new("unit", 0.0).unwrap_err();
    AngularFactor::try_new("unit", f64::INFINITY).unwrap_err();
    PeriodicAxis::try_new("subnormal axis", factor(f64::MAX)).unwrap_err();
    GeographicFrame::try_new(degree.longitude().unit(), factor(f64::from_bits(1)), 0.0)
        .unwrap_err();
}

#[test]
fn frame_conversion_uses_one_greenwich_seam_for_points_and_lanes() {
    let degree = degrees();
    let converted = degree
        .to_geodesic_point(crate::geometry::Point::new_raw_xy(180.0, 45.0))
        .unwrap();
    assert_bits(converted.x, -180.0);
    assert_bits(
        degree
            .point_from_geodesic(crate::geometry::Point::new_raw_xy(180.0, 45.0))
            .unwrap()
            .x,
        -180.0,
    );
    assert_bits(
        degree
            .to_geodesic_point(crate::geometry::Point::new_raw_xy(0.0, 90.0_f64.next_up()))
            .unwrap()
            .y,
        90.0,
    );

    let paris = GeographicFrame::try_new(
        factor(std::f64::consts::PI / 180.0),
        factor(std::f64::consts::PI / 180.0),
        2.0_f64.to_radians(),
    )
    .unwrap();
    let converted = paris
        .to_geodesic_point(crate::geometry::Point::new_raw_xy(0.0, 45.0))
        .unwrap();
    assert_eq!(converted.x, 2.0);
    assert_eq!(converted.y, 45.0);
    let restored = paris.point_from_geodesic(converted).unwrap();
    assert_eq!(restored.x, 0.0);
    assert_eq!(restored.y, 45.0);

    let mut xs = [0.0, 180.0];
    let mut ys = [45.0, -45.0];
    paris.to_geodesic_lanes(&mut xs, &mut ys).unwrap();
    assert_eq!(xs, [2.0, -178.0]);
    paris.lanes_from_geodesic(&mut xs, &mut ys).unwrap();
    assert_eq!(xs, [0.0, -180.0]);
}

#[test]
fn computed_native_points_canonicalize_longitude_and_preserve_other_axes() {
    for (frame, longitude, expected) in [
        (degrees(), 180.0, -180.0),
        (
            frame(std::f64::consts::TAU / 400.0),
            200.0,
            -199.999_999_999_999_97,
        ),
        (frame(1.0), std::f64::consts::PI, -std::f64::consts::PI),
    ] {
        let point = crate::geometry::Point::new_axes(
            longitude,
            -0.0,
            crate::geometry::ZOrdinate(Some(-0.0)),
            crate::geometry::MOrdinate(Some(7.0)),
        )
        .unwrap();
        let emitted = frame.canonical_computed_point(point).unwrap();

        assert_bits(emitted.x, expected);
        assert_bits(emitted.y, -0.0);
        assert_bits(emitted.z().unwrap(), -0.0);
        assert_eq!(emitted.m(), Some(7.0));
    }

    let zero = degrees()
        .canonical_computed_point(crate::geometry::Point::new_raw_xy(-0.0, 0.0))
        .unwrap();
    assert_bits(zero.x, 0.0);
}

#[expect(
    clippy::float_cmp,
    reason = "the rejected latitude is the stored 100.0 spelling, not a nearby out-of-range value"
)]
#[test]
fn lane_conversion_leaves_the_failing_pair_and_later_pairs_unchanged() {
    let paris = GeographicFrame::try_new(
        factor(std::f64::consts::PI / 180.0),
        factor(std::f64::consts::PI / 180.0),
        2.0_f64.to_radians(),
    )
    .unwrap();

    let mut xs = [0.0, 1.0, 10.0, 99.0];
    let mut ys = [45.0, 100.0, 20.0];
    let error = paris
        .to_geodesic_lanes_indexed(&mut xs, &mut ys)
        .unwrap_err();
    assert_eq!(error.0, 1);
    assert!(matches!(
        error.1,
        GeographicError::LatitudeOutOfRange { value } if value == 100.0
    ));
    assert_eq!(xs, [2.0, 1.0, 10.0, 99.0]);
    assert_eq!(ys, [45.0, 100.0, 20.0]);

    let mut xs = [2.0, f64::NAN, 5.0, 99.0];
    let mut ys = [45.0, 10.0, 20.0];
    let error = paris
        .lanes_indexed_from_geodesic(&mut xs, &mut ys)
        .unwrap_err();
    assert_eq!(error.0, 1);
    assert!(matches!(error.1, GeographicError::NonFinite {
        name: "geodesic coordinate",
        ..
    }));
    assert_eq!(xs[0], 0.0);
    assert_eq!(xs[1].to_bits(), f64::NAN.to_bits());
    assert_eq!(xs[2], 5.0);
    assert_eq!(xs[3], 99.0);
    assert_eq!(ys, [45.0, 10.0, 20.0]);
}

#[test]
fn longitude_canonicalization_pins_aliases_neighbors_and_nonfinite() {
    let axis = degrees().longitude();
    for (input, expected) in [
        (0.0, 0.0),
        (-0.0, 0.0),
        (360.0, 0.0),
        (720.0, 0.0),
        (-720.0, 0.0),
        (180.0, -180.0),
        (-180.0, -180.0),
        (540.0, -180.0),
    ] {
        assert_bits(axis.canonicalize(input).unwrap().get(), expected);
    }
    assert_eq!(
        axis.canonicalize(180.0_f64.next_down()).unwrap().get(),
        180.0_f64.next_down()
    );
    assert!(axis.canonicalize(180.0_f64.next_up()).unwrap().get() < -179.0);
    axis.canonicalize(f64::NAN).unwrap_err();
    axis.canonicalize(f64::INFINITY).unwrap_err();
}

#[test]
fn nearest_longitude_alias_rounds_the_selected_source_translation_once() {
    let axis = degrees().longitude();
    assert_bits(
        axis.nearest_longitude_alias(-190.0, 177.813_015_064_085_82)
            .unwrap(),
        -182.186_984_935_914_18,
    );
}

#[test]
fn nearest_longitude_alias_fuses_an_integer_shift_rounded_to_the_exactness_boundary() {
    let axis = frame(std::f64::consts::TAU / 107.0).longitude();
    // 84_179_432_287_299 * 107 = 2^53 + 1, whose rounded product is 2^53.
    assert_bits(
        axis.nearest_longitude_alias(0.0, -(2.0_f64.powi(53)))
            .unwrap(),
        1.0,
    );
}

#[test]
fn nearest_longitude_alias_keeps_period_counts_larger_than_a_significand_exact() {
    let axis = degrees().longitude();
    // 27_777_777_777_777_778 * 360 - 10_000_000_000_000_000_000 = 80.
    assert_bits(axis.nearest_longitude_alias(1.0, -1.0e19).unwrap(), 80.0);
    assert_bits(axis.nearest_longitude_alias(-1.0, 1.0e19).unwrap(), -80.0);
    assert_bits(
        axis.nearest_longitude_alias(1.0, -f64::MAX).unwrap(),
        -128.0,
    );
    assert_bits(axis.nearest_longitude_alias(-1.0, f64::MAX).unwrap(), 128.0);

    let radians = frame(1.0).longitude();
    assert_bits(
        radians.nearest_longitude_alias(1.0, -1.0e19).unwrap(),
        exact_native_alias_oracle(-1.0e19, radians.period(), 1_591_549_430_918_953_420),
    );

    let grads = frame(std::f64::consts::TAU / 400.0).longitude();
    for (source, reference, periods) in [
        (-(1.0e19_f64.next_up()), 1.0, 25_000_000_000_000_005),
        (1.0e19_f64.next_up(), -1.0, -25_000_000_000_000_005),
    ] {
        assert_bits(
            grads.nearest_longitude_alias(reference, source).unwrap(),
            exact_native_alias_oracle(source, grads.period(), periods),
        );
    }

    let selected_period_tie = frame(std::f64::consts::TAU / 16.0_f64.next_down()).longitude();
    assert_bits(
        selected_period_tie
            .nearest_longitude_alias(0.0, -(2.0_f64.powi(54) + 8.0))
            .unwrap(),
        -8.0,
    );
}

#[test]
fn nearest_longitude_alias_forms_whole_period_shifts_from_the_source() {
    for axis in [
        degrees().longitude(),
        frame(std::f64::consts::TAU / 400.0).longitude(),
        frame(1.0).longitude(),
    ] {
        // A nearby reference is not an alias of the source. Select k from
        // the exact operands, then round source+kP instead of copying reference.
        assert_bits(
            axis.nearest_longitude_alias(1.0e-16, -axis.period())
                .unwrap(),
            0.0,
        );
        assert_bits(
            axis.nearest_longitude_alias(-1.0e-16, axis.period())
                .unwrap(),
            0.0,
        );
    }
}

#[test]
fn nearest_longitude_alias_matches_exact_native_period_shifts() {
    for (axis, longitudes) in [
        (degrees().longitude(), [
            177.813_015_064_085_82,
            -122.427_194,
            0.1,
            -0.1,
        ]),
        (frame(std::f64::consts::TAU / 400.0).longitude(), [
            177.813_015_064_085_82,
            -122.427_194,
            0.1,
            -0.1,
        ]),
        (frame(1.0).longitude(), [
            1.778_130_150_640_858_2,
            -1.224_271_94,
            0.1,
            -0.1,
        ]),
    ] {
        for longitude in longitudes {
            for periods in [-5_i32, -2, -1, 1, 2, 5] {
                let reference =
                    longitude + f64::from(periods) * axis.period() + axis.period() / 16.0;
                assert_bits(
                    axis.nearest_longitude_alias(reference, longitude).unwrap(),
                    exact_native_alias_oracle(longitude, axis.period(), i128::from(periods)),
                );
            }
        }
    }
}

// Align represented dyadics as integers, perform the translation exactly, then
// round once when converting the integer sum. Power-of-two scaling is exact for
// the normal-range representative inputs above.
fn exact_native_alias_oracle(longitude: f64, period: f64, periods: i128) -> f64 {
    let (longitude_significand, longitude_exponent) = decompose(longitude.abs());
    let (period_significand, period_exponent) = decompose(period);
    let exponent = longitude_exponent.min(period_exponent);
    let aligned = |significand: u64, source_exponent: i32| {
        i128::from(significand)
            .checked_shl(u32::try_from(source_exponent.checked_sub(exponent).unwrap()).unwrap())
            .unwrap()
    };
    let longitude = aligned(longitude_significand, longitude_exponent)
        .checked_mul(if longitude.is_sign_negative() { -1 } else { 1 })
        .unwrap();
    let shift = aligned(period_significand, period_exponent)
        .checked_mul(periods)
        .unwrap();
    let sum = longitude.checked_add(shift).unwrap();
    sum as f64 * 2.0_f64.powi(exponent)
}

#[test]
fn nearest_longitude_alias_reduces_before_rounding_and_keeps_tie_direction() {
    let axis = degrees().longitude();
    let source = 122.427_194_f64;
    let lifted = source + axis.period();
    let canonical = axis.canonicalize(lifted).unwrap().get();

    assert_bits(canonical, source.next_down());
    // Reducing the lifted displacement first recovers the chart spelling;
    // canonicalizing the rounded alias first loses one bit of the source.
    assert_bits(
        axis.nearest_longitude_alias(lifted, canonical).unwrap(),
        lifted,
    );
    assert_bits(axis.nearest_longitude_alias(530.0, 170.0).unwrap(), 530.0);

    // At a half-period tie, retain the route point's spelling in the
    // source-directed tie direction.
    assert_bits(axis.nearest_longitude_alias(180.0, 0.0).unwrap(), 0.0);
    assert_bits(axis.nearest_longitude_alias(0.0, 180.0).unwrap(), 180.0);
    assert_bits(axis.nearest_longitude_alias(540.0, 0.0).unwrap(), 360.0);
}

#[test]
fn nearest_longitude_alias_preserves_source_bits_for_no_shift() {
    let axis = degrees().longitude();
    let source = 0.2_f64;
    assert_eq!(
        axis.nearest_longitude_alias(78.0, source)
            .unwrap()
            .to_bits(),
        source.to_bits()
    );
    let negative_zero = -0.0;
    assert_eq!(
        axis.nearest_longitude_alias(negative_zero, negative_zero)
            .unwrap()
            .to_bits(),
        negative_zero.to_bits()
    );
}

#[test]
fn nearest_longitude_alias_pins_native_ties_and_finite_subtraction_overflow() {
    for axis in [
        degrees().longitude(),
        frame(std::f64::consts::TAU / 400.0).longitude(),
        frame(1.0).longitude(),
    ] {
        let half = axis.half_period();
        let period = axis.period();
        assert_bits(axis.nearest_longitude_alias(half, 0.0).unwrap(), 0.0);
        assert_bits(axis.nearest_longitude_alias(0.0, half).unwrap(), half);
        assert_bits(axis.nearest_longitude_alias(period, 0.0).unwrap(), period);
        assert_bits(axis.nearest_longitude_alias(-period, 0.0).unwrap(), -period);
        assert_bits(
            axis.nearest_longitude_alias(f64::MAX, -f64::MAX).unwrap(),
            f64::MAX,
        );
    }
}

#[test]
fn huge_finite_longitudes_match_binary_oracle() {
    let values = [
        f64::MAX,
        -f64::MAX,
        f64::from_bits((2040_u64 << 52) | 0x1234_5678_9ABC),
        f64::from_bits((1800_u64 << 52) | 1),
    ];
    for axis in [
        degrees().longitude(),
        frame(std::f64::consts::TAU / 400.0).longitude(),
        frame(1.0).longitude(),
    ] {
        for value in values {
            assert_bits(
                axis.canonicalize(value).unwrap().get(),
                exact_periodic_oracle(value, axis.period()),
            );
        }
    }
}

// Binary64 decomposition reduces the represented inputs as integers at a
// shared power-of-two scale; it does not call floating remainder.
fn exact_periodic_oracle(value: f64, period: f64) -> f64 {
    let negative = value.is_sign_negative();
    let (significand, exponent) = decompose(value.abs());
    let (period_significand, period_exponent) = decompose(period);
    let shift = exponent
        .checked_sub(period_exponent)
        .expect("binary64 exponent difference fits i32");
    assert!(shift >= 0);
    let mut remainder = significand.rem_euclid(period_significand);
    for _ in 0..shift {
        let doubled = u128::from(remainder)
            .checked_mul(2)
            .expect("a reduced binary64 significand fits in u128");
        remainder = doubled.rem_euclid(u128::from(period_significand)) as u64;
    }
    let mut result = (remainder as f64) * 2.0_f64.powi(period_exponent);
    if negative {
        result = -result;
    }
    let half = period / 2.0;
    if result < -half {
        result += period;
    }
    if result >= half {
        result -= period;
    }
    if result == 0.0 { 0.0 } else { result }
}

fn decompose(value: f64) -> (u64, i32) {
    let bits = value.to_bits();
    let biased = ((bits >> 52) & 0x7FF) as i32;
    assert!(biased > 0);
    (
        (bits & ((1_u64 << 52) - 1)) | (1_u64 << 52),
        biased
            .checked_sub(1023)
            .and_then(|exponent| exponent.checked_sub(52))
            .expect("normal binary64 exponent fits i32"),
    )
}

#[test]
fn latitude_admission_is_exact_at_poles_for_all_unit_scales() {
    assert_bits(Latitude::try_new(degrees(), -0.0).unwrap().get(), 0.0);
    for frame in [
        degrees(),
        frame(1.0),
        frame(std::f64::consts::PI / 200.0),
        frame(1.0e-300),
        frame(1.0e300),
    ] {
        let pole = frame.pole();
        for sign in [1.0, -1.0] {
            let exact = sign * pole;
            let inside = if sign > 0.0 {
                exact.next_down()
            } else {
                exact.next_up()
            };
            let admitted_outside = if sign > 0.0 {
                exact.next_up()
            } else {
                exact.next_down()
            };
            let rejected_outside = if sign > 0.0 {
                admitted_outside.next_up()
            } else {
                admitted_outside.next_down()
            };
            assert_bits(Latitude::try_new(frame, exact).unwrap().get(), exact);
            assert_bits(Latitude::try_new(frame, inside).unwrap().get(), inside);
            assert_bits(
                Latitude::try_new(frame, admitted_outside).unwrap().get(),
                exact,
            );
            Latitude::try_new(frame, rejected_outside).unwrap_err();
        }
    }
    Latitude::try_new(degrees(), f64::NAN).unwrap_err();
}

#[test]
fn shortest_alias_steps_preserve_half_turn_direction() {
    let axis = degrees().longitude();
    let edge: EdgeDirection = axis.shortest_alias_step(170.0, -170.0).unwrap();
    assert_eq!(edge, EdgeDirection::Minor(20.0));
    assert_eq!(edge.delta(axis), 20.0);
    assert_eq!(
        axis.shortest_alias_step(-170.0, 170.0).unwrap(),
        EdgeDirection::Minor(-20.0)
    );
    assert_eq!(
        axis.shortest_alias_step(0.0, 180.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Positive)
    );
    assert_eq!(
        axis.shortest_alias_step(0.0, -180.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Negative)
    );
    assert_eq!(
        axis.shortest_alias_step(0.0, 540.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Positive)
    );
    assert_eq!(
        axis.shortest_alias_step(0.0, -540.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Negative)
    );

    let grads = frame(std::f64::consts::PI / 200.0).longitude();
    assert_eq!(
        grads.shortest_alias_step(0.0, 400.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
    assert_eq!(
        grads.shortest_alias_step(0.0, -400.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
}

#[test]
fn intervals_distinguish_empty_arc_full_and_windows() {
    let axis = degrees().longitude();
    let ordered = CircularInterval::try_arc(axis, -20.0, 30.0).unwrap();
    assert_eq!(ordered.west_east(axis), Some((-20.0, 10.0)));
    assert_eq!(
        ordered.windows(axis),
        Some(LongitudeWindows::One([-20.0, 10.0]))
    );
    let wrapped = CircularInterval::try_arc(axis, 170.0, 30.0).unwrap();
    assert_eq!(wrapped.west_east(axis), Some((170.0, -160.0)));
    assert_eq!(
        wrapped.windows(axis),
        Some(LongitudeWindows::Two([170.0, 180.0], [-180.0, -160.0]))
    );
    assert_eq!(CircularInterval::Empty.windows(axis), None);
    assert_eq!(
        CircularInterval::Full.windows(axis),
        Some(LongitudeWindows::Full)
    );
    CircularInterval::try_arc(axis, 0.0, 360.0).unwrap_err();
}

#[test]
fn extreme_axis_interval_arithmetic_remains_finite() {
    let unit = factor(std::f64::consts::TAU / f64::MAX);
    let axis = PeriodicAxis::try_new("extreme unit", unit).unwrap();
    assert_eq!(axis.period(), f64::MAX);
    let start = axis.half_period().next_down();
    let sweep = axis.period().next_down();
    assert!((start + sweep).is_infinite());
    let arc = CircularInterval::try_arc(axis, start, sweep).unwrap();
    let expected_east = -axis.half_period() + (sweep - (axis.half_period() - start));
    assert_eq!(arc.west_east(axis), Some((start, expected_east)));
    let windows = arc.windows(axis).unwrap();
    assert_eq!(
        windows,
        LongitudeWindows::Two([start, axis.half_period()], [
            -axis.half_period(),
            expected_east
        ],)
    );
    for window in match windows {
        LongitudeWindows::Two(left, right) => [left, right],
        LongitudeWindows::One(window) => [window, window],
        LongitudeWindows::Full => [[-axis.half_period(), axis.half_period()]; 2],
    } {
        assert!(window[0].is_finite() && window[1].is_finite());
    }
    let point = CircularInterval::try_arc(axis, -axis.half_period(), 0.0).unwrap();
    let first = union_circular_intervals(axis, &[arc, point]);
    let second = union_circular_intervals(axis, &[arc, point]);
    assert_eq!(first, second);
    assert!(first.windows(axis).is_some_and(|windows| match windows {
        LongitudeWindows::One(window) => window.into_iter().all(f64::is_finite),
        LongitudeWindows::Two(left, right) => left.into_iter().chain(right).all(f64::is_finite),
        LongitudeWindows::Full => true,
    }));
}

#[test]
fn largest_gap_union_handles_disconnected_ties_and_full() {
    let axis = degrees().longitude();
    let arc = |start, sweep| CircularInterval::try_arc(axis, start, sweep).unwrap();
    let disconnected = union_circular_intervals(axis, &[arc(170.0, 5.0), arc(-5.0, 10.0)]);
    assert_eq!(disconnected, arc(-5.0, 180.0));
    let tie = union_circular_intervals(axis, &[
        arc(-135.0, 0.0),
        arc(-45.0, 0.0),
        arc(45.0, 0.0),
        arc(135.0, 0.0),
    ]);
    assert_eq!(tie, arc(-135.0, 270.0));
    assert_eq!(
        union_circular_intervals(axis, &[arc(-180.0, 180.0), arc(0.0, 180.0)]),
        CircularInterval::Full
    );
    assert_eq!(union_circular_intervals(axis, &[]), CircularInterval::Empty);
    assert_eq!(
        union_circular_intervals(axis, &[arc(1.0, 2.0)]),
        arc(1.0, 2.0)
    );
    assert_eq!(
        union_circular_intervals(axis, &[CircularInterval::Full]),
        CircularInterval::Full
    );
}

#[test]
fn seam_transits_preserve_direction_aliases_and_multiple_windings() {
    let axis = degrees().longitude();
    assert_eq!(axis.seam_transit(170.0, -170.0).unwrap(), 1);
    assert_eq!(axis.seam_transit(-170.0, 170.0).unwrap(), -1);
    assert_eq!(axis.seam_transit(0.0, 360.0).unwrap(), 0);
    assert_eq!(axis.seam_transit(0.0, -360.0).unwrap(), 0);
    assert_eq!(axis.seam_transit(0.0, 180.0).unwrap(), 1);
    assert_eq!(axis.seam_transit(0.0, -180.0).unwrap(), 0);
    assert_eq!(axis.seam_transit(180.0, 0.0).unwrap(), -1);
    assert_eq!(axis.seam_transit(-180.0, 0.0).unwrap(), 0);
    assert_eq!(axis.full_turn_delta(-180.0, 180.0), None);
    assert_eq!(axis.full_turn_delta(180.0, -180.0), None);
    assert_eq!(
        axis.shortest_alias_step(-180.0, 180.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
    assert_eq!(
        axis.shortest_alias_step(180.0, -180.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
    assert_eq!(axis.full_turn_delta(0.0, 360.0), Some(360.0));
    assert_eq!(axis.full_turn_delta(360.0, 0.0), Some(-360.0));
    let positive_circle = axis.chart_step(0.0, 360.0).unwrap();
    assert_eq!(positive_circle, ChartStep::Circle(TurnDirection::Positive));
    assert_eq!(positive_circle.transit(axis, 0.0, 360.0).unwrap(), 1);
    let negative_circle = axis.chart_step(360.0, 0.0).unwrap();
    assert_eq!(negative_circle, ChartStep::Circle(TurnDirection::Negative));
    assert_eq!(negative_circle.transit(axis, 360.0, 0.0).unwrap(), -1);
    assert_eq!(axis.full_turn_delta(1.0e-20, 360.0), None);
    assert_eq!(
        axis.chart_step(1.0e-20, 360.0).unwrap(),
        ChartStep::Shortest(EdgeDirection::Minor(-1.0e-20))
    );
    assert_eq!(
        axis.chart_step(180.0_f64.next_down(), -180.0).unwrap(),
        ChartStep::Shortest(EdgeDirection::Minor(2.0_f64.powi(-45)))
    );
    assert_eq!(
        axis.chart_step(-180.0, 180.0).unwrap(),
        ChartStep::Shortest(EdgeDirection::Minor(0.0))
    );
    assert_eq!(axis.chart_transit(-180.0, 180.0).unwrap(), 0);
    axis.seam_transit(f64::NAN, 0.0).unwrap_err();
    axis.seam_transit(0.0, f64::INFINITY).unwrap_err();

    let positive = ring(vec![0.0, 120.0, 240.0, 360.0, 480.0, 600.0, 720.0], vec![
        0.0;
        7
    ]);
    let positive_summary =
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&positive)).unwrap();
    assert_eq!(positive_summary.turn(), RingTurn::Other);
    assert_eq!(positive_summary.chart_turn, 2);
    let negative = ring(
        vec![0.0, -120.0, -240.0, -360.0, -480.0, -600.0, -720.0],
        vec![0.0; 7],
    );
    let negative_summary =
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&negative)).unwrap();
    assert_eq!(negative_summary.turn(), RingTurn::Other);
    assert_eq!(negative_summary.chart_turn, -2);
}

#[test]
fn chart_connections_keep_canonical_endpoints_and_integer_lifts() {
    let axis = degrees().longitude();
    let eastward = axis.chart_connection(170.0, -170.0).unwrap();
    assert_bits(eastward.start.get(), 170.0);
    assert_bits(eastward.end.get(), -170.0);
    assert_eq!(eastward.source_lift, 1);
    assert_eq!(eastward.source_lift, eastward.seam_transit());
    assert_eq!(
        eastward.step,
        ChartStep::Shortest(EdgeDirection::Minor(20.0))
    );

    let westward = axis.chart_connection(-170.0, 170.0).unwrap();
    assert_bits(westward.start.get(), -170.0);
    assert_bits(westward.end.get(), 170.0);
    assert_eq!(westward.source_lift, -1);
    assert_eq!(westward.source_lift, westward.seam_transit());
    assert_eq!(
        westward.step,
        ChartStep::Shortest(EdgeDirection::Minor(-20.0))
    );

    let circle = axis.chart_connection(0.0, 360.0).unwrap();
    assert_eq!(circle.step, ChartStep::Circle(TurnDirection::Positive));
    assert_eq!(circle.source_lift, 1);
    assert_eq!(circle.source_lift, circle.seam_transit());
    assert_bits(circle.start.get(), 0.0);
    assert_bits(circle.end.get(), 0.0);

    let proper_minor = axis.chart_connection(1.0e-20, 360.0).unwrap();
    assert_eq!(
        proper_minor.step,
        ChartStep::Shortest(EdgeDirection::Minor(-1.0e-20))
    );
    assert_eq!(proper_minor.source_lift, 0);
    assert_eq!(proper_minor.source_lift, proper_minor.seam_transit());
    assert_bits(proper_minor.start.get(), 1.0e-20);
    assert_bits(proper_minor.end.get(), 0.0);

    let positive_half = axis.chart_connection(0.0, 180.0).unwrap();
    assert_eq!(
        positive_half.step,
        ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Positive))
    );
    assert_eq!(positive_half.source_lift, 1);
    assert_eq!(positive_half.source_lift, positive_half.seam_transit());
    let negative_half = axis.chart_connection(0.0, -180.0).unwrap();
    assert_eq!(
        negative_half.step,
        ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Negative))
    );
    assert_eq!(negative_half.source_lift, 0);
    assert_eq!(negative_half.source_lift, negative_half.seam_transit());

    let seam_aliases = axis.chart_connection(-180.0, 180.0).unwrap();
    assert_bits(seam_aliases.start.get(), -180.0);
    assert_bits(seam_aliases.end.get(), -180.0);
    assert_eq!(seam_aliases.source_lift, 0);
    assert_eq!(seam_aliases.source_lift, seam_aliases.seam_transit());

    let positive_seam_start = axis.chart_connection(180.0, 0.0).unwrap();
    assert_bits(positive_seam_start.start.get(), -180.0);
    assert_bits(positive_seam_start.end.get(), 0.0);
    assert_eq!(positive_seam_start.source_lift, -1);
    assert_eq!(
        positive_seam_start.source_lift,
        positive_seam_start.seam_transit()
    );
}

#[test]
fn rounded_half_period_span_uses_exact_chart_transit() {
    let axis = degrees().longitude();
    let from = -90.0;
    let to = 90.0_f64.next_up();
    assert_bits(to - from, 180.0);
    assert!(!axis.ordinary_chart_edge(from, to));

    let connection = axis.chart_connection(from, to).unwrap();
    assert_eq!(connection.source_lift, -1);
    assert!(matches!(
        connection.step,
        ChartStep::Shortest(EdgeDirection::Minor(delta)) if delta.is_sign_negative()
    ));
    assert_eq!(connection.seam_transit(), -1);
    assert_eq!(axis.chart_transit(from, to).unwrap(), -1);
}

#[test]
fn seam_transit_summary_accepts_the_polar_cap_without_numeric_winding() {
    let cap = ring(vec![0.1, 90.1, -179.8, -89.7], vec![80.0; 4]);
    let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&cap)).unwrap();
    assert_eq!(summary.turn(), RingTurn::Full(TurnDirection::Positive));
    assert!(!summary.exact_axis_band);
    assert_eq!(
        summary.domain,
        RingDomain::FullLongitude(RingLatitudeSpan::Ordinary)
    );
}

#[test]
fn identity_and_expansion_carriers_are_explicit() {
    assert_ne!(SeamIdentity::Seam, SeamIdentity::Pole);
    assert_eq!(SeamIdentity::Ordinary, SeamIdentity::Ordinary);
    let frame = degrees();
    assert_eq!(
        frame
            .longitude()
            .canonicalize(180.0)
            .unwrap()
            .identity(frame.longitude()),
        SeamIdentity::Seam
    );
    assert_eq!(
        Latitude::try_new(frame, 90.0).unwrap().identity(frame),
        SeamIdentity::Pole
    );
    assert_eq!(frame.longitude().seam_transit(170.0, -170.0).unwrap(), 1);
}
