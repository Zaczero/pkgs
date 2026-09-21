use super::value::EdgeDirection;
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

fn assert_bits(value: f64, expected: f64) {
    assert_eq!(value.to_bits(), expected.to_bits());
}

fn xy_seq(xs: Vec<f64>, ys: Vec<f64>) -> crate::geometry::CoordSeq {
    crate::geometry::CoordSeq::try_from_vecs(xs, ys, None, None).unwrap()
}

fn ring(xs: Vec<f64>, ys: Vec<f64>) -> crate::geometry::Ring {
    crate::geometry::Ring::closed_coordseq(xy_seq(xs, ys)).unwrap()
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
    assert_bits(coordinate.native().x, 540.0);
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
    let grads = frame(std::f64::consts::TAU / 400.0);
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
fn latitude_admission_is_one_exterior_ulp_and_contextual() {
    let pole = degrees().pole();
    let admitted =
        crate::geometry::Shape::Point(crate::geometry::Point::new(0.0, pole.next_up()).unwrap());
    let view = ScalarGeographicView::try_new(degrees(), &admitted).unwrap();
    view.for_each_coordinate(|coordinate| {
        assert_bits(coordinate.latitude().get(), pole);
        assert_bits(coordinate.native().y, pole.next_up());
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
fn ring_summary_preserves_exact_full_turn_direction_and_world_band() {
    let positive = ring(vec![0.0, 120.0, 240.0, 360.0], vec![
        -90.0, 90.0, 90.0, -90.0,
    ]);
    let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&positive)).unwrap();
    assert_eq!(summary.turn, RingTurn::Full(TurnDirection::Positive));
    assert_eq!(
        summary.domain,
        RingDomain::FullLongitude(RingLatitudeSpan::PoleToPole)
    );

    let negative = ring(vec![0.0, -120.0, -240.0, -360.0], vec![0.0; 4]);
    assert_eq!(
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&negative))
            .unwrap()
            .turn,
        RingTurn::Full(TurnDirection::Negative)
    );
    let retraced = ring(vec![0.0, 120.0, 0.0, 0.0], vec![0.0; 4]);
    assert_eq!(
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&retraced))
            .unwrap()
            .turn,
        RingTurn::None
    );
}

fn axis_box(frame: GeographicFrame, west: f64, east: f64, south: f64, north: f64) -> RingSummary {
    let ring = ring(vec![west, east, east, west, west], vec![
        south, south, north, north, south,
    ]);
    RingSummary::try_new(frame, &GeographicRing::from_ring(&ring)).unwrap()
}

#[test]
fn exact_axis_boxes_are_world_bands_without_fabricating_turns() {
    for frame in [degrees(), frame(1.0), frame(std::f64::consts::TAU / 400.0)] {
        let half = frame.longitude().half_period();
        let summary = axis_box(frame, -half, half, -frame.pole() / 2.0, frame.pole() / 2.0);
        assert_eq!(summary.turn, RingTurn::None);
        assert_eq!(
            summary.domain,
            RingDomain::FullLongitude(RingLatitudeSpan::Ordinary)
        );
        let reverse = axis_box(frame, half, -half, -frame.pole(), frame.pole());
        assert_eq!(reverse.turn, RingTurn::None);
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
        RingDomain::Ordinary
    );
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
    let multipart = MultiPolygon::from_values(CoordinateAxes::XY, vec![
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
fn admitted_walk_keeps_nested_empty_order_axes_wrap_and_ulp() {
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
    assert_bits(seen[0].native().x, 540.0);
    assert_bits(seen[0].longitude().get(), -180.0);
    assert_eq!(seen[0].native().z(), Some(3.0));
    assert_eq!(seen[0].native().m(), Some(4.0));
    assert_bits(seen[1].latitude().get(), pole);
    assert_bits(seen[1].native().y, pole.next_up());
    assert_bits(seen[2].native().x, 10.0);
    assert_bits(seen[2].native().y, 1.0);
    assert_bits(seen[3].native().x, 20.0);
    assert_bits(seen[3].native().y, 2.0);
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
    let multipart = MultiPolygon::from_values(CoordinateAxes::XY, vec![
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
    assert_eq!(grad.longitude().period(), 399.999_999_999_999_94);
    assert_eq!(grad.pole(), 99.999_999_999_999_99);
    let radian = frame(1.0);
    assert_eq!(radian.longitude().period(), std::f64::consts::TAU);
    assert_eq!(radian.pole(), std::f64::consts::FRAC_PI_2);
    assert_eq!(
        degree.longitude().unit().radians_per_unit(),
        std::f64::consts::PI / 180.0
    );
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
    let shift = exponent - period_exponent;
    assert!(shift >= 0);
    let mut remainder = significand % period_significand;
    for _ in 0..shift {
        remainder = ((u128::from(remainder) * 2) % u128::from(period_significand)) as u64;
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
        biased - 1023 - 52,
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
fn shortest_edges_preserve_half_turn_direction() {
    let axis = degrees().longitude();
    let edge: EdgeDirection = axis.shortest_edge(170.0, -170.0).unwrap();
    assert_eq!(edge, EdgeDirection::Minor(20.0));
    assert_eq!(edge.delta(axis), 20.0);
    assert_eq!(
        axis.shortest_edge(-170.0, 170.0).unwrap(),
        EdgeDirection::Minor(-20.0)
    );
    assert_eq!(
        axis.shortest_edge(0.0, 180.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Positive)
    );
    assert_eq!(
        axis.shortest_edge(0.0, -180.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Negative)
    );
    assert_eq!(
        axis.shortest_edge(0.0, 540.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Positive)
    );
    assert_eq!(
        axis.shortest_edge(0.0, -540.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Negative)
    );

    let grads = frame(std::f64::consts::PI / 200.0).longitude();
    assert_eq!(
        grads.shortest_edge(0.0, 400.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
    assert_eq!(
        grads.shortest_edge(0.0, -400.0).unwrap(),
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
    let first = union_circular_intervals(axis, [arc, point]);
    let second = union_circular_intervals(axis, [arc, point]);
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
    let disconnected = union_circular_intervals(axis, [arc(170.0, 5.0), arc(-5.0, 10.0)]);
    assert_eq!(disconnected, arc(-5.0, 180.0));
    let tie = union_circular_intervals(axis, [
        arc(-135.0, 0.0),
        arc(-45.0, 0.0),
        arc(45.0, 0.0),
        arc(135.0, 0.0),
    ]);
    assert_eq!(tie, arc(-135.0, 270.0));
    assert_eq!(
        union_circular_intervals(axis, [arc(-180.0, 180.0), arc(0.0, 180.0)]),
        CircularInterval::Full
    );
    assert_eq!(union_circular_intervals(axis, []), CircularInterval::Empty);
    assert_eq!(
        union_circular_intervals(axis, [arc(1.0, 2.0)]),
        arc(1.0, 2.0)
    );
    assert_eq!(
        union_circular_intervals(axis, [CircularInterval::Full]),
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
    axis.seam_transit(f64::NAN, 0.0).unwrap_err();
    axis.seam_transit(0.0, f64::INFINITY).unwrap_err();

    let positive = ring(vec![0.0, 120.0, 240.0, 360.0, 480.0, 600.0, 720.0], vec![
        0.0;
        7
    ]);
    assert_eq!(
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&positive))
            .unwrap()
            .turn,
        RingTurn::Other
    );
    let negative = ring(
        vec![0.0, -120.0, -240.0, -360.0, -480.0, -600.0, -720.0],
        vec![0.0; 7],
    );
    assert_eq!(
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&negative))
            .unwrap()
            .turn,
        RingTurn::Other
    );
}

#[test]
fn seam_transit_summary_accepts_the_polar_cap_without_numeric_winding() {
    let cap = ring(vec![0.1, 90.1, -179.8, -89.7], vec![80.0; 4]);
    let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&cap)).unwrap();
    assert_eq!(summary.turn, RingTurn::Full(TurnDirection::Positive));
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
