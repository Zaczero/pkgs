use super::*;

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
#[rstest::rstest]
#[case::ring_vec_180_0_90_0_0_0_90_0_180_0_vec_5_0_5_0_5_0_5_0_5_0(ring(vec![-180.0, -90.0, 0.0, 90.0, 180.0], vec![
            -5.0, 5.0, 5.0, 5.0, -5.0,
        ]))]
#[case::ring_from_points_0_0_20_0_120_0_20_0_240_0_20_0_9b44bbc7(ring_from_points(&[
    (0.0, 20.0),
    (120.0, -20.0),
    (240.0, 20.0),
    (360.0, -20.0),
    (480.0, 20.0),
    (600.0, -20.0),
    (0.0, 20.0),
]))]
fn coordinate_domain_errors_precede_pending_ambiguous_ring_gates(#[case] shell: crate::Ring) {
    let hole = ring(vec![10.0, 20.0, 20.0, 10.0], vec![
        100.0, 100.0, 101.0, 100.0,
    ]);
    let shape = crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(shell, vec![hole]));

    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();
    assert_eq!(error.location.ring, Some(1));
    assert_eq!(error.location.coordinate, 0);
    assert!(matches!(
        error.source,
        GeographicError::LatitudeOutOfRange { .. }
    ));
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
