use super::*;

#[test]
fn geographic_ring_keeps_nested_visible_window_after_parent_drop() {
    let (ring, visible_x_address) =
        nested_window_ring(vec![91.0, -91.0, 60.0, 60.0, 70.0, 60.0, 91.0]);

    assert_eq!(ring.len(), 4);
    assert_eq!(ring.coords().xs(), &[10.0, 20.0, 20.0, 10.0]);
    assert_eq!(ring.coords().ys(), &[60.0, 60.0, 70.0, 60.0]);
    assert_eq!(ring.coords().xs().as_ptr().addr(), visible_x_address);

    let geographic = GeographicRing::from_ring(&ring);
    assert_eq!(geographic.native_xs(), &[10.0, 20.0, 20.0, 10.0]);
    assert_eq!(geographic.native_xs().as_ptr().addr(), visible_x_address);
    let summary = RingSummary::try_new(degrees(), &geographic).unwrap();
    assert_eq!(summary.chart_turn, 0);
    assert_eq!(summary.domain, RingDomain::Ordinary);
    assert_eq!(summary.south.get(), 60.0);
    assert_eq!(summary.north.get(), 70.0);
}
#[test]
fn geographic_ring_reports_local_coordinate_index_for_nested_window() {
    let (ring, visible_x_address) =
        nested_window_ring(vec![91.0, -91.0, 60.0, 60.0, 91.0, 60.0, 91.0]);
    assert_eq!(ring.coords().xs().as_ptr().addr(), visible_x_address);

    let error = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&ring)).unwrap_err();
    assert_eq!(error.location.coordinate, 2);
    assert_bits(error.native.x, 20.0);
    assert_bits(error.native.y, 91.0);
    assert!(matches!(
        error.source,
        GeographicError::LatitudeOutOfRange { .. }
    ));
}
#[test]
fn scalar_errors_preserve_nested_logical_members_and_ring_indices() {
    use crate::geometry::{
        CoordinateAxes, LineSeq, MultiLineString, MultiMember, MultiPolygon, Polygon, Shape,
        SharedSlice,
    };

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

    let valid_line =
        LineSeq::try_new(xy_seq(vec![0.0, 1.0], vec![0.0, 1.0])).expect("valid XY line");
    let invalid_line = LineSeq::try_new(xy_seq(vec![0.0, 1.0], vec![0.0, 91.0]))
        .expect("finite line outside the latitude range");
    let multipart = MultiLineString::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(valid_line),
        MultiMember::Value(invalid_line.clone()),
        MultiMember::Value(invalid_line),
    ])
    .expect("multipart lines have homogeneous XY axes");
    let nested_line = Shape::geometry_collection(vec![
        Shape::geometry_collection(vec![Shape::MultiLineString(multipart)])
            .expect("test collection is valid"),
    ])
    .expect("test collection is valid");
    let error = ScalarGeographicView::try_new(degrees(), &nested_line)
        .expect_err("the first invalid line member must be rejected");
    assert_eq!(error.location.members.as_slice(), [0, 0, 2]);
    assert_eq!(error.location.ring, None);
    assert_eq!(error.location.coordinate, 1);
    assert_bits(error.native.x, 1.0);
    assert_bits(error.native.y, 91.0);
    assert!(matches!(
        error.source,
        GeographicError::LatitudeOutOfRange { .. }
    ));
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
#[test]
fn member_path_inline_depth_preserves_literal_logical_ordinals() {
    use crate::geometry::Shape;

    let shape = nested_member_path(&[2, 1, 3, 0], Shape::Polygon(invalid_hole_polygon()));
    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();

    assert_eq!(error.location.members.as_slice(), &[2, 1, 3, 0]);
    assert_eq!(error.location.ring, Some(1));
    assert_eq!(error.location.coordinate, 2);
    assert_bits(error.native.x, 1.5);
    assert_bits(error.native.y, 91.0);
    assert!(matches!(
        error.source,
        GeographicError::LatitudeOutOfRange { .. }
    ));
}
#[test]
fn member_path_spill_pop_preserves_the_sibling_path() {
    use crate::geometry::{CoordinateAxes, MultiMember, MultiPolygon, Polygon, Shape};

    let valid = Polygon::new(
        ring_from_points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]),
        Vec::new(),
    );
    let multipart = Shape::MultiPolygon(
        MultiPolygon::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(valid),
            MultiMember::Empty,
            MultiMember::Value(invalid_hole_polygon()),
        ])
        .unwrap(),
    );
    let shape = nested_member_path(&[2, 1, 3, 0], multipart);
    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();

    assert_eq!(error.location.members.as_slice(), &[2, 1, 3, 0, 2]);
    assert_eq!(error.location.ring, Some(1));
    assert_eq!(error.location.coordinate, 2);
    assert_bits(error.native.x, 1.5);
    assert_bits(error.native.y, 91.0);
}
#[test]
fn member_path_depth_six_preserves_nested_empty_ordinals() {
    use crate::geometry::{
        CoordinateAxes, GeometryKind, MultiMember, MultiPolygon, Point, Polygon, Shape,
    };

    let valid = Polygon::new(
        ring_from_points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]),
        Vec::new(),
    );
    let multipart = Shape::MultiPolygon(
        MultiPolygon::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(valid),
            MultiMember::Empty,
            MultiMember::Value(invalid_hole_polygon()),
        ])
        .unwrap(),
    );
    let branch = Shape::geometry_collection(vec![
        Shape::Point(Point::new(5.0, 6.0).unwrap()),
        Shape::Empty(GeometryKind::Point, CoordinateAxes::XY),
        multipart,
    ])
    .unwrap();
    let shape = nested_member_path(&[2, 1, 3, 0], branch);
    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();

    assert_eq!(error.location.members.as_slice(), &[2, 1, 3, 0, 2, 3]);
    assert_eq!(error.location.ring, Some(1));
    assert_eq!(error.location.coordinate, 2);
    assert_bits(error.native.x, 1.5);
    assert_bits(error.native.y, 91.0);
}
#[test]
fn member_path_spill_unwinds_before_outer_sibling() {
    use crate::geometry::{CoordinateAxes, GeometryKind, Point, Shape};

    let deep_success = nested_member_path(&[1, 0], Shape::Point(Point::new(5.0, 6.0).unwrap()));
    let branch = Shape::geometry_collection(vec![
        deep_success,
        Shape::Empty(GeometryKind::Point, CoordinateAxes::XY),
        Shape::Empty(GeometryKind::Point, CoordinateAxes::XY),
        Shape::Polygon(invalid_hole_polygon()),
    ])
    .unwrap();
    let shape = nested_member_path(&[2, 1, 3], branch);
    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();

    assert_eq!(error.location.members.as_slice(), &[2, 1, 3, 3]);
    assert_eq!(error.location.ring, Some(1));
    assert_eq!(error.location.coordinate, 2);
}
#[test]
fn pending_ring_gate_keeps_spilled_member_path_after_later_pops() {
    use crate::geometry::{
        CoordinateAxes, GeometryKind, MultiMember, MultiPolygon, Point, Polygon, Shape,
    };

    let ambiguous = ring(vec![0.0, 360.0, 300.0, 0.0], vec![
        -10.0, -10.0, 10.0, -10.0,
    ]);
    let multipart = Shape::MultiPolygon(
        MultiPolygon::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Empty,
            MultiMember::Value(Polygon::new(ambiguous, Vec::new())),
        ])
        .unwrap(),
    );
    let later_success = nested_member_path(&[1, 0], Shape::Point(Point::new(5.0, 6.0).unwrap()));
    let branch = Shape::geometry_collection(vec![
        multipart,
        later_success,
        Shape::Empty(GeometryKind::Point, CoordinateAxes::XY),
    ])
    .unwrap();
    let shape = nested_member_path(&[2, 1, 3], branch);
    let error = ScalarGeographicView::try_new(degrees(), &shape).unwrap_err();

    assert_eq!(error.location.members.as_slice(), &[2, 1, 3, 0, 2]);
    assert_eq!(error.location.ring, Some(0));
    assert_eq!(error.location.coordinate, 0);
    assert!(matches!(
        error.source,
        GeographicError::AmbiguousFullTurnRing { .. }
    ));
}
