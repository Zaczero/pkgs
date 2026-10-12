#[test]
fn packed_polygon_windows_with_unequal_prefixes_equal_without_buffering() {
    let left_points = concat3(
        &closed_box(9.0, 9.0, 9.5, 9.5),
        &closed_box(1.0, 0.0, 3.0, 2.0),
        &closed_box(4.0, 0.0, 5.0, 1.0),
    );
    let left = packed_polygons(
        CoordSeq::try_from_points(&left_points).unwrap(),
        vec![0, 5, 10, 15],
        vec![0, 1, 2, 3],
        1..3,
    );
    let prefix = [pt(8.0, 8.0), pt(8.0, 7.0), pt(7.0, 8.0), pt(8.0, 8.0)];
    let vis0 = closed_box(1.0, 0.0, 3.0, 2.0);
    let vis1 = closed_box(4.0, 0.0, 5.0, 1.0);
    let mut right_points = [pt(0.0, 0.0); 14];
    right_points[..4].copy_from_slice(&prefix);
    right_points[4..9].copy_from_slice(&vis0);
    right_points[9..].copy_from_slice(&vis1);
    let right = packed_polygons(
        CoordSeq::try_from_points(&right_points).unwrap(),
        vec![0, 4, 9, 14],
        vec![0, 1, 2, 3],
        1..3,
    );
    let left_view = left.coordinate_view();
    let right_view = right.coordinate_view();
    assert_eq!(left_view.len(), 10);
    assert_eq!(right_view.len(), 10);
    let equal = crate::test_support::assert_no_allocations(|| {
        left_view.equal_visible(&right_view, None, None)
    });
    assert!(equal);
    let mut mismatched = right_points;
    mismatched[6] = pt(1.5, 0.5);
    let other = packed_polygons(
        CoordSeq::try_from_points(&mismatched).unwrap(),
        vec![0, 4, 9, 14],
        vec![0, 1, 2, 3],
        1..3,
    );
    assert!(!left_view.equal_visible(&other.coordinate_view(), None, None));
}

#[test]
fn packed_polygon_empty_windows_compare_equal() {
    let left = packed_polygons(
        CoordSeq::try_from_points(&closed_box(0.0, 0.0, 1.0, 1.0)).unwrap(),
        vec![0, 5],
        vec![0, 1],
        0..0,
    );
    let right = packed_polygons(
        CoordSeq::try_from_points(&[pt(2.0, 2.0), pt(3.0, 2.0), pt(2.0, 3.0), pt(2.0, 2.0)])
            .unwrap(),
        vec![0, 4],
        vec![0, 1],
        0..0,
    );
    assert_eq!(left.coordinate_view().len(), 0);
    assert_eq!(right.coordinate_view().len(), 0);
    assert!(
        left.coordinate_view()
            .equal_visible(&right.coordinate_view(), None, None)
    );
}

#[test]
fn packed_polygon_window_zm_compares_optional_axes() {
    use crate::geometry::{MOrdinate, ZOrdinate};
    let ring = |z: f64, m: f64| {
        [
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            Point::new_axes(1.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            Point::new_axes(1.0, 1.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            Point::new_axes(0.0, 1.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
            Point::new_axes(0.0, 0.0, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap(),
        ]
    };
    let mut left_points = [pt(0.0, 0.0); 10];
    left_points[..5].copy_from_slice(&ring(0.0, 10.0));
    left_points[5..].copy_from_slice(&ring(1.0, 20.0));
    let mut right_points = left_points;
    let prefix_start =
        Point::new_axes(2.0, 2.0, ZOrdinate(Some(9.0)), MOrdinate(Some(10.0))).unwrap();
    right_points[0] = prefix_start;
    right_points[4] = prefix_start;
    let left = packed_polygons(
        CoordSeq::try_from_points(&left_points).unwrap(),
        vec![0, 5, 10],
        vec![0, 1, 2],
        1..2,
    );
    let right = packed_polygons(
        CoordSeq::try_from_points(&right_points).unwrap(),
        vec![0, 5, 10],
        vec![0, 1, 2],
        1..2,
    );
    assert!(
        left.coordinate_view()
            .equal_visible(&right.coordinate_view(), None, None)
    );
    let mut z_mismatch = right_points;
    let z_start = Point::new_axes(0.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(20.0))).unwrap();
    z_mismatch[5] = z_start;
    z_mismatch[9] = z_start;
    let z_other = packed_polygons(
        CoordSeq::try_from_points(&z_mismatch).unwrap(),
        vec![0, 5, 10],
        vec![0, 1, 2],
        1..2,
    );
    assert!(
        !left
            .coordinate_view()
            .equal_visible(&z_other.coordinate_view(), None, None)
    );
    let mut m_mismatch = right_points;
    m_mismatch[6] = Point::new_axes(1.0, 0.0, ZOrdinate(Some(1.0)), MOrdinate(Some(99.0))).unwrap();
    let m_other = packed_polygons(
        CoordSeq::try_from_points(&m_mismatch).unwrap(),
        vec![0, 5, 10],
        vec![0, 1, 2],
        1..2,
    );
    assert!(
        !left
            .coordinate_view()
            .equal_visible(&m_other.coordinate_view(), None, None)
    );
}
