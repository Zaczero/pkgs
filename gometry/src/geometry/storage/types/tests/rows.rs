use super::*;

#[test]
fn mixed_row_views_borrow_without_allocating_and_reject_other_kinds() {
    let coords =
        CoordSeq::try_from_vecs(vec![2.0, 3.0], vec![4.0, 5.0], Some(vec![6.0, 7.0]), None)
            .unwrap();
    let point = Point::new_raw_xy(0.0, 1.0);
    let points =
        GeometryArrayStorage::Mixed(vec![Shape::Point(point), Shape::Point(coords.point_at(1))]);
    let line = Shape::LineString(LineSeq::from_proven(coords.clone()));
    let lines = GeometryArrayStorage::Mixed(vec![line.clone()]);
    let other = GeometryArrayStorage::Mixed(vec![Shape::Point(point), line]);
    let ((point_rows, line_rows), allocations) = crate::test_support::allocation_count(|| {
        (points.point_rows().unwrap(), lines.line_rows().unwrap())
    });
    assert_eq!(allocations, 0);
    assert_eq!(point_rows.len(), 2);
    assert_eq!(point_rows.get(0), point);
    assert_eq!(point_rows.get(1), coords.point_at(1));
    assert_eq!(line_rows.row_xy(0), (coords.xs(), coords.ys()));
    assert!(line_rows.packed_column_view().is_none());
    assert!(other.point_rows().is_none());
    assert!(other.line_rows().is_none());
}
