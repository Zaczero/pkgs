use super::multipart::dense_multi_point_coordinates;
use crate::geometry::{CoordSeq, CoordinateAxes, MOrdinate, Point, Shape, ZOrdinate};
use crate::test_support::allocation_count;

fn assert_matches_staged_coordinates(points: &[Point], axes: CoordinateAxes) {
    let members: Vec<_> = points.iter().copied().map(Shape::Point).collect();
    let staged_members = members.clone();

    let (staged, staged_allocations) = allocation_count(|| {
        let mut staged_points = Vec::with_capacity(staged_members.len());
        for member in staged_members {
            let Shape::Point(point) = member else {
                unreachable!("staged MultiPoint members are points");
            };
            staged_points.push(point);
        }
        CoordSeq::try_from_points(&staged_points).expect("admitted points form coordinates")
    });
    let (direct, direct_allocations) = allocation_count(|| {
        dense_multi_point_coordinates(members, axes).expect("admitted points form coordinates")
    });

    assert_eq!(staged_allocations, direct_allocations.saturating_add(1));
    assert_eq!(staged.axes(), direct.axes());
    assert_eq!(
        staged.points().collect::<Vec<_>>(),
        direct.points().collect::<Vec<_>>()
    );
}

#[test]
fn dense_xy_multi_point_columns_remove_the_point_staging_allocation() {
    let points = [
        Point::new_raw_axes(1.0, 2.0, ZOrdinate(None), MOrdinate(None)),
        Point::new_raw_axes(3.0, 4.0, ZOrdinate(None), MOrdinate(None)),
        Point::new_raw_axes(5.0, 6.0, ZOrdinate(None), MOrdinate(None)),
    ];
    assert_matches_staged_coordinates(&points, CoordinateAxes::XY);
}

#[test]
fn dense_xyzm_multi_point_columns_remove_the_point_staging_allocation() {
    let points = [
        Point::new_raw_axes(1.0, 2.0, ZOrdinate(Some(3.0)), MOrdinate(Some(4.0))),
        Point::new_raw_axes(5.0, 6.0, ZOrdinate(Some(7.0)), MOrdinate(Some(8.0))),
        Point::new_raw_axes(9.0, 10.0, ZOrdinate(Some(11.0)), MOrdinate(Some(12.0))),
    ];
    assert_matches_staged_coordinates(&points, CoordinateAxes::XYZM);
}
