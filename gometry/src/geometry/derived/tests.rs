use super::{native_concave_hull, point_xy_cmp};
use crate::geometry::{CoordSeq, Point, Shape, empty_geometry};

fn point(x: f64, y: f64) -> Point {
    Point::new_unchecked_xy(x, y)
}

fn hull_points() -> Vec<Point> {
    vec![
        point(0.0, 0.0),
        point(4.0, 0.0),
        point(4.0, 4.0),
        point(2.0, 1.0),
        point(0.0, 4.0),
        point(1.0, 2.0),
        point(3.0, 2.0),
        point(2.0, 3.0),
    ]
}

fn multipoint(points: &[Point]) -> Shape {
    Shape::MultiPoint(CoordSeq::from_points(points))
}

#[test]
fn convex_hull_dedups_signed_zero_duplicates() {
    let points = vec![point(0.0, 0.0), point(-0.0, 0.0), point(0.0, -0.0)];
    let hull = multipoint(&points).convex_hull().expect("convex hull");
    assert!(hull.equals(&Shape::Point(point(0.0, 0.0))));
    assert!(!matches!(hull, Shape::LineString(_)));
}

fn assert_covers_points(hull: &Shape, points: &[Point]) {
    for &point in points {
        assert!(hull.covers(&Shape::Point(point)));
    }
}

#[test]
fn native_concave_hull_contract_invariants_and_reference_envelope() {
    let mut points = hull_points();
    points.sort_unstable_by(point_xy_cmp);
    let source = multipoint(&points);
    let convex = source.convex_hull().expect("convex hull").area();
    let shape_from_indices = |indices: Vec<usize>| {
        let hull: Vec<Point> = indices.into_iter().map(|index| points[index]).collect();
        super::shape_from_open_hull(&hull, empty_geometry)
    };
    let native = shape_from_indices(native_concave_hull(&points, 1.0, 0.0));
    let native_lo = shape_from_indices(native_concave_hull(&points, 0.75, 0.0));
    let native_hi = shape_from_indices(native_concave_hull(&points, 3.0, 0.0));
    let native_threshold = shape_from_indices(native_concave_hull(&points, 0.75, 5.0));

    assert_covers_points(&native, &points);
    assert!(native.area() <= convex);
    assert!(native_lo.area() <= native_hi.area());
    assert!(native_threshold.area() >= native_lo.area());
}

#[test]
fn concave_hull_is_invariant_to_input_point_order() {
    fn permute(points: &mut [Point], index: usize, expected: &Shape) {
        if index == points.len() {
            let hull = multipoint(points)
                .concave_hull(1.0, 0.0)
                .expect("concave hull");
            assert!(hull.equals_exact(expected, 0.0, false, false));
            return;
        }
        for swap in index..points.len() {
            points.swap(index, swap);
            permute(points, index + 1, expected);
            points.swap(index, swap);
        }
    }

    let mut points = vec![
        point(0.0, 0.0),
        point(4.0, 0.0),
        point(4.0, 4.0),
        point(0.0, 4.0),
        point(2.0, 1.0),
        point(1.0, 3.0),
    ];
    let expected = multipoint(&points)
        .concave_hull(1.0, 0.0)
        .expect("concave hull");
    permute(&mut points, 0, &expected);
}
