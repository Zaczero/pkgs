use crate::geometry::derived::concave::point_xy_cmp;
use crate::geometry::derived::native_concave_hull;
use crate::geometry::{CoordSeq, LineSeq, Point, Shape, empty_geometry};

fn minimum_area_rectangle_area(hull: &[Point]) -> f64 {
    let ring = super::minimum_area_rectangle(hull).expect("finite rectangle");
    crate::geometry::open_point_cycle_decision(
        &(&ring).into_iter().take(ring.len() - 1).collect::<Vec<_>>(),
    )
    .magnitude()
    .get()
}

#[test]
fn minimum_area_rectangle_axis_aligned_square() {
    let square = vec![
        point(0.0, 0.0),
        point(4.0, 0.0),
        point(4.0, 4.0),
        point(0.0, 4.0),
    ];
    let hull = super::monotone_chain_hull(&square);
    assert!((minimum_area_rectangle_area(&hull) - 16.0).abs() <= 1.0e-12);
}

#[test]
fn minimum_area_rectangle_equilateral_triangle() {
    let side = 4.0;
    let height = side * 3.0_f64.sqrt() / 2.0;
    let hull =
        super::monotone_chain_hull(&[point(0.0, 0.0), point(side, 0.0), point(side / 2.0, height)]);
    assert!((minimum_area_rectangle_area(&hull) - side * height).abs() <= 1.0e-9);
}

#[test]
fn minimum_rotated_rectangle_multipoint_matches_hull_rectangle() {
    let points = vec![
        point(0.0, 0.0),
        point(3.0, 0.0),
        point(1.0, 2.0),
        point(4.0, 1.0),
    ];
    let multipoint = Shape::MultiPoint(CoordSeq::from_points(&points));
    let rectangle = multipoint
        .minimum_rotated_rectangle()
        .expect("minimum rotated rectangle");
    let hull = super::monotone_chain_hull(&points);
    let expected_area = minimum_area_rectangle_area(&hull);
    assert!((rectangle.area() - expected_area).abs() <= 1.0e-12);
    assert!(expected_area > 0.0);
}

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

#[test]
fn lineal_centroid_keeps_world_metric_weights_in_an_axis_frame() {
    let line = |points: &[(f64, f64)]| {
        LineSeq::try_new(CoordSeq::from_points(
            &points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>(),
        ))
        .expect("two-point test line")
    };
    let unequal = Shape::MultiLineString(vec![
        line(&[(-1e308, 0.0), (1e308, 0.0)]),
        line(&[(-9e307, 10.0), (9e307, 10.0)]),
    ]);
    let (_, y) = super::lineal_centroid(&unequal).expect("lineal centroid");
    assert!((y - 4.736_842_105_263_158).abs() <= 1e-14);

    let opposite = Shape::MultiLineString(vec![
        line(&[(1e308, -1e308), (1e308, 1e308)]),
        line(&[(-1e308, -1e308), (-1e308, 1e308)]),
    ]);
    let (x, y) = super::lineal_centroid(&opposite).expect("lineal centroid");
    assert_eq!(x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(y.to_bits(), 0.0_f64.to_bits());
}
