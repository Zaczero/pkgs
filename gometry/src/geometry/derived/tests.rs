use std::cmp::Ordering;

use crate::geometry::derived::concave::point_xy_cmp;
use crate::geometry::derived::native_concave_hull;
use crate::geometry::{
    CoordSeq, Coordinates as _, LineSeq, MOrdinate, MultiPolygon, Point, Polygon, Ring, Shape,
    ZOrdinate, empty_geometry,
};

fn hull_vertices(points: &[Point]) -> Vec<Point> {
    super::monotone_chain_hull(points)
        .into_iter()
        .map(|index| points[index])
        .collect()
}

fn minimum_area_rectangle_area(hull: &[Point]) -> f64 {
    let ring = super::minimum_area_rectangle(hull).expect("finite rectangle");
    let mut points = ring.iter_coords();
    points.next_back().expect("closed rectangle");
    crate::geometry::open_point_cycle_magnitude(&points.collect::<Vec<_>>()).get()
}

#[test]
fn minimum_area_rectangle_axis_aligned_square() {
    let square = vec![
        point(0.0, 0.0),
        point(4.0, 0.0),
        point(4.0, 4.0),
        point(0.0, 4.0),
    ];
    let hull = hull_vertices(&square);
    assert!((minimum_area_rectangle_area(&hull) - 16.0).abs() <= 1.0e-12);
}

#[test]
fn minimum_area_rectangle_equilateral_triangle() {
    let side = 4.0;
    let height = side * 3.0_f64.sqrt() / 2.0;
    let points = [point(0.0, 0.0), point(side, 0.0), point(side / 2.0, height)];
    let hull = hull_vertices(&points);
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
    let multipoint = Shape::multi_point(
        crate::geometry::CoordinateAxes::XY,
        CoordSeq::try_from_points(&points).unwrap(),
    );
    let rectangle = multipoint
        .minimum_rotated_rectangle()
        .expect("minimum rotated rectangle");
    let hull = hull_vertices(&points);
    let expected_area = minimum_area_rectangle_area(&hull);
    assert!((rectangle.area() - expected_area).abs() <= 1.0e-12);
    assert!(expected_area > 0.0);
}

#[test]
fn minimum_rotated_rectangle_preserves_point_identity_and_ordinates() {
    let point = Point::new_axes(
        -42.25,
        11.125,
        ZOrdinate(Some(99.5)),
        MOrdinate(Some(-88.75)),
    )
    .unwrap();
    let source = Shape::Point(point);

    assert_eq!(
        source
            .minimum_rotated_rectangle()
            .expect("point rectangle identity"),
        source
    );
}

fn point(x: f64, y: f64) -> Point {
    Point::new_raw_xy(x, y)
}

fn point_z(x: f64, y: f64, z: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(None)).unwrap()
}

fn centroid_test_polygon(points: &[(f64, f64)]) -> Polygon {
    let points = points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>();
    let coordinates = CoordSeq::try_from_points(&points).expect("polygon coordinates");
    Polygon::new(
        Ring::try_from_closed(coordinates).expect("closed polygon ring"),
        Vec::new(),
    )
}

#[test]
fn polygon_centroid_returns_extrema_from_its_areal_pass() {
    let first =
        centroid_test_polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
    let second = centroid_test_polygon(&[
        (10.0, -3.0),
        (14.0, -3.0),
        (14.0, 1.0),
        (10.0, 1.0),
        (10.0, -3.0),
    ]);
    let shape = Shape::MultiPolygon(MultiPolygon::from_dense(vec![first, second]).unwrap());

    let (Shape::Point(centroid), bounds) = shape.centroid_with_bounds().expect("polygon centroid")
    else {
        panic!("the areal centroid is a point");
    };
    approx::assert_relative_eq!(centroid.x, 7.0, max_relative = 64.0 * f64::EPSILON);
    approx::assert_relative_eq!(centroid.y, 0.5, max_relative = 64.0 * f64::EPSILON);
    assert_eq!(bounds, shape.bounds());
}

#[test]
fn packed_polygon_centroid_returns_extrema_from_its_areal_pass() {
    let xs = [0.0, 4.0, 4.0, 0.0, 0.0];
    let ys = [0.0, 0.0, 4.0, 4.0, 0.0];
    let polygon = Shape::Polygon(centroid_test_polygon(&[
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (0.0, 4.0),
        (0.0, 0.0),
    ]));
    let (centroid, bounds) =
        super::centroid_polygon_row_columns_with_bounds(&xs, &ys, &[0, 5], 0..1);

    let (x, y) = centroid.expect("the square has an areal centroid");
    approx::assert_relative_eq!(x, 2.0, max_relative = 64.0 * f64::EPSILON);
    approx::assert_relative_eq!(y, 2.0, max_relative = 64.0 * f64::EPSILON);
    assert_eq!(bounds, polygon.bounds());
}

#[test]
fn degenerate_polygon_centroid_keeps_its_extrema_for_the_general_dimension_fallback() {
    let shape = Shape::Polygon(centroid_test_polygon(&[
        (-8.0, 3.0),
        (4.0, 3.0),
        (0.0, 3.0),
        (-8.0, 3.0),
    ]));

    let (Shape::Point(centroid), bounds) = shape.centroid_with_bounds().expect("lineal fallback")
    else {
        panic!("the lineal centroid is a point");
    };
    approx::assert_relative_eq!(centroid.x, -2.0, max_relative = 64.0 * f64::EPSILON);
    approx::assert_relative_eq!(centroid.y, 3.0, max_relative = 64.0 * f64::EPSILON);
    assert_eq!(bounds, shape.bounds());
}

#[test]
fn areal_centroid_replays_multipolygon_when_positive_child_weight_rounds_to_zero() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let tiny = f64::from_bits(1);
    let x = exact_power_of_two(51);
    let ordinary = centroid_test_polygon(&[
        (-0.5, -0.5),
        (0.5, -0.5),
        (0.5, 0.5),
        (-0.5, 0.5),
        (-0.5, -0.5),
    ]);
    let small = centroid_test_polygon(&[
        (x, 0.0),
        (x + 0.5, 0.0),
        (x + 0.5, tiny),
        (x, tiny),
        (x, 0.0),
    ]);
    let shape =
        Shape::MultiPolygon(MultiPolygon::from_dense(vec![ordinary, small]).expect("two polygons"));

    let (centroid_x, centroid_y) = super::areal_centroid(&shape)
        .expect("exact areal summary")
        .expect("positive exact aggregate area");
    let expected_x = f64::from_bits(1_u64 << 50);
    assert_eq!(centroid_x.to_bits(), expected_x.to_bits());
    assert_eq!(centroid_y.to_bits(), 0.0_f64.to_bits());
}

#[test]
fn areal_centroid_propagates_rounded_zero_through_nested_collections() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let side = exact_power_of_two(-540);
    let square = centroid_test_polygon(&[
        (0.0, 0.0),
        (side, 0.0),
        (side, side),
        (0.0, side),
        (0.0, 0.0),
    ]);
    let slender = centroid_test_polygon(&[
        (3.0 * side, 0.0),
        (5.0 * side, 0.0),
        (5.0 * side, side / 2.0),
        (3.0 * side, side / 2.0),
        (3.0 * side, 0.0),
    ]);
    let inner = Shape::geometry_collection(vec![Shape::Polygon(square), Shape::Polygon(slender)])
        .expect("inner collection");
    let nested = Shape::geometry_collection(vec![inner]).expect("outer collection");

    let (centroid_x, centroid_y) = super::areal_centroid(&nested)
        .expect("exact areal summary")
        .expect("positive exact nested area");
    assert_eq!(centroid_x.to_bits(), (2.25 * side).to_bits());
    assert_eq!(centroid_y.to_bits(), (0.375 * side).to_bits());
}

#[test]
fn exact_zero_area_collection_still_uses_its_lineal_centroid() {
    let degenerate = centroid_test_polygon(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (0.0, 0.0)]);
    let collection =
        Shape::geometry_collection(vec![Shape::Polygon(degenerate)]).expect("collection");

    assert!(
        super::areal_centroid(&collection)
            .expect("exact area summary")
            .is_none()
    );
    let (x, y) = super::lineal_centroid(&collection)
        .expect("lineal summary")
        .expect("the collinear ring has positive length");
    approx::assert_relative_eq!(x, 1.0, max_relative = 64.0 * f64::EPSILON);
    assert_eq!(y, 0.0);
}

fn origin_on(hull: &Shape) -> Point {
    hull.points_vec()
        .expect("hull point count fits")
        .into_iter()
        .find(|point| point.x == 0.0 && point.y == 0.0)
        .expect("hull contains the origin")
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
    Shape::multi_point(
        crate::geometry::CoordinateAxes::XY,
        CoordSeq::try_from_points(points).unwrap(),
    )
}

#[test]
fn convex_hull_dedups_signed_zero_duplicates() {
    let points = vec![point(0.0, 0.0), point(-0.0, 0.0), point(0.0, -0.0)];
    let hull = multipoint(&points).convex_hull().expect("convex hull");
    assert!(hull.equals(&Shape::Point(point(0.0, 0.0))).unwrap());
    assert!(!matches!(hull, Shape::LineString(_)));
}

#[test]
fn convex_hull_copies_first_seen_vertex_at_duplicate_xy() {
    let points = vec![
        point_z(0.0, 0.0, 10.0),
        point_z(1.0, 0.0, 20.0),
        point_z(0.0, 1.0, 30.0),
        point_z(0.0, 0.0, 99.0),
    ];
    let hull = multipoint(&points).convex_hull().expect("convex hull");
    assert!(hull.has_z());
    let origin = origin_on(&hull);
    assert_eq!(origin.z(), Some(10.0));
    assert_eq!(origin.x.to_bits(), 0.0_f64.to_bits());
}

#[test]
fn axis_column_hull_keeps_first_extrema_ordinates() {
    let first = Point::new_axes(-0.0, -0.0, ZOrdinate(Some(10.0)), MOrdinate(Some(20.0))).unwrap();
    for vertical in [false, true] {
        let mut points = vec![first];
        for ordinal in 1..=300 {
            let (x, y) = if vertical {
                (-0.0, f64::from(ordinal))
            } else {
                (f64::from(ordinal), -0.0)
            };
            points.push(
                Point::new_axes(
                    x,
                    y,
                    ZOrdinate(Some(1000.0 + f64::from(ordinal))),
                    MOrdinate(Some(2000.0 + f64::from(ordinal))),
                )
                .unwrap(),
            );
        }
        let last = if vertical {
            Point::new_axes(
                -0.0,
                300.0,
                ZOrdinate(Some(1300.0)),
                MOrdinate(Some(2300.0)),
            )
        } else {
            Point::new_axes(
                300.0,
                -0.0,
                ZOrdinate(Some(1300.0)),
                MOrdinate(Some(2300.0)),
            )
        }
        .unwrap();
        points
            .push(Point::new_axes(0.0, 0.0, ZOrdinate(Some(99.0)), MOrdinate(Some(99.0))).unwrap());
        points.push(
            Point::new_axes(last.x, last.y, ZOrdinate(Some(99.0)), MOrdinate(Some(99.0))).unwrap(),
        );
        for source in [
            multipoint(&points),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            ),
        ] {
            assert_eq!(source.convex_hull().unwrap().points_vec().unwrap(), vec![
                first, last
            ]);
        }
    }
}

#[test]
fn geometric_xy_cmp_equates_signed_zeros() {
    assert_eq!(
        super::geometric_xy_cmp(point(-0.0, 1.0), point(0.0, 1.0)),
        Ordering::Equal
    );
    assert!(super::geometric_xy_cmp(point(-0.0, 0.0), point(0.0, 1.0)).is_lt());
}

fn mixed_signed_zero_rectangle() -> Vec<Point> {
    vec![
        point(-0.0, 1.0),
        point(0.0, 0.0),
        point(1.0, 0.0),
        point(1.0, 2.0),
        point(-0.0, 2.0),
    ]
}

#[expect(
    clippy::float_cmp,
    reason = "hull vertices are identified by stored coordinates, including signed-zero spelling"
)]
fn assert_open_hull_is_mixed_zero_rectangle(hull: &Shape) {
    let verts = hull.points_vec().expect("hull point count fits");
    let (_, open) = verts.split_last().expect("closed polygon hull");
    assert_eq!(open.len(), 4);
    assert!(
        open.iter().all(|point| !(point.x == 0.0 && point.y == 1.0)),
        "collinear midpoint must not survive the strict hull"
    );
    let origin = open
        .iter()
        .find(|point| point.x == 0.0 && point.y == 0.0)
        .expect("bottom-left");
    assert_eq!(origin.x.to_bits(), 0.0_f64.to_bits());
    let top_left = open
        .iter()
        .find(|point| point.x == 0.0 && point.y == 2.0)
        .expect("top-left");
    assert_eq!(top_left.x.to_bits(), (-0.0_f64).to_bits());
}

#[test]
fn convex_hull_mixed_signed_zero_rectangle_drops_collinear_midpoint() {
    let hull = multipoint(&mixed_signed_zero_rectangle())
        .convex_hull()
        .expect("convex hull");
    assert_open_hull_is_mixed_zero_rectangle(&hull);
}

fn open_hull_contains(hull: &Shape, expected: Point) -> bool {
    let verts = hull.points_vec().expect("hull point count fits");
    let Some((_, open)) = verts.split_last() else {
        return false;
    };
    open.iter().any(|point| {
        point.x.to_bits() == expected.x.to_bits()
            && point.y.to_bits() == expected.y.to_bits()
            && point.z() == expected.z()
            && point.m() == expected.m()
    })
}

#[test]
fn concave_hull_uniform_zero_rectangle_keeps_collinear_edge_site() {
    let midpoint = point_z(0.0, 1.0, 50.0);
    let points = vec![
        point_z(0.0, 0.0, 10.0),
        point_z(1.0, 0.0, 20.0),
        point_z(1.0, 2.0, 30.0),
        point_z(0.0, 2.0, 40.0),
        midpoint,
    ];
    let hull = multipoint(&points)
        .concave_hull(10.0, 0.0)
        .expect("concave hull");
    assert!(hull.has_z());
    assert!(open_hull_contains(&hull, midpoint));
}

#[test]
fn concave_hull_mixed_signed_zero_rectangle_keeps_collinear_edge_site() {
    let midpoint = Point::new_axes(-0.0, 1.0, ZOrdinate(Some(50.0)), MOrdinate(Some(5.0))).unwrap();
    let points = vec![
        midpoint,
        Point::new_axes(0.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(Some(1.0))).unwrap(),
        Point::new_axes(1.0, 0.0, ZOrdinate(Some(20.0)), MOrdinate(Some(2.0))).unwrap(),
        Point::new_axes(1.0, 2.0, ZOrdinate(Some(30.0)), MOrdinate(Some(3.0))).unwrap(),
        Point::new_axes(-0.0, 2.0, ZOrdinate(Some(40.0)), MOrdinate(Some(4.0))).unwrap(),
    ];
    let hull = multipoint(&points)
        .concave_hull(10.0, 0.0)
        .expect("concave hull");
    assert!(hull.has_z());
    assert!(hull.has_m());
    assert!(open_hull_contains(&hull, midpoint));
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "hull vertices are identified by stored coordinates, including signed-zero spelling"
)]
fn convex_hull_transposed_signed_zero_collinear_drops_midpoint() {
    let points = vec![
        point(0.0, -0.0),
        point(0.0, 1.0),
        point(2.0, 1.0),
        point(2.0, 0.0),
        point(1.0, -0.0),
    ];
    let hull = multipoint(&points).convex_hull().expect("convex hull");
    let verts = hull.points_vec().expect("hull point count fits");
    let open = &verts[..verts.len() - 1];
    assert_eq!(open.len(), 4);
    assert!(open.iter().all(|point| !(point.x == 1.0 && point.y == 0.0)));
}

#[test]
fn convex_hull_mixed_signed_zero_keeps_first_seen_zm() {
    let points = vec![
        Point::new_axes(-0.0, 1.0, ZOrdinate(Some(50.0)), MOrdinate(Some(5.0))).unwrap(),
        Point::new_axes(0.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(Some(1.0))).unwrap(),
        Point::new_axes(1.0, 0.0, ZOrdinate(Some(20.0)), MOrdinate(Some(2.0))).unwrap(),
        Point::new_axes(1.0, 2.0, ZOrdinate(Some(30.0)), MOrdinate(Some(3.0))).unwrap(),
        Point::new_axes(-0.0, 2.0, ZOrdinate(Some(40.0)), MOrdinate(Some(4.0))).unwrap(),
    ];
    let hull = multipoint(&points).convex_hull().expect("convex hull");
    assert!(hull.has_z());
    assert!(hull.has_m());
    let origin = origin_on(&hull);
    assert_eq!(origin.z(), Some(10.0));
    assert_eq!(origin.m(), Some(1.0));
    assert_eq!(origin.x.to_bits(), 0.0_f64.to_bits());
}

#[test]
fn minimum_rotated_rectangle_mixed_signed_zero_matches_rectangle_area() {
    let rectangle = multipoint(&mixed_signed_zero_rectangle())
        .minimum_rotated_rectangle()
        .expect("minimum rotated rectangle");
    assert!((rectangle.area() - 2.0).abs() <= 1.0e-12);
}

#[test]
fn convex_hull_keeps_first_seen_signed_zero_spelling() {
    let points = vec![
        point_z(0.0, 0.0, 10.0),
        point_z(-0.0, 0.0, 20.0),
        point_z(1.0, 0.0, 30.0),
        point_z(0.0, 1.0, 40.0),
    ];
    let hull = multipoint(&points).convex_hull().expect("convex hull");
    let origin = origin_on(&hull);
    assert_eq!(origin.z(), Some(10.0));
    assert_eq!(origin.x.to_bits(), 0.0_f64.to_bits());
}

#[test]
fn concave_hull_copies_first_seen_vertex_at_duplicate_xy() {
    let points = vec![
        point_z(0.0, 0.0, 10.0),
        point_z(4.0, 0.0, 20.0),
        point_z(4.0, 4.0, 30.0),
        point_z(0.0, 4.0, 40.0),
        point_z(2.0, 1.0, 50.0),
        point_z(0.0, 0.0, 99.0),
    ];
    let hull = multipoint(&points)
        .concave_hull(1.0, 0.0)
        .expect("concave hull");
    assert!(hull.has_z());
    let origin = origin_on(&hull);
    assert_eq!(origin.z(), Some(10.0));
}

fn assert_covers_points(hull: &Shape, points: &[Point]) {
    for &point in points {
        assert!(hull.covers(&Shape::Point(point)).unwrap());
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
        super::shape_from_open_hull(&hull, empty_geometry).unwrap()
    };
    let native = shape_from_indices(native_concave_hull(&points, 1.0, 0.0).unwrap());
    let native_baseline = shape_from_indices(native_concave_hull(&points, 0.75, 0.0).unwrap());
    let native_threshold = shape_from_indices(native_concave_hull(&points, 0.75, 5.0).unwrap());

    assert_covers_points(&native, &points);
    assert!(native.area() <= convex);
    assert!(native_threshold.area() >= native_baseline.area());
}

#[test]
fn concave_hull_is_invariant_to_input_point_order() {
    let points = [
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
    for order in [
        [5, 4, 3, 2, 1, 0],
        [2, 3, 4, 5, 0, 1],
        [4, 0, 5, 1, 3, 2],
        [1, 5, 2, 4, 0, 3],
    ] {
        let permuted = order.map(|index| points[index]);
        let hull = multipoint(&permuted)
            .concave_hull(1.0, 0.0)
            .expect("concave hull");
        assert!(hull.equals_exact(&expected, 0.0, false, false));
    }
}

#[test]
fn lineal_centroid_keeps_world_metric_weights_in_an_axis_frame() {
    let line = |points: &[(f64, f64)]| {
        LineSeq::try_new(
            CoordSeq::try_from_points(
                &points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .expect("two-point test line")
    };
    let unequal = Shape::MultiLineString(
        vec![
            line(&[(-1e308, 0.0), (1e308, 0.0)]),
            line(&[(-9e307, 10.0), (9e307, 10.0)]),
        ]
        .into(),
    );
    let (_, y) = super::lineal_centroid(&unequal)
        .expect("lineal summary")
        .expect("lineal centroid");
    assert!((y - 4.736_842_105_263_158).abs() <= 1e-14);

    let opposite = Shape::MultiLineString(
        vec![
            line(&[(1e308, -1e308), (1e308, 1e308)]),
            line(&[(-1e308, -1e308), (-1e308, 1e308)]),
        ]
        .into(),
    );
    let (x, y) = super::lineal_centroid(&opposite)
        .expect("lineal summary")
        .expect("lineal centroid");
    assert_eq!(x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(y.to_bits(), 0.0_f64.to_bits());
}
