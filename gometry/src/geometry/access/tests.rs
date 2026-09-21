use crate::geometry::access::*;
use crate::geometry::{
    CoordSeq, CoordinateAxes, GeometryKind, LineSeq, MOrdinate, MultiLineString, MultiMember,
    MultiPoint, MultiPolygon, Point, Polygon, Ring, Shape, ZOrdinate,
};

fn point(x: f64, y: f64, z: f64, m: f64) -> Point {
    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).expect("finite point")
}

fn line(points: &[Point]) -> LineSeq {
    LineSeq::try_new(CoordSeq::try_from_points(points).unwrap()).expect("line has enough vertices")
}

fn ring(points: &[Point]) -> Ring {
    Ring::closed(points.to_vec()).expect("ring is valid")
}

#[test]
fn point_walkers_share_nested_preorder_and_short_circuit() {
    let points = [
        point(1.0, 1.0, 101.0, 1001.0),
        point(2.0, 2.0, 102.0, 1002.0),
        point(3.0, 3.0, 103.0, 1003.0),
        point(4.0, 4.0, 104.0, 1004.0),
        point(5.0, 5.0, 105.0, 1005.0),
        point(6.0, 6.0, 106.0, 1006.0),
    ];
    let multipoint = MultiPoint::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Empty,
        MultiMember::Value(points[1]),
        MultiMember::Empty,
        MultiMember::Value(points[2]),
    ])
    .expect("homogeneous multipoint");
    let polygon = Polygon::new(ring(&[points[3], points[4], points[5], points[3]]), vec![]);
    let shape = Shape::geometry_collection(vec![
        Shape::typed_empty(GeometryKind::Point, CoordinateAxes::XYZM),
        Shape::Point(points[0]),
        Shape::geometry_collection(vec![
            Shape::MultiPoint(multipoint),
            Shape::LineString(line(&[points[3], points[4]])),
            Shape::Polygon(polygon),
        ])
        .expect("nested collection"),
        Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XYZM),
    ])
    .expect("collection");

    let mut visited = Vec::new();
    shape.for_each_point(|point| visited.push(point));
    assert_eq!(visited, [
        points[0], points[1], points[2], points[3], points[4], points[3], points[4], points[5],
        points[3]
    ]);

    let mut any_visited = Vec::new();
    assert!(shape.any_point(|point| {
        any_visited.push(point);
        point == points[2]
    }));
    assert_eq!(any_visited, [points[0], points[1], points[2]]);

    let mut try_visited = Vec::new();
    let result: Result<(), Point> = shape.try_for_each_point(|point| {
        try_visited.push(point);
        (point != points[2]).then_some(()).ok_or(point)
    });
    assert_eq!(result, Err(points[2]));
    assert_eq!(try_visited, [points[0], points[1], points[2]]);
}

#[test]
fn segment_walkers_share_chain_order_without_crossing_and_keep_ordinates() {
    let line_points = [
        point(10.0, 10.0, 110.0, 1010.0),
        point(11.0, 11.0, 111.0, 1011.0),
        point(12.0, 12.0, 112.0, 1012.0),
    ];
    let shell_points = [
        point(20.0, 20.0, 120.0, 1020.0),
        point(21.0, 20.0, 121.0, 1021.0),
        point(21.0, 21.0, 122.0, 1022.0),
        point(20.0, 20.0, 120.0, 1020.0),
    ];
    let hole_points = [
        point(20.25, 20.25, 125.0, 1025.0),
        point(20.75, 20.25, 126.0, 1026.0),
        point(20.75, 20.75, 127.0, 1027.0),
        point(20.25, 20.25, 125.0, 1025.0),
    ];
    let polygon = Polygon::new(ring(&shell_points), vec![ring(&hole_points)]);
    let lines = MultiLineString::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Empty,
        MultiMember::Value(line(&line_points)),
    ])
    .expect("homogeneous multiline");
    let polygons = MultiPolygon::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Empty,
        MultiMember::Value(polygon),
    ])
    .expect("homogeneous multipolygon");
    let shape = Shape::geometry_collection(vec![
        Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XYZM),
        Shape::MultiLineString(lines),
        Shape::geometry_collection(vec![Shape::MultiPolygon(polygons)]).expect("nested collection"),
    ])
    .expect("collection");

    let expected_pairs = vec![
        (line_points[0], line_points[1]),
        (line_points[1], line_points[2]),
        (shell_points[0], shell_points[1]),
        (shell_points[1], shell_points[2]),
        (shell_points[2], shell_points[3]),
        (hole_points[0], hole_points[1]),
        (hole_points[1], hole_points[2]),
        (hole_points[2], hole_points[3]),
    ];
    let mut pairs = Vec::new();
    shape.for_each_vertex_pair(|start, end| pairs.push((start, end)));
    assert_eq!(pairs, expected_pairs);

    let mut segments = Vec::new();
    shape.for_each_segment(|segment| {
        segments.push((
            (segment.start.x, segment.start.y),
            (segment.end.x, segment.end.y),
        ));
    });
    assert_eq!(
        segments,
        expected_pairs
            .iter()
            .map(|(start, end)| ((start.x, start.y), (end.x, end.y)))
            .collect::<Vec<_>>()
    );
}

#[test]
fn first_chain_error_stops_nested_collection_and_multipart_siblings() {
    let first = line(&[point(0.0, 0.0, 1.0, 1.0), point(1.0, 0.0, 1.0, 1.0)]);
    let multipart_sibling = line(&[point(10.0, 0.0, 1.0, 1.0), point(11.0, 0.0, 1.0, 1.0)]);
    let collection_sibling = line(&[point(20.0, 0.0, 1.0, 1.0), point(21.0, 0.0, 1.0, 1.0)]);
    let lines = MultiLineString::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Value(first),
        MultiMember::Value(multipart_sibling),
    ])
    .expect("multipart");
    let shape = Shape::geometry_collection(vec![
        Shape::geometry_collection(vec![Shape::MultiLineString(lines)]).expect("nested"),
        Shape::LineString(collection_sibling),
    ])
    .expect("collection");

    let error = shape.try_for_each_segment_chain(|chain| {
        if chain.point_at(0).x == 0.0 {
            return Err("first-chain");
        }
        panic!("visited sibling after first-chain error");
    });
    assert_eq!(error, Err("first-chain"));
}

#[test]
fn column_all_finite_rejects_non_finite() {
    let column = vec![0.0, 1.0, f64::NAN, 3.0];
    assert!(!column_all_finite(&column));
}

#[test]
fn column_all_finite_accepts_finite() {
    let column = vec![0.0, 1.0, 2.0, 3.0];
    assert!(column_all_finite(&column));
}

#[test]
fn column_minmax_empty_is_none() {
    assert_eq!(column_minmax(&[]), None);
}

#[test]
fn column_minmax_single_element() {
    assert_eq!(column_minmax(&[5.0]), Some((5.0, 5.0)));
}

#[test]
fn column_minmax_finds_extremes() {
    assert_eq!(column_minmax(&[3.0, -1.0, 7.0, 2.0]), Some((-1.0, 7.0)));
}

#[test]
fn column_mean2_matches_scalar_mean() {
    let xs: Vec<f64> = (0..64).map(f64::from).collect();
    let ys: Vec<f64> = (0..64).map(|index| f64::from(index) * 2.0).collect();
    let count = xs.len() as f64;
    let expected = (
        xs.iter().sum::<f64>() / count,
        ys.iter().sum::<f64>() / count,
    );
    let (mx, my) = column_mean2(&xs, &ys).expect("non-empty");
    let tol = 1e-9 * expected.0.abs().max(expected.1.abs()).max(1.0);
    assert!((mx - expected.0).abs() <= tol && (my - expected.1).abs() <= tol);
    assert_eq!(column_mean2(&[], &[]), None);
}

#[test]
fn column_mean_rescues_overflowing_sum_of_finite_values() {
    // The flat `Σ` overflows f64 range (`2e308 == inf`), but the true mean is
    // finite and exactly representable; the online-mean rescue must recover it
    // instead of committing a non-finite value a `Point::new` would reject.
    let values = [1e308, 1e308];
    assert!((column_sum(&values) / values.len() as f64).is_infinite());
    assert_eq!(column_mean(&values), Some(1e308));
    // A genuinely non-finite input still propagates (its mean IS non-finite).
    assert!(
        column_mean(&[f64::INFINITY, 1.0])
            .expect("non-empty")
            .is_infinite()
    );
    assert!(column_mean(&[f64::NAN, 1.0]).expect("non-empty").is_nan());
    // Ordinary magnitudes stay bit-identical to the flat `Σ / n`.
    let ordinary = [1.0, 2.0, 3.0, 4.5];
    assert_eq!(
        column_mean(&ordinary),
        Some(column_sum(&ordinary) / ordinary.len() as f64)
    );
}

#[test]
fn line_length_columns_falls_back_for_tiny_nonzero_segments() {
    let xs: Vec<f64> = (0..=8).map(|i| f64::from(i) * 1e-200).collect();
    let ys = vec![0.0; xs.len()];
    let expected: f64 = xs
        .array_windows::<2>()
        .map(|[x0, x1]| (x1 - x0).hypot(0.0))
        .sum();
    let length = line_length_columns(&xs, &ys);

    assert_eq!(expected.to_bits(), 8e-200_f64.to_bits());
    assert_eq!(length.to_bits(), expected.to_bits());
}
