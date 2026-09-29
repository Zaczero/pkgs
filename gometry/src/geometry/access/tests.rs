use crate::geometry::access::*;
use crate::geometry::{
    CoordSeq, CoordWindow, CoordinateAxes, GeometryKind, LineSeq, MColumn, MOrdinate,
    MultiLineString, MultiMember, MultiPoint, MultiPolygon, Point, Polygon, Ring, Shape,
    ShapeCountQuery, SharedSlice, ZOrdinate,
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

fn repeated_point_tree(point: Point, depth: usize) -> Shape {
    let mut shape = Shape::Point(point);
    for _ in 0..depth {
        shape = Shape::geometry_collection(vec![shape.clone(), shape])
            .expect("repeated DAG stays within the accepted collection depth");
    }
    shape
}

#[test]
fn owner_aware_point_sets_and_summaries_finish_at_depth_128() {
    let source = point(0.0, -0.0, 7.0, 29.0);
    let shape = repeated_point_tree(source, 128);

    let unique = shape.unique_points().expect("one full-identity point");
    assert_eq!(ShapeCountQuery::default().coordinates(&unique).unwrap(), 1);
    assert_eq!(unique.axes(), CoordinateAxes::XYZM);
    let unique_points = unique.points_vec().unwrap();
    let [unique] = unique_points.as_slice() else {
        panic!("the point set contains one Point");
    };
    assert_eq!(unique.z().unwrap(), 7.0);
    assert_eq!(unique.m().unwrap(), 29.0);
    assert_eq!(unique.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(unique.y.to_bits(), (-0.0_f64).to_bits());

    let bounds = shape.bounds().expect("nonempty point bounds");
    assert_eq!(bounds.minx().to_bits(), 0.0_f64.to_bits());
    assert_eq!(bounds.maxx().to_bits(), 0.0_f64.to_bits());
    assert_eq!(bounds.miny().to_bits(), (-0.0_f64).to_bits());
    assert_eq!(bounds.maxy().to_bits(), (-0.0_f64).to_bits());
    assert_eq!(shape.z_extremes(), Some((7.0, 7.0)));
    assert_eq!(shape.m_extremes(), Some((29.0, 29.0)));
    assert_eq!(shape.axes(), CoordinateAxes::XYZM);
    assert!(shape.axes_all_z());
    assert!(!shape.is_empty());
    assert_eq!(
        shape.topological_dimension(),
        crate::geometry::Dimension::Point
    );
}

#[test]
fn empty_axes_fold_over_a_depth_128_shared_dag() {
    let mut shape = Shape::typed_empty(GeometryKind::Point, CoordinateAxes::XYZ);
    for _ in 0..128 {
        shape = Shape::geometry_collection(vec![shape.clone(), shape])
            .expect("repeated empty DAG stays within the collection depth");
    }

    let unique = shape.unique_points().unwrap();

    assert!(unique.is_empty());
    assert_eq!(unique.axes(), CoordinateAxes::XYZ);
    assert_eq!(shape.axes(), CoordinateAxes::XYZ);
}

#[test]
fn owner_aware_sets_leave_occurrence_point_walks_unchanged() {
    let shape = repeated_point_tree(point(2.0, 3.0, 7.0, 29.0), 10);
    let mut occurrences = 0;
    shape.for_each_point(|_| occurrences += 1);
    assert_eq!(occurrences, 1024);
    let mut counts = ShapeCountQuery::default();
    assert_eq!(counts.coordinates(&shape).unwrap(), 1024);
    assert_eq!(
        counts.coordinates(&shape.unique_points().unwrap()).unwrap(),
        1
    );
}

#[test]
fn shared_segment_occurrences_remain_visible_to_walkers_and_counts() {
    let mut shape = Shape::LineString(line(&[
        point(0.0, 0.0, 1.0, 1.0),
        point(1.0, 0.0, 1.0, 1.0),
    ]));
    for _ in 0..10 {
        shape = Shape::geometry_collection(vec![shape.clone(), shape])
            .expect("repeated line DAG stays within the accepted collection depth");
    }

    let mut segments = 0;
    shape.for_each_segment(|_| segments += 1);
    let mut counts = ShapeCountQuery::default();

    assert_eq!(segments, 1_usize << 10);
    assert_eq!(counts.segments(&shape).unwrap(), 1_usize << 10);
}

#[test]
fn distinct_point_sets_handle_a_depth_31_shared_dag() {
    let expected = point(1.0, 2.0, 3.0, 4.0);
    let shape = repeated_point_tree(expected, 31);
    let mut counts = ShapeCountQuery::default();

    assert_eq!(counts.coordinates(&shape).unwrap(), 1_usize << 31);

    let xy = shape.try_unique_xy_points().unwrap();
    let vertices = shape.first_seen_xy_vertices().unwrap();
    let full_points = shape.distinct_point_values();
    let unique = shape.unique_points().unwrap();

    assert_eq!(xy, vec![Point::new_raw_xy(1.0, 2.0)]);
    assert_eq!(vertices, vec![expected]);
    assert_eq!(full_points, vec![expected]);
    assert_eq!(unique.points_vec().unwrap(), vec![expected]);
}

#[test]
fn shared_coordinate_runs_keep_distinct_point_set_results() {
    let source = Shape::LineString(line(&[
        point(0.0, 0.0, 3.0, 4.0),
        point(1.0, 0.0, 5.0, 6.0),
    ]));
    let shape = Shape::geometry_collection(vec![source; 64]).unwrap();

    let mut counts = ShapeCountQuery::default();
    assert_eq!(counts.coordinates(&shape).unwrap(), 128);

    let xy = shape.try_unique_xy_points().unwrap();
    let full_points = shape.distinct_point_values();
    let expected = vec![point(0.0, 0.0, 3.0, 4.0), point(1.0, 0.0, 5.0, 6.0)];
    assert_eq!(xy, vec![
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0)
    ]);
    assert_eq!(full_points, expected);
}

#[test]
fn xy_and_full_point_sets_keep_their_ordinate_identities() {
    let xs = SharedSlice::from_vec(vec![0.0, 1.0]);
    let ys = SharedSlice::from_vec(vec![10.0, 11.0]);
    let ms = SharedSlice::from_vec((0_u32..128).map(f64::from).collect());
    let members = (0..64)
        .map(|line| {
            let coords = CoordSeq::from_shared_columns_with_window(
                xs.clone(),
                ys.clone(),
                None,
                Some(MColumn::Local {
                    owner: ms.clone(),
                    offset: line * 2,
                }),
                CoordWindow::from_range(0..2, xs.len()),
            )
            .unwrap();
            Shape::LineString(LineSeq::from_proven(coords))
        })
        .collect();
    let shape = Shape::geometry_collection(members).unwrap();

    let xy = shape.try_unique_xy_points().unwrap();
    let full = shape.distinct_point_values();
    let expected_xy = vec![Point::new_raw_xy(0.0, 10.0), Point::new_raw_xy(1.0, 11.0)];
    let xy_representatives = [
        Point::new_axes(0.0, 10.0, ZOrdinate(None), MOrdinate(None)).unwrap(),
        Point::new_axes(1.0, 11.0, ZOrdinate(None), MOrdinate(None)).unwrap(),
    ];
    let expected_full = xy_representatives
        .into_iter()
        .cycle()
        .zip(0_u32..128)
        .map(|(point, measure)| {
            Point::new_axes(
                point.x,
                point.y,
                ZOrdinate(None),
                MOrdinate(Some(f64::from(measure))),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(xy, expected_xy);
    assert_eq!(full, expected_full);
}

#[test]
fn summary_memos_replay_nonadjacent_signed_zero_bounds_and_extrema() {
    let positive = Shape::geometry_collection(vec![Shape::Point(point(0.0, 0.0, 0.0, 0.0))])
        .expect("positive-zero member");
    let negative = Shape::Point(point(-0.0, 0.0, -0.0, -0.0));
    let shape = Shape::geometry_collection(vec![positive.clone(), negative, positive])
        .expect("interleaved shared member");

    let bounds = shape.bounds().expect("nonempty bounds");
    for value in [bounds.minx(), bounds.maxx(), bounds.miny(), bounds.maxy()] {
        assert_eq!(value.to_bits(), 0.0_f64.to_bits());
    }
    let (min_z, max_z) = shape.z_extremes().expect("Z present");
    assert_eq!((min_z.to_bits(), max_z.to_bits()), (0, 0));
    let (min_m, max_m) = shape.m_extremes().expect("M present");
    assert_eq!((min_m.to_bits(), max_m.to_bits()), (0, 0));

    let extremes = shape.extremes().expect("nonempty extremes");
    assert_eq!(extremes[0].x.to_bits(), 0.0_f64.to_bits());
}

#[test]
fn shared_run_summaries_replay_nonadjacent_signed_zero_occurrences() {
    let line = Shape::LineString(line(&[
        point(0.0, 0.0, 0.0, 0.0),
        point(1.0, 0.0, 0.0, 0.0),
    ]));
    let middle = Shape::Point(point(0.0, -0.0, -0.0, -0.0));
    let shape = Shape::geometry_collection(vec![line.clone(), middle, line]).unwrap();

    let bounds = shape.bounds().unwrap();

    assert_eq!(bounds.miny().to_bits(), 0.0_f64.to_bits());
    assert_eq!(bounds.maxy().to_bits(), 0.0_f64.to_bits());
    assert_eq!(shape.z_extremes().unwrap().0.to_bits(), 0.0_f64.to_bits());
    assert_eq!(shape.m_extremes().unwrap().0.to_bits(), 0.0_f64.to_bits());
}

#[test]
fn bounds_summary_replays_nan_payload_and_infinity_occurrences() {
    let nan_a = f64::from_bits(0x7FF8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7FF8_0000_0000_0002);
    let first =
        Shape::geometry_collection(vec![Shape::Point(Point::new_raw_xy(nan_a, f64::INFINITY))])
            .unwrap();
    let shape = Shape::geometry_collection(vec![
        first.clone(),
        Shape::Point(Point::new_raw_xy(nan_b, f64::NEG_INFINITY)),
        first,
    ])
    .unwrap();

    let bounds = shape.bounds().expect("three point occurrences");
    assert_eq!(bounds.minx().to_bits(), nan_a.to_bits());
    assert_eq!(bounds.maxx().to_bits(), nan_a.to_bits());
    assert_eq!(bounds.miny(), f64::NEG_INFINITY);
    assert_eq!(bounds.maxy(), f64::INFINITY);
}

#[test]
fn shared_collection_extrema_preserve_nan_payload_and_infinity_folds() {
    let nan_a = f64::from_bits(0x7FF8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7FF8_0000_0000_0002);
    let raw_point = |z, m| {
        Shape::Point(Point::new_raw_axes(
            1.0,
            2.0,
            ZOrdinate(Some(z)),
            MOrdinate(Some(m)),
        ))
    };
    let shared = Shape::geometry_collection(vec![raw_point(nan_a, f64::INFINITY)]).unwrap();
    let with_shared_owner = Shape::geometry_collection(vec![
        shared.clone(),
        raw_point(nan_b, f64::NEG_INFINITY),
        shared,
    ])
    .unwrap();
    let without_shared_owner = Shape::geometry_collection(vec![
        Shape::geometry_collection(vec![raw_point(nan_a, f64::INFINITY)]).unwrap(),
        raw_point(nan_b, f64::NEG_INFINITY),
        Shape::geometry_collection(vec![raw_point(nan_a, f64::INFINITY)]).unwrap(),
    ])
    .unwrap();

    let actual_z = with_shared_owner.z_extremes().unwrap();
    let expected_z = without_shared_owner.z_extremes().unwrap();
    assert!(actual_z.0.is_nan() && actual_z.1.is_nan());
    assert_eq!(actual_z.0.to_bits(), expected_z.0.to_bits());
    assert_eq!(actual_z.1.to_bits(), expected_z.1.to_bits());

    assert_eq!(
        with_shared_owner.m_extremes(),
        Some((f64::NEG_INFINITY, f64::INFINITY))
    );
    assert_eq!(
        with_shared_owner.m_extremes(),
        without_shared_owner.m_extremes()
    );
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
fn sparse_multiline_parts_keep_empty_order_for_indexed_and_double_ended_access() {
    let first = line(&[point(-0.0, 1.0, 11.0, 101.0), point(2.0, 2.0, 12.0, 102.0)]);
    let last = line(&[point(5.0, 5.0, 15.0, 105.0), point(6.0, 6.0, 16.0, 106.0)]);
    let lines = MultiLineString::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Empty,
        MultiMember::Value(first.clone()),
        MultiMember::Empty,
        MultiMember::Value(last.clone()),
    ])
    .expect("homogeneous multipart");
    let expected_first = Shape::LineString(first);
    let expected = [
        Shape::Empty(GeometryKind::LineString, CoordinateAxes::XYZM),
        expected_first.clone(),
        Shape::Empty(GeometryKind::LineString, CoordinateAxes::XYZM),
        Shape::LineString(last),
    ];
    let shape = Shape::MultiLineString(lines);

    for (index, part) in expected.iter().enumerate() {
        assert_eq!(shape.part_at(index), Some(part.clone()));
    }
    assert_eq!(shape.part_at(expected.len()), None);
    let Shape::LineString(first_part) = shape.part_at(1).expect("present line member") else {
        panic!("member 1 is a line");
    };
    let first_point = first_part.point_at(0);
    assert_eq!(first_point.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(first_point.z(), Some(11.0));
    assert_eq!(first_point.m(), Some(101.0));
    let (part, allocations) = crate::test_support::allocation_count(|| shape.part_at(1));
    assert_eq!(part, Some(expected_first));
    assert_eq!(allocations, 0);

    let mut parts = shape.parts();
    let mut expected_parts = expected.into_iter();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts.next(), expected_parts.next());
    assert_eq!(parts.len(), 3);
    assert_eq!(parts.next_back(), expected_parts.next_back());
    assert_eq!(parts.len(), 2);
    assert_eq!(parts.next(), expected_parts.next());
    assert_eq!(parts.next_back(), expected_parts.next_back());
    assert_eq!(parts.len(), 0);
    assert_eq!(parts.next(), None);
}

#[test]
fn sparse_multipolygon_parts_keep_empty_order_for_indexed_and_double_ended_access() {
    let first = Polygon::new(
        ring(&[
            point(0.0, 0.0, 10.0, 100.0),
            point(1.0, 0.0, 11.0, 101.0),
            point(1.0, 1.0, 12.0, 102.0),
            point(0.0, 0.0, 10.0, 100.0),
        ]),
        vec![],
    );
    let last = Polygon::new(
        ring(&[
            point(5.0, 5.0, 15.0, 105.0),
            point(6.0, 5.0, 16.0, 106.0),
            point(6.0, 6.0, 17.0, 107.0),
            point(5.0, 5.0, 15.0, 105.0),
        ]),
        vec![],
    );
    let polygons = MultiPolygon::try_new(CoordinateAxes::XYZM, vec![
        MultiMember::Empty,
        MultiMember::Value(first.clone()),
        MultiMember::Empty,
        MultiMember::Value(last.clone()),
    ])
    .expect("homogeneous multipart");
    let expected = [
        Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZM),
        Shape::Polygon(first),
        Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZM),
        Shape::Polygon(last),
    ];
    let shape = Shape::MultiPolygon(polygons);

    for (index, part) in expected.iter().enumerate() {
        assert_eq!(shape.part_at(index), Some(part.clone()));
    }
    assert_eq!(shape.part_at(expected.len()), None);

    let mut parts = shape.parts();
    let mut expected_parts = expected.into_iter();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts.next(), expected_parts.next());
    assert_eq!(parts.len(), 3);
    assert_eq!(parts.next_back(), expected_parts.next_back());
    assert_eq!(parts.len(), 2);
    assert_eq!(parts.next(), expected_parts.next());
    assert_eq!(parts.next_back(), expected_parts.next_back());
    assert_eq!(parts.len(), 0);
    assert_eq!(parts.next(), None);
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
fn column_minmax_scans_a_full_chunk_and_scalar_tail_while_skipping_nan() {
    let len = REDUCE_LANES.wrapping_add(3);
    let values: Vec<f64> = (0..len)
        .map(|index| {
            if index == 0 {
                -11.0
            } else if index == REDUCE_LANES.wrapping_sub(1) {
                13.0
            } else if index == 3 || index == REDUCE_LANES {
                f64::NAN
            } else if index == len.wrapping_sub(1) {
                17.0
            } else {
                4.0
            }
        })
        .collect();

    assert_eq!(column_minmax(&values), Some((-11.0, 17.0)));
}

#[test]
fn simd_reduce_f64_keeps_chunk_order_and_the_scalar_tail_range() {
    let chunks = REDUCE_LANES.wrapping_mul(2);
    let len = chunks.wrapping_add(3);
    let mut chunk_starts = Vec::new();
    let mut tail_ranges = Vec::new();

    let result = simd_reduce_f64(
        len,
        0_usize,
        0_usize,
        |chunk_count, scalar_count, start| {
            chunk_starts.push(start);
            (chunk_count.wrapping_add(1), scalar_count)
        },
        |scalar_count, range| {
            tail_ranges.push(range.clone());
            scalar_count.wrapping_add(range.len())
        },
        |chunk_count, scalar_count| (chunk_count, scalar_count),
    );

    assert_eq!(chunk_starts, [0, REDUCE_LANES]);
    assert_eq!(tail_ranges.len(), 1);
    let tail = tail_ranges.first().expect("one scalar tail");
    assert_eq!(tail.start, chunks);
    assert_eq!(tail.end, len);
    assert_eq!(result, (2, 3));
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
