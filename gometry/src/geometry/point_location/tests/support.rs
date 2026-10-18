fn winding_class(ring: &RingPointIndex, x: f64, y: f64) -> RingClass {
    match ring.winding_off_boundary(x, y) {
        None => RingClass::Boundary,
        Some(0) => RingClass::Exterior,
        Some(_) => RingClass::Interior,
    }
}

fn closed_ring(pts: &[(f64, f64)]) -> Ring {
    let xy: Vec<XY> = pts.iter().map(|&(x, y)| XY::new(x, y)).collect();
    Ring::from_closed(CoordSeq::try_from_xy(&xy).unwrap()).unwrap()
}

fn unit_square() -> Polygon {
    Polygon {
        shell: closed_ring(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]),
        holes: SharedSlice::empty(),
    }
}

fn dense_square_points() -> Vec<(f64, f64)> {
    let mut points = Vec::with_capacity(37);
    points.extend((0..=8).map(|x| (f64::from(x), 0.0)));
    points.push((10.0, 0.0));
    points.extend((1..=8).map(|y| (10.0, f64::from(y))));
    points.push((10.0, 10.0));
    points.extend((0..=8).rev().map(|x| (f64::from(x), 10.0)));
    points.extend((1..=8).rev().map(|y| (0.0, f64::from(y))));
    points.push((0.0, 0.0));
    points
}

fn dense_square() -> Shape {
    Shape::Polygon(Polygon {
        shell: closed_ring(&dense_square_points()),
        holes: SharedSlice::empty(),
    })
}

fn exact_location_indexes(
    shape: &Shape,
) -> (
    ExactLocationIndex<SourceLocationLabels>,
    Option<ExactLocationIndex<AreaLocationLabels>>,
) {
    let inventory = ExactSourceInventory::from_shapes_with_occurrences(&[shape])
        .expect("source point-location inventory");
    let mut budget = crate::geometry::ExpansionBudget::new("test", "point-location cells");
    let partition = inventory
        .partition(&mut budget)
        .expect("source point-location partition");
    let source = ExactLocationIndex::from_cells(
        inventory
            .source_location_cells(&partition)
            .expect("full-source point-location cells"),
    );
    let support = inventory
        .metric_support(&partition, AreaLocationStrategy::PersistentSlabs)
        .expect("area point-location cells");
    let area = support
        .area_location_cells
        .map(ExactLocationIndex::from_cells);
    (source, area)
}

fn assert_exact_location_matches_source(
    wkt: &str,
    points: &[[ExactDyadic; 3]],
    directions: &[[ExactDyadic; 2]],
) {
    let shape = crate::io::parse_wkt(wkt).expect("point-location counter-case");
    let (source, area) = exact_location_indexes(&shape);
    let tester = PointBatchTester::new(&shape).expect("prepared exact location tester");
    let mut members = vec![shape.clone()];
    members.extend(
        (0..32).map(|index| Shape::Point(Point::new_raw_xy(1_000.0 + f64::from(index), 1_000.0))),
    );
    let padded = Shape::geometry_collection(members).unwrap();
    let plan = crate::geometry::PreparedPlan::transient(&padded);
    let queries = plan.prepare_point_queries(usize::MAX).unwrap();
    assert!(queries.uses_slabs());
    for point in points {
        let streaming = cartesian_exact_point_location_and_area(&shape, point)
            .expect("streaming exact point location");
        let expected = tester
            .location_and_area_query(ExactProbe::new(point).source_query().unwrap())
            .expect("prepared exact point location");
        assert_eq!(expected, streaming, "{wkt}, point={point:?}");
        assert_eq!(
            queries.locate_exact(point).unwrap(),
            streaming,
            "{wkt}, point={point:?}"
        );
        assert_eq!(
            source.locate_exact(point).unwrap(),
            expected,
            "{wkt}, point={point:?}",
        );
        assert_eq!(
            area.as_ref()
                .is_some_and(|index| index.locate_exact(point).unwrap()),
            expected.area,
            "{wkt}, point={point:?}",
        );
        for direction in directions {
            assert_eq!(
                queries.locate_exact_offset(point, direction).unwrap(),
                cartesian_exact_offset_location_and_area(&shape, point, direction).unwrap(),
                "{wkt}, point={point:?}, direction={direction:?}",
            );
            let streaming_offset =
                cartesian_exact_offset_location_and_area(&shape, point, direction)
                    .expect("streaming exact offset");
            let expected_offset = tester
                .location_and_area_query(
                    ExactProbe::new(point)
                        .source_query()
                        .unwrap()
                        .offset(direction),
                )
                .expect("prepared exact offset");
            assert_eq!(expected_offset, streaming_offset);
            assert_eq!(
                source
                    .locate_exact_offset(point, direction)
                    .expect("indexed exact offset"),
                expected_offset,
                "{wkt}, point={point:?}, direction={direction:?}",
            );
        }
    }
}

fn exact_xy(x: f64, y: f64) -> [ExactDyadic; 3] {
    [
        ExactDyadic::from_f64(x),
        ExactDyadic::from_f64(y),
        ExactDyadic::one(),
    ]
}

fn exact_direction(x: f64, y: f64) -> [ExactDyadic; 2] {
    [ExactDyadic::from_f64(x), ExactDyadic::from_f64(y)]
}

fn square_with_hole() -> Polygon {
    Polygon {
        shell: closed_ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]),
        holes: SharedSlice::from_vec(vec![closed_ring(&[
            (3.0, 3.0),
            (7.0, 3.0),
            (7.0, 7.0),
            (3.0, 7.0),
            (3.0, 3.0),
        ])]),
    }
}

fn sheared_comb(teeth: usize) -> Shape {
    let teeth = teeth as f64;
    let mut points = vec![(1.0, -2.0), (2.0 * teeth, -2.0), (2.0 * teeth, -1.0)];
    for index in (0..teeth as usize).rev() {
        let left = 1.0 + 2.0 * index as f64;
        let right = left + 1.0;
        if points.last() != Some(&(right, -1.0)) {
            points.push((right, -1.0));
        }
        points.extend([(right, 1.0), (left, 1.0), (left, -1.0)]);
    }
    let mut points: Vec<_> = points
        .into_iter()
        .map(|(x, y)| (x + 4.0 * teeth * y, y))
        .collect();
    points.push(points[0]);
    Shape::Polygon(Polygon::new(closed_ring(&points), Vec::new()))
}
