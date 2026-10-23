#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
fn scalar_rows_relate_from_fixed_graphs_in_both_cartesian_orders(#[case] scalar_is_left: bool) {
    let scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
    let rows = [line((-1.0, 1.0), (3.0, 1.0)), line((3.0, 3.0), (4.0, 4.0))];
    let cache = crate::geometry::FrameDependentCaches::default();

    let actual = scalar_vs_shapes(
        &Predicate::Intersects.spec(),
        &scalar,
        rows.iter()
            .enumerate()
            .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
        scalar_is_left,
        None,
        TopologyDomain::Cartesian,
        &cache,
    )
    .expect("fixture admits");
    assert_eq!(actual, [true, false]);
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn refuted_scalar_rows_do_not_admit_topology() {
    let rows = [line((10.0, 10.0), (11.0, 11.0))];
    let cartesian = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
    let cartesian_cache = crate::geometry::FrameDependentCaches::default();
    assert_eq!(
        scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &cartesian,
            rows.iter()
                .enumerate()
                .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
            true,
            None,
            TopologyDomain::Cartesian,
            &cartesian_cache,
        )
        .expect("fixture admits"),
        [false]
    );
    let geographic = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
    let geographic_cache = crate::geometry::FrameDependentCaches::default();
    assert_eq!(
        scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &geographic,
            rows.iter()
                .enumerate()
                .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &geographic_cache,
        )
        .expect("fixture admits"),
        [false]
    );
    assert_eq!(geographic.geographic_exact_topology_accesses(), 0);
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn admitted_scalar_rows_use_exact_membership() {
    let rows = [line((-1.0, 0.5), (3.0, 0.5)), line((-1.0, 1.5), (3.0, 1.5))];
    let cartesian = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
    let cartesian_cache = crate::geometry::FrameDependentCaches::default();
    assert_eq!(
        scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &cartesian,
            rows.iter()
                .enumerate()
                .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
            true,
            None,
            TopologyDomain::Cartesian,
            &cartesian_cache,
        )
        .expect("fixture admits"),
        [true, true]
    );
    let line_scalar = PreparedPlan::new(line((-1.0, 1.0), (3.0, 1.0)));
    let polygon_rows = [rectangle(0.0, 0.0, 2.0, 2.0)];
    for scalar_is_left in [true, false] {
        assert_eq!(
            scalar_vs_shapes(
                &Predicate::Intersects.spec(),
                &line_scalar,
                polygon_rows
                    .iter()
                    .enumerate()
                    .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
                scalar_is_left,
                None,
                TopologyDomain::Cartesian,
                &crate::geometry::FrameDependentCaches::default(),
            )
            .expect("fixture admits"),
            [true]
        );
    }
    let geographic = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
    let geographic_cache = crate::geometry::FrameDependentCaches::default();
    assert_eq!(
        scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            &geographic,
            rows.iter()
                .enumerate()
                .map(|(index, shape)| (index, Some(ShapeRow::Shape(shape)))),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &geographic_cache,
        )
        .expect("fixture admits"),
        [true, true]
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_line_scalar_does_not_enter_cartesian_direct_relation() {
    let scalar = PreparedPlan::new(line((175.0, 0.0), (-175.0, 0.0)));
    let polygon = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(170.0, -10.0),
                Point::new_raw_xy(-170.0, -10.0),
                Point::new_raw_xy(-170.0, 10.0),
                Point::new_raw_xy(170.0, 10.0),
                Point::new_raw_xy(170.0, -10.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    assert_eq!(
        scalar_vs_shapes(
            &Predicate::Within.spec(),
            &scalar,
            [(0, Some(ShapeRow::Shape(&polygon)))].into_iter(),
            true,
            None,
            TopologyDomain::Geographic(degrees()),
            &crate::geometry::FrameDependentCaches::default(),
        )
        .expect("fixture admits"),
        [true]
    );
}

#[test]
fn scalar_rows_relate_prepared_array_rows_from_fixed_graphs() {
    let scalar = PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0));
    let array = PyGeometryArray::mixed_shapes(
        vec![line((-1.0, 1.0), (3.0, 1.0)), line((3.0, 3.0), (4.0, 4.0))],
        Frame::None,
    )
    .unwrap();
    let cache = crate::geometry::FrameDependentCaches::default();
    let actual = scalar_vs_shapes(
        &Predicate::Intersects.spec(),
        &scalar,
        array
            .row_operation()
            .rows_with_missing(|| ())
            .map(|row| match row {
                crate::array::ArrayRow::Missing { logical, .. } => (logical, None),
                crate::array::ArrayRow::Present(row) => (row.logical(), Some(row.row())),
            }),
        false,
        Some(&array),
        TopologyDomain::Cartesian,
        &cache,
    )
    .expect("fixture admits");
    assert_eq!(actual, [true, false]);
}

#[rstest::rstest]
#[case::intersects(Predicate::Intersects, false)]
#[case::disjoint(Predicate::Disjoint, true)]
fn shared_collection_intersection_uses_occupied_support_once(
    #[case] predicate: Predicate,
    #[case] expected: bool,
) {
    let source = parse_wkt_shape("POLYGON ((0 0,4 0,4 4,2 4,2 7,2 4,0 4,0 0))");
    let source = shared_collection(source, 128);
    let probe = parse_wkt_shape("LINESTRING (1 6,3 6)");
    assert_eq!(cartesian_predicate(predicate, &source, &probe), expected);
    assert_eq!(cartesian_predicate(predicate, &probe, &source), expected);
}

#[rstest::rstest]
#[case::sound(PreparedPlan::new(rectangle(0.0, 0.0, 2.0, 2.0)), line((-1.0, 1.0), (3.0, 1.0)))]
#[case::unsound(PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0)), rectangle(170.0, 40.0, -170.0, 50.0))]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_scalar_rows_cover_sound_and_unsound_operands_in_both_orders(
    #[case] scalar: PreparedPlan,
    #[case] row: Shape,
) {
    let scalar = &scalar;
    let row = &row;
    let cache = crate::geometry::FrameDependentCaches::default();
    for scalar_is_left in [true, false] {
        let actual = scalar_vs_shapes(
            &Predicate::Intersects.spec(),
            scalar,
            [(0, Some(ShapeRow::Shape(row)))].into_iter(),
            scalar_is_left,
            None,
            TopologyDomain::Geographic(degrees()),
            &cache,
        )
        .expect("fixture admits");
        assert_eq!(actual, [true]);
    }
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_scalar_rows_relate_prepared_unsound_array_rows() {
    let scalar = PreparedPlan::new(rectangle(172.0, 42.0, 178.0, 48.0));
    let array =
        PyGeometryArray::mixed_shapes(vec![rectangle(170.0, 40.0, -170.0, 50.0)], Frame::None)
            .unwrap();
    let cache = crate::geometry::FrameDependentCaches::default();
    let actual = scalar_vs_shapes(
        &Predicate::Intersects.spec(),
        &scalar,
        array
            .row_operation()
            .rows_with_missing(|| ())
            .map(|row| match row {
                crate::array::ArrayRow::Missing { logical, .. } => (logical, None),
                crate::array::ArrayRow::Present(row) => (row.logical(), Some(row.row())),
            }),
        false,
        Some(&array),
        TopologyDomain::Geographic(degrees()),
        &cache,
    )
    .expect("fixture admits");
    assert_eq!(actual, [true]);
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn geographic_try_for_domain_rejects_invalid_right_latitude() {
    let left = PreparedPlan::new(rectangle(0.0, 0.0, 1.0, 1.0));
    let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(10.0, 91.0)));
    let left_cache = crate::geometry::FrameDependentCaches::default();
    let right_cache = crate::geometry::FrameDependentCaches::default();
    let Err(error) = PreparedTopologyPair::try_for_domain(
        &left,
        &left_cache,
        &right,
        &right_cache,
        TopologyDomain::Geographic(degrees()),
    ) else {
        panic!("invalid latitude must fail admission")
    };
    let message = error.to_string();
    assert!(
        message.contains("invalid longitude/latitude (10, 91)"),
        "{message}"
    );
}
