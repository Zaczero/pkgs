#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn projected_candidate_preparation_does_not_materialize_polygon_shape() {
    let frame = Frame::new(Some(frame::crs_arc_static("EPSG:3857")), None).unwrap();
    let array = PyGeometryArray::packed_polygons(
        CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 1.0, 0.0, 0.0],
            vec![0.0, 0.0, 1.0, 1.0, 0.0],
            None,
            None,
        )
        .unwrap(),
        CsrOffsetColumn::<RingLevel>::try_new(vec![0, 5], 5).unwrap(),
        CsrOffsetColumn::<PolygonLevel>::try_new(vec![0, 1], 1).unwrap(),
        frame.clone(),
    )
    .unwrap();
    let index = spatial_index(vec![PyGeometry::with_frame(
        ShapeData::new(Shape::Point(Point::new_raw_xy(10.0, 10.0))),
        frame,
    )])
    .unwrap();
    let plan = PreparedIndexQuery::for_array(&index, &array, None, None, None).unwrap();
    let row = array
        .row_operation()
        .present_row_at(0)
        .expect("polygon row")
        .row();
    let bounds = row.quick_bounds().expect("polygon bounds");
    let prepared = crate::test_support::assert_no_allocations(|| {
        plan.row(&index, row, BoundsSeed::Value(Some(bounds)), None)
    })
    .expect("seeded projected candidate preparation must not allocate");
    assert_eq!(prepared.bounds, Some(bounds));
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn empty_cartesian_query_has_no_index_matches() {
    pyo3::Python::initialize();
    let item = PyGeometry::with_frame(
        ShapeData::new(Shape::Point(Point::new_raw_xy(0.0, 0.0))),
        Frame::None,
    );
    let index = spatial_index(vec![item]).unwrap();
    let query = PreparedPlan::new(ShapeData::new(Shape::empty_polygon()));
    let matches = index
        .topological_matches(
            &query,
            None,
            Predicate::Intersects,
            QueryDomain::default(),
            None,
        )
        .unwrap();
    assert!(matches.is_empty());
}

#[test]
fn topological_refinement_resolves_each_candidate_once() {
    fn assert_candidate_resolutions(index: &PySpatialIndex) {
        let query = PreparedPlan::new(ShapeData::new(Shape::Point(Point::new_raw_xy(0.0, 0.0))));
        let mut candidates = vec![0, 1, 2];
        let mut products = index.rows.operation();

        IndexRowOperation::reset_resolution_count();
        super::refine::retain_topological(
            &query,
            None,
            &Predicate::Intersects.spec(),
            TopologyDomain::Cartesian,
            &mut products,
            &mut candidates,
            0,
        )
        .unwrap();

        assert_eq!(candidates, [0]);
        assert_eq!(IndexRowOperation::resolution_count(), 3);
    }

    let boxed = spatial_index(
        (0..3)
            .map(|x| {
                PyGeometry::with_frame(
                    ShapeData::new(Shape::Point(Point::new_raw_xy(f64::from(x), 0.0))),
                    Frame::None,
                )
            })
            .collect(),
    )
    .unwrap();
    assert_candidate_resolutions(&boxed);

    let packed = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(vec![0.0, 1.0, 2.0], vec![0.0, 0.0, 0.0], None, None).unwrap(),
        Frame::None,
    )
    .unwrap();
    let packed = super::super::build::packed_spatial_index(&packed)
        .unwrap()
        .expect("packed rows build an index");
    assert_candidate_resolutions(&packed);
}

#[test]
fn cartesian_topological_refinement_does_not_create_frame_sidecars() {
    let stored = PyGeometry::with_frame(
        ShapeData::new(Shape::Point(Point::new_raw_xy(0.0, 0.0))),
        Frame::None,
    );
    let observed = stored.clone();
    let index = spatial_index(vec![stored]).unwrap();
    let query = PyGeometry::with_frame(
        ShapeData::new(Shape::Point(Point::new_raw_xy(0.0, 0.0))),
        Frame::None,
    );

    assert_eq!(
        index
            .query_exact(&query, IndexPredicate::Topological(Predicate::Intersects), None, None)
            .unwrap(),
        [0]
    );
    assert_eq!(observed.row_products.initialized_counts().2, 0);
}
