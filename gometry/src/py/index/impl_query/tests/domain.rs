#[rstest::rstest]
#[case::predicate_crosses(Predicate::Crosses)]
#[case::predicate_overlaps(Predicate::Overlaps)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn refinement_kind_gate_precedes_geographic_admission(#[case] predicate: Predicate) {
    let frame = Frame::new(Some(frame::crs_arc_static("EPSG:4326")), None).unwrap();
    let index = spatial_index(vec![PyGeometry::with_frame(
        ShapeData::new(Shape::Point(Point::new_raw_xy(0.0, 0.0))),
        frame.clone(),
    )])
    .unwrap();
    let query = PyGeometry::with_frame(
        ShapeData::new(Shape::Point(Point::new_raw_xy(0.0, 91.0))),
        frame,
    );

    let plan = PreparedIndexQuery::Topological {
        query_domain: QueryDomain::default(),
        predicate,
    };
    let query_plan = query.prepared_plan();
    let query_cache = query.frame_cache();
    assert_eq!(
        index
            .refine_prepared(&query_plan, Some(&query_cache), vec![0], &plan)
            .unwrap(),
        Vec::<usize>::new()
    );
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn native_array_queries_retain_the_logical_error_row() {
    let index = spatial_index(Vec::new()).unwrap();
    let frame = Frame::new(Some(frame::crs_arc_static("EPSG:4326")), None).unwrap();
    let present = PyGeometryArray::packed_points(
        CoordSeq::try_from_vecs(vec![7.0], vec![95.0], None, None).unwrap(),
        frame,
    )
    .unwrap();
    let queries = PyGeometryArray::scatter_present_rows(
        &present,
        ValidityMask::from_vec(2, vec![true, false]).unwrap(),
    )
    .unwrap();

    let query_error = index
        .query_array(
            &queries,
            IndexPredicate::Topological(Predicate::Intersects),
            None,
            None,
        )
        .err()
        .expect("invalid query latitude");
    let candidate_error = index
        .candidate_array(&queries, None, None)
        .err()
        .expect("invalid candidate latitude");
    let nearest_error = index
        .nearest_array(
            &queries,
            1,
            None,
            false,
            None,
            super::super::NearestOptions {
                exclude_equal: false,
                include_ties: false,
            },
        )
        .err()
        .expect("invalid nearest latitude");
    for error in [query_error, candidate_error, nearest_error] {
        let crate::error::BatchError::Row { row, error } = error else {
            panic!("array domain failure must retain its logical row");
        };
        assert_eq!(row, 1);
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))
        ));
    }
}
