#[rstest::rstest]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
#[case::predicate_crosses(Predicate::Crosses)]
#[case::predicate_overlaps(Predicate::Overlaps)]
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
    assert_eq!(
        index.refine_prepared(&query, vec![0], &plan).unwrap(),
        Vec::<usize>::new()
    );
}
