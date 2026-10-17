    #[test]
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
                &crate::geometry::FrameDependentCaches::default(),
                Predicate::Intersects,
                QueryDomain::default(),
                None,
            )
            .unwrap();
        assert!(matches.is_empty());
    }
