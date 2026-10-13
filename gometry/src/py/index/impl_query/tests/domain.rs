    #[test]
fn refinement_kind_gate_precedes_geographic_admission() {
        crate::test_support::initialize_python();
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
        for predicate in [Predicate::Crosses, Predicate::Overlaps] {
            let plan = PreparedIndexQuery::Topological {
                query_domain: QueryDomain::default(),
                predicate,
            };
            assert_eq!(
                index.refine_prepared(&query, vec![0], &plan).unwrap(),
                Vec::<usize>::new()
            );
        }
    }
