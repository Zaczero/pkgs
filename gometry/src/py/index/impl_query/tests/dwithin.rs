    #[test]
fn dwithin_exact_refinement_preserves_unset_query_bounds() {
        let index = spatial_index(vec![PyGeometry::with_frame(
            ShapeData::new(Shape::Point(Point::new_raw_xy(0.0, 0.0))),
            Frame::None,
        )])
        .unwrap();
        let array = PyGeometryArray::mixed_shapes(
            vec![Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_vecs(vec![0.0, 1.0], vec![0.0, 1.0], None, None).unwrap(),
                )
                .expect("fixture line has two vertices"),
            )],
            Frame::None,
        )
        .unwrap();
        let plan = PreparedIndexQuery::for_array(
            &index,
            &array,
            Some(IndexPredicate::Dwithin),
            Some(DwithinDistance::try_new("distance", 0.0).unwrap()),
            None,
        )
        .unwrap();
        let mut matches = Vec::new();

        index
            .planar_dwithin_query_row_matches_append(
                array.storage().row(0),
                &plan,
                &mut matches,
                BoundsSeed::Unset,
                None,
            )
            .unwrap();

        assert_eq!(matches, [0]);
        assert_eq!(array.row_products.initialized_counts(), (0, 0, 0));
    }

    #[test]
#[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
fn public_spatial_index_keeps_a_full_turn_line_for_nearest_and_dwithin() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let frame = Frame::new(Some(frame::crs_arc_static("EPSG:4326")), None)
                .expect("CRS84 is a valid geographic frame");
            let circle = Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_vecs(vec![10.0, 370.0], vec![60.0, 60.0], None, None)
                        .expect("full-turn line coordinates are valid"),
                )
                .expect("two vertices form a line"),
            );
            let decoys = Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_vecs(
                        vec![-160.0, -120.0, -80.0, -40.0, 0.0, 40.0, 80.0, 120.0],
                        vec![0.0; 8],
                        None,
                        None,
                    )
                    .expect("decoy line coordinates are valid"),
                )
                .expect("eight vertices form a line"),
            );
            let target = crate::Typed(PyGeometry::with_frame(
                Shape::geometry_collection(vec![circle, decoys])
                    .expect("two lines form a geometry collection"),
                frame.clone(),
            ))
            .into_pyobject(py)
            .expect("collection binds as its public typed geometry");
            let query = crate::Typed(PyGeometry::with_frame(
                Shape::Point(Point::new_raw_xy(200.0, 50.0)),
                frame,
            ))
            .into_pyobject(py)
            .expect("point binds as its public typed geometry");
            let values = PyList::new(py, [target.clone()]).expect("indexed sequence");
            let index = build_spatial_index(py, values.as_any())
                .expect("public spatial-index construction path");
            let index = Bound::new(py, index).expect("spatial index binds");

            let nearest_kwargs = PyDict::new(py);
            nearest_kwargs
                .set_item("max_distance", 1_200_000.0)
                .expect("nearest ceiling is a keyword");
            let nearest = index
                .call_method("nearest", (&query,), Some(&nearest_kwargs))
                .expect("public nearest query succeeds");
            assert_eq!(
                nearest
                    .call_method0("tolist")
                    .expect("nearest results form an array")
                    .extract::<Vec<i64>>()
                    .expect("nearest handles are integers"),
                [0],
            );

            let dwithin_kwargs = PyDict::new(py);
            dwithin_kwargs
                .set_item("predicate", "dwithin")
                .expect("query predicate is a keyword");
            dwithin_kwargs
                .set_item("distance", 1_200_000.0)
                .expect("query distance is a keyword");
            let matches = index
                .call_method("query", (&query,), Some(&dwithin_kwargs))
                .expect("public dwithin query succeeds");
            assert_eq!(
                matches
                    .call_method0("tolist")
                    .expect("query matches form an array")
                    .extract::<Vec<i64>>()
                    .expect("query handles are integers"),
                [0],
            );
        });
    }
