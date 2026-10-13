    #[test]
fn point_probe_policy_indexes_polygon_hierarchy() {
        let mut polygon = unit_square();
        polygon.holes = SharedSlice::from_vec(
            std::iter::repeat_with(|| {
                closed_ring(&[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0)])
            })
            .take(33)
            .collect::<Vec<_>>(),
        );
        let tester = PointBatchTester::new(&Shape::Polygon(polygon)).unwrap();
        assert!(tester.uses_indexed_plan());
    }

    #[test]
fn point_probe_policy_indexes_33_multipolygon_parts() {
        let parts = (0..33)
            .map(|index| {
                let x = f64::from(index) * 2.0;
                Polygon {
                    shell: closed_ring(&[
                        (x, 0.0),
                        (x + 1.0, 0.0),
                        (x + 1.0, 1.0),
                        (x, 1.0),
                        (x, 0.0),
                    ]),
                    holes: SharedSlice::empty(),
                }
            })
            .collect::<Vec<_>>();
        let tester = PointBatchTester::new(&Shape::MultiPolygon(
            MultiPolygon::from_dense(parts).unwrap(),
        ))
        .unwrap();
        assert!(tester.uses_indexed_plan());
    }

    #[test]
fn explicitly_closed_ring_indexes_only_its_adjacent_edges() {
        let ring = closed_ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let index = RingPointIndex::build(&ring).unwrap();
        let mut edges = 0;
        index.edges.for_each_edge(0.0, |edge| {
            let (ax, ay, bx, by) = index.edge_xy(edge);
            assert!(!same_point(XY::new(ax, ay), XY::new(bx, by)));
            edges += 1;
            ControlFlow::<()>::Continue(())
        });
        assert_eq!(edges, 4);
        for (point, expected) in [
            (Point::new_raw_xy(0.0, 0.0), Loc::Boundary),
            (Point::new_raw_xy(0.0, 2.0), Loc::Boundary),
            (Point::new_raw_xy(2.0, 2.0), Loc::Interior),
            (Point::new_raw_xy(-1.0, 2.0), Loc::Exterior),
        ] {
            let polygon = Polygon::new(ring.clone(), Vec::new());
            assert_eq!(
                PolygonPointIndex::build(&polygon)
                    .unwrap()
                    .location_and_area(point)
                    .location,
                expected,
            );
        }
    }

    #[test]
fn dense_ring_classification_matches_shape_kernel() {
        let points: Vec<_> = dense_square_points()
            .into_iter()
            .map(|(x, y)| XY::new(x, y))
            .collect();
        let coords = CoordSeq::try_from_xy(&points).unwrap();
        assert_eq!(coords.xs().len() - 1, 36);
        let ring = RingPointIndex::build(&Ring::from_closed(coords.clone()).unwrap()).unwrap();
        assert!(ring.edges.dense_band_edges(5.0).is_some());

        for (x, y) in [(5.0, 0.0), (5.0, 5.0), (9.0, 5.0), (-1.0, 5.0)] {
            assert_eq!(
                winding_class(&ring, x, y),
                ring_classify_point(&coords, XY::new(x, y).point()),
                "classification mismatch at ({x}, {y})"
            );
        }
    }

    #[test]
fn full_window_ring_index_shares_xy_and_survives_source_drop() {
        let coords = CoordSeq::try_from_vecs(
            [0.0, 2.0, 2.0, 0.0, 0.0].into(),
            [0.0, 0.0, 2.0, 2.0, 0.0].into(),
            None,
            None,
        )
        .unwrap();
        let columns = coords.column_arcs();
        let source_xs = columns.xs.owner().unwrap();
        let source_ys = columns.ys.owner().unwrap();
        let ring = RingPointIndex::build(&Ring::from_closed(coords.clone()).unwrap()).unwrap();

        assert!(Arc::ptr_eq(&ring.xs.owner().unwrap(), &source_xs));
        assert!(Arc::ptr_eq(&ring.ys.owner().unwrap(), &source_ys));

        drop(columns);
        drop(coords);
        drop(source_xs);
        drop(source_ys);

        assert_eq!(winding_class(&ring, 1.0, 1.0), RingClass::Interior);
        assert_eq!(winding_class(&ring, 0.0, 1.0), RingClass::Boundary);
        assert_eq!(winding_class(&ring, 3.0, 1.0), RingClass::Exterior);
    }

    #[test]
fn windowed_ring_index_reads_visible_coordinates() {
        let source_xs = SharedSlice::from_vec(vec![1000.0, 0.0, 2.0, 2.0, 0.0, 0.0, -1000.0]);
        let source_ys = SharedSlice::from_vec(vec![1000.0, 0.0, 0.0, 2.0, 2.0, 0.0, -1000.0]);
        let source_xs_weak = Arc::downgrade(&source_xs.owner().unwrap());
        let source_ys_weak = Arc::downgrade(&source_ys.owner().unwrap());
        let coords =
            CoordSeq::from_shared_columns(source_xs.clone(), source_ys.clone(), None, None)
                .unwrap();
        let visible = coords.view(CoordWindow::from_range(1..6, coords.len()));
        let ring = RingPointIndex::build(&Ring::from_closed(visible.clone()).unwrap()).unwrap();

        drop(visible);
        drop(coords);
        drop(source_xs);
        drop(source_ys);
        assert!(source_xs_weak.upgrade().is_none());
        assert!(source_ys_weak.upgrade().is_none());

        assert_eq!(winding_class(&ring, 1.0, 1.0), RingClass::Interior);
        assert_eq!(winding_class(&ring, 0.0, 1.0), RingClass::Boundary);
        assert_eq!(winding_class(&ring, 3.0, 1.0), RingClass::Exterior);
    }

    #[test]
fn xyzm_ring_index_reads_xy_columns() {
        let coords = CoordSeq::try_from_vecs(
            [0.0, 2.0, 2.0, 0.0, 0.0].into(),
            [0.0, 0.0, 2.0, 2.0, 0.0].into(),
            Some([10.0, 11.0, 12.0, 13.0, 10.0].into()),
            Some([20.0, 21.0, 22.0, 23.0, 20.0].into()),
        )
        .unwrap();
        let columns = coords.column_arcs();
        let z_weak = Arc::downgrade(&columns.zs.as_ref().expect("Z column").owner().unwrap());
        let m = columns.m_parts().expect("M column").0;
        let m_weak = Arc::downgrade(&m.owner().unwrap());
        let ring = RingPointIndex::build(&Ring::from_closed(coords.clone()).unwrap()).unwrap();

        assert!(Arc::ptr_eq(
            &ring.xs.owner().unwrap(),
            &columns.xs.owner().unwrap()
        ));
        assert!(Arc::ptr_eq(
            &ring.ys.owner().unwrap(),
            &columns.ys.owner().unwrap()
        ));
        drop(m);
        drop(columns);
        drop(coords);
        assert!(z_weak.upgrade().is_none());
        assert!(m_weak.upgrade().is_none());
        assert_eq!(winding_class(&ring, 1.0, 1.0), RingClass::Interior);
        assert_eq!(winding_class(&ring, 0.0, 1.0), RingClass::Boundary);
        assert_eq!(winding_class(&ring, 3.0, 1.0), RingClass::Exterior);
    }
