    #[test]
fn contains_and_covers_match_shape_kernels() {
        let poly = square_with_hole();
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape).unwrap();
        let probes = [
            (5.0, 5.0),
            (1.0, 1.0),
            (0.0, 5.0),
            (3.0, 5.0),
            (20.0, 20.0),
            (5.0, 1.0),
        ];
        for &(x, y) in &probes {
            let p = Point::new_raw_xy(x, y);
            assert_eq!(
                tester.contains_point(p),
                shape.contains_point(p),
                "contains at ({x},{y})"
            );
            assert_eq!(
                tester.covers_point(p),
                shape.covers_point(p),
                "covers at ({x},{y})"
            );
        }
    }

    #[test]
fn multipolygon_is_union_not_xor() {
        let a = unit_square();
        let b = Polygon {
            shell: closed_ring(&[(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5), (0.5, 0.5)]),
            holes: SharedSlice::empty(),
        };
        let shape = Shape::MultiPolygon(vec![a, b].into());
        let tester = PointBatchTester::new(&shape).unwrap();
        let overlap = Point::new_raw_xy(0.75, 0.75);
        assert!(tester.contains_point(overlap));
        let out = Point::new_raw_xy(3.0, 3.0);
        assert!(!tester.contains_point(out));
    }

    #[test]
fn hole_envelope_skips_non_covering_holes() {
        let poly = square_with_hole();
        let tester = PointBatchTester::new(&Shape::Polygon(poly)).unwrap();
        let p = Point::new_raw_xy(1.0, 9.0);
        assert!(tester.contains_point(p));
    }

    #[test]
fn tall_edge_ring_constructs_and_classifies() {
        let n_side = 2000_usize;
        let mut pts = Vec::with_capacity(2 * n_side + 3);
        for i in 0..=n_side {
            pts.push(XY::new(0.0, i as f64));
        }
        pts.push(XY::new(1.0, n_side as f64));
        for i in (0..n_side).rev() {
            pts.push(XY::new(1.0, i as f64));
        }
        pts.push(XY::new(0.0, 0.0));
        let poly = Polygon {
            shell: Ring::from_closed(CoordSeq::try_from_xy(&pts).unwrap()).unwrap(),
            holes: SharedSlice::empty(),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape).unwrap();
        assert!(tester.contains_point(Point::new_raw_xy(0.5, n_side as f64 * 0.5)));
        assert!(!tester.contains_point(Point::new_raw_xy(-1.0, n_side as f64 * 0.5)));
        let bytes = tester.heap_bytes();
        let verts = pts.len();
        assert!(
            bytes < verts * 128,
            "tall-edge prepared index not proportional: {bytes} bytes for {verts} verts"
        );
    }

    #[test]
fn many_sparse_holes_match_shape() {
        let shell = closed_ring(&[
            (0.0, 0.0),
            (1000.0, 0.0),
            (1000.0, 1000.0),
            (0.0, 1000.0),
            (0.0, 0.0),
        ]);
        let mut holes = Vec::new();
        for i in 0..100_i32 {
            let cx = 50.0 + f64::from(i % 10) * 90.0;
            let cy = 50.0 + f64::from(i / 10) * 90.0;
            let r = 10.0;
            holes.push(closed_ring(&[
                (cx - r, cy - r),
                (cx + r, cy - r),
                (cx + r, cy + r),
                (cx - r, cy + r),
                (cx - r, cy - r),
            ]));
        }
        let poly = Polygon {
            shell,
            holes: SharedSlice::from_vec(holes),
        };
        let shape = Shape::Polygon(poly);
        let tester = PointBatchTester::new(&shape).unwrap();
        for i in 0..200_i32 {
            let x = f64::from(i * 37 + 13) % 1000.0;
            let y = f64::from(i * 53 + 7) % 1000.0;
            let p = Point::new_raw_xy(x, y);
            assert_eq!(
                tester.contains_point(p),
                shape.contains_point(p),
                "mismatch at ({x},{y})"
            );
            assert_eq!(
                tester.covers_point(p),
                shape.covers_point(p),
                "covers mismatch at ({x},{y})"
            );
        }
    }

    #[test]
fn degenerate_and_zero_length_edges_fail_open_without_grid() {
        let shape = Shape::Polygon(Polygon {
            shell: closed_ring(&[(0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)]),
            holes: SharedSlice::empty(),
        });
        let tester = PointBatchTester::new(&shape).unwrap();
        assert!(!tester.uses_indexed_plan());
        for point in [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 1.0),
            Point::new_raw_xy(-1.0, 0.0),
        ] {
            assert_eq!(tester.contains_point(point), shape.contains_point(point));
            assert_eq!(tester.covers_point(point), shape.covers_point(point));
        }
    }
