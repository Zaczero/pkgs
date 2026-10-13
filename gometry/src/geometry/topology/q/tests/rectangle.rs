

    #[test]
    fn exact_rectangle_query_classifies_cover_contact_and_disjoint_regions() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&source);
        for (bounds, expected) in [
            (
                Bounds::new_raw(2.0, 2.0, 8.0, 8.0),
                ExactGeographicRectangleLocation::Interior,
            ),
            (
                Bounds::new_raw(0.0, 2.0, 8.0, 8.0),
                ExactGeographicRectangleLocation::Interior,
            ),
            (
                Bounds::new_raw(8.0, 2.0, 12.0, 8.0),
                ExactGeographicRectangleLocation::Boundary,
            ),
            (
                Bounds::new_raw(12.0, 12.0, 14.0, 14.0),
                ExactGeographicRectangleLocation::Outside,
            ),
        ] {
            assert_eq!(topology.classify_rectangle(bounds).unwrap(), Some(expected));
        }
        let adjacent_max = 1.0_f64.next_up();
        let adjacent_bounds = Bounds::new_raw(1.0, 0.0, adjacent_max, 1.0);
        assert_eq!(f64::midpoint(1.0, adjacent_max), 1.0);
        let adjacent_source = Shape::Polygon(Polygon::new(
            ring(&[
                (1.0, 0.0),
                (adjacent_max, 0.0),
                (adjacent_max, 1.0),
                (1.0, 1.0),
                (1.0, 0.0),
            ]),
            vec![],
        ));
        assert_eq!(
            exact_topology(&adjacent_source)
                .classify_rectangle(adjacent_bounds)
                .unwrap(),
            None,
        );
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(-180.0, 1.0, -179.0, 2.0))
                .unwrap(),
            None
        );

        let donut = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])],
        ));
        let donut_topology = exact_topology(&donut);
        assert_eq!(
            donut_topology
                .classify_rectangle(Bounds::new_raw(4.0, 4.0, 6.0, 6.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Outside)
        );
        assert_eq!(
            donut_topology
                .classify_rectangle(Bounds::new_raw(2.0, 2.0, 5.0, 5.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );
        assert_eq!(
            donut_topology
                .classify_rectangle(Bounds::new_raw(3.0, 3.0, 7.0, 7.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );

        let line_in_hole =
            Shape::geometry_collection(vec![donut, line(&[(4.0, 5.0), (6.0, 5.0)])]).unwrap();
        assert_eq!(
            exact_topology(&line_in_hole)
                .classify_rectangle(Bounds::new_raw(3.0, 3.0, 7.0, 7.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );

        let seam = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, -10.0),
                (-170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, -10.0),
            ]),
            vec![],
        ));
        let seam_topology = exact_topology(&seam);
        assert_eq!(
            seam_topology
                .classify_rectangle(Bounds::new_raw(175.0, -5.0, 179.0, 5.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Interior)
        );
        assert_eq!(
            seam_topology
                .classify_rectangle(Bounds::new_raw(-5.0, -5.0, 5.0, 5.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Outside)
        );

        let polar = Shape::Polygon(Polygon::new(
            ring(&[
                (-10.0, 80.0),
                (10.0, 80.0),
                (10.0, 90.0),
                (-10.0, 90.0),
                (-10.0, 80.0),
            ]),
            vec![],
        ));
        assert_eq!(
            exact_topology(&polar)
                .classify_rectangle(Bounds::new_raw(0.0, 81.0, 1.0, 82.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Interior)
        );

        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 5.0),
                    Point::new_raw_xy(10.0, 5.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        assert_eq!(
            exact_topology(&line)
                .classify_rectangle(Bounds::new_raw(2.0, 4.0, 8.0, 6.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );

        let point = Shape::Point(Point::new_raw_xy(5.0, 5.0));
        assert_eq!(
            exact_topology(&point)
                .classify_rectangle(Bounds::new_raw(4.0, 4.0, 6.0, 6.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(0.0, 85.0, 1.0, 90.0))
                .unwrap(),
            None
        );
        for bounds in [
            Bounds::new_raw(-180.0, 1.0, -179.0, 2.0),
            Bounds::new_raw(0.0, 85.0, 10.0, 90.0),
            Bounds::new_raw(-180.0, -1.0, 180.0, 1.0),
        ] {
            assert_eq!(topology.classify_rectangle(bounds).unwrap(), None);
        }
    }

    #[test]
    fn exact_rectangle_query_splits_at_the_selected_q_cut() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, -10.0),
                (-170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, -10.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&source);
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(-1.0, -1.0, 1.0, 1.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Outside)
        );

        let cap = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
            ]),
            vec![],
        ));
        assert_eq!(
            exact_topology(&cap)
                .classify_rectangle(Bounds::new_raw(-140.0, 81.0, -130.0, 82.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Interior)
        );
    }