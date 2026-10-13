

    #[test]
    fn seam_alias_rectangle_is_collapsed_line_support() {
        let shape = Shape::Polygon(Polygon::new(
            ring(&[
                (-180.0, -85.0),
                (180.0, -85.0),
                (180.0, 85.0),
                (-180.0, 85.0),
                (-180.0, -85.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&shape);
        assert_eq!(topology.sources.rings[0].kind, RingKind::Collapsed);
        assert_eq!(
            topology.sources.rings[0].summary.domain,
            crate::geographic::RingDomain::SeamOnly
        );
        let central = topology
            .locate_point(crate::geometry::Point::new_raw_xy(0.0, 0.0))
            .unwrap();
        assert_eq!(central.geometry, Loc::Exterior);
        assert_eq!(central.area, Loc::Exterior);
        let seam = topology
            .locate_point(crate::geometry::Point::new_raw_xy(180.0, 0.0))
            .unwrap();
        assert_eq!(seam.geometry, Loc::Interior);
        assert_eq!(seam.area, Loc::Exterior);
    }

    #[test]
    fn retraced_collapsed_shell_keeps_curve_support_without_area() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 0.0), (0.0, 0.0), (0.0, 0.0)]),
            vec![],
        ));
        let topology = exact_topology(&source);
        assert_eq!(
            topology.locate_point(Point::new_raw_xy(2.0, 0.0)).unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            },
        );
        assert_eq!(
            topology.locate_point(Point::new_raw_xy(2.0, 1.0)).unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Exterior,
                area: Loc::Exterior,
            },
        );
    }

    #[test]
    fn exact_queries_keep_only_collapsed_shell_support_outside_areal_holes() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (10.0, 0.0), (5.0, 0.0), (0.0, 0.0)]),
            vec![ring(&[
                (2.0, -1.0),
                (8.0, -1.0),
                (8.0, 1.0),
                (2.0, 1.0),
                (2.0, -1.0),
            ])],
        ));
        let topology = exact_topology(&source);
        assert_eq!(
            topology.locate_point(Point::new_raw_xy(1.0, 0.0)).unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            }
        );
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(5.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(0.5, -0.5, 1.5, 0.5))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(4.0, -0.5, 6.0, 0.5))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Outside)
        );
    }