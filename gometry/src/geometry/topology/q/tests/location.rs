

    #[test]
    fn exact_point_query_locates_areal_faces_and_boundaries() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            vec![],
        ));
        let topology = exact_topology(&source);
        for (point, expected) in [
            (Point::new_raw_xy(2.0, 2.0), ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Interior,
            }),
            (Point::new_raw_xy(0.0, 2.0), ExactGeographicPointLocation {
                geometry: Loc::Boundary,
                area: Loc::Boundary,
            }),
            (Point::new_raw_xy(8.0, 8.0), ExactGeographicPointLocation {
                geometry: Loc::Exterior,
                area: Loc::Exterior,
            }),
        ] {
            assert_eq!(topology.locate_point(point).unwrap(), expected);
        }
        assert!(matches!(
            topology.locate_point(Point::new_raw_xy(0.0, 91.0)),
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
                )
        ));
    }

    #[test]
    fn exact_point_query_includes_line_and_isolated_point_support() {
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(10.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let line_topology = exact_topology(&line);
        assert_eq!(
            line_topology
                .locate_point(Point::new_raw_xy(5.0, 0.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            }
        );
        assert_eq!(
            line_topology
                .locate_point(Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Boundary
        );

        let point = Shape::Point(Point::new_raw_xy(12.0, 3.0));
        let point_topology = exact_topology(&point);
        assert_eq!(
            point_topology
                .locate_point(Point::new_raw_xy(12.0, 3.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            }
        );
        assert_eq!(
            point_topology
                .locate_point(Point::new_raw_xy(12.0, 4.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
    }

    #[test]
    fn exact_point_query_includes_seam_and_pole_alias_point_support() {
        let source = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(-180.0, 5.0)),
            Shape::Point(Point::new_raw_xy(180.0, 5.0)),
            Shape::Point(Point::new_raw_xy(-30.0, 90.0)),
            Shape::Point(Point::new_raw_xy(120.0, 90.0)),
            Shape::Point(Point::new_raw_xy(-180.0, -90.0)),
            Shape::Point(Point::new_raw_xy(47.0, -90.0)),
        ])
        .unwrap();
        let topology = exact_topology(&source);
        for point in [
            Point::new_raw_xy(-180.0, 5.0),
            Point::new_raw_xy(180.0, 5.0),
            Point::new_raw_xy(-30.0, 90.0),
            Point::new_raw_xy(120.0, 90.0),
            Point::new_raw_xy(-180.0, -90.0),
            Point::new_raw_xy(47.0, -90.0),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Exterior,
                },
                "source point alias {point:?}",
            );
        }
        for longitude in [-180.0, 180.0] {
            let point = Point::new_raw_xy(longitude, 6.0);
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Exterior,
                    area: Loc::Exterior,
                },
                "seam latitude without source point support {point:?}",
            );
        }
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(0.0, 5.0))
                .unwrap()
                .geometry,
            Loc::Exterior,
        );
    }

    #[test]
    fn chart_seam_point_on_a_crossing_line_is_interior() {
        let source = line(&[(170.0, 0.0), (-170.0, 0.0)]);
        let topology = exact_topology(&source);
        for longitude in [-180.0, 180.0] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 0.0))
                    .unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Exterior,
                },
                "chart seam longitude {longitude}",
            );
        }
        for longitude in [170.0, -170.0] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 0.0))
                    .unwrap()
                    .geometry,
                Loc::Boundary,
                "source endpoint longitude {longitude}",
            );
        }
    }

    #[test]
    fn exact_point_query_uses_mod2_line_boundaries() {
        let source = Shape::geometry_collection(vec![
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(1.0, 0.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(1.0, 0.0),
                        Point::new_raw_xy(2.0, 0.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
        ])
        .unwrap();
        let topology = exact_topology(&source);
        for (point, expected) in [
            (Point::new_raw_xy(0.0, 0.0), Loc::Boundary),
            (Point::new_raw_xy(1.0, 0.0), Loc::Interior),
            (Point::new_raw_xy(2.0, 0.0), Loc::Boundary),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: expected,
                    area: Loc::Exterior,
                },
            );
        }
    }

    #[test]
    fn duplicate_line_members_cancel_their_endpoint_boundaries() {
        let source = Shape::geometry_collection(vec![
            line(&[(0.0, 0.0), (2.0, 0.0)]),
            line(&[(0.0, 0.0), (2.0, 0.0)]),
            Shape::Point(Point::new_raw_xy(4.0, 4.0)),
        ])
        .unwrap();
        let topology = exact_topology(&source);
        for point in [
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Exterior,
                },
                "duplicate line support at {point:?}",
            );
        }
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(4.0, 4.0))
                .unwrap()
                .geometry,
            Loc::Interior,
        );
    }

    #[test]
    fn exact_point_query_maps_antimeridian_aliases_to_the_same_q_face() {
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
        for longitude in [-180.0, 180.0] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 0.0))
                    .unwrap()
                    .geometry,
                Loc::Interior
            );
        }
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
        for longitude in [-181.0, 179.0, 539.0] {
            let point = Point::new_raw_xy(longitude, 5.0);
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Interior,
                },
                "wrapped seam rectangle alias {point:?}",
            );
        }
    }

    #[test]
    fn exact_point_query_aggregates_overlapping_polygon_members_by_union() {
        let source = Shape::geometry_collection(vec![
            Shape::Polygon(Polygon::new(
                ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
                vec![],
            )),
            Shape::Polygon(Polygon::new(
                ring(&[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0), (2.0, 2.0)]),
                vec![],
            )),
        ])
        .unwrap();
        let topology = exact_topology(&source);
        assert_eq!(
            topology.locate_point(Point::new_raw_xy(3.0, 3.0)).unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Interior,
            },
        );
        assert_eq!(
            topology.locate_point(Point::new_raw_xy(1.0, 5.0)).unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Exterior,
                area: Loc::Exterior,
            },
        );
    }

    #[test]
    fn exact_point_query_keeps_mixed_area_curve_and_point_support_distinct() {
        let polygon = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            vec![ring(&[
                (1.0, 1.0),
                (3.0, 1.0),
                (3.0, 3.0),
                (1.0, 3.0),
                (1.0, 1.0),
            ])],
        ));
        let source = Shape::geometry_collection(vec![
            polygon,
            line(&[(-1.0, 2.0), (2.0, 2.0), (5.0, 2.0)]),
            Shape::Point(Point::new_raw_xy(8.0, 8.0)),
        ])
        .unwrap();
        let topology = exact_topology(&source);
        let one_ulp = 1.0_f64.next_up();
        for (point, geometry, area) in [
            (Point::new_raw_xy(0.5, 2.0), Loc::Interior, Loc::Interior),
            (Point::new_raw_xy(0.0, 2.0), Loc::Boundary, Loc::Boundary),
            (Point::new_raw_xy(1.0, 2.0), Loc::Boundary, Loc::Boundary),
            (Point::new_raw_xy(2.0, 2.0), Loc::Interior, Loc::Exterior),
            (Point::new_raw_xy(3.0, 2.0), Loc::Boundary, Loc::Boundary),
            (Point::new_raw_xy(4.0, 2.0), Loc::Boundary, Loc::Boundary),
            (Point::new_raw_xy(-1.0, 2.0), Loc::Boundary, Loc::Exterior),
            (Point::new_raw_xy(-0.5, 2.0), Loc::Interior, Loc::Exterior),
            (Point::new_raw_xy(2.0, 1.5), Loc::Exterior, Loc::Exterior),
            (Point::new_raw_xy(3.5, 2.0), Loc::Interior, Loc::Interior),
            (Point::new_raw_xy(5.0, 2.0), Loc::Boundary, Loc::Exterior),
            (Point::new_raw_xy(8.0, 8.0), Loc::Interior, Loc::Exterior),
            (
                Point::new_raw_xy(one_ulp, 1.0),
                Loc::Boundary,
                Loc::Boundary,
            ),
            (
                Point::new_raw_xy(1.0, one_ulp),
                Loc::Boundary,
                Loc::Boundary,
            ),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation { geometry, area },
                "source support at {point:?}",
            );
        }
    }

    #[test]
    fn exact_point_query_keeps_an_adjacent_float_source_endpoint() {
        let endpoint = (-170.0_f64).next_down();
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, 0.0),
                (endpoint, 0.0),
                (endpoint, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ]),
            vec![],
        ));
        assert_eq!(
            exact_topology(&source)
                .locate_point(Point::new_raw_xy(endpoint, 0.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Boundary,
                area: Loc::Boundary,
            },
        );
    }

    #[test]
    fn exact_point_query_keeps_a_seam_donut_outside_its_hole() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, -10.0),
                (-170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, -10.0),
            ]),
            vec![ring(&[
                (175.0, -5.0),
                (-175.0, -5.0),
                (-175.0, 5.0),
                (175.0, 5.0),
                (175.0, -5.0),
            ])],
        ));
        let topology = exact_topology(&source);
        for longitude in [-180.0, 180.0] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, -10.0))
                    .unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Boundary,
                    area: Loc::Boundary,
                },
                "lower seam shell boundary at longitude {longitude}",
            );
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 0.0))
                    .unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Exterior,
                    area: Loc::Exterior,
                },
            );
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 5.0))
                    .unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Boundary,
                    area: Loc::Boundary,
                },
            );
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 8.0))
                    .unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Interior,
                },
            );
        }
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(179.0, 8.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Interior,
            },
        );
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(180.0, 10.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Boundary,
                area: Loc::Boundary,
            },
        );
    }

    #[test]
    fn exact_point_query_preserves_original_mixed_seam_hole_and_cap_support() {
        let seam_polygon = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 12.0),
                (170.0, 12.0),
                (170.0, 0.0),
            ]),
            vec![ring(&[
                (175.0, 3.0),
                (-175.0, 3.0),
                (-175.0, 8.0),
                (175.0, 8.0),
                (175.0, 3.0),
            ])],
        ));
        let north_cap = Shape::Polygon(Polygon::new(
            ring(&[
                (-180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (-180.0, 80.0),
            ]),
            vec![],
        ));
        let source = Shape::geometry_collection(vec![
            seam_polygon,
            north_cap,
            line(&[(175.0, 6.0), (180.0, 6.0), (-175.0, 6.0)]),
        ])
        .unwrap();
        let topology = exact_topology(&source);
        for longitude in [-180.0, 180.0] {
            for (latitude, geometry, area) in [
                (0.0, Loc::Boundary, Loc::Boundary),
                (3.0, Loc::Boundary, Loc::Boundary),
                (5.0, Loc::Exterior, Loc::Exterior),
                (6.0, Loc::Interior, Loc::Exterior),
                (8.0, Loc::Boundary, Loc::Boundary),
                (12.0, Loc::Boundary, Loc::Boundary),
            ] {
                assert_eq!(
                    topology
                        .locate_point(Point::new_raw_xy(longitude, latitude))
                        .unwrap(),
                    ExactGeographicPointLocation { geometry, area },
                    "seam source at ({longitude}, {latitude})",
                );
            }
        }
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(179.0, 10.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Interior,
            },
        );
        for (point, expected) in [
            (Point::new_raw_xy(0.0, 5.0), ExactGeographicPointLocation {
                geometry: Loc::Exterior,
                area: Loc::Exterior,
            }),
            (
                Point::new_raw_xy(37.0, 80.0),
                ExactGeographicPointLocation {
                    geometry: Loc::Boundary,
                    area: Loc::Boundary,
                },
            ),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                expected,
                "off-seam source probe {point:?}",
            );
        }
        for longitude in [-120.0, 0.0, 37.0, 120.0] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 85.0))
                    .unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Interior,
                },
                "north-cap interior at longitude {longitude}",
            );
        }
        for (longitude, latitude, geometry, area) in [
            (0.0, 90.0, Loc::Interior, Loc::Interior),
            (-137.0, 90.0, Loc::Interior, Loc::Interior),
            (223.0, 90.0, Loc::Interior, Loc::Interior),
            (-137.0, -90.0, Loc::Exterior, Loc::Exterior),
        ] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, latitude))
                    .unwrap(),
                ExactGeographicPointLocation { geometry, area },
                "pole location at ({longitude}, {latitude})",
            );
        }
        for point in [
            Point::new_raw_xy(175.0, 6.0),
            Point::new_raw_xy(-175.0, 6.0),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Boundary,
                    area: Loc::Boundary,
                },
                "line endpoint on the source hole boundary {point:?}",
            );
        }
        for point in [
            Point::new_raw_xy(175.0, 3.0),
            Point::new_raw_xy(-175.0, 3.0),
            Point::new_raw_xy(-175.0, 8.0),
            Point::new_raw_xy(175.0, 8.0),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Boundary,
                    area: Loc::Boundary,
                },
                "source hole vertex {point:?}",
            );
        }
        let one_ulp = 1.0_f64.next_up();
        for point in [
            Point::new_raw_xy(one_ulp, 1.0),
            Point::new_raw_xy(1.0, one_ulp),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Exterior,
                    area: Loc::Exterior,
                },
                "one-ULP exterior probe {point:?}",
            );
        }
    }

    #[test]
    fn exact_point_query_resolves_source_nodes_and_crossings() {
        let planar = Shape::geometry_collection(vec![
            Shape::Polygon(Polygon::new(
                ring(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
                vec![],
            )),
            line(&[(-1.0, 1.0), (1.0, 1.0), (3.0, 1.0)]),
        ])
        .unwrap();
        let planar_topology = exact_topology(&planar);
        for (point, geometry, area) in [
            (Point::new_raw_xy(0.0, 1.0), Loc::Boundary, Loc::Boundary),
            (Point::new_raw_xy(1.0, 1.0), Loc::Interior, Loc::Interior),
            (Point::new_raw_xy(2.0, 1.0), Loc::Boundary, Loc::Boundary),
            (Point::new_raw_xy(-1.0, 1.0), Loc::Boundary, Loc::Exterior),
            (Point::new_raw_xy(-0.5, 1.0), Loc::Interior, Loc::Exterior),
            (Point::new_raw_xy(3.0, 1.0), Loc::Boundary, Loc::Exterior),
        ] {
            assert_eq!(
                planar_topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation { geometry, area },
                "planar source node at {point:?}",
            );
        }

        let geographic = Shape::geometry_collection(vec![
            Shape::Polygon(Polygon::new(
                ring(&[
                    (170.0, 0.0),
                    (-170.0, 0.0),
                    (-170.0, 10.0),
                    (170.0, 10.0),
                    (170.0, 0.0),
                ]),
                vec![],
            )),
            line(&[(175.0, 5.0), (180.0, 5.0), (-175.0, 5.0)]),
        ])
        .unwrap();
        let geographic_topology = exact_topology(&geographic);
        for (point, geometry, area) in [
            (Point::new_raw_xy(0.0, 5.0), Loc::Exterior, Loc::Exterior),
            (Point::new_raw_xy(170.0, 5.0), Loc::Boundary, Loc::Boundary),
            (Point::new_raw_xy(175.0, 5.0), Loc::Interior, Loc::Interior),
            (Point::new_raw_xy(180.0, 5.0), Loc::Interior, Loc::Interior),
            (Point::new_raw_xy(-175.0, 5.0), Loc::Interior, Loc::Interior),
            (Point::new_raw_xy(180.0, 0.0), Loc::Boundary, Loc::Boundary),
        ] {
            assert_eq!(
                geographic_topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation { geometry, area },
                "geographic source node at {point:?}",
            );
        }
    }

    #[test]
    fn exact_point_query_respects_holes_and_full_turn_fill() {
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
                .locate_point(Point::new_raw_xy(2.0, 5.0))
                .unwrap()
                .geometry,
            Loc::Interior
        );
        assert_eq!(
            donut_topology
                .locate_point(Point::new_raw_xy(5.0, 5.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
        assert_eq!(
            donut_topology
                .locate_point(Point::new_raw_xy(5.0, 3.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Boundary,
                area: Loc::Boundary,
            },
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
        let cap_topology = exact_topology(&cap);
        assert_eq!(
            cap_topology
                .locate_point(Point::new_raw_xy(0.0, 85.0))
                .unwrap()
                .geometry,
            Loc::Interior
        );
        for longitude in [-120.0, 120.0] {
            assert_eq!(
                cap_topology
                    .locate_point(Point::new_raw_xy(longitude, 85.0))
                    .unwrap()
                    .geometry,
                Loc::Interior,
            );
        }
        assert_eq!(
            cap_topology
                .locate_point(Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
        assert_eq!(
            cap_topology
                .locate_point(Point::new_raw_xy(123.0, 90.0))
                .unwrap()
                .geometry,
            Loc::Interior
        );
    }