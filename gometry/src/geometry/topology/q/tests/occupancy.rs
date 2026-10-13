

    #[test]
    fn ordinary_source_ring_has_two_exact_quotient_faces() {
        let shell = ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        assert_eq!(q_graph(&shape).1.global_face_count, 2);
    }

    #[test]
    fn seam_crossing_source_ring_has_two_exact_quotient_faces() {
        let shell = ring(&[
            (170.0, -10.0),
            (-170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, -10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (inside, outside) = source_shell_faces(&q, &sources, 0);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outside).is_empty());
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn horizontal_full_turn_ring_has_two_exact_quotient_faces() {
        let shell = ring(&[
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (north_cap, south_side) = source_shell_faces(&q, &sources, 0);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(north_cap), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(south_side).is_empty());
    }

    #[test]
    fn south_horizontal_full_turn_ring_keeps_its_summary_selected_cap() {
        let shell = ring(&[
            (0.0, -80.0),
            (90.0, -80.0),
            (180.0, -80.0),
            (-90.0, -80.0),
            (0.0, -80.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (north_side, south_cap) = source_shell_faces(&q, &sources, 0);
        assert_eq!(q.global_face_count, 2);
        assert!(q.face_rows.rows_for(north_side).is_empty());
        assert_eq!(q.face_rows.rows_for(south_cap), &[SourceRowId(0)]);
    }

    #[test]
    fn varying_latitude_full_turn_ring_changes_occupancy_at_source_edges() {
        let shell = ring(&[
            (0.0, 70.0),
            (90.0, 75.0),
            (180.0, 80.0),
            (-90.0, 75.0),
            (0.0, 70.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (north_side, south_side) = source_shell_faces(&q, &sources, 0);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(north_side), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(south_side).is_empty());
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn reversed_varying_latitude_full_turn_preserves_signed_winding_fill() {
        let shell = ring(&[
            (0.0, 70.0),
            (-90.0, 75.0),
            (180.0, 80.0),
            (90.0, 75.0),
            (0.0, 70.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (outside, inside) = source_shell_faces(&q, &sources, 0);
        assert_eq!(sources.rings.first().unwrap().summary.chart_turn, -1);
        assert_eq!(q.face_rows.rows_for(inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outside).is_empty());
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn varying_full_turn_crossing_the_equator_is_rejected_at_admission() {
        let shell = ring(&[
            (0.0, -10.0),
            (90.0, 10.0),
            (180.0, 20.0),
            (-90.0, 10.0),
            (0.0, -10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        assert_ambiguous_ring_admission(&shape, &[]);
    }

    #[test]
    fn reversed_ambiguous_ring_in_a_collection_is_rejected_at_admission() {
        let polygon = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, -10.0),
                (-90.0, 10.0),
                (180.0, 20.0),
                (90.0, 10.0),
                (0.0, -10.0),
            ]),
            vec![],
        ));
        let collection = Shape::geometry_collection(vec![
            polygon,
            Shape::Point(crate::geometry::Point::new_raw_xy(37.0, 90.0)),
        ])
        .unwrap();
        assert_ambiguous_ring_admission(&collection, &[0]);
    }

    #[test]
    fn multi_turn_ring_keeps_its_signed_source_transition_multiplicity() {
        let shell = ring(&[
            (0.0, 10.0),
            (120.0, 10.0),
            (240.0, 10.0),
            (360.0, 10.0),
            (480.0, 10.0),
            (600.0, 10.0),
            (720.0, 10.0),
            (0.0, 10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (north_cap, south_side) = source_shell_faces(&q, &sources, 0);
        assert_eq!(sources.rings.first().unwrap().summary.chart_turn, 2);
        assert!(q.spans.iter().any(|span| {
            let transitions = span
                .owners
                .iter()
                .filter_map(|owner| owner.ring_transition)
                .filter(|transition| transition.ring == SourceRingId(0))
                .collect::<Vec<_>>();
            transitions.len() >= 2 && transitions.iter().all(|transition| transition.delta == -1)
        }));
        assert_eq!(q.face_rows.rows_for(north_cap), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(south_side).is_empty());
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn negative_multi_turn_horizontal_ring_keeps_its_summary_selected_cap() {
        let shell = ring(&[
            (0.0, 10.0),
            (-120.0, 10.0),
            (-240.0, 10.0),
            (-360.0, 10.0),
            (-480.0, 10.0),
            (-600.0, 10.0),
            (-720.0, 10.0),
            (0.0, 10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (south_side, north_cap) = source_shell_faces(&q, &sources, 0);
        assert_eq!(sources.rings.first().unwrap().summary.chart_turn, -2);
        assert!(q.spans.iter().any(|span| {
            let transitions = span
                .owners
                .iter()
                .filter_map(|owner| owner.ring_transition)
                .filter(|transition| transition.ring == SourceRingId(0))
                .collect::<Vec<_>>();
            transitions.len() >= 2 && transitions.iter().all(|transition| transition.delta == 1)
        }));
        assert!(q.face_rows.rows_for(south_side).is_empty());
        assert_eq!(q.face_rows.rows_for(north_cap), &[SourceRowId(0)]);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn exact_full_longitude_axis_band_occupies_only_its_source_band() {
        let shell = ring(&[
            (-180.0, -10.0),
            (-90.0, -10.0),
            (0.0, -10.0),
            (90.0, -10.0),
            (180.0, -10.0),
            (180.0, 10.0),
            (90.0, 10.0),
            (0.0, 10.0),
            (-90.0, 10.0),
            (-180.0, 10.0),
            (-180.0, -10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (band_side, exterior_side) = source_shell_faces(&q, &sources, 0);
        assert!(sources.rings.first().unwrap().summary.exact_axis_band);
        assert_eq!(q.global_face_count, 3);
        assert_eq!(q.face_rows.rows_for(band_side), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(exterior_side).is_empty());
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);

        let topology = exact_topology(&shape);
        for longitude in [0.0, 179.0, 180.0] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 0.0))
                    .unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Interior,
                },
            );
        }
        assert_eq!(
            topology.locate_point(Point::new_raw_xy(0.0, 10.0)).unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Boundary,
                area: Loc::Boundary,
            },
        );
    }

    #[test]
    fn pole_to_pole_full_longitude_axis_band_occupies_the_sphere() {
        let world = Shape::Polygon(Polygon::new(
            ring(&[
                (-180.0, -90.0),
                (-90.0, -90.0),
                (0.0, -90.0),
                (90.0, -90.0),
                (180.0, -90.0),
                (180.0, 90.0),
                (90.0, 90.0),
                (0.0, 90.0),
                (-90.0, 90.0),
                (-180.0, 90.0),
                (-180.0, -90.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&world);
        assert!(topology.sources.rings[0].summary.exact_axis_band);
        for point in [
            crate::geometry::Point::new_raw_xy(0.0, 0.0),
            crate::geometry::Point::new_raw_xy(179.0, 0.0),
            crate::geometry::Point::new_raw_xy(-179.0, 0.0),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Interior,
                }
            );
        }
    }

    #[test]
    fn north_pole_ring_vertex_keeps_its_exact_cut_fan_wedge() {
        let shell = ring(&[
            (-120.0, 80.0),
            (0.0, 80.0),
            (120.0, 80.0),
            (0.0, 90.0),
            (-120.0, 80.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (inside, outside) = source_shell_faces(&q, &sources, 0);
        assert!(pole_fan_degree(&q, ChartPole::North) >= 2);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outside).is_empty());
        assert!(
            q.face_rows
                .rows_for(*q.cut_interval_faces.last().unwrap())
                .is_empty()
        );
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn admitted_exterior_pole_ulp_is_normalized_before_chart_noding() {
        let pole = 90.0_f64;
        let exterior_ulp = pole.next_up();
        let cap = Shape::Polygon(Polygon::new(
            ring(&[
                (-10.0, 80.0),
                (10.0, 80.0),
                (10.0, pole),
                (-10.0, pole),
                (-10.0, 80.0),
            ]),
            vec![],
        ));
        let admitted_cap = Shape::Polygon(Polygon::new(
            ring(&[
                (-10.0, 80.0),
                (10.0, 80.0),
                (10.0, exterior_ulp),
                (-10.0, exterior_ulp),
                (-10.0, 80.0),
            ]),
            vec![],
        ));
        let (sources, q) = q_graph_for_shapes(&[cap, admitted_cap]);

        assert!(sources.edges.iter().any(|edge| {
            edge.source
                .iter()
                .any(|point| point.y.to_bits() == exterior_ulp.to_bits())
        }));
        assert!(q.ports.iter().all(|port| {
            port.point
                .y
                .try_cmp(&ExactRational::from_f64(pole))
                .unwrap()
                .is_le()
        }));
    }

    #[test]
    fn south_pole_ring_vertex_keeps_its_exact_cut_fan_wedge() {
        let shell = ring(&[
            (-120.0, -80.0),
            (0.0, -80.0),
            (120.0, -80.0),
            (0.0, -90.0),
            (-120.0, -80.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (left, right) = source_shell_faces(&q, &sources, 0);
        assert!(pole_fan_degree(&q, ChartPole::South) >= 2);
        assert!(q.face_rows.rows_for(left).is_empty());
        assert_eq!(q.face_rows.rows_for(right), &[SourceRowId(0)]);
        assert!(
            q.face_rows
                .rows_for(*q.cut_interval_faces.first().unwrap())
                .is_empty()
        );
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn duplicate_polygon_members_aggregate_area_by_union() {
        let square = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            vec![],
        ));
        for copies in [2, 3] {
            let source = Shape::geometry_collection(vec![square.clone(); copies]).unwrap();
            let topology = exact_topology(&source);
            assert_eq!(
                topology.locate_point(Point::new_raw_xy(2.0, 2.0)).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Interior,
                },
            );
            assert_eq!(
                topology.locate_point(Point::new_raw_xy(2.0, 0.0)).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Boundary,
                    area: Loc::Boundary,
                },
            );
        }
    }

    #[test]
    fn polar_hole_excludes_its_cap_and_the_pole() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 70.0),
                (60.0, 70.0),
                (120.0, 70.0),
                (180.0, 70.0),
                (-120.0, 70.0),
                (-60.0, 70.0),
                (0.0, 70.0),
            ]),
            vec![ring(&[
                (0.0, 80.0),
                (60.0, 80.0),
                (120.0, 80.0),
                (180.0, 80.0),
                (-120.0, 80.0),
                (-60.0, 80.0),
                (0.0, 80.0),
            ])],
        ));
        let topology = exact_topology(&source);
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(0.0, 75.0))
                .unwrap()
                .geometry,
            Loc::Interior,
        );
        for point in [Point::new_raw_xy(0.0, 85.0), Point::new_raw_xy(0.0, 90.0)] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Exterior,
                    area: Loc::Exterior,
                },
            );
        }
    }

    #[test]
    fn native_grad_full_turn_ring_uses_its_admitted_pole() {
        let unit = AngularFactor::try_new("test grads", std::f64::consts::TAU / 400.0).unwrap();
        let frame = GeographicFrame::try_new(unit, unit, 0.0).unwrap();
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (-200.0, 80.0),
                (-100.0, 80.0),
                (0.0, 80.0),
                (100.0, 80.0),
                (200.0, 80.0),
                (-200.0, 80.0),
            ]),
            vec![],
        ));
        let view = ScalarGeographicView::try_new(frame, &source).expect("native grad cap admits");
        let topology = ExactGeographicTopology::from_views(frame, std::iter::once(view)).unwrap();
        assert_eq!(frame.pole(), 100.0);
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(0.0, frame.pole()))
                .unwrap()
                .geometry,
            Loc::Interior,
        );
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(0.0, 70.0))
                .unwrap()
                .geometry,
            Loc::Exterior,
        );
    }

    #[test]
    fn separate_nearby_components_do_not_fill_the_gap_between_them() {
        let left = Shape::Polygon(Polygon::new(
            ring(&[
                (-1.01e-7, -1e-9),
                (-9.9e-8, -1e-9),
                (-9.9e-8, 1e-9),
                (-1.01e-7, 1e-9),
                (-1.01e-7, -1e-9),
            ]),
            vec![],
        ));
        let right = Shape::Polygon(Polygon::new(
            ring(&[
                (9.9e-8, -1e-9),
                (1.01e-7, -1e-9),
                (1.01e-7, 1e-9),
                (9.9e-8, 1e-9),
                (9.9e-8, -1e-9),
            ]),
            vec![],
        ));
        let source = Shape::geometry_collection(vec![left, right]).unwrap();
        let topology = exact_topology(&source);
        assert_eq!(
            topology.locate_point(Point::new_raw_xy(0.0, 0.0)).unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Exterior,
                area: Loc::Exterior,
            },
        );
        for x in [-1.0e-7, 1.0e-7] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(x, 0.0))
                    .unwrap()
                    .geometry,
                Loc::Interior,
                "component interior at x={x}",
            );
        }
    }

    #[test]
    fn polar_hole_intersection_with_its_pole_cap_is_empty() {
        let polar_hole = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 70.0),
                (60.0, 70.0),
                (120.0, 70.0),
                (180.0, 70.0),
                (-120.0, 70.0),
                (-60.0, 70.0),
                (0.0, 70.0),
            ]),
            vec![ring(&[
                (0.0, 80.0),
                (60.0, 80.0),
                (120.0, 80.0),
                (180.0, 80.0),
                (-120.0, 80.0),
                (-60.0, 80.0),
                (0.0, 80.0),
            ])],
        ));
        let pole_cap = Shape::Polygon(Polygon::new(
            ring(&[(120.0, 90.0), (110.0, 85.0), (130.0, 85.0), (120.0, 90.0)]),
            Vec::new(),
        ));

        let intersection = crate::geometry::overlay_all(
            &[&polar_hole, &pole_cap],
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();

        assert!(intersection.is_empty(), "{intersection:?}");
    }