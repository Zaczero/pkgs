

    /// Independent exact ray-crossing winding of the lifted source ring at the
    /// north germ of `cut`; only Q's edge lift is shared with the seed.
    fn reference_north_germ_winding(
        sources: &ExactGeographicChartInventory,
        ring_index: usize,
        cut: &ExactRational,
    ) -> Option<i64> {
        use crate::geometry::numerics::product::exact_power_of_two;
        use crate::geographic::Latitude;
        let frame = sources.frame;
        let ring = &sources.rings[ring_index];
        let period = ExactRational::from_f64(frame.longitude().period());
        let edges = sources
            .edges
            .get(ring.source_edges.clone())
            .expect("ring source edges belong to the chart inventory");
        let latitude = |y: f64| ExactRational::from_f64(Latitude::from_admitted(frame, y).get());
        let edge = |edge: &super::super::exact::ExactSourceEdge| {
            super::super::exact::local_longitude_edge(
                frame,
                edge.source[0].x,
                edge.retained_longitude(),
            )
        };
        let mut x = ExactRational::from_f64(edge(&edges[0]).start_x);
        let mut lifted = vec![(x.clone(), latitude(edges[0].source[0].y))];
        for source_edge in edges {
            x = x
                .try_add(&ExactRational::from_dyadic(
                    edge(source_edge)
                        .delta_x(frame.longitude().period())
                        .unwrap(),
                ))
                .unwrap();
            lifted.push((x.clone(), latitude(source_edge.source[1].y)));
        }
        let net = lifted.last().unwrap().0.try_subtract(&lifted[0].0).unwrap();
        let turns = (-3_i64..=3).find(|&turn| {
            let multiple = period
                .try_product(&ExactRational::from_f64(turn as f64))
                .unwrap();
            net.try_cmp(&multiple).unwrap().is_eq()
        })?;
        if turns.abs() > 1 {
            return None;
        }
        if turns != 0 {
            let closure_latitude = match ring.summary.full_turn_pole()? {
                FullTurnPole::Bounded(pole) => frame.chart_pole_latitude(pole),
                FullTurnPole::Ambiguous => return None,
            };
            let end = lifted.last().unwrap().0.clone();
            let start = lifted[0].0.clone();
            lifted.push((end, ExactRational::from_f64(closure_latitude)));
            lifted.push((start, ExactRational::from_f64(closure_latitude)));
        }
        let germ_y = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North))
            .try_subtract(&ExactRational::from_f64(exact_power_of_two(-60)))
            .unwrap();
        let image_radius = i64::try_from(edges.len()).unwrap().checked_add(1).unwrap();
        let k_low = image_radius.checked_neg().unwrap();
        let k_high = image_radius;
        let mut total = 0_i64;
        for k in k_low..=k_high {
            let germ_x = cut
                .try_add(
                    &period
                        .try_product(&ExactRational::from_f64(k as f64))
                        .unwrap(),
                )
                .unwrap();
            for (a, b) in lifted.iter().zip(lifted.iter().cycle().skip(1)) {
                let a_below = a.1.try_cmp(&germ_y).unwrap().is_le();
                let b_below = b.1.try_cmp(&germ_y).unwrap().is_le();
                if a_below == b_below {
                    continue;
                }
                let first =
                    b.0.try_subtract(&a.0)
                        .unwrap()
                        .try_product(&germ_y.try_subtract(&a.1).unwrap())
                        .unwrap();
                let second = germ_x
                    .try_subtract(&a.0)
                    .unwrap()
                    .try_product(&b.1.try_subtract(&a.1).unwrap())
                    .unwrap();
                match (a_below, first.try_subtract(&second).unwrap().sign()) {
                    (true, ExactSign::Positive) => {
                        total = total.checked_add(1).expect("fixture winding fits i64");
                    },
                    (false, ExactSign::Negative) => {
                        total = total.checked_sub(1).expect("fixture winding fits i64");
                    },
                    _ => {},
                }
            }
        }
        Some(total)
    }

    fn assert_seed_matches_reference(frame: GeographicFrame, shape: &Shape) -> Vec<Option<i64>> {
        let (sources, q) = q_graph_in_frame(frame, shape);
        let period = frame.longitude().period();
        let mut cuts = vec![q.cut];
        cuts.extend(
            [17.25, -133.5, 0.5, 179.75, -179.25]
                .map(|degrees| ExactRational::from_f64(period * (degrees / 360.0))),
        );
        let mut compared = Vec::new();
        for cut in &cuts {
            let seeds = north_pole_ring_windings(&sources, cut).unwrap();
            for (index, &seed) in seeds.iter().enumerate() {
                if matches!(
                    sources.rings[index].summary.domain,
                    crate::geographic::RingDomain::SeamOnly
                ) {
                    continue;
                }
                let reference = reference_north_germ_winding(&sources, index, cut);
                if let Some(reference) = reference {
                    assert_eq!(seed, reference, "ring {index} cut {:?}", cut.round());
                }
                compared.push(reference);
            }
        }
        compared
    }

    fn explicit_period_fixtures() -> Vec<(&'static str, Vec<(f64, f64)>)> {
        let hop = vec![
            (45.0, 60.0),
            (135.0, 60.0),
            (225.0, 60.0),
            (315.0, 60.0),
            (405.0, 60.0),
            (405.0, 90.0),
            (45.0, 90.0),
            (45.0, 60.0),
        ];
        let stepped = vec![
            (45.0, 60.0),
            (135.0, 60.0),
            (225.0, 60.0),
            (315.0, 60.0),
            (405.0, 60.0),
            (405.0, 90.0),
            (315.0, 90.0),
            (225.0, 90.0),
            (135.0, 90.0),
            (45.0, 90.0),
            (45.0, 60.0),
        ];
        let oblique = vec![
            (0.0, 60.0),
            (90.0, 60.0),
            (180.0, 60.0),
            (270.0, 60.0),
            (360.0, 60.0),
            (450.0, 90.0),
            (360.0, 90.0),
            (270.0, 90.0),
            (180.0, 90.0),
            (90.0, 90.0),
            (0.0, 60.0),
        ];
        vec![
            ("circle plus bump", vec![
                (0.0, 60.0),
                (360.0, 60.0),
                (315.0, 65.0),
                (0.0, 60.0),
            ]),
            ("hop closure", hop),
            ("stepped closure", stepped),
            ("oblique closure", oblique),
            ("explicit full-period cap", vec![
                (0.0, 60.0),
                (360.0, 60.0),
                (360.0, 90.0),
                (0.0, 90.0),
                (0.0, 60.0),
            ]),
            ("explicit period pole edge, unmarked cap", vec![
                (0.0, 60.0),
                (90.0, 60.0),
                (180.0, 60.0),
                (270.0, 60.0),
                (0.0, 90.0),
                (360.0, 90.0),
                (0.0, 60.0),
            ]),
            ("south circle plus bump", vec![
                (0.0, -60.0),
                (360.0, -60.0),
                (315.0, -65.0),
                (0.0, -60.0),
            ]),
            ("reversed circle plus bump", vec![
                (0.0, 60.0),
                (315.0, 65.0),
                (360.0, 60.0),
                (0.0, 60.0),
            ]),
            ("rotated circle plus bump", vec![
                (90.0, 60.0),
                (450.0, 60.0),
                (405.0, 65.0),
                (90.0, 60.0),
            ]),
        ]
    }

    #[test]
    fn seed_equals_the_exact_lifted_winding_for_explicit_period_edges() {
        for (frame_name, frame) in native_frames() {
            for (name, points) in explicit_period_fixtures() {
                let shape = Shape::Polygon(Polygon::new(native_ring(frame, &points), Vec::new()));
                let compared = assert_seed_matches_reference(frame, &shape);
                assert!(
                    compared.iter().any(Option::is_some),
                    "{frame_name} {name}: the independent reference must cover a cut"
                );
            }
        }
    }

    fn exact_topology(shape: &Shape) -> ExactGeographicTopology {
        let frame = degrees();
        let view =
            crate::geographic::ScalarGeographicView::try_new(frame, shape).expect("fixture admits");
        ExactGeographicTopology::from_views(frame, std::iter::once(view)).unwrap()
    }

    fn assert_ambiguous_ring_admission(shape: &Shape, members: &[usize]) {
        let error = crate::geographic::ScalarGeographicView::try_new(degrees(), shape)
            .expect_err("an unmarked equator-straddling once-winding ring is ambiguous");
        assert_eq!(error.location.members.as_slice(), members);
        assert_eq!(error.location.ring, Some(0));
        assert_eq!(error.location.coordinate, 0);
        assert!(matches!(
            error.source,
            crate::geographic::GeographicError::AmbiguousFullTurnRing { .. }
        ));
    }

    fn multipolygon(polygons: Vec<Polygon>) -> Shape {
        Shape::MultiPolygon(
            MultiPolygon::try_new(
                CoordinateAxes::XY,
                polygons.into_iter().map(MultiMember::Value).collect(),
            )
            .unwrap(),
        )
    }

    fn nested_box_shapes(count: usize) -> Vec<Shape> {
        (1..=count)
            .rev()
            .map(|size| {
                let half = size as f64 / 100.0;
                Shape::Polygon(Polygon::new(
                    ring(&[
                        (-half, -half),
                        (half, -half),
                        (half, half),
                        (-half, half),
                        (-half, -half),
                    ]),
                    vec![],
                ))
            })
            .collect()
    }

    fn source_shell_faces(
        q: &ExactGeographicQ,
        sources: &ExactGeographicChartInventory,
        edge_index: usize,
    ) -> (GlobalFaceId, GlobalFaceId) {
        let edge = sources
            .edges
            .get(edge_index)
            .expect("the fixture source edge exists")
            .id;
        for (span_index, span) in q.spans.iter().enumerate() {
            if let Some(owner) = span.owners.iter().find(|owner| owner.source_edge == edge) {
                let [left, right] = q
                    .span_global_faces
                    .get(span_index)
                    .copied()
                    .flatten()
                    .expect("a shell source span has two incident Q faces");
                return if owner.reversed {
                    (right, left)
                } else {
                    (left, right)
                };
            }
        }
        panic!("the selected source edge has no Q span");
    }

    fn assert_source_transition_cycles(q: &ExactGeographicQ) {
        let Some(seed_face) = q.seed_face else {
            assert!(q.seed_winding.is_empty());
            return;
        };
        let seed = seed_face.0 as usize;
        let mut face_windings = vec![None::<Vec<i64>>; q.global_face_count];
        face_windings[seed] = Some(q.seed_winding.to_vec());
        let mut pending = vec![seed_face];
        while let Some(face) = pending.pop() {
            let face_index = face.0 as usize;
            let current = face_windings[face_index]
                .as_ref()
                .expect("every queued face has a winding")
                .clone();
            for (span_index, sides) in q.span_global_faces.iter().enumerate() {
                let Some([left, right]) = sides else {
                    continue;
                };
                let span = &q.spans[span_index];
                if left == right {
                    assert!(!span_has_ring_transition(span).unwrap());
                    continue;
                }
                let (neighbor, multiplier) = if *left == face {
                    (*right, 1)
                } else if *right == face {
                    (*left, -1)
                } else {
                    continue;
                };
                let mut expected = current.clone();
                for_each_span_ring_transition(&span.owners, &mut |transition| {
                    let winding = expected
                        .get_mut(transition.ring.0 as usize)
                        .expect("source ring IDs index the seed winding");
                    let delta = transition
                        .delta
                        .checked_mul(multiplier)
                        .expect("test transition multiplication cannot overflow");
                    *winding = winding
                        .checked_add(delta)
                        .expect("test winding stays within the source edge count");
                    Ok(())
                })
                .unwrap();
                let neighbor_index = neighbor.0 as usize;
                if let Some(previous) = &face_windings[neighbor_index] {
                    assert_eq!(
                        previous, &expected,
                        "inconsistent source ring transition cycle"
                    );
                } else {
                    face_windings[neighbor_index] = Some(expected);
                    pending.push(neighbor);
                }
            }
        }
        assert!(face_windings.iter().all(Option::is_some));
    }

    fn assert_global_cut_event_wedges(q: &ExactGeographicQ) {
        assert_eq!(
            q.cut_interval_faces.len(),
            q.cut_index
                .events
                .len()
                .checked_add(1)
                .expect("test event count fits a CSR offset")
        );
        for (event, interval_pair) in q
            .cut_index
            .events
            .iter()
            .zip(q.cut_interval_faces.windows(2))
        {
            let [south_interval, north_interval] = interval_pair else {
                unreachable!("windows(2) returns two cut intervals");
            };
            assert_eq!(
                q.global_face_of_local
                    .get(event.south_face.0 as usize)
                    .copied()
                    .expect("cut event south face has a global ID"),
                *south_interval
            );
            assert_eq!(
                q.global_face_of_local
                    .get(event.north_face.0 as usize)
                    .copied()
                    .expect("cut event north face has a global ID"),
                *north_interval
            );
            assert!(
                q.nodes
                    .get(event.node.0 as usize)
                    .expect("cut event has a Q node")
                    .ports
                    .len()
                    >= 2
            );
        }
    }

    fn pole_fan_degree(q: &ExactGeographicQ, pole: ChartPole) -> usize {
        let node = q
            .nodes
            .iter()
            .find(|node| node.pole == Some(pole))
            .expect("the source ring has a quotient pole node");
        let Some([start, end]) = q.outgoing_offsets.windows(2).nth(node.id.0 as usize) else {
            unreachable!("pole node has an outgoing fan offset pair");
        };
        end.checked_sub(*start)
            .expect("outgoing fan offsets are monotone")
    }

    #[test]
    fn isolated_source_points_resolve_hosts_in_the_global_face_table() {
        let polygon = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![],
        ));
        let collection = Shape::geometry_collection(vec![
            polygon,
            Shape::Point(crate::geometry::Point::new_raw_xy(5.0, 5.0)),
            Shape::Point(crate::geometry::Point::new_raw_xy(20.0, 20.0)),
        ])
        .unwrap();
        let (_, q) = q_graph(&collection);
        let inside = q
            .source_point_faces
            .first()
            .copied()
            .flatten()
            .expect("the interior point has an exact host face");
        let outside = q
            .source_point_faces
            .get(1)
            .copied()
            .flatten()
            .expect("the exterior point has an exact host face");
        assert_eq!(q.face_rows.rows_for(inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outside).is_empty());
        assert_ne!(inside, outside);
    }

    #[test]
    fn isolated_collapsed_ring_node_resolves_a_global_host_without_point_sources() {
        let outer = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![],
        ));
        let collapsed = Shape::Polygon(Polygon::new(
            ring(&[(5.0, 5.0), (5.0, 5.0), (5.0, 5.0), (5.0, 5.0)]),
            vec![],
        ));
        let (sources, q) = q_graph_for_shapes(&[outer, collapsed]);
        assert!(sources.points.is_empty());
        let edge = sources
            .edges
            .iter()
            .find(|edge| {
                matches!(edge.kind, SourceEdgeKind::Ring {
                    polygon,
                    ..
                } if polygon.0 == 1)
            })
            .expect("the collapsed polygon has source ring edges");
        let port_id = q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .and_then(|ports| ports.first().copied())
            .expect("the collapsed ring edge has source chart ports");
        let node = q
            .ports
            .get(port_id.0 as usize)
            .expect("the collapsed ring edge has a Q port")
            .node;
        let host = q
            .source_node_faces
            .get(node.0 as usize)
            .copied()
            .flatten()
            .expect("the isolated collapsed ring node has an exact host");
        assert_eq!(q.face_rows.rows_for(host), &[SourceRowId(0)]);
    }

    #[test]
    fn isolated_north_pole_point_uses_its_quotient_cut_host_face() {
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
        let collection = Shape::geometry_collection(vec![
            cap,
            Shape::Point(crate::geometry::Point::new_raw_xy(37.0, 90.0)),
        ])
        .unwrap();
        let (_, q) = q_graph(&collection);
        let north = q
            .source_point_faces
            .first()
            .copied()
            .flatten()
            .expect("the isolated pole point has an exact cut-interval host");
        assert_eq!(q.face_rows.rows_for(north), &[SourceRowId(0)]);
    }

    #[test]
    fn line_only_q_hosts_an_isolated_point_in_its_single_global_face() {
        let line = Shape::LineString(
            crate::geometry::LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    crate::geometry::Point::new_raw_xy(0.0, 0.0),
                    crate::geometry::Point::new_raw_xy(10.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let collection = Shape::geometry_collection(vec![
            line,
            Shape::Point(crate::geometry::Point::new_raw_xy(20.0, 5.0)),
        ])
        .unwrap();
        let (_, q) = q_graph(&collection);
        assert_eq!(q.global_face_count, 1);
        assert_eq!(q.face_rows.count(GlobalFaceId(0)), 0);
        assert!(q.face_rows.intervals.is_empty());
        assert_eq!(q.source_point_faces.as_ref(), &[Some(GlobalFaceId(0))]);
    }

    #[test]
    fn nested_disconnected_shells_link_the_inner_exterior_to_the_host_face() {
        let outer = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![],
        ));
        let inner = Shape::Polygon(Polygon::new(
            ring(&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]),
            vec![],
        ));
        let collection = Shape::geometry_collection(vec![outer, inner]).unwrap();
        let (sources, q) = q_graph(&collection);
        let (outer_inside, outer_outside) = source_shell_faces(&q, &sources, 0);
        let (inner_inside, inner_outside) = source_shell_faces(&q, &sources, 4);
        assert_eq!(q.global_face_count, 3);
        assert_eq!(inner_outside, outer_inside);
        assert_ne!(inner_inside, inner_outside);
        assert_ne!(outer_outside, outer_inside);
        assert_eq!(q.face_rows.rows_for(outer_inside), &[SourceRowId(0)]);
        assert_eq!(q.face_rows.rows_for(inner_inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outer_outside).is_empty());
    }

    #[test]
    fn disjoint_shell_exteriors_share_one_global_face() {
        let rectangle = |west: f64| {
            Shape::Polygon(Polygon::new(
                ring(&[
                    (west, 0.0),
                    (west + 4.0, 0.0),
                    (west + 4.0, 4.0),
                    (west, 4.0),
                    (west, 0.0),
                ]),
                vec![],
            ))
        };
        let collection = Shape::geometry_collection(vec![rectangle(0.0), rectangle(20.0)]).unwrap();
        let (sources, q) = q_graph(&collection);
        let (first_inside, first_outside) = source_shell_faces(&q, &sources, 0);
        let (second_inside, second_outside) = source_shell_faces(&q, &sources, 4);
        assert_eq!(q.global_face_count, 3);
        assert_eq!(first_outside, second_outside);
        assert_ne!(first_inside, first_outside);
        assert_ne!(second_inside, second_outside);
        assert_ne!(first_inside, second_inside);
        assert_eq!(q.face_rows.rows_for(first_inside), &[SourceRowId(0)]);
        assert_eq!(q.face_rows.rows_for(second_inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(first_outside).is_empty());
    }

    #[test]
    fn nested_shell_hole_and_disconnected_island_publish_exact_row_occupancy() {
        let annulus = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (3.0, 3.0),
                (3.0, 7.0),
                (7.0, 7.0),
                (7.0, 3.0),
                (3.0, 3.0),
            ])],
        ));
        let island = Shape::Polygon(Polygon::new(
            ring(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)]),
            vec![],
        ));
        let (sources, q) = q_graph_for_shapes(&[annulus, island]);
        let (shell_left, shell_right) = source_shell_faces(&q, &sources, 0);
        let (hole_left, hole_right) = source_shell_faces(&q, &sources, 4);
        let (island_left, island_right) = source_shell_faces(&q, &sources, 8);
        assert_eq!(q.global_face_count, 4);
        assert_eq!(q.face_rows.rows_for(shell_left), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(shell_right).is_empty());
        assert_eq!(q.face_rows.rows_for(hole_left), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(hole_right).is_empty());
        assert_eq!(q.face_rows.rows_for(island_left), &[SourceRowId(1)]);
        assert!(q.face_rows.rows_for(island_right).is_empty());
        assert_eq!(hole_right, island_right);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn nested_component_host_sweep_and_row_work_scale_with_the_graph() {
        let small_shapes = nested_box_shapes(64);
        let (_, small) = q_graph_for_shapes(&small_shapes);
        let large_shapes = nested_box_shapes(256);
        let (_, large) = q_graph_for_shapes(&large_shapes);
        assert_eq!(small.global_face_count, 65);
        assert_eq!(large.global_face_count, 257);
        assert_eq!(large.spans.len(), 1_024);
        assert_eq!(small.face_rows.intervals.len(), 64);
        assert_eq!(large.face_rows.intervals.len(), 256);
        for face in 0..large.global_face_count {
            let face = GlobalFaceId(u32::try_from(face).unwrap());
            let depth = large.face_rows.count(face);
            for row in 0..256 {
                assert_eq!(
                    large
                        .face_rows
                        .contains(face, SourceRowId(u32::try_from(row).unwrap())),
                    row < depth,
                );
            }
        }
        assert!(large.host_status_max_height <= 22);
        assert!(large.host_status_comparisons < 256 * 1_024);
        assert!(large.host_status_comparisons < small.host_status_comparisons * 6);
    }

    fn box_polygon(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon {
        Polygon::new(
            ring(&[(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]),
            Vec::new(),
        )
    }

    fn exact_topology_rows(shapes: &[Shape]) -> ExactGeographicTopology {
        let frame = degrees();
        ExactGeographicTopology::from_views(
            frame,
            shapes
                .iter()
                .map(|shape| ScalarGeographicView::try_new(frame, shape).expect("fixture admits")),
        )
        .unwrap()
    }

    fn polygon_count(shape: &Shape) -> usize {
        let wkt = crate::io::to_wkt(shape).unwrap();
        if wkt.starts_with("MULTIPOLYGON") {
            wkt.matches(")), ((")
                .count()
                .checked_add(1)
                .expect("WKT polygon count fits usize")
        } else {
            usize::from(wkt.starts_with("POLYGON ("))
        }
    }

    fn quotient_union_polygons(topology: &ExactGeographicTopology) -> usize {
        polygon_count(
            &exact_geographic_overlay::<crate::crs::GeodesicMetric>(
                topology,
                OverlayOp::Union,
                CoordinateAxes::XY,
                false,
                None,
            )
            .unwrap(),
        )
    }

    fn planar_union_polygons(polygons: &[Polygon]) -> usize {
        let shapes = polygons
            .iter()
            .cloned()
            .map(Shape::Polygon)
            .collect::<Vec<_>>();
        polygon_count(
            &overlay_all(
                &shapes,
                crate::geometry::TopologyDomain::Cartesian,
                OverlayOp::Union,
                Strictness::Lenient,
            )
            .unwrap(),
        )
    }

    fn location(topology: &ExactGeographicTopology, x: f64, y: f64) -> (Loc, Loc) {
        let at = topology
            .locate_point(crate::geometry::Point::new_raw_xy(x, y))
            .unwrap();
        (at.geometry, at.area)
    }

    fn relation(rows: &[Shape]) -> String {
        String::from_utf8(exact_topology_rows(rows).relate_pair().unwrap().0.to_vec()).unwrap()
    }

    /// A westward host ray that ends on a node holding an exact east dart must still place
    /// the component in the face east of the span it hit, not in the face CCW of that dart.
    #[test]
    fn host_ray_ending_at_an_east_dart_node_keeps_the_component_in_the_outer_face() {
        let seam_box = || box_polygon(170.0, 20.0, 190.0, 40.0);
        let step = |points: &[(f64, f64)]| {
            let mut closed = points.to_vec();
            closed.push(points[0]);
            Polygon::new(ring(&closed), Vec::new())
        };
        let pentagon = step(&[
            (0.0, 10.0),
            (20.0, 10.0),
            (20.0, 20.0),
            (40.0, 20.0),
            (0.0, 30.0),
        ]);
        let step_ring = step(&[
            (0.0, 10.0),
            (20.0, 10.0),
            (20.0, 20.0),
            (40.0, 20.0),
            (40.0, 30.0),
            (0.0, 30.0),
        ]);
        let corner_touching = [
            box_polygon(20.0, 20.0, 40.0, 30.0),
            box_polygon(0.0, 10.0, 20.0, 20.0),
        ];
        for (name, members, faces, union_polygons) in [
            ("pentagon", vec![pentagon], 3_usize, 2_usize),
            ("step ring", vec![step_ring], 3, 2),
            ("corner-touching boxes", corner_touching.to_vec(), 4, 3),
        ] {
            let mut polygons = members;
            polygons.push(seam_box());
            let topology = exact_topology(&multipolygon(polygons));
            assert_eq!(
                topology.q.global_face_count, faces,
                "{name}: one face per interior plus one outer face"
            );
            assert!(
                topology
                    .q
                    .component_hosts
                    .windows(2)
                    .all(|pair| pair.first() == pair.last()),
                "{name}: every component's outer face is the one global outer face"
            );
            assert_eq!(
                location(&topology, 170.0, 30.0),
                (Loc::Boundary, Loc::Boundary),
                "{name}"
            );
            assert_eq!(
                location(&topology, 175.0, 30.0),
                (Loc::Interior, Loc::Interior),
                "{name}"
            );
            assert_eq!(
                location(&topology, 165.0, 30.0),
                (Loc::Exterior, Loc::Exterior),
                "{name}"
            );
            assert_eq!(quotient_union_polygons(&topology), union_polygons, "{name}");
        }
    }

    /// The same ray ends on a node without an east dart, or misses every node: unchanged.
    #[test]
    fn host_ray_ending_at_a_node_without_an_east_dart_or_missing_it_stays_correct() {
        for seam_south in [20.0, 19.0] {
            let topology = exact_topology(&multipolygon(vec![
                box_polygon(0.0, 10.0, 20.0, 20.0),
                box_polygon(170.0, seam_south, 190.0, seam_south + 20.0),
            ]));
            assert_eq!(topology.q.global_face_count, 3);
            assert_eq!(
                location(&topology, 170.0, seam_south + 10.0),
                (Loc::Boundary, Loc::Boundary)
            );
            assert_eq!(quotient_union_polygons(&topology), 2);
        }
    }

    /// Row occupancy follows the faces: a third component hosted beside a shared edge or an
    /// overlap must not inherit the neighbouring polygon's interior.
    #[test]
    fn host_row_occupancy_does_not_leak_from_a_neighbouring_polygon_interior() {
        let a = box_polygon(20.0, 20.0, 40.0, 40.0);
        let d = box_polygon(100.0, 20.0, 120.0, 40.0);
        for (name, b) in [
            ("shared edge", box_polygon(10.0, 0.0, 30.0, 20.0)),
            ("overlap", box_polygon(10.0, 0.0, 30.0, 25.0)),
        ] {
            let members = multipolygon(vec![a.clone(), b, d.clone()]);
            for shift in [0.0, 360.0] {
                let outside = Shape::Polygon(box_polygon(130.0 + shift, 25.0, 140.0 + shift, 35.0));
                assert_eq!(
                    relation(&[members.clone(), outside]),
                    "FF2FF1212",
                    "{name} shift {shift}: a polygon over empty background is not occupied"
                );
            }
            let inside_d = Shape::Polygon(box_polygon(105.0, 25.0, 115.0, 35.0));
            assert_eq!(relation(&[members, inside_d]), "212FF1FF2", "{name}");
        }
    }

    /// An isolated point queries upward; a hit node with an east dart is already correct there.
    #[test]
    fn host_ray_for_an_isolated_point_ending_at_an_east_dart_node_is_outside() {
        let members = multipolygon(vec![
            box_polygon(20.0, 20.0, 40.0, 30.0),
            box_polygon(40.0, 10.0, 60.0, 20.0),
        ]);
        let point = Shape::Point(crate::geometry::Point::new_raw_xy(100.0, 20.0));
        assert_eq!(relation(&[members, point]), "FF2FF10F2");
    }

    /// The quotient route agrees with the planar partition on boxes that never touch the seam.
    #[test]
    fn host_quotient_route_matches_the_planar_partition_without_a_seam() {
        let pentagon = Polygon::new(
            ring(&[
                (0.0, 10.0),
                (20.0, 10.0),
                (20.0, 20.0),
                (40.0, 20.0),
                (0.0, 30.0),
                (0.0, 10.0),
            ]),
            Vec::new(),
        );
        let ordinary = box_polygon(100.0, 20.0, 120.0, 40.0);
        let mut fixtures = vec![vec![pentagon, ordinary]];
        for (offset_x, offset_y) in [(5.0, 10.0), (10.0, 10.0), (20.0, 10.0)] {
            fixtures.push(vec![
                box_polygon(0.0, -10.0, 20.0, 10.0),
                box_polygon(offset_x, offset_y - 10.0, offset_x + 20.0, offset_y + 10.0),
                box_polygon(100.0, 0.0, 120.0, 20.0),
            ]);
        }
        for polygons in fixtures {
            let topology = exact_topology(&multipolygon(polygons.clone()));
            assert_eq!(
                quotient_union_polygons(&topology),
                planar_union_polygons(&polygons),
                "{polygons:?}"
            );
            assert!(
                topology
                    .q
                    .component_hosts
                    .windows(2)
                    .all(|pair| pair.first() == pair.last()),
                "{polygons:?}: components share one outer face"
            );
        }
    }