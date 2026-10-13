    #[test]
    fn canonical_coordinate_owner_preserves_all_nearest_and_pole_port_bits() -> Result<()> {
        let frame = GeographicFrame::crs84();
        for (name, shapes) in fixtures()? {
            let topology = topology(&shapes)?;
            let actual = QCoordinates::build(&topology)?;
            for node in &topology.q.nodes {
                let first_port_id =
                    *topology.q.node_ports.get(node.ports.start).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q node has no first port".into())
                    })?;
                let first_port = topology
                    .q
                    .ports
                    .get(first_port_id.0 as usize)
                    .ok_or_else(|| GeometryErrorKind::Invalid("Q first port is unknown".into()))?;
                let expected = XY::new(
                    round_canonical_longitude(frame, &first_port.point.x)?,
                    Latitude::try_new(frame, first_port.point.y.round()?)
                        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                        .get(),
                );
                let actual_xy = actual.xy(node.id)?;
                assert_eq!(
                    [actual_xy.x.to_bits(), actual_xy.y.to_bits()],
                    [expected.x.to_bits(), expected.y.to_bits()],
                    "{name}/node {:?}",
                    node.id
                );
                if node.pole.is_some() {
                    for &port_id in
                        topology
                            .q
                            .node_ports
                            .get(node.ports.clone())
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q pole ports are unknown".into())
                            })?
                    {
                        let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q pole port is unknown".into())
                        })?;
                        assert_eq!(
                            actual.pole_port_longitude[port_id.0 as usize]
                                .expect("every pole port has its longitude")
                                .to_bits(),
                            round_canonical_longitude(frame, &port.point.x)?.to_bits(),
                            "{name}/pole port {port_id:?}",
                        );
                    }
                }
            }
        }
        Ok(())
    }
    #[test]
    fn canonical_inexact_axes_match_the_previous_exact_port_alias_rule() -> Result<()> {
        let mut stored = 0_usize;
        let mut constructed_exact = 0_usize;
        let mut constructed_inexact = 0_usize;
        let mut cut = 0_usize;
        let mut pole = 0_usize;
        let mut pinned_latitude = 0_usize;

        for (name, shapes) in fixtures()? {
            let topology = topology(&shapes)?;
            let frame = topology.sources.frame;
            let coordinates = QCoordinates::build(&topology)?;
            let mut stored_nodes = std::collections::BTreeSet::new();
            for &port_id in topology
                .q
                .source_edge_ports
                .iter()
                .flatten()
                .chain(&topology.q.source_point_ports)
            {
                stored_nodes.insert(
                    topology
                        .q
                        .ports
                        .get(port_id.0 as usize)
                        .ok_or_else(|| GeometryErrorKind::Invalid("stored port is unknown".into()))?
                        .node,
                );
            }

            for node in &topology.q.nodes {
                let nearest = coordinates.xy(node.id)?;
                let old_axes = if node.pole.is_some() || matches!(&node.key, QNodeKey::Cut(_)) {
                    [false, false]
                } else {
                    let port_id = topology
                        .q
                        .node_ports
                        .get(node.ports.clone())
                        .and_then(|ports| {
                            ports.iter().copied().find(|port_id| {
                                topology
                                    .q
                                    .ports
                                    .get(port_id.0 as usize)
                                    .is_some_and(|port| !port.cut)
                            })
                        })
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("source Q node has no non-cut port".into())
                        })?;
                    let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source Q port is unknown".into())
                    })?;
                    let exact = ExactNodePosition {
                        longitude: nearest_exact_longitude_alias(
                            frame,
                            &port.point.x,
                            &ExactRational::from_f64(nearest.x),
                        )?,
                        latitude: port.point.y.clone(),
                    };
                    inexact_axes(&exact, nearest)?
                };
                let old_inexact = old_axes.into_iter().any(|axis| axis);
                let actual_inexact = coordinates.has_inexact_nodes(&topology, [node.id])?;
                assert_eq!(actual_inexact, old_inexact, "{name}/node {:?}", node.id);

                if node.pole.is_some() {
                    pole += 1;
                    assert!(!actual_inexact);
                } else if matches!(&node.key, QNodeKey::Cut(_)) {
                    cut += 1;
                    assert!(!actual_inexact);
                } else if stored_nodes.contains(&node.id) {
                    stored += 1;
                    assert!(!actual_inexact, "stored node moved");
                } else if old_inexact {
                    constructed_inexact += 1;
                    if name == "inexact_crossing_with_pinned_latitude" {
                        assert_eq!(old_axes, [true, false]);
                        pinned_latitude += 1;
                    }
                } else {
                    constructed_exact += 1;
                }
            }
        }

        assert!(stored > 0);
        assert!(constructed_exact > 0);
        assert!(constructed_inexact > 0);
        assert!(cut > 0);
        assert!(pole > 0);
        assert!(pinned_latitude > 0);
        Ok(())
    }
    #[test]
    fn canonical_node_table_matches_port_rounding_bits() -> Result<()> {
        let frame = GeographicFrame::crs84();
        for (name, shapes) in fixtures()? {
            let topology = topology(&shapes)?;
            let canonical = QCoordinates::build(&topology)?;
            for node in &topology.q.nodes {
                let port_id =
                    *topology.q.node_ports.get(node.ports.start).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q node has no first port".into())
                    })?;
                let port =
                    topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q first port is unknown".into())
                    })?;
                let old = XY::new(
                    round_canonical_longitude(frame, &port.point.x)?,
                    Latitude::try_new(frame, port.point.y.round()?)
                        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                        .get(),
                );
                let actual = canonical.xy(node.id)?;
                assert_eq!(
                    (actual.x.to_bits(), actual.y.to_bits()),
                    (old.x.to_bits(), old.y.to_bits()),
                    "{name}/QNodeId({})",
                    node.id.0
                );
                if node.pole.is_some() {
                    for &port_id in
                        topology
                            .q
                            .node_ports
                            .get(node.ports.clone())
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q pole ports are unknown".into())
                            })?
                    {
                        let pole_port = &topology.q.ports[port_id.0 as usize];
                        assert_eq!(
                            canonical.pole_port_longitude[port_id.0 as usize]
                                .expect("pole has a per-port longitude")
                                .to_bits(),
                            round_canonical_longitude(frame, &pole_port.point.x)?.to_bits(),
                            "{name}/QPortId({})",
                            port_id.0
                        );
                    }
                }
            }
        }
        Ok(())
    }
    #[test]
    fn q_domain_keeps_both_native_neighbors_and_orders_the_true_left_site() -> Result<()> {
        let frame = GeographicFrame::crs84();
        let half = frame.longitude().half_period();
        let ulp = 2.0_f64.powi(-45);
        let line = |start: Point, end: Point| -> Result<crate::geometry::Shape> {
            let coordinates = CoordSeq::try_from_points(&[start, end])?;
            Ok(crate::geometry::Shape::LineString(
                crate::geometry::LineSeq::from_proven(coordinates),
            ))
        };
        let shapes = [
            line(
                Point::new_raw_xy(half - ulp, 0.0),
                Point::new_raw_xy(half + ulp, 4.0),
            )?,
            line(
                Point::new_raw_xy(half - ulp, 2.0),
                Point::new_raw_xy(half + ulp, -2.0),
            )?,
            crate::geometry::Shape::Point(Point::new_raw_xy(half - ulp, 1.0)),
        ];
        let topology = topology(&shapes)?;
        let mut budget = EvaluationBudget::new("q native domain", 0)?;
        let plan =
            super::super::overlay::QEmissionPlan::build(&topology, OverlayOp::Union, &mut budget)?;
        let source_points = vec![None; topology.sources.points.len()];
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Union.name(),
            &mut budget,
        )?;
        let mut emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        // The line-only union is source-reusable; exercise Native completion explicitly.
        emission.force_native = true;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
            None;
        let problem = QEmbeddingProblem::new(
            &topology,
            &emission,
            &nearest,
            &ordinates,
            crate::geometry::CoordinateAxes::XY,
            false,
            "q native domain",
            metric_access,
            &mut budget,
        )?;

        let exact_longitude =
            ExactRational::from_f64(half).try_subtract(&ExactRational::from_f64(ulp / 2.0))?;
        let exact_latitude = ExactRational::one();
        let (target_index, _) = problem
            .exact_positions
            .iter()
            .enumerate()
            .find(|(_, exact)| {
                exact.as_ref().is_some_and(|exact| {
                    exact.longitude == exact_longitude && exact.latitude == exact_latitude
                })
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("the crossing has no constructed Q node".into())
            })?;
        let target = QNodeId(u32::try_from(target_index).map_err(|_| ResourceError::SizeOverflow)?);
        let nearest_target = nearest.candidate(target)?;
        assert_eq!(nearest_target.native.x, -half);
        assert_eq!(nearest_target.periods, 1);

        let left = QCandidate {
            native: XY::new(half - ulp, 1.0),
            periods: 0,
        };
        let right = QCandidate {
            native: XY::new(-half + ulp, 1.0),
            periods: 1,
        };
        let seam = QCandidate {
            native: XY::new(-half, 1.0),
            periods: 1,
        };
        let domain = problem
            .domains
            .get(target.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("crossing node has no domain".into()))?;
        assert!(domain.contains(&left));
        assert!(domain.contains(&right));
        assert!(!domain.contains(&seam));
        let mut compare_work = Unmetered;
        assert_eq!(
            problem.compare_candidates(
                QVariableNode::Source(target),
                left,
                right,
                &mut compare_work,
            )?,
            Ordering::Less,
            "native+lift distances are u²/4 on the left and 9u²/4 on the right",
        );

        let point_port =
            *topology.q.source_point_ports.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("fixed witness point has no port".into())
            })?;
        let fixed_node = topology
            .q
            .ports
            .get(point_port.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("fixed witness port is unknown".into()))?
            .node;
        let (class, _) = problem
            .node_location
            .get(target.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| GeometryErrorKind::Invalid("crossing node is not movable".into()))?;
        assert!(
            problem.classes.get(class).is_some_and(|variables| {
                !variables.contains(&QVariableNode::Source(fixed_node))
            })
        );
        let variables = problem
            .classes
            .get(class)
            .ok_or_else(|| GeometryErrorKind::Invalid("crossing class is unknown".into()))?;
        let mut fixed_work = Unmetered;
        let fixed_keys =
            <QEmbeddingProblem<'_, '_, '_, '_, crate::crs::GeodesicMetric> as Problem>::fixed_keys(
                &problem,
                class,
                variables,
                &mut fixed_work,
            )?;
        assert!(fixed_keys.contains(&emitted_node_key(frame, XY::new(half - ulp, 1.0),)?));

        let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
        let live_keys = drawing.live_node_keys(frame, &mut budget)?;
        let certification = QEmbeddingCertification::<crate::crs::GeodesicMetric> {
            topology: &topology,
            emission: &emission,
            nearest: &nearest,
            ordinates: &ordinates,
            declared_axes: crate::geometry::CoordinateAxes::XY,
            strict_axes: false,
            operation: "q native domain",
            metric_access: None,
        };
        let pass =
            match certify_and_search_q_embedding(&certification, &live_keys, drawing, &mut budget)?
            {
                SearchOutcome::Pass(pass) => pass,
                SearchOutcome::Exhausted => {
                    return Err(GeometryErrorKind::unrepresentable_output("q native domain"));
                },
            };
        assert!(
            pass.searched,
            "the nearest seam site is outside the admitted domain"
        );
        let assigned = pass.coordinates.candidate(target)?;
        assert_eq!(assigned.native.x.to_bits(), right.native.x.to_bits());
        assert_eq!(assigned.native.y.to_bits(), right.native.y.to_bits());
        assert_eq!(assigned.periods, right.periods);

        let super::super::emission::SealedQDrawing::Native(native) = pass.output.sealed else {
            return Err(GeometryErrorKind::Invalid(
                "the seam witness did not run Native completion".into(),
            )
            .into());
        };
        assert!(
            !native.line_traces.is_empty()
                && native
                    .line_traces
                    .iter()
                    .all(|line| line.trace.rows.edge_count() > 0),
            "Native completion must retain final producer leaf records"
        );
        let shape = native.sealed.into_scalar()?.0;
        let mut crossing_leaf_ends = 0;
        let mut fixed_records = Vec::new();
        shape.try_for_each_component(&mut |component| -> Result<()> {
            match component {
                crate::geometry::Component::LineString(line) => {
                    let points = line.points().collect::<Vec<_>>();
                    for pair in points.windows(2) {
                        crossing_leaf_ends += pair
                            .iter()
                            .filter(|point| {
                                point.x.to_bits() == assigned.native.x.to_bits()
                                    && point.y.to_bits() == assigned.native.y.to_bits()
                            })
                            .count();
                    }
                },
                crate::geometry::Component::Point(point) => fixed_records.push(point),
                crate::geometry::Component::Polygon(_) => {},
            }
            Ok(())
        })?;
        assert!(
            crossing_leaf_ends >= 4,
            "both crossing lines must retain their incident final leaves at the right site"
        );
        assert!(
            fixed_records.iter().any(|point| {
                point.x.to_bits() == (half - ulp).to_bits()
                    && point.y.to_bits() == 1.0_f64.to_bits()
            }),
            "the fixed left-site point must remain in the final Native part records"
        );
        Ok(())
    }
