
    #[test]
    fn full_source_line_walk_proves_literal_reuse() -> Result<()> {
        let line = crate::io::parse_wkt("LINESTRING Z (0 0 1, 1 1 2)")?;
        let topology = topology(std::slice::from_ref(&line))?;
        let mut work = Unmetered;
        let plan = QEmissionPlan::build(&topology, OverlayOp::Union, &mut work)?;
        let coordinates = QCoordinates::build_with_work(&topology, &mut work)?;
        let source_points = vec![None; topology.q.nodes.len()];
        let drawing = plan.drawing(&topology, &coordinates, &source_points, &mut work)?;
        let DrawnPolygons::Regions { rings, .. } = &drawing.polygons else {
            return Err(GeometryErrorKind::Invalid(
                "line-only source reuse unexpectedly selected a whole sphere".into(),
            )
            .into());
        };
        assert!(rings.is_empty());
        let drawn = drawing.lines.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("source line reuse drawing is missing".into())
        })?;
        let walk = plan
            .fragment_recipes
            .walks()
            .iter()
            .find(|walk| walk.owner == RecipeWalkOwner::Line(0))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source line reuse recipe walk is missing".into())
            })?;
        assert!(source_line_reuse_is_complete(
            &topology,
            plan.fragment_recipes.as_slice(),
            None,
            walk,
            &drawn.path,
            &mut work,
        )?);
        Ok(())
    }
    #[test]
    fn reversed_source_shell_can_be_reused_as_a_selected_hole() -> Result<()> {
        let source = crate::io::parse_wkt("POLYGON ((170 40, -170 40, -170 50, 170 50, 170 40))")?;
        let cutter = crate::io::parse_wkt("POLYGON ((172 42, 178 42, 178 48, 172 48, 172 42))")?;
        let topology = topology(&[source, cutter])?;
        let operation = OverlayOp::Difference;
        let mut budget = EvaluationBudget::new(operation.name(), 0)?;
        let plan = QEmissionPlan::build(&topology, operation, &mut budget)?;
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication =
            plan.fragment_recipes
                .publish(&topology, &nearest, operation.name(), &mut budget)?;
        let source_points = vec![None; topology.q.nodes.len()];
        let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
        let ring_roles = match check_q_ring_roles(&topology, &drawing, &mut budget)? {
            QRingRoleCheck::Pass(roles) => roles,
            QRingRoleCheck::NativeRequired(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "source reuse control unexpectedly needs Native region assembly".into(),
                )
                .into());
            },
            QRingRoleCheck::Reject { .. } => {
                return Err(GeometryErrorKind::Invalid(
                    "difference source rings did not retain their selected roles".into(),
                )
                .into());
            },
        };
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let problem: QEmbeddingProblem<'_, '_, '_, '_, crate::crs::GeodesicMetric> =
            QEmbeddingProblem::new(
                &topology,
                &emission,
                &nearest,
                &ordinates,
                crate::geometry::CoordinateAxes::XY,
                false,
                operation.name(),
                None,
                &mut budget,
            )?;
        let publication_coordinates = vec![None; problem.publication_nodes.nodes().len()];
        let reused = problem.source_reused_recipe_ids(
            &drawing,
            &ring_roles,
            &nearest,
            &publication_coordinates,
            &mut budget,
        )?;

        let source_polygon = topology.sources.polygons.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("difference source polygon is missing".into())
        })?;
        let cutter_polygon = topology.sources.polygons.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("difference cutter polygon is missing".into())
        })?;
        let source_ring = source_polygon.rings.start;
        let cutter_ring = cutter_polygon.rings.start;
        let DrawnPolygons::Regions { rings, .. } = &drawing.polygons else {
            return Err(GeometryErrorKind::Invalid(
                "difference unexpectedly selected a whole sphere".into(),
            )
            .into());
        };
        let planned_rings = match &plan.polygons {
            PolygonPlan::Regions { rings, .. } => rings,
            PolygonPlan::WholeSphere(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "difference unexpectedly planned a whole sphere".into(),
                )
                .into());
            },
        };
        let (cutter_index, reverse) = rings
            .iter()
            .enumerate()
            .find_map(|(index, ring)| {
                let RecipeWalkOwner::SourceRing(ring_id, _) = ring.recipe_walk else {
                    return None;
                };
                let Some(PlannedAreaRing::Source {
                    ring: planned_ring,
                    reverse,
                    ..
                }) = planned_rings.get(index).copied()
                else {
                    return None;
                };
                (ring_id == cutter_ring && planned_ring == cutter_ring).then_some((index, reverse))
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("difference did not reuse the cutter shell".into())
            })?;
        let source_ring_row = topology
            .sources
            .rings
            .get(source_ring)
            .ok_or_else(|| GeometryErrorKind::Invalid("source shell row is missing".into()))?;
        let cutter_ring_row = topology
            .sources
            .rings
            .get(cutter_ring)
            .ok_or_else(|| GeometryErrorKind::Invalid("cutter shell row is missing".into()))?;
        assert!(!source_ring_row.hole);
        assert!(!cutter_ring_row.hole);
        assert!(ring_roles[cutter_index]);
        assert!(reverse);
        let source_walks = plan.fragment_recipes.walks();
        let source_recipe_ids = source_walks
            .iter()
            .find(|walk| matches!(walk.owner, RecipeWalkOwner::SourceRing(ring, _) if ring == source_ring))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("difference source shell recipe is missing".into())
            })?;
        let cutter_recipe_ids = source_walks
            .iter()
            .find(|walk| matches!(walk.owner, RecipeWalkOwner::SourceRing(ring, _) if ring == cutter_ring))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("difference cutter shell recipe is missing".into())
            })?;
        assert!(
            source_recipe_ids
                .fragments
                .iter()
                .all(|recipe| reused.contains(recipe)),
            "the unchanged source shell keeps its raw reuse path"
        );
        assert!(
            cutter_recipe_ids
                .fragments
                .iter()
                .all(|recipe| reused.contains(recipe)),
            "the reversed cutter shell keeps its certified planned reuse path"
        );

        let mut wrong_roles = ring_roles;
        wrong_roles[cutter_index] = false;
        assert!(
            problem
                .source_reused_recipe_ids(
                    &drawing,
                    &wrong_roles,
                    &nearest,
                    &publication_coordinates,
                    &mut budget,
                )
                .is_err(),
            "a selected shell role without its planned traversal reversal must reject"
        );
        Ok(())
    }
    #[test]
    fn native_demand_counts_only_strict_interior_bank_events() -> Result<()> {
        let axis = GeographicFrame::crs84().longitude();
        let exact = |value| ExactRational::from_f64(value);
        let mut budget = EvaluationBudget::new("Native demand", 0)?;
        for (start, end, expected) in [
            (170.0, 180.0, false),
            (180.0, 190.0, false),
            (170.0, 190.0, true),
            (190.0, 170.0, true),
            (-180.0, -170.0, false),
            (-190.0, -180.0, false),
            (-190.0, -170.0, true),
            (539.0, 541.0, true),
            (540.0, 550.0, false),
        ] {
            assert_eq!(
                strict_native_bank_event(axis, &exact(start), &exact(end), &mut budget)?,
                expected,
                "unexpected strict-bank result for {start} -> {end}",
            );
        }
        Ok(())
    }
