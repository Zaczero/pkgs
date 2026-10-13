#[test]
fn meridian_recipe_uses_latitude_for_its_producer_parameter() -> Result<()> {
    let frame = GeographicFrame::crs84();
    let shape = crate::io::parse_wkt("LINESTRING ZM (0 0 1 10, 0 10 3 30)")?;
    let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(&shape))
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let topology = ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
    let plan = build_q_plan(&topology, OverlayOp::Union)?;
    let recipe = plan
        .fragment_recipes
        .as_slice()
        .iter()
        .find(|recipe| recipe.use_kind == super::super::emission::SelectedUse::Curve)
        .ok_or_else(|| GeometryErrorKind::Invalid("meridian recipe is missing".into()))?;
    if !recipe.exact_displacement.is_zero() {
        return Err(GeometryErrorKind::Invalid(
            "meridian recipe acquired a longitude displacement".into(),
        )
        .into());
    }
    let range = recipe.ranges.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("meridian recipe has no source range".into())
    })?;
    assert_eq!(range.producer_start, ExactRational::zero());
    assert_eq!(range.producer_end, ExactRational::one());

    let mut work = crate::geometry::Unmetered;
    let trace = plan
        .fragment_recipes
        .source_trace_fragment(recipe.id, &mut work)?;
    let requests = trace
        .rows
        .requests_at(0, &ExactRational::from_f64(0.5), &mut work)?;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].parameter(), &ExactRational::from_f64(0.5));
    Ok(())
}

#[test]
fn synthetic_cut_is_affine_and_source_vertices_split_recipes() -> Result<()> {
    let frame = GeographicFrame::crs84();
    let line = crate::io::parse_wkt(
        "LINESTRING ZM (-180 0 1 10, -60 0 2 20, 60 0 3 30, \
         120 10 4 40, 180 0 5 50)",
    )?;
    let point = crate::io::parse_wkt("POINT (-120 90)")?;
    let shapes = [line, point];
    let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let topology = ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
    let plan = build_q_plan(&topology, OverlayOp::Union)?;
    let directed_ports = |step: LineStep| -> Result<[QPortId; 2]> {
        let span = topology.q.spans.get(step.span).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q curve path names an unknown span".into())
        })?;
        Ok(match step.from_endpoint {
            SpanEndpoint::Start => span.ports,
            SpanEndpoint::End => [span.ports[1], span.ports[0]],
        })
    };
    let mut cut_join = None;
    for (line_index, line_path) in plan
        .line_paths
        .iter()
        .enumerate()
        .filter(|(_, line)| line.origin == QLineOrigin::Curve)
    {
        let (Some(first_step), Some(last_step)) = (
            line_path.path.first().copied(),
            line_path.path.last().copied(),
        ) else {
            continue;
        };
        let first_from = directed_ports(first_step)?[0];
        let last_to = directed_ports(last_step)?[1];
        let first_node = topology
            .q
            .ports
            .get(first_from.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("curve start port is unknown".into()))?
            .node;
        let last_node = topology
            .q
            .ports
            .get(last_to.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("curve end port is unknown".into()))?
            .node;
        let line_closed = first_node == last_node;
        for (step_index, &left) in line_path.path.iter().enumerate() {
            let right = match line_path.path.get(step_index + 1).copied() {
                Some(right) => right,
                None if line_closed => *line_path.path.first().ok_or_else(|| {
                    GeometryErrorKind::Invalid("closed curve path is empty".into())
                })?,
                None => continue,
            };
            let [left_from, left_to] = directed_ports(left)?;
            let [right_from, _] = directed_ports(right)?;
            let left_span =
                topology.q.spans.get(left.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("left Q span is unknown".into())
                })?;
            let right_span =
                topology.q.spans.get(right.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("right Q span is unknown".into())
                })?;
            let left_port =
                topology.q.ports.get(left_to.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("left cut port is unknown".into())
                })?;
            let right_port = topology.q.ports.get(right_from.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("right cut port is unknown".into())
            })?;
            if !left_port.cut || !right_port.cut || left_port.node != right_port.node {
                continue;
            }
            let left_owner = left_span.owners.get(left.owner).ok_or_else(|| {
                GeometryErrorKind::Invalid("left cut span has no source owner".into())
            })?;
            let right_owner = right_span.owners.get(right.owner).ok_or_else(|| {
                GeometryErrorKind::Invalid("right cut span has no source owner".into())
            })?;
            if left_owner.source_edge != right_owner.source_edge {
                continue;
            }
            let left_forward = (left_from == left_span.ports[0]) != left_owner.reversed;
            let right_forward = (right_from == right_span.ports[0]) != right_owner.reversed;
            let left_end = if left_forward {
                &left_owner.source_range.end
            } else {
                &left_owner.source_range.start
            };
            let right_start = if right_forward {
                &right_owner.source_range.start
            } else {
                &right_owner.source_range.end
            };
            if left_end != right_start {
                continue;
            }
            cut_join = Some((
                line_index,
                line_closed,
                left_owner.source_edge,
                left_end.clone(),
            ));
            break;
        }
        if cut_join.is_some() {
            break;
        }
    }
    let (line_index, line_closed, source_edge, source_parameter) =
        cut_join.ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "curve paths have no adjacent or closure synthetic cut ports".into(),
            )
        })?;

    let walk = plan
        .fragment_recipes
        .walks()
        .iter()
        .find(|walk| walk.owner == super::super::emission::RecipeWalkOwner::Line(line_index))
        .ok_or_else(|| GeometryErrorKind::Invalid("full-turn recipe walk is missing".into()))?;
    let mut cut_recipe = None;
    let mut recipe_ranges = Vec::new();
    for &recipe_id in &walk.fragments {
        let recipe = plan
            .fragment_recipes
            .as_slice()
            .get(recipe_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("line recipe is unknown".into()))?;
        let recipe_sources = recipe
            .ranges
            .iter()
            .map(|range| range.source_edge)
            .collect::<BTreeSet<_>>();
        if !recipe.ranges.is_empty() {
            assert_eq!(recipe_sources.len(), 1);
        }
        recipe_ranges.extend(recipe.ranges.iter().map(|range| {
            (
                range.source_edge,
                range.range.start.clone(),
                range.range.end.clone(),
                range.producer_start.clone(),
                range.producer_end.clone(),
            )
        }));
    }
    for index in 0..recipe_ranges.len() {
        let left = recipe_ranges
            .get(index)
            .ok_or_else(|| GeometryErrorKind::Invalid("cut recipe range is unknown".into()))?;
        let right = match recipe_ranges.get(index + 1) {
            Some(right) => right,
            None if line_closed => recipe_ranges.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("closed cut recipe has no range".into())
            })?,
            None => continue,
        };
        if left.0 == source_edge
            && right.0 == source_edge
            && left.2 == source_parameter
            && right.1 == source_parameter
        {
            cut_recipe = Some((left.3.clone(), left.4.clone(), right.4.clone()));
            break;
        }
    }
    let (start, middle, end) = cut_recipe.ok_or_else(|| {
        GeometryErrorKind::Invalid(
            "synthetic cut join did not retain adjacent source intervals".into(),
        )
    })?;
    assert_eq!(start, ExactRational::zero());
    assert_eq!(source_parameter, ExactRational::from_f64(0.5));
    assert_eq!(middle, ExactRational::from_f64(0.5));
    assert_eq!(end, ExactRational::one());
    Ok(())
}
