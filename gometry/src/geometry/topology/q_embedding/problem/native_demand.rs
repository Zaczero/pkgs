use super::super::*;

impl<'topology, 'emission, 'access, 'metric, M: ChartMetric>
    QEmbeddingProblem<'topology, 'emission, 'access, 'metric, M>
{
    fn source_reused_recipe_ids(
        &self,
        drawing: &QEmissionDrawing<'topology>,
        ring_roles: &[bool],
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<BTreeSet<FragmentRecipeId>> {
        let walks = super::super::super::emission::index_recipe_walks(
            self.emission.fragment_walks,
            budget,
        )?;
        let mut cursors = BTreeMap::new();
        let mut reused = BTreeSet::new();
        match &drawing.polygons {
            DrawnPolygons::WholeSphere(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        budget.charge_into_error(1)?;
                        let walk = super::super::super::emission::next_recipe_walk(
                            &walks,
                            &mut cursors,
                            ring.recipe_walk,
                            budget,
                        )?;
                        let RecipeWalkOwner::SourceRing(ring_id) = ring.recipe_walk else {
                            return Err(GeometryErrorKind::Invalid(
                                "whole-sphere source polygon has a non-source recipe owner".into(),
                            )
                            .into());
                        };
                        let source_ring =
                            self.topology.sources.rings.get(ring_id).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "whole-sphere source reuse ring is unknown".into(),
                                )
                            })?;
                        if source_ring.hole != ring.hole {
                            return Err(GeometryErrorKind::Invalid(
                                "whole-sphere source reuse changed its shell/hole role".into(),
                            )
                            .into());
                        }
                        if source_ring_reuse_matches(
                            self.topology,
                            source_ring,
                            false,
                            &ring.vertices,
                            self.emission.fragment_recipes,
                            walk,
                        )? && self.source_q_vertices_are_unchanged(
                            &ring.vertices,
                            coordinates,
                            budget,
                        )? && self.source_walk_publication_is_unchanged(
                            walk,
                            coordinates,
                            publication_coordinates,
                            budget,
                        )? {
                            add_source_reused_recipe_walk(
                                walk,
                                ring.vertices.len().saturating_sub(1),
                                self.emission.fragment_recipes,
                                self.emission.publication,
                                &mut reused,
                                budget,
                            )?;
                        }
                    }
                }
            },
            DrawnPolygons::Regions { rings, .. } => {
                for (ring_index, ring) in rings.iter().enumerate() {
                    budget.charge_into_error(1)?;
                    let walk = super::super::super::emission::next_recipe_walk(
                        &walks,
                        &mut cursors,
                        ring.recipe_walk,
                        budget,
                    )?;
                    if !matches!(ring.origin, DrawnAreaOrigin::Source) {
                        continue;
                    }
                    let RecipeWalkOwner::SourceRing(ring_id) = ring.recipe_walk else {
                        return Err(GeometryErrorKind::Invalid(
                            "source area ring has a non-source recipe owner".into(),
                        )
                        .into());
                    };
                    let source_ring =
                        self.topology.sources.rings.get(ring_id).ok_or_else(|| {
                            GeometryErrorKind::Invalid("source area reuse ring is unknown".into())
                        })?;
                    let is_hole = *ring_roles.get(ring_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "source area reuse has no certified shell/hole role".into(),
                        )
                    })?;
                    let planned = match &self.emission.plan.polygons {
                        PolygonPlan::Regions { rings, .. } => rings.get(ring_index).copied(),
                        PolygonPlan::WholeSphere(_) => None,
                    };
                    let Some(PlannedAreaRing::Source {
                        ring: planned_ring,
                        region: planned_region,
                        reverse,
                        ..
                    }) = planned
                    else {
                        return Err(GeometryErrorKind::Invalid(
                            "source drawing has no matching source area plan".into(),
                        )
                        .into());
                    };
                    if source_ring.hole ^ reverse != is_hole {
                        return Err(GeometryErrorKind::Invalid(
                            "source area reuse role disagrees with its planned traversal".into(),
                        )
                        .into());
                    }
                    if planned_ring != ring_id || planned_region != ring.region {
                        return Err(GeometryErrorKind::Invalid(
                            "source drawing changed its planned source area identity".into(),
                        )
                        .into());
                    }
                    if source_ring_reuse_matches(
                        self.topology,
                        source_ring,
                        reverse,
                        &ring.vertices,
                        self.emission.fragment_recipes,
                        walk,
                    )? && self.source_q_vertices_are_unchanged(
                        &ring.vertices,
                        coordinates,
                        budget,
                    )? && self.source_walk_publication_is_unchanged(
                        walk,
                        coordinates,
                        publication_coordinates,
                        budget,
                    )? {
                        add_source_reused_recipe_walk(
                            walk,
                            ring.vertices.len().saturating_sub(1),
                            self.emission.fragment_recipes,
                            self.emission.publication,
                            &mut reused,
                            budget,
                        )?;
                    }
                }
            },
        }
        for line in &drawing.lines {
            budget.charge_into_error(1)?;
            let walk = super::super::super::emission::next_recipe_walk(
                &walks,
                &mut cursors,
                line.recipe_walk,
                budget,
            )?;
            if source_line_reuse_is_complete(
                self.topology,
                self.emission.fragment_recipes,
                Some(self.emission.publication),
                walk,
                &line.path,
                budget,
            )? && self.source_q_vertices_are_unchanged(&line.path, coordinates, budget)?
                && self.source_walk_publication_is_unchanged(
                    walk,
                    coordinates,
                    publication_coordinates,
                    budget,
                )?
            {
                budget.charge_into_error(
                    u64::try_from(walk.fragments.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                reused.extend(walk.fragments.iter().copied());
            }
        }
        for (owner, owner_walks) in &walks {
            budget.charge_into_error(1)?;
            if cursors.get(owner).copied().unwrap_or_default() != owner_walks.len() {
                return Err(GeometryErrorKind::Invalid(
                    "certified drawing did not consume each recipe walk exactly once".into(),
                )
                .into());
            }
        }
        Ok(reused)
    }
    fn source_q_vertices_are_unchanged(
        &self,
        vertices: &[EmittedVertex<'_>],
        coordinates: &QCoordinates,
        budget: &mut impl Work,
    ) -> Result<bool> {
        for vertex in vertices {
            budget.charge_into_error(1)?;
            if coordinates.candidate(vertex.port.node)?
                != self.nearest.candidate(vertex.port.node)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn source_walk_publication_is_unchanged(
        &self,
        walk: &RecipeWalk,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<bool> {
        for &recipe_id in &walk.fragments {
            let fragment = self
                .emission
                .publication
                .fragments
                .get(recipe_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source-reuse walk has no publication fragment".into(),
                    )
                })?;
            for sample_index in fragment.sample_range.clone() {
                budget.charge_into_error(1)?;
                let node_id =
                    *self
                        .publication_sample_nodes
                        .get(sample_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "source-reuse sample has no emission node".into(),
                            )
                        })?;
                let node = self
                    .publication_nodes
                    .nodes()
                    .get(node_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "source-reuse sample emission node is unknown".into(),
                        )
                    })?;
                let (current, nearest) = match &node.authority {
                    NodeAuthority::Source(source) => (
                        coordinates.candidate(*source)?,
                        self.nearest.candidate(*source)?,
                    ),
                    NodeAuthority::Computed(_) => {
                        let current = publication_coordinates
                            .get(node_id.0 as usize)
                            .copied()
                            .flatten()
                            .or_else(|| {
                                self.publication_nearest
                                    .get(node_id.0 as usize)
                                    .copied()
                                    .flatten()
                            })
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source-reuse sample has no assigned candidate".into(),
                                )
                            })?;
                        let nearest = self
                            .publication_nearest
                            .get(node_id.0 as usize)
                            .copied()
                            .flatten()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source-reuse sample has no nearest candidate".into(),
                                )
                            })?;
                        (current, nearest)
                    },
                };
                if current != nearest {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
    fn input_port_has_rounding_residual(
        &self,
        port_id: QPortId,
        source_nodes: &[bool],
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<bool> {
        budget.charge_into_error(1)?;
        let port = self
            .topology
            .q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Native-demand Q port is unknown".into()))?;
        if port.cut {
            let assigned = producer
                .source_ports
                .get(port_id.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Native-demand Q cut port has no assigned image".into(),
                    )
                })?;
            if assigned == &port.point {
                return Ok(false);
            }
            let native_longitude = exact_canonical_longitude_with_work(
                self.topology.sources.frame,
                &assigned.x,
                budget,
            )?;
            return Ok(
                !exact_coordinate_has_binary64_value_with_work(&native_longitude, budget)?
                    || !exact_coordinate_has_binary64_value_with_work(&assigned.y, budget)?,
            );
        }
        let is_source = *source_nodes
            .get(port.node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Native-demand Q node is unknown".into()))?;
        if is_source {
            return Ok(false);
        }
        let Some(position) = self
            .exact_positions
            .get(port.node.0 as usize)
            .and_then(Option::as_ref)
        else {
            return Ok(false);
        };
        Ok(!exact_node_position_has_binary64_value_with_work(
            position, budget,
        )?)
    }
    fn drawing_has_rounding_residual(
        &self,
        drawing: &QEmissionDrawing<'topology>,
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<bool> {
        let q = &self.topology.q;
        let source_endpoint_count = q
            .source_edge_ports
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(q.source_point_ports.len()))
            .and_then(|count| count.checked_add(q.nodes.len()))
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_into_error(
            u64::try_from(source_endpoint_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut source_nodes = vec![false; q.nodes.len()];
        for &[start, end] in &q.source_edge_ports {
            for port_id in [start, end] {
                let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Native-demand input edge references an unknown Q port".into(),
                    )
                })?;
                *source_nodes.get_mut(port.node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Native-demand input edge references an unknown Q node".into(),
                    )
                })? = true;
            }
        }
        for &port_id in &q.source_point_ports {
            let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "Native-demand input point references an unknown Q port".into(),
                )
            })?;
            *source_nodes.get_mut(port.node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "Native-demand input point references an unknown Q node".into(),
                )
            })? = true;
        }
        match &drawing.polygons {
            DrawnPolygons::WholeSphere(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        for vertex in &ring.vertices {
                            if self.input_port_has_rounding_residual(
                                vertex.port.id,
                                &source_nodes,
                                producer,
                                budget,
                            )? {
                                return Ok(true);
                            }
                        }
                    }
                }
            },
            DrawnPolygons::Regions { rings, .. } => {
                for ring in rings {
                    for vertex in &ring.vertices {
                        if self.input_port_has_rounding_residual(
                            vertex.port.id,
                            &source_nodes,
                            producer,
                            budget,
                        )? {
                            return Ok(true);
                        }
                    }
                }
            },
        }
        for line in &drawing.lines {
            for vertex in &line.path {
                if self.input_port_has_rounding_residual(
                    vertex.port.id,
                    &source_nodes,
                    producer,
                    budget,
                )? {
                    return Ok(true);
                }
            }
        }
        for vertex in &drawing.points {
            if self.input_port_has_rounding_residual(
                vertex.port.id,
                &source_nodes,
                producer,
                budget,
            )? {
                return Ok(true);
            }
        }
        for chord in &self.publication_chords {
            budget.charge_into_error(1)?;
            for endpoint in &chord.endpoints {
                let port_id = match endpoint {
                    PublishedEndpoint::Source { port, .. } => *port,
                    PublishedEndpoint::Published { node, sample } => {
                        let emission_node = self
                            .publication_nodes
                            .nodes()
                            .get(node.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Native-demand publication node is unknown".into(),
                                )
                            })?;
                        let NodeAuthority::Source(source_node) = &emission_node.authority else {
                            let position = self
                                .publication_exact_positions
                                .get(node.0 as usize)
                                .and_then(Option::as_ref);
                            if let Some(position) = position
                                && !exact_node_position_has_binary64_value_with_work(
                                    position, budget,
                                )?
                            {
                                return Ok(true);
                            }
                            continue;
                        };
                        let Some(port_id) = self
                            .publication_sample_source_ports
                            .get(*sample)
                            .copied()
                            .flatten()
                        else {
                            return Ok(false);
                        };
                        let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Native-demand publication sample has an unknown Q port".into(),
                            )
                        })?;
                        if port.node != *source_node {
                            return Err(GeometryErrorKind::Invalid(
                                "Native-demand publication sample changed its Q node".into(),
                            )
                            .into());
                        }
                        port_id
                    },
                };
                if self.input_port_has_rounding_residual(
                    port_id,
                    &source_nodes,
                    producer,
                    budget,
                )? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
    fn native_demand_after_input_port_check(
        &self,
        drawing: &QEmissionDrawing<'topology>,
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<bool> {
        self.drawing_has_rounding_residual(drawing, producer, budget)
    }
    fn actual_native_demand(
        &self,
        drawing: &QEmissionDrawing<'topology>,
        ring_roles: &[bool],
        producer: &AssignedProducerProjection,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<bool> {
        if self.emission.force_native {
            return Ok(true);
        }
        if producer.leaves.len() != self.publication_chords.len() {
            return Err(GeometryErrorKind::Invalid(
                "assigned producer projection does not cover every published leaf".into(),
            )
            .into());
        }
        let source_reused = self.source_reused_recipe_ids(
            drawing,
            ring_roles,
            coordinates,
            publication_coordinates,
            budget,
        )?;
        let axis = self.topology.sources.frame.longitude();
        for leaf in &producer.leaves {
            budget.charge_into_error(1)?;
            if source_reused.contains(&leaf.recipe) {
                continue;
            }
            let [start, end] = &leaf.points;
            if strict_native_bank_event(axis, &start.x, &end.x, budget)? {
                return self.native_demand_after_input_port_check(drawing, producer, budget);
            }
        }
        if let DrawnPolygons::Regions { rings, .. } = &drawing.polygons {
            for ring in rings {
                budget.charge_into_error(1)?;
                if !matches!(ring.origin, DrawnAreaOrigin::Cycle) {
                    continue;
                }
                let selected_regions = self.emission.selected_regions;
                let mut low = 0_usize;
                let mut high = selected_regions.len();
                while low < high {
                    budget.charge_into_error(1)?;
                    let middle = low.midpoint(high);
                    let candidate = selected_regions.get(middle).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "generated area use has no selected-region authority".into(),
                        )
                    })?;
                    if candidate.root < ring.region {
                        low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    } else {
                        high = middle;
                    }
                }
                let region = selected_regions
                    .get(low)
                    .filter(|region| region.root == ring.region)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "generated area use has no selected-region authority".into(),
                        )
                    })?;
                if region.poles.includes(ChartPole::North)
                    || region.poles.includes(ChartPole::South)
                {
                    return self.native_demand_after_input_port_check(drawing, producer, budget);
                }
                for vertex in &ring.vertices {
                    budget.charge_into_error(1)?;
                    if vertex.port.cut
                        || axis
                            .canonicalize(vertex.point.x)
                            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
                            .identity(axis)
                            == SeamIdentity::Seam
                    {
                        return self
                            .native_demand_after_input_port_check(drawing, producer, budget);
                    }
                }
            }
        }
        Ok(false)
    }
}
