use super::super::{
    AssignedProducerProjection, BTreeMap, ChartMetric, ChartPole, ChartRestriction, DrawnPolygons,
    EvaluationBudget, ExactChartPoint, ExactRational, FragmentRecipeId, GeometryErrorKind,
    NativeAreaEdgeKind, NativeChordPieces, NativeCutFeature, NativeGraphEdgeOwner, NativeLineRun,
    NativeLiteralChordAuthority, NativeLiteralChordUse, NativeReference,
    NativeReferenceGraphResult, NativeSeamEvent, NativeSeamEventData, NativeSeamVertices,
    NodeAuthority, OccurrenceSource, QCandidate, QCoordinates, QEmbeddingProblem, QEmissionDrawing,
    QFamilyState, QRingRoleCheck, RecipeWalkOwner, ResourceError, Result, XY, admit_logical_layout,
    align_native_cycle_starts, append_native_line_edge, assign_selected_pole_seam_band,
    assigned_emission_site, candidate_from_emission_site, common_refine_native_edges,
    finish_native_line_run, logical_layout_for_plan, merge_native_port_data, native_area_facts,
    native_emission_port_key, native_graph_cycle_recipe, native_graph_cycle_vertex,
    native_graph_face_source_table, native_known_span_boundaries, native_point_contact_vertices,
    native_pole_closure_regions, native_pole_edges, native_pole_walk_edges, native_port_data,
    native_reference_cycles_and_parts, native_seam_bands_from_assigned_graph, native_seam_edges,
    native_selected_pole_sectors, native_whole_sphere_cell_edges,
    new_native_reference_graph_builder, normalize_native_chart_lifts, normalize_native_seam_events,
    try_sort_by_with_work, validate_native_reference, validate_native_source_ranges,
};
use crate::geographic::retained_exact::sites::rounded_native_neighbors_with_work;
use crate::geometry::topology::emission::draw::diagnostic_line_tag;
use crate::work::Work as _;

impl<'topology, M: ChartMetric> QEmbeddingProblem<'topology, '_, '_, '_, M> {
    pub(super) fn prepare_native_reference(
        &mut self,
        drawing: &QEmissionDrawing<'topology>,
        ring_roles: &QRingRoleCheck,
        producer: &AssignedProducerProjection,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        features: &[NativeCutFeature],
        budget: &mut EvaluationBudget,
    ) -> Result<NativeReference<'topology>> {
        if producer.leaves.len() != self.publication_chords.len() {
            return Err(GeometryErrorKind::Invalid(
                "native reference does not match its assigned producer projection".into(),
            )
            .into());
        }
        let area_facts = native_area_facts(drawing, self.emission.selected_face_roots)?;
        let whole_sphere = matches!(&drawing.polygons, DrawnPolygons::WholeSphere(_));
        let has_area = whole_sphere || !area_facts.is_empty();
        let whole_sphere_region = if whole_sphere {
            Some(
                self.emission
                    .selected_face_roots
                    .iter()
                    .flatten()
                    .copied()
                    .next()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "whole-sphere reference has no selected region root".into(),
                        )
                    })?,
            )
        } else {
            None
        };
        let selected_poles = native_selected_pole_sectors(
            &self.topology.q,
            self.emission.selected_face_roots,
            budget,
        )?;
        let pole_closure_sectors = native_pole_closure_regions(&selected_poles.sectors, budget)?;
        let mut pole_closures = Vec::new();
        if has_area {
            for &closure in &selected_poles.closures {
                budget.charge_into_error(1)?;
                let (region, _) = closure;
                // A source fan already supplies its pole port and sector bounds;
                // terminal and isolated sectors need a synthetic closure.
                if !pole_closure_sectors.contains(&closure) {
                    continue;
                }
                let mut area_region = whole_sphere;
                for &candidate in area_facts.values() {
                    budget.charge_into_error(1)?;
                    area_region |= candidate == region;
                    if area_region {
                        break;
                    }
                }
                if area_region && !pole_closures.contains(&closure) {
                    budget.charge_into_error(1)?;
                    pole_closures.push(closure);
                }
            }
        }
        let mut builder = super::super::super::emission::EmissionBuilder::new(
            self.topology,
            self.emission.plan,
            &self.source_inventory,
            whole_sphere,
            budget,
        )?;
        let sample_nodes =
            builder.reserve_publication_nodes(&self.emission.publication.samples, budget)?;
        if sample_nodes != self.publication_sample_nodes {
            return Err(GeometryErrorKind::Invalid(
                "native emission node order differs from publication search".into(),
            )
            .into());
        }
        let mut cut_nodes = Vec::with_capacity(features.len());
        for feature in features {
            budget.charge_into_error(1)?;
            cut_nodes.push(builder.intern_point(&feature.point, budget)?);
        }

        let mut presentation_candidates = {
            let node_count = builder.nodes().nodes().len();
            budget.charge_into_error(
                u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            vec![None; node_count]
        };
        let whole_sphere_edges = if let Some(region) = whole_sphere_region {
            let QFamilyState::Native(families) = &mut self.family_state else {
                return Err(GeometryErrorKind::Invalid(
                    "whole-sphere presentation preceded Native family activation".into(),
                )
                .into());
            };
            native_whole_sphere_cell_edges(
                &mut builder,
                self.topology.sources.frame,
                region,
                &mut presentation_candidates,
                families,
                budget,
            )?
        } else {
            Vec::new()
        };

        let node_count = builder.nodes().nodes().len();
        let mut has_publication_sample_by_node = vec![false; node_count];
        for &node in &sample_nodes {
            let authority = &builder
                .nodes()
                .nodes()
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native publication node is unknown".into())
                })?
                .authority;
            if matches!(authority, NodeAuthority::Computed(_)) {
                let slot = has_publication_sample_by_node
                    .get_mut(node.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("native sample node slot is unknown".into())
                    })?;
                *slot = true;
            }
        }
        let mut cut_node = vec![false; node_count];
        for &node in &cut_nodes {
            let slot = cut_node.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut node slot is unknown".into())
            })?;
            *slot = true;
        }
        let mut sites = Vec::with_capacity(node_count);
        for (node_index, node) in builder.nodes().nodes().iter().enumerate() {
            let site = match &node.authority {
                NodeAuthority::Source(source) => {
                    let index = source.0 as usize;
                    let candidate = QCandidate {
                        native: coordinates.xy(*source)?,
                        periods: *coordinates.longitude_periods.get(index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native source node has no assigned longitude lift".into(),
                            )
                        })?,
                    };
                    Some(assigned_emission_site(
                        self.topology.sources.frame,
                        candidate,
                    )?)
                },
                NodeAuthority::Computed(_) => {
                    let has_publication_sample = *has_publication_sample_by_node
                        .get(node_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native publication node has no sample state".into(),
                            )
                        })?;
                    let publication_candidate = if has_publication_sample {
                        publication_coordinates
                            .get(node_index)
                            .copied()
                            .flatten()
                            .or_else(|| self.publication_nearest.get(node_index).copied().flatten())
                    } else {
                        None
                    };
                    if let Some(candidate) = publication_candidate {
                        Some(assigned_emission_site(
                            self.topology.sources.frame,
                            candidate,
                        )?)
                    } else if let Some(candidate) =
                        presentation_candidates.get(node_index).copied().flatten()
                    {
                        Some(assigned_emission_site(
                            self.topology.sources.frame,
                            candidate,
                        )?)
                    } else if cut_node.get(node_index).copied().unwrap_or(false) {
                        None
                    } else {
                        return Err(GeometryErrorKind::Invalid(
                            "native computed node has no source, publication, or cut owner".into(),
                        )
                        .into());
                    }
                },
            };
            sites.push(site);
        }

        let mut chords_by_recipe = vec![Vec::<usize>::new(); self.emission.fragment_recipes.len()];
        for (chord_index, chord) in self.publication_chords.iter().enumerate() {
            chords_by_recipe
                .get_mut(chord.recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native producer chord has an unknown recipe".into())
                })?
                .push(chord_index);
        }
        let mut cuts_by_leaf = BTreeMap::<(FragmentRecipeId, usize), Vec<usize>>::new();
        for (feature_index, feature) in features.iter().enumerate() {
            let recipe = feature.family.fragment_recipe().ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut has no fragment family".into())
            })?;
            cuts_by_leaf
                .entry((recipe, feature.trace_edge))
                .or_default()
                .push(feature_index);
        }
        for indices in cuts_by_leaf.values_mut() {
            try_sort_by_with_work(indices, budget, |left, right, budget| {
                budget.charge_into_error(1)?;
                features
                    .get(*left)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("native cut trace parameter is unknown".into())
                    })?
                    .edge_parameter
                    .try_cmp_with_work(
                        &features
                            .get(*right)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "native cut trace parameter is unknown".into(),
                                )
                            })?
                            .edge_parameter,
                        budget,
                    )
            })?;
        }

        let mut expanded_traces = Vec::with_capacity(self.emission.publication.fragments.len());
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        for (recipe_index, fragment) in self.emission.publication.fragments.iter().enumerate() {
            budget.charge_into_error(1)?;
            if fragment.recipe.0 as usize != recipe_index {
                return Err(GeometryErrorKind::Invalid(
                    "native source traces are not recipe-aligned".into(),
                )
                .into());
            }
            let edge_count = fragment.trace.rows.edge_count();
            let mut slices = Vec::with_capacity(edge_count);
            for edge in 0..edge_count {
                budget.charge_into_error(1)?;
                slices.push((edge, zero.clone(), one.clone()));
            }
            expanded_traces.push(fragment.trace.compose_edges(&slices, budget)?);
        }

        let port_points = self.global_q_port_points(coordinates, budget)?;
        let mut area_edges = whole_sphere_edges;
        let mut expected_area_leaves = Vec::new();
        let mut expected_line_leaves = Vec::new();
        let mut seam_events = Vec::<NativeSeamEventData>::new();
        let mut line_traces = Vec::new();
        let mut diagnostic_line_tags = Vec::new();
        let mut literal_chord_uses = Vec::new();
        let mut line_graph_edges = Vec::new();
        let mut reference_ports = Vec::new();
        for walk in self.emission.fragment_walks {
            if whole_sphere && matches!(walk.owner, RecipeWalkOwner::SourceRing(_, _)) {
                continue;
            }
            let area_region = match walk.owner {
                RecipeWalkOwner::Line(_) => None,
                owner @ (RecipeWalkOwner::Cycle(_) | RecipeWalkOwner::SourceRing(..)) => {
                    Some(area_facts.get(&owner).copied().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native area walk has no selected-region fact".into(),
                        )
                    })?)
                },
            };
            let line_tag = match walk.owner {
                RecipeWalkOwner::Line(line_index) => {
                    let line = self
                        .emission
                        .plan
                        .line_paths
                        .get(line_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native line walk has no planned Q line".into(),
                            )
                        })?;
                    diagnostic_line_tag(self.topology, line, budget)?
                },
                RecipeWalkOwner::Cycle(_) | RecipeWalkOwner::SourceRing(..) => None,
            };
            let mut line_run = NativeLineRun::default();
            let area_walk_start = area_edges.len();
            for &recipe_id in &walk.fragments {
                let fragment = self
                    .emission
                    .publication
                    .fragments
                    .get(recipe_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native recipe walk has no published fragment".into(),
                        )
                    })?;
                let recipe = self
                    .emission
                    .fragment_recipes
                    .get(recipe_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("native recipe walk is unknown".into())
                    })?;
                if area_region.is_some()
                    != matches!(
                        recipe.use_kind,
                        super::super::super::emission::SelectedUse::AreaBoundary
                    )
                {
                    return Err(GeometryErrorKind::Invalid(
                        "native recipe use does not match its selected walk".into(),
                    )
                    .into());
                }
                let chord_indices =
                    chords_by_recipe.get(recipe_id.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native recipe has no assigned producer chords".into(),
                        )
                    })?;
                let edge_count = fragment.points.len().checked_sub(1).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native producer fragment is empty".into())
                })?;
                if chord_indices.len() != edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer chord count differs from its trace leaves".into(),
                    )
                    .into());
                }
                for (trace_edge, &chord_index) in chord_indices.iter().enumerate() {
                    let chord = self.publication_chords.get(chord_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native producer chord is unknown".into())
                    })?;
                    let assigned_leaf = producer.leaves.get(chord_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native producer leaf projection is unknown".into(),
                        )
                    })?;
                    if assigned_leaf.recipe != chord.recipe
                        || assigned_leaf.trace_edge != chord.trace_edge
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "native producer leaf projection changed its writer identity".into(),
                        )
                        .into());
                    }
                    if chord.recipe != recipe_id || chord.trace_edge != trace_edge {
                        return Err(GeometryErrorKind::Invalid(
                            "native producer leaves are not in recipe order".into(),
                        )
                        .into());
                    }
                    if area_region.is_some() {
                        budget.charge_into_error(1)?;
                        expected_area_leaves.push((recipe_id, trace_edge));
                    } else {
                        budget.charge_into_error(1)?;
                        expected_line_leaves.push((recipe_id, trace_edge));
                    }
                    let no_cuts = Vec::new();
                    let feature_indices = cuts_by_leaf
                        .get(&(recipe_id, trace_edge))
                        .unwrap_or(&no_cuts);
                    let expanded_trace =
                        expanded_traces.get(recipe_id.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native producer has no expanded source trace".into(),
                            )
                        })?;
                    let pieces = self.native_chord_pieces(
                        &mut builder,
                        chord,
                        &assigned_leaf.points,
                        expanded_trace,
                        area_region,
                        feature_indices,
                        features,
                        &sites,
                        budget,
                    )?;
                    let NativeChordPieces {
                        edges,
                        break_after,
                        seam_events: piece_seam_events,
                    } = pieces;
                    seam_events.extend(piece_seam_events);
                    if area_region.is_some() {
                        area_edges.extend(edges);
                    } else {
                        for (edge, break_after) in edges.into_iter().zip(break_after) {
                            append_native_line_edge(
                                self.topology.sources.frame,
                                &mut line_run,
                                edge,
                                &mut line_graph_edges,
                                budget,
                            )?;
                            if break_after && !line_run.chords.is_empty() {
                                let trace_line = line_traces.len();
                                line_traces.push(finish_native_line_run(
                                    &mut builder,
                                    std::mem::take(&mut line_run),
                                    &mut reference_ports,
                                    trace_line,
                                    &mut literal_chord_uses,
                                    budget,
                                )?);
                                if let Some(tag) = line_tag {
                                    diagnostic_line_tags.push((trace_line, tag));
                                }
                            }
                        }
                    }
                }
            }
            if let Some(region) = area_region {
                // Distinct chart occurrences of one pole are connected in Q,
                // but need the selected walk's pole arc in the planar drawing.
                let (pole_edges, pole_events) = native_pole_walk_edges(
                    self.topology.sources.frame,
                    region,
                    area_edges
                        .get(area_walk_start..)
                        .expect("native area walk starts in the current edge table"),
                    budget,
                )?;
                area_edges.extend(pole_edges);
                for vertices in pole_events {
                    seam_events.push(NativeSeamEventData {
                        event: NativeSeamEvent {
                            latitude: vertices.latitude.try_clone_with_work(budget)?,
                            output_vertex: true,
                        },
                        vertices,
                    });
                }
            } else if !line_run.chords.is_empty() {
                let trace_line = line_traces.len();
                line_traces.push(finish_native_line_run(
                    &mut builder,
                    line_run,
                    &mut reference_ports,
                    trace_line,
                    &mut literal_chord_uses,
                    budget,
                )?);
                if let Some(tag) = line_tag {
                    diagnostic_line_tags.push((trace_line, tag));
                }
            }
        }

        let mut walk_poles = Vec::new();
        for edge in &area_edges {
            budget.charge_into_error(1)?;
            if let NativeGraphEdgeOwner::Area { region } = edge.owner
                && let NativeAreaEdgeKind::Restriction(restriction) = &edge.kind
                && let ChartRestriction::PolarCap { pole } = restriction.restriction
            {
                walk_poles.push((region, pole));
            }
        }
        try_sort_by_with_work(&mut walk_poles, budget, |left, right, budget| {
            budget.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
        walk_poles.dedup();
        self.register_native_pole_families(&walk_poles, budget)?;

        for vertex in &drawing.points {
            let node = builder.source_node(vertex.port.node)?;
            let site = sites
                .get(node.0 as usize)
                .and_then(Option::as_ref)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native isolated point has no assigned site".into())
                })?;
            let chart_point = port_points
                .get(vertex.port.id.0 as usize)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native isolated point has no chart spelling".into())
                })?;
            let frame = self.topology.sources.frame;
            let half = ExactRational::from_f64(frame.longitude().half_period());
            let negative_half = ExactRational::zero().try_subtract_with_work(&half, budget)?;
            let bank = if chart_point.x.try_cmp_with_work(&half, budget)?.is_eq() {
                Some(super::super::super::emission::ChartSide::East)
            } else if chart_point
                .x
                .try_cmp_with_work(&negative_half, budget)?
                .is_eq()
            {
                Some(super::super::super::emission::ChartSide::West)
            } else {
                None
            };
            budget.charge_into_error(1)?;
            let port_data = native_port_data(
                frame,
                node,
                candidate_from_emission_site(site),
                chart_point,
                None,
                vec![OccurrenceSource {
                    origin: super::super::super::emission::VertexOrigin::SourceNode(
                        vertex.port.node,
                    ),
                    source_trace: None,
                }],
                bank,
                budget,
            )?;
            let spelling = port_data.spelling.clone();
            reference_ports.push(port_data);
            let port = builder.source_port(
                vertex.port.id,
                super::super::super::emission::VertexOrigin::SourceNode(vertex.port.node),
                Some(vertex.point),
                spelling,
            )?;
            builder.point(port)?;
        }

        let known_span_boundaries = native_known_span_boundaries(
            self.topology,
            &area_edges,
            &line_graph_edges,
            &builder,
            &port_points,
            &sites,
            budget,
        )?;
        area_edges = common_refine_native_edges(
            area_edges,
            &known_span_boundaries,
            self.topology.sources.frame,
            budget,
        )?;
        line_graph_edges = common_refine_native_edges(
            line_graph_edges,
            &known_span_boundaries,
            self.topology.sources.frame,
            budget,
        )?;
        validate_native_source_ranges(
            self.topology,
            self.emission.selected_face_roots,
            &area_edges,
            &expected_area_leaves,
            &expanded_traces,
            true,
            budget,
        )?;
        validate_native_source_ranges(
            self.topology,
            self.emission.selected_face_roots,
            &line_graph_edges,
            &expected_line_leaves,
            &expanded_traces,
            false,
            budget,
        )?;
        normalize_native_chart_lifts(
            &mut area_edges,
            &mut line_graph_edges,
            &mut reference_ports,
            self.topology.sources.frame,
            budget,
        )?;
        let mut graph_builder =
            new_native_reference_graph_builder(&area_edges, &reference_ports, budget)?;
        let pole_path_port_count = if has_area && !whole_sphere {
            [ChartPole::South, ChartPole::North]
                .iter()
                .filter(|&&pole| pole_closures.iter().any(|&(_, selected)| selected == pole))
                .count()
                .checked_mul(3)
                .ok_or(ResourceError::SizeOverflow)?
        } else {
            0
        };
        budget.charge_into_error(
            u64::try_from(pole_path_port_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        reference_ports.reserve(pole_path_port_count);
        let mut graph = if has_area {
            if !whole_sphere {
                let mut band_events = seam_events
                    .iter()
                    .map(|event| event.event.clone())
                    .collect::<Vec<_>>();
                let mut bands = native_seam_bands_from_assigned_graph(
                    &graph_builder,
                    &area_edges,
                    self.topology.sources.frame,
                    &mut band_events,
                    budget,
                )?;
                // A selected polar sector can be the unbounded side of the open
                // chart graph; its terminal seam band still bounds the cap.
                for &(region, pole) in &pole_closures {
                    assign_selected_pole_seam_band(
                        &mut bands,
                        self.topology.sources.frame,
                        region,
                        pole,
                        budget,
                    )?;
                }
                for (pole, band) in [
                    (ChartPole::South, bands.first()),
                    (ChartPole::North, bands.last()),
                ] {
                    if let Some(band) = band {
                        for region in [band.west_region, band.east_region].into_iter().flatten() {
                            if !selected_poles.closures.contains(&(region, pole)) {
                                return Err(GeometryErrorKind::Invalid(
                                    "assigned seam pole has no selected source-sector germ".into(),
                                )
                                .into());
                            }
                        }
                    }
                }
                for (pole, latitude) in [
                    (
                        ChartPole::South,
                        self.topology
                            .sources
                            .frame
                            .chart_pole_latitude(ChartPole::South),
                    ),
                    (
                        ChartPole::North,
                        self.topology
                            .sources
                            .frame
                            .chart_pole_latitude(ChartPole::North),
                    ),
                ] {
                    if !pole_closures.iter().any(|&(_, selected)| selected == pole) {
                        continue;
                    }
                    let latitude = ExactRational::from_f64(latitude);
                    let chart_point =
                        ExactChartPoint::from_rationals(ExactRational::zero(), latitude.clone());
                    let node = builder.intern_point(&chart_point, budget)?;
                    let node_index = node.0 as usize;
                    if node_index == sites.len() {
                        let [_, longitude, _] = rounded_native_neighbors_with_work(
                            self.topology.sources.frame.longitude(),
                            &chart_point.x,
                            budget,
                        )?;
                        sites.push(Some(assigned_emission_site(
                            self.topology.sources.frame,
                            QCandidate {
                                native: XY::new(
                                    longitude.native,
                                    latitude.round_with_work(budget)?,
                                ),
                                periods: longitude.periods,
                            },
                        )?));
                    } else if node_index > sites.len() {
                        return Err(GeometryErrorKind::Invalid(
                            "native pole node is outside its assigned site table".into(),
                        )
                        .into());
                    } else if sites
                        .get(node_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native pole node has no assigned site slot".into(),
                            )
                        })?
                        .is_none()
                    {
                        let [_, longitude, _] = rounded_native_neighbors_with_work(
                            self.topology.sources.frame.longitude(),
                            &chart_point.x,
                            budget,
                        )?;
                        *sites.get_mut(node_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native pole node has no assigned site slot".into(),
                            )
                        })? = Some(assigned_emission_site(
                            self.topology.sources.frame,
                            QCandidate {
                                native: XY::new(
                                    longitude.native,
                                    latitude.round_with_work(budget)?,
                                ),
                                periods: longitude.periods,
                            },
                        )?);
                    }
                    let site = sites
                        .get(node_index)
                        .and_then(Option::as_ref)
                        .copied()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("native pole site is unknown".into())
                        })?;
                    let candidate = candidate_from_emission_site(site);
                    let east = native_port_data(
                        self.topology.sources.frame,
                        node,
                        candidate,
                        &chart_point,
                        None,
                        vec![OccurrenceSource {
                            origin: super::super::super::emission::VertexOrigin::Presentation,
                            source_trace: None,
                        }],
                        Some(super::super::super::emission::ChartSide::East),
                        budget,
                    )?;
                    let west = native_port_data(
                        self.topology.sources.frame,
                        node,
                        candidate,
                        &chart_point,
                        None,
                        vec![OccurrenceSource {
                            origin: super::super::super::emission::VertexOrigin::Presentation,
                            source_trace: None,
                        }],
                        Some(super::super::super::emission::ChartSide::West),
                        budget,
                    )?;
                    // Seam aliases alone carry no chart turn; ordinary pole spellings make
                    // the named hemisphere explicit without a half-period edge.
                    let quarter_period =
                        self.topology.sources.frame.longitude().half_period() / 2.0;
                    for longitude in [-quarter_period, 0.0, quarter_period] {
                        budget.charge_into_error(1)?;
                        let point = ExactChartPoint::from_rationals(
                            ExactRational::from_f64(longitude),
                            chart_point.y.try_clone_with_work(budget)?,
                        );
                        let [_, nearest, _] = rounded_native_neighbors_with_work(
                            self.topology.sources.frame.longitude(),
                            &point.x,
                            budget,
                        )?;
                        reference_ports.push(native_port_data(
                            self.topology.sources.frame,
                            node,
                            QCandidate {
                                native: XY::new(nearest.native, point.y.round_with_work(budget)?),
                                periods: nearest.periods,
                            },
                            &point,
                            None,
                            vec![OccurrenceSource {
                                origin: super::super::super::emission::VertexOrigin::Presentation,
                                source_trace: None,
                            }],
                            None,
                            budget,
                        )?);
                    }
                    seam_events.push(NativeSeamEventData {
                        event: NativeSeamEvent {
                            latitude: latitude.clone(),
                            output_vertex: true,
                        },
                        vertices: NativeSeamVertices {
                            latitude,
                            east,
                            west,
                        },
                    });
                }

                let normalized_events =
                    normalize_native_seam_events(self.topology.sources.frame, seam_events, budget)?;
                budget.charge_into_error(
                    u64::try_from(normalized_events.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut native_seam_vertices = Vec::with_capacity(normalized_events.len());
                for event in &normalized_events {
                    budget.charge_into_error(1)?;
                    native_seam_vertices.push(NativeSeamVertices {
                        latitude: event.vertices.latitude.try_clone_with_work(budget)?,
                        east: event.vertices.east.try_clone_with_work(budget)?,
                        west: event.vertices.west.try_clone_with_work(budget)?,
                    });
                }
                let seam_edges = native_seam_edges(
                    &bands,
                    &native_seam_vertices,
                    &mut |region, start, end, budget| {
                        graph_builder.has_area_edge(region, start, end, budget)
                    },
                    budget,
                )?;
                area_edges.extend(seam_edges);
                graph_builder.append_edges(&area_edges, budget)?;
                let pole_edges = native_pole_edges(
                    self.topology.sources.frame,
                    &native_seam_vertices,
                    &pole_closures,
                    &area_edges,
                    &line_graph_edges,
                    &reference_ports,
                    &mut |region, start, end, budget| {
                        graph_builder.has_area_edge(region, start, end, budget)
                    },
                    budget,
                )?;
                budget.charge_into_error(
                    u64::try_from(pole_edges.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut materialized_poles = Vec::with_capacity(pole_edges.len());
                for edge in &pole_edges {
                    budget.charge_into_error(1)?;
                    let NativeAreaEdgeKind::Restriction(restriction) = &edge.kind else {
                        return Err(GeometryErrorKind::Invalid(
                            "native pole constructor returned a non-pole edge".into(),
                        )
                        .into());
                    };
                    let ChartRestriction::PolarCap { pole } = restriction.restriction else {
                        return Err(GeometryErrorKind::Invalid(
                            "native pole constructor returned a non-polar restriction".into(),
                        )
                        .into());
                    };
                    let NativeGraphEdgeOwner::Area { region } = edge.owner else {
                        return Err(GeometryErrorKind::Invalid(
                            "native pole presentation has no selected-region owner".into(),
                        )
                        .into());
                    };
                    materialized_poles.push((region, pole));
                }
                try_sort_by_with_work(&mut materialized_poles, budget, |left, right, budget| {
                    budget.charge_into_error(1)?;
                    Ok(left.cmp(right))
                })?;
                budget.charge_into_error(
                    u64::try_from(materialized_poles.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                materialized_poles.dedup();
                self.register_native_pole_families(&materialized_poles, budget)?;
                area_edges.extend(pole_edges);
            }

            graph_builder.append_edges(&area_edges, budget)?;
            graph_builder.append_line_edges(area_edges.len(), &line_graph_edges, budget)?;
            let mut graph = graph_builder.finish(self.topology.sources.frame, budget)?;
            graph.face_source_uses =
                native_graph_face_source_table(&graph, &area_edges, &line_graph_edges, budget)?;
            let NativeReferenceGraphResult {
                mut cycles,
                parts,
                face_regions,
                face_groups,
                face_cells,
            } = native_reference_cycles_and_parts(
                &graph,
                &area_edges,
                &line_graph_edges,
                whole_sphere,
                true,
                budget,
            )?;
            align_native_cycle_starts(
                self.topology.sources.frame,
                self.topology,
                drawing,
                ring_roles,
                builder.nodes(),
                &graph,
                &mut cycles,
                budget,
            )?;
            graph.cycles = cycles;
            graph.parts = parts;
            graph.face_regions = face_regions;
            graph.face_groups = face_groups;
            graph.face_cells = face_cells;
            let mut cycle_chords = Vec::with_capacity(graph.cycles.len());
            for cycle in &graph.cycles {
                let edge_count = cycle.edges.len();
                let mut ports = Vec::with_capacity(edge_count);
                for vertex_index in 0..edge_count {
                    let previous_index = if vertex_index == 0 {
                        edge_count.checked_sub(1).ok_or_else(|| {
                            GeometryErrorKind::Invalid("native area cycle is empty".into())
                        })?
                    } else {
                        vertex_index
                            .checked_sub(1)
                            .ok_or(ResourceError::SizeOverflow)?
                    };
                    let incoming = cycle.edges.get(previous_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native graph cycle has no incoming atom".into())
                    })?;
                    let outgoing = cycle.edges.get(vertex_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native graph cycle has no outgoing atom".into())
                    })?;
                    let incoming_vertex = native_graph_cycle_vertex(
                        self.topology.sources.frame,
                        &graph,
                        incoming,
                        1,
                        &area_edges,
                        budget,
                    )?;
                    let outgoing_vertex = native_graph_cycle_vertex(
                        self.topology.sources.frame,
                        &graph,
                        outgoing,
                        0,
                        &area_edges,
                        budget,
                    )?;
                    let vertex = merge_native_port_data(
                        self.topology.sources.frame,
                        &incoming_vertex,
                        &outgoing_vertex,
                        budget,
                    )?;
                    ports.push(builder.port(vertex)?);
                }
                let mut chords = Vec::with_capacity(edge_count);
                for (index, cycle_edge) in cycle.edges.iter().enumerate() {
                    let next = index
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?
                        .checked_rem(edge_count)
                        .ok_or(ResourceError::SizeOverflow)?;
                    let start = *ports.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native area start port is unknown".into())
                    })?;
                    let end = *ports.get(next).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native area end port is unknown".into())
                    })?;
                    let recipe = native_graph_cycle_recipe(&graph, cycle_edge, &area_edges)?;
                    let chord = builder.chord([start, end], recipe)?;
                    if chord.0 as usize != literal_chord_uses.len() {
                        return Err(GeometryErrorKind::Invalid(
                            "native area chord is out of order in the literal-use map".into(),
                        )
                        .into());
                    }
                    budget.charge_into_error(
                        u64::try_from(cycle_edge.contributors.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    literal_chord_uses.push(NativeLiteralChordUse {
                        chord,
                        authority: NativeLiteralChordAuthority::ReferenceAtom {
                            atom: cycle_edge.atom,
                            reversed: cycle_edge.reversed,
                            writer_edges: cycle_edge.contributors.clone(),
                        },
                    });
                    chords.push(chord);
                }
                cycle_chords.push(chords);
            }
            for part in &graph.parts {
                let shell = cycle_chords.get(part.shell).cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid("native part shell cycle is unknown".into())
                })?;
                let holes = part
                    .holes
                    .iter()
                    .map(|&hole| {
                        cycle_chords.get(hole).cloned().ok_or_else(|| {
                            GeometryErrorKind::Invalid("native part hole cycle is unknown".into())
                                .into()
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                builder.part(part.face, part.region, shell, holes)?;
            }
            graph
        } else {
            graph_builder.append_line_edges(area_edges.len(), &line_graph_edges, budget)?;
            let mut graph = graph_builder.finish(self.topology.sources.frame, budget)?;
            graph.face_source_uses =
                native_graph_face_source_table(&graph, &area_edges, &line_graph_edges, budget)?;
            let NativeReferenceGraphResult {
                cycles,
                parts,
                face_regions,
                face_groups,
                face_cells,
            } = native_reference_cycles_and_parts(
                &graph,
                &area_edges,
                &line_graph_edges,
                whole_sphere,
                false,
                budget,
            )?;
            graph.cycles = cycles;
            graph.parts = parts;
            graph.face_regions = face_regions;
            graph.face_groups = face_groups;
            graph.face_cells = face_cells;
            graph
        };

        let plan = builder.finish();
        if literal_chord_uses.len() != plan.chords.len() {
            return Err(GeometryErrorKind::Invalid(
                "native literal chord-use rows do not cover the emission plan".into(),
            )
            .into());
        }
        for (index, use_row) in literal_chord_uses.iter().enumerate() {
            budget.charge_into_error(1)?;
            if use_row.chord.0 as usize != index {
                return Err(GeometryErrorKind::Invalid(
                    "native literal chord-use rows are not chord-indexed".into(),
                )
                .into());
            }
        }
        budget.charge_into_error(
            u64::try_from(plan.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut port_vertices = Vec::with_capacity(plan.ports.len());
        for port in &plan.ports {
            budget.charge_into_error(1)?;
            port_vertices.push(graph.local_vertex(native_emission_port_key(port), budget)?);
        }
        let point_contact_vertices = native_point_contact_vertices(&plan, &port_vertices, budget)?;
        graph.common = graph
            .common
            .with_output_point_contacts(&point_contact_vertices, budget)?;
        let logical_walks = logical_layout_for_plan(&plan, &port_vertices, budget)?;
        let dimension =
            admit_logical_layout(&graph.common, logical_walks, Some(&graph.faces), budget)?;
        let reference = NativeReference {
            plan,
            dimension,
            port_vertices,
            sites,
            feature_nodes: cut_nodes,
            publication_sample_nodes: sample_nodes,
            line_traces,
            diagnostic_line_tags,
            literal_chord_uses,
            line_graph_edges,
            area_edges,
            pole_sectors: selected_poles.sectors,
            graph,
        };
        validate_native_reference(&reference, budget)?;
        Ok(reference)
    }
}
