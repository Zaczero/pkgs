use super::super::*;

pub(in crate::geometry::topology::q_embedding) struct NativeCandidateAssignment<
    'reference,
    'topology,
> {
    pub(in crate::geometry::topology::q_embedding) reference:
        &'reference NativeReference<'topology>,
    pub(in crate::geometry::topology::q_embedding) checked: CheckedAssignment<'reference>,
}
impl<'reference, 'topology> NativeCandidateAssignment<'reference, 'topology> {
    pub(in crate::geometry::topology::q_embedding) fn issue(
        reference: &'reference NativeReference<'topology>,
        checked: CheckedAssignment<'reference>,
    ) -> Result<Self> {
        if !checked.is_for(&reference.graph.common, &reference.dimension) {
            return Err(GeometryErrorKind::Invalid(
                "native checked assignment does not belong to its reference graph and layout"
                    .into(),
            )
            .into());
        }
        Ok(Self { reference, checked })
    }

    pub(in crate::geometry::topology::q_embedding) const fn points(
        &self,
    ) -> &NativeReferencePoints {
        self.checked.points()
    }
}
pub(in crate::geometry::topology::q_embedding) fn native_candidate_edges_preserve_embedding<
    'reference,
    'topology,
>(
    reference: &'reference NativeReference<'topology>,
    points: NativeReferencePoints,
    budget: &mut impl Work,
) -> Result<Option<NativeCandidateAssignment<'reference, 'topology>>> {
    let graph = &reference.graph;
    let Some(checked) = admit_candidate(&graph.common, &reference.dimension, points, budget)?
    else {
        return Ok(None);
    };
    if !native_graph_sewing_matches(
        &graph.sewing,
        &graph.vertex_keys,
        &checked.points().exact,
        reference.plan.topology.sources.frame,
        budget,
    )? {
        return Ok(None);
    }
    Ok(Some(NativeCandidateAssignment::issue(reference, checked)?))
}
pub(in crate::geometry::topology::q_embedding) struct NativeCandidateState<'reference, 'topology> {
    pub(in crate::geometry::topology::q_embedding) port_records: Vec<Option<Point>>,
    pub(in crate::geometry::topology::q_embedding) assignment:
        NativeCandidateAssignment<'reference, 'topology>,
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_materialized_candidate<
    'reference,
    'topology,
>(
    reference: &'reference NativeReference<'topology>,
    parts: &super::emission::NativeMaterializedParts,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    budget: &mut impl Work,
) -> Result<Option<NativeCandidateState<'reference, 'topology>>> {
    parts.validate_literal_uses(budget)?;
    if parts.points.len() != reference.plan.points.len()
        || parts.lines.len() != reference.plan.lines.len()
        || parts.polygons.len() != reference.plan.parts.len()
    {
        return Ok(None);
    }
    let graph_vertex_count = reference.graph.points.exact.len();
    budget.charge_into_error(
        u64::try_from(reference.plan.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut port_records = vec![None; reference.plan.ports.len()];
    budget.charge_into_error(
        u64::try_from(graph_vertex_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    budget.charge_into_error(
        u64::try_from(graph_vertex_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut exact_points = reference.graph.points.exact.clone();
    let frame = reference.plan.topology.sources.frame;
    for (vertex, key) in reference.graph.vertex_keys.iter().enumerate() {
        budget.charge_into_error(1)?;
        let node_index = key.node.0 as usize;
        let assigned = sites.get(node_index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("native graph vertex has no assigned node site".into())
        })?;
        let longitude = match key.spelling {
            NativeChartSpellingKey::Interior(_) => assigned.longitude.native().get(),
            NativeChartSpellingKey::SeamEast => frame.longitude().half_period(),
            NativeChartSpellingKey::SeamWest => -frame.longitude().half_period(),
            NativeChartSpellingKey::Pole(bits) => f64::from_bits(bits),
        };
        // An unchanged Native site can still be the rounded image of an
        // unrepresentable exact Q vertex.
        let assigned_point = ExactChartPoint::from_rationals(
            ExactRational::from_f64(longitude),
            ExactRational::from_f64(assigned.latitude),
        );
        let point = exact_points.get_mut(vertex).ok_or_else(|| {
            GeometryErrorKind::Invalid("native candidate graph vertex index is unknown".into())
        })?;
        if point != &assigned_point {
            *point = assigned_point;
        }
    }
    for (location, &record) in parts.literal_uses.points.iter().zip(&parts.points) {
        if !validate_native_materialized_port(
            reference,
            location.image,
            record,
            sites,
            ordinates,
            location.axes,
            &mut port_records,
            &exact_points,
            budget,
        )? {
            return Ok(None);
        }
    }
    for (line, locations) in parts.lines.iter().zip(&parts.literal_uses.lines) {
        if line.len() != locations.images.len() {
            return Ok(None);
        }
        for (&port, record) in locations.images.iter().zip(line.points()) {
            if !validate_native_materialized_port(
                reference,
                port,
                record,
                sites,
                ordinates,
                locations.axes,
                &mut port_records,
                &exact_points,
                budget,
            )? {
                return Ok(None);
            }
        }
    }
    for (polygon, uses) in parts.polygons.iter().zip(&parts.literal_uses.polygons) {
        if polygon.holes.len() != uses.holes.len() {
            return Ok(None);
        }
        for (&port, record) in uses
            .shell
            .images
            .iter()
            .zip(polygon.shell.coords().points())
        {
            if !validate_native_materialized_port(
                reference,
                port,
                record,
                sites,
                ordinates,
                uses.axes,
                &mut port_records,
                &exact_points,
                budget,
            )? {
                return Ok(None);
            }
        }
        for (hole, hole_locations) in polygon.holes.iter().zip(&uses.holes) {
            if hole.coords().len() != hole_locations.images.len() {
                return Ok(None);
            }
            for (&port, record) in hole_locations.images.iter().zip(hole.coords().points()) {
                if !validate_native_materialized_port(
                    reference,
                    port,
                    record,
                    sites,
                    ordinates,
                    uses.axes,
                    &mut port_records,
                    &exact_points,
                    budget,
                )? {
                    return Ok(None);
                }
            }
        }
    }
    budget.charge_into_error(
        u64::try_from(exact_points.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let rounded = exact_points
        .iter()
        .map(|point| {
            Ok(XY::new(
                point.x.round_with_work(budget)?,
                point.y.round_with_work(budget)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let candidate_points = NativeReferencePoints {
        exact: exact_points,
        rounded,
    };

    for (line_index, (line, ports)) in parts
        .lines
        .iter()
        .zip(&parts.literal_uses.lines)
        .enumerate()
    {
        let chords = reference.plan.lines.get(line_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native materialized line has no plan walk".into())
        })?;
        if line.len()
            != chords
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
            || !validate_native_materialized_chord_row(
                reference,
                chords,
                &ports.images,
                &port_records,
                &candidate_points,
                budget,
            )?
        {
            return Ok(None);
        }
    }
    for (part_index, (polygon, uses)) in parts
        .polygons
        .iter()
        .zip(&parts.literal_uses.polygons)
        .enumerate()
    {
        let part = reference.plan.parts.get(part_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native materialized polygon has no logical part".into())
        })?;
        if polygon.shell.coords().len()
            != part
                .shell
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
            || uses.shell.images.first() != uses.shell.images.last()
            || polygon.shell.coords().points().next() != polygon.shell.coords().points().next_back()
            || !validate_native_materialized_chord_row(
                reference,
                &part.shell,
                &uses.shell.images,
                &port_records,
                &candidate_points,
                budget,
            )?
            || polygon.holes.len() != part.holes.len()
            || uses.holes.len() != part.holes.len()
        {
            return Ok(None);
        }
        for (hole_index, (hole, hole_locations)) in
            polygon.holes.iter().zip(&uses.holes).enumerate()
        {
            let chords = part.holes.get(hole_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native materialized hole has no logical walk".into())
            })?;
            if hole.coords().len()
                != chords
                    .len()
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?
                || hole_locations.images.first() != hole_locations.images.last()
                || hole.coords().points().next() != hole.coords().points().next_back()
                || !validate_native_materialized_chord_row(
                    reference,
                    chords,
                    &hole_locations.images,
                    &port_records,
                    &candidate_points,
                    budget,
                )?
            {
                return Ok(None);
            }
        }
    }
    let Some(assignment) =
        native_candidate_edges_preserve_embedding(reference, candidate_points, budget)?
    else {
        return Ok(None);
    };
    Ok(Some(NativeCandidateState {
        port_records,
        assignment,
    }))
}
#[expect(
    clippy::suspicious_operation_groupings,
    reason = "NativeGraphCycle.edges stores the shell boundary; its shell field is a boolean role flag"
)]
pub(in crate::geometry::topology::q_embedding) fn validate_native_reference(
    reference: &NativeReference<'_>,
    budget: &mut impl Work,
) -> Result<()> {
    let graph = &reference.graph;
    let written_edge_count = reference
        .area_edges
        .len()
        .checked_add(reference.line_graph_edges.len())
        .ok_or(ResourceError::SizeOverflow)?;
    let walk_count = graph
        .topology
        .face_starts
        .len()
        .checked_sub(1)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("native reference has no face-start sentinel".into())
        })?;
    let face_count = usize::try_from(graph.faces.count).map_err(|_| ResourceError::SizeOverflow)?;
    if graph.faces.walk_faces.len() != walk_count
        || graph.written_edges.len() != written_edge_count
        || graph.parts.len() != reference.plan.parts.len()
        || reference.line_traces.len() != reference.plan.lines.len()
        || reference.literal_chord_uses.len() != reference.plan.chords.len()
        || reference.port_vertices.len() != reference.plan.ports.len()
        || graph.vertex_keys.len() != graph.points.exact.len()
        || graph.topology.component_of.len() != graph.points.exact.len()
        || graph.face_regions.len() != face_count
        || graph.face_groups.len() != face_count
        || graph.face_cells.len() != face_count
        || graph.face_source_uses.offsets.len()
            != face_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
        || graph.face_source_uses.offsets.first().copied() != Some(0)
        || graph.face_source_uses.offsets.last().copied()
            != Some(
                u32::try_from(graph.face_source_uses.uses.len())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )
        || graph.producer_uses.offsets.len()
            != graph
                .producer_uses
                .keys
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
        || graph.producer_uses.offsets.first().copied() != Some(0)
        || graph.producer_uses.offsets.last().copied()
            != Some(
                u32::try_from(graph.producer_uses.atom_uses.len())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )
    {
        return Err(GeometryErrorKind::Invalid(
            "native reference graph and literal plan have misaligned rows".into(),
        )
        .into());
    }
    for (port_index, port) in reference.plan.ports.iter().enumerate() {
        budget.charge_into_error(1)?;
        let vertex = *reference.port_vertices.get(port_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native literal port has no reference vertex".into())
        })?;
        if graph.vertex_keys.get(vertex as usize) != Some(&native_emission_port_key(port)) {
            return Err(GeometryErrorKind::Invalid(
                "native literal port does not map back to its reference chart vertex".into(),
            )
            .into());
        }
    }
    validate_native_written_chord_transport(reference, budget)?;
    for (face, &area_group) in graph.face_groups.iter().enumerate() {
        budget.charge_into_error(1)?;
        let group = area_group as usize;
        if group >= face_count
            || graph.face_groups.get(group).copied() != Some(area_group)
            || graph.face_regions.get(face) != graph.face_regions.get(group)
            || graph.face_cells.get(face) != graph.face_cells.get(group)
        {
            return Err(GeometryErrorKind::Invalid(
                "native area-face grouping is not canonical or reciprocal".into(),
            )
            .into());
        }
    }
    validate_native_graph_face_source_table(reference, budget)?;
    let frame = reference.plan.topology.sources.frame;
    validate_native_graph_sewing(graph, frame, budget)?;
    budget.charge_into_error(
        u64::try_from(written_edge_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut producer_written = vec![false; written_edge_count];
    for pair in graph.producer_uses.keys.array_windows::<2>() {
        budget.charge_into_error(1)?;
        if pair[0] >= pair[1] {
            return Err(GeometryErrorKind::Invalid(
                "native producer-use keys are not unique and ordered".into(),
            )
            .into());
        }
    }
    for (use_index, &key) in graph.producer_uses.keys.iter().enumerate() {
        budget.charge_into_error(1)?;
        let start = *graph.producer_uses.offsets.get(use_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native producer-use offset is unknown".into())
        })? as usize;
        let end = *graph
            .producer_uses
            .offsets
            .get(
                use_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native producer-use terminal offset is unknown".into())
            })? as usize;
        if start >= end {
            return Err(GeometryErrorKind::Invalid(
                "native producer use has no atom sequence".into(),
            )
            .into());
        }
        let mut interval = ExactRational::zero();
        for use_row in graph
            .producer_uses
            .atom_uses
            .get(start..end)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native producer-use atom range is incomplete".into())
            })?
        {
            budget.charge_into_error(1)?;
            let edge = native_reference_written_edge(reference, use_row.written_edge)?;
            let NativeAreaEdgeKind::Producer {
                recipe,
                trace_edge,
                producer_start,
                producer_end,
                ..
            } = &edge.kind
            else {
                return Err(GeometryErrorKind::Invalid(
                    "native producer-use map references a presentation edge".into(),
                )
                .into());
            };
            let mapping = graph
                .written_edges
                .get(use_row.written_edge)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native producer use has no written-edge mapping".into(),
                    )
                })?;
            if *recipe != key.recipe
                || *trace_edge != key.trace_edge
                || !producer_start.try_cmp_with_work(&interval, budget)?.is_eq()
                || !producer_start
                    .try_cmp_with_work(producer_end, budget)?
                    .is_lt()
                || !producer_start.try_eq_with_work(&use_row.producer_range.start, budget)?
                || !producer_end.try_eq_with_work(&use_row.producer_range.end, budget)?
                || mapping.atom != use_row.atom
                || mapping.reversed != use_row.reversed
            {
                return Err(GeometryErrorKind::Invalid(
                    "native ordered producer-use map lost its atom interval or sign".into(),
                )
                .into());
            }
            let seen = producer_written
                .get_mut(use_row.written_edge)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native producer use index is outside its edge table".into(),
                    )
                })?;
            if *seen {
                return Err(GeometryErrorKind::Invalid(
                    "native producer edge appears in more than one ordered-use row".into(),
                )
                .into());
            }
            *seen = true;
            interval = producer_end.try_clone_with_work(budget)?;
        }
        if !interval
            .try_cmp_with_work(&ExactRational::one(), budget)?
            .is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "native ordered producer-use map does not cover its producer leaf".into(),
            )
            .into());
        }
    }
    for written_edge in 0..written_edge_count {
        budget.charge_into_error(1)?;
        let edge = native_reference_written_edge(reference, written_edge)?;
        let mapped = producer_written.get(written_edge).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("native producer-use coverage row is unknown".into())
        })?;
        if matches!(&edge.kind, NativeAreaEdgeKind::Producer { .. }) != mapped {
            return Err(GeometryErrorKind::Invalid(
                "native producer edge is absent from its ordered atom-use map".into(),
            )
            .into());
        }
    }
    let provider_nodes = reference.plan.node_registry.nodes();
    budget.charge_into_error(
        u64::try_from(graph.vertex_keys.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    for (vertex_index, key) in graph.vertex_keys.iter().enumerate() {
        budget.charge_into_error(1)?;
        if graph.local_vertex(*key, budget)?
            != u32::try_from(vertex_index).map_err(|_| ResourceError::SizeOverflow)?
        {
            return Err(GeometryErrorKind::Invalid(
                "native chart provider-to-local map is not reciprocal".into(),
            )
            .into());
        }
        if provider_nodes.get(key.node.0 as usize).is_none() {
            return Err(GeometryErrorKind::Invalid(
                "native chart vertex has no explicit provider-node mapping".into(),
            )
            .into());
        }
        let component = *graph
            .topology
            .component_of
            .get(vertex_index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native chart vertex has no component".into())
            })? as usize;
        let host = *graph.faces.component_hosts.get(component).ok_or_else(|| {
            GeometryErrorKind::Invalid("native chart component has no host face".into())
        })?;
        if host as usize >= face_count {
            return Err(GeometryErrorKind::Invalid(
                "native chart component host is outside the face table".into(),
            )
            .into());
        }
    }
    budget.charge_into_error(
        u64::try_from(reference.pole_sectors.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    for selected in &reference.pole_sectors {
        budget.charge_into_error(1)?;
        let face = selected.sector.face();
        if reference
            .plan
            .selected_face_roots
            .get(face.0 as usize)
            .copied()
            != Some(Some(selected.region))
        {
            return Err(GeometryErrorKind::Invalid(
                "native pole sector lost its selected source-region germ".into(),
            )
            .into());
        }
    }
    budget.charge_into_error(
        u64::try_from(written_edge_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut contributor_seen = vec![false; written_edge_count];
    for (atom_index, atom) in graph.atoms.iter().enumerate() {
        for contributor in &atom.contributors {
            budget.charge_into_error(1)?;
            let seen = contributor_seen
                .get_mut(contributor.written_edge)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native graph atom contributor references an unknown written edge".into(),
                    )
                })?;
            if std::mem::replace(seen, true) {
                return Err(GeometryErrorKind::Invalid(
                    "native written edge contributes to multiple graph atoms".into(),
                )
                .into());
            }
            let edge = native_reference_written_edge(reference, contributor.written_edge)?;
            let expected_region = match edge.owner {
                NativeGraphEdgeOwner::Area { region } => Some(region),
                NativeGraphEdgeOwner::Line => None,
            };
            if (contributor.written_edge < reference.area_edges.len())
                != matches!(edge.owner, NativeGraphEdgeOwner::Area { .. })
            {
                return Err(GeometryErrorKind::Invalid(
                    "native writer ownership differs from its area/line range".into(),
                )
                .into());
            }
            let mapping = graph
                .written_edges
                .get(contributor.written_edge)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native graph atom contributor has no written-edge mapping".into(),
                    )
                })?;
            if mapping.atom as usize != atom_index
                || mapping.reversed != contributor.reversed
                || expected_region != contributor.region
            {
                return Err(GeometryErrorKind::Invalid(
                    "native graph atom changed a signed written-edge contributor".into(),
                )
                .into());
            }
        }
    }
    for (written_index, &seen) in contributor_seen.iter().enumerate() {
        budget.charge_into_error(1)?;
        if !seen {
            return Err(GeometryErrorKind::Invalid(
                "native written edge is absent from its graph atom contributors".into(),
            )
            .into());
        }
        graph.written_edges.get(written_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native written-edge mapping is incomplete".into())
        })?;
    }
    if reference.plan.whole_sphere {
        validate_native_whole_sphere_cover(reference, budget)?;
    }
    for (part, reference_part) in reference.plan.parts.iter().zip(&graph.parts) {
        budget.charge_into_error(1)?;
        if part.face != reference_part.face || part.region != reference_part.region {
            return Err(GeometryErrorKind::Invalid(
                "native literal part changed its frozen face or selected region".into(),
            )
            .into());
        }
        let shell = graph.cycles.get(reference_part.shell).ok_or_else(|| {
            GeometryErrorKind::Invalid("native reference shell cycle is unknown".into())
        })?;
        if part.shell.len() != shell.edges.len() || part.holes.len() != reference_part.holes.len() {
            return Err(GeometryErrorKind::Invalid(
                "native literal part differs from its fixed graph walks".into(),
            )
            .into());
        }
        for (hole, &cycle_index) in part.holes.iter().zip(&reference_part.holes) {
            budget.charge_into_error(1)?;
            let cycle = graph.cycles.get(cycle_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native reference hole cycle is unknown".into())
            })?;
            if hole.len() != cycle.edges.len() {
                return Err(GeometryErrorKind::Invalid(
                    "native literal hole differs from its fixed graph walk".into(),
                )
                .into());
            }
        }
    }
    Ok(())
}
