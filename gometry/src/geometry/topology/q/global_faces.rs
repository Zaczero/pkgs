use super::{
    ChartPole, ExactChartPoint, ExactGeographicChartInventory, ExactRational, ExactSign,
    FallibleAvl, GeographicFrame, GeometryErrorKind, GlobalFaceId, GlobalFaceStructure,
    GlobalFaceTopology, HostGerm, HostQuery, HostQueryTarget, HostRoot, Ordering, QComponentId,
    QCutEvent, QCutIndex, QDart, QLocalFace, QLocalFaceId, QNodeId, QPort, QuotientFaces,
    ROOT_CUT_FACE_SIDE, ResourceError, Result, RingTransition, SeedRaySpan, SourceEdgeId,
    SourceRingId, SourceRowId, SpanOccurrence, SweepEvent, SweepEventKind, UnionFind,
    apply_cut_event_winding, apply_ring_transition, compare_span_to_query, compare_spans_at,
    face_in_direction, for_each_span_ring_transition, resolve_host_queries,
    span_has_ring_transition, try_sort_by,
};

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct SeedFaceGerm {
    pub(in crate::geometry::topology::q) local_face: QLocalFaceId,
    pub(in crate::geometry::topology::q) germ: HostGerm,
}

pub(in crate::geometry::topology::q) fn select_seed_face_germ(
    topology: &QuotientFaces,
) -> Result<Option<SeedFaceGerm>> {
    for (span_index, span) in topology.spans.iter().enumerate() {
        if !span_has_ring_transition(span)?
            || topology
                .span_darts
                .get(span_index)
                .copied()
                .flatten()
                .is_none()
        {
            continue;
        }
        let start = topology
            .ports
            .get(span.ports[0].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q seed span has no first port".into()))?;
        let end = topology
            .ports
            .get(span.ports[1].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q seed span has no second port".into()))?;
        let dx = end.point.x.try_subtract(&start.point.x)?;
        let dy = end.point.y.try_subtract(&start.point.y)?;
        if dx.is_zero() && dy.is_zero() {
            return Err(GeometryErrorKind::Invalid("Q seed span is degenerate".into()).into());
        }
        let point = ExactChartPoint::from_rationals(
            start.point.x.try_midpoint(&end.point.x)?,
            start.point.y.try_midpoint(&end.point.y)?,
        );
        let left_face = topology
            .span_local_faces
            .get(span_index)
            .and_then(|faces| *faces)
            .map(|faces| faces[0])
            .ok_or_else(|| GeometryErrorKind::Invalid("Q seed span has no left face".into()))?;
        return Ok(Some(SeedFaceGerm {
            local_face: left_face,
            germ: HostGerm {
                point,
                dx: ExactRational::zero().try_subtract(&dy)?,
                dy: dx,
            },
        }));
    }
    Ok(None)
}

pub(in crate::geometry::topology::q) fn node_has_edges(
    topology: &QuotientFaces,
    node: QNodeId,
) -> Result<bool> {
    let index = node.0 as usize;
    let next = index.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    let start = topology
        .outgoing_offsets
        .get(index)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no fan offset".into()))?;
    let end = topology
        .outgoing_offsets
        .get(next)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no fan end".into()))?;
    Ok(start != end)
}

pub(in crate::geometry::topology::q) fn north_pole_ring_windings(
    sources: &ExactGeographicChartInventory,
    cut: &ExactRational,
) -> Result<Vec<i64>> {
    let frame = sources.frame;
    let axis = frame.longitude();
    let north_pole = frame.chart_pole_latitude(ChartPole::North);
    let period = ExactRational::from_f64(axis.period());
    let mut windings = Vec::with_capacity(sources.rings.len());
    for ring in &sources.rings {
        if matches!(ring.summary.domain, crate::geographic::RingDomain::SeamOnly) {
            windings.push(0);
            continue;
        }
        let mut winding = if ring.summary.bounded_pole() == Some(ChartPole::North) {
            ring.summary.chart_turn
        } else {
            0
        };
        if ring.summary.north.get().to_bits() == north_pole.to_bits() {
            let edges = sources
                .edges
                .get(ring.source_edges.clone())
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source ring has an invalid edge range".into())
                })?;
            for edge in edges {
                let [start, end] = edge.source;
                if crate::geographic::Latitude::from_admitted(frame, start.y)
                    .get()
                    .to_bits()
                    != north_pole.to_bits()
                    || crate::geographic::Latitude::from_admitted(frame, end.y)
                        .get()
                        .to_bits()
                        != north_pole.to_bits()
                {
                    continue;
                }
                let local_edge = super::super::exact::local_longitude_edge(
                    frame,
                    start.x,
                    edge.retained_longitude(),
                );
                let start_x = local_edge.start_x;
                let delta = local_edge.delta_x(frame.longitude().period())?;
                if delta.is_zero() {
                    continue;
                }
                let start_x = ExactRational::from_f64(start_x);
                let delta = ExactRational::from_dyadic(delta);
                let end_x = start_x.try_add(&delta)?;
                let (low, high) = if start_x.try_cmp(&end_x)?.is_lt() {
                    (&start_x, &end_x)
                } else {
                    (&end_x, &start_x)
                };

                // `local_longitude_edge` starts in [-P/2, P/2], except that a
                // positive seam spelling may use +P/2; its delta is at most P.
                // The cut is canonical in [-P/2, P/2), so only k=-1, 0, 1
                // can put a lattice meridian strictly inside this interval.
                // Its width is at most P, and strict endpoints leave at most
                // one such meridian.
                let mut crosses_cut = false;
                for offset in -1_i8..=1 {
                    let lattice = cut.try_add(
                        &period.try_product(&ExactRational::from_f64(f64::from(offset)))?,
                    )?;
                    if low.try_cmp(&lattice)?.is_lt() && lattice.try_cmp(high)?.is_lt() {
                        crosses_cut = true;
                        break;
                    }
                }
                if crosses_cut {
                    let delta_winding = if delta.sign() == ExactSign::Negative {
                        1
                    } else {
                        -1
                    };
                    winding = winding
                        .checked_add(delta_winding)
                        .ok_or(ResourceError::SizeOverflow)?;
                }
            }
        }
        windings.push(winding);
    }
    Ok(windings)
}

pub(in crate::geometry::topology::q) fn apply_span_ring_transitions(
    winding: &mut [i64],
    owners: &[QSpanOwner],
    multiplier: i64,
) -> Result<()> {
    for_each_span_ring_transition(owners, &mut |transition| {
        apply_ring_transition(winding, transition, multiplier)
    })
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "component, port, face, and status IDs index the dense Q columns that construct them"
)]
pub(in crate::geometry::topology) fn global_face_structure(
    frame: GeographicFrame,
    topology: &QuotientFaces,
) -> Result<GlobalFaceStructure> {
    let component_count = topology.cut_index.component_runs.len();
    let mut edge_component = vec![false; component_count];
    for face in &topology.local_faces {
        edge_component[face.component.0 as usize] = true;
    }
    let mut roots = vec![None; component_count];
    for component in 0..component_count {
        if !edge_component[component] {
            continue;
        }
        let component_id =
            QComponentId(u32::try_from(component).map_err(|_| ResourceError::SizeOverflow)?);
        let root = if let Some(port) = topology.cut_index.root_ports[component] {
            let port_data = &topology.ports[port.0 as usize];
            let face = if port_data.cut {
                topology.cut_index.local_face_at(
                    component_id,
                    &port_data.point.y,
                    ROOT_CUT_FACE_SIDE,
                )?
            } else {
                face_in_direction(
                    topology.face_chart(),
                    &topology.darts,
                    &topology.outgoing,
                    &topology.outgoing_offsets,
                    &topology.dart_faces,
                    port_data.node,
                    &(ExactRational::from_f64(-1.0), ExactRational::zero()),
                )?
            };
            HostRoot {
                component: component_id,
                face,
                port: (!port_data.cut).then_some(port),
                point: port_data.point.clone(),
            }
        } else {
            let latitude = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South));
            HostRoot {
                component: component_id,
                face: topology
                    .cut_index
                    .local_face_at(component_id, &latitude, 1)?,
                port: None,
                point: ExactChartPoint::from_rationals(topology.cut.clone(), latitude),
            }
        };
        roots[component] = Some(root);
    }

    let mut reference: Option<QComponentId> = None;
    for root in roots.iter().flatten() {
        let replace = if let Some(previous) = reference {
            let previous = roots[previous.0 as usize]
                .as_ref()
                .expect("the reference is an edge-bearing component");
            root.point
                .compare_lex(&previous.point)?
                .then_with(|| root.component.cmp(&previous.component))
                .is_lt()
        } else {
            true
        };
        if replace {
            reference = Some(root.component);
        }
    }
    if reference.is_none() {
        reference = edge_component
            .iter()
            .position(|&is_edge| is_edge)
            .map(|index| QComponentId(u32::try_from(index).expect("component IDs were admitted")));
    }
    let Some(reference) = reference else {
        let span_global_faces = vec![None; topology.spans.len()];
        return Ok(GlobalFaceStructure {
            local_to_global: Vec::new(),
            span_global_faces,
            component_hosts: vec![None; component_count],
            cut_interval_faces: vec![GlobalFaceId(0); topology.cut_index.intervals.len()],
            source_node_faces: vec![Some(GlobalFaceId(0)); topology.nodes.len()],
            count: 1,
            seed: None,
            seed_ray_spans: Vec::new(),
            #[cfg(test)]
            host_status_comparisons: 0,
            #[cfg(test)]
            host_status_max_height: 0,
        });
    };

    let mut host_faces = vec![None; component_count];
    let mut queries = Vec::new();
    let mut isolated_node_host_by_node = vec![None; topology.nodes.len()];
    let seed = select_seed_face_germ(topology)?;
    let mut seed_ray_spans = Vec::new();
    for (component, root) in roots.iter().enumerate() {
        let Some(root) = root else {
            continue;
        };
        if root.component == reference {
            continue;
        }
        if let Some(port) = root.port {
            queries.push(HostQuery {
                component: root.component,
                target: HostQueryTarget::Component(root.component),
                germ: HostGerm {
                    point: topology.ports[port.0 as usize].point.clone(),
                    dx: ExactRational::zero(),
                    dy: ExactRational::zero().try_subtract(&ExactRational::one())?,
                },
            });
        } else {
            let host = topology.cut_index.host_face_on_cut(
                root.component,
                reference,
                &root.point.y,
                ROOT_CUT_FACE_SIDE,
            )?;
            if topology.local_faces[host.0 as usize].component == root.component {
                return Err(GeometryErrorKind::Invalid(
                    "a pole-only Q component selected itself as its host".into(),
                )
                .into());
            }
            host_faces[component] = Some(host);
        }
    }
    for node in &topology.nodes {
        if node_has_edges(topology, node.id)? || node.pole.is_some() {
            continue;
        }
        let port_id = topology
            .node_ports
            .get(node.ports.start)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("isolated Q node has no chart port".into())
            })?;
        let port = topology.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("isolated Q node has an unknown chart port".into())
        })?;
        queries.push(HostQuery {
            component: topology.node_components[node.id.0 as usize],
            target: HostQueryTarget::IsolatedNode(node.id),
            germ: HostGerm {
                point: port.point.clone(),
                dx: ExactRational::zero(),
                dy: ExactRational::one(),
            },
        });
    }
    let mut events = Vec::new();
    let event_capacity = topology
        .spans
        .len()
        .checked_mul(2)
        .and_then(|count| count.checked_add(queries.len()))
        .and_then(|count| count.checked_add(usize::from(seed.is_some())))
        .ok_or(ResourceError::SizeOverflow)?;
    events.reserve(event_capacity);
    for (span_id, span) in topology.spans.iter().enumerate() {
        if topology.span_darts[span_id].is_none() {
            continue;
        }
        let start = &topology.ports[span.ports[0].0 as usize].point;
        let end = &topology.ports[span.ports[1].0 as usize].point;
        match start.y.try_cmp(&end.y)? {
            Ordering::Less => {
                events.push(SweepEvent {
                    y: start.y.clone(),
                    kind: SweepEventKind::Start(span_id),
                });
                events.push(SweepEvent {
                    y: end.y.clone(),
                    kind: SweepEventKind::End(span_id),
                });
            },
            Ordering::Greater => {
                events.push(SweepEvent {
                    y: end.y.clone(),
                    kind: SweepEventKind::Start(span_id),
                });
                events.push(SweepEvent {
                    y: start.y.clone(),
                    kind: SweepEventKind::End(span_id),
                });
            },
            Ordering::Equal => {},
        }
    }
    for (query, root) in queries.iter().enumerate() {
        events.push(SweepEvent {
            y: root.germ.point.y.clone(),
            kind: SweepEventKind::Query(query),
        });
    }
    if let Some(seed) = &seed {
        events.push(SweepEvent {
            y: seed.germ.point.y.clone(),
            kind: SweepEventKind::Seed,
        });
    }
    try_sort_by(&mut events, |left, right| left.y.try_cmp(&right.y))?;
    let status_capacity = topology
        .span_darts
        .iter()
        .filter(|span| span.is_some())
        .count();
    let mut status = FallibleAvl::with_capacity(status_capacity);
    let mut ends = Vec::new();
    let mut starts = Vec::new();
    let mut queries_below = Vec::new();
    let mut query_events = Vec::new();
    #[cfg(test)]
    let mut host_status_comparisons = 0_usize;
    #[cfg(test)]
    let mut host_status_max_height = 0_usize;
    let mut root_face_by_component = vec![None; component_count];
    let mut host_face_by_component = host_faces;
    for root in roots.iter().flatten() {
        root_face_by_component[root.component.0 as usize] = Some(root.face);
    }
    let mut cursor = 0_usize;
    while cursor < events.len() {
        let event_y = events[cursor].y.clone();
        let group_start = cursor;
        while cursor < events.len() && events[cursor].y.try_cmp(&event_y)?.is_eq() {
            cursor += 1;
        }
        ends.clear();
        starts.clear();
        queries_below.clear();
        query_events.clear();
        let seed_event = events[group_start..cursor]
            .iter()
            .any(|event| matches!(event.kind, SweepEventKind::Seed));
        for event in &events[group_start..cursor] {
            match event.kind {
                SweepEventKind::End(span) => ends.push(span),
                SweepEventKind::Start(span) => starts.push(span),
                SweepEventKind::Query(query) => {
                    let query_data = queries.get(query).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q sweep has an unknown host query".into())
                    })?;
                    if query_data.germ.dy.try_cmp(&ExactRational::zero())?.is_lt() {
                        queries_below.push(query);
                    } else {
                        query_events.push(query);
                    }
                },
                SweepEventKind::Seed => {},
            }
        }
        {
            let mut observe_comparison = || {
                #[cfg(test)]
                {
                    host_status_comparisons = host_status_comparisons.saturating_add(1);
                }
            };
            resolve_host_queries(
                &queries_below,
                &queries,
                reference,
                topology,
                &status,
                &mut host_face_by_component,
                &mut isolated_node_host_by_node,
                &mut observe_comparison,
            )?;
        }
        for &span in &ends {
            let mut compare_below = |left: &usize, right: &usize| {
                #[cfg(test)]
                {
                    host_status_comparisons = host_status_comparisons.saturating_add(1);
                }
                compare_spans_at(
                    &topology.ports,
                    &topology.spans,
                    *left,
                    *right,
                    &event_y,
                    -1,
                )
            };
            if !status.remove(&span, &mut compare_below)? {
                return Err(GeometryErrorKind::Invalid(
                    "the exact Q host sweep ended an inactive span".into(),
                )
                .into());
            }
            #[cfg(test)]
            {
                host_status_max_height = host_status_max_height.max(status.tree_height());
            }
        }
        try_sort_by(&mut starts, |left, right| {
            #[cfg(test)]
            {
                host_status_comparisons = host_status_comparisons.saturating_add(1);
            }
            compare_spans_at(&topology.ports, &topology.spans, *left, *right, &event_y, 1)
        })?;
        for &span in &starts {
            let mut compare_above = |left: &usize, right: &usize| {
                #[cfg(test)]
                {
                    host_status_comparisons = host_status_comparisons.saturating_add(1);
                }
                compare_spans_at(&topology.ports, &topology.spans, *left, *right, &event_y, 1)
            };
            if !status.insert(span, &mut compare_above)? {
                return Err(GeometryErrorKind::Invalid(
                    "the exact Q host sweep started an active span twice".into(),
                )
                .into());
            }
            #[cfg(test)]
            {
                host_status_max_height = host_status_max_height.max(status.tree_height());
            }
        }
        starts.clear();
        {
            let mut observe_comparison = || {
                #[cfg(test)]
                {
                    host_status_comparisons = host_status_comparisons.saturating_add(1);
                }
            };
            resolve_host_queries(
                &query_events,
                &queries,
                reference,
                topology,
                &status,
                &mut host_face_by_component,
                &mut isolated_node_host_by_node,
                &mut observe_comparison,
            )?;
        }
        ends.clear();
        query_events.clear();
        if seed_event {
            let seed = seed
                .as_ref()
                .ok_or_else(|| GeometryErrorKind::Invalid("Q sweep lost its seed germ".into()))?;
            status.for_each(&mut |span_id| {
                let span = topology.spans.get(*span_id).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q seed sweep has an unknown span".into())
                })?;
                let order = {
                    #[cfg(test)]
                    {
                        host_status_comparisons = host_status_comparisons.saturating_add(1);
                    }
                    compare_span_to_query(&topology.ports, &topology.spans, *span_id, &seed.germ)?
                };
                if order.is_eq() {
                    return Err(GeometryErrorKind::Invalid(
                        "the exact Q seed germ lies on an unmerged span".into(),
                    )
                    .into());
                }
                if order.is_gt() {
                    let start = topology
                        .ports
                        .get(span.ports[0].0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q seed span has no first port".into())
                        })?;
                    let end = topology
                        .ports
                        .get(span.ports[1].0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q seed span has no second port".into())
                        })?;
                    let vertical_order = start.point.y.try_cmp(&end.point.y)?;
                    if vertical_order.is_eq() {
                        return Err(GeometryErrorKind::Invalid(
                            "a horizontal Q span entered the exact seed status".into(),
                        )
                        .into());
                    }
                    let multiplier = if vertical_order.is_lt() { -1 } else { 1 };
                    seed_ray_spans.push(SeedRaySpan {
                        span: *span_id,
                        multiplier,
                    });
                }
                Ok(())
            })?;
        }
    }

    let edge_component_count = edge_component.iter().filter(|&&edge| edge).count();
    let mut face_union = UnionFind::new(topology.local_faces.len());
    let mut links = 0_usize;
    for (component, root_face) in root_face_by_component.iter().enumerate() {
        if !edge_component[component] || reference.0 as usize == component {
            continue;
        }
        let root_face = root_face.ok_or_else(|| {
            GeometryErrorKind::Invalid("an edge component has no local root face".into())
        })?;
        let host_face = host_face_by_component[component].ok_or_else(|| {
            GeometryErrorKind::Invalid("an edge component has no exact host face".into())
        })?;
        let root_component = topology.local_faces[root_face.0 as usize].component;
        let host_component = topology.local_faces[host_face.0 as usize].component;
        if root_component.0 as usize != component || host_component == root_component {
            return Err(GeometryErrorKind::Invalid(
                "a Q host link does not connect distinct edge components".into(),
            )
            .into());
        }
        let root = face_union.find(root_face.0 as usize);
        let host = face_union.find(host_face.0 as usize);
        if root == host {
            return Err(GeometryErrorKind::Invalid(
                "Q component host links contain a cycle".into(),
            )
            .into());
        }
        face_union.union(root, host);
        links = links.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    }
    let expected_links = edge_component_count
        .checked_sub(1)
        .ok_or(ResourceError::SizeOverflow)?;
    if links != expected_links {
        return Err(GeometryErrorKind::Invalid(
            "the Q host sweep did not link every edge component".into(),
        )
        .into());
    }

    let mut root_to_global = vec![None; topology.local_faces.len()];
    let mut local_to_global = Vec::with_capacity(topology.local_faces.len());
    let mut global_face_count = 0_usize;
    for local in 0..topology.local_faces.len() {
        let root = face_union.find(local);
        let global = if let Some(global) = root_to_global[root] {
            global
        } else {
            let global = GlobalFaceId(
                u32::try_from(global_face_count).map_err(|_| ResourceError::SizeOverflow)?,
            );
            root_to_global[root] = Some(global);
            global_face_count = global_face_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            global
        };
        local_to_global.push(global);
    }
    let count = global_face_count;
    let span_global_faces = topology
        .span_local_faces
        .iter()
        .map(|faces| {
            faces
                .map(|faces| {
                    Ok([
                        local_to_global[faces[0].0 as usize],
                        local_to_global[faces[1].0 as usize],
                    ])
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    let mut component_hosts = vec![None; component_count];
    for (component, root) in root_face_by_component.iter().enumerate() {
        if let Some(root) = root {
            component_hosts[component] = Some(local_to_global[root.0 as usize]);
        }
    }
    let cut_interval_faces = topology
        .cut_index
        .validate_global_intervals(&local_to_global, reference)?;
    let mut source_node_faces = Vec::with_capacity(topology.nodes.len());
    for node in &topology.nodes {
        if node_has_edges(topology, node.id)? {
            source_node_faces.push(None);
            continue;
        }
        let local = if let Some(pole) = node.pole {
            let interval = topology.cut_index.pole_interval_id(pole)?;
            *cut_interval_faces.get(interval.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q pole has no cut-interval host".into())
            })?
        } else {
            let local = isolated_node_host_by_node
                .get(node.id.0 as usize)
                .and_then(|face| *face)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("an isolated Q node has no exact host face".into())
                })?;
            *local_to_global.get(local.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q node host is unknown".into())
            })?
        };
        source_node_faces.push(Some(local));
    }
    validate_global_euler(topology, edge_component_count, count)?;
    Ok(GlobalFaceStructure {
        local_to_global,
        span_global_faces,
        component_hosts,
        cut_interval_faces,
        source_node_faces,
        count: count.max(1),
        seed,
        seed_ray_spans,
        #[cfg(test)]
        host_status_comparisons,
        #[cfg(test)]
        host_status_max_height,
    })
}

pub(in crate::geometry::topology::q) fn build_global_face_topology(
    frame: GeographicFrame,
    sources: &ExactGeographicChartInventory,
    topology: &QuotientFaces,
) -> Result<GlobalFaceTopology> {
    let structure = global_face_structure(frame, topology)?;
    if structure.local_to_global.is_empty() {
        return Ok(GlobalFaceTopology {
            local_to_global: structure.local_to_global,
            span_global_faces: structure.span_global_faces,
            component_hosts: structure.component_hosts,
            cut_interval_faces: structure.cut_interval_faces,
            source_node_faces: structure.source_node_faces,
            source_point_faces: vec![Some(GlobalFaceId(0)); sources.points.len()],
            seed_face: None,
            seed_winding: Vec::new(),
            count: structure.count,
            #[cfg(test)]
            host_status_comparisons: structure.host_status_comparisons,
            #[cfg(test)]
            host_status_max_height: structure.host_status_max_height,
        });
    }

    let mut seed_winding = north_pole_ring_windings(sources, &topology.cut)?;
    if let Some(seed) = &structure.seed {
        for event in topology.cut_index.events.iter().rev() {
            if event.y.try_cmp(&seed.germ.point.y)?.is_gt() {
                apply_cut_event_winding(topology, event.node, &mut seed_winding)?;
            }
        }
        let mut seed_ray_winding = vec![0_i64; sources.rings.len()];
        for crossing in &structure.seed_ray_spans {
            let span = topology.spans.get(crossing.span).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q seed sweep has an unknown span".into())
            })?;
            apply_span_ring_transitions(&mut seed_ray_winding, &span.owners, crossing.multiplier)?;
        }
        for (value, delta) in seed_winding.iter_mut().zip(seed_ray_winding) {
            *value = value
                .checked_add(delta)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    let seed_face = if let Some(seed) = &structure.seed {
        Some(
            *structure
                .local_to_global
                .get(seed.local_face.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q seed face is outside its topology".into())
                })?,
        )
    } else {
        seed_winding
            .iter()
            .any(|&winding| winding != 0)
            .then_some(GlobalFaceId(0))
    };
    if topology.source_point_ports.len() != sources.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-point ports do not match the source inventory".into(),
        )
        .into());
    }
    let mut source_point_faces = Vec::with_capacity(sources.points.len());
    for &port_id in &topology.source_point_ports {
        let port = topology.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q source point has an unknown port".into())
        })?;
        let node_index = port.node.0 as usize;
        let node = topology.nodes.get(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q source point has an unknown node".into())
        })?;
        if node_has_edges(topology, node.id)? {
            source_point_faces.push(None);
            continue;
        }
        source_point_faces.push(Some(
            *structure
                .source_node_faces
                .get(node_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source point has no node host entry".into())
                })?
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("an isolated Q source point has no host face".into())
                })?,
        ));
    }
    Ok(GlobalFaceTopology {
        local_to_global: structure.local_to_global,
        span_global_faces: structure.span_global_faces,
        component_hosts: structure.component_hosts,
        cut_interval_faces: structure.cut_interval_faces,
        source_node_faces: structure.source_node_faces,
        source_point_faces,
        seed_face,
        seed_winding,
        count: structure.count,
        #[cfg(test)]
        host_status_comparisons: structure.host_status_comparisons,
        #[cfg(test)]
        host_status_max_height: structure.host_status_max_height,
    })
}

pub(in crate::geometry::topology::q) fn validate_global_euler(
    topology: &QuotientFaces,
    edge_component_count: usize,
    global_face_count: usize,
) -> Result<()> {
    let vertices = topology
        .outgoing_offsets
        .windows(2)
        .filter(|window| window.first() != window.get(1))
        .count();
    let edges = topology
        .span_darts
        .iter()
        .filter(|dart| dart.is_some())
        .count();
    let characteristic = vertices
        .checked_add(global_face_count)
        .and_then(|value| value.checked_sub(edges));
    let expected = edge_component_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    if characteristic != Some(expected) {
        return Err(GeometryErrorKind::Invalid(
            "global Q faces do not satisfy the spherical Euler relation".into(),
        )
        .into());
    }
    Ok(())
}
