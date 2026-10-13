use super::*;

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q) struct CollapsedShellCandidate {
    pub(in crate::geometry::topology::q) row: SourceRowId,
    pub(in crate::geometry::topology::q) polygon: SourcePolygonId,
    pub(in crate::geometry::topology::q) holes_before: usize,
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q) struct CollapsedRingPointCandidate {
    pub(in crate::geometry::topology::q) node: QNodeId,
    pub(in crate::geometry::topology::q) row: SourceRowId,
    pub(in crate::geometry::topology::q) polygon: SourcePolygonId,
    pub(in crate::geometry::topology::q) trimmed: bool,
}

pub(in crate::geometry::topology::q) fn collapsed_ring_node(
    ring: &super::super::exact::ExactSourceRing,
    topology: &QuotientFaces,
) -> Result<Option<QNodeId>> {
    if ring.kind != RingKind::Collapsed {
        return Ok(None);
    }
    let mut node = None;
    for edge_index in ring.source_edges.clone() {
        let ports = topology.source_edge_ports.get(edge_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("collapsed ring has no source edge ports".into())
        })?;
        for port_id in ports {
            let endpoint_node = topology
                .ports
                .get(port_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("collapsed ring has an unknown Q port".into())
                })?
                .node;
            if node.is_some_and(|node| node != endpoint_node) {
                return Ok(None);
            }
            node = Some(endpoint_node);
        }
    }
    Ok(node)
}

pub(in crate::geometry::topology::q) fn collapsed_point_candidates(
    sources: &ExactGeographicChartInventory,
    topology: &QuotientFaces,
    global: &GlobalFaceTopology,
    face_count: usize,
) -> Result<(Vec<CollapsedRingPointCandidate>, CollapsedPointFaces)> {
    let mut collapsed_hole_nodes = Vec::<(SourcePolygonId, QNodeId)>::new();
    for ring in sources
        .rings
        .iter()
        .filter(|ring| ring.hole && ring.kind == RingKind::Collapsed)
    {
        for edge_index in ring.source_edges.clone() {
            let ports = topology.source_edge_ports.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("collapsed hole has no source edge ports".into())
            })?;
            for port_id in ports {
                let node = topology
                    .ports
                    .get(port_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("collapsed hole has an unknown Q port".into())
                    })?
                    .node;
                collapsed_hole_nodes.push((ring.polygon, node));
            }
        }
    }
    for span in &topology.spans {
        if span.nodes[0] != span.nodes[1] {
            continue;
        }
        for owner in &span.owners {
            let edge = sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q collapsed span has an unknown source edge".into())
                })?;
            if let SourceEdgeKind::Ring {
                polygon,
                hole: true,
                kind: RingKind::Collapsed,
                ..
            } = edge.kind
            {
                collapsed_hole_nodes.push((polygon, span.nodes[0]));
            }
        }
    }
    collapsed_hole_nodes.sort_unstable();
    collapsed_hole_nodes.dedup();

    let mut candidates = Vec::<CollapsedRingPointCandidate>::new();
    for (polygon_index, polygon) in sources.polygons.iter().enumerate() {
        let Some(shell) = sources.rings.get(polygon.rings.start) else {
            continue;
        };
        if let Some(node) = collapsed_ring_node(shell, topology)? {
            candidates.push(CollapsedRingPointCandidate {
                node,
                row: polygon.row,
                polygon: SourcePolygonId(
                    u32::try_from(polygon_index).map_err(|_| ResourceError::SizeOverflow)?,
                ),
                trimmed: false,
            });
        }
    }
    for span in &topology.spans {
        if span.nodes[0] != span.nodes[1] {
            continue;
        }
        for owner in &span.owners {
            let edge = sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q collapsed span has an unknown source edge".into())
                })?;
            if let SourceEdgeKind::Ring {
                polygon,
                hole: false,
                kind: RingKind::Collapsed,
                ..
            } = edge.kind
            {
                candidates.push(CollapsedRingPointCandidate {
                    node: span.nodes[0],
                    row: edge.row,
                    polygon,
                    trimmed: false,
                });
            }
        }
    }
    candidates.sort_unstable_by_key(|candidate| (candidate.node, candidate.polygon));
    candidates.dedup_by_key(|candidate| (candidate.node, candidate.polygon));
    candidates.retain(|candidate| {
        collapsed_hole_nodes
            .binary_search(&(candidate.polygon, candidate.node))
            .is_err()
    });

    let mut face_candidates = Vec::<(usize, usize)>::new();
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        let node_index = candidate.node.0 as usize;
        let start = *topology.outgoing_offsets.get(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("collapsed ring point has no fan start".into())
        })?;
        let end = *topology
            .outgoing_offsets
            .get(
                node_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("collapsed ring point has no fan end".into())
            })?;
        if start == end {
            let face = global
                .source_node_faces
                .get(node_index)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "isolated collapsed ring point has no global host face".into(),
                    )
                })?;
            face_candidates.push((face.0 as usize, candidate_index));
            continue;
        }
        let fan = topology.outgoing.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("collapsed ring point has an invalid fan".into())
        })?;
        for &dart_id in fan {
            let local = *topology.dart_faces.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("collapsed ring point fan has no local face".into())
            })?;
            let face = *global
                .local_to_global
                .get(local.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("collapsed ring point face has no global ID".into())
                })?;
            face_candidates.push((face.0 as usize, candidate_index));
        }
    }
    let face_candidates = CollapsedPointFaces::build(face_count, &mut face_candidates)?;
    Ok((candidates, face_candidates))
}

pub(in crate::geometry::topology::q) fn trim_collapsed_points_on_face(
    face: GlobalFaceId,
    face_candidates: &CollapsedPointFaces,
    candidates: &mut [CollapsedRingPointCandidate],
    state: &FaceOccupancyState,
) -> Result<()> {
    let candidates_for_face = face_candidates
        .row(face.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("collapsed point has no face index".into()))?;
    for &candidate_index in candidates_for_face {
        let candidate = candidates.get_mut(candidate_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("collapsed point face index is unknown".into())
        })?;
        let holes = state
            .polygon_holes
            .get(candidate.polygon.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("collapsed point polygon is unknown".into())
            })?;
        if *holes != 0 {
            candidate.trimmed = true;
        }
    }
    Ok(())
}

pub(in crate::geometry::topology::q) fn append_explicit_node_point_support(
    sources: &ExactGeographicChartInventory,
    topology: &QuotientFaces,
    memberships: &mut Vec<(usize, SourceRowId)>,
) -> Result<()> {
    if topology.source_point_ports.len() != sources.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-point ports do not match the source inventory".into(),
        )
        .into());
    }
    for (source, &port_id) in sources.points.iter().zip(&topology.source_point_ports) {
        let port = topology.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q source point has an unknown port".into())
        })?;
        memberships.push((port.node.0 as usize, source.row));
    }
    for span in &topology.spans {
        if span.nodes[0] != span.nodes[1] {
            continue;
        }
        for owner in &span.owners {
            let edge = sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q collapsed span has an unknown source edge".into())
                })?;
            if matches!(edge.kind, SourceEdgeKind::Line { .. }) {
                memberships.push((span.nodes[0].0 as usize, edge.row));
            }
        }
    }
    Ok(())
}

pub(in crate::geometry::topology::q) fn span_has_collapsed_shell(
    span: &ChartSpan,
    sources: &ExactGeographicChartInventory,
) -> Result<bool> {
    for owner in &span.owners {
        let edge = sources
            .edges
            .get(owner.source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has an unknown source edge".into())
            })?;
        if matches!(edge.kind, SourceEdgeKind::Ring {
            hole: false,
            kind: RingKind::Collapsed,
            ..
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(in crate::geometry::topology::q) fn collect_collapsed_shell_candidates(
    span: &ChartSpan,
    sources: &ExactGeographicChartInventory,
    state: Option<&FaceOccupancyState>,
    shells: &mut Vec<(SourceRowId, SourcePolygonId)>,
    holes: &mut Vec<(SourceRowId, SourcePolygonId)>,
    candidates: &mut Vec<CollapsedShellCandidate>,
) -> Result<()> {
    shells.clear();
    holes.clear();
    candidates.clear();
    for owner in &span.owners {
        let edge = sources
            .edges
            .get(owner.source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has an unknown source edge".into())
            })?;
        if let SourceEdgeKind::Ring {
            polygon,
            hole,
            kind: RingKind::Collapsed,
            ..
        } = edge.kind
        {
            let membership = (edge.row, polygon);
            if hole {
                holes.push(membership);
            } else {
                shells.push(membership);
            }
        }
    }
    shells.sort_unstable();
    shells.dedup();
    holes.sort_unstable();
    holes.dedup();
    for &(row, polygon) in shells.iter() {
        if holes.binary_search(&(row, polygon)).is_ok() {
            continue;
        }
        let holes_before = if let Some(state) = state {
            *state.polygon_holes.get(polygon.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "collapsed shell references an unknown polygon occupancy".into(),
                )
            })?
        } else {
            0
        };
        candidates.push(CollapsedShellCandidate {
            row,
            polygon,
            holes_before,
        });
    }
    Ok(())
}
