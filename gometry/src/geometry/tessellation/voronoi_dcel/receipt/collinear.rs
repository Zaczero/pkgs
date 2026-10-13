//! Receipt construction for the analytic collinear producer.

use super::{
    EmbeddedPolygon, ExactSign, NormalizedClip, PointKey, PointSelectionChart, Result, Site,
    VoronoiAssignmentReceipt, VoronoiCellReceipt, VoronoiClipAtomReceipt, VoronoiDartReceipt,
    VoronoiDiagramReceipt, VoronoiDirectedEdgeUse, VoronoiEdgeOrigin, VoronoiEdgeReceipt,
    VoronoiProducer, VoronoiSeparatorReceipt, VoronoiVertexOrigin, VoronoiVertexReceipt,
    chart_vertex_origins, clip_segment_receipts, exact, exact_xy_coordinates, site_receipts,
    try_hash_map_with_capacity, voronoi_error,
};

pub(in crate::geometry::tessellation::voronoi_dcel) fn collinear_polygon_receipt(
    sites: &[Site],
    clip: &NormalizedClip<'_>,
    cells: &mut [EmbeddedPolygon],
    chart: Option<&PointSelectionChart>,
) -> Result<VoronoiDiagramReceipt> {
    let mut receipt = site_receipts(VoronoiProducer::Collinear, sites)?;
    receipt.clip_segments = clip_segment_receipts(clip, None)?;
    let vertex_count = cells
        .iter()
        .flat_map(|cell| &cell.producer_vertices)
        .try_fold(0_usize, |count, ring| {
            count
                .checked_add(ring.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)
        })?;
    let mut vertex_ids =
        try_hash_map_with_capacity::<[exact::ExactRational; 2], u32>(vertex_count)?;
    let mut site_ids = try_hash_map_with_capacity::<[exact::ExactRational; 2], u32>(sites.len())?;
    for site in sites {
        let _ = site_ids.insert(exact_xy_coordinates(&site.exact_point)?, site.id as u32);
    }
    let mut edge_ids =
        try_hash_map_with_capacity::<(Vec<VoronoiEdgeOrigin>, u32, u32), u32>(vertex_count)?;
    let mut first_darts = Vec::new();
    for cell in cells {
        let owner = cell
            .owner
            .ok_or_else(|| voronoi_error("collinear output cell has no site owner"))?;
        if cell.producer_vertices.len() != cell.edge_roles.len() {
            return Err(voronoi_error(
                "collinear output vertex and edge role rings do not align",
            ));
        }
        let mut ring_ids = Vec::with_capacity(cell.producer_vertices.len());
        for (ring_index, vertices) in cell.producer_vertices.iter().enumerate() {
            let mut ids = Vec::with_capacity(vertices.len());
            for vertex in vertices {
                let key = exact_xy_coordinates(&vertex.exact_point)?;
                let id = if let Some(&id) = vertex_ids.get(&key) {
                    let assignment = receipt
                        .vertices
                        .get(id as usize)
                        .and_then(|receipt| receipt.assignment)
                        .ok_or_else(|| voronoi_error("collinear vertex assignment is absent"))?;
                    if PointKey::new(assignment) != PointKey::new(vertex.assignment) {
                        return Err(voronoi_error(
                            "one exact collinear vertex has conflicting assignments",
                        ));
                    }
                    let receipt_vertex = receipt
                        .vertices
                        .get_mut(id as usize)
                        .ok_or_else(|| voronoi_error("collinear vertex identity is absent"))?;
                    if !receipt_vertex
                        .origins
                        .iter()
                        .any(|origin| matches!(origin, VoronoiVertexOrigin::OverlayNode { node } if *node == vertex.id))
                    {
                        receipt_vertex
                            .origins
                            .push(VoronoiVertexOrigin::OverlayNode { node: vertex.id });
                    }
                    id
                } else {
                    let id = u32::try_from(receipt.vertices.len())
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                    let mut origins = chart_vertex_origins(chart, &vertex.exact_point)?;
                    origins.push(VoronoiVertexOrigin::OverlayNode { node: vertex.id });
                    if let Some(&site) = site_ids.get(&key) {
                        origins.push(VoronoiVertexOrigin::SourceSite { site });
                    }
                    receipt.vertices.push(VoronoiVertexReceipt {
                        id,
                        exact_xy: key.clone(),
                        origins,
                        assignment: Some(vertex.assignment),
                    });
                    receipt.assignments.push(VoronoiAssignmentReceipt {
                        vertex: id,
                        embedded: id,
                        point: vertex.assignment,
                    });
                    let _ = vertex_ids.insert(key, id);
                    id
                };
                ids.push(id);
            }
            if ids.len()
                != cell
                    .edge_roles
                    .get(ring_index)
                    .ok_or_else(|| voronoi_error("collinear output edge role ring is absent"))?
                    .len()
            {
                return Err(voronoi_error(
                    "collinear output ring edges do not match its vertices",
                ));
            }
            ring_ids.push(ids);
        }

        let mut ring_uses = Vec::with_capacity(ring_ids.len());
        for (ring_index, (ids, roles)) in ring_ids.iter().zip(&mut cell.edge_roles).enumerate() {
            let vertices = cell
                .producer_vertices
                .get(ring_index)
                .ok_or_else(|| voronoi_error("collinear output exact vertex ring is absent"))?;
            let exact_ring: Vec<_> = vertices
                .iter()
                .map(|vertex| vertex.exact_point.clone())
                .collect();
            let orientation = exact::incidence::cycle_orientation(&exact_ring)?;
            let solid_left = if ring_index == 0 {
                orientation == ExactSign::Positive
            } else {
                orientation == ExactSign::Negative
            };
            let mut uses = Vec::with_capacity(roles.len());
            for ((&start, &end), role) in ids
                .iter()
                .zip(ids.iter().cycle().skip(1))
                .zip(roles.iter_mut())
            {
                if start == end {
                    return Err(voronoi_error("collinear output has a zero-length edge"));
                }
                let low = start.min(end);
                let high = start.max(end);
                let forward = start == low;
                let mut origins = role.origins.clone();
                origins.sort_unstable();
                origins.dedup();
                let key = (origins.clone(), low, high);
                let filled_left_canonical = solid_left == forward;
                let edge_id = if let Some(&edge) = edge_ids.get(&key) {
                    let _ = receipt
                        .edges
                        .get(edge as usize)
                        .ok_or_else(|| voronoi_error("collinear edge identity is absent"))?;
                    edge
                } else {
                    let edge = u32::try_from(receipt.edges.len())
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                    let clip_sources: Vec<_> = origins
                        .iter()
                        .filter_map(|origin| match origin {
                            VoronoiEdgeOrigin::SourceClip { segment } => Some(*segment),
                            VoronoiEdgeOrigin::Bisector { .. }
                            | VoronoiEdgeOrigin::Frame { .. } => None,
                        })
                        .collect();
                    let frame_side = origins.iter().find_map(|origin| match origin {
                        VoronoiEdgeOrigin::Frame { side } => Some(*side),
                        VoronoiEdgeOrigin::SourceClip { .. }
                        | VoronoiEdgeOrigin::Bisector { .. } => None,
                    });
                    receipt.edges.push(VoronoiEdgeReceipt {
                        id: edge,
                        endpoints: [low, high],
                        separator: None,
                        clip_sources,
                        frame_side,
                        owner_transition: [None, None],
                        origins: origins.clone(),
                    });
                    let _ = edge_ids.insert(key, edge);
                    first_darts.push(None);
                    edge
                };
                role.edge = Some(edge_id);
                role.forward = forward;
                let dart_id = u32::try_from(receipt.darts.len())
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                let twin = *first_darts
                    .get(edge_id as usize)
                    .ok_or_else(|| voronoi_error("collinear edge has no dart row"))?;
                let twin_owner = if let Some(twin) = twin {
                    let previous = receipt
                        .darts
                        .get_mut(twin as usize)
                        .ok_or_else(|| voronoi_error("collinear twin dart is absent"))?;
                    if previous.origin != end || previous.target != start || previous.twin.is_some()
                    {
                        return Err(voronoi_error(
                            "collinear shared edge has inconsistent directed uses",
                        ));
                    }
                    previous.twin = Some(dart_id);
                    Some(
                        previous
                            .owner
                            .ok_or_else(|| voronoi_error("collinear twin has no owner"))?,
                    )
                } else {
                    *first_darts
                        .get_mut(edge_id as usize)
                        .ok_or_else(|| voronoi_error("collinear edge has no dart row"))? =
                        Some(dart_id);
                    None
                };
                let separator = {
                    let edge = receipt
                        .edges
                        .get_mut(edge_id as usize)
                        .ok_or_else(|| voronoi_error("collinear edge identity is absent"))?;
                    let (side_owner, opposite_owner) = if filled_left_canonical {
                        let [left, right] = &mut edge.owner_transition;
                        (left, right)
                    } else {
                        let [left, right] = &mut edge.owner_transition;
                        (right, left)
                    };
                    if side_owner.is_some() {
                        return Err(voronoi_error(
                            "collinear edge has duplicate directed uses on one owner side",
                        ));
                    }
                    *side_owner = Some(owner);
                    if let Some(twin_owner) = twin_owner {
                        if *opposite_owner != Some(twin_owner) {
                            return Err(voronoi_error(
                                "collinear opposite dart does not match its recorded owner side",
                            ));
                        }
                        if twin_owner == owner {
                            None
                        } else {
                            let owners = if owner < twin_owner {
                                [owner, twin_owner]
                            } else {
                                [twin_owner, owner]
                            };
                            let origin_pair = edge.origins.iter().find_map(|origin| match origin {
                                VoronoiEdgeOrigin::Bisector { sites } if *sites == owners => {
                                    Some(*sites)
                                },
                                VoronoiEdgeOrigin::SourceClip { .. }
                                | VoronoiEdgeOrigin::Bisector { .. }
                                | VoronoiEdgeOrigin::Frame { .. } => None,
                            });
                            let separator = origin_pair.ok_or_else(|| {
                                voronoi_error(
                                    "collinear shared owners have no matching bisector origin",
                                )
                            })?;
                            edge.separator = Some(separator);
                            Some(separator)
                        }
                    } else {
                        None
                    }
                };
                receipt.darts.push(VoronoiDartReceipt {
                    id: dart_id,
                    origin: start,
                    target: end,
                    twin,
                    edge: edge_id,
                    face: None,
                    owner: Some(owner),
                });
                if let Some(separator) = separator {
                    receipt.separators.push(VoronoiSeparatorReceipt {
                        sites: separator,
                        vertices: vec![low, high],
                        edges: vec![edge_id],
                    });
                }
                for source_use in &role.source_uses {
                    if source_use.source_operand == Some(1) {
                        let segment = receipt
                            .clip_segments
                            .get_mut(source_use.source_segment as usize)
                            .ok_or_else(|| voronoi_error("collinear clip segment is absent"))?;
                        segment.atoms.push(VoronoiClipAtomReceipt {
                            edge: edge_id,
                            reversed: !source_use.forward,
                            start_parameter: source_use.start_parameter.clone(),
                            end_parameter: source_use.end_parameter.clone(),
                        });
                        for (vertex, parameter) in [
                            (start, &source_use.start_parameter),
                            (end, &source_use.end_parameter),
                        ] {
                            if let Some(receipt_vertex) = receipt.vertices.get_mut(vertex as usize)
                            {
                                receipt_vertex
                                    .origins
                                    .push(VoronoiVertexOrigin::SourceClip {
                                        segment: source_use.source_segment,
                                        parameter: parameter.clone(),
                                        source_ring: source_use
                                            .source_ring
                                            .map(|ring| ring as usize),
                                        source_edge: source_use
                                            .source_edge
                                            .map(|edge| edge as usize),
                                    });
                            }
                        }
                    }
                }
                uses.push(VoronoiDirectedEdgeUse {
                    edge: edge_id,
                    forward,
                });
            }
            ring_uses.push(uses);
        }
        let mut ring_uses = ring_uses.into_iter();
        let shell = ring_uses
            .next()
            .ok_or_else(|| voronoi_error("collinear output cell has no shell"))?;
        receipt.cells.push(VoronoiCellReceipt {
            owner,
            face: None,
            shell,
            holes: ring_uses.collect(),
        });
    }
    Ok(receipt)
}
