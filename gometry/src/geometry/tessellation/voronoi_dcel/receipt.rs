//! Provenance receipts for Voronoi producer outputs.

use super::{
    EmbeddedEdges, EmbeddedPolygon, EmbeddedVertices, ExactCellPart, ExactEdgeChain, ExactSign,
    ExactSimpleRing, ExactSubdivision, FrameSide, NormalizedClip, OverlayNodeReceipt,
    OwnerTransition, Point, PointKey, PointSelectionChart, Polygon, Result, Ring, Site,
    VoronoiAssignmentReceipt, VoronoiBoundary, VoronoiCellReceipt, VoronoiDartReceipt,
    VoronoiDiagramReceipt, VoronoiDirectedEdgeUse, VoronoiEdgeOrigin, VoronoiEdgeReceipt,
    VoronoiFrameSide, VoronoiProducer, VoronoiRingEdgeRole, VoronoiSeparatorReceipt,
    VoronoiSiteReceipt, VoronoiSourceSiteDecision, VoronoiSourceSiteReceipt, VoronoiVertexOrigin,
    VoronoiVertexReceipt, XY, empty_edges_with_receipt, ensure_coordseq_vertex_capacity, exact,
    exact_chart_point, lex_xy, try_hash_map_with_capacity, voronoi_error,
};

mod collinear;

pub(super) use collinear::collinear_polygon_receipt;

pub(super) fn site_receipts(
    producer: VoronoiProducer,
    sites: &[Site],
) -> Result<VoronoiDiagramReceipt> {
    let mut receipt = VoronoiDiagramReceipt::empty(producer);
    receipt.sites.reserve(sites.len());
    for site in sites {
        let id = u32::try_from(site.id).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        receipt.sites.push(VoronoiSiteReceipt {
            id,
            source_ordinal: site.source_ordinal,
            source_record: site.point,
            retained_chart_site: site.retained_chart_site,
            quotient_aliases: site.quotient_aliases.clone(),
            snapped_sources: site.snapped_sources.clone(),
        });
        receipt.source_sites.push(VoronoiSourceSiteReceipt {
            source_ordinal: site.source_ordinal,
            decision: VoronoiSourceSiteDecision::Selected { site: id },
        });
        receipt
            .source_sites
            .extend(
                site.quotient_aliases
                    .iter()
                    .map(|&source_ordinal| VoronoiSourceSiteReceipt {
                        source_ordinal,
                        decision: VoronoiSourceSiteDecision::QuotientAlias { site: id },
                    }),
            );
        receipt
            .source_sites
            .extend(
                site.snapped_sources
                    .iter()
                    .map(|&source_ordinal| VoronoiSourceSiteReceipt {
                        source_ordinal,
                        decision: VoronoiSourceSiteDecision::Snapped { site: id },
                    }),
            );
    }
    receipt
        .source_sites
        .sort_unstable_by_key(|source| source.source_ordinal);
    Ok(receipt)
}

pub(in crate::geometry::tessellation) fn empty_polygon_outputs(
    sites: &[Site],
) -> Result<crate::geometry::VoronoiPolygonOutputs> {
    let receipt = std::sync::Arc::new(site_receipts(VoronoiProducer::Empty, sites)?);
    Ok(crate::geometry::VoronoiPolygonOutputs {
        outputs: Vec::new(),
        receipt,
    })
}

pub(in crate::geometry::tessellation) fn lone_site_empty_polygon_outputs(
    sites: &[Site],
) -> Result<crate::geometry::VoronoiPolygonOutputs> {
    let receipt = std::sync::Arc::new(site_receipts(VoronoiProducer::LoneSite, sites)?);
    Ok(crate::geometry::VoronoiPolygonOutputs {
        outputs: Vec::new(),
        receipt,
    })
}

pub(in crate::geometry::tessellation) fn lone_site_empty_edges(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    chart: Option<&PointSelectionChart>,
) -> Result<EmbeddedEdges> {
    let receipt = match boundary {
        VoronoiBoundary::Polygon(polygon) => {
            lone_site_polygon_outputs(sites, polygon, chart)?.receipt
        },
        VoronoiBoundary::Envelope | VoronoiBoundary::Padded => {
            std::sync::Arc::new(site_receipts(VoronoiProducer::LoneSite, sites)?)
        },
    };
    empty_edges_with_receipt(receipt)
}

pub(in crate::geometry::tessellation) fn lone_site_polygon_outputs(
    sites: &[Site],
    polygon: &Polygon,
    chart: Option<&PointSelectionChart>,
) -> Result<crate::geometry::VoronoiPolygonOutputs> {
    let [site] = sites else {
        return Err(voronoi_error(
            "lone-site receipt requires one admitted site",
        ));
    };
    let mut receipt = site_receipts(VoronoiProducer::LoneSite, sites)?;
    let owner = u32::try_from(site.id).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    let mut output_edge_id = 0_u32;
    let mut vertex_id = 0_u32;
    let mut edge_roles = Vec::with_capacity(polygon.holes.len().saturating_add(1));
    for (ring_index, ring) in std::iter::once(&polygon.shell)
        .chain(polygon.holes.iter())
        .enumerate()
    {
        let open_count = ring
            .len()
            .checked_sub(1)
            .ok_or_else(|| voronoi_error("lone-site clip ring is empty"))?;
        let ring_start = vertex_id;
        let points: Vec<_> = ring.iter().take(open_count).collect();
        let exact_points = points
            .iter()
            .map(|point| exact_chart_point(chart, point.xy()))
            .collect::<Result<Vec<_>>>()?;
        let orientation = exact::incidence::cycle_orientation(&exact_points)?;
        let filled_left = if ring_index == 0 {
            orientation == ExactSign::Positive
        } else {
            orientation == ExactSign::Negative
        };
        let owner_transition = if filled_left {
            [Some(owner), None]
        } else {
            [None, Some(owner)]
        };
        let mut role_row = Vec::with_capacity(open_count);
        for (point, exact_point) in points.iter().zip(&exact_points) {
            let id = vertex_id;
            vertex_id = vertex_id
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let mut origins = Vec::new();
            if exact_point.same_position(&site.exact_point)? {
                origins.push(VoronoiVertexOrigin::SourceSite { site: owner });
            }
            receipt.vertices.push(VoronoiVertexReceipt {
                origins,
                assignment: Some(point.xy()),
            });
            receipt.assignments.push(VoronoiAssignmentReceipt {
                vertex: id,
                embedded: id,
                point: point.xy(),
            });
        }
        let mut ring_edge_ids = Vec::with_capacity(open_count);
        for edge_index in 0..open_count {
            let next_index = edge_index
                .checked_add(1)
                .filter(|&next| next < open_count)
                .unwrap_or(0);
            receipt.edges.push(VoronoiEdgeReceipt {
                id: output_edge_id,
                endpoints: [
                    ring_start
                        .checked_add(
                            u32::try_from(edge_index)
                                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        )
                        .ok_or(crate::error::ResourceError::SizeOverflow)?,
                    ring_start
                        .checked_add(
                            u32::try_from(next_index)
                                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        )
                        .ok_or(crate::error::ResourceError::SizeOverflow)?,
                ],
                separator: None,
                clip_sources: vec![output_edge_id],
                frame_side: None,
                owner_transition,
                origins: vec![VoronoiEdgeOrigin::SourceClip {
                    segment: output_edge_id,
                }],
            });
            let [edge_start, edge_end] = receipt
                .edges
                .last()
                .ok_or_else(|| voronoi_error("lone-site receipt edge is absent"))?
                .endpoints;
            receipt.darts.push(VoronoiDartReceipt {
                id: output_edge_id,
                origin: edge_start,
                target: edge_end,
                twin: None,
                edge: output_edge_id,
                face: None,
                owner: Some(owner),
            });
            role_row.push(VoronoiRingEdgeRole {
                edge: Some(output_edge_id),
                forward: true,
                origins: vec![VoronoiEdgeOrigin::SourceClip {
                    segment: output_edge_id,
                }],
            });
            ring_edge_ids.push(output_edge_id);
            output_edge_id = output_edge_id
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        let ring_uses = ring_edge_ids
            .into_iter()
            .map(|edge| VoronoiDirectedEdgeUse {
                edge,
                forward: true,
            })
            .collect::<Vec<_>>();
        receipt.cells.push(VoronoiCellReceipt {
            owner,
            face: None,
            shell: if ring_index == 0 {
                ring_uses.clone()
            } else {
                Vec::new()
            },
            holes: if ring_index == 0 {
                Vec::new()
            } else {
                vec![ring_uses]
            },
        });
        edge_roles.push(role_row);
    }
    let mut cell = receipt
        .cells
        .first()
        .cloned()
        .ok_or_else(|| voronoi_error("lone-site receipt has no shell"))?;
    cell.holes = receipt
        .cells
        .iter()
        .skip(1)
        .flat_map(|cell| cell.holes.iter().cloned())
        .collect();
    receipt.cells.clear();
    receipt.cells.push(cell);
    Ok(crate::geometry::VoronoiPolygonOutputs {
        outputs: vec![crate::geometry::VoronoiPolygonOutput {
            polygon: polygon.clone(),
            owner: Some(owner),
            edge_roles,
        }],
        receipt: std::sync::Arc::new(receipt),
    })
}

pub(super) const fn receipt_frame_side(side: FrameSide) -> VoronoiFrameSide {
    match side {
        FrameSide::Bottom => VoronoiFrameSide::Bottom,
        FrameSide::Right => VoronoiFrameSide::Right,
        FrameSide::Top => VoronoiFrameSide::Top,
        FrameSide::Left => VoronoiFrameSide::Left,
    }
}

pub(super) fn exact_diagram_receipt(
    producer: VoronoiProducer,
    sites: &[Site],
    subdivision: &ExactSubdivision,
    parts: &[ExactCellPart],
    chains: &[ExactEdgeChain],
    clip: &NormalizedClip<'_>,
    exact_to_embedded: &EmbeddedVertices,
    points: &[XY],
) -> Result<VoronoiDiagramReceipt> {
    let mut receipt = site_receipts(producer, sites)?;

    let mut assignments = vec![None; subdivision.vertices.len()];
    for (vertex, embedded) in exact_to_embedded.0.iter().enumerate() {
        if let Some(embedded) = embedded {
            let point = *points.get(embedded.0 as usize).ok_or_else(|| {
                voronoi_error("receipt assignment is outside the embedded points")
            })?;
            *assignments
                .get_mut(vertex)
                .ok_or_else(|| voronoi_error("receipt vertex is outside the assignment map"))? =
                Some(point);
            receipt.assignments.push(VoronoiAssignmentReceipt {
                vertex: u32::try_from(vertex)
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                embedded: embedded.0,
                point,
            });
        }
    }

    let mut vertex_origins: Vec<Vec<VoronoiVertexOrigin>> = std::iter::repeat_with(Vec::new)
        .take(subdivision.vertices.len())
        .collect();
    for site in sites {
        if let Some(vertex) = subdivision.vertex_at(&site.exact_point)? {
            vertex_origins
                .get_mut(vertex.0 as usize)
                .ok_or_else(|| voronoi_error("source-site vertex is absent from the receipt"))?
                .push(VoronoiVertexOrigin::SourceSite {
                    site: u32::try_from(site.id)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                });
        }
    }
    for edge in &subdivision.edges {
        let separator = edge.separator.map(|pair| pair.map(|site| site.0));
        let frame_side = edge.frame_side.map(receipt_frame_side);
        for vertex in edge.endpoints {
            let origins = vertex_origins
                .get_mut(vertex.0 as usize)
                .ok_or_else(|| voronoi_error("edge vertex is absent from the receipt"))?;
            if let Some(sites) = separator {
                origins.push(VoronoiVertexOrigin::Bisector { sites });
            }
            if let Some(side) = frame_side {
                origins.push(VoronoiVertexOrigin::Frame { side });
            }
        }
    }
    receipt.vertices.reserve(subdivision.vertices.len());
    for (index, _) in subdivision.vertices.iter().enumerate() {
        let origins = vertex_origins
            .get_mut(index)
            .ok_or_else(|| voronoi_error("diagram vertex origins are absent"))?;
        let assignment = *assignments
            .get(index)
            .ok_or_else(|| voronoi_error("diagram vertex assignment is absent"))?;
        receipt.vertices.push(VoronoiVertexReceipt {
            origins: std::mem::take(origins),
            assignment,
        });
    }

    receipt.edges.reserve(subdivision.edges.len());
    for (index, edge) in subdivision.edges.iter().enumerate() {
        let separator = edge.separator.map(|pair| pair.map(|site| site.0));
        let frame_side = edge.frame_side.map(receipt_frame_side);
        let mut origins = Vec::new();
        if let Some(sites) = separator {
            origins.push(VoronoiEdgeOrigin::Bisector { sites });
        }
        if let Some(side) = frame_side {
            origins.push(VoronoiEdgeOrigin::Frame { side });
        }
        if clip.source.is_some() {
            origins.extend(
                edge.clip_sources
                    .iter()
                    .map(|source| VoronoiEdgeOrigin::SourceClip { segment: source.0 }),
            );
        }
        receipt.edges.push(VoronoiEdgeReceipt {
            id: u32::try_from(index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            endpoints: edge.endpoints.map(|vertex| vertex.0),
            separator,
            clip_sources: edge.clip_sources.iter().map(|source| source.0).collect(),
            frame_side,
            owner_transition: match edge.forward.owner {
                OwnerTransition::Preserve => [None, None],
                OwnerTransition::Boundary { left, right } => {
                    [left.map(|site| site.0), right.map(|site| site.0)]
                },
            },
            origins,
        });
    }
    for (slot, _) in subdivision.edge_of_slot.iter().enumerate() {
        let dart = subdivision.dart(slot as u32);
        receipt.darts.push(VoronoiDartReceipt {
            id: u32::try_from(slot).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            origin: dart.origin.0,
            target: dart.target.0,
            twin: Some(dart.twin),
            edge: dart.edge.0,
            face: Some(dart.face.0),
            owner: subdivision.label(dart.face).owner.map(|site| site.0),
        });
    }
    for part in parts {
        let ring_uses = |ring: &ExactSimpleRing| -> Vec<VoronoiDirectedEdgeUse> {
            ring.edges
                .iter()
                .zip(ring.vertices.array_windows::<2>())
                .map(|(&edge, &[start, end])| VoronoiDirectedEdgeUse {
                    edge: edge.0,
                    forward: subdivision.edge(edge).endpoints == [start, end],
                })
                .collect()
        };
        receipt.cells.push(VoronoiCellReceipt {
            owner: part.owner.0,
            face: Some(part.face.0),
            shell: ring_uses(&part.shell),
            holes: part.holes.iter().map(ring_uses).collect(),
        });
    }
    for chain in chains {
        receipt.separators.push(VoronoiSeparatorReceipt {
            sites: chain.separator.map(|site| site.0),
            vertices: chain.vertices.iter().map(|vertex| vertex.0).collect(),
            edges: chain.edges.iter().map(|edge| edge.0).collect(),
        });
    }
    Ok(receipt)
}

fn exact_ring_edge_roles(
    subdivision: &ExactSubdivision,
    ring: &ExactSimpleRing,
    clip: &NormalizedClip<'_>,
) -> Result<Vec<VoronoiRingEdgeRole>> {
    ring.edges
        .iter()
        .zip(ring.vertices.array_windows::<2>())
        .map(|(&edge_id, &[start, end])| {
            let edge = subdivision.edge(edge_id);
            let mut origins = Vec::new();
            if let Some(pair) = edge.separator {
                origins.push(VoronoiEdgeOrigin::Bisector {
                    sites: pair.map(|site| site.0),
                });
            }
            if let Some(side) = edge.frame_side {
                origins.push(VoronoiEdgeOrigin::Frame {
                    side: receipt_frame_side(side),
                });
            }
            if clip.source.is_some() && !edge.clip_sources.is_empty() {
                origins.extend(
                    edge.clip_sources
                        .iter()
                        .map(|source| VoronoiEdgeOrigin::SourceClip { segment: source.0 }),
                );
            }
            let forward = edge.endpoints == [start, end];
            Ok(VoronoiRingEdgeRole {
                edge: Some(edge_id.0),
                forward,
                origins,
            })
        })
        .collect()
}

pub(super) fn receipt_ring_uses(
    roles: &[VoronoiRingEdgeRole],
) -> Result<Vec<VoronoiDirectedEdgeUse>> {
    roles
        .iter()
        .map(|role| {
            Ok(VoronoiDirectedEdgeUse {
                edge: role
                    .edge
                    .ok_or_else(|| voronoi_error("published Voronoi edge has no receipt ID"))?,
                forward: role.forward,
            })
        })
        .collect()
}

pub(super) fn embedded_exact_ring(
    subdivision: &ExactSubdivision,
    ring: &ExactSimpleRing,
    exact_to_embedded: &EmbeddedVertices,
    points: &[XY],
    clip: &NormalizedClip<'_>,
) -> Result<(Ring, Vec<VoronoiRingEdgeRole>, Vec<OverlayNodeReceipt>)> {
    ensure_coordseq_vertex_capacity(ring.vertices.len())?;
    let (_, open_vertices) = ring
        .vertices
        .split_last()
        .expect("simple Voronoi rings are closed");
    let mut coordinates = Vec::with_capacity(ring.vertices.len());
    let mut producer_vertices = Vec::with_capacity(open_vertices.len());
    for &vertex in open_vertices {
        let point = exact_to_embedded.point(vertex, points)?;
        coordinates.push(Point::new_raw_xy(point.x, point.y));
        producer_vertices.push(OverlayNodeReceipt {
            id: vertex.0,
            exact_point: subdivision.vertex(vertex).point.clone(),
            assignment: point,
        });
    }
    let mut roles = exact_ring_edge_roles(subdivision, ring, clip)?;
    if open_vertices.len() > 2 {
        let (start, _) = coordinates
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| lex_xy(left.xy(), right.xy()))
            .expect("non-empty ring");
        coordinates.rotate_left(start);
        roles.rotate_left(start);
        producer_vertices.rotate_left(start);
    }
    let first = *coordinates
        .first()
        .expect("an exact boundary ring has at least three open vertices");
    coordinates.push(first);
    Ok((Ring::closed(coordinates)?, roles, producer_vertices))
}
