//! Separator-edge production for the analytic collinear lane.

use super::{
    AtomicEdge, CertifiedDelaunay, ClipSegmentId, CollinearClipEvents, EdgeId, EmbeddedEdges,
    ExactEdgeChain, ExactSubdivision, ExactVertex, ExpansionBudget, IndexedTopology,
    NormalizedClip, OwnerTransition, PointSelectionChart, Result, Site, SiteId, Transition,
    VertexId, VoronoiBoundary, VoronoiProducer, VoronoiProduct, collinear_clip_events, embed,
    empty_packed_edges, exact, exact_cycle_error, fixed_coordinates, lex_position,
    normalize_clip_with_chart, orient_separator, sort_unique_along, try_dedup_by, try_sort_by,
    voronoi_error,
};

#[cfg(test)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn build_collinear_edges(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
) -> Result<EmbeddedEdges> {
    build_collinear_edges_with_chart(sites, boundary, budget, mesh, None)
}

pub(in crate::geometry::tessellation) fn build_collinear_edges_with_chart(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
    chart: Option<&PointSelectionChart>,
) -> Result<EmbeddedEdges> {
    if let Some(last) = sites.len().checked_sub(1) {
        u32::try_from(last).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    }
    let order = mesh
        .collinear_order()
        .ok_or_else(|| voronoi_error("non-collinear sites entered the collinear Voronoi lane"))?;
    if matches!(boundary, VoronoiBoundary::Envelope) {
        if let Some(first) = sites.first() {
            let mut same_x = true;
            let mut same_y = true;
            for site in sites.iter().skip(1) {
                same_x &= site.exact_point.compare_x(&first.exact_point)?.is_eq();
                same_y &= site.exact_point.compare_y(&first.exact_point)?.is_eq();
            }
            if same_x || same_y {
                return empty_packed_edges(chart);
            }
        } else {
            return empty_packed_edges(chart);
        }
    }
    let clip = normalize_clip_with_chart(sites, boundary, false, chart)?;
    let CollinearClipEvents {
        separators: exact_chains,
        edge_events,
    } = collinear_clip_events(sites, order, &clip)?;
    let mut split_rings = Vec::with_capacity(clip.rings.len());
    let mut edge_events = edge_events.into_iter();
    for ring in &clip.rings {
        let mut split_ring = Vec::new();
        for (start, end) in ring.iter().zip(ring.iter().cycle().skip(1)) {
            let mut events = edge_events.next().ok_or_else(exact_cycle_error)?;
            events.push(start.clone());
            sort_unique_along(start, end, &mut events)?;
            for point in events {
                let repeated = match split_ring.last() {
                    Some(known) => point.same_position(known)?,
                    None => false,
                };
                if !repeated {
                    split_ring.push(point);
                }
            }
        }
        split_rings.push(split_ring);
    }
    if edge_events.next().is_some() {
        return Err(exact_cycle_error());
    }
    let mut split_fixed = Vec::with_capacity(split_rings.len());
    for ring in &split_rings {
        split_fixed.push(
            ring.iter()
                .map(exact::ExactPoint::round_nearest_even)
                .collect::<Result<Vec<_>>>()?,
        );
    }
    let split_clip = NormalizedClip {
        rings: split_rings,
        fixed: split_fixed.clone(),
        chart_fixed: split_fixed,
        source_roots: Vec::new(),
        source: clip.source,
        geometry: clip.geometry.clone(),
        polygon_baseline: clip.polygon_baseline,
        contacts: Vec::new(),
    };
    let separator_points = exact_chains.iter().try_fold(0_usize, |count, (_, chain)| {
        count
            .checked_add(chain.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let clip_point_count = split_clip.rings.iter().try_fold(0_usize, |count, ring| {
        count
            .checked_add(ring.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut exact_points = Vec::with_capacity(
        separator_points
            .checked_add(clip_point_count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?,
    );
    exact_points.extend(
        exact_chains
            .iter()
            .flat_map(|(_, chain)| chain.iter().cloned()),
    );
    exact_points.extend(split_clip.rings.iter().flatten().cloned());
    try_sort_by(&mut exact_points, exact::ExactPoint::compare_lex)?;
    try_dedup_by(&mut exact_points, exact::ExactPoint::same_position)?;
    let separator_edge_count = exact_chains.iter().try_fold(0_usize, |count, (_, chain)| {
        let chain_edge_count = chain
            .len()
            .checked_sub(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        count
            .checked_add(chain_edge_count)
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let clip_edge_count = split_clip.rings.iter().try_fold(0_usize, |count, ring| {
        count
            .checked_add(ring.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let edge_count = separator_edge_count
        .checked_add(clip_edge_count)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    IndexedTopology::admit_counts(exact_points.len(), edge_count)?;
    let clip_source_count =
        u32::try_from(clip_edge_count).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    let fixed = fixed_coordinates(&exact_points, sites, &clip)?;
    let vertex_id = |point: &exact::ExactPoint| -> Result<VertexId> {
        let index = lex_position(&exact_points, point)?.expect("collinear event was interned");
        Ok(VertexId(index as u32))
    };
    let mut edges = Vec::with_capacity(edge_count);
    let mut chains = Vec::with_capacity(exact_chains.len());
    for ([left, right], mut chain) in exact_chains {
        let ids = [SiteId(left as u32), SiteId(right as u32)];
        let separator = [ids[0].min(ids[1]), ids[0].max(ids[1])];
        let (oriented_start, _) = orient_separator(
            chain.first().unwrap().clone(),
            chain.last().unwrap().clone(),
            ids,
            sites,
        )?;
        if !oriented_start.same_position(chain.first().unwrap())? {
            chain.reverse();
        }
        let mut chain_edges = Vec::new();
        let mut chain_vertices = Vec::new();
        chain_edges.reserve(
            chain
                .len()
                .checked_sub(1)
                .expect("emitted separator chains contain at least one edge"),
        );
        chain_vertices.reserve(chain.len());
        for point in &chain {
            chain_vertices.push(vertex_id(point)?);
        }
        for &[left, right] in chain_vertices.array_windows::<2>() {
            chain_edges.push(EdgeId(edges.len() as u32));
            edges.push(AtomicEdge {
                endpoints: [left, right],
                forward: Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: OwnerTransition::Boundary {
                        left: Some(ids[0]),
                        right: Some(ids[1]),
                    },
                },
                separator: Some(separator),
                clip_sources: Vec::new(),
                frame_side: None,
            });
        }
        chains.push(ExactEdgeChain {
            separator,
            vertices: chain_vertices,
            edges: chain_edges,
        });
    }
    let clip_segments = split_clip
        .rings
        .iter()
        .flat_map(|ring| ring.iter().zip(ring.iter().cycle().skip(1)));
    for (clip_source, (start, end)) in (0..clip_source_count).zip(clip_segments) {
        edges.push(AtomicEdge {
            endpoints: [vertex_id(start)?, vertex_id(end)?],
            forward: Transition {
                frame_delta: 0,
                clip_delta: 1,
                owner: OwnerTransition::Preserve,
            },
            separator: None,
            clip_sources: vec![ClipSegmentId(clip_source)],
            frame_side: None,
        });
    }
    let mut vertices = Vec::with_capacity(exact_points.len());
    vertices.extend(
        exact_points
            .into_iter()
            .zip(fixed)
            .map(|(point, fixed_xy)| ExactVertex { point, fixed_xy }),
    );
    let subdivision = ExactSubdivision {
        vertices,
        edges,
        topology: IndexedTopology::default(),
        edge_of_slot: Vec::new(),
        labels: Vec::new(),
    };
    let embedded = embed(
        &subdivision,
        &[],
        &chains,
        &split_clip,
        sites,
        VoronoiProduct::Edges,
        budget,
        VoronoiProducer::Collinear,
        chart,
    )?;
    Ok(embedded.into_edges())
}
