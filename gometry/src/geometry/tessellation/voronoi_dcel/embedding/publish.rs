//! Final binary64 projection and publication.

use super::super::{
    BoxIndex, ClipTrace, EdgeId, EmbeddedEdgeData, EmbeddedEdges, EmbeddedPolygon,
    EmbeddedVertexId, EmbeddedVertices, EmbeddedVoronoi, EvaluationBudget, ExactCellPart,
    ExactEdgeChain, ExactSubdivision, ExpansionBudget, LiveVertices, NormalizedClip, Point,
    Polygon, Result, Ring, Search, Shape, Site, Strictness, VertexId, VoronoiEmbedding,
    VoronoiProblem, VoronoiProducer, VoronoiProduct, admit_embedded_clip,
    binary_cartesian_difference_without_partition_budget, coverage_is_valid, embedded_exact_ring,
    ensure_coordseq_vertex_capacity, exact, exact_diagram_receipt, exact_embedding_unrepresentable,
    pack_separator_chains, polygon_is_valid, polygon_output_count, receipt_ring_uses,
    voronoi_error, walk_label,
};

pub(in crate::geometry::tessellation::voronoi_dcel) fn embed(
    subdivision: &ExactSubdivision,
    parts: &[ExactCellPart],
    chains: &[ExactEdgeChain],
    clip: &NormalizedClip<'_>,
    sites: &[Site],
    product: VoronoiProduct,
    budget: &mut ExpansionBudget,
    producer: VoronoiProducer,
) -> Result<EmbeddedVoronoi> {
    let mut live = LiveVertices::new(subdivision.vertices.len());
    let mut live_edges = Vec::new();
    for part in parts {
        for ring in std::iter::once(&part.shell).chain(&part.holes) {
            live_edges.reserve(ring.edges.len());
            live_edges.extend_from_slice(&ring.edges);
            for vertex in &ring.vertices {
                live.mark(*vertex);
            }
        }
    }
    for chain in chains {
        live_edges.reserve(chain.edges.len());
        live_edges.extend_from_slice(&chain.edges);
        for vertex in &chain.vertices {
            live.mark(*vertex);
        }
    }
    for (edge_index, edge) in subdivision.edges.iter().enumerate() {
        if !edge.clip_sources.is_empty() {
            live_edges.push(EdgeId(edge_index as u32));
            for vertex in edge.endpoints {
                live.mark(vertex);
            }
        }
    }
    live_edges.sort_unstable();
    live_edges.dedup();
    for point in clip.rings.iter().flatten() {
        let vertex = subdivision
            .vertex_at(point)?
            .ok_or_else(|| voronoi_error("exact clip vertex is absent from the snap arena"))?;
        live.mark(vertex);
    }
    let mut points = Vec::new();
    let mut exact_to_embedded = EmbeddedVertices::new(subdivision.vertices.len());
    let mut live_nodes = Vec::new();
    for (index, (vertex, &is_live)) in subdivision.vertices.iter().zip(live.iter()).enumerate() {
        if !is_live {
            continue;
        }
        let point = vertex
            .fixed_xy
            .map_or_else(|| vertex.point.round_nearest_even(), Ok)?;
        let embedded = EmbeddedVertexId(points.len() as u32);
        let vertex = VertexId(index as u32);
        exact_to_embedded.set(vertex, embedded);
        live_nodes.push(vertex);
        points.push(point);
    }
    live_nodes.sort_unstable();
    let operation = if product.includes_polygons() {
        "voronoi_cells"
    } else {
        "voronoi_edge_lines"
    };
    let mut search_budget = EvaluationBudget::new(operation, 0)?;
    let clip_trace = ClipTrace::build(subdivision, clip, &mut search_budget)?;
    let (assignment, constructed) = {
        let embedding = VoronoiEmbedding::new(
            subdivision,
            clip,
            &clip_trace,
            parts,
            product.includes_polygons(),
            &live_nodes,
            &live_edges,
            &exact_to_embedded,
            &points,
            sites,
        )?;
        let conflicts = embedding.collect_conflicts(&points, &mut search_budget)?;
        if conflicts.is_empty() {
            (None, 0)
        } else {
            let seeds = embedding.seed_nodes(&conflicts);
            let problem = VoronoiProblem::new(&embedding, &seeds)?;
            let constructed = problem.classes.iter().try_fold(0_usize, |count, class| {
                count
                    .checked_add(class.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)
            })?;
            search_budget.set_constructed(constructed)?;
            (
                Some(Search::with_budget(problem, search_budget).run()?),
                constructed,
            )
        }
    };
    if let Some(assignment) = assignment {
        for class in assignment.classes {
            for (node, candidate) in class.variables.into_iter().zip(class.values) {
                exact_to_embedded.set_point(node, &mut points, candidate);
            }
        }
        let embedding = VoronoiEmbedding::new(
            subdivision,
            clip,
            &clip_trace,
            parts,
            product.includes_polygons(),
            &live_nodes,
            &live_edges,
            &exact_to_embedded,
            &points,
            sites,
        )?;
        let mut verification_budget = EvaluationBudget::new(operation, constructed)?;
        if !embedding
            .collect_conflicts(&points, &mut verification_budget)?
            .is_empty()
        {
            return Err(exact_embedding_unrepresentable(operation, constructed));
        }
    }
    let mut published = 0_usize;
    if product.includes_polygons() {
        for part in parts {
            let part_count = polygon_output_count(
                part.shell.vertices.len(),
                part.holes.iter().map(|hole| hole.vertices.len()),
            )?;
            published = published
                .checked_add(part_count)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
    }
    let edge_positions = if product.includes_edges() {
        chains.iter().try_fold(0_usize, |count, chain| {
            count
                .checked_add(chain.vertices.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)
        })?
    } else {
        0
    };
    published = published
        .checked_add(edge_positions)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    if product.includes_edges() {
        published = published
            .checked_add(chains.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let baseline = if product.includes_polygons() {
        clip.polygon_baseline.unwrap_or(0)
    } else {
        0
    };
    budget.add(published.saturating_sub(baseline))?;
    let mut receipt = exact_diagram_receipt(
        producer,
        sites,
        subdivision,
        parts,
        chains,
        clip,
        &exact_to_embedded,
        &points,
    )?;
    let mut labelled = Vec::with_capacity(if product.includes_polygons() {
        parts.len()
    } else {
        0
    });
    for part in parts {
        if walk_label(subdivision, part.face).owner != Some(part.owner) {
            return Err(voronoi_error("embedded cell lost its exact face owner"));
        }
        if !product.includes_polygons() {
            continue;
        }
        let (shell, shell_roles, shell_vertices) =
            embedded_exact_ring(subdivision, &part.shell, &exact_to_embedded, &points, clip)?;
        let holes_with_roles = part
            .holes
            .iter()
            .map(|hole| embedded_exact_ring(subdivision, hole, &exact_to_embedded, &points, clip))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let holes: Vec<_> = holes_with_roles
            .iter()
            .map(|(ring, ..)| ring.clone())
            .collect();
        let edge_roles: Vec<_> = std::iter::once(shell_roles)
            .chain(holes_with_roles.iter().map(|(_, roles, _)| roles.clone()))
            .collect();
        let producer_vertices = std::iter::once(shell_vertices)
            .chain(
                holes_with_roles
                    .into_iter()
                    .map(|(_, _, vertices)| vertices),
            )
            .collect();
        let polygon = Polygon::new(shell, holes);
        if !polygon_is_valid(&polygon)? {
            return Err(exact_embedding_unrepresentable(operation, constructed));
        }
        labelled.push((part.owner, EmbeddedPolygon {
            polygon,
            owner: Some(part.owner.0),
            edge_roles,
            producer_vertices,
        }));
    }
    if product.includes_polygons() {
        if receipt.cells.len() != labelled.len() {
            return Err(voronoi_error(
                "exact cell receipts do not match published polygon parts",
            ));
        }
        for (cell, (owner, output)) in receipt.cells.iter_mut().zip(&labelled) {
            if cell.owner != owner.0 || output.owner != Some(owner.0) {
                return Err(voronoi_error(
                    "exact cell receipt lost its published site owner",
                ));
            }
            let (shell, holes) = output
                .edge_roles
                .split_first()
                .ok_or_else(|| voronoi_error("published exact cell has no shell roles"))?;
            cell.shell = receipt_ring_uses(shell)?;
            cell.holes = holes
                .iter()
                .map(|ring| receipt_ring_uses(ring))
                .collect::<Result<Vec<_>>>()?;
        }
    }
    if product.includes_polygons() {
        let embedded_ring = |ring: &[exact::ExactPoint]| -> Result<Ring> {
            let capacity = ring
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ensure_coordseq_vertex_capacity(capacity)?;
            let mut coordinates = Vec::with_capacity(capacity);
            for point in ring {
                let vertex = subdivision
                    .vertex_at(point)?
                    .ok_or_else(|| voronoi_error("generated clip vertex is not live"))?;
                let xy = exact_to_embedded.point(vertex, &points)?;
                coordinates.push(Point::new_raw_xy(xy.x, xy.y));
            }
            let first = *coordinates
                .first()
                .expect("normalized clip rings contain at least one coordinate");
            coordinates.push(first);
            Ring::closed(coordinates)
        };
        let (clip_shell, clip_holes) = clip
            .rings
            .split_first()
            .expect("normalized Voronoi clips contain their outer ring");
        let shell = embedded_ring(clip_shell)?;
        let holes = clip_holes
            .iter()
            .map(|ring| embedded_ring(ring))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let comparison_polygon = Polygon::new(shell, holes);
        admit_embedded_clip(&comparison_polygon)?;
        let comparison_clip = Shape::Polygon(comparison_polygon);
        if clip.source.is_some() {
            let source = Shape::Polygon(
                clip.geometry
                    .as_ref()
                    .expect("source clips retain their normalized chart geometry")
                    .clone(),
            );
            let source_gap = binary_cartesian_difference_without_partition_budget(
                &source,
                &comparison_clip,
                Strictness::Lenient,
            )?;
            let source_spill = binary_cartesian_difference_without_partition_budget(
                &comparison_clip,
                &source,
                Strictness::Lenient,
            )?;
            if !source_gap.is_empty() || !source_spill.is_empty() {
                return Err(voronoi_error(
                    "embedded Voronoi clip differs topologically from its source",
                ));
            }
        }
        if labelled.is_empty() {
            return Err(voronoi_error(
                "exact Voronoi subdivision produced no labelled cells",
            ));
        }
        let mut shape_rows = Vec::with_capacity(labelled.len());
        shape_rows.extend(
            labelled
                .iter()
                .map(|(_, output)| Shape::Polygon(output.polygon.clone())),
        );
        if !coverage_is_valid(&shape_rows, 0.0)? {
            return Err(exact_embedding_unrepresentable(operation, constructed));
        }
        let mut row_boxes = Vec::with_capacity(shape_rows.len());
        row_boxes.extend(shape_rows.iter().map(Shape::bounds));
        let row_index = BoxIndex::build(&row_boxes)?;
        for (site_index, site) in sites.iter().enumerate() {
            let rounded_site = site.rounded_xy()?;
            let point = Shape::Point(Point::new_raw_xy(rounded_site.x, rounded_site.y));
            if !comparison_clip.covers(&point)? {
                continue;
            }
            let mut own = false;
            #[expect(
                clippy::indexing_slicing,
                reason = "row_index is built from shape_rows cloned in the same order as labelled, so every hit is an ordinal into this parallel cell table"
            )]
            let verify_row = |row: usize| {
                let (owner, output) = &labelled[row];
                if Shape::Polygon(output.polygon.clone()).covers(&point)? {
                    if owner.0 as usize != site_index {
                        return Err(exact_embedding_unrepresentable(operation, constructed));
                    }
                    own = true;
                }
                Ok(())
            };
            row_index.for_each_containing(Some(rounded_site), verify_row)?;
            if !own {
                return Err(exact_embedding_unrepresentable(operation, constructed));
            }
        }
        // Exact vertices are rounded once and shared by every incident cell.  The
        // certificates above preserve the subdivision topology, validity, and
        // interior-disjointness, but binary64 overlay may still observe an ULP-scale
        // gap between the rounded partition and the source clip.  That numerical
        // equality is deliberately not part of the topological-snap contract.
    }
    let edge_data = if product.includes_edges() {
        pack_separator_chains(chains, edge_positions, |vertex| {
            exact_to_embedded.point(vertex, &points)
        })?
    } else {
        EmbeddedEdgeData::Segments(Vec::new())
    };
    let polygons = labelled.into_iter().map(|(_, output)| output).collect();
    let receipt = std::sync::Arc::new(receipt);
    let edges = EmbeddedEdges {
        data: edge_data,
        receipt: std::sync::Arc::clone(&receipt),
    };
    Ok(EmbeddedVoronoi {
        polygons,
        edges,
        receipt,
    })
}
