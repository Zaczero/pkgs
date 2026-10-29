//! Exact face selection for already-noded integer grid rings.

use super::{
    ExactPartition, ExactSourceInventory, ExactSpan, KeptNodes, NodeId, PointArena, Result,
    RingKind, Segment, SegmentProvenance, XY, embedding,
};
use crate::error::ResourceError;
use crate::geometry::tile_grid::GridPoint;
use crate::geometry::{HashMap, OverlayOp};

type GridRing = Vec<GridPoint>;
type GridPolygon = Vec<GridRing>;

fn node(
    point: GridPoint,
    index: &mut HashMap<GridPoint, u32>,
    points: &mut Vec<GridPoint>,
) -> Result<u32> {
    if let Some(&existing) = index.get(&point) {
        return Ok(existing);
    }
    let id = u32::try_from(points.len()).map_err(|_| ResourceError::SizeOverflow)?;
    index.insert(point.clone(), id);
    points.push(point);
    Ok(id)
}

/// Select the union of the source polygons, optionally intersected with a
/// rectangle. The rings must already be noded together, including the rectangle.
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "node and edge IDs are allocated in these parallel inventories; the optional rectangle contributes exactly one ring"
)]
pub(in crate::geometry) fn polygonize_grid(
    polygons: &[GridPolygon],
    rectangle: Option<&GridRing>,
    track_rings: bool,
) -> Result<(Vec<GridPolygon>, Vec<bool>)> {
    let state = crate::collections::collection_hash_state()?.clone();
    let mut point_index = HashMap::with_hasher(state.clone());
    let mut edge_index = HashMap::with_hasher(state);
    let mut points = Vec::new();
    let mut spans = Vec::new();
    let mut owners: Vec<Vec<(u32, bool)>> = Vec::new();
    let mut source_segments = Vec::new();
    let mut source_operands = Vec::new();
    let mut source_rings = Vec::new();
    let mut ring_kinds = Vec::new();
    let mut ring_polygons = Vec::new();
    let mut ring_holes = Vec::new();
    let mut polygon_operands = Vec::new();
    let mut polygon_ring_ranges = Vec::new();
    let mut polygon_rings = vec![Vec::new(); if rectangle.is_some() { 2 } else { 1 }];
    let mut unique = Vec::new();

    let rectangle_polygon = rectangle.map(|ring| vec![ring.clone()]);
    for (operand, polygon) in polygons
        .iter()
        .map(|polygon| (0_u32, polygon))
        .chain(rectangle_polygon.iter().map(|polygon| (1_u32, polygon)))
    {
        let polygon_id =
            u32::try_from(polygon_operands.len()).map_err(|_| ResourceError::SizeOverflow)?;
        let ring_start = ring_kinds.len();
        polygon_operands.push(operand);
        for (ring_offset, ring) in polygon.iter().enumerate() {
            let ring_id =
                u32::try_from(ring_kinds.len()).map_err(|_| ResourceError::SizeOverflow)?;
            ring_kinds.push(RingKind::Winding);
            ring_polygons.push(polygon_id);
            ring_holes.push(ring_offset != 0);
            for pair in ring.windows(2) {
                let [start, end] = pair else {
                    unreachable!("a two-point window has two entries");
                };
                if start == end {
                    continue;
                }
                let start_id = node(start.clone(), &mut point_index, &mut points)?;
                let end_id = node(end.clone(), &mut point_index, &mut points)?;
                let source_id = u32::try_from(source_segments.len())
                    .map_err(|_| ResourceError::SizeOverflow)?;
                let segment = Segment {
                    start: XY::new(start[0].rounded()?, start[1].rounded()?),
                    end: XY::new(end[0].rounded()?, end[1].rounded()?),
                };
                source_segments.push(segment);
                source_operands.push(operand);
                source_rings.push(Some(ring_id));
                let key = if start_id < end_id {
                    (start_id, end_id)
                } else {
                    (end_id, start_id)
                };
                if let Some(&edge) = edge_index.get(&key) {
                    let span: ExactSpan = spans[edge as usize];
                    owners[edge as usize].push((source_id, span.start.0 != start_id));
                } else {
                    let edge =
                        u32::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?;
                    edge_index.insert(key, edge);
                    unique.push(segment);
                    spans.push(ExactSpan {
                        start: NodeId(start_id),
                        end: NodeId(end_id),
                        unique: edge,
                    });
                    owners.push(vec![(source_id, false)]);
                }
            }
        }
        let range = ring_start..ring_kinds.len();
        polygon_ring_ranges.push(range.clone());
        polygon_rings[operand as usize].push(range);
    }
    let source_ring_count = ring_kinds.len() - usize::from(rectangle.is_some());
    let mut offsets = Vec::with_capacity(owners.len() + 1);
    let mut entries = Vec::new();
    offsets.push(0);
    for edge_owners in owners {
        entries.extend(edge_owners);
        offsets.push(u32::try_from(entries.len()).map_err(|_| ResourceError::SizeOverflow)?);
    }
    let provenance = SegmentProvenance::from_csr(offsets, entries);
    let inventory = ExactSourceInventory {
        source_segments,
        unique,
        provenance: provenance.clone(),
        source_operands,
        source_rings,
        ring_kinds,
        ring_polygons,
        ring_holes,
        polygon_count: polygon_operands.len(),
        polygon_operands,
        polygon_ring_ranges,
        source_points: None,
        polygon_rings,
        ring_count: source_ring_count + usize::from(rectangle.is_some()),
        isolated: Vec::new(),
        isolated_points: Vec::new(),
        isolated_operands: Vec::new(),
        line_boundaries: Vec::new(),
    };
    let mut arena = PointArena::with_capacity(&inventory.unique, points.len());
    for point in &points {
        arena.push_grid(point)?;
    }
    let partition = ExactPartition {
        unique: &inventory.unique,
        points: arena,
        spans,
        provenance,
        isolated: Vec::new(),
        point_nodes: Vec::new(),
        point_sources: Vec::new(),
        winding_seed: None,
        ordinate_bindings: None,
        kept_nodes: KeptNodes::All,
        embedding_domain: embedding::EmbeddingDomain::Plane,
    };
    let (topology, edges) = partition.local_topology()?;
    let global = partition.global_faces(&topology, &edges)?;
    let mut occupied = vec![false; source_ring_count];
    if track_rings {
        partition.visit_face_windings(
            &topology,
            &edges,
            &global,
            &inventory.winding_source_rings(),
            inventory.ring_count,
            |_, winding| {
                if rectangle.is_none() || winding[source_ring_count] != 0 {
                    for (occupied, &value) in occupied.iter_mut().zip(winding) {
                        *occupied |= value != 0;
                    }
                }
                Ok(())
            },
        )?;
    }
    let operation = if rectangle.is_some() {
        OverlayOp::Intersection
    } else {
        OverlayOp::Union
    };
    let mut cells =
        inventory.select_cells(&partition, &topology, &edges, &global, operation, false)?;
    cells.spans.fill(false);
    cells.nodes.fill(false);
    let plan = cells.emission_plan(&partition, &topology, &edges, &global)?;
    let emission = plan.emission(&topology, &edges);
    let parts = plan.build_node_parts(&partition, &topology, &emission)?;
    let polygons = parts
        .polygons
        .into_iter()
        .map(|rings| {
            rings
                .into_iter()
                .map(|ring| {
                    ring.into_iter()
                        .map(|node| points[node.0 as usize].clone())
                        .collect()
                })
                .collect()
        })
        .collect();
    Ok((polygons, occupied))
}
