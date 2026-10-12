//! Separator incidence and chain assembly.

use super::{
    EdgeId, ExactEdgeChain, ExactSubdivision, Result, SiteId, VertexId, selected_cell_label,
    try_sort_by, voronoi_error, walk_label,
};

/// One separator group's atoms at each vertex, as `(vertex, slot, other end)`
/// sorted by vertex and then slot.
pub(super) type SeparatorIncidence = [(VertexId, usize, VertexId)];

pub(super) fn incident_at(incidence: &SeparatorIncidence, vertex: VertexId) -> &SeparatorIncidence {
    let start = incidence.partition_point(|&(known, ..)| known < vertex);
    let end = incidence.partition_point(|&(known, ..)| known <= vertex);
    #[expect(
        clippy::indexing_slicing,
        reason = "partition_point returns an ordered half-open range within this vertex-sorted incidence slice"
    )]
    &incidence[start..end]
}

#[expect(
    clippy::indexing_slicing,
    reason = "incidence slots were enumerated from this exact group and `used` has one flag per group edge"
)]
pub(super) fn walk_separator_from(
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    incidence: &SeparatorIncidence,
    used: &mut [bool],
    start: VertexId,
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut current = start;
    let mut vertices = Vec::new();
    let mut chain_edges = Vec::new();
    vertices.push(current);
    while let Some(&(_, slot, next)) = incident_at(incidence, current)
        .iter()
        .find(|&&(_, slot, _)| !used[slot])
    {
        used[slot] = true;
        chain_edges.push(group[slot].1);
        current = next;
        vertices.push(current);
    }
    if vertices.len() < 2 {
        return Err(voronoi_error("empty Voronoi edge chain"));
    }
    if vertices.first() == vertices.last() {
        return Err(voronoi_error("Voronoi separator forms a closed cycle"));
    }
    if vertices.iter().rev().lt(vertices.iter()) {
        vertices.reverse();
        chain_edges.reverse();
    }
    result.push(ExactEdgeChain {
        separator,
        vertices,
        edges: chain_edges,
    });
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "used has one flag per group edge, and both traversal passes obtain slots by enumerating that same group"
)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn assemble_separator_group(
    subdivision: &ExactSubdivision,
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut incidence = Vec::with_capacity(group.len().saturating_mul(2));
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        let [left, right] = subdivision.edge(*edge_id).endpoints;
        incidence.extend([(left, slot, right), (right, slot, left)]);
    }
    incidence.sort_unstable();
    if incidence
        .array_windows::<3>()
        .any(|[first, _, third]| first.0 == third.0)
    {
        return Err(voronoi_error("Voronoi separator branches"));
    }
    let mut used = Vec::with_capacity(group.len());
    used.resize(group.len(), false);
    let end_of_path = |vertex: VertexId| incident_at(&incidence, vertex).len() == 1;
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let [left, right] = subdivision.edge(*edge_id).endpoints;
        let start = if end_of_path(left) {
            left
        } else if end_of_path(right) {
            right
        } else {
            continue;
        };
        walk_separator_from(separator, group, &incidence, &mut used, start, result)?;
    }
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let [start, _] = subdivision.edge(*edge_id).endpoints;
        walk_separator_from(separator, group, &incidence, &mut used, start, result)?;
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "forward_slot is a dense edge-keyed map sized from subdivision.edges and populated from that same admitted edge_of_slot column"
)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn assemble_edges(
    subdivision: &ExactSubdivision,
) -> Result<Vec<ExactEdgeChain>> {
    let mut forward_slot = Vec::with_capacity(subdivision.edges.len());
    forward_slot.resize(subdivision.edges.len(), u32::MAX);
    for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
        let dart = subdivision.dart(slot as u32);
        let atomic = subdivision.edge(dart.edge);
        let [start, _] = atomic.endpoints;
        if dart.origin == start {
            forward_slot[edge as usize] = slot as u32;
        }
    }
    let mut selected = Vec::new();
    for (index, edge) in subdivision.edges.iter().enumerate() {
        let Some(separator) = edge.separator else {
            continue;
        };
        let slot = forward_slot[index] as usize;
        debug_assert_ne!(slot, u32::MAX as usize);
        let dart = subdivision.dart(slot as u32);
        let twin = subdivision.dart(dart.twin);
        let left = walk_label(subdivision, dart.face);
        let right = walk_label(subdivision, twin.face);
        if selected_cell_label(left) || selected_cell_label(right) {
            selected.push((separator, EdgeId(index as u32)));
        }
    }
    selected.sort_unstable();
    let mut result = Vec::new();
    for group in selected.chunk_by(|left, right| left.0 == right.0) {
        let separator = group
            .first()
            .expect("chunk_by returns non-empty separator groups")
            .0;
        assemble_separator_group(subdivision, separator, group, &mut result)?;
    }
    try_sort_by(&mut result, |left, right| {
        Ok(left
            .separator
            .cmp(&right.separator)
            .then_with(|| left.vertices.cmp(&right.vertices)))
    })?;
    Ok(result)
}
