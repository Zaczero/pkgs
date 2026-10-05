//! Frozen point-location labels derived from an exact partition.

use super::{ExactPartition, ExactSourceInventory, GlobalFaces, NodeId};
use crate::error::{ResourceError, Result};
use crate::geometry::arrangement::IndexedTopology;
use crate::geometry::point_location::{
    AreaLocationLabels, ExactLocationCells, ExactLocationDart, ExactLocationEdge,
    ExactLocationPoints, SourceLocationLabels,
};
use crate::geometry::source_support::SourcePointLocation;
use crate::geometry::{GeometryErrorKind, Loc};

pub(in crate::geometry) type SourceLocationCells = ExactLocationCells<SourceLocationLabels>;
pub(in crate::geometry) type AreaLocationCells = ExactLocationCells<AreaLocationLabels>;

struct ExactLocationEdgeTable {
    edges: Vec<ExactLocationEdge>,
    node_fans: Vec<Vec<ExactLocationDart>>,
    node_hosts: Vec<u32>,
}

/// Select how Hausdorff fills area membership from the shared arrangement.
#[derive(Clone, Copy, Debug)]
pub(in crate::geometry) enum AreaLocationStrategy {
    /// Scan a strictly bounded set of selected face-boundary edges.
    BoundedScan,
    /// Build a persistent exact slab search for arbitrary arrangement sizes.
    PersistentSlabs,
}

impl AreaLocationStrategy {
    pub(in crate::geometry) const fn for_span_count(span_count: usize) -> Self {
        // Four undirected spans can contribute at most eight directed face
        // edges, so a complete exact scan is bounded for these tiny supports.
        const BOUNDED_SCAN_SPAN_LIMIT: usize = 4;

        if span_count <= BOUNDED_SCAN_SPAN_LIMIT {
            Self::BoundedScan
        } else {
            Self::PersistentSlabs
        }
    }
}

impl ExactPartition<'_> {
    /// Freeze exact homogeneous arrangement coordinates for metric support and
    /// its point locator to share.
    pub(in crate::geometry) fn freeze_location_points(&self) -> Result<ExactLocationPoints> {
        let mut points = Vec::with_capacity(self.node_count());
        for index in 0..self.node_count() {
            let node = NodeId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?);
            points.push(self.exact_owned(node)?);
        }
        Ok(points.into())
    }
}

impl ExactSourceInventory {
    /// Full source labels retain source occurrence semantics at spans and nodes.
    #[expect(
        clippy::indexing_slicing,
        reason = "face and span IDs in the partition facts index label columns built from the same exact partition"
    )]
    pub(in crate::geometry) fn source_location_cells(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<SourceLocationCells> {
        if partition.winding_seed.is_some() {
            return Err(GeometryErrorKind::Invalid(
                "full point location requires a complete source partition".into(),
            )
            .into());
        }
        let operands = self.operand_count();
        if operands != 1 {
            return Err(GeometryErrorKind::Invalid(
                "full point-location labels require one source operand".into(),
            )
            .into());
        }
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        let line_boundaries = self.node_source_flags(partition, facts.point_membership.len())?;
        let ExactLocationEdgeTable {
            edges: location_edges,
            node_fans,
            node_hosts,
        } = exact_location_edges(partition, &topology, &edges, &global)?;
        let faces = facts
            .face_membership
            .iter()
            .map(|&area| area_location(area))
            .collect();
        let spans = facts
            .span_faces
            .iter()
            .enumerate()
            .map(|(span, &[left, right])| {
                let location = self.span_location_from_facts(
                    0,
                    span,
                    left as usize,
                    right as usize,
                    &facts,
                    operands,
                );
                SourcePointLocation {
                    location,
                    area: facts.face_membership[left as usize]
                        || facts.face_membership[right as usize],
                }
            })
            .collect();
        let mut nodes = Vec::with_capacity(partition.node_count());
        for index in 0..partition.node_count() {
            nodes.push(self.node_location_and_area_from_facts(
                0,
                NodeId(index as u32),
                &topology,
                &edges,
                &global,
                &facts,
                &line_boundaries,
                &facts.point_membership,
            ));
        }
        ExactLocationCells::new(
            partition.freeze_location_points()?,
            location_edges,
            node_fans,
            node_hosts,
            SourceLocationLabels::new(faces, spans, nodes),
        )
    }

    /// Area-only labels project the already-selected union faces onto their
    /// closure; this shares the partition and topology already built by the
    /// metric-support owner.
    #[expect(
        clippy::indexing_slicing,
        reason = "face, span, node, and component IDs address tables emitted from this same exact partition and its global faces"
    )]
    pub(super) fn area_location_cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        occupied_faces: &[bool],
        points: ExactLocationPoints,
    ) -> Result<AreaLocationCells> {
        let ExactLocationEdgeTable {
            edges: location_edges,
            node_fans,
            node_hosts,
        } = exact_location_edges(partition, topology, edges, global)?;
        let mut occupied_spans = Vec::with_capacity(partition.spans.len());
        occupied_spans.resize(partition.spans.len(), false);
        let mut occupied_nodes = Vec::with_capacity(partition.node_count());
        occupied_nodes.resize(partition.node_count(), false);
        for (span_id, edge) in location_edges.iter().enumerate() {
            let [left, right] = edge.faces;
            let occupied = occupied_faces[left as usize] || occupied_faces[right as usize];
            occupied_spans[span_id] = occupied;
            if occupied {
                occupied_nodes[edge.start as usize] = true;
                occupied_nodes[edge.end as usize] = true;
            }
        }
        for node in &partition.isolated {
            let component = topology.component_of[node.0 as usize] as usize;
            occupied_nodes[node.0 as usize] |=
                occupied_faces[global.component_hosts[component] as usize];
        }
        ExactLocationCells::new(
            points,
            location_edges,
            node_fans,
            node_hosts,
            AreaLocationLabels::new(occupied_faces.to_vec(), occupied_spans, occupied_nodes),
        )
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "span, dart, face, and node IDs are emitted by this exact partition's admitted topology"
)]
fn exact_location_edges(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
) -> Result<ExactLocationEdgeTable> {
    let mut faces = Vec::with_capacity(partition.spans.len());
    faces.resize(partition.spans.len(), [u32::MAX; 2]);
    let mut node_fans = Vec::with_capacity(partition.node_count());
    node_fans.resize_with(partition.node_count(), Vec::new);
    for (slot, &span_id) in edges.iter().enumerate() {
        let span = partition.spans[span_id as usize];
        let left_face = global.walk_faces[topology.face_of[slot] as usize];
        node_fans[topology.owners[slot] as usize].push(ExactLocationDart {
            target: topology.targets[slot],
            span: span_id,
            left_face,
        });
        if topology.owners[slot] == span.start.0 {
            let twin = topology.twins[slot] as usize;
            faces[span_id as usize] = [
                left_face,
                global.walk_faces[topology.face_of[twin] as usize],
            ];
        }
    }
    let mut result = Vec::with_capacity(partition.spans.len());
    for (index, span) in partition.spans.iter().enumerate() {
        let [left, right] = faces[index];
        if left == u32::MAX || right == u32::MAX {
            return Err(GeometryErrorKind::Invalid(
                "exact point-location span has no oriented face pair".into(),
            )
            .into());
        }
        result.push(ExactLocationEdge {
            start: span.start.0,
            end: span.end.0,
            faces: [left, right],
        });
    }
    let mut node_hosts = Vec::with_capacity(partition.node_count());
    for index in 0..partition.node_count() {
        node_hosts.push(global.node_host_face(topology, NodeId(index as u32)));
    }
    Ok(ExactLocationEdgeTable {
        edges: result,
        node_fans,
        node_hosts,
    })
}

const fn area_location(area: bool) -> SourcePointLocation {
    SourcePointLocation {
        location: if area { Loc::Interior } else { Loc::Exterior },
        area,
    }
}
