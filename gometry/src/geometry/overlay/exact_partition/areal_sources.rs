#[cfg(test)]
use std::ops::Range;

use super::{BoundaryPlan, ExactPartition, IndexedTopology, NodeId, Result};
#[cfg(test)]
use super::{Polygon, ResourceError, Segment};
#[cfg(test)]
use crate::geometry::Coordinates as _;
use crate::geometry::graph::GlobalFaces;

#[cfg(test)]
pub(super) struct ArealSources {
    pub(super) segments: Vec<Segment>,
    pub(super) source_rings: Vec<Option<u32>>,
    pub(super) polygon_rings: Vec<Range<usize>>,
    pub(super) ring_count: usize,
    pub(super) collapsed_rings: Vec<(u32, Range<usize>)>,
}

pub(super) struct ArealCells {
    pub(super) faces: Vec<bool>,
    pub(super) spans: Vec<bool>,
    pub(super) nodes: Vec<bool>,
}

pub(super) struct BoundarySelection {
    pub(super) cells: ArealCells,
    pub(super) boundary_nodes: Vec<NodeId>,
    pub(super) has_curve_support: bool,
    pub(super) has_point_support: bool,
}

impl ArealCells {
    pub(super) fn emission_plan(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<BoundaryPlan> {
        self.emission_plan_with_protected(partition, topology, edges, global, None, None)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Slot, twin, face, occupancy, and span IDs are established by the same topology and selection"
    )]
    pub(super) fn selected_boundary_spans(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Vec<bool> {
        let mut selected = Vec::with_capacity(self.spans.len());
        selected.resize(self.spans.len(), false);
        for (slot, &span) in edges.iter().enumerate() {
            let left = global.walk_faces[topology.face_of[slot] as usize] as usize;
            let twin = topology.twins[slot] as usize;
            let right = global.walk_faces[topology.face_of[twin] as usize] as usize;
            if self.faces[left] && !self.faces[right] {
                selected[span as usize] = true;
            }
        }
        selected
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "lower_dimensions returns admitted NodeIds and each topology column shares the partition node/face domains"
    )]
    pub(super) fn emission_plan_with_protected(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        extra_protected: Option<&[bool]>,
        preserved_spans: Option<&[bool]>,
    ) -> Result<BoundaryPlan> {
        let (lines, points) = self.lower_dimensions(partition, topology, edges, global);
        let mut plan = global.boundary_plan_with_protected(
            &lines,
            topology,
            edges,
            partition,
            &self.faces,
            extra_protected,
            preserved_spans,
        )?;
        plan.points.reserve(points.len());
        for node in points {
            let host = global.node_host_face(topology, node);
            plan.points.push((node, plan.face_quotient[host as usize]));
        }
        Ok(plan)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "edge and face IDs come from this partition's topology, while covered masks are sized from the same span and node counts"
    )]
    pub(super) fn lower_dimensions(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> (Vec<u32>, Vec<NodeId>) {
        let mut covered_spans = Vec::with_capacity(self.spans.len());
        covered_spans.resize(self.spans.len(), false);
        for (&span, &walk) in edges.iter().zip(&topology.face_of) {
            let face = global.walk_faces[walk as usize];
            covered_spans[span as usize] |= self.faces[face as usize];
        }
        let mut covered_nodes = Vec::with_capacity(self.nodes.len());
        covered_nodes.resize(self.nodes.len(), false);
        let mut lines = Vec::new();
        for (index, ((span, &selected), &covered)) in partition
            .spans
            .iter()
            .zip(&self.spans)
            .zip(&covered_spans)
            .enumerate()
        {
            if selected || covered {
                covered_nodes[span.start.0 as usize] = true;
                covered_nodes[span.end.0 as usize] = true;
            }
            if selected && !covered {
                lines.push(index as u32);
            }
        }
        for node in &partition.isolated {
            let component = topology.component_of[node.0 as usize];
            covered_nodes[node.0 as usize] |=
                self.faces[global.component_hosts[component as usize] as usize];
        }
        let mut points = Vec::new();
        for (index, (&selected, &covered)) in self.nodes.iter().zip(&covered_nodes).enumerate() {
            if selected && !covered {
                points.push(NodeId(index as u32));
            }
        }
        (lines, points)
    }
}

#[cfg(test)]
impl ArealSources {
    pub(super) fn new(polygons: &[impl AsRef<Polygon>]) -> Result<Self> {
        let mut sources = Self {
            segments: Vec::new(),
            source_rings: Vec::new(),
            polygon_rings: Vec::new(),
            ring_count: 0,
            collapsed_rings: Vec::new(),
        };
        sources.polygon_rings.reserve(polygons.len());
        for polygon in polygons {
            let first_ring = sources.ring_count;
            for ring in polygon.as_ref().rings() {
                let ring_id =
                    u32::try_from(sources.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
                sources.ring_count = sources
                    .ring_count
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                let collapsed = crate::geometry::ring_is_collinear(ring.coords());
                let first_segment = sources.segments.len();
                let count = ring.len().saturating_sub(1);
                let total = sources
                    .segments
                    .len()
                    .checked_add(count)
                    .ok_or(ResourceError::SizeOverflow)?;
                u32::try_from(total).map_err(|_| ResourceError::SizeOverflow)?;
                sources.segments.reserve(count);
                sources.source_rings.reserve(count);
                let first = ring
                    .iter_coords()
                    .next()
                    .expect("a Ring has at least four coordinates");
                let mut pairs = ring.segment_pairs().peekable();
                while let Some([start, mut end]) = pairs.next() {
                    if pairs.peek().is_none() {
                        end = first;
                    }
                    sources.segments.push(Segment {
                        start: start.xy(),
                        end: end.xy(),
                    });
                    sources.source_rings.push((!collapsed).then_some(ring_id));
                }
                if collapsed {
                    sources
                        .collapsed_rings
                        .push((ring_id, first_segment..sources.segments.len()));
                }
            }
            sources.polygon_rings.push(first_ring..sources.ring_count);
        }
        Ok(sources)
    }

    pub(super) fn occupied(&self, windings: &[i64], odd: bool) -> bool {
        let filled = |ring: usize| {
            if odd {
                windings[ring] % 2 != 0
            } else {
                windings[ring] != 0
            }
        };
        self.polygon_rings.iter().any(|rings| {
            let mut rings = rings.clone();
            rings.next().is_some_and(filled) && !rings.any(filled)
        })
    }

    pub(super) fn cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        odd: bool,
    ) -> Result<ArealCells> {
        let mut cells = ArealCells {
            faces: Vec::new(),
            spans: Vec::new(),
            nodes: Vec::new(),
        };
        cells.faces.reserve(global.count as usize);
        cells.faces.resize(global.count as usize, false);
        cells.spans.reserve(partition.spans.len());
        cells.spans.resize(partition.spans.len(), false);
        cells.nodes.reserve(partition.node_count());
        cells.nodes.resize(partition.node_count(), false);
        partition.visit_face_windings(
            topology,
            edges,
            global,
            &self.source_rings,
            self.ring_count,
            |face, winding| {
                cells.faces[face as usize] = self.occupied(winding, odd);
                Ok(())
            },
        )?;
        for (slot, &span) in edges.iter().enumerate() {
            let face = global.walk_faces[topology.face_of[slot] as usize];
            cells.spans[span as usize] |= cells.faces[face as usize];
        }
        for (index, span) in partition.spans.iter().enumerate() {
            cells.spans[index] |= partition
                .owners(index)
                .iter()
                .any(|&(source, _)| self.source_rings[source as usize].is_none());
            if cells.spans[index] {
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
        }
        for &(node, source) in &partition.point_sources {
            cells.nodes[node.0 as usize] |= self.source_rings[source as usize].is_none();
        }
        for node in &partition.isolated {
            let component = topology.component_of[node.0 as usize];
            let host = global.component_hosts[component as usize];
            cells.nodes[node.0 as usize] |= cells.faces[host as usize];
        }
        Ok(cells)
    }
}
