//! Exact source partition of unique stored segments.
//!
//! Borrows unique undirected representatives and existing CSR provenance.
//! A node is a stored endpoint or the crossing of two unique segments. Every
//! node carries its nearest-even binary64, certified in double-double for a
//! crossing; a crossing's exact homogeneous point is built only when a
//! comparison ties on the rounded coordinates, since rounding is monotone.
//! Exact cyclic walks and leftward ray contacts define global faces; stored-ring
//! winding selects cells and an emission plan. Emission checks rounded
//! incidence, cyclic order, and global-face identity.
//!
//! Candidates come from the shared fallible sweep over unique segments plus
//! extra isolated points as zero-length items. Contact-derived cuts, compact
//! nodes, and canonical atoms are generated items. Sweep scratch is
//! [`ResourceError`], not the item cap.

use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::mem;
use std::ops::{ControlFlow, Range};

use crate::error::ResourceError;
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch, compare_departures};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::build_certified_overlay_shape;
use crate::geometry::overlay::noding::{
    SegmentProvenance, SegmentSource, dedup_undirected_segments_sourced, placement_offsets,
};
use crate::geometry::segments::stored_cross_sign;
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::{
    Bounds, Contact, CoordSeq, Coordinates as _, ExpansionBudget, GeometryErrorKind, GeometryKind,
    HashMap, LineSeq, Loc, MultiLineString, Point, PointKey, Polygon, PolygonizeFull, Result, Ring,
    RingKind, Segment, Shape, XY, for_each_candidate_pair, same_point, segment_contact_exact,
    single_chain,
};

mod construction;
mod coverage;
mod emission;
#[cfg(test)]
mod metrics;
mod rays;
mod relate;

use construction::{
    AxisValue, PointArena, interpolate_ordinate, parameter_along, partition_unique_segments,
};
#[cfg(test)]
use emission::PlannedChord;
use emission::{
    BoundaryPlan, CellFacts, CoverageFaceState, GlobalFaces, NodeParts,
    emitted_pair_preserves_incidence, polygonize_faces,
};
#[cfg(test)]
pub(in crate::geometry) use metrics::{
    partition_builds, partition_metrics, reset_partition_builds,
};
#[cfg(test)]
pub(crate) use metrics::{partition_calls, reset_partition_calls};
#[cfg(test)]
use metrics::{record_partition_call, update as update_partition_metrics};
use rays::{RayPoints as _, shoot_left};

#[cfg(test)]
use crate::geometry::segments::crossing_homogeneous;
#[cfg(test)]
use crate::geometry::tessellation::exact::{compare_ratios, round_ratio};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(in crate::geometry) struct NodeId(pub u32);

#[cfg(test)]
struct ArealSources {
    segments: Vec<Segment>,
    source_rings: Vec<Option<u32>>,
    polygon_rings: Vec<Range<usize>>,
    ring_count: usize,
    collapsed_rings: Vec<(u32, Range<usize>)>,
}

struct ArealCells {
    faces: Vec<bool>,
    spans: Vec<bool>,
    nodes: Vec<bool>,
}

impl ArealCells {
    fn emission_plan(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<BoundaryPlan> {
        self.emission_plan_with_protected(partition, topology, edges, global, None)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Slot, twin, face, occupancy, and span IDs are established by the same topology and selection"
    )]
    fn selected_boundary_spans(
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
    fn emission_plan_with_protected(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        extra_protected: Option<&[bool]>,
    ) -> Result<BoundaryPlan> {
        let (lines, points) = self.lower_dimensions(partition, topology, edges, global);
        let mut plan = global.boundary_plan_with_protected(
            &lines,
            topology,
            edges,
            partition,
            &self.faces,
            extra_protected,
        )?;
        plan.points.reserve(points.len());
        for node in points {
            let index = node.0 as usize;
            let first = topology.starts[index] as usize;
            let host = if topology.starts[index] == topology.starts[index + 1] {
                global.component_hosts[topology.component_of[index] as usize]
            } else {
                // All incident spans were omitted, so their face sectors merge.
                global.walk_faces[topology.face_of[first] as usize]
            };
            plan.points.push((node, plan.face_quotient[host as usize]));
        }
        Ok(plan)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "edge and face IDs come from this partition's topology, while covered masks are sized from the same span and node counts"
    )]
    fn lower_dimensions(
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
    fn new(polygons: &[impl AsRef<Polygon>]) -> Result<Self> {
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

    fn occupied(&self, windings: &[i64], odd: bool) -> bool {
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

    fn cells(
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

/// What a node is: a stored endpoint, or the crossing of two unique segments'
/// carriers, whose exact homogeneous point is built only for a comparison its
/// rounded coordinates cannot decide.
#[derive(Debug)]
pub(in crate::geometry) enum NodeRecipe {
    Stored,
    Crossing {
        segments: [u32; 2],
        exact: std::cell::OnceCell<Box<[ExactDyadic; 3]>>,
    },
}

#[derive(Clone, Copy)]
enum Axis {
    X,
    Y,
}

impl Axis {
    const fn of(self, point: XY) -> f64 {
        match self {
            Self::X => point.x,
            Self::Y => point.y,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::X => 0,
            Self::Y => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactSpan {
    pub start: NodeId,
    pub end: NodeId,
    pub unique: u32,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactMetricNode {
    pub homogeneous: [ExactDyadic; 3],
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactMetricSpan {
    pub start: ExactMetricNode,
    pub end: ExactMetricNode,
    pub start_parameter: [ExactDyadic; 2],
    pub end_parameter: [ExactDyadic; 2],
    pub source: Segment,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactMetricSupport {
    pub spans: Vec<ExactMetricSpan>,
    pub points: Vec<ExactMetricNode>,
    pub faces: Vec<Vec<Vec<ExactMetricNode>>>,
    /// Partition-span incidence selected by occupied/unoccupied global-face
    /// adjacency. The order is the same as `spans`; lower-dimensional support
    /// hidden by a positive-area face is deliberately absent. Ordinary metric
    /// support leaves this unbuilt.
    pub boundary_spans: Option<Vec<bool>>,
    /// Point incidence parallel to `points`. Positive-area support does not
    /// retain isolated points as a metric boundary target. Ordinary metric
    /// support leaves this unbuilt.
    pub boundary_points: Option<Vec<bool>>,
}

/// One maximal walk over exact spans, cut at an incidence junction or at a
/// change in the caller's ownership class. `nodes` contains the span
/// endpoints in walk order; a cyclic walk repeats its first node at the end.
#[derive(Debug)]
pub(in crate::geometry) struct ExactChainView {
    pub nodes: Vec<NodeId>,
    pub spans: Vec<u32>,
    pub shared: bool,
}

/// Exact incidence views used by coverage consumers. `chain_of_span` maps
/// every partition span to the one chain that owns its simplification
/// decision.
#[derive(Debug)]
pub(in crate::geometry) struct ExactChainViews {
    pub chains: Vec<ExactChainView>,
    pub chain_of_span: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactLineSource {
    pub operand: u32,
    pub reversed: bool,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactLineEmission {
    pub segments: Vec<Segment>,
    owners: Vec<ExactLineSource>,
    offsets: Vec<u32>,
}

impl ExactLineEmission {
    #[expect(
        clippy::indexing_slicing,
        reason = "the span ordinal is admitted by ExactPartition and offsets has one checked terminal entry"
    )]
    pub(in crate::geometry) fn sources(&self, span: usize) -> &[ExactLineSource] {
        let start = self.offsets[span] as usize;
        let end = self.offsets[span.wrapping_add(1)] as usize;
        &self.owners[start..end]
    }
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactPartition<'a> {
    unique: &'a [Segment],
    /// Every node's point, indexed by `NodeId`. A stored node keeps its first
    /// source spelling: `PointKey` canonicalizes signed zero for identity, and
    /// emission must not do the same to an endpoint already in the input.
    points: PointArena<'a>,
    pub spans: Vec<ExactSpan>,
    pub provenance: SegmentProvenance,
    pub isolated: Vec<NodeId>,
    /// Node ownership in explicit input-point occurrence order, including duplicates.
    pub point_nodes: Vec<NodeId>,
    pub point_sources: Vec<(NodeId, u32)>,
    binding_starts: Vec<u32>,
    /// The unique segments through each node, by `binding_starts`.
    bindings: Vec<u32>,
    kept_nodes: KeptNodes,
}

/// Which nodes emission keeps as vertices, beyond those selection requires.
#[derive(Debug)]
pub(in crate::geometry) enum KeptNodes {
    /// Only the nodes selection requires.
    Selected,
    /// Every node: coverage and polygonization publish each source vertex.
    All,
    /// Every node no polygon ring passes through, indexed by node: linework
    /// keeps its vertices whatever else shares the arrangement, while ring
    /// vertices on a straight run dissolve.
    OffRings(Vec<bool>),
}

/// Fallible Cartesian source inventory shared by overlay, relation, clipping,
/// repair, and raw linework consumers.
///
/// The inventory is deliberately source-oriented: duplicate/reversed segment
/// occurrences remain in `source_*` until the exact partition has attributed
/// them to one unique edge. Ring identity and operand identity therefore stay
/// available for winding and set-operation selection after noding.
pub(in crate::geometry) struct ExactSourceInventory {
    source_segments: Vec<Segment>,
    unique: Vec<Segment>,
    provenance: SegmentProvenance,
    source_operands: Vec<u32>,
    source_rings: Vec<Option<u32>>,
    source_ring_kinds: Vec<Option<RingKind>>,
    ring_kinds: Vec<RingKind>,
    ring_polygons: Vec<u32>,
    ring_holes: Vec<bool>,
    polygon_count: usize,
    polygon_operands: Vec<u32>,
    polygon_ring_ranges: Vec<Range<usize>>,
    source_points: Vec<[Point; 2]>,
    polygon_rings: Vec<Vec<Range<usize>>>,
    ring_count: usize,
    isolated: Vec<XY>,
    isolated_points: Vec<Point>,
    isolated_operands: Vec<u32>,
    line_boundaries: Vec<(u32, PointKey)>,
}

impl ExactSourceInventory {
    pub(in crate::geometry) fn from_shapes<S: Borrow<Shape>>(shapes: &[S]) -> Result<Self> {
        let mut staged = StagedExactSources::new(shapes.len());
        for (operand, shape) in shapes.iter().enumerate() {
            let operand = u32::try_from(operand).map_err(|_| ResourceError::SizeOverflow)?;
            staged.append_shape(Borrow::<Shape>::borrow(shape), operand)?;
        }
        Self::from_staged(staged)
    }

    /// Stage polygonal inputs as one operand for their union, or `None` when
    /// an input is not polygonal. Union selects a cell any operand occupies,
    /// and a polygonal operand is the union of its polygons, so the selection
    /// is unchanged while each per-operand cell fact keeps one entry instead
    /// of one per input. Line inputs keep their own operands: line boundary
    /// counts are per operand.
    pub(in crate::geometry) fn from_polygonal_union(shapes: &[&Shape]) -> Result<Option<Self>> {
        if !shapes
            .iter()
            .all(|shape| matches!(shape, Shape::Polygon(_) | Shape::MultiPolygon(_)))
        {
            return Ok(None);
        }
        let mut staged = StagedExactSources::new(1);
        for shape in shapes {
            staged.append_shape(shape, 0)?;
        }
        Self::from_staged(staged).map(Some)
    }

    fn from_staged(staged: StagedExactSources) -> Result<Self> {
        let (unique, provenance) =
            dedup_undirected_segments_sourced(staged.segments.iter().copied())?;
        Ok(Self {
            source_segments: staged.segments,
            unique,
            provenance,
            source_operands: staged.source_operands,
            source_rings: staged.source_rings,
            source_ring_kinds: staged.source_ring_kinds,
            ring_kinds: staged.ring_kinds,
            ring_polygons: staged.ring_polygons,
            ring_holes: staged.ring_holes,
            polygon_count: staged.polygon_count,
            polygon_operands: staged.polygon_operands,
            polygon_ring_ranges: staged.polygon_ring_ranges,
            source_points: staged.source_points,
            polygon_rings: staged.polygon_rings,
            ring_count: staged.ring_count,
            isolated: staged.isolated,
            isolated_points: staged.isolated_points,
            isolated_operands: staged.isolated_operands,
            line_boundaries: staged.line_boundaries,
        })
    }

    pub(in crate::geometry) fn from_polygon_operands<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
        left: &[P],
        right: &[Q],
    ) -> Result<Self> {
        let mut staged = StagedExactSources::new(2);
        for polygon in left {
            staged.append_polygon(polygon.as_ref(), 0)?;
        }
        for polygon in right {
            staged.append_polygon(polygon.as_ref(), 1)?;
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn from_segments(segments: &[Segment]) -> Result<Self> {
        let mut staged = StagedExactSources::new(1);
        staged.reserve_segments(segments.len());
        for &segment in segments {
            staged.push_segment(
                segment,
                &[segment.start.point(), segment.end.point()],
                0,
                None,
                None,
            );
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn from_raw_loops(loops: &[(Vec<f64>, Vec<f64>)]) -> Result<Self> {
        let mut staged = StagedExactSources::new(1);
        for (xs, ys) in loops {
            let ring_id =
                u32::try_from(staged.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
            staged.ring_count = staged
                .ring_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            // Raw winding callers pass open coordinate columns for a cyclic
            // loop; the final-to-first edge is part of that source contract.
            // This ingress is not the open-line inventory used by overlay.
            let closed = xs.len() >= 3;
            let coordinate_pairs = || xs.iter().copied().zip(ys.iter().copied());
            let mut endpoints = coordinate_pairs();
            let first = endpoints.next().map(|(x, y)| XY::new(x, y));
            let last = endpoints.next_back().map(|(x, y)| XY::new(x, y));
            let repeated_closure = closed
                && first
                    .zip(last)
                    .is_some_and(|(first, last)| same_point(first, last));
            let count = xs.len().saturating_sub(usize::from(repeated_closure));
            staged.ring_kinds.push(RingKind::Winding);
            staged.reserve_segments(count);
            let points = || {
                coordinate_pairs().take(count).map(|(x, y)| {
                    let point = Point::new_raw_xy(x, y);
                    (point.xy(), point)
                })
            };
            if let Some(first) = points().next() {
                for ((start_xy, start), (end_xy, end)) in
                    points().zip(points().skip(1).chain(std::iter::once(first)))
                {
                    staged.push_segment(
                        Segment {
                            start: start_xy,
                            end: end_xy,
                        },
                        &[start, end],
                        0,
                        closed.then_some(ring_id),
                        closed.then_some(RingKind::Winding),
                    );
                }
            }
        }
        Self::from_staged(staged)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source IDs and node IDs in the partition are admitted by source staging and PointArena"
    )]
    pub(in crate::geometry) fn partition(
        &self,
        budget: &mut ExpansionBudget,
    ) -> Result<ExactPartition<'_>> {
        #[cfg(test)]
        record_partition_call();
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.partition_builds = metrics.partition_builds.wrapping_add(1);
            metrics.source_segments = self.source_segments.len();
            metrics.unique_source_segments = self.unique.len();
        });
        let mut partition =
            partition_unique_segments(&self.unique, &self.provenance, &self.isolated, budget)?;
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.canonical_spans = partition.spans.len();
            metrics.canonical_nodes = partition.node_count();
        });
        let mut on_ring = Vec::with_capacity(partition.node_count());
        on_ring.resize(partition.node_count(), false);
        for (index, span) in partition.spans.iter().enumerate() {
            if partition
                .owners(index)
                .iter()
                .any(|&(source, _)| self.source_rings[source as usize].is_some())
            {
                on_ring[span.start.0 as usize] = true;
                on_ring[span.end.0 as usize] = true;
            }
        }
        partition.kept_nodes = KeptNodes::OffRings(on_ring);
        Ok(partition)
    }

    pub(in crate::geometry) fn source_segments(&self) -> &[Segment] {
        &self.source_segments
    }

    pub(in crate::geometry) fn source_operands(&self) -> &[u32] {
        &self.source_operands
    }

    /// Exact closed support for scalar metrics, before coordinate publication.
    ///
    /// Face walks carry filled membership; selected spans retain their original
    /// carriers and exact parameter windows.
    pub(in crate::geometry) fn metric_support(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<ExactMetricSupport> {
        self.metric_support_impl(partition, false)
    }

    /// Exact closed support plus the areal boundary incidence needed by
    /// directed filled-set metrics. Boundary flags follow global-face
    /// adjacency without constructing an emission plan.
    pub(in crate::geometry) fn metric_support_with_boundary(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<ExactMetricSupport> {
        self.metric_support_impl(partition, true)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node, span, walk, and dart IDs address the checked partition topology; boundary flags remain parallel to partition spans"
    )]
    fn metric_support_impl(
        &self,
        partition: &ExactPartition<'_>,
        with_boundary: bool,
    ) -> Result<ExactMetricSupport> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(
            partition,
            &topology,
            &edges,
            &global,
            crate::geometry::OverlayOp::Union,
            false,
        )?;
        let has_selected_area = with_boundary && cells.faces.iter().any(|&selected| selected);
        let selected_boundary_spans = with_boundary.then(|| {
            if has_selected_area {
                cells.selected_boundary_spans(&topology, &edges, &global)
            } else {
                cells.spans.clone()
            }
        });
        let mut spans = Vec::new();
        let mut boundary_spans = with_boundary.then(Vec::new);
        spans.reserve(cells.spans.iter().filter(|&&value| value).count());
        if let Some(boundary_spans) = boundary_spans.as_mut() {
            boundary_spans.reserve(cells.spans.iter().filter(|&&value| value).count());
        }
        let mut covered = Vec::with_capacity(partition.node_count());
        covered.resize(partition.node_count(), false);
        for (index, (span, &selected)) in partition.spans.iter().zip(&cells.spans).enumerate() {
            if !selected {
                continue;
            }
            let start = partition.exact_owned(span.start)?;
            let end = partition.exact_owned(span.end)?;
            let source = partition.unique[span.unique as usize];
            let start_parameter = parameter_along(source, &start)?;
            let end_parameter = parameter_along(source, &end)?;
            covered[span.start.0 as usize] = true;
            covered[span.end.0 as usize] = true;
            spans.push(ExactMetricSpan {
                start: ExactMetricNode { homogeneous: start },
                end: ExactMetricNode { homogeneous: end },
                start_parameter,
                end_parameter,
                source: partition.unique[span.unique as usize],
            });
            if let Some(boundary_spans) = boundary_spans.as_mut() {
                boundary_spans.push(
                    selected_boundary_spans
                        .as_ref()
                        .expect("boundary incidence was requested")[index],
                );
            }
        }
        let mut points = Vec::new();
        let mut boundary_points = with_boundary.then(Vec::new);
        points.reserve(cells.nodes.iter().filter(|&&value| value).count());
        for (index, (&selected, &is_covered)) in cells.nodes.iter().zip(&covered).enumerate() {
            if selected && !is_covered {
                points.push(ExactMetricNode {
                    homogeneous: partition.exact_owned(NodeId(index as u32))?,
                });
                if let Some(boundary_points) = boundary_points.as_mut() {
                    boundary_points.push(!has_selected_area);
                }
            }
        }
        let mut faces = Vec::with_capacity(global.count as usize);
        faces.resize_with(global.count as usize, Vec::new);
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            let face = global.walk_faces[walk] as usize;
            if face == 0 || !cells.faces[face] {
                continue;
            }
            let (start, end) = (start as usize, end as usize);
            let mut cycle = Vec::with_capacity(end.saturating_sub(start));
            for &slot in &topology.face_slots[start..end] {
                let node = NodeId(topology.owners[slot as usize]);
                cycle.push(ExactMetricNode {
                    homogeneous: partition.exact_owned(node)?,
                });
            }
            faces[face].push(cycle);
        }
        faces.retain(|cycles| !cycles.is_empty());
        Ok(ExactMetricSupport {
            spans,
            points,
            faces,
            boundary_spans,
            boundary_points,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node_bindings is a CSR row lookup for a NodeId admitted by the same ExactPartition"
    )]
    fn node_bindings<'a>(&self, node: NodeId, partition: &'a ExactPartition<'_>) -> &'a [u32] {
        let index = node.0 as usize;
        let start = partition.binding_starts[index] as usize;
        let end = partition.binding_starts[index + 1] as usize;
        &partition.bindings[start..end]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "unique, source, and point IDs come from this inventory's checked provenance and partition"
    )]
    fn resolve_node_point(
        &self,
        partition: &ExactPartition<'_>,
        node: NodeId,
        xy: XY,
        axes: crate::geometry::CoordinateAxes,
    ) -> Result<Point> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(Point::new_raw_xy(xy.x, xy.y));
        }
        let mut z = AxisValue::default();
        let mut m = AxisValue::default();
        let bindings = self.node_bindings(node, partition);
        if !bindings.is_empty() {
            let point = partition.exact(node)?;
            for &unique in bindings {
                // The parameter from the unique segment's start, and from its
                // end for a source stored in reverse.
                let [num, den] = parameter_along(partition.unique[unique as usize], &point)?;
                let from_end = den.clone().try_subtract(num.clone())?;
                for &(source, reversed) in self.provenance.owners(unique as usize) {
                    let points = self.source_points.get(source as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "exact partition ordinate binding references an unknown source".into(),
                        )
                    })?;
                    let along = if reversed { &from_end } else { &num };
                    let [start, end] = *points;
                    if axes.has_z() {
                        z.observe(interpolate_ordinate(start.z(), end.z(), along, &den)?);
                    }
                    if axes.has_m() {
                        m.observe(interpolate_ordinate(start.m(), end.m(), along, &den)?);
                    }
                }
            }
        }
        for &(bound, source) in &partition.point_sources {
            if bound != node {
                continue;
            }
            let source = source as usize;
            let points = self.source_points.get(source).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "exact partition point binding references an unknown source".into(),
                )
            })?;
            for point in points {
                if axes.has_z() {
                    z.observe(point.z());
                }
                if axes.has_m() {
                    m.observe(point.m());
                }
            }
        }
        if partition.point_nodes.len() != self.isolated_points.len() {
            return Err(GeometryErrorKind::Invalid(
                "exact partition isolated-point provenance is out of sync".into(),
            )
            .into());
        }
        for (index, &bound) in partition.point_nodes.iter().enumerate() {
            if bound != node {
                continue;
            }
            let point = self.isolated_points[index];
            if axes.has_z() {
                z.observe(point.z());
            }
            if axes.has_m() {
                m.observe(point.m());
            }
        }
        Ok(Point::new_raw_axes(
            xy.x,
            xy.y,
            crate::geometry::ZOrdinate(z.value()),
            crate::geometry::MOrdinate(m.value()),
        ))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the binding-start windows and node-aligned output are built from the same ExactPartition"
    )]
    fn ordinate_protected_nodes(
        &self,
        partition: &ExactPartition<'_>,
        axes: crate::geometry::CoordinateAxes,
    ) -> Result<Option<Vec<bool>>> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(None);
        }
        let mut protected = Vec::with_capacity(partition.node_count());
        protected.resize(partition.node_count(), false);
        for (node, &[start, end]) in partition.binding_starts.array_windows::<2>().enumerate() {
            for &unique in &partition.bindings[start as usize..end as usize] {
                let mut source_has_ordinate = false;
                for &(source, _) in self.provenance.owners(unique as usize) {
                    let points = self.source_points.get(source as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "exact partition ordinate binding references an unknown source".into(),
                        )
                    })?;
                    source_has_ordinate |= (axes.has_z()
                        && (points[0].z().is_some() || points[1].z().is_some()))
                        || (axes.has_m() && (points[0].m().is_some() || points[1].m().is_some()));
                }
                if source_has_ordinate {
                    protected[node] = true;
                    break;
                }
            }
        }
        Ok(Some(protected))
    }

    fn resolve_node_points(
        &self,
        partition: &ExactPartition<'_>,
        nodes: &[NodeId],
        rounded: impl Fn(NodeId) -> Result<XY>,
        declared_axes: crate::geometry::CoordinateAxes,
    ) -> Result<HashMap<NodeId, Point>> {
        let mut by_node =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        by_node.reserve(nodes.len());
        for &node in nodes {
            if by_node.contains_key(&node) {
                continue;
            }
            let point = self.resolve_node_point(partition, node, rounded(node)?, declared_axes)?;
            by_node.insert(node, point);
        }
        Ok(by_node)
    }

    fn winding_source_rings(&self) -> Vec<Option<u32>> {
        self.source_rings
            .iter()
            .zip(&self.source_ring_kinds)
            .map(|(&ring, &kind)| {
                matches!(kind, Some(RingKind::Winding))
                    .then(|| ring.expect("a ring source always carries its ring identity"))
            })
            .collect()
    }

    pub(in crate::geometry) fn line_emission(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<ExactLineEmission> {
        partition.emit_lines(&self.source_operands)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node pairs and resolved point entries are emitted from the same ExactPartition"
    )]
    pub(in crate::geometry) fn node_shape(
        &self,
        partition: &ExactPartition<'_>,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
    ) -> Result<Shape> {
        let mut items = Vec::with_capacity(partition.spans.len());
        items.extend(partition.spans.iter().map(|span| [span.start, span.end]));
        if items.is_empty() {
            return Ok(Shape::multi_line_string(declared_axes, Vec::new()));
        }
        // Certifies the rounded chords; the points below read the same rounding.
        partition.round_incident_items(&items)?;
        let mut nodes = Vec::new();
        let node_capacity = items
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        nodes.reserve(node_capacity);
        nodes.extend(items.iter().flatten().copied());
        let by_node = self.resolve_node_points(
            partition,
            &nodes,
            |node| Ok(partition.node_xy(node)),
            declared_axes,
        )?;
        let axes = bucket_axes(&nodes, &by_node, declared_axes, strict, "node")?;
        let mut lines = Vec::with_capacity(items.len());
        for item in items {
            let points = vec![
                with_axes(by_node[&item[0]], axes),
                with_axes(by_node[&item[1]], axes),
            ];
            lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
        }
        Ok(Shape::multi_line_string(axes, lines))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span, node, dart, face, and operand IDs come from the checked partition and its aligned diagnostic rows"
    )]
    pub(in crate::geometry) fn polygonize_diagnostics(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<PolygonizeFull> {
        let emission = self.line_emission(partition)?;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let mut used = Vec::with_capacity(partition.spans.len());
        used.resize(partition.spans.len(), false);
        for (slot, &span) in edges.iter().enumerate() {
            used[span as usize] |= global.walk_faces[topology.face_of[slot] as usize] != 0;
        }

        let mut degree = vec![0_usize; partition.node_count()];
        for span in &partition.spans {
            degree[span.start.0 as usize] = degree[span.start.0 as usize].saturating_add(1);
            degree[span.end.0 as usize] = degree[span.end.0 as usize].saturating_add(1);
        }

        let mut cuts = Vec::new();
        let mut dangles = Vec::new();
        let mut invalid_rings = Vec::new();
        for rows in [&mut cuts, &mut dangles, &mut invalid_rings] {
            rows.reserve(self.polygon_rings.len());
            rows.resize_with(self.polygon_rings.len(), Vec::new);
        }

        for (index, (span, &segment)) in partition.spans.iter().zip(&emission.segments).enumerate()
        {
            let owners = emission.sources(index);
            let mut rows = Vec::with_capacity(owners.len());
            for source in owners {
                if !rows.contains(&source.operand) {
                    rows.push(source.operand);
                }
            }
            for row in rows {
                let row = row as usize;
                if owners.len() > 1 {
                    invalid_rings[row].push(diagnostic_line(segment, true)?);
                } else if !used[index]
                    && (degree[span.start.0 as usize] <= 1 || degree[span.end.0 as usize] <= 1)
                {
                    dangles[row].push(diagnostic_line(segment, false)?);
                } else if !used[index] {
                    cuts[row].push(diagnostic_line(segment, false)?);
                }
            }
        }
        for rows in [&mut cuts, &mut dangles, &mut invalid_rings] {
            for row in rows {
                row.sort_unstable_by(compare_diagnostic_lines);
            }
        }

        Ok(PolygonizeFull {
            polygons: Vec::new(),
            cuts,
            dangles,
            invalid_rings,
        })
    }

    /// Measure the occupied Cartesian support without serializing rounded
    /// boundary coordinates. Face selection follows the same stored-ring
    /// winding fold as ordinary overlay; the selected homogeneous face
    /// integrals are accumulated as one exact rational before final rounding.
    #[expect(
        clippy::indexing_slicing,
        reason = "face, dart, twin, and node IDs address vectors built by this partition's checked topology"
    )]
    pub(in crate::geometry) fn occupied_cartesian_area(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<f64> {
        debug_assert_eq!(
            self.operand_count(),
            1,
            "occupied area is an operation over one composed operand"
        );
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        let mut occupancy = PolygonOccupancy::new(&self.polygon_rings, self.ring_count, false)?;
        partition.visit_face_windings_with_hook(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            &mut occupancy,
            |winding, ring, direction, occupancy| {
                occupancy.cross(winding, ring, direction);
                Ok(())
            },
            |face, _, occupancy| {
                selected[face as usize] = occupancy.operand_occupied(0);
                Ok(())
            },
        )?;
        let mut area = crate::geometry::ExactRationalArea::new();
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            if !selected[global.walk_faces[walk] as usize] {
                continue;
            }
            let range = start as usize..end as usize;
            for &slot in &topology.face_slots[range] {
                let slot = slot as usize;
                let twin_face =
                    global.walk_faces[topology.face_of[topology.twins[slot] as usize] as usize];
                if selected[twin_face as usize] {
                    continue;
                }
                area.add_homogeneous_cross(
                    &*partition.exact(NodeId(topology.owners[slot]))?,
                    &*partition.exact(NodeId(topology.targets[slot]))?,
                )?;
            }
        }
        area.round_abs_half()
    }

    /// Serialize the occupied Cartesian support from the same selected faces
    /// used by the exact fill selection. The caller may reuse this
    /// named owner for metrics whose conversion is not planar, such as the
    /// geographic area metric.
    pub(in crate::geometry) fn occupied_cartesian_support(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<Shape> {
        let (points, lines, polygons) =
            self.overlay_parts_with_fill(partition, crate::geometry::OverlayOp::Union)?;
        build_certified_overlay_shape(points, lines, polygons)
    }

    const fn operand_count(&self) -> usize {
        self.polygon_rings.len()
    }

    /// Select and serialize all relatively open cells for one Cartesian
    /// operation. The exact partition is the only incidence owner; this
    /// method never reconstructs a graph from rounded coordinates.
    pub(in crate::geometry) fn overlay_parts(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        self.overlay_parts_with_fill(partition, operation)
    }

    pub(in crate::geometry) fn overlay_shape(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
    ) -> Result<Shape> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(partition, &topology, &edges, &global, operation, false)?;
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let plan = cells.emission_plan_with_protected(
            partition,
            &topology,
            &edges,
            &global,
            ordinate_protected.as_deref(),
        )?;
        let parts = plan.emit_node_parts(partition, &topology)?;
        let line_node_count = parts.lines.iter().try_fold(0_usize, |count, line| {
            count
                .checked_add(line.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        let polygon_node_count = parts.polygons.iter().try_fold(0_usize, |count, polygon| {
            polygon.iter().try_fold(count, |count, ring| {
                count
                    .checked_add(ring.len())
                    .ok_or(ResourceError::SizeOverflow)
            })
        })?;
        let node_capacity = parts
            .points
            .len()
            .checked_add(line_node_count)
            .and_then(|count| count.checked_add(polygon_node_count))
            .ok_or(ResourceError::SizeOverflow)?;
        let mut nodes = Vec::with_capacity(node_capacity);
        nodes.extend(parts.points.iter().copied());
        for line in &parts.lines {
            nodes.extend(line.iter().copied());
        }
        for polygon in &parts.polygons {
            for ring in polygon {
                nodes.extend(ring.iter().copied());
            }
        }
        let mut rounded =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        rounded.reserve(parts.rounded.len());
        for &(node, point) in &parts.rounded {
            rounded.entry(node).or_insert(point);
        }
        let by_node = self.resolve_node_points(
            partition,
            &nodes,
            |node| {
                rounded.get(&node).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("exact overlay output lost rounded XY".into()).into()
                })
            },
            declared_axes,
        )?;
        self.node_parts_to_shape_resolved(parts, &by_node, declared_axes, strict, operation.name())
    }

    /// Select both closed children of one subdivision cut from one exact
    /// arrangement. The cut rectangles are separate polygon operands, but
    /// their seam and every source crossing share the same node recipes,
    /// topology, rounding pass, and ordinate resolution.
    #[expect(
        clippy::indexing_slicing,
        reason = "plan node IDs are admitted by this partition and index its node-aligned seen/coordinate columns"
    )]
    pub(in crate::geometry) fn subdivision_parts(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
    ) -> Result<[Shape; 2]> {
        let source_operand = 0;
        let operands = self.operand_count();
        if clip_operands
            .iter()
            .any(|&operand| operand >= operands || operand == source_operand)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision clip operand is out of range".into(),
            )
            .into());
        }

        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        if let Some(line_source) = LineSource::from_shape(source_shape) {
            return self.subdivision_line_parts(
                partition,
                line_source,
                &topology,
                &global,
                &facts,
                declared_axes,
                strict,
                clip_operands,
            );
        }
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;

        let mut plans = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells = self.select_subdivision_cells(
                partition,
                &topology,
                &global,
                &facts,
                source_operand,
                clip_operand,
            );
            plans.push(cells.emission_plan_with_protected(
                partition,
                &topology,
                &edges,
                &global,
                ordinate_protected.as_deref(),
            )?);
        }

        let mut seen = Vec::with_capacity(partition.node_count());
        seen.resize(partition.node_count(), false);
        let mut nodes = Vec::new();
        for plan in &plans {
            for [start, end] in plan.incident_items(&topology) {
                for node in [start, end] {
                    if !seen[node.0 as usize] {
                        seen[node.0 as usize] = true;
                        nodes.push(node);
                    }
                }
            }
        }
        let rounded_nodes = partition.round_nodes(&nodes);
        ExactPartition::check_node_injectivity(&nodes, &rounded_nodes)?;
        let mut coordinates = Vec::with_capacity(partition.node_count());
        coordinates.resize(partition.node_count(), None);
        for (&node, &coordinate) in nodes.iter().zip(&rounded_nodes) {
            coordinates[node.0 as usize] = Some(coordinate);
        }

        let mut node_parts = Vec::with_capacity(plans.len());
        for plan in plans {
            node_parts.push(plan.emit_node_parts_with_coordinates(
                partition,
                &topology,
                &coordinates,
            )?);
        }

        let mut rounded =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        rounded.reserve(nodes.len());
        let mut output_nodes = Vec::with_capacity(nodes.len());
        let mut output_seen = Vec::with_capacity(partition.node_count());
        output_seen.resize(partition.node_count(), false);
        for parts in &node_parts {
            for &(node, point) in &parts.rounded {
                rounded.entry(node).or_insert(point);
                if !output_seen[node.0 as usize] {
                    output_seen[node.0 as usize] = true;
                    output_nodes.push(node);
                }
            }
        }
        let by_node = self.resolve_node_points(
            partition,
            &output_nodes,
            |node| {
                rounded.get(&node).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("exact overlay output lost rounded XY".into()).into()
                })
            },
            declared_axes,
        )?;
        let mut parts = node_parts.into_iter();
        let first = self.node_parts_to_shape_resolved(
            parts
                .next()
                .expect("two subdivision plans produce two outputs"),
            &by_node,
            declared_axes,
            strict,
            "subdivide",
        )?;
        let second = self.node_parts_to_shape_resolved(
            parts
                .next()
                .expect("two subdivision plans produce two outputs"),
            &by_node,
            declared_axes,
            strict,
            "subdivide",
        )?;
        Ok([first, second])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "selected spans and node masks are sized from the same partition whose span and node IDs the routine visits"
    )]
    fn subdivision_line_parts(
        &self,
        partition: &ExactPartition<'_>,
        source: LineSource<'_>,
        topology: &IndexedTopology,
        global: &GlobalFaces,
        facts: &CellFacts,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
    ) -> Result<[Shape; 2]> {
        let mut selected = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells =
                self.select_subdivision_cells(partition, topology, global, facts, 0, clip_operand);
            selected.push(cells.spans);
        }

        let mut seen = Vec::with_capacity(partition.node_count());
        seen.resize(partition.node_count(), false);
        let mut nodes = Vec::new();
        for spans in &selected {
            for (index, &is_selected) in spans.iter().enumerate() {
                if !is_selected {
                    continue;
                }
                let span = partition.spans[index];
                for node in [span.start, span.end] {
                    if !seen[node.0 as usize] {
                        seen[node.0 as usize] = true;
                        nodes.push(node);
                    }
                }
            }
        }
        ExactPartition::check_node_injectivity(&nodes, &partition.round_nodes(&nodes))?;
        let by_node = self.resolve_node_points(
            partition,
            &nodes,
            |node| Ok(partition.node_xy(node)),
            declared_axes,
        )?;
        let views = partition.source_span_views(&self.source_segments)?;
        let ranges = source_line_ranges(source)?;
        let mut outputs = Vec::with_capacity(selected.len());
        for spans in selected {
            outputs.push(self.emit_source_line_shape(
                partition,
                source.kind(),
                &ranges,
                &views,
                &spans,
                &by_node,
                declared_axes,
                strict,
            )?);
        }
        let mut outputs = outputs.into_iter();
        Ok([
            outputs.next().expect("two subdivision line selections"),
            outputs.next().expect("two subdivision line selections"),
        ])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs and their selection bits are produced from the same ExactPartition, so both indexed columns share its span domain"
    )]
    fn emit_source_line_shape(
        &self,
        partition: &ExactPartition<'_>,
        source_kind: GeometryKind,
        ranges: &[Range<usize>],
        views: &[Vec<(u32, bool)>],
        selected: &[bool],
        by_node: &HashMap<NodeId, Point>,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
    ) -> Result<Shape> {
        let mut node_runs = Vec::with_capacity(selected.len());
        let mut source_offset = 0;
        for range in ranges {
            debug_assert_eq!(range.start, source_offset);
            source_offset = range.end;
            let mut run = Vec::new();
            for source in range.clone() {
                for &(span_id, reversed) in views.get(source).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact subdivision line provenance lost a source segment".into(),
                    )
                })? {
                    if !selected[span_id as usize] {
                        if run.len() >= 2 {
                            node_runs.push(std::mem::take(&mut run));
                        } else {
                            run.clear();
                        }
                        continue;
                    }
                    let span = partition.spans[span_id as usize];
                    let (start, end) = if reversed {
                        (span.end, span.start)
                    } else {
                        (span.start, span.end)
                    };
                    if run.last().copied() != Some(start) && run.len() >= 2 {
                        node_runs.push(std::mem::take(&mut run));
                    }
                    if run.is_empty() {
                        run.reserve(2);
                        run.push(start);
                    }
                    if run.last().copied() != Some(end) {
                        run.push(end);
                    }
                }
            }
            if run.len() >= 2 {
                node_runs.push(run);
            }
        }

        let mut line_nodes = Vec::new();
        for run in &node_runs {
            line_nodes.reserve(run.len());
            line_nodes.extend(run.iter().copied());
        }
        let line_axes = bucket_axes(&line_nodes, by_node, declared_axes, strict, "subdivide")?;
        let mut lines = Vec::with_capacity(node_runs.len());
        for run in node_runs {
            let mut points = Vec::with_capacity(run.len());
            for node in run {
                points.push(with_axes(by_node[&node], line_axes));
            }
            lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
        }
        if lines.is_empty() {
            return Ok(Shape::typed_empty(source_kind, declared_axes));
        }
        Ok(Shape::narrow_lines(line_axes, lines))
    }

    fn node_parts_to_shape_resolved(
        &self,
        parts: NodeParts,
        by_node: &HashMap<NodeId, Point>,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        operation: &str,
    ) -> Result<Shape> {
        let line_node_count = parts.lines.iter().try_fold(0_usize, |count, line| {
            count
                .checked_add(line.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        let polygon_node_count = parts.polygons.iter().try_fold(0_usize, |count, polygon| {
            polygon.iter().try_fold(count, |count, ring| {
                count
                    .checked_add(ring.len())
                    .ok_or(ResourceError::SizeOverflow)
            })
        })?;
        let point_axes = bucket_axes(&parts.points, by_node, declared_axes, strict, operation)?;
        let mut line_nodes = Vec::with_capacity(line_node_count);
        for line in &parts.lines {
            line_nodes.extend(line.iter().copied());
        }
        let line_axes = bucket_axes(&line_nodes, by_node, declared_axes, strict, operation)?;
        let mut polygon_nodes = Vec::with_capacity(polygon_node_count);
        for polygon in &parts.polygons {
            for ring in polygon {
                polygon_nodes.extend(ring.iter().copied());
            }
        }
        let polygon_axes = bucket_axes(&polygon_nodes, by_node, declared_axes, strict, operation)?;

        let mut points = Vec::with_capacity(parts.points.len());
        for node in parts.points {
            points.push(with_axes(by_node[&node], point_axes));
        }
        let mut lines = Vec::with_capacity(parts.lines.len());
        for line in parts.lines {
            let mut coordinates = Vec::with_capacity(line.len());
            for node in line {
                coordinates.push(with_axes(by_node[&node], line_axes));
            }
            lines.push(CoordSeq::try_from_points(&coordinates)?);
        }
        let mut polygons = Vec::with_capacity(parts.polygons.len());
        for polygon in parts.polygons {
            let mut rings = Vec::with_capacity(polygon.len());
            for ring in polygon {
                let mut coordinates = Vec::with_capacity(ring.len());
                for node in ring {
                    coordinates.push(with_axes(by_node[&node], polygon_axes));
                }
                rings.push(Ring::try_from_closed(CoordSeq::try_from_points(
                    &coordinates,
                )?)?);
            }
            let mut ring_iter = rings.into_iter();
            let shell = ring_iter.next().expect("polygon has a shell");
            let mut holes = Vec::with_capacity(ring_iter.size_hint().0);
            holes.extend(ring_iter);
            polygons.push(Polygon::new(shell, holes));
        }
        let point_part = (!points.is_empty())
            .then(|| Shape::narrow_points(point_axes, points))
            .transpose()?;
        let line_part = (!lines.is_empty()).then(|| {
            let mut proven = Vec::with_capacity(lines.len());
            for line in lines {
                proven.push(LineSeq::from_proven(line));
            }
            Shape::narrow_lines(line_axes, proven)
        });
        let polygon_part =
            (!polygons.is_empty()).then(|| Shape::narrow_polygons(polygon_axes, polygons));
        let mut output = Vec::with_capacity(3);
        output.extend([point_part, line_part, polygon_part].into_iter().flatten());
        match <[Shape; 1]>::try_from(output) {
            Ok([single]) => Ok(single),
            Err(parts) => Shape::geometry_collection(parts),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "topology IDs and CSR rows are built from this partition; CellFacts admission checked each row-major product and both operand IDs are validated by subdivision_parts"
    )]
    fn select_subdivision_cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        global: &GlobalFaces,
        facts: &CellFacts,
        source_operand: usize,
        clip_operand: usize,
    ) -> ArealCells {
        let operands = self.operand_count();
        let mut cells = ArealCells {
            faces: Vec::new(),
            spans: Vec::new(),
            nodes: Vec::new(),
        };
        cells.faces.reserve(global.count as usize);
        let source_faces = facts
            .face_membership
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_faces = facts
            .face_membership
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        cells.faces.extend(
            source_faces
                .zip(clip_faces)
                .map(|(&source, &clip)| source && clip),
        );
        cells.spans.reserve(partition.spans.len());
        let source_spans = facts
            .span_support
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_spans = facts
            .span_support
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        cells.spans.extend(
            source_spans
                .zip(clip_spans)
                .map(|(&source, &clip)| source && clip),
        );
        cells.nodes.reserve(partition.node_count());
        cells.nodes.resize(partition.node_count(), false);
        for (span, &selected) in partition.spans.iter().zip(&cells.spans) {
            if selected {
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
        }

        let source_points = facts
            .point_membership
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_points = facts
            .point_membership
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        let face_membership = |face: u32, operand: usize| {
            let offset = (face as usize).wrapping_mul(operands).wrapping_add(operand);
            facts.face_membership[offset]
        };
        for (((selected, starts), &component), (&source_point, &clip_point)) in cells
            .nodes
            .iter_mut()
            .zip(topology.starts.array_windows::<2>())
            .zip(&topology.component_of)
            .zip(source_points.zip(clip_points))
        {
            let &[start, end] = starts;
            let range = start as usize..end as usize;
            let memberships = |operand: usize, point_membership: bool| {
                let face_membership = if range.is_empty() {
                    let host = global.component_hosts[component as usize];
                    face_membership(host, operand)
                } else {
                    range.clone().any(|slot| {
                        let face = global.walk_faces[topology.face_of[slot] as usize];
                        face_membership(face, operand)
                    })
                };
                face_membership || point_membership
            };
            *selected |=
                memberships(source_operand, source_point) && memberships(clip_operand, clip_point);
        }
        cells
    }

    pub(in crate::geometry) fn structure_parts(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        self.overlay_parts_with_fill(partition, crate::geometry::OverlayOp::Union)
    }

    fn overlay_parts_with_fill(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(partition, &topology, &edges, &global, operation, false)?;
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        plan.emit_xy_parts(partition, &topology)
    }

    pub(in crate::geometry) fn repair_parts(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.linework_source_rings();
        let cells = self.select_cells_with_winding_rings(
            partition,
            &topology,
            &edges,
            &global,
            crate::geometry::OverlayOp::Union,
            true,
            &winding_rings,
        )?;
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        plan.emit_xy_parts(partition, &topology)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face and dart IDs are produced by the checked local/global topology built from this partition"
    )]
    pub(in crate::geometry) fn linework_polygons(
        &self,
        partition: &mut ExactPartition<'_>,
        build_area: bool,
    ) -> Result<Vec<Polygon>> {
        partition.kept_nodes = KeptNodes::All;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        if !build_area {
            return polygonize_faces(partition, &topology, &edges, &global);
        }
        let mut cycles: Vec<Vec<Vec<NodeId>>> = Vec::with_capacity(global.count as usize);
        cycles.resize_with(global.count as usize, Vec::new);
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            let (start, end) = (start as usize, end as usize);
            let mut cycle = Vec::with_capacity(end.saturating_sub(start));
            for &slot in &topology.face_slots[start..end] {
                cycle.push(NodeId(topology.owners[slot as usize]));
            }
            let face = global.walk_faces[walk] as usize;
            cycles[face].push(cycle);
        }
        let mut samples = Vec::with_capacity(global.count as usize);
        samples.resize(global.count as usize, None);
        for (face_cycles, sample_slot) in cycles.iter().zip(&mut samples).skip(1) {
            for &node in face_cycles.iter().flatten() {
                let earlier = match *sample_slot {
                    None => true,
                    Some(prior) => partition.compare_nodes(node, prior)?.is_lt(),
                };
                if earlier {
                    *sample_slot = Some(node);
                }
            }
        }
        // A face's nesting depth counts the other faces whose cycles contain
        // its sample. Rounding is monotone, so a cycle can contain the sample
        // only if the sample's rounded coordinates lie within the rounded
        // bounds of the cycle's nodes; samples sorted by x visit only those.
        let mut by_x = Vec::with_capacity(global.count as usize);
        for (face, sample) in samples.iter().enumerate() {
            if let Some(sample) = sample {
                by_x.push((partition.node_xy(*sample), face, *sample));
            }
        }
        by_x.sort_unstable_by(|left, right| left.0.x.total_cmp(&right.0.x));
        let mut depths = vec![0_u32; global.count as usize];
        for (other, other_cycles) in cycles.iter().enumerate().skip(1) {
            let Some([min_x, min_y, max_x, max_y]) = other_cycles
                .iter()
                .flatten()
                .map(|&node| partition.node_xy(node))
                .map(|xy| [xy.x, xy.y, xy.x, xy.y])
                .reduce(|[a, b, c, d], [x, y, z, w]| [a.min(x), b.min(y), c.max(z), d.max(w)])
            else {
                continue;
            };
            let first = by_x.partition_point(|(xy, ..)| xy.x < min_x);
            for &(xy, face, sample) in by_x.iter().skip(first) {
                if xy.x > max_x {
                    break;
                }
                if face == other || xy.y < min_y || xy.y > max_y {
                    continue;
                }
                for cycle in other_cycles {
                    if cycle_contains_node(partition, cycle, sample)? {
                        depths[face] = depths[face].wrapping_add(1);
                        break;
                    }
                }
            }
        }
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.extend(
            samples
                .iter()
                .zip(&depths)
                .enumerate()
                .map(|(face, (sample, &depth))| {
                    face != 0 && sample.is_some() && depth.is_multiple_of(2)
                }),
        );
        let cells = ArealCells {
            faces: selected,
            spans: {
                let mut values = Vec::with_capacity(partition.spans.len());
                values.resize(partition.spans.len(), false);
                values
            },
            nodes: {
                let mut values = Vec::with_capacity(partition.node_count());
                values.resize(partition.node_count(), false);
                values
            },
        };
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        let (_, _, polygons) = plan.emit_xy_parts(partition, &topology)?;
        Ok(polygons)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs yielded by visit_face_windings index the selected vector sized from GlobalFaces"
    )]
    pub(in crate::geometry) fn winding_polygons(
        &self,
        partition: &ExactPartition<'_>,
        keep: impl Fn(i64) -> bool,
    ) -> Result<Vec<Polygon>> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        partition.visit_face_windings(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            |face, winding| {
                selected[face as usize] = keep(winding.iter().sum());
                Ok(())
            },
        )?;
        self.emit_selected_winding_polygons(partition, &topology, &edges, &global, selected)
    }

    /// Select the union of independently wound polygon groups from one exact
    /// source partition. Each range contains one polygon's canonical shell
    /// followed by its canonical holes; a face belongs when any group's
    /// signed winding is negative.
    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs yielded by visit_face_windings index the selected vector sized from GlobalFaces"
    )]
    pub(in crate::geometry) fn winding_group_union_polygons(
        &self,
        partition: &ExactPartition<'_>,
        groups: &[Range<usize>],
    ) -> Result<Vec<Polygon>> {
        let mut cursor = 0_usize;
        for group in groups {
            if group.start != cursor || group.start >= group.end || group.end > self.ring_count {
                return Err(GeometryErrorKind::Invalid(
                    "exact winding polygon groups are not contiguous".into(),
                )
                .into());
            }
            cursor = group.end;
        }
        if cursor != self.ring_count {
            return Err(GeometryErrorKind::Invalid(
                "exact winding polygon groups do not cover all rings".into(),
            )
            .into());
        }
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        partition.visit_face_windings(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            |face, winding| {
                let mut occupied = false;
                for group in groups {
                    let mut sum = 0_i64;
                    for index in group.clone() {
                        let value = *winding.get(index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "exact winding group references an unknown ring".into(),
                            )
                        })?;
                        sum = sum.checked_add(value).ok_or(ResourceError::SizeOverflow)?;
                    }
                    occupied |= sum <= -1;
                    if occupied {
                        break;
                    }
                }
                selected[face as usize] = occupied;
                Ok(())
            },
        )?;
        self.emit_selected_winding_polygons(partition, &topology, &edges, &global, selected)
    }

    fn emit_selected_winding_polygons(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        selected: Vec<bool>,
    ) -> Result<Vec<Polygon>> {
        let cells = ArealCells {
            faces: selected,
            spans: {
                let mut values = Vec::with_capacity(partition.spans.len());
                values.resize(partition.spans.len(), false);
                values
            },
            nodes: {
                let mut values = Vec::with_capacity(partition.node_count());
                values.resize(partition.node_count(), false);
                values
            },
        };
        let plan = cells.emission_plan(partition, topology, edges, global)?;
        let (_, _, polygons) = plan.emit_xy_parts(partition, topology)?;
        Ok(polygons)
    }
}

pub(crate) fn occupied_cartesian_area(shape: &Shape) -> Result<f64> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("area", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.occupied_cartesian_area(&partition)
}

pub(crate) fn occupied_cartesian_support(shape: &Shape) -> Result<Shape> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("area", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.occupied_cartesian_support(&partition)
}

#[cfg(test)]
pub(crate) fn cartesian_support_nonempty(shape: &Shape) -> Result<bool> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("support", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let (topology, edges) = partition.local_topology()?;
    let global = partition.global_faces(&topology, &edges)?;
    let cells = inventory.select_cells(
        &partition,
        &topology,
        &edges,
        &global,
        crate::geometry::OverlayOp::Union,
        false,
    )?;
    Ok(cells.faces.iter().any(|&value| value)
        || cells.spans.iter().any(|&value| value)
        || cells.nodes.iter().any(|&value| value))
}

#[expect(
    clippy::indexing_slicing,
    reason = "the only indirect access is by a span ID produced by the same admitted partition"
)]
pub(crate) fn exact_intersects_bounds(shape: &Shape, bounds: Bounds) -> Result<bool> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("intersects", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    for [x, y] in bounds.corners() {
        if crate::geometry::cartesian_point_location(shape, Point::new_raw_xy(x, y))
            != Loc::Exterior
        {
            return Ok(true);
        }
    }
    let (topology, edges) = partition.local_topology()?;
    let global = partition.global_faces(&topology, &edges)?;
    let cells = inventory.select_cells(
        &partition,
        &topology,
        &edges,
        &global,
        crate::geometry::OverlayOp::Union,
        false,
    )?;
    for (index, &selected) in cells.nodes.iter().enumerate() {
        if selected && partition.node_in_bounds(NodeId(index as u32), bounds)? {
            return Ok(true);
        }
    }
    let [[x0, y0], [x1, y1], [x2, y2], [x3, y3]] = bounds.corners();
    let rectangle = [
        Segment {
            start: XY::new(x0, y0),
            end: XY::new(x1, y1),
        },
        Segment {
            start: XY::new(x1, y1),
            end: XY::new(x2, y2),
        },
        Segment {
            start: XY::new(x2, y2),
            end: XY::new(x3, y3),
        },
        Segment {
            start: XY::new(x3, y3),
            end: XY::new(x0, y0),
        },
    ];
    for (&span, &selected) in partition.spans.iter().zip(&cells.spans) {
        if !selected {
            continue;
        }
        let segment = partition.unique[span.unique as usize];
        if bounds.contains_xy(segment.start)
            || bounds.contains_xy(segment.end)
            || rectangle
                .iter()
                .copied()
                .any(|candidate| segment_contact_exact(segment, candidate) != Contact::Disjoint)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[expect(
    clippy::indexing_slicing,
    reason = "closed selects three points and open selects two from the matching fixed array"
)]
fn diagnostic_line(segment: Segment, closed: bool) -> Result<Shape> {
    let points = if closed {
        [segment.start, segment.end, segment.start]
    } else {
        [segment.start, segment.end, segment.end]
    };
    let count = if closed { 3 } else { 2 };
    Ok(Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(&points[..count])?,
    )))
}

fn compare_diagnostic_lines(left: &Shape, right: &Shape) -> Ordering {
    let key = |shape: &Shape| {
        let Shape::LineString(line) = shape else {
            unreachable!("polygonize diagnostics are line strings")
        };
        (
            PointKey::new(line.first().expect("diagnostic line has a start")),
            PointKey::new(line.point_at(1)),
        )
    };
    key(left).cmp(&key(right))
}

struct StagedExactSources {
    segments: Vec<Segment>,
    source_points: Vec<[Point; 2]>,
    source_operands: Vec<u32>,
    source_rings: Vec<Option<u32>>,
    source_ring_kinds: Vec<Option<RingKind>>,
    ring_kinds: Vec<RingKind>,
    ring_polygons: Vec<u32>,
    ring_holes: Vec<bool>,
    polygon_count: usize,
    polygon_operands: Vec<u32>,
    polygon_ring_ranges: Vec<Range<usize>>,
    polygon_rings: Vec<Vec<Range<usize>>>,
    ring_count: usize,
    isolated: Vec<XY>,
    isolated_points: Vec<Point>,
    isolated_operands: Vec<u32>,
    line_boundaries: Vec<(u32, PointKey)>,
}

impl StagedExactSources {
    fn new(operand_count: usize) -> Self {
        let mut polygon_rings = Vec::with_capacity(operand_count);
        polygon_rings.resize_with(operand_count, Vec::new);
        Self {
            segments: Vec::new(),
            source_points: Vec::new(),
            source_operands: Vec::new(),
            source_rings: Vec::new(),
            source_ring_kinds: Vec::new(),
            ring_kinds: Vec::new(),
            ring_polygons: Vec::new(),
            ring_holes: Vec::new(),
            polygon_count: 0,
            polygon_operands: Vec::new(),
            polygon_ring_ranges: Vec::new(),
            polygon_rings,
            ring_count: 0,
            isolated: Vec::new(),
            isolated_points: Vec::new(),
            isolated_operands: Vec::new(),
            line_boundaries: Vec::new(),
        }
    }

    fn append_shape(&mut self, shape: &Shape, operand: u32) -> Result<()> {
        shape.try_for_each_component(&mut |component| match component {
            crate::geometry::Component::Point(point) => {
                self.append_isolated_point(point, operand);
                Ok(())
            },
            crate::geometry::Component::LineString(line) => {
                self.append_line_coords(line.as_coords(), operand);
                Ok(())
            },
            crate::geometry::Component::Polygon(polygon) => self.append_polygon(polygon, operand),
        })
    }

    /// One point with no segment to carry it: the three isolated columns move
    /// together, so they reserve together.
    fn append_isolated_point(&mut self, point: Point, operand: u32) {
        self.isolated.push(point.xy());
        self.isolated_points.push(point);
        self.isolated_operands.push(operand);
    }

    fn append_line_coords(&mut self, line: &CoordSeq, operand: u32) {
        self.reserve_segments(line.coord_count().saturating_sub(1));
        let mut first = None;
        let mut last = None;
        for [start, end] in line.segment_pairs() {
            let (start_xy, end_xy) = (start.xy(), end.xy());
            if !same_point(start_xy, end_xy) {
                first.get_or_insert(start_xy);
                last = Some(end_xy);
            }
            self.push_segment(
                Segment {
                    start: start_xy,
                    end: end_xy,
                },
                &[start, end],
                operand,
                None,
                None,
            );
        }
        if first.is_none() {
            self.isolated.reserve(line.coord_count());
            self.isolated_points.reserve(line.coord_count());
            self.isolated_operands.reserve(line.coord_count());
            for point in line.iter_coords() {
                self.isolated.push(point.xy());
                self.isolated_points.push(point);
                self.isolated_operands.push(operand);
            }
            return;
        }
        if let (Some(first), Some(last)) = (first, last)
            && !same_point(first, last)
        {
            self.line_boundaries.reserve(2);
            self.line_boundaries.push((operand, PointKey::new(first)));
            self.line_boundaries.push((operand, PointKey::new(last)));
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "operand IDs are admitted by the source inventory's operand count"
    )]
    fn append_polygon(&mut self, polygon: &Polygon, operand: u32) -> Result<()> {
        let first_ring = self.ring_count;
        let polygon_id =
            u32::try_from(self.polygon_count).map_err(|_| ResourceError::SizeOverflow)?;
        self.polygon_count = self
            .polygon_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        self.polygon_operands.push(operand);
        for (ring_index, ring) in polygon.rings().enumerate() {
            let ring_id =
                u32::try_from(self.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
            self.ring_count = self
                .ring_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let kind = if crate::geometry::ring_is_collinear(ring.coords()) {
                RingKind::Collapsed
            } else {
                RingKind::Winding
            };
            self.ring_polygons.push(polygon_id);
            self.ring_holes.push(ring_index > 0);
            self.ring_kinds.push(kind);
            let count = ring.len().saturating_sub(1);
            self.reserve_segments(count);
            let mut pairs = ring.segment_pairs().peekable();
            let [first, first_end] = pairs.next().expect("a Ring has at least four coordinates");
            let mut pairs = std::iter::once([first, first_end]).chain(pairs).peekable();
            while let Some([start, mut end]) = pairs.next() {
                if pairs.peek().is_none() {
                    end = first;
                }
                self.push_segment(
                    Segment {
                        start: start.xy(),
                        end: end.xy(),
                    },
                    &[start, end],
                    operand,
                    Some(ring_id),
                    Some(kind),
                );
            }
        }
        self.polygon_rings[operand as usize].push(first_ring..self.ring_count);
        self.polygon_ring_ranges.push(first_ring..self.ring_count);
        Ok(())
    }

    fn reserve_segments(&mut self, count: usize) {
        self.segments.reserve(count);
        self.source_points.reserve(count);
        self.source_operands.reserve(count);
        self.source_rings.reserve(count);
        self.source_ring_kinds.reserve(count);
    }

    fn push_segment(
        &mut self,
        segment: Segment,
        source_points: &[Point; 2],
        operand: u32,
        ring: Option<u32>,
        ring_kind: Option<RingKind>,
    ) {
        self.segments.push(segment);
        self.source_points.push(*source_points);
        self.source_operands.push(operand);
        self.source_rings.push(ring);
        self.source_ring_kinds.push(ring_kind);
    }
}

fn select_operation(operation: crate::geometry::OverlayOp, membership: &[bool]) -> bool {
    match operation {
        crate::geometry::OverlayOp::Union => membership.iter().any(|&value| value),
        crate::geometry::OverlayOp::Intersection => membership.iter().all(|&value| value),
        crate::geometry::OverlayOp::Difference => {
            membership.first().copied().unwrap_or(false)
                && membership.iter().skip(1).all(|&value| !value)
        },
        crate::geometry::OverlayOp::SymmetricDifference => {
            membership.iter().filter(|&&value| value).count() % 2 == 1
        },
    }
}

const fn ring_occupied(winding: i64, odd: bool) -> bool {
    if odd { winding % 2 != 0 } else { winding != 0 }
}

/// Which polygons the face walk currently stands in, kept up to date as it
/// crosses one ring edge at a time: a polygon is occupied when its shell ring
/// is filled and none of its holes is, and every operand counts its occupied
/// polygons. A face then reads each operand in O(1) instead of rescanning
/// every polygon.
struct PolygonOccupancy {
    odd: bool,
    /// Owning polygon of each ring, and whether the ring is that polygon's shell.
    ring_polygon: Vec<Option<(u32, bool)>>,
    polygon_operand: Vec<u32>,
    shell_filled: Vec<bool>,
    filled_holes: Vec<u32>,
    occupied: Vec<u32>,
}

impl PolygonOccupancy {
    #[expect(
        clippy::indexing_slicing,
        reason = "staged ring IDs and polygon rows are assigned from ring/polygon identity ranges"
    )]
    fn new(polygon_rings: &[Vec<Range<usize>>], ring_count: usize, odd: bool) -> Result<Self> {
        let mut ring_polygon = Vec::with_capacity(ring_count);
        ring_polygon.resize(ring_count, None);
        let mut polygon_operand = Vec::new();
        for (operand, polygons) in polygon_rings.iter().enumerate() {
            for rings in polygons {
                let polygon = u32::try_from(polygon_operand.len())
                    .map_err(|_| ResourceError::SizeOverflow)?;
                for ring in rings.clone() {
                    ring_polygon[ring] = Some((polygon, ring == rings.start));
                }
                polygon_operand.push(operand as u32);
            }
        }
        let polygons = polygon_operand.len();
        let mut shell_filled = Vec::with_capacity(polygons);
        shell_filled.resize(polygons, false);
        let filled_holes = vec![0; polygons];
        let occupied = vec![0; polygon_rings.len()];
        Ok(Self {
            odd,
            ring_polygon,
            polygon_operand,
            shell_filled,
            filled_holes,
            occupied,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ring and polygon IDs in the walk were admitted by source staging and PolygonOccupancy::new"
    )]
    fn cross(&mut self, winding: &mut [i64], ring: u32, direction: i64) {
        let ring = ring as usize;
        let before = ring_occupied(winding[ring], self.odd);
        winding[ring] = winding[ring].wrapping_add(direction);
        let after = ring_occupied(winding[ring], self.odd);
        let Some((polygon, shell)) = self.ring_polygon[ring].filter(|_| before != after) else {
            return;
        };
        let polygon = polygon as usize;
        let was = self.shell_filled[polygon] && self.filled_holes[polygon] == 0;
        if shell {
            self.shell_filled[polygon] = after;
        } else if after {
            self.filled_holes[polygon] = self.filled_holes[polygon].wrapping_add(1);
        } else {
            self.filled_holes[polygon] = self.filled_holes[polygon].wrapping_sub(1);
        }
        let now = self.shell_filled[polygon] && self.filled_holes[polygon] == 0;
        if was != now {
            let count = &mut self.occupied[self.polygon_operand[polygon] as usize];
            if now {
                *count = count.wrapping_add(1);
            } else {
                *count = count.wrapping_sub(1);
            }
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "operand indexes are admitted from ExactSourceInventory::polygon_rings"
    )]
    fn operand_occupied(&self, operand: usize) -> bool {
        self.occupied[operand] != 0
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "ring IDs in each stored polygon range were admitted with the source ring inventory"
)]
fn rings_occupied(winding: &[i64], mut rings: Range<usize>, odd: bool) -> bool {
    rings
        .next()
        .is_some_and(|ring| ring_occupied(winding[ring], odd))
        && rings.all(|ring| !ring_occupied(winding[ring], odd))
}

fn bucket_axes(
    nodes: &[NodeId],
    points: &HashMap<NodeId, Point>,
    declared: crate::geometry::CoordinateAxes,
    strict: bool,
    operation: &str,
) -> Result<crate::geometry::CoordinateAxes> {
    if nodes.is_empty() {
        return Ok(crate::geometry::CoordinateAxes::XY);
    }
    let mut has_z = declared.has_z();
    let mut has_m = declared.has_m();
    for node in nodes {
        let point = points.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact overlay output lost a live node".into())
        })?;
        has_z &= point.z().is_some();
        has_m &= point.m().is_some();
    }
    let axes = crate::geometry::CoordinateAxes::new(
        crate::geometry::HasZ(has_z),
        crate::geometry::HasM(has_m),
    );
    if strict && axes != declared {
        return Err(GeometryErrorKind::ordinate_dropped(operation));
    }
    Ok(axes)
}

fn with_axes(point: Point, axes: crate::geometry::CoordinateAxes) -> Point {
    Point::new_raw_axes(
        point.x,
        point.y,
        crate::geometry::ZOrdinate(axes.has_z().then(|| point.z().expect("retained Z"))),
        crate::geometry::MOrdinate(axes.has_m().then(|| point.m().expect("retained M"))),
    )
}

const fn location(occupied: bool) -> Loc {
    if occupied {
        Loc::Interior
    } else {
        Loc::Exterior
    }
}

/// Certify a rounded embedding whose endpoint identities were decided by a
/// different exact construction (currently Voronoi). ExactPartition owns the
/// final noding/incidence pass; the external owner retains cocircular centers,
/// clipping policy, and fan order.
#[expect(
    clippy::indexing_slicing,
    reason = "candidate pair IDs address segments built one-for-one from the input items"
)]
pub(in crate::geometry) fn certify_emitted_incidence(items: &[([u32; 2], Segment)]) -> Result<()> {
    let mut coordinates: HashMap<u32, PointKey> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut by_coordinate: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let endpoint_capacity = items
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    coordinates.reserve(endpoint_capacity);
    by_coordinate.reserve(endpoint_capacity);
    let mut segments = Vec::with_capacity(items.len());
    for &([start_id, end_id], segment) in items {
        for (id, point) in [(start_id, segment.start), (end_id, segment.end)] {
            let key = PointKey::new(point);
            if let Some(previous) = coordinates.insert(id, key)
                && previous != key
            {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: id,
                    second: id,
                }
                .into());
            }
            if let Some(previous) = by_coordinate.insert(key, id)
                && previous != id
            {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: previous,
                    second: id,
                }
                .into());
            }
        }
        segments.push(segment);
    }
    let inventory = ExactSourceInventory::from_segments(&segments)?;
    let mut budget = ExpansionBudget::new("exact_partition", "Voronoi incidence");
    let partition = inventory.partition(&mut budget)?;
    drop(partition.local_topology()?);
    if let ControlFlow::Break((first, second)) =
        for_each_candidate_pair(&segments, single_chain, |left, right| {
            let endpoints = |index: usize| {
                let ([start, end], segment) = items[index];
                [(NodeId(start), segment.start), (NodeId(end), segment.end)]
            };
            if emitted_pair_preserves_incidence(endpoints(left), endpoints(right)) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break((left, right))
            }
        })?
    {
        return Err(GeometryErrorKind::ExactUnexpectedContact { first, second }.into());
    }
    Ok(())
}

fn cycle_contains_node(
    partition: &ExactPartition<'_>,
    cycle: &[NodeId],
    query: NodeId,
) -> Result<bool> {
    let mut inside = false;
    for (&a, &b) in cycle
        .iter()
        .zip(cycle.iter().skip(1).chain(cycle.iter().take(1)))
    {
        let y_a = partition.compare_axis(a, query, Axis::Y)?;
        let y_b = partition.compare_axis(b, query, Axis::Y)?;
        let a_is_below = y_a != Ordering::Greater;
        let b_is_below = y_b != Ordering::Greater;
        // The orientation decides only an edge that straddles the ray or
        // whose extent could hold the query.
        let may_hold = y_a != Ordering::Greater
            && y_b != Ordering::Less
            && partition.compare_axis(query, a, Axis::X)? != Ordering::Less
            && partition.compare_axis(query, b, Axis::X)? != Ordering::Greater;
        if a_is_below == b_is_below && !may_hold {
            continue;
        }
        let orientation = partition.orientation(a, b, query)?;
        if may_hold && orientation == ExactSign::Zero {
            return Ok(false);
        }
        if a_is_below == b_is_below {
            continue;
        }
        let crosses_left = (a_is_below && orientation == ExactSign::Negative)
            || (b_is_below && orientation == ExactSign::Positive);
        if crosses_left {
            inside = !inside;
        }
    }
    Ok(inside)
}

fn node_source_range(records: &[(NodeId, u32)], node: NodeId) -> Range<usize> {
    let start = records.partition_point(|(record_node, _)| record_node.0 < node.0);
    let count = records
        .iter()
        .skip(start)
        .take_while(|(record_node, _)| record_node.0 == node.0)
        .count();
    start..start.wrapping_add(count)
}

#[derive(Clone, Copy)]
enum LineSource<'a> {
    LineString(&'a LineSeq),
    MultiLineString(&'a MultiLineString),
}

impl<'a> LineSource<'a> {
    fn from_shape(shape: &'a Shape) -> Option<Self> {
        match shape {
            Shape::LineString(line) => Some(Self::LineString(line)),
            Shape::MultiLineString(lines) => Some(Self::MultiLineString(lines)),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        }
    }

    const fn kind(self) -> GeometryKind {
        match self {
            Self::LineString(_) => GeometryKind::LineString,
            Self::MultiLineString(_) => GeometryKind::MultiLineString,
        }
    }
}

fn source_line_ranges(source: LineSource<'_>) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::with_capacity(match source {
        LineSource::LineString(_) => 1,
        LineSource::MultiLineString(lines) => lines.len(),
    });
    let mut offset = 0_usize;
    let mut push = |length: usize| -> Result<()> {
        let end = offset
            .checked_add(length.saturating_sub(1))
            .ok_or(ResourceError::SizeOverflow)?;
        ranges.push(offset..end);
        offset = end;
        Ok(())
    };
    match source {
        LineSource::LineString(line) => push(line.len())?,
        LineSource::MultiLineString(lines) => {
            for line in lines.iter() {
                push(line.len())?;
            }
        },
    }
    Ok(ranges)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the incoming span and node IDs come from the exact chain walk and the CSR row is owned by its topology"
)]
fn next_incident_span(
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    node: NodeId,
    incoming: u32,
) -> Option<u32> {
    let range =
        topology.starts[node.0 as usize] as usize..topology.starts[node.0 as usize + 1] as usize;
    edge_of_slot[range]
        .iter()
        .copied()
        .find(|&span| span != incoming)
}

#[expect(
    clippy::indexing_slicing,
    reason = "span and node IDs are returned by the checked topology walk and address its aligned visit/pin columns"
)]
fn walk_exact_chain(
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    spans: &[ExactSpan],
    node_pin: &[bool],
    visited: &mut [bool],
    origin: NodeId,
    first: u32,
) -> Result<(Vec<NodeId>, Vec<u32>)> {
    let mut nodes = Vec::with_capacity(2);
    nodes.push(origin);
    let mut chain_spans = Vec::new();
    let mut current = origin;
    let mut span_id = first;
    loop {
        let span_index = span_id as usize;
        if visited[span_index] {
            return Err(
                GeometryErrorKind::Invalid("exact chain walk revisited a span".into()).into(),
            );
        }
        visited[span_index] = true;
        let span = spans[span_index];
        let target = if span.start == current {
            span.end
        } else if span.end == current {
            span.start
        } else {
            return Err(GeometryErrorKind::Invalid(
                "exact chain span is detached from its walk".into(),
            )
            .into());
        };
        chain_spans.push(span_id);
        nodes.push(target);
        if node_pin[target.0 as usize] || target == origin {
            break;
        }
        span_id = next_incident_span(topology, edge_of_slot, target, span_id).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact chain has no degree-two continuation".into())
        })?;
        current = target;
    }
    Ok((nodes, chain_spans))
}

impl ExactPartition<'_> {
    pub(in crate::geometry) const fn node_count(&self) -> usize {
        self.points.len()
    }

    /// A node's nearest-even binary64 point.
    #[expect(
        clippy::indexing_slicing,
        reason = "NodeId is admitted by this partition's PointArena"
    )]
    pub(in crate::geometry) fn node_xy(&self, node: NodeId) -> XY {
        self.points.rounded[node.0 as usize]
    }

    fn is_stored(&self, node: NodeId) -> bool {
        self.points.is_stored(node.0)
    }

    fn stored_key(&self, node: usize) -> Option<PointKey> {
        self.points.stored_key(node as u32)
    }

    /// A node's exact homogeneous point: built in place for a stored node,
    /// constructed once and kept for a crossing.
    pub(in crate::geometry) fn exact(&self, node: NodeId) -> Result<Cow<'_, [ExactDyadic; 3]>> {
        self.points.exact(node.0)
    }

    fn exact_owned(&self, node: NodeId) -> Result<[ExactDyadic; 3]> {
        match self.exact(node)? {
            Cow::Owned(point) => Ok(point),
            Cow::Borrowed(point) => Ok(point.clone()),
        }
    }

    fn compare_axis(&self, left: NodeId, right: NodeId, axis: Axis) -> Result<Ordering> {
        self.points.compare_axis(left.0, right.0, axis)
    }

    fn compare_axis_to(&self, node: NodeId, axis: Axis, value: f64) -> Result<Ordering> {
        self.points.compare_axis_to(node.0, axis, value)
    }

    fn orientation(&self, a: NodeId, b: NodeId, c: NodeId) -> Result<ExactSign> {
        self.points.orientation(a.0, b.0, c.0)
    }

    /// Return each source segment's exact atomic span run in source direction.
    ///
    /// The partition provenance is the only source-to-atom relation here.
    /// Ordering the inherited atoms from the stored source endpoint gives
    /// callers a view of the source spelling without rebuilding a coordinate
    /// graph.
    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs are assigned from self.spans; each zipped source segment owns one pre-sized view"
    )]
    pub(in crate::geometry) fn source_span_views(
        &self,
        source_segments: &[Segment],
    ) -> Result<Vec<Vec<(u32, bool)>>> {
        let mut views = Vec::with_capacity(source_segments.len());
        views.resize_with(source_segments.len(), Vec::new);
        for (span, owners) in self.provenance.iter().enumerate() {
            for &(source, reversed) in owners {
                let view = views.get_mut(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                view.push((span as u32, reversed));
            }
        }
        for (segment, view) in source_segments.iter().zip(&mut views) {
            if view.len() < 2 {
                continue;
            }
            // Each atom's entry is its node nearest the source start; ordered
            // along the source, every exit is the next atom's entry.
            let entry = |(span, reversed): (u32, bool)| {
                let span = self.spans[span as usize];
                if reversed { span.end.0 } else { span.start.0 }
            };
            let exit = |(span, reversed): (u32, bool)| {
                let span = self.spans[span as usize];
                if reversed { span.start } else { span.end }
            };
            let mut ordered = std::mem::take(view);
            self.points
                .order_along(*segment, &mut ordered, entry, |_, _| {
                    Err(GeometryErrorKind::Invalid(
                        "exact partition source atoms share an entry node".into(),
                    )
                    .into())
                })?;
            let first = NodeId(entry(ordered[0]));
            let last = exit(ordered[ordered.len().wrapping_sub(1)]);
            if self.stored_key(first.0 as usize) != Some(PointKey::new(segment.start))
                || self.stored_key(last.0 as usize) != Some(PointKey::new(segment.end))
            {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition source atoms do not reach their endpoint".into(),
                )
                .into());
            }
            if ordered
                .array_windows::<2>()
                .any(|&[left, right]| exit(left).0 != entry(right))
            {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition source atoms are not a contiguous run".into(),
                )
                .into());
            }
            *view = ordered;
        }
        Ok(views)
    }

    /// Build maximal chain views directly from exact span incidence.
    ///
    /// `source_operands` maps the partition's source ordinals to coverage row
    /// identities. An atom is shared only when its exact provenance names two
    /// distinct rows. A chain ends at every non-degree-two node and wherever
    /// that shared/exterior class changes.
    #[expect(
        clippy::indexing_slicing,
        reason = "span and node IDs come from this partition; degree-two continuation rows are built from its checked topology"
    )]
    pub(in crate::geometry) fn coverage_chain_views(
        &self,
        source_operands: &[u32],
    ) -> Result<ExactChainViews> {
        let (topology, edge_of_slot) = self.local_topology()?;
        let mut shared_by_span = Vec::with_capacity(self.spans.len());
        for owners in self.provenance.iter() {
            let mut first = None;
            let mut second = None;
            let mut count = 0_usize;
            for &(source, _) in owners {
                let operand = *source_operands.get(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                if Some(operand) == first || Some(operand) == second {
                    continue;
                }
                if first.is_none() {
                    first = Some(operand);
                } else if second.is_none() {
                    second = Some(operand);
                }
                count = count.wrapping_add(1);
            }
            shared_by_span.push(count == 2);
        }

        let mut node_pin = Vec::with_capacity(self.node_count());
        node_pin.resize(self.node_count(), false);
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let slots = start as usize..end as usize;
            node_pin[node] = slots.len() != 2
                || shared_by_span[edge_of_slot[slots.start] as usize]
                    != shared_by_span[edge_of_slot[slots.start.wrapping_add(1)] as usize];
        }

        let mut visited = Vec::with_capacity(self.spans.len());
        visited.resize(self.spans.len(), false);
        let mut chain_of_span = Vec::with_capacity(self.spans.len());
        chain_of_span.resize(self.spans.len(), u32::MAX);
        let mut chains = Vec::with_capacity(self.spans.len());

        for seed in 0..self.spans.len() {
            if visited[seed] {
                continue;
            }
            let span = self.spans[seed];
            let (origin, first) = if node_pin[span.start.0 as usize] {
                if node_pin[span.end.0 as usize]
                    && self.compare_nodes(span.end, span.start)? == Ordering::Less
                {
                    (span.end, seed as u32)
                } else {
                    (span.start, seed as u32)
                }
            } else if node_pin[span.end.0 as usize] {
                (span.end, seed as u32)
            } else {
                (span.start, seed as u32)
            };
            let (mut nodes, mut spans) = walk_exact_chain(
                &topology,
                &edge_of_slot,
                &self.spans,
                &node_pin,
                &mut visited,
                origin,
                first,
            )?;
            if !node_pin[origin.0 as usize] {
                let count = spans.len();
                let mut minimum = 0;
                for index in 1..count {
                    if self.compare_nodes(nodes[index], nodes[minimum])? == Ordering::Less {
                        minimum = index;
                    }
                }
                nodes[..count].rotate_left(minimum);
                nodes[count] = nodes[0];
                spans.rotate_left(minimum);
                if self.compare_nodes(nodes[1], nodes[count.wrapping_sub(1)])? == Ordering::Greater
                {
                    nodes[1..count].reverse();
                    spans.reverse();
                }
            }
            let shared = shared_by_span[seed];
            debug_assert!(
                spans
                    .iter()
                    .all(|&span| shared_by_span[span as usize] == shared)
            );
            let chain = u32::try_from(chains.len()).map_err(|_| ResourceError::SizeOverflow)?;
            for &span in &spans {
                chain_of_span[span as usize] = chain;
            }
            chains.push(ExactChainView {
                nodes,
                spans,
                shared,
            });
        }
        Ok(ExactChainViews {
            chains,
            chain_of_span,
        })
    }

    pub(in crate::geometry) fn owners(&self, span: usize) -> &[SegmentSource] {
        self.provenance.owners(span)
    }

    fn node_in_bounds(&self, node: NodeId, bounds: Bounds) -> Result<bool> {
        Ok(
            self.compare_axis_to(node, Axis::X, bounds.minx())? != Ordering::Less
                && self.compare_axis_to(node, Axis::X, bounds.maxx())? != Ordering::Greater
                && self.compare_axis_to(node, Axis::Y, bounds.miny())? != Ordering::Less
                && self.compare_axis_to(node, Axis::Y, bounds.maxy())? != Ordering::Greater,
        )
    }

    fn round_nodes(&self, nodes: &[NodeId]) -> Vec<XY> {
        nodes.iter().map(|&node| self.node_xy(node)).collect()
    }

    fn check_node_injectivity(nodes: &[NodeId], coordinates: &[XY]) -> Result<()> {
        debug_assert_eq!(nodes.len(), coordinates.len());
        let mut owners = HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        owners.reserve(nodes.len());
        for (&node, &coordinate) in nodes.iter().zip(coordinates) {
            if let Some(first) = owners.insert(PointKey::new(coordinate), node)
                && first != node
            {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: first.0,
                    second: node.0,
                }
                .into());
            }
        }
        Ok(())
    }

    /// Whether rounding moves any of `items`: only crossings round, so
    /// items between stored nodes keep the exact geometry whose injectivity,
    /// incidence, fans and faces the partition already established.
    fn rounding_moves(&self, items: &[[NodeId; 2]]) -> bool {
        let moved = items.iter().flatten().any(|&node| !self.is_stored(node));
        #[cfg(test)]
        if !moved {
            update_partition_metrics(|metrics| {
                metrics.unmoved_emissions = metrics.unmoved_emissions.wrapping_add(1);
            });
        }
        moved
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each item endpoint is a NodeId admitted by this partition and seen is sized from its PointArena"
    )]
    fn round_incident_items(&self, items: &[[NodeId; 2]]) -> Result<Vec<Segment>> {
        let mut segments = Vec::with_capacity(items.len());
        segments.extend(items.iter().map(|&[start, end]| Segment {
            start: self.node_xy(start),
            end: self.node_xy(end),
        }));
        if !self.rounding_moves(items) {
            return Ok(segments);
        }
        let mut seen = Vec::with_capacity(self.node_count());
        seen.resize(self.node_count(), false);
        let mut nodes = Vec::new();
        for &node in items.iter().flatten() {
            if !mem::replace(&mut seen[node.0 as usize], true) {
                nodes.push(node);
            }
        }
        Self::check_node_injectivity(&nodes, &self.round_nodes(&nodes))?;
        // A repeated node represents a live isolated point, not a new cut.
        if let ControlFlow::Break((first, second)) =
            for_each_candidate_pair(&segments, single_chain, |left, right| {
                let endpoints = |index: usize| {
                    let segment = segments[index];
                    [
                        (items[index][0], segment.start),
                        (items[index][1], segment.end),
                    ]
                };
                if emitted_pair_preserves_incidence(endpoints(left), endpoints(right)) {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break((left, right))
                }
            })?
        {
            return Err(GeometryErrorKind::ExactUnexpectedContact { first, second }.into());
        }
        Ok(segments)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "darts and span IDs are generated by local_topology and source operands match admitted provenance rows"
    )]
    fn emit_lines(&self, source_operands: &[u32]) -> Result<ExactLineEmission> {
        let mut items = Vec::with_capacity(self.spans.len());
        items.extend(self.spans.iter().map(|span| [span.start, span.end]));

        let (topology, edge_of_slot) = self.local_topology()?;
        let segments = self.round_incident_items(&items)?;

        let mut departures = Vec::with_capacity(self.spans.len());
        departures.resize(self.spans.len(), [u32::MAX; 2]);
        for (slot, &span) in edge_of_slot.iter().enumerate() {
            let span_index = span as usize;
            let span = self.spans[span_index];
            let slot = slot as u32;
            if topology.owners[slot as usize] == span.start.0 {
                departures[span_index][0] = slot;
            } else {
                debug_assert_eq!(topology.owners[slot as usize], span.end.0);
                departures[span_index][1] = slot;
            }
        }
        let mut darts = Vec::with_capacity(departures.len());
        for [start, end] in departures {
            debug_assert!(start != u32::MAX && end != u32::MAX);
            darts.push([start, end]);
        }
        if self.rounding_moves(&items) {
            Self::check_emitted_fans(&topology, &darts, &segments)?;
        }

        let mut offsets = Vec::new();
        let offset_capacity = self
            .spans
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        offsets.reserve(offset_capacity);
        offsets.push(0);
        let mut owners = Vec::new();
        for span_index in 0..self.spans.len() {
            for &(source, reversed) in self.owners(span_index) {
                let operand = source_operands[source as usize];
                owners.push(ExactLineSource { operand, reversed });
            }
            offsets.push(u32::try_from(owners.len()).map_err(|_| ResourceError::SizeOverflow)?);
        }
        Ok(ExactLineEmission {
            segments,
            owners,
            offsets,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "dart IDs and fan ranges are produced by local_topology and the emitted-dart vector is sized from those slots"
    )]
    fn check_emitted_fans(
        topology: &IndexedTopology,
        darts: &[[u32; 2]],
        segments: &[Segment],
    ) -> Result<()> {
        debug_assert_eq!(darts.len(), segments.len());
        // Emitted departures by topology slot.
        let mut departures: Vec<Option<(XY, XY)>> = Vec::with_capacity(topology.targets.len());
        departures.resize(topology.targets.len(), None);
        for (&[start, end], segment) in darts.iter().zip(segments) {
            debug_assert!(
                departures[start as usize].is_none() && departures[end as usize].is_none()
            );
            departures[start as usize] = Some((segment.start, segment.end));
            departures[end as usize] = Some((segment.end, segment.start));
        }
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let mut first = None;
            let mut previous = None;
            let mut descents = 0_usize;
            let mut count = 0_usize;
            for (target, departure) in
                (start..end).zip(&mut departures[start as usize..end as usize])
            {
                let Some((origin, point)) = departure.take() else {
                    continue;
                };
                let current = (target, point);
                first.get_or_insert((origin, current));
                if let Some((last_target, last_point)) = previous {
                    descents = descents.wrapping_add(usize::from(
                        compare_departures(origin, last_point, last_target, point, target)
                            == Ordering::Greater,
                    ));
                }
                previous = Some(current);
                count = count.wrapping_add(1);
            }
            if count > 2 {
                let (origin, (first_target, first_point)) =
                    first.expect("a node with multiple emitted darts has a first dart");
                let (last_target, last_point) =
                    previous.expect("a node with multiple emitted darts has a last dart");
                descents = descents.wrapping_add(usize::from(
                    compare_departures(origin, last_point, last_target, first_point, first_target)
                        == Ordering::Greater,
                ));
                // A cyclically sorted sequence crosses the positive-x cut once.
                if descents != 1 {
                    return Err(GeometryErrorKind::ExactCyclicOrder { node: node as u32 }.into());
                }
            }
        }
        debug_assert!(
            departures.iter().all(Option::is_none),
            "emitted chords must retain exact dart identities"
        );
        Ok(())
    }

    pub(in crate::geometry) fn compare_nodes(
        &self,
        left: NodeId,
        right: NodeId,
    ) -> Result<Ordering> {
        match self.compare_axis(left, right, Axis::X)? {
            Ordering::Less => Ok(Ordering::Less),
            Ordering::Equal => self.compare_axis(left, right, Axis::Y),
            Ordering::Greater => Ok(Ordering::Greater),
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact rounded coordinates are ordered by their stored binary64 values to keep emitted topology deterministic"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "departure indexes are span IDs in the same partition used to build the local topology"
    )]
    fn compare_departures(&self, origin: NodeId, left: usize, right: usize) -> Ordering {
        let departure = |index: usize| {
            let span = self.spans[index];
            debug_assert!(span.start == origin || span.end == origin);
            let source = self.unique[span.unique as usize];
            if span.start == origin {
                (source.start, source.end)
            } else {
                (source.end, source.start)
            }
        };
        // An atom follows its representative source's increasing parameter.
        let (a, b) = departure(left);
        let (c, d) = departure(right);
        let upper = |start: XY, end: XY| end.y > start.y || (end.y == start.y && end.x > start.x);
        let halves = upper(c, d).cmp(&upper(a, b));
        if halves != Ordering::Equal {
            return halves;
        }
        let sign = stored_cross_sign(a, b, c, d);
        debug_assert!(
            left == right || sign != ExactSign::Zero,
            "distinct atoms share a departure ray"
        );
        match sign {
            ExactSign::Positive => Ordering::Less,
            ExactSign::Negative => Ordering::Greater,
            ExactSign::Zero => Ordering::Equal,
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "NodeIds and edge IDs come from this partition; checked CSR degree prefixes bound every topology row and cursor"
    )]
    pub(in crate::geometry) fn local_topology(&self) -> Result<(IndexedTopology, Vec<u32>)> {
        let vertices = self.node_count();
        let slots = self
            .spans
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(slots).map_err(|_| ResourceError::SizeOverflow)?;
        let mut topology = IndexedTopology::default();
        let vertex_capacity = vertices.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        topology.starts.reserve(vertex_capacity);
        topology.starts.resize(vertex_capacity, 0);
        for span in &self.spans {
            let start = span.start.0.wrapping_add(1) as usize;
            topology.starts[start] = topology.starts[start]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let end = span.end.0.wrapping_add(1) as usize;
            topology.starts[end] = topology.starts[end]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut total = 0_u32;
        for offset in topology.starts.iter_mut().skip(1) {
            total = total
                .checked_add(*offset)
                .ok_or(ResourceError::SizeOverflow)?;
            *offset = total;
        }
        let mut cursor = Vec::with_capacity(vertices);
        cursor.extend(topology.starts.iter().take(vertices).copied());
        let mut edge_of_slot = vec![0; slots];
        for (edge, span) in self.spans.iter().enumerate() {
            for node in [span.start, span.end] {
                let node = node.0 as usize;
                edge_of_slot[cursor[node] as usize] = edge as u32;
                cursor[node] = cursor[node].wrapping_add(1);
            }
        }
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let range = start as usize..end as usize;
            // Degree-two nodes are ordered too: visibility wedges read the
            // angular origin.
            edge_of_slot[range].sort_unstable_by(|&left, &right| {
                self.compare_departures(NodeId(node as u32), left as usize, right as usize)
            });
        }
        topology.targets.reserve(slots);
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let range = start as usize..end as usize;
            for &edge in &edge_of_slot[range] {
                let span = self.spans[edge as usize];
                topology.targets.push(if span.start.0 == node as u32 {
                    span.end.0
                } else {
                    span.start.0
                });
            }
        }
        topology.owners.reserve(slots);
        topology.twins.reserve(slots);
        topology.component_of.reserve(vertices);
        topology.face_of.reserve(slots);
        topology
            .face_starts
            .reserve(slots.checked_add(1).ok_or(ResourceError::SizeOverflow)?);
        topology.face_slots.reserve(slots);
        let mut scratch = IndexedTopologyScratch::default();
        scratch.component_stack.reserve(vertices);
        scratch.successor.reserve(slots);
        scratch.edge_slots.reserve(self.spans.len());
        topology.finish(&edge_of_slot, &mut scratch);
        Ok((topology, edge_of_slot))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "component IDs are densely assigned by IndexedTopology::finish and minima is appended in that same order"
    )]
    fn component_minima(&self, topology: &IndexedTopology) -> Result<Vec<NodeId>> {
        let mut minima: Vec<NodeId> = Vec::new();
        for (node, &component) in topology.component_of.iter().enumerate() {
            let node = NodeId(node as u32);
            if component as usize == minima.len() {
                minima.push(node);
            } else {
                let minimum = &mut minima[component as usize];
                if self.compare_nodes(node, *minimum)? == Ordering::Less {
                    *minimum = node;
                }
            }
        }
        Ok(minima)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "root node and departure slots are IDs from this checked IndexedTopology"
    )]
    fn exterior_walk(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        root: NodeId,
    ) -> Result<Option<u32>> {
        let range = topology.starts[root.0 as usize] as usize
            ..topology.starts[root.0 as usize + 1] as usize;
        let mut chosen = None;
        let mut has_upper = false;
        for slot in range {
            let target = NodeId(topology.targets[slot]);
            let upper = self.compare_axis(target, root, Axis::Y)? != Ordering::Less;
            // The leftward wedge follows the highest upper ray, or the highest
            // lower ray when every departure points down and right.
            if chosen.is_none() || (upper && !has_upper) {
                chosen = Some(slot);
                has_upper = upper;
                continue;
            }
            if upper != has_upper {
                continue;
            }
            let prior = chosen.expect("a departure was selected before comparing its successor");
            if self.compare_departures(root, edges[prior] as usize, edges[slot] as usize)
                == Ordering::Less
            {
                chosen = Some(slot);
            }
        }
        Ok(chosen.map(|slot| topology.face_of[slot]))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs are bounded by the checked global walk count and links/labels are allocated to that count"
    )]
    fn global_faces(&self, topology: &IndexedTopology, edges: &[u32]) -> Result<GlobalFaces> {
        let minima = self.component_minima(topology)?;
        let walks = topology.face_starts.len().saturating_sub(1);
        let size = walks.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(size).map_err(|_| ResourceError::SizeOverflow)?;
        let mut links = Vec::with_capacity(size);
        links.extend(0..size as u32);
        let mut component_hosts = Vec::with_capacity(minima.len());
        // Each component's host is the face just east of the first span a
        // leftward ray from its minimum meets. One component has no other
        // component to meet: it lies in the unbounded face.
        let hits = if minima.len() == 1 {
            let none = vec![None];
            none
        } else {
            let mut queries = Vec::with_capacity(minima.len());
            queries.extend(minima.iter().map(|root| root.0));
            shoot_left(
                &self.points,
                self.spans.len(),
                |index| [self.spans[index].start.0, self.spans[index].end.0],
                &queries,
            )?
        };
        for (&root, hit) in minima.iter().zip(hits) {
            let host = hit.map_or(0, |span| {
                self.contact_walk(topology, edges, span).wrapping_add(1)
            });
            component_hosts.push(host);
            if let Some(exterior) = self.exterior_walk(topology, edges, root)? {
                // Every link reaches a component with a strictly smaller
                // minimum x, so exterior links cannot form a cycle.
                links[exterior.wrapping_add(1) as usize] = host;
            }
        }
        for start in 1..size {
            let mut root = start as u32;
            while links[root as usize] != root {
                root = links[root as usize];
            }
            let mut node = start as u32;
            while links[node as usize] != root {
                let next = links[node as usize];
                links[node as usize] = root;
                node = next;
            }
        }
        let mut labels = vec![0; size];
        let mut count = 1_u32;
        for node in 1..size {
            if links[node] == node as u32 {
                labels[node] = count;
                count = count.wrapping_add(1);
            }
        }
        for host in &mut component_hosts {
            *host = labels[links[*host as usize] as usize];
        }
        let mut walk_faces = Vec::with_capacity(walks);
        walk_faces.extend(links[1..].iter().map(|&root| labels[root as usize]));
        Ok(GlobalFaces {
            walk_faces,
            component_hosts,
            count,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "dart and edge IDs are generated by local_topology and provenance rows are bounded by its checked CSR"
    )]
    fn cross_face_winding<T>(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        source_rings: &[Option<u32>],
        winding: &mut [i64],
        slot: u32,
        direction: i64,
        state: &mut T,
        cross_ring: &mut impl FnMut(&mut [i64], u32, i64, &mut T) -> Result<()>,
    ) -> Result<()> {
        let slot = slot as usize;
        let edge = edges[slot] as usize;
        let backward = topology.owners[slot] != self.spans[edge].start.0;
        for &(source, reversed) in self.owners(edge) {
            if let Some(ring) = source_rings[source as usize] {
                let direction = if reversed ^ backward {
                    direction
                } else {
                    direction.wrapping_neg()
                };
                cross_ring(winding, ring, direction, state)?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ring IDs passed to the visit callback are admitted by source staging and winding is sized from ring_count"
    )]
    fn visit_face_windings(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        faces: &GlobalFaces,
        source_rings: &[Option<u32>],
        ring_count: usize,
        mut visit: impl FnMut(u32, &[i64]) -> Result<()>,
    ) -> Result<()> {
        let mut state = ();
        self.visit_face_windings_with_hook(
            topology,
            edges,
            faces,
            source_rings,
            ring_count,
            &mut state,
            |winding, ring, direction, ()| {
                let index = ring as usize;
                winding[index] = winding[index].wrapping_add(direction);
                Ok(())
            },
            |face, winding, ()| visit(face, winding),
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "walk, face, twin, and ring IDs index arrays sized from checked topology and source counts"
    )]
    fn visit_face_windings_with_hook<T>(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        faces: &GlobalFaces,
        source_rings: &[Option<u32>],
        ring_count: usize,
        state: &mut T,
        mut cross_ring: impl FnMut(&mut [i64], u32, i64, &mut T) -> Result<()>,
        mut visit: impl FnMut(u32, &[i64], &T) -> Result<()>,
    ) -> Result<()> {
        let count = faces.count as usize;
        let mut starts = Vec::new();
        let start_count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        starts.reserve(start_count);
        starts.resize(start_count, 0_usize);
        for &walk in &topology.face_of {
            let face = faces.walk_faces[walk as usize] as usize;
            let next = face.wrapping_add(1);
            starts[next] = starts[next].wrapping_add(1);
        }
        let mut total = 0_usize;
        for offset in starts.iter_mut().skip(1) {
            total = total.wrapping_add(*offset);
            *offset = total;
        }
        let mut cursor = Vec::with_capacity(count);
        cursor.extend_from_slice(&starts[..count]);
        let mut slots = vec![0_u32; edges.len()];
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let face = faces.walk_faces[walk as usize] as usize;
            slots[cursor[face]] = slot as u32;
            cursor[face] = cursor[face].wrapping_add(1);
        }
        let mut winding = vec![0_i64; ring_count];
        let mut visited = Vec::with_capacity(count);
        visited.resize(count, false);
        let mut stack = Vec::with_capacity(count);
        visited[0] = true;
        visit(0, &winding, state)?;
        stack.push((0_u32, starts[0], None));
        while let Some((face, next, incoming)) = stack.last_mut() {
            if *next == starts[(*face as usize).wrapping_add(1)] {
                if let Some(slot) = *incoming {
                    self.cross_face_winding(
                        topology,
                        edges,
                        source_rings,
                        &mut winding,
                        slot,
                        -1,
                        state,
                        &mut cross_ring,
                    )?;
                }
                stack.pop();
                continue;
            }
            let slot = slots[*next];
            *next = next.wrapping_add(1);
            let twin = topology.twins[slot as usize] as usize;
            let neighbor = faces.walk_faces[topology.face_of[twin] as usize];
            if visited[neighbor as usize] {
                continue;
            }
            self.cross_face_winding(
                topology,
                edges,
                source_rings,
                &mut winding,
                slot,
                1,
                state,
                &mut cross_ring,
            )?;
            visited[neighbor as usize] = true;
            visit(neighbor, &winding, state)?;
            stack.push((neighbor, starts[neighbor as usize], Some(slot)));
        }
        debug_assert!(visited.iter().all(|&seen| seen));
        debug_assert!(winding.iter().all(|&value| value == 0));
        Ok(())
    }

    /// The walk on the east side of `span`: the departure from its upper
    /// endpoint keeps that side on its left.
    #[expect(
        clippy::indexing_slicing,
        reason = "the contact span comes from the same partition and its departure row comes from local_topology"
    )]
    fn contact_walk(&self, topology: &IndexedTopology, edges: &[u32], span: u32) -> u32 {
        let atom = self.spans[span as usize];
        let source = self.unique[atom.unique as usize];
        let origin = if source.end.y > source.start.y {
            atom.end
        } else {
            atom.start
        };
        let slot = (topology.starts[origin.0 as usize] as usize
            ..topology.starts[origin.0 as usize + 1] as usize)
            .find(|&slot| edges[slot] == span)
            .expect("contact atom has a departure slot");
        topology.face_of[slot]
    }
}

#[cfg(test)]
mod tests;
