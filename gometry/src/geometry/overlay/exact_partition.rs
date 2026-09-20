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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::mem;
use std::ops::{ControlFlow, Range};

use crate::error::ResourceError;
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch, compare_departures};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::build_certified_overlay_shape;
use crate::geometry::overlay::noding::{SegmentProvenance, SegmentSource};
use crate::geometry::segments::stored_cross_sign;
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::{
    Bounds, Contact, CoordSeq, Coordinates as _, ExpansionBudget, GeometryErrorKind, GeometryKind,
    HashMap, LineSeq, Loc, Point, PointKey, Polygon, PolygonizeFull, Result, Ring, RingKind,
    Segment, Shape, XY, for_each_candidate_pair, same_point, segment_contact_exact, single_chain,
};
use crate::{try_push_vec, try_reserve_vec};

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
pub(crate) use metrics::{
    boundary_plan_calls, partition_calls, reset_boundary_plan_calls, reset_partition_calls,
};
#[cfg(test)]
pub(in crate::geometry) use metrics::{
    partition_builds, partition_metrics, reset_partition_builds,
};
#[cfg(test)]
use metrics::{
    record_boundary_plan_call, record_partition_call, update as update_partition_metrics,
};
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

    fn selected_boundary_spans(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<Vec<bool>> {
        #[cfg(test)]
        record_boundary_plan_call();
        // `walks` come only from occupied/unoccupied face adjacency. The
        // separate `lines` list is produced by lower_dimensions for support
        // with no occupied face on either side, so it cannot carry an areal
        // shell or hole boundary.
        let plan = self.emission_plan(partition, topology, edges, global)?;
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, partition.spans.len())?;
        selected.resize(partition.spans.len(), false);
        for chord in plan.walks.iter().flatten() {
            for &slot in &plan.source_darts[chord.sources.clone()] {
                selected[edges[slot as usize] as usize] = true;
            }
        }
        Ok(selected)
    }

    fn emission_plan_with_protected(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        extra_protected: Option<&[bool]>,
    ) -> Result<BoundaryPlan> {
        let (lines, points) = self.lower_dimensions(partition, topology, edges, global)?;
        let mut plan = global.boundary_plan_with_protected(
            &lines,
            topology,
            edges,
            partition,
            &self.faces,
            extra_protected,
        )?;
        try_reserve_vec(&mut plan.points, points.len())?;
        for node in points {
            let index = node.0 as usize;
            let first = topology.starts[index] as usize;
            let host = if topology.starts[index] == topology.starts[index + 1] {
                global.component_hosts[topology.component_of[index] as usize]
            } else {
                // All incident spans were omitted, so their face sectors merge.
                global.walk_faces[topology.face_of[first] as usize]
            };
            try_push_vec(&mut plan.points, (node, plan.face_quotient[host as usize]))?;
        }
        Ok(plan)
    }

    fn lower_dimensions(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<(Vec<u32>, Vec<NodeId>)> {
        let mut covered_spans = Vec::new();
        try_reserve_vec(&mut covered_spans, self.spans.len())?;
        covered_spans.resize(self.spans.len(), false);
        for (slot, &span) in edges.iter().enumerate() {
            let face = global.walk_faces[topology.face_of[slot] as usize];
            covered_spans[span as usize] |= self.faces[face as usize];
        }
        let mut covered_nodes = Vec::new();
        try_reserve_vec(&mut covered_nodes, self.nodes.len())?;
        covered_nodes.resize(self.nodes.len(), false);
        let mut lines = Vec::new();
        for (index, span) in partition.spans.iter().enumerate() {
            if self.spans[index] || covered_spans[index] {
                covered_nodes[span.start.0 as usize] = true;
                covered_nodes[span.end.0 as usize] = true;
            }
            if self.spans[index] && !covered_spans[index] {
                try_reserve_vec(&mut lines, 1)?;
                try_push_vec(&mut lines, index as u32)?;
            }
        }
        for node in &partition.isolated {
            let component = topology.component_of[node.0 as usize];
            covered_nodes[node.0 as usize] |=
                self.faces[global.component_hosts[component as usize] as usize];
        }
        let mut points = Vec::new();
        for (index, &selected) in self.nodes.iter().enumerate() {
            if selected && !covered_nodes[index] {
                try_reserve_vec(&mut points, 1)?;
                try_push_vec(&mut points, NodeId(index as u32))?;
            }
        }
        Ok((lines, points))
    }
}

#[cfg(test)]
impl ArealSources {
    #[expect(
        clippy::float_cmp,
        reason = "Ring closure is an exact coordinate identity, not an approximate comparison"
    )]
    fn new(polygons: &[impl AsRef<Polygon>]) -> Result<Self> {
        let mut sources = Self {
            segments: Vec::new(),
            source_rings: Vec::new(),
            polygon_rings: Vec::new(),
            ring_count: 0,
            collapsed_rings: Vec::new(),
        };
        try_reserve_vec(&mut sources.polygon_rings, polygons.len())?;
        for polygon in polygons {
            let first_ring = sources.ring_count;
            for ring in polygon.as_ref().rings() {
                let ring_id =
                    u32::try_from(sources.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
                sources.ring_count = sources
                    .ring_count
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                let (xs, ys) = (ring.xs(), ring.ys());
                let collapsed = crate::geometry::ring_is_collinear(ring.coords());
                let first_segment = sources.segments.len();
                let mut count = xs.len();
                if count > 1 && xs[0] == xs[count - 1] && ys[0] == ys[count - 1] {
                    count -= 1;
                }
                let total = sources
                    .segments
                    .len()
                    .checked_add(count)
                    .ok_or(ResourceError::SizeOverflow)?;
                u32::try_from(total).map_err(|_| ResourceError::SizeOverflow)?;
                try_reserve_vec(&mut sources.segments, count)?;
                try_reserve_vec(&mut sources.source_rings, count)?;
                for from in 0..count {
                    let to = if from + 1 == count { 0 } else { from + 1 };
                    try_push_vec(&mut sources.segments, Segment {
                        start: XY::new(xs[from], ys[from]),
                        end: XY::new(xs[to], ys[to]),
                    })?;
                    try_push_vec(&mut sources.source_rings, (!collapsed).then_some(ring_id))?;
                }
                if collapsed {
                    try_reserve_vec(&mut sources.collapsed_rings, 1)?;
                    try_push_vec(
                        &mut sources.collapsed_rings,
                        (ring_id, first_segment..sources.segments.len()),
                    )?;
                }
            }
            try_push_vec(&mut sources.polygon_rings, first_ring..sources.ring_count)?;
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
        try_reserve_vec(&mut cells.faces, global.count as usize)?;
        cells.faces.resize(global.count as usize, false);
        try_reserve_vec(&mut cells.spans, partition.spans.len())?;
        cells.spans.resize(partition.spans.len(), false);
        try_reserve_vec(&mut cells.nodes, partition.node_count())?;
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
    /// Partition-span incidence selected by the filled union's boundary
    /// walks. The order is the same as `spans`; lower-dimensional support
    /// hidden by a positive-area face is deliberately absent. Ordinary
    /// metric support leaves this unbuilt.
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
    pub(in crate::geometry) fn sources(&self, span: usize) -> &[ExactLineSource] {
        let start = self.offsets[span] as usize;
        let end = self.offsets[span + 1] as usize;
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
    pub(in crate::geometry) fn from_shapes(shapes: &[&Shape]) -> Result<Self> {
        let mut staged = StagedExactSources::new(shapes.len())?;
        for (operand, shape) in shapes.iter().enumerate() {
            let operand = u32::try_from(operand).map_err(|_| ResourceError::SizeOverflow)?;
            staged.append_shape(shape, operand)?;
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
        let mut staged = StagedExactSources::new(1)?;
        for shape in shapes {
            staged.append_shape(shape, 0)?;
        }
        Self::from_staged(staged).map(Some)
    }

    fn from_staged(staged: StagedExactSources) -> Result<Self> {
        let (unique, provenance) = dedup_exact_segments(&staged.segments)?;
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
        let mut staged = StagedExactSources::new(2)?;
        for polygon in left {
            staged.append_polygon(polygon.as_ref(), 0)?;
        }
        for polygon in right {
            staged.append_polygon(polygon.as_ref(), 1)?;
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn from_segments(segments: &[Segment]) -> Result<Self> {
        let mut staged = StagedExactSources::new(1)?;
        for &segment in segments {
            staged.push_segment(
                segment,
                &[segment.start.point(), segment.end.point()],
                0,
                None,
                None,
            )?;
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn from_raw_loops(loops: &[(Vec<f64>, Vec<f64>)]) -> Result<Self> {
        let mut staged = StagedExactSources::new(1)?;
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
            let count = xs.len().saturating_sub(usize::from(
                closed
                    && same_point(
                        XY::new(xs[0], ys[0]),
                        XY::new(xs[xs.len() - 1], ys[ys.len() - 1]),
                    ),
            ));
            try_reserve_vec(&mut staged.ring_kinds, 1)?;
            try_push_vec(&mut staged.ring_kinds, RingKind::Winding)?;
            for index in 0..count {
                let next = if index + 1 == count { 0 } else { index + 1 };
                staged.push_segment(
                    Segment {
                        start: XY::new(xs[index], ys[index]),
                        end: XY::new(xs[next], ys[next]),
                    },
                    &[
                        Point::new_raw_xy(xs[index], ys[index]),
                        Point::new_raw_xy(xs[next], ys[next]),
                    ],
                    0,
                    closed.then_some(ring_id),
                    closed.then_some(RingKind::Winding),
                )?;
            }
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn partition(
        &self,
        budget: &mut ExpansionBudget,
    ) -> Result<ExactPartition<'_>> {
        #[cfg(test)]
        record_partition_call();
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.partition_builds += 1;
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
        let mut on_ring = Vec::new();
        try_reserve_vec(&mut on_ring, partition.node_count())?;
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
    /// directed inradius. The boundary plan is built here, beside the face
    /// selection, rather than reconstructed by a metric consumer.
    pub(in crate::geometry) fn metric_support_with_boundary(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<ExactMetricSupport> {
        self.metric_support_impl(partition, true)
    }

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
        let mut selected_boundary_spans = if with_boundary {
            Some(cells.selected_boundary_spans(partition, &topology, &edges, &global)?)
        } else {
            None
        };
        let has_selected_area = with_boundary && cells.faces.iter().any(|&selected| selected);
        if !has_selected_area
            && let Some(selected_boundary_spans) = selected_boundary_spans.as_mut()
        {
            for (selected, &cell) in selected_boundary_spans.iter_mut().zip(&cells.spans) {
                *selected = cell;
            }
        }
        let mut spans = Vec::new();
        let mut boundary_spans = with_boundary.then(Vec::new);
        try_reserve_vec(
            &mut spans,
            cells.spans.iter().filter(|&&value| value).count(),
        )?;
        if let Some(boundary_spans) = boundary_spans.as_mut() {
            try_reserve_vec(
                boundary_spans,
                cells.spans.iter().filter(|&&value| value).count(),
            )?;
        }
        let mut covered = Vec::new();
        try_reserve_vec(&mut covered, partition.node_count())?;
        covered.resize(partition.node_count(), false);
        for (index, &selected) in cells.spans.iter().enumerate() {
            if !selected {
                continue;
            }
            let span = partition.spans[index];
            let start = partition.exact_owned(span.start)?;
            let end = partition.exact_owned(span.end)?;
            let source = partition.unique[span.unique as usize];
            let start_parameter = parameter_along(source, &start)?;
            let end_parameter = parameter_along(source, &end)?;
            covered[span.start.0 as usize] = true;
            covered[span.end.0 as usize] = true;
            try_push_vec(&mut spans, ExactMetricSpan {
                start: ExactMetricNode { homogeneous: start },
                end: ExactMetricNode { homogeneous: end },
                start_parameter,
                end_parameter,
                source: partition.unique[span.unique as usize],
            })?;
            if let Some(boundary_spans) = boundary_spans.as_mut() {
                try_push_vec(
                    boundary_spans,
                    selected_boundary_spans
                        .as_ref()
                        .expect("boundary incidence was requested")[index],
                )?;
            }
        }
        let mut points = Vec::new();
        let mut boundary_points = with_boundary.then(Vec::new);
        try_reserve_vec(
            &mut points,
            cells.nodes.iter().filter(|&&value| value).count(),
        )?;
        for (index, &selected) in cells.nodes.iter().enumerate() {
            if selected && !covered[index] {
                try_push_vec(&mut points, ExactMetricNode {
                    homogeneous: partition.exact_owned(NodeId(index as u32))?,
                })?;
                if let Some(boundary_points) = boundary_points.as_mut() {
                    try_push_vec(boundary_points, !has_selected_area)?;
                }
            }
        }
        let mut faces = Vec::new();
        try_reserve_vec(&mut faces, global.count as usize)?;
        faces.resize_with(global.count as usize, Vec::new);
        for walk in 0..topology.face_starts.len().saturating_sub(1) {
            let face = global.walk_faces[walk] as usize;
            if face == 0 || !cells.faces[face] {
                continue;
            }
            let start = topology.face_starts[walk] as usize;
            let end = topology.face_starts[walk + 1] as usize;
            let mut cycle = Vec::new();
            try_reserve_vec(&mut cycle, end - start)?;
            for &slot in &topology.face_slots[start..end] {
                let node = NodeId(topology.owners[slot as usize]);
                try_push_vec(&mut cycle, ExactMetricNode {
                    homogeneous: partition.exact_owned(node)?,
                })?;
            }
            try_push_vec(&mut faces[face], cycle)?;
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

    fn node_bindings<'a>(&self, node: NodeId, partition: &'a ExactPartition<'_>) -> &'a [u32] {
        let index = node.0 as usize;
        let start = partition.binding_starts[index] as usize;
        let end = partition.binding_starts[index + 1] as usize;
        &partition.bindings[start..end]
    }

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
                let from_end = den.try_clone()?.try_subtract(num.try_clone()?)?;
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

    fn ordinate_protected_nodes(
        &self,
        partition: &ExactPartition<'_>,
        axes: crate::geometry::CoordinateAxes,
    ) -> Result<Option<Vec<bool>>> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(None);
        }
        let mut protected = Vec::new();
        try_reserve_vec(&mut protected, partition.node_count())?;
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
        by_node
            .try_reserve(nodes.len())
            .map_err(crate::error::Error::from)?;
        for &node in nodes {
            if by_node.contains_key(&node) {
                continue;
            }
            let point = self.resolve_node_point(partition, node, rounded(node)?, declared_axes)?;
            by_node.insert(node, point);
        }
        Ok(by_node)
    }

    fn winding_source_rings(&self) -> Result<Vec<Option<u32>>> {
        let mut rings = Vec::new();
        try_reserve_vec(&mut rings, self.source_rings.len())?;
        for (&ring, &kind) in self.source_rings.iter().zip(&self.source_ring_kinds) {
            try_push_vec(
                &mut rings,
                matches!(kind, Some(RingKind::Winding))
                    .then(|| ring.expect("a ring source always carries its ring identity")),
            )?;
        }
        Ok(rings)
    }

    pub(in crate::geometry) fn line_emission(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<ExactLineEmission> {
        partition.emit_lines(&self.source_operands)
    }

    pub(in crate::geometry) fn node_shape(
        &self,
        partition: &ExactPartition<'_>,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
    ) -> Result<Shape> {
        let mut items = Vec::new();
        try_reserve_vec(&mut items, partition.spans.len())?;
        items.extend(partition.spans.iter().map(|span| [span.start, span.end]));
        if items.is_empty() {
            return Shape::multi_line_string(declared_axes, Vec::new());
        }
        // Certifies the rounded chords; the points below read the same rounding.
        partition.round_incident_items(&items)?;
        let mut nodes = Vec::new();
        let node_capacity = items
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut nodes, node_capacity)?;
        nodes.extend(items.iter().flatten().copied());
        let by_node = self.resolve_node_points(
            partition,
            &nodes,
            |node| Ok(partition.node_xy(node)),
            declared_axes,
        )?;
        let axes = bucket_axes(&nodes, &by_node, declared_axes, strict, "node")?;
        let mut lines = Vec::new();
        try_reserve_vec(&mut lines, items.len())?;
        for item in items {
            let mut points = Vec::new();
            try_reserve_vec(&mut points, 2)?;
            try_push_vec(&mut points, with_axes(by_node[&item[0]], axes))?;
            try_push_vec(&mut points, with_axes(by_node[&item[1]], axes))?;
            try_push_vec(
                &mut lines,
                LineSeq::from_proven(CoordSeq::try_from_points(&points)?),
            )?;
        }
        Shape::multi_line_string(axes, lines)
    }

    pub(in crate::geometry) fn polygonize_diagnostics(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<PolygonizeFull> {
        let emission = self.line_emission(partition)?;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let mut used = Vec::new();
        try_reserve_vec(&mut used, partition.spans.len())?;
        used.resize(partition.spans.len(), false);
        for (slot, &span) in edges.iter().enumerate() {
            used[span as usize] |= global.walk_faces[topology.face_of[slot] as usize] != 0;
        }

        let mut degree = Vec::new();
        try_reserve_vec(&mut degree, partition.node_count())?;
        degree.resize(partition.node_count(), 0_usize);
        for span in &partition.spans {
            degree[span.start.0 as usize] = degree[span.start.0 as usize].saturating_add(1);
            degree[span.end.0 as usize] = degree[span.end.0 as usize].saturating_add(1);
        }

        let mut cuts = Vec::new();
        let mut dangles = Vec::new();
        let mut invalid_rings = Vec::new();
        for rows in [&mut cuts, &mut dangles, &mut invalid_rings] {
            try_reserve_vec(rows, self.polygon_rings.len())?;
            rows.resize_with(self.polygon_rings.len(), Vec::new);
        }

        for (index, (span, &segment)) in partition.spans.iter().zip(&emission.segments).enumerate()
        {
            let owners = emission.sources(index);
            let mut rows = Vec::new();
            try_reserve_vec(&mut rows, owners.len())?;
            for source in owners {
                if !rows.contains(&source.operand) {
                    try_push_vec(&mut rows, source.operand)?;
                }
            }
            for row in rows {
                let row = row as usize;
                if owners.len() > 1 {
                    try_push_vec(&mut invalid_rings[row], diagnostic_line(segment, true)?)?;
                } else if !used[index]
                    && (degree[span.start.0 as usize] <= 1 || degree[span.end.0 as usize] <= 1)
                {
                    try_push_vec(&mut dangles[row], diagnostic_line(segment, false)?)?;
                } else if !used[index] {
                    try_push_vec(&mut cuts[row], diagnostic_line(segment, false)?)?;
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
        let winding_rings = self.winding_source_rings()?;
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, global.count as usize)?;
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
        for walk in 0..topology.face_starts.len().saturating_sub(1) {
            if !selected[global.walk_faces[walk] as usize] {
                continue;
            }
            let range =
                topology.face_starts[walk] as usize..topology.face_starts[walk + 1] as usize;
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
        let mut nodes = Vec::new();
        try_reserve_vec(&mut nodes, node_capacity)?;
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
        rounded
            .try_reserve(parts.rounded.len())
            .map_err(crate::error::Error::from)?;
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
        let winding_rings = self.winding_source_rings()?;
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        if matches!(
            source_shape,
            Shape::LineString(_) | Shape::MultiLineString(_)
        ) {
            return self.subdivision_line_parts(
                partition,
                source_shape,
                &topology,
                &global,
                &facts,
                declared_axes,
                strict,
                clip_operands,
            );
        }
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;

        let mut plans = Vec::new();
        try_reserve_vec(&mut plans, clip_operands.len())?;
        for &clip_operand in &clip_operands {
            let cells = self.select_subdivision_cells(
                partition,
                &topology,
                &global,
                &facts,
                source_operand,
                clip_operand,
            )?;
            try_push_vec(
                &mut plans,
                cells.emission_plan_with_protected(
                    partition,
                    &topology,
                    &edges,
                    &global,
                    ordinate_protected.as_deref(),
                )?,
            )?;
        }

        let mut seen = Vec::new();
        try_reserve_vec(&mut seen, partition.node_count())?;
        seen.resize(partition.node_count(), false);
        let mut nodes = Vec::new();
        for plan in &plans {
            for [start, end] in plan.incident_items(&topology)? {
                for node in [start, end] {
                    if !seen[node.0 as usize] {
                        seen[node.0 as usize] = true;
                        try_push_vec(&mut nodes, node)?;
                    }
                }
            }
        }
        let rounded_nodes = partition.round_nodes(&nodes)?;
        ExactPartition::check_node_injectivity(&nodes, &rounded_nodes)?;
        let mut coordinates = Vec::new();
        try_reserve_vec(&mut coordinates, partition.node_count())?;
        coordinates.resize(partition.node_count(), None);
        for (&node, &coordinate) in nodes.iter().zip(&rounded_nodes) {
            coordinates[node.0 as usize] = Some(coordinate);
        }

        let mut node_parts = Vec::new();
        try_reserve_vec(&mut node_parts, plans.len())?;
        for plan in plans {
            try_push_vec(
                &mut node_parts,
                plan.emit_node_parts_with_coordinates(partition, &topology, &coordinates)?,
            )?;
        }

        let mut rounded =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        rounded
            .try_reserve(nodes.len())
            .map_err(crate::error::Error::from)?;
        let mut output_nodes = Vec::new();
        try_reserve_vec(&mut output_nodes, nodes.len())?;
        let mut output_seen = Vec::new();
        try_reserve_vec(&mut output_seen, partition.node_count())?;
        output_seen.resize(partition.node_count(), false);
        for parts in &node_parts {
            for &(node, point) in &parts.rounded {
                rounded.entry(node).or_insert(point);
                if !output_seen[node.0 as usize] {
                    output_seen[node.0 as usize] = true;
                    try_push_vec(&mut output_nodes, node)?;
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

    fn subdivision_line_parts(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        topology: &IndexedTopology,
        global: &GlobalFaces,
        facts: &CellFacts,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
    ) -> Result<[Shape; 2]> {
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, clip_operands.len())?;
        for &clip_operand in &clip_operands {
            let cells =
                self.select_subdivision_cells(partition, topology, global, facts, 0, clip_operand)?;
            try_push_vec(&mut selected, cells.spans)?;
        }

        let mut seen = Vec::new();
        try_reserve_vec(&mut seen, partition.node_count())?;
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
                        try_push_vec(&mut nodes, node)?;
                    }
                }
            }
        }
        ExactPartition::check_node_injectivity(&nodes, &partition.round_nodes(&nodes)?)?;
        let by_node = self.resolve_node_points(
            partition,
            &nodes,
            |node| Ok(partition.node_xy(node)),
            declared_axes,
        )?;
        let views = partition.source_span_views(&self.source_segments)?;
        let ranges = source_line_ranges(source_shape)?;
        let mut outputs = Vec::new();
        try_reserve_vec(&mut outputs, selected.len())?;
        for spans in selected {
            try_push_vec(
                &mut outputs,
                self.emit_source_line_shape(
                    partition,
                    source_shape,
                    &ranges,
                    &views,
                    &spans,
                    &by_node,
                    declared_axes,
                    strict,
                )?,
            )?;
        }
        let mut outputs = outputs.into_iter();
        Ok([
            outputs.next().expect("two subdivision line selections"),
            outputs.next().expect("two subdivision line selections"),
        ])
    }

    fn emit_source_line_shape(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        ranges: &[Range<usize>],
        views: &[Vec<(u32, bool)>],
        selected: &[bool],
        by_node: &HashMap<NodeId, Point>,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
    ) -> Result<Shape> {
        let mut node_runs = Vec::new();
        try_reserve_vec(&mut node_runs, selected.len())?;
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
                    if !selected.get(span_id as usize).copied().unwrap_or(false) {
                        if run.len() >= 2 {
                            try_push_vec(&mut node_runs, std::mem::take(&mut run))?;
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
                        try_push_vec(&mut node_runs, std::mem::take(&mut run))?;
                    }
                    if run.is_empty() {
                        try_reserve_vec(&mut run, 2)?;
                        try_push_vec(&mut run, start)?;
                    }
                    if run.last().copied() != Some(end) {
                        try_push_vec(&mut run, end)?;
                    }
                }
            }
            if run.len() >= 2 {
                try_push_vec(&mut node_runs, run)?;
            }
        }

        let mut line_nodes = Vec::new();
        for run in &node_runs {
            try_reserve_vec(&mut line_nodes, run.len())?;
            line_nodes.extend(run.iter().copied());
        }
        let line_axes = bucket_axes(&line_nodes, by_node, declared_axes, strict, "subdivide")?;
        let mut lines = Vec::new();
        try_reserve_vec(&mut lines, node_runs.len())?;
        for run in node_runs {
            let mut points = Vec::new();
            try_reserve_vec(&mut points, run.len())?;
            for node in run {
                try_push_vec(&mut points, with_axes(by_node[&node], line_axes))?;
            }
            try_push_vec(
                &mut lines,
                LineSeq::from_proven(CoordSeq::try_from_points(&points)?),
            )?;
        }
        if lines.is_empty() {
            return Ok(Shape::typed_empty(
                match source_shape {
                    Shape::LineString(_) => GeometryKind::LineString,
                    Shape::MultiLineString(_) => GeometryKind::MultiLineString,
                    _ => unreachable!("line subdivision source is lineal"),
                },
                declared_axes,
            ));
        }
        Shape::narrow_lines(line_axes, lines)
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
        let mut line_nodes = Vec::new();
        try_reserve_vec(&mut line_nodes, line_node_count)?;
        for line in &parts.lines {
            line_nodes.extend(line.iter().copied());
        }
        let line_axes = bucket_axes(&line_nodes, by_node, declared_axes, strict, operation)?;
        let mut polygon_nodes = Vec::new();
        try_reserve_vec(&mut polygon_nodes, polygon_node_count)?;
        for polygon in &parts.polygons {
            for ring in polygon {
                polygon_nodes.extend(ring.iter().copied());
            }
        }
        let polygon_axes = bucket_axes(&polygon_nodes, by_node, declared_axes, strict, operation)?;

        let mut points = Vec::new();
        try_reserve_vec(&mut points, parts.points.len())?;
        for node in parts.points {
            try_push_vec(&mut points, with_axes(by_node[&node], point_axes))?;
        }
        let mut lines = Vec::new();
        try_reserve_vec(&mut lines, parts.lines.len())?;
        for line in parts.lines {
            let mut coordinates = Vec::new();
            try_reserve_vec(&mut coordinates, line.len())?;
            for node in line {
                try_push_vec(&mut coordinates, with_axes(by_node[&node], line_axes))?;
            }
            try_push_vec(&mut lines, CoordSeq::try_from_points(&coordinates)?)?;
        }
        let mut polygons = Vec::new();
        try_reserve_vec(&mut polygons, parts.polygons.len())?;
        for polygon in parts.polygons {
            let mut rings = Vec::new();
            try_reserve_vec(&mut rings, polygon.len())?;
            for ring in polygon {
                let mut coordinates = Vec::new();
                try_reserve_vec(&mut coordinates, ring.len())?;
                for node in ring {
                    try_push_vec(&mut coordinates, with_axes(by_node[&node], polygon_axes))?;
                }
                try_push_vec(
                    &mut rings,
                    Ring::try_from_closed(CoordSeq::try_from_points(&coordinates)?)?,
                )?;
            }
            let mut ring_iter = rings.into_iter();
            let shell = ring_iter.next().expect("polygon has a shell");
            let mut holes = Vec::new();
            try_reserve_vec(&mut holes, ring_iter.size_hint().0)?;
            for ring in ring_iter {
                try_push_vec(&mut holes, ring)?;
            }
            try_push_vec(&mut polygons, Polygon::new(shell, holes)?)?;
        }
        let point_part = (!points.is_empty())
            .then(|| Shape::narrow_points(point_axes, points))
            .transpose()?;
        let line_part = (!lines.is_empty())
            .then(|| {
                let mut proven = Vec::new();
                try_reserve_vec(&mut proven, lines.len())?;
                for line in lines {
                    try_push_vec(&mut proven, LineSeq::from_proven(line))?;
                }
                Shape::narrow_lines(line_axes, proven)
            })
            .transpose()?;
        let polygon_part = (!polygons.is_empty())
            .then(|| Shape::narrow_polygons(polygon_axes, polygons))
            .transpose()?;
        let mut output = Vec::new();
        try_reserve_vec(&mut output, 3)?;
        output.extend([point_part, line_part, polygon_part].into_iter().flatten());
        match <[Shape; 1]>::try_from(output) {
            Ok([single]) => Ok(single),
            Err(parts) => Shape::geometry_collection(parts),
        }
    }

    fn select_subdivision_cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        global: &GlobalFaces,
        facts: &CellFacts,
        source_operand: usize,
        clip_operand: usize,
    ) -> Result<ArealCells> {
        let operands = self.operand_count();
        let mut cells = ArealCells {
            faces: Vec::new(),
            spans: Vec::new(),
            nodes: Vec::new(),
        };
        try_reserve_vec(&mut cells.faces, global.count as usize)?;
        cells.faces.resize(global.count as usize, false);
        for face in 0..global.count as usize {
            cells.faces[face] = facts.face_membership[face * operands + source_operand]
                && facts.face_membership[face * operands + clip_operand];
        }
        try_reserve_vec(&mut cells.spans, partition.spans.len())?;
        cells.spans.resize(partition.spans.len(), false);
        try_reserve_vec(&mut cells.nodes, partition.node_count())?;
        cells.nodes.resize(partition.node_count(), false);
        for span in 0..partition.spans.len() {
            cells.spans[span] = facts.span_support[span * operands + source_operand]
                && facts.span_support[span * operands + clip_operand];
            if cells.spans[span] {
                let span = partition.spans[span];
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
        }

        for node in 0..partition.node_count() {
            let range = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            let memberships = |operand: usize| {
                let face_membership = if range.is_empty() {
                    let component = topology.component_of[node] as usize;
                    let host = global.component_hosts[component] as usize;
                    facts.face_membership[host * operands + operand]
                } else {
                    range.clone().any(|slot| {
                        let face = global.walk_faces[topology.face_of[slot] as usize] as usize;
                        facts.face_membership[face * operands + operand]
                    })
                };
                face_membership || facts.point_membership[node * operands + operand]
            };
            cells.nodes[node] |= memberships(source_operand) && memberships(clip_operand);
        }
        Ok(cells)
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
        let winding_rings = self.linework_source_rings()?;
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
        let mut cycles: Vec<Vec<Vec<NodeId>>> = Vec::new();
        try_reserve_vec(&mut cycles, global.count as usize)?;
        cycles.resize_with(global.count as usize, Vec::new);
        for walk in 0..topology.face_starts.len().saturating_sub(1) {
            let start = topology.face_starts[walk] as usize;
            let end = topology.face_starts[walk + 1] as usize;
            let mut cycle = Vec::new();
            try_reserve_vec(&mut cycle, end.saturating_sub(start))?;
            for &slot in &topology.face_slots[start..end] {
                try_push_vec(&mut cycle, NodeId(topology.owners[slot as usize]))?;
            }
            let face = global.walk_faces[walk] as usize;
            try_push_vec(&mut cycles[face], cycle)?;
        }
        let mut samples = Vec::new();
        try_reserve_vec(&mut samples, global.count as usize)?;
        samples.resize(global.count as usize, None);
        for face in 1..global.count as usize {
            for &node in cycles[face].iter().flatten() {
                let earlier = match samples[face] {
                    None => true,
                    Some(prior) => partition.compare_nodes(node, prior)?.is_lt(),
                };
                if earlier {
                    samples[face] = Some(node);
                }
            }
        }
        // A face's nesting depth counts the other faces whose cycles contain
        // its sample. Rounding is monotone, so a cycle can contain the sample
        // only if the sample's rounded coordinates lie within the rounded
        // bounds of the cycle's nodes; samples sorted by x visit only those.
        let mut by_x = Vec::new();
        try_reserve_vec(&mut by_x, global.count as usize)?;
        for (face, sample) in samples.iter().enumerate() {
            if let Some(sample) = sample {
                by_x.push((partition.node_xy(*sample), face));
            }
        }
        by_x.sort_unstable_by(|left, right| left.0.x.total_cmp(&right.0.x));
        let mut depths = Vec::new();
        try_reserve_vec(&mut depths, global.count as usize)?;
        depths.resize(global.count as usize, 0_u32);
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
            let first = by_x.partition_point(|(xy, _)| xy.x < min_x);
            for &(xy, face) in &by_x[first..] {
                if xy.x > max_x {
                    break;
                }
                if face == other || xy.y < min_y || xy.y > max_y {
                    continue;
                }
                let sample = samples[face].expect("sorted faces have samples");
                for cycle in other_cycles {
                    if cycle_contains_node(partition, cycle, sample)? {
                        depths[face] += 1;
                        break;
                    }
                }
            }
        }
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, global.count as usize)?;
        selected.extend(
            (0..global.count as usize)
                .map(|face| face != 0 && samples[face].is_some() && depths[face] % 2 == 0),
        );
        let cells = ArealCells {
            faces: selected,
            spans: {
                let mut values = Vec::new();
                try_reserve_vec(&mut values, partition.spans.len())?;
                values.resize(partition.spans.len(), false);
                values
            },
            nodes: {
                let mut values = Vec::new();
                try_reserve_vec(&mut values, partition.node_count())?;
                values.resize(partition.node_count(), false);
                values
            },
        };
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        let (_, _, polygons) = plan.emit_xy_parts(partition, &topology)?;
        Ok(polygons)
    }

    pub(in crate::geometry) fn winding_polygons(
        &self,
        partition: &ExactPartition<'_>,
        keep: impl Fn(i64) -> bool,
    ) -> Result<Vec<Polygon>> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings()?;
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, global.count as usize)?;
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
        let winding_rings = self.winding_source_rings()?;
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, global.count as usize)?;
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
                let mut values = Vec::new();
                try_reserve_vec(&mut values, partition.spans.len())?;
                values.resize(partition.spans.len(), false);
                values
            },
            nodes: {
                let mut values = Vec::new();
                try_reserve_vec(&mut values, partition.node_count())?;
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

pub(crate) fn exact_intersects_bounds(shape: &Shape, bounds: Bounds) -> Result<bool> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("intersects", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    for [x, y] in bounds.corners() {
        if crate::geometry::cartesian_point_location(shape, Point::new_raw_xy(x, y))?
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
    let corners = bounds.corners();
    let rectangle = [
        Segment {
            start: XY::new(corners[0][0], corners[0][1]),
            end: XY::new(corners[1][0], corners[1][1]),
        },
        Segment {
            start: XY::new(corners[1][0], corners[1][1]),
            end: XY::new(corners[2][0], corners[2][1]),
        },
        Segment {
            start: XY::new(corners[2][0], corners[2][1]),
            end: XY::new(corners[3][0], corners[3][1]),
        },
        Segment {
            start: XY::new(corners[3][0], corners[3][1]),
            end: XY::new(corners[0][0], corners[0][1]),
        },
    ];
    for (index, &span) in partition.spans.iter().enumerate() {
        if !cells.spans[index] {
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
    fn new(operand_count: usize) -> Result<Self> {
        let mut polygon_rings = Vec::new();
        try_reserve_vec(&mut polygon_rings, operand_count)?;
        polygon_rings.resize_with(operand_count, Vec::new);
        Ok(Self {
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
        })
    }

    fn append_shape(&mut self, shape: &Shape, operand: u32) -> Result<()> {
        shape.try_for_each_component(&mut |component| match component {
            crate::geometry::Component::Point(point) => self.append_isolated_point(point, operand),
            crate::geometry::Component::LineString(line) => self.append_line(line, operand),
            crate::geometry::Component::Polygon(polygon) => self.append_polygon(polygon, operand),
        })
    }

    /// One point with no segment to carry it: the three isolated columns move
    /// together, so they reserve together.
    fn append_isolated_point(&mut self, point: Point, operand: u32) -> Result<()> {
        try_reserve_vec(&mut self.isolated, 1)?;
        try_reserve_vec(&mut self.isolated_points, 1)?;
        try_reserve_vec(&mut self.isolated_operands, 1)?;
        try_push_vec(&mut self.isolated, point.xy())?;
        try_push_vec(&mut self.isolated_points, point)?;
        try_push_vec(&mut self.isolated_operands, operand)?;
        Ok(())
    }

    fn append_line(&mut self, line: &LineSeq, operand: u32) -> Result<()> {
        self.append_line_coords(line.as_coords(), operand)
    }

    fn append_line_coords(&mut self, line: &CoordSeq, operand: u32) -> Result<()> {
        let mut first = None;
        let mut last = None;
        for [start, end] in line.segment_pairs() {
            let (start_xy, end_xy) = (start.xy(), end.xy());
            if same_point(start_xy, end_xy) {
                self.push_segment(
                    Segment {
                        start: start_xy,
                        end: end_xy,
                    },
                    &[start, end],
                    operand,
                    None,
                    None,
                )?;
                continue;
            }
            first.get_or_insert(start_xy);
            last = Some(end_xy);
            self.push_segment(
                Segment {
                    start: start_xy,
                    end: end_xy,
                },
                &[start, end],
                operand,
                None,
                None,
            )?;
        }
        if first.is_none() {
            for point in line.iter_coords() {
                try_reserve_vec(&mut self.isolated, 1)?;
                try_reserve_vec(&mut self.isolated_points, 1)?;
                try_reserve_vec(&mut self.isolated_operands, 1)?;
                try_push_vec(&mut self.isolated, point.xy())?;
                try_push_vec(&mut self.isolated_points, point)?;
                try_push_vec(&mut self.isolated_operands, operand)?;
            }
            return Ok(());
        }
        if let (Some(first), Some(last)) = (first, last)
            && !same_point(first, last)
        {
            try_reserve_vec(&mut self.line_boundaries, 2)?;
            try_push_vec(&mut self.line_boundaries, (operand, PointKey::new(first)))?;
            try_push_vec(&mut self.line_boundaries, (operand, PointKey::new(last)))?;
        }
        Ok(())
    }

    fn append_polygon(&mut self, polygon: &Polygon, operand: u32) -> Result<()> {
        let first_ring = self.ring_count;
        let polygon_id =
            u32::try_from(self.polygon_count).map_err(|_| ResourceError::SizeOverflow)?;
        self.polygon_count = self
            .polygon_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut self.polygon_operands, 1)?;
        try_push_vec(&mut self.polygon_operands, operand)?;
        for (ring_index, ring) in polygon.rings().enumerate() {
            let ring_id =
                u32::try_from(self.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
            self.ring_count = self
                .ring_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let (xs, ys) = (ring.xs(), ring.ys());
            let mut count = xs.len();
            if count > 1 && same_point(XY::new(xs[0], ys[0]), XY::new(xs[count - 1], ys[count - 1]))
            {
                count -= 1;
            }
            let kind = if crate::geometry::ring_is_collinear(ring.coords()) {
                RingKind::Collapsed
            } else {
                RingKind::Winding
            };
            try_reserve_vec(&mut self.ring_polygons, 1)?;
            try_reserve_vec(&mut self.ring_holes, 1)?;
            try_reserve_vec(&mut self.ring_kinds, 1)?;
            try_push_vec(&mut self.ring_polygons, polygon_id)?;
            try_push_vec(&mut self.ring_holes, ring_index > 0)?;
            try_push_vec(&mut self.ring_kinds, kind)?;
            for index in 0..count {
                let next = if index + 1 == count { 0 } else { index + 1 };
                let start = ring.point_at(index);
                let end = ring.point_at(next);
                self.push_segment(
                    Segment {
                        start: start.xy(),
                        end: end.xy(),
                    },
                    &[start, end],
                    operand,
                    Some(ring_id),
                    Some(kind),
                )?;
            }
        }
        try_reserve_vec(&mut self.polygon_rings[operand as usize], 1)?;
        try_push_vec(
            &mut self.polygon_rings[operand as usize],
            first_ring..self.ring_count,
        )?;
        try_reserve_vec(&mut self.polygon_ring_ranges, 1)?;
        try_push_vec(&mut self.polygon_ring_ranges, first_ring..self.ring_count)?;
        Ok(())
    }

    fn push_segment(
        &mut self,
        segment: Segment,
        source_points: &[Point; 2],
        operand: u32,
        ring: Option<u32>,
        ring_kind: Option<RingKind>,
    ) -> Result<()> {
        try_reserve_vec(&mut self.segments, 1)?;
        try_reserve_vec(&mut self.source_points, 1)?;
        try_reserve_vec(&mut self.source_operands, 1)?;
        try_reserve_vec(&mut self.source_rings, 1)?;
        try_reserve_vec(&mut self.source_ring_kinds, 1)?;
        try_push_vec(&mut self.segments, segment)?;
        try_push_vec(&mut self.source_points, *source_points)?;
        try_push_vec(&mut self.source_operands, operand)?;
        try_push_vec(&mut self.source_rings, ring)?;
        try_push_vec(&mut self.source_ring_kinds, ring_kind)?;
        Ok(())
    }
}

fn dedup_exact_segments(segments: &[Segment]) -> Result<(Vec<Segment>, SegmentProvenance)> {
    let mut index_of: HashMap<(PointKey, PointKey), u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    index_of
        .try_reserve(segments.len())
        .map_err(|_| ResourceError::Allocation)?;
    let mut unique: Vec<Segment> = Vec::new();
    try_reserve_vec(&mut unique, segments.len())?;
    let mut assignment = Vec::new();
    try_reserve_vec(&mut assignment, segments.len())?;
    for &segment in segments {
        let key = crate::geometry::undirected_segment_edge_key(segment);
        if let Some(&unique_index) = index_of.get(&key) {
            let representative = unique[unique_index as usize];
            try_push_vec(
                &mut assignment,
                (
                    unique_index,
                    !same_point(representative.start, segment.start),
                ),
            )?;
        } else {
            let unique_index =
                u32::try_from(unique.len()).map_err(|_| ResourceError::SizeOverflow)?;
            index_of.insert(key, unique_index);
            try_push_vec(&mut unique, segment)?;
            try_push_vec(&mut assignment, (unique_index, false))?;
        }
    }
    if unique.is_empty() {
        let mut offsets = Vec::new();
        try_reserve_vec(&mut offsets, 1)?;
        try_push_vec(&mut offsets, 0)?;
        return Ok((unique, SegmentProvenance::from_csr(offsets, Vec::new())));
    }
    let mut counts = Vec::new();
    try_reserve_vec(&mut counts, unique.len())?;
    counts.resize(unique.len(), 0_u32);
    for &(index, _) in &assignment {
        counts[index as usize] = counts[index as usize]
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
    }
    let mut offsets: Vec<u32> = Vec::new();
    try_reserve_vec(
        &mut offsets,
        unique
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    try_push_vec(&mut offsets, 0)?;
    for count in counts {
        let next = offsets
            .last()
            .copied()
            .unwrap_or(0_u32)
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
        try_push_vec(&mut offsets, next)?;
    }
    let mut entries = Vec::new();
    try_reserve_vec(&mut entries, assignment.len())?;
    entries.resize(assignment.len(), (0, false));
    let mut cursor = Vec::new();
    try_reserve_vec(&mut cursor, unique.len())?;
    cursor.extend_from_slice(&offsets[..unique.len()]);
    for (ordinal, &(index, reversed)) in assignment.iter().enumerate() {
        let slot = cursor[index as usize] as usize;
        entries[slot] = (
            u32::try_from(ordinal).map_err(|_| ResourceError::SizeOverflow)?,
            reversed,
        );
        cursor[index as usize] += 1;
    }
    Ok((unique, SegmentProvenance::from_csr(offsets, entries)))
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
    fn new(polygon_rings: &[Vec<Range<usize>>], ring_count: usize, odd: bool) -> Result<Self> {
        let mut ring_polygon = Vec::new();
        try_reserve_vec(&mut ring_polygon, ring_count)?;
        ring_polygon.resize(ring_count, None);
        let mut polygon_operand = Vec::new();
        for (operand, polygons) in polygon_rings.iter().enumerate() {
            for rings in polygons {
                let polygon = u32::try_from(polygon_operand.len())
                    .map_err(|_| ResourceError::SizeOverflow)?;
                for ring in rings.clone() {
                    ring_polygon[ring] = Some((polygon, ring == rings.start));
                }
                try_push_vec(&mut polygon_operand, operand as u32)?;
            }
        }
        let polygons = polygon_operand.len();
        let mut shell_filled = Vec::new();
        try_reserve_vec(&mut shell_filled, polygons)?;
        shell_filled.resize(polygons, false);
        let mut filled_holes = Vec::new();
        try_reserve_vec(&mut filled_holes, polygons)?;
        filled_holes.resize(polygons, 0);
        let mut occupied = Vec::new();
        try_reserve_vec(&mut occupied, polygon_rings.len())?;
        occupied.resize(polygon_rings.len(), 0);
        Ok(Self {
            odd,
            ring_polygon,
            polygon_operand,
            shell_filled,
            filled_holes,
            occupied,
        })
    }

    fn cross(&mut self, winding: &mut [i64], ring: u32, direction: i64) {
        let ring = ring as usize;
        let before = ring_occupied(winding[ring], self.odd);
        winding[ring] += direction;
        let after = ring_occupied(winding[ring], self.odd);
        let Some((polygon, shell)) = self.ring_polygon[ring].filter(|_| before != after) else {
            return;
        };
        let polygon = polygon as usize;
        let was = self.shell_filled[polygon] && self.filled_holes[polygon] == 0;
        if shell {
            self.shell_filled[polygon] = after;
        } else if after {
            self.filled_holes[polygon] += 1;
        } else {
            self.filled_holes[polygon] -= 1;
        }
        let now = self.shell_filled[polygon] && self.filled_holes[polygon] == 0;
        if was != now {
            let count = &mut self.occupied[self.polygon_operand[polygon] as usize];
            if now {
                *count += 1;
            } else {
                *count -= 1;
            }
        }
    }

    fn operand_occupied(&self, operand: usize) -> bool {
        self.occupied[operand] != 0
    }
}

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
pub(in crate::geometry) fn certify_emitted_incidence(items: &[([u32; 2], Segment)]) -> Result<()> {
    let mut coordinates: HashMap<u32, PointKey> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut by_coordinate: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let endpoint_capacity = items
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    coordinates
        .try_reserve(endpoint_capacity)
        .map_err(|_| ResourceError::Allocation)?;
    by_coordinate
        .try_reserve(endpoint_capacity)
        .map_err(|_| ResourceError::Allocation)?;
    let mut segments = Vec::new();
    try_reserve_vec(&mut segments, items.len())?;
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
        try_push_vec(&mut segments, segment)?;
    }
    let inventory = ExactSourceInventory::from_segments(&segments)?;
    let mut budget = ExpansionBudget::new("exact_partition", "Voronoi incidence");
    let partition = inventory.partition(&mut budget)?;
    let _ = partition.local_topology()?;
    let _ = for_each_candidate_pair(&segments, single_chain, |left, right| {
        match crate::geometry::segment_contact_exact(segments[left], segments[right]) {
            Contact::Disjoint => std::ops::ControlFlow::Continue(()),
            Contact::Touch { point } => {
                let mut shared = [0_u32; 2];
                let mut shared_count = 0;
                for id in items[left].0 {
                    if items[right].0.contains(&id) {
                        shared[shared_count] = id;
                        shared_count += 1;
                    }
                }
                if shared_count == 1 && PointKey::new(point) == coordinates[&shared[0]] {
                    std::ops::ControlFlow::Continue(())
                } else {
                    std::ops::ControlFlow::Break(())
                }
            },
            Contact::Cross | Contact::Collinear { .. } => std::ops::ControlFlow::Break(()),
        }
    })?;
    Ok(())
}

fn cycle_contains_node(
    partition: &ExactPartition<'_>,
    cycle: &[NodeId],
    query: NodeId,
) -> Result<bool> {
    let mut inside = false;
    for index in 0..cycle.len() {
        let a = cycle[index];
        let b = cycle[(index + 1) % cycle.len()];
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

const fn node_source_range(records: &[(NodeId, u32)], node: NodeId) -> Range<usize> {
    let mut low = 0;
    let mut high = records.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if records[middle].0.0 < node.0 {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    let start = low;
    while high < records.len() && records[high].0.0 == node.0 {
        high += 1;
    }
    start..high
}

fn source_line_ranges(source: &Shape) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::new();
    try_reserve_vec(&mut ranges, source.part_count())?;
    let mut offset = 0_usize;
    let mut push = |length: usize| -> Result<()> {
        let end = offset
            .checked_add(length.saturating_sub(1))
            .ok_or(ResourceError::SizeOverflow)?;
        try_push_vec(&mut ranges, offset..end)?;
        offset = end;
        Ok(())
    };
    match source {
        Shape::LineString(line) => push(line.len())?,
        Shape::MultiLineString(lines) => {
            for line in lines.iter() {
                push(line.len())?;
            }
        },
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision line ranges require lineal source".into(),
            )
            .into());
        },
    }
    Ok(ranges)
}

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

fn walk_exact_chain(
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    spans: &[ExactSpan],
    node_pin: &[bool],
    visited: &mut [bool],
    origin: NodeId,
    first: u32,
) -> Result<(Vec<NodeId>, Vec<u32>)> {
    let mut nodes = Vec::new();
    try_reserve_vec(&mut nodes, 2)?;
    try_push_vec(&mut nodes, origin)?;
    let mut chain_spans = Vec::new();
    try_reserve_vec(&mut chain_spans, 1)?;
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
        try_reserve_vec(&mut chain_spans, 1)?;
        try_push_vec(&mut chain_spans, span_id)?;
        try_reserve_vec(&mut nodes, 1)?;
        try_push_vec(&mut nodes, target)?;
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

fn try_clone_point(point: &[ExactDyadic; 3]) -> Result<[ExactDyadic; 3]> {
    Ok([
        point[0].try_clone()?,
        point[1].try_clone()?,
        point[2].try_clone()?,
    ])
}

impl ExactPartition<'_> {
    pub(in crate::geometry) const fn node_count(&self) -> usize {
        self.points.len()
    }

    /// A node's nearest-even binary64 point.
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
            Cow::Borrowed(point) => try_clone_point(point),
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
    pub(in crate::geometry) fn source_span_views(
        &self,
        source_segments: &[Segment],
    ) -> Result<Vec<Vec<(u32, bool)>>> {
        let mut views = Vec::new();
        try_reserve_vec(&mut views, source_segments.len())?;
        views.resize_with(source_segments.len(), Vec::new);
        for (span, owners) in self.provenance.iter().enumerate() {
            for &(source, reversed) in owners {
                let view = views.get_mut(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                try_reserve_vec(view, 1)?;
                try_push_vec(view, (span as u32, reversed))?;
            }
        }
        for (source, segment) in source_segments.iter().enumerate() {
            if views[source].len() < 2 {
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
            let mut ordered = std::mem::take(&mut views[source]);
            self.points
                .order_along(*segment, &mut ordered, entry, |_, _| {
                    Err(GeometryErrorKind::Invalid(
                        "exact partition source atoms share an entry node".into(),
                    )
                    .into())
                })?;
            let first = NodeId(entry(ordered[0]));
            let last = exit(ordered[ordered.len() - 1]);
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
            views[source] = ordered;
        }
        Ok(views)
    }

    /// Build maximal chain views directly from exact span incidence.
    ///
    /// `source_operands` maps the partition's source ordinals to coverage row
    /// identities. An atom is shared only when its exact provenance names two
    /// distinct rows. A chain ends at every non-degree-two node and wherever
    /// that shared/exterior class changes.
    pub(in crate::geometry) fn coverage_chain_views(
        &self,
        source_operands: &[u32],
    ) -> Result<ExactChainViews> {
        let (topology, edge_of_slot) = self.local_topology()?;
        let mut shared_by_span = Vec::new();
        try_reserve_vec(&mut shared_by_span, self.spans.len())?;
        for owners in self.provenance.iter() {
            let mut first = None;
            let mut second = None;
            let mut count = 0;
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
                count += 1;
            }
            try_push_vec(&mut shared_by_span, count == 2)?;
        }

        let mut node_pin = Vec::new();
        try_reserve_vec(&mut node_pin, self.node_count())?;
        node_pin.resize(self.node_count(), false);
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let slots = start as usize..end as usize;
            node_pin[node] = slots.len() != 2
                || shared_by_span[edge_of_slot[slots.start] as usize]
                    != shared_by_span[edge_of_slot[slots.start + 1] as usize];
        }

        let mut visited = Vec::new();
        try_reserve_vec(&mut visited, self.spans.len())?;
        visited.resize(self.spans.len(), false);
        let mut chain_of_span = Vec::new();
        try_reserve_vec(&mut chain_of_span, self.spans.len())?;
        chain_of_span.resize(self.spans.len(), u32::MAX);
        let mut chains = Vec::new();
        try_reserve_vec(&mut chains, self.spans.len())?;

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
                if self.compare_nodes(nodes[1], nodes[count - 1])? == Ordering::Greater {
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
            try_reserve_vec(&mut chains, 1)?;
            try_push_vec(&mut chains, ExactChainView {
                nodes,
                spans,
                shared,
            })?;
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

    fn round_nodes(&self, nodes: &[NodeId]) -> Result<Vec<XY>> {
        let mut coordinates = Vec::new();
        try_reserve_vec(&mut coordinates, nodes.len())?;
        for &node in nodes {
            try_push_vec(&mut coordinates, self.node_xy(node))?;
        }
        Ok(coordinates)
    }

    fn check_node_injectivity(nodes: &[NodeId], coordinates: &[XY]) -> Result<()> {
        debug_assert_eq!(nodes.len(), coordinates.len());
        let mut owners = HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        owners
            .try_reserve(nodes.len())
            .map_err(|_| ResourceError::Allocation)?;
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
            update_partition_metrics(|metrics| metrics.unmoved_emissions += 1);
        }
        moved
    }

    fn round_incident_items(&self, items: &[[NodeId; 2]]) -> Result<Vec<Segment>> {
        let mut segments = Vec::new();
        try_reserve_vec(&mut segments, items.len())?;
        segments.extend(items.iter().map(|&[start, end]| Segment {
            start: self.node_xy(start),
            end: self.node_xy(end),
        }));
        if !self.rounding_moves(items) {
            return Ok(segments);
        }
        let mut seen = Vec::new();
        try_reserve_vec(&mut seen, self.node_count())?;
        seen.resize(self.node_count(), false);
        let mut nodes = Vec::new();
        for &node in items.iter().flatten() {
            if !mem::replace(&mut seen[node.0 as usize], true) {
                try_push_vec(&mut nodes, node)?;
            }
        }
        Self::check_node_injectivity(&nodes, &self.round_nodes(&nodes)?)?;
        // A repeated node represents a live isolated point, not a new cut.
        let mut contact = None;
        let _ = for_each_candidate_pair(&segments, single_chain, |left, right| {
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
                contact = Some((left, right));
                ControlFlow::Break(())
            }
        })?;
        if let Some((first, second)) = contact {
            return Err(GeometryErrorKind::ExactUnexpectedContact { first, second }.into());
        }
        Ok(segments)
    }

    fn emit_lines(&self, source_operands: &[u32]) -> Result<ExactLineEmission> {
        let mut items = Vec::new();
        try_reserve_vec(&mut items, self.spans.len())?;
        items.extend(self.spans.iter().map(|span| [span.start, span.end]));

        let (topology, edge_of_slot) = self.local_topology()?;
        let segments = self.round_incident_items(&items)?;

        let mut departures = Vec::new();
        try_reserve_vec(&mut departures, self.spans.len())?;
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
        let mut darts = Vec::new();
        try_reserve_vec(&mut darts, departures.len())?;
        for [start, end] in departures {
            debug_assert!(start != u32::MAX && end != u32::MAX);
            try_push_vec(&mut darts, [start, end])?;
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
        try_reserve_vec(&mut offsets, offset_capacity)?;
        try_push_vec(&mut offsets, 0)?;
        let mut owners = Vec::new();
        for span_index in 0..self.spans.len() {
            for &(source, reversed) in self.owners(span_index) {
                let operand = source_operands[source as usize];
                try_reserve_vec(&mut owners, 1)?;
                try_push_vec(&mut owners, ExactLineSource { operand, reversed })?;
            }
            try_push_vec(
                &mut offsets,
                u32::try_from(owners.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
        }
        Ok(ExactLineEmission {
            segments,
            owners,
            offsets,
        })
    }

    fn check_emitted_fans(
        topology: &IndexedTopology,
        darts: &[[u32; 2]],
        segments: &[Segment],
    ) -> Result<()> {
        debug_assert_eq!(darts.len(), segments.len());
        // Emitted departures by topology slot.
        let mut departures: Vec<Option<(XY, XY)>> = Vec::new();
        try_reserve_vec(&mut departures, topology.targets.len())?;
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
            let mut descents = 0;
            let mut count = 0;
            for (target, departure) in
                (start..end).zip(&mut departures[start as usize..end as usize])
            {
                let Some((origin, point)) = departure.take() else {
                    continue;
                };
                let current = (target, point);
                first.get_or_insert((origin, current));
                if let Some((last_target, last_point)) = previous {
                    descents += usize::from(
                        compare_departures(origin, last_point, last_target, point, target)
                            == Ordering::Greater,
                    );
                }
                previous = Some(current);
                count += 1;
            }
            if count > 2 {
                let (origin, (first_target, first_point)) =
                    first.expect("a node with multiple emitted darts has a first dart");
                let (last_target, last_point) =
                    previous.expect("a node with multiple emitted darts has a last dart");
                descents += usize::from(
                    compare_departures(origin, last_point, last_target, first_point, first_target)
                        == Ordering::Greater,
                );
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
            Ordering::Equal => self.compare_axis(left, right, Axis::Y),
            order => Ok(order),
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact rounded coordinates are ordered by their stored binary64 values to keep emitted topology deterministic"
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
        try_reserve_vec(&mut topology.starts, vertex_capacity)?;
        topology.starts.resize(vertex_capacity, 0);
        for span in &self.spans {
            topology.starts[span.start.0 as usize + 1] = topology.starts[span.start.0 as usize + 1]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            topology.starts[span.end.0 as usize + 1] = topology.starts[span.end.0 as usize + 1]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        for node in 0..vertices {
            topology.starts[node + 1] = topology.starts[node + 1]
                .checked_add(topology.starts[node])
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut cursor = Vec::new();
        try_reserve_vec(&mut cursor, vertices)?;
        cursor.extend_from_slice(&topology.starts[..vertices]);
        let mut edge_of_slot = Vec::new();
        try_reserve_vec(&mut edge_of_slot, slots)?;
        edge_of_slot.resize(slots, 0);
        for (edge, span) in self.spans.iter().enumerate() {
            for node in [span.start, span.end] {
                edge_of_slot[cursor[node.0 as usize] as usize] = edge as u32;
                cursor[node.0 as usize] += 1;
            }
        }
        for node in 0..vertices {
            let range = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            // Degree-two nodes are ordered too: visibility wedges read the
            // angular origin.
            edge_of_slot[range].sort_unstable_by(|&left, &right| {
                self.compare_departures(NodeId(node as u32), left as usize, right as usize)
            });
        }
        try_reserve_vec(&mut topology.targets, slots)?;
        for node in 0..vertices {
            let range = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            for &edge in &edge_of_slot[range] {
                let span = self.spans[edge as usize];
                try_push_vec(
                    &mut topology.targets,
                    if span.start.0 == node as u32 {
                        span.end.0
                    } else {
                        span.start.0
                    },
                )?;
            }
        }
        try_reserve_vec(&mut topology.owners, slots)?;
        try_reserve_vec(&mut topology.twins, slots)?;
        try_reserve_vec(&mut topology.component_of, vertices)?;
        try_reserve_vec(&mut topology.face_of, slots)?;
        try_reserve_vec(
            &mut topology.face_starts,
            slots.checked_add(1).ok_or(ResourceError::SizeOverflow)?,
        )?;
        try_reserve_vec(&mut topology.face_slots, slots)?;
        let mut scratch = IndexedTopologyScratch::default();
        try_reserve_vec(&mut scratch.component_stack, vertices)?;
        try_reserve_vec(&mut scratch.successor, slots)?;
        try_reserve_vec(&mut scratch.edge_slots, self.spans.len())?;
        topology.finish(&edge_of_slot, &mut scratch);
        Ok((topology, edge_of_slot))
    }

    fn component_minima(&self, topology: &IndexedTopology) -> Result<Vec<NodeId>> {
        let mut minima: Vec<NodeId> = Vec::new();
        for (node, &component) in topology.component_of.iter().enumerate() {
            let node = NodeId(node as u32);
            if component as usize == minima.len() {
                try_reserve_vec(&mut minima, 1)?;
                try_push_vec(&mut minima, node)?;
            } else {
                let minimum = &mut minima[component as usize];
                if self.compare_nodes(node, *minimum)? == Ordering::Less {
                    *minimum = node;
                }
            }
        }
        Ok(minima)
    }

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

    fn global_faces(&self, topology: &IndexedTopology, edges: &[u32]) -> Result<GlobalFaces> {
        let minima = self.component_minima(topology)?;
        let walks = topology.face_starts.len().saturating_sub(1);
        let size = walks.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(size).map_err(|_| ResourceError::SizeOverflow)?;
        let mut links = Vec::new();
        try_reserve_vec(&mut links, size)?;
        links.extend(0..size as u32);
        let mut component_hosts = Vec::new();
        try_reserve_vec(&mut component_hosts, minima.len())?;
        // Each component's host is the face just east of the first span a
        // leftward ray from its minimum meets. One component has no other
        // component to meet: it lies in the unbounded face.
        let hits = if minima.len() == 1 {
            let mut none = Vec::new();
            try_push_vec(&mut none, None)?;
            none
        } else {
            let mut queries = Vec::new();
            try_reserve_vec(&mut queries, minima.len())?;
            queries.extend(minima.iter().map(|root| root.0));
            shoot_left(
                &self.points,
                self.spans.len(),
                |index| [self.spans[index].start.0, self.spans[index].end.0],
                &queries,
            )?
        };
        for (&root, hit) in minima.iter().zip(hits) {
            let host = hit.map_or(0, |span| self.contact_walk(topology, edges, span) + 1);
            try_push_vec(&mut component_hosts, host)?;
            if let Some(exterior) = self.exterior_walk(topology, edges, root)? {
                // Every link reaches a component with a strictly smaller
                // minimum x, so exterior links cannot form a cycle.
                links[exterior as usize + 1] = host;
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
        let mut labels = Vec::new();
        try_reserve_vec(&mut labels, size)?;
        labels.resize(size, 0);
        let mut count = 1;
        for node in 1..size {
            if links[node] == node as u32 {
                labels[node] = count;
                count += 1;
            }
        }
        for host in &mut component_hosts {
            *host = labels[links[*host as usize] as usize];
        }
        let mut walk_faces = Vec::new();
        try_reserve_vec(&mut walk_faces, walks)?;
        walk_faces.extend(links[1..].iter().map(|&root| labels[root as usize]));
        Ok(GlobalFaces {
            walk_faces,
            component_hosts,
            count,
        })
    }

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
                    -direction
                };
                cross_ring(winding, ring, direction, state)?;
            }
        }
        Ok(())
    }

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
                winding[ring as usize] += direction;
                Ok(())
            },
            |face, winding, ()| visit(face, winding),
        )
    }

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
        try_reserve_vec(&mut starts, start_count)?;
        starts.resize(start_count, 0_usize);
        for &walk in &topology.face_of {
            starts[faces.walk_faces[walk as usize] as usize + 1] += 1;
        }
        for face in 0..count {
            starts[face + 1] += starts[face];
        }
        let mut cursor = Vec::new();
        try_reserve_vec(&mut cursor, count)?;
        cursor.extend_from_slice(&starts[..count]);
        let mut slots = Vec::new();
        try_reserve_vec(&mut slots, edges.len())?;
        slots.resize(edges.len(), 0_u32);
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let face = faces.walk_faces[walk as usize] as usize;
            slots[cursor[face]] = slot as u32;
            cursor[face] += 1;
        }
        let mut winding = Vec::new();
        try_reserve_vec(&mut winding, ring_count)?;
        winding.resize(ring_count, 0_i64);
        let mut visited = Vec::new();
        try_reserve_vec(&mut visited, count)?;
        visited.resize(count, false);
        let mut stack = Vec::new();
        try_reserve_vec(&mut stack, count)?;
        visited[0] = true;
        visit(0, &winding, state)?;
        try_push_vec(&mut stack, (0_u32, starts[0], None))?;
        while let Some((face, next, incoming)) = stack.last_mut() {
            if *next == starts[*face as usize + 1] {
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
            *next += 1;
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
            try_push_vec(
                &mut stack,
                (neighbor, starts[neighbor as usize], Some(slot)),
            )?;
        }
        debug_assert!(visited.iter().all(|&seen| seen));
        debug_assert!(winding.iter().all(|&value| value == 0));
        Ok(())
    }

    /// The walk on the east side of `span`: the departure from its upper
    /// endpoint keeps that side on its left.
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
mod tests {
    use super::*;

    impl ExactPartition<'_> {
        fn stored_node(&self, point: XY) -> Option<NodeId> {
            let key = PointKey::new(point);
            (0..self.node_count())
                .find(|&node| self.stored_key(node) == Some(key))
                .map(|node| NodeId(node as u32))
        }

        /// The span a leftward ray from `query` meets first, by the sweep.
        fn left_contact(&self, query: NodeId) -> Option<u32> {
            shoot_left(
                &self.points,
                self.spans.len(),
                |index| [self.spans[index].start.0, self.spans[index].end.0],
                &[query.0],
            )
            .unwrap()[0]
        }

        fn crossings(&self) -> Vec<NodeId> {
            (0..self.node_count() as u32)
                .map(NodeId)
                .filter(|&node| !self.is_stored(node))
                .collect()
        }
    }

    use std::fmt::Write as _;

    #[test]
    fn line_contraction_preserves_bends_and_protected_points() {
        for protect in [false, true] {
            let mut sources = vec![
                seg(2.0, 0.0, 1.0, 0.0),
                seg(0.0, 0.0, 1.0, 0.0),
                seg(2.0, 0.0, 2.0, 1.0),
            ];
            if protect {
                sources.push(seg(1.0, 0.0, 1.0, 0.0));
            }
            let case = run(&sources);
            let partition = case.partition();
            let (topology, edges) = partition.local_topology().unwrap();
            let faces = partition.global_faces(&topology, &edges).unwrap();
            let plan = faces
                .boundary_plan(
                    &(0..partition.spans.len() as u32).collect::<Vec<_>>(),
                    &topology,
                    &edges,
                    &partition,
                    &[false],
                )
                .unwrap();
            let emitted = plan.round_items(&partition, &topology).unwrap();
            assert_eq!(emitted.len(), if protect { 3 } else { 2 });
            assert_eq!(plan.source_darts.len(), 3);
            assert!(emitted.iter().any(|segment| {
                segment.start == XY::new(2.0, 0.0) && segment.end == XY::new(2.0, 1.0)
            }));
            if !protect {
                assert!(emitted.iter().any(|segment| {
                    segment.start == XY::new(2.0, 0.0) && segment.end == XY::new(0.0, 0.0)
                }));
            }
        }
    }

    #[test]
    fn boundary_contraction_preserves_explicit_points_and_source_paths() {
        for (name, protect, attach_line) in [
            ("unprotected", false, false),
            ("explicit point", true, false),
            ("line junction", false, true),
        ] {
            let mut sources = vec![
                seg(1.0, 0.0, 2.0, 0.0),
                seg(2.0, 0.0, 2.0, 2.0),
                seg(2.0, 2.0, 0.0, 2.0),
                seg(0.0, 2.0, 0.0, 0.0),
                seg(0.0, 0.0, 1.0, 0.0),
            ];
            if protect {
                sources.push(seg(1.0, 0.0, 1.0, 0.0));
            }
            if attach_line {
                sources.push(seg(1.0, 0.0, 1.0, -1.0));
            }
            let case = run(&sources);
            let partition = case.partition();
            let (topology, edges) = partition.local_topology().unwrap();
            let faces = partition.global_faces(&topology, &edges).unwrap();
            assert_eq!(faces.count, 2);
            let lines = if attach_line {
                let endpoint = partition.stored_node(XY::new(1.0, -1.0)).unwrap();
                vec![
                    partition
                        .spans
                        .iter()
                        .position(|span| span.start == endpoint || span.end == endpoint)
                        .unwrap() as u32,
                ]
            } else {
                Vec::new()
            };
            let plan = faces
                .boundary_plan(&lines, &topology, &edges, &partition, &[false, true])
                .unwrap();
            assert_eq!(plan.walks.len(), 1);
            assert_eq!(plan.source_darts.len(), if attach_line { 6 } else { 5 });
            let emitted = plan.round_items(&partition, &topology).unwrap();
            assert_eq!(
                plan.walks[0].len(),
                if protect || attach_line { 5 } else { 4 },
                "{name}"
            );
            assert_eq!(
                emitted.len(),
                plan.walks[0].len() + usize::from(attach_line)
            );
            assert_eq!(
                emitted
                    .iter()
                    .any(|segment| segment.start == XY::new(1.0, 0.0)),
                protect || attach_line,
            );
            if !protect && !attach_line {
                assert!(emitted.iter().any(|segment| {
                    segment.start == XY::new(0.0, 0.0) && segment.end == XY::new(2.0, 0.0)
                }));
            }
        }
    }

    #[test]
    fn rounded_chord_can_hit_an_isolated_node_without_node_collision() {
        let point = XY::new(0.5, 1.0 / 6.0);
        let case = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(point.x, point.y, point.x, point.y),
        ]);
        let partition = case.partition();
        let start = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
        let crossing = partition.crossings()[0];
        let isolated = partition.point_nodes[0];
        assert!(partition.isolated.contains(&isolated));
        assert!(partition.spans.iter().any(|span| {
            (span.start == start && span.end == crossing)
                || (span.end == start && span.start == crossing)
        }));
        let nodes = [start, crossing, isolated];
        let coordinates = partition.round_nodes(&nodes).unwrap();
        ExactPartition::check_node_injectivity(&nodes, &coordinates).unwrap();
        let emitted = partition
            .round_incident_items(&[[start, crossing]])
            .unwrap();
        assert_eq!(emitted.len(), 1);
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let mut cells = ArealCells {
            faces: vec![false; faces.count as usize],
            spans: vec![false; partition.spans.len()],
            nodes: vec![false; partition.node_count()],
        };
        for (index, span) in partition.spans.iter().enumerate() {
            cells.spans[index] = (span.start == start && span.end == crossing)
                || (span.end == start && span.start == crossing);
        }
        cells.nodes[isolated.0 as usize] = true;
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &faces)
            .unwrap();
        let items = plan.incident_items(&topology).unwrap();
        assert_eq!(items.len(), 2);
        let error = plan.emit_xy_parts(&partition, &topology).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactUnexpectedContact { .. })
        ));
        assert!(!emitted_pair_preserves_incidence(
            [(start, coordinates[0]), (crossing, coordinates[1])],
            [(isolated, coordinates[2]); 2],
        ));
    }

    #[test]
    fn emitted_chords_allow_only_prescribed_endpoint_contacts() {
        let a = (NodeId(0), XY::new(0.0, 0.0));
        let b = (NodeId(1), XY::new(2.0, 0.0));
        let c = (NodeId(2), XY::new(2.0, 2.0));
        let d = (NodeId(3), XY::new(4.0, 0.0));
        let middle = (NodeId(4), XY::new(1.0, 0.0));
        for right in [[b, c], [b, d], [b, b], [c, c]] {
            assert!(emitted_pair_preserves_incidence([a, b], right));
            assert!(emitted_pair_preserves_incidence(right, [a, b]));
        }
        for right in [[middle, c], [middle, d], [middle, middle], [b, a]] {
            assert!(!emitted_pair_preserves_incidence([a, b], right));
            assert!(!emitted_pair_preserves_incidence(right, [a, b]));
        }
        let crossing = [
            (NodeId(5), XY::new(1.0, -1.0)),
            (NodeId(6), XY::new(1.0, 1.0)),
        ];
        assert!(!emitted_pair_preserves_incidence([a, b], crossing));
        assert!(!emitted_pair_preserves_incidence(crossing, [a, b]));
    }

    #[test]
    fn live_node_injectivity_distinguishes_repetition_from_rounding_collision() {
        let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
        let mut partition = case.partition();
        // A node at exact (x, 0), as a crossing whose exact point is known.
        let mut push = |x: ExactDyadic| {
            let node = NodeId(partition.node_count() as u32);
            let rounded = round_ratio(&x, &dyadic(1.0)).unwrap();
            let exact = std::cell::OnceCell::new();
            exact.set(Box::new([x, dyadic(0.0), dyadic(1.0)])).unwrap();
            partition
                .points
                .push(
                    NodeRecipe::Crossing {
                        segments: [0, 0],
                        exact,
                    },
                    XY::new(rounded, 0.0),
                )
                .unwrap();
            node
        };
        let first = push(ExactDyadic::from_f64(1.0));
        let second = push(
            ExactDyadic::from_f64(1.0)
                .try_add(ExactDyadic::from_f64(2.0_f64.powi(-54)))
                .unwrap(),
        );
        assert_eq!(
            partition.compare_nodes(first, second).unwrap(),
            Ordering::Less
        );
        let nodes = [first, second];
        let coordinates = partition.round_nodes(&nodes).unwrap();
        assert_eq!(coordinates, [XY::new(1.0, 0.0); 2]);
        assert!(ExactPartition::check_node_injectivity(&nodes, &coordinates).is_err());
        ExactPartition::check_node_injectivity(&[first, first], &coordinates).unwrap();
        let endpoint = partition.stored_node(XY::new(2.0, 0.0)).unwrap();
        let nodes = [first, endpoint];
        ExactPartition::check_node_injectivity(&nodes, &partition.round_nodes(&nodes).unwrap())
            .unwrap();
        ExactPartition::check_node_injectivity(&[], &[]).unwrap();
    }

    #[test]
    fn selected_nodes_round_in_requested_order() {
        let case = run(&[seg(0.0, 0.0, 3.0, 1.0), seg(1.0, -1.0, 1.0, 1.0)]);
        let partition = case.partition();
        let crossing = partition.crossings()[0];
        let endpoint = partition.stored_node(XY::new(3.0, 1.0)).unwrap();
        let rounded = partition
            .round_nodes(&[endpoint, crossing, endpoint])
            .unwrap();
        assert_eq!(rounded[0], XY::new(3.0, 1.0));
        assert_eq!(rounded[1].x, 1.0);
        assert_eq!(rounded[1].y.to_bits(), 0x3FD5_5555_5555_5555);
        assert_eq!(rounded[2], XY::new(3.0, 1.0));
        assert!(partition.round_nodes(&[]).unwrap().is_empty());
    }

    #[test]
    fn collapsed_ring_sources_retain_support_without_winding_labels() {
        let polygons = [
            "POLYGON ((0 0,4 0,2 0,0 0),(1 1,3 1,3 3,1 3,1 1))",
            "POLYGON ((8 0,12 0,12 4,8 4,8 0),(9 2,11 2,10 2,9 2))",
        ]
        .map(|wkt| match crate::io::parse_wkt(wkt).unwrap() {
            crate::geometry::Shape::Polygon(polygon) => polygon,
            _ => unreachable!(),
        });
        let sources = super::ArealSources::new(&polygons).unwrap();
        assert_eq!(sources.collapsed_rings, [(0, 0..3), (3, 11..14)]);
        assert_eq!(sources.polygon_rings, [0..2, 2..4]);
        assert_eq!(sources.source_rings, [
            None,
            None,
            None,
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(2),
            Some(2),
            None,
            None,
            None,
        ]);
        assert_eq!(sources.segments[0].start, XY::new(0.0, 0.0));
        assert_eq!(sources.segments[0].end, XY::new(4.0, 0.0));
        assert_eq!(sources.segments[11].start, XY::new(9.0, 2.0));
        assert_eq!(sources.segments[11].end, XY::new(11.0, 2.0));
        assert!(!sources.occupied(&[0, 1, 0, 0], false));
        assert!(sources.occupied(&[0, 0, 1, 0], false));
        let case = run(&sources.segments);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let mut labels = Vec::new();
        partition
            .visit_face_windings(
                &topology,
                &edges,
                &faces,
                &sources.source_rings,
                sources.ring_count,
                |_, winding| {
                    labels.push((
                        *winding.first_chunk::<4>().expect("four rings are labelled"),
                        sources.occupied(winding, false),
                    ));
                    Ok(())
                },
            )
            .unwrap();
        labels.sort_unstable();
        assert_eq!(labels, [
            ([0, 0, 0, 0], false),
            ([0, 0, 1, 0], true),
            ([0, 1, 0, 0], false),
        ]);
        let cells = sources
            .cells(&partition, &topology, &edges, &faces, false)
            .unwrap();
        assert_eq!(cells.faces.iter().filter(|&&filled| filled).count(), 1);
        for (index, span) in partition.spans.iter().enumerate() {
            assert!(
                partition.is_stored(span.start),
                "stored-only fixture constructed a node"
            );
            let start = partition.node_xy(span.start);
            let expected = start.x >= 8.0 || start.y == 0.0;
            assert_eq!(cells.spans[index], expected);
        }
        for (index, &filled) in cells.nodes.iter().enumerate() {
            assert!(
                partition.is_stored(NodeId(index as u32)),
                "stored-only fixture constructed a node"
            );
            let point = partition.node_xy(NodeId(index as u32));
            assert_eq!(filled, point.x >= 8.0 || point.y == 0.0);
        }
        let (lines, points) = cells
            .lower_dimensions(&partition, &topology, &edges, &faces)
            .unwrap();
        assert!(points.is_empty());
        let mut intervals = lines
            .iter()
            .map(|&index| {
                let span = partition.spans[index as usize];
                assert!(
                    partition.is_stored(span.start),
                    "stored-only fixture constructed a node"
                );
                let start = partition.node_xy(span.start);
                assert!(
                    partition.is_stored(span.end),
                    "stored-only fixture constructed a node"
                );
                let end = partition.node_xy(span.end);
                assert_eq!((start.y, end.y), (0.0, 0.0));
                (start.x.min(end.x), start.x.max(end.x))
            })
            .collect::<Vec<_>>();
        intervals.sort_by(|left, right| left.0.total_cmp(&right.0));
        assert_eq!(intervals, [(0.0, 2.0), (2.0, 4.0)]);
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &faces)
            .unwrap();
        let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
        assert!(points.is_empty());
        assert_eq!(lines.len(), 1);
        let mut endpoints = lines[0].xs().to_vec();
        endpoints.sort_by(f64::total_cmp);
        assert_eq!(endpoints, [0.0, 4.0]);
        assert_eq!(lines[0].ys(), [0.0, 0.0]);
        assert_eq!(polygons.len(), 1);
        assert!(polygons[0].holes.is_empty());
    }

    #[test]
    fn areal_cells_exclude_cancelled_spurs_and_keep_collapsed_points() {
        let polygons = [
            "POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))",
            "POLYGON ((6 6,6 6,6 6,6 6))",
        ]
        .map(|wkt| match crate::io::parse_wkt(wkt).unwrap() {
            crate::geometry::Shape::Polygon(polygon) => polygon,
            _ => unreachable!(),
        });
        let sources = ArealSources::new(&polygons).unwrap();
        let (unique, provenance) =
            dedup_undirected_segments_sourced(sources.segments.iter().copied());
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let cells = sources
            .cells(&partition, &topology, &edges, &faces, false)
            .unwrap();
        let mut selected = Vec::new();
        for (index, &filled) in cells.nodes.iter().enumerate() {
            if filled {
                assert!(
                    partition.is_stored(NodeId(index as u32)),
                    "stored-only fixture constructed a node"
                );
                let point = partition.node_xy(NodeId(index as u32));
                selected.push(point);
            }
        }
        selected.sort_by(|left, right| left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y)));
        assert_eq!(selected, [
            XY::new(0.0, 0.0),
            XY::new(0.0, 4.0),
            XY::new(4.0, 0.0),
            XY::new(4.0, 4.0),
            XY::new(6.0, 6.0),
        ]);
        assert_eq!(cells.spans.iter().filter(|&&filled| filled).count(), 4);
        let (lines, points) = cells
            .lower_dimensions(&partition, &topology, &edges, &faces)
            .unwrap();
        assert!(lines.is_empty());
        assert_eq!(points, [partition.stored_node(XY::new(6.0, 6.0)).unwrap()]);
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &faces)
            .unwrap();
        assert_eq!(plan.points, [(points[0], 0)]);
        assert!(plan.lines.is_empty());
        assert_eq!(plan.walks.len(), 1);
        let rounded = plan.round_items(&partition, &topology).unwrap();
        let mut actual: Vec<_> = rounded.iter().map(|span| (span.start, span.end)).collect();
        assert_eq!(actual.pop(), Some((XY::new(6.0, 6.0), XY::new(6.0, 6.0))));
        let first = actual
            .iter()
            .position(|&(start, _)| start == XY::new(0.0, 0.0))
            .unwrap();
        actual.rotate_left(first);
        assert_eq!(actual, [
            (XY::new(0.0, 0.0), XY::new(4.0, 0.0)),
            (XY::new(4.0, 0.0), XY::new(4.0, 4.0)),
            (XY::new(4.0, 4.0), XY::new(0.0, 4.0)),
            (XY::new(0.0, 4.0), XY::new(0.0, 0.0)),
        ]);
    }

    #[test]
    fn zero_length_sources_keep_owners_at_incident_and_isolated_nodes() {
        let (unique, provenance) = dedup_undirected_segments_sourced([
            seg(1.0, 0.0, 1.0, 0.0),
            seg(0.0, 0.0, 2.0, 0.0),
            seg(1.0, 0.0, 1.0, 0.0),
            seg(3.0, 2.0, 3.0, 2.0),
        ]);
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
        let incident = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
        let isolated = partition.stored_node(XY::new(3.0, 2.0)).unwrap();
        assert_eq!(partition.point_sources, [
            (incident, 0),
            (incident, 2),
            (isolated, 3)
        ]);
        assert_eq!(partition.isolated, [isolated]);
        assert_eq!(partition.spans.len(), 2);
        assert!(partition.point_nodes.is_empty());
    }

    #[test]
    fn stored_ring_sources_keep_direction_and_shell_hole_composition() {
        let polygon = |wkt| match crate::io::parse_wkt(wkt).unwrap() {
            crate::geometry::Shape::Polygon(polygon) => polygon,
            _ => unreachable!(),
        };
        let polygons = [
            polygon("POLYGON ((0 0,0 4,4 4,4 0,0 0),(1 1,1 3,3 3,3 1,1 1))"),
            polygon("POLYGON ((8 0,10 0,10 2,8 2,8 0))"),
        ];
        let sources = super::ArealSources::new(&polygons).unwrap();
        assert_eq!(sources.ring_count, 3);
        assert_eq!(sources.polygon_rings, [0..2, 2..3]);
        assert_eq!(sources.segments[0].start, super::XY::new(0.0, 0.0));
        assert_eq!(sources.segments[0].end, super::XY::new(0.0, 4.0));
        assert_eq!(sources.segments[4].start, super::XY::new(1.0, 1.0));
        assert_eq!(sources.segments[4].end, super::XY::new(1.0, 3.0));
        assert_eq!(sources.source_rings, [
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(2),
            Some(2)
        ]);
        let case = run(&sources.segments);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let mut labels = Vec::new();
        partition
            .visit_face_windings(
                &topology,
                &edges,
                &faces,
                &sources.source_rings,
                sources.ring_count,
                |_, winding| {
                    labels.push((
                        *winding
                            .first_chunk::<3>()
                            .expect("three rings are labelled"),
                        sources.occupied(winding, false),
                    ));
                    Ok(())
                },
            )
            .unwrap();
        labels.sort_unstable();
        assert_eq!(labels, [
            ([-1, -1, 0], false),
            ([-1, 0, 0], true),
            ([0, 0, 0], false),
            ([0, 0, 1], true)
        ]);
        for (windings, nonzero, parity) in [
            ([0, 0, 0], false, false),
            ([-2, 0, 0], true, false),
            ([-1, -2, 0], false, true),
            ([-1, 1, 0], false, false),
            ([0, 1, 0], false, false),
            ([-1, -1, 1], true, true),
        ] {
            assert_eq!(sources.occupied(&windings, false), nonzero, "{windings:?}");
            assert_eq!(sources.occupied(&windings, true), parity, "{windings:?}");
        }
    }

    #[test]
    fn occupied_support_owner_composes_stored_rings_and_parts() {
        let cases = [
            ("POLYGON ((0 0,1 0,1 1,0 1,0 0,1 0,1 1,0 1,0 0))", 1.0),
            ("POLYGON ((0 0,1 1,1 0,0 1,0 0))", 0.5),
            (
                "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((0.5 0,1.5 0,1.5 1,0.5 1,0.5 0)))",
                1.5,
            ),
            ("POLYGON ((0 0,1 0,1 1,0 1,0 0),(3 0,4 0,4 1,3 1,3 0))", 1.0),
            (
                "POLYGON ((0 0,1 0,1 1,0 1,0 0),(-0.5 -0.5,1.5 -0.5,1.5 1.5,-0.5 1.5,-0.5 -0.5))",
                0.0,
            ),
        ];
        for (wkt, expected) in cases {
            let shape = crate::io::parse_wkt(wkt).expect("valid test WKT");
            let support = occupied_cartesian_support(&shape).expect("exact occupied support");
            assert_eq!(support.area(), expected, "{wkt}");
        }
    }

    #[test]
    fn occupied_support_line_ordering_propagates_allocation_failures() {
        let mut parts = vec![crate::io::parse_wkt("POLYGON ((0 0,2 0,2 2,0 2,0 0))").unwrap()];
        for index in 0..64 {
            let x = 4.0 + f64::from(index) * 2.0;
            parts.push(Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(x, 0.0),
                        Point::new_raw_xy(x + 1.0, 0.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ));
        }
        let shape = Shape::geometry_collection(parts).unwrap();
        let (result, count) =
            crate::test_support::allocation_count(|| occupied_cartesian_support(&shape));
        let expected = result.unwrap();
        assert!(count > 0);
        for allowed in 0..=count {
            match crate::test_support::fail_allocations_after(allowed, || {
                occupied_cartesian_support(&shape)
            }) {
                Err(error) => assert!(matches!(
                    error.kind(),
                    crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
                )),
                Ok(actual) => assert!(actual.equals(&expected).unwrap()),
            }
        }
    }

    #[test]
    fn occupied_area_owner_rounds_exact_selected_faces() {
        let cases = [
            ("POLYGON ((0 0,1 0,1 1,0 1,0 0,1 0,1 1,0 1,0 0))", 1.0),
            ("POLYGON ((0 0,1 1,1 0,0 1,0 0))", 0.5),
            (
                "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((0.5 0,1.5 0,1.5 1,0.5 1,0.5 0)))",
                1.5,
            ),
            ("POLYGON ((0 0,1 0,1 1,0 1,0 0),(3 0,4 0,4 1,3 1,3 0))", 1.0),
            (
                "POLYGON ((0 0,1 0,1 1,0 1,0 0),(-0.5 -0.5,1.5 -0.5,1.5 1.5,-0.5 1.5,-0.5 -0.5))",
                0.0,
            ),
        ];
        for (wkt, expected) in cases {
            let shape = crate::io::parse_wkt(wkt).expect("valid test WKT");
            assert_eq!(
                occupied_cartesian_area(&shape).expect("exact occupied area"),
                expected,
                "{wkt}"
            );
        }
    }

    use super::super::noding::dedup_undirected_segments_sourced;
    use crate::geometry::generated_budget::PAIRWISE_WORK_LIMIT;

    /// One shared operand must select exactly the cells separate operands do.
    #[test]
    fn polygonal_union_as_one_operand_matches_separate_operands() {
        let shapes: Vec<Shape> = [
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
            "POLYGON ((2 2,6 2,6 6,2 6,2 2))",
            "POLYGON ((4 0,8 0,8 2,4 2,4 0))",
            "POLYGON ((1.5 1.5,2.5 1.5,2.5 2.5,1.5 2.5,1.5 1.5))",
            "MULTIPOLYGON (((10 10,11 10,11 11,10 11,10 10)),((11 11,12 11,12 12,11 12,11 11)))",
            "POLYGON ((0 0,1 -1,2 0,1 1,0 0))",
        ]
        .iter()
        .map(|wkt| crate::io::parse_wkt(wkt).unwrap())
        .collect();
        let union = |inventory: ExactSourceInventory, inputs: &[&Shape]| {
            let mut budget = ExpansionBudget::new("union", "exact partition");
            let partition = inventory.partition(&mut budget).unwrap();
            let axes =
                super::super::overlay_declared_axes(inputs, crate::geometry::OverlayOp::Union);
            inventory
                .overlay_shape(&partition, crate::geometry::OverlayOp::Union, axes, true)
                .unwrap()
        };
        for count in 2..=shapes.len() {
            let inputs: Vec<&Shape> = shapes[..count].iter().collect();
            let shared = ExactSourceInventory::from_polygonal_union(&inputs)
                .unwrap()
                .expect("polygonal inputs share one operand");
            let separate = ExactSourceInventory::from_shapes(&inputs).unwrap();
            assert_eq!(
                union(shared, &inputs),
                union(separate, &inputs),
                "{count} inputs"
            );
        }
        let line = crate::io::parse_wkt("LINESTRING (0 0,1 1)").unwrap();
        assert!(
            ExactSourceInventory::from_polygonal_union(&[&shapes[0], &line])
                .unwrap()
                .is_none()
        );
    }

    fn seg(x0: f64, y0: f64, x1: f64, y1: f64) -> Segment {
        Segment {
            start: XY::new(x0, y0),
            end: XY::new(x1, y1),
        }
    }

    struct Case {
        unique: Vec<Segment>,
        provenance: SegmentProvenance,
        isolated: Vec<XY>,
    }

    impl Case {
        fn from_segments(segments: &[Segment]) -> Self {
            let mut edges = Vec::new();
            let mut isolated = Vec::new();
            for &segment in segments {
                if same_point(segment.start, segment.end) {
                    isolated.push(segment.start);
                } else {
                    edges.push(segment);
                }
            }
            let (unique, provenance) = dedup_undirected_segments_sourced(edges);
            Self {
                unique,
                provenance,
                isolated,
            }
        }

        fn partition(&self) -> ExactPartition<'_> {
            let mut budget = ExpansionBudget::new("exact_partition", "items");
            partition_unique_segments(&self.unique, &self.provenance, &self.isolated, &mut budget)
                .unwrap()
        }
    }

    fn run(segments: &[Segment]) -> Case {
        Case::from_segments(segments)
    }

    fn metric_support_for(wkt: &str) -> ExactMetricSupport {
        let shape = crate::io::parse_wkt(wkt).expect("valid metric-support WKT");
        let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
        let mut budget = ExpansionBudget::new("hausdorff", "metric support");
        let partition = inventory.partition(&mut budget).unwrap();
        inventory.metric_support(&partition).unwrap()
    }

    fn metric_support_with_boundary_for(wkt: &str) -> ExactMetricSupport {
        let shape = crate::io::parse_wkt(wkt).expect("valid metric-support WKT");
        let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
        let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "source");
        let partition = inventory.partition(&mut budget).unwrap();
        inventory.metric_support_with_boundary(&partition).unwrap()
    }

    fn dyadic(value: f64) -> ExactDyadic {
        ExactDyadic::from_f64(value)
    }

    fn ratio_matches(
        actual_num: &ExactDyadic,
        actual_den: &ExactDyadic,
        num: f64,
        den: f64,
    ) -> bool {
        compare_ratios(actual_num, actual_den, &dyadic(num), &dyadic(den)).unwrap()
            == Ordering::Equal
    }

    fn node_at(node: &ExactMetricNode, x: f64, y: f64) -> bool {
        node_at_ratios(node, x, 1.0, y, 1.0)
    }

    fn node_at_ratios(
        node: &ExactMetricNode,
        x_num: f64,
        x_den: f64,
        y_num: f64,
        y_den: f64,
    ) -> bool {
        let [actual_x, actual_y, actual_w] = &node.homogeneous;
        ratio_matches(actual_x, actual_w, x_num, x_den)
            && ratio_matches(actual_y, actual_w, y_num, y_den)
    }

    fn span_between(span: &ExactMetricSpan, first: (f64, f64), second: (f64, f64)) -> bool {
        (node_at(&span.start, first.0, first.1) && node_at(&span.end, second.0, second.1))
            || (node_at(&span.start, second.0, second.1) && node_at(&span.end, first.0, first.1))
    }

    fn source_is(span: &ExactMetricSpan, source: Segment) -> bool {
        PointKey::new(span.source.start) == PointKey::new(source.start)
            && PointKey::new(span.source.end) == PointKey::new(source.end)
    }

    fn assert_ratio(actual: &[ExactDyadic; 2], num: f64, den: f64) {
        assert!(
            ratio_matches(&actual[0], &actual[1], num, den),
            "expected {num}/{den}, got {actual:?}"
        );
    }

    #[test]
    fn metric_support_unions_retraced_linework_and_drops_covered_points() {
        let support = metric_support_for(
            "GEOMETRYCOLLECTION(\
             LINESTRING (0 0,2 0,4 0),\
             LINESTRING (4 0,1 0,4 0),\
             LINESTRING (4 0,0 0),\
             POINT (2 0),\
             POINT (8 1))",
        );
        assert!(support.faces.is_empty());
        assert_eq!(support.spans.len(), 3);
        assert!(support.boundary_spans.is_none());
        assert!(
            support
                .spans
                .iter()
                .any(|span| span_between(span, (0.0, 0.0), (1.0, 0.0)))
        );
        assert!(
            support
                .spans
                .iter()
                .any(|span| span_between(span, (1.0, 0.0), (2.0, 0.0)))
        );
        assert!(
            support
                .spans
                .iter()
                .any(|span| span_between(span, (2.0, 0.0), (4.0, 0.0)))
        );
        assert_eq!(support.points.len(), 1);
        assert!(support.boundary_points.is_none());
        assert!(node_at(&support.points[0], 8.0, 1.0));
    }

    #[test]
    fn metric_support_keeps_crossing_nodes_and_original_source_parameters_exact() {
        let support = metric_support_for(
            "GEOMETRYCOLLECTION(\
             LINESTRING (0 0,3 1),\
             LINESTRING (1 -1,1 2))",
        );
        let source = seg(0.0, 0.0, 3.0, 1.0);
        let spans: Vec<_> = support
            .spans
            .iter()
            .filter(|span| source_is(span, source))
            .collect();
        assert_eq!(spans.len(), 2);
        let mut crossing_nodes = Vec::new();
        for span in spans {
            let endpoint = if node_at_ratios(&span.start, 1.0, 1.0, 1.0, 3.0) {
                assert_ratio(&span.start_parameter, 1.0, 3.0);
                &span.start
            } else {
                assert!(node_at_ratios(&span.end, 1.0, 1.0, 1.0, 3.0));
                assert_ratio(&span.end_parameter, 1.0, 3.0);
                &span.end
            };
            crossing_nodes.push(endpoint.homogeneous.clone());
        }
        assert_eq!(crossing_nodes.len(), 2);
        assert_eq!(crossing_nodes[0], crossing_nodes[1]);
        assert!(
            support.spans.iter().any(|span| {
                source_is(span, source)
                    && ((ratio_matches(
                        &span.start_parameter[0],
                        &span.start_parameter[1],
                        0.0,
                        1.0,
                    ) && ratio_matches(
                        &span.end_parameter[0],
                        &span.end_parameter[1],
                        1.0,
                        3.0,
                    )) || (ratio_matches(
                        &span.start_parameter[0],
                        &span.start_parameter[1],
                        1.0,
                        3.0,
                    ) && ratio_matches(
                        &span.end_parameter[0],
                        &span.end_parameter[1],
                        1.0,
                        1.0,
                    )))
            }),
            "the oblique source must retain both exact parameter windows"
        );
    }

    #[test]
    fn metric_support_groups_filled_hole_walks_and_ignores_external_holes() {
        let support = metric_support_for(
            "POLYGON ((0 0,6 0,6 6,0 6,0 0),\
                      (1 1,3 1,3 3,1 3,1 1),\
                      (10 10,12 10,12 12,10 12,10 10))",
        );
        assert_eq!(support.spans.len(), 8);
        assert_eq!(support.points.len(), 0);
        assert_eq!(support.faces.len(), 1);
        assert_eq!(support.faces[0].len(), 2);
        let mut cycle_lengths: Vec<_> = support.faces[0].iter().map(Vec::len).collect();
        cycle_lengths.sort_unstable();
        assert_eq!(cycle_lengths, [4, 4]);
        assert!(support.spans.iter().all(|span| {
            [&span.start, &span.end]
                .into_iter()
                .all(|node| !node_at(node, 10.0, 10.0) && !node_at(node, 12.0, 12.0))
        }));
        assert!(
            support.faces[0]
                .iter()
                .flatten()
                .all(|node| { !node_at(node, 10.0, 10.0) && !node_at(node, 12.0, 12.0) })
        );
    }

    #[test]
    fn metric_support_handles_bowties_collapsed_rings_and_coincident_cancellation() {
        let bowtie = metric_support_for("POLYGON ((0 0,4 4,0 4,4 0,0 0))");
        assert_eq!(bowtie.spans.len(), 6);
        assert_eq!(bowtie.faces.len(), 2);
        assert!(bowtie.faces.iter().all(|face| face.len() == 1));
        assert!(bowtie.faces.iter().all(|face| face[0].len() == 3));

        let collapsed = metric_support_for("POLYGON ((0 0,4 0,2 0,0 0))");
        assert!(collapsed.points.is_empty());
        assert!(collapsed.faces.is_empty());
        assert_eq!(collapsed.spans.len(), 2);
        assert!(
            collapsed
                .spans
                .iter()
                .any(|span| span_between(span, (0.0, 0.0), (2.0, 0.0)))
        );
        assert!(
            collapsed
                .spans
                .iter()
                .any(|span| span_between(span, (2.0, 0.0), (4.0, 0.0)))
        );

        for wkt in [
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
            "POLYGON ((0 0,4 0,2 0,0 0),(0 0,4 0,2 0,0 0))",
        ] {
            let cancelled = metric_support_for(wkt);
            assert!(cancelled.spans.is_empty(), "{wkt}");
            assert!(cancelled.points.is_empty(), "{wkt}");
            assert!(cancelled.faces.is_empty(), "{wkt}");
        }
    }

    #[test]
    fn metric_support_preserves_multipart_union_and_mixed_dimension_points() {
        let multipart = metric_support_for(
            "MULTIPOLYGON (\
             ((0 0,2 0,2 2,0 2,0 0)),\
             ((2 0,4 0,4 2,2 2,2 0)))",
        );
        assert_eq!(multipart.faces.len(), 2);
        assert_eq!(multipart.spans.len(), 7);
        for endpoint in [
            ((0.0, 0.0), (2.0, 0.0)),
            ((2.0, 0.0), (4.0, 0.0)),
            ((4.0, 0.0), (4.0, 2.0)),
            ((4.0, 2.0), (2.0, 2.0)),
            ((2.0, 2.0), (0.0, 2.0)),
            ((0.0, 2.0), (0.0, 0.0)),
        ] {
            assert!(
                multipart
                    .spans
                    .iter()
                    .any(|span| span_between(span, endpoint.0, endpoint.1))
            );
        }

        let mixed = metric_support_for(
            "GEOMETRYCOLLECTION(\
             POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
             POINT (10 1))",
        );
        assert_eq!(mixed.spans.len(), 4);
        assert_eq!(mixed.faces.len(), 1);
        assert_eq!(mixed.faces[0].len(), 1);
        assert_eq!(mixed.points.len(), 1);
        assert!(node_at(&mixed.points[0], 10.0, 1.0));
    }

    #[test]
    fn metric_support_boundary_incidence_excludes_internal_and_residual_items() {
        let adjacent = metric_support_with_boundary_for(
            "MULTIPOLYGON (\
             ((0 0,2 0,2 2,0 2,0 0)),\
             ((2 0,4 0,4 2,2 2,2 0)))",
        );
        let adjacent_boundary = adjacent
            .boundary_spans
            .as_ref()
            .expect("boundary selection");
        assert_eq!(adjacent_boundary.len(), adjacent.spans.len());
        assert_eq!(
            adjacent_boundary
                .iter()
                .filter(|&&selected| selected)
                .count(),
            6
        );
        assert!(
            adjacent
                .spans
                .iter()
                .zip(adjacent_boundary)
                .any(|(span, &selected)| {
                    !selected && span_between(span, (2.0, 0.0), (2.0, 2.0))
                })
        );

        let overlapping = metric_support_with_boundary_for(
            "MULTIPOLYGON (\
             ((0 0,4 0,4 4,0 4,0 0)),\
             ((2 0,6 0,6 4,2 4,2 0)))",
        );
        let overlapping_boundary = overlapping
            .boundary_spans
            .as_ref()
            .expect("boundary selection");
        assert!(
            overlapping
                .spans
                .iter()
                .zip(overlapping_boundary)
                .any(|(span, &selected)| {
                    !selected && span_between(span, (2.0, 0.0), (2.0, 4.0))
                })
        );
        assert!(
            overlapping
                .spans
                .iter()
                .zip(overlapping_boundary)
                .any(|(span, &selected)| {
                    !selected && span_between(span, (4.0, 0.0), (4.0, 4.0))
                })
        );

        let holed = metric_support_with_boundary_for(
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),\
                      (4 4,6 4,6 6,4 6,4 4))",
        );
        let holed_boundary = holed.boundary_spans.as_ref().expect("boundary selection");
        assert!(
            holed
                .spans
                .iter()
                .zip(holed_boundary)
                .all(|(_, &selected)| selected)
        );

        let residual = metric_support_with_boundary_for(
            "GEOMETRYCOLLECTION(\
             POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
             LINESTRING (1 1,3 1,1 1),\
             POINT (2 2))",
        );
        assert!(!residual.faces.is_empty());
        assert_eq!(residual.points.len(), 1);
        assert!(
            residual
                .boundary_points
                .as_ref()
                .expect("boundary selection")
                .iter()
                .all(|&selected| !selected)
        );
        let residual_boundary = residual
            .boundary_spans
            .as_ref()
            .expect("boundary selection");
        assert_eq!(
            residual_boundary
                .iter()
                .filter(|&&selected| selected)
                .count(),
            4
        );
        assert!(
            residual
                .spans
                .iter()
                .zip(residual_boundary)
                .any(|(span, &selected)| {
                    !selected && span_between(span, (1.0, 1.0), (3.0, 1.0))
                })
        );
        let external = metric_support_with_boundary_for(
            "GEOMETRYCOLLECTION(\
             POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
             LINESTRING (10 0,12 0))",
        );
        let external_boundary = external
            .boundary_spans
            .as_ref()
            .expect("boundary selection");
        assert!(
            external
                .spans
                .iter()
                .zip(external_boundary)
                .any(|(span, &selected)| {
                    !selected && span_between(span, (10.0, 0.0), (12.0, 0.0))
                })
        );
        let collapsed_inside = metric_support_with_boundary_for(
            "GEOMETRYCOLLECTION(\
             POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
             POLYGON ((1 1,3 1,1 1,1 1)))",
        );
        let collapsed_inside_boundary = collapsed_inside
            .boundary_spans
            .as_ref()
            .expect("boundary selection");
        assert_eq!(
            collapsed_inside_boundary
                .iter()
                .filter(|&&selected| selected)
                .count(),
            4
        );
        assert!(collapsed_inside_boundary.iter().any(|&selected| !selected));

        let collapsed = metric_support_with_boundary_for("POLYGON ((0 0,4 0,2 0,0 0))");
        assert!(collapsed.faces.is_empty());
        assert!(
            collapsed
                .boundary_spans
                .as_ref()
                .expect("boundary selection")
                .iter()
                .all(|&selected| selected)
        );
    }

    #[test]
    fn metric_support_boundary_incidence_scales_without_pairwise_matching() {
        let count = 128;
        let mut wkt = String::from("MULTIPOLYGON (");
        for index in 0..count {
            if index != 0 {
                wkt.push(',');
            }
            write!(
                wkt,
                "(({} 0,{} 0,{} 1,{} 1,{} 0))",
                index,
                index + 1,
                index + 1,
                index,
                index
            )
            .unwrap();
        }
        wkt.push(')');
        let support = metric_support_with_boundary_for(&wkt);
        let boundary_spans = support.boundary_spans.as_ref().expect("boundary selection");
        assert_eq!(boundary_spans.len(), support.spans.len());
        assert_eq!(
            boundary_spans.iter().filter(|&&selected| selected).count(),
            2 * count + 2
        );
        assert_eq!(
            boundary_spans.iter().filter(|&&selected| !selected).count(),
            count - 1
        );
    }

    #[test]
    fn ordinary_metric_support_does_not_build_a_boundary_plan() {
        let wkt = "POLYGON ((0 0,4 0,4 4,0 4,0 0))";
        reset_boundary_plan_calls();
        let ordinary = metric_support_for(wkt);
        assert!(ordinary.boundary_spans.is_none());
        assert_eq!(boundary_plan_calls(), 0);

        reset_boundary_plan_calls();
        let boundary = metric_support_with_boundary_for(wkt);
        assert!(boundary.boundary_spans.is_some());
        assert_eq!(boundary_plan_calls(), 1);
    }

    #[test]
    fn linework_selection_keeps_minimal_faces_and_disconnected_support() {
        let crossing = [
            seg(0.0, 0.0, 2.0, 2.0),
            seg(0.0, 2.0, 2.0, 0.0),
            seg(0.0, 0.0, 2.0, 0.0),
            seg(2.0, 0.0, 2.0, 2.0),
            seg(2.0, 2.0, 0.0, 2.0),
            seg(0.0, 2.0, 0.0, 0.0),
        ];
        let inventory = ExactSourceInventory::from_segments(&crossing).unwrap();
        let mut budget = ExpansionBudget::new("polygonize", "partition");
        let mut partition = inventory.partition(&mut budget).unwrap();
        let faces = inventory.linework_polygons(&mut partition, false).unwrap();
        assert_eq!(faces.len(), 4);
        assert_eq!(faces.iter().map(Polygon::area).sum::<f64>(), 4.0);

        let disconnected = [
            seg(0.0, 0.0, 2.0, 2.0),
            seg(2.0, 2.0, 0.0, 2.0),
            seg(0.0, 2.0, 2.0, 0.0),
            seg(2.0, 0.0, 0.0, 0.0),
            seg(5.0, 0.0, 6.0, 0.0),
            seg(6.0, 0.0, 6.0, 1.0),
            seg(6.0, 1.0, 5.0, 1.0),
            seg(5.0, 1.0, 5.0, 0.0),
        ];
        let inventory = ExactSourceInventory::from_segments(&disconnected).unwrap();
        let mut budget = ExpansionBudget::new("build_area", "partition");
        let mut partition = inventory.partition(&mut budget).unwrap();
        let parts = inventory.linework_polygons(&mut partition, true).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts.iter().map(Polygon::area).sum::<f64>(), 3.0);
    }

    #[test]
    fn generated_budget_charges_only_net_canonical_growth() {
        use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

        let crossing = [seg(0.0, 0.0, 2.0, 2.0), seg(0.0, 2.0, 2.0, 0.0)];
        let cases = [
            (vec![seg(0.0, 0.0, 1.0, 0.0)], 0),
            (vec![seg(0.0, 0.0, 1.0, 0.0); 2], 0),
            (vec![seg(0.0, 0.0, 1.0, 0.0), seg(1.0, 0.0, 2.0, 0.0)], 0),
            (vec![seg(1.0, 1.0, 1.0, 1.0)], 0),
            (crossing.to_vec(), 3),
            (vec![seg(0.0, 0.0, 4.0, 0.0), seg(1.0, 0.0, 3.0, 0.0)], 1),
            (vec![seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 0.0, 1.0, 0.0)], 1),
            (vec![crossing[0], crossing[1], seg(1.0, 1.0, 1.0, 1.0)], 2),
            (
                vec![
                    crossing[0],
                    crossing[1],
                    seg(10.0, 0.0, 14.0, 0.0),
                    seg(10.0, 0.0, 12.0, 0.0),
                    seg(12.0, 0.0, 14.0, 0.0),
                ],
                2,
            ),
        ];
        for (segments, growth) in cases {
            let case = run(&segments);
            let mut budget = ExpansionBudget::new("exact_partition", "items");
            budget.add(GENERATED_ITEM_LIMIT - growth).unwrap();
            let partition = partition_unique_segments(
                &case.unique,
                &case.provenance,
                &case.isolated,
                &mut budget,
            )
            .unwrap();
            assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
            assert!(partition.node_count() > 0);
            if growth > 0 {
                let mut short = ExpansionBudget::new("exact_partition", "items");
                short.add(GENERATED_ITEM_LIMIT - growth + 1).unwrap();
                let Err(error) = partition_unique_segments(
                    &case.unique,
                    &case.provenance,
                    &case.isolated,
                    &mut short,
                ) else {
                    panic!("one fewer generated item must reject the partition");
                };
                assert!(matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        GeometryErrorKind::GeneratedOutputTooLarge { .. }
                    )
                ));
            }
        }
    }

    fn crossing_node(partition: &ExactPartition<'_>) -> NodeId {
        let mut crossings =
            (0..partition.node_count() as u32).filter(|&node| !partition.is_stored(NodeId(node)));
        let node = crossings.next().expect("expected a crossing node");
        assert_eq!(crossings.next(), None, "expected one crossing node");
        NodeId(node)
    }

    /// The node's exact point is `(x[0] / x[1], y[0] / y[1])`.
    fn assert_exact_at(
        partition: &ExactPartition<'_>,
        node: NodeId,
        x: [&ExactDyadic; 2],
        y: [&ExactDyadic; 2],
    ) {
        let point = partition.exact(node).unwrap();
        let [px, py, pw] = &*point;
        assert_eq!(compare_ratios(px, pw, x[0], x[1]).unwrap(), Ordering::Equal);
        assert_eq!(compare_ratios(py, pw, y[0], y[1]).unwrap(), Ordering::Equal);
    }

    fn constructed_count(partition: &ExactPartition) -> usize {
        partition.crossings().len()
    }

    fn component_count(partition: &ExactPartition) -> usize {
        let n = partition.node_count();
        if n == 0 {
            return 0;
        }
        let mut parent: Vec<u32> = (0..n as u32).collect();
        let find = |parent: &mut [u32], mut node: u32| -> u32 {
            while parent[node as usize] != node {
                parent[node as usize] = parent[parent[node as usize] as usize];
                node = parent[node as usize];
            }
            node
        };
        for span in &partition.spans {
            let a = find(&mut parent, span.start.0);
            let b = find(&mut parent, span.end.0);
            if a != b {
                parent[b as usize] = a;
            }
        }
        let mut roots = Vec::new();
        for node in 0..n as u32 {
            let root = find(&mut parent, node);
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
        roots.len()
    }

    #[test]
    fn public_entry_matches_inner_on_empty() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(Vec::<Segment>::new());
        let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
        assert_eq!(partition.node_count(), 0);
    }

    #[test]
    fn generated_t_has_seven_nodes_five_spans_two_components() {
        let rn_sixth = 1.0_f64 / 6.0;
        let case = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.5, 0.0, 0.5, rn_sixth),
        ]);
        let partition = case.partition();
        assert_eq!(partition.unique.len(), 3);
        assert_eq!(partition.node_count(), 7);
        assert_eq!(partition.spans.len(), 5);
        assert_eq!(constructed_count(&partition), 1);
        assert_eq!(component_count(&partition), 2);
        let c_end = partition.stored_node(XY::new(0.5, rn_sixth)).unwrap();
        let on_a = partition
            .spans
            .iter()
            .any(|span| span.unique == 0 && (span.start == c_end || span.end == c_end));
        assert!(!on_a, "C's endpoint must not node onto A");
        let (one, three) = (dyadic(1.0), dyadic(3.0));
        assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
            &one, &three,
        ]);
    }

    #[test]
    fn endpoint_near_miss_five_nodes_four_spans() {
        let nd = 1.0_f64.next_down();
        let case = run(&[seg(0.0, 0.0, 1.0, 1.0), seg(1.0, nd, 0.0, 2.0)]);
        let partition = case.partition();
        assert_eq!(partition.node_count(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 1);
        let q_num = ExactDyadic::from_f64(1.0);
        let two = ExactDyadic::from_f64(2.0);
        let p54 = {
            let mut p = ExactDyadic::from_f64(1.0);
            for _ in 0..54 {
                p = p.try_product(&two).unwrap();
            }
            p
        };
        let den = p54
            .try_clone()
            .unwrap()
            .try_add(q_num.try_clone().unwrap())
            .unwrap();
        // The crossing lies on y = x at 2^54 / (2^54 + 1).
        assert_exact_at(&partition, crossing_node(&partition), [&p54, &den], [
            &p54, &den,
        ]);
    }

    #[test]
    fn concurrent_three_sources_one_constructed_node() {
        let case = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.0, 1.0, 3.0, -1.0),
        ]);
        let partition = case.partition();
        assert_eq!(constructed_count(&partition), 1);
        let (one, three) = (dyadic(1.0), dyadic(3.0));
        assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
            &one, &three,
        ]);
    }

    #[test]
    fn overlap_reversed_duplicate_keeps_reverse_owner() {
        let case = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(1.0, 0.0, 3.0, 0.0),
            seg(2.0, -1.0, 2.0, 1.0),
            seg(4.0, 0.0, 0.0, 0.0),
        ]);
        let partition = case.partition();
        let reversed = partition.spans.iter().enumerate().any(|(index, _)| {
            partition
                .owners(index)
                .iter()
                .any(|&(ordinal, reversed)| ordinal == 3 && reversed)
        });
        assert!(reversed, "reversed duplicate must keep the reverse flag");
        let overlap = partition.spans.iter().enumerate().filter(|(index, _)| {
            let owners = partition.owners(*index);
            owners.iter().any(|(ordinal, _)| *ordinal == 0)
                && owners.iter().any(|(ordinal, _)| *ordinal == 1)
        });
        assert!(
            overlap.count() > 0,
            "overlap of A and B must be one aggregated atom"
        );
    }

    #[test]
    fn overlap_aggregate_owners_are_one_atom() {
        let case = run(&[seg(0.0, 0.0, 4.0, 0.0), seg(1.0, 0.0, 3.0, 0.0)]);
        let partition = case.partition();
        let shared: Vec<_> = partition
            .spans
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                let owners = partition.owners(*index);
                owners.iter().any(|(ordinal, _)| *ordinal == 0)
                    && owners.iter().any(|(ordinal, _)| *ordinal == 1)
            })
            .collect();
        assert_eq!(shared.len(), 1);
        assert_eq!(partition.owners(shared[0].0).len(), 2);
    }

    #[test]
    fn degenerate_point_is_isolated_node() {
        let case = run(&[seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 1.0, 1.0, 1.0)]);
        let partition = case.partition();
        assert_eq!(partition.isolated.len(), 1);
        assert_eq!(
            partition.stored_node(XY::new(1.0, 1.0)),
            Some(partition.isolated[0])
        );
    }

    #[test]
    fn explicit_point_occurrences_retain_incident_and_isolated_ownership() {
        let case = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(2.0, 0.0, 2.0, 0.0),
            seg(0.0, 0.0, 0.0, 0.0),
            seg(2.0, 0.0, 2.0, 0.0),
            seg(7.0, 3.0, 7.0, 3.0),
        ]);
        let partition = case.partition();
        let middle = partition.stored_node(XY::new(2.0, 0.0)).unwrap();
        let endpoint = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
        let isolated = partition.stored_node(XY::new(7.0, 3.0)).unwrap();
        assert_eq!(partition.point_nodes, [middle, endpoint, middle, isolated]);
        assert_eq!(partition.isolated, [isolated]);
        assert_eq!(partition.spans.len(), 2);
    }

    #[test]
    fn constructed_crossing_splits_every_overlapping_source_in_both_orders() {
        let mut sources = [
            seg(0.0, 0.0, 4.0, 0.0),
            seg(1.0, 0.0, 3.0, 0.0),
            seg(0.0, -1.0, 4.0, 2.0),
            seg(4.0, 0.0, 0.0, 0.0),
            seg(10.0, 10.0, 11.0, 11.0),
            seg(11.0, 11.0, 12.0, 12.0),
            seg(20.0, 10.0, 21.0, 11.0),
        ];
        for reverse in [false, true] {
            if reverse {
                sources.reverse();
            }
            let case = run(&sources);
            let partition = case.partition();
            let constructed: Vec<_> = partition.crossings().into_iter().collect();
            assert_eq!(constructed.len(), 1);
            let crossing = constructed[0];
            let departures: Vec<_> = [
                XY::new(3.0, 0.0),
                XY::new(4.0, 2.0),
                XY::new(1.0, 0.0),
                XY::new(0.0, -1.0),
            ]
            .into_iter()
            .map(|point| {
                let target = partition.stored_node(point).unwrap();
                partition
                    .spans
                    .iter()
                    .position(|span| {
                        (span.start == crossing && span.end == target)
                            || (span.end == crossing && span.start == target)
                    })
                    .unwrap()
            })
            .collect();
            for (position, &left) in departures.iter().enumerate() {
                for &right in &departures[position + 1..] {
                    assert_eq!(
                        partition.compare_departures(crossing, left, right),
                        Ordering::Less
                    );
                    assert_eq!(
                        partition.compare_departures(crossing, right, left),
                        Ordering::Greater
                    );
                }
            }
            let exact = partition.exact(crossing).unwrap();
            let [x, y, w] = &*exact;
            let (topology, edges) = partition.local_topology().unwrap();
            let row = topology.starts[crossing.0 as usize] as usize
                ..topology.starts[crossing.0 as usize + 1] as usize;
            assert_eq!(row.len(), 4);
            for (&edge, &expected) in edges[row].iter().zip(&departures) {
                assert_eq!(edge as usize, expected);
            }
            let four = ExactDyadic::from_f64(4.0);
            let three = ExactDyadic::from_f64(3.0);
            let zero = ExactDyadic::from_f64(0.0);
            let one = ExactDyadic::from_f64(1.0);
            assert_eq!(
                compare_ratios(x, w, &four, &three).unwrap(),
                Ordering::Equal
            );
            assert_eq!(compare_ratios(y, w, &zero, &one).unwrap(), Ordering::Equal);
            let left = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
            let right = partition.stored_node(XY::new(3.0, 0.0)).unwrap();
            assert_eq!(
                partition.compare_nodes(left, crossing).unwrap(),
                Ordering::Less
            );
            assert_eq!(
                partition.compare_nodes(right, crossing).unwrap(),
                Ordering::Greater
            );
            let mut owner_counts: Vec<_> = partition
                .spans
                .iter()
                .enumerate()
                .filter(|(_, span)| span.start == crossing || span.end == crossing)
                .map(|(index, _)| partition.owners(index).len())
                .collect();
            owner_counts.sort_unstable();
            assert_eq!(owner_counts, [1, 1, 3, 3]);
            assert_eq!(partition.spans.len(), 9);
        }
    }

    #[test]
    fn local_walks_keep_nested_components_and_bridge_sides_separate() {
        let case = run(&[
            seg(8.0, 8.0, 9.0, 9.0),
            seg(4.0, 2.0, 3.0, 4.0),
            seg(0.0, 0.0, 8.0, 0.0),
            seg(8.0, 0.0, 8.0, 8.0),
            seg(8.0, 8.0, 0.0, 8.0),
            seg(0.0, 8.0, 0.0, 0.0),
            seg(2.0, 2.0, 4.0, 2.0),
            seg(3.0, 4.0, 2.0, 2.0),
            seg(6.0, 6.0, 6.0, 6.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let outer = partition.stored_node(XY::new(0.0, 0.0)).unwrap().0 as usize;
        let inner = partition.stored_node(XY::new(2.0, 2.0)).unwrap().0 as usize;
        let isolated = partition.stored_node(XY::new(6.0, 6.0)).unwrap().0 as usize;
        let minima = partition.component_minima(&topology).unwrap();
        assert_eq!(minima.len(), 3);
        assert_eq!(
            minima[topology.component_of[outer] as usize],
            NodeId(outer as u32)
        );
        assert_eq!(
            minima[topology.component_of[inner] as usize],
            NodeId(inner as u32)
        );
        assert_eq!(
            minima[topology.component_of[isolated] as usize],
            NodeId(isolated as u32)
        );
        assert_ne!(topology.component_of[outer], topology.component_of[inner]);
        assert_ne!(
            topology.component_of[outer],
            topology.component_of[isolated]
        );
        assert_ne!(
            topology.component_of[inner],
            topology.component_of[isolated]
        );
        assert_eq!(topology.starts[isolated], topology.starts[isolated + 1]);
        let mut lengths: Vec<_> = topology
            .face_starts
            .array_windows::<2>()
            .map(|[start, end]| end - start)
            .collect();
        lengths.sort_unstable();
        assert_eq!(lengths, [3, 3, 4, 6]);
        let tip = partition.stored_node(XY::new(9.0, 9.0)).unwrap().0 as usize;
        let slot = topology.starts[tip] as usize;
        let twin = topology.twins[slot] as usize;
        assert_eq!(topology.face_of[slot], topology.face_of[twin]);
        assert_eq!(edges[slot], edges[twin]);
        let outer_walk = partition
            .exterior_walk(&topology, &edges, NodeId(outer as u32))
            .unwrap()
            .unwrap();
        assert_eq!(outer_walk, topology.face_of[slot]);
        let inner_walk = partition
            .exterior_walk(&topology, &edges, NodeId(inner as u32))
            .unwrap()
            .unwrap();
        assert_ne!(inner_walk, outer_walk);
        assert_eq!(
            partition
                .exterior_walk(&topology, &edges, NodeId(isolated as u32))
                .unwrap(),
            None
        );
        assert_eq!(partition.left_contact(NodeId(outer as u32)), None);
        let wall_start = partition.stored_node(XY::new(0.0, 8.0)).unwrap();
        let wall = partition
            .spans
            .iter()
            .position(|span| {
                (span.start == wall_start && span.end == NodeId(outer as u32))
                    || (span.end == wall_start && span.start == NodeId(outer as u32))
            })
            .unwrap();
        for query in [inner, isolated] {
            assert_eq!(
                partition.left_contact(NodeId(query as u32)),
                Some(wall as u32)
            );
        }
        let host = partition.contact_walk(&topology, &edges, wall as u32);
        assert_ne!(host, outer_walk);
        let wall_slot = (topology.starts[outer] as usize..topology.starts[outer + 1] as usize)
            .find(|&slot| edges[slot] == wall as u32)
            .unwrap();
        assert_eq!(host, topology.face_of[topology.twins[wall_slot] as usize]);
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(faces.count, 3);
        assert_eq!(faces.walk_faces[outer_walk as usize], 0);
        assert_eq!(
            faces.walk_faces[inner_walk as usize],
            faces.walk_faces[host as usize]
        );
        assert_ne!(faces.walk_faces[host as usize], 0);
        assert_eq!(
            faces.component_hosts[topology.component_of[isolated] as usize],
            faces.walk_faces[host as usize]
        );
    }

    #[test]
    fn exterior_walk_at_minimum_with_only_lower_departures() {
        let case = run(&[
            seg(0.0, 0.0, 2.0, -3.0),
            seg(2.0, -3.0, 3.0, -1.0),
            seg(3.0, -1.0, 0.0, 0.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let root = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
        let upper_target = partition.stored_node(XY::new(3.0, -1.0)).unwrap();
        let slot = (topology.starts[root.0 as usize] as usize
            ..topology.starts[root.0 as usize + 1] as usize)
            .find(|&slot| topology.targets[slot] == upper_target.0)
            .unwrap();
        let exterior = partition
            .exterior_walk(&topology, &edges, root)
            .unwrap()
            .unwrap();
        assert_eq!(exterior, topology.face_of[slot]);
        assert_ne!(exterior, topology.face_of[topology.twins[slot] as usize]);
    }

    /// Host faces for a shell with a grid of holes come from one sweep: four
    /// times the holes costs about four times the orientation tests, not
    /// sixteen.
    #[test]
    fn hole_hosts_scale_with_the_hole_count() {
        let orientations = |side: u32| {
            let extent = 10.0 * f64::from(side) + 10.0;
            let mut segments = vec![
                seg(0.0, 0.0, extent, 0.0),
                seg(extent, 0.0, extent, extent),
                seg(extent, extent, 0.0, extent),
                seg(0.0, extent, 0.0, 0.0),
            ];
            for row in 0..side {
                for column in 0..side {
                    let (x, y) = (10.0 * f64::from(column) + 5.0, 10.0 * f64::from(row) + 5.0);
                    segments.extend([
                        seg(x, y, x, y + 2.0),
                        seg(x, y + 2.0, x + 2.0, y + 2.0),
                        seg(x + 2.0, y + 2.0, x + 2.0, y),
                        seg(x + 2.0, y, x, y),
                    ]);
                }
            }
            let case = run(&segments);
            let partition = case.partition();
            let (topology, edges) = partition.local_topology().unwrap();
            reset_partition_builds();
            let faces = partition.global_faces(&topology, &edges).unwrap();
            assert_eq!(faces.count as usize, 2 + (side * side) as usize);
            partition_metrics().ray_orientations
        };
        let (small, large) = (orientations(16), orientations(32));
        assert!(large < 6 * small, "{small} -> {large}");
    }

    /// A ray just above a horizontal span or a vertex touching the query's
    /// height passes over them: a point beside a horizontal span meets
    /// nothing, and one beside a V meets the V's rising right side, whose
    /// east face is the unbounded one either way.
    #[test]
    fn rays_pass_over_horizontal_spans_and_tangent_vertices() {
        for (segments, contact) in [
            (vec![seg(0.0, 0.0, 2.0, 0.0), seg(4.0, 0.0, 4.0, 0.0)], None),
            (
                vec![
                    seg(0.0, 2.0, 2.0, 0.0),
                    seg(2.0, 0.0, 3.0, 2.0),
                    seg(4.0, 0.0, 4.0, 0.0),
                ],
                Some((XY::new(2.0, 0.0), XY::new(3.0, 2.0))),
            ),
        ] {
            let case = run(&segments);
            let partition = case.partition();
            let (topology, edges) = partition.local_topology().unwrap();
            let query = partition.stored_node(XY::new(4.0, 0.0)).unwrap();
            let expected = contact.map(|(start, end)| {
                let (start, end) = (
                    partition.stored_node(start).unwrap(),
                    partition.stored_node(end).unwrap(),
                );
                partition
                    .spans
                    .iter()
                    .position(|span| span.start == start && span.end == end)
                    .unwrap() as u32
            });
            assert_eq!(partition.left_contact(query), expected);
            let faces = partition.global_faces(&topology, &edges).unwrap();
            assert_eq!(
                faces.component_hosts[topology.component_of[query.0 as usize] as usize],
                0
            );
        }
    }

    #[test]
    fn vertex_visibility_links_disconnected_exteriors_to_infinity() {
        let case = run(&[
            seg(0.0, 2.0, 2.0, 0.0),
            seg(2.0, 0.0, 3.0, 2.0),
            seg(3.0, 2.0, 0.0, 2.0),
            seg(4.0, 0.0, 6.0, 0.0),
            seg(6.0, 0.0, 5.0, 1.0),
            seg(5.0, 1.0, 4.0, 0.0),
            seg(8.0, 0.0, 8.0, 0.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let root = partition.stored_node(XY::new(4.0, 0.0)).unwrap();
        let (valley, rim) = (
            partition.stored_node(XY::new(2.0, 0.0)).unwrap(),
            partition.stored_node(XY::new(3.0, 2.0)).unwrap(),
        );
        let contact = partition
            .spans
            .iter()
            .position(|span| {
                (span.start == valley && span.end == rim)
                    || (span.start == rim && span.end == valley)
            })
            .unwrap() as u32;
        assert_eq!(partition.left_contact(root), Some(contact));
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(faces.count, 3);
        assert_eq!(faces.component_hosts, [0, 0, 0]);
        let exterior = partition
            .exterior_walk(&topology, &edges, root)
            .unwrap()
            .unwrap();
        assert_eq!(faces.walk_faces[exterior as usize], 0);
        let host = partition.contact_walk(&topology, &edges, contact);
        assert_eq!(faces.walk_faces[host as usize], 0);
        let mut windings = Vec::new();
        partition
            .visit_face_windings(
                &topology,
                &edges,
                &faces,
                &[Some(0), Some(0), Some(0), Some(1), Some(1), Some(1), None],
                2,
                |_, winding| {
                    windings.push(*winding.first_chunk::<2>().expect("two rings are labelled"));
                    Ok(())
                },
            )
            .unwrap();
        windings.sort_unstable();
        assert_eq!(windings, [[0, 0], [0, 1], [1, 0]]);
    }

    #[test]
    fn face_windings_keep_nested_reversed_ring_multiplicity() {
        let case = run(&[
            seg(0.0, 0.0, 8.0, 0.0),
            seg(8.0, 0.0, 8.0, 8.0),
            seg(8.0, 8.0, 0.0, 8.0),
            seg(0.0, 8.0, 0.0, 0.0),
            seg(2.0, 2.0, 3.0, 4.0),
            seg(3.0, 4.0, 4.0, 2.0),
            seg(4.0, 2.0, 2.0, 2.0),
            seg(2.0, 2.0, 3.0, 4.0),
            seg(3.0, 4.0, 4.0, 2.0),
            seg(4.0, 2.0, 2.0, 2.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let mut windings = Vec::new();
        partition
            .visit_face_windings(
                &topology,
                &edges,
                &faces,
                &[
                    Some(0),
                    Some(0),
                    Some(0),
                    Some(0),
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(1),
                ],
                2,
                |_, winding| {
                    windings.push(*winding.first_chunk::<2>().expect("two rings are labelled"));
                    Ok(())
                },
            )
            .unwrap();
        windings.sort_unstable();
        assert_eq!(windings, [[0, 0], [1, -2], [1, 0]]);
    }

    #[test]
    fn selected_boundaries_keep_hole_direction_and_host_face() {
        let case = run(&[
            seg(0.0, 0.0, 6.0, 0.0),
            seg(6.0, 0.0, 6.0, 6.0),
            seg(6.0, 6.0, 0.0, 6.0),
            seg(0.0, 6.0, 0.0, 0.0),
            seg(2.0, 2.0, 4.0, 2.0),
            seg(4.0, 2.0, 4.0, 4.0),
            seg(4.0, 4.0, 2.0, 4.0),
            seg(2.0, 4.0, 2.0, 2.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(faces.count, 3);
        let inner = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
        let host = faces.component_hosts[topology.component_of[inner.0 as usize] as usize];
        let mut occupied = vec![false; 3];
        occupied[host as usize] = true;
        let plan = faces
            .boundary_plan(&Vec::new(), &topology, &edges, &partition, &occupied)
            .unwrap();
        assert_eq!(plan.face_quotient, [0, 1, 2]);
        assert_eq!(
            plan.walks[0]
                .iter()
                .map(|chord| chord.sides)
                .collect::<Vec<_>>(),
            vec![[host, 0]; 4]
        );
        assert_eq!(plan.walks[1].len(), 4);
        assert_eq!(plan.polygon_walks(&partition, &topology).unwrap(), [vec![
            0, 1
        ]]);
        let mut displaced = plan.round_items(&partition, &topology).unwrap();
        assert_eq!(displaced.len(), 8);
        for segment in &mut displaced[4..] {
            segment.start.x += 20.0;
            segment.end.x += 20.0;
        }
        let items = plan.incident_items(&topology).unwrap();
        plan.check_emitted_fans(&topology, &displaced).unwrap();
        let error = plan
            .check_global_faces(&partition, &items, &displaced)
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactGlobalFace { .. })
        ));
        for sides in plan.walks[1].iter().map(|chord| chord.sides) {
            assert_eq!(sides[0], host);
            assert_ne!(sides[1], 0);
            assert_ne!(sides[1], host);
        }
        let walks = plan.walks;
        assert_eq!(walks.len(), 2);
        for (walk, expected) in
            walks
                .iter()
                .zip([[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0)], [
                    (2.0, 2.0),
                    (2.0, 4.0),
                    (4.0, 4.0),
                    (4.0, 2.0),
                ]])
        {
            let actual: Vec<_> = walk
                .iter()
                .map(|chord| {
                    let slot = plan.source_darts[chord.sources.start];
                    assert_eq!(
                        faces.walk_faces[topology.face_of[slot as usize] as usize],
                        host
                    );
                    let node = NodeId(topology.owners[slot as usize]);
                    assert!(partition.is_stored(node), "square corners are stored");
                    let point = partition.node_xy(node);
                    (point.x, point.y)
                })
                .collect();
            assert_eq!(actual, expected);
        }
        let empty = faces
            .boundary_plan(&Vec::new(), &topology, &edges, &partition, &[false; 3])
            .unwrap();
        assert!(empty.walks.is_empty());
        assert!(empty.source_darts.is_empty());
        assert!(empty.round_items(&partition, &topology).unwrap().is_empty());
        assert_eq!(empty.face_quotient, [0, 0, 0]);
    }

    #[test]
    fn selected_boundaries_split_a_hole_touching_its_shell() {
        let case = run(&[
            seg(0.0, 0.0, 6.0, 0.0),
            seg(6.0, 0.0, 6.0, 6.0),
            seg(6.0, 6.0, 0.0, 6.0),
            seg(0.0, 6.0, 0.0, 0.0),
            seg(0.0, 3.0, 2.0, 2.0),
            seg(2.0, 2.0, 2.0, 4.0),
            seg(2.0, 4.0, 0.0, 3.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let origin = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
        let east = partition.stored_node(XY::new(6.0, 0.0)).unwrap();
        let bottom = (topology.starts[origin.0 as usize]..topology.starts[origin.0 as usize + 1])
            .find(|&slot| topology.targets[slot as usize] == east.0)
            .unwrap();
        let host = faces.walk_faces[topology.face_of[bottom as usize] as usize];
        let mut occupied = vec![false; faces.count as usize];
        occupied[host as usize] = true;
        let plan = faces
            .boundary_plan(&Vec::new(), &topology, &edges, &partition, &occupied)
            .unwrap();
        let mut lengths: Vec<_> = plan.walks.iter().map(Vec::len).collect();
        lengths.sort_unstable();
        assert_eq!(lengths, [3, 5]);
        let polygons = plan.polygon_walks(&partition, &topology).unwrap();
        assert_eq!(polygons.len(), 1);
        assert_eq!(
            polygons[0]
                .iter()
                .map(|&ring| plan.walks[ring].len())
                .collect::<Vec<_>>(),
            [5, 3]
        );
        let contact = partition.stored_node(XY::new(0.0, 3.0)).unwrap();
        for walk in &plan.walks {
            assert_eq!(
                walk.iter()
                    .filter(|chord| {
                        topology.owners[plan.source_darts[chord.sources.start] as usize]
                            == contact.0
                    })
                    .count(),
                1
            );
        }
        assert_eq!(plan.round_items(&partition, &topology).unwrap().len(), 8);
    }

    #[test]
    fn selected_polygon_walks_classify_constructed_extrema() {
        for sources in [
            [
                seg(-2.0, 0.0, 2.0, 0.0),
                seg(-1.0, -2.0, 1.0, 4.0),
                seg(-1.0, 2.0, 2.0, -1.0),
            ],
            [
                seg(-4.0, 4.0, 4.0, -4.0),
                seg(-4.0, 9.0, 4.0, -7.0),
                seg(-4.0, 11.0, 4.0, -13.0),
            ],
        ] {
            let case = run(&sources);
            let partition = case.partition();
            let (topology, edges) = partition.local_topology().unwrap();
            let faces = partition.global_faces(&topology, &edges).unwrap();
            let occupied: Vec<_> = (0..faces.count).map(|face| face != 0).collect();
            let plan = faces
                .boundary_plan(&Vec::new(), &topology, &edges, &partition, &occupied)
                .unwrap();
            let polygons = plan.polygon_walks(&partition, &topology).unwrap();
            assert_eq!(polygons.len(), 1);
            assert_eq!(polygons[0].len(), 1);
            assert_eq!(plan.walks[polygons[0][0]].len(), 3);
            for chord in &plan.walks[polygons[0][0]] {
                let node = NodeId(topology.owners[plan.source_darts[chord.sources.start] as usize]);
                assert!(!partition.is_stored(node));
            }
            assert_eq!(plan.round_items(&partition, &topology).unwrap().len(), 3);
        }
    }

    #[test]
    fn certified_parts_serialize_polygon_line_and_isolated_point() {
        let case = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(4.0, 0.0, 4.0, 4.0),
            seg(4.0, 4.0, 0.0, 4.0),
            seg(0.0, 4.0, 0.0, 0.0),
            seg(6.0, 1.0, 8.0, 1.0),
            seg(10.0, 3.0, 10.0, 3.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let cells = ArealCells {
            faces: (0..faces.count).map(|face| face != 0).collect(),
            spans: vec![true; partition.spans.len()],
            nodes: vec![true; partition.node_count()],
        };
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &faces)
            .unwrap();
        let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
        assert_eq!(points.iter().map(|point| point.xy()).collect::<Vec<_>>(), [
            XY::new(10.0, 3.0)
        ]);
        assert_eq!(lines.len(), 1);
        let mut endpoints = lines[0].xs().to_vec();
        endpoints.sort_by(f64::total_cmp);
        assert_eq!(endpoints, [6.0, 8.0]);
        assert_eq!(lines[0].ys(), [1.0, 1.0]);
        assert_eq!(polygons.len(), 1);
        assert!(polygons[0].holes.is_empty());
        let shell = &polygons[0].shell;
        assert_eq!(shell.coords().xs().len(), 5);
        let mut corners: Vec<_> = shell
            .iter()
            .take(4)
            .map(|point| (point.x, point.y))
            .collect();
        corners.sort_by(|left, right| left.partial_cmp(right).unwrap());
        assert_eq!(corners, [(0.0, 0.0), (0.0, 4.0), (4.0, 0.0), (4.0, 4.0)]);
    }

    #[test]
    fn certified_line_serialization_keeps_bends_and_protected_vertices() {
        let case = run(&[
            seg(2.0, 0.0, 1.0, 0.0),
            seg(0.0, 0.0, 1.0, 0.0),
            seg(2.0, 0.0, 2.0, 3.0),
            seg(1.0, 0.0, 1.0, 0.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let cells = ArealCells {
            faces: vec![false; faces.count as usize],
            spans: vec![true; partition.spans.len()],
            nodes: vec![true; partition.node_count()],
        };
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &faces)
            .unwrap();
        let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
        assert!(points.is_empty());
        assert!(polygons.is_empty());
        assert_eq!(lines.len(), 1);
        let mut coordinates: Vec<_> = lines[0]
            .xs()
            .iter()
            .copied()
            .zip(lines[0].ys().iter().copied())
            .collect();
        if coordinates[0] != (0.0, 0.0) {
            coordinates.reverse();
        }
        assert_eq!(coordinates, [
            (0.0, 0.0),
            (1.0, 0.0),
            (2.0, 0.0),
            (2.0, 3.0)
        ]);
    }

    #[test]
    fn certified_line_serialization_closes_cycles_and_stops_at_junctions() {
        for spur in [false, true] {
            let mut segments = vec![
                seg(0.0, 0.0, 4.0, 0.0),
                seg(4.0, 4.0, 4.0, 0.0),
                seg(4.0, 4.0, 0.0, 4.0),
                seg(0.0, 0.0, 0.0, 4.0),
            ];
            if spur {
                segments.push(seg(0.0, 0.0, -2.0, 0.0));
            }
            let case = run(&segments);
            let partition = case.partition();
            let (topology, edges) = partition.local_topology().unwrap();
            let faces = partition.global_faces(&topology, &edges).unwrap();
            let cells = ArealCells {
                faces: vec![false; faces.count as usize],
                spans: vec![true; partition.spans.len()],
                nodes: vec![true; partition.node_count()],
            };
            let plan = cells
                .emission_plan(&partition, &topology, &edges, &faces)
                .unwrap();
            let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
            assert!(points.is_empty());
            assert!(polygons.is_empty());
            assert_eq!(lines.len(), if spur { 2 } else { 1 });
            let cycle = lines.iter().find(|line| line.xs().len() == 5).unwrap();
            let coordinates: Vec<_> = cycle
                .xs()
                .iter()
                .copied()
                .zip(cycle.ys().iter().copied())
                .collect();
            assert_eq!(coordinates.first(), coordinates.last());
            let mut corners = coordinates[..4].to_vec();
            let origin = corners
                .iter()
                .position(|&point| point == (0.0, 0.0))
                .unwrap();
            corners.rotate_left(origin);
            if corners[1] == (4.0, 0.0) {
                corners[1..].reverse();
            }
            assert_eq!(corners, [(0.0, 0.0), (0.0, 4.0), (4.0, 4.0), (4.0, 0.0)]);
            if spur {
                let line = lines.iter().find(|line| line.xs().len() == 2).unwrap();
                let mut xs = line.xs().to_vec();
                xs.sort_by(f64::total_cmp);
                assert_eq!(xs, [-2.0, 0.0]);
                assert_eq!(line.ys(), [0.0, 0.0]);
            }
        }
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "These assertions pin exact emitted ordinates, not approximate geometry"
    )]
    fn selected_polygon_walks_keep_an_island_inside_a_hole_separate() {
        let case = run(&[
            seg(0.0, 0.0, 10.0, 0.0),
            seg(10.0, 0.0, 10.0, 10.0),
            seg(10.0, 10.0, 0.0, 10.0),
            seg(0.0, 10.0, 0.0, 0.0),
            seg(2.0, 2.0, 8.0, 2.0),
            seg(8.0, 2.0, 8.0, 8.0),
            seg(8.0, 8.0, 2.0, 8.0),
            seg(2.0, 8.0, 2.0, 2.0),
            seg(4.0, 4.0, 6.0, 4.0),
            seg(6.0, 4.0, 6.0, 6.0),
            seg(6.0, 6.0, 4.0, 6.0),
            seg(4.0, 6.0, 4.0, 4.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let hole = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
        let island = partition.stored_node(XY::new(4.0, 4.0)).unwrap();
        let island_host = faces.component_hosts[topology.component_of[island.0 as usize] as usize];
        let outer_interior = faces.component_hosts[topology.component_of[hole.0 as usize] as usize];
        let occupied: Vec<_> = (0..faces.count)
            .map(|face| face == outer_interior || (face != 0 && face != island_host))
            .collect();
        let plan = faces
            .boundary_plan(&Vec::new(), &topology, &edges, &partition, &occupied)
            .unwrap();
        let polygons = plan.polygon_walks(&partition, &topology).unwrap();
        let mut ring_counts: Vec<_> = polygons.iter().map(Vec::len).collect();
        ring_counts.sort_unstable();
        assert_eq!(ring_counts, [1, 2]);
        for polygon in polygons {
            let first = &plan.walks[polygon[0]][0];
            let node = NodeId(topology.owners[plan.source_darts[first.sources.start] as usize]);
            assert!(partition.is_stored(node), "stored square corner");
            let x = partition.node_xy(node).x;
            if polygon.len() == 1 {
                assert!((4.0..=6.0).contains(&x));
            } else {
                assert!(x == 0.0 || x == 10.0);
            }
        }
        assert_eq!(plan.round_items(&partition, &topology).unwrap().len(), 12);
        let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
        assert!(points.is_empty());
        assert!(lines.is_empty());
        assert_eq!(polygons.len(), 2);
        let outer = polygons
            .iter()
            .find(|polygon| !polygon.holes.is_empty())
            .unwrap();
        assert_eq!(outer.holes.len(), 1);
        assert!(
            outer
                .shell
                .coords()
                .xs()
                .iter()
                .all(|&x| x == 0.0 || x == 10.0)
        );
        assert!(
            outer.holes[0]
                .coords()
                .xs()
                .iter()
                .all(|&x| x == 2.0 || x == 8.0)
        );
        let island = polygons
            .iter()
            .find(|polygon| polygon.holes.is_empty())
            .unwrap();
        assert!(
            island
                .shell
                .coords()
                .xs()
                .iter()
                .all(|&x| x == 4.0 || x == 6.0)
        );
    }

    #[test]
    fn selected_boundaries_keep_point_touching_regions_separate() {
        let case = run(&[
            seg(0.0, 0.0, 2.0, 0.0),
            seg(2.0, 0.0, 2.0, 2.0),
            seg(2.0, 2.0, 0.0, 2.0),
            seg(0.0, 2.0, 0.0, 0.0),
            seg(2.0, 2.0, 4.0, 2.0),
            seg(4.0, 2.0, 4.0, 4.0),
            seg(4.0, 4.0, 2.0, 4.0),
            seg(2.0, 4.0, 2.0, 2.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(faces.count, 3);
        let plan = faces
            .boundary_plan(&Vec::new(), &topology, &edges, &partition, &[
                false, true, true,
            ])
            .unwrap();
        assert_eq!(plan.face_quotient, [0, 1, 2]);
        assert_eq!(
            plan.walks
                .iter()
                .map(|walk| walk.iter().map(|chord| chord.sides).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            [vec![[1, 0]; 4], vec![[2, 0]; 4]]
        );
        assert_eq!(plan.round_items(&partition, &topology).unwrap().len(), 8);
        assert_eq!(plan.walks.len(), 2);
        let walks = faces
            .boundary_walks(&topology, |face, other| {
                [false, true, true][face] && ![false, true, true][other]
            })
            .unwrap();
        assert_eq!(walks.len(), 2);
        let contact = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
        for walk in walks {
            assert_eq!(walk.len(), 4);
            assert_eq!(
                walk.iter()
                    .filter(|&&slot| topology.owners[slot as usize] == contact.0)
                    .count(),
                1
            );
        }
        assert!(
            faces
                .boundary_walks(&topology, |face, other| [false, false, false][face]
                    && ![false, false, false][other])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn emitted_fan_accepts_rotation_but_rejects_reflection() {
        let case = run(&[
            seg(0.0, 0.0, 2.0, 0.0),
            seg(0.0, 0.0, 0.0, 3.0),
            seg(0.0, 0.0, -4.0, 0.0),
            seg(0.0, 0.0, 0.0, -5.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let plan = faces
            .boundary_plan(&[0, 1, 2, 3], &topology, &edges, &partition, &[false])
            .unwrap();
        let segments = plan.round_items(&partition, &topology).unwrap();
        let rotated: Vec<_> = segments
            .iter()
            .map(|segment| Segment {
                start: XY::new(-segment.start.y, segment.start.x),
                end: XY::new(-segment.end.y, segment.end.x),
            })
            .collect();
        plan.check_emitted_fans(&topology, &rotated).unwrap();
        let reflected: Vec<_> = segments
            .iter()
            .map(|segment| Segment {
                start: XY::new(segment.start.x, -segment.start.y),
                end: XY::new(segment.end.x, -segment.end.y),
            })
            .collect();
        let error = plan.check_emitted_fans(&topology, &reflected).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactCyclicOrder { .. })
        ));
    }

    #[test]
    fn joined_chords_keep_endpoint_dart_order() {
        let case = run(&[
            seg(0.0, 0.0, 1.0, 0.0),
            seg(1.0, 0.0, 2.0, 0.0),
            seg(0.0, 0.0, 0.0, 1.0),
            seg(0.0, 1.0, 0.0, 3.0),
            seg(0.0, 0.0, -1.0, 0.0),
            seg(-1.0, 0.0, -4.0, 0.0),
            seg(0.0, 0.0, 0.0, -1.0),
            seg(0.0, -1.0, 0.0, -5.0),
        ]);
        let partition = case.partition();
        let (topology, _) = partition.local_topology().unwrap();
        let origin = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
        let mut darts = Vec::new();
        let mut chords = Vec::new();
        for endpoint in [
            XY::new(2.0, 0.0),
            XY::new(0.0, 3.0),
            XY::new(-4.0, 0.0),
            XY::new(0.0, -5.0),
        ] {
            let leaf = partition.stored_node(endpoint).unwrap();
            let end_slot = topology.starts[leaf.0 as usize];
            let middle = topology.targets[end_slot as usize];
            let start_slot = (topology.starts[origin.0 as usize]
                ..topology.starts[origin.0 as usize + 1])
                .find(|&slot| topology.targets[slot as usize] == middle)
                .unwrap();
            darts.push([start_slot, end_slot]);
            chords.push(Segment {
                start: XY::new(0.0, 0.0),
                end: endpoint,
            });
        }
        ExactPartition::check_emitted_fans(&topology, &darts, &chords).unwrap();
        let plan = BoundaryPlan {
            source_darts: darts
                .iter()
                .flat_map(|&[start, end]| [start, topology.twins[end as usize]])
                .collect(),
            walks: Vec::new(),
            lines: (0..4)
                .map(|index| PlannedChord {
                    sources: index * 2..index * 2 + 2,
                    sides: [0, 0],
                })
                .collect(),
            points: Vec::new(),
            face_quotient: vec![0],
        };
        let emitted = plan.round_items(&partition, &topology).unwrap();
        assert_eq!(
            emitted
                .iter()
                .map(|segment| (segment.start, segment.end))
                .collect::<Vec<_>>(),
            [
                (XY::new(0.0, 0.0), XY::new(2.0, 0.0)),
                (XY::new(0.0, 0.0), XY::new(0.0, 3.0)),
                (XY::new(0.0, 0.0), XY::new(-4.0, 0.0)),
                (XY::new(0.0, 0.0), XY::new(0.0, -5.0)),
            ]
        );
        chords.swap(1, 2);
        let error = ExactPartition::check_emitted_fans(&topology, &darts, &chords).unwrap_err();
        assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(
            GeometryErrorKind::ExactCyclicOrder { node }
        ) if *node == origin.0));
    }

    #[test]
    fn point_left_by_omitted_spoke_keeps_its_bounded_host() {
        let case = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(4.0, 0.0, 4.0, 4.0),
            seg(4.0, 4.0, 0.0, 4.0),
            seg(0.0, 4.0, 0.0, 0.0),
            seg(0.0, 0.0, 2.0, 2.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let point = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
        let mut cells = ArealCells {
            faces: vec![false; faces.count as usize],
            spans: vec![true; partition.spans.len()],
            nodes: vec![false; partition.node_count()],
        };
        for (index, span) in partition.spans.iter().enumerate() {
            if span.start == point || span.end == point {
                cells.spans[index] = false;
            }
        }
        cells.nodes[point.0 as usize] = true;
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &faces)
            .unwrap();
        assert_eq!(plan.points, [(point, 1)]);
        assert_eq!(plan.lines.len(), 4);
        assert!(plan.walks.is_empty());
        let items = plan.incident_items(&topology).unwrap();
        assert_eq!(items.len(), 5);
        assert_eq!(items[4], [point, point]);
        let rounded = plan.round_items(&partition, &topology).unwrap();
        assert_eq!(rounded[4].start, XY::new(2.0, 2.0));
        assert_eq!(rounded[4].end, XY::new(2.0, 2.0));
        let reflected: Vec<_> = rounded
            .iter()
            .map(|segment| Segment {
                start: XY::new(segment.start.x, -segment.start.y),
                end: XY::new(segment.end.x, -segment.end.y),
            })
            .collect();
        plan.check_emitted_fans(&topology, &reflected).unwrap();
        let error = plan
            .check_global_faces(&partition, &items, &reflected)
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactGlobalFace { .. })
        ));
        let mut displaced = rounded;
        displaced[4] = seg(6.0, 6.0, 6.0, 6.0);
        let error = plan
            .check_global_faces(&partition, &items, &displaced)
            .unwrap_err();
        assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(
            GeometryErrorKind::ExactGlobalFace { node }
        ) if *node == point.0));
    }

    #[test]
    fn retained_line_cycle_preserves_its_bounded_face() {
        let case = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(4.0, 0.0, 4.0, 4.0),
            seg(4.0, 4.0, 0.0, 4.0),
            seg(0.0, 4.0, 0.0, 0.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let plan = faces
            .boundary_plan(&[0, 1, 2, 3], &topology, &edges, &partition, &[
                false, false,
            ])
            .unwrap();
        assert!(plan.walks.is_empty());
        assert_eq!(
            plan.lines
                .iter()
                .flat_map(|chord| &plan.source_darts[chord.sources.clone()])
                .map(|&slot| edges[slot as usize])
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert_eq!(
            plan.lines
                .iter()
                .map(|chord| chord.sides)
                .collect::<Vec<_>>(),
            [[1, 0]; 4]
        );
        assert_eq!(plan.face_quotient, [0, 1]);
        let open = faces
            .boundary_plan(&[0, 1, 2], &topology, &edges, &partition, &[false, false])
            .unwrap();
        assert_eq!(open.face_quotient, [0, 0]);
        assert_eq!(
            open.lines
                .iter()
                .map(|chord| chord.sides)
                .collect::<Vec<_>>(),
            [[0, 0]; 3]
        );
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "This test distinguishes exact axis alignment from diagonal source spans"
    )]
    fn omitted_atoms_merge_faces_and_preserve_isolated_hosts() {
        let case = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(4.0, 0.0, 4.0, 4.0),
            seg(4.0, 4.0, 0.0, 4.0),
            seg(0.0, 4.0, 0.0, 0.0),
            seg(0.0, 0.0, 4.0, 4.0),
            seg(1.0, 2.0, 1.0, 2.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(faces.count, 3);
        let plan = faces
            .boundary_plan(&Vec::new(), &topology, &edges, &partition, &[
                false, true, true,
            ])
            .unwrap();
        assert_eq!(plan.face_quotient, [0, 1, 1]);
        assert_eq!(
            plan.walks
                .iter()
                .map(|walk| walk.iter().map(|chord| chord.sides).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            [vec![[1, 0]; 4]]
        );
        assert_eq!(plan.walks.len(), 1);
        let walks = faces
            .boundary_walks(&topology, |face, other| {
                [false, true, true][face] && ![false, true, true][other]
            })
            .unwrap();
        assert_eq!(walks.len(), 1);
        let boundary: Vec<_> = walks[0]
            .iter()
            .map(|&slot| NodeId(topology.owners[slot as usize]))
            .collect();
        assert_eq!(boundary, [
            partition.stored_node(XY::new(0.0, 0.0)).unwrap(),
            partition.stored_node(XY::new(4.0, 0.0)).unwrap(),
            partition.stored_node(XY::new(4.0, 4.0)).unwrap(),
            partition.stored_node(XY::new(0.0, 4.0)).unwrap(),
        ]);
        let mut retained = vec![true; partition.spans.len()];
        let identity = faces.quotient(&topology, &edges, &retained).unwrap();
        for (face, &mapped) in identity.iter().enumerate() {
            assert_eq!(mapped as usize, face);
        }
        for (edge, span) in partition.spans.iter().enumerate() {
            let source = partition.unique[span.unique as usize];
            if source.start.x != source.end.x && source.start.y != source.end.y {
                retained[edge] = false;
            }
        }
        let merged = faces.quotient(&topology, &edges, &retained).unwrap();
        assert_eq!(merged[0], 0);
        assert_ne!(merged[1], 0);
        assert_eq!(merged[1], merged[2]);
        let point = partition.stored_node(XY::new(1.0, 2.0)).unwrap();
        let host = faces.component_hosts[topology.component_of[point.0 as usize] as usize];
        assert_eq!(merged[host as usize], merged[1]);
        retained.fill(false);
        let erased = faces.quotient(&topology, &edges, &retained).unwrap();
        assert_eq!(erased, [0, 0, 0]);
    }

    #[test]
    fn face_windings_cancel_opposite_sources_and_ignore_open_linework() {
        let case = run(&[
            seg(0.0, 0.0, 4.0, 0.0),
            seg(4.0, 0.0, 2.0, 3.0),
            seg(2.0, 3.0, 0.0, 0.0),
            seg(0.0, 0.0, 2.0, 3.0),
            seg(2.0, 3.0, 4.0, 0.0),
            seg(4.0, 0.0, 0.0, 0.0),
            seg(-1.0, 1.0, 5.0, 1.0),
        ]);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let mut windings = Vec::new();
        partition
            .visit_face_windings(
                &topology,
                &edges,
                &faces,
                &[Some(0), Some(0), Some(0), Some(0), Some(0), Some(0), None],
                1,
                |_, winding| {
                    windings.push(winding[0]);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(windings, [0, 0, 0]);
    }

    #[test]
    fn degenerate_unique_without_isolated_slice_does_not_vanish() {
        let (unique, provenance) = dedup_undirected_segments_sourced(vec![
            seg(0.0, 0.0, 2.0, 0.0),
            seg(1.0, 1.0, 1.0, 1.0),
        ]);
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
        assert!(partition.stored_node(XY::new(1.0, 1.0)).is_some());
        assert_eq!(partition.isolated.len(), 1);
    }

    #[test]
    fn signed_zero_endpoints_share_a_stored_node() {
        let case = run(&[seg(-0.0, 0.0, 1.0, 0.0), seg(0.0, 0.0, 0.0, 1.0)]);
        let partition = case.partition();
        let a = partition.stored_node(XY::new(-0.0, 0.0)).unwrap();
        let b = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn full_finite_exponent_pair_is_five_nodes_four_spans() {
        let min_sub = f64::from_bits(1);
        let a = seg(0.0, 0.0, f64::MAX, min_sub);
        let b = seg(min_sub, -f64::MAX, f64::MAX, f64::MAX);
        let case = run(&[a, b]);
        let partition = case.partition();
        assert_eq!(partition.node_count(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 1);
        for point in [a.start, a.end, b.start, b.end] {
            assert!(partition.stored_node(point).is_some());
        }
        let crossing = crossing_node(&partition);
        for endpoint in [a.start, a.end, b.start, b.end] {
            let endpoint = partition.stored_node(endpoint).unwrap();
            assert_ne!(
                partition.compare_nodes(crossing, endpoint).unwrap(),
                Ordering::Equal
            );
        }
    }

    #[test]
    fn isolated_on_line_cuts_the_edge() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(vec![seg(0.0, 0.0, 2.0, 0.0)]);
        let partition =
            partition_unique_segments(&unique, &provenance, &[XY::new(1.0, 0.0)], &mut budget)
                .unwrap();
        assert_eq!(partition.node_count(), 3);
        assert_eq!(partition.spans.len(), 2);
        assert!(partition.isolated.is_empty());
        let mid = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
        assert!(
            partition
                .spans
                .iter()
                .any(|span| span.start == mid || span.end == mid)
        );
    }

    #[test]
    fn isolated_at_crossing_is_stored_not_constructed() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(vec![
            seg(0.0, 0.0, 2.0, 2.0),
            seg(0.0, 2.0, 2.0, 0.0),
        ]);
        let partition =
            partition_unique_segments(&unique, &provenance, &[XY::new(1.0, 1.0)], &mut budget)
                .unwrap();
        assert_eq!(partition.node_count(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 0);
        assert!(partition.isolated.is_empty());
        assert!(partition.is_stored(partition.stored_node(XY::new(1.0, 1.0)).unwrap()));
    }

    #[test]
    fn stored_endpoint_concurrency_is_six_nodes_five_spans() {
        let case = run(&[
            seg(0.0, 0.0, 2.0, 0.0),
            seg(1.0, 0.0, 1.0, 1.0),
            seg(0.0, 1.0, 2.0, -1.0),
        ]);
        let partition = case.partition();
        assert_eq!(partition.node_count(), 6);
        assert_eq!(partition.spans.len(), 5);
        assert_eq!(constructed_count(&partition), 0);
        let junction = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
        let incident = partition
            .spans
            .iter()
            .filter(|span| span.start == junction || span.end == junction)
            .count();
        assert_eq!(incident, 5);
    }

    #[test]
    fn concurrent_three_sources_recipe_when_vertical_is_first() {
        let case = run(&[
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.0, 0.0, 3.0, 1.0),
            seg(0.0, 1.0, 3.0, -1.0),
        ]);
        let partition = case.partition();
        assert_eq!(constructed_count(&partition), 1);
        let (one, three) = (dyadic(1.0), dyadic(3.0));
        assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
            &one, &three,
        ]);
    }

    /// Ordinary crossings are certified in double-double and never build an
    /// exact point: the lane is the one that actually runs.
    #[test]
    fn generic_crossings_round_without_exact_points() {
        let ring = |cx: f64, cy: f64, phase: f64| -> Vec<Segment> {
            let corners: Vec<XY> = (0..7)
                .map(|index| {
                    let angle = phase + std::f64::consts::TAU * f64::from(index) / 7.0;
                    XY::new(cx + 25.0 * angle.cos(), cy + 25.0 * angle.sin())
                })
                .collect();
            (0..7)
                .map(|index| Segment {
                    start: corners[index],
                    end: corners[(index + 1) % 7],
                })
                .collect()
        };
        let mut segments = ring(500_000.0, 5_200_000.0, 0.1);
        segments.extend(ring(500_020.0, 5_200_015.0, 0.2));
        reset_partition_builds();
        let case = run(&segments);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        partition.global_faces(&topology, &edges).unwrap();
        let crossings = (0..partition.node_count() as u32)
            .filter(|&node| !partition.is_stored(NodeId(node)))
            .count();
        assert!(crossings >= 2);
        let metrics = partition_metrics();
        assert_eq!(metrics.declined_crossings, 0);
        assert_eq!(metrics.exact_crossings, 0);
    }

    /// The crossing's exact y is 1.5 + 2^-52 + 2^-53, the midpoint between two
    /// doubles: the certificate declines and the exact rounding goes to even.
    #[test]
    fn rounding_midpoint_crossing_declines_and_rounds_to_even() {
        let x = 1.0 + f64::EPSILON;
        reset_partition_builds();
        let case = run(&[seg(0.0, 0.0, 2.0, 3.0), seg(x, 0.0, x, 3.0)]);
        let partition = case.partition();
        assert_eq!(partition_metrics().declined_crossings, 1);
        assert_eq!(
            partition.node_xy(crossing_node(&partition)),
            XY::new(x, 1.5 + 2.0 * f64::EPSILON)
        );
    }

    /// Three nearly concurrent segments cross at three points that differ
    /// below the last bit, two of which round to one double; they stay three
    /// nodes, ordered exactly.
    #[test]
    fn crossings_rounding_to_one_double_stay_distinct() {
        let case = run(&[
            seg(0.0, 0.0, 3.0, 1.0),
            seg(1.0, -1.0, 1.0, 1.0),
            seg(0.0, 1.0, 2.0, -1.0 / 3.0),
        ]);
        let partition = case.partition();
        let crossings: Vec<NodeId> = (0..partition.node_count() as u32)
            .map(NodeId)
            .filter(|&node| !partition.is_stored(node))
            .collect();
        assert_eq!(crossings.len(), 3);
        let mut shared_roundings = 0;
        for (index, &first) in crossings.iter().enumerate() {
            for &second in &crossings[index + 1..] {
                shared_roundings +=
                    usize::from(partition.node_xy(first) == partition.node_xy(second));
                assert_ne!(
                    partition.compare_nodes(first, second).unwrap(),
                    Ordering::Equal
                );
            }
        }
        assert_eq!(shared_roundings, 1);
    }

    /// Coordinates outside the certificate window and nearly parallel carriers
    /// decline to the exact construction, which still rounds nearest-even.
    #[test]
    fn out_of_window_and_near_parallel_crossings_decline() {
        for segments in [
            [
                seg(0.0, 0.0, 3e300, 1e300),
                seg(1e300, -1e300, 1e300, 1e300),
            ],
            [seg(0.0, 0.0, 1.0, 1.0), seg(0.0, 1e-17, 1.0, 1.0 - 1e-16)],
        ] {
            reset_partition_builds();
            let case = run(&segments);
            let partition = case.partition();
            assert_eq!(partition_metrics().declined_crossings, 1);
            let crossing = crossing_node(&partition);
            let [first_unique, second_unique] = partition.unique.first_chunk::<2>().unwrap();
            let [x, y, w] = crossing_homogeneous(*first_unique, *second_unique).unwrap();
            assert_eq!(
                partition.node_xy(crossing),
                XY::new(round_ratio(&x, &w).unwrap(), round_ratio(&y, &w).unwrap())
            );
        }
    }

    #[test]
    fn star_crossings_keep_one_recipe_per_constructed_node() {
        let mut segments = vec![seg(0.0, 0.0, 5.0, 0.0)];
        for x in 1..=4 {
            segments.push(seg(f64::from(x), -1.0, f64::from(x), 1.0));
        }
        let case = run(&segments);
        let partition = case.partition();
        assert_eq!(constructed_count(&partition), 4);
        let (zero, one) = (dyadic(0.0), dyadic(1.0));
        let mut found = Vec::new();
        for node in 0..partition.node_count() as u32 {
            let node = NodeId(node);
            if partition.is_stored(node) {
                continue;
            }
            let point = partition.node_xy(node);
            assert_eq!(point.y, 0.0);
            assert_exact_at(&partition, node, [&dyadic(point.x), &one], [&zero, &one]);
            found.push(point.x);
        }
        found.sort_by(f64::total_cmp);
        assert_eq!(found, [1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn disjoint_thirty_two_thousand_segments_are_not_capped_as_pairs() {
        let mut segments = Vec::with_capacity(32_000);
        for index in 0..32_000 {
            let x = f64::from(index);
            segments.push(seg(x, 0.0, x, 1.0));
        }
        let case = run(&segments);
        let partition = case.partition();
        assert_eq!(partition.node_count(), 64_000);
        assert_eq!(partition.spans.len(), 32_000);
        assert_eq!(constructed_count(&partition), 0);
    }

    #[test]
    fn pairwise_limit_is_not_an_allocation_error() {
        let err = ExpansionBudget::pairwise("exact_partition", "pairs", PAIRWISE_WORK_LIMIT, 2)
            .unwrap_err();
        assert!(matches!(
            err.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge { .. })
        ));
    }

    #[test]
    fn rectangle_intersection_uses_exact_source_contacts() {
        let crossing = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_xy(&[XY::new(-1.0, 0.5), XY::new(2.0, 0.5)]).unwrap(),
        ));
        let bounds = Bounds::new_raw(0.0, 0.0, 1.0, 1.0);
        assert!(exact_intersects_bounds(&crossing, bounds).unwrap());

        let touching = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_xy(&[XY::new(-1.0, 0.0), XY::new(0.0, 0.0)]).unwrap(),
        ));
        assert!(exact_intersects_bounds(&touching, bounds).unwrap());

        let disjoint = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_xy(&[XY::new(2.0, 2.0), XY::new(3.0, 3.0)]).unwrap(),
        ));
        assert!(!exact_intersects_bounds(&disjoint, bounds).unwrap());
    }
}
