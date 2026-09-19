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
#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::mem;
use std::ops::{ControlFlow, Range};

use crate::error::ResourceError;
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch, compare_departures};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::build_certified_overlay_shape;
use crate::geometry::overlay::noding::{SegmentProvenance, SegmentSource};
use crate::geometry::segments::{
    crossing_homogeneous, crossing_point, point_in_segment_envelope, point_on_segment,
    segment_envelopes_disjoint, stored_cross_sign, stored_orientation_sign, try_sort_by,
};
use crate::geometry::tessellation::exact::{ExactDyadic, Interval, compare_ratios, round_ratio};
use crate::geometry::{
    Bounds, Contact, CoordSeq, CoordinateAxes, Coordinates as _, CoverageOverlapRule, De9im,
    Dimension, ExactRationalArea, ExpansionBudget, GeometryErrorKind, GeometryKind, HashMap,
    LineSeq, Loc, Point, PointKey, Polygon, PolygonizeFull, Result, Ring, RingKind, Segment, Shape,
    XY, for_each_candidate_pair, point_distance, same_point, same_topological_coordinate,
    segment_contact_exact, single_chain,
};
use crate::{try_push_vec, try_reserve_vec};

#[cfg(test)]
thread_local! {
    static PARTITION_CALLS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_partition_calls() {
    PARTITION_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn partition_calls() -> usize {
    PARTITION_CALLS.with(Cell::get)
}

#[cfg(test)]
thread_local! {
    static BOUNDARY_PLAN_CALLS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_boundary_plan_calls() {
    BOUNDARY_PLAN_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn boundary_plan_calls() -> usize {
    BOUNDARY_PLAN_CALLS.with(Cell::get)
}

#[cfg(test)]
fn record_boundary_plan_call() {
    BOUNDARY_PLAN_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
}

#[cfg(test)]
std::thread_local! {
    static PARTITION_METRICS: std::cell::Cell<PartitionMetrics> =
        const { std::cell::Cell::new(PartitionMetrics::ZERO) };
}

#[cfg(test)]
pub(in crate::geometry) fn reset_partition_builds() {
    PARTITION_METRICS.with(|metrics| metrics.set(PartitionMetrics::ZERO));
}

#[cfg(test)]
pub(in crate::geometry) fn partition_builds() -> usize {
    PARTITION_METRICS.with(|metrics| metrics.get().partition_builds)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::geometry) struct PartitionMetrics {
    pub partition_builds: usize,
    pub source_segments: usize,
    pub unique_source_segments: usize,
    pub candidate_pairs_attempted: usize,
    pub candidate_pairs_distinct: usize,
    pub candidate_pool_items: usize,
    pub overlap_contacts: usize,
    pub overlap_cuts: usize,
    pub source_cut_entries: usize,
    pub canonical_spans: usize,
    pub canonical_nodes: usize,
    pub global_faces: usize,
    pub topology_halfedges: usize,
    pub publication_nodes: usize,
    pub published_rows: usize,
    /// Crossings whose double-double certificate declined at construction.
    pub declined_crossings: usize,
    /// Crossings whose exact point a comparison had to build afterwards.
    pub exact_crossings: usize,
    /// Orientation tests made by leftward ray sweeps.
    pub ray_orientations: usize,
    /// Emissions whose items all end at stored nodes, so rounding moved
    /// nothing and the emission checks had nothing to find.
    pub unmoved_emissions: usize,
}

#[cfg(test)]
impl PartitionMetrics {
    const ZERO: Self = Self {
        partition_builds: 0,
        source_segments: 0,
        unique_source_segments: 0,
        candidate_pairs_attempted: 0,
        candidate_pairs_distinct: 0,
        candidate_pool_items: 0,
        overlap_contacts: 0,
        overlap_cuts: 0,
        source_cut_entries: 0,
        canonical_spans: 0,
        canonical_nodes: 0,
        global_faces: 0,
        topology_halfedges: 0,
        publication_nodes: 0,
        published_rows: 0,
        declined_crossings: 0,
        exact_crossings: 0,
        ray_orientations: 0,
        unmoved_emissions: 0,
    };
}

#[cfg(test)]
pub(in crate::geometry) fn partition_metrics() -> PartitionMetrics {
    PARTITION_METRICS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn update_partition_metrics(update: impl FnOnce(&mut PartitionMetrics)) {
    PARTITION_METRICS.with(|metrics| {
        let mut current = metrics.get();
        update(&mut current);
        metrics.set(current);
    });
}

#[cfg(test)]
fn record_candidate_pair() {
    update_partition_metrics(|metrics| {
        metrics.candidate_pairs_attempted += 1;
        metrics.candidate_pairs_distinct += 1;
    });
}

#[cfg(test)]
fn record_overlap(cuts: usize) {
    update_partition_metrics(|metrics| {
        metrics.overlap_contacts += 1;
        metrics.overlap_cuts += cuts;
    });
}

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

struct GlobalFaces {
    walk_faces: Vec<u32>,
    component_hosts: Vec<u32>,
    count: u32,
}

struct CellFacts {
    face_membership: Vec<bool>,
    span_raw: Vec<bool>,
    span_curve: Vec<bool>,
    span_support: Vec<bool>,
    span_faces: Vec<[u32; 2]>,
    point_membership: Vec<bool>,
}

struct PlannedChord {
    sources: Range<usize>,
    sides: [u32; 2],
}

struct BoundaryPlan {
    source_darts: Vec<u32>,
    /// Chords on the boundary between an occupied and an unoccupied areal
    /// face. These are the shell and hole cycles of the selected filled area.
    walks: Vec<Vec<PlannedChord>>,
    /// Chords retained from lower-dimensional support with no occupied areal
    /// face on either side. They are residual lines, not areal boundary cycles.
    lines: Vec<PlannedChord>,
    points: Vec<(NodeId, u32)>,
    face_quotient: Vec<u32>,
}

struct NodeParts {
    points: Vec<NodeId>,
    lines: Vec<Vec<NodeId>>,
    polygons: Vec<Vec<Vec<NodeId>>>,
    rounded: Vec<(NodeId, XY)>,
}

struct CoverageFaceState {
    polygon_occupied: Vec<bool>,
    row_polygon_counts: Vec<usize>,
    active_positions: Vec<usize>,
    active_rows: Vec<usize>,
}

impl CoverageFaceState {
    fn new(polygon_count: usize, row_count: usize) -> Result<Self> {
        let mut polygon_occupied = Vec::new();
        try_reserve_vec(&mut polygon_occupied, polygon_count)?;
        polygon_occupied.resize(polygon_count, false);
        let mut row_polygon_counts = Vec::new();
        try_reserve_vec(&mut row_polygon_counts, row_count)?;
        row_polygon_counts.resize(row_count, 0_usize);
        let mut active_positions = Vec::new();
        try_reserve_vec(&mut active_positions, row_count)?;
        active_positions.resize(row_count, usize::MAX);
        let mut active_rows = Vec::new();
        try_reserve_vec(&mut active_rows, row_count)?;
        Ok(Self {
            polygon_occupied,
            row_polygon_counts,
            active_positions,
            active_rows,
        })
    }

    fn update_ring(
        &mut self,
        inventory: &ExactSourceInventory,
        winding: &mut [i64],
        ring: u32,
        delta: i64,
    ) -> Result<()> {
        let ring_index = ring as usize;
        let before = winding[ring_index] != 0;
        winding[ring_index] += delta;
        let after = winding[ring_index] != 0;
        if before == after {
            return Ok(());
        }
        let polygon = *inventory.ring_polygons.get(ring_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact coverage winding references an unknown ring".into())
        })? as usize;
        let occupied = rings_occupied(
            winding,
            inventory.polygon_ring_ranges[polygon].clone(),
            false,
        );
        if occupied == self.polygon_occupied[polygon] {
            return Ok(());
        }
        self.polygon_occupied[polygon] = occupied;
        let row = inventory.polygon_operands[polygon] as usize;
        if occupied {
            let count = self.row_polygon_counts.get_mut(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact coverage polygon operand is out of range".into())
            })?;
            let was_empty = *count == 0;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if was_empty {
                let position = self.active_rows.len();
                try_push_vec(&mut self.active_rows, row)?;
                self.active_positions[row] = position;
            }
        } else {
            let count = self.row_polygon_counts.get_mut(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact coverage polygon operand is out of range".into())
            })?;
            *count = count.checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact coverage polygon occupancy underflow".into())
            })?;
            if *count == 0 {
                let position = self.active_positions[row];
                let last = self.active_rows.pop().ok_or_else(|| {
                    GeometryErrorKind::Invalid("exact coverage active rows are out of sync".into())
                })?;
                if position != self.active_rows.len() {
                    self.active_rows[position] = last;
                    self.active_positions[last] = position;
                }
                self.active_positions[row] = usize::MAX;
            }
        }
        Ok(())
    }
}

impl BoundaryPlan {
    fn line_walks(&self, topology: &IndexedTopology) -> Result<Vec<Vec<(usize, bool)>>> {
        if self.lines.is_empty() {
            return Ok(Vec::new());
        }
        let mut ends = Vec::new();
        try_reserve_vec(&mut ends, self.lines.len())?;
        let mut incidence = Vec::new();
        try_reserve_vec(&mut incidence, topology.starts.len() - 1)?;
        incidence.resize(topology.starts.len() - 1, (0_usize, [0_usize; 2]));
        for (index, chord) in self.lines.iter().enumerate() {
            let pair = [
                topology.owners[self.source_darts[chord.sources.start] as usize] as usize,
                topology.targets[self.source_darts[chord.sources.end - 1] as usize] as usize,
            ];
            for node in pair {
                let (degree, edges) = &mut incidence[node];
                if *degree < 2 {
                    edges[*degree] = index;
                }
                *degree += 1;
            }
            try_push_vec(&mut ends, pair)?;
        }
        let mut used = Vec::new();
        try_reserve_vec(&mut used, self.lines.len())?;
        used.resize(self.lines.len(), false);
        let mut walks = Vec::new();
        // Open chains precede the remaining all-degree-two cycles.
        for cycles in [false, true] {
            for seed in 0..ends.len() {
                if used[seed] {
                    continue;
                }
                let start = ends[seed].iter().position(|&node| incidence[node].0 != 2);
                if !cycles && start.is_none() {
                    continue;
                }
                let mut from = ends[seed][start.unwrap_or(0)];
                let mut edge = seed;
                let mut walk = Vec::new();
                loop {
                    used[edge] = true;
                    let reversed = ends[edge][0] != from;
                    try_reserve_vec(&mut walk, 1)?;
                    try_push_vec(&mut walk, (edge, reversed))?;
                    let far = ends[edge][usize::from(!reversed)];
                    let (degree, adjacent) = incidence[far];
                    if degree != 2 {
                        break;
                    }
                    let next = adjacent[usize::from(adjacent[0] == edge)];
                    if used[next] {
                        break;
                    }
                    edge = next;
                    from = far;
                }
                try_reserve_vec(&mut walks, 1)?;
                try_push_vec(&mut walks, walk)?;
            }
        }
        Ok(walks)
    }

    fn emit_xy_parts(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        let groups = self.polygon_walks(partition, topology)?;
        let line_walks = self.line_walks(topology)?;
        let segments = self.round_items(partition, topology)?;
        let mut remaining = segments.as_slice();
        let mut rings = Vec::new();
        try_reserve_vec(&mut rings, self.walks.len())?;
        for walk in &self.walks {
            let (chords, rest) = remaining.split_at(walk.len());
            remaining = rest;
            try_push_vec(
                &mut rings,
                Some(closed_ring(chords.iter().map(|chord| chord.start))?),
            )?;
        }
        let mut polygons = Vec::new();
        try_reserve_vec(&mut polygons, groups.len())?;
        for group in groups {
            let mut indices = group.into_iter();
            let shell = rings[indices.next().expect("polygon has a shell")]
                .take()
                .expect("each ring has one polygon owner");
            let mut holes = Vec::new();
            try_reserve_vec(&mut holes, indices.len())?;
            for index in indices {
                try_push_vec(
                    &mut holes,
                    rings[index]
                        .take()
                        .expect("each ring has one polygon owner"),
                )?;
            }
            try_push_vec(&mut polygons, Polygon::new(shell, holes)?)?;
        }
        let (chords, isolated) = remaining.split_at(self.lines.len());
        let mut lines = Vec::new();
        try_reserve_vec(&mut lines, line_walks.len())?;
        for walk in line_walks {
            let mut xs = Vec::new();
            let mut ys = Vec::new();
            let coordinate_count = walk
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            try_reserve_vec(&mut xs, coordinate_count)?;
            try_reserve_vec(&mut ys, coordinate_count)?;
            for (index, reversed) in walk {
                let chord = chords[index];
                let (start, end) = if reversed {
                    (chord.end, chord.start)
                } else {
                    (chord.start, chord.end)
                };
                if xs.is_empty() {
                    try_push_vec(&mut xs, start.x)?;
                    try_push_vec(&mut ys, start.y)?;
                }
                try_push_vec(&mut xs, end.x)?;
                try_push_vec(&mut ys, end.y)?;
            }
            if xs.len() > 2
                && same_point(
                    XY::new(xs[0], ys[0]),
                    XY::new(xs[xs.len() - 1], ys[ys.len() - 1]),
                )
            {
                canonicalize_closed_xy(&mut xs, &mut ys);
                let open = xs.len() - 1;
                let next = (xs[1], ys[1]);
                let previous = (xs[open - 1], ys[open - 1]);
                if next > previous {
                    xs[1..open].reverse();
                    ys[1..open].reverse();
                    xs[open] = xs[0];
                    ys[open] = ys[0];
                }
            }
            if let (Some(first), Some(last)) =
                (xs.first().zip(ys.first()), xs.last().zip(ys.last()))
                && (last.0, last.1) < (first.0, first.1)
            {
                xs.reverse();
                ys.reverse();
            }
            try_push_vec(&mut lines, CoordSeq::try_from_vecs(xs, ys, None, None)?)?;
        }
        lines.sort_unstable_by(crate::geometry::compare_point_slices);
        let mut points = Vec::new();
        try_reserve_vec(&mut points, isolated.len())?;
        for item in isolated {
            try_push_vec(&mut points, Point::new_raw_xy(item.start.x, item.start.y))?;
        }
        Ok((points, lines, polygons))
    }

    fn emit_node_parts(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<NodeParts> {
        let items = self.incident_items(topology)?;
        let rounded_segments = self.round_items(partition, topology)?;
        self.emit_node_parts_from_segments(partition, topology, &items, &rounded_segments, true)
    }

    fn emit_node_parts_with_coordinates(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        coordinates: &[Option<XY>],
    ) -> Result<NodeParts> {
        let items = self.incident_items(topology)?;
        let mut rounded_segments = Vec::new();
        try_reserve_vec(&mut rounded_segments, items.len())?;
        for &[start, end] in &items {
            let start = coordinates
                .get(start.0 as usize)
                .and_then(Option::as_ref)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact subdivision output lost a rounded start node".into(),
                    )
                })?;
            let end = coordinates
                .get(end.0 as usize)
                .and_then(Option::as_ref)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact subdivision output lost a rounded end node".into(),
                    )
                })?;
            try_push_vec(&mut rounded_segments, Segment { start, end })?;
        }
        self.check_emitted_fans(topology, &rounded_segments)?;
        self.check_global_faces(partition, &items, &rounded_segments)?;
        self.emit_node_parts_from_segments(partition, topology, &items, &rounded_segments, true)
    }

    /// A row's parts from nodes rounded once for every row, whose
    /// injectivity the caller certified across the whole coverage.
    fn emit_node_parts_with_shared_rounding(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<NodeParts> {
        let items = self.incident_items(topology)?;
        let mut rounded_segments = Vec::new();
        try_reserve_vec(&mut rounded_segments, items.len())?;
        rounded_segments.extend(items.iter().map(|&[start, end]| Segment {
            start: partition.node_xy(start),
            end: partition.node_xy(end),
        }));
        if partition.rounding_moves(&items) {
            self.check_emitted_fans(topology, &rounded_segments)?;
            self.check_global_faces(partition, &items, &rounded_segments)?;
        }
        self.emit_node_parts_from_segments(partition, topology, &items, &rounded_segments, false)
    }

    fn emit_node_parts_from_segments(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        items: &[[NodeId; 2]],
        rounded_segments: &[Segment],
        retain_rounded: bool,
    ) -> Result<NodeParts> {
        let mut rounded = Vec::new();
        if retain_rounded {
            let rounded_capacity = items
                .len()
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?;
            try_reserve_vec(&mut rounded, rounded_capacity)?;
            for (&[start, end], segment) in items.iter().zip(rounded_segments) {
                try_push_vec(&mut rounded, (start, segment.start))?;
                try_push_vec(&mut rounded, (end, segment.end))?;
            }
        }
        let node_chord = |chord: &PlannedChord| {
            let first = self.source_darts[chord.sources.start] as usize;
            let last = self.source_darts[chord.sources.end - 1] as usize;
            [
                NodeId(topology.owners[first]),
                NodeId(topology.targets[last]),
            ]
        };

        let groups = self.polygon_walks(partition, topology)?;
        let mut rings: Vec<Option<Vec<NodeId>>> = Vec::new();
        try_reserve_vec(&mut rings, self.walks.len())?;
        rings.resize_with(self.walks.len(), || None);
        for (index, walk) in self.walks.iter().enumerate() {
            let mut nodes = Vec::new();
            let node_capacity = walk
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            try_reserve_vec(&mut nodes, node_capacity)?;
            for chord in walk {
                try_push_vec(&mut nodes, node_chord(chord)[0])?;
            }
            if let Some(&first) = nodes.first() {
                try_push_vec(&mut nodes, first)?;
                canonicalize_closed_nodes(partition, &mut nodes)?;
            }
            rings[index] = Some(nodes);
        }
        let mut polygons = Vec::new();
        try_reserve_vec(&mut polygons, groups.len())?;
        for group in groups {
            let mut indices = group.into_iter();
            let shell = rings[indices.next().expect("polygon has a shell")]
                .take()
                .expect("each ring has one polygon owner");
            let mut polygon_rings = Vec::new();
            let ring_capacity = indices
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            try_reserve_vec(&mut polygon_rings, ring_capacity)?;
            try_push_vec(&mut polygon_rings, shell)?;
            for index in indices {
                try_push_vec(
                    &mut polygon_rings,
                    rings[index]
                        .take()
                        .expect("each ring has one polygon owner"),
                )?;
            }
            try_push_vec(&mut polygons, polygon_rings)?;
        }

        let line_walks = self.line_walks(topology)?;
        let mut lines = Vec::new();
        try_reserve_vec(&mut lines, line_walks.len())?;
        for walk in line_walks {
            let mut nodes = Vec::new();
            let node_capacity = walk
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            try_reserve_vec(&mut nodes, node_capacity)?;
            for (index, reversed) in walk {
                let [start, end] = node_chord(&self.lines[index]);
                let (start, end) = if reversed { (end, start) } else { (start, end) };
                if nodes.is_empty() {
                    try_push_vec(&mut nodes, start)?;
                }
                try_push_vec(&mut nodes, end)?;
            }
            if nodes.len() > 2 && nodes.first() == nodes.last() {
                canonicalize_closed_nodes(partition, &mut nodes)?;
                let open = nodes.len() - 1;
                if partition.compare_nodes(nodes[1], nodes[open - 1])? == Ordering::Greater {
                    nodes[1..open].reverse();
                    nodes[open] = nodes[0];
                }
            }
            if let (Some(&first), Some(&last)) = (nodes.first(), nodes.last())
                && partition.compare_nodes(last, first)? == Ordering::Less
            {
                nodes.reverse();
            }
            try_push_vec(&mut lines, nodes)?;
        }
        let mut points = Vec::new();
        try_reserve_vec(&mut points, self.points.len())?;
        points.extend(self.points.iter().map(|&(node, _)| node));
        Ok(NodeParts {
            points,
            lines,
            polygons,
            rounded,
        })
    }

    fn polygon_walks(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<Vec<Vec<usize>>> {
        let mut shells = Vec::new();
        try_reserve_vec(&mut shells, self.face_quotient.len())?;
        shells.resize(self.face_quotient.len(), None);
        let mut holes = Vec::new();
        let mut polygons = Vec::new();
        for (index, walk) in self.walks.iter().enumerate() {
            let is_shell = walk_is_shell(
                partition,
                topology,
                walk.len(),
                |chord| self.source_darts[walk[chord].sources.start],
                |chord| self.source_darts[walk[chord].sources.end - 1],
            )?;
            // Each bounded occupied face has one outer cycle; its other
            // cycles bound holes, even when they touch that outer cycle.
            let host = walk[0].sides[0] as usize;
            debug_assert!(walk.iter().all(|chord| chord.sides[0] as usize == host));
            if is_shell {
                debug_assert!(shells[host].is_none());
                shells[host] = Some(polygons.len());
                let mut rings = Vec::new();
                try_reserve_vec(&mut rings, 1)?;
                try_push_vec(&mut rings, index)?;
                try_reserve_vec(&mut polygons, 1)?;
                try_push_vec(&mut polygons, rings)?;
            } else {
                try_reserve_vec(&mut holes, 1)?;
                try_push_vec(&mut holes, (host, index))?;
            }
        }
        for (host, hole) in holes {
            let rings = &mut polygons[shells[host].expect("bounded occupied face has a shell")];
            try_reserve_vec(rings, 1)?;
            try_push_vec(rings, hole)?;
        }
        Ok(polygons)
    }

    fn check_emitted_fans(&self, topology: &IndexedTopology, segments: &[Segment]) -> Result<()> {
        let mut darts = Vec::new();
        for chord in self.walks.iter().flatten().chain(&self.lines) {
            try_reserve_vec(&mut darts, 1)?;
            let first = self.source_darts[chord.sources.start];
            let last = self.source_darts[chord.sources.end - 1];
            try_push_vec(&mut darts, [first, topology.twins[last as usize]])?;
        }
        debug_assert_eq!(segments.len(), darts.len() + self.points.len());
        ExactPartition::check_emitted_fans(topology, &darts, &segments[..darts.len()])
    }

    fn round_items(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
    ) -> Result<Vec<Segment>> {
        let items = self.incident_items(topology)?;
        let segments = partition.round_incident_items(&items)?;
        if partition.rounding_moves(&items) {
            self.check_emitted_fans(topology, &segments)?;
            self.check_global_faces(partition, &items, &segments)?;
        }
        Ok(segments)
    }

    // Incidence and cyclic order fix each connected embedding up to its exterior.
    // Fixing that face and the hosts of disconnected components fixes the global faces.
    #[expect(
        clippy::float_cmp,
        reason = "exact rounded coordinates are ordered by their stored binary64 values to keep emitted topology deterministic"
    )]
    fn check_global_faces(
        &self,
        partition: &ExactPartition<'_>,
        items: &[[NodeId; 2]],
        segments: &[Segment],
    ) -> Result<()> {
        let mut sides = Vec::new();
        try_reserve_vec(&mut sides, items.len())?;
        for chord in self.walks.iter().flatten().chain(&self.lines) {
            try_push_vec(&mut sides, chord.sides)?;
        }
        for &(_, host) in &self.points {
            try_push_vec(&mut sides, [host, host])?;
        }
        debug_assert_eq!(sides.len(), items.len());
        debug_assert_eq!(segments.len(), items.len());
        let node_count = partition.node_count();
        // Every node at its emitted coordinate, as stored doubles.
        let mut coordinates = Vec::new();
        try_reserve_vec(&mut coordinates, node_count)?;
        coordinates.extend((0..node_count as u32).map(|node| partition.node_xy(NodeId(node))));
        let mut incident_starts = Vec::new();
        try_reserve_vec(&mut incident_starts, node_count + 1)?;
        incident_starts.resize(node_count + 1, 0_u32);
        let mut parents = Vec::new();
        try_reserve_vec(&mut parents, node_count)?;
        parents.extend(0..node_count as u32);
        for (&[start, end], segment) in items.iter().zip(segments) {
            coordinates[start.0 as usize] = segment.start;
            coordinates[end.0 as usize] = segment.end;
            incident_starts[start.0 as usize + 1] += 1;
            if end != start {
                incident_starts[end.0 as usize + 1] += 1;
            }
            let mut left = start.0;
            let mut right = end.0;
            while parents[left as usize] != left {
                parents[left as usize] = parents[parents[left as usize] as usize];
                left = parents[left as usize];
            }
            while parents[right as usize] != right {
                parents[right as usize] = parents[parents[right as usize] as usize];
                right = parents[right as usize];
            }
            parents[left.max(right) as usize] = left.min(right);
        }
        for node in 0..node_count {
            incident_starts[node + 1] += incident_starts[node];
        }
        let mut incident = Vec::new();
        try_reserve_vec(&mut incident, incident_starts[node_count] as usize)?;
        incident.resize(incident_starts[node_count] as usize, 0_u32);
        {
            let mut cursor = Vec::new();
            try_reserve_vec(&mut cursor, node_count)?;
            cursor.extend_from_slice(&incident_starts[..node_count]);
            for (index, &[start, end]) in items.iter().enumerate() {
                for node in [start, end].into_iter().take(1 + usize::from(end != start)) {
                    incident[cursor[node.0 as usize] as usize] = index as u32;
                    cursor[node.0 as usize] += 1;
                }
            }
        }
        // Each component's root is its lexicographically least emitted node.
        let mut roots: Vec<Option<(usize, XY)>> = Vec::new();
        try_reserve_vec(&mut roots, node_count)?;
        roots.resize(node_count, None);
        for node in 0..node_count {
            if incident_starts[node] == incident_starts[node + 1] {
                continue;
            }
            let mut root = node as u32;
            while parents[root as usize] != root {
                root = parents[root as usize];
            }
            let point = coordinates[node];
            let entry = &mut roots[root as usize];
            if entry.is_none_or(|(_, prior): (usize, XY)| {
                point.x < prior.x || (point.x == prior.x && point.y < prior.y)
            }) {
                *entry = Some((node, point));
            }
        }
        // The face each component's own plan puts around it: its root's
        // exterior wedge, or the host of an isolated point.
        let mut queries = Vec::new();
        let mut exteriors = Vec::new();
        for &(node, point) in roots.iter().flatten() {
            let mut exterior = None;
            let mut chosen = None;
            let mut has_upper = false;
            let range = incident_starts[node] as usize..incident_starts[node + 1] as usize;
            for &index in &incident[range] {
                let index = index as usize;
                let [start, end] = items[index];
                let (target, target_point, face) = if start.0 as usize == node {
                    (end.0, segments[index].end, sides[index][0])
                } else {
                    (start.0, segments[index].start, sides[index][1])
                };
                if target as usize == node {
                    exterior = Some(face);
                    continue;
                }
                let upper = target_point.y >= point.y;
                if chosen.is_none()
                    || (upper && !has_upper)
                    || (upper == has_upper && {
                        let (prior_target, prior_point) = chosen
                            .expect("a selected departure exists before comparing the next one");
                        compare_departures(point, prior_point, prior_target, target_point, target)
                            == Ordering::Less
                    })
                {
                    chosen = Some((target, target_point));
                    exterior = Some(face);
                    has_upper = upper;
                }
            }
            try_push_vec(&mut queries, node as u32)?;
            try_push_vec(
                &mut exteriors,
                exterior.expect("a component root has an incident emitted edge"),
            )?;
        }
        // The emitted embedding keeps the exact incidence and fans, so the
        // face just east of the first chord a leftward ray from a root meets
        // is the face holding that component.
        let hits = shoot_left(
            &EmittedPoints(&coordinates),
            items.len(),
            |index| [items[index][0].0, items[index][1].0],
            &queries,
        )?;
        for ((&node, &exterior), hit) in queries.iter().zip(&exteriors).zip(hits) {
            let host = hit.map_or(0, |chord| {
                let chord = chord as usize;
                let segment = segments[chord];
                if segment.start.y < segment.end.y {
                    sides[chord][1]
                } else {
                    sides[chord][0]
                }
            });
            if host != exterior {
                return Err(GeometryErrorKind::ExactGlobalFace { node }.into());
            }
        }
        Ok(())
    }

    fn incident_items(&self, topology: &IndexedTopology) -> Result<Vec<[NodeId; 2]>> {
        let mut items = Vec::new();
        for chord in self.walks.iter().flatten().chain(&self.lines) {
            try_reserve_vec(&mut items, 1)?;
            let first = self.source_darts[chord.sources.start] as usize;
            let last = self.source_darts[chord.sources.end - 1] as usize;
            try_push_vec(&mut items, [
                NodeId(topology.owners[first]),
                NodeId(topology.targets[last]),
            ])?;
        }
        try_reserve_vec(&mut items, self.points.len())?;
        for &(node, _) in &self.points {
            try_push_vec(&mut items, [node, node])?;
        }
        Ok(items)
    }
}

/// Every bounded face of the arrangement as its own polygon, in face order.
/// One plan that emits every span as its own chord certifies the rounded
/// arrangement once; each face's rings then read those certified node
/// coordinates instead of re-rounding the edges it shares with neighbors.
/// Dangling edges inside a face bound nothing and are neither certified nor
/// emitted.
fn polygonize_faces(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
) -> Result<Vec<Polygon>> {
    let face_of = |slot: usize| global.walk_faces[topology.face_of[slot] as usize];
    let flags = |len: usize| -> Result<Vec<bool>> {
        let mut flags = Vec::new();
        try_reserve_vec(&mut flags, len)?;
        flags.resize(len, false);
        Ok(flags)
    };
    let mut spans = flags(partition.spans.len())?;
    for (slot, &span) in edges.iter().enumerate() {
        spans[span as usize] |= face_of(slot) != face_of(topology.twins[slot] as usize);
    }
    let lines = ArealCells {
        faces: flags(global.count as usize)?,
        spans,
        nodes: flags(partition.node_count())?,
    };
    lines
        .emission_plan(partition, topology, edges, global)?
        .round_items(partition, topology)?;
    let walks = global.boundary_walks(topology, |face, other| face != 0 && face != other)?;
    let mut shells = Vec::new();
    try_reserve_vec(&mut shells, global.count as usize)?;
    shells.resize(global.count as usize, None);
    let mut polygons: Vec<(usize, Ring, Vec<Ring>)> = Vec::new();
    let mut holes = Vec::new();
    for walk in &walks {
        let face = face_of(walk[0] as usize) as usize;
        let ring = closed_ring(
            walk.iter()
                .map(|&slot| partition.node_xy(NodeId(topology.owners[slot as usize]))),
        )?;
        if walk_is_shell(
            partition,
            topology,
            walk.len(),
            |step| walk[step],
            |step| walk[step],
        )? {
            debug_assert!(shells[face].is_none());
            shells[face] = Some(polygons.len());
            try_reserve_vec(&mut polygons, 1)?;
            try_push_vec(&mut polygons, (face, ring, Vec::new()))?;
        } else {
            try_reserve_vec(&mut holes, 1)?;
            try_push_vec(&mut holes, (face, ring))?;
        }
    }
    for (face, hole) in holes {
        let polygon = shells[face].expect("bounded face has a shell");
        try_reserve_vec(&mut polygons[polygon].2, 1)?;
        try_push_vec(&mut polygons[polygon].2, hole)?;
    }
    polygons.sort_unstable_by_key(|&(face, ..)| face);
    let mut output = Vec::new();
    try_reserve_vec(&mut output, polygons.len())?;
    for (_, shell, holes) in polygons {
        try_push_vec(&mut output, Polygon::new(shell, holes)?)?;
    }
    Ok(output)
}

/// Whether a closed boundary walk is the outer cycle of the face on its left,
/// judged at its exact lexicographically least vertex. `first(i)` and
/// `last(i)` are the first and last darts of the walk's `i`-th step.
fn walk_is_shell(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    steps: usize,
    first: impl Fn(usize) -> u32,
    last: impl Fn(usize) -> u32,
) -> Result<bool> {
    let mut minimum = 0;
    for candidate in 1..steps {
        if partition.compare_nodes(
            NodeId(topology.owners[first(candidate) as usize]),
            NodeId(topology.owners[first(minimum) as usize]),
        )? == Ordering::Less
        {
            minimum = candidate;
        }
    }
    let outgoing = first(minimum);
    let prior = if minimum == 0 { steps - 1 } else { minimum - 1 };
    let incoming = topology.twins[last(prior) as usize];
    let root = NodeId(topology.owners[outgoing as usize]);
    let upper = |slot: u32| -> Result<bool> {
        Ok(
            partition.compare_axis(NodeId(topology.targets[slot as usize]), root, Axis::Y)?
                != Ordering::Less,
        )
    };
    let outgoing_upper = upper(outgoing)?;
    let incoming_upper = upper(incoming)?;
    // At the exact leftmost vertex both rays point right. The fan's angular
    // cut is +x, so a lower outgoing ray precedes an upper one.
    Ok(if outgoing_upper == incoming_upper {
        outgoing < incoming
    } else {
        !outgoing_upper
    })
}

fn canonicalize_closed_nodes(partition: &ExactPartition<'_>, nodes: &mut [NodeId]) -> Result<()> {
    let open = nodes.len().saturating_sub(1);
    if open == 0 {
        return Ok(());
    }
    let mut start = 0;
    for index in 1..open {
        if partition.compare_nodes(nodes[index], nodes[start])? == Ordering::Less {
            start = index;
        }
    }
    nodes[..open].rotate_left(start);
    nodes[open] = nodes[0];
    Ok(())
}

/// A closed ring through `vertices`, rotated to start at its least vertex.
fn closed_ring(vertices: impl ExactSizeIterator<Item = XY> + Clone) -> Result<Ring> {
    let count = vertices
        .len()
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    try_reserve_vec(&mut xs, count)?;
    try_reserve_vec(&mut ys, count)?;
    let first = vertices.clone().next().expect("a ring has vertices");
    for vertex in vertices.chain([first]) {
        xs.push(vertex.x);
        ys.push(vertex.y);
    }
    canonicalize_closed_xy(&mut xs, &mut ys);
    Ok(Ring::from_closed(CoordSeq::try_from_vecs(
        xs, ys, None, None,
    )?))
}

fn canonicalize_closed_xy(xs: &mut [f64], ys: &mut [f64]) {
    let open = xs.len().saturating_sub(1);
    if open == 0 {
        return;
    }
    let start = (0..open)
        .min_by(|&left, &right| {
            xs[left]
                .total_cmp(&xs[right])
                .then_with(|| ys[left].total_cmp(&ys[right]))
        })
        .unwrap_or(0);
    xs[..open].rotate_left(start);
    ys[..open].rotate_left(start);
    xs[open] = xs[0];
    ys[open] = ys[0];
}

fn emitted_pair_preserves_incidence(left: [(NodeId, XY); 2], right: [(NodeId, XY); 2]) -> bool {
    if left[0].0 != left[1].0
        && left
            .iter()
            .all(|&(node, _)| right.iter().any(|&(other, _)| node == other))
    {
        return false;
    }
    for (endpoints, other) in [(left, right), (right, left)] {
        for (node, point) in endpoints {
            if point_on_segment(point, other[0].1, other[1].1)
                && !other.iter().any(|&(other_node, _)| node == other_node)
            {
                return false;
            }
        }
    }
    let signs = |endpoints: [(NodeId, XY); 2], other: [(NodeId, XY); 2]| {
        other.map(|(_, point)| {
            stored_orientation_sign(
                endpoints[0].1.x,
                endpoints[0].1.y,
                endpoints[1].1.x,
                endpoints[1].1.y,
                point.x,
                point.y,
            )
        })
    };
    let straddles = |pair: [ExactSign; 2]| {
        pair[0] != ExactSign::Zero && pair[1] != ExactSign::Zero && pair[0] != pair[1]
    };
    !(straddles(signs(left, right)) && straddles(signs(right, left)))
}

impl GlobalFaces {
    #[cfg(test)]
    fn boundary_plan(
        &self,
        lines: &[u32],
        topology: &IndexedTopology,
        edges: &[u32],
        partition: &ExactPartition<'_>,
        occupied: &[bool],
    ) -> Result<BoundaryPlan> {
        self.boundary_plan_with_protected(lines, topology, edges, partition, occupied, None)
    }

    fn boundary_plan_with_protected(
        &self,
        lines: &[u32],
        topology: &IndexedTopology,
        edges: &[u32],
        partition: &ExactPartition<'_>,
        occupied: &[bool],
        extra_protected: Option<&[bool]>,
    ) -> Result<BoundaryPlan> {
        if let Some(extra_protected) = extra_protected
            && extra_protected.len() != partition.node_count()
        {
            return Err(GeometryErrorKind::Invalid(
                "exact overlay protection does not match node count".into(),
            )
            .into());
        }
        let mut walks =
            self.boundary_walks(topology, |face, other| occupied[face] && !occupied[other])?;
        let mut retained = Vec::new();
        try_reserve_vec(&mut retained, partition.spans.len())?;
        retained.resize(partition.spans.len(), false);
        for &span in lines {
            retained[span as usize] = true;
        }
        for walk in &walks {
            for &slot in walk {
                retained[edges[slot as usize] as usize] = true;
            }
        }
        let face_quotient = self.quotient(topology, edges, &retained)?;
        let mut protected = Vec::new();
        try_reserve_vec(&mut protected, partition.node_count())?;
        protected.resize(partition.node_count(), false);
        match &partition.kept_nodes {
            KeptNodes::Selected => {},
            KeptNodes::All => protected.fill(true),
            KeptNodes::OffRings(on_ring) => {
                for (protect, &on_ring) in protected.iter_mut().zip(on_ring) {
                    *protect = !on_ring;
                }
            },
        }
        if let Some(extra_protected) = extra_protected {
            for (protected, &extra) in protected.iter_mut().zip(extra_protected) {
                *protected |= extra;
            }
        }
        for &node in &partition.point_nodes {
            protected[node.0 as usize] = true;
        }
        for &(node, _) in &partition.point_sources {
            protected[node.0 as usize] = true;
        }
        let sides = |slot: u32| {
            [slot, topology.twins[slot as usize]].map(|dart| {
                face_quotient[self.walk_faces[topology.face_of[dart as usize] as usize] as usize]
            })
        };
        let can_join = |left: u32, right: u32| -> Result<bool> {
            let node = topology.owners[right as usize] as usize;
            if protected[node] || sides(left) != sides(right) {
                return Ok(false);
            }
            let row = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            if row.filter(|&slot| retained[edges[slot] as usize]).count() != 2 {
                return Ok(false);
            }
            let source = |slot: u32| {
                partition.unique[partition.spans[edges[slot as usize] as usize].unique as usize]
            };
            let a = source(left);
            let b = source(right);
            let difference =
                |end, start| ExactDyadic::from_f64(end).try_subtract(ExactDyadic::from_f64(start));
            let ax = difference(a.end.x, a.start.x)?;
            let ay = difference(a.end.y, a.start.y)?;
            let bx = difference(b.end.x, b.start.x)?;
            let by = difference(b.end.y, b.start.y)?;
            Ok(ax
                .try_product(&by)?
                .try_subtract(ay.try_product(&bx)?)?
                .is_zero())
        };
        let mut span_slots = Vec::new();
        try_reserve_vec(&mut span_slots, partition.spans.len())?;
        span_slots.resize(partition.spans.len(), 0);
        for (slot, &span) in edges.iter().enumerate() {
            if topology.owners[slot] == partition.spans[span as usize].start.0 {
                span_slots[span as usize] = slot;
            }
        }
        let mut source_darts = Vec::new();
        let mut make_chord = |slot: u32| -> Result<PlannedChord> {
            try_reserve_vec(&mut source_darts, 1)?;
            let start = source_darts.len();
            try_push_vec(&mut source_darts, slot)?;
            Ok(PlannedChord {
                sources: start..start + 1,
                sides: sides(slot),
            })
        };
        let mut planned_walks = Vec::new();
        try_reserve_vec(&mut planned_walks, walks.len())?;
        for walk in &mut walks {
            // Start at a retained corner so a joined source range never wraps.
            for index in 0..walk.len() {
                let prior = if index == 0 {
                    walk.len() - 1
                } else {
                    index - 1
                };
                if !can_join(walk[prior], walk[index])? {
                    walk.rotate_left(index);
                    break;
                }
            }
            let mut chords = Vec::new();
            try_reserve_vec(&mut chords, walk.len())?;
            for (index, &slot) in walk.iter().enumerate() {
                let chord = make_chord(slot)?;
                if index > 0 && can_join(walk[index - 1], slot)? {
                    let previous: &mut PlannedChord = chords
                        .last_mut()
                        .expect("a joined chord has a preceding planned chord");
                    previous.sources.end = chord.sources.end;
                } else {
                    try_push_vec(&mut chords, chord)?;
                }
            }
            try_push_vec(&mut planned_walks, chords)?;
        }
        let mut planned_lines = Vec::new();
        try_reserve_vec(&mut planned_lines, lines.len())?;
        let mut line_remaining = Vec::new();
        try_reserve_vec(&mut line_remaining, partition.spans.len())?;
        line_remaining.resize(partition.spans.len(), false);
        for &span in lines {
            line_remaining[span as usize] = true;
        }
        let continuation = |slot: u32, remaining: &[bool]| -> Result<Option<u32>> {
            let node = topology.targets[slot as usize] as usize;
            let twin = topology.twins[slot as usize];
            for next in topology.starts[node]..topology.starts[node + 1] {
                if next != twin && remaining[edges[next as usize] as usize] && can_join(slot, next)?
                {
                    return Ok(Some(next));
                }
            }
            Ok(None)
        };
        for &span in lines {
            if !line_remaining[span as usize] {
                continue;
            }
            let mut backwards = topology.twins[span_slots[span as usize]];
            while let Some(next) = continuation(backwards, &line_remaining)? {
                backwards = next;
            }
            let mut slot = topology.twins[backwards as usize];
            let mut chord = make_chord(slot)?;
            loop {
                line_remaining[edges[slot as usize] as usize] = false;
                let Some(next) = continuation(slot, &line_remaining)? else {
                    break;
                };
                chord.sources.end = make_chord(next)?.sources.end;
                slot = next;
            }
            try_push_vec(&mut planned_lines, chord)?;
        }
        Ok(BoundaryPlan {
            source_darts,
            walks: planned_walks,
            lines: planned_lines,
            points: Vec::new(),
            face_quotient,
        })
    }

    /// Closed boundary walks over the darts whose global face `bounds` its
    /// twin's face: `bounds(face, twin_face)`. Each walk stays within one
    /// face, since the next dart clockwise from an incoming twin bounds the
    /// same face on its left.
    fn boundary_walks(
        &self,
        topology: &IndexedTopology,
        bounds: impl Fn(usize, usize) -> bool,
    ) -> Result<Vec<Vec<u32>>> {
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, topology.targets.len())?;
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let other = topology.face_of[topology.twins[slot] as usize];
            try_push_vec(
                &mut selected,
                bounds(
                    self.walk_faces[walk as usize] as usize,
                    self.walk_faces[other as usize] as usize,
                ),
            )?;
        }
        let mut visited = Vec::new();
        try_reserve_vec(&mut visited, selected.len())?;
        visited.resize(selected.len(), false);
        let mut positions = Vec::new();
        try_reserve_vec(&mut positions, topology.starts.len() - 1)?;
        positions.resize(topology.starts.len() - 1, None);
        let mut walks = Vec::new();
        for start in 0..selected.len() {
            if !selected[start] || visited[start] {
                continue;
            }
            let mut walk = Vec::new();
            let mut slot = start;
            loop {
                debug_assert!(!visited[slot]);
                visited[slot] = true;
                try_reserve_vec(&mut walk, 1)?;
                positions[topology.owners[slot] as usize] = Some(walk.len());
                try_push_vec(&mut walk, slot as u32)?;
                let target = topology.targets[slot] as usize;
                // A tangent hole can share a boundary walk with its shell.
                // Cut closed cycles by exact identity before any rounding.
                if let Some(begin) = positions[target] {
                    let mut cycle = Vec::new();
                    try_reserve_vec(&mut cycle, walk.len() - begin)?;
                    for dart in walk.drain(begin..) {
                        positions[topology.owners[dart as usize] as usize] = None;
                        try_push_vec(&mut cycle, dart)?;
                    }
                    try_reserve_vec(&mut walks, 1)?;
                    try_push_vec(&mut walks, cycle)?;
                }
                let first = topology.starts[target] as usize;
                let end = topology.starts[target + 1] as usize;
                let twin = topology.twins[slot] as usize;
                // Skip omitted wedges clockwise from the incoming twin.
                slot = twin;
                loop {
                    slot = if slot == first { end - 1 } else { slot - 1 };
                    if selected[slot] {
                        break;
                    }
                    debug_assert_ne!(slot, twin, "selected boundary has no successor");
                }
                if slot == start {
                    break;
                }
            }
            debug_assert!(walk.is_empty());
        }
        Ok(walks)
    }

    fn quotient(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        retained: &[bool],
    ) -> Result<Vec<u32>> {
        let mut parents = Vec::new();
        try_reserve_vec(&mut parents, self.count as usize)?;
        parents.extend(0..self.count);
        for (slot, &edge) in edges.iter().enumerate() {
            let twin = topology.twins[slot] as usize;
            if slot > twin || retained[edge as usize] {
                continue;
            }
            let mut left = self.walk_faces[topology.face_of[slot] as usize];
            let mut right = self.walk_faces[topology.face_of[twin] as usize];
            while parents[left as usize] != left {
                parents[left as usize] = parents[parents[left as usize] as usize];
                left = parents[left as usize];
            }
            while parents[right as usize] != right {
                parents[right as usize] = parents[parents[right as usize] as usize];
                right = parents[right as usize];
            }
            // Keep infinity's identity when its face absorbs a bounded face.
            parents[left.max(right) as usize] = left.min(right);
        }
        for face in 0..parents.len() {
            parents[face] = parents[parents[face] as usize];
        }
        Ok(parents)
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

    pub(in crate::geometry) fn partition(
        &self,
        budget: &mut ExpansionBudget,
    ) -> Result<ExactPartition<'_>> {
        #[cfg(test)]
        PARTITION_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
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
        for (node, range) in partition.binding_starts.windows(2).enumerate() {
            for &unique in &partition.bindings[range[0] as usize..range[1] as usize] {
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
                    Ring::from_closed(CoordSeq::try_from_points(&coordinates)?),
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

    /// Visit each global face with the rows whose exact areal supports occupy
    /// that face. The active row list is updated from sparse ring crossings,
    /// avoiding a face-by-row membership matrix for disjoint coverage.
    fn visit_coverage_face_rows(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        row_count: usize,
        mut visit: impl FnMut(u32, &[usize]) -> Result<()>,
    ) -> Result<()> {
        let winding_rings = self.winding_source_rings()?;
        let mut state = CoverageFaceState::new(self.polygon_count, row_count)?;
        partition.visit_face_windings_with_hook(
            topology,
            edges,
            global,
            &winding_rings,
            self.ring_count,
            &mut state,
            |winding, ring, direction, state| state.update_ring(self, winding, ring, direction),
            |face, _, state| visit(face, &state.active_rows),
        )
    }

    /// Label every bounded global face and publish each coverage row from that
    /// one exact partition.
    ///
    /// Face membership and gap ownership are decided from stored-ring
    /// windings and exact span provenance.  Boundary plans are then built one
    /// row at a time, but all of them consume the same rounded node map and
    /// the same resolved ordinate map.  No emitted coordinate participates in
    /// selection, and no row is sent through a second partition.
    pub(in crate::geometry) fn coverage_clean_rows(
        &self,
        partition: &mut ExactPartition<'_>,
        row_areas: &[f64],
        gap_width: f64,
        overlaps: CoverageOverlapRule,
        declared_axes: CoordinateAxes,
    ) -> Result<Vec<Shape>> {
        let row_count = self.operand_count();
        if row_areas.len() != row_count {
            return Err(GeometryErrorKind::Invalid(
                "coverage row areas are out of sync with exact operands".into(),
            )
            .into());
        }
        let row_count_u32 = u32::try_from(row_count).map_err(|_| ResourceError::SizeOverflow)?;
        // Cleaning historically kept every source node while extracting
        // minimal faces. Keep that identity and provenance contract in the
        // direct partition publisher rather than relying on emitted geometry
        // to preserve a source vertex.
        partition.kept_nodes = KeptNodes::All;

        #[cfg(test)]
        let topology_faces_phase = crate::test_support::allocation_phase(
            crate::test_support::AllocationPhase::TopologyFaces,
        );
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.topology_halfedges = topology.face_of.len();
            metrics.global_faces = global.count as usize;
        });
        let (face_starts, face_slots) = self.global_face_slots(&topology, &edges, &global)?;
        let mut face_owners = Vec::new();
        try_reserve_vec(&mut face_owners, global.count as usize)?;
        face_owners.resize(global.count as usize, u32::MAX);
        let mut face_seen = Vec::new();
        try_reserve_vec(&mut face_seen, global.count as usize)?;
        face_seen.resize(global.count as usize, false);

        let mut candidates = Vec::new();
        try_reserve_vec(&mut candidates, row_count)?;
        let mut border_lengths = Vec::new();
        try_reserve_vec(&mut border_lengths, row_count)?;
        border_lengths.resize(row_count, 0.0);
        let mut seen_rows = Vec::new();

        self.visit_coverage_face_rows(
            partition,
            &topology,
            &edges,
            &global,
            row_count,
            |face, active_rows| {
                let face_index = face as usize;
                let seen = face_seen.get_mut(face_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact coverage winding visited an unknown global face".into(),
                    )
                })?;
                *seen = true;
                if face == 0 {
                    return Ok(());
                }
                candidates.clear();
                for &row in active_rows {
                    try_push_vec(&mut candidates, row)?;
                }
                candidates.sort_unstable();
                let needs_stats = candidates.is_empty() && gap_width > 0.0
                    || candidates.len() > 1
                        && matches!(overlaps, CoverageOverlapRule::LongestBorder);
                let (area, perimeter) = if needs_stats {
                    let measure_area = candidates.is_empty();
                    self.coverage_face_stats(
                        partition,
                        &topology,
                        &edges,
                        &face_starts,
                        &face_slots,
                        face as usize,
                        &mut border_lengths,
                        &mut seen_rows,
                        measure_area,
                    )?
                } else {
                    (0.0, 0.0)
                };
                let owner = match candidates.as_slice() {
                    [] if gap_width > 0.0
                        && perimeter > 0.0
                        && 2.0 * area / perimeter < gap_width =>
                    {
                        (0..row_count)
                            .filter_map(|row| {
                                let border = border_lengths[row];
                                (border > 0.0).then_some((row, border))
                            })
                            // The ascending scan and ordinary `max_by` tie
                            // behavior retain the historical highest row on
                            // equal gap borders.
                            .max_by(|left, right| left.1.total_cmp(&right.1))
                            .map(|(row, _)| row)
                    },
                    [] => None,
                    [only] => Some(*only),
                    _ => Some(match overlaps {
                        CoverageOverlapRule::MinIndex => candidates[0],
                        CoverageOverlapRule::MaxArea => candidates
                            .iter()
                            .copied()
                            .max_by(|left, right| row_areas[*left].total_cmp(&row_areas[*right]))
                            .expect("non-empty face candidates"),
                        CoverageOverlapRule::MinArea => candidates
                            .iter()
                            .copied()
                            .min_by(|left, right| row_areas[*left].total_cmp(&row_areas[*right]))
                            .expect("non-empty face candidates"),
                        CoverageOverlapRule::LongestBorder => candidates
                            .iter()
                            .copied()
                            .map(|row| (row, border_lengths[row]))
                            // Reversed row comparison retains the historical
                            // lowest row on equal overlap borders.
                            .max_by(|left, right| {
                                left.1.total_cmp(&right.1).then(right.0.cmp(&left.0))
                            })
                            .map(|(row, _)| row)
                            .expect("non-empty face candidates"),
                    }),
                };
                if let Some(owner) = owner {
                    face_owners[face as usize] =
                        u32::try_from(owner).map_err(|_| ResourceError::SizeOverflow)?;
                }
                Ok(())
            },
        )?;
        #[cfg(test)]
        drop(topology_faces_phase);

        let output_nodes = self.certify_coverage_partition(
            partition,
            &face_owners,
            &face_seen,
            row_count,
            &topology,
            &edges,
            &global,
        )?;
        #[cfg(test)]
        let _publication_phase = crate::test_support::allocation_phase(
            crate::test_support::AllocationPhase::Publication,
        );
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.publication_nodes = output_nodes.len();
        });
        ExactPartition::check_node_injectivity(
            &output_nodes,
            &partition.round_nodes(&output_nodes)?,
        )?;
        let by_node = self.resolve_node_points(
            partition,
            &output_nodes,
            |node| Ok(partition.node_xy(node)),
            declared_axes,
        )?;

        let mut outputs = Vec::new();
        try_reserve_vec(&mut outputs, row_count)?;
        for row in 0..row_count_u32 {
            let mut cells = ArealCells {
                faces: Vec::new(),
                spans: Vec::new(),
                nodes: Vec::new(),
            };
            try_reserve_vec(&mut cells.faces, face_owners.len())?;
            for &owner in &face_owners {
                try_push_vec(&mut cells.faces, owner == row)?;
            }
            try_reserve_vec(&mut cells.spans, partition.spans.len())?;
            cells.spans.resize(partition.spans.len(), false);
            try_reserve_vec(&mut cells.nodes, partition.node_count())?;
            cells.nodes.resize(partition.node_count(), false);
            let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
            let parts = plan.emit_node_parts_with_shared_rounding(partition, &topology)?;
            try_push_vec(
                &mut outputs,
                self.coverage_shape_from_node_parts(parts, &by_node, declared_axes)?,
            )?;
        }
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.published_rows = outputs.len();
        });
        Ok(outputs)
    }

    fn certify_coverage_partition(
        &self,
        partition: &ExactPartition<'_>,
        face_owners: &[u32],
        face_seen: &[bool],
        row_count: usize,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<Vec<NodeId>> {
        if face_owners.len() != global.count as usize
            || face_owners.first().copied() != Some(u32::MAX)
            || face_seen.len() != global.count as usize
            || face_seen.iter().any(|&seen| !seen)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact coverage face labels are out of sync with global faces".into(),
            )
            .into());
        }
        for &owner in &face_owners[1..] {
            if owner != u32::MAX && owner as usize >= row_count {
                return Err(GeometryErrorKind::Invalid(
                    "exact coverage face owner is out of range".into(),
                )
                .into());
            }
        }
        let mut boundary_spans = 0_usize;
        for (slot, _) in edges.iter().enumerate() {
            let twin = topology.twins[slot] as usize;
            if slot > twin {
                continue;
            }
            let left = face_owners[global.walk_faces[topology.face_of[slot] as usize] as usize];
            let right = face_owners[global.walk_faces[topology.face_of[twin] as usize] as usize];
            if left != right && (left != u32::MAX || right != u32::MAX) {
                boundary_spans = boundary_spans
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
        }
        let mut output_nodes = Vec::new();
        let node_capacity = boundary_spans
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut output_nodes, node_capacity)?;
        for (slot, &edge) in edges.iter().enumerate() {
            let twin = topology.twins[slot] as usize;
            if slot > twin {
                continue;
            }
            let left = face_owners[global.walk_faces[topology.face_of[slot] as usize] as usize];
            let right = face_owners[global.walk_faces[topology.face_of[twin] as usize] as usize];
            if left == right || (left == u32::MAX && right == u32::MAX) {
                continue;
            }
            let span = partition.spans[edge as usize];
            try_push_vec(&mut output_nodes, span.start)?;
            try_push_vec(&mut output_nodes, span.end)?;
        }
        output_nodes.sort_unstable_by_key(|node| node.0);
        output_nodes.dedup_by_key(|node| node.0);
        Ok(output_nodes)
    }

    fn coverage_face_stats(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        face_starts: &[u32],
        face_slots: &[u32],
        face: usize,
        border_lengths: &mut [f64],
        seen_rows: &mut Vec<u32>,
        measure_area: bool,
    ) -> Result<(f64, f64)> {
        border_lengths.fill(0.0);
        let start = face_starts[face] as usize;
        let end = face_starts[face + 1] as usize;
        let mut area = measure_area.then(ExactRationalArea::new);
        let mut perimeter = 0.0;
        for &slot in &face_slots[start..end] {
            let slot = slot as usize;
            let span_index = edges[slot] as usize;
            let span = partition.spans[span_index];
            let start_xy = partition.node_xy(span.start);
            let end_xy = partition.node_xy(span.end);
            let length = point_distance(start_xy, end_xy);
            perimeter += length;
            if let Some(area) = &mut area {
                area.add_homogeneous_cross(
                    &*partition.exact(NodeId(topology.owners[slot]))?,
                    &*partition.exact(NodeId(topology.targets[slot]))?,
                )?;
            }
            seen_rows.clear();
            for &(source, _) in partition.owners(span_index) {
                let operand = *self.source_operands.get(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                if seen_rows.contains(&operand) {
                    continue;
                }
                try_push_vec(seen_rows, operand)?;
                let operand = operand as usize;
                let border = border_lengths.get_mut(operand).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact coverage provenance operand is out of range".into(),
                    )
                })?;
                *border += length;
            }
        }
        let area = match area {
            Some(area) => area.round_abs_half()?,
            None => 0.0,
        };
        Ok((area, perimeter))
    }

    fn coverage_shape_from_node_parts(
        &self,
        parts: NodeParts,
        by_node: &HashMap<NodeId, Point>,
        declared_axes: CoordinateAxes,
    ) -> Result<Shape> {
        debug_assert!(parts.points.is_empty());
        debug_assert!(parts.lines.is_empty());
        let node_count = parts
            .polygons
            .iter()
            .flat_map(|polygon| polygon.iter())
            .map(Vec::len)
            .try_fold(0_usize, |count, ring| {
                count.checked_add(ring).ok_or(ResourceError::SizeOverflow)
            })?;
        let mut nodes = Vec::new();
        try_reserve_vec(&mut nodes, node_count)?;
        for polygon in &parts.polygons {
            for ring in polygon {
                for &node in ring {
                    try_push_vec(&mut nodes, node)?;
                }
            }
        }
        let axes = bucket_axes(&nodes, by_node, declared_axes, false, "coverage_clean")?;
        let mut polygons = Vec::new();
        try_reserve_vec(&mut polygons, parts.polygons.len())?;
        for polygon in parts.polygons {
            let mut rings = Vec::new();
            try_reserve_vec(&mut rings, polygon.len())?;
            for ring in polygon {
                let mut points = Vec::new();
                try_reserve_vec(&mut points, ring.len())?;
                for node in ring {
                    let point = by_node.get(&node).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "exact coverage output lost a rounded node".into(),
                        )
                    })?;
                    try_push_vec(&mut points, with_axes(*point, axes))?;
                }
                try_push_vec(
                    &mut rings,
                    Ring::from_closed(CoordSeq::try_from_points(&points)?),
                )?;
            }
            let mut rings = rings.into_iter();
            let shell = rings.next().expect("coverage polygon has a shell");
            let mut holes = Vec::new();
            try_reserve_vec(&mut holes, rings.size_hint().0)?;
            for ring in rings {
                try_push_vec(&mut holes, ring)?;
            }
            try_push_vec(&mut polygons, Polygon::new(shell, holes)?)?;
        }
        Shape::narrow_polygons(axes, polygons)
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

    pub(in crate::geometry) fn relate(&self, partition: &ExactPartition<'_>) -> Result<De9im> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings()?;
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        let line_boundaries = self.node_source_flags(partition)?;
        let operands = self.operand_count();
        let mut matrix = De9im::empty_disjoint();
        for face in 0..global.count as usize {
            matrix.set_at_least(
                location(facts.face_membership[face * operands]),
                location(facts.face_membership[face * operands + 1]),
                Dimension::Surface,
            );
        }
        for span in 0..partition.spans.len() {
            let left_face = facts.span_faces[span][0] as usize;
            let right_face = facts.span_faces[span][1] as usize;
            let left =
                self.span_location_from_facts(0, span, left_face, right_face, &facts, operands);
            let right =
                self.span_location_from_facts(1, span, left_face, right_face, &facts, operands);
            if left != Loc::Exterior || right != Loc::Exterior {
                matrix.set_at_least(left, right, Dimension::Curve);
            }
        }
        for node in 0..partition.node_count() {
            let left = self.node_location_from_facts(
                0,
                NodeId(node as u32),
                &topology,
                &edges,
                &global,
                &facts,
                &line_boundaries,
                &facts.point_membership,
            );
            let right = self.node_location_from_facts(
                1,
                NodeId(node as u32),
                &topology,
                &edges,
                &global,
                &facts,
                &line_boundaries,
                &facts.point_membership,
            );
            if left != Loc::Exterior || right != Loc::Exterior {
                matrix.set_at_least(left, right, Dimension::Point);
            }
        }
        Ok(matrix)
    }

    fn build_cell_facts(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        winding_rings: &[Option<u32>],
        odd: bool,
    ) -> Result<CellFacts> {
        let operands = self.operand_count();
        let face_cells = (global.count as usize)
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let span_cells = partition
            .spans
            .len()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let raw_cells = span_cells
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        let face_slots = self.global_face_slots(topology, edges, global)?;
        let span_faces = self.span_faces(topology, edges, global)?;
        let mut facts = CellFacts {
            face_membership: Vec::new(),
            span_raw: Vec::new(),
            span_curve: Vec::new(),
            span_support: Vec::new(),
            span_faces,
            point_membership: Vec::new(),
        };
        try_reserve_vec(&mut facts.face_membership, face_cells)?;
        facts.face_membership.resize(face_cells, false);
        try_reserve_vec(&mut facts.span_raw, raw_cells)?;
        facts.span_raw.resize(raw_cells, false);
        try_reserve_vec(&mut facts.span_curve, span_cells)?;
        facts.span_curve.resize(span_cells, false);
        try_reserve_vec(&mut facts.span_support, span_cells)?;
        facts.span_support.resize(span_cells, false);
        let node_cells = partition
            .node_count()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut facts.point_membership, node_cells)?;
        facts.point_membership.resize(node_cells, false);
        let mut shell_support = Vec::new();
        let mut hole_support = Vec::new();
        try_reserve_vec(&mut shell_support, self.polygon_count)?;
        try_reserve_vec(&mut hole_support, self.polygon_count)?;
        shell_support.resize(self.polygon_count, false);
        hole_support.resize(self.polygon_count, false);
        let mut touched_polygons = Vec::new();
        try_reserve_vec(&mut touched_polygons, partition.spans.len())?;
        let mut point_sources_by_node = Vec::new();
        try_reserve_vec(&mut point_sources_by_node, partition.point_sources.len())?;
        point_sources_by_node.extend_from_slice(&partition.point_sources);
        point_sources_by_node.sort_unstable_by_key(|(node, _)| node.0);
        let mut point_polygon_index =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        point_polygon_index
            .try_reserve(partition.point_sources.len())
            .map_err(crate::error::Error::from)?;
        let mut point_polygon_states = Vec::new();
        try_reserve_vec(&mut point_polygon_states, partition.point_sources.len())?;
        let mut point_shell_faces = Vec::new();
        for &(node, source) in &partition.point_sources {
            let source_index = source as usize;
            let Some(ring) = self.source_rings[source_index] else {
                continue;
            };
            if self.ring_holes[ring as usize]
                || self.source_ring_kinds[source_index] != Some(RingKind::Collapsed)
            {
                continue;
            }
            let polygon = self.ring_polygons[ring as usize];
            let state = if let Some(&state) = point_polygon_index.get(&(node, polygon)) {
                state
            } else {
                let state = u32::try_from(point_polygon_states.len())
                    .map_err(|_| ResourceError::SizeOverflow)?;
                point_polygon_index.insert((node, polygon), state);
                try_reserve_vec(&mut point_polygon_states, 1)?;
                try_push_vec(&mut point_polygon_states, (node, polygon, false))?;
                state
            };
            let range = topology.starts[node.0 as usize] as usize
                ..topology.starts[node.0 as usize + 1] as usize;
            if range.is_empty() {
                let component = topology.component_of[node.0 as usize] as usize;
                try_push_vec(
                    &mut point_shell_faces,
                    (global.component_hosts[component], state),
                )?;
            } else {
                for slot in range {
                    try_push_vec(
                        &mut point_shell_faces,
                        (global.walk_faces[topology.face_of[slot] as usize], state),
                    )?;
                }
            }
        }
        point_shell_faces.sort_unstable_by_key(|(face, _)| *face);
        let mut point_shell_cursor = 0;
        let mut occupancy = PolygonOccupancy::new(&self.polygon_rings, self.ring_count, odd)?;
        partition.visit_face_windings_with_hook(
            topology,
            edges,
            global,
            winding_rings,
            self.ring_count,
            &mut occupancy,
            |winding, ring, direction, occupancy| {
                occupancy.cross(winding, ring, direction);
                Ok(())
            },
            |face, winding, occupancy| {
                let face = face as usize;
                for operand in 0..operands {
                    let occupied = occupancy.operand_occupied(operand);
                    debug_assert_eq!(
                        occupied,
                        self.polygon_rings[operand]
                            .iter()
                            .any(|rings| rings_occupied(winding, rings.clone(), odd)),
                        "tracked occupancy diverged from the ring windings"
                    );
                    facts.face_membership[face * operands + operand] = occupied;
                }
                while point_shell_cursor < point_shell_faces.len()
                    && point_shell_faces[point_shell_cursor].0 == face as u32
                {
                    let (_, state_index) = point_shell_faces[point_shell_cursor];
                    let state = &mut point_polygon_states[state_index as usize];
                    let node = state.0;
                    let polygon = state.1 as usize;
                    let hole = self.polygon_ring_ranges[polygon]
                        .clone()
                        .skip(1)
                        .any(|hole| {
                            if self.ring_kinds[hole] == RingKind::Winding {
                                ring_occupied(winding[hole], odd)
                            } else {
                                self.collapsed_ring_at_node(
                                    partition,
                                    topology,
                                    edges,
                                    &point_sources_by_node,
                                    node,
                                    hole,
                                )
                            }
                        });
                    state.2 |= hole;
                    point_shell_cursor += 1;
                }
                let start = face_slots.0[face] as usize;
                let end = face_slots.0[face + 1] as usize;
                for &slot in &face_slots.1[start..end] {
                    let slot = slot as usize;
                    let span = edges[slot] as usize;
                    let side = usize::from(facts.span_faces[span][1] as usize == face);
                    for &(source, _) in partition.owners(span) {
                        let source = source as usize;
                        let Some(ring) = self.source_rings[source] else {
                            continue;
                        };
                        let occupied = match self.source_ring_kinds[source] {
                            Some(RingKind::Winding) => ring_occupied(winding[ring as usize], odd),
                            Some(RingKind::Collapsed) => true,
                            None => false,
                        };
                        if !occupied {
                            continue;
                        }
                        let polygon = self.ring_polygons[ring as usize] as usize;
                        if !shell_support[polygon] && !hole_support[polygon] {
                            try_reserve_vec(&mut touched_polygons, 1)?;
                            try_push_vec(&mut touched_polygons, polygon as u32)?;
                        }
                        if self.ring_holes[ring as usize] {
                            hole_support[polygon] = true;
                        } else {
                            shell_support[polygon] = true;
                        }
                    }
                    for &polygon in &touched_polygons {
                        let polygon = polygon as usize;
                        if shell_support[polygon] && !hole_support[polygon] {
                            hole_support[polygon] = self.polygon_ring_ranges[polygon]
                                .clone()
                                .skip(1)
                                .any(|hole| {
                                    self.ring_kinds[hole] == RingKind::Winding
                                        && ring_occupied(winding[hole], odd)
                                });
                        }
                        if shell_support[polygon] && !hole_support[polygon] {
                            let operand = self.polygon_operands[polygon] as usize;
                            facts.span_raw[(span * 2 + side) * operands + operand] = true;
                        }
                        shell_support[polygon] = false;
                        hole_support[polygon] = false;
                    }
                    touched_polygons.clear();
                }
                Ok(())
            },
        )?;
        for (node, polygon, hole) in point_polygon_states {
            if !hole {
                let operand = self.polygon_operands[polygon as usize] as usize;
                facts.point_membership[node.0 as usize * operands + operand] = true;
            }
        }
        if partition.point_nodes.len() != self.isolated_operands.len() {
            return Err(GeometryErrorKind::Invalid(
                "exact partition point sources lost their occurrence mapping".into(),
            )
            .into());
        }
        for (index, &node) in partition.point_nodes.iter().enumerate() {
            let operand = self.isolated_operands[index] as usize;
            facts.point_membership[node.0 as usize * operands + operand] = true;
        }
        for &(node, source) in &partition.point_sources {
            let source = source as usize;
            if self.source_rings[source].is_none() {
                let operand = self.source_operands[source] as usize;
                facts.point_membership[node.0 as usize * operands + operand] = true;
            }
        }

        for span in 0..partition.spans.len() {
            let left = facts.span_faces[span][0] as usize;
            let right = facts.span_faces[span][1] as usize;
            for operand in 0..operands {
                let index = span * operands + operand;
                facts.span_curve[index] = self.explicit_curve_support(partition, span, operand);
                facts.span_support[index] = facts.span_curve[index]
                    || facts.span_raw[(span * 2) * operands + operand]
                    || facts.span_raw[(span * 2 + 1) * operands + operand]
                    || facts.face_membership[left * operands + operand]
                    || facts.face_membership[right * operands + operand];
            }
        }
        Ok(facts)
    }

    fn global_face_slots(
        &self,
        topology: &IndexedTopology,
        _edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<(Vec<u32>, Vec<u32>)> {
        let count = global.count as usize;
        let mut counts = Vec::new();
        try_reserve_vec(&mut counts, count)?;
        counts.resize(count, 0_u32);
        for &walk in &topology.face_of {
            let face = global.walk_faces[walk as usize] as usize;
            counts[face] = counts[face]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut starts: Vec<u32> = Vec::new();
        try_reserve_vec(
            &mut starts,
            count.checked_add(1).ok_or(ResourceError::SizeOverflow)?,
        )?;
        try_push_vec(&mut starts, 0)?;
        for count in counts {
            let next = starts
                .last()
                .copied()
                .unwrap_or(0_u32)
                .checked_add(count)
                .ok_or(ResourceError::SizeOverflow)?;
            try_push_vec(&mut starts, next)?;
        }
        let mut slots = Vec::new();
        try_reserve_vec(&mut slots, topology.face_of.len())?;
        slots.resize(topology.face_of.len(), 0);
        let mut cursor = Vec::new();
        try_reserve_vec(&mut cursor, count)?;
        cursor.extend_from_slice(&starts[..count]);
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let face = global.walk_faces[walk as usize] as usize;
            let index = cursor[face] as usize;
            slots[index] = u32::try_from(slot).map_err(|_| ResourceError::SizeOverflow)?;
            cursor[face] += 1;
        }
        Ok((starts, slots))
    }

    fn span_faces(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<Vec<[u32; 2]>> {
        let mut faces = Vec::new();
        let face_capacity = edges.iter().copied().max().map_or(Ok(0_usize), |edge| {
            (edge as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)
        })?;
        try_reserve_vec(&mut faces, face_capacity)?;
        if !edges.is_empty() {
            faces.resize(face_capacity, [u32::MAX; 2]);
        }
        for (slot, &span) in edges.iter().enumerate() {
            let face = global.walk_faces[topology.face_of[slot] as usize];
            let twin = topology.twins[slot] as usize;
            let other = global.walk_faces[topology.face_of[twin] as usize];
            let pair = &mut faces[span as usize];
            if pair[0] == u32::MAX {
                *pair = [face, other];
            }
        }
        Ok(faces)
    }

    fn explicit_curve_support(
        &self,
        partition: &ExactPartition<'_>,
        span: usize,
        operand: usize,
    ) -> bool {
        partition.owners(span).iter().any(|&(source, _)| {
            let source = source as usize;
            self.source_operands[source] as usize == operand && self.source_rings[source].is_none()
        })
    }

    fn collapsed_ring_at_node(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        point_sources_by_node: &[(NodeId, u32)],
        node: NodeId,
        ring: usize,
    ) -> bool {
        let mut range = topology.starts[node.0 as usize] as usize
            ..topology.starts[node.0 as usize + 1] as usize;
        if range.any(|slot| {
            partition
                .owners(edges[slot] as usize)
                .iter()
                .any(|&(source, _)| {
                    self.source_rings[source as usize] == Some(ring as u32)
                        && self.source_ring_kinds[source as usize] == Some(RingKind::Collapsed)
                })
        }) {
            return true;
        }
        let mut range = node_source_range(point_sources_by_node, node);
        range.any(|index| {
            let source = point_sources_by_node[index].1 as usize;
            self.source_rings[source] == Some(ring as u32)
                && self.source_ring_kinds[source] == Some(RingKind::Collapsed)
        })
    }

    fn node_source_flags(&self, partition: &ExactPartition<'_>) -> Result<Vec<bool>> {
        let operands = self.operand_count();
        let cells = partition
            .node_count()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut line_boundaries = Vec::new();
        try_reserve_vec(&mut line_boundaries, cells)?;
        line_boundaries.resize(cells, false);

        let mut stored = HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        stored
            .try_reserve(partition.node_count())
            .map_err(crate::error::Error::from)?;
        for index in 0..partition.node_count() {
            if let Some(key) = partition.stored_key(index) {
                stored.insert(key, index as u32);
            }
        }
        for &(operand, key) in &self.line_boundaries {
            let Some(&node) = stored.get(&key) else {
                continue;
            };
            let index = node as usize * operands + operand as usize;
            if let Some(value) = line_boundaries.get_mut(index) {
                *value = !*value;
            }
        }
        Ok(line_boundaries)
    }

    fn span_location_from_facts(
        &self,
        operand: usize,
        span: usize,
        left_face: usize,
        right_face: usize,
        facts: &CellFacts,
        operands: usize,
    ) -> Loc {
        let left = facts.face_membership[left_face * operands + operand];
        let right = facts.face_membership[right_face * operands + operand];
        if left || right {
            return match (left, right) {
                (true, true) => Loc::Interior,
                _ => Loc::Boundary,
            };
        }
        if facts.span_support[span * operands + operand] {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    fn node_location_from_facts(
        &self,
        operand: usize,
        node: NodeId,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        facts: &CellFacts,
        line_boundaries: &[bool],
        point_membership: &[bool],
    ) -> Loc {
        let operands = self.operand_count();
        let range = topology.starts[node.0 as usize] as usize
            ..topology.starts[node.0 as usize + 1] as usize;
        let (any_area, all_area) = if range.is_empty() {
            let component = topology.component_of[node.0 as usize] as usize;
            let face = global.component_hosts[component] as usize;
            let value = facts.face_membership[face * operands + operand];
            (value, value)
        } else {
            let mut any = false;
            let mut all = true;
            for slot in range.clone() {
                let face = global.walk_faces[topology.face_of[slot] as usize] as usize;
                let value = facts.face_membership[face * operands + operand];
                any |= value;
                all &= value;
            }
            (any, all)
        };
        if any_area {
            return if all_area {
                Loc::Interior
            } else {
                Loc::Boundary
            };
        }

        let mut line = false;
        let mut residual_degree = 0_usize;
        for slot in range {
            let span = edges[slot] as usize;
            let index = span * operands + operand;
            line |= facts.span_support[index];
            if facts.span_raw[span * 2 * operands + operand]
                || facts.span_raw[(span * 2 + 1) * operands + operand]
            {
                residual_degree += usize::from(!facts.span_curve[index]);
            }
        }
        let node_index = node.0 as usize * operands + operand;
        if line {
            return if line_boundaries[node_index] ^ (residual_degree % 2 == 1) {
                Loc::Boundary
            } else {
                Loc::Interior
            };
        }
        if point_membership[node_index] {
            Loc::Interior
        } else {
            Loc::Exterior
        }
    }

    fn select_cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        operation: crate::geometry::OverlayOp,
        odd: bool,
    ) -> Result<ArealCells> {
        let winding_rings = self.winding_source_rings()?;
        self.select_cells_with_winding_rings(
            partition,
            topology,
            edges,
            global,
            operation,
            odd,
            &winding_rings,
        )
    }

    fn select_cells_with_winding_rings(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        operation: crate::geometry::OverlayOp,
        odd: bool,
        winding_rings: &[Option<u32>],
    ) -> Result<ArealCells> {
        let operands = self.operand_count();
        let facts =
            self.build_cell_facts(partition, topology, edges, global, winding_rings, odd)?;
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
        for face in 0..global.count as usize {
            cells.faces[face] = select_operation(
                operation,
                &facts.face_membership[face * operands..][..operands],
            );
        }

        let node_cells = partition
            .node_count()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut node_membership = Vec::new();
        try_reserve_vec(&mut node_membership, node_cells)?;
        node_membership.resize(node_cells, false);
        for span_index in 0..partition.spans.len() {
            let membership = &facts.span_support[span_index * operands..][..operands];
            cells.spans[span_index] = select_operation(operation, membership);
            if cells.spans[span_index] {
                let span = partition.spans[span_index];
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
            for operand in 0..operands {
                if facts.span_support[span_index * operands + operand] {
                    let span = partition.spans[span_index];
                    node_membership[span.start.0 as usize * operands + operand] = true;
                    node_membership[span.end.0 as usize * operands + operand] = true;
                }
            }
        }
        for (membership, &point) in node_membership.iter_mut().zip(&facts.point_membership) {
            *membership |= point;
        }
        for &node in &partition.isolated {
            let component = topology.component_of[node.0 as usize] as usize;
            let host = global.component_hosts[component] as usize;
            for operand in 0..operands {
                node_membership[node.0 as usize * operands + operand] |=
                    facts.face_membership[host * operands + operand];
            }
        }
        for node in 0..partition.node_count() {
            let range = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            if range.is_empty() {
                continue;
            }
            for operand in 0..operands {
                let any = range.clone().any(|slot| {
                    let face = global.walk_faces[topology.face_of[slot] as usize] as usize;
                    facts.face_membership[face * operands + operand]
                });
                node_membership[node * operands + operand] |= any;
            }
        }
        for node in 0..partition.node_count() {
            let membership = &node_membership[node * operands..][..operands];
            cells.nodes[node] |= select_operation(operation, membership);
        }
        Ok(cells)
    }

    fn linework_source_rings(&self) -> Result<Vec<Option<u32>>> {
        let mut winding_rings = Vec::new();
        try_reserve_vec(&mut winding_rings, self.source_rings.len())?;
        for (&ring, &kind) in self.source_rings.iter().zip(&self.source_ring_kinds) {
            try_push_vec(
                &mut winding_rings,
                matches!(kind, Some(RingKind::Winding))
                    .then(|| ring.expect("a ring source always carries its ring identity")),
            )?;
        }
        Ok(winding_rings)
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
        match shape {
            Shape::Point(point) => {
                try_reserve_vec(&mut self.isolated, 1)?;
                try_reserve_vec(&mut self.isolated_points, 1)?;
                try_reserve_vec(&mut self.isolated_operands, 1)?;
                try_push_vec(&mut self.isolated, point.xy())?;
                try_push_vec(&mut self.isolated_points, *point)?;
                try_push_vec(&mut self.isolated_operands, operand)?;
            },
            Shape::MultiPoint(points) => {
                for point in points.points() {
                    try_reserve_vec(&mut self.isolated, 1)?;
                    try_reserve_vec(&mut self.isolated_points, 1)?;
                    try_reserve_vec(&mut self.isolated_operands, 1)?;
                    try_push_vec(&mut self.isolated, point.xy())?;
                    try_push_vec(&mut self.isolated_points, point)?;
                    try_push_vec(&mut self.isolated_operands, operand)?;
                }
            },
            Shape::LineString(line) => self.append_line(line, operand)?,
            Shape::MultiLineString(lines) => {
                for line in lines.iter() {
                    self.append_line(line, operand)?;
                }
            },
            Shape::Polygon(polygon) => self.append_polygon(polygon, operand)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.iter() {
                    self.append_polygon(polygon, operand)?;
                }
            },
            Shape::GeometryCollection(geometries) => {
                for geometry in geometries {
                    self.append_shape(geometry, operand)?;
                }
            },
            Shape::Empty(..) => {},
        }
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
                .windows(2)
                .any(|pair| exit(pair[0]).0 != entry(pair[1]))
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
        for (node, range) in topology.starts.windows(2).enumerate() {
            let slots = range[0] as usize..range[1] as usize;
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
        for (node, range) in topology.starts.windows(2).enumerate() {
            let mut first = None;
            let mut previous = None;
            let mut descents = 0;
            let mut count = 0;
            for (target, departure) in
                (range[0]..range[1]).zip(&mut departures[range[0] as usize..range[1] as usize])
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

/// Stored endpoints and segment crossings: each point's nearest-even binary64
/// and, for a crossing, its exact homogeneous point built on first demand.
/// Construction handles and the partition nodes they compact into share this
/// one owner of the rounding-order rule.
#[derive(Debug)]
struct PointArena<'a> {
    unique: &'a [Segment],
    recipes: Vec<NodeRecipe>,
    rounded: Vec<XY>,
}

impl<'a> PointArena<'a> {
    fn with_capacity(unique: &'a [Segment], capacity: usize) -> Result<Self> {
        let mut recipes = Vec::new();
        let mut rounded = Vec::new();
        try_reserve_vec(&mut recipes, capacity)?;
        try_reserve_vec(&mut rounded, capacity)?;
        Ok(Self {
            unique,
            recipes,
            rounded,
        })
    }

    const fn len(&self) -> usize {
        self.recipes.len()
    }

    fn push(&mut self, recipe: NodeRecipe, rounded: XY) -> Result<u32> {
        let index = u32::try_from(self.recipes.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_push_vec(&mut self.recipes, recipe)?;
        try_push_vec(&mut self.rounded, rounded)?;
        Ok(index)
    }

    /// A stored endpoint, keeping the source spelling of signed zero.
    fn push_stored(&mut self, point: XY) -> Result<u32> {
        self.push(NodeRecipe::Stored, point)
    }

    /// The crossing of two unique segments' carriers, lower index first,
    /// rounded by the double-double certificate or, when it declines, by the
    /// exact construction it then keeps.
    fn push_crossing(&mut self, segments: [u32; 2]) -> Result<u32> {
        let (first, second) = (
            self.unique[segments[0] as usize],
            self.unique[segments[1] as usize],
        );
        let crossing = crossing_point(first, second)?;
        let exact = std::cell::OnceCell::new();
        if let Some(point) = crossing.exact {
            #[cfg(test)]
            update_partition_metrics(|metrics| metrics.declined_crossings += 1);
            let _ = exact.set(Box::try_new(point)?);
        }
        self.push(NodeRecipe::Crossing { segments, exact }, crossing.point)
    }

    /// Move an entry out, leaving a stored placeholder behind.
    fn take(&mut self, index: u32) -> (NodeRecipe, XY) {
        (
            mem::replace(&mut self.recipes[index as usize], NodeRecipe::Stored),
            self.rounded[index as usize],
        )
    }

    fn is_stored(&self, index: u32) -> bool {
        matches!(self.recipes[index as usize], NodeRecipe::Stored)
    }

    fn stored_key(&self, index: u32) -> Option<PointKey> {
        self.is_stored(index)
            .then(|| PointKey::new(self.rounded[index as usize]))
    }

    fn exact(&self, index: u32) -> Result<Cow<'_, [ExactDyadic; 3]>> {
        match &self.recipes[index as usize] {
            NodeRecipe::Stored => {
                let point = self.rounded[index as usize];
                Ok(Cow::Owned([
                    ExactDyadic::from_f64(point.x),
                    ExactDyadic::from_f64(point.y),
                    ExactDyadic::from_f64(1.0),
                ]))
            },
            NodeRecipe::Crossing { segments, exact } => {
                let point = exact.get_or_try_init(|| -> Result<_> {
                    #[cfg(test)]
                    update_partition_metrics(|metrics| metrics.exact_crossings += 1);
                    Ok(Box::try_new(crossing_homogeneous(
                        self.unique[segments[0] as usize],
                        self.unique[segments[1] as usize],
                    )?)?)
                })?;
                Ok(Cow::Borrowed(&**point))
            },
        }
    }

    /// Order a point's coordinate against a binary64 value by the same rule.
    fn compare_axis_to(&self, index: u32, axis: Axis, value: f64) -> Result<Ordering> {
        let rounded = axis.of(self.rounded[index as usize]);
        if rounded < value {
            return Ok(Ordering::Less);
        }
        if rounded > value {
            return Ok(Ordering::Greater);
        }
        if self.is_stored(index) {
            return Ok(Ordering::Equal);
        }
        let point = self.exact(index)?;
        let axis = axis.index();
        compare_ratios(
            &point[axis],
            &point[2],
            &ExactDyadic::from_f64(value),
            &ExactDyadic::from_f64(1.0),
        )
    }

    /// Certified outward bounds on a coordinate: the stored value, or the
    /// neighbours of a crossing's nearest-even rounding.
    fn axis_bounds(&self, index: u32, axis: Axis) -> (f64, f64) {
        let rounded = axis.of(self.rounded[index as usize]);
        if self.is_stored(index) {
            (rounded, rounded)
        } else {
            (rounded.next_down(), rounded.next_up())
        }
    }

    /// Order items whose points lie on `segment`'s carrier from its start to
    /// its end, reporting each adjacent pair at one point to `equal`.
    fn order_along<T: Copy>(
        &self,
        segment: Segment,
        items: &mut [T],
        point: impl Fn(T) -> u32,
        equal: impl FnMut(T, T) -> Result<()>,
    ) -> Result<()> {
        let (axis, increasing) = segment_axis(segment);
        RayPoints::sort_by_axis(self, items, axis, point, equal)?;
        if !increasing {
            items.reverse();
        }
        Ok(())
    }
}

/// The exact parameter of `node` along `segment`'s carrier, as `[num, den]`
/// with `node = start + (num / den) (end - start)`, read on x unless the
/// segment is vertical.
fn parameter_along(segment: Segment, point: &[ExactDyadic; 3]) -> Result<[ExactDyadic; 2]> {
    let [x, y, w] = point;
    let (coordinate, start, end) = if same_topological_coordinate(segment.start.x, segment.end.x) {
        (y, segment.start.y, segment.end.y)
    } else {
        (x, segment.start.x, segment.end.x)
    };
    let start = ExactDyadic::from_f64(start);
    let direction = ExactDyadic::from_f64(end).try_subtract(start.try_clone()?)?;
    Ok([
        coordinate
            .try_clone()?
            .try_subtract(start.try_product(w)?)?,
        w.try_product(&direction)?,
    ])
}

#[derive(Clone, Copy, Default)]
struct AxisValue {
    value: Option<f64>,
    conflict: bool,
}

impl AxisValue {
    const fn observe(&mut self, value: Option<f64>) {
        let Some(value) = value else {
            return;
        };
        match self.value {
            None => self.value = Some(value),
            Some(previous) if previous.to_bits() == value.to_bits() => {},
            Some(_) => self.conflict = true,
        }
    }

    fn value(self) -> Option<f64> {
        (!self.conflict).then_some(self.value).flatten()
    }
}

/// The ordinate at parameter `num / den` along a source from `start` to
/// `end`, rounded once. A source endpoint keeps its stored spelling, signed
/// zero included.
fn interpolate_ordinate(
    start: Option<f64>,
    end: Option<f64>,
    num: &ExactDyadic,
    den: &ExactDyadic,
) -> Result<Option<f64>> {
    let (Some(start), Some(end)) = (start, end) else {
        return Ok(None);
    };
    if start.to_bits() == end.to_bits() || num.is_zero() {
        return Ok(Some(start));
    }
    if num == den {
        return Ok(Some(end));
    }
    let start = ExactDyadic::from_f64(start);
    let end = ExactDyadic::from_f64(end);
    let left = den.try_clone()?.try_subtract(num.try_clone()?)?;
    let numerator = start.try_product(&left)?.try_add(end.try_product(num)?)?;
    Ok(Some(round_ratio(&numerator, den)?))
}

/// Points a leftward ray sweep reads: rounded coordinates, an order on each
/// axis, and orientation.
trait RayPoints {
    fn rounded(&self, index: u32) -> XY;
    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering>;
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign>;

    /// Sort items by their points' coordinate on `axis`, reporting each
    /// adjacent pair at one coordinate to `equal`. The rounded coordinate
    /// sorts; exact points break only runs of equal rounded coordinates.
    fn sort_by_axis<T: Copy>(
        &self,
        items: &mut [T],
        axis: Axis,
        point: impl Fn(T) -> u32,
        mut equal: impl FnMut(T, T) -> Result<()>,
    ) -> Result<()> {
        let key = |item: T| {
            let value = axis.of(self.rounded(point(item)));
            if value == 0.0 { 0.0 } else { value }
        };
        items.sort_unstable_by(|&left, &right| key(left).total_cmp(&key(right)));
        let mut run = 0;
        while run < items.len() {
            let mut end = run + 1;
            while end < items.len() && key(items[end]).total_cmp(&key(items[run])).is_eq() {
                end += 1;
            }
            if end - run > 1 {
                let tied = &mut items[run..end];
                try_sort_by(tied, |&left, &right| {
                    self.compare_axis(point(left), point(right), axis)
                })?;
                for pair in 1..tied.len() {
                    if self.compare_axis(point(tied[pair - 1]), point(tied[pair]), axis)?
                        == Ordering::Equal
                    {
                        equal(tied[pair - 1], tied[pair])?;
                    }
                }
            }
            run = end;
        }
        Ok(())
    }
}

impl RayPoints for PointArena<'_> {
    fn rounded(&self, index: u32) -> XY {
        self.rounded[index as usize]
    }

    /// Order two points along one axis. Nearest-even rounding is monotone, so
    /// rounded coordinates that differ order the exact points the same way;
    /// only a tie that is not between two stored values reads exact points.
    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering> {
        let (l, r) = (
            axis.of(self.rounded[left as usize]),
            axis.of(self.rounded[right as usize]),
        );
        if l < r {
            return Ok(Ordering::Less);
        }
        if l > r {
            return Ok(Ordering::Greater);
        }
        if left == right || (self.is_stored(left) && self.is_stored(right)) {
            return Ok(Ordering::Equal);
        }
        let (left, right) = (self.exact(left)?, self.exact(right)?);
        let index = axis.index();
        compare_ratios(&left[index], &left[2], &right[index], &right[2])
    }

    /// The orientation of `c` against the directed line through `a` and `b`:
    /// the stored predicate when all three are stored, otherwise an interval
    /// filter over the certified bounds and the exact homogeneous determinant.
    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        if self.is_stored(a) && self.is_stored(b) && self.is_stored(c) {
            let [a, b, c] = [a, b, c].map(|index| self.rounded[index as usize]);
            return Ok(stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y));
        }
        let interval = |index: u32, axis: Axis| {
            let (lo, hi) = self.axis_bounds(index, axis);
            Interval { lo, hi }
        };
        let (ax, ay) = (interval(a, Axis::X), interval(a, Axis::Y));
        let determinant = interval(b, Axis::X)
            .sub(ax)
            .mul(interval(c, Axis::Y).sub(ay))
            .sub(
                interval(b, Axis::Y)
                    .sub(ay)
                    .mul(interval(c, Axis::X).sub(ax)),
            );
        if determinant.is_finite()
            && let Some(sign) = determinant.sign()
        {
            return Ok(sign);
        }
        let [ax, ay, aw] = &*self.exact(a)?;
        let [bx, by, bw] = &*self.exact(b)?;
        let [cx, cy, cw] = &*self.exact(c)?;
        let dx = bx.try_product(aw)?.try_subtract(ax.try_product(bw)?)?;
        let dy = by.try_product(aw)?.try_subtract(ay.try_product(bw)?)?;
        let cx_rel = cx.try_product(aw)?.try_subtract(ax.try_product(cw)?)?;
        let cy_rel = cy.try_product(aw)?.try_subtract(ay.try_product(cw)?)?;
        let sign = dx
            .try_product(&cy_rel)?
            .try_subtract(dy.try_product(&cx_rel)?)?
            .sign();
        // The determinant carries aw^2 bw cw, and aw^2 is positive.
        let flipped = (bw.sign() == ExactSign::Negative) != (cw.sign() == ExactSign::Negative);
        Ok(match (flipped, sign) {
            (true, ExactSign::Negative) => ExactSign::Positive,
            (true, ExactSign::Positive) => ExactSign::Negative,
            (_, sign) => sign,
        })
    }
}

/// Emitted node coordinates, exact as the stored doubles they are.
struct EmittedPoints<'p>(&'p [XY]);

impl RayPoints for EmittedPoints<'_> {
    fn rounded(&self, index: u32) -> XY {
        self.0[index as usize]
    }

    fn compare_axis(&self, left: u32, right: u32, axis: Axis) -> Result<Ordering> {
        let (left, right) = (
            axis.of(self.0[left as usize]),
            axis.of(self.0[right as usize]),
        );
        Ok(if left < right {
            Ordering::Less
        } else if left > right {
            Ordering::Greater
        } else {
            Ordering::Equal
        })
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        let [a, b, c] = [a, b, c].map(|index| self.0[index as usize]);
        Ok(stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y))
    }
}

/// Leftward ray shooting for many queries in one sweep over y.
///
/// The ray from a query runs toward -x just above the query's height, so an
/// edge whose lower endpoint lies at that height is crossed and one whose
/// upper endpoint does is not; every hit is an edge, and the region just east
/// of it holds the query. A ray passing just above a vertex therefore meets
/// the faces its wedge toward +x belongs to, and a horizontal edge at the
/// query's height is never crossed. Each query must be its component's
/// lexicographic minimum, so its own edges never lie strictly left of it.
/// Answers follow `queries`, as indices into `edges`.
///
/// The status holds the edges crossing the sweep line in x order: an edge
/// enters at its lower endpoint and leaves at its upper one, placed and found
/// by binary search on orientations, so the sweep costs O((n + q) log n)
/// comparisons plus the status shifts.
fn shoot_left(
    points: &impl RayPoints,
    edge_count: usize,
    edge: impl Fn(usize) -> [u32; 2],
    queries: &[u32],
) -> Result<Vec<Option<u32>>> {
    let mut order = Vec::new();
    try_reserve_vec(&mut order, queries.len())?;
    order.extend(0..queries.len());
    points.sort_by_axis(&mut order, Axis::Y, |index| queries[index], |_, _| Ok(()))?;
    // [lower, upper, edge] for the edges some ray crosses: a horizontal or
    // zero-length edge never does, nor one no query height falls within.
    let mut starts: Vec<[u32; 3]> = Vec::new();
    for index in 0..edge_count {
        let [a, b] = edge(index);
        let (lower, upper) = match points.compare_axis(a, b, Axis::Y)? {
            Ordering::Less => (a, b),
            Ordering::Greater => (b, a),
            Ordering::Equal => continue,
        };
        let first = first_not_left(&order, |query| {
            Ok(points.compare_axis(queries[query], lower, Axis::Y)? == Ordering::Less)
        })?;
        if let Some(&query) = order.get(first)
            && points.compare_axis(queries[query], upper, Axis::Y)? == Ordering::Less
        {
            let index = u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?;
            try_push_vec(&mut starts, [lower, upper, index])?;
        }
    }
    let mut ends = Vec::new();
    try_reserve_vec(&mut ends, starts.len())?;
    ends.extend_from_slice(&starts);
    points.sort_by_axis(&mut starts, Axis::Y, |[lower, ..]| lower, |_, _| Ok(()))?;
    points.sort_by_axis(&mut ends, Axis::Y, |[_, upper, _]| upper, |_, _| Ok(()))?;
    let mut answers = Vec::new();
    try_reserve_vec(&mut answers, queries.len())?;
    answers.resize(queries.len(), None);

    let orientation = |a, b, c| {
        #[cfg(test)]
        update_partition_metrics(|metrics| metrics.ray_orientations += 1);
        points.orientation(a, b, c)
    };
    let negative = |a, b, c| -> Result<bool> { Ok(orientation(a, b, c)? == ExactSign::Negative) };
    let positive = |a, b, c| -> Result<bool> { Ok(orientation(a, b, c)? == ExactSign::Positive) };
    // Whether `edge` lies left of every point just above `point`'s height on
    // the ray through it; for an edge starting at `point`, the fan order.
    let left_above = |[lower, upper, _]: [u32; 3], [start, end, _]: [u32; 3]| -> Result<bool> {
        if lower == start {
            positive(start, end, upper)
        } else {
            negative(lower, upper, start)
        }
    };
    // The same just below the height where `ending` leaves, among the edges
    // converging on its upper endpoint by their lower directions.
    let left_below = |[lower, upper, _]: [u32; 3], [start, end, _]: [u32; 3]| -> Result<bool> {
        if upper == end {
            positive(start, end, lower)
        } else {
            negative(lower, upper, end)
        }
    };
    let mut status: Vec<[u32; 3]> = Vec::new();
    try_reserve_vec(&mut status, starts.len())?;
    let (mut next_start, mut next_end, mut next_query) = (0, 0, 0);
    while next_query < order.len() {
        let mut height = queries[order[next_query]];
        if let Some(&[lower, ..]) = starts.get(next_start)
            && points.compare_axis(lower, height, Axis::Y)? == Ordering::Less
        {
            height = lower;
        }
        if let Some(&[_, upper, _]) = ends.get(next_end)
            && points.compare_axis(upper, height, Axis::Y)? == Ordering::Less
        {
            height = upper;
        }
        while let Some(&ending) = ends.get(next_end)
            && points.compare_axis(ending[1], height, Axis::Y)? == Ordering::Equal
        {
            let index = first_not_left(&status, |edge| left_below(edge, ending))?;
            if status.get(index).map(|edge| edge[2]) != Some(ending[2]) {
                return Err(GeometryErrorKind::Invalid(
                    "leftward ray sweep lost an edge from its status".into(),
                )
                .into());
            }
            status.remove(index);
            next_end += 1;
        }
        while let Some(&starting) = starts.get(next_start)
            && points.compare_axis(starting[0], height, Axis::Y)? == Ordering::Equal
        {
            let index = first_not_left(&status, |edge| left_above(edge, starting))?;
            status.insert(index, starting);
            next_start += 1;
        }
        while let Some(&query) = order.get(next_query)
            && points.compare_axis(queries[query], height, Axis::Y)? == Ordering::Equal
        {
            let point = queries[query];
            let index = first_not_left(&status, |[lower, upper, _]| negative(lower, upper, point))?;
            answers[query] = index.checked_sub(1).map(|left| status[left][2]);
            next_query += 1;
        }
    }
    Ok(answers)
}

/// The first item that is not left of the probe, for items ordered so that
/// `left` holds on a prefix.
fn first_not_left<T: Copy>(items: &[T], mut left: impl FnMut(T) -> Result<bool>) -> Result<usize> {
    let (mut low, mut high) = (0, items.len());
    while low < high {
        let middle = low + (high - low) / 2;
        if left(items[middle])? {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Ok(low)
}

/// Construction handles: interned stored endpoints and crossings, unioned
/// where they denote one point.
struct Forest<'a> {
    parent: Vec<u32>,
    points: PointArena<'a>,
}

impl<'a> Forest<'a> {
    fn with_capacity(unique: &'a [Segment], capacity: usize) -> Result<Self> {
        let mut parent = Vec::new();
        try_reserve_vec(&mut parent, capacity)?;
        Ok(Self {
            parent,
            points: PointArena::with_capacity(unique, capacity)?,
        })
    }

    fn find(&mut self, mut node: u32) -> u32 {
        while self.parent[node as usize] != node {
            self.parent[node as usize] = self.parent[self.parent[node as usize] as usize];
            node = self.parent[node as usize];
        }
        node
    }

    fn adopt(&mut self, handle: u32) -> Result<u32> {
        debug_assert_eq!(handle as usize, self.parent.len());
        try_push_vec(&mut self.parent, handle)?;
        Ok(handle)
    }

    fn push_stored(&mut self, point: XY) -> Result<u32> {
        let handle = self.points.push_stored(point)?;
        self.adopt(handle)
    }

    fn push_crossing(&mut self, segments: [u32; 2]) -> Result<u32> {
        let handle = self.points.push_crossing(segments)?;
        self.adopt(handle)
    }

    /// Join two handles denoting one point. A stored root wins, and between
    /// crossings the lower segment pair, so a node's recipe is deterministic.
    fn union(&mut self, a: u32, b: u32) -> Result<()> {
        let a = self.find(a);
        let b = self.find(b);
        if a == b {
            return Ok(());
        }
        let (keep, merge) = match (
            &self.points.recipes[a as usize],
            &self.points.recipes[b as usize],
        ) {
            (NodeRecipe::Stored, NodeRecipe::Stored) => {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition unioned distinct stored points".into(),
                )
                .into());
            },
            (NodeRecipe::Stored, NodeRecipe::Crossing { .. }) => (a, b),
            (NodeRecipe::Crossing { .. }, NodeRecipe::Stored) => (b, a),
            (
                NodeRecipe::Crossing { segments: left, .. },
                NodeRecipe::Crossing {
                    segments: right, ..
                },
            ) => {
                if right < left {
                    (b, a)
                } else {
                    (a, b)
                }
            },
        };
        self.parent[merge as usize] = keep;
        Ok(())
    }
}

/// The axis on which a segment's direction is largest, so it orders points
/// on its carrier, and whether the segment runs toward increasing values.
fn segment_axis(segment: Segment) -> (Axis, bool) {
    let (dx, dy) = (
        segment.end.x - segment.start.x,
        segment.end.y - segment.start.y,
    );
    if dx.abs() >= dy.abs() {
        (Axis::X, dx > 0.0)
    } else {
        (Axis::Y, dy > 0.0)
    }
}

/// Whether `point`, on the carrier of the nondegenerate `segment`, lies
/// strictly between its endpoints.
fn strictly_inside(segment: Segment, point: XY) -> bool {
    let (axis, _) = segment_axis(segment);
    let (start, end, value) = (axis.of(segment.start), axis.of(segment.end), axis.of(point));
    start.min(end) < value && value < start.max(end)
}

pub(in crate::geometry) fn partition_unique_segments<'a>(
    unique: &'a [Segment],
    provenance: &SegmentProvenance,
    isolated: &[XY],
    budget: &mut ExpansionBudget,
) -> Result<ExactPartition<'a>> {
    debug_assert_eq!(provenance.len(), unique.len());
    let endpoint_capacity = unique
        .len()
        .checked_mul(2)
        .and_then(|count| count.checked_add(isolated.len()))
        .ok_or(ResourceError::SizeOverflow)?;
    let mut endpoint_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    endpoint_of
        .try_reserve(endpoint_capacity)
        .map_err(crate::error::Error::from)?;
    let mut forest = Forest::with_capacity(unique, endpoint_capacity)?;

    let intern_stored =
        |point: XY, endpoint_of: &mut HashMap<PointKey, u32>, forest: &mut Forest| -> Result<u32> {
            let key = PointKey::new(point);
            if let Some(&id) = endpoint_of.get(&key) {
                return Ok(id);
            }
            let id = forest.push_stored(point)?;
            endpoint_of.insert(key, id);
            Ok(id)
        };

    let mut start_handle = Vec::new();
    let mut end_handle = Vec::new();
    try_reserve_vec(&mut start_handle, unique.len())?;
    try_reserve_vec(&mut end_handle, unique.len())?;
    for &segment in unique {
        try_push_vec(
            &mut start_handle,
            intern_stored(segment.start, &mut endpoint_of, &mut forest)?,
        )?;
        try_push_vec(
            &mut end_handle,
            intern_stored(segment.end, &mut endpoint_of, &mut forest)?,
        )?;
    }

    let pool_capacity = unique
        .len()
        .checked_add(isolated.len())
        .ok_or(ResourceError::SizeOverflow)?;
    let mut pool = Vec::new();
    try_reserve_vec(&mut pool, pool_capacity)?;
    pool.extend_from_slice(unique);
    let mut extra_handles = Vec::new();
    try_reserve_vec(&mut extra_handles, isolated.len())?;
    let mut point_handles = Vec::new();
    try_reserve_vec(&mut point_handles, isolated.len())?;
    for &point in isolated {
        let key = PointKey::new(point);
        let existed = endpoint_of.contains_key(&key);
        let handle = intern_stored(point, &mut endpoint_of, &mut forest)?;
        try_push_vec(&mut point_handles, handle)?;
        if existed {
            continue;
        }
        try_push_vec(&mut extra_handles, handle)?;
        try_push_vec(&mut pool, Segment {
            start: point,
            end: point,
        })?;
    }
    #[cfg(test)]
    update_partition_metrics(|metrics| {
        metrics.candidate_pool_items = pool.len();
    });

    // Interior contacts as (unique, handle): a crossing, a stored point
    // touching a segment's interior, or an overlap end.
    let mut contacts: Vec<(u32, u32)> = Vec::new();
    let handles = EndpointHandles {
        start: &start_handle,
        end: &end_handle,
        extra: &extra_handles,
    };
    let mut pair_error = None;
    #[cfg(test)]
    let candidate_phase =
        crate::test_support::allocation_phase(crate::test_support::AllocationPhase::CandidatePairs);
    if for_each_candidate_pair(&pool, single_chain, |left, right| {
        #[cfg(test)]
        record_candidate_pair();
        match visit_pool_pair(
            unique,
            &pool,
            left,
            right,
            &handles,
            &mut contacts,
            &mut forest,
        ) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => {
                pair_error = Some(error);
                ControlFlow::Break(())
            },
        }
    })?
    .is_break()
        && let Some(error) = pair_error
    {
        return Err(error);
    }
    #[cfg(test)]
    drop(candidate_phase);
    #[cfg(test)]
    update_partition_metrics(|metrics| {
        metrics.source_cut_entries = contacts.len();
    });

    // Group contacts by segment (counting placement), then order each group
    // along its segment: by rounded coordinate first, and exactly only within
    // a run of equal rounded coordinates, whose equal points are one node.
    let mut contact_starts = Vec::new();
    try_reserve_vec(&mut contact_starts, unique.len() + 1)?;
    contact_starts.resize(unique.len() + 1, 0_u32);
    for &(segment, _) in &contacts {
        contact_starts[segment as usize + 1] += 1;
    }
    for index in 0..unique.len() {
        contact_starts[index + 1] += contact_starts[index];
    }
    let mut along = Vec::new();
    try_reserve_vec(&mut along, contacts.len())?;
    along.resize(contacts.len(), 0_u32);
    {
        let mut cursor = Vec::new();
        try_reserve_vec(&mut cursor, unique.len())?;
        cursor.extend_from_slice(&contact_starts[..unique.len()]);
        for &(segment, handle) in &contacts {
            along[cursor[segment as usize] as usize] = handle;
            cursor[segment as usize] += 1;
        }
    }
    drop(contacts);
    let mut equal_pairs: Vec<(u32, u32)> = Vec::new();
    for (index, &segment) in unique.iter().enumerate() {
        let group = &mut along[contact_starts[index] as usize..contact_starts[index + 1] as usize];
        if group.len() > 1 {
            forest.points.order_along(
                segment,
                group,
                |handle| handle,
                |left, right| try_push_vec(&mut equal_pairs, (left, right)),
            )?;
        }
    }
    for (left, right) in equal_pairs {
        forest.union(left, right)?;
    }

    // Compact roots into nodes, moving each root's point out of the forest.
    let mut compact_of: Vec<Option<u32>> = Vec::new();
    try_reserve_vec(&mut compact_of, forest.parent.len())?;
    compact_of.resize(forest.parent.len(), None);
    let mut points = PointArena::with_capacity(unique, 0)?;
    for handle in 0..forest.parent.len() as u32 {
        let root = forest.find(handle);
        if compact_of[root as usize].is_some() {
            continue;
        }
        let (recipe, rounded) = forest.points.take(root);
        if matches!(recipe, NodeRecipe::Crossing { .. }) {
            budget.add(1)?;
        }
        compact_of[root as usize] = Some(points.push(recipe, rounded)?);
    }
    for handle in 0..forest.parent.len() {
        let root = forest.find(handle as u32);
        compact_of[handle] = compact_of[root as usize];
    }
    let node_of = |handle: u32| -> Result<u32> {
        compact_of[handle as usize].ok_or_else(|| {
            GeometryErrorKind::Invalid("partition handle was not compacted".into()).into()
        })
    };

    // Walk every segment from its start through its ordered contacts to its
    // end: consecutive distinct nodes are its atoms, and every node met is
    // bound to it once. Segments are visited in ascending order, so a
    // counting placement keeps each node's bindings in that order.
    let node_count = points.len();
    let mut last_unique = Vec::new();
    try_reserve_vec(&mut last_unique, node_count)?;
    last_unique.resize(node_count, u32::MAX);
    let mut binding_nodes: Vec<(u32, u32)> = Vec::new();
    let mut binding_starts = Vec::new();
    try_reserve_vec(&mut binding_starts, node_count + 1)?;
    binding_starts.resize(node_count + 1, 0_u32);
    let mut intervals: Vec<(u32, u32, u32)> = Vec::new();
    for (index, segment) in unique.iter().enumerate() {
        // A zero-length segment is an isolated point, bound as a point source.
        if same_point(segment.start, segment.end) {
            continue;
        }
        let source = index as u32;
        let group = &along[contact_starts[index] as usize..contact_starts[index + 1] as usize];
        let walk = std::iter::once(start_handle[index])
            .chain(group.iter().copied())
            .chain(std::iter::once(end_handle[index]));
        let mut previous: Option<u32> = None;
        for handle in walk {
            let node = node_of(handle)?;
            if last_unique[node as usize] != source {
                last_unique[node as usize] = source;
                binding_starts[node as usize + 1] += 1;
                try_push_vec(&mut binding_nodes, (node, source))?;
            }
            if let Some(start) = previous
                && start != node
            {
                try_push_vec(&mut intervals, (source, start, node))?;
            }
            previous = Some(node);
        }
    }
    for node in 0..node_count {
        binding_starts[node + 1] = binding_starts[node + 1]
            .checked_add(binding_starts[node])
            .ok_or(ResourceError::SizeOverflow)?;
    }
    let mut bindings = Vec::new();
    try_reserve_vec(&mut bindings, binding_nodes.len())?;
    bindings.resize(binding_nodes.len(), 0_u32);
    {
        let mut cursor = Vec::new();
        try_reserve_vec(&mut cursor, node_count)?;
        cursor.extend_from_slice(&binding_starts[..node_count]);
        for &(node, source) in &binding_nodes {
            bindings[cursor[node as usize] as usize] = source;
            cursor[node as usize] += 1;
        }
    }

    // The budget counts operation-wide net growth: overlap coalescing can
    // offset span growth elsewhere, while stored nodes and source atoms are free.
    let baseline = unique
        .iter()
        .filter(|segment| !same_point(segment.start, segment.end))
        .count();
    let (spans, atom_provenance) = aggregate_atoms(intervals, provenance, baseline, budget)?;

    let mut degree: Vec<u32> = Vec::new();
    try_reserve_vec(&mut degree, node_count)?;
    degree.resize(node_count, 0);
    for span in &spans {
        degree[span.start.0 as usize] = degree[span.start.0 as usize].saturating_add(1);
        degree[span.end.0 as usize] = degree[span.end.0 as usize].saturating_add(1);
    }
    let mut isolated_nodes = Vec::new();
    for (index, &count) in degree.iter().enumerate() {
        if count == 0 {
            try_push_vec(&mut isolated_nodes, NodeId(index as u32))?;
        }
    }

    let mut point_nodes = Vec::new();
    try_reserve_vec(&mut point_nodes, point_handles.len())?;
    for handle in point_handles {
        try_push_vec(&mut point_nodes, NodeId(node_of(handle)?))?;
    }

    let mut point_sources = Vec::new();
    for (source, segment) in unique.iter().enumerate() {
        if same_point(segment.start, segment.end) {
            let node = NodeId(node_of(start_handle[source])?);
            let owners = provenance.owners(source);
            try_reserve_vec(&mut point_sources, owners.len())?;
            point_sources.extend(owners.iter().map(|&(ordinal, _)| (node, ordinal)));
        }
    }

    Ok(ExactPartition {
        unique,
        points,
        spans,
        provenance: atom_provenance,
        isolated: isolated_nodes,
        point_nodes,
        point_sources,
        binding_starts,
        bindings,
        kept_nodes: KeptNodes::Selected,
    })
}

/// The stored handles a pair visit can cut with: each unique segment's
/// endpoints, and the isolated points pooled after the segments.
struct EndpointHandles<'h> {
    start: &'h [u32],
    end: &'h [u32],
    extra: &'h [u32],
}

fn visit_pool_pair(
    unique: &[Segment],
    pool: &[Segment],
    left: usize,
    right: usize,
    handles: &EndpointHandles<'_>,
    contacts: &mut Vec<(u32, u32)>,
    forest: &mut Forest<'_>,
) -> Result<()> {
    let count = unique.len();
    match (left < count, right < count) {
        (true, true) => classify_pair_into(unique, left, right, handles, contacts, forest),
        (true, false) => push_point_on_source(
            unique[left],
            left,
            pool[right].start,
            handles.extra[right - count],
            contacts,
        ),
        (false, true) => push_point_on_source(
            unique[right],
            right,
            pool[left].start,
            handles.extra[left - count],
            contacts,
        ),
        (false, false) => Ok(()),
    }
}

fn push_contact(contacts: &mut Vec<(u32, u32)>, segment: usize, handle: u32) -> Result<()> {
    let segment = u32::try_from(segment).map_err(|_| ResourceError::SizeOverflow)?;
    try_push_vec(contacts, (segment, handle))
}

/// A stored point exactly on a segment's carrier and strictly between its
/// endpoints cuts that segment.
fn push_point_on_source(
    segment: Segment,
    index: usize,
    point: XY,
    handle: u32,
    contacts: &mut Vec<(u32, u32)>,
) -> Result<()> {
    if same_point(segment.start, segment.end) || !point_in_segment_envelope(point, segment) {
        return Ok(());
    }
    let side = stored_orientation_sign(
        segment.start.x,
        segment.start.y,
        segment.end.x,
        segment.end.y,
        point.x,
        point.y,
    );
    if side == ExactSign::Zero && strictly_inside(segment, point) {
        push_contact(contacts, index, handle)?;
    }
    Ok(())
}

/// Cut two collinear segments at every endpoint of one strictly inside the
/// other.
fn push_overlap(
    [a, b]: [Segment; 2],
    [left, right]: [usize; 2],
    handles: &EndpointHandles<'_>,
    contacts: &mut Vec<(u32, u32)>,
) -> Result<()> {
    #[cfg(test)]
    let before = contacts.len();
    for (host, host_index, guest, guest_index) in [(a, left, b, right), (b, right, a, left)] {
        for (point, handle) in [
            (guest.start, handles.start[guest_index]),
            (guest.end, handles.end[guest_index]),
        ] {
            if strictly_inside(host, point) {
                push_contact(contacts, host_index, handle)?;
            }
        }
    }
    #[cfg(test)]
    if contacts.len() > before {
        record_overlap(contacts.len() - before);
    }
    Ok(())
}

fn classify_pair_into(
    unique: &[Segment],
    left: usize,
    right: usize,
    handles: &EndpointHandles<'_>,
    contacts: &mut Vec<(u32, u32)>,
    forest: &mut Forest<'_>,
) -> Result<()> {
    let a = unique[left];
    let b = unique[right];
    if same_point(a.start, a.end) {
        return push_point_on_source(b, right, a.start, handles.start[left], contacts);
    }
    if same_point(b.start, b.end) {
        return push_point_on_source(a, left, b.start, handles.start[right], contacts);
    }
    if segment_envelopes_disjoint(a, b) {
        return Ok(());
    }
    #[cfg(test)]
    let _contact_phase = crate::test_support::allocation_phase(
        crate::test_support::AllocationPhase::OverlapContacts,
    );
    let orient = |segment: Segment, point: XY| {
        stored_orientation_sign(
            segment.start.x,
            segment.start.y,
            segment.end.x,
            segment.end.y,
            point.x,
            point.y,
        )
    };
    // A shared endpoint lies on both segments, so the pair meets elsewhere
    // only along a collinear overlap. Adjacent ring segments are most pairs,
    // and their zero orientation is the one the floating filter cannot certify.
    let shared = |point: XY| same_point(point, a.start) || same_point(point, a.end);
    let unshared = match (shared(b.start), shared(b.end)) {
        // Unique segments never repeat, so this is `a` itself.
        (true, true) => return Ok(()),
        (true, false) => Some(b.end),
        (false, true) => Some(b.start),
        (false, false) => None,
    };
    if let Some(other) = unshared {
        if orient(a, other) == ExactSign::Zero {
            return push_overlap([a, b], [left, right], handles, contacts);
        }
        return Ok(());
    }
    let (o_b0, o_b1) = (orient(a, b.start), orient(a, b.end));
    if o_b0 == ExactSign::Zero && o_b1 == ExactSign::Zero {
        return push_overlap([a, b], [left, right], handles, contacts);
    }
    // One endpoint on the other carrier: a touch when it lies strictly
    // inside, otherwise the segments do not meet.
    for (zero, host, index, point, handle) in [
        (
            o_b0 == ExactSign::Zero,
            a,
            left,
            b.start,
            handles.start[right],
        ),
        (o_b1 == ExactSign::Zero, a, left, b.end, handles.end[right]),
    ] {
        if zero {
            if strictly_inside(host, point) {
                push_contact(contacts, index, handle)?;
            }
            return Ok(());
        }
    }
    let (o_a0, o_a1) = (orient(b, a.start), orient(b, a.end));
    for (zero, host, index, point, handle) in [
        (
            o_a0 == ExactSign::Zero,
            b,
            right,
            a.start,
            handles.start[left],
        ),
        (o_a1 == ExactSign::Zero, b, right, a.end, handles.end[left]),
    ] {
        if zero {
            if strictly_inside(host, point) {
                push_contact(contacts, index, handle)?;
            }
            return Ok(());
        }
    }
    // Strictly opposite sides on both carriers: a proper crossing, interior
    // to both segments.
    if o_b0 != o_b1 && o_a0 != o_a1 {
        let segment = |index: usize| u32::try_from(index).map_err(|_| ResourceError::SizeOverflow);
        let handle =
            forest.push_crossing([segment(left.min(right))?, segment(left.max(right))?])?;
        push_contact(contacts, left, handle)?;
        push_contact(contacts, right, handle)?;
    }
    Ok(())
}

fn aggregate_atoms(
    intervals: Vec<(u32, u32, u32)>,
    source_provenance: &SegmentProvenance,
    baseline: usize,
    budget: &mut ExpansionBudget,
) -> Result<(Vec<ExactSpan>, SegmentProvenance)> {
    let mut atom_of: HashMap<(u32, u32), u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    atom_of
        .try_reserve(intervals.len())
        .map_err(crate::error::Error::from)?;
    let mut spans: Vec<ExactSpan> = Vec::new();
    let mut contributors: Vec<(u32, u32, bool)> = Vec::new();
    try_reserve_vec(&mut contributors, intervals.len())?;
    for (unique, start, end) in intervals {
        let lo = start.min(end);
        let hi = start.max(end);
        if let Some(&atom) = atom_of.get(&(lo, hi)) {
            let reversed = start != spans[atom as usize].start.0;
            try_push_vec(&mut contributors, (atom, unique, reversed))?;
            continue;
        }
        if spans.len() >= baseline {
            budget.add(1)?;
        }
        let atom = u32::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut spans, 1)?;
        try_push_vec(&mut spans, ExactSpan {
            start: NodeId(start),
            end: NodeId(end),
            unique,
        })?;
        atom_of.insert((lo, hi), atom);
        try_push_vec(&mut contributors, (atom, unique, false))?;
    }

    let atom_count = spans.len();
    let mut counts: Vec<usize> = Vec::new();
    try_reserve_vec(&mut counts, atom_count)?;
    counts.resize(atom_count, 0);
    for &(atom, unique, _) in &contributors {
        counts[atom as usize] = counts[atom as usize]
            .checked_add(source_provenance.owners(unique as usize).len())
            .ok_or(ResourceError::SizeOverflow)?;
    }
    let mut offsets: Vec<u32> = Vec::new();
    try_reserve_vec(
        &mut offsets,
        atom_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    try_push_vec(&mut offsets, 0)?;
    for &count in &counts {
        let next = (offsets[offsets.len() - 1] as usize)
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
        let next = u32::try_from(next).map_err(|_| ResourceError::SizeOverflow)?;
        try_push_vec(&mut offsets, next)?;
    }
    let mut entries: Vec<SegmentSource> = Vec::new();
    try_reserve_vec(&mut entries, offsets[atom_count] as usize)?;
    entries.resize(offsets[atom_count] as usize, (0, false));
    let mut cursor: Vec<u32> = Vec::new();
    try_reserve_vec(&mut cursor, atom_count)?;
    cursor.extend_from_slice(&offsets[..atom_count]);
    for &(atom, unique, reversed_geom) in &contributors {
        for &(ordinal, reversed) in source_provenance.owners(unique as usize) {
            let slot = cursor[atom as usize] as usize;
            entries[slot] = (ordinal, reversed != reversed_geom);
            cursor[atom as usize] += 1;
        }
    }
    Ok((spans, SegmentProvenance::from_csr(offsets, entries)))
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
                        [winding[0], winding[1], winding[2], winding[3]],
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
                        [winding[0], winding[1], winding[2]],
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
            .windows(2)
            .map(|window| window[1] - window[0])
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
                    windings.push([winding[0], winding[1]]);
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
                    windings.push([winding[0], winding[1]]);
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
            let [x, y, w] = crossing_homogeneous(partition.unique[0], partition.unique[1]).unwrap();
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
