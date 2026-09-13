//! Exact source partition of unique stored segments.
//!
//! Borrows unique undirected representatives and existing CSR provenance.
//! Constructed nodes keep one source-parameter recipe after equal-param union;
//! stored nodes keep `PointKey`. Exact cyclic walks and visibility links define
//! global faces; stored-ring winding selects cells and an emission plan.
//! Emission checks rounded incidence, cyclic order, and global-face identity.
//!
//! Candidates come from the shared fallible sweep over unique segments plus
//! extra isolated points as zero-length items. Contact-derived cuts, compact
//! nodes, and canonical atoms are generated items. Sweep scratch is
//! [`ResourceError`], not the item cap.

use std::cmp::Ordering;
use std::mem;
use std::ops::{ControlFlow, Range};

use crate::error::ResourceError;
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch, compare_departures};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::build_certified_overlay_shape;
use crate::geometry::overlay::noding::{SegmentProvenance, SegmentSource};
use crate::geometry::segments::{
    point_in_segment_envelope, point_on_segment, segment_envelopes_disjoint,
    stored_orientation_sign,
};
use crate::geometry::tessellation::exact::{
    ExactDyadic, outward_ratio_bounds, round_ratio, try_compare_ratios,
};
use crate::geometry::{
    Bounds, Contact, CoordSeq, Coordinates as _, De9im, Dimension, ExpansionBudget,
    GeometryErrorKind, HashMap, LineSeq, Loc, Point, PointKey, Polygon, PolygonizeFull, Result,
    Ring, RingKind, Segment, Shape, XY, for_each_candidate_pair, same_point, segment_contact_exact,
    single_chain,
};
use crate::{try_push_vec, try_reserve_vec};

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
        try_reserve_vec(&mut cells.nodes, partition.recipes.len())?;
        cells.nodes.resize(partition.recipes.len(), false);
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum NodeRecipe {
    Stored(PointKey),
    Constructed {
        unique: u32,
        num: ExactDyadic,
        den: ExactDyadic,
    },
}

impl NodeRecipe {
    const fn stored_key(&self) -> Option<PointKey> {
        match self {
            Self::Stored(key) => Some(*key),
            Self::Constructed { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactSpan {
    pub start: NodeId,
    pub end: NodeId,
    pub unique: u32,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VisibilityHit {
    Node(NodeId),
    Span(u32),
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

#[derive(Clone, Copy, Debug)]
struct RoundedExtent {
    x_lo: f64,
    x_hi: f64,
    y_lo: f64,
    y_hi: f64,
}

#[derive(Clone, Copy, Debug)]
struct VisibilityEntry {
    edge: u32,
    extent: RoundedExtent,
    prefix_y_hi: f64,
}

#[derive(Debug, Default)]
struct VisibilityIndex {
    entries: Vec<VisibilityEntry>,
}

impl VisibilityIndex {
    fn build(partition: &ExactPartition<'_>) -> Result<Self> {
        let mut entries = Vec::new();
        try_reserve_vec(&mut entries, partition.spans.len())?;
        for (edge, span) in partition.spans.iter().enumerate() {
            let [ax, ay, aw] = partition.homogeneous(span.start)?;
            let [bx, by, bw] = partition.homogeneous(span.end)?;
            let (ax_lo, ax_hi) = outward_ratio_bounds(&ax, &aw)?;
            let (ay_lo, ay_hi) = outward_ratio_bounds(&ay, &aw)?;
            let (bx_lo, bx_hi) = outward_ratio_bounds(&bx, &bw)?;
            let (by_lo, by_hi) = outward_ratio_bounds(&by, &bw)?;
            try_push_vec(&mut entries, VisibilityEntry {
                edge: u32::try_from(edge).map_err(|_| ResourceError::SizeOverflow)?,
                extent: RoundedExtent {
                    x_lo: ax_lo.min(bx_lo),
                    x_hi: ax_hi.max(bx_hi),
                    y_lo: ay_lo.min(by_lo),
                    y_hi: ay_hi.max(by_hi),
                },
                prefix_y_hi: f64::NEG_INFINITY,
            })?;
        }
        entries.sort_unstable_by(|left, right| {
            left.extent
                .y_lo
                .total_cmp(&right.extent.y_lo)
                .then_with(|| left.edge.cmp(&right.edge))
        });
        let mut prefix_y_hi = f64::NEG_INFINITY;
        for entry in &mut entries {
            prefix_y_hi = prefix_y_hi.max(entry.extent.y_hi);
            entry.prefix_y_hi = prefix_y_hi;
        }
        Ok(Self { entries })
    }

    fn candidate_range(&self, query_y_lo: f64, query_y_hi: f64) -> Range<usize> {
        let mut low = 0;
        let mut high = self.entries.len();
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if self.entries[middle - 1].extent.y_lo <= query_y_hi {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let end = low;
        low = 0;
        high = end;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.entries[middle].prefix_y_hi < query_y_lo {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low..end
    }
}

struct PlannedChord {
    sources: Range<usize>,
    sides: [u32; 2],
}

struct BoundaryPlan {
    source_darts: Vec<u32>,
    walks: Vec<Vec<PlannedChord>>,
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
            let mut xs = Vec::new();
            let mut ys = Vec::new();
            let coordinate_count = chords
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            try_reserve_vec(&mut xs, coordinate_count)?;
            try_reserve_vec(&mut ys, coordinate_count)?;
            for chord in chords {
                try_push_vec(&mut xs, chord.start.x)?;
                try_push_vec(&mut ys, chord.start.y)?;
            }
            try_push_vec(&mut xs, chords[0].start.x)?;
            try_push_vec(&mut ys, chords[0].start.y)?;
            canonicalize_closed_xy(&mut xs, &mut ys);
            try_push_vec(
                &mut rings,
                Some(Ring::from_closed(CoordSeq::try_from_vecs(
                    xs, ys, None, None,
                )?)),
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
        lines.sort_by(crate::geometry::compare_point_slices);
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
        // Round and certify once, while retaining the exact node identities for
        // the publication pass.  No emitted coordinate is used to rediscover
        // its source node.
        let items = self.incident_items(topology)?;
        let rounded_segments = self.round_items(partition, topology)?;
        let mut rounded = Vec::new();
        let rounded_capacity = items
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut rounded, rounded_capacity)?;
        for (&[start, end], segment) in items.iter().zip(&rounded_segments) {
            try_push_vec(&mut rounded, (start, segment.start))?;
            try_push_vec(&mut rounded, (end, segment.end))?;
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
            let departure = |chord: &PlannedChord| self.source_darts[chord.sources.start];
            let mut minimum = 0;
            for candidate in 1..walk.len() {
                if partition.compare_nodes(
                    NodeId(topology.owners[departure(&walk[candidate]) as usize]),
                    NodeId(topology.owners[departure(&walk[minimum]) as usize]),
                )? == Ordering::Less
                {
                    minimum = candidate;
                }
            }
            let outgoing = departure(&walk[minimum]);
            let prior = &walk[if minimum == 0 {
                walk.len() - 1
            } else {
                minimum - 1
            }];
            let incoming = topology.twins[self.source_darts[prior.sources.end - 1] as usize];
            let root = NodeId(topology.owners[outgoing as usize]);
            let [_, ry, rw] = partition.homogeneous(root)?;
            let upper = |slot: u32| -> Result<bool> {
                let [_, y, w] = partition.homogeneous(NodeId(topology.targets[slot as usize]))?;
                Ok(try_compare_ratios(&y, &w, &ry, &rw)? != Ordering::Less)
            };
            let outgoing_upper = upper(outgoing)?;
            let incoming_upper = upper(incoming)?;
            // At the exact leftmost vertex both rays point right. The fan's
            // angular cut is +x, so a lower outgoing ray precedes an upper one.
            let is_shell = if outgoing_upper == incoming_upper {
                outgoing < incoming
            } else {
                !outgoing_upper
            };
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
        self.check_emitted_fans(topology, &segments)?;
        self.check_global_faces(partition, &items, &segments)?;
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
        let mut parents = Vec::new();
        let mut coordinates = Vec::new();
        try_reserve_vec(&mut parents, partition.recipes.len())?;
        try_reserve_vec(&mut coordinates, partition.recipes.len())?;
        for node in 0..partition.recipes.len() {
            try_push_vec(
                &mut parents,
                u32::try_from(node).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
        }
        coordinates.resize(partition.recipes.len(), None);
        for (&[start, end], segment) in items.iter().zip(segments) {
            coordinates[start.0 as usize] = Some(segment.start);
            coordinates[end.0 as usize] = Some(segment.end);
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
        let mut roots = Vec::new();
        try_reserve_vec(&mut roots, parents.len())?;
        roots.resize(parents.len(), None);
        for node in 0..parents.len() {
            parents[node] = parents[parents[node] as usize];
            let Some(point) = coordinates[node] else {
                continue;
            };
            let root = &mut roots[parents[node] as usize];
            if root.is_none_or(|(_, prior): (usize, XY)| {
                point.x < prior.x || (point.x == prior.x && point.y < prior.y)
            }) {
                *root = Some((node, point));
            }
        }
        let mut winding = Vec::new();
        try_reserve_vec(&mut winding, self.face_quotient.len())?;
        winding.resize(self.face_quotient.len(), 0_i64);
        for (component, root) in roots.into_iter().enumerate() {
            let Some((node, point)) = root else { continue };
            let mut exterior = None;
            let mut chosen = None;
            let mut has_upper = false;
            for (index, &[start, end]) in items.iter().enumerate() {
                let (target, target_point, face) = if start.0 as usize == node {
                    (end.0, segments[index].end, sides[index][0])
                } else if end.0 as usize == node {
                    (start.0, segments[index].start, sides[index][1])
                } else {
                    continue;
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
            let exterior = exterior.expect("a component root has an incident emitted edge");
            winding.fill(0);
            // Other components supply closed face-boundary chains. Their winding
            // identifies the host without a ray-intersection coordinate or noder.
            for (index, &[start, _]) in items.iter().enumerate() {
                if parents[start.0 as usize] as usize == component {
                    continue;
                }
                let Segment { start, end } = segments[index];
                let delta = if start.y <= point.y && point.y < end.y {
                    i64::from(
                        stored_orientation_sign(start.x, start.y, end.x, end.y, point.x, point.y)
                            == ExactSign::Positive,
                    )
                } else if end.y <= point.y && point.y < start.y {
                    -i64::from(
                        stored_orientation_sign(start.x, start.y, end.x, end.y, point.x, point.y)
                            == ExactSign::Negative,
                    )
                } else {
                    0
                };
                winding[sides[index][0] as usize] += delta;
                winding[sides[index][1] as usize] -= delta;
            }
            if winding.iter().enumerate().any(|(face, &value)| {
                let expected = i64::from(face == exterior as usize) - i64::from(face == 0);
                value != expected
            }) {
                return Err(GeometryErrorKind::ExactGlobalFace { node: node as u32 }.into());
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
            && extra_protected.len() != partition.recipes.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "exact overlay protection does not match node count".into(),
            )
            .into());
        }
        let mut walks = self.boundary_walks(topology, occupied)?;
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
        try_reserve_vec(&mut protected, partition.recipes.len())?;
        protected.resize(partition.recipes.len(), false);
        if partition.protect_stored {
            for (node, recipe) in partition.recipes.iter().enumerate() {
                if partition.protect_all || matches!(recipe, NodeRecipe::Stored(_)) {
                    protected[node] = true;
                }
            }
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
            let difference = |end, start| {
                ExactDyadic::try_from_f64(end)?.try_subtract(ExactDyadic::try_from_f64(start)?)
            };
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

    fn boundary_walks(
        &self,
        topology: &IndexedTopology,
        occupied: &[bool],
    ) -> Result<Vec<Vec<u32>>> {
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, topology.targets.len())?;
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let other = topology.face_of[topology.twins[slot] as usize];
            try_push_vec(
                &mut selected,
                occupied[self.walk_faces[walk as usize] as usize]
                    && !occupied[self.walk_faces[other as usize] as usize],
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
    pub recipes: Vec<NodeRecipe>,
    pub spans: Vec<ExactSpan>,
    pub provenance: SegmentProvenance,
    pub isolated: Vec<NodeId>,
    /// Node ownership in explicit input-point occurrence order, including duplicates.
    pub point_nodes: Vec<NodeId>,
    pub point_sources: Vec<(NodeId, u32)>,
    binding_starts: Vec<u32>,
    bindings: Vec<NodeOnUnique>,
    /// The first source spelling of each stored node. `PointKey` deliberately
    /// canonicalizes signed zero for identity; emission must not do the same
    /// to an endpoint that was already present in the input.
    stored_points: Vec<Option<XY>>,
    protect_stored: bool,
    protect_all: bool,
    visibility: VisibilityIndex,
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
        let mut partition =
            partition_unique_segments(&self.unique, &self.provenance, &self.isolated, budget)?;
        partition.protect_stored = self.polygon_rings.iter().all(Vec::is_empty);
        partition.protect_all = partition.protect_stored;
        Ok(partition)
    }

    pub(in crate::geometry) fn source_segments(&self) -> &[Segment] {
        &self.source_segments
    }

    pub(in crate::geometry) fn source_operands(&self) -> &[u32] {
        &self.source_operands
    }

    fn node_bindings<'a>(
        &self,
        node: NodeId,
        partition: &'a ExactPartition<'_>,
    ) -> &'a [NodeOnUnique] {
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
        for binding in self.node_bindings(node, partition) {
            let owners = self.provenance.owners(binding.unique as usize);
            for &(source, reversed) in owners {
                let source = source as usize;
                let points = self.source_points.get(source).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition ordinate binding references an unknown source".into(),
                    )
                })?;
                let parameter = if reversed {
                    binding.parameter.reverse()?
                } else {
                    binding.parameter.clone()
                };
                let [start, end] = *points;
                if axes.has_z() {
                    z.observe(interpolate_ordinate(start.z(), end.z(), &parameter)?);
                }
                if axes.has_m() {
                    m.observe(interpolate_ordinate(start.m(), end.m(), &parameter)?);
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
        try_reserve_vec(&mut protected, partition.recipes.len())?;
        protected.resize(partition.recipes.len(), false);
        for (node, range) in partition.binding_starts.windows(2).enumerate() {
            for binding in &partition.bindings[range[0] as usize..range[1] as usize] {
                let mut source_has_ordinate = false;
                for &(source, _) in self.provenance.owners(binding.unique as usize) {
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
        rounded: &HashMap<NodeId, XY>,
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
            let point = self.resolve_node_point(
                partition,
                node,
                *rounded.get(&node).ok_or_else(|| {
                    GeometryErrorKind::Invalid("exact overlay output lost rounded XY".into())
                })?,
                declared_axes,
            )?;
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
        let rounded_segments = partition.round_incident_items(&items)?;
        let mut nodes = Vec::new();
        let node_capacity = items
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut nodes, node_capacity)?;
        let mut rounded =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        rounded
            .try_reserve(node_capacity)
            .map_err(crate::error::Error::from)?;
        for (&[start, end], segment) in items.iter().zip(&rounded_segments) {
            try_push_vec(&mut nodes, start)?;
            try_push_vec(&mut nodes, end)?;
            rounded.entry(start).or_insert(segment.start);
            rounded.entry(end).or_insert(segment.end);
        }
        let by_node = self.resolve_node_points(partition, &nodes, &rounded, declared_axes)?;
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
        try_reserve_vec(&mut degree, partition.recipes.len())?;
        degree.resize(partition.recipes.len(), 0_usize);
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
        partition.visit_face_windings(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            |face, winding| {
                selected[face as usize] = self.operand_occupied(winding, 0, false);
                Ok(())
            },
        )?;
        let mut area = crate::geometry::ExactRationalArea::new()?;
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
                    partition.homogeneous(NodeId(topology.owners[slot]))?,
                    partition.homogeneous(NodeId(topology.targets[slot]))?,
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
        for (node, point) in parts.rounded {
            rounded.entry(node).or_insert(point);
        }
        let by_node = self.resolve_node_points(partition, &nodes, &rounded, declared_axes)?;
        let point_axes = bucket_axes(
            &parts.points,
            &by_node,
            declared_axes,
            strict,
            operation.name(),
        )?;
        let mut line_nodes = Vec::new();
        try_reserve_vec(&mut line_nodes, line_node_count)?;
        for line in &parts.lines {
            line_nodes.extend(line.iter().copied());
        }
        let line_axes = bucket_axes(
            &line_nodes,
            &by_node,
            declared_axes,
            strict,
            operation.name(),
        )?;
        let mut polygon_nodes = Vec::new();
        try_reserve_vec(&mut polygon_nodes, polygon_node_count)?;
        for polygon in &parts.polygons {
            for ring in polygon {
                polygon_nodes.extend(ring.iter().copied());
            }
        }
        let polygon_axes = bucket_axes(
            &polygon_nodes,
            &by_node,
            declared_axes,
            strict,
            operation.name(),
        )?;
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
        partition.protect_stored = true;
        partition.protect_all = true;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
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
            for cycle in &cycles[face] {
                for &node in cycle {
                    if samples[face].is_none_or(|prior| {
                        partition
                            .compare_nodes(node, prior)
                            .is_ok_and(|o| o.is_lt())
                    }) {
                        samples[face] = Some(node);
                    }
                }
            }
        }
        let mut selected = Vec::new();
        try_reserve_vec(&mut selected, global.count as usize)?;
        selected.resize(global.count as usize, false);
        for face in 1..global.count as usize {
            let Some(sample) = samples[face] else {
                continue;
            };
            let mut depth = 0;
            for (other, other_cycles) in cycles.iter().enumerate().skip(1) {
                if other == face {
                    continue;
                }
                let mut contained = false;
                for cycle in other_cycles {
                    let inside = cycle_contains_node(partition, cycle, sample)?;
                    if inside {
                        contained = true;
                        break;
                    }
                }
                if contained {
                    depth += 1;
                }
            }
            selected[face] = !build_area || depth % 2 == 0;
        }
        if !build_area {
            let mut polygons = Vec::new();
            try_reserve_vec(&mut polygons, global.count as usize)?;
            for face in 1..global.count as usize {
                if samples[face].is_none() {
                    continue;
                }
                let mut faces = Vec::new();
                try_reserve_vec(&mut faces, global.count as usize)?;
                faces.resize(global.count as usize, false);
                faces[face] = true;
                let mut spans = Vec::new();
                try_reserve_vec(&mut spans, partition.spans.len())?;
                spans.resize(partition.spans.len(), false);
                let mut nodes = Vec::new();
                try_reserve_vec(&mut nodes, partition.recipes.len())?;
                nodes.resize(partition.recipes.len(), false);
                let cells = ArealCells {
                    faces,
                    spans,
                    nodes,
                };
                let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
                let (_, _, mut parts) = plan.emit_xy_parts(partition, &topology)?;
                try_reserve_vec(&mut polygons, parts.len())?;
                polygons.append(&mut parts);
            }
            return Ok(polygons);
        }
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
                try_reserve_vec(&mut values, partition.recipes.len())?;
                values.resize(partition.recipes.len(), false);
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
                try_reserve_vec(&mut values, partition.recipes.len())?;
                values.resize(partition.recipes.len(), false);
                values
            },
        };
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        let (_, _, polygons) = plan.emit_xy_parts(partition, &topology)?;
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
        for node in 0..partition.recipes.len() {
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
            .recipes
            .len()
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
        partition.visit_face_windings(
            topology,
            edges,
            global,
            winding_rings,
            self.ring_count,
            |face, winding| {
                let face = face as usize;
                for operand in 0..operands {
                    facts.face_membership[face * operands + operand] =
                        self.operand_occupied(winding, operand, odd);
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
            .recipes
            .len()
            .checked_mul(operands)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut line_boundaries = Vec::new();
        try_reserve_vec(&mut line_boundaries, cells)?;
        line_boundaries.resize(cells, false);

        let mut stored = HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        stored
            .try_reserve(partition.recipes.len())
            .map_err(crate::error::Error::from)?;
        for (index, recipe) in partition.recipes.iter().enumerate() {
            if let Some(key) = recipe.stored_key() {
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
        try_reserve_vec(&mut cells.nodes, partition.recipes.len())?;
        cells.nodes.resize(partition.recipes.len(), false);
        for face in 0..global.count as usize {
            cells.faces[face] = select_operation(
                operation,
                &facts.face_membership[face * operands..][..operands],
            );
        }

        let node_cells = partition
            .recipes
            .len()
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
        for node in 0..partition.recipes.len() {
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
        for node in 0..partition.recipes.len() {
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

    fn operand_occupied(&self, winding: &[i64], operand: usize, odd: bool) -> bool {
        self.polygon_rings[operand].iter().any(|rings| {
            let mut rings = rings.clone();
            let filled = |ring| {
                if odd {
                    winding[ring] % 2 != 0
                } else {
                    winding[ring] != 0
                }
            };
            rings.next().is_some_and(filled) && !rings.any(filled)
        })
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
            Contact::Cross { .. } | Contact::Collinear { .. } => std::ops::ControlFlow::Break(()),
        }
    })?;
    Ok(())
}

fn cycle_contains_node(
    partition: &ExactPartition<'_>,
    cycle: &[NodeId],
    query: NodeId,
) -> Result<bool> {
    let [qx, qy, qw] = partition.homogeneous(query)?;
    let mut inside = false;
    for index in 0..cycle.len() {
        let a = cycle[index];
        let b = cycle[(index + 1) % cycle.len()];
        let [ax, ay, aw] = partition.homogeneous(a)?;
        let [bx, by, bw] = partition.homogeneous(b)?;
        let y_a = try_compare_ratios(&ay, &aw, &qy, &qw)?;
        let y_b = try_compare_ratios(&by, &bw, &qy, &qw)?;
        let dx = bx.try_product(&aw)?.try_subtract(ax.try_product(&bw)?)?;
        let dy = by.try_product(&aw)?.try_subtract(ay.try_product(&bw)?)?;
        let qx_rel = qx.try_product(&aw)?.try_subtract(ax.try_product(&qw)?)?;
        let qy_rel = qy.try_product(&aw)?.try_subtract(ay.try_product(&qw)?)?;
        let mut orientation = dx
            .try_product(&qy_rel)?
            .try_subtract(dy.try_product(&qx_rel)?)?
            .sign();
        if matches!(
            (bw.sign(), qw.sign()),
            (ExactSign::Negative, ExactSign::Positive) | (ExactSign::Positive, ExactSign::Negative)
        ) {
            orientation = match orientation {
                ExactSign::Negative => ExactSign::Positive,
                ExactSign::Zero => ExactSign::Zero,
                ExactSign::Positive => ExactSign::Negative,
            };
        }
        if orientation == ExactSign::Zero
            && try_compare_ratios(&qx, &qw, &ax, &aw)? != Ordering::Less
            && try_compare_ratios(&qx, &qw, &bx, &bw)? != Ordering::Greater
            && try_compare_ratios(&qy, &qw, &ay, &aw)? != Ordering::Less
            && try_compare_ratios(&qy, &qw, &by, &bw)? != Ordering::Greater
        {
            return Ok(false);
        }
        let a_is_below = y_a != Ordering::Greater;
        let b_is_below = y_b != Ordering::Greater;
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

impl ExactPartition<'_> {
    pub(in crate::geometry) fn node_xy(&self, node: NodeId) -> Result<XY> {
        Ok(match self.recipe(node) {
            NodeRecipe::Stored(key) => {
                self.stored_points[node.0 as usize].unwrap_or_else(|| key.xy())
            },
            NodeRecipe::Constructed { .. } => {
                let [x, y, w] = self.homogeneous(node)?;
                let x = round_ratio(&x, &w)?;
                let y = round_ratio(&y, &w)?;
                XY::new(
                    if x == 0.0 { 0.0 } else { x },
                    if y == 0.0 { 0.0 } else { y },
                )
            },
        })
    }

    pub(in crate::geometry) fn stored_node(&self, point: XY) -> Option<NodeId> {
        let key = PointKey::new(point);
        self.recipes
            .iter()
            .position(|recipe| matches!(recipe, NodeRecipe::Stored(candidate) if *candidate == key))
            .map(|node| NodeId(node as u32))
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
            let start = self.stored_node(segment.start).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "exact partition source endpoint has no stored node".into(),
                )
            })?;
            let end = self.stored_node(segment.end).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "exact partition source endpoint has no stored node".into(),
                )
            })?;
            let mut remaining = std::mem::take(&mut views[source]);
            let mut ordered = Vec::new();
            try_reserve_vec(&mut ordered, remaining.len())?;
            let mut current = start;
            while !remaining.is_empty() {
                let index = remaining.iter().position(|&(span, reversed)| {
                    let span = self.spans[span as usize];
                    if reversed {
                        span.end == current
                    } else {
                        span.start == current
                    }
                });
                let Some(index) = index else {
                    return Err(GeometryErrorKind::Invalid(
                        "exact partition source atoms are not a contiguous run".into(),
                    )
                    .into());
                };
                let (span_id, reversed) = remaining.swap_remove(index);
                let span = self.spans[span_id as usize];
                current = if reversed { span.start } else { span.end };
                try_push_vec(&mut ordered, (span_id, reversed))?;
            }
            if current != end {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition source atoms do not reach their endpoint".into(),
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
        try_reserve_vec(&mut node_pin, self.recipes.len())?;
        node_pin.resize(self.recipes.len(), false);
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

    pub(in crate::geometry) fn recipe(&self, node: NodeId) -> &NodeRecipe {
        &self.recipes[node.0 as usize]
    }

    pub(in crate::geometry) fn owners(&self, span: usize) -> &[SegmentSource] {
        self.provenance.owners(span)
    }

    /// Temporary homogeneous coordinates; node identity remains the recipe index.
    pub(in crate::geometry) fn homogeneous(&self, node: NodeId) -> Result<[ExactDyadic; 3]> {
        match self.recipe(node) {
            NodeRecipe::Stored(key) => {
                let point = self.stored_points[node.0 as usize].unwrap_or_else(|| key.xy());
                Ok([
                    ExactDyadic::try_from_f64(point.x)?,
                    ExactDyadic::try_from_f64(point.y)?,
                    ExactDyadic::try_from_f64(1.0)?,
                ])
            },
            NodeRecipe::Constructed { unique, num, den } => {
                let source = self.unique[*unique as usize];
                let coordinate = |start, end| -> Result<ExactDyadic> {
                    let start = ExactDyadic::try_from_f64(start)?;
                    let delta = ExactDyadic::try_from_f64(end)?.try_subtract(start.try_clone()?)?;
                    den.try_product(&start)?.try_add(num.try_product(&delta)?)
                };
                Ok([
                    coordinate(source.start.x, source.end.x)?,
                    coordinate(source.start.y, source.end.y)?,
                    den.try_clone()?,
                ])
            },
        }
    }

    fn node_in_bounds(&self, node: NodeId, bounds: Bounds) -> Result<bool> {
        let [x, y, w] = self.homogeneous(node)?;
        let one = ExactDyadic::try_from_f64(1.0)?;
        let minx = ExactDyadic::try_from_f64(bounds.minx())?;
        let miny = ExactDyadic::try_from_f64(bounds.miny())?;
        let maxx = ExactDyadic::try_from_f64(bounds.maxx())?;
        let maxy = ExactDyadic::try_from_f64(bounds.maxy())?;
        Ok(try_compare_ratios(&x, &w, &minx, &one)? != Ordering::Less
            && try_compare_ratios(&x, &w, &maxx, &one)? != Ordering::Greater
            && try_compare_ratios(&y, &w, &miny, &one)? != Ordering::Less
            && try_compare_ratios(&y, &w, &maxy, &one)? != Ordering::Greater)
    }

    fn round_nodes(&self, nodes: &[NodeId]) -> Result<Vec<XY>> {
        let mut coordinates = Vec::new();
        try_reserve_vec(&mut coordinates, nodes.len())?;
        for &node in nodes {
            try_push_vec(&mut coordinates, self.node_xy(node)?)?;
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

    fn round_incident_items(&self, items: &[[NodeId; 2]]) -> Result<Vec<Segment>> {
        let mut nodes = Vec::new();
        let node_capacity = items
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut nodes, node_capacity)?;
        let mut indices =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        indices
            .try_reserve(node_capacity)
            .map_err(|_| ResourceError::Allocation)?;
        for item in items {
            for &node in item {
                if indices.contains_key(&node) {
                    continue;
                }
                indices.insert(node, nodes.len());
                try_push_vec(&mut nodes, node)?;
            }
        }
        let coordinates = self.round_nodes(&nodes)?;
        Self::check_node_injectivity(&nodes, &coordinates)?;
        let mut segments = Vec::new();
        try_reserve_vec(&mut segments, items.len())?;
        for &[start, end] in items {
            try_push_vec(&mut segments, Segment {
                start: coordinates[indices[&start]],
                end: coordinates[indices[&end]],
            })?;
        }
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
        Self::check_emitted_fans(&topology, &darts, &segments)?;

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
        let departure_capacity = darts
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut departures =
            HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        departures
            .try_reserve(departure_capacity)
            .map_err(|_| ResourceError::Allocation)?;
        for (&[start, end], segment) in darts.iter().zip(segments) {
            let prior_start = departures.insert(start, (segment.start, segment.end));
            let prior_end = departures.insert(end, (segment.end, segment.start));
            debug_assert!(prior_start.is_none() && prior_end.is_none());
        }
        for (node, range) in topology.starts.windows(2).enumerate() {
            let mut first = None;
            let mut previous = None;
            let mut descents = 0;
            let mut count = 0;
            for slot in range[0] as usize..range[1] as usize {
                let target = slot as u32;
                let Some((origin, point)) = departures.remove(&target) else {
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
            departures.is_empty(),
            "emitted chords must retain exact dart identities"
        );
        Ok(())
    }

    pub(in crate::geometry) fn compare_nodes(
        &self,
        left: NodeId,
        right: NodeId,
    ) -> Result<Ordering> {
        if left == right {
            return Ok(Ordering::Equal);
        }
        let [lx, ly, lw] = self.homogeneous(left)?;
        let [rx, ry, rw] = self.homogeneous(right)?;
        let x = try_compare_ratios(&lx, &lw, &rx, &rw)?;
        if x == Ordering::Equal {
            try_compare_ratios(&ly, &lw, &ry, &rw)
        } else {
            Ok(x)
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact rounded coordinates are ordered by their stored binary64 values to keep emitted topology deterministic"
    )]
    fn compare_departures(&self, origin: NodeId, left: usize, right: usize) -> Result<Ordering> {
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
            return Ok(halves);
        }
        let difference = |end, start| {
            ExactDyadic::try_from_f64(end)?.try_subtract(ExactDyadic::try_from_f64(start)?)
        };
        let ax = difference(b.x, a.x)?;
        let ay = difference(b.y, a.y)?;
        let bx = difference(d.x, c.x)?;
        let by = difference(d.y, c.y)?;
        let sign = ax
            .try_product(&by)?
            .try_subtract(ay.try_product(&bx)?)?
            .sign();
        debug_assert!(
            left == right || sign != ExactSign::Zero,
            "distinct atoms share a departure ray"
        );
        Ok(match sign {
            ExactSign::Positive => Ordering::Less,
            ExactSign::Negative => Ordering::Greater,
            ExactSign::Zero => Ordering::Equal,
        })
    }

    pub(in crate::geometry) fn local_topology(&self) -> Result<(IndexedTopology, Vec<u32>)> {
        let vertices = self.recipes.len();
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
        let mut row = Vec::new();
        for node in 0..vertices {
            let range = topology.starts[node] as usize..topology.starts[node + 1] as usize;
            // Visibility wedges need the angular origin even for degree two.
            if range.len() <= 1 {
                continue;
            }
            row.clear();
            try_reserve_vec(&mut row, range.len())?;
            row.extend_from_slice(&edge_of_slot[range.clone()]);
            let order = try_sort_indices(row.len(), |left, right| {
                self.compare_departures(
                    NodeId(node as u32),
                    row[left as usize] as usize,
                    row[right as usize] as usize,
                )
            })?;
            for (slot, index) in range.zip(order) {
                edge_of_slot[slot] = row[index as usize];
            }
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
        let [_, ry, rw] = self.homogeneous(root)?;
        for slot in range {
            let target = NodeId(topology.targets[slot]);
            let [_, ty, tw] = self.homogeneous(target)?;
            let upper = try_compare_ratios(&ty, &tw, &ry, &rw)? != Ordering::Less;
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
            if self.compare_departures(root, edges[prior] as usize, edges[slot] as usize)?
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
        for root in minima {
            let host = self
                .first_left_contact(topology, root)?
                .map_or(0, |hit| self.contact_walk(topology, edges, hit) + 1);
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

    fn visit_face_windings(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        faces: &GlobalFaces,
        source_rings: &[Option<u32>],
        ring_count: usize,
        mut visit: impl FnMut(u32, &[i64]) -> Result<()>,
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
        let cross = |winding: &mut [i64], slot: u32, direction: i64| {
            let slot = slot as usize;
            let edge = edges[slot] as usize;
            let backward = topology.owners[slot] != self.spans[edge].start.0;
            for &(source, reversed) in self.owners(edge) {
                if let Some(ring) = source_rings[source as usize] {
                    winding[ring as usize] += if reversed ^ backward {
                        direction
                    } else {
                        -direction
                    };
                }
            }
        };
        let mut visited = Vec::new();
        try_reserve_vec(&mut visited, count)?;
        visited.resize(count, false);
        let mut stack = Vec::new();
        try_reserve_vec(&mut stack, count)?;
        visited[0] = true;
        visit(0, &winding)?;
        try_push_vec(&mut stack, (0_u32, starts[0], None))?;
        while let Some((face, next, incoming)) = stack.last_mut() {
            if *next == starts[*face as usize + 1] {
                if let Some(slot) = *incoming {
                    cross(&mut winding, slot, -1);
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
            cross(&mut winding, slot, 1);
            visited[neighbor as usize] = true;
            visit(neighbor, &winding)?;
            try_push_vec(
                &mut stack,
                (neighbor, starts[neighbor as usize], Some(slot)),
            )?;
        }
        debug_assert!(visited.iter().all(|&seen| seen));
        debug_assert!(winding.iter().all(|&value| value == 0));
        Ok(())
    }

    fn contact_walk(&self, topology: &IndexedTopology, edges: &[u32], hit: VisibilityHit) -> u32 {
        let slot = match hit {
            VisibilityHit::Node(node) => {
                // A first leftward contact cannot have a departure toward the
                // query: that atom would have supplied a closer contact.
                topology.starts[node.0 as usize + 1] as usize - 1
            },
            VisibilityHit::Span(edge) => {
                let span = self.spans[edge as usize];
                let source = self.unique[span.unique as usize];
                let origin = if source.end.y > source.start.y {
                    span.end
                } else {
                    span.start
                };
                (topology.starts[origin.0 as usize] as usize
                    ..topology.starts[origin.0 as usize + 1] as usize)
                    .find(|&slot| edges[slot] == edge)
                    .expect("contact atom has a departure slot")
            },
        };
        topology.face_of[slot]
    }

    fn first_left_contact(
        &self,
        topology: &IndexedTopology,
        query: NodeId,
    ) -> Result<Option<VisibilityHit>> {
        let component = topology.component_of[query.0 as usize];
        let [qx, qy, qw] = self.homogeneous(query)?;
        let (_, query_x_hi) = outward_ratio_bounds(&qx, &qw)?;
        let (query_y_lo, query_y_hi) = outward_ratio_bounds(&qy, &qw)?;
        let mut nearest: Option<(ExactDyadic, ExactDyadic, f64, VisibilityHit)> = None;
        let candidate_range = self.visibility.candidate_range(query_y_lo, query_y_hi);
        let mut candidates = Vec::new();
        try_reserve_vec(&mut candidates, candidate_range.len())?;
        candidates.extend_from_slice(&self.visibility.entries[candidate_range]);
        // The exact path retained the first hit on an equal-x tie. Restore
        // span order after the y-index walk so that tie handling is unchanged.
        candidates.sort_unstable_by_key(|entry| entry.edge);
        for entry in candidates {
            let edge = entry.edge as usize;
            let span = self.spans[edge];
            if topology.component_of[span.start.0 as usize] == component {
                continue;
            }
            if query_y_hi < entry.extent.y_lo || query_y_lo > entry.extent.y_hi {
                continue;
            }
            // The exact path rejects every contact at or right of the query.
            // A lower extent bound at or beyond the query's upper enclosure
            // proves that rejection without entering exact arithmetic.
            if entry.extent.x_lo >= query_x_hi {
                continue;
            }
            // A leftward ray keeps the greatest hit below the query. An extent
            // entirely to its left cannot improve that exact nearest contact.
            if let Some((_, _, nearest_x_lo, _)) = &nearest
                && entry.extent.x_hi < *nearest_x_lo
            {
                continue;
            }
            let [ax, ay, aw] = self.homogeneous(span.start)?;
            let [bx, by, bw] = self.homogeneous(span.end)?;
            let a_side = try_compare_ratios(&ay, &aw, &qy, &qw)?;
            let b_side = try_compare_ratios(&by, &bw, &qy, &qw)?;
            let (x, w, hit) = if a_side == Ordering::Equal && b_side == Ordering::Equal {
                if try_compare_ratios(&ax, &aw, &bx, &bw)? == Ordering::Greater {
                    (ax, aw, VisibilityHit::Node(span.start))
                } else {
                    (bx, bw, VisibilityHit::Node(span.end))
                }
            } else if a_side == Ordering::Equal {
                (ax, aw, VisibilityHit::Node(span.start))
            } else if b_side == Ordering::Equal {
                (bx, bw, VisibilityHit::Node(span.end))
            } else if a_side == b_side {
                continue;
            } else {
                let source = self.unique[span.unique as usize];
                let sx = ExactDyadic::try_from_f64(source.start.x)?;
                let sy = ExactDyadic::try_from_f64(source.start.y)?;
                let dx = ExactDyadic::try_from_f64(source.end.x)?.try_subtract(sx.try_clone()?)?;
                let dy = ExactDyadic::try_from_f64(source.end.y)?.try_subtract(sy.try_clone()?)?;
                let w = dy.try_product(&qw)?;
                let height = qy.try_clone()?.try_subtract(sy.try_product(&qw)?)?;
                let x = sx.try_product(&w)?.try_add(dx.try_product(&height)?)?;
                (x, w, VisibilityHit::Span(entry.edge))
            };
            if try_compare_ratios(&x, &w, &qx, &qw)? != Ordering::Less {
                continue;
            }
            if let Some((nx, nw, ..)) = &nearest
                && try_compare_ratios(&x, &w, nx, nw)? != Ordering::Greater
            {
                continue;
            }
            let nearest_x_lo = outward_ratio_bounds(&x, &w)?.0;
            nearest = Some((x, w, nearest_x_lo, hit));
        }
        Ok(nearest.map(|(_, _, _, hit)| hit))
    }
}

#[derive(Clone, Debug)]
enum SourceParam {
    Start,
    End,
    Ratio { num: ExactDyadic, den: ExactDyadic },
}

#[derive(Clone, Debug)]
struct NodeOnUnique {
    unique: u32,
    parameter: SourceParam,
}

impl SourceParam {
    fn reverse(&self) -> Result<Self> {
        Ok(match self {
            Self::Start => Self::End,
            Self::End => Self::Start,
            Self::Ratio { num, den } => Self::Ratio {
                num: den.try_clone()?.try_subtract(num.try_clone()?)?,
                den: den.try_clone()?,
            },
        })
    }

    fn cmp(&self, other: &Self, zero: &ExactDyadic, one: &ExactDyadic) -> Result<Ordering> {
        match (self, other) {
            (Self::Start, Self::Start) | (Self::End, Self::End) => Ok(Ordering::Equal),
            (Self::Start, Self::End) => Ok(Ordering::Less),
            (Self::End, Self::Start) => Ok(Ordering::Greater),
            (Self::Start, Self::Ratio { num, den }) => try_compare_ratios(zero, one, num, den),
            (Self::End, Self::Ratio { num, den }) => try_compare_ratios(one, one, num, den),
            (Self::Ratio { num, den }, Self::Start) => try_compare_ratios(num, den, zero, one),
            (Self::Ratio { num, den }, Self::End) => try_compare_ratios(num, den, one, one),
            (
                Self::Ratio {
                    num: left_n,
                    den: left_d,
                },
                Self::Ratio {
                    num: right_n,
                    den: right_d,
                },
            ) => try_compare_ratios(left_n, left_d, right_n, right_d),
        }
    }

    fn is_open_unit(&self, zero: &ExactDyadic, one: &ExactDyadic) -> Result<bool> {
        match self {
            Self::Start | Self::End => Ok(false),
            Self::Ratio { num, den } => Ok(try_compare_ratios(num, den, zero, one)?
                == Ordering::Greater
                && try_compare_ratios(num, den, one, one)? == Ordering::Less),
        }
    }
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

fn interpolate_ordinate(
    start: Option<f64>,
    end: Option<f64>,
    parameter: &SourceParam,
) -> Result<Option<f64>> {
    let (Some(start), Some(end)) = (start, end) else {
        return Ok(None);
    };
    Ok(match parameter {
        SourceParam::Start => Some(start),
        SourceParam::End => Some(end),
        SourceParam::Ratio { num, den } => {
            if start.to_bits() == end.to_bits() {
                Some(start)
            } else {
                let start = ExactDyadic::try_from_f64(start)?;
                let end = ExactDyadic::try_from_f64(end)?;
                let left = den.try_clone()?.try_subtract(num.try_clone()?)?;
                let numerator = start.try_product(&left)?.try_add(end.try_product(num)?)?;
                Some(round_ratio(&numerator, den)?)
            }
        },
    })
}

struct SourceCut {
    param: SourceParam,
    handle: u32,
}

#[derive(Clone, Copy)]
enum HandleKind {
    Stored(PointKey),
    Constructed { unique: u32, cut: u32 },
}

struct Forest {
    parent: Vec<u32>,
    kind: Vec<HandleKind>,
}

impl Forest {
    fn with_capacity(capacity: usize) -> Result<Self> {
        let mut parent = Vec::new();
        let mut kind = Vec::new();
        try_reserve_vec(&mut parent, capacity)?;
        try_reserve_vec(&mut kind, capacity)?;
        Ok(Self { parent, kind })
    }

    fn find(&mut self, mut node: u32) -> u32 {
        while self.parent[node as usize] != node {
            self.parent[node as usize] = self.parent[self.parent[node as usize] as usize];
            node = self.parent[node as usize];
        }
        node
    }

    fn push(&mut self, kind: HandleKind) -> Result<u32> {
        let id = u32::try_from(self.parent.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut self.parent, 1)?;
        try_reserve_vec(&mut self.kind, 1)?;
        try_push_vec(&mut self.parent, id)?;
        try_push_vec(&mut self.kind, kind)?;
        Ok(id)
    }

    fn union(&mut self, a: u32, b: u32) -> Result<()> {
        let a = self.find(a);
        let b = self.find(b);
        if a == b {
            return Ok(());
        }
        match (&self.kind[a as usize], &self.kind[b as usize]) {
            (HandleKind::Stored(left), HandleKind::Stored(right)) if left != right => Err(
                GeometryErrorKind::Invalid("exact partition unioned distinct stored points".into())
                    .into(),
            ),
            (HandleKind::Stored(_), HandleKind::Stored(_) | HandleKind::Constructed { .. }) => {
                self.parent[b as usize] = a;
                Ok(())
            },
            (HandleKind::Constructed { .. }, HandleKind::Stored(_)) => {
                self.parent[a as usize] = b;
                Ok(())
            },
            (
                HandleKind::Constructed { unique: left, .. },
                HandleKind::Constructed { unique: right, .. },
            ) => {
                if right < left {
                    self.parent[a as usize] = b;
                } else {
                    self.parent[b as usize] = a;
                }
                Ok(())
            },
        }
    }
}

pub(in crate::geometry) fn partition_unique_segments<'a>(
    unique: &'a [Segment],
    provenance: &SegmentProvenance,
    isolated: &[XY],
    budget: &mut ExpansionBudget,
) -> Result<ExactPartition<'a>> {
    debug_assert_eq!(provenance.len(), unique.len());
    let zero = ExactDyadic::try_from_f64(0.0)?;
    let one = ExactDyadic::try_from_f64(1.0)?;

    let endpoint_capacity = unique
        .len()
        .checked_mul(2)
        .and_then(|count| count.checked_add(isolated.len()))
        .ok_or(ResourceError::SizeOverflow)?;
    let mut endpoint_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut stored_coordinates: HashMap<PointKey, XY> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    endpoint_of
        .try_reserve(endpoint_capacity)
        .map_err(crate::error::Error::from)?;
    stored_coordinates
        .try_reserve(endpoint_capacity)
        .map_err(crate::error::Error::from)?;
    let mut forest = Forest::with_capacity(endpoint_capacity)?;

    let intern_stored = |point: XY,
                         endpoint_of: &mut HashMap<PointKey, u32>,
                         stored_coordinates: &mut HashMap<PointKey, XY>,
                         forest: &mut Forest|
     -> Result<u32> {
        let key = PointKey::new(point);
        if let Some(&id) = endpoint_of.get(&key) {
            return Ok(id);
        }
        let id = forest.push(HandleKind::Stored(key))?;
        endpoint_of.insert(key, id);
        stored_coordinates.insert(key, point);
        Ok(id)
    };

    let mut start_handle = Vec::new();
    let mut end_handle = Vec::new();
    try_reserve_vec(&mut start_handle, unique.len())?;
    try_reserve_vec(&mut end_handle, unique.len())?;
    for &segment in unique {
        try_push_vec(
            &mut start_handle,
            intern_stored(
                segment.start,
                &mut endpoint_of,
                &mut stored_coordinates,
                &mut forest,
            )?,
        )?;
        try_push_vec(
            &mut end_handle,
            intern_stored(
                segment.end,
                &mut endpoint_of,
                &mut stored_coordinates,
                &mut forest,
            )?,
        )?;
    }
    let mut cuts: Vec<Vec<SourceCut>> = Vec::new();
    try_reserve_vec(&mut cuts, unique.len())?;
    for _ in 0..unique.len() {
        try_push_vec(&mut cuts, Vec::new())?;
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
        let handle = intern_stored(
            point,
            &mut endpoint_of,
            &mut stored_coordinates,
            &mut forest,
        )?;
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
    let unique_len = unique.len();
    let mut pair_error = None;
    match for_each_candidate_pair(&pool, single_chain, |left, right| {
        match visit_pool_pair(
            unique,
            unique_len,
            left,
            right,
            &start_handle,
            &end_handle,
            &extra_handles,
            &pool,
            &mut cuts,
            &mut forest,
            &zero,
            &one,
        ) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => {
                pair_error = Some(error);
                ControlFlow::Break(())
            },
        }
    })? {
        ControlFlow::Break(()) => {
            if let Some(error) = pair_error {
                return Err(error);
            }
        },
        ControlFlow::Continue(()) => {},
    }

    let mut order: Vec<Vec<u32>> = Vec::new();
    try_reserve_vec(&mut order, unique.len())?;
    for source in 0..unique.len() {
        if !same_point(unique[source].start, unique[source].end) {
            push_contact_cut(&mut cuts[source], SourceParam::Start, start_handle[source])?;
            push_contact_cut(&mut cuts[source], SourceParam::End, end_handle[source])?;
        }
        try_push_vec(
            &mut order,
            try_sort_indices(cuts[source].len(), |left, right| {
                cuts[source][left as usize].param.cmp(
                    &cuts[source][right as usize].param,
                    &zero,
                    &one,
                )
            })?,
        )?;
        union_equal_params(&cuts[source], &order[source], &mut forest, &zero, &one)?;
    }

    let mut compact_of: Vec<Option<u32>> = Vec::new();
    try_reserve_vec(&mut compact_of, forest.parent.len())?;
    compact_of.resize(forest.parent.len(), None);
    let mut recipes: Vec<NodeRecipe> = Vec::new();
    let mut stored_points: Vec<Option<XY>> = Vec::new();
    for handle in 0..forest.parent.len() {
        let root = forest.find(handle as u32);
        if compact_of[root as usize].is_some() {
            continue;
        }
        let id = u32::try_from(recipes.len()).map_err(|_| ResourceError::SizeOverflow)?;
        match forest.kind[root as usize] {
            HandleKind::Stored(key) => {
                try_reserve_vec(&mut recipes, 1)?;
                try_push_vec(&mut recipes, NodeRecipe::Stored(key))?;
                try_reserve_vec(&mut stored_points, 1)?;
                try_push_vec(&mut stored_points, stored_coordinates.get(&key).copied())?;
            },
            HandleKind::Constructed { unique, cut } => {
                budget.add(1)?;
                let param = cuts[unique as usize][cut as usize].param.clone();
                let SourceParam::Ratio { num, den } = param else {
                    return Err(GeometryErrorKind::Invalid(
                        "constructed partition node has no source-parameter recipe".into(),
                    )
                    .into());
                };
                try_reserve_vec(&mut recipes, 1)?;
                try_push_vec(&mut recipes, NodeRecipe::Constructed { unique, num, den })?;
                try_reserve_vec(&mut stored_points, 1)?;
                try_push_vec(&mut stored_points, None)?;
            },
        }
        compact_of[root as usize] = Some(id);
    }
    for handle in 0..forest.parent.len() {
        let root = forest.find(handle as u32);
        compact_of[handle] = compact_of[root as usize];
    }

    let mut binding_pairs: Vec<(u32, NodeOnUnique)> = Vec::new();
    for (unique_index, source_cuts) in cuts.iter().enumerate() {
        let unique_index = u32::try_from(unique_index).map_err(|_| ResourceError::SizeOverflow)?;
        for cut in source_cuts {
            let root = forest.find(cut.handle);
            let node = compact_of[root as usize].ok_or_else(|| {
                GeometryErrorKind::Invalid("partition binding was not compacted".into())
            })?;
            try_reserve_vec(&mut binding_pairs, 1)?;
            try_push_vec(
                &mut binding_pairs,
                (node, NodeOnUnique {
                    unique: unique_index,
                    parameter: cut.param.clone(),
                }),
            )?;
        }
    }
    binding_pairs.sort_unstable_by_key(|(node, binding)| (*node, binding.unique));
    binding_pairs.dedup_by(|left, right| left.0 == right.0 && left.1.unique == right.1.unique);
    let mut binding_counts = Vec::new();
    try_reserve_vec(&mut binding_counts, recipes.len())?;
    binding_counts.resize(recipes.len(), 0_u32);
    for &(node, _) in &binding_pairs {
        binding_counts[node as usize] = binding_counts[node as usize]
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
    }
    let mut binding_starts = Vec::new();
    try_reserve_vec(
        &mut binding_starts,
        recipes
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?,
    )?;
    try_push_vec(&mut binding_starts, 0)?;
    for count in binding_counts {
        let next = binding_starts
            .last()
            .copied()
            .unwrap_or(0_u32)
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
        try_push_vec(&mut binding_starts, next)?;
    }
    let mut bindings = Vec::new();
    try_reserve_vec(&mut bindings, binding_pairs.len())?;
    bindings.extend(binding_pairs.into_iter().map(|(_, binding)| binding));

    let mut intervals: Vec<(u32, u32, u32)> = Vec::new();
    for source in 0..unique.len() {
        if same_point(unique[source].start, unique[source].end) {
            continue;
        }
        let mut prev: Option<u32> = None;
        for &index in &order[source] {
            let handle = forest.find(cuts[source][index as usize].handle);
            let id = compact_of[handle as usize].ok_or_else(|| {
                GeometryErrorKind::Invalid("partition handle was not compacted".into())
            })?;
            if prev == Some(id) {
                continue;
            }
            if let Some(start) = prev
                && start != id
            {
                try_reserve_vec(&mut intervals, 1)?;
                try_push_vec(&mut intervals, (source as u32, start, id))?;
            }
            prev = Some(id);
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
    try_reserve_vec(&mut degree, recipes.len())?;
    degree.resize(recipes.len(), 0);
    for span in &spans {
        degree[span.start.0 as usize] = degree[span.start.0 as usize].saturating_add(1);
        degree[span.end.0 as usize] = degree[span.end.0 as usize].saturating_add(1);
    }
    let mut isolated_nodes = Vec::new();
    for (index, &count) in degree.iter().enumerate() {
        if count == 0 {
            try_reserve_vec(&mut isolated_nodes, 1)?;
            try_push_vec(&mut isolated_nodes, NodeId(index as u32))?;
        }
    }

    let mut point_nodes = Vec::new();
    try_reserve_vec(&mut point_nodes, point_handles.len())?;
    for handle in point_handles {
        let node = compact_of[handle as usize].ok_or_else(|| {
            GeometryErrorKind::Invalid("point source handle was not compacted".into())
        })?;
        try_push_vec(&mut point_nodes, NodeId(node))?;
    }

    let mut point_sources = Vec::new();
    for (source, segment) in unique.iter().enumerate() {
        if same_point(segment.start, segment.end) {
            let node =
                NodeId(compact_of[start_handle[source] as usize].expect("compacted endpoint"));
            let owners = provenance.owners(source);
            try_reserve_vec(&mut point_sources, owners.len())?;
            point_sources.extend(owners.iter().map(|&(ordinal, _)| (node, ordinal)));
        }
    }

    let mut partition = ExactPartition {
        unique,
        recipes,
        spans,
        provenance: atom_provenance,
        isolated: isolated_nodes,
        point_nodes,
        point_sources,
        binding_starts,
        bindings,
        stored_points,
        protect_stored: false,
        protect_all: false,
        visibility: VisibilityIndex::default(),
    };
    partition.visibility = VisibilityIndex::build(&partition)?;
    Ok(partition)
}

fn push_contact_cut(cuts: &mut Vec<SourceCut>, param: SourceParam, handle: u32) -> Result<()> {
    try_reserve_vec(cuts, 1)?;
    try_push_vec(cuts, SourceCut { param, handle })?;
    Ok(())
}

fn push_point_on_source(
    segment: Segment,
    point: XY,
    handle: u32,
    cuts: &mut Vec<SourceCut>,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    if same_point(segment.start, segment.end) {
        return Ok(());
    }
    if !point_in_segment_envelope(point, segment) {
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
    if side != ExactSign::Zero {
        return Ok(());
    }
    let param = param_on_segment(segment, point)?;
    if param.is_open_unit(zero, one)? {
        push_contact_cut(cuts, param, handle)?;
    }
    Ok(())
}

const fn pair_index(which: u32, left: usize, right: usize) -> usize {
    if which == 0 { left } else { right }
}

const fn endpoint_handle(
    source: usize,
    at_start: bool,
    start_handle: &[u32],
    end_handle: &[u32],
) -> u32 {
    if at_start {
        start_handle[source]
    } else {
        end_handle[source]
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "pair visit needs the unique pool, endpoint handles, and cut sinks together"
)]
fn visit_pool_pair(
    unique: &[Segment],
    unique_len: usize,
    left: usize,
    right: usize,
    start_handle: &[u32],
    end_handle: &[u32],
    extra_handles: &[u32],
    pool: &[Segment],
    cuts: &mut [Vec<SourceCut>],
    forest: &mut Forest,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    match (left < unique_len, right < unique_len) {
        (true, true) => classify_pair_into(
            unique,
            left,
            right,
            start_handle,
            end_handle,
            cuts,
            forest,
            zero,
            one,
        ),
        (true, false) => {
            let handle = extra_handles[right - unique_len];
            push_point_on_source(
                unique[left],
                pool[right].start,
                handle,
                &mut cuts[left],
                zero,
                one,
            )
        },
        (false, true) => {
            let handle = extra_handles[left - unique_len];
            push_point_on_source(
                unique[right],
                pool[left].start,
                handle,
                &mut cuts[right],
                zero,
                one,
            )
        },
        (false, false) => Ok(()),
    }
}

fn classify_pair_into(
    unique: &[Segment],
    left: usize,
    right: usize,
    start_handle: &[u32],
    end_handle: &[u32],
    cuts: &mut [Vec<SourceCut>],
    forest: &mut Forest,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    let a = unique[left];
    let b = unique[right];
    if same_point(a.start, a.end) {
        return push_point_on_source(b, a.start, start_handle[left], &mut cuts[right], zero, one);
    }
    if same_point(b.start, b.end) {
        return push_point_on_source(a, b.start, start_handle[right], &mut cuts[left], zero, one);
    }
    if segment_envelopes_disjoint(a, b) {
        return Ok(());
    }
    match classify_pair(a, b, zero, one)? {
        PairContact::None => Ok(()),
        PairContact::Touch {
            source,
            handle_source,
            at_start,
            param,
        } => {
            let source = pair_index(source, left, right);
            let handle_source = pair_index(handle_source, left, right);
            let handle = endpoint_handle(handle_source, at_start, start_handle, end_handle);
            push_contact_cut(&mut cuts[source], param, handle)
        },
        PairContact::Overlap { cuts: overlap } => {
            for cut in overlap {
                let source = pair_index(cut.source, left, right);
                let handle_source = pair_index(cut.handle_source, left, right);
                let handle = endpoint_handle(handle_source, cut.at_start, start_handle, end_handle);
                push_contact_cut(&mut cuts[source], cut.param, handle)?;
            }
            Ok(())
        },
        PairContact::Cross { t, u } => {
            let unique_index = left.min(right);
            let unique = u32::try_from(unique_index).map_err(|_| ResourceError::SizeOverflow)?;
            let (first_param, second, second_param) = if unique_index == left {
                (t, right, u)
            } else {
                (u, left, t)
            };
            let cut =
                u32::try_from(cuts[unique_index].len()).map_err(|_| ResourceError::SizeOverflow)?;
            let handle = forest.push(HandleKind::Constructed { unique, cut })?;
            push_contact_cut(&mut cuts[unique_index], first_param, handle)?;
            push_contact_cut(&mut cuts[second], second_param, handle)
        },
    }
}

fn union_equal_params(
    cuts: &[SourceCut],
    order: &[u32],
    forest: &mut Forest,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    let mut index = 0;
    while index < order.len() {
        let mut end = index + 1;
        while end < order.len()
            && cuts[order[end] as usize]
                .param
                .cmp(&cuts[order[index] as usize].param, zero, one)?
                == Ordering::Equal
        {
            forest.union(
                cuts[order[index] as usize].handle,
                cuts[order[end] as usize].handle,
            )?;
            end += 1;
        }
        index = end;
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

fn try_sort_indices(
    n: usize,
    mut compare: impl FnMut(u32, u32) -> Result<Ordering>,
) -> Result<Vec<u32>> {
    let count = u32::try_from(n).map_err(|_| ResourceError::SizeOverflow)?;
    let mut src = Vec::new();
    try_reserve_vec(&mut src, n)?;
    src.extend(0..count);
    if n < 2 {
        return Ok(src);
    }
    let mut dst = Vec::new();
    try_reserve_vec(&mut dst, n)?;
    dst.resize(n, 0);
    let mut width = 1;
    while width < n {
        let mut start = 0;
        while start < n {
            let mid = start.saturating_add(width).min(n);
            let end = mid.saturating_add(width).min(n);
            merge_indices(&src, &mut dst, start, mid, end, &mut compare)?;
            start = end;
        }
        mem::swap(&mut src, &mut dst);
        width = width.saturating_mul(2);
    }
    Ok(src)
}

fn merge_indices(
    src: &[u32],
    dst: &mut [u32],
    start: usize,
    mid: usize,
    end: usize,
    compare: &mut impl FnMut(u32, u32) -> Result<Ordering>,
) -> Result<()> {
    let mut left = start;
    let mut right = mid;
    let mut out = start;
    while left < mid && right < end {
        let order = compare(src[left], src[right])?;
        if order.is_le() {
            dst[out] = src[left];
            left += 1;
        } else {
            dst[out] = src[right];
            right += 1;
        }
        out += 1;
    }
    while left < mid {
        dst[out] = src[left];
        left += 1;
        out += 1;
    }
    while right < end {
        dst[out] = src[right];
        right += 1;
        out += 1;
    }
    Ok(())
}

enum PairContact {
    None,
    Touch {
        source: u32,
        handle_source: u32,
        at_start: bool,
        param: SourceParam,
    },
    Overlap {
        cuts: Vec<OverlapCut>,
    },
    Cross {
        t: SourceParam,
        u: SourceParam,
    },
}

struct OverlapCut {
    source: u32,
    handle_source: u32,
    at_start: bool,
    param: SourceParam,
}

fn classify_pair(
    a: Segment,
    b: Segment,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<PairContact> {
    let o_b0 =
        stored_orientation_sign(a.start.x, a.start.y, a.end.x, a.end.y, b.start.x, b.start.y);
    let o_b1 = stored_orientation_sign(a.start.x, a.start.y, a.end.x, a.end.y, b.end.x, b.end.y);
    let o_a0 =
        stored_orientation_sign(b.start.x, b.start.y, b.end.x, b.end.y, a.start.x, a.start.y);
    let o_a1 = stored_orientation_sign(b.start.x, b.start.y, b.end.x, b.end.y, a.end.x, a.end.y);
    if o_b0 == ExactSign::Zero && o_b1 == ExactSign::Zero {
        return overlap_or_touch(a, b, zero, one);
    }
    if o_b0 == ExactSign::Zero {
        let param = param_on_segment(a, b.start)?;
        if param.is_open_unit(zero, one)? {
            return Ok(PairContact::Touch {
                source: 0,
                handle_source: 1,
                at_start: true,
                param,
            });
        }
        return Ok(PairContact::None);
    }
    if o_b1 == ExactSign::Zero {
        let param = param_on_segment(a, b.end)?;
        if param.is_open_unit(zero, one)? {
            return Ok(PairContact::Touch {
                source: 0,
                handle_source: 1,
                at_start: false,
                param,
            });
        }
        return Ok(PairContact::None);
    }
    if o_a0 == ExactSign::Zero {
        let param = param_on_segment(b, a.start)?;
        if param.is_open_unit(zero, one)? {
            return Ok(PairContact::Touch {
                source: 1,
                handle_source: 0,
                at_start: true,
                param,
            });
        }
        return Ok(PairContact::None);
    }
    if o_a1 == ExactSign::Zero {
        let param = param_on_segment(b, a.end)?;
        if param.is_open_unit(zero, one)? {
            return Ok(PairContact::Touch {
                source: 1,
                handle_source: 0,
                at_start: false,
                param,
            });
        }
        return Ok(PairContact::None);
    }
    let straddle_b = matches!(
        (o_b0, o_b1),
        (ExactSign::Negative, ExactSign::Positive) | (ExactSign::Positive, ExactSign::Negative)
    );
    let straddle_a = matches!(
        (o_a0, o_a1),
        (ExactSign::Negative, ExactSign::Positive) | (ExactSign::Positive, ExactSign::Negative)
    );
    if straddle_a && straddle_b {
        let (t, u) = crossing_params(a, b)?;
        if t.is_open_unit(zero, one)? && u.is_open_unit(zero, one)? {
            return Ok(PairContact::Cross { t, u });
        }
    }
    Ok(PairContact::None)
}

fn overlap_or_touch(
    a: Segment,
    b: Segment,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<PairContact> {
    let t0 = param_on_segment(a, b.start)?;
    let t1 = param_on_segment(a, b.end)?;
    let u0 = param_on_segment(b, a.start)?;
    let u1 = param_on_segment(b, a.end)?;
    let mut cuts = Vec::new();
    if t0.is_open_unit(zero, one)? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            source: 0,
            handle_source: 1,
            at_start: true,
            param: t0,
        });
    }
    if t1.is_open_unit(zero, one)? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            source: 0,
            handle_source: 1,
            at_start: false,
            param: t1,
        });
    }
    if u0.is_open_unit(zero, one)? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            source: 1,
            handle_source: 0,
            at_start: true,
            param: u0,
        });
    }
    if u1.is_open_unit(zero, one)? {
        try_reserve_vec(&mut cuts, 1)?;
        cuts.push(OverlapCut {
            source: 1,
            handle_source: 0,
            at_start: false,
            param: u1,
        });
    }
    if cuts.is_empty() {
        return Ok(PairContact::None);
    }
    Ok(PairContact::Overlap { cuts })
}

fn param_on_segment(segment: Segment, point: XY) -> Result<SourceParam> {
    let dx = ExactDyadic::try_from_f64(segment.end.x)?
        .try_subtract(ExactDyadic::try_from_f64(segment.start.x)?)?;
    let dy = ExactDyadic::try_from_f64(segment.end.y)?
        .try_subtract(ExactDyadic::try_from_f64(segment.start.y)?)?;
    if dx.cmp_magnitude(&dy).is_ge() {
        let num = ExactDyadic::try_from_f64(point.x)?
            .try_subtract(ExactDyadic::try_from_f64(segment.start.x)?)?;
        Ok(SourceParam::Ratio { num, den: dx })
    } else {
        let num = ExactDyadic::try_from_f64(point.y)?
            .try_subtract(ExactDyadic::try_from_f64(segment.start.y)?)?;
        Ok(SourceParam::Ratio { num, den: dy })
    }
}

fn crossing_params(a: Segment, b: Segment) -> Result<(SourceParam, SourceParam)> {
    let rx =
        ExactDyadic::try_from_f64(a.end.x)?.try_subtract(ExactDyadic::try_from_f64(a.start.x)?)?;
    let ry =
        ExactDyadic::try_from_f64(a.end.y)?.try_subtract(ExactDyadic::try_from_f64(a.start.y)?)?;
    let sx =
        ExactDyadic::try_from_f64(b.end.x)?.try_subtract(ExactDyadic::try_from_f64(b.start.x)?)?;
    let sy =
        ExactDyadic::try_from_f64(b.end.y)?.try_subtract(ExactDyadic::try_from_f64(b.start.y)?)?;
    let cx = ExactDyadic::try_from_f64(b.start.x)?
        .try_subtract(ExactDyadic::try_from_f64(a.start.x)?)?;
    let cy = ExactDyadic::try_from_f64(b.start.y)?
        .try_subtract(ExactDyadic::try_from_f64(a.start.y)?)?;
    let den = rx.try_product(&sy)?.try_subtract(ry.try_product(&sx)?)?;
    let t_num = cx.try_product(&sy)?.try_subtract(cy.try_product(&sx)?)?;
    let u_num = cx.try_product(&ry)?.try_subtract(cy.try_product(&rx)?)?;
    Ok((
        SourceParam::Ratio {
            num: t_num,
            den: den.try_clone()?,
        },
        SourceParam::Ratio { num: u_num, den },
    ))
}

#[cfg(test)]
mod tests {
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
                let endpoint = stored_node(&partition, XY::new(1.0, -1.0)).unwrap();
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
        let start = stored_node(&partition, XY::new(0.0, 0.0)).unwrap();
        let crossing = NodeId(
            partition
                .recipes
                .iter()
                .position(|recipe| matches!(recipe, NodeRecipe::Constructed { .. }))
                .unwrap() as u32,
        );
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
            nodes: vec![false; partition.recipes.len()],
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
        let first = NodeId(partition.recipes.len() as u32);
        partition.recipes.push(NodeRecipe::Constructed {
            unique: 0,
            num: ExactDyadic::try_from_f64(1.0).unwrap(),
            den: ExactDyadic::try_from_f64(2.0).unwrap(),
        });
        let second = NodeId(partition.recipes.len() as u32);
        partition.recipes.push(NodeRecipe::Constructed {
            unique: 0,
            num: ExactDyadic::try_from_f64(1.0)
                .unwrap()
                .try_add(ExactDyadic::try_from_f64(2.0_f64.powi(-54)).unwrap())
                .unwrap(),
            den: ExactDyadic::try_from_f64(2.0).unwrap(),
        });
        assert_eq!(
            partition.compare_nodes(first, second).unwrap(),
            Ordering::Less
        );
        let nodes = [first, second];
        let coordinates = partition.round_nodes(&nodes).unwrap();
        assert_eq!(coordinates, [XY::new(1.0, 0.0); 2]);
        assert!(ExactPartition::check_node_injectivity(&nodes, &coordinates).is_err());
        ExactPartition::check_node_injectivity(&[first, first], &coordinates).unwrap();
        let endpoint = stored_node(&partition, XY::new(2.0, 0.0)).unwrap();
        let nodes = [first, endpoint];
        ExactPartition::check_node_injectivity(&nodes, &partition.round_nodes(&nodes).unwrap())
            .unwrap();
        ExactPartition::check_node_injectivity(&[], &[]).unwrap();
    }

    #[test]
    fn selected_nodes_round_in_requested_order() {
        let case = run(&[seg(0.0, 0.0, 3.0, 1.0), seg(1.0, -1.0, 1.0, 1.0)]);
        let partition = case.partition();
        let crossing = NodeId(
            partition
                .recipes
                .iter()
                .position(|recipe| matches!(recipe, NodeRecipe::Constructed { .. }))
                .unwrap() as u32,
        );
        let endpoint = stored_node(&partition, XY::new(3.0, 1.0)).unwrap();
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
            let NodeRecipe::Stored(start) = partition.recipe(span.start) else {
                panic!("stored-only fixture constructed a node");
            };
            let expected = start.xy().x >= 8.0 || start.xy().y == 0.0;
            assert_eq!(cells.spans[index], expected);
        }
        for (index, &filled) in cells.nodes.iter().enumerate() {
            let NodeRecipe::Stored(point) = partition.recipe(NodeId(index as u32)) else {
                panic!("stored-only fixture constructed a node");
            };
            assert_eq!(filled, point.xy().x >= 8.0 || point.xy().y == 0.0);
        }
        let (lines, points) = cells
            .lower_dimensions(&partition, &topology, &edges, &faces)
            .unwrap();
        assert!(points.is_empty());
        let mut intervals = lines
            .iter()
            .map(|&index| {
                let span = partition.spans[index as usize];
                let NodeRecipe::Stored(start) = partition.recipe(span.start) else {
                    panic!("stored-only fixture constructed a node");
                };
                let NodeRecipe::Stored(end) = partition.recipe(span.end) else {
                    panic!("stored-only fixture constructed a node");
                };
                assert_eq!((start.xy().y, end.xy().y), (0.0, 0.0));
                (start.xy().x.min(end.xy().x), start.xy().x.max(end.xy().x))
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
                let NodeRecipe::Stored(point) = partition.recipe(NodeId(index as u32)) else {
                    panic!("stored-only fixture constructed a node");
                };
                selected.push(point.xy());
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
        assert_eq!(
            points,
            [stored_node(&partition, XY::new(6.0, 6.0)).unwrap()]
        );
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
        let incident = stored_node(&partition, XY::new(1.0, 0.0)).unwrap();
        let isolated = stored_node(&partition, XY::new(3.0, 2.0)).unwrap();
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
    use super::*;
    use crate::geometry::generated_budget::PAIRWISE_WORK_LIMIT;

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
            assert!(!partition.recipes.is_empty());
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

    fn constructed_recipe<'a>(partition: &'a ExactPartition<'_>) -> &'a NodeRecipe {
        partition
            .recipes
            .iter()
            .find(|recipe| matches!(recipe, NodeRecipe::Constructed { .. }))
            .expect("expected a constructed recipe")
    }

    fn assert_ratio_eq(
        num: &ExactDyadic,
        den: &ExactDyadic,
        expected_n: &ExactDyadic,
        expected_d: &ExactDyadic,
    ) {
        assert_eq!(
            try_compare_ratios(num, den, expected_n, expected_d).unwrap(),
            Ordering::Equal
        );
    }

    fn constructed_count(partition: &ExactPartition) -> usize {
        partition
            .recipes
            .iter()
            .filter(|recipe| matches!(recipe, NodeRecipe::Constructed { .. }))
            .count()
    }

    fn stored_node(partition: &ExactPartition, point: XY) -> Option<NodeId> {
        let key = PointKey::new(point);
        partition
            .recipes
            .iter()
            .position(|recipe| matches!(recipe, NodeRecipe::Stored(stored) if *stored == key))
            .map(|index| NodeId(index as u32))
    }

    fn component_count(partition: &ExactPartition) -> usize {
        let n = partition.recipes.len();
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

    fn recipe_is_open_unit(recipe: &NodeRecipe) -> bool {
        let NodeRecipe::Constructed { num, den, .. } = recipe else {
            return false;
        };
        let zero = ExactDyadic::try_from_f64(0.0).unwrap();
        let one = ExactDyadic::try_from_f64(1.0).unwrap();
        try_compare_ratios(num, den, &zero, &one).unwrap() == Ordering::Greater
            && try_compare_ratios(num, den, &one, &one).unwrap() == Ordering::Less
    }

    #[test]
    fn public_entry_matches_inner_on_empty() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(Vec::<Segment>::new());
        let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
        assert_eq!(partition.recipes.len(), 0);
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
        assert_eq!(partition.recipes.len(), 7);
        assert_eq!(partition.spans.len(), 5);
        assert_eq!(constructed_count(&partition), 1);
        assert_eq!(component_count(&partition), 2);
        let c_end = stored_node(&partition, XY::new(0.5, rn_sixth)).unwrap();
        let on_a = partition
            .spans
            .iter()
            .any(|span| span.unique == 0 && (span.start == c_end || span.end == c_end));
        assert!(!on_a, "C's endpoint must not node onto A");
        let NodeRecipe::Constructed { unique, num, den } = constructed_recipe(&partition) else {
            panic!("expected a constructed recipe");
        };
        let one = ExactDyadic::try_from_f64(1.0).unwrap();
        let three = ExactDyadic::try_from_f64(3.0).unwrap();
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
        match *unique {
            0 => assert_ratio_eq(num, den, &one, &three),
            1 => assert_ratio_eq(num, den, &two, &three),
            other => panic!("unexpected constructed unique {other}"),
        }
    }

    #[test]
    fn endpoint_near_miss_five_nodes_four_spans() {
        let nd = 1.0_f64.next_down();
        let case = run(&[seg(0.0, 0.0, 1.0, 1.0), seg(1.0, nd, 0.0, 2.0)]);
        let partition = case.partition();
        assert_eq!(partition.recipes.len(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 1);
        let q_num = ExactDyadic::try_from_f64(1.0).unwrap();
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
        let p54 = {
            let mut p = ExactDyadic::try_from_f64(1.0).unwrap();
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
        let NodeRecipe::Constructed {
            unique,
            num,
            den: recipe_den,
        } = constructed_recipe(&partition)
        else {
            panic!("expected a constructed recipe");
        };
        match *unique {
            0 => assert_ratio_eq(num, recipe_den, &p54, &den),
            other => panic!("unexpected constructed unique {other}"),
        }
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
        let NodeRecipe::Constructed { unique, num, den } = constructed_recipe(&partition) else {
            panic!("expected a constructed recipe");
        };
        let one = ExactDyadic::try_from_f64(1.0).unwrap();
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
        let three = ExactDyadic::try_from_f64(3.0).unwrap();
        match *unique {
            0 => assert_ratio_eq(num, den, &one, &three),
            1 => assert_ratio_eq(num, den, &two, &three),
            other => panic!("unexpected constructed unique {other}"),
        }
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
            stored_node(&partition, XY::new(1.0, 1.0)),
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
        let middle = stored_node(&partition, XY::new(2.0, 0.0)).unwrap();
        let endpoint = stored_node(&partition, XY::new(0.0, 0.0)).unwrap();
        let isolated = stored_node(&partition, XY::new(7.0, 3.0)).unwrap();
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
            let constructed: Vec<_> = partition
                .recipes
                .iter()
                .enumerate()
                .filter(|(_, recipe)| matches!(recipe, NodeRecipe::Constructed { .. }))
                .map(|(index, _)| NodeId(index as u32))
                .collect();
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
                let target = stored_node(&partition, point).unwrap();
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
                        partition.compare_departures(crossing, left, right).unwrap(),
                        Ordering::Less
                    );
                    assert_eq!(
                        partition.compare_departures(crossing, right, left).unwrap(),
                        Ordering::Greater
                    );
                }
            }
            let [x, y, w] = partition.homogeneous(crossing).unwrap();
            let (topology, edges) = partition.local_topology().unwrap();
            let row = topology.starts[crossing.0 as usize] as usize
                ..topology.starts[crossing.0 as usize + 1] as usize;
            assert_eq!(row.len(), 4);
            for (&edge, &expected) in edges[row].iter().zip(&departures) {
                assert_eq!(edge as usize, expected);
            }
            let four = ExactDyadic::try_from_f64(4.0).unwrap();
            let three = ExactDyadic::try_from_f64(3.0).unwrap();
            let zero = ExactDyadic::try_from_f64(0.0).unwrap();
            let one = ExactDyadic::try_from_f64(1.0).unwrap();
            assert_eq!(
                try_compare_ratios(&x, &w, &four, &three).unwrap(),
                Ordering::Equal
            );
            assert_eq!(
                try_compare_ratios(&y, &w, &zero, &one).unwrap(),
                Ordering::Equal
            );
            let left = stored_node(&partition, XY::new(1.0, 0.0)).unwrap();
            let right = stored_node(&partition, XY::new(3.0, 0.0)).unwrap();
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
        let outer = stored_node(&partition, XY::new(0.0, 0.0)).unwrap().0 as usize;
        let inner = stored_node(&partition, XY::new(2.0, 2.0)).unwrap().0 as usize;
        let isolated = stored_node(&partition, XY::new(6.0, 6.0)).unwrap().0 as usize;
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
        let tip = stored_node(&partition, XY::new(9.0, 9.0)).unwrap().0 as usize;
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
        assert_eq!(
            partition
                .first_left_contact(&topology, NodeId(outer as u32))
                .unwrap(),
            None
        );
        let wall_start = stored_node(&partition, XY::new(0.0, 8.0)).unwrap();
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
                partition
                    .first_left_contact(&topology, NodeId(query as u32))
                    .unwrap(),
                Some(VisibilityHit::Span(wall as u32))
            );
        }
        let host = partition.contact_walk(&topology, &edges, VisibilityHit::Span(wall as u32));
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
        let root = stored_node(&partition, XY::new(0.0, 0.0)).unwrap();
        let upper_target = stored_node(&partition, XY::new(3.0, -1.0)).unwrap();
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

    #[test]
    fn visibility_keeps_horizontal_and_tangent_vertex_contacts() {
        for segments in [
            vec![seg(0.0, 0.0, 2.0, 0.0), seg(4.0, 0.0, 4.0, 0.0)],
            vec![
                seg(0.0, 2.0, 2.0, 0.0),
                seg(2.0, 0.0, 3.0, 2.0),
                seg(4.0, 0.0, 4.0, 0.0),
            ],
        ] {
            let case = run(&segments);
            let partition = case.partition();
            let (topology, _) = partition.local_topology().unwrap();
            let query = stored_node(&partition, XY::new(4.0, 0.0)).unwrap();
            let contact = stored_node(&partition, XY::new(2.0, 0.0)).unwrap();
            assert_eq!(
                partition.first_left_contact(&topology, query).unwrap(),
                Some(VisibilityHit::Node(contact))
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
        let root = stored_node(&partition, XY::new(4.0, 0.0)).unwrap();
        let contact = stored_node(&partition, XY::new(2.0, 0.0)).unwrap();
        assert_eq!(
            partition.first_left_contact(&topology, root).unwrap(),
            Some(VisibilityHit::Node(contact))
        );
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(faces.count, 3);
        assert_eq!(faces.component_hosts, [0, 0, 0]);
        let exterior = partition
            .exterior_walk(&topology, &edges, root)
            .unwrap()
            .unwrap();
        assert_eq!(faces.walk_faces[exterior as usize], 0);
        let host = partition.contact_walk(&topology, &edges, VisibilityHit::Node(contact));
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
        let inner = stored_node(&partition, XY::new(2.0, 2.0)).unwrap();
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
                    let NodeRecipe::Stored(key) =
                        partition.recipe(NodeId(topology.owners[slot as usize]))
                    else {
                        panic!("square corners are stored");
                    };
                    let point = key.xy();
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
        let origin = stored_node(&partition, XY::new(0.0, 0.0)).unwrap();
        let east = stored_node(&partition, XY::new(6.0, 0.0)).unwrap();
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
        let contact = stored_node(&partition, XY::new(0.0, 3.0)).unwrap();
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
                assert!(!matches!(partition.recipe(node), NodeRecipe::Stored(_)));
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
            nodes: vec![true; partition.recipes.len()],
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
            nodes: vec![true; partition.recipes.len()],
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
                nodes: vec![true; partition.recipes.len()],
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
        let hole = stored_node(&partition, XY::new(2.0, 2.0)).unwrap();
        let island = stored_node(&partition, XY::new(4.0, 4.0)).unwrap();
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
            let NodeRecipe::Stored(key) = partition.recipe(node) else {
                panic!("stored square corner")
            };
            if polygon.len() == 1 {
                assert!((4.0..=6.0).contains(&key.xy().x));
            } else {
                assert!(key.xy().x == 0.0 || key.xy().x == 10.0);
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
            .boundary_walks(&topology, &[false, true, true])
            .unwrap();
        assert_eq!(walks.len(), 2);
        let contact = stored_node(&partition, XY::new(2.0, 2.0)).unwrap();
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
                .boundary_walks(&topology, &[false, false, false])
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
        let origin = stored_node(&partition, XY::new(0.0, 0.0)).unwrap();
        let mut darts = Vec::new();
        let mut chords = Vec::new();
        for endpoint in [
            XY::new(2.0, 0.0),
            XY::new(0.0, 3.0),
            XY::new(-4.0, 0.0),
            XY::new(0.0, -5.0),
        ] {
            let leaf = stored_node(&partition, endpoint).unwrap();
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
        let point = stored_node(&partition, XY::new(2.0, 2.0)).unwrap();
        let mut cells = ArealCells {
            faces: vec![false; faces.count as usize],
            spans: vec![true; partition.spans.len()],
            nodes: vec![false; partition.recipes.len()],
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
            .boundary_walks(&topology, &[false, true, true])
            .unwrap();
        assert_eq!(walks.len(), 1);
        let boundary: Vec<_> = walks[0]
            .iter()
            .map(|&slot| partition.recipe(NodeId(topology.owners[slot as usize])))
            .collect();
        assert_eq!(boundary, [
            partition.recipe(stored_node(&partition, XY::new(0.0, 0.0)).unwrap()),
            partition.recipe(stored_node(&partition, XY::new(4.0, 0.0)).unwrap()),
            partition.recipe(stored_node(&partition, XY::new(4.0, 4.0)).unwrap()),
            partition.recipe(stored_node(&partition, XY::new(0.0, 4.0)).unwrap()),
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
        let point = stored_node(&partition, XY::new(1.0, 2.0)).unwrap();
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
        assert!(stored_node(&partition, XY::new(1.0, 1.0)).is_some());
        assert_eq!(partition.isolated.len(), 1);
    }

    #[test]
    fn signed_zero_endpoints_share_a_stored_node() {
        let case = run(&[seg(-0.0, 0.0, 1.0, 0.0), seg(0.0, 0.0, 0.0, 1.0)]);
        let partition = case.partition();
        let a = stored_node(&partition, XY::new(-0.0, 0.0)).unwrap();
        let b = stored_node(&partition, XY::new(0.0, 0.0)).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn full_finite_exponent_pair_is_five_nodes_four_spans() {
        let min_sub = f64::from_bits(1);
        let a = seg(0.0, 0.0, f64::MAX, min_sub);
        let b = seg(min_sub, -f64::MAX, f64::MAX, f64::MAX);
        let case = run(&[a, b]);
        let partition = case.partition();
        assert_eq!(partition.recipes.len(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 1);
        for point in [a.start, a.end, b.start, b.end] {
            assert!(stored_node(&partition, point).is_some());
        }
        let constructed = partition
            .recipes
            .iter()
            .find(|recipe| matches!(recipe, NodeRecipe::Constructed { .. }))
            .unwrap();
        assert!(recipe_is_open_unit(constructed));
    }

    #[test]
    fn isolated_on_line_cuts_the_edge() {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        let (unique, provenance) = dedup_undirected_segments_sourced(vec![seg(0.0, 0.0, 2.0, 0.0)]);
        let partition =
            partition_unique_segments(&unique, &provenance, &[XY::new(1.0, 0.0)], &mut budget)
                .unwrap();
        assert_eq!(partition.recipes.len(), 3);
        assert_eq!(partition.spans.len(), 2);
        assert!(partition.isolated.is_empty());
        let mid = stored_node(&partition, XY::new(1.0, 0.0)).unwrap();
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
        assert_eq!(partition.recipes.len(), 5);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(constructed_count(&partition), 0);
        assert!(partition.isolated.is_empty());
        assert!(matches!(
            partition.recipe(stored_node(&partition, XY::new(1.0, 1.0)).unwrap()),
            NodeRecipe::Stored(_)
        ));
    }

    #[test]
    fn stored_endpoint_concurrency_is_six_nodes_five_spans() {
        let case = run(&[
            seg(0.0, 0.0, 2.0, 0.0),
            seg(1.0, 0.0, 1.0, 1.0),
            seg(0.0, 1.0, 2.0, -1.0),
        ]);
        let partition = case.partition();
        assert_eq!(partition.recipes.len(), 6);
        assert_eq!(partition.spans.len(), 5);
        assert_eq!(constructed_count(&partition), 0);
        let junction = stored_node(&partition, XY::new(1.0, 0.0)).unwrap();
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
        let NodeRecipe::Constructed { unique, num, den } = constructed_recipe(&partition) else {
            panic!("expected a constructed recipe");
        };
        let two = ExactDyadic::try_from_f64(2.0).unwrap();
        let three = ExactDyadic::try_from_f64(3.0).unwrap();
        match *unique {
            0 => assert_ratio_eq(num, den, &two, &three),
            1 => {
                let one = ExactDyadic::try_from_f64(1.0).unwrap();
                assert_ratio_eq(num, den, &one, &three);
            },
            other => panic!("unexpected constructed unique {other}"),
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
        let five = ExactDyadic::try_from_f64(5.0).unwrap();
        for recipe in &partition.recipes {
            let NodeRecipe::Constructed { unique, num, den } = recipe else {
                continue;
            };
            assert_eq!(*unique, 0);
            let k = (1..=4).find(|&k| {
                let k_dy = ExactDyadic::try_from_f64(f64::from(k)).unwrap();
                try_compare_ratios(num, den, &k_dy, &five).unwrap() == Ordering::Equal
            });
            assert!(k.is_some(), "constructed param must be k/5 for k=1..=4");
        }
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
        assert_eq!(partition.recipes.len(), 64_000);
        assert_eq!(partition.spans.len(), 32_000);
        assert_eq!(constructed_count(&partition), 0);
    }

    #[test]
    fn try_add_matches_infallible_add_on_minsub() {
        let a = ExactDyadic::try_from_f64(1.0).unwrap();
        let b = ExactDyadic::try_from_f64(f64::from_bits(1)).unwrap();
        let fallible = a
            .try_clone()
            .unwrap()
            .try_subtract(b.try_clone().unwrap())
            .unwrap();
        let infail = ExactDyadic::from_f64(1.0).subtract(ExactDyadic::from_f64(f64::from_bits(1)));
        assert_eq!(fallible, infail);
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
