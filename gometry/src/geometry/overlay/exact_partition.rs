//! Exact source partition of unique stored segments.
//!
//! Borrows unique undirected representatives and existing CSR provenance.
//! Constructed nodes keep one source-parameter recipe after equal-param union;
//! stored nodes keep `PointKey`. Arrangement, overlay, and checked embedding
//! are later cuts.
//!
//! Candidates come from the shared fallible sweep over unique segments plus
//! extra isolated points as zero-length items. Contact-derived cuts, compact
//! nodes, and canonical atoms are generated items. Sweep scratch is
//! [`ResourceError`], not the item cap.

use std::cmp::Ordering;
use std::mem;
use std::ops::{ControlFlow, Range};

use crate::error::ResourceError;
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::noding::{SegmentProvenance, SegmentSource};
use crate::geometry::segments::{
    point_in_segment_envelope, point_on_segment, segment_envelopes_disjoint,
    stored_orientation_sign,
};
use crate::geometry::tessellation::exact::{ExactDyadic, round_ratio, try_compare_ratios};
use crate::geometry::{
    ExpansionBudget, GeometryErrorKind, HashMap, PointKey, Polygon, Result, Segment, XY,
    for_each_candidate_pair, same_point, single_chain,
};
use crate::try_reserve_vec;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(in crate::geometry) struct NodeId(pub u32);

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
                lines.push(index as u32);
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
                points.push(NodeId(index as u32));
            }
        }
        Ok((lines, points))
    }
}

impl ArealSources {
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
                    sources.segments.push(Segment {
                        start: XY::new(xs[from], ys[from]),
                        end: XY::new(xs[to], ys[to]),
                    });
                    sources.source_rings.push((!collapsed).then_some(ring_id));
                }
                if collapsed {
                    try_reserve_vec(&mut sources.collapsed_rings, 1)?;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactSpan {
    pub start: NodeId,
    pub end: NodeId,
    pub unique: u32,
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

struct BoundaryPlan {
    walks: Vec<Vec<u32>>,
    lines: Vec<u32>,
    face_quotient: Vec<u32>,
    walk_sides: Vec<Vec<[u32; 2]>>,
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
    fn boundary_plan(
        &self,
        lines: Vec<u32>,
        topology: &IndexedTopology,
        edges: &[u32],
        span_count: usize,
        occupied: &[bool],
    ) -> Result<BoundaryPlan> {
        let walks = self.boundary_walks(topology, occupied)?;
        let mut retained = Vec::new();
        try_reserve_vec(&mut retained, span_count)?;
        retained.resize(span_count, false);
        for &span in &lines {
            retained[span as usize] = true;
        }
        for walk in &walks {
            for &slot in walk {
                retained[edges[slot as usize] as usize] = true;
            }
        }
        let face_quotient = self.quotient(topology, edges, &retained)?;
        let mut walk_sides = Vec::new();
        try_reserve_vec(&mut walk_sides, walks.len())?;
        for walk in &walks {
            let mut sides = Vec::new();
            try_reserve_vec(&mut sides, walk.len())?;
            for &slot in walk {
                let slot = slot as usize;
                sides.push([slot, topology.twins[slot] as usize].map(|dart| {
                    face_quotient[self.walk_faces[topology.face_of[dart] as usize] as usize]
                }));
            }
            walk_sides.push(sides);
        }
        Ok(BoundaryPlan {
            walks,
            lines,
            face_quotient,
            walk_sides,
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
            selected.push(
                occupied[self.walk_faces[walk as usize] as usize]
                    && !occupied[self.walk_faces[other as usize] as usize],
            );
        }
        let mut visited = Vec::new();
        try_reserve_vec(&mut visited, selected.len())?;
        visited.resize(selected.len(), false);
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
                walk.push(slot as u32);
                let target = topology.targets[slot] as usize;
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
            try_reserve_vec(&mut walks, 1)?;
            walks.push(walk);
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
}

impl<'a> ExactPartition<'a> {
    pub(in crate::geometry) const fn unique(&self) -> &'a [Segment] {
        self.unique
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
                let point = key.xy();
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

    fn round_nodes(&self, nodes: &[NodeId]) -> Result<Vec<XY>> {
        let mut coordinates = Vec::new();
        try_reserve_vec(&mut coordinates, nodes.len())?;
        for &node in nodes {
            coordinates.push(match self.recipe(node) {
                NodeRecipe::Stored(key) => key.xy(),
                NodeRecipe::Constructed { .. } => {
                    let [x, y, w] = self.homogeneous(node)?;
                    XY::new(round_ratio(&x, &w)?, round_ratio(&y, &w)?)
                },
            });
        }
        Ok(coordinates)
    }

    fn check_node_injectivity(nodes: &[NodeId], coordinates: &[XY]) -> Result<()> {
        debug_assert_eq!(nodes.len(), coordinates.len());
        let mut owners = HashMap::new();
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
        let mut indices = HashMap::new();
        for item in items {
            for &node in item {
                if indices.contains_key(&node) {
                    continue;
                }
                indices
                    .try_reserve(1)
                    .map_err(|_| ResourceError::Allocation)?;
                try_reserve_vec(&mut nodes, 1)?;
                indices.insert(node, nodes.len());
                nodes.push(node);
            }
        }
        let coordinates = self.round_nodes(&nodes)?;
        Self::check_node_injectivity(&nodes, &coordinates)?;
        let mut segments = Vec::new();
        try_reserve_vec(&mut segments, items.len())?;
        for &[start, end] in items {
            segments.push(Segment {
                start: coordinates[indices[&start]],
                end: coordinates[indices[&end]],
            });
        }
        // A repeated node represents a live isolated point, not a new cut.
        let mut contact = None;
        let _ = try_for_each_candidate_pair(&segments, single_chain, |left, right| {
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
        try_reserve_vec(
            &mut topology.starts,
            vertices.checked_add(1).ok_or(ResourceError::SizeOverflow)?,
        )?;
        topology.starts.resize(vertices + 1, 0);
        for span in &self.spans {
            topology.starts[span.start.0 as usize + 1] += 1;
            topology.starts[span.end.0 as usize + 1] += 1;
        }
        for node in 0..vertices {
            topology.starts[node + 1] += topology.starts[node];
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
                topology.targets.push(if span.start.0 == node as u32 {
                    span.end.0
                } else {
                    span.start.0
                });
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
            let prior = chosen.unwrap();
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
            component_hosts.push(host);
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
        try_reserve_vec(&mut starts, count + 1)?;
        starts.resize(count + 1, 0_usize);
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
        stack.push((0_u32, starts[0], None));
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
            stack.push((neighbor, starts[neighbor as usize], Some(slot)));
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
        let mut nearest: Option<(ExactDyadic, ExactDyadic, VisibilityHit)> = None;
        for (edge, span) in self.spans.iter().enumerate() {
            if topology.component_of[span.start.0 as usize] == component {
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
                (x, w, VisibilityHit::Span(edge as u32))
            };
            if try_compare_ratios(&x, &w, &qx, &qw)? != Ordering::Less {
                continue;
            }
            if let Some((nx, nw, _)) = &nearest
                && try_compare_ratios(&x, &w, nx, nw)? != Ordering::Greater
            {
                continue;
            }
            nearest = Some((x, w, hit));
        }
        Ok(nearest.map(|(_, _, hit)| hit))
    }
}

enum SourceParam {
    Start,
    End,
    Ratio { num: ExactDyadic, den: ExactDyadic },
}

impl SourceParam {
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
        self.parent.push(id);
        self.kind.push(kind);
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

    let mut endpoint_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    endpoint_of
        .try_reserve(
            unique
                .len()
                .saturating_mul(2)
                .saturating_add(isolated.len()),
        )
        .map_err(crate::error::Error::from)?;
    let mut forest = Forest::with_capacity(
        unique
            .len()
            .saturating_mul(2)
            .saturating_add(isolated.len()),
    )?;

    let intern_stored =
        |point: XY, endpoint_of: &mut HashMap<PointKey, u32>, forest: &mut Forest| -> Result<u32> {
            let key = PointKey::new(point);
            if let Some(&id) = endpoint_of.get(&key) {
                return Ok(id);
            }
            let id = forest.push(HandleKind::Stored(key))?;
            endpoint_of.insert(key, id);
            Ok(id)
        };

    let mut start_handle = Vec::new();
    let mut end_handle = Vec::new();
    try_reserve_vec(&mut start_handle, unique.len())?;
    try_reserve_vec(&mut end_handle, unique.len())?;
    for &segment in unique {
        start_handle.push(intern_stored(segment.start, &mut endpoint_of, &mut forest)?);
        end_handle.push(intern_stored(segment.end, &mut endpoint_of, &mut forest)?);
    }
    let mut cuts: Vec<Vec<SourceCut>> = Vec::new();
    try_reserve_vec(&mut cuts, unique.len())?;
    for _ in 0..unique.len() {
        cuts.push(Vec::new());
    }

    let mut pool = Vec::new();
    try_reserve_vec(&mut pool, unique.len().saturating_add(isolated.len()))?;
    pool.extend_from_slice(unique);
    let mut extra_handles = Vec::new();
    try_reserve_vec(&mut extra_handles, isolated.len())?;
    let mut point_handles = Vec::new();
    try_reserve_vec(&mut point_handles, isolated.len())?;
    for &point in isolated {
        let key = PointKey::new(point);
        let existed = endpoint_of.contains_key(&key);
        let handle = intern_stored(point, &mut endpoint_of, &mut forest)?;
        point_handles.push(handle);
        if existed {
            continue;
        }
        extra_handles.push(handle);
        pool.push(Segment {
            start: point,
            end: point,
        });
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
        order.push(try_sort_indices(cuts[source].len(), |left, right| {
            cuts[source][left as usize]
                .param
                .cmp(&cuts[source][right as usize].param, &zero, &one)
        })?);
        union_equal_params(&cuts[source], &order[source], &mut forest, &zero, &one)?;
    }

    let mut compact_of: Vec<Option<u32>> = Vec::new();
    try_reserve_vec(&mut compact_of, forest.parent.len())?;
    compact_of.resize(forest.parent.len(), None);
    let mut recipes: Vec<NodeRecipe> = Vec::new();
    for handle in 0..forest.parent.len() {
        let root = forest.find(handle as u32);
        if compact_of[root as usize].is_some() {
            continue;
        }
        let id = u32::try_from(recipes.len()).map_err(|_| ResourceError::SizeOverflow)?;
        match forest.kind[root as usize] {
            HandleKind::Stored(key) => {
                try_reserve_vec(&mut recipes, 1)?;
                recipes.push(NodeRecipe::Stored(key));
            },
            HandleKind::Constructed { unique, cut } => {
                budget.add(1)?;
                let param = mem::replace(
                    &mut cuts[unique as usize][cut as usize].param,
                    SourceParam::Start,
                );
                let SourceParam::Ratio { num, den } = param else {
                    return Err(GeometryErrorKind::Invalid(
                        "constructed partition node has no source-parameter recipe".into(),
                    )
                    .into());
                };
                try_reserve_vec(&mut recipes, 1)?;
                recipes.push(NodeRecipe::Constructed { unique, num, den });
            },
        }
        compact_of[root as usize] = Some(id);
    }
    for handle in 0..forest.parent.len() {
        let root = forest.find(handle as u32);
        compact_of[handle] = compact_of[root as usize];
    }

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
                intervals.push((source as u32, start, id));
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
            isolated_nodes.push(NodeId(index as u32));
        }
    }

    let mut point_nodes = Vec::new();
    try_reserve_vec(&mut point_nodes, point_handles.len())?;
    for handle in point_handles {
        let node = compact_of[handle as usize].ok_or_else(|| {
            GeometryErrorKind::Invalid("point source handle was not compacted".into())
        })?;
        point_nodes.push(NodeId(node));
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

    Ok(ExactPartition {
        unique,
        recipes,
        spans,
        provenance: atom_provenance,
        isolated: isolated_nodes,
        point_nodes,
        point_sources,
    })
}

fn push_contact_cut(cuts: &mut Vec<SourceCut>, param: SourceParam, handle: u32) -> Result<()> {
    try_reserve_vec(cuts, 1)?;
    cuts.push(SourceCut { param, handle });
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
            contributors.push((atom, unique, reversed));
            continue;
        }
        if spans.len() >= baseline {
            budget.add(1)?;
        }
        let atom = u32::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut spans, 1)?;
        spans.push(ExactSpan {
            start: NodeId(start),
            end: NodeId(end),
            unique,
        });
        atom_of.insert((lo, hi), atom);
        contributors.push((atom, unique, false));
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
    offsets.push(0);
    for &count in &counts {
        let next = (offsets[offsets.len() - 1] as usize)
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
        let next = u32::try_from(next).map_err(|_| ResourceError::SizeOverflow)?;
        offsets.push(next);
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
        let error = partition
            .round_incident_items(&[[start, crossing], [isolated; 2]])
            .unwrap_err();
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
        assert_eq!(rounded[1].y.to_bits(), 0x3FD5555555555555);
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
        assert_eq!(points, [stored_node(&partition, XY::new(6.0, 6.0)).unwrap()]);
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
        assert_eq!(partition.unique().len(), 3);
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
            .boundary_plan(Vec::new(), &topology, &edges, partition.spans.len(), &occupied)
            .unwrap();
        assert_eq!(plan.face_quotient, [0, 1, 2]);
        assert_eq!(plan.walk_sides[0], vec![[host, 0]; 4]);
        assert_eq!(plan.walk_sides[1].len(), 4);
        for sides in &plan.walk_sides[1] {
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
                .map(|&slot| {
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
            .boundary_plan(Vec::new(), &topology, &edges, partition.spans.len(), &[false; 3])
            .unwrap();
        assert!(empty.walks.is_empty());
        assert!(empty.walk_sides.is_empty());
        assert_eq!(empty.face_quotient, [0, 0, 0]);
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
            .boundary_plan(Vec::new(), &topology, &edges, partition.spans.len(), &[
                false, true, true,
            ])
            .unwrap();
        assert_eq!(plan.face_quotient, [0, 1, 2]);
        assert_eq!(plan.walk_sides, [vec![[1, 0]; 4], vec![[2, 0]; 4]]);
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
            .boundary_plan(vec![0, 1, 2, 3], &topology, &edges, 4, &[false, false])
            .unwrap();
        assert!(plan.walks.is_empty());
        assert_eq!(plan.lines, [0, 1, 2, 3]);
        assert_eq!(plan.face_quotient, [0, 1]);
        let open = faces
            .boundary_plan(vec![0, 1, 2], &topology, &edges, 4, &[false, false])
            .unwrap();
        assert_eq!(open.face_quotient, [0, 0]);
    }

    #[test]
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
            .boundary_plan(Vec::new(), &topology, &edges, partition.spans.len(), &[
                false, true, true,
            ])
            .unwrap();
        assert_eq!(plan.face_quotient, [0, 1, 1]);
        assert_eq!(plan.walk_sides, [vec![[1, 0]; 4]]);
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
}
