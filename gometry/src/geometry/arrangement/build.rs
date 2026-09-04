use crate::geometry::arrangement;
use super::winding::twin_slot_spares;
use crate::geometry::arrangement::{
    Arrangement, Columns, Face, FinishSpares, TypedSpares, WindingWeight, arrangement_spares,
    build_csr, dedup_vertices_and_edges, order_single_loop_rows, positional_loop_cuts,
    positional_loop_pieces, region_spares, single_loop_cuts, single_loop_pieces,
    sort_rows_counterclockwise, split_ring_at_pinches, typed_spares, walk_faces,
};
use crate::geometry::{HashMap, PointKey, Segment, XY, wrap_index};

impl Arrangement<i32> {
    /// Build from noded directed segments with unit weights. Coincident
    /// segments (either direction) collapse into one undirected half-edge
    /// pair carrying the net directed multiplicity. Face rings are NOT
    /// collected — region consumers walk boundaries through
    /// [`Arrangement::region_rings`]; use [`Arrangement::new_with_rings`]
    /// to materialize every face's ring.
    pub(crate) fn new(segments: &[Segment]) -> Self {
        Self::weighted(segments, |_| 1)
    }

    /// [`Arrangement::new`] with every [`Face::ring`] materialized — for
    /// consumers that read the faces themselves (coverage cleaning) rather
    /// than a winding-selected region boundary.
    pub(crate) fn new_with_rings(segments: &[Segment]) -> Self {
        Self::build::<true>(segments, |_| 1)
    }

    /// Build from already-deduplicated edges whose endpoint ordinals index
    /// `points`. This avoids rebuilding the vertex map when a caller already
    /// owns the planar graph and needs only its face columns.
    pub(crate) fn from_indexed_edges(points: &[XY], edges: &[(u32, u32)]) -> Self {
        let mut spares = arrangement_spares();
        let mut typed = typed_spares::<i32>();
        let mut arrangement_points = std::mem::take(&mut spares.points);
        arrangement_points.clear();
        arrangement_points.extend_from_slice(points);
        let mut indexed_edges = std::mem::take(&mut typed.edges);
        indexed_edges.clear();
        indexed_edges.reserve(edges.len());
        for &(start, end) in edges {
            debug_assert_ne!(start, end, "graph bundles exclude zero-length edges");
            indexed_edges.push((start, end, 1));
        }
        let (starts, mut targets, owners, mut multiplicities) = build_csr(
            arrangement_points.len(),
            &indexed_edges,
            std::mem::take(&mut spares.starts),
            std::mem::take(&mut spares.targets),
            std::mem::take(&mut spares.owners),
            std::mem::take(&mut typed.multiplicities),
        );
        indexed_edges.clear();
        sort_rows_counterclockwise(
            &arrangement_points,
            &starts,
            &mut targets,
            &mut multiplicities,
        );
        let mut twins = std::mem::take(&mut spares.twins);
        populate_twins(&mut twins, &targets, &owners);
        Self::finish_topology::<false>(
            Columns {
                points: arrangement_points,
                starts,
                targets,
                owners,
                twins,
                multiplicities,
            },
            std::mem::take(&mut spares.ids),
            false,
            FinishSpares {
                component_of: std::mem::take(&mut spares.component_of),
                face_of: std::mem::take(&mut spares.face_of),
                face_starts: std::mem::take(&mut spares.face_starts),
                face_slots: std::mem::take(&mut spares.face_slots),
            },
            spares,
            typed,
        )
    }

    /// Specialized constructor for ONE closed loop's raw (un-noded)
    /// segments — the stroke/offset buffer's dominant caseload. Produces
    /// bit-identical topology columns to `self_node_segments` +
    /// [`Arrangement::new`] (hence bit-identical faces and rings, since the
    /// walks downstream are shared) while skipping the atomic segment soup,
    /// the vertex hash dedup, and the global edge sort: on a single chained
    /// loop, vertex identity is POSITIONAL, and every noding cut is a
    /// transversal crossing visited exactly twice — dense ids and CSR rows
    /// fall out of the ordinal walk.
    ///
    /// Returns `None` (the caller falls back to the general path) on any
    /// input outside that shape: collinear overlaps, key-coincident
    /// vertices (T-junctions, repeated loop vertices), concurrent or
    /// ulp-twin crossings, duplicate edges, or a chain that is not bitwise
    /// closed.
    ///
    /// The vertex-key map is left EMPTY — `component_of_point` must not be
    /// called on a fast-path arrangement (single-loop consumers seed their
    /// one component with `[0]`, never by probe).
    pub(crate) fn from_single_loop(segments: &[Segment]) -> Option<Self> {
        let n = segments.len();
        let (cuts, group_count) = single_loop_cuts(segments)?;
        let mut spares = arrangement_spares();
        let mut typed = typed_spares::<i32>();
        let mut ids = std::mem::take(&mut spares.ids);
        ids.clear();
        let vertex_count = n + group_count as usize;
        let mut points = std::mem::take(&mut spares.points);
        points.clear();
        points.reserve(vertex_count);
        let (is_cut, pieces) = single_loop_pieces(segments, &cuts, group_count, &mut points);
        debug_assert_eq!(points.len(), vertex_count);
        // Degrees are structural — chain vertices 2, crossings 4 — so the
        // CSR shape needs no edge sort.
        let mut starts = std::mem::take(&mut spares.starts);
        starts.clear();
        starts.reserve(vertex_count + 1);
        starts.push(0);
        let mut total = 0_u32;
        for &cut in &is_cut {
            total += if cut { 4 } else { 2 };
            starts.push(total);
        }
        let half_edge_count = total as usize;
        debug_assert_eq!(half_edge_count, 2 * pieces.len());
        let mut targets = std::mem::take(&mut spares.targets);
        targets.clear();
        targets.resize(half_edge_count, 0);
        let mut owners = std::mem::take(&mut spares.owners);
        owners.clear();
        owners.resize(half_edge_count, 0);
        let mut twins = std::mem::take(&mut spares.twins);
        let mut multiplicities = std::mem::take(&mut typed.multiplicities);
        multiplicities.clear();
        multiplicities.resize(half_edge_count, 0);
        let mut filled = std::mem::take(&mut spares.filled);
        filled.clear();
        filled.resize(vertex_count, 0);
        for &(from, to) in &pieces {
            let slot = (starts[from as usize] + filled[from as usize]) as usize;
            filled[from as usize] += 1;
            targets[slot] = to;
            owners[slot] = from;
            multiplicities[slot] = 1;
            let twin = (starts[to as usize] + filled[to as usize]) as usize;
            filled[to as usize] += 1;
            targets[twin] = from;
            owners[twin] = to;
            multiplicities[twin] = -1;
        }
        if !order_single_loop_rows(&points, &starts, &mut targets, &mut multiplicities) {
            // Restock the pools (contents are stale by definition) and let
            // the general path own the case.
            spares.points = points;
            spares.starts = starts;
            spares.targets = targets;
            spares.owners = owners;
            spares.twins = twins;
            spares.ids = ids;
            spares.filled = filled;
            typed.multiplicities = multiplicities;
            return None;
        }
        populate_twins(&mut twins, &targets, &owners);
        spares.filled = filled;
        Some(Self::finish_topology::<false>(
            Columns {
                points,
                starts,
                targets,
                owners,
                twins,
                multiplicities,
            },
            ids,
            true,
            FinishSpares {
                component_of: std::mem::take(&mut spares.component_of),
                face_of: std::mem::take(&mut spares.face_of),
                face_starts: std::mem::take(&mut spares.face_starts),
                face_slots: std::mem::take(&mut spares.face_slots),
            },
            spares,
            typed,
        ))
    }
}

impl<W: WindingWeight> Arrangement<W> {
    /// Positional MULTI-LOOP constructor — [`Arrangement::from_single_loop`]'s
    /// identity extended to K clean closed loops (binary-overlay rings,
    /// dissolve leaves): per-loop positional vertices, crossing groups
    /// shared globally (a cross-loop crossing groups exactly like a
    /// self-crossing — same key, two distinct global ordinals), per-loop
    /// winding weights, and the same bails — the global duplicate-key
    /// check covers cross-loop vertex coincidence and T-junctions.
    /// Populates `ids` (consumers probe components) and runs the
    /// DISCONNECTED topology pass. `None` hands the case to the general
    /// build, the exact oracle.
    pub(crate) fn from_loops(
        segments: &[Segment],
        loop_ranges: &[(u32, u32)],
        weight_of_loop: impl Fn(usize) -> W,
    ) -> Option<Self> {
        let n = segments.len();
        let (cuts, group_count) = positional_loop_cuts(segments, loop_ranges)?;
        let mut spares = arrangement_spares();
        let mut typed = typed_spares::<W>();
        let mut ids = std::mem::take(&mut spares.ids);
        ids.clear();
        let vertex_count = n + group_count as usize;
        let mut points = std::mem::take(&mut spares.points);
        points.clear();
        points.reserve(vertex_count);
        let (is_cut, pieces, anchor_ids) =
            positional_loop_pieces(segments, loop_ranges, &cuts, group_count, &mut points);
        debug_assert_eq!(points.len(), vertex_count);
        let mut starts = std::mem::take(&mut spares.starts);
        starts.clear();
        starts.reserve(vertex_count + 1);
        starts.push(0);
        let mut total = 0_u32;
        for &cut in &is_cut {
            total += if cut { 4 } else { 2 };
            starts.push(total);
        }
        let half_edge_count = total as usize;
        debug_assert_eq!(half_edge_count, 2 * pieces.len());
        let mut targets = std::mem::take(&mut spares.targets);
        targets.clear();
        targets.resize(half_edge_count, 0);
        let mut owners = std::mem::take(&mut spares.owners);
        owners.clear();
        owners.resize(half_edge_count, 0);
        let mut twins = std::mem::take(&mut spares.twins);
        let mut multiplicities = std::mem::take(&mut typed.multiplicities);
        multiplicities.clear();
        multiplicities.resize(half_edge_count, W::UNSET);
        let mut filled = std::mem::take(&mut spares.filled);
        filled.clear();
        filled.resize(vertex_count, 0);
        for &(from, to, loop_index) in &pieces {
            let weight = weight_of_loop(loop_index as usize);
            let slot = (starts[from as usize] + filled[from as usize]) as usize;
            filled[from as usize] += 1;
            targets[slot] = to;
            owners[slot] = from;
            multiplicities[slot] = weight;
            let twin = (starts[to as usize] + filled[to as usize]) as usize;
            filled[to as usize] += 1;
            targets[twin] = from;
            owners[twin] = to;
            multiplicities[twin] = weight.neg();
        }
        if !order_single_loop_rows(&points, &starts, &mut targets, &mut multiplicities) {
            spares.points = points;
            spares.starts = starts;
            spares.targets = targets;
            spares.owners = owners;
            spares.twins = twins;
            spares.ids = ids;
            spares.filled = filled;
            typed.multiplicities = multiplicities;
            return None;
        }
        populate_twins(&mut twins, &targets, &owners);
        // Only the loop ANCHORS are ever looked up (component probes map
        // anchors -> components); inserting all 40k vertices for K=2
        // lookups was pure waste. Duplicate keys already bailed.
        ids.reserve(anchor_ids.len());
        for &anchor in &anchor_ids {
            ids.insert(PointKey::new(points[anchor as usize]), anchor);
        }
        spares.filled = filled;
        Some(Self::finish_topology::<false>(
            Columns {
                points,
                starts,
                targets,
                owners,
                twins,
                multiplicities,
            },
            ids,
            false,
            FinishSpares {
                component_of: std::mem::take(&mut spares.component_of),
                face_of: std::mem::take(&mut spares.face_of),
                face_starts: std::mem::take(&mut spares.face_starts),
                face_slots: std::mem::take(&mut spares.face_slots),
            },
            spares,
            typed,
        ))
    }

    /// Build from noded directed segments, `weight_of(index)` giving each
    /// source segment's winding weight (a binary overlay tags operands
    /// with per-operand units). Face rings are not collected (see
    /// [`Arrangement::new`]).
    pub(crate) fn weighted(segments: &[Segment], weight_of: impl Fn(usize) -> W) -> Self {
        Self::build::<false>(segments, weight_of)
    }

    pub(crate) fn build<const RINGS: bool>(
        segments: &[Segment],
        weight_of: impl Fn(usize) -> W,
    ) -> Self {
        let mut spares = arrangement_spares();
        let mut typed = typed_spares::<W>();
        let (ids, points, edges, connected) = dedup_vertices_and_edges(
            segments,
            weight_of,
            std::mem::take(&mut spares.ids),
            std::mem::take(&mut spares.points),
            std::mem::take(&mut typed.edges),
        );
        let vertex_count = points.len();
        let (starts, mut targets, owners, mut multiplicities) = build_csr(
            vertex_count,
            &edges,
            std::mem::take(&mut spares.starts),
            std::mem::take(&mut spares.targets),
            std::mem::take(&mut spares.owners),
            std::mem::take(&mut typed.multiplicities),
        );
        typed.edges = edges;
        typed.edges.clear();
        sort_rows_counterclockwise(&points, &starts, &mut targets, &mut multiplicities);
        let mut twins = std::mem::take(&mut spares.twins);
        populate_twins(&mut twins, &targets, &owners);
        Self::finish_topology::<RINGS>(
            Columns {
                points,
                starts,
                targets,
                owners,
                twins,
                multiplicities,
            },
            ids,
            connected,
            FinishSpares {
                component_of: std::mem::take(&mut spares.component_of),
                face_of: std::mem::take(&mut spares.face_of),
                face_starts: std::mem::take(&mut spares.face_starts),
                face_slots: std::mem::take(&mut spares.face_slots),
            },
            spares,
            typed,
        )
    }

    /// Post-CSR assembly shared by every constructor: connected components,
    /// the global face walk, and the face CSR. The topology columns are
    /// already final — everything here is derived.
    pub(crate) fn finish_topology<const RINGS: bool>(
        columns: Columns<W>,
        ids: HashMap<PointKey, u32>,
        connected: bool,
        mut spares: FinishSpares,
        mut core: crate::scratch_pool::ScratchLease<
            arrangement::ArrangementSpares,
        >,
        mut typed: crate::scratch_pool::ScratchLease<TypedSpares<W>>,
    ) -> Self {
        let Columns {
            points,
            starts,
            targets,
            owners,
            twins,
            multiplicities,
        } = columns;
        let vertex_count = points.len();
        let half_edge_count = targets.len();
        // Components over vertices — skipped entirely when the construction
        // already PROVED connectivity (one chained closed loop).
        let (component_of, component_count) = if connected {
            let mut component_of = std::mem::take(&mut spares.component_of);
            component_of.clear();
            component_of.resize(vertex_count, 0);
            (component_of, 1)
        } else {
            let mut components = crate::collections::UnionFind::new(vertex_count);
            for slot in 0..half_edge_count {
                components.union(owners[slot] as usize, targets[slot] as usize);
            }
            // Roots are dense vertex indices: a sentinel-filled Vec replaces
            // a hash map for the first-seen component numbering.
            let mut component_id_of_root = std::mem::take(&mut core.component_id_of_root);
            component_id_of_root.clear();
            component_id_of_root.resize(vertex_count, u32::MAX);
            let mut component_count = 0_u32;
            let mut component_of = std::mem::take(&mut spares.component_of);
            component_of.clear();
            component_of.reserve(vertex_count);
            for vertex in 0..vertex_count {
                let id = &mut component_id_of_root[components.find(vertex)];
                if *id == u32::MAX {
                    *id = component_count;
                    component_count += 1;
                }
                component_of.push(*id);
            }
            core.component_id_of_root = component_id_of_root;
            (component_of, component_count)
        };
        let (face_of, faces) = walk_faces::<RINGS>(
            &points,
            &starts,
            &targets,
            &owners,
            &twins,
            &component_of,
            std::mem::take(&mut spares.face_of),
        );
        // CSR by face for the winding fill.
        let mut face_starts = std::mem::take(&mut spares.face_starts);
        face_starts.clear();
        face_starts.resize(faces.len() + 1, 0);
        for &face in &face_of {
            face_starts[face as usize + 1] += 1;
        }
        for index in 0..faces.len() {
            face_starts[index + 1] += face_starts[index];
        }
        let mut face_cursor = std::mem::take(&mut core.face_cursor);
        face_cursor.clear();
        face_cursor.extend_from_slice(&face_starts);
        let mut face_slots = std::mem::take(&mut spares.face_slots);
        face_slots.clear();
        face_slots.resize(half_edge_count, 0);
        for (slot, &face) in face_of.iter().enumerate() {
            face_slots[face_cursor[face as usize] as usize] = slot as u32;
            face_cursor[face as usize] += 1;
        }
        core.points = points;
        core.starts = starts;
        core.targets = targets;
        core.owners = owners;
        core.twins = twins;
        core.face_of = face_of;
        core.face_starts = face_starts;
        core.face_slots = face_slots;
        core.ids = ids;
        core.component_of = component_of;
        core.face_cursor = face_cursor;
        typed.multiplicities = multiplicities;
        Self {
            core,
            typed,
            faces,
            component_count,
            thread_bound: std::marker::PhantomData,
        }
    }

    /// Mutable face access — lets consumers `take` the rings they keep
    /// instead of cloning them out.
    pub(crate) fn faces_mut(&mut self) -> &mut [Face] {
        &mut self.faces
    }

    /// Winding number per face from one `seed` value per component
    /// (`seeds[component]` = the winding of that component's OUTER region),
    /// filled by BFS across twin half-edges: crossing `a -> b` from its
    /// left face onto its right face subtracts the half-edge's directed
    /// multiplicity.
    ///
    /// Precondition: the source segments form CLOSED cycles (every vertex
    /// has even degree). A dangling edge inside a face would create an
    /// extra zero-area walk that this seeding would mislabel with the
    /// component's outside winding.
    pub(crate) fn face_windings(&mut self, seeds: &[W]) -> Vec<W> {
        debug_assert_eq!(seeds.len(), self.component_count as usize);
        let mut windings = vec![W::UNSET; self.faces.len()];
        let mut queue = std::mem::take(&mut self.core.winding_queue);
        queue.clear();
        // Every component's outer face is its MOST NEGATIVE walk (it
        // encloses the whole component, so no other walk can be more
        // negative). Seeding anything else — in particular zero-area
        // degenerate faces from coincident strokes — would mislabel an
        // interior face with the outside winding and poison the fill.
        let mut outer = std::mem::take(&mut self.core.outer);
        outer.clear();
        outer.resize(self.component_count as usize, None);
        for (face_id, face) in self.faces.iter().enumerate() {
            let slot = &mut outer[face.component as usize];
            let better = slot.is_none_or(|current| {
                face.area_sign
                    .prefers_as_outer_face_than(self.faces[current as usize].area_sign)
            });
            if better {
                *slot = Some(face_id as u32);
            }
        }
        for (component, face_id) in outer.iter().enumerate() {
            if let Some(face_id) = face_id {
                windings[*face_id as usize] = seeds[component];
                queue.push_back(*face_id);
            }
        }
        while let Some(face_id) = queue.pop_front() {
            let winding = windings[face_id as usize];
            // The half-edge `a -> b` has its face on the LEFT; crossing to
            // the twin's face (the right side) subtracts its multiplicity.
            let range = self.face_starts[face_id as usize] as usize
                ..self.face_starts[face_id as usize + 1] as usize;
            for &slot in &self.face_slots[range] {
                let slot = slot as usize;
                let twin = self.twin_slot(slot);
                let neighbor = self.face_of[twin];
                if windings[neighbor as usize] != W::UNSET {
                    continue;
                }
                windings[neighbor as usize] = winding.sub(self.typed.multiplicities[slot]);
                queue.push_back(neighbor);
            }
        }
        // Faces the BFS could not reach belong to fully degenerate
        // sub-structures (zero-area coincident strokes); they enclose
        // nothing and take their component's outside winding.
        for (face_id, winding) in windings.iter_mut().enumerate() {
            if *winding == W::UNSET {
                *winding = seeds[self.faces[face_id].component as usize];
            }
        }
        self.core.outer = outer;
        self.core.winding_queue = queue;
        windings
    }

    /// One arbitrary vertex per component — the seed-probe locations.
    pub(crate) fn component_probes(&self) -> Vec<XY> {
        let mut points = Vec::new();
        self.component_probes_into(&mut points);
        points
    }

    pub(crate) fn component_probes_into(&self, points: &mut Vec<XY>) {
        let mut probes = vec![None; self.component_count as usize];
        self.component_probes_from_slots(points, &mut probes);
    }

    pub(crate) fn component_probes_from_slots(
        &self,
        points: &mut Vec<XY>,
        probes: &mut Vec<Option<XY>>,
    ) {
        probes.clear();
        probes.resize(self.component_count as usize, None);
        for (vertex, &component) in self.component_of.iter().enumerate() {
            let slot = &mut probes[component as usize];
            if slot.is_none() {
                *slot = Some(self.points[vertex]);
            }
        }
        // Every component is seeded by at least one vertex above.
        points.clear();
        points.extend(
            probes
                .iter()
                .copied()
                .map(|point| point.expect("each component has a probe vertex")),
        );
    }

    /// Component id of the loop whose first point is `point` (must be a
    /// vertex of the arrangement).
    pub(crate) fn component_of_point(&self, point: XY) -> u32 {
        let vertex = self.ids[&PointKey::new(point)];
        self.component_of[vertex as usize]
    }

    /// Boundary rings of the region whose faces pass `keep`, walked
    /// directly on the half-edges: every boundary half-edge (kept face on
    /// the LEFT, twin's face not) lies on exactly one ring; the successor
    /// at each head vertex is the first boundary half-edge clockwise of
    /// the arrival reversal (the fan swept between them is the arrival's
    /// own kept region). A walk through a PINCH vertex — the region
    /// touching itself at a point — can revisit it (two corner-touching
    /// lobes, or a hole touching its shell: no single local successor
    /// rule resolves both), so every emitted walk is split at repeated
    /// vertices into SIMPLE rings, whose shoelace sign then classifies
    /// shell vs hole (the JTS maximal-to-minimal edge-ring resolution).
    /// Shells come out counter-clockwise, holes clockwise — rings are
    /// closed (first point repeated).
    pub(crate) fn region_rings(&self, windings: &[W], keep: impl Fn(W) -> bool) -> Vec<Vec<XY>> {
        // A half-edge is on the kept region's boundary iff its left face is
        // kept and its twin's (right) face is not.
        let is_boundary = |slot| {
            let twin = self.twin_slot(slot);
            keep(windings[self.face_of[slot] as usize])
                && !keep(windings[self.face_of[twin] as usize])
        };
        let half_edge_count = self.targets.len();
        let mut spares = region_spares();
        let mut used = std::mem::take(&mut spares.used);
        used.clear();
        used.resize(half_edge_count, false);
        let mut rings = Vec::new();
        // Pinch-splitter scratch, reused across rings AND across calls:
        // vertices carry DENSE ids, so a generation-stamped position table
        // replaces the old per-vertex coordinate hashing entirely.
        let mut position_of = std::mem::take(&mut spares.position_of);
        position_of.clear();
        position_of.resize(self.points.len(), (0, 0));
        let mut generation = 0_u32;
        let mut ring = std::mem::take(&mut spares.ring);
        let mut ring_ids = std::mem::take(&mut spares.ring_ids);
        let mut path = std::mem::take(&mut spares.path);
        let mut path_ids = std::mem::take(&mut spares.path_ids);
        for seed in 0..half_edge_count {
            if used[seed] || !is_boundary(seed) {
                continue;
            }
            ring.clear();
            ring_ids.clear();
            let tail = self.owners[seed];
            ring.push(self.points[tail as usize]);
            ring_ids.push(tail);
            let mut slot = seed;
            loop {
                used[slot] = true;
                let head = self.targets[slot];
                ring.push(self.points[head as usize]);
                ring_ids.push(head);
                let row =
                    self.starts[head as usize] as usize..self.starts[head as usize + 1] as usize;
                let row_len = row.end - row.start;
                let reverse = self.twin_slot(slot);
                let mut position = reverse - row.start;
                let next = loop {
                    position = wrap_index(position + row_len - 1, row_len);
                    let candidate = row.start + position;
                    if is_boundary(candidate) {
                        break candidate;
                    }
                    debug_assert_ne!(
                        candidate, reverse,
                        "boundary walk must progress past the reversal"
                    );
                };
                slot = next;
                if slot == seed {
                    break;
                }
            }
            generation += 1;
            split_ring_at_pinches(
                &ring,
                &ring_ids,
                &mut rings,
                &mut position_of,
                generation,
                &mut path,
                &mut path_ids,
            );
        }
        spares.used = used;
        spares.position_of = position_of;
        spares.ring = ring;
        spares.ring_ids = ring_ids;
        spares.path = path;
        spares.path_ids = path_ids;
        rings
    }

    pub(crate) fn slot(&self, from: u32, to: u32) -> usize {
        let mut range =
            self.starts[from as usize] as usize..self.starts[from as usize + 1] as usize;
        range
            .find(|&slot| self.targets[slot] == to)
            .expect("twin half-edge exists")
    }

    pub(crate) fn twin_slot(&self, slot: usize) -> usize {
        self.twins[slot] as usize
    }

    #[cfg(test)]
    pub(crate) fn vertex_count(&self) -> usize {
        self.points.len()
    }

    /// Visit every undirected edge piece once (its lower-id half-edge):
    /// the directed multiplicity, both endpoint vertex ids, and the faces
    /// on its two sides — the native relate's boundary-row currency.
    pub(crate) fn for_each_edge_piece(&self, mut visit: impl FnMut(W, u32, u32, u32, u32)) {
        for slot in 0..self.targets.len() {
            let from = self.owners[slot];
            let to = self.targets[slot];
            if from >= to {
                continue;
            }
            let twin = self.twin_slot(slot);
            visit(
                self.typed.multiplicities[slot],
                from,
                to,
                self.face_of[slot],
                self.face_of[twin],
            );
        }
    }
}

/// Build the reciprocal half-edge column after angular row ordering. Every
/// retained entry is one `u32` (four bytes) per half-edge.
fn populate_twins(twins: &mut Vec<u32>, targets: &[u32], owners: &[u32]) {
    twins.clear();
    twins.resize(targets.len(), u32::MAX);
    let mut scratch = twin_slot_spares();
    scratch.by_endpoints.clear();
    scratch.by_endpoints.reserve(targets.len());
    for (slot, (&owner, &target)) in std::iter::zip(owners, targets).enumerate() {
        assert!(
            scratch
                .by_endpoints
                .insert((owner, target), slot as u32)
                .is_none()
        );
    }
    for (slot, (&owner, &target)) in std::iter::zip(owners, targets).enumerate() {
        twins[slot] = scratch.by_endpoints[&(target, owner)];
    }
}
