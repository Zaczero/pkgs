use super::*;
use crate::geometry::*;

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
        let spares = arrangement_spares_with(std::cell::RefCell::take);
        let typed = take_typed_spares::<i32>();
        let mut ids = spares.ids;
        ids.clear();
        let vertex_count = n + group_count as usize;
        let mut points = spares.points;
        points.clear();
        points.reserve(vertex_count);
        let (is_cut, pieces) = single_loop_pieces(segments, &cuts, group_count, &mut points);
        debug_assert_eq!(points.len(), vertex_count);
        // Degrees are structural — chain vertices 2, crossings 4 — so the
        // CSR shape needs no edge sort.
        let mut starts = spares.starts;
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
        let mut targets = spares.targets;
        targets.clear();
        targets.resize(half_edge_count, 0);
        let mut owners = spares.owners;
        owners.clear();
        owners.resize(half_edge_count, 0);
        let mut multiplicities = typed.multiplicities;
        multiplicities.clear();
        multiplicities.resize(half_edge_count, 0);
        let mut filled = vec![0_u32; vertex_count];
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
            arrangement_spares_with(|cell| {
                let mut spare = cell.borrow_mut();
                spare.points = points;
                spare.starts = starts;
                spare.targets = targets;
                spare.owners = owners;
                spare.ids = ids;
                spare.component_of = spares.component_of;
                spare.face_of = spares.face_of;
                spare.face_starts = spares.face_starts;
                spare.face_slots = spares.face_slots;
            });
            restore_typed_spares::<i32>(TypedSpares {
                edges: typed.edges,
                multiplicities,
            });
            return None;
        }
        // The edge buffer goes back unused (this path never builds one).
        restore_typed_spares::<i32>(TypedSpares {
            edges: typed.edges,
            multiplicities: Vec::new(),
        });
        Some(Self::finish_topology::<false>(
            Columns {
                points,
                starts,
                targets,
                owners,
                multiplicities,
            },
            ids,
            true,
            FinishSpares {
                component_of: spares.component_of,
                face_of: spares.face_of,
                face_starts: spares.face_starts,
                face_slots: spares.face_slots,
            },
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
        let spares = arrangement_spares_with(std::cell::RefCell::take);
        let typed = take_typed_spares::<W>();
        let mut ids = spares.ids;
        ids.clear();
        let vertex_count = n + group_count as usize;
        let mut points = spares.points;
        points.clear();
        points.reserve(vertex_count);
        let (is_cut, pieces, anchor_ids) =
            positional_loop_pieces(segments, loop_ranges, &cuts, group_count, &mut points);
        debug_assert_eq!(points.len(), vertex_count);
        let mut starts = spares.starts;
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
        let mut targets = spares.targets;
        targets.clear();
        targets.resize(half_edge_count, 0);
        let mut owners = spares.owners;
        owners.clear();
        owners.resize(half_edge_count, 0);
        let mut multiplicities = typed.multiplicities;
        multiplicities.clear();
        multiplicities.resize(half_edge_count, W::UNSET);
        let mut filled = vec![0_u32; vertex_count];
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
            arrangement_spares_with(|cell| {
                let mut spare = cell.borrow_mut();
                spare.points = points;
                spare.starts = starts;
                spare.targets = targets;
                spare.owners = owners;
                spare.ids = ids;
                spare.component_of = spares.component_of;
                spare.face_of = spares.face_of;
                spare.face_starts = spares.face_starts;
                spare.face_slots = spares.face_slots;
            });
            restore_typed_spares::<W>(TypedSpares {
                edges: typed.edges,
                multiplicities,
            });
            return None;
        }
        restore_typed_spares::<W>(TypedSpares {
            edges: typed.edges,
            multiplicities: Vec::new(),
        });
        // Only the loop ANCHORS are ever looked up (component probes map
        // anchors -> components); inserting all 40k vertices for K=2
        // lookups was pure waste. Duplicate keys already bailed.
        ids.reserve(anchor_ids.len());
        for &anchor in &anchor_ids {
            ids.insert(PointKey::new(points[anchor as usize]), anchor);
        }
        Some(Self::finish_topology::<false>(
            Columns {
                points,
                starts,
                targets,
                owners,
                multiplicities,
            },
            ids,
            false,
            FinishSpares {
                component_of: spares.component_of,
                face_of: spares.face_of,
                face_starts: spares.face_starts,
                face_slots: spares.face_slots,
            },
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
        let spares = arrangement_spares_with(std::cell::RefCell::take);
        let typed = take_typed_spares::<W>();
        let (ids, points, edges, connected) =
            dedup_vertices_and_edges(segments, weight_of, spares.ids, spares.points, typed.edges);
        let vertex_count = points.len();
        let (starts, mut targets, owners, mut multiplicities) = build_csr(
            vertex_count,
            &edges,
            spares.starts,
            spares.targets,
            spares.owners,
            typed.multiplicities,
        );
        // The edge list is fully consumed by the CSR build — return its
        // capacity now (multiplicities go back on Drop).
        restore_typed_spares::<W>(TypedSpares {
            edges: {
                let mut edges = edges;
                edges.clear();
                edges
            },
            multiplicities: Vec::new(),
        });
        sort_rows_counterclockwise(&points, &starts, &mut targets, &mut multiplicities);
        Self::finish_topology::<RINGS>(
            Columns {
                points,
                starts,
                targets,
                owners,
                multiplicities,
            },
            ids,
            connected,
            FinishSpares {
                component_of: spares.component_of,
                face_of: spares.face_of,
                face_starts: spares.face_starts,
                face_slots: spares.face_slots,
            },
        )
    }

    /// Post-CSR assembly shared by every constructor: connected components,
    /// the global face walk, and the face CSR. The topology columns are
    /// already final — everything here is derived.
    pub(crate) fn finish_topology<const RINGS: bool>(
        columns: Columns<W>,
        ids: HashMap<PointKey, u32>,
        connected: bool,
        spares: FinishSpares,
    ) -> Self {
        let Columns {
            points,
            starts,
            targets,
            owners,
            multiplicities,
        } = columns;
        let vertex_count = points.len();
        let half_edge_count = targets.len();
        // Components over vertices — skipped entirely when the construction
        // already PROVED connectivity (one chained closed loop).
        let (component_of, component_count) = if connected {
            let mut component_of = spares.component_of;
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
            let mut component_id_of_root = vec![u32::MAX; vertex_count];
            let mut component_count = 0_u32;
            let mut component_of = Vec::with_capacity(vertex_count);
            for vertex in 0..vertex_count {
                let id = &mut component_id_of_root[components.find(vertex)];
                if *id == u32::MAX {
                    *id = component_count;
                    component_count += 1;
                }
                component_of.push(*id);
            }
            (component_of, component_count)
        };
        let (face_of, faces) = walk_faces::<RINGS>(
            &points,
            &starts,
            &targets,
            &owners,
            &component_of,
            spares.face_of,
        );
        // CSR by face for the winding fill.
        let mut face_starts = spares.face_starts;
        face_starts.clear();
        face_starts.resize(faces.len() + 1, 0);
        for &face in &face_of {
            face_starts[face as usize + 1] += 1;
        }
        for index in 0..faces.len() {
            face_starts[index + 1] += face_starts[index];
        }
        let mut face_cursor = face_starts.clone();
        let mut face_slots = spares.face_slots;
        face_slots.clear();
        face_slots.resize(half_edge_count, 0);
        for (slot, &face) in face_of.iter().enumerate() {
            face_slots[face_cursor[face as usize] as usize] = slot as u32;
            face_cursor[face as usize] += 1;
        }
        Self {
            points,
            starts,
            targets,
            owners,
            multiplicities,
            face_of,
            face_starts,
            face_slots,
            faces,
            ids,
            component_of,
            component_count,
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
    pub(crate) fn face_windings(&self, seeds: &[W]) -> Vec<W> {
        debug_assert_eq!(seeds.len(), self.component_count as usize);
        let mut windings = vec![W::UNSET; self.faces.len()];
        let mut queue = std::collections::VecDeque::new();
        // Every component's outer face is its MOST NEGATIVE walk (it
        // encloses the whole component, so no other walk can be more
        // negative). Seeding anything else — in particular zero-area
        // degenerate faces from coincident strokes — would mislabel an
        // interior face with the outside winding and poison the fill.
        let mut outer: Vec<Option<u32>> = vec![None; self.component_count as usize];
        for (face_id, face) in self.faces.iter().enumerate() {
            let slot = &mut outer[face.component as usize];
            let better = slot.is_none_or(|current| {
                face.decision_area
                    .prefers_as_outer_face_than(self.faces[current as usize].decision_area)
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
                let twin = self.slot(self.targets[slot], self.owners[slot]);
                let neighbor = self.face_of[twin];
                if windings[neighbor as usize] != W::UNSET {
                    continue;
                }
                windings[neighbor as usize] = winding.sub(self.multiplicities[slot]);
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
        windings
    }

    /// One arbitrary vertex per component — the seed-probe locations.
    pub(crate) fn component_probes(&self) -> Vec<XY> {
        let mut probes = vec![None; self.component_count as usize];
        for (vertex, &component) in self.component_of.iter().enumerate() {
            let slot = &mut probes[component as usize];
            if slot.is_none() {
                *slot = Some(self.points[vertex]);
            }
        }
        // Every component is seeded by at least one vertex above.
        probes
            .into_iter()
            .map(|point| point.expect("each component has a probe vertex"))
            .collect()
    }

    /// Component id of the loop whose first point is `point` (must be a
    /// vertex of the arrangement).
    pub(crate) fn component_of_point(&self, point: XY) -> u32 {
        let vertex = self.ids[&PointKey::new(point)];
        self.component_of[vertex as usize]
    }

    /// Undirected edge count at `point` — the CSR row width (one slot per
    /// incident half-edge). Matches the polygonize segment-graph degree on
    /// noded linework.
    pub(crate) fn vertex_degree(&self, point: XY) -> usize {
        self.ids.get(&PointKey::new(point)).map_or(0, |&vertex| {
            let vertex = vertex as usize;
            (self.starts[vertex + 1] - self.starts[vertex]) as usize
        })
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
            let twin = self.slot(self.targets[slot], self.owners[slot]);
            keep(windings[self.face_of[slot] as usize])
                && !keep(windings[self.face_of[twin] as usize])
        };
        let half_edge_count = self.targets.len();
        let spares = region_spares_with(std::cell::RefCell::take);
        let mut used = spares.used;
        used.clear();
        used.resize(half_edge_count, false);
        let mut rings = Vec::new();
        // Pinch-splitter scratch, reused across rings AND across calls:
        // vertices carry DENSE ids, so a generation-stamped position table
        // replaces the old per-vertex coordinate hashing entirely.
        let mut position_of = spares.position_of;
        position_of.clear();
        position_of.resize(self.points.len(), (0, 0));
        let mut generation = 0_u32;
        let mut ring = spares.ring;
        let mut ring_ids = spares.ring_ids;
        let mut path = spares.path;
        let mut path_ids = spares.path_ids;
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
                let reverse = self.slot(head, self.owners[slot]);
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
        region_spares_with(|cell| {
            cell.replace(RegionSpares {
                used,
                position_of,
                ring,
                ring_ids,
                path,
                path_ids,
            })
        });
        rings
    }

    pub(crate) fn slot(&self, from: u32, to: u32) -> usize {
        let mut range =
            self.starts[from as usize] as usize..self.starts[from as usize + 1] as usize;
        range
            .find(|&slot| self.targets[slot] == to)
            .expect("twin half-edge exists")
    }

    pub(crate) const fn vertex_count(&self) -> usize {
        self.points.len()
    }

    /// The noded coordinate of vertex `vertex` — touch-point extraction's
    /// id-to-point map.
    pub(crate) fn vertex_point(&self, vertex: u32) -> XY {
        self.points[vertex as usize]
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
            let twin = self.slot(to, from);
            visit(
                self.multiplicities[slot],
                from,
                to,
                self.face_of[slot],
                self.face_of[twin],
            );
        }
    }
}
