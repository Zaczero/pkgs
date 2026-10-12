//! General DCEL assignment and conflict discovery.

use super::super::clip::SegmentIntersectionExt as _;
use super::super::{
    Bounds, BoxIndex, CertifiedPrimalEdge, ClipHostFailure, ClipRingParity, ClipTrace, ControlFlow,
    EdgeId, EmbeddedVertices, EvaluationBudget, ExactCellPart, ExactSign, ExactSubdivision,
    LiveEdgeIncidence, NodeId, NormalizedClip, Ordering, OwnerTransition, PointKey,
    PolygonPointIndex, ProximityChord, Result, SearchAccount, Segment, Site, SiteAdjacency, SiteId,
    VertexId, VoronoiEmbedding, VoronoiFan, VoronoiHostGraph, VoronoiOwnerPoints, VoronoiPoints,
    VoronoiRing, XY, YStabbingIndex, clip_certificate_data, clip_source_trace_error,
    emitted_pair_preserves_incidence, exact, exact_cycle_error, for_each_candidate_pair,
    for_each_overlapping_bounds_cross_pair, rounded_segment_covered_by_clip, single_chain,
    site_by_id, try_hash_map_with_capacity, visit_component_faces, voronoi_error,
};
use super::fans::{FanSortMode, FanSortOutcome, rounded_ring_orientation_matches, sort_fan};

impl SiteAdjacency {
    #[expect(
        clippy::indexing_slicing,
        reason = "SiteId is admitted against the canonical site count and offsets has one terminal entry for each site row"
    )]
    fn neighbors(&self, site: SiteId) -> &[SiteId] {
        let site = site.0 as usize;
        let start = self.offsets[site] as usize;
        let end = self.offsets[site + 1] as usize;
        &self.neighbors[start..end]
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "CertifiedPrimalEdge endpoints are site ordinals from the already-admitted mesh; degrees/cursors/neighbors share that domain, and cumulative u32 offsets are checked before publication"
)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn certified_site_adjacency(
    edges: &[CertifiedPrimalEdge],
    site_count: usize,
) -> Result<SiteAdjacency> {
    let mut degrees = vec![0_usize; site_count];
    for edge in edges {
        let sites = match *edge {
            CertifiedPrimalEdge::Interior { sites, .. }
            | CertifiedPrimalEdge::Hull { sites, .. } => sites,
        };
        let [left, right] = sites;
        degrees[left] += 1;
        degrees[right] += 1;
    }
    let mut offsets = Vec::with_capacity(site_count + 1);
    offsets.push(0_u32);
    for degree in degrees {
        let next = (offsets.last().copied().unwrap_or(0) as usize)
            .checked_add(degree)
            .ok_or_else(|| voronoi_error("too many site adjacencies"))?;
        offsets.push(u32::try_from(next).map_err(|_| voronoi_error("too many site adjacencies"))?);
    }
    let neighbor_count = offsets.last().copied().unwrap_or(0) as usize;
    let mut neighbors = Vec::with_capacity(neighbor_count);
    neighbors.resize(neighbor_count, SiteId(0));
    let mut cursors = Vec::with_capacity(site_count);
    cursors.extend(
        offsets
            .iter()
            .take(site_count)
            .map(|offset| *offset as usize),
    );
    for edge in edges {
        let sites = match *edge {
            CertifiedPrimalEdge::Interior { sites, .. }
            | CertifiedPrimalEdge::Hull { sites, .. } => sites,
        };
        let [left, right] = sites;
        neighbors[cursors[left]] = SiteId(right as u32);
        cursors[left] += 1;
        neighbors[cursors[right]] = SiteId(left as u32);
        cursors[right] += 1;
    }
    for &[start, end] in offsets.array_windows::<2>() {
        neighbors[start as usize..end as usize].sort_unstable();
    }
    Ok(SiteAdjacency { offsets, neighbors })
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn nearest_site_walk(
    point: &exact::ExactPoint,
    sites: &[Site],
    adjacency: &SiteAdjacency,
    mut current: SiteId,
) -> Result<SiteId> {
    for _ in 0..sites.len() {
        let neighbors = adjacency.neighbors(current);
        let mut best = current;
        let nearer = |candidate: SiteId, known: SiteId| {
            exact::squared_distance_cmp_points(
                point,
                &site_by_id(sites, candidate).exact_point,
                &site_by_id(sites, known).exact_point,
            )
        };
        for &candidate in neighbors {
            if nearer(candidate, best)? == Ordering::Less {
                best = candidate;
            }
        }
        if best != current {
            current = best;
            continue;
        }
        for &candidate in neighbors {
            if nearer(candidate, current)? == Ordering::Equal {
                return Err(voronoi_error(
                    "private exact frame atom has a tied nearest site",
                ));
            }
        }
        return Ok(current);
    }
    Err(voronoi_error("certified nearest-site walk did not descend"))
}

#[cfg(test)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn nearest_site(
    point: &exact::ExactPoint,
    sites: &[Site],
) -> Result<SiteId> {
    let mut nearest = 0;
    let mut tied = false;
    for candidate in 1..sites.len() {
        match exact::squared_distance_cmp_points(
            point,
            &sites[candidate].exact_point,
            &sites[nearest].exact_point,
        )? {
            Ordering::Less => {
                nearest = candidate;
                tied = false;
            },
            Ordering::Equal => tied = true,
            Ordering::Greater => {},
        }
    }
    if tied {
        return Err(voronoi_error(
            "private exact frame atom has a tied nearest site",
        ));
    }
    Ok(SiteId(
        u32::try_from(nearest).map_err(|_| voronoi_error("too many Voronoi sites"))?,
    ))
}

impl<'a> VoronoiEmbedding<'a> {
    pub(super) fn chord(&self, index: usize) -> ProximityChord {
        *self
            .chords
            .get(index)
            .expect("chord index belongs to the Voronoi embedding")
    }

    pub(super) fn ring(&self, index: usize) -> VoronoiRing {
        *self
            .rings
            .get(index)
            .expect("ring index belongs to the Voronoi embedding")
    }

    pub(super) fn ring_owner(&self, index: usize) -> SiteId {
        let ring = self.ring(index);
        self.parts
            .get(ring.part)
            .expect("ring part belongs to the Voronoi embedding")
            .owner
    }

    pub(super) fn visit_clip_ring_vertices(&self, ring: usize, mut visit: impl FnMut(VertexId)) {
        let ring = self
            .clip_trace
            .rings
            .get(ring)
            .expect("clip-host failure names an admitted clip ring");
        for atom in &ring.atoms {
            for vertex in self.subdivision.edge(atom.edge).endpoints {
                visit(vertex);
            }
        }
    }

    pub(super) fn new(
        subdivision: &'a ExactSubdivision,
        clip: &'a NormalizedClip<'a>,
        clip_trace: &'a ClipTrace,
        parts: &'a [ExactCellPart],
        includes_polygons: bool,
        live_nodes: &'a [VertexId],
        live_edges: &'a [EdgeId],
        vertices: &'a EmbeddedVertices,
        coordinates: &'a [XY],
        sites: &'a [Site],
    ) -> Result<Self> {
        let vertex_count = subdivision.vertices.len();
        let mut chords = Vec::with_capacity(live_edges.len());
        let mut incident_chords = Vec::new();
        incident_chords.resize_with(vertex_count, Vec::new);
        for (index, &edge_id) in live_edges.iter().enumerate() {
            let [start, end] = subdivision.edge(edge_id).endpoints;
            let nearest = Segment {
                start: vertices.point(start, coordinates)?,
                end: vertices.point(end, coordinates)?,
            };
            let chord = ProximityChord {
                nodes: [start.0, end.0],
                nearest,
                can_move: [start, end].map(|vertex| subdivision.vertex(vertex).fixed_xy.is_none()),
            };
            incident_chords
                .get_mut(start.0 as usize)
                .expect("live edge start belongs to the admitted vertex table")
                .push(index);
            if end != start {
                incident_chords
                    .get_mut(end.0 as usize)
                    .expect("live edge end belongs to the admitted vertex table")
                    .push(index);
            }
            chords.push(chord);
        }
        let incidence = LiveEdgeIncidence::build(subdivision, live_edges, vertex_count);
        let xy_of = |vertex| vertices.point(vertex, coordinates);
        let mut fans = Vec::new();
        for (origin, edges) in incidence.rows() {
            if edges.len() <= 2
                || (!includes_polygons
                    && edges
                        .iter()
                        .all(|edge| subdivision.edge(*edge).separator.is_none()))
            {
                continue;
            }
            let mut exact_order = Vec::new();
            sort_fan(
                origin,
                edges,
                FanSortMode::Exact,
                subdivision,
                &xy_of,
                &mut exact_order,
            )?;
            fans.push(VoronoiFan {
                origin,
                edges: edges.to_vec(),
                exact_order,
            });
        }
        let mut rings = Vec::new();
        for (part, exact_part) in parts.iter().enumerate() {
            rings.push(VoronoiRing { part, hole: None });
            rings.extend(
                exact_part
                    .holes
                    .iter()
                    .enumerate()
                    .map(|(hole, _)| VoronoiRing {
                        part,
                        hole: Some(hole),
                    }),
            );
        }
        let mut rings_by_node = Vec::new();
        rings_by_node.resize_with(vertex_count, Vec::new);
        for (ring_index, ring_ref) in rings.iter().copied().enumerate() {
            let (_, open_vertices) = ring_ref
                .exact(parts)
                .vertices
                .split_last()
                .expect("simple Voronoi rings are closed");
            for &vertex in open_vertices {
                rings_by_node
                    .get_mut(vertex.0 as usize)
                    .expect("ring vertex belongs to the admitted vertex table")
                    .push(ring_index);
            }
        }

        let mut edge_to_chord = Vec::new();
        edge_to_chord.resize(subdivision.edges.len(), usize::MAX);
        for (chord, &edge_id) in live_edges.iter().enumerate() {
            *edge_to_chord
                .get_mut(edge_id.0 as usize)
                .expect("live edge belongs to the exact subdivision") = chord;
        }
        let mut host_graphs = Vec::new();
        if includes_polygons {
            let unassigned_part = u32::try_from(parts.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
            let mut face_part = Vec::new();
            face_part.resize(subdivision.labels.len(), unassigned_part);
            for (part_index, part) in parts.iter().enumerate() {
                let part_index = u32::try_from(part_index)
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                for ring in std::iter::once(&part.shell).chain(&part.holes) {
                    let face = face_part
                        .get_mut(ring.face.0 as usize)
                        .ok_or_else(exact_cycle_error)?;
                    if *face != unassigned_part && *face != part_index {
                        return Err(voronoi_error(
                            "one exact face belongs to multiple emitted Voronoi parts",
                        ));
                    }
                    *face = part_index;
                }
            }
            let mut edge_faces = Vec::new();
            edge_faces.resize(subdivision.edges.len(), [u32::MAX; 2]);
            for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
                let dart = subdivision.dart(slot as u32);
                let endpoints = subdivision.edge(EdgeId(edge)).endpoints;
                let side = usize::from(dart.origin != endpoints[0]);
                *edge_faces
                    .get_mut(edge as usize)
                    .and_then(|sides| sides.get_mut(side))
                    .ok_or_else(exact_cycle_error)? = dart.face.0;
            }
            let mut part_start = 0;
            while part_start < parts.len() {
                let owner = parts
                    .get(part_start)
                    .expect("owner group starts within the sorted part table")
                    .owner;
                let mut part_end = part_start.wrapping_add(1);
                while part_end < parts.len()
                    && parts
                        .get(part_end)
                        .expect("owner group ends within the sorted part table")
                        .owner
                        == owner
                {
                    part_end = part_end.wrapping_add(1);
                }
                let owner_parts = parts
                    .get(part_start..part_end)
                    .expect("owner parts form a bounded slice");
                let mut owner_edges = Vec::new();
                for part in owner_parts {
                    for ring in std::iter::once(&part.shell).chain(&part.holes) {
                        owner_edges.extend_from_slice(&ring.edges);
                    }
                }
                owner_edges.sort_unstable();
                owner_edges.dedup();
                let mut owner_vertices = Vec::with_capacity(owner_edges.len());
                for &edge in &owner_edges {
                    owner_vertices.extend(subdivision.edge(edge).endpoints.map(|vertex| vertex.0));
                }
                owner_vertices.sort_unstable();
                owner_vertices.dedup();
                let mut edges = Vec::with_capacity(owner_edges.len());
                let mut sides = Vec::with_capacity(owner_edges.len());
                let mut chords = Vec::with_capacity(owner_edges.len());
                for edge in owner_edges {
                    let endpoints = subdivision.edge(edge).endpoints;
                    let mut local_endpoints = [0_u32; 2];
                    for (local, vertex) in local_endpoints.iter_mut().zip(endpoints) {
                        *local = u32::try_from(
                            owner_vertices
                                .binary_search(&vertex.0)
                                .map_err(|_| exact_cycle_error())?,
                        )
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                    }
                    let exact_faces = *edge_faces
                        .get(edge.0 as usize)
                        .ok_or_else(exact_cycle_error)?;
                    let mut owner_sides = [0_u32; 2];
                    for (owner_side, face) in owner_sides.iter_mut().zip(exact_faces) {
                        if face == u32::MAX {
                            return Err(exact_cycle_error());
                        }
                        let part = face_part
                            .get(face as usize)
                            .copied()
                            .ok_or_else(exact_cycle_error)?;
                        if part != unassigned_part
                            && parts
                                .get(part as usize)
                                .is_some_and(|part| part.owner == owner)
                        {
                            *owner_side = part
                                .checked_add(1)
                                .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        }
                    }
                    let chord = *edge_to_chord
                        .get(edge.0 as usize)
                        .filter(|&&chord| chord != usize::MAX)
                        .ok_or_else(exact_cycle_error)?;
                    edges.push(local_endpoints);
                    sides.push(owner_sides);
                    chords.push(chord);
                }
                host_graphs.push(VoronoiHostGraph {
                    owner,
                    vertices: owner_vertices,
                    edges,
                    sides,
                    chords,
                });
                part_start = part_end;
            }
        }
        let separator_chords = live_edges
            .iter()
            .enumerate()
            .filter_map(|(index, &edge)| {
                subdivision.edge(edge).separator.is_some().then_some(index)
            })
            .collect();

        Ok(Self {
            subdivision,
            clip,
            clip_trace,
            sites,
            parts,
            includes_polygons,
            live_nodes,
            live_edges,
            vertices,
            coordinates,
            chords,
            incident_chords,
            fans,
            rings,
            rings_by_node,
            host_graphs,
            separator_chords,
        })
    }

    pub(super) fn xy(&self, vertex: VertexId, coordinates: &[XY]) -> Result<XY> {
        self.vertices.point(vertex, coordinates)
    }

    pub(super) fn exact_movable(&self, vertex: VertexId) -> bool {
        self.subdivision.vertex(vertex).fixed_xy.is_none()
    }

    pub(super) fn compare_nodes(&self, left: VertexId, right: VertexId) -> Result<Ordering> {
        self.subdivision
            .vertex(left)
            .point
            .compare_lex(&self.subdivision.vertex(right).point)
    }

    pub(super) fn ring_orientation(&self, ring: usize, coordinates: &[XY]) -> Result<bool> {
        let ring = self
            .rings
            .get(ring)
            .copied()
            .expect("ring index belongs to the Voronoi ring table")
            .exact(self.parts);
        rounded_ring_orientation_matches(&ring.vertices, ring.orientation, |vertex| {
            self.xy(vertex, coordinates)
        })
    }

    pub(super) fn extend_ring_seeds(&self, ring: usize, seeds: &mut Vec<VertexId>) {
        let ring = self
            .rings
            .get(ring)
            .copied()
            .expect("ring index belongs to the Voronoi ring table")
            .exact(self.parts);
        let (_, open_vertices) = ring
            .vertices
            .split_last()
            .expect("simple Voronoi rings are closed");
        seeds.extend_from_slice(open_vertices);
    }

    pub(super) fn segment(&self, chord: usize, coordinates: &[XY]) -> Result<Segment> {
        let [start, end] = self
            .subdivision
            .edge(
                *self
                    .live_edges
                    .get(chord)
                    .expect("chord index belongs to the Voronoi edge table"),
            )
            .endpoints;
        Ok(Segment {
            start: self.xy(start, coordinates)?,
            end: self.xy(end, coordinates)?,
        })
    }

    pub(super) fn certifies_incidence_pair(&self, left: usize, right: usize) -> bool {
        self.includes_polygons
            || [left, right].into_iter().any(|chord| {
                let edge = *self
                    .live_edges
                    .get(chord)
                    .expect("incidence-pair index belongs to the live edge table");
                self.subdivision.edge(edge).separator.is_some()
            })
    }

    pub(super) fn clip_host_failures<A: SearchAccount>(
        &self,
        coordinates: &[XY],
        account: &mut A,
    ) -> Result<Vec<ClipHostFailure>> {
        if self.separator_chords.is_empty() {
            return Ok(Vec::new());
        }
        let certificate = clip_certificate_data(
            self.subdivision,
            self.clip,
            self.clip_trace,
            |vertex| self.xy(vertex, coordinates),
            account,
        )?;
        let clip_edge_count = certificate.edges.len();
        let separator_count = self.separator_chords.len();
        let index_units = clip_edge_count
            .div_ceil(16)
            .checked_add(separator_count.div_ceil(16))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        account.charge_evaluations(
            u64::try_from(index_units).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let bounds = |start: XY, end: XY| {
            Bounds::new_raw(
                start.x.min(end.x),
                start.y.min(end.y),
                start.x.max(end.x),
                start.y.max(end.y),
            )
        };
        let clip_bounds: Vec<_> = certificate
            .edges
            .iter()
            .map(|edge| bounds(edge.rounded[0], edge.rounded[1]))
            .collect();
        let mut segments = Vec::with_capacity(separator_count);
        for &chord in &self.separator_chords {
            segments.push(self.segment(chord, coordinates)?);
        }
        let separator_bounds: Vec<_> = segments
            .iter()
            .map(|segment| bounds(segment.start, segment.end))
            .collect();

        let sweep_count = clip_edge_count
            .checked_add(separator_count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        account.charge_evaluations(
            u64::try_from(sweep_count.div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let mut candidate_pairs = Vec::new();
        let mut callback_error = None;
        let completion = for_each_overlapping_bounds_cross_pair(
            &separator_bounds,
            &clip_bounds,
            |segment, edge| {
                if callback_error.is_some() {
                    return ControlFlow::Break(());
                }
                if let Err(error) = account.charge_evaluations(1) {
                    callback_error = Some(error);
                    return ControlFlow::Break(());
                }
                candidate_pairs.push((segment, edge));
                ControlFlow::Continue(())
            },
        )?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        debug_assert!(completion.is_continue());
        candidate_pairs.sort_unstable();
        let candidate_units = u64::try_from(candidate_pairs.len().div_ceil(16))
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        account.charge_evaluations(candidate_units)?;

        let offset_count = separator_count
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut candidate_offsets = Vec::with_capacity(offset_count);
        let mut candidate_edges = Vec::with_capacity(candidate_pairs.len());
        candidate_offsets.push(0);
        let mut pairs = candidate_pairs.into_iter().peekable();
        for segment in 0..separator_count {
            while pairs
                .peek()
                .is_some_and(|&(candidate, _)| candidate == segment)
            {
                let (_, edge) = pairs
                    .next()
                    .expect("the candidate segment was checked above");
                candidate_edges.push(edge);
            }
            candidate_offsets.push(candidate_edges.len());
        }
        if pairs.next().is_some() {
            return Err(clip_source_trace_error());
        }

        account.charge_evaluations(
            u64::try_from(clip_edge_count.div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let edge_ids: Vec<_> = (0..clip_edge_count).collect();
        let y_index = YStabbingIndex::build(edge_ids, |&edge| {
            let [start, end] = certificate
                .edges
                .get(edge)
                .expect("YStabbingIndex ordinals align with the assigned clip edges")
                .rounded;
            (
                start.x.min(end.x),
                start.x.max(end.x),
                start.y.min(end.y),
                start.y.max(end.y),
            )
        })?;
        account.charge_evaluations(
            u64::try_from(self.clip_trace.atom_count.div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let point_index = PolygonPointIndex::build(&certificate.polygon)?;
        let mut events = Vec::new();
        let mut parity = ClipRingParity::new(self.clip_trace.rings.len());
        let mut failures = Vec::new();
        for (segment_index, (&chord, &[start, end])) in self
            .separator_chords
            .iter()
            .zip(candidate_offsets.array_windows::<2>())
            .enumerate()
        {
            let candidates = candidate_edges
                .get(start..end)
                .ok_or_else(clip_source_trace_error)?;
            let segment = *segments
                .get(segment_index)
                .expect("separator segment ordinal belongs to the built segment table");
            if let Some(boundary) = rounded_segment_covered_by_clip(
                segment,
                &certificate.edges,
                candidates,
                &point_index,
                &y_index,
                &mut events,
                &mut parity,
                account,
            )? {
                failures.push(ClipHostFailure { chord, boundary });
            }
        }
        Ok(failures)
    }

    pub(super) fn separator_preserved(
        &self,
        chord: usize,
        mut xy_of: impl FnMut(VertexId) -> Result<XY>,
    ) -> Result<bool> {
        let edge_id = *self
            .live_edges
            .get(chord)
            .expect("separator chord belongs to the live edge table");
        let edge = self.subdivision.edge(edge_id);
        if edge.separator.is_none() {
            return Ok(true);
        }
        let [start, end] = edge.endpoints;
        let (start, end) = (
            exact::ExactPoint::from_xy(xy_of(start)?),
            exact::ExactPoint::from_xy(xy_of(end)?),
        );
        let side_of = |site: SiteId| {
            exact::orient_points(&start, &end, &site_by_id(self.sites, site).exact_point)
        };
        let (left, right) = match edge.forward.owner {
            OwnerTransition::Boundary { left, right } if left.is_some() || right.is_some() => {
                (left, right)
            },
            OwnerTransition::Boundary { .. } | OwnerTransition::Preserve => {
                return Err(voronoi_error("live separator has incomplete ownership"));
            },
        };
        let left_inside = match left {
            Some(site) => side_of(site)? == ExactSign::Positive,
            None => true,
        };
        let right_inside = match right {
            Some(site) => side_of(site)? == ExactSign::Negative,
            None => true,
        };
        Ok(left_inside && right_inside)
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::tessellation::voronoi_dcel) enum VoronoiConflict {
    Collision(VertexId, VertexId),
    Contact(usize, usize),
    ClipHost(ClipHostFailure),
    Fan(VertexId),
    Separator(usize),
    Ring(usize),
    Host {
        owner: SiteId,
        root: VertexId,
        hit: Option<usize>,
    },
}

impl VoronoiEmbedding<'_> {
    pub(in crate::geometry::tessellation::voronoi_dcel) fn collect_conflicts(
        &self,
        coordinates: &[XY],
        budget: &mut EvaluationBudget,
    ) -> Result<Vec<VoronoiConflict>> {
        let mut conflicts = Vec::new();
        let mut owners = try_hash_map_with_capacity(self.live_nodes.len())?;
        for &vertex in self.live_nodes {
            let point = self.xy(vertex, coordinates)?;
            if let Some(previous) = owners.insert(PointKey::new(point), vertex)
                && previous != vertex
            {
                conflicts.push(VoronoiConflict::Collision(previous, vertex));
            }
        }

        let mut segments = Vec::with_capacity(self.chords.len());
        for chord in 0..self.chords.len() {
            segments.push(self.segment(chord, coordinates)?);
        }
        let pair_completion = for_each_candidate_pair(&segments, single_chain, |left, right| {
            if self.certifies_incidence_pair(left, right) {
                let endpoints = |index: usize| {
                    let chord = self.chord(index);
                    let [start, end] = chord.nodes;
                    let segment = *segments
                        .get(index)
                        .expect("candidate-pair index belongs to the emitted segment table");
                    [(NodeId(start), segment.start), (NodeId(end), segment.end)]
                };
                if !emitted_pair_preserves_incidence(endpoints(left), endpoints(right)) {
                    conflicts.push(VoronoiConflict::Contact(left, right));
                }
            }
            ControlFlow::<()>::Continue(())
        })?;
        debug_assert!(pair_completion.is_continue());

        let mut marked = Vec::new();
        let mut marked_boxes = Vec::new();
        let mut marked_chords = Vec::new();
        for (chord, &edge_id) in self.live_edges.iter().enumerate() {
            if self.subdivision.edge(edge_id).separator.is_none() {
                continue;
            }
            let segment = *segments
                .get(chord)
                .expect("separator chord belongs to the emitted segment table");
            marked.push([segment.start, segment.end]);
            marked_boxes.push(Some(Bounds::new_raw(
                segment.start.x.min(segment.end.x),
                segment.start.y.min(segment.end.y),
                segment.start.x.max(segment.end.x),
                segment.start.y.max(segment.end.y),
            )));
            marked_chords.push(chord);
        }
        let marked_index = BoxIndex::build(&marked_boxes)?;
        for site in self.sites {
            let query = &site.exact_point;
            let visit = |separator: usize| -> Result<()> {
                let [start, end] = *marked
                    .get(separator)
                    .expect("separator index belongs to the marked segment table");
                if exact::segment_intersection(
                    &exact::ExactPoint::from_xy(start),
                    &exact::ExactPoint::from_xy(end),
                    query,
                    query,
                )?
                .is_point()
                {
                    conflicts.push(VoronoiConflict::Separator(
                        *marked_chords
                            .get(separator)
                            .expect("marked edge maps to its emitted chord"),
                    ));
                }
                Ok(())
            };
            marked_index.for_each_containing(Some(site.rounded_xy()?), visit)?;
        }

        for chord in 0..self.live_edges.len() {
            if !self.separator_preserved(chord, |vertex| self.xy(vertex, coordinates))? {
                conflicts.push(VoronoiConflict::Separator(chord));
            }
        }
        for failure in self.clip_host_failures(coordinates, budget)? {
            conflicts.push(VoronoiConflict::ClipHost(failure));
        }

        for fan in &self.fans {
            let xy_of = |vertex| self.xy(vertex, coordinates);
            let mut rounded_order = Vec::new();
            if sort_fan(
                fan.origin,
                &fan.edges,
                FanSortMode::Rounded,
                self.subdivision,
                &xy_of,
                &mut rounded_order,
            )? != FanSortOutcome::Sorted
                || rounded_order != fan.exact_order
            {
                conflicts.push(VoronoiConflict::Fan(fan.origin));
            }
        }
        for ring in 0..self.rings.len() {
            if !self.ring_orientation(ring, coordinates)? {
                conflicts.push(VoronoiConflict::Ring(ring));
            }
        }

        let points = VoronoiPoints {
            vertices: self.vertices,
            coordinates,
        };
        for host_graph in &self.host_graphs {
            let owner_points = VoronoiOwnerPoints {
                points: &points,
                vertices: &host_graph.vertices,
            };
            let host_completion = visit_component_faces(
                &owner_points,
                host_graph.vertices.len(),
                host_graph.edges.len(),
                |edge| host_graph.edge(edge),
                |edge| host_graph.side_keys(edge),
                |root, hit| {
                    conflicts.push(VoronoiConflict::Host {
                        owner: host_graph.owner,
                        root: host_graph.vertex(root),
                        hit: hit.map(|edge| host_graph.chord(edge)),
                    });
                    ControlFlow::Continue(())
                },
            )?;
            debug_assert!(host_completion.is_continue());
        }
        Ok(conflicts)
    }

    pub(in crate::geometry::tessellation::voronoi_dcel) fn seed_nodes(
        &self,
        conflicts: &[VoronoiConflict],
    ) -> Vec<VertexId> {
        let mut seeds = Vec::new();
        for &conflict in conflicts {
            match conflict {
                VoronoiConflict::Collision(first, second) => {
                    seeds.extend([first, second]);
                },
                VoronoiConflict::Contact(left, right) => {
                    for chord in [left, right] {
                        let edge = self.chord(chord);
                        seeds.extend([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])]);
                    }
                },
                VoronoiConflict::ClipHost(failure) => {
                    let chord = self.chord(failure.chord);
                    seeds.extend([VertexId(chord.nodes[0]), VertexId(chord.nodes[1])]);
                    self.visit_clip_ring_vertices(failure.boundary.ring, |node| {
                        seeds.push(node);
                    });
                },
                VoronoiConflict::Fan(origin) => {
                    seeds.push(origin);
                    if let Some(fan) = self.fans.iter().find(|fan| fan.origin == origin) {
                        for edge_id in &fan.edges {
                            let [start, end] = self.subdivision.edge(*edge_id).endpoints;
                            seeds.extend([start, end]);
                        }
                    }
                },
                VoronoiConflict::Separator(chord) => {
                    let edge = self.chord(chord);
                    seeds.extend([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])]);
                },
                VoronoiConflict::Ring(ring) => self.extend_ring_seeds(ring, &mut seeds),
                VoronoiConflict::Host { owner, root, hit } => {
                    let rings = self
                        .rings_by_node
                        .get(root.0 as usize)
                        .expect("host root belongs to the Voronoi vertex domain");
                    let mut found_owner_ring = false;
                    for ring in rings
                        .iter()
                        .copied()
                        .filter(|&ring| self.ring_owner(ring) == owner)
                    {
                        found_owner_ring = true;
                        self.extend_ring_seeds(ring, &mut seeds);
                    }
                    if !found_owner_ring {
                        seeds.push(root);
                    }
                    if let Some(hit) = hit {
                        let edge = self.chord(hit);
                        seeds.extend([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])]);
                    }
                },
            }
        }
        seeds.sort_unstable();
        seeds.dedup();
        seeds
    }
}
