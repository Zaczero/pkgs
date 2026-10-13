//! Candidate search and repair problem contract.

use super::super::clip::SegmentIntersectionExt as _;
use super::super::{
    Bounds, ClassAssignment, ControlFlow, GlobalCheck, NodeId, Ordering, PointKey, Problem,
    RefCell, Result, SearchAccount, VertexId, VoronoiClassState, VoronoiEmbedding,
    VoronoiOwnerPoints, VoronoiPoints, VoronoiProblem, VoronoiProximity, Work, XY,
    build_proximity_classes, emitted_pair_preserves_incidence, exact, exact_chord_radius,
    for_each_overlapping_bounds_cross_pair, possible_swept_bounds, proximity_limit,
    segments_within_chebyshev, try_hash_map_with_capacity, visit_component_faces, voronoi_error,
};
use super::fans::{FanSortMode, FanSortOutcome, rounded_ring_orientation_matches, sort_fan};

impl<'p, 'a> VoronoiProblem<'p, 'a> {
    pub(in crate::geometry::tessellation::voronoi_dcel) fn new(
        embedding: &'p VoronoiEmbedding<'a>,
        seeds: &[VertexId],
    ) -> Result<Self> {
        let node_count = embedding.subdivision.vertices.len();
        let mut movable_cache = vec![None; node_count];
        let mut search_seeds = seeds.to_vec();
        search_seeds.sort_unstable();
        search_seeds.dedup();
        let seed_ids: Vec<_> = seeds.iter().map(|node| node.0).collect();
        let proximity = build_proximity_classes(
            node_count,
            &embedding.chords,
            &embedding.incident_chords,
            &seed_ids,
            &mut movable_cache,
            |node| Ok(embedding.exact_movable(VertexId(node))),
            |left, right| {
                let (left, right) = (VertexId(left), VertexId(right));
                let left_seeded = search_seeds.binary_search(&left).is_ok();
                let right_seeded = search_seeds.binary_search(&right).is_ok();
                Ok(if left_seeded == right_seeded {
                    embedding.compare_nodes(left, right)?
                } else {
                    right_seeded.cmp(&left_seeded)
                })
            },
            segments_within_chebyshev,
        )?;
        let classes: Vec<Vec<VertexId>> = proximity
            .classes
            .into_iter()
            .map(|class| class.into_iter().map(VertexId).collect())
            .collect();
        Ok(Self {
            embedding,
            classes,
            search_seeds,
            node_location: proximity.node_location,
            radius_cache: RefCell::new(proximity.radii),
            movable_cache: RefCell::new(movable_cache),
            proximity: RefCell::new(None),
            initial_evaluations: proximity.initial_evaluations,
        })
    }

    fn local_xy(&self, node: VertexId, class: usize, assigned: &[Option<XY>]) -> XY {
        match self.node_location.get(node.0 as usize).copied().flatten() {
            Some((owner, rank)) if owner == class => assigned
                .get(rank)
                .copied()
                .flatten()
                .expect("ready Voronoi predicates have each class endpoint assigned"),
            _ => self
                .embedding
                .xy(node, self.embedding.coordinates)
                .expect("every checked Voronoi vertex is live"),
        }
    }

    fn predicate_class_rank(
        &self,
        nodes: impl IntoIterator<Item = VertexId>,
    ) -> Result<Option<(usize, usize)>> {
        let mut class = None;
        let mut ready_rank = 0;
        for node in nodes {
            if let Some((owner, rank)) = self.node_location.get(node.0 as usize).copied().flatten()
            {
                if class.is_some_and(|known| known != owner) {
                    return Err(voronoi_error(
                        "embedding proximity split one Voronoi predicate across classes",
                    ));
                }
                class = Some(owner);
                ready_rank = ready_rank.max(rank);
            }
        }
        Ok(class.map(|class| (class, ready_rank)))
    }

    fn ring_class_rank(&self, nodes: impl IntoIterator<Item = VertexId>) -> Option<(usize, usize)> {
        let mut class = None;
        let mut ready_rank = 0;
        for node in nodes {
            if let Some((owner, rank)) = self.node_location.get(node.0 as usize).copied().flatten()
            {
                if class.is_some_and(|known| known != owner) {
                    return None;
                }
                class = Some(owner);
                ready_rank = ready_rank.max(rank);
            }
        }
        class.map(|class| (class, ready_rank))
    }

    fn prepare_proximity<A: SearchAccount>(&self, account: &mut A) -> Result<()> {
        if self.proximity.borrow().is_some() {
            return Ok(());
        }
        let all_bounds: Vec<_> = self
            .embedding
            .chords
            .iter()
            .copied()
            .map(possible_swept_bounds)
            .collect();
        let mut left_indices = Vec::new();
        let mut left_bounds = Vec::new();
        for (index, chord) in self.embedding.chords.iter().enumerate() {
            if chord.nodes.iter().any(|&node| {
                self.node_location
                    .get(node as usize)
                    .copied()
                    .flatten()
                    .is_some()
            }) {
                left_indices.push(index);
                left_bounds.push(
                    *all_bounds
                        .get(index)
                        .expect("Voronoi chord bounds align with the chord table"),
                );
            }
        }
        account.charge_evaluations(
            u64::try_from(self.embedding.chords.len().div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let mut pairs = Vec::new();
        let mut radius_cache = self.radius_cache.borrow_mut();
        let mut movable_cache = self.movable_cache.borrow_mut();
        let mut callback_error = None;
        let flow =
            for_each_overlapping_bounds_cross_pair(&left_bounds, &all_bounds, |left, right| {
                if callback_error.is_some() {
                    return ControlFlow::Break(());
                }
                let left_index = *left_indices
                    .get(left)
                    .expect("overlap pair left index belongs to the frontier table");
                if left_index == right {
                    return ControlFlow::Continue(());
                }
                let left_chord = self.embedding.chord(left_index);
                let right_chord = self.embedding.chord(right);
                if !self.embedding.certifies_incidence_pair(left_index, right) {
                    return ControlFlow::Continue(());
                }
                let radii = (|| {
                    Ok((
                        exact_chord_radius(
                            left_chord,
                            &mut radius_cache,
                            left_index,
                            &mut movable_cache,
                            &mut |node| Ok(self.embedding.exact_movable(VertexId(node))),
                        )?,
                        exact_chord_radius(
                            right_chord,
                            &mut radius_cache,
                            right,
                            &mut movable_cache,
                            &mut |node| Ok(self.embedding.exact_movable(VertexId(node))),
                        )?,
                    ))
                })();
                let (left_radius, right_radius) = match radii {
                    Ok(radii) => radii,
                    Err(error) => {
                        callback_error = Some(error);
                        return ControlFlow::Break(());
                    },
                };
                if segments_within_chebyshev(
                    left_chord.nearest,
                    right_chord.nearest,
                    proximity_limit(left_radius, right_radius),
                ) {
                    pairs.push((left_index.min(right), left_index.max(right)));
                }
                ControlFlow::Continue(())
            })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        debug_assert!(flow.is_continue());
        pairs.sort_unstable();
        pairs.dedup();
        drop(radius_cache);
        drop(movable_cache);

        let mut sites = Vec::with_capacity(self.embedding.sites.len());
        for site in self.embedding.sites {
            let point = site.rounded_xy()?;
            sites.push(Bounds::new_raw(point.x, point.y, point.x, point.y));
        }
        let mut separator_bounds = Vec::new();
        let mut separator_indices = Vec::new();
        for &chord in &self.embedding.separator_chords {
            if self.embedding.chord(chord).nodes.iter().any(|&node| {
                self.node_location
                    .get(node as usize)
                    .copied()
                    .flatten()
                    .is_some()
            }) {
                separator_indices.push(chord);
                separator_bounds.push(
                    *all_bounds
                        .get(chord)
                        .expect("separator bounds align with the emitted chord table"),
                );
            }
        }
        account.charge_evaluations(
            u64::try_from(sites.len().div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let mut site_pairs = Vec::new();
        let site_pair_completion = for_each_overlapping_bounds_cross_pair(
            &separator_bounds,
            &sites,
            |separator, site| {
                site_pairs.push((
                    *separator_indices
                        .get(separator)
                        .expect("separator index belongs to the prepared table"),
                    site,
                ));
                ControlFlow::<()>::Continue(())
            },
        )?;
        debug_assert!(site_pair_completion.is_continue());
        site_pairs.sort_unstable();
        site_pairs.dedup();

        let mut classes = Vec::with_capacity(self.classes.len());
        for variables in &self.classes {
            classes.push(VoronoiClassState {
                pairs_by_rank: vec![Vec::new(); variables.len()],
                separators_by_rank: vec![Vec::new(); variables.len()],
                site_pairs_by_rank: vec![Vec::new(); variables.len()],
                fans_by_rank: vec![Vec::new(); variables.len()],
                rings_by_rank: vec![Vec::new(); variables.len()],
            });
        }
        for (pair_index, &(left, right)) in pairs.iter().enumerate() {
            let left = self.embedding.chord(left);
            let right = self.embedding.chord(right);
            if let Some((class, rank)) = self.predicate_class_rank([
                VertexId(left.nodes[0]),
                VertexId(left.nodes[1]),
                VertexId(right.nodes[0]),
                VertexId(right.nodes[1]),
            ])? {
                classes
                    .get_mut(class)
                    .expect("pair class belongs to the Voronoi problem")
                    .pairs_by_rank
                    .get_mut(rank)
                    .expect("pair rank belongs to its Voronoi class")
                    .push(pair_index);
            }
        }
        for (pair_index, &(chord, _site)) in site_pairs.iter().enumerate() {
            let edge = self.embedding.chord(chord);
            if let Some((class, rank)) =
                self.predicate_class_rank([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])])?
            {
                classes
                    .get_mut(class)
                    .expect("site-pair class belongs to the Voronoi problem")
                    .site_pairs_by_rank
                    .get_mut(rank)
                    .expect("site-pair rank belongs to its Voronoi class")
                    .push(pair_index);
            }
        }
        for &chord in &self.embedding.separator_chords {
            let edge = self.embedding.chord(chord);
            if let Some((class, rank)) =
                self.predicate_class_rank([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])])?
            {
                classes
                    .get_mut(class)
                    .expect("separator class belongs to the Voronoi problem")
                    .separators_by_rank
                    .get_mut(rank)
                    .expect("separator rank belongs to its Voronoi class")
                    .push(chord);
            }
        }
        for (fan_index, fan) in self.embedding.fans.iter().enumerate() {
            let mut nodes = Vec::with_capacity(fan.edges.len().saturating_mul(2).saturating_add(1));
            nodes.push(fan.origin);
            for edge_id in &fan.edges {
                let [start, end] = self.embedding.subdivision.edge(*edge_id).endpoints;
                nodes.extend([start, end]);
            }
            nodes.sort_unstable();
            nodes.dedup();
            if let Some((class, rank)) = self.predicate_class_rank(nodes.iter().copied())? {
                classes
                    .get_mut(class)
                    .expect("fan class belongs to the Voronoi problem")
                    .fans_by_rank
                    .get_mut(rank)
                    .expect("fan rank belongs to its Voronoi class")
                    .push(fan_index);
            }
        }
        for (ring_index, ring) in self.embedding.rings.iter().copied().enumerate() {
            let exact_ring = ring.exact(self.embedding.parts);
            let (_, open_vertices) = exact_ring
                .vertices
                .split_last()
                .expect("simple Voronoi rings are closed");
            if let Some((class, rank)) = self.ring_class_rank(open_vertices.iter().copied()) {
                classes
                    .get_mut(class)
                    .expect("ring class belongs to the Voronoi problem")
                    .rings_by_rank
                    .get_mut(rank)
                    .expect("ring rank belongs to its Voronoi class")
                    .push(ring_index);
            }
        }
        *self.proximity.borrow_mut() = Some(VoronoiProximity {
            pairs,
            site_pairs,
            classes,
        });
        Ok(())
    }
}

impl VoronoiProblem<'_, '_> {
    fn mark_node_class(&self, node: VertexId, blamed: &mut [bool]) {
        if let Some((class, _)) = self.node_location.get(node.0 as usize).copied().flatten() {
            *blamed
                .get_mut(class)
                .expect("class ID belongs to the Voronoi search") = true;
        }
    }

    fn check_pair(&self, left: usize, right: usize, class: usize, assigned: &[Option<XY>]) -> bool {
        let endpoints = |chord_index: usize| {
            let chord = self.embedding.chord(chord_index);
            let [start, end] = chord.nodes;
            [
                (
                    NodeId(start),
                    self.local_xy(VertexId(start), class, assigned),
                ),
                (NodeId(end), self.local_xy(VertexId(end), class, assigned)),
            ]
        };
        emitted_pair_preserves_incidence(endpoints(left), endpoints(right))
    }

    fn check_fan(&self, fan_index: usize, class: usize, assigned: &[Option<XY>]) -> Result<bool> {
        let fan = self
            .embedding
            .fans
            .get(fan_index)
            .expect("Voronoi fan index belongs to the prepared fan table");
        let xy_of = |node| Ok(self.local_xy(node, class, assigned));
        let mut rounded_order = Vec::new();
        match sort_fan(
            fan.origin,
            &fan.edges,
            FanSortMode::Rounded,
            self.embedding.subdivision,
            &xy_of,
            &mut rounded_order,
        )? {
            FanSortOutcome::Sorted => Ok(rounded_order == fan.exact_order),
            FanSortOutcome::AngularTie => Ok(false),
        }
    }

    fn check_ring_orientation(
        &self,
        ring_index: usize,
        class: usize,
        assigned: &[Option<XY>],
    ) -> Result<bool> {
        let ring = self
            .embedding
            .rings
            .get(ring_index)
            .copied()
            .expect("ring index belongs to the Voronoi ring table")
            .exact(self.embedding.parts);
        rounded_ring_orientation_matches(&ring.vertices, ring.orientation, |vertex| {
            Ok(self.local_xy(vertex, class, assigned))
        })
    }

    fn check_site_pair(&self, pair: usize, class: usize, assigned: &[Option<XY>]) -> Result<bool> {
        let proximity = self.proximity.borrow();
        let proximity = proximity
            .as_ref()
            .expect("site-pair rows are prepared before local search");
        let (chord, site) = *proximity
            .site_pairs
            .get(pair)
            .expect("site-pair ordinal belongs to the prepared table");
        let [start_node, end_node] = self.embedding.chord(chord).nodes;
        let [start, end] = [
            self.local_xy(VertexId(start_node), class, assigned),
            self.local_xy(VertexId(end_node), class, assigned),
        ];
        let start = exact::ExactPoint::from_xy(start);
        let end = exact::ExactPoint::from_xy(end);
        let site = exact::ExactPoint::from_xy(
            self.embedding
                .sites
                .get(site)
                .expect("site-pair site belongs to the canonical site table")
                .point
                .xy(),
        );
        Ok(!exact::incidence::segment_intersection(&start, &end, &site, &site)?.is_point())
    }

    fn check_separator(&self, chord: usize, class: usize, assigned: &[Option<XY>]) -> Result<bool> {
        self.embedding
            .separator_preserved(chord, |vertex| Ok(self.local_xy(vertex, class, assigned)))
    }
}

impl Problem for VoronoiProblem<'_, '_> {
    type Node = VertexId;
    type Candidate = XY;
    type Key = PointKey;
    type ClassState = VoronoiClassState;
    type Output = ();

    fn classes(&self) -> &[Vec<Self::Node>] {
        &self.classes
    }

    fn fixed_keys<W: Work>(
        &self,
        class: usize,
        _variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>> {
        work.charge_product_into_error(self.embedding.live_nodes.len(), 1)?;
        let mut keys = Vec::with_capacity(self.embedding.live_nodes.len());
        for &node in self.embedding.live_nodes {
            work.charge_into_error(1)?;
            if self
                .node_location
                .get(node.0 as usize)
                .copied()
                .flatten()
                .is_none_or(|(owner, _)| owner != class)
            {
                work.charge_into_error(1)?;
                keys.push(PointKey::new(
                    self.embedding.xy(node, self.embedding.coordinates)?,
                ));
            }
        }
        Ok(keys)
    }

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        _work: &mut W,
    ) -> Result<Ordering> {
        let left_seeded = self.search_seeds.binary_search(&left).is_ok();
        let right_seeded = self.search_seeds.binary_search(&right).is_ok();
        Ok(if left_seeded == right_seeded {
            self.embedding.compare_nodes(left, right)?
        } else {
            right_seeded.cmp(&left_seeded)
        })
    }

    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()> {
        let nearest = self.embedding.xy(node, self.embedding.coordinates)?;
        let x_values = [nearest.x.next_down(), nearest.x, nearest.x.next_up()];
        let y_values = [nearest.y.next_down(), nearest.y, nearest.y.next_up()];
        let candidate_count = x_values
            .len()
            .checked_mul(y_values.len())
            .expect("three candidate coordinates per axis fit in usize");
        work.charge_product_into_error(candidate_count, 1)?;
        candidates.reserve(candidate_count);
        for &x in &x_values {
            for &y in &y_values {
                let candidate = XY::new(x, y);
                if candidate.x.is_finite() && candidate.y.is_finite() {
                    candidates.push(candidate);
                }
            }
        }
        Ok(())
    }

    fn compare_candidates<W: Work>(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        _work: &mut W,
    ) -> Result<Ordering> {
        let exact = &self.embedding.subdivision.vertex(node).point;
        Ok(
            exact::incidence::squared_distance_cmp_point(exact, left, right)?
                .then_with(|| left.x.total_cmp(&right.x))
                .then_with(|| left.y.total_cmp(&right.y)),
        )
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        PointKey::new(candidate)
    }

    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        _variables: &[Self::Node],
        account: &mut A,
    ) -> Result<Self::ClassState> {
        self.prepare_proximity(account)?;
        let prepared = self.proximity.borrow();
        Ok(prepared
            .as_ref()
            .expect("Voronoi proximity state is prepared before class search")
            .classes
            .get(class)
            .expect("class ID belongs to the Voronoi search")
            .clone())
    }

    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        _variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        account: &mut A,
    ) -> Result<bool> {
        let prepared = self.proximity.borrow();
        let prepared = prepared
            .as_ref()
            .expect("Voronoi proximity state is prepared before local search");
        for &pair in class_state
            .pairs_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            account.charge_evaluations(1)?;
            let (left, right) = *prepared
                .pairs
                .get(pair)
                .expect("pair index belongs to the Voronoi proximity table");
            if !self.check_pair(left, right, class, assigned) {
                return Ok(false);
            }
        }
        for &chord in class_state
            .separators_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            account.charge_evaluations(1)?;
            if !self.check_separator(chord, class, assigned)? {
                return Ok(false);
            }
        }
        for &site_pair in class_state
            .site_pairs_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            account.charge_evaluations(1)?;
            if !self.check_site_pair(site_pair, class, assigned)? {
                return Ok(false);
            }
        }
        for &fan in class_state
            .fans_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            account.charge_evaluations(1)?;
            if !self.check_fan(fan, class, assigned)? {
                return Ok(false);
            }
        }
        for &ring in class_state
            .rings_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            let vertices = self
                .embedding
                .ring(ring)
                .exact(self.embedding.parts)
                .vertices
                .len();
            account.charge_evaluations(
                u64::try_from(vertices.div_ceil(16))
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            )?;
            if !self.check_ring_orientation(ring, class, assigned)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn check_global<A: SearchAccount>(
        &mut self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        account: &mut A,
    ) -> Result<GlobalCheck<Self::Output>> {
        let mut coordinates = self.embedding.coordinates.to_vec();
        for assignment in assignments {
            for (&node, &candidate) in assignment.variables.iter().zip(assignment.values) {
                self.embedding
                    .vertices
                    .set_point(node, &mut coordinates, candidate);
            }
        }
        let ring_vertices = self
            .embedding
            .rings
            .iter()
            .try_fold(0_usize, |count, ring| {
                count
                    .checked_add(ring.exact(self.embedding.parts).vertices.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)
            })?;
        let host_items = self
            .embedding
            .host_graphs
            .iter()
            .try_fold(0_usize, |count, graph| {
                count
                    .checked_add(graph.vertices.len())
                    .and_then(|count| count.checked_add(graph.edges.len()))
                    .ok_or(crate::error::ResourceError::SizeOverflow)
            })?;
        let units = ring_vertices
            .div_ceil(16)
            .checked_add(host_items.div_ceil(16))
            .and_then(|units| units.checked_add(self.embedding.live_nodes.len().div_ceil(16)))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        account.charge_evaluations(
            u64::try_from(units).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;

        let mut blamed = vec![false; self.classes.len()];
        let mut rejected = false;
        let mut coordinates_by_key = try_hash_map_with_capacity(self.embedding.live_nodes.len())?;
        for &node in self.embedding.live_nodes {
            let point = self.embedding.xy(node, &coordinates)?;
            if let Some(previous) = coordinates_by_key.insert(PointKey::new(point), node)
                && previous != node
            {
                rejected = true;
                self.mark_node_class(previous, &mut blamed);
                self.mark_node_class(node, &mut blamed);
            }
        }
        for ring in 0..self.embedding.rings.len() {
            if !self.embedding.ring_orientation(ring, &coordinates)? {
                rejected = true;
                let exact = self.embedding.ring(ring).exact(self.embedding.parts);
                let (_, open_vertices) = exact
                    .vertices
                    .split_last()
                    .expect("simple Voronoi rings are closed");
                for &node in open_vertices {
                    self.mark_node_class(node, &mut blamed);
                }
            }
        }
        for failure in self.embedding.clip_host_failures(&coordinates, account)? {
            rejected = true;
            let chord = self.embedding.chord(failure.chord);
            self.mark_node_class(VertexId(chord.nodes[0]), &mut blamed);
            self.mark_node_class(VertexId(chord.nodes[1]), &mut blamed);
            self.embedding
                .visit_clip_ring_vertices(failure.boundary.ring, |node| {
                    self.mark_node_class(node, &mut blamed);
                });
            if let Some(atom) = failure.boundary.atom {
                for node in self.embedding.subdivision.edge(atom).endpoints {
                    self.mark_node_class(node, &mut blamed);
                }
            }
        }
        let points = VoronoiPoints {
            vertices: self.embedding.vertices,
            coordinates: &coordinates,
        };
        for host_graph in &self.embedding.host_graphs {
            let owner_points = VoronoiOwnerPoints {
                points: &points,
                vertices: &host_graph.vertices,
            };
            let host_flow = visit_component_faces(
                &owner_points,
                host_graph.vertices.len(),
                host_graph.edges.len(),
                |edge| host_graph.edge(edge),
                |edge| host_graph.side_keys(edge),
                |root, hit| {
                    rejected = true;
                    let root = host_graph.vertex(root);
                    let rings = self
                        .embedding
                        .rings_by_node
                        .get(root.0 as usize)
                        .expect("host root belongs to the Voronoi vertex domain");
                    let mut found_owner_ring = false;
                    for ring in rings
                        .iter()
                        .copied()
                        .filter(|&ring| self.embedding.ring_owner(ring) == host_graph.owner)
                    {
                        found_owner_ring = true;
                        let exact = self.embedding.ring(ring).exact(self.embedding.parts);
                        let (_, open_vertices) = exact
                            .vertices
                            .split_last()
                            .expect("simple Voronoi rings are closed");
                        for &node in open_vertices {
                            self.mark_node_class(node, &mut blamed);
                        }
                    }
                    if !found_owner_ring {
                        self.mark_node_class(root, &mut blamed);
                    }
                    if let Some(hit) = hit {
                        let edge = self.embedding.chord(host_graph.chord(hit));
                        self.mark_node_class(VertexId(edge.nodes[0]), &mut blamed);
                        self.mark_node_class(VertexId(edge.nodes[1]), &mut blamed);
                    }
                    ControlFlow::Continue(())
                },
            )?;
            debug_assert!(host_flow.is_continue());
        }
        let classes = blamed
            .into_iter()
            .enumerate()
            .filter_map(|(class, blamed)| blamed.then_some(class))
            .collect::<Vec<_>>();
        Ok(if rejected {
            GlobalCheck::Reject(classes)
        } else {
            GlobalCheck::Pass(())
        })
    }

    fn initial_evaluations(&self) -> u64 {
        self.initial_evaluations
    }
}
