use super::{
    Axis, ExactPartition, ExactSign, GeometryErrorKind, GlobalFaces, IndexedTopology,
    IndexedTopologyScratch, NodeId, Ordering, ResourceError, Result, XY, remove_antenna_vertices,
    same_point, shoot_left, stored_cross_sign, stored_orientation_sign,
};

pub(super) fn remove_node_antennas(mut nodes: Vec<NodeId>) -> Option<Vec<NodeId>> {
    if nodes.first() == nodes.last() {
        nodes.pop();
    }
    let mut nodes = remove_antenna_vertices(nodes, |left, right| left == right)?;
    let first = *nodes.first()?;
    nodes.push(first);
    Some(nodes)
}

impl ExactPartition<'_> {
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
    pub(super) fn compare_departures(&self, origin: NodeId, left: usize, right: usize) -> Ordering {
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
        let slots = IndexedTopology::admit_counts(vertices, self.spans.len())?;
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
            let row = &mut edge_of_slot[range];
            if let [left, right] = row {
                if self.compare_departures(NodeId(node as u32), *left as usize, *right as usize)
                    == Ordering::Greater
                {
                    std::mem::swap(left, right);
                }
            } else {
                row.sort_unstable_by(|&left, &right| {
                    self.compare_departures(NodeId(node as u32), left as usize, right as usize)
                });
            }
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
        scratch.first_slot.reserve(self.spans.len());
        topology.finish(&edge_of_slot, &mut scratch);
        Ok((topology, edge_of_slot))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "component IDs are densely assigned by IndexedTopology::finish and minima is appended in that same order"
    )]
    pub(super) fn component_minima(&self, topology: &IndexedTopology) -> Result<Vec<NodeId>> {
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
    pub(super) fn exterior_walk(
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
    pub(super) fn global_faces(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
    ) -> Result<GlobalFaces> {
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
    pub(super) fn cross_face_winding<T>(
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
    pub(super) fn visit_face_windings(
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
    pub(super) fn visit_face_windings_with_hook<T>(
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
        let seed_face = if let Some((face, seed)) = &self.winding_seed {
            if seed.len() != ring_count {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition winding seed has the wrong ring count".into(),
                )
                .into());
            }
            if *face == 0 || *face as usize >= count {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition winding seed is not an interior face".into(),
                )
                .into());
            }
            winding.copy_from_slice(seed);
            *face
        } else {
            0
        };
        let mut visited = Vec::with_capacity(count);
        visited.resize(count, false);
        let mut stack = Vec::with_capacity(count);
        visited[0] = true;
        if seed_face == 0 {
            visit(0, &winding, state)?;
        } else {
            // The omitted source arcs can change winding across the unbounded
            // local face. Seed inside the clip instead and keep that face out
            // of the propagation path.
            visited[seed_face as usize] = true;
            visit(seed_face, &winding, state)?;
        }
        stack.push((seed_face, starts[seed_face as usize], None));
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
        #[cfg(debug_assertions)]
        {
            // The window seed deliberately avoids face zero, whose winding is
            // not constant after source arcs outside the rectangle are omitted.
            // This can leave zero-area walks outside the seed's dual component;
            // every open face must still have a path within the window.
            let walk_has_area = |start: u32, end: u32| {
                let mut points = (start as usize..end as usize)
                    .map(|slot| self.nearest_xy(NodeId(topology.owners[slot])));
                let Some(first) = points.next() else {
                    return false;
                };
                let Some(second) = points.find(|&point| !same_point(point, first)) else {
                    return false;
                };
                points.any(|point| {
                    stored_orientation_sign(first.x, first.y, second.x, second.y, point.x, point.y)
                        != ExactSign::Zero
                })
            };
            let unvisited_open_faces = faces
                .walk_faces
                .iter()
                .enumerate()
                .filter_map(|(walk, &face)| (!visited[face as usize]).then_some((walk, face)))
                .filter_map(|(walk, face)| {
                    let (Some(&start), Some(&end)) = (
                        topology.face_starts.get(walk),
                        topology.face_starts.get(walk.wrapping_add(1)),
                    ) else {
                        unreachable!("global walk ordinal comes from this topology");
                    };
                    walk_has_area(start, end).then_some(face)
                })
                .collect::<Vec<_>>();
            debug_assert!(
                unvisited_open_faces.is_empty(),
                "seeded face-winding left open faces {unvisited_open_faces:?} unvisited"
            );
        }
        if let Some((_, seed)) = &self.winding_seed {
            debug_assert_eq!(&winding, seed);
        } else {
            debug_assert!(winding.iter().all(|&value| value == 0));
        }
        Ok(())
    }

    /// The walk on the east side of `span`: the departure from its upper
    /// endpoint keeps that side on its left.
    #[expect(
        clippy::indexing_slicing,
        reason = "the contact span comes from the same partition and its departure row comes from local_topology"
    )]
    pub(super) fn contact_walk(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        span: usize,
    ) -> u32 {
        let atom = self.spans[span];
        let source = self.unique[atom.unique as usize];
        let origin = if source.end.y > source.start.y {
            atom.end
        } else {
            atom.start
        };
        let slot = (topology.starts[origin.0 as usize] as usize
            ..topology.starts[origin.0 as usize + 1] as usize)
            .find(|&slot| edges[slot] as usize == span)
            .expect("contact atom has a departure slot");
        topology.face_of[slot]
    }
}
