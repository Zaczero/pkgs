//! Assembling cells and edges from the labelled subdivision.
//!
//! Selected faces become rings, rings become cell parts, and the separator
//! incidence is walked once to emit each shared edge exactly one time.

#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;

use super::{
    BoundaryFaceId, EdgeId, ExactSign, ExactSubdivision, ExactVertex, NodedExactCycle,
    OwnerCycleContainment, OwnerShellPosition, Result, SiteId, VertexId, XY, exact,
    selected_cell_label, validate_assembled_exact_cycles, validate_owner_cycle_facts,
    voronoi_error, walk_label,
};
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::overlay::{RayAxis, RayPoints, shoot_left};
use crate::geometry::segments::ordering::try_sort_by;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct OwnerLocalWork {
    pub(super) global_edge_slot_visits: usize,
    pub(super) owner_ring_edges: usize,
    pub(super) owner_vertices: usize,
    pub(super) face_walks: usize,
    pub(super) components: usize,
    pub(super) host_queries: usize,
    pub(super) ray_hits: usize,
    pub(super) host_unions: usize,
    pub(super) min_vertex_comparisons: usize,
    pub(super) ray_hint_reads: usize,
    pub(super) ray_axis_comparisons: usize,
    pub(super) ray_orientations: usize,
    pub(super) ring_side_checks: usize,
}

#[cfg(test)]
impl OwnerLocalWork {
    const ZERO: Self = Self {
        global_edge_slot_visits: 0,
        owner_ring_edges: 0,
        owner_vertices: 0,
        face_walks: 0,
        components: 0,
        host_queries: 0,
        ray_hits: 0,
        host_unions: 0,
        min_vertex_comparisons: 0,
        ray_hint_reads: 0,
        ray_axis_comparisons: 0,
        ray_orientations: 0,
        ring_side_checks: 0,
    };
}

#[cfg(test)]
thread_local! {
    static OWNER_LOCAL_WORK: Cell<OwnerLocalWork> = const { Cell::new(OwnerLocalWork::ZERO) };
}

#[cfg(test)]
fn update_owner_local_work(update: impl FnOnce(&mut OwnerLocalWork)) {
    OWNER_LOCAL_WORK.with(|work| {
        let mut value = work.get();
        update(&mut value);
        work.set(value);
    });
}

#[cfg(test)]
pub(super) fn reset_owner_local_work() {
    OWNER_LOCAL_WORK.with(|work| work.set(OwnerLocalWork::ZERO));
}

#[cfg(test)]
pub(super) fn owner_local_work() -> OwnerLocalWork {
    OWNER_LOCAL_WORK.with(Cell::get)
}

pub(super) struct ExactSimpleRing {
    pub(super) face: BoundaryFaceId,
    pub(super) vertices: Vec<VertexId>,
    pub(super) edges: Vec<EdgeId>,
    pub(super) orientation: ExactSign,
}

pub(super) struct ExactCellPart {
    pub(super) owner: SiteId,
    pub(super) face: BoundaryFaceId,
    pub(super) shell: ExactSimpleRing,
    pub(super) holes: Vec<ExactSimpleRing>,
}

pub(super) struct ExactEdgeChain {
    pub(super) separator: [SiteId; 2],
    pub(super) vertices: Vec<VertexId>,
    pub(super) edges: Vec<EdgeId>,
}

pub(super) struct OwnerRayPoints<'a> {
    vertices: &'a [ExactVertex],
    global_vertices: &'a [VertexId],
    rounded: Option<Vec<XY>>,
}

impl<'a> OwnerRayPoints<'a> {
    #[expect(
        clippy::indexing_slicing,
        reason = "global vertex IDs were admitted by the ExactSubdivision whose vertex column is borrowed here"
    )]
    pub(super) fn new(vertices: &'a [ExactVertex], global_vertices: &'a [VertexId]) -> Self {
        // Nearest-even rounding is monotone, so these keys preserve exact
        // coordinate order whenever every owner vertex has a finite hint. If
        // any hint is unavailable, the constant key sends the whole ordering
        // run through the exact comparator instead.
        let rounded = global_vertices
            .iter()
            .map(|vertex| {
                vertices[vertex.0 as usize]
                    .point
                    .finite_rounding()
                    .ok()
                    .flatten()
            })
            .collect::<Option<Vec<XY>>>();
        Self {
            vertices,
            global_vertices,
            rounded,
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "owner-local vertex IDs are collected from the admitted subdivision"
    )]
    fn point(&self, local: u32) -> &exact::ExactPoint {
        &self.vertices[self.global_vertices[local as usize].0 as usize].point
    }
}

impl RayPoints for OwnerRayPoints<'_> {
    #[expect(
        clippy::indexing_slicing,
        reason = "the hint vector is aligned with the owner-local vertex IDs when present"
    )]
    fn rounded(&self, index: u32) -> XY {
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.ray_hint_reads = work.ray_hint_reads.wrapping_add(1);
        });
        self.rounded
            .as_ref()
            .map_or(XY::new(0.0, 0.0), |points| points[index as usize])
    }

    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering> {
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.ray_axis_comparisons = work.ray_axis_comparisons.wrapping_add(1);
        });
        match axis {
            RayAxis::X => self.point(left).compare_x(self.point(right)),
            RayAxis::Y => self.point(left).compare_y(self.point(right)),
        }
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.ray_orientations = work.ray_orientations.wrapping_add(1);
        });
        exact::orient_points(self.point(a), self.point(b), self.point(c))
    }
}

struct VertexPathPositions(Vec<Option<usize>>);

impl VertexPathPositions {
    fn new(vertex_count: usize) -> Self {
        Self(vec![None; vertex_count])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "VertexId values belong to the ExactSubdivision whose vertex count sized this pinch-position column"
    )]
    fn get(&self, vertex: VertexId) -> Option<usize> {
        self.0[vertex.0 as usize]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "VertexId values belong to the ExactSubdivision whose vertex count sized this pinch-position column"
    )]
    fn set(&mut self, vertex: VertexId, position: Option<usize>) {
        self.0[vertex.0 as usize] = position;
    }

    fn clear_after(&mut self, path: &[VertexId], position: usize) {
        for &vertex in path.iter().skip(position).skip(1) {
            self.set(vertex, None);
        }
    }
}

pub(super) fn canonicalize_ring(vertices: &mut [VertexId], edges: &mut [EdgeId]) {
    // Interned vertex IDs follow exact lexicographic point order, and a simple
    // open ring has unique vertices, so its least vertex identifies its least
    // cyclic rotation.
    let Some((closing, open)) = vertices.split_last_mut() else {
        unreachable!("simple exact rings are closed and nonempty")
    };
    let least = open
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| left.cmp(right))
        .map_or(0, |(index, _)| index);
    open.rotate_left(least);
    edges.rotate_left(least);
    *closing = *open
        .first()
        .expect("simple exact rings have at least three open vertices");
}

/// The simple ring through `vertices` (closed) and `edges`, canonically
/// rotated, with its exact orientation.
pub(super) fn simple_ring(
    subdivision: &ExactSubdivision,
    face: BoundaryFaceId,
    mut vertices: Vec<VertexId>,
    mut edges: Vec<EdgeId>,
) -> Result<ExactSimpleRing> {
    let (_, open_vertices) = vertices
        .split_last()
        .expect("boundary face walks are closed exact rings");
    let orientation = exact::cycle_orientation(
        open_vertices
            .iter()
            .map(|&vertex| &subdivision.vertex(vertex).point),
    )?;
    canonicalize_ring(&mut vertices, &mut edges);
    Ok(ExactSimpleRing {
        face,
        vertices,
        edges,
        orientation,
    })
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "face walks are nonempty admitted cycles, and repeated-vertex positions come from this path; the matching edge suffix is one item shorter and remains in bounds too"
)]
pub(super) fn split_boundary_face(
    subdivision: &ExactSubdivision,
    boundary_face: BoundaryFaceId,
    result: &mut Vec<ExactSimpleRing>,
) -> Result<()> {
    let walk = subdivision.face_slots(boundary_face);
    let owner = |slot: u32| subdivision.dart(slot).origin;
    let edge = |slot: u32| subdivision.dart(slot).edge;
    let mut distinct = Vec::with_capacity(walk.len());
    distinct.extend(walk.iter().map(|&slot| owner(slot)));
    distinct.sort_unstable();
    if distinct.array_windows::<2>().all(|[a, b]| a != b) {
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        vertices.reserve(walk.len() + 1);
        edges.reserve(walk.len());
        vertices.extend(walk.iter().map(|&slot| owner(slot)));
        vertices.push(
            *vertices
                .first()
                .expect("a boundary face contains at least one slot"),
        );
        edges.extend(walk.iter().map(|&slot| edge(slot)));
        result.push(simple_ring(subdivision, boundary_face, vertices, edges)?);
        return Ok(());
    }
    let mut path = Vec::new();
    let mut path_edges = Vec::new();
    path.reserve(walk.len());
    path_edges.reserve(walk.len());
    let mut positions = VertexPathPositions::new(subdivision.vertices.len());
    let first_slot = *walk
        .first()
        .expect("a boundary face contains at least one slot");
    let mut incoming = None;
    for &slot in walk.iter().chain(std::iter::once(&first_slot)) {
        let vertex = owner(slot);
        if let Some(position) = positions.get(vertex) {
            let mut vertices = Vec::new();
            let mut edges = Vec::new();
            vertices.reserve(path.len() - position + 1);
            edges.reserve(path_edges.len() - position + 1);
            vertices.extend_from_slice(&path[position..]);
            vertices.push(vertex);
            edges.extend_from_slice(&path_edges[position..]);
            edges.push(incoming.expect("a repeated boundary vertex has an incoming edge"));
            if vertices.len() < 4 {
                return Err(voronoi_error("pinch split produced a short exact ring"));
            }
            positions.clear_after(&path, position);
            path.truncate(position + 1);
            path_edges.truncate(position);
            result.push(simple_ring(subdivision, boundary_face, vertices, edges)?);
        } else {
            positions.set(vertex, Some(path.len()));
            path.push(vertex);
            if let Some(edge) = incoming {
                path_edges.push(edge);
            }
        }
        incoming = Some(edge(slot));
    }
    if path.len() != 1 || !path_edges.is_empty() {
        return Err(voronoi_error(
            "pinch split left a residual exact boundary path",
        ));
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "ExactSubdivision admits every dart edge ID and each atomic edge owns exactly two twin slots"
)]
pub(super) fn inverse_edge_slots(subdivision: &ExactSubdivision) -> Vec<[u32; 2]> {
    let mut edge_slots = vec![[u32::MAX; 2]; subdivision.edges.len()];
    for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
        let pair = &mut edge_slots[edge as usize];
        if pair[0] == u32::MAX {
            pair[0] = slot as u32;
        } else {
            debug_assert_eq!(pair[1], u32::MAX);
            pair[1] = slot as u32;
        }
    }
    #[cfg(test)]
    update_owner_local_work(|work| {
        work.global_edge_slot_visits = work
            .global_edge_slot_visits
            .wrapping_add(subdivision.edge_of_slot.len());
    });
    debug_assert!(
        edge_slots
            .iter()
            .all(|pair| pair[0] != u32::MAX && pair[1] != u32::MAX)
    );
    edge_slots
}

pub(super) struct OwnerLocalFaces {
    slots: Vec<u32>,
    topology: IndexedTopology,
    face_roots: Vec<usize>,
    infinity: usize,
}

impl OwnerLocalFaces {
    pub(super) fn build(
        subdivision: &ExactSubdivision,
        rings: &[&ExactSimpleRing],
        slots_by_edge: &[[u32; 2]],
    ) -> Result<Self> {
        Self::build_with_host_side::<true>(subdivision, rings, slots_by_edge)
    }

    #[cfg(test)]
    pub(super) fn reversed_host_cycle_facts(
        subdivision: &ExactSubdivision,
        rings: &[&ExactSimpleRing],
        slots_by_edge: &[[u32; 2]],
        cycle_rings: &[usize],
        shell: &ExactSimpleRing,
        holes: &[ExactSimpleRing],
    ) -> Result<Vec<OwnerCycleContainment>> {
        let faces = Self::build_with_host_side::<false>(subdivision, rings, slots_by_edge)?;
        let forest = OwnerRingForest::build(subdivision, rings, slots_by_edge, &faces)?;
        Ok(forest.into_nesting().cycle_facts(cycle_rings, shell, holes))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "global slots come from the admitted edge inverse; local vertices, edges, faces, and component indices are constructed together from those slots"
    )]
    fn build_with_host_side<const HOST_FACE_IS_EAST: bool>(
        subdivision: &ExactSubdivision,
        rings: &[&ExactSimpleRing],
        slots_by_edge: &[[u32; 2]],
    ) -> Result<Self> {
        let source = &subdivision.topology;
        let mut edges = Vec::new();
        for ring in rings {
            edges.extend_from_slice(&ring.edges);
        }
        edges.sort_unstable();
        edges.dedup();
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.owner_ring_edges = work.owner_ring_edges.wrapping_add(edges.len());
        });

        let mut slots = Vec::with_capacity(edges.len().saturating_mul(2));
        for edge in &edges {
            slots.extend_from_slice(&slots_by_edge[edge.0 as usize]);
        }
        slots.sort_unstable();
        let mut vertices = Vec::with_capacity(slots.len());
        for &slot in &slots {
            vertices.push(VertexId(source.owners[slot as usize]));
            vertices.push(VertexId(source.targets[slot as usize]));
        }
        vertices.sort_unstable();
        vertices.dedup();
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.owner_vertices = work.owner_vertices.wrapping_add(vertices.len());
        });

        let slot_count = IndexedTopology::admit_counts(vertices.len(), edges.len())?;
        debug_assert_eq!(slots.len(), slot_count);
        let mut topology = IndexedTopology::default();
        topology.starts.reserve(vertices.len().saturating_add(1));
        topology.starts.push(0);
        topology.targets.reserve(slot_count);
        let mut edge_of_slot = Vec::with_capacity(slot_count);
        let mut next_slot = 0;
        for &origin in &vertices {
            while let Some(&slot) = slots.get(next_slot)
                && source.owners[slot as usize] == origin.0
            {
                let target = VertexId(source.targets[slot as usize]);
                let local_target = vertices
                    .binary_search(&target)
                    .map_err(|_| voronoi_error("owner-local dart has no target vertex"))?;
                let global_edge = EdgeId(subdivision.edge_of_slot[slot as usize]);
                let local_edge = edges
                    .binary_search(&global_edge)
                    .map_err(|_| voronoi_error("owner-local dart has no selected edge"))?;
                topology.targets.push(local_target as u32);
                edge_of_slot.push(local_edge as u32);
                next_slot = next_slot.wrapping_add(1);
            }
            topology.starts.push(topology.targets.len() as u32);
        }
        if next_slot != slots.len() {
            return Err(voronoi_error(
                "owner-local slots do not match their origin rows",
            ));
        }

        let vertex_count = vertices.len();
        topology.owners.reserve(slot_count);
        topology.twins.reserve(slot_count);
        topology.component_of.reserve(vertex_count);
        topology.face_of.reserve(slot_count);
        topology.face_starts.reserve(slot_count.saturating_add(1));
        topology.face_slots.reserve(slot_count);
        let mut scratch = IndexedTopologyScratch::default();
        scratch.component_stack.reserve(vertex_count);
        scratch.successor.reserve(slot_count);
        scratch.first_slot.reserve(edges.len());
        topology.finish(&edge_of_slot, &mut scratch);
        drop(scratch);

        let mut edge_slots = vec![[u32::MAX; 2]; edges.len()];
        for (slot, &edge) in edge_of_slot.iter().enumerate() {
            let pair = &mut edge_slots[edge as usize];
            if pair[0] == u32::MAX {
                pair[0] = slot as u32;
            } else {
                debug_assert_eq!(pair[1], u32::MAX);
                pair[1] = slot as u32;
            }
        }
        debug_assert!(
            edge_slots
                .iter()
                .all(|pair| pair[0] != u32::MAX && pair[1] != u32::MAX)
        );
        drop(edge_of_slot);

        let component_count = topology
            .component_of
            .iter()
            .copied()
            .max()
            .map_or(0, |component| component as usize + 1);
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.face_walks = work
                .face_walks
                .wrapping_add(topology.face_starts.len().saturating_sub(1));
            work.components = work.components.wrapping_add(component_count);
        });
        let mut minima = vec![None; component_count];
        let points = OwnerRayPoints::new(&subdivision.vertices, &vertices);
        for (local_vertex, _) in vertices.iter().enumerate() {
            let component = topology.component_of[local_vertex] as usize;
            let minimum = &mut minima[component];
            if let Some(known) = *minimum {
                #[cfg(test)]
                update_owner_local_work(|work| {
                    work.min_vertex_comparisons = work.min_vertex_comparisons.wrapping_add(1);
                });
                if points
                    .point(local_vertex as u32)
                    .compare_lex(points.point(known as u32))?
                    .is_lt()
                {
                    *minimum = Some(local_vertex);
                }
            } else {
                *minimum = Some(local_vertex);
            }
        }
        let queries: Vec<_> = minima
            .iter()
            .map(|minimum| minimum.expect("every owner-local component has a vertex") as u32)
            .collect();
        drop(minima);
        let hits = shoot_left(
            &points,
            edges.len(),
            |edge| {
                let slot = edge_slots[edge][0] as usize;
                [topology.owners[slot], topology.targets[slot]]
            },
            &queries,
        )?;
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.host_queries = work.host_queries.wrapping_add(queries.len());
            work.ray_hits = work
                .ray_hits
                .wrapping_add(hits.iter().filter(|hit| hit.is_some()).count());
        });
        drop(queries);
        drop(edges);

        let face_count = topology.face_starts.len().wrapping_sub(1);
        let mut exterior = vec![None; component_count];
        for (face, bounds) in topology.face_starts.array_windows::<2>().enumerate() {
            let [start, end] = *bounds;
            let walk = &topology.face_slots[start as usize..end as usize];
            let sign = exact::cycle_orientation(walk.iter().map(|&slot| {
                &subdivision
                    .vertex(vertices[topology.owners[slot as usize] as usize])
                    .point
            }))?;
            if sign != ExactSign::Negative {
                continue;
            }
            let component =
                topology.component_of[topology.owners[walk[0] as usize] as usize] as usize;
            if exterior[component].replace(face).is_some() {
                return Err(voronoi_error(
                    "owner-local component has multiple exterior walks",
                ));
            }
        }
        if exterior.iter().any(Option::is_none) {
            return Err(voronoi_error("owner-local component has no exterior walk"));
        }

        let infinity = face_count;
        let mut parents: Vec<_> = (0..=face_count).collect();
        #[cfg(test)]
        let mut host_unions = 0_usize;
        for (component, hit) in hits.iter().copied().enumerate() {
            let host = if let Some(edge) = hit {
                let [first, second] = edge_slots[edge].map(|slot| slot as usize);
                let endpoints = [topology.owners[first], topology.targets[first]];
                let [low, high] = if points
                    .compare_axis(endpoints[0], endpoints[1], RayAxis::Y)?
                    .is_lt()
                {
                    endpoints
                } else {
                    [endpoints[1], endpoints[0]]
                };
                let upward = [first, second]
                    .into_iter()
                    .find(|&slot| topology.owners[slot] == low && topology.targets[slot] == high)
                    .ok_or_else(|| voronoi_error("owner-local hit has no upward dart"))?;
                let host_slot = if HOST_FACE_IS_EAST {
                    topology.twins[upward] as usize
                } else {
                    upward
                };
                topology.face_of[host_slot] as usize
            } else {
                infinity
            };
            let component_exterior =
                exterior[component].expect("owner-local exterior walks were checked");
            #[cfg(test)]
            {
                host_unions = host_unions.wrapping_add(usize::from(super::uf_union(
                    &mut parents,
                    component_exterior,
                    host,
                )));
            }
            #[cfg(not(test))]
            {
                let _ = super::uf_union(&mut parents, component_exterior, host);
            }
        }
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.host_unions = work.host_unions.wrapping_add(host_unions);
        });
        drop(hits);
        drop(points);
        drop(edge_slots);
        drop(exterior);
        drop(vertices);

        let mut face_roots = Vec::with_capacity(face_count);
        for face in 0..face_count {
            face_roots.push(super::subdivision::uf_root(&mut parents, face));
        }
        let infinity = super::subdivision::uf_root(&mut parents, infinity);
        drop(parents);

        Ok(Self {
            slots,
            topology,
            face_roots,
            infinity,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ring vertex/edge columns are admitted together by simple_ring and slots_by_edge indexes the same ExactSubdivision"
    )]
    pub(super) fn ring_side_faces(
        &self,
        subdivision: &ExactSubdivision,
        slots_by_edge: &[[u32; 2]],
        ring: &ExactSimpleRing,
    ) -> Result<(usize, usize)> {
        #[cfg(test)]
        update_owner_local_work(|work| {
            work.ring_side_checks = work.ring_side_checks.wrapping_add(1);
        });
        let mut left_face = None;
        let mut right_face = None;
        debug_assert_eq!(ring.vertices.len().checked_sub(1), Some(ring.edges.len()));
        for (edge, endpoints) in ring.edges.iter().zip(ring.vertices.array_windows::<2>()) {
            let [start, end] = *endpoints;
            let [first, second] = slots_by_edge[edge.0 as usize];
            let global_slot = [first, second]
                .into_iter()
                .find(|&slot| {
                    subdivision.topology.owners[slot as usize] == start.0
                        && subdivision.topology.targets[slot as usize] == end.0
                })
                .ok_or_else(|| voronoi_error("owner-local ring lost its directed slot"))?;
            let local_slot = self
                .slots
                .binary_search(&global_slot)
                .map_err(|_| voronoi_error("owner-local ring slot is absent"))?;
            let left = self.face_roots[self.topology.face_of[local_slot] as usize];
            let right = self.face_roots
                [self.topology.face_of[self.topology.twins[local_slot] as usize] as usize];
            match (left_face, right_face) {
                (None, None) => {
                    left_face = Some(left);
                    right_face = Some(right);
                },
                (Some(known_left), Some(known_right))
                    if known_left == left && known_right == right => {},
                _ => {
                    return Err(voronoi_error(
                        "owner-local ring has nonconstant left or right face",
                    ));
                },
            }
        }
        let left = left_face.ok_or_else(|| voronoi_error("owner-local ring has no darts"))?;
        let right =
            right_face.ok_or_else(|| voronoi_error("owner-local ring has no twin darts"))?;
        Ok((left, right))
    }
}

struct OwnerRingForest {
    parent: Vec<Option<usize>>,
    entry: Vec<usize>,
    exit: Vec<usize>,
    nearest_positive: Vec<Option<usize>>,
}

impl OwnerRingForest {
    fn into_nesting(self) -> OwnerRingNesting {
        let Self {
            parent,
            entry,
            exit,
            nearest_positive,
        } = self;
        drop(parent);
        drop(nearest_positive);
        OwnerRingNesting { entry, exit }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "rings, face roots, exteriors, parents and traversal intervals are parallel columns built from one owner-local topology"
    )]
    fn build(
        subdivision: &ExactSubdivision,
        rings: &[&ExactSimpleRing],
        slots_by_edge: &[[u32; 2]],
        faces: &OwnerLocalFaces,
    ) -> Result<Self> {
        let mut interiors = Vec::with_capacity(rings.len());
        let mut exteriors = Vec::with_capacity(rings.len());
        for ring in rings {
            let (left, right) = faces.ring_side_faces(subdivision, slots_by_edge, ring)?;
            let (interior, exterior) = if ring.orientation == ExactSign::Positive {
                (left, right)
            } else {
                (right, left)
            };
            if interior == exterior {
                return Err(voronoi_error(
                    "owner-local ring has identical interior and exterior faces",
                ));
            }
            interiors.push((interior, interiors.len()));
            exteriors.push(exterior);
        }
        interiors.sort_unstable();
        if interiors
            .array_windows::<2>()
            .any(|[left, right]| left.0 == right.0)
        {
            return Err(voronoi_error(
                "owner-local face is interior to more than one ring",
            ));
        }

        let mut parent = vec![None; rings.len()];
        for (child, &exterior) in exteriors.iter().enumerate() {
            if exterior == faces.infinity {
                continue;
            }
            let host = interiors
                .binary_search_by_key(&exterior, |&(face, _)| face)
                .map(|index| interiors[index].1)
                .map_err(|_| voronoi_error("owner-local ring exterior has no host face"))?;
            if host == child {
                return Err(voronoi_error(
                    "owner-local ring exterior resolves to itself",
                ));
            }
            parent[child] = Some(host);
        }
        drop(interiors);
        drop(exteriors);

        let mut first_child = vec![None; rings.len()];
        let mut next_sibling = vec![None; rings.len()];
        for (child, &host) in parent.iter().enumerate() {
            if let Some(host) = host {
                next_sibling[child] = first_child[host];
                first_child[host] = Some(child);
            }
        }
        let mut entry = vec![usize::MAX; rings.len()];
        let mut exit = vec![0; rings.len()];
        let mut nearest_positive = vec![None; rings.len()];
        let mut clock = 0;
        let mut stack = Vec::with_capacity(rings.len().saturating_mul(2));
        for root in 0..rings.len() {
            if parent[root].is_some() {
                continue;
            }
            stack.push((root, false, None));
            while let Some((ring, leaving, inherited_positive)) = stack.pop() {
                if leaving {
                    exit[ring] = clock;
                    continue;
                }
                if entry[ring] != usize::MAX {
                    return Err(voronoi_error(
                        "owner-local ring parent graph contains a cycle",
                    ));
                }
                entry[ring] = clock;
                clock = clock.wrapping_add(1);
                let nearest = if rings[ring].orientation == ExactSign::Positive {
                    Some(ring)
                } else {
                    inherited_positive
                };
                nearest_positive[ring] = nearest;
                stack.push((ring, true, nearest));
                let mut child = first_child[ring];
                while let Some(child_ring) = child {
                    stack.push((child_ring, false, nearest));
                    child = next_sibling[child_ring];
                }
            }
        }
        if clock != rings.len() {
            return Err(voronoi_error(
                "owner-local ring parent graph has no exterior root",
            ));
        }
        drop(first_child);
        drop(next_sibling);
        drop(stack);

        Ok(Self {
            parent,
            entry,
            exit,
            nearest_positive,
        })
    }
}

struct OwnerRingNesting {
    entry: Vec<usize>,
    exit: Vec<usize>,
}

impl OwnerRingNesting {
    #[expect(
        clippy::indexing_slicing,
        reason = "ring ordinals and DFS intervals are constructed together from one owner-local ring sequence"
    )]
    fn contains(&self, ancestor: usize, child: usize) -> bool {
        (self.entry[ancestor]..self.exit[ancestor]).contains(&self.entry[child])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "cycle ring IDs and shell/hole geometry columns are constructed in the same ordered part"
    )]
    fn cycle_facts(
        &self,
        cycle_rings: &[usize],
        shell_geometry: &ExactSimpleRing,
        holes: &[ExactSimpleRing],
    ) -> Vec<OwnerCycleContainment> {
        let shell = cycle_rings[0];
        let (_, shell_vertices) = shell_geometry
            .vertices
            .split_last()
            .expect("simple exact shells are closed");
        let mut shell_vertices = shell_vertices.to_vec();
        shell_vertices.sort_unstable();
        let mut facts = Vec::with_capacity(cycle_rings.len());
        facts.push(OwnerCycleContainment {
            shell_position: OwnerShellPosition::Inside,
            nested_descendant: false,
        });
        for (hole_index, &ring) in cycle_rings[1..].iter().enumerate() {
            let (_, vertices) = holes[hole_index]
                .vertices
                .split_last()
                .expect("simple exact holes are closed");
            let shell_member = self.contains(shell, ring);
            let has_strict_vertex = vertices
                .iter()
                .any(|vertex| shell_vertices.binary_search(vertex).is_err());
            let shell_position = if !shell_member {
                OwnerShellPosition::Outside
            } else if has_strict_vertex {
                OwnerShellPosition::Inside
            } else {
                OwnerShellPosition::BoundaryOnly
            };
            facts.push(OwnerCycleContainment {
                shell_position,
                nested_descendant: false,
            });
        }
        drop(shell_vertices);

        let mut holes_by_entry = Vec::with_capacity(cycle_rings.len().saturating_sub(1));
        holes_by_entry.extend_from_slice(&cycle_rings[1..]);
        holes_by_entry.sort_unstable_by_key(|&ring| self.entry[ring]);
        for (cycle, &ring) in cycle_rings.iter().enumerate().skip(1) {
            let next = holes_by_entry
                .partition_point(|&candidate| self.entry[candidate] <= self.entry[ring]);
            facts[cycle].nested_descendant = holes_by_entry
                .get(next)
                .is_some_and(|&candidate| self.entry[candidate] < self.exit[ring]);
        }
        facts
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "ring ordinals, shell slots, and hole positions are parallel tables populated from this same ring sequence and its BoxIndex"
)]
pub(super) fn assemble_owner_parts(
    subdivision: &ExactSubdivision,
    owner: SiteId,
    walks: impl IntoIterator<Item = BoundaryFaceId>,
    slots_by_edge: &mut Option<Vec<[u32; 2]>>,
    parts: &mut Vec<ExactCellPart>,
) -> Result<()> {
    let mut rings = Vec::new();
    for orbit in walks {
        split_boundary_face(subdivision, orbit, &mut rings)?;
    }
    if rings.is_empty() {
        return Ok(());
    }
    let mut ring_indices = Vec::new();
    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    for (index, ring) in rings.iter().enumerate() {
        match ring.orientation {
            ExactSign::Positive => {
                positives.push(ring_indices.len());
                ring_indices.push(index);
            },
            ExactSign::Negative => {
                negatives.push(ring_indices.len());
                ring_indices.push(index);
            },
            ExactSign::Zero => {},
        }
    }
    let mut holes_by_shell = Vec::with_capacity(positives.len());
    holes_by_shell.resize_with(positives.len(), Vec::new);
    if positives.is_empty() {
        if negatives.is_empty() {
            return Ok(());
        }
        return Err(voronoi_error(
            "negative exact face ring has no same-face shell",
        ));
    }

    let owner_local = if positives.len() == 1 && negatives.is_empty() {
        None
    } else {
        if slots_by_edge.is_none() {
            *slots_by_edge = Some(inverse_edge_slots(subdivision));
        }
        let edge_slots = slots_by_edge
            .as_deref()
            .expect("the shared edge-slot inverse was initialized");
        let owner_rings: Vec<_> = ring_indices.iter().map(|&index| &rings[index]).collect();
        let faces = OwnerLocalFaces::build(subdivision, &owner_rings, edge_slots)?;
        let mut forest = OwnerRingForest::build(subdivision, &owner_rings, edge_slots, &faces)?;
        for &positive in &positives {
            if forest.parent[positive].is_some() {
                return Err(voronoi_error(
                    "exact same-face ring forest has depth greater than one",
                ));
            }
        }
        drop(std::mem::take(&mut forest.parent));

        let mut shell_slot = vec![usize::MAX; ring_indices.len()];
        for (slot, &positive) in positives.iter().enumerate() {
            shell_slot[positive] = slot;
        }
        for &negative in &negatives {
            let Some(shell) = forest.nearest_positive[negative] else {
                return Err(voronoi_error(
                    "negative exact face ring has no same-face shell",
                ));
            };
            let slot = shell_slot[shell];
            if slot == usize::MAX {
                return Err(voronoi_error(
                    "negative exact face ring has no same-face shell",
                ));
            }
            holes_by_shell[slot].push(negative);
        }
        drop(faces);
        Some(forest.into_nesting())
    };
    drop(negatives);
    let mut taken = Vec::with_capacity(rings.len());
    taken.extend(
        rings
            .into_iter()
            .map(|ring| (ring.orientation != ExactSign::Zero).then_some(ring)),
    );
    let mut rings = taken;
    for (slot, positive) in positives.into_iter().enumerate() {
        let shell = rings[ring_indices[positive]].take().unwrap();
        let hole_indices = std::mem::take(&mut holes_by_shell[slot]);
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "the non-zero-sized hole vector and its additional shell entry fit in the process address space"
        )]
        let ring_count = hole_indices.len() + 1;
        let mut cycle_rings = Vec::with_capacity(ring_count);
        cycle_rings.push(positive);
        cycle_rings.extend_from_slice(&hole_indices);
        let mut holes = Vec::with_capacity(hole_indices.len());
        holes.extend(
            hole_indices
                .iter()
                .map(|&hole| rings[ring_indices[hole]].take().unwrap()),
        );
        drop(hole_indices);
        if walk_label(subdivision, shell.face).owner != Some(owner)
            || holes
                .iter()
                .any(|hole| walk_label(subdivision, hole.face).owner != Some(owner))
        {
            return Err(voronoi_error("exact ring escaped its owner group"));
        }
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "the shell and hole edge vectors are simultaneously live, non-zero-sized allocations whose combined lengths fit the process address space"
        )]
        let edge_count =
            shell.edges.len() + holes.iter().map(|hole| hole.edges.len()).sum::<usize>();
        let mut edge_ids: Vec<EdgeId> = Vec::with_capacity(edge_count);
        edge_ids.extend(
            shell
                .edges
                .iter()
                .chain(holes.iter().flat_map(|hole| hole.edges.iter())),
        );
        edge_ids.sort_unstable();
        if edge_ids.array_windows::<2>().any(|[a, b]| a == b) {
            return Err(voronoi_error(
                "exact face shell and holes share an atomic edge",
            ));
        }
        drop(edge_ids);
        let mut exact_rings = Vec::with_capacity(ring_count);
        for ring in std::iter::once(&shell).chain(holes.iter()) {
            let (_, open_vertices) = ring
                .vertices
                .split_last()
                .expect("simple exact rings are closed");
            let mut points = Vec::with_capacity(open_vertices.len());
            points.extend(
                open_vertices
                    .iter()
                    .map(|&vertex| subdivision.vertex(vertex).point.clone()),
            );
            exact_rings.push(points);
        }
        let mut noded_cycles = Vec::with_capacity(exact_rings.len());
        noded_cycles.extend(
            exact_rings
                .iter()
                .zip(std::iter::once(&shell).chain(holes.iter()))
                .map(|(points, ring)| NodedExactCycle { points, ring }),
        );
        let cycle_count = noded_cycles.len();
        validate_assembled_exact_cycles(subdivision, &noded_cycles, 0, || {
            owner_local.as_ref().map_or(Ok(()), |forest| {
                let facts = forest.cycle_facts(&cycle_rings, &shell, &holes);
                validate_owner_cycle_facts(&facts, cycle_count, 0)
            })
        })?;
        drop(noded_cycles);
        drop(exact_rings);
        drop(cycle_rings);
        parts.push(ExactCellPart {
            owner,
            face: shell.face,
            shell,
            holes,
        });
    }
    Ok(())
}

pub(super) fn assemble_cells(subdivision: &ExactSubdivision) -> Result<Vec<ExactCellPart>> {
    let mut selected = Vec::with_capacity(subdivision.labels.len());
    for (index, label) in subdivision.labels.iter().enumerate() {
        if !selected_cell_label(*label) {
            continue;
        }
        let owner = label
            .owner
            .ok_or_else(|| voronoi_error("selected exact walk has no owner"))?;
        selected.push((owner, BoundaryFaceId(index as u32)));
    }
    selected.sort_unstable();
    let mut parts = Vec::new();
    let mut slots_by_edge = None;
    for group in selected.chunk_by(|left, right| left.0 == right.0) {
        let owner = group
            .first()
            .map(|(owner, _)| *owner)
            .expect("chunk_by returns nonempty owner groups");
        assemble_owner_parts(
            subdivision,
            owner,
            group.iter().map(|(_, face)| *face),
            &mut slots_by_edge,
            &mut parts,
        )?;
    }
    drop(selected);
    drop(slots_by_edge);
    try_sort_by(&mut parts, |left, right| {
        Ok(left
            .owner
            .cmp(&right.owner)
            .then_with(|| left.shell.vertices.cmp(&right.shell.vertices)))
    })?;
    Ok(parts)
}

/// One separator group's atoms at each vertex, as `(vertex, slot, other end)`
/// sorted by vertex and then slot.
pub(super) type SeparatorIncidence = [(VertexId, usize, VertexId)];

pub(super) fn incident_at(incidence: &SeparatorIncidence, vertex: VertexId) -> &SeparatorIncidence {
    let start = incidence.partition_point(|&(known, ..)| known < vertex);
    let end = incidence.partition_point(|&(known, ..)| known <= vertex);
    #[expect(
        clippy::indexing_slicing,
        reason = "partition_point returns an ordered half-open range within this vertex-sorted incidence slice"
    )]
    &incidence[start..end]
}

#[expect(
    clippy::indexing_slicing,
    reason = "incidence slots were enumerated from this exact group and `used` has one flag per group edge"
)]
pub(super) fn walk_separator_from(
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    incidence: &SeparatorIncidence,
    used: &mut [bool],
    start: VertexId,
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut current = start;
    let mut vertices = Vec::new();
    let mut chain_edges = Vec::new();
    vertices.push(current);
    while let Some(&(_, slot, next)) = incident_at(incidence, current)
        .iter()
        .find(|&&(_, slot, _)| !used[slot])
    {
        used[slot] = true;
        chain_edges.push(group[slot].1);
        current = next;
        vertices.push(current);
    }
    if vertices.len() < 2 {
        return Err(voronoi_error("empty Voronoi edge chain"));
    }
    if vertices.first() == vertices.last() {
        return Err(voronoi_error("Voronoi separator forms a closed cycle"));
    }
    if vertices.iter().rev().lt(vertices.iter()) {
        vertices.reverse();
        chain_edges.reverse();
    }
    result.push(ExactEdgeChain {
        separator,
        vertices,
        edges: chain_edges,
    });
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "used has one flag per group edge, and both traversal passes obtain slots by enumerating that same group"
)]
pub(super) fn assemble_separator_group(
    subdivision: &ExactSubdivision,
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut incidence = Vec::with_capacity(group.len().saturating_mul(2));
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        let [left, right] = subdivision.edge(*edge_id).endpoints;
        incidence.extend([(left, slot, right), (right, slot, left)]);
    }
    incidence.sort_unstable();
    if incidence
        .array_windows::<3>()
        .any(|[first, _, third]| first.0 == third.0)
    {
        return Err(voronoi_error("Voronoi separator branches"));
    }
    let mut used = Vec::with_capacity(group.len());
    used.resize(group.len(), false);
    let end_of_path = |vertex: VertexId| incident_at(&incidence, vertex).len() == 1;
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let [left, right] = subdivision.edge(*edge_id).endpoints;
        let start = if end_of_path(left) {
            left
        } else if end_of_path(right) {
            right
        } else {
            continue;
        };
        walk_separator_from(separator, group, &incidence, &mut used, start, result)?;
    }
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let [start, _] = subdivision.edge(*edge_id).endpoints;
        walk_separator_from(separator, group, &incidence, &mut used, start, result)?;
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "forward_slot is a dense edge-keyed map sized from subdivision.edges and populated from that same admitted edge_of_slot column"
)]
pub(super) fn assemble_edges(subdivision: &ExactSubdivision) -> Result<Vec<ExactEdgeChain>> {
    let mut forward_slot = Vec::with_capacity(subdivision.edges.len());
    forward_slot.resize(subdivision.edges.len(), u32::MAX);
    for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
        let dart = subdivision.dart(slot as u32);
        let atomic = subdivision.edge(dart.edge);
        let [start, _] = atomic.endpoints;
        if dart.origin == start {
            forward_slot[edge as usize] = slot as u32;
        }
    }
    let mut selected = Vec::new();
    for (index, edge) in subdivision.edges.iter().enumerate() {
        let Some(separator) = edge.separator else {
            continue;
        };
        let slot = forward_slot[index] as usize;
        debug_assert_ne!(slot, u32::MAX as usize);
        let dart = subdivision.dart(slot as u32);
        let twin = subdivision.dart(dart.twin);
        let left = walk_label(subdivision, dart.face);
        let right = walk_label(subdivision, twin.face);
        if selected_cell_label(left) || selected_cell_label(right) {
            selected.push((separator, EdgeId(index as u32)));
        }
    }
    selected.sort_unstable();
    let mut result = Vec::new();
    for group in selected.chunk_by(|left, right| left.0 == right.0) {
        let separator = group
            .first()
            .expect("chunk_by returns non-empty separator groups")
            .0;
        assemble_separator_group(subdivision, separator, group, &mut result)?;
    }
    try_sort_by(&mut result, |left, right| {
        Ok(left
            .separator
            .cmp(&right.separator)
            .then_with(|| left.vertices.cmp(&right.vertices)))
    })?;
    Ok(result)
}
