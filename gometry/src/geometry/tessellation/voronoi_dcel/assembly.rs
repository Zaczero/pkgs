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
    selected_cell_label, subdivision, try_sort_by, uf_union, validate_assembled_exact_cycles,
    validate_owner_cycle_facts, voronoi_error, walk_label,
};
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::overlay::{RayAxis, RayPoints, shoot_left};

mod cells;
mod edges;

#[cfg(test)]
pub(super) use cells::OwnerLocalFaces;
pub(super) use cells::assemble_cells;
pub(super) use edges::assemble_edges;
#[cfg(test)]
pub(super) use edges::assemble_separator_group;

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
