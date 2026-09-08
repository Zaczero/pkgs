use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::geometry::derived::{geometric_xy_cmp, monotone_chain_hull};
use crate::geometry::tessellation::{EMPTY, Triangulation, next_halfedge, prev_halfedge};
use crate::geometry::{
    Point, Ring, Segment, delaunay_triangulation, point_distance, point_segment_distance,
    ring_winding,
};

pub(crate) fn native_concave_hull(
    points: &[Point],
    concavity: f64,
    length_threshold: f64,
) -> crate::error::Result<Vec<usize>> {
    if points.len() < 3 {
        return Ok(monotone_chain_hull(points));
    }
    let triangulation = delaunay_triangulation(points);
    if triangulation.triangles.is_empty() {
        return Ok(monotone_chain_hull(points));
    }

    let triangle_count = triangulation.triangles.len() / 3;
    let mut alive = vec![true; triangle_count];
    let mut boundary_degree = vec![0_i16; points.len()];
    let mut heap = BinaryHeap::new();
    for halfedge in 0..triangulation.triangles.len() {
        if is_boundary_halfedge(&triangulation, &alive, halfedge) {
            bump_boundary_degree(&mut boundary_degree, &triangulation, halfedge, 1);
            heap.push(ConcaveBoundaryEdge::new(points, &triangulation, halfedge));
        }
    }

    while let Some(edge) = heap.pop() {
        let halfedge = edge.halfedge;
        let triangle = halfedge / 3;
        if !alive[triangle]
            || !is_boundary_halfedge(&triangulation, &alive, halfedge)
            || triangle_boundary_count(&triangulation, &alive, triangle) != 1
        {
            continue;
        }
        let length = edge_length(points, &triangulation, halfedge);
        if length <= length_threshold {
            break;
        }
        if !can_peel_boundary_triangle(
            points,
            &triangulation,
            &boundary_degree,
            halfedge,
            length,
            concavity,
        )? {
            continue;
        }

        alive[triangle] = false;
        bump_boundary_degree(&mut boundary_degree, &triangulation, halfedge, -1);
        let left = prev_halfedge(halfedge);
        let right = next_halfedge(halfedge);
        bump_boundary_degree(&mut boundary_degree, &triangulation, left, 1);
        bump_boundary_degree(&mut boundary_degree, &triangulation, right, 1);
        push_live_twin_boundary(points, &triangulation, &alive, left, &mut heap);
        push_live_twin_boundary(points, &triangulation, &alive, right, &mut heap);
    }

    Ok(extract_concave_boundary(points, &triangulation, &alive)
        .unwrap_or_else(|| monotone_chain_hull(points)))
}

#[derive(Debug, Copy, Clone)]
struct ConcaveBoundaryEdge {
    length: f64,
    edge_key: (usize, usize),
    halfedge: usize,
}

impl ConcaveBoundaryEdge {
    fn new(points: &[Point], triangulation: &Triangulation, halfedge: usize) -> Self {
        let (start, end) = edge_indices(triangulation, halfedge);
        let edge_key = if point_xy_cmp(&points[start], &points[end]).is_gt() {
            (end, start)
        } else {
            (start, end)
        };
        Self {
            length: edge_point_distance(points[start], points[end]),
            edge_key,
            halfedge,
        }
    }
}

impl Eq for ConcaveBoundaryEdge {}

impl PartialEq for ConcaveBoundaryEdge {
    fn eq(&self, other: &Self) -> bool {
        self.length.total_cmp(&other.length) == Ordering::Equal && self.halfedge == other.halfedge
    }
}

impl Ord for ConcaveBoundaryEdge {
    fn cmp(&self, other: &Self) -> Ordering {
        self.length
            .total_cmp(&other.length)
            .then_with(|| other.edge_key.cmp(&self.edge_key))
            .then_with(|| other.halfedge.cmp(&self.halfedge))
    }
}

impl PartialOrd for ConcaveBoundaryEdge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn can_peel_boundary_triangle(
    points: &[Point],
    triangulation: &Triangulation,
    boundary_degree: &[i16],
    halfedge: usize,
    length: f64,
    concavity: f64,
) -> crate::error::Result<bool> {
    let opposite = triangulation.triangles[prev_halfedge(halfedge)];
    if boundary_degree[opposite] != 0 {
        return Ok(false);
    }
    let max_length = if concavity == 0.0 {
        f64::INFINITY
    } else {
        length / concavity
    };
    let a = points[triangulation.triangles[halfedge]];
    let b = points[triangulation.triangles[next_halfedge(halfedge)]];
    let c = points[opposite];
    Ok(point_segment_distance(c, Segment {
        start: a.into(),
        end: b.into(),
    })? <= max_length
        && edge_point_distance(a, c).min(edge_point_distance(c, b)) < max_length)
}

fn edge_length(points: &[Point], triangulation: &Triangulation, halfedge: usize) -> f64 {
    let (start, end) = edge_indices(triangulation, halfedge);
    edge_point_distance(points[start], points[end])
}

fn edge_point_distance(a: Point, b: Point) -> f64 {
    point_distance(a, b)
}

pub(crate) fn canonicalize_concave_hull_points(points: &mut [Point]) {
    // Keep the point-index bijection deterministic for the half-edge peeling
    // logic; the triangulation orders its insertions itself, so this sort
    // buys determinism, not speed.
    points.sort_unstable_by(point_xy_cmp);
}

fn edge_indices(triangulation: &Triangulation, halfedge: usize) -> (usize, usize) {
    (
        triangulation.triangles[halfedge],
        triangulation.triangles[next_halfedge(halfedge)],
    )
}

pub(crate) fn point_xy_cmp(left: &Point, right: &Point) -> Ordering {
    geometric_xy_cmp(*left, *right)
}

fn is_boundary_halfedge(triangulation: &Triangulation, alive: &[bool], halfedge: usize) -> bool {
    if !alive[halfedge / 3] {
        return false;
    }
    let twin = triangulation.halfedges[halfedge];
    twin == EMPTY || !alive[twin / 3]
}

fn triangle_boundary_count(
    triangulation: &Triangulation,
    alive: &[bool],
    triangle: usize,
) -> usize {
    (triangle * 3..triangle * 3 + 3)
        .filter(|&halfedge| is_boundary_halfedge(triangulation, alive, halfedge))
        .count()
}

fn bump_boundary_degree(
    degree: &mut [i16],
    triangulation: &Triangulation,
    halfedge: usize,
    delta: i8,
) {
    let start = triangulation.triangles[halfedge];
    let end = triangulation.triangles[next_halfedge(halfedge)];
    degree[start] += i16::from(delta);
    degree[end] += i16::from(delta);
    debug_assert!(degree[start] >= 0 && degree[end] >= 0);
}

fn push_live_twin_boundary(
    points: &[Point],
    triangulation: &Triangulation,
    alive: &[bool],
    peeled_halfedge: usize,
    heap: &mut BinaryHeap<ConcaveBoundaryEdge>,
) {
    let twin = triangulation.halfedges[peeled_halfedge];
    if twin != EMPTY && is_boundary_halfedge(triangulation, alive, twin) {
        heap.push(ConcaveBoundaryEdge::new(points, triangulation, twin));
    }
}

fn extract_concave_boundary(
    points: &[Point],
    triangulation: &Triangulation,
    alive: &[bool],
) -> Option<Vec<usize>> {
    // Boundary vertices have degree exactly two, so the adjacency is a single
    // flat `[u32; 2]` per vertex (`NONE` sentinel) — one allocation instead of
    // one `Vec` per point. `overflowed` flags any degree>2 vertex the two slots
    // cannot represent, preserving the malformed-boundary rejection below.
    const NONE: u32 = u32::MAX;
    let mut adjacency = vec![[NONE; 2]; points.len()];
    let mut overflowed = false;
    for halfedge in 0..triangulation.triangles.len() {
        if is_boundary_halfedge(triangulation, alive, halfedge) {
            let start = triangulation.triangles[halfedge];
            let end = triangulation.triangles[next_halfedge(halfedge)];
            overflowed |= push_unique_neighbor(&mut adjacency[start], end as u32);
            overflowed |= push_unique_neighbor(&mut adjacency[end], start as u32);
        }
    }
    let start = adjacency.iter().position(|slot| slot[0] != NONE)?;
    // Valid boundary vertices are empty ([NONE,NONE]) or degree two ([x,y]);
    // a single filled slot is degree one. Either that or an overflow is malformed.
    if overflowed
        || adjacency
            .iter()
            .any(|slot| slot[0] != NONE && slot[1] == NONE)
    {
        debug_assert!(false, "concave hull boundary vertices must have degree two");
        return None;
    }

    let mut ring = Vec::with_capacity(adjacency.iter().filter(|slot| slot[0] != NONE).count());
    let mut previous = u32::MAX;
    let mut current = start as u32;
    loop {
        ring.push(current as usize);
        let neighbors = adjacency[current as usize];
        let next = if neighbors[0] == previous {
            neighbors[1]
        } else {
            neighbors[0]
        };
        previous = current;
        current = next;
        if current as usize == start {
            break;
        }
        if ring.len() > points.len() {
            debug_assert!(false, "concave hull boundary must be one ring");
            return None;
        }
    }
    canonicalize_open_hull_indices(points, ring)
}

/// CCW open cycle rotated to its lexicographically least XY vertex.
fn canonicalize_open_hull_indices(points: &[Point], mut ring: Vec<usize>) -> Option<Vec<usize>> {
    if ring.is_empty() {
        return None;
    }
    let mut closed: Vec<Point> = ring.iter().map(|&index| points[index]).collect();
    closed.push(points[ring[0]]);
    let winding = ring_winding(&closed);
    if closed.len() < Ring::MIN_VERTICES_CLOSED || winding.is_degenerate() {
        return None;
    }
    if winding.reverse_for_clockwise(false) {
        ring.reverse();
    }
    let start = (1..ring.len()).fold(0, |best, candidate| {
        if point_xy_cmp(&points[ring[candidate]], &points[ring[best]]).is_lt() {
            candidate
        } else {
            best
        }
    });
    ring.rotate_left(start);
    Some(ring)
}

/// Record `candidate` in a degree-two adjacency slot. Returns `true` if the
/// vertex already holds two distinct neighbors (degree>2 overflow).
const fn push_unique_neighbor(neighbors: &mut [u32; 2], candidate: u32) -> bool {
    const NONE: u32 = u32::MAX;
    if neighbors[0] == candidate || neighbors[1] == candidate {
        return false;
    }
    if neighbors[0] == NONE {
        neighbors[0] = candidate;
    } else if neighbors[1] == NONE {
        neighbors[1] = candidate;
    } else {
        return true;
    }
    false
}
