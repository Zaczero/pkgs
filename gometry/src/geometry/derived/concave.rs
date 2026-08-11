use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::geometry::derived::monotone_chain_hull;
use crate::geometry::{
    HashMap, Point, PointKey, Ring, Segment, canonical_ring, delaunay_triangulation_spade,
    point_distance, point_segment_distance, ring_winding, same_point,
};

pub(crate) fn native_concave_hull(
    points: &[Point],
    concavity: f64,
    length_threshold: f64,
) -> Vec<usize> {
    if points.len() < 3 {
        return hull_indices_from_points(points);
    }
    // `spade` over `delaunator` here: delaunator's `legalize` cascades to
    // ~O(n²·⁹) on near-cocircular inputs (dense points on a smooth curve),
    // while spade stays far closer to linear — see `delaunay_triangulation_spade`.
    let triangulation = delaunay_triangulation_spade(points);
    if triangulation.triangles.is_empty() {
        return hull_indices_from_points(points);
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
        ) {
            continue;
        }

        alive[triangle] = false;
        bump_boundary_degree(&mut boundary_degree, &triangulation, halfedge, -1);
        let left = delaunator::prev_halfedge(halfedge);
        let right = delaunator::next_halfedge(halfedge);
        bump_boundary_degree(&mut boundary_degree, &triangulation, left, 1);
        bump_boundary_degree(&mut boundary_degree, &triangulation, right, 1);
        push_live_twin_boundary(points, &triangulation, &alive, left, &mut heap);
        push_live_twin_boundary(points, &triangulation, &alive, right, &mut heap);
    }

    extract_concave_boundary(points, &triangulation, &alive)
        .unwrap_or_else(|| hull_indices_from_points(points))
}

fn hull_indices_from_points(points: &[Point]) -> Vec<usize> {
    let hull = monotone_chain_hull(points);
    let indices_by_xy: HashMap<PointKey, usize> = points
        .iter()
        .enumerate()
        .map(|(index, &point)| (PointKey::new(point), index))
        .collect();
    hull.iter()
        .map(|point| {
            *indices_by_xy
                .get(&PointKey::new(*point))
                .expect("monotone-chain hull vertices come from input points")
        })
        .collect()
}

#[derive(Debug, Copy, Clone)]
struct ConcaveBoundaryEdge {
    length: f64,
    edge_key: (usize, usize),
    halfedge: usize,
}

impl ConcaveBoundaryEdge {
    fn new(points: &[Point], triangulation: &delaunator::Triangulation, halfedge: usize) -> Self {
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
    triangulation: &delaunator::Triangulation,
    boundary_degree: &[i16],
    halfedge: usize,
    length: f64,
    concavity: f64,
) -> bool {
    let opposite = triangulation.triangles[delaunator::prev_halfedge(halfedge)];
    if boundary_degree[opposite] != 0 {
        return false;
    }
    let max_length = if concavity == 0.0 {
        f64::INFINITY
    } else {
        length / concavity
    };
    let a = points[triangulation.triangles[halfedge]];
    let b = points[triangulation.triangles[delaunator::next_halfedge(halfedge)]];
    let c = points[opposite];
    point_segment_distance(c, Segment {
        start: a.into(),
        end: b.into(),
    }) <= max_length
        && edge_point_distance(a, c).min(edge_point_distance(c, b)) < max_length
}

fn edge_length(
    points: &[Point],
    triangulation: &delaunator::Triangulation,
    halfedge: usize,
) -> f64 {
    let (start, end) = edge_indices(triangulation, halfedge);
    edge_point_distance(points[start], points[end])
}

fn edge_point_distance(a: Point, b: Point) -> f64 {
    point_distance(a, b)
}

pub(crate) fn canonicalize_concave_hull_points(points: &mut [Point]) {
    // Keep the point-index bijection deterministic for the half-edge peeling
    // logic. Spade's bulk loader does its own spatial sort, so feeding it a
    // caller-side shuffle only adds work.
    points.sort_unstable_by(point_xy_cmp);
}

fn edge_indices(triangulation: &delaunator::Triangulation, halfedge: usize) -> (usize, usize) {
    (
        triangulation.triangles[halfedge],
        triangulation.triangles[delaunator::next_halfedge(halfedge)],
    )
}

pub(crate) fn point_xy_cmp(left: &Point, right: &Point) -> Ordering {
    left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
}

fn is_boundary_halfedge(
    triangulation: &delaunator::Triangulation,
    alive: &[bool],
    halfedge: usize,
) -> bool {
    if !alive[halfedge / 3] {
        return false;
    }
    let twin = triangulation.halfedges[halfedge];
    twin == delaunator::EMPTY || !alive[twin / 3]
}

fn triangle_boundary_count(
    triangulation: &delaunator::Triangulation,
    alive: &[bool],
    triangle: usize,
) -> usize {
    (triangle * 3..triangle * 3 + 3)
        .filter(|&halfedge| is_boundary_halfedge(triangulation, alive, halfedge))
        .count()
}

fn bump_boundary_degree(
    degree: &mut [i16],
    triangulation: &delaunator::Triangulation,
    halfedge: usize,
    delta: i8,
) {
    let start = triangulation.triangles[halfedge];
    let end = triangulation.triangles[delaunator::next_halfedge(halfedge)];
    degree[start] += i16::from(delta);
    degree[end] += i16::from(delta);
    debug_assert!(degree[start] >= 0 && degree[end] >= 0);
}

fn push_live_twin_boundary(
    points: &[Point],
    triangulation: &delaunator::Triangulation,
    alive: &[bool],
    peeled_halfedge: usize,
    heap: &mut BinaryHeap<ConcaveBoundaryEdge>,
) {
    let twin = triangulation.halfedges[peeled_halfedge];
    if twin != delaunator::EMPTY && is_boundary_halfedge(triangulation, alive, twin) {
        heap.push(ConcaveBoundaryEdge::new(points, triangulation, twin));
    }
}

fn extract_concave_boundary(
    points: &[Point],
    triangulation: &delaunator::Triangulation,
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
            let end = triangulation.triangles[delaunator::next_halfedge(halfedge)];
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
    let mut closed: Vec<Point> = ring.iter().map(|&index| points[index]).collect();
    closed.push(points[start]);
    if closed.len() < Ring::MIN_VERTICES_CLOSED || ring_winding(&closed).is_degenerate() {
        return None;
    }
    let mut canonical = canonical_ring(&closed, false);
    if canonical
        .last()
        .is_some_and(|&last| same_point(last, canonical[0]))
    {
        canonical.pop();
    }
    let indices_by_xy: HashMap<PointKey, usize> = ring
        .into_iter()
        .map(|index| (PointKey::new(points[index]), index))
        .collect();
    Some(
        canonical
            .into_iter()
            .map(|point| {
                *indices_by_xy
                    .get(&PointKey::new(point))
                    .expect("canonical concave ring vertices come from boundary")
            })
            .collect(),
    )
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
