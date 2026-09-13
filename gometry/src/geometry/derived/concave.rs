use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::geometry::derived::{geometric_xy_cmp, monotone_chain_hull};
use crate::geometry::tessellation::{EMPTY, Triangulation, next_halfedge, prev_halfedge};
use crate::geometry::{
    GeometryErrorKind, Point, Ring, Segment, delaunay_triangulation, open_xy_cycle_winding,
    point_distance, point_segment_distance,
};

pub(crate) fn native_concave_hull(
    points: &[Point],
    concavity: f64,
    length_threshold: f64,
) -> crate::error::Result<Vec<usize>> {
    // A point, line, or wholly collinear set has no Delaunay faces. These are
    // the intentional convex-hull reductions; once faces exist, boundary
    // extraction must produce the requested concave ring or return an error.
    if points.len() < 3 {
        return Ok(monotone_chain_hull(points));
    }
    let triangulation = delaunay_triangulation(points)?;
    if triangulation.triangles.is_empty() {
        return Ok(monotone_chain_hull(points));
    }

    let triangle_count = triangulation.triangles.len() / 3;
    let mut alive = Vec::new();
    crate::try_reserve_vec(&mut alive, triangle_count)?;
    alive.resize(triangle_count, true);
    let mut boundary_degree = Vec::new();
    crate::try_reserve_vec(&mut boundary_degree, points.len())?;
    boundary_degree.resize(points.len(), 0_i16);
    let mut heap = BinaryHeap::new();
    for halfedge in 0..triangulation.triangles.len() {
        if is_boundary_halfedge(&triangulation, &alive, halfedge) {
            bump_boundary_degree(&mut boundary_degree, &triangulation, halfedge, 1);
            crate::try_push_heap(
                &mut heap,
                ConcaveBoundaryEdge::new(points, &triangulation, halfedge),
            )?;
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
        push_live_twin_boundary(points, &triangulation, &alive, left, &mut heap)?;
        push_live_twin_boundary(points, &triangulation, &alive, right, &mut heap)?;
    }

    extract_concave_boundary(points, &triangulation, &alive)
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
) -> crate::error::Result<()> {
    let twin = triangulation.halfedges[peeled_halfedge];
    if twin != EMPTY && is_boundary_halfedge(triangulation, alive, twin) {
        crate::try_push_heap(heap, ConcaveBoundaryEdge::new(points, triangulation, twin))?;
    }
    Ok(())
}

fn extract_concave_boundary(
    points: &[Point],
    triangulation: &Triangulation,
    alive: &[bool],
) -> crate::error::Result<Vec<usize>> {
    // Boundary vertices have degree exactly two, so the adjacency is a single
    // flat `[u32; 2]` per vertex (`NONE` sentinel) — one allocation instead of
    // one `Vec` per point. `overflowed` flags any degree>2 vertex the two slots
    // cannot represent, preserving the malformed-boundary rejection below.
    const NONE: u32 = u32::MAX;
    let mut adjacency = Vec::new();
    crate::try_reserve_vec(&mut adjacency, points.len())?;
    adjacency.resize(points.len(), [NONE; 2]);
    let mut overflowed = false;
    for halfedge in 0..triangulation.triangles.len() {
        if is_boundary_halfedge(triangulation, alive, halfedge) {
            let start = triangulation.triangles[halfedge];
            let end = triangulation.triangles[next_halfedge(halfedge)];
            overflowed |= push_unique_neighbor(&mut adjacency[start], end as u32);
            overflowed |= push_unique_neighbor(&mut adjacency[end], start as u32);
        }
    }
    let Some(start) = adjacency.iter().position(|slot| slot[0] != NONE) else {
        return Err(GeometryErrorKind::triangulation(
            "concave hull boundary has no live edge",
        ));
    };
    // Valid boundary vertices are empty ([NONE,NONE]) or degree two ([x,y]);
    // a single filled slot is degree one. Either that or an overflow is malformed.
    if overflowed
        || adjacency
            .iter()
            .any(|slot| slot[0] != NONE && slot[1] == NONE)
    {
        return Err(GeometryErrorKind::triangulation(
            "concave hull boundary vertices must have degree two",
        ));
    }

    let boundary_vertex_count = adjacency.iter().filter(|slot| slot[0] != NONE).count();
    let mut ring = Vec::new();
    crate::try_reserve_vec(&mut ring, boundary_vertex_count)?;
    let mut previous = u32::MAX;
    let mut current = start as u32;
    loop {
        crate::try_push_vec(&mut ring, current as usize)?;
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
            return Err(GeometryErrorKind::triangulation(
                "concave hull boundary must be one ring",
            ));
        }
    }
    canonicalize_open_hull_indices(points, ring)
}

/// CCW open cycle rotated to its lexicographically least XY vertex.
fn canonicalize_open_hull_indices(
    points: &[Point],
    mut ring: Vec<usize>,
) -> crate::error::Result<Vec<usize>> {
    if ring.is_empty() {
        return Err(GeometryErrorKind::triangulation(
            "concave hull boundary is empty",
        ));
    }
    if ring.len() < Ring::MIN_VERTICES_OPEN {
        return Err(GeometryErrorKind::triangulation(
            "concave hull boundary is degenerate",
        ));
    }
    let mut cycle = Vec::new();
    crate::try_reserve_vec(&mut cycle, ring.len())?;
    for &index in &ring {
        crate::try_push_vec(&mut cycle, points[index].xy())?;
    }
    let winding = open_xy_cycle_winding(&cycle);
    if winding.is_degenerate() {
        return Err(GeometryErrorKind::triangulation(
            "concave hull boundary is degenerate",
        ));
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
    Ok(ring)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn points(xy: &[(f64, f64)]) -> Vec<Point> {
        xy.iter().map(|&(x, y)| Point::new_raw_xy(x, y)).collect()
    }

    fn assert_triangulation_error(error: &crate::error::Error, message: &str) {
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Triangulation(detail))
                if detail.as_ref() == message
        ));
    }

    #[test]
    fn low_cardinality_and_collinear_inputs_keep_explicit_hull_degeneracy() {
        assert_eq!(
            native_concave_hull(&points(&[(0.0, 0.0)]), 1.0, 0.0).unwrap(),
            vec![0]
        );
        assert_eq!(
            native_concave_hull(&points(&[(0.0, 0.0), (1.0, 0.0)]), 1.0, 0.0).unwrap(),
            vec![0, 1]
        );
        assert_eq!(
            native_concave_hull(&points(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]), 1.0, 0.0,).unwrap(),
            vec![0, 2]
        );
    }

    #[test]
    fn empty_extracted_boundary_is_an_error_not_a_convex_hull() {
        let points = points(&[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]);
        let triangulation = Triangulation {
            triangles: vec![0, 1, 2],
            halfedges: vec![0, 1, 2],
            hull: vec![0, 1, 2],
        };
        let error = extract_concave_boundary(&points, &triangulation, &[true]).unwrap_err();
        assert_triangulation_error(&error, "concave hull boundary has no live edge");
    }

    #[test]
    fn malformed_extracted_boundary_is_an_error_not_a_convex_hull() {
        let points = points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]);
        let triangulation = Triangulation {
            triangles: vec![0, 1, 2, 0, 2, 3],
            halfedges: vec![EMPTY; 6],
            hull: vec![0, 1, 2, 3],
        };
        let error = extract_concave_boundary(&points, &triangulation, &[true, true]).unwrap_err();
        assert_triangulation_error(
            &error,
            "concave hull boundary vertices must have degree two",
        );
    }

    #[test]
    fn degenerate_extracted_boundary_is_an_error_after_triangulation() {
        let points = points(&[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]);
        let error = canonicalize_open_hull_indices(&points, vec![0, 1, 2]).unwrap_err();
        assert_triangulation_error(&error, "concave hull boundary is degenerate");
    }

    #[test]
    fn boundary_staging_propagates_each_allocation_and_retries() {
        let points = points(&[(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]);
        let triangulation = Triangulation {
            triangles: vec![0, 1, 2],
            halfedges: vec![EMPTY; 3],
            hull: vec![0, 1, 2],
        };
        let alive = [true];
        let (boundary, allocations) = crate::test_support::allocation_count(|| {
            extract_concave_boundary(&points, &triangulation, &alive)
                .expect("valid boundary extraction")
        });
        assert_eq!(boundary, vec![0, 1, 2]);
        assert!(allocations > 0, "boundary staging must allocate");

        for allowed in 0..allocations {
            let result = crate::test_support::fail_allocations_after(allowed, || {
                extract_concave_boundary(&points, &triangulation, &alive)
            });
            let Err(error) = result else {
                panic!("boundary allocation prefix {allowed} must fail");
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
            ));
        }

        assert_eq!(
            extract_concave_boundary(&points, &triangulation, &alive).unwrap(),
            boundary
        );
    }
}
