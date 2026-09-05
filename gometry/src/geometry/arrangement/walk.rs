#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::cmp::Ordering;

use crate::geometry::arrangement::WindingWeight;
use crate::geometry::{
    HashMap, Orientation, PointKey, Segment, XY,
    open_xy_cycle_sign, orientation, same_point,
};

pub(crate) fn split_ring_at_pinches(
    ring: &[XY],
    ids: &[u32],
    rings: &mut Vec<Vec<XY>>,
    position_of: &mut [(u32, u32)],
    generation: u32,
    path: &mut Vec<XY>,
    path_ids: &mut Vec<u32>,
) {
    path.clear();
    path_ids.clear();
    for (&point, &id) in std::iter::zip(ring, ids) {
        let (stamp, position) = position_of[id as usize];
        if stamp == generation {
            let start = position as usize;
            let mut piece = path.split_off(start);
            let piece_ids = path_ids.split_off(start);
            for &inner in &piece_ids[1..] {
                // Inner vertices may legitimately repeat later — drop them
                // from the open path's table (stamp 0 is never current).
                position_of[inner as usize].0 = 0;
            }
            piece.push(point);
            rings.push(piece);
            // The pinch vertex stays on the open path (its table entry
            // still maps to `start`).
        } else {
            position_of[id as usize] = (generation, path.len() as u32);
        }
        path.push(point);
        path_ids.push(id);
    }
    debug_assert!(path.len() <= 1, "closed walk must consume the path");
}

/// Face walks over the CSR adjacency: the successor of arriving `u -> v`
/// departs `v` along the neighbor immediately clockwise of the reversal
/// `v -> u`. Every directed half-edge lands on exactly one face. `RINGS`
/// gates ring and exact-magnitude materialization: winding consumers read only
/// exact signs and the face CSR (boundaries come from
/// [`Arrangement::region_rings`]), so their walks skip both costs.
pub(crate) fn arrangement_faces<const RINGS: bool>(
    points: &[XY], topology: &crate::geometry::arrangement::IndexedTopology,
) -> Vec<crate::geometry::arrangement::Face> {
    let mut faces = Vec::new();
    // One cycle scratch for sign classification when rings are not kept
    // (`RINGS=false`); for `RINGS=true` each completed cycle is moved into the
    // face and the next face reuses a fresh buffer (no free-list needed —
    // faces own their rings for the arrangement lifetime).
    let mut cycle_scratch = Vec::with_capacity(16);
    for face in 0..topology.face_starts.len() - 1 {
        let mut cycle = if RINGS {
            Vec::with_capacity(16)
        } else {
            cycle_scratch.clear();
            std::mem::take(&mut cycle_scratch)
        };
        for &slot in &topology.face_slots[topology.face_starts[face] as usize..topology.face_starts[face + 1] as usize] {
            let slot = slot as usize;
            if cycle.is_empty() { cycle.push(points[topology.owners[slot] as usize]); }
            let to = points[topology.targets[slot] as usize];
            cycle.push(to);
        }
        let area_sign = open_xy_cycle_sign(&cycle);
        faces.push(crate::geometry::arrangement::Face {
            ring: if RINGS {
                cycle
            } else {
                cycle_scratch = cycle;
                Vec::new()
            },
            area_sign,
            component: topology.component_of[topology.owners[topology.face_slots[topology.face_starts[face] as usize] as usize] as usize],
        });
    }
    faces
}

/// Vertex dedup + undirected edge dedup with net direction counting.
/// Noded input arrives in CHAINS (each source segment's atomic pieces in
/// order), so the previous piece's end id usually answers the next
/// piece's start without a hash lookup; edges dedup by sort-and-merge —
/// no edge map at all.
#[expect(clippy::type_complexity, reason = "private construction plumbing")]
pub(crate) fn dedup_vertices_and_edges<W: WindingWeight>(
    segments: &[Segment],
    weight_of: impl Fn(usize) -> W,
    // Recycled buffers (cleared here): capacity survives across rows.
    mut ids: HashMap<PointKey, u32>,
    mut points: Vec<XY>,
    mut edges: Vec<(u32, u32, W)>,
) -> (HashMap<PointKey, u32>, Vec<XY>, Vec<(u32, u32, W)>, bool) {
    // Worst case every segment contributes two unique endpoints
    // (disconnected soups); chains share and stay under it.
    ids.clear();
    ids.reserve(segments.len() * 2);
    points.clear();
    points.reserve(segments.len() * 2);
    let mut id_of = |point: XY, points: &mut Vec<XY>| -> u32 {
        *ids.entry(PointKey::new(point)).or_insert_with(|| {
            points.push(point);
            (points.len() - 1) as u32
        })
    };
    edges.clear();
    edges.reserve(segments.len());
    let mut previous: Option<(PointKey, u32)> = None;
    // One fully chained, closed walk visits every vertex along a single
    // path — the graph is CONNECTED by construction and the component
    // pass can skip its union-find entirely (single noded loops, the
    // stroke buffer's whole caseload).
    let mut chained = true;
    for (index, segment) in segments.iter().enumerate() {
        if same_point(segment.start, segment.end) {
            continue;
        }
        let weight = weight_of(index);
        // Chained pieces share the previous end BY KEY — the same identity
        // the dedup map uses (`PointKey` and `same_point` share one ±0.0
        // canonicalization).
        let start_key = PointKey::new(segment.start);
        let a = match previous {
            Some((key, id)) if key == start_key => id,
            None => id_of(segment.start, &mut points),
            Some(_) => {
                chained = false;
                id_of(segment.start, &mut points)
            },
        };
        let end_key = PointKey::new(segment.end);
        let b = id_of(segment.end, &mut points);
        previous = Some((end_key, b));
        if a <= b {
            edges.push((a, b, weight));
        } else {
            edges.push((b, a, weight.neg()));
        }
    }
    // Sort-and-merge duplicate edges into net multiplicities.
    edges.sort_unstable_by_key(|&(a, b, _)| (a, b));
    edges.dedup_by(|right, left| {
        if (left.0, left.1) == (right.0, right.1) {
            left.2 = left.2.add(right.2);
            true
        } else {
            false
        }
    });
    // Closed: the walk returned to vertex 0 (the loop's first point).
    let connected = chained && previous.is_some_and(|(_, id)| id == 0) && !points.is_empty();
    (ids, points, edges, connected)
}

/// Two-pass CSR fill over the deduplicated edges (both half-edges placed).
pub(crate) fn build_csr<W: WindingWeight>(
    vertex_count: usize,
    edges: &[(u32, u32, W)],
    // Recycled buffers (cleared/refilled here).
    mut starts: Vec<u32>,
    mut targets: Vec<u32>,
    mut multiplicities: Vec<W>,
    mut edge_of_slot: Vec<u32>,
) -> (Vec<u32>, Vec<u32>, Vec<W>, Vec<u32>) {
    starts.clear();
    starts.resize(vertex_count + 1, 0);
    for &(a, b, _) in edges {
        starts[a as usize + 1] += 1;
        starts[b as usize + 1] += 1;
    }
    for index in 0..vertex_count {
        starts[index + 1] += starts[index];
    }
    let half_edge_count = starts[vertex_count] as usize;
    let mut cursor = starts.clone();
    targets.clear();
    targets.resize(half_edge_count, 0);
    multiplicities.clear();
    multiplicities.resize(half_edge_count, W::UNSET);
    edge_of_slot.clear();
    edge_of_slot.resize(half_edge_count, 0);
    for (edge, &(a, b, multiplicity)) in edges.iter().enumerate() {
        let slot_a = cursor[a as usize] as usize;
        targets[slot_a] = b;
        multiplicities[slot_a] = multiplicity;
        edge_of_slot[slot_a] = edge as u32;
        cursor[a as usize] += 1;
        let slot_b = cursor[b as usize] as usize;
        targets[slot_b] = a;
        multiplicities[slot_b] = multiplicity.neg();
        edge_of_slot[slot_b] = edge as u32;
        cursor[b as usize] += 1;
    }
    (starts, targets, multiplicities, edge_of_slot)
}

/// Sort every CSR row counter-clockwise by departure angle.
pub(crate) fn sort_rows_counterclockwise<W: WindingWeight>(
    points: &[XY],
    starts: &[u32],
    targets: &mut [u32],
    multiplicities: &mut [W],
    edge_of_slot: &mut [u32],
) {
    // One scratch row reused across every vertex (rows are tiny; per-row
    // allocations dominated this pass).
    let mut row: Vec<(u32, W, u32)> = Vec::new();
    for vertex in 0..starts.len() - 1 {
        let range = starts[vertex] as usize..starts[vertex + 1] as usize;
        // Degree <= 2 needs NO ordering work at all: a two-neighbor fan has
        // exactly one cyclic order — and chained-loop interiors (the vast
        // majority of noded-stroke vertices) are all degree 2.
        if range.len() <= 2 {
            continue;
        }
        row.clear();
        row.extend(
            range
                .clone()
                .map(|slot| (targets[slot], multiplicities[slot], edge_of_slot[slot])),
        );
        let origin = points[vertex];
        row.sort_unstable_by(|left, right| {
            compare_departures(
                origin,
                points[left.0 as usize],
                left.0,
                points[right.0 as usize],
                right.0,
            )
        });
        for (offset, &(target, multiplicity, edge)) in range.zip(&row) {
            targets[offset] = target;
            multiplicities[offset] = multiplicity;
            edge_of_slot[offset] = edge;
        }
    }
}

/// Counter-clockwise order starting at the positive x axis. Quadrants first
/// restrict the certified orientation to an angle smaller than pi, making its
/// sign transitive. Collinear rays are then an equivalence class, ordered by
/// target solely to make the comparator total.
pub(crate) fn compare_departures(
    origin: XY,
    left: XY,
    left_target: u32,
    right: XY,
    right_target: u32,
) -> Ordering {
    let quadrant = |point: XY| match (point.y >= origin.y, point.x >= origin.x) {
        (true, true) => 0,
        (true, false) => 1,
        (false, false) => 2,
        (false, true) => 3,
    };
    quadrant(left)
        .cmp(&quadrant(right))
        .then_with(|| match orientation(origin, left, right) {
            Orientation::CounterClockwise => Ordering::Less,
            Orientation::Clockwise => Ordering::Greater,
            Orientation::Collinear => left_target.cmp(&right_target),
        })
}
