#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::arrangement::{Face, WindingWeight};
use crate::geometry::{
    HashMap, PointKey, Segment, XY, axis_pow2_scale, open_xy_cycle_decision, same_point,
    scaled_residual, wrap_index,
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
/// gates ring materialization: the winding consumers read only areas and
/// the face CSR (boundaries come from [`Arrangement::region_rings`]), so
/// their walks skip the per-face ring allocation entirely.
pub(crate) fn walk_faces<const RINGS: bool>(
    points: &[XY],
    starts: &[u32],
    targets: &[u32],
    owners: &[u32],
    component_of: &[u32],
    // Recycled buffer (cleared/refilled here).
    face_of: Vec<u32>,
) -> (Vec<u32>, Vec<Face>) {
    let slot_of = |from: u32, to: u32| -> usize {
        let mut range = starts[from as usize] as usize..starts[from as usize + 1] as usize;
        range
            .find(|&slot| targets[slot] == to)
            .expect("twin half-edge exists")
    };
    let mut face_of = face_of;
    face_of.clear();
    face_of.resize(targets.len(), u32::MAX);
    let mut faces: Vec<Face> = Vec::new();
    // One cycle scratch for the decision-area walk when rings are not kept
    // (`RINGS=false`); for `RINGS=true` each completed cycle is moved into the
    // face and the next face reuses a fresh buffer (no free-list needed —
    // faces own their rings for the arrangement lifetime).
    let mut cycle_scratch = Vec::with_capacity(16);
    for seed in 0..targets.len() {
        if face_of[seed] != u32::MAX {
            continue;
        }
        let face_id = faces.len() as u32;
        let mut cycle = if RINGS {
            Vec::with_capacity(16)
        } else {
            cycle_scratch.clear();
            std::mem::take(&mut cycle_scratch)
        };
        cycle.push(points[owners[seed] as usize]);
        let mut slot = seed;
        loop {
            face_of[slot] = face_id;
            let to = points[targets[slot] as usize];
            cycle.push(to);
            // Continue clockwise of the reversal at `to`.
            let reverse = slot_of(targets[slot], owners[slot]);
            let range = starts[targets[slot] as usize] as usize
                ..starts[targets[slot] as usize + 1] as usize;
            let row_len = range.end - range.start;
            let position = reverse - range.start;
            slot = range.start + wrap_index(position + row_len - 1, row_len);
            if slot == seed {
                break;
            }
        }
        let decision_area = open_xy_cycle_decision(&cycle);
        faces.push(Face {
            ring: if RINGS {
                cycle
            } else {
                cycle_scratch = cycle;
                Vec::new()
            },
            decision_area,
            component: component_of[owners[seed] as usize],
        });
    }
    (face_of, faces)
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
    mut owners: Vec<u32>,
    mut multiplicities: Vec<W>,
) -> (Vec<u32>, Vec<u32>, Vec<u32>, Vec<W>) {
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
    owners.clear();
    owners.resize(half_edge_count, 0);
    multiplicities.clear();
    multiplicities.resize(half_edge_count, W::UNSET);
    for &(a, b, multiplicity) in edges {
        let slot_a = cursor[a as usize] as usize;
        targets[slot_a] = b;
        owners[slot_a] = a;
        multiplicities[slot_a] = multiplicity;
        cursor[a as usize] += 1;
        let slot_b = cursor[b as usize] as usize;
        targets[slot_b] = a;
        owners[slot_b] = b;
        multiplicities[slot_b] = multiplicity.neg();
        cursor[b as usize] += 1;
    }
    (starts, targets, owners, multiplicities)
}

/// Sort every CSR row counter-clockwise by departure angle (ties by target
/// id; noded input never produces two distinct neighbors at one angle).
pub(crate) fn sort_rows_counterclockwise<W: WindingWeight>(
    points: &[XY],
    starts: &[u32],
    targets: &mut [u32],
    multiplicities: &mut [W],
) {
    // One scratch row reused across every vertex (rows are tiny; per-row
    // allocations dominated this pass).
    let mut row: Vec<(f64, u32, W)> = Vec::new();
    for vertex in 0..starts.len() - 1 {
        let range = starts[vertex] as usize..starts[vertex + 1] as usize;
        // Degree <= 2 needs NO ordering work at all: a two-neighbor fan has
        // exactly one cyclic order — and chained-loop interiors (the vast
        // majority of noded-stroke vertices) are all degree 2.
        if range.len() <= 2 {
            continue;
        }
        let origin = points[vertex];
        let (scale_x, scale_y) =
            departure_scales(points, origin, range.clone().map(|slot| targets[slot]));
        row.clear();
        // Pseudo-angle (monotonic in true angle, no libm `atan2` call):
        // only the CYCLIC order matters to the face walk, so the rotated
        // starting point is irrelevant.
        row.extend(range.clone().map(|slot| {
            let to = points[targets[slot] as usize];
            (
                departure_angle(origin, to, scale_x, scale_y),
                targets[slot],
                multiplicities[slot],
            )
        }));
        row.sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        for (offset, &(_, target, multiplicity)) in range.zip(&row) {
            targets[offset] = target;
            multiplicities[offset] = multiplicity;
        }
    }
}

/// One common positive diagonal frame for a vertex fan. Applying the same
/// frame to every departure preserves its cyclic order, while scaling before
/// subtraction keeps opposite finite extremes finite.
pub(crate) fn departure_scales(
    points: &[XY],
    origin: XY,
    targets: impl IntoIterator<Item = u32>,
) -> (f64, f64) {
    let (mut max_x, mut max_y) = (origin.x.abs(), origin.y.abs());
    for target in targets {
        let point = points[target as usize];
        max_x = max_x.max(point.x.abs());
        max_y = max_y.max(point.y.abs());
    }
    (axis_pow2_scale(max_x), axis_pow2_scale(max_y))
}

pub(crate) fn departure_angle(origin: XY, target: XY, scale_x: f64, scale_y: f64) -> f64 {
    crate::geometry::predicates::pseudo_angle(
        scaled_residual(target.x, origin.x, scale_x),
        scaled_residual(target.y, origin.y, scale_y),
    )
}
