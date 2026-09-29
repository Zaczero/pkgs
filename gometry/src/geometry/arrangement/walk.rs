use std::cmp::Ordering;

#[cfg(test)]
use crate::error::{ResourceError, Result};
#[cfg(test)]
use crate::geometry::arrangement::{Face, IndexedTopology, WindingWeight};
#[cfg(test)]
use crate::geometry::{HashMap, PointKey, Segment, open_xy_cycle_sign, same_point};
use crate::geometry::{Orientation, XY, orientation};

/// Assign one face to every directed half-edge and retain its exact area sign.
#[cfg(test)]
pub(crate) fn arrangement_faces(points: &[XY], topology: &IndexedTopology) -> Vec<Face> {
    let mut faces = Vec::with_capacity(topology.face_starts.len() - 1);
    let mut cycle = Vec::new();
    for face in 0..topology.face_starts.len() - 1 {
        let cycle_len = (topology.face_starts[face + 1] - topology.face_starts[face]) as usize + 1;
        cycle.clear();
        cycle.reserve(cycle_len);
        for &slot in &topology.face_slots
            [topology.face_starts[face] as usize..topology.face_starts[face + 1] as usize]
        {
            let slot = slot as usize;
            if cycle.is_empty() {
                cycle.push(points[topology.owners[slot] as usize]);
            }
            let to = points[topology.targets[slot] as usize];
            cycle.push(to);
        }
        let area_sign = open_xy_cycle_sign(&cycle);
        faces.push(Face {
            area_sign,
            component: topology.component_of[topology.owners
                [topology.face_slots[topology.face_starts[face] as usize] as usize]
                as usize],
        });
    }
    faces
}

/// Vertex dedup + undirected edge dedup with net direction counting.
/// Noded input arrives in CHAINS (each source segment's atomic pieces in
/// order), so the previous piece's end id usually answers the next
/// piece's start without a hash lookup; edges dedup by sort-and-merge —
/// no edge map at all.
#[cfg(test)]
#[expect(clippy::type_complexity, reason = "private construction plumbing")]
pub(crate) fn dedup_vertices_and_edges<W: WindingWeight>(
    segments: impl ExactSizeIterator<Item = Segment>,
    weight_of: impl Fn(usize) -> W,
    // Recycled buffers (cleared here): capacity survives across rows.
    mut ids: HashMap<PointKey, u32>,
    mut points: Vec<XY>,
    mut edges: Vec<(u32, u32, W)>,
) -> Result<(HashMap<PointKey, u32>, Vec<XY>, Vec<(u32, u32, W)>)> {
    ids.clear();
    let segment_count = segments.len();
    // Worst case every segment contributes two unique endpoints
    // (disconnected soups); chains share and stay under it.
    let endpoint_count = segment_count
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    ids.reserve(endpoint_count);
    points.clear();
    points.reserve(endpoint_count);
    let mut id_of = |point: XY, points: &mut Vec<XY>| -> Result<u32> {
        match ids.entry(PointKey::new(point)) {
            std::collections::hash_map::Entry::Occupied(entry) => Ok(*entry.get()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let id = u32::try_from(points.len()).map_err(|_| ResourceError::SizeOverflow)?;
                points.push(point);
                entry.insert(id);
                Ok(id)
            },
        }
    };
    edges.clear();
    edges.reserve(segment_count);
    let mut previous: Option<(PointKey, u32)> = None;
    for (index, segment) in segments.enumerate() {
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
            _ => id_of(segment.start, &mut points)?,
        };
        let end_key = PointKey::new(segment.end);
        let b = id_of(segment.end, &mut points)?;
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
    Ok((ids, points, edges))
}

/// Two-pass CSR fill over the deduplicated edges (both half-edges placed).
#[cfg(test)]
#[expect(clippy::type_complexity, reason = "private construction plumbing")]
pub(crate) fn build_csr<W: WindingWeight>(
    vertex_count: usize,
    edges: &[(u32, u32, W)],
    // Recycled buffers (cleared/refilled here).
    mut starts: Vec<u32>,
    mut targets: Vec<u32>,
    mut multiplicities: Vec<W>,
    mut edge_of_slot: Vec<u32>,
) -> Result<(Vec<u32>, Vec<u32>, Vec<W>, Vec<u32>)> {
    let half_edge_count = IndexedTopology::admit_counts(vertex_count, edges.len())?;
    let start_count = vertex_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    starts.clear();
    starts.reserve(start_count);
    starts.resize(start_count, 0);
    for &(a, b, _) in edges {
        starts[a as usize + 1] += 1;
        starts[b as usize + 1] += 1;
    }
    for index in 0..vertex_count {
        starts[index + 1] += starts[index];
    }
    let mut cursor = starts.clone();
    targets.clear();
    targets.reserve(half_edge_count);
    targets.resize(half_edge_count, 0);
    multiplicities.clear();
    multiplicities.reserve(half_edge_count);
    multiplicities.resize(half_edge_count, W::UNSET);
    edge_of_slot.clear();
    edge_of_slot.reserve(half_edge_count);
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
    Ok((starts, targets, multiplicities, edge_of_slot))
}

/// Sort every CSR row counter-clockwise by departure angle.
#[cfg(test)]
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
        row.reserve(range.len());
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
