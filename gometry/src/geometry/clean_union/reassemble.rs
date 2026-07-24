use super::*;

/// Endpoint-chain the directed result-boundary arcs into rings, then classify
/// CCW shells / CW holes and assemble a `Polygon`/`MultiPolygon`.
pub(crate) fn reassemble(arcs: &[(XY, XY)]) -> Option<Shape> {
    assemble_rings(reassemble_to_rings(arcs)?)
}

/// Endpoint-chain directed arcs into closed (open-cycle) rings, requiring
/// exactly ONE outgoing arc per vertex. An empty arc set yields no rings (a
/// valid empty `symmetric_difference` piece, not a failure). `None` on a pinch
/// (a repeated `from` — two outgoing arcs) or a walk that fails to close.
pub(crate) fn reassemble_to_rings(arcs: &[(XY, XY)]) -> Option<Vec<Vec<XY>>> {
    if arcs.is_empty() {
        return Some(Vec::new());
    }
    let mut ids: HashMap<PointKey, u32> = HashMap::with_capacity(arcs.len());
    let mut from_id: Vec<u32> = Vec::with_capacity(arcs.len());
    let mut to_id: Vec<u32> = Vec::with_capacity(arcs.len());
    for &(from, to) in arcs {
        let next = ids.len() as u32;
        from_id.push(*ids.entry(PointKey::new(from)).or_insert(next));
        let next = ids.len() as u32;
        to_id.push(*ids.entry(PointKey::new(to)).or_insert(next));
    }
    let mut out_of = vec![u32::MAX; ids.len()];
    for (index, &vertex) in from_id.iter().enumerate() {
        if out_of[vertex as usize] != u32::MAX {
            return None;
        }
        out_of[vertex as usize] = index as u32;
    }
    let mut visited = vec![false; arcs.len()];
    let mut rings: Vec<Vec<XY>> = Vec::new();
    // Reserve each ring from the arcs still unwalked: exact for the dominant
    // single-ring union (no regrowth), and never beyond what genuinely remains.
    let mut remaining = arcs.len();
    for seed in 0..arcs.len() {
        if visited[seed] {
            continue;
        }
        let mut ring: Vec<XY> = Vec::with_capacity(remaining);
        let mut cursor = seed;
        while !visited[cursor] {
            visited[cursor] = true;
            remaining -= 1;
            ring.push(arcs[cursor].0);
            let next = out_of[to_id[cursor] as usize];
            if next == u32::MAX {
                return None;
            }
            cursor = next as usize;
        }
        if cursor != seed || ring.len() < 3 {
            return None;
        }
        rings.push(ring);
    }
    Some(rings)
}

/// Classify rings into CCW shells / CW holes and assemble. Single shell takes
/// all holes (no nesting decision); multiple shells without holes form a
/// `MultiPolygon`; multi-shell WITH holes (a rare nesting) defers.
pub(crate) fn assemble_rings(rings: Vec<Vec<XY>>) -> Option<Shape> {
    let mut shells: Vec<Vec<XY>> = Vec::new();
    let mut holes: Vec<Vec<XY>> = Vec::new();
    for ring in rings {
        if open_xy_cycle_winding(&ring).is_ccw() {
            shells.push(ring);
        } else {
            holes.push(ring);
        }
    }
    let parts: Vec<Polygon> = match shells.len() {
        0 => return None,
        1 => {
            let shell = shells.pop().expect("one shell");
            // Single-shell assembly assigns every hole to that shell; verify each
            // truly nests inside it. A hole outside the shell means the result is
            // a multi-component nesting this assembler does not resolve — defer.
            if holes
                .iter()
                .any(|hole| !topology::ring_contains_interior_open(&shell, hole[0]))
            {
                return None;
            }
            vec![Polygon::new(
                Ring::from_trusted_closed(closed_coordseq(shell)),
                holes
                    .into_iter()
                    .map(|h| Ring::from_trusted_closed(closed_coordseq(h)))
                    .collect(),
            )]
        },
        _ => {
            // Several shells with holes would need point-in-shell nesting; the
            // common multi-piece result (symdiff crescents, a split difference)
            // has none, so defer the rare nested case.
            if !holes.is_empty() {
                return None;
            }
            shells
                .into_iter()
                .map(|shell| {
                    Polygon::new(
                        Ring::from_trusted_closed(closed_coordseq(shell)),
                        Vec::new(),
                    )
                })
                .collect()
        },
    };
    Some(polygon_parts_to_shape(parts))
}

/// Close an open XY ring into a `CoordSeq` (append the first vertex). Builds
/// the columns straight from the `XY` vec (`From<Vec<XY>>`) — no per-vertex
/// `Point` round-trip and re-gather.
pub(crate) fn closed_coordseq(mut ring: Vec<XY>) -> CoordSeq {
    ring.push(ring[0]);
    CoordSeq::from(ring)
}
