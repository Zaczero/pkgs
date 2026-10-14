use super::{
    BinaryHeap, ExactChainView, ExactPartition, NodeId, Result, Reverse, Segment, SimplifyMethod,
    TopologyGuard, XY, point_segment_distance, same_point, undirected_segment_edge_key,
};

/// Exact chain coordinates plus the shared/exterior ownership class.
pub(super) struct Chain {
    pub(super) nodes: Vec<NodeId>,
    pub(super) spans: Vec<u32>,
    pub(super) points: Vec<XY>,
    pub(super) source_segments: Vec<Segment>,
    pub(super) shared: bool,
}

/// A chain's retained original node indices and their source coordinates.
pub(super) struct SimplifiedChain {
    pub(super) kept: Vec<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CoverageEdgeIntent {
    Source,
    Constructed,
}

pub(super) type CoverageEdgeRoles = Vec<Vec<Vec<CoverageEdgeIntent>>>;

pub(super) fn exact_chain(
    partition: &ExactPartition<'_>,
    view: ExactChainView,
    source_ordinals: &[usize],
    source_segments: &[Segment],
) -> Chain {
    let points = view
        .nodes
        .iter()
        .map(|&node| partition.nearest_xy(node))
        .collect();
    let mut chain_source_segments = Vec::new();
    for &source in source_ordinals {
        let segment = source_segments
            .get(source)
            .expect("chain source ordinal comes from the source segment inventory");
        if same_point(segment.start, segment.end)
            || chain_source_segments.iter().any(|prior| {
                undirected_segment_edge_key(*prior) == undirected_segment_edge_key(*segment)
            })
        {
            continue;
        }
        chain_source_segments.push(*segment);
    }
    Chain {
        nodes: view.nodes,
        spans: view.spans,
        points,
        source_segments: chain_source_segments,
        shared: view.shared,
    }
}

/// Simplify one chain with endpoints pinned and a global topology guard: a
/// vertex is dropped only when its shortcut neither crosses any other
/// surviving linework nor sweeps another vertex (which could silently move a
/// small neighbor onto the wrong side). `method` picks the importance
/// criterion — effective triangle area (Visvalingam-Whyatt) or chord
/// deviation (Douglas-Peucker) — both read `tolerance` on the same distance
/// scale.
pub(super) fn simplify_chain_kept(
    count: usize,
    xy: impl Fn(usize) -> XY,
    cyclic: bool,
    tolerance: f64,
    method: SimplifyMethod,
    guard: &TopologyGuard<'_>,
    chain_segments: &[Segment],
) -> Result<Vec<usize>> {
    simplify_chain_kept_with_try_remove(
        count,
        &xy,
        cyclic,
        tolerance,
        method,
        |_, _, _, previous, point, next| {
            Ok(guard.removal_is_safe(previous, point, next, chain_segments))
        },
    )
}

pub(in crate::geometry) fn simplify_chain_kept_with_try_remove(
    count: usize,
    xy: impl Fn(usize) -> XY,
    cyclic: bool,
    tolerance: f64,
    method: SimplifyMethod,
    try_remove: impl FnMut(usize, usize, usize, XY, XY, XY) -> Result<bool>,
) -> Result<Vec<usize>> {
    match method {
        SimplifyMethod::Vw => {
            let area_tolerance = vw_area_tolerance(tolerance);
            simplify_chain_guarded_with_try_remove(
                count,
                &xy,
                cyclic,
                &|prev, vertex, next| {
                    let area = triangle_area(xy(prev), xy(vertex), xy(next));
                    Ok((area < area_tolerance).then_some(area))
                },
                try_remove,
            )
        },
        SimplifyMethod::Dp => {
            simplify_chain_guarded_with_try_remove(
                count,
                &xy,
                cyclic,
                &|prev, vertex, next| {
                    // Point-to-segment deviation of `vertex` from the finite
                    // `prev`-`next` shortcut; importance orders smallest
                    // first. The shortcut is a segment, not its supporting
                    // line, so a projection past either endpoint must be
                    // measured to that endpoint. Distance space also keeps
                    // tiny tolerances from collapsing when squared.
                    let deviation = point_segment_distance(xy(vertex), Segment {
                        start: xy(prev),
                        end: xy(next),
                    })?;
                    Ok((deviation < tolerance).then_some(deviation))
                },
                try_remove,
            )
        },
    }
}

/// Guarded greedy simplification of one chain, generic over the importance
/// criterion (finite-shortcut deviation for Douglas-Peucker, effective
/// triangle area for Visvalingam-Whyatt): `candidate` scores a removable
/// vertex from its two neighbors, `None` once it must survive. The guard is
/// probed lazily in importance order — the first safe candidate IS the
/// minimum safe one, so each round pays O(failures + 1) guard queries instead
/// of guarding every candidate. Endpoints (and a cyclic chain's anchor/closure)
/// are pinned; cyclic chains keep at least a triangle.
///
/// `xy` is random-access original XY (borrowed columns or an index slice).
/// Survivors are original indices so the caller can select from the source
/// carrier without staging a `Point` AoS just to walk the chain.
pub(in crate::geometry) fn simplify_chain_guarded(
    count: usize,
    xy: impl Fn(usize) -> XY,
    cyclic: bool,
    candidate: &impl Fn(usize, usize, usize) -> Result<Option<f64>>,
    guard: &TopologyGuard<'_>,
) -> Result<Vec<usize>> {
    simplify_chain_guarded_with_segments(count, xy, cyclic, candidate, guard, &[])
}

pub(in crate::geometry) fn simplify_chain_guarded_with_segments(
    count: usize,
    xy: impl Fn(usize) -> XY,
    cyclic: bool,
    candidate: &impl Fn(usize, usize, usize) -> Result<Option<f64>>,
    guard: &TopologyGuard<'_>,
    chain_segments: &[Segment],
) -> Result<Vec<usize>> {
    simplify_chain_guarded_with_try_remove(
        count,
        xy,
        cyclic,
        candidate,
        |_, _, _, previous, point, next| {
            Ok(guard.removal_is_safe(previous, point, next, chain_segments))
        },
    )
}

/// Guarded greedy simplification with a caller-owned decision and update
/// boundary. A `true` result commits the candidate's topology-state update;
/// `false` must leave that state unchanged.
pub(in crate::geometry) fn simplify_chain_guarded_with_try_remove(
    count: usize,
    xy: impl Fn(usize) -> XY,
    cyclic: bool,
    candidate: &impl Fn(usize, usize, usize) -> Result<Option<f64>>,
    mut try_remove: impl FnMut(usize, usize, usize, XY, XY, XY) -> Result<bool>,
) -> Result<Vec<usize>> {
    let floor = if cyclic { 4 } else { 2 };
    if count <= floor {
        return Ok((0..count).collect());
    }
    // Lazy min-heap over a doubly linked interior walk: each removal
    // re-evaluates only its two neighbors (generation stamps invalidate
    // stale entries), so the whole pass is O(n log n) instead of a full
    // rescan + sort + shift per removal. A declined candidate is revisited
    // only when one of its neighbors changes; a stateful guard must therefore
    // remain conservative when a nonlocal obstruction later disappears.
    let total_order_key = |importance: f64| -> u64 {
        let bits = importance.to_bits();
        if bits >> 63 == 1 {
            !bits
        } else {
            bits ^ (1 << 63)
        }
    };
    let mut prev: Vec<_> = (0..count).map(|index| index.wrapping_sub(1)).collect();
    let mut next: Vec<_> = (1..=count).collect();
    let mut generation = vec![0_usize; count];
    let mut removed = vec![false; count];
    let mut heap = BinaryHeap::with_capacity(count);
    for index in 1..count.wrapping_sub(1) {
        if let Some(importance) = candidate(index.wrapping_sub(1), index, index.wrapping_add(1))? {
            heap.push(Reverse((total_order_key(importance), index, 0)));
        }
    }
    let mut live = count;
    while live > floor {
        let Some(Reverse((_, index, stamp))) = heap.pop() else {
            break;
        };
        if *removed
            .get(index)
            .expect("heap vertex index is within the chain")
            || *generation
                .get(index)
                .expect("heap generation index is within the chain")
                != stamp
        {
            continue;
        }
        let (before, after) = (
            *prev
                .get(index)
                .expect("heap vertex index is within the chain"),
            *next
                .get(index)
                .expect("heap vertex index is within the chain"),
        );
        if !try_remove(before, index, after, xy(before), xy(index), xy(after))? {
            continue;
        }
        *removed
            .get_mut(index)
            .expect("heap vertex index is within the chain") = true;
        live = live.wrapping_sub(1);
        *next
            .get_mut(before)
            .expect("previous vertex index is within the chain") = after;
        *prev
            .get_mut(after)
            .expect("next vertex index is within the chain") = before;
        for neighbor in [before, after] {
            if neighbor == 0 || neighbor == count.wrapping_sub(1) {
                continue;
            }
            // A vertex is refreshed at most once per removal, fewer than count times.
            let stamp = generation
                .get_mut(neighbor)
                .expect("neighbor vertex index is within the chain");
            *stamp = stamp.wrapping_add(1);
            if let Some(importance) = candidate(
                *prev
                    .get(neighbor)
                    .expect("neighbor index is within the chain"),
                neighbor,
                *next
                    .get(neighbor)
                    .expect("neighbor index is within the chain"),
            )? {
                heap.push(Reverse((
                    total_order_key(importance),
                    neighbor,
                    *generation
                        .get(neighbor)
                        .expect("neighbor generation index is within the chain"),
                )));
            }
        }
    }
    let mut kept = Vec::with_capacity(live);
    let mut cursor = 0;
    while cursor < count {
        kept.push(cursor);
        cursor = *next
            .get(cursor)
            .expect("simplification link index is within the chain");
    }
    Ok(kept)
}

/// The Visvalingam-Whyatt effective-area threshold for a distance-scale
/// `tolerance` — GEOS's coverage-simplifier convention, shared by
/// `simplify_vw` and `coverage_simplify` so the same tolerance value means
/// the same thing across the whole simplify family.
pub(crate) fn vw_area_tolerance(tolerance: f64) -> f64 {
    tolerance * tolerance / 2.0
}

pub(in crate::geometry) fn triangle_area(
    a: impl Into<XY>,
    b: impl Into<XY>,
    c: impl Into<XY>,
) -> f64 {
    let (a, b, c) = (a.into(), b.into(), c.into());
    0.5 * ((b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)).abs()
}
