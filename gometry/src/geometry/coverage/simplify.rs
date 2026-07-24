#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::borrow::Cow;
use std::collections::BinaryHeap;

use super::*;
use crate::error::Result;

// --- Simplification ---------------------------------------------------------

/// One maximal linework chain between coverage nodes: its vertex run and
/// whether it is shared between rows (an interior interface) or exterior
/// boundary.
struct Chain {
    points: Vec<XY>,
    shared: bool,
}

/// Decompose the coverage's unique linework into chains and a segment→chain
/// lookup. Nodes are vertices of degree != 2 or where the shared/boundary
/// class flips; a fully closed degree-2 ring becomes one cyclic chain pinned
/// at its lexicographically smallest vertex.
/// The coverage's unique linework as a vertex graph, classified shared vs
/// exterior — the substrate the chain decomposition walks.
struct LineworkGraph {
    segment_class: HashMap<(PointKey, PointKey), (Segment, bool)>,
    adjacency: HashMap<PointKey, Vec<PointKey>>,
    points: HashMap<PointKey, XY>,
}

impl LineworkGraph {
    fn build(rows: &[CoverageRow<'_>]) -> Self {
        let occurrences = edge_row_occurrences(rows);
        let mut graph = Self {
            segment_class: HashMap::new(),
            adjacency: HashMap::new(),
            points: HashMap::new(),
        };
        for row in rows {
            for &segment in &row.segments {
                let key = undirected_segment_edge_key(segment);
                if graph.segment_class.contains_key(&key) {
                    continue;
                }
                let shared = occurrences[&key] == 2;
                graph.segment_class.insert(key, (segment, shared));
                let (a, b) = (PointKey::new(segment.start), PointKey::new(segment.end));
                graph.points.insert(a, segment.start);
                graph.points.insert(b, segment.end);
                graph.adjacency.entry(a).or_default().push(b);
                graph.adjacency.entry(b).or_default().push(a);
            }
        }
        graph
    }

    /// Chain endpoints: degree != 2, or a shared/exterior class flip.
    fn is_node(&self, key: PointKey) -> bool {
        let neighbors = &self.adjacency[&key];
        if neighbors.len() != 2 {
            return true;
        }
        let class = |other: PointKey| self.segment_class[&ordered_edge(key, other)].1;
        class(neighbors[0]) != class(neighbors[1])
    }

    /// The unique continuation of a degree-2 walk arriving at `at` from
    /// `previous`.
    fn step(&self, at: PointKey, previous: PointKey) -> PointKey {
        self.adjacency[&at]
            .iter()
            .copied()
            .find(|&n| n != previous)
            .expect("degree-2 interior vertex")
    }

    /// The anchor of the chain containing `start -> end`: the nearest node
    /// walking backwards, or — on a fully cyclic chain — its smallest
    /// vertex, paired with the walk direction.
    fn chain_anchor(&self, start: PointKey, end: PointKey) -> (PointKey, PointKey) {
        if self.is_node(start) {
            return (start, end);
        }
        let mut previous = end;
        let mut cursor = start;
        loop {
            let next = self.step(cursor, previous);
            previous = cursor;
            cursor = next;
            if self.is_node(cursor) || cursor == start {
                break;
            }
        }
        if cursor != start || self.is_node(cursor) {
            return (cursor, previous);
        }
        // Cyclic chain: pin the smallest vertex as the anchor.
        let mut ring = vec![start];
        let mut prev = start;
        let mut at = end;
        while at != start {
            ring.push(at);
            let next = self.step(at, prev);
            prev = at;
            at = next;
        }
        let anchor = ring
            .iter()
            .enumerate()
            .min_by_key(|(_, key)| **key)
            .map(|(index, _)| index)
            .expect("non-empty ring");
        ring.rotate_left(anchor);
        (ring[0], ring[1])
    }
}

fn coverage_chains(rows: &[CoverageRow<'_>]) -> (Vec<Chain>, HashMap<(PointKey, PointKey), usize>) {
    let graph = LineworkGraph::build(rows);
    let mut chains: Vec<Chain> = Vec::new();
    let mut chain_of: HashMap<(PointKey, PointKey), usize> = HashMap::new();
    // Deterministic seed order: sorted segment keys.
    let mut keys: Vec<(PointKey, PointKey)> = graph.segment_class.keys().copied().collect();
    keys.sort_unstable();
    for key in keys {
        if chain_of.contains_key(&key) {
            continue;
        }
        let (segment, shared) = graph.segment_class[&key];
        let (origin, towards) =
            graph.chain_anchor(PointKey::new(segment.start), PointKey::new(segment.end));
        // Walk forward from the anchor, collecting the chain.
        let mut run = vec![origin];
        let mut prev = origin;
        let mut at = towards;
        loop {
            run.push(at);
            if graph.is_node(at) || at == origin {
                break;
            }
            let next = graph.step(at, prev);
            prev = at;
            at = next;
        }
        let id = chains.len();
        for &[start, end] in run.array_windows::<2>() {
            chain_of.insert(ordered_edge(start, end), id);
        }
        chains.push(Chain {
            points: run.iter().map(|key| graph.points[key]).collect(),
            shared,
        });
    }
    (chains, chain_of)
}

/// Simplify one chain with endpoints pinned and a global topology guard: a
/// vertex is dropped only when its shortcut neither crosses any other
/// surviving linework nor sweeps another vertex (which could silently move a
/// small neighbor onto the wrong side). `method` picks the importance
/// criterion — effective triangle area (Visvalingam-Whyatt) or chord
/// deviation (Douglas-Peucker) — both read `tolerance` on the same distance
/// scale.
fn simplify_chain(
    chain: &[XY],
    cyclic: bool,
    tolerance: f64,
    method: SimplifyMethod,
    guard: &TopologyGuard<'_>,
) -> Vec<XY> {
    match method {
        SimplifyMethod::Vw => {
            let area_tolerance = vw_area_tolerance(tolerance);
            simplify_chain_guarded(
                chain,
                cyclic,
                &|prev, vertex, next| {
                    let area = triangle_area(prev, vertex, next);
                    (area < area_tolerance).then_some(area)
                },
                guard,
            )
        },
        SimplifyMethod::Dp => {
            let tolerance_sq = tolerance * tolerance;
            simplify_chain_guarded(
                chain,
                cyclic,
                &|prev, vertex, next| {
                    // Squared perpendicular deviation of `vertex` from the
                    // `prev`-`next` chord; importance orders smallest first.
                    let (p, v, n): (XY, XY, XY) = (prev, vertex, next);
                    let base_sq = (n.x - p.x).powi(2) + (n.y - p.y).powi(2);
                    let deviation_sq = if base_sq == 0.0 {
                        (v.x - p.x).powi(2) + (v.y - p.y).powi(2)
                    } else {
                        let cross = (n.x - p.x) * (v.y - p.y) - (n.y - p.y) * (v.x - p.x);
                        cross * cross / base_sq
                    };
                    (deviation_sq < tolerance_sq).then_some(deviation_sq)
                },
                guard,
            )
        },
    }
}

/// Guarded greedy simplification of one chain, generic over the importance
/// criterion (chord deviation for Douglas-Peucker, effective triangle area
/// for Visvalingam-Whyatt): `candidate` scores a removable vertex from its
/// two neighbors, `None` once it must survive. The guard is probed lazily
/// in importance order — the first safe candidate IS the minimum safe one,
/// so each round pays O(failures + 1) guard queries instead of guarding
/// every candidate. Endpoints (and a cyclic chain's anchor/closure) are
/// pinned; cyclic chains keep at least a triangle.
pub(in crate::geometry) fn simplify_chain_guarded<P: Copy + Into<XY>>(
    alive: &[P],
    cyclic: bool,
    candidate: &impl Fn(P, P, P) -> Option<f64>,
    guard: &TopologyGuard<'_>,
) -> Vec<P> {
    let floor = if cyclic { 4 } else { 2 };
    let count = alive.len();
    if count <= floor {
        return alive.to_vec();
    }
    // Lazy min-heap over a doubly linked interior walk: each removal
    // re-evaluates only its two neighbors (generation stamps invalidate
    // stale entries), so the whole pass is O(n log n) instead of a full
    // rescan + sort + shift per removal. Selection order is identical to
    // the rescan form: the guard is a pure function of the triple (it
    // checks the STATIC original linework), so an unsafe candidate stays
    // unsafe until a neighbor changes — which re-pushes it.
    let total_order_key = |importance: f64| -> u64 {
        let bits = importance.to_bits();
        if bits >> 63 == 1 {
            !bits
        } else {
            bits ^ (1 << 63)
        }
    };
    let mut prev: Vec<usize> = (0..count).map(|index| index.wrapping_sub(1)).collect();
    let mut next: Vec<usize> = (1..=count).collect();
    let mut generation = vec![0_u32; count];
    let mut removed = vec![false; count];
    let mut heap: BinaryHeap<Reverse<(u64, usize, u32)>> = BinaryHeap::with_capacity(count);
    for index in 1..count - 1 {
        if let Some(importance) = candidate(alive[index - 1], alive[index], alive[index + 1]) {
            heap.push(Reverse((total_order_key(importance), index, 0)));
        }
    }
    let mut live = count;
    while live > floor {
        let Some(Reverse((_, index, stamp))) = heap.pop() else {
            break;
        };
        if removed[index] || generation[index] != stamp {
            continue;
        }
        let (before, after) = (prev[index], next[index]);
        if !guard.removal_is_safe(
            alive[before].into(),
            alive[index].into(),
            alive[after].into(),
        ) {
            continue;
        }
        removed[index] = true;
        live -= 1;
        next[before] = after;
        prev[after] = before;
        for neighbor in [before, after] {
            if neighbor == 0 || neighbor == count - 1 {
                continue;
            }
            generation[neighbor] += 1;
            if let Some(importance) = candidate(
                alive[prev[neighbor]],
                alive[neighbor],
                alive[next[neighbor]],
            ) {
                heap.push(Reverse((
                    total_order_key(importance),
                    neighbor,
                    generation[neighbor],
                )));
            }
        }
    }
    let mut kept = Vec::with_capacity(live);
    let mut cursor = 0;
    while cursor < count {
        kept.push(alive[cursor]);
        cursor = next[cursor];
    }
    kept
}

/// The Visvalingam-Whyatt effective-area threshold for a distance-scale
/// `tolerance` — GEOS's coverage-simplifier convention, shared by
/// `simplify_vw` and `coverage_simplify` so the same tolerance value means
/// the same thing across the whole simplify family.
#[doc(hidden)]
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

/// The topology guard behind chain simplification: indexes every original
/// segment and vertex once; a removal is safe when the shortcut crosses no
/// other segment and no foreign vertex lies inside the swept triangle.
/// Checking against the ORIGINAL linework is conservative (a segment already
/// simplified away can still veto), which can under-simplify but can never
/// produce a crossing.
pub(in crate::geometry) struct TopologyGuard<'a> {
    segments: &'a [Segment],
    // Both trees build LAZILY on the first probe: exact-collinear removals
    // (the dominant case on digitized/gridded inputs) are accepted without
    // probing, so an all-collinear pass never pays either bulk load.
    segment_index: std::cell::OnceCell<SegmentIndex>,
    vertex_cloud: Vec<[f64; 2]>,
    vertex_tree: std::cell::OnceCell<BulkRTree<[f64; 2]>>,
}

impl<'a> TopologyGuard<'a> {
    /// Wrap the original segments and the vertex cloud they span.
    pub(in crate::geometry) fn new(
        segments: &'a [Segment],
        vertices: impl IntoIterator<Item = XY>,
    ) -> Self {
        Self {
            segments,
            segment_index: std::cell::OnceCell::new(),
            vertex_cloud: vertices.into_iter().map(|p| [p.x, p.y]).collect(),
            vertex_tree: std::cell::OnceCell::new(),
        }
    }

    fn segment_index(&self) -> &SegmentIndex {
        self.segment_index
            .get_or_init(|| SegmentIndex::build(self.segments))
    }

    fn vertex_tree(&self) -> &BulkRTree<[f64; 2]> {
        self.vertex_tree
            .get_or_init(|| BulkRTree::bulk_load_with_params(self.vertex_cloud.clone()))
    }

    pub(in crate::geometry) fn removal_is_safe(&self, prev: XY, vertex: XY, next: XY) -> bool {
        // Exact-collinear interior vertex: the shortcut occupies precisely
        // the union of the two original segments — the linework's point set
        // is unchanged, so both probes are vacuous. This keeps gridded /
        // densified inputs from ever touching the trees.
        if orientation(prev, vertex, next) == Orientation::Collinear
            && vertex.x >= prev.x.min(next.x)
            && vertex.x <= prev.x.max(next.x)
            && vertex.y >= prev.y.min(next.y)
            && vertex.y <= prev.y.max(next.y)
        {
            return true;
        }
        // No crossing of the shortcut with any segment that does not share
        // one of its endpoints.
        let shortcut = Segment {
            start: prev,
            end: next,
        };
        let crossing = self
            .segment_index()
            .intersecting_candidates(shortcut)
            .any(|entry| {
                let touches_endpoint = [entry.segment.start, entry.segment.end]
                    .into_iter()
                    .any(|p| same_point(p, prev) || same_point(p, next) || same_point(p, vertex));
                !touches_endpoint && segments_intersect(shortcut, entry.segment)
            });
        if crossing {
            return false;
        }
        // No foreign vertex strictly inside the swept triangle.
        let (min_x, max_x) = (
            prev.x.min(vertex.x).min(next.x),
            prev.x.max(vertex.x).max(next.x),
        );
        let (min_y, max_y) = (
            prev.y.min(vertex.y).min(next.y),
            prev.y.max(vertex.y).max(next.y),
        );
        let inside = self
            .vertex_tree()
            .locate_in_envelope_intersecting(AABB::from_corners([min_x, min_y], [max_x, max_y]))
            .any(|coords| {
                let point = XY::new(coords[0], coords[1]);
                !(same_point(point, prev) || same_point(point, vertex) || same_point(point, next))
                    && point_in_triangle(point, prev, vertex, next)
            });
        !inside
    }
}

fn point_in_triangle(point: XY, a: XY, b: XY, c: XY) -> bool {
    let oab = orientation(a, b, point);
    let obc = orientation(b, c, point);
    let oca = orientation(c, a, point);
    let clockwise = [oab, obc, oca]
        .iter()
        .all(|&o| o != Orientation::CounterClockwise);
    let counter = [oab, obc, oca].iter().all(|&o| o != Orientation::Clockwise);
    clockwise || counter
}

/// Simplify a valid polygonal coverage's boundaries, preserving topology.
///
/// Shared interfaces are simplified ONCE and spliced into both sides, so
/// neighbors keep vector-identical linework. `simplify_boundary` controls
/// whether exterior (unshared) edges simplify too.
pub(crate) fn coverage_simplify<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    tolerance: f64,
    method: SimplifyMethod,
    simplify_boundary: bool,
) -> Result<Vec<Shape>> {
    if !(tolerance.is_finite() && tolerance >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("tolerance", tolerance).into());
    }
    let prepared = valid_coverage_rows(rows, "coverage_simplify")?;
    if same_topological_coordinate(tolerance, 0.0) {
        return Ok(prepared.iter().map(|row| row.shape.clone()).collect());
    }
    let (chains, chain_of) = coverage_chains(&prepared);

    let all_segments: Vec<Segment> = prepared
        .iter()
        .flat_map(|row| row.segments.iter().copied())
        .collect();
    let guard = TopologyGuard::new(
        &all_segments,
        chains.iter().flat_map(|chain| chain.points.iter().copied()),
    );
    let simplified: Vec<Cow<'_, [XY]>> = chains
        .iter()
        .map(|chain| {
            if !simplify_boundary && !chain.shared {
                return Cow::Borrowed(chain.points.as_slice());
            }
            let cyclic = chain.points.len() > 1
                && same_point(chain.points[0], *chain.points.last().expect("non-empty"));
            Cow::Owned(simplify_chain(
                &chain.points,
                cyclic,
                tolerance,
                method,
                &guard,
            ))
        })
        .collect();

    Ok(prepared
        .iter()
        .map(|row| rebuild_row(row.shape, &chain_of, &simplified))
        .collect())
}

/// Rebuild one row's polygons by splicing simplified chains into each ring.
fn rebuild_row(
    shape: &Shape,
    chain_of: &HashMap<(PointKey, PointKey), usize>,
    simplified: &[Cow<'_, [XY]>],
) -> Shape {
    let rebuild_ring = |ring: &CoordSeq| -> Vec<Point> {
        let original: Vec<Point> = ring.iter_coords().collect();
        if original.len() < 4 {
            return original;
        }
        // Match collect_polygon_segments: elide stutter vertices so chain
        // lookups never probe zero-length edges absent from the linework graph.
        let mut open: Vec<Point> = original[..original.len() - 1].to_vec();
        dedup_consecutive_points(&mut open);
        if open.len() < 3 {
            return original;
        }
        let source_point_by_key: HashMap<PointKey, Point> = open
            .iter()
            .copied()
            .map(|point| (PointKey::new(point), point))
            .collect();
        let source_point = |point: XY| {
            source_point_by_key
                .get(&PointKey::new(point))
                .copied()
                .expect("coverage_simplify keeps only source vertices")
        };
        let n = open.len();
        // Rotate the walk to start at a chain boundary so every chain is
        // traversed in one contiguous run.
        let chain_at = |i: usize| {
            chain_of[&ordered_edge(PointKey::new(open[i]), PointKey::new(open[(i + 1) % n]))]
        };
        let start = (0..n)
            .find(|&i| chain_at((i + n - 1) % n) != chain_at(i))
            .unwrap_or(0);
        let mut out: Vec<Point> = Vec::new();
        let mut i = 0;
        while i < n {
            let here = (start + i) % n;
            let id = chain_at(here);
            // The run of this chain along the ring.
            let mut len = 1;
            while len < n && chain_at((start + i + len) % n) == id {
                len += 1;
            }
            let run_start = open[here].xy();
            let replacement = &simplified[id];
            let forward = same_point(replacement[0], run_start);
            // Splice all but the final point (the next run repeats it).
            if forward {
                out.extend(
                    replacement[..replacement.len() - 1]
                        .iter()
                        .copied()
                        .map(source_point),
                );
            } else {
                out.extend(
                    replacement
                        .iter()
                        .rev()
                        .take(replacement.len() - 1)
                        .copied()
                        .map(source_point),
                );
            }
            i += len;
        }
        if let Some(&first) = out.first() {
            out.push(first);
        }
        out
    };
    match shape {
        Shape::Polygon(polygon) => Shape::Polygon(rebuild_polygon(polygon, &rebuild_ring)),
        Shape::MultiPolygon(polygons) => Shape::MultiPolygon(
            polygons
                .iter()
                .map(|polygon| rebuild_polygon(polygon, &rebuild_ring))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn rebuild_polygon(polygon: &Polygon, rebuild_ring: &impl Fn(&CoordSeq) -> Vec<Point>) -> Polygon {
    Polygon::new(
        Ring::from_trusted_closed(rebuild_ring(polygon.shell.coords())),
        polygon
            .holes
            .iter()
            .map(|hole| Ring::from_trusted_closed(rebuild_ring(hole.coords())))
            .collect(),
    )
}
