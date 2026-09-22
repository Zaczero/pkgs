use std::collections::BinaryHeap;

use crate::error::Result;
use crate::geometry::coverage::{AABB, HashMap, Reverse, valid_coverage_rows};
use crate::geometry::overlay::{ExactChainView, ExactPartition, ExactSourceInventory, NodeId};
use crate::geometry::{
    BulkRTree, CoordSeq, Coordinates as _, GeometryErrorKind, Orientation, Point, PointKey,
    Polygon, Ring, Segment, SegmentIndex, Shape, SimplifyMethod, XY, dedup_consecutive_points,
    line_segments, orientation, point_segment_distance, same_point, same_topological_coordinate,
    segment_contact_exact, undirected_segment_edge_key,
};

// --- Simplification ---------------------------------------------------------

/// Exact chain coordinates plus the shared/exterior ownership class.
struct Chain {
    nodes: Vec<NodeId>,
    spans: Vec<u32>,
    points: Vec<XY>,
    source_segments: Vec<Segment>,
    shared: bool,
}

/// A chain's retained original node indices and their source coordinates.
struct SimplifiedChain {
    kept: Vec<usize>,
}

fn exact_chain(
    partition: &ExactPartition<'_>,
    view: ExactChainView,
    source_spans: &[Vec<(u32, bool)>],
    source_segments: &[Segment],
) -> Result<Chain> {
    let mut points = Vec::new();
    crate::try_reserve_vec(&mut points, view.nodes.len())?;
    for &node in &view.nodes {
        points.push(partition.node_xy(node));
    }
    let mut chain_source_segments = Vec::new();
    for (source, segment) in source_segments.iter().enumerate() {
        if same_point(segment.start, segment.end)
            || !source_spans[source]
                .iter()
                .any(|&(span, _)| view.spans.contains(&span))
            || chain_source_segments.iter().any(|prior| {
                undirected_segment_edge_key(*prior) == undirected_segment_edge_key(*segment)
            })
        {
            continue;
        }
        crate::try_push_vec(&mut chain_source_segments, *segment)?;
    }
    Ok(Chain {
        nodes: view.nodes,
        spans: view.spans,
        points,
        source_segments: chain_source_segments,
        shared: view.shared,
    })
}

/// Simplify one chain with endpoints pinned and a global topology guard: a
/// vertex is dropped only when its shortcut neither crosses any other
/// surviving linework nor sweeps another vertex (which could silently move a
/// small neighbor onto the wrong side). `method` picks the importance
/// criterion — effective triangle area (Visvalingam-Whyatt) or chord
/// deviation (Douglas-Peucker) — both read `tolerance` on the same distance
/// scale.
fn simplify_chain_kept(
    count: usize,
    xy: impl Fn(usize) -> XY,
    cyclic: bool,
    tolerance: f64,
    method: SimplifyMethod,
    guard: &TopologyGuard<'_>,
    chain_segments: &[Segment],
) -> Result<Vec<usize>> {
    match method {
        SimplifyMethod::Vw => {
            let area_tolerance = vw_area_tolerance(tolerance);
            simplify_chain_guarded_with_segments(
                count,
                xy,
                cyclic,
                &|prev, vertex, next| {
                    let area = triangle_area(prev, vertex, next);
                    Ok((area < area_tolerance).then_some(area))
                },
                guard,
                chain_segments,
            )
        },
        SimplifyMethod::Dp => {
            simplify_chain_guarded_with_segments(
                count,
                xy,
                cyclic,
                &|prev, vertex, next| {
                    // Point-to-segment deviation of `vertex` from the finite
                    // `prev`-`next` shortcut; importance orders smallest
                    // first. The shortcut is a segment, not its supporting
                    // line, so a projection past either endpoint must be
                    // measured to that endpoint. Distance space also keeps
                    // tiny tolerances from collapsing when squared.
                    let deviation = point_segment_distance(vertex, Segment {
                        start: prev,
                        end: next,
                    })?;
                    Ok((deviation < tolerance).then_some(deviation))
                },
                guard,
                chain_segments,
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
    candidate: &impl Fn(XY, XY, XY) -> Result<Option<f64>>,
    guard: &TopologyGuard<'_>,
) -> Result<Vec<usize>> {
    simplify_chain_guarded_with_segments(count, xy, cyclic, candidate, guard, &[])
}

pub(in crate::geometry) fn simplify_chain_guarded_with_segments(
    count: usize,
    xy: impl Fn(usize) -> XY,
    cyclic: bool,
    candidate: &impl Fn(XY, XY, XY) -> Result<Option<f64>>,
    guard: &TopologyGuard<'_>,
    chain_segments: &[Segment],
) -> Result<Vec<usize>> {
    let floor = if cyclic { 4 } else { 2 };
    if count <= floor {
        return Ok((0..count).collect());
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
    let mut prev: Vec<_> = (0..count).map(|index| index.wrapping_sub(1)).collect();
    let mut next: Vec<_> = (1..=count).collect();
    let mut generation = vec![0_usize; count];
    let mut removed = vec![false; count];
    let mut heap = BinaryHeap::with_capacity(count);
    for index in 1..count - 1 {
        if let Some(importance) = candidate(xy(index - 1), xy(index), xy(index + 1))? {
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
        if !guard.removal_is_safe(xy(before), xy(index), xy(after), chain_segments)? {
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
            // A vertex is refreshed at most once per removal, fewer than count times.
            generation[neighbor] = generation[neighbor].wrapping_add(1);
            if let Some(importance) =
                candidate(xy(prev[neighbor]), xy(neighbor), xy(next[neighbor]))?
            {
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
        kept.push(cursor);
        cursor = next[cursor];
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
    ///
    /// `vertex_cloud` is the guard's required XY index payload, gathered by
    /// walking original carriers once. Construction does not allocate.
    pub(in crate::geometry) const fn new(
        segments: &'a [Segment],
        vertex_cloud: Vec<[f64; 2]>,
    ) -> Self {
        Self {
            segments,
            segment_index: std::cell::OnceCell::new(),
            vertex_cloud,
            vertex_tree: std::cell::OnceCell::new(),
        }
    }

    /// Walk original XY once into the vertex cloud the index stores.
    pub(in crate::geometry) fn try_vertex_cloud(
        vertices: impl IntoIterator<Item = XY>,
    ) -> Result<Vec<[f64; 2]>> {
        let vertices = vertices.into_iter();
        let mut vertex_cloud = Vec::new();
        crate::try_reserve_vec(&mut vertex_cloud, vertices.size_hint().0)?;
        for point in vertices {
            crate::try_push_vec(&mut vertex_cloud, [point.x, point.y])?;
        }
        Ok(vertex_cloud)
    }

    fn segment_index(&self) -> Result<&SegmentIndex> {
        self.segment_index
            .get_or_try_init(|| SegmentIndex::build(self.segments))
    }

    fn vertex_tree(&self) -> &BulkRTree<[f64; 2]> {
        self.vertex_tree
            .get_or_init(|| BulkRTree::bulk_load_with_params(self.vertex_cloud.clone()))
    }

    pub(in crate::geometry) fn removal_is_safe(
        &self,
        prev: XY,
        vertex: XY,
        next: XY,
        chain_segments: &[Segment],
    ) -> Result<bool> {
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
            return Ok(true);
        }
        // The contact guard and the swept-region guard below are separate:
        // the first preserves the embedding against linework contacts, while
        // the second preserves the ownership of vertices inside the region
        // between the old two-edge path and this shortcut.
        let shortcut = Segment {
            start: prev,
            end: next,
        };
        let contact = self
            .segment_index()?
            .intersecting_candidates(shortcut)
            .any(|entry| {
                let chain_segment = if chain_segments.is_empty() {
                    // The public scalar simplifier has no shared-chain
                    // provenance. Preserve its established local-endpoint
                    // exemption; coverage callers always provide the exact
                    // source segment view above.
                    [entry.segment.start, entry.segment.end]
                        .into_iter()
                        .any(|point| {
                            same_point(point, prev)
                                || same_point(point, vertex)
                                || same_point(point, next)
                        })
                } else {
                    chain_segments.iter().any(|segment| {
                        undirected_segment_edge_key(*segment)
                            == undirected_segment_edge_key(entry.segment)
                    })
                };
                if chain_segment {
                    return false;
                }
                match segment_contact_exact(shortcut, entry.segment) {
                    crate::geometry::Contact::Disjoint => false,
                    crate::geometry::Contact::Cross => true,
                    crate::geometry::Contact::Touch { point } => {
                        !same_point(point, prev) && !same_point(point, next)
                    },
                    crate::geometry::Contact::Collinear { span } => {
                        !same_point(span.start, span.end)
                            || (!same_point(span.start, prev) && !same_point(span.start, next))
                    },
                }
            });
        if contact {
            return Ok(false);
        }
        // No foreign vertex strictly inside the swept triangle. On the
        // original valid linework, a segment entering this region would have
        // to cross one of its two original sides (already an invalid source
        // contact) or have an endpoint in the region. Thus these two guards
        // together certify the replacement without a dynamic spatial graph.
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
        Ok(!inside)
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

#[derive(Clone, Copy)]
struct SourceAtom {
    span: u32,
    reversed: bool,
}

fn source_ordinals_for_ring(
    ring: &CoordSeq,
    source_segments: &[Segment],
    cursor: &mut usize,
) -> Result<Vec<u32>> {
    let mut ordinals = Vec::new();
    for segment in line_segments(ring).filter(|segment| !same_point(segment.start, segment.end)) {
        let ordinal = (*cursor..source_segments.len()).find(|&index| {
            let source = source_segments[index];
            same_point(source.start, segment.start) && same_point(source.end, segment.end)
        });
        let Some(ordinal) = ordinal else {
            return Err(GeometryErrorKind::Invalid(
                "coverage source ring is absent from the exact inventory".into(),
            )
            .into());
        };
        *cursor = ordinal + 1;
        crate::try_push_vec(&mut ordinals, ordinal as u32)?;
    }
    Ok(ordinals)
}

fn source_run_nodes(partition: &ExactPartition<'_>, run: &[SourceAtom]) -> Result<Vec<NodeId>> {
    let Some(&first) = run.first() else {
        return Ok(Vec::new());
    };
    let first_span = partition.spans[first.span as usize];
    let first_node = if first.reversed {
        first_span.end
    } else {
        first_span.start
    };
    let mut nodes = Vec::new();
    crate::try_reserve_vec(&mut nodes, run.len() + 1)?;
    nodes.push(first_node);
    let mut current = first_node;
    for &atom in run {
        let span = partition.spans[atom.span as usize];
        let (start, end) = if atom.reversed {
            (span.end, span.start)
        } else {
            (span.start, span.end)
        };
        if start != current {
            return Err(GeometryErrorKind::Invalid(
                "coverage source atoms are not contiguous in ring order".into(),
            )
            .into());
        }
        nodes.push(end);
        current = end;
    }
    Ok(nodes)
}

fn chain_run_indices(
    chain: &Chain,
    run: &[SourceAtom],
    run_nodes: &[NodeId],
) -> Result<Option<Vec<usize>>> {
    let count = chain.spans.len();
    if run.is_empty() || run.len() > count {
        return Ok(None);
    }
    let cyclic = chain.nodes.first() == chain.nodes.last();
    let matches = |start: usize, forward: bool| -> Result<Option<Vec<usize>>> {
        for index in 0..run.len() {
            let (span_index, node_index) = if cyclic {
                if forward {
                    ((start + index) % count, (start + index) % count)
                } else {
                    (
                        (start + count - 1 - index) % count,
                        (start + count - index) % count,
                    )
                }
            } else if forward {
                (start + index, start + index)
            } else {
                (start - 1 - index, start - index)
            };
            if chain.spans[span_index] != run[index].span
                || chain.nodes[node_index] != run_nodes[index]
            {
                return Ok(None);
            }
        }
        let mut indices = Vec::new();
        crate::try_reserve_vec(&mut indices, run.len() + 1)?;
        for index in 0..=run.len() {
            let node_index = if cyclic {
                if forward {
                    (start + index) % count
                } else {
                    (start + count - index) % count
                }
            } else if forward {
                start + index
            } else {
                start - index
            };
            indices.push(node_index);
        }
        Ok(Some(indices))
    };

    if cyclic {
        for start in 0..count {
            if let Some(indices) = matches(start, true)? {
                return Ok(Some(indices));
            }
            if let Some(indices) = matches(start, false)? {
                return Ok(Some(indices));
            }
        }
    } else {
        for start in 0..count.saturating_sub(run.len() - 1) {
            if let Some(indices) = matches(start, true)? {
                return Ok(Some(indices));
            }
        }
        for start in run.len()..=count {
            if let Some(indices) = matches(start, false)? {
                return Ok(Some(indices));
            }
        }
    }
    Ok(None)
}

fn replacement_for_chain_run(
    partition: &ExactPartition<'_>,
    chain: &Chain,
    simplified: &SimplifiedChain,
    run: &[SourceAtom],
) -> Result<Vec<XY>> {
    let run_nodes = source_run_nodes(partition, run)?;
    let indices = chain_run_indices(chain, run, &run_nodes)?.ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage source run is absent from its exact chain".into())
    })?;
    let mut replacement = Vec::new();
    crate::try_reserve_vec(&mut replacement, indices.len())?;
    for (position, &index) in indices.iter().enumerate() {
        if position == indices.len() - 1 || simplified.kept.contains(&index) {
            replacement.push(chain.points[index]);
        }
    }
    Ok(replacement)
}

/// Simplify a valid polygonal coverage's boundaries, preserving topology.
///
/// Shared interfaces are derived from exact partition incidence, simplified
/// once, and replayed to every source owner. `simplify_boundary` controls
/// whether exterior (unshared) chains simplify too.
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
    let prepared = prepared.rows;
    if same_topological_coordinate(tolerance, 0.0) {
        return Ok(prepared.iter().map(|row| row.shape.clone()).collect());
    }

    let mut all_segments = Vec::new();
    for row in &prepared {
        crate::try_reserve_vec(&mut all_segments, row.segments.len())?;
        all_segments.extend(row.segments.iter().copied());
    }
    let source_refs: Vec<&Shape> = prepared.iter().map(|row| row.shape).collect();
    let inventory = ExactSourceInventory::from_shapes(&source_refs)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_simplify", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let views = partition.coverage_chain_views(inventory.source_operands())?;
    let source_spans = partition.source_span_views(inventory.source_segments())?;
    let mut chains = Vec::new();
    crate::try_reserve_vec(&mut chains, views.chains.len())?;
    for view in views.chains {
        chains.push(exact_chain(
            &partition,
            view,
            &source_spans,
            inventory.source_segments(),
        )?);
    }
    let guard = TopologyGuard::new(
        &all_segments,
        TopologyGuard::try_vertex_cloud(
            all_segments
                .iter()
                .flat_map(|segment| [segment.start, segment.end]),
        )?,
    );
    let simplified: Vec<SimplifiedChain> = chains
        .iter()
        .map(|chain| {
            if !simplify_boundary && !chain.shared {
                return Ok(SimplifiedChain {
                    kept: (0..chain.points.len()).collect(),
                });
            }
            let cyclic = chain.points.len() > 1
                && same_point(chain.points[0], *chain.points.last().expect("non-empty"));
            let kept = simplify_chain_kept(
                chain.points.len(),
                |index| chain.points[index],
                cyclic,
                tolerance,
                method,
                &guard,
                &chain.source_segments,
            )?;
            Ok(SimplifiedChain { kept })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut source_cursor = 0;
    prepared
        .iter()
        .map(|row| {
            rebuild_row(
                row.shape,
                &mut source_cursor,
                inventory.source_segments(),
                &source_spans,
                &views.chain_of_span,
                &chains,
                &simplified,
                &partition,
            )
        })
        .collect()
}

/// Rebuild one row's polygons by splicing simplified chains into each ring.
fn rebuild_row(
    shape: &Shape,
    source_cursor: &mut usize,
    source_segments: &[Segment],
    source_spans: &[Vec<(u32, bool)>],
    chain_of_span: &[u32],
    chains: &[Chain],
    simplified: &[SimplifiedChain],
    partition: &ExactPartition<'_>,
) -> Result<Shape> {
    let mut rebuild_ring = |ring: &CoordSeq| -> Result<Vec<Point>> {
        let original: Vec<Point> = ring.iter_coords().collect();
        let source_ordinals = source_ordinals_for_ring(ring, source_segments, source_cursor)?;
        if original.len() < 4 {
            return Ok(original);
        }
        // Match collect_polygon_segments: elide stutter vertices so chain
        // lookups never probe zero-length edges absent from the exact partition.
        let mut open: Vec<Point> = original[..original.len() - 1].to_vec();
        dedup_consecutive_points(&mut open);
        if open.len() < 3 {
            return Ok(original);
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
                .unwrap_or_else(|| Point::new_raw_xy(point.x, point.y))
        };
        let mut atoms = Vec::new();
        for &ordinal in &source_ordinals {
            let source = source_spans.get(ordinal as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage source ordinal is out of range".into())
            })?;
            if source.is_empty() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage source segment has no exact atom".into(),
                )
                .into());
            }
            for &(span, reversed) in source {
                crate::try_push_vec(&mut atoms, SourceAtom { span, reversed })?;
            }
        }
        let chain_at = |index: usize| chain_of_span[atoms[index].span as usize] as usize;
        let count = atoms.len();
        let start = (0..count)
            .find(|&index| chain_at((index + count - 1) % count) != chain_at(index))
            .unwrap_or(0);
        let mut out: Vec<Point> = Vec::new();
        let mut consumed = 0;
        while consumed < count {
            let here = (start + consumed) % count;
            let id = chain_at(here);
            let mut len = 1;
            while len < count && chain_at((start + consumed + len) % count) == id {
                len += 1;
            }
            let mut run = Vec::new();
            crate::try_reserve_vec(&mut run, len)?;
            for offset in 0..len {
                run.push(atoms[(start + consumed + offset) % count]);
            }
            let replacement =
                replacement_for_chain_run(partition, &chains[id], &simplified[id], &run)?;
            for &point in replacement.iter().take(replacement.len().saturating_sub(1)) {
                out.push(source_point(point));
            }
            consumed += len;
        }
        if let Some(&first) = out.first() {
            out.push(first);
        }
        Ok(out)
    };
    match shape {
        Shape::Polygon(polygon) => Ok(Shape::Polygon(rebuild_polygon(polygon, &mut rebuild_ring)?)),
        Shape::MultiPolygon(polygons) => Shape::multi_polygon(
            polygons.axes(),
            polygons
                .iter()
                .map(|polygon| rebuild_polygon(polygon, &mut rebuild_ring))
                .collect::<Result<Vec<_>>>()?,
        ),
        other => Ok(other.clone()),
    }
}

fn rebuild_polygon(
    polygon: &Polygon,
    rebuild_ring: &mut impl FnMut(&CoordSeq) -> Result<Vec<Point>>,
) -> Result<Polygon> {
    Ok(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&rebuild_ring(
            polygon.shell.coords(),
        )?)?)?,
        polygon
            .holes
            .iter()
            .map(|hole| {
                Ring::try_from_closed(CoordSeq::try_from_points(&rebuild_ring(hole.coords())?)?)
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

#[cfg(test)]
mod guarded_scratch_tests {
    use super::{TopologyGuard, simplify_chain_guarded};
    use crate::geometry::{Segment, XY};

    fn dummy_guard() -> TopologyGuard<'static> {
        TopologyGuard::new(&[], Vec::new())
    }

    fn collinear_x(index: usize) -> XY {
        XY::new(index as f64, 0.0)
    }

    #[test]
    fn short_chain_keeps_both_endpoints() {
        let guard = dummy_guard();
        let kept =
            simplify_chain_guarded(2, collinear_x, false, &|_, _, _| Ok(None), &guard).unwrap();
        assert_eq!(kept, [0, 1]);
    }

    #[test]
    fn declined_candidates_keep_every_vertex() {
        let guard = dummy_guard();
        let never = |_: XY, _: XY, _: XY| Ok(None);
        let kept = simplify_chain_guarded(6, collinear_x, false, &never, &guard).unwrap();
        assert_eq!(kept, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn collinear_removal_keeps_only_endpoints() {
        let guard = dummy_guard();
        let drop_all = |_: XY, _: XY, _: XY| Ok(Some(0.0));
        let kept = simplify_chain_guarded(8, collinear_x, false, &drop_all, &guard).unwrap();
        assert_eq!(kept, vec![0, 7]);
    }

    #[test]
    fn collinear_removal_keeps_endpoints_in_order() {
        let guard = dummy_guard();
        let drop_all = |_: XY, _: XY, _: XY| Ok(Some(0.0));
        let kept = simplify_chain_guarded(5, collinear_x, false, &drop_all, &guard).unwrap();
        assert_eq!(kept, vec![0, 4]);
        let cyclic = simplify_chain_guarded(6, collinear_x, true, &drop_all, &guard).unwrap();
        assert_eq!(cyclic.first().copied(), Some(0));
        assert_eq!(cyclic.last().copied(), Some(5));
        assert_eq!(cyclic.len(), 4);
    }

    #[test]
    fn coverage_contact_guard_rejects_a_foreign_collinear_collapse() {
        let previous = XY::new(0.0, 0.0);
        let vertex = XY::new(1.0, 1.0);
        let next = XY::new(2.0, 0.0);
        let chain_segments = [
            Segment {
                start: previous,
                end: vertex,
            },
            Segment {
                start: vertex,
                end: next,
            },
        ];
        let segments = [chain_segments[0], chain_segments[1], Segment {
            start: previous,
            end: next,
        }];
        let guard = TopologyGuard::new(&segments, Vec::new());
        assert!(
            !guard
                .removal_is_safe(previous, vertex, next, &chain_segments)
                .unwrap()
        );
    }
}
