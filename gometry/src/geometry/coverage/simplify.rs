use std::collections::BinaryHeap;
use std::num::NonZeroUsize;

use crate::error::Result;
use crate::geometry::coverage::{AABB, HashMap, Reverse, valid_coverage_rows};
use crate::geometry::overlay::{ExactChainView, ExactPartition, ExactSourceInventory, NodeId};
use crate::geometry::segment_index::ChordIndex;
use crate::geometry::{
    BulkRTree, CoordSeq, Coordinates as _, GeometryErrorKind, Orientation, Point, PointKey,
    Polygon, Ring, Segment, Shape, SimplifyMethod, XY, dedup_consecutive_points, line_segments,
    orientation, point_segment_distance, same_point, same_topological_coordinate,
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
                &xy,
                cyclic,
                &|prev, vertex, next| {
                    let area = triangle_area(xy(prev), xy(vertex), xy(next));
                    Ok((area < area_tolerance).then_some(area))
                },
                guard,
                chain_segments,
            )
        },
        SimplifyMethod::Dp => {
            simplify_chain_guarded_with_segments(
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
        if !guard.removal_is_safe(xy(before), xy(index), xy(after), chain_segments) {
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
    segment_index: std::cell::OnceCell<ChordIndex>,
    vertex_cloud: std::cell::Cell<Vec<[f64; 2]>>,
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
            vertex_cloud: std::cell::Cell::new(vertex_cloud),
            vertex_tree: std::cell::OnceCell::new(),
        }
    }

    fn segment_index(&self) -> &ChordIndex {
        self.segment_index
            .get_or_init(|| ChordIndex::build(self.segments))
    }

    fn vertex_tree(&self) -> &BulkRTree<[f64; 2]> {
        self.vertex_tree
            .get_or_init(|| BulkRTree::bulk_load_with_params(self.vertex_cloud.take()))
    }

    pub(in crate::geometry) fn removal_is_safe(
        &self,
        prev: XY,
        vertex: XY,
        next: XY,
        chain_segments: &[Segment],
    ) -> bool {
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
        // The contact guard and the swept-region guard below are separate:
        // the first preserves the embedding against linework contacts, while
        // the second preserves the ownership of vertices inside the region
        // between the old two-edge path and this shortcut.
        let shortcut = Segment {
            start: prev,
            end: next,
        };
        let no_contact = self.segment_index().all_candidates(shortcut, |entry| {
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
                return true;
            }
            match segment_contact_exact(shortcut, entry.segment) {
                crate::geometry::Contact::Disjoint => true,
                crate::geometry::Contact::Cross => false,
                crate::geometry::Contact::Touch(endpoint) => {
                    let point = endpoint.point(shortcut, entry.segment);
                    same_point(point, prev) || same_point(point, next)
                },
                contact @ crate::geometry::Contact::Collinear => {
                    let span = contact
                        .shared_span(shortcut, entry.segment)
                        .expect("collinear contact has a shared span");
                    same_point(span.start, span.end)
                        && (same_point(span.start, prev) || same_point(span.start, next))
                },
            }
        });
        if !no_contact {
            return false;
        }
        // No foreign vertex inside or on the swept triangle. On the
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

#[derive(Clone, Copy)]
struct SourceAtom {
    span: u32,
    reversed: bool,
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "NonZeroUsize proves the cyclic remainder divisor cannot be zero"
)]
const fn cyclic_index(index: usize, count: NonZeroUsize) -> usize {
    index.wrapping_rem(count.get())
}

fn source_ordinals_for_ring(
    ring: &CoordSeq,
    source_segments: &[Segment],
    cursor: &mut usize,
) -> Result<Vec<u32>> {
    let mut ordinals = Vec::with_capacity(ring.len().saturating_sub(1));
    for segment in line_segments(ring).filter(|segment| !same_point(segment.start, segment.end)) {
        let ordinal = (*cursor..source_segments.len()).find(|&index| {
            let source = *source_segments
                .get(index)
                .expect("source ordinal search stays within the inventory");
            same_point(source.start, segment.start) && same_point(source.end, segment.end)
        });
        let Some(ordinal) = ordinal else {
            return Err(GeometryErrorKind::Invalid(
                "coverage source ring is absent from the exact inventory".into(),
            )
            .into());
        };
        *cursor = ordinal.wrapping_add(1);
        ordinals.push(ordinal as u32);
    }
    Ok(ordinals)
}

fn source_run_nodes(partition: &ExactPartition<'_>, run: &[SourceAtom]) -> Result<Vec<NodeId>> {
    let Some(&first) = run.first() else {
        return Ok(Vec::new());
    };
    let first_span = *partition
        .spans
        .get(first.span as usize)
        .expect("coverage source span is within the exact partition");
    let first_node = if first.reversed {
        first_span.end
    } else {
        first_span.start
    };
    let mut nodes = Vec::with_capacity(run.len().wrapping_add(1));
    nodes.push(first_node);
    let mut current = first_node;
    for &atom in run {
        let span = *partition
            .spans
            .get(atom.span as usize)
            .expect("coverage source span is within the exact partition");
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
) -> Option<Vec<usize>> {
    let count = chain.spans.len();
    let count_nonzero = NonZeroUsize::new(count)?;
    if run.is_empty() || run.len() > count {
        return None;
    }
    let cyclic = chain.nodes.first() == chain.nodes.last();
    let matches = |start: usize, forward: bool| {
        for (index, atom) in run.iter().enumerate() {
            let (span_index, node_index) = if cyclic {
                if forward {
                    let index = cyclic_index(start.wrapping_add(index), count_nonzero);
                    (index, index)
                } else {
                    (
                        cyclic_index(
                            start
                                .wrapping_add(count)
                                .wrapping_sub(1)
                                .wrapping_sub(index),
                            count_nonzero,
                        ),
                        cyclic_index(start.wrapping_add(count).wrapping_sub(index), count_nonzero),
                    )
                }
            } else if forward {
                let index = start.wrapping_add(index);
                (index, index)
            } else {
                (
                    start.wrapping_sub(1).wrapping_sub(index),
                    start.wrapping_sub(index),
                )
            };
            if *chain
                .spans
                .get(span_index)
                .expect("matched span index is within the exact chain")
                != atom.span
                || *chain
                    .nodes
                    .get(node_index)
                    .expect("matched node index is within the exact chain")
                    != *run_nodes
                        .get(index)
                        .expect("source run has one node for every span")
            {
                return None;
            }
        }
        let mut indices = Vec::with_capacity(run.len().wrapping_add(1));
        for index in 0..=run.len() {
            let node_index = if cyclic {
                if forward {
                    cyclic_index(start.wrapping_add(index), count_nonzero)
                } else {
                    cyclic_index(start.wrapping_add(count).wrapping_sub(index), count_nonzero)
                }
            } else if forward {
                start.wrapping_add(index)
            } else {
                start.wrapping_sub(index)
            };
            indices.push(node_index);
        }
        Some(indices)
    };

    if cyclic {
        for start in 0..count {
            if let Some(indices) = matches(start, true) {
                return Some(indices);
            }
            if let Some(indices) = matches(start, false) {
                return Some(indices);
            }
        }
    } else {
        for start in 0..count.wrapping_sub(run.len().wrapping_sub(1)) {
            if let Some(indices) = matches(start, true) {
                return Some(indices);
            }
        }
        for start in run.len()..=count {
            if let Some(indices) = matches(start, false) {
                return Some(indices);
            }
        }
    }
    None
}

fn replacement_for_chain_run(
    partition: &ExactPartition<'_>,
    chain: &Chain,
    simplified: &SimplifiedChain,
    run: &[SourceAtom],
    capture_edge_roles: bool,
) -> Result<(Vec<XY>, Option<Vec<bool>>)> {
    let run_nodes = source_run_nodes(partition, run)?;
    let indices = chain_run_indices(chain, run, &run_nodes).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage source run is absent from its exact chain".into())
    })?;
    let mut replacement = Vec::with_capacity(indices.len());
    let mut retained_indices = capture_edge_roles.then(|| Vec::with_capacity(indices.len()));
    for (position, &index) in indices.iter().enumerate() {
        if position == indices.len().wrapping_sub(1) || simplified.kept.contains(&index) {
            replacement.push(
                *chain
                    .points
                    .get(index)
                    .expect("simplified chain index is within its point sequence"),
            );
            if let Some(retained_indices) = &mut retained_indices {
                retained_indices.push(index);
            }
        }
    }
    let edge_roles = retained_indices
        .map(|indices| {
            let span_count = NonZeroUsize::new(chain.spans.len()).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage chain has no source spans".into())
            })?;
            let cyclic = chain.nodes.first() == chain.nodes.last();
            Ok::<_, crate::error::Error>(
                indices
                    .array_windows::<2>()
                    .map(|[left, right]| {
                        let adjacent = if cyclic {
                            cyclic_index(left.wrapping_add(1), span_count) == *right
                                || cyclic_index(right.wrapping_add(1), span_count) == *left
                        } else {
                            left.abs_diff(*right) == 1
                        };
                        !adjacent
                    })
                    .collect(),
            )
        })
        .transpose()?;
    Ok((replacement, edge_roles))
}

/// Simplify a valid polygonal coverage's boundaries, preserving topology.
///
/// Shared interfaces are derived from exact partition incidence, simplified
/// once, and replayed to every source owner. `simplify_boundary` controls
/// whether exterior (unshared) chains simplify too.
pub(crate) fn coverage_simplify_with_frame<S: std::borrow::Borrow<Shape>>(
    rows: &[S],
    tolerance: f64,
    method: SimplifyMethod,
    simplify_boundary: bool,
    geographic_frame: Option<crate::geographic::GeographicFrame>,
) -> Result<Vec<Shape>> {
    if !(tolerance.is_finite() && tolerance >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("tolerance", tolerance).into());
    }
    let prepared = valid_coverage_rows(rows, "coverage_simplify")?;
    let prepared = prepared.rows;
    if same_topological_coordinate(tolerance, 0.0) {
        return Ok(prepared.iter().map(|row| row.shape.clone()).collect());
    }

    let mut all_segments = Vec::with_capacity(prepared.iter().map(|row| row.segments.len()).sum());
    for row in &prepared {
        all_segments.extend_from_slice(&row.segments);
    }
    let source_refs: Vec<&Shape> = prepared.iter().map(|row| row.shape).collect();
    let inventory = ExactSourceInventory::from_shapes(&source_refs)?;
    let mut budget = crate::geometry::ExpansionBudget::new("coverage_simplify", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let views = partition.coverage_chain_views(inventory.source_operands())?;
    let source_spans = partition.source_span_views(inventory.source_segments())?;
    let mut chain_sources = vec![Vec::new(); views.chains.len()];
    for (source, spans) in source_spans.iter().enumerate() {
        for &(span, _) in spans {
            let chain = *views
                .chain_of_span
                .get(span as usize)
                .expect("source span belongs to the coverage chain inventory");
            let sources = chain_sources
                .get_mut(chain as usize)
                .expect("coverage span has a chain owner");
            if sources.last() != Some(&source) {
                sources.push(source);
            }
        }
    }
    let mut chains = Vec::with_capacity(views.chains.len());
    for (view, sources) in views.chains.into_iter().zip(&chain_sources) {
        chains.push(exact_chain(
            &partition,
            view,
            sources,
            inventory.source_segments(),
        ));
    }
    let guard = TopologyGuard::new(
        &all_segments,
        all_segments
            .iter()
            .flat_map(|segment| {
                [[segment.start.x, segment.start.y], [
                    segment.end.x,
                    segment.end.y,
                ]]
            })
            .collect(),
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
                && chain
                    .points
                    .first()
                    .zip(chain.points.last())
                    .is_some_and(|(&first, &last)| same_point(first, last));
            let kept = simplify_chain_kept(
                chain.points.len(),
                |index| {
                    *chain
                        .points
                        .get(index)
                        .expect("simplification index is within its chain")
                },
                cyclic,
                tolerance,
                method,
                &guard,
                &chain.source_segments,
            )?;
            Ok(SimplifiedChain { kept })
        })
        .collect::<Result<Vec<_>>>()?;
    drop(guard);
    drop(all_segments);

    let mut source_cursor = 0;
    let capture_edge_roles = geographic_frame.is_some();
    let mut computed_edge_budget =
        crate::geometry::ExpansionBudget::new("coverage_simplify", "computed edges");
    prepared
        .iter()
        .map(|row| {
            let (shape, edge_roles) = rebuild_row(
                row.shape,
                &mut source_cursor,
                inventory.source_segments(),
                &source_spans,
                &views.chain_of_span,
                &chains,
                &simplified,
                &partition,
                capture_edge_roles,
            )?;
            match (geographic_frame, edge_roles) {
                (Some(frame), Some(edge_roles)) => {
                    publish_coverage_edge_roles(shape, edge_roles, frame, &mut computed_edge_budget)
                },
                (None, None) => Ok(shape),
                _ => Err(GeometryErrorKind::Invalid(
                    "coverage edge roles do not match the publication frame".into(),
                )
                .into()),
            }
        })
        .collect()
}

fn publish_coverage_edge_roles(
    shape: Shape,
    edge_roles: Vec<Vec<Vec<bool>>>,
    frame: crate::geographic::GeographicFrame,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    match shape {
        Shape::Polygon(polygon) => {
            let [roles] = edge_roles.as_slice() else {
                return Err(GeometryErrorKind::Invalid(
                    "coverage polygon roles do not match the rebuilt polygon".into(),
                )
                .into());
            };
            Ok(Shape::Polygon(
                crate::geometry::publish_polygon_with_edge_roles(&polygon, roles, frame, budget)?,
            ))
        },
        Shape::MultiPolygon(polygons) => {
            if polygons.len() != edge_roles.len() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage multipolygon roles do not match its polygon parts".into(),
                )
                .into());
            }
            let mut edge_roles = edge_roles.into_iter();
            let output = polygons.try_map_values(polygons.axes(), |polygon| {
                let roles = edge_roles.next().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage multipolygon is missing a polygon role row".into(),
                    )
                })?;
                crate::geometry::publish_polygon_with_edge_roles(polygon, &roles, frame, budget)
            })?;
            if edge_roles.next().is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "coverage multipolygon has unused polygon role rows".into(),
                )
                .into());
            }
            Ok(Shape::MultiPolygon(output))
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => {
            if edge_roles.is_empty() {
                Ok(shape)
            } else {
                Err(GeometryErrorKind::Invalid(
                    "coverage edge roles do not match the rebuilt geometry".into(),
                )
                .into())
            }
        },
    }
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
    capture_edge_roles: bool,
) -> Result<(Shape, Option<crate::geometry::PolygonPartEdgeRoles>)> {
    let mut rebuild_ring = |ring: &CoordSeq| -> Result<(Vec<Point>, Option<Vec<bool>>)> {
        let original: Vec<Point> = ring.iter_coords().collect();
        let source_ordinals = source_ordinals_for_ring(ring, source_segments, source_cursor)?;
        if original.len() < 4 {
            let roles = capture_edge_roles.then(|| vec![false; original.len().saturating_sub(1)]);
            return Ok((original, roles));
        }
        // Match collect_polygon_segments: elide stutter vertices so chain
        // lookups never probe zero-length edges absent from the exact partition.
        let (_, open_ring) = original
            .split_last()
            .expect("ring with at least four coordinates is non-empty");
        let mut open = open_ring.to_vec();
        dedup_consecutive_points(&mut open);
        if open.len() < 3 {
            let roles = capture_edge_roles.then(|| vec![false; original.len().saturating_sub(1)]);
            return Ok((original, roles));
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
            atoms.extend(
                source
                    .iter()
                    .map(|&(span, reversed)| SourceAtom { span, reversed }),
            );
        }
        let chain_at = |index: usize| {
            let atom = atoms
                .get(index)
                .expect("cyclic coverage atom index is within the ring");
            *chain_of_span
                .get(atom.span as usize)
                .expect("coverage span has an assigned chain") as usize
        };
        let count = atoms.len();
        let mut out: Vec<Point> = Vec::with_capacity(count.wrapping_add(1));
        let mut edge_roles = capture_edge_roles.then(|| Vec::with_capacity(count));
        let Some(atom_count) = NonZeroUsize::new(count) else {
            return Ok((out, edge_roles));
        };
        let start = (0..count)
            .find(|&index| {
                chain_at(cyclic_index(
                    index.wrapping_add(count).wrapping_sub(1),
                    atom_count,
                )) != chain_at(index)
            })
            .unwrap_or(0);
        let mut consumed = 0;
        while consumed < count {
            let here = cyclic_index(start.wrapping_add(consumed), atom_count);
            let id = chain_at(here);
            let mut len = 1;
            while len < count
                && chain_at(cyclic_index(
                    start.wrapping_add(consumed).wrapping_add(len),
                    atom_count,
                )) == id
            {
                len = len.wrapping_add(1);
            }
            let mut run = Vec::with_capacity(len);
            for offset in 0..len {
                run.push(
                    *atoms
                        .get(cyclic_index(
                            start.wrapping_add(consumed).wrapping_add(offset),
                            atom_count,
                        ))
                        .expect("coverage atom index is within the ring"),
                );
            }
            let (replacement, run_edge_roles) = replacement_for_chain_run(
                partition,
                chains
                    .get(id)
                    .expect("coverage span chain is in the chain table"),
                simplified
                    .get(id)
                    .expect("coverage span chain has a simplification result"),
                &run,
                capture_edge_roles,
            )?;
            for &point in replacement.iter().take(replacement.len().saturating_sub(1)) {
                out.push(source_point(point));
            }
            match (&mut edge_roles, run_edge_roles) {
                (Some(edge_roles), Some(run_edge_roles)) => edge_roles.extend(run_edge_roles),
                (None, None) => {},
                _ => {
                    return Err(GeometryErrorKind::Invalid(
                        "coverage edge roles do not match the rebuild mode".into(),
                    )
                    .into());
                },
            }
            consumed = consumed.wrapping_add(len);
        }
        if let Some(&first) = out.first() {
            out.push(first);
        }
        if edge_roles
            .as_ref()
            .is_some_and(|roles| roles.len() != out.len().saturating_sub(1))
        {
            return Err(GeometryErrorKind::Invalid(
                "coverage edge roles do not match a rebuilt ring".into(),
            )
            .into());
        }
        Ok((out, edge_roles))
    };
    match shape {
        Shape::Polygon(polygon) => {
            let (polygon, roles) = rebuild_polygon(polygon, &mut rebuild_ring)?;
            Ok((Shape::Polygon(polygon), roles.map(|roles| vec![roles])))
        },
        Shape::MultiPolygon(polygons) => {
            let mut rebuilt = Vec::with_capacity(polygons.len());
            let mut polygon_roles = capture_edge_roles.then(|| Vec::with_capacity(polygons.len()));
            for polygon in polygons {
                let (polygon, roles) = rebuild_polygon(polygon, &mut rebuild_ring)?;
                rebuilt.push(polygon);
                match (&mut polygon_roles, roles) {
                    (Some(polygon_roles), Some(roles)) => polygon_roles.push(roles),
                    (None, None) => {},
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "coverage edge roles do not match the rebuild mode".into(),
                        )
                        .into());
                    },
                }
            }
            Ok((
                Shape::multi_polygon(polygons.axes(), rebuilt),
                polygon_roles,
            ))
        },
        Shape::Empty(..)
        | Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_) => Ok((shape.clone(), capture_edge_roles.then(Vec::new))),
    }
}

fn rebuild_polygon(
    polygon: &Polygon,
    rebuild_ring: &mut impl FnMut(&CoordSeq) -> Result<(Vec<Point>, Option<Vec<bool>>)>,
) -> Result<(Polygon, Option<Vec<Vec<bool>>>)> {
    let (shell, shell_roles) = rebuild_ring(polygon.shell.coords())?;
    let capture_edge_roles = shell_roles.is_some();
    let shell = Ring::try_from_closed(CoordSeq::try_from_points(&shell)?)?;
    let mut holes = Vec::with_capacity(polygon.holes.len());
    let mut roles = Vec::with_capacity(polygon.holes.len().wrapping_add(1));
    if let Some(shell_roles) = shell_roles {
        roles.push(shell_roles);
    }
    for hole in polygon.holes.iter() {
        let (points, hole_roles) = rebuild_ring(hole.coords())?;
        holes.push(Ring::try_from_closed(CoordSeq::try_from_points(&points)?)?);
        match (capture_edge_roles, hole_roles) {
            (true, Some(hole_roles)) => roles.push(hole_roles),
            (false, None) => {},
            _ => {
                return Err(GeometryErrorKind::Invalid(
                    "coverage ring roles do not match the polygon rebuild mode".into(),
                )
                .into());
            },
        }
    }
    let edge_roles = capture_edge_roles.then_some(roles);
    Ok((Polygon::new(shell, holes), edge_roles))
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
    fn vertex_index_is_lazy_and_adds_no_staging_allocation() {
        use crate::geometry::BulkRTree;
        use crate::test_support::{allocation_count, assert_no_allocations};

        for count in [1, 32, 4096] {
            let cloud: Vec<_> = (0..count).map(|index| [f64::from(index), 1.0]).collect();
            let direct_cloud = cloud.clone();
            let guard = assert_no_allocations(|| TopologyGuard::new(&[], cloud));
            assert_no_allocations(|| {
                assert!(guard.removal_is_safe(collinear_x(0), collinear_x(1), collinear_x(2), &[]));
            });
            let (direct, direct_allocations) =
                allocation_count(|| BulkRTree::bulk_load_with_params(direct_cloud));
            let (indexed, lazy_allocations) = allocation_count(|| guard.vertex_tree());
            assert_eq!(indexed.size(), direct.size());
            assert_eq!(lazy_allocations, direct_allocations, "vertex count {count}");
            assert_no_allocations(|| {
                assert!(std::ptr::eq(indexed, guard.vertex_tree()));
            });
        }
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
        let never = |_: usize, _: usize, _: usize| Ok(None);
        let kept = simplify_chain_guarded(6, collinear_x, false, &never, &guard).unwrap();
        assert_eq!(kept, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn collinear_removal_keeps_only_endpoints() {
        let guard = dummy_guard();
        let drop_all = |_: usize, _: usize, _: usize| Ok(Some(0.0));
        let kept = simplify_chain_guarded(8, collinear_x, false, &drop_all, &guard).unwrap();
        assert_eq!(kept, vec![0, 7]);
    }

    #[test]
    fn collinear_removal_keeps_endpoints_in_order() {
        let guard = dummy_guard();
        let drop_all = |_: usize, _: usize, _: usize| Ok(Some(0.0));
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
        assert!(!guard.removal_is_safe(previous, vertex, next, &chain_segments));
    }
}

#[cfg(test)]
mod computed_edge_publication_tests {
    use super::coverage_simplify_with_frame;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{CoordSeq, Point, Polygon, Ring, Shape, SimplifyMethod};

    #[test]
    fn coverage_simplification_subdivides_a_wide_generated_chord() {
        let source = Shape::Polygon(Polygon::new(
            Ring::try_from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(90.0, 0.0),
                    Point::new_raw_xy(180.0, 0.0),
                    Point::new_raw_xy(270.0, 0.0),
                    Point::new_raw_xy(270.0, 10.0),
                    Point::new_raw_xy(180.0, 10.0),
                    Point::new_raw_xy(90.0, 10.0),
                    Point::new_raw_xy(0.0, 10.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        ));
        let output = coverage_simplify_with_frame(
            std::slice::from_ref(&source),
            0.1,
            SimplifyMethod::Vw,
            true,
            Some(GeographicFrame::crs84()),
        )
        .unwrap();
        let Shape::Polygon(polygon) = output.first().unwrap() else {
            panic!("coverage simplification preserves the polygon row");
        };
        let points = polygon.shell.coords().iter().collect::<Vec<_>>();
        assert!(points.contains(&Point::new_raw_xy(135.0, 0.0)));
        assert!(points.contains(&Point::new_raw_xy(135.0, 10.0)));
        assert!(
            points.iter().all(|point| (-180.0..180.0).contains(&point.x)
                || point.x.to_bits() == 270.0_f64.to_bits())
        );
    }
}
