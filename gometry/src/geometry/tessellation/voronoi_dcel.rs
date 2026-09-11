//! Exact labelled Voronoi subdivision and its sole binary64 projection.

#![allow(
    clippy::assertions_on_result_states,
    clippy::clone_on_copy,
    clippy::collapsible_if,
    clippy::explicit_iter_loop,
    clippy::float_cmp,
    clippy::inconsistent_struct_constructor,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::missing_const_for_fn,
    clippy::needless_range_loop,
    clippy::ptr_arg,
    clippy::redundant_closure_for_method_calls,
    clippy::too_many_lines,
    clippy::unnecessary_semicolon,
    clippy::unwrap_used,
    reason = "the DCEL mirrors the numbered construction and its proof tests keep mutations visibly local"
)]

use std::cmp::Ordering;
use std::collections::VecDeque;

use rstar::AABB;

use crate::error::Result;
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::tessellation::{
    CertifiedDelaunay, CertifiedPrimalEdge, DelaunayComplex, EMPTY, Site, delaunay_complex, exact,
};
use crate::geometry::{
    BulkRTree, CoordSeq, CoordWindow, CsrOffsetColumn, ExpansionBudget, GeometryErrorKind, HashMap,
    HashMapExt as _, LineSeq, Point, PointKey, Polygon, RTreeObject as _, Ring, RingClass,
    RingEdgeMembership, Segment, Shape, Strictness, VoronoiBoundary, XY, coverage_is_valid,
    line_segments, polygon_is_valid, ring_edge_membership,
};
#[cfg(test)]
use crate::geometry::{
    GENERATED_ITEM_LIMIT, tessellation::certified_delaunay, tessellation::shape,
};

macro_rules! dense_id {
    ($name:ident) => {
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        struct $name(u32);
    };
}

dense_id!(SiteId);
dense_id!(VertexId);
dense_id!(EdgeId);
dense_id!(BoundaryFaceId);
dense_id!(EmbeddedVertexId);
dense_id!(ClipSegmentId);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnerTransition {
    Preserve,
    Boundary {
        left: Option<SiteId>,
        right: Option<SiteId>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrimitiveOwner {
    Preserve,
    Boundary {
        left: Option<SiteId>,
        right: Option<SiteId>,
    },
    FrameNearest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Transition {
    frame_delta: i8,
    clip_delta: i32,
    owner: OwnerTransition,
}

impl Transition {
    const fn reversed(self) -> Self {
        Self {
            frame_delta: -self.frame_delta,
            clip_delta: -self.clip_delta,
            owner: match self.owner {
                OwnerTransition::Preserve => OwnerTransition::Preserve,
                OwnerTransition::Boundary { left, right } => OwnerTransition::Boundary {
                    left: right,
                    right: left,
                },
            },
        }
    }
}

struct ScaffoldPrimitive {
    line: exact::ExactLine,
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    frame_delta: i8,
    clip_delta: i32,
    owner: PrimitiveOwner,
    clip_source: Option<ClipSegmentId>,
    frame_side: Option<u8>,
    events: Vec<exact::ExactPoint>,
}

struct ClippedSeparator {
    pair: [SiteId; 2],
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    events: Vec<exact::ExactPoint>,
}

struct NormalizedClip<'a> {
    rings: Vec<Vec<exact::ExactPoint>>,
    fixed: Vec<Vec<XY>>,
    source: Option<&'a Polygon>,
    contacts: Vec<ExactCycleContact>,
}

impl NormalizedClip<'_> {
    /// Extent of the outer fixed ring as `[minx, maxx, miny, maxy]`. The
    /// boundary-tiling certificate and its regressions index the sides in that
    /// order.
    fn frame_bounds(&self) -> [f64; 4] {
        let mut bounds = [
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        for point in &self.fixed[0] {
            bounds[0] = bounds[0].min(point.x);
            bounds[1] = bounds[1].max(point.x);
            bounds[2] = bounds[2].min(point.y);
            bounds[3] = bounds[3].max(point.y);
        }
        bounds
    }
}

#[derive(Clone)]
struct AtomSeed {
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    frame_delta: i8,
    clip_delta: i32,
    owner: PrimitiveOwner,
    separator: Option<[SiteId; 2]>,
    clip_source: Option<ClipSegmentId>,
    frame_side: Option<u8>,
}

struct NodedVertex {
    point: exact::ExactPoint,
    fixed_xy: Option<XY>,
}

struct AtomicEdge {
    endpoints: [VertexId; 2],
    forward: Transition,
    separator: Option<[SiteId; 2]>,
    clip_sources: Box<[ClipSegmentId]>,
    frame_side: Option<u8>,
}

struct NodedAtoms {
    vertices: Vec<NodedVertex>,
    edges: Vec<AtomicEdge>,
}

#[derive(Clone)]
struct ExactPairTouch {
    rings: [usize; 2],
    point: exact::ExactPoint,
    directions: [Vec<exact::ExactPoint>; 2],
}

#[derive(Clone)]
struct ExactCycleContact {
    segments: [usize; 2],
    point: exact::ExactPoint,
}

struct ValidatedExactCycles {
    contacts: Vec<ExactCycleContact>,
}

fn voronoi_error(message: impl Into<String>) -> crate::error::Error {
    GeometryErrorKind::voronoi(message.into())
}

/// The one failure every direct-dual certificate reports: the exact
/// subdivision is correct but no binary64 rounding of it preserves the
/// topology, so the caller must not receive a silently repaired diagram.
fn embedding_failure() -> crate::error::Error {
    voronoi_error("exact Voronoi subdivision has no topology-preserving binary64 embedding")
}

struct IndexedExactEnvelope {
    index: usize,
    envelope: AABB<[f64; 2]>,
}

impl crate::geometry::RTreeObject for IndexedExactEnvelope {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

/// A one-ULP outward expansion of the nearest binary64 projection contains the
/// exact coordinate. Points which cannot produce such an envelope stay in the
/// unindexed fallback candidate set.
fn exact_envelope(points: &[exact::ExactPoint]) -> Option<AABB<[f64; 2]>> {
    let mut minimum = [f64::INFINITY; 2];
    let mut maximum = [f64::NEG_INFINITY; 2];
    for point in points {
        let point = point.round_nearest_even().ok()?;
        let lower = [point.x.next_down(), point.y.next_down()];
        let upper = [point.x.next_up(), point.y.next_up()];
        if !lower.into_iter().chain(upper).all(f64::is_finite) {
            return None;
        }
        for axis in 0..2 {
            minimum[axis] = minimum[axis].min(lower[axis]);
            maximum[axis] = maximum[axis].max(upper[axis]);
        }
    }
    Some(AABB::from_corners(minimum, maximum))
}

fn exact_envelope_candidates(
    tree: &BulkRTree<IndexedExactEnvelope>,
    unindexed: &[usize],
    query: Option<AABB<[f64; 2]>>,
    count: usize,
) -> Vec<bool> {
    let mut candidates = vec![query.is_none(); count];
    if let Some(query) = query {
        for entry in tree.locate_in_envelope_intersecting(query) {
            candidates[entry.index] = true;
        }
        for &index in unindexed {
            candidates[index] = true;
        }
    }
    candidates
}

fn reserve_exact<T>(values: &mut Vec<T>, additional: usize) -> Result<()> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| voronoi_error("could not allocate exact Voronoi subdivision"))
}

fn budgeted_try_push<T>(values: &mut Vec<T>, value: T, budget: &mut ExpansionBudget) -> Result<()> {
    budget.add(1)?;
    if values.len() == values.capacity() {
        values
            .try_reserve(1)
            .map_err(|_| voronoi_error("could not allocate exact Voronoi subdivision"))?;
    }
    values.push(value);
    Ok(())
}

fn exact_cycle_error() -> crate::error::Error {
    voronoi_error("exact Voronoi cycle set is not a valid polygon boundary")
}

fn admit_embedded_clip(polygon: &Polygon) -> Result<()> {
    if polygon_is_valid(polygon)? {
        Ok(())
    } else {
        Err(voronoi_error(
            "embedded Voronoi clip is not a valid polygon",
        ))
    }
}

fn indexed_exact_segment_candidates(
    cycles: &[&[exact::ExactPoint]],
    budget: &mut ExpansionBudget,
) -> Result<Vec<[usize; 2]>> {
    use rstar::primitives::{GeomWithData, Rectangle};

    let mut segments = Vec::new();
    reserve_exact(&mut segments, cycles.iter().map(|cycle| cycle.len()).sum())?;
    for cycle in cycles {
        for edge in 0..cycle.len() {
            segments.push((&cycle[edge], &cycle[(edge + 1) % cycle.len()]));
        }
    }
    let mut indexed = Vec::new();
    reserve_exact(&mut indexed, segments.len())?;
    let mut unroundable = Vec::new();
    for (index, (start, end)) in segments.iter().enumerate() {
        let Ok(start) = start.round_nearest_even() else {
            budgeted_try_push(&mut unroundable, index, budget)?;
            continue;
        };
        let Ok(end) = end.round_nearest_even() else {
            budgeted_try_push(&mut unroundable, index, budget)?;
            continue;
        };
        if ![start.x, start.y, end.x, end.y]
            .iter()
            .all(|value| value.is_finite())
        {
            budgeted_try_push(&mut unroundable, index, budget)?;
            continue;
        }
        indexed.push(GeomWithData::new(
            Rectangle::from_corners([start.x.min(end.x), start.y.min(end.y)], [
                start.x.max(end.x),
                start.y.max(end.y),
            ]),
            index,
        ));
    }
    let tree = BulkRTree::bulk_load_with_params(indexed);
    let mut candidates = Vec::new();
    for item in tree.iter() {
        for other in tree.locate_in_envelope_intersecting(item.envelope()) {
            if item.data < other.data {
                budgeted_try_push(&mut candidates, [item.data, other.data], budget)?;
            }
        }
    }
    for &left in &unroundable {
        for right in 0..segments.len() {
            if left != right {
                budgeted_try_push(&mut candidates, [left.min(right), left.max(right)], budget)?;
            }
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    Ok(candidates)
}

fn validate_exact_cycles(
    cycles: &[&[exact::ExactPoint]],
    shell: usize,
    budget: &mut ExpansionBudget,
) -> Result<ValidatedExactCycles> {
    if cycles.is_empty() || shell >= cycles.len() || cycles.iter().any(|cycle| cycle.len() < 3) {
        return Err(exact_cycle_error());
    }
    let offsets: Vec<_> = cycles
        .iter()
        .scan(0_usize, |offset, cycle| {
            let current = *offset;
            *offset = offset.saturating_add(cycle.len());
            Some(current)
        })
        .collect();
    let candidates = indexed_exact_segment_candidates(cycles, budget)?;
    let mut contacts = Vec::new();
    let mut pair_touches: Vec<ExactPairTouch> = Vec::new();
    for [left_segment, right_segment] in candidates {
        let left_ring = offsets.partition_point(|offset| *offset <= left_segment) - 1;
        let right_ring = offsets.partition_point(|offset| *offset <= right_segment) - 1;
        let left_edge = left_segment - offsets[left_ring];
        let right_edge = right_segment - offsets[right_ring];
        let left = [
            &cycles[left_ring][left_edge],
            &cycles[left_ring][(left_edge + 1) % cycles[left_ring].len()],
        ];
        let adjacent = left_ring == right_ring
            && (right_edge == left_edge + 1
                || (left_edge == 0 && right_edge + 1 == cycles[left_ring].len()));
        let right = [
            &cycles[right_ring][right_edge],
            &cycles[right_ring][(right_edge + 1) % cycles[right_ring].len()],
        ];
        match exact::segment_intersection(left[0], left[1], right[0], right[1]) {
            exact::SegmentIntersection::None => {},
            exact::SegmentIntersection::Overlap { start, end } => {
                let _ = (start, end);
                return Err(exact_cycle_error());
            },
            exact::SegmentIntersection::Point(point) => {
                let left_endpoint = left.iter().position(|end| end.same_position(&point));
                let right_endpoint = right.iter().position(|end| end.same_position(&point));
                if adjacent {
                    if left_endpoint.is_none() || right_endpoint.is_none() {
                        return Err(exact_cycle_error());
                    }
                    continue;
                }
                if left_ring == right_ring || (left_endpoint.is_none() && right_endpoint.is_none())
                {
                    return Err(exact_cycle_error());
                }
                let segments = [
                    offsets[left_ring] + left_edge,
                    offsets[right_ring] + right_edge,
                ];
                budgeted_try_push(
                    &mut contacts,
                    ExactCycleContact {
                        segments,
                        point: point.clone(),
                    },
                    budget,
                )?;
                if let Some(touch) = pair_touches.iter_mut().find(|touch| {
                    touch.rings == [left_ring, right_ring] && touch.point.same_position(&point)
                }) {
                    for (slot, (segment, endpoint)) in
                        [(left, left_endpoint), (right, right_endpoint)]
                            .into_iter()
                            .enumerate()
                    {
                        for direction in
                            segment
                                .into_iter()
                                .enumerate()
                                .filter_map(|(index, direction)| {
                                    endpoint
                                        .is_none_or(|endpoint| index != endpoint)
                                        .then_some(direction)
                                })
                        {
                            if !touch.directions[slot]
                                .iter()
                                .any(|known| known.same_position(direction))
                            {
                                budgeted_try_push(
                                    &mut touch.directions[slot],
                                    direction.clone(),
                                    budget,
                                )?;
                            }
                        }
                    }
                } else {
                    let mut directions = [Vec::new(), Vec::new()];
                    for (slot, (segment, endpoint)) in
                        [(left, left_endpoint), (right, right_endpoint)]
                            .into_iter()
                            .enumerate()
                    {
                        for (index, direction) in segment.into_iter().enumerate() {
                            if endpoint.is_none_or(|endpoint| index != endpoint) {
                                budgeted_try_push(
                                    &mut directions[slot],
                                    direction.clone(),
                                    budget,
                                )?;
                            }
                        }
                    }
                    budget.add(1)?;
                    pair_touches
                        .try_reserve(1)
                        .map_err(|_| exact_cycle_error())?;
                    pair_touches.push(ExactPairTouch {
                        rings: [left_ring, right_ring],
                        point,
                        directions,
                    });
                }
            },
        }
    }
    for touch in &pair_touches {
        let mut rays: Vec<_> = touch.directions[0]
            .iter()
            .map(|target| (0_usize, target))
            .chain(touch.directions[1].iter().map(|target| (1_usize, target)))
            .collect();
        for index in 1..rays.len() {
            let mut cursor = index;
            while cursor > 0
                && exact::angle_ccw_cmp(&touch.point, rays[cursor].1, rays[cursor - 1].1)
                    .map_err(|_| exact_cycle_error())?
                    == Ordering::Less
            {
                rays.swap(cursor, cursor - 1);
                cursor -= 1;
            }
        }
        for pair in rays.windows(2) {
            exact::angle_ccw_cmp(&touch.point, pair[0].1, pair[1].1)
                .map_err(|_| exact_cycle_error())?;
        }
        if rays.len() > 1 {
            exact::angle_ccw_cmp(&touch.point, rays.last().unwrap().1, rays[0].1)
                .map_err(|_| exact_cycle_error())?;
        }
        let changes = (0..rays.len())
            .filter(|&index| rays[index].0 != rays[(index + 1) % rays.len()].0)
            .count();
        if changes != 2 {
            return Err(exact_cycle_error());
        }
    }
    let mut touch_points: Vec<_> = pair_touches
        .iter()
        .map(|touch| touch.point.clone())
        .collect();
    touch_points.sort_by(exact::ExactPoint::compare_lex);
    touch_points.dedup_by(|left, right| left.same_position(right));
    let mut parents: Vec<_> = (0..cycles.len() + touch_points.len()).collect();
    let mut incidences = Vec::new();
    for touch in &pair_touches {
        let point = touch_points
            .iter()
            .position(|point| point.same_position(&touch.point))
            .expect("touch point was interned");
        for ring in touch.rings {
            budgeted_try_push(&mut incidences, (point, ring), budget)?;
        }
    }
    incidences.sort_unstable();
    incidences.dedup();
    for (point, ring) in incidences {
        let left = uf_root(&mut parents, ring);
        let right = uf_root(&mut parents, cycles.len() + point);
        if left == right {
            return Err(exact_cycle_error());
        }
        parents[right] = left;
    }
    for hole in 0..cycles.len() {
        if hole == shell {
            continue;
        }
        let mut saw_inside = false;
        for point in cycles[hole] {
            budget.add(cycles[shell].len())?;
            match exact::point_in_cycle(cycles[shell], point) {
                exact::PointInCycle::Outside => return Err(exact_cycle_error()),
                exact::PointInCycle::Inside => saw_inside = true,
                exact::PointInCycle::Boundary => {},
            }
        }
        if !saw_inside {
            return Err(exact_cycle_error());
        }
        for other in 0..cycles.len() {
            if other == shell || other == hole {
                continue;
            }
            budget.add(cycles[other].len().saturating_mul(cycles[hole].len()))?;
            if cycles[other].iter().any(|point| {
                exact::point_in_cycle(cycles[hole], point) == exact::PointInCycle::Inside
            }) {
                return Err(exact_cycle_error());
            }
        }
    }
    contacts.sort_by(|left, right| {
        left.segments
            .cmp(&right.segments)
            .then_with(|| left.point.compare_lex(&right.point))
    });
    contacts.dedup_by(|left, right| {
        left.segments == right.segments && left.point.same_position(&right.point)
    });
    Ok(ValidatedExactCycles { contacts })
}

fn open_ring(ring: &Ring) -> Result<(Vec<exact::ExactPoint>, Vec<XY>)> {
    let coordinates: Vec<_> = ring.iter().map(|point| point.xy()).collect();
    let mut exact_points: Vec<_> = coordinates[..coordinates.len() - 1]
        .iter()
        .copied()
        .map(exact::ExactPoint::from_xy)
        .collect();
    let mut fixed = coordinates[..coordinates.len() - 1].to_vec();
    let mut index = 1;
    while index < exact_points.len() {
        if exact_points[index].same_position(&exact_points[index - 1]) {
            exact_points.remove(index);
            fixed.remove(index);
        } else {
            index += 1;
        }
    }
    if exact_points.len() < 3 {
        return Err(voronoi_error(
            "exact Voronoi clip ring has fewer than three vertices",
        ));
    }
    Ok((exact_points, fixed))
}

fn reverse_ring(points: &mut Vec<exact::ExactPoint>, fixed: &mut Vec<XY>) {
    points.reverse();
    fixed.reverse();
}

fn rotate_ring(points: &mut Vec<exact::ExactPoint>, fixed: &mut Vec<XY>) {
    let least = (0..points.len())
        .min_by(|&left, &right| {
            for offset in 0..points.len() {
                let order = points[(left + offset) % points.len()]
                    .compare_lex(&points[(right + offset) % points.len()]);
                if order.is_ne() {
                    return order;
                }
            }
            Ordering::Equal
        })
        .expect("nonempty ring");
    points.rotate_left(least);
    fixed.rotate_left(least);
}

fn normalize_clip<'a>(
    sites: &[Site],
    boundary: VoronoiBoundary<'a>,
    budget: &mut ExpansionBudget,
) -> Result<NormalizedClip<'a>> {
    let mut rings = Vec::new();
    let mut fixed = Vec::new();
    let source = match boundary {
        VoronoiBoundary::Polygon(polygon) => {
            if !polygon_is_valid(polygon)? {
                return Err(voronoi_error("Voronoi clip polygon is invalid"));
            }
            for (ring_index, ring) in std::iter::once(&polygon.shell)
                .chain(polygon.holes.iter())
                .enumerate()
            {
                let (mut points, mut coordinates) = open_ring(ring)?;
                let orientation = exact::cycle_orientation(&points)?;
                let wanted = if ring_index == 0 {
                    ExactSign::Positive
                } else {
                    ExactSign::Negative
                };
                if orientation != wanted {
                    reverse_ring(&mut points, &mut coordinates);
                }
                rotate_ring(&mut points, &mut coordinates);
                rings.push(points);
                fixed.push(coordinates);
            }
            if rings.len() > 2 {
                let mut holes: Vec<_> = rings.drain(1..).zip(fixed.drain(1..)).collect();
                holes.sort_by(|(left, _), (right, _)| compare_exact_sequences(left, right));
                for (ring, coordinates) in holes {
                    rings.push(ring);
                    fixed.push(coordinates);
                }
            }
            Some(polygon)
        },
        VoronoiBoundary::Envelope | VoronoiBoundary::Padded => {
            let points: Vec<_> = sites.iter().map(|site| site.point).collect();
            let rectangle =
                exact::rectangular_boundary(&points, matches!(boundary, VoronoiBoundary::Padded))?;
            fixed.push(
                rectangle
                    .iter()
                    .map(exact::ExactPoint::round_nearest_even)
                    .collect::<Result<Vec<_>>>()?,
            );
            rings.push(rectangle.into());
            None
        },
    };
    let open: Vec<_> = rings.iter().map(Vec::as_slice).collect();
    let contacts = validate_exact_cycles(&open, 0, budget)?.contacts;
    Ok(NormalizedClip {
        rings,
        fixed,
        source,
        contacts,
    })
}

fn compare_exact_sequences(left: &[exact::ExactPoint], right: &[exact::ExactPoint]) -> Ordering {
    left.iter()
        .zip(right)
        .map(|(left, right)| left.compare_lex(right))
        .find(|order| order.is_ne())
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

struct SiteAdjacency {
    offsets: Box<[u32]>,
    neighbors: Box<[SiteId]>,
}

fn certified_site_adjacency(
    edges: &[CertifiedPrimalEdge],
    site_count: usize,
    budget: &mut ExpansionBudget,
) -> Result<SiteAdjacency> {
    budget.add(
        site_count
            .saturating_add(1)
            .saturating_add(edges.len().saturating_mul(2)),
    )?;
    let mut degrees = Vec::new();
    degrees.reserve_exact(site_count);
    degrees.resize(site_count, 0_usize);
    for edge in edges {
        let sites = match *edge {
            CertifiedPrimalEdge::Interior { sites, .. }
            | CertifiedPrimalEdge::Hull { sites, .. } => sites,
        };
        degrees[sites[0]] = degrees[sites[0]].saturating_add(1);
        degrees[sites[1]] = degrees[sites[1]].saturating_add(1);
    }
    let mut offsets = Vec::new();
    offsets.reserve_exact(site_count + 1);
    offsets.push(0_u32);
    for degree in degrees {
        let next = (offsets.last().copied().unwrap_or(0) as usize)
            .checked_add(degree)
            .ok_or_else(|| voronoi_error("too many site adjacencies"))?;
        offsets.push(u32::try_from(next).map_err(|_| voronoi_error("too many site adjacencies"))?);
    }
    let neighbor_count = offsets.last().copied().unwrap_or(0) as usize;
    let mut neighbors = Vec::new();
    neighbors.reserve_exact(neighbor_count);
    neighbors.resize(neighbor_count, SiteId(0));
    let mut cursors = Vec::new();
    cursors.reserve_exact(site_count);
    cursors.extend(offsets[..site_count].iter().map(|offset| *offset as usize));
    for edge in edges {
        let sites = match *edge {
            CertifiedPrimalEdge::Interior { sites, .. }
            | CertifiedPrimalEdge::Hull { sites, .. } => sites,
        };
        neighbors[cursors[sites[0]]] = SiteId(sites[1] as u32);
        cursors[sites[0]] += 1;
        neighbors[cursors[sites[1]]] = SiteId(sites[0] as u32);
        cursors[sites[1]] += 1;
    }
    for site in 0..site_count {
        neighbors[offsets[site] as usize..offsets[site + 1] as usize].sort_unstable();
    }
    Ok(SiteAdjacency {
        offsets: offsets.into_boxed_slice(),
        neighbors: neighbors.into_boxed_slice(),
    })
}

fn nearest_site_walk(
    point: &exact::ExactPoint,
    sites: &[Site],
    adjacency: &SiteAdjacency,
    mut current: SiteId,
    budget: &mut ExpansionBudget,
) -> Result<SiteId> {
    for _ in 0..sites.len() {
        let start = adjacency.offsets[current.0 as usize] as usize;
        let end = adjacency.offsets[current.0 as usize + 1] as usize;
        budget.add(end - start)?;
        let mut best = current;
        for &candidate in &adjacency.neighbors[start..end] {
            if exact::squared_distance_cmp_point(
                point,
                sites[candidate.0 as usize].point.xy(),
                sites[best.0 as usize].point.xy(),
            ) == Ordering::Less
            {
                best = candidate;
            }
        }
        if best != current {
            current = best;
            continue;
        }
        if adjacency.neighbors[start..end].iter().any(|candidate| {
            exact::squared_distance_cmp_point(
                point,
                sites[candidate.0 as usize].point.xy(),
                sites[current.0 as usize].point.xy(),
            ) == Ordering::Equal
        }) {
            return Err(voronoi_error(
                "private exact frame atom has a tied nearest site",
            ));
        }
        return Ok(current);
    }
    Err(voronoi_error("certified nearest-site walk did not descend"))
}

#[cfg(test)]
fn nearest_site(point: &exact::ExactPoint, sites: &[Site]) -> Result<SiteId> {
    let mut nearest = 0;
    let mut tied = false;
    for candidate in 1..sites.len() {
        match exact::squared_distance_cmp_point(
            point,
            sites[candidate].point.xy(),
            sites[nearest].point.xy(),
        ) {
            Ordering::Less => {
                nearest = candidate;
                tied = false;
            },
            Ordering::Equal => tied = true,
            Ordering::Greater => {},
        }
    }
    if tied {
        return Err(voronoi_error(
            "private exact frame atom has a tied nearest site",
        ));
    }
    Ok(SiteId(
        u32::try_from(nearest).map_err(|_| voronoi_error("too many Voronoi sites"))?,
    ))
}

fn orient_separator(
    mut start: exact::ExactPoint,
    mut end: exact::ExactPoint,
    sites: [SiteId; 2],
    input: &[Site],
) -> Result<(exact::ExactPoint, exact::ExactPoint)> {
    match exact::orient_points(
        &start,
        &end,
        &exact::ExactPoint::from_xy(input[sites[0].0 as usize].point.xy()),
    ) {
        ExactSign::Positive => {},
        ExactSign::Negative => std::mem::swap(&mut start, &mut end),
        ExactSign::Zero => {
            return Err(voronoi_error("Voronoi separator contains its source site"));
        },
    }
    Ok((start, end))
}

/// The frame hits of the perpendicular bisector of two sites: certified lazy
/// points on a frame of stored doubles, the exact construction otherwise.
fn bisector_frame_intersections(
    sites: [XY; 2],
    frame: &[exact::ExactPoint; 4],
) -> Result<Vec<(u8, exact::ExactPoint)>> {
    let corners: Option<[XY; 4]> = frame
        .iter()
        .map(exact::ExactPoint::as_double)
        .collect::<Option<Vec<_>>>()
        .and_then(|corners| corners.try_into().ok());
    if let Some(corners) = corners {
        let mut hits = Vec::new();
        let mut certified = true;
        for side in 0..4 {
            let ends = [corners[side], corners[(side + 1) % 4]];
            match exact::ExactPoint::certified_bisector_hit(sites, ends) {
                exact::FrameHit::Declined => {
                    certified = false;
                    break;
                },
                exact::FrameHit::Parallel => {},
                exact::FrameHit::Point(hit) => {
                    let within = |low: &exact::ExactPoint, high: &exact::ExactPoint| {
                        let x = (hit.compare_x(low) != Ordering::Less
                            && hit.compare_x(high) != Ordering::Greater)
                            || (hit.compare_x(high) != Ordering::Less
                                && hit.compare_x(low) != Ordering::Greater);
                        let y = (hit.compare_y(low) != Ordering::Less
                            && hit.compare_y(high) != Ordering::Greater)
                            || (hit.compare_y(high) != Ordering::Less
                                && hit.compare_y(low) != Ordering::Greater);
                        x && y
                    };
                    if within(&frame[side], &frame[(side + 1) % 4]) {
                        hits.push((side as u8, hit));
                    }
                },
            }
        }
        if certified {
            // Adjacent for the corner dedup below; the order itself carries
            // nothing, since a bisector meets a convex frame at most twice and
            // the caller keeps the one on its side. Rounded keys keep this off
            // the exact comparator.
            let key = |point: &exact::ExactPoint| {
                point
                    .round_nearest_even()
                    .unwrap_or(XY::new(f64::NAN, f64::NAN))
            };
            hits.sort_by(|left, right| {
                lex_xy(key(&left.1), key(&right.1)).then(left.0.cmp(&right.0))
            });
            hits.dedup_by(|left, right| left.1.same_position(&right.1));
            return Ok(hits);
        }
    }
    frame_intersections(
        &exact::ExactLine::perpendicular_bisector(sites[0], sites[1]),
        frame,
    )
}

fn frame_intersections(
    line: &exact::ExactLine,
    frame: &[exact::ExactPoint; 4],
) -> Result<Vec<(u8, exact::ExactPoint)>> {
    let mut hits = Vec::new();
    for side in 0..4 {
        let edge = exact::ExactLine::through_points(&frame[side], &frame[(side + 1) % 4])?;
        let hit = exact::line_intersection(line, &edge);
        if exact::signed_line_product(line, &hit) == ExactSign::Zero
            && exact::point_in_cycle(frame, &hit) != exact::PointInCycle::Outside
            && exact::segment_intersection(&frame[side], &frame[(side + 1) % 4], &hit, &hit)
                .is_point()
        {
            hits.push((side as u8, hit));
        }
    }
    hits.sort_by(|left, right| left.1.compare_lex(&right.1).then(left.0.cmp(&right.0)));
    hits.dedup_by(|left, right| left.1.same_position(&right.1));
    Ok(hits)
}

trait SegmentIntersectionExt {
    fn is_point(&self) -> bool;
}

impl SegmentIntersectionExt for exact::SegmentIntersection {
    fn is_point(&self) -> bool {
        matches!(self, Self::Point(_))
    }
}

fn clip_rectangle(clip: &NormalizedClip<'_>) -> Result<[exact::ExactPoint; 4]> {
    if let [ring] = clip.rings.as_slice()
        && ring.len() == 4
    {
        let rect = [
            ring[0].clone(),
            ring[1].clone(),
            ring[2].clone(),
            ring[3].clone(),
        ];
        let axis_aligned = rect[0].compare_y(&rect[1]).is_eq()
            && rect[2].compare_y(&rect[3]).is_eq()
            && rect[1].compare_x(&rect[2]).is_eq()
            && rect[3].compare_x(&rect[0]).is_eq()
            && rect[0].compare_x(&rect[1]).is_lt()
            && rect[1].compare_y(&rect[2]).is_lt();
        if axis_aligned {
            return Ok(rect);
        }
    }
    exact::axis_aligned_bounds(clip.rings.iter().flatten())
}

fn in_rectangle(rect: &[exact::ExactPoint; 4], point: &exact::ExactPoint) -> bool {
    exact::point_in_cycle(rect, point) != exact::PointInCycle::Outside
}

fn sort_unique_along(
    origin: &exact::ExactPoint,
    hint: &exact::ExactPoint,
    mut points: Vec<exact::ExactPoint>,
) -> Vec<exact::ExactPoint> {
    points.sort_by(|left, right| exact::compare_along(origin, hint, left, right));
    points.dedup_by(|left, right| left.same_position(right));
    points
}

fn span_from_points(
    origin: &exact::ExactPoint,
    hint: &exact::ExactPoint,
    points: Vec<exact::ExactPoint>,
) -> Option<(exact::ExactPoint, exact::ExactPoint)> {
    let points = sort_unique_along(origin, hint, points);
    (points.len() >= 2).then(|| (points[0].clone(), points[points.len() - 1].clone()))
}

fn separator_rectangle_hits(
    pair: [XY; 2],
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
    budget: &mut ExpansionBudget,
) -> Result<Vec<exact::ExactPoint>> {
    let certified = bisector_frame_intersections(pair, rect)?;
    budget.add(certified.len().saturating_add(8))?;
    let mut hits = Vec::new();
    reserve_exact(&mut hits, certified.len().saturating_add(8))?;
    hits.extend(certified.into_iter().map(|(_, point)| point));
    for side in 0..4 {
        let start = &rect[side];
        let end = &rect[(side + 1) % 4];
        if exact::signed_line_product(line, start) == ExactSign::Zero
            && exact::signed_line_product(line, end) == ExactSign::Zero
        {
            hits.push(start.clone());
            hits.push(end.clone());
        }
    }
    Ok(hits)
}

fn clip_segment_to_rectangle(
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
    pair: [XY; 2],
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
    budget: &mut ExpansionBudget,
) -> Result<Option<(exact::ExactPoint, exact::ExactPoint)>> {
    budget.add(4)?;
    let mut points = separator_rectangle_hits(pair, line, rect, budget)?;
    budget.add(2)?;
    reserve_exact(&mut points, 2)?;
    if in_rectangle(rect, start) {
        points.push(start.clone());
    }
    if in_rectangle(rect, end) {
        points.push(end.clone());
    }
    points.retain(|point| exact::segment_intersection(start, end, point, point).is_point());
    Ok(span_from_points(start, end, points))
}

fn hull_point_on_ray(
    center: &exact::ExactPoint,
    mid: &exact::ExactPoint,
    sites: [&exact::ExactPoint; 2],
    opposite_sign: ExactSign,
    point: &exact::ExactPoint,
) -> bool {
    if point.same_position(center) {
        return true;
    }
    if center.same_position(mid) {
        return exact::orient_points(sites[0], sites[1], point) != opposite_sign;
    }
    if exact::orient_points(sites[0], sites[1], center) == opposite_sign {
        exact::compare_along(center, mid, center, point) != Ordering::Greater
    } else {
        exact::compare_along(mid, center, center, point) != Ordering::Greater
    }
}

fn clip_hull_ray_to_rectangle(
    center: &exact::ExactPoint,
    pair: [usize; 2],
    opposite: usize,
    line: &exact::ExactLine,
    rect: &[exact::ExactPoint; 4],
    sites: &[Site],
    budget: &mut ExpansionBudget,
) -> Result<Option<(exact::ExactPoint, exact::ExactPoint)>> {
    budget.add(4)?;
    let hits = separator_rectangle_hits(
        [sites[pair[0]].point.xy(), sites[pair[1]].point.xy()],
        line,
        rect,
        budget,
    )?;
    let left = exact::ExactPoint::from_xy(sites[pair[0]].point.xy());
    let right = exact::ExactPoint::from_xy(sites[pair[1]].point.xy());
    let mid = left.midpoint(&right);
    let opposite_sign = exact::orient_points(
        &left,
        &right,
        &exact::ExactPoint::from_xy(sites[opposite].point.xy()),
    );
    budget.add(hits.len().saturating_add(1))?;
    let mut points = Vec::new();
    reserve_exact(&mut points, hits.len().saturating_add(1))?;
    if in_rectangle(rect, center) {
        points.push(center.clone());
    }
    for hit in hits {
        if hull_point_on_ray(center, &mid, [&left, &right], opposite_sign, &hit) {
            points.push(hit);
        }
    }
    if points.len() < 2 {
        return Ok(None);
    }
    let (origin, hint) = if center.same_position(&mid) {
        let Some(hint) = points
            .iter()
            .find(|point| !point.same_position(center))
            .cloned()
        else {
            return Ok(None);
        };
        (center.clone(), hint)
    } else if exact::orient_points(&left, &right, center) == opposite_sign {
        (center.clone(), mid)
    } else {
        (mid, center.clone())
    };
    Ok(span_from_points(&origin, &hint, points))
}

fn coincident_frame_owner(
    pair: [SiteId; 2],
    ccw_start: &exact::ExactPoint,
    ccw_end: &exact::ExactPoint,
    sites: &[Site],
) -> Result<SiteId> {
    let mut inside = None;
    for &id in &pair {
        match exact::orient_points(
            ccw_start,
            ccw_end,
            &exact::ExactPoint::from_xy(sites[id.0 as usize].point.xy()),
        ) {
            ExactSign::Positive => {
                if inside.replace(id).is_some() {
                    return Err(voronoi_error(
                        "both separator sites lie inside a coincident frame side",
                    ));
                }
            },
            ExactSign::Negative => {},
            ExactSign::Zero => {
                return Err(voronoi_error("Voronoi separator contains its source site"));
            },
        }
    }
    inside.ok_or_else(|| voronoi_error("coincident frame separator has no interior site"))
}

fn build_scaffold(
    rect: &[exact::ExactPoint; 4],
    clip: &NormalizedClip<'_>,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ScaffoldPrimitive>> {
    let outer_is_rect = clip.rings.first().is_some_and(|ring| {
        ring.len() == 4 && (0..4).all(|side| ring[side].same_position(&rect[side]))
    });
    let clip_rings = if outer_is_rect {
        &clip.rings[1..]
    } else {
        clip.rings.as_slice()
    };
    let scaffold_count = 4_usize.saturating_add(clip_rings.iter().map(Vec::len).sum::<usize>());
    budget.add(scaffold_count)?;
    let mut scaffold = Vec::new();
    scaffold
        .try_reserve(scaffold_count)
        .map_err(|_| voronoi_error("could not allocate exact Voronoi subdivision"))?;
    for side in 0..4 {
        scaffold.push(ScaffoldPrimitive {
            line: exact::ExactLine::through_points(&rect[side], &rect[(side + 1) % 4])?,
            start: rect[side].clone(),
            end: rect[(side + 1) % 4].clone(),
            frame_delta: 1,
            clip_delta: i32::from(outer_is_rect),
            owner: PrimitiveOwner::FrameNearest,
            clip_source: outer_is_rect.then_some(ClipSegmentId(side as u32)),
            frame_side: Some(side as u8),
            events: Vec::new(),
        });
    }
    let mut clip_segment = if outer_is_rect { 4 } else { 0 };
    for ring in clip_rings {
        for index in 0..ring.len() {
            scaffold.push(ScaffoldPrimitive {
                line: exact::ExactLine::through_points(
                    &ring[index],
                    &ring[(index + 1) % ring.len()],
                )?,
                start: ring[index].clone(),
                end: ring[(index + 1) % ring.len()].clone(),
                frame_delta: 0,
                clip_delta: 1,
                owner: PrimitiveOwner::Preserve,
                clip_source: Some(ClipSegmentId(clip_segment)),
                frame_side: None,
                events: Vec::new(),
            });
            clip_segment = clip_segment
                .checked_add(1)
                .ok_or_else(|| voronoi_error("too many clip segments"))?;
        }
    }
    for contact in &clip.contacts {
        for segment in contact.segments {
            let primitive = scaffold
                .iter_mut()
                .find(|primitive| primitive.clip_source == Some(ClipSegmentId(segment as u32)))
                .ok_or_else(|| voronoi_error("validated clip contact has no source segment"))?;
            budgeted_try_push(&mut primitive.events, contact.point.clone(), budget)?;
        }
    }
    register_frame_clip_contacts(&mut scaffold, budget)?;
    for primitive in &mut scaffold {
        if exact::signed_line_product(&primitive.line, &primitive.start) != ExactSign::Zero
            || exact::signed_line_product(&primitive.line, &primitive.end) != ExactSign::Zero
        {
            return Err(voronoi_error(
                "exact Voronoi primitive endpoints leave their support line",
            ));
        }
        budget.add(2)?;
        reserve_exact(&mut primitive.events, 2)?;
        primitive
            .events
            .extend([primitive.start.clone(), primitive.end.clone()]);
    }
    Ok(scaffold)
}

fn emit_clipped_separators(
    sites: &[Site],
    primal_edges: &[CertifiedPrimalEdge],
    complex: &DelaunayComplex,
    rect: &[exact::ExactPoint; 4],
    budget: &mut ExpansionBudget,
) -> Result<Vec<ClippedSeparator>> {
    let mut separators = Vec::new();
    for &primal in primal_edges {
        let (pair, span, line) = match primal {
            CertifiedPrimalEdge::Interior { sites: pair, faces } => {
                let components = [
                    complex.component_of_face(faces[0]),
                    complex.component_of_face(faces[1]),
                ];
                if components[0] == components[1] {
                    continue;
                }
                let line = exact::ExactLine::perpendicular_bisector(
                    sites[pair[0]].point.xy(),
                    sites[pair[1]].point.xy(),
                );
                let Some(span) = clip_segment_to_rectangle(
                    complex.center(components[0]),
                    complex.center(components[1]),
                    [sites[pair[0]].point.xy(), sites[pair[1]].point.xy()],
                    &line,
                    rect,
                    budget,
                )?
                else {
                    continue;
                };
                (pair, span, line)
            },
            CertifiedPrimalEdge::Hull {
                sites: pair,
                face,
                opposite,
            } => {
                let line = exact::ExactLine::perpendicular_bisector(
                    sites[pair[0]].point.xy(),
                    sites[pair[1]].point.xy(),
                );
                let Some(span) = clip_hull_ray_to_rectangle(
                    complex.center(complex.component_of_face(face)),
                    pair,
                    opposite,
                    &line,
                    rect,
                    sites,
                    budget,
                )?
                else {
                    continue;
                };
                (pair, span, line)
            },
        };
        let ids = [SiteId(pair[0] as u32), SiteId(pair[1] as u32)];
        let (start, end) = orient_separator(span.0, span.1, ids, sites)?;
        if exact::signed_line_product(&line, &start) != ExactSign::Zero
            || exact::signed_line_product(&line, &end) != ExactSign::Zero
        {
            return Err(voronoi_error(
                "exact Voronoi primitive endpoints leave their support line",
            ));
        }
        budget.add(1)?;
        separators
            .try_reserve(1)
            .map_err(|_| voronoi_error("could not allocate exact Voronoi subdivision"))?;
        budget.add(2)?;
        let mut events = Vec::new();
        reserve_exact(&mut events, 2)?;
        events.extend([start.clone(), end.clone()]);
        separators.push(ClippedSeparator {
            pair: ids,
            start,
            end,
            events,
        });
    }
    Ok(separators)
}

fn merge_owner(left: OwnerTransition, right: OwnerTransition) -> Result<OwnerTransition> {
    match (left, right) {
        (OwnerTransition::Preserve, value) | (value, OwnerTransition::Preserve) => Ok(value),
        (left, right) if left == right => Ok(left),
        _ => Err(voronoi_error(
            "conflicting owner transitions on one exact Voronoi atom",
        )),
    }
}

fn fixed_coordinates(
    points: &[exact::ExactPoint],
    sites: &[Site],
    clip: &NormalizedClip<'_>,
) -> Result<Vec<Option<XY>>> {
    let mut fixed = Vec::new();
    fixed.reserve_exact(points.len());
    fixed.resize(points.len(), None);
    for candidate in sites
        .iter()
        .map(|site| site.point.xy())
        .chain(clip.fixed.iter().flatten().copied())
    {
        let point = exact::ExactPoint::from_xy(candidate);
        if let Ok(index) = points.binary_search_by(|known| known.compare_lex(&point)) {
            if let Some(existing) = fixed[index]
                && PointKey::new(existing) != PointKey::new(candidate)
            {
                return Err(voronoi_error(
                    "one exact Voronoi vertex has conflicting source coordinates",
                ));
            }
            fixed[index] = Some(candidate);
        }
    }
    Ok(fixed)
}

#[expect(
    clippy::too_many_arguments,
    reason = "atom emission needs the endpoints, events, windings, owner, and seed sink together"
)]
fn emit_atom_windows(
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
    events: &mut Vec<exact::ExactPoint>,
    frame_delta: i8,
    clip_delta: i32,
    owner: PrimitiveOwner,
    separator: Option<[SiteId; 2]>,
    clip_source: Option<ClipSegmentId>,
    frame_side: Option<u8>,
    seeds: &mut Vec<AtomSeed>,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    events.sort_by(|left, right| exact::compare_along(start, end, left, right));
    events.dedup_by(|left, right| left.same_position(right));
    if !events
        .first()
        .is_some_and(|point| point.same_position(start))
        || !events.last().is_some_and(|point| point.same_position(end))
    {
        return Err(voronoi_error(
            "exact primitive event order lost an endpoint",
        ));
    }
    for pair in events.windows(2) {
        if exact::compare_along(start, end, &pair[0], &pair[1]) != Ordering::Less {
            return Err(voronoi_error(
                "exact primitive has unordered duplicate events",
            ));
        }
        budgeted_try_push(
            seeds,
            AtomSeed {
                start: pair[0].clone(),
                end: pair[1].clone(),
                frame_delta,
                clip_delta,
                owner,
                separator,
                clip_source,
                frame_side,
            },
            budget,
        )?;
    }
    Ok(())
}

fn register_frame_clip_contacts(
    scaffold: &mut [ScaffoldPrimitive],
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let (frames, clips) = scaffold.split_at_mut(4);
    budget.add(4_usize.saturating_mul(clips.len()))?;
    for clip_prim in clips {
        for frame_prim in frames.iter_mut() {
            match exact::segment_intersection(
                &frame_prim.start,
                &frame_prim.end,
                &clip_prim.start,
                &clip_prim.end,
            ) {
                exact::SegmentIntersection::None => {},
                exact::SegmentIntersection::Point(point) => {
                    budgeted_try_push(&mut frame_prim.events, point.clone(), budget)?;
                    budgeted_try_push(&mut clip_prim.events, point, budget)?;
                },
                exact::SegmentIntersection::Overlap { start, end } => {
                    push_split_events(
                        &mut frame_prim.events,
                        &mut clip_prim.events,
                        start,
                        end,
                        budget,
                    )?;
                },
            }
        }
    }
    Ok(())
}

fn push_split_events(
    left: &mut Vec<exact::ExactPoint>,
    right: &mut Vec<exact::ExactPoint>,
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    budget.add(4)?;
    reserve_exact(left, 2)?;
    left.extend([start.clone(), end.clone()]);
    reserve_exact(right, 2)?;
    right.extend([start, end]);
    Ok(())
}

fn node_scaffold_and_separators(
    mut scaffold: Vec<ScaffoldPrimitive>,
    mut separators: Vec<ClippedSeparator>,
    sites: &[Site],
    adjacency: &SiteAdjacency,
    clip: &NormalizedClip<'_>,
    rect: &[exact::ExactPoint; 4],
    budget: &mut ExpansionBudget,
) -> Result<NodedAtoms> {
    budget.add(separators.len().saturating_mul(scaffold.len()))?;
    for separator in &mut separators {
        for primitive in &mut scaffold {
            match exact::segment_intersection(
                &separator.start,
                &separator.end,
                &primitive.start,
                &primitive.end,
            ) {
                exact::SegmentIntersection::None => {},
                exact::SegmentIntersection::Point(point) => {
                    budgeted_try_push(&mut separator.events, point.clone(), budget)?;
                    budgeted_try_push(&mut primitive.events, point, budget)?;
                },
                exact::SegmentIntersection::Overlap { start, end } => {
                    push_split_events(
                        &mut separator.events,
                        &mut primitive.events,
                        start,
                        end,
                        budget,
                    )?;
                },
            }
        }
    }
    let mut seeds = Vec::new();
    for primitive in &mut scaffold {
        emit_atom_windows(
            &primitive.start,
            &primitive.end,
            &mut primitive.events,
            primitive.frame_delta,
            primitive.clip_delta,
            primitive.owner,
            None,
            primitive.clip_source,
            primitive.frame_side,
            &mut seeds,
            budget,
        )?;
    }
    for separator in &mut separators {
        emit_atom_windows(
            &separator.start,
            &separator.end,
            &mut separator.events,
            0,
            0,
            PrimitiveOwner::Boundary {
                left: Some(separator.pair[0]),
                right: Some(separator.pair[1]),
            },
            Some(separator.pair),
            None,
            None,
            &mut seeds,
            budget,
        )?;
    }
    let mut event_points = Vec::new();
    reserve_exact(&mut event_points, seeds.len().saturating_mul(2))?;
    for seed in &seeds {
        event_points.extend([seed.start.clone(), seed.end.clone()]);
    }
    event_points.sort_by(exact::ExactPoint::compare_lex);
    event_points.dedup_by(|left, right| left.same_position(right));
    budget.add(event_points.len())?;
    let fixed = fixed_coordinates(&event_points, sites, clip)?;
    let vertices = event_points
        .into_iter()
        .zip(fixed)
        .map(|(point, fixed_xy)| NodedVertex { fixed_xy, point })
        .collect::<Vec<_>>();
    let vertex_id = |point: &exact::ExactPoint| -> Result<VertexId> {
        let index = vertices
            .binary_search_by(|candidate| candidate.point.compare_lex(point))
            .map_err(|_| voronoi_error("exact event was not globally interned"))?;
        Ok(VertexId(
            u32::try_from(index).map_err(|_| voronoi_error("too many exact vertices"))?,
        ))
    };
    #[derive(Clone)]
    struct MappedSeed {
        endpoints: [VertexId; 2],
        reversed: bool,
        frame_delta: i8,
        clip_delta: i32,
        owner: PrimitiveOwner,
        separator: Option<[SiteId; 2]>,
        clip_source: Option<ClipSegmentId>,
        frame_side: Option<u8>,
    }
    let mut mapped = Vec::new();
    reserve_exact(&mut mapped, seeds.len())?;
    for seed in seeds {
        let mut endpoints = [vertex_id(&seed.start)?, vertex_id(&seed.end)?];
        if endpoints[0] == endpoints[1] {
            return Err(voronoi_error("exact primitive produced a zero-length atom"));
        }
        let reversed = endpoints[0] > endpoints[1];
        if reversed {
            endpoints.swap(0, 1);
        }
        mapped.push(MappedSeed {
            endpoints,
            reversed,
            frame_delta: if reversed {
                -seed.frame_delta
            } else {
                seed.frame_delta
            },
            clip_delta: if reversed {
                -seed.clip_delta
            } else {
                seed.clip_delta
            },
            owner: seed.owner,
            separator: seed.separator,
            clip_source: seed.clip_source,
            frame_side: seed.frame_side,
        });
    }
    mapped.sort_by_key(|seed| seed.endpoints);
    let mut edges = Vec::new();
    let mut start = 0;
    let mut frame_owner = SiteId(0);
    while start < mapped.len() {
        let mut end = start + 1;
        while end < mapped.len() && mapped[end].endpoints == mapped[start].endpoints {
            end += 1;
        }
        let mut frame_delta = 0_i8;
        let mut clip_delta = 0_i32;
        let mut owner = OwnerTransition::Preserve;
        let mut separator = None;
        let mut clip_sources = Vec::new();
        let mut frame_side = None;
        for seed in &mapped[start..end] {
            frame_delta = frame_delta
                .checked_add(seed.frame_delta)
                .ok_or_else(|| voronoi_error("exact frame winding overflow"))?;
            clip_delta = clip_delta
                .checked_add(seed.clip_delta)
                .ok_or_else(|| voronoi_error("exact clip winding overflow"))?;
            if let Some(value) = seed.separator {
                if separator.is_some_and(|existing| existing != value) {
                    return Err(voronoi_error("two Voronoi separators share one open atom"));
                }
                separator = Some(value);
            }
            if let Some(value) = seed.clip_source {
                clip_sources.push(value);
            }
            if let Some(value) = seed.frame_side {
                if frame_side.is_some_and(|existing| existing != value) {
                    return Err(voronoi_error("two private frame sides share one atom"));
                }
                frame_side = Some(value);
            }
        }
        if let (Some(pair), Some(side)) = (separator, frame_side) {
            let ccw_start = &rect[side as usize];
            let ccw_end = &rect[(side as usize + 1) % 4];
            let inside = coincident_frame_owner(pair, ccw_start, ccw_end, sites)?;
            let mut coincident = OwnerTransition::Boundary {
                left: Some(inside),
                right: None,
            };
            let stored_start = &vertices[mapped[start].endpoints[0].0 as usize].point;
            let stored_end = &vertices[mapped[start].endpoints[1].0 as usize].point;
            if exact::compare_along(ccw_start, ccw_end, stored_start, stored_end)
                == Ordering::Greater
            {
                coincident = Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: coincident,
                }
                .reversed()
                .owner;
            }
            owner = coincident;
        } else {
            for seed in &mapped[start..end] {
                let resolved = match seed.owner {
                    PrimitiveOwner::Preserve => OwnerTransition::Preserve,
                    PrimitiveOwner::Boundary { left, right } => {
                        let owner = OwnerTransition::Boundary { left, right };
                        if seed.reversed {
                            Transition {
                                frame_delta: 0,
                                clip_delta: 0,
                                owner,
                            }
                            .reversed()
                            .owner
                        } else {
                            owner
                        }
                    },
                    PrimitiveOwner::FrameNearest => {
                        let midpoint = vertices[seed.endpoints[0].0 as usize]
                            .point
                            .midpoint(&vertices[seed.endpoints[1].0 as usize].point);
                        frame_owner =
                            nearest_site_walk(&midpoint, sites, adjacency, frame_owner, budget)?;
                        let owner = OwnerTransition::Boundary {
                            left: Some(frame_owner),
                            right: None,
                        };
                        if seed.reversed {
                            Transition {
                                frame_delta: 0,
                                clip_delta: 0,
                                owner,
                            }
                            .reversed()
                            .owner
                        } else {
                            owner
                        }
                    },
                };
                owner = merge_owner(owner, resolved)?;
            }
        }
        clip_sources.sort_unstable();
        clip_sources.dedup();
        if frame_delta != 0 || clip_delta != 0 || separator.is_some() {
            budget.add(1)?;
            edges.push(AtomicEdge {
                endpoints: mapped[start].endpoints,
                forward: Transition {
                    frame_delta,
                    clip_delta,
                    owner,
                },
                separator,
                clip_sources: clip_sources.into_boxed_slice(),
                frame_side,
            });
        }
        start = end;
    }
    Ok(NodedAtoms { vertices, edges })
}

struct ExactVertex {
    point: exact::ExactPoint,
    fixed_xy: Option<XY>,
}

fn uf_root(parents: &mut [usize], mut node: usize) -> usize {
    while parents[node] != node {
        parents[node] = parents[parents[node]];
        node = parents[node];
    }
    node
}

fn build_exact_subdivision(
    atoms: NodedAtoms,
    budget: &mut ExpansionBudget,
) -> Result<ExactSubdivision> {
    let vertices = atoms.vertices.len();
    let edges = atoms.edges.len();
    let slots = edges.saturating_mul(2);
    budget.add(slots)?;
    budget.add(slots)?;
    let mut topology = IndexedTopology::default();
    let mut scratch = IndexedTopologyScratch::default();
    let mut edge_of_slot = Vec::new();
    let mut cursors = Vec::new();
    let mut exact_vertices = Vec::new();
    for reserve in [
        topology.starts.try_reserve(vertices + 1),
        topology.targets.try_reserve(slots),
        topology.owners.try_reserve(slots),
        topology.twins.try_reserve(slots),
        topology.component_of.try_reserve(vertices),
        topology.face_of.try_reserve(slots),
        topology.face_starts.try_reserve(slots + 1),
        topology.face_slots.try_reserve(slots),
        scratch.component_stack.try_reserve(vertices),
        scratch.face_cursor.try_reserve(vertices),
        scratch.successor.try_reserve(slots),
        scratch.edge_slots.try_reserve(edges),
        edge_of_slot.try_reserve(slots),
        cursors.try_reserve(vertices),
        exact_vertices.try_reserve(vertices),
    ] {
        reserve.map_err(|_| voronoi_error("could not allocate exact Voronoi topology"))?;
    }
    topology.starts.resize(vertices + 1, 0);
    for edge in &atoms.edges {
        topology.starts[edge.endpoints[0].0 as usize + 1] += 1;
        topology.starts[edge.endpoints[1].0 as usize + 1] += 1;
    }
    for index in 1..topology.starts.len() {
        topology.starts[index] += topology.starts[index - 1];
    }
    cursors.extend_from_slice(&topology.starts[..vertices]);
    topology.targets.resize(slots, 0);
    edge_of_slot.resize(slots, 0);
    for (edge, atom) in atoms.edges.iter().enumerate() {
        let edge = u32::try_from(edge).map_err(|_| voronoi_error("too many exact edges"))?;
        let [start, end] = atom.endpoints;
        for (origin, target) in [(start, end), (end, start)] {
            let slot = cursors[origin.0 as usize] as usize;
            topology.targets[slot] = target.0;
            edge_of_slot[slot] = edge;
            cursors[origin.0 as usize] += 1;
        }
    }
    for vertex in 0..vertices {
        let row = topology.starts[vertex] as usize..topology.starts[vertex + 1] as usize;
        if row.len() < 2 {
            return Err(voronoi_error("exact Voronoi subdivision contains a dangle"));
        }
        let origin = &atoms.vertices[vertex].point;
        for index in row.start + 1..row.end {
            let mut cursor = index;
            while cursor > row.start
                && exact::angle_ccw_cmp(
                    origin,
                    &atoms.vertices[topology.targets[cursor] as usize].point,
                    &atoms.vertices[topology.targets[cursor - 1] as usize].point,
                )? == Ordering::Less
            {
                topology.targets.swap(cursor, cursor - 1);
                edge_of_slot.swap(cursor, cursor - 1);
                cursor -= 1;
            }
        }
        for slot in row.start + 1..row.end {
            exact::angle_ccw_cmp(
                origin,
                &atoms.vertices[topology.targets[slot - 1] as usize].point,
                &atoms.vertices[topology.targets[slot] as usize].point,
            )
            .map_err(|_| voronoi_error("exact angular tie"))?;
        }
    }
    exact_vertices.extend(atoms.vertices.into_iter().map(|vertex| ExactVertex {
        point: vertex.point,
        fixed_xy: vertex.fixed_xy,
    }));
    topology.finish(&edge_of_slot, &mut scratch);
    for slot in 0..slots {
        if topology.face_of[slot] == topology.face_of[topology.twins[slot] as usize] {
            return Err(voronoi_error("exact Voronoi subdivision contains a bridge"));
        }
    }
    Ok(ExactSubdivision {
        vertices: exact_vertices,
        edges: atoms.edges,
        topology,
        edge_of_slot,
        labels: Vec::new(),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegionLabel {
    frame_winding: i8,
    clip_winding: i32,
    owner: Option<SiteId>,
}

struct ExactSubdivision {
    vertices: Vec<ExactVertex>,
    edges: Vec<AtomicEdge>,
    topology: IndexedTopology,
    edge_of_slot: Vec<u32>,
    labels: Vec<RegionLabel>,
}

fn slot_transition(subdivision: &ExactSubdivision, slot: usize) -> Transition {
    let atomic = &subdivision.edges[subdivision.edge_of_slot[slot] as usize];
    if subdivision.topology.owners[slot] == atomic.endpoints[0].0 {
        atomic.forward
    } else {
        atomic.forward.reversed()
    }
}

fn selected_cell_label(label: RegionLabel) -> bool {
    label.frame_winding == 1 && label.clip_winding == 1
}

fn walk_label(subdivision: &ExactSubdivision, face: BoundaryFaceId) -> RegionLabel {
    subdivision.labels[face.0 as usize]
}

fn validate_exact_subdivision(subdivision: &ExactSubdivision) -> Result<()> {
    let topology = &subdivision.topology;
    let boundary_faces = topology.face_starts.len().saturating_sub(1);
    if subdivision.labels.len() != boundary_faces {
        return Err(voronoi_error("one exact boundary face has no label"));
    }
    for edge in &subdivision.edges {
        if edge.clip_sources.windows(2).any(|pair| pair[0] >= pair[1])
            || (edge.forward.clip_delta != 0 && edge.clip_sources.is_empty())
        {
            return Err(voronoi_error(
                "exact atomic edge has incoherent clip provenance",
            ));
        }
    }
    for slot in 0..topology.targets.len() {
        let twin = topology.twins[slot] as usize;
        let edge = subdivision.edge_of_slot[slot] as usize;
        let left = BoundaryFaceId(topology.face_of[slot]);
        let right = BoundaryFaceId(topology.face_of[twin]);
        let transition = slot_transition(subdivision, slot);
        if topology.twins[twin] != slot as u32
            || topology.owners[slot] != topology.targets[twin]
            || topology.targets[slot] != topology.owners[twin]
            || subdivision.edge_of_slot[twin] as usize != edge
            || topology.component_of[topology.owners[slot] as usize]
                != topology.component_of[topology.owners[twin] as usize]
            || topology.predecessor_slot(topology.successor_slot(slot as u32)) != slot as u32
            || crossed_label(walk_label(subdivision, left), transition)?
                != walk_label(subdivision, right)
            || crossed_label(walk_label(subdivision, right), transition.reversed())?
                != walk_label(subdivision, left)
        {
            return Err(voronoi_error(
                "exact indexed topology invariants or twin labels disagree",
            ));
        }
    }
    for label in &subdivision.labels {
        if !matches!(label.frame_winding, 0 | 1)
            || !matches!(label.clip_winding, 0 | 1)
            || (label.frame_winding == 1 && label.owner.is_none())
        {
            return Err(voronoi_error(
                "exact boundary walk has an incoherent checked label",
            ));
        }
    }
    Ok(())
}

fn crossed_label(left: RegionLabel, transition: Transition) -> Result<RegionLabel> {
    let owner = match transition.owner {
        OwnerTransition::Preserve => left.owner,
        OwnerTransition::Boundary {
            left: expected,
            right,
        } => {
            if left.owner != expected {
                return Err(voronoi_error(
                    "Voronoi owner transition disagrees with propagated face label",
                ));
            }
            right
        },
    };
    Ok(RegionLabel {
        frame_winding: left.frame_winding - transition.frame_delta,
        clip_winding: left.clip_winding - transition.clip_delta,
        owner,
    })
}

fn assign_walk_label(
    labels: &mut [Option<RegionLabel>],
    queue: &mut VecDeque<BoundaryFaceId>,
    face: BoundaryFaceId,
    label: RegionLabel,
) -> Result<()> {
    match labels[face.0 as usize] {
        None => {
            labels[face.0 as usize] = Some(label);
            queue.push_back(face);
            Ok(())
        },
        Some(existing) if existing == label => Ok(()),
        Some(_) => Err(voronoi_error("inconsistent exact region-label propagation")),
    }
}

fn propagate_walk_labels(
    subdivision: &ExactSubdivision,
    labels: &mut [Option<RegionLabel>],
    queue: &mut VecDeque<BoundaryFaceId>,
) -> Result<()> {
    let topology = &subdivision.topology;
    while let Some(left) = queue.pop_front() {
        let label = labels[left.0 as usize].expect("queued walk is labelled");
        let walk = &topology.face_slots[topology.face_starts[left.0 as usize] as usize
            ..topology.face_starts[left.0 as usize + 1] as usize];
        for &slot in walk {
            let transition = slot_transition(subdivision, slot as usize);
            let right = BoundaryFaceId(topology.face_of[topology.twins[slot as usize] as usize]);
            let right_label = crossed_label(label, transition)?;
            if crossed_label(right_label, transition.reversed())? != label {
                return Err(voronoi_error("twin label equations are not inverses"));
            }
            assign_walk_label(labels, queue, right, right_label)?;
        }
    }
    Ok(())
}

fn graft_components(
    subdivision: &mut ExactSubdivision,
    sites: &[Site],
    adjacency: &SiteAdjacency,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let topology = &subdivision.topology;
    let boundary_faces = topology.face_starts.len() - 1;
    let components = topology
        .component_of
        .iter()
        .copied()
        .max()
        .map_or(0, |id| id as usize + 1);
    subdivision
        .labels
        .try_reserve(boundary_faces)
        .map_err(|_| voronoi_error("could not allocate exact region graft"))?;
    let mut exterior = Vec::new();
    exterior
        .try_reserve(components)
        .map_err(|_| voronoi_error("could not allocate exact region graft"))?;
    exterior.resize(components, None);
    let mut points = Vec::new();
    for face in 0..boundary_faces {
        let walk = &topology.face_slots
            [topology.face_starts[face] as usize..topology.face_starts[face + 1] as usize];
        budget.add(walk.len())?;
        points.clear();
        reserve_exact(&mut points, walk.len())?;
        points.extend(walk.iter().map(|&slot| {
            subdivision.vertices[topology.owners[slot as usize] as usize]
                .point
                .clone()
        }));
        let sign = exact::cycle_orientation(&points)?;
        let component = topology.component_of[topology.owners[walk[0] as usize] as usize] as usize;
        if sign == ExactSign::Negative
            && exterior[component]
                .replace(BoundaryFaceId(face as u32))
                .is_some()
        {
            return Err(voronoi_error(
                "edge component has more than one negative boundary face",
            ));
        }
    }
    if exterior.iter().any(Option::is_none) {
        return Err(voronoi_error(
            "edge component has no negative boundary face",
        ));
    }
    let frame_edge = subdivision
        .edges
        .iter()
        .position(|edge| edge.frame_side.is_some())
        .ok_or_else(|| voronoi_error("private frame is absent"))?;
    let frame_component =
        topology.component_of[subdivision.edges[frame_edge].endpoints[0].0 as usize] as usize;
    let mut labels = Vec::new();
    labels
        .try_reserve(boundary_faces)
        .map_err(|_| voronoi_error("could not allocate exact region graft"))?;
    labels.resize(boundary_faces, None);
    let mut queue = VecDeque::new();
    queue
        .try_reserve(boundary_faces)
        .map_err(|_| voronoi_error("could not allocate exact region graft"))?;
    let outside_frame = RegionLabel {
        frame_winding: 0,
        clip_winding: 0,
        owner: None,
    };
    assign_walk_label(
        &mut labels,
        &mut queue,
        exterior[frame_component].expect("component exterior was checked"),
        outside_frame,
    )?;
    propagate_walk_labels(subdivision, &mut labels, &mut queue)?;
    for outside in exterior.iter().copied().map(Option::unwrap) {
        if labels[outside.0 as usize].is_some() {
            continue;
        }
        let slot = topology.face_slots[topology.face_starts[outside.0 as usize] as usize] as usize;
        let edge = &subdivision.edges[subdivision.edge_of_slot[slot] as usize];
        if edge.clip_sources.is_empty() || edge.frame_side.is_some() {
            return Err(voronoi_error("unvisited exact component has no clip seed"));
        }
        let transition = slot_transition(subdivision, slot);
        if transition.frame_delta != 0 || transition.clip_delta.unsigned_abs() != 1 {
            return Err(voronoi_error(
                "unvisited exact component has no simple clip seed",
            ));
        }
        let start = &subdivision.vertices[topology.owners[slot] as usize].point;
        let end = &subdivision.vertices[topology.targets[slot] as usize].point;
        let owner = nearest_site_walk(&start.midpoint(end), sites, adjacency, SiteId(0), budget)?;
        let label = RegionLabel {
            frame_winding: 1,
            clip_winding: i32::from(transition.clip_delta > 0),
            owner: Some(owner),
        };
        assign_walk_label(&mut labels, &mut queue, outside, label)?;
        propagate_walk_labels(subdivision, &mut labels, &mut queue)?;
    }
    subdivision.labels.clear();
    for label in labels {
        subdivision.labels.push(
            label.ok_or_else(|| {
                voronoi_error("exact subdivision left an unlabelled boundary face")
            })?,
        );
    }
    validate_exact_subdivision(subdivision)
}

struct ExactSimpleRing {
    face: BoundaryFaceId,
    vertices: Box<[VertexId]>,
    edges: Box<[EdgeId]>,
    orientation: ExactSign,
}

struct ExactCellPart {
    owner: SiteId,
    face: BoundaryFaceId,
    shell: ExactSimpleRing,
    holes: Vec<ExactSimpleRing>,
}

struct ExactEdgeChain {
    separator: [SiteId; 2],
    vertices: Box<[VertexId]>,
    edges: Box<[EdgeId]>,
}

fn canonicalize_ring(vertices: &mut Vec<VertexId>, edges: &mut Vec<EdgeId>) {
    let open_len = vertices.len() - 1;
    let least = (0..open_len)
        .min_by(|&left, &right| {
            (0..open_len)
                .map(|offset| {
                    vertices[(left + offset) % open_len].cmp(&vertices[(right + offset) % open_len])
                })
                .find(|order| order.is_ne())
                .unwrap_or(Ordering::Equal)
        })
        .unwrap();
    vertices.pop();
    vertices.rotate_left(least);
    edges.rotate_left(least);
    vertices.push(vertices[0]);
}

fn split_boundary_face(
    subdivision: &ExactSubdivision,
    boundary_face: BoundaryFaceId,
) -> Result<Vec<ExactSimpleRing>> {
    let topology = &subdivision.topology;
    let walk = &topology.face_slots[topology.face_starts[boundary_face.0 as usize] as usize
        ..topology.face_starts[boundary_face.0 as usize + 1] as usize];
    let direct_vertices: Vec<_> = walk
        .iter()
        .map(|&slot| VertexId(topology.owners[slot as usize]))
        .collect();
    let direct_edges: Vec<_> = walk
        .iter()
        .map(|&slot| EdgeId(subdivision.edge_of_slot[slot as usize]))
        .collect();
    if direct_vertices
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        == direct_vertices.len()
    {
        let mut vertices = direct_vertices;
        let mut edges = direct_edges;
        vertices.push(vertices[0]);
        let points: Vec<_> = vertices[..vertices.len() - 1]
            .iter()
            .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone())
            .collect();
        let orientation = exact::cycle_orientation(&points)?;
        canonicalize_ring(&mut vertices, &mut edges);
        return Ok(vec![ExactSimpleRing {
            face: boundary_face,
            vertices: vertices.into_boxed_slice(),
            edges: edges.into_boxed_slice(),
            orientation,
        }]);
    }
    let mut path = Vec::<VertexId>::new();
    let mut path_edges = Vec::<EdgeId>::new();
    let mut positions = vec![None; subdivision.vertices.len()];
    let mut result = Vec::new();
    for step in 0..=walk.len() {
        let slot = walk[step % walk.len()] as usize;
        let vertex = VertexId(topology.owners[slot]);
        let incoming = (step > 0)
            .then(|| EdgeId(subdivision.edge_of_slot[walk[(step - 1) % walk.len()] as usize]));
        if let Some(position) = positions[vertex.0 as usize] {
            let mut vertices = path[position..].to_vec();
            vertices.push(vertex);
            let mut edges = path_edges[position..].to_vec();
            edges.push(incoming.expect("repeated boundary vertex has an incoming edge"));
            if vertices.len() < 4 {
                return Err(voronoi_error("pinch split produced a short exact ring"));
            }
            for removed in &path[position + 1..] {
                positions[removed.0 as usize] = None;
            }
            path.truncate(position + 1);
            path_edges.truncate(position);
            let points: Vec<_> = vertices[..vertices.len() - 1]
                .iter()
                .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone())
                .collect();
            let orientation = exact::cycle_orientation(&points)?;
            canonicalize_ring(&mut vertices, &mut edges);
            result.push(ExactSimpleRing {
                face: boundary_face,
                vertices: vertices.into_boxed_slice(),
                edges: edges.into_boxed_slice(),
                orientation,
            });
        } else {
            positions[vertex.0 as usize] = Some(path.len());
            path.push(vertex);
            if let Some(edge) = incoming {
                path_edges.push(edge);
            }
        }
    }
    if path.len() != 1 || !path_edges.is_empty() {
        return Err(voronoi_error(
            "pinch split left a residual exact boundary path",
        ));
    }
    Ok(result)
}

fn assemble_owner_parts(
    subdivision: &ExactSubdivision,
    owner: SiteId,
    walks: impl IntoIterator<Item = BoundaryFaceId>,
    budget: &mut ExpansionBudget,
    parts: &mut Vec<ExactCellPart>,
) -> Result<()> {
    let mut rings = Vec::new();
    for orbit in walks {
        rings.extend(split_boundary_face(subdivision, orbit)?);
    }
    if rings.is_empty() {
        return Ok(());
    }
    let positives: Vec<_> = rings
        .iter()
        .enumerate()
        .filter(|(_, ring)| ring.orientation == ExactSign::Positive)
        .map(|(index, _)| index)
        .collect();
    let negatives: Vec<_> = rings
        .iter()
        .enumerate()
        .filter(|(_, ring)| ring.orientation == ExactSign::Negative)
        .map(|(index, _)| index)
        .collect();
    let mut ring_points = Vec::new();
    reserve_exact(&mut ring_points, rings.len())?;
    for ring in &rings {
        let count = ring.vertices.len().saturating_sub(1);
        budget.add(count)?;
        let mut points = Vec::new();
        reserve_exact(&mut points, count)?;
        points.extend(
            ring.vertices[..count]
                .iter()
                .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone()),
        );
        ring_points.push(points);
    }
    let mut indexed_rings = Vec::new();
    let mut unindexed_rings = Vec::new();
    for (index, points) in ring_points.iter().enumerate() {
        if let Some(envelope) = exact_envelope(points) {
            indexed_rings.push(IndexedExactEnvelope { index, envelope });
        } else {
            unindexed_rings.push(index);
        }
    }
    let ring_tree = BulkRTree::bulk_load_with_params(indexed_rings);
    for &positive in &positives {
        let candidates = exact_envelope_candidates(
            &ring_tree,
            &unindexed_rings,
            exact_envelope(std::slice::from_ref(&ring_points[positive][0])),
            rings.len(),
        );
        for other in 0..rings.len() {
            if other == positive || !candidates[other] {
                continue;
            }
            budget.add(
                ring_points[positive]
                    .len()
                    .saturating_mul(ring_points[other].len()),
            )?;
            let classifications: Vec<_> = ring_points[positive]
                .iter()
                .map(|point| exact::point_in_cycle(&ring_points[other], point))
                .collect();
            if !classifications.contains(&exact::PointInCycle::Outside)
                && classifications.contains(&exact::PointInCycle::Inside)
            {
                return Err(voronoi_error(
                    "exact same-face ring forest has depth greater than one",
                ));
            }
        }
    }
    let mut holes_by_shell = vec![Vec::new(); positives.len()];
    for negative in negatives {
        let candidate_rings = exact_envelope_candidates(
            &ring_tree,
            &unindexed_rings,
            exact_envelope(std::slice::from_ref(&ring_points[negative][0])),
            rings.len(),
        );
        budget.add(
            positives
                .iter()
                .filter(|positive| candidate_rings[**positive])
                .map(|positive| {
                    ring_points[negative]
                        .len()
                        .saturating_mul(ring_points[*positive].len())
                })
                .sum(),
        )?;
        let candidates: Vec<_> = positives
            .iter()
            .enumerate()
            .filter(|(_, positive)| candidate_rings[**positive])
            .filter(|(_, positive)| {
                let classifications: Vec<_> = ring_points[negative]
                    .iter()
                    .map(|point| exact::point_in_cycle(&ring_points[**positive], point))
                    .collect();
                !classifications.contains(&exact::PointInCycle::Outside)
                    && classifications.contains(&exact::PointInCycle::Inside)
            })
            .map(|(slot, _)| slot)
            .collect();
        if candidates.is_empty() {
            return Err(voronoi_error(
                "negative exact face ring has no same-face shell",
            ));
        }
        budget.add(
            candidates
                .iter()
                .map(|slot| ring_points[positives[*slot]].len())
                .sum::<usize>()
                .saturating_mul(2),
        )?;
        let mut candidates = candidates.into_iter();
        let mut parent = candidates.next().expect("nonempty ring candidates");
        for candidate in candidates {
            let parent_point =
                &subdivision.vertices[rings[positives[parent]].vertices[0].0 as usize].point;
            if exact::point_in_cycle(&ring_points[positives[candidate]], parent_point)
                == exact::PointInCycle::Inside
            {
                continue;
            }
            let candidate_point =
                &subdivision.vertices[rings[positives[candidate]].vertices[0].0 as usize].point;
            if exact::point_in_cycle(&ring_points[positives[parent]], candidate_point)
                != exact::PointInCycle::Inside
            {
                return Err(voronoi_error("exact face ring forest is non-laminar"));
            }
            parent = candidate;
        }
        holes_by_shell[parent].push(negative);
    }
    let mut rings: Vec<Option<ExactSimpleRing>> = rings.into_iter().map(Some).collect();
    for (slot, positive) in positives.into_iter().enumerate() {
        let shell = rings[positive].take().unwrap();
        let holes: Vec<_> = holes_by_shell[slot]
            .iter()
            .map(|&hole| rings[hole].take().unwrap())
            .collect();
        if walk_label(subdivision, shell.face).owner != Some(owner)
            || holes
                .iter()
                .any(|hole| walk_label(subdivision, hole.face).owner != Some(owner))
        {
            return Err(voronoi_error("exact ring escaped its owner group"));
        }
        let mut edge_ids = std::collections::BTreeSet::new();
        if shell
            .edges
            .iter()
            .chain(holes.iter().flat_map(|hole| hole.edges.iter()))
            .any(|edge| !edge_ids.insert(*edge))
        {
            return Err(voronoi_error(
                "exact face shell and holes share an atomic edge",
            ));
        }
        let exact_rings: Vec<Vec<_>> = std::iter::once(&shell)
            .chain(holes.iter())
            .map(|ring| {
                ring.vertices[..ring.vertices.len() - 1]
                    .iter()
                    .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone())
                    .collect()
            })
            .collect();
        let exact_ring_refs: Vec<_> = exact_rings.iter().map(Vec::as_slice).collect();
        validate_exact_cycles(&exact_ring_refs, 0, budget)?;
        budget.add(1)?;
        parts.push(ExactCellPart {
            owner,
            face: shell.face,
            shell,
            holes,
        });
    }
    Ok(())
}

fn assemble_cells(
    subdivision: &ExactSubdivision,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ExactCellPart>> {
    let mut selected = Vec::new();
    reserve_exact(&mut selected, subdivision.labels.len())?;
    for (index, label) in subdivision.labels.iter().enumerate() {
        if !selected_cell_label(*label) {
            continue;
        }
        let owner = label
            .owner
            .ok_or_else(|| voronoi_error("selected exact walk has no owner"))?;
        selected.push((owner, BoundaryFaceId(index as u32)));
    }
    selected.sort_unstable();
    let mut parts = Vec::new();
    let mut start = 0;
    while start < selected.len() {
        let owner = selected[start].0;
        let mut end = start + 1;
        while end < selected.len() && selected[end].0 == owner {
            end += 1;
        }
        assemble_owner_parts(
            subdivision,
            owner,
            selected[start..end].iter().map(|(_, face)| *face),
            budget,
            &mut parts,
        )?;
        start = end;
    }
    parts.sort_by(|left, right| {
        left.owner
            .cmp(&right.owner)
            .then_with(|| left.shell.vertices.cmp(&right.shell.vertices))
    });
    Ok(parts)
}

fn walk_separator_from(
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    incident: &std::collections::BTreeMap<VertexId, Vec<(usize, VertexId)>>,
    used: &mut [bool],
    start: VertexId,
    budget: &mut ExpansionBudget,
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut current = start;
    let mut vertices = vec![current];
    let mut chain_edges = Vec::new();
    loop {
        let next = incident
            .get(&current)
            .into_iter()
            .flatten()
            .find(|(slot, _)| !used[*slot]);
        let Some(&(slot, next_vertex)) = next else {
            break;
        };
        used[slot] = true;
        chain_edges.push(group[slot].1);
        current = next_vertex;
        vertices.push(current);
    }
    if vertices.len() < 2 {
        return Err(voronoi_error("empty Voronoi edge chain"));
    }
    if vertices.first() == vertices.last() {
        return Err(voronoi_error("Voronoi separator forms a closed cycle"));
    }
    let mut reversed = vertices.clone();
    reversed.reverse();
    if reversed < vertices {
        vertices = reversed;
        chain_edges.reverse();
    }
    budget.add(vertices.len())?;
    result.push(ExactEdgeChain {
        separator,
        vertices: vertices.into_boxed_slice(),
        edges: chain_edges.into_boxed_slice(),
    });
    Ok(())
}

fn assemble_separator_group(
    subdivision: &ExactSubdivision,
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    budget: &mut ExpansionBudget,
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut incident: std::collections::BTreeMap<VertexId, Vec<(usize, VertexId)>> =
        std::collections::BTreeMap::new();
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        let [left, right] = subdivision.edges[edge_id.0 as usize].endpoints;
        incident.entry(left).or_default().push((slot, right));
        incident.entry(right).or_default().push((slot, left));
    }
    if incident.values().any(|neighbors| neighbors.len() > 2) {
        return Err(voronoi_error("Voronoi separator branches"));
    }
    let mut used = vec![false; group.len()];
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let [left, right] = subdivision.edges[edge_id.0 as usize].endpoints;
        let start = if incident
            .get(&left)
            .is_some_and(|neighbors| neighbors.len() == 1)
        {
            left
        } else if incident
            .get(&right)
            .is_some_and(|neighbors| neighbors.len() == 1)
        {
            right
        } else {
            continue;
        };
        walk_separator_from(
            separator, group, &incident, &mut used, start, budget, result,
        )?;
    }
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let start = subdivision.edges[edge_id.0 as usize].endpoints[0];
        walk_separator_from(
            separator, group, &incident, &mut used, start, budget, result,
        )?;
    }
    Ok(())
}

fn assemble_edges(
    subdivision: &ExactSubdivision,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ExactEdgeChain>> {
    let mut forward_slot = Vec::new();
    forward_slot
        .try_reserve(subdivision.edges.len())
        .map_err(|_| voronoi_error("could not allocate exact edge slots"))?;
    forward_slot.resize(subdivision.edges.len(), u32::MAX);
    for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
        let atomic = &subdivision.edges[edge as usize];
        if subdivision.topology.owners[slot] == atomic.endpoints[0].0 {
            forward_slot[edge as usize] = slot as u32;
        }
    }
    let mut selected: Vec<_> = subdivision
        .edges
        .iter()
        .enumerate()
        .filter_map(|(index, edge)| {
            let separator = edge.separator?;
            let slot = forward_slot[index] as usize;
            debug_assert_ne!(slot, u32::MAX as usize);
            let left = walk_label(
                subdivision,
                BoundaryFaceId(subdivision.topology.face_of[slot]),
            );
            let twin = subdivision.topology.twins[slot] as usize;
            let right = walk_label(
                subdivision,
                BoundaryFaceId(subdivision.topology.face_of[twin]),
            );
            (selected_cell_label(left) || selected_cell_label(right))
                .then_some((separator, EdgeId(index as u32)))
        })
        .collect();
    selected.sort_unstable();
    let mut result = Vec::new();
    let mut start = 0;
    while start < selected.len() {
        let separator = selected[start].0;
        let mut end = start + 1;
        while end < selected.len() && selected[end].0 == separator {
            end += 1;
        }
        assemble_separator_group(
            subdivision,
            separator,
            &selected[start..end],
            budget,
            &mut result,
        )?;
        start = end;
    }
    result.sort_by(|left, right| {
        left.separator
            .cmp(&right.separator)
            .then_with(|| left.vertices.cmp(&right.vertices))
    });
    Ok(result)
}

pub(super) struct EmbeddedVoronoi {
    polygons: Vec<Polygon>,
    edges: EmbeddedEdges,
    clip: Shape,
}

/// Voronoi edges before any line geometry exists. Exact and collinear lanes
/// emit one packed XY column; the dual lane certifies cancellation on raw
/// segments and packs only when a caller asks for a column or rows.
#[derive(Clone)]
pub(super) enum EmbeddedEdges {
    Packed {
        coords: CoordSeq,
        offsets: CsrOffsetColumn,
    },
    Segments(Vec<[XY; 2]>),
}

pub(super) fn empty_packed_edges() -> Result<EmbeddedEdges> {
    Ok(EmbeddedEdges::Packed {
        coords: CoordSeq::try_from_vecs(Vec::new(), Vec::new(), None, None)?,
        offsets: CsrOffsetColumn::try_new(vec![0], 0)?,
    })
}

fn pack_separator_chains(
    chains: &[ExactEdgeChain],
    xy_of: impl Fn(VertexId) -> Result<XY>,
) -> Result<EmbeddedEdges> {
    let vertices: usize = chains.iter().map(|chain| chain.vertices.len()).sum();
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    reserve_exact(&mut xs, vertices)?;
    reserve_exact(&mut ys, vertices)?;
    let mut ends = Vec::new();
    reserve_exact(&mut ends, chains.len().saturating_add(1))?;
    ends.push(0);
    for chain in chains {
        for &vertex in chain.vertices.iter() {
            let point = xy_of(vertex)?;
            xs.push(point.x);
            ys.push(point.y);
        }
        ends.push(xs.len());
    }
    Ok(EmbeddedEdges::Packed {
        coords: CoordSeq::try_from_vecs(xs, ys, None, None)?,
        offsets: CsrOffsetColumn::try_new(ends, vertices)?,
    })
}

impl EmbeddedEdges {
    /// Canonical form: each dual segment runs from its lexicographically
    /// smaller endpoint, and the list is ordered by both endpoints. Packed
    /// chains keep their construction order.
    fn canonical(self) -> Self {
        match self {
            Self::Packed { coords, offsets } => Self::Packed { coords, offsets },
            Self::Segments(mut segments) => {
                for segment in &mut segments {
                    if lex_xy(segment[0], segment[1]).is_gt() {
                        segment.swap(0, 1);
                    }
                }
                segments.sort_unstable_by(|left, right| {
                    lex_xy(left[0], right[0]).then(lex_xy(left[1], right[1]))
                });
                Self::Segments(segments)
            },
        }
    }

    pub(super) fn into_line_seqs(self) -> Result<Vec<LineSeq>> {
        match self.canonical() {
            Self::Packed { coords, offsets } => {
                let ends = offsets.as_slice();
                if ends.len() <= 1 {
                    return Ok(Vec::new());
                }
                (0..ends.len() - 1)
                    .map(|line| {
                        let start = ends[line] as usize;
                        let end = ends[line + 1] as usize;
                        let xs = coords.xs()[start..end].to_vec();
                        let ys = coords.ys()[start..end].to_vec();
                        LineSeq::try_new(CoordSeq::try_from_vecs(xs, ys, None, None)?)
                    })
                    .collect()
            },
            Self::Segments(segments) => segments
                .into_iter()
                .map(|[a, b]| {
                    LineSeq::try_new(CoordSeq::try_from_vecs(
                        vec![a.x, b.x],
                        vec![a.y, b.y],
                        None,
                        None,
                    )?)
                })
                .collect(),
        }
    }

    /// One packed XY line column with CSR vertex offsets, built directly:
    /// no per-edge geometry, no per-edge allocation.
    pub(super) fn into_line_column(self) -> Result<(CoordSeq, CsrOffsetColumn)> {
        match self.canonical() {
            Self::Packed { coords, offsets } => Ok((coords, offsets)),
            Self::Segments(segments) => {
                let vertices = segments.len() * 2;
                let mut xs = Vec::with_capacity(vertices);
                let mut ys = Vec::with_capacity(vertices);
                for [a, b] in &segments {
                    xs.extend([a.x, b.x]);
                    ys.extend([a.y, b.y]);
                }
                let ends = (0..=segments.len()).map(|edge| edge * 2).collect();
                Ok((
                    CoordSeq::try_from_vecs(xs, ys, None, None)?,
                    CsrOffsetColumn::try_new(ends, vertices)?,
                ))
            },
        }
    }
}

impl EmbeddedVoronoi {
    pub(super) fn into_polygons(self) -> Vec<Polygon> {
        self.polygons
    }

    pub(super) fn into_edges(self) -> EmbeddedEdges {
        self.edges
    }
}

fn marked_site_candidates(
    segments: &[(EdgeId, [XY; 2])],
    sites: &[Site],
    budget: &mut ExpansionBudget,
) -> Result<Vec<(EdgeId, SiteId)>> {
    let mut indexed = Vec::new();
    indexed.reserve_exact(sites.len());
    indexed.extend(sites.iter().enumerate().map(|(index, site)| {
        rstar::primitives::GeomWithData::new([site.point.x, site.point.y], SiteId(index as u32))
    }));
    let tree = BulkRTree::bulk_load_with_params(indexed);
    let mut candidates = Vec::new();
    for &(edge, endpoints) in segments {
        let query = rstar::AABB::from_corners(
            [
                endpoints[0].x.min(endpoints[1].x),
                endpoints[0].y.min(endpoints[1].y),
            ],
            [
                endpoints[0].x.max(endpoints[1].x),
                endpoints[0].y.max(endpoints[1].y),
            ],
        );
        for site in tree.locate_in_envelope_intersecting(query) {
            budgeted_try_push(&mut candidates, (edge, site.data), budget)?;
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    Ok(candidates)
}

fn fan_target(subdivision: &ExactSubdivision, edge: EdgeId, vertex: VertexId) -> VertexId {
    let endpoints = subdivision.edges[edge.0 as usize].endpoints;
    if endpoints[0] == vertex {
        endpoints[1]
    } else {
        endpoints[0]
    }
}

fn rounded_fan_angle_cmp(origin: XY, left: XY, right: XY) -> Result<Ordering> {
    let half = |target: XY| target.y > origin.y || (target.y == origin.y && target.x >= origin.x);
    match (half(left), half(right)) {
        (true, false) => Ok(Ordering::Less),
        (false, true) => Ok(Ordering::Greater),
        _ => match exact::orient2d(origin, left, right) {
            ExactSign::Positive => Ok(Ordering::Less),
            ExactSign::Negative => Ok(Ordering::Greater),
            ExactSign::Zero => Err(voronoi_error("rounded Voronoi fan contains an angular tie")),
        },
    }
}

fn sort_fan(
    vertex: VertexId,
    incident: &[EdgeId],
    rounded: bool,
    subdivision: &ExactSubdivision,
    exact_to_embedded: &[Option<EmbeddedVertexId>],
    points: &[XY],
    order: &mut Vec<EdgeId>,
) -> Result<()> {
    let embedded_point = |vertex: VertexId| -> Result<XY> {
        let embedded = exact_to_embedded[vertex.0 as usize]
            .ok_or_else(|| voronoi_error("live edge has a non-live endpoint"))?;
        Ok(points[embedded.0 as usize])
    };
    let exact_origin = &subdivision.vertices[vertex.0 as usize].point;
    let rounded_origin = rounded.then(|| embedded_point(vertex)).transpose()?;
    let compare = |left: EdgeId, right: EdgeId| -> Result<Ordering> {
        let left = fan_target(subdivision, left, vertex);
        let right = fan_target(subdivision, right, vertex);
        if let Some(origin) = rounded_origin {
            rounded_fan_angle_cmp(origin, embedded_point(left)?, embedded_point(right)?)
        } else {
            exact::angle_ccw_cmp(
                exact_origin,
                &subdivision.vertices[left.0 as usize].point,
                &subdivision.vertices[right.0 as usize].point,
            )
        }
    };

    order.clear();
    order.extend_from_slice(incident);
    for index in 1..order.len() {
        let mut cursor = index;
        while cursor > 0 && compare(order[cursor], order[cursor - 1])? == Ordering::Less {
            order.swap(cursor, cursor - 1);
            cursor -= 1;
        }
    }
    for pair in order.windows(2) {
        compare(pair[0], pair[1])?;
    }
    if order.len() > 1 {
        compare(*order.last().unwrap(), order[0])?;
    }
    if let Some((least, _)) = order.iter().enumerate().min_by_key(|(_, edge)| *edge) {
        order.rotate_left(least);
    }
    Ok(())
}

fn embed(
    subdivision: &ExactSubdivision,
    parts: Vec<ExactCellPart>,
    chains: &[ExactEdgeChain],
    clip: &NormalizedClip<'_>,
    sites: &[Site],
    require_cells: bool,
    budget: &mut ExpansionBudget,
) -> Result<EmbeddedVoronoi> {
    let mut live = vec![false; subdivision.vertices.len()];
    let mut live_edges = std::collections::BTreeSet::new();
    for part in &parts {
        live_edges.extend(part.shell.edges.iter().copied());
        live_edges.extend(
            part.holes
                .iter()
                .flat_map(|hole| hole.edges.iter().copied()),
        );
        for vertex in part
            .shell
            .vertices
            .iter()
            .chain(part.holes.iter().flat_map(|hole| hole.vertices.iter()))
        {
            live[vertex.0 as usize] = true;
        }
    }
    for chain in chains {
        live_edges.extend(chain.edges.iter().copied());
        for vertex in &chain.vertices {
            live[vertex.0 as usize] = true;
        }
    }
    for (edge_index, edge) in subdivision.edges.iter().enumerate() {
        if !edge.clip_sources.is_empty() {
            live_edges.insert(EdgeId(edge_index as u32));
            live[edge.endpoints[0].0 as usize] = true;
            live[edge.endpoints[1].0 as usize] = true;
        }
    }
    for point in clip.rings.iter().flatten() {
        let vertex = subdivision
            .vertices
            .iter()
            .position(|candidate| candidate.point.same_position(point))
            .ok_or_else(|| voronoi_error("exact clip vertex is absent from the snap arena"))?;
        live[vertex] = true;
    }
    let mut points = Vec::new();
    let mut exact_to_embedded = vec![None; subdivision.vertices.len()];
    let mut keys = std::collections::BTreeSet::new();
    for (index, vertex) in subdivision.vertices.iter().enumerate() {
        if !live[index] {
            continue;
        }
        let point = vertex
            .fixed_xy
            .map_or_else(|| vertex.point.round_nearest_even(), Ok)?;
        if !keys.insert(PointKey::new(point)) {
            return Err(embedding_failure());
        }
        let embedded = EmbeddedVertexId(points.len() as u32);
        exact_to_embedded[index] = Some(embedded);
        budgeted_try_push(&mut points, point, budget)?;
    }
    let embedded_point = |vertex: VertexId| -> Result<XY> {
        let embedded = exact_to_embedded[vertex.0 as usize]
            .ok_or_else(|| voronoi_error("live edge has a non-live endpoint"))?;
        Ok(points[embedded.0 as usize])
    };
    let rounded_exact = |vertex| embedded_point(vertex).map(exact::ExactPoint::from_xy);
    let live_edge_ids: Vec<_> = live_edges.iter().copied().collect();
    let marked_segments = live_edge_ids
        .iter()
        .filter_map(|&edge_id| {
            let edge = &subdivision.edges[edge_id.0 as usize];
            edge.separator.is_some().then(|| {
                Ok((edge_id, [
                    embedded_point(edge.endpoints[0])?,
                    embedded_point(edge.endpoints[1])?,
                ]))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let marked_candidates = marked_site_candidates(&marked_segments, sites, budget)?;
    let mut marked_start = 0;
    for &edge_id in &live_edges {
        let edge = &subdivision.edges[edge_id.0 as usize];
        let rounded = [
            rounded_exact(edge.endpoints[0])?,
            rounded_exact(edge.endpoints[1])?,
        ];
        if rounded[0].same_position(&rounded[1]) {
            return Err(embedding_failure());
        }
        if edge.separator.is_none() {
            continue;
        }
        let check_left = |site: SiteId| {
            exact::orient_points(
                &rounded[0],
                &rounded[1],
                &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
            ) == ExactSign::Positive
        };
        let check_right = |site: SiteId| {
            exact::orient_points(
                &rounded[0],
                &rounded[1],
                &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
            ) == ExactSign::Negative
        };
        match edge.forward.owner {
            OwnerTransition::Boundary {
                left: Some(left),
                right: Some(right),
            } => {
                if !check_left(left) || !check_right(right) {
                    return Err(embedding_failure());
                }
            },
            OwnerTransition::Boundary {
                left: Some(left),
                right: None,
            } => {
                if !check_left(left) {
                    return Err(embedding_failure());
                }
            },
            OwnerTransition::Boundary {
                left: None,
                right: Some(right),
            } => {
                if !check_right(right) {
                    return Err(embedding_failure());
                }
            },
            OwnerTransition::Boundary {
                left: None,
                right: None,
            }
            | OwnerTransition::Preserve => {
                return Err(voronoi_error("live separator has incomplete ownership"));
            },
        }
        while marked_start < marked_candidates.len() && marked_candidates[marked_start].0 < edge_id
        {
            marked_start += 1;
        }
        let mut candidate = marked_start;
        while candidate < marked_candidates.len() && marked_candidates[candidate].0 == edge_id {
            let site = &sites[marked_candidates[candidate].1.0 as usize];
            if exact::segment_intersection(
                &rounded[0],
                &rounded[1],
                &exact::ExactPoint::from_xy(site.point.xy()),
                &exact::ExactPoint::from_xy(site.point.xy()),
            )
            .is_point()
            {
                return Err(embedding_failure());
            }
            candidate += 1;
        }
    }
    let mut emitted_incidence = Vec::new();
    for &edge_id in &live_edge_ids {
        let edge = &subdivision.edges[edge_id.0 as usize];
        budgeted_try_push(
            &mut emitted_incidence,
            ([edge.endpoints[0].0, edge.endpoints[1].0], Segment {
                start: embedded_point(edge.endpoints[0])?,
                end: embedded_point(edge.endpoints[1])?,
            }),
            budget,
        )?;
    }
    crate::geometry::overlay::certify_emitted_incidence(&emitted_incidence)
        .map_err(|_| embedding_failure())?;
    budget.add(live_edge_ids.len().saturating_mul(2))?;
    let mut incidence_offsets = vec![0_usize; subdivision.vertices.len() + 1];
    for &edge_id in &live_edge_ids {
        for endpoint in subdivision.edges[edge_id.0 as usize].endpoints {
            incidence_offsets[endpoint.0 as usize + 1] += 1;
        }
    }
    for index in 1..incidence_offsets.len() {
        incidence_offsets[index] = incidence_offsets[index]
            .checked_add(incidence_offsets[index - 1])
            .ok_or_else(|| voronoi_error("too many live edge incidences"))?;
    }
    let mut incident_edges = Vec::new();
    incident_edges.reserve_exact(live_edge_ids.len().saturating_mul(2));
    incident_edges.resize(live_edge_ids.len().saturating_mul(2), EdgeId(0));
    let mut cursors = incidence_offsets[..subdivision.vertices.len()].to_vec();
    for &edge_id in &live_edge_ids {
        for endpoint in subdivision.edges[edge_id.0 as usize].endpoints {
            let cursor = &mut cursors[endpoint.0 as usize];
            incident_edges[*cursor] = edge_id;
            *cursor += 1;
        }
    }
    let mut exact_order = Vec::new();
    let mut rounded_order = Vec::new();
    for vertex_index in 0..subdivision.vertices.len() {
        let vertex = VertexId(vertex_index as u32);
        if !live[vertex_index] {
            continue;
        }
        let incident =
            &incident_edges[incidence_offsets[vertex_index]..incidence_offsets[vertex_index + 1]];
        sort_fan(
            vertex,
            incident,
            false,
            subdivision,
            &exact_to_embedded,
            &points,
            &mut exact_order,
        )?;
        sort_fan(
            vertex,
            incident,
            true,
            subdivision,
            &exact_to_embedded,
            &points,
            &mut rounded_order,
        )?;
        if exact_order != rounded_order {
            return Err(embedding_failure());
        }
    }
    let published_coordinates = parts
        .iter()
        .map(|part| {
            part.shell.vertices.len()
                + part
                    .holes
                    .iter()
                    .map(|hole| hole.vertices.len())
                    .sum::<usize>()
        })
        .sum::<usize>()
        .saturating_add(chains.iter().map(|chain| chain.vertices.len()).sum());
    let published_containers = parts
        .iter()
        .map(|part| 2_usize.saturating_add(part.holes.len()))
        .sum::<usize>()
        .saturating_add(chains.len());
    budget.add(published_coordinates.saturating_add(published_containers))?;
    let coordinates = |vertices: &[VertexId]| -> Result<Vec<Point>> {
        let mut coordinates = vertices
            .iter()
            .map(|vertex| {
                let embedded = exact_to_embedded[vertex.0 as usize]
                    .ok_or_else(|| voronoi_error("non-live exact vertex was rendered"))?;
                let point = points[embedded.0 as usize];
                Ok(Point::new_raw_xy(point.x, point.y))
            })
            .collect::<Result<Vec<_>>>()?;
        // A closed ring starts at its lexicographically smallest rounded
        // vertex: the canonical form both lanes publish. Open edge chains
        // pass through unchanged.
        if coordinates.len() > 3 && coordinates[0].xy() == coordinates[coordinates.len() - 1].xy() {
            let open = coordinates.len() - 1;
            let start = (0..open)
                .min_by(|&left, &right| lex_xy(coordinates[left].xy(), coordinates[right].xy()))
                .expect("non-empty ring");
            coordinates.pop();
            coordinates.rotate_left(start);
            coordinates.push(coordinates[0]);
        }
        Ok(coordinates)
    };
    let mut labelled = Vec::new();
    for part in parts {
        if walk_label(subdivision, part.face).owner != Some(part.owner) {
            return Err(voronoi_error("embedded cell lost its exact face owner"));
        }
        let shell_coordinates = coordinates(&part.shell.vertices)?;
        let rounded_shell: Vec<_> = shell_coordinates[..shell_coordinates.len() - 1]
            .iter()
            .map(|point| exact::ExactPoint::from_xy(point.xy()))
            .collect();
        if exact::cycle_orientation(&rounded_shell)? != part.shell.orientation {
            return Err(embedding_failure());
        }
        let shell = Ring::closed(shell_coordinates)?;
        let holes = part
            .holes
            .iter()
            .map(|hole| {
                let coordinates = coordinates(&hole.vertices)?;
                let rounded: Vec<_> = coordinates[..coordinates.len() - 1]
                    .iter()
                    .map(|point| exact::ExactPoint::from_xy(point.xy()))
                    .collect();
                if exact::cycle_orientation(&rounded)? != hole.orientation {
                    return Err(embedding_failure());
                }
                Ring::closed(coordinates)
            })
            .collect::<Result<Vec<_>>>()?;
        let polygon = Polygon::new(shell, holes)?;
        if !polygon_is_valid(&polygon)? {
            return Err(embedding_failure());
        }
        labelled.push((part.owner, polygon));
    }
    let polygons: Vec<_> = labelled
        .iter()
        .map(|(_, polygon)| polygon.clone())
        .collect();
    let embedded_ring = |ring: &[exact::ExactPoint]| -> Result<Ring> {
        let mut coordinates = Vec::with_capacity(ring.len() + 1);
        for point in ring {
            let vertex = subdivision
                .vertices
                .iter()
                .position(|candidate| candidate.point.same_position(point))
                .ok_or_else(|| voronoi_error("generated clip vertex is not live"))?;
            let embedded = exact_to_embedded[vertex]
                .ok_or_else(|| voronoi_error("generated clip vertex is not embedded"))?;
            let xy = points[embedded.0 as usize];
            coordinates.push(Point::new_raw_xy(xy.x, xy.y));
        }
        coordinates.push(coordinates[0]);
        Ring::closed(coordinates)
    };
    let shell = embedded_ring(&clip.rings[0])?;
    let holes = clip.rings[1..]
        .iter()
        .map(|ring| embedded_ring(ring))
        .collect::<Result<Vec<_>>>()?;
    let comparison_clip = Shape::Polygon(Polygon::new(shell, holes)?);
    if let Shape::Polygon(polygon) = &comparison_clip {
        admit_embedded_clip(polygon)?;
    }
    if let Some(source) = clip.source {
        let source = Shape::Polygon(source.clone());
        let source_gap = source.difference(&comparison_clip, Strictness::Lenient)?;
        let source_spill = comparison_clip.difference(&source, Strictness::Lenient)?;
        if !source_gap.is_empty() || !source_spill.is_empty() {
            return Err(voronoi_error(
                "embedded Voronoi clip differs topologically from its source",
            ));
        }
    };
    if require_cells && labelled.is_empty() {
        return Err(voronoi_error(
            "exact Voronoi subdivision produced no labelled cells",
        ));
    }
    if !labelled.is_empty() {
        let shape_rows: Vec<_> = polygons.iter().cloned().map(Shape::Polygon).collect();
        if !coverage_is_valid(&shape_rows, 0.0)? {
            return Err(embedding_failure());
        }
        budget.add(shape_rows.len())?;
        let mut row_entries = Vec::new();
        row_entries.reserve_exact(shape_rows.len());
        row_entries.extend(shape_rows.iter().enumerate().map(|(row, shape)| {
            let bounds = shape.bounds().expect("admitted polygon has bounds");
            rstar::primitives::GeomWithData::new(
                rstar::primitives::Rectangle::from_corners([bounds.minx(), bounds.miny()], [
                    bounds.maxx(),
                    bounds.maxy(),
                ]),
                row,
            )
        }));
        let row_index: BulkRTree<
            rstar::primitives::GeomWithData<rstar::primitives::Rectangle<[f64; 2]>, usize>,
        > = BulkRTree::bulk_load_with_params(row_entries);
        let clip_boundary_segments = match &comparison_clip {
            Shape::Polygon(polygon) => polygon
                .shell
                .len()
                .saturating_add(polygon.holes.iter().map(|hole| hole.len()).sum()),
            _ => unreachable!("comparison clip is a polygon"),
        };
        budget.add(
            sites
                .len()
                .saturating_mul(1_usize.saturating_add(clip_boundary_segments)),
        )?;
        for (site_index, site) in sites.iter().enumerate() {
            let point = Shape::Point(site.point.clone());
            if !comparison_clip.covers(&point)? {
                continue;
            }
            let mut own = false;
            let query = rstar::AABB::from_corners([site.point.x, site.point.y], [
                site.point.x,
                site.point.y,
            ]);
            let mut candidates = Vec::new();
            for entry in row_index.locate_in_envelope_intersecting(query) {
                budgeted_try_push(&mut candidates, entry.data, budget)?;
            }
            candidates.sort_unstable();
            candidates.dedup();
            for row in candidates {
                let (owner, polygon) = &labelled[row];
                budget.add(
                    1_usize
                        .saturating_add(polygon.shell.len())
                        .saturating_add(polygon.holes.iter().map(|hole| hole.len()).sum::<usize>()),
                )?;
                if Shape::Polygon(polygon.clone()).covers(&point)? {
                    if owner.0 as usize != site_index {
                        return Err(embedding_failure());
                    }
                    own = true;
                }
            }
            if !own {
                return Err(embedding_failure());
            }
        }
        // Exact vertices are rounded once and shared by every incident cell.  The
        // certificates above preserve the subdivision topology, validity, and
        // interior-disjointness, but binary64 overlay may still observe an ULP-scale
        // gap between the rounded partition and the source clip.  That numerical
        // equality is deliberately not part of the topological-snap contract.
    }
    let edges = pack_separator_chains(chains, embedded_point)?;
    Ok(EmbeddedVoronoi {
        polygons,
        edges,
        clip: comparison_clip,
    })
}

pub(super) fn build(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    combinatorial_dual: bool,
    mesh: CertifiedDelaunay,
) -> Result<EmbeddedVoronoi> {
    if sites
        .iter()
        .enumerate()
        .any(|(index, site)| site.id != index)
    {
        return Err(voronoi_error("Voronoi sites are not canonically indexed"));
    }
    let normal_or_zero = sites.iter().all(|site| {
        let xy = site.point.xy();
        (xy.x == 0.0 || xy.x.is_normal()) && (xy.y == 0.0 || xy.y.is_normal())
    });
    if combinatorial_dual
        && sites.len() >= 3
        && normal_or_zero
        && matches!(
            boundary,
            VoronoiBoundary::Envelope | VoronoiBoundary::Padded
        )
    {
        return dual_build(sites, boundary, budget, mesh);
    }
    let clip = normalize_clip(sites, boundary, budget)?;
    if mesh.collinear_order().is_some() {
        return Err(voronoi_error(
            "collinear sites require the exact one-dimensional lane",
        ));
    }
    let complex = delaunay_complex(&mesh, sites)?;
    let primal_edges = mesh.primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len(), budget)?;
    let rect = clip_rectangle(&clip)?;
    let scaffold = build_scaffold(&rect, &clip, budget)?;
    let separators = emit_clipped_separators(sites, &primal_edges, &complex, &rect, budget)?;
    let atoms = node_scaffold_and_separators(
        scaffold, separators, sites, &adjacency, &clip, &rect, budget,
    )?;
    let mut subdivision = build_exact_subdivision(atoms, budget)?;
    graft_components(&mut subdivision, sites, &adjacency, budget)?;
    let parts = assemble_cells(&subdivision, budget)?;
    let chains = assemble_edges(&subdivision, budget)?;
    embed(&subdivision, parts, &chains, &clip, sites, true, budget)
}

pub(super) fn build_edges(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    combinatorial_dual: bool,
    mesh: CertifiedDelaunay,
) -> Result<EmbeddedEdges> {
    Ok(build(sites, boundary, budget, combinatorial_dual, mesh)?.into_edges())
}

fn ring_classify_xy(ring: &[XY], point: XY) -> RingClass {
    let mut inside = false;
    for index in 0..ring.len() {
        let edge = [ring[index], ring[(index + 1) % ring.len()]];
        match ring_edge_membership(edge[0].x, edge[0].y, edge[1].x, edge[1].y, point.x, point.y) {
            RingEdgeMembership::Boundary => return RingClass::Boundary,
            RingEdgeMembership::Crossing => inside = !inside,
            RingEdgeMembership::None => {},
        }
    }
    if inside {
        RingClass::Interior
    } else {
        RingClass::Exterior
    }
}

fn point_covered_by_clip_xy(point: XY, rings: &[Vec<XY>]) -> bool {
    ring_classify_xy(&rings[0], point) != RingClass::Exterior
        && rings[1..]
            .iter()
            .all(|hole| ring_classify_xy(hole, point) != RingClass::Interior)
}

fn point_on_segment_xy(point: XY, start: XY, end: XY) -> bool {
    point.x >= start.x.min(end.x)
        && point.x <= start.x.max(end.x)
        && point.y >= start.y.min(end.y)
        && point.y <= start.y.max(end.y)
        && exact::orient2d(start, end, point) == ExactSign::Zero
}

fn point_covered_by_clip(point: &exact::ExactPoint, rings: &[Vec<exact::ExactPoint>]) -> bool {
    exact::point_in_cycle(&rings[0], point) != exact::PointInCycle::Outside
        && rings[1..]
            .iter()
            .all(|hole| exact::point_in_cycle(hole, point) != exact::PointInCycle::Inside)
}

fn rounded_segment_covered_by_clip(
    segment: Segment,
    exact_rings: &[Vec<exact::ExactPoint>],
    rounded_rings: &[Vec<XY>],
    events: &mut Vec<exact::ExactPoint>,
) -> Result<bool> {
    if !point_covered_by_clip_xy(segment.start, rounded_rings)
        || !point_covered_by_clip_xy(segment.end, rounded_rings)
    {
        return Ok(false);
    }
    let mut interior_contact = false;
    for ring in rounded_rings {
        for index in 0..ring.len() {
            let edge = [ring[index], ring[(index + 1) % ring.len()]];
            let segment_start = exact::orient2d(segment.start, segment.end, edge[0]);
            let segment_end = exact::orient2d(segment.start, segment.end, edge[1]);
            let edge_start = exact::orient2d(edge[0], edge[1], segment.start);
            let edge_end = exact::orient2d(edge[0], edge[1], segment.end);
            if segment_start != ExactSign::Zero
                && segment_end != ExactSign::Zero
                && edge_start != ExactSign::Zero
                && edge_end != ExactSign::Zero
                && segment_start != segment_end
                && edge_start != edge_end
            {
                return Ok(false);
            }
            if (point_on_segment_xy(edge[0], segment.start, segment.end)
                && edge[0] != segment.start
                && edge[0] != segment.end)
                || (point_on_segment_xy(edge[1], segment.start, segment.end)
                    && edge[1] != segment.start
                    && edge[1] != segment.end)
            {
                interior_contact = true;
            }
        }
    }
    let start = exact::ExactPoint::from_xy(segment.start);
    let end = exact::ExactPoint::from_xy(segment.end);
    if !interior_contact {
        let midpoint = start.midpoint(&end);
        return Ok(point_covered_by_clip(&midpoint, exact_rings));
    }
    if !point_covered_by_clip(&start, exact_rings) || !point_covered_by_clip(&end, exact_rings) {
        return Ok(false);
    }
    events.clear();
    events.extend([start.clone(), end.clone()]);
    for ring in exact_rings {
        for edge in 0..ring.len() {
            match exact::segment_intersection(
                &start,
                &end,
                &ring[edge],
                &ring[(edge + 1) % ring.len()],
            ) {
                exact::SegmentIntersection::None => {},
                exact::SegmentIntersection::Point(point) => events.push(point),
                exact::SegmentIntersection::Overlap { start, end } => {
                    events.extend([start, end]);
                },
            }
        }
    }
    events.sort_by(|left, right| exact::compare_along(&start, &end, left, right));
    events.dedup_by(|left, right| left.same_position(right));
    for pair in events.windows(2) {
        if exact::compare_along(&start, &end, &pair[0], &pair[1]) != Ordering::Less {
            continue;
        }
        if !point_covered_by_clip(&pair[0].midpoint(&pair[1]), exact_rings) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn build_collinear_edges(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: CertifiedDelaunay,
) -> Result<EmbeddedEdges> {
    let order = mesh
        .collinear_order()
        .ok_or_else(|| voronoi_error("non-collinear sites entered the collinear Voronoi lane"))?;
    if matches!(boundary, VoronoiBoundary::Envelope) {
        let min_x = sites
            .iter()
            .map(|site| site.point.x)
            .reduce(f64::min)
            .unwrap();
        let max_x = sites
            .iter()
            .map(|site| site.point.x)
            .reduce(f64::max)
            .unwrap();
        let min_y = sites
            .iter()
            .map(|site| site.point.y)
            .reduce(f64::min)
            .unwrap();
        let max_y = sites
            .iter()
            .map(|site| site.point.y)
            .reduce(f64::max)
            .unwrap();
        if min_x == max_x || min_y == max_y {
            return empty_packed_edges();
        }
    }
    let clip = normalize_clip(sites, boundary, budget)?;
    let clip_segments = clip.rings.iter().map(Vec::len).sum::<usize>();
    budget.add(order.len().saturating_sub(1).saturating_mul(clip_segments))?;
    let mut exact_chains = Vec::new();
    for pair in order.windows(2) {
        let line = exact::ExactLine::perpendicular_bisector(
            sites[pair[0]].point.xy(),
            sites[pair[1]].point.xy(),
        );
        let mut events = Vec::new();
        for ring in &clip.rings {
            for edge in 0..ring.len() {
                let start = &ring[edge];
                let end = &ring[(edge + 1) % ring.len()];
                let start_sign = exact::signed_line_product(&line, start);
                let end_sign = exact::signed_line_product(&line, end);
                if start_sign == ExactSign::Zero {
                    budgeted_try_push(&mut events, start.clone(), budget)?;
                }
                if end_sign == ExactSign::Zero {
                    budgeted_try_push(&mut events, end.clone(), budget)?;
                }
                if start_sign != ExactSign::Zero
                    && end_sign != ExactSign::Zero
                    && start_sign != end_sign
                {
                    let support = exact::ExactLine::through_points(start, end)?;
                    let hit = exact::line_intersection(&line, &support);
                    if exact::segment_intersection(start, end, &hit, &hit).is_point() {
                        budgeted_try_push(&mut events, hit, budget)?;
                    }
                }
            }
        }
        events.sort_by(exact::ExactPoint::compare_lex);
        events.dedup_by(|left, right| left.same_position(right));
        let mut current: Vec<exact::ExactPoint> = Vec::new();
        for interval in events.windows(2) {
            budget.add(clip_segments)?;
            if interval[0].same_position(&interval[1]) {
                continue;
            }
            let midpoint = interval[0].midpoint(&interval[1]);
            let covered = exact::point_in_cycle(&clip.rings[0], &midpoint)
                != exact::PointInCycle::Outside
                && clip.rings[1..].iter().all(|hole| {
                    exact::point_in_cycle(hole, &midpoint) != exact::PointInCycle::Inside
                });
            if covered {
                if current.is_empty() {
                    budgeted_try_push(&mut current, interval[0].clone(), budget)?;
                }
                budgeted_try_push(&mut current, interval[1].clone(), budget)?;
            } else if current.len() >= 2 {
                budgeted_try_push(
                    &mut exact_chains,
                    ([pair[0], pair[1]], std::mem::take(&mut current)),
                    budget,
                )?;
            } else {
                current.clear();
            }
        }
        if current.len() >= 2 {
            budgeted_try_push(&mut exact_chains, ([pair[0], pair[1]], current), budget)?;
        }
    }
    let separator_points = exact_chains
        .iter()
        .map(|(_, chain)| chain.len())
        .sum::<usize>();
    budget.add(separator_points.saturating_mul(clip_segments))?;
    let mut split_rings = Vec::new();
    reserve_exact(&mut split_rings, clip.rings.len())?;
    for ring in &clip.rings {
        let mut split_ring = Vec::new();
        for edge in 0..ring.len() {
            let start = &ring[edge];
            let end = &ring[(edge + 1) % ring.len()];
            let mut events = Vec::new();
            budgeted_try_push(&mut events, start.clone(), budget)?;
            for point in exact_chains.iter().flat_map(|(_, chain)| chain) {
                if exact::segment_intersection(start, end, point, point).is_point() {
                    budgeted_try_push(&mut events, point.clone(), budget)?;
                }
            }
            events.sort_by(|left, right| exact::compare_along(start, end, left, right));
            events.dedup_by(|left, right| left.same_position(right));
            for point in events {
                if split_ring
                    .last()
                    .is_none_or(|known: &exact::ExactPoint| !known.same_position(&point))
                {
                    budgeted_try_push(&mut split_ring, point, budget)?;
                }
            }
        }
        budgeted_try_push(&mut split_rings, split_ring, budget)?;
    }
    let split_clip = NormalizedClip {
        rings: split_rings,
        fixed: clip.fixed.clone(),
        source: clip.source,
        contacts: Vec::new(),
    };
    let mut exact_points: Vec<_> = exact_chains
        .iter()
        .flat_map(|(_, chain)| chain.iter().cloned())
        .collect();
    exact_points.extend(split_clip.rings.iter().flatten().cloned());
    exact_points.sort_by(exact::ExactPoint::compare_lex);
    exact_points.dedup_by(|left, right| left.same_position(right));
    let fixed = fixed_coordinates(&exact_points, sites, &clip)?;
    let vertices = exact_points
        .iter()
        .zip(fixed)
        .map(|(point, fixed_xy)| ExactVertex {
            point: point.clone(),
            fixed_xy,
        })
        .collect::<Vec<_>>();
    let vertex_id = |point: &exact::ExactPoint| -> VertexId {
        VertexId(
            exact_points
                .binary_search_by(|known| known.compare_lex(point))
                .expect("collinear event was interned") as u32,
        )
    };
    let mut edges = Vec::new();
    let mut chains = Vec::new();
    for ([left, right], mut chain) in exact_chains {
        let ids = [SiteId(left as u32), SiteId(right as u32)];
        let (oriented_start, _) = orient_separator(
            chain.first().unwrap().clone(),
            chain.last().unwrap().clone(),
            ids,
            sites,
        )?;
        if !oriented_start.same_position(chain.first().unwrap()) {
            chain.reverse();
        }
        let mut chain_edges = Vec::new();
        for pair in chain.windows(2) {
            let edge_id = EdgeId(edges.len() as u32);
            edges.push(AtomicEdge {
                endpoints: [vertex_id(&pair[0]), vertex_id(&pair[1])],
                forward: Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: OwnerTransition::Boundary {
                        left: Some(ids[0]),
                        right: Some(ids[1]),
                    },
                },
                separator: Some([ids[0].min(ids[1]), ids[0].max(ids[1])]),
                clip_sources: Box::new([]),
                frame_side: None,
            });
            chain_edges.push(edge_id);
        }
        chains.push(ExactEdgeChain {
            separator: [ids[0].min(ids[1]), ids[0].max(ids[1])],
            vertices: chain.iter().map(vertex_id).collect(),
            edges: chain_edges.into_boxed_slice(),
        });
    }
    let mut clip_source = 0_u32;
    for ring in &split_clip.rings {
        for edge in 0..ring.len() {
            budget.add(1)?;
            edges
                .try_reserve(1)
                .map_err(|_| voronoi_error("could not allocate exact Voronoi subdivision"))?;
            edges.push(AtomicEdge {
                endpoints: [
                    vertex_id(&ring[edge]),
                    vertex_id(&ring[(edge + 1) % ring.len()]),
                ],
                forward: Transition {
                    frame_delta: 0,
                    clip_delta: 1,
                    owner: OwnerTransition::Preserve,
                },
                separator: None,
                clip_sources: vec![ClipSegmentId(clip_source)].into_boxed_slice(),
                frame_side: None,
            });
            clip_source = clip_source
                .checked_add(1)
                .ok_or_else(|| voronoi_error("too many split clip atoms"))?;
        }
    }
    let subdivision = ExactSubdivision {
        vertices,
        edges,
        topology: IndexedTopology::default(),
        edge_of_slot: Vec::new(),
        labels: Vec::new(),
    };
    let embedded = embed(
        &subdivision,
        Vec::new(),
        &chains,
        &split_clip,
        sites,
        false,
        budget,
    )?;
    let (comparison_rings, comparison_rounded_rings): (Vec<Vec<exact::ExactPoint>>, Vec<Vec<XY>>) =
        match &embedded.clip {
            Shape::Polygon(polygon) => std::iter::once(&polygon.shell)
                .chain(polygon.holes.iter())
                .map(|ring| {
                    let mut exact = Vec::new();
                    let mut rounded = Vec::new();
                    for point in ring {
                        rounded.push(point.xy());
                        exact.push(exact::ExactPoint::from_xy(point.xy()));
                    }
                    (exact, rounded)
                })
                .unzip(),
            _ => return Err(voronoi_error("collinear Voronoi clip is not a polygon")),
        };
    let packed = embedded.into_edges();
    let EmbeddedEdges::Packed { coords, offsets } = &packed else {
        return Err(voronoi_error("collinear Voronoi edges are packed chains"));
    };
    let ends = offsets.as_slice();
    let clip_segments = split_clip.rings.iter().map(Vec::len).sum::<usize>();
    let mut events = Vec::new();
    reserve_exact(
        &mut events,
        clip_segments.saturating_mul(2).saturating_add(2),
    )?;
    let output_segments = ends
        .windows(2)
        .map(|line| line[1].saturating_sub(line[0]).saturating_sub(1) as usize)
        .sum::<usize>();
    budget.add(output_segments.saturating_mul(clip_segments))?;
    for line in 0..ends.len().saturating_sub(1) {
        let start = ends[line] as usize;
        let end = ends[line + 1] as usize;
        let edge = LineSeq::row_view(coords, CoordWindow::from_range(start..end, coords.len()));
        budget.add(edge.len())?;
        for segment in line_segments(&edge) {
            if !rounded_segment_covered_by_clip(
                segment,
                &comparison_rings,
                &comparison_rounded_rings,
                &mut events,
            )? {
                return Err(embedding_failure());
            }
        }
    }
    Ok(packed)
}

#[cfg(test)]
mod tests {
    #[test]
    fn direct_dual_subnormal_admission_matches_legacy_builder() {
        for (units, admitted) in [(1_u64, false), (4, true)] {
            let step = f64::from_bits(units);
            let sites: Vec<_> = [(0.0, 0.0), (step, 0.0), (0.0, step)]
                .into_iter()
                .enumerate()
                .map(|(id, (x, y))| Site {
                    id,
                    point: Point::new_raw_xy(x, y),
                })
                .collect();
            let mut dual_budget = ExpansionBudget::new("test", "subnormal dual");
            let dual = dual_build(
                &sites,
                VoronoiBoundary::Envelope,
                &mut dual_budget,
                certified_delaunay(&sites),
            );
            let mut legacy_budget = ExpansionBudget::new("test", "subnormal legacy");
            let legacy = build(
                &sites,
                VoronoiBoundary::Envelope,
                &mut legacy_budget,
                false,
                certified_delaunay(&sites),
            );

            match (admitted, dual, legacy) {
                (false, Err(dual), Err(legacy)) => {
                    assert!(matches!(
                        dual.kind(),
                        crate::error::ErrorKind::Geometry(GeometryErrorKind::Voronoi(_))
                    ));
                    assert!(matches!(
                        legacy.kind(),
                        crate::error::ErrorKind::Geometry(GeometryErrorKind::Voronoi(_))
                    ));
                },
                (true, Ok(dual), Ok(legacy)) => {
                    for diagram in [dual, legacy] {
                        assert_eq!(diagram.polygons.len(), 3, "units={units}");
                        let cells: Vec<_> = diagram
                            .polygons
                            .iter()
                            .cloned()
                            .map(Shape::Polygon)
                            .collect();
                        assert!(cells.iter().all(|cell| matches!(cell, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())));
                        assert!(coverage_is_valid(&cells, 0.0).unwrap(), "units={units}");
                        let union = Shape::union_all(&cells, Strictness::Lenient).unwrap();
                        assert!(
                            diagram
                                .clip
                                .difference(&union, Strictness::Lenient)
                                .unwrap()
                                .is_empty()
                        );
                        assert!(
                            union
                                .difference(&diagram.clip, Strictness::Lenient)
                                .unwrap()
                                .is_empty()
                        );
                    }
                },
                (expected, dual, legacy) => panic!(
                    "units={units} expected admitted={expected}, dual admitted={}, legacy admitted={}",
                    dual.is_ok(),
                    legacy.is_ok(),
                ),
            }
        }
    }

    use super::*;

    #[test]
    fn rounded_fan_order_matches_exact_stored_points() {
        let points = [
            XY::new(-1.0, -0.0),
            XY::new(1.0, -0.0),
            XY::new(0.0, 1.0),
            XY::new(-1.0, 0.0),
            XY::new(0.0, -1.0),
        ];
        for origin in points {
            for left in points {
                for right in points {
                    let actual = rounded_fan_angle_cmp(origin, left, right);
                    let exact = exact::angle_ccw_cmp(
                        &exact::ExactPoint::from_xy(origin),
                        &exact::ExactPoint::from_xy(left),
                        &exact::ExactPoint::from_xy(right),
                    );
                    assert_eq!(actual.is_err(), exact.is_err());
                    if let (Ok(actual), Ok(exact)) = (actual, exact) {
                        assert_eq!(actual, exact);
                    }
                }
            }
        }
    }

    fn cycle(points: &[(f64, f64)]) -> Vec<exact::ExactPoint> {
        points
            .iter()
            .map(|&(x, y)| exact::ExactPoint::from_xy(XY::new(x, y)))
            .collect()
    }

    fn cycle_xy(points: &[(f64, f64)]) -> Vec<XY> {
        points.iter().map(|&(x, y)| XY::new(x, y)).collect()
    }

    #[test]
    fn rounded_segment_certificate_matches_closed_clip_membership() {
        let rectangle_points = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)];
        let hole_points = [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0)];
        let rectangle = cycle(&rectangle_points);
        let rectangle_xy = cycle_xy(&rectangle_points);
        let hole = cycle(&hole_points);
        let hole_xy = cycle_xy(&hole_points);
        let mut events = Vec::new();
        assert!(
            rounded_segment_covered_by_clip(
                Segment {
                    start: XY::new(0.0, 0.0),
                    end: XY::new(4.0, 0.0),
                },
                &[rectangle.clone()],
                &[rectangle_xy.clone()],
                &mut events,
            )
            .unwrap()
        );
        assert!(
            rounded_segment_covered_by_clip(
                Segment {
                    start: XY::new(0.5, 0.5),
                    end: XY::new(3.5, 3.5),
                },
                &[rectangle.clone()],
                &[rectangle_xy.clone()],
                &mut events,
            )
            .unwrap()
        );
        assert!(
            !rounded_segment_covered_by_clip(
                Segment {
                    start: XY::new(0.5, 2.0),
                    end: XY::new(3.5, 2.0),
                },
                &[rectangle.clone(), hole.clone()],
                &[rectangle_xy.clone(), hole_xy.clone()],
                &mut events,
            )
            .unwrap()
        );
        assert!(
            rounded_segment_covered_by_clip(
                Segment {
                    start: XY::new(1.0, 1.0),
                    end: XY::new(3.0, 1.0),
                },
                &[rectangle.clone(), hole],
                &[rectangle_xy.clone(), hole_xy],
                &mut events,
            )
            .unwrap()
        );
        let concave_points = [
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (3.0, 4.0),
            (3.0, 1.0),
            (1.0, 1.0),
            (1.0, 4.0),
            (0.0, 4.0),
        ];
        let concave = cycle(&concave_points);
        let concave_xy = cycle_xy(&concave_points);
        assert!(
            !rounded_segment_covered_by_clip(
                Segment {
                    start: XY::new(0.5, 3.5),
                    end: XY::new(3.5, 3.5),
                },
                &[concave],
                &[concave_xy],
                &mut events,
            )
            .unwrap()
        );
        let l_shape_points = [
            (0.0, 0.0),
            (4.0, 0.0),
            (4.0, 1.0),
            (1.0, 1.0),
            (1.0, 4.0),
            (0.0, 4.0),
        ];
        let l_shape = cycle(&l_shape_points);
        let l_shape_xy = cycle_xy(&l_shape_points);
        assert!(
            !rounded_segment_covered_by_clip(
                Segment {
                    start: XY::new(4.0, 1.0),
                    end: XY::new(1.0, 4.0),
                },
                &[l_shape],
                &[l_shape_xy],
                &mut events,
            )
            .unwrap()
        );
        let slanted_edge_points = [
            (2.949570888116326e-6, 1.7697425328697956e-6),
            (3.0973519953936948e-6, 1.8584111972362169e-6),
            (3.0e-6, 2.5e-6),
        ];
        let slanted_edge = cycle(&slanted_edge_points);
        let slanted_edge_xy = cycle_xy(&slanted_edge_points);
        let slanted_segment = Segment {
            start: XY::new(2.949570888116328e-6, 1.7697425328697968e-6),
            end: XY::new(3.0973519953936927e-6, 1.8584111972362156e-6),
        };
        let slanted_midpoint = XY::new(
            f64::midpoint(slanted_segment.start.x, slanted_segment.end.x),
            f64::midpoint(slanted_segment.start.y, slanted_segment.end.y),
        );
        assert_eq!(
            exact::orient2d(
                slanted_segment.start,
                slanted_segment.end,
                slanted_edge_xy[0],
            ),
            ExactSign::Zero
        );
        assert_eq!(
            exact::orient2d(
                slanted_segment.start,
                slanted_segment.end,
                slanted_edge_xy[1],
            ),
            ExactSign::Zero
        );
        assert_eq!(
            exact::orient2d(
                slanted_edge_xy[0],
                slanted_edge_xy[1],
                slanted_segment.start,
            ),
            ExactSign::Zero
        );
        assert_eq!(
            exact::orient2d(slanted_edge_xy[0], slanted_edge_xy[1], slanted_segment.end,),
            ExactSign::Zero
        );
        assert!(slanted_edge_xy[0].x < slanted_segment.start.x);
        assert!(slanted_segment.end.x < slanted_edge_xy[1].x);
        assert_ne!(
            exact::orient2d(slanted_segment.start, slanted_segment.end, slanted_midpoint,),
            ExactSign::Zero
        );
        let slanted_result = rounded_segment_covered_by_clip(
            slanted_segment,
            &[slanted_edge],
            &[slanted_edge_xy],
            &mut events,
        )
        .unwrap();
        assert!(slanted_result);
        assert!(
            !rounded_segment_covered_by_clip(
                Segment {
                    start: XY::new(-1.0, 2.0),
                    end: XY::new(2.0, 2.0),
                },
                &[rectangle],
                &[rectangle_xy],
                &mut events,
            )
            .unwrap()
        );
    }

    fn validates(cycles: &[Vec<exact::ExactPoint>]) -> bool {
        let refs: Vec<_> = cycles.iter().map(Vec::as_slice).collect();
        let mut budget = ExpansionBudget::new("test", "exact cycles");
        validate_exact_cycles(&refs, 0, &mut budget).is_ok()
    }

    #[test]
    fn finite_max_exact_candidate_boxes_remain_sparse() {
        let high = cycle(&[(f64::MAX, f64::MAX), (f64::MAX, f64::MAX.next_down())]);
        let low = cycle(&[(0.0, 0.0), (0.0, 1.0)]);
        let cycles = [high.as_slice(), low.as_slice()];
        let mut budget = ExpansionBudget::new("test", "exact candidates");
        let candidates = indexed_exact_segment_candidates(&cycles, &mut budget).unwrap();
        assert_eq!(candidates, [[0, 1], [2, 3]]);
    }

    #[test]
    fn embedded_clip_admission_rejects_crossed_polygon() {
        let ring = Ring::closed(vec![
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(0.0, 2.0),
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
        ])
        .unwrap();
        assert!(admit_embedded_clip(&Polygon::new(ring, Vec::new()).unwrap()).is_err());
    }

    fn reversed_cycles(cycles: &[Vec<exact::ExactPoint>]) -> Vec<Vec<exact::ExactPoint>> {
        std::iter::once(&cycles[0])
            .chain(cycles[1..].iter().rev())
            .map(|cycle| cycle.iter().cloned().rev().collect())
            .collect()
    }

    fn assert_boundary_walk_euler(subdivision: &ExactSubdivision) {
        let components = subdivision
            .topology
            .component_of
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let faces = subdivision.topology.face_starts.len() - 1;
        assert_eq!(
            subdivision.vertices.len() as isize - subdivision.edges.len() as isize + faces as isize,
            2 * components as isize
        );
        if !subdivision.labels.is_empty() {
            assert_eq!(subdivision.labels.len(), faces);
        }
    }

    #[test]
    fn exact_cycle_validator_accepts_one_shell_hole_tangency() {
        let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
        let hole = cycle(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)]);
        assert!(validates(&[shell, hole]));
    }

    #[test]
    fn exact_cycle_validator_rejects_two_shell_hole_contacts() {
        let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
        let hole = cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)]);
        assert!(!validates(&[shell, hole]));
    }

    #[test]
    fn exact_cycle_validator_rejects_crossed_ring() {
        let crossed = cycle(&[(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0)]);
        assert!(!validates(&[crossed]));
    }

    #[test]
    fn exact_touch_graph_covers_all_five_outcomes_and_reversed_discovery() {
        let shell = cycle(&[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0), (0.0, 8.0)]);
        let one_shell_touch = vec![shell.clone(), cycle(&[(0.0, 4.0), (2.0, 5.0), (2.0, 3.0)])];
        let two_shell_touches = vec![shell.clone(), cycle(&[(0.0, 2.0), (2.0, 4.0), (0.0, 6.0)])];
        let two_holes_touch_once = vec![
            shell.clone(),
            cycle(&[(2.0, 2.0), (4.0, 2.0), (4.0, 4.0), (2.0, 4.0)]),
            cycle(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0)]),
        ];
        let four_hole_star = vec![
            cycle(&[(-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)]),
            cycle(&[(0.0, 0.0), (4.0, -1.0), (4.0, 1.0)]),
            cycle(&[(0.0, 0.0), (1.0, 4.0), (-1.0, 4.0)]),
            cycle(&[(0.0, 0.0), (-4.0, 1.0), (-4.0, -1.0)]),
            cycle(&[(0.0, 0.0), (-1.0, -4.0), (1.0, -4.0)]),
        ];
        let interleaved = vec![
            shell,
            cycle(&[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0)]),
            cycle(&[(4.0, 0.0), (8.0, 4.0), (4.0, 8.0), (0.0, 4.0)]),
        ];
        let cases = [
            (one_shell_touch, true),
            (two_shell_touches, false),
            (two_holes_touch_once, true),
            (four_hole_star, true),
            (interleaved, false),
        ];
        for (cycles, expected) in cases {
            let restored = validates(&cycles);
            let reversed = validates(&reversed_cycles(&cycles));
            assert_eq!(restored, expected);
            assert_eq!(reversed, restored);
        }
    }

    #[test]
    fn ordinary_five_site_diagram_is_owned_and_admitted() {
        let sites: Vec<_> = [(0.0, 0.0), (0.0, 1.0), (0.5, 0.4), (1.0, 0.0), (1.0, 1.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let diagram = build(
            &sites,
            VoronoiBoundary::Padded,
            &mut budget,
            true,
            certified_delaunay(&sites),
        )
        .unwrap();
        assert_eq!(diagram.polygons.len(), sites.len());
        assert!(
            diagram
                .polygons
                .iter()
                .all(|polygon| polygon_is_valid(polygon).unwrap())
        );
    }

    #[test]
    fn cosine_circle_fails_open_to_generic_coverage() {
        let sites: Vec<_> = (0..8)
            .map(|index| {
                let angle = f64::from(index) * std::f64::consts::TAU / 8.0;
                Point::new_raw_xy(10.0 * angle.cos(), 10.0 * angle.sin())
            })
            .enumerate()
            .map(|(id, point)| Site { id, point })
            .collect();
        let mut budget = ExpansionBudget::new("test", "cosine circle");
        let diagram = build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut budget,
            true,
            certified_delaunay(&sites),
        )
        .unwrap();
        assert_eq!(diagram.polygons.len(), sites.len());
        assert!(
            diagram
                .polygons
                .iter()
                .all(|polygon| polygon_is_valid(polygon).unwrap())
        );
    }

    fn assert_directed_partition(cells: &[Shape], bounds: [f64; 4]) {
        let mut edges: std::collections::BTreeMap<[PointKey; 2], Vec<[PointKey; 2]>> =
            std::collections::BTreeMap::new();
        for cell in cells {
            let Shape::Polygon(polygon) = cell else {
                panic!("Voronoi cells are polygons");
            };
            for index in 0..polygon.shell.len() - 1 {
                let edge = [
                    PointKey::new(polygon.shell.point_at(index).xy()),
                    PointKey::new(polygon.shell.point_at(index + 1).xy()),
                ];
                let key = if edge[0] <= edge[1] {
                    edge
                } else {
                    [edge[1], edge[0]]
                };
                edges.entry(key).or_default().push(edge);
            }
        }
        let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for directions in edges.values() {
            match directions.as_slice() {
                [edge] => {
                    let a = edge[0].xy();
                    let b = edge[1].xy();
                    let (side, correctly_directed) = if a.y == bounds[2] && b.y == bounds[2] {
                        (0, a.x < b.x)
                    } else if a.x == bounds[1] && b.x == bounds[1] {
                        (1, a.y < b.y)
                    } else if a.y == bounds[3] && b.y == bounds[3] {
                        (2, a.x > b.x)
                    } else if a.x == bounds[0] && b.x == bounds[0] {
                        (3, a.y > b.y)
                    } else {
                        panic!("unmatched edge is not on the clip boundary");
                    };
                    assert!(correctly_directed, "boundary edge has the wrong direction");
                    let (lo, hi) = if side % 2 == 0 {
                        (a.x.min(b.x), a.x.max(b.x))
                    } else {
                        (a.y.min(b.y), a.y.max(b.y))
                    };
                    sides[side].push((lo, hi));
                },
                [first, second] => {
                    assert_eq!(first[0], second[1], "internal edge directions disagree");
                    assert_eq!(first[1], second[0], "internal edge directions disagree");
                },
                _ => panic!("edge has neither one nor two owners"),
            }
        }
        for (side_index, side) in sides.iter_mut().enumerate() {
            side.sort_by(|left, right| left.0.total_cmp(&right.0));
            let (start, end) = if side_index % 2 == 0 {
                (bounds[0], bounds[1])
            } else {
                (bounds[2], bounds[3])
            };
            let mut cursor = start;
            for &(lo, hi) in side.iter() {
                assert_eq!(lo, cursor, "clip boundary has a gap or overlap");
                cursor = hi;
            }
            assert_eq!(cursor, end, "clip boundary is not tiled");
        }
    }

    #[test]
    fn extreme_cluster_is_a_valid_binary64_partition() {
        let points = [
            (1.552_304_940_926_043_4e127, -2.245_414_791_420_301e127),
            (1.552_304_940_919_857_5e127, -2.245_414_791_417_300_5e127),
            (1.552_304_940_926_441_2e127, -2.245_414_791_413_082e127),
            (1.552_304_940_932_905_3e127, -2.245_414_791_419_367e127),
            (1.552_304_940_935_468e127, -2.245_414_791_425_475_7e127),
            (1.552_304_940_933_517_1e127, -2.245_414_791_411_899e127),
            (1.552_304_940_934_345_6e127, -2.245_414_791_424_966e127),
            (1.552_304_940_920_426_7e127, -2.245_414_791_410_440_7e127),
            (1.552_304_940_920_431e127, -2.245_414_791_420_247_5e127),
            (1.552_304_940_933_332_2e127, -2.245_414_791_413_508e127),
            (1.552_304_940_916_651_8e127, -2.245_414_791_420_999e127),
            (1.552_304_940_931_348_2e127, -2.245_414_791_412_127e127),
            (1.552_304_940_931_112_7e127, -2.245_414_791_410_722_6e127),
            (1.552_304_940_915_999_5e127, -2.245_414_791_419_609_6e127),
            (1.552_304_940_921_551e127, -2.245_414_791_419_905_5e127),
            (1.552_304_940_935_471_1e127, -2.245_414_791_419_521e127),
            (1.552_304_940_921_168_7e127, -2.245_414_791_406_195_6e127),
            (1.552_304_940_932_703_6e127, -2.245_414_791_411_408e127),
            (1.552_304_940_925_026_4e127, -2.245_414_791_418_615e127),
            (1.552_304_940_934_617_9e127, -2.245_414_791_422_664_4e127),
            (1.552_304_940_917_422_5e127, -2.245_414_791_420_993e127),
            (1.552_304_940_928_658_7e127, -2.245_414_791_420_993e127),
            (1.552_304_940_921_511e127, -2.245_414_791_417_344_7e127),
            (1.552_304_940_919_395e127, -2.245_414_791_412_605_6e127),
            (1.552_304_940_921_255e127, -2.245_414_791_417_260_6e127),
            (1.552_304_940_933_857e127, -2.245_414_791_409_97e127),
        ];
        let source = Shape::MultiPoint(
            points
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect::<Vec<_>>()
                .into(),
        );
        let cells = source
            .voronoi_polygons(0.0, VoronoiBoundary::Padded)
            .expect("extreme cluster should be a valid partition");
        assert_eq!(cells.len(), points.len());
        assert!(cells.iter().all(|cell| match cell {
            Shape::Polygon(polygon) => polygon_is_valid(polygon).unwrap(),
            _ => false,
        }));
        let canonical = shape::canonical_voronoi_sites(
            points
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect(),
            0.0,
        );
        for (site, cell) in canonical.iter().zip(&cells) {
            let Shape::Polygon(polygon) = cell else {
                unreachable!("Voronoi cells are polygons");
            };
            let rounded_ring: Vec<_> = polygon
                .shell
                .iter()
                .take(polygon.shell.len() - 1)
                .map(|point| exact::ExactPoint::from_xy(point.xy()))
                .collect();
            assert_ne!(
                exact::point_in_cycle(&rounded_ring, &exact::ExactPoint::from_xy(site.point.xy()),),
                exact::PointInCycle::Outside,
            );
        }
        let mut clip_budget = ExpansionBudget::new("test", "extreme cluster clip");
        let padded = normalize_clip(&canonical, VoronoiBoundary::Padded, &mut clip_budget).unwrap();
        let bounds = padded.frame_bounds();
        assert_directed_partition(&cells, bounds);
        // The generic coverage oracle intentionally retains its midpoint
        // certificate and rejects this one-ULP boundary case. The independent
        // union/difference oracle still observes the same rectangular result.
        assert!(!coverage_is_valid(&cells, 0.0).unwrap());
        let clip = Shape::Polygon(
            Polygon::new(
                rectangle(bounds[0], bounds[2], bounds[1], bounds[3]),
                Vec::new(),
            )
            .unwrap(),
        );
        let union = Shape::union_all(&cells, Strictness::Lenient).unwrap();
        assert!(
            clip.difference(&union, Strictness::Lenient)
                .unwrap()
                .is_empty()
        );
        assert!(
            union
                .difference(&clip, Strictness::Lenient)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn shifted_six_site_diagram_rejects_colliding_binary64_vertices() {
        let offset = 1e16;
        let sites: Vec<_> = [
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (5.0, 5.0),
            (2.0, 8.0),
        ]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(offset + x, offset + y),
        })
        .collect();
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let error = match build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut budget,
            true,
            certified_delaunay(&sites),
        ) {
            Ok(_) => panic!("colliding exact vertices unexpectedly embedded"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("no topology-preserving binary64 embedding")
        );
    }

    fn grafted_clip_subdivision(sites: &[Site], clip_polygon: &Polygon) -> ExactSubdivision {
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let clip =
            normalize_clip(sites, VoronoiBoundary::Polygon(clip_polygon), &mut budget).unwrap();
        let mesh = certified_delaunay(sites);
        let complex = delaunay_complex(&mesh, sites).unwrap();
        let primal_edges = mesh.primal_edges();
        let adjacency = certified_site_adjacency(&primal_edges, sites.len(), &mut budget).unwrap();
        let rect = clip_rectangle(&clip).unwrap();
        let scaffold = build_scaffold(&rect, &clip, &mut budget).unwrap();
        let separators =
            emit_clipped_separators(sites, &primal_edges, &complex, &rect, &mut budget).unwrap();
        let atoms = node_scaffold_and_separators(
            scaffold,
            separators,
            sites,
            &adjacency,
            &clip,
            &rect,
            &mut budget,
        )
        .unwrap();
        let mut subdivision = build_exact_subdivision(atoms, &mut budget).unwrap();
        graft_components(&mut subdivision, sites, &adjacency, &mut budget).unwrap();
        subdivision
    }

    fn walk_points(subdivision: &ExactSubdivision, face: BoundaryFaceId) -> Vec<exact::ExactPoint> {
        let topology = &subdivision.topology;
        let walk = &topology.face_slots[topology.face_starts[face.0 as usize] as usize
            ..topology.face_starts[face.0 as usize + 1] as usize];
        walk.iter()
            .map(|&slot| {
                subdivision.vertices[topology.owners[slot as usize] as usize]
                    .point
                    .clone()
            })
            .collect()
    }

    fn first_equal_label_misattaches_a_hole(subdivision: &ExactSubdivision) -> bool {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        for (index, label) in subdivision.labels.iter().enumerate() {
            if !selected_cell_label(*label) {
                continue;
            }
            let face = BoundaryFaceId(index as u32);
            match exact::cycle_orientation(&walk_points(subdivision, face)).unwrap() {
                ExactSign::Positive => positives.push((face, *label)),
                ExactSign::Negative => negatives.push((face, *label)),
                ExactSign::Zero => panic!("selected walk has zero orientation"),
            }
        }
        positives.sort_by_key(|(face, _)| *face);
        negatives.into_iter().any(|(negative, label)| {
            let parent = positives
                .iter()
                .find(|(_, other)| other == &label)
                .expect("negative walk has a same-label positive")
                .0;
            let probe = &walk_points(subdivision, negative)[0];
            exact::point_in_cycle(&walk_points(subdivision, parent), probe)
                != exact::PointInCycle::Inside
        })
    }

    fn rectangle(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Ring {
        Ring::closed(
            [
                (min_x, min_y),
                (max_x, min_y),
                (max_x, max_y),
                (min_x, max_y),
                (min_x, min_y),
            ]
            .into_iter()
            .map(|(x, y)| Point::new_raw_xy(x, y))
            .collect(),
        )
        .unwrap()
    }

    fn sites_xy(points: &[(f64, f64)]) -> Vec<Site> {
        points
            .iter()
            .enumerate()
            .map(|(id, &(x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect()
    }

    fn separator_fixture() -> (Vec<Site>, Polygon) {
        (
            sites_xy(&[
                (-4.0, -3.0),
                (4.0, -2.0),
                (3.0, 4.0),
                (-3.0, 3.0),
                (0.0, 0.0),
            ]),
            Polygon::new(rectangle(-6.0, -6.0, 6.0, 6.0), Vec::new()).unwrap(),
        )
    }

    fn coincident_frame_fixture() -> (Vec<Site>, Polygon) {
        (
            sites_xy(&[(0.0, -1.0), (0.0, 1.0), (2.0, 0.5)]),
            Polygon::new(rectangle(-1.0, 0.0, 0.5, 2.0), Vec::new()).unwrap(),
        )
    }

    fn node_polygon(sites: &[Site], polygon: &Polygon, budget: &mut ExpansionBudget) -> NodedAtoms {
        let clip = normalize_clip(sites, VoronoiBoundary::Polygon(polygon), budget).unwrap();
        let rect = clip_rectangle(&clip).unwrap();
        let mesh = certified_delaunay(sites);
        let complex = delaunay_complex(&mesh, sites).unwrap();
        let primal_edges = mesh.primal_edges();
        let adjacency = certified_site_adjacency(&primal_edges, sites.len(), budget).unwrap();
        let scaffold = build_scaffold(&rect, &clip, budget).unwrap();
        let separators =
            emit_clipped_separators(sites, &primal_edges, &complex, &rect, budget).unwrap();
        node_scaffold_and_separators(
            scaffold, separators, sites, &adjacency, &clip, &rect, budget,
        )
        .unwrap()
    }

    fn old_frame_halfplane_holds(
        edge: &AtomicEdge,
        vertices: &[NodedVertex],
        sites: &[Site],
    ) -> bool {
        let OwnerTransition::Boundary { left, right } = edge.forward.owner else {
            return true;
        };
        let start = &vertices[edge.endpoints[0].0 as usize].point;
        let end = &vertices[edge.endpoints[1].0 as usize].point;
        left.is_none_or(|site| {
            exact::orient_points(
                start,
                end,
                &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
            ) == ExactSign::Positive
        }) && right.is_none_or(|site| {
            exact::orient_points(
                start,
                end,
                &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
            ) == ExactSign::Negative
        })
    }

    #[test]
    fn canonical_separator_emitter_keeps_one_sorted_directed_edge_per_dual() {
        let (sites, polygon) = separator_fixture();
        let mut budget = ExpansionBudget::new("test", "canonical separators");
        let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), &mut budget).unwrap();
        let rect = clip_rectangle(&clip).unwrap();
        let mesh = certified_delaunay(&sites);
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let primal_edges = mesh.primal_edges();
        let separators =
            emit_clipped_separators(&sites, &primal_edges, &complex, &rect, &mut budget).unwrap();
        let expected = primal_edges
            .iter()
            .filter(|edge| match edge {
                CertifiedPrimalEdge::Interior { faces, .. } => {
                    complex.component_of_face(faces[0]) != complex.component_of_face(faces[1])
                },
                CertifiedPrimalEdge::Hull { .. } => true,
            })
            .count();
        assert_eq!(separators.len(), expected);
        for separator in &separators {
            assert!(separator.pair[0] < separator.pair[1]);
            let left = exact::ExactPoint::from_xy(sites[separator.pair[0].0 as usize].point.xy());
            assert_eq!(
                exact::orient_points(&separator.start, &separator.end, &left),
                ExactSign::Positive
            );
        }
    }

    #[test]
    fn hull_center_outside_clip_aabb_keeps_the_enter_exit_chord() {
        let (sites, polygon) = coincident_frame_fixture();
        let mut budget = ExpansionBudget::new("test", "external center chord");
        let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), &mut budget).unwrap();
        let rect = clip_rectangle(&clip).unwrap();
        let mesh = certified_delaunay(&sites);
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let separators =
            emit_clipped_separators(&sites, &mesh.primal_edges(), &complex, &rect, &mut budget)
                .unwrap();
        let pair = [SiteId(0), SiteId(1)];
        let chord = separators
            .iter()
            .find(|separator| separator.pair == pair)
            .expect("A-B hull ray clips to R");
        let bottom = [
            exact::ExactPoint::from_xy(XY::new(-1.0, 0.0)),
            exact::ExactPoint::from_xy(XY::new(0.5, 0.0)),
        ];
        assert!(
            chord.start.same_position(&bottom[0]) && chord.end.same_position(&bottom[1])
                || chord.start.same_position(&bottom[1]) && chord.end.same_position(&bottom[0])
        );
        let center = complex.center(0);
        assert!(!in_rectangle(&rect, center));
        let adjacency =
            certified_site_adjacency(&mesh.primal_edges(), sites.len(), &mut budget).unwrap();
        let scaffold = build_scaffold(&rect, &clip, &mut budget).unwrap();
        let atoms = node_scaffold_and_separators(
            scaffold,
            separators,
            &sites,
            &adjacency,
            &clip,
            &rect,
            &mut budget,
        )
        .unwrap();
        assert!(
            !atoms
                .vertices
                .iter()
                .any(|vertex| vertex.point.same_position(center))
        );
    }

    #[test]
    fn coincident_frame_separator_seeds_inside_owner_without_nearest_tie() {
        let (sites, polygon) = coincident_frame_fixture();
        let mut budget = ExpansionBudget::new("test", "coincident frame");
        let atoms = node_polygon(&sites, &polygon, &mut budget);
        let coincident: Vec<_> = atoms
            .edges
            .iter()
            .filter(|edge| {
                edge.separator == Some([SiteId(0), SiteId(1)]) && edge.frame_side.is_some()
            })
            .collect();
        assert!(!coincident.is_empty());
        for edge in &coincident {
            match edge.forward.owner {
                OwnerTransition::Boundary {
                    left: Some(SiteId(1)),
                    right: None,
                }
                | OwnerTransition::Boundary {
                    left: None,
                    right: Some(SiteId(1)),
                } => {},
                other => panic!("coincident frame owner {other:?}"),
            }
            let midpoint = atoms.vertices[edge.endpoints[0].0 as usize]
                .point
                .midpoint(&atoms.vertices[edge.endpoints[1].0 as usize].point);
            assert!(nearest_site(&midpoint, &sites).is_err());
        }
        let mut fail_budget = ExpansionBudget::new("test", "frame-only tie");
        let clip =
            normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), &mut fail_budget).unwrap();
        let rect = clip_rectangle(&clip).unwrap();
        let mesh = certified_delaunay(&sites);
        let adjacency =
            certified_site_adjacency(&mesh.primal_edges(), sites.len(), &mut fail_budget).unwrap();
        let scaffold = build_scaffold(&rect, &clip, &mut fail_budget).unwrap();
        assert!(
            node_scaffold_and_separators(
                scaffold,
                Vec::new(),
                &sites,
                &adjacency,
                &clip,
                &rect,
                &mut fail_budget,
            )
            .is_err()
        );
    }

    #[test]
    fn packed_separator_column_matches_chain_coordinates() {
        let (sites, polygon) = separator_fixture();
        let mut budget = ExpansionBudget::new("test", "packed chains");
        let packed = build_edges(
            &sites,
            VoronoiBoundary::Polygon(&polygon),
            &mut budget,
            true,
            certified_delaunay(&sites),
        )
        .unwrap();
        let (coords, offsets) = packed.clone().into_line_column().unwrap();
        let lines = packed.into_line_seqs().unwrap();
        assert!(!lines.is_empty());
        let ends = offsets.as_slice();
        assert_eq!(ends.len(), lines.len() + 1);
        assert_eq!(ends[0], 0);
        assert_eq!(ends[ends.len() - 1] as usize, coords.len());
        let mut cursor = 0_usize;
        for (line, &end) in lines.iter().zip(&ends[1..]) {
            let end = end as usize;
            assert_eq!(end - cursor, line.len());
            for index in 0..line.len() {
                assert_eq!(
                    coords.point_at(cursor + index).xy(),
                    line.point_at(index).xy()
                );
            }
            cursor = end;
        }
    }

    #[test]
    fn coincident_frame_separator_emits_one_packed_chain_and_excludes_frame_only() {
        let (sites, polygon) = coincident_frame_fixture();
        let mut budget = ExpansionBudget::new("test", "coincident packed");
        let lines = build_edges(
            &sites,
            VoronoiBoundary::Polygon(&polygon),
            &mut budget,
            true,
            certified_delaunay(&sites),
        )
        .unwrap()
        .into_line_seqs()
        .unwrap();
        let bottom = [XY::new(-1.0, 0.0), XY::new(0.5, 0.0)];
        let matching = lines
            .iter()
            .filter(|line| {
                (0..line.len().saturating_sub(1)).any(|index| {
                    let a = line.point_at(index).xy();
                    let b = line.point_at(index + 1).xy();
                    a == bottom[0] && b == bottom[1] || a == bottom[1] && b == bottom[0]
                })
            })
            .count();
        assert_eq!(matching, 1);
        let frame_only = lines.iter().any(|line| {
            (0..line.len()).all(|index| {
                let xy = line.point_at(index).xy();
                (xy.x == -1.0 || xy.x == 0.5 || xy.y == 0.0 || xy.y == 2.0)
                    && !(xy == bottom[0]
                        || xy == bottom[1]
                        || (xy.y == 0.0 && xy.x > -1.0 && xy.x < 0.5))
            }) && (0..line.len().saturating_sub(1)).all(|index| {
                let a = line.point_at(index).xy();
                let b = line.point_at(index + 1).xy();
                !(a == bottom[0] && b == bottom[1] || a == bottom[1] && b == bottom[0])
            })
        });
        assert!(!frame_only, "frame-only support appeared as a packed chain");
    }

    #[test]
    fn interior_first_separator_order_emits_one_full_chain() {
        let sites = sites_xy(&[(0.0, -1.0), (0.0, 1.0), (2.0, 0.5)]);
        let polygon = Polygon::new(
            closed_ring(&[
                (-1.0, 0.0),
                (-0.5, 0.0),
                (0.0, 0.0),
                (0.5, 0.0),
                (0.5, 2.0),
                (-1.0, 2.0),
            ]),
            Vec::new(),
        )
        .unwrap();
        let mut budget = ExpansionBudget::new("test", "interior-first chain");
        let atoms = node_polygon(&sites, &polygon, &mut budget);
        let mut subdivision = build_exact_subdivision(atoms, &mut budget).unwrap();
        let primal_edges = certified_delaunay(&sites).primal_edges();
        let adjacency = certified_site_adjacency(&primal_edges, sites.len(), &mut budget).unwrap();
        graft_components(&mut subdivision, &sites, &adjacency, &mut budget).unwrap();
        let assembled = assemble_edges(&subdivision, &mut budget).unwrap();
        let separator = [SiteId(0), SiteId(1)];
        let matching: Vec<_> = assembled
            .iter()
            .filter(|chain| chain.separator == separator)
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].edges.len(), 3);
        assert_eq!(matching[0].vertices.len(), 4);
        let known = [
            XY::new(-1.0, 0.0),
            XY::new(-0.5, 0.0),
            XY::new(0.0, 0.0),
            XY::new(0.5, 0.0),
        ];
        let round = |vertex: VertexId| {
            let vertex = &subdivision.vertices[vertex.0 as usize];
            vertex
                .fixed_xy
                .map_or_else(|| vertex.point.round_nearest_even(), Ok)
                .unwrap()
        };
        let assembled_coordinates: Vec<_> =
            matching[0].vertices.iter().copied().map(round).collect();
        assert_eq!(assembled_coordinates.as_slice(), known.as_slice());
        let mut group: Vec<_> = matching[0]
            .edges
            .iter()
            .map(|&edge| (separator, edge))
            .collect();
        group.rotate_left(1);
        let [left, right] = subdivision.edges[group[0].1.0 as usize].endpoints;
        let mut degree = std::collections::BTreeMap::<VertexId, usize>::new();
        for (_, edge_id) in &group {
            for vertex in subdivision.edges[edge_id.0 as usize].endpoints {
                *degree.entry(vertex).or_default() += 1;
            }
        }
        assert_eq!(degree[&left], 2);
        assert_eq!(degree[&right], 2);
        let mut chains = Vec::new();
        assemble_separator_group(&subdivision, separator, &group, &mut budget, &mut chains)
            .unwrap();
        assert_eq!(chains.len(), 1);
        assert_eq!(chains[0].edges.len(), 3);
        assert_eq!(chains[0].vertices.len(), 4);
        let shuffled_coordinates: Vec<_> = chains[0].vertices.iter().copied().map(round).collect();
        assert_eq!(shuffled_coordinates.as_slice(), known.as_slice());
    }

    #[test]
    fn edges_only_embed_still_rejects_colliding_binary64_vertices() {
        let offset = 1e16;
        let sites: Vec<_> = [
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (5.0, 5.0),
            (2.0, 8.0),
        ]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(offset + x, offset + y),
        })
        .collect();
        let clip = Polygon::new(
            rectangle(offset - 1.0, offset - 1.0, offset + 11.0, offset + 11.0),
            Vec::new(),
        )
        .unwrap();
        let mut budget = ExpansionBudget::new("test", "edges-only collision");
        let error = match build_edges(
            &sites,
            VoronoiBoundary::Polygon(&clip),
            &mut budget,
            true,
            certified_delaunay(&sites),
        ) {
            Ok(_) => {
                panic!("colliding exact vertices unexpectedly embedded on the edges-only path")
            },
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("no topology-preserving binary64 embedding")
        );
    }

    fn closed_ring(points: &[(f64, f64)]) -> Ring {
        Ring::closed(
            points
                .iter()
                .chain(points.first())
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect(),
        )
        .unwrap()
    }

    fn assert_cells_cover_clip(cells: &[Shape], clip: &Polygon) {
        assert!(!cells.is_empty());
        assert!(cells.iter().all(|cell| match cell {
            Shape::Polygon(polygon) => polygon_is_valid(polygon).unwrap(),
            _ => false,
        }));
        let union = Shape::union_all(cells, Strictness::Lenient).unwrap();
        let clip_shape = Shape::Polygon(clip.clone());
        assert!(
            clip_shape
                .difference(&union, Strictness::Lenient)
                .unwrap()
                .is_empty()
        );
        assert!(
            union
                .difference(&clip_shape, Strictness::Lenient)
                .unwrap()
                .is_empty()
        );
        for (left, right) in cells
            .iter()
            .enumerate()
            .flat_map(|(index, left)| cells[index + 1..].iter().map(move |right| (left, right)))
        {
            assert_eq!(
                left.intersection(right, Strictness::Lenient)
                    .unwrap()
                    .area(),
                0.0
            );
        }
    }

    fn assert_one_cell_equals_clip(cells: &[Shape], clip: &Polygon) {
        assert_eq!(cells.len(), 1);
        let Shape::Polygon(_) = &cells[0] else {
            panic!("Voronoi cells are polygons");
        };
        let cell = &cells[0];
        let clip_shape = Shape::Polygon(clip.clone());
        assert!(
            clip_shape
                .difference(cell, Strictness::Lenient)
                .unwrap()
                .is_empty()
        );
        assert!(
            cell.difference(&clip_shape, Strictness::Lenient)
                .unwrap()
                .is_empty()
        );
    }

    fn site_shape(points: &[(f64, f64)]) -> Shape {
        Shape::MultiPoint(
            points
                .iter()
                .map(|&(x, y)| Point::new_raw_xy(x, y))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    #[test]
    fn diamond_clip_in_square_aabb_covers_clip_with_no_separator_through_contacts() {
        let diamond = Polygon::new(
            closed_ring(&[(0.0, 1.0), (1.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
            Vec::new(),
        )
        .unwrap();
        let sites = [(10.0, 0.0), (12.0, 0.0), (11.0, 1.0)];
        let cells = site_shape(&sites)
            .voronoi_polygons(0.0, VoronoiBoundary::Polygon(&diamond))
            .unwrap();
        assert_one_cell_equals_clip(&cells, &diamond);
    }

    #[test]
    fn partial_collinear_frame_overlap_covers_clip() {
        let clip = Polygon::new(
            closed_ring(&[(1.0, 0.0), (3.0, 0.0), (3.0, 2.0), (0.0, 2.0)]),
            Vec::new(),
        )
        .unwrap();
        let sites = [(2.0, 1.0), (1.2, 1.4), (2.4, 1.5)];
        let cells = site_shape(&sites)
            .voronoi_polygons(0.0, VoronoiBoundary::Polygon(&clip))
            .unwrap();
        assert_cells_cover_clip(&cells, &clip);
    }

    #[test]
    fn cyclically_rotated_rectangle_clip_covers_clip() {
        let clip = Polygon::new(
            closed_ring(&[(2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]),
            Vec::new(),
        )
        .unwrap();
        let sites = [(0.5, 0.5), (1.5, 0.5), (1.0, 1.5)];
        let cells = site_shape(&sites)
            .voronoi_polygons(0.0, VoronoiBoundary::Polygon(&clip))
            .unwrap();
        assert_cells_cover_clip(&cells, &clip);
    }

    #[test]
    fn holed_diamond_clip_covers_shell_minus_hole() {
        let clip = Polygon::new(
            closed_ring(&[(0.0, 1.0), (1.0, 0.0), (2.0, 1.0), (1.0, 2.0)]),
            vec![closed_ring(&[
                (0.75, 0.75),
                (1.25, 0.75),
                (1.25, 1.25),
                (0.75, 1.25),
            ])],
        )
        .unwrap();
        let sites = [(10.0, 0.0), (12.0, 0.0), (11.0, 1.0)];
        let cells = site_shape(&sites)
            .voronoi_polygons(0.0, VoronoiBoundary::Polygon(&clip))
            .unwrap();
        assert_one_cell_equals_clip(&cells, &clip);
        let Shape::Polygon(cell) = &cells[0] else {
            panic!("Voronoi cells are polygons");
        };
        assert_eq!(cell.holes.len(), 1);
    }

    #[test]
    fn public_polygon_admits_sites_on_and_outside_clip_aabb() {
        let sites = sites_xy(&[(0.0, 0.0), (1.0, 0.0), (3.0, 1.0), (1.0, 1.0)]);
        let polygon = Polygon::new(rectangle(0.0, 0.0, 2.0, 2.0), Vec::new()).unwrap();
        let mut budget = ExpansionBudget::new("test", "sites on and outside R");
        let atoms = node_polygon(&sites, &polygon, &mut budget);
        assert!(
            atoms.edges.iter().any(|edge| {
                edge.separator.is_none()
                    && edge.frame_side.is_some()
                    && !old_frame_halfplane_holds(edge, &atoms.vertices, &sites)
            }),
            "old frame halfplane still holds on every frame-only atom"
        );
        build(
            &sites,
            VoronoiBoundary::Polygon(&polygon),
            &mut budget,
            true,
            certified_delaunay(&sites),
        )
        .unwrap();
    }

    #[test]
    fn subnormal_clip_aabb_exact_lane_admits_sites_on_corners() {
        let step = f64::from_bits(4);
        let sites = sites_xy(&[(0.0, 0.0), (step, 0.0), (0.0, step)]);
        let mut envelope_budget = ExpansionBudget::new("test", "units4 envelope");
        build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut envelope_budget,
            false,
            certified_delaunay(&sites),
        )
        .unwrap();
        let polygon = Polygon::new(rectangle(0.0, 0.0, step, step), Vec::new()).unwrap();
        let mut polygon_budget = ExpansionBudget::new("test", "units4 polygon");
        build(
            &sites,
            VoronoiBoundary::Polygon(&polygon),
            &mut polygon_budget,
            true,
            certified_delaunay(&sites),
        )
        .unwrap();
    }

    #[test]
    fn canonical_separator_atoms_keep_one_identity_and_do_not_branch() {
        let (sites, polygon) = separator_fixture();
        let mut budget = ExpansionBudget::new("test", "canonical separators");
        let atoms = node_polygon(&sites, &polygon, &mut budget);
        let mut degree = std::collections::BTreeMap::<([SiteId; 2], VertexId), usize>::new();
        for edge in atoms.edges {
            if let Some(pair) = edge.separator {
                *degree.entry((pair, edge.endpoints[0])).or_default() += 1;
                *degree.entry((pair, edge.endpoints[1])).or_default() += 1;
            }
        }
        assert!(degree.values().all(|degree| *degree <= 2));
    }

    #[test]
    fn crossed_component_keeps_distinct_faces_with_the_same_owner() {
        let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
            -12.0, -1.0, 12.0, 1.0,
        )])
        .unwrap();
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let atoms = node_polygon(&sites, &clip_polygon, &mut budget);
        let mut subdivision = build_exact_subdivision(atoms, &mut budget).unwrap();
        assert_boundary_walk_euler(&subdivision);
        let primal_edges = certified_delaunay(&sites).primal_edges();
        let adjacency = certified_site_adjacency(&primal_edges, sites.len(), &mut budget).unwrap();
        graft_components(&mut subdivision, &sites, &adjacency, &mut budget).unwrap();
        assert_boundary_walk_euler(&subdivision);
        let parts = assemble_cells(&subdivision, &mut budget).unwrap();
        for part in &parts {
            assert_eq!(part.shell.edges.len() + 1, part.shell.vertices.len());
            assert!(
                part.holes
                    .iter()
                    .all(|hole| hole.edges.len() + 1 == hole.vertices.len())
            );
        }
        let top_faces: std::collections::BTreeSet<_> = parts
            .iter()
            .filter(|part| part.owner == SiteId(1))
            .map(|part| part.face)
            .collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(top_faces.len(), 2);
    }

    #[test]
    fn isolated_component_exterior_inherits_the_containing_face() {
        let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
            -1.0, -1.0, 1.0, 1.0,
        )])
        .unwrap();
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let atoms = node_polygon(&sites, &clip_polygon, &mut budget);
        let mut subdivision = build_exact_subdivision(atoms, &mut budget).unwrap();
        assert_boundary_walk_euler(&subdivision);
        let primal_edges = certified_delaunay(&sites).primal_edges();
        let adjacency = certified_site_adjacency(&primal_edges, sites.len(), &mut budget).unwrap();
        graft_components(&mut subdivision, &sites, &adjacency, &mut budget).unwrap();
        assert_boundary_walk_euler(&subdivision);
        let inherited: Vec<_> = subdivision
            .labels
            .iter()
            .enumerate()
            .filter(|(index, label)| {
                selected_cell_label(**label)
                    && exact::cycle_orientation(&walk_points(
                        &subdivision,
                        BoundaryFaceId(*index as u32),
                    ))
                    .unwrap()
                        == ExactSign::Negative
            })
            .map(|(_, label)| *label)
            .collect();
        assert_eq!(inherited.len(), 1);
        assert_eq!(inherited[0].frame_winding, 1);
        assert_eq!(inherited[0].clip_winding, 1);
        assert!(inherited[0].owner.is_some());
        let parts = assemble_cells(&subdivision, &mut budget).unwrap();
        assert_eq!(parts.iter().map(|part| part.holes.len()).sum::<usize>(), 1);
    }

    #[test]
    fn two_isolated_holes_in_split_same_owner_cell_pair_by_containment() {
        let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![
            rectangle(-12.0, -1.0, 12.0, 1.0),
            rectangle(-8.0, 6.0, -6.0, 8.0),
            rectangle(-1.2, -1.7, -0.6, -1.2),
        ])
        .unwrap();
        let subdivision = grafted_clip_subdivision(&sites, &clip_polygon);
        assert_boundary_walk_euler(&subdivision);
        assert!(
            first_equal_label_misattaches_a_hole(&subdivision),
            "first equal-label attachment must pick a non-containing shell on this split same-owner diagram"
        );
        let mut budget = ExpansionBudget::new("test", "owner-grouped assembly");
        let restored = assemble_cells(&subdivision, &mut budget).unwrap();
        let top: Vec<_> = restored
            .iter()
            .filter(|part| part.owner == SiteId(1))
            .collect();
        assert_eq!(top.len(), 2);
        assert_eq!(top.iter().map(|part| part.holes.len()).sum::<usize>(), 2);
        assert!(top.iter().all(|part| part.holes.len() == 1));
    }

    #[test]
    fn validate_rejects_a_mutated_twin_walk_label() {
        let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
            -1.0, -1.0, 1.0, 1.0,
        )])
        .unwrap();
        let mut subdivision = grafted_clip_subdivision(&sites, &clip_polygon);
        validate_exact_subdivision(&subdivision).unwrap();
        let slot = (0..subdivision.topology.targets.len())
            .find(|&slot| {
                let left = walk_label(
                    &subdivision,
                    BoundaryFaceId(subdivision.topology.face_of[slot]),
                );
                selected_cell_label(left)
            })
            .expect("selected walk has a slot");
        let face = subdivision.topology.face_of[slot] as usize;
        subdivision.labels[face] = RegionLabel {
            frame_winding: 0,
            clip_winding: 0,
            owner: None,
        };
        assert!(validate_exact_subdivision(&subdivision).is_err());
    }

    #[test]
    fn collinear_holed_clip_splits_each_separator_into_two_pieces() {
        let sites: Vec<_> = [(-2.0, 0.0), (0.0, 0.0), (2.0, 0.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let clip = Polygon::new(rectangle(-3.0, -3.0, 3.0, 3.0), vec![rectangle(
            -1.5, -1.0, 1.5, 1.0,
        )])
        .unwrap();
        let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
        let edges = build_collinear_edges(
            &sites,
            VoronoiBoundary::Polygon(&clip),
            &mut budget,
            certified_delaunay(&sites),
        )
        .unwrap()
        .into_line_seqs()
        .unwrap();
        assert_eq!(edges.len(), 4);
        let mut endpoints: Vec<_> = edges
            .iter()
            .map(|edge| {
                let first = edge.get(0).unwrap().xy();
                let last = edge.get(edge.len() - 1).unwrap().xy();
                (first.x, first.y, last.x, last.y)
            })
            .collect();
        endpoints.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.total_cmp(&right.1))
        });
        assert_eq!(endpoints, vec![
            (-1.0, -3.0, -1.0, -1.0),
            (-1.0, 1.0, -1.0, 3.0),
            (1.0, -3.0, 1.0, -1.0),
            (1.0, 1.0, 1.0, 3.0),
        ]);
    }

    #[test]
    fn frame_nearest_ignores_a_tie_that_is_not_the_minimum() {
        let sites: Vec<_> = [(-1.0, 0.0), (1.0, 0.0), (0.0, 0.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let point = exact::ExactPoint::from_xy(XY::new(0.0, 10.0));
        assert_eq!(nearest_site(&point, &sites).unwrap(), SiteId(2));
        let true_tie = exact::ExactPoint::from_xy(XY::new(0.0, 0.0));
        assert!(nearest_site(&true_tie, &sites[..2]).is_err());
    }

    #[test]
    fn finite_extreme_collinear_envelope_is_explicitly_degenerate() {
        let sites: Vec<_> = [(-f64::MAX, 0.0), (f64::MAX, 0.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
        assert!(
            build_collinear_edges(
                &sites,
                VoronoiBoundary::Envelope,
                &mut budget,
                certified_delaunay(&sites),
            )
            .unwrap()
            .into_line_seqs()
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn sloped_collinear_envelope_emits_analytic_bisector() {
        let sites: Vec<_> = [(0.0, 0.0), (2.0, 2.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
        let edges = build_collinear_edges(
            &sites,
            VoronoiBoundary::Envelope,
            &mut budget,
            certified_delaunay(&sites),
        )
        .unwrap()
        .into_line_seqs()
        .unwrap();
        assert_eq!(edges.len(), 1);
        let mut endpoints: Vec<_> = edges[0].iter().map(|point| point.xy()).collect();
        endpoints.sort_by(|left, right| left.x.total_cmp(&right.x));
        assert_eq!(endpoints, [XY::new(0.0, 2.0), XY::new(2.0, 0.0)]);
    }

    #[test]
    fn tolerance_clustered_sites_do_not_tie_private_frame_ownership() {
        let sites = super::shape::canonical_voronoi_sites(
            [(0.0, 0.0), (0.09, 0.0), (0.18, 0.0), (0.0, 1.0), (1.0, 0.0)]
                .into_iter()
                .map(|(x, y)| Point::new_raw_xy(x, y))
                .collect(),
            0.1,
        );
        let clip = Polygon::new(rectangle(-1.0, -1.0, 2.0, 2.0), Vec::new()).unwrap();
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let diagram = build(
            &sites,
            VoronoiBoundary::Polygon(&clip),
            &mut budget,
            true,
            certified_delaunay(&sites),
        )
        .unwrap();
        assert!(!diagram.into_edges().into_line_seqs().unwrap().is_empty());
    }

    #[test]
    fn semantic_admission_mutants_change_validity_overlap_gap_and_spill() {
        let shape = |min_x, min_y, max_x, max_y| {
            Shape::Polygon(Polygon::new(rectangle(min_x, min_y, max_x, max_y), Vec::new()).unwrap())
        };
        let clip = shape(0.0, 0.0, 4.0, 4.0);
        let restored_rows = vec![shape(0.0, 0.0, 2.0, 4.0), shape(2.0, 0.0, 4.0, 4.0)];
        let mutated_overlap = vec![clip.clone(), shape(0.0, 0.0, 2.0, 4.0)];
        let restored_coverage = coverage_is_valid(&restored_rows, 0.0).unwrap();
        let mutated_coverage = coverage_is_valid(&mutated_overlap, 0.0).unwrap();
        assert_ne!(
            mutated_coverage, restored_coverage,
            "mutated overlap={mutated_coverage}, restored partition={restored_coverage}"
        );
        assert!(!mutated_coverage && restored_coverage);

        let restored_union = Shape::union_all(&restored_rows, Strictness::Lenient).unwrap();
        let mutated_gap_union = Shape::union_all(
            &[shape(0.0, 0.0, 1.9, 4.0), shape(2.1, 0.0, 4.0, 4.0)],
            Strictness::Lenient,
        )
        .unwrap();
        let restored_gap = !clip
            .difference(&restored_union, Strictness::Lenient)
            .unwrap()
            .is_empty();
        let mutated_gap = !clip
            .difference(&mutated_gap_union, Strictness::Lenient)
            .unwrap()
            .is_empty();
        assert_ne!(
            mutated_gap, restored_gap,
            "mutated gap={mutated_gap}, restored gap={restored_gap}"
        );

        let mutated_spill_union = Shape::union_all(
            &[clip.clone(), shape(5.0, 0.0, 6.0, 1.0)],
            Strictness::Lenient,
        )
        .unwrap();
        let restored_spill = !restored_union
            .difference(&clip, Strictness::Lenient)
            .unwrap()
            .is_empty();
        let mutated_spill = !mutated_spill_union
            .difference(&clip, Strictness::Lenient)
            .unwrap()
            .is_empty();
        assert_ne!(
            mutated_spill, restored_spill,
            "mutated spill={mutated_spill}, restored spill={restored_spill}"
        );

        let restored_polygon = Polygon::new(rectangle(0.0, 0.0, 4.0, 4.0), Vec::new()).unwrap();
        let mutated_polygon = Polygon::new(
            Ring::closed(
                [(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0), (0.0, 0.0)]
                    .into_iter()
                    .map(|(x, y)| Point::new_raw_xy(x, y))
                    .collect(),
            )
            .unwrap(),
            Vec::new(),
        )
        .unwrap();
        let restored_valid = polygon_is_valid(&restored_polygon).unwrap();
        let mutated_valid = polygon_is_valid(&mutated_polygon).unwrap();
        assert_ne!(
            mutated_valid, restored_valid,
            "mutated valid={mutated_valid}, restored valid={restored_valid}"
        );
    }

    #[test]
    fn transition_and_budget_mutants_report_distinct_restored_values() {
        let left = RegionLabel {
            frame_winding: 1,
            clip_winding: 1,
            owner: Some(SiteId(0)),
        };
        let transition = Transition {
            frame_delta: 0,
            clip_delta: 1,
            owner: OwnerTransition::Boundary {
                left: Some(SiteId(0)),
                right: Some(SiteId(1)),
            },
        };
        let restored = crossed_label(left, transition).unwrap();
        let mutated = crossed_label(left, Transition {
            clip_delta: 0,
            owner: OwnerTransition::Preserve,
            ..transition
        })
        .unwrap();
        assert_ne!(
            mutated, restored,
            "mutated transition={mutated:?}, restored transition={restored:?}"
        );
        assert_eq!(restored.clip_winding, 0);
        assert_eq!(restored.owner, Some(SiteId(1)));

        let inherited = left;
        let zero_seeded = RegionLabel {
            frame_winding: 0,
            clip_winding: 0,
            owner: None,
        };
        assert_ne!(
            zero_seeded, inherited,
            "mutated inherited label={zero_seeded:?}, restored inherited label={inherited:?}"
        );

        let exact = ExpansionBudget::check("test", "work", GENERATED_ITEM_LIMIT).unwrap();
        let plus_one_rejected =
            ExpansionBudget::check("test", "work", GENERATED_ITEM_LIMIT + 1).is_err();
        assert_ne!(
            plus_one_rejected,
            exact != GENERATED_ITEM_LIMIT,
            "limit+1 rejected={plus_one_rejected}, exact admitted={exact}"
        );
        let finite_product = ExpansionBudget::product("test", "work", 4, 4).unwrap();
        let overflow_rejected = ExpansionBudget::product("test", "work", usize::MAX, 2).is_err();
        assert_ne!(
            overflow_rejected,
            finite_product != 16,
            "overflow rejected={overflow_rejected}, restored product={finite_product}"
        );
    }

    #[test]
    fn dual_completeness_rejects_a_missing_cell_mutant() {
        let sites: Vec<_> = [(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (2.0, 2.0), (0.8, 1.1)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_raw_xy(x, y),
            })
            .collect();
        let mut construction_budget = ExpansionBudget::new("test", "dual completeness");
        let clip =
            normalize_clip(&sites, VoronoiBoundary::Envelope, &mut construction_budget).unwrap();
        let mesh = certified_delaunay(&sites);
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let restored =
            dual_build_rings(&sites, &mesh, complex, &clip, &mut construction_budget).unwrap();
        let mut mutated = restored.clone();
        mutated.rings.pop();

        let mut restored_budget = ExpansionBudget::new("test", "restored dual completeness");
        let restored_diagram = dual_embed(&restored, &clip, &sites, &mut restored_budget).unwrap();
        let restored_admitted = true;
        let mut missing_rows: Vec<_> = restored_diagram
            .polygons
            .iter()
            .cloned()
            .map(Shape::Polygon)
            .collect();
        missing_rows.pop();
        assert!(
            coverage_is_valid(&missing_rows, 0.0).unwrap(),
            "coverage validity deliberately does not certify clip completeness"
        );
        let mut mutated_budget = ExpansionBudget::new("test", "mutated dual completeness");
        let mutated_admitted = dual_embed(&mutated, &clip, &sites, &mut mutated_budget).is_ok();
        assert_ne!(
            mutated_admitted, restored_admitted,
            "mutated missing-cell admitted={mutated_admitted}, restored admitted={restored_admitted}"
        );
        assert!(!mutated_admitted && restored_admitted);
    }

    fn shared_dual_vertex(rings: &DualRings) -> (usize, usize, exact::ExactPoint) {
        for (ring_index, ring) in rings.rings.iter().enumerate() {
            for (vertex_index, vertex) in ring.iter().enumerate() {
                let point = dual_vertex_point(vertex, &rings.centers)
                    .expect("dual mutation fixture has valid vertices");
                if rings.rings.iter().enumerate().any(|(other_index, other)| {
                    other_index != ring_index
                        && other.iter().any(|other_vertex| {
                            dual_vertex_point(other_vertex, &rings.centers)
                                .is_some_and(|other_point| point.same_position(other_point))
                        })
                }) {
                    return (ring_index, vertex_index, point.clone());
                }
            }
        }
        panic!("dual mutation fixture has no shared vertex");
    }

    fn shared_boundary_vertex(
        rings: &DualRings,
        bounds: [f64; 4],
    ) -> (usize, usize, exact::ExactPoint, usize) {
        for (ring_index, ring) in rings.rings.iter().enumerate() {
            for (vertex_index, vertex) in ring.iter().enumerate() {
                let point = dual_vertex_point(vertex, &rings.centers)
                    .expect("dual mutation fixture has valid vertices");
                let rounded = point.round_nearest_even().unwrap();
                let Some(side) = rounded_boundary_side(rounded, bounds) else {
                    continue;
                };
                if rings.rings.iter().enumerate().any(|(other_index, other)| {
                    other_index != ring_index
                        && other.iter().any(|other_vertex| {
                            dual_vertex_point(other_vertex, &rings.centers)
                                .is_some_and(|other_point| point.same_position(other_point))
                        })
                }) {
                    return (ring_index, vertex_index, point.clone(), side);
                }
            }
        }
        panic!("dual mutation fixture has no shared boundary vertex");
    }

    fn rounded_duplicate(point: &exact::ExactPoint) -> exact::ExactPoint {
        let rounded = point.round_nearest_even().unwrap();
        let base = exact::ExactPoint::from_xy(rounded);
        let upper = exact::ExactPoint::from_xy(XY::new(rounded.x.next_up(), rounded.y));
        base.midpoint(&upper).midpoint(&base)
    }

    fn rounded_boundary_side(point: XY, bounds: [f64; 4]) -> Option<usize> {
        if point.y == bounds[2] {
            Some(0)
        } else if point.x == bounds[1] {
            Some(1)
        } else if point.y == bounds[3] {
            Some(2)
        } else if point.x == bounds[0] {
            Some(3)
        } else {
            None
        }
    }

    #[test]
    fn dual_local_validator_rejects_deterministic_partition_mutations() {
        let sites: Vec<_> = [
            (0.0, 0.0),
            (1.0, 0.0),
            (0.0, 1.0),
            (1.0, 1.0),
            (0.5, 0.4),
            (0.2, 0.7),
        ]
        .into_iter()
        .enumerate()
        .map(|(id, (x, y))| Site {
            id,
            point: Point::new_raw_xy(x, y),
        })
        .collect();
        let mut construction_budget = ExpansionBudget::new("test", "dual mutation fixture");
        let clip =
            normalize_clip(&sites, VoronoiBoundary::Envelope, &mut construction_budget).unwrap();
        let mesh = certified_delaunay(&sites);
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let restored =
            dual_build_rings(&sites, &mesh, complex, &clip, &mut construction_budget).unwrap();
        let mut restored_budget = ExpansionBudget::new("test", "restored dual mutation fixture");
        assert!(dual_embed(&restored, &clip, &sites, &mut restored_budget).is_ok());

        let mut mutations = Vec::new();

        let mut missing_cell = restored.clone();
        missing_cell.rings.pop();
        mutations.push(("missing cell", missing_cell));

        let mut reversed_ring = restored.clone();
        reversed_ring.rings[0].reverse();
        mutations.push(("reversed ring", reversed_ring));

        let (shared_ring, shared_vertex, shared_point) = shared_dual_vertex(&restored);
        let mut moved_shared_vertex = restored.clone();
        let moved = shared_point.round_nearest_even().unwrap();
        moved_shared_vertex.rings[shared_ring][shared_vertex] = DualVertex::Generated(Box::new(
            exact::ExactPoint::from_xy(XY::new(moved.x + 0.125, moved.y)),
        ));
        mutations.push(("moved shared vertex", moved_shared_vertex));

        let mut duplicate_key = restored.clone();
        let duplicate = rounded_duplicate(&shared_point);
        assert_eq!(
            PointKey::new(duplicate.round_nearest_even().unwrap()),
            PointKey::new(shared_point.round_nearest_even().unwrap())
        );
        assert!(!duplicate.same_position(&shared_point));
        duplicate_key.rings[shared_ring][shared_vertex] =
            DualVertex::Generated(Box::new(duplicate));
        mutations.push(("duplicate rounded key", duplicate_key));

        let bounds = clip.frame_bounds();
        let mut wrong_boundary_direction = restored.clone();
        let mut swapped_boundary = false;
        'find_boundary: for ring in &mut wrong_boundary_direction.rings {
            for index in 0..ring.len() {
                let next = (index + 1) % ring.len();
                let left = dual_vertex_point(&ring[index], &restored.centers)
                    .unwrap()
                    .round_nearest_even()
                    .unwrap();
                let right = dual_vertex_point(&ring[next], &restored.centers)
                    .unwrap()
                    .round_nearest_even()
                    .unwrap();
                if rounded_boundary_side(left, bounds).is_some()
                    && rounded_boundary_side(left, bounds) == rounded_boundary_side(right, bounds)
                {
                    ring.swap(index, next);
                    swapped_boundary = true;
                    break 'find_boundary;
                }
            }
        }
        assert!(
            swapped_boundary,
            "dual mutation fixture has no boundary edge"
        );
        mutations.push(("wrong boundary direction", wrong_boundary_direction));

        let (boundary_ring, boundary_vertex, boundary_point, boundary_side) =
            shared_boundary_vertex(&restored, bounds);
        let boundary_rounded = boundary_point.round_nearest_even().unwrap();
        for (name, direction) in [("gap", 1.0), ("overlap", -1.0)] {
            let mut mutation = restored.clone();
            let shifted = if boundary_side % 2 == 0 {
                XY::new(boundary_rounded.x + direction * 0.125, boundary_rounded.y)
            } else {
                XY::new(boundary_rounded.x, boundary_rounded.y + direction * 0.125)
            };
            mutation.rings[boundary_ring][boundary_vertex] =
                DualVertex::Generated(Box::new(exact::ExactPoint::from_xy(shifted)));
            mutations.push((name, mutation));
        }

        let mut crossed_ring = restored.clone();
        let crossed_index = crossed_ring
            .rings
            .iter()
            .position(|ring| ring.len() >= 5)
            .expect("dual mutation fixture has no crossable ring");
        crossed_ring.rings[crossed_index].swap(1, 2);
        mutations.push(("crossed ring", crossed_ring));

        for (name, mutation) in mutations {
            let mut budget = ExpansionBudget::new("test", "mutated dual partition");
            assert!(
                dual_embed(&mutation, &clip, &sites, &mut budget).is_err(),
                "dual mutation unexpectedly admitted: {name}"
            );
        }
    }
}

// =====================================================================
// The certified combinatorial dual.
//
// The Voronoi DCEL is the dual graph of the ALREADY-CERTIFIED Delaunay
// complex. The cell of site `p` is the radial walk of the faces incident to
// `p`, mapped through `DelaunayComplex::component_of_face` to one exact
// circumcenter each. That walk is pure half-edge combinatorics: it evaluates
// NO geometric predicate. Exact geometry is then needed only for
//   (a) capping the two unbounded rays of a hull cell on the private frame,
//   (b) clipping each cell to the clip rectangle, and
//   (c) the rounding certificate.
// Everything `emit_clipped_separators` / `node_scaffold_and_separators` /
// `graft_components` / `assemble_cells` does is replaced by (a) and (b).
//
// Rectangle clips (`Envelope` / `Padded`) use the direct half-edge lane. The
// general polygon lane retains the exact clipped-DCEL construction below: the
// certified complex fixes the unclipped abstract dual, but clipping can delete
// every separator around an isolated clip ring, whose owning site must still
// be established geometrically.
// =====================================================================

const fn dual_next_halfedge(edge: usize) -> usize {
    if edge % 3 == 2 { edge - 2 } else { edge + 1 }
}

/// A dual ring vertex. `Center(c)` names the one exact center stored by the
/// ring collection; generated vertices own only frame, ray, or clip points.
#[derive(Clone)]
enum DualVertex {
    Center(u32),
    Generated(Box<exact::ExactPoint>),
}

/// The edge leaving a dual ring vertex. Consecutive Voronoi vertices around a
/// site lie on the bisector of the site and one neighbour, so a clip crossing
/// on such an edge is a bisector meeting an axis-aligned line: a certified
/// one-dimensional rounding. Frame and clip sides are axis-aligned, and their
/// crossings with another axis-aligned line are exact doubles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeTag {
    Bisector(u32),
    Axis,
}

#[derive(Clone)]
struct Tagged {
    vertex: DualVertex,
    leaving: EdgeTag,
}

fn reverse_tagged(ring: &mut [Tagged]) {
    ring.reverse();
    let count = ring.len();
    let tags: Vec<EdgeTag> = ring.iter().map(|tagged| tagged.leaving).collect();
    for (index, tagged) in ring.iter_mut().enumerate() {
        tagged.leaving = tags[(index + 1) % count];
    }
}

fn dedup_tagged(ring: &mut Vec<Tagged>, centers: &[exact::ExactPoint]) {
    ring.dedup_by(|later, earlier| {
        let same = dual_vertex_point(&later.vertex, centers)
            .zip(dual_vertex_point(&earlier.vertex, centers))
            .is_some_and(|(left, right)| left.same_position(right));
        if same {
            earlier.leaving = later.leaving;
        }
        same
    });
    while ring.len() > 1
        && dual_vertex_point(&ring[0].vertex, centers)
            .zip(dual_vertex_point(
                &ring.last().expect("non-empty").vertex,
                centers,
            ))
            .is_some_and(|(left, right)| left.same_position(right))
    {
        ring.pop();
    }
}

#[derive(Clone)]
struct DualRings {
    rings: Vec<Vec<DualVertex>>,
    centers: Vec<exact::ExactPoint>,
}

/// The radial walk. `triangles[e]` is the origin of half-edge `e`; the walk
/// `incoming -> halfedges[next(incoming)]` rotates around the shared endpoint.
/// Starting from a hull half-edge when one exists makes an unbounded cell's
/// walk begin and end on the hull. Faces for all sites share one flat backing
/// allocation; the offsets are the only per-site state.
#[cfg(test)]
mod convex_cell_certificate {
    use super::*;

    fn xy(points: &[(f64, f64)]) -> Vec<XY> {
        points.iter().map(|&(x, y)| XY::new(x, y)).collect()
    }

    /// A square of sites with its centre: every circumcenter lies on the
    /// envelope, so a clip hit and a center share one vertex, and the diagram
    /// must still cancel every interior edge at any scale.
    #[test]
    fn clip_hits_coinciding_with_centers_share_one_vertex() {
        for scale in [1.0, 1e-20, 1e-150, 1e150] {
            let sites: Vec<Site> = shape::canonical_voronoi_sites(
                [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.5, 0.5)]
                    .into_iter()
                    .map(|(x, y)| Point::new_raw_xy(x * scale, y * scale))
                    .collect(),
                0.0,
            );
            let mut budget = ExpansionBudget::new("test", "coinciding vertices");
            let cells = build(
                &sites,
                VoronoiBoundary::Envelope,
                &mut budget,
                true,
                certified_delaunay(&sites),
            )
            .unwrap_or_else(|error| panic!("scale {scale:e}: {error:?}"))
            .into_polygons();
            assert_eq!(cells.len(), 5);
            let vertex_count: usize = cells.iter().map(|cell| cell.shell.iter().count()).sum();
            let shapes: Vec<_> = cells.iter().cloned().map(Shape::Polygon).collect();
            assert!(shapes.iter().all(
                |shape| matches!(shape, Shape::Polygon(polygon) if polygon_is_valid(polygon).unwrap())
            ));
            assert!(coverage_is_valid(&shapes, 0.0).unwrap());
            assert!(vertex_count > 0);
        }
    }

    #[test]
    fn reversing_a_tagged_ring_keeps_each_edge_with_its_tag() {
        let tagged = |x: f64, leaving: EdgeTag| Tagged {
            vertex: DualVertex::Generated(Box::new(exact::ExactPoint::from_xy(XY::new(x, 0.0)))),
            leaving,
        };
        // v0 -(a)-> v1 -(b)-> v2 -(c)-> v0
        let mut ring = vec![
            tagged(0.0, EdgeTag::Bisector(0)),
            tagged(1.0, EdgeTag::Bisector(1)),
            tagged(2.0, EdgeTag::Axis),
        ];
        reverse_tagged(&mut ring);
        // v2 -(b)-> v1 -(a)-> v0 -(c)-> v2
        let leaving: Vec<EdgeTag> = ring.iter().map(|tagged| tagged.leaving).collect();
        assert_eq!(leaving, [
            EdgeTag::Bisector(1),
            EdgeTag::Bisector(0),
            EdgeTag::Axis
        ]);
        let xs: Vec<f64> = ring
            .iter()
            .map(|tagged| {
                dual_vertex_point(&tagged.vertex, &[])
                    .unwrap()
                    .round_nearest_even()
                    .unwrap()
                    .x
            })
            .collect();
        assert_eq!(xs, [2.0, 1.0, 0.0]);
    }

    #[test]
    fn one_construction_reached_from_two_cells_is_one_position() {
        let (a, b) = (XY::new(0.0, 0.0), XY::new(4.0, 2.0));
        let side = [XY::new(-10.0, 7.0), XY::new(10.0, 7.0)];
        let other_side = [XY::new(-10.0, -7.0), XY::new(10.0, -7.0)];
        let hit = |sites: [XY; 2], side: [XY; 2]| match exact::ExactPoint::certified_bisector_hit(
            sites, side,
        ) {
            exact::FrameHit::Point(point) => point,
            other => panic!("expected a certified hit, got {other:?}"),
        };
        let forward = hit([a, b], side);
        let swapped = hit([b, a], [side[1], side[0]]);
        assert!(forward.same_position(&swapped));
        assert!(!forward.same_position(&hit([a, b], other_side)));
        let center = exact::circumcenter(a, b, XY::new(0.0, 5.0)).unwrap();
        assert!(!forward.same_position(&center));
        let center_permuted = exact::circumcenter(XY::new(0.0, 5.0), a, b).unwrap();
        assert!(center.same_position(&center_permuted));
    }

    #[test]
    fn accepts_convex_counter_clockwise_cells_with_the_site_inside_or_on_an_edge() {
        let square = xy(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
        assert!(certify_convex_cell(&square, XY::new(1.0, 2.0)));
        assert!(certify_convex_cell(&square, XY::new(4.0, 2.0)));
        assert!(certify_convex_cell(&square, XY::new(0.0, 0.0)));
        let hexagon = xy(&[
            (2.0, 0.0),
            (4.0, 1.0),
            (4.0, 3.0),
            (2.0, 4.0),
            (0.0, 3.0),
            (0.0, 1.0),
        ]);
        assert!(certify_convex_cell(&hexagon, XY::new(2.0, 2.0)));
    }

    #[test]
    fn refuses_every_mutation_the_exact_lane_would_refuse() {
        let square = xy(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
        // Site outside.
        assert!(!certify_convex_cell(&square, XY::new(5.0, 2.0)));
        // Clockwise.
        let mut clockwise = square.clone();
        clockwise.reverse();
        assert!(!certify_convex_cell(&clockwise, XY::new(1.0, 2.0)));
        // Two vertices swapped: a bow tie.
        let mut swapped = square.clone();
        swapped.swap(1, 2);
        assert!(!certify_convex_cell(&swapped, XY::new(1.0, 2.0)));
        // A duplicated vertex and a collinear vertex both give a zero turn.
        let mut duplicated = square.clone();
        duplicated.insert(1, XY::new(4.0, 0.0));
        assert!(!certify_convex_cell(&duplicated, XY::new(1.0, 2.0)));
        // A forward collinear vertex is a clipped hull cell's shape: accepted.
        let mut collinear = square.clone();
        collinear.insert(1, XY::new(2.0, 0.0));
        assert!(certify_convex_cell(&collinear, XY::new(1.0, 2.0)));
        // A collinear reversal (a spike) is not.
        let spike = xy(&[
            (0.0, 0.0),
            (4.0, 0.0),
            (6.0, 0.0),
            (4.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
        ]);
        assert!(!certify_convex_cell(&spike, XY::new(1.0, 2.0)));
        let backtrack = xy(&[(0.0, 0.0), (4.0, 0.0), (3.0, 0.0), (4.0, 4.0), (0.0, 4.0)]);
        assert!(!certify_convex_cell(&backtrack, XY::new(1.0, 2.0)));
        // A reflex vertex.
        let reflex = xy(&[(0.0, 0.0), (4.0, 0.0), (2.0, 1.0), (4.0, 4.0), (0.0, 4.0)]);
        assert!(!certify_convex_cell(&reflex, XY::new(1.0, 2.0)));
        // A pentagram turns left everywhere yet self-intersects.
        let pentagram: Vec<XY> = (0..5)
            .map(|k| {
                let angle = std::f64::consts::TAU * f64::from(k * 2) / 5.0;
                XY::new(angle.cos(), angle.sin())
            })
            .collect();
        for index in 0..5 {
            let (a, b, c) = (
                pentagram[index],
                pentagram[(index + 1) % 5],
                pentagram[(index + 2) % 5],
            );
            assert_eq!(exact::orient2d(a, b, c), ExactSign::Positive);
        }
        assert!(!certify_convex_cell(&pentagram, XY::new(0.0, 0.0)));
        // Too few vertices.
        assert!(!certify_convex_cell(&square[..2], XY::new(1.0, 2.0)));
    }
}

fn dual_incident_faces(mesh: &CertifiedDelaunay, site_count: usize) -> IncidentFaces {
    let triangles = mesh.triangles();
    let halfedges = mesh.halfedges();
    let flat = |edge: usize| triangles[edge / 3][edge % 3];
    let mut start_edge = vec![usize::MAX; site_count];
    for edge in 0..halfedges.len() {
        let endpoint = flat(dual_next_halfedge(edge));
        if start_edge[endpoint] == usize::MAX || halfedges[edge] == EMPTY {
            start_edge[endpoint] = edge;
        }
    }
    let mut offsets = Vec::with_capacity(site_count + 1);
    let mut boundaries = Vec::with_capacity(site_count);
    offsets.push(0);
    for site in 0..site_count {
        let start = start_edge[site];
        let (length, boundary) = incident_faces_for_site(start, halfedges, None);
        offsets.push(offsets.last().copied().unwrap_or(0) + length);
        boundaries.push(boundary);
    }
    let mut faces = vec![0; offsets.last().copied().unwrap_or(0)];
    let mut neighbors = vec![0; faces.len()];
    for site in 0..site_count {
        let start = start_edge[site];
        let range = offsets[site]..offsets[site + 1];
        incident_faces_for_site(
            start,
            halfedges,
            Some(IncidenceOutput {
                faces: &mut faces[range.clone()],
                neighbors: &mut neighbors[range],
                flat: &flat,
            }),
        );
    }
    IncidentFaces {
        offsets,
        faces,
        neighbors,
        boundaries,
    }
}

struct IncidentFaces {
    offsets: Vec<usize>,
    faces: Vec<usize>,
    /// Parallel to `faces`: the site across the edge from each face to the
    /// next one around the site (for the last face of a hull cell, the far
    /// end of the outgoing hull edge).
    neighbors: Vec<u32>,
    boundaries: Vec<Option<(usize, usize)>>,
}

/// Where one site's walk writes its faces and neighbours.
struct IncidenceOutput<'a> {
    faces: &'a mut [usize],
    neighbors: &'a mut [u32],
    flat: &'a dyn Fn(usize) -> usize,
}

fn incident_faces_for_site(
    start: usize,
    halfedges: &[usize],
    mut output: Option<IncidenceOutput<'_>>,
) -> (usize, Option<(usize, usize)>) {
    if start == usize::MAX {
        return (0, None);
    }
    let mut length = 0;
    let mut incoming = start;
    loop {
        let outgoing = dual_next_halfedge(incoming);
        if let Some(output) = output.as_mut() {
            output.faces[length] = incoming / 3;
            // `outgoing` runs from the site to the neighbour shared with the
            // next face around it.
            output.neighbors[length] = (output.flat)(dual_next_halfedge(outgoing)) as u32;
        }
        length += 1;
        let next = halfedges[outgoing];
        if next == EMPTY {
            // `start` was a hull half-edge (q -> site) with no twin and
            // `outgoing` is the hull half-edge (site -> r).
            return (length, Some((start, outgoing)));
        }
        incoming = next;
        if incoming == start {
            return (length, None);
        }
    }
}

/// Cap one unbounded dual ray on the private frame. Identical construction to
/// the shipped hull-edge lane in `emit_clipped_separators`.
fn dual_ray_hit(
    sites: &[Site],
    pair: [usize; 2],
    opposite: usize,
    frame: &[exact::ExactPoint; 4],
) -> Result<(u8, exact::ExactPoint)> {
    let bisector_sites = [sites[pair[0]].point.xy(), sites[pair[1]].point.xy()];
    let left = exact::ExactPoint::from_xy(bisector_sites[0]);
    let right = exact::ExactPoint::from_xy(bisector_sites[1]);
    let opposite_sign = exact::orient_points(
        &left,
        &right,
        &exact::ExactPoint::from_xy(sites[opposite].point.xy()),
    );
    bisector_frame_intersections(bisector_sites, frame)?
        .into_iter()
        .find(|(_, point)| exact::orient_points(&left, &right, point) != opposite_sign)
        .ok_or_else(|| voronoi_error("certified Voronoi hull ray misses exact frame"))
}

fn dual_close_hull_chain(
    faces: &[usize],
    neighbors: &[u32],
    entry_site: u32,
    complex: &DelaunayComplex,
    center_points: &[exact::ExactPoint],
    entry_ray: &(u8, exact::ExactPoint),
    exit_ray: &(u8, exact::ExactPoint),
    frame: &[exact::ExactPoint; 4],
    exterior: bool,
) -> Result<Vec<Tagged>> {
    let mut ring = Vec::with_capacity(faces.len() + 6);
    ring.push(Tagged {
        vertex: DualVertex::Generated(Box::new(entry_ray.1.clone())),
        leaving: EdgeTag::Bisector(entry_site),
    });
    ring.extend(
        faces
            .iter()
            .zip(neighbors)
            .map(|(&face, &neighbor)| Tagged {
                vertex: DualVertex::Center(complex.component_of_face(face) as u32),
                leaving: EdgeTag::Bisector(neighbor),
            }),
    );
    ring.push(Tagged {
        vertex: DualVertex::Generated(Box::new(exit_ray.1.clone())),
        leaving: EdgeTag::Axis,
    });
    let exit_side = exit_ray.0 as usize;
    let entry_side = entry_ray.0 as usize;
    let corner = |side: usize| Tagged {
        vertex: DualVertex::Generated(Box::new(frame[side].clone())),
        leaving: EdgeTag::Axis,
    };
    if exterior {
        let steps = match (exit_side + 4 - entry_side) % 4 {
            0 => 4,
            steps => steps,
        };
        let mut side = exit_side;
        for _ in 0..steps {
            ring.push(corner(side));
            side = (side + 3) % 4;
        }
    } else {
        let steps = (entry_side + 4 - exit_side) % 4;
        let mut side = exit_side;
        for _ in 0..steps {
            side = (side + 1) % 4;
            ring.push(corner(side));
        }
    }
    dedup_tagged(&mut ring, center_points);
    let vertices: Vec<DualVertex> = ring.iter().map(|tagged| tagged.vertex.clone()).collect();
    if dual_cycle_orientation(&vertices, center_points)? == ExactSign::Negative {
        reverse_tagged(&mut ring);
    }
    Ok(ring)
}

fn dual_vertex_point<'a>(
    vertex: &'a DualVertex,
    centers: &'a [exact::ExactPoint],
) -> Option<&'a exact::ExactPoint> {
    match vertex {
        DualVertex::Center(component) => centers.get(*component as usize),
        DualVertex::Generated(point) => Some(point),
    }
}

fn dual_cycle_orientation(ring: &[DualVertex], centers: &[exact::ExactPoint]) -> Result<ExactSign> {
    if ring.len() < 3 {
        return Err(voronoi_error("exact cycle has fewer than three vertices"));
    }
    for index in 0..ring.len() {
        let left = dual_vertex_point(&ring[index], centers).expect("valid center id");
        let center =
            dual_vertex_point(&ring[(index + 1) % ring.len()], centers).expect("valid center id");
        let right =
            dual_vertex_point(&ring[(index + 2) % ring.len()], centers).expect("valid center id");
        let sign = exact::orient_points(left, center, right);
        if sign != ExactSign::Zero {
            return Ok(sign);
        }
    }
    Err(voronoi_error("exact cycle has zero orientation"))
}

fn dual_point_in_cycle(
    cycle: &[DualVertex],
    centers: &[exact::ExactPoint],
    point: &exact::ExactPoint,
) -> exact::PointInCycle {
    let mut winding = 0_i32;
    let on_segment = |start: &exact::ExactPoint, end: &exact::ExactPoint| {
        let between = |left: Ordering, right: Ordering| {
            (left != Ordering::Less && right != Ordering::Greater)
                || (right != Ordering::Less && left != Ordering::Greater)
        };
        exact::orient_points(start, end, point) == ExactSign::Zero
            && between(point.compare_x(start), point.compare_x(end))
            && between(point.compare_y(start), point.compare_y(end))
    };
    for index in 0..cycle.len() {
        let start = dual_vertex_point(&cycle[index], centers).expect("valid center id");
        let end =
            dual_vertex_point(&cycle[(index + 1) % cycle.len()], centers).expect("valid center id");
        if on_segment(start, end) {
            return exact::PointInCycle::Boundary;
        }
        let start_y = start.compare_y(point);
        let end_y = end.compare_y(point);
        let orientation = exact::orient_points(start, end, point);
        if start_y != Ordering::Greater
            && end_y == Ordering::Greater
            && orientation == ExactSign::Positive
        {
            winding += 1;
        } else if end_y != Ordering::Greater
            && start_y == Ordering::Greater
            && orientation == ExactSign::Negative
        {
            winding -= 1;
        }
    }
    if winding == 0 {
        exact::PointInCycle::Outside
    } else {
        exact::PointInCycle::Inside
    }
}

/// Sutherland-Hodgman against a CCW convex rectangle, in exact arithmetic.
/// "Inside" is `orient_points(side_start, side_end, p) != Negative`, i.e. the
/// same exact predicate the rest of the module uses; a crossing vertex is the
/// exact intersection of two support lines, never a rounded one.
/// Sutherland-Hodgman against the clip rectangle, in exact arithmetic.
///
/// The clip is axis-aligned, so the half-plane test collapses from a general
/// orientation determinant to ONE coordinate comparison. For the bottom side
/// `a = (minx, miny) -> b = (maxx, miny)` the determinant is
/// `(maxx - minx) * (py - miny)`, and `maxx > minx` is checked once by the
/// caller, so its sign IS the sign of `py - miny`. The same reduction holds on
/// the other three sides. Identical verdict, one filtered comparison instead of
/// six interval operations. A degenerate rectangle falls back to the general
/// orientation test.
fn dual_clip_convex(
    ring: Vec<Tagged>,
    site: XY,
    sites: &[Site],
    rect: &[exact::ExactPoint; 4],
    axis_aligned: bool,
    centers: &[exact::ExactPoint],
) -> Result<Vec<Tagged>> {
    // Certified crossings need the clip sides as doubles.
    let corners: Option<[XY; 4]> = axis_aligned
        .then(|| {
            rect.iter()
                .map(exact::ExactPoint::as_double)
                .collect::<Option<Vec<_>>>()
        })
        .flatten()
        .and_then(|corners| corners.try_into().ok());
    let mut current = ring;
    for side in 0..4 {
        if current.is_empty() {
            return Ok(current);
        }
        let (a, b) = (&rect[side], &rect[(side + 1) % 4]);
        let inside = |point: &exact::ExactPoint| -> bool {
            if !axis_aligned {
                return exact::orient_points(a, b, point) != ExactSign::Negative;
            }
            match side {
                0 => point.compare_y(&rect[0]) != Ordering::Less,
                1 => point.compare_x(&rect[1]) != Ordering::Greater,
                2 => point.compare_y(&rect[2]) != Ordering::Greater,
                _ => point.compare_x(&rect[3]) != Ordering::Less,
            }
        };
        // The crossing of the edge `from -> to` with this side. A bisector
        // edge meets the side at a certified rounding; an axis-aligned edge
        // meets it at an exact double; anything the certificate declines is
        // the exact intersection of the two support lines, as before.
        let crossing = |from: &exact::ExactPoint,
                        tag: EdgeTag,
                        to: &exact::ExactPoint|
         -> Result<Option<exact::ExactPoint>> {
            if let Some(corners) = corners {
                let ends = [corners[side], corners[(side + 1) % 4]];
                match tag {
                    EdgeTag::Bisector(neighbor) => {
                        let pair = [site, sites[neighbor as usize].point.xy()];
                        if let exact::FrameHit::Point(hit) =
                            exact::ExactPoint::certified_bisector_hit(pair, ends)
                        {
                            return Ok(Some(hit));
                        }
                    },
                    EdgeTag::Axis => {
                        // An edge along a frame or clip side shares one exact
                        // coordinate: a ray hit's side coordinate is exact in
                        // its bounds, a corner is a double. The crossing with
                        // a perpendicular side is then an exact double.
                        let shared = |axis: usize| {
                            let (from, to) = (from.bounds()[axis], to.bounds()[axis]);
                            (from.lo == from.hi && to.lo == to.hi && from.lo == to.lo)
                                .then_some(from.lo)
                        };
                        let hit = match side {
                            0 | 2 => shared(0).map(|x| XY::new(x, ends[0].y)),
                            _ => shared(1).map(|y| XY::new(ends[0].x, y)),
                        };
                        if let Some(hit) = hit
                            && hit.x.is_finite()
                            && hit.y.is_finite()
                        {
                            return Ok(Some(exact::ExactPoint::from_xy(hit)));
                        }
                    },
                }
            }
            let boundary = exact::ExactLine::through_points(a, b)?;
            let edge = exact::ExactLine::through_points(from, to)?;
            let hit = exact::line_intersection(&boundary, &edge);
            Ok(hit.is_finite().then_some(hit))
        };
        let mut next = Vec::with_capacity(current.len() + 1);
        for index in 0..current.len() {
            let this_point =
                dual_vertex_point(&current[index].vertex, centers).expect("valid center id");
            let this_in = inside(this_point);
            let previous = (index + current.len() - 1) % current.len();
            let previous_point =
                dual_vertex_point(&current[previous].vertex, centers).expect("valid center id");
            let prev_in = inside(previous_point);
            if this_in != prev_in {
                let tag = current[previous].leaving;
                if let Some(hit) = crossing(previous_point, tag, this_point)? {
                    next.push(Tagged {
                        vertex: DualVertex::Generated(Box::new(hit)),
                        // Entering: the rest of the original edge follows.
                        // Leaving: the new edge runs along this side.
                        leaving: if this_in { tag } else { EdgeTag::Axis },
                    });
                }
            }
            if this_in {
                next.push(current[index].clone());
            }
        }
        dedup_tagged(&mut next, centers);
        current = next;
    }
    Ok(current)
}

fn dual_build_rings(
    sites: &[Site],
    mesh: &CertifiedDelaunay,
    complex: DelaunayComplex,
    clip: &NormalizedClip<'_>,
    budget: &mut ExpansionBudget,
) -> Result<DualRings> {
    let centers = &complex.centers;
    let site_points: Vec<_> = sites
        .iter()
        .map(|site| exact::ExactPoint::from_xy(site.point.xy()))
        .collect();
    let frame_inputs = || {
        site_points
            .iter()
            .chain(clip.rings.iter().flatten())
            .chain(centers.iter())
    };
    let frame = match exact::enclosing_frame_binary64(frame_inputs()) {
        Some(frame) => frame,
        None => exact::enclosing_frame(&frame_inputs().cloned().collect::<Vec<_>>())?,
    };
    let rect: [exact::ExactPoint; 4] = clip.rings[0]
        .clone()
        .try_into()
        .map_err(|_| voronoi_error("certified rectangular clip requires four corners"))?;
    // Certified once: the clip really is a positively-oriented axis-aligned
    // rectangle with strictly positive extent. Only then may the half-plane
    // test collapse to a coordinate comparison.
    let axis_aligned = rect[0].compare_y(&rect[1]).is_eq()
        && rect[2].compare_y(&rect[3]).is_eq()
        && rect[1].compare_x(&rect[2]).is_eq()
        && rect[3].compare_x(&rect[0]).is_eq()
        && rect[0].compare_x(&rect[1]).is_lt()
        && rect[1].compare_y(&rect[2]).is_lt();

    // A box certified to lie inside the rectangle, for the interior fast
    // lane: the inner edges of the corners' certified intervals. A padded
    // corner is a dyadic that need not be a double, and this stays exact for
    // it; an unbounded interval fails every strict comparison and keeps the
    // cell on the exact lane.
    let interior_bounds = axis_aligned.then(|| {
        let (low, high) = (rect[0].bounds(), rect[2].bounds());
        [low[0].hi, high[0].lo, low[1].hi, high[1].lo]
    });
    // The incidence walk has one rotational sense for the whole mesh; the
    // first interior cell settles it exactly and every later one reuses it.
    let mut walk_clockwise: Option<bool> = None;

    let triangles = mesh.triangles();
    let halfedges = mesh.halfedges();
    let flat = |edge: usize| triangles[edge / 3][edge % 3];
    let incidence = dual_incident_faces(mesh, sites.len());
    let mut rings = Vec::with_capacity(sites.len());

    for site in 0..sites.len() {
        let faces = &incidence.faces[incidence.offsets[site]..incidence.offsets[site + 1]];
        let boundary = incidence.boundaries[site];
        budget.add(faces.len().saturating_add(1))?;
        let neighbors = &incidence.neighbors[incidence.offsets[site]..incidence.offsets[site + 1]];
        let untag = |ring: &[Tagged]| {
            ring.iter()
                .map(|tagged| tagged.vertex.clone())
                .collect::<Vec<_>>()
        };
        let ring = if let Some((first_hull, last_hull)) = boundary {
            // (q -> site) and (site -> r): the two hull edges bounding the cell.
            let entry = [flat(first_hull), flat(dual_next_halfedge(first_hull))];
            let entry_opposite = flat(dual_next_halfedge(dual_next_halfedge(first_hull)));
            let exit = [flat(last_hull), flat(dual_next_halfedge(last_hull))];
            let exit_opposite = flat(dual_next_halfedge(dual_next_halfedge(last_hull)));
            debug_assert_eq!(halfedges[first_hull], EMPTY);
            debug_assert_eq!(halfedges[last_hull], EMPTY);
            let entry_ray = dual_ray_hit(sites, entry, entry_opposite, &frame)?;
            let exit_ray = dual_ray_hit(sites, exit, exit_opposite, &frame)?;
            let site_point = exact::ExactPoint::from_xy(sites[site].point.xy());
            // Positive topology fixes the open center chain. The two frame
            // paths are complementary clipped faces; exact ownership decides
            // only which one contains this site, including the zero-separator
            // isolated-ring case. Ring handedness is normalization, not a
            // second candidate.
            let interior = dual_close_hull_chain(
                faces,
                neighbors,
                entry[0] as u32,
                &complex,
                centers,
                &entry_ray,
                &exit_ray,
                &frame,
                false,
            )?;
            if dual_point_in_cycle(&untag(&interior), centers, &site_point)
                == exact::PointInCycle::Inside
            {
                interior
            } else {
                let exterior = dual_close_hull_chain(
                    faces,
                    neighbors,
                    entry[0] as u32,
                    &complex,
                    centers,
                    &entry_ray,
                    &exit_ray,
                    &frame,
                    true,
                )?;
                if dual_point_in_cycle(&untag(&exterior), centers, &site_point)
                    != exact::PointInCycle::Inside
                {
                    return Err(voronoi_error(
                        "dual hull cell has no admissible frame closure",
                    ));
                }
                exterior
            }
        } else {
            // Interior fast lane. Two distinct components around one site have
            // distinct exact centers (both circles pass through the site, so a
            // shared center would be one circle, hence one component), which
            // makes consecutive-id dedup exact. The certified mesh orders the
            // faces counter-clockwise, and every center strictly inside the
            // rectangle means the clip is the identity; the embedding's
            // certificate still proves the rounded cell.
            let mut ring: Vec<Tagged> = Vec::with_capacity(faces.len() + 6);
            let mut interior = interior_bounds.is_some();
            for (&face, &neighbor) in faces.iter().zip(neighbors) {
                let component = complex.component_of_face(face);
                match ring.last_mut() {
                    // Cocircular faces share the vertex; the edge leaving it
                    // is the last face's.
                    Some(last) if matches!(last.vertex, DualVertex::Center(id) if id as usize == component) =>
                    {
                        last.leaving = EdgeTag::Bisector(neighbor);
                    },
                    _ => ring.push(Tagged {
                        vertex: DualVertex::Center(component as u32),
                        leaving: EdgeTag::Bisector(neighbor),
                    }),
                }
                interior = interior
                    && interior_bounds
                        .is_some_and(|bounds| centers[component].strictly_inside(bounds));
            }
            if ring.len() > 1
                && let (DualVertex::Center(first), DualVertex::Center(last)) =
                    (&ring[0].vertex, &ring[ring.len() - 1].vertex)
                && first == last
            {
                let leaving = ring[ring.len() - 1].leaving;
                ring.pop();
                ring[0].leaving = leaving;
            }
            if interior && ring.len() >= 3 {
                let clockwise = if let Some(clockwise) = walk_clockwise {
                    clockwise
                } else {
                    let clockwise =
                        dual_cycle_orientation(&untag(&ring), centers)? == ExactSign::Negative;
                    walk_clockwise = Some(clockwise);
                    clockwise
                };
                let mut ring = untag(&ring);
                if clockwise {
                    ring.reverse();
                }
                budget.add(ring.len())?;
                rings.push(ring);
                continue;
            }
            dedup_tagged(&mut ring, centers);
            if ring.len() >= 3
                && dual_cycle_orientation(&untag(&ring), centers)? == ExactSign::Negative
            {
                reverse_tagged(&mut ring);
            }
            ring
        };
        let clipped = dual_clip_convex(
            ring,
            sites[site].point.xy(),
            sites,
            &rect,
            axis_aligned,
            centers,
        )?;
        if clipped.len() < 3 {
            return Err(voronoi_error(
                "dual Voronoi cell degenerated under the clip",
            ));
        }
        budget.add(clipped.len())?;
        rings.push(untag(&clipped));
    }
    Ok(DualRings {
        rings,
        centers: complex.centers,
    })
}

fn lex_xy(left: XY, right: XY) -> Ordering {
    left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
}

/// Accept a rounded cell on its doubles alone: a positively oriented, convex,
/// simple ring that covers `site`. Every turn is a left turn or a forward
/// collinear step (a clipped hull cell keeps the points where its rays meet a
/// rectangle side, so consecutive vertices on one side are collinear), never
/// a right turn, a repeated vertex, or a reversal; and the lexicographic edge
/// direction changes exactly twice around the ring, which pins the turning
/// number to one: a pentagram turns left at every vertex and changes direction
/// four times. `site` may lie on an edge. Each sign comes from the exact
/// predicate, so a declined float filter costs one dyadic evaluation, never a
/// whole exact cell, and a refusal here only routes the cell to the exact
/// certificates.
fn certify_convex_cell(ring: &[XY], site: XY) -> bool {
    let count = ring.len();
    if count < 3 {
        return false;
    }
    let at = |index: usize| ring[index % count];
    let mut direction_changes = 0_usize;
    for index in 0..count {
        let (a, b, c) = (at(index), at(index + 1), at(index + 2));
        let (ahead, onward) = (lex_xy(a, b), lex_xy(b, c));
        // Three vertices on one axis-aligned side (a clipped hull cell's
        // frame or clip edge) are exactly collinear; no filter can certify
        // a zero, so decide it on the shared coordinate.
        let turn = if (a.x == b.x && b.x == c.x) || (a.y == b.y && b.y == c.y) {
            ExactSign::Zero
        } else {
            exact::orient2d(a, b, c)
        };
        match turn {
            ExactSign::Positive => {},
            ExactSign::Zero if ahead == onward && ahead != Ordering::Equal => {},
            ExactSign::Zero | ExactSign::Negative => return false,
        }
        if ahead != onward {
            direction_changes += 1;
        }
        if exact::orient2d(a, b, site) == ExactSign::Negative {
            return false;
        }
    }
    direction_changes == 2
}

/// Round the dual once and run the certificates that survive the redesign.
///
/// What is UNCHANGED from `embed`: the sole nearest-even rounding, the global
/// injectivity check over distinct exact vertices, the per-ring orientation
/// check against the rounded ring, and `polygon_is_valid` per cell.
///
/// What is GONE, because the structure it certified no longer exists: the
/// arrangement's rounded no-crossing scan and the per-vertex exact-vs-rounded
/// fan-order comparison. The dual never builds that arrangement. Instead,
/// rounded edge cancellation proves that the valid cells have exactly the
/// rectangular clip boundary, and exact point membership checks each canonical
/// site's rounded cell. Polygon clips remain on the exact arrangement lane.
fn dual_embed(
    rings: &DualRings,
    clip: &NormalizedClip<'_>,
    sites: &[Site],
    budget: &mut ExpansionBudget,
) -> Result<EmbeddedVoronoi> {
    if rings.rings.len() != sites.len() {
        return Err(embedding_failure());
    }
    // Vertex identity WITHOUT an exact positional sort.
    //
    // A `Center` vertex IS its component, so its rounding is memoised once per
    // component. Everything else is hashed on its ROUNDED key, and a key
    // collision is then resolved EXACTLY: two exact points that share a rounded
    // key are the same vertex only if `same_position` says so, and otherwise the
    // rounding is not injective and the diagram is refused. That is the same
    // verdict the sort produced -- rounding is a function, so equal exact points
    // always land in the same bucket -- reached in O(V) instead of O(V log V)
    // exact comparisons.
    // Every distinct vertex also gets one dense id: a center's component id,
    // or the next id after the components for a generated vertex. With the
    // rounding proven injective, ids and rounded points are in bijection, so
    // the cancellation certificate below can run on packed integer pairs.
    // A center memoises its rounding AND its id: a clip hit interned earlier
    // at the same position owns the vertex, and every later reference to the
    // center must carry that id, or the cancellation certificate sees one
    // edge under two names.
    let mut center_rounded: Vec<Option<(XY, u32)>> = vec![None; rings.centers.len()];
    let mut interned: HashMap<PointKey, (&exact::ExactPoint, u32)> = HashMap::new();
    let mut vertex_xy: Vec<XY> = vec![XY::new(0.0, 0.0); rings.centers.len()];
    fn resolve<'r>(
        vertex: &'r DualVertex,
        centers: &'r [exact::ExactPoint],
        center_rounded: &mut [Option<(XY, u32)>],
        interned: &mut HashMap<PointKey, (&'r exact::ExactPoint, u32)>,
        vertex_xy: &mut Vec<XY>,
    ) -> Result<(XY, u32)> {
        if let DualVertex::Center(component) = *vertex
            && let Some(memo) = center_rounded[component as usize]
        {
            return Ok(memo);
        }
        let point = dual_vertex_point(vertex, centers).expect("valid center id");
        let mut xy = point.round_nearest_even()?;
        let key = PointKey::new(xy);
        let id = match interned.entry(key) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                let &(representative, id) = existing.get();
                if !representative.same_position(point) {
                    return Err(embedding_failure());
                }
                // Equal exact generated vertices can reach signed zero through
                // different algebraic expressions. Reuse the first admitted
                // representative so shared binary64 vertices are bit-identical.
                xy = representative.round_nearest_even()?;
                id
            },
            std::collections::hash_map::Entry::Vacant(slot) => {
                let id = if let DualVertex::Center(component) = *vertex {
                    component
                } else {
                    vertex_xy.push(xy);
                    u32::try_from(vertex_xy.len() - 1).map_err(|_| embedding_failure())?
                };
                slot.insert((point, id));
                id
            },
        };
        if let DualVertex::Center(component) = *vertex {
            center_rounded[component as usize] = Some((xy, id));
        }
        vertex_xy[id as usize] = xy;
        Ok((xy, id))
    }
    let mut offsets = Vec::with_capacity(rings.rings.len() + 1);
    offsets.push(0);
    let total: usize = rings.rings.iter().map(Vec::len).sum();
    let mut rounded_all: Vec<XY> = Vec::with_capacity(total);
    let mut ids_all: Vec<u32> = Vec::with_capacity(total);
    for ring in &rings.rings {
        for vertex in ring {
            let (xy, id) = resolve(
                vertex,
                &rings.centers,
                &mut center_rounded,
                &mut interned,
                &mut vertex_xy,
            )?;
            rounded_all.push(xy);
            ids_all.push(id);
        }
        offsets.push(rounded_all.len());
    }
    budget.add(rounded_all.len())?;
    let mut polygons = Vec::with_capacity(rings.rings.len());
    for site_index in 0..rings.rings.len() {
        let rounded = &mut rounded_all[offsets[site_index]..offsets[site_index + 1]];
        // The ring starts at its lexicographically smallest rounded vertex,
        // the canonical form both lanes publish; rounding is injective here,
        // so the minimum is unique.
        let start = rounded
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| lex_xy(**left, **right))
            .map_or(0, |(index, _)| index);
        rounded.rotate_left(start);
        let site_xy = sites[site_index].point.xy();
        if !certify_convex_cell(rounded, site_xy) {
            let rounded_ring: Vec<_> = rounded
                .iter()
                .map(|&xy| exact::ExactPoint::from_xy(xy))
                .collect();
            if exact::cycle_orientation(&rounded_ring)? != ExactSign::Positive {
                return Err(embedding_failure());
            }
            let closed: Vec<Point> = rounded
                .iter()
                .chain(std::iter::once(&rounded[0]))
                .map(|xy| Point::new_raw_xy(xy.x, xy.y))
                .collect();
            if !polygon_is_valid(&Polygon::new(Ring::closed(closed)?, Vec::new())?)? {
                return Err(embedding_failure());
            }
            let site = exact::ExactPoint::from_xy(site_xy);
            if exact::point_in_cycle(&rounded_ring, &site) == exact::PointInCycle::Outside {
                return Err(embedding_failure());
            }
        }
        budget.add(rounded.len())?;
        let closed = rounded.iter().chain(std::iter::once(&rounded[0]));
        let xs: Vec<f64> = closed.clone().map(|xy| xy.x).collect();
        let ys: Vec<f64> = closed.map(|xy| xy.y).collect();
        let ring = Ring::closed_coordseq(CoordSeq::try_from_vecs(xs, ys, None, None)?)?;
        polygons.push(Polygon::new(ring, Vec::new())?);
    }
    // Completeness by EDGE CANCELLATION rather than by a full overlay union.
    //
    // Every rounded cell edge is entered directed. An edge interior to the
    // partition is walked once in each direction by its two owners and cancels;
    // an edge on the outer boundary is walked exactly once. So: if every
    // undirected edge occurs either twice with opposite directions or once, and
    // the once-occurring edges tile the clip rectangle's four sides exactly with
    // no gap and no overlap, then the union of the cells IS the clip rectangle.
    // Combined with positive simple-cell validation and exact site ownership,
    // this is the same partition statement the union/difference pair made,
    // computed on stored binary64 with no overlay. Winding-number membership
    // alone cannot prove this: a gap and an overlap can leave every tested site
    // inside its own cell, while cancellation and directed boundary tiling
    // reject both.
    // Each directed edge packs as (smaller id, larger id, forward?), so equal
    // undirected edges are adjacent after one integer sort and a run of two
    // with opposite directions cancels.
    let mut directed: Vec<u64> = Vec::with_capacity(total);
    for site_index in 0..rings.rings.len() {
        let ids = &ids_all[offsets[site_index]..offsets[site_index + 1]];
        for index in 0..ids.len() {
            let (from, to) = (ids[index], ids[(index + 1) % ids.len()]);
            let (low, high, forward) = if from <= to {
                (from, to, 1)
            } else {
                (to, from, 0)
            };
            directed.push((u64::from(low) << 33) | (u64::from(high) << 1) | forward);
        }
    }
    budget.add(directed.len())?;
    directed.sort_unstable();
    let bounds = clip.frame_bounds();
    // side 0: y == miny, 1: x == maxx, 2: y == maxy, 3: x == minx
    let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    let mut internal_edges = Vec::new();
    let mut cursor = 0;
    while cursor < directed.len() {
        let key = directed[cursor] >> 1;
        let mut run = cursor + 1;
        while run < directed.len() && directed[run] >> 1 == key {
            run += 1;
        }
        let (low, high) = ((key >> 32) as usize, (key & 0xFFFF_FFFF) as usize);
        let (a, b) = (vertex_xy[low], vertex_xy[high]);
        let count = run - cursor;
        if count == 2 {
            if directed[cursor] & 1 == directed[cursor + 1] & 1 {
                return Err(embedding_failure());
            }
            internal_edges.push([a, b]);
            cursor = run;
            continue;
        }
        if count != 1 {
            return Err(embedding_failure());
        }
        let forward = directed[cursor] & 1 == 1;
        cursor = run;
        let (a, b) = if forward { (a, b) } else { (b, a) };
        let side = if a.y == bounds[2] && b.y == bounds[2] {
            0
        } else if a.x == bounds[1] && b.x == bounds[1] {
            1
        } else if a.y == bounds[3] && b.y == bounds[3] {
            2
        } else if a.x == bounds[0] && b.x == bounds[0] {
            3
        } else {
            return Err(embedding_failure());
        };
        let correctly_directed = match side {
            0 => a.x < b.x,
            1 => a.y < b.y,
            2 => a.x > b.x,
            _ => a.y > b.y,
        };
        if !correctly_directed {
            return Err(embedding_failure());
        }
        let (lo, hi) = if side % 2 == 0 {
            (a.x.min(b.x), a.x.max(b.x))
        } else {
            (a.y.min(b.y), a.y.max(b.y))
        };
        sides[side].push((lo, hi));
    }
    for (index, side) in sides.iter_mut().enumerate() {
        side.sort_by(|left, right| left.0.total_cmp(&right.0));
        let (start, end) = if index % 2 == 0 {
            (bounds[0], bounds[1])
        } else {
            (bounds[2], bounds[3])
        };
        let mut cursor = start;
        for &(lo, hi) in side.iter() {
            if lo != cursor {
                return Err(embedding_failure());
            }
            cursor = hi;
        }
        if cursor != end {
            return Err(embedding_failure());
        }
    }
    let edges = EmbeddedEdges::Segments(internal_edges);
    let clip_coordinates: Vec<Point> = clip.fixed[0]
        .iter()
        .map(|xy| Point::new_raw_xy(xy.x, xy.y))
        .chain(std::iter::once(Point::new_raw_xy(
            clip.fixed[0][0].x,
            clip.fixed[0][0].y,
        )))
        .collect();
    let comparison_clip =
        Shape::Polygon(Polygon::new(Ring::closed(clip_coordinates)?, Vec::new())?);
    Ok(EmbeddedVoronoi {
        polygons,
        edges,
        clip: comparison_clip,
    })
}

pub(super) fn dual_build(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: CertifiedDelaunay,
) -> Result<EmbeddedVoronoi> {
    let clip = normalize_clip(sites, boundary, budget)?;
    if mesh.collinear_order().is_some() {
        return Err(voronoi_error(
            "collinear sites require the exact one-dimensional lane",
        ));
    }
    let complex = delaunay_complex(&mesh, sites)?;
    let rings = dual_build_rings(sites, &mesh, complex, &clip, budget)?;
    dual_embed(&rings, &clip, sites, budget)
}
