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

use crate::error::Result;
use crate::geometry::tessellation::{
    CertifiedDelaunay, CertifiedPrimalEdge, DelaunayComplex, Site, certified_delaunay,
    delaunay_complex, exact,
};
use crate::geometry::{
    BulkRTree, CoordSeq, ExpansionBudget, GeometryErrorKind, HashMap, HashMapExt as _, LineSeq,
    Point, PointKey, Polygon, RTreeObject as _, Ring, Shape, Strictness, VoronoiBoundary, XY,
    coverage_is_valid, polygon_is_valid,
};
#[cfg(test)]
use crate::geometry::{GENERATED_ITEM_LIMIT, tessellation::shape};

macro_rules! dense_id {
    ($name:ident) => {
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        struct $name(u32);
    };
}

dense_id!(SiteId);
dense_id!(PrimitiveId);
dense_id!(VertexId);
dense_id!(EdgeId);
dense_id!(HalfEdgeId);
dense_id!(GraphComponentId);
dense_id!(OrbitId);
dense_id!(FaceId);
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

struct DirectedPrimitive {
    id: PrimitiveId,
    line: exact::ExactLine,
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    frame_delta: i8,
    clip_delta: i32,
    owner: PrimitiveOwner,
    separator: Option<[SiteId; 2]>,
    clip_source: Option<ClipSegmentId>,
    frame_side: Option<u8>,
    events: Vec<exact::ExactPoint>,
}

struct NormalizedClip<'a> {
    rings: Vec<Vec<exact::ExactPoint>>,
    fixed: Vec<Vec<XY>>,
    source: Option<&'a Polygon>,
    contacts: Vec<ExactCycleContact>,
}

#[derive(Clone)]
struct AtomSeed {
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    transition: Transition,
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
    if polygon_is_valid(polygon) {
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
            if !polygon_is_valid(polygon) {
                return Err(voronoi_error("Voronoi clip polygon is invalid"));
            }
            for (ring_index, ring) in std::iter::once(&polygon.shell)
                .chain(polygon.holes.iter())
                .enumerate()
            {
                let (mut points, mut coordinates) = open_ring(ring)?;
                let orientation = exact::cycle_orientation(&points)?;
                let wanted = if ring_index == 0 {
                    exact::ExactSign::Positive
                } else {
                    exact::ExactSign::Negative
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
    degrees
        .try_reserve_exact(site_count)
        .map_err(|_| voronoi_error("could not allocate certified site adjacency"))?;
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
    offsets
        .try_reserve_exact(site_count + 1)
        .map_err(|_| voronoi_error("could not allocate certified site adjacency"))?;
    offsets.push(0_u32);
    for degree in degrees {
        let next = (offsets.last().copied().unwrap_or(0) as usize)
            .checked_add(degree)
            .ok_or_else(|| voronoi_error("too many site adjacencies"))?;
        offsets.push(u32::try_from(next).map_err(|_| voronoi_error("too many site adjacencies"))?);
    }
    let neighbor_count = offsets.last().copied().unwrap_or(0) as usize;
    let mut neighbors = Vec::new();
    neighbors
        .try_reserve_exact(neighbor_count)
        .map_err(|_| voronoi_error("could not allocate certified site adjacency"))?;
    neighbors.resize(neighbor_count, SiteId(0));
    let mut cursors = Vec::new();
    cursors
        .try_reserve_exact(site_count)
        .map_err(|_| voronoi_error("could not allocate certified site adjacency"))?;
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
        exact::ExactSign::Positive => {},
        exact::ExactSign::Negative => std::mem::swap(&mut start, &mut end),
        exact::ExactSign::Zero => {
            return Err(voronoi_error("Voronoi separator contains its source site"));
        },
    }
    Ok((start, end))
}

fn frame_intersections(
    line: &exact::ExactLine,
    frame: &[exact::ExactPoint; 4],
) -> Result<Vec<(u8, exact::ExactPoint)>> {
    let mut hits = Vec::new();
    for side in 0..4 {
        let edge = exact::ExactLine::through_points(&frame[side], &frame[(side + 1) % 4])?;
        let hit = exact::line_intersection(line, &edge);
        if exact::signed_line_product(line, &hit) == exact::ExactSign::Zero
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

fn build_primitives(
    sites: &[Site],
    primal_edges: &[CertifiedPrimalEdge],
    complex: &DelaunayComplex,
    clip: &NormalizedClip<'_>,
    budget: &mut ExpansionBudget,
) -> Result<Vec<DirectedPrimitive>> {
    let mut frame_inputs: Vec<_> = sites
        .iter()
        .map(|site| exact::ExactPoint::from_xy(site.point.xy()))
        .collect();
    frame_inputs.extend(clip.rings.iter().flatten().cloned());
    frame_inputs.extend((0..complex.component_count()).map(|id| complex.center(id).clone()));
    let frame = match exact::enclosing_frame_binary64(&frame_inputs) {
        Some(frame) => frame,
        None => exact::enclosing_frame(&frame_inputs)?,
    };
    let mut primitives = Vec::new();
    for side in 0..4 {
        primitives.push(DirectedPrimitive {
            id: PrimitiveId(0),
            line: exact::ExactLine::through_points(&frame[side], &frame[(side + 1) % 4])?,
            start: frame[side].clone(),
            end: frame[(side + 1) % 4].clone(),
            frame_delta: 1,
            clip_delta: 0,
            owner: PrimitiveOwner::FrameNearest,
            separator: None,
            clip_source: None,
            frame_side: Some(side as u8),
            events: Vec::new(),
        });
    }
    let mut clip_segment = 0_u32;
    for ring in &clip.rings {
        for index in 0..ring.len() {
            primitives.push(DirectedPrimitive {
                id: PrimitiveId(0),
                line: exact::ExactLine::through_points(
                    &ring[index],
                    &ring[(index + 1) % ring.len()],
                )?,
                start: ring[index].clone(),
                end: ring[(index + 1) % ring.len()].clone(),
                frame_delta: 0,
                clip_delta: 1,
                owner: PrimitiveOwner::Preserve,
                separator: None,
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
            let primitive = primitives
                .iter_mut()
                .find(|primitive| primitive.clip_source == Some(ClipSegmentId(segment as u32)))
                .ok_or_else(|| voronoi_error("validated clip contact has no source segment"))?;
            budgeted_try_push(&mut primitive.events, contact.point.clone(), budget)?;
        }
    }
    for &primal in primal_edges {
        let (site_pair, start, end) = match primal {
            CertifiedPrimalEdge::Interior { sites: pair, faces } => {
                let components = [
                    complex.component_of_face(faces[0]),
                    complex.component_of_face(faces[1]),
                ];
                if components[0] == components[1] {
                    continue;
                }
                (
                    pair,
                    complex.center(components[0]).clone(),
                    complex.center(components[1]).clone(),
                )
            },
            CertifiedPrimalEdge::Hull {
                sites: pair,
                face,
                opposite,
            } => {
                let center = complex.center(complex.component_of_face(face)).clone();
                let line = exact::ExactLine::perpendicular_bisector(
                    sites[pair[0]].point.xy(),
                    sites[pair[1]].point.xy(),
                );
                let opposite_point = exact::ExactPoint::from_xy(sites[opposite].point.xy());
                let opposite_sign = exact::orient_points(
                    &exact::ExactPoint::from_xy(sites[pair[0]].point.xy()),
                    &exact::ExactPoint::from_xy(sites[pair[1]].point.xy()),
                    &opposite_point,
                );
                let hits = frame_intersections(&line, &frame)?;
                let hit = hits
                    .into_iter()
                    .map(|(_, point)| point)
                    .find(|point| {
                        exact::orient_points(
                            &exact::ExactPoint::from_xy(sites[pair[0]].point.xy()),
                            &exact::ExactPoint::from_xy(sites[pair[1]].point.xy()),
                            point,
                        ) != opposite_sign
                    })
                    .ok_or_else(|| {
                        voronoi_error("certified Voronoi hull ray misses exact frame")
                    })?;
                (pair, center, hit)
            },
        };
        let ids = [SiteId(site_pair[0] as u32), SiteId(site_pair[1] as u32)];
        let (start, end) = orient_separator(start, end, ids, sites)?;
        budget.add(1)?;
        primitives
            .try_reserve(1)
            .map_err(|_| voronoi_error("could not allocate exact Voronoi subdivision"))?;
        primitives.push(DirectedPrimitive {
            id: PrimitiveId(0),
            line: exact::ExactLine::perpendicular_bisector(
                sites[site_pair[0]].point.xy(),
                sites[site_pair[1]].point.xy(),
            ),
            start,
            end,
            frame_delta: 0,
            clip_delta: 0,
            owner: PrimitiveOwner::Boundary {
                left: Some(ids[0]),
                right: Some(ids[1]),
            },
            separator: Some(ids),
            clip_source: None,
            frame_side: None,
            events: Vec::new(),
        });
    }
    for (index, primitive) in primitives.iter_mut().enumerate() {
        if exact::signed_line_product(&primitive.line, &primitive.start) != exact::ExactSign::Zero
            || exact::signed_line_product(&primitive.line, &primitive.end) != exact::ExactSign::Zero
        {
            return Err(voronoi_error(
                "exact Voronoi primitive endpoints leave their support line",
            ));
        }
        primitive.id =
            PrimitiveId(u32::try_from(index).map_err(|_| voronoi_error("too many primitives"))?);
        reserve_exact(&mut primitive.events, 2)?;
        primitive
            .events
            .extend([primitive.start.clone(), primitive.end.clone()]);
    }
    let clip_count = clip_segment as usize;
    let dual_start = 4_usize.saturating_add(clip_count);
    let dual_count = primitives.len().saturating_sub(dual_start);
    let pair_count = dual_count.saturating_mul(clip_count.saturating_add(4));
    budget.add(pair_count)?;
    let mut event_pairs = Vec::new();
    reserve_exact(&mut event_pairs, pair_count)?;
    for dual in dual_start..primitives.len() {
        event_pairs.extend((4..dual_start).map(|clip| (dual, clip, false)));
        event_pairs.extend((0..4).map(|frame| (dual, frame, true)));
    }
    for (left, right, dual_frame) in event_pairs {
        match exact::segment_intersection(
            &primitives[left].start,
            &primitives[left].end,
            &primitives[right].start,
            &primitives[right].end,
        ) {
            exact::SegmentIntersection::None => {},
            exact::SegmentIntersection::Point(point) => {
                if dual_frame {
                    if ![&primitives[left].start, &primitives[left].end]
                        .iter()
                        .any(|endpoint| endpoint.same_position(&point))
                    {
                        continue;
                    }
                }
                budgeted_try_push(&mut primitives[left].events, point.clone(), budget)?;
                budgeted_try_push(&mut primitives[right].events, point, budget)?;
            },
            exact::SegmentIntersection::Overlap { start, end } => {
                budget.add(4)?;
                reserve_exact(&mut primitives[left].events, 2)?;
                primitives[left].events.extend([start.clone(), end.clone()]);
                reserve_exact(&mut primitives[right].events, 2)?;
                primitives[right].events.extend([start, end]);
            },
        }
    }
    Ok(primitives)
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
    fixed
        .try_reserve_exact(points.len())
        .map_err(|_| voronoi_error("could not allocate exact fixed-coordinate table"))?;
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

fn node_primitives(
    mut primitives: Vec<DirectedPrimitive>,
    sites: &[Site],
    adjacency: &SiteAdjacency,
    clip: &NormalizedClip<'_>,
    budget: &mut ExpansionBudget,
) -> Result<NodedAtoms> {
    let mut seeds = Vec::new();
    for primitive in &mut primitives {
        primitive.events.sort_by(|left, right| {
            exact::compare_along(&primitive.start, &primitive.end, left, right)
        });
        primitive
            .events
            .dedup_by(|left, right| left.same_position(right));
        if !primitive
            .events
            .first()
            .is_some_and(|point| point.same_position(&primitive.start))
            || !primitive
                .events
                .last()
                .is_some_and(|point| point.same_position(&primitive.end))
        {
            return Err(voronoi_error(
                "exact primitive event order lost an endpoint",
            ));
        }
    }
    let mut frame_owner = SiteId(0);
    for primitive in &primitives {
        for pair in primitive.events.windows(2) {
            if exact::compare_along(&primitive.start, &primitive.end, &pair[0], &pair[1])
                != Ordering::Less
            {
                return Err(voronoi_error(
                    "exact primitive has unordered duplicate events",
                ));
            }
            let owner = match primitive.owner {
                PrimitiveOwner::Preserve => OwnerTransition::Preserve,
                PrimitiveOwner::Boundary { left, right } => {
                    OwnerTransition::Boundary { left, right }
                },
                PrimitiveOwner::FrameNearest => OwnerTransition::Boundary {
                    left: Some({
                        frame_owner = nearest_site_walk(
                            &pair[0].midpoint(&pair[1]),
                            sites,
                            adjacency,
                            frame_owner,
                            budget,
                        )?;
                        frame_owner
                    }),
                    right: None,
                },
            };
            budgeted_try_push(
                &mut seeds,
                AtomSeed {
                    start: pair[0].clone(),
                    end: pair[1].clone(),
                    transition: Transition {
                        frame_delta: primitive.frame_delta,
                        clip_delta: primitive.clip_delta,
                        owner,
                    },
                    separator: primitive.separator,
                    clip_source: primitive.clip_source,
                    frame_side: primitive.frame_side,
                },
                budget,
            )?;
        }
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
        transition: Transition,
        separator: Option<[SiteId; 2]>,
        clip_source: Option<ClipSegmentId>,
        frame_side: Option<u8>,
    }
    let mut mapped = Vec::new();
    reserve_exact(&mut mapped, seeds.len())?;
    for seed in seeds {
        let mut endpoints = [vertex_id(&seed.start)?, vertex_id(&seed.end)?];
        let mut transition = seed.transition;
        if endpoints[0] == endpoints[1] {
            return Err(voronoi_error("exact primitive produced a zero-length atom"));
        }
        if endpoints[0] > endpoints[1] {
            endpoints.swap(0, 1);
            transition = transition.reversed();
        }
        mapped.push(MappedSeed {
            endpoints,
            transition,
            separator: seed.separator,
            clip_source: seed.clip_source,
            frame_side: seed.frame_side,
        });
    }
    mapped.sort_by_key(|seed| seed.endpoints);
    let mut edges = Vec::new();
    let mut start = 0;
    while start < mapped.len() {
        let mut end = start + 1;
        while end < mapped.len() && mapped[end].endpoints == mapped[start].endpoints {
            end += 1;
        }
        let mut transition = Transition {
            frame_delta: 0,
            clip_delta: 0,
            owner: OwnerTransition::Preserve,
        };
        let mut separator = None;
        let mut clip_sources = Vec::new();
        let mut frame_side = None;
        for seed in &mapped[start..end] {
            transition.frame_delta = transition
                .frame_delta
                .checked_add(seed.transition.frame_delta)
                .ok_or_else(|| voronoi_error("exact frame winding overflow"))?;
            transition.clip_delta = transition
                .clip_delta
                .checked_add(seed.transition.clip_delta)
                .ok_or_else(|| voronoi_error("exact clip winding overflow"))?;
            transition.owner = merge_owner(transition.owner, seed.transition.owner)?;
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
        clip_sources.sort_unstable();
        clip_sources.dedup();
        if transition.frame_delta != 0 || transition.clip_delta != 0 || separator.is_some() {
            budget.add(1)?;
            edges.push(AtomicEdge {
                endpoints: mapped[start].endpoints,
                forward: transition,
                separator,
                clip_sources: clip_sources.into_boxed_slice(),
                frame_side,
            });
        }
        start = end;
    }
    Ok(NodedAtoms { vertices, edges })
}

struct AtomicHalfEdge {
    origin: VertexId,
    target: VertexId,
    twin: HalfEdgeId,
    edge: EdgeId,
    transition: Transition,
}

struct RotationVertex {
    point: exact::ExactPoint,
    fixed_xy: Option<XY>,
    star_start: u32,
    star_len: u32,
}

struct WalkedHalfEdge {
    origin: VertexId,
    target: VertexId,
    twin: HalfEdgeId,
    next: HalfEdgeId,
    prev: HalfEdgeId,
    edge: EdgeId,
    component: GraphComponentId,
    orbit: OrbitId,
    transition: Transition,
}

struct LocalOrbit {
    seed: HalfEdgeId,
    component: GraphComponentId,
    orientation: exact::ExactSign,
    half_edges: Box<[HalfEdgeId]>,
}

struct LocalDcel {
    vertices: Vec<RotationVertex>,
    edges: Vec<AtomicEdge>,
    half_edges: Vec<WalkedHalfEdge>,
    stars: Vec<HalfEdgeId>,
    orbits: Vec<LocalOrbit>,
    exterior_orbit: Vec<OrbitId>,
    frame_component: GraphComponentId,
}

fn uf_root(parents: &mut [usize], mut node: usize) -> usize {
    while parents[node] != node {
        parents[node] = parents[parents[node]];
        node = parents[node];
    }
    node
}

fn build_local_dcel(atoms: NodedAtoms, budget: &mut ExpansionBudget) -> Result<LocalDcel> {
    let edge_count = atoms.edges.len();
    budget.add(2_usize.saturating_mul(edge_count))?;
    budget.add(2_usize.saturating_mul(edge_count))?;
    let mut half_edges = Vec::new();
    reserve_exact(&mut half_edges, edge_count * 2)?;
    let mut outgoing = vec![Vec::<HalfEdgeId>::new(); atoms.vertices.len()];
    for (index, edge) in atoms.edges.iter().enumerate() {
        let edge_id =
            EdgeId(u32::try_from(index).map_err(|_| voronoi_error("too many exact edges"))?);
        let forward =
            HalfEdgeId(u32::try_from(index * 2).map_err(|_| voronoi_error("too many half-edges"))?);
        let reverse = HalfEdgeId(forward.0 + 1);
        half_edges.push(AtomicHalfEdge {
            origin: edge.endpoints[0],
            target: edge.endpoints[1],
            twin: reverse,
            edge: edge_id,
            transition: edge.forward,
        });
        half_edges.push(AtomicHalfEdge {
            origin: edge.endpoints[1],
            target: edge.endpoints[0],
            twin: forward,
            edge: edge_id,
            transition: edge.forward.reversed(),
        });
        outgoing[edge.endpoints[0].0 as usize].push(forward);
        outgoing[edge.endpoints[1].0 as usize].push(reverse);
    }
    for (vertex, star) in outgoing.iter_mut().enumerate() {
        if star.len() < 2 {
            return Err(voronoi_error("exact Voronoi subdivision contains a dangle"));
        }
        let origin = &atoms.vertices[vertex].point;
        for index in 1..star.len() {
            let mut cursor = index;
            while cursor > 0
                && exact::angle_ccw_cmp(
                    origin,
                    &atoms.vertices[half_edges[star[cursor].0 as usize].target.0 as usize].point,
                    &atoms.vertices[half_edges[star[cursor - 1].0 as usize].target.0 as usize]
                        .point,
                )? == Ordering::Less
            {
                star.swap(cursor, cursor - 1);
                cursor -= 1;
            }
        }
        for pair in star.windows(2) {
            exact::angle_ccw_cmp(
                origin,
                &atoms.vertices[half_edges[pair[0].0 as usize].target.0 as usize].point,
                &atoms.vertices[half_edges[pair[1].0 as usize].target.0 as usize].point,
            )
            .map_err(|_| {
                voronoi_error(format!(
                    "exact angular tie at {:?} toward {:?} and {:?}",
                    origin.round_nearest_even(),
                    atoms.vertices[half_edges[pair[0].0 as usize].target.0 as usize]
                        .point
                        .round_nearest_even(),
                    atoms.vertices[half_edges[pair[1].0 as usize].target.0 as usize]
                        .point
                        .round_nearest_even(),
                ))
            })?;
        }
    }
    let mut stars: Vec<HalfEdgeId> = Vec::new();
    reserve_exact(&mut stars, edge_count * 2)?;
    let mut vertices = Vec::new();
    reserve_exact(&mut vertices, atoms.vertices.len())?;
    for (vertex, star) in atoms.vertices.into_iter().zip(&outgoing) {
        vertices.push(RotationVertex {
            point: vertex.point,
            fixed_xy: vertex.fixed_xy,
            star_start: u32::try_from(stars.len())
                .map_err(|_| voronoi_error("too many star slots"))?,
            star_len: u32::try_from(star.len())
                .map_err(|_| voronoi_error("Voronoi star is too large"))?,
        });
        stars.extend(star);
    }
    let mut next = vec![None; half_edges.len()];
    let mut prev = vec![None; half_edges.len()];
    for (index, half_edge) in half_edges.iter().enumerate() {
        let vertex = &vertices[half_edge.target.0 as usize];
        let star =
            &stars[vertex.star_start as usize..(vertex.star_start + vertex.star_len) as usize];
        let twin_position = star
            .iter()
            .position(|&candidate| candidate == half_edge.twin)
            .ok_or_else(|| voronoi_error("half-edge twin is absent from target star"))?;
        let successor = star[(twin_position + star.len() - 1) % star.len()];
        next[index] = Some(successor);
        if prev[successor.0 as usize]
            .replace(HalfEdgeId(index as u32))
            .is_some()
        {
            return Err(voronoi_error("half-edge has two exact predecessors"));
        }
    }
    let next = next
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| voronoi_error("unset half-edge successor"))?;
    let prev = prev
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| voronoi_error("unset half-edge predecessor"))?;
    let mut parents: Vec<_> = (0..vertices.len()).collect();
    for edge in &atoms.edges {
        let left = uf_root(&mut parents, edge.endpoints[0].0 as usize);
        let right = uf_root(&mut parents, edge.endpoints[1].0 as usize);
        if left != right {
            parents[right] = left;
        }
    }
    let mut roots: Vec<_> = (0..vertices.len())
        .map(|vertex| uf_root(&mut parents, vertex))
        .collect();
    let mut unique = roots.clone();
    unique.sort_unstable();
    unique.dedup();
    unique.sort_by_key(|root| {
        roots
            .iter()
            .position(|candidate| candidate == root)
            .unwrap()
    });
    for root in &mut roots {
        *root = unique
            .iter()
            .position(|candidate| candidate == root)
            .unwrap();
    }
    let mut temporary_orbit = vec![None; half_edges.len()];
    let mut cycles = Vec::new();
    for seed in 0..half_edges.len() {
        if temporary_orbit[seed].is_some() {
            continue;
        }
        let temporary = cycles.len();
        let mut cycle = Vec::new();
        let mut current = HalfEdgeId(seed as u32);
        loop {
            if let Some(existing) = temporary_orbit[current.0 as usize] {
                if existing != temporary || current.0 as usize != seed {
                    return Err(voronoi_error("exact half-edge walks merge local orbits"));
                }
                break;
            }
            temporary_orbit[current.0 as usize] = Some(temporary);
            cycle.push(current);
            current = next[current.0 as usize];
            if cycle.len() > half_edges.len() {
                return Err(voronoi_error("exact orbit does not close"));
            }
        }
        cycles.push(cycle);
    }
    cycles.sort_by_key(|cycle| cycle.iter().min().copied().unwrap());
    let mut orbit_by_halfedge = vec![OrbitId(u32::MAX); half_edges.len()];
    let mut orbits = Vec::new();
    let mut exterior = vec![None; unique.len()];
    for (orbit_index, mut cycle) in cycles.into_iter().enumerate() {
        let least = cycle
            .iter()
            .enumerate()
            .min_by_key(|(_, id)| **id)
            .unwrap()
            .0;
        cycle.rotate_left(least);
        let points: Vec<_> = cycle
            .iter()
            .map(|id| {
                vertices[half_edges[id.0 as usize].origin.0 as usize]
                    .point
                    .clone()
            })
            .collect();
        let orientation = exact::cycle_orientation(&points)?;
        let component =
            GraphComponentId(roots[half_edges[cycle[0].0 as usize].origin.0 as usize] as u32);
        let orbit = OrbitId(orbit_index as u32);
        for half_edge in &cycle {
            orbit_by_halfedge[half_edge.0 as usize] = orbit;
        }
        if orientation == exact::ExactSign::Negative
            && exterior[component.0 as usize].replace(orbit).is_some()
        {
            return Err(voronoi_error(
                "edge component has more than one negative local orbit",
            ));
        }
        orbits.push(LocalOrbit {
            seed: cycle[0],
            component,
            orientation,
            half_edges: cycle.into_boxed_slice(),
        });
    }
    let exterior_orbit = exterior
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| voronoi_error("edge component has no negative local orbit"))?;
    for (index, half_edge) in half_edges.iter().enumerate() {
        if orbit_by_halfedge[index] == orbit_by_halfedge[half_edge.twin.0 as usize] {
            return Err(voronoi_error("exact Voronoi subdivision contains a bridge"));
        }
    }
    let frame_edge = atoms
        .edges
        .iter()
        .position(|edge| edge.frame_side.is_some())
        .ok_or_else(|| voronoi_error("private frame is absent"))?;
    let frame_component =
        GraphComponentId(roots[atoms.edges[frame_edge].endpoints[0].0 as usize] as u32);
    let walked = half_edges
        .into_iter()
        .enumerate()
        .map(|(index, half_edge)| WalkedHalfEdge {
            origin: half_edge.origin,
            target: half_edge.target,
            twin: half_edge.twin,
            next: next[index],
            prev: prev[index],
            edge: half_edge.edge,
            component: GraphComponentId(roots[half_edge.origin.0 as usize] as u32),
            orbit: orbit_by_halfedge[index],
            transition: half_edge.transition,
        })
        .collect();
    Ok(LocalDcel {
        vertices,
        edges: atoms.edges,
        half_edges: walked,
        stars,
        orbits,
        exterior_orbit,
        frame_component,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FaceLabel {
    frame_winding: i8,
    clip_winding: i32,
    owner: Option<SiteId>,
}

struct GlobalHalfEdge {
    origin: VertexId,
    target: VertexId,
    twin: HalfEdgeId,
    next: HalfEdgeId,
    prev: HalfEdgeId,
    edge: EdgeId,
    component: GraphComponentId,
    orbit: OrbitId,
    face: FaceId,
    transition: Transition,
}

struct ExactFace {
    outer: Option<OrbitId>,
    inner: Vec<OrbitId>,
    label: FaceLabel,
    unbounded: bool,
}

struct ExactSubdivision {
    vertices: Vec<RotationVertex>,
    edges: Vec<AtomicEdge>,
    half_edges: Vec<GlobalHalfEdge>,
    stars: Vec<HalfEdgeId>,
    orbits: Vec<LocalOrbit>,
    faces: Vec<ExactFace>,
    unbounded: FaceId,
}

fn validate_exact_subdivision(subdivision: &ExactSubdivision) -> Result<()> {
    if subdivision.unbounded != FaceId(0)
        || subdivision.faces.first().is_none_or(|face| !face.unbounded)
        || subdivision.faces.iter().skip(1).any(|face| face.unbounded)
    {
        return Err(voronoi_error(
            "exact subdivision has no unique unbounded face",
        ));
    }
    let mut star_membership = vec![0_u8; subdivision.half_edges.len()];
    for (vertex_index, vertex) in subdivision.vertices.iter().enumerate() {
        let start = vertex.star_start as usize;
        let end = start + vertex.star_len as usize;
        for &half_edge in subdivision
            .stars
            .get(start..end)
            .ok_or_else(|| voronoi_error("exact subdivision star row is out of bounds"))?
        {
            let edge = subdivision
                .half_edges
                .get(half_edge.0 as usize)
                .ok_or_else(|| voronoi_error("exact subdivision star names no half-edge"))?;
            if edge.origin
                != VertexId(
                    u32::try_from(vertex_index)
                        .map_err(|_| voronoi_error("too many exact vertices"))?,
                )
            {
                return Err(voronoi_error("exact subdivision star has the wrong origin"));
            }
            *star_membership
                .get_mut(half_edge.0 as usize)
                .ok_or_else(|| voronoi_error("exact subdivision star id is out of bounds"))? += 1;
        }
    }
    if star_membership.iter().any(|count| *count != 1) {
        return Err(voronoi_error(
            "exact subdivision half-edge does not occur in exactly one star",
        ));
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
    let mut orbit_faces = vec![None; subdivision.orbits.len()];
    for (index, half_edge) in subdivision.half_edges.iter().enumerate() {
        let twin = &subdivision.half_edges[half_edge.twin.0 as usize];
        let next = &subdivision.half_edges[half_edge.next.0 as usize];
        let prev = &subdivision.half_edges[half_edge.prev.0 as usize];
        if twin.twin != HalfEdgeId(index as u32)
            || twin.origin != half_edge.target
            || twin.target != half_edge.origin
            || twin.edge != half_edge.edge
            || twin.component != half_edge.component
            || twin.transition != half_edge.transition.reversed()
            || next.prev != HalfEdgeId(index as u32)
            || prev.next != HalfEdgeId(index as u32)
            || next.component != half_edge.component
            || subdivision.orbits[half_edge.orbit.0 as usize].component != half_edge.component
            || half_edge.face.0 as usize >= subdivision.faces.len()
        {
            return Err(voronoi_error("exact global half-edge invariants disagree"));
        }
        match &mut orbit_faces[half_edge.orbit.0 as usize] {
            Some(face) if *face != half_edge.face => {
                return Err(voronoi_error(
                    "one exact local orbit maps to multiple global faces",
                ));
            },
            slot @ None => *slot = Some(half_edge.face),
            Some(_) => {},
        }
    }
    Ok(())
}

fn crossed_label(left: FaceLabel, transition: Transition) -> Result<FaceLabel> {
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
    Ok(FaceLabel {
        frame_winding: left.frame_winding - transition.frame_delta,
        clip_winding: left.clip_winding - transition.clip_delta,
        owner,
    })
}

fn graft_components(local: LocalDcel, budget: &mut ExpansionBudget) -> Result<ExactSubdivision> {
    let component_count = local.exterior_orbit.len();
    let mut orbit_point_cache = Vec::new();
    reserve_exact(&mut orbit_point_cache, local.orbits.len())?;
    for orbit in 0..local.orbits.len() {
        let edge_count = local.orbits[orbit].half_edges.len();
        budget.add(edge_count)?;
        let mut points = Vec::new();
        reserve_exact(&mut points, edge_count)?;
        points.extend(local.orbits[orbit].half_edges.iter().map(|half_edge| {
            local.vertices[local.half_edges[half_edge.0 as usize].origin.0 as usize]
                .point
                .clone()
        }));
        orbit_point_cache.push(points);
    }
    let component_min: Vec<_> = (0..component_count)
        .map(|component| {
            local
                .half_edges
                .iter()
                .filter(|half_edge| half_edge.component.0 as usize == component)
                .map(|half_edge| half_edge.origin)
                .min()
                .expect("component has an edge")
        })
        .collect();
    let positive: Vec<Vec<OrbitId>> = (0..component_count)
        .map(|component| {
            local
                .orbits
                .iter()
                .enumerate()
                .filter(|(_, orbit)| {
                    orbit.component.0 as usize == component
                        && orbit.orientation == exact::ExactSign::Positive
                })
                .map(|(orbit, _)| OrbitId(orbit as u32))
                .collect()
        })
        .collect();
    let mut parent_orbit = vec![None; component_count];
    for component in 0..component_count {
        if component == local.frame_component.0 as usize {
            continue;
        }
        let probe = &local.vertices[component_min[component].0 as usize].point;
        let mut candidates = Vec::new();
        for other in 0..component_count {
            if other == component {
                continue;
            }
            for &orbit in &positive[other] {
                budget.add(orbit_point_cache[orbit.0 as usize].len())?;
                match exact::point_in_cycle(&orbit_point_cache[orbit.0 as usize], probe) {
                    exact::PointInCycle::Inside => candidates.push(orbit),
                    exact::PointInCycle::Boundary => {
                        return Err(voronoi_error(
                            "disconnected exact components meet without a noded edge",
                        ));
                    },
                    exact::PointInCycle::Outside => {},
                }
            }
        }
        budget.add(
            candidates
                .iter()
                .map(|orbit| orbit_point_cache[orbit.0 as usize].len())
                .sum::<usize>()
                .saturating_mul(candidates.len()),
        )?;
        let parent = candidates
            .iter()
            .copied()
            .find(|candidate| {
                let candidate_probe = &local.vertices[local.half_edges
                    [local.orbits[candidate.0 as usize].seed.0 as usize]
                    .origin
                    .0 as usize]
                    .point;
                candidates.iter().all(|other| {
                    candidate == other
                        || exact::point_in_cycle(
                            &orbit_point_cache[other.0 as usize],
                            candidate_probe,
                        ) == exact::PointInCycle::Inside
                })
            })
            .ok_or_else(|| {
                voronoi_error("exact component containment is non-laminar or parentless")
            })?;
        if candidates
            .iter()
            .filter(|candidate| **candidate == parent)
            .count()
            != 1
        {
            return Err(voronoi_error(
                "exact component has no unique containing parent face",
            ));
        }
        parent_orbit[component] = Some(parent);
    }
    let mut depth = vec![None; component_count];
    depth[local.frame_component.0 as usize] = Some(0_usize);
    for _ in 0..component_count {
        for component in 0..component_count {
            if depth[component].is_some() {
                continue;
            }
            let parent = parent_orbit[component].expect("non-root parent");
            let parent_component = local.orbits[parent.0 as usize].component.0 as usize;
            if let Some(parent_depth) = depth[parent_component] {
                depth[component] = Some(parent_depth + 1);
            }
        }
    }
    let depth = depth
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| voronoi_error("exact component containment forest is cyclic"))?;
    let mut order: Vec<_> = (0..component_count).collect();
    order.sort_by_key(|&component| (depth[component], component_min[component]));
    let mut face_of_orbit: Vec<Option<FaceId>> = vec![None; local.orbits.len()];
    let mut label_of_orbit: Vec<Option<FaceLabel>> = vec![None; local.orbits.len()];
    let mut faces = vec![ExactFace {
        outer: None,
        inner: Vec::new(),
        label: FaceLabel {
            frame_winding: 0,
            clip_winding: 0,
            owner: None,
        },
        unbounded: true,
    }];
    for component in order {
        let exterior = local.exterior_orbit[component];
        let (exterior_face, exterior_label) = if component == local.frame_component.0 as usize {
            (FaceId(0), faces[0].label)
        } else {
            let parent = parent_orbit[component].expect("child parent");
            let face = face_of_orbit[parent.0 as usize]
                .ok_or_else(|| voronoi_error("parent face was not created before child"))?;
            faces[face.0 as usize].inner.push(exterior);
            (face, faces[face.0 as usize].label)
        };
        face_of_orbit[exterior.0 as usize] = Some(exterior_face);
        label_of_orbit[exterior.0 as usize] = Some(exterior_label);
        for &orbit in &positive[component] {
            let face = FaceId(
                u32::try_from(faces.len()).map_err(|_| voronoi_error("too many exact faces"))?,
            );
            face_of_orbit[orbit.0 as usize] = Some(face);
            faces.push(ExactFace {
                outer: Some(orbit),
                inner: Vec::new(),
                label: exterior_label,
                unbounded: false,
            });
        }
        let component_orbits = std::iter::once(exterior)
            .chain(positive[component].iter().copied())
            .count();
        budget.add(component_orbits)?;
        let mut queue = VecDeque::new();
        queue
            .try_reserve(component_orbits)
            .map_err(|_| voronoi_error("could not allocate exact label queue"))?;
        queue.push_back(exterior);
        while let Some(left) = queue.pop_front() {
            let left_label = label_of_orbit[left.0 as usize]
                .ok_or_else(|| voronoi_error("unlabelled queued exact local orbit"))?;
            for &half_edge_id in &local.orbits[left.0 as usize].half_edges {
                let half_edge = &local.half_edges[half_edge_id.0 as usize];
                let left = half_edge.orbit;
                let right = local.half_edges[half_edge.twin.0 as usize].orbit;
                debug_assert_eq!(left, half_edge.orbit);
                let right_label = crossed_label(left_label, half_edge.transition)?;
                match label_of_orbit[right.0 as usize] {
                    Some(existing) if existing != right_label => {
                        return Err(voronoi_error("inconsistent exact face-label propagation"));
                    },
                    None => {
                        label_of_orbit[right.0 as usize] = Some(right_label);
                        queue.push_back(right);
                    },
                    _ => {},
                }
            }
        }
        for orbit in std::iter::once(exterior).chain(positive[component].iter().copied()) {
            let label = label_of_orbit[orbit.0 as usize]
                .ok_or_else(|| voronoi_error("unlabelled exact local orbit"))?;
            let face = face_of_orbit[orbit.0 as usize].expect("face assigned");
            if face != exterior_face || orbit == exterior {
                faces[face.0 as usize].label = label;
            }
        }
    }
    for face in &mut faces {
        face.inner.sort_unstable();
    }
    for (index, face) in faces.iter().enumerate() {
        if !matches!(face.label.frame_winding, 0 | 1) || !matches!(face.label.clip_winding, 0 | 1) {
            return Err(voronoi_error("exact Voronoi face has non-binary winding"));
        }
        if index == 0 {
            if face.label
                != (FaceLabel {
                    frame_winding: 0,
                    clip_winding: 0,
                    owner: None,
                })
            {
                return Err(voronoi_error("exact unbounded face has a nonempty label"));
            }
        } else if face.label.frame_winding != 1 || face.label.owner.is_none() {
            return Err(voronoi_error("bounded exact face has no unique owner"));
        }
    }
    let global_half_edges = local
        .half_edges
        .into_iter()
        .map(|half_edge| {
            Ok(GlobalHalfEdge {
                origin: half_edge.origin,
                target: half_edge.target,
                twin: half_edge.twin,
                next: half_edge.next,
                prev: half_edge.prev,
                edge: half_edge.edge,
                component: half_edge.component,
                orbit: half_edge.orbit,
                face: face_of_orbit[half_edge.orbit.0 as usize]
                    .ok_or_else(|| voronoi_error("half-edge orbit has no global face"))?,
                transition: half_edge.transition,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let subdivision = ExactSubdivision {
        vertices: local.vertices,
        edges: local.edges,
        half_edges: global_half_edges,
        stars: local.stars,
        orbits: local.orbits,
        faces,
        unbounded: FaceId(0),
    };
    validate_exact_subdivision(&subdivision)?;
    Ok(subdivision)
}

struct ExactSimpleRing {
    face: FaceId,
    vertices: Box<[VertexId]>,
    edges: Box<[EdgeId]>,
    orientation: exact::ExactSign,
}

struct ExactCellPart {
    owner: SiteId,
    face: FaceId,
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

fn split_orbit(
    subdivision: &ExactSubdivision,
    face: FaceId,
    orbit: OrbitId,
) -> Result<Vec<ExactSimpleRing>> {
    let walk = &subdivision.orbits[orbit.0 as usize].half_edges;
    let direct_vertices: Vec<_> = walk
        .iter()
        .map(|half_edge| subdivision.half_edges[half_edge.0 as usize].origin)
        .collect();
    if direct_vertices
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        == direct_vertices.len()
    {
        let mut vertices = direct_vertices;
        let mut edges: Vec<_> = walk
            .iter()
            .map(|half_edge| subdivision.half_edges[half_edge.0 as usize].edge)
            .collect();
        vertices.push(vertices[0]);
        let points: Vec<_> = vertices[..vertices.len() - 1]
            .iter()
            .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone())
            .collect();
        let orientation = exact::cycle_orientation(&points)?;
        canonicalize_ring(&mut vertices, &mut edges);
        return Ok(vec![ExactSimpleRing {
            face,
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
        let half_edge_id = walk[step % walk.len()];
        let half_edge = &subdivision.half_edges[half_edge_id.0 as usize];
        let vertex = half_edge.origin;
        let incoming = (step > 0)
            .then(|| subdivision.half_edges[walk[(step - 1) % walk.len()].0 as usize].edge);
        if let Some(position) = positions[vertex.0 as usize] {
            let mut vertices = path[position..].to_vec();
            vertices.push(vertex);
            let mut edges = path_edges[position..].to_vec();
            edges.push(incoming.expect("a repeated walk vertex has an incoming edge"));
            if vertices.len() < 4 || edges.len() + 1 != vertices.len() {
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
                face,
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

fn assemble_cells(
    subdivision: &ExactSubdivision,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ExactCellPart>> {
    let mut parts = Vec::new();
    for (face_index, face) in subdivision.faces.iter().enumerate() {
        if face.label.frame_winding != 1 || face.label.clip_winding != 1 {
            continue;
        }
        let face_id = FaceId(face_index as u32);
        let mut rings = Vec::new();
        for orbit in face.outer.into_iter().chain(face.inner.iter().copied()) {
            rings.extend(split_orbit(subdivision, face_id, orbit)?);
        }
        let positives: Vec<_> = rings
            .iter()
            .enumerate()
            .filter(|(_, ring)| ring.orientation == exact::ExactSign::Positive)
            .map(|(index, _)| index)
            .collect();
        let negatives: Vec<_> = rings
            .iter()
            .enumerate()
            .filter(|(_, ring)| ring.orientation == exact::ExactSign::Negative)
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
        for &positive in &positives {
            for other in 0..rings.len() {
                if other == positive {
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
            budget.add(
                positives
                    .iter()
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
                    .saturating_mul(candidates.len()),
            )?;
            let parent = candidates
                .iter()
                .copied()
                .find(|candidate| {
                    let candidate_point = &subdivision.vertices
                        [rings[positives[*candidate]].vertices[0].0 as usize]
                        .point;
                    candidates.iter().all(|other| {
                        candidate == other
                            || exact::point_in_cycle(
                                &ring_points[positives[*other]],
                                candidate_point,
                            ) == exact::PointInCycle::Inside
                    })
                })
                .ok_or_else(|| voronoi_error("exact face ring forest is non-laminar"))?;
            holes_by_shell[parent].push(negative);
        }
        let owner = face.label.owner.expect("kept face owner");
        let mut rings: Vec<Option<ExactSimpleRing>> = rings.into_iter().map(Some).collect();
        for (slot, positive) in positives.into_iter().enumerate() {
            let shell = rings[positive].take().unwrap();
            let holes: Vec<_> = holes_by_shell[slot]
                .iter()
                .map(|&hole| rings[hole].take().unwrap())
                .collect();
            if shell.face != face_id || holes.iter().any(|hole| hole.face != face_id) {
                return Err(voronoi_error("exact ring escaped its global face"));
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
                face: face_id,
                shell,
                holes,
            });
        }
    }
    parts.sort_by(|left, right| {
        left.owner
            .cmp(&right.owner)
            .then_with(|| left.shell.vertices.cmp(&right.shell.vertices))
    });
    Ok(parts)
}

fn assemble_edges(
    subdivision: &ExactSubdivision,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ExactEdgeChain>> {
    let mut selected: Vec<_> = subdivision
        .edges
        .iter()
        .enumerate()
        .filter_map(|(index, edge)| {
            let separator = edge.separator?;
            let left = &subdivision.faces[subdivision.half_edges[index * 2].face.0 as usize].label;
            let right =
                &subdivision.faces[subdivision.half_edges[index * 2 + 1].face.0 as usize].label;
            ((left.frame_winding == 1 && left.clip_winding == 1)
                || (right.frame_winding == 1 && right.clip_winding == 1))
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
        let group = &selected[start..end];
        let mut used = vec![false; group.len()];
        for seed in 0..group.len() {
            if used[seed] {
                continue;
            }
            let mut degree = std::collections::BTreeMap::<VertexId, usize>::new();
            for (_, edge) in group {
                for vertex in subdivision.edges[edge.0 as usize].endpoints {
                    *degree.entry(vertex).or_default() += 1;
                }
            }
            if degree.values().any(|degree| *degree > 2) {
                return Err(voronoi_error("Voronoi separator branches"));
            }
            let seed_edge = &subdivision.edges[group[seed].1.0 as usize];
            let mut current = if degree[&seed_edge.endpoints[0]] == 1 {
                seed_edge.endpoints[0]
            } else {
                seed_edge.endpoints[1]
            };
            let mut vertices = vec![current];
            let mut chain_edges = Vec::new();
            loop {
                let next = group.iter().enumerate().find(|(slot, (_, edge))| {
                    !used[*slot]
                        && subdivision.edges[edge.0 as usize]
                            .endpoints
                            .contains(&current)
                });
                let Some((slot, (_, edge_id))) = next else {
                    break;
                };
                used[slot] = true;
                chain_edges.push(*edge_id);
                let edge = &subdivision.edges[edge_id.0 as usize];
                current = if edge.endpoints[0] == current {
                    edge.endpoints[1]
                } else {
                    edge.endpoints[0]
                };
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
        }
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
    edges: Vec<LineSeq>,
    clip: Shape,
}

impl EmbeddedVoronoi {
    pub(super) fn into_polygons(self) -> Vec<Polygon> {
        self.polygons
    }
    pub(super) fn into_edges(self) -> Vec<LineSeq> {
        self.edges
    }
}

fn marked_site_candidates(
    segments: &[(EdgeId, [XY; 2])],
    sites: &[Site],
    budget: &mut ExpansionBudget,
) -> Result<Vec<(EdgeId, SiteId)>> {
    let mut indexed = Vec::new();
    indexed
        .try_reserve_exact(sites.len())
        .map_err(|_| voronoi_error("could not allocate Voronoi site index"))?;
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

fn embed(
    subdivision: &ExactSubdivision,
    parts: Vec<ExactCellPart>,
    chains: Vec<ExactEdgeChain>,
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
    for chain in &chains {
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
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
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
            matches!(edge.forward.owner, OwnerTransition::Boundary {
                left: Some(_),
                right: Some(_)
            })
            .then(|| {
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
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        }
        if let OwnerTransition::Boundary { left, right } = edge.forward.owner {
            let (Some(left), Some(right)) = (left, right) else {
                return Err(voronoi_error("live separator has incomplete ownership"));
            };
            let left_point = exact::ExactPoint::from_xy(sites[left.0 as usize].point.xy());
            let right_point = exact::ExactPoint::from_xy(sites[right.0 as usize].point.xy());
            if exact::orient_points(&rounded[0], &rounded[1], &left_point)
                != exact::ExactSign::Positive
                || exact::orient_points(&rounded[0], &rounded[1], &right_point)
                    != exact::ExactSign::Negative
            {
                return Err(voronoi_error(
                    "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                ));
            }
            while marked_start < marked_candidates.len()
                && marked_candidates[marked_start].0 < edge_id
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
                    return Err(voronoi_error(
                        "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                    ));
                }
                candidate += 1;
            }
        }
    }
    let indexed_edges: Vec<_> = live_edge_ids
        .iter()
        .map(|&edge_id| {
            let edge = &subdivision.edges[edge_id.0 as usize];
            let start = embedded_point(edge.endpoints[0])?;
            let end = embedded_point(edge.endpoints[1])?;
            Ok(rstar::primitives::GeomWithData::new(
                rstar::primitives::Rectangle::from_corners(
                    [start.x.min(end.x), start.y.min(end.y)],
                    [start.x.max(end.x), start.y.max(end.y)],
                ),
                edge_id,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let edge_tree = BulkRTree::bulk_load_with_params(indexed_edges.clone());
    let mut rounded_candidates = Vec::new();
    for item in &indexed_edges {
        for other in edge_tree.locate_in_envelope_intersecting(item.envelope()) {
            if item.data < other.data {
                budgeted_try_push(&mut rounded_candidates, [item.data, other.data], budget)?;
            }
        }
    }
    rounded_candidates.sort_unstable();
    rounded_candidates.dedup();
    for [left_id, right_id] in rounded_candidates {
        let left = &subdivision.edges[left_id.0 as usize];
        let left_points = [
            rounded_exact(left.endpoints[0])?,
            rounded_exact(left.endpoints[1])?,
        ];
        let right = &subdivision.edges[right_id.0 as usize];
        let right_points = [
            rounded_exact(right.endpoints[0])?,
            rounded_exact(right.endpoints[1])?,
        ];
        let shared: Vec<_> = left
            .endpoints
            .iter()
            .filter(|vertex| right.endpoints.contains(vertex))
            .copied()
            .collect();
        match exact::segment_intersection(
            &left_points[0],
            &left_points[1],
            &right_points[0],
            &right_points[1],
        ) {
            exact::SegmentIntersection::None if shared.is_empty() => {},
            exact::SegmentIntersection::Point(point)
                if shared.len() == 1 && point.same_position(&rounded_exact(shared[0])?) => {},
            _ => {
                return Err(voronoi_error(
                    "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                ));
            },
        }
    }
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
    incident_edges
        .try_reserve_exact(live_edge_ids.len().saturating_mul(2))
        .map_err(|_| voronoi_error("could not allocate live edge incidence"))?;
    incident_edges.resize(live_edge_ids.len().saturating_mul(2), EdgeId(0));
    let mut cursors = incidence_offsets[..subdivision.vertices.len()].to_vec();
    for &edge_id in &live_edge_ids {
        for endpoint in subdivision.edges[edge_id.0 as usize].endpoints {
            let cursor = &mut cursors[endpoint.0 as usize];
            incident_edges[*cursor] = edge_id;
            *cursor += 1;
        }
    }
    for vertex_index in 0..subdivision.vertices.len() {
        let vertex = VertexId(vertex_index as u32);
        if !live[vertex_index] {
            continue;
        }
        let incident =
            &incident_edges[incidence_offsets[vertex_index]..incidence_offsets[vertex_index + 1]];
        let sort_fan = |rounded: bool| -> Result<Vec<EdgeId>> {
            let origin = if rounded {
                rounded_exact(vertex)?
            } else {
                subdivision.vertices[vertex_index].point.clone()
            };
            let mut rays: Vec<_> = incident
                .iter()
                .map(|edge_id| {
                    let edge = &subdivision.edges[edge_id.0 as usize];
                    let target = if edge.endpoints[0] == vertex {
                        edge.endpoints[1]
                    } else {
                        edge.endpoints[0]
                    };
                    let point = if rounded {
                        rounded_exact(target)?
                    } else {
                        subdivision.vertices[target.0 as usize].point.clone()
                    };
                    Ok((*edge_id, point))
                })
                .collect::<Result<Vec<_>>>()?;
            // Vertex degree is small, and a fallible insertion sort keeps an exact
            // angular tie from being disguised as `Ordering::Equal` inside an
            // infallible standard-library comparator.
            for index in 1..rays.len() {
                let mut cursor = index;
                while cursor > 0
                    && exact::angle_ccw_cmp(&origin, &rays[cursor].1, &rays[cursor - 1].1)?
                        == Ordering::Less
                {
                    rays.swap(cursor, cursor - 1);
                    cursor -= 1;
                }
            }
            for pair in rays.windows(2) {
                exact::angle_ccw_cmp(&origin, &pair[0].1, &pair[1].1)
                    .map_err(|_| voronoi_error("rounded Voronoi fan contains an angular tie"))?;
            }
            if rays.len() > 1 {
                exact::angle_ccw_cmp(&origin, &rays.last().unwrap().1, &rays[0].1)
                    .map_err(|_| voronoi_error("rounded Voronoi fan contains an angular tie"))?;
            }
            let mut order: Vec<_> = rays.into_iter().map(|ray| ray.0).collect();
            if let Some((least, _)) = order.iter().enumerate().min_by_key(|(_, edge)| *edge) {
                order.rotate_left(least);
            }
            Ok(order)
        };
        if sort_fan(false)? != sort_fan(true)? {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
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
        vertices
            .iter()
            .map(|vertex| {
                let embedded = exact_to_embedded[vertex.0 as usize]
                    .ok_or_else(|| voronoi_error("non-live exact vertex was rendered"))?;
                let point = points[embedded.0 as usize];
                Ok(Point::new_unchecked_xy(point.x, point.y))
            })
            .collect()
    };
    let mut labelled = Vec::new();
    for part in parts {
        if subdivision.faces[part.face.0 as usize].label.owner != Some(part.owner) {
            return Err(voronoi_error("embedded cell lost its exact face owner"));
        }
        let shell_coordinates = coordinates(&part.shell.vertices)?;
        let rounded_shell: Vec<_> = shell_coordinates[..shell_coordinates.len() - 1]
            .iter()
            .map(|point| exact::ExactPoint::from_xy(point.xy()))
            .collect();
        if exact::cycle_orientation(&rounded_shell)? != part.shell.orientation {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
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
                    return Err(voronoi_error(
                        "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                    ));
                }
                Ring::closed(coordinates)
            })
            .collect::<Result<Vec<_>>>()?;
        let polygon = Polygon::new(shell, holes);
        if !polygon_is_valid(&polygon) {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        }
        labelled.push((part.owner, polygon));
    }
    let polygons: Vec<_> = labelled
        .iter()
        .map(|(_, polygon)| polygon.clone())
        .collect();
    let embedded_ring = |ring: &[exact::ExactPoint]| -> Result<Ring> {
        let mut coordinates = ring
            .iter()
            .map(|point| {
                let vertex = subdivision
                    .vertices
                    .iter()
                    .position(|candidate| candidate.point.same_position(point))
                    .ok_or_else(|| voronoi_error("generated clip vertex is not live"))?;
                let embedded = exact_to_embedded[vertex]
                    .ok_or_else(|| voronoi_error("generated clip vertex is not embedded"))?;
                let xy = points[embedded.0 as usize];
                Ok(Point::new_unchecked_xy(xy.x, xy.y))
            })
            .collect::<Result<Vec<_>>>()?;
        coordinates.push(coordinates[0].clone());
        Ring::closed(coordinates)
    };
    let shell = embedded_ring(&clip.rings[0])?;
    let holes = clip.rings[1..]
        .iter()
        .map(|ring| embedded_ring(ring))
        .collect::<Result<Vec<_>>>()?;
    let comparison_clip = Shape::Polygon(Polygon::new(shell, holes));
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
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        }
        budget.add(shape_rows.len())?;
        let mut row_entries = Vec::new();
        row_entries
            .try_reserve_exact(shape_rows.len())
            .map_err(|_| voronoi_error("could not allocate Voronoi row index"))?;
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
            if !comparison_clip.covers(&point) {
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
                if Shape::Polygon(polygon.clone()).covers(&point) {
                    if owner.0 as usize != site_index {
                        return Err(voronoi_error(
                            "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                        ));
                    }
                    own = true;
                }
            }
            if !own {
                return Err(voronoi_error(
                    "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                ));
            }
        }
        // Exact vertices are rounded once and shared by every incident cell.  The
        // certificates above preserve the subdivision topology, validity, and
        // interior-disjointness, but binary64 overlay may still observe an ULP-scale
        // gap between the rounded partition and the source clip.  That numerical
        // equality is deliberately not part of the topological-snap contract.
    }
    let edges = chains
        .into_iter()
        .map(|chain| LineSeq::try_new(CoordSeq::from(coordinates(&chain.vertices)?)))
        .collect::<Result<Vec<_>>>()?;
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
        && sites.len() > 5
        && normal_or_zero
        && std::env::var_os("GOMETRY_VORO_REFERENCE").is_none()
        && matches!(
            boundary,
            VoronoiBoundary::Envelope | VoronoiBoundary::Padded
        )
    {
        return dual_build(sites, boundary, budget);
    }
    let clip = normalize_clip(sites, boundary, budget)?;
    let mesh = certified_delaunay(sites)?;
    if mesh.collinear_order().is_some() {
        return Err(voronoi_error(
            "collinear sites require the exact one-dimensional lane",
        ));
    }
    let complex = delaunay_complex(&mesh, sites)?;
    let primal_edges = mesh.primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len(), budget)?;
    let primitives = build_primitives(sites, &primal_edges, &complex, &clip, budget)?;
    let atoms = node_primitives(primitives, sites, &adjacency, &clip, budget)?;
    let local = build_local_dcel(atoms, budget)?;
    let subdivision = graft_components(local, budget)?;
    let parts = assemble_cells(&subdivision, budget)?;
    let chains = assemble_edges(&subdivision, budget)?;
    embed(&subdivision, parts, chains, &clip, sites, true, budget)
}

pub(super) fn build_collinear_edges(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
) -> Result<Vec<LineSeq>> {
    let mesh = certified_delaunay(sites)?;
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
            return Ok(Vec::new());
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
                if start_sign == exact::ExactSign::Zero {
                    budgeted_try_push(&mut events, start.clone(), budget)?;
                }
                if end_sign == exact::ExactSign::Zero {
                    budgeted_try_push(&mut events, end.clone(), budget)?;
                }
                if start_sign != exact::ExactSign::Zero
                    && end_sign != exact::ExactSign::Zero
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
        .map(|(point, fixed_xy)| RotationVertex {
            point: point.clone(),
            fixed_xy,
            star_start: 0,
            star_len: 0,
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
        half_edges: Vec::new(),
        stars: Vec::new(),
        orbits: Vec::new(),
        faces: vec![ExactFace {
            outer: None,
            inner: Vec::new(),
            label: FaceLabel {
                frame_winding: 0,
                clip_winding: 0,
                owner: None,
            },
            unbounded: true,
        }],
        unbounded: FaceId(0),
    };
    let embedded = embed(
        &subdivision,
        Vec::new(),
        chains,
        &split_clip,
        sites,
        false,
        budget,
    )?;
    let comparison_clip = &embedded.clip;
    for edge in &embedded.edges {
        budget.add(edge.len())?;
        if !Shape::LineString(edge.clone())
            .difference(comparison_clip, Strictness::Lenient)?
            .is_empty()
        {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        }
    }
    Ok(embedded.edges)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cycle(points: &[(f64, f64)]) -> Vec<exact::ExactPoint> {
        points
            .iter()
            .map(|&(x, y)| exact::ExactPoint::from_xy(XY::new(x, y)))
            .collect()
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
            Point::new_unchecked_xy(0.0, 0.0),
            Point::new_unchecked_xy(2.0, 2.0),
            Point::new_unchecked_xy(0.0, 2.0),
            Point::new_unchecked_xy(2.0, 0.0),
            Point::new_unchecked_xy(0.0, 0.0),
        ])
        .unwrap();
        assert!(admit_embedded_clip(&Polygon::new(ring, Vec::new())).is_err());
    }

    fn reversed_cycles(cycles: &[Vec<exact::ExactPoint>]) -> Vec<Vec<exact::ExactPoint>> {
        std::iter::once(&cycles[0])
            .chain(cycles[1..].iter().rev())
            .map(|cycle| cycle.iter().cloned().rev().collect())
            .collect()
    }

    fn assert_local_euler_and_frame_root(local: &LocalDcel) {
        let components = local.exterior_orbit.len();
        assert_eq!(
            local.vertices.len() as isize - local.edges.len() as isize
                + local.orbits.len() as isize,
            2 * components as isize,
            "each connected planar component has V-E+F=2"
        );

        let frame_exterior = local.exterior_orbit[local.frame_component.0 as usize];
        let restored_outside_twins = local
            .half_edges
            .iter()
            .filter(|half_edge| {
                local.edges[half_edge.edge.0 as usize].frame_side.is_some()
                    && half_edge.transition.frame_delta == -1
                    && half_edge.orbit == frame_exterior
            })
            .count();
        let mutated_exterior = local
            .exterior_orbit
            .iter()
            .copied()
            .find(|orbit| *orbit != frame_exterior);
        if let Some(mutated_exterior) = mutated_exterior {
            let mutated_outside_twins = local
                .half_edges
                .iter()
                .filter(|half_edge| {
                    local.edges[half_edge.edge.0 as usize].frame_side.is_some()
                        && half_edge.transition.frame_delta == -1
                        && half_edge.orbit == mutated_exterior
                })
                .count();
            assert_ne!(mutated_outside_twins, restored_outside_twins);
            assert_eq!(mutated_outside_twins, 0);
        }
        assert!(restored_outside_twins >= 4);
    }

    fn assert_global_euler(subdivision: &ExactSubdivision) {
        let components = subdivision
            .half_edges
            .iter()
            .map(|half_edge| half_edge.component)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        assert_eq!(
            subdivision.vertices.len() as isize - subdivision.edges.len() as isize
                + subdivision.faces.len() as isize,
            1 + components as isize
        );
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
                point: Point::new_unchecked_xy(x, y),
            })
            .collect();
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let diagram = build(&sites, VoronoiBoundary::Padded, &mut budget, true).unwrap();
        assert_eq!(diagram.polygons.len(), sites.len());
        assert!(diagram.polygons.iter().all(polygon_is_valid));
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
            point: Point::new_unchecked_xy(offset + x, offset + y),
        })
        .collect();
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let error = match build(&sites, VoronoiBoundary::Envelope, &mut budget, true) {
            Ok(_) => panic!("colliding exact vertices unexpectedly embedded"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("no topology-preserving binary64 embedding")
        );
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
            .map(|(x, y)| Point::new_unchecked_xy(x, y))
            .collect(),
        )
        .unwrap()
    }

    #[test]
    fn crossed_component_keeps_distinct_faces_with_the_same_owner() {
        let sites: Vec<_> = [(-10.0, -10.0), (0.0, 10.0), (10.0, -10.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_unchecked_xy(x, y),
            })
            .collect();
        let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
            -12.0, -1.0, 12.0, 1.0,
        )]);
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let clip =
            normalize_clip(&sites, VoronoiBoundary::Polygon(&clip_polygon), &mut budget).unwrap();
        let mesh = certified_delaunay(&sites).unwrap();
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let primal_edges = mesh.primal_edges();
        let adjacency = certified_site_adjacency(&primal_edges, sites.len(), &mut budget).unwrap();
        let primitives =
            build_primitives(&sites, &primal_edges, &complex, &clip, &mut budget).unwrap();
        let atoms = node_primitives(primitives, &sites, &adjacency, &clip, &mut budget).unwrap();
        let local = build_local_dcel(atoms, &mut budget).unwrap();
        assert_local_euler_and_frame_root(&local);
        let subdivision = graft_components(local, &mut budget).unwrap();
        assert_global_euler(&subdivision);
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
                point: Point::new_unchecked_xy(x, y),
            })
            .collect();
        let clip_polygon = Polygon::new(rectangle(-20.0, -20.0, 20.0, 20.0), vec![rectangle(
            -1.0, -1.0, 1.0, 1.0,
        )]);
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let clip =
            normalize_clip(&sites, VoronoiBoundary::Polygon(&clip_polygon), &mut budget).unwrap();
        let mesh = certified_delaunay(&sites).unwrap();
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let primal_edges = mesh.primal_edges();
        let adjacency = certified_site_adjacency(&primal_edges, sites.len(), &mut budget).unwrap();
        let primitives =
            build_primitives(&sites, &primal_edges, &complex, &clip, &mut budget).unwrap();
        let atoms = node_primitives(primitives, &sites, &adjacency, &clip, &mut budget).unwrap();
        let local = build_local_dcel(atoms, &mut budget).unwrap();
        assert!(local.exterior_orbit.len() > 1);
        assert_local_euler_and_frame_root(&local);
        let subdivision = graft_components(local, &mut budget).unwrap();
        assert_global_euler(&subdivision);
        let inherited = subdivision
            .faces
            .iter()
            .enumerate()
            .filter(|(face, value)| *face != 0 && !value.inner.is_empty())
            .map(|(_, value)| value.label)
            .collect::<Vec<_>>();
        assert_eq!(inherited.len(), 1);
        assert_eq!(inherited[0].frame_winding, 1);
        assert_eq!(inherited[0].clip_winding, 1);
        assert!(inherited[0].owner.is_some());
        let parts = assemble_cells(&subdivision, &mut budget).unwrap();
        assert_eq!(parts.iter().map(|part| part.holes.len()).sum::<usize>(), 1);
    }

    #[test]
    fn collinear_holed_clip_splits_each_separator_into_two_pieces() {
        let sites: Vec<_> = [(-2.0, 0.0), (0.0, 0.0), (2.0, 0.0)]
            .into_iter()
            .enumerate()
            .map(|(id, (x, y))| Site {
                id,
                point: Point::new_unchecked_xy(x, y),
            })
            .collect();
        let clip = Polygon::new(rectangle(-3.0, -3.0, 3.0, 3.0), vec![rectangle(
            -1.5, -1.0, 1.5, 1.0,
        )]);
        let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
        let edges =
            build_collinear_edges(&sites, VoronoiBoundary::Polygon(&clip), &mut budget).unwrap();
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
                point: Point::new_unchecked_xy(x, y),
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
                point: Point::new_unchecked_xy(x, y),
            })
            .collect();
        let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
        assert!(
            build_collinear_edges(&sites, VoronoiBoundary::Envelope, &mut budget)
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
                point: Point::new_unchecked_xy(x, y),
            })
            .collect();
        let mut budget = ExpansionBudget::new("test", "collinear Voronoi topology");
        let edges = build_collinear_edges(&sites, VoronoiBoundary::Envelope, &mut budget).unwrap();
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
                .map(|(x, y)| Point::new_unchecked_xy(x, y))
                .collect(),
            0.1,
        );
        let clip = Polygon::new(rectangle(-1.0, -1.0, 2.0, 2.0), Vec::new());
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let diagram = build(&sites, VoronoiBoundary::Polygon(&clip), &mut budget, true).unwrap();
        assert!(!diagram.edges.is_empty());
    }

    #[test]
    fn semantic_admission_mutants_change_validity_overlap_gap_and_spill() {
        let shape = |min_x, min_y, max_x, max_y| {
            Shape::Polygon(Polygon::new(
                rectangle(min_x, min_y, max_x, max_y),
                Vec::new(),
            ))
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

        let restored_polygon = Polygon::new(rectangle(0.0, 0.0, 4.0, 4.0), Vec::new());
        let mutated_polygon = Polygon::new(
            Ring::closed(
                [(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0), (0.0, 0.0)]
                    .into_iter()
                    .map(|(x, y)| Point::new_unchecked_xy(x, y))
                    .collect(),
            )
            .unwrap(),
            Vec::new(),
        );
        let restored_valid = polygon_is_valid(&restored_polygon);
        let mutated_valid = polygon_is_valid(&mutated_polygon);
        assert_ne!(
            mutated_valid, restored_valid,
            "mutated valid={mutated_valid}, restored valid={restored_valid}"
        );
    }

    #[test]
    fn transition_and_budget_mutants_report_distinct_restored_values() {
        let left = FaceLabel {
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
        let zero_seeded = FaceLabel {
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
                point: Point::new_unchecked_xy(x, y),
            })
            .collect();
        let mut construction_budget = ExpansionBudget::new("test", "dual completeness");
        let clip =
            normalize_clip(&sites, VoronoiBoundary::Envelope, &mut construction_budget).unwrap();
        let mesh = certified_delaunay(&sites).unwrap();
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let restored =
            dual_build_rings(&sites, &mesh, &complex, &clip, &mut construction_budget).unwrap();
        let mut mutated = restored.clone();
        mutated.rings.pop();

        let mut restored_budget = ExpansionBudget::new("test", "restored dual completeness");
        let restored_diagram = dual_embed(&restored, &clip, &mut restored_budget).unwrap();
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
        let mutated_admitted = dual_embed(&mutated, &clip, &mut mutated_budget).is_ok();
        assert_ne!(
            mutated_admitted, restored_admitted,
            "mutated missing-cell admitted={mutated_admitted}, restored admitted={restored_admitted}"
        );
        assert!(!mutated_admitted && restored_admitted);
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
// Everything `build_primitives` / `node_primitives` / `build_local_dcel` /
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

/// A dual ring vertex. `Center(c)` IS component `c`'s circumcenter, so its
/// identity is the component id and its rounding can be memoised once per
/// component instead of resolved by an exact positional sort. `Generated`
/// covers frame corners, hull-ray caps and clip crossings.
#[derive(Clone)]
enum DualVertex {
    Center(u32),
    Generated,
}

#[derive(Clone)]
struct DualRings {
    rings: Vec<Vec<(DualVertex, exact::ExactPoint)>>,
    component_count: usize,
}

/// The radial walk. `triangles[e]` is the origin of half-edge `e`; the walk
/// `incoming -> halfedges[next(incoming)]` rotates around the shared endpoint.
/// Starting from a hull half-edge when one exists makes an unbounded cell's
/// walk begin and end on the hull.
fn dual_incident_faces(mesh: &CertifiedDelaunay, site_count: usize) -> Vec<IncidentFaces> {
    let triangles = mesh.triangles();
    let halfedges = mesh.halfedges();
    let flat = |edge: usize| triangles[edge / 3][edge % 3];
    let mut start_edge = vec![usize::MAX; site_count];
    for edge in 0..halfedges.len() {
        let endpoint = flat(dual_next_halfedge(edge));
        if start_edge[endpoint] == usize::MAX || halfedges[edge] == delaunator::EMPTY {
            start_edge[endpoint] = edge;
        }
    }
    let mut out = Vec::with_capacity(site_count);
    for site in 0..site_count {
        let start = start_edge[site];
        let mut faces = Vec::new();
        let mut boundary = None;
        if start == usize::MAX {
            out.push((faces, boundary));
            continue;
        }
        let mut incoming = start;
        loop {
            faces.push(incoming / 3);
            let outgoing = dual_next_halfedge(incoming);
            let next = halfedges[outgoing];
            if next == delaunator::EMPTY {
                // `start` was a hull half-edge (q -> site) with no twin and
                // `outgoing` is the hull half-edge (site -> r).
                boundary = Some((start, outgoing));
                break;
            }
            incoming = next;
            if incoming == start {
                break;
            }
        }
        out.push((faces, boundary));
    }
    out
}

type IncidentFaces = (Vec<usize>, Option<(usize, usize)>);

/// Cap one unbounded dual ray on the private frame. Identical construction to
/// the shipped hull-edge lane in `build_primitives`.
fn dual_ray_hit(
    sites: &[Site],
    pair: [usize; 2],
    opposite: usize,
    frame: &[exact::ExactPoint; 4],
) -> Result<(u8, exact::ExactPoint)> {
    let line = exact::ExactLine::perpendicular_bisector(
        sites[pair[0]].point.xy(),
        sites[pair[1]].point.xy(),
    );
    let left = exact::ExactPoint::from_xy(sites[pair[0]].point.xy());
    let right = exact::ExactPoint::from_xy(sites[pair[1]].point.xy());
    let opposite_sign = exact::orient_points(
        &left,
        &right,
        &exact::ExactPoint::from_xy(sites[opposite].point.xy()),
    );
    frame_intersections(&line, frame)?
        .into_iter()
        .find(|(_, point)| exact::orient_points(&left, &right, point) != opposite_sign)
        .ok_or_else(|| voronoi_error("certified Voronoi hull ray misses exact frame"))
}

fn dual_close_hull_chain(
    centers: &[(DualVertex, exact::ExactPoint)],
    entry_ray: &(u8, exact::ExactPoint),
    exit_ray: &(u8, exact::ExactPoint),
    frame: &[exact::ExactPoint; 4],
    exterior: bool,
) -> Result<Vec<(DualVertex, exact::ExactPoint)>> {
    let mut ring = Vec::with_capacity(centers.len() + 6);
    ring.push((DualVertex::Generated, entry_ray.1.clone()));
    ring.extend(centers.iter().cloned());
    ring.push((DualVertex::Generated, exit_ray.1.clone()));
    let exit_side = exit_ray.0 as usize;
    let entry_side = entry_ray.0 as usize;
    if exterior {
        let steps = match (exit_side + 4 - entry_side) % 4 {
            0 => 4,
            steps => steps,
        };
        let mut side = exit_side;
        for _ in 0..steps {
            ring.push((DualVertex::Generated, frame[side].clone()));
            side = (side + 3) % 4;
        }
    } else {
        let steps = (entry_side + 4 - exit_side) % 4;
        let mut side = exit_side;
        for _ in 0..steps {
            side = (side + 1) % 4;
            ring.push((DualVertex::Generated, frame[side].clone()));
        }
    }
    ring.dedup_by(|left, right| left.1.same_position(&right.1));
    while ring.len() > 1 && ring[0].1.same_position(&ring.last().expect("non-empty").1) {
        ring.pop();
    }
    let points: Vec<_> = ring.iter().map(|(_, point)| point.clone()).collect();
    if exact::cycle_orientation(&points)? == exact::ExactSign::Negative {
        ring.reverse();
    }
    Ok(ring)
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
    ring: Vec<(DualVertex, exact::ExactPoint)>,
    rect: &[exact::ExactPoint; 4],
    axis_aligned: bool,
) -> Result<Vec<(DualVertex, exact::ExactPoint)>> {
    let mut current = ring;
    for side in 0..4 {
        if current.is_empty() {
            return Ok(current);
        }
        let (a, b) = (&rect[side], &rect[(side + 1) % 4]);
        let boundary = exact::ExactLine::through_points(a, b)?;
        let inside = |point: &exact::ExactPoint| -> bool {
            if !axis_aligned {
                return exact::orient_points(a, b, point) != exact::ExactSign::Negative;
            }
            match side {
                0 => point.compare_y(&rect[0]) != Ordering::Less,
                1 => point.compare_x(&rect[1]) != Ordering::Greater,
                2 => point.compare_y(&rect[2]) != Ordering::Greater,
                _ => point.compare_x(&rect[3]) != Ordering::Less,
            }
        };
        let mut next = Vec::with_capacity(current.len() + 1);
        for index in 0..current.len() {
            let this_in = inside(&current[index].1);
            let prev_in = inside(&current[(index + current.len() - 1) % current.len()].1);
            if this_in != prev_in {
                let edge = exact::ExactLine::through_points(
                    &current[(index + current.len() - 1) % current.len()].1,
                    &current[index].1,
                )?;
                let hit = exact::line_intersection(&boundary, &edge);
                if hit.is_finite() {
                    next.push((DualVertex::Generated, hit));
                }
            }
            if this_in {
                next.push(current[index].clone());
            }
        }
        next.dedup_by(|left, right| left.1.same_position(&right.1));
        while next.len() > 1 && next[0].1.same_position(&next.last().expect("non-empty").1) {
            next.pop();
        }
        current = next;
    }
    Ok(current)
}

fn dual_build_rings(
    sites: &[Site],
    mesh: &CertifiedDelaunay,
    complex: &DelaunayComplex,
    clip: &NormalizedClip<'_>,
    budget: &mut ExpansionBudget,
) -> Result<DualRings> {
    let mut frame_inputs: Vec<_> = sites
        .iter()
        .map(|site| exact::ExactPoint::from_xy(site.point.xy()))
        .collect();
    frame_inputs.extend(clip.rings.iter().flatten().cloned());
    frame_inputs.extend((0..complex.component_count()).map(|id| complex.center(id).clone()));
    let frame = match exact::enclosing_frame_binary64(&frame_inputs) {
        Some(frame) => frame,
        None => exact::enclosing_frame(&frame_inputs)?,
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

    let triangles = mesh.triangles();
    let halfedges = mesh.halfedges();
    let flat = |edge: usize| triangles[edge / 3][edge % 3];
    let incidence = dual_incident_faces(mesh, sites.len());
    let mut rings = Vec::with_capacity(sites.len());

    for (site, (faces, boundary)) in incidence.into_iter().enumerate() {
        budget.add(faces.len().saturating_add(1))?;
        let mut ring = if let Some((first_hull, last_hull)) = boundary {
            // (q -> site) and (site -> r): the two hull edges bounding the cell.
            let entry = [flat(first_hull), flat(dual_next_halfedge(first_hull))];
            let entry_opposite = flat(dual_next_halfedge(dual_next_halfedge(first_hull)));
            let exit = [flat(last_hull), flat(dual_next_halfedge(last_hull))];
            let exit_opposite = flat(dual_next_halfedge(dual_next_halfedge(last_hull)));
            debug_assert_eq!(halfedges[first_hull], delaunator::EMPTY);
            debug_assert_eq!(halfedges[last_hull], delaunator::EMPTY);
            let entry_ray = dual_ray_hit(sites, entry, entry_opposite, &frame)?;
            let exit_ray = dual_ray_hit(sites, exit, exit_opposite, &frame)?;
            let centers: Vec<(DualVertex, exact::ExactPoint)> = faces
                .iter()
                .map(|&face| {
                    let component = complex.component_of_face(face);
                    (
                        DualVertex::Center(component as u32),
                        complex.center(component).clone(),
                    )
                })
                .collect();
            let site_point = exact::ExactPoint::from_xy(sites[site].point.xy());
            // Positive topology fixes the open center chain. The two frame
            // paths are complementary clipped faces; exact ownership decides
            // only which one contains this site, including the zero-separator
            // isolated-ring case. Ring handedness is normalization, not a
            // second candidate.
            let interior = dual_close_hull_chain(&centers, &entry_ray, &exit_ray, &frame, false)?;
            let interior_points: Vec<_> = interior.iter().map(|(_, point)| point.clone()).collect();
            if exact::point_in_cycle(&interior_points, &site_point) == exact::PointInCycle::Inside {
                interior
            } else {
                let exterior =
                    dual_close_hull_chain(&centers, &entry_ray, &exit_ray, &frame, true)?;
                let exterior_points: Vec<_> =
                    exterior.iter().map(|(_, point)| point.clone()).collect();
                if exact::point_in_cycle(&exterior_points, &site_point)
                    != exact::PointInCycle::Inside
                {
                    return Err(voronoi_error(
                        "dual hull cell has no admissible frame closure",
                    ));
                }
                exterior
            }
        } else {
            let mut ring = Vec::with_capacity(faces.len() + 6);
            for &face in &faces {
                let component = complex.component_of_face(face);
                ring.push((
                    DualVertex::Center(component as u32),
                    complex.center(component).clone(),
                ));
            }
            ring
        };
        ring.dedup_by(|left, right| left.1.same_position(&right.1));
        while ring.len() > 1 && ring[0].1.same_position(&ring.last().expect("non-empty").1) {
            ring.pop();
        }
        if ring.len() >= 3 {
            let points: Vec<_> = ring.iter().map(|(_, point)| point.clone()).collect();
            if exact::cycle_orientation(&points)? == exact::ExactSign::Negative {
                ring.reverse();
            }
        }
        let clipped = dual_clip_convex(ring, &rect, axis_aligned)?;
        if clipped.len() < 3 {
            return Err(voronoi_error(
                "dual Voronoi cell degenerated under the clip",
            ));
        }
        budget.add(clipped.len())?;
        rings.push(clipped);
        let _ = site;
    }
    Ok(DualRings {
        rings,
        component_count: complex.component_count(),
    })
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
/// `coverage_is_valid` proves that the emitted binary64 cells are individually
/// valid with disjoint interiors, while directed edge cancellation separately
/// proves that their union has exactly the rectangular clip boundary. Neither
/// certificate establishes nearest-site ownership or arbitrary polygon/hole
/// completeness; ownership follows the certified face mapping, and polygon
/// clips remain on the exact arrangement lane.
fn dual_embed(
    rings: &DualRings,
    clip: &NormalizedClip<'_>,
    budget: &mut ExpansionBudget,
) -> Result<EmbeddedVoronoi> {
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
    let mut center_rounded: Vec<Option<XY>> = vec![None; rings.component_count];
    let mut interned: HashMap<PointKey, exact::ExactPoint> = HashMap::new();
    let mut resolve = |vertex: &DualVertex, point: &exact::ExactPoint| -> Result<XY> {
        if let DualVertex::Center(component) = *vertex
            && let Some(xy) = center_rounded[component as usize]
        {
            return Ok(xy);
        }
        let mut xy = point.round_nearest_even()?;
        let key = PointKey::new(xy);
        match interned.entry(key) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                if !existing.get().same_position(point) {
                    return Err(voronoi_error(
                        "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                    ));
                }
                // Equal exact generated vertices can reach signed zero through
                // different algebraic expressions. Reuse the first admitted
                // representative so shared binary64 vertices are bit-identical.
                xy = existing.get().round_nearest_even()?;
            },
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(point.clone());
            },
        }
        if let DualVertex::Center(component) = *vertex {
            center_rounded[component as usize] = Some(xy);
        }
        Ok(xy)
    };
    let mut rounded_rings: Vec<Vec<XY>> = Vec::with_capacity(rings.rings.len());
    for ring in &rings.rings {
        let mut rounded = Vec::with_capacity(ring.len());
        for (vertex, point) in ring {
            rounded.push(resolve(vertex, point)?);
        }
        rounded_rings.push(rounded);
    }
    budget.add(rounded_rings.iter().map(Vec::len).sum::<usize>())?;
    let mut polygons = Vec::with_capacity(rounded_rings.len());
    for (exact_ring, rounded) in rings.rings.iter().zip(&mut rounded_rings) {
        let start = exact_ring
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| left.1.compare_lex(&right.1))
            .map_or(0, |(index, _)| index);
        rounded.rotate_left(start);
        let mut coordinates: Vec<Point> = rounded
            .iter()
            .map(|xy| Point::new_unchecked_xy(xy.x, xy.y))
            .collect();
        let rounded_ring: Vec<_> = coordinates
            .iter()
            .map(|point| exact::ExactPoint::from_xy(point.xy()))
            .collect();
        if exact::cycle_orientation(&rounded_ring)? != exact::ExactSign::Positive {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        }
        coordinates.push(coordinates[0].clone());
        let polygon = Polygon::new(Ring::closed(coordinates)?, Vec::new());
        if !polygon_is_valid(&polygon) {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        }
        budget.add(rounded.len())?;
        polygons.push(polygon);
    }
    let shape_rows: Vec<_> = polygons.iter().cloned().map(Shape::Polygon).collect();
    if !coverage_is_valid(&shape_rows, 0.0)? {
        return Err(voronoi_error(
            "exact Voronoi subdivision has no topology-preserving binary64 embedding",
        ));
    }
    // Completeness by EDGE CANCELLATION rather than by a full overlay union.
    //
    // Every rounded cell edge is entered directed. An edge interior to the
    // partition is walked once in each direction by its two owners and cancels;
    // an edge on the outer boundary is walked exactly once. So: if every
    // undirected edge occurs either twice with opposite directions or once, and
    // the once-occurring edges tile the clip rectangle's four sides exactly with
    // no gap and no overlap, then the union of the cells IS the clip rectangle.
    // Combined with `coverage_is_valid` (each cell valid, interiors disjoint)
    // that is the same statement the union/difference pair made, computed on
    // stored binary64 with no overlay.
    let mut directed: HashMap<[PointKey; 2], (i32, usize)> = HashMap::new();
    let mut edge_count = 0_usize;
    for polygon in &polygons {
        for index in 0..polygon.shell.len().saturating_sub(1) {
            let (left, right) = (
                PointKey::new(polygon.shell.point_at(index).xy()),
                PointKey::new(polygon.shell.point_at(index + 1).xy()),
            );
            let (key, delta) = if left <= right {
                ([left, right], 1)
            } else {
                ([right, left], -1)
            };
            let occurrence = directed.entry(key).or_insert((0, 0));
            occurrence.0 += delta;
            occurrence.1 += 1;
            edge_count += 1;
        }
    }
    budget.add(edge_count)?;
    let bounds = [
        clip.fixed[0]
            .iter()
            .map(|xy| xy.x)
            .fold(f64::INFINITY, f64::min),
        clip.fixed[0]
            .iter()
            .map(|xy| xy.x)
            .fold(f64::NEG_INFINITY, f64::max),
        clip.fixed[0]
            .iter()
            .map(|xy| xy.y)
            .fold(f64::INFINITY, f64::min),
        clip.fixed[0]
            .iter()
            .map(|xy| xy.y)
            .fold(f64::NEG_INFINITY, f64::max),
    ];
    // side 0: y == miny, 1: x == maxx, 2: y == maxy, 3: x == minx
    let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    let mut internal_edges = Vec::new();
    let mut occurrences: Vec<_> = directed.iter().collect();
    occurrences.sort_unstable_by_key(|(key, _)| **key);
    for &(key, &(balance, count)) in &occurrences {
        if balance == 0 {
            if count != 2 {
                return Err(voronoi_error(
                    "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                ));
            }
            internal_edges.push(*key);
            continue;
        }
        if balance.abs() != 1 || count != 1 {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        }
        let (a, b) = (key[0].xy(), key[1].xy());
        let side = if a.y == bounds[2] && b.y == bounds[2] {
            0
        } else if a.x == bounds[1] && b.x == bounds[1] {
            1
        } else if a.y == bounds[3] && b.y == bounds[3] {
            2
        } else if a.x == bounds[0] && b.x == bounds[0] {
            3
        } else {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        };
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
                return Err(voronoi_error(
                    "exact Voronoi subdivision has no topology-preserving binary64 embedding",
                ));
            }
            cursor = hi;
        }
        if cursor != end {
            return Err(voronoi_error(
                "exact Voronoi subdivision has no topology-preserving binary64 embedding",
            ));
        }
    }
    internal_edges.sort_unstable();
    let edges = internal_edges
        .into_iter()
        .map(|key| {
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_unchecked_xy(key[0].xy().x, key[0].xy().y),
                Point::new_unchecked_xy(key[1].xy().x, key[1].xy().y),
            ]))
        })
        .collect::<Result<Vec<_>>>()?;
    let clip_coordinates: Vec<Point> = clip.fixed[0]
        .iter()
        .map(|xy| Point::new_unchecked_xy(xy.x, xy.y))
        .chain(std::iter::once(Point::new_unchecked_xy(
            clip.fixed[0][0].x,
            clip.fixed[0][0].y,
        )))
        .collect();
    let comparison_clip = Shape::Polygon(Polygon::new(Ring::closed(clip_coordinates)?, Vec::new()));
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
) -> Result<EmbeddedVoronoi> {
    let clip = normalize_clip(sites, boundary, budget)?;
    let mesh = certified_delaunay(sites)?;
    if mesh.collinear_order().is_some() {
        return Err(voronoi_error(
            "collinear sites require the exact one-dimensional lane",
        ));
    }
    let complex = delaunay_complex(&mesh, sites)?;
    let rings = dual_build_rings(sites, &mesh, &complex, &clip, budget)?;
    dual_embed(&rings, &clip, budget)
}
