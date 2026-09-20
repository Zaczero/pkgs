//! Exact labelled Voronoi subdivision and its sole binary64 projection.

#![expect(
    clippy::clone_on_copy,
    clippy::explicit_iter_loop,
    clippy::float_cmp,
    clippy::inconsistent_struct_constructor,
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::needless_range_loop,
    clippy::unnecessary_semicolon,
    clippy::unwrap_used,
    reason = "the DCEL mirrors the numbered construction and its proof tests keep mutations visibly local"
)]

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::ControlFlow;

use crate::collections::try_hash_map_with_capacity;
use crate::error::{ResourceError, Result};
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::certify_emitted_incidence;
use crate::geometry::segment_index::YStabbingIndex;
use crate::geometry::tessellation::{
    CertifiedDelaunay, CertifiedPrimalEdge, DelaunayComplex, EMPTY, Site, delaunay_complex, exact,
};
use crate::geometry::{
    Bounds, CoordSeq, CoordWindow, CsrOffsetColumn, ExpansionBudget, GeometryErrorKind, LineSeq,
    Point, PointKey, Polygon, Ring, RingClass, RingEdgeMembership, Segment, Shape, Strictness,
    VoronoiBoundary, XY, coverage_is_valid, for_each_indexed_overlapping_bounds_pair,
    line_segments, polygon_is_valid, ring_edge_membership, try_dedup_by, try_sort_by,
};
#[cfg(test)]
use crate::geometry::{
    GENERATED_ITEM_LIMIT, tessellation::certified_delaunay, tessellation::shape,
};
use crate::{try_collect_vec, try_push_vec, try_reserve_vec};

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
    clip_sources: Vec<ClipSegmentId>,
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

/// The box of the points' nearest binary64 roundings. Rounding is monotone,
/// so exact boxes that meet have rounded boxes that meet, and an exact point in
/// an exact box rounds into its rounded box. A point without a finite rounding
/// has no box, and a missing box meets every box.
fn rounded_box(points: &[exact::ExactPoint]) -> Result<Option<Bounds>> {
    let mut bounds: Option<Bounds> = None;
    for point in points {
        let Some(point) = point.finite_rounding()? else {
            return Ok(None);
        };
        match &mut bounds {
            Some(bounds) => bounds.include_xy(point),
            None => bounds = Some(Bounds::new_raw(point.x, point.y, point.x, point.y)),
        }
    }
    Ok(bounds)
}

/// Closed boxes indexed for point queries. A missing box contains every
/// point, and a missing point lies in every box.
struct BoxIndex {
    index: YStabbingIndex<u32>,
    unbounded: Vec<u32>,
    count: usize,
}

impl BoxIndex {
    fn build(boxes: &[Option<Bounds>]) -> Result<Self> {
        let mut bounded = Vec::new();
        let mut unbounded = Vec::new();
        try_reserve_vec(&mut bounded, boxes.len())?;
        for (item, bounds) in boxes.iter().enumerate() {
            if bounds.is_some() {
                bounded.push(item as u32);
            } else {
                try_push_vec(&mut unbounded, item as u32)?;
            }
        }
        let index = YStabbingIndex::build(bounded, |&item| {
            let bounds = boxes[item as usize].expect("indexed boxes are present");
            (bounds.minx(), bounds.maxx(), bounds.miny(), bounds.maxy())
        })?;
        Ok(Self {
            index,
            unbounded,
            count: boxes.len(),
        })
    }

    /// Visit every box containing `point`, in no particular order, stopping at
    /// the first error.
    fn for_each_containing(
        &self,
        point: Option<XY>,
        mut visit: impl FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let Some(point) = point else {
            return (0..self.count).try_for_each(visit);
        };
        for &item in &self.unbounded {
            visit(item as usize)?;
        }
        let stopped =
            self.index
                .for_each_at_point(point.x, point.y, |&item| match visit(item as usize) {
                    Ok(()) => ControlFlow::Continue(()),
                    Err(error) => ControlFlow::Break(error),
                });
        stopped.map_or(Ok(()), Err)
    }
}

fn budgeted_try_push<T>(values: &mut Vec<T>, value: T, budget: &mut ExpansionBudget) -> Result<()> {
    budget.add(1)?;
    try_push_vec(values, value)
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
    let segment_count = cycles.iter().map(|cycle| cycle.len()).sum();
    let mut boxes = Vec::new();
    try_reserve_vec(&mut boxes, segment_count)?;
    for cycle in cycles {
        for edge in 0..cycle.len() {
            let ends = [cycle[edge].clone(), cycle[(edge + 1) % cycle.len()].clone()];
            boxes.push(rounded_box(&ends)?);
        }
    }
    let mut candidates = Vec::new();
    let mut failure = Ok(());
    let _ = for_each_indexed_overlapping_bounds_pair(&boxes, |left, right| {
        failure = budgeted_try_push(&mut candidates, [left, right], budget);
        if failure.is_err() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })?;
    failure?;
    for (left, bounds) in boxes.iter().enumerate() {
        if bounds.is_none() {
            for right in 0..segment_count {
                if left != right {
                    budgeted_try_push(&mut candidates, [left.min(right), left.max(right)], budget)?;
                }
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
    let mut offsets = Vec::new();
    try_reserve_vec(&mut offsets, cycles.len())?;
    offsets.extend(cycles.iter().scan(0_usize, |offset, cycle| {
        let current = *offset;
        *offset = offset.saturating_add(cycle.len());
        Some(current)
    }));
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
        match exact::segment_intersection(left[0], left[1], right[0], right[1])? {
            exact::SegmentIntersection::None => {},
            exact::SegmentIntersection::Overlap { .. } => return Err(exact_cycle_error()),
            exact::SegmentIntersection::Point(point) => {
                let left_endpoint = position_of(&left, &point)?;
                let right_endpoint = position_of(&right, &point)?;
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
                let mut known_touch = None;
                for (index, touch) in pair_touches.iter().enumerate() {
                    if touch.rings == [left_ring, right_ring]
                        && touch.point.same_position(&point)?
                    {
                        known_touch = Some(index);
                        break;
                    }
                }
                if let Some(index) = known_touch {
                    let touch = &mut pair_touches[index];
                    for (slot, (segment, endpoint)) in
                        [(left, left_endpoint), (right, right_endpoint)]
                            .into_iter()
                            .enumerate()
                    {
                        for (index, direction) in segment.into_iter().enumerate() {
                            if endpoint == Some(index)
                                || position_of(&touch.directions[slot], direction)?.is_some()
                            {
                                continue;
                            }
                            budgeted_try_push(
                                &mut touch.directions[slot],
                                direction.clone(),
                                budget,
                            )?;
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
                    budgeted_try_push(
                        &mut pair_touches,
                        ExactPairTouch {
                            rings: [left_ring, right_ring],
                            point,
                            directions,
                        },
                        budget,
                    )?;
                }
            },
        }
    }
    for touch in &pair_touches {
        let mut rays = Vec::new();
        try_reserve_vec(
            &mut rays,
            touch.directions[0].len() + touch.directions[1].len(),
        )?;
        rays.extend(
            touch.directions[0]
                .iter()
                .map(|target| (0_usize, target))
                .chain(touch.directions[1].iter().map(|target| (1_usize, target))),
        );
        let angle = |left: &exact::ExactPoint, right: &exact::ExactPoint| {
            exact::angle_ccw_cmp(&touch.point, left, right).map_err(|_| exact_cycle_error())
        };
        try_sort_by(&mut rays, |left, right| angle(left.1, right.1))?;
        for [left, right] in rays.array_windows::<2>() {
            angle(left.1, right.1)?;
        }
        if rays.len() > 1 {
            angle(rays.last().unwrap().1, rays[0].1)?;
        }
        let changes = (0..rays.len())
            .filter(|&index| rays[index].0 != rays[(index + 1) % rays.len()].0)
            .count();
        if changes != 2 {
            return Err(exact_cycle_error());
        }
    }
    let mut touch_points = Vec::new();
    try_reserve_vec(&mut touch_points, pair_touches.len())?;
    touch_points.extend(pair_touches.iter().map(|touch| touch.point.clone()));
    try_sort_by(&mut touch_points, exact::ExactPoint::compare_lex)?;
    try_dedup_by(&mut touch_points, exact::ExactPoint::same_position)?;
    let mut parents = Vec::new();
    try_reserve_vec(&mut parents, cycles.len() + touch_points.len())?;
    parents.extend(0..cycles.len() + touch_points.len());
    let mut incidences = Vec::new();
    for touch in &pair_touches {
        let point = position_of(&touch_points, &touch.point)?.expect("touch point was interned");
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
            match exact::point_in_cycle(cycles[shell], point)? {
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
            for point in cycles[other] {
                if exact::point_in_cycle(cycles[hole], point)? == exact::PointInCycle::Inside {
                    return Err(exact_cycle_error());
                }
            }
        }
    }
    try_sort_by(&mut contacts, |left, right| {
        Ok(match left.segments.cmp(&right.segments) {
            Ordering::Equal => left.point.compare_lex(&right.point)?,
            order => order,
        })
    })?;
    try_dedup_by(&mut contacts, |later, earlier| {
        Ok(later.segments == earlier.segments && later.point.same_position(&earlier.point)?)
    })?;
    Ok(ValidatedExactCycles { contacts })
}

/// The index of the first point at `point`'s exact position.
fn position_of<P: std::borrow::Borrow<exact::ExactPoint>>(
    points: &[P],
    point: &exact::ExactPoint,
) -> Result<Option<usize>> {
    for (index, candidate) in points.iter().enumerate() {
        if candidate.borrow().same_position(point)? {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn open_ring(ring: &Ring) -> Result<(Vec<exact::ExactPoint>, Vec<XY>)> {
    let open = ring.len() - 1;
    let mut exact_points = Vec::new();
    let mut fixed = Vec::new();
    try_reserve_vec(&mut exact_points, open)?;
    try_reserve_vec(&mut fixed, open)?;
    for point in ring.iter().take(open) {
        let point = point.xy();
        let exact = exact::ExactPoint::from_xy(point);
        if let Some(previous) = exact_points.last()
            && exact.same_position(previous)?
        {
            continue;
        }
        exact_points.push(exact);
        fixed.push(point);
    }
    if exact_points.len() < 3 {
        return Err(voronoi_error(
            "exact Voronoi clip ring has fewer than three vertices",
        ));
    }
    Ok((exact_points, fixed))
}

fn reverse_ring(points: &mut [exact::ExactPoint], fixed: &mut [XY]) {
    points.reverse();
    fixed.reverse();
}

fn rotate_ring(points: &mut [exact::ExactPoint], fixed: &mut [XY]) -> Result<()> {
    let count = points.len();
    let mut least = 0;
    for candidate in 1..count {
        for offset in 0..count {
            match points[(candidate + offset) % count]
                .compare_lex(&points[(least + offset) % count])?
            {
                Ordering::Less => least = candidate,
                Ordering::Greater => {},
                Ordering::Equal => continue,
            }
            break;
        }
    }
    points.rotate_left(least);
    fixed.rotate_left(least);
    Ok(())
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
            let mut clip_rings = Vec::new();
            try_reserve_vec(&mut clip_rings, 1 + polygon.holes.len())?;
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
                rotate_ring(&mut points, &mut coordinates)?;
                clip_rings.push((points, coordinates));
            }
            try_sort_by(&mut clip_rings[1..], |(left, _), (right, _)| {
                compare_exact_sequences(left, right)
            })?;
            try_reserve_vec(&mut rings, clip_rings.len())?;
            try_reserve_vec(&mut fixed, clip_rings.len())?;
            for (points, coordinates) in clip_rings {
                rings.push(points);
                fixed.push(coordinates);
            }
            Some(polygon)
        },
        VoronoiBoundary::Envelope | VoronoiBoundary::Padded => {
            let mut points = Vec::new();
            try_reserve_vec(&mut points, sites.len())?;
            points.extend(sites.iter().map(|site| site.point));
            let rectangle =
                exact::rectangular_boundary(&points, matches!(boundary, VoronoiBoundary::Padded))?;
            let mut corners = Vec::new();
            try_reserve_vec(&mut corners, rectangle.len())?;
            for corner in &rectangle {
                corners.push(corner.round_nearest_even()?);
            }
            let mut ring = Vec::new();
            try_reserve_vec(&mut ring, rectangle.len())?;
            ring.extend(rectangle);
            try_reserve_vec(&mut rings, 1)?;
            try_reserve_vec(&mut fixed, 1)?;
            rings.push(ring);
            fixed.push(corners);
            None
        },
    };
    let mut open = Vec::new();
    try_reserve_vec(&mut open, rings.len())?;
    open.extend(rings.iter().map(Vec::as_slice));
    let contacts = validate_exact_cycles(&open, 0, budget)?.contacts;
    Ok(NormalizedClip {
        rings,
        fixed,
        source,
        contacts,
    })
}

fn compare_exact_sequences(
    left: &[exact::ExactPoint],
    right: &[exact::ExactPoint],
) -> Result<Ordering> {
    for (left, right) in left.iter().zip(right) {
        let order = left.compare_lex(right)?;
        if order.is_ne() {
            return Ok(order);
        }
    }
    Ok(left.len().cmp(&right.len()))
}

struct SiteAdjacency {
    offsets: Vec<u32>,
    neighbors: Vec<SiteId>,
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
    try_reserve_vec(&mut degrees, site_count)?;
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
    try_reserve_vec(&mut offsets, site_count + 1)?;
    offsets.push(0_u32);
    for degree in degrees {
        let next = (offsets.last().copied().unwrap_or(0) as usize)
            .checked_add(degree)
            .ok_or_else(|| voronoi_error("too many site adjacencies"))?;
        offsets.push(u32::try_from(next).map_err(|_| voronoi_error("too many site adjacencies"))?);
    }
    let neighbor_count = offsets.last().copied().unwrap_or(0) as usize;
    let mut neighbors = Vec::new();
    try_reserve_vec(&mut neighbors, neighbor_count)?;
    neighbors.resize(neighbor_count, SiteId(0));
    let mut cursors = Vec::new();
    try_reserve_vec(&mut cursors, site_count)?;
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
    Ok(SiteAdjacency { offsets, neighbors })
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
        let nearer = |candidate: SiteId, known: SiteId| {
            exact::squared_distance_cmp_point(
                point,
                sites[candidate.0 as usize].point.xy(),
                sites[known.0 as usize].point.xy(),
            )
        };
        for &candidate in &adjacency.neighbors[start..end] {
            if nearer(candidate, best)? == Ordering::Less {
                best = candidate;
            }
        }
        if best != current {
            current = best;
            continue;
        }
        for &candidate in &adjacency.neighbors[start..end] {
            if nearer(candidate, current)? == Ordering::Equal {
                return Err(voronoi_error(
                    "private exact frame atom has a tied nearest site",
                ));
            }
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
        )? {
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
    )? {
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
    if let [Some(a), Some(b), Some(c), Some(d)] = frame.each_ref().map(exact::ExactPoint::as_double)
    {
        let corners = [a, b, c, d];
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
                    if between(&hit, &frame[side], &frame[(side + 1) % 4])? {
                        try_push_vec(&mut hits, (side as u8, hit))?;
                    }
                },
            }
        }
        if certified {
            // Adjacent for the corner dedup below; the order itself carries
            // nothing, since a bisector meets a convex frame at most twice and
            // the caller keeps the one on its side. Rounded keys keep this off
            // the exact comparator.
            try_sort_by(&mut hits, |left, right| {
                Ok(
                    lex_xy(left.1.round_nearest_even()?, right.1.round_nearest_even()?)
                        .then(left.0.cmp(&right.0)),
                )
            })?;
            try_dedup_by(&mut hits, |later, earlier| {
                later.1.same_position(&earlier.1)
            })?;
            return Ok(hits);
        }
    }
    frame_intersections(
        &exact::ExactLine::perpendicular_bisector(sites[0], sites[1])?,
        frame,
    )
}

/// Whether `point` lies in the closed box spanned by `low` and `high`.
fn between(
    point: &exact::ExactPoint,
    low: &exact::ExactPoint,
    high: &exact::ExactPoint,
) -> Result<bool> {
    let within = |low: Ordering, high: Ordering| {
        (low != Ordering::Less && high != Ordering::Greater)
            || (high != Ordering::Less && low != Ordering::Greater)
    };
    Ok(within(point.compare_x(low)?, point.compare_x(high)?)
        && within(point.compare_y(low)?, point.compare_y(high)?))
}

fn frame_intersections(
    line: &exact::ExactLine,
    frame: &[exact::ExactPoint; 4],
) -> Result<Vec<(u8, exact::ExactPoint)>> {
    let mut hits = Vec::new();
    for side in 0..4 {
        let edge = exact::ExactLine::through_points(&frame[side], &frame[(side + 1) % 4])?;
        let hit = exact::line_intersection(line, &edge)?;
        if exact::signed_line_product(line, &hit)? == ExactSign::Zero
            && exact::point_in_cycle(frame, &hit)? != exact::PointInCycle::Outside
            && exact::segment_intersection(&frame[side], &frame[(side + 1) % 4], &hit, &hit)?
                .is_point()
        {
            try_push_vec(&mut hits, (side as u8, hit))?;
        }
    }
    try_sort_by(&mut hits, |left, right| {
        Ok(left.1.compare_lex(&right.1)?.then(left.0.cmp(&right.0)))
    })?;
    try_dedup_by(&mut hits, |later, earlier| {
        later.1.same_position(&earlier.1)
    })?;
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
        && let [a, b, c, d] = ring.as_slice()
    {
        let rect = [a.clone(), b.clone(), c.clone(), d.clone()];
        if axis_aligned_rectangle(&rect)? {
            return Ok(rect);
        }
    }
    exact::axis_aligned_bounds(clip.rings.iter().flatten())
}

/// Whether the four corners form a counter-clockwise axis-aligned rectangle
/// of positive extent, starting at its lower-left corner.
fn axis_aligned_rectangle(rect: &[exact::ExactPoint; 4]) -> Result<bool> {
    Ok(rect[0].compare_y(&rect[1])?.is_eq()
        && rect[2].compare_y(&rect[3])?.is_eq()
        && rect[1].compare_x(&rect[2])?.is_eq()
        && rect[3].compare_x(&rect[0])?.is_eq()
        && rect[0].compare_x(&rect[1])?.is_lt()
        && rect[1].compare_y(&rect[2])?.is_lt())
}

fn in_rectangle(rect: &[exact::ExactPoint; 4], point: &exact::ExactPoint) -> Result<bool> {
    Ok(exact::point_in_cycle(rect, point)? != exact::PointInCycle::Outside)
}

/// Sort points along `origin -> hint` and drop repeated positions.
fn sort_unique_along(
    origin: &exact::ExactPoint,
    hint: &exact::ExactPoint,
    points: &mut Vec<exact::ExactPoint>,
) -> Result<()> {
    try_sort_by(points, |left, right| {
        exact::compare_along(origin, hint, left, right)
    })?;
    try_dedup_by(points, exact::ExactPoint::same_position)
}

fn span_from_points(
    origin: &exact::ExactPoint,
    hint: &exact::ExactPoint,
    mut points: Vec<exact::ExactPoint>,
) -> Result<Option<(exact::ExactPoint, exact::ExactPoint)>> {
    sort_unique_along(origin, hint, &mut points)?;
    Ok(match points.as_slice() {
        [first, .., last] => Some((first.clone(), last.clone())),
        _ => None,
    })
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
    try_reserve_vec(&mut hits, certified.len().saturating_add(8))?;
    hits.extend(certified.into_iter().map(|(_, point)| point));
    for side in 0..4 {
        let start = &rect[side];
        let end = &rect[(side + 1) % 4];
        if exact::signed_line_product(line, start)? == ExactSign::Zero
            && exact::signed_line_product(line, end)? == ExactSign::Zero
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
    try_reserve_vec(&mut points, 2)?;
    if in_rectangle(rect, start)? {
        points.push(start.clone());
    }
    if in_rectangle(rect, end)? {
        points.push(end.clone());
    }
    let mut kept = 0;
    for index in 0..points.len() {
        if exact::segment_intersection(start, end, &points[index], &points[index])?.is_point() {
            points.swap(kept, index);
            kept += 1;
        }
    }
    points.truncate(kept);
    span_from_points(start, end, points)
}

fn hull_point_on_ray(
    center: &exact::ExactPoint,
    mid: &exact::ExactPoint,
    sites: [&exact::ExactPoint; 2],
    opposite_sign: ExactSign,
    point: &exact::ExactPoint,
) -> Result<bool> {
    if point.same_position(center)? {
        return Ok(true);
    }
    if center.same_position(mid)? {
        return Ok(exact::orient_points(sites[0], sites[1], point)? != opposite_sign);
    }
    Ok(
        if exact::orient_points(sites[0], sites[1], center)? == opposite_sign {
            exact::compare_along(center, mid, center, point)? != Ordering::Greater
        } else {
            exact::compare_along(mid, center, center, point)? != Ordering::Greater
        },
    )
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
    let mid = left.midpoint(&right)?;
    let opposite_sign = exact::orient_points(
        &left,
        &right,
        &exact::ExactPoint::from_xy(sites[opposite].point.xy()),
    )?;
    budget.add(hits.len().saturating_add(1))?;
    let mut points = Vec::new();
    try_reserve_vec(&mut points, hits.len().saturating_add(1))?;
    if in_rectangle(rect, center)? {
        points.push(center.clone());
    }
    for hit in hits {
        if hull_point_on_ray(center, &mid, [&left, &right], opposite_sign, &hit)? {
            points.push(hit);
        }
    }
    if points.len() < 2 {
        return Ok(None);
    }
    let (origin, hint) = if center.same_position(&mid)? {
        let mut hint = None;
        for point in &points {
            if !point.same_position(center)? {
                hint = Some(point.clone());
                break;
            }
        }
        let Some(hint) = hint else {
            return Ok(None);
        };
        (center.clone(), hint)
    } else if exact::orient_points(&left, &right, center)? == opposite_sign {
        (center.clone(), mid)
    } else {
        (mid, center.clone())
    };
    span_from_points(&origin, &hint, points)
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
        )? {
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
    let mut outer_is_rect = clip.rings.first().is_some_and(|ring| ring.len() == 4);
    for side in 0..4 {
        outer_is_rect = outer_is_rect && clip.rings[0][side].same_position(&rect[side])?;
    }
    let clip_rings = if outer_is_rect {
        &clip.rings[1..]
    } else {
        clip.rings.as_slice()
    };
    let scaffold_count = 4_usize.saturating_add(clip_rings.iter().map(Vec::len).sum::<usize>());
    budget.add(scaffold_count)?;
    let mut scaffold = Vec::new();
    try_reserve_vec(&mut scaffold, scaffold_count)?;
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
        if exact::signed_line_product(&primitive.line, &primitive.start)? != ExactSign::Zero
            || exact::signed_line_product(&primitive.line, &primitive.end)? != ExactSign::Zero
        {
            return Err(voronoi_error(
                "exact Voronoi primitive endpoints leave their support line",
            ));
        }
        budget.add(2)?;
        try_reserve_vec(&mut primitive.events, 2)?;
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
                )?;
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
                )?;
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
        if exact::signed_line_product(&line, &start)? != ExactSign::Zero
            || exact::signed_line_product(&line, &end)? != ExactSign::Zero
        {
            return Err(voronoi_error(
                "exact Voronoi primitive endpoints leave their support line",
            ));
        }
        budget.add(2)?;
        let mut events = Vec::new();
        try_reserve_vec(&mut events, 2)?;
        events.extend([start.clone(), end.clone()]);
        budgeted_try_push(
            &mut separators,
            ClippedSeparator {
                pair: ids,
                start,
                end,
                events,
            },
            budget,
        )?;
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
    try_reserve_vec(&mut fixed, points.len())?;
    fixed.resize(points.len(), None);
    for candidate in sites
        .iter()
        .map(|site| site.point.xy())
        .chain(clip.fixed.iter().flatten().copied())
    {
        let point = exact::ExactPoint::from_xy(candidate);
        if let Some(index) = lex_position(points, &point)? {
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

/// The index of `point` in lexicographically sorted distinct `points`.
fn lex_position(points: &[exact::ExactPoint], point: &exact::ExactPoint) -> Result<Option<usize>> {
    let (mut low, mut high) = (0, points.len());
    while low < high {
        let middle = low + (high - low) / 2;
        match points[middle].compare_lex(point)? {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(Some(middle)),
        }
    }
    Ok(None)
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
    sort_unique_along(start, end, events)?;
    let bounded = match events.as_slice() {
        [first, .., last] => first.same_position(start)? && last.same_position(end)?,
        [only] => only.same_position(start)? && only.same_position(end)?,
        [] => false,
    };
    if !bounded {
        return Err(voronoi_error(
            "exact primitive event order lost an endpoint",
        ));
    }
    for [left, right] in events.array_windows::<2>() {
        if exact::compare_along(start, end, left, right)? != Ordering::Less {
            return Err(voronoi_error(
                "exact primitive has unordered duplicate events",
            ));
        }
        budgeted_try_push(
            seeds,
            AtomSeed {
                start: left.clone(),
                end: right.clone(),
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
            )? {
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
    try_reserve_vec(left, 2)?;
    left.extend([start.clone(), end.clone()]);
    try_reserve_vec(right, 2)?;
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
            )? {
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
    try_reserve_vec(&mut event_points, seeds.len().saturating_mul(2))?;
    for seed in &seeds {
        event_points.extend([seed.start.clone(), seed.end.clone()]);
    }
    try_sort_by(&mut event_points, exact::ExactPoint::compare_lex)?;
    try_dedup_by(&mut event_points, exact::ExactPoint::same_position)?;
    budget.add(event_points.len())?;
    let fixed = fixed_coordinates(&event_points, sites, clip)?;
    let vertex_id = |point: &exact::ExactPoint| -> Result<VertexId> {
        let index = lex_position(&event_points, point)?
            .ok_or_else(|| voronoi_error("exact event was not globally interned"))?;
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
    try_reserve_vec(&mut mapped, seeds.len())?;
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
    try_sort_by(&mut mapped, |left, right| {
        Ok(left.endpoints.cmp(&right.endpoints))
    })?;
    let mut vertices = Vec::new();
    try_reserve_vec(&mut vertices, event_points.len())?;
    vertices.extend(
        event_points
            .into_iter()
            .zip(fixed)
            .map(|(point, fixed_xy)| NodedVertex { fixed_xy, point }),
    );
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
                try_push_vec(&mut clip_sources, value)?;
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
            if exact::compare_along(ccw_start, ccw_end, stored_start, stored_end)?
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
                            .midpoint(&vertices[seed.endpoints[1].0 as usize].point)?;
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
            budgeted_try_push(
                &mut edges,
                AtomicEdge {
                    endpoints: mapped[start].endpoints,
                    forward: Transition {
                        frame_delta,
                        clip_delta,
                        owner,
                    },
                    separator,
                    clip_sources,
                    frame_side,
                },
                budget,
            )?;
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
        reserve.map_err(|_| ResourceError::Allocation)?;
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
        if edge.clip_sources.array_windows::<2>().any(|[a, b]| a >= b)
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
    try_reserve_vec(&mut subdivision.labels, boundary_faces)?;
    let mut exterior = Vec::new();
    try_reserve_vec(&mut exterior, components)?;
    exterior.resize(components, None);
    let mut points = Vec::new();
    for face in 0..boundary_faces {
        let walk = &topology.face_slots
            [topology.face_starts[face] as usize..topology.face_starts[face + 1] as usize];
        budget.add(walk.len())?;
        points.clear();
        try_reserve_vec(&mut points, walk.len())?;
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
    try_reserve_vec(&mut labels, boundary_faces)?;
    labels.resize(boundary_faces, None);
    let mut queue = VecDeque::new();
    queue
        .try_reserve(boundary_faces)
        .map_err(|_| ResourceError::Allocation)?;
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
        let owner = nearest_site_walk(&start.midpoint(end)?, sites, adjacency, SiteId(0), budget)?;
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
    vertices: Vec<VertexId>,
    edges: Vec<EdgeId>,
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
    vertices: Vec<VertexId>,
    edges: Vec<EdgeId>,
}

fn canonicalize_ring(vertices: &mut [VertexId], edges: &mut [EdgeId]) {
    let open = vertices.len() - 1;
    let least = (0..open)
        .min_by(|&left, &right| {
            (0..open)
                .map(|offset| {
                    vertices[(left + offset) % open].cmp(&vertices[(right + offset) % open])
                })
                .find(|order| order.is_ne())
                .unwrap_or(Ordering::Equal)
        })
        .unwrap();
    vertices[..open].rotate_left(least);
    vertices[open] = vertices[0];
    edges.rotate_left(least);
}

/// The simple ring through `vertices` (closed) and `edges`, canonically
/// rotated, with its exact orientation.
fn simple_ring(
    subdivision: &ExactSubdivision,
    face: BoundaryFaceId,
    mut vertices: Vec<VertexId>,
    mut edges: Vec<EdgeId>,
) -> Result<ExactSimpleRing> {
    let mut points = Vec::new();
    try_reserve_vec(&mut points, vertices.len() - 1)?;
    points.extend(
        vertices[..vertices.len() - 1]
            .iter()
            .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone()),
    );
    let orientation = exact::cycle_orientation(&points)?;
    canonicalize_ring(&mut vertices, &mut edges);
    Ok(ExactSimpleRing {
        face,
        vertices,
        edges,
        orientation,
    })
}

fn split_boundary_face(
    subdivision: &ExactSubdivision,
    boundary_face: BoundaryFaceId,
    result: &mut Vec<ExactSimpleRing>,
) -> Result<()> {
    let topology = &subdivision.topology;
    let walk = &topology.face_slots[topology.face_starts[boundary_face.0 as usize] as usize
        ..topology.face_starts[boundary_face.0 as usize + 1] as usize];
    let owner = |slot: u32| VertexId(topology.owners[slot as usize]);
    let edge = |slot: u32| EdgeId(subdivision.edge_of_slot[slot as usize]);
    let mut distinct = Vec::new();
    try_reserve_vec(&mut distinct, walk.len())?;
    distinct.extend(walk.iter().map(|&slot| owner(slot)));
    distinct.sort_unstable();
    if distinct.array_windows::<2>().all(|[a, b]| a != b) {
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        try_reserve_vec(&mut vertices, walk.len() + 1)?;
        try_reserve_vec(&mut edges, walk.len())?;
        vertices.extend(walk.iter().map(|&slot| owner(slot)));
        vertices.push(vertices[0]);
        edges.extend(walk.iter().map(|&slot| edge(slot)));
        return try_push_vec(
            result,
            simple_ring(subdivision, boundary_face, vertices, edges)?,
        );
    }
    let mut path = Vec::new();
    let mut path_edges = Vec::new();
    try_reserve_vec(&mut path, walk.len())?;
    try_reserve_vec(&mut path_edges, walk.len())?;
    let mut positions = Vec::new();
    try_reserve_vec(&mut positions, subdivision.vertices.len())?;
    positions.resize(subdivision.vertices.len(), None);
    for step in 0..=walk.len() {
        let vertex = owner(walk[step % walk.len()]);
        let incoming = (step > 0).then(|| edge(walk[(step - 1) % walk.len()]));
        if let Some(position) = positions[vertex.0 as usize] {
            let mut vertices = Vec::new();
            let mut edges = Vec::new();
            try_reserve_vec(&mut vertices, path.len() - position + 1)?;
            try_reserve_vec(&mut edges, path_edges.len() - position + 1)?;
            vertices.extend_from_slice(&path[position..]);
            vertices.push(vertex);
            edges.extend_from_slice(&path_edges[position..]);
            edges.push(incoming.expect("repeated boundary vertex has an incoming edge"));
            if vertices.len() < 4 {
                return Err(voronoi_error("pinch split produced a short exact ring"));
            }
            for removed in &path[position + 1..] {
                positions[removed.0 as usize] = None;
            }
            path.truncate(position + 1);
            path_edges.truncate(position);
            try_push_vec(
                result,
                simple_ring(subdivision, boundary_face, vertices, edges)?,
            )?;
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
    Ok(())
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
        split_boundary_face(subdivision, orbit, &mut rings)?;
    }
    if rings.is_empty() {
        return Ok(());
    }
    let mut positives = Vec::new();
    let mut negatives = Vec::new();
    for (index, ring) in rings.iter().enumerate() {
        match ring.orientation {
            ExactSign::Positive => try_push_vec(&mut positives, index)?,
            ExactSign::Negative => try_push_vec(&mut negatives, index)?,
            ExactSign::Zero => {},
        }
    }
    let mut ring_points = Vec::new();
    try_reserve_vec(&mut ring_points, rings.len())?;
    for ring in &rings {
        let count = ring.vertices.len().saturating_sub(1);
        budget.add(count)?;
        let mut points = Vec::new();
        try_reserve_vec(&mut points, count)?;
        points.extend(
            ring.vertices[..count]
                .iter()
                .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone()),
        );
        ring_points.push(points);
    }
    let mut ring_boxes = Vec::new();
    try_reserve_vec(&mut ring_boxes, rings.len())?;
    for points in &ring_points {
        ring_boxes.push(rounded_box(points)?);
    }
    let ring_index = BoxIndex::build(&ring_boxes)?;
    // A ring nests in another only if its first vertex lies in that ring's box.
    let first_vertex = |ring: usize| ring_points[ring][0].finite_rounding();
    // Whether `points` lie in the closed `cycle` with one strictly inside.
    let nested = |points: &[exact::ExactPoint], cycle: &[exact::ExactPoint]| -> Result<bool> {
        let mut inside = false;
        for point in points {
            match exact::point_in_cycle(cycle, point)? {
                exact::PointInCycle::Outside => return Ok(false),
                exact::PointInCycle::Inside => inside = true,
                exact::PointInCycle::Boundary => {},
            }
        }
        Ok(inside)
    };
    for &positive in &positives {
        ring_index.for_each_containing(first_vertex(positive)?, |other| {
            if other == positive {
                return Ok(());
            }
            budget.add(
                ring_points[positive]
                    .len()
                    .saturating_mul(ring_points[other].len()),
            )?;
            if nested(&ring_points[positive], &ring_points[other])? {
                return Err(voronoi_error(
                    "exact same-face ring forest has depth greater than one",
                ));
            }
            Ok(())
        })?;
    }
    let mut holes_by_shell = Vec::new();
    try_reserve_vec(&mut holes_by_shell, positives.len())?;
    holes_by_shell.resize_with(positives.len(), Vec::new);
    // The slot of each positive ring among `positives`.
    let mut shell_slot = Vec::new();
    try_reserve_vec(&mut shell_slot, rings.len())?;
    shell_slot.resize(rings.len(), usize::MAX);
    for (slot, &positive) in positives.iter().enumerate() {
        shell_slot[positive] = slot;
    }
    for &negative in &negatives {
        let mut candidates = Vec::new();
        ring_index.for_each_containing(first_vertex(negative)?, |other| {
            if shell_slot[other] == usize::MAX {
                return Ok(());
            }
            budget.add(
                ring_points[negative]
                    .len()
                    .saturating_mul(ring_points[other].len()),
            )?;
            if nested(&ring_points[negative], &ring_points[other])? {
                try_push_vec(&mut candidates, shell_slot[other])?;
            }
            Ok(())
        })?;
        candidates.sort_unstable();
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
            if exact::point_in_cycle(&ring_points[positives[candidate]], parent_point)?
                == exact::PointInCycle::Inside
            {
                continue;
            }
            let candidate_point =
                &subdivision.vertices[rings[positives[candidate]].vertices[0].0 as usize].point;
            if exact::point_in_cycle(&ring_points[positives[parent]], candidate_point)?
                != exact::PointInCycle::Inside
            {
                return Err(voronoi_error("exact face ring forest is non-laminar"));
            }
            parent = candidate;
        }
        try_push_vec(&mut holes_by_shell[parent], negative)?;
    }
    let mut taken = Vec::new();
    try_reserve_vec(&mut taken, rings.len())?;
    taken.extend(rings.into_iter().map(Some));
    let mut rings = taken;
    for (slot, positive) in positives.into_iter().enumerate() {
        let shell = rings[positive].take().unwrap();
        let mut holes = Vec::new();
        try_reserve_vec(&mut holes, holes_by_shell[slot].len())?;
        holes.extend(
            holes_by_shell[slot]
                .iter()
                .map(|&hole| rings[hole].take().unwrap()),
        );
        if walk_label(subdivision, shell.face).owner != Some(owner)
            || holes
                .iter()
                .any(|hole| walk_label(subdivision, hole.face).owner != Some(owner))
        {
            return Err(voronoi_error("exact ring escaped its owner group"));
        }
        let mut edge_ids: Vec<EdgeId> = Vec::new();
        try_reserve_vec(
            &mut edge_ids,
            shell.edges.len() + holes.iter().map(|hole| hole.edges.len()).sum::<usize>(),
        )?;
        edge_ids.extend(
            shell
                .edges
                .iter()
                .chain(holes.iter().flat_map(|hole| hole.edges.iter())),
        );
        edge_ids.sort_unstable();
        if edge_ids.array_windows::<2>().any(|[a, b]| a == b) {
            return Err(voronoi_error(
                "exact face shell and holes share an atomic edge",
            ));
        }
        let mut exact_rings = Vec::new();
        try_reserve_vec(&mut exact_rings, 1 + holes.len())?;
        for ring in std::iter::once(&shell).chain(holes.iter()) {
            let mut points = Vec::new();
            try_reserve_vec(&mut points, ring.vertices.len() - 1)?;
            points.extend(
                ring.vertices[..ring.vertices.len() - 1]
                    .iter()
                    .map(|vertex| subdivision.vertices[vertex.0 as usize].point.clone()),
            );
            exact_rings.push(points);
        }
        let mut exact_ring_refs = Vec::new();
        try_reserve_vec(&mut exact_ring_refs, exact_rings.len())?;
        exact_ring_refs.extend(exact_rings.iter().map(Vec::as_slice));
        validate_exact_cycles(&exact_ring_refs, 0, budget)?;
        budgeted_try_push(
            parts,
            ExactCellPart {
                owner,
                face: shell.face,
                shell,
                holes,
            },
            budget,
        )?;
    }
    Ok(())
}

fn assemble_cells(
    subdivision: &ExactSubdivision,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ExactCellPart>> {
    let mut selected = Vec::new();
    try_reserve_vec(&mut selected, subdivision.labels.len())?;
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
    try_sort_by(&mut parts, |left, right| {
        Ok(left
            .owner
            .cmp(&right.owner)
            .then_with(|| left.shell.vertices.cmp(&right.shell.vertices)))
    })?;
    Ok(parts)
}

/// One separator group's atoms at each vertex, as `(vertex, slot, other end)`
/// sorted by vertex and then slot.
type SeparatorIncidence = [(VertexId, usize, VertexId)];

fn incident_at(incidence: &SeparatorIncidence, vertex: VertexId) -> &SeparatorIncidence {
    let start = incidence.partition_point(|&(known, ..)| known < vertex);
    let end = incidence.partition_point(|&(known, ..)| known <= vertex);
    &incidence[start..end]
}

fn walk_separator_from(
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    incidence: &SeparatorIncidence,
    used: &mut [bool],
    start: VertexId,
    budget: &mut ExpansionBudget,
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut current = start;
    let mut vertices = Vec::new();
    let mut chain_edges = Vec::new();
    try_push_vec(&mut vertices, current)?;
    while let Some(&(_, slot, next)) = incident_at(incidence, current)
        .iter()
        .find(|&&(_, slot, _)| !used[slot])
    {
        used[slot] = true;
        try_push_vec(&mut chain_edges, group[slot].1)?;
        current = next;
        try_push_vec(&mut vertices, current)?;
    }
    if vertices.len() < 2 {
        return Err(voronoi_error("empty Voronoi edge chain"));
    }
    if vertices.first() == vertices.last() {
        return Err(voronoi_error("Voronoi separator forms a closed cycle"));
    }
    if vertices.iter().rev().lt(vertices.iter()) {
        vertices.reverse();
        chain_edges.reverse();
    }
    budget.add(vertices.len())?;
    try_push_vec(result, ExactEdgeChain {
        separator,
        vertices,
        edges: chain_edges,
    })
}

fn assemble_separator_group(
    subdivision: &ExactSubdivision,
    separator: [SiteId; 2],
    group: &[([SiteId; 2], EdgeId)],
    budget: &mut ExpansionBudget,
    result: &mut Vec<ExactEdgeChain>,
) -> Result<()> {
    let mut incidence = Vec::new();
    try_reserve_vec(&mut incidence, group.len().saturating_mul(2))?;
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        let [left, right] = subdivision.edges[edge_id.0 as usize].endpoints;
        incidence.extend([(left, slot, right), (right, slot, left)]);
    }
    incidence.sort_unstable();
    if incidence.array_windows::<3>().any(|[first, _, third]| first.0 == third.0) {
        return Err(voronoi_error("Voronoi separator branches"));
    }
    let mut used = Vec::new();
    try_reserve_vec(&mut used, group.len())?;
    used.resize(group.len(), false);
    let end_of_path = |vertex: VertexId| incident_at(&incidence, vertex).len() == 1;
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let [left, right] = subdivision.edges[edge_id.0 as usize].endpoints;
        let start = if end_of_path(left) {
            left
        } else if end_of_path(right) {
            right
        } else {
            continue;
        };
        walk_separator_from(
            separator, group, &incidence, &mut used, start, budget, result,
        )?;
    }
    for (slot, (_, edge_id)) in group.iter().enumerate() {
        if used[slot] {
            continue;
        }
        let start = subdivision.edges[edge_id.0 as usize].endpoints[0];
        walk_separator_from(
            separator, group, &incidence, &mut used, start, budget, result,
        )?;
    }
    Ok(())
}

fn assemble_edges(
    subdivision: &ExactSubdivision,
    budget: &mut ExpansionBudget,
) -> Result<Vec<ExactEdgeChain>> {
    let mut forward_slot = Vec::new();
    try_reserve_vec(&mut forward_slot, subdivision.edges.len())?;
    forward_slot.resize(subdivision.edges.len(), u32::MAX);
    for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
        let atomic = &subdivision.edges[edge as usize];
        if subdivision.topology.owners[slot] == atomic.endpoints[0].0 {
            forward_slot[edge as usize] = slot as u32;
        }
    }
    let mut selected = Vec::new();
    for (index, edge) in subdivision.edges.iter().enumerate() {
        let Some(separator) = edge.separator else {
            continue;
        };
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
        if selected_cell_label(left) || selected_cell_label(right) {
            try_push_vec(&mut selected, (separator, EdgeId(index as u32)))?;
        }
    }
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
    try_sort_by(&mut result, |left, right| {
        Ok(left
            .separator
            .cmp(&right.separator)
            .then_with(|| left.vertices.cmp(&right.vertices)))
    })?;
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
    let mut ends = Vec::new();
    try_push_vec(&mut ends, 0)?;
    Ok(EmbeddedEdges::Packed {
        coords: CoordSeq::try_from_vecs(Vec::new(), Vec::new(), None, None)?,
        offsets: CsrOffsetColumn::try_new(ends, 0)?,
    })
}

/// A fallibly allocated copy of `values`.
fn copied<T: Copy>(values: &[T]) -> Result<Vec<T>> {
    let mut copy = Vec::new();
    try_reserve_vec(&mut copy, values.len())?;
    copy.extend_from_slice(values);
    Ok(copy)
}

fn pack_separator_chains(
    chains: &[ExactEdgeChain],
    xy_of: impl Fn(VertexId) -> Result<XY>,
) -> Result<EmbeddedEdges> {
    let vertices: usize = chains.iter().map(|chain| chain.vertices.len()).sum();
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    try_reserve_vec(&mut xs, vertices)?;
    try_reserve_vec(&mut ys, vertices)?;
    let mut ends = Vec::new();
    try_reserve_vec(&mut ends, chains.len().saturating_add(1))?;
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

    #[cfg(test)]
    pub(super) fn into_line_seqs(self) -> Result<Vec<LineSeq>> {
        match self.canonical() {
            Self::Packed { coords, offsets } => {
                let ends = offsets.as_slice();
                try_collect_vec(ends.array_windows::<2>().map(|&[start, end]| {
                    let range = start as usize..end as usize;
                    LineSeq::try_new(CoordSeq::try_from_vecs(
                        copied(&coords.xs()[range.clone()])?,
                        copied(&coords.ys()[range])?,
                        None,
                        None,
                    )?)
                }))
            },
            Self::Segments(segments) => try_collect_vec(segments.into_iter().map(|[a, b]| {
                LineSeq::try_new(CoordSeq::try_from_vecs(
                    copied(&[a.x, b.x])?,
                    copied(&[a.y, b.y])?,
                    None,
                    None,
                )?)
            })),
        }
    }

    /// One packed XY line column with CSR vertex offsets, built directly:
    /// no per-edge geometry, no per-edge allocation.
    pub(super) fn into_line_column(self) -> Result<(CoordSeq, CsrOffsetColumn)> {
        match self.canonical() {
            Self::Packed { coords, offsets } => Ok((coords, offsets)),
            Self::Segments(segments) => {
                let vertices = segments.len() * 2;
                let mut xs = Vec::new();
                let mut ys = Vec::new();
                let mut ends = Vec::new();
                try_reserve_vec(&mut xs, vertices)?;
                try_reserve_vec(&mut ys, vertices)?;
                try_reserve_vec(&mut ends, segments.len() + 1)?;
                for [a, b] in &segments {
                    xs.extend([a.x, b.x]);
                    ys.extend([a.y, b.y]);
                }
                ends.extend((0..=segments.len()).map(|edge| edge * 2));
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
    try_reserve_vec(order, incident.len())?;
    order.extend_from_slice(incident);
    try_sort_by(order, |&left, &right| compare(left, right))?;
    for &[left, right] in order.array_windows::<2>() {
        compare(left, right)?;
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
    // The subdivision's vertices are the lexicographically sorted distinct
    // event points, so a position is found by binary search.
    let vertex_at = |point: &exact::ExactPoint| -> Result<Option<usize>> {
        let (mut low, mut high) = (0, subdivision.vertices.len());
        while low < high {
            let middle = low + (high - low) / 2;
            match subdivision.vertices[middle].point.compare_lex(point)? {
                Ordering::Less => low = middle + 1,
                Ordering::Greater => high = middle,
                Ordering::Equal => return Ok(Some(middle)),
            }
        }
        Ok(None)
    };
    let mut live = Vec::new();
    try_reserve_vec(&mut live, subdivision.vertices.len())?;
    live.resize(subdivision.vertices.len(), false);
    let mut live_edges = Vec::new();
    for part in &parts {
        for ring in std::iter::once(&part.shell).chain(&part.holes) {
            try_reserve_vec(&mut live_edges, ring.edges.len())?;
            live_edges.extend_from_slice(&ring.edges);
            for vertex in &ring.vertices {
                live[vertex.0 as usize] = true;
            }
        }
    }
    for chain in chains {
        try_reserve_vec(&mut live_edges, chain.edges.len())?;
        live_edges.extend_from_slice(&chain.edges);
        for vertex in &chain.vertices {
            live[vertex.0 as usize] = true;
        }
    }
    for (edge_index, edge) in subdivision.edges.iter().enumerate() {
        if !edge.clip_sources.is_empty() {
            try_push_vec(&mut live_edges, EdgeId(edge_index as u32))?;
            live[edge.endpoints[0].0 as usize] = true;
            live[edge.endpoints[1].0 as usize] = true;
        }
    }
    live_edges.sort_unstable();
    live_edges.dedup();
    for point in clip.rings.iter().flatten() {
        let vertex = vertex_at(point)?
            .ok_or_else(|| voronoi_error("exact clip vertex is absent from the snap arena"))?;
        live[vertex] = true;
    }
    let mut points = Vec::new();
    let mut exact_to_embedded = Vec::new();
    try_reserve_vec(&mut exact_to_embedded, subdivision.vertices.len())?;
    exact_to_embedded.resize(subdivision.vertices.len(), None);
    for (index, vertex) in subdivision.vertices.iter().enumerate() {
        if !live[index] {
            continue;
        }
        let point = vertex
            .fixed_xy
            .map_or_else(|| vertex.point.round_nearest_even(), Ok)?;
        let embedded = EmbeddedVertexId(points.len() as u32);
        exact_to_embedded[index] = Some(embedded);
        budgeted_try_push(&mut points, point, budget)?;
    }
    let mut keys = Vec::new();
    try_reserve_vec(&mut keys, points.len())?;
    keys.extend(points.iter().map(|&point| PointKey::new(point)));
    keys.sort_unstable();
    if keys.array_windows::<2>().any(|[a, b]| a == b) {
        return Err(embedding_failure());
    }
    let embedded_point = |vertex: VertexId| -> Result<XY> {
        let embedded = exact_to_embedded[vertex.0 as usize]
            .ok_or_else(|| voronoi_error("live edge has a non-live endpoint"))?;
        Ok(points[embedded.0 as usize])
    };
    let rounded_exact = |vertex| embedded_point(vertex).map(exact::ExactPoint::from_xy);
    for &edge_id in &live_edges {
        let edge = &subdivision.edges[edge_id.0 as usize];
        let rounded = [
            rounded_exact(edge.endpoints[0])?,
            rounded_exact(edge.endpoints[1])?,
        ];
        if rounded[0].same_position(&rounded[1])? {
            return Err(embedding_failure());
        }
        if edge.separator.is_none() {
            continue;
        }
        let side_of = |site: SiteId| {
            exact::orient_points(
                &rounded[0],
                &rounded[1],
                &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
            )
        };
        let (left, right) = match edge.forward.owner {
            OwnerTransition::Boundary { left, right } if left.is_some() || right.is_some() => {
                (left, right)
            },
            OwnerTransition::Boundary { .. } | OwnerTransition::Preserve => {
                return Err(voronoi_error("live separator has incomplete ownership"));
            },
        };
        if let Some(left) = left
            && side_of(left)? != ExactSign::Positive
        {
            return Err(embedding_failure());
        }
        if let Some(right) = right
            && side_of(right)? != ExactSign::Negative
        {
            return Err(embedding_failure());
        }
    }
    // No site may lie on a rounded separator: each site probes the boxes of
    // the separators around it.
    let mut marked = Vec::new();
    let mut marked_boxes = Vec::new();
    for &edge_id in &live_edges {
        let edge = &subdivision.edges[edge_id.0 as usize];
        if edge.separator.is_some() {
            let [start, end] = edge.endpoints.map(embedded_point);
            let (start, end) = (start?, end?);
            try_push_vec(&mut marked, [start, end])?;
            try_push_vec(
                &mut marked_boxes,
                Some(Bounds::new_raw(
                    start.x.min(end.x),
                    start.y.min(end.y),
                    start.x.max(end.x),
                    start.y.max(end.y),
                )),
            )?;
        }
    }
    let marked_index = BoxIndex::build(&marked_boxes)?;
    for site in sites {
        let point = site.point.xy();
        marked_index.for_each_containing(Some(point), |separator| {
            budget.add(1)?;
            let [start, end] = marked[separator].map(exact::ExactPoint::from_xy);
            let site = exact::ExactPoint::from_xy(point);
            if exact::segment_intersection(&start, &end, &site, &site)?.is_point() {
                return Err(embedding_failure());
            }
            Ok(())
        })?;
    }
    let mut emitted_incidence = Vec::new();
    for &edge_id in &live_edges {
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
    certify_emitted_incidence(&emitted_incidence).map_err(|error| {
        if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) {
            error
        } else {
            embedding_failure()
        }
    })?;
    budget.add(live_edges.len().saturating_mul(2))?;
    let mut incidence_offsets = Vec::new();
    try_reserve_vec(&mut incidence_offsets, subdivision.vertices.len() + 1)?;
    incidence_offsets.resize(subdivision.vertices.len() + 1, 0_usize);
    for &edge_id in &live_edges {
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
    try_reserve_vec(&mut incident_edges, live_edges.len().saturating_mul(2))?;
    incident_edges.resize(live_edges.len().saturating_mul(2), EdgeId(0));
    let mut cursors = copied(&incidence_offsets[..subdivision.vertices.len()])?;
    for &edge_id in &live_edges {
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
    // A closed ring starts at its lexicographically smallest rounded vertex:
    // the canonical form both lanes publish.
    let closed_ring = |vertices: &[VertexId], orientation: ExactSign| -> Result<Ring> {
        let open = vertices.len() - 1;
        let mut coordinates = Vec::new();
        try_reserve_vec(&mut coordinates, vertices.len())?;
        for vertex in &vertices[..open] {
            let embedded = exact_to_embedded[vertex.0 as usize]
                .ok_or_else(|| voronoi_error("non-live exact vertex was rendered"))?;
            let point = points[embedded.0 as usize];
            coordinates.push(Point::new_raw_xy(point.x, point.y));
        }
        let mut rounded = Vec::new();
        try_reserve_vec(&mut rounded, open)?;
        rounded.extend(
            coordinates
                .iter()
                .map(|point| exact::ExactPoint::from_xy(point.xy())),
        );
        if exact::cycle_orientation(&rounded)? != orientation {
            return Err(embedding_failure());
        }
        if open > 2 {
            let start = (0..open)
                .min_by(|&left, &right| lex_xy(coordinates[left].xy(), coordinates[right].xy()))
                .expect("non-empty ring");
            coordinates.rotate_left(start);
        }
        coordinates.push(coordinates[0]);
        Ring::closed(coordinates)
    };
    let mut labelled = Vec::new();
    try_reserve_vec(&mut labelled, parts.len())?;
    for part in parts {
        if walk_label(subdivision, part.face).owner != Some(part.owner) {
            return Err(voronoi_error("embedded cell lost its exact face owner"));
        }
        let shell = closed_ring(&part.shell.vertices, part.shell.orientation)?;
        let holes = try_collect_vec(
            part.holes
                .iter()
                .map(|hole| closed_ring(&hole.vertices, hole.orientation)),
        )?;
        let polygon = Polygon::new(shell, holes)?;
        if !polygon_is_valid(&polygon)? {
            return Err(embedding_failure());
        }
        labelled.push((part.owner, polygon));
    }
    let mut polygons = Vec::new();
    try_reserve_vec(&mut polygons, labelled.len())?;
    polygons.extend(labelled.iter().map(|(_, polygon)| polygon.clone()));
    let embedded_ring = |ring: &[exact::ExactPoint]| -> Result<Ring> {
        let mut coordinates = Vec::new();
        try_reserve_vec(&mut coordinates, ring.len() + 1)?;
        for point in ring {
            let vertex = vertex_at(point)?
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
    let holes = try_collect_vec(clip.rings[1..].iter().map(|ring| embedded_ring(ring)))?;
    let comparison_polygon = Polygon::new(shell, holes)?;
    admit_embedded_clip(&comparison_polygon)?;
    let comparison_clip = Shape::Polygon(comparison_polygon);
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
        let mut shape_rows = Vec::new();
        try_reserve_vec(&mut shape_rows, polygons.len())?;
        shape_rows.extend(polygons.iter().cloned().map(Shape::Polygon));
        if !coverage_is_valid(&shape_rows, 0.0)? {
            return Err(embedding_failure());
        }
        budget.add(shape_rows.len())?;
        let mut row_boxes = Vec::new();
        try_reserve_vec(&mut row_boxes, shape_rows.len())?;
        row_boxes.extend(shape_rows.iter().map(Shape::bounds));
        let row_index = BoxIndex::build(&row_boxes)?;
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
            row_index.for_each_containing(Some(site.point.xy()), |row| {
                let (owner, polygon) = &labelled[row];
                budget.add(
                    2_usize
                        .saturating_add(polygon.shell.len())
                        .saturating_add(polygon.holes.iter().map(|hole| hole.len()).sum::<usize>()),
                )?;
                if Shape::Polygon(polygon.clone()).covers(&point)? {
                    if owner.0 as usize != site_index {
                        return Err(embedding_failure());
                    }
                    own = true;
                }
                Ok(())
            })?;
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
    mesh: &CertifiedDelaunay,
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
    let complex = delaunay_complex(mesh, sites)?;
    let primal_edges = mesh.primal_edges()?;
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
    mesh: &CertifiedDelaunay,
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

fn point_covered_by_clip(
    point: &exact::ExactPoint,
    rings: &[Vec<exact::ExactPoint>],
) -> Result<bool> {
    if exact::point_in_cycle(&rings[0], point)? == exact::PointInCycle::Outside {
        return Ok(false);
    }
    for hole in &rings[1..] {
        if exact::point_in_cycle(hole, point)? == exact::PointInCycle::Inside {
            return Ok(false);
        }
    }
    Ok(true)
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
        return point_covered_by_clip(&start.midpoint(&end)?, exact_rings);
    }
    if !point_covered_by_clip(&start, exact_rings)? || !point_covered_by_clip(&end, exact_rings)? {
        return Ok(false);
    }
    events.clear();
    try_reserve_vec(events, 2)?;
    events.extend([start.clone(), end.clone()]);
    for ring in exact_rings {
        for edge in 0..ring.len() {
            match exact::segment_intersection(
                &start,
                &end,
                &ring[edge],
                &ring[(edge + 1) % ring.len()],
            )? {
                exact::SegmentIntersection::None => {},
                exact::SegmentIntersection::Point(point) => try_push_vec(events, point)?,
                exact::SegmentIntersection::Overlap { start, end } => {
                    try_reserve_vec(events, 2)?;
                    events.extend([start, end]);
                },
            }
        }
    }
    sort_unique_along(&start, &end, events)?;
    for [left, right] in events.array_windows::<2>() {
        if exact::compare_along(&start, &end, left, right)? != Ordering::Less {
            continue;
        }
        if !point_covered_by_clip(&left.midpoint(right)?, exact_rings)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Voronoi cells of collinear sites: the strips between consecutive
/// perpendicular bisectors. Each strip is cut from the clip frame by at most
/// two exact half-planes and rounded once; neighbouring cells round the same
/// exact bisector crossings, so they share their vertices. A polygon clip then
/// intersects each strip with the polygon.
pub(super) fn build_collinear_polygons(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
) -> Result<Vec<Polygon>> {
    let order = mesh
        .collinear_order()
        .ok_or_else(|| voronoi_error("non-collinear sites entered the collinear Voronoi lane"))?;
    if matches!(boundary, VoronoiBoundary::Envelope) {
        let first = sites[0].point;
        if sites.iter().all(|site| site.point.x == first.x)
            || sites.iter().all(|site| site.point.y == first.y)
        {
            return Ok(Vec::new());
        }
    }
    let clip = normalize_clip(sites, boundary, budget)?;
    let frame = match clip.source {
        None => clip.rings[0].clone(),
        Some(polygon) => {
            let extent = |values: &[f64]| {
                values
                    .iter()
                    .fold((f64::INFINITY, f64::NEG_INFINITY), |(low, high), &value| {
                        (low.min(value), high.max(value))
                    })
            };
            let (min_x, max_x) = extent(polygon.shell.xs());
            let (min_y, max_y) = extent(polygon.shell.ys());
            [
                (min_x, min_y),
                (max_x, min_y),
                (max_x, max_y),
                (min_x, max_y),
            ]
            .map(|(x, y)| exact::ExactPoint::from_xy(XY::new(x, y)))
            .to_vec()
        },
    };
    let mut cells = Vec::new();
    for (position, &site) in order.iter().enumerate() {
        let here = sites[site].point.xy();
        let mut ring = frame.clone();
        let neighbours = [
            position.checked_sub(1),
            (position + 1 < order.len()).then_some(position + 1),
        ];
        for neighbour in neighbours.into_iter().flatten() {
            let line =
                exact::ExactLine::perpendicular_bisector(here, sites[order[neighbour]].point.xy())?;
            let keep = exact::signed_line_product(&line, &exact::ExactPoint::from_xy(here))?;
            ring = clip_to_half_plane(&ring, &line, keep, budget)?;
        }
        let mut vertices: Vec<XY> = Vec::new();
        for point in &ring {
            let rounded = point.round_nearest_even()?;
            if vertices.last() != Some(&rounded) {
                budgeted_try_push(&mut vertices, rounded, budget)?;
            }
        }
        while vertices.len() > 1 && vertices.first() == vertices.last() {
            vertices.pop();
        }
        if vertices.len() < 3 {
            continue;
        }
        // Start at the least vertex, as every other emitted cell does.
        let least = (0..vertices.len())
            .min_by(|&left, &right| {
                let (left, right) = (vertices[left], vertices[right]);
                left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
            })
            .unwrap_or(0);
        vertices.rotate_left(least);
        let first = vertices[0];
        budgeted_try_push(&mut vertices, first, budget)?;
        let strip = Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_xy(&vertices)?)?,
            Vec::new(),
        )?;
        match clip.source {
            None => budgeted_try_push(&mut cells, strip, budget)?,
            Some(polygon) => {
                let clipped = Shape::Polygon(strip)
                    .intersection(&Shape::Polygon(polygon.clone()), Strictness::Strict)?;
                for part in clipped.parts() {
                    if let Shape::Polygon(part) = part {
                        budgeted_try_push(&mut cells, part, budget)?;
                    }
                }
            },
        }
    }
    Ok(cells)
}

/// The part of a convex ring on the `keep` side of `line`, closed.
fn clip_to_half_plane(
    ring: &[exact::ExactPoint],
    line: &exact::ExactLine,
    keep: ExactSign,
    budget: &mut ExpansionBudget,
) -> Result<Vec<exact::ExactPoint>> {
    let mut kept = Vec::new();
    for index in 0..ring.len() {
        let start = &ring[index];
        let end = &ring[(index + 1) % ring.len()];
        let start_side = exact::signed_line_product(line, start)?;
        let end_side = exact::signed_line_product(line, end)?;
        if start_side == keep || start_side == ExactSign::Zero {
            budgeted_try_push(&mut kept, start.clone(), budget)?;
        }
        if start_side != ExactSign::Zero && end_side != ExactSign::Zero && start_side != end_side {
            let support = exact::ExactLine::through_points(start, end)?;
            budgeted_try_push(&mut kept, exact::line_intersection(line, &support)?, budget)?;
        }
    }
    Ok(kept)
}

pub(super) fn build_collinear_edges(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
) -> Result<EmbeddedEdges> {
    let order = mesh
        .collinear_order()
        .ok_or_else(|| voronoi_error("non-collinear sites entered the collinear Voronoi lane"))?;
    if matches!(boundary, VoronoiBoundary::Envelope) {
        let extent = |coordinate: fn(&Site) -> f64| {
            let values = sites.iter().map(coordinate);
            (
                values.clone().reduce(f64::min).unwrap(),
                values.reduce(f64::max).unwrap(),
            )
        };
        let (min_x, max_x) = extent(|site| site.point.x);
        let (min_y, max_y) = extent(|site| site.point.y);
        if min_x == max_x || min_y == max_y {
            return empty_packed_edges();
        }
    }
    let clip = normalize_clip(sites, boundary, budget)?;
    let clip_segments = clip.rings.iter().map(Vec::len).sum::<usize>();
    budget.add(order.len().saturating_sub(1).saturating_mul(clip_segments))?;
    let mut exact_chains = Vec::new();
    for &[left, right] in order.array_windows::<2>() {
        let line = exact::ExactLine::perpendicular_bisector(
            sites[left].point.xy(),
            sites[right].point.xy(),
        )?;
        let mut events = Vec::new();
        for ring in &clip.rings {
            for edge in 0..ring.len() {
                let start = &ring[edge];
                let end = &ring[(edge + 1) % ring.len()];
                let start_sign = exact::signed_line_product(&line, start)?;
                let end_sign = exact::signed_line_product(&line, end)?;
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
                    let hit = exact::line_intersection(&line, &support)?;
                    if exact::segment_intersection(start, end, &hit, &hit)?.is_point() {
                        budgeted_try_push(&mut events, hit, budget)?;
                    }
                }
            }
        }
        try_sort_by(&mut events, exact::ExactPoint::compare_lex)?;
        try_dedup_by(&mut events, exact::ExactPoint::same_position)?;
        let mut current: Vec<exact::ExactPoint> = Vec::new();
        for [from, to] in events.array_windows::<2>() {
            budget.add(clip_segments)?;
            if from.same_position(to)? {
                continue;
            }
            if point_covered_by_clip(&from.midpoint(to)?, &clip.rings)? {
                if current.is_empty() {
                    budgeted_try_push(&mut current, from.clone(), budget)?;
                }
                budgeted_try_push(&mut current, to.clone(), budget)?;
            } else if current.len() >= 2 {
                budgeted_try_push(
                    &mut exact_chains,
                    ([left, right], std::mem::take(&mut current)),
                    budget,
                )?;
            } else {
                current.clear();
            }
        }
        if current.len() >= 2 {
            budgeted_try_push(&mut exact_chains, ([left, right], current), budget)?;
        }
    }
    let separator_points = exact_chains
        .iter()
        .map(|(_, chain)| chain.len())
        .sum::<usize>();
    budget.add(separator_points.saturating_mul(clip_segments))?;
    let mut split_rings = Vec::new();
    try_reserve_vec(&mut split_rings, clip.rings.len())?;
    for ring in &clip.rings {
        let mut split_ring = Vec::new();
        for edge in 0..ring.len() {
            let start = &ring[edge];
            let end = &ring[(edge + 1) % ring.len()];
            let mut events = Vec::new();
            budgeted_try_push(&mut events, start.clone(), budget)?;
            for point in exact_chains.iter().flat_map(|(_, chain)| chain) {
                if exact::segment_intersection(start, end, point, point)?.is_point() {
                    budgeted_try_push(&mut events, point.clone(), budget)?;
                }
            }
            sort_unique_along(start, end, &mut events)?;
            for point in events {
                let repeated = match split_ring.last() {
                    Some(known) => point.same_position(known)?,
                    None => false,
                };
                if !repeated {
                    budgeted_try_push(&mut split_ring, point, budget)?;
                }
            }
        }
        budgeted_try_push(&mut split_rings, split_ring, budget)?;
    }
    let mut split_fixed = Vec::new();
    try_reserve_vec(&mut split_fixed, clip.fixed.len())?;
    for ring in &clip.fixed {
        split_fixed.push(copied(ring)?);
    }
    let split_clip = NormalizedClip {
        rings: split_rings,
        fixed: split_fixed,
        source: clip.source,
        contacts: Vec::new(),
    };
    let mut exact_points = Vec::new();
    try_reserve_vec(
        &mut exact_points,
        separator_points.saturating_add(split_clip.rings.iter().map(Vec::len).sum()),
    )?;
    exact_points.extend(
        exact_chains
            .iter()
            .flat_map(|(_, chain)| chain.iter().cloned()),
    );
    exact_points.extend(split_clip.rings.iter().flatten().cloned());
    try_sort_by(&mut exact_points, exact::ExactPoint::compare_lex)?;
    try_dedup_by(&mut exact_points, exact::ExactPoint::same_position)?;
    let fixed = fixed_coordinates(&exact_points, sites, &clip)?;
    let vertex_id = |point: &exact::ExactPoint| -> Result<VertexId> {
        let index = lex_position(&exact_points, point)?.expect("collinear event was interned");
        Ok(VertexId(index as u32))
    };
    let mut edges = Vec::new();
    let mut chains = Vec::new();
    for ([left, right], mut chain) in exact_chains {
        let ids = [SiteId(left as u32), SiteId(right as u32)];
        let separator = [ids[0].min(ids[1]), ids[0].max(ids[1])];
        let (oriented_start, _) = orient_separator(
            chain.first().unwrap().clone(),
            chain.last().unwrap().clone(),
            ids,
            sites,
        )?;
        if !oriented_start.same_position(chain.first().unwrap())? {
            chain.reverse();
        }
        let mut chain_edges = Vec::new();
        let mut chain_vertices = Vec::new();
        try_reserve_vec(&mut chain_edges, chain.len() - 1)?;
        try_reserve_vec(&mut chain_vertices, chain.len())?;
        for point in &chain {
            chain_vertices.push(vertex_id(point)?);
        }
        for &[left, right] in chain_vertices.array_windows::<2>() {
            chain_edges.push(EdgeId(edges.len() as u32));
            try_push_vec(&mut edges, AtomicEdge {
                endpoints: [left, right],
                forward: Transition {
                    frame_delta: 0,
                    clip_delta: 0,
                    owner: OwnerTransition::Boundary {
                        left: Some(ids[0]),
                        right: Some(ids[1]),
                    },
                },
                separator: Some(separator),
                clip_sources: Vec::new(),
                frame_side: None,
            })?;
        }
        try_push_vec(&mut chains, ExactEdgeChain {
            separator,
            vertices: chain_vertices,
            edges: chain_edges,
        })?;
    }
    let mut clip_source = 0_u32;
    for ring in &split_clip.rings {
        for edge in 0..ring.len() {
            let mut clip_sources = Vec::new();
            try_push_vec(&mut clip_sources, ClipSegmentId(clip_source))?;
            budgeted_try_push(
                &mut edges,
                AtomicEdge {
                    endpoints: [
                        vertex_id(&ring[edge])?,
                        vertex_id(&ring[(edge + 1) % ring.len()])?,
                    ],
                    forward: Transition {
                        frame_delta: 0,
                        clip_delta: 1,
                        owner: OwnerTransition::Preserve,
                    },
                    separator: None,
                    clip_sources,
                    frame_side: None,
                },
                budget,
            )?;
            clip_source = clip_source
                .checked_add(1)
                .ok_or_else(|| voronoi_error("too many split clip atoms"))?;
        }
    }
    let mut vertices = Vec::new();
    try_reserve_vec(&mut vertices, exact_points.len())?;
    vertices.extend(
        exact_points
            .into_iter()
            .zip(fixed)
            .map(|(point, fixed_xy)| ExactVertex { point, fixed_xy }),
    );
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
    let Shape::Polygon(polygon) = &embedded.clip else {
        return Err(voronoi_error("collinear Voronoi clip is not a polygon"));
    };
    let mut comparison_rings = Vec::new();
    let mut comparison_rounded_rings = Vec::new();
    try_reserve_vec(&mut comparison_rings, 1 + polygon.holes.len())?;
    try_reserve_vec(&mut comparison_rounded_rings, 1 + polygon.holes.len())?;
    for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
        let mut exact_ring = Vec::new();
        let mut rounded = Vec::new();
        try_reserve_vec(&mut exact_ring, ring.len())?;
        try_reserve_vec(&mut rounded, ring.len())?;
        for point in ring {
            rounded.push(point.xy());
            exact_ring.push(exact::ExactPoint::from_xy(point.xy()));
        }
        comparison_rings.push(exact_ring);
        comparison_rounded_rings.push(rounded);
    }
    let packed = embedded.into_edges();
    let EmbeddedEdges::Packed { coords, offsets } = &packed else {
        return Err(voronoi_error("collinear Voronoi edges are packed chains"));
    };
    let ends = offsets.as_slice();
    let clip_segments = split_clip.rings.iter().map(Vec::len).sum::<usize>();
    let mut events = Vec::new();
    try_reserve_vec(
        &mut events,
        clip_segments.saturating_mul(2).saturating_add(2),
    )?;
    let output_segments = ends
        .array_windows::<2>()
        .map(|&[start, end]| end.saturating_sub(start).saturating_sub(1) as usize)
        .sum::<usize>();
    budget.add(output_segments.saturating_mul(clip_segments))?;
    for &[start, end] in ends.array_windows::<2>() {
        let edge = LineSeq::row_view(
            coords,
            CoordWindow::from_range(start as usize..end as usize, coords.len()),
        );
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
                &certified_delaunay(&sites).unwrap(),
            );
            let mut legacy_budget = ExpansionBudget::new("test", "subnormal legacy");
            let legacy = build(
                &sites,
                VoronoiBoundary::Envelope,
                &mut legacy_budget,
                false,
                &certified_delaunay(&sites).unwrap(),
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
                std::slice::from_ref(&rectangle),
                std::slice::from_ref(&rectangle_xy),
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
                std::slice::from_ref(&rectangle),
                std::slice::from_ref(&rectangle_xy),
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
            (2.949_570_888_116_326e-6, 1.769_742_532_869_795_6e-6),
            (3.097_351_995_393_694_8e-6, 1.858_411_197_236_216_9e-6),
            (3.0e-6, 2.5e-6),
        ];
        let slanted_edge = cycle(&slanted_edge_points);
        let slanted_edge_xy = cycle_xy(&slanted_edge_points);
        let slanted_segment = Segment {
            start: XY::new(2.949_570_888_116_328e-6, 1.769_742_532_869_796_8e-6),
            end: XY::new(3.097_351_995_393_692_7e-6, 1.858_411_197_236_215_6e-6),
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
            &certified_delaunay(&sites).unwrap(),
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
            &certified_delaunay(&sites).unwrap(),
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
        let mut edges: std::collections::BTreeMap<[PointKey; 2], Vec<[XY; 2]>> =
            std::collections::BTreeMap::new();
        for cell in cells {
            let Shape::Polygon(polygon) = cell else {
                panic!("Voronoi cells are polygons");
            };
            for index in 0..polygon.shell.len() - 1 {
                let edge = [
                    polygon.shell.point_at(index).xy(),
                    polygon.shell.point_at(index + 1).xy(),
                ];
                let keys = edge.map(PointKey::new);
                let key = if keys[0] <= keys[1] {
                    keys
                } else {
                    [keys[1], keys[0]]
                };
                edges.entry(key).or_default().push(edge);
            }
        }
        let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for directions in edges.values() {
            match directions.as_slice() {
                [edge] => {
                    let [a, b] = *edge;
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
                    assert_eq!(
                        PointKey::new(first[0]),
                        PointKey::new(second[1]),
                        "internal edge directions disagree"
                    );
                    assert_eq!(
                        PointKey::new(first[1]),
                        PointKey::new(second[0]),
                        "internal edge directions disagree"
                    );
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
        )
        .unwrap();
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
                exact::point_in_cycle(&rounded_ring, &exact::ExactPoint::from_xy(site.point.xy()),)
                    .unwrap(),
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
        let Err(error) = build(
            &sites,
            VoronoiBoundary::Envelope,
            &mut budget,
            true,
            &certified_delaunay(&sites).unwrap(),
        ) else {
            panic!("colliding exact vertices unexpectedly embedded")
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
        let mesh = certified_delaunay(sites).unwrap();
        let complex = delaunay_complex(&mesh, sites).unwrap();
        let primal_edges = mesh.primal_edges().unwrap();
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
            exact::point_in_cycle(&walk_points(subdivision, parent), probe).unwrap()
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
        let mesh = certified_delaunay(sites).unwrap();
        let complex = delaunay_complex(&mesh, sites).unwrap();
        let primal_edges = mesh.primal_edges().unwrap();
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
            )
            .unwrap()
                == ExactSign::Positive
        }) && right.is_none_or(|site| {
            exact::orient_points(
                start,
                end,
                &exact::ExactPoint::from_xy(sites[site.0 as usize].point.xy()),
            )
            .unwrap()
                == ExactSign::Negative
        })
    }

    #[test]
    fn canonical_separator_emitter_keeps_one_sorted_directed_edge_per_dual() {
        let (sites, polygon) = separator_fixture();
        let mut budget = ExpansionBudget::new("test", "canonical separators");
        let clip = normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), &mut budget).unwrap();
        let rect = clip_rectangle(&clip).unwrap();
        let mesh = certified_delaunay(&sites).unwrap();
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let primal_edges = mesh.primal_edges().unwrap();
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
                exact::orient_points(&separator.start, &separator.end, &left).unwrap(),
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
        let mesh = certified_delaunay(&sites).unwrap();
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let separators = emit_clipped_separators(
            &sites,
            &mesh.primal_edges().unwrap(),
            &complex,
            &rect,
            &mut budget,
        )
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
            chord.start.same_position(&bottom[0]).unwrap()
                && chord.end.same_position(&bottom[1]).unwrap()
                || chord.start.same_position(&bottom[1]).unwrap()
                    && chord.end.same_position(&bottom[0]).unwrap()
        );
        let center = complex.center(0);
        assert!(!in_rectangle(&rect, center).unwrap());
        let adjacency =
            certified_site_adjacency(&mesh.primal_edges().unwrap(), sites.len(), &mut budget)
                .unwrap();
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
                .any(|vertex| vertex.point.same_position(center).unwrap())
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
                .midpoint(&atoms.vertices[edge.endpoints[1].0 as usize].point)
                .unwrap();
            nearest_site(&midpoint, &sites).unwrap_err();
        }
        let mut fail_budget = ExpansionBudget::new("test", "frame-only tie");
        let clip =
            normalize_clip(&sites, VoronoiBoundary::Polygon(&polygon), &mut fail_budget).unwrap();
        let rect = clip_rectangle(&clip).unwrap();
        let mesh = certified_delaunay(&sites).unwrap();
        let adjacency =
            certified_site_adjacency(&mesh.primal_edges().unwrap(), sites.len(), &mut fail_budget)
                .unwrap();
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
            &certified_delaunay(&sites).unwrap(),
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
            &certified_delaunay(&sites).unwrap(),
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
        let primal_edges = certified_delaunay(&sites).unwrap().primal_edges().unwrap();
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
        let Err(error) = build_edges(
            &sites,
            VoronoiBoundary::Polygon(&clip),
            &mut budget,
            true,
            &certified_delaunay(&sites).unwrap(),
        ) else {
            panic!("colliding exact vertices unexpectedly embedded on the edges-only path")
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
            &certified_delaunay(&sites).unwrap(),
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
            &certified_delaunay(&sites).unwrap(),
        )
        .unwrap();
        let polygon = Polygon::new(rectangle(0.0, 0.0, step, step), Vec::new()).unwrap();
        let mut polygon_budget = ExpansionBudget::new("test", "units4 polygon");
        build(
            &sites,
            VoronoiBoundary::Polygon(&polygon),
            &mut polygon_budget,
            true,
            &certified_delaunay(&sites).unwrap(),
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
        let primal_edges = certified_delaunay(&sites).unwrap().primal_edges().unwrap();
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
        let primal_edges = certified_delaunay(&sites).unwrap().primal_edges().unwrap();
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
            &certified_delaunay(&sites).unwrap(),
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
        nearest_site(&true_tie, &sites[..2]).unwrap_err();
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
                &certified_delaunay(&sites).unwrap(),
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
            &certified_delaunay(&sites).unwrap(),
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
        )
        .unwrap();
        let clip = Polygon::new(rectangle(-1.0, -1.0, 2.0, 2.0), Vec::new()).unwrap();
        let mut budget = ExpansionBudget::new("test", "Voronoi topology");
        let diagram = build(
            &sites,
            VoronoiBoundary::Polygon(&clip),
            &mut budget,
            true,
            &certified_delaunay(&sites).unwrap(),
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
        let mesh = certified_delaunay(&sites).unwrap();
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
                let point = dual_vertex_point(vertex, &rings.centers);
                if rings.rings.iter().enumerate().any(|(other_index, other)| {
                    other_index != ring_index
                        && other.iter().any(|other_vertex| {
                            point
                                .same_position(dual_vertex_point(other_vertex, &rings.centers))
                                .unwrap()
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
                let point = dual_vertex_point(vertex, &rings.centers);
                let rounded = point.round_nearest_even().unwrap();
                let Some(side) = rounded_boundary_side(rounded, bounds) else {
                    continue;
                };
                if rings.rings.iter().enumerate().any(|(other_index, other)| {
                    other_index != ring_index
                        && other.iter().any(|other_vertex| {
                            point
                                .same_position(dual_vertex_point(other_vertex, &rings.centers))
                                .unwrap()
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
        base.midpoint(&upper).unwrap().midpoint(&base).unwrap()
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
        let mesh = certified_delaunay(&sites).unwrap();
        let complex = delaunay_complex(&mesh, &sites).unwrap();
        let restored =
            dual_build_rings(&sites, &mesh, complex, &clip, &mut construction_budget).unwrap();
        let mut restored_budget = ExpansionBudget::new("test", "restored dual mutation fixture");
        dual_embed(&restored, &clip, &sites, &mut restored_budget).unwrap();

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
        assert!(!duplicate.same_position(&shared_point).unwrap());
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
                    .round_nearest_even()
                    .unwrap();
                let right = dual_vertex_point(&ring[next], &restored.centers)
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
#[cfg_attr(test, derive(Clone))]
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

struct Tagged {
    vertex: DualVertex,
    leaving: EdgeTag,
}

impl Tagged {
    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            vertex: match &self.vertex {
                DualVertex::Center(component) => DualVertex::Center(*component),
                DualVertex::Generated(point) => {
                    DualVertex::Generated(Box::try_new((**point).clone())?)
                },
            },
            leaving: self.leaving,
        })
    }
}

/// The ring's vertices, without their edge tags.
fn untag(ring: Vec<Tagged>) -> Result<Vec<DualVertex>> {
    let mut vertices = Vec::new();
    try_reserve_vec(&mut vertices, ring.len())?;
    vertices.extend(ring.into_iter().map(|tagged| tagged.vertex));
    Ok(vertices)
}

fn reverse_tagged(ring: &mut [Tagged]) {
    // Reversal turns each edge around: the edge leaving a vertex afterwards
    // is the one that used to arrive at it.
    ring.reverse();
    if let Some(first) = ring.first().map(|tagged| tagged.leaving) {
        for index in 0..ring.len() - 1 {
            ring[index].leaving = ring[index + 1].leaving;
        }
        let last = ring.len() - 1;
        ring[last].leaving = first;
    }
}

fn dedup_tagged(ring: &mut Vec<Tagged>, centers: &[exact::ExactPoint]) -> Result<()> {
    let same = |left: &Tagged, right: &Tagged| {
        dual_vertex_point(&left.vertex, centers)
            .same_position(dual_vertex_point(&right.vertex, centers))
    };
    let mut failure = None;
    ring.dedup_by(|later, earlier| {
        let same = failure.is_none()
            && same(later, earlier).unwrap_or_else(|error| {
                failure = Some(error);
                false
            });
        if same {
            earlier.leaving = later.leaving;
        }
        same
    });
    if let Some(error) = failure {
        return Err(error);
    }
    while ring.len() > 1 && same(&ring[0], ring.last().expect("non-empty"))? {
        ring.pop();
    }
    Ok(())
}

#[cfg_attr(test, derive(Clone))]
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
            )
            .unwrap();
            let mut budget = ExpansionBudget::new("test", "coinciding vertices");
            let cells = build(
                &sites,
                VoronoiBoundary::Envelope,
                &mut budget,
                true,
                &certified_delaunay(&sites).unwrap(),
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
        assert!(forward.same_position(&swapped).unwrap());
        assert!(!forward.same_position(&hit([a, b], other_side)).unwrap());
        let center = exact::circumcenter(a, b, XY::new(0.0, 5.0)).unwrap();
        assert!(!forward.same_position(&center).unwrap());
        let center_permuted = exact::circumcenter(XY::new(0.0, 5.0), a, b).unwrap();
        assert!(center.same_position(&center_permuted).unwrap());
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

fn dual_incident_faces(mesh: &CertifiedDelaunay, site_count: usize) -> Result<IncidentFaces> {
    let triangles = mesh.triangles();
    let halfedges = mesh.halfedges();
    let flat = |edge: usize| triangles[edge / 3][edge % 3];
    let mut start_edge = Vec::new();
    try_reserve_vec(&mut start_edge, site_count)?;
    start_edge.resize(site_count, usize::MAX);
    for edge in 0..halfedges.len() {
        let endpoint = flat(dual_next_halfedge(edge));
        if start_edge[endpoint] == usize::MAX || halfedges[edge] == EMPTY {
            start_edge[endpoint] = edge;
        }
    }
    let mut offsets = Vec::new();
    let mut boundaries = Vec::new();
    try_reserve_vec(&mut offsets, site_count + 1)?;
    try_reserve_vec(&mut boundaries, site_count)?;
    offsets.push(0);
    for site in 0..site_count {
        let start = start_edge[site];
        let (length, boundary) = incident_faces_for_site(start, halfedges, None);
        offsets.push(offsets.last().copied().unwrap_or(0) + length);
        boundaries.push(boundary);
    }
    let total = offsets.last().copied().unwrap_or(0);
    let mut faces = Vec::new();
    let mut neighbors = Vec::new();
    try_reserve_vec(&mut faces, total)?;
    try_reserve_vec(&mut neighbors, total)?;
    faces.resize(total, 0);
    neighbors.resize(total, 0);
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
    Ok(IncidentFaces {
        offsets,
        faces,
        neighbors,
        boundaries,
    })
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
    )?;
    for hit in bisector_frame_intersections(bisector_sites, frame)? {
        if exact::orient_points(&left, &right, &hit.1)? != opposite_sign {
            return Ok(hit);
        }
    }
    Err(voronoi_error(
        "certified Voronoi hull ray misses exact frame",
    ))
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
    let generated = |point: &exact::ExactPoint, leaving| -> Result<Tagged> {
        Ok(Tagged {
            vertex: DualVertex::Generated(Box::try_new(point.clone())?),
            leaving,
        })
    };
    let mut ring = Vec::new();
    try_reserve_vec(&mut ring, faces.len() + 6)?;
    ring.push(generated(&entry_ray.1, EdgeTag::Bisector(entry_site))?);
    ring.extend(
        faces
            .iter()
            .zip(neighbors)
            .map(|(&face, &neighbor)| Tagged {
                vertex: DualVertex::Center(complex.component_of_face(face) as u32),
                leaving: EdgeTag::Bisector(neighbor),
            }),
    );
    ring.push(generated(&exit_ray.1, EdgeTag::Axis)?);
    let exit_side = exit_ray.0 as usize;
    let entry_side = entry_ray.0 as usize;
    if exterior {
        let steps = match (exit_side + 4 - entry_side) % 4 {
            0 => 4,
            steps => steps,
        };
        let mut side = exit_side;
        for _ in 0..steps {
            ring.push(generated(&frame[side], EdgeTag::Axis)?);
            side = (side + 3) % 4;
        }
    } else {
        let steps = (entry_side + 4 - exit_side) % 4;
        let mut side = exit_side;
        for _ in 0..steps {
            side = (side + 1) % 4;
            ring.push(generated(&frame[side], EdgeTag::Axis)?);
        }
    }
    dedup_tagged(&mut ring, center_points)?;
    if dual_cycle_orientation(&ring, center_points)? == ExactSign::Negative {
        reverse_tagged(&mut ring);
    }
    Ok(ring)
}

fn dual_vertex_point<'a>(
    vertex: &'a DualVertex,
    centers: &'a [exact::ExactPoint],
) -> &'a exact::ExactPoint {
    match vertex {
        DualVertex::Center(component) => &centers[*component as usize],
        DualVertex::Generated(point) => point,
    }
}

fn dual_cycle_orientation(ring: &[Tagged], centers: &[exact::ExactPoint]) -> Result<ExactSign> {
    if ring.len() < 3 {
        return Err(voronoi_error("exact cycle has fewer than three vertices"));
    }
    let point = |index: usize| dual_vertex_point(&ring[index % ring.len()].vertex, centers);
    for index in 0..ring.len() {
        let sign = exact::orient_points(point(index), point(index + 1), point(index + 2))?;
        if sign != ExactSign::Zero {
            return Ok(sign);
        }
    }
    Err(voronoi_error("exact cycle has zero orientation"))
}

fn dual_point_in_cycle(
    cycle: &[Tagged],
    centers: &[exact::ExactPoint],
    point: &exact::ExactPoint,
) -> Result<exact::PointInCycle> {
    let mut winding = 0_i32;
    for index in 0..cycle.len() {
        let start = dual_vertex_point(&cycle[index].vertex, centers);
        let end = dual_vertex_point(&cycle[(index + 1) % cycle.len()].vertex, centers);
        let orientation = exact::orient_points(start, end, point)?;
        if orientation == ExactSign::Zero && between(point, start, end)? {
            return Ok(exact::PointInCycle::Boundary);
        }
        let start_y = start.compare_y(point)?;
        let end_y = end.compare_y(point)?;
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
    Ok(if winding == 0 {
        exact::PointInCycle::Outside
    } else {
        exact::PointInCycle::Inside
    })
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
    let corners = match rect.each_ref().map(exact::ExactPoint::as_double) {
        [Some(a), Some(b), Some(c), Some(d)] if axis_aligned => Some([a, b, c, d]),
        _ => None,
    };
    let mut current = ring;
    for side in 0..4 {
        if current.is_empty() {
            return Ok(current);
        }
        let (a, b) = (&rect[side], &rect[(side + 1) % 4]);
        let inside = |point: &exact::ExactPoint| -> Result<bool> {
            if !axis_aligned {
                return Ok(exact::orient_points(a, b, point)? != ExactSign::Negative);
            }
            Ok(match side {
                0 => point.compare_y(&rect[0])? != Ordering::Less,
                1 => point.compare_x(&rect[1])? != Ordering::Greater,
                2 => point.compare_y(&rect[2])? != Ordering::Greater,
                _ => point.compare_x(&rect[3])? != Ordering::Less,
            })
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
            let hit = exact::line_intersection(&boundary, &edge)?;
            Ok(hit.is_finite().then_some(hit))
        };
        let mut next = Vec::new();
        try_reserve_vec(&mut next, current.len() + 1)?;
        let mut previous = current.len() - 1;
        let mut previous_in = inside(dual_vertex_point(&current[previous].vertex, centers))?;
        for index in 0..current.len() {
            let this_point = dual_vertex_point(&current[index].vertex, centers);
            let this_in = inside(this_point)?;
            if this_in != previous_in {
                let tag = current[previous].leaving;
                let previous_point = dual_vertex_point(&current[previous].vertex, centers);
                if let Some(hit) = crossing(previous_point, tag, this_point)? {
                    try_push_vec(&mut next, Tagged {
                        vertex: DualVertex::Generated(Box::try_new(hit)?),
                        // Entering: the rest of the original edge follows.
                        // Leaving: the new edge runs along this side.
                        leaving: if this_in { tag } else { EdgeTag::Axis },
                    })?;
                }
            }
            if this_in {
                try_push_vec(&mut next, current[index].try_clone()?)?;
            }
            (previous, previous_in) = (index, this_in);
        }
        dedup_tagged(&mut next, centers)?;
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
    let mut site_points = Vec::new();
    try_reserve_vec(&mut site_points, sites.len())?;
    site_points.extend(
        sites
            .iter()
            .map(|site| exact::ExactPoint::from_xy(site.point.xy())),
    );
    let frame_inputs = || {
        site_points
            .iter()
            .chain(clip.rings.iter().flatten())
            .chain(centers.iter())
    };
    let frame = if let Some(frame) = exact::enclosing_frame_binary64(frame_inputs())? {
        frame
    } else {
        let mut inputs = Vec::new();
        try_reserve_vec(&mut inputs, frame_inputs().count())?;
        inputs.extend(frame_inputs().cloned());
        exact::enclosing_frame(&inputs)?
    };
    let [a, b, c, d] = clip.rings[0].as_slice() else {
        return Err(voronoi_error(
            "certified rectangular clip requires four corners",
        ));
    };
    let rect = [a.clone(), b.clone(), c.clone(), d.clone()];
    // Certified once: the clip really is a positively-oriented axis-aligned
    // rectangle with strictly positive extent. Only then may the half-plane
    // test collapse to a coordinate comparison.
    let axis_aligned = axis_aligned_rectangle(&rect)?;

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
    let incidence = dual_incident_faces(mesh, sites.len())?;
    let mut rings = Vec::new();
    try_reserve_vec(&mut rings, sites.len())?;

    for site in 0..sites.len() {
        let faces = &incidence.faces[incidence.offsets[site]..incidence.offsets[site + 1]];
        let boundary = incidence.boundaries[site];
        budget.add(faces.len().saturating_add(1))?;
        let neighbors = &incidence.neighbors[incidence.offsets[site]..incidence.offsets[site + 1]];
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
            if dual_point_in_cycle(&interior, centers, &site_point)? == exact::PointInCycle::Inside
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
                if dual_point_in_cycle(&exterior, centers, &site_point)?
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
            let mut ring: Vec<Tagged> = Vec::new();
            try_reserve_vec(&mut ring, faces.len() + 6)?;
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
                    let clockwise = dual_cycle_orientation(&ring, centers)? == ExactSign::Negative;
                    walk_clockwise = Some(clockwise);
                    clockwise
                };
                let mut ring = untag(ring)?;
                if clockwise {
                    ring.reverse();
                }
                budget.add(ring.len())?;
                rings.push(ring);
                continue;
            }
            dedup_tagged(&mut ring, centers)?;
            if ring.len() >= 3 && dual_cycle_orientation(&ring, centers)? == ExactSign::Negative {
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
        rings.push(untag(clipped)?);
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
    let total: usize = rings.rings.iter().map(Vec::len).sum();
    let mut center_rounded: Vec<Option<(XY, u32)>> = Vec::new();
    try_reserve_vec(&mut center_rounded, rings.centers.len())?;
    center_rounded.resize(rings.centers.len(), None);
    // Distinct vertices are the centers plus a boundary's worth of generated
    // points; sizing for every ring reference would triple the table.
    let mut interned = try_hash_map_with_capacity(rings.centers.len())?;
    let mut vertex_xy: Vec<XY> = Vec::new();
    try_reserve_vec(&mut vertex_xy, rings.centers.len())?;
    vertex_xy.resize(rings.centers.len(), XY::new(0.0, 0.0));
    fn resolve<'r>(
        vertex: &'r DualVertex,
        centers: &'r [exact::ExactPoint],
        center_rounded: &mut [Option<(XY, u32)>],
        interned: &mut crate::collections::HashMap<PointKey, (&'r exact::ExactPoint, u32)>,
        vertex_xy: &mut Vec<XY>,
    ) -> Result<(XY, u32)> {
        if let DualVertex::Center(component) = *vertex
            && let Some(memo) = center_rounded[component as usize]
        {
            return Ok(memo);
        }
        let point = dual_vertex_point(vertex, centers);
        let mut xy = point.round_nearest_even()?;
        let key = PointKey::new(xy);
        interned
            .try_reserve(1)
            .map_err(|_| ResourceError::Allocation)?;
        let id = match interned.entry(key) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                let &(representative, id) = existing.get();
                if !representative.same_position(point)? {
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
                    try_push_vec(vertex_xy, xy)?;
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
    let mut offsets = Vec::new();
    let mut rounded_all: Vec<XY> = Vec::new();
    let mut ids_all: Vec<u32> = Vec::new();
    try_reserve_vec(&mut offsets, rings.rings.len() + 1)?;
    try_reserve_vec(&mut rounded_all, total)?;
    try_reserve_vec(&mut ids_all, total)?;
    offsets.push(0);
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
    let mut polygons = Vec::new();
    try_reserve_vec(&mut polygons, rings.rings.len())?;
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
        let closed = rounded.iter().chain(std::iter::once(&rounded[0]));
        if !certify_convex_cell(rounded, site_xy) {
            let mut rounded_ring = Vec::new();
            try_reserve_vec(&mut rounded_ring, rounded.len())?;
            rounded_ring.extend(rounded.iter().map(|&xy| exact::ExactPoint::from_xy(xy)));
            if exact::cycle_orientation(&rounded_ring)? != ExactSign::Positive {
                return Err(embedding_failure());
            }
            let mut points = Vec::new();
            try_reserve_vec(&mut points, rounded.len() + 1)?;
            points.extend(closed.clone().map(|xy| Point::new_raw_xy(xy.x, xy.y)));
            if !polygon_is_valid(&Polygon::new(Ring::closed(points)?, Vec::new())?)? {
                return Err(embedding_failure());
            }
            let site = exact::ExactPoint::from_xy(site_xy);
            if exact::point_in_cycle(&rounded_ring, &site)? == exact::PointInCycle::Outside {
                return Err(embedding_failure());
            }
        }
        budget.add(rounded.len())?;
        let mut xs = Vec::new();
        let mut ys = Vec::new();
        crate::boundary::try_reserve_pair(&mut xs, &mut ys, rounded.len() + 1)?;
        xs.extend(rounded.iter().map(|xy| xy.x));
        ys.extend(rounded.iter().map(|xy| xy.y));
        xs.push(rounded[0].x);
        ys.push(rounded[0].y);
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
    let mut directed: Vec<u64> = Vec::new();
    try_reserve_vec(&mut directed, total)?;
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
    // Each interior edge takes two directed entries.
    let mut internal_edges = Vec::new();
    try_reserve_vec(&mut internal_edges, directed.len() / 2)?;
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
        try_push_vec(&mut sides[side], (lo, hi))?;
    }
    for (index, side) in sides.iter_mut().enumerate() {
        side.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
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
    let mut clip_coordinates = Vec::new();
    try_reserve_vec(&mut clip_coordinates, clip.fixed[0].len() + 1)?;
    clip_coordinates.extend(
        clip.fixed[0]
            .iter()
            .chain(std::iter::once(&clip.fixed[0][0]))
            .map(|xy| Point::new_raw_xy(xy.x, xy.y)),
    );
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
    mesh: &CertifiedDelaunay,
) -> Result<EmbeddedVoronoi> {
    let clip = normalize_clip(sites, boundary, budget)?;
    if mesh.collinear_order().is_some() {
        return Err(voronoi_error(
            "collinear sites require the exact one-dimensional lane",
        ));
    }
    let complex = delaunay_complex(mesh, sites)?;
    let rings = dual_build_rings(sites, mesh, complex, &clip, budget)?;
    dual_embed(&rings, &clip, sites, budget)
}
