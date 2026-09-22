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
use crate::error::Result;
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
use crate::{try_push_vec, try_reserve_vec};

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

mod assembly;
mod clip;
mod scaffold;
mod subdivision;

#[cfg(test)]
use assembly::assemble_separator_group;
use assembly::{ExactCellPart, ExactEdgeChain, assemble_cells, assemble_edges};
#[cfg(test)]
use clip::in_rectangle;
use clip::{
    SegmentIntersectionExt as _, axis_aligned_rectangle, between, bisector_frame_intersections,
    clip_hull_ray_to_rectangle, clip_rectangle, clip_segment_to_rectangle, orient_separator,
    sort_unique_along,
};
use scaffold::{
    build_scaffold, emit_clipped_separators, fixed_coordinates, lex_position,
    node_scaffold_and_separators,
};
use subdivision::{
    ExactSubdivision, ExactVertex, build_exact_subdivision, graft_components, selected_cell_label,
    uf_root, walk_label,
};
#[cfg(test)]
use subdivision::{RegionLabel, crossed_label, validate_exact_subdivision};

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
                ends.array_windows::<2>()
                    .map(|&[start, end]| {
                        let range = start as usize..end as usize;
                        LineSeq::try_new(CoordSeq::try_from_vecs(
                            copied(&coords.xs()[range.clone()])?,
                            copied(&coords.ys()[range])?,
                            None,
                            None,
                        )?)
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()
            },
            Self::Segments(segments) => segments
                .into_iter()
                .map(|[a, b]| {
                    LineSeq::try_new(CoordSeq::try_from_vecs(
                        copied(&[a.x, b.x])?,
                        copied(&[a.y, b.y])?,
                        None,
                        None,
                    )?)
                })
                .collect::<std::result::Result<Vec<_>, _>>(),
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
        let holes = part
            .holes
            .iter()
            .map(|hole| closed_ring(&hole.vertices, hole.orientation))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let polygon = Polygon::new(shell, holes);
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
    let holes = clip.rings[1..]
        .iter()
        .map(|ring| embedded_ring(ring))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let comparison_polygon = Polygon::new(shell, holes);
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
        );
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
mod tests;

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
    fn clone_tagged(&self) -> Self {
        Self {
            vertex: match &self.vertex {
                DualVertex::Center(component) => DualVertex::Center(*component),
                DualVertex::Generated(point) => DualVertex::Generated(Box::new((**point).clone())),
            },
            leaving: self.leaving,
        }
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
mod convex_cell_certificate;

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
            vertex: DualVertex::Generated(Box::new(point.clone())),
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
                        vertex: DualVertex::Generated(Box::new(hit)),
                        // Entering: the rest of the original edge follows.
                        // Leaving: the new edge runs along this side.
                        leaving: if this_in { tag } else { EdgeTag::Axis },
                    })?;
                }
            }
            if this_in {
                try_push_vec(&mut next, current[index].clone_tagged())?;
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
        interned.reserve(1);
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
            if !polygon_is_valid(&Polygon::new(Ring::closed(points)?, Vec::new()))? {
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
        polygons.push(Polygon::new(ring, Vec::new()));
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
