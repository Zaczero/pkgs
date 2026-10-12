#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::collections::BTreeSet;

use ahash::{HashMapExt as _, HashSetExt as _};
use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use super::earcut_bridges::BridgeArena;
use super::earcut_sweep::ring_has_contact;
use super::exact::orient2d_normal_components;
use crate::error::Result;
#[cfg(test)]
use crate::geometry::dedup_consecutive_points;
use crate::geometry::exact_arith::ExactSign;
#[cfg(test)]
use crate::geometry::orientation_xy;
use crate::geometry::{
    Bounds, BulkRTree, CoordSeqBuilder, CoordinateAxes, Coordinates, GeometryErrorKind, HasM, HasZ,
    HashMap, HashSet, MOrdinate, Orientation, Point, PointKey, Polygon, Ring, RingWinding, Segment,
    SegmentIndex, Shape, XY, ZOrdinate, open_point_cycle_winding, orientation, point_on_segment,
    ring_contains_interior, same_point, segments_intersect, wrap_index,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EarcutSiteId {
    pub(crate) ring: usize,
    pub(crate) vertex: usize,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EarcutOccurrenceId(pub(crate) usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EarcutCornerId {
    pub(crate) site: EarcutSiteId,
    pub(crate) occurrence: EarcutOccurrenceId,
}

impl EarcutCornerId {
    pub(super) const fn source(ring: usize, vertex: usize, occurrence: usize) -> Self {
        Self {
            site: EarcutSiteId { ring, vertex },
            occurrence: EarcutOccurrenceId(occurrence),
        }
    }

    pub(super) const fn copied(self, occurrence: usize) -> Self {
        Self {
            site: self.site,
            occurrence: EarcutOccurrenceId(occurrence),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct EarcutTriangle {
    pub(crate) face: usize,
    pub(crate) points: [Point; 3],
    pub(crate) corners: [EarcutCornerId; 3],
}

/// Walk a polygon's ear-cut triangles through one caller-owned sink. The
/// generated-output budget lives at that caller, so it can reject before a
/// triangle shape or array row is emitted.
pub(crate) fn earcut_polygon_with(
    polygon: &Polygon,
    emit: &mut impl FnMut(Point, Point, Point) -> Result<()>,
) -> Result<()> {
    earcut_polygon_with_ids(polygon, &mut |triangle| {
        let [a, b, c] = triangle.points;
        emit(a, b, c)
    })
}

/// Emit the ear selection with original ring-site and live occurrence IDs.
/// Bridge copies keep their source site but receive independent occurrences.
pub(crate) fn earcut_polygon_with_ids(
    polygon: &Polygon,
    emit: &mut impl FnMut(EarcutTriangle) -> Result<()>,
) -> Result<()> {
    let mut next_occurrence = 0_usize;
    let (mut ring, mut ring_ids) =
        cleaned_earcut_ring_with_ids(&polygon.shell, false, 0, &mut next_occurrence)?;
    if ring.len() < 3 || open_point_cycle_winding(&ring).is_degenerate() {
        return Ok(());
    }

    let mut holes = Vec::with_capacity(polygon.holes.len());
    let mut hole_ids = Vec::with_capacity(polygon.holes.len());
    for (hole_index, hole) in polygon.holes.iter().enumerate() {
        let (hole, ids) = cleaned_earcut_ring_with_ids(
            hole,
            true,
            hole_index.wrapping_add(1),
            &mut next_occurrence,
        )?;
        if hole.len() < 3 || open_point_cycle_winding(&hole).is_degenerate() {
            return Err(GeometryErrorKind::triangulation("polygon hole degenerates"));
        }
        validate_hole_inside_shell(&ring, &hole)?;
        holes.push(hole);
        hole_ids.push(ids);
    }
    validate_hole_interactions(&ring, &holes)?;
    eliminate_holes_with_ids(&mut ring, &mut ring_ids, &holes, &hole_ids, next_occurrence)?;
    earcut_linked_ring_with_ids(&ring, &ring_ids, emit)
}

#[cfg(test)]
pub(crate) fn cleaned_earcut_ring<C: Coordinates + ?Sized>(
    ring: &C,
    exterior_cw: bool,
) -> Result<Vec<Point>> {
    let mut next_occurrence = 0_usize;
    cleaned_earcut_ring_with_ids(ring, exterior_cw, 0, &mut next_occurrence)
        .map(|(points, _)| points)
}

fn cleaned_earcut_ring_with_ids<C: Coordinates + ?Sized>(
    ring: &C,
    exterior_cw: bool,
    source_ring: usize,
    next_occurrence: &mut usize,
) -> Result<(Vec<Point>, Vec<EarcutCornerId>)> {
    let mut vertices = open_ring(ring);
    let mut identities = source_identities(source_ring, vertices.len(), next_occurrence)?;
    dedup_circular_points_with_ids(&mut vertices, &mut identities);
    strip_collinear_vertices_with_ids(&mut vertices, Some(&mut identities));
    orient_earcut_ring_with_ids(&mut vertices, Some(&mut identities), exterior_cw);
    validate_earcut_ring(&vertices)?;
    Ok((vertices, identities))
}

#[cfg(test)]
pub(crate) fn orient_earcut_ring(vertices: &mut [Point], exterior_cw: bool) {
    orient_earcut_ring_with_ids(vertices, None, exterior_cw);
}

fn orient_earcut_ring_with_ids(
    vertices: &mut [Point],
    identities: Option<&mut Vec<EarcutCornerId>>,
    exterior_cw: bool,
) {
    if let Some(identities) = identities.as_deref() {
        debug_assert_eq!(vertices.len(), identities.len());
    }
    if vertices.len() < 3 {
        return;
    }
    let is_clockwise = open_point_cycle_winding(vertices) == RingWinding::Clockwise;
    if is_clockwise != exterior_cw {
        vertices.reverse();
        if let Some(identities) = identities {
            identities.reverse();
        }
    }
}

fn dedup_circular_points_with_ids(vertices: &mut Vec<Point>, identities: &mut Vec<EarcutCornerId>) {
    debug_assert_eq!(vertices.len(), identities.len());
    let mut write = 0_usize;
    for read in 0..vertices.len() {
        let point = *vertices
            .get(read)
            .expect("read index is within the source ring");
        if write != 0
            && same_point(
                *vertices
                    .get(write.wrapping_sub(1))
                    .expect("a nonzero write index has a preceding point"),
                point,
            )
        {
            continue;
        }
        let identity = *identities
            .get(read)
            .expect("each source point has an identity");
        *vertices
            .get_mut(write)
            .expect("the write index trails the read index") = point;
        *identities
            .get_mut(write)
            .expect("each output point has an identity slot") = identity;
        write = write.wrapping_add(1);
    }
    vertices.truncate(write);
    identities.truncate(write);
    while vertices.len() > 1
        && same_point(
            *vertices.first().expect("checked non-empty"),
            *vertices.last().expect("checked non-empty"),
        )
    {
        vertices.pop();
        identities.pop();
    }
}

#[cfg(test)]
pub(crate) fn strip_collinear_vertices(vertices: &mut Vec<Point>) {
    strip_collinear_vertices_with_ids(vertices, None);
}

fn strip_collinear_vertices_with_ids(
    vertices: &mut Vec<Point>,
    mut identities: Option<&mut Vec<EarcutCornerId>>,
) {
    if vertices.len() < 3 {
        return;
    }
    if let Some(identities) = identities.as_deref() {
        debug_assert_eq!(vertices.len(), identities.len());
    }
    let source_identities = identities.as_deref_mut().map(std::mem::take);
    let is_collinear = |a: Point, b: Point, c: Point| -> bool {
        orientation(a, b, c) == Orientation::Collinear && point_on_segment(b, a, c)
    };
    // O(n) single pass with a write-head stack (was an O(n²) `Vec::remove`
    // loop — quadratic for densified/segmentized rings with many collinear
    // vertices). A collinear-run's interior points collapse to its endpoints,
    // and that fixpoint is unique, so this keeps the EXACT same vertex set:
    // after pushing a vertex, while the last three are collinear, drop the
    // middle by overwriting it with the last and popping (O(1), no shift).
    let mut out: Vec<Point> = Vec::with_capacity(vertices.len());
    let mut out_identities = source_identities
        .as_ref()
        .map(|_| Vec::with_capacity(vertices.len()));
    for (index, &curr) in vertices.iter().enumerate() {
        out.push(curr);
        if let (Some(input_ids), Some(output_ids)) =
            (source_identities.as_ref(), &mut out_identities)
        {
            output_ids.push(
                *input_ids
                    .get(index)
                    .expect("each source point has an identity"),
            );
        }
        loop {
            match out.as_mut_slice() {
                [.., first, middle, last] if is_collinear(*first, *middle, *last) => {
                    *middle = *last;
                    out.pop();
                    if let Some(output_ids) = &mut out_identities {
                        let last_id = *output_ids.last().expect("collinear output has a last ID");
                        let middle = output_ids
                            .len()
                            .checked_sub(2)
                            .expect("collinear output has a middle ID");
                        *output_ids
                            .get_mut(middle)
                            .expect("collinear output has a middle ID") = last_id;
                        output_ids.pop();
                    }
                },
                _ => break,
            }
        }
    }
    // Seam fixup: the ring is closed, so re-check the wrap-around triples
    // (…, last, first) and (last, first, second), collapsing the middle until
    // stable. A simple polygon's seam holds O(1) collinear vertices.
    loop {
        let removes_last = match out.as_slice() {
            [first, .., second_last, last] => is_collinear(*second_last, *last, *first),
            _ => false,
        };
        if removes_last {
            out.pop();
            if let Some(output_ids) = &mut out_identities {
                output_ids.pop();
            }
            continue;
        }
        let removes_first = match out.as_slice() {
            [first, second, .., last] => is_collinear(*last, *first, *second),
            _ => false,
        };
        if removes_first {
            out.remove(0);
            if let Some(output_ids) = &mut out_identities {
                output_ids.remove(0);
            }
        } else {
            break;
        }
    }
    *vertices = out;
    if let (Some(identities), Some(out_identities)) = (identities, out_identities) {
        *identities = out_identities;
    }
}

pub(crate) fn validate_earcut_ring(ring: &[Point]) -> Result<()> {
    if ring.len() < 3 {
        return Ok(());
    }
    let mut seen = HashSet::with_capacity(ring.len());
    for &point in ring {
        if !seen.insert(PointKey::new(point)) {
            return Err(GeometryErrorKind::triangulation(
                "polygon ring self-intersects",
            ));
        }
    }
    let cycle = EarcutCycle::new(ring);
    for (prev, curr, next) in cycle.triples() {
        if orientation(prev, curr, next) == Orientation::Collinear
            && !point_on_segment(curr, prev, next)
        {
            return Err(GeometryErrorKind::triangulation(
                "polygon ring self-intersects",
            ));
        }
    }
    if ring_has_contact(ring) {
        return Err(GeometryErrorKind::triangulation(
            "polygon ring self-intersects",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn ring_has_contact_box_scan(ring: &[Point]) -> bool {
    let segments: Vec<Segment> = EarcutCycle::new(ring)
        .edges()
        .map(|(_, start, end)| ring_segment_points(start, end))
        .collect();
    let index = SegmentIndex::build(&segments);
    for (left, &left_segment) in segments.iter().enumerate() {
        for entry in index.intersecting_candidates(left_segment) {
            let right = entry.ordinal;
            if right <= left || ring_segments_adjacent(ring.len(), left, right) {
                continue;
            }
            let right_segment = segments[right];
            super::earcut_sweep::record_work();
            if segments_intersect(left_segment, right_segment) {
                return true;
            }
        }
    }
    false
}

pub(crate) fn validate_hole_inside_shell(shell: &[Point], hole: &[Point]) -> Result<()> {
    if !ring_contains_interior(
        &closed_ring(shell),
        *hole.first().expect("validated holes are non-empty"),
    ) {
        return Err(GeometryErrorKind::triangulation(
            "polygon hole is not strictly inside the shell",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum EarcutRingId {
    Shell,
    Hole(HoleId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HoleId(usize);

impl HoleId {
    const fn from_index(index: usize) -> Self {
        Self(index)
    }

    const fn index(self) -> usize {
        self.0
    }
}

struct HoleList<'holes> {
    rings: &'holes [Vec<Point>],
}

impl<'holes> HoleList<'holes> {
    const fn new(rings: &'holes [Vec<Point>]) -> Self {
        Self { rings }
    }

    fn ids(&self) -> impl Iterator<Item = HoleId> + '_ {
        (0..self.rings.len()).map(HoleId::from_index)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HoleId is created by enumerating this unchanged hole slice before bridge elimination"
    )]
    fn ring(&self, hole: HoleId) -> &[Point] {
        &self.rings[hole.index()]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RingVertexId(usize);

struct EarcutCycle<'ring> {
    points: &'ring [Point],
}

impl<'ring> EarcutCycle<'ring> {
    const fn new(points: &'ring [Point]) -> Self {
        Self { points }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "RingVertexId comes from enumerating this unchanged ring and is used only before the next splice"
    )]
    const fn point(&self, vertex: RingVertexId) -> Point {
        self.points[vertex.0]
    }

    const fn next(&self, vertex: RingVertexId) -> RingVertexId {
        RingVertexId(wrap_index(vertex.0.wrapping_add(1), self.points.len()))
    }

    #[cfg(test)]
    const fn previous(&self, vertex: RingVertexId) -> RingVertexId {
        let prior = vertex.0.wrapping_add(self.points.len()).wrapping_sub(1);
        RingVertexId(wrap_index(prior, self.points.len()))
    }

    fn edges(&self) -> impl Iterator<Item = (RingVertexId, Point, Point)> + '_ {
        self.points
            .iter()
            .zip(self.points.iter().cycle().skip(1))
            .enumerate()
            .map(|(index, (start, end))| (RingVertexId(index), *start, *end))
    }

    #[cfg(test)]
    fn vertices_from(
        &self,
        start: RingVertexId,
    ) -> impl Iterator<Item = (RingVertexId, Point)> + '_ {
        self.points
            .iter()
            .enumerate()
            .skip(start.0)
            .chain(self.points.iter().enumerate().take(start.0))
            .map(|(index, point)| (RingVertexId(index), *point))
    }

    fn triples(&self) -> impl Iterator<Item = (Point, Point, Point)> + '_ {
        let len = self.points.len();
        let previous = self
            .points
            .iter()
            .rev()
            .take(1)
            .chain(self.points.iter().take(len.wrapping_sub(1)));
        let current = self.points.iter();
        let next = self.points.iter().skip(1).chain(self.points.iter().take(1));
        previous
            .zip(current)
            .zip(next)
            .map(|((previous, current), next)| (*previous, *current, *next))
    }
}

struct RingSegments {
    segments: Vec<Segment>,
    owners: Vec<EarcutRingId>,
}

impl RingSegments {
    const fn new() -> Self {
        Self {
            segments: Vec::new(),
            owners: Vec::new(),
        }
    }

    fn push_ring(&mut self, ring: &[Point], id: EarcutRingId) {
        for (_, start, end) in EarcutCycle::new(ring).edges() {
            self.segments.push(ring_segment_points(start, end));
            self.owners.push(id);
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "SegmentEntry.ordinal was assigned by SegmentIndex from this same paired ring-segment stream"
    )]
    fn candidate(&self, ordinal: usize) -> (Segment, EarcutRingId) {
        (self.segments[ordinal], self.owners[ordinal])
    }
}

fn push_ring_segments(ring: &[Point], id: EarcutRingId, segments: &mut RingSegments) {
    segments.push_ring(ring, id);
}

pub(crate) fn validate_hole_interactions(shell: &[Point], holes: &[Vec<Point>]) -> Result<()> {
    if holes.is_empty() {
        return Ok(());
    }
    let mut segments = RingSegments::new();
    push_ring_segments(shell, EarcutRingId::Shell, &mut segments);
    for (index, hole) in holes.iter().enumerate() {
        push_ring_segments(
            hole,
            EarcutRingId::Hole(HoleId::from_index(index)),
            &mut segments,
        );
    }
    let index = SegmentIndex::build(&segments.segments);
    for (left, (&left_segment, &left_id)) in
        segments.segments.iter().zip(&segments.owners).enumerate()
    {
        for entry in index.intersecting_candidates(left_segment) {
            let right = entry.ordinal;
            if right <= left {
                continue;
            }
            let (right_segment, right_id) = segments.candidate(right);
            if matches!(
                (left_id, right_id),
                (EarcutRingId::Shell, EarcutRingId::Shell)
            ) {
                continue;
            }
            if left_id == right_id {
                continue;
            }
            if !segments_intersect(left_segment, right_segment) {
                continue;
            }
            let intersection = match (left_id, right_id) {
                (EarcutRingId::Shell, EarcutRingId::Hole(_))
                | (EarcutRingId::Hole(_), EarcutRingId::Shell) => {
                    super::RingPairIntersection::ShellHole
                },
                _ => super::RingPairIntersection::HoleHole,
            };
            return Err(super::ring_pair_intersection_error(intersection));
        }
    }
    validate_holes_disjoint(holes)
}

struct HoleContainmentPrep {
    closed: Vec<Point>,
    rep: Point,
    bounds: Bounds,
}

struct HoleContainmentPreps(Vec<HoleContainmentPrep>);

impl HoleContainmentPreps {
    #[expect(
        clippy::indexing_slicing,
        reason = "HoleId comes from the same hole enumeration stored in this containment table"
    )]
    fn get(&self, hole: HoleId) -> &HoleContainmentPrep {
        &self.0[hole.index()]
    }

    fn iter(&self) -> impl Iterator<Item = (HoleId, &HoleContainmentPrep)> + '_ {
        self.0
            .iter()
            .enumerate()
            .map(|(index, prep)| (HoleId::from_index(index), prep))
    }
}

/// Reject nested holes: each pair is tested only when one bbox contains the
/// other's representative vertex (the necessary containment condition).
fn validate_holes_disjoint(holes: &[Vec<Point>]) -> Result<()> {
    if holes.len() < 2 {
        return Ok(());
    }
    let preps = HoleContainmentPreps(
        holes
            .iter()
            .map(|hole| {
                let closed = closed_ring(hole);
                let bounds = Bounds::from_xy_iter(closed.iter().map(Point::xy));
                HoleContainmentPrep {
                    closed,
                    rep: *hole.first().expect("validated holes are non-empty"),
                    bounds,
                }
            })
            .collect(),
    );
    let tree: BulkRTree<GeomWithData<Rectangle<[f64; 2]>, HoleId>> =
        BulkRTree::bulk_load_with_params(
            preps
                .iter()
                .map(|(hole, prep)| {
                    GeomWithData::new(
                        Rectangle::from_corners([prep.bounds.minx(), prep.bounds.miny()], [
                            prep.bounds.maxx(),
                            prep.bounds.maxy(),
                        ]),
                        hole,
                    )
                })
                .collect(),
        );
    for (hole, prep) in preps.iter() {
        let point_envelope = AABB::from_corners([prep.rep.x, prep.rep.y], [prep.rep.x, prep.rep.y]);
        for entry in tree.locate_in_envelope_intersecting(point_envelope) {
            let other = entry.data;
            if other == hole {
                continue;
            }
            let other = preps.get(other);
            if ring_contains_interior(&prep.closed, other.rep)
                || ring_contains_interior(&other.closed, prep.rep)
            {
                return Err(GeometryErrorKind::triangulation(
                    "polygon holes must be disjoint",
                ));
            }
        }
    }
    Ok(())
}

const fn ring_segment_points(start: Point, end: Point) -> Segment {
    Segment {
        start: start.xy(),
        end: end.xy(),
    }
}

pub(super) const fn ring_segments_adjacent(len: usize, left: usize, right: usize) -> bool {
    left.abs_diff(right) == 1 || (left == 0 && right == len.wrapping_sub(1))
}

fn source_identities(
    source_ring: usize,
    vertex_count: usize,
    next_occurrence: &mut usize,
) -> Result<Vec<EarcutCornerId>> {
    let mut identities = Vec::with_capacity(vertex_count);
    for vertex in 0..vertex_count {
        let occurrence = *next_occurrence;
        *next_occurrence = next_occurrence
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        identities.push(EarcutCornerId::source(source_ring, vertex, occurrence));
    }
    Ok(identities)
}

fn eliminate_holes_with_ids(
    ring: &mut Vec<Point>,
    ring_ids: &mut Vec<EarcutCornerId>,
    holes: &[Vec<Point>],
    hole_ids: &[Vec<EarcutCornerId>],
    next_occurrence: usize,
) -> Result<()> {
    if holes.is_empty() {
        return Ok(());
    }
    if ring.len() != ring_ids.len()
        || holes.len() != hole_ids.len()
        || holes
            .iter()
            .zip(hole_ids)
            .any(|(hole, ids)| hole.len() != ids.len())
    {
        return Err(GeometryErrorKind::Invalid(
            "earcut source identities do not match the rings".into(),
        )
        .into());
    }
    let holes = HoleList::new(holes);
    let mut order: Vec<_> = holes.ids().collect();
    order.sort_unstable_by(|&left, &right| {
        compare_hole_leftmost_xy_slope(holes.ring(left), holes.ring(right))
            .then_with(|| left.index().cmp(&right.index()))
    });
    let capacity = holes.rings.iter().fold(ring.len(), |count, hole| {
        count.wrapping_add(hole.len()).wrapping_add(2)
    });
    let mut arena = BridgeArena::new_with_ids(ring, ring_ids, capacity, next_occurrence);
    for (batch, hole_id) in order.into_iter().enumerate() {
        let hole_ring = holes.ring(hole_id);
        let start = leftmost_vertex(hole_ring);
        let hole = rotate_ring(hole_ring, start);
        let hole_identity_ring = hole_ids
            .get(hole_id.index())
            .expect("HoleId belongs to the validated source hole table");
        let hole_identities = rotate_corner_ids(hole_identity_ring, start);
        let bridge = arena
            .find_bridge(*hole.first().expect("validated holes are non-empty"))
            .ok_or_else(|| {
                GeometryErrorKind::triangulation("failed to find a visible bridge for polygon hole")
            })?;
        arena.splice_with_ids(bridge, &hole, &hole_identities, batch.wrapping_add(1));
    }
    let (points, identities) = arena.into_points_and_ids();
    *ring = points;
    *ring_ids = identities;
    Ok(())
}

fn rotate_corner_ids(identities: &[EarcutCornerId], start: RingVertexId) -> Vec<EarcutCornerId> {
    identities
        .iter()
        .skip(start.0)
        .chain(identities.iter().take(start.0))
        .copied()
        .collect()
}

fn compare_hole_leftmost_xy_slope(left: &[Point], right: &[Point]) -> std::cmp::Ordering {
    let left_cycle = EarcutCycle::new(left);
    let right_cycle = EarcutCycle::new(right);
    let left_index = leftmost_vertex(left);
    let right_index = leftmost_vertex(right);
    let left_point = left_cycle.point(left_index);
    let right_point = right_cycle.point(right_index);
    left_point
        .x
        .total_cmp(&right_point.x)
        .then_with(|| left_point.y.total_cmp(&right_point.y))
        .then_with(|| {
            let left_next = left_cycle.point(left_cycle.next(left_index));
            let right_next = right_cycle.point(right_cycle.next(right_index));
            let left_dx = left_next.x - left_point.x;
            let right_dx = right_next.x - right_point.x;
            let left_slope = if left_dx == 0.0 {
                f64::INFINITY
            } else {
                (left_next.y - left_point.y) / left_dx
            };
            let right_slope = if right_dx == 0.0 {
                f64::INFINITY
            } else {
                (right_next.y - right_point.y) / right_dx
            };
            left_slope.total_cmp(&right_slope)
        })
}

fn leftmost_vertex(ring: &[Point]) -> RingVertexId {
    let cycle = EarcutCycle::new(ring);
    ring.iter()
        .enumerate()
        .skip(1)
        .fold(RingVertexId(0), |best, (index, &point)| {
            let current = cycle.point(best);
            if point
                .x
                .total_cmp(&current.x)
                .then_with(|| point.y.total_cmp(&current.y))
                == std::cmp::Ordering::Less
            {
                RingVertexId(index)
            } else {
                best
            }
        })
}

fn rotate_ring(ring: &[Point], start: RingVertexId) -> Vec<Point> {
    ring.iter()
        .skip(start.0)
        .chain(ring.iter().take(start.0))
        .copied()
        .collect()
}

#[cfg(test)]
pub(crate) fn filter_earcut_ring_points(ring: &mut Vec<Point>) {
    dedup_consecutive_points(ring);
    strip_collinear_vertices(ring);
}

#[expect(
    clippy::float_cmp,
    reason = "exact comparison is intentional (sentinel / degenerate / exact-literal check)"
)]
#[cfg(test)]
fn find_hole_bridge(ring: &[Point], hole: &[Point]) -> Option<RingVertexId> {
    let hole_point = *hole.first()?;
    let (hx, hy) = (hole_point.x, hole_point.y);
    let cycle = EarcutCycle::new(ring);
    let mut qx = f64::NEG_INFINITY;
    let mut m = None;

    for (index, a, b) in cycle.edges() {
        if same_point(hole_point, a) {
            return Some(index);
        }
        let next = cycle.next(index);
        if same_point(hole_point, b) {
            return Some(next);
        }
        if a.y >= hy && hy >= b.y && (a.y - b.y) != 0.0 {
            let x = a.x + (hy - a.y) * (b.x - a.x) / (b.y - a.y);
            if x <= hx && x > qx {
                qx = x;
                m = Some(if a.x < b.x { index } else { next });
                if x == hx {
                    return m;
                }
            }
        }
    }

    let mut m = m?;
    if hx == qx {
        return Some(m);
    }

    let (mx, my) = {
        let point = cycle.point(m);
        (point.x, point.y)
    };
    let mut tan_min = f64::INFINITY;
    for (p, pt) in cycle.vertices_from(m) {
        let inside = if hy < my {
            point_in_triangle(hx, hy, mx, my, qx, hy, pt.x, pt.y)
        } else {
            point_in_triangle(qx, hy, mx, my, hx, hy, pt.x, pt.y)
        };
        if hx >= pt.x && pt.x >= mx && hx != pt.x && inside {
            let tan = (hy - pt.y).abs() / (hx - pt.x);
            if locally_inside_for_bridge(&cycle, p, hole_point)
                && (tan < tan_min
                    || (tan == tan_min
                        && (pt.x > cycle.point(m).x
                            || (pt.x == cycle.point(m).x && hole_bridge_sector_ok(&cycle, m, p)))))
            {
                m = p;
                tan_min = tan;
            }
        }
    }
    Some(m)
}

pub(super) fn point_in_triangle(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    cx: f64,
    cy: f64,
    px: f64,
    py: f64,
) -> bool {
    (cx - px) * (ay - py) >= (ax - px) * (cy - py)
        && (ax - px) * (by - py) >= (bx - px) * (ay - py)
        && (bx - px) * (cy - py) >= (cx - px) * (by - py)
}

#[cfg(test)]
fn locally_inside_for_bridge(cycle: &EarcutCycle<'_>, p: RingVertexId, target: Point) -> bool {
    let prev = cycle.point(cycle.previous(p));
    let curr = cycle.point(p);
    let next = cycle.point(cycle.next(p));
    let (tx, ty) = (target.x, target.y);
    if orientation_xy(prev.x, prev.y, curr.x, curr.y, next.x, next.y)
        == Orientation::CounterClockwise
    {
        orientation_xy(curr.x, curr.y, tx, ty, next.x, next.y) != Orientation::CounterClockwise
            && orientation_xy(curr.x, curr.y, prev.x, prev.y, tx, ty)
                != Orientation::CounterClockwise
    } else {
        orientation_xy(curr.x, curr.y, tx, ty, prev.x, prev.y) == Orientation::CounterClockwise
            || orientation_xy(curr.x, curr.y, next.x, next.y, tx, ty)
                == Orientation::CounterClockwise
    }
}

#[cfg(test)]
fn hole_bridge_sector_ok(cycle: &EarcutCycle<'_>, m: RingVertexId, p: RingVertexId) -> bool {
    let m_prev = cycle.point(cycle.previous(m));
    let m_curr = cycle.point(m);
    let m_next = cycle.point(cycle.next(m));
    let p_prev = cycle.point(cycle.previous(p));
    let p_next = cycle.point(cycle.next(p));
    orientation_xy(m_prev.x, m_prev.y, m_curr.x, m_curr.y, p_prev.x, p_prev.y)
        == Orientation::CounterClockwise
        && orientation_xy(p_next.x, p_next.y, m_curr.x, m_curr.y, m_next.x, m_next.y)
            == Orientation::CounterClockwise
}

#[cfg(test)]
fn splice_hole_bridge(ring: &mut Vec<Point>, bridge: RingVertexId, hole: &[Point]) {
    let bridge_point = EarcutCycle::new(ring).point(bridge);
    let hole_point = *hole.first().expect("validated holes are non-empty");
    let head_len = bridge.0.wrapping_add(1);
    let capacity = ring.len().wrapping_add(hole.len()).wrapping_add(2);
    let mut spliced = Vec::with_capacity(capacity);
    spliced.extend(ring.iter().take(head_len).copied());
    spliced.extend_from_slice(hole);
    spliced.push(hole_point);
    spliced.push(bridge_point);
    spliced.extend(ring.iter().skip(head_len).copied());
    *ring = spliced;
}

#[cfg(test)]
mod earcut_tests {
    use super::*;

    #[test]
    fn indexed_hole_bridges_preserve_linear_selection_and_filtering() {
        for (count, offset, scale) in [
            (1_usize, 0.0, 1.0),
            (2, 0.0, 1.0),
            (4, 0.0, 1.0),
            (8, 0.0, 1.0),
            (16, 0.0, 1.0),
            (32, 0.0, 1.0),
            (64, 0.0, 1.0),
            (8, 1.0e12, 0.125),
            (8, 0.0, 1.0e-170),
            (8, 0.0, 1.0e150),
        ] {
            let width = (count as f64).sqrt().ceil() as usize;
            let side = width as f64 + 1.0;
            let mut shell = vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(side, 0.0),
                Point::new_raw_xy(side, side),
                Point::new_raw_xy(0.0, side),
            ];
            let mut holes = Vec::new();
            for index in 0..count {
                let x = (index % width) as f64;
                let y = (index / width) as f64;
                let mut hole = vec![
                    Point::new_raw_xy(x + 0.2, y + 0.2),
                    Point::new_raw_xy(x + 0.8, y + 0.2),
                    Point::new_raw_xy(x + 0.8, y + 0.8),
                    Point::new_raw_xy(x + 0.2, y + 0.8),
                ];
                orient_earcut_ring(&mut hole, true);
                holes.push(hole);
            }
            for point in shell.iter_mut().chain(holes.iter_mut().flatten()) {
                point.x = offset + point.x * scale;
                point.y = offset + point.y * scale;
            }
            holes.reverse();
            let mut reference = shell.clone();
            let mut indexed =
                BridgeArena::new(&shell, shell.len().wrapping_add(count.wrapping_mul(6)));
            let mut order: Vec<_> = holes.iter().enumerate().collect();
            order.sort_unstable_by(|(li, left), (ri, right)| {
                compare_hole_leftmost_xy_slope(left, right).then_with(|| li.cmp(ri))
            });
            for (batch, (_, hole)) in order.into_iter().enumerate() {
                let hole = rotate_ring(hole, leftmost_vertex(hole));
                let bridge = find_hole_bridge(&reference, &hole).expect("visible reference bridge");
                let walk = indexed.filtered_walk();
                assert_eq!(
                    indexed.find_bridge(*hole.first().expect("nonempty hole")),
                    walk.get(bridge.0).map(|(id, _)| *id),
                    "{count} holes, bridge {batch}"
                );
                splice_hole_bridge(&mut reference, bridge, &hole);
                filter_earcut_ring_points(&mut reference);
                indexed.splice(
                    walk.get(bridge.0).expect("reference bridge").0,
                    &hole,
                    batch.wrapping_add(1),
                );
                assert_eq!(
                    indexed
                        .filtered_walk()
                        .into_iter()
                        .map(|(_, point)| point)
                        .collect::<Vec<_>>(),
                    reference,
                    "{count} holes, filtered splice {batch}"
                );
            }
        }
    }

    const INDEXED_EAR_NEIGHBOR_RING: [Point; 68] = [
        Point::new_raw_xy(-3.0, 1.0),
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(2.0, -1.0),
        Point::new_raw_xy(4.0, 0.0),
        Point::new_raw_xy(7.0, 1.0),
        Point::new_raw_xy(7.0, 2.0),
        Point::new_raw_xy(7.0, 3.0),
        Point::new_raw_xy(7.0, 4.0),
        Point::new_raw_xy(7.0, 5.0),
        Point::new_raw_xy(7.0, 6.0),
        Point::new_raw_xy(7.0, 7.0),
        Point::new_raw_xy(7.0, 8.0),
        Point::new_raw_xy(7.0, 9.0),
        Point::new_raw_xy(7.0, 10.0),
        Point::new_raw_xy(7.0, 11.0),
        Point::new_raw_xy(7.0, 12.0),
        Point::new_raw_xy(7.0, 13.0),
        Point::new_raw_xy(7.0, 14.0),
        Point::new_raw_xy(7.0, 15.0),
        Point::new_raw_xy(7.0, 16.0),
        Point::new_raw_xy(7.0, 17.0),
        Point::new_raw_xy(7.0, 18.0),
        Point::new_raw_xy(7.0, 19.0),
        Point::new_raw_xy(7.0, 20.0),
        Point::new_raw_xy(7.0, 21.0),
        Point::new_raw_xy(7.0, 22.0),
        Point::new_raw_xy(7.0, 23.0),
        Point::new_raw_xy(7.0, 24.0),
        Point::new_raw_xy(7.0, 25.0),
        Point::new_raw_xy(7.0, 26.0),
        Point::new_raw_xy(7.0, 27.0),
        Point::new_raw_xy(7.0, 28.0),
        Point::new_raw_xy(7.0, 29.0),
        Point::new_raw_xy(7.0, 30.0),
        Point::new_raw_xy(7.0, 31.0),
        Point::new_raw_xy(7.0, 32.0),
        Point::new_raw_xy(7.0, 33.0),
        Point::new_raw_xy(7.0, 34.0),
        Point::new_raw_xy(7.0, 35.0),
        Point::new_raw_xy(7.0, 36.0),
        Point::new_raw_xy(7.0, 37.0),
        Point::new_raw_xy(7.0, 38.0),
        Point::new_raw_xy(7.0, 39.0),
        Point::new_raw_xy(7.0, 40.0),
        Point::new_raw_xy(7.0, 41.0),
        Point::new_raw_xy(7.0, 42.0),
        Point::new_raw_xy(7.0, 43.0),
        Point::new_raw_xy(7.0, 44.0),
        Point::new_raw_xy(7.0, 45.0),
        Point::new_raw_xy(7.0, 46.0),
        Point::new_raw_xy(7.0, 47.0),
        Point::new_raw_xy(7.0, 48.0),
        Point::new_raw_xy(7.0, 49.0),
        Point::new_raw_xy(7.0, 50.0),
        Point::new_raw_xy(7.0, 51.0),
        Point::new_raw_xy(7.0, 52.0),
        Point::new_raw_xy(7.0, 53.0),
        Point::new_raw_xy(7.0, 54.0),
        Point::new_raw_xy(7.0, 55.0),
        Point::new_raw_xy(7.0, 56.0),
        Point::new_raw_xy(7.0, 57.0),
        Point::new_raw_xy(7.0, 58.0),
        Point::new_raw_xy(7.0, 59.0),
        Point::new_raw_xy(7.0, 60.0),
        Point::new_raw_xy(7.0, 61.0),
        Point::new_raw_xy(7.0, 62.0),
        Point::new_raw_xy(7.0, 64.0),
        Point::new_raw_xy(-3.0, 64.0),
    ];

    #[test]
    fn indexed_ear_removal_updates_both_reclassified_neighbors() {
        let mut arena = EarArena::new(&INDEXED_EAR_NEIGHBOR_RING);
        let convex_vertices =
            |tree: &EarCandidateTree| tree.convex.iter().map(|id| id.index()).collect::<Vec<_>>();

        let candidates = arena
            .candidates
            .as_ref()
            .expect("68 vertices use the index");
        assert!(is_ear(&arena, NodeId::from_index(2)));
        assert!(candidates.reflex[1]);
        assert!(candidates.reflex[3]);
        assert_eq!(candidates.nodes[0].live, 63);
        assert_eq!(candidates.nodes[candidates.leaves[1]].live, 1);
        assert_eq!(candidates.nodes[candidates.leaves[3]].live, 1);
        assert_eq!(convex_vertices(candidates), [0, 2, 4, 66, 67]);

        arena.remove(NodeId::from_index(2));

        let candidates = arena
            .candidates
            .as_ref()
            .expect("indexed ring stays indexed");
        assert_eq!(arena.active, 67);
        assert_eq!(
            arena.node(NodeId::from_index(1)).next,
            NodeId::from_index(3)
        );
        assert_eq!(
            arena.node(NodeId::from_index(3)).prev,
            NodeId::from_index(1)
        );
        assert!(!candidates.reflex[1]);
        assert!(!candidates.reflex[3]);
        assert_eq!(candidates.nodes[0].live, 61);
        assert_eq!(candidates.nodes[candidates.leaves[1]].live, 0);
        assert_eq!(candidates.nodes[candidates.leaves[3]].live, 0);
        assert_eq!(convex_vertices(candidates), [0, 1, 3, 4, 66, 67]);
    }
}

#[derive(Clone, Copy)]
struct EarNode {
    point: Point,
    identity: EarcutCornerId,
    prev: NodeId,
    next: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct NodeId(usize);

impl NodeId {
    const fn from_index(index: usize) -> Self {
        Self(index)
    }

    const fn index(self) -> usize {
        self.0
    }
}

struct EarArena {
    nodes: Vec<EarNode>,
    active: usize,
    candidates: Option<EarCandidateTree>,
}

impl EarArena {
    #[cfg(test)]
    fn new(points: &[Point]) -> Self {
        let identities = points
            .iter()
            .enumerate()
            .map(|(index, _)| EarcutCornerId::source(0, index, index))
            .collect::<Vec<_>>();
        Self::new_with_ids(points, &identities)
    }

    fn new_with_ids(points: &[Point], identities: &[EarcutCornerId]) -> Self {
        debug_assert_eq!(points.len(), identities.len());
        let nodes: Vec<EarNode> = points
            .iter()
            .copied()
            .enumerate()
            .map(|(index, point)| EarNode {
                point,
                identity: *identities
                    .get(index)
                    .expect("each Earcut point has an identity"),
                prev: NodeId::from_index(wrap_index(
                    index.wrapping_add(points.len()).wrapping_sub(1),
                    points.len(),
                )),
                next: NodeId::from_index(wrap_index(index.wrapping_add(1), points.len())),
            })
            .collect();
        let candidates = (points.len() > 64).then(|| EarCandidateTree::new(points));
        Self {
            nodes,
            active: points.len(),
            candidates,
        }
    }

    fn node(&self, id: NodeId) -> &EarNode {
        arena_node(&self.nodes, id)
    }

    fn node_mut(&mut self, id: NodeId) -> &mut EarNode {
        arena_node_mut(&mut self.nodes, id)
    }

    fn remove(&mut self, id: NodeId) {
        let (prev, next) = {
            let node = self.node(id);
            (node.prev, node.next)
        };
        self.node_mut(prev).next = next;
        self.node_mut(next).prev = prev;
        let (nodes, candidates) = (&self.nodes, &mut self.candidates);
        if let Some(candidates) = candidates {
            candidates.convex.remove(&id);
            for neighbor in [prev, next] {
                let node = arena_node(nodes, neighbor);
                let reflex = orientation(
                    arena_node(nodes, node.prev).point,
                    node.point,
                    arena_node(nodes, node.next).point,
                ) != Orientation::CounterClockwise;
                candidates.set_reflex(neighbor, reflex);
            }
        }
        self.active = self.active.wrapping_sub(1);
    }
}

enum EarCandidateBranch {
    Vertex(NodeId),
    Children([usize; 2]),
}

struct EarCandidateNode {
    bounds: Bounds,
    parent: Option<usize>,
    live: usize,
    branch: EarCandidateBranch,
}

struct EarCandidateTree {
    // An ear obstruction has a reflex or collinear witness. Subtree counts
    // and boxes track those corners; cuts update the two surviving neighbors.
    nodes: Vec<EarCandidateNode>,
    // Bridge copies at one XY share a geometric witness but retain distinct
    // corner orientations in the ring.
    leaves: Vec<usize>,
    reflex: Vec<bool>,
    // Removal preserves original ring ordinal order, including wraparound.
    convex: BTreeSet<NodeId>,
    normal_arithmetic: bool,
}

#[expect(
    clippy::indexing_slicing,
    reason = "candidate node indices and leaf addresses are created together by the balanced tree builder"
)]
impl EarCandidateTree {
    fn new(points: &[Point]) -> Self {
        const MIN_COORDINATE: f64 = f64::from_bits((1023 - 200) << 52);
        const MAX_COORDINATE: f64 = f64::from_bits((1023 + 200) << 52);

        fn build(
            tree: &mut EarCandidateTree,
            points: &[Point],
            live: &[usize],
            order: &mut [NodeId],
            parent: Option<usize>,
        ) -> usize {
            let bounds = Bounds::from_xy_iter(order.iter().map(|id| points[id.index()].xy()));
            let index = tree.nodes.len();
            tree.nodes.push(EarCandidateNode {
                bounds,
                parent,
                live: order.len(),
                branch: EarCandidateBranch::Vertex(order[0]),
            });
            if order.len() == 1 {
                let vertex = order[0].index();
                tree.leaves[vertex] = index;
                tree.nodes[index].live = live[vertex];
            } else {
                let split = order.len() / 2;
                let x_axis = bounds.maxx() - bounds.minx() >= bounds.maxy() - bounds.miny();
                order.select_nth_unstable_by(split, |left, right| {
                    let l = points[left.index()];
                    let r = points[right.index()];
                    (if x_axis {
                        l.x.total_cmp(&r.x)
                    } else {
                        l.y.total_cmp(&r.y)
                    })
                    .then_with(|| left.index().cmp(&right.index()))
                });
                let (left, right) = order.split_at_mut(split);
                let left = build(tree, points, live, left, Some(index));
                let right = build(tree, points, live, right, Some(index));
                tree.nodes[index].branch = EarCandidateBranch::Children([left, right]);
                tree.refresh(index);
            }
            index
        }
        let mut representatives = HashMap::with_capacity(points.len());
        let mut order = Vec::with_capacity(points.len());
        let mut aliases = Vec::with_capacity(points.len());
        let mut live = vec![0_usize; points.len()];
        let mut reflex = Vec::with_capacity(points.len());
        // Such stored ordinates lie on the 2^-252 lattice, so any nonzero
        // rounded difference is at least 2^-252 and at most 2^201. Box
        // extrema select stored ordinates and inherit the same certificate.
        let normal_arithmetic = points.iter().all(|point| {
            [point.x, point.y].iter().all(|value| {
                *value == 0.0 || (MIN_COORDINATE..=MAX_COORDINATE).contains(&value.abs())
            })
        });
        for (index, &point) in points.iter().enumerate() {
            let vertex = NodeId::from_index(index);
            let representative =
                *representatives
                    .entry(PointKey::new(point))
                    .or_insert_with(|| {
                        order.push(vertex);
                        vertex
                    });
            aliases.push(representative);
            let previous = wrap_index(
                index.wrapping_add(points.len()).wrapping_sub(1),
                points.len(),
            );
            let next = wrap_index(index.wrapping_add(1), points.len());
            let is_reflex =
                orientation(points[previous], point, points[next]) != Orientation::CounterClockwise;
            reflex.push(is_reflex);
            live[representative.index()] =
                live[representative.index()].wrapping_add(usize::from(is_reflex));
        }
        let convex = reflex
            .iter()
            .enumerate()
            .filter_map(|(index, &reflex)| (!reflex).then_some(NodeId::from_index(index)))
            .collect();
        let mut tree = Self {
            nodes: Vec::with_capacity(order.len().wrapping_mul(2).wrapping_sub(1)),
            leaves: vec![0; points.len()],
            reflex,
            convex,
            normal_arithmetic,
        };
        build(&mut tree, points, &live, &mut order, None);
        tree.leaves = aliases
            .iter()
            .map(|vertex| tree.leaves[vertex.index()])
            .collect();
        tree
    }

    fn is_reflex(&self, vertex: NodeId) -> bool {
        self.reflex[vertex.index()]
    }

    fn next_convex(&self, vertex: NodeId) -> Option<NodeId> {
        self.convex
            .range(vertex..)
            .next()
            .or_else(|| self.convex.first())
            .copied()
    }

    fn refresh(&mut self, index: usize) {
        let EarCandidateBranch::Children([left, right]) = self.nodes[index].branch else {
            unreachable!("only internal candidate nodes are refreshed");
        };
        let left = &self.nodes[left];
        let right = &self.nodes[right];
        let live = left.live.wrapping_add(right.live);
        let bounds = if left.live == 0 {
            right.bounds
        } else if right.live == 0 {
            left.bounds
        } else {
            let mut bounds = left.bounds;
            bounds.include_bounds(right.bounds);
            bounds
        };
        self.nodes[index].live = live;
        self.nodes[index].bounds = bounds;
    }

    fn set_reflex(&mut self, vertex: NodeId, reflex: bool) {
        if self.is_reflex(vertex) == reflex {
            return;
        }
        let leaf = self.leaves[vertex.index()];
        self.reflex[vertex.index()] = reflex;
        if reflex {
            self.convex.remove(&vertex);
        } else {
            self.convex.insert(vertex);
        }
        self.nodes[leaf].live = if reflex {
            self.nodes[leaf].live.wrapping_add(1)
        } else {
            self.nodes[leaf].live.wrapping_sub(1)
        };
        let mut cursor = self.nodes[leaf].parent;
        while let Some(index) = cursor {
            self.refresh(index);
            cursor = self.nodes[index].parent;
        }
    }

    fn any_in(
        &self,
        bounds: Bounds,
        triangle: [XY; 3],
        mut predicate: impl FnMut(NodeId) -> bool,
    ) -> bool {
        fn visit(
            tree: &EarCandidateTree,
            index: usize,
            bounds: Bounds,
            triangle: &[XY; 3],
            predicate: &mut impl FnMut(NodeId) -> bool,
        ) -> bool {
            #[cfg(test)]
            EAR_CANDIDATE_VISITS.with(|count| count.set(count.get().wrapping_add(1)));
            let node = &tree.nodes[index];
            if node.live == 0 || !node.bounds.intersects(bounds) {
                return false;
            }
            match node.branch {
                EarCandidateBranch::Vertex(vertex) => predicate(vertex),
                EarCandidateBranch::Children([left, right]) => {
                    if tree.normal_arithmetic {
                        for (a, b) in triangle.iter().zip(triangle.iter().cycle().skip(1)) {
                            // Maximize the edge determinant over this box.
                            // A certified negative maximum excludes it; zero
                            // and uncertain signs preserve boundary witnesses.
                            let corner = XY::new(
                                if b.y < a.y {
                                    node.bounds.maxx()
                                } else {
                                    node.bounds.minx()
                                },
                                if b.x > a.x {
                                    node.bounds.maxy()
                                } else {
                                    node.bounds.miny()
                                },
                            );
                            if orient2d_normal_components(
                                a.x - corner.x,
                                a.y - corner.y,
                                b.x - corner.x,
                                b.y - corner.y,
                            ) == Some(ExactSign::Negative)
                            {
                                return false;
                            }
                        }
                    }
                    visit(tree, left, bounds, triangle, predicate)
                        || visit(tree, right, bounds, triangle, predicate)
                },
            }
        }
        visit(self, 0, bounds, &triangle, &mut predicate)
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "NodeId is enumerated from the same fixed EarArena node allocation"
)]
const fn arena_node(nodes: &[EarNode], id: NodeId) -> &EarNode {
    &nodes[id.index()]
}

#[expect(
    clippy::indexing_slicing,
    reason = "NodeId is enumerated from the same fixed EarArena node allocation"
)]
const fn arena_node_mut(nodes: &mut [EarNode], id: NodeId) -> &mut EarNode {
    &mut nodes[id.index()]
}

pub(crate) fn earcut_linked_ring_with_ids(
    points: &[Point],
    identities: &[EarcutCornerId],
    emit: &mut impl FnMut(EarcutTriangle) -> Result<()>,
) -> Result<()> {
    if points.len() < 3 {
        return Ok(());
    }
    if points.len() != identities.len() {
        return Err(GeometryErrorKind::Invalid(
            "earcut site identities do not match the ring".into(),
        )
        .into());
    }
    let mut arena = EarArena::new_with_ids(points, identities);
    let mut current = NodeId::from_index(0);
    let mut guard = 0_usize;
    let mut selected_face = 0_usize;
    while arena.active > 3 {
        if let Some(candidates) = &arena.candidates
            && candidates.is_reflex(current)
        {
            current = candidates.next_convex(current).unwrap_or(current);
        }
        if is_ear(&arena, current) {
            let (prev, next) = {
                let node = arena.node(current);
                (node.prev, node.next)
            };
            let nodes = [arena.node(prev), arena.node(current), arena.node(next)];
            emit(EarcutTriangle {
                face: selected_face,
                points: nodes.map(|node| node.point),
                corners: nodes.map(|node| node.identity),
            })?;
            selected_face = selected_face.wrapping_add(1);
            arena.remove(current);
            current = arena.node(next).next;
            guard = 0;
            continue;
        }
        current = arena.node(current).next;
        guard = guard.wrapping_add(1);
        if guard > arena.nodes.len() {
            return Err(GeometryErrorKind::triangulation(
                "earcut failed to find a valid ear",
            ));
        }
    }
    if arena.active == 3 {
        let first = current;
        let second = arena.node(first).next;
        let third = arena.node(second).next;
        let nodes = [arena.node(first), arena.node(second), arena.node(third)];
        emit(EarcutTriangle {
            face: selected_face,
            points: nodes.map(|node| node.point),
            corners: nodes.map(|node| node.identity),
        })?;
    }
    Ok(())
}

fn is_ear(arena: &EarArena, index: NodeId) -> bool {
    #[cfg(test)]
    EAR_SCAN_STEPS.with(|count| count.set(count.get().wrapping_add(1)));
    let (prev, next) = {
        let node = arena.node(index);
        (node.prev, node.next)
    };
    let (a, b, c) = (
        arena.node(prev).point,
        arena.node(index).point,
        arena.node(next).point,
    );
    let reflex = arena.candidates.as_ref().map_or_else(
        || orientation(a, b, c) != Orientation::CounterClockwise,
        |tree| tree.is_reflex(index),
    );
    if reflex {
        return false;
    }
    !ear_contains_vertex(arena, prev, index, next, a, b, c)
}

fn ear_contains_vertex(
    arena: &EarArena,
    prev: NodeId,
    current: NodeId,
    next: NodeId,
    a: Point,
    b: Point,
    c: Point,
) -> bool {
    let minx = a.x.min(b.x).min(c.x);
    let miny = a.y.min(b.y).min(c.y);
    let maxx = a.x.max(b.x).max(c.x);
    let maxy = a.y.max(b.y).max(c.y);
    if let Some(candidates) = &arena.candidates {
        let bounds = Bounds::new_raw(minx, miny, maxx, maxy);
        return candidates.any_in(bounds, [a.xy(), b.xy(), c.xy()], |candidate| {
            candidate_inside_ear(
                candidate,
                prev,
                current,
                next,
                arena.node(candidate).point,
                a,
                b,
                c,
            )
        });
    }
    let mut candidate = arena.node(next).next;
    while candidate != prev {
        let point = arena.node(candidate).point;
        if point.x >= minx
            && point.x <= maxx
            && point.y >= miny
            && point.y <= maxy
            && candidate_inside_ear(candidate, prev, current, next, point, a, b, c)
        {
            return true;
        }
        candidate = arena.node(candidate).next;
    }
    false
}

fn candidate_inside_ear(
    candidate: NodeId,
    prev: NodeId,
    current: NodeId,
    next: NodeId,
    point: Point,
    a: Point,
    b: Point,
    c: Point,
) -> bool {
    #[cfg(test)]
    EAR_CANDIDATE_VISITS.with(|count| count.set(count.get().wrapping_add(1)));
    candidate != prev
        && candidate != current
        && candidate != next
        && !same_point(point, a)
        && !same_point(point, b)
        && !same_point(point, c)
        && point_in_ccw_triangle(point, a, b, c)
}

#[cfg(test)]
thread_local! {
    static EAR_CANDIDATE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static EAR_SCAN_STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn count_ear_candidates<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    EAR_CANDIDATE_VISITS.with(|count| count.set(0));
    EAR_SCAN_STEPS.with(|count| count.set(0));
    let result = operation();
    (result, EAR_CANDIDATE_VISITS.with(std::cell::Cell::get))
}

#[cfg(test)]
pub(super) fn ear_scan_steps() -> usize {
    EAR_SCAN_STEPS.with(std::cell::Cell::get)
}

pub(crate) fn open_ring<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    let len = points.coord_count();
    let closed = len > 1 && same_point(points.nth_coord(0), points.nth_coord(len.wrapping_sub(1)));
    let keep = if closed { len.wrapping_sub(1) } else { len };
    points.iter_coords().take(keep).collect()
}

pub(crate) fn closed_ring<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    let mut ring = points.iter_coords().collect::<Vec<_>>();
    if let Some(first) = ring.first().copied() {
        ring.push(first);
    }
    ring
}

/// Closed ear-clip triangle whose axes are the intersection of the three
/// source corners — drop an unshared ordinate, never invent one.
pub(crate) fn try_triangle_shape(a: Point, b: Point, c: Point) -> Result<Shape> {
    let has_z = a.axes.has_z() && b.axes.has_z() && c.axes.has_z();
    let has_m = a.axes.has_m() && b.axes.has_m() && c.axes.has_m();
    let coerce = |point: Point| {
        Point::new_raw_axes(
            point.x,
            point.y,
            ZOrdinate(has_z.then_some(point.z)),
            MOrdinate(has_m.then_some(point.m)),
        )
    };
    let (a, b, c) = (coerce(a), coerce(b), coerce(c));
    let axes = CoordinateAxes::new(HasZ(has_z), HasM(has_m));
    let mut builder = CoordSeqBuilder::with_capacity(axes, 4);
    builder.push(a);
    builder.push(b);
    builder.push(c);
    builder.push(a);
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(builder.finish()?)?,
        Vec::new(),
    )))
}

pub(crate) fn point_in_ccw_triangle(
    point: impl Into<XY>,
    a: impl Into<XY>,
    b: impl Into<XY>,
    c: impl Into<XY>,
) -> bool {
    let (point, a, b, c) = (point.into(), a.into(), b.into(), c.into());
    orientation(a, b, point) != Orientation::Clockwise
        && orientation(b, c, point) != Orientation::Clockwise
        && orientation(c, a, point) != Orientation::Clockwise
}
