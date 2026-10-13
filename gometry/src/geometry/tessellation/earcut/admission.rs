use ahash::HashSetExt as _;
use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use super::super::earcut_sweep::ring_has_contact;
use super::*;

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
pub(in crate::geometry::tessellation) fn ring_has_contact_box_scan(ring: &[Point]) -> bool {
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
            super::super::earcut_sweep::record_work();
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
pub(super) struct HoleId(usize);

impl HoleId {
    pub(super) const fn from_index(index: usize) -> Self {
        Self(index)
    }

    pub(super) const fn index(self) -> usize {
        self.0
    }
}

pub(super) struct HoleList<'holes> {
    pub(super) rings: &'holes [Vec<Point>],
}

impl<'holes> HoleList<'holes> {
    pub(super) const fn new(rings: &'holes [Vec<Point>]) -> Self {
        Self { rings }
    }

    pub(super) fn ids(&self) -> impl Iterator<Item = HoleId> + '_ {
        (0..self.rings.len()).map(HoleId::from_index)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "HoleId is created by enumerating this unchanged hole slice before bridge elimination"
    )]
    pub(super) fn ring(&self, hole: HoleId) -> &[Point] {
        &self.rings[hole.index()]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RingVertexId(pub(super) usize);

pub(super) struct EarcutCycle<'ring> {
    points: &'ring [Point],
}

impl<'ring> EarcutCycle<'ring> {
    pub(super) const fn new(points: &'ring [Point]) -> Self {
        Self { points }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "RingVertexId comes from enumerating this unchanged ring and is used only before the next splice"
    )]
    pub(super) const fn point(&self, vertex: RingVertexId) -> Point {
        self.points[vertex.0]
    }

    pub(super) const fn next(&self, vertex: RingVertexId) -> RingVertexId {
        RingVertexId(wrap_index(vertex.0.wrapping_add(1), self.points.len()))
    }

    #[cfg(test)]
    pub(super) const fn previous(&self, vertex: RingVertexId) -> RingVertexId {
        let prior = vertex.0.wrapping_add(self.points.len()).wrapping_sub(1);
        RingVertexId(wrap_index(prior, self.points.len()))
    }

    pub(super) fn edges(&self) -> impl Iterator<Item = (RingVertexId, Point, Point)> + '_ {
        self.points
            .iter()
            .zip(self.points.iter().cycle().skip(1))
            .enumerate()
            .map(|(index, (start, end))| (RingVertexId(index), *start, *end))
    }

    #[cfg(test)]
    pub(super) fn vertices_from(
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
                    super::super::RingPairIntersection::ShellHole
                },
                _ => super::super::RingPairIntersection::HoleHole,
            };
            return Err(super::super::ring_pair_intersection_error(intersection));
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

pub(in crate::geometry::tessellation) const fn ring_segments_adjacent(
    len: usize,
    left: usize,
    right: usize,
) -> bool {
    left.abs_diff(right) == 1 || (left == 0 && right == len.wrapping_sub(1))
}
