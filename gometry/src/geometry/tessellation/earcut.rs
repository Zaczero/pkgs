#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use ahash::HashSetExt as _;
use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use crate::curves::morton_interleave;
use crate::error::Result;
use crate::geometry::{
    Bounds, BulkRTree, CoordSeqBuilder, CoordinateAxes, Coordinates, GeometryErrorKind, HasM, HasZ,
    HashSet, MOrdinate, Orientation, Point, PointKey, Polygon, Ring, RingWinding, Segment,
    SegmentIndex, Shape, XY, ZOrdinate, dedup_consecutive_points, open_point_cycle_winding,
    orientation, orientation_xy, point_on_segment, ring_contains_interior, same_point,
    segments_intersect, wrap_index,
};

/// Walk a polygon's ear-cut triangles through one caller-owned sink. The
/// generated-output budget lives at that caller, so it can reject before a
/// triangle shape or array row is emitted.
pub(crate) fn earcut_polygon_with(
    polygon: &Polygon,
    emit: &mut impl FnMut(Point, Point, Point) -> Result<()>,
) -> Result<()> {
    let mut ring = cleaned_earcut_ring(&polygon.shell, false)?;
    if ring.len() < 3 || open_point_cycle_winding(&ring).is_degenerate() {
        return Ok(());
    }

    let mut holes = Vec::with_capacity(polygon.holes.len());
    for hole in polygon.holes.iter() {
        let hole = cleaned_earcut_ring(hole, true)?;
        if hole.len() < 3 || open_point_cycle_winding(&hole).is_degenerate() {
            return Err(GeometryErrorKind::triangulation("polygon hole degenerates"));
        }
        validate_hole_inside_shell(&ring, &hole)?;
        holes.push(hole);
    }
    validate_hole_interactions(&ring, &holes)?;
    eliminate_holes(&mut ring, &holes)?;
    earcut_linked_ring_with(&ring, emit)
}

pub(crate) fn cleaned_earcut_ring<C: Coordinates + ?Sized>(
    ring: &C,
    exterior_cw: bool,
) -> Result<Vec<Point>> {
    let mut vertices = open_ring(ring);
    dedup_circular_points(&mut vertices);
    strip_collinear_vertices(&mut vertices);
    orient_earcut_ring(&mut vertices, exterior_cw);
    validate_earcut_ring(&vertices)?;
    Ok(vertices)
}

pub(crate) fn orient_earcut_ring(vertices: &mut [Point], exterior_cw: bool) {
    if vertices.len() < 3 {
        return;
    }
    let is_clockwise = open_point_cycle_winding(vertices) == RingWinding::Clockwise;
    if is_clockwise != exterior_cw {
        vertices.reverse();
    }
}

pub(crate) fn dedup_circular_points(vertices: &mut Vec<Point>) {
    dedup_consecutive_points(vertices);
    while vertices.len() > 1
        && same_point(
            *vertices.first().expect("checked non-empty"),
            *vertices.last().expect("checked non-empty"),
        )
    {
        vertices.pop();
    }
}

pub(crate) fn strip_collinear_vertices(vertices: &mut Vec<Point>) {
    if vertices.len() < 3 {
        return;
    }
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
    for &curr in vertices.iter() {
        out.push(curr);
        loop {
            match out.as_mut_slice() {
                [.., first, middle, last] if is_collinear(*first, *middle, *last) => {
                    *middle = *last;
                    out.pop();
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
            continue;
        }
        let removes_first = match out.as_slice() {
            [first, second, .., last] => is_collinear(*last, *first, *second),
            _ => false,
        };
        if removes_first {
            out.remove(0);
        } else {
            break;
        }
    }
    *vertices = out;
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
    let segments: Vec<Segment> = cycle
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
            #[expect(
                clippy::indexing_slicing,
                reason = "candidate ordinals come from the SegmentIndex built from this exact segment slice"
            )]
            let right_segment = segments[right];
            if segments_intersect(left_segment, right_segment) {
                return Err(GeometryErrorKind::triangulation(
                    "polygon ring self-intersects",
                ));
            }
        }
    }
    Ok(())
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
            return Err(match (left_id, right_id) {
                (EarcutRingId::Shell, EarcutRingId::Hole(_))
                | (EarcutRingId::Hole(_), EarcutRingId::Shell) => {
                    GeometryErrorKind::triangulation("polygon hole touches or crosses the shell")
                },
                _ => GeometryErrorKind::triangulation("polygon holes touch or cross"),
            });
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

const fn ring_segments_adjacent(len: usize, left: usize, right: usize) -> bool {
    left.abs_diff(right) == 1 || (left == 0 && right == len.wrapping_sub(1))
}

pub(crate) fn eliminate_holes(ring: &mut Vec<Point>, holes: &[Vec<Point>]) -> Result<()> {
    let holes = HoleList::new(holes);
    let mut order: Vec<_> = holes.ids().collect();
    order.sort_unstable_by(|&left, &right| {
        compare_hole_leftmost_xy_slope(holes.ring(left), holes.ring(right))
            .then_with(|| left.index().cmp(&right.index()))
    });
    for hole_id in order {
        let hole_ring = holes.ring(hole_id);
        let hole = rotate_ring(hole_ring, leftmost_vertex(hole_ring));
        let bridge = find_hole_bridge(ring, &hole).ok_or_else(|| {
            GeometryErrorKind::triangulation("failed to find a visible bridge for polygon hole")
        })?;
        splice_hole_bridge(ring, bridge, &hole);
        filter_earcut_ring_points(ring);
    }
    Ok(())
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

pub(crate) fn filter_earcut_ring_points(ring: &mut Vec<Point>) {
    dedup_consecutive_points(ring);
    strip_collinear_vertices(ring);
}

#[expect(
    clippy::float_cmp,
    reason = "exact comparison is intentional (sentinel / degenerate / exact-literal check)"
)]
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

fn point_in_triangle(
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

#[derive(Clone, Copy)]
struct EarNode {
    point: Point,
    prev: NodeId,
    next: NodeId,
    active: bool,
    z: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NodeId(usize);

impl NodeId {
    const fn from_index(index: usize) -> Self {
        Self(index)
    }

    const fn index(self) -> usize {
        self.0
    }
}

pub(crate) struct EarArena {
    nodes: Vec<EarNode>,
    z_order: Vec<NodeId>,
    active: usize,
    z_scale: Option<ZScale>,
}

impl EarArena {
    pub(crate) fn new(points: &[Point]) -> Self {
        let z_scale = ZScale::new(points);
        let nodes: Vec<EarNode> = points
            .iter()
            .copied()
            .enumerate()
            .map(|(index, point)| EarNode {
                point,
                prev: NodeId::from_index(wrap_index(
                    index.wrapping_add(points.len()).wrapping_sub(1),
                    points.len(),
                )),
                next: NodeId::from_index(wrap_index(index.wrapping_add(1), points.len())),
                active: true,
                z: z_scale.map_or(0, |scale| scale.key(point)),
            })
            .collect();
        let mut z_order = nodes
            .iter()
            .enumerate()
            .map(|(index, _)| NodeId::from_index(index))
            .collect::<Vec<_>>();
        z_order.sort_unstable_by_key(|&node| arena_node(&nodes, node).z);
        Self {
            nodes,
            z_order,
            active: points.len(),
            z_scale,
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
        self.node_mut(id).active = false;
        self.active = self.active.wrapping_sub(1);
    }

    fn active_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| node.active.then_some(NodeId::from_index(index)))
    }

    #[cfg(test)]
    pub(crate) const fn has_z_scale(&self) -> bool {
        self.z_scale.is_some()
    }

    fn z_window(&self, min: u64, max: u64) -> Option<&[NodeId]> {
        let lo = self
            .z_order
            .partition_point(|&candidate| self.node(candidate).z < min);
        let hi = self
            .z_order
            .partition_point(|&candidate| self.node(candidate).z <= max);
        self.z_order.get(lo..hi)
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

#[derive(Clone, Copy)]
pub(crate) struct ZScale {
    pub(crate) minx: f64,
    pub(crate) miny: f64,
    pub(crate) width: f64,
    pub(crate) height: f64,
}

impl ZScale {
    pub(crate) fn new(points: &[Point]) -> Option<Self> {
        let first = *points.first()?;
        let (mut minx, mut miny, mut maxx, mut maxy) = (first.x, first.y, first.x, first.y);
        for point in points.iter().skip(1) {
            minx = minx.min(point.x);
            miny = miny.min(point.y);
            maxx = maxx.max(point.x);
            maxy = maxy.max(point.y);
        }
        let bounds = Bounds::new_raw(minx, miny, maxx, maxy);
        let width = bounds.maxx() - bounds.minx();
        let height = bounds.maxy() - bounds.miny();
        (width.is_finite()
            && height.is_finite()
            && (width > 0.0 || height > 0.0)
            && !(width == 0.0 && height == 0.0))
            .then_some(Self {
                minx: bounds.minx(),
                miny: bounds.miny(),
                width,
                height,
            })
    }

    pub(crate) fn key(self, point: Point) -> u64 {
        let cell = |value: f64, min: f64, span: f64| {
            (((value - min) / span).clamp(0.0, 1.0) * f64::from(u32::MAX)) as u32
        };
        if self.width == 0.0 {
            u64::from(cell(point.y, self.miny, self.height))
        } else if self.height == 0.0 {
            u64::from(cell(point.x, self.minx, self.width))
        } else {
            morton_interleave(
                cell(point.x, self.minx, self.width),
                cell(point.y, self.miny, self.height),
            )
        }
    }

    pub(crate) fn range(self, a: Point, b: Point, c: Point) -> (u64, u64) {
        if self.width == 0.0 {
            let min_y = a.y.min(b.y).min(c.y);
            let max_y = a.y.max(b.y).max(c.y);
            let min = Point::new_raw_xy(a.x, min_y);
            let max = Point::new_raw_xy(a.x, max_y);
            (self.key(min), self.key(max))
        } else if self.height == 0.0 {
            let min_x = a.x.min(b.x).min(c.x);
            let max_x = a.x.max(b.x).max(c.x);
            let min = Point::new_raw_xy(min_x, a.y);
            let max = Point::new_raw_xy(max_x, a.y);
            (self.key(min), self.key(max))
        } else {
            let min = Point::new_raw_xy(a.x.min(b.x).min(c.x), a.y.min(b.y).min(c.y));
            let max = Point::new_raw_xy(a.x.max(b.x).max(c.x), a.y.max(b.y).max(c.y));
            (self.key(min), self.key(max))
        }
    }
}

pub(crate) fn earcut_linked_ring_with(
    points: &[Point],
    emit: &mut impl FnMut(Point, Point, Point) -> Result<()>,
) -> Result<()> {
    if points.len() < 3 {
        return Ok(());
    }
    let mut arena = EarArena::new(points);
    let mut current = NodeId::from_index(0);
    let mut guard = 0_usize;
    while arena.active > 3 {
        if is_ear(&arena, current) {
            let (prev, next) = {
                let node = arena.node(current);
                (node.prev, node.next)
            };
            let triangle = (
                arena.node(prev).point,
                arena.node(current).point,
                arena.node(next).point,
            );
            emit(triangle.0, triangle.1, triangle.2)?;
            arena.remove(current);
            current = next;
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
        let first = arena
            .active_ids()
            .next()
            .expect("three active nodes remain");
        let second = arena.node(first).next;
        let third = arena.node(second).next;
        emit(
            arena.node(first).point,
            arena.node(second).point,
            arena.node(third).point,
        )?;
    }
    Ok(())
}

fn is_ear(arena: &EarArena, index: NodeId) -> bool {
    if !arena.node(index).active {
        return false;
    }
    let (prev, next) = {
        let node = arena.node(index);
        (node.prev, node.next)
    };
    let (a, b, c) = (
        arena.node(prev).point,
        arena.node(index).point,
        arena.node(next).point,
    );
    if orientation(a, b, c) != Orientation::CounterClockwise {
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
    if let Some(scale) = arena.z_scale {
        let (min_z, max_z) = scale.range(a, b, c);
        // `z_order` is sorted by `node.z`, so the candidates in the ear's
        // z-range are a CONTIGUOUS slice — binary-search its bounds and scan
        // only that window (mapbox earcut's O(log n + k) hashing) instead of
        // the whole list, which made every ear test O(n) (overall O(n²)).
        return arena.z_window(min_z, max_z).is_some_and(|window| {
            window.iter().copied().any(|candidate| {
                let node = arena.node(candidate);
                node.active
                    && candidate_inside_ear(candidate, prev, current, next, node.point, a, b, c)
            })
        });
    }
    arena.active_ids().any(|candidate| {
        let point = arena.node(candidate).point;
        candidate_inside_ear(candidate, prev, current, next, point, a, b, c)
    })
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
    candidate != prev
        && candidate != current
        && candidate != next
        && !same_point(point, a)
        && !same_point(point, b)
        && !same_point(point, c)
        && point_in_ccw_triangle(point, a, b, c)
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
