#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use super::*;
use crate::curves::morton_interleave;
use crate::error::Result;

pub(crate) fn polygon_triangles(polygon: &Polygon) -> Result<Vec<Shape>> {
    earcut_polygon(polygon)
}

pub(crate) fn earcut_polygon(polygon: &Polygon) -> Result<Vec<Shape>> {
    let mut ring = cleaned_earcut_ring(&polygon.shell, false)?;
    if ring.len() < 3 || open_point_cycle_decision(&ring).sign() == AreaSign::Zero {
        return Ok(Vec::new());
    }

    let mut holes = Vec::with_capacity(polygon.holes.len());
    for hole in polygon.holes.iter() {
        let hole = cleaned_earcut_ring(hole, true)?;
        if hole.len() < 3 || open_point_cycle_decision(&hole).sign() == AreaSign::Zero {
            return Err(GeometryErrorKind::triangulation("polygon hole degenerates"));
        }
        validate_hole_inside_shell(&ring, &hole)?;
        holes.push(hole);
    }
    validate_hole_interactions(&ring, &holes)?;
    eliminate_holes(&mut ring, &holes, polygon)?;
    earcut_linked_ring(&ring)
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
        while out.len() >= 3 {
            let n = out.len();
            if is_collinear(out[n - 3], out[n - 2], out[n - 1]) {
                out[n - 2] = out[n - 1];
                out.pop();
            } else {
                break;
            }
        }
    }
    // Seam fixup: the ring is closed, so re-check the wrap-around triples
    // (…, last, first) and (last, first, second), collapsing the middle until
    // stable. A simple polygon's seam holds O(1) collinear vertices.
    while out.len() >= 3 {
        let n = out.len();
        if is_collinear(out[n - 2], out[n - 1], out[0]) {
            out.pop();
        } else if is_collinear(out[n - 1], out[0], out[1]) {
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
    for index in 0..ring.len() {
        let len = ring.len();
        let prev = ring[wrap_index(index + len - 1, len)];
        let curr = ring[index];
        let next = ring[wrap_index(index + 1, len)];
        if orientation(prev, curr, next) == Orientation::Collinear
            && !point_on_segment(curr, prev, next)
        {
            return Err(GeometryErrorKind::triangulation(
                "polygon ring self-intersects",
            ));
        }
    }
    let segments: Vec<Segment> = (0..ring.len())
        .map(|index| ring_segment(ring, index))
        .collect();
    let index = SegmentIndex::build(&segments);
    for left in 0..segments.len() {
        for entry in index.intersecting_candidates(segments[left]) {
            let right = entry.ordinal;
            if right <= left || ring_segments_adjacent(ring.len(), left, right) {
                continue;
            }
            if segments_intersect(segments[left], segments[right]) {
                return Err(GeometryErrorKind::triangulation(
                    "polygon ring self-intersects",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_hole_inside_shell(shell: &[Point], hole: &[Point]) -> Result<()> {
    if !ring_contains_interior(&closed_ring_unchecked(shell), hole[0]) {
        return Err(GeometryErrorKind::triangulation(
            "polygon hole is not strictly inside the shell",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum EarcutRingId {
    Shell,
    Hole(usize),
}

fn push_ring_segments(
    ring: &[Point],
    id: EarcutRingId,
    segments: &mut Vec<Segment>,
    owners: &mut Vec<EarcutRingId>,
) {
    for index in 0..ring.len() {
        segments.push(ring_segment(ring, index));
        owners.push(id);
    }
}

pub(crate) fn validate_hole_interactions(shell: &[Point], holes: &[Vec<Point>]) -> Result<()> {
    let mut segments = Vec::new();
    let mut owners = Vec::new();
    push_ring_segments(shell, EarcutRingId::Shell, &mut segments, &mut owners);
    for (index, hole) in holes.iter().enumerate() {
        push_ring_segments(hole, EarcutRingId::Hole(index), &mut segments, &mut owners);
    }
    let index = SegmentIndex::build(&segments);
    for left in 0..segments.len() {
        for entry in index.intersecting_candidates(segments[left]) {
            let right = entry.ordinal;
            if right <= left {
                continue;
            }
            let (left_id, right_id) = (owners[left], owners[right]);
            if matches!(
                (left_id, right_id),
                (EarcutRingId::Shell, EarcutRingId::Shell)
            ) {
                continue;
            }
            if left_id == right_id {
                continue;
            }
            if !segments_intersect(segments[left], segments[right]) {
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

/// Reject nested holes: each pair is tested only when one bbox contains the
/// other's representative vertex (the necessary containment condition).
fn validate_holes_disjoint(holes: &[Vec<Point>]) -> Result<()> {
    if holes.len() < 2 {
        return Ok(());
    }
    let preps: Vec<HoleContainmentPrep> = holes
        .iter()
        .map(|hole| {
            let closed = closed_ring_unchecked(hole);
            let bounds = Bounds::from_xy_iter(closed.iter().map(Point::xy));
            HoleContainmentPrep {
                closed,
                rep: hole[0],
                bounds,
            }
        })
        .collect();
    let tree: BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>> =
        BulkRTree::bulk_load_with_params(
            preps
                .iter()
                .enumerate()
                .map(|(index, prep)| {
                    GeomWithData::new(
                        Rectangle::from_corners([prep.bounds.minx(), prep.bounds.miny()], [
                            prep.bounds.maxx(),
                            prep.bounds.maxy(),
                        ]),
                        index,
                    )
                })
                .collect(),
        );
    for (index, prep) in preps.iter().enumerate() {
        let point_envelope = AABB::from_corners([prep.rep.x, prep.rep.y], [prep.rep.x, prep.rep.y]);
        for entry in tree.locate_in_envelope_intersecting(point_envelope) {
            let other = entry.data;
            if other == index {
                continue;
            }
            if ring_contains_interior(&prep.closed, preps[other].rep)
                || ring_contains_interior(&preps[other].closed, prep.rep)
            {
                return Err(GeometryErrorKind::triangulation(
                    "polygon holes must be disjoint",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) const fn ring_segment(ring: &[Point], index: usize) -> Segment {
    Segment {
        start: ring[index].xy(),
        end: ring[wrap_index(index + 1, ring.len())].xy(),
    }
}

pub(crate) const fn ring_segments_adjacent(len: usize, left: usize, right: usize) -> bool {
    left.abs_diff(right) == 1 || (left == 0 && right == len - 1)
}

pub(crate) fn eliminate_holes(
    ring: &mut Vec<Point>,
    holes: &[Vec<Point>],
    _polygon: &Polygon,
) -> Result<()> {
    let mut order = (0..holes.len()).collect::<Vec<_>>();
    order.sort_by(|&left, &right| compare_hole_leftmost_xy_slope(&holes[left], &holes[right]));
    for hole_index in order {
        let hole = rotate_ring(&holes[hole_index], leftmost_vertex(&holes[hole_index]));
        let bridge = find_hole_bridge(ring, &hole).ok_or_else(|| {
            GeometryErrorKind::triangulation("failed to find a visible bridge for polygon hole")
        })?;
        splice_hole_bridge(ring, bridge, &hole);
        filter_earcut_ring_points(ring);
    }
    Ok(())
}

fn compare_hole_leftmost_xy_slope(left: &[Point], right: &[Point]) -> std::cmp::Ordering {
    let left_index = leftmost_vertex(left);
    let right_index = leftmost_vertex(right);
    let left_point = left[left_index];
    let right_point = right[right_index];
    left_point
        .x
        .total_cmp(&right_point.x)
        .then_with(|| left_point.y.total_cmp(&right_point.y))
        .then_with(|| {
            let left_next = left[wrap_index(left_index + 1, left.len())];
            let right_next = right[wrap_index(right_index + 1, right.len())];
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

pub(crate) fn leftmost_vertex(ring: &[Point]) -> usize {
    (1..ring.len()).fold(0, |best, index| {
        let point = ring[index];
        let current = ring[best];
        if point
            .x
            .total_cmp(&current.x)
            .then_with(|| point.y.total_cmp(&current.y))
            == std::cmp::Ordering::Less
        {
            index
        } else {
            best
        }
    })
}

pub(crate) fn rotate_ring(ring: &[Point], start: usize) -> Vec<Point> {
    ring[start..]
        .iter()
        .chain(&ring[..start])
        .copied()
        .collect()
}

pub(crate) fn filter_earcut_ring_points(ring: &mut Vec<Point>) {
    dedup_consecutive_points(ring);
    strip_collinear_vertices(ring);
}

#[expect(clippy::many_single_char_names, reason = "standard math notation")]
#[expect(
    clippy::float_cmp,
    reason = "exact comparison is intentional (sentinel / degenerate / exact-literal check)"
)]
pub(crate) fn find_hole_bridge(ring: &[Point], hole: &[Point]) -> Option<usize> {
    let hole_point = hole[0];
    let (hx, hy) = (hole_point.x, hole_point.y);
    let n = ring.len();
    let mut qx = f64::NEG_INFINITY;
    let mut m = None;

    for index in 0..n {
        if same_point(hole_point, ring[index]) {
            return Some(index);
        }
        let next = wrap_index(index + 1, n);
        if same_point(hole_point, ring[next]) {
            return Some(next);
        }
        let a = ring[index];
        let b = ring[next];
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

    let mx = ring[m].x;
    let my = ring[m].y;
    let stop = m;
    let mut tan_min = f64::INFINITY;
    let mut p = m;
    loop {
        let pt = ring[p];
        let inside = if hy < my {
            point_in_triangle(hx, hy, mx, my, qx, hy, pt.x, pt.y)
        } else {
            point_in_triangle(qx, hy, mx, my, hx, hy, pt.x, pt.y)
        };
        if hx >= pt.x && pt.x >= mx && hx != pt.x && inside {
            let tan = (hy - pt.y).abs() / (hx - pt.x);
            if locally_inside_for_bridge(ring, p, hole_point)
                && (tan < tan_min
                    || (tan == tan_min
                        && (pt.x > ring[m].x
                            || (pt.x == ring[m].x && hole_bridge_sector_ok(ring, m, p)))))
            {
                m = p;
                tan_min = tan;
            }
        }
        p = wrap_index(p + 1, n);
        if p == stop {
            break;
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

fn locally_inside_for_bridge(ring: &[Point], p: usize, target: Point) -> bool {
    let n = ring.len();
    let prev = ring[wrap_index(p + n - 1, n)];
    let curr = ring[p];
    let next = ring[wrap_index(p + 1, n)];
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

fn hole_bridge_sector_ok(ring: &[Point], m: usize, p: usize) -> bool {
    let n = ring.len();
    let m_prev = ring[wrap_index(m + n - 1, n)];
    let m_curr = ring[m];
    let m_next = ring[wrap_index(m + 1, n)];
    let p_prev = ring[wrap_index(p + n - 1, n)];
    let p_next = ring[wrap_index(p + 1, n)];
    orientation_xy(m_prev.x, m_prev.y, m_curr.x, m_curr.y, p_prev.x, p_prev.y)
        == Orientation::CounterClockwise
        && orientation_xy(p_next.x, p_next.y, m_curr.x, m_curr.y, m_next.x, m_next.y)
            == Orientation::CounterClockwise
}

pub(crate) fn splice_hole_bridge(ring: &mut Vec<Point>, bridge_index: usize, hole: &[Point]) {
    let bridge_point = ring[bridge_index];
    let hole_point = hole[0];
    let mut spliced = Vec::with_capacity(ring.len() + hole.len() + 2);
    spliced.extend_from_slice(&ring[..=bridge_index]);
    spliced.extend_from_slice(hole);
    spliced.push(hole_point);
    spliced.push(bridge_point);
    spliced.extend_from_slice(&ring[bridge_index + 1..]);
    *ring = spliced;
}

#[derive(Clone, Copy)]
pub(crate) struct EarNode {
    pub(crate) point: Point,
    pub(crate) prev: usize,
    pub(crate) next: usize,
    pub(crate) active: bool,
    pub(crate) z: u64,
}

pub(crate) struct EarArena {
    pub(crate) nodes: Vec<EarNode>,
    pub(crate) z_order: Vec<usize>,
    pub(crate) active: usize,
    pub(crate) z_scale: Option<ZScale>,
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
                prev: wrap_index(index + points.len() - 1, points.len()),
                next: wrap_index(index + 1, points.len()),
                active: true,
                z: z_scale.map_or(0, |scale| scale.key(point)),
            })
            .collect();
        let mut z_order = (0..points.len()).collect::<Vec<_>>();
        z_order.sort_unstable_by_key(|&index| nodes[index].z);
        Self {
            nodes,
            z_order,
            active: points.len(),
            z_scale,
        }
    }

    pub(crate) fn remove(&mut self, index: usize) {
        let prev = self.nodes[index].prev;
        let next = self.nodes[index].next;
        self.nodes[prev].next = next;
        self.nodes[next].prev = prev;
        self.nodes[index].active = false;
        self.active -= 1;
    }

    pub(crate) fn active_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| node.active.then_some(index))
    }
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
        for point in &points[1..] {
            minx = minx.min(point.x);
            miny = miny.min(point.y);
            maxx = maxx.max(point.x);
            maxy = maxy.max(point.y);
        }
        let bounds = Bounds::new_unchecked(minx, miny, maxx, maxy);
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
            let min = Point::new_unchecked_xy(a.x, min_y);
            let max = Point::new_unchecked_xy(a.x, max_y);
            (self.key(min), self.key(max))
        } else if self.height == 0.0 {
            let min_x = a.x.min(b.x).min(c.x);
            let max_x = a.x.max(b.x).max(c.x);
            let min = Point::new_unchecked_xy(min_x, a.y);
            let max = Point::new_unchecked_xy(max_x, a.y);
            (self.key(min), self.key(max))
        } else {
            let min = Point::new_unchecked_xy(a.x.min(b.x).min(c.x), a.y.min(b.y).min(c.y));
            let max = Point::new_unchecked_xy(a.x.max(b.x).max(c.x), a.y.max(b.y).max(c.y));
            (self.key(min), self.key(max))
        }
    }
}

pub(crate) fn earcut_linked_ring(points: &[Point]) -> Result<Vec<Shape>> {
    if points.len() < 3 {
        return Ok(Vec::new());
    }
    let mut arena = EarArena::new(points);
    let mut triangles = Vec::with_capacity(points.len().saturating_sub(2));
    let mut current = 0;
    let mut guard = 0;
    while arena.active > 3 {
        if is_ear(&arena, current) {
            let prev = arena.nodes[current].prev;
            let next = arena.nodes[current].next;
            triangles.push(triangle_shape(
                arena.nodes[prev].point,
                arena.nodes[current].point,
                arena.nodes[next].point,
            ));
            arena.remove(current);
            current = next;
            guard = 0;
            continue;
        }
        current = arena.nodes[current].next;
        guard += 1;
        if guard > arena.nodes.len() {
            return Err(GeometryErrorKind::triangulation(
                "earcut failed to find a valid ear",
            ));
        }
    }
    let remaining = arena.active_indices().collect::<Vec<_>>();
    if remaining.len() == 3 {
        let first = remaining[0];
        let second = arena.nodes[first].next;
        let third = arena.nodes[second].next;
        triangles.push(triangle_shape(
            arena.nodes[first].point,
            arena.nodes[second].point,
            arena.nodes[third].point,
        ));
    }
    Ok(triangles)
}

pub(crate) fn is_ear(arena: &EarArena, index: usize) -> bool {
    if !arena.nodes[index].active {
        return false;
    }
    let prev = arena.nodes[index].prev;
    let next = arena.nodes[index].next;
    let (a, b, c) = (
        arena.nodes[prev].point,
        arena.nodes[index].point,
        arena.nodes[next].point,
    );
    if orientation(a, b, c) != Orientation::CounterClockwise {
        return false;
    }
    !ear_contains_vertex(arena, prev, index, next, a, b, c)
}

pub(crate) fn ear_contains_vertex(
    arena: &EarArena,
    prev: usize,
    current: usize,
    next: usize,
    a: Point,
    b: Point,
    c: Point,
) -> bool {
    if let Some(scale) = arena.z_scale {
        let (min_z, max_z) = scale.range(a, b, c);
        // `z_order` is sorted by `node.z`, so the candidates in the ear's
        // z-range are a CONTIGUOUS slice — binary-search its bounds and scan
        // only that window (mapbox earcut's O(log n + k) hashing) instead of
        // the whole list, which made every ear test O(n) (overall O(n^2)).
        let lo = arena
            .z_order
            .partition_point(|&candidate| arena.nodes[candidate].z < min_z);
        let hi = arena
            .z_order
            .partition_point(|&candidate| arena.nodes[candidate].z <= max_z);
        return arena.z_order[lo..hi].iter().copied().any(|candidate| {
            let node = arena.nodes[candidate];
            node.active && candidate_inside_ear(candidate, prev, current, next, node.point, a, b, c)
        });
    }
    arena.active_indices().any(|candidate| {
        let point = arena.nodes[candidate].point;
        candidate_inside_ear(candidate, prev, current, next, point, a, b, c)
    })
}

pub(crate) fn candidate_inside_ear(
    candidate: usize,
    prev: usize,
    current: usize,
    next: usize,
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
    let closed = len > 1 && same_point(points.nth_coord(0), points.nth_coord(len - 1));
    let keep = if closed { len - 1 } else { len };
    points
        .iter_coords()
        .take(keep)
        .map(Point::force_2d)
        .collect()
}

pub(crate) fn closed_ring_unchecked<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    let mut ring = points.iter_coords().collect::<Vec<_>>();
    if let Some(first) = ring.first().copied() {
        ring.push(first);
    }
    ring
}

pub(crate) fn triangle_shape(a: Point, b: Point, c: Point) -> Shape {
    Shape::Polygon(Polygon::new(
        Ring::from_trusted_closed(vec![a, b, c, a]),
        Vec::new(),
    ))
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
