//! Shared topology staging and membership primitives.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ops::Range;
use std::simd::cmp::SimdPartialOrd as _;
use std::simd::num::SimdFloat as _;

use crate::geometry::{
    Arc, CCW_ERRBOUND_A, CoordSeq, PointKey, Polygon, REDUCE_LANES, ReduceSimd, Segment, XY,
    ray_crossing_is_right, simd_reduce_f64, wrap_index,
};

mod sections;
mod staging;
pub(super) use sections::{
    compare_along_segment, operand_covers_boundary, other_contains, sort_dedup_cuts,
};
pub(super) use staging::StagedRings;
use staging::oriented_open_ring;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operand {
    Left,
    Right,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum SectionEnd {
    Cross,
    Reseed,
    None,
}

pub(super) const fn section_end(cross: bool, shared: bool) -> SectionEnd {
    if cross && !shared {
        SectionEnd::Cross
    } else {
        SectionEnd::Reseed
    }
}

#[derive(Clone, Copy)]
pub(super) struct Cut {
    pub(super) point: XY,
    pub(super) key: PointKey,
    pub(super) cross: bool,
}

pub(super) fn add_cut(cuts: &mut Vec<Cut>, point: XY, cross: bool) {
    cuts.push(Cut {
        point,
        key: PointKey::new(point),
        cross,
    });
}

#[derive(Clone, Debug)]
pub(super) struct OrientedRing {
    pub operand: Operand,
    pub polygon: u32,
    pub ring: u32,
    pub is_hole: bool,
    pub points: std::sync::Arc<[XY]>,
    pub segments: Range<usize>,
    /// Cached representative interior point of the owning polygon (shell rings
    /// only; `None` on holes and on the uncached raw-`Shape` path).
    pub probe: Option<XY>,
}

#[derive(Clone, Debug)]
pub(super) struct OperandPool {
    pub segments: Vec<Segment>,
    pub operands: Vec<Operand>,
    pub ring_of: Vec<u32>,
    pub rings: Vec<OrientedRing>,
    pub split: usize,
}

thread_local! {
    // One relate builds one pool of the same ~N shape every call; reuse the
    // column buffers per-thread (taken cleared on build, returned on drop) so
    // the ~N-segment Vecs are allocated ONCE, not once per relate — mirrors
    // the `BoundaryContacts` and `SECTIONS_SCRATCH` scratch in `relate_ng`.
    static POOL_SEGMENTS: std::cell::Cell<Vec<Segment>> = const { std::cell::Cell::new(Vec::new()) };
    static POOL_OPERANDS: std::cell::Cell<Vec<Operand>> = const { std::cell::Cell::new(Vec::new()) };
    static POOL_RING_OF: std::cell::Cell<Vec<u32>> = const { std::cell::Cell::new(Vec::new()) };
    static POOL_RINGS: std::cell::Cell<Vec<OrientedRing>> = const { std::cell::Cell::new(Vec::new()) };
}

impl OperandPool {
    /// An empty pool reusing the per-thread scratch buffers, pre-sized for
    /// `segment_capacity` edges and `ring_capacity` rings.
    fn with_scratch(segment_capacity: usize, ring_capacity: usize) -> Self {
        let mut segments = POOL_SEGMENTS.with(std::cell::Cell::take);
        let mut operands = POOL_OPERANDS.with(std::cell::Cell::take);
        let mut ring_of = POOL_RING_OF.with(std::cell::Cell::take);
        let mut rings = POOL_RINGS.with(std::cell::Cell::take);
        segments.clear();
        segments.reserve(segment_capacity);
        operands.clear();
        operands.reserve(segment_capacity);
        ring_of.clear();
        ring_of.reserve(segment_capacity);
        rings.clear();
        rings.reserve(ring_capacity);
        Self {
            segments,
            operands,
            ring_of,
            rings,
            split: 0,
        }
    }
}

impl Drop for OperandPool {
    fn drop(&mut self) {
        POOL_SEGMENTS.with(|cell| cell.set(std::mem::take(&mut self.segments)));
        POOL_OPERANDS.with(|cell| cell.set(std::mem::take(&mut self.operands)));
        POOL_RING_OF.with(|cell| cell.set(std::mem::take(&mut self.ring_of)));
        POOL_RINGS.with(|cell| cell.set(std::mem::take(&mut self.rings)));
    }
}

/// Even-odd point membership in a single open ring (orientation-free).
///
/// Sign-form ray crossing (see [`ray_crossing_lanes`]), matching the
/// established predicate kernel exactly so SIMD and scalar membership share
/// one decision on every input, including huge-but-finite coordinates.
pub(super) fn ring_contains_interior_open(ring: &[XY], point: XY) -> bool {
    let n = ring.len();
    if n < crate::geometry::access::crossover::RAY_CROSSING {
        return ring_contains_interior_open_scalar(ring, point);
    }
    let px = ReduceSimd::splat(point.x);
    let py = ReduceSimd::splat(point.y);
    let crossings = simd_reduce_f64(
        n,
        (),
        0_u32,
        |(), crossings, start| {
            let ax = ReduceSimd::from_array(std::array::from_fn(|lane| ring[start + lane].x));
            let ay = ReduceSimd::from_array(std::array::from_fn(|lane| ring[start + lane].y));
            // Partner vertex is the next one. Within a full SIMD chunk
            // `start + lane + 1` lands in `1..=n`, hitting `n` only on the single
            // cyclic-wrap lane when `n` is a multiple of the lane count; a
            // branchless select handles that instead of the per-chunk integer
            // `div` the `% n` form compiled to. (Non-multiple `n` keeps the wrap
            // edge in the scalar tail, so `j < n` always holds here.)
            let bx = ReduceSimd::from_array(std::array::from_fn(|lane| {
                let j = start + lane + 1;
                ring[if j < n { j } else { 0 }].x
            }));
            let by = ReduceSimd::from_array(std::array::from_fn(|lane| {
                let j = start + lane + 1;
                ring[if j < n { j } else { 0 }].y
            }));
            ray_crossing_lanes(ax, ay, bx, by, px, py).map_or_else(
                || {
                    (
                        (),
                        crossings
                            + ring_crossing_count_scalar(ring, point, start..start + REDUCE_LANES),
                    )
                },
                |crosses| ((), crossings + crosses.to_bitmask().count_ones()),
            )
        },
        |crossings, range| crossings + ring_crossing_count_scalar(ring, point, range),
        |(), crossings| crossings,
    );
    crossings % 2 == 1
}

/// SIMD ray-crossing decision for one chunk of ring edges, or `None` when any
/// straddling lane's crossing sign cannot be trusted in plain `f64` (the
/// caller re-runs that chunk through the exact scalar path).
///
/// The old division form materialized the intersection X, so opposite-sign
/// huge coordinates overflowed `bx - ax` to infinity and corrupted the parity
/// (direction-dependent wrong `contains` near |x| ~ 1e308). The sign form
/// evaluates `orient2d(a, b, p)` per lane and trusts it only past the
/// all-input Ozaki filter, using Shewchuk's conservative A-stage constant:
/// straddling lanes with
/// `|det| <= errbound * (|t1| + |t2| + MIN_POSITIVE)`
/// (ties, near-degenerate, overflowed, or NaN terms all land here) demote the
/// whole chunk to [`ray_crossing_is_right`], mirroring the guarded-sqrt
/// chunk-rescue pattern in the length kernels. Division-free, so the fast
/// path also drops the one `vdivpd` per chunk.
fn ray_crossing_lanes(
    ax: ReduceSimd,
    ay: ReduceSimd,
    bx: ReduceSimd,
    by: ReduceSimd,
    px: ReduceSimd,
    py: ReduceSimd,
) -> Option<std::simd::Mask<i64, REDUCE_LANES>> {
    let straddles = ay.simd_gt(py) ^ by.simd_gt(py);
    let t1 = (bx - ax) * (py - ay);
    let t2 = (px - ax) * (by - ay);
    let det = t1 - t2;
    let trusted = det.abs().simd_gt(
        ((t1.abs() + t2.abs()) + ReduceSimd::splat(f64::MIN_POSITIVE))
            * ReduceSimd::splat(CCW_ERRBOUND_A),
    );
    if (straddles & !trusted).any() {
        return None;
    }
    let upward = by.simd_gt(ay);
    let zero = ReduceSimd::splat(0.0);
    Some(straddles & ((upward & det.simd_gt(zero)) | (!upward & det.simd_lt(zero))))
}

fn ring_crossing_count_scalar(ring: &[XY], point: XY, range: std::ops::Range<usize>) -> u32 {
    let n = ring.len();
    let mut crossings = 0_u32;
    for i in range {
        let (a, b) = (ring[i], ring[wrap_index(i + 1, n)]);
        if (a.y > point.y) != (b.y > point.y)
            && ray_crossing_is_right(a.x, a.y, b.x, b.y, point.x, point.y)
        {
            crossings += 1;
        }
    }
    crossings
}

fn ring_contains_interior_open_scalar(ring: &[XY], point: XY) -> bool {
    let n = ring.len();
    let mut inside = false;
    for i in 0..ring.len() {
        let (a, b) = (ring[i], ring[wrap_index(i + 1, n)]);
        if (a.y > point.y) != (b.y > point.y)
            && ray_crossing_is_right(a.x, a.y, b.x, b.y, point.x, point.y)
        {
            inside = !inside;
        }
    }
    inside
}

/// SIMD even-odd ray-crossing parity over a CLOSED ring's `SoA` columns — `xs`
/// and `ys` carry the repeated closing vertex, so consecutive pairs are exactly
/// the ring edges. Native columns load DIRECTLY (`Simd::from_slice`, no
/// per-lane gather — unlike the `&[XY]` `AoS` path), making this the fast lane
/// for any `Coordinates`-backed point-in-ring. Counting crossings is
/// order-independent, so the SIMD body + scalar tail give parity bit-identical
/// to the scalar toggle loop (same per-edge `f64` math in every lane).
pub(super) fn ring_contains_interior_columns(xs: &[f64], ys: &[f64], point: XY) -> bool {
    let segments = xs.len().saturating_sub(1);
    if segments < crate::geometry::access::crossover::RAY_CROSSING {
        return ring_crossing_columns_scalar(xs, ys, point, 0..segments) % 2 == 1;
    }
    let px = ReduceSimd::splat(point.x);
    let py = ReduceSimd::splat(point.y);
    let crossings = simd_reduce_f64(
        segments,
        (),
        0_u32,
        |(), crossings, start| {
            let ax = ReduceSimd::from_slice(&xs[start..start + REDUCE_LANES]);
            let ay = ReduceSimd::from_slice(&ys[start..start + REDUCE_LANES]);
            let bx = ReduceSimd::from_slice(&xs[start + 1..start + 1 + REDUCE_LANES]);
            let by = ReduceSimd::from_slice(&ys[start + 1..start + 1 + REDUCE_LANES]);
            ray_crossing_lanes(ax, ay, bx, by, px, py).map_or_else(
                || {
                    (
                        (),
                        crossings
                            + ring_crossing_columns_scalar(
                                xs,
                                ys,
                                point,
                                start..start + REDUCE_LANES,
                            ),
                    )
                },
                |crosses| ((), crossings + crosses.to_bitmask().count_ones()),
            )
        },
        |crossings, range| crossings + ring_crossing_columns_scalar(xs, ys, point, range),
        |(), crossings| crossings,
    );
    crossings % 2 == 1
}

fn ring_crossing_columns_scalar(xs: &[f64], ys: &[f64], point: XY, range: Range<usize>) -> u32 {
    let mut crossings = 0_u32;
    for i in range {
        let (ax, ay, bx, by) = (xs[i], ys[i], xs[i + 1], ys[i + 1]);
        if (ay > point.y) != (by > point.y)
            && ray_crossing_is_right(ax, ay, bx, by, point.x, point.y)
        {
            crossings += 1;
        }
    }
    crossings
}

/// SIMD ray-crossing count PLUS a boundary-candidate flag over a CLOSED ring's
/// `SoA` columns — the fast lane behind `ring_classify_point` (the
/// `contains`/`within`/`covers` predicate). The flag is set iff the point lies
/// inside SOME edge's bounding box, the necessary precondition for an exact
/// boundary hit; the caller resolves those rare near-edge cases with the exact
/// `orientation_xy` test. Far-from-boundary points (the common case) take this
/// pure-SIMD path: the crossing parity decides interior/exterior with the same
/// sign-form decision as the scalar loop (see [`ray_crossing_lanes`]), and the
/// boundary envelope is four packed compares per lane.
pub(super) fn ring_classify_crossings_columns(xs: &[f64], ys: &[f64], point: XY) -> (u32, bool) {
    let segments = xs.len().saturating_sub(1);
    if segments < crate::geometry::access::crossover::RAY_CROSSING {
        return ring_classify_crossings_scalar(xs, ys, point, 0..segments);
    }
    let px = ReduceSimd::splat(point.x);
    let py = ReduceSimd::splat(point.y);
    simd_reduce_f64(
        segments,
        (),
        (0_u32, false),
        |(), (crossings, candidate), start| {
            let ax = ReduceSimd::from_slice(&xs[start..start + REDUCE_LANES]);
            let ay = ReduceSimd::from_slice(&ys[start..start + REDUCE_LANES]);
            let bx = ReduceSimd::from_slice(&xs[start + 1..start + 1 + REDUCE_LANES]);
            let by = ReduceSimd::from_slice(&ys[start + 1..start + 1 + REDUCE_LANES]);
            // Boundary candidate: point inside this edge's bbox (four packed
            // compares); collapse the `REDUCE_LANES` lanes with `.any()`.
            let in_x = px.simd_ge(ax.simd_min(bx)) & px.simd_le(ax.simd_max(bx));
            let in_y = py.simd_ge(ay.simd_min(by)) & py.simd_le(ay.simd_max(by));
            let near = (in_x & in_y).any();
            // Crossing parity (same sign-form decision as the scalar loop).
            ray_crossing_lanes(ax, ay, bx, by, px, py).map_or_else(
                || {
                    let (c, n) =
                        ring_classify_crossings_scalar(xs, ys, point, start..start + REDUCE_LANES);
                    ((), (crossings + c, candidate || near || n))
                },
                |crosses| {
                    (
                        (),
                        (
                            crossings + crosses.to_bitmask().count_ones(),
                            candidate || near,
                        ),
                    )
                },
            )
        },
        |(crossings, candidate), range| {
            let (c, near) = ring_classify_crossings_scalar(xs, ys, point, range);
            (crossings + c, candidate || near)
        },
        |(), acc| acc,
    )
}

fn ring_classify_crossings_scalar(
    xs: &[f64],
    ys: &[f64],
    point: XY,
    range: Range<usize>,
) -> (u32, bool) {
    let mut crossings = 0_u32;
    let mut candidate = false;
    for i in range {
        let (ax, ay, bx, by) = (xs[i], ys[i], xs[i + 1], ys[i + 1]);
        if point.x >= ax.min(bx)
            && point.x <= ax.max(bx)
            && point.y >= ay.min(by)
            && point.y <= ay.max(by)
        {
            candidate = true;
        }
        if (ay > point.y) != (by > point.y)
            && ray_crossing_is_right(ax, ay, bx, by, point.x, point.y)
        {
            crossings += 1;
        }
    }
    (crossings, candidate)
}

/// Even-odd membership across shell + hole rings.
pub(super) fn polygon_rings_contain_interior<'a>(
    rings: impl IntoIterator<Item = &'a [XY]>,
    point: XY,
) -> bool {
    rings.into_iter().fold(false, |inside, ring| {
        inside ^ ring_contains_interior_open(ring, point)
    })
}

/// Stage both operands into one thin segment pool plus parallel provenance.
///
/// Rings are staged left first, then right; [`OperandPool::split`] is the
/// segment index where right-operand segments begin.
fn operand_ring_vertices<P: AsRef<Polygon>>(polygons: &[P]) -> usize {
    polygons
        .iter()
        .map(|polygon| {
            let polygon = polygon.as_ref();
            polygon.shell.coords().xs().len()
                + polygon
                    .holes
                    .iter()
                    .map(|hole| hole.coords().xs().len())
                    .sum::<usize>()
        })
        .sum()
}

pub(super) fn operand_ring_count<P: AsRef<Polygon>>(polygons: &[P]) -> usize {
    polygons
        .iter()
        .map(|polygon| 1 + polygon.as_ref().holes.len())
        .sum()
}

pub(super) fn build_operand_pool<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
    left: &[P],
    right: &[Q],
) -> OperandPool {
    // Pre-size to the total ring-vertex count (an upper bound on segments) so the
    // thin hot sweep pool and its parallel provenance never realloc while staging.
    let capacity = operand_ring_vertices(left) + operand_ring_vertices(right);
    let mut pool = OperandPool::with_scratch(
        capacity,
        operand_ring_count(left) + operand_ring_count(right),
    );
    // Staging stays operand-contiguous for cheap split tests, but noding callers
    // must chain by `ring_of`: a geometric continuation across rings or operands
    // is still a semantic boundary whose candidate pair must reach the visitor.
    push_operand(Operand::Left, left, &mut pool);
    pool.split = pool.segments.len();
    push_operand(Operand::Right, right, &mut pool);
    pool
}

/// Merge two pre-oriented operands ([`StagedRings::build`]) into a pool.
///
/// Identical layout and provenance to [`build_operand_pool`], but the costly
/// ring orientation is already done — this only re-materializes the thin
/// segment sweep pool and its parallel provenance from the cached points.
pub(super) fn build_operand_pool_staged(left: &StagedRings, right: &StagedRings) -> OperandPool {
    let capacity = left.edge_count + right.edge_count;
    let mut pool = OperandPool::with_scratch(capacity, left.rings.len() + right.rings.len());
    push_staged_operand(Operand::Left, left, &mut pool);
    pool.split = pool.segments.len();
    push_staged_operand(Operand::Right, right, &mut pool);
    pool
}

fn push_staged_operand(operand: Operand, staged: &StagedRings, pool: &mut OperandPool) {
    for ring in &staged.rings {
        let ring_id = pool.rings.len() as u32;
        let start = pool.segments.len();
        push_ring_edges(&ring.points, &mut pool.segments);
        let end = pool.segments.len();
        pool.operands.resize(end, operand);
        pool.ring_of.resize(end, ring_id);
        pool.rings.push(OrientedRing {
            operand,
            polygon: ring.polygon,
            ring: ring.ring,
            is_hole: ring.is_hole,
            points: Arc::clone(&ring.points),
            segments: start..end,
            probe: ring.probe,
        });
    }
}

fn push_operand<P: AsRef<Polygon>>(operand: Operand, polygons: &[P], pool: &mut OperandPool) {
    for (polygon_index, polygon) in polygons.iter().enumerate() {
        let polygon = polygon.as_ref();
        push_ring(
            operand,
            polygon_index,
            0,
            false,
            polygon.shell.coords(),
            pool,
        );
        for (hole_index, hole) in polygon.holes.iter().enumerate() {
            push_ring(
                operand,
                polygon_index,
                hole_index + 1,
                true,
                hole.coords(),
                pool,
            );
        }
    }
}

fn push_ring(
    operand: Operand,
    polygon_index: usize,
    ring_index: usize,
    is_hole: bool,
    coords: &CoordSeq,
    pool: &mut OperandPool,
) {
    let Some(points) = oriented_open_ring(coords, is_hole) else {
        return;
    };
    let ring_id = pool.rings.len() as u32;
    let start = pool.segments.len();
    push_ring_edges(&points, &mut pool.segments);
    let end = pool.segments.len();
    pool.operands.resize(end, operand);
    pool.ring_of.resize(end, ring_id);
    pool.rings.push(OrientedRing {
        operand,
        polygon: polygon_index as u32,
        ring: ring_index as u32,
        is_hole,
        points: points.into(),
        segments: start..end,
        // The uncached raw-`Shape` path: the relate probe falls back to its
        // own scanline.
        probe: None,
    });
}

fn push_ring_edges(points: &[XY], segments: &mut Vec<Segment>) {
    let n = points.len();
    for i in 0..n {
        segments.push(Segment {
            start: points[i],
            end: points[wrap_index(i + 1, n)],
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::REDUCE_SIMD_MIN;
    use crate::geometry::predicates::ring_contains_interior;

    fn division_reference(ring: &[XY], point: XY) -> bool {
        let mut closed = ring.to_vec();
        closed.push(ring[0]);
        ring_contains_interior(&CoordSeq::from(closed), point)
    }

    fn assert_vector_matches_division_reference(ring: &[XY], probes: &[XY]) {
        assert!(
            ring.len() >= REDUCE_SIMD_MIN,
            "test ring should exercise SIMD path",
        );
        for &point in probes {
            assert_eq!(
                ring_contains_interior_open(ring, point),
                division_reference(ring, point),
                "ring={ring:?} point={point:?}",
            );
        }
    }

    fn edge_probes(ring: &[XY]) -> Vec<XY> {
        let mut probes = Vec::with_capacity(ring.len() * 4);
        for i in 0..ring.len() {
            let a = ring[i];
            let b = ring[(i + 1) % ring.len()];
            let mid = XY::new(a.x.midpoint(b.x), a.y.midpoint(b.y));
            probes.push(a);
            probes.push(XY::new(a.x - 10.0, a.y));
            probes.push(mid);
            probes.push(XY::new(mid.x - 10.0, mid.y));
        }
        probes
    }

    #[test]
    fn open_ring_simd_matches_scalar_on_adversarial_rings() {
        let rings: Vec<Vec<XY>> = vec![
            vec![
                XY::new(0.0, 0.0),
                XY::new(4.0, 0.0),
                XY::new(8.0, 0.0),
                XY::new(8.0, 3.0),
                XY::new(6.0, 3.0),
                XY::new(6.0, 6.0),
                XY::new(2.0, 6.0),
                XY::new(2.0, 3.0),
                XY::new(0.0, 3.0),
            ],
            vec![
                XY::new(0.0, 0.0),
                XY::new(5.0, 5.0),
                XY::new(10.0, 0.0),
                XY::new(7.0, 0.0),
                XY::new(7.0, -4.0),
                XY::new(3.0, -4.0),
                XY::new(3.0, 0.0),
                XY::new(10.0, 5.0),
                XY::new(0.0, 5.0),
            ],
            vec![
                XY::new(-5.0, -1.0),
                XY::new(-2.0, -1.0),
                XY::new(-2.0, 2.0),
                XY::new(1.0, 2.0),
                XY::new(1.0, -1.0),
                XY::new(4.0, -1.0),
                XY::new(4.0, 4.0),
                XY::new(-5.0, 4.0),
            ],
        ];
        for ring in rings {
            let mut probes = edge_probes(&ring);
            probes.extend([
                XY::new(1.0, 1.0),
                XY::new(3.0, 0.0),
                XY::new(6.0, 0.0),
                XY::new(6.0, 3.0),
                XY::new(100.0, 0.0),
                XY::new(-100.0, 0.0),
            ]);
            assert_vector_matches_division_reference(&ring, &probes);
        }
    }

    #[test]
    fn open_ring_simd_matches_division_reference_on_review_divergences() {
        let huge = vec![
            XY::new(0.0, -1e155),
            XY::new(1e155, 1e155),
            XY::new(5e154, 1e155),
            XY::new(0.0, 1e155),
            XY::new(-5e154, 1e155),
            XY::new(-1e155, 1e155),
            XY::new(-1e155, 0.0),
            XY::new(-1e155, -1e155),
        ];
        let huge_probe = XY::new(1e154, 0.0);
        assert!(division_reference(&huge, huge_probe));
        assert_eq!(
            ring_contains_interior_open(&huge, huge_probe),
            division_reference(&huge, huge_probe),
        );

        let on_edge = vec![
            XY::new(0.0, 1.0),
            XY::new(0.0, -1.0),
            XY::new(1.0, -1.0),
            XY::new(1.0, -0.5),
            XY::new(1.0, 0.0),
            XY::new(1.0, 0.5),
            XY::new(1.0, 1.0),
            XY::new(0.5, 1.0),
        ];
        let on_edge_probe = XY::new(0.0, 0.0);
        assert!(division_reference(&on_edge, on_edge_probe));
        assert_eq!(
            ring_contains_interior_open(&on_edge, on_edge_probe),
            division_reference(&on_edge, on_edge_probe),
        );
    }

    /// The `(bx - ax) -> inf` overflow class: a triangle with opposite-sign
    /// coordinates at the top of the finite range used to give
    /// DIRECTION-DEPENDENT wrong parity (the division form materialized an
    /// infinite intersection X, counting or dropping a genuine crossing by
    /// edge order). The sign-form decision must classify exactly like the
    /// same triangle scaled to a safe magnitude, in both vertex orders.
    #[test]
    fn ray_crossing_parity_is_scale_and_direction_stable_at_extreme_coordinates() {
        for scale in [1e307_f64, 1e308_f64] {
            let s = scale / 1e308;
            let triangle = [
                XY::new(-1e308 * s, -1.0),
                XY::new(1e308 * s, 1.0),
                XY::new(1e308 * s, -1.0),
            ];
            let mut reversed = triangle;
            reversed.reverse();
            // (0, 0.5) sits ABOVE the long diagonal (its y at x=0 is ~0):
            // exterior. (5e307 * s, 0.0) sits below it and above the bottom
            // edge: interior.
            let exterior = [XY::new(0.0, 0.5), XY::new(-1e308 * s, 0.9999)];
            let interior = [XY::new(5e307 * s, 0.0)];
            for ring in [&triangle, &reversed] {
                for probe in exterior {
                    assert!(
                        !ring_contains_interior_open(ring, probe),
                        "scale={scale:e} ring={ring:?} probe={probe:?} must be exterior",
                    );
                }
                for probe in interior {
                    assert!(
                        ring_contains_interior_open(ring, probe),
                        "scale={scale:e} ring={ring:?} probe={probe:?} must be interior",
                    );
                }
            }
        }
    }

    /// The same overflow class through the SIMD chunk path: densify the safe
    /// bottom edge so the ring crosses the SIMD crossover while the huge
    /// opposite-sign diagonal lane demotes its chunk to the exact fallback.
    #[test]
    fn ray_crossing_simd_chunks_demote_untrusted_lanes_to_exact() {
        let n = 96_usize;
        let mut ring = vec![XY::new(-1e308, -1.0), XY::new(1e308, 1.0)];
        // Bottom edge from (1e308, -1) back towards (-1e308, -1), densified.
        for i in 0..n {
            let fraction = (i as f64) / (n as f64);
            ring.push(XY::new(1e308 * (1.0 - 2.0 * fraction), -1.0));
        }
        let columns: Vec<XY> = ring.clone();
        let xs: Vec<f64> = columns
            .iter()
            .map(|p| p.x)
            .chain(std::iter::once(columns[0].x))
            .collect();
        let ys: Vec<f64> = columns
            .iter()
            .map(|p| p.y)
            .chain(std::iter::once(columns[0].y))
            .collect();
        for (probe, expected) in [
            (XY::new(0.0, 0.5), false),
            (XY::new(5e307, 0.0), true),
            // Above the diagonal (its y at x = -5e307 is exactly -0.5).
            (XY::new(-5e307, -0.25), false),
            // Below the diagonal, above the bottom edge.
            (XY::new(-5e307, -0.75), true),
        ] {
            assert_eq!(
                ring_contains_interior_open(&ring, probe),
                expected,
                "AoS SIMD path probe={probe:?}",
            );
            assert_eq!(
                ring_contains_interior_columns(&xs, &ys, probe),
                expected,
                "columns SIMD path probe={probe:?}",
            );
            let (crossings, _) = ring_classify_crossings_columns(&xs, &ys, probe);
            assert_eq!(
                crossings % 2 == 1,
                expected,
                "classify columns path probe={probe:?}",
            );
        }
    }

    #[test]
    fn ray_crossing_simd_declines_wrong_nonzero_underflow_sign() {
        let mu = f64::from_bits(1);
        let splat = ReduceSimd::splat;
        assert!(
            ray_crossing_lanes(
                splat(1.0),
                splat(-4096.0 * mu),
                splat(-2.0_f64.powi(-12)),
                splat(6144.0 * mu),
                splat(f64::from_bits(0x3FD9_9733_3333_3333)),
                splat(2048.0 * mu),
            )
            .is_none()
        );
    }

    #[test]
    fn open_ring_simd_matches_division_reference_on_curated_rings() {
        let base_ring = |n: usize| {
            (0..n)
                .map(|i| {
                    let phase = ((i * 17) % 11) as f64 * 0.01 - 0.05;
                    let angle = (i as f64 + phase) * std::f64::consts::TAU / n as f64;
                    let radius = 8.0 + ((i * 37) % 19) as f64;
                    XY::new(angle.cos() * radius, angle.sin() * radius)
                })
                .collect::<Vec<_>>()
        };
        let mut rings = Vec::new();
        rings.push(("n256", base_ring(256), false, false));
        rings.push(("n263_tail", base_ring(263), false, false));
        let mut rotated_reversed = base_ring(257);
        rotated_reversed.rotate_left(37);
        rotated_reversed.reverse();
        rings.push(("n257_rotated_reversed", rotated_reversed, false, false));
        let mut swapped = base_ring(264);
        swapped.swap(1, 132);
        swapped.swap(2, 133);
        rings.push(("n264_swapped", swapped, false, false));
        let mut horizontal = base_ring(265);
        horizontal[3].y = horizontal[2].y;
        horizontal[4].y = horizontal[2].y;
        rings.push(("n265_horizontal", horizontal, false, false));
        let mut huge = base_ring(266);
        for point in &mut huge {
            point.x *= 1e150;
            point.y *= 1e150;
        }
        rings.push(("n266_huge", huge, true, false));
        let mut rounded_vertical = base_ring(267);
        for point in &mut rounded_vertical {
            point.x = point.x.round();
            point.y = point.y.round();
        }
        rounded_vertical[1].x = rounded_vertical[0].x;
        rounded_vertical[1].y = rounded_vertical[0].y - 2.0;
        rings.push(("n267_rounded_vertical", rounded_vertical, false, true));
        let mut combined = base_ring(271);
        combined.reverse();
        combined.swap(1, 135);
        combined.swap(2, 136);
        combined[3].y = combined[2].y;
        combined[4].y = combined[2].y;
        for point in &mut combined {
            point.x = (point.x * 1e150).round();
            point.y = (point.y * 1e150).round();
        }
        combined[1].x = combined[0].x;
        combined[1].y = combined[0].y - 2e150;
        rings.push(("n271_combined", combined, true, true));

        for (name, ring, huge, vertical) in rings {
            assert!(
                ring.len() >= crate::geometry::access::crossover::RAY_CROSSING,
                "{name} must exercise the ray-crossing SIMD crossover",
            );
            let mut probes = edge_probes(&ring);
            if huge {
                probes.extend([
                    XY::new(0.0, 0.0),
                    XY::new(1e151, 1e151),
                    XY::new(-1e151, -1e151),
                    XY::new(7e150, -1.1e151),
                ]);
            } else {
                probes.extend([
                    XY::new(0.0, 0.0),
                    XY::new(32.0, 32.0),
                    XY::new(-32.0, -32.0),
                    XY::new(7.0, -11.0),
                ]);
            }
            if vertical {
                probes.push(XY::new(ring[0].x, ring[0].y.midpoint(ring[1].y)));
            }
            assert_vector_matches_division_reference(&ring, &probes);
        }
    }
}
