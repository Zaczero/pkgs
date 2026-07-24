#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Typed signed-area lanes: measurement magnitude vs deterministic orientation
//! decisions. Topology never branches on a raw signed `f64`.

use std::simd::num::SimdFloat;

use crate::geometry::access::{REDUCE_LANES, ReduceSimd, crossover, simd_reduce_f64};
use crate::geometry::types::{Coordinates, Point, XY};
use crate::geometry::wrap_index;

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(transparent)]
pub(crate) struct AreaMeasure(f64);

const _: () = assert!(size_of::<AreaMeasure>() == size_of::<f64>());

impl AreaMeasure {
    pub(crate) const fn get(self) -> f64 {
        self.0
    }

    pub(crate) fn abs_cmp(self, other: Self) -> std::cmp::Ordering {
        self.0.abs().total_cmp(&other.0.abs())
    }
}

/// Signed measurement accumulator — no `Ord`/`PartialEq<f64>`/`Deref` so
/// `> 0.0` / `== 0.0` outside this module is a compile error.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub(crate) struct SignedAreaMeasure(f64);

const _: () = assert!(size_of::<SignedAreaMeasure>() == size_of::<f64>());

impl SignedAreaMeasure {
    const fn new(value: f64) -> Self {
        Self(value)
    }

    const fn magnitude(self) -> AreaMeasure {
        AreaMeasure(self.0.abs())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AreaSign {
    Negative,
    Zero,
    Positive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingWinding {
    Clockwise,
    CounterClockwise,
    Degenerate,
}

impl RingWinding {
    pub(crate) const fn is_ccw(self) -> bool {
        matches!(self, Self::CounterClockwise)
    }

    pub(crate) const fn is_degenerate(self) -> bool {
        matches!(self, Self::Degenerate)
    }

    /// Reverse the ring when its winding does not match `clockwise`.
    pub(crate) const fn reverse_for_clockwise(self, clockwise: bool) -> bool {
        matches!(self, Self::CounterClockwise) == clockwise
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RingDecisionArea {
    sign: AreaSign,
    magnitude: AreaMeasure,
}

impl RingDecisionArea {
    pub(crate) const fn sign(self) -> AreaSign {
        self.sign
    }

    pub(crate) const fn magnitude(self) -> AreaMeasure {
        self.magnitude
    }

    /// True when `self` is a better outer-face candidate than `other`
    /// (more negative signed area, or larger magnitude among negatives).
    pub(crate) fn prefers_as_outer_face_than(self, other: Self) -> bool {
        let (self_sign, other_sign) = (self.sign(), other.sign());
        if self_sign == AreaSign::Negative && other_sign != AreaSign::Negative {
            return true;
        }
        if other_sign == AreaSign::Negative {
            return false;
        }
        match (self_sign, other_sign) {
            (AreaSign::Negative, AreaSign::Negative) => {
                self.magnitude().abs_cmp(other.magnitude()).is_gt()
            },
            (AreaSign::Zero, AreaSign::Positive) => true,
            (AreaSign::Positive, AreaSign::Positive) => {
                self.magnitude().abs_cmp(other.magnitude()).is_lt()
            },
            _ => false,
        }
    }
}

const fn area_sign_to_winding(sign: AreaSign) -> RingWinding {
    match sign {
        AreaSign::Negative => RingWinding::Clockwise,
        AreaSign::Zero => RingWinding::Degenerate,
        AreaSign::Positive => RingWinding::CounterClockwise,
    }
}

// --- Shewchuk expansion substrate (adapted from the `robust` crate, v1.2.0;
// Jonathan Richard Shewchuk, "Adaptive Precision Floating-Point Arithmetic and
// Fast Robust Geometric Predicates", 1997) -----------------------------------

const SPLITTER: f64 = 134_217_729.0;
const EPSILON: f64 = f64::EPSILON;
const ERRBOUND: f64 = (3.0 + 16.0 * EPSILON) * EPSILON;

fn split(a: f64) -> (f64, f64) {
    let c = SPLITTER * a;
    let abig = c - a;
    let ahi = c - abig;
    let alo = a - ahi;
    (ahi, alo)
}

fn two_product(a: f64, b: f64) -> (f64, f64) {
    let x = a * b;
    (x, two_product_tail(a, b, x))
}

fn two_product_tail(a: f64, b: f64, x: f64) -> f64 {
    let (ahi, alo) = split(a);
    let (bhi, blo) = split(b);
    let err1 = x - ahi * bhi;
    let err2 = err1 - alo * bhi;
    let err3 = err2 - ahi * blo;
    alo * blo - err3
}

fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let x = a + b;
    (x, two_sum_tail(a, b, x))
}

fn two_sum_tail(a: f64, b: f64, x: f64) -> f64 {
    let bvirt = x - a;
    let avirt = x - bvirt;
    let bround = b - bvirt;
    let around = a - avirt;
    around + bround
}

fn two_diff(a: f64, b: f64) -> (f64, f64) {
    let x = a - b;
    (x, two_diff_tail(a, b, x))
}

fn two_diff_tail(a: f64, b: f64, x: f64) -> f64 {
    let bvirt = a - x;
    let avirt = x + bvirt;
    let bround = bvirt - b;
    let around = a - avirt;
    around + bround
}

fn fast_two_sum(a: f64, b: f64) -> (f64, f64) {
    let x = a + b;
    (x, fast_two_sum_tail(a, b, x))
}

fn fast_two_sum_tail(a: f64, b: f64, x: f64) -> f64 {
    let bvirt = x - a;
    b - bvirt
}

fn estimate(e: &[f64]) -> f64 {
    let mut q = e[0];
    for cur in &e[1..] {
        q += *cur;
    }
    q
}

fn fast_expansion_sum_zeroelim(e: &[f64], f: &[f64], h: &mut [f64]) -> usize {
    if f.is_empty() {
        h[..e.len()].copy_from_slice(e);
        return e.len();
    }
    if e.is_empty() {
        h[..f.len()].copy_from_slice(f);
        return f.len();
    }
    let mut enow = e[0];
    let mut fnow = f[0];
    let mut eindex = 0;
    let mut findex = 0;
    let mut q;
    let mut hh;
    if (fnow > enow) == (fnow > -enow) {
        q = enow;
        eindex += 1;
    } else {
        q = fnow;
        findex += 1;
    }

    let mut hindex = 0;
    if eindex < e.len() && findex < f.len() {
        enow = e[eindex];
        fnow = f[findex];
        let (qnew, tail) = if (fnow > enow) == (fnow > -enow) {
            eindex += 1;
            fast_two_sum(enow, q)
        } else {
            findex += 1;
            fast_two_sum(fnow, q)
        };
        q = qnew;
        hh = tail;
        if hh != 0.0 {
            h[hindex] = hh;
            hindex += 1;
        }

        while eindex < e.len() && findex < f.len() {
            enow = e[eindex];
            fnow = f[findex];
            let (qnew, tail) = if (fnow > enow) == (fnow > -enow) {
                eindex += 1;
                two_sum(q, enow)
            } else {
                findex += 1;
                two_sum(q, fnow)
            };
            q = qnew;
            hh = tail;
            if hh != 0.0 {
                h[hindex] = hh;
                hindex += 1;
            }
        }
    }

    while eindex < e.len() {
        enow = e[eindex];
        let (qnew, tail) = two_sum(q, enow);
        q = qnew;
        hh = tail;
        eindex += 1;
        if hh != 0.0 {
            h[hindex] = hh;
            hindex += 1;
        }
    }

    while findex < f.len() {
        fnow = f[findex];
        let (qnew, tail) = two_sum(q, fnow);
        q = qnew;
        hh = tail;
        findex += 1;
        if hh != 0.0 {
            h[hindex] = hh;
            hindex += 1;
        }
    }

    if q != 0.0 || hindex == 0 {
        h[hindex] = q;
        hindex += 1;
    }
    hindex
}

/// Scale/origin for the filtered ring-sign path — no column allocation.
/// Returns `(ox, oy, scale)` with `scale == 0.0` for a fully-collapsed ring.
fn ring_normalize_params(xs: &[f64], ys: &[f64]) -> (f64, f64, f64) {
    if xs.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let ox = xs[0];
    let oy = ys[0];
    let max_delta = xs
        .iter()
        .zip(ys.iter())
        .map(|(&x, &y)| (x - ox).abs().max((y - oy).abs()))
        .fold(0.0_f64, f64::max);
    if max_delta == 0.0 {
        return (ox, oy, 0.0);
    }
    let scale = if max_delta.is_finite() && max_delta > 1.0 {
        1.0 / max_delta
    } else {
        1.0
    };
    (ox, oy, scale)
}

/// Expansion sum of cross terms for the uncertain filtered path. Columns are
/// already origin-normalized (and typically scale-normalized). Two ping-pong
/// buffers avoid cloning the live expansion prefix on every edge.
fn expansion_cross_terms_edges(
    xs: &[f64],
    ys: &[f64],
    edge_count: usize,
    mut edge_at: impl FnMut(usize) -> (usize, usize),
) -> Vec<f64> {
    if edge_count == 0 {
        return vec![0.0];
    }
    let (start_idx, _) = edge_at(0);
    let origin_x = xs[start_idx];
    let origin_y = ys[start_idx];
    let mut live = vec![0.0];
    let mut scratch = Vec::new();
    for edge in 0..edge_count {
        let (from, to) = edge_at(edge);
        let ax = xs[from] - origin_x;
        let ay = ys[from] - origin_y;
        let bx = xs[to] - origin_x;
        let by = ys[to] - origin_y;
        let (axby1, axby0) = two_product(ax, by);
        let (aybx1, aybx0) = two_product(ay, bx);
        let (s1, s0) = two_diff(axby1, aybx1);
        let terms = [s0, s1, axby0, aybx0];
        scratch.clear();
        scratch.resize(live.len() + terms.len(), 0.0);
        let hlen = fast_expansion_sum_zeroelim(&live, &terms, &mut scratch);
        // Ping-pong: live expansion is `scratch[..hlen]`; swap so `live` holds
        // it and `scratch` keeps capacity for the next edge (no `to_vec`).
        std::mem::swap(&mut live, &mut scratch);
        live.truncate(hlen);
    }
    live
}

fn classify_half_area(half: f64) -> AreaSign {
    if half > 0.0 {
        AreaSign::Positive
    } else if half < 0.0 {
        AreaSign::Negative
    } else {
        AreaSign::Zero
    }
}

/// Filtered ring-sign over an edge enumeration. Ordinary (decisive) rings
/// allocate nothing; expansion buffers are built only on the uncertain path.
fn exact_half_area_sign_filtered_edges(
    xs: &[f64],
    ys: &[f64],
    edge_count: usize,
    mut edge_at: impl FnMut(usize) -> (usize, usize),
) -> AreaSign {
    if edge_count == 0 {
        return AreaSign::Zero;
    }
    let (ox, oy, scale) = ring_normalize_params(xs, ys);
    if scale == 0.0 {
        return AreaSign::Zero;
    }
    let mut approx = 0.0_f64;
    let mut max_term = 0.0_f64;
    for edge in 0..edge_count {
        let (i, j) = edge_at(edge);
        let axi = (xs[i] - ox) * scale;
        let ayi = (ys[i] - oy) * scale;
        let bxj = (xs[j] - ox) * scale;
        let byj = (ys[j] - oy) * scale;
        let term = axi * byj - bxj * ayi;
        approx += term;
        max_term = max_term.max(term.abs());
    }
    let half = approx / 2.0;
    let bound = ERRBOUND * max_term * edge_count as f64;
    if half > bound {
        return AreaSign::Positive;
    }
    if half < -bound {
        return AreaSign::Negative;
    }
    if max_term == 0.0 {
        return AreaSign::Zero;
    }
    // Uncertain: materialize normalized columns once for the expansion path
    // (scale/origin already known). Ping-pong expansion avoids per-edge clone.
    let mut nxs = Vec::with_capacity(xs.len());
    let mut nys = Vec::with_capacity(ys.len());
    for index in 0..xs.len() {
        nxs.push((xs[index] - ox) * scale);
        nys.push((ys[index] - oy) * scale);
    }
    let expansion = expansion_cross_terms_edges(&nxs, &nys, edge_count, edge_at);
    classify_half_area(estimate(&expansion) / 2.0)
}

pub(crate) fn exact_ring_area_sign_columns(xs: &[f64], ys: &[f64], pairs: usize) -> AreaSign {
    if pairs == 0 {
        return AreaSign::Zero;
    }
    exact_half_area_sign_filtered_edges(xs, ys, pairs, |index| (index, index + 1))
}

fn exact_open_xy_sign(points: &[XY]) -> AreaSign {
    let count = points.len();
    if count < 2 {
        return AreaSign::Zero;
    }
    // Work on XY directly — no SoA transpose for the ordinary filtered path.
    // Build thin column views only if the expansion fallback fires.
    let mut approx = 0.0_f64;
    let mut max_term = 0.0_f64;
    let ox = points[0].x;
    let oy = points[0].y;
    let max_delta = points
        .iter()
        .map(|p| (p.x - ox).abs().max((p.y - oy).abs()))
        .fold(0.0_f64, f64::max);
    if max_delta == 0.0 {
        return AreaSign::Zero;
    }
    let scale = if max_delta.is_finite() && max_delta > 1.0 {
        1.0 / max_delta
    } else {
        1.0
    };
    for index in 0..count {
        let i = index;
        let j = wrap_index(index + 1, count);
        let axi = (points[i].x - ox) * scale;
        let ayi = (points[i].y - oy) * scale;
        let bxj = (points[j].x - ox) * scale;
        let byj = (points[j].y - oy) * scale;
        let term = axi * byj - bxj * ayi;
        approx += term;
        max_term = max_term.max(term.abs());
    }
    let half = approx / 2.0;
    let bound = ERRBOUND * max_term * count as f64;
    if half > bound {
        return AreaSign::Positive;
    }
    if half < -bound {
        return AreaSign::Negative;
    }
    if max_term == 0.0 {
        return AreaSign::Zero;
    }
    let xs: Vec<f64> = points.iter().map(|p| (p.x - ox) * scale).collect();
    let ys: Vec<f64> = points.iter().map(|p| (p.y - oy) * scale).collect();
    let expansion = expansion_cross_terms_edges(&xs, &ys, count, |index| {
        (index, wrap_index(index + 1, count))
    });
    classify_half_area(estimate(&expansion) / 2.0)
}

fn exact_open_point_sign(points: &[Point]) -> AreaSign {
    let count = points.len();
    if count < 2 {
        return AreaSign::Zero;
    }
    // Point slices are rare vs arrangement XY cycles; one column gather still
    // beats the prior path that allocated columns AND the full edge list AND
    // always-normalized copies before the filter.
    let xs: Vec<f64> = points.iter().map(|point| point.x).collect();
    let ys: Vec<f64> = points.iter().map(|point| point.y).collect();
    exact_half_area_sign_filtered_edges(&xs, &ys, count, |index| {
        (index, wrap_index(index + 1, count))
    })
}

/// Ring vertex-pair counts below this use the scalar fold — LLVM auto-vectorizes
/// it to packed `mulpd`/`addpd` (asm-verified) with no per-ring horizontal
/// reduce; at or above it the 512-bit `simd_reduce_f64` path's extra ILP wins
/// (measured: scalar ties to ~64 verts, SIMD wins 15–25% at 256+). Centralized
/// in [`crossover`].
const SHOELACE_SIMD_MIN: usize = crossover::OFFSET_PAIR_MEASURE;

pub(crate) fn shoelace_measure_columns(xs: &[f64], ys: &[f64], pairs: usize) -> SignedAreaMeasure {
    // Small rings (the overwhelmingly common case — most polygons have well
    // under ~64 vertices): a tight reassociation-friendly scalar fold over two
    // offset slices. LLVM auto-vectorizes it at the natural width and elides
    // bounds checks (the offset slices are length-`pairs`). The forced 512-bit
    // path below pays per-ring tail padding + a horizontal `reduce_sum`, which
    // only amortizes on long rings — many tiny per-ring reductions are NOT
    // stream-shaped (see the 512-bit-for-streams-only float policy).
    if pairs < SHOELACE_SIMD_MIN {
        let cross = xs[..pairs]
            .iter()
            .zip(&xs[1..=pairs])
            .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
            .fold(0.0_f64, |area, ((&x0, &x1), (&y0, &y1))| {
                area.algebraic_add(x0.algebraic_mul(y1).algebraic_sub(x1.algebraic_mul(y0)))
            });
        return SignedAreaMeasure::new(cross / 2.0);
    }
    let (x0, _) = xs[..pairs].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..pairs].as_chunks::<REDUCE_LANES>();
    let (y1, _) = ys[1..=pairs].as_chunks::<REDUCE_LANES>();
    let simd_fold = |mut acc: ReduceSimd, start: usize| {
        let chunk = start / REDUCE_LANES;
        let (x0, x1) = (
            ReduceSimd::from_array(x0[chunk]),
            ReduceSimd::from_array(x1[chunk]),
        );
        let (y0, y1) = (
            ReduceSimd::from_array(y0[chunk]),
            ReduceSimd::from_array(y1[chunk]),
        );
        acc += x0 * y1 - x1 * y0;
        acc
    };
    let lanes = x0.len() * REDUCE_LANES;
    let value = simd_reduce_f64(
        pairs,
        ReduceSimd::splat(0.0),
        (),
        |acc, (), start| (simd_fold(acc, start), ()),
        |(), range| {
            if range.start != 0 {
                debug_assert_eq!(range.start, lanes);
            }
        },
        |mut acc, ()| {
            if lanes < pairs {
                let (tx0, tx1) = (
                    ReduceSimd::load_or_default(&xs[lanes..pairs]),
                    ReduceSimd::load_or_default(&xs[lanes + 1..=pairs]),
                );
                let (ty0, ty1) = (
                    ReduceSimd::load_or_default(&ys[lanes..pairs]),
                    ReduceSimd::load_or_default(&ys[lanes + 1..=pairs]),
                );
                acc += tx0 * ty1 - tx1 * ty0;
            }
            acc.reduce_sum() / 2.0
        },
    );
    SignedAreaMeasure::new(value)
}

pub(crate) fn ring_area_measure_columns(xs: &[f64], ys: &[f64], pairs: usize) -> AreaMeasure {
    shoelace_measure_columns(xs, ys, pairs).magnitude()
}

pub(crate) fn ring_area_measure<C: Coordinates + ?Sized>(points: &C) -> AreaMeasure {
    let pairs = points.coord_count().saturating_sub(1);
    if pairs == 0 {
        return AreaMeasure(0.0);
    }
    if let Some((xs, ys)) = points.xy_columns() {
        ring_area_measure_columns(xs, ys, pairs)
    } else {
        let count = points.coord_count();
        let mut xs = Vec::with_capacity(count);
        let mut ys = Vec::with_capacity(count);
        for point in points.iter_coords() {
            xs.push(point.x);
            ys.push(point.y);
        }
        ring_area_measure_columns(&xs, &ys, pairs)
    }
}

pub(crate) fn ring_decision_area<C: Coordinates + ?Sized>(points: &C) -> RingDecisionArea {
    let pairs = points.coord_count().saturating_sub(1);
    if pairs == 0 {
        return RingDecisionArea {
            sign: AreaSign::Zero,
            magnitude: AreaMeasure(0.0),
        };
    }
    let sign = if let Some((xs, ys)) = points.xy_columns() {
        exact_ring_area_sign_columns(xs, ys, pairs)
    } else {
        let count = points.coord_count();
        let mut xs = Vec::with_capacity(count);
        let mut ys = Vec::with_capacity(count);
        for point in points.iter_coords() {
            xs.push(point.x);
            ys.push(point.y);
        }
        exact_ring_area_sign_columns(&xs, &ys, pairs)
    };
    RingDecisionArea {
        sign,
        magnitude: ring_area_measure(points),
    }
}

pub(crate) fn ring_winding<C: Coordinates + ?Sized>(points: &C) -> RingWinding {
    area_sign_to_winding(ring_decision_area(points).sign())
}

pub(crate) fn closed_columns_winding(xs: &[f64], ys: &[f64], pairs: usize) -> RingWinding {
    area_sign_to_winding(exact_ring_area_sign_columns(xs, ys, pairs))
}

pub(crate) fn open_xy_cycle_winding(points: &[XY]) -> RingWinding {
    area_sign_to_winding(exact_open_xy_sign(points))
}

pub(crate) fn open_point_cycle_winding(points: &[Point]) -> RingWinding {
    area_sign_to_winding(exact_open_point_sign(points))
}

pub(crate) fn open_xy_cycle_decision(points: &[XY]) -> RingDecisionArea {
    RingDecisionArea {
        sign: exact_open_xy_sign(points),
        magnitude: open_xy_cycle_magnitude(points),
    }
}

pub(crate) fn open_point_cycle_decision(points: &[Point]) -> RingDecisionArea {
    RingDecisionArea {
        sign: exact_open_point_sign(points),
        magnitude: open_point_cycle_magnitude(points),
    }
}

fn open_cycle_magnitude_columns(xs: &[f64], ys: &[f64]) -> AreaMeasure {
    if xs.len() < 2 {
        return AreaMeasure(0.0);
    }
    let ox = xs[0];
    let oy = ys[0];
    let max_delta = xs
        .iter()
        .zip(ys.iter())
        .map(|(&x, &y)| (x - ox).abs().max((y - oy).abs()))
        .fold(0.0_f64, f64::max);
    if max_delta == 0.0 {
        return AreaMeasure(0.0);
    }
    let (scale, unscale) = if max_delta.is_finite() && max_delta > 1.0 {
        let scale = 1.0 / max_delta;
        (scale, 1.0 / (scale * scale))
    } else {
        (1.0, 1.0)
    };
    let pairs = xs.len();
    let mut sum = 0.0_f64;
    for index in 0..pairs {
        let next = wrap_index(index + 1, pairs);
        let ax = (xs[index] - ox) * scale;
        let ay = (ys[index] - oy) * scale;
        let bx = (xs[next] - ox) * scale;
        let by = (ys[next] - oy) * scale;
        sum = sum.algebraic_add(ax.algebraic_mul(by).algebraic_sub(bx.algebraic_mul(ay)));
    }
    AreaMeasure((sum * unscale / 2.0).abs())
}

pub(crate) fn open_point_cycle_magnitude(points: &[Point]) -> AreaMeasure {
    if points.len() < 2 {
        return AreaMeasure(0.0);
    }
    let xs: Vec<f64> = points.iter().map(|point| point.x).collect();
    let ys: Vec<f64> = points.iter().map(|point| point.y).collect();
    open_cycle_magnitude_columns(&xs, &ys)
}

fn open_xy_cycle_magnitude(points: &[XY]) -> AreaMeasure {
    if points.len() < 2 {
        return AreaMeasure(0.0);
    }
    let xs: Vec<f64> = points.iter().map(|point| point.x).collect();
    let ys: Vec<f64> = points.iter().map(|point| point.y).collect();
    open_cycle_magnitude_columns(&xs, &ys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_winding_stable_near_cancellation_when_measurement_may_differ() {
        let xs: Vec<f64> = vec![
            0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0,
        ];
        let ys: Vec<f64> = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
        ];
        let pairs = xs.len() - 1;
        assert!(pairs >= REDUCE_LANES);
        let winding = closed_columns_winding(&xs, &ys, pairs);
        assert_eq!(winding, RingWinding::CounterClockwise);
        let measure = ring_area_measure_columns(&xs, &ys, pairs).get();
        let scalar_reference = xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + (x0 * y1 - x1 * y0)
            })
            / 2.0;
        assert!(scalar_reference > 0.0);
        assert_eq!(measure.to_bits(), scalar_reference.to_bits());
    }

    #[test]
    fn shoelace_simd_path_matches_scalar_reference_on_large_ring() {
        // A ring big enough (`pairs >= SHOELACE_SIMD_MIN`) to take the 512-bit
        // path — guards that the small-ring scalar fast path and the SIMD body
        // agree (the threshold split must not change results).
        let k = SHOELACE_SIMD_MIN + 9;
        let mut xs: Vec<f64> = (0..k)
            .map(|i| (i as f64 * std::f64::consts::TAU / k as f64).cos() * 100.0)
            .collect();
        let mut ys: Vec<f64> = (0..k)
            .map(|i| (i as f64 * std::f64::consts::TAU / k as f64).sin() * 100.0)
            .collect();
        xs.push(xs[0]);
        ys.push(ys[0]);
        let pairs = xs.len() - 1;
        assert!(pairs >= SHOELACE_SIMD_MIN);
        let simd = ring_area_measure_columns(&xs, &ys, pairs).get();
        let scalar = xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + (x0 * y1 - x1 * y0)
            })
            .abs()
            / 2.0;
        assert!(
            (simd - scalar).abs() <= 1e-6 * scalar,
            "simd={simd} scalar={scalar}"
        );
    }
}
