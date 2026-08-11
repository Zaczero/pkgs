//! Typed signed-area lanes: measurement magnitude vs deterministic orientation
//! decisions. Topology never branches on a raw signed `f64`.

use std::simd::num::SimdFloat as _;

use crate::geometry::access::{REDUCE_LANES, ReduceSimd, crossover, simd_reduce_f64};
use crate::geometry::tessellation::exact::two_sum;
use crate::geometry::types::{Coordinates, Point, XY};
use crate::geometry::{axis_pow2_scale, scaled_residual, unscale_area, wrap_index};

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

    /// True when `self` is a better outer-face candidate than `other`.
    pub(crate) fn prefers_as_outer_face_than(self, other: Self) -> bool {
        let (self_sign, other_sign) = (self.sign(), other.sign());
        if self_sign == AreaSign::Negative && other_sign != AreaSign::Negative {
            return true;
        }
        if other_sign == AreaSign::Negative {
            return false;
        }
        match (self_sign, other_sign) {
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

const EPSILON: f64 = f64::EPSILON;
const ERRBOUND: f64 = (3.0 + 16.0 * EPSILON) * EPSILON;

fn two_product(a: f64, b: f64) -> (f64, f64) {
    let product = a * b;
    // Dekker splitting multiplies a finite near-max operand by 2^27 before
    // it can recover the tail. That intermediate overflows even when the
    // final product is ordinary (e.g. 2^1022 * 2^-1022), turning the exact
    // rescue into NaN. FMA computes the exact residual without that enlarged
    // intermediary.
    let tail = if product.is_finite() {
        a.mul_add(b, -product)
    } else {
        0.0
    };
    (product, tail)
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

/// Add two non-overlapping expansions without exposing their scratch storage to
/// callers.  The exact-measurement fallbacks are deliberately cold, so keeping
/// the common arithmetic substrate here is clearer than teaching every caller
/// its own partial-expansion representation.
fn expansion_sum(left: &[f64], right: &[f64]) -> Vec<f64> {
    let mut output = vec![0.0; left.len() + right.len()];
    let len = fast_expansion_sum_zeroelim(left, right, &mut output);
    output.truncate(len);
    output
}

fn exact_product_expansion(left: f64, right: f64) -> Vec<f64> {
    let (head, tail) = two_product(left, right);
    if tail == 0.0 {
        vec![head]
    } else {
        vec![tail, head]
    }
}

/// Exact expansion for the shared-frame residual `value * scale - origin *
/// scale`.  The ordinary frame may round that subtraction to zero before the
/// exact shoelace fallback sees it (the `2^-28` triangle is the smallest
/// useful counterexample), so the fallback must retain both subtraction tails.
fn exact_scaled_residual(value: f64, origin: f64, scale: f64) -> Vec<f64> {
    let value = exact_product_expansion(value, scale);
    let mut origin = exact_product_expansion(origin, scale);
    for term in &mut origin {
        *term = -*term;
    }
    expansion_sum(&value, &origin)
}

fn expansion_product(left: &[f64], right: &[f64]) -> Vec<f64> {
    let mut product = vec![0.0];
    for &left_part in left {
        for &right_part in right {
            let term = exact_product_expansion(left_part, right_part);
            product = expansion_sum(&product, &term);
        }
    }
    product
}

/// Exact local triangle-fan sums for the rare case where a shared `f64`
/// shoelace frame rounded a representable area to zero.  Inputs are already
/// translated and power-of-two scaled by the caller's one polygon-wide frame;
/// the returned values are therefore all in that same local coordinate system.
///
/// The three values are twice-area and the two corresponding centroid
/// numerators (`Σ (xᵢ+xᵢ₊₁) cross`, likewise `y`).  Keeping them together is
/// important: area and centroid must never disagree about whether a polygon
/// has a representably nonzero interior.
pub(crate) fn exact_ring_area_centroid_sums_local(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> (f64, f64, f64) {
    let mut area = vec![0.0];
    let mut moment_x = vec![0.0];
    let mut moment_y = vec![0.0];
    for index in 0..pairs {
        let x0 = exact_scaled_residual(xs[index], ox, sx);
        let y0 = exact_scaled_residual(ys[index], oy, sy);
        let x1 = exact_scaled_residual(xs[index + 1], ox, sx);
        let y1 = exact_scaled_residual(ys[index + 1], oy, sy);
        let forward = expansion_product(&x0, &y1);
        let mut reverse = expansion_product(&y0, &x1);
        for term in &mut reverse {
            *term = -*term;
        }
        let cross = expansion_sum(&forward, &reverse);
        area = expansion_sum(&area, &cross);
        let x_sum = expansion_sum(&x0, &x1);
        let y_sum = expansion_sum(&y0, &y1);
        moment_x = expansion_sum(&moment_x, &expansion_product(&cross, &x_sum));
        moment_y = expansion_sum(&moment_y, &expansion_product(&cross, &y_sum));
    }
    (estimate(&area), estimate(&moment_x), estimate(&moment_y))
}

/// Axis residual extent for scale selection. Prefer the finite `|x - origin|`
/// span; when subtraction overflows, fall back to `max(|x|, |origin|)` so a
/// power-of-two scale still maps `x*s - origin*s` into a normal range.
fn axis_extent_for_scale(values: impl IntoIterator<Item = f64>, origin: f64) -> (f64, bool) {
    let mut max_delta = 0.0_f64;
    let mut max_abs = origin.abs();
    let mut overflow = false;
    for x in values {
        max_abs = max_abs.max(x.abs());
        let d = (x - origin).abs();
        if d.is_finite() {
            max_delta = max_delta.max(d);
        } else {
            overflow = true;
        }
    }
    if overflow {
        (max_abs, true)
    } else {
        (max_delta, false)
    }
}

/// Scale/origin for the filtered ring-sign path — no column allocation.
/// Returns `(ox, oy, sx, sy)` with `sx == 0.0` for a fully-collapsed ring
/// (both axes zero). Per-axis positive power-of-two scales preserve sign of
/// the shoelace cross product while keeping each edge term normal.
fn ring_normalize_params(xs: &[f64], ys: &[f64]) -> (f64, f64, f64, f64) {
    if xs.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    let ox = xs[0];
    let oy = ys[0];
    let (max_dx, overflow_x) = axis_extent_for_scale(xs.iter().copied(), ox);
    let (max_dy, overflow_y) = axis_extent_for_scale(ys.iter().copied(), oy);
    if !overflow_x && !overflow_y && max_dx == 0.0 && max_dy == 0.0 {
        return (ox, oy, 0.0, 0.0);
    }
    (ox, oy, axis_pow2_scale(max_dx), axis_pow2_scale(max_dy))
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
        // Merge the two exact product expansions before adding the edge to
        // the live expansion.  Appending loose terms loses the increasing-
        // magnitude invariant that `fast_expansion_sum_zeroelim` requires;
        // the second product also retains its subtraction sign.
        let axby = [axby0, axby1];
        let aybx = [-aybx0, -aybx1];
        let mut term = [0.0; 4];
        let term_len = fast_expansion_sum_zeroelim(&axby, &aybx, &mut term);
        scratch.clear();
        scratch.resize(live.len() + term_len, 0.0);
        let hlen = fast_expansion_sum_zeroelim(&live, &term[..term_len], &mut scratch);
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
    let (ox, oy, sx, sy) = ring_normalize_params(xs, ys);
    if sx == 0.0 {
        return AreaSign::Zero;
    }
    let mut approx = 0.0_f64;
    let mut max_term = 0.0_f64;
    for edge in 0..edge_count {
        let (i, j) = edge_at(edge);
        let axi = scaled_residual(xs[i], ox, sx);
        let ayi = scaled_residual(ys[i], oy, sy);
        let bxj = scaled_residual(xs[j], ox, sx);
        let byj = scaled_residual(ys[j], oy, sy);
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
    // Uncertain: materialize normalized columns once for the expansion path
    // (scale/origin already known). This includes `max_term == 0.0`: unequal
    // exact product tails can survive even when every rounded cross term is
    // zero. Ping-pong expansion avoids per-edge clone.
    let mut nxs = Vec::with_capacity(xs.len());
    let mut nys = Vec::with_capacity(ys.len());
    for index in 0..xs.len() {
        nxs.push(scaled_residual(xs[index], ox, sx));
        nys.push(scaled_residual(ys[index], oy, sy));
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
    // Build thin column views only if the expansion fallback fires. Per-axis
    // power-of-two scales (same rule as closed rings) keep tiny and mixed-axis
    // rings decisive.
    let mut approx = 0.0_f64;
    let mut max_term = 0.0_f64;
    let ox = points[0].x;
    let oy = points[0].y;
    let (max_dx, overflow_x) = axis_extent_for_scale(points.iter().map(|p| p.x), ox);
    let (max_dy, overflow_y) = axis_extent_for_scale(points.iter().map(|p| p.y), oy);
    if !overflow_x && !overflow_y && max_dx == 0.0 && max_dy == 0.0 {
        return AreaSign::Zero;
    }
    let sx = axis_pow2_scale(max_dx);
    let sy = axis_pow2_scale(max_dy);
    for index in 0..count {
        let i = index;
        let j = wrap_index(index + 1, count);
        let axi = scaled_residual(points[i].x, ox, sx);
        let ayi = scaled_residual(points[i].y, oy, sy);
        let bxj = scaled_residual(points[j].x, ox, sx);
        let byj = scaled_residual(points[j].y, oy, sy);
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
    let xs: Vec<f64> = points
        .iter()
        .map(|p| scaled_residual(p.x, ox, sx))
        .collect();
    let ys: Vec<f64> = points
        .iter()
        .map(|p| scaled_residual(p.y, oy, sy))
        .collect();
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
    // Translation-invariant origin shift: products become O(ring_extent²)
    // instead of O(coord²), so large absolute coordinates (e.g. UTM) no longer
    // cancel catastrophic intermediate cross-products. Algebraically identical
    // on a closed ring (ox/oy terms telescope); sign is preserved.
    if pairs == 0 {
        return SignedAreaMeasure::new(0.0);
    }
    let ox = xs[0];
    let oy = ys[0];
    let value = shoelace_shifted(xs, ys, pairs, ox, oy, 1.0, 1.0);
    if value.is_finite()
        && value != 0.0
        && !shoelace_shifted_needs_exact(xs, ys, pairs, ox, oy, 1.0, 1.0, value)
    {
        return SignedAreaMeasure::new(value);
    }
    // A finite rounded result is not necessarily a measured result: the same
    // exact product tails that decide winding can hold a representable area,
    // or can show that a nonzero approximation has lost every reliable bit.
    // The expansion is the one area-magnitude owner used by public measure,
    // open-cycle decisions, and triangulation admission.
    if value.is_finite() {
        return SignedAreaMeasure::new(exact_half_area_measure_columns(xs, ys, pairs));
    }
    // Extreme-but-finite rescue: scale each axis independently, form residuals
    // as `x*sx - origin_x*sx` / `y*sy - origin_y*sy` (never subtract then
    // scale — that overflows on `±1e308`), then divide in two steps. An
    // isotropic scale makes a valid huge-X/tiny-Y rectangle evaluate as
    // `0 * inf`; per-axis scaling keeps both residual columns normal.
    // Ordinary inputs never enter here (finite above).
    let mut max_abs_x = 0.0_f64;
    let mut max_abs_y = 0.0_f64;
    for (&x, &y) in xs[..=pairs].iter().zip(&ys[..=pairs]) {
        max_abs_x = max_abs_x.max(x.abs());
        max_abs_y = max_abs_y.max(y.abs());
    }
    if !(max_abs_x.is_finite() && max_abs_y.is_finite()) {
        return SignedAreaMeasure::new(value);
    }
    let sx = axis_pow2_scale(max_abs_x);
    let sy = axis_pow2_scale(max_abs_y);
    let scaled = shoelace_shifted(xs, ys, pairs, ox, oy, sx, sy);
    if scaled.is_finite() && !shoelace_shifted_needs_exact(xs, ys, pairs, ox, oy, sx, sy, scaled) {
        SignedAreaMeasure::new(unscale_area(scaled, sx, sy))
    } else {
        SignedAreaMeasure::new(exact_half_area_measure_columns(xs, ys, pairs))
    }
}

/// Exact-expansion signed half-area for the rare finite-zero measurement
/// fallback. Its power-of-two frame is undone in one exponent operation, so a
/// normal final result never passes through a lossy subnormal intermediate.
fn exact_half_area_measure_columns(xs: &[f64], ys: &[f64], pairs: usize) -> f64 {
    exact_cycle_area_measure_columns(xs, ys, pairs, |index| (index, index + 1))
}

/// The one exact area-magnitude owner for closed rings and open cycles.
///
/// Callers differ only in their final edge (`n - 1 → 0` for an open cycle),
/// never in the normalization or accumulation that establishes magnitude.
/// Keeping that distinction as data prevents a fast helper, a topology owner,
/// and a preflight from gradually adopting incompatible loss rules.
fn exact_cycle_area_measure_columns(
    xs: &[f64],
    ys: &[f64],
    edge_count: usize,
    edge_at: impl FnMut(usize) -> (usize, usize),
) -> f64 {
    let (ox, oy, sx, sy) = ring_normalize_params(xs, ys);
    if sx == 0.0 {
        return 0.0;
    }
    let mut expansion = vec![0.0];
    let mut edge_at = edge_at;
    for edge in 0..edge_count {
        let (start, end) = edge_at(edge);
        // Keep the product and subtraction tails from the affine frame.  A
        // rounded `scaled_residual` here would make this supposedly exact
        // owner disagree with the polygon fallback on thin rotated cycles.
        let x0 = exact_scaled_residual(xs[start], ox, sx);
        let y0 = exact_scaled_residual(ys[start], oy, sy);
        let x1 = exact_scaled_residual(xs[end], ox, sx);
        let y1 = exact_scaled_residual(ys[end], oy, sy);
        let forward = expansion_product(&x0, &y1);
        let mut reverse = expansion_product(&y0, &x1);
        for term in &mut reverse {
            *term = -*term;
        }
        let cross = expansion_sum(&forward, &reverse);
        expansion = expansion_sum(&expansion, &cross);
    }
    unscale_area(estimate(&expansion) / 2.0, sx, sy)
}

/// Origin-shifted, optionally per-axis-scaled shoelace half-sum / 2.
/// Unit scales keep the ordinary path bit-identical. Non-unit scales form
/// `x*sx - origin_x*sx` and `y*sy - origin_y*sy`.
fn shoelace_shifted(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> f64 {
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
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
                // Ordinary path: plain origin shift (bit-identical). Scaled
                // rescue: `x*s - origin*s`, never `(x-origin)*s`.
                let (sx0, sy0) = if identity {
                    (x0 - ox, y0 - oy)
                } else {
                    (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
                };
                let (sx1, sy1) = if identity {
                    (x1 - ox, y1 - oy)
                } else {
                    (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
                };
                area.algebraic_add(sx0.algebraic_mul(sy1).algebraic_sub(sx1.algebraic_mul(sy0)))
            });
        return cross / 2.0;
    }
    let (x0, _) = xs[..pairs].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..pairs].as_chunks::<REDUCE_LANES>();
    let (y1, _) = ys[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (ovx, ovy) = (ReduceSimd::splat(ox), ReduceSimd::splat(oy));
    let sx_v = ReduceSimd::splat(sx);
    let sy_v = ReduceSimd::splat(sy);
    let residual_x = |v: ReduceSimd, o: ReduceSimd| {
        if identity {
            v - o
        } else {
            // x*s - origin*s (scale before subtraction).
            v * sx_v - o * sx_v
        }
    };
    let residual_y = |v: ReduceSimd, o: ReduceSimd| {
        if identity { v - o } else { v * sy_v - o * sy_v }
    };
    let simd_fold = |mut acc: ReduceSimd, start: usize| {
        let chunk = start / REDUCE_LANES;
        let (x0, x1) = (
            residual_x(ReduceSimd::from_array(x0[chunk]), ovx),
            residual_x(ReduceSimd::from_array(x1[chunk]), ovx),
        );
        let (y0, y1) = (
            residual_y(ReduceSimd::from_array(y0[chunk]), ovy),
            residual_y(ReduceSimd::from_array(y1[chunk]), ovy),
        );
        acc += x0 * y1 - x1 * y0;
        acc
    };
    let lanes = x0.len() * REDUCE_LANES;
    simd_reduce_f64(
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
                    residual_x(ReduceSimd::load_or_default(&xs[lanes..pairs]), ovx),
                    residual_x(ReduceSimd::load_or_default(&xs[lanes + 1..=pairs]), ovx),
                );
                let (ty0, ty1) = (
                    residual_y(ReduceSimd::load_or_default(&ys[lanes..pairs]), ovy),
                    residual_y(ReduceSimd::load_or_default(&ys[lanes + 1..=pairs]), ovy),
                );
                acc += tx0 * ty1 - tx1 * ty0;
            }
            acc.reduce_sum() / 2.0
        },
    )
}

/// Whether the ordinary shared-frame shoelace fold has no reliable sign or
/// magnitude bits left.  The decisive scale is the sum of the two products
/// *before* each subtraction: a finite nonzero cross term can already have
/// lost its answer to product cancellation, so testing only the resulting
/// zero/non-finite value misses the `2^-27` rotated triangle.
fn shoelace_shifted_needs_exact(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
    measured: f64,
) -> bool {
    if !measured.is_finite() {
        return false;
    }
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    let mut product_scale = 0.0_f64;
    for ((&x0, &x1), (&y0, &y1)) in xs[..pairs]
        .iter()
        .zip(&xs[1..=pairs])
        .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
    {
        let (x0, y0) = if identity {
            (x0 - ox, y0 - oy)
        } else {
            (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
        };
        let (x1, y1) = if identity {
            (x1 - ox, y1 - oy)
        } else {
            (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
        };
        product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
    }
    if !product_scale.is_finite() {
        return true;
    }
    // Each edge has two rounded products, one rounded subtraction, and one
    // contribution to the fold.  This conservative IEEE-754 roundoff bound
    // is a property of the performed arithmetic, not a geometry-size knob.
    let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
    let roundoff = product_scale * f64::EPSILON * operations;
    measured.abs() <= roundoff
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

/// Polygon area in one common, power-of-two-normalized frame for the shell
/// and every hole. Per-ring rescues are insufficient at extreme magnitudes:
/// a shell and a hole can each legitimately measure as infinity even though
/// their mathematical difference has one unambiguous sign. The shared frame
/// performs `shell - holes` before one final unscale.
pub(crate) fn polygon_area_measure_with(
    visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
) -> f64 {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    let mut any = false;
    visit(&mut |xs, ys, _hole| {
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            any = true;
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    });
    if !(any && min_x.is_finite() && min_y.is_finite()) {
        return f64::NAN;
    }
    let ox = f64::midpoint(min_x, max_x);
    let oy = f64::midpoint(min_y, max_y);
    let sx = axis_pow2_scale(max_x.abs().max(min_x.abs()).max(ox.abs()));
    let sy = axis_pow2_scale(max_y.abs().max(min_y.abs()).max(oy.abs()));
    let mut area = 0.0_f64;
    let mut needs_exact = false;
    visit(&mut |xs, ys, hole| {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return;
        }
        let signed_ring = shoelace_shifted(xs, ys, pairs, ox, oy, sx, sy);
        let ring = signed_ring.abs();
        if !ring.is_finite() {
            // Overflow is an arithmetic-loss signal, not proof that the
            // geometric result is unrepresentable: the exact shared frame can
            // still cancel it to a finite answer.
            needs_exact = true;
        } else if shoelace_shifted_needs_exact(xs, ys, pairs, ox, oy, sx, sy, signed_ring) {
            // A shared frame is required for shell/hole cancellation. Replay
            // that same frame when the ordinary operations consumed all
            // reliable answer bits, including a finite nonzero result.
            needs_exact = true;
        } else if hole {
            area -= ring;
        } else {
            area += ring;
        }
    });
    if needs_exact || area == 0.0 {
        let mut exact_area2 = 0.0_f64;
        visit(&mut |xs, ys, hole| {
            let pairs = xs.len().saturating_sub(1);
            if pairs == 0 {
                return;
            }
            let (ring_area2, ..) =
                exact_ring_area_centroid_sums_local(xs, ys, pairs, ox, oy, sx, sy);
            if hole {
                exact_area2 -= ring_area2.abs();
            } else {
                exact_area2 += ring_area2.abs();
            }
        });
        area = exact_area2 / 2.0;
    }
    unscale_area(area, sx, sy)
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
    let count = xs.len();
    AreaMeasure(
        exact_cycle_area_measure_columns(xs, ys, count, |index| {
            (index, wrap_index(index + 1, count))
        })
        .abs(),
    )
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
    fn shared_two_sum_preserves_shoelace_bits() {
        // The overflow case is asserted separately below: its error term is a
        // NaN, whose SIGN is not a contract. Every case here has a fully
        // specified bit pattern.
        let cases = [
            (5e-324, 5e-324, (0x0000_0000_0000_0002, 0)),
            (1e-308, 5e-324, (0x0007_30D6_7819_E8D3, 0)),
            (f64::MIN_POSITIVE, -5e-324, (0x000F_FFFF_FFFF_FFFF, 0)),
            (1.0, -(1.0 - f64::EPSILON / 2.0), (0x3CA0_0000_0000_0000, 0)),
            (0.0, -0.0, (0, 0)),
            (-0.0, -0.0, (0x8000_0000_0000_0000, 0)),
        ];
        for (left, right, expected) in cases {
            let actual = two_sum(left, right);
            assert_eq!((actual.0.to_bits(), actual.1.to_bits()), expected);
        }

        // 2^1023 + 2^1023 overflows, so `sum` is +inf and the error term is
        // born from `inf - inf`. The sum is exactly specified and stays
        // pinned; the error term is a NaN, and IEEE 754-2019 s6.3 specifies
        // the sign bit only for copy, negate, abs and copySign -- not for a
        // NaN an operation produces. That freedom is real and observable
        // here: x86-64 `subsd` yields the negative default NaN
        // 0xFFF8_0000_0000_0000, while LLVM's constant folder yields the
        // positive 0x7FF8_0000_0000_0000, so pinning the bits asserted
        // nothing about `two_sum` and everything about whether the
        // expression happened to be folded at compile time.
        let (overflow_sum, overflow_err) = two_sum(2.0_f64.powi(1023), 2.0_f64.powi(1023));
        assert_eq!(overflow_sum.to_bits(), 0x7FF0_0000_0000_0000);
        assert!(overflow_err.is_nan());

        let mixed_xs = [0.0, f64::from_bits(1), 2.0_f64.powi(-20), 0.0];
        let mixed_ys = [0.0, f64::from_bits(2), 2.0_f64.powi(-20), 0.0];
        assert_eq!(
            shoelace_measure_columns(&mixed_xs, &mixed_ys, 3)
                .0
                .to_bits(),
            0x8000_0000_0000_0000
        );

        let huge_xs = [0.0, 1e300, 1e300, 0.0, 0.0];
        let huge_ys = [0.0, 0.0, 1e300, 1e300, 0.0];
        assert_eq!(
            shoelace_measure_columns(&huge_xs, &huge_ys, 4).0.to_bits(),
            0x7FF0_0000_0000_0000
        );
    }

    /// Origin-shifted scalar shoelace — reference for both measurement paths.
    fn shifted_scalar_area(xs: &[f64], ys: &[f64]) -> f64 {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return 0.0;
        }
        let (ox, oy) = (xs[0], ys[0]);
        xs.array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + ((x0 - ox) * (y1 - oy) - (x1 - ox) * (y0 - oy))
            })
            / 2.0
    }

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
        // First vertex is origin — shift is a no-op, so bit-match holds.
        let scalar_reference = shifted_scalar_area(&xs, &ys);
        assert!(scalar_reference > 0.0);
        assert_eq!(measure.to_bits(), scalar_reference.to_bits());
    }

    #[test]
    fn exact_product_tails_keep_a_closed_near_collinear_ring() {
        // Every rounded cross term is zero, so this input proves the exact
        // expansion fallback is taken rather than merely exercising the fast
        // filtered sign path. Its stored-double twice-area is 2^-54.
        let e = 2.0_f64.powi(-27);
        let xs = [0.0, 1.0, 1.0 - e, 0.0];
        let ys = [0.0, 1.0 + e, 1.0, 0.0];
        let pairs = xs.len() - 1;
        let rounded_terms: Vec<f64> = xs[..pairs]
            .iter()
            .zip(&xs[1..])
            .zip(ys[..pairs].iter().zip(&ys[1..]))
            .map(|((&x0, &x1), (&y0, &y1))| x0 * y1 - x1 * y0)
            .collect();
        assert_eq!(rounded_terms, [0.0, 0.0, 0.0]);
        assert_eq!(
            exact_ring_area_sign_columns(&xs, &ys, pairs),
            AreaSign::Positive
        );
        assert_eq!(
            ring_area_measure_columns(&xs, &ys, pairs).get().to_bits(),
            2.0_f64.powi(-55).to_bits()
        );
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
        let scalar = shifted_scalar_area(&xs, &ys).abs();
        assert!(
            (simd - scalar).abs() <= 1e-6 * scalar,
            "simd={simd} scalar={scalar}"
        );
    }

    #[test]
    fn shoelace_large_absolute_coords_match_shifted_truth() {
        // ~200 m² irregular building at UTM-scale easting/northing. Naive
        // shoelace cross-products (~2.6e12) cancel to ~200 with ~1e-4 absolute
        // error; origin-shifted measurement recovers the true area.
        let xs = [500_000.1_f64, 500_020.3, 500_019.7, 500_000.5, 500_000.1];
        let ys = [
            5_200_000.2_f64,
            5_200_000.1,
            5_200_010.4,
            5_200_010.3,
            5_200_000.2,
        ];
        let pairs = xs.len() - 1;
        assert!(pairs < SHOELACE_SIMD_MIN);
        let measure = ring_area_measure_columns(&xs, &ys, pairs).get();
        let truth = shifted_scalar_area(&xs, &ys).abs();
        let naive = xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + (x0 * y1 - x1 * y0)
            })
            .abs()
            / 2.0;
        assert!(
            (measure - truth).abs() <= 1e-12 * truth.max(1.0),
            "measure={measure} truth={truth} naive={naive}"
        );
        // Naive loses ~1e-4 absolute (~1e-6 relative) at this scale.
        assert!(
            (naive - truth).abs() > 1e-5,
            "naive={naive} measure={measure} truth={truth}"
        );
        assert!(
            (naive - truth).abs() > (measure - truth).abs() * 1e6,
            "naive={naive} measure={measure} truth={truth}"
        );
    }

    #[test]
    fn shoelace_simd_large_absolute_coords_match_shifted_truth() {
        // Same UTM-scale conditioning on the SIMD path: a circle of radius 10
        // translated to (500000, 5200000), pairs past the gate. Both paths use
        // the origin shift, so they must agree; naive (unshifted) diverges.
        let k = SHOELACE_SIMD_MIN + 9;
        let ox = 500_000.0_f64;
        let oy = 5_200_000.0_f64;
        let r = 10.0_f64;
        let mut xs: Vec<f64> = (0..k)
            .map(|i| ox + (i as f64 * std::f64::consts::TAU / k as f64).cos() * r)
            .collect();
        let mut ys: Vec<f64> = (0..k)
            .map(|i| oy + (i as f64 * std::f64::consts::TAU / k as f64).sin() * r)
            .collect();
        xs.push(xs[0]);
        ys.push(ys[0]);
        let pairs = xs.len() - 1;
        assert!(pairs >= SHOELACE_SIMD_MIN);
        let measure = ring_area_measure_columns(&xs, &ys, pairs).get();
        let shifted = shifted_scalar_area(&xs, &ys).abs();
        let naive = xs
            .array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .fold(0.0_f64, |area, ([x0, x1], [y0, y1])| {
                area + (x0 * y1 - x1 * y0)
            })
            .abs()
            / 2.0;
        assert!(
            (measure - shifted).abs() <= 1e-12 * shifted.max(1.0),
            "measure={measure} shifted={shifted}"
        );
        assert!(
            (naive - shifted).abs() > (measure - shifted).abs(),
            "naive={naive} measure={measure} shifted={shifted}"
        );
    }
}
