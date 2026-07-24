#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use std::simd::num::SimdFloat;

use super::*;
use crate::geometry::*;

/// Map ``-0.0`` to ``+0.0``; leave every other value unchanged.
///
/// Rust's ``Iterator::sum`` for ``f64`` uses ``-0.0`` as the empty identity,
/// so empty multipart/collection folds (area/length) surface negative zero
/// unless canonicalized here.
pub(crate) fn canonicalize_zero(v: f64) -> f64 {
    if v == 0.0 { 0.0 } else { v }
}

/// IEEE-754 f64 exponent field all-ones ⇒ the value is ±inf or NaN.
const NON_FINITE_EXP_MASK: u64 = 0x7FF0_0000_0000_0000;

/// Branchless SIMD finiteness scan: `true` iff EVERY value in `column` is
/// finite. Accumulates the non-finite predicate VERTICALLY into one wide mask
/// (no per-chunk horizontal reduce, no `.all()` short-circuit), reducing once at
/// the end so LLVM unrolls the `REDUCE_LANES`-wide vector into ILP-friendly
/// compares on the v2 baseline. ~2× the scalar `iter().all(is_finite)` on a
/// finite column (measured 85.8→40.7 µs / 400k f64, idiomatic Rust, no
/// intrinsics). The one finiteness kernel behind packed Arrow import and the
/// affine / CRS-projection output validation of large coordinate columns; tiny
/// fixed-arity checks (a handful of coords, `[f64; 4]` bounds) stay scalar.
pub(crate) fn column_all_finite(column: &[f64]) -> bool {
    let exp = std::simd::Simd::<u64, REDUCE_LANES>::splat(NON_FINITE_EXP_MASK);
    let (chunks, tail) = column.as_chunks::<REDUCE_LANES>();
    let mut non_finite = std::simd::Mask::<i64, REDUCE_LANES>::splat(false);
    for chunk in chunks {
        let bits = ReduceSimd::from_array(*chunk).to_bits();
        non_finite |= (bits & exp).simd_eq(exp);
    }
    !non_finite.any() && tail.iter().all(|value| value.is_finite())
}

/// Bit-exact boolean reduction: `true` iff ANY index satisfies `scalar` /
/// `vector`. Full `REDUCE_LANES` chunks run `vector` (chunk-start index);
/// inputs below [`REDUCE_SIMD_MIN`] and the tail use `scalar` exclusively.
/// Vertical-OR accumulates chunk masks, then `.any()` — plain ops only
/// (decision/validation paths).
pub(crate) fn simd_mask_any(
    len: usize,
    scalar: impl FnMut(usize) -> bool,
    vector: impl FnMut(usize) -> std::simd::Mask<i64, REDUCE_LANES>,
) -> bool {
    simd_mask_reduce(
        len,
        scalar,
        vector,
        false,
        |acc, mask| acc | mask,
        |acc| acc.any().then_some(true),
        |acc, value| acc || value,
        |acc| acc.then_some(true),
    )
}

/// Collect indices where `scalar` / `vector` is true. Full `REDUCE_LANES`
/// chunks run `vector` (chunk-start index); inputs below [`REDUCE_SIMD_MIN`]
/// and the tail use `scalar` exclusively. A count pass sizes the reserve;
/// chunk masks expand via `to_bitmask` — plain ops only (decision paths).
pub(crate) fn simd_select_indices(
    len: usize,
    mut scalar: impl FnMut(usize) -> bool,
    mut vector: impl FnMut(usize) -> std::simd::Mask<i64, REDUCE_LANES>,
) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    if len < REDUCE_SIMD_MIN {
        return (0..len).filter(|&index| scalar(index)).collect();
    }
    let chunks = len / REDUCE_LANES;
    let mut count = 0_usize;
    for chunk in 0..chunks {
        count += vector(chunk * REDUCE_LANES).to_bitmask().count_ones() as usize;
    }
    let lanes = chunks * REDUCE_LANES;
    for index in lanes..len {
        if scalar(index) {
            count += 1;
        }
    }
    let mut indices = Vec::with_capacity(count);
    for chunk in 0..chunks {
        let start = chunk * REDUCE_LANES;
        let bitmask = vector(start).to_bitmask();
        for lane in 0..REDUCE_LANES {
            if bitmask & (1_u64 << lane) != 0 {
                indices.push(start + lane);
            }
        }
    }
    for index in lanes..len {
        if scalar(index) {
            indices.push(index);
        }
    }
    indices
}

/// First-minimum index under `f64::total_cmp` (NaN ordering preserved).
/// Full `REDUCE_LANES` chunks evaluate `vector_score` when every lane is
/// finite; otherwise the chunk falls back to `scalar_score` lane-by-lane.
/// Inputs below [`REDUCE_SIMD_MIN`] and the tail use `scalar_score` only.
pub(crate) fn simd_argmin_f64(
    len: usize,
    mut scalar_score: impl FnMut(usize) -> f64,
    mut vector_score: impl FnMut(usize) -> ReduceSimd,
) -> Option<usize> {
    if len == 0 {
        return None;
    }
    if len < REDUCE_SIMD_MIN {
        return (0..len)
            .map(|index| (index, scalar_score(index)))
            .min_by(|left, right| left.1.total_cmp(&right.1))
            .map(|(index, _)| index);
    }
    let mut best_index = 0_usize;
    let mut best_score = scalar_score(0);
    let chunks = len / REDUCE_LANES;
    for chunk in 0..chunks {
        let start = chunk * REDUCE_LANES;
        let scores = vector_score(start);
        if scores.is_finite().all() {
            // Vector min + first-set-bit: preserves first-minimum ties (the
            // lowest lane with the chunk min) without eight scalar branches.
            // All-finite scores use IEEE min, which agrees with `total_cmp`
            // for finite values (NaN is gated out above).
            let chunk_min = scores.reduce_min();
            if chunk_min.total_cmp(&best_score).is_lt() {
                let bitmask = scores.simd_eq(ReduceSimd::splat(chunk_min)).to_bitmask();
                let lane = bitmask.trailing_zeros() as usize;
                best_score = chunk_min;
                best_index = start + lane;
            }
        } else {
            for index in start..start + REDUCE_LANES {
                let score = scalar_score(index);
                if score.total_cmp(&best_score).is_lt() {
                    best_score = score;
                    best_index = index;
                }
            }
        }
    }
    let lanes = chunks * REDUCE_LANES;
    for index in lanes..len {
        let score = scalar_score(index);
        if score.total_cmp(&best_score).is_lt() {
            best_score = score;
            best_index = index;
        }
    }
    Some(best_index)
}

/// Map each index to `f64` through a SIMD kernel. Full `REDUCE_LANES` chunks
/// run `vector` (chunk-start index); inputs below [`REDUCE_SIMD_MIN`] and the
/// tail use `scalar` exclusively — plain ops, left-associative in the caller.
pub(crate) fn simd_indexed_map_f64(
    len: usize,
    out: &mut [f64],
    mut scalar: impl FnMut(usize) -> f64,
    mut vector: impl FnMut(usize) -> ReduceSimd,
) {
    debug_assert_eq!(len, out.len());
    if len == 0 {
        return;
    }
    if len < REDUCE_SIMD_MIN {
        for (index, value) in out.iter_mut().enumerate().take(len) {
            *value = scalar(index);
        }
        return;
    }
    let chunks = len / REDUCE_LANES;
    {
        let (out_chunks, _) = out.as_chunks_mut::<REDUCE_LANES>();
        for (chunk, out_chunk) in out_chunks.iter_mut().enumerate().take(chunks) {
            vector(chunk * REDUCE_LANES).copy_to_slice(out_chunk);
        }
    }
    let lanes = chunks * REDUCE_LANES;
    for (offset, value) in out[lanes..].iter_mut().enumerate() {
        *value = scalar(lanes + offset);
    }
}

/// Bit-exact boolean reduction: `true` iff EVERY index satisfies `scalar` /
/// `vector`. Full `REDUCE_LANES` chunks run `vector` (chunk-start index);
/// inputs below [`REDUCE_SIMD_MIN`] and the tail use `scalar` exclusively.
/// Vertical-AND accumulates chunk masks, then `.all()` — plain ops only
/// (decision/validation paths).
pub(crate) fn simd_mask_all(
    len: usize,
    scalar: impl FnMut(usize) -> bool,
    vector: impl FnMut(usize) -> std::simd::Mask<i64, REDUCE_LANES>,
) -> bool {
    simd_mask_reduce(
        len,
        scalar,
        vector,
        true,
        |acc, mask| acc & mask,
        |acc| (!acc.all()).then_some(false),
        |acc, value| acc && value,
        |acc| (!acc).then_some(false),
    )
}

fn simd_mask_reduce(
    len: usize,
    scalar: impl FnMut(usize) -> bool,
    mut vector: impl FnMut(usize) -> std::simd::Mask<i64, REDUCE_LANES>,
    identity: bool,
    combine_mask: impl Fn(
        std::simd::Mask<i64, REDUCE_LANES>,
        std::simd::Mask<i64, REDUCE_LANES>,
    ) -> std::simd::Mask<i64, REDUCE_LANES>,
    done_mask: impl Fn(std::simd::Mask<i64, REDUCE_LANES>) -> Option<bool>,
    combine_scalar: impl Fn(bool, bool) -> bool,
    done_scalar: impl Fn(bool) -> Option<bool>,
) -> bool {
    if len == 0 {
        return identity;
    }
    if len < REDUCE_SIMD_MIN {
        return scalar_mask_reduce(0..len, identity, scalar, combine_scalar, done_scalar);
    }
    let chunks = len / REDUCE_LANES;
    let mut acc = std::simd::Mask::<i64, REDUCE_LANES>::splat(identity);
    for chunk in 0..chunks {
        acc = combine_mask(acc, vector(chunk * REDUCE_LANES));
    }
    if let Some(done) = done_mask(acc) {
        return done;
    }
    let lanes = chunks * REDUCE_LANES;
    scalar_mask_reduce(lanes..len, identity, scalar, combine_scalar, done_scalar)
}

fn scalar_mask_reduce(
    indices: std::ops::Range<usize>,
    mut acc: bool,
    mut scalar: impl FnMut(usize) -> bool,
    combine: impl Fn(bool, bool) -> bool,
    done: impl Fn(bool) -> Option<bool>,
) -> bool {
    for index in indices {
        acc = combine(acc, scalar(index));
        if let Some(done) = done(acc) {
            return done;
        }
    }
    acc
}

/// Shared wide-lane reduction driver over a logical `f64` column/window.
///
/// Callers own the actual lane loads so sibling columns, shifted windows, and
/// fallback bookkeeping stay local to the kernel; the common shape is fixed
/// here: scalar below the measured crossover, full `REDUCE_LANES` chunks in the
/// SIMD body, then a caller-defined tail. `finish` receives the lane
/// accumulator plus any scalar side channel accumulated by the tail or rare
/// fallbacks.
pub(in crate::geometry) fn simd_reduce_f64<A, R, O>(
    len: usize,
    simd_seed: A,
    scalar_seed: R,
    mut fold_simd: impl FnMut(A, R, usize) -> (A, R),
    mut fold_scalar: impl FnMut(R, std::ops::Range<usize>) -> R,
    finish: impl FnOnce(A, R) -> O,
) -> O {
    if len < REDUCE_SIMD_MIN {
        return finish(simd_seed, fold_scalar(scalar_seed, 0..len));
    }
    let chunks = len / REDUCE_LANES;
    let mut acc = simd_seed;
    let mut scalar = scalar_seed;
    for chunk in 0..chunks {
        let (next_acc, next_scalar) = fold_simd(acc, scalar, chunk * REDUCE_LANES);
        acc = next_acc;
        scalar = next_scalar;
    }
    let lanes = chunks * REDUCE_LANES;
    if lanes < len {
        scalar = fold_scalar(scalar, lanes..len);
    }
    finish(acc, scalar)
}

/// Map four equal-length `f64` columns through a guarded SIMD distance kernel.
///
/// Full `REDUCE_LANES` chunks run `vector`; lanes where the returned mask is
/// set (`bad`) are overwritten by the scalar fallback. The tail and inputs
/// below [`REDUCE_SIMD_MIN`] use `scalar` exclusively.
pub(crate) fn pair_map4_guarded_f64(
    lx: &[f64],
    ly: &[f64],
    rx: &[f64],
    ry: &[f64],
    out: &mut [f64],
    mut scalar: impl FnMut(f64, f64, f64, f64) -> f64,
    mut vector: impl FnMut(
        ReduceSimd,
        ReduceSimd,
        ReduceSimd,
        ReduceSimd,
    ) -> (ReduceSimd, std::simd::Mask<i64, REDUCE_LANES>),
) {
    debug_assert_eq!(lx.len(), ly.len());
    debug_assert_eq!(lx.len(), rx.len());
    debug_assert_eq!(lx.len(), ry.len());
    debug_assert_eq!(lx.len(), out.len());
    let len = lx.len();
    if len < REDUCE_SIMD_MIN {
        for index in 0..len {
            out[index] = scalar(lx[index], ly[index], rx[index], ry[index]);
        }
        return;
    }
    let (lx_chunks, _) = lx.as_chunks::<REDUCE_LANES>();
    let (ly_chunks, _) = ly.as_chunks::<REDUCE_LANES>();
    let (rx_chunks, _) = rx.as_chunks::<REDUCE_LANES>();
    let (ry_chunks, _) = ry.as_chunks::<REDUCE_LANES>();
    {
        let (out_chunks, _) = out.as_chunks_mut::<REDUCE_LANES>();
        for chunk in 0..lx_chunks.len() {
            let (distance, bad) = vector(
                ReduceSimd::from_array(lx_chunks[chunk]),
                ReduceSimd::from_array(ly_chunks[chunk]),
                ReduceSimd::from_array(rx_chunks[chunk]),
                ReduceSimd::from_array(ry_chunks[chunk]),
            );
            let mut chunk_out = distance.to_array();
            if bad.any() {
                let start = chunk * REDUCE_LANES;
                for (lane, value) in chunk_out.iter_mut().enumerate().take(REDUCE_LANES) {
                    if bad.test(lane) {
                        let index = start + lane;
                        *value = scalar(lx[index], ly[index], rx[index], ry[index]);
                    }
                }
            }
            out_chunks[chunk] = chunk_out;
        }
    }
    let lanes = lx_chunks.len() * REDUCE_LANES;
    for index in lanes..len {
        out[index] = scalar(lx[index], ly[index], rx[index], ry[index]);
    }
}

/// Select a per-element boolean over four equal-length `f64` columns.
///
/// Full `REDUCE_LANES` chunks store the SIMD mask into `out`; the tail and
/// inputs below [`REDUCE_SIMD_MIN`] use `scalar` exclusively.
pub(crate) fn pair_select_mask(
    lx: &[f64],
    ly: &[f64],
    rx: &[f64],
    ry: &[f64],
    out: &mut [bool],
    mut scalar: impl FnMut(f64, f64, f64, f64) -> bool,
    mut vector: impl FnMut(
        ReduceSimd,
        ReduceSimd,
        ReduceSimd,
        ReduceSimd,
    ) -> std::simd::Mask<i64, REDUCE_LANES>,
) {
    debug_assert_eq!(lx.len(), ly.len());
    debug_assert_eq!(lx.len(), rx.len());
    debug_assert_eq!(lx.len(), ry.len());
    debug_assert_eq!(lx.len(), out.len());
    let len = lx.len();
    if len < REDUCE_SIMD_MIN {
        for index in 0..len {
            out[index] = scalar(lx[index], ly[index], rx[index], ry[index]);
        }
        return;
    }
    let (lx_chunks, _) = lx.as_chunks::<REDUCE_LANES>();
    let (ly_chunks, _) = ly.as_chunks::<REDUCE_LANES>();
    let (rx_chunks, _) = rx.as_chunks::<REDUCE_LANES>();
    let (ry_chunks, _) = ry.as_chunks::<REDUCE_LANES>();
    {
        let (out_chunks, _) = out.as_chunks_mut::<REDUCE_LANES>();
        for chunk in 0..lx_chunks.len() {
            let mask = vector(
                ReduceSimd::from_array(lx_chunks[chunk]),
                ReduceSimd::from_array(ly_chunks[chunk]),
                ReduceSimd::from_array(rx_chunks[chunk]),
                ReduceSimd::from_array(ry_chunks[chunk]),
            );
            out_chunks[chunk] = mask.to_array();
        }
    }
    let lanes = lx_chunks.len() * REDUCE_LANES;
    for index in lanes..len {
        out[index] = scalar(lx[index], ly[index], rx[index], ry[index]);
    }
}

/// Map one equal-length `f64` column through a SIMD kernel.
///
/// Full `REDUCE_LANES` chunks run `vector`; the tail and inputs below
/// [`REDUCE_SIMD_MIN`] use `scalar` exclusively.
pub(crate) fn simd_map_f64(
    src: &[f64],
    dst: &mut [f64],
    mut scalar: impl FnMut(f64) -> f64,
    mut vector: impl FnMut(ReduceSimd) -> ReduceSimd,
) {
    debug_assert_eq!(src.len(), dst.len());
    let len = src.len();
    if len < REDUCE_SIMD_MIN {
        for index in 0..len {
            dst[index] = scalar(src[index]);
        }
        return;
    }
    let (src_chunks, _) = src.as_chunks::<REDUCE_LANES>();
    {
        let (dst_chunks, _) = dst.as_chunks_mut::<REDUCE_LANES>();
        for chunk in 0..src_chunks.len() {
            vector(ReduceSimd::from_array(src_chunks[chunk])).copy_to_slice(&mut dst_chunks[chunk]);
        }
    }
    let lanes = src_chunks.len() * REDUCE_LANES;
    for index in lanes..len {
        dst[index] = scalar(src[index]);
    }
}

/// Fallible [`simd_map_f64`]: `vector`/`scalar` errors propagate immediately.
pub(crate) fn try_simd_map_f64<E>(
    src: &[f64],
    dst: &mut [f64],
    mut scalar: impl FnMut(f64) -> Result<f64, E>,
    mut vector: impl FnMut(ReduceSimd) -> Result<ReduceSimd, E>,
) -> Result<(), E> {
    debug_assert_eq!(src.len(), dst.len());
    let len = src.len();
    if len < REDUCE_SIMD_MIN {
        for index in 0..len {
            dst[index] = scalar(src[index])?;
        }
        return Ok(());
    }
    let (src_chunks, _) = src.as_chunks::<REDUCE_LANES>();
    {
        let (dst_chunks, _) = dst.as_chunks_mut::<REDUCE_LANES>();
        for chunk in 0..src_chunks.len() {
            vector(ReduceSimd::from_array(src_chunks[chunk]))?
                .copy_to_slice(&mut dst_chunks[chunk]);
        }
    }
    let lanes = src_chunks.len() * REDUCE_LANES;
    for index in lanes..len {
        dst[index] = scalar(src[index])?;
    }
    Ok(())
}

/// Map two equal-length `f64` columns through a 2-in/2-out SIMD kernel.
///
/// Full `REDUCE_LANES` chunks run `vector`; the tail and inputs below
/// [`REDUCE_SIMD_MIN`] use `scalar` exclusively.
pub(crate) fn simd_xy_map_f64(
    xs: &[f64],
    ys: &[f64],
    out_x: &mut [f64],
    out_y: &mut [f64],
    mut scalar: impl FnMut(f64, f64) -> (f64, f64),
    mut vector: impl FnMut(ReduceSimd, ReduceSimd) -> (ReduceSimd, ReduceSimd),
) {
    debug_assert_eq!(xs.len(), ys.len());
    debug_assert_eq!(xs.len(), out_x.len());
    debug_assert_eq!(xs.len(), out_y.len());
    let len = xs.len();
    if len < REDUCE_SIMD_MIN {
        for index in 0..len {
            let (x, y) = scalar(xs[index], ys[index]);
            out_x[index] = x;
            out_y[index] = y;
        }
        return;
    }
    let (x_chunks, _) = xs.as_chunks::<REDUCE_LANES>();
    let (y_chunks, _) = ys.as_chunks::<REDUCE_LANES>();
    {
        let (ox_chunks, _) = out_x.as_chunks_mut::<REDUCE_LANES>();
        let (oy_chunks, _) = out_y.as_chunks_mut::<REDUCE_LANES>();
        for chunk in 0..x_chunks.len() {
            let (vx, vy) = vector(
                ReduceSimd::from_array(x_chunks[chunk]),
                ReduceSimd::from_array(y_chunks[chunk]),
            );
            vx.copy_to_slice(&mut ox_chunks[chunk]);
            vy.copy_to_slice(&mut oy_chunks[chunk]);
        }
    }
    let lanes = x_chunks.len() * REDUCE_LANES;
    for index in lanes..len {
        let (x, y) = scalar(xs[index], ys[index]);
        out_x[index] = x;
        out_y[index] = y;
    }
}

/// Two-input/ONE-output sibling of [`simd_xy_map_f64`] for maps whose second
/// output column is the identity (single-axis affine): both source columns
/// feed the formula, but only one output column is produced — no ignored
/// column is allocated or written.
pub(crate) fn simd_xy_map_one_f64(
    xs: &[f64],
    ys: &[f64],
    out: &mut [f64],
    mut scalar: impl FnMut(f64, f64) -> f64,
    mut vector: impl FnMut(ReduceSimd, ReduceSimd) -> ReduceSimd,
) {
    debug_assert_eq!(xs.len(), ys.len());
    debug_assert_eq!(xs.len(), out.len());
    let len = xs.len();
    if len < REDUCE_SIMD_MIN {
        for index in 0..len {
            out[index] = scalar(xs[index], ys[index]);
        }
        return;
    }
    let (x_chunks, _) = xs.as_chunks::<REDUCE_LANES>();
    let (y_chunks, _) = ys.as_chunks::<REDUCE_LANES>();
    {
        let (out_chunks, _) = out.as_chunks_mut::<REDUCE_LANES>();
        for chunk in 0..x_chunks.len() {
            vector(
                ReduceSimd::from_array(x_chunks[chunk]),
                ReduceSimd::from_array(y_chunks[chunk]),
            )
            .copy_to_slice(&mut out_chunks[chunk]);
        }
    }
    let lanes = x_chunks.len() * REDUCE_LANES;
    for index in lanes..len {
        out[index] = scalar(xs[index], ys[index]);
    }
}

/// Per-column `(min, max)` over a contiguous ordinate slice — the kernel
/// behind every planar bounds scan.
///
/// `simd_min`/`simd_max` carry the same NaN-skipping `f64::min`/`max` semantics
/// as the scalar fold (so the result is byte-identical), but on the `SoA`
/// columns they lower to `minpd`/`maxpd` plus a `cmpunordpd`/`blendvpd`
/// NaN-fixup on top (not bare `minpd`). The NaN-fixup is inherent to the
/// NaN-skipping IEEE `minimumNumber` semantics; reaching bare `minpd` would
/// take target intrinsics (forbidden — we write idiomatic Rust and let the
/// compiler vectorize), and measured no win on this v2 target anyway, so the
/// NaN-safe form is kept. Scalar `f64::min` lowers to a similar NaN-fixup
/// compare/blend sequence that neither vectorizes nor pipelines (measured
/// ~1.6 cyc/ordinate). Padding is unnecessary — the wide fold consumes whole
/// `[f64; REDUCE_LANES]` chunks and the short tail folds scalar, seeding both
/// accumulators from the first element so a single-element column is exact.
/// Plain column sum — measurement arithmetic (reassociation OK in the fold).
/// Σ of a column. The reassociation-friendly fold auto-vectorizes to packed
/// `addpd` in release (same shape as the asm-verified shoelace fold); a flat Σ
/// is memory-bandwidth-bound, where the explicit `f64x8` body has no headroom to
/// beat the auto-vectorized scalar — so this stays scalar for clarity. The SIMD
/// ILP advantage shows on compute-bound reductions (the cross-product folds),
/// not a plain sum.
pub(crate) fn column_sum(values: &[f64]) -> f64 {
    values
        .iter()
        .fold(0.0_f64, |acc, &value| acc.algebraic_add(value))
}

/// Column mean — `None` on an empty slice.
///
/// The flat `Σ / n` fast lane auto-vectorizes and is bit-identical to the scalar
/// mean for ordinary magnitudes. When it overflows f64 range even though the
/// inputs (and the true mean) are finite — e.g. every value at `1e308` — it
/// commits a non-finite mean that a downstream `Point::new` would reject as
/// "coordinates must be finite", blaming valid input. Detect that spurious
/// overflow and recompute with an overflow-safe online mean before returning.
/// Genuine non-finite inputs (`inf`/`nan`) are left to propagate — their mean is
/// legitimately non-finite and the online form would only corrupt it.
pub(crate) fn column_mean(values: &[f64]) -> Option<f64> {
    let count = values.len();
    if count == 0 {
        return None;
    }
    let mean = column_sum(values) / count as f64;
    if mean.is_finite() || !column_all_finite(values) {
        return Some(mean);
    }
    Some(online_mean(values))
}

/// Overflow-safe running mean, `m += (x - m) / n` (Welford). The correctness
/// fallback for [`column_mean`] when the flat `Σ` overflows though the finite
/// inputs have a finite mean: each increment stays bounded by the value range,
/// so no oversized total is ever formed. Plain arithmetic — this is a
/// correctness rescue, not a measurement fold, so it is never reassociated.
fn online_mean(values: &[f64]) -> f64 {
    let mut mean = 0.0_f64;
    for (index, &value) in values.iter().enumerate() {
        mean += (value - mean) / (index + 1) as f64;
    }
    mean
}

/// Column means over two same-length ordinate slices — `None` when empty. Two
/// independent `column_sum`s each auto-vectorize to `addpd`; the means are
/// bandwidth-bound so the previous hand-fused `f64x8` body had no headroom over
/// them (−40 LOC).
pub(crate) fn column_mean2(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    debug_assert_eq!(xs.len(), ys.len());
    Some((column_mean(xs)?, column_mean(ys)?))
}

pub(crate) fn column_minmax(values: &[f64]) -> Option<(f64, f64)> {
    let (&first, rest) = values.split_first()?;
    let (chunks, tail) = values.as_chunks::<REDUCE_LANES>();
    // One `f64x{REDUCE_LANES}` accumulator per extreme: a logical 512-bit
    // vector (`SIMD_VECTOR_BITS`) that LLVM unrolls into `REDUCE_LANES / 2` = 4
    // independent 128-bit `minpd` chains on the v2 (SSE) baseline — the wide
    // logical width is the unrolling lever, giving enough ILP to hide op latency.
    Some(simd_reduce_f64(
        values.len(),
        (ReduceSimd::splat(first), ReduceSimd::splat(first)),
        (first, first),
        |(lo, hi), scalar, start| {
            let lane = ReduceSimd::from_array(chunks[start / REDUCE_LANES]);
            ((lo.simd_min(lane), hi.simd_max(lane)), scalar)
        },
        |(mut lo, mut hi), range| {
            let values = if range.start == 0 {
                rest
            } else {
                &tail[..range.len()]
            };
            for &value in values {
                lo = lo.min(value);
                hi = hi.max(value);
            }
            (lo, hi)
        },
        |(lo, hi), (mut scalar_lo, mut scalar_hi)| {
            scalar_lo = scalar_lo.min(lo.reduce_min());
            scalar_hi = scalar_hi.max(hi.reduce_max());
            (scalar_lo, scalar_hi)
        },
    ))
}

/// Fused planar XY bounds over contiguous columns. Small/medium inputs (the
/// common per-row case) take a single scalar min/max pass that LLVM
/// auto-vectorizes; huge single rings cross over to two
/// [`column_minmax`] passes (the `row_bounds` crossover policy).
const XY_BOUNDS_SIMD_MIN: usize = super::crossover::FUSED_XY_BOUNDS;

pub(crate) fn xy_bounds_columns(xs: &[f64], ys: &[f64]) -> [f64; 4] {
    debug_assert!(!xs.is_empty());
    debug_assert_eq!(xs.len(), ys.len());
    if xs.len() >= XY_BOUNDS_SIMD_MIN {
        let (minx, maxx) = column_minmax(xs).expect("non-empty columns");
        let (miny, maxy) = column_minmax(ys).expect("non-empty columns");
        return [minx, miny, maxx, maxy];
    }
    let (&first_x, xs_tail) = xs.split_first().expect("non-empty columns");
    let (&first_y, ys_tail) = ys.split_first().expect("non-empty columns");
    let mut minx = first_x;
    let mut miny = first_y;
    let mut maxx = first_x;
    let mut maxy = first_y;
    for (&x, &y) in std::iter::zip(xs_tail, ys_tail) {
        minx = minx.min(x);
        miny = miny.min(y);
        maxx = maxx.max(x);
        maxy = maxy.max(y);
    }
    [minx, miny, maxx, maxy]
}

/// Area-weighted centroid sums about `anchor` over a ring's contiguous
/// columns: the UNSIGNED triangle-fan accumulators `(Σ 2·area, Σ 2·area·(ax +
/// x_i + x_{i+1}), Σ 2·area·(ay + y_i + y_{i+1}))` — the caller applies the
/// ring's CW/CCW sign and the final `/3/area` divides. The SIMD twin of
/// [`shoelace_measure_columns`] for the area centroid: same offset-by-one column
/// shape, three lane accumulators instead of one. Zero-padded tail edges
/// contribute exactly `0` (the base-relative cross product `(-ax)(-ay) −
/// (-ax)(-ay)` cancels). Small rings take an algebraic scalar fold
/// (measurement); large rings reduce in plain SIMD lanes.
/// Centroid is measurement arithmetic, so lane order is not part of the
/// contract (matching `area`).
pub(crate) fn centroid_ring_sums(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    anchor: (f64, f64),
) -> (f64, f64, f64) {
    let (ax, ay) = anchor;
    let scalar_fold = |range: std::ops::Range<usize>| {
        range.fold((0.0_f64, 0.0_f64, 0.0_f64), |(sa, sx, sy), index| {
            let (x1, y1) = (xs[index], ys[index]);
            let (x2, y2) = (xs[index + 1], ys[index + 1]);
            let area2 = x1
                .algebraic_sub(ax)
                .algebraic_mul(y2.algebraic_sub(ay))
                .algebraic_sub(x2.algebraic_sub(ax).algebraic_mul(y1.algebraic_sub(ay)));
            (
                sa.algebraic_add(area2),
                sx.algebraic_add(area2.algebraic_mul(ax.algebraic_add(x1).algebraic_add(x2))),
                sy.algebraic_add(area2.algebraic_mul(ay.algebraic_add(y1).algebraic_add(y2))),
            )
        })
    };
    // Same offset-pair cross-product shape as `shoelace_measure_columns`, so it
    // shares the measured crossover: the scalar fold auto-vectorizes and wins on
    // small rings; the 512-bit body's ILP only pays off past it.
    if pairs < super::crossover::OFFSET_PAIR_MEASURE {
        return scalar_fold(0..pairs);
    }
    let (x0, _) = xs[..pairs].as_chunks::<REDUCE_LANES>();
    let (x1c, _) = xs[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..pairs].as_chunks::<REDUCE_LANES>();
    let (y1c, _) = ys[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (avx, avy) = (ReduceSimd::splat(ax), ReduceSimd::splat(ay));
    let edge = |x1: ReduceSimd, x2: ReduceSimd, y1: ReduceSimd, y2: ReduceSimd| {
        let area2 = (x1 - avx) * (y2 - avy) - (x2 - avx) * (y1 - avy);
        (area2, area2 * (avx + x1 + x2), area2 * (avy + y1 + y2))
    };
    let lanes = x0.len() * REDUCE_LANES;
    simd_reduce_f64(
        pairs,
        (
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
        ),
        (),
        |(sa, sx, sy), (), start| {
            let chunk = start / REDUCE_LANES;
            let (da, dx, dy) = edge(
                ReduceSimd::from_array(x0[chunk]),
                ReduceSimd::from_array(x1c[chunk]),
                ReduceSimd::from_array(y0[chunk]),
                ReduceSimd::from_array(y1c[chunk]),
            );
            ((sa + da, sx + dx, sy + dy), ())
        },
        |(), range| {
            if range.start != 0 {
                debug_assert_eq!(range.start, lanes);
            }
        },
        |(mut sa, mut sx, mut sy), ()| {
            if lanes < pairs {
                let (da, dx, dy) = edge(
                    ReduceSimd::load_or_default(&xs[lanes..pairs]),
                    ReduceSimd::load_or_default(&xs[lanes + 1..=pairs]),
                    ReduceSimd::load_or_default(&ys[lanes..pairs]),
                    ReduceSimd::load_or_default(&ys[lanes + 1..=pairs]),
                );
                sa += da;
                sx += dx;
                sy += dy;
            }
            (sa.reduce_sum(), sx.reduce_sum(), sy.reduce_sum())
        },
    )
}

pub(crate) fn line_length(points: &CoordSeq) -> f64 {
    line_length_columns(points.xs(), points.ys())
}

/// [`line_length`] straight over ordinate slices — the packed-`Lines`
/// batch lane reads its CSR windows through this with no `CoordSeq` view
/// (and so no `Arc` traffic) per row.
pub(crate) fn line_length_columns(xs: &[f64], ys: &[f64]) -> f64 {
    line_length_columns_dims::<false>(xs, ys, &[])
}

/// 3D sibling of [`line_length`]: `sqrt(dx² + dy² + dz²)` over contiguous
/// columns. The scalar fold pays TWO chained `hypot` libcalls per segment,
/// so the wide lanes win even more than in 2D; the same rare
/// huge-coordinate chunks fall back to the overflow-safe scalar chain.
pub(crate) fn line_length_3d_columns(xs: &[f64], ys: &[f64], zs: &[f64]) -> f64 {
    line_length_columns_dims::<true>(xs, ys, zs)
}

/// Shared planar line-length kernel: `HAS_Z = false` is 2-D (`dx/dy` only,
/// scalar rescue via [`point_distance`]); `HAS_Z = true` folds the `dz` lane
/// and the chained-`hypot` scalar form. Op order per monomorphization matches
/// the former twin bodies so release asm stays byte-identical.
fn line_length_columns_dims<const HAS_Z: bool>(xs: &[f64], ys: &[f64], zs: &[f64]) -> f64 {
    let segments = xs.len().saturating_sub(1);
    if segments == 0 {
        return 0.0;
    }
    debug_assert!(!HAS_Z || zs.len() == xs.len());
    // The columns are already contiguous (SoA), so consecutive-vertex deltas
    // process in wide SIMD lanes with no gather; `hypot` is a scalar libcall
    // LLVM cannot auto-vectorize, hence the explicit lanes. Small inputs (the
    // common case) take the scalar guarded-sqrt fold, which doubles as the
    // overflow/underflow rescue for SIMD chunks (it falls back to `hypot`
    // itself exactly where needed).
    let (x0, _) = xs[..segments].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=segments].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..segments].as_chunks::<REDUCE_LANES>();
    let (y1, _) = ys[1..=segments].as_chunks::<REDUCE_LANES>();
    // Empty chunks when `!HAS_Z` — monomorphized away; never indexed in 2-D.
    let z0 = if HAS_Z {
        zs[..segments].as_chunks::<REDUCE_LANES>().0
    } else {
        &[]
    };
    let z1 = if HAS_Z {
        zs[1..=segments].as_chunks::<REDUCE_LANES>().0
    } else {
        &[]
    };
    let simd_fold = |mut acc: ReduceSimd, mut scalar: f64, start: usize| {
        let index = start / REDUCE_LANES;
        let dx = ReduceSimd::from_array(x1[index]) - ReduceSimd::from_array(x0[index]);
        let dy = ReduceSimd::from_array(y1[index]) - ReduceSimd::from_array(y0[index]);
        // Keep per-lane op order identical to the old twin bodies so
        // monomorphized asm matches (dz insert only in the HAS_Z arm).
        if HAS_Z {
            let dz = ReduceSimd::from_array(z1[index]) - ReduceSimd::from_array(z0[index]);
            let squared = dx * dx + dy * dy + dz * dz;
            let length = squared.sqrt();
            let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0))
                & dy.simd_eq(ReduceSimd::splat(0.0))
                & dz.simd_eq(ReduceSimd::splat(0.0));
            let underflow = squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
            if length.is_finite().all() && !underflow.any() {
                acc += length;
            } else {
                scalar += (start..start + REDUCE_LANES)
                    .map(|segment| line_length_segment_hypot::<HAS_Z>(xs, ys, zs, segment))
                    .sum::<f64>();
            }
            return (acc, scalar);
        }
        let squared = dx * dx + dy * dy;
        let length = squared.sqrt();
        let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
        let underflow = squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
        if length.is_finite().all() && !underflow.any() {
            acc += length;
        } else {
            // Rare huge-coordinate overflow or tiny nonzero underflow: fall
            // back to scalar `point_distance`, which uses `hypot` exactly for
            // these lanes.
            scalar += (start..start + REDUCE_LANES)
                .map(|segment| line_length_segment_hypot::<HAS_Z>(xs, ys, zs, segment))
                .sum::<f64>();
        }
        (acc, scalar)
    };
    let lanes = x0.len() * REDUCE_LANES;
    simd_reduce_f64(
        segments,
        ReduceSimd::splat(0.0),
        0.0,
        simd_fold,
        |scalar, range| {
            if range.start == 0 {
                return scalar
                    + range
                        .map(|segment| line_length_segment_hypot::<HAS_Z>(xs, ys, zs, segment))
                        .sum::<f64>();
            }
            scalar
                + line_length_overlap_tail::<HAS_Z>(xs, ys, zs, segments, lanes).unwrap_or_else(
                    || {
                        (lanes..segments)
                            .map(|segment| line_length_segment_hypot::<HAS_Z>(xs, ys, zs, segment))
                            .sum()
                    },
                )
        },
        |acc, scalar| acc.reduce_sum() + scalar,
    )
}

/// Scalar segment length: 2-D via [`point_distance`]; 3-D via guarded sqrt with
/// chained-`hypot` rescue (full underflow with a nonzero delta, or overflow).
fn line_length_segment_hypot<const HAS_Z: bool>(
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    segment: usize,
) -> f64 {
    if HAS_Z {
        let dx = xs[segment + 1] - xs[segment];
        let dy = ys[segment + 1] - ys[segment];
        let dz = zs[segment + 1] - zs[segment];
        let squared = dx * dx + dy * dy + dz * dz;
        if squared.is_finite() && (squared != 0.0 || (dx == 0.0 && dy == 0.0 && dz == 0.0)) {
            squared.sqrt()
        } else {
            dx.hypot(dy).hypot(dz)
        }
    } else {
        point_distance(
            Point::new_unchecked_xy(xs[segment], ys[segment]),
            Point::new_unchecked_xy(xs[segment + 1], ys[segment + 1]),
        )
    }
}

/// Masked overlap window for the non-empty SIMD tail: re-reads the last full
/// lane-width of segments, zeroes already-summed lanes, returns `None` when
/// the window needs the scalar hypot rescue.
fn line_length_overlap_tail<const HAS_Z: bool>(
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    segments: usize,
    lanes: usize,
) -> Option<f64> {
    // The last full lane-width of segments re-reads a few already-summed lanes;
    // the lane mask zeroes those, so the tail costs one unaligned window
    // instead of a scalar sqrt chain (zero-padded copies measured SLOWER).
    let start = segments - REDUCE_LANES;
    let dx = ReduceSimd::from_slice(&xs[start + 1..]) - ReduceSimd::from_slice(&xs[start..]);
    let dy = ReduceSimd::from_slice(&ys[start + 1..]) - ReduceSimd::from_slice(&ys[start..]);
    if HAS_Z {
        let dz = ReduceSimd::from_slice(&zs[start + 1..]) - ReduceSimd::from_slice(&zs[start..]);
        let lane_index = ReduceSimd::from_array(std::array::from_fn(|lane| lane as f64));
        let fresh = lane_index.simd_ge(ReduceSimd::splat((lanes - start) as f64));
        let squared = dx * dx + dy * dy + dz * dz;
        let length = fresh.select(squared.sqrt(), ReduceSimd::splat(0.0));
        let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0))
            & dy.simd_eq(ReduceSimd::splat(0.0))
            & dz.simd_eq(ReduceSimd::splat(0.0));
        let underflow = fresh & squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
        (length.is_finite().all() && !underflow.any()).then(|| length.reduce_sum())
    } else {
        let lane_index = ReduceSimd::from_array(std::array::from_fn(|lane| lane as f64));
        let fresh = lane_index.simd_ge(ReduceSimd::splat((lanes - start) as f64));
        let squared = dx * dx + dy * dy;
        let length = fresh.select(squared.sqrt(), ReduceSimd::splat(0.0));
        let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
        let underflow = fresh & squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
        (length.is_finite().all() && !underflow.any()).then(|| length.reduce_sum())
    }
}
