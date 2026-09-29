#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::ops::Not as _;
use std::simd::StdFloat as _;
use std::simd::cmp::SimdPartialEq as _;
use std::simd::num::SimdFloat as _;

use crate::geometry::access::{REDUCE_LANES, REDUCE_SIMD_MIN, ReduceSimd};
use crate::geometry::{CoordSeq, Point, Result, point_distance, squared_norm_is_trustworthy};

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
/// IEEE-754 f64 exponent field mask (bits 52..62).
const EXP_FIELD_MASK: u64 = 0x7FF0_0000_0000_0000;

/// SIMD sibling of [`crate::geometry::squared_norm_is_trustworthy`]: a lane is
/// **untrusted** (needs cold hypot rescue) when the squared residual is not a
/// normal positive **and** not exact zero with all-zero deltas. Exponent-bit
/// classification — same rule as the scalar path, not a third variant.
///
/// Public so packed pair-distance and other column SIMD lanes share one rule
/// (a private copy that only rescued exact-zero underflow left multi-scale
/// packed distance wrong on positive-subnormal squares).
pub(crate) fn squared_norm_untrusted_mask(
    squared: ReduceSimd,
    zero_delta: std::simd::Mask<i64, REDUCE_LANES>,
) -> std::simd::Mask<i64, REDUCE_LANES> {
    use std::simd::cmp::SimdPartialEq as _;
    let bits = squared.to_bits();
    let exp = bits & std::simd::Simd::<u64, REDUCE_LANES>::splat(EXP_FIELD_MASK);
    let is_normal = exp.simd_ne(std::simd::Simd::splat(0))
        & exp.simd_ne(std::simd::Simd::splat(EXP_FIELD_MASK));
    let is_zero = squared.simd_eq(ReduceSimd::splat(0.0));
    // trusted = is_normal | (is_zero & zero_delta); untrusted = !trusted
    (is_normal | (is_zero & zero_delta)).not()
}

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
        count = count.wrapping_add(
            vector(chunk.wrapping_mul(REDUCE_LANES))
                .to_bitmask()
                .count_ones() as usize,
        );
    }
    let lanes = chunks.wrapping_mul(REDUCE_LANES);
    for index in lanes..len {
        if scalar(index) {
            count = count.wrapping_add(1);
        }
    }
    let mut indices = Vec::with_capacity(count);
    for chunk in 0..chunks {
        let start = chunk.wrapping_mul(REDUCE_LANES);
        let bitmask = vector(start).to_bitmask();
        for lane in 0..REDUCE_LANES {
            if bitmask & (1_u64 << lane) != 0 {
                indices.push(start.wrapping_add(lane));
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
        let start = chunk.wrapping_mul(REDUCE_LANES);
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
                best_index = start.wrapping_add(lane);
            }
        } else {
            for index in start..start.wrapping_add(REDUCE_LANES) {
                let score = scalar_score(index);
                if score.total_cmp(&best_score).is_lt() {
                    best_score = score;
                    best_index = index;
                }
            }
        }
    }
    let lanes = chunks.wrapping_mul(REDUCE_LANES);
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
            vector(chunk.wrapping_mul(REDUCE_LANES)).copy_to_slice(out_chunk);
        }
    }
    let lanes = chunks.wrapping_mul(REDUCE_LANES);
    for (offset, value) in out.split_at_mut(lanes).1.iter_mut().enumerate() {
        *value = scalar(lanes.wrapping_add(offset));
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
        acc = combine_mask(acc, vector(chunk.wrapping_mul(REDUCE_LANES)));
    }
    if let Some(done) = done_mask(acc) {
        return done;
    }
    let lanes = chunks.wrapping_mul(REDUCE_LANES);
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
        let (next_acc, next_scalar) = fold_simd(acc, scalar, chunk.wrapping_mul(REDUCE_LANES));
        acc = next_acc;
        scalar = next_scalar;
    }
    let lanes = chunks.wrapping_mul(REDUCE_LANES);
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
        let inputs = std::iter::zip(
            std::iter::zip(lx.iter(), ly.iter()),
            std::iter::zip(rx.iter(), ry.iter()),
        );
        for (((&lx, &ly), (&rx, &ry)), out) in std::iter::zip(inputs, out.iter_mut()) {
            *out = scalar(lx, ly, rx, ry);
        }
        return;
    }
    let (lx_chunks, lx_tail) = lx.as_chunks::<REDUCE_LANES>();
    let (ly_chunks, ly_tail) = ly.as_chunks::<REDUCE_LANES>();
    let (rx_chunks, rx_tail) = rx.as_chunks::<REDUCE_LANES>();
    let (ry_chunks, ry_tail) = ry.as_chunks::<REDUCE_LANES>();
    {
        // Zip rather than index. The five chunk slices come from five
        // independent `&[f64]` parameters, so nothing tells the optimizer they
        // share a length: indexing four of them off the fifth's counter left a
        // bounds check per column per chunk, and each check is a side exit into
        // a panic block. Those extra exits are the expensive part — they block
        // store sinking and make the unroller conservative, and the wide
        // 8-lane logical vector exists precisely to *get* that unroll on the
        // 128-bit baseline. Zipping makes shortest-length structural, so the
        // loop has one exit and no checks. `columns_within` in
        // `geometry/util.rs` is the same shape.
        let (out_chunks, _) = out.as_chunks_mut::<REDUCE_LANES>();
        let inputs = std::iter::zip(
            std::iter::zip(lx_chunks.iter(), ly_chunks.iter()),
            std::iter::zip(rx_chunks.iter(), ry_chunks.iter()),
        );
        for (((lx_chunk, ly_chunk), (rx_chunk, ry_chunk)), out_chunk) in
            std::iter::zip(inputs, out_chunks)
        {
            let (distance, bad) = vector(
                ReduceSimd::from_array(*lx_chunk),
                ReduceSimd::from_array(*ly_chunk),
                ReduceSimd::from_array(*rx_chunk),
                ReduceSimd::from_array(*ry_chunk),
            );
            // Materialize the lane array only on the rescue path. Writing it
            // unconditionally and then patching individual lanes makes it an
            // address-taken local, which forces a stack round-trip (8 stores +
            // 8 loads per 64-byte chunk) on the overwhelmingly common
            // all-good-lanes path.
            if bad.any() {
                let mut chunk_out = distance.to_array();
                let lane_inputs = std::iter::zip(
                    std::iter::zip(lx_chunk.iter(), ly_chunk.iter()),
                    std::iter::zip(rx_chunk.iter(), ry_chunk.iter()),
                );
                for (lane, (value, ((&lx, &ly), (&rx, &ry)))) in
                    std::iter::zip(chunk_out.iter_mut(), lane_inputs).enumerate()
                {
                    if bad.test(lane) {
                        *value = scalar(lx, ly, rx, ry);
                    }
                }
                *out_chunk = chunk_out;
            } else {
                *out_chunk = distance.to_array();
            }
        }
    }
    let (_, out_tail) = out.as_chunks_mut::<REDUCE_LANES>();
    let inputs = std::iter::zip(
        std::iter::zip(lx_tail.iter(), ly_tail.iter()),
        std::iter::zip(rx_tail.iter(), ry_tail.iter()),
    );
    for (((&lx, &ly), (&rx, &ry)), out) in std::iter::zip(inputs, out_tail.iter_mut()) {
        *out = scalar(lx, ly, rx, ry);
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
        let inputs = std::iter::zip(
            std::iter::zip(lx.iter(), ly.iter()),
            std::iter::zip(rx.iter(), ry.iter()),
        );
        for (((&lx, &ly), (&rx, &ry)), out) in std::iter::zip(inputs, out.iter_mut()) {
            *out = scalar(lx, ly, rx, ry);
        }
        return;
    }
    let (lx_chunks, lx_tail) = lx.as_chunks::<REDUCE_LANES>();
    let (ly_chunks, ly_tail) = ly.as_chunks::<REDUCE_LANES>();
    let (rx_chunks, rx_tail) = rx.as_chunks::<REDUCE_LANES>();
    let (ry_chunks, ry_tail) = ry.as_chunks::<REDUCE_LANES>();
    {
        let (out_chunks, _) = out.as_chunks_mut::<REDUCE_LANES>();
        let inputs = std::iter::zip(
            std::iter::zip(lx_chunks.iter(), ly_chunks.iter()),
            std::iter::zip(rx_chunks.iter(), ry_chunks.iter()),
        );
        for (((lx_chunk, ly_chunk), (rx_chunk, ry_chunk)), out_chunk) in
            std::iter::zip(inputs, out_chunks)
        {
            let mask = vector(
                ReduceSimd::from_array(*lx_chunk),
                ReduceSimd::from_array(*ly_chunk),
                ReduceSimd::from_array(*rx_chunk),
                ReduceSimd::from_array(*ry_chunk),
            );
            *out_chunk = mask.to_array();
        }
    }
    let (_, out_tail) = out.as_chunks_mut::<REDUCE_LANES>();
    let inputs = std::iter::zip(
        std::iter::zip(lx_tail.iter(), ly_tail.iter()),
        std::iter::zip(rx_tail.iter(), ry_tail.iter()),
    );
    for (((&lx, &ly), (&rx, &ry)), out) in std::iter::zip(inputs, out_tail.iter_mut()) {
        *out = scalar(lx, ly, rx, ry);
    }
}

/// Map one equal-length `f64` column through a SIMD kernel.
///
/// Full `REDUCE_LANES` chunks run `vector`; the tail and inputs below
/// [`REDUCE_SIMD_MIN`] use `scalar` exclusively.
#[expect(
    clippy::indexing_slicing,
    reason = "the caller admits equal-length source and destination columns before mapping"
)]
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
    let lanes = src_chunks.len().wrapping_mul(REDUCE_LANES);
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
    if src.len() < REDUCE_SIMD_MIN {
        for (&src, dst) in std::iter::zip(src.iter(), dst.iter_mut()) {
            *dst = scalar(src)?;
        }
        return Ok(());
    }
    let (src_chunks, src_tail) = src.as_chunks::<REDUCE_LANES>();
    let (dst_chunks, dst_tail) = dst.as_chunks_mut::<REDUCE_LANES>();
    for (src_chunk, dst_chunk) in std::iter::zip(src_chunks, dst_chunks) {
        vector(ReduceSimd::from_array(*src_chunk))?.copy_to_slice(dst_chunk);
    }
    for (&src, dst) in std::iter::zip(src_tail, dst_tail) {
        *dst = scalar(src)?;
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
    if xs.len() < REDUCE_SIMD_MIN {
        let inputs = std::iter::zip(xs.iter(), ys.iter());
        let outputs = std::iter::zip(out_x.iter_mut(), out_y.iter_mut());
        for ((&x, &y), (out_x, out_y)) in std::iter::zip(inputs, outputs) {
            let (x, y) = scalar(x, y);
            *out_x = x;
            *out_y = y;
        }
        return;
    }
    let (x_chunks, x_tail) = xs.as_chunks::<REDUCE_LANES>();
    let (y_chunks, y_tail) = ys.as_chunks::<REDUCE_LANES>();
    let (ox_chunks, ox_tail) = out_x.as_chunks_mut::<REDUCE_LANES>();
    let (oy_chunks, oy_tail) = out_y.as_chunks_mut::<REDUCE_LANES>();
    let inputs = std::iter::zip(x_chunks, y_chunks);
    let outputs = std::iter::zip(ox_chunks, oy_chunks);
    for ((x_chunk, y_chunk), (ox_chunk, oy_chunk)) in std::iter::zip(inputs, outputs) {
        let (vx, vy) = vector(
            ReduceSimd::from_array(*x_chunk),
            ReduceSimd::from_array(*y_chunk),
        );
        vx.copy_to_slice(ox_chunk);
        vy.copy_to_slice(oy_chunk);
    }
    let inputs = std::iter::zip(x_tail.iter(), y_tail.iter());
    let outputs = std::iter::zip(ox_tail.iter_mut(), oy_tail.iter_mut());
    for ((&x, &y), (out_x, out_y)) in std::iter::zip(inputs, outputs) {
        let (x, y) = scalar(x, y);
        *out_x = x;
        *out_y = y;
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
    if xs.len() < REDUCE_SIMD_MIN {
        for ((&x, &y), out) in std::iter::zip(std::iter::zip(xs.iter(), ys.iter()), out.iter_mut())
        {
            *out = scalar(x, y);
        }
        return;
    }
    let (x_chunks, x_tail) = xs.as_chunks::<REDUCE_LANES>();
    let (y_chunks, y_tail) = ys.as_chunks::<REDUCE_LANES>();
    let (out_chunks, out_tail) = out.as_chunks_mut::<REDUCE_LANES>();
    let inputs = std::iter::zip(x_chunks, y_chunks);
    for ((x_chunk, y_chunk), out_chunk) in std::iter::zip(inputs, out_chunks) {
        vector(
            ReduceSimd::from_array(*x_chunk),
            ReduceSimd::from_array(*y_chunk),
        )
        .copy_to_slice(out_chunk);
    }
    for ((&x, &y), out) in std::iter::zip(
        std::iter::zip(x_tail.iter(), y_tail.iter()),
        out_tail.iter_mut(),
    ) {
        *out = scalar(x, y);
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
        mean += (value - mean) / index.wrapping_add(1) as f64;
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
    let mut chunks = chunks.iter();
    Some(simd_reduce_f64(
        values.len(),
        (ReduceSimd::splat(first), ReduceSimd::splat(first)),
        (first, first),
        |(lo, hi), scalar, _start| {
            let lane =
                ReduceSimd::from_array(*chunks.next().expect("one aligned column chunk per fold"));
            ((lo.simd_min(lane), hi.simd_max(lane)), scalar)
        },
        |(mut lo, mut hi), range| {
            let values = if range.start == 0 { rest } else { tail };
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
/// columns: the SIGNED triangle-fan accumulators `(Σ 2·area, Σ 2·area·(ax +
/// x_i + x_{i+1}), Σ 2·area·(ay + y_i + y_{i+1}))` — signed cross products
/// about the anchor; the caller applies the ring's CW/CCW shell/hole sign and
/// the final `/3/area` divides. This is the centroid counterpart of the
/// packed polygon shoelace owner [`crate::geometry::polygon_area_measure_columns`]:
/// same offset-by-one column shape, three lane accumulators instead of one.
/// Zero-padded tail edges contribute exactly `0` (the base-relative cross
/// product `(-ax)(-ay) − (-ax)(-ay)` cancels). Small rings take an algebraic
/// scalar fold (measurement); large rings reduce in plain SIMD lanes. Centroid
/// is measurement arithmetic, so lane order is not part of the contract
/// (matching `area`).
/// Ordinary (world-frame) ring centroid sums. Bit-identical on typical input.
/// Extreme rings may return non-finite moments; the areal centroid drivers
/// then re-accumulate with one shared local scale so multi-ring weights stay
/// consistent and the world moment (~e³) is never formed.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "SIMD floating-point expressions retain the centroid's established binary64 operation order"
)]
pub(crate) fn centroid_ring_sums(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    anchor: (f64, f64),
) -> (f64, f64, f64) {
    let (ax, ay) = anchor;
    let scalar_fold = |range: std::ops::Range<usize>| {
        let xs = xs
            .get(range.start..=range.end)
            .expect("ring segment range fits the x column");
        let ys = ys
            .get(range.start..=range.end)
            .expect("ring segment range fits the y column");
        let mut sums = (0.0_f64, 0.0_f64, 0.0_f64);
        for (&[x1, x2], &[y1, y2]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
            let area2 = x1
                .algebraic_sub(ax)
                .algebraic_mul(y2.algebraic_sub(ay))
                .algebraic_sub(x2.algebraic_sub(ax).algebraic_mul(y1.algebraic_sub(ay)));
            sums.0 = sums.0.algebraic_add(area2);
            sums.1 = sums
                .1
                .algebraic_add(area2.algebraic_mul(ax.algebraic_add(x1).algebraic_add(x2)));
            sums.2 = sums
                .2
                .algebraic_add(area2.algebraic_mul(ay.algebraic_add(y1).algebraic_add(y2)));
        }
        sums
    };
    if pairs < super::crossover::OFFSET_PAIR_MEASURE {
        return scalar_fold(0..pairs);
    }
    let x_pairs = xs.get(..pairs).expect("ring pairs fit the x column");
    let x_shifted = xs.get(1..).expect("ring pairs have a following x ordinate");
    let y_pairs = ys.get(..pairs).expect("ring pairs fit the y column");
    let y_shifted = ys.get(1..).expect("ring pairs have a following y ordinate");
    let (x0, _) = x_pairs.as_chunks::<REDUCE_LANES>();
    let (x1c, _) = x_shifted.as_chunks::<REDUCE_LANES>();
    let (y0, _) = y_pairs.as_chunks::<REDUCE_LANES>();
    let (y1c, _) = y_shifted.as_chunks::<REDUCE_LANES>();
    let (avx, avy) = (ReduceSimd::splat(ax), ReduceSimd::splat(ay));
    let edge = |x1: ReduceSimd, x2: ReduceSimd, y1: ReduceSimd, y2: ReduceSimd| {
        let area2 = (x1 - avx) * (y2 - avy) - (x2 - avx) * (y1 - avy);
        (area2, area2 * (avx + x1 + x2), area2 * (avy + y1 + y2))
    };
    let mut chunks = std::iter::zip(
        std::iter::zip(x0.iter(), x1c.iter()),
        std::iter::zip(y0.iter(), y1c.iter()),
    );
    let lanes = x0.len().wrapping_mul(REDUCE_LANES);
    simd_reduce_f64(
        pairs,
        (
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
        ),
        (),
        |(sa, sx, sy), (), _start| {
            let ((x0, x1), (y0, y1)) = chunks.next().expect("SIMD chunks match the ring pairs");
            let (da, dx, dy) = edge(
                ReduceSimd::from_array(*x0),
                ReduceSimd::from_array(*x1),
                ReduceSimd::from_array(*y0),
                ReduceSimd::from_array(*y1),
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
                let x0 = xs.get(lanes..pairs).expect("ring tail fits the x column");
                let x1 = xs
                    .get(lanes.wrapping_add(1)..=pairs)
                    .expect("shifted ring tail fits the x column");
                let y0 = ys.get(lanes..pairs).expect("ring tail fits the y column");
                let y1 = ys
                    .get(lanes.wrapping_add(1)..=pairs)
                    .expect("shifted ring tail fits the y column");
                let (da, dx, dy) = edge(
                    ReduceSimd::load_or_default(x0),
                    ReduceSimd::load_or_default(x1),
                    ReduceSimd::load_or_default(y0),
                    ReduceSimd::load_or_default(y1),
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
/// and the chained-`hypot` scalar form.
///
/// Guard shape: one combined squared-value bad-lane mask *before* the vector
/// `sqrt`, then cold scalar-hypot rescue only for flagged lanes. The residual
/// tail (segments not filling a full SIMD chunk) is a plain scalar fold — no
/// overlapping tail that re-evaluates already-reduced segments.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the SIMD squared-distance and ordered length folds define the existing measurement and rescue boundary"
)]
#[expect(
    clippy::indexing_slicing,
    reason = "the chunk indices are bounded by equal-length windows from admitted coordinate columns"
)]
fn line_length_columns_dims<const HAS_Z: bool>(xs: &[f64], ys: &[f64], zs: &[f64]) -> f64 {
    let segments = xs.len().saturating_sub(1);
    if segments == 0 {
        return 0.0;
    }
    debug_assert!(!HAS_Z || zs.len() == xs.len());
    // Short runs (roads / building rings): a tight scalar guarded-sqrt fold
    // beats the SIMD driver setup once the residual tail is no longer an
    // overlapping replay. Measured crossover stays at one full chunk for the
    // long-line path below.
    if segments < REDUCE_SIMD_MIN {
        return if HAS_Z {
            std::iter::zip(
                std::iter::zip(xs.array_windows::<2>(), ys.array_windows::<2>()),
                zs.array_windows::<2>(),
            )
            .map(|((x, y), z)| {
                let [x0, x1] = *x;
                let [y0, y1] = *y;
                let [z0, z1] = *z;
                line_length_segment_values::<HAS_Z>(x0, x1, y0, y1, z0, z1)
            })
            .sum()
        } else {
            std::iter::zip(xs.array_windows::<2>(), ys.array_windows::<2>())
                .map(|(x, y)| {
                    let [x0, x1] = *x;
                    let [y0, y1] = *y;
                    line_length_segment_values::<HAS_Z>(x0, x1, y0, y1, 0.0, 0.0)
                })
                .sum()
        };
    }
    // Contiguous SoA columns → consecutive-vertex deltas in wide lanes with no
    // gather. Compact guard: one combined squared-value bad mask before sqrt;
    // cold scalar-hypot only for flagged lanes. Residual tail is scalar only
    // (no overlap replay of already-reduced segments).
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
        if HAS_Z {
            let dz = ReduceSimd::from_array(z1[index]) - ReduceSimd::from_array(z0[index]);
            let squared = dx * dx + dy * dy + dz * dz;
            let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0))
                & dy.simd_eq(ReduceSimd::splat(0.0))
                & dz.simd_eq(ReduceSimd::splat(0.0));
            let bad = squared_norm_untrusted_mask(squared, zero_delta);
            if bad.any() {
                let length = squared.sqrt();
                let bits = bad.to_bitmask();
                let mut chunk = length.to_array();
                for (lane, slot) in chunk.iter_mut().enumerate() {
                    if bits & (1_u64 << lane) != 0 {
                        *slot = line_length_segment_hypot::<HAS_Z>(
                            xs,
                            ys,
                            zs,
                            start.wrapping_add(lane),
                        );
                    }
                }
                scalar += chunk.iter().sum::<f64>();
            } else {
                acc += squared.sqrt();
            }
            return (acc, scalar);
        }
        let squared = dx * dx + dy * dy;
        let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
        let bad = squared_norm_untrusted_mask(squared, zero_delta);
        if bad.any() {
            let length = squared.sqrt();
            let bits = bad.to_bitmask();
            let mut chunk = length.to_array();
            for (lane, slot) in chunk.iter_mut().enumerate() {
                if bits & (1_u64 << lane) != 0 {
                    *slot =
                        line_length_segment_hypot::<HAS_Z>(xs, ys, zs, start.wrapping_add(lane));
                }
            }
            scalar += chunk.iter().sum::<f64>();
        } else {
            acc += squared.sqrt();
        }
        (acc, scalar)
    };
    simd_reduce_f64(
        segments,
        ReduceSimd::splat(0.0),
        0.0,
        simd_fold,
        |scalar, range| {
            debug_assert!(range.start >= REDUCE_SIMD_MIN);
            scalar
                + range
                    .map(|segment| line_length_segment_hypot::<HAS_Z>(xs, ys, zs, segment))
                    .sum::<f64>()
        },
        |acc, scalar| acc.reduce_sum() + scalar,
    )
}

/// Scalar segment length: 2-D via [`point_distance`]; 3-D via guarded sqrt with
/// chained-`hypot` rescue (full underflow with a nonzero delta, or overflow).
fn line_length_segment_values<const HAS_Z: bool>(
    x0: f64,
    x1: f64,
    y0: f64,
    y1: f64,
    z0: f64,
    z1: f64,
) -> f64 {
    if HAS_Z {
        let dx = x1 - x0;
        let dy = y1 - y0;
        let dz = z1 - z0;
        let squared = dx * dx + dy * dy + dz * dz;
        if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0 && dz == 0.0) {
            squared.sqrt()
        } else {
            dx.hypot(dy).hypot(dz)
        }
    } else {
        point_distance(Point::new_raw_xy(x0, y0), Point::new_raw_xy(x1, y1))
    }
}

/// Locate and measure one segment on the scalar fallback path.
#[expect(
    clippy::indexing_slicing,
    reason = "scalar segment ordinals come from the admitted line range and read its adjacent vertices"
)]
fn line_length_segment_hypot<const HAS_Z: bool>(
    xs: &[f64],
    ys: &[f64],
    zs: &[f64],
    segment: usize,
) -> f64 {
    let next = segment.wrapping_add(1);
    let (x0, x1) = (xs[segment], xs[next]);
    let (y0, y1) = (ys[segment], ys[next]);
    let [z0, z1] = if HAS_Z {
        [zs[segment], zs[next]]
    } else {
        [0.0, 0.0]
    };
    line_length_segment_values::<HAS_Z>(x0, x1, y0, y1, z0, z1)
}
