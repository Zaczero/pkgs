//! Structural inspection of `Shape`: type/dimension/axes, point traversal,
//! parts/rings/exterior/interiors, and the bounds/area/length reducers.

use std::simd::Simd;

mod compare;
mod reduce;
mod shape;

pub(crate) use compare::{
    bounds_from_iter, compare_f64, compare_point_slices, compare_points, compare_polygons,
    compare_shapes,
};
pub(crate) use reduce::{column_all_finite, column_minmax, *};
pub(crate) use shape::{ShapePart, *};

/// Logical SIMD vector width (bits) for the column kernels — the single source
/// for SIMD lane sizing across gometry; per-type lane counts derive from it via
/// `size_of` at compile time, so retargeting is a one-line change here.
///
/// This is a LOGICAL width, intentionally wider than the v2 physical 128-bit
/// SSE register: the compiler unrolls it into several 128-bit ops, and those
/// independent `min`/`add` chains give the instruction-level parallelism that
/// hides op latency on a latency-bound reduction. 512 bits (8 `f64` lanes) is
/// the MEASURED optimum at the v2 baseline — cache-cold 200k-point `bounds`
/// (µs/op): 2 lanes 167 · 4 lanes 150 · **8 lanes 145** · 16 lanes 207 · 32
/// lanes 230. Below 8 loses ILP; past 8 the accumulators spill the 16 xmm regs.
pub(super) const SIMD_VECTOR_BITS: usize = 512;

/// `f64` lane count for the reduction kernels (`column_minmax`, `line_length`,
/// `ring_area_measure`, …), derived: `SIMD_VECTOR_BITS / f64_bits` = `512 / 64` =
/// **8**.
pub(crate) const REDUCE_LANES: usize = SIMD_VECTOR_BITS / (8 * size_of::<f64>());
pub(crate) type ReduceSimd = Simd<f64, REDUCE_LANES>;

/// Minimum element count before the wide-lane column folds beat scalar.
///
/// The folds are allocation-free (offset `as_chunks` views over the
/// column storage), so the crossover is pure lane economics: measured at
/// the 40-vertex bulk-data norm, lanes win from ~one full chunk (length
/// −37%, shoelace −10% vs the prior 64 gate); 2-5 vertex shapes stay
/// scalar. Derived as exactly one full SIMD chunk so it tracks
/// `SIMD_VECTOR_BITS` automatically.
pub(crate) const REDUCE_SIMD_MIN: usize = REDUCE_LANES;

/// Centralized scalar→SIMD crossover policy — one home for the input-size
/// thresholds at which a hand-rolled `portable_simd` reduction starts to beat
/// the clean scalar fold LLVM auto-vectorizes in release. They are NOT one
/// value: each crossover is measured per reduction shape (x86-64-v2 caps
/// auto-vec at 128-bit `mulpd`, so explicit `f64x8` only pays off once its
/// 4×128-bit ILP + horizontal-reduce cost amortizes). Below the threshold the
/// scalar fold wins (less per-call overhead, still auto-vectorized); at or
/// above it the SIMD body wins. Inspect with `cargo asm`; re-measure through
/// the interleaved `bench_ab.py` A/B harness on a representative covering
/// case before changing any value.
pub(crate) mod crossover {
    /// Offset-pair cross-product reductions (shoelace area, centroid fan):
    /// scalar wins/ties to ~64 verts (measured), SIMD wins clearly at 256+.
    pub(crate) const OFFSET_PAIR_MEASURE: usize = 32;
    /// Fused planar bbox (x/y min-max): the scalar 4-accumulator loop
    /// auto-vectorizes well to ~4k coords; only huge single rings cross over.
    pub(crate) const FUSED_XY_BOUNDS: usize = 4_096;
    /// Even-odd ray-crossing point-in-ring: the scalar loop SHORT-CIRCUITS the
    /// per-edge division on the straddle test, while the SIMD body divides every
    /// lane unconditionally — so scalar wins to large rings on typical polygons
    /// (sparse straddles). Measured crossover.
    pub(crate) const RAY_CROSSING: usize = 256;
}

#[cfg(test)]
mod tests;
