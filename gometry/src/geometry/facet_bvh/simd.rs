use std::simd::Simd;
use std::simd::num::SimdFloat as _;

use crate::geometry::facet_bvh::FACET_SEGMENTS;

type FacetSimd = Simd<f64, FACET_SEGMENTS>;

/// The full-facet SIMD kernel: minimum clamped-projection squared distance
/// from `(px, py)` to the 8 consecutive segments spanned by the 9 vertices
/// in `xs`/`ys`, in portable SIMD lanes the compiler lowers for the build
/// target (the x86-64-v2 baseline splits the 8-lane vectors across SSE
/// registers).
///
/// Degenerate (zero-length) segments have `dot == 0` exactly, so flooring
/// the divisor keeps their projection at the start point (0/floor = 0) with
/// no mask. Subnormal-length segments divide by the floor instead of their
/// true square — their endpoints are ~1e-154 apart, far below measurement
/// resolution either way.
pub(crate) fn simd_point_facet_distance_squared(xs: &[f64], ys: &[f64], px: f64, py: f64) -> f64 {
    let x0 = FacetSimd::from_slice(&xs[..FACET_SEGMENTS]);
    let y0 = FacetSimd::from_slice(&ys[..FACET_SEGMENTS]);
    let x1 = FacetSimd::from_slice(&xs[1..=FACET_SEGMENTS]);
    let y1 = FacetSimd::from_slice(&ys[1..=FACET_SEGMENTS]);
    let dx = x1 - x0;
    let dy = y1 - y0;
    // Plain SIMD mul+add, NOT `StdFloat::mul_add`: without an FMA target
    // feature (the x86-64-v2 baseline) LLVM legalizes vector fma to a libm
    // `fma` CALL PER LANE — confirmed in the disassembly (7+ indirect
    // calls per refine). Distances are measurements; both forms carry the
    // same 1-ulp grade, and the scalar twin already uses plain ops.
    let length2 = (dx * dx + dy * dy).simd_max(FacetSimd::splat(f64::MIN_POSITIVE));
    let qx = FacetSimd::splat(px) - x0;
    let qy = FacetSimd::splat(py) - y0;
    let zero = FacetSimd::splat(0.0);
    let fraction = ((qx * dx + qy * dy) / length2).simd_clamp(zero, FacetSimd::splat(1.0));
    // Keep the residual in the segment-start frame. Reconstructing a world-
    // coordinate foot before subtracting the probe loses a small distance at
    // large (including ordinary UTM) coordinate bases.
    let ex = qx - fraction * dx;
    let ey = qy - fraction * dy;
    (ex * ex + ey * ey).reduce_min()
}
