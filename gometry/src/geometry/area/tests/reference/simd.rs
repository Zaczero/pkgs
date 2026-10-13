use std::simd::num::SimdFloat as _;

use super::super::shoelace::SHOELACE_SIMD_MIN;
use super::{REDUCE_LANES, ReduceSimd, scaled_residual, simd_reduce_f64};

#[expect(
    clippy::arithmetic_side_effects,
    reason = "This test-only SIMD oracle preserves the former binary64 shoelace reduction order"
)]
pub(super) fn shoelace_shifted(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> f64 {
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    if pairs < SHOELACE_SIMD_MIN {
        let cross = xs[..pairs]
            .iter()
            .zip(&xs[1..=pairs])
            .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
            .fold(0.0_f64, |area, ((&x0, &x1), (&y0, &y1))| {
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
    let residual_x = |values: ReduceSimd, origin: ReduceSimd| {
        if identity {
            values - origin
        } else {
            values * sx_v - origin * sx_v
        }
    };
    let residual_y = |values: ReduceSimd, origin: ReduceSimd| {
        if identity {
            values - origin
        } else {
            values * sy_v - origin * sy_v
        }
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
