use std::hint::unlikely;
use std::num::NonZeroUsize;

use crate::color::{
    BLUE, ComponentVectors, GREEN, LANES, MAX_COMPONENTS, RED, V4, component_vectors, row_vectors,
};
use crate::errors::Error;
use crate::{base83, cos, srgb};

struct ComponentGrid {
    x_components: usize,
    y_components: usize,
    blocks: usize,
    vectors: ComponentVectors,
}

fn sign_pow_05(v: f32) -> f32 {
    v.signum() * v.abs().sqrt()
}

pub fn encode_rgb(
    rgb: &[u8],
    width: usize,
    height: usize,
    x_components: u8,
    y_components: u8,
) -> Result<String, Error> {
    let rgb_len = width * height * 3;
    if unlikely(rgb.len() != rgb_len) {
        return Err(Error::InvalidRGBBufferLength {
            expected: rgb_len,
            got: rgb.len(),
        });
    }

    let Some(width) = NonZeroUsize::new(width) else {
        return Ok(String::new());
    };
    let Some(height) = NonZeroUsize::new(height) else {
        return Ok(String::new());
    };

    validate_component_count("x", x_components)?;
    validate_component_count("y", y_components)?;

    Ok(encode_rgb_impl(
        rgb,
        width,
        height,
        x_components,
        y_components,
    ))
}

fn validate_component_count(axis: &'static str, count: u8) -> Result<(), Error> {
    if unlikely(count == 0 || usize::from(count) > MAX_COMPONENTS) {
        return Err(Error::InvalidComponentCount { axis, got: count });
    }
    Ok(())
}

fn encode_rgb_impl(
    rgb: &[u8],
    width: NonZeroUsize,
    height: NonZeroUsize,
    x_components: u8,
    y_components: u8,
) -> String {
    let width = width.get();
    let height = height.get();
    let x_components = x_components as usize;
    let y_components = y_components as usize;
    let mut factors = accumulate_factors(rgb, width, height, x_components, y_components);
    scale_factors(&mut factors, width, height);
    encode_factors(&factors)
}

fn accumulate_factors(
    rgb: &[u8],
    width: usize,
    height: usize,
    x_components: usize,
    y_components: usize,
) -> ComponentGrid {
    let blocks = x_components.div_ceil(LANES);
    match blocks {
        1 => accumulate_factors_with_blocks::<1>(rgb, width, height, x_components, y_components),
        2 => accumulate_factors_with_blocks::<2>(rgb, width, height, x_components, y_components),
        3 => accumulate_factors_with_blocks::<3>(rgb, width, height, x_components, y_components),
        _ => unreachable!("component counts are validated before encoding"),
    }
}

fn accumulate_factors_with_blocks<const BLOCKS: usize>(
    rgb: &[u8],
    width: usize,
    height: usize,
    x_components: usize,
    y_components: usize,
) -> ComponentGrid {
    debug_assert_eq!(x_components.div_ceil(LANES), BLOCKS);

    let cos_y = cos::cos_axis_cached(height, y_components);
    let cos_x_simd = cos::cos_axis_simd_cached(width, x_components);
    let (cos_x_pixels, []) = cos_x_simd.as_chunks::<BLOCKS>() else {
        unreachable!("cosine cache rows are generated with the selected block count")
    };
    assert_eq!(cos_x_pixels.len(), width);
    let mut factors = component_vectors();

    // Separable basis:
    //
    //   C[i,j] = sum_y cos_y[y,j] * (sum_x cos_x[x,i] * pixel[x,y])
    //
    // This computes all coefficients in a single image scan.
    for y in 0..height {
        let mut row = row_vectors();

        let rgb_row = &rgb[y * width * 3..(y + 1) * width * 3];
        for (cos_x_pixel, pixel) in cos_x_pixels.iter().zip(rgb_row.chunks_exact(3)) {
            let r = srgb::srgb_u8_to_linear(pixel[0]);
            let g = srgb::srgb_u8_to_linear(pixel[1]);
            let b = srgb::srgb_u8_to_linear(pixel[2]);

            let r_v = V4::splat(r);
            let g_v = V4::splat(g);
            let b_v = V4::splat(b);

            for (k, &basis_x) in cos_x_pixel.iter().enumerate() {
                row[RED][k] += basis_x * r_v;
                row[GREEN][k] += basis_x * g_v;
                row[BLUE][k] += basis_x * b_v;
            }
        }

        let cos_y_row = &cos_y[y * y_components..(y + 1) * y_components];
        for (j, &cos_y_value) in cos_y_row.iter().enumerate() {
            let cos_y_vector = V4::splat(cos_y_value);
            let base = j * BLOCKS;
            for channel in [RED, GREEN, BLUE] {
                for k in 0..BLOCKS {
                    factors[channel][base + k] += row[channel][k] * cos_y_vector;
                }
            }
        }
    }

    ComponentGrid {
        x_components,
        y_components,
        blocks: BLOCKS,
        vectors: factors,
    }
}

fn scale_factors(factors: &mut ComponentGrid, width: usize, height: usize) {
    let inv_wh = 1.0_f32 / (width * height) as f32;
    let scale_ac = inv_wh * 2.0;
    for j in 0..factors.y_components {
        let base = j * factors.blocks;
        for block in 0..factors.blocks {
            let scale = if unlikely(j == 0 && block == 0) {
                V4::from_array([inv_wh, scale_ac, scale_ac, scale_ac])
            } else {
                V4::splat(scale_ac)
            };
            let idx = base + block;
            factors.vectors[RED][idx] *= scale;
            factors.vectors[GREEN][idx] *= scale;
            factors.vectors[BLUE][idx] *= scale;
        }
    }
}

fn max_ac_value(factors: &ComponentGrid) -> f32 {
    let mut maximum_value = 0.0_f32;
    for j in 0..factors.y_components {
        let base = j * factors.blocks;
        for block in 0..factors.blocks {
            let idx = base + block;
            let r = factors.vectors[RED][idx].to_array();
            let g = factors.vectors[GREEN][idx].to_array();
            let b = factors.vectors[BLUE][idx].to_array();

            let start_lane = usize::from(j == 0 && block == 0);
            let lanes = if block + 1 == factors.blocks {
                factors.x_components - block * LANES
            } else {
                LANES
            };
            for lane in start_lane..lanes {
                maximum_value =
                    maximum_value.max(r[lane].abs().max(g[lane].abs()).max(b[lane].abs()));
            }
        }
    }
    maximum_value
}

fn encode_factors(factors: &ComponentGrid) -> String {
    let num_components = factors.x_components * factors.y_components;
    let maximum_value = max_ac_value(factors);
    let (quantised_max_value, maximum_value) = if num_components > 1 {
        let quantised = (maximum_value.mul_add(166.0, -0.5).floor() as i32).clamp(0, 82) as u32;
        (quantised, (quantised as f32 + 1.0) / 166.0)
    } else {
        (0, 1.0)
    };

    let total_len = 1 + 1 + 4 + 2 * (num_components - 1);
    let mut out = Vec::with_capacity(total_len);

    let size_flag =
        ((factors.x_components - 1) + (factors.y_components - 1) * MAX_COMPONENTS) as u32;
    base83::push_base83::<1>(&mut out, size_flag);
    base83::push_base83::<1>(&mut out, quantised_max_value);

    let dc_r = factors.vectors[RED][0].to_array()[0];
    let dc_g = factors.vectors[GREEN][0].to_array()[0];
    let dc_b = factors.vectors[BLUE][0].to_array()[0];
    let dc_value = (u32::from(srgb::linear_to_srgb_u8(dc_r)) << 16)
        | (u32::from(srgb::linear_to_srgb_u8(dc_g)) << 8)
        | u32::from(srgb::linear_to_srgb_u8(dc_b));
    base83::push_base83::<4>(&mut out, dc_value);

    for j in 0..factors.y_components {
        for i in 0..factors.x_components {
            if j == 0 && i == 0 {
                continue;
            }

            let block = i / LANES;
            let lane = i % LANES;
            let idx = j * factors.blocks + block;

            let r = factors.vectors[RED][idx].to_array()[lane] / maximum_value;
            let g = factors.vectors[GREEN][idx].to_array()[lane] / maximum_value;
            let b = factors.vectors[BLUE][idx].to_array()[lane] / maximum_value;

            let quant_r = sign_pow_05(r).mul_add(9.0, 9.5).floor() as i32;
            let quant_g = sign_pow_05(g).mul_add(9.0, 9.5).floor() as i32;
            let quant_b = sign_pow_05(b).mul_add(9.0, 9.5).floor() as i32;

            let quant_r = quant_r.clamp(0, 18) as u32;
            let quant_g = quant_g.clamp(0, 18) as u32;
            let quant_b = quant_b.clamp(0, 18) as u32;

            let ac_value = quant_r * 19 * 19 + quant_g * 19 + quant_b;
            base83::push_base83::<2>(&mut out, ac_value);
        }
    }

    // Safety: all bytes are ASCII (base83).
    unsafe { String::from_utf8_unchecked(out) }
}
