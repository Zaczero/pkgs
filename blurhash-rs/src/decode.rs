use std::hint::unlikely;
use std::simd::Simd;
use std::simd::num::SimdFloat;

use crate::base83;
use crate::cos;
use crate::errors::Error;
use crate::srgb;

type V4 = Simd<f32, 4>;

fn sign_pow_2(v: f32) -> f32 {
    let abs = v.abs();
    v.signum() * abs * abs
}

pub(crate) fn decode_rgb_into(
    blurhash: &str,
    width: usize,
    height: usize,
    punch: f32,
    out_pixels: &mut [u8],
) -> Result<(), Error> {
    if unlikely(width == 0 || height == 0) {
        return Ok(());
    }

    let bytes = blurhash.as_bytes();
    if unlikely(bytes.len() < 6) {
        return Err(Error::BlurhashLengthMismatch {
            expected: 6,
            got: bytes.len(),
        });
    }

    let Some(size_flag) = base83::decode_byte(bytes[0]) else {
        return Err(Error::BlurhashMalformed { index: 0 });
    };
    let size_flag = size_flag as usize;
    let num_y = (size_flag / 9) + 1;
    let num_x = (size_flag % 9) + 1;
    let num_components = num_x * num_y;

    let expected_len = 4 + 2 * num_components;
    if unlikely(bytes.len() != expected_len) {
        return Err(Error::BlurhashLengthMismatch {
            expected: expected_len,
            got: bytes.len(),
        });
    }

    let Some(quantised_max_value) = base83::decode_byte(bytes[1]) else {
        return Err(Error::BlurhashMalformed { index: 1 });
    };
    let quantised_max_value = quantised_max_value as f32;
    let max_value = ((quantised_max_value + 1.0) / 166.0) * punch.max(1.0);

    let out_len = width * height * 3;
    if unlikely(out_pixels.len() != out_len) {
        return Err(Error::InvalidRGBBufferLength {
            expected: out_len,
            got: out_pixels.len(),
        });
    }

    let blocks = num_x.div_ceil(4);

    let zero = V4::splat(0.0);
    let mut colors_r_v = [zero; 27];
    let mut colors_g_v = [zero; 27];
    let mut colors_b_v = [zero; 27];

    let Some(dc_value) = base83::decode_u32(&bytes[2..6]) else {
        return Err(Error::BlurhashMalformed { index: 2 });
    };
    colors_r_v[0].as_mut_array()[0] = srgb::srgb_u8_to_linear((dc_value >> 16) as u8);
    colors_g_v[0].as_mut_array()[0] = srgb::srgb_u8_to_linear(((dc_value >> 8) & 255) as u8);
    colors_b_v[0].as_mut_array()[0] = srgb::srgb_u8_to_linear((dc_value & 255) as u8);

    for idx in 1..num_components {
        let start = 4 + idx * 2;
        let Some(value) = base83::decode_u32(&bytes[start..start + 2]) else {
            return Err(Error::BlurhashMalformed { index: start });
        };

        let quant_r = (value / (19 * 19)) as i32;
        let quant_g = ((value / 19) % 19) as i32;
        let quant_b = (value % 19) as i32;

        let i = idx % num_x;
        let slot = (idx / num_x) * blocks + (i / 4);
        let lane = i % 4;

        let r = sign_pow_2((quant_r as f32 - 9.0) / 9.0) * max_value;
        let g = sign_pow_2((quant_g as f32 - 9.0) / 9.0) * max_value;
        let b = sign_pow_2((quant_b as f32 - 9.0) / 9.0) * max_value;

        colors_r_v[slot].as_mut_array()[lane] = r;
        colors_g_v[slot].as_mut_array()[lane] = g;
        colors_b_v[slot].as_mut_array()[lane] = b;
    }

    let cos_y = cos::cos_axis_cached(height, num_y);
    let cos_x_simd = cos::cos_axis_simd4_cached(width, num_x);

    // Separable basis:
    //
    //   pixel[x,y] = sum_i cos_x[x,i] * (sum_j colors[i,j] * cos_y[y,j])
    for y in 0..height {
        let mut row_r = [zero; 3];
        let mut row_g = [zero; 3];
        let mut row_b = [zero; 3];

        let cos_y_row = &cos_y[y * num_y..(y + 1) * num_y];
        for (j, &cosy) in cos_y_row.iter().enumerate() {
            let cosy = V4::splat(cosy);
            let base = j * blocks;

            for k in 0..blocks {
                let idx = base + k;
                row_r[k] += colors_r_v[idx] * cosy;
                row_g[k] += colors_g_v[idx] * cosy;
                row_b[k] += colors_b_v[idx] * cosy;
            }
        }

        let mut out_idx = y * width * 3;
        for x in 0..width {
            let base = x * blocks;
            let bases = &cos_x_simd[base..base + blocks];

            let mut acc_r = zero;
            let mut acc_g = zero;
            let mut acc_b = zero;
            for k in 0..blocks {
                let basis_x = bases[k];
                acc_r += row_r[k] * basis_x;
                acc_g += row_g[k] * basis_x;
                acc_b += row_b[k] * basis_x;
            }

            let r = acc_r.reduce_sum();
            let g = acc_g.reduce_sum();
            let b = acc_b.reduce_sum();

            out_pixels[out_idx] = srgb::linear_to_srgb_u8(r);
            out_pixels[out_idx + 1] = srgb::linear_to_srgb_u8(g);
            out_pixels[out_idx + 2] = srgb::linear_to_srgb_u8(b);
            out_idx += 3;
        }
    }

    Ok(())
}
