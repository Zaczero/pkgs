use core::hint::unlikely;
use std::simd::Simd;
use std::simd::num::SimdFloat;

use crate::base83;
use crate::cos;
use crate::srgb;

type V4 = Simd<f32, 4>;

pub type DecodeResult<T> = Result<T, &'static str>;

fn sign_pow_2(v: f32) -> f32 {
    let abs = v.abs();
    v.signum() * abs * abs
}

pub fn decode_rgb_into(
    blurhash: &str,
    width: usize,
    height: usize,
    punch: f32,
    out_pixels: &mut [u8],
) -> DecodeResult<()> {
    if unlikely(width == 0 || height == 0) {
        return Err("Image is empty");
    }
    if unlikely(!punch.is_finite()) {
        return Err("punch must be finite");
    }

    let bytes = blurhash.as_bytes();
    if unlikely(bytes.len() < 6) {
        return Err("blurhash is too short");
    }

    let Some(size_flag) = base83::decode_byte(bytes[0]) else {
        return Err("blurhash is malformed");
    };
    let size_flag = size_flag as usize;
    let num_y = (size_flag / 9) + 1;
    let num_x = (size_flag % 9) + 1;
    let num_components = num_x * num_y;

    let expected_len = 4 + 2 * num_components;
    if unlikely(bytes.len() != expected_len) {
        return Err("blurhash length mismatch");
    }

    let Some(quantised_max_value) = base83::decode_byte(bytes[1]) else {
        return Err("blurhash is malformed");
    };
    let quantised_max_value = quantised_max_value as f32;
    let max_value = ((quantised_max_value + 1.0) / 166.0) * punch.max(1.0);

    let out_len = width
        .checked_mul(height)
        .and_then(|v| v.checked_mul(3))
        .ok_or("Image is too large")?;

    if unlikely(out_pixels.len() != out_len) {
        return Err("Invalid output buffer length");
    }

    let blocks = (num_x + 3) / 4;

    let zero = V4::splat(0.0);
    let mut colors_r_v = [zero; 27];
    let mut colors_g_v = [zero; 27];
    let mut colors_b_v = [zero; 27];

    let Some(dc_value) = base83::decode_u32(&bytes[2..6]) else {
        return Err("blurhash is malformed");
    };
    colors_r_v[0].as_mut_array()[0] = srgb::srgb_u8_to_linear((dc_value >> 16) as u8);
    colors_g_v[0].as_mut_array()[0] = srgb::srgb_u8_to_linear(((dc_value >> 8) & 255) as u8);
    colors_b_v[0].as_mut_array()[0] = srgb::srgb_u8_to_linear((dc_value & 255) as u8);

    for idx in 1..num_components {
        let start = 4 + idx * 2;
        let Some(value) = base83::decode_u32(&bytes[start..start + 2]) else {
            return Err("blurhash is malformed");
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
        for j in 0..num_y {
            let cosy = V4::splat(cos_y_row[j]);
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

            out_pixels[out_idx + 0] = srgb::linear_to_srgb_u8(r);
            out_pixels[out_idx + 1] = srgb::linear_to_srgb_u8(g);
            out_pixels[out_idx + 2] = srgb::linear_to_srgb_u8(b);
            out_idx += 3;
        }
    }

    Ok(())
}
