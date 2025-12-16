use core::hint::unlikely;
use std::simd::Simd;

use crate::base83;
use crate::cos;
use crate::srgb;

type V4 = Simd<f32, 4>;

pub type EncodeResult<T> = Result<T, &'static str>;

fn sign_pow_05(v: f32) -> f32 {
    v.signum() * v.abs().sqrt()
}

pub fn encode_rgb(
    rgb: &[u8],
    width: usize,
    height: usize,
    x_components: usize,
    y_components: usize,
) -> EncodeResult<String> {
    if unlikely(width == 0 || height == 0) {
        return Err("Image is empty");
    }
    if unlikely(x_components == 0 || x_components > 9 || y_components == 0 || y_components > 9) {
        return Err("x_components and y_components must be in 1..=9");
    }

    let expected_len = width
        .checked_mul(height)
        .and_then(|v| v.checked_mul(3))
        .ok_or("Image is too large")?;

    if unlikely(rgb.len() != expected_len) {
        return Err("Invalid RGB buffer length");
    }

    encode_rgb_impl(rgb, width, height, x_components, y_components)
}

fn encode_rgb_impl(
    rgb: &[u8],
    width: usize,
    height: usize,
    x_components: usize,
    y_components: usize,
) -> EncodeResult<String> {
    let num_components = x_components * y_components;

    let blocks = (x_components + 3) / 4;

    let cos_y = cos::cos_axis_cached(height, y_components);
    let cos_x_simd = cos::cos_axis_simd4_cached(width, x_components);

    let zero = V4::splat(0.0);
    let mut factors_r = [zero; 27];
    let mut factors_g = [zero; 27];
    let mut factors_b = [zero; 27];

    // Separable basis:
    //
    //   C[i,j] = sum_y cos_y[y,j] * (sum_x cos_x[x,i] * pixel[x,y])
    //
    // This computes all coefficients in a single image scan.
    for y in 0..height {
        let mut row_r = [zero; 3];
        let mut row_g = [zero; 3];
        let mut row_b = [zero; 3];

        let mut pixel = y * width * 3;
        for x in 0..width {
            let r = srgb::srgb_u8_to_linear(rgb[pixel + 0]);
            let g = srgb::srgb_u8_to_linear(rgb[pixel + 1]);
            let b = srgb::srgb_u8_to_linear(rgb[pixel + 2]);
            pixel += 3;

            let r_v = V4::splat(r);
            let g_v = V4::splat(g);
            let b_v = V4::splat(b);

            let base = x * blocks;
            let bases = &cos_x_simd[base..base + blocks];
            for k in 0..blocks {
                let basis_x = bases[k];
                row_r[k] += basis_x * r_v;
                row_g[k] += basis_x * g_v;
                row_b[k] += basis_x * b_v;
            }
        }

        let cos_y_row = &cos_y[y * y_components..(y + 1) * y_components];
        for j in 0..y_components {
            let cosy = V4::splat(cos_y_row[j]);
            let base = j * blocks;
            for k in 0..blocks {
                let idx = base + k;
                factors_r[idx] += row_r[k] * cosy;
                factors_g[idx] += row_g[k] * cosy;
                factors_b[idx] += row_b[k] * cosy;
            }
        }
    }

    let inv_wh = 1.0f32 / (width * height) as f32;
    let scale_ac = inv_wh * 2.0;
    for j in 0..y_components {
        let base = j * blocks;
        for block in 0..blocks {
            let scale = if unlikely(j == 0 && block == 0) {
                V4::from_array([inv_wh, scale_ac, scale_ac, scale_ac])
            } else {
                V4::splat(scale_ac)
            };
            let idx = base + block;
            factors_r[idx] *= scale;
            factors_g[idx] *= scale;
            factors_b[idx] *= scale;
        }
    }

    let mut maximum_value = 0.0f32;
    for j in 0..y_components {
        let base = j * blocks;
        for block in 0..blocks {
            let idx = base + block;
            let r = factors_r[idx].to_array();
            let g = factors_g[idx].to_array();
            let b = factors_b[idx].to_array();

            let start_lane = if j == 0 && block == 0 { 1 } else { 0 };
            let lanes = if block + 1 == blocks {
                x_components - block * 4
            } else {
                4
            };
            for lane in start_lane..lanes {
                maximum_value =
                    maximum_value.max(r[lane].abs().max(g[lane].abs()).max(b[lane].abs()));
            }
        }
    }

    let (quantised_max_value, maximum_value) = if num_components > 1 {
        let quantised = ((maximum_value * 166.0 - 0.5).floor() as i32).clamp(0, 82) as u32;
        (quantised, (quantised as f32 + 1.0) / 166.0)
    } else {
        (0, 1.0)
    };

    let total_len = 1 + 1 + 4 + 2 * (num_components - 1);
    let mut out = Vec::with_capacity(total_len);

    let size_flag = ((x_components - 1) + (y_components - 1) * 9) as u32;
    base83::push_base83(&mut out, size_flag, 1);
    base83::push_base83(&mut out, quantised_max_value, 1);

    let dc_r = factors_r[0].to_array()[0];
    let dc_g = factors_g[0].to_array()[0];
    let dc_b = factors_b[0].to_array()[0];
    let dc_value = ((srgb::linear_to_srgb_u8(dc_r) as u32) << 16)
        | ((srgb::linear_to_srgb_u8(dc_g) as u32) << 8)
        | (srgb::linear_to_srgb_u8(dc_b) as u32);
    base83::push_base83(&mut out, dc_value, 4);

    for j in 0..y_components {
        for i in 0..x_components {
            if j == 0 && i == 0 {
                continue;
            }

            let block = i / 4;
            let lane = i % 4;
            let idx = j * blocks + block;

            let r = factors_r[idx].to_array()[lane] / maximum_value;
            let g = factors_g[idx].to_array()[lane] / maximum_value;
            let b = factors_b[idx].to_array()[lane] / maximum_value;

            let quant_r = (sign_pow_05(r) * 9.0 + 9.5).floor() as i32;
            let quant_g = (sign_pow_05(g) * 9.0 + 9.5).floor() as i32;
            let quant_b = (sign_pow_05(b) * 9.0 + 9.5).floor() as i32;

            let quant_r = quant_r.clamp(0, 18) as u32;
            let quant_g = quant_g.clamp(0, 18) as u32;
            let quant_b = quant_b.clamp(0, 18) as u32;

            let ac_value = quant_r * 19 * 19 + quant_g * 19 + quant_b;
            base83::push_base83(&mut out, ac_value, 2);
        }
    }

    // Safety: we only ever push bytes from the base83 charset.
    Ok(unsafe { String::from_utf8_unchecked(out) })
}
