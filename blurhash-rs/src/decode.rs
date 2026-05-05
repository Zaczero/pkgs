use std::hint::unlikely;
use std::simd::num::SimdFloat;

use crate::color::{BLUE, ComponentVectors, GREEN, RED, V4, component_vectors, row_vectors};
use crate::errors::Error;
use crate::{base83, cos, srgb};

struct DecodeLayout {
    num_x: usize,
    num_y: usize,
    num_components: usize,
    max_value: f32,
}

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
) -> Result<(), Error> {
    if unlikely(width == 0 || height == 0) {
        return Ok(());
    }

    let bytes = blurhash.as_bytes();
    let layout = decode_layout(bytes, punch)?;
    validate_output_buffer(width, height, out_pixels)?;
    let colors = decode_component_vectors(bytes, &layout)?;
    render_pixels(width, height, out_pixels, &layout, &colors);

    Ok(())
}

fn decode_layout(bytes: &[u8], punch: f32) -> Result<DecodeLayout, Error> {
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
    let quantised_max_value = f32::from(quantised_max_value);
    let max_value = ((quantised_max_value + 1.0) / 166.0) * punch.max(1.0);

    Ok(DecodeLayout {
        num_x,
        num_y,
        num_components,
        max_value,
    })
}

const fn validate_output_buffer(
    width: usize,
    height: usize,
    out_pixels: &[u8],
) -> Result<(), Error> {
    let out_len = width * height * 3;
    if out_pixels.len() != out_len {
        return Err(Error::InvalidRGBBufferLength {
            expected: out_len,
            got: out_pixels.len(),
        });
    }
    Ok(())
}

fn decode_component_vectors(
    bytes: &[u8],
    layout: &DecodeLayout,
) -> Result<ComponentVectors, Error> {
    let mut colors = component_vectors();
    let Some(dc_value) = base83::decode_u32(&bytes[2..6]) else {
        return Err(Error::BlurhashMalformed { index: 2 });
    };
    colors[RED][0].as_mut_array()[0] = srgb::srgb_u8_to_linear((dc_value >> 16) as u8);
    colors[GREEN][0].as_mut_array()[0] = srgb::srgb_u8_to_linear(((dc_value >> 8) & 255) as u8);
    colors[BLUE][0].as_mut_array()[0] = srgb::srgb_u8_to_linear((dc_value & 255) as u8);

    let blocks = layout.num_x.div_ceil(4);
    for idx in 1..layout.num_components {
        let start = 4 + idx * 2;
        let Some(value) = base83::decode_u32(&bytes[start..start + 2]) else {
            return Err(Error::BlurhashMalformed { index: start });
        };

        let quant_r = (value / (19 * 19)) as i32;
        let quant_g = ((value / 19) % 19) as i32;
        let quant_b = (value % 19) as i32;

        let i = idx % layout.num_x;
        let slot = (idx / layout.num_x) * blocks + (i / 4);
        let lane = i % 4;

        let r = sign_pow_2((quant_r as f32 - 9.0) / 9.0) * layout.max_value;
        let g = sign_pow_2((quant_g as f32 - 9.0) / 9.0) * layout.max_value;
        let b = sign_pow_2((quant_b as f32 - 9.0) / 9.0) * layout.max_value;

        colors[RED][slot].as_mut_array()[lane] = r;
        colors[GREEN][slot].as_mut_array()[lane] = g;
        colors[BLUE][slot].as_mut_array()[lane] = b;
    }

    Ok(colors)
}

fn render_pixels(
    width: usize,
    height: usize,
    out_pixels: &mut [u8],
    layout: &DecodeLayout,
    colors: &ComponentVectors,
) {
    let blocks = layout.num_x.div_ceil(4);
    let cos_y = cos::cos_axis_cached(height, layout.num_y);
    let cos_x_simd = cos::cos_axis_simd4_cached(width, layout.num_x);
    let zero = V4::splat(0.0);

    // Separable basis:
    //
    //   pixel[x,y] = sum_i cos_x[x,i] * (sum_j colors[i,j] * cos_y[y,j])
    for y in 0..height {
        let mut row = row_vectors();

        let cos_y_row = &cos_y[y * layout.num_y..(y + 1) * layout.num_y];
        for (j, &cos_y_value) in cos_y_row.iter().enumerate() {
            let cos_y_vector = V4::splat(cos_y_value);
            let base = j * blocks;

            for channel in [RED, GREEN, BLUE] {
                for (row_slot, &color) in row[channel][..blocks]
                    .iter_mut()
                    .zip(&colors[channel][base..base + blocks])
                {
                    *row_slot += color * cos_y_vector;
                }
            }
        }

        let mut out_idx = y * width * 3;
        for x in 0..width {
            let base = x * blocks;
            let bases = &cos_x_simd[base..base + blocks];

            let mut acc_r = zero;
            let mut acc_g = zero;
            let mut acc_b = zero;
            for (k, &basis_x) in bases.iter().enumerate() {
                acc_r += row[RED][k] * basis_x;
                acc_g += row[GREEN][k] * basis_x;
                acc_b += row[BLUE][k] * basis_x;
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
}
