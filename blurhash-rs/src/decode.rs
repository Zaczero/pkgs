use std::hint::unlikely;
use std::simd::num::SimdFloat;

use crate::color::{
    BLUE, ComponentVectors, GREEN, LANES, MAX_COMPONENTS, RED, V4, component_vectors, row_vectors,
};
use crate::errors::Error;
use crate::{base83, cos, srgb};

const AC_QUANT_LEVELS: usize = 19;
const AC_QUANT_LEVELS_U32: u32 = AC_QUANT_LEVELS as u32;
const AC_VALUE_LIMIT: u32 = AC_QUANT_LEVELS_U32 * AC_QUANT_LEVELS_U32 * AC_QUANT_LEVELS_U32;

const AC_DECODE_SCALE: [f32; AC_QUANT_LEVELS] = {
    let mut table = [0.0; AC_QUANT_LEVELS];
    let mut quant = 0;
    while quant < table.len() {
        let scaled = (quant as f32 - 9.0) / 9.0;
        table[quant] = if scaled < 0.0 {
            -(scaled * scaled)
        } else {
            scaled * scaled
        };
        quant += 1;
    }
    table
};

struct DecodeLayout {
    num_x: usize,
    num_y: usize,
    num_components: usize,
    max_value: f32,
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
    let num_y = (size_flag / MAX_COMPONENTS) + 1;
    let num_x = (size_flag % MAX_COMPONENTS) + 1;
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
    let Some(dc_bytes) = bytes[2..].first_chunk::<4>() else {
        unreachable!("layout validation ensures a four-character DC component")
    };
    let Some(dc_value) = base83::decode_array(dc_bytes) else {
        return Err(Error::BlurhashMalformed { index: 2 });
    };
    colors[RED][0].as_mut_array()[0] = srgb::srgb_u8_to_linear((dc_value >> 16) as u8);
    colors[GREEN][0].as_mut_array()[0] = srgb::srgb_u8_to_linear(((dc_value >> 8) & 255) as u8);
    colors[BLUE][0].as_mut_array()[0] = srgb::srgb_u8_to_linear((dc_value & 255) as u8);

    let blocks = layout.num_x.div_ceil(LANES);
    for idx in 1..layout.num_components {
        let start = 4 + idx * 2;
        let Some(value_bytes) = bytes[start..].first_chunk::<2>() else {
            unreachable!("layout validation ensures every AC component has two characters")
        };
        let Some(value) = base83::decode_array(value_bytes) else {
            return Err(Error::BlurhashMalformed { index: start });
        };
        if unlikely(value >= AC_VALUE_LIMIT) {
            return Err(Error::BlurhashMalformed { index: start });
        }

        let quant_r = (value / (AC_QUANT_LEVELS_U32 * AC_QUANT_LEVELS_U32)) as usize;
        let quant_g = ((value / AC_QUANT_LEVELS_U32) % AC_QUANT_LEVELS_U32) as usize;
        let quant_b = (value % AC_QUANT_LEVELS_U32) as usize;

        let i = idx % layout.num_x;
        let slot = (idx / layout.num_x) * blocks + (i / LANES);
        let lane = i % LANES;

        let r = AC_DECODE_SCALE[quant_r] * layout.max_value;
        let g = AC_DECODE_SCALE[quant_g] * layout.max_value;
        let b = AC_DECODE_SCALE[quant_b] * layout.max_value;

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
    let blocks = layout.num_x.div_ceil(LANES);
    match blocks {
        1 => render_pixels_with_blocks::<1>(width, height, out_pixels, layout, colors),
        2 => render_pixels_with_blocks::<2>(width, height, out_pixels, layout, colors),
        3 => render_pixels_with_blocks::<3>(width, height, out_pixels, layout, colors),
        _ => unreachable!("component counts are decoded from one base83 size byte"),
    }
}

fn render_pixels_with_blocks<const BLOCKS: usize>(
    width: usize,
    height: usize,
    out_pixels: &mut [u8],
    layout: &DecodeLayout,
    colors: &ComponentVectors,
) {
    debug_assert_eq!(layout.num_x.div_ceil(LANES), BLOCKS);

    let cos_y = cos::cos_axis_cached(height, layout.num_y);
    let cos_x_simd = cos::cos_axis_simd_cached(width, layout.num_x);
    let (cos_x_pixels, []) = cos_x_simd.as_chunks::<BLOCKS>() else {
        unreachable!("cosine cache rows are generated with the selected block count")
    };
    assert_eq!(cos_x_pixels.len(), width);
    let zero = V4::splat(0.0);

    // Separable basis:
    //
    //   pixel[x,y] = sum_i cos_x[x,i] * (sum_j colors[i,j] * cos_y[y,j])
    for y in 0..height {
        let mut row = row_vectors();

        let cos_y_row = &cos_y[y * layout.num_y..(y + 1) * layout.num_y];
        for (j, &cos_y_value) in cos_y_row.iter().enumerate() {
            let cos_y_vector = V4::splat(cos_y_value);
            let base = j * BLOCKS;

            for channel in [RED, GREEN, BLUE] {
                for k in 0..BLOCKS {
                    row[channel][k] += colors[channel][base + k] * cos_y_vector;
                }
            }
        }

        let out_row = &mut out_pixels[y * width * 3..(y + 1) * width * 3];
        let (pixels, []) = out_row.as_chunks_mut::<3>() else {
            unreachable!("output rows are validated to contain whole pixels")
        };
        for (cos_x_pixel, pixel) in cos_x_pixels.iter().zip(pixels) {
            let mut acc_r = zero;
            let mut acc_g = zero;
            let mut acc_b = zero;
            for (k, &basis_x) in cos_x_pixel.iter().enumerate() {
                acc_r += row[RED][k] * basis_x;
                acc_g += row[GREEN][k] * basis_x;
                acc_b += row[BLUE][k] * basis_x;
            }

            let r = acc_r.reduce_sum();
            let g = acc_g.reduce_sum();
            let b = acc_b.reduce_sum();

            *pixel = [
                srgb::linear_to_srgb_u8(r),
                srgb::linear_to_srgb_u8(g),
                srgb::linear_to_srgb_u8(b),
            ];
        }
    }
}
