use std::hint::unlikely;
use std::simd::num::SimdFloat as _;

use crate::color::{
    BLUE, ComponentVectors, GREEN, LANES, MAX_COMPONENTS, RED, V4, component_vectors, row_vectors,
};
use crate::errors::BlurhashError;
use crate::{RgbDimensions, base83, cos, srgb};

const AC_QUANT_LEVELS: usize = 19;
const AC_QUANT_LEVELS_U32: u32 = AC_QUANT_LEVELS as u32;
const AC_QUANT_LEVELS_SQUARED: u32 = AC_QUANT_LEVELS_U32 * AC_QUANT_LEVELS_U32;
const AC_VALUE_LIMIT: u32 = AC_QUANT_LEVELS_SQUARED * AC_QUANT_LEVELS_U32;

const AC_DECODE_SCALE: [f32; AC_QUANT_LEVELS] = {
    let mut table = [0.0; AC_QUANT_LEVELS];
    let mut quant = 0_usize;
    while let Some(slot) = table.get_mut(quant) {
        let scaled = (quant as f32 - 9.0) / 9.0;
        *slot = if scaled < 0.0 {
            -(scaled * scaled)
        } else {
            scaled * scaled
        };
        quant = quant.wrapping_add(1);
    }
    table
};

pub(crate) struct DecodeLayout {
    pub(crate) num_x: usize,
    pub(crate) num_y: usize,
    num_components: usize,
    max_value: f32,
    punch: f64,
}

pub(crate) fn decode_rgb_into(
    blurhash: &str,
    dimensions: &RgbDimensions,
    layout: &DecodeLayout,
    out_pixels: &mut [u8],
) -> Result<(), BlurhashError> {
    let bytes = blurhash.as_bytes();
    validate_output_buffer(dimensions.rgb_len(), out_pixels)?;
    let mut colors = decode_component_vectors(bytes, layout)?;
    if layout.num_components == 1 || layout.punch == 0.0 {
        let rgb = colors.each_ref().map(|channel| {
            srgb::linear_to_srgb_u8(
                *channel
                    .first()
                    .expect("the component grid includes DC")
                    .as_array()
                    .first()
                    .expect("a component vector includes its first lane"),
            )
        });
        let (pixels, []) = out_pixels.as_chunks_mut::<3>() else {
            unreachable!("the output buffer contains whole RGB pixels")
        };
        pixels.fill(rgb);
        return Ok(());
    }
    #[expect(
        clippy::float_cmp,
        reason = "exact unit contrast preserves the original DC accumulation order"
    )]
    if layout.punch == 1.0 {
        render_pixels::<false>(
            dimensions.width(),
            dimensions.height(),
            out_pixels,
            layout,
            &colors,
            [0.0; 3],
        );
    } else {
        let mut dc = [0.0; 3];
        for (channel, dc) in colors.iter_mut().zip(&mut dc) {
            let constant = channel
                .first_mut()
                .expect("the component grid includes DC")
                .as_mut_array()
                .first_mut()
                .expect("a component vector includes its first lane");
            *dc = *constant;
            *constant = 0.0;
        }
        render_pixels::<true>(
            dimensions.width(),
            dimensions.height(),
            out_pixels,
            layout,
            &colors,
            dc,
        );
    }

    Ok(())
}

pub(crate) fn decode_layout(bytes: &[u8], punch: f64) -> Result<DecodeLayout, BlurhashError> {
    if !punch.is_finite() || punch < 0.0 {
        return Err(BlurhashError::InvalidPunch);
    }
    if unlikely(bytes.len() < 6) {
        return Err(BlurhashError::BlurhashLengthMismatch {
            expected: 6,
            got: bytes.len(),
        });
    }

    let Some(&[size_byte, max_value_byte]) = bytes.first_chunk::<2>() else {
        return Err(BlurhashError::BlurhashMalformed { index: 0 });
    };
    let Some(size_flag) = base83::decode_byte(size_byte) else {
        return Err(BlurhashError::BlurhashMalformed { index: 0 });
    };
    let size_flag = size_flag as usize;
    let num_y = (size_flag / MAX_COMPONENTS).wrapping_add(1);
    let num_x = (size_flag % MAX_COMPONENTS).wrapping_add(1);
    let num_components = num_x.wrapping_mul(num_y);
    let expected_len = 2_usize.wrapping_mul(num_components).wrapping_add(4);

    if unlikely(bytes.len() != expected_len) {
        return Err(BlurhashError::BlurhashLengthMismatch {
            expected: expected_len,
            got: bytes.len(),
        });
    }

    let Some(quantised_max_value) = base83::decode_byte(max_value_byte) else {
        return Err(BlurhashError::BlurhashMalformed { index: 1 });
    };
    let quantised_max_value = f32::from(quantised_max_value);
    let max_value = (quantised_max_value + 1.0) / 166.0;

    Ok(DecodeLayout {
        num_x,
        num_y,
        num_components,
        max_value,
        punch,
    })
}

const fn validate_output_buffer(
    expected_len: usize,
    out_pixels: &[u8],
) -> Result<(), BlurhashError> {
    if out_pixels.len() != expected_len {
        return Err(BlurhashError::InvalidRGBBufferLength {
            expected: expected_len,
            got: out_pixels.len(),
        });
    }
    Ok(())
}

fn decode_component_vectors(
    bytes: &[u8],
    layout: &DecodeLayout,
) -> Result<ComponentVectors, BlurhashError> {
    let mut colors = component_vectors();
    let Some(dc_bytes) = bytes.get(2..).and_then(|suffix| suffix.first_chunk::<4>()) else {
        unreachable!("layout validation ensures a four-character DC component")
    };
    let Some(dc_value) = base83::decode_array(dc_bytes) else {
        return Err(BlurhashError::BlurhashMalformed { index: 2 });
    };
    set_component(
        &mut colors[RED],
        0,
        0,
        srgb::srgb_u8_to_linear((dc_value >> 16) as u8),
    );
    set_component(
        &mut colors[GREEN],
        0,
        0,
        srgb::srgb_u8_to_linear(((dc_value >> 8) & 255) as u8),
    );
    set_component(
        &mut colors[BLUE],
        0,
        0,
        srgb::srgb_u8_to_linear((dc_value & 255) as u8),
    );

    let blocks = layout.num_x.div_ceil(LANES);
    for idx in 1..layout.num_components {
        let start = idx.wrapping_mul(2).wrapping_add(4);
        let Some(value_bytes) = bytes
            .get(start..)
            .and_then(|suffix| suffix.first_chunk::<2>())
        else {
            unreachable!("layout validation ensures every AC component has two characters")
        };
        let Some(value) = base83::decode_array(value_bytes) else {
            return Err(BlurhashError::BlurhashMalformed { index: start });
        };
        if unlikely(value >= AC_VALUE_LIMIT) {
            return Err(BlurhashError::BlurhashMalformed { index: start });
        }

        let quant_r = (value / AC_QUANT_LEVELS_SQUARED) as usize;
        let quant_g = ((value / AC_QUANT_LEVELS_U32) % AC_QUANT_LEVELS_U32) as usize;
        let quant_b = (value % AC_QUANT_LEVELS_U32) as usize;

        #[expect(
            clippy::arithmetic_side_effects,
            reason = "decoded BlurHash component counts are in 1..=9"
        )]
        let i = idx % layout.num_x;
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "decoded BlurHash component counts are in 1..=9"
        )]
        let slot = (idx / layout.num_x)
            .wrapping_mul(blocks)
            .wrapping_add(i / LANES);
        let lane = i % LANES;

        let (r, g, b) = (
            decode_scale(quant_r) * layout.max_value,
            decode_scale(quant_g) * layout.max_value,
            decode_scale(quant_b) * layout.max_value,
        );

        set_component(&mut colors[RED], slot, lane, r);
        set_component(&mut colors[GREEN], slot, lane, g);
        set_component(&mut colors[BLUE], slot, lane, b);
    }

    Ok(colors)
}

const fn decode_scale(quant: usize) -> f32 {
    AC_DECODE_SCALE
        .get(quant)
        .copied()
        .expect("validated AC quantization fits the scale table")
}

fn set_component(channel: &mut [V4], slot: usize, lane: usize, value: f32) {
    let Some(value_slot) = channel
        .get_mut(slot)
        .and_then(|vector| vector.as_mut_array().get_mut(lane))
    else {
        unreachable!("validated component coordinates fit fixed vector storage")
    };
    *value_slot = value;
}

fn render_pixels<const SCALE_AC: bool>(
    width: usize,
    height: usize,
    out_pixels: &mut [u8],
    layout: &DecodeLayout,
    colors: &ComponentVectors,
    dc: [f32; 3],
) {
    let blocks = layout.num_x.div_ceil(LANES);
    match blocks {
        1 => {
            render_pixels_with_blocks::<1, SCALE_AC>(width, height, out_pixels, layout, colors, dc);
        },
        2 => {
            render_pixels_with_blocks::<2, SCALE_AC>(width, height, out_pixels, layout, colors, dc);
        },
        3 => {
            render_pixels_with_blocks::<3, SCALE_AC>(width, height, out_pixels, layout, colors, dc);
        },
        _ => unreachable!("component counts are decoded from one base83 size byte"),
    }
}

fn render_pixels_with_blocks<const BLOCKS: usize, const SCALE_AC: bool>(
    width: usize,
    height: usize,
    out_pixels: &mut [u8],
    layout: &DecodeLayout,
    colors: &ComponentVectors,
    dc: [f32; 3],
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
    for (cos_y_row, out_row) in cos_y
        .chunks_exact(layout.num_y)
        .zip(out_pixels.chunks_exact_mut(width.wrapping_mul(3)))
        .take(height)
    {
        let mut row = row_vectors();
        for (j, &cos_y_value) in cos_y_row.iter().enumerate() {
            let cos_y_vector = V4::splat(cos_y_value);
            let base = j.wrapping_mul(BLOCKS);
            let end = base.wrapping_add(BLOCKS);
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "this accumulates the separable BlurHash cosine basis"
            )]
            for (target, source) in row.iter_mut().zip(colors) {
                let source = source
                    .get(base..end)
                    .expect("component row fits admitted grid");
                for (target, &source) in target.iter_mut().zip(source) {
                    *target += source * cos_y_vector;
                }
            }
        }

        let (pixels, []) = out_row.as_chunks_mut::<3>() else {
            unreachable!("output rows are validated to contain whole pixels")
        };
        for (cos_x_pixel, pixel) in cos_x_pixels.iter().zip(pixels) {
            let mut acc_r = zero;
            let mut acc_g = zero;
            let mut acc_b = zero;
            let [red, green, blue] = &row;
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "this reduces the separable BlurHash cosine basis into one pixel"
            )]
            for (((&red, &green), &blue), &basis_x) in
                red.iter().zip(green).zip(blue).zip(cos_x_pixel)
            {
                acc_r += red * basis_x;
                acc_g += green * basis_x;
                acc_b += blue * basis_x;
            }

            let mut r = acc_r.reduce_sum();
            let mut g = acc_g.reduce_sum();
            let mut b = acc_b.reduce_sum();
            if SCALE_AC {
                // Scale after the bounded AC sum so finite binary64 punch
                // values cannot overflow coefficients or erase the DC term.
                let scale = |value, dc| {
                    (f64::from(value) * layout.punch + f64::from(dc)).clamp(0.0, 1.0) as f32
                };
                let [dc_r, dc_g, dc_b] = dc;
                r = scale(r, dc_r);
                g = scale(g, dc_g);
                b = scale(b, dc_b);
            }
            *pixel = [
                srgb::linear_to_srgb_u8(r),
                srgb::linear_to_srgb_u8(g),
                srgb::linear_to_srgb_u8(b),
            ];
        }
    }
}
