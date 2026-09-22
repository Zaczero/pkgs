use std::hint::unlikely;

use crate::color::{
    BLUE, ComponentVectors, GREEN, LANES, MAX_COMPONENTS, RED, V4, component_vectors, row_vectors,
};
use crate::errors::BlurhashError;
use crate::{RgbDimensions, base83, cos, srgb};

struct ComponentGrid {
    x_components: usize,
    y_components: usize,
    blocks: usize,
    vectors: ComponentVectors,
}

fn sign_pow_05(v: f32) -> f32 {
    v.signum() * v.abs().sqrt()
}

pub(crate) fn encode_rgb(
    rgb: &[u8],
    dimensions: &RgbDimensions,
    x_components: u8,
    y_components: u8,
) -> Result<String, BlurhashError> {
    let rgb_len = dimensions.rgb_len();
    if unlikely(rgb.len() != rgb_len) {
        return Err(BlurhashError::InvalidRGBBufferLength {
            expected: rgb_len,
            got: rgb.len(),
        });
    }

    validate_component_count("x", x_components)?;
    validate_component_count("y", y_components)?;
    dimensions.validate_working_storage(usize::from(x_components), usize::from(y_components))?;

    Ok(encode_rgb_impl(rgb, dimensions, x_components, y_components))
}

fn validate_component_count(axis: &'static str, count: u8) -> Result<(), BlurhashError> {
    if unlikely(count == 0 || usize::from(count) > MAX_COMPONENTS) {
        return Err(BlurhashError::InvalidComponentCount { axis, got: count });
    }
    Ok(())
}

fn encode_rgb_impl(
    rgb: &[u8],
    dimensions: &RgbDimensions,
    x_components: u8,
    y_components: u8,
) -> String {
    let width = dimensions.width();
    let height = dimensions.height();
    let x_components = x_components as usize;
    let y_components = y_components as usize;
    let mut factors = accumulate_factors(rgb, width, height, x_components, y_components);
    scale_factors(&mut factors, dimensions.pixels());
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
    let rgb_row_len = width.wrapping_mul(3);
    for (rgb_row, cos_y_row) in rgb
        .chunks_exact(rgb_row_len)
        .zip(cos_y.chunks_exact(y_components))
        .take(height)
    {
        let mut row = row_vectors();

        for (cos_x_pixel, pixel) in cos_x_pixels.iter().zip(rgb_row.as_chunks::<3>().0) {
            let &[r, g, b] = pixel;
            let r = srgb::srgb_u8_to_linear(r);
            let g = srgb::srgb_u8_to_linear(g);
            let b = srgb::srgb_u8_to_linear(b);

            let r_v = V4::splat(r);
            let g_v = V4::splat(g);
            let b_v = V4::splat(b);

            let [red, green, blue] = &mut row;
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "this accumulates the separable BlurHash cosine basis"
            )]
            for (((red, green), blue), &basis_x) in
                red.iter_mut().zip(green).zip(blue).zip(cos_x_pixel)
            {
                *red += basis_x * r_v;
                *green += basis_x * g_v;
                *blue += basis_x * b_v;
            }
        }

        for (j, &cos_y_value) in cos_y_row.iter().enumerate() {
            let cos_y_vector = V4::splat(cos_y_value);
            let base = j.wrapping_mul(BLOCKS);
            let end = base.wrapping_add(BLOCKS);
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "this accumulates the separable BlurHash cosine basis"
            )]
            for (target, source) in factors.iter_mut().zip(&row) {
                let target = target
                    .get_mut(base..end)
                    .expect("component row fits admitted grid");
                for (target, &source) in target.iter_mut().zip(source) {
                    *target += source * cos_y_vector;
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

fn scale_factors(factors: &mut ComponentGrid, pixel_count: usize) {
    let inv_wh = 1.0_f32 / pixel_count as f32;
    let scale_ac = inv_wh * 2.0;
    for j in 0..factors.y_components {
        let base = j.wrapping_mul(factors.blocks);
        for block in 0..factors.blocks {
            let scale = if unlikely(j == 0 && block == 0) {
                V4::from_array([inv_wh, scale_ac, scale_ac, scale_ac])
            } else {
                V4::splat(scale_ac)
            };
            let idx = base.wrapping_add(block);
            scale_component(&mut factors.vectors[RED], idx, scale);
            scale_component(&mut factors.vectors[GREEN], idx, scale);
            scale_component(&mut factors.vectors[BLUE], idx, scale);
        }
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "scaling a decoded floating-point component is the operation's numerical contract"
)]
fn scale_component(channel: &mut [V4], index: usize, scale: V4) {
    let Some(value) = channel.get_mut(index) else {
        unreachable!("validated component index fits vector storage")
    };
    *value *= scale;
}

fn max_ac_value(factors: &ComponentGrid) -> f32 {
    let mut maximum_value = 0.0_f32;
    for j in 0..factors.y_components {
        let base = j.wrapping_mul(factors.blocks);
        for block in 0..factors.blocks {
            let idx = base.wrapping_add(block);
            let Some(red) = factors.vectors[RED].get(idx) else {
                unreachable!("validated component index fits red storage")
            };
            let Some(green) = factors.vectors[GREEN].get(idx) else {
                unreachable!("validated component index fits green storage")
            };
            let Some(blue) = factors.vectors[BLUE].get(idx) else {
                unreachable!("validated component index fits blue storage")
            };
            let r = red.to_array();
            let g = green.to_array();
            let b = blue.to_array();

            let start_lane = usize::from(j == 0 && block == 0);
            let lanes = if block.wrapping_add(1) == factors.blocks {
                factors.x_components.wrapping_sub(block.wrapping_mul(LANES))
            } else {
                LANES
            };
            for ((&red, &green), &blue) in r
                .get(start_lane..lanes)
                .expect("active lanes fit the SIMD block")
                .iter()
                .zip(
                    g.get(start_lane..lanes)
                        .expect("active lanes fit the SIMD block"),
                )
                .zip(
                    b.get(start_lane..lanes)
                        .expect("active lanes fit the SIMD block"),
                )
            {
                maximum_value = maximum_value.max(red.abs().max(green.abs()).max(blue.abs()));
            }
        }
    }
    maximum_value
}

fn encode_factors(factors: &ComponentGrid) -> String {
    let num_components = factors.x_components.wrapping_mul(factors.y_components);
    let maximum_value = max_ac_value(factors);
    let (quantised_max_value, maximum_value) = if num_components > 1 {
        let quantised = ((maximum_value * 166.0 - 0.5).floor() as i32).clamp(0, 82) as u32;
        (quantised, (quantised as f32 + 1.0) / 166.0)
    } else {
        (0, 1.0)
    };

    let total_len = num_components
        .wrapping_sub(1)
        .wrapping_mul(2)
        .wrapping_add(6);
    let mut out = Vec::with_capacity(total_len);

    let size_flag = (factors.x_components.wrapping_sub(1).wrapping_add(
        factors
            .y_components
            .wrapping_sub(1)
            .wrapping_mul(MAX_COMPONENTS),
    )) as u32;
    base83::push_base83::<1>(&mut out, size_flag);
    base83::push_base83::<1>(&mut out, quantised_max_value);

    let dc_r = component_lane(&factors.vectors[RED], 0, 0);
    let dc_g = component_lane(&factors.vectors[GREEN], 0, 0);
    let dc_b = component_lane(&factors.vectors[BLUE], 0, 0);
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
            let idx = j.wrapping_mul(factors.blocks).wrapping_add(block);

            let r = component_lane(&factors.vectors[RED], idx, lane) / maximum_value;
            let g = component_lane(&factors.vectors[GREEN], idx, lane) / maximum_value;
            let b = component_lane(&factors.vectors[BLUE], idx, lane) / maximum_value;

            let quant_r = (sign_pow_05(r) * 9.0 + 9.5).floor() as i32;
            let quant_g = (sign_pow_05(g) * 9.0 + 9.5).floor() as i32;
            let quant_b = (sign_pow_05(b) * 9.0 + 9.5).floor() as i32;

            let quant_r = quant_r.clamp(0, 18) as u32;
            let quant_g = quant_g.clamp(0, 18) as u32;
            let quant_b = quant_b.clamp(0, 18) as u32;

            let ac_value = quant_r
                .wrapping_mul(19)
                .wrapping_mul(19)
                .wrapping_add(quant_g.wrapping_mul(19))
                .wrapping_add(quant_b);
            base83::push_base83::<2>(&mut out, ac_value);
        }
    }

    // Safety: all bytes are ASCII (base83).
    unsafe { String::from_utf8_unchecked(out) }
}

fn component_lane(channel: &[V4], index: usize, lane: usize) -> f32 {
    channel
        .get(index)
        .and_then(|vector| vector.as_array().get(lane))
        .copied()
        .expect("validated component coordinates fit vector storage")
}
