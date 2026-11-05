//! Separable cosine accumulation and type-guaranteed ASCII BlurHash output.

use std::ascii::Char;
use std::hint::unlikely;

use crate::color::{ComponentVectors, LANES, MAX_COMPONENTS, V4, component_vectors, row_vectors};
use crate::errors::BlurhashError;
use crate::{RgbDimensions, base83, cos, srgb};

/// Native cosine coefficients and their grid layout.
struct Factors {
    x_components: usize,
    y_components: usize,
    blocks: usize,
    vectors: ComponentVectors,
}

impl Factors {
    /// The DC coefficient at the first lane of each color channel.
    fn dc(&self) -> [f32; 3] {
        self.vectors.each_ref().map(|channel| {
            *channel
                .first()
                .expect("component storage includes DC")
                .as_array()
                .first()
                .expect("SIMD storage includes its first lane")
        })
    }

    /// Equal-length RGB lane views in wire order, excluding DC and padded lanes.
    fn ac_blocks(&self) -> impl Iterator<Item = [&[f32]; 3]> + '_ {
        let active_vectors = self
            .blocks
            .checked_mul(self.y_components)
            .expect("admitted coefficient storage fits usize");
        let last_block = self.blocks.checked_sub(1).expect("positive block count");
        let tail_lanes = self
            .x_components
            .checked_sub(
                last_block
                    .checked_mul(LANES)
                    .expect("component block offset fits admitted grid"),
            )
            .expect("final component block starts within admitted row");
        let [red, green, blue] = self.vectors.each_ref().map(|channel| {
            channel
                .get(..active_vectors)
                .expect("active coefficient prefix fits storage")
        });
        red.iter()
            .zip(green)
            .zip(blue)
            .enumerate()
            .map(move |(index, ((red, green), blue))| {
                let block = index
                    .checked_rem(self.blocks)
                    .expect("positive block count");
                let lanes = if block == last_block {
                    tail_lanes
                } else {
                    LANES
                };
                let first_lane = usize::from(index == 0);
                [red, green, blue].map(|vector| {
                    vector
                        .as_array()
                        .get(first_lane..lanes)
                        .expect("active lanes fit the SIMD block")
                })
            })
    }
}

fn sign_pow_05(v: f32) -> f32 {
    v.signum() * v.abs().sqrt()
}

/// A validated image and component grid, borrowing the input for native encoding.
///
/// Construction admits all lengths and dimensions before output allocation.
/// Writing needs no Python access and can run detached into final ASCII storage.
pub(crate) struct RgbEncoder<'a> {
    rgb: &'a [u8],
    dimensions: &'a RgbDimensions,
    x_components: u8,
    y_components: u8,
}

impl<'a> RgbEncoder<'a> {
    /// Admits the packed RGB buffer, component counts and native working layout.
    pub(crate) fn new(
        rgb: &'a [u8],
        dimensions: &'a RgbDimensions,
        x_components: u8,
        y_components: u8,
    ) -> Result<Self, BlurhashError> {
        let rgb_len = dimensions.rgb_len();
        if unlikely(rgb.len() != rgb_len) {
            return Err(BlurhashError::InvalidRGBBufferLength {
                expected: rgb_len,
                got: rgb.len(),
            });
        }
        validate_component_count("x", x_components)?;
        validate_component_count("y", y_components)?;
        dimensions
            .validate_working_storage(usize::from(x_components), usize::from(y_components))?;
        Ok(Self {
            rgb,
            dimensions,
            x_components,
            y_components,
        })
    }

    /// Four header characters plus two per coefficient.
    pub(crate) fn len(&self) -> usize {
        usize::from(self.x_components)
            .checked_mul(usize::from(self.y_components))
            .and_then(|components| components.checked_mul(2))
            .and_then(|characters| characters.checked_add(4))
            .expect("validated BlurHash character count fits usize")
    }

    /// Computes and writes the hash; panics if output is not exactly len() characters.
    pub(crate) fn write(&self, output: &mut [Char]) {
        assert_eq!(output.len(), self.len(), "BlurHash output length");
        let factors = encode_rgb_impl(
            self.rgb,
            self.dimensions,
            self.x_components,
            self.y_components,
        );
        encode_factors(&factors, output);
    }
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
) -> Factors {
    let width = dimensions.width();
    let height = dimensions.height();
    let x_components = x_components as usize;
    let y_components = y_components as usize;
    let mut factors = accumulate_factors(rgb, width, height, x_components, y_components);
    scale_factors(&mut factors, dimensions.pixels());
    factors
}

fn accumulate_factors(
    rgb: &[u8],
    width: usize,
    height: usize,
    x_components: usize,
    y_components: usize,
) -> Factors {
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
) -> Factors {
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
    let rgb_row_len = width.checked_mul(3).expect("RGB row size was admitted");
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

        #[expect(
            clippy::arithmetic_side_effects,
            reason = "floating-point SIMD accumulation cannot panic"
        )]
        for (channel, source) in factors.iter_mut().zip(&row) {
            for (target, &cos_y_value) in channel
                .as_chunks_mut::<BLOCKS>()
                .0
                .iter_mut()
                .zip(cos_y_row)
            {
                let cos_y_vector = V4::splat(cos_y_value);
                for (target, &source) in target.iter_mut().zip(source) {
                    *target += source * cos_y_vector;
                }
            }
        }
    }

    Factors {
        x_components,
        y_components,
        blocks: BLOCKS,
        vectors: factors,
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "floating-point SIMD scaling cannot panic"
)]
fn scale_factors(factors: &mut Factors, pixel_count: usize) {
    let inv_wh = 1.0_f32 / pixel_count as f32;
    let scale_ac = inv_wh * 2.0;
    let active_vectors = factors
        .blocks
        .checked_mul(factors.y_components)
        .expect("admitted coefficient storage fits usize");
    for channel in &mut factors.vectors {
        for (index, value) in channel
            .get_mut(..active_vectors)
            .expect("active coefficient prefix fits storage")
            .iter_mut()
            .enumerate()
        {
            let scale = if unlikely(index == 0) {
                V4::from_array([inv_wh, scale_ac, scale_ac, scale_ac])
            } else {
                V4::splat(scale_ac)
            };
            *value *= scale;
        }
    }
}

fn max_ac_value(factors: &Factors) -> f32 {
    let mut maximum = 0.0_f32;
    for [red, green, blue] in factors.ac_blocks() {
        for ((&red, &green), &blue) in red.iter().zip(green).zip(blue) {
            maximum = maximum.max(red.abs().max(green.abs()).max(blue.abs()));
        }
    }
    maximum
}

fn encode_factors(factors: &Factors, mut out: &mut [Char]) {
    let num_components = factors
        .x_components
        .checked_mul(factors.y_components)
        .expect("admitted component grid fits usize");
    let maximum_value = max_ac_value(factors);
    let (quantised_max_value, maximum_value) = if num_components > 1 {
        let quantised = ((maximum_value * 166.0 - 0.5).floor() as i32).clamp(0, 82) as u32;
        (quantised, (quantised as f32 + 1.0) / 166.0)
    } else {
        (0, 1.0)
    };

    let x_flag = factors
        .x_components
        .checked_sub(1)
        .expect("positive X component count");
    let y_flag = factors
        .y_components
        .checked_sub(1)
        .expect("positive Y component count");
    let size_flag = y_flag
        .checked_mul(MAX_COMPONENTS)
        .and_then(|row| row.checked_add(x_flag))
        .expect("admitted size flag fits base83") as u32;
    out = base83::write_base83::<1>(out, size_flag);
    out = base83::write_base83::<1>(out, quantised_max_value);

    let [dc_r, dc_g, dc_b] = factors.dc();
    let dc_value = (u32::from(srgb::linear_to_srgb_u8(dc_r)) << 16)
        | (u32::from(srgb::linear_to_srgb_u8(dc_g)) << 8)
        | u32::from(srgb::linear_to_srgb_u8(dc_b));
    out = base83::write_base83::<4>(out, dc_value);

    for [red, green, blue] in factors.ac_blocks() {
        for ((&r, &g), &b) in red.iter().zip(green).zip(blue) {
            let r = r / maximum_value;
            let g = g / maximum_value;
            let b = b / maximum_value;

            let quant_r = (sign_pow_05(r) * 9.0 + 9.5).floor() as i32;
            let quant_g = (sign_pow_05(g) * 9.0 + 9.5).floor() as i32;
            let quant_b = (sign_pow_05(b) * 9.0 + 9.5).floor() as i32;

            let quant_r = quant_r.clamp(0, 18) as u32;
            let quant_g = quant_g.clamp(0, 18) as u32;
            let quant_b = quant_b.clamp(0, 18) as u32;

            let ac_value = quant_r
                .checked_mul(19)
                .and_then(|value| value.checked_add(quant_g))
                .and_then(|value| value.checked_mul(19))
                .and_then(|value| value.checked_add(quant_b))
                .expect("three base19 quantization digits fit u32");
            out = base83::write_base83::<2>(out, ac_value);
        }
    }
    debug_assert!(out.is_empty());
}
