use std::hint::unlikely;
use std::simd::u64x2;

use crate::constants::{
    CHARSET, DECODE_INVALID, DECODE_LUT, DECODE_OFFSET, X_SCALE, X_SCALE_INV, Y_SCALE, Y_SCALE_INV,
};
use crate::errors::{DecodeError, EncodeError};

const COORD_BITS: u8 = 32;
const MIN_ZOOM_BIT_COUNT: u8 = 8;
const BITS_PER_DIGIT: u8 = 3;
pub(crate) const MAX_ZOOM: u8 = 22;
const MAX_ZOOM_BIT_COUNT: u8 = MAX_ZOOM + MIN_ZOOM_BIT_COUNT;
const _: () = assert!(MAX_ZOOM_BIT_COUNT <= COORD_BITS);
const _: () = assert!(MAX_ZOOM_BIT_COUNT.is_multiple_of(BITS_PER_DIGIT));

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the fixed shifts are all below the u64 lane width and implement Morton bit spreading"
)]
fn interleave_bits(x: u32, y: u32) -> u64 {
    let mut v = u64x2::from_array([u64::from(x), u64::from(y)]);
    v = (v | (v << 16)) & u64x2::splat(0x0000_FFFF_0000_FFFF);
    v = (v | (v << 8)) & u64x2::splat(0x00FF_00FF_00FF_00FF);
    v = (v | (v << 4)) & u64x2::splat(0x0F0F_0F0F_0F0F_0F0F);
    v = (v | (v << 2)) & u64x2::splat(0x3333_3333_3333_3333);
    v = (v | (v << 1)) & u64x2::splat(0x5555_5555_5555_5555);
    let [sx, sy] = v.to_array();
    (sx << 1) | sy
}

pub(crate) fn encode(lon: f64, lat: f64, zoom: u8) -> Result<String, EncodeError> {
    if unlikely(zoom > MAX_ZOOM) {
        return Err(EncodeError::ZoomOutOfRange { zoom });
    }
    if unlikely(!lon.is_finite()) {
        return Err(EncodeError::NonFiniteCoordinate {
            coordinate: "longitude",
        });
    }
    if unlikely(!lat.is_finite()) {
        return Err(EncodeError::NonFiniteCoordinate {
            coordinate: "latitude",
        });
    }

    let normalized_lat = lat + 90.0;

    // In-range inputs (virtually every call) need no modular wraparound:
    // a remainder calls `fmod` on the x86-64-v2 baseline. The fast arm
    // keeps the identical remaining operations, so it is bit-exact.
    let (wrapped_x, wrapped_y) =
        if (0.0..180.0).contains(&normalized_lat) && (-180.0..180.0).contains(&lon) {
            (lon + 180.0, 180.0 - (normalized_lat - 180.0).abs())
        } else {
            // Reduce before adding offsets: large finite inputs can absorb
            // those offsets, and their quotient no longer retains parity.
            let lat = lat % 360.0;
            let lat = if lat > 180.0 {
                lat - 360.0
            } else if lat < -180.0 {
                lat + 360.0
            } else {
                lat
            };
            // Classify the pole before adding 90, which can round a nearby
            // latitude onto it. The north pole includes the longitude flip.
            let reflected = !(-90.0..90.0).contains(&lat);
            let lat = if lat >= 90.0 {
                180.0 - lat
            } else if lat < -90.0 {
                -180.0 - lat
            } else {
                lat
            };
            let lon = lon % 360.0;
            // A final modulo would wrap a value rounded up to 360 onto the
            // wrong seam; the u32 projection keeps it in the final column.
            let wrapped_x = if reflected {
                if lon < 0.0 { lon + 360.0 } else { lon }
            } else if lon < -180.0 {
                lon + 540.0
            } else if lon >= 180.0 {
                lon - 180.0
            } else {
                lon + 180.0
            };
            (wrapped_x, 180.0 - ((lat + 90.0) - 180.0).abs())
        };
    let encoded_x = (wrapped_x * X_SCALE) as u32;
    let encoded_y = (wrapped_y * Y_SCALE) as u32;
    let cell = interleave_bits(encoded_x, encoded_y);

    let bit_count = zoom.wrapping_add(MIN_ZOOM_BIT_COUNT);
    let padding = (bit_count % BITS_PER_DIGIT) as usize;
    let digit_count = bit_count.div_ceil(BITS_PER_DIGIT) as usize;

    let mut out = String::with_capacity(digit_count.wrapping_add(padding));

    for i in 0..digit_count {
        let shift = 58_u32.wrapping_sub((i as u32).wrapping_mul(6));
        let digit = ((cell >> shift) & 0x3F) as usize;
        let Some(&byte) = CHARSET.get(digit) else {
            unreachable!("six-bit digit indexes the 64-byte alphabet")
        };
        out.push(byte as char);
    }
    for _ in 0..padding {
        out.push('-');
    }

    Ok(out)
}

pub(crate) fn decode(s: &str) -> Result<(f64, f64, u8), DecodeError> {
    let mut x = 0;
    let mut y = 0;
    let mut z = 0_u8;
    let mut offset_bits = 0_u8;

    for c in s.bytes() {
        let packed = DECODE_LUT
            .get(c as usize)
            .copied()
            .expect("every byte has a lookup entry");
        if unlikely(packed == DECODE_INVALID) {
            continue;
        }
        if packed == DECODE_OFFSET {
            offset_bits = if offset_bits == 0 {
                BITS_PER_DIGIT - 1
            } else {
                offset_bits.wrapping_sub(1)
            };
            continue;
        }

        if unlikely(z == MAX_ZOOM_BIT_COUNT) {
            return Err(DecodeError::TooLong);
        }

        x = (x << BITS_PER_DIGIT) | u32::from(packed & 0b111);
        y = (y << BITS_PER_DIGIT) | u32::from(packed >> 3);
        z = z.wrapping_add(BITS_PER_DIGIT);
    }

    if unlikely(z < MIN_ZOOM_BIT_COUNT.wrapping_add(offset_bits)) {
        return Err(DecodeError::TooShort);
    }

    let shift = COORD_BITS.wrapping_sub(z);
    x <<= shift;
    y <<= shift;

    z = z.wrapping_sub(MIN_ZOOM_BIT_COUNT).wrapping_sub(offset_bits);

    Ok((
        (f64::from(x) * X_SCALE_INV) - 180.0,
        (f64::from(y) * Y_SCALE_INV) - 90.0,
        z,
    ))
}

#[cfg(kani)]
mod proofs {
    use super::interleave_bits;

    #[kani::proof]
    fn morton_bits_preserve_both_coordinate_lanes() {
        let x: u32 = kani::any();
        let y: u32 = kani::any();
        let bit: u32 = kani::any();
        kani::assume(bit < 32);
        let packed = interleave_bits(x, y);
        assert_eq!((packed >> (2 * bit)) & 1, u64::from((y >> bit) & 1));
        assert_eq!((packed >> (2 * bit + 1)) & 1, u64::from((x >> bit) & 1));
    }
}
