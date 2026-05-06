use std::hint::unlikely;
use std::simd::u64x2;

use crate::constants::{
    CHARSET, DECODE_INVALID, DECODE_LUT, DECODE_OFFSET, X_SCALE, X_SCALE_INV, Y_SCALE, Y_SCALE_INV,
};
use crate::errors::{DecodeError, EncodeError};

const COORD_BITS: u8 = 32;
const MIN_ZOOM_BIT_COUNT: u8 = 8;
const BITS_PER_DIGIT: u8 = 3;
pub const MAX_ZOOM: u8 = 22;
const MAX_ZOOM_BIT_COUNT: u8 = MAX_ZOOM + MIN_ZOOM_BIT_COUNT;
const _: () = assert!(MAX_ZOOM_BIT_COUNT <= COORD_BITS);
const _: () = assert!(MAX_ZOOM_BIT_COUNT % BITS_PER_DIGIT == 0);

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

pub fn encode(lon: f64, lat: f64, zoom: u8) -> Result<String, EncodeError> {
    if unlikely(zoom > MAX_ZOOM) {
        return Err(EncodeError::ZoomOutOfRange { zoom });
    }

    // how many 180° half-turns (parity decides whether lon flips by 180°)
    let normalized_lat = lat + 90.0;
    let half_turns = (normalized_lat / 180.0).floor() as i64;
    let parity = (half_turns & 1) as f64;

    let encoded_x = (180.0_f64.mul_add(parity, lon + 180.0).rem_euclid(360.0) * X_SCALE) as u32;
    let encoded_y = ((180.0 - (normalized_lat.rem_euclid(360.0) - 180.0).abs()) * Y_SCALE) as u32;
    let cell = interleave_bits(encoded_x, encoded_y);

    let bit_count = zoom + MIN_ZOOM_BIT_COUNT;
    let padding = (bit_count % BITS_PER_DIGIT) as usize;
    let digit_count = bit_count.div_ceil(BITS_PER_DIGIT) as usize;

    let mut out = String::with_capacity(digit_count + padding);

    for i in 0..digit_count {
        let shift = 58 - (i as u32 * 6);
        let digit = ((cell >> shift) & 0x3F) as usize;
        out.push(CHARSET[digit] as char);
    }
    for _ in 0..padding {
        out.push('-');
    }

    Ok(out)
}

pub fn decode(s: &str) -> Result<(f64, f64, u8), DecodeError> {
    let mut x = 0;
    let mut y = 0;
    let mut z = 0_u8;
    let mut offset_bits = 0_u8;

    for c in s.bytes() {
        let packed = DECODE_LUT[c as usize];
        if unlikely(packed == DECODE_INVALID) {
            continue;
        }
        if packed == DECODE_OFFSET {
            offset_bits = if offset_bits == 0 {
                BITS_PER_DIGIT - 1
            } else {
                offset_bits - 1
            };
            continue;
        }

        if unlikely(z == MAX_ZOOM_BIT_COUNT) {
            return Err(DecodeError::TooLong);
        }

        x = (x << BITS_PER_DIGIT) | u32::from(packed & 0b111);
        y = (y << BITS_PER_DIGIT) | u32::from(packed >> 3);
        z += BITS_PER_DIGIT;
    }

    if unlikely(z < MIN_ZOOM_BIT_COUNT + offset_bits) {
        return Err(DecodeError::TooShort);
    }

    let shift = COORD_BITS - z;
    x <<= shift;
    y <<= shift;

    z -= MIN_ZOOM_BIT_COUNT;
    z -= offset_bits;

    Ok((
        (f64::from(x) * X_SCALE_INV) - 180.0,
        (f64::from(y) * Y_SCALE_INV) - 90.0,
        z,
    ))
}
