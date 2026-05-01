use std::hint::unlikely;
use std::simd::u64x2;

use crate::constants::{
    CHARSET, DECODE_INVALID, DECODE_LUT, DECODE_OFFSET, X_SCALE, X_SCALE_INV, Y_SCALE, Y_SCALE_INV,
};

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

pub fn encode(lon: f64, lat: f64, zoom: u8) -> String {
    // how many 180° half-turns (parity decides whether lon flips by 180°)
    let normalized_lat = lat + 90.0;
    let half_turns = (normalized_lat / 180.0).floor() as i64;
    let parity = (half_turns & 1) as f64;

    let encoded_x = (180.0_f64.mul_add(parity, lon + 180.0).rem_euclid(360.0) * X_SCALE) as u32;
    let encoded_y = ((180.0 - (normalized_lat.rem_euclid(360.0) - 180.0).abs()) * Y_SCALE) as u32;
    let cell = interleave_bits(encoded_x, encoded_y);

    let bit_count = zoom + 8;
    let padding = (bit_count % 3) as usize;
    let digit_count = bit_count.div_ceil(3) as usize;

    let mut out = String::with_capacity(digit_count + padding);

    for i in 0..digit_count {
        let shift = 58 - (i as u32 * 6);
        let digit = ((cell >> shift) & 0x3F) as usize;
        out.push(CHARSET[digit] as char);
    }
    for _ in 0..padding {
        out.push('-');
    }

    out
}

pub fn decode(s: &str) -> (f64, f64, u8) {
    let mut x = 0;
    let mut y = 0;
    let mut z = 0;
    let mut offsets = 0_u8;

    for c in s.bytes() {
        let packed = DECODE_LUT[c as usize];
        if unlikely(packed == DECODE_INVALID) {
            continue;
        }
        if packed == DECODE_OFFSET {
            offsets += 1;
            continue;
        }

        x = (x << 3) | u32::from(packed & 0b111);
        y = (y << 3) | u32::from(packed >> 3);
        z += 3;
    }

    let shift = 32 - z;
    x <<= shift;
    y <<= shift;
    z -= 8;
    z -= (3 - offsets) % 3;

    (
        (f64::from(x) * X_SCALE_INV) - 180.0,
        (f64::from(y) * Y_SCALE_INV) - 90.0,
        z,
    )
}
