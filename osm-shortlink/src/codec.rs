use std::hint::unlikely;
use std::simd::u64x2;

use crate::constants::{
    CHARSET, DECODE_INVALID, DECODE_LUT, DECODE_OFFSET, X_SCALE, X_SCALE_INV, Y_SCALE, Y_SCALE_INV,
};

fn interleave_bits(x: u32, y: u32) -> u64 {
    let mut v = u64x2::from_array([x as u64, y as u64]);
    v = (v | (v << 16)) & u64x2::splat(0x0000_FFFF_0000_FFFF);
    v = (v | (v << 8)) & u64x2::splat(0x00FF_00FF_00FF_00FF);
    v = (v | (v << 4)) & u64x2::splat(0x0F0F_0F0F_0F0F_0F0F);
    v = (v | (v << 2)) & u64x2::splat(0x3333_3333_3333_3333);
    v = (v | (v << 1)) & u64x2::splat(0x5555_5555_5555_5555);
    let [sx, sy] = v.to_array();
    (sx << 1) | sy
}

pub(crate) fn encode(lon: f64, lat: f64, zoom: u8) -> String {
    // how many 180° half-turns (parity decides whether lon flips by 180°)
    let t = lat + 90.0;
    let half_turns = (t / 180.0).floor() as i64;
    let parity = (half_turns & 1) as f64;

    let x = ((lon + 180.0 + 180.0 * parity).rem_euclid(360.0) * X_SCALE) as u32;
    let y = ((180.0 - (t.rem_euclid(360.0) - 180.0).abs()) * Y_SCALE) as u32;
    let c = interleave_bits(x, y);

    let n = zoom + 8;
    let r = (n % 3) as usize;
    let d = n.div_ceil(3) as usize;

    let mut out = String::with_capacity(d + r);

    for i in 0..d {
        let shift = 58 - (i as u32 * 6);
        let digit = ((c >> shift) & 0x3F) as usize;
        out.push(CHARSET[digit] as char);
    }
    for _ in 0..r {
        out.push('-');
    }

    out
}

pub(crate) fn decode(s: &str) -> (f64, f64, u8) {
    let mut x = 0;
    let mut y = 0;
    let mut z = 0;
    let mut offsets = 0u8;

    for c in s.bytes() {
        let packed = DECODE_LUT[c as usize];
        if unlikely(packed == DECODE_INVALID) {
            continue;
        }
        if packed == DECODE_OFFSET {
            offsets += 1;
            continue;
        }

        x = (x << 3) | (packed & 0b111) as u32;
        y = (y << 3) | (packed >> 3) as u32;
        z += 3;
    }

    let shift = 32 - z;
    x <<= shift;
    y <<= shift;
    z -= 8;
    z -= (3 - offsets) % 3;

    (
        (x as f64 * X_SCALE_INV) - 180.0,
        (y as f64 * Y_SCALE_INV) - 90.0,
        z,
    )
}
