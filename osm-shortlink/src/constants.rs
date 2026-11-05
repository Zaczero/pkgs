//! Coordinate scaling and byte admission generated from the typed encoding alphabet.

use std::ascii::Char;

// 64 characters encode six interleaved coordinate bits.
pub(crate) const CHARSET: &[Char; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~"
        .as_ascii()
        .unwrap();

pub(crate) const X_SCALE: f64 = ((u32::MAX as f64) + 1.0) / 360.0;
pub(crate) const Y_SCALE: f64 = ((u32::MAX as f64) + 1.0) / 180.0;
pub(crate) const X_SCALE_INV: f64 = 360.0 / (u32::MAX as f64 + 1.0);
pub(crate) const Y_SCALE_INV: f64 = 180.0 / (u32::MAX as f64 + 1.0);

pub(crate) const DECODE_INVALID: u8 = 0xFF;
pub(crate) const DECODE_OFFSET: u8 = 0xFE;

// LUT mapping ASCII byte -> packed (x_chunk | (y_chunk << 3)).
// Special values:
// - DECODE_INVALID: not a valid shortlink character
// - DECODE_OFFSET: '-' or '=' offset (legacy) character
pub(crate) const DECODE_LUT: [u8; 256] = {
    let mut lut = [DECODE_INVALID; 256];
    *lut.get_mut(b'-' as usize).unwrap() = DECODE_OFFSET;
    *lut.get_mut(b'=' as usize).unwrap() = DECODE_OFFSET;

    let mut i = 0;
    while let Some(&byte) = CHARSET.get(i) {
        let t = i as u32;
        let x_chunk = (((t >> 1) & 1) | (((t >> 3) & 1) << 1) | (((t >> 5) & 1) << 2)) as u8;
        let y_chunk = ((t & 1) | (((t >> 2) & 1) << 1) | (((t >> 4) & 1) << 2)) as u8;
        let packed = x_chunk | (y_chunk << 3);

        *lut.get_mut(byte.to_u8() as usize)
            .expect("alphabet byte fits lookup table") = packed;
        i += 1;
    }

    // Resolve '@' for backwards compatibility.
    *lut.get_mut(b'@' as usize).unwrap() = *lut.get(b'~' as usize).unwrap();
    lut
};
