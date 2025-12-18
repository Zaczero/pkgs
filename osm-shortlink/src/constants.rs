// 64 chars to encode 6 bits.
pub(crate) const CHARSET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~";

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
const fn build_decode_lut() -> [u8; 256] {
    let mut lut = [DECODE_INVALID; 256];
    lut[b'-' as usize] = DECODE_OFFSET;
    lut[b'=' as usize] = DECODE_OFFSET;

    let mut i = 0;
    while i < 64 {
        let t = i as u32;
        let x_chunk = (((t >> 1) & 1) | (((t >> 3) & 1) << 1) | (((t >> 5) & 1) << 2)) as u8;
        let y_chunk = ((t & 1) | (((t >> 2) & 1) << 1) | (((t >> 4) & 1) << 2)) as u8;
        let packed = x_chunk | (y_chunk << 3);

        lut[CHARSET[i] as usize] = packed;
        i += 1;
    }

    // Resolve '@' for backwards compatibility.
    lut[b'@' as usize] = lut[b'~' as usize];
    lut
}

pub(crate) const DECODE_LUT: [u8; 256] = build_decode_lut();
