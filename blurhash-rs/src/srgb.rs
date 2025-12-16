use core::hint::unlikely;

include!(concat!(env!("OUT_DIR"), "/srgb_tables.rs"));

pub fn srgb_u8_to_linear(v: u8) -> f32 {
    SRGB_U8_TO_LINEAR[v as usize]
}

pub fn linear_to_srgb_u8(v: f32) -> u8 {
    if unlikely(v <= 0.0) {
        return 0;
    }
    if unlikely(v >= 1.0) {
        return 255;
    }

    // Approximation mode: map linear -> u8 via a dense LUT.
    //
    // This is much faster than calling powf() per channel per pixel and is
    // visually indistinguishable for BlurHash previews, but not byte-identical
    // to reference implementations.
    let len = LINEAR_TO_SRGB_U8.len();
    let idx = ((v * (len as f32 - 1.0)) + 0.5) as usize;
    LINEAR_TO_SRGB_U8[idx.min(len - 1)]
}
