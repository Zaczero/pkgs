//! Generated sRGB transfer tables and bounded channel lookup.

include!(concat!(env!("OUT_DIR"), "/srgb_tables.rs"));

pub(crate) const fn srgb_u8_to_linear(v: u8) -> f32 {
    SRGB_U8_TO_LINEAR
        .get(v as usize)
        .copied()
        .expect("every byte has an sRGB lookup entry")
}

pub(crate) fn linear_to_srgb_u8(v: f32) -> u8 {
    const LAST_INDEX: usize = LINEAR_TO_SRGB_U8
        .len()
        .checked_sub(1)
        .expect("nonempty sRGB table");

    if v <= 0.0 {
        return 0;
    }
    if v >= 1.0 {
        return 255;
    }

    // Approximation mode: map linear -> u8 via a dense LUT.
    //
    // This is much faster than calling powf() per channel per pixel and is
    // visually indistinguishable for BlurHash previews, but not byte-identical
    // to reference implementations.
    let idx = (v * LAST_INDEX as f32 + 0.5) as usize;
    LINEAR_TO_SRGB_U8
        .get(idx.min(LAST_INDEX))
        .copied()
        .expect("clamped index fits the nonempty sRGB table")
}
