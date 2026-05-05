use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use std::simd::u8x32;

const SIMD_WIDTH: usize = 32;

pub fn header_value_is_valid(value: &[u8]) -> bool {
    if value.len() < SIMD_WIDTH {
        return header_value_is_valid_scalar(value);
    }

    let (chunks, remainder) = value.as_chunks::<SIMD_WIDTH>();
    let tab = u8x32::splat(b'\t');
    let min = u8x32::splat(32);
    let del = u8x32::splat(127);

    for chunk in chunks {
        let value = u8x32::from_array(*chunk);
        let valid = value.simd_eq(tab) | (value.simd_ge(min) & value.simd_ne(del));
        if !valid.all() {
            return false;
        }
    }

    header_value_is_valid_scalar(remainder)
}

fn header_value_is_valid_scalar(value: &[u8]) -> bool {
    value
        .iter()
        .all(|byte| *byte == b'\t' || (*byte >= 32 && *byte != 127))
}
