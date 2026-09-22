pub(crate) const fn zigzag_encode(value: i32) -> u32 {
    (value.wrapping_shl(1) ^ value.wrapping_shr(31)) as u32
}

pub(crate) const fn zigzag_decode(value: u32) -> i32 {
    (value.wrapping_shr(1) as i32) ^ ((value & 1) as i32).wrapping_neg()
}
