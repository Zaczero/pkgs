pub(crate) const fn zigzag_encode(value: i32) -> u32 {
    (value.wrapping_shl(1) ^ value.wrapping_shr(31)) as u32
}

pub(crate) const fn zigzag_decode(value: u32) -> i32 {
    (value.wrapping_shr(1) as i32) ^ ((value & 1) as i32).wrapping_neg()
}

#[cfg(kani)]
mod proofs {
    use super::{zigzag_decode, zigzag_encode};

    #[kani::proof]
    fn signed_deltas_follow_the_zigzag_wire_mapping() {
        let delta: i32 = kani::any();
        let wide = i64::from(delta);
        let wire = if wide < 0 { -2 * wide - 1 } else { 2 * wide } as u32;
        assert_eq!(zigzag_encode(delta), wire);
        assert_eq!(zigzag_decode(wire), delta);
    }
}
