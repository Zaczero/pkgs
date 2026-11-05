pub(crate) const fn zigzag_encode(value: i32) -> u32 {
    ((value << 1) ^ (value >> 31)) as u32
}

pub(crate) const fn zigzag_decode(value: u32) -> i32 {
    let magnitude = (value >> 1) as i32;
    if value & 1 == 0 {
        magnitude
    } else {
        !magnitude
    }
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
