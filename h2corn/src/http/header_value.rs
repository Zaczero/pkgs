use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::Simd;

/// Field-value grammar from RFC 9110 5.5: HTAB, or a visible/obs-text octet.
fn lanes_are_valid<const N: usize>(value: Simd<u8, N>) -> bool {
    let tab = Simd::splat(b'\t');
    let min = Simd::splat(32);
    let del = Simd::splat(127);
    (value.simd_eq(tab) | (value.simd_ge(min) & value.simd_ne(del))).all()
}

pub(crate) fn header_value_is_valid(value: &[u8]) -> bool {
    let (wide, rest) = value.as_chunks::<32>();
    for chunk in wide {
        if !lanes_are_valid(Simd::from_array(*chunk)) {
            return false;
        }
    }
    // The x86-64-v2 baseline has no AVX2, so the 32-lane form above is already
    // two 128-bit operations and this tier costs the same per byte. It exists
    // because ordinary header values are tens of bytes: without it, everything
    // shorter than 32 bytes ran one comparison per byte with a data-dependent
    // branch, and that is the common case rather than the tail.
    let (narrow, rest) = rest.as_chunks::<16>();
    for chunk in narrow {
        if !lanes_are_valid(Simd::from_array(*chunk)) {
            return false;
        }
    }
    header_value_is_valid_scalar(rest)
}

fn header_value_is_valid_scalar(value: &[u8]) -> bool {
    value
        .iter()
        .all(|byte| *byte == b'\t' || (*byte >= 32 && *byte != 127))
}

#[cfg(test)]
mod tests {
    use super::header_value_is_valid;

    /// Every length across both vector tiers and the scalar tail must agree
    /// with the byte-at-a-time grammar, including at the 16- and 32-byte
    /// boundaries where a tier hands over.
    #[test]
    fn every_length_and_position_matches_the_scalar_grammar() {
        for len in 0..80_usize {
            assert!(
                header_value_is_valid(&vec![b'a'; len]),
                "valid value of length {len} was rejected"
            );
            for bad in [0_u8, 31, 127] {
                for position in 0..len {
                    let mut value = vec![b'a'; len];
                    value[position] = bad;
                    assert!(
                        !header_value_is_valid(&value),
                        "byte {bad} at {position} of {len} was accepted"
                    );
                }
            }
            // HTAB is the one control character the grammar admits.
            let mut tabbed = vec![b'a'; len];
            if len != 0 {
                tabbed[len - 1] = b'\t';
                assert!(header_value_is_valid(&tabbed));
            }
        }
    }
}
