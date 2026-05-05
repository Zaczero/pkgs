use std::array;
use std::simd::Simd;

const WS_MASK_LANES: usize = 32;

pub(super) fn apply_websocket_mask_phase(payload: &mut [u8], mask: [u8; 4], phase: usize) -> usize {
    type MaskChunk = Simd<u8, WS_MASK_LANES>;

    let (prefix, lanes, suffix) = payload.as_simd_mut::<WS_MASK_LANES>();

    for (index, byte) in prefix.iter_mut().enumerate() {
        *byte ^= mask[(phase + index) & 3];
    }

    let lane_phase = (phase + prefix.len()) & 3;
    let repeated = MaskChunk::from_array(array::from_fn(|index| mask[(lane_phase + index) & 3]));
    let suffix_phase = (lane_phase + lanes.len() * WS_MASK_LANES) & 3;
    for lane in lanes {
        *lane ^= repeated;
    }

    for (index, byte) in suffix.iter_mut().enumerate() {
        *byte ^= mask[(suffix_phase + index) & 3];
    }
    (phase + payload.len()) & 3
}

#[cfg(test)]
mod tests {
    use super::apply_websocket_mask_phase;

    fn apply_websocket_mask_scalar(payload: &mut [u8], mask: [u8; 4]) {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index & 3];
        }
    }

    fn copy_masked_scalar(payload: &[u8], mask: [u8; 4], phase: usize) -> Vec<u8> {
        payload
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ mask[(phase + index) & 3])
            .collect()
    }

    #[test]
    fn websocket_mask_matches_scalar_for_varied_lengths() {
        let mask = [1_u8, 2, 3, 4];
        for len in [0, 1, 3, 4, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1024] {
            let base = (0..len).map(|value| value as u8).collect::<Vec<_>>();
            let mut simd = base.clone();
            let mut scalar = base;

            apply_websocket_mask_phase(&mut simd, mask, 0);
            apply_websocket_mask_scalar(&mut scalar, mask);

            assert_eq!(simd, scalar, "mismatch for payload length {len}");
        }
    }

    #[test]
    fn websocket_mask_matches_scalar_for_misaligned_slices() {
        let mask = [1_u8, 2, 3, 4];
        for offset in [0, 1, 2, 3, 5, 7, 16, 31] {
            for len in [0, 1, 3, 4, 31, 32, 33, 63, 64, 65] {
                let total = offset + len + 8;
                let base = (0..total).map(|value| value as u8).collect::<Vec<_>>();
                let mut simd = base.clone();
                let mut scalar = base;

                apply_websocket_mask_phase(&mut simd[offset..offset + len], mask, 0);
                apply_websocket_mask_scalar(&mut scalar[offset..offset + len], mask);

                assert_eq!(
                    simd, scalar,
                    "mismatch for payload length {len} at offset {offset}"
                );
            }
        }
    }

    #[test]
    fn phased_websocket_mask_matches_scalar_for_varied_lengths_and_phases() {
        let mask = [1_u8, 2, 3, 4];
        for phase in 0..4 {
            for len in [0, 1, 3, 4, 31, 32, 33, 127] {
                let src = (0..len).map(|value| value as u8).collect::<Vec<_>>();
                let mut copied = src.clone();
                let next_phase = apply_websocket_mask_phase(&mut copied, mask, phase);

                assert_eq!(copied, copy_masked_scalar(&src, mask, phase));
                assert_eq!(next_phase, (phase + len) & 3);
            }
        }
    }
}
