use std::simd::{Simd, ToBytes};

const WS_MASK_LANES: usize = 32;

#[derive(Clone, Copy)]
pub(super) struct MaskKey {
    words: [u32; 4],
}

impl MaskKey {
    pub(super) const fn new(mask: [u8; 4]) -> Self {
        Self {
            words: [
                u32::from_ne_bytes([mask[0], mask[1], mask[2], mask[3]]),
                u32::from_ne_bytes([mask[1], mask[2], mask[3], mask[0]]),
                u32::from_ne_bytes([mask[2], mask[3], mask[0], mask[1]]),
                u32::from_ne_bytes([mask[3], mask[0], mask[1], mask[2]]),
            ],
        }
    }

    const fn word(self, phase: usize) -> u32 {
        self.words[phase & 3]
    }
}

fn apply_u32_mask_phase(payload: &mut [u8], key: MaskKey, phase: usize) -> usize {
    let payload_len = payload.len();
    let mask_word = key.word(phase);
    let (words, suffix) = payload.as_chunks_mut::<4>();
    for word in words {
        *word = (u32::from_ne_bytes(*word) ^ mask_word).to_ne_bytes();
    }

    let suffix_phase = (phase + payload_len - suffix.len()) & 3;
    let suffix_mask = key.word(suffix_phase).to_ne_bytes();
    for (index, byte) in suffix.iter_mut().enumerate() {
        *byte ^= suffix_mask[index];
    }
    (phase + payload_len) & 3
}

pub(super) fn apply_websocket_mask_phase(payload: &mut [u8], key: MaskKey, phase: usize) -> usize {
    let payload_len = payload.len();
    if payload_len < 4 {
        let mask = key.word(phase).to_ne_bytes();
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index];
        }
        return (phase + payload_len) & 3;
    }

    let (prefix, lanes, suffix) = payload.as_simd_mut::<WS_MASK_LANES>();
    let lane_phase = apply_u32_mask_phase(prefix, key, phase);
    let repeated = Simd::<u32, { WS_MASK_LANES / 4 }>::splat(key.word(lane_phase)).to_ne_bytes();
    let lanes_len = lanes.len();
    for lane in lanes {
        *lane ^= repeated;
    }

    let suffix_phase = (lane_phase + lanes_len * WS_MASK_LANES) & 3;
    apply_u32_mask_phase(suffix, key, suffix_phase);
    (phase + payload_len) & 3
}

#[cfg(test)]
mod tests {
    use super::{MaskKey, apply_websocket_mask_phase};

    fn apply_websocket_mask_scalar(payload: &mut [u8], mask: [u8; 4], phase: usize) -> usize {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[(phase + index) & 3];
        }
        (phase + payload.len()) & 3
    }

    fn copy_masked_scalar(payload: &[u8], mask: [u8; 4], phase: usize) -> Vec<u8> {
        payload
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ mask[(phase + index) & 3])
            .collect()
    }

    #[test]
    fn websocket_mask_is_exhaustively_equivalent_across_phases_and_alignment() {
        const MASKS: [[u8; 4]; 4] = [[0, 0, 0, 0], [u8::MAX; 4], [1, 2, 3, 4], [
            0x37, 0xFA, 0x21, 0x3D,
        ]];
        const LARGE_LENGTHS: [usize; 12] = [
            1_023, 1_024, 1_500, 2_047, 2_048, 4_095, 4_096, 8_191, 8_192, 16_383, 0x4000, 0x4001,
        ];
        let mut candidate = vec![0; 0x4001 + 64];
        let mut reference = candidate.clone();

        for mask in MASKS {
            for phase in 0..4 {
                for offset in 0..32 {
                    for len in (0..=512).chain(LARGE_LENGTHS) {
                        let total = offset + len + 32;
                        for (index, byte) in candidate[..total].iter_mut().enumerate() {
                            *byte = index.wrapping_mul(29) as u8;
                        }
                        reference[..total].copy_from_slice(&candidate[..total]);

                        let candidate_phase = apply_websocket_mask_phase(
                            &mut candidate[offset..offset + len],
                            MaskKey::new(mask),
                            phase,
                        );
                        let reference_phase = apply_websocket_mask_scalar(
                            &mut reference[offset..offset + len],
                            mask,
                            phase,
                        );

                        assert_eq!(
                            candidate[..total],
                            reference[..total],
                            "len={len}, offset={offset}, phase={phase}"
                        );
                        assert_eq!(candidate_phase, reference_phase);
                    }
                }
            }
        }
    }

    #[test]
    fn websocket_mask_phase_is_preserved_across_every_segment_boundary() {
        let mask = [0x37, 0xFA, 0x21, 0x3D];
        for len in 0_usize..=256 {
            let source = (0..len)
                .map(|value| value.wrapping_mul(31) as u8)
                .collect::<Vec<_>>();
            let expected = copy_masked_scalar(&source, mask, 0);
            for split in 0..=len {
                let mut candidate = source.clone();
                let key = MaskKey::new(mask);
                let phase = apply_websocket_mask_phase(&mut candidate[..split], key, 0);
                let final_phase = apply_websocket_mask_phase(&mut candidate[split..], key, phase);
                assert_eq!(candidate, expected, "len={len}, split={split}");
                assert_eq!(final_phase, len & 3);
            }
        }
    }
}
