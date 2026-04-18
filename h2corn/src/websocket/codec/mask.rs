use std::{array, mem::MaybeUninit, simd::Simd};

const WS_MASK_LANES: usize = 32;

pub(super) fn apply_websocket_mask(payload: &mut [u8], mask: [u8; 4]) {
    type MaskChunk = Simd<u8, WS_MASK_LANES>;

    let (prefix, lanes, suffix) = payload.as_simd_mut::<WS_MASK_LANES>();

    for (index, byte) in prefix.iter_mut().enumerate() {
        *byte ^= mask[index & 3];
    }

    let phase = prefix.len() & 3;
    let repeated = MaskChunk::from_array(array::from_fn(|index| mask[(phase + index) & 3]));
    for lane in lanes {
        *lane ^= repeated;
    }

    for (index, byte) in suffix.iter_mut().enumerate() {
        *byte ^= mask[(phase + index) & 3];
    }
}

pub(super) fn copy_masked_into(
    dst: &mut [MaybeUninit<u8>],
    src: &[u8],
    mask: [u8; 4],
    mut phase: usize,
) -> usize {
    debug_assert_eq!(dst.len(), src.len());

    for (slot, byte) in dst.iter_mut().zip(src) {
        slot.write(*byte ^ mask[phase]);
        phase = (phase + 1) & 3;
    }
    phase
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;

    use super::{apply_websocket_mask, copy_masked_into};

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

            apply_websocket_mask(&mut simd, mask);
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

                apply_websocket_mask(&mut simd[offset..offset + len], mask);
                apply_websocket_mask_scalar(&mut scalar[offset..offset + len], mask);

                assert_eq!(
                    simd, scalar,
                    "mismatch for payload length {len} at offset {offset}"
                );
            }
        }
    }

    #[test]
    fn copy_masked_into_matches_scalar_for_varied_lengths_and_phases() {
        let mask = [1_u8, 2, 3, 4];
        for phase in 0..4 {
            for len in [0, 1, 3, 4, 31, 32, 33, 127] {
                let src = (0..len).map(|value| value as u8).collect::<Vec<_>>();
                let mut dst = vec![MaybeUninit::uninit(); len];
                let next_phase = copy_masked_into(&mut dst, &src, mask, phase);
                let copied = dst
                    .into_iter()
                    .map(|byte| unsafe { byte.assume_init() })
                    .collect::<Vec<_>>();

                assert_eq!(copied, copy_masked_scalar(&src, mask, phase));
                assert_eq!(next_phase, (phase + len) & 3);
            }
        }
    }
}
