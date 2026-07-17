#![feature(portable_simd)]

use std::hint::black_box;
use std::simd::{Simd, ToBytes};
use std::time::Instant;
use std::{array, env};

#[path = "../src/websocket/codec/mask.rs"]
mod production;

type Kernel = fn(&mut [u8], [u8; 4], usize, usize) -> usize;

const LENGTHS: [usize; 12] = [1, 2, 8, 16, 32, 64, 125, 1_500, 2_048, 4_096, 8_192, 16_384];
const MASK: [u8; 4] = [0x37, 0xFA, 0x21, 0x3D];
const MIN_CALLS: usize = 300_000;
const MAX_CALLS: usize = 20_000_000;
const TARGET_BYTES: usize = 640_000_000;
const TARGET_CHUNK_APPLICATIONS: usize = 40_000_000;

/// The pre-change aligned 32-lane implementation retained only as benchmark
/// control. Production code is imported from `src/websocket/codec/mask.rs`.
fn apply_legacy_mask_phase(payload: &mut [u8], mask: [u8; 4], phase: usize) -> usize {
    let (prefix, lanes, suffix) = payload.as_simd_mut::<32>();
    for (index, byte) in prefix.iter_mut().enumerate() {
        *byte ^= mask[(phase + index) & 3];
    }
    let lane_phase = (phase + prefix.len()) & 3;
    let repeated =
        Simd::<u8, 32>::from_array(array::from_fn(|index| mask[(lane_phase + index) & 3]));
    let suffix_phase = (lane_phase + lanes.len() * 32) & 3;
    for lane in lanes {
        *lane ^= repeated;
    }
    for (index, byte) in suffix.iter_mut().enumerate() {
        *byte ^= mask[(suffix_phase + index) & 3];
    }
    (phase + payload.len()) & 3
}

fn apply_in_chunks<K: Copy>(
    payload: &mut [u8],
    key: K,
    phase: usize,
    chunk_size: usize,
    mut apply: impl FnMut(&mut [u8], K, usize) -> usize,
) -> usize {
    if chunk_size == 0 || chunk_size >= payload.len() {
        return apply(payload, key, phase);
    }

    let mut phase = phase;
    for chunk in payload.chunks_mut(chunk_size) {
        phase = apply(chunk, key, phase);
    }
    phase
}

fn legacy(payload: &mut [u8], mask: [u8; 4], phase: usize, chunk_size: usize) -> usize {
    apply_in_chunks(payload, mask, phase, chunk_size, apply_legacy_mask_phase)
}

/// The eager four-word key used by production before the rotate-on-demand
/// representation. Retained as the direct construction and reuse control.
#[derive(Clone, Copy)]
struct EagerMaskKey {
    words: [u32; 4],
}

impl EagerMaskKey {
    const fn new(mask: [u8; 4]) -> Self {
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

fn apply_eager_u32_mask_phase(payload: &mut [u8], key: EagerMaskKey, phase: usize) -> usize {
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

fn apply_eager_mask_phase(payload: &mut [u8], key: EagerMaskKey, phase: usize) -> usize {
    let payload_len = payload.len();
    if payload_len < 4 {
        let mask = key.word(phase).to_ne_bytes();
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index];
        }
        return (phase + payload_len) & 3;
    }

    let (prefix, lanes, suffix) = payload.as_simd_mut::<32>();
    let lane_phase = apply_eager_u32_mask_phase(prefix, key, phase);
    let repeated = Simd::<u32, 8>::splat(key.word(lane_phase)).to_ne_bytes();
    let lanes_len = lanes.len();
    for lane in lanes {
        *lane ^= repeated;
    }

    let suffix_phase = (lane_phase + lanes_len * 32) & 3;
    apply_eager_u32_mask_phase(suffix, key, suffix_phase);
    (phase + payload_len) & 3
}

fn eager_key(payload: &mut [u8], mask: [u8; 4], phase: usize, chunk_size: usize) -> usize {
    apply_in_chunks(
        payload,
        EagerMaskKey::new(mask),
        phase,
        chunk_size,
        apply_eager_mask_phase,
    )
}

fn production(payload: &mut [u8], mask: [u8; 4], phase: usize, chunk_size: usize) -> usize {
    apply_in_chunks(
        payload,
        production::MaskKey::new(mask),
        phase,
        chunk_size,
        production::apply_websocket_mask_phase,
    )
}

fn kernel(name: &str) -> Kernel {
    match name {
        "production" => production,
        "eager-key" => eager_key,
        "legacy" => legacy,
        _ => panic!("unknown kernel: {name}"),
    }
}

fn state(index: usize, workload: &str) -> (usize, usize) {
    match workload {
        // Cycle the exact Cartesian product before repeating. Coupling phase to
        // offset would silently exercise only one quarter of these states.
        "cartesian" => (index & 31, (index >> 5) & 3),
        // A complete frame is unmasked in one call beginning at mask phase 0.
        "phase0" => (index & 31, 0),
        _ => panic!("unknown state workload: {workload}"),
    }
}

fn apply_scalar_mask(payload: &mut [u8], mask: [u8; 4], phase: usize) -> usize {
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte ^= mask[(phase + index) & 3];
    }
    (phase + payload.len()) & 3
}

fn validate_kernel(kernel: Kernel, chunk_size: usize) {
    for len in (0..=256).chain(LENGTHS) {
        for offset in 0..32 {
            for phase in 0..4 {
                let total = offset + len + 32;
                let mut candidate = (0..total)
                    .map(|index| index.wrapping_mul(29).wrapping_add(17) as u8)
                    .collect::<Vec<_>>();
                let mut reference = candidate.clone();
                let candidate_phase = kernel(
                    &mut candidate[offset..offset + len],
                    MASK,
                    phase,
                    chunk_size,
                );
                let reference_phase =
                    apply_scalar_mask(&mut reference[offset..offset + len], MASK, phase);
                assert_eq!(
                    candidate, reference,
                    "len={len}, offset={offset}, phase={phase}"
                );
                assert_eq!(
                    candidate_phase, reference_phase,
                    "len={len}, offset={offset}, phase={phase}"
                );
            }
        }
    }
}

fn calls_for(len: usize, chunk_size: usize) -> usize {
    let byte_target = (TARGET_BYTES / len.max(1)).clamp(MIN_CALLS, MAX_CALLS);
    if chunk_size == 0 {
        return byte_target;
    }
    let chunks = len.div_ceil(chunk_size).max(1);
    byte_target.min((TARGET_CHUNK_APPLICATIONS / chunks).max(1))
}

fn main() {
    let name = env::args()
        .nth(1)
        .expect("usage: mask_kernel KERNEL [SCALE] [STATE_WORKLOAD] [CHUNK_SIZE]");
    let scale = env::args()
        .nth(2)
        .map_or(1.0, |value| value.parse::<f64>().expect("invalid scale"));
    let workload = env::args().nth(3).unwrap_or_else(|| "cartesian".into());
    let chunk_size = env::args().nth(4).map_or(0, |value| {
        value.parse::<usize>().expect("invalid chunk size")
    });
    let kernel = kernel(&name);
    validate_kernel(kernel, chunk_size);
    let kernel = black_box(kernel);

    print!(
        "{{\"kernel\":\"{name}\",\"workload\":\"{workload}\",\"chunk_size\":{chunk_size},\"cells\":{{"
    );
    for (cell_index, len) in LENGTHS.into_iter().enumerate() {
        // Every cell runs for roughly 100 ms or longer on the reference host;
        // shorter cells overstate process-frequency and code-placement noise.
        // Segmented cells additionally cap total kernel applications so tiny
        // chunks do not turn a bounded comparison into a multi-hour run.
        let calls = (calls_for(len, chunk_size) as f64 * scale).round().max(1.0) as usize;
        let mut storage = vec![0xA5; len + 64];
        let mut checksum = 0usize;

        for index in 0..calls.min(20_000) {
            let (offset, phase) = state(index, &workload);
            checksum ^= kernel(
                black_box(&mut storage[offset..offset + len]),
                MASK,
                phase,
                chunk_size,
            );
        }

        let started = Instant::now();
        for index in 0..calls {
            let (offset, phase) = state(index, &workload);
            checksum ^= kernel(
                black_box(&mut storage[offset..offset + len]),
                MASK,
                phase,
                chunk_size,
            );
        }
        let elapsed = started.elapsed().as_secs_f64();
        black_box(checksum);
        if cell_index != 0 {
            print!(",");
        }
        print!("\"{len}\":{:.6}", elapsed * 1e9 / calls as f64);
    }
    println!("}}}}");
}
