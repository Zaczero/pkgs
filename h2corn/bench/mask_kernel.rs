#![feature(portable_simd)]

use std::hint::black_box;
use std::simd::Simd;
use std::time::Instant;
use std::{array, env};

#[path = "../src/websocket/codec/mask.rs"]
mod production;

type Kernel = fn(&mut [u8], [u8; 4], usize) -> usize;

const LENGTHS: [usize; 12] = [1, 2, 8, 16, 32, 64, 125, 1_500, 2_048, 4_096, 8_192, 16_384];
const MASK: [u8; 4] = [0x37, 0xFA, 0x21, 0x3D];

/// The pre-change aligned 32-lane implementation retained only as benchmark
/// control. Production code is imported from `src/websocket/codec/mask.rs`.
fn legacy(payload: &mut [u8], mask: [u8; 4], phase: usize) -> usize {
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

fn production(payload: &mut [u8], mask: [u8; 4], phase: usize) -> usize {
    production::apply_websocket_mask_phase(payload, production::MaskKey::new(mask), phase)
}

fn kernel(name: &str) -> Kernel {
    match name {
        "production" => production,
        "legacy" => legacy,
        _ => panic!("unknown kernel: {name}"),
    }
}

fn main() {
    let name = env::args()
        .nth(1)
        .expect("usage: mask_kernel KERNEL [SCALE]");
    let scale = env::args()
        .nth(2)
        .map_or(1.0, |value| value.parse::<f64>().expect("invalid scale"));
    let kernel = black_box(kernel(&name));

    print!("{{\"kernel\":\"{name}\",\"cells\":{{");
    for (cell_index, len) in LENGTHS.into_iter().enumerate() {
        let calls = ((64_000_000usize / len.max(1)).clamp(30_000, 2_000_000) as f64 * scale)
            .round()
            .max(1.0) as usize;
        let mut storage = vec![0xA5; len + 64];
        let mut checksum = 0usize;

        for index in 0..calls.min(20_000) {
            let offset = (index.wrapping_mul(7)) & 31;
            checksum ^= kernel(
                black_box(&mut storage[offset..offset + len]),
                MASK,
                index & 3,
            );
        }

        let started = Instant::now();
        for index in 0..calls {
            let offset = (index.wrapping_mul(7)) & 31;
            checksum ^= kernel(
                black_box(&mut storage[offset..offset + len]),
                MASK,
                index & 3,
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
