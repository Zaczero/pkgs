use std::hint::black_box;

use criterion::{Bencher, BenchmarkId, Criterion};
use h3o::VertexIndex;

const HEXAGONS: [u64; 6] = [
    0x24B734E649928FFF,
    0x23B734E649928FFF,
    0x22B734E649929FFF,
    0x23B734E649929FFF,
    0x24B734E649929FFF,
    0x25B734E649929FFF,
];

const PENTAGONS: [u64; 5] = [
    0x20B0800000000FFF,
    0x21B0800000000FFF,
    0x22B0800000000FFF,
    0x23B0800000000FFF,
    0x24B0800000000FFF,
];

pub fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("isValidVertex");

    for (vertex, index) in HEXAGONS.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("h3o/Hexagon", vertex), index, bench_h3o);
        group.bench_with_input(BenchmarkId::new("h3/Hexagon", vertex), index, bench_h3);
    }

    for (vertex, index) in PENTAGONS.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("h3o/Pentagon", vertex), index, bench_h3o);
        group.bench_with_input(BenchmarkId::new("h3/Pentagon", vertex), index, bench_h3);
    }

    group.finish();
}

// -----------------------------------------------------------------------------

fn bench_h3o(b: &mut Bencher<'_>, index: &u64) {
    b.iter(|| VertexIndex::try_from(black_box(*index)))
}

fn bench_h3(b: &mut Bencher<'_>, index: &u64) {
    b.iter(|| unsafe { h3ron_h3_sys::isValidVertex(black_box(*index)) })
}
