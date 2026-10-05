use std::hint::black_box;

use criterion::{Bencher, BenchmarkId, Criterion};
use h3o::DirectedEdgeIndex;

const HEXAGONS: [u64; 16] = [
    0x12073FFFFFFFFFFF,
    0x121737FFFFFFFFFF,
    0x122734FFFFFFFFFF,
    0x123734EFFFFFFFFF,
    0x124734A9FFFFFFFF,
    0x125734E67FFFFFFF,
    0x126734E64FFFFFFF,
    0x127734E64DFFFFFF,
    0x128734E6499FFFFF,
    0x129734E64993FFFF,
    0x12A734E64992FFFF,
    0x12B734E649929FFF,
    0x12C734E649929DFF,
    0x12D734E64992D6FF,
    0x12E734E64992D6DF,
    0x12F734E64992D6D8,
];

const PENTAGONS: [u64; 16] = [
    0x12009FFFFFFFFFFF,
    0x121083FFFFFFFFFF,
    0x1220807FFFFFFFFF,
    0x1230800FFFFFFFFF,
    0x12408001FFFFFFFF,
    0x125080003FFFFFFF,
    0x1260800007FFFFFF,
    0x1270800000FFFFFF,
    0x12808000001FFFFF,
    0x129080000003FFFF,
    0x12A0800000007FFF,
    0x12B0800000000FFF,
    0x12C08000000001FF,
    0x12D080000000003F,
    0x12E0800000000007,
    0x12F0800000000000,
];

pub fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("isValidDirectedEdge");

    for (resolution, index) in HEXAGONS.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::new("h3o/Hexagon", resolution),
            index,
            bench_h3o,
        );
        group.bench_with_input(BenchmarkId::new("h3/Hexagon", resolution), index, bench_h3);
    }

    for (resolution, index) in PENTAGONS.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::new("h3o/Pentagon", resolution),
            index,
            bench_h3o,
        );
        group.bench_with_input(BenchmarkId::new("h3/Pentagon", resolution), index, bench_h3);
    }

    group.finish();
}

// -----------------------------------------------------------------------------

fn bench_h3o(b: &mut Bencher<'_>, index: &u64) {
    b.iter(|| DirectedEdgeIndex::try_from(black_box(*index)))
}

fn bench_h3(b: &mut Bencher<'_>, index: &u64) {
    b.iter(|| unsafe { h3ron_h3_sys::isValidDirectedEdge(black_box(*index)) })
}
