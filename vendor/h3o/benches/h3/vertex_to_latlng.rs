use std::hint::black_box;

use criterion::{Bencher, BenchmarkId, Criterion};
use h3o::{LatLng, VertexIndex};

const HEXAGONS: [u64; 16] = [
    0x2204BFFFFFFFFFFF,
    0x251723FFFFFFFFFF,
    0x2027267FFFFFFFFF,
    0x2137348FFFFFFFFF,
    0x22473485FFFFFFFF,
    0x215734E2BFFFFFFF,
    0x246734E647FFFFFF,
    0x257734E648FFFFFF,
    0x208734E6491FFFFF,
    0x259734E64983FFFF,
    0x24A734E64992FFFF,
    0x25B734E649929FFF,
    0x24C734E649928BFF,
    0x25D734E649929D3F,
    0x24E734E649929D2F,
    0x23F734E64992D6D8,
];

const PENTAGONS: [u64; 16] = [
    0x25007FFFFFFFFFFF,
    0x211083FFFFFFFFFF,
    0x2220807FFFFFFFFF,
    0x2330800FFFFFFFFF,
    0x24408001FFFFFFFF,
    0x205080003FFFFFFF,
    0x2160800007FFFFFF,
    0x2270800000FFFFFF,
    0x23808000001FFFFF,
    0x249080000003FFFF,
    0x20A0800000007FFF,
    0x21B0800000000FFF,
    0x22C08000000001FF,
    0x23D080000000003F,
    0x24E0800000000007,
    0x20F0800000000000,
];

pub fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("vertexToLatLng");

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
    let index = VertexIndex::try_from(*index).expect("cell index");
    b.iter(|| LatLng::from(black_box(index)))
}

fn bench_h3(b: &mut Bencher<'_>, index: &u64) {
    let mut result = h3ron_h3_sys::LatLng { lat: 0., lng: 0. };
    b.iter(|| unsafe {
        h3ron_h3_sys::vertexToLatLng(black_box(*index), &mut result);
    })
}
