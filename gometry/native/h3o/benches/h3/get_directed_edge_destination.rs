use std::hint::black_box;

use criterion::{Bencher, BenchmarkId, Criterion};
use h3o::DirectedEdgeIndex;

pub fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("getDirectedEdgeDestination");

    for (resolution, (hexagon, pentagon)) in [
        (0x150F_3FFF_FFFF_FFFF, 0x1401_FFFF_FFFF_FFFF),
        (0x111F_27FF_FFFF_FFFF, 0x161D_B3FF_FFFF_FFFF),
        (0x142F_367F_FFFF_FFFF, 0x162D_B37F_FFFF_FFFF),
        (0x143F_365F_FFFF_FFFF, 0x1531_F65F_FFFF_FFFF),
        (0x114E_C69B_FFFF_FFFF, 0x1441_F265_FFFF_FFFF),
    ]
    .iter()
    .enumerate()
    {
        group.bench_with_input(
            BenchmarkId::new("h3o/Hexagon", resolution),
            hexagon,
            bench_h3o,
        );
        group.bench_with_input(
            BenchmarkId::new("h3/Hexagon", resolution),
            hexagon,
            bench_h3,
        );

        group.bench_with_input(
            BenchmarkId::new("h3o/Pentagon", resolution),
            pentagon,
            bench_h3o,
        );
        group.bench_with_input(
            BenchmarkId::new("h3/Pentagon", resolution),
            pentagon,
            bench_h3,
        );
    }

    group.finish();
}

// -----------------------------------------------------------------------------

fn bench_h3o(b: &mut Bencher<'_>, index: &u64) {
    let index = DirectedEdgeIndex::try_from(*index).expect("edge index");
    b.iter(|| black_box(index).destination())
}

fn bench_h3(b: &mut Bencher<'_>, index: &u64) {
    let mut out: u64 = 0;
    b.iter(|| unsafe { h3ron_h3_sys::getDirectedEdgeDestination(black_box(*index), &mut out) })
}
