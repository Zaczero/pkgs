use std::collections::BinaryHeap;

use super::chart_hausdorff::ChartHausdorffInterval;
use crate::crs::PointEdgeScratch;
#[derive(Default)]
pub(crate) struct ChartScratch {
    pub(crate) stack: Vec<(u32, f64)>,
    pub(crate) outer: BinaryHeap<ChartHausdorffInterval>,
    pub(crate) point_edge: PointEdgeScratch,
}

impl ChartScratch {
    const fn new() -> Self {
        Self {
            stack: Vec::new(),
            outer: BinaryHeap::new(),
            point_edge: PointEdgeScratch::new(),
        }
    }
}

impl crate::scratch_pool::RetainScratch for ChartScratch {
    fn clear_for_reuse(&mut self) {
        self.stack.clear();
        self.outer.clear();
        self.point_edge.clear();
    }

    fn retained_bytes(&self) -> usize {
        crate::scratch_pool::flat_vec_retained_bytes(&self.stack)
            .saturating_add(
                self.outer
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ChartHausdorffInterval>()),
            )
            .saturating_add(self.point_edge.retained_bytes())
    }
}

thread_local! {
    static CHART_SCRATCH: crate::scratch_pool::ScratchPool<ChartScratch> =
        const { crate::scratch_pool::ScratchPool::new(ChartScratch::new()) };
}

pub(crate) fn chart_scratch() -> crate::scratch_pool::ScratchLease<ChartScratch> {
    crate::scratch_pool::ScratchPool::checkout(&CHART_SCRATCH)
}
