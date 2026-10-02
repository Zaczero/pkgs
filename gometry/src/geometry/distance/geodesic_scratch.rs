use std::collections::BinaryHeap;

use super::geodesic_hausdorff_helpers::HausdorffInterval;
use crate::crs::PointEdgeScratch;
use crate::geometry::{GeodesicSegmentScratch, Point, RunIdentity, Shape};
#[derive(Default)]
pub(crate) struct GeodesicScratch {
    pub(crate) stack: Vec<u32>,
    pub(crate) chart_stack: Vec<(u32, f64)>,
    pub(crate) outer: BinaryHeap<HausdorffInterval>,
    pub(crate) segment: GeodesicSegmentScratch,
    pub(crate) point_edge: PointEdgeScratch,
}

impl GeodesicScratch {
    const fn new() -> Self {
        Self {
            stack: Vec::new(),
            chart_stack: Vec::new(),
            outer: BinaryHeap::new(),
            segment: GeodesicSegmentScratch::new(),
            point_edge: PointEdgeScratch::new(),
        }
    }
}

impl crate::scratch_pool::RetainScratch for GeodesicScratch {
    fn clear_for_reuse(&mut self) {
        self.stack.clear();
        self.chart_stack.clear();
        self.outer.clear();
        self.segment.clear();
        self.point_edge.clear();
    }

    fn retained_bytes(&self) -> usize {
        crate::scratch_pool::flat_vec_retained_bytes(&self.stack)
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(
                &self.chart_stack,
            ))
            .saturating_add(
                self.outer
                    .capacity()
                    .saturating_mul(std::mem::size_of::<HausdorffInterval>()),
            )
            .saturating_add(self.segment.retained_bytes())
            .saturating_add(self.point_edge.retained_bytes())
    }
}

thread_local! {
    static GEODESIC_SCRATCH: crate::scratch_pool::ScratchPool<GeodesicScratch> =
        const { crate::scratch_pool::ScratchPool::new(GeodesicScratch::new()) };
}

pub(crate) fn geodesic_scratch() -> crate::scratch_pool::ScratchLease<GeodesicScratch> {
    crate::scratch_pool::ScratchPool::checkout(&GEODESIC_SCRATCH)
}

pub(crate) fn collect_point_only_into(shape: &Shape, out: &mut Vec<Point>) {
    out.clear();
    shape.append_first_point_only(RunIdentity::Full, out);
}
