use crate::error::Result;
use crate::geometry::distance::{CapGroup, RowProbe};
use crate::geometry::{GeodesicMetric, GeodesicSegment, Point, Shape};
#[derive(Default)]
pub(crate) struct GeodesicScratch {
    pub(crate) left_points: Vec<Point>,
    pub(crate) right_points: Vec<Point>,
    pub(crate) left_edges: Vec<GeodesicSegment>,
    pub(crate) right_edges: Vec<GeodesicSegment>,
    pub(crate) left_point_only: Vec<Point>,
    pub(crate) right_point_only: Vec<Point>,
    pub(crate) cap_groups: Vec<CapGroup>,
    pub(crate) rows: Vec<RowProbe>,
    pub(crate) stack: Vec<u32>,
}

impl GeodesicScratch {
    const fn new() -> Self {
        Self {
            left_points: Vec::new(),
            right_points: Vec::new(),
            left_edges: Vec::new(),
            right_edges: Vec::new(),
            left_point_only: Vec::new(),
            right_point_only: Vec::new(),
            cap_groups: Vec::new(),
            rows: Vec::new(),
            stack: Vec::new(),
        }
    }
}

impl crate::scratch_pool::RetainScratch for GeodesicScratch {
    fn clear_for_reuse(&mut self) {
        self.left_points.clear();
        self.right_points.clear();
        self.left_edges.clear();
        self.right_edges.clear();
        self.left_point_only.clear();
        self.right_point_only.clear();
        self.cap_groups.clear();
        self.rows.clear();
        self.stack.clear();
    }

    fn retained_bytes(&self) -> usize {
        crate::scratch_pool::flat_vec_retained_bytes(&self.left_points)
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(
                &self.right_points,
            ))
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(
                &self.left_edges,
            ))
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(
                &self.right_edges,
            ))
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(
                &self.left_point_only,
            ))
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(
                &self.right_point_only,
            ))
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(
                &self.cap_groups,
            ))
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(&self.rows))
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(&self.stack))
    }
}

thread_local! {
    static GEODESIC_SCRATCH: crate::scratch_pool::ScratchPool<GeodesicScratch> =
        const { crate::scratch_pool::ScratchPool::new(GeodesicScratch::new()) };
}

pub(crate) fn geodesic_scratch() -> crate::scratch_pool::ScratchLease<GeodesicScratch> {
    crate::scratch_pool::ScratchPool::checkout(&GEODESIC_SCRATCH)
}

pub(crate) fn collect_geodesic_segments_into(
    shape: &Shape,
    metric: &impl GeodesicMetric,
    out: &mut Vec<GeodesicSegment>,
) -> Result<()> {
    out.clear();
    crate::boundary::try_reserve_vec(out, shape.segment_count())?;
    shape.try_for_each_vertex_pair(|start, end| {
        crate::try_push_vec(out, metric.make_segment(start, end)?)
    })
}

pub(crate) fn collect_point_only_into(
    shape: &Shape,
    out: &mut Vec<Point>,
) -> crate::error::Result<()> {
    out.clear();
    shape.append_point_only(out)
}
