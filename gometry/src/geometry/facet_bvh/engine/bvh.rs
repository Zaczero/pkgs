use std::ops::ControlFlow;

use crate::HeapSize;
use crate::geometry::facet_bvh::{
    Facet, PreparedLinework, aabb_distance, aabb_distance_squared, aabb_distance_squared_lower,
    aabb_max_distance_squared, aabbs_overlap, point_aabb, segment_aabb, union_aabb,
};
use crate::geometry::segments::projection::segment_projection;
use crate::geometry::{
    BvhCode, BvhNode, CoordinateAxes, Point, Segment, SegmentProjection, SquaredDistanceKey,
    axis_pow2_scale, clearance_aabb_filter, clearance_strip_exceeds, point_on_segment,
    point_pair_distance_key, point_segment_distance_squared, point_segment_exact_distance_key,
    point_segment_selection, scaled_residual, segments_intersect, source_dot_cmp,
};

#[cfg(test)]
std::thread_local! {
    pub(crate) static NEAREST_WORK: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_BVH_NODES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_BVH_SEGMENTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_BVH_FACETS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_STRIP_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static CLEARANCE_STRIP_BUILD_VERTICES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// One nearest-pair candidate. Source segments retain their ordinal until
/// the final witness is emitted with full source ordinates.
pub(crate) struct NearestCandidate {
    pub(in crate::geometry) probe: Point,
    pub(in crate::geometry) target: NearestTarget,
    pub(in crate::geometry) distance_key: SquaredDistanceKey,
}

/// The exact selected projection and its directed source edge, retained until
/// the witness ordinates are resolved.
pub(crate) struct NearestSourceProjection {
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) projection: SegmentProjection,
}

pub(crate) enum NearestTarget {
    Segment(usize),
    Point(Point),
}

/// Flat binary AABB tree over a [`PreparedLinework`]'s facets,
/// longest-axis-median built. `2F − 1` nodes for `F` facets, root at 0.
///
/// Layout is traversal-driven: the AABBs live in their own dense array
/// (32 B each — the only bytes the pruned majority of node visits touch,
/// two per cache line with no straddle), the per-node code words in a
/// second; and a node's two children are ALLOCATED ADJACENTLY, so the
/// nearer-child ordering reads both child bounds from neighboring memory
/// instead of two distant tree regions.
pub(crate) struct FacetBvh {
    aabbs: Box<[[f64; 4]]>,
    codes: Box<[u32]>,
}

impl NearestCandidate {
    pub(crate) const fn segment_ordinal(&self) -> Option<usize> {
        match self.target {
            NearestTarget::Segment(ordinal) => Some(ordinal),
            NearestTarget::Point(_) => None,
        }
    }

    pub(crate) fn target_point<const SOURCE_CONTEXT: bool>(
        &self,
        linework: &PreparedLinework,
        source_axes: CoordinateAxes,
    ) -> crate::error::Result<(Point, Option<NearestSourceProjection>)> {
        match self.target {
            NearestTarget::Point(point) => Ok((point, None)),
            NearestTarget::Segment(ordinal) => {
                let (start, end) = linework.vertex_pair_at_segment(ordinal);
                let segment = Segment {
                    start: start.xy(),
                    end: end.xy(),
                };
                let projection = segment_projection(self.probe, segment)?;
                let point = projection.interpolate_point(start, end)?;
                let source = if SOURCE_CONTEXT {
                    let has_source_ordinates =
                        (source_axes.has_z() && start.z().is_some() && end.z().is_some())
                            || (source_axes.has_m() && start.m().is_some() && end.m().is_some());
                    (has_source_ordinates && !projection.is_start() && !projection.is_end())
                        .then_some(NearestSourceProjection {
                            start,
                            end,
                            projection,
                        })
                } else {
                    None
                };
                Ok((point, source))
            },
        }
    }
}

impl FacetBvh {
    fn node_aabb(&self, index: u32) -> [f64; 4] {
        *self
            .aabbs
            .get(index as usize)
            .expect("BVH traversal node index is within the built tree")
    }

    fn decode_node(&self, index: u32) -> BvhNode {
        BvhCode::new(
            *self
                .codes
                .get(index as usize)
                .expect("BVH traversal node index is within the built tree"),
        )
        .decode()
    }
}

impl HeapSize for FacetBvh {
    fn heap_bytes(&self) -> usize {
        self.aabbs
            .heap_bytes()
            .saturating_add(self.codes.heap_bytes())
    }
}

mod build;
mod clearance;
#[cfg(test)]
#[path = "bvh/tests/clearance.rs"]
mod clearance_strip_tests;
mod contact;
mod nearest;
#[cfg(test)]
#[path = "bvh/tests/nearest.rs"]
mod public_nearest_work_tests;

use build::facet_for_leaf;
#[cfg(test)]
use build::finite_midpoint;
pub(crate) use clearance::ClearanceFacetBvh;
use clearance::{ClearanceStrip, ClearanceSupport, clearance_strip_for_facets};
pub(super) use nearest::refine_facet_nearest;
pub(crate) use nearest::update_nearest_candidate;
