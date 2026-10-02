#[cfg(test)]
use super::geodesic_hausdorff_counters::bvh_target_query;
use crate::error::Result;
use crate::geometry::facet_bvh::BVH_MIN_INDEXED_SEGMENTS;
use crate::geometry::{
    GeodesicFacetBvh, GeodesicMetric, GeodesicParts, GeodesicSegmentQuery,
    GeodesicSegmentQueryResult, GeodesicSegmentScratch, GeodesicTargetFeature,
    GeodesicWitnessCandidate, GeodesicWitnessNeed, Point, WitnessOrder, geodesic_witness_is_better,
    witness_refine_limit,
};

pub(crate) fn geodesic_target_witness(
    point: Point,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    stack: &mut Vec<u32>,
    segment_scratch: &mut GeodesicSegmentScratch,
) -> Result<Option<GeodesicWitnessCandidate>> {
    if let Some(bvh) = target_parts.geodesic_bvh(metric) {
        #[cfg(test)]
        bvh_target_query();
        return bvh.witness_point(point, target_parts, metric, stack, segment_scratch);
    }

    let mut best = None;
    for (index, &segment) in target_parts.segments.iter().enumerate() {
        let order = WitnessOrder::ordinal(index);
        let cutoff = witness_refine_limit(best.as_ref(), order);
        let result = metric.point_segment_query(
            point,
            &segment,
            GeodesicSegmentQuery::Minimum {
                cutoff,
                witness: GeodesicWitnessNeed::Yes,
            },
            segment_scratch,
        )?;
        let GeodesicSegmentQueryResult::Attained {
            minimum,
            foot: Some(target),
        } = result
        else {
            continue;
        };
        let candidate = GeodesicWitnessCandidate {
            target,
            distance: minimum.distance,
            lower: minimum.lower,
            evaluation_error: minimum.evaluation_error,
            feature: GeodesicTargetFeature::Segment {
                index,
                along: minimum.along,
            },
            order,
        };
        if geodesic_witness_is_better(best.as_ref(), &candidate) {
            best = Some(candidate);
        }
    }
    for (index, &target) in target_parts.point_only.iter().enumerate() {
        let order = WitnessOrder::from_rank(WitnessOrder::point_feature(
            target_parts.segments.len(),
            index,
        ));
        let cutoff = witness_refine_limit(best.as_ref(), order);
        if metric.point_distance_lower_bound(point, target)? > cutoff {
            continue;
        }
        let distance = metric.segment_length(point, target);
        let evaluation_error = metric.geodesic_numerical_policy().evaluation_allowance(1.0);
        let candidate = GeodesicWitnessCandidate {
            target,
            distance,
            lower: (distance - evaluation_error).max(0.0),
            evaluation_error,
            feature: GeodesicTargetFeature::Point,
            order,
        };
        if geodesic_witness_is_better(best.as_ref(), &candidate) {
            best = Some(candidate);
        }
    }
    Ok(best)
}

impl GeodesicParts {
    pub(crate) fn geodesic_bvh(&self, metric: &impl GeodesicMetric) -> Option<&GeodesicFacetBvh> {
        self.facet_bvh
            .get_or_init(|| {
                if self.segments.len() >= BVH_MIN_INDEXED_SEGMENTS {
                    GeodesicFacetBvh::build(&self.segments, metric)
                } else {
                    None
                }
            })
            .as_ref()
    }
}
