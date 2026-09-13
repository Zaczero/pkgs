use crate::error::Result;
#[cfg(test)]
use crate::geometry::distance::geodesic_hausdorff_counters;
use crate::geometry::distance::geodesic_scratch;
use crate::geometry::facet_bvh::BVH_MIN_INDEXED_SEGMENTS;
use crate::geometry::{
    GeodesicFacetBvh, GeodesicMetric, GeodesicParts, GeodesicSegment, GeodesicSegmentQuery,
    GeodesicSegmentQueryResult, GeodesicSegmentScratch, GeodesicTargetFeature,
    GeodesicWitnessCandidate, GeodesicWitnessNeed, Point, geodesic_witness_is_better,
    witness_refine_limit,
};
pub(crate) const GEODESIC_CAP_GROUP: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RowProbe {
    pub(crate) bound: f64,
    pub(crate) vertex: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CapGroup {
    pub(crate) anchor: Point,
    pub(crate) reach: f64,
}

crate::heapless!(CapGroup);

pub(crate) struct GeodesicSweepCapsAccum {
    anchor: Option<Point>,
    global_reach: f64,
    groups: Vec<CapGroup>,
    group_anchor: Option<Point>,
    group_reach: f64,
    group_len: usize,
}

impl GeodesicSweepCapsAccum {
    pub(crate) fn try_new(segment_count: usize) -> Result<Self> {
        let mut groups = Vec::new();
        crate::boundary::try_reserve_vec(&mut groups, segment_count.div_ceil(GEODESIC_CAP_GROUP))?;
        Ok(Self {
            anchor: None,
            global_reach: 0.0,
            groups,
            group_anchor: None,
            group_reach: 0.0,
            group_len: 0,
        })
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    pub(crate) fn push_segment(&mut self, segment: GeodesicSegment, metric: &impl GeodesicMetric) {
        if self.anchor.is_none() {
            self.anchor = Some(segment.start);
        }
        if self.group_len == 0 {
            self.group_anchor = Some(segment.start);
            self.group_reach = 0.0;
        }
        let group_anchor = self.group_anchor.expect("group anchor set with segment");
        let from_group = metric.segment_length(group_anchor, segment.start);
        self.group_reach = self.group_reach.max(from_group + segment.length);
        self.group_len += 1;
        if self.group_len == GEODESIC_CAP_GROUP {
            self.flush_group(metric);
        }
    }

    fn flush_group(&mut self, metric: &impl GeodesicMetric) {
        if self.group_len == 0 {
            return;
        }
        let anchor = self.anchor.expect("cap anchor set with segment");
        let group_anchor = self.group_anchor.expect("group anchor set with segment");
        self.global_reach = self
            .global_reach
            .max(metric.segment_length(anchor, group_anchor) + self.group_reach);
        self.groups.push(CapGroup {
            anchor: group_anchor,
            reach: self.group_reach,
        });
        self.group_len = 0;
    }

    pub(crate) fn finish(
        mut self,
        point_only: &[Point],
        metric: &impl GeodesicMetric,
    ) -> Option<GeodesicSweepCaps> {
        self.flush_group(metric);
        let anchor = self.anchor.or_else(|| point_only.first().copied())?;
        let mut global_reach = self.global_reach;
        for &point in point_only {
            global_reach = global_reach.max(metric.segment_length(anchor, point));
        }
        Some(GeodesicSweepCaps {
            anchor,
            global_reach,
            groups: self.groups,
        })
    }
}

pub(in crate::geometry) struct GeodesicSweepCaps {
    pub(in crate::geometry) anchor: Point,
    pub(in crate::geometry) global_reach: f64,
    pub(in crate::geometry) groups: Vec<CapGroup>,
}

#[derive(Clone, Copy)]
pub(crate) struct GeodesicSweepCapsView<'a> {
    pub(crate) anchor: Point,
    pub(crate) global_reach: f64,
    pub(crate) groups: &'a [CapGroup],
}

impl GeodesicSweepCaps {
    pub(crate) fn view(&self) -> GeodesicSweepCapsView<'_> {
        GeodesicSweepCapsView {
            anchor: self.anchor,
            global_reach: self.global_reach,
            groups: &self.groups,
        }
    }
}

pub(crate) fn geodesic_ordered_rows_into(
    vertices: &[Point],
    caps: GeodesicSweepCapsView<'_>,
    metric: &impl GeodesicMetric,
    deterministic_ties: bool,
    rows: &mut Vec<RowProbe>,
) {
    rows.clear();
    rows.reserve(vertices.len());
    rows.extend(vertices.iter().enumerate().map(|(index, &vertex)| {
        let bound =
            (metric.point_distance_lower_bound(vertex, caps.anchor) - caps.global_reach).max(0.0);
        RowProbe {
            bound,
            vertex: index as u32,
        }
    }));
    if deterministic_ties {
        rows.sort_unstable_by(|left, right| {
            left.bound
                .total_cmp(&right.bound)
                .then(left.vertex.cmp(&right.vertex))
        });
    } else {
        rows.sort_unstable_by(|left, right| left.bound.total_cmp(&right.bound));
    }
}

pub(crate) fn geodesic_capped_sweep_with_parts(
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    best: f64,
    rows: &mut Vec<RowProbe>,
    segment_scratch: &mut GeodesicSegmentScratch,
) -> Result<f64> {
    let Some(caps) = target_parts.caps.as_ref() else {
        return Ok(best);
    };
    geodesic_capped_distance_sweep_core(
        vertices,
        &target_parts.segments,
        &target_parts.point_only,
        caps.view(),
        metric,
        best,
        rows,
        segment_scratch,
    )
}

fn geodesic_capped_distance_sweep_core(
    vertices: &[Point],
    segments: &[GeodesicSegment],
    point_only: &[Point],
    caps: GeodesicSweepCapsView<'_>,
    metric: &impl GeodesicMetric,
    mut best: f64,
    rows: &mut Vec<RowProbe>,
    segment_scratch: &mut GeodesicSegmentScratch,
) -> Result<f64> {
    geodesic_ordered_rows_into(vertices, caps, metric, false, rows);
    for &RowProbe {
        bound: row_bound,
        vertex: vertex_index,
    } in rows.iter()
    {
        if row_bound > best {
            break;
        }
        let vertex = vertices[vertex_index as usize];
        for (
            group_index,
            &CapGroup {
                anchor: group_anchor,
                reach,
            },
        ) in caps.groups.iter().enumerate()
        {
            if metric.point_distance_lower_bound(vertex, group_anchor) - reach > best {
                continue;
            }
            let first = group_index * GEODESIC_CAP_GROUP;
            let chunk = &segments[first..segments.len().min(first + GEODESIC_CAP_GROUP)];
            for &segment in chunk {
                let bound =
                    metric.point_distance_lower_bound(vertex, segment.start) - segment.length;
                if bound > best {
                    continue;
                }
                let result = metric.point_segment_query(
                    vertex,
                    segment,
                    GeodesicSegmentQuery::Minimum {
                        cutoff: best,
                        witness: GeodesicWitnessNeed::No,
                    },
                    segment_scratch,
                )?;
                if let GeodesicSegmentQueryResult::Attained { minimum, .. } = result {
                    best = best.min(minimum.distance);
                }
            }
        }
        for &point in point_only {
            if metric.point_distance_lower_bound(vertex, point) > best {
                continue;
            }
            best = best.min(metric.segment_length(vertex, point));
        }
    }
    Ok(best)
}

pub(crate) fn geodesic_capped_dwithin_with_parts(
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
    rows: &mut Vec<RowProbe>,
    segment_scratch: &mut GeodesicSegmentScratch,
) -> Result<bool> {
    let Some(caps) = target_parts.caps.as_ref() else {
        return Ok(false);
    };
    let segments = &target_parts.segments;
    let point_only = &target_parts.point_only;
    geodesic_ordered_rows_into(vertices, caps.view(), metric, false, rows);
    for &RowProbe {
        bound: row_bound,
        vertex: vertex_index,
    } in rows.iter()
    {
        if row_bound > distance {
            break;
        }
        let vertex = vertices[vertex_index as usize];
        for (
            group_index,
            &CapGroup {
                anchor: group_anchor,
                reach,
            },
        ) in caps.groups.iter().enumerate()
        {
            if metric.point_distance_lower_bound(vertex, group_anchor) - reach > distance {
                continue;
            }
            let first = group_index * GEODESIC_CAP_GROUP;
            let chunk = &segments[first..segments.len().min(first + GEODESIC_CAP_GROUP)];
            for &segment in chunk {
                let bound =
                    metric.point_distance_lower_bound(vertex, segment.start) - segment.length;
                if bound > distance {
                    continue;
                }
                let result = metric.point_segment_query(
                    vertex,
                    segment,
                    GeodesicSegmentQuery::Threshold {
                        limit: distance,
                        witness: GeodesicWitnessNeed::No,
                    },
                    segment_scratch,
                )?;
                match result {
                    GeodesicSegmentQueryResult::Attained { .. } => return Ok(true),
                    GeodesicSegmentQueryResult::ThresholdOutside => {},
                    GeodesicSegmentQueryResult::ThresholdResolvedOutside { minimum } => {
                        debug_assert!(minimum > distance);
                    },
                    GeodesicSegmentQueryResult::Excluded => {
                        unreachable!("threshold query returned a minimum exclusion")
                    },
                }
            }
        }
        for &point in point_only {
            if metric.point_distance_lower_bound(vertex, point) > distance {
                continue;
            }
            if metric.segment_length(vertex, point) <= distance {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn geodesic_capped_witness_sweep_with_parts(
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    best: Option<GeodesicWitnessCandidate>,
    order_offset: usize,
    swapped: bool,
    rows: &mut Vec<RowProbe>,
    segment_scratch: &mut GeodesicSegmentScratch,
) -> Result<Option<GeodesicWitnessCandidate>> {
    let Some(caps) = target_parts.caps.as_ref() else {
        return Ok(best);
    };
    geodesic_capped_witness_sweep_core(
        vertices,
        &target_parts.segments,
        &target_parts.point_only,
        caps.view(),
        metric,
        best,
        order_offset,
        swapped,
        rows,
        segment_scratch,
    )
}

pub(crate) fn geodesic_target_witness(
    point: Point,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    stack: &mut Vec<u32>,
    segment_scratch: &mut GeodesicSegmentScratch,
) -> Result<Option<GeodesicWitnessCandidate>> {
    let width = target_parts.segments.len() + target_parts.point_only.len();
    if let Some(bvh) = target_parts.geodesic_bvh(metric)? {
        #[cfg(test)]
        geodesic_hausdorff_counters::bvh_target_query();
        return bvh.witness_point(
            point,
            0,
            target_parts,
            metric,
            stack,
            segment_scratch,
            None,
            0,
            false,
            width,
        );
    }

    let mut best = None;
    for (index, &segment) in target_parts.segments.iter().enumerate() {
        let cutoff = witness_refine_limit(best.as_ref(), index);
        let result = metric.point_segment_query(
            point,
            segment,
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
            probe: point,
            target,
            distance: minimum.distance,
            feature: GeodesicTargetFeature::Segment {
                index,
                along: minimum.along,
            },
            order: index,
            swapped: false,
        };
        if geodesic_witness_is_better(best.as_ref(), &candidate) {
            best = Some(candidate);
        }
    }
    for (index, &target) in target_parts.point_only.iter().enumerate() {
        let order = target_parts.segments.len() + index;
        let cutoff = witness_refine_limit(best.as_ref(), order);
        if metric.point_distance_lower_bound(point, target) > cutoff {
            continue;
        }
        let distance = metric.segment_length(point, target);
        let candidate = GeodesicWitnessCandidate {
            probe: point,
            target,
            distance,
            feature: GeodesicTargetFeature::Point,
            order,
            swapped: false,
        };
        if geodesic_witness_is_better(best.as_ref(), &candidate) {
            best = Some(candidate);
        }
    }
    Ok(best)
}

fn geodesic_capped_witness_sweep_core(
    vertices: &[Point],
    segments: &[GeodesicSegment],
    point_only: &[Point],
    caps: GeodesicSweepCapsView<'_>,
    metric: &impl GeodesicMetric,
    mut best: Option<GeodesicWitnessCandidate>,
    order_offset: usize,
    swapped: bool,
    rows: &mut Vec<RowProbe>,
    segment_scratch: &mut GeodesicSegmentScratch,
) -> Result<Option<GeodesicWitnessCandidate>> {
    geodesic_ordered_rows_into(vertices, caps, metric, true, rows);
    let width = segments.len() + point_only.len();
    for &RowProbe {
        bound: row_bound,
        vertex: vertex_index,
    } in rows.iter()
    {
        let best_distance = best
            .as_ref()
            .map_or(f64::INFINITY, |candidate| candidate.distance);
        if row_bound > best_distance {
            break;
        }
        let vertex = vertices[vertex_index as usize];
        for (
            group_index,
            &CapGroup {
                anchor: group_anchor,
                reach,
            },
        ) in caps.groups.iter().enumerate()
        {
            let best_distance = best
                .as_ref()
                .map_or(f64::INFINITY, |candidate| candidate.distance);
            if metric.point_distance_lower_bound(vertex, group_anchor) - reach > best_distance {
                continue;
            }
            let first = group_index * GEODESIC_CAP_GROUP;
            let chunk = &segments[first..segments.len().min(first + GEODESIC_CAP_GROUP)];
            for (offset, &segment) in chunk.iter().enumerate() {
                let edge_index = first + offset;
                let order = order_offset + vertex_index as usize * width + edge_index;
                let best_distance = witness_refine_limit(best.as_ref(), order);
                let bound =
                    metric.point_distance_lower_bound(vertex, segment.start) - segment.length;
                if bound > best_distance {
                    continue;
                }
                let result = metric.point_segment_query(
                    vertex,
                    segment,
                    GeodesicSegmentQuery::Minimum {
                        cutoff: best_distance,
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
                    probe: vertex,
                    target,
                    distance: minimum.distance,
                    feature: GeodesicTargetFeature::Segment {
                        index: edge_index,
                        along: minimum.along,
                    },
                    order,
                    swapped,
                };
                if geodesic_witness_is_better(best.as_ref(), &candidate) {
                    best = Some(candidate);
                }
            }
        }
        for (point_index, &point) in point_only.iter().enumerate() {
            let order = order_offset + vertex_index as usize * width + segments.len() + point_index;
            let best_distance = witness_refine_limit(best.as_ref(), order);
            if metric.point_distance_lower_bound(vertex, point) > best_distance {
                continue;
            }
            let distance = metric.segment_length(vertex, point);
            let candidate = GeodesicWitnessCandidate {
                probe: vertex,
                target: point,
                distance,
                feature: GeodesicTargetFeature::Point,
                order,
                swapped,
            };
            if geodesic_witness_is_better(best.as_ref(), &candidate) {
                best = Some(candidate);
            }
        }
    }
    Ok(best)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn geodesic_witness_sweep_with_parts(
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    best: Option<GeodesicWitnessCandidate>,
    order_offset: usize,
    swapped: bool,
) -> Result<Option<GeodesicWitnessCandidate>> {
    let mut scratch = geodesic_scratch();
    let scratch = &mut *scratch;
    let width = target_parts.segments.len() + target_parts.point_only.len();
    if let Some(bvh) = target_parts.geodesic_bvh(metric)? {
        let mut best = best;
        for (vertex_index, &vertex) in vertices.iter().enumerate() {
            best = bvh.witness_point(
                vertex,
                vertex_index as u32,
                target_parts,
                metric,
                &mut scratch.stack,
                &mut scratch.segment,
                best,
                order_offset,
                swapped,
                width,
            )?;
        }
        Ok(best)
    } else {
        geodesic_capped_witness_sweep_with_parts(
            vertices,
            target_parts,
            metric,
            best,
            order_offset,
            swapped,
            &mut scratch.rows,
            &mut scratch.segment,
        )
    }
}

impl GeodesicParts {
    pub(crate) fn geodesic_bvh(
        &self,
        metric: &impl GeodesicMetric,
    ) -> Result<Option<&GeodesicFacetBvh>> {
        self.facet_bvh
            .get_or_try_init(|| {
                if self.segments.len() >= BVH_MIN_INDEXED_SEGMENTS {
                    GeodesicFacetBvh::build(&self.segments, metric)
                } else {
                    Ok(None)
                }
            })
            .map(|bvh| bvh.as_ref())
    }
}
