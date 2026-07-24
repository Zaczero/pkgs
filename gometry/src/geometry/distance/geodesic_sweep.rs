#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
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
    lengths: Vec<f64>,
    groups: Vec<CapGroup>,
    group_anchor: Option<Point>,
    group_reach: f64,
    group_len: usize,
}

impl GeodesicSweepCapsAccum {
    pub(crate) fn new(segment_count: usize) -> Self {
        Self {
            anchor: None,
            global_reach: 0.0,
            lengths: Vec::with_capacity(segment_count),
            groups: Vec::with_capacity(segment_count.div_ceil(GEODESIC_CAP_GROUP)),
            group_anchor: None,
            group_reach: 0.0,
            group_len: 0,
        }
    }

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
        self.lengths.push(segment.length);
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
            lengths: self.lengths,
            groups: self.groups,
        })
    }
}

pub(in crate::geometry) struct GeodesicSweepCaps {
    pub(in crate::geometry) anchor: Point,
    pub(in crate::geometry) global_reach: f64,
    pub(in crate::geometry) lengths: Vec<f64>,
    pub(in crate::geometry) groups: Vec<CapGroup>,
}

#[derive(Clone, Copy)]
pub(crate) struct GeodesicSweepCapsView<'a> {
    pub(crate) anchor: Point,
    pub(crate) global_reach: f64,
    pub(crate) lengths: &'a [f64],
    pub(crate) groups: &'a [CapGroup],
}

impl GeodesicSweepCaps {
    pub(crate) fn view(&self) -> GeodesicSweepCapsView<'_> {
        GeodesicSweepCapsView {
            anchor: self.anchor,
            global_reach: self.global_reach,
            lengths: &self.lengths,
            groups: &self.groups,
        }
    }
}

pub(crate) trait GeodesicSweepEdge {
    fn start(&self) -> Point;
    fn end(&self) -> Point;
    fn length(&self, metric: &impl GeodesicMetric) -> f64;
}

impl GeodesicSweepEdge for Segment {
    fn start(&self) -> Point {
        self.start.point()
    }

    fn end(&self) -> Point {
        self.end.point()
    }

    fn length(&self, metric: &impl GeodesicMetric) -> f64 {
        metric.segment_length(self.start(), self.end())
    }
}

impl GeodesicSweepEdge for GeodesicSegment {
    fn start(&self) -> Point {
        self.start
    }

    fn end(&self) -> Point {
        self.end
    }

    fn length(&self, _metric: &impl GeodesicMetric) -> f64 {
        self.length
    }
}

pub(crate) fn geodesic_sweep_caps_into<'a, E: GeodesicSweepEdge>(
    edges: &[E],
    point_only: &[Point],
    metric: &impl GeodesicMetric,
    lengths: &'a mut Vec<f64>,
    groups: &'a mut Vec<CapGroup>,
) -> Option<GeodesicSweepCapsView<'a>> {
    lengths.clear();
    groups.clear();
    let anchor = edges
        .first()
        .map(GeodesicSweepEdge::start)
        .or_else(|| point_only.first().copied())?;
    let mut global_reach = 0.0_f64;
    lengths.reserve(edges.len());
    groups.reserve(edges.len().div_ceil(GEODESIC_CAP_GROUP));
    for chunk in edges.chunks(GEODESIC_CAP_GROUP) {
        let group_anchor = chunk[0].start();
        let mut reach = 0.0_f64;
        for edge in chunk {
            let length = edge.length(metric);
            let from_group = metric.segment_length(group_anchor, edge.start());
            reach = reach.max(from_group + length);
            lengths.push(length);
        }
        global_reach = global_reach.max(metric.segment_length(anchor, group_anchor) + reach);
        groups.push(CapGroup {
            anchor: group_anchor,
            reach,
        });
    }
    for &point in point_only {
        global_reach = global_reach.max(metric.segment_length(anchor, point));
    }
    Some(GeodesicSweepCapsView {
        anchor,
        global_reach,
        lengths,
        groups,
    })
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

pub(crate) fn geodesic_capped_witness_sweep(
    vertices: &[Point],
    edges: &[GeodesicSegment],
    point_only: &[Point],
    metric: &impl GeodesicMetric,
    mut best: Option<GeodesicWitnessCandidate>,
    order_offset: usize,
    swapped: bool,
    cap_lengths: &mut Vec<f64>,
    cap_groups: &mut Vec<CapGroup>,
    rows: &mut Vec<RowProbe>,
) -> Option<GeodesicWitnessCandidate> {
    let Some(caps) = geodesic_sweep_caps_into(edges, point_only, metric, cap_lengths, cap_groups)
    else {
        return best;
    };
    geodesic_ordered_rows_into(vertices, caps, metric, true, rows);
    let width = edges.len() + point_only.len();
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
        // Tier 3: group caps, then member caps, then exact.
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
            let chunk = &edges[first..edges.len().min(first + GEODESIC_CAP_GROUP)];
            for (offset, edge) in chunk.iter().enumerate() {
                let edge_index = first + offset;
                let order = order_offset + vertex_index as usize * width + edge_index;
                let best_distance = witness_refine_limit(best.as_ref(), order);
                let bound = metric.point_distance_lower_bound(vertex, edge.start())
                    - caps.lengths[edge_index];
                if bound > best_distance {
                    continue;
                }
                let witness = metric.point_segment_witness(vertex, *edge, best_distance);
                let candidate = GeodesicWitnessCandidate {
                    probe: vertex,
                    target: witness.foot,
                    distance: witness.distance,
                    order,
                    swapped,
                };
                if geodesic_witness_is_better(best.as_ref(), &candidate) {
                    best = Some(candidate);
                }
            }
        }
        for (point_index, &point) in point_only.iter().enumerate() {
            let order = order_offset + vertex_index as usize * width + edges.len() + point_index;
            let best_distance = witness_refine_limit(best.as_ref(), order);
            if metric.point_distance_lower_bound(vertex, point) > best_distance {
                continue;
            }
            let distance = metric.segment_length(vertex, point);
            let witness = GeodesicSegmentWitness {
                distance,
                foot: point,
                along: 0.0,
            };
            let candidate = GeodesicWitnessCandidate {
                probe: vertex,
                target: witness.foot,
                distance: witness.distance,
                order,
                swapped,
            };
            if geodesic_witness_is_better(best.as_ref(), &candidate) {
                best = Some(candidate);
            }
        }
    }
    best
}

pub(crate) fn geodesic_capped_witness_sweep_with_parts(
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    mut best: Option<GeodesicWitnessCandidate>,
    order_offset: usize,
    swapped: bool,
    rows: &mut Vec<RowProbe>,
) -> Option<GeodesicWitnessCandidate> {
    let Some(caps) = target_parts.caps.as_ref() else {
        return best;
    };
    geodesic_ordered_rows_into(vertices, caps.view(), metric, true, rows);
    let width = target_parts.segments.len() + target_parts.point_only.len();
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
            let chunk = &target_parts.segments
                [first..target_parts.segments.len().min(first + GEODESIC_CAP_GROUP)];
            for (offset, &segment) in chunk.iter().enumerate() {
                let edge_index = first + offset;
                let order = order_offset + vertex_index as usize * width + edge_index;
                let best_distance = witness_refine_limit(best.as_ref(), order);
                let bound = metric.point_distance_lower_bound(vertex, segment.start)
                    - caps.lengths[edge_index];
                if bound > best_distance {
                    continue;
                }
                let witness = metric.point_segment_witness(vertex, segment, best_distance);
                let candidate = GeodesicWitnessCandidate {
                    probe: vertex,
                    target: witness.foot,
                    distance: witness.distance,
                    order,
                    swapped,
                };
                if geodesic_witness_is_better(best.as_ref(), &candidate) {
                    best = Some(candidate);
                }
            }
        }
        if !target_parts.point_only.is_empty() {
            for (point_index, &point) in target_parts.point_only.iter().enumerate() {
                let order = order_offset
                    + vertex_index as usize * width
                    + target_parts.segments.len()
                    + point_index;
                let best_distance = witness_refine_limit(best.as_ref(), order);
                if metric.point_distance_lower_bound(vertex, point) > best_distance {
                    continue;
                }
                let distance = metric.segment_length(vertex, point);
                let witness = GeodesicSegmentWitness {
                    distance,
                    foot: point,
                    along: 0.0,
                };
                let candidate = GeodesicWitnessCandidate {
                    probe: vertex,
                    target: witness.foot,
                    distance: witness.distance,
                    order,
                    swapped,
                };
                if geodesic_witness_is_better(best.as_ref(), &candidate) {
                    best = Some(candidate);
                }
            }
        }
    }
    best
}

fn geodesic_capped_witness_vertex_with_parts(
    vertex: Point,
    vertex_index: u32,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    mut best: Option<GeodesicWitnessCandidate>,
    order_offset: usize,
    swapped: bool,
) -> Option<GeodesicWitnessCandidate> {
    let caps = target_parts.caps.as_ref()?;
    let width = target_parts.segments.len() + target_parts.point_only.len();
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
        let chunk = &target_parts.segments
            [first..target_parts.segments.len().min(first + GEODESIC_CAP_GROUP)];
        for (offset, &segment) in chunk.iter().enumerate() {
            let edge_index = first + offset;
            let order = order_offset + vertex_index as usize * width + edge_index;
            let best_distance = witness_refine_limit(best.as_ref(), order);
            let bound =
                metric.point_distance_lower_bound(vertex, segment.start) - caps.lengths[edge_index];
            if bound > best_distance {
                continue;
            }
            let witness = metric.point_segment_witness(vertex, segment, best_distance);
            let candidate = GeodesicWitnessCandidate {
                probe: vertex,
                target: witness.foot,
                distance: witness.distance,
                order,
                swapped,
            };
            if geodesic_witness_is_better(best.as_ref(), &candidate) {
                best = Some(candidate);
            }
        }
    }
    if !target_parts.point_only.is_empty() {
        for (point_index, &point) in target_parts.point_only.iter().enumerate() {
            let order = order_offset
                + vertex_index as usize * width
                + target_parts.segments.len()
                + point_index;
            let best_distance = witness_refine_limit(best.as_ref(), order);
            if metric.point_distance_lower_bound(vertex, point) > best_distance {
                continue;
            }
            let distance = metric.segment_length(vertex, point);
            let witness = GeodesicSegmentWitness {
                distance,
                foot: point,
                along: 0.0,
            };
            let candidate = GeodesicWitnessCandidate {
                probe: vertex,
                target: witness.foot,
                distance: witness.distance,
                order,
                swapped,
            };
            if geodesic_witness_is_better(best.as_ref(), &candidate) {
                best = Some(candidate);
            }
        }
    }
    best
}

pub(crate) fn geodesic_witness_sweep_with_parts(
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    best: Option<GeodesicWitnessCandidate>,
    order_offset: usize,
    swapped: bool,
) -> Option<GeodesicWitnessCandidate> {
    let mut scratch_guard = GeodesicScratchGuard::take();
    let scratch = &mut scratch_guard.scratch;
    let width = target_parts.segments.len() + target_parts.point_only.len();
    if let Some(bvh) = target_parts.geodesic_bvh(metric) {
        let mut best = best;
        for (vertex_index, &vertex) in vertices.iter().enumerate() {
            best = match bvh.witness_point(
                vertex,
                vertex_index as u32,
                target_parts,
                metric,
                &mut scratch.stack,
                best,
                order_offset,
                swapped,
                width,
            ) {
                WitnessPointOutcome::Best(updated) => updated,
                WitnessPointOutcome::LimitExceeded => geodesic_capped_witness_vertex_with_parts(
                    vertex,
                    vertex_index as u32,
                    target_parts,
                    metric,
                    best,
                    order_offset,
                    swapped,
                ),
            };
        }
        best
    } else {
        geodesic_capped_witness_sweep_with_parts(
            vertices,
            target_parts,
            metric,
            best,
            order_offset,
            swapped,
            &mut scratch.rows,
        )
    }
}

impl GeodesicParts {
    pub(crate) fn geodesic_bvh(&self, metric: &impl GeodesicMetric) -> Option<&GeodesicFacetBvh> {
        self.facet_bvh
            .get_or_init(|| {
                (self.segments.len() >= GEODESIC_BVH_MIN_INDEXED_SEGMENTS)
                    .then(|| GeodesicFacetBvh::build(&self.segments, metric))
                    .flatten()
            })
            .as_ref()
    }
}
