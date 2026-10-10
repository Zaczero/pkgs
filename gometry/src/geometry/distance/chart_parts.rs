use std::ops::ControlFlow;
use std::sync::OnceLock;

use crate::HeapSize;
use crate::crs::{ChartEdge, POINT_EDGE_EVALUATION_SAFETY_CEILING, PointEdgeScratch};
use crate::error::Result;
use crate::geographic::lonlat::invalid_lonlat_error;
use crate::geographic::{
    ChartStep, EdgeDirection, GeographicFrame, GeographicInputs, TurnDirection,
};
use crate::geometry::overlay::overlay_all_geographic_from_with_metric;
use crate::geometry::{
    ChartBox, ChartEdgeBvh, ChartMetric, Coordinates as _, DwithinOutcome, FrameDependentCaches,
    GeodesicMetric, GeographicMetricKey, GeographicPrepared, OverlayOp, Point, PointEdgeQuery,
    PointEdgeQueryResult, PreparedPlan, RunIdentity, Shape, Strictness,
    graph_intersects_data_geographic,
};

const CHART_BVH_MIN_EDGES: usize = 8;

#[derive(Clone, Copy)]
pub(crate) struct ChartEdgePart {
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) bounds: ChartBox,
    pub(crate) edge: ChartEdge,
}
crate::heapless!(ChartEdgePart);

pub(crate) struct ChartParts {
    pub(crate) edges: Vec<ChartEdgePart>,
    pub(crate) vertices: Vec<Point>,
    pub(crate) point_only: Vec<Point>,
    bvh: OnceLock<Option<ChartEdgeBvh>>,
}

impl ChartParts {
    pub(crate) fn measured_route(
        points: impl Iterator<Item = Point>,
        metric: &impl ChartMetric,
    ) -> Result<Self> {
        let mut edges = Vec::new();
        let mut previous = None;
        for end in points {
            if let Some(start) = previous {
                let edge = metric.chart_edge(start, end)?;
                edges.push(ChartEdgePart {
                    start,
                    end,
                    bounds: edge.bounds(),
                    edge,
                });
            }
            previous = Some(end);
        }
        Ok(Self {
            edges,
            vertices: Vec::new(),
            point_only: Vec::new(),
            bvh: OnceLock::new(),
        })
    }

    pub(crate) fn build(shape: &Shape, metric: &impl ChartMetric) -> Result<Self> {
        let mut vertices = Vec::new();
        shape.for_each_first_point_run(|point| vertices.push(point));

        let mut point_only = Vec::new();
        shape.append_first_point_only(RunIdentity::Full, &mut point_only);

        let mut edges = Vec::new();
        let mut visit = |chain: &crate::geometry::CoordSeq| {
            if chain.len() == 1 {
                if let Some(point) = chain.first() {
                    point_only.reserve(1);
                    point_only.push(point);
                }
                return Ok::<_, crate::error::Error>(());
            }
            let segment_count = chain.len().saturating_sub(1);
            edges.reserve(segment_count);
            for [start, end] in chain.segment_pairs() {
                let edge = metric.chart_edge(start, end)?;
                edges.push(ChartEdgePart {
                    start,
                    end,
                    bounds: edge.bounds(),
                    edge,
                });
            }
            Ok(())
        };
        if matches!(shape, Shape::LineString(_) | Shape::MultiLineString(_)) {
            // LRS stations count every stored occurrence, including shared
            // multipart chains and zero-length edges.
            shape.try_for_each_segment_chain(&mut visit)?;
        } else {
            shape.try_for_each_first_segment_chain(RunIdentity::Full, &mut visit)?;
        }
        Ok(Self {
            edges,
            vertices,
            point_only,
            bvh: OnceLock::new(),
        })
    }

    pub(crate) fn bvh(&self) -> Option<&ChartEdgeBvh> {
        self.bvh
            .get_or_init(|| {
                (self.edges.len() >= CHART_BVH_MIN_EDGES)
                    .then(|| ChartEdgeBvh::build(&self.edges))
                    .flatten()
            })
            .as_ref()
    }
}

impl HeapSize for ChartParts {
    fn heap_bytes(&self) -> usize {
        self.edges
            .heap_bytes()
            .saturating_add(self.vertices.heap_bytes())
            .saturating_add(self.point_only.heap_bytes())
            .saturating_add(self.bvh.heap_bytes())
    }
}

impl PreparedPlan {
    pub(in crate::geometry) fn cached_chart_parts(
        &self,
        frame_cache: &crate::geometry::FrameDependentCaches,
        key: &crate::geometry::GeographicMetricKey,
        metric: &impl ChartMetric,
    ) -> Result<std::sync::Arc<ChartParts>> {
        if let Some(parts) = frame_cache.cached_chart_parts(key) {
            return Ok(parts);
        }
        GeographicPrepared::try_new(self, frame_cache, metric.chart_frame())?;
        let parts = std::sync::Arc::new(ChartParts::build(self.shape(), metric)?);
        let entry = frame_cache.merge_chart_parts(key, &parts);
        Ok(entry.unwrap_or(parts))
    }
}

pub(crate) fn geographic_intersects_cached(
    left: &PreparedPlan,
    left_cache: &crate::geometry::FrameDependentCaches,
    right: &PreparedPlan,
    right_cache: &crate::geometry::FrameDependentCaches,
    frame: GeographicFrame,
) -> Result<bool> {
    let left = GeographicPrepared::try_new(left, left_cache, frame)?;
    let right = GeographicPrepared::try_new(right, right_cache, frame)?;
    graph_intersects_data_geographic(&left, &right)
}

pub(crate) fn geographic_intersection_witness_cached<M: ChartMetric>(
    left: &PreparedPlan,
    left_cache: &FrameDependentCaches,
    right: &PreparedPlan,
    right_cache: &FrameDependentCaches,
    frame: GeographicFrame,
    metric: &M,
) -> Result<Option<Point>> {
    let left = GeographicPrepared::try_new(left, left_cache, frame)?;
    let right = GeographicPrepared::try_new(right, right_cache, frame)?;
    let inputs = GeographicInputs::try_from_first_and_shapes(
        left.view(),
        std::iter::once(right.plan().shape()),
    )
    .map_err(|error| invalid_lonlat_error(error.native.x, error.native.y))?;
    let metric_access = || Ok(metric);
    let intersection = overlay_all_geographic_from_with_metric(
        &inputs,
        OverlayOp::Intersection,
        Strictness::Lenient,
        false,
        &crate::geometry::SourceArcContext::new(
            crate::geometry::TopologyDomain::Geographic(frame),
            Some(&metric_access),
        ),
    )?;
    Ok(
        match intersection.try_for_each_first_point_run(ControlFlow::Break) {
            ControlFlow::Break(point) => Some(point),
            ControlFlow::Continue(()) => None,
        },
    )
}

pub(crate) struct ChartPairMinimum {
    pub(crate) first: Point,
    pub(crate) second: Point,
    pub(crate) distance: f64,
    pub(crate) lower_bound: f64,
    pub(crate) intent: ChartStep,
    role: crate::geometry::WitnessRole,
    order: u128,
}

#[derive(Clone, Copy)]
enum ChartTarget<'a> {
    Point(Point),
    Edge {
        part: &'a ChartEdgePart,
        parameter: f64,
    },
}

#[derive(Clone, Copy)]
struct ChartPairCandidate<'a> {
    distance: f64,
    lower_bound: f64,
    source_ordinal: usize,
    target: ChartTarget<'a>,
}

impl ChartPairCandidate<'_> {
    fn role(&self) -> crate::geometry::WitnessRole {
        match self.target {
            ChartTarget::Point(_) => crate::geometry::WitnessRole::Point,
            ChartTarget::Edge { part, parameter } => part.edge.witness_role(parameter),
        }
    }

    fn precedes(&self, best: Option<&Self>, query: PointEdgeQuery) -> bool {
        best.is_none_or(|best| {
            self.distance < best.distance
                || self.distance.total_cmp(&best.distance).is_eq()
                    && match query {
                        PointEdgeQuery::Minimum { .. } => self.source_ordinal < best.source_ordinal,
                        PointEdgeQuery::Nearest { .. } | PointEdgeQuery::Locate { .. } => self
                            .role()
                            .cmp_tie(best.role(), || {
                                self.source_ordinal.cmp(&best.source_ordinal)
                            })
                            .is_lt(),
                        PointEdgeQuery::Dwithin { .. } => {
                            unreachable!("threshold queries do not select witnesses")
                        },
                    }
        })
    }
}

fn point_chart_candidate<'a, M>(
    point: Point,
    target: &'a ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
    query: PointEdgeQuery,
    admit_edge: impl Fn(&ChartEdgePart) -> bool,
) -> Result<Option<ChartPairCandidate<'a>>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let mut best = None;
    let mut best_distance = match query {
        PointEdgeQuery::Minimum { cutoff }
        | PointEdgeQuery::Nearest { cutoff }
        | PointEdgeQuery::Locate { cutoff } => cutoff,
        PointEdgeQuery::Dwithin { .. } => {
            unreachable!("minimum search requires an objective query")
        },
    };
    let mut lower_bound = f64::INFINITY;
    let mut visit_edge = |edge_index: usize, envelope_lower: f64, best_distance: &mut f64| {
        let part = target
            .edges
            .get(edge_index)
            .expect("chart BVH edge id belongs to its prepared chart parts");
        if !admit_edge(part) {
            return Ok(None);
        }
        let cutoff = if matches!(
            query,
            PointEdgeQuery::Nearest { .. } | PointEdgeQuery::Locate { .. }
        ) || best
            .as_ref()
            .is_none_or(|best: &ChartPairCandidate<'a>| edge_index < best.source_ordinal)
        {
            best_distance.next_up()
        } else {
            *best_distance
        };
        let query = match query {
            PointEdgeQuery::Minimum { .. } => PointEdgeQuery::Minimum { cutoff },
            PointEdgeQuery::Nearest { .. } => PointEdgeQuery::Nearest { cutoff },
            PointEdgeQuery::Locate { .. } => PointEdgeQuery::Locate { cutoff },
            PointEdgeQuery::Dwithin { .. } => {
                unreachable!("minimum search requires an objective query")
            },
        };
        match metric.point_edge_query(point, &part.edge, envelope_lower, query, scratch)? {
            PointEdgeQueryResult::Minimum(minimum) => {
                lower_bound = lower_bound.min(minimum.lower_bound);
                if minimum.distance >= cutoff {
                    return Ok(None);
                }
                let candidate = ChartPairCandidate {
                    distance: minimum.distance,
                    lower_bound: minimum.lower_bound,
                    source_ordinal: edge_index,
                    target: ChartTarget::Edge {
                        part,
                        parameter: minimum.parameter,
                    },
                };
                if candidate.precedes(best.as_ref(), query) {
                    *best_distance = minimum.distance;
                    best = Some(candidate);
                }
            },
            PointEdgeQueryResult::Excluded => {},
            PointEdgeQueryResult::Within { .. }
            | PointEdgeQueryResult::Outside { .. }
            | PointEdgeQueryResult::Unresolved { .. } => {
                unreachable!("minimum chart-edge queries return minima or exclusions")
            },
        }
        Ok::<Option<()>, crate::error::Error>(None)
    };
    if let Some(bvh) = target.bvh() {
        bvh.visit_point_candidates(
            &target.edges,
            point,
            metric,
            &mut best_distance,
            None,
            stack,
            &mut visit_edge,
        )?;
    } else if !target.edges.is_empty() {
        let bound = metric.point_envelope_bound(point);
        for (edge_index, part) in target.edges.iter().enumerate() {
            visit_edge(edge_index, bound(part.bounds), &mut best_distance)?;
        }
    }
    for (index, &target_point) in target.point_only.iter().enumerate().filter(|_| {
        matches!(
            query,
            PointEdgeQuery::Minimum { .. } | PointEdgeQuery::Nearest { .. }
        )
    }) {
        let point_lower_bound = metric.point_distance_lower_bound(point, target_point)?;
        lower_bound = lower_bound.min(point_lower_bound);
        if best_distance.is_finite() && point_lower_bound > best_distance.next_up() {
            continue;
        }
        let distance = metric.segment_length(point, target_point);
        // Both backing Vecs are non-ZST and allocation-bounded, so this combined ordinal fits usize.
        let source_ordinal = target.edges.len().wrapping_add(index);
        let candidate = ChartPairCandidate {
            distance,
            lower_bound: point_lower_bound,
            source_ordinal,
            target: ChartTarget::Point(target_point),
        };
        if candidate.precedes(best.as_ref(), query) {
            best_distance = best_distance.min(distance);
            best = Some(candidate);
        }
    }
    Ok(best.map(|mut candidate| {
        candidate.lower_bound = candidate
            .lower_bound
            .min(lower_bound)
            .min(candidate.distance);
        candidate
    }))
}

fn materialize_point_chart_candidate<M>(
    point: Point,
    candidate: &ChartPairCandidate<'_>,
    metric: &M,
) -> Result<ChartPairMinimum>
where
    M: ChartMetric,
{
    let role = candidate.role();
    let target_point = match candidate.target {
        ChartTarget::Point(point) => point,
        ChartTarget::Edge { part, parameter } => {
            metric.chart_edge_witness(part.start, part.end, &part.edge, parameter, None)?
        },
    };
    let frame = metric.chart_frame();
    let step = frame
        .longitude()
        .shortest_alias_step(point.x, target_point.x)
        .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
    Ok(ChartPairMinimum {
        first: point,
        second: target_point,
        distance: candidate.distance,
        lower_bound: candidate.lower_bound,
        intent: ChartStep::Shortest(step),
        role,
        order: candidate.source_ordinal as u128,
    })
}

pub(crate) fn point_chart_minimum<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
) -> Result<Option<ChartPairMinimum>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    point_chart_candidate(
        point,
        target,
        metric,
        scratch,
        stack,
        PointEdgeQuery::Minimum {
            cutoff: f64::INFINITY,
        },
        |_| true,
    )?
    .map(|candidate| materialize_point_chart_candidate(point, &candidate, metric))
    .transpose()
}

pub(crate) fn point_chart_distance<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
) -> Result<f64>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    Ok(point_chart_candidate(
        point,
        target,
        metric,
        scratch,
        stack,
        PointEdgeQuery::Minimum {
            cutoff: f64::INFINITY,
        },
        |_| true,
    )?
    .map_or(f64::INFINITY, |candidate| candidate.distance))
}

pub(crate) fn point_chart_locate<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    admit_edge: impl Fn(&ChartEdgePart) -> bool,
) -> Result<Option<(usize, f64)>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let mut scratch = super::chart_scratch();
    let scratch = &mut *scratch;
    let candidate = point_chart_candidate(
        point,
        target,
        metric,
        &mut scratch.point_edge,
        &mut scratch.stack,
        PointEdgeQuery::Locate {
            cutoff: f64::INFINITY,
        },
        admit_edge,
    )?;
    Ok(candidate.map(|candidate| {
        let ChartTarget::Edge { parameter, .. } = candidate.target else {
            unreachable!("locate searches only stored chart edges");
        };
        (candidate.source_ordinal, parameter)
    }))
}

pub(crate) fn point_chart_dwithin<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    limit: f64,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
) -> Result<DwithinOutcome>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let mut best = limit;
    let mut outcome = DwithinOutcome::Outside;
    let mut visit_edge = |edge_index: usize, envelope_lower: f64, _best: &mut f64| {
        let part = target
            .edges
            .get(edge_index)
            .expect("chart BVH edge id belongs to its prepared chart parts");
        match metric.point_edge_query(
            point,
            &part.edge,
            envelope_lower,
            PointEdgeQuery::Dwithin { limit },
            scratch,
        )? {
            PointEdgeQueryResult::Within { evaluations } => {
                debug_assert!(evaluations <= POINT_EDGE_EVALUATION_SAFETY_CEILING);
                Ok(Some(()))
            },
            PointEdgeQueryResult::Outside {
                lower_bound,
                evaluations,
            } => {
                debug_assert!(evaluations <= POINT_EDGE_EVALUATION_SAFETY_CEILING);
                debug_assert!(lower_bound > limit);
                Ok(None)
            },
            PointEdgeQueryResult::Unresolved {
                lower_bound,
                evaluations,
            } => {
                debug_assert!(evaluations <= POINT_EDGE_EVALUATION_SAFETY_CEILING);
                debug_assert!(lower_bound <= limit);
                outcome = outcome.combine(DwithinOutcome::Unresolved);
                Ok(None)
            },
            PointEdgeQueryResult::Excluded => Ok(None),
            PointEdgeQueryResult::Minimum(_) => {
                unreachable!("threshold chart-edge queries return a threshold result")
            },
        }
    };
    if let Some(bvh) = target.bvh() {
        if bvh
            .visit_point_candidates(
                &target.edges,
                point,
                metric,
                &mut best,
                Some(limit),
                stack,
                &mut visit_edge,
            )?
            .is_some()
        {
            return Ok(DwithinOutcome::Within);
        }
    } else if !target.edges.is_empty() {
        let bound = metric.point_envelope_bound(point);
        for (edge_index, part) in target.edges.iter().enumerate() {
            if visit_edge(edge_index, bound(part.bounds), &mut best)?.is_some() {
                return Ok(DwithinOutcome::Within);
            }
        }
    }
    for &target in &target.point_only {
        if metric.point_distance_lower_bound(point, target)? > limit.next_up() {
            continue;
        }
        if metric.segment_length(point, target) <= limit {
            return Ok(DwithinOutcome::Within);
        }
    }
    Ok(outcome)
}

fn reverse_chart_step(step: ChartStep) -> ChartStep {
    match step {
        ChartStep::Circle(TurnDirection::Positive) => ChartStep::Circle(TurnDirection::Negative),
        ChartStep::Circle(TurnDirection::Negative) => ChartStep::Circle(TurnDirection::Positive),
        ChartStep::Shortest(EdgeDirection::Minor(delta)) => {
            ChartStep::Shortest(EdgeDirection::Minor(-delta))
        },
        ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Positive)) => {
            ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Negative))
        },
        ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Negative)) => {
            ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Positive))
        },
    }
}

impl ChartPairMinimum {
    fn precedes(&self, best: Option<&Self>) -> bool {
        best.is_none_or(|best| {
            self.distance < best.distance
                || self.distance.total_cmp(&best.distance).is_eq()
                    && self
                        .role
                        .cmp_tie(best.role, || self.order.cmp(&best.order))
                        .is_lt()
        })
    }
}

const fn feature_width(parts: &ChartParts) -> u128 {
    (parts.edges.len() as u128).wrapping_add(parts.point_only.len() as u128)
}

pub(crate) fn chart_distance_with_parts<M>(
    left: &ChartParts,
    right: &ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
) -> Result<f64>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let mut best = f64::INFINITY;
    for &point in &left.vertices {
        if let Some(candidate) = point_chart_candidate(
            point,
            right,
            metric,
            scratch,
            stack,
            PointEdgeQuery::Minimum { cutoff: best },
            |_| true,
        )? {
            best = best.min(candidate.distance);
        }
    }
    for &point in &right.vertices {
        if let Some(candidate) = point_chart_candidate(
            point,
            left,
            metric,
            scratch,
            stack,
            PointEdgeQuery::Minimum { cutoff: best },
            |_| true,
        )? {
            best = best.min(candidate.distance);
        }
    }
    Ok(best)
}

pub(crate) fn chart_dwithin_with_parts<M>(
    left: &ChartParts,
    right: &ChartParts,
    metric: &M,
    limit: f64,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
) -> Result<bool>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let mut outcome = DwithinOutcome::Outside;
    for &point in &left.vertices {
        outcome = outcome.combine(point_chart_dwithin(
            point, right, metric, limit, scratch, stack,
        )?);
        if outcome == DwithinOutcome::Within {
            return Ok(true);
        }
    }
    for &point in &right.vertices {
        outcome = outcome.combine(point_chart_dwithin(
            point, left, metric, limit, scratch, stack,
        )?);
        if outcome == DwithinOutcome::Within {
            return Ok(true);
        }
    }
    outcome.into_bool()
}

pub(crate) fn chart_nearest_points_with_parts<M>(
    left: &ChartParts,
    right: &ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
) -> Result<Option<ChartPairMinimum>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let right_width = feature_width(right);
    let left_width = feature_width(left);
    let reverse_order_base = (left.vertices.len() as u128).wrapping_mul(right_width);
    let mut best = None;
    for (row, &point) in left.vertices.iter().enumerate() {
        let Some(raw) = point_chart_candidate(
            point,
            right,
            metric,
            scratch,
            stack,
            PointEdgeQuery::Nearest {
                cutoff: f64::INFINITY,
            },
            |_| true,
        )?
        else {
            continue;
        };
        let mut candidate = materialize_point_chart_candidate(point, &raw, metric)?;
        candidate.order = (row as u128)
            .wrapping_mul(right_width)
            .wrapping_add(candidate.order);
        if candidate.precedes(best.as_ref()) {
            best = Some(candidate);
        }
    }
    for (row, &point) in right.vertices.iter().enumerate() {
        let Some(raw) = point_chart_candidate(
            point,
            left,
            metric,
            scratch,
            stack,
            PointEdgeQuery::Nearest {
                cutoff: f64::INFINITY,
            },
            |_| true,
        )?
        else {
            continue;
        };
        let mut candidate = materialize_point_chart_candidate(point, &raw, metric)?;
        candidate.order = reverse_order_base
            .wrapping_add((row as u128).wrapping_mul(left_width))
            .wrapping_add(candidate.order);
        if candidate.precedes(best.as_ref()) {
            std::mem::swap(&mut candidate.first, &mut candidate.second);
            candidate.intent = reverse_chart_step(candidate.intent);
            best = Some(candidate);
        }
    }
    Ok(best.map(|mut candidate| {
        candidate.lower_bound = 0.0;
        candidate
    }))
}

pub(crate) fn cached_chart_distance<M>(
    left: &PreparedPlan,
    left_cache: &FrameDependentCaches,
    right: &PreparedPlan,
    right_cache: &FrameDependentCaches,
    crs: &str,
    semi_major: f64,
    flattening: f64,
    metric: &M,
) -> Result<f64>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    if geographic_intersects_cached(left, left_cache, right, right_cache, metric.chart_frame())? {
        return Ok(0.0);
    }
    let key = GeographicMetricKey::new_at_generation(
        crs,
        semi_major,
        flattening,
        metric.runtime_generation(),
    );
    let left_parts = left.cached_chart_parts(left_cache, &key, metric)?;
    let right_parts = right.cached_chart_parts(right_cache, &key, metric)?;
    let mut scratch = super::chart_scratch();
    let scratch = &mut *scratch;
    let (point_edge, chart_stack) = (&mut scratch.point_edge, &mut scratch.stack);
    chart_distance_with_parts(&left_parts, &right_parts, metric, point_edge, chart_stack)
}

pub(crate) fn cached_chart_dwithin<M>(
    left: &PreparedPlan,
    left_cache: &FrameDependentCaches,
    right: &PreparedPlan,
    right_cache: &FrameDependentCaches,
    crs: &str,
    semi_major: f64,
    flattening: f64,
    metric: &M,
    limit: f64,
) -> Result<bool>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    if limit.is_nan() {
        return Err(crate::crs::CrsError::message(
            "geodesic dwithin limit is not finite",
        ));
    }
    if limit < 0.0 || limit.is_infinite() && limit.is_sign_negative() {
        return Ok(false);
    }
    if left.shape().is_empty() || right.shape().is_empty() {
        metric.ensure_shape(left.shape())?;
        metric.ensure_shape(right.shape())?;
        return Ok(false);
    }
    if limit.is_infinite() {
        metric.ensure_shape(left.shape())?;
        metric.ensure_shape(right.shape())?;
        return Ok(true);
    }
    if geographic_intersects_cached(left, left_cache, right, right_cache, metric.chart_frame())? {
        return Ok(true);
    }
    if limit == 0.0 {
        return Ok(false);
    }
    let key = GeographicMetricKey::new_at_generation(
        crs,
        semi_major,
        flattening,
        metric.runtime_generation(),
    );
    let left_parts = left.cached_chart_parts(left_cache, &key, metric)?;
    let right_parts = right.cached_chart_parts(right_cache, &key, metric)?;
    let mut scratch = super::chart_scratch();
    let scratch = &mut *scratch;
    let (point_edge, chart_stack) = (&mut scratch.point_edge, &mut scratch.stack);
    chart_dwithin_with_parts(
        &left_parts,
        &right_parts,
        metric,
        limit,
        point_edge,
        chart_stack,
    )
}

pub(crate) fn cached_chart_nearest_points_with_intent<M>(
    left: &PreparedPlan,
    left_cache: &FrameDependentCaches,
    right: &PreparedPlan,
    right_cache: &FrameDependentCaches,
    crs: &str,
    semi_major: f64,
    flattening: f64,
    metric: &M,
) -> Result<Option<ChartPairMinimum>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    if left.shape().is_empty() || right.shape().is_empty() {
        return Ok(None);
    }
    if geographic_intersects_cached(left, left_cache, right, right_cache, metric.chart_frame())? {
        let witness = geographic_intersection_witness_cached(
            left,
            left_cache,
            right,
            right_cache,
            metric.chart_frame(),
            metric,
        )?
        .ok_or_else(|| {
            crate::crs::CrsError::message(
                "exact geographic intersection did not provide a nearest-point witness",
            )
        })?;
        return Ok(Some(ChartPairMinimum {
            first: witness,
            second: witness,
            distance: 0.0,
            lower_bound: 0.0,
            intent: ChartStep::Shortest(EdgeDirection::Minor(0.0)),
            role: crate::geometry::WitnessRole::Point,
            order: 0,
        }));
    }
    let key = GeographicMetricKey::new_at_generation(
        crs,
        semi_major,
        flattening,
        metric.runtime_generation(),
    );
    let left_parts = left.cached_chart_parts(left_cache, &key, metric)?;
    let right_parts = right.cached_chart_parts(right_cache, &key, metric)?;
    let mut scratch = super::chart_scratch();
    let scratch = &mut *scratch;
    let (point_edge, chart_stack) = (&mut scratch.point_edge, &mut scratch.stack);
    chart_nearest_points_with_parts(&left_parts, &right_parts, metric, point_edge, chart_stack)
}

pub(crate) fn cached_chart_nearest_points<M>(
    left: &PreparedPlan,
    left_cache: &FrameDependentCaches,
    right: &PreparedPlan,
    right_cache: &FrameDependentCaches,
    crs: &str,
    semi_major: f64,
    flattening: f64,
    metric: &M,
) -> Result<Option<(Point, Point)>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    Ok(cached_chart_nearest_points_with_intent(
        left,
        left_cache,
        right,
        right_cache,
        crs,
        semi_major,
        flattening,
        metric,
    )?
    .map(|candidate| (candidate.first, candidate.second)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chart_metric(geodesic: &geographiclib_rs::Geodesic) -> crate::crs::EllipsoidMetric<'_> {
        crate::crs::EllipsoidMetric::for_crs84_geometry_test(geodesic)
    }

    #[test]
    fn chart_parts_point_only_storage_tracks_emitted_support_not_line_vertices() {
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = chart_metric(&geodesic);
        let coordinates: Vec<_> = (0..1024)
            .map(|index| Point::new_raw_xy(f64::from(index) * 0.1, 0.0))
            .collect();
        let shape = Shape::LineString(crate::geometry::LineSeq::from_proven(
            crate::geometry::CoordSeq::try_from_points(&coordinates).unwrap(),
        ));
        let parts = ChartParts::build(&shape, &metric).unwrap();

        assert_eq!(parts.edges.len(), coordinates.len() - 1);
        assert!(parts.point_only.is_empty());
        assert_eq!(parts.point_only.capacity(), 0);
    }

    #[test]
    fn chart_parts_lineal_occurrences_keep_shared_edges_with_bounded_allocations() {
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = chart_metric(&geodesic);
        let line = crate::geometry::LineSeq::from_proven(
            crate::geometry::CoordSeq::try_from_xy(&[
                crate::geometry::XY::new(0.0, 0.0),
                crate::geometry::XY::new(1.0, 1.0),
            ])
            .unwrap(),
        );
        let line_count = 1024;
        let shape = Shape::MultiLineString(
            crate::geometry::MultiLineString::from_dense(vec![line; line_count])
                .expect("the fixture contains line members"),
        );
        let (parts, allocations) =
            crate::test_support::allocation_count(|| ChartParts::build(&shape, &metric).unwrap());

        assert_eq!(parts.edges.len(), line_count);
        assert_eq!(parts.vertices.len(), 2);
        assert!(parts.point_only.is_empty());
        assert!(
            allocations <= 32,
            "building {line_count} shared line occurrences used {allocations} allocations"
        );
    }

    fn selected<'a>(
        point: Point,
        parts: &'a ChartParts,
        metric: &crate::crs::EllipsoidMetric<'_>,
        query: PointEdgeQuery,
    ) -> ChartPairCandidate<'a> {
        point_chart_candidate(
            point,
            parts,
            metric,
            &mut PointEdgeScratch::default(),
            &mut Vec::new(),
            query,
            |_| true,
        )
        .unwrap()
        .unwrap()
    }

    #[test]
    fn chart_witness_role_later_interior_wins_only_equal_backend_distances() {
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = chart_metric(&geodesic);
        let point = Point::new_raw_xy(0.0, 0.0);
        let endpoint = ChartParts::build(
            &crate::io::parse_wkt("LINESTRING (0 1,1 2)").unwrap(),
            &metric,
        )
        .unwrap();
        let interior = ChartParts::build(
            &crate::io::parse_wkt("LINESTRING (-1 1,1 1)").unwrap(),
            &metric,
        )
        .unwrap();
        for query in [
            PointEdgeQuery::Minimum {
                cutoff: f64::INFINITY,
            },
            PointEdgeQuery::Nearest {
                cutoff: f64::INFINITY,
            },
            PointEdgeQuery::Locate {
                cutoff: f64::INFINITY,
            },
        ] {
            assert_eq!(
                selected(point, &endpoint, &metric, query)
                    .distance
                    .to_bits(),
                110_574.388_557_798_78_f64.to_bits()
            );
            assert_eq!(
                selected(point, &interior, &metric, query)
                    .distance
                    .to_bits(),
                110_574.388_557_798_78_f64.to_bits()
            );
        }
        let parts = ChartParts::build(
            &crate::io::parse_wkt(
                "MULTILINESTRING ZM ((0 1 10 100,1 2 20 200),(-1 1 30 300,1 1 50 500))",
            )
            .unwrap(),
            &metric,
        )
        .unwrap();
        let minimum = selected(point, &parts, &metric, PointEdgeQuery::Minimum {
            cutoff: f64::INFINITY,
        });
        assert_eq!(minimum.source_ordinal, 0);
        for query in [
            PointEdgeQuery::Nearest {
                cutoff: f64::INFINITY,
            },
            PointEdgeQuery::Locate {
                cutoff: f64::INFINITY,
            },
        ] {
            let candidate = selected(point, &parts, &metric, query);
            assert_eq!(candidate.source_ordinal, 1);
            assert_eq!(candidate.role(), crate::geometry::WitnessRole::Interior);
            assert_eq!(candidate.distance.to_bits(), minimum.distance.to_bits());
            let witness = materialize_point_chart_candidate(point, &candidate, &metric).unwrap();
            assert_eq!(witness.second.z(), Some(40.0));
            assert_eq!(witness.second.m(), Some(400.0));
        }
        let farther = ChartParts::build(
            &crate::io::parse_wkt(
                "MULTILINESTRING ((0 1,1 2),(-1 1.0000000000000002,1 1.0000000000000002))",
            )
            .unwrap(),
            &metric,
        )
        .unwrap();
        let farther_interior = ChartParts::build(
            &crate::io::parse_wkt("LINESTRING (-1 1.0000000000000002,1 1.0000000000000002)")
                .unwrap(),
            &metric,
        )
        .unwrap();
        assert_eq!(
            selected(point, &farther_interior, &metric, PointEdgeQuery::Nearest {
                cutoff: f64::INFINITY
            })
            .distance
            .to_bits(),
            110_574.388_557_798_78_f64.to_bits() + 2,
        );
        for query in [
            PointEdgeQuery::Nearest {
                cutoff: f64::INFINITY,
            },
            PointEdgeQuery::Locate {
                cutoff: f64::INFINITY,
            },
        ] {
            assert_eq!(selected(point, &farther, &metric, query).source_ordinal, 0);
        }
    }

    #[test]
    fn chart_witness_role_pair_fold_preserves_reverse_interior_provenance() {
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = chart_metric(&geodesic);
        let point = ChartParts::build(
            &crate::io::parse_wkt("POINT ZM (0 0 7 70)").unwrap(),
            &metric,
        )
        .unwrap();
        let collection = ChartParts::build(&crate::io::parse_wkt("GEOMETRYCOLLECTION ZM (POINT ZM (0 1 10 100),LINESTRING ZM (-1 1 30 300,1 1 50 500))").unwrap(), &metric).unwrap();
        for reverse in [false, true] {
            let (left, right) = if reverse {
                (&collection, &point)
            } else {
                (&point, &collection)
            };
            let result = chart_nearest_points_with_parts(
                left,
                right,
                &metric,
                &mut PointEdgeScratch::default(),
                &mut Vec::new(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(result.role, crate::geometry::WitnessRole::Interior);
            assert_eq!(result.lower_bound, 0.0);
            let witness = if reverse { result.first } else { result.second };
            assert_eq!(witness.z(), Some(40.0));
            assert_eq!(witness.m(), Some(400.0));
        }
    }

    #[test]
    fn chart_witness_role_bvh_admits_later_equal_interior_after_endpoint() {
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = chart_metric(&geodesic);
        let point = Point::new_raw_xy(0.0, 0.0);
        let shape = crate::io::parse_wkt("MULTILINESTRING ((0 1,-1 2),(-120 10,-119 10),(-100 10,-99 10),(-80 10,-79 10),(80 10,81 10),(100 10,101 10),(120 10,121 10),(140 10,141 10),(-1 1,1 1))").unwrap();
        let parts = ChartParts::build(&shape, &metric).unwrap();
        let mut visited = Vec::new();
        let mut best_distance = f64::INFINITY;
        parts
            .bvh()
            .unwrap()
            .visit_point_candidates(
                &parts.edges,
                point,
                &metric,
                &mut best_distance,
                None,
                &mut Vec::new(),
                |ordinal, _, _| {
                    visited.push(ordinal);
                    Ok::<Option<()>, crate::error::Error>(None)
                },
            )
            .unwrap();
        assert!(
            visited.iter().position(|&id| id == 0).unwrap()
                < visited.iter().position(|&id| id == 8).unwrap(),
            "{visited:?}"
        );
        let streaming = ChartParts::build(&shape, &metric).unwrap();
        assert!(streaming.bvh.set(None).is_ok());
        for source in [&parts, &streaming] {
            for query in [
                PointEdgeQuery::Nearest {
                    cutoff: f64::INFINITY,
                },
                PointEdgeQuery::Locate {
                    cutoff: f64::INFINITY,
                },
            ] {
                let result = selected(point, source, &metric, query);
                assert_eq!(result.source_ordinal, 8);
                assert_eq!(result.role(), crate::geometry::WitnessRole::Interior);
            }
        }
    }

    #[test]
    fn chart_witness_role_rounding_does_not_turn_interior_into_endpoint() {
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        let metric = chart_metric(&geodesic);
        let start = Point::new_raw_xy(0.0, 1.0);
        let end = Point::new_raw_xy(f64::from_bits(1), 1.0_f64.next_up());
        let edge = metric.chart_edge(start, end).unwrap();
        let witness = metric
            .chart_edge_witness(start, end, &edge, 0.5, None)
            .unwrap();
        assert_eq!(witness.x.to_bits(), start.x.to_bits());
        assert_eq!(witness.y.to_bits(), start.y.to_bits());
        assert_eq!(
            edge.witness_role(0.5),
            crate::geometry::WitnessRole::Interior
        );
        assert_eq!(edge.witness_role(0.0), crate::geometry::WitnessRole::Point);
        let degenerate = metric.chart_edge(start, start).unwrap();
        assert_eq!(
            degenerate.witness_role(0.5),
            crate::geometry::WitnessRole::Point
        );
        let pole = metric
            .chart_edge(Point::new_raw_xy(-1.0, 90.0), Point::new_raw_xy(1.0, 90.0))
            .unwrap();
        assert_eq!(pole.witness_role(0.5), crate::geometry::WitnessRole::Point);
        let circle = metric
            .chart_edge(Point::new_raw_xy(0.0, 1.0), Point::new_raw_xy(360.0, 1.0))
            .unwrap();
        assert_eq!(
            circle.witness_role(0.5),
            crate::geometry::WitnessRole::Interior
        );
    }

    #[test]
    fn chart_control_distance_and_witness_separations_match_the_independent_minimum() {
        let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
        let metric = crate::crs::GeodesicMetric::from_model(&model).unwrap();
        let resolved = crate::crs::ResolvedMetric::from_model(&model).unwrap();
        let left = PreparedPlan::new(
            crate::io::parse_wkt("LINESTRING (-70 30, -68 31, -66 30, -64 31)").unwrap(),
        );
        let right = PreparedPlan::new(
            crate::io::parse_wkt("LINESTRING (-70 30.3, -68 31.3, -66 30.3, -64 31.3)").unwrap(),
        );
        let left_cache = FrameDependentCaches::default();
        let right_cache = FrameDependentCaches::default();
        let distance =
            crate::pair_distance_geodesic(&metric, &left, &left_cache, &right, &right_cache)
                .unwrap();
        let (first, second) = crate::metric_nearest_points_resolved(
            &resolved,
            &left,
            &left_cache,
            &right,
            &right_cache,
        )
        .unwrap()
        .unwrap();
        let separation = |first, second| {
            crate::pair_distance_geodesic(
                &metric,
                &PreparedPlan::new(Shape::Point(first)),
                &FrameDependentCaches::default(),
                &PreparedPlan::new(Shape::Point(second)),
                &FrameDependentCaches::default(),
            )
            .unwrap()
        };
        let nearest_separation = separation(first, second);
        let shortest = crate::metric_shortest_line_resolved(
            &resolved,
            &left,
            &right,
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        )
        .unwrap();
        let Shape::LineString(line) = shortest else {
            panic!("disjoint control must produce a shortest line");
        };
        let shortest_separation = separation(line.first().unwrap(), line.last().unwrap());
        let expected = 28_757.993_325_000_867;
        for value in [distance, nearest_separation, shortest_separation] {
            assert!(
                (value - expected).abs() <= 13.0e-9,
                "distance {value:.17} differs from the independent 70-digit minimum"
            );
        }
        assert_eq!(
            distance.to_bits(),
            nearest_separation.to_bits(),
            "distance={distance:.17}, nearest separation={nearest_separation:.17}"
        );
        assert_eq!(
            nearest_separation.to_bits(),
            shortest_separation.to_bits(),
            "nearest separation={nearest_separation:.17}, shortest-line separation={shortest_separation:.17}"
        );
    }

    #[test]
    fn point_chart_minimum_uses_selected_endpoints_for_collection_intent_in_native_units() {
        let geodesic = geographiclib_rs::Geodesic::wgs84();
        for (unit_name, radians_per_unit) in [
            ("degree", std::f64::consts::PI / 180.0),
            ("grad", std::f64::consts::PI / 200.0),
            ("radian", 1.0),
        ] {
            let unit =
                crate::geographic::AngularFactor::try_new(unit_name, radians_per_unit).unwrap();
            let frame = GeographicFrame::try_new(unit, unit, 0.0).unwrap();
            let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, frame);
            let native_per_degree = (std::f64::consts::PI / 180.0) / radians_per_unit;

            for (case, source_degrees, target_degrees) in [
                (
                    "rounded endpoints",
                    (-123.456_789, -55.678_912),
                    (168.434_969_3, 54.449_710_9),
                ),
                ("seam aliases", (179.8, 0.0), (-179.8, 1.0)),
                ("positive half turn", (0.0, 0.0), (180.0, 0.0)),
                ("negative half turn", (180.0, 0.0), (0.0, 0.0)),
                ("zero-longitude connection", (12.5, -40.0), (12.5, 40.0)),
                (
                    "equivalent full-period positions",
                    (0.0, 15.0),
                    (360.0, 15.0),
                ),
            ] {
                let source = Point::new_raw_xy(
                    source_degrees.0 * native_per_degree,
                    source_degrees.1 * native_per_degree,
                );
                let target_point = Point::new_raw_xy(
                    target_degrees.0 * native_per_degree,
                    target_degrees.1 * native_per_degree,
                );
                let target_collection =
                    Shape::geometry_collection(vec![Shape::Point(target_point)]).unwrap();
                let point_parts = ChartParts::build(&Shape::Point(target_point), &metric).unwrap();
                let collection_parts = ChartParts::build(&target_collection, &metric).unwrap();

                let mut point_scratch = PointEdgeScratch::new();
                let mut point_stack = Vec::new();
                let point_minimum = point_chart_minimum(
                    source,
                    &point_parts,
                    &metric,
                    &mut point_scratch,
                    &mut point_stack,
                )
                .unwrap()
                .unwrap();

                let mut collection_scratch = PointEdgeScratch::new();
                let mut collection_stack = Vec::new();
                let collection_minimum = point_chart_minimum(
                    source,
                    &collection_parts,
                    &metric,
                    &mut collection_scratch,
                    &mut collection_stack,
                )
                .unwrap()
                .unwrap();

                let expected_step = frame
                    .longitude()
                    .shortest_alias_step(source.x, target_point.x)
                    .unwrap();
                assert_eq!(
                    collection_minimum.intent,
                    ChartStep::Shortest(expected_step),
                    "{unit_name}: {case}",
                );
                assert_eq!(
                    collection_minimum.first.x.to_bits(),
                    source.x.to_bits(),
                    "{unit_name}: {case} source longitude",
                );
                assert_eq!(
                    collection_minimum.first.y.to_bits(),
                    source.y.to_bits(),
                    "{unit_name}: {case} source latitude",
                );
                assert_eq!(
                    collection_minimum.second.x.to_bits(),
                    target_point.x.to_bits(),
                    "{unit_name}: {case} target longitude",
                );
                assert_eq!(
                    collection_minimum.second.y.to_bits(),
                    target_point.y.to_bits(),
                    "{unit_name}: {case} target latitude",
                );
                assert_eq!(
                    collection_minimum.distance.to_bits(),
                    point_minimum.distance.to_bits(),
                    "{unit_name}: {case} point/collection distance",
                );
                assert_eq!(
                    collection_minimum.lower_bound.to_bits(),
                    point_minimum.lower_bound.to_bits(),
                    "{unit_name}: {case} point/collection lower bound",
                );
                assert_eq!(
                    collection_minimum.order, point_minimum.order,
                    "{unit_name}: {case} point/collection order",
                );
            }
        }
    }
}
