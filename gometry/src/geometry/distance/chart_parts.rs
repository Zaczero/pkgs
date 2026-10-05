use std::ops::ControlFlow;
use std::sync::OnceLock;

use crate::HeapSize;
use crate::crs::{ChartEdge, PointEdgeScratch};
use crate::error::Result;
use crate::geographic::lonlat::invalid_lonlat_error;
use crate::geographic::{
    ChartStep, EdgeDirection, GeographicFrame, GeographicInputs, TurnDirection,
};
use crate::geometry::overlay::overlay_all_geographic_from;
use crate::geometry::{
    ChartBox, ChartEdgeBvh, ChartMetric, Coordinates as _, FrameDependentCaches, GeodesicMetric,
    GeographicMetricKey, GeographicPrepared, OverlayOp, Point, PointEdgeQuery,
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
        if let Some(capacity) = shape.point_set_capacity_hint(RunIdentity::Full) {
            let _reservation = point_only.try_reserve(capacity);
        }
        shape.for_each_first_point_only(RunIdentity::Full, |point| point_only.push(point));

        let mut edges = Vec::new();
        if let Some(capacity) = shape.segment_count_capacity_hint(RunIdentity::Full) {
            let _reservation = edges.try_reserve(capacity);
        }
        let mut visit = |chain: &crate::geometry::CoordSeq| {
            if chain.len() == 1 {
                if let Some(point) = chain.first() {
                    point_only.push(point);
                }
                return Ok::<_, crate::error::Error>(());
            }
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

pub(crate) fn geographic_intersection_witness_cached(
    left: &PreparedPlan,
    left_cache: &FrameDependentCaches,
    right: &PreparedPlan,
    right_cache: &FrameDependentCaches,
    frame: GeographicFrame,
) -> Result<Option<Point>> {
    let left = GeographicPrepared::try_new(left, left_cache, frame)?;
    let right = GeographicPrepared::try_new(right, right_cache, frame)?;
    let inputs = GeographicInputs::try_from_first_and_shapes(
        left.view(),
        std::iter::once(right.plan().shape()),
    )
    .map_err(|error| invalid_lonlat_error(error.native.x, error.native.y))?;
    let intersection =
        overlay_all_geographic_from(&inputs, OverlayOp::Intersection, Strictness::Lenient, false)?;
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
    order: u128,
}

#[derive(Clone, Copy)]
enum ChartTarget {
    Point(Point),
    Edge { parameter: f64 },
}

#[derive(Clone, Copy)]
struct ChartPairCandidate {
    distance: f64,
    lower_bound: f64,
    order: u128,
    target: ChartTarget,
}

fn candidate_is_better(distance: f64, order: u128, best: Option<&ChartPairCandidate>) -> bool {
    best.is_none_or(|best| {
        distance < best.distance
            || (distance.total_cmp(&best.distance).is_eq() && order < best.order)
    })
}

fn point_chart_candidate<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
    query: PointEdgeQuery,
    admit_edge: impl Fn(&ChartEdgePart) -> bool,
) -> Result<Option<ChartPairCandidate>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let mut best = None;
    let mut best_distance = match query {
        PointEdgeQuery::Minimum { cutoff } | PointEdgeQuery::Locate { cutoff } => cutoff,
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
        let cutoff = if best
            .as_ref()
            .is_none_or(|best: &ChartPairCandidate| (edge_index as u128) < best.order)
        {
            best_distance.next_up()
        } else {
            *best_distance
        };
        let query = match query {
            PointEdgeQuery::Minimum { .. } => PointEdgeQuery::Minimum { cutoff },
            PointEdgeQuery::Locate { .. } => PointEdgeQuery::Locate { cutoff },
            PointEdgeQuery::Dwithin { .. } => {
                unreachable!("minimum search requires an objective query")
            },
        };
        match metric.point_edge_query(point, &part.edge, envelope_lower, query, scratch)? {
            PointEdgeQueryResult::Minimum(minimum) => {
                lower_bound = lower_bound.min(minimum.lower_bound);
                if candidate_is_better(minimum.distance, edge_index as u128, best.as_ref()) {
                    *best_distance = minimum.distance;
                    best = Some(ChartPairCandidate {
                        distance: minimum.distance,
                        lower_bound: minimum.lower_bound,
                        order: edge_index as u128,
                        target: ChartTarget::Edge {
                            parameter: minimum.parameter,
                        },
                    });
                }
            },
            PointEdgeQueryResult::Excluded => {},
            PointEdgeQueryResult::Within { .. } | PointEdgeQueryResult::Outside { .. } => {
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
    for (index, &target_point) in target
        .point_only
        .iter()
        .enumerate()
        .filter(|_| matches!(query, PointEdgeQuery::Minimum { .. }))
    {
        let point_lower_bound = metric.point_distance_lower_bound(point, target_point)?;
        lower_bound = lower_bound.min(point_lower_bound);
        if best_distance.is_finite() && point_lower_bound > best_distance.next_up() {
            continue;
        }
        let distance = metric.segment_length(point, target_point);
        let order = (target.edges.len() as u128).wrapping_add(index as u128);
        if candidate_is_better(distance, order, best.as_ref()) {
            best_distance = best_distance.min(distance);
            best = Some(ChartPairCandidate {
                distance,
                lower_bound: point_lower_bound,
                order,
                target: ChartTarget::Point(target_point),
            });
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
    candidate: &ChartPairCandidate,
    target: &ChartParts,
    metric: &M,
) -> Result<ChartPairMinimum>
where
    M: ChartMetric,
{
    let target = match candidate.target {
        ChartTarget::Point(point) => (point, point.x),
        ChartTarget::Edge { parameter } => {
            let part = target
                .edges
                .get(candidate.order as usize)
                .expect("candidate ordinal belongs to chart parts");
            (
                metric.chart_edge_witness(part.start, part.end, &part.edge, parameter, None)?,
                metric.chart_edge_position(&part.edge, parameter).x,
            )
        },
    };
    let frame = metric.chart_frame();
    let axis = frame.longitude();
    let first_chart_x = axis
        .nearest_longitude_alias(target.1, point.x)
        .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
    let step = axis
        .shortest_alias_step(first_chart_x, target.1)
        .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?;
    Ok(ChartPairMinimum {
        first: point,
        second: target.0,
        distance: candidate.distance,
        lower_bound: candidate.lower_bound,
        intent: ChartStep::Shortest(step),
        order: candidate.order,
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
    .map(|candidate| materialize_point_chart_candidate(point, &candidate, target, metric))
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
        let ChartTarget::Edge { parameter } = candidate.target else {
            unreachable!("locate searches only stored chart edges");
        };
        (candidate.order as usize, parameter)
    }))
}

pub(crate) fn point_chart_dwithin<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    limit: f64,
    scratch: &mut PointEdgeScratch,
    stack: &mut Vec<(u32, f64)>,
) -> Result<bool>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let mut best = limit;
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
                debug_assert!(evaluations <= 128);
                Ok(Some(()))
            },
            PointEdgeQueryResult::Outside {
                lower_bound,
                evaluations,
                budget_exhausted,
            } => {
                debug_assert!(evaluations <= 128);
                debug_assert!(budget_exhausted || lower_bound > limit);
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
            return Ok(true);
        }
    } else if !target.edges.is_empty() {
        let bound = metric.point_envelope_bound(point);
        for (edge_index, part) in target.edges.iter().enumerate() {
            if visit_edge(edge_index, bound(part.bounds), &mut best)?.is_some() {
                return Ok(true);
            }
        }
    }
    for &target in &target.point_only {
        if metric.point_distance_lower_bound(point, target)? > limit.next_up() {
            continue;
        }
        if metric.segment_length(point, target) <= limit {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Clone, Copy)]
struct ChartShapePairCandidate {
    first: Point,
    second: Point,
    distance: f64,
    intent: ChartStep,
    order: u128,
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

fn shape_pair_candidate_is_better(
    distance: f64,
    order: u128,
    best: Option<&ChartShapePairCandidate>,
) -> bool {
    best.is_none_or(|best| {
        distance < best.distance
            || (distance.total_cmp(&best.distance).is_eq() && order < best.order)
    })
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
    for &point in &left.vertices {
        if point_chart_dwithin(point, right, metric, limit, scratch, stack)? {
            return Ok(true);
        }
    }
    for &point in &right.vertices {
        if point_chart_dwithin(point, left, metric, limit, scratch, stack)? {
            return Ok(true);
        }
    }
    Ok(false)
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
        let Some(candidate) = point_chart_minimum(point, right, metric, scratch, stack)? else {
            continue;
        };
        let order = (row as u128)
            .wrapping_mul(right_width)
            .wrapping_add(candidate.order);
        if shape_pair_candidate_is_better(candidate.distance, order, best.as_ref()) {
            best = Some(ChartShapePairCandidate {
                first: candidate.first,
                second: candidate.second,
                distance: candidate.distance,
                intent: candidate.intent,
                order,
            });
        }
    }
    for (row, &point) in right.vertices.iter().enumerate() {
        let Some(candidate) = point_chart_minimum(point, left, metric, scratch, stack)? else {
            continue;
        };
        let order = reverse_order_base
            .wrapping_add((row as u128).wrapping_mul(left_width))
            .wrapping_add(candidate.order);
        if shape_pair_candidate_is_better(candidate.distance, order, best.as_ref()) {
            best = Some(ChartShapePairCandidate {
                first: candidate.second,
                second: candidate.first,
                distance: candidate.distance,
                intent: reverse_chart_step(candidate.intent),
                order,
            });
        }
    }
    Ok(best.map(|candidate| ChartPairMinimum {
        first: candidate.first,
        second: candidate.second,
        distance: candidate.distance,
        lower_bound: 0.0,
        intent: candidate.intent,
        order: candidate.order,
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
}
