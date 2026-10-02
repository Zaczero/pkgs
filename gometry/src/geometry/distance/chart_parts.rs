use crate::HeapSize;
use crate::crs::PointEdgeScratch;
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    ChartBox, ChartMetric, Coordinates as _, FrameDependentCaches, GeodesicMetric,
    GeodesicPartsKey, GeographicPrepared, Point, PointEdgeQuery, PointEdgeQueryResult,
    PreparedPlan, RunIdentity, Shape, graph_relate_data_geographic,
};

#[derive(Clone, Copy)]
pub(crate) struct ChartEdgePart {
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) bounds: ChartBox,
}
crate::heapless!(ChartEdgePart);

pub(crate) struct ChartParts {
    pub(crate) edges: Vec<ChartEdgePart>,
    pub(crate) point_only: Vec<Point>,
}

impl ChartParts {
    fn build(shape: &Shape, metric: &impl ChartMetric) -> Result<Self> {
        let mut point_only = Vec::new();
        if let Some(capacity) = shape.point_set_capacity_hint(RunIdentity::Full) {
            let _reservation = point_only.try_reserve(capacity);
        }
        shape.for_each_first_point_only(RunIdentity::Full, |point| point_only.push(point));

        let mut edges = Vec::new();
        if let Some(capacity) = shape.segment_count_capacity_hint(RunIdentity::Full) {
            let _reservation = edges.try_reserve(capacity);
        }
        shape.try_for_each_first_segment_chain(RunIdentity::Full, |chain| {
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
                    bounds: metric.chart_edge_bounds(&edge),
                });
            }
            Ok(())
        })?;
        Ok(Self { edges, point_only })
    }
}

impl HeapSize for ChartParts {
    fn heap_bytes(&self) -> usize {
        self.edges
            .heap_bytes()
            .saturating_add(self.point_only.heap_bytes())
    }
}

impl PreparedPlan {
    pub(in crate::geometry) fn cached_chart_parts(
        &self,
        frame_cache: &crate::geometry::FrameDependentCaches,
        key: &crate::geometry::GeodesicPartsKey,
        metric: &impl ChartMetric,
    ) -> Result<std::sync::Arc<ChartParts>> {
        if let Some(parts) = frame_cache.cached_chart_parts(key) {
            return Ok(parts);
        }
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
    Ok(graph_relate_data_geographic(left, right)?.is_intersects())
}

pub(crate) struct ChartPairMinimum {
    pub(crate) first: Point,
    pub(crate) second: Point,
}

#[derive(Clone, Copy)]
enum ChartTarget {
    Point(Point),
    Edge { part: ChartEdgePart, parameter: f64 },
}

#[derive(Clone, Copy)]
struct ChartPairCandidate {
    distance: f64,
    target: ChartTarget,
}

fn point_chart_candidate<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
) -> Result<Option<ChartPairCandidate>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let mut best = None;
    for &part in &target.edges {
        let edge = metric.chart_edge(part.start, part.end)?;
        let cutoff = best
            .as_ref()
            .map_or(f64::INFINITY, |candidate: &ChartPairCandidate| {
                candidate.distance
            });
        match metric.point_edge_query(
            point,
            &edge,
            part.bounds,
            PointEdgeQuery::Minimum { cutoff },
            scratch,
        )? {
            PointEdgeQueryResult::Minimum(minimum) if minimum.distance < cutoff => {
                best = Some(ChartPairCandidate {
                    distance: minimum.distance,
                    target: ChartTarget::Edge {
                        part,
                        parameter: minimum.parameter,
                    },
                });
            },
            PointEdgeQueryResult::Minimum(_) | PointEdgeQueryResult::Excluded => {},
            PointEdgeQueryResult::Within { .. } | PointEdgeQueryResult::Outside { .. } => {
                unreachable!("minimum chart-edge queries return minima or exclusions")
            },
        }
    }
    for &target in &target.point_only {
        let distance = metric.segment_length(point, target);
        if best
            .as_ref()
            .is_none_or(|candidate: &ChartPairCandidate| distance < candidate.distance)
        {
            best = Some(ChartPairCandidate {
                distance,
                target: ChartTarget::Point(target),
            });
        }
    }
    Ok(best)
}

fn materialize_point_chart_candidate<M>(
    point: Point,
    candidate: ChartPairCandidate,
    metric: &M,
) -> Result<ChartPairMinimum>
where
    M: ChartMetric,
{
    let target = match candidate.target {
        ChartTarget::Point(point) => point,
        ChartTarget::Edge { part, parameter } => {
            let edge = metric.chart_edge(part.start, part.end)?;
            metric.chart_edge_witness(part.start, part.end, &edge, parameter)?
        },
    };
    Ok(ChartPairMinimum {
        first: point,
        second: target,
    })
}

#[expect(
    clippy::float_cmp,
    reason = "admitted canonical coordinates compare by exact topological identity"
)]
fn same_chart_point(left: Point, right: Point, frame: GeographicFrame) -> bool {
    let (left_lon, left_lat) = frame
        .to_geodesic_xy(left.x, left.y)
        .expect("intersection operands were admitted");
    let (right_lon, right_lat) = frame
        .to_geodesic_xy(right.x, right.y)
        .expect("intersection operands were admitted");
    (left_lat == right_lat && left_lat.abs() == 90.0)
        || (left_lon == right_lon && left_lat == right_lat)
}

fn point_chart_intersection_witness<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
) -> Result<ChartPairMinimum>
where
    M: ChartMetric,
{
    if let Some(&target_point) = target
        .point_only
        .iter()
        .find(|&&target_point| same_chart_point(point, target_point, metric.chart_frame()))
    {
        return Ok(ChartPairMinimum {
            first: point,
            second: target_point,
        });
    }
    for &part in &target.edges {
        let edge = metric.chart_edge(part.start, part.end)?;
        if let Some(parameter) = metric.chart_edge_parameter_for_point(point, &edge) {
            let target_point = metric.chart_edge_witness(part.start, part.end, &edge, parameter)?;
            return Ok(ChartPairMinimum {
                first: point,
                second: target_point.with_xy_raw(point.x, point.y),
            });
        }
    }
    // For a filled polygon the point may be in its interior, not on a
    // boundary. The exact relation has already established membership.
    Ok(ChartPairMinimum {
        first: point,
        second: point,
    })
}

pub(crate) fn point_chart_minimum<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
) -> Result<Option<ChartPairMinimum>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    point_chart_candidate(point, target, metric, scratch)?
        .map(|candidate| materialize_point_chart_candidate(point, candidate, metric))
        .transpose()
}

pub(crate) fn point_chart_distance<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    scratch: &mut PointEdgeScratch,
) -> Result<f64>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    Ok(point_chart_candidate(point, target, metric, scratch)?
        .map_or(f64::INFINITY, |candidate| candidate.distance))
}

pub(crate) fn point_chart_dwithin<M>(
    point: Point,
    target: &ChartParts,
    metric: &M,
    limit: f64,
    scratch: &mut PointEdgeScratch,
) -> Result<bool>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    for part in &target.edges {
        let edge = metric.chart_edge(part.start, part.end)?;
        match metric.point_edge_query(
            point,
            &edge,
            part.bounds,
            PointEdgeQuery::Dwithin { limit },
            scratch,
        )? {
            PointEdgeQueryResult::Within { evaluations } => {
                debug_assert!(evaluations <= 128);
                return Ok(true);
            },
            PointEdgeQueryResult::Outside {
                lower_bound,
                evaluations,
                budget_exhausted,
            } => {
                debug_assert!(evaluations <= 128);
                debug_assert!(budget_exhausted || lower_bound > limit);
            },
            PointEdgeQueryResult::Excluded => {},
            PointEdgeQueryResult::Minimum(_) => {
                unreachable!("threshold chart-edge queries return a threshold result")
            },
        }
    }
    for &target in &target.point_only {
        if metric.segment_length(point, target) <= limit {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn cached_point_chart_minimum<M>(
    point: Point,
    point_plan: &PreparedPlan,
    point_cache: &FrameDependentCaches,
    target: &PreparedPlan,
    target_cache: &FrameDependentCaches,
    crs: &str,
    semi_major: f64,
    flattening: f64,
    metric: &M,
) -> Result<Option<ChartPairMinimum>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let intersects = geographic_intersects_cached(
        point_plan,
        point_cache,
        target,
        target_cache,
        metric.chart_frame(),
    )?;
    let key = GeodesicPartsKey::new_at_generation(
        crs,
        semi_major,
        flattening,
        metric.runtime_generation(),
    );
    let parts = target.cached_chart_parts(target_cache, &key, metric)?;
    if intersects {
        return point_chart_intersection_witness(point, &parts, metric).map(Some);
    }
    let mut scratch = super::geodesic_scratch();
    point_chart_minimum(point, &parts, metric, &mut scratch.point_edge)
}

pub(crate) fn cached_point_chart_distance<M>(
    point: Point,
    point_plan: &PreparedPlan,
    point_cache: &FrameDependentCaches,
    target: &PreparedPlan,
    target_cache: &FrameDependentCaches,
    crs: &str,
    semi_major: f64,
    flattening: f64,
    metric: &M,
) -> Result<f64>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    if geographic_intersects_cached(
        point_plan,
        point_cache,
        target,
        target_cache,
        metric.chart_frame(),
    )? {
        return Ok(0.0);
    }
    let key = GeodesicPartsKey::new_at_generation(
        crs,
        semi_major,
        flattening,
        metric.runtime_generation(),
    );
    let parts = target.cached_chart_parts(target_cache, &key, metric)?;
    let mut scratch = super::geodesic_scratch();
    point_chart_distance(point, &parts, metric, &mut scratch.point_edge)
}

pub(crate) fn cached_point_chart_dwithin<M>(
    point: Point,
    point_plan: &PreparedPlan,
    point_cache: &FrameDependentCaches,
    target: &PreparedPlan,
    target_cache: &FrameDependentCaches,
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
    if limit.is_infinite() {
        return Ok(!point_plan.shape().is_empty() && !target.shape().is_empty());
    }
    if geographic_intersects_cached(
        point_plan,
        point_cache,
        target,
        target_cache,
        metric.chart_frame(),
    )? {
        return Ok(true);
    }
    if limit == 0.0 {
        return Ok(false);
    }
    let key = GeodesicPartsKey::new_at_generation(
        crs,
        semi_major,
        flattening,
        metric.runtime_generation(),
    );
    let parts = target.cached_chart_parts(target_cache, &key, metric)?;
    let mut scratch = super::geodesic_scratch();
    point_chart_dwithin(point, &parts, metric, limit, &mut scratch.point_edge)
}

pub(crate) fn is_chart_linework(shape: &Shape) -> bool {
    match shape {
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => true,
        Shape::GeometryCollection(geometries) => geometries.iter().all(is_chart_linework),
        Shape::Polygon(_) | Shape::MultiPolygon(_) => false,
    }
}

pub(crate) fn uses_chart_edge_model(shape: &Shape) -> bool {
    is_chart_linework(shape) && has_chart_edges(shape)
}

fn has_chart_edges(shape: &Shape) -> bool {
    match shape {
        Shape::LineString(line) => line.len() > 1,
        Shape::MultiLineString(lines) => lines.iter().any(|line| line.len() > 1),
        Shape::GeometryCollection(geometries) => geometries.iter().any(has_chart_edges),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => false,
    }
}
