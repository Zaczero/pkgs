use std::num::NonZeroU32;
use std::sync::Arc;

use crate::broadcast::{degrade_linref_linestring, degrade_linref_point};
use crate::crs::local::{
    metric_concave_hull, metric_constructive_shape_budgeted, metric_maximum_inscribed_circle,
    metric_minimum_bounding_circle, metric_minimum_bounding_radius,
    metric_optional_constructive_shape,
};
use crate::crs::{ResolvedMetric, metric_interpolate_m};
use crate::dispatch::MetricOp;
use crate::dispatch::metric::{CachelessRowContext, GeodesicBinaryRow, LinearReferenceRow};
use crate::error::Result;
use crate::geometry::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, DerivedPointStrategy, Dimension,
    GeometryKind, MeasureRange, OriginSpec, PreparedPlan, Shape, SimplifyMethod, SmoothMethod,
    Strictness, TopologyDomain, same_topological_coordinate,
};
use crate::numeric::Positive;
use crate::{
    F64Param, I64Param, line_interpolate_shape, line_interpolate_shape_m, line_substring_shape,
    line_substring_shape_m, metric_maximum_inscribed_radius, metric_minimum_clearance,
    metric_minimum_clearance_line,
};

fn scale_length(value: f64, to_metre: f64) -> f64 {
    value * to_metre
}

fn scale_area(value: f64, to_metre: f64) -> f64 {
    value * to_metre * to_metre
}

fn publish_planar_geographic_edges(
    output: Shape,
    model: &crate::crs::MetricModel,
    domain: TopologyDomain,
    operation: &'static str,
) -> Result<Shape> {
    match (model, domain) {
        (crate::crs::MetricModel::Planar { .. }, TopologyDomain::Geographic(frame)) => {
            crate::geometry::publish_raw_computed_shape(&output, frame, operation)
        },
        _ => Ok(output),
    }
}

fn publish_planar_geographic_edges_budgeted(
    output: Shape,
    model: &crate::crs::MetricModel,
    domain: TopologyDomain,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    match (model, domain) {
        (crate::crs::MetricModel::Planar { .. }, TopologyDomain::Geographic(frame)) => {
            crate::geometry::publish_raw_computed_shape_with_budget(&output, frame, budget)
        },
        _ => Ok(output),
    }
}

fn publish_planar_geographic_constructive(
    output: Shape,
    model: &crate::crs::MetricModel,
    domain: TopologyDomain,
    operation: &'static str,
) -> Result<Shape> {
    match (model, domain, output) {
        (
            crate::crs::MetricModel::Planar { .. },
            TopologyDomain::Geographic(frame),
            Shape::Point(point),
        ) => Ok(Shape::Point(
            frame
                .canonical_computed_point(point)
                .map_err(|error| crate::crs::CrsError::invalid(error.to_string()))?,
        )),
        (crate::crs::MetricModel::Planar { .. }, TopologyDomain::Geographic(frame), output) => {
            crate::geometry::publish_raw_computed_shape(&output, frame, operation)
        },
        (_, _, output) => Ok(output),
    }
}

/// Centroid kernel shared by the spine-routed free function.
pub(crate) fn unary_centroid(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<Shape> {
    crate::geometry::unary_antimeridian_centroid(data.shape(), ctx.domain)
}

/// Envelope kernel shared by the spine-routed free function.
pub(crate) fn unary_envelope(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<Shape> {
    if let TopologyDomain::Geographic(frame) = ctx.domain {
        return crate::geographic::GeographicSupportEnvelope::try_from_shape_with_bounds(
            frame,
            data.shape(),
            data.bounds(),
        )
        .map_err(crate::error::Error::from)?
        .into_shape(frame);
    }
    let output = data.bounds().map_or_else(
        || Ok(Shape::empty_polygon()),
        crate::geometry::bounds_to_shape,
    )?;
    publish_computed_point_or_raw_if_geographic(output, ctx.domain, "envelope")
}

/// Convex-hull kernel shared by the spine-routed free function.
pub(crate) fn unary_convex_hull(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    publish_raw_computed_if_geographic(data.shape().convex_hull()?, ctx.domain, "convex_hull")
}

/// Simplify kernel shared by the spine-routed free function.
pub(crate) fn unary_simplify(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    tolerance: &F64Param,
    method: SimplifyMethod,
    preserve_topology: bool,
) -> Result<Shape> {
    let tolerance = tolerance.get(ctx.lane.row());
    match (ctx.domain, method) {
        (crate::geometry::TopologyDomain::Geographic(frame), SimplifyMethod::Vw) => data
            .shape()
            .simplify_vw_geographic(tolerance, preserve_topology, frame.longitude()),
        (crate::geometry::TopologyDomain::Geographic(frame), SimplifyMethod::Dp) => data
            .shape()
            .simplify_dp_geographic(tolerance, preserve_topology, frame.longitude()),
        (crate::geometry::TopologyDomain::Cartesian, SimplifyMethod::Vw) => {
            data.shape().simplify_vw(tolerance, preserve_topology)
        },
        (crate::geometry::TopologyDomain::Cartesian, SimplifyMethod::Dp) => {
            data.shape().simplify_dp(tolerance, preserve_topology)
        },
    }
}

/// Buffer kernel shared by the spine-routed free function.
pub(crate) fn unary_buffer(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    distance: &F64Param,
    cap_style: BufferCapStyle,
    join_style: BufferJoinStyle,
    quadrant_segments: NonZeroU32,
    miter_limit: Positive,
    side: BufferSide,
) -> Result<Shape> {
    let mut budget = crate::geometry::ExpansionBudget::new("buffer", "quadrant_segments");
    unary_buffer_budgeted(
        data,
        ctx,
        distance,
        cap_style,
        join_style,
        quadrant_segments,
        miter_limit,
        side,
        &mut budget,
    )
}

pub(crate) fn unary_buffer_budgeted(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    distance: &F64Param,
    cap_style: BufferCapStyle,
    join_style: BufferJoinStyle,
    quadrant_segments: NonZeroU32,
    miter_limit: Positive,
    side: BufferSide,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    let distance = distance.get(ctx.lane.row());
    if same_topological_coordinate(distance, 0.0) {
        return match side {
            BufferSide::Both => data.shape().buffer_with_style_budgeted(
                distance,
                cap_style,
                join_style,
                quadrant_segments,
                miter_limit,
                budget,
            ),
            BufferSide::Left | BufferSide::Right => data.shape().buffer_sided_budgeted(
                distance,
                side,
                join_style,
                quadrant_segments,
                miter_limit,
                budget,
            ),
        };
    }
    let model = ctx.metric.require_model(MetricOp::Buffer)?;
    let output = metric_constructive_shape_budgeted(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        distance,
        budget,
        |shape, d, budget| match side {
            BufferSide::Both => shape.buffer_with_style_budgeted(
                d,
                cap_style,
                join_style,
                quadrant_segments,
                miter_limit,
                budget,
            ),
            BufferSide::Left | BufferSide::Right => shape.buffer_sided_budgeted(
                d,
                side,
                join_style,
                quadrant_segments,
                miter_limit,
                budget,
            ),
        },
    )?;
    publish_planar_geographic_edges_budgeted(output, model, ctx.domain, budget)
}

/// Offset-curve kernel shared by the spine-routed free function.
pub(crate) fn unary_offset_curve(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    distance: &F64Param,
    join_style: BufferJoinStyle,
    quadrant_segments: NonZeroU32,
    miter_limit: Positive,
) -> Result<Shape> {
    let mut budget = crate::geometry::ExpansionBudget::new("offset_curve", "quadrant_segments");
    unary_offset_curve_budgeted(
        data,
        ctx,
        distance,
        join_style,
        quadrant_segments,
        miter_limit,
        &mut budget,
    )
}

pub(crate) fn unary_offset_curve_budgeted(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    distance: &F64Param,
    join_style: BufferJoinStyle,
    quadrant_segments: NonZeroU32,
    miter_limit: Positive,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    let distance = distance.get(ctx.lane.row());
    if same_topological_coordinate(distance, 0.0) {
        return data.shape().offset_curve_budgeted(
            distance,
            join_style,
            quadrant_segments,
            miter_limit,
            budget,
        );
    }
    let model = ctx.metric.require_model(MetricOp::OffsetCurve)?;
    let output = metric_constructive_shape_budgeted(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        distance,
        budget,
        |shape, d, budget| {
            shape.offset_curve_budgeted(d, join_style, quadrant_segments, miter_limit, budget)
        },
    )?;
    publish_planar_geographic_edges_budgeted(output, model, ctx.domain, budget)
}

/// Point-on-surface kernel shared by the spine-routed free function.
pub(crate) fn unary_point_on_surface(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    crate::geometry::unary_antimeridian_derived(
        data.shape(),
        ctx.domain,
        DerivedPointStrategy::Interior,
        Shape::point_on_surface,
    )
}

/// Concave-hull kernel shared by the spine-routed free function.
pub(crate) fn unary_concave_hull(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    concavity: &F64Param,
    length_threshold: &F64Param,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::ConcaveHull)?;
    let concavity = concavity.get(ctx.lane.row());
    let output = metric_concave_hull(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        concavity,
        length_threshold.get(ctx.lane.row()),
    )?;
    publish_planar_geographic_edges(output, model, ctx.domain, "concave_hull")
}

/// Polylabel kernel shared by the spine-routed free function.
pub(crate) fn unary_polylabel(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    tolerance: Option<&F64Param>,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::Polylabel)?;
    let output = metric_optional_constructive_shape(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        tolerance.map(|value| value.get(ctx.lane.row())),
        Shape::polylabel,
    )?;
    publish_planar_geographic_constructive(output, model, ctx.domain, "polylabel")
}

/// Maximum-inscribed-circle kernel shared by the spine-routed free function.
pub(crate) fn unary_maximum_inscribed_circle(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    tolerance: Option<&F64Param>,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::MaximumInscribedCircle)?;
    let output = metric_maximum_inscribed_circle(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        tolerance.map(|value| value.get(ctx.lane.row())),
    )?;
    publish_planar_geographic_constructive(output, model, ctx.domain, "maximum_inscribed_circle")
}

/// Maximum-inscribed-radius kernel — CRS-aware, the metric twin of the
/// inscribed circle (so both calls retain the same certified planar center and
/// radius contract).
pub(crate) fn unary_maximum_inscribed_radius(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    tolerance: Option<&F64Param>,
) -> Result<f64> {
    // geometry — there are no output vertices onto which Z/M could be carried,
    let model = ctx.metric.require_model(MetricOp::MaximumInscribedRadius)?;
    metric_maximum_inscribed_radius(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        tolerance.map(|value| value.get(ctx.lane.row())),
    )
}

/// Minimum-bounding-circle kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_bounding_circle(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::MinimumBoundingCircle)?;
    let output = metric_minimum_bounding_circle(model, ctx.frame.crs_str(), data.shape())?;
    publish_planar_geographic_edges(output, model, ctx.domain, "minimum_bounding_circle")
}

/// Minimum-bounding-circle radius kernel shared by scalar and array methods.
pub(crate) fn unary_minimum_bounding_radius(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<f64> {
    let model = ctx.metric.require_model(MetricOp::MinimumBoundingRadius)?;
    metric_minimum_bounding_radius(model, data.shape())
}

/// Minimum-rotated-rectangle kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_rotated_rectangle(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    publish_raw_computed_if_geographic(
        data.shape().minimum_rotated_rectangle()?,
        ctx.domain,
        "minimum_rotated_rectangle",
    )
}

fn publish_raw_computed_if_geographic(
    shape: Shape,
    domain: TopologyDomain,
    operation: &'static str,
) -> Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => Ok(shape),
        TopologyDomain::Geographic(frame) => {
            crate::geometry::publish_raw_computed_shape(&shape, frame, operation)
        },
    }
}

fn publish_computed_point_or_raw_if_geographic(
    shape: Shape,
    domain: TopologyDomain,
    operation: &'static str,
) -> Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => Ok(shape),
        TopologyDomain::Geographic(frame) => {
            crate::geometry::publish_computed_point_or_raw_shape(&shape, frame, operation)
        },
    }
}

/// Boundary kernel shared by the spine-routed free function.
pub(crate) fn unary_boundary(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<Shape> {
    crate::geometry::boundary(data, ctx.domain)
}

/// ``is_empty`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_empty(data: &PreparedPlan, _ctx: &CachelessRowContext<'_>) -> Result<bool> {
    Ok(data.shape().is_empty())
}

/// ``is_closed`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_closed(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<bool> {
    Ok(crate::geometry::shape_is_closed_in_domain(
        data.shape(),
        ctx.domain,
    ))
}

/// ``is_ring`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_ring(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<bool> {
    crate::geometry::is_ring_data_in_frame(data, ctx.domain)
}

/// ``is_ccw`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_ccw(data: &PreparedPlan, _ctx: &CachelessRowContext<'_>) -> Result<bool> {
    Ok(data.shape().is_ccw())
}

/// ``is_simple`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_simple(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<bool> {
    crate::geometry::is_simple_data_in_frame(data, ctx.domain)
}

/// ``is_valid`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_valid(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<bool> {
    Ok(crate::geometry::validate_data_in_frame(data, ctx.domain)?.is_none())
}

/// ``is_convex`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_convex(data: &PreparedPlan, _ctx: &CachelessRowContext<'_>) -> Result<bool> {
    Ok(match data.shape() {
        Shape::Polygon(_) | Shape::Empty(GeometryKind::Polygon, _) => data.shape().is_convex()?,
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => false,
    })
}

/// ``crosses_antimeridian`` kernel shared by the spine-routed free function.
pub(crate) fn unary_crosses_antimeridian(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<bool> {
    let frame = crate::geometry::antimeridian_frame(ctx.frame, ctx.domain, "crosses_antimeridian")?;
    Ok(crate::geometry::crosses_antimeridian_in_frame(
        data.shape(),
        frame,
    ))
}

/// ``split_antimeridian`` kernel shared by the spine-routed free function.
pub(crate) fn unary_split_antimeridian(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    let frame = crate::geometry::antimeridian_frame(ctx.frame, ctx.domain, "split_antimeridian")?;
    data.shape().split_antimeridian_in_frame(frame)
}

/// ``node`` kernel shared by the spine-routed free function.
pub(crate) fn unary_node(data: &PreparedPlan, _ctx: &CachelessRowContext<'_>) -> Result<Shape> {
    data.shape().node(Strictness::Lenient)
}

/// Build-area kernel shared by the scalar and array unary method spine.
pub(crate) fn unary_build_area(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    data.shape().build_area(false)
}

/// Smooth kernel shared by the spine-routed free function.
pub(crate) fn unary_smooth(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    iterations: &I64Param,
    method: SmoothMethod,
    keep_endpoints: bool,
) -> Result<Shape> {
    let mut budget = crate::geometry::ExpansionBudget::new("smooth", "iterations");
    smooth_for_domain(
        data,
        ctx,
        iterations.get(ctx.lane.row()) as i32,
        method,
        keep_endpoints,
        &mut budget,
    )
}

pub(crate) fn unary_smooth_budgeted(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    iterations: &I64Param,
    method: SmoothMethod,
    keep_endpoints: bool,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    smooth_for_domain(
        data,
        ctx,
        iterations.get(ctx.lane.row()) as i32,
        method,
        keep_endpoints,
        budget,
    )
}

fn smooth_for_domain(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    iterations: i32,
    method: SmoothMethod,
    keep_endpoints: bool,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    match ctx.domain {
        TopologyDomain::Cartesian => {
            data.shape()
                .smooth_budgeted(iterations, method, keep_endpoints, budget)
        },
        TopologyDomain::Geographic(frame) => {
            let (output, preserved_vertices) = data.shape().smooth_budgeted_with_vertex_intents(
                iterations,
                method,
                keep_endpoints,
                budget,
            )?;
            crate::geometry::publish_changed_raw_computed_shape(
                data.shape(),
                &output,
                frame,
                &preserved_vertices,
                budget,
            )
        },
    }
}

/// Resolve segmentize's route option against its CRS-aware metric.
pub(crate) fn segmentize_placement(
    metric: Option<&ResolvedMetric>,
    path: Option<crate::NavigationPath>,
) -> Result<(crate::geometry::SegmentPlacementOwned, f64)> {
    Ok(match metric {
        Some(ResolvedMetric::Geodesic(metric)) => {
            let placement = match path {
                None => {
                    crate::geometry::SegmentPlacementOwned::Chart(*metric.geodesic, metric.frame)
                },
                Some(crate::NavigationPath::Geodesic) => {
                    crate::geometry::SegmentPlacementOwned::Geodesic(*metric.geodesic, metric.frame)
                },
                Some(crate::NavigationPath::Rhumb) => {
                    crate::geometry::SegmentPlacementOwned::Rhumb {
                        crs: Arc::clone(&metric.crs),
                        state: Arc::clone(&metric.state),
                        frame: metric.frame,
                    }
                },
            };
            (placement, 1.0)
        },
        Some(ResolvedMetric::Planar { to_metre }) => {
            if matches!(path, Some(crate::NavigationPath::Rhumb)) {
                return Err(segmentize_rhumb_crs_error());
            }
            (
                crate::geometry::SegmentPlacementOwned::Planar,
                to_metre.get(),
            )
        },
        None => {
            if matches!(path, Some(crate::NavigationPath::Rhumb)) {
                return Err(segmentize_rhumb_crs_error());
            }
            (crate::geometry::SegmentPlacementOwned::Planar, 1.0)
        },
    })
}

fn segmentize_rhumb_crs_error() -> crate::error::Error {
    crate::crs::CrsError::message(
        "segmentize path='rhumb' requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one",
    )
}

/// Segmentize kernel shared by the spine-routed free function.
pub(crate) fn unary_segmentize(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    max_segment_length: &F64Param,
    path: Option<crate::NavigationPath>,
) -> Result<Shape> {
    let metric = ctx.metric.require_resolved(MetricOp::Segmentize)?;
    let mut budget = crate::geometry::ExpansionBudget::new("segmentize", "max_length");
    let length = max_segment_length.get(ctx.lane.row());
    let (placement, to_metre) = segmentize_placement(Some(metric), path)?;
    if let Some(frame) = placement.frame() {
        crate::crs::ensure_geographic_domain_in_frame(frame, data.shape())?;
    }
    data.shape()
        .segmentize_budgeted(length / to_metre, &placement.as_borrowed(), &mut budget)
}

/// Densify kernel shared by the spine-routed free function.
pub(crate) fn unary_densify(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    fraction: &F64Param,
    path: Option<crate::NavigationPath>,
) -> Result<Shape> {
    let fraction = fraction.get(ctx.lane.row());
    let metric = ctx.metric.require_resolved(MetricOp::Segmentize)?;
    let (placement, _) = segmentize_placement(Some(metric), path)?;
    if let Some(frame) = placement.frame() {
        crate::crs::ensure_geographic_domain_in_frame(frame, data.shape())?;
    }
    data.shape().densified_budgeted_with_placement(
        fraction,
        &placement.as_borrowed(),
        &mut crate::geometry::ExpansionBudget::new("segmentize", "fraction"),
    )
}

/// Area kernel shared by the spine-routed free function.
///
/// Polygonal operands are measured as stored: the ring sum is exact for a
/// valid polygon, and repairing an invalid one is the caller's decision. The
/// members of a valid geometry collection may overlap, so a collection
/// measures its occupied support once.
pub(crate) fn unary_area(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<f64> {
    let metric = ctx.metric.require_resolved(MetricOp::Area)?;
    let shape = data.shape();
    let collection = matches!(shape, Shape::GeometryCollection(_));
    match metric {
        ResolvedMetric::Planar { to_metre } => {
            let area = match ctx.domain {
                crate::geometry::TopologyDomain::Cartesian if collection => {
                    crate::geometry::occupied_cartesian_area(shape)?
                },
                domain @ crate::geometry::TopologyDomain::Geographic(_) if collection => {
                    crate::geometry::occupied_support_shape(shape, domain)?.area()
                },
                crate::geometry::TopologyDomain::Cartesian
                | crate::geometry::TopologyDomain::Geographic(_) => shape.area(),
            };
            Ok(scale_area(area, to_metre.get()))
        },
        ResolvedMetric::Geodesic(metric) => {
            crate::crs::ensure_geographic_domain_in_frame(metric.frame, shape)?;
            if collection {
                let support = crate::geometry::occupied_support_shape(shape, ctx.domain)?;
                crate::crs::chart_shape_area(metric, &support, metric.frame)
            } else {
                crate::crs::chart_shape_area(metric, shape, metric.frame)
            }
        },
    }
}

/// Length kernel shared by the spine-routed free function.
pub(crate) fn unary_length(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<f64> {
    let metric = ctx.metric.require_resolved(MetricOp::Length)?;
    match metric {
        ResolvedMetric::Planar { to_metre } => {
            Ok(scale_length(data.shape().length(), to_metre.get()))
        },
        ResolvedMetric::Geodesic(metric) => {
            crate::crs::ensure_geographic_domain_in_frame(metric.frame, data.shape())?;
            crate::crs::chart_shape_length(
                metric,
                data.shape(),
                data.shape_data().existing_bounds(),
            )
        },
    }
}

/// 3D length kernel shared by the spine-routed free function.
pub(crate) fn unary_length_3d(data: &PreparedPlan, ctx: &CachelessRowContext<'_>) -> Result<f64> {
    let to_metre = ctx
        .metric
        .require_model(MetricOp::Length3d)?
        .coordinate_scale();
    if data.shape().is_empty() || data.shape().topological_dimension() == Dimension::Point {
        return Ok(0.0);
    }
    match data.shape().length_3d() {
        Ok(value) => Ok(scale_length(value, to_metre)),
        Err(_) if ctx.lane.is_array() => Ok(f64::NAN),
        Err(error) => Err(error),
    }
}

/// Affine-transform kernel shared by the spine-routed free function.
pub(crate) fn unary_affine_transform(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    matrix: &[f64; 6],
) -> Result<Shape> {
    data.shape().affine_transform(matrix)
}

/// Translate kernel shared by the spine-routed free function.
pub(crate) fn unary_translate(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    xoff: f64,
    yoff: f64,
) -> Result<Shape> {
    data.shape().translate(xoff, yoff)
}

/// Rotate kernel shared by the spine-routed free function.
pub(crate) fn unary_rotate(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    angle: f64,
    origin: OriginSpec,
) -> Result<Shape> {
    data.shape()
        .rotate(angle, origin.resolve_shape(data.shape())?)
}

/// Scale kernel shared by the spine-routed free function.
pub(crate) fn unary_scale(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    xfact: f64,
    yfact: f64,
    origin: OriginSpec,
) -> Result<Shape> {
    data.shape()
        .scale(xfact, yfact, origin.resolve_shape(data.shape())?)
}

/// Skew kernel shared by the spine-routed free function.
pub(crate) fn unary_skew(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    x_angle: f64,
    y_angle: f64,
    origin: OriginSpec,
) -> Result<Shape> {
    data.shape()
        .skew(x_angle, y_angle, origin.resolve_shape(data.shape())?)
}

/// Reverse kernel shared by the spine-routed free function.
pub(crate) fn unary_reverse(data: &PreparedPlan, _ctx: &CachelessRowContext<'_>) -> Result<Shape> {
    data.shape().reverse()
}

/// Normalize kernel shared by the spine-routed free function.
pub(crate) fn unary_normalize(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    data.shape().normalize()
}

/// Swap-XY kernel shared by the spine-routed free function.
pub(crate) fn unary_swap_xy(data: &PreparedPlan, _ctx: &CachelessRowContext<'_>) -> Result<Shape> {
    data.shape().swap_xy()
}

/// Orient-polygons kernel shared by the spine-routed free function.
pub(crate) fn unary_orient_polygons(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    ccw: bool,
) -> Result<Shape> {
    data.shape().orient_polygons(!ccw)
}

/// Unique-points kernel shared by the spine-routed free function.
pub(crate) fn unary_unique_points(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    data.shape().unique_points()
}

/// Remove-repeated-points kernel shared by the spine-routed free function.
pub(crate) fn unary_remove_repeated_points(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    tolerance: &F64Param,
) -> Result<Shape> {
    data.shape()
        .remove_repeated_points(tolerance.get(ctx.lane.row()))
}

/// Snap-to-grid kernel shared by the spine-routed free function.
pub(crate) fn unary_snap_to_grid(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    size: (f64, f64),
    origin: (f64, f64),
    repair: bool,
) -> Result<Shape> {
    if repair {
        data.shape().snap_to_grid_repaired(size, origin, ctx.domain)
    } else {
        data.shape().snap_to_grid(size, origin)
    }
}

/// Force-2D kernel shared by the spine-routed free function.
pub(crate) fn unary_force_2d(data: &PreparedPlan, _ctx: &CachelessRowContext<'_>) -> Result<Shape> {
    data.shape().force_2d()
}

/// Force-3D kernel shared by the spine-routed free function.
pub(crate) fn unary_force_3d(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    z: f64,
) -> Result<Shape> {
    data.shape().set_z(Some(z), false)
}

/// Set-Z kernel shared by the spine-routed free function.
pub(crate) fn unary_set_z(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    value: Option<f64>,
    overwrite: bool,
) -> Result<Shape> {
    data.shape().set_z(value, overwrite)
}

/// Set-M kernel shared by the spine-routed free function.
pub(crate) fn unary_set_m(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    value: Option<f64>,
    overwrite: bool,
) -> Result<Shape> {
    data.shape().set_m(value, overwrite)
}

/// Quantize kernel shared by the spine-routed free function.
pub(crate) fn unary_quantize(
    data: &PreparedPlan,
    _ctx: &CachelessRowContext<'_>,
    precision: i32,
) -> Result<Shape> {
    data.shape().quantize(precision)
}

/// Clip-by-rect kernel shared by the spine-routed free function.
pub(crate) fn unary_clip_by_rect(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    rect: Bounds,
    drop: bool,
) -> Result<Shape> {
    if let crate::geometry::TopologyDomain::Geographic(frame) = ctx.domain {
        return data
            .shape()
            .clip_by_rect_in_geographic_frame(rect, drop, frame);
    }
    data.clip_by_rect_with_bounds(rect, drop, data.bounds())
}

/// Bounds kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Option<Bounds>>"
)]
pub(crate) fn unary_bounds(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<Option<Bounds>> {
    match ctx.domain {
        crate::geometry::TopologyDomain::Cartesian => Ok(data.bounds()),
        crate::geometry::TopologyDomain::Geographic(frame) => {
            let bounds = data.bounds();
            if bounds.is_some_and(|bounds| frame.bounds_certify_ordinary_support(bounds)) {
                return Ok(bounds);
            }
            Ok(
                crate::geographic::GeographicSupportEnvelope::try_from_shape(frame, data.shape())
                    .ok()
                    .and_then(|support| support.bounds(frame)),
            )
        },
    }
}

/// Minimum-clearance kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_clearance(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<f64> {
    let model = ctx.metric.require_model(MetricOp::MinimumClearance)?;
    metric_minimum_clearance(model, ctx.frame.crs_str(), data.shape())
}

/// Minimum-clearance witness kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_clearance_line(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::MinimumClearanceLine)?;
    metric_minimum_clearance_line(model, ctx.frame.crs_str(), data.shape())
}

/// Distance kernel shared by the spine-routed free function.
pub(crate) fn binary_distance(
    left: &PreparedPlan,
    right: &PreparedPlan,
    ctx: &GeodesicBinaryRow<'_>,
) -> Result<f64> {
    crate::broadcast::pair_distance_geodesic(
        ctx.metric,
        left,
        ctx.left_cache,
        right,
        ctx.right_cache,
    )
}

/// ``dwithin`` kernel shared by the spine-routed free function.
pub(crate) fn binary_dwithin(
    left: &PreparedPlan,
    right: &PreparedPlan,
    ctx: &GeodesicBinaryRow<'_>,
    distance: f64,
) -> Result<bool> {
    crate::broadcast::pair_dwithin_geodesic(
        ctx.metric,
        left,
        ctx.left_cache,
        right,
        ctx.right_cache,
        distance,
    )
}

/// 3D distance kernel shared by the spine-routed free function.
pub(crate) fn binary_distance_3d(
    left: &PreparedPlan,
    right: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
) -> Result<f64> {
    let to_metre = ctx
        .metric
        .require_model(MetricOp::Distance3d)?
        .coordinate_scale();
    if left.axes_all_z() && right.axes_all_z() {
        return Ok(scale_length(
            crate::geometry::distance_3d_with_parts(
                left.distance_3d_parts(),
                right.distance_3d_parts(),
            )?,
            to_metre,
        ));
    }
    if ctx.lane.is_array() {
        return Ok(f64::NAN);
    }
    Ok(scale_length(left.distance_3d(right)?, to_metre))
}

/// Overlay kernel shared by the spine-routed set-op free functions.
pub(crate) fn binary_overlay(
    left: &PreparedPlan,
    right: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    op: crate::OverlayOp,
    strictness: crate::geometry::Strictness,
) -> Result<Shape> {
    op.apply_in_frame(left.shape(), right.shape(), ctx.domain, strictness)
}

/// ``line_interpolate_point`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_interpolate_point(
    data: &PreparedPlan,
    ctx: &LinearReferenceRow<'_>,
    distance: &F64Param,
    normalized: bool,
) -> Result<Shape> {
    degrade_linref_point(
        line_interpolate_shape(
            ctx.model,
            data,
            ctx.cache,
            distance.get(ctx.op.lane.row()),
            normalized,
        ),
        ctx.op.lane,
    )
}

/// ``line_substring`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_substring(
    data: &PreparedPlan,
    ctx: &LinearReferenceRow<'_>,
    start_distance: &F64Param,
    end_distance: &F64Param,
    normalized: bool,
) -> Result<Shape> {
    let range = MeasureRange::substring_distance(
        start_distance.get(ctx.op.lane.row()),
        end_distance.get(ctx.op.lane.row()),
    )?;
    degrade_linref_linestring(
        line_substring_shape(ctx.model, data, ctx.cache, range, normalized),
        ctx.op.lane,
    )
}

/// ``line_interpolate_point_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_interpolate_point_m(
    data: &PreparedPlan,
    ctx: &LinearReferenceRow<'_>,
    m: &F64Param,
) -> Result<Shape> {
    degrade_linref_point(
        line_interpolate_shape_m(ctx.model, data, ctx.cache, m.get(ctx.op.lane.row())),
        ctx.op.lane,
    )
}

/// ``line_substring_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_substring_m(
    data: &PreparedPlan,
    ctx: &LinearReferenceRow<'_>,
    start_m: &F64Param,
    end_m: &F64Param,
) -> Result<Shape> {
    let range = MeasureRange::substring_measure(
        start_m.get(ctx.op.lane.row()),
        end_m.get(ctx.op.lane.row()),
    )?;
    degrade_linref_linestring(
        line_substring_shape_m(ctx.model, data, ctx.cache, range),
        ctx.op.lane,
    )
}

/// ``interpolate_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_interpolate_m(
    data: &PreparedPlan,
    ctx: &CachelessRowContext<'_>,
    start_m: &F64Param,
    end_m: &F64Param,
    overwrite: bool,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::InterpolateM)?;
    let range =
        MeasureRange::interpolate_m(start_m.get(ctx.lane.row()), end_m.get(ctx.lane.row()))?;
    metric_interpolate_m(model, data.shape(), range, overwrite)
}
