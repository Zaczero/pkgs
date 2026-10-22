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
use crate::dispatch::metric::{GeodesicBinaryRow, LinearReferenceRow, MetricRowContext};
use crate::error::Result;
use crate::geographic::PointSelectionChart;
use crate::geographic::chart::{
    point_set_convex_hull_shape, point_set_minimum_rotated_rectangle_shape,
};
use crate::geometry::publication::{
    GeometryOutput, finish_geographic_line, finish_geographic_point_selection,
    geographic_line_pending_row, geographic_point_selection_pending_row,
};
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

fn publish_planar_geographic_edges_with_context(
    output: Shape,
    model: &crate::crs::MetricModel,
    ctx: &MetricRowContext<'_>,
    operation: &'static str,
) -> Result<Shape> {
    match (model, ctx.domain) {
        (crate::crs::MetricModel::Planar { .. }, TopologyDomain::Geographic(frame)) => {
            let metric_access = || {
                ctx.metric
                    .source_chart_metric(ctx.frame.crs_str())?
                    .ok_or_else(|| {
                        crate::crs::CrsError::invalid(
                            "geographic computed-edge source ordinates require the operation's CRS metric",
                        )
                    })
            };
            let source_context =
                crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access));
            crate::geometry::publish_raw_computed_shape_with_context(
                &output,
                frame,
                operation,
                &source_context,
            )
        },
        _ => Ok(output),
    }
}

fn publish_planar_geographic_edges_budgeted_with_context(
    output: Shape,
    model: &crate::crs::MetricModel,
    ctx: &MetricRowContext<'_>,
    budget: &mut crate::geometry::ExpansionBudget,
) -> Result<Shape> {
    match (model, ctx.domain) {
        (crate::crs::MetricModel::Planar { .. }, TopologyDomain::Geographic(frame)) => {
            let metric_access = || {
                ctx.metric
                    .source_chart_metric(ctx.frame.crs_str())?
                    .ok_or_else(|| {
                        crate::crs::CrsError::invalid(
                            "geographic computed-edge source ordinates require the operation's CRS metric",
                        )
                    })
            };
            crate::geometry::publish_raw_computed_shape_with_budget_and_context(
                &output,
                frame,
                &crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access)),
                budget,
            )
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
pub(crate) fn unary_centroid(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<Shape> {
    crate::geometry::unary_antimeridian_centroid(data.shape(), ctx.domain)
}

/// Envelope kernel shared by the spine-routed free function.
pub(crate) fn unary_envelope(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<Shape> {
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
pub(crate) fn unary_convex_hull(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<Shape> {
    let output = convex_hull_shape(data, ctx)?;
    match ctx.domain {
        TopologyDomain::Cartesian => Ok(output),
        TopologyDomain::Geographic(_) => {
            finish_geographic_point_selection(output, ctx.frame.clone())
        },
    }
}

pub(crate) fn unary_convex_hull_array(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<GeometryOutput> {
    let output = convex_hull_shape(data, ctx)?;
    match ctx.domain {
        TopologyDomain::Cartesian => Ok(GeometryOutput::Shape(output)),
        TopologyDomain::Geographic(_) => Ok(GeometryOutput::Pending(
            geographic_point_selection_pending_row(output, ctx.lane.row(), "convex_hull")?,
        )),
    }
}

fn convex_hull_shape(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<Shape> {
    let shape = data.shape();
    ctx.with_source_arc_capture_for_axes(shape.axes(), |source_capture| {
        if !matches!(shape, Shape::Point(_) | Shape::Empty(..))
            && let TopologyDomain::Geographic(frame) = ctx.domain
            && PointSelectionChart::accepts_point_set(shape)
            && let Some(hull) =
                point_set_convex_hull_shape(shape, frame, data.bounds(), source_capture)?
        {
            return Ok(hull);
        }
        publish_raw_computed_if_geographic(shape.convex_hull()?, ctx, "convex_hull", source_capture)
    })
}

/// Simplify kernel shared by the spine-routed free function.
pub(crate) fn unary_simplify(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
    publish_planar_geographic_edges_budgeted_with_context(output, model, ctx, budget)
}

/// Offset-curve kernel shared by the spine-routed free function.
pub(crate) fn unary_offset_curve(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
    publish_planar_geographic_edges_budgeted_with_context(output, model, ctx, budget)
}

/// Point-on-surface kernel shared by the spine-routed free function.
pub(crate) fn unary_point_on_surface(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
    publish_planar_geographic_edges_with_context(output, model, ctx, "concave_hull")
}

/// Polylabel kernel shared by the spine-routed free function.
pub(crate) fn unary_polylabel(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::MinimumBoundingCircle)?;
    let output = metric_minimum_bounding_circle(model, ctx.frame.crs_str(), data.shape())?;
    publish_planar_geographic_edges_with_context(output, model, ctx, "minimum_bounding_circle")
}

/// Minimum-bounding-circle radius kernel shared by scalar and array methods.
pub(crate) fn unary_minimum_bounding_radius(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<f64> {
    let model = ctx.metric.require_model(MetricOp::MinimumBoundingRadius)?;
    metric_minimum_bounding_radius(model, data.shape())
}

/// Minimum-rotated-rectangle kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_rotated_rectangle(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<Shape> {
    let output = minimum_rotated_rectangle_shape(data, ctx)?;
    match ctx.domain {
        TopologyDomain::Cartesian => Ok(output),
        TopologyDomain::Geographic(_) => {
            finish_geographic_point_selection(output, ctx.frame.clone())
        },
    }
}

pub(crate) fn unary_minimum_rotated_rectangle_array(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<GeometryOutput> {
    let output = minimum_rotated_rectangle_shape(data, ctx)?;
    match ctx.domain {
        TopologyDomain::Cartesian => Ok(GeometryOutput::Shape(output)),
        TopologyDomain::Geographic(_) => Ok(GeometryOutput::Pending(
            geographic_point_selection_pending_row(
                output,
                ctx.lane.row(),
                "minimum_rotated_rectangle",
            )?,
        )),
    }
}

fn minimum_rotated_rectangle_shape(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<Shape> {
    let shape = data.shape();
    ctx.with_source_arc_capture_for_axes(shape.axes(), |source_capture| {
        if !matches!(shape, Shape::Point(_) | Shape::Empty(..))
            && let TopologyDomain::Geographic(frame) = ctx.domain
            && PointSelectionChart::accepts_point_set(shape)
            && let Some(rectangle) = point_set_minimum_rotated_rectangle_shape(
                shape,
                frame,
                data.bounds(),
                source_capture,
            )?
        {
            return Ok(rectangle);
        }
        publish_raw_computed_if_geographic(
            shape.minimum_rotated_rectangle()?,
            ctx,
            "minimum_rotated_rectangle",
            source_capture,
        )
    })
}

fn publish_raw_computed_if_geographic(
    shape: Shape,
    ctx: &MetricRowContext<'_>,
    operation: &'static str,
    source_capture: &crate::geometry::SourceArcCapture<'_, '_, crate::crs::GeodesicMetric>,
) -> Result<Shape> {
    match ctx.domain {
        TopologyDomain::Cartesian => Ok(shape),
        TopologyDomain::Geographic(frame) => {
            crate::geometry::publish_raw_computed_shape_with_capture(
                &shape,
                frame,
                operation,
                source_capture,
            )
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
pub(crate) fn unary_boundary(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<Shape> {
    let metric_access = || {
        ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic boundary source ordinates require the operation's CRS metric",
            )
        })
    };
    crate::geometry::boundary_with_metric(
        data,
        &crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access)),
    )
}

pub(crate) fn unary_boundary_array(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<GeometryOutput> {
    let metric_access = || {
        ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic boundary source ordinates require the operation's CRS metric",
            )
        })
    };
    crate::geometry::boundary_array_with_metric(
        data,
        &crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access)),
        ctx.frame,
        ctx.lane.row(),
    )
}

/// ``is_empty`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_empty(data: &PreparedPlan, _ctx: &MetricRowContext<'_>) -> Result<bool> {
    Ok(data.shape().is_empty())
}

/// ``is_closed`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_closed(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<bool> {
    Ok(crate::geometry::shape_is_closed_in_domain(
        data.shape(),
        ctx.domain,
    ))
}

/// ``is_ring`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_ring(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<bool> {
    crate::geometry::is_ring_data_in_frame(data, ctx.domain)
}

/// ``is_ccw`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_ccw(data: &PreparedPlan, _ctx: &MetricRowContext<'_>) -> Result<bool> {
    Ok(data.shape().is_ccw())
}

/// ``is_simple`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_simple(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<bool> {
    crate::geometry::is_simple_data_in_frame(data, ctx.domain)
}

/// ``is_valid`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_valid(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<bool> {
    Ok(crate::geometry::validate_data_in_frame(data, ctx.domain)?.is_none())
}

/// ``is_convex`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_convex(data: &PreparedPlan, _ctx: &MetricRowContext<'_>) -> Result<bool> {
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
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
) -> Result<Shape> {
    let frame = crate::geometry::antimeridian_frame(ctx.frame, ctx.domain, "split_antimeridian")?;
    if ctx.frame.crs_ref().is_none() {
        return data.shape().split_antimeridian_in_crs84();
    }
    let metric_access = || {
        ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic antimeridian source ordinates require the operation's CRS metric",
            )
        })
    };
    data.shape()
        .split_antimeridian_in_frame_with_metric(frame, Some(&metric_access))
}

/// ``node`` kernel shared by the spine-routed free function.
pub(crate) fn unary_node(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<Shape> {
    let metric_access = || {
        ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic node source ordinates require the operation's CRS metric",
            )
        })
    };
    let source_context = crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access));
    data.shape().node_with_publication_frame(
        Strictness::Lenient,
        &source_context,
        ctx.frame.clone(),
    )
}

pub(crate) fn unary_node_array(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<GeometryOutput> {
    let metric_access = || {
        ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic node source ordinates require the operation's CRS metric",
            )
        })
    };
    let source_context = crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access));
    if ctx.domain.is_geographic() {
        return Ok(GeometryOutput::Pending(
            crate::geometry::geographic_node_pending_row(
                data.shape(),
                &source_context,
                ctx.frame,
                ctx.lane.row(),
            )?,
        ));
    }
    Ok(GeometryOutput::Shape(
        data.shape().node_with_publication_frame(
            Strictness::Lenient,
            &source_context,
            ctx.frame.clone(),
        )?,
    ))
}

/// Build-area kernel shared by the scalar and array unary method spine.
pub(crate) fn unary_build_area(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<Shape> {
    let metric_access = || {
        ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic build_area source ordinates require the operation's CRS metric",
            )
        })
    };
    let source_context = crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access));
    if ctx.domain.is_geographic() {
        return crate::geometry::geographic_build_area_with_publication_frame(
            data.shape(),
            false,
            &source_context,
            ctx.frame.clone(),
        );
    }
    data.shape().build_area_with_context(false, &source_context)
}

pub(crate) fn unary_build_area_array(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<GeometryOutput> {
    let metric_access = || {
        ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
            crate::crs::CrsError::invalid(
                "geographic build_area source ordinates require the operation's CRS metric",
            )
        })
    };
    let source_context = crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access));
    if ctx.domain.is_geographic() {
        return Ok(GeometryOutput::Pending(
            crate::geometry::geographic_build_area_pending_row(
                data.shape(),
                &source_context,
                ctx.frame,
                ctx.lane.row(),
            )?,
        ));
    }
    Ok(GeometryOutput::Shape(
        data.shape()
            .build_area_with_context(false, &source_context)?,
    ))
}

/// Smooth kernel shared by the spine-routed free function.
pub(crate) fn unary_smooth(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
            let metric_access = || {
                ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic computed-edge source ordinates require the operation's CRS metric",
                    )
                })
            };
            let source_context =
                crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access));
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
                &source_context,
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
        "segmentize path='rhumb' requires a geographic CRS; use with_crs(...) or to_crs(...) to attach one",
    )
}

/// Segmentize kernel shared by the spine-routed free function.
pub(crate) fn unary_segmentize(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
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
    ctx: &MetricRowContext<'_>,
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
pub(crate) fn unary_area(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<f64> {
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
pub(crate) fn unary_length(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<f64> {
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
pub(crate) fn unary_length_3d(data: &PreparedPlan, ctx: &MetricRowContext<'_>) -> Result<f64> {
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
    _ctx: &MetricRowContext<'_>,
    matrix: &[f64; 6],
) -> Result<Shape> {
    data.shape().affine_transform(matrix)
}

/// Translate kernel shared by the spine-routed free function.
pub(crate) fn unary_translate(
    data: &PreparedPlan,
    _ctx: &MetricRowContext<'_>,
    xoff: f64,
    yoff: f64,
) -> Result<Shape> {
    data.shape().translate(xoff, yoff)
}

/// Rotate kernel shared by the spine-routed free function.
pub(crate) fn unary_rotate(
    data: &PreparedPlan,
    _ctx: &MetricRowContext<'_>,
    angle: f64,
    origin: OriginSpec,
) -> Result<Shape> {
    data.shape()
        .rotate(angle, origin.resolve_shape(data.shape())?)
}

/// Scale kernel shared by the spine-routed free function.
pub(crate) fn unary_scale(
    data: &PreparedPlan,
    _ctx: &MetricRowContext<'_>,
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
    _ctx: &MetricRowContext<'_>,
    x_angle: f64,
    y_angle: f64,
    origin: OriginSpec,
) -> Result<Shape> {
    data.shape()
        .skew(x_angle, y_angle, origin.resolve_shape(data.shape())?)
}

/// Reverse kernel shared by the spine-routed free function.
pub(crate) fn unary_reverse(data: &PreparedPlan, _ctx: &MetricRowContext<'_>) -> Result<Shape> {
    data.shape().reverse()
}

/// Normalize kernel shared by the spine-routed free function.
pub(crate) fn unary_normalize(data: &PreparedPlan, _ctx: &MetricRowContext<'_>) -> Result<Shape> {
    data.shape().normalize()
}

/// Swap-XY kernel shared by the spine-routed free function.
pub(crate) fn unary_swap_xy(data: &PreparedPlan, _ctx: &MetricRowContext<'_>) -> Result<Shape> {
    data.shape().swap_xy()
}

/// Orient-polygons kernel shared by the spine-routed free function.
pub(crate) fn unary_orient_polygons(
    data: &PreparedPlan,
    _ctx: &MetricRowContext<'_>,
    ccw: bool,
) -> Result<Shape> {
    data.shape().orient_polygons(!ccw)
}

/// Unique-points kernel shared by the spine-routed free function.
pub(crate) fn unary_unique_points(
    data: &PreparedPlan,
    _ctx: &MetricRowContext<'_>,
) -> Result<Shape> {
    data.shape().unique_points()
}

/// Remove-repeated-points kernel shared by the spine-routed free function.
pub(crate) fn unary_remove_repeated_points(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
    tolerance: &F64Param,
) -> Result<Shape> {
    data.shape()
        .remove_repeated_points(tolerance.get(ctx.lane.row()))
}

/// Snap-to-grid kernel shared by the spine-routed free function.
pub(crate) fn unary_snap_to_grid(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
    size: (f64, f64),
    origin: (f64, f64),
    repair: bool,
) -> Result<Shape> {
    if repair {
        let metric_access = || {
            ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic snap_to_grid repair requires the operation's CRS metric",
                )
            })
        };
        let source_context =
            crate::geometry::SourceArcContext::new(ctx.domain, Some(&metric_access));
        data.shape()
            .snap_to_grid_repaired(size, origin, ctx.domain, &source_context)
    } else {
        data.shape().snap_to_grid(size, origin)
    }
}

/// Force-2D kernel shared by the spine-routed free function.
pub(crate) fn unary_force_2d(data: &PreparedPlan, _ctx: &MetricRowContext<'_>) -> Result<Shape> {
    data.shape().force_2d()
}

/// Force-3D kernel shared by the spine-routed free function.
pub(crate) fn unary_force_3d(
    data: &PreparedPlan,
    _ctx: &MetricRowContext<'_>,
    z: f64,
) -> Result<Shape> {
    data.shape().replace_z(Some(z), false)
}

/// Quantize kernel shared by the spine-routed free function.
pub(crate) fn unary_quantize(
    data: &PreparedPlan,
    _ctx: &MetricRowContext<'_>,
    precision: i32,
) -> Result<Shape> {
    data.shape().quantize(precision)
}

/// Clip-by-rect kernel shared by the spine-routed free function.
pub(crate) fn unary_clip_by_rect(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
    rect: Bounds,
    drop: bool,
) -> Result<Shape> {
    if let crate::geometry::TopologyDomain::Geographic(frame) = ctx.domain {
        let metric_access = || {
            ctx.metric
                .geographic_chart_metric()?
                .ok_or_else(|| crate::crs::CrsError::invalid(
                    "geographic rectangle clipping requires the operation's CRS metric for source ordinates",
                ))
        };
        return data.shape().clip_by_rect_in_geographic_frame(
            rect,
            drop,
            frame,
            ctx.frame,
            &metric_access,
        );
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
    ctx: &MetricRowContext<'_>,
) -> Result<Option<Bounds>> {
    match ctx.domain {
        crate::geometry::TopologyDomain::Cartesian => Ok(data.bounds()),
        crate::geometry::TopologyDomain::Geographic(frame) => Ok(
            crate::geographic::GeographicSupportProducts::public_bounds_for_shape(
                frame,
                data.shape(),
                data.bounds(),
            ),
        ),
    }
}

/// Minimum-clearance kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_clearance(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<f64> {
    let model = ctx.metric.require_model(MetricOp::MinimumClearance)?;
    metric_minimum_clearance(model, ctx.frame.crs_str(), data.shape())
}

/// Minimum-clearance witness kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_clearance_line(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::MinimumClearanceLine)?;
    let output = metric_minimum_clearance_line(model, ctx.frame.crs_str(), data.shape())?;
    match ctx.domain {
        TopologyDomain::Cartesian => Ok(output),
        TopologyDomain::Geographic(_) => finish_geographic_line(output, ctx.frame.clone()),
    }
}

pub(crate) fn unary_minimum_clearance_line_array(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
) -> Result<GeometryOutput> {
    let model = ctx.metric.require_model(MetricOp::MinimumClearanceLine)?;
    let output = metric_minimum_clearance_line(model, ctx.frame.crs_str(), data.shape())?;
    match ctx.domain {
        TopologyDomain::Cartesian => Ok(GeometryOutput::Shape(output)),
        TopologyDomain::Geographic(_) => Ok(GeometryOutput::Pending(geographic_line_pending_row(
            output,
            ctx.lane.row(),
            "minimum_clearance_line",
        )?)),
    }
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
    ctx: &MetricRowContext<'_>,
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
    Ok(scale_length(left.distance_3d(right)?, to_metre))
}

/// Overlay kernel shared by the spine-routed set-op free functions.
pub(crate) fn binary_overlay(
    left: &PreparedPlan,
    right: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
    op: crate::OverlayOp,
    strictness: crate::geometry::Strictness,
) -> Result<GeometryOutput> {
    match ctx.domain {
        TopologyDomain::Cartesian => Ok(GeometryOutput::Shape(op.apply(
            left.shape(),
            right.shape(),
            strictness,
        )?)),
        TopologyDomain::Geographic(frame) => {
            let metric_access = || {
                ctx.metric.geographic_chart_metric()?.ok_or_else(|| {
                    crate::crs::CrsError::invalid(
                        "geographic overlay source ordinates require the operation's CRS metric",
                    )
                })
            };
            if ctx.lane.is_array() {
                crate::geometry::overlay_all_geographic_output_with_metric_and_frame(
                    &[left.shape(), right.shape()],
                    frame,
                    ctx.frame,
                    op,
                    strictness,
                    &metric_access,
                    ctx.lane.row(),
                )
            } else {
                Ok(GeometryOutput::Shape(
                    crate::geometry::overlay_all_geographic_with_metric_and_frame(
                        &[left.shape(), right.shape()],
                        frame,
                        ctx.frame,
                        op,
                        strictness,
                        &metric_access,
                    )?,
                ))
            }
        },
    }
}

/// ``line_interpolate_point`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_interpolate_point(
    data: &PreparedPlan,
    ctx: &LinearReferenceRow<'_>,
    distance: &F64Param,
    normalized: bool,
) -> Result<Shape> {
    degrade_linref_point(line_interpolate_shape(
        ctx.metric,
        data,
        ctx.cache,
        distance.get(ctx.lane.row()),
        normalized,
    ))
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
        start_distance.get(ctx.lane.row()),
        end_distance.get(ctx.lane.row()),
    )?;
    degrade_linref_linestring(line_substring_shape(
        ctx.metric, data, ctx.cache, range, normalized,
    ))
}

/// ``line_interpolate_point_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_interpolate_point_m(
    data: &PreparedPlan,
    ctx: &LinearReferenceRow<'_>,
    m: &F64Param,
) -> Result<Shape> {
    degrade_linref_point(line_interpolate_shape_m(
        ctx.metric,
        data,
        ctx.cache,
        m.get(ctx.lane.row()),
    ))
}

/// ``line_substring_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_substring_m(
    data: &PreparedPlan,
    ctx: &LinearReferenceRow<'_>,
    start_m: &F64Param,
    end_m: &F64Param,
) -> Result<Shape> {
    let range =
        MeasureRange::substring_measure(start_m.get(ctx.lane.row()), end_m.get(ctx.lane.row()))?;
    degrade_linref_linestring(line_substring_shape_m(ctx.metric, data, ctx.cache, range))
}

/// ``interpolate_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_interpolate_m(
    data: &PreparedPlan,
    ctx: &MetricRowContext<'_>,
    start_m: &F64Param,
    end_m: &F64Param,
    overwrite: bool,
) -> Result<Shape> {
    let model = ctx.metric.require_model(MetricOp::InterpolateM)?;
    let range =
        MeasureRange::interpolate_m(start_m.get(ctx.lane.row()), end_m.get(ctx.lane.row()))?;
    metric_interpolate_m(model, data.shape(), range, overwrite)
}

#[cfg(test)]
mod root_capture_tests {
    use super::{unary_convex_hull, unary_minimum_rotated_rectangle};
    use crate::crs::frame::Frame;
    use crate::dispatch::metric::{Lane, MetricResolver, MetricRowContext, MetricScratch};
    use crate::geometry::{
        CoordSeq, CoordinateAxes, LineSeq, Point, Polygon, PreparedPlan, Ring, RootWorkCounters,
        Shape, reset_root_work_counters, root_work_counters,
    };

    fn line(points: &[Point]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(points).expect("line coordinates"))
                .expect("valid line"),
        )
    }

    fn polygon(points: &[Point]) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(points).expect("polygon coordinates"))
                .expect("closed ring"),
            Vec::new(),
        ))
    }

    fn points(points: &[Point]) -> Shape {
        Shape::multi_point(
            CoordinateAxes::XY,
            CoordSeq::try_from_points(points).expect("multipoint coordinates"),
        )
    }

    #[rstest::rstest]
    #[case::line(line(&[Point::new_raw_xy(0.0, 1.0), Point::new_raw_xy(360.0, 60.0)]))]
    #[case::polygon(polygon(&[
        Point::new_raw_xy(-10.0, -5.0),
        Point::new_raw_xy(10.0, -5.0),
        Point::new_raw_xy(10.0, 5.0),
        Point::new_raw_xy(-10.0, 5.0),
        Point::new_raw_xy(-10.0, -5.0),
    ]))]
    #[case::multipoint(points(&[
        Point::new_raw_xy(170.0, 0.0),
        Point::new_raw_xy(-170.0, 0.0),
        Point::new_raw_xy(180.0, 10.0),
    ]))]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn geographic_xy_hull_and_rectangle_kernels_skip_root_capture(#[case] input: Shape) {
        let _guard = crate::crs::RUNTIME_STATE_TEST_LOCK.lock().unwrap();
        let frame = Frame::Crs("OGC:CRS84".into());
        let mut scratch = MetricScratch::default();
        let (domain, metric) = MetricResolver::GeographicCarry
            .resolve_topology_ctx(&frame, &mut scratch)
            .expect("geographic topology context");
        let context = MetricRowContext {
            frame: &frame,
            domain,
            metric,
            lane: Lane::Scalar,
            frame_caches: None,
        };
        let data = PreparedPlan::new(input);

        reset_root_work_counters();
        let hull = unary_convex_hull(&data, &context).expect("geographic XY hull");
        assert!(!hull.is_empty());
        assert_eq!(hull.axes(), CoordinateAxes::XY);
        assert_eq!(root_work_counters(), RootWorkCounters::default());

        reset_root_work_counters();
        let rectangle =
            unary_minimum_rotated_rectangle(&data, &context).expect("geographic XY MRR");
        assert!(!rectangle.is_empty());
        assert_eq!(rectangle.axes(), CoordinateAxes::XY);
        assert_eq!(root_work_counters(), RootWorkCounters::default());
    }
}
