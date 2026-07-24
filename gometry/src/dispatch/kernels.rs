use std::num::NonZeroU32;

use super::metric::{MetricCtx, OpCtx};
use crate::boundary::geographic::invalid_lonlat_error;
use crate::boundary::input::OriginSpec;
use crate::broadcast::{degrade_linref_linestring, degrade_linref_point};
use crate::crs::{CrsError, MetricModel, ResolvedMetric};
use crate::error::Result;
use crate::geometry::{
    Bounds, BufferCapStyle, BufferJoinStyle, BufferSide, Dimension, EmptyKind, MeasureRange, Shape,
    ShapeData, SimplifyMethod, SmoothMethod, Strictness, same_topological_coordinate,
};
use crate::numeric::Positive;
use crate::{
    F64Param, I64Param, line_interpolate_shape, line_substring_shape, metric_concave_hull,
    metric_constructive_shape, metric_frechet_densified, metric_hausdorff_densified,
    metric_interpolate_m, metric_maximum_inscribed_radius, metric_minimum_bounding_circle,
    metric_minimum_bounding_radius, metric_minimum_clearance, metric_minimum_clearance_line,
    metric_optional_constructive_shape, pair_distance_resolved_result,
    pair_dwithin_resolved_result,
};

fn scale_length(value: f64, to_metre: f64) -> f64 {
    value * to_metre
}

fn scale_area(value: f64, to_metre: f64) -> f64 {
    value * to_metre * to_metre
}

/// Centroid kernel shared by the spine-routed free function.
pub(crate) fn unary_centroid(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<Shape> {
    crate::geometry::unary_antimeridian_derived(
        data.shape(),
        ctx.geographic,
        false,
        Shape::centroid,
    )
}

/// Envelope kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "uniform dispatch kernels return Result<Shape>"
)]
pub(crate) fn unary_envelope(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    Ok(data.shape().envelope())
}

/// Convex-hull kernel shared by the spine-routed free function.
pub(crate) fn unary_convex_hull(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    data.shape().convex_hull()
}

/// Simplify kernel shared by the spine-routed free function.
pub(crate) fn unary_simplify(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    tolerance: &F64Param,
    method: SimplifyMethod,
    preserve_topology: bool,
) -> Result<Shape> {
    let tolerance = tolerance.get(ctx.lane.row());
    match method {
        SimplifyMethod::Vw => data.shape().simplify_vw(tolerance, preserve_topology),
        SimplifyMethod::Dp => data.shape().simplify_dp(tolerance, preserve_topology),
    }
}

/// Buffer kernel shared by the spine-routed free function.
pub(crate) fn unary_buffer(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    distance: &F64Param,
    cap_style: BufferCapStyle,
    join_style: BufferJoinStyle,
    quadrant_segments: NonZeroU32,
    miter_limit: Positive,
    side: BufferSide,
) -> Result<Shape> {
    let distance = distance.get(ctx.lane.row());
    if same_topological_coordinate(distance, 0.0) {
        return match side {
            BufferSide::Both => data.shape().buffer_with_style(
                distance,
                cap_style,
                join_style,
                quadrant_segments,
                miter_limit,
            ),
            side => data.shape().buffer_sided(distance, side),
        };
    }
    let model = ctx.metric.require_model("buffer")?;
    metric_constructive_shape(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        distance,
        |shape, d| match side {
            BufferSide::Both => {
                shape.buffer_with_style(d, cap_style, join_style, quadrant_segments, miter_limit)
            },
            side => shape.buffer_sided(d, side),
        },
    )
}

/// Offset-curve kernel shared by the spine-routed free function.
pub(crate) fn unary_offset_curve(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    distance: &F64Param,
    join_style: BufferJoinStyle,
    quadrant_segments: NonZeroU32,
    miter_limit: Positive,
) -> Result<Shape> {
    let distance = distance.get(ctx.lane.row());
    if same_topological_coordinate(distance, 0.0) {
        return data
            .shape()
            .offset_curve(distance, join_style, quadrant_segments, miter_limit);
    }
    let model = ctx.metric.require_model("offset_curve")?;
    metric_constructive_shape(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        distance,
        |shape, d| shape.offset_curve(d, join_style, quadrant_segments, miter_limit),
    )
}

/// Point-on-surface kernel shared by the spine-routed free function.
pub(crate) fn unary_point_on_surface(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<Shape> {
    crate::geometry::unary_antimeridian_derived(
        data.shape(),
        ctx.geographic,
        true,
        Shape::point_on_surface,
    )
}

/// Concave-hull kernel shared by the spine-routed free function.
pub(crate) fn unary_concave_hull(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    concavity: &F64Param,
    length_threshold: &F64Param,
) -> Result<Shape> {
    let model = ctx.metric.require_model("concave_hull")?;
    let concavity = concavity.get(ctx.lane.row());
    metric_concave_hull(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        concavity,
        length_threshold.get(ctx.lane.row()),
    )
}

/// Polylabel kernel shared by the spine-routed free function.
pub(crate) fn unary_polylabel(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    tolerance: Option<&F64Param>,
) -> Result<Shape> {
    let model = ctx.metric.require_model("polylabel")?;
    metric_optional_constructive_shape(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        tolerance.map(|value| value.get(ctx.lane.row())),
        Shape::polylabel,
    )
}

/// Maximum-inscribed-circle kernel shared by the spine-routed free function.
pub(crate) fn unary_maximum_inscribed_circle(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    tolerance: Option<&F64Param>,
) -> Result<Shape> {
    let model = ctx.metric.require_model("maximum_inscribed_circle")?;
    metric_optional_constructive_shape(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        tolerance.map(|value| value.get(ctx.lane.row())),
        Shape::maximum_inscribed_circle,
    )
}

/// Maximum-inscribed-radius kernel — CRS-aware, the metric twin of the inscribed
/// circle (so the radius matches the disk, and the center is `polylabel`).
pub(crate) fn unary_maximum_inscribed_radius(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    tolerance: Option<&F64Param>,
) -> Result<f64> {
    // geometry — there are no output vertices onto which Z/M could be carried,
    let model = ctx.metric.require_model("maximum_inscribed_radius")?;
    metric_maximum_inscribed_radius(
        model,
        ctx.frame.crs_str(),
        data.shape(),
        tolerance.map(|value| value.get(ctx.lane.row())),
    )
}

/// Minimum-bounding-circle kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_bounding_circle(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<Shape> {
    let model = ctx.metric.require_model("minimum_bounding_circle")?;
    metric_minimum_bounding_circle(model, ctx.frame.crs_str(), data.shape())
}

/// Minimum-bounding-circle radius kernel shared by scalar and array methods.
pub(crate) fn unary_minimum_bounding_radius(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<f64> {
    let model = ctx.metric.require_model("minimum_bounding_radius")?;
    metric_minimum_bounding_radius(model, data.shape())
}

/// Minimum-rotated-rectangle kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_rotated_rectangle(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    data.shape().minimum_rotated_rectangle()
}

/// Boundary kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Shape>"
)]
pub(crate) fn unary_boundary(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    Ok(data.shape().boundary())
}

fn ensure_antimeridian_geographic_frame(ctx: &OpCtx<'_>, op: &str) -> Result<()> {
    let model = match ctx.metric {
        MetricCtx::Antimeridian(model) | MetricCtx::Metric { model, .. } => model,
        MetricCtx::None => return Ok(()),
    };
    if ctx.frame.crs_ref().is_some() && matches!(model, MetricModel::Planar { .. }) {
        return Err(CrsError::invalid(format!(
            "{op} requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one"
        )));
    }
    Ok(())
}

fn validate_split_domain_shape(shape: &Shape) -> Result<()> {
    let mut error = None;
    shape.any_point(|point| {
        let in_domain = (-180.0..=180.0).contains(&point.x) && (-90.0..=90.0).contains(&point.y);
        if in_domain {
            false
        } else {
            error = Some(invalid_lonlat_error(point.x, point.y));
            true
        }
    });
    error.map_or(Ok(()), Err)
}

/// ``is_empty`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_empty(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<bool> {
    Ok(data.shape().is_empty())
}

/// ``is_closed`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_closed(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<bool> {
    Ok(data.shape().is_closed())
}

/// ``is_ring`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_ring(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<bool> {
    Ok(crate::geometry::is_ring_data_in_frame(data, ctx.geographic))
}

/// ``is_ccw`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_ccw(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<bool> {
    Ok(data.shape().is_ccw())
}

/// ``is_simple`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_simple(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<bool> {
    Ok(crate::geometry::is_simple_data_in_frame(
        data,
        ctx.geographic,
    ))
}

/// ``is_valid`` kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<bool>"
)]
pub(crate) fn unary_is_valid(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<bool> {
    Ok(crate::geometry::validate_data_in_frame(data, ctx.geographic).is_none())
}

/// ``is_convex`` kernel shared by the spine-routed free function.
pub(crate) fn unary_is_convex(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<bool> {
    Ok(match data.shape() {
        Shape::Polygon(_) | Shape::Empty(EmptyKind::Polygon, _) => data.shape().is_convex()?,
        _ => false,
    })
}

/// ``crosses_antimeridian`` kernel shared by the spine-routed free function.
pub(crate) fn unary_crosses_antimeridian(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<bool> {
    ensure_antimeridian_geographic_frame(ctx, "crosses_antimeridian")?;
    Ok(data.shape().crosses_antimeridian())
}

/// ``split_antimeridian`` kernel shared by the spine-routed free function.
pub(crate) fn unary_split_antimeridian(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<Shape> {
    ensure_antimeridian_geographic_frame(ctx, "split_antimeridian")?;
    validate_split_domain_shape(data.shape())?;
    data.shape().split_antimeridian()
}

/// ``node`` kernel shared by the spine-routed free function.
pub(crate) fn unary_node(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    data.shape().node(Strictness::Lenient)
}

/// Build-area kernel shared by the spine-routed free function.
pub(crate) fn unary_build_area(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    data.shape().build_area(false)
}

/// Smooth kernel shared by the spine-routed free function.
pub(crate) fn unary_smooth(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    iterations: &I64Param,
    method: SmoothMethod,
    keep_endpoints: bool,
) -> Result<Shape> {
    data.shape().smooth(
        iterations.get(ctx.lane.row()) as i32,
        method,
        keep_endpoints,
    )
}

/// Segmentize kernel shared by the spine-routed free function.
pub(crate) fn unary_segmentize(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    max_segment_length: &F64Param,
) -> Result<Shape> {
    data.shape()
        .segmentize(max_segment_length.get(ctx.lane.row()))
}

/// Densify kernel shared by the spine-routed free function.
pub(crate) fn unary_densify(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    fraction: &F64Param,
) -> Result<Shape> {
    data.shape().densified(fraction.get(ctx.lane.row()))
}

/// Area kernel shared by the spine-routed free function.
pub(crate) fn unary_area(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<f64> {
    let metric = ctx.metric.require_resolved("area")?;
    match metric {
        ResolvedMetric::Planar { to_metre } => Ok(scale_area(data.shape().area(), to_metre.get())),
        ResolvedMetric::Geodesic { geodesic, .. } => {
            crate::crs::ensure_geographic_domain(data.shape())?;
            Ok(crate::crs::geodesic_shape_area(geodesic, data.shape()))
        },
    }
}

/// Length kernel shared by the spine-routed free function.
pub(crate) fn unary_length(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<f64> {
    let metric = ctx.metric.require_resolved("length")?;
    match metric {
        ResolvedMetric::Planar { to_metre } => {
            Ok(scale_length(data.shape().length(), to_metre.get()))
        },
        ResolvedMetric::Geodesic { geodesic, .. } => {
            crate::crs::ensure_geographic_domain(data.shape())?;
            Ok(crate::crs::geodesic_shape_length(geodesic, data.shape()))
        },
    }
}

/// 3D length kernel shared by the spine-routed free function.
pub(crate) fn unary_length_3d(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<f64> {
    let to_metre = crate::crs::ensure_3d_metric(ctx.frame.crs_str())?;
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
    data: &ShapeData,
    _ctx: &OpCtx<'_>,
    matrix: &[f64; 6],
) -> Result<Shape> {
    data.shape().affine_transform(matrix)
}

/// Translate kernel shared by the spine-routed free function.
pub(crate) fn unary_translate(
    data: &ShapeData,
    _ctx: &OpCtx<'_>,
    xoff: f64,
    yoff: f64,
) -> Result<Shape> {
    data.shape().translate(xoff, yoff)
}

/// Rotate kernel shared by the spine-routed free function.
pub(crate) fn unary_rotate(
    data: &ShapeData,
    _ctx: &OpCtx<'_>,
    angle: f64,
    origin: OriginSpec,
) -> Result<Shape> {
    data.shape()
        .rotate(angle, origin.resolve_shape(data.shape())?)
}

/// Scale kernel shared by the spine-routed free function.
pub(crate) fn unary_scale(
    data: &ShapeData,
    _ctx: &OpCtx<'_>,
    xfact: f64,
    yfact: f64,
    origin: OriginSpec,
) -> Result<Shape> {
    data.shape()
        .scale(xfact, yfact, origin.resolve_shape(data.shape())?)
}

/// Reverse kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Shape>"
)]
pub(crate) fn unary_reverse(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    Ok(data.shape().reverse())
}

/// Normalize kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Shape>"
)]
pub(crate) fn unary_normalize(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    Ok(data.shape().normalize())
}

/// Swap-XY kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Shape>"
)]
pub(crate) fn unary_swap_xy(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    Ok(data.shape().swap_xy())
}

/// Orient-polygons kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Shape>"
)]
pub(crate) fn unary_orient_polygons(
    data: &ShapeData,
    _ctx: &OpCtx<'_>,
    ccw: bool,
) -> Result<Shape> {
    Ok(data.shape().orient_polygons(!ccw))
}

/// Unique-points kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Shape>"
)]
pub(crate) fn unary_unique_points(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    Ok(data.shape().unique_points())
}

/// Remove-repeated-points kernel shared by the spine-routed free function.
pub(crate) fn unary_remove_repeated_points(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    tolerance: &F64Param,
) -> Result<Shape> {
    data.shape()
        .remove_repeated_points(tolerance.get(ctx.lane.row()))
}

/// Snap-to-grid kernel shared by the spine-routed free function.
pub(crate) fn unary_snap_to_grid(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    size: (f64, f64),
    origin: (f64, f64),
    repair: bool,
) -> Result<Shape> {
    if repair {
        data.shape()
            .snap_to_grid_repaired(size, origin, ctx.geographic)
    } else {
        data.shape().snap_to_grid(size, origin)
    }
}

/// Force-2D kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Shape>"
)]
pub(crate) fn unary_force_2d(data: &ShapeData, _ctx: &OpCtx<'_>) -> Result<Shape> {
    Ok(data.shape().force_2d())
}

/// Force-3D kernel shared by the spine-routed free function.
pub(crate) fn unary_force_3d(data: &ShapeData, _ctx: &OpCtx<'_>, z: f64) -> Result<Shape> {
    data.shape().set_z(Some(z), false)
}

/// Set-Z kernel shared by the spine-routed free function.
pub(crate) fn unary_set_z(
    data: &ShapeData,
    _ctx: &OpCtx<'_>,
    value: Option<f64>,
    overwrite: bool,
) -> Result<Shape> {
    data.shape().set_z(value, overwrite)
}

/// Set-M kernel shared by the spine-routed free function.
pub(crate) fn unary_set_m(
    data: &ShapeData,
    _ctx: &OpCtx<'_>,
    value: Option<f64>,
    overwrite: bool,
) -> Result<Shape> {
    data.shape().set_m(value, overwrite)
}

/// Quantize kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Shape>"
)]
pub(crate) fn unary_quantize(data: &ShapeData, _ctx: &OpCtx<'_>, precision: i32) -> Result<Shape> {
    Ok(data.shape().quantize(precision))
}

/// Clip-by-rect kernel shared by the spine-routed free function.
pub(crate) fn unary_clip_by_rect(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    rect: Bounds,
    drop: bool,
) -> Result<Shape> {
    if ctx.geographic && data.crosses_antimeridian() {
        let split = data.shape().split_antimeridian()?;
        split.clip_by_rect_with_bounds(rect, drop, None)
    } else {
        data.clip_by_rect_with_bounds(rect, drop, data.bounds())
    }
}

/// Bounds kernel shared by the spine-routed free function.
#[expect(
    clippy::unnecessary_wraps,
    reason = "dispatch spine requires Result<Option<Bounds>>"
)]
pub(crate) fn unary_bounds(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<Option<Bounds>> {
    if crate::geometry::geographic_crossing(ctx.frame, data) {
        Ok(crate::geometry::geographic_crossing_bounds(data.shape()))
    } else {
        Ok(data.bounds())
    }
}

/// Minimum-clearance kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_clearance(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<f64> {
    let model = ctx.metric.require_model("minimum_clearance")?;
    metric_minimum_clearance(model, ctx.frame.crs_str(), data.shape())
}

/// Minimum-clearance witness kernel shared by the spine-routed free function.
pub(crate) fn unary_minimum_clearance_line(data: &ShapeData, ctx: &OpCtx<'_>) -> Result<Shape> {
    let model = ctx.metric.require_model("minimum_clearance_line")?;
    metric_minimum_clearance_line(model, ctx.frame.crs_str(), data.shape())
}

/// Distance kernel shared by the spine-routed free function.
pub(crate) fn binary_distance(left: &ShapeData, right: &ShapeData, ctx: &OpCtx<'_>) -> Result<f64> {
    let metric = ctx.metric.require_resolved("distance")?;
    pair_distance_resolved_result(
        metric,
        left,
        ctx.left_frame_cache.expect("binary left frame cache"),
        right,
        ctx.right_frame_cache.expect("binary right frame cache"),
    )
}

/// ``dwithin`` kernel shared by the spine-routed free function.
pub(crate) fn binary_dwithin(
    left: &ShapeData,
    right: &ShapeData,
    ctx: &OpCtx<'_>,
    distance: f64,
) -> Result<bool> {
    let metric = ctx.metric.require_resolved("dwithin")?;
    pair_dwithin_resolved_result(
        metric,
        left,
        ctx.left_frame_cache.expect("binary left frame cache"),
        right,
        ctx.right_frame_cache.expect("binary right frame cache"),
        distance,
    )
}

/// 3D distance kernel shared by the spine-routed free function.
pub(crate) fn binary_distance_3d(
    left: &ShapeData,
    right: &ShapeData,
    ctx: &OpCtx<'_>,
) -> Result<f64> {
    let to_metre = crate::crs::ensure_3d_metric(ctx.frame.crs_str())?;
    if left.axes_all_z() && right.axes_all_z() {
        return Ok(scale_length(
            crate::geometry::distance_3d_with_parts(
                left.distance_3d_parts(),
                right.distance_3d_parts(),
            ),
            to_metre,
        ));
    }
    if ctx.lane.is_array() {
        return Ok(f64::NAN);
    }
    Ok(scale_length(left.distance_3d(right)?, to_metre))
}

/// Hausdorff-distance kernel shared by the spine-routed free function.
pub(crate) fn binary_hausdorff_distance(
    left: &ShapeData,
    right: &ShapeData,
    ctx: &OpCtx<'_>,
    densify: Option<f64>,
) -> Result<f64> {
    let model = ctx.metric.require_model("hausdorff_distance")?;
    metric_hausdorff_densified(model, left.shape(), right.shape(), densify)
}

/// Fréchet-distance kernel shared by the spine-routed free function.
pub(crate) fn binary_frechet_distance(
    left: &ShapeData,
    right: &ShapeData,
    ctx: &OpCtx<'_>,
    densify: Option<f64>,
) -> Result<f64> {
    let model = ctx.metric.require_model("frechet_distance")?;
    metric_frechet_densified(model, left.shape(), right.shape(), densify)
}

fn linref_point_lane(result: Result<Shape>, array_lane: bool) -> Result<Shape> {
    if array_lane {
        degrade_linref_point(result)
    } else {
        result
    }
}

fn linref_linestring_lane(result: Result<Shape>, array_lane: bool) -> Result<Shape> {
    if array_lane {
        degrade_linref_linestring(result)
    } else {
        result
    }
}

/// Overlay kernel shared by the spine-routed set-op free functions.
pub(crate) fn binary_overlay(
    left: &ShapeData,
    right: &ShapeData,
    _ctx: &OpCtx<'_>,
    op: crate::OverlayOp,
    strictness: crate::geometry::Strictness,
) -> Result<Shape> {
    op.kernel(strictness)(left, right)
}

/// ``line_interpolate_point`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_interpolate_point(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    model: &MetricModel,
    distance: &F64Param,
    normalized: bool,
) -> Result<Shape> {
    linref_point_lane(
        line_interpolate_shape(
            model,
            data,
            ctx.left_frame_cache.expect("unary frame cache"),
            distance.get(ctx.lane.row()),
            normalized,
        ),
        ctx.lane.is_array(),
    )
}

/// ``line_substring`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_substring(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    model: &MetricModel,
    start_distance: &F64Param,
    end_distance: &F64Param,
    normalized: bool,
) -> Result<Shape> {
    let range = MeasureRange::substring_distance(
        start_distance.get(ctx.lane.row()),
        end_distance.get(ctx.lane.row()),
    )?;
    linref_linestring_lane(
        line_substring_shape(
            model,
            data,
            ctx.left_frame_cache.expect("unary frame cache"),
            range,
            normalized,
        ),
        ctx.lane.is_array(),
    )
}

/// ``line_interpolate_point_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_interpolate_point_m(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    m: &F64Param,
) -> Result<Shape> {
    linref_point_lane(
        data.shape().line_interpolate_point_m(m.get(ctx.lane.row())),
        ctx.lane.is_array(),
    )
}

/// ``line_substring_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_line_substring_m(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    start_m: &F64Param,
    end_m: &F64Param,
) -> Result<Shape> {
    let range =
        MeasureRange::substring_measure(start_m.get(ctx.lane.row()), end_m.get(ctx.lane.row()))?;
    linref_linestring_lane(data.shape().line_substring_m(range), ctx.lane.is_array())
}

/// ``interpolate_m`` kernel shared by the spine-routed free function.
pub(crate) fn unary_interpolate_m(
    data: &ShapeData,
    ctx: &OpCtx<'_>,
    start_m: &F64Param,
    end_m: &F64Param,
    overwrite: bool,
) -> Result<Shape> {
    let model = ctx.metric.require_model("interpolate_m")?;
    let range =
        MeasureRange::interpolate_m(start_m.get(ctx.lane.row()), end_m.get(ctx.lane.row()))?;
    metric_interpolate_m(model, data.shape(), range, overwrite)
}
