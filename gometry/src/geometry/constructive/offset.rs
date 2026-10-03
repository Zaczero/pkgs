#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::num::NonZeroU32;

use crate::geometry::constructive::{
    Result, WalkColumns, WalkJoinRule, WalkPlan, materialize_walk, source_vector,
};
use crate::geometry::{
    BufferJoinStyle, CoordSeq, Coordinates, ExpansionBudget, GeometryErrorKind, GeometryKind,
    LineSeq, MOrdinate, MultiLineString, Point, Segment, Shape, XY, ZOrdinate, axis_pow2_scale,
    dedup_consecutive_points, interpolate_f64, same_point, same_topological_coordinate,
    scaled_residual,
};
use crate::{Finite, Positive};

/// The finite source vertex run, with only exact zero-length edges removed,
/// plus its closure flag. This is structural edge admission, not a
/// tolerance-based simplification; offset curves and sided-buffer walks share
/// the same source order.
pub(crate) struct OffsetSource {
    vertices: Vec<Point>,
    closed: bool,
}

pub(crate) fn offset_source<C: Coordinates + ?Sized>(points: &C) -> Option<OffsetSource> {
    let mut vertices = points
        .iter_coords()
        .filter(|point| point.x.is_finite() && point.y.is_finite())
        .collect::<Vec<_>>();
    dedup_consecutive_points(&mut vertices);
    if vertices.len() < 2 {
        return None;
    }
    let closed = vertices
        .first()
        .zip(vertices.last())
        .is_some_and(|(first, last)| same_point(*first, *last));
    if closed {
        vertices.pop();
        if vertices.len() < 2 {
            return None;
        }
    }
    Some(OffsetSource { vertices, closed })
}

fn offset_walk_columns(
    mut walk: Vec<Point>,
    closed: bool,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: NonZeroU32,
    budget: &mut ExpansionBudget,
    track_sources: bool,
    reject_nonfinite: bool,
) -> Result<Option<(Vec<Point>, WalkColumns, bool)>> {
    let left = distance > 0.0;
    if left {
        walk.reverse();
    }
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments.get());
    let Some(plan) = WalkPlan::new(&walk, closed, distance.abs(), rule)? else {
        return Ok(None);
    };
    let Some(columns) =
        materialize_walk(&plan, step_angle, track_sources, budget, reject_nonfinite)?
    else {
        return Ok(None);
    };
    Ok(Some((walk, columns, left)))
}

/// One positively wound strip loop: the offset side followed by the source
/// in the opposite direction. Closed inputs retain their repeated endpoint,
/// so both ends meet there with flat caps instead of forming an annular seam.
pub(crate) fn sided_buffer_loop<C: Coordinates + ?Sized>(
    points: &C,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<(Vec<f64>, Vec<f64>)>> {
    let Some(mut source) = offset_source(points) else {
        return Ok(None);
    };
    if source.closed
        && let Some(&first) = source.vertices.first()
    {
        source.vertices.push(first);
    }
    let source_count = source.vertices.len();
    let Some((walk, columns, _left)) = offset_walk_columns(
        source.vertices,
        false,
        distance,
        rule,
        quadrant_segments,
        budget,
        false,
        true,
    )?
    else {
        return Ok(None);
    };
    let (mut xs, mut ys, _) = columns.into_columns();
    budget.add(source_count)?;
    xs.reserve(source_count);
    ys.reserve(source_count);
    xs.extend(walk.iter().rev().map(|point| point.x));
    ys.extend(walk.iter().rev().map(|point| point.y));
    Ok(Some((xs, ys)))
}

/// The planned offset curve with the buffer's join vocabulary: round
/// fillet arcs (`quadrant_segments`), limit-clipped miters, or bevels — positive
/// `distance` offsets LEFT of the line direction, negative RIGHT, and the
/// output keeps the input direction either way (the left side is the
/// right-offset of the REVERSED walk, un-reversed after emission). Closed
/// inputs offset cyclically and close the output. `None` means that the
/// unrepresentable offset has no line result or the source has no segment.
pub(crate) fn offset_curve_points<C: Coordinates + ?Sized>(
    points: &C,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Vec<Point>>> {
    // Fewer than two occupied source vertices is a genuine typed collapse.
    let Some(source) = offset_source(points) else {
        return Ok(None);
    };
    let closed = source.closed;
    let Some((walk, columns, left)) = offset_walk_columns(
        source.vertices,
        closed,
        distance,
        rule,
        quadrant_segments,
        budget,
        true,
        false,
    )?
    else {
        return Ok(None);
    };
    let (xs, ys, sources) = columns.into_columns();
    let sources = sources.expect("tracked walk columns retain a source per emitted vertex");
    // Each output vertex inherits the ordinates of the source vertex it
    // derives from (arc and join points belong to their corner) — the same
    // per-vertex Z/M carry the miter emitter always had.
    let mut result: Vec<Point> = std::iter::zip(std::iter::zip(xs, ys), sources)
        .map(|((x, y), origin)| {
            let origin = *walk
                .get(origin as usize)
                .expect("offset walk source ordinal belongs to its source vertices");
            Point::new_raw_axes(x, y, ZOrdinate(origin.z()), MOrdinate(origin.m()))
        })
        .collect();
    if closed && let Some(&first) = result.first() {
        // `WalkPlan` is open for the offset/winding shared representation;
        // this public closed curve owns one more coordinate and admits it
        // before appending, rather than reviving the old input-size estimate.
        budget.add(1)?;
        result.push(first);
    }
    if left {
        result.reverse();
    }
    // A walk that emitted fewer than two vertices is another genuine
    // geometric collapse and becomes the typed empty at the caller.
    Ok((result.len() >= 2).then_some(result))
}

/// The default miter limit, in offset widths from the vertex.
///
/// A near-reversal turn otherwise sends the miter intersection toward
/// infinity; styled miter walks clip it flat at the declared reach.
pub(crate) const DEFAULT_MITER_LIMIT: f64 = 5.0;

/// Shared miter geometry before the caller applies its local join vocabulary.
pub(crate) enum MiterConstruction {
    WithinLimit(XY),
    Clipped { entry: XY, exit: XY },
    Unusable,
}

pub(crate) fn miter_join(
    incoming: Segment,
    outgoing: Segment,
    vertex: Point,
    distance: f64,
    limit: f64,
) -> MiterConstruction {
    let Some(point) = line_intersection(incoming, outgoing) else {
        return MiterConstruction::Unusable;
    };
    if miter_within_limit(point, vertex, distance, limit) {
        return MiterConstruction::WithinLimit(point);
    }
    let Some((entry, exit)) = clipped_miter(incoming, outgoing, vertex, point, distance, limit)
    else {
        return MiterConstruction::Unusable;
    };
    MiterConstruction::Clipped { entry, exit }
}

/// Compare miter reach against `limit * distance` without squaring either
/// quantity. The ordinary comparison keeps its direct rounding; subnormal and
/// overflow cases compare both sides in a common finite scale. Equality is
/// intentionally within-limit, so the clipped form begins strictly above the
/// declared reach.
fn miter_within_limit(point: XY, vertex: Point, distance: f64, limit: f64) -> bool {
    let distance = distance.abs();
    let same = same_point(point, vertex);
    let Some((dx, dy, source_scale)) = source_vector(vertex.xy(), point) else {
        return false;
    };
    let reach = dx.hypot(dy);
    let scaled_distance = distance * source_scale;
    let allowed = scaled_distance * limit;
    if same {
        return true;
    }
    if distance == 0.0 {
        return false;
    }
    if reach.is_normal() && allowed.is_normal() {
        return reach <= allowed;
    }
    if reach.is_finite() && allowed.is_infinite() {
        return true;
    }
    if reach.is_infinite() && allowed.is_finite() {
        return false;
    }
    let scale = dx.abs().max(dy.abs()).max(scaled_distance);
    if scale == 0.0 || !scale.is_finite() {
        return scaled_distance.is_infinite();
    }
    (dx / scale).hypot(dy / scale) <= (scaled_distance / scale) * limit
}

fn clipped_miter(
    incoming: Segment,
    outgoing: Segment,
    vertex: Point,
    point: XY,
    distance: f64,
    limit: f64,
) -> Option<(XY, XY)> {
    let allowed = distance.abs() * limit;
    if !allowed.is_finite() {
        return None;
    }
    let (ux, uy) = unit_direction(point, vertex)?;
    let mid = Point::new_raw_xy(vertex.x + allowed * ux, vertex.y + allowed * uy);
    if !mid.x.is_finite() || !mid.y.is_finite() {
        return None;
    }
    let clip = Segment {
        start: mid.into(),
        end: Point::new_raw_xy(mid.x - uy, mid.y + ux).into(),
    };
    let entry = line_intersection(incoming, clip)?;
    let exit = line_intersection(outgoing, clip)?;
    (entry.x.is_finite() && entry.y.is_finite() && exit.x.is_finite() && exit.y.is_finite())
        .then_some((entry, exit))
}

fn unit_direction(point: XY, vertex: Point) -> Option<(f64, f64)> {
    let (dx, dy, _) = source_vector(vertex.xy(), point)?;
    let scale = dx.abs().max(dy.abs());
    let (dx, dy) = if scale.is_finite() && scale > 0.0 {
        (dx / scale, dy / scale)
    } else {
        return None;
    };
    let length = dx.hypot(dy);
    (length > 0.0 && length.is_finite()).then_some((dx / length, dy / length))
}

pub(crate) fn line_intersection(left: Segment, right: Segment) -> Option<XY> {
    let x_extent = left
        .start
        .x
        .abs()
        .max(left.end.x.abs())
        .max(right.start.x.abs())
        .max(right.end.x.abs());
    let y_extent = left
        .start
        .y
        .abs()
        .max(left.end.y.abs())
        .max(right.start.y.abs())
        .max(right.end.y.abs());
    if !x_extent.is_finite() || !y_extent.is_finite() {
        return None;
    }
    let (x_scale, y_scale) = (axis_pow2_scale(x_extent), axis_pow2_scale(y_extent));
    let left_dx = scaled_residual(left.end.x, left.start.x, x_scale);
    let left_dy = scaled_residual(left.end.y, left.start.y, y_scale);
    let right_dx = scaled_residual(right.end.x, right.start.x, x_scale);
    let right_dy = scaled_residual(right.end.y, right.start.y, y_scale);
    let offset_x = scaled_residual(right.start.x, left.start.x, x_scale);
    let offset_y = scaled_residual(right.start.y, left.start.y, y_scale);
    if ![left_dx, left_dy, right_dx, right_dy, offset_x, offset_y]
        .into_iter()
        .all(f64::is_finite)
    {
        return None;
    }
    let denominator = left_dx * right_dy - left_dy * right_dx;
    if denominator == 0.0 || !denominator.is_finite() {
        return None;
    }
    let numerator = offset_x * right_dy - offset_y * right_dx;
    let fraction = numerator / denominator;
    if !fraction.is_finite() {
        return None;
    }
    let point = XY::new(
        interpolate_f64(left.start.x, left.end.x, fraction),
        interpolate_f64(left.start.y, left.end.y, fraction),
    );
    (point.x.is_finite() && point.y.is_finite()).then_some(point)
}

pub(crate) fn reversed_points<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    points.iter_coords().rev().collect()
}

impl Shape {
    pub(crate) fn offset_curve_budgeted(
        &self,
        distance: f64,
        join_style: BufferJoinStyle,
        quadrant_segments: NonZeroU32,
        miter_limit: Positive,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        let distance = Finite::try_new("distance", distance)?.get();
        if same_topological_coordinate(distance, 0.0) {
            return match self {
                Self::LineString(_)
                | Self::MultiLineString(_)
                | Self::Empty(
                    GeometryKind::LineString
                    | GeometryKind::MultiLineString
                    | GeometryKind::GeometryCollection,
                    _,
                ) => Ok(self.clone()),
                Self::GeometryCollection(geometries) => Ok(Self::GeometryCollection(
                    geometries.try_map_members(|geometry| {
                        geometry.offset_curve_budgeted(
                            distance,
                            join_style,
                            quadrant_segments,
                            miter_limit,
                            budget,
                        )
                    })?,
                )),
                Self::Point(_)
                | Self::MultiPoint(_)
                | Self::Polygon(_)
                | Self::MultiPolygon(_)
                | Self::Empty(
                    GeometryKind::Point
                    | GeometryKind::MultiPoint
                    | GeometryKind::Polygon
                    | GeometryKind::MultiPolygon,
                    _,
                ) => Err(GeometryErrorKind::LinealRequired.into()),
            };
        }
        let rule = WalkJoinRule::new(join_style, miter_limit.get());
        let mut offset = |line: &CoordSeq| -> Result<Option<Vec<Point>>> {
            // The planned walk owns every style. A missing result is handled
            // as typed empty or documented raw-offset unrepresentability;
            // it never changes the requested join style.
            offset_curve_points(line, distance, rule, quadrant_segments, budget)
        };
        Ok(match self {
            Self::LineString(points) => match offset(points)? {
                None => Self::typed_empty(GeometryKind::LineString, points.axes()),
                Some(line) => {
                    Self::LineString(LineSeq::try_new(CoordSeq::try_from_points(&line)?)?)
                },
            },
            Self::MultiLineString(lines) => {
                let members = lines
                    .members_with_values()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Ok(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(line) => offset(line.as_coords())?
                            .map_or(Ok(crate::geometry::MultiMember::Empty), |line| {
                                Ok(crate::geometry::MultiMember::Value(LineSeq::try_new(
                                    CoordSeq::try_from_points(&line)?,
                                )?))
                            }),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::MultiLineString(MultiLineString::try_new(lines.axes(), members)?)
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(|geometry| {
                    geometry.offset_curve_budgeted(
                        distance,
                        join_style,
                        quadrant_segments,
                        miter_limit,
                        budget,
                    )
                })?)
            },
            Self::Empty(
                GeometryKind::LineString
                | GeometryKind::MultiLineString
                | GeometryKind::GeometryCollection,
                _,
            ) => self.clone(),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::Empty(
                GeometryKind::Point
                | GeometryKind::MultiPoint
                | GeometryKind::Polygon
                | GeometryKind::MultiPolygon,
                _,
            ) => return Err(GeometryErrorKind::LinealRequired.into()),
        })
    }
}
