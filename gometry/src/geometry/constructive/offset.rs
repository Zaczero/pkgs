#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::num::NonZeroU32;

use crate::geometry::constructive::{
    Result, WalkJoinRule, WalkPlan, materialize_walk, unit_right_normal,
};
use crate::geometry::{
    BufferJoinStyle, CoordSeq, Coordinates, ExpansionBudget, GeometryErrorKind, GeometryKind,
    LineSeq, MOrdinate, MultiLineString, Point, Polygon, RepairMethod, Ring, Segment, Shape,
    Strictness, XY, ZOrdinate, column_all_finite, dedup_consecutive_points,
    interpolate_segment_point, push_distinct_point, same_point, same_topological_coordinate,
    wrap_index,
};
use crate::{Finite, Positive};

/// Construct one-sided strips directly when the whole strip is already valid
/// (the overwhelmingly common case), otherwise use the union of its
/// per-segment quads — the swept region itself, which stays correct when the
/// path self-crosses and a whole-ring repair would not (even-odd ring repair
/// cancels faces covered by two sweep arms).
pub(crate) fn sided_strip_parts<C: Coordinates + ?Sized>(
    points: &C,
    signed: f64,
) -> Result<Vec<Polygon>> {
    let Some(source) = offset_source(points) else {
        return Ok(Vec::new());
    };
    let Some(strip) = sided_strip_source(&source, signed)? else {
        return Ok(Vec::new());
    };
    let strip = Shape::Polygon(strip);
    if strip.validate()?.is_none() {
        let Shape::Polygon(polygon) = strip else {
            unreachable!("strip is a polygon")
        };
        return Ok(vec![polygon]);
    }
    let quads = sided_strip_quads(points, signed)?;
    Ok(match Shape::union_all(&quads, Strictness::Lenient)? {
        Shape::Polygon(polygon) => vec![polygon],
        Shape::MultiPolygon(polygons) => polygons.iter().cloned().collect(),
        _ => Vec::new(),
    })
}

/// The strip's per-segment quads — the same miter-joined offset geometry as
/// the direct strip, one quad per deduplicated input segment, X/Y only (the
/// repair lane never carries Z/M, matching the whole-ring repair it
/// replaces). A locally inverted miter can self-cross an individual quad;
/// those are rebuilt before the union, which requires valid parts.
pub(crate) fn sided_strip_quads<C: Coordinates + ?Sized>(
    points: &C,
    signed: f64,
) -> Result<Vec<Shape>> {
    let Some(source) = offset_source(points) else {
        return Ok(Vec::new());
    };
    let vertices = &source.vertices;
    let segments = offset_segments(vertices, source.closed, signed);
    let count = source.segment_count();
    if segments.len() != count {
        // Deduplicated vertices offset 1:1; a dropped segment means a
        // non-finite offset (coordinates at the f64 edge), not an empty
        // geometric strip.
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    let closed = source.closed;
    // One join per joined vertex (every vertex when closed, the interior
    // ones when open), computed once and shared by the two adjacent quads.
    let joins: Vec<OffsetJoin> = if closed {
        (0..count)
            .map(|index| {
                let previous = segments[wrap_index(index + count - 1, count)];
                offset_join(previous, segments[index], vertices[index], signed)
            })
            .collect()
    } else {
        (1..count)
            .map(|index| {
                offset_join(
                    segments[index - 1],
                    segments[index],
                    vertices[index],
                    signed,
                )
            })
            .collect()
    };
    let offset_start = |index: usize| {
        if closed {
            joins[index].outgoing()
        } else if index == 0 {
            segments[0].start
        } else {
            joins[index - 1].outgoing()
        }
    };
    let offset_end = |index: usize| {
        if closed {
            joins[wrap_index(index + 1, count)].incoming()
        } else if index + 1 == count {
            segments[count - 1].end
        } else {
            joins[index].incoming()
        }
    };
    let normalized = |quad: Shape| {
        if quad.validate()?.is_none() {
            Ok(quad)
        } else {
            quad.repair(RepairMethod::Structure, false)
        }
    };
    let mut parts = Vec::with_capacity(count + joins.len());
    for index in 0..count {
        let start = vertices[index];
        let end = vertices[wrap_index(index + 1, vertices.len())];
        parts.push(normalized(Shape::Polygon(Polygon::new(
            xy_ring([start, end, offset_end(index), offset_start(index), start])?,
            Vec::new(),
        )?))?);
    }
    // A bevel join leaves a wedge between the two raw offset endpoints that
    // no per-segment quad covers; fill it so the union is the full sweep.
    for (join_index, join) in joins.iter().enumerate() {
        let OffsetJoin::Bevel { incoming, outgoing } = join else {
            continue;
        };
        if same_point(*incoming, *outgoing) {
            continue;
        }
        let vertex = vertices[if closed { join_index } else { join_index + 1 }];
        parts.push(normalized(Shape::Polygon(Polygon::new(
            xy_ring([vertex, *incoming, *outgoing, vertex])?,
            Vec::new(),
        )?))?);
    }
    Ok(parts)
}

/// A small X/Y ring assembled straight into ordinate columns — no
/// intermediate `Vec<Point>` per part.
pub(crate) fn xy_ring<const N: usize>(points: [Point; N]) -> Result<Ring> {
    Ok(Ring::from_closed(CoordSeq::try_from_vecs(
        points.iter().map(|point| point.x).collect(),
        points.iter().map(|point| point.y).collect(),
        None,
        None,
    )?))
}

/// The finite source vertex run, with only exact zero-length edges removed,
/// plus its closure flag. This is structural edge admission, not a
/// tolerance-based simplification: raw offsets and sided strips use the same
/// source order so their edge and join indexes cannot drift apart.
pub(crate) struct OffsetSource {
    vertices: Vec<Point>,
    closed: bool,
}

impl OffsetSource {
    /// One offset segment per polyline segment (cyclic when closed).
    fn segment_count(&self) -> usize {
        self.vertices.len() - usize::from(!self.closed)
    }
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
    let closed = same_point(vertices[0], vertices[vertices.len() - 1]);
    if closed {
        vertices.pop();
        if vertices.len() < 2 {
            return None;
        }
    }
    Some(OffsetSource { vertices, closed })
}

/// The closed strip polygon between a line and its offset at `signed`
/// distance: forward along the line, back along the offset (flat ends).
fn sided_strip_source(source: &OffsetSource, signed: f64) -> Result<Option<Polygon>> {
    let offset = match offset_line_source(source, signed) {
        RawOffset::Value(offset) => offset,
        RawOffset::Empty => return Ok(None),
        RawOffset::Unrepresentable => {
            return Err(GeometryErrorKind::message(
                "buffer calculation produced non-finite coordinates",
            ));
        },
    };
    let mut shell = source.vertices.clone();
    shell.extend(offset.into_iter().rev());
    if let Some(first) = shell.first().copied() {
        shell.push(first);
    }
    if shell.len() < 4 {
        Ok(None)
    } else {
        Ok(Some(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(&shell)?),
            Vec::new(),
        )?))
    }
}

/// The planned offset curve with the buffer's join vocabulary: round
/// fillet arcs (`quadrant_segments`), limit-clipped miters, or bevels — positive
/// `distance` offsets LEFT of the line direction, negative RIGHT, and the
/// output keeps the input direction either way (the left side is the
/// right-offset of the REVERSED walk, un-reversed after emission). Closed
/// inputs offset cyclically and close the output. `None` means that the raw
/// offset cannot be represented (or the source has no segment); it is not a
/// request to switch to the legacy raw-miter emitter.
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
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments.get());
    let left = distance > 0.0;
    let mut walk = source.vertices;
    if left {
        walk.reverse();
    }
    // A failed normal is an unrepresentable raw offset, not permission to
    // change the requested style to the legacy emitter.
    let Some(plan) = WalkPlan::new(&walk, source.closed, distance.abs(), rule, step_angle)? else {
        return Ok(None);
    };
    // `reject_nonfinite = false` is the raw-offset overflow contract: the
    // materializer reports unrepresentable derived coordinates as `None`.
    let Some(columns) = materialize_walk(&plan, step_angle, true, budget, false)? else {
        return Ok(None);
    };
    let (xs, ys, sources) = columns.into_columns();
    let sources = sources.expect("tracked walk columns retain a source per emitted vertex");
    // Coordinates at the f64 edge overflow the offset arithmetic — no
    // offset curve (the alternate emitter's guard agrees, so the surface
    // yields the documented empty line).
    if !column_all_finite(&xs) || !column_all_finite(&ys) {
        return Ok(None);
    }
    // Each output vertex inherits the ordinates of the source vertex it
    // derives from (arc and join points belong to their corner) — the same
    // per-vertex Z/M carry the miter emitter always had.
    let mut result: Vec<Point> = std::iter::zip(std::iter::zip(xs, ys), sources)
        .map(|((x, y), origin)| {
            let origin = walk[origin as usize];
            Point::new_raw_axes(x, y, ZOrdinate(origin.z()), MOrdinate(origin.m()))
        })
        .collect();
    if source.closed
        && let Some(&first) = result.first()
    {
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

#[cfg(test)]
pub(crate) fn offset_line<C: Coordinates + ?Sized>(
    points: &C,
    distance: f64,
) -> Option<Vec<Point>> {
    let source = offset_source(points)?;
    match offset_line_source(&source, distance) {
        RawOffset::Value(offset) => Some(offset),
        RawOffset::Empty | RawOffset::Unrepresentable => None,
    }
}

enum RawOffset {
    /// The occupied source has no representable line result.
    Empty,
    /// A finite source edge or its translated offset exceeded binary64.
    Unrepresentable,
    /// A source-ordered offset line.
    Value(Vec<Point>),
}

fn offset_line_source(source: &OffsetSource, distance: f64) -> RawOffset {
    let vertices = &source.vertices;
    let segments = offset_segments(vertices, source.closed, distance);
    if segments.len() != source.segment_count() {
        // Deduplicated vertices offset 1:1; a dropped segment means a
        // non-finite offset (coordinates at the f64 edge) — no offset line.
        return RawOffset::Unrepresentable;
    }

    let mut result = Vec::with_capacity(vertices.len() + usize::from(source.closed));
    let push_join = |result: &mut Vec<Point>, join: OffsetJoin| match join {
        OffsetJoin::Miter(point) => push_distinct_point(result, point),
        OffsetJoin::Bevel { incoming, outgoing } => {
            push_distinct_point(result, incoming);
            push_distinct_point(result, outgoing);
        },
    };
    if source.closed {
        for index in 0..vertices.len() {
            let previous = segments[wrap_index(index + segments.len() - 1, segments.len())];
            let current = segments[index];
            let join = offset_join(previous, current, vertices[index], distance);
            push_join(&mut result, join);
        }
        if let Some(first) = result.first().copied() {
            result.push(first);
        }
    } else {
        push_distinct_point(&mut result, segments[0].start);
        for index in 1..vertices.len() - 1 {
            let join = offset_join(
                segments[index - 1],
                segments[index],
                vertices[index],
                distance,
            );
            push_join(&mut result, join);
        }
        push_distinct_point(&mut result, segments[segments.len() - 1].end);
    }

    match result.len() >= 2 {
        true => RawOffset::Value(result),
        false => RawOffset::Empty,
    }
}

pub(crate) fn offset_segments(points: &[Point], closed: bool, distance: f64) -> Vec<OffsetEdge> {
    let limit = if closed {
        points.len()
    } else {
        points.len() - 1
    };
    (0..limit)
        .filter_map(|index| {
            let start = points[index];
            let end = points[wrap_index(index + 1, points.len())];
            offset_segment(start, end, distance)
        })
        .collect()
}

/// One offset edge with FULL ordinate carry — the offset lane keeps
/// Z/M through translation, and the joins re-attach the vertex's
/// ordinates (the planar [`Segment`] is XY-only by design).
#[derive(Clone, Copy)]
pub(crate) struct OffsetEdge {
    pub start: Point,
    pub end: Point,
}

pub(crate) fn offset_segment(start: Point, end: Point, distance: f64) -> Option<OffsetEdge> {
    // Use the walk's one normal kernel for raw and styled constructions. The
    // raw lane negates the right normal to offset to the left.
    let (nx, ny) = unit_right_normal(start, end)?;
    let offset_x = -nx * distance;
    let offset_y = -ny * distance;
    // Offsetting coordinates near f64::MAX by a same-sign distance can overflow;
    // drop such a segment rather than panicking on the finite-coordinate assert.
    Some(OffsetEdge {
        start: start.translate_xy(offset_x, offset_y).ok()?,
        end: end.translate_xy(offset_x, offset_y).ok()?,
    })
}

/// The default miter limit, in offset widths from the vertex (the
/// GEOS/JTS default).
///
/// A near-reversal turn otherwise sends the miter intersection toward
/// infinity. Raw offsets bevel an over-limit join; styled miter walks clip
/// it flat at the declared reach.
pub(crate) const DEFAULT_MITER_LIMIT: f64 = 5.0;

/// One join between consecutive offset segments: the miter intersection
/// when it stays within [`DEFAULT_MITER_LIMIT`] offset widths of the vertex,
/// otherwise the bevel — the two raw offset endpoints joined by a straight
/// edge (the GEOS/JTS limited-miter shape; collapsing to a midpoint would
/// pull the strip inside the offset distance).
pub(crate) enum OffsetJoin {
    Miter(Point),
    Bevel { incoming: Point, outgoing: Point },
}

impl OffsetJoin {
    /// The join point ending the incoming segment's offset edge.
    const fn incoming(&self) -> Point {
        match self {
            Self::Miter(point)
            | Self::Bevel {
                incoming: point, ..
            } => *point,
        }
    }

    /// The join point starting the outgoing segment's offset edge.
    const fn outgoing(&self) -> Point {
        match self {
            Self::Miter(point)
            | Self::Bevel {
                outgoing: point, ..
            } => *point,
        }
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn offset_join(
    previous: OffsetEdge,
    current: OffsetEdge,
    vertex: Point,
    distance: f64,
) -> OffsetJoin {
    let carry = |point: XY| {
        vertex
            .with_xy(point.x, point.y)
            .expect("join point preserves finite vertex ordinates")
    };
    match miter_join(
        Segment {
            start: previous.start.xy(),
            end: previous.end.xy(),
        },
        Segment {
            start: current.start.xy(),
            end: current.end.xy(),
        },
        vertex,
        distance,
        DEFAULT_MITER_LIMIT,
        MiterFinish::Bevel,
    ) {
        MiterConstruction::WithinLimit(point) => OffsetJoin::Miter(carry(point)),
        // Near-parallel carriers and over-limit raw joins both retain the
        // legacy raw-offset bevel. Styled miter joins use MiterFinish::Clip
        // below and therefore do not silently share this outcome.
        MiterConstruction::OverLimit
        | MiterConstruction::Unusable
        | MiterConstruction::Clipped { .. } => OffsetJoin::Bevel {
            incoming: carry(previous.end.xy()),
            outgoing: carry(current.start.xy()),
        },
    }
}

/// The explicit local finish for an over-limit miter. Raw offsets use a bevel
/// to retain their established shape; styled miter walks use a clipped miter.
#[derive(Clone, Copy)]
pub(crate) enum MiterFinish {
    Bevel,
    Clip,
}

/// Shared miter geometry before the caller applies its local join vocabulary.
pub(crate) enum MiterConstruction {
    WithinLimit(XY),
    OverLimit,
    Clipped { entry: XY, exit: XY },
    Unusable,
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn miter_join(
    incoming: Segment,
    outgoing: Segment,
    vertex: Point,
    distance: f64,
    limit: f64,
    finish: MiterFinish,
) -> MiterConstruction {
    let Some(point) = line_intersection(incoming, outgoing) else {
        return MiterConstruction::Unusable;
    };
    if miter_within_limit(point, vertex, distance, limit) {
        return MiterConstruction::WithinLimit(point);
    }
    if matches!(finish, MiterFinish::Bevel) {
        return MiterConstruction::OverLimit;
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
    if distance == 0.0 {
        return same;
    }
    let dx = point.x - vertex.x;
    let dy = point.y - vertex.y;
    let reach = dx.hypot(dy);
    let allowed = distance * limit;
    if same {
        return true;
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
    let scale = point
        .x
        .abs()
        .max(point.y.abs())
        .max(vertex.x.abs())
        .max(vertex.y.abs())
        .max(distance);
    if scale == 0.0 || !scale.is_finite() {
        return false;
    }
    let scaled_dx = point.x / scale - vertex.x / scale;
    let scaled_dy = point.y / scale - vertex.y / scale;
    scaled_dx.hypot(scaled_dy) <= (distance / scale) * limit
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
    let dx = point.x - vertex.x;
    let dy = point.y - vertex.y;
    let scale = dx.abs().max(dy.abs());
    let (dx, dy) = if scale.is_finite() && scale > 0.0 {
        (dx / scale, dy / scale)
    } else {
        let scale = point
            .x
            .abs()
            .max(point.y.abs())
            .max(vertex.x.abs())
            .max(vertex.y.abs());
        if scale == 0.0 || !scale.is_finite() {
            return None;
        }
        (
            point.x / scale - vertex.x / scale,
            point.y / scale - vertex.y / scale,
        )
    };
    let length = dx.hypot(dy);
    (length > 0.0 && length.is_finite()).then_some((dx / length, dy / length))
}

pub(crate) fn line_intersection(left: Segment, right: Segment) -> Option<XY> {
    let left_dx = left.end.x - left.start.x;
    let left_dy = left.end.y - left.start.y;
    let right_dx = right.end.x - right.start.x;
    let right_dy = right.end.y - right.start.y;
    let denominator = left_dx * right_dy - left_dy * right_dx;
    // Nonzero finite determinants stay on the fast path. Zero / subnormal /
    // nonfinite (near-parallel or scale-collapsed) reject without an absolute
    // epsilon — a fixed 1e-12 bevelled valid right-angle miters at 1e-6 scale.
    if denominator == 0.0 || !denominator.is_normal() {
        // Subnormal-but-nonzero can still yield a usable fraction after
        // max-abs normalization of the two direction vectors.
        let scale = left_dx
            .abs()
            .max(left_dy.abs())
            .max(right_dx.abs())
            .max(right_dy.abs());
        if scale == 0.0 || !scale.is_finite() {
            return None;
        }
        let (ldx, ldy) = (left_dx / scale, left_dy / scale);
        let (rdx, rdy) = (right_dx / scale, right_dy / scale);
        let denom = ldx * rdy - ldy * rdx;
        if denom == 0.0 || !denom.is_finite() {
            return None;
        }
        let offset_x = (right.start.x - left.start.x) / scale;
        let offset_y = (right.start.y - left.start.y) / scale;
        let fraction = (offset_x * rdy - offset_y * rdx) / denom;
        if !fraction.is_finite() {
            return None;
        }
        return Some(interpolate_segment_point(left.start, left.end, fraction));
    }
    let offset_x = right.start.x - left.start.x;
    let offset_y = right.start.y - left.start.y;
    let fraction = (offset_x * right_dy - offset_y * right_dx) / denominator;
    if !fraction.is_finite() {
        // Coordinates large enough to saturate the numerator leave the
        // intersection parameter undefined; treat the segments as having no
        // usable crossing rather than interpolating a non-finite point.
        return None;
    }
    Some(interpolate_segment_point(left.start, left.end, fraction))
}

pub(crate) fn reversed_points<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    points.iter_coords().rev().collect()
}

impl Shape {
    pub(crate) fn offset_curve(
        &self,
        distance: f64,
        join_style: BufferJoinStyle,
        quadrant_segments: NonZeroU32,
        miter_limit: Positive,
    ) -> Result<Self> {
        let mut budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
        self.offset_curve_budgeted(
            distance,
            join_style,
            quadrant_segments,
            miter_limit,
            &mut budget,
        )
    }

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
                Self::LineString(_) | Self::MultiLineString(_) => Ok(self.clone()),
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
                _ => Err(GeometryErrorKind::LinealRequired.into()),
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
                    .members()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Ok(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(index) => offset(
                            lines.as_slice()[index].as_coords(),
                        )?
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
            _ => return Err(GeometryErrorKind::LinealRequired.into()),
        })
    }
}
