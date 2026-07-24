#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::num::NonZeroU32;

use super::*;
use crate::{Finite, Positive};

/// when it is already valid (the overwhelmingly common case), otherwise the
/// union of its per-segment quads — the swept region itself, which stays
/// correct when the path self-crosses and a whole-ring repair would not
/// (even-odd ring repair cancels faces covered by two sweep arms).
pub(crate) fn sided_strip_parts<C: Coordinates + ?Sized>(
    points: &C,
    signed: f64,
) -> Result<Vec<Polygon>> {
    let Some(strip) = sided_strip(points, signed) else {
        return Ok(Vec::new());
    };
    let strip = Shape::Polygon(strip);
    if strip.validate().is_none() {
        let Shape::Polygon(polygon) = strip else {
            unreachable!("strip is a polygon")
        };
        return Ok(vec![polygon]);
    }
    let quads = sided_strip_quads(points, signed)?;
    Ok(match Shape::union_all(&quads, Strictness::Lenient)? {
        Shape::Polygon(polygon) => vec![polygon],
        Shape::MultiPolygon(polygons) => polygons,
        _ => Vec::new(),
    })
}

/// The strip's per-segment quads — the same miter-joined offset geometry as
/// [`sided_strip`], one quad per deduplicated input segment, X/Y only (the
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
        // non-finite offset (coordinates at the f64 edge) — no strip.
        return Ok(Vec::new());
    }
    let closed = source.closed;
    // One join per joined vertex (every vertex when closed, the interior
    // ones when open), computed once and shared by the two adjacent quads.
    let joins: Vec<OffsetJoin> = if closed {
        (0..count)
            .map(|index| {
                let previous = segments[wrap_index(index + count - 1, count)];
                offset_join(previous, segments[index], vertices[index])
            })
            .collect()
    } else {
        (1..count)
            .map(|index| offset_join(segments[index - 1], segments[index], vertices[index]))
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
        if quad.validate().is_none() {
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
            xy_ring([start, end, offset_end(index), offset_start(index), start]),
            Vec::new(),
        )))?);
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
            xy_ring([vertex, *incoming, *outgoing, vertex]),
            Vec::new(),
        )))?);
    }
    Ok(parts)
}

/// A small X/Y ring assembled straight into ordinate columns — no
/// intermediate `Vec<Point>` per part.
pub(crate) fn xy_ring<const N: usize>(points: [Point; N]) -> Ring {
    Ring::from_trusted_closed(CoordSeq::from_columns(
        Arc::new(points.map(|point| point.x)),
        Arc::new(points.map(|point| point.y)),
        None,
        None,
    ))
}

/// The deduplicated finite vertex run of a line plus its closure flag — the
/// one normalization in front of every offset construction, so
/// [`offset_line`] and the sided-strip quads can never disagree on
/// vertex/segment alignment.
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
pub(crate) fn sided_strip<C: Coordinates + ?Sized>(points: &C, signed: f64) -> Option<Polygon> {
    let offset = offset_line(points, signed)?;
    let mut shell: Vec<Point> = points
        .iter_coords()
        .filter(|point| point.x.is_finite() && point.y.is_finite())
        .collect();
    if shell.len() < 2 {
        return None;
    }
    shell.extend(offset.into_iter().rev());
    if let Some(first) = shell.first().copied() {
        shell.push(first);
    }
    (shell.len() >= 4).then(|| Polygon::new(Ring::from_trusted_closed(shell), Vec::new()))
}

/// The planned offset curve with the buffer's join vocabulary: round
/// fillet arcs (`quadrant_segments`), limit-clipped miters, or bevels — positive
/// `distance` offsets LEFT of the line direction, negative RIGHT, and the
/// output keeps the input direction either way (the left side is the
/// right-offset of the REVERSED walk, un-reversed after emission). Closed
/// inputs offset cyclically and close the output. `None` when the walk
/// plan degenerates (near-folds) — callers fall back to [`offset_line`].
pub(crate) fn offset_curve_points<C: Coordinates + ?Sized>(
    points: &C,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Vec<Point>> {
    let source = offset_source(points)?;
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments.get());
    let left = distance > 0.0;
    let mut walk = source.vertices;
    if left {
        walk.reverse();
    }
    let plan = WalkPlan::new(&walk, source.closed, distance.abs(), rule, step_angle)?;
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    let mut sources = Vec::new();
    plan.emit_tracked(step_angle, &mut xs, &mut ys, &mut sources);
    // Coordinates at the f64 edge overflow the offset arithmetic — no
    // offset curve (the alternate emitter's guard agrees, so the surface
    // yields the documented empty line).
    if !column_all_finite(&xs) || !column_all_finite(&ys) {
        return None;
    }
    // Each output vertex inherits the ordinates of the source vertex it
    // derives from (arc and join points belong to their corner) — the same
    // per-vertex Z/M carry the miter emitter always had.
    let mut result: Vec<Point> = std::iter::zip(std::iter::zip(xs, ys), sources)
        .map(|((x, y), origin)| {
            let origin = walk[origin as usize];
            Point::new_unchecked_axes(x, y, ZOrdinate(origin.z()), MOrdinate(origin.m()))
        })
        .collect();
    if source.closed
        && let Some(&first) = result.first()
    {
        result.push(first);
    }
    if left {
        result.reverse();
    }
    (result.len() >= 2).then_some(result)
}

pub(crate) fn offset_line<C: Coordinates + ?Sized>(
    points: &C,
    distance: f64,
) -> Option<Vec<Point>> {
    let source = offset_source(points)?;
    let vertices = &source.vertices;
    let segments = offset_segments(vertices, source.closed, distance);
    if segments.len() != source.segment_count() {
        // Deduplicated vertices offset 1:1; a dropped segment means a
        // non-finite offset (coordinates at the f64 edge) — no offset line.
        return None;
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
            let join = offset_join(previous, current, vertices[index]);
            push_join(&mut result, join);
        }
        if let Some(first) = result.first().copied() {
            result.push(first);
        }
    } else {
        push_distinct_point(&mut result, segments[0].start);
        for index in 1..vertices.len() - 1 {
            let join = offset_join(segments[index - 1], segments[index], vertices[index]);
            push_join(&mut result, join);
        }
        push_distinct_point(&mut result, segments[segments.len() - 1].end);
    }

    (result.len() >= 2).then_some(result)
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
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    // Plain sqrt over hypot's libm call; the finite guard keeps hypot's
    // overflow behavior (an overflowed length must drop the segment, not
    // silently offset by zero).
    let length = (dx * dx + dy * dy).sqrt();
    if length == 0.0 || !length.is_finite() {
        return None;
    }
    let offset_x = -dy / length * distance;
    let offset_y = dx / length * distance;
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
/// infinity. Shared by buffers (where over-limit spikes clip flat) and
/// offset curves (where they fall back to a bevel).
pub(crate) const DEFAULT_MITER_LIMIT: f64 = 5.0;

/// Buffer input-simplification tolerance as a fraction of `|distance|`. A
/// vertex deviating less than `|distance| * FACTOR` cannot move the offset
/// result beyond its arc-faceting error, so the noding/dissolve workload (and
/// output size) shed the vertices that do not shape the buffer. GEOS's
/// `BufferInputLineSimplifier` uses 0.01, but it is DIRECTION-AWARE (it only
/// removes vertices on the concave side the offset fills in, never the convex
/// side that shapes the outer boundary). This plain Douglas-Peucker is not
/// direction-aware, so it uses an order-of-magnitude tighter bound: 0.001 keeps
/// a smooth high-`quadrant_segments` arc faithful (its per-step sagitta stays above the
/// tolerance, so no boundary-shaping vertex is dropped) while still collapsing
/// the densely-sampled wiggles whose self-overlapping stroke is the real cost.
pub(crate) const BUFFER_INPUT_SIMPLIFY_FACTOR: f64 = 0.001;

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

pub(crate) fn offset_join(previous: OffsetEdge, current: OffsetEdge, vertex: Point) -> OffsetJoin {
    let squared = |a: Point, b: Point| {
        let dx = a.x - b.x;
        let dy = a.y - b.y;
        dx * dx + dy * dy
    };
    let carry = |point: XY| {
        vertex
            .with_xy(point.x, point.y)
            .expect("join point preserves finite vertex ordinates")
    };
    // |previous.end - vertex| is exactly the offset width, so the miter
    // limit needs no extra parameter.
    let limit = DEFAULT_MITER_LIMIT * DEFAULT_MITER_LIMIT * squared(previous.end, vertex);
    line_intersection(
        Segment {
            start: previous.start.xy(),
            end: previous.end.xy(),
        },
        Segment {
            start: current.start.xy(),
            end: current.end.xy(),
        },
    )
    .filter(|point| {
        let dx = point.x - vertex.x;
        let dy = point.y - vertex.y;
        dx * dx + dy * dy <= limit
    })
    .map_or_else(
        // Near-parallel segments make both bevel points coincide, so
        // the straight-through join degenerates back to a single point.
        || OffsetJoin::Bevel {
            incoming: carry(previous.end.xy()),
            outgoing: carry(current.start.xy()),
        },
        |point| OffsetJoin::Miter(carry(point)),
    )
}

pub(crate) fn line_intersection(left: Segment, right: Segment) -> Option<XY> {
    let left_dx = left.end.x - left.start.x;
    let left_dy = left.end.y - left.start.y;
    let right_dx = right.end.x - right.start.x;
    let right_dy = right.end.y - right.start.y;
    let denominator = left_dx * right_dy - left_dy * right_dx;
    if denominator.abs() <= 1e-12 {
        return None;
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
        let distance = Finite::try_new("distance", distance)?.get();
        if same_topological_coordinate(distance, 0.0) {
            return match self {
                Self::LineString(_) | Self::MultiLineString(_) => Ok(self.clone()),
                Self::GeometryCollection(geometries) => Ok(Self::GeometryCollection(
                    geometries
                        .iter()
                        .map(|geometry| {
                            geometry.offset_curve(
                                distance,
                                join_style,
                                quadrant_segments,
                                miter_limit,
                            )
                        })
                        .collect::<Result<_, _>>()?,
                )),
                _ => Err(GeometryErrorKind::LinealRequired.into()),
            };
        }
        validate_offset_expansion(self, quadrant_segments)?;
        let rule = WalkJoinRule::new(join_style, miter_limit.get());
        let offset = |line: &CoordSeq| -> Option<Vec<Point>> {
            // The planned walk owns every style; degenerate plans
            // (near-folds the robust orientation rejects) fall back to the
            // raw miter emitter, exactly like the buffer falls back to the
            // geo engine.
            offset_curve_points(line, distance, rule, quadrant_segments)
                .or_else(|| offset_line(line, distance))
        };
        Ok(match self {
            Self::LineString(points) => offset(points).map_or_else(
                || Self::LineString(LineSeq::empty(CoordinateAxes::XY)),
                |line| {
                    Self::LineString(
                        LineSeq::try_new(CoordSeq::from(line))
                            .expect("offset curve has at least two vertices"),
                    )
                },
            ),
            Self::MultiLineString(lines) => {
                let offsets = lines
                    .iter()
                    .filter_map(|line| offset(line))
                    .map(CoordSeq::from)
                    .map(|line| {
                        LineSeq::try_new(line).expect("offset curve has at least two vertices")
                    })
                    .collect::<Vec<_>>();
                Self::MultiLineString(offsets)
            },
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| {
                        geometry.offset_curve(distance, join_style, quadrant_segments, miter_limit)
                    })
                    .collect::<Result<_, _>>()?,
            ),
            _ => return Err(GeometryErrorKind::LinealRequired.into()),
        })
    }
}
