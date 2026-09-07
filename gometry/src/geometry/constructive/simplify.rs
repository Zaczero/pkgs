#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::simd::Select as _;
use std::simd::cmp::SimdPartialOrd as _;

use crate::NonNegative;
use crate::error::Result;
use crate::geometry::constructive::{WalkJoinRule, WalkPlan, materialize_walk, vw_keep};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, ExpansionBudget, GeometryErrorKind, GeometryKind, LineSeq,
    MultiLineString, MultiMember, MultiPolygon, Orientation, Point, PointKey, Polygon, REDUCE_LANES,
    ReduceSimd, Ring, Segment, Shape, TopologyGuard, XY, dedup_consecutive_points, line_segments,
    open_point_cycle_winding, orientation, point_on_segment, point_segment_distance,
    point_segment_distance_squared, same_point, simplified_polygon_delta_is_simple,
    simplify_chain_guarded, triangle_area,
    vw_area_tolerance, wrap_index,
};

/// The Douglas-Peucker keep mask over raw columns: `keep` and `stack` are
/// cleared and refilled (reusable across packed-array rows); `Some(kept_count)`
/// unless the chain is too short to simplify. The packed-array lane appends
/// kept vertices straight into new CSR columns from this mask. `stack` must
/// stay batch-local — never a receiver cache or lock (free-threading).
pub(crate) fn rdp_keep(
    xs: &[f64],
    ys: &[f64],
    tolerance: f64,
    keep: &mut Vec<bool>,
    stack: &mut Vec<(usize, usize)>,
) -> Option<usize> {
    let count = xs.len();
    if count <= 2 {
        return None;
    }
    let tolerance_squared = tolerance * tolerance;
    keep.clear();
    keep.resize(count, false);
    keep[0] = true;
    keep[count - 1] = true;
    let mut kept = 2_usize;
    stack.clear();
    stack.push((0_usize, count - 1));
    while let Some((start, end)) = stack.pop() {
        if end - start < 2 {
            continue;
        }
        if let Some(farthest) = rdp_split(xs, ys, start, end, tolerance, tolerance_squared) {
            keep[farthest] = true;
            kept += 1;
            stack.push((start, farthest));
            stack.push((farthest, end));
        }
    }
    Some(kept)
}

/// The farthest interior vertex of one Douglas-Peucker range, when its
/// deviation exceeds the tolerance. Wide branch-free lanes carry the
/// bulk (the scalar loop compiles to serialized `mulsd`); among EQUAL
/// maxima the surviving split vertex may differ from scalar order — any
/// farthest vertex is a correct split.
///
/// `tolerance` is the raw distance-scale threshold (must stay available when
/// `tolerance_squared` overflows to +inf — recovery from tol² alone is
/// impossible).
#[expect(
    clippy::too_many_lines,
    reason = "SIMD + unsquared cold path must stay one decision tree"
)]
pub(crate) fn rdp_split(
    xs: &[f64],
    ys: &[f64],
    start: usize,
    end: usize,
    tolerance: f64,
    tolerance_squared: f64,
) -> Option<usize> {
    let (ax, ay) = (xs[start], ys[start]);
    let (bx, by) = (xs[end], ys[end]);
    let (dx, dy) = (bx - ax, by - ay);
    let chord_squared = dx * dx + dy * dy;
    let threshold = tolerance_squared * chord_squared;
    let mut best = 0.0_f64;
    let mut farthest = start;
    let exact_scan = |best: &mut f64, farthest: &mut usize| {
        let chord = Segment {
            start: XY::new(ax, ay),
            end: XY::new(bx, by),
        };
        *best = f64::NEG_INFINITY;
        for index in start + 1..end {
            let deviation = point_segment_distance_squared(XY::new(xs[index], ys[index]), chord);
            if deviation > *best {
                *best = deviation;
                *farthest = index;
            }
        }
    };
    // When tolerance² underflows/overflows or the chord collapses, recover
    // with unsquared distances against the raw (positive) tolerance. Once
    // only tolerance² survives the public API, recovery is impossible — so
    // callers pass both.
    if !threshold.is_finite()
        || threshold == 0.0
        || !threshold.is_normal()
        || chord_squared <= 0.0
        || !tolerance_squared.is_normal()
    {
        // Prefer the raw finite distance tolerance (survives when tol² is
        // +inf). Fall back to sqrt of a finite square; +inf/NaN/negative
        // raw means "no finite threshold" → never split (endpoints only).
        let tol = if tolerance > 0.0 && tolerance.is_finite() {
            tolerance
        } else if tolerance_squared.is_normal() {
            tolerance_squared.sqrt()
        } else if tolerance_squared > 0.0 && tolerance_squared.is_finite() {
            tolerance_squared.sqrt().max(f64::MIN_POSITIVE)
        } else if tolerance == 0.0 {
            let chord = Segment {
                start: XY::new(ax, ay),
                end: XY::new(bx, by),
            };
            let mut split = None;
            let mut best_d = f64::NAN;
            for index in start + 1..end {
                let point = XY::new(xs[index], ys[index]);
                if point_on_segment(point, chord.start, chord.end) {
                    continue;
                }
                let d = point_segment_distance(point, chord);
                if split.is_none() || d > best_d {
                    split = Some(index);
                    best_d = d;
                }
            }
            return split;
        } else {
            return None;
        };
        let chord = Segment {
            start: XY::new(ax, ay),
            end: XY::new(bx, by),
        };
        let mut best_d = f64::NEG_INFINITY;
        for index in start + 1..end {
            let d = point_segment_distance(XY::new(xs[index], ys[index]), chord);
            if d > best_d {
                best_d = d;
                farthest = index;
            }
        }
        return (best_d > tol).then_some(farthest);
    }
    let mut index = start + 1;
    if end - index >= REDUCE_LANES {
        let lane_offsets =
            std::simd::Simd::<u64, REDUCE_LANES>::from_array(std::array::from_fn(|lane| {
                lane as u64
            }));
        let (ax_s, ay_s) = (ReduceSimd::splat(ax), ReduceSimd::splat(ay));
        let (bx_s, by_s) = (ReduceSimd::splat(bx), ReduceSimd::splat(by));
        let (dx_s, dy_s) = (ReduceSimd::splat(dx), ReduceSimd::splat(dy));
        let chord_s = ReduceSimd::splat(chord_squared);
        let mut best_scores = ReduceSimd::splat(0.0);
        let mut best_lanes = std::simd::Simd::<u64, REDUCE_LANES>::splat(0);
        let mut cursor = std::simd::Simd::<u64, REDUCE_LANES>::splat(index as u64) + lane_offsets;
        while index + REDUCE_LANES <= end {
            let x = ReduceSimd::from_slice(&xs[index..index + REDUCE_LANES]);
            let y = ReduceSimd::from_slice(&ys[index..index + REDUCE_LANES]);
            let (qx, qy) = (x - ax_s, y - ay_s);
            let along = qx * dx_s + qy * dy_s;
            let cross = qx * dy_s - qy * dx_s;
            let (rx, ry) = (x - bx_s, y - by_s);
            let score = along.simd_le(ReduceSimd::splat(0.0)).select(
                (qx * qx + qy * qy) * chord_s,
                along
                    .simd_ge(chord_s)
                    .select((rx * rx + ry * ry) * chord_s, cross * cross),
            );
            let improved = score.simd_gt(best_scores);
            best_scores = improved.select(score, best_scores);
            best_lanes = improved.cast::<i64>().select(cursor, best_lanes);
            cursor += std::simd::Simd::splat(REDUCE_LANES as u64);
            index += REDUCE_LANES;
        }
        for lane in 0..REDUCE_LANES {
            if best_scores[lane] > best {
                best = best_scores[lane];
                farthest = best_lanes[lane] as usize;
            }
        }
    }
    for scalar_index in index..end {
        let (qx, qy) = (xs[scalar_index] - ax, ys[scalar_index] - ay);
        let along = qx * dx + qy * dy;
        let score = if along <= 0.0 {
            (qx * qx + qy * qy) * chord_squared
        } else if along >= chord_squared {
            let (rx, ry) = (xs[scalar_index] - bx, ys[scalar_index] - by);
            (rx * rx + ry * ry) * chord_squared
        } else {
            let cross = qx * dy - qy * dx;
            cross * cross
        };
        if score > best {
            best = score;
            farthest = scalar_index;
        }
    }
    if !best.is_finite() {
        exact_scan(&mut best, &mut farthest);
        return (best > tolerance_squared).then_some(farthest);
    }
    (best > threshold).then_some(farthest)
}

/// Clean strictly-turning vertex cycle of a ring: closing duplicate,
/// repeated vertices, and collinear vertices stripped (they contribute
/// nothing to the offset and would emit zero-sweep duplicate joins);
/// oriented CCW (`clockwise = false`, shells) or CW (holes), so the ring
/// interior side is consistent for the offset walk. `None` below 3 strict
/// vertices.
pub(crate) fn strict_cycle(coords: &CoordSeq, clockwise: bool) -> Option<Vec<Point>> {
    let mut ring: Vec<Point> = coords.iter_coords().collect();
    dedup_consecutive_points(&mut ring);
    if ring.len() > 1 && same_point(ring[0], ring[ring.len() - 1]) {
        ring.pop();
    }
    if ring.len() < 3 {
        return None;
    }
    if open_point_cycle_winding(&ring).reverse_for_clockwise(clockwise) {
        ring.reverse();
    }
    let strict: Vec<Point> = (0..ring.len())
        .filter(|&index| {
            let previous = ring[wrap_index(index + ring.len() - 1, ring.len())];
            let next = ring[wrap_index(index + 1, ring.len())];
            orientation(previous, ring[index], next) != Orientation::Collinear
        })
        .map(|index| ring[index])
        .collect();
    (strict.len() >= 3).then_some(strict)
}

/// Raw offset loop of a strictly-turning cycle walked with the interior on
/// the LEFT: each edge offsets to the right (outward) by `distance`; convex
/// (counter-clockwise) turns join with an inscribed arc of step
/// `<= pi/2/quadrant_segments`, reflex turns with a clipped miter where the offset
/// edges still meet, otherwise THROUGH the original vertex (the standard
/// raw-offset excursion — the winding arrangement cancels it). Open column
/// form; the caller closes.
pub(crate) fn raw_offset_loop(
    strict: &[Point],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<(Vec<f64>, Vec<f64>)>> {
    // A finite endpoint pair can still produce an infinite delta before the
    // normal is computed. This is a failed derived calculation, not a
    // geometric fallback: the public buffer must not turn it into EMPTY.
    if strict.iter().enumerate().any(|(index, from)| {
        let to = strict[(index + 1) % strict.len()];
        !(to.x - from.x).is_finite() || !(to.y - from.y).is_finite()
    }) {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    let quadrant_segments = quadrant_segments.get();
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments);
    let Some(plan) = WalkPlan::new(strict, true, distance, rule, step_angle) else {
        return Ok(None);
    };
    // Capacity: two offset points per edge, up to one reflex insert per
    // vertex, and one full turn (4 quadrants) of arc steps.
    let Some(columns) = materialize_walk(&plan, step_angle, false, budget, true)? else {
        return Ok(None);
    };
    let (xs, ys, _) = columns.into_columns();
    Ok(Some((xs, ys)))
}

/// Validate buffer style parameters before touching the winding engine.
pub(crate) const fn validate_buffer_style(
    quadrant_segments: std::num::NonZeroU32,
    miter_limit: crate::Positive,
) {
    let _ = (quadrant_segments, miter_limit);
}

/// The two halves of `bounds` cut at `cut` across the chosen axis.
pub(crate) const fn axis_halves(bounds: Bounds, split_x: bool, cut: f64) -> [Bounds; 2] {
    let (minx, miny, maxx, maxy) = bounds.into_tuple();
    if split_x {
        [
            Bounds::new_raw(minx, miny, cut, maxy),
            Bounds::new_raw(cut, miny, maxx, maxy),
        ]
    } else {
        [
            Bounds::new_raw(minx, miny, maxx, cut),
            Bounds::new_raw(minx, cut, maxx, maxy),
        ]
    }
}

/// A cut between the two distinct vertex ordinates straddling the
/// sorted median — both sides keep at least one vertex strictly,
/// so a clip sheds vertices regardless of how skewed the bounds
/// are. `None` when the axis has fewer than two distinct ordinates
/// or the midpoint collapses onto an endpoint.
pub(crate) fn median_cut(shape: &Shape, split_x: bool, bounds: Bounds) -> Option<f64> {
    let mut ordinates = Vec::with_capacity(shape.coord_count());
    shape.for_each_point(|point| ordinates.push(if split_x { point.x } else { point.y }));
    ordinates.sort_unstable_by(f64::total_cmp);
    let middle = ordinates.len() / 2;
    let pair = (1..ordinates.len())
        .filter(|&index| ordinates[index - 1] < ordinates[index])
        .min_by_key(|&index| index.abs_diff(middle))?;
    let cut = f64::midpoint(ordinates[pair - 1], ordinates[pair]);
    let (low, high) = if split_x {
        (bounds.minx(), bounds.maxx())
    } else {
        (bounds.miny(), bounds.maxy())
    };
    (low < cut && cut < high).then_some(cut)
}

pub(crate) fn split(
    shape: Shape,
    max_vertices: usize,
    drop: bool,
    out: &mut Vec<Shape>,
) -> Result<()> {
    if shape.is_empty() {
        return Ok(());
    }
    let count = shape.coord_count();
    if count <= max_vertices {
        out.push(shape);
        return Ok(());
    }
    let Some(bounds) = shape.bounds() else {
        return Ok(());
    };
    // Cut the longer axis at its midpoint, falling back to the other
    // axis when the float midpoint cannot strictly split the
    // interval; a cluster degenerate on both axes cannot be cut.
    let x_mid = f64::midpoint(bounds.minx(), bounds.maxx());
    let y_mid = f64::midpoint(bounds.miny(), bounds.maxy());
    let x_splits = bounds.minx() < x_mid && x_mid < bounds.maxx();
    let y_splits = bounds.miny() < y_mid && y_mid < bounds.maxy();
    if !x_splits && !y_splits {
        out.push(shape);
        return Ok(());
    }
    let x_longer = bounds.maxx() - bounds.minx() >= bounds.maxy() - bounds.miny();
    let split_x = if x_longer {
        x_splits
    } else {
        !y_splits && x_splits
    };
    let cut = if split_x { x_mid } else { y_mid };
    let halves = axis_halves(bounds, split_x, cut);
    let clipped = [
        shape.clip_by_rect(halves[0], drop)?,
        shape.clip_by_rect(halves[1], drop)?,
    ];
    // A midpoint cut that fails to shed vertices on some half (cut
    // vertices appearing as fast as detail separates — the dense-
    // cluster-in-huge-bounds shape) would burn one recursion per
    // float halving of the bounds; a cut that loses the shape entirely
    // (extreme-aspect clips can numerically drop both halves) must never
    // drop coverage. Both re-cut at the vertex median, which always
    // sheds. A half the median cut *still* cannot shrink (every dropped
    // vertex traded for a crossing) is spatially unsplittable: emit it.
    let stalled = clipped.iter().any(|half| half.coord_count() >= count)
        || clipped.iter().all(Shape::is_empty);
    if stalled {
        let median = median_cut(&shape, split_x, bounds)
            .map(|cut| (split_x, cut))
            .or_else(|| {
                let other = !split_x;
                median_cut(&shape, other, bounds).map(|cut| (other, cut))
            });
        let Some((split_x, cut)) = median else {
            out.push(shape);
            return Ok(());
        };
        let halves = axis_halves(bounds, split_x, cut);
        let halves = [
            shape.clip_by_rect(halves[0], drop)?,
            shape.clip_by_rect(halves[1], drop)?,
        ];
        if halves.iter().all(Shape::is_empty) {
            // The median cut lost coverage too: emit the input whole
            // rather than silently dropping it.
            out.push(shape);
            return Ok(());
        }
        for half in halves {
            if half.is_empty() {
                continue;
            }
            if half.coord_count() >= count {
                out.push(half);
            } else {
                split(half, max_vertices, drop, out)?;
            }
        }
        return Ok(());
    }
    for half in clipped {
        if !half.is_empty() {
            split(half, max_vertices, drop, out)?;
        }
    }
    Ok(())
}

/// Whether `coords` contains at least three XY-distinct vertices under
/// [`PointKey`] equality (±0.0 unified, NaN by payload bits).
fn has_three_distinct_xy(coords: &CoordSeq) -> bool {
    let mut first = None;
    let mut second = None;
    for (&x, &y) in coords.xs().iter().zip(coords.ys()) {
        let key = PointKey::new(XY::new(x, y));
        match (first, second) {
            (None, _) => first = Some(key),
            (Some(a), None) if key != a => second = Some(key),
            (Some(a), Some(b)) if key != a && key != b => return true,
            _ => {},
        }
    }
    false
}

/// Operation-local keep-count identity. `Unchanged` means every chain kept
/// every vertex, so the caller clones `self` instead of staging output.
enum RawSimplify {
    Unchanged,
    Changed(Shape),
}

impl RawSimplify {
    fn into_shape(self, original: &Shape) -> Shape {
        match self {
            Self::Unchanged => original.clone(),
            Self::Changed(shape) => shape,
        }
    }
}

#[derive(Default)]
struct KeepScratch {
    keep: Vec<bool>,
    stack: Vec<(usize, usize)>,
}

/// One keep-mask pass. `None` when the kernel says the chain is too short or
/// every vertex survives — reuse the input chain, do not `select`.
fn kept_coords(
    points: &CoordSeq,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Option<usize>,
) -> Option<CoordSeq> {
    let kept = keep_mask(points.xs(), points.ys(), scratch)?;
    (kept != points.len()).then(|| {
        points.select(
            scratch
                .keep
                .iter()
                .enumerate()
                .filter_map(|(index, &kept)| kept.then_some(index)),
        )
    })
}

enum RingKeep {
    Same,
    Drop,
    New(Ring),
}

fn keep_ring(
    ring: &Ring,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Option<usize>,
) -> RingKeep {
    match kept_coords(ring.coords(), scratch, keep_mask) {
        None => RingKeep::Same,
        Some(coords) if coords.len() < Ring::MIN_VERTICES_CLOSED => RingKeep::Drop,
        Some(coords) => RingKeep::New(Ring::from_closed(coords)),
    }
}

enum PolygonKeep {
    Same,
    Empty,
    New(Polygon),
}

fn try_clone_ring_prefix(holes: &[Ring], end: usize) -> Result<Vec<Ring>> {
    let mut out = Vec::new();
    crate::try_reserve_vec(&mut out, end)?;
    for hole in &holes[..end] {
        crate::try_push_vec(&mut out, hole.clone())?;
    }
    Ok(out)
}

fn keep_polygon(
    polygon: &Polygon,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Option<usize>,
) -> Result<PolygonKeep> {
    let new_shell = match keep_ring(&polygon.shell, scratch, keep_mask) {
        RingKeep::Drop => return Ok(PolygonKeep::Empty),
        RingKeep::Same => None,
        RingKeep::New(shell) => Some(shell),
    };
    let mut holes_out = if new_shell.is_some() {
        let mut holes = Vec::new();
        crate::try_reserve_vec(&mut holes, polygon.holes.len())?;
        Some(holes)
    } else {
        None
    };
    for (index, hole) in polygon.holes.iter().enumerate() {
        match keep_ring(hole, scratch, keep_mask) {
            RingKeep::Same => {
                if let Some(holes) = holes_out.as_mut() {
                    crate::try_push_vec(holes, hole.clone())?;
                }
            },
            RingKeep::Drop => {
                if holes_out.is_none() {
                    holes_out = Some(try_clone_ring_prefix(&polygon.holes, index)?);
                }
            },
            RingKeep::New(ring) => {
                if let Some(holes) = holes_out.as_mut() {
                    crate::try_push_vec(holes, ring)?;
                } else {
                    let mut holes = try_clone_ring_prefix(&polygon.holes, index)?;
                    crate::try_push_vec(&mut holes, ring)?;
                    holes_out = Some(holes);
                }
            },
        }
    }
    Ok(match holes_out {
        None => PolygonKeep::Same,
        Some(holes) => PolygonKeep::New(Polygon::new(
            new_shell.unwrap_or_else(|| polygon.shell.clone()),
            holes,
        )),
    })
}

fn try_prefix_line_members(
    lines: &MultiLineString,
    end: usize,
) -> Result<Vec<MultiMember<LineSeq>>> {
    let mut out = Vec::new();
    crate::try_reserve_vec(&mut out, lines.len())?;
    for member in lines.members().take(end) {
        let item = match member {
            MultiMember::Empty => MultiMember::Empty,
            MultiMember::Value(value) => MultiMember::Value(lines.as_slice()[value].clone()),
        };
        crate::try_push_vec(&mut out, item)?;
    }
    Ok(out)
}

fn keep_multi_lines(
    lines: &MultiLineString,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Option<usize>,
) -> Result<RawSimplify> {
    let mut out: Option<Vec<MultiMember<LineSeq>>> = None;
    for (index, member) in lines.members().enumerate() {
        match member {
            MultiMember::Empty => {
                if let Some(members) = out.as_mut() {
                    crate::try_push_vec(members, MultiMember::Empty)?;
                }
            },
            MultiMember::Value(value) => {
                let line = &lines.as_slice()[value];
                match kept_coords(line, scratch, keep_mask) {
                    None => {
                        if let Some(members) = out.as_mut() {
                            crate::try_push_vec(members, MultiMember::Value(line.clone()))?;
                        }
                    },
                    Some(coords) => {
                        if let Some(members) = out.as_mut() {
                            crate::try_push_vec(
                                members,
                                MultiMember::Value(LineSeq::from_proven(coords)),
                            )?;
                        } else {
                            let mut members = try_prefix_line_members(lines, index)?;
                            crate::try_push_vec(
                                &mut members,
                                MultiMember::Value(LineSeq::from_proven(coords)),
                            )?;
                            out = Some(members);
                        }
                    },
                }
            },
        }
    }
    Ok(match out {
        None => RawSimplify::Unchanged,
        Some(members) => RawSimplify::Changed(Shape::MultiLineString(MultiLineString::from_values(
            lines.axes(),
            members,
        ))),
    })
}

fn try_prefix_polygon_members(
    polygons: &MultiPolygon,
    end: usize,
) -> Result<Vec<MultiMember<Polygon>>> {
    let mut out = Vec::new();
    crate::try_reserve_vec(&mut out, polygons.len())?;
    for member in polygons.members().take(end) {
        let item = match member {
            MultiMember::Empty => MultiMember::Empty,
            MultiMember::Value(value) => MultiMember::Value(polygons.as_slice()[value].clone()),
        };
        crate::try_push_vec(&mut out, item)?;
    }
    Ok(out)
}

fn keep_multi_polygons(
    polygons: &MultiPolygon,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Option<usize>,
) -> Result<RawSimplify> {
    let mut out: Option<Vec<MultiMember<Polygon>>> = None;
    for (index, member) in polygons.members().enumerate() {
        match member {
            MultiMember::Empty => {
                if let Some(members) = out.as_mut() {
                    crate::try_push_vec(members, MultiMember::Empty)?;
                }
            },
            MultiMember::Value(value) => {
                let polygon = &polygons.as_slice()[value];
                match keep_polygon(polygon, scratch, keep_mask)? {
                    PolygonKeep::Same => {
                        if let Some(members) = out.as_mut() {
                            crate::try_push_vec(members, MultiMember::Value(polygon.clone()))?;
                        }
                    },
                    PolygonKeep::Empty => {
                        if let Some(members) = out.as_mut() {
                            crate::try_push_vec(members, MultiMember::Empty)?;
                        } else {
                            let mut members = try_prefix_polygon_members(polygons, index)?;
                            crate::try_push_vec(&mut members, MultiMember::Empty)?;
                            out = Some(members);
                        }
                    },
                    PolygonKeep::New(polygon) => {
                        if let Some(members) = out.as_mut() {
                            crate::try_push_vec(members, MultiMember::Value(polygon))?;
                        } else {
                            let mut members = try_prefix_polygon_members(polygons, index)?;
                            crate::try_push_vec(&mut members, MultiMember::Value(polygon))?;
                            out = Some(members);
                        }
                    },
                }
            },
        }
    }
    Ok(match out {
        None => RawSimplify::Unchanged,
        Some(members) => RawSimplify::Changed(Shape::MultiPolygon(MultiPolygon::from_values(
            polygons.axes(),
            members,
        ))),
    })
}

impl Shape {
    /// Douglas-Peucker simplification. With `preserve_topology` (the
    /// default), an output that raw DP made invalid (a collapsed or
    /// self-intersecting ring) or non-simple (a previously simple line now
    /// crossing itself) is recomputed by guarded greedy removal: vertices
    /// whose chord deviation is within `tolerance` drop smallest-first, but
    /// only when the shortcut neither crosses surviving linework nor sweeps
    /// another vertex — same-shape output, always topology-safe. The raw
    /// pass is the fast path; the kept-topology check is the BRANCH
    /// CONDITION for the guard, not defensive ceremony — without it every
    /// call would pay the slow guarded walk — and it runs on the REDUCED
    /// output through the indexed validity kernel, so it is cheap.
    pub fn simplify_dp(&self, tolerance: f64, preserve_topology: bool) -> Result<Self> {
        match self.simplify_raw_dp(tolerance)? {
            RawSimplify::Unchanged => {
                if preserve_topology {
                    Ok(self.clone())
                } else {
                    self.clone().dropping_collapsed_rings()
                }
            },
            RawSimplify::Changed(raw) => {
                // Without the topology guard, raw DP can collapse a polygon ring
                // below its three distinct corners (a sliver) — a degenerate
                // INVALID polygon. Drop collapsed areal rings so a vanished
                // polygon is `POLYGON EMPTY` (matching Shapely), never an
                // invalid 2-vertex ring.
                if !preserve_topology {
                    return raw.dropping_collapsed_rings();
                }
                // Keep-count identity already returned `Unchanged`. The validity
                // branch-check only runs when vertices actually dropped.
                if self.simplify_kept_topology(&raw) {
                    Ok(raw)
                } else {
                    self.simplify_guarded(tolerance)
                }
            },
        }
    }

    /// Drop areal rings that simplification collapsed below three distinct
    /// corners (they enclose no area): a vanished shell makes the polygon
    /// `POLYGON EMPTY`, a vanished hole is removed; lineal/puntal parts are
    /// untouched. Keeps `simplify`/`simplify_vw` with `preserve_topology=False`
    /// from leaking a degenerate invalid polygon.
    fn dropping_collapsed_rings(self) -> Result<Self> {
        fn clean(polygon: &Polygon) -> Option<Polygon> {
            if !has_three_distinct_xy(polygon.shell.coords()) {
                return None;
            }
            Some(Polygon::new(
                polygon.shell.clone(),
                polygon
                    .holes
                    .iter()
                    .filter(|hole| has_three_distinct_xy(hole.coords()))
                    .cloned()
                    .collect(),
            ))
        }
        Ok(match self {
            Self::Polygon(polygon) => clean(&polygon).map_or_else(
                || Self::typed_empty(GeometryKind::Polygon, polygon.axes()),
                Self::Polygon,
            ),
            Self::MultiPolygon(polygons) => {
                let members = polygons
                    .members()
                    .filter_map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Some(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(index) => {
                            clean(&polygons.as_slice()[index])
                                .map(crate::geometry::MultiMember::Value)
                                .or(Some(crate::geometry::MultiMember::Empty))
                        },
                    })
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    Self::empty_polygon()
                } else {
                    Self::MultiPolygon(MultiPolygon::from_values(polygons.axes(), members))
                }
            },
            Self::GeometryCollection(parts) => Self::GeometryCollection(
                parts.try_map_owned_members(Self::dropping_collapsed_rings)?,
            ),
            other => other,
        })
    }

    /// Whether raw DP output preserved what `preserve_topology` promises:
    /// polygonal output stays valid and non-collapsed; a simple lineal
    /// input stays simple.
    fn simplify_kept_topology(&self, simplified: &Self) -> bool {
        match self {
            Self::Polygon(_) | Self::MultiPolygon(_) => {
                // Delta guard: vertex-subset removals can only break the
                // NEW chord edges (for the contract's valid-input
                // promise), so the chords are checked instead of
                // re-validating the whole polygon.
                simplified_polygon_delta_is_simple(self, simplified)
                    && (self.is_empty() || !simplified.is_empty())
            },
            Self::LineString(_) | Self::MultiLineString(_) => {
                !self.is_simple() || simplified.is_simple()
            },
            Self::GeometryCollection(geometries) => {
                let Self::GeometryCollection(parts) = simplified else {
                    return false;
                };
                geometries
                    .iter()
                    .zip(parts)
                    .all(|(original, part)| original.simplify_kept_topology(part))
            },
            _ => true,
        }
    }

    /// Guarded greedy simplification: per chain (line or ring), repeatedly
    /// remove the vertex with the smallest chord deviation `<= tolerance`
    /// whose removal the topology guard accepts.
    fn simplify_guarded(&self, tolerance: f64) -> Result<Self> {
        self.simplify_guarded_with(|prev, vertex, next| {
            let deviation = point_segment_distance(vertex, Segment {
                start: prev.into(),
                end: next.into(),
            });
            (deviation <= tolerance).then_some(deviation)
        })
    }

    /// Guarded greedy simplification over every chain with a pluggable
    /// importance criterion (chord deviation for Douglas-Peucker, triangle
    /// area for Visvalingam-Whyatt): the least important removable vertex
    /// the topology guard accepts drops first.
    fn simplify_guarded_with(
        &self,
        candidate: impl Fn(Point, Point, Point) -> Option<f64>,
    ) -> Result<Self> {
        let mut segments = Vec::new();
        self.for_each_segment_chain(|chain| segments.extend(line_segments(chain)));
        let guard = TopologyGuard::new(&segments, self.points_vec().iter().map(Point::xy));
        self.map_chains(&|points: &CoordSeq| {
            let alive: Vec<Point> = points.iter_coords().collect();
            let closed = alive.len() >= 2
                && alive
                    .first()
                    .zip(alive.last())
                    .is_some_and(|(first, last)| same_point(*first, *last));
            Some(simplify_chain_guarded(&alive, closed, &candidate, &guard))
        })
    }

    pub fn simplify_dp_raw(&self, tolerance: f64) -> Result<Self> {
        Ok(self.simplify_raw_dp(tolerance)?.into_shape(self))
    }

    /// Visvalingam-Whyatt simplification with the same topology contract
    /// as `simplify`: raw VW is the fast path; a guarded greedy pass only
    /// kicks in when it broke validity or simplicity. `tolerance` is
    /// distance-scale — the effective-area threshold is `tolerance² / 2`,
    /// the same conversion `coverage_simplify` (and GEOS) uses — so the
    /// same value is directly comparable between `simplify`, `simplify_vw`,
    /// and `coverage_simplify`.
    pub fn simplify_vw(&self, tolerance: f64, preserve_topology: bool) -> Result<Self> {
        match self.simplify_raw_vw(tolerance)? {
            RawSimplify::Unchanged => {
                if preserve_topology {
                    Ok(self.clone())
                } else {
                    self.clone().dropping_collapsed_rings()
                }
            },
            RawSimplify::Changed(raw) => {
                if !preserve_topology {
                    return raw.dropping_collapsed_rings();
                }
                if self.simplify_kept_topology(&raw) {
                    Ok(raw)
                } else {
                    let area_tolerance = vw_area_tolerance(tolerance);
                    self.simplify_guarded_with(|prev, vertex, next| {
                        let area = triangle_area(prev, vertex, next);
                        (area < area_tolerance).then_some(area)
                    })
                }
            },
        }
    }

    pub fn simplify_vw_raw(&self, tolerance: f64) -> Result<Self> {
        Ok(self.simplify_raw_vw(tolerance)?.into_shape(self))
    }

    fn simplify_raw_dp(&self, tolerance: f64) -> Result<RawSimplify> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let mut scratch = KeepScratch::default();
        self.simplify_raw(&mut scratch, &mut |xs, ys, scratch| {
            rdp_keep(xs, ys, tolerance, &mut scratch.keep, &mut scratch.stack)
        })
    }

    fn simplify_raw_vw(&self, tolerance: f64) -> Result<RawSimplify> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let mut scratch = KeepScratch::default();
        self.simplify_raw(&mut scratch, &mut |xs, ys, scratch| {
            vw_keep(xs, ys, tolerance, &mut scratch.keep)
        })
    }

    fn simplify_raw(
        &self,
        scratch: &mut KeepScratch,
        keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Option<usize>,
    ) -> Result<RawSimplify> {
        Ok(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => RawSimplify::Unchanged,
            Self::LineString(points) => match kept_coords(points, scratch, keep_mask) {
                None => RawSimplify::Unchanged,
                Some(coords) => {
                    RawSimplify::Changed(Self::LineString(LineSeq::from_proven(coords)))
                },
            },
            Self::MultiLineString(lines) => keep_multi_lines(lines, scratch, keep_mask)?,
            Self::Polygon(polygon) => match keep_polygon(polygon, scratch, keep_mask)? {
                PolygonKeep::Same => RawSimplify::Unchanged,
                PolygonKeep::Empty => RawSimplify::Changed(Self::typed_empty(
                    GeometryKind::Polygon,
                    polygon.axes(),
                )),
                PolygonKeep::New(polygon) => RawSimplify::Changed(Self::Polygon(polygon)),
            },
            Self::MultiPolygon(polygons) => keep_multi_polygons(polygons, scratch, keep_mask)?,
            Self::GeometryCollection(geometries) => {
                let mut out: Option<Vec<Shape>> = None;
                for (index, geometry) in geometries.iter().enumerate() {
                    match geometry.simplify_raw(scratch, keep_mask)? {
                        RawSimplify::Unchanged => {
                            if let Some(members) = out.as_mut() {
                                crate::try_push_vec(members, geometry.clone())?;
                            }
                        },
                        RawSimplify::Changed(shape) => {
                            if let Some(members) = out.as_mut() {
                                crate::try_push_vec(members, shape)?;
                            } else {
                                let mut members = Vec::new();
                                crate::try_reserve_vec(&mut members, geometries.len())?;
                                for part in &geometries.as_slice()[..index] {
                                    crate::try_push_vec(&mut members, part.clone())?;
                                }
                                crate::try_push_vec(&mut members, shape)?;
                                out = Some(members);
                            }
                        },
                    }
                }
                match out {
                    None => RawSimplify::Unchanged,
                    Some(members) => RawSimplify::Changed(Self::GeometryCollection(
                        geometries.try_rebuild_members(members)?,
                    )),
                }
            },
        })
    }
}

#[cfg(test)]
mod collapsed_ring_distinct_tests {
    use std::sync::Arc;

    use super::has_three_distinct_xy;
    use crate::geometry::CoordSeq;
    use crate::test_support::fail_allocations_after;

    fn columns(xs: &[f64], ys: &[f64]) -> CoordSeq {
        CoordSeq::from_columns(Arc::from(xs), Arc::from(ys), None, None)
    }

    #[test]
    fn three_distinct_xy_counts_zero_two_and_three() {
        assert!(!has_three_distinct_xy(&columns(&[], &[])));
        assert!(!has_three_distinct_xy(&columns(&[0.0; 4], &[0.0; 4])));
        assert!(!has_three_distinct_xy(&columns(
            &[0.0, 1.0, 0.0, 1.0, 0.0],
            &[0.0, 0.0, 0.0, 0.0, 0.0],
        )));
        assert!(has_three_distinct_xy(&columns(&[0.0, 1.0, 0.0, 0.0], &[
            0.0, 0.0, 1.0, 0.0
        ],)));
    }

    #[test]
    fn three_collinear_xy_remain_distinct() {
        assert!(has_three_distinct_xy(&columns(&[0.0, 1.0, 2.0, 0.0], &[
            0.0, 0.0, 0.0, 0.0
        ],)));
    }

    #[test]
    fn signed_zero_shares_a_point_key() {
        assert!(!has_three_distinct_xy(&columns(&[0.0, -0.0, 0.0], &[
            0.0, 0.0, -0.0
        ],)));
    }

    #[test]
    fn three_distinct_xy_scan_allocates_nothing() {
        let two = columns(&[0.0, 1.0, 0.0, 1.0, 0.0], &[0.0; 5]);
        let three = columns(&[0.0, 1.0, 0.0, 0.0], &[0.0, 0.0, 1.0, 0.0]);
        fail_allocations_after(0, || {
            assert!(!has_three_distinct_xy(&two));
            assert!(has_three_distinct_xy(&three));
        });
    }
}

#[cfg(test)]
mod keep_identity_tests {
    use std::sync::Arc;

    use super::RawSimplify;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{
        CoordSeq, CoordinateAxes, GeometryKind, LineSeq, MultiLineString, MultiPolygon, Polygon,
        Ring, Shape,
    };
    use crate::test_support::fail_allocations_after;

    fn line_xy(xs: &[f64], ys: &[f64]) -> LineSeq {
        LineSeq::from_proven(CoordSeq::from_columns(
            Arc::from(xs),
            Arc::from(ys),
            None,
            None,
        ))
    }

    fn closed_ring(xs: &[f64], ys: &[f64]) -> Ring {
        Ring::from_closed(CoordSeq::from_columns(
            Arc::from(xs),
            Arc::from(ys),
            None,
            None,
        ))
    }

    fn square() -> Shape {
        Shape::Polygon(Polygon::new(
            closed_ring(&[0.0, 4.0, 4.0, 0.0, 0.0], &[0.0, 0.0, 4.0, 4.0, 0.0]),
            Vec::new(),
        ))
    }

    #[test]
    fn vw_zero_keep_all_shares_line_columns() {
        let line = Shape::LineString(line_xy(&[0.0, 1.0, 2.0], &[0.0, 1.0, 0.0]));
        let Shape::LineString(src) = &line else {
            panic!("line fixture");
        };
        let out = line.simplify_vw(0.0, true).unwrap();
        assert_eq!(out, line);
        let Shape::LineString(got) = &out else {
            panic!("keep-all remains a line");
        };
        assert_eq!(got.xs().as_ptr(), src.xs().as_ptr());
        assert_eq!(got.ys().as_ptr(), src.ys().as_ptr());
    }

    #[test]
    fn dp_zero_drops_collinear_interior() {
        let line = Shape::LineString(line_xy(&[0.0, 1.0, 2.0], &[0.0, 0.0, 0.0]));
        let Shape::LineString(out) = line.simplify_dp(0.0, true).unwrap() else {
            panic!("collinear DP remains a line");
        };
        assert_eq!(out.xs(), &[0.0, 2.0]);
        assert_eq!(out.ys(), &[0.0, 0.0]);
    }

    #[test]
    fn dp_below_feature_keep_all_on_a_corner() {
        let line = Shape::LineString(line_xy(&[0.0, 1.0, 2.0], &[0.0, 1.0, 0.0]));
        let Shape::LineString(src) = &line else {
            panic!("line fixture");
        };
        let out = line.simplify_dp(1e-9, true).unwrap();
        let Shape::LineString(got) = &out else {
            panic!("keep-all remains a line");
        };
        assert_eq!(got.xs().as_ptr(), src.xs().as_ptr());
        assert_eq!(out, line);
    }

    #[test]
    fn vw_zero_preserves_nan_and_signed_zero_bits() {
        let nan = f64::NAN;
        let line = Shape::LineString(LineSeq::from_proven(CoordSeq::from_columns(
            Arc::from([-0.0, 1.0, 2.0]),
            Arc::from([0.0, -0.0, 0.0]),
            Some(Arc::from([nan, 1.0, nan])),
            Some(Arc::from([0.0, nan, -0.0])),
        )));
        let out = line.simplify_vw(0.0, true).unwrap();
        assert_eq!(out, line);
        let Shape::LineString(src) = &line else {
            panic!("line fixture");
        };
        let Shape::LineString(got) = &out else {
            panic!("keep-all remains a line");
        };
        assert_eq!(got.xs().as_ptr(), src.xs().as_ptr());
        assert_eq!(got.zs().unwrap()[0].to_bits(), nan.to_bits());
        assert_eq!(got.xs()[0].to_bits(), (-0.0_f64).to_bits());
        assert_eq!(got.ms().unwrap()[2].to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn dp_zero_keeps_repeated_xy_with_distinct_zm() {
        let line = Shape::LineString(LineSeq::from_proven(CoordSeq::from_columns(
            Arc::from([0.0, 1.0, 0.0]),
            Arc::from([0.0, 0.0, 0.0]),
            Some(Arc::from([1.0, 2.0, 9.0])),
            Some(Arc::from([10.0, 20.0, 90.0])),
        )));
        let Shape::LineString(out) = line.simplify_dp(0.0, true).unwrap() else {
            panic!("A-B-A DP remains a line");
        };
        assert_eq!(out.xs(), &[0.0, 0.0]);
        assert_eq!(out.zs(), Some(&[1.0, 9.0][..]));
        assert_eq!(out.ms(), Some(&[10.0, 90.0][..]));
    }

    #[test]
    fn middle_hole_drop_clones_unchanged_prefix() {
        let shell = closed_ring(&[0.0, 8.0, 8.0, 0.0, 0.0], &[0.0, 0.0, 8.0, 8.0, 0.0]);
        let keep_a = closed_ring(&[1.0, 2.0, 2.0, 1.0, 1.0], &[1.0, 1.0, 2.0, 2.0, 1.0]);
        let drop = closed_ring(&[4.0, 6.0, 5.0, 4.0], &[4.0, 4.0, 4.01, 4.0]);
        let keep_b = closed_ring(&[1.0, 2.0, 2.0, 1.0, 1.0], &[5.0, 5.0, 6.0, 6.0, 5.0]);
        let a_ptr = keep_a.xs().as_ptr();
        let b_ptr = keep_b.xs().as_ptr();
        let polygon = Shape::Polygon(Polygon::new(shell, vec![keep_a, drop, keep_b]));
        // 0.5 keeps the 1×1 holes (diagonal offset ≈ 0.707) and drops the
        // 0.01-high spike below four closed vertices.
        let Shape::Polygon(out) = polygon.simplify_dp_raw(0.5).unwrap() else {
            panic!("shell remains a polygon");
        };
        assert_eq!(out.holes.len(), 2);
        assert_eq!(out.holes[0].xs().as_ptr(), a_ptr);
        assert_eq!(out.holes[1].xs().as_ptr(), b_ptr);
    }

    #[test]
    fn shell_drop_is_typed_empty() {
        let sliver = Shape::Polygon(Polygon::new(
            closed_ring(&[0.0, 1.0, 0.5, 0.0], &[0.0, 0.0, 0.1, 0.0]),
            Vec::new(),
        ));
        assert_eq!(
            sliver.simplify_dp_raw(1.0).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XY)
        );
    }

    #[test]
    fn collapsed_keep_all_drops_only_when_topology_is_off() {
        let collapsed = Shape::Polygon(Polygon::new(
            closed_ring(&[0.0, 0.0, 0.0, 0.0], &[0.0, 0.0, 0.0, 0.0]),
            Vec::new(),
        ));
        assert_eq!(collapsed.simplify_vw(0.0, true).unwrap(), collapsed);
        assert_eq!(
            collapsed.simplify_vw(0.0, false).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XY)
        );
    }

    #[test]
    fn middle_multiline_member_stages_after_first_change() {
        let first = line_xy(&[0.0, 0.0], &[0.0, 1.0]);
        let middle = line_xy(&[0.0, 1.0, 2.0], &[0.0, 0.0, 0.0]);
        let last = line_xy(&[3.0, 3.0], &[0.0, 1.0]);
        let first_ptr = first.xs().as_ptr();
        let last_ptr = last.xs().as_ptr();
        let shape = Shape::MultiLineString(
            MultiLineString::from_dense(vec![first, middle, last]).unwrap(),
        );
        let Shape::MultiLineString(out) = shape.simplify_dp(0.0, true).unwrap() else {
            panic!("multilinestring remains");
        };
        assert_eq!(out.as_slice()[0].xs().as_ptr(), first_ptr);
        assert_eq!(out.as_slice()[1].xs(), &[0.0, 2.0]);
        assert_eq!(out.as_slice()[2].xs().as_ptr(), last_ptr);
    }

    #[test]
    fn prefix_line_member_staging_reports_allocation_failure() {
        let first = line_xy(&[0.0, 0.0], &[0.0, 1.0]);
        let middle = line_xy(&[0.0, 1.0, 2.0], &[0.0, 0.0, 0.0]);
        let last = line_xy(&[3.0, 3.0], &[0.0, 1.0]);
        let lines = MultiLineString::from_dense(vec![first, middle, last]).unwrap();
        let result = fail_allocations_after(0, || super::try_prefix_line_members(&lines, 1));
        assert!(matches!(
            result,
            Err(error) if matches!(error.kind(), ErrorKind::Resource(ResourceError::Allocation))
        ));
    }

    #[test]
    fn empty_collection_is_unchanged() {
        let empty = Shape::geometry_collection(vec![]).unwrap();
        assert_eq!(empty.simplify_dp(1.0, true).unwrap(), empty);
        assert_eq!(empty.simplify_vw(1.0, false).unwrap(), empty);
        assert!(matches!(
            empty.simplify_raw_dp(1.0).unwrap(),
            RawSimplify::Unchanged
        ));
    }

    #[test]
    fn middle_collection_member_clones_prefix() {
        let first = Shape::LineString(line_xy(&[0.0, 0.0], &[0.0, 1.0]));
        let middle = Shape::LineString(line_xy(&[0.0, 1.0, 2.0], &[0.0, 0.0, 0.0]));
        let last = Shape::LineString(line_xy(&[3.0, 3.0], &[0.0, 1.0]));
        let Shape::LineString(first_line) = &first else {
            panic!("line fixture");
        };
        let first_ptr = first_line.xs().as_ptr();
        let collection = Shape::geometry_collection(vec![first, middle, last]).unwrap();
        let Shape::GeometryCollection(out) = collection.simplify_dp(0.0, true).unwrap() else {
            panic!("collection remains");
        };
        let Shape::LineString(got_first) = &out.as_slice()[0] else {
            panic!("first member remains a line");
        };
        assert_eq!(got_first.xs().as_ptr(), first_ptr);
        let Shape::LineString(got_middle) = &out.as_slice()[1] else {
            panic!("middle member remains a line");
        };
        assert_eq!(got_middle.xs(), &[0.0, 2.0]);
    }

    #[test]
    fn square_keep_all_is_unchanged_for_both_methods() {
        let polygon = square();
        assert!(matches!(
            polygon.simplify_raw_dp(1e-9).unwrap(),
            RawSimplify::Unchanged
        ));
        assert!(matches!(
            polygon.simplify_raw_vw(0.0).unwrap(),
            RawSimplify::Unchanged
        ));
        assert_eq!(polygon.simplify_dp(1e-9, true).unwrap(), polygon);
        assert_eq!(polygon.simplify_vw(0.0, false).unwrap(), polygon);
    }

    #[test]
    fn multipolygon_member_emptying_is_changed() {
        let kept = Polygon::new(
            closed_ring(&[0.0, 4.0, 4.0, 0.0, 0.0], &[0.0, 0.0, 4.0, 4.0, 0.0]),
            Vec::new(),
        );
        let sliver = Polygon::new(
            closed_ring(&[10.0, 11.0, 10.5, 10.0], &[0.0, 0.0, 0.1, 0.0]),
            Vec::new(),
        );
        let shape = Shape::MultiPolygon(MultiPolygon::from_dense(vec![kept, sliver]).unwrap());
        let Shape::MultiPolygon(out) = shape.simplify_dp_raw(1.0).unwrap() else {
            panic!("multipolygon remains");
        };
        assert_eq!(out.len(), 2);
        assert_eq!(out.as_slice().len(), 1);
    }
}

#[cfg(test)]
mod dp_zero_split_tests {
    use std::sync::Arc;

    use super::rdp_keep;
    use crate::geometry::{CoordSeq, LineSeq, Shape, XY, point_on_segment};

    fn kept_mask(xs: &[f64], ys: &[f64]) -> Vec<bool> {
        let mut keep = Vec::new();
        let mut stack = Vec::new();
        rdp_keep(xs, ys, 0.0, &mut keep, &mut stack).expect("chain long enough to mask");
        keep
    }

    fn line_xy(xs: &[f64], ys: &[f64]) -> Shape {
        Shape::LineString(LineSeq::from_proven(CoordSeq::from_columns(
            Arc::from(xs),
            Arc::from(ys),
            None,
            None,
        )))
    }

    fn discarded_lie_on_surviving_chords(xs: &[f64], ys: &[f64], keep: &[bool]) {
        let kept: Vec<usize> = keep
            .iter()
            .enumerate()
            .filter_map(|(index, &kept)| kept.then_some(index))
            .collect();
        for (index, &is_kept) in keep.iter().enumerate() {
            if is_kept {
                continue;
            }
            let Some(left) = kept.iter().rev().find(|&&k| k < index).copied() else {
                panic!("dropped vertex {index} has no kept predecessor");
            };
            let Some(right) = kept.iter().find(|&&k| k > index).copied() else {
                panic!("dropped vertex {index} has no kept successor");
            };
            assert!(
                point_on_segment(
                    XY::new(xs[index], ys[index]),
                    XY::new(xs[left], ys[left]),
                    XY::new(xs[right], ys[right]),
                ),
                "dropped {index} must lie on surviving chord {left}..{right}"
            );
        }
    }

    #[test]
    fn l_bend_keeps_the_corner() {
        let xs = [0.0, 1.0, 2.0];
        let ys = [0.0, 1.0, 0.0];
        let keep = kept_mask(&xs, &ys);
        assert_eq!(keep, [true, true, true]);
        let Shape::LineString(out) = line_xy(&xs, &ys).simplify_dp(0.0, true).unwrap() else {
            panic!("line");
        };
        assert_eq!(out.xs(), &xs);
        assert_eq!(out.ys(), &ys);
    }

    #[test]
    fn collinear_interior_drops() {
        let xs = [0.0, 1.0, 2.0];
        let ys = [0.0, 0.0, 0.0];
        let keep = kept_mask(&xs, &ys);
        assert_eq!(keep, [true, false, true]);
        discarded_lie_on_surviving_chords(&xs, &ys, &keep);
    }

    #[test]
    fn backtrack_outside_endpoint_is_kept() {
        let xs = [0.0, 2.0, 1.0];
        let ys = [0.0, 0.0, 0.0];
        let keep = kept_mask(&xs, &ys);
        assert_eq!(keep, [true, true, true]);
    }

    #[test]
    fn farthest_nonmember_drops_avoidable_collinear() {
        let xs = [0.0, 1.0, 2.0, 2.0];
        let ys = [0.0, 0.0, 0.0, 1.0];
        let keep = kept_mask(&xs, &ys);
        assert_eq!(keep, [true, false, true, true]);
        discarded_lie_on_surviving_chords(&xs, &ys, &keep);
        let Shape::LineString(out) = line_xy(&xs, &ys).simplify_dp(0.0, true).unwrap() else {
            panic!("line");
        };
        assert_eq!(out.xs(), &[0.0, 2.0, 2.0]);
        assert_eq!(out.ys(), &[0.0, 0.0, 1.0]);
    }

    #[test]
    fn repeated_endpoint_keeps_the_off_chord_vertex() {
        let xs = [0.0, 1.0, 0.0];
        let ys = [0.0, 1.0, 0.0];
        let keep = kept_mask(&xs, &ys);
        assert_eq!(keep, [true, true, true]);
    }

    #[test]
    fn signed_zero_on_axis_is_on_chord() {
        let xs = [0.0, 1.0, 2.0];
        let ys = [0.0, -0.0, 0.0];
        let keep = kept_mask(&xs, &ys);
        assert_eq!(keep, [true, false, true]);
        discarded_lie_on_surviving_chords(&xs, &ys, &keep);
    }

    #[test]
    fn subnormal_offset_is_a_nonmember() {
        let xs = [0.0, 0.5, 1.0];
        let ys = [0.0, f64::from_bits(1), 0.0];
        let keep = kept_mask(&xs, &ys);
        assert_eq!(keep, [true, true, true]);
    }

    #[test]
    fn huge_finite_bend_is_kept() {
        let xs = [0.0, 1e100, 2e100];
        let ys = [0.0, 1e100, 0.0];
        let keep = kept_mask(&xs, &ys);
        assert_eq!(keep, [true, true, true]);
    }

    #[test]
    fn xyzm_l_bend_keeps_corner_ordinates() {
        let line = Shape::LineString(LineSeq::from_proven(CoordSeq::from_columns(
            Arc::from([0.0, 1.0, 2.0]),
            Arc::from([0.0, 1.0, 0.0]),
            Some(Arc::from([1.0, 7.0, 3.0])),
            Some(Arc::from([10.0, 70.0, 30.0])),
        )));
        let Shape::LineString(out) = line.simplify_dp(0.0, true).unwrap() else {
            panic!("line");
        };
        assert_eq!(out.zs(), Some(&[1.0, 7.0, 3.0][..]));
        assert_eq!(out.ms(), Some(&[10.0, 70.0, 30.0][..]));
    }

    #[test]
    fn collection_member_uses_the_same_mask() {
        let bent = line_xy(&[0.0, 1.0, 2.0], &[0.0, 1.0, 0.0]);
        let collinear = line_xy(&[0.0, 1.0, 2.0], &[0.0, 0.0, 0.0]);
        let collection = Shape::geometry_collection(vec![bent, collinear]).unwrap();
        let Shape::GeometryCollection(out) = collection.simplify_dp(0.0, true).unwrap() else {
            panic!("collection");
        };
        let Shape::LineString(bent_out) = &out.as_slice()[0] else {
            panic!("bent member");
        };
        let Shape::LineString(col_out) = &out.as_slice()[1] else {
            panic!("collinear member");
        };
        assert_eq!(bent_out.xs(), &[0.0, 1.0, 2.0]);
        assert_eq!(col_out.xs(), &[0.0, 2.0]);
    }

    #[test]
    fn minus_zero_tolerance_matches_plus_zero() {
        let xs = [0.0, 1.0, 2.0];
        let ys = [0.0, 1.0, 0.0];
        let mut keep_pos = Vec::new();
        let mut keep_neg = Vec::new();
        let mut stack = Vec::new();
        rdp_keep(&xs, &ys, 0.0, &mut keep_pos, &mut stack).unwrap();
        rdp_keep(&xs, &ys, -0.0, &mut keep_neg, &mut stack).unwrap();
        assert_eq!(keep_pos, keep_neg);
        assert_eq!(keep_pos, [true, true, true]);
    }
}
