#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#[cfg(test)]
use std::cell::Cell;
use std::simd::Select as _;
use std::simd::cmp::SimdPartialOrd as _;

use crate::NonNegative;
use crate::error::Result;
use crate::geometry::constructive::{WalkJoinRule, WalkPlan, materialize_walk, vw_keep};
use crate::geometry::overlay::{ExactSourceInventory, rect_polygon};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, ExpansionBudget, GeometryKind, LineSeq, MultiLineString,
    MultiMember, MultiPolygon, Orientation, Point, PointKey, Polygon, REDUCE_LANES, ReduceSimd,
    Ring, Segment, Shape, TopologyGuard, XY, dedup_consecutive_points, line_segments,
    open_point_cycle_winding, orientation, point_on_segment, point_segment_distance,
    point_segment_distance_squared, same_point, simplified_polygon_delta_is_simple,
    simplify_chain_guarded, triangle_area, vw_area_tolerance, wrap_index,
};

#[cfg(test)]
thread_local! {
    static SUBDIVISION_WORK: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

#[cfg(test)]
fn reset_subdivision_work() {
    SUBDIVISION_WORK.with(|work| work.set((0, 0)));
}

#[cfg(test)]
fn subdivision_work() -> (usize, usize) {
    SUBDIVISION_WORK.with(Cell::get)
}

/// The Douglas-Peucker keep mask over raw columns: `keep` and `stack` are
/// cleared and refilled (reusable across packed-array rows); `Ok(Some(kept_count))`
/// unless the chain is too short to simplify. Keep-mask and split-stack
/// growth report allocation failure. The packed-array lane appends kept
/// vertices straight into new CSR columns from this mask. `stack` must stay
/// batch-local — never a receiver cache or lock (free-threading).
pub(crate) fn rdp_keep(
    xs: &[f64],
    ys: &[f64],
    tolerance: f64,
    keep: &mut Vec<bool>,
    stack: &mut Vec<(usize, usize)>,
) -> Result<Option<usize>> {
    let count = xs.len();
    if count <= 2 {
        return Ok(None);
    }
    let tolerance_squared = tolerance * tolerance;
    keep.clear();
    crate::try_reserve_vec(keep, count)?;
    keep.resize(count, false);
    keep[0] = true;
    keep[count - 1] = true;
    let mut kept = 2_usize;
    stack.clear();
    crate::try_push_vec(stack, (0_usize, count - 1))?;
    while let Some((start, end)) = stack.pop() {
        if end - start < 2 {
            continue;
        }
        if let Some(farthest) = rdp_split(xs, ys, start, end, tolerance, tolerance_squared)? {
            keep[farthest] = true;
            kept += 1;
            crate::try_push_vec(stack, (start, farthest))?;
            crate::try_push_vec(stack, (farthest, end))?;
        }
    }
    Ok(Some(kept))
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
) -> Result<Option<usize>> {
    let (ax, ay) = (xs[start], ys[start]);
    let (bx, by) = (xs[end], ys[end]);
    let (dx, dy) = (bx - ax, by - ay);
    let chord_squared = dx * dx + dy * dy;
    let threshold = tolerance_squared * chord_squared;
    let mut best = 0.0_f64;
    let mut farthest = start;
    let exact_scan = |best: &mut f64, farthest: &mut usize| -> Result<()> {
        let chord = Segment {
            start: XY::new(ax, ay),
            end: XY::new(bx, by),
        };
        *best = f64::NEG_INFINITY;
        for index in start + 1..end {
            let deviation = point_segment_distance_squared(XY::new(xs[index], ys[index]), chord)?;
            if deviation > *best {
                *best = deviation;
                *farthest = index;
            }
        }
        Ok(())
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
                let d = point_segment_distance(point, chord)?;
                if split.is_none() || d > best_d {
                    split = Some(index);
                    best_d = d;
                }
            }
            return Ok(split);
        } else {
            return Ok(None);
        };
        let chord = Segment {
            start: XY::new(ax, ay),
            end: XY::new(bx, by),
        };
        let mut best_d = f64::NEG_INFINITY;
        for index in start + 1..end {
            let d = point_segment_distance(XY::new(xs[index], ys[index]), chord)?;
            if d > best_d {
                best_d = d;
                farthest = index;
            }
        }
        return Ok((best_d > tol).then_some(farthest));
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
        exact_scan(&mut best, &mut farthest)?;
        return Ok((best > tolerance_squared).then_some(farthest));
    }
    Ok((best > threshold).then_some(farthest))
}

/// Clean strictly-turning vertex cycle of a ring: closing duplicate,
/// repeated vertices, and collinear vertices stripped (they contribute
/// nothing to the offset and would emit zero-sweep duplicate joins);
/// oriented CCW (`clockwise = false`, shells) or CW (holes), so the ring
/// interior side is consistent for the offset walk. `None` below 3 strict
/// vertices.
pub(crate) fn strict_cycle(coords: &CoordSeq, clockwise: bool) -> Result<Option<Vec<Point>>> {
    let mut ring = Vec::new();
    crate::try_reserve_vec(&mut ring, coords.len())?;
    for point in coords.iter_coords() {
        crate::try_push_vec(&mut ring, point)?;
    }
    dedup_consecutive_points(&mut ring);
    if ring.len() > 1 && same_point(ring[0], ring[ring.len() - 1]) {
        ring.pop();
    }
    if ring.len() < 3 {
        return Ok(None);
    }
    if open_point_cycle_winding(&ring).reverse_for_clockwise(clockwise) {
        ring.reverse();
    }
    let mut strict = Vec::new();
    crate::try_reserve_vec(&mut strict, ring.len())?;
    for index in 0..ring.len() {
        let previous = ring[wrap_index(index + ring.len() - 1, ring.len())];
        let next = ring[wrap_index(index + 1, ring.len())];
        if orientation(previous, ring[index], next) != Orientation::Collinear {
            crate::try_push_vec(&mut strict, ring[index])?;
        }
    }
    Ok((strict.len() >= 3).then_some(strict))
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
    let quadrant_segments = quadrant_segments.get();
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments);
    let Some(plan) = WalkPlan::new(strict, true, distance, rule)? else {
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

/// Choose a representable cut between source-coordinate ordinates around the
/// median. The source values, rather than a rounded child, own this decision:
/// a dense cluster beside a remote vertex therefore gets a finite complexity
/// split without a second median retry.
fn source_median_cut(shape: &Shape, split_x: bool, bounds: Bounds) -> Result<Option<f64>> {
    let mut ordinates = Vec::new();
    crate::try_reserve_vec(&mut ordinates, shape.coord_count())?;
    shape.for_each_point(|point| {
        ordinates.push(if split_x { point.x } else { point.y });
    });
    ordinates.sort_unstable_by(f64::total_cmp);
    let middle = ordinates.len() / 2;
    let (low, high) = if split_x {
        (bounds.minx(), bounds.maxx())
    } else {
        (bounds.miny(), bounds.maxy())
    };
    Ok((1..ordinates.len())
        .filter_map(|index| {
            if ordinates[index - 1] >= ordinates[index] {
                return None;
            }
            let cut = f64::midpoint(ordinates[index - 1], ordinates[index]);
            (low < cut && cut < high).then_some((index.abs_diff(middle), index, cut))
        })
        .min_by_key(|&(distance, index, _)| (distance, index))
        .map(|(_, _, cut)| cut))
}

pub(crate) fn split(
    shape: Shape,
    max_vertices: usize,
    drop: bool,
    out: &mut Vec<Shape>,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    struct WorkItem {
        shape: Shape,
        bounds: Option<Bounds>,
        terminal: bool,
    }

    let mut work = Vec::new();
    let mut staged = Vec::new();
    let shape_bounds = shape.bounds();
    crate::try_reserve_vec(&mut work, 1)?;
    crate::try_push_vec(&mut work, WorkItem {
        shape,
        bounds: shape_bounds,
        terminal: false,
    })?;
    while let Some(item) = work.pop() {
        #[cfg(test)]
        SUBDIVISION_WORK.with(|work| {
            let (items, splits) = work.get();
            work.set((items.saturating_add(1), splits));
        });
        if item.shape.is_empty() {
            continue;
        }
        if item.terminal || item.shape.coord_count() <= max_vertices {
            crate::try_push_vec(&mut staged, item.shape)?;
            continue;
        }
        let Some(bounds) = item.bounds else {
            crate::try_push_vec(&mut staged, item.shape)?;
            continue;
        };

        let x_cut = source_median_cut(&item.shape, true, bounds)?;
        let y_cut = source_median_cut(&item.shape, false, bounds)?;
        let x_longer = (bounds.maxx() - bounds.minx()) >= (bounds.maxy() - bounds.miny());
        let candidate = if x_longer {
            x_cut
                .map(|cut| (true, cut))
                .or_else(|| y_cut.map(|cut| (false, cut)))
        } else {
            y_cut
                .map(|cut| (false, cut))
                .or_else(|| x_cut.map(|cut| (true, cut)))
        };
        let Some((split_x, cut)) = candidate else {
            crate::try_push_vec(&mut staged, item.shape)?;
            continue;
        };
        let halves = axis_halves(bounds, split_x, cut);
        let left_rect = Shape::Polygon(rect_polygon(halves[0])?);
        let right_rect = Shape::Polygon(rect_polygon(halves[1])?);
        let sources = [&item.shape, &left_rect, &right_rect];
        let inventory = ExactSourceInventory::from_shapes(&sources)?;
        let partition = inventory.partition(budget)?;
        let children =
            inventory
                .subdivision_parts(&partition, &item.shape, item.shape.axes(), drop, [1, 2])?;
        if children.iter().all(Shape::is_empty) {
            // A cut that cannot be represented by the exact arrangement must
            // not silently lose the source support.
            crate::try_push_vec(&mut staged, item.shape)?;
            continue;
        }
        #[cfg(test)]
        let mut progressed = false;
        for child in children.into_iter().rev() {
            if child.is_empty() {
                continue;
            }
            let child_bounds = child.bounds();
            let progress = child_bounds.is_some_and(|child_bounds| {
                let envelope_progress = if split_x {
                    if child_bounds.maxx() <= cut {
                        child_bounds.maxx() < bounds.maxx()
                    } else {
                        child_bounds.minx() > bounds.minx()
                    }
                } else if child_bounds.maxy() <= cut {
                    child_bounds.maxy() < bounds.maxy()
                } else {
                    child_bounds.miny() > bounds.miny()
                };
                let source_complexity_progress = child.coord_count() < item.shape.coord_count();
                matches!(
                    &item.shape,
                    Shape::LineString(_) | Shape::MultiLineString(_)
                ) && envelope_progress
                    || source_complexity_progress
            });
            #[cfg(test)]
            {
                progressed |= progress;
            }
            let terminal = child.coord_count() <= max_vertices || !progress;
            crate::try_push_vec(&mut work, WorkItem {
                shape: child,
                bounds: child_bounds,
                terminal,
            })?;
        }
        #[cfg(test)]
        if progressed {
            SUBDIVISION_WORK.with(|work| {
                let (items, splits) = work.get();
                work.set((items, splits.saturating_add(1)));
            });
        }
    }
    crate::try_reserve_vec(out, staged.len())?;
    out.append(&mut staged);
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
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
) -> Result<Option<CoordSeq>> {
    let Some(kept) = keep_mask(points.xs(), points.ys(), scratch)? else {
        return Ok(None);
    };
    (kept != points.len())
        .then(|| {
            points.select(
                scratch
                    .keep
                    .iter()
                    .enumerate()
                    .filter_map(|(index, &kept)| kept.then_some(index)),
            )
        })
        .transpose()
}

enum RingKeep {
    Same,
    Drop,
    New(Ring),
}

fn keep_ring(
    ring: &Ring,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
) -> Result<RingKeep> {
    Ok(match kept_coords(ring.coords(), scratch, keep_mask)? {
        None => RingKeep::Same,
        Some(coords) if coords.len() < Ring::MIN_VERTICES_CLOSED => RingKeep::Drop,
        Some(coords) => RingKeep::New(Ring::from_closed(coords)),
    })
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
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
) -> Result<PolygonKeep> {
    let new_shell = match keep_ring(&polygon.shell, scratch, keep_mask)? {
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
        match keep_ring(hole, scratch, keep_mask)? {
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
        )?),
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
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
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
                match kept_coords(line, scratch, keep_mask)? {
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
        Some(members) => RawSimplify::Changed(Shape::MultiLineString(
            MultiLineString::from_values(lines.axes(), members)?,
        )),
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
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
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
        )?)),
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
                // polygon is `POLYGON EMPTY`, never an invalid 2-vertex ring.
                if !preserve_topology {
                    return raw.dropping_collapsed_rings();
                }
                // Keep-count identity already returned `Unchanged`. The validity
                // branch-check only runs when vertices actually dropped.
                if self.simplify_kept_topology(&raw)? {
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
        fn clean(polygon: &Polygon) -> Result<Option<Polygon>> {
            if !has_three_distinct_xy(polygon.shell.coords()) {
                return Ok(None);
            }
            Ok(Some(Polygon::new(
                polygon.shell.clone(),
                polygon
                    .holes
                    .iter()
                    .filter(|hole| has_three_distinct_xy(hole.coords()))
                    .cloned()
                    .collect(),
            )?))
        }
        Ok(match self {
            Self::Polygon(polygon) => clean(&polygon)?.map_or_else(
                || Self::typed_empty(GeometryKind::Polygon, polygon.axes()),
                Self::Polygon,
            ),
            Self::MultiPolygon(polygons) => {
                let members = polygons
                    .members()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => {
                            Ok(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(index) => {
                            Ok(clean(&polygons.as_slice()[index])?.map_or(
                                crate::geometry::MultiMember::Empty,
                                crate::geometry::MultiMember::Value,
                            ))
                        },
                    })
                    .collect::<Result<Vec<_>>>()?;
                if members.is_empty() {
                    Self::empty_polygon()
                } else {
                    Self::MultiPolygon(MultiPolygon::from_values(polygons.axes(), members)?)
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
    fn simplify_kept_topology(&self, simplified: &Self) -> Result<bool> {
        match self {
            Self::Polygon(_) | Self::MultiPolygon(_) => {
                // Delta guard: vertex-subset removals can only break the
                // NEW chord edges (for the contract's valid-input
                // promise), so the chords are checked instead of
                // re-validating the whole polygon.
                Ok(simplified_polygon_delta_is_simple(self, simplified)?
                    && (self.is_empty() || !simplified.is_empty()))
            },
            Self::LineString(_) | Self::MultiLineString(_) => {
                Ok(!self.is_simple()? || simplified.is_simple()?)
            },
            Self::GeometryCollection(geometries) => {
                let Self::GeometryCollection(parts) = simplified else {
                    return Ok(false);
                };
                for (original, part) in geometries.iter().zip(parts) {
                    if !original.simplify_kept_topology(part)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
            _ => Ok(true),
        }
    }

    /// Guarded greedy simplification: per chain (line or ring), repeatedly
    /// remove the vertex with the smallest chord deviation `<= tolerance`
    /// whose removal the topology guard accepts.
    fn simplify_guarded(&self, tolerance: f64) -> Result<Self> {
        self.simplify_guarded_with(|prev, vertex, next| {
            let deviation = point_segment_distance(vertex, Segment {
                start: prev,
                end: next,
            })?;
            Ok((deviation <= tolerance).then_some(deviation))
        })
    }

    /// Guarded greedy simplification over every chain with a pluggable
    /// importance criterion (chord deviation for Douglas-Peucker, triangle
    /// area for Visvalingam-Whyatt): the least important removable vertex
    /// the topology guard accepts drops first.
    fn simplify_guarded_with(
        &self,
        candidate: impl Fn(XY, XY, XY) -> Result<Option<f64>>,
    ) -> Result<Self> {
        let mut segments = Vec::new();
        self.try_for_each_segment_chain(|chain| {
            for segment in line_segments(chain) {
                crate::try_push_vec(&mut segments, segment)?;
            }
            Ok::<_, crate::error::Error>(())
        })?;
        let mut vertex_cloud = Vec::new();
        crate::try_reserve_vec(&mut vertex_cloud, self.coord_count())?;
        // Original XY only: the guard indexes the source point set, never
        // simplified output. Isolated points participate in the vertex
        // probe, so the walk is every vertex, not only segment chains.
        self.try_for_each_point(|point| {
            vertex_cloud.push([point.x, point.y]);
            Ok::<_, crate::error::Error>(())
        })?;
        let guard = TopologyGuard::new(&segments, vertex_cloud);
        self.map_chains(&|points: &CoordSeq| {
            let xs = points.xs();
            let ys = points.ys();
            let count = xs.len();
            let closed = count >= 2
                && same_point(XY::new(xs[0], ys[0]), XY::new(xs[count - 1], ys[count - 1]));
            let kept = simplify_chain_guarded(
                count,
                |index| XY::new(xs[index], ys[index]),
                closed,
                &candidate,
                &guard,
            )?;
            if kept.len() == count {
                Ok(None)
            } else {
                Ok(Some(points.select(kept.into_iter())?))
            }
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
                if self.simplify_kept_topology(&raw)? {
                    Ok(raw)
                } else {
                    let area_tolerance = vw_area_tolerance(tolerance);
                    self.simplify_guarded_with(|prev, vertex, next| {
                        let area = triangle_area(prev, vertex, next);
                        Ok((area < area_tolerance).then_some(area))
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
        keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
    ) -> Result<RawSimplify> {
        Ok(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => RawSimplify::Unchanged,
            Self::LineString(points) => kept_coords(points, scratch, keep_mask)?
                .map_or(RawSimplify::Unchanged, |coords| {
                    RawSimplify::Changed(Self::LineString(LineSeq::from_proven(coords)))
                }),
            Self::MultiLineString(lines) => keep_multi_lines(lines, scratch, keep_mask)?,
            Self::Polygon(polygon) => match keep_polygon(polygon, scratch, keep_mask)? {
                PolygonKeep::Same => RawSimplify::Unchanged,
                PolygonKeep::Empty => {
                    RawSimplify::Changed(Self::typed_empty(GeometryKind::Polygon, polygon.axes()))
                },
                PolygonKeep::New(polygon) => RawSimplify::Changed(Self::Polygon(polygon)),
            },
            Self::MultiPolygon(polygons) => keep_multi_polygons(polygons, scratch, keep_mask)?,
            Self::GeometryCollection(geometries) => {
                let mut out: Option<Vec<Self>> = None;
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

    use super::has_three_distinct_xy;
    use crate::geometry::CoordSeq;
    use crate::test_support::fail_allocations_after;

    fn columns(xs: &[f64], ys: &[f64]) -> CoordSeq {
        CoordSeq::try_from_vecs(xs.into(), ys.into(), None, None).unwrap()
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

    use super::RawSimplify;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{
        CoordSeq, CoordinateAxes, GeometryKind, LineSeq, MultiLineString, MultiPolygon, Polygon,
        Ring, Shape,
    };
    use crate::test_support::fail_allocations_after;

    fn line_xy(xs: &[f64], ys: &[f64]) -> LineSeq {
        LineSeq::from_proven(CoordSeq::try_from_vecs(xs.into(), ys.into(), None, None).unwrap())
    }

    fn closed_ring(xs: &[f64], ys: &[f64]) -> Ring {
        Ring::from_closed(CoordSeq::try_from_vecs(xs.into(), ys.into(), None, None).unwrap())
    }

    fn square() -> Shape {
        Shape::Polygon(
            Polygon::new(
                closed_ring(&[0.0, 4.0, 4.0, 0.0, 0.0], &[0.0, 0.0, 4.0, 4.0, 0.0]),
                Vec::new(),
            )
            .unwrap(),
        )
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
        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_vecs(
                [-0.0, 1.0, 2.0].into(),
                [0.0, -0.0, 0.0].into(),
                Some([nan, 1.0, nan].into()),
                Some([0.0, nan, -0.0].into()),
            )
            .unwrap(),
        ));
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
        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_vecs(
                [0.0, 1.0, 0.0].into(),
                [0.0, 0.0, 0.0].into(),
                Some([1.0, 2.0, 9.0].into()),
                Some([10.0, 20.0, 90.0].into()),
            )
            .unwrap(),
        ));
        let Shape::LineString(out) = line.simplify_dp(0.0, true).unwrap() else {
            panic!("A-B-A DP remains a line");
        };
        assert_eq!(out.xs(), &[0.0, 1.0, 0.0]);
        assert_eq!(out.zs(), Some(&[1.0, 2.0, 9.0][..]));
        assert_eq!(out.ms(), Some(&[10.0, 20.0, 90.0][..]));
    }

    #[test]
    fn middle_hole_drop_clones_unchanged_prefix() {
        let shell = closed_ring(&[0.0, 8.0, 8.0, 0.0, 0.0], &[0.0, 0.0, 8.0, 8.0, 0.0]);
        let keep_a = closed_ring(&[1.0, 2.0, 2.0, 1.0, 1.0], &[1.0, 1.0, 2.0, 2.0, 1.0]);
        let drop = closed_ring(&[4.0, 6.0, 5.0, 4.0], &[4.0, 4.0, 4.01, 4.0]);
        let keep_b = closed_ring(&[1.0, 2.0, 2.0, 1.0, 1.0], &[5.0, 5.0, 6.0, 6.0, 5.0]);
        let a_ptr = keep_a.xs().as_ptr();
        let b_ptr = keep_b.xs().as_ptr();
        let polygon = Shape::Polygon(Polygon::new(shell, vec![keep_a, drop, keep_b]).unwrap());
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
        let sliver = Shape::Polygon(
            Polygon::new(
                closed_ring(&[0.0, 1.0, 0.5, 0.0], &[0.0, 0.0, 0.1, 0.0]),
                Vec::new(),
            )
            .unwrap(),
        );
        assert_eq!(
            sliver.simplify_dp_raw(1.0).unwrap(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XY)
        );
    }

    #[test]
    fn collapsed_keep_all_drops_only_when_topology_is_off() {
        let collapsed = Shape::Polygon(
            Polygon::new(
                closed_ring(&[0.0, 0.0, 0.0, 0.0], &[0.0, 0.0, 0.0, 0.0]),
                Vec::new(),
            )
            .unwrap(),
        );
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
            MultiLineString::from_dense(vec![first, middle, last])
                .unwrap()
                .unwrap(),
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
        let lines = MultiLineString::from_dense(vec![first, middle, last])
            .unwrap()
            .unwrap();
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
        )
        .unwrap();
        let sliver = Polygon::new(
            closed_ring(&[10.0, 11.0, 10.5, 10.0], &[0.0, 0.0, 0.1, 0.0]),
            Vec::new(),
        )
        .unwrap();
        let shape = Shape::MultiPolygon(
            MultiPolygon::from_dense(vec![kept, sliver])
                .unwrap()
                .unwrap(),
        );
        let Shape::MultiPolygon(out) = shape.simplify_dp_raw(1.0).unwrap() else {
            panic!("multipolygon remains");
        };
        assert_eq!(out.len(), 2);
        assert_eq!(out.as_slice().len(), 1);
    }
}

#[cfg(test)]
mod dp_zero_split_tests {

    use super::rdp_keep;
    use crate::geometry::{CoordSeq, LineSeq, Shape, XY, point_on_segment};

    fn kept_mask(xs: &[f64], ys: &[f64]) -> Vec<bool> {
        let mut keep = Vec::new();
        let mut stack = Vec::new();
        rdp_keep(xs, ys, 0.0, &mut keep, &mut stack)
            .expect("keep-mask scratch")
            .expect("chain long enough to mask");
        keep
    }

    fn line_xy(xs: &[f64], ys: &[f64]) -> Shape {
        Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_vecs(xs.into(), ys.into(), None, None).unwrap(),
        ))
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
        let line = Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_vecs(
                [0.0, 1.0, 2.0].into(),
                [0.0, 1.0, 0.0].into(),
                Some([1.0, 7.0, 3.0].into()),
                Some([10.0, 70.0, 30.0].into()),
            )
            .unwrap(),
        ));
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
        rdp_keep(&xs, &ys, 0.0, &mut keep_pos, &mut stack)
            .unwrap()
            .unwrap();
        rdp_keep(&xs, &ys, -0.0, &mut keep_neg, &mut stack)
            .unwrap()
            .unwrap();
        assert_eq!(keep_pos, keep_neg);
        assert_eq!(keep_pos, [true, true, true]);
    }
}

#[cfg(test)]
mod guarded_borrow_tests {

    use super::RawSimplify;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{CoordSeq, Polygon, Ring, Shape, TopologyGuard, XY};
    use crate::test_support::{allocation_count, fail_allocations_after};

    fn closed_ring(xs: &[f64], ys: &[f64], zs: Option<&[f64]>, ms: Option<&[f64]>) -> Ring {
        Ring::from_closed(
            CoordSeq::try_from_vecs(
                xs.into(),
                ys.into(),
                zs.map(<[f64]>::to_vec),
                ms.map(<[f64]>::to_vec),
            )
            .unwrap(),
        )
    }

    fn sliver_xyzm() -> Shape {
        Shape::Polygon(
            Polygon::new(
                closed_ring(
                    &[0.0, 1.0, 0.5, 0.0],
                    &[0.0, 0.0, 0.1, 0.0],
                    Some(&[1.0, 2.0, 3.0, 1.0]),
                    Some(&[10.0, 20.0, 30.0, 10.0]),
                ),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn vertex_cloud_walk_rejects_allocation_and_stores_xy() {
        let vertices = [XY::new(1.0, 2.0), XY::new(3.0, 4.0)];
        let error =
            fail_allocations_after(0, || TopologyGuard::try_vertex_cloud(vertices)).unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
        assert_eq!(TopologyGuard::try_vertex_cloud(vertices).unwrap(), vec![
            [1.0, 2.0],
            [3.0, 4.0]
        ]);
    }

    #[test]
    fn guarded_sliver_keep_all_reuses_source_columns() {
        let polygon = sliver_xyzm();
        assert!(matches!(
            polygon.simplify_raw_dp(1.0).unwrap(),
            RawSimplify::Changed(_)
        ));
        let Shape::Polygon(src) = &polygon else {
            panic!("sliver fixture");
        };
        let src_xs = src.shell.xs().as_ptr();
        let src_zs = src.shell.zs().unwrap().as_ptr();
        let src_ms = src.shell.ms().unwrap().as_ptr();
        let Shape::Polygon(out) = polygon.simplify_dp(1.0, true).unwrap() else {
            panic!("topology-preserving sliver remains a polygon");
        };
        assert_eq!(out.shell.xs().as_ptr(), src_xs);
        assert_eq!(out.shell.zs().unwrap().as_ptr(), src_zs);
        assert_eq!(out.shell.ms().unwrap().as_ptr(), src_ms);
        assert_eq!(out.shell.zs(), Some(&[1.0, 2.0, 3.0, 1.0][..]));
        assert_eq!(out.shell.ms(), Some(&[10.0, 20.0, 30.0, 10.0][..]));
    }

    #[test]
    fn guarded_construction_reports_allocation_failure() {
        let polygon = sliver_xyzm();
        let (out, allocations) = allocation_count(|| polygon.simplify_dp(1.0, true).unwrap());
        assert!(matches!(out, Shape::Polygon(_)));
        assert!(allocations > 0);
        for allowed in 0..allocations {
            let error = fail_allocations_after(allowed, || polygon.simplify_dp(1.0, true))
                .expect_err("every reachable allocation on the sliver path must propagate");
            assert!(matches!(
                error.kind(),
                ErrorKind::Resource(ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn guarded_select_keeps_source_index_ordinates() {
        let polygon = Shape::Polygon(
            Polygon::new(
                closed_ring(
                    &[0.0, 10.0, 10.0, 5.0, 0.0, 0.0],
                    &[0.0, 0.0, 0.01, 0.01, 0.01, 0.0],
                    Some(&[1.0, 2.0, 3.0, 4.0, 5.0, 1.0]),
                    Some(&[10.0, 20.0, 30.0, 40.0, 50.0, 10.0]),
                ),
                Vec::new(),
            )
            .unwrap(),
        );
        assert!(matches!(
            polygon.simplify_raw_dp(1.0).unwrap(),
            RawSimplify::Changed(_)
        ));
        let Shape::Polygon(out) = polygon.simplify_dp(1.0, true).unwrap() else {
            panic!("topology-preserving thin ring remains a polygon");
        };
        let xs = out.shell.xs();
        let ys = out.shell.ys();
        let zs = out.shell.zs().expect("XYZM");
        let ms = out.shell.ms().expect("XYZM");
        assert!(
            (4..6).contains(&xs.len()),
            "guarded cyclic floor is a triangle, got {}",
            xs.len()
        );
        let original = [
            (0.0, 0.0, 1.0, 10.0),
            (10.0, 0.0, 2.0, 20.0),
            (10.0, 0.01, 3.0, 30.0),
            (5.0, 0.01, 4.0, 40.0),
            (0.0, 0.01, 5.0, 50.0),
        ];
        for index in 0..xs.len() {
            assert!(
                original.contains(&(xs[index], ys[index], zs[index], ms[index])),
                "output vertex is not an original index"
            );
        }
    }
}

#[cfg(test)]
mod keep_mask_scratch_tests {

    use super::rdp_keep;
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::constructive::vw_keep;
    use crate::test_support::{allocation_count, fail_allocations_after};

    fn collinear_xy(count: usize) -> (Vec<f64>, Vec<f64>) {
        ((0..count).map(|index| index as f64).collect(), vec![
            0.0;
            count
        ])
    }

    fn assert_allocation_error(error: &crate::error::Error) {
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    #[test]
    fn rdp_keep_rejects_every_allocation() {
        let (xs, ys) = collinear_xy(8);
        let run = || {
            let mut keep = Vec::new();
            let mut stack = Vec::new();
            rdp_keep(&xs, &ys, 0.0, &mut keep, &mut stack)
        };
        let (kept, allocations) = allocation_count(|| run().unwrap());
        assert_eq!(kept, Some(2));
        assert!(allocations > 0);
        for allowed in 0..allocations {
            let error = fail_allocations_after(allowed, run)
                .expect_err("DP keep-mask scratch must propagate allocation failure");
            assert_allocation_error(&error);
        }
    }

    #[test]
    fn vw_keep_rejects_every_allocation() {
        let (xs, ys) = collinear_xy(8);
        let run = || {
            let mut keep = Vec::new();
            vw_keep(&xs, &ys, 1.0, &mut keep)
        };
        let (kept, allocations) = allocation_count(|| run().unwrap());
        assert_eq!(kept, Some(2));
        assert!(allocations > 0);
        for allowed in 0..allocations {
            let error = fail_allocations_after(allowed, run)
                .expect_err("VW keep-mask scratch must propagate allocation failure");
            assert_allocation_error(&error);
        }
    }

    #[test]
    fn reused_small_keep_grows_fallibly_for_a_larger_chain() {
        let small = collinear_xy(8);
        let mut keep = Vec::new();
        let mut stack = Vec::new();
        rdp_keep(&small.0, &small.1, 0.0, &mut keep, &mut stack).unwrap();
        let capacity = keep.capacity();
        assert!(capacity >= 8);
        let (xs, ys) = collinear_xy(capacity + 1);
        stack.clear();
        let error = fail_allocations_after(0, || rdp_keep(&xs, &ys, 0.0, &mut keep, &mut stack))
            .expect_err("growing a reused keep mask must not abort in resize");
        assert_allocation_error(&error);

        let mut keep = Vec::new();
        vw_keep(&small.0, &small.1, 1.0, &mut keep).unwrap();
        let capacity = keep.capacity();
        let (xs, ys) = collinear_xy(capacity + 1);
        let error = fail_allocations_after(0, || vw_keep(&xs, &ys, 1.0, &mut keep))
            .expect_err("growing a reused VW keep mask must not abort in resize");
        assert_allocation_error(&error);
    }
}

#[cfg(test)]
mod subdivision_tests {
    use super::*;
    use crate::geometry::overlay::{partition_calls, reset_partition_calls};
    use crate::test_support::{allocation_count, fail_allocations_after};

    fn ring_shape(points: &[Point]) -> Shape {
        Shape::Polygon(
            Polygon::new(
                Ring::from_closed(CoordSeq::try_from_points(points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn line_shape(points: &[Point]) -> Shape {
        Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(points).unwrap(),
        ))
    }

    fn all_points(shape: &Shape) -> Vec<Point> {
        let mut points = Vec::new();
        shape.for_each_point(|point| points.push(point));
        points
    }

    #[test]
    fn one_exact_partition_emits_both_children() {
        let shape = ring_shape(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(2.0, 1.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(0.0, 2.0),
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(0.0, 0.0),
        ]);
        reset_partition_calls();
        reset_subdivision_work();
        let parts = shape.subdivide(8, false).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(partition_calls(), 1);
        assert_eq!(subdivision_work(), (3, 1));
        assert!(parts.iter().all(|part| part.coord_count() <= 8));
    }

    #[test]
    fn allocation_prefix_failure_publishes_no_subdivision_parts() {
        let shape = ring_shape(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(2.0, 1.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(0.0, 2.0),
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(0.0, 0.0),
        ]);
        let (parts, allocations) = allocation_count(|| shape.subdivide(8, false).unwrap());
        assert_eq!(parts.len(), 2);
        assert!(allocations > 0);
        for allowed in 0..allocations {
            let result = fail_allocations_after(allowed, || shape.subdivide(8, false));
            match result {
                Ok(parts) => assert_eq!(
                    parts.len(),
                    2,
                    "retry at allocation prefix {allowed}/{allocations} published a partial result",
                ),
                Err(error) => assert!(matches!(
                    error.kind(),
                    crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
                )),
            }
        }
        assert_eq!(shape.coord_count(), 9);
    }

    #[test]
    fn source_gap_search_skips_an_unrepresentable_cluster_gap() {
        let mut points = Vec::new();
        for index in 0..8 {
            points.push(Point::new_raw_xy(
                f64::from_bits(1.0_f64.to_bits() + index),
                0.0,
            ));
        }
        points.push(Point::new_raw_xy(1_000_000.0, 0.0));
        let shape = Shape::multi_point(
            crate::geometry::CoordinateAxes::XY,
            CoordSeq::try_from_points(&points).unwrap(),
        );
        reset_partition_calls();
        let parts = shape.subdivide(8, false).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(partition_calls(), 1);
        assert!(parts.iter().all(|part| !part.is_empty()));
    }

    #[test]
    fn adjacent_sub_ulp_cluster_is_documented_unsplittable() {
        let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
        let points = (0..9)
            .map(|index| Point::new_raw_xy(if index % 2 == 0 { 1.0 } else { adjacent }, 0.0))
            .collect::<Vec<_>>();
        let shape = line_shape(&points);
        reset_partition_calls();
        let parts = shape.subdivide(8, false).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].coord_count(), shape.coord_count());
        assert_eq!(partition_calls(), 0);
    }

    #[test]
    fn mixed_dimension_subdivision_keeps_each_selected_dimension() {
        let polygon = ring_shape(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(2.0, 2.0),
            Point::new_raw_xy(0.0, 2.0),
            Point::new_raw_xy(0.0, 0.0),
        ]);
        let line = line_shape(&[Point::new_raw_xy(-1.0, 1.0), Point::new_raw_xy(3.0, 1.0)]);
        let shape = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(1.0, 1.0)),
            line,
            polygon,
        ])
        .unwrap();
        let parts = shape.subdivide(7, false).unwrap();
        assert!(parts.len() >= 2);
        assert!(parts.iter().all(|part| !part.is_empty()));
        assert!(parts.iter().any(|part| {
            matches!(part, Shape::GeometryCollection(_))
                || matches!(part, Shape::Polygon(_))
                || matches!(part, Shape::LineString(_))
        }));
    }

    #[test]
    fn exact_seam_rounding_and_zm_resolution_are_shared() {
        let coordinates = CoordSeq::try_from_vecs(
            vec![0.0, 1.0, 2.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0],
            vec![0.0, 0.0, 0.0, 1.0, 2.0, 2.0, 2.0, 1.0, 0.0],
            Some(vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 0.0]),
            Some(vec![
                100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 100.0,
            ]),
        )
        .unwrap();
        let shape =
            Shape::Polygon(Polygon::new(Ring::from_closed(coordinates), Vec::new()).unwrap());
        let parts = shape.subdivide(8, false).unwrap();
        assert_eq!(parts.len(), 2);
        let source = all_points(&shape);
        let mut new_shared = 0;
        for left in all_points(&parts[0]) {
            for right in all_points(&parts[1]) {
                if left.x.to_bits() != right.x.to_bits() || left.y.to_bits() != right.y.to_bits() {
                    continue;
                }
                assert_eq!(left.z().map(f64::to_bits), right.z().map(f64::to_bits));
                assert_eq!(left.m().map(f64::to_bits), right.m().map(f64::to_bits));
                if !source.iter().any(|point| {
                    point.x.to_bits() == left.x.to_bits() && point.y.to_bits() == left.y.to_bits()
                }) {
                    new_shared += 1;
                }
            }
        }
        assert!(new_shared > 0);
    }

    #[test]
    fn tangent_holes_and_collapsed_support_follow_exact_selection() {
        for wkt in [
            "POLYGON ((0 0,1 0,2 0,3 0,4 0,4 4,0 4,0 0),(2 1,3 1,3 2,2 2,2 1))",
            "POLYGON ((0 0,4 0,2 0,0 0),(1 1,3 1,3 3,1 3,1 1))",
        ] {
            let shape = crate::io::parse_wkt(wkt).unwrap();
            let parts = shape.subdivide(8, false).unwrap();
            assert!(!parts.is_empty(), "{wkt}");
            assert!(parts.iter().all(|part| !part.is_empty()), "{wkt}");
        }
    }
}
