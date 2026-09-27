#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
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
    simplify_chain_guarded, triangle_area, vw_area_tolerance,
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
/// unless the chain is too short to simplify. The packed-array lane appends
/// kept vertices straight into new CSR columns from this mask. `stack` must
/// stay batch-local — never a receiver cache or lock (free-threading).
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
    keep.reserve(count);
    keep.resize(count, false);
    *keep
        .first_mut()
        .expect("Douglas-Peucker input has two endpoints") = true;
    *keep
        .last_mut()
        .expect("Douglas-Peucker input has two endpoints") = true;
    let mut kept = 2_usize;
    stack.clear();
    stack.push((0_usize, count.wrapping_sub(1)));
    while let Some((start, end)) = stack.pop() {
        if end.wrapping_sub(start) < 2 {
            continue;
        }
        if let Some(farthest) = rdp_split(xs, ys, start, end, tolerance, tolerance_squared)? {
            *keep
                .get_mut(farthest)
                .expect("Douglas-Peucker split belongs to its source range") = true;
            kept = kept.wrapping_add(1);
            stack.push((start, farthest));
            stack.push((farthest, end));
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
pub(crate) fn rdp_split(
    xs: &[f64],
    ys: &[f64],
    start: usize,
    end: usize,
    tolerance: f64,
    tolerance_squared: f64,
) -> Result<Option<usize>> {
    let (ax, ay) = (
        *xs.get(start)
            .expect("Douglas-Peucker start belongs to its source range"),
        *ys.get(start)
            .expect("Douglas-Peucker start belongs to its source range"),
    );
    let (bx, by) = (
        *xs.get(end)
            .expect("Douglas-Peucker end belongs to its source range"),
        *ys.get(end)
            .expect("Douglas-Peucker end belongs to its source range"),
    );
    let (dx, dy) = (bx - ax, by - ay);
    let chord_squared = dx * dx + dy * dy;
    let threshold = tolerance_squared * chord_squared;
    let mut best = 0.0_f64;
    let mut farthest = start;
    let interior_start = start.wrapping_add(1);
    let interior_xs = xs
        .get(interior_start..end)
        .expect("Douglas-Peucker interior belongs to its source range");
    let interior_ys = ys
        .get(interior_start..end)
        .expect("Douglas-Peucker interior belongs to its source range");
    let exact_scan = |best: &mut f64, farthest: &mut usize| -> Result<()> {
        let chord = Segment {
            start: XY::new(ax, ay),
            end: XY::new(bx, by),
        };
        *best = f64::NEG_INFINITY;
        for (offset, (&x, &y)) in interior_xs.iter().zip(interior_ys).enumerate() {
            let index = interior_start.wrapping_add(offset);
            let deviation = point_segment_distance_squared(XY::new(x, y), chord)?;
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
            for (offset, (&x, &y)) in interior_xs.iter().zip(interior_ys).enumerate() {
                let index = interior_start.wrapping_add(offset);
                let point = XY::new(x, y);
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
        for (offset, (&x, &y)) in interior_xs.iter().zip(interior_ys).enumerate() {
            let index = interior_start.wrapping_add(offset);
            let d = point_segment_distance(XY::new(x, y), chord)?;
            if d > best_d {
                best_d = d;
                farthest = index;
            }
        }
        return Ok((best_d > tol).then_some(farthest));
    }
    let mut index = interior_start;
    if end.wrapping_sub(index) >= REDUCE_LANES {
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
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "SIMD cursor lanes are bounded source ordinals"
        )]
        let mut cursor = std::simd::Simd::<u64, REDUCE_LANES>::splat(index as u64) + lane_offsets;
        while index.wrapping_add(REDUCE_LANES) <= end {
            let lane_end = index.wrapping_add(REDUCE_LANES);
            let x = ReduceSimd::from_slice(
                xs.get(index..lane_end)
                    .expect("Douglas-Peucker SIMD window belongs to its source range"),
            );
            let y = ReduceSimd::from_slice(
                ys.get(index..lane_end)
                    .expect("Douglas-Peucker SIMD window belongs to its source range"),
            );
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "SIMD lanes preserve the binary64 point-to-segment score"
            )]
            let score = {
                let (qx, qy) = (x - ax_s, y - ay_s);
                let along = qx * dx_s + qy * dy_s;
                let cross = qx * dy_s - qy * dx_s;
                let (rx, ry) = (x - bx_s, y - by_s);
                along.simd_le(ReduceSimd::splat(0.0)).select(
                    (qx * qx + qy * qy) * chord_s,
                    along
                        .simd_ge(chord_s)
                        .select((rx * rx + ry * ry) * chord_s, cross * cross),
                )
            };
            let improved = score.simd_gt(best_scores);
            best_scores = improved.select(score, best_scores);
            best_lanes = improved.cast::<i64>().select(cursor, best_lanes);
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "SIMD cursor lanes are bounded source ordinals"
            )]
            {
                cursor += std::simd::Simd::splat(REDUCE_LANES as u64);
            }
            index = index.wrapping_add(REDUCE_LANES);
        }
        for (&score, &source_index) in best_scores.as_array().iter().zip(best_lanes.as_array()) {
            if score > best {
                best = score;
                farthest = source_index as usize;
            }
        }
    }
    let scalar_xs = xs
        .get(index..end)
        .expect("Douglas-Peucker scalar suffix belongs to its source range");
    let scalar_ys = ys
        .get(index..end)
        .expect("Douglas-Peucker scalar suffix belongs to its source range");
    for (offset, (&x, &y)) in scalar_xs.iter().zip(scalar_ys).enumerate() {
        let scalar_index = index.wrapping_add(offset);
        let (qx, qy) = (x - ax, y - ay);
        let along = qx * dx + qy * dy;
        let score = if along <= 0.0 {
            (qx * qx + qy * qy) * chord_squared
        } else if along >= chord_squared {
            let (rx, ry) = (x - bx, y - by);
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
pub(crate) fn strict_cycle(coords: &CoordSeq, clockwise: bool) -> Option<Vec<Point>> {
    let mut ring = Vec::with_capacity(coords.len());
    ring.extend(coords.iter_coords());
    dedup_consecutive_points(&mut ring);
    if ring
        .first()
        .zip(ring.last())
        .is_some_and(|(first, last)| same_point(*first, *last))
    {
        ring.pop();
    }
    if ring.len() < 3 {
        return None;
    }
    if open_point_cycle_winding(&ring).reverse_for_clockwise(clockwise) {
        ring.reverse();
    }
    let mut strict = Vec::with_capacity(ring.len());
    for (index, &vertex) in ring.iter().enumerate() {
        let previous = *ring
            .get(index.wrapping_sub(1))
            .or_else(|| ring.last())
            .expect("a strict cycle has a previous vertex");
        let next = *ring
            .get(index.wrapping_add(1))
            .or_else(|| ring.first())
            .expect("a strict cycle has a next vertex");
        if orientation(previous, vertex, next) != Orientation::Collinear {
            strict.push(vertex);
        }
    }
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
fn source_median_cut(shape: &Shape, split_x: bool, bounds: Bounds) -> Option<f64> {
    let mut ordinates = Vec::with_capacity(shape.coord_count());
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
    ordinates
        .windows(2)
        .enumerate()
        .filter_map(|(lower_index, pair)| {
            let upper_index = lower_index.wrapping_add(1);
            let lower = *pair.first().expect("median window has a lower ordinate");
            let upper = *pair.last().expect("median window has an upper ordinate");
            if lower >= upper {
                return None;
            }
            let cut = f64::midpoint(lower, upper);
            (low < cut && cut < high).then_some((upper_index.abs_diff(middle), upper_index, cut))
        })
        .min_by_key(|&(distance, index, _)| (distance, index))
        .map(|(_, _, cut)| cut)
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

    let mut work = Vec::with_capacity(1);
    let mut staged = Vec::new();
    let shape_bounds = shape.bounds();
    work.push(WorkItem {
        shape,
        bounds: shape_bounds,
        terminal: false,
    });
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
            staged.push(item.shape);
            continue;
        }
        let Some(bounds) = item.bounds else {
            staged.push(item.shape);
            continue;
        };

        let x_cut = source_median_cut(&item.shape, true, bounds);
        let y_cut = source_median_cut(&item.shape, false, bounds);
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
            staged.push(item.shape);
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
            staged.push(item.shape);
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
            work.push(WorkItem {
                shape: child,
                bounds: child_bounds,
                terminal,
            });
        }
        #[cfg(test)]
        if progressed {
            SUBDIVISION_WORK.with(|work| {
                let (items, splits) = work.get();
                work.set((items, splits.saturating_add(1)));
            });
        }
    }
    out.reserve(staged.len());
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
    #[cfg(test)]
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
    Ok(
        kept_coords(ring.coords(), scratch, keep_mask)?.map_or(RingKeep::Same, |coords| {
            Ring::from_closed(coords).map_or(RingKeep::Drop, RingKeep::New)
        }),
    )
}

enum PolygonKeep {
    Same,
    Empty,
    New(Polygon),
}

fn clone_ring_prefix(holes: &[Ring], end: usize) -> Vec<Ring> {
    holes
        .get(..end)
        .expect("hole prefix ends within the source holes")
        .to_vec()
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
    let mut holes_out = new_shell
        .is_some()
        .then(|| Vec::with_capacity(polygon.holes.len()));
    for (index, hole) in polygon.holes.iter().enumerate() {
        match keep_ring(hole, scratch, keep_mask)? {
            RingKeep::Same => {
                if let Some(holes) = holes_out.as_mut() {
                    holes.push(hole.clone());
                }
            },
            RingKeep::Drop => {
                if holes_out.is_none() {
                    holes_out = Some(clone_ring_prefix(&polygon.holes, index));
                }
            },
            RingKeep::New(ring) => {
                if let Some(holes) = holes_out.as_mut() {
                    holes.push(ring);
                } else {
                    let mut holes = clone_ring_prefix(&polygon.holes, index);
                    holes.push(ring);
                    holes_out = Some(holes);
                }
            },
        }
    }
    Ok(holes_out.map_or(PolygonKeep::Same, |holes| {
        PolygonKeep::New(Polygon::new(
            new_shell.unwrap_or_else(|| polygon.shell.clone()),
            holes,
        ))
    }))
}

fn prefix_members<T: Clone>(
    values: &[T],
    members: impl ExactSizeIterator<Item = MultiMember<usize>>,
    end: usize,
) -> Vec<MultiMember<T>> {
    let mut values = values.iter();
    let mut out = Vec::with_capacity(members.len());
    out.extend(members.take(end).map(|member| {
        match member {
            MultiMember::Empty => MultiMember::Empty,
            MultiMember::Value(_) => MultiMember::Value(
                values
                    .next()
                    .expect("sparse member values follow stored value order")
                    .clone(),
            ),
        }
    }));
    out
}

fn keep_multi_lines(
    lines: &MultiLineString,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
) -> Result<RawSimplify> {
    let mut out: Option<Vec<MultiMember<LineSeq>>> = None;
    let mut values = lines.iter();
    for (index, member) in lines.members().enumerate() {
        match member {
            MultiMember::Empty => {
                if let Some(members) = out.as_mut() {
                    members.push(MultiMember::Empty);
                }
            },
            MultiMember::Value(_) => {
                let line = values
                    .next()
                    .expect("sparse member values follow stored line order");
                match kept_coords(line, scratch, keep_mask)? {
                    None => {
                        if let Some(members) = out.as_mut() {
                            members.push(MultiMember::Value(line.clone()));
                        }
                    },
                    Some(coords) => {
                        if let Some(members) = out.as_mut() {
                            members.push(MultiMember::Value(LineSeq::from_proven(coords)));
                        } else {
                            let mut members =
                                prefix_members(lines.as_slice(), lines.members(), index);
                            members.push(MultiMember::Value(LineSeq::from_proven(coords)));
                            out = Some(members);
                        }
                    },
                }
            },
        }
    }
    Ok(match out {
        None => RawSimplify::Unchanged,
        Some(members) => RawSimplify::Changed(Shape::MultiLineString(MultiLineString::try_new(
            lines.axes(),
            members,
        )?)),
    })
}

fn keep_multi_polygons(
    polygons: &MultiPolygon,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
) -> Result<RawSimplify> {
    let mut out: Option<Vec<MultiMember<Polygon>>> = None;
    let mut values = polygons.iter();
    for (index, member) in polygons.members().enumerate() {
        match member {
            MultiMember::Empty => {
                if let Some(members) = out.as_mut() {
                    members.push(MultiMember::Empty);
                }
            },
            MultiMember::Value(_) => {
                let polygon = values
                    .next()
                    .expect("sparse member values follow stored polygon order");
                match keep_polygon(polygon, scratch, keep_mask)? {
                    PolygonKeep::Same => {
                        if let Some(members) = out.as_mut() {
                            members.push(MultiMember::Value(polygon.clone()));
                        }
                    },
                    PolygonKeep::Empty => {
                        if let Some(members) = out.as_mut() {
                            members.push(MultiMember::Empty);
                        } else {
                            let mut members =
                                prefix_members(polygons.as_slice(), polygons.members(), index);
                            members.push(MultiMember::Empty);
                            out = Some(members);
                        }
                    },
                    PolygonKeep::New(polygon) => {
                        if let Some(members) = out.as_mut() {
                            members.push(MultiMember::Value(polygon));
                        } else {
                            let mut members =
                                prefix_members(polygons.as_slice(), polygons.members(), index);
                            members.push(MultiMember::Value(polygon));
                            out = Some(members);
                        }
                    },
                }
            },
        }
    }
    Ok(match out {
        None => RawSimplify::Unchanged,
        Some(members) => RawSimplify::Changed(Shape::MultiPolygon(MultiPolygon::try_new(
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
                let mut values = polygons.iter();
                let members = polygons
                    .members()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => crate::geometry::MultiMember::Empty,
                        crate::geometry::MultiMember::Value(_) => clean(
                            values
                                .next()
                                .expect("sparse polygon values follow member order"),
                        )
                        .map_or(
                            crate::geometry::MultiMember::Empty,
                            crate::geometry::MultiMember::Value,
                        ),
                    })
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    Self::empty_polygon()
                } else {
                    Self::MultiPolygon(MultiPolygon::try_new(polygons.axes(), members)?)
                }
            },
            Self::GeometryCollection(parts) => Self::GeometryCollection(
                parts.try_map_owned_members(Self::dropping_collapsed_rings)?,
            ),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::LineString(_)
            | Self::MultiLineString(_)
            | Self::Empty(..) => self,
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
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Ok(true),
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
        let mut segments = Vec::with_capacity(self.segment_count());
        let Ok(()) = self.try_for_each_segment_chain(|chain| {
            segments.extend(line_segments(chain));
            Ok::<_, std::convert::Infallible>(())
        });
        let mut vertex_cloud = Vec::with_capacity(self.coord_count());
        // Original XY only: the guard indexes the source point set, never
        // simplified output. Isolated points participate in the vertex
        // probe, so the walk is every vertex, not only segment chains.
        let Ok(()) = self.try_for_each_point(|point| {
            vertex_cloud.push([point.x, point.y]);
            Ok::<_, std::convert::Infallible>(())
        });
        let guard = TopologyGuard::new(&segments, vertex_cloud);
        self.map_chains(&|points: &CoordSeq| {
            let xs = points.xs();
            let ys = points.ys();
            let count = xs.len();
            let closed = count >= 2
                && xs
                    .first()
                    .zip(ys.first())
                    .zip(xs.last().zip(ys.last()))
                    .is_some_and(|((first_x, first_y), (last_x, last_y))| {
                        same_point(XY::new(*first_x, *first_y), XY::new(*last_x, *last_y))
                    });
            let kept = simplify_chain_guarded(
                count,
                |index| {
                    XY::new(
                        *xs.get(index)
                            .expect("guarded simplification vertex belongs to its source"),
                        *ys.get(index)
                            .expect("guarded simplification vertex belongs to its source"),
                    )
                },
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

    #[cfg(test)]
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

    #[cfg(test)]
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
            Ok(vw_keep(xs, ys, tolerance, &mut scratch.keep))
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
                                members.push(geometry.clone());
                            }
                        },
                        RawSimplify::Changed(shape) => {
                            if let Some(members) = out.as_mut() {
                                members.push(shape);
                            } else {
                                let mut members = Vec::with_capacity(geometries.len());
                                members.extend(
                                    geometries
                                        .as_slice()
                                        .get(..index)
                                        .expect("collection prefix ends at the changed member")
                                        .iter()
                                        .cloned(),
                                );
                                members.push(shape);
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
        crate::test_support::assert_no_allocations(|| {
            assert!(!has_three_distinct_xy(&two));
            assert!(has_three_distinct_xy(&three));
        });
    }
}

#[cfg(test)]
mod keep_identity_tests {

    use super::RawSimplify;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, GeometryKind, LineSeq, MultiLineString, MultiPolygon, Polygon,
        Ring, Shape,
    };

    fn line_xy(xs: &[f64], ys: &[f64]) -> LineSeq {
        LineSeq::from_proven(CoordSeq::try_from_vecs(xs.into(), ys.into(), None, None).unwrap())
    }

    fn closed_ring(xs: &[f64], ys: &[f64]) -> Ring {
        Ring::from_closed(CoordSeq::try_from_vecs(xs.into(), ys.into(), None, None).unwrap())
            .unwrap()
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
        let polygon = Shape::Polygon(Polygon::new(shell, vec![keep_a, drop, keep_b]));
        // 0.5 keeps the 1×1 holes (diagonal offset ≈ 0.707) and drops the
        // 0.01-high spike below four closed vertices.
        let Shape::Polygon(out) = polygon.simplify_dp_raw(0.5).unwrap() else {
            panic!("shell remains a polygon");
        };
        let [kept_a, kept_b] = out.holes.as_slice() else {
            panic!("both 1x1 holes survive the simplification");
        };
        assert_eq!(kept_a.xs().as_ptr(), a_ptr);
        assert_eq!(kept_b.xs().as_ptr(), b_ptr);
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
        let shape =
            Shape::MultiLineString(MultiLineString::from_dense(vec![first, middle, last]).unwrap());
        let Shape::MultiLineString(out) = shape.simplify_dp(0.0, true).unwrap() else {
            panic!("multilinestring remains");
        };
        assert_eq!(out.as_slice()[0].xs().as_ptr(), first_ptr);
        assert_eq!(out.as_slice()[1].xs(), &[0.0, 2.0]);
        assert_eq!(out.as_slice()[2].xs().as_ptr(), last_ptr);
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
        assert_eq!(out.member_count(), 2);
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
    use crate::geometry::{CoordSeq, Polygon, Ring, Shape};

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
        .unwrap()
    }

    fn sliver_xyzm() -> Shape {
        Shape::Polygon(Polygon::new(
            closed_ring(
                &[0.0, 1.0, 0.5, 0.0],
                &[0.0, 0.0, 0.1, 0.0],
                Some(&[1.0, 2.0, 3.0, 1.0]),
                Some(&[10.0, 20.0, 30.0, 10.0]),
            ),
            Vec::new(),
        ))
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
    fn guarded_select_keeps_source_index_ordinates() {
        let polygon = Shape::Polygon(Polygon::new(
            closed_ring(
                &[0.0, 10.0, 10.0, 5.0, 0.0, 0.0],
                &[0.0, 0.0, 0.01, 0.01, 0.01, 0.0],
                Some(&[1.0, 2.0, 3.0, 4.0, 5.0, 1.0]),
                Some(&[10.0, 20.0, 30.0, 40.0, 50.0, 10.0]),
            ),
            Vec::new(),
        ));
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
    use crate::geometry::constructive::vw_keep;

    fn collinear_xy(count: usize) -> (Vec<f64>, Vec<f64>) {
        ((0..count).map(|index| index as f64).collect(), vec![
            0.0;
            count
        ])
    }

    #[test]
    fn rdp_keep_removes_collinear_interior_vertices() {
        let (xs, ys) = collinear_xy(8);
        let run = || {
            let mut keep = Vec::new();
            let mut stack = Vec::new();
            rdp_keep(&xs, &ys, 0.0, &mut keep, &mut stack)
        };
        let kept = run().unwrap();
        assert_eq!(kept, Some(2));
    }

    #[test]
    fn vw_keep_removes_collinear_interior_vertices() {
        let (xs, ys) = collinear_xy(8);
        let run = || {
            let mut keep = Vec::new();
            vw_keep(&xs, &ys, 1.0, &mut keep)
        };
        let kept = run();
        assert_eq!(kept, Some(2));
    }

    #[test]
    fn reused_keep_storage_handles_a_larger_chain() {
        let small = collinear_xy(8);
        let mut keep = Vec::new();
        let mut stack = Vec::new();
        rdp_keep(&small.0, &small.1, 0.0, &mut keep, &mut stack).unwrap();
        let capacity = keep.capacity();
        assert!(capacity >= 8);
        let (xs, ys) = collinear_xy(capacity + 1);
        stack.clear();
        assert_eq!(
            rdp_keep(&xs, &ys, 0.0, &mut keep, &mut stack).unwrap(),
            Some(2)
        );

        let mut keep = Vec::new();
        vw_keep(&small.0, &small.1, 1.0, &mut keep);
        let capacity = keep.capacity();
        let (xs, ys) = collinear_xy(capacity + 1);
        assert_eq!(vw_keep(&xs, &ys, 1.0, &mut keep), Some(2));
    }
}

#[cfg(test)]
mod subdivision_tests {
    use super::*;
    use crate::geometry::overlay::{partition_calls, reset_partition_calls};

    fn ring_shape(points: &[Point]) -> Shape {
        Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::try_from_points(points).unwrap()).unwrap(),
            Vec::new(),
        ))
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
        let shape = Shape::Polygon(Polygon::new(
            Ring::from_closed(coordinates).unwrap(),
            Vec::new(),
        ));
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
