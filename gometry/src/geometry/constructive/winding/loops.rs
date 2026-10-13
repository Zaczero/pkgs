use super::stroke::{collinear_extent, raw_closed_stroke_loops, raw_stroke_loop};
use super::*;

/// One ordinate-column loop of the raw offset linework.
pub(crate) type LoopColumns = (Vec<f64>, Vec<f64>);

/// Append one polygon's expansion loops: the shell's offset loop plus
/// each hole shrunk in its own arrangement (inward offsets can INVERT —
/// partially or fully — and the per-loop cleaning keeps exactly the
/// surviving lobes, CCW pieces re-oriented CW so they subtract).
pub(crate) fn expansion_loops(
    loops: &mut Vec<LoopColumns>,
    polygon: &Polygon,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<()>> {
    let Some(shell) = strict_cycle(polygon.shell.coords(), false) else {
        return Ok(None);
    };
    let Some(raw) = raw_offset_loop(&shell, distance, rule, quadrant_segments, budget)? else {
        return Ok(None);
    };
    loops.push(raw);
    for hole in polygon.holes.iter() {
        // Degenerate (zero-area) holes do not affect the buffer.
        let Some(ring) = strict_cycle(hole.coords(), true) else {
            continue;
        };
        let Some(raw) = raw_offset_loop(&ring, distance, rule, quadrant_segments, budget)? else {
            return Ok(None);
        };
        extend_cleaned(loops, &raw)?;
    }
    Ok(Some(()))
}

/// Append one polygon's erosion loops (see [`winding_erosion_budgeted`]): the
/// flipped-orientation walks, with the SHELL taking the per-loop
/// inversion cleaning.
pub(crate) fn erosion_loops(
    loops: &mut Vec<LoopColumns>,
    polygon: &Polygon,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<()>> {
    let Some(shell) = strict_cycle(polygon.shell.coords(), true) else {
        return Ok(None);
    };
    let Some(raw) = raw_offset_loop(&shell, distance, rule, quadrant_segments, budget)? else {
        return Ok(None);
    };
    // Surviving eroded lobes are CW pieces; keep them CW (they ARE the
    // negative-winding region's boundary in the final pass).
    extend_cleaned(loops, &raw)?;
    for hole in polygon.holes.iter() {
        let Some(ring) = strict_cycle(hole.coords(), false) else {
            continue;
        };
        // Grown holes never invert; their self-crossings (reflex
        // corners) resolve in the global winding.
        let Some(raw) = raw_offset_loop(&ring, distance, rule, quadrant_segments, budget)? else {
            return Ok(None);
        };
        loops.push(raw);
    }
    Ok(Some(()))
}

/// Append the stroke loop for one chain; a chain that dedups to one point is
/// a full disk, and an empty chain contributes nothing.
/// `true` (clean) when the emitted loop used only outside joins — the
/// precondition for the direct-stroke fast path even being plausible.
pub(crate) fn stroke_loops(
    loops: &mut Vec<LoopColumns>,
    chain: &CoordSeq,
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<bool>> {
    // Reused per-thread gather: the second of the two per-row emitter
    // allocations (the reversed-side scratch is the other).
    thread_local! {
        static GATHER: crate::scratch_pool::ScratchPool<Vec<Point>> = const {
            crate::scratch_pool::ScratchPool::new(Vec::new())
        };
    }
    let mut points = crate::scratch_pool::ScratchPool::checkout(&GATHER);
    points.reserve(chain.len());
    points.extend(chain.iter_coords());
    dedup_consecutive_points(&mut points);

    match points.as_slice() {
        [] => Ok(Some(true)),
        [point] => {
            loops.push(circle_loop(*point, distance, quadrant_segments, budget)?);
            // A lone circle is simple by construction.
            Ok(Some(true))
        },
        chain => {
            // A monotone collinear chain can collapse to its extent segment:
            // its original endpoints are the extrema and it has no interior
            // reversal join to preserve.
            let extent;
            let chain: &[Point] = if chain.len() >= 3
                && let Some(segment) = collinear_extent(chain)
            {
                extent = segment;
                &extent
            } else {
                chain
            };
            if chain.len() >= 3
                && chain
                    .first()
                    .zip(chain.last())
                    .is_some_and(|(first, last)| same_point(*first, *last))
            {
                let Some((right, left, clean)) = raw_closed_stroke_loops(
                    chain
                        .get(..chain.len().wrapping_sub(1))
                        .expect("closed stroke omits its stored closure vertex"),
                    distance,
                    rule,
                    quadrant_segments,
                    budget,
                )?
                else {
                    return Ok(None);
                };
                loops.push(right);
                loops.push(left);
                return Ok(Some(clean));
            }
            let Some((columns, clean)) =
                raw_stroke_loop(chain, distance, cap_style, rule, quadrant_segments, budget)?
            else {
                return Ok(None);
            };
            loops.push(columns);
            Ok(Some(clean))
        },
    }
}

/// One full-circle CCW loop around `center` — the buffer of a point and
/// the stroke of a zero-length chain. Inscribed vertices on the circle,
/// per the arc doctrine (`4 * quadrant_segments` steps).
const fn circle_step_count(quadrant_segments: std::num::NonZeroU32) -> usize {
    (quadrant_segments.get() as usize).wrapping_mul(4)
}

pub(crate) fn circle_loop(
    center: Point,
    distance: f64,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<LoopColumns> {
    let steps = circle_step_count(quadrant_segments);
    // Circle size is exact without walking it, so admit the exact realized
    // count before the allocation rather than applying an input estimate.
    budget.add(steps)?;
    let mut xs = Vec::with_capacity(steps);
    let mut ys = Vec::with_capacity(steps);
    // Rotation recurrence — one `sin_cos` per circle (see `emit_arc`).
    let (sin_step, cos_step) = (std::f64::consts::TAU / steps as f64).sin_cos();
    let (mut sin_theta, mut cos_theta) = (0.0_f64, 1.0_f64);
    for step in 0..steps {
        if step > 0 {
            (cos_theta, sin_theta) = (
                cos_theta * cos_step - sin_theta * sin_step,
                sin_theta * cos_step + cos_theta * sin_step,
            );
        }
        xs.push(center.x + distance * cos_theta);
        ys.push(center.y + distance * sin_theta);
    }
    if !column_all_finite(&xs) || !column_all_finite(&ys) {
        return Err(crate::geometry::GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    Ok((xs, ys))
}

/// Append one circle for every distinct topological X/Y coordinate, retaining
/// the first member's ordinates as the disk center provenance.
pub(crate) fn multipoint_loops(
    loops: &mut Vec<LoopColumns>,
    points: &MultiPoint,
    distance: f64,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let mut seen = HashSet::with_capacity(points.len());
    for point in points.iter_coords() {
        // `canonical_f64_bits` matches `same_topological_coordinate`: ±0.0
        // share one key while every other finite coordinate remains bit-exact.
        if !seen.insert((canonical_f64_bits(point.x), canonical_f64_bits(point.y))) {
            continue;
        }
        loops.push(circle_loop(point, distance, quadrant_segments, budget)?);
    }
    Ok(())
}

/// The buffer of a single point: the inscribed-circle polygon directly —
/// no arrangement work at all.
pub(crate) fn point_buffer(
    center: Point,
    distance: f64,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let steps = circle_step_count(quadrant_segments);
    // `circle_loop` emits the open circle and `close_xy_loop` appends the
    // statically known final coordinate. Reserve the whole realized polygon
    // before the 16M open-circle allocation, not one coordinate too late.
    budget.check_additional(steps.wrapping_add(1))?;
    let (mut xs, mut ys) = circle_loop(center, distance, quadrant_segments, budget)?;
    close_xy_loop(&mut xs, &mut ys, budget)?;
    let seq = CoordSeq::from_owned_columns(xs, ys, None, None)?;
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(seq)?,
        Vec::new(),
    )))
}

/// Collect expansion loops across a collection's parts, recursively.
pub(super) fn collection_loops(
    loops: &mut Vec<LoopColumns>,
    parts: &[Shape],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<()>> {
    for part in parts {
        match part {
            Shape::Point(point) => {
                loops.push(circle_loop(*point, distance, quadrant_segments, budget)?);
            },
            Shape::MultiPoint(points) => {
                multipoint_loops(loops, points, distance, quadrant_segments, budget)?;
            },
            Shape::LineString(chain) => {
                if stroke_loops(
                    loops,
                    chain,
                    distance,
                    cap_style,
                    rule,
                    quadrant_segments,
                    budget,
                )?
                .is_none()
                {
                    return Ok(None);
                }
            },
            Shape::MultiLineString(lines) => {
                for chain in lines {
                    if stroke_loops(
                        loops,
                        chain,
                        distance,
                        cap_style,
                        rule,
                        quadrant_segments,
                        budget,
                    )?
                    .is_none()
                    {
                        return Ok(None);
                    }
                }
            },
            Shape::Polygon(polygon) => {
                if expansion_loops(loops, polygon, distance, rule, quadrant_segments, budget)?
                    .is_none()
                {
                    return Ok(None);
                }
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    if expansion_loops(loops, polygon, distance, rule, quadrant_segments, budget)?
                        .is_none()
                    {
                        return Ok(None);
                    }
                }
            },
            Shape::GeometryCollection(inner) => {
                if collection_loops(
                    loops,
                    inner,
                    distance,
                    cap_style,
                    rule,
                    quadrant_segments,
                    budget,
                )?
                .is_none()
                {
                    return Ok(None);
                }
            },
            Shape::Empty(..) => {},
        }
    }
    Ok(Some(()))
}
