use super::arcs::{emit_arc, emit_cap_with_peer, source_vector, unit_right_normal};
use super::loops::stroke_loops;
use super::*;

/// Stroke lineal input through the winding engine. The requested cap applies
/// at each open source-chain endpoint; interior turns, including reversals,
/// follow `rule`. Overlapping strokes (self-intersections, sharp folds,
/// multi-part overlaps) resolve in the `winding >= 1` selection; zero-length
/// chains are disks (GEOS semantics).
/// `None` means that no usable constructive loop remained; non-finite
/// derived coordinates are returned as typed errors.
pub(in crate::geometry) fn winding_stroke_budgeted(
    chains: &[&CoordSeq],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    let mut loops: Vec<LoopColumns> = Vec::new();
    let mut clean = true;
    for chain in chains {
        let Some(stroke_clean) = stroke_loops(
            &mut loops,
            chain,
            distance,
            cap_style,
            rule,
            quadrant_segments,
            budget,
        )?
        else {
            return Ok(None);
        };
        clean &= stroke_clean;
    }
    if loops.is_empty() {
        // Every chain was empty: the buffer is legitimately empty.
        return Ok(Some(Shape::empty_polygon()));
    }
    // Direct-stroke fast path: a SINGLE emitted loop with only OUTSIDE
    // joins that closes into a simple, positive-area ring IS the buffer
    // (the `winding >= 1` region of one simple CCW loop is exactly its
    // interior) — no self-noding, no arrangement. Inside joins cancel in
    // the winding selection, so `clean == false` skips the doomed
    // simplicity probe entirely; distant-approach overlaps still fail it
    // and take the winding engine unchanged.
    if clean
        && let [(xs, ys)] = loops.as_slice()
        && let Some(polygon) = simple_positive_loop_polygon(xs, ys, budget)?
    {
        return Ok(Some(Shape::Polygon(polygon)));
    }
    let parts = winding_region(&loops, |winding| winding >= 1)?;
    if parts.is_empty() {
        return Ok(None);
    }
    Ok(Some(polygon_parts_to_shape(parts)))
}

/// One positive-winding loop as a directly valid polygon: close the columns
/// into a ring and accept only a simple result; all other cases use the same
/// exact winding selection as overlapping loops.
fn simple_positive_loop_polygon(
    xs: &[f64],
    ys: &[f64],
    budget: &mut ExpansionBudget,
) -> Result<Option<Polygon>> {
    if xs.len() < 3 {
        return Ok(None);
    }
    // The direct-stroke result owns this final closure; charge it before the
    // exact final columns are allocated so raw-walk accounting cannot stop one
    // coordinate short of the public ring.
    let capacity = xs.len().wrapping_add(1);
    budget.add(1)?;
    let mut ring_xs = Vec::with_capacity(capacity);
    let mut ring_ys = Vec::with_capacity(capacity);
    ring_xs.extend_from_slice(xs);
    ring_ys.extend_from_slice(ys);
    ring_xs.push(*xs.first().expect("nonempty stroke loop"));
    ring_ys.push(*ys.first().expect("nonempty stroke loop"));
    let ring = CoordSeq::try_from_vecs(ring_xs, ring_ys, None, None)?;
    if ring_winding(&ring).is_ccw()
        && Shape::LineString(LineSeq::from_proven(ring.clone())).is_simple()?
    {
        Ok(Some(Polygon::new(Ring::try_from_closed(ring)?, Vec::new())))
    } else {
        Ok(None)
    }
}

/// One-sided strips share the winding stroke's positive-face selection. Each
/// source contributes one source/offset ring; a simple sole ring needs no
/// arrangement, while overlapping or self-crossing rings are selected together.
pub(in crate::geometry) fn winding_sided_stroke_budgeted(
    chains: &[&CoordSeq],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let mut loops = Vec::with_capacity(chains.len());
    for chain in chains {
        if let Some(loop_columns) =
            sided_buffer_loop(chain, distance, rule, quadrant_segments, budget)?
        {
            loops.push(loop_columns);
        }
    }
    if loops.is_empty() {
        return Ok(Shape::empty_polygon());
    }
    if let [(xs, ys)] = loops.as_slice()
        && let Some(polygon) = simple_positive_loop_polygon(xs, ys, budget)?
    {
        return Ok(Shape::Polygon(polygon));
    }
    let parts = winding_region(&loops, |winding| winding >= 1)?;
    Ok(polygon_parts_to_shape(parts))
}

/// Buffer of a whole `GeometryCollection` in ONE winding pass: every part
/// reduces to raw loops in the SAME arrangement — circles for points,
/// stroke loops for chains, offset loops for polygons — and the union of
/// all the part buffers IS the `winding >= 1` region (GEOS instead
/// buffers each part and runs a boolean union cascade). Negative
/// distances first normalize the selected polygonal support, then erode that
/// one union; puntal/lineal parts annihilate exactly.
#[cfg(test)]
pub(crate) fn winding_stroke(
    chains: &[&CoordSeq],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let mut budget = ExpansionBudget::new("constructive test", "quadrant_segments");
    winding_stroke_budgeted(
        chains,
        distance,
        cap_style,
        rule,
        quadrant_segments,
        &mut budget,
    )
    .expect("bounded constructive test must not exhaust generated-work budget")
}

/// The extent segment `[lo, hi]` of a monotone collinear chain with at least
/// three distinct-adjacent vertices, or `None` when the chain bends or reverses.
/// Only monotone chains can collapse to the extent without changing which
/// source vertices own endpoint caps and interior joins.
pub(super) fn collinear_extent(chain: &[Point]) -> Option<[Point; 2]> {
    let (&anchor, tail) = chain.split_first()?;
    let direction = *tail.iter().find(|point| !same_point(**point, anchor))?;
    if chain
        .iter()
        .any(|&point| orientation(anchor, direction, point) != Orientation::Collinear)
    {
        return None;
    }
    if chain
        .array_windows::<3>()
        .any(|[a, b, c]| middle_outside_segment_bounds(*a, *b, *c))
    {
        return None;
    }
    // One non-constant ordinate orders every point on a collinear chain.
    // Comparing stored coordinates avoids the distance square and dot
    // product, both of which can overflow for finite opposite-sign sources.
    let axis_x = !same_topological_coordinate(direction.x, anchor.x);
    let ordinate = |point: &Point| {
        if axis_x { point.x } else { point.y }
    };
    let lo = *chain
        .iter()
        .min_by(|left, right| ordinate(left).total_cmp(&ordinate(right)))?;
    let hi = *chain
        .iter()
        .max_by(|left, right| ordinate(left).total_cmp(&ordinate(right)))?;
    Some([lo, hi])
}

/// Two cyclic offset loops around a closed line. A closed line has no caps:
/// its forward and reversed walks meet at the source's first vertex through
/// their own cyclic joins, and the winding arrangement combines the outer
/// and inner loops into the stroke region.
pub(super) fn raw_closed_stroke_loops(
    chain: &[Point],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<(LoopColumns, LoopColumns, bool)>> {
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments.get());
    let Some(right_plan) = WalkPlan::new_stroke(chain, true, distance, rule)? else {
        return Ok(None);
    };
    let mut backward = Vec::with_capacity(chain.len());
    let first = *chain
        .first()
        .expect("a closed walk plan has a source start vertex");
    backward.push(first);
    backward.extend(chain.iter().skip(1).rev().copied());
    let Some(left_plan) = WalkPlan::new_stroke(&backward, true, distance, rule)? else {
        return Ok(None);
    };
    let Some(right_columns) = materialize_walk(&right_plan, step_angle, false, budget, true)?
    else {
        return Ok(None);
    };
    let Some(left_columns) = materialize_walk(&left_plan, step_angle, false, budget, true)? else {
        return Ok(None);
    };
    let (right_xs, right_ys, _) = right_columns.into_columns();
    let (left_xs, left_ys, _) = left_columns.into_columns();
    let clean = right_plan
        .joins
        .iter()
        .chain(&left_plan.joins)
        .all(|join| !matches!(join, WalkJoin::Excursion));
    Ok(Some(((right_xs, right_ys), (left_xs, left_ys), clean)))
}

/// One closed raw stroke loop around an open chain (see
/// [`winding_stroke_budgeted`]). Interior joins follow the polygon rules: the
/// styled join on the outside of a turn, crossings (or through-vertex
/// excursions for deep folds) on the inside.
pub(crate) fn raw_stroke_loop(
    chain: &[Point],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<(LoopColumns, bool)>> {
    let quadrant_segments = quadrant_segments.get();
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments);
    let Some(right_plan) = WalkPlan::new_stroke(chain, false, distance, rule)? else {
        return Ok(None);
    };
    let start = *chain
        .first()
        .expect("a planned open stroke has a source start vertex");
    let end = *chain
        .last()
        .expect("a planned open stroke has a source end vertex");
    // Keep the reversed source walk alive alongside its plan. The plan is
    // immutable after validation, and retaining both plans across the count
    // and materialization passes prevents a second normal/join calculation.
    thread_local! {
        static BACKWARD: crate::scratch_pool::ScratchPool<Vec<Point>> = const {
            crate::scratch_pool::ScratchPool::new(Vec::new())
        };
    }
    let mut backward = crate::scratch_pool::ScratchPool::checkout(&BACKWARD);
    backward.reserve(chain.len());
    backward.extend(chain.iter().rev().copied());
    let Some(left_plan) = WalkPlan::new_stroke(backward.as_slice(), false, distance, rule)? else {
        return Ok(None);
    };
    let (enx, eny) = right_plan.last_normal();
    // The reversed walk's final edge is the source's first edge; its normal
    // closes the start cap on the left side of the original chain.
    let (snx, sny) = left_plan.last_normal();

    let (clean, count_len) = {
        let mut count_only = WalkCount::budgeted(budget);
        let clean = emit_stroke_loop(
            &right_plan,
            &left_plan,
            end,
            enx,
            eny,
            start,
            snx,
            sny,
            distance,
            cap_style,
            step_angle,
            &mut count_only,
        )?;
        if !count_only.all_finite() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "buffer calculation produced non-finite coordinates",
            ));
        }
        (clean, count_only.len())
    };
    budget.add(count_len)?;
    let mut columns = WalkColumns::new(count_len, false);
    let real_clean = emit_stroke_loop(
        &right_plan,
        &left_plan,
        end,
        enx,
        eny,
        start,
        snx,
        sny,
        distance,
        cap_style,
        step_angle,
        &mut columns,
    )?;
    debug_assert_eq!(columns.len(), count_len);
    debug_assert_eq!(real_clean, clean);
    let (xs, ys, _) = columns.into_columns();
    Ok(Some(((xs, ys), clean && real_clean)))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the stroke emitter receives one immutable plan per side plus the output sink; bundling them would obscure the two-sided walk"
)]
fn emit_stroke_loop<S: WalkSink>(
    right_plan: &WalkPlan<'_>,
    left_plan: &WalkPlan<'_>,
    end: Point,
    enx: f64,
    eny: f64,
    start: Point,
    snx: f64,
    sny: f64,
    distance: f64,
    cap_style: BufferCapStyle,
    step_angle: f64,
    sink: &mut S,
) -> Result<bool> {
    // Forward (right) side, end cap, backward (left) side, start cap.
    let right_clean = stroke_side(right_plan, step_angle, sink)?;
    emit_cap_with_peer(
        end,
        enx,
        eny,
        start,
        distance,
        cap_style,
        step_angle,
        right_plan.source_len().wrapping_sub(1) as u32,
        sink,
    )?;
    let left_clean = stroke_side(left_plan, step_angle, sink)?;
    emit_cap_with_peer(
        start, snx, sny, end, distance, cap_style, step_angle, 0, sink,
    )?;
    Ok(right_clean && left_clean)
}

/// One directed side of a stroke: right-offset edges with the polygon
/// join rules. The left-side caller supplies a plan over the reversed walk.
/// Emit one stroke side; `true` when no join fell back to an
/// `Excursion` — a consuming inside `Cross` trims its overlap away, but
/// an excursion lobe cancels ONLY in the winding selection, so its raw
/// loop can never be a directly valid ring.
pub(crate) fn stroke_side<S: WalkSink>(
    plan: &WalkPlan<'_>,
    step_angle: f64,
    sink: &mut S,
) -> Result<bool> {
    let excursion_free = plan
        .joins
        .iter()
        .all(|join| !matches!(join, WalkJoin::Excursion));
    plan.emit_into(step_angle, sink)?;
    Ok(excursion_free)
}
