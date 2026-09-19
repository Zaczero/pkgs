#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::cmp::Ordering;

use ahash::HashSetExt as _;

use crate::geometry::constructive::{
    Result, WalkColumns, WalkCount, WalkJoin, WalkJoinRule, WalkPlan, WalkSink, close_xy_loop,
    emit_cap, extend_cleaned, materialize_walk, raw_offset_loop, strict_cycle, winding_region,
};
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    BufferCapStyle, CoordSeq, Coordinates as _, ExpansionBudget, GeometryErrorKind, GeometryKind,
    HashSet, LineSeq, MultiPoint, Orientation, Point, Polygon, Ring, Shape, XY, axis_pow2_scale,
    canonical_f64_bits, column_all_finite, dedup_consecutive_points, interpolate_f64,
    line_is_simple, orientation, polygon_parts_to_shape, power_of_two_exponent, ring_winding,
    same_point, same_topological_coordinate, scale_by_power_of_two, scaled_residual,
    shell_is_convex,
};

/// A polygon encloses no interior when every ring has zero signed area — it
/// is collinear, coincident, or otherwise degenerate, and is geometrically
/// indistinguishable from its boundary linework.
pub(crate) fn polygon_encloses_no_area(polygon: &Polygon) -> bool {
    polygon
        .rings()
        .all(|ring| ring_winding(ring).is_degenerate())
}

/// A degenerate (zero-area, hence collinear) ring as an OPEN polyline: the
/// closing vertex is dropped so the boundary buffers as a stadium around the
/// collinear span, not a retracing closed loop (which the winding engine
/// collapses to a thinner, wrong region). Rings are stored closed, so the
/// final vertex repeats the first; a coincident-point ring collapses to a
/// disk all the same.
pub(crate) fn ring_as_open_line(ring: &CoordSeq) -> Result<CoordSeq> {
    let n = ring.len();
    let take = if n >= 2 && ring.first() == ring.last() {
        n - 1
    } else {
        n
    };
    let mut points = Vec::new();
    crate::try_reserve_vec(&mut points, take)?;
    for index in 0..take {
        crate::try_push_vec(&mut points, ring.point_at(index))?;
    }
    CoordSeq::try_from_points(&points)
}

fn ring_as_open_linework(ring: &CoordSeq) -> Result<Option<LineSeq>> {
    let line = ring_as_open_line(ring)?;
    match line.len() {
        0 => Ok(None),
        1 => {
            let point = line.point_at(0);
            Ok(Some(LineSeq::segment(point, point)?))
        },
        _ => Ok(Some(LineSeq::from_proven(line))),
    }
}

/// Reinterpret a shape's zero-area polygons as their boundary linework so the
/// winding buffer engine can resolve them (see [`buffer_with_style`]).
/// Non-degenerate polygons and non-polygonal parts pass through unchanged.
pub(crate) fn degenerate_polygonal_as_linework(shape: &Shape) -> Result<Shape> {
    Ok(match shape {
        Shape::Polygon(polygon) if polygon_encloses_no_area(polygon) => {
            let mut lines = Vec::new();
            crate::try_reserve_vec(&mut lines, polygon.rings().count())?;
            for ring in polygon.rings() {
                if let Some(line) = ring_as_open_linework(ring.coords())? {
                    crate::try_push_vec(&mut lines, line)?;
                }
            }
            Shape::multi_line_string(polygon.shell.coords().axes(), lines)?
        },
        Shape::MultiPolygon(polygons) => {
            let mut members = Vec::new();
            crate::try_reserve_vec(&mut members, polygons.member_count())?;
            for member in polygons.members() {
                let shape = match member {
                    crate::geometry::MultiMember::Empty => {
                        Shape::typed_empty(GeometryKind::Polygon, polygons.axes())
                    },
                    crate::geometry::MultiMember::Value(index) => degenerate_polygonal_as_linework(
                        &Shape::Polygon(polygons.as_slice()[index].clone()),
                    )?,
                };
                crate::try_push_vec(&mut members, shape)?;
            }
            Shape::geometry_collection(members)?
        },
        Shape::GeometryCollection(parts) => {
            Shape::GeometryCollection(parts.try_map_members(degenerate_polygonal_as_linework)?)
        },
        other => other.clone(),
    })
}

/// Convex hole-free polygonal buffer — constructive fast path for both
/// expansion and certified erosion.
///
/// **Expansion** (`distance > 0`): each edge offsets outward and consecutive
/// offset edges join per the style rule (arc, miter, or bevel chord). A
/// convex ring's outward offset never self-intersects, so no boolean
/// resolution runs (the general engine pays its full noding/graph machinery
/// even for a box).
///
/// **Erosion** (`distance < 0`): build the raw inward-offset loop (CW shell
/// walk, same machinery as [`erosion_loops`]) and **accept only when the
/// result is simple and correctly oriented** (Clockwise — the monotone
/// eroded shell). Self-intersection or inversion rejects (`None`) and the
/// caller falls through to [`winding_erosion_budgeted`] unchanged. That certificate
/// is the only gate: it never rewrites concave/styled-erosion semantics
/// (bevel corner allowances, deep notches, …). Distance zero stays on the
/// winding valid-region path. XY output, like every buffer.
pub(crate) fn convex_buffer_budgeted(
    polygon: &Polygon,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    if distance == 0.0 || !polygon.holes.is_empty() {
        return Ok(None);
    }
    if !shell_is_convex(polygon.shell.coords()) {
        return Ok(None);
    }
    if distance > 0.0 {
        let Some(strict) = strict_cycle(polygon.shell.coords(), false)? else {
            return Ok(None);
        };
        let Some((mut xs, mut ys)) =
            raw_offset_loop(&strict, distance, rule, quadrant_segments, budget)?
        else {
            return Ok(None);
        };
        close_xy_loop(&mut xs, &mut ys, budget)?;
        let seq = CoordSeq::from_owned_columns(xs, ys, None, None)?;
        return Ok(Some(Shape::Polygon(Polygon::new(
            Ring::from_closed(seq),
            Vec::new(),
        )?)));
    }
    // Negative distance: inward offset + validity certificate.
    convex_erosion_certified(polygon, -distance, rule, quadrant_segments, budget)
}

/// Inward raw offset of a convex hole-free shell, accepted only when simple
/// and CW (the exact monotone eroded result). `None` → winding erosion.
fn convex_erosion_certified(
    polygon: &Polygon,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    // CW shell walk so the right-side offset goes inward (mirrors erosion_loops).
    let Some(strict) = strict_cycle(polygon.shell.coords(), true)? else {
        return Ok(None);
    };
    let Some((xs, ys)) = raw_offset_loop(&strict, distance, rule, quadrant_segments, budget)?
    else {
        return Ok(None);
    };
    Ok(simple_erosion_polygon(&xs, &ys, budget)?.map(Shape::Polygon))
}

/// One eroded offset loop as a directly valid polygon.
///
/// The raw offset is a CW open walk. Reverse into a closed CCW ring in one
/// allocation (arrangement shell convention), then accept only when the
/// result is CCW and simple — inversion (was CCW open → CW after reverse)
/// and self-intersection reject to [`winding_erosion_budgeted`].
fn simple_erosion_polygon(
    xs: &[f64],
    ys: &[f64],
    budget: &mut ExpansionBudget,
) -> Result<Option<Polygon>> {
    if xs.len() < 3 {
        return Ok(None);
    }
    // This is the direct output owner: the raw walk remains open for the
    // winding engine, while the certified erosion result appends its closure.
    let capacity = xs
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.add(1)?;
    let mut ring_xs = Vec::new();
    let mut ring_ys = Vec::new();
    crate::try_reserve_pair(&mut ring_xs, &mut ring_ys, capacity)?;
    // Reverse the CW offset into CCW presentation while closing.
    for i in (0..xs.len()).rev() {
        ring_xs.push(xs[i]);
        ring_ys.push(ys[i]);
    }
    ring_xs.push(ring_xs[0]);
    ring_ys.push(ring_ys[0]);
    let ring = CoordSeq::from_owned_columns(ring_xs, ring_ys, None, None)?;
    // Original CW offset → CCW here. Inversion/degenerate was non-CW open
    // and becomes non-CCW after reverse.
    if !ring_winding(&ring).is_ccw() {
        return Ok(None);
    }
    if !line_is_simple(&ring)? {
        return Ok(None);
    }
    Ok(Some(Polygon::new(Ring::from_closed(ring), Vec::new())?))
}

/// General positive round-join polygonal buffer — the own winding-number
/// engine, composed from ONE primitive ([`winding_region`]) applied twice:
///
/// 1. every hole shrinks in its own arrangement (`winding <= -1` of its raw
///    offset loop — partial and full inversion cancel exactly, so a vanished
///    hole contributes nothing and a pinched hole yields its surviving lobes),
///    and
/// 2. the final arrangement of shell offset loops plus the CLEAN shrunk hole
///    rings keeps `winding >= 1` — overlapping parts merge, and a part
///    expanding into another part's shrunk hole fills it through plain winding
///    algebra.
///
/// `None` means the winding route has no usable loop; non-finite emitted
/// coordinates are returned as typed errors instead.
pub(in crate::geometry) fn winding_buffer_budgeted(
    polygons: &[Polygon],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    let mut loops: Vec<LoopColumns> = Vec::new();
    for polygon in polygons {
        if expansion_loops(
            &mut loops,
            polygon,
            distance,
            rule,
            quadrant_segments,
            budget,
        )?
        .is_none()
        {
            return Ok(None);
        }
    }
    let parts = winding_region(&loops, |winding| winding >= 1)?;
    if parts.is_empty() {
        return Ok(None);
    }
    Ok(Some(polygon_parts_to_shape(parts)?))
}

/// Erosion (negative buffer) of polygonal input — first normalize the complete
/// occupied polygon union, then run the mirrored winding construction: every
/// ring's walk orientation flips (shells CW, holes CCW), so the right-side
/// offset goes INWARD for shells and grows holes; the eroded region is
/// `winding <= -1`. Normalizing before offsetting is what makes a shared edge
/// one source boundary rather than two independently eroded components.
/// The inverting side here is the SHELL (over-erosion pinches and vanishes), so
/// shells take the per-loop cleaning that holes take under expansion. Fully
/// eroded input legitimately yields the empty polygon.
pub(in crate::geometry) fn winding_erosion_budgeted(
    polygons: &[Polygon],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    let mut operands = Vec::new();
    crate::try_reserve_vec(&mut operands, polygons.len())?;
    for polygon in polygons {
        crate::try_push_vec(&mut operands, polygon)?;
    }
    let normalized = normalize_polygon_union(&operands)?;
    erosion_normalized_budgeted(normalized, distance, rule, quadrant_segments, budget)
}

/// Normalize the selected areal operands in one exact linear arrangement.
///
/// A stored polygon first goes through the same unmoved winding admission as
/// `buffer(0)`: shell winding is negative after the erosion orientation flip,
/// holes are positive, and only the selected `<= -1` region survives. This
/// preserves self-crossing ring semantics. All canonicalized source rings are
/// then selected by polygon group in one arrangement, so overlapping
/// components cannot cancel and a shared edge is one boundary before erosion.
fn normalize_polygon_union(polygons: &[&Polygon]) -> Result<Vec<Polygon>> {
    if polygons.is_empty() {
        return Ok(Vec::new());
    }
    let mut loops = Vec::new();
    let mut groups = Vec::new();
    crate::try_reserve_vec(&mut loops, polygons.len())?;
    crate::try_reserve_vec(&mut groups, polygons.len())?;
    for polygon in polygons {
        let start = loops.len();
        let Some(shell) = strict_cycle(polygon.shell.coords(), true)? else {
            continue;
        };
        crate::try_push_vec(&mut loops, cycle_columns(&shell)?)?;
        for hole in polygon.holes.iter() {
            let Some(hole) = strict_cycle(hole.coords(), false)? else {
                continue;
            };
            crate::try_push_vec(&mut loops, cycle_columns(&hole)?)?;
        }
        let end = loops.len();
        crate::try_push_vec(&mut groups, start..end)?;
    }
    if loops.is_empty() {
        return Ok(Vec::new());
    }
    let inventory = ExactSourceInventory::from_raw_loops(&loops)?;
    let mut partition_budget = ExpansionBudget::new("winding_region", "exact partition");
    let partition = inventory.partition(&mut partition_budget)?;
    inventory.winding_group_union_polygons(&partition, &groups)
}

fn cycle_columns(points: &[Point]) -> Result<LoopColumns> {
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    crate::try_reserve_pair(&mut xs, &mut ys, points.len())?;
    for point in points {
        crate::try_push_vec(&mut xs, point.x)?;
        crate::try_push_vec(&mut ys, point.y)?;
    }
    Ok((xs, ys))
}

/// Erode an already-normalized polygon union. The input owns the normalized
/// boundaries so distance zero can return the exact occupied support without
/// rebuilding an unmoved offset arrangement.
fn erosion_normalized_budgeted(
    polygons: Vec<Polygon>,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    if polygons.is_empty() {
        return Ok(Some(Shape::empty_polygon()));
    }
    if distance == 0.0 {
        return Ok(Some(polygon_parts_to_shape(polygons)?));
    }
    let mut loops: Vec<LoopColumns> = Vec::new();
    for polygon in &polygons {
        if erosion_loops(
            &mut loops,
            polygon,
            distance,
            rule,
            quadrant_segments,
            budget,
        )?
        .is_none()
        {
            return Ok(None);
        }
    }
    let parts = winding_region(&loops, |winding| winding <= -1)?;
    Ok(Some(polygon_parts_to_shape(parts)?))
}

/// Borrow every polygonal member of a collection without turning lineal or
/// puntal members into erosion operands. An explicit stack keeps the
/// collection traversal independent of nesting depth while preserving input
/// order for budget and error determinism.
fn collection_polygon_operands(parts: &[Shape]) -> Result<Vec<&Polygon>> {
    let mut stack = Vec::new();
    crate::try_reserve_vec(&mut stack, parts.len())?;
    for part in parts.iter().rev() {
        crate::try_push_vec(&mut stack, part)?;
    }
    let mut polygons = Vec::new();
    while let Some(part) = stack.pop() {
        match part {
            Shape::Polygon(polygon) => crate::try_push_vec(&mut polygons, polygon)?,
            Shape::MultiPolygon(more) => {
                for polygon in more.iter() {
                    crate::try_push_vec(&mut polygons, polygon)?;
                }
            },
            Shape::GeometryCollection(inner) => {
                for nested in inner.iter().rev() {
                    crate::try_push_vec(&mut stack, nested)?;
                }
            },
            _ => {},
        }
    }
    Ok(polygons)
}

/// Stroke buffer of lineal input through the winding engine: every chain
/// emits ONE closed loop — the right side walked forward, an end cap, the
/// left side walked backward, a start cap — and overlapping strokes
/// (self-intersections, sharp folds, multi-part overlaps) resolve in the
/// `winding >= 1` selection. Caps follow the style: round (a pi arc),
/// flat (the direct chord), square (extended by the distance); outside
/// joins follow `rule`; zero-length chains are disks (GEOS semantics).
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
        && let Some(polygon) = simple_stroke_polygon(xs, ys, budget)?
    {
        return Ok(Some(Shape::Polygon(polygon)));
    }
    let parts = winding_region(&loops, |winding| winding >= 1)?;
    if parts.is_empty() {
        return Ok(None);
    }
    Ok(Some(polygon_parts_to_shape(parts)?))
}

/// One stroke loop as a directly valid polygon: close the columns into a
/// ring and accept only a simple, positive-area result (the complete gate
/// for the single-loop fast path; anything else falls back to winding).
fn simple_stroke_polygon(
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
    let capacity = xs
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.add(1)?;
    let mut ring_xs = Vec::new();
    let mut ring_ys = Vec::new();
    crate::try_reserve_pair(&mut ring_xs, &mut ring_ys, capacity)?;
    ring_xs.extend_from_slice(xs);
    ring_ys.extend_from_slice(ys);
    ring_xs.push(xs[0]);
    ring_ys.push(ys[0]);
    let ring = CoordSeq::try_from_vecs(ring_xs, ring_ys, None, None)?;
    if ring_winding(&ring).is_ccw()
        && Shape::LineString(LineSeq::from_proven(ring.clone())).is_simple()?
    {
        Ok(Some(Polygon::new(Ring::from_closed(ring), Vec::new())?))
    } else {
        Ok(None)
    }
}

/// Buffer of a whole `GeometryCollection` in ONE winding pass: every part
/// reduces to raw loops in the SAME arrangement — circles for points,
/// stroke loops for chains, offset loops for polygons — and the union of
/// all the part buffers IS the `winding >= 1` region (GEOS instead
/// buffers each part and runs a boolean union cascade). Negative
/// distances first normalize the selected polygonal support, then erode that
/// one union; puntal/lineal parts annihilate exactly.
pub(in crate::geometry) fn winding_collection_budgeted(
    parts: &[Shape],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    if distance <= 0.0 {
        let polygons = collection_polygon_operands(parts)?;
        let normalized = normalize_polygon_union(&polygons)?;
        return erosion_normalized_budgeted(normalized, -distance, rule, quadrant_segments, budget);
    }
    let mut loops: Vec<LoopColumns> = Vec::new();
    if collection_loops(
        &mut loops,
        parts,
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
    if loops.is_empty() {
        return Ok(Some(Shape::empty_polygon()));
    }
    let pieces = winding_region(&loops, |winding| winding >= 1)?;
    if pieces.is_empty() {
        return Ok(None);
    }
    Ok(Some(polygon_parts_to_shape(pieces)?))
}

// Direct constructive tests characterize these algorithms without an FFI
// operation boundary. Keep that focused surface test-only: production always
// threads the caller's shared budget, while these bounded fixtures retain the
// concise historical helpers.
#[cfg(test)]
pub(crate) fn convex_buffer(
    polygon: &Polygon,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let mut budget = ExpansionBudget::new("constructive test", "quadrant_segments");
    convex_buffer_budgeted(polygon, distance, rule, quadrant_segments, &mut budget)
        .expect("bounded constructive test must not exhaust generated-work budget")
}

#[cfg(test)]
pub(crate) fn winding_buffer(
    polygons: &[Polygon],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let mut budget = ExpansionBudget::new("constructive test", "quadrant_segments");
    winding_buffer_budgeted(polygons, distance, rule, quadrant_segments, &mut budget)
        .expect("bounded constructive test must not exhaust generated-work budget")
}

#[cfg(test)]
pub(crate) fn winding_erosion(
    polygons: &[Polygon],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let mut budget = ExpansionBudget::new("constructive test", "quadrant_segments");
    winding_erosion_budgeted(polygons, distance, rule, quadrant_segments, &mut budget)
        .expect("bounded constructive test must not exhaust generated-work budget")
}

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
    let Some(shell) = strict_cycle(polygon.shell.coords(), false)? else {
        return Ok(None);
    };
    let Some(raw) = raw_offset_loop(&shell, distance, rule, quadrant_segments, budget)? else {
        return Ok(None);
    };
    crate::try_push_vec(loops, raw)?;
    for hole in polygon.holes.iter() {
        // Degenerate (zero-area) holes do not affect the buffer.
        let Some(ring) = strict_cycle(hole.coords(), true)? else {
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
    let Some(shell) = strict_cycle(polygon.shell.coords(), true)? else {
        return Ok(None);
    };
    let Some(raw) = raw_offset_loop(&shell, distance, rule, quadrant_segments, budget)? else {
        return Ok(None);
    };
    // Surviving eroded lobes are CW pieces; keep them CW (they ARE the
    // negative-winding region's boundary in the final pass).
    extend_cleaned(loops, &raw)?;
    for hole in polygon.holes.iter() {
        let Some(ring) = strict_cycle(hole.coords(), false)? else {
            continue;
        };
        // Grown holes never invert; their self-crossings (reflex
        // corners) resolve in the global winding.
        let Some(raw) = raw_offset_loop(&ring, distance, rule, quadrant_segments, budget)? else {
            return Ok(None);
        };
        crate::try_push_vec(loops, raw)?;
    }
    Ok(Some(()))
}

/// Append one chain's stroke loop. Chains that dedup to a single point
/// are zero-length: their stroke is the full disk (GEOS semantics);
/// empty chains contribute nothing.
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
    crate::try_reserve_vec(&mut points, chain.len())?;
    points.extend(chain.iter_coords());
    dedup_consecutive_points(&mut points);

    match points.as_slice() {
        [] => Ok(Some(true)),
        [point] => {
            let Some(circle) = circle_loop(*point, distance, quadrant_segments, budget)? else {
                return Ok(None);
            };
            crate::try_push_vec(loops, circle)?;
            // A lone circle is simple by construction.
            Ok(Some(true))
        },
        chain => {
            // A collinear polyline (including one that folds back, like a
            // closed degenerate ring `A..A`) covers exactly the interval
            // between its extreme vertices, so its buffer is the stadium of
            // that extent segment — not the thin self-retracing region a raw
            // stroke of the folded path would select.
            let extent;
            let chain: &[Point] = if chain.len() >= 3
                && let Some(segment) = collinear_extent(chain)
            {
                extent = segment;
                &extent
            } else {
                chain
            };
            if chain.len() >= 3 && same_point(chain[0], chain[chain.len() - 1]) {
                let Some((right, left, clean)) = raw_closed_stroke_loops(
                    &chain[..chain.len() - 1],
                    distance,
                    rule,
                    quadrant_segments,
                    budget,
                )?
                else {
                    return Ok(None);
                };
                crate::try_push_vec(loops, right)?;
                crate::try_push_vec(loops, left)?;
                return Ok(Some(clean));
            }
            let Some((columns, clean)) =
                raw_stroke_loop(chain, distance, cap_style, rule, quadrant_segments, budget)?
            else {
                return Ok(None);
            };
            crate::try_push_vec(loops, columns)?;
            Ok(Some(clean))
        },
    }
}

/// One full-circle CCW loop around `center` — the buffer of a point and
/// the stroke of a zero-length chain. Inscribed vertices on the circle,
/// per the arc doctrine (`4 * quadrant_segments` steps).
fn circle_step_count(quadrant_segments: std::num::NonZeroU32) -> Result<usize> {
    (quadrant_segments.get() as usize)
        .checked_mul(4)
        .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
}

pub(crate) fn circle_loop(
    center: Point,
    distance: f64,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<LoopColumns>> {
    let steps = circle_step_count(quadrant_segments)?;
    // Circle size is exact without walking it, so admit the exact realized
    // count before the allocation rather than applying an input estimate.
    budget.add(steps)?;
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    crate::try_reserve_pair(&mut xs, &mut ys, steps)?;
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
    Ok(Some((xs, ys)))
}

/// Append one circle for every distinct topological X/Y coordinate, retaining
/// the first member's ordinates as the disk center provenance.
pub(crate) fn multipoint_loops(
    loops: &mut Vec<LoopColumns>,
    points: &MultiPoint,
    distance: f64,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<()>> {
    let mut seen = HashSet::new();
    seen.try_reserve(points.len())?;
    for point in points.iter_coords() {
        // `canonical_f64_bits` matches `same_topological_coordinate`: ±0.0
        // share one key while every other finite coordinate remains bit-exact.
        if !seen.insert((canonical_f64_bits(point.x), canonical_f64_bits(point.y))) {
            continue;
        }
        let Some(circle) = circle_loop(point, distance, quadrant_segments, budget)? else {
            return Ok(None);
        };
        crate::try_push_vec(loops, circle)?;
    }
    Ok(Some(()))
}

/// The buffer of a single point: the inscribed-circle polygon directly —
/// no arrangement work at all.
pub(crate) fn point_buffer(
    center: Point,
    distance: f64,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    let steps = circle_step_count(quadrant_segments)?;
    // `circle_loop` emits the open circle and `close_xy_loop` appends the
    // statically known final coordinate. Reserve the whole realized polygon
    // before the 16M open-circle allocation, not one coordinate too late.
    budget.check_additional(
        steps
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?,
    )?;
    let Some((mut xs, mut ys)) = circle_loop(center, distance, quadrant_segments, budget)? else {
        return Ok(None);
    };
    close_xy_loop(&mut xs, &mut ys, budget)?;
    let seq = CoordSeq::from_owned_columns(xs, ys, None, None)?;
    Ok(Some(Shape::Polygon(Polygon::new(
        Ring::from_closed(seq),
        Vec::new(),
    )?)))
}

/// Collect expansion loops across a collection's parts, recursively.
pub(crate) fn collection_loops(
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
                let Some(circle) = circle_loop(*point, distance, quadrant_segments, budget)? else {
                    return Ok(None);
                };
                crate::try_push_vec(loops, circle)?;
            },
            Shape::MultiPoint(points) => {
                if multipoint_loops(loops, points, distance, quadrant_segments, budget)?.is_none() {
                    return Ok(None);
                }
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

/// The extent segment `[lo, hi]` of a collinear chain (≥ 3 distinct-adjacent
/// vertices all on one line), or `None` when the chain bends. A connected
/// collinear polyline — even one that doubles back or closes on itself —
/// covers exactly the interval between its projection extremes, so stroking
/// that segment yields the correct stadium where stroking the folded path
/// would not (see the caller in [`stroke_loops`]).
pub(crate) fn collinear_extent(chain: &[Point]) -> Option<[Point; 2]> {
    let anchor = chain[0];
    let direction = *chain[1..]
        .iter()
        .find(|point| !same_point(**point, anchor))?;
    if chain
        .iter()
        .any(|&point| orientation(anchor, direction, point) != Orientation::Collinear)
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
pub(crate) fn raw_closed_stroke_loops(
    chain: &[Point],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<(LoopColumns, LoopColumns, bool)>> {
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments.get());
    let Some(right_plan) = WalkPlan::new(chain, true, distance, rule)? else {
        return Ok(None);
    };
    let mut backward = Vec::new();
    crate::try_reserve_vec(&mut backward, chain.len())?;
    backward.push(chain[0]);
    backward.extend(chain[1..].iter().rev().copied());
    let Some(left_plan) = WalkPlan::new(&backward, true, distance, rule)? else {
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
    let Some(right_plan) = WalkPlan::new(chain, false, distance, rule)? else {
        return Ok(None);
    };
    // Keep the reversed source walk alive alongside its plan. The plan is
    // immutable after validation, and retaining both plans across the count
    // and materialization passes prevents a second normal/join calculation.
    thread_local! {
        static BACKWARD: crate::scratch_pool::ScratchPool<Vec<Point>> = const {
            crate::scratch_pool::ScratchPool::new(Vec::new())
        };
    }
    let mut backward = crate::scratch_pool::ScratchPool::checkout(&BACKWARD);
    crate::try_reserve_vec(&mut backward, chain.len())?;
    backward.extend(chain.iter().rev().copied());
    let Some(left_plan) = WalkPlan::new(backward.as_slice(), false, distance, rule)? else {
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
            chain[chain.len() - 1],
            enx,
            eny,
            chain[0],
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
    let mut columns = WalkColumns::try_new(count_len, false)?;
    let real_clean = emit_stroke_loop(
        &right_plan,
        &left_plan,
        chain[chain.len() - 1],
        enx,
        eny,
        chain[0],
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
        (right_plan.source_len() - 1) as u32,
        sink,
    )?;
    let left_clean = stroke_side(left_plan, step_angle, sink)?;
    emit_cap_with_peer(
        start, snx, sny, end, distance, cap_style, step_angle, 0, sink,
    )?;
    Ok(right_clean && left_clean)
}

/// Source vector in a power-of-two frame. Preserve a finite world-space
/// residual when it exists; only an overflowing subtraction scales both
/// operands before forming the residual. The returned scale is the frame
/// factor applied to both components.
pub(crate) fn source_vector(from: XY, to: XY) -> Option<(f64, f64, f64)> {
    let raw_dx = to.x - from.x;
    let raw_dy = to.y - from.y;
    let scale = if raw_dx.is_finite() && raw_dy.is_finite() {
        1.0
    } else {
        let extent = from
            .x
            .abs()
            .max(from.y.abs())
            .max(to.x.abs())
            .max(to.y.abs());
        if !extent.is_finite() {
            return None;
        }
        axis_pow2_scale(extent)
    };
    let dx = scaled_residual(to.x, from.x, scale);
    let dy = scaled_residual(to.y, from.y, scale);
    (dx.is_finite() && dy.is_finite()).then_some((dx, dy, scale))
}

/// Unit right-side normal of the edge `from -> to` (`None` for degenerate
/// or unrepresentable source coordinates).
pub(crate) fn unit_right_normal(from: Point, to: Point) -> Option<(f64, f64)> {
    let (dx, dy, _) = source_vector(from.xy(), to.xy())?;
    let squared = dx * dx + dy * dy;
    // Ordinary finite normal: fast squared-length path (bit-stable).
    if squared.is_normal() {
        let length = squared.sqrt();
        return Some((dy / length, -dx / length));
    }
    // Zero / subnormal / overflow: max-abs normalize so stadium buffers of
    // length 1e-200 and 1e155 stay representable.
    let scale = dx.abs().max(dy.abs());
    if scale == 0.0 || !scale.is_finite() {
        return None;
    }
    let (nx, ny) = (dx / scale, dy / scale);
    let length = (nx * nx + ny * ny).sqrt();
    if length == 0.0 || !length.is_finite() {
        return None;
    }
    Some((ny / length, -nx / length))
}

/// Emit the inscribed arc points of a join/cap around `center` from
/// `from_angle` sweeping counter-clockwise by `sweep` (normalized into
/// `[0, tau)`), stepping at most `step_angle`.
pub(crate) fn emit_arc<S: WalkSink>(
    center: Point,
    from_angle: f64,
    mut sweep: f64,
    distance: f64,
    step_angle: f64,
    source: u32,
    sink: &mut S,
) -> Result<()> {
    if sweep < 0.0 {
        sweep += std::f64::consts::TAU;
    }
    let steps = (sweep / step_angle).ceil() as usize;
    if steps <= 1 {
        return Ok(());
    }
    // Rotation recurrence: two `sin_cos` calls per ARC instead of two
    // libm calls per vertex; drift over <= a full circle of steps is
    // far below the join placement tolerance.
    let (sin_step, cos_step) = (sweep / steps as f64).sin_cos();
    let (mut sin_theta, mut cos_theta) = from_angle.sin_cos();
    for _ in 1..steps {
        (cos_theta, sin_theta) = (
            cos_theta * cos_step - sin_theta * sin_step,
            sin_theta * cos_step + cos_theta * sin_step,
        );
        sink.push(
            center.x + distance * cos_theta,
            center.y + distance * sin_theta,
            source,
        )?;
    }
    Ok(())
}

/// Emit a round cap with the exact intersections of its circle and the other
/// endpoint's cap circle included as noding vertices. Chord-only arcs can
/// otherwise cross at an interior point of two chords, which is inside both
/// circles even though the true circle intersection is on both loci.
fn emit_cap_with_peer<S: WalkSink>(
    tip: Point,
    nx: f64,
    ny: f64,
    peer: Point,
    distance: f64,
    cap_style: BufferCapStyle,
    step_angle: f64,
    source: u32,
    sink: &mut S,
) -> Result<()> {
    if !matches!(cap_style, BufferCapStyle::Round) {
        return emit_cap(tip, nx, ny, distance, cap_style, step_angle, source, sink);
    }
    let from_angle = ny.atan2(nx);
    let breaks = cap_circle_intersections(tip, peer, distance.abs(), from_angle)?;
    if breaks.iter().all(Option::is_none) {
        return emit_cap(tip, nx, ny, distance, cap_style, step_angle, source, sink);
    }
    emit_arc_with_breaks(
        tip,
        from_angle,
        std::f64::consts::PI,
        distance,
        step_angle,
        source,
        &breaks,
        sink,
    )
}

/// Return the circle-circle intersections that lie strictly inside this cap's
/// semicircle. The centers are canonicalized before construction so the same
/// point bits are supplied to both caps when they share an intersection.
fn cap_circle_intersections(
    center: Point,
    peer: Point,
    radius: f64,
    from_angle: f64,
) -> Result<[Option<(f64, Point)>; 2]> {
    let mut result = [None, None];
    if !radius.is_finite() || radius <= 0.0 {
        return Ok(result);
    }
    let (first, second) = match center.x.total_cmp(&peer.x) {
        Ordering::Less => (center, peer),
        Ordering::Equal if center.y.total_cmp(&peer.y).is_le() => (center, peer),
        _ => (peer, center),
    };
    let raw_dx = second.x - first.x;
    let raw_dy = second.y - first.y;
    let extent = if raw_dx.is_finite() && raw_dy.is_finite() {
        raw_dx.abs().max(raw_dy.abs()).max(radius)
    } else {
        first
            .x
            .abs()
            .max(first.y.abs())
            .max(second.x.abs())
            .max(second.y.abs())
            .max(radius)
    };
    if !extent.is_finite() {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    let scale = axis_pow2_scale(extent);
    let (dx, dy) = (
        scaled_residual(second.x, first.x, scale),
        scaled_residual(second.y, first.y, scale),
    );
    let scaled_radius = radius * scale;
    let separation = dx.hypot(dy);
    if ![dx, dy, scaled_radius, separation]
        .into_iter()
        .all(f64::is_finite)
    {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    if separation == 0.0 {
        return Ok(result);
    }
    let half = separation * 0.5;
    if half >= scaled_radius {
        return Ok(result);
    }
    let height = (scaled_radius - half).sqrt() * (scaled_radius + half).sqrt();
    if !height.is_finite() {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    if height == 0.0 {
        return Ok(result);
    }
    let midpoint = Point::new_raw_xy(
        interpolate_f64(first.x, second.x, 0.5),
        interpolate_f64(first.y, second.y, 0.5),
    );
    if !midpoint.x.is_finite() || !midpoint.y.is_finite() {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    let (px, py) = (-dy / separation, dx / separation);
    let mut count = 0;
    for index in 0..2 {
        let sign = if index == 0 { 1.0 } else { -1.0 };
        let (dir_x, dir_y) = if same_point(center, first) {
            (dx * 0.5 + sign * px * height, dy * 0.5 + sign * py * height)
        } else {
            (
                -dx * 0.5 + sign * px * height,
                -dy * 0.5 + sign * py * height,
            )
        };
        let offset = (dir_y.atan2(dir_x) - from_angle).rem_euclid(std::f64::consts::TAU);
        if offset <= 1e-12 || offset >= std::f64::consts::PI - 1e-12 {
            continue;
        }
        let height_world = scale_by_power_of_two(height, -power_of_two_exponent(scale));
        if !height_world.is_finite() {
            return Err(GeometryErrorKind::message(
                "buffer calculation produced non-finite coordinates",
            ));
        }
        let point = Point::new_raw_xy(
            midpoint.x + sign * px * height_world,
            midpoint.y + sign * py * height_world,
        );
        if !point.x.is_finite() || !point.y.is_finite() {
            return Err(GeometryErrorKind::message(
                "buffer calculation produced non-finite coordinates",
            ));
        }
        result[count] = Some((offset, point));
        count += 1;
        if count == result.len() {
            break;
        }
    }
    if count == 2 {
        let left = result[0].map(|(offset, _)| offset);
        let right = result[1].map(|(offset, _)| offset);
        if let (Some(left), Some(right)) = (left, right)
            && right < left
        {
            result.swap(0, 1);
        }
    }
    Ok(result)
}

/// Emit an arc whose interior contains exact circle intersections. Ordinary
/// arcs stay on the recurrence fast path; this bounded path only subdivides
/// the few cap intervals that contain an endpoint-circle crossing.
fn emit_arc_with_breaks<S: WalkSink>(
    center: Point,
    mut from_angle: f64,
    mut sweep: f64,
    distance: f64,
    step_angle: f64,
    source: u32,
    breaks: &[Option<(f64, Point)>; 2],
    sink: &mut S,
) -> Result<()> {
    if sweep < 0.0 {
        sweep += std::f64::consts::TAU;
    }
    let mut angles = [0.0; 4];
    let mut exact = [None; 4];
    let mut count = 2;
    angles[1] = sweep;
    for &(offset, point) in breaks.iter().flatten() {
        if offset > 0.0 && offset < sweep {
            angles[count] = offset;
            exact[count] = Some(point);
            count += 1;
        }
    }
    for left in 0..count {
        for right in left + 1..count {
            if angles[right] < angles[left] {
                angles.swap(left, right);
                exact.swap(left, right);
            }
        }
    }
    let mut unique = 1;
    for index in 1..count {
        if angles[index] - angles[unique - 1] <= 1e-12 {
            if exact[unique - 1].is_none() {
                exact[unique - 1] = exact[index];
            }
            continue;
        }
        angles[unique] = angles[index];
        exact[unique] = exact[index];
        unique += 1;
    }
    from_angle += angles[0];
    for interval in 0..unique - 1 {
        let span = angles[interval + 1] - angles[interval];
        let steps = (span / step_angle).ceil() as usize;
        for step in 1..=steps {
            if interval + 1 == unique - 1 && step == steps {
                continue;
            }
            if step == steps
                && let Some(point) = exact[interval + 1]
            {
                sink.push(point.x, point.y, source)?;
                continue;
            }
            let theta = from_angle + angles[interval] + span * step as f64 / steps as f64;
            let (sin_theta, cos_theta) = theta.sin_cos();
            sink.push(
                center.x + distance * cos_theta,
                center.y + distance * sin_theta,
                source,
            )?;
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extreme_cap_contacts_stay_on_their_scaled_distance_circles() {
        let center = Point::new_raw_xy(-1.5e308, 0.0);
        let peer = Point::new_raw_xy(1.5e308, 0.0);
        let radius = 1.6e308;
        let contacts = cap_circle_intersections(center, peer, radius, -std::f64::consts::FRAC_PI_2)
            .expect("finite circle contacts");

        assert_eq!(contacts.iter().flatten().count(), 2);
        let scale = axis_pow2_scale(radius);
        let radius = radius * scale;
        for (_, point) in contacts.into_iter().flatten() {
            let distance = scaled_residual(point.x, center.x, scale)
                .hypot(scaled_residual(point.y, center.y, scale));
            assert!(
                (distance - radius).abs() <= 16.0 * f64::EPSILON * radius,
                "distance {distance} differs from radius {radius}"
            );
        }
    }
}
