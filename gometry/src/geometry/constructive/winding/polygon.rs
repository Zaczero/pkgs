use super::loops::{LoopColumns, collection_loops, erosion_loops, expansion_loops};
use super::*;

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
        let Some(strict) = strict_cycle(polygon.shell.coords(), false) else {
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
            Ring::try_from_closed(seq)?,
            Vec::new(),
        ))));
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
    let Some(strict) = strict_cycle(polygon.shell.coords(), true) else {
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
    let capacity = xs.len().wrapping_add(1);
    budget.add(1)?;
    let mut ring_xs = Vec::with_capacity(capacity);
    let mut ring_ys = Vec::with_capacity(capacity);
    // Reverse the CW offset into CCW presentation while closing.
    for (&x, &y) in xs.iter().zip(ys).rev() {
        ring_xs.push(x);
        ring_ys.push(y);
    }
    ring_xs.push(*ring_xs.first().expect("nonempty erosion loop"));
    ring_ys.push(*ring_ys.first().expect("nonempty erosion loop"));
    let ring = CoordSeq::from_owned_columns(ring_xs, ring_ys, None, None)?;
    // Original CW offset → CCW here. Inversion/degenerate was non-CW open
    // and becomes non-CCW after reverse.
    if !ring_winding(&ring).is_ccw() {
        return Ok(None);
    }
    if !line_is_simple(&ring)? {
        return Ok(None);
    }
    Ok(Some(Polygon::new(Ring::try_from_closed(ring)?, Vec::new())))
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
    Ok(Some(polygon_parts_to_shape(parts)))
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
    let normalized = normalize_polygon_union(polygons.iter())?;
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
fn normalize_polygon_union<'a>(
    polygons: impl ExactSizeIterator<Item = &'a Polygon>,
) -> Result<Vec<Polygon>> {
    if polygons.len() == 0 {
        return Ok(Vec::new());
    }
    let mut loops = Vec::with_capacity(polygons.len());
    let mut groups = Vec::with_capacity(polygons.len());
    for polygon in polygons {
        let start = loops.len();
        let Some(shell) = strict_cycle(polygon.shell.coords(), true) else {
            continue;
        };
        loops.push(cycle_columns(&shell));
        for hole in polygon.holes.iter() {
            let Some(hole) = strict_cycle(hole.coords(), false) else {
                continue;
            };
            loops.push(cycle_columns(&hole));
        }
        let end = loops.len();
        groups.push(start..end);
    }
    if loops.is_empty() {
        return Ok(Vec::new());
    }
    let inventory = ExactSourceInventory::from_raw_loops(&loops)?;
    let mut partition_budget = ExpansionBudget::new("winding_region", "exact partition");
    let partition = inventory.partition(&mut partition_budget)?;
    inventory.winding_group_union_polygons(&partition, &groups)
}

fn cycle_columns(points: &[Point]) -> LoopColumns {
    let mut xs = Vec::with_capacity(points.len());
    let mut ys = Vec::with_capacity(points.len());
    for point in points {
        xs.push(point.x);
        ys.push(point.y);
    }
    (xs, ys)
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
        return Ok(Some(polygon_parts_to_shape(polygons)));
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
    Ok(Some(polygon_parts_to_shape(parts)))
}

/// Borrow every polygonal member of a collection without turning lineal or
/// puntal members into erosion operands. An explicit stack keeps the
/// collection traversal independent of nesting depth while preserving input
/// order for budget and error determinism.
fn collection_polygon_operands(parts: &[Shape]) -> Vec<&Polygon> {
    let mut stack = Vec::with_capacity(parts.len());
    stack.extend(parts.iter().rev());
    let mut polygons = Vec::new();
    while let Some(part) = stack.pop() {
        match part {
            Shape::Polygon(polygon) => polygons.push(polygon),
            Shape::MultiPolygon(more) => {
                polygons.reserve(more.len());
                polygons.extend(more.iter());
            },
            Shape::GeometryCollection(inner) => {
                stack.reserve(inner.len());
                stack.extend(inner.iter().rev());
            },
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(..) => {},
        }
    }
    polygons
}

pub(in crate::geometry) fn winding_collection_budgeted(
    parts: &[Shape],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Option<Shape>> {
    if distance <= 0.0 {
        let polygons = collection_polygon_operands(parts);
        let normalized = normalize_polygon_union(polygons.iter().copied())?;
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
    Ok(Some(polygon_parts_to_shape(pieces)))
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
