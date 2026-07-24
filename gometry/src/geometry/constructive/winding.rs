#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

fn charge_scaled(
    budget: &mut ExpansionBudget,
    count: usize,
    factor: usize,
    constant: usize,
) -> Result<()> {
    let scaled = ExpansionBudget::product(budget.operation(), "quadrant_segments", count, factor)?;
    budget.add(scaled)?;
    budget.add(constant)?;
    Ok(())
}

fn charge_buffer_shape(
    shape: &Shape,
    quadrant_segments: usize,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let circle = quadrant_segments
        .checked_mul(4)
        .and_then(|value| value.checked_add(1))
        .unwrap_or(usize::MAX);
    let stroke_overhead = quadrant_segments
        .checked_mul(8)
        .and_then(|value| value.checked_add(8))
        .unwrap_or(usize::MAX);
    match shape {
        Shape::Point(_) => {
            budget.add(circle)?;
        },
        Shape::MultiPoint(points) => {
            let total =
                ExpansionBudget::product("buffer", "quadrant_segments", points.len(), circle)?;
            budget.add(total)?;
        },
        Shape::LineString(line) => charge_scaled(budget, line.len(), 4, stroke_overhead)?,
        Shape::MultiLineString(lines) => {
            for line in lines {
                charge_scaled(budget, line.len(), 4, stroke_overhead)?;
            }
        },
        Shape::Polygon(polygon) => {
            for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                // Degenerate polygon rings fall back to stroked linework, so
                // charge the larger stroke bound rather than only raw offset.
                charge_scaled(budget, ring.len(), 4, stroke_overhead)?;
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                    charge_scaled(budget, ring.len(), 4, stroke_overhead)?;
                }
            }
        },
        Shape::GeometryCollection(parts) => {
            for part in parts {
                charge_buffer_shape(part, quadrant_segments, budget)?;
            }
        },
        Shape::Empty(..) => {},
    }
    Ok(())
}

/// Conservative exact-shape bound for arc-amplified buffer construction.
/// The coefficients mirror the winding emitters' documented capacities.
pub(crate) fn validate_buffer_expansion(
    shape: &Shape,
    quadrant_segments: std::num::NonZeroU32,
) -> Result<()> {
    let mut budget = ExpansionBudget::new("buffer", "quadrant_segments");
    charge_buffer_shape(shape, quadrant_segments.get() as usize, &mut budget)
}

fn charge_offset_shape(shape: &Shape, overhead: usize, budget: &mut ExpansionBudget) -> Result<()> {
    match shape {
        Shape::LineString(line) => charge_scaled(budget, line.len(), 3, overhead),
        Shape::MultiLineString(lines) => {
            for line in lines {
                charge_scaled(budget, line.len(), 3, overhead)?;
            }
            Ok(())
        },
        Shape::GeometryCollection(parts) => {
            for part in parts {
                charge_offset_shape(part, overhead, budget)?;
            }
            Ok(())
        },
        _ => Ok(()),
    }
}

pub(crate) fn validate_offset_expansion(
    shape: &Shape,
    quadrant_segments: std::num::NonZeroU32,
) -> Result<()> {
    let q = quadrant_segments.get() as usize;
    let overhead = q
        .checked_mul(4)
        .and_then(|value| value.checked_add(2))
        .unwrap_or(usize::MAX);
    let mut budget = ExpansionBudget::new("offset_curve", "quadrant_segments");
    charge_offset_shape(shape, overhead, &mut budget)
}

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
pub(crate) fn ring_as_open_line(ring: &CoordSeq) -> CoordSeq {
    let n = ring.len();
    let take = if n >= 2 && ring.first() == ring.last() {
        n - 1
    } else {
        n
    };
    let points: Vec<Point> = (0..take).map(|index| ring.point_at(index)).collect();
    CoordSeq::from_points(&points)
}

fn ring_as_open_linework(ring: &CoordSeq) -> Option<LineSeq> {
    let line = ring_as_open_line(ring);
    match line.len() {
        0 => None,
        1 => {
            let point = line.point_at(0);
            Some(
                LineSeq::try_new(CoordSeq::from(vec![point, point]))
                    .expect("duplicated point forms zero-length linework"),
            )
        },
        _ => Some(LineSeq::try_new(line).expect("open ring linework is lineal")),
    }
}

/// Reinterpret a shape's zero-area polygons as their boundary linework so the
/// winding buffer engine can resolve them (see [`buffer_with_style`]).
/// Non-degenerate polygons and non-polygonal parts pass through unchanged.
pub(crate) fn degenerate_polygonal_as_linework(shape: &Shape) -> Shape {
    match shape {
        Shape::Polygon(polygon) if polygon_encloses_no_area(polygon) => {
            Shape::MultiLineString(polygon.rings().filter_map(ring_as_open_linework).collect())
        },
        Shape::MultiPolygon(polygons) => Shape::GeometryCollection(
            polygons
                .iter()
                .map(|polygon| degenerate_polygonal_as_linework(&Shape::Polygon(polygon.clone())))
                .collect(),
        ),
        Shape::GeometryCollection(parts) => {
            Shape::GeometryCollection(parts.iter().map(degenerate_polygonal_as_linework).collect())
        },
        other => other.clone(),
    }
}

/// Expansion of a convex hole-free polygon: each edge offsets outward and
/// consecutive offset edges join per the style rule (arc, miter, or bevel
/// chord) — the offset of a convex ring never self-intersects, so no
/// boolean resolution runs at all (the general engine pays its full
/// noding/graph machinery even for a box). `None` routes everything else
/// — concave shells, holes, erosion, degenerate rings, coordinate
/// overflow — onward. XY output, like every buffer (a 2-D operation).
pub(crate) fn convex_buffer(
    polygon: &Polygon,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    if distance <= 0.0 || !polygon.holes.is_empty() {
        return None;
    }
    if !shell_is_convex(polygon.shell.coords()) {
        return None;
    }
    let strict = strict_cycle(polygon.shell.coords(), false)?;
    let (mut xs, mut ys) = raw_offset_loop(&strict, distance, rule, quadrant_segments)?;
    xs.push(xs[0]);
    ys.push(ys[0]);
    let seq = CoordSeq::from_owned_columns(xs, ys, None, None).ok()?;
    Some(Shape::Polygon(Polygon::new(
        Ring::from_trusted_closed(seq),
        Vec::new(),
    )))
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
/// `None` falls back to the geo engine (degenerate shells, overflow).
pub(in crate::geometry) fn winding_buffer(
    polygons: &[Polygon],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let mut loops: Vec<LoopColumns> = Vec::new();
    for polygon in polygons {
        expansion_loops(&mut loops, polygon, distance, rule, quadrant_segments)?;
    }
    let parts = winding_region(&loops, |winding| winding >= 1);
    if parts.is_empty() {
        return None;
    }
    Some(polygon_parts_to_shape(parts))
}

/// Erosion (negative buffer) of polygonal input — the mirrored
/// winding construction: every ring's walk orientation flips (shells CW,
/// holes CCW), so the right-side offset goes INWARD for shells and grows
/// holes; the eroded region is `winding <= -1`. The inverting side here
/// is the SHELL (over-erosion pinches and vanishes), so shells take the
/// per-loop cleaning that holes take under expansion. Fully eroded input
/// legitimately yields the empty polygon.
pub(in crate::geometry) fn winding_erosion(
    polygons: &[Polygon],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let mut loops: Vec<LoopColumns> = Vec::new();
    for polygon in polygons {
        erosion_loops(&mut loops, polygon, distance, rule, quadrant_segments)?;
    }
    let parts = winding_region(&loops, |winding| winding <= -1);
    Some(polygon_parts_to_shape(parts))
}

/// Stroke buffer of lineal input through the winding engine: every chain
/// emits ONE closed loop — the right side walked forward, an end cap, the
/// left side walked backward, a start cap — and overlapping strokes
/// (self-intersections, sharp folds, multi-part overlaps) resolve in the
/// `winding >= 1` selection. Caps follow the style: round (a pi arc),
/// flat (the direct chord), square (extended by the distance); outside
/// joins follow `rule`; zero-length chains are disks (GEOS semantics).
/// `None` falls back to the geo engine (overflow).
pub(in crate::geometry) fn winding_stroke(
    chains: &[&CoordSeq],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let mut loops: Vec<LoopColumns> = Vec::new();
    let mut clean = true;
    for chain in chains {
        clean &= stroke_loops(
            &mut loops,
            chain,
            distance,
            cap_style,
            rule,
            quadrant_segments,
        )?;
    }
    if loops.is_empty() {
        // Every chain was empty: the buffer is legitimately empty.
        return Some(Shape::empty_polygon());
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
        && let Some(polygon) = simple_stroke_polygon(xs, ys)
    {
        return Some(Shape::Polygon(polygon));
    }
    let parts = winding_region(&loops, |winding| winding >= 1);
    if parts.is_empty() {
        return None;
    }
    Some(polygon_parts_to_shape(parts))
}

/// One stroke loop as a directly valid polygon: close the columns into a
/// ring and accept only a simple, positive-area result (the complete gate
/// for the single-loop fast path; anything else falls back to winding).
fn simple_stroke_polygon(xs: &[f64], ys: &[f64]) -> Option<Polygon> {
    if xs.len() < 3 {
        return None;
    }
    let mut ring_xs = Vec::with_capacity(xs.len() + 1);
    let mut ring_ys = Vec::with_capacity(ys.len() + 1);
    ring_xs.extend_from_slice(xs);
    ring_ys.extend_from_slice(ys);
    ring_xs.push(xs[0]);
    ring_ys.push(ys[0]);
    let ring = CoordSeq::from_columns(ring_xs.into(), ring_ys.into(), None, None);
    (ring_winding(&ring).is_ccw()
        && Shape::LineString(LineSeq::from_trusted(ring.clone())).is_simple())
    .then(|| Polygon {
        shell: Ring::from_trusted_closed(ring),
        holes: Vec::new().into(),
    })
}

/// Buffer of a whole `GeometryCollection` in ONE winding pass: every part
/// reduces to raw loops in the SAME arrangement — circles for points,
/// stroke loops for chains, offset loops for polygons — and the union of
/// all the part buffers IS the `winding >= 1` region (GEOS instead
/// buffers each part and runs a boolean union cascade). Negative
/// distances erode the polygonal parts (their union of erosions is the
/// `winding <= -1` region) and annihilate puntal/lineal parts exactly.
pub(in crate::geometry) fn winding_collection(
    parts: &[Shape],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let mut loops: Vec<LoopColumns> = Vec::new();
    if distance <= 0.0 {
        // Distance 0 included: the unmoved flipped rings select the
        // valid region of the polygonal parts (see `winding_route`).
        collection_erosion_loops(&mut loops, parts, -distance, rule, quadrant_segments)?;
        let pieces = winding_region(&loops, |winding| winding <= -1);
        return Some(polygon_parts_to_shape(pieces));
    }
    collection_loops(
        &mut loops,
        parts,
        distance,
        cap_style,
        rule,
        quadrant_segments,
    )?;
    if loops.is_empty() {
        return Some(Shape::empty_polygon());
    }
    let pieces = winding_region(&loops, |winding| winding >= 1);
    if pieces.is_empty() {
        return None;
    }
    Some(polygon_parts_to_shape(pieces))
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
) -> Option<()> {
    let shell = strict_cycle(polygon.shell.coords(), false)?;
    loops.push(raw_offset_loop(&shell, distance, rule, quadrant_segments)?);
    for hole in polygon.holes.iter() {
        // Degenerate (zero-area) holes do not affect the buffer.
        let Some(ring) = strict_cycle(hole.coords(), true) else {
            continue;
        };
        let raw = raw_offset_loop(&ring, distance, rule, quadrant_segments)?;
        extend_cleaned(loops, &raw);
    }
    Some(())
}

/// Append one polygon's erosion loops (see [`winding_erosion`]): the
/// flipped-orientation walks, with the SHELL taking the per-loop
/// inversion cleaning.
pub(crate) fn erosion_loops(
    loops: &mut Vec<LoopColumns>,
    polygon: &Polygon,
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<()> {
    let shell = strict_cycle(polygon.shell.coords(), true)?;
    let raw = raw_offset_loop(&shell, distance, rule, quadrant_segments)?;
    // Surviving eroded lobes are CW pieces; keep them CW (they ARE the
    // negative-winding region's boundary in the final pass).
    extend_cleaned(loops, &raw);
    for hole in polygon.holes.iter() {
        let Some(ring) = strict_cycle(hole.coords(), false) else {
            continue;
        };
        // Grown holes never invert; their self-crossings (reflex
        // corners) resolve in the global winding.
        loops.push(raw_offset_loop(&ring, distance, rule, quadrant_segments)?);
    }
    Some(())
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
) -> Option<bool> {
    // Reused per-thread gather: the second of the two per-row emitter
    // allocations (the reversed-side scratch is the other).
    thread_local! {
        static GATHER: std::cell::RefCell<Vec<Point>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    let mut points = GATHER.with(|cell| {
        let mut scratch = cell.take();
        scratch.clear();
        scratch.extend(chain.iter_coords());
        scratch
    });
    dedup_consecutive_points(&mut points);
    let result = match points.as_slice() {
        [] => Some(true),
        [point] => {
            loops.push(circle_loop(*point, distance, quadrant_segments)?);
            // A lone circle is simple by construction.
            Some(true)
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
            raw_stroke_loop(chain, distance, cap_style, rule, quadrant_segments).map(
                |(columns, clean)| {
                    loops.push(columns);
                    clean
                },
            )
        },
    };
    GATHER.with(|cell| cell.replace(points));
    result
}

/// One full-circle CCW loop around `center` — the buffer of a point and
/// the stroke of a zero-length chain. Inscribed vertices on the circle,
/// per the arc doctrine (`4 * quadrant_segments` steps).
pub(crate) fn circle_loop(
    center: Point,
    distance: f64,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<LoopColumns> {
    let steps = 4 * quadrant_segments.get() as usize;
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
        return None;
    }
    Some((xs, ys))
}

/// The buffer of a single point: the inscribed-circle polygon directly —
/// no arrangement work at all.
pub(crate) fn point_buffer(
    center: Point,
    distance: f64,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<Shape> {
    let (mut xs, mut ys) = circle_loop(center, distance, quadrant_segments)?;
    xs.push(xs[0]);
    ys.push(ys[0]);
    let seq = CoordSeq::from_owned_columns(xs, ys, None, None).ok()?;
    Some(Shape::Polygon(Polygon::new(
        Ring::from_trusted_closed(seq),
        Vec::new(),
    )))
}

/// Collect expansion loops across a collection's parts, recursively.
pub(crate) fn collection_loops(
    loops: &mut Vec<LoopColumns>,
    parts: &[Shape],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<()> {
    for part in parts {
        match part {
            Shape::Point(point) => loops.push(circle_loop(*point, distance, quadrant_segments)?),
            Shape::MultiPoint(points) => {
                for point in points.iter_coords() {
                    loops.push(circle_loop(point, distance, quadrant_segments)?);
                }
            },
            Shape::LineString(chain) => {
                stroke_loops(loops, chain, distance, cap_style, rule, quadrant_segments)?;
            },
            Shape::MultiLineString(lines) => {
                for chain in lines {
                    stroke_loops(loops, chain, distance, cap_style, rule, quadrant_segments)?;
                }
            },
            Shape::Polygon(polygon) => {
                expansion_loops(loops, polygon, distance, rule, quadrant_segments)?;
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    expansion_loops(loops, polygon, distance, rule, quadrant_segments)?;
                }
            },
            Shape::GeometryCollection(inner) => {
                collection_loops(loops, inner, distance, cap_style, rule, quadrant_segments)?;
            },
            Shape::Empty(..) => {},
        }
    }
    Some(())
}

/// Collect erosion loops across a collection's polygonal parts,
/// recursively (puntal/lineal parts erode to nothing).
pub(crate) fn collection_erosion_loops(
    loops: &mut Vec<LoopColumns>,
    parts: &[Shape],
    distance: f64,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<()> {
    for part in parts {
        match part {
            Shape::Polygon(polygon) => {
                erosion_loops(loops, polygon, distance, rule, quadrant_segments)?;
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    erosion_loops(loops, polygon, distance, rule, quadrant_segments)?;
                }
            },
            Shape::GeometryCollection(inner) => {
                collection_erosion_loops(loops, inner, distance, rule, quadrant_segments)?;
            },
            _ => {},
        }
    }
    Some(())
}

/// The extent segment `[lo, hi]` of a collinear chain (≥ 3 distinct-adjacent
/// vertices all on one line), or `None` when the chain bends. A connected
/// collinear polyline — even one that doubles back or closes on itself —
/// covers exactly the interval between its projection extremes, so stroking
/// that segment yields the correct stadium where stroking the folded path
/// would not (see the caller in [`stroke_loops`]).
pub(crate) fn collinear_extent(chain: &[Point]) -> Option<[Point; 2]> {
    let anchor = chain[0];
    // The vertex farthest from the anchor fixes a non-degenerate direction
    // (post-dedup the chain has ≥ 2 distinct vertices, so this is never the
    // anchor itself).
    let dist2 = |point: &Point| {
        let dx = point.x - anchor.x;
        let dy = point.y - anchor.y;
        dx * dx + dy * dy
    };
    let far = *chain[1..]
        .iter()
        .max_by(|left, right| dist2(left).total_cmp(&dist2(right)))?;
    if chain
        .iter()
        .any(|&point| orientation(anchor, far, point) != Orientation::Collinear)
    {
        return None;
    }
    let projection = |point: &Point| {
        (point.x - anchor.x) * (far.x - anchor.x) + (point.y - anchor.y) * (far.y - anchor.y)
    };
    let lo = *chain
        .iter()
        .min_by(|left, right| projection(left).total_cmp(&projection(right)))?;
    let hi = *chain
        .iter()
        .max_by(|left, right| projection(left).total_cmp(&projection(right)))?;
    Some([lo, hi])
}

/// One closed raw stroke loop around an open chain (see
/// [`winding_stroke`]). Interior joins follow the polygon rules: the
/// styled join on the outside of a turn, crossings (or through-vertex
/// excursions for deep folds) on the inside.
pub(crate) fn raw_stroke_loop(
    chain: &[Point],
    distance: f64,
    cap_style: BufferCapStyle,
    rule: WalkJoinRule,
    quadrant_segments: std::num::NonZeroU32,
) -> Option<(LoopColumns, bool)> {
    let quadrant_segments = quadrant_segments.get();
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments);
    let count = chain.len();
    let mut xs = Vec::with_capacity(4 * count + 8 * quadrant_segments as usize + 8);
    let mut ys = Vec::with_capacity(xs.capacity());
    // Forward (right) side, end cap, backward (left) side, start cap.
    let right_clean = stroke_side(chain, false, distance, rule, step_angle, &mut xs, &mut ys)?;
    let (end_prev, end) = (chain[count - 2], chain[count - 1]);
    let (enx, eny) = unit_right_normal(end_prev, end)?;
    emit_cap(
        end, enx, eny, distance, cap_style, step_angle, &mut xs, &mut ys,
    );
    let left_clean = stroke_side(chain, true, distance, rule, step_angle, &mut xs, &mut ys)?;
    let (start_next, start) = (chain[1], chain[0]);
    let (snx, sny) = unit_right_normal(start_next, start)?;
    emit_cap(
        start, snx, sny, distance, cap_style, step_angle, &mut xs, &mut ys,
    );
    if !column_all_finite(&xs) || !column_all_finite(&ys) {
        return None;
    }
    Some(((xs, ys), right_clean && left_clean))
}

/// Unit right-side normal of the edge `from -> to` (`None` for degenerate
/// or overflowing edges).
pub(crate) fn unit_right_normal(from: Point, to: Point) -> Option<(f64, f64)> {
    let (dx, dy) = (to.x - from.x, to.y - from.y);
    let length = (dx * dx + dy * dy).sqrt();
    if length == 0.0 || !length.is_finite() {
        return None;
    }
    Some((dy / length, -dx / length))
}

/// Emit the inscribed arc points of a join/cap around `center` from
/// `from_angle` sweeping counter-clockwise by `sweep` (normalized into
/// `[0, tau)`), stepping at most `step_angle`.
pub(crate) fn emit_arc(
    center: Point,
    from_angle: f64,
    mut sweep: f64,
    distance: f64,
    step_angle: f64,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) {
    if sweep < 0.0 {
        sweep += std::f64::consts::TAU;
    }
    let steps = (sweep / step_angle).ceil() as usize;
    if steps <= 1 {
        return;
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
        xs.push(center.x + distance * cos_theta);
        ys.push(center.y + distance * sin_theta);
    }
}

/// One directed side of a stroke: right-offset edges with the polygon
/// join rules. `reversed` walks the chain backward (which IS the left
/// side — the normals flip with the walk).
/// Emit one stroke side; `true` when no join fell back to an
/// `Excursion` — a consuming inside `Cross` trims its overlap away, but
/// an excursion lobe cancels ONLY in the winding selection, so its raw
/// loop can never be a directly valid ring.
pub(crate) fn stroke_side(
    chain: &[Point],
    reversed: bool,
    distance: f64,
    rule: WalkJoinRule,
    step_angle: f64,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) -> Option<bool> {
    // Reused per-thread scratch: bulk buffers stroke thousands of rows,
    // and the reversed-side gather was one of two allocations per row.
    thread_local! {
        static BACKWARD: std::cell::RefCell<Vec<Point>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    let backward = reversed.then(|| {
        BACKWARD.with(|cell| {
            let mut scratch = cell.take();
            scratch.clear();
            scratch.extend(chain.iter().rev().copied());
            scratch
        })
    });
    let walk: &[Point] = backward.as_deref().unwrap_or(chain);
    let plan = WalkPlan::new(walk, false, distance, rule, step_angle)?;
    let excursion_free = plan
        .joins
        .iter()
        .all(|join| !matches!(join, WalkJoin::Excursion));
    plan.emit(step_angle, xs, ys);
    if let Some(scratch) = backward {
        BACKWARD.with(|cell| cell.replace(scratch));
    }
    Some(excursion_free)
}
