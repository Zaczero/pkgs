#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::NonNegative;
use crate::error::Result;
/// One chain through iterative Douglas-Peucker as a keep-mask over the
/// original points: surviving vertices are the INPUT vertices (bit-exact,
/// full Z/M), endpoints always survive, and a range splits at its
/// farthest interior vertex while that deviation exceeds the tolerance
/// (matching the classic strictly-greater rule). `None` when EVERY
/// vertex survives — the caller reuses the input chain without
/// re-materializing it (tolerances below the feature size cost only the
/// scan). Deviations compare in MULTIPLIED squared space (`cross²` vs
/// `tolerance² × chord²` — no division per candidate); non-finite
/// products rescue through the exact general kernel.
pub(crate) fn rdp_chain(points: &CoordSeq, tolerance: f64) -> Option<Vec<Point>> {
    let mut keep = Vec::new();
    let kept = rdp_keep(points.xs(), points.ys(), tolerance, &mut keep)?;
    if kept == points.len() {
        return None;
    }
    Some(
        points
            .iter_coords()
            .zip(&keep)
            .filter_map(|(point, &kept)| kept.then_some(point))
            .collect(),
    )
}

/// The Douglas-Peucker keep mask over raw columns: `keep` is cleared and
/// refilled (reusable across rows); `Some(kept_count)` unless the chain
/// is too short to simplify. The packed-array lane appends kept vertices
/// straight into new CSR columns from this mask.
pub(crate) fn rdp_keep(
    xs: &[f64],
    ys: &[f64],
    tolerance: f64,
    keep: &mut Vec<bool>,
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
    let mut stack = vec![(0_usize, count - 1)];
    while let Some((start, end)) = stack.pop() {
        if end - start < 2 {
            continue;
        }
        if let Some(farthest) = rdp_split(xs, ys, start, end, tolerance_squared) {
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
pub(crate) fn rdp_split(
    xs: &[f64],
    ys: &[f64],
    start: usize,
    end: usize,
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
    if !threshold.is_finite() || chord_squared <= 0.0 {
        exact_scan(&mut best, &mut farthest);
        return (best > tolerance_squared).then_some(farthest);
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
) -> Option<(Vec<f64>, Vec<f64>)> {
    let quadrant_segments = quadrant_segments.get();
    let step_angle = std::f64::consts::FRAC_PI_2 / f64::from(quadrant_segments);
    let plan = WalkPlan::new(strict, true, distance, rule, step_angle)?;
    // Capacity: two offset points per edge, up to one reflex insert per
    // vertex, and one full turn (4 quadrants) of arc steps.
    let mut xs = Vec::with_capacity(3 * strict.len() + 4 * quadrant_segments as usize + 2);
    let mut ys = Vec::with_capacity(xs.capacity());
    plan.emit(step_angle, &mut xs, &mut ys);
    if !column_all_finite(&xs) || !column_all_finite(&ys) {
        return None;
    }
    Some((xs, ys))
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
            Bounds::new_unchecked(minx, miny, cut, maxy),
            Bounds::new_unchecked(cut, miny, maxx, maxy),
        ]
    } else {
        [
            Bounds::new_unchecked(minx, miny, maxx, cut),
            Bounds::new_unchecked(minx, cut, maxx, maxy),
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
        let raw = self.simplify_dp_raw(tolerance)?;
        // Without the topology guard, raw DP can collapse a polygon ring below
        // its three distinct corners (a sliver) — a degenerate INVALID polygon.
        // Drop collapsed areal rings so a vanished polygon is `POLYGON EMPTY`
        // (matching Shapely), never an invalid 2-vertex ring.
        if !preserve_topology {
            return Ok(raw.dropping_collapsed_rings());
        }
        // An unchanged output has, by identity, exactly the input's
        // topology — the validity branch-check only runs when vertices
        // actually dropped (tolerances below the feature size skip it
        // entirely).
        if raw == *self || self.simplify_kept_topology(&raw) {
            return Ok(raw);
        }
        Ok(self.simplify_guarded(tolerance))
    }

    /// Drop areal rings that simplification collapsed below three distinct
    /// corners (they enclose no area): a vanished shell makes the polygon
    /// `POLYGON EMPTY`, a vanished hole is removed; lineal/puntal parts are
    /// untouched. Keeps `simplify`/`simplify_vw` with `preserve_topology=False`
    /// from leaking a degenerate invalid polygon.
    fn dropping_collapsed_rings(self) -> Self {
        fn clean(polygon: &Polygon) -> Option<Polygon> {
            if unique_xy_points(&polygon.shell).len() < 3 {
                return None;
            }
            Some(Polygon::new(
                polygon.shell.clone(),
                polygon
                    .holes
                    .iter()
                    .filter(|hole| unique_xy_points(*hole).len() >= 3)
                    .cloned()
                    .collect(),
            ))
        }
        match self {
            Self::Polygon(polygon) => {
                clean(&polygon).map_or_else(Self::empty_polygon, Self::Polygon)
            },
            Self::MultiPolygon(polygons) => {
                let kept: Vec<Polygon> = polygons.iter().filter_map(clean).collect();
                if kept.is_empty() {
                    Self::empty_polygon()
                } else {
                    Self::MultiPolygon(kept)
                }
            },
            Self::GeometryCollection(parts) => Self::GeometryCollection(
                parts
                    .into_iter()
                    .map(Self::dropping_collapsed_rings)
                    .collect(),
            ),
            other => other,
        }
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
    fn simplify_guarded(&self, tolerance: f64) -> Self {
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
    ) -> Self {
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
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        // Own Douglas-Peucker: a keep-mask over the ORIGINAL points, so
        // Z/M survive verbatim with no geo round-trip, no libm `hypot`
        // distances (the scalar-libcall trap geo's kernel pays per
        // candidate), and no post-hoc ordinate resolution pass.
        Ok(self.map_chains(&|points: &CoordSeq| rdp_chain(points, tolerance)))
    }

    /// Visvalingam-Whyatt simplification with the same topology contract
    /// as `simplify`: raw VW is the fast path; a guarded greedy pass only
    /// kicks in when it broke validity or simplicity. `tolerance` is
    /// distance-scale — the effective-area threshold is `tolerance^2 / 2`,
    /// the same conversion `coverage_simplify` (and GEOS) uses — so the
    /// same value is directly comparable between `simplify`, `simplify_vw`,
    /// and `coverage_simplify`.
    pub fn simplify_vw(&self, tolerance: f64, preserve_topology: bool) -> Result<Self> {
        let raw = self.simplify_vw_raw(tolerance)?;
        if !preserve_topology {
            return Ok(raw.dropping_collapsed_rings());
        }
        if raw == *self || self.simplify_kept_topology(&raw) {
            return Ok(raw);
        }
        let area_tolerance = vw_area_tolerance(tolerance);
        Ok(self.simplify_guarded_with(|prev, vertex, next| {
            let area = triangle_area(prev, vertex, next);
            (area < area_tolerance).then_some(area)
        }))
    }

    pub fn simplify_vw_raw(&self, tolerance: f64) -> Result<Self> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let area_tolerance = vw_area_tolerance(tolerance);
        // Native heap-driven Visvalingam-Whyatt — no geo round-trip, and
        // surviving vertices KEEP their Z/M (matching the DP route).
        let vw_line = |points: &CoordSeq| -> CoordSeq { vw_filter(points, area_tolerance) };
        let vw_ring = |ring: &Ring| -> Ring {
            Ring::from_trusted_closed(vw_filter(ring.coords(), area_tolerance))
        };
        let vw_polygon = |polygon: &Polygon| -> Polygon {
            Polygon::new(
                vw_ring(&polygon.shell),
                polygon.holes.iter().map(&vw_ring).collect(),
            )
        };
        let simplified = match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => return Ok(self.clone()),
            Self::LineString(points) => Self::LineString(
                LineSeq::try_new(vw_line(points))
                    .expect("line simplification keeps empty or at least two vertices"),
            ),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| {
                        LineSeq::try_new(vw_line(line))
                            .expect("line simplification keeps empty or at least two vertices")
                    })
                    .collect(),
            ),
            Self::Polygon(polygon) => Self::Polygon(vw_polygon(polygon)),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.iter().map(vw_polygon).collect())
            },
            Self::GeometryCollection(geometries) => {
                return Ok(Self::GeometryCollection(
                    geometries
                        .iter()
                        .map(|geometry| geometry.simplify_vw_raw(tolerance))
                        .collect::<Result<_, _>>()?,
                ));
            },
        };
        // Visvalingam-Whyatt also keeps a subset of the input vertices, so
        // Z/M carry by exact match like simplify.
        carry_ordinates(simplified, &[self], "simplify_vw", false)
    }
}
