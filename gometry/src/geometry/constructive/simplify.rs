mod plan;
mod rdp;

use crate::NonNegative;
use crate::error::Result;
use crate::geographic::{ChartSpanGuard, PeriodicAxis};
use crate::geometry::constructive::{WalkJoinRule, WalkPlan, materialize_walk, vw_keep};
use crate::geometry::{
    CoordSeq, Coordinates as _, ExpansionBudget, GeometryKind, LineSeq, MultiLineString,
    MultiMember, MultiPolygon, Orientation, Point, PointKey, Polygon, Ring, Segment, Shape,
    ShapeCountQuery, TopologyGuard, XY, dedup_consecutive_points, line_segments,
    open_point_cycle_winding, orientation, point_segment_distance, same_point,
    simplified_polygon_delta_is_simple, simplify_chain_guarded, triangle_area, vw_area_tolerance,
};

pub(crate) trait RdpEdgeSpanGuard: Copy {
    fn span_guard<'a>(&self, xs: &'a [f64]) -> Option<ChartSpanGuard<'a>>;
}

#[derive(Clone, Copy)]
pub(crate) struct PlanarRdpGuard;

impl RdpEdgeSpanGuard for PlanarRdpGuard {
    fn span_guard<'a>(&self, _xs: &'a [f64]) -> Option<ChartSpanGuard<'a>> {
        None
    }
}

#[derive(Clone, Copy)]
pub(crate) struct GeographicRdpGuard<'a>(pub(crate) &'a PeriodicAxis);

impl RdpEdgeSpanGuard for GeographicRdpGuard<'_> {
    fn span_guard<'a>(&self, xs: &'a [f64]) -> Option<ChartSpanGuard<'a>> {
        Some(ChartSpanGuard::new(xs, *self.0))
    }
}

/// The Douglas-Peucker keep mask over raw columns: `keep` and `stack` are
/// cleared and refilled (reusable across packed-array rows); `Ok(Some(kept_count))`
/// unless the chain is too short to simplify. The packed-array lane appends
/// kept vertices straight into new CSR columns from this mask. `stack` must
/// stay batch-local — never a receiver cache or lock (free-threading).
#[cfg(test)]
fn rdp_keep(
    xs: &[f64],
    ys: &[f64],
    tolerance: f64,
    keep: &mut Vec<bool>,
    stack: &mut Vec<(usize, usize)>,
) -> Result<Option<usize>> {
    rdp_keep_with_edge_guard(xs, ys, tolerance, PlanarRdpGuard, keep, stack)
}

/// Monomorphized DP keep mask with a planar or geographic edge-span policy.
pub(crate) fn rdp_keep_with_edge_guard<G: RdpEdgeSpanGuard>(
    xs: &[f64],
    ys: &[f64],
    tolerance: f64,
    edge_guard: G,
    keep: &mut Vec<bool>,
    stack: &mut Vec<(usize, usize)>,
) -> Result<Option<usize>> {
    let count = xs.len();
    if count <= 2 {
        return Ok(None);
    }
    if !tolerance.is_finite() {
        return rdp_keep_with_query(xs, edge_guard, keep, stack, |_, _| Ok(None));
    }
    if count > 1024 {
        let mut index: Option<rdp::Index<'_>> = None;
        let mut scanned = 0_usize;
        // Balanced partitions fit this scan budget. An unbalanced chain builds
        // the range index before repeated scans can become quadratic.
        let budget = count.saturating_mul(8 * count.ilog2() as usize);
        rdp_keep_with_query(xs, edge_guard, keep, stack, |start, end| {
            if let Some(index) = &index {
                return index.split(start, end, tolerance);
            }
            scanned = scanned.saturating_add(end.wrapping_sub(start));
            if scanned > budget {
                let accelerated = rdp::Index::new(xs, ys)?;
                let split = accelerated.split(start, end, tolerance)?;
                index = Some(accelerated);
                Ok(split)
            } else {
                rdp::split(xs, ys, start, end, tolerance)
            }
        })
    } else {
        let tolerance_squared = tolerance * tolerance;
        rdp_keep_with_query(xs, edge_guard, keep, stack, |start, end| {
            rdp::split_inline!(xs, ys, start, end, tolerance, tolerance_squared)
        })
    }
}

fn rdp_keep_with_query<G: RdpEdgeSpanGuard>(
    xs: &[f64],
    edge_guard: G,
    keep: &mut Vec<bool>,
    stack: &mut Vec<(usize, usize)>,
    mut query: impl FnMut(usize, usize) -> Result<Option<usize>>,
) -> Result<Option<usize>> {
    let count = xs.len();
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
    let (mut start, mut end) = (0_usize, count.wrapping_sub(1));
    let span_guard = edge_guard.span_guard(xs);
    loop {
        let split = if span_guard
            .as_ref()
            .is_some_and(|guard| guard.rejects_span(start, end))
        {
            Some(start.wrapping_add(end.wrapping_sub(start) / 2))
        } else {
            query(start, end)?
        };
        if let Some(farthest) = split {
            *keep
                .get_mut(farthest)
                .expect("Douglas-Peucker split belongs to its source range") = true;
            kept = kept.wrapping_add(1);
            if farthest.wrapping_sub(start) >= 2 {
                if end.wrapping_sub(farthest) >= 2 {
                    stack.push((start, farthest));
                    start = farthest;
                } else {
                    end = farthest;
                }
                continue;
            }
            if end.wrapping_sub(farthest) >= 2 {
                start = farthest;
                continue;
            }
        }
        let Some(next) = stack.pop() else {
            break;
        };
        (start, end) = next;
    }
    Ok(Some(kept))
}

#[cfg(test)]
#[test]
fn near_tolerance_chain_does_not_split() {
    let xs = [
        0.0,
        134_217_728.0,
        134_217_732.0,
        134_217_736.0,
        268_435_457.0,
    ];
    let ys = [
        0.0,
        134_217_729.0,
        134_217_733.0,
        134_217_737.0,
        268_435_457.0,
    ];
    // Each interior point is one unit above y=x and projects inside the chord.
    // Its cross product is 268_435_457, while the chord length is that value
    // times sqrt(2), so the segment distance is 1 / sqrt(2), below tolerance.
    assert_eq!(rdp::split(&xs, &ys, 0, 4, 0.707_106_79).unwrap(), None);
}

#[cfg(test)]
#[test]
fn equal_height_sites_choose_the_first_source_position() {
    let xs: [f64; 32] = std::array::from_fn(|index| index as f64);
    let ys: [f64; 32] =
        std::array::from_fn(|index| if index == 2 || index == 9 { 1.0 } else { 0.0 });
    // Equal maxima at positions 2 and 9 are one unit above the horizontal chord.
    assert_eq!(rdp::split(&xs, &ys, 0, 31, 0.5).unwrap(), Some(2));
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

fn has_two_distinct_xy(coords: &CoordSeq) -> bool {
    let Some((&first_x, &first_y)) = coords.xs().first().zip(coords.ys().first()) else {
        return false;
    };
    let first = PointKey::new(XY::new(first_x, first_y));
    coords
        .xs()
        .iter()
        .zip(coords.ys())
        .skip(1)
        .any(|(&x, &y)| PointKey::new(XY::new(x, y)) != first)
}

fn coords_are_closed(coords: &CoordSeq) -> bool {
    coords
        .xs()
        .first()
        .zip(coords.ys().first())
        .zip(coords.xs().last().zip(coords.ys().last()))
        .is_some_and(|((&first_x, &first_y), (&last_x, &last_y))| {
            same_point(XY::new(first_x, first_y), XY::new(last_x, last_y))
        })
}

fn line_kept_structure(original: &LineSeq, simplified: &LineSeq) -> bool {
    let original = original.as_coords();
    let simplified = simplified.as_coords();
    if !has_two_distinct_xy(original) {
        return true;
    }
    if !has_two_distinct_xy(simplified) {
        return false;
    }
    if !coords_are_closed(original) {
        return true;
    }
    coords_are_closed(simplified)
        && (!has_three_distinct_xy(original)
            || (simplified.len() >= 4 && has_three_distinct_xy(simplified)))
}

/// Operation-local keep-count identity. `Unchanged` means every chain kept
/// every vertex, so the caller clones `self` instead of staging output.
enum RawSimplify {
    Unchanged,
    Changed(Shape),
}

/// Source vertex indices retained by polygonal geographic simplification.
///
/// `polygons` follows the output shape's polygon-value order. Each polygon
/// contains one index list per output ring, in shell-then-hole order.
pub(crate) struct PolygonalSimplificationPlan {
    pub(crate) polygons: Vec<Vec<Vec<usize>>>,
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
    kept_vertices: Option<&mut Vec<usize>>,
) -> Result<Option<CoordSeq>> {
    let Some(kept) = keep_mask(points.xs(), points.ys(), scratch)? else {
        if let Some(kept_vertices) = kept_vertices {
            kept_vertices.extend(0..points.len());
        }
        return Ok(None);
    };
    if kept == points.len() {
        if let Some(kept_vertices) = kept_vertices {
            kept_vertices.extend(0..points.len());
        }
        return Ok(None);
    }
    if let Some(kept_vertices) = kept_vertices {
        kept_vertices.extend(
            scratch
                .keep
                .iter()
                .enumerate()
                .filter_map(|(index, &keep)| keep.then_some(index)),
        );
    }
    Ok(Some(points.select_mask(&scratch.keep, kept)))
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
    capture_plan: bool,
) -> Result<(RingKeep, Option<Vec<usize>>)> {
    let mut kept_vertices = capture_plan.then(Vec::new);
    let output = kept_coords(ring.coords(), scratch, keep_mask, kept_vertices.as_mut())?;
    Ok(match output {
        None => (RingKeep::Same, kept_vertices),
        Some(coords) => Ring::from_closed(coords).map_or((RingKeep::Drop, None), |ring| {
            (RingKeep::New(ring), kept_vertices)
        }),
    })
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
    capture_plan: bool,
) -> Result<(PolygonKeep, Option<Vec<Vec<usize>>>)> {
    let mut ring_plans =
        capture_plan.then(|| Vec::with_capacity(polygon.holes.len().saturating_add(1)));
    let (shell_keep, shell_plan) = keep_ring(&polygon.shell, scratch, keep_mask, capture_plan)?;
    let new_shell = match shell_keep {
        RingKeep::Drop => return Ok((PolygonKeep::Empty, None)),
        RingKeep::Same => None,
        RingKeep::New(shell) => Some(shell),
    };
    if let Some(ring_plans) = &mut ring_plans {
        ring_plans.push(shell_plan.expect("a retained shell has a source vertex plan"));
    }
    let mut holes_out = new_shell
        .is_some()
        .then(|| Vec::with_capacity(polygon.holes.len()));
    for (index, hole) in polygon.holes.iter().enumerate() {
        let (ring_keep, ring_plan) = keep_ring(hole, scratch, keep_mask, capture_plan)?;
        match ring_keep {
            RingKeep::Same => {
                if let Some(holes) = holes_out.as_mut() {
                    holes.push(hole.clone());
                }
                if let Some(ring_plans) = &mut ring_plans {
                    ring_plans.push(ring_plan.expect("a retained hole has a source vertex plan"));
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
                if let Some(ring_plans) = &mut ring_plans {
                    ring_plans.push(ring_plan.expect("a rebuilt hole has a source vertex plan"));
                }
            },
        }
    }
    Ok((
        holes_out.map_or(PolygonKeep::Same, |holes| {
            PolygonKeep::New(Polygon::new(
                new_shell.unwrap_or_else(|| polygon.shell.clone()),
                holes,
            ))
        }),
        ring_plans,
    ))
}

fn prefix_members<'a, T: Clone + 'a>(
    members: impl Iterator<Item = MultiMember<&'a T>>,
    capacity: usize,
    end: usize,
) -> Vec<MultiMember<T>> {
    let mut out = Vec::with_capacity(capacity);
    out.extend(members.take(end).map(|member| match member {
        MultiMember::Empty => MultiMember::Empty,
        MultiMember::Value(value) => MultiMember::Value(value.clone()),
    }));
    out
}

fn keep_multi_lines(
    lines: &MultiLineString,
    scratch: &mut KeepScratch,
    keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
) -> Result<RawSimplify> {
    let mut out: Option<Vec<MultiMember<LineSeq>>> = None;
    for (index, member) in lines.members_with_values().enumerate() {
        match member {
            MultiMember::Empty => {
                if let Some(members) = out.as_mut() {
                    members.push(MultiMember::Empty);
                }
            },
            MultiMember::Value(line) => match kept_coords(line, scratch, keep_mask, None)? {
                None => {
                    if let Some(members) = out.as_mut() {
                        members.push(MultiMember::Value(line.clone()));
                    }
                },
                Some(coords) => {
                    if let Some(members) = out.as_mut() {
                        members.push(MultiMember::Value(LineSeq::from_proven(coords)));
                    } else {
                        let mut members = prefix_members(
                            lines.members_with_values(),
                            lines.member_count(),
                            index,
                        );
                        members.push(MultiMember::Value(LineSeq::from_proven(coords)));
                        out = Some(members);
                    }
                },
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
    capture_plan: bool,
) -> Result<(RawSimplify, Vec<Vec<Vec<usize>>>)> {
    let mut out: Option<Vec<MultiMember<Polygon>>> = None;
    let mut plans = Vec::with_capacity(if capture_plan { polygons.len() } else { 0 });
    for (index, member) in polygons.members_with_values().enumerate() {
        match member {
            MultiMember::Empty => {
                if let Some(members) = out.as_mut() {
                    members.push(MultiMember::Empty);
                }
            },
            MultiMember::Value(polygon) => {
                let (keep, plan) = keep_polygon(polygon, scratch, keep_mask, capture_plan)?;
                match keep {
                    PolygonKeep::Same => {
                        if let Some(members) = out.as_mut() {
                            members.push(MultiMember::Value(polygon.clone()));
                        }
                        if capture_plan {
                            plans.push(plan.expect("a retained polygon has a source vertex plan"));
                        }
                    },
                    PolygonKeep::Empty => {
                        if let Some(members) = out.as_mut() {
                            members.push(MultiMember::Empty);
                        } else {
                            let mut members = prefix_members(
                                polygons.members_with_values(),
                                polygons.member_count(),
                                index,
                            );
                            members.push(MultiMember::Empty);
                            out = Some(members);
                        }
                    },
                    PolygonKeep::New(polygon) => {
                        if let Some(members) = out.as_mut() {
                            members.push(MultiMember::Value(polygon));
                        } else {
                            let mut members = prefix_members(
                                polygons.members_with_values(),
                                polygons.member_count(),
                                index,
                            );
                            members.push(MultiMember::Value(polygon));
                            out = Some(members);
                        }
                        if capture_plan {
                            plans.push(plan.expect("a rebuilt polygon has a source vertex plan"));
                        }
                    },
                }
            },
        }
    }
    Ok((
        match out {
            None => RawSimplify::Unchanged,
            Some(members) => RawSimplify::Changed(Shape::MultiPolygon(MultiPolygon::try_new(
                polygons.axes(),
                members,
            )?)),
        },
        plans,
    ))
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
        self.simplify_dp_with_edge_guard::<false>(
            tolerance,
            preserve_topology,
            crate::geographic::GeographicFrame::crs84().longitude(),
        )
    }

    pub(crate) fn simplify_dp_geographic(
        &self,
        tolerance: f64,
        preserve_topology: bool,
        axis: PeriodicAxis,
    ) -> Result<Self> {
        self.simplify_dp_with_edge_guard::<true>(tolerance, preserve_topology, axis)
    }

    fn simplify_dp_with_edge_guard<const CHECK_EDGE_SPAN: bool>(
        &self,
        tolerance: f64,
        preserve_topology: bool,
        axis: PeriodicAxis,
    ) -> Result<Self> {
        match self.simplify_raw_dp_with_edge_guard::<CHECK_EDGE_SPAN>(tolerance, axis)? {
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
                    self.simplify_guarded::<CHECK_EDGE_SPAN>(tolerance, axis)
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
                    .members_with_values()
                    .map(|member| match member {
                        crate::geometry::MultiMember::Empty => crate::geometry::MultiMember::Empty,
                        crate::geometry::MultiMember::Value(polygon) => clean(polygon).map_or(
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

    /// Whether raw output satisfies the per-geometry topology checks:
    /// valid polygonal input stays valid and non-collapsed; simple lineal input
    /// stays simple, and closed line strings retain a closed non-degenerate
    /// form. Geometry-collection children are checked independently, so
    /// relationships between separate children are not preserved.
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
            Self::LineString(original) => {
                let Self::LineString(simplified_line) = simplified else {
                    return Ok(false);
                };
                if !line_kept_structure(original, simplified_line) {
                    return Ok(false);
                }
                Ok(!self.is_simple()? || simplified.is_simple()?)
            },
            Self::MultiLineString(original) => {
                let Self::MultiLineString(simplified_lines) = simplified else {
                    return Ok(false);
                };
                if original.len() != simplified_lines.len()
                    || !original
                        .as_slice()
                        .iter()
                        .zip(simplified_lines.as_slice())
                        .all(|(original, simplified)| line_kept_structure(original, simplified))
                {
                    return Ok(false);
                }
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
    fn simplify_guarded<const CHECK_EDGE_SPAN: bool>(
        &self,
        tolerance: f64,
        axis: PeriodicAxis,
    ) -> Result<Self> {
        self.simplify_guarded_with::<CHECK_EDGE_SPAN>(axis, |prev, vertex, next| {
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
    fn simplify_guarded_with<const CHECK_EDGE_SPAN: bool>(
        &self,
        axis: PeriodicAxis,
        candidate: impl Fn(XY, XY, XY) -> Result<Option<f64>>,
    ) -> Result<Self> {
        self.simplify_guarded_with_capture::<CHECK_EDGE_SPAN>(axis, candidate, false)
            .map(|(shape, _)| shape)
    }

    fn simplify_guarded_with_capture<const CHECK_EDGE_SPAN: bool>(
        &self,
        axis: PeriodicAxis,
        candidate: impl Fn(XY, XY, XY) -> Result<Option<f64>>,
        capture_plan: bool,
    ) -> Result<(Self, Option<Vec<Vec<usize>>>)> {
        let mut counts = ShapeCountQuery::default();
        let segment_count = counts.segments(self)?;
        let coordinate_count = counts.coordinates(self)?;
        drop(counts);
        let mut segments = Vec::with_capacity(segment_count);
        let Ok(()) = self.try_for_each_segment_chain(|chain| {
            segments.extend(line_segments(chain));
            Ok::<_, std::convert::Infallible>(())
        });
        let mut vertex_cloud = Vec::with_capacity(coordinate_count);
        // Original XY only: the guard indexes the source point set, never
        // simplified output. Isolated points participate in the vertex
        // probe, so the walk is every vertex, not only segment chains.
        let Ok(()) = self.try_for_each_point(|point| {
            vertex_cloud.push([point.x, point.y]);
            Ok::<_, std::convert::Infallible>(())
        });
        let guard = TopologyGuard::new(&segments, vertex_cloud);
        let mut chain_plans = capture_plan.then(Vec::new);
        let shape = self.map_chains(&mut |points: &CoordSeq| {
            let xs = points.xs();
            let ys = points.ys();
            let count = xs.len();
            let span_guard = CHECK_EDGE_SPAN.then(|| ChartSpanGuard::new(xs, axis));
            let closed = count >= 2
                && xs
                    .first()
                    .zip(ys.first())
                    .zip(xs.last().zip(ys.last()))
                    .is_some_and(|((first_x, first_y), (last_x, last_y))| {
                        same_point(XY::new(*first_x, *first_y), XY::new(*last_x, *last_y))
                    });
            let xy = |index| {
                XY::new(
                    *xs.get(index)
                        .expect("guarded simplification vertex belongs to its source"),
                    *ys.get(index)
                        .expect("guarded simplification vertex belongs to its source"),
                )
            };
            let kept = simplify_chain_guarded(
                count,
                xy,
                closed,
                &|prev, vertex, next| {
                    if span_guard
                        .as_ref()
                        .is_some_and(|guard| guard.rejects_span(prev, next))
                    {
                        Ok(None)
                    } else {
                        candidate(xy(prev), xy(vertex), xy(next))
                    }
                },
                &guard,
            )?;
            let output = if kept.len() == count {
                None
            } else {
                Some(points.select(kept.iter().copied())?)
            };
            if let Some(chain_plans) = &mut chain_plans {
                chain_plans.push(kept);
            }
            Ok(output)
        })?;
        Ok((shape, chain_plans))
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
        self.simplify_vw_with_edge_guard::<false>(
            tolerance,
            preserve_topology,
            crate::geographic::GeographicFrame::crs84().longitude(),
        )
    }

    pub(crate) fn simplify_vw_geographic(
        &self,
        tolerance: f64,
        preserve_topology: bool,
        axis: PeriodicAxis,
    ) -> Result<Self> {
        self.simplify_vw_with_edge_guard::<true>(tolerance, preserve_topology, axis)
    }

    fn simplify_vw_with_edge_guard<const CHECK_EDGE_SPAN: bool>(
        &self,
        tolerance: f64,
        preserve_topology: bool,
        axis: PeriodicAxis,
    ) -> Result<Self> {
        match self.simplify_raw_vw_with_edge_guard::<CHECK_EDGE_SPAN>(tolerance, axis)? {
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
                    self.simplify_guarded_with::<CHECK_EDGE_SPAN>(axis, |prev, vertex, next| {
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

    #[cfg(test)]
    fn simplify_raw_dp(&self, tolerance: f64) -> Result<RawSimplify> {
        self.simplify_raw_dp_with_edge_guard::<false>(
            tolerance,
            crate::geographic::GeographicFrame::crs84().longitude(),
        )
    }

    fn simplify_raw_dp_with_edge_guard<const CHECK_EDGE_SPAN: bool>(
        &self,
        tolerance: f64,
        axis: PeriodicAxis,
    ) -> Result<RawSimplify> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let mut scratch = KeepScratch::default();
        self.simplify_raw(&mut scratch, &mut |xs, ys, scratch| {
            if CHECK_EDGE_SPAN {
                rdp_keep_with_edge_guard(
                    xs,
                    ys,
                    tolerance,
                    GeographicRdpGuard(&axis),
                    &mut scratch.keep,
                    &mut scratch.stack,
                )
            } else {
                rdp_keep_with_edge_guard(
                    xs,
                    ys,
                    tolerance,
                    PlanarRdpGuard,
                    &mut scratch.keep,
                    &mut scratch.stack,
                )
            }
        })
    }

    #[cfg(test)]
    fn simplify_raw_vw(&self, tolerance: f64) -> Result<RawSimplify> {
        self.simplify_raw_vw_with_edge_guard::<false>(
            tolerance,
            crate::geographic::GeographicFrame::crs84().longitude(),
        )
    }

    fn simplify_raw_vw_with_edge_guard<const CHECK_EDGE_SPAN: bool>(
        &self,
        tolerance: f64,
        axis: PeriodicAxis,
    ) -> Result<RawSimplify> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let mut scratch = KeepScratch::default();
        self.simplify_raw(&mut scratch, &mut |xs, ys, scratch| {
            Ok(if CHECK_EDGE_SPAN {
                crate::geometry::vw_keep_geographic(xs, ys, tolerance, axis, &mut scratch.keep)
            } else {
                vw_keep(xs, ys, tolerance, &mut scratch.keep)
            })
        })
    }

    fn simplify_raw(
        &self,
        scratch: &mut KeepScratch,
        keep_mask: &mut impl FnMut(&[f64], &[f64], &mut KeepScratch) -> Result<Option<usize>>,
    ) -> Result<RawSimplify> {
        Ok(match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => RawSimplify::Unchanged,
            Self::LineString(points) => kept_coords(points, scratch, keep_mask, None)?
                .map_or(RawSimplify::Unchanged, |coords| {
                    RawSimplify::Changed(Self::LineString(LineSeq::from_proven(coords)))
                }),
            Self::MultiLineString(lines) => keep_multi_lines(lines, scratch, keep_mask)?,
            Self::Polygon(polygon) => match keep_polygon(polygon, scratch, keep_mask, false)?.0 {
                PolygonKeep::Same => RawSimplify::Unchanged,
                PolygonKeep::Empty => {
                    RawSimplify::Changed(Self::typed_empty(GeometryKind::Polygon, polygon.axes()))
                },
                PolygonKeep::New(polygon) => RawSimplify::Changed(Self::Polygon(polygon)),
            },
            Self::MultiPolygon(polygons) => {
                keep_multi_polygons(polygons, scratch, keep_mask, false)?.0
            },
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
    use crate::geometry::segment_index::take_contact_visits;
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
    fn dp_sawtooth_source_work_grows_below_quadratic() {
        let mut counts = Vec::new();
        for count in [2048, 4096] {
            let xs: Vec<_> = (0..count).map(f64::from).collect();
            let ys: Vec<_> = (0..count).map(|index| f64::from(index % 2)).collect();
            let line = Shape::LineString(line_xy(&xs, &ys));
            super::rdp::take_source_visits();
            let simplified = line.simplify_dp(0.1, false).unwrap();
            counts.push(super::rdp::take_source_visits());
            assert_eq!(simplified, line);
        }
        assert!(
            counts[1] * 10 < counts[0] * 27,
            "doubling sawtooth source visits must stay below 2.7×: {counts:?}",
        );
    }

    #[test]
    fn guarded_radial_simplification_contact_work_stays_below_quadratic() {
        let mut counts = Vec::new();
        for count in [2048, 4096] {
            let mut xs = Vec::new();
            let mut ys = Vec::new();
            for index in 0..count {
                let angle = f64::from(index) * std::f64::consts::TAU / f64::from(count);
                let radius = 0.8 + 0.2 * (f64::from(index * 37 % 101) / 100.0);
                xs.push(radius * angle.cos());
                ys.push(radius * angle.sin());
            }
            xs.push(xs[0]);
            ys.push(ys[0]);
            let polygon = Shape::Polygon(Polygon::new(closed_ring(&xs, &ys), Vec::new()));
            take_contact_visits();
            let simplified = polygon
                .simplify_vw((1.0 / f64::from(count)).sqrt(), true)
                .unwrap();
            counts.push(take_contact_visits());
            assert!(!simplified.is_empty());
            assert!(simplified.is_simple().unwrap());
        }
        assert!(counts[0] > 0, "the fixture must probe chord contacts");
        assert!(
            counts[1] * 10 < counts[0] * 30,
            "doubling dense radial input must not quadruple contact probes: {counts:?}",
        );
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
