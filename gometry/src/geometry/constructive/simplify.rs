#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

mod rdp;

use crate::NonNegative;
use crate::error::{ResourceError, Result};
use crate::geographic::{ChartSpanGuard, GeographicFrame, GeographicInputs, PeriodicAxis};
use crate::geometry::constructive::{WalkJoinRule, WalkPlan, materialize_walk, vw_keep};
use crate::geometry::overlay::{ExactSourceInventory, SubdivisionPart, rect_polygon};
use crate::geometry::topology::{FeatureFamilyRegistry, PreparedNativeSource, SourceTrace};
use crate::geometry::{
    Bounds, Component, CoordSeq, CoordWindow, Coordinates as _, ExpansionBudget, GeometryErrorKind,
    GeometryKind, LineSeq, MultiLineString, MultiMember, MultiPolygon, Orientation, Point,
    PointKey, Polygon, Ring, Segment, Shape, ShapeCountQuery, SharedShapeOwners, SourceArcContext,
    TopologyGuard, Unmetered, Work, XY, dedup_consecutive_points, line_segments,
    open_point_cycle_winding, orientation, point_segment_distance, same_point,
    simplified_polygon_delta_is_simple, simplify_chain_guarded, triangle_area, vw_area_tolerance,
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

/// Choose the representable cut nearest the median of distinct source-coordinate
/// ordinates. The source values, rather than a rounded child, own this decision:
/// a dense cluster beside a remote vertex therefore gets a finite complexity
/// split without a second median retry.
fn source_median_cut(
    shape: &Shape,
    split_x: bool,
    bounds: Bounds,
    owner_coordinate_count: usize,
) -> Option<f64> {
    let ordinates = source_ordinates(shape, split_x, owner_coordinate_count);
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

fn source_vertex_cut(
    shape: &Shape,
    split_x: bool,
    bounds: Bounds,
    owner_coordinate_count: usize,
) -> Option<f64> {
    let ordinates = source_ordinates(shape, split_x, owner_coordinate_count);
    let middle = ordinates.len() / 2;
    let (low, high) = if split_x {
        (bounds.minx(), bounds.maxx())
    } else {
        (bounds.miny(), bounds.maxy())
    };
    ordinates
        .iter()
        .enumerate()
        .filter_map(|(index, &ordinate)| {
            (low < ordinate && ordinate < high).then_some((index.abs_diff(middle), index, ordinate))
        })
        .min_by_key(|&(distance, index, _)| (distance, index))
        .map(|(_, _, cut)| cut)
}

fn bounds_midpoint_cut(bounds: Bounds, split_x: bool) -> Option<f64> {
    let (low, high) = if split_x {
        (bounds.minx(), bounds.maxx())
    } else {
        (bounds.miny(), bounds.maxy())
    };
    let cut = f64::midpoint(low, high);
    (low < cut && cut < high).then_some(cut)
}

fn source_ordinates(shape: &Shape, split_x: bool, owner_coordinate_count: usize) -> Vec<f64> {
    let mut ordinates = Vec::with_capacity(owner_coordinate_count);
    shape.for_each_first_point_run(|point| {
        ordinates.push(if split_x { point.x } else { point.y });
    });
    ordinates.sort_unstable_by(f64::total_cmp);
    #[expect(
        clippy::float_cmp,
        reason = "source ordinates are finite and exact equality merges signed zero but preserves adjacent vertices"
    )]
    ordinates.dedup_by(|left, right| *left == *right);
    ordinates
}

fn push_subdivision_members(
    item: &mut SubdivisionWorkItem,
    max_vertices: usize,
    work: &mut Vec<SubdivisionWorkItem>,
) -> Result<bool> {
    let shape = &item.shape;
    if !matches!(
        shape,
        Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
    ) {
        return Ok(false);
    }
    if matches!(shape, Shape::MultiPolygon(_))
        && shape.parts().any(|member| {
            ShapeCountQuery::default()
                .coordinate_count(&member)
                .is_none_or(|count| count > max_vertices)
        })
    {
        return Ok(false);
    }
    let members = shape.parts().collect::<Vec<_>>();
    if members.is_empty() {
        return Ok(false);
    }
    let member_traces = match item.traces.take() {
        Some(traces) if traces.len() == members.len() => Some(traces),
        Some(_) => {
            return Err(GeometryErrorKind::Invalid(
                "subdivision source traces do not match multipart members".into(),
            )
            .into());
        },
        None => None,
    };
    let source_line_members = item.source_line_members.take();
    let vertex_cut = item.vertex_cut;
    if let Some(traces) = member_traces {
        for (member, trace) in members.into_iter().zip(traces).rev() {
            work.push(SubdivisionWorkItem::with_traces(
                member,
                None,
                None,
                vec![trace],
                vertex_cut,
                item.trace_axes.unwrap_or_else(|| item.shape.axes()),
            ));
        }
    } else {
        let member_line_ranges = if let Some(parent_range) = source_line_members
            && parent_range.len() == source_line_member_count(shape)?
        {
            let mut next = parent_range.start;
            let mut ranges = Vec::with_capacity(members.len());
            for member in &members {
                let count = source_line_member_count(member)?;
                let end = next.checked_add(count).ok_or(ResourceError::SizeOverflow)?;
                ranges.push(Some(next..end));
                next = end;
            }
            ranges
        } else {
            std::iter::repeat_with(|| None)
                .take(members.len())
                .collect::<Vec<_>>()
        };
        for (member, source_line_members) in members.into_iter().zip(member_line_ranges).rev() {
            work.push(SubdivisionWorkItem::new(
                member,
                vertex_cut,
                source_line_members,
            ));
        }
    }
    Ok(true)
}

fn stage_line_vertices(
    item: &SubdivisionWorkItem,
    max_vertices: usize,
    work: &mut impl Work,
    staged: &mut Vec<SubdivisionWorkItem>,
) -> Result<bool> {
    let Shape::LineString(line) = &item.shape else {
        return Ok(false);
    };
    if line.len() <= max_vertices || max_vertices < 2 {
        return Ok(false);
    }
    let trace = match item.traces.as_deref() {
        Some([trace]) => Some(trace),
        Some(_) => {
            return Err(GeometryErrorKind::Invalid(
                "one line-string item has multiple source traces".into(),
            )
            .into());
        },
        None => None,
    };
    let mut start = 0;
    while start < line.len().saturating_sub(1) {
        let end = start.saturating_add(max_vertices).min(line.len());
        let window = CoordWindow::from_range(start..end, line.len());
        let shape = Shape::LineString(LineSeq::row_view(line.as_coords(), window));
        let coordinate_count = Some(end.saturating_sub(start));
        let bounds = shape.bounds();
        let traces = if let Some(trace) = trace {
            let mut slices = Vec::with_capacity(end.saturating_sub(start).saturating_sub(1));
            for edge in start..end.saturating_sub(1) {
                work.charge(1)?;
                slices.push((
                    edge,
                    crate::geometry::tessellation::exact::ExactRational::zero(),
                    crate::geometry::tessellation::exact::ExactRational::one(),
                ));
            }
            Some(vec![trace.compose_edges(&slices, work)?])
        } else {
            None
        };
        staged.push(match traces {
            Some(traces) => SubdivisionWorkItem::with_traces(
                shape,
                bounds,
                coordinate_count,
                traces,
                item.vertex_cut,
                item.trace_axes.unwrap_or_else(|| item.shape.axes()),
            ),
            None => SubdivisionWorkItem {
                shape,
                bounds,
                coordinate_count,
                traces: None,
                source_line_members: None,
                trace_axes: None,
                vertex_cut: item.vertex_cut,
            },
        });
        start = end.saturating_sub(1);
    }
    Ok(true)
}

fn stage_line_vertices_with_operation_work(
    item: &SubdivisionWorkItem,
    max_vertices: usize,
    native: &mut NativeSubdivisionContext<'_>,
    staged: &mut Vec<SubdivisionWorkItem>,
) -> Result<bool> {
    if item.traces.is_some()
        && let Some(work) = native.budget.as_mut()
    {
        return stage_line_vertices(item, max_vertices, work, staged);
    }
    let mut work = Unmetered;
    stage_line_vertices(item, max_vertices, &mut work, staged)
}

fn owner_coordinate_count(shape: &Shape, logical_count: Option<usize>) -> usize {
    if let Some(count) = logical_count
        && SharedShapeOwners::for_shape(shape).is_none()
    {
        return count;
    }
    let mut count = 0_usize;
    shape.for_each_first_point_run(|_| {
        count = count.saturating_add(1);
    });
    count
}

fn source_line_member_count(shape: &Shape) -> Result<usize> {
    let mut count = 0_usize;
    shape.try_for_each_component(&mut |component| -> Result<()> {
        if matches!(component, Component::LineString(_)) {
            count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        Ok(())
    })?;
    Ok(count)
}

fn collect_native_line_members(
    shape: &Shape,
    frame: GeographicFrame,
    max_vertices: usize,
    first_member: usize,
) -> Result<Vec<(usize, crate::geometry::CoordinateAxes)>> {
    collect_native_line_members_with_known(shape, frame, max_vertices, first_member, &[])
}

fn collect_native_line_members_with_known(
    shape: &Shape,
    frame: GeographicFrame,
    max_vertices: usize,
    first_member: usize,
    known: &[(usize, crate::geometry::CoordinateAxes)],
) -> Result<Vec<(usize, crate::geometry::CoordinateAxes)>> {
    let mut next_member = first_member;
    let mut output = Vec::new();
    shape.try_for_each_component(&mut |component| -> Result<()> {
        let Component::LineString(line) = component else {
            return Ok(());
        };
        let member = next_member;
        next_member = next_member
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if line.len() <= max_vertices {
            return Ok(());
        }
        if let Ok(index) = known.binary_search_by_key(&member, |&(member, _)| member) {
            output.push((member, known[index].1));
            return Ok(());
        }
        let source = Shape::LineString(line.clone());
        let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(&source))
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        if crate::geometry::geographic_topology_route(inputs.views().iter().copied())
            == crate::geometry::GeographicTopologyRoute::Quotient
        {
            output.push((member, source.axes()));
        }
        Ok(())
    })?;
    Ok(output)
}

struct SubdivisionWorkItem {
    shape: Shape,
    bounds: Option<Bounds>,
    coordinate_count: Option<usize>,
    traces: Option<Vec<SourceTrace>>,
    source_line_members: Option<std::ops::Range<usize>>,
    trace_axes: Option<crate::geometry::CoordinateAxes>,
    vertex_cut: bool,
}

impl SubdivisionWorkItem {
    fn new(
        shape: Shape,
        vertex_cut: bool,
        source_line_members: Option<std::ops::Range<usize>>,
    ) -> Self {
        let bounds = shape.bounds();
        let coordinate_count = ShapeCountQuery::default().coordinate_count(&shape);
        Self {
            shape,
            bounds,
            coordinate_count,
            traces: None,
            source_line_members,
            trace_axes: None,
            vertex_cut,
        }
    }

    const fn with_traces(
        shape: Shape,
        bounds: Option<Bounds>,
        coordinate_count: Option<usize>,
        traces: Vec<SourceTrace>,
        vertex_cut: bool,
        trace_axes: crate::geometry::CoordinateAxes,
    ) -> Self {
        Self {
            shape,
            bounds,
            coordinate_count,
            traces: Some(traces),
            source_line_members: None,
            trace_axes: Some(trace_axes),
            vertex_cut,
        }
    }
}

struct PreparedNativeLine {
    shape: Shape,
    traces: Vec<SourceTrace>,
    trace_axes: crate::geometry::CoordinateAxes,
}

struct TracedLineReconstruction {
    item: usize,
    lines: Vec<LineSeq>,
    row_ranges: Vec<std::ops::Range<usize>>,
    declared_axes: crate::geometry::CoordinateAxes,
}

pub(crate) struct NativeSubdivisionContext<'a> {
    original: &'a Shape,
    frame: Option<GeographicFrame>,
    topology: Option<crate::geometry::ExactGeographicTopology>,
    budget: Option<crate::geometry::embedding_search::EvaluationBudget>,
    prepared: BTreeMap<usize, PreparedNativeLine>,
    skipped: BTreeSet<usize>,
    selected_members: BTreeSet<usize>,
    family_state: Option<FeatureFamilyRegistry>,
    initialized: bool,
}

impl<'a> NativeSubdivisionContext<'a> {
    pub(crate) const fn new(original: &'a Shape, frame: Option<GeographicFrame>) -> Self {
        Self {
            original,
            frame,
            topology: None,
            budget: None,
            prepared: BTreeMap::new(),
            skipped: BTreeSet::new(),
            selected_members: BTreeSet::new(),
            family_state: None,
            initialized: false,
        }
    }

    fn prepare(
        &mut self,
        max_vertices: usize,
        expansion_budget: &mut ExpansionBudget,
        known_candidates: &[(usize, crate::geometry::CoordinateAxes)],
    ) -> Result<()> {
        if self.initialized {
            return Ok(());
        }
        let Some(frame) = self.frame else {
            return Err(GeometryErrorKind::Invalid(
                "Cartesian subdivision cannot prepare native geographic lines".into(),
            )
            .into());
        };
        let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(self.original))
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let selected = collect_native_line_members_with_known(
            self.original,
            frame,
            max_vertices,
            0,
            known_candidates,
        )?;
        let selected_members = selected
            .iter()
            .map(|(member, _)| *member)
            .collect::<BTreeSet<_>>();
        if known_candidates
            .iter()
            .any(|(member, _)| !selected_members.contains(member))
        {
            return Err(GeometryErrorKind::Invalid(
                "the admitted Native root inventory omitted the selected source line".into(),
            )
            .into());
        }
        if selected.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "native subdivision entry has no oversized nonordinary source line".into(),
            )
            .into());
        }
        let selected_axes = selected.iter().copied().collect::<BTreeMap<_, _>>();
        if selected_axes.len() != selected.len() {
            return Err(GeometryErrorKind::Invalid(
                "native source member was selected more than once".into(),
            )
            .into());
        }

        let mut budget = crate::geometry::embedding_search::EvaluationBudget::new("subdivide", 0)?;
        let topology =
            crate::geometry::ExactGeographicTopology::from_views_with_work_and_expansion_budget(
                frame,
                inputs.views().iter().copied(),
                expansion_budget,
                &mut budget,
            )?;

        let mut prepared = BTreeMap::new();
        let members = selected_axes.keys().copied().collect::<Vec<_>>();
        let mut native = crate::geometry::topology::overlay::prepare_native_source_lines::<
            crate::crs::GeodesicMetric,
        >(
            &topology,
            &members,
            crate::geometry::CoordinateAxes::XY,
            false,
            "subdivide",
            None,
            expansion_budget,
            &mut budget,
        )?;
        let family_state = native.family_state.take();
        let prepared_members = Self::collect_prepared_lines(native, &mut prepared, &selected_axes)?;
        if prepared.len() != prepared_members.len() {
            return Err(GeometryErrorKind::Invalid(
                "native source preparation did not retain every noded line member".into(),
            )
            .into());
        }
        let skipped = selected_members
            .difference(&prepared_members)
            .copied()
            .collect();
        self.topology = Some(topology);
        self.budget = Some(budget);
        self.prepared = prepared;
        self.skipped = skipped;
        self.selected_members = selected_members;
        self.family_state = family_state;
        self.initialized = true;
        Ok(())
    }

    fn collect_prepared_lines(
        prepared: PreparedNativeSource,
        output: &mut BTreeMap<usize, PreparedNativeLine>,
        selected_axes: &BTreeMap<usize, crate::geometry::CoordinateAxes>,
    ) -> Result<BTreeSet<usize>> {
        let shape = prepared.shape;
        let source_member_count = prepared.source_members.len();
        let source_members = prepared.source_members.into_iter().collect::<BTreeSet<_>>();
        if source_members.len() != source_member_count
            || source_members
                .iter()
                .any(|member| !selected_axes.contains_key(member))
        {
            return Err(GeometryErrorKind::Invalid(
                "native source plan has duplicated or unselected line members".into(),
            )
            .into());
        }
        let lines = match &shape {
            Shape::LineString(_) => vec![shape.clone()],
            Shape::MultiLineString(_) => shape.parts().collect(),
            Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => Vec::new(),
            _ => {
                return Err(GeometryErrorKind::Invalid(
                    "native source preparation emitted a non-lineal shape".into(),
                )
                .into());
            },
        };
        if lines.len() != prepared.line_traces.len() {
            return Err(GeometryErrorKind::Invalid(
                "native source line traces do not match their literal members".into(),
            )
            .into());
        }
        let mut grouped = BTreeMap::<usize, (Vec<LineSeq>, Vec<SourceTrace>)>::new();
        for (line, traced) in lines.into_iter().zip(prepared.line_traces) {
            if !source_members.contains(&traced.member) {
                return Err(GeometryErrorKind::Invalid(
                    "native line trace references an unprepared source member".into(),
                )
                .into());
            }
            let Shape::LineString(line) = line else {
                return Err(GeometryErrorKind::Invalid(
                    "native source member is not a line string".into(),
                )
                .into());
            };
            let entry = grouped
                .entry(traced.member)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            entry.0.push(line);
            entry.1.push(traced.trace);
        }
        let axes = shape.axes();
        let grouped_len = grouped.len();
        for (member, (lines, traces)) in grouped {
            let line = Shape::narrow_lines(axes, lines);
            let trace_axes = *selected_axes.get(&member).ok_or_else(|| {
                GeometryErrorKind::Invalid("native line has no original axis mask".into())
            })?;
            if output
                .insert(member, PreparedNativeLine {
                    shape: line,
                    traces,
                    trace_axes,
                })
                .is_some()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native source line member was prepared more than once".into(),
                )
                .into());
            }
        }
        if grouped_len != source_members.len() {
            return Err(GeometryErrorKind::Invalid(
                "native source member has no written line trace".into(),
            )
            .into());
        }
        Ok(source_members)
    }

    fn has_prepared_member(&self, members: &std::ops::Range<usize>) -> bool {
        self.prepared.range(members.clone()).next().is_some()
    }

    fn take_prepared_member(&mut self, member: usize) -> Option<PreparedNativeLine> {
        self.prepared.remove(&member)
    }

    fn has_skipped_member(&self, members: &std::ops::Range<usize>) -> bool {
        self.skipped.range(members.clone()).next().is_some()
    }

    fn has_selected_member(&self, members: &std::ops::Range<usize>) -> bool {
        self.selected_members
            .range(members.clone())
            .next()
            .is_some()
    }

    fn finalize<M: crate::geometry::ChartMetric>(
        &mut self,
        staged: &mut [SubdivisionWorkItem],
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<()> {
        if !self.initialized {
            return Ok(());
        }
        let topology = self.topology.as_ref().ok_or_else(|| {
            GeometryErrorKind::Invalid("native subdivision lost its root topology".into())
        })?;
        let budget = self.budget.as_mut().ok_or_else(|| {
            GeometryErrorKind::Invalid("native subdivision lost its operation budget".into())
        })?;
        let zero = crate::geometry::tessellation::exact::ExactRational::zero();
        let one = crate::geometry::tessellation::exact::ExactRational::one();
        let mut request_rows = Vec::new();
        let mut reconstructions = Vec::new();
        for (item_index, item) in staged.iter().enumerate() {
            let Some(traces) = item.traces.as_ref() else {
                continue;
            };
            let declared_axes = item.trace_axes.ok_or_else(|| {
                GeometryErrorKind::Invalid("native source line lost its ordinate axes".into())
            })?;
            let lines = match &item.shape {
                Shape::LineString(line) => vec![line.clone()],
                Shape::MultiLineString(lines) => lines.iter().cloned().collect(),
                Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => {
                    Vec::new()
                },
                _ => {
                    return Err(GeometryErrorKind::Invalid(
                        "native source trace is attached to a non-lineal result".into(),
                    )
                    .into());
                },
            };
            if lines.len() != traces.len() {
                return Err(GeometryErrorKind::Invalid(
                    "native source traces do not match the final line members".into(),
                )
                .into());
            }
            let mut row_ranges = Vec::with_capacity(lines.len());
            for (line, trace) in lines.iter().zip(traces) {
                let edge_count = line
                    .len()
                    .checked_sub(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                if trace.edge_count() != edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "native source trace edge count differs from its final line".into(),
                    )
                    .into());
                }
                budget
                    .charge(u64::try_from(line.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
                let row_start = request_rows.len();
                for vertex in 0..line.len() {
                    let mut row = crate::geometry::ordinates::RequestRowInput {
                        axes: declared_axes,
                        requests: Vec::new(),
                    };
                    if vertex > 0 {
                        let incoming = trace.requests_at(vertex - 1, &one, budget)?;
                        budget.charge(
                            u64::try_from(incoming.len())
                                .map_err(|_| ResourceError::SizeOverflow)?,
                        )?;
                        row.requests.extend(incoming);
                    }
                    if vertex < edge_count {
                        let outgoing = trace.requests_at(vertex, &zero, budget)?;
                        budget.charge(
                            u64::try_from(outgoing.len())
                                .map_err(|_| ResourceError::SizeOverflow)?,
                        )?;
                        row.requests.extend(outgoing);
                    }
                    request_rows.push(row);
                }
                row_ranges.push(row_start..request_rows.len());
            }
            reconstructions.push(TracedLineReconstruction {
                item: item_index,
                lines,
                row_ranges,
                declared_axes,
            });
        }
        if request_rows.is_empty() {
            return Ok(());
        }
        let ordinates = if reconstructions.iter().all(|reconstruction| {
            !reconstruction.declared_axes.has_z() && !reconstruction.declared_axes.has_m()
        }) {
            vec![crate::geometry::ordinates::NodeOrdinates::default(); request_rows.len()]
        } else {
            crate::geometry::ordinates::resolve_request_rows(
                &request_rows,
                topology,
                context.metric_access(),
                budget,
            )?
        };
        if ordinates.len() != request_rows.len() {
            return Err(GeometryErrorKind::Invalid(
                "native root resolution did not return one row per line vertex".into(),
            )
            .into());
        }
        for reconstruction in reconstructions {
            let mut has_z = reconstruction.declared_axes.has_z();
            let mut has_m = reconstruction.declared_axes.has_m();
            for range in &reconstruction.row_ranges {
                for row in ordinates.get(range.clone()).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native line ordinate rows are incomplete".into())
                })? {
                    has_z &= row.z.value().is_some();
                    has_m &= row.m.value().is_some();
                }
            }
            let axes = crate::geometry::CoordinateAxes::new(
                crate::geometry::HasZ(has_z),
                crate::geometry::HasM(has_m),
            );
            if strict && axes != reconstruction.declared_axes {
                return Err(GeometryErrorKind::ordinate_dropped("subdivide"));
            }
            let item = staged.get_mut(reconstruction.item).ok_or_else(|| {
                GeometryErrorKind::Invalid("native output item is unknown".into())
            })?;
            let mut output_lines = Vec::with_capacity(reconstruction.lines.len());
            for (line, range) in reconstruction
                .lines
                .into_iter()
                .zip(reconstruction.row_ranges)
            {
                budget
                    .charge(u64::try_from(line.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
                let mut points = Vec::with_capacity(line.len());
                for (point, ordinate_index) in line.as_coords().iter_coords().zip(range) {
                    let ordinate = *ordinates.get(ordinate_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native line ordinate row is unknown".into())
                    })?;
                    points.push(Point::new_raw_axes(
                        point.x,
                        point.y,
                        crate::geometry::ZOrdinate(
                            axes.has_z().then(|| ordinate.z.value()).flatten(),
                        ),
                        crate::geometry::MOrdinate(
                            axes.has_m().then(|| ordinate.m.value()).flatten(),
                        ),
                    ));
                }
                output_lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
            }
            item.shape = Shape::narrow_lines(axes, output_lines);
            item.bounds = item.shape.bounds();
            item.coordinate_count = ShapeCountQuery::default().coordinate_count(&item.shape);
            item.trace_axes = Some(axes);
            item.traces = None;
        }
        Ok(())
    }

    fn ensure_consumed(&self) -> Result<()> {
        if self.initialized && (self.topology.is_none() || self.budget.is_none()) {
            return Err(GeometryErrorKind::Invalid(
                "native subdivision lost its operation-owned topology or budget".into(),
            )
            .into());
        }
        if self.initialized
            && self
                .selected_members
                .difference(&self.skipped)
                .next()
                .is_some()
            && self.family_state.is_none()
        {
            return Err(GeometryErrorKind::Invalid(
                "native subdivision lost its frozen feature-family history".into(),
            )
            .into());
        }
        if self.prepared.is_empty() {
            return Ok(());
        }
        Err(GeometryErrorKind::Invalid(
            "native source members were prepared but never reached subdivision".into(),
        )
        .into())
    }
}

pub(crate) fn split<M: crate::geometry::ChartMetric>(
    shape: Shape,
    max_vertices: usize,
    drop: bool,
    context: &SourceArcContext<'_, '_, M>,
    out: &mut Vec<Shape>,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let original = shape.clone();
    let frame = match context.domain() {
        crate::geometry::TopologyDomain::Cartesian => None,
        crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
    };
    let mut native = NativeSubdivisionContext::new(&original, frame);
    split_with_context(shape, max_vertices, drop, context, &mut native, out, budget)
}

fn split_with_context<M: crate::geometry::ChartMetric>(
    shape: Shape,
    max_vertices: usize,
    drop: bool,
    context: &SourceArcContext<'_, '_, M>,
    native: &mut NativeSubdivisionContext<'_>,
    out: &mut Vec<Shape>,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let mut work = Vec::with_capacity(1);
    let mut staged = Vec::new();
    let source_line_count = source_line_member_count(&shape)?;
    let item = SubdivisionWorkItem::new(shape, false, Some(0..source_line_count));
    work.push(item);
    while let Some(mut item) = work.pop() {
        #[cfg(test)]
        SUBDIVISION_WORK.with(|work| {
            let (items, splits) = work.get();
            work.set((items.saturating_add(1), splits));
        });
        if item.shape.is_empty() {
            continue;
        }
        let mut item_coordinate_count = item.coordinate_count;
        if item_coordinate_count.is_some_and(|count| count <= max_vertices) {
            staged.push(item);
            continue;
        }
        if max_vertices >= 2
            && item.traces.is_none()
            && let (Some(frame), Some(line_members)) =
                (native.frame, item.source_line_members.clone())
        {
            let line_count = source_line_member_count(&item.shape)?;
            if line_members.len() != line_count {
                return Err(GeometryErrorKind::Invalid(
                    "subdivision source line members do not match the current source shape".into(),
                )
                .into());
            }
            if !native.initialized {
                let candidates = collect_native_line_members(
                    &item.shape,
                    frame,
                    max_vertices,
                    line_members.start,
                )?;
                if !candidates.is_empty() {
                    native.prepare(max_vertices, budget, &candidates)?;
                }
            }
            if native.has_selected_member(&line_members) {
                if matches!(&item.shape, Shape::LineString(_)) {
                    let member = line_members.start;
                    if let Some(prepared) = native.take_prepared_member(member) {
                        item = SubdivisionWorkItem::with_traces(
                            prepared.shape,
                            None,
                            None,
                            prepared.traces,
                            item.vertex_cut,
                            prepared.trace_axes,
                        );
                        item.bounds = item.shape.bounds();
                        item.coordinate_count =
                            ShapeCountQuery::default().coordinate_count(&item.shape);
                        item_coordinate_count = item.coordinate_count;
                    } else if !native.skipped.contains(&member) {
                        return Err(GeometryErrorKind::Invalid(
                            "selected native source line has neither a prepared trace nor a no-span classification"
                                .into(),
                        )
                        .into());
                    }
                } else if native.has_prepared_member(&line_members) {
                    if push_subdivision_members(&mut item, max_vertices, &mut work)? {
                        continue;
                    }
                    return Err(GeometryErrorKind::Invalid(
                        "native source line is not reachable through its member path".into(),
                    )
                    .into());
                } else if !native.has_skipped_member(&line_members) {
                    return Err(GeometryErrorKind::Invalid(
                        "native source line is not reachable through its member path".into(),
                    )
                    .into());
                } else {
                    // Source lines with no Q span stay on the ordinary path.
                }
            }
        }
        if item_coordinate_count.is_some_and(|count| count <= max_vertices) {
            staged.push(item);
            continue;
        }
        if matches!(&item.shape, Shape::MultiPolygon(_))
            && push_subdivision_members(&mut item, max_vertices, &mut work)?
        {
            continue;
        }
        let Some(bounds) = item.bounds else {
            if push_subdivision_members(&mut item, max_vertices, &mut work)? {
                continue;
            }
            if stage_line_vertices_with_operation_work(&item, max_vertices, native, &mut staged)? {
                continue;
            }
            return Err(GeometryErrorKind::unrepresentable_output("subdivide"));
        };
        let owner_coordinate_count = owner_coordinate_count(&item.shape, item_coordinate_count);
        let source_cut = |split_x| {
            if item.vertex_cut {
                if let Some(cut) = bounds_midpoint_cut(bounds, split_x) {
                    return Some(cut);
                }
                match source_vertex_cut(&item.shape, split_x, bounds, owner_coordinate_count) {
                    Some(cut) => Some(cut),
                    None => source_median_cut(&item.shape, split_x, bounds, owner_coordinate_count),
                }
            } else {
                source_median_cut(&item.shape, split_x, bounds, owner_coordinate_count)
            }
        };
        let x_cut = source_cut(true);
        let y_cut = source_cut(false);
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
            if !item.vertex_cut {
                work.push(SubdivisionWorkItem {
                    vertex_cut: true,
                    ..item
                });
                continue;
            }
            if push_subdivision_members(&mut item, max_vertices, &mut work)? {
                continue;
            }
            if stage_line_vertices_with_operation_work(&item, max_vertices, native, &mut staged)? {
                continue;
            }
            return Err(GeometryErrorKind::unrepresentable_output("subdivide"));
        };
        let halves = axis_halves(bounds, split_x, cut);
        let left_rect = Shape::Polygon(rect_polygon(halves[0])?);
        let right_rect = Shape::Polygon(rect_polygon(halves[1])?);
        let sources = [&item.shape, &left_rect, &right_rect];
        let inventory = ExactSourceInventory::from_shapes(&sources)?;
        let mut partition = inventory.partition(budget)?;
        if let Some(frame) = native.frame {
            partition.set_geographic_frame(frame);
        }
        let trace_rooted = item.traces.is_some() && native.topology.is_some();
        let children = {
            if trace_rooted {
                let trace_work = native.budget.as_mut().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native traced subdivision has no operation budget".into(),
                    )
                })?;
                inventory.subdivision_parts_with_budget(
                    &partition,
                    &item.shape,
                    item.traces.as_deref(),
                    crate::geometry::CoordinateAxes::XY,
                    false,
                    [1, 2],
                    budget,
                    context,
                    trace_work,
                )?
            } else {
                let [left, right] = inventory.subdivision_parts(
                    &partition,
                    &item.shape,
                    item.shape.axes(),
                    drop,
                    [1, 2],
                    budget,
                    context,
                )?;
                [
                    SubdivisionPart {
                        shape: left,
                        traces: None,
                    },
                    SubdivisionPart {
                        shape: right,
                        traces: None,
                    },
                ]
            }
        };
        if children.iter().all(|child| child.shape.is_empty()) {
            // A cut that cannot be represented by the exact arrangement must
            // not silently lose the source support or violate the size bound.
            if !item.vertex_cut {
                work.push(SubdivisionWorkItem {
                    vertex_cut: true,
                    ..item
                });
                continue;
            }
            if stage_line_vertices_with_operation_work(&item, max_vertices, native, &mut staged)? {
                continue;
            }
            if push_subdivision_members(&mut item, max_vertices, &mut work)? {
                continue;
            }
            return Err(GeometryErrorKind::unrepresentable_output("subdivide"));
        }
        let child_coordinate_counts = {
            let mut counts = ShapeCountQuery::default();
            let [left, right] = &children;
            [left, right].map(|child| {
                (!child.shape.is_empty()).then(|| counts.coordinate_count(&child.shape))
            })
        };
        #[cfg(test)]
        let mut progressed = false;
        for (child, child_coordinate_count) in
            children.into_iter().zip(child_coordinate_counts).rev()
        {
            let Some(child_coordinate_count) = child_coordinate_count else {
                continue;
            };
            let child_bounds = child.shape.bounds();
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
                let source_complexity_progress =
                    match (item_coordinate_count, child_coordinate_count) {
                        (None, Some(_)) => true,
                        (Some(parent), Some(child)) => child < parent,
                        (None | Some(_), None) => false,
                    };
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
            work.push(SubdivisionWorkItem {
                shape: child.shape,
                bounds: child_bounds,
                coordinate_count: child_coordinate_count,
                traces: child.traces,
                source_line_members: None,
                trace_axes: item.trace_axes,
                vertex_cut: !progress,
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
    native.finalize(&mut staged, drop, context)?;
    native.ensure_consumed()?;
    out.reserve(staged.len());
    out.extend(staged.into_iter().map(|item| item.shape));
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
    if kept == points.len() {
        return Ok(None);
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
            MultiMember::Value(line) => match kept_coords(line, scratch, keep_mask)? {
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
) -> Result<RawSimplify> {
    let mut out: Option<Vec<MultiMember<Polygon>>> = None;
    for (index, member) in polygons.members_with_values().enumerate() {
        match member {
            MultiMember::Empty => {
                if let Some(members) = out.as_mut() {
                    members.push(MultiMember::Empty);
                }
            },
            MultiMember::Value(polygon) => match keep_polygon(polygon, scratch, keep_mask)? {
                PolygonKeep::Same => {
                    if let Some(members) = out.as_mut() {
                        members.push(MultiMember::Value(polygon.clone()));
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
                },
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
        self.map_chains(&mut |points: &CoordSeq| {
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
        let parts = shape
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(partition_calls(), 1);
        assert_eq!(subdivision_work(), (3, 1));
        let mut counts = ShapeCountQuery::default();
        assert!(
            parts
                .iter()
                .all(|part| { counts.coordinates(part).is_ok_and(|count| count <= 8) })
        );
    }

    #[test]
    fn geographic_subdivide_splits_a_wide_computed_cut_edge() {
        let shape = ring_shape(&[
            Point::new_raw_xy(-150.0, -500.0),
            Point::new_raw_xy(-50.0, -500.0),
            Point::new_raw_xy(50.0, -500.0),
            Point::new_raw_xy(150.0, -500.0),
            Point::new_raw_xy(150.0, 0.0),
            Point::new_raw_xy(150.0, 500.0),
            Point::new_raw_xy(50.0, 500.0),
            Point::new_raw_xy(-50.0, 500.0),
            Point::new_raw_xy(-150.0, 500.0),
            Point::new_raw_xy(-150.0, 0.0),
            Point::new_raw_xy(-150.0, -500.0),
        ]);
        let longitude =
            crate::geographic::AngularFactor::try_new("longitude", std::f64::consts::PI / 180.0)
                .unwrap();
        let latitude =
            crate::geographic::AngularFactor::try_new("latitude", std::f64::consts::PI / 2000.0)
                .unwrap();
        let frame = crate::geographic::GeographicFrame::try_new(longitude, latitude, 0.0).unwrap();
        let no_metric = || -> Result<&crate::crs::GeodesicMetric> {
            Err(crate::crs::CrsError::invalid(
                "unexpected source-arc metric access",
            ))
        };
        let parts = shape
            .subdivide_with_context(
                8,
                false,
                &crate::geometry::SourceArcContext::new(
                    crate::geometry::TopologyDomain::Geographic(frame),
                    Some(&no_metric),
                ),
            )
            .unwrap();
        assert!(parts.len() >= 2);
        let mut counts = ShapeCountQuery::default();
        assert!(
            parts
                .iter()
                .all(|part| counts.coordinates(part).is_ok_and(|count| count <= 8))
        );
        assert!(
            parts
                .iter()
                .flat_map(all_points)
                .any(|point| point == Point::new_raw_xy(0.0, -250.0)),
            "the 300-degree generated cut edge needs its canonical midpoint: {parts:?}"
        );
        for point in all_points(&shape) {
            assert!(
                parts
                    .iter()
                    .flat_map(all_points)
                    .any(|output| output == point),
                "subdivision lost source vertex {point:?}: {parts:?}"
            );
        }
    }

    #[test]
    fn recursive_subdivision_keeps_each_child_count_with_its_shape() {
        let mut points = (0..32)
            .map(|index| {
                let angle = f64::from(index) * std::f64::consts::TAU / 32.0;
                Point::new_raw_xy(angle.cos(), angle.sin())
            })
            .collect::<Vec<_>>();
        points.push(points[0]);
        let shape = ring_shape(&points);

        reset_subdivision_work();
        let parts = shape
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();

        let (work_items, splits) = subdivision_work();
        assert!(work_items > 3);
        assert!(splits > 1);
        let mut counts = ShapeCountQuery::default();
        assert!(
            parts
                .iter()
                .all(|part| counts.coordinates(part).is_ok_and(|count| count <= 8))
        );
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
        let parts = shape
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(partition_calls(), 1);
        assert!(parts.iter().all(|part| !part.is_empty()));
    }

    #[test]
    fn adjacent_sub_ulp_line_uses_coordinate_sequence_cuts() {
        let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
        let points = (0..9)
            .map(|index| Point::new_raw_xy(if index % 2 == 0 { 1.0 } else { adjacent }, 0.0))
            .collect::<Vec<_>>();
        let shape = line_shape(&points);
        reset_partition_calls();
        let parts = shape
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
        assert_eq!(parts.len(), 2);
        let mut counts = ShapeCountQuery::default();
        assert!(parts.iter().all(|part| {
            counts
                .coordinate_count(part)
                .is_some_and(|count| count <= 8)
        }));
        assert_eq!(partition_calls(), 0);
    }

    #[test]
    fn source_ordinate_dedup_merges_signed_zero_and_keeps_adjacent_values() {
        let adjacent = f64::from_bits(1.0_f64.to_bits() + 1);
        let shape = line_shape(&[
            Point::new_raw_xy(-0.0, 0.0),
            Point::new_raw_xy(-30.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(-10.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(adjacent, 0.0),
        ]);

        let ordinates = source_ordinates(&shape, true, 6);

        assert_eq!(ordinates.len(), 5);
        assert_eq!(ordinates[0].to_bits(), (-30.0_f64).to_bits());
        assert_eq!(ordinates[1].to_bits(), (-10.0_f64).to_bits());
        assert_eq!(ordinates[2].abs().to_bits(), 0.0_f64.to_bits());
        assert_eq!(ordinates[3].to_bits(), 1.0_f64.to_bits());
        assert_eq!(ordinates[4].to_bits(), adjacent.to_bits());
    }

    #[test]
    fn over_budget_clipped_multipart_children_are_revisited_by_member() {
        let shape =
            crate::io::parse_wkt("POLYGON((0 0,20 0,20 20,0 20,0 0),(0 0,5 2,2 5,0 0))").unwrap();
        let parts = shape
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
        let mut counts = ShapeCountQuery::default();
        assert!(parts.iter().all(|part| {
            counts
                .coordinate_count(part)
                .is_some_and(|count| count <= 8)
        }));
    }

    #[test]
    fn over_budget_multipolygon_is_separated_before_spatial_partitioning() {
        let shape = crate::io::parse_wkt(
            "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((10 10,20 10,20 20,10 20,10 10)))",
        )
        .unwrap();
        reset_partition_calls();
        let parts = shape
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().all(|part| matches!(part, Shape::Polygon(_))));
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
        let parts = shape
            .subdivide_with_context(7, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
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
        let parts = shape
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
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
            let parts = shape
                .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
                .unwrap();
            assert!(!parts.is_empty(), "{wkt}");
            assert!(parts.iter().all(|part| !part.is_empty()), "{wkt}");
        }
    }

    #[test]
    fn subdivision_uses_distinct_ordinates_for_mixed_shared_multiplicity() {
        let rectangle = |min_x, max_x| {
            ring_shape(&[
                Point::new_raw_xy(min_x, 0.0),
                Point::new_raw_xy(max_x, 0.0),
                Point::new_raw_xy(max_x, 1.0),
                Point::new_raw_xy(min_x, 1.0),
                Point::new_raw_xy(min_x, 0.0),
            ])
        };
        let first = Shape::geometry_collection(vec![rectangle(0.0, 2.0)]).unwrap();
        let nested =
            Shape::geometry_collection(vec![first.clone(), rectangle(10.0, 12.0)]).unwrap();
        let shared = Shape::geometry_collection(vec![first, nested]).unwrap();
        let expanded = crate::io::parse_wkb(
            &crate::io::to_wkb(&shared, None, false).expect("shared input serializes"),
        )
        .expect("unshared expansion parses")
        .shape;

        let shared_parts = shared
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
        let expanded_parts = expanded
            .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
            .unwrap();
        let wkb = |parts: &[Shape]| {
            parts
                .iter()
                .map(|part| crate::io::to_wkb(part, None, false).unwrap())
                .collect::<Vec<_>>()
        };
        let shared_parts_wkb = wkb(&shared_parts);
        assert_eq!(shared_parts_wkb, wkb(&expanded_parts));
        assert_eq!(
            shared_parts_wkb,
            wkb(&shared
                .subdivide_with_context(8, false, &crate::geometry::SourceArcContext::cartesian())
                .unwrap())
        );

        let mut counts = ShapeCountQuery::default();
        assert!(shared_parts.iter().all(|part| {
            counts
                .coordinate_count(part)
                .is_some_and(|count| count <= 8)
        }));
        let source_union = Shape::union_all_topo(
            &[&shared],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::Strictness::Lenient,
        )
        .unwrap();
        let parts_union = Shape::union_all_topo(
            &shared_parts,
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::Strictness::Lenient,
        )
        .unwrap();
        assert_eq!(
            crate::io::to_wkb(&source_union, None, false).unwrap(),
            crate::io::to_wkb(&parts_union, None, false).unwrap()
        );
    }
}
