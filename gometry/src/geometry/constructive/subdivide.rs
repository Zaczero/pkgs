//! Recursively split shapes while carrying source traces through each cut.

#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
#[cfg(test)]
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::crs::GeodesicMetric;
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, GeographicInputs};
use crate::geometry::embedding_search::EvaluationBudget;
use crate::geometry::ordinates::{NodeOrdinates, RequestRowInput, resolve_request_rows};
use crate::geometry::overlay::{
    ExactSourceInventory, SubdivisionPart, SubdivisionRootCatalog, SubdivisionTraceRequest,
    rect_polygon,
};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::topology::overlay::prepare_native_source_lines;
use crate::geometry::topology::{FeatureFamilyRegistry, PreparedNativeSource, SourceTrace};
use crate::geometry::{
    Bounds, Component, CoordSeq, CoordWindow, Coordinates as _, ExpansionBudget, GeometryErrorKind,
    GeometryKind, LineSeq, Point, Shape, ShapeCountQuery, ShapeOwnerKey, SharedShapeOwners,
    SourceArcContext, Unmetered, Work,
};

#[cfg(test)]
thread_local! {
    static SUBDIVISION_WORK: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    static SOURCE_LINE_MEMBER_COUNT_CALLS: Cell<usize> = const { Cell::new(0) };
    static SUBDIVISION_LINE_MEMBER_RANGES: RefCell<Vec<(u64, usize, usize)>> = const { RefCell::new(Vec::new()) };
    static SOURCE_ORDINATE_AXIS_SCANS: Cell<usize> = const { Cell::new(0) };
    static ORDINARY_ROOT_CATALOG_BUILDS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn reset_subdivision_work() {
    SUBDIVISION_WORK.with(|work| work.set((0, 0)));
}

#[cfg(test)]
fn reset_source_line_member_count_calls() {
    SOURCE_LINE_MEMBER_COUNT_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn source_line_member_count_calls() -> usize {
    SOURCE_LINE_MEMBER_COUNT_CALLS.with(Cell::get)
}

#[cfg(test)]
fn reset_subdivision_line_member_ranges() {
    SUBDIVISION_LINE_MEMBER_RANGES.with(|ranges| ranges.borrow_mut().clear());
}

#[cfg(test)]
fn subdivision_line_member_ranges() -> Vec<(u64, usize, usize)> {
    SUBDIVISION_LINE_MEMBER_RANGES.with(|ranges| ranges.borrow().clone())
}

#[cfg(test)]
fn subdivision_work() -> (usize, usize) {
    SUBDIVISION_WORK.with(Cell::get)
}

#[cfg(test)]
fn reset_source_ordinate_axis_scans() {
    SOURCE_ORDINATE_AXIS_SCANS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn source_ordinate_axis_scans() -> usize {
    SOURCE_ORDINATE_AXIS_SCANS.with(Cell::get)
}

#[cfg(test)]
fn reset_ordinary_root_catalog_builds() {
    ORDINARY_ROOT_CATALOG_BUILDS.with(|builds| builds.set(0));
}

#[cfg(test)]
fn ordinary_root_catalog_builds() -> usize {
    ORDINARY_ROOT_CATALOG_BUILDS.with(Cell::get)
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
fn source_median_cut<W: Work>(
    shape: &Shape,
    split_x: bool,
    bounds: Bounds,
    owner_coordinate_count: usize,
    work: &mut W,
) -> Result<Option<f64>> {
    let ordinates = source_ordinates(shape, split_x, owner_coordinate_count, work)?;
    let middle = ordinates.len() / 2;
    let (low, high) = if split_x {
        (bounds.minx(), bounds.maxx())
    } else {
        (bounds.miny(), bounds.maxy())
    };
    let mut best: Option<(usize, usize, f64)> = None;
    for (lower_index, pair) in ordinates.windows(2).enumerate() {
        work.charge_into_error(1)?;
        let upper_index = lower_index.wrapping_add(1);
        let lower = *pair.first().expect("median window has a lower ordinate");
        let upper = *pair.last().expect("median window has an upper ordinate");
        let cut = f64::midpoint(lower, upper);
        if low < cut && cut < high {
            let candidate = (upper_index.abs_diff(middle), upper_index, cut);
            if best
                .is_none_or(|(distance, index, _)| (candidate.0, candidate.1) < (distance, index))
            {
                best = Some(candidate);
            }
        }
    }
    Ok(best.map(|(_, _, cut)| cut))
}

fn source_vertex_cut<W: Work>(
    shape: &Shape,
    split_x: bool,
    bounds: Bounds,
    owner_coordinate_count: usize,
    work: &mut W,
) -> Result<Option<f64>> {
    let ordinates = source_ordinates(shape, split_x, owner_coordinate_count, work)?;
    let middle = ordinates.len() / 2;
    let (low, high) = if split_x {
        (bounds.minx(), bounds.maxx())
    } else {
        (bounds.miny(), bounds.maxy())
    };
    let mut best: Option<(usize, usize, f64)> = None;
    for (index, &ordinate) in ordinates.iter().enumerate() {
        work.charge_into_error(1)?;
        if low < ordinate && ordinate < high {
            let candidate = (index.abs_diff(middle), index, ordinate);
            if best.is_none_or(|(distance, best_index, _)| {
                (candidate.0, candidate.1) < (distance, best_index)
            }) {
                best = Some(candidate);
            }
        }
    }
    Ok(best.map(|(_, _, cut)| cut))
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

fn source_cut<W: Work>(
    shape: &Shape,
    split_x: bool,
    bounds: Bounds,
    owner_coordinate_count: usize,
    vertex_cut: bool,
    work: &mut W,
) -> Result<Option<f64>> {
    if vertex_cut {
        if let Some(cut) = bounds_midpoint_cut(bounds, split_x) {
            return Ok(Some(cut));
        }
        source_vertex_cut(shape, split_x, bounds, owner_coordinate_count, work)?.map_or_else(
            || source_median_cut(shape, split_x, bounds, owner_coordinate_count, work),
            |cut| Ok(Some(cut)),
        )
    } else {
        source_median_cut(shape, split_x, bounds, owner_coordinate_count, work)
    }
}

fn preferred_source_cut<W: Work>(
    shape: &Shape,
    bounds: Bounds,
    owner_coordinate_count: usize,
    vertex_cut: bool,
    work: &mut W,
) -> Result<Option<(bool, f64)>> {
    let x_longer = (bounds.maxx() - bounds.minx()) >= (bounds.maxy() - bounds.miny());
    let first = source_cut(
        shape,
        x_longer,
        bounds,
        owner_coordinate_count,
        vertex_cut,
        work,
    )?;
    if let Some(cut) = first {
        return Ok(Some((x_longer, cut)));
    }
    let second = source_cut(
        shape,
        !x_longer,
        bounds,
        owner_coordinate_count,
        vertex_cut,
        work,
    )?;
    Ok(second.map(|cut| (!x_longer, cut)))
}

fn source_ordinates<W: Work>(
    shape: &Shape,
    split_x: bool,
    owner_coordinate_count: usize,
    work: &mut W,
) -> Result<Vec<f64>> {
    let count = u64::try_from(owner_coordinate_count).map_err(|_| ResourceError::SizeOverflow)?;
    work.charge_into_error(count)?;
    work.charge_into_error(count)?;
    #[cfg(test)]
    SOURCE_ORDINATE_AXIS_SCANS.with(|calls| calls.set(calls.get().saturating_add(1)));
    let mut ordinates = Vec::with_capacity(owner_coordinate_count);
    shape.for_each_first_point_run(|point| {
        ordinates.push(if split_x { point.x } else { point.y });
    });
    if ordinates.len() != owner_coordinate_count {
        return Err(GeometryErrorKind::Invalid(
            "subdivision source ordinate count does not match its admitted owner count".into(),
        )
        .into());
    }
    try_sort_by_with_work(&mut ordinates, work, |left, right, _work| {
        Ok(left.total_cmp(right))
    })?;
    let mut unique = 0_usize;
    for index in 0..ordinates.len() {
        work.charge_into_error(1)?;
        let ordinate = *ordinates.get(index).ok_or(ResourceError::SizeOverflow)?;
        #[expect(
            clippy::float_cmp,
            reason = "source ordinates are finite and exact equality merges signed zero but preserves adjacent vertices"
        )]
        let duplicate = if unique == 0 {
            false
        } else {
            *ordinates
                .get(unique.wrapping_sub(1))
                .ok_or(ResourceError::SizeOverflow)?
                == ordinate
        };
        if !duplicate {
            if unique != index {
                *ordinates
                    .get_mut(unique)
                    .ok_or(ResourceError::SizeOverflow)? = ordinate;
            }
            unique = unique.wrapping_add(1);
        }
    }
    ordinates.truncate(unique);
    Ok(ordinates)
}

fn push_subdivision_members(
    item: &mut SubdivisionWorkItem,
    max_vertices: usize,
    work: &mut Vec<SubdivisionWorkItem>,
    line_member_counts: &mut SourceLineMemberCountMemo,
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
    if let Some(polygon_traces) = item.polygon_traces.take() {
        if !matches!(shape, Shape::MultiPolygon(_)) {
            return Err(GeometryErrorKind::Invalid(
                "polygon ring traces were attached to a non-multipart polygon shape".into(),
            )
            .into());
        }
        let mut traces = polygon_traces.into_iter();
        let mut member_traces = Vec::with_capacity(members.len());
        for member in &members {
            let Shape::Polygon(polygon) = member else {
                return Err(GeometryErrorKind::Invalid(
                    "multipolygon member is not a polygon".into(),
                )
                .into());
            };
            let ring_count = polygon.rings().count();
            let ring_traces = traces.by_ref().take(ring_count).collect::<Vec<_>>();
            if ring_traces.len() != ring_count {
                return Err(GeometryErrorKind::Invalid(
                    "polygon ring traces do not match multipart members".into(),
                )
                .into());
            }
            member_traces.push(ring_traces);
        }
        if traces.next().is_some() {
            return Err(GeometryErrorKind::Invalid(
                "polygon ring traces contain an unassigned multipart member".into(),
            )
            .into());
        }
        for (member, polygon_traces) in members.into_iter().zip(member_traces).rev() {
            work.push(SubdivisionWorkItem::with_polygon_traces(
                member,
                polygon_traces,
                item.vertex_cut,
                item.trace_axes.unwrap_or_else(|| item.shape.axes()),
            ));
        }
        return Ok(true);
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
        let member_line_ranges = if let Some(parent_range) = source_line_members {
            if parent_range.len() == source_line_member_count(shape, line_member_counts)? {
                let mut next = parent_range.start;
                let mut ranges = Vec::with_capacity(members.len());
                for member in &members {
                    let count = source_line_member_count(member, line_member_counts)?;
                    let end = next.checked_add(count).ok_or(ResourceError::SizeOverflow)?;
                    ranges.push(next..end);
                    next = end;
                }
                Some(ranges)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(member_line_ranges) = member_line_ranges {
            for (member, source_line_members) in members.into_iter().zip(member_line_ranges).rev() {
                work.push(SubdivisionWorkItem::new(
                    member,
                    vertex_cut,
                    Some(source_line_members),
                ));
            }
        } else {
            for member in members.into_iter().rev() {
                work.push(SubdivisionWorkItem::new(member, vertex_cut, None));
            }
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
                work.charge_into_error(1)?;
                slices.push((edge, ExactRational::zero(), ExactRational::one()));
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
                polygon_traces: None,
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

#[derive(Default)]
struct SourceLineMemberCountMemo {
    collections: HashMap<ShapeOwnerKey, usize>,
}

fn source_line_member_count(shape: &Shape, memo: &mut SourceLineMemberCountMemo) -> Result<usize> {
    #[cfg(test)]
    SOURCE_LINE_MEMBER_COUNT_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    if let Shape::GeometryCollection(collection) = shape {
        let owner = collection
            .members_are_shared()
            .then(|| ShapeOwnerKey::new(collection.as_slice()));
        if let Some(count) = owner.and_then(|owner| memo.collections.get(&owner).copied()) {
            return Ok(count);
        }
        let mut count = 0_usize;
        for member in collection.as_slice() {
            count = count
                .checked_add(source_line_member_count(member, memo)?)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        if let Some(owner) = owner {
            memo.collections.insert(owner, count);
        }
        return Ok(count);
    }
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
    line_member_counts: &mut SourceLineMemberCountMemo,
) -> Result<Vec<(usize, crate::geometry::CoordinateAxes)>> {
    collect_native_line_members_with_known(
        shape,
        frame,
        max_vertices,
        first_member,
        &[],
        line_member_counts,
    )
}

fn collect_native_line_members_with_known(
    shape: &Shape,
    frame: GeographicFrame,
    max_vertices: usize,
    first_member: usize,
    known: &[(usize, crate::geometry::CoordinateAxes)],
    line_member_counts: &mut SourceLineMemberCountMemo,
) -> Result<Vec<(usize, crate::geometry::CoordinateAxes)>> {
    let mut next_member = first_member;
    let mut output = Vec::new();
    collect_native_line_members_inner(
        shape,
        frame,
        max_vertices,
        known,
        line_member_counts,
        &mut next_member,
        &mut output,
    )?;
    Ok(output)
}

fn collect_native_line_members_inner(
    shape: &Shape,
    frame: GeographicFrame,
    max_vertices: usize,
    known: &[(usize, crate::geometry::CoordinateAxes)],
    line_member_counts: &mut SourceLineMemberCountMemo,
    next_member: &mut usize,
    output: &mut Vec<(usize, crate::geometry::CoordinateAxes)>,
) -> Result<()> {
    if let Shape::GeometryCollection(collection) = shape {
        if collection.members_are_shared() {
            let line_count = source_line_member_count(shape, line_member_counts)?;
            if line_count == 0 {
                *next_member = (*next_member)
                    .checked_add(line_count)
                    .ok_or(ResourceError::SizeOverflow)?;
                return Ok(());
            }
        }
        for member in collection.as_slice() {
            collect_native_line_members_inner(
                member,
                frame,
                max_vertices,
                known,
                line_member_counts,
                next_member,
                output,
            )?;
        }
        return Ok(());
    }
    shape.try_for_each_component(&mut |component| -> Result<()> {
        let Component::LineString(line) = component else {
            return Ok(());
        };
        let member = *next_member;
        *next_member = (*next_member)
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if line.len() <= max_vertices {
            return Ok(());
        }
        if let Ok(index) = known.binary_search_by_key(&member, |&(member, _)| member) {
            let (_, axes) = known.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("known native line member is missing".into())
            })?;
            output.push((member, *axes));
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
    Ok(())
}

struct SubdivisionWorkItem {
    shape: Shape,
    bounds: Option<Bounds>,
    coordinate_count: Option<usize>,
    traces: Option<Vec<SourceTrace>>,
    polygon_traces: Option<Vec<SourceTrace>>,
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
            polygon_traces: None,
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
            polygon_traces: None,
            source_line_members: None,
            trace_axes: Some(trace_axes),
            vertex_cut,
        }
    }

    fn with_polygon_traces(
        shape: Shape,
        polygon_traces: Vec<SourceTrace>,
        vertex_cut: bool,
        trace_axes: crate::geometry::CoordinateAxes,
    ) -> Self {
        let bounds = shape.bounds();
        let coordinate_count = ShapeCountQuery::default().coordinate_count(&shape);
        Self {
            shape,
            bounds,
            coordinate_count,
            traces: None,
            polygon_traces: Some(polygon_traces),
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

fn traced_output_axes(
    declared_axes: crate::geometry::CoordinateAxes,
    row_ranges: &[std::ops::Range<usize>],
    ordinates: &[NodeOrdinates],
    strict: bool,
) -> Result<crate::geometry::CoordinateAxes> {
    let mut has_z = declared_axes.has_z();
    let mut has_m = declared_axes.has_m();
    for range in row_ranges {
        for row in ordinates.get(range.clone()).ok_or_else(|| {
            GeometryErrorKind::Invalid("traced output ordinate rows are incomplete".into())
        })? {
            let z = row.z.value().is_some();
            let m = row.m.value().is_some();
            has_z &= z;
            has_m &= m;
        }
    }
    let axes = crate::geometry::CoordinateAxes::new(
        crate::geometry::HasZ(has_z),
        crate::geometry::HasM(has_m),
    );
    if strict && axes != declared_axes {
        return Err(GeometryErrorKind::ordinate_dropped("subdivide"));
    }
    Ok(axes)
}

fn point_with_traced_ordinates(
    point: Point,
    ordinate: NodeOrdinates,
    axes: crate::geometry::CoordinateAxes,
) -> Point {
    Point::new_raw_axes(
        point.x,
        point.y,
        crate::geometry::ZOrdinate(axes.has_z().then(|| ordinate.z.value()).flatten()),
        crate::geometry::MOrdinate(axes.has_m().then(|| ordinate.m.value()).flatten()),
    )
}

#[derive(Clone, Copy)]
enum SubdivisionPath {
    Ordinary,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Task 430/R11 keeps NativePrepared test-only until recursive family projection is supplied"
        )
    )]
    NativePrepared,
}

struct NativeSubdivisionContext<'a> {
    original: &'a Shape,
    frame: Option<GeographicFrame>,
    path: SubdivisionPath,
    topology: Option<crate::geometry::ExactGeographicTopology>,
    budget: Option<EvaluationBudget>,
    prepared: BTreeMap<usize, PreparedNativeLine>,
    skipped: BTreeSet<usize>,
    selected_members: BTreeSet<usize>,
    family_state: Option<FeatureFamilyRegistry>,
    ordinary_roots: Option<SubdivisionRootCatalog>,
    ordinary_line_traces: Option<Vec<SourceTrace>>,
    ordinary_polygon_traces: Option<Vec<SourceTrace>>,
    initialized: bool,
}

impl<'a> NativeSubdivisionContext<'a> {
    const fn new(
        original: &'a Shape,
        frame: Option<GeographicFrame>,
        path: SubdivisionPath,
    ) -> Self {
        Self {
            original,
            frame,
            path,
            topology: None,
            budget: None,
            prepared: BTreeMap::new(),
            skipped: BTreeSet::new(),
            selected_members: BTreeSet::new(),
            family_state: None,
            ordinary_roots: None,
            ordinary_line_traces: None,
            ordinary_polygon_traces: None,
            initialized: false,
        }
    }

    fn prepare(
        &mut self,
        max_vertices: usize,
        expansion_budget: &mut ExpansionBudget,
        known_candidates: &[(usize, crate::geometry::CoordinateAxes)],
        line_member_counts: &mut SourceLineMemberCountMemo,
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
            line_member_counts,
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

        let mut budget = EvaluationBudget::new("subdivide", 0)?;
        let topology =
            crate::geometry::ExactGeographicTopology::from_views_with_work_and_expansion_budget(
                frame,
                inputs.views().iter().copied(),
                expansion_budget,
                &mut budget,
            )?;

        let mut prepared = BTreeMap::new();
        let members = selected_axes.keys().copied().collect::<Vec<_>>();
        let mut native = prepare_native_source_lines::<GeodesicMetric>(
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
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => {
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
        let zero = ExactRational::zero();
        let one = ExactRational::one();
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
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => {
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
                budget.charge_into_error(
                    u64::try_from(line.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let row_start = request_rows.len();
                for vertex in 0..line.len() {
                    let mut row = RequestRowInput {
                        axes: declared_axes,
                        requests: Vec::new(),
                    };
                    if vertex > 0 {
                        let previous_vertex =
                            vertex.checked_sub(1).ok_or(ResourceError::SizeOverflow)?;
                        let incoming = trace.requests_at(previous_vertex, &one, budget)?;
                        budget.charge_into_error(
                            u64::try_from(incoming.len())
                                .map_err(|_| ResourceError::SizeOverflow)?,
                        )?;
                        row.requests.extend(incoming);
                    }
                    if vertex < edge_count {
                        let outgoing = trace.requests_at(vertex, &zero, budget)?;
                        budget.charge_into_error(
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
            vec![NodeOrdinates::default(); request_rows.len()]
        } else {
            resolve_request_rows(&request_rows, topology, context.metric_access(), budget)?
        };
        if ordinates.len() != request_rows.len() {
            return Err(GeometryErrorKind::Invalid(
                "native root resolution did not return one row per line vertex".into(),
            )
            .into());
        }
        for reconstruction in reconstructions {
            let axes = traced_output_axes(
                reconstruction.declared_axes,
                &reconstruction.row_ranges,
                &ordinates,
                strict,
            )?;
            let item = staged.get_mut(reconstruction.item).ok_or_else(|| {
                GeometryErrorKind::Invalid("native output item is unknown".into())
            })?;
            let mut output_lines = Vec::with_capacity(reconstruction.lines.len());
            for (line, range) in reconstruction
                .lines
                .into_iter()
                .zip(reconstruction.row_ranges)
            {
                budget.charge_into_error(
                    u64::try_from(line.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut points = Vec::with_capacity(line.len());
                for (point, ordinate_index) in line.as_coords().iter_coords().zip(range) {
                    let ordinate = *ordinates.get(ordinate_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native line ordinate row is unknown".into())
                    })?;
                    points.push(point_with_traced_ordinates(point, ordinate, axes));
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
    // Recursive Native children need a producer-to-family projection that
    // this public subdivision entry point does not carry.
    let mut native = NativeSubdivisionContext::new(&original, frame, SubdivisionPath::Ordinary);
    let mut line_member_counts = SourceLineMemberCountMemo::default();
    let mut item = SubdivisionWorkItem::new(shape, false, None);
    if let Some(frame) = frame
        && item
            .coordinate_count
            .is_some_and(|count| count > max_vertices)
        && matches!(
            &item.shape,
            Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
        )
    {
        // Area output XY must stay attached to its clip topology; only line
        // support needs source-chart XY relocation.
        let root_xy_positions_needed =
            matches!(
                &item.shape,
                Shape::LineString(_) | Shape::MultiLineString(_)
            ) && SubdivisionRootCatalog::needs_root_xy_positions(&item.shape, item.bounds, frame);
        let axes = item.shape.axes();
        if axes.has_z() || axes.has_m() || root_xy_positions_needed {
            #[cfg(test)]
            ORDINARY_ROOT_CATALOG_BUILDS.with(|builds| {
                builds.set(builds.get().saturating_add(1));
            });
            match &original {
                Shape::LineString(_) | Shape::MultiLineString(_) => {
                    let (roots, traces) = SubdivisionRootCatalog::from_line_shape(
                        &original,
                        frame,
                        root_xy_positions_needed,
                    )?;
                    native.ordinary_roots = Some(roots);
                    native.ordinary_line_traces = Some(traces);
                },
                Shape::Polygon(_) | Shape::MultiPolygon(_) => {
                    let (roots, traces) = SubdivisionRootCatalog::from_polygon_shape(
                        &original,
                        frame,
                        root_xy_positions_needed,
                    )?;
                    native.ordinary_roots = Some(roots);
                    native.ordinary_polygon_traces = Some(traces);
                },
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => {},
            }
        }
    }
    item.source_line_members = if frame.is_some() {
        Some(0..source_line_member_count(&item.shape, &mut line_member_counts)?)
    } else {
        None
    };
    if native.ordinary_roots.is_some()
        && matches!(
            &item.shape,
            Shape::LineString(_) | Shape::MultiLineString(_)
        )
    {
        item.traces = Some(native.ordinary_line_traces.take().ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "ordinary subdivision root catalog lost its source line traces".into(),
            )
        })?);
        item.trace_axes = Some(item.shape.axes());
    } else if native.ordinary_roots.is_some()
        && matches!(&item.shape, Shape::Polygon(_) | Shape::MultiPolygon(_))
    {
        item.polygon_traces = Some(native.ordinary_polygon_traces.take().ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "ordinary subdivision root catalog lost its polygon ring traces".into(),
            )
        })?);
        item.trace_axes = Some(item.shape.axes());
    }
    split_with_context(
        item,
        max_vertices,
        drop,
        context,
        &mut native,
        &mut line_member_counts,
        out,
        budget,
    )
}

fn split_with_context<M: crate::geometry::ChartMetric>(
    item: SubdivisionWorkItem,
    max_vertices: usize,
    drop: bool,
    context: &SourceArcContext<'_, '_, M>,
    native: &mut NativeSubdivisionContext<'_>,
    line_member_counts: &mut SourceLineMemberCountMemo,
    out: &mut Vec<Shape>,
    budget: &mut ExpansionBudget,
) -> Result<()> {
    let mut work = Vec::with_capacity(1);
    let mut staged = Vec::new();
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
        #[cfg(test)]
        if native.frame.is_some()
            && let (Shape::LineString(line), Some(range)) = (&item.shape, &item.source_line_members)
        {
            let identity = line
                .as_coords()
                .zs()
                .and_then(|zs| zs.first())
                .copied()
                .unwrap_or(0.0)
                .to_bits();
            SUBDIVISION_LINE_MEMBER_RANGES.with(|ranges| {
                ranges.borrow_mut().push((identity, range.start, range.end));
            });
        }
        let mut item_coordinate_count = item.coordinate_count;
        if item_coordinate_count.is_some_and(|count| count <= max_vertices) {
            staged.push(item);
            continue;
        }
        if matches!(native.path, SubdivisionPath::NativePrepared)
            && max_vertices >= 2
            && item.traces.is_none()
            && let (Some(frame), Some(line_members)) =
                (native.frame, item.source_line_members.clone())
        {
            let line_count = source_line_member_count(&item.shape, line_member_counts)?;
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
                    line_member_counts,
                )?;
                if !candidates.is_empty() {
                    native.prepare(max_vertices, budget, &candidates, line_member_counts)?;
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
                    if push_subdivision_members(
                        &mut item,
                        max_vertices,
                        &mut work,
                        line_member_counts,
                    )? {
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
            && push_subdivision_members(&mut item, max_vertices, &mut work, line_member_counts)?
        {
            continue;
        }
        let Some(bounds) = item.bounds else {
            if push_subdivision_members(&mut item, max_vertices, &mut work, line_member_counts)? {
                continue;
            }
            if stage_line_vertices_with_operation_work(&item, max_vertices, native, &mut staged)? {
                continue;
            }
            return Err(GeometryErrorKind::unrepresentable_output("subdivide"));
        };
        let owner_coordinate_count = owner_coordinate_count(&item.shape, item_coordinate_count);
        let candidate = if let Some(source_work) = native.budget.as_mut() {
            preferred_source_cut(
                &item.shape,
                bounds,
                owner_coordinate_count,
                item.vertex_cut,
                source_work,
            )?
        } else {
            let mut source_work = Unmetered;
            preferred_source_cut(
                &item.shape,
                bounds,
                owner_coordinate_count,
                item.vertex_cut,
                &mut source_work,
            )?
        };
        let Some((split_x, cut)) = candidate else {
            if !item.vertex_cut {
                work.push(SubdivisionWorkItem {
                    vertex_cut: true,
                    ..item
                });
                continue;
            }
            if push_subdivision_members(&mut item, max_vertices, &mut work, line_member_counts)? {
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
        let polygon_rooted = item.polygon_traces.is_some() && native.ordinary_roots.is_some();
        let trace_rooted =
            item.traces.is_some() && (native.topology.is_some() || native.ordinary_roots.is_some());
        let declared_axes = item.trace_axes.unwrap_or_else(|| item.shape.axes());
        let children = {
            if polygon_rooted {
                let roots = native.ordinary_roots.as_ref().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "ordinary polygon traces have no original root catalog".into(),
                    )
                })?;
                let polygon_traces = item.polygon_traces.as_deref().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "ordinary polygon root catalog lost its ring traces".into(),
                    )
                })?;
                let mut trace_work = Unmetered;
                inventory.subdivision_polygon_parts_with_roots(
                    &partition,
                    &SubdivisionTraceRequest {
                        source_shape: &item.shape,
                        source_traces: Some(polygon_traces),
                        roots,
                        root_positions: roots.root_xy_positions_needed().then_some(roots),
                        declared_axes,
                        strict: drop,
                        clip_operands: [1, 2],
                        context,
                        fallback_inventory: Some(&inventory),
                        reuse_existing_source_vertices: true,
                    },
                    budget,
                    &mut trace_work,
                )?
            } else if trace_rooted {
                if let Some(topology) = native.topology.as_ref() {
                    let trace_work = native.budget.as_mut().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native traced subdivision has no operation budget".into(),
                        )
                    })?;
                    inventory.subdivision_parts_with_budget(
                        &partition,
                        &SubdivisionTraceRequest {
                            source_shape: &item.shape,
                            source_traces: item.traces.as_deref(),
                            roots: topology,
                            root_positions: None,
                            declared_axes: crate::geometry::CoordinateAxes::XY,
                            strict: false,
                            clip_operands: [1, 2],
                            context,
                            fallback_inventory: Some(&inventory),
                            reuse_existing_source_vertices: false,
                        },
                        budget,
                        trace_work,
                    )?
                } else {
                    let roots = native.ordinary_roots.as_ref().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "ordinary source traces have no original root catalog".into(),
                        )
                    })?;
                    let mut trace_work = Unmetered;
                    inventory.subdivision_parts_with_roots(
                        &partition,
                        &SubdivisionTraceRequest {
                            source_shape: &item.shape,
                            source_traces: item.traces.as_deref(),
                            roots,
                            root_positions: roots.root_xy_positions_needed().then_some(roots),
                            declared_axes,
                            strict: drop,
                            clip_operands: [1, 2],
                            context,
                            fallback_inventory: Some(&inventory),
                            reuse_existing_source_vertices: true,
                        },
                        budget,
                        &mut trace_work,
                    )?
                }
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
                        polygon_traces: None,
                    },
                    SubdivisionPart {
                        shape: right,
                        traces: None,
                        polygon_traces: None,
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
            if push_subdivision_members(&mut item, max_vertices, &mut work, line_member_counts)? {
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
                polygon_traces: child.polygon_traces,
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

#[cfg(test)]
mod tests;
