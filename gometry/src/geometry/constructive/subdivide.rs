//! Recursively split shapes while carrying source traces through each cut.

#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
#[cfg(test)]
use std::cell::Cell;

use crate::error::{ResourceError, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::overlay::{
    ExactSourceInventory, SubdivisionPart, SubdivisionRootCatalog, SubdivisionTraceRequest,
    rect_polygon,
};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::topology::SourceTrace;
use crate::geometry::{
    Bounds, CoordWindow, ExpansionBudget, GeometryErrorKind, LineSeq, Shape, ShapeCountQuery,
    SharedShapeOwners, SourceArcContext,
};
use crate::work::{Unmetered, Work};

#[cfg(test)]
thread_local! {
    static SUBDIVISION_WORK: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    static SOURCE_ORDINATE_AXIS_SCANS: Cell<usize> = const { Cell::new(0) };
    static ORDINARY_ROOT_CATALOG_BUILDS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn reset_subdivision_work() {
    SUBDIVISION_WORK.with(|work| work.set((0, 0)));
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
        for member in members.into_iter().rev() {
            work.push(SubdivisionWorkItem::new(member, vertex_cut));
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
                trace_axes: None,
                vertex_cut: item.vertex_cut,
            },
        });
        start = end.saturating_sub(1);
    }
    Ok(true)
}

fn stage_line_vertices_unmetered(
    item: &SubdivisionWorkItem,
    max_vertices: usize,
    staged: &mut Vec<SubdivisionWorkItem>,
) -> Result<bool> {
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

struct SubdivisionWorkItem {
    shape: Shape,
    bounds: Option<Bounds>,
    coordinate_count: Option<usize>,
    traces: Option<Vec<SourceTrace>>,
    polygon_traces: Option<Vec<SourceTrace>>,
    trace_axes: Option<crate::geometry::CoordinateAxes>,
    vertex_cut: bool,
}

impl SubdivisionWorkItem {
    fn new(shape: Shape, vertex_cut: bool) -> Self {
        let bounds = shape.bounds();
        let coordinate_count = ShapeCountQuery::default().coordinate_count(&shape);
        Self {
            shape,
            bounds,
            coordinate_count,
            traces: None,
            polygon_traces: None,
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
            trace_axes: Some(trace_axes),
            vertex_cut,
        }
    }
}

struct SubdivisionContext {
    frame: Option<GeographicFrame>,
    ordinary_roots: Option<SubdivisionRootCatalog>,
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
    let mut subdivision = SubdivisionContext {
        frame,
        ordinary_roots: None,
    };
    let mut item = SubdivisionWorkItem::new(shape, false);
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
                    subdivision.ordinary_roots = Some(roots);
                    item.traces = Some(traces);
                    item.trace_axes = Some(axes);
                },
                Shape::Polygon(_) | Shape::MultiPolygon(_) => {
                    let (roots, traces) = SubdivisionRootCatalog::from_polygon_shape(
                        &original,
                        frame,
                        root_xy_positions_needed,
                    )?;
                    subdivision.ordinary_roots = Some(roots);
                    item.polygon_traces = Some(traces);
                    item.trace_axes = Some(axes);
                },
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => {},
            }
        }
    }
    split_with_context(item, max_vertices, drop, context, &subdivision, out, budget)
}

fn split_with_context<M: crate::geometry::ChartMetric>(
    item: SubdivisionWorkItem,
    max_vertices: usize,
    drop: bool,
    context: &SourceArcContext<'_, '_, M>,
    subdivision: &SubdivisionContext,
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
        let item_coordinate_count = item.coordinate_count;
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
            if stage_line_vertices_unmetered(&item, max_vertices, &mut staged)? {
                continue;
            }
            return Err(GeometryErrorKind::unrepresentable_output("subdivide"));
        };
        let owner_coordinate_count = owner_coordinate_count(&item.shape, item_coordinate_count);
        let mut source_work = Unmetered;
        let candidate = preferred_source_cut(
            &item.shape,
            bounds,
            owner_coordinate_count,
            item.vertex_cut,
            &mut source_work,
        )?;
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
            if stage_line_vertices_unmetered(&item, max_vertices, &mut staged)? {
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
        if let Some(frame) = subdivision.frame {
            partition.set_geographic_frame(frame);
        }
        let polygon_rooted = item.polygon_traces.is_some() && subdivision.ordinary_roots.is_some();
        let trace_rooted = item.traces.is_some() && subdivision.ordinary_roots.is_some();
        let declared_axes = item.trace_axes.unwrap_or_else(|| item.shape.axes());
        let children = {
            if polygon_rooted {
                let roots = subdivision.ordinary_roots.as_ref().ok_or_else(|| {
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
                let roots = subdivision.ordinary_roots.as_ref().ok_or_else(|| {
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
            if stage_line_vertices_unmetered(&item, max_vertices, &mut staged)? {
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
                polygon_traces: child.polygon_traces,
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
    out.reserve(staged.len());
    out.extend(staged.into_iter().map(|item| item.shape));
    Ok(())
}

#[cfg(test)]
mod tests;
