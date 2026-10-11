use std::borrow::Borrow;
use std::cmp::Ordering;

use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use super::exact::SourceRowId;
use super::q::{ExactGeographicTopology, row_locations};
use crate::collections::{HashSet, try_hash_map_with_capacity, try_hash_set_with_capacity};
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, ScalarGeographicView};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    BulkRTree, ExpansionBudget, GeometryErrorKind, Loc, Segment, Shape, Unmetered, XY,
};

type RowIndex = BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>>;
type SpanIndex = BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>>;

#[derive(Clone, Copy)]
struct ChartBounds {
    min: [f64; 2],
    max: [f64; 2],
}

impl ChartBounds {
    fn include(&mut self, other: Self) {
        self.min[0] = self.min[0].min(other.min[0]);
        self.min[1] = self.min[1].min(other.min[1]);
        self.max[0] = self.max[0].max(other.max[0]);
        self.max[1] = self.max[1].max(other.max[1]);
    }

    fn rectangle(self) -> Rectangle<[f64; 2]> {
        Rectangle::from_corners(self.min, self.max)
    }
}

#[derive(Clone)]
struct ExactSegment {
    start: [ExactRational; 2],
    end: [ExactRational; 2],
}

struct NodeEdge {
    row: usize,
    edge: usize,
    interior: bool,
}

/// Return each offending original source edge in its input row.
///
/// Q supplies the shared geographic noding and row-face relation. The
/// returned coordinates remain the original source coordinates; chart cuts
/// and quotient aliases never become diagnostic linework.
pub(crate) fn invalid_coverage_source_segments<S: Borrow<Shape>>(
    rows: &[S],
    frame: GeographicFrame,
    gap_width: f64,
    operation: &'static str,
) -> Result<Vec<Vec<Segment>>> {
    if !(gap_width.is_finite() && gap_width >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("gap_width", gap_width).into());
    }

    let mut views = Vec::with_capacity(rows.len());
    for row in rows {
        let shape = row.borrow();
        let view = ScalarGeographicView::try_new(frame, shape).map_err(|diagnostic| {
            crate::geographic::lonlat::geographic_admission_error(&diagnostic)
        })?;
        views.push(view);
    }
    let mut expansion_budget = ExpansionBudget::new(operation, "exact chart noding");
    let mut work = Unmetered;
    let topology = ExactGeographicTopology::from_views_with_work_and_expansion_budget(
        frame,
        views.into_iter(),
        &mut expansion_budget,
        &mut work,
    )?;

    let source_rows = topology.sources.row_count;
    let source_edges = &topology.sources.edges;
    let mut locations = Vec::with_capacity(source_rows);
    for row in 0..source_rows {
        let row = u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?;
        locations.push(row_locations(&topology, SourceRowId(row))?);
    }

    let mut span_rows = Vec::with_capacity(topology.q.spans.len());
    let mut span_edges = Vec::with_capacity(topology.q.spans.len());
    let mut row_span_counts = vec![0_usize; source_rows];
    let mut shared_span_counts = try_hash_map_with_capacity(topology.q.spans.len())?;
    let mut row_bounds: Vec<Option<ChartBounds>> = vec![None; source_rows];
    let mut span_bounds = Vec::with_capacity(topology.q.spans.len());
    let mut incident_spans = vec![Vec::new(); topology.q.nodes.len()];
    let mut node_edges: Vec<Vec<NodeEdge>> =
        (0..topology.q.nodes.len()).map(|_| Vec::new()).collect();

    for (span_index, span) in topology.q.spans.iter().enumerate() {
        let start = topology
            .q
            .ports
            .get(span.ports[0].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q span has no start port".into())
            })?;
        let end = topology
            .q
            .ports
            .get(span.ports[1].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("coverage Q span has no end port".into()))?;
        let bounds = ChartBounds::from_points(&start.point, &end.point)?;
        span_bounds.push(bounds);
        for (endpoint, node) in span.nodes.iter().enumerate() {
            if endpoint == 1 && span.nodes[0] == *node {
                continue;
            }
            incident_spans
                .get_mut(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage Q span has an unknown node".into())
                })?
                .push(span_index);
        }

        let mut rows_on_span = Vec::with_capacity(span.owners.len());
        let mut edges_on_span = Vec::with_capacity(span.owners.len());
        for owner in &span.owners {
            let edge_index = owner.source_edge.0 as usize;
            let edge = source_edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q span has an unknown source edge".into())
            })?;
            let row = edge.row.0 as usize;
            rows_on_span.push(row);
            edges_on_span.push(edge_index);

            for endpoint in 0..2 {
                let node = span.nodes[endpoint].0 as usize;
                let parameter = if (endpoint == 0) == owner.reversed {
                    &owner.source_range.end
                } else {
                    &owner.source_range.start
                };
                let interior = !parameter.is_zero()
                    && parameter.try_cmp(&ExactRational::from_f64(1.0))?.is_lt();
                let events = node_edges.get_mut(node).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage Q span has an unknown node".into())
                })?;
                if !events.iter().any(|event| {
                    event.row == row && event.edge == edge_index && event.interior == interior
                }) {
                    events.push(NodeEdge {
                        row,
                        edge: edge_index,
                        interior,
                    });
                }
            }
        }
        rows_on_span.sort_unstable();
        rows_on_span.dedup();
        edges_on_span.sort_unstable();
        edges_on_span.dedup();
        for &row in &rows_on_span {
            let count = row_span_counts.get_mut(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q row is out of range".into())
            })?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if let Some(row_bounds) = row_bounds.get_mut(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q row is out of range".into())
            })? {
                row_bounds.include(bounds);
            } else {
                *row_bounds.get_mut(row).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage Q row is out of range".into())
                })? = Some(bounds);
            }
        }
        for left in 0..rows_on_span.len() {
            for right in left.wrapping_add(1)..rows_on_span.len() {
                let pair = (
                    *rows_on_span
                        .get(left)
                        .expect("coverage row pair is within its span"),
                    *rows_on_span
                        .get(right)
                        .expect("coverage row pair is within its span"),
                );
                let count = shared_span_counts.entry(pair).or_insert(0_usize);
                *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        span_rows.push(rows_on_span);
        span_edges.push(edges_on_span);
    }

    expand_winding_row_bounds(&topology, &mut row_bounds)?;

    let mut coincident_pairs = try_hash_set_with_capacity(shared_span_counts.len())?;
    for (pair, shared_count) in shared_span_counts {
        let [left, right] = [pair.0, pair.1];
        if shared_count != 0
            && row_span_counts.get(left) == Some(&shared_count)
            && row_span_counts.get(right) == Some(&shared_count)
        {
            coincident_pairs.insert(pair);
        }
    }

    let row_index = build_row_index(&row_bounds);
    let span_index = build_span_index(&span_bounds);
    let mut invalid_edges = vec![false; source_edges.len()];

    for (span_index, rows_on_span) in span_rows.iter().enumerate() {
        if rows_on_span.is_empty() {
            continue;
        }
        let bounds = *span_bounds
            .get(span_index)
            .expect("coverage Q span has a retained chart envelope");
        let query = AABB::from_bounds(bounds.min, bounds.max);
        for candidate in row_index.locate_in_envelope_intersecting(query) {
            let candidate_row = candidate.data;
            for &source_row in rows_on_span {
                if source_row == candidate_row {
                    continue;
                }
                let ordinary_shared = rows_on_span.len() == 2
                    && rows_on_span
                        .first()
                        .zip(rows_on_span.get(1))
                        .is_some_and(|(&left, &right)| !coincident_pairs.contains(&(left, right)))
                    && rows_on_span.contains(&candidate_row);
                if ordinary_shared {
                    continue;
                }
                if locations
                    .get(candidate_row)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "coverage Q candidate row is out of range".into(),
                        )
                    })?
                    .span(span_index)?
                    .is_some_and(|location| location != Loc::Exterior)
                {
                    mark_edges(
                        &span_edges,
                        source_edges,
                        span_index,
                        source_row,
                        &mut invalid_edges,
                    )?;
                }
            }
        }
    }

    mark_t_junction_edges(&node_edges, &incident_spans, &span_rows, &mut invalid_edges);

    if gap_width > 0.0 {
        mark_near_gap_edges(
            frame,
            gap_width,
            &topology,
            &span_rows,
            &span_edges,
            &span_bounds,
            &span_index,
            &coincident_pairs,
            &mut invalid_edges,
        )?;
    }

    let mut output = vec![Vec::new(); source_rows];
    for (edge_index, edge) in source_edges.iter().enumerate() {
        if !invalid_edges
            .get(edge_index)
            .copied()
            .expect("coverage invalid-edge flag matches its source inventory")
        {
            continue;
        }
        let row = edge.row.0 as usize;
        output
            .get_mut(row)
            .ok_or_else(|| GeometryErrorKind::Invalid("coverage edge row is out of range".into()))?
            .push(Segment {
                start: XY::new(edge.source[0].x, edge.source[0].y),
                end: XY::new(edge.source[1].x, edge.source[1].y),
            });
    }
    Ok(output)
}

impl ChartBounds {
    fn from_points(
        start: &super::exact::ExactChartPoint,
        end: &super::exact::ExactChartPoint,
    ) -> Result<Self> {
        let (start_min_x, start_max_x) = start.x.outward_bounds()?;
        let (start_min_y, start_max_y) = start.y.outward_bounds()?;
        let (end_min_x, end_max_x) = end.x.outward_bounds()?;
        let (end_min_y, end_max_y) = end.y.outward_bounds()?;
        let [min_x, min_y] = [start_min_x.min(end_min_x), start_min_y.min(end_min_y)];
        let [max_x, max_y] = [start_max_x.max(end_max_x), start_max_y.max(end_max_y)];
        Ok(Self {
            min: [min_x, min_y],
            max: [max_x, max_y],
        })
    }
}

fn build_row_index(bounds: &[Option<ChartBounds>]) -> RowIndex {
    let entries = bounds
        .iter()
        .enumerate()
        .filter_map(|(row, bounds)| bounds.map(|bounds| GeomWithData::new(bounds.rectangle(), row)))
        .collect();
    BulkRTree::bulk_load_with_params(entries)
}

fn build_span_index(bounds: &[ChartBounds]) -> SpanIndex {
    let entries = bounds
        .iter()
        .enumerate()
        .map(|(span, bounds)| GeomWithData::new(bounds.rectangle(), span))
        .collect();
    BulkRTree::bulk_load_with_params(entries)
}

fn expand_winding_row_bounds(
    topology: &ExactGeographicTopology,
    row_bounds: &mut [Option<ChartBounds>],
) -> Result<()> {
    let frame = topology.sources.frame;
    let chart_min = topology.q.cut.outward_bounds()?.0;
    let chart_max = topology
        .q
        .cut
        .try_add(&ExactRational::from_f64(frame.longitude().period()))?
        .outward_bounds()?
        .1;
    let south_pole = frame.native_latitude_pole(crate::geographic::GeodeticPole::South);
    let north_pole = frame.native_latitude_pole(crate::geographic::GeodeticPole::North);
    let full_latitude = [south_pole.min(north_pole), south_pole.max(north_pole)];

    for ring in &topology.sources.rings {
        if ring.summary.chart_turn == 0
            && !matches!(
                ring.summary.domain,
                crate::geographic::RingDomain::FullLongitude(_)
            )
        {
            continue;
        }
        let polygon = topology
            .sources
            .polygons
            .get(ring.polygon.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage winding ring has no source polygon".into())
            })?;
        let row = polygon.row.0 as usize;
        let bounds = row_bounds.get_mut(row).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage winding ring has an unknown source row".into())
        })?;
        let Some(bounds) = bounds else {
            continue;
        };
        bounds.min = [
            bounds.min[0].min(chart_min),
            bounds.min[1].min(full_latitude[0]),
        ];
        bounds.max = [
            bounds.max[0].max(chart_max),
            bounds.max[1].max(full_latitude[1]),
        ];
    }
    Ok(())
}

fn mark_edges(
    span_edges: &[Vec<usize>],
    source_edges: &[super::exact::ExactSourceEdge],
    span_index: usize,
    row: usize,
    invalid_edges: &mut [bool],
) -> Result<()> {
    let edges = span_edges.get(span_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage Q span edge map is incomplete".into())
    })?;
    for &edge in edges {
        let source = source_edges.get(edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage source edge is out of range".into())
        })?;
        if source.row.0 as usize != row {
            continue;
        }
        let flag = invalid_edges.get_mut(edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("coverage source edge is out of range".into())
        })?;
        *flag = true;
    }
    Ok(())
}

fn mark_t_junction_edges(
    node_edges: &[Vec<NodeEdge>],
    incident_spans: &[Vec<usize>],
    span_rows: &[Vec<usize>],
    invalid_edges: &mut [bool],
) {
    for (node, events) in node_edges.iter().enumerate() {
        let Some(incident) = incident_spans.get(node) else {
            continue;
        };
        for (left_index, left) in events.iter().enumerate() {
            for right in events.iter().skip(left_index.wrapping_add(1)) {
                if left.row == right.row
                    || !(left.interior || right.interior)
                    || shared_incident_span_count(incident, span_rows, left.row, right.row) >= 2
                {
                    continue;
                }
                if let Some(flag) = invalid_edges.get_mut(left.edge) {
                    *flag = true;
                }
                if let Some(flag) = invalid_edges.get_mut(right.edge) {
                    *flag = true;
                }
            }
        }
    }
}

fn shared_incident_span_count(
    incident: &[usize],
    span_rows: &[Vec<usize>],
    left: usize,
    right: usize,
) -> usize {
    incident
        .iter()
        .filter(|&&span| {
            span_rows.get(span).is_some_and(|rows| {
                rows.binary_search(&left).is_ok() && rows.binary_search(&right).is_ok()
            })
        })
        .count()
}

fn mark_near_gap_edges(
    frame: GeographicFrame,
    gap_width: f64,
    topology: &ExactGeographicTopology,
    span_rows: &[Vec<usize>],
    span_edges: &[Vec<usize>],
    span_bounds: &[ChartBounds],
    span_index: &SpanIndex,
    coincident_pairs: &HashSet<(usize, usize)>,
    invalid_edges: &mut [bool],
) -> Result<()> {
    let gap = ExactRational::from_f64(gap_width);
    let gap_squared = gap.try_square()?;
    let period = frame.longitude().period();
    let shifts = [
        (-period, ExactRational::from_f64(-period)),
        (0.0, ExactRational::from_f64(0.0)),
        (period, ExactRational::from_f64(period)),
    ];

    for (query_index, rows_on_query) in span_rows.iter().enumerate() {
        let ordinary_shared = rows_on_query.len() == 2
            && rows_on_query
                .first()
                .zip(rows_on_query.get(1))
                .is_some_and(|(&left, &right)| !coincident_pairs.contains(&(left, right)));
        if rows_on_query.is_empty() || ordinary_shared {
            continue;
        }
        let bounds = *span_bounds
            .get(query_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("coverage gap span has no bounds".into()))?;
        let query_segment = exact_span(topology, query_index)?;
        for &(shift_f64, ref shift) in &shifts {
            let query = AABB::from_bounds(
                [
                    bounds.min[0] + shift_f64 - gap_width,
                    bounds.min[1] - gap_width,
                ],
                [
                    bounds.max[0] + shift_f64 + gap_width,
                    bounds.max[1] + gap_width,
                ],
            );
            for candidate in span_index.locate_in_envelope_intersecting(query) {
                let candidate_index = candidate.data;
                if candidate_index == query_index {
                    continue;
                }
                let candidate_rows = span_rows.get(candidate_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage gap candidate span is out of range".into())
                })?;
                if candidate_rows.is_empty() {
                    continue;
                }
                let mut near_other_row = false;
                for &source_row in rows_on_query {
                    if candidate_rows
                        .iter()
                        .any(|&candidate_row| candidate_row != source_row)
                        && exact_gap_within(
                            &query_segment,
                            shift,
                            &exact_span(topology, candidate_index)?,
                            &gap_squared,
                        )?
                    {
                        near_other_row = true;
                        break;
                    }
                }
                if near_other_row {
                    for &edge in span_edges.get(query_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "coverage gap span edge map is incomplete".into(),
                        )
                    })? {
                        *invalid_edges.get_mut(edge).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "coverage gap source edge is out of range".into(),
                            )
                        })? = true;
                    }
                }
            }
        }
    }
    Ok(())
}

fn exact_span(topology: &ExactGeographicTopology, span_index: usize) -> Result<ExactSegment> {
    let span = topology.q.spans.get(span_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage exact span index is out of range".into())
    })?;
    let start = topology
        .q
        .ports
        .get(span.ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage Q span has no start port".into()))?;
    let end = topology
        .q
        .ports
        .get(span.ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("coverage Q span has no end port".into()))?;
    Ok(ExactSegment {
        start: [start.point.x.clone(), start.point.y.clone()],
        end: [end.point.x.clone(), end.point.y.clone()],
    })
}

fn exact_gap_within(
    query: &ExactSegment,
    shift: &ExactRational,
    candidate: &ExactSegment,
    gap_squared: &ExactRational,
) -> Result<bool> {
    let query = translate_segment(query, shift)?;
    let query_midpoint = midpoint(&query.start, &query.end)?;
    let candidate_midpoint = midpoint(&candidate.start, &candidate.end)?;
    for point in [&query.start, &query.end, &query_midpoint] {
        if exact_point_segment_near(point, &candidate.start, &candidate.end, gap_squared)? {
            return Ok(true);
        }
    }
    for point in [&candidate.start, &candidate.end, &candidate_midpoint] {
        if exact_point_segment_near(point, &query.start, &query.end, gap_squared)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn translate_segment(segment: &ExactSegment, shift: &ExactRational) -> Result<ExactSegment> {
    Ok(ExactSegment {
        start: [segment.start[0].try_add(shift)?, segment.start[1].clone()],
        end: [segment.end[0].try_add(shift)?, segment.end[1].clone()],
    })
}

fn midpoint(start: &[ExactRational; 2], end: &[ExactRational; 2]) -> Result<[ExactRational; 2]> {
    let two = ExactRational::from_f64(2.0);
    Ok([
        start[0].try_add(&end[0])?.try_quotient(&two)?,
        start[1].try_add(&end[1])?.try_quotient(&two)?,
    ])
}

fn exact_point_segment_near(
    point: &[ExactRational; 2],
    start: &[ExactRational; 2],
    end: &[ExactRational; 2],
    gap_squared: &ExactRational,
) -> Result<bool> {
    let distance = point_segment_distance_squared(point, start, end)?;
    Ok(!distance.is_zero() && distance.try_cmp(gap_squared)? == Ordering::Less)
}

fn point_segment_distance_squared(
    point: &[ExactRational; 2],
    start: &[ExactRational; 2],
    end: &[ExactRational; 2],
) -> Result<ExactRational> {
    let dx = end[0].try_subtract(&start[0])?;
    let dy = end[1].try_subtract(&start[1])?;
    let px = point[0].try_subtract(&start[0])?;
    let py = point[1].try_subtract(&start[1])?;
    let length_squared = dx.try_square()?.try_add(&dy.try_square()?)?;
    let projection = px.try_product(&dx)?.try_add(&py.try_product(&dy)?)?;
    let zero = ExactRational::from_f64(0.0);
    if length_squared.is_zero() {
        return px.try_square()?.try_add(&py.try_square()?);
    }
    if projection.try_cmp(&zero)? != Ordering::Greater {
        return px.try_square()?.try_add(&py.try_square()?);
    }
    if projection.try_cmp(&length_squared)? != Ordering::Less {
        let qx = point[0].try_subtract(&end[0])?;
        let qy = point[1].try_subtract(&end[1])?;
        return qx.try_square()?.try_add(&qy.try_square()?);
    }
    let cross = px.try_product(&dy)?.try_subtract(&py.try_product(&dx)?)?;
    cross.try_square()?.try_quotient(&length_squared)
}
