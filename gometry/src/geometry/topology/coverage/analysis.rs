use std::borrow::Borrow;

use rstar::AABB;
use rstar::primitives::{GeomWithData, Rectangle};

use super::super::exact::SourceRowId;
use super::super::q::{ExactGeographicTopology, row_locations};
use super::GeographicCoverageAnalysis;
use super::gaps::{mark_edges, mark_near_gap_edges, mark_t_junction_edges};
use crate::collections::{try_hash_map_with_capacity, try_hash_set_with_capacity};
use crate::error::{ResourceError, Result};
use crate::geographic::lonlat::geographic_admission_error;
use crate::geographic::{GeographicFrame, ScalarGeographicView};
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{BulkRTree, ExpansionBudget, GeometryErrorKind, Loc, Shape};
use crate::work::{Unmetered, Work};

type RowIndex = BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>>;

pub(super) type SpanIndex = BulkRTree<GeomWithData<Rectangle<[f64; 2]>, usize>>;

#[derive(Clone, Copy)]
pub(super) struct ChartBounds {
    pub(super) min: [f64; 2],
    pub(super) max: [f64; 2],
}

impl ChartBounds {
    const fn include(&mut self, other: Self) {
        self.min[0] = self.min[0].min(other.min[0]);
        self.min[1] = self.min[1].min(other.min[1]);
        self.max[0] = self.max[0].max(other.max[0]);
        self.max[1] = self.max[1].max(other.max[1]);
    }

    fn rectangle(self) -> Rectangle<[f64; 2]> {
        Rectangle::from_corners(self.min, self.max)
    }
}

pub(super) struct NodeEdge {
    pub(super) row: usize,
    pub(super) edge: usize,
    pub(super) interior: bool,
}

pub(crate) fn analyze_geographic_coverage<S: Borrow<Shape>>(
    rows: &[S],
    frame: GeographicFrame,
    gap_width: f64,
    operation: &'static str,
) -> Result<GeographicCoverageAnalysis> {
    let mut work = Unmetered;
    analyze_geographic_coverage_with_work(rows, frame, gap_width, operation, &mut work)
}

pub(crate) fn analyze_geographic_coverage_with_work<S: Borrow<Shape>>(
    rows: &[S],
    frame: GeographicFrame,
    gap_width: f64,
    operation: &'static str,
    work: &mut impl Work,
) -> Result<GeographicCoverageAnalysis> {
    if !(gap_width.is_finite() && gap_width >= 0.0) {
        return Err(GeometryErrorKind::NonNegativeFinite("gap_width", gap_width).into());
    }

    let mut views = Vec::with_capacity(rows.len());
    for row in rows {
        work.charge_into_error(1)?;
        let shape = row.borrow();
        let view = ScalarGeographicView::try_new(frame, shape)
            .map_err(|diagnostic| geographic_admission_error(&diagnostic))?;
        views.push(view);
    }
    let mut expansion_budget = ExpansionBudget::new(operation, "exact chart noding");
    let topology = ExactGeographicTopology::from_views_with_work_and_expansion_budget(
        frame,
        views.into_iter(),
        &mut expansion_budget,
        work,
    )?;
    let mut face_rows: Vec<Vec<usize>> = std::iter::repeat_with(Vec::new)
        .take(topology.q.global_face_count)
        .collect();
    topology
        .q
        .face_rows
        .for_each_face_occupants(work, |face, occupants| {
            let rows = face_rows.get_mut(face.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q face occupancy references an unknown face".into())
            })?;
            rows.extend(occupants.iter().map(|row| row.0 as usize));
            Ok(())
        })?;

    let source_rows = topology.sources.row_count;
    let source_edges = &topology.sources.edges;
    let mut locations = Vec::with_capacity(source_rows);
    for row in 0..source_rows {
        work.charge_into_error(1)?;
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
    let mut node_edges: Vec<Vec<NodeEdge>> = std::iter::repeat_with(Vec::new)
        .take(topology.q.nodes.len())
        .collect();

    for (span_index, span) in topology.q.spans.iter().enumerate() {
        work.charge_into_error(1)?;
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
        let [first_node, _] = span.nodes;
        for (endpoint, node) in span.nodes.into_iter().enumerate() {
            work.charge_into_error(1)?;
            if endpoint == 1 && first_node == node {
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
            work.charge_into_error(1)?;
            let edge_index = owner.source_edge.0 as usize;
            let edge = source_edges.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("coverage Q span has an unknown source edge".into())
            })?;
            let row = edge.row.0 as usize;
            rows_on_span.push(row);
            edges_on_span.push(edge_index);

            for (endpoint, node) in span.nodes.into_iter().enumerate() {
                work.charge_into_error(1)?;
                let node = node.0 as usize;
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
            work.charge_into_error(1)?;
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
                work.charge_into_error(1)?;
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

    expand_winding_row_bounds(&topology, &mut row_bounds, work)?;

    let mut coincident_pairs = try_hash_set_with_capacity(shared_span_counts.len())?;
    #[expect(
        clippy::iter_over_hash_type,
        reason = "hash order only populates a membership set that is queried by key"
    )]
    for (pair, shared_count) in shared_span_counts {
        work.charge_into_error(1)?;
        let (left, right) = pair;
        if shared_count != 0
            && row_span_counts.get(left) == Some(&shared_count)
            && row_span_counts.get(right) == Some(&shared_count)
        {
            coincident_pairs.insert(pair);
        }
    }
    drop(row_span_counts);

    work.charge_product_into_error(row_bounds.len(), 1)?;
    let row_index = build_row_index(&row_bounds);
    drop(row_bounds);
    let mut invalid_edges = vec![false; source_edges.len()];

    for (span_index, rows_on_span) in span_rows.iter().enumerate() {
        work.charge_into_error(1)?;
        if rows_on_span.is_empty() {
            continue;
        }
        let bounds = *span_bounds
            .get(span_index)
            .expect("coverage Q span has a retained chart envelope");
        let query = AABB::from_bounds(bounds.min, bounds.max);
        for candidate in row_index.locate_in_envelope_intersecting(query) {
            work.charge_into_error(1)?;
            let candidate_row = candidate.data;
            for &source_row in rows_on_span {
                work.charge_into_error(1)?;
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
                        work,
                    )?;
                }
            }
        }
    }
    drop((locations, row_index));

    mark_t_junction_edges(
        &node_edges,
        &incident_spans,
        &span_rows,
        &mut invalid_edges,
        work,
    )?;
    drop((node_edges, incident_spans));

    if gap_width > 0.0 {
        work.charge_product_into_error(span_bounds.len(), 1)?;
        let span_index = build_span_index(&span_bounds);
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
            work,
        )?;
    }

    Ok(GeographicCoverageAnalysis {
        topology,
        face_rows,
        span_rows,
        invalid_source_edge_flags: invalid_edges,
        invalid_source_owners: Vec::new(),
    })
}

impl ChartBounds {
    fn from_points(
        start: &super::super::exact::ExactChartPoint,
        end: &super::super::exact::ExactChartPoint,
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
    work: &mut impl Work,
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
        work.charge_into_error(1)?;
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
