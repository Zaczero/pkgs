use std::cmp::Ordering;

use rstar::AABB;

use super::super::q::ExactGeographicTopology;
use super::analysis::{ChartBounds, NodeEdge, SpanIndex};
use crate::collections::HashSet;
use crate::error::{ResourceError, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::GeometryErrorKind;
use crate::geometry::tessellation::exact::ExactRational;
use crate::work::Work;

#[derive(Clone)]
struct ExactSegment {
    start: [ExactRational; 2],
    end: [ExactRational; 2],
}

pub(super) fn mark_edges(
    span_edges: &[Vec<usize>],
    source_edges: &[super::super::exact::ExactSourceEdge],
    span_index: usize,
    row: usize,
    invalid_edges: &mut [bool],
    work: &mut impl Work,
) -> Result<()> {
    let edges = span_edges.get(span_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("coverage Q span edge map is incomplete".into())
    })?;
    for &edge in edges {
        work.charge_into_error(1)?;
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

pub(super) fn mark_t_junction_edges(
    node_edges: &[Vec<NodeEdge>],
    incident_spans: &[Vec<usize>],
    span_rows: &[Vec<usize>],
    invalid_edges: &mut [bool],
    work: &mut impl Work,
) -> Result<()> {
    for (node, events) in node_edges.iter().enumerate() {
        work.charge_into_error(1)?;
        let Some(incident) = incident_spans.get(node) else {
            continue;
        };
        for (left_index, left) in events.iter().enumerate() {
            for right in events.iter().skip(left_index.wrapping_add(1)) {
                work.charge_into_error(1)?;
                if left.row == right.row
                    || !(left.interior || right.interior)
                    || shared_incident_span_count(incident, span_rows, left.row, right.row, work)?
                        >= 2
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
    Ok(())
}

fn shared_incident_span_count(
    incident: &[usize],
    span_rows: &[Vec<usize>],
    left: usize,
    right: usize,
    work: &mut impl Work,
) -> Result<usize> {
    let mut count = 0_usize;
    for &span in incident {
        work.charge_into_error(1)?;
        if span_rows.get(span).is_some_and(|rows| {
            rows.binary_search(&left).is_ok() && rows.binary_search(&right).is_ok()
        }) {
            count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    Ok(count)
}

pub(super) fn mark_near_gap_edges(
    frame: GeographicFrame,
    gap_width: f64,
    topology: &ExactGeographicTopology,
    span_rows: &[Vec<usize>],
    span_edges: &[Vec<usize>],
    span_bounds: &[ChartBounds],
    span_index: &SpanIndex,
    coincident_pairs: &HashSet<(usize, usize)>,
    invalid_edges: &mut [bool],
    work: &mut impl Work,
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
        work.charge_into_error(1)?;
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
        for (shift_f64, shift) in &shifts {
            work.charge_into_error(1)?;
            let query = AABB::from_bounds(
                [
                    bounds.min[0] + *shift_f64 - gap_width,
                    bounds.min[1] - gap_width,
                ],
                [
                    bounds.max[0] + *shift_f64 + gap_width,
                    bounds.max[1] + gap_width,
                ],
            );
            for candidate in span_index.locate_in_envelope_intersecting(query) {
                work.charge_into_error(1)?;
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
                    work.charge_into_error(1)?;
                    if candidate_rows
                        .iter()
                        .any(|&candidate_row| candidate_row != source_row)
                        && exact_gap_within(
                            &query_segment,
                            shift,
                            &exact_span(topology, candidate_index)?,
                            &gap_squared,
                            work,
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
                        work.charge_into_error(1)?;
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
    work: &mut impl Work,
) -> Result<bool> {
    let query = translate_segment(query, shift, work)?;
    let query_midpoint = midpoint(&query.start, &query.end, work)?;
    let candidate_midpoint = midpoint(&candidate.start, &candidate.end, work)?;
    for point in [&query.start, &query.end, &query_midpoint] {
        if exact_point_segment_near(point, &candidate.start, &candidate.end, gap_squared, work)? {
            return Ok(true);
        }
    }
    for point in [&candidate.start, &candidate.end, &candidate_midpoint] {
        if exact_point_segment_near(point, &query.start, &query.end, gap_squared, work)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn translate_segment(
    segment: &ExactSegment,
    shift: &ExactRational,
    work: &mut impl Work,
) -> Result<ExactSegment> {
    Ok(ExactSegment {
        start: [
            segment.start[0].try_add_with_work(shift, work)?,
            segment.start[1].try_clone_with_work(work)?,
        ],
        end: [
            segment.end[0].try_add_with_work(shift, work)?,
            segment.end[1].try_clone_with_work(work)?,
        ],
    })
}

fn midpoint(
    start: &[ExactRational; 2],
    end: &[ExactRational; 2],
    work: &mut impl Work,
) -> Result<[ExactRational; 2]> {
    let two = ExactRational::from_f64(2.0);
    Ok([
        start[0]
            .try_add_with_work(&end[0], work)?
            .try_quotient_with_work(&two, work)?,
        start[1]
            .try_add_with_work(&end[1], work)?
            .try_quotient_with_work(&two, work)?,
    ])
}

fn exact_point_segment_near(
    point: &[ExactRational; 2],
    start: &[ExactRational; 2],
    end: &[ExactRational; 2],
    gap_squared: &ExactRational,
    work: &mut impl Work,
) -> Result<bool> {
    let distance = point_segment_distance_squared(point, start, end, work)?;
    Ok(!distance.is_zero() && distance.try_cmp_with_work(gap_squared, work)? == Ordering::Less)
}

fn point_segment_distance_squared(
    point: &[ExactRational; 2],
    start: &[ExactRational; 2],
    end: &[ExactRational; 2],
    work: &mut impl Work,
) -> Result<ExactRational> {
    let dx = end[0].try_subtract_with_work(&start[0], work)?;
    let dy = end[1].try_subtract_with_work(&start[1], work)?;
    let px = point[0].try_subtract_with_work(&start[0], work)?;
    let py = point[1].try_subtract_with_work(&start[1], work)?;
    let length_squared = dx
        .try_square_with_work(work)?
        .try_add_with_work(&dy.try_square_with_work(work)?, work)?;
    let zero = ExactRational::from_f64(0.0);
    if length_squared.is_zero() {
        return px
            .try_square_with_work(work)?
            .try_add_with_work(&py.try_square_with_work(work)?, work);
    }
    let projection = px
        .try_product_with_work(&dx, work)?
        .try_add_with_work(&py.try_product_with_work(&dy, work)?, work)?;
    if projection.try_cmp_with_work(&zero, work)? != Ordering::Greater {
        return px
            .try_square_with_work(work)?
            .try_add_with_work(&py.try_square_with_work(work)?, work);
    }
    if projection.try_cmp_with_work(&length_squared, work)? != Ordering::Less {
        let qx = point[0].try_subtract_with_work(&end[0], work)?;
        let qy = point[1].try_subtract_with_work(&end[1], work)?;
        return qx
            .try_square_with_work(work)?
            .try_add_with_work(&qy.try_square_with_work(work)?, work);
    }
    let cross = px
        .try_product_with_work(&dy, work)?
        .try_subtract_with_work(&py.try_product_with_work(&dx, work)?, work)?;
    cross
        .try_square_with_work(work)?
        .try_quotient_with_work(&length_squared, work)
}
