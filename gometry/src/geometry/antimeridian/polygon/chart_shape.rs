use super::super::lineage::{EdgeUseId, EventId, MemberId};
use super::chart_events::{ChartSourceCapture, append_source_edge_events, chart_longitude};
use super::*;

fn append_source_chart_edge<M: ChartMetric>(
    output: &mut CoordSeqBuilder,
    source: [Point; 2],
    chart_start: Point,
    chart_end: Point,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    include_seam: bool,
    circle_witnesses: bool,
    band: Option<(f64, f64, f64)>,
    member: Option<MemberId>,
    mut capture: Option<&mut SplitCapture>,
    mut lineage_vertices: Option<&mut Vec<Vec<EventId>>>,
    lineage_edges: &mut Vec<EdgeUseId>,
) -> Result<()> {
    if output.len() == 0 {
        output.push(chart_start);
        if let Some(vertices) = &mut lineage_vertices {
            vertices.push(Vec::new());
        }
    }
    let source_root = match (capture.as_deref_mut(), member) {
        (Some(capture), Some(member)) => Some(capture.edge_root(member, &source, frame)?),
        _ => None,
    };
    let mut source_capture = ChartSourceCapture::new(source_root, member, capture.as_deref_mut());
    let start_event = source_capture.record(source_root, &ExactRational::zero())?;
    if let (Some(vertices), Some(event)) = (&mut lineage_vertices, start_event) {
        vertices
            .last_mut()
            .expect("a chart edge has an opening coordinate")
            .push(event);
    }
    append_source_edge_events(
        output,
        &source,
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
        include_seam,
        circle_witnesses,
        band,
        &mut source_capture,
    )?;
    let recorded = source_capture.event_ids();
    if let Some(vertices) = &mut lineage_vertices {
        vertices.extend(recorded.into_iter().skip(1).map(|event| vec![event]));
    }
    let end_event = source_capture.record(source_root, &ExactRational::one())?;
    output.push(chart_end);
    if let (Some(vertices), Some(event)) = (lineage_vertices, end_event) {
        vertices.push(vec![event]);
    }
    lineage_edges.extend(source_capture.edge_uses()?);
    Ok(())
}

#[cfg(test)]
pub(super) fn chart_line<M: ChartMetric>(
    line: &LineSeq,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<LineSeq> {
    chart_line_captured(
        line,
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
        None,
        None,
    )
    .map(|(line, _)| line)
}

fn chart_line_captured<M: ChartMetric>(
    line: &LineSeq,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    member: Option<super::super::lineage::MemberId>,
    mut capture: Option<&mut SplitCapture>,
) -> Result<(LineSeq, Option<super::super::lineage::LineageSequence>)> {
    let member = match capture.as_deref_mut() {
        Some(capture) => Some(capture.next_member()?),
        None => member,
    };
    let mut start = line.point_at(0);
    let mut start_chart = chart_point(start, frame, longitude_scale, latitude_scale)?;
    let mut chart = CoordSeqBuilder::with_capacity(start.axes(), line.len());
    let mut lineage_vertices = member.map(|_| Vec::new());
    let mut lineage_edges = Vec::new();
    for end in line.iter_coords().skip(1) {
        let end_chart = chart_point(end, frame, longitude_scale, latitude_scale)?;
        append_source_chart_edge(
            &mut chart,
            [start, end],
            start_chart,
            end_chart,
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
            true,
            true,
            None,
            member,
            capture.as_deref_mut(),
            lineage_vertices.as_mut(),
            &mut lineage_edges,
        )?;
        start = end;
        start_chart = end_chart;
    }
    if chart.len() == 0 {
        chart.push(start_chart);
        if let Some(vertices) = &mut lineage_vertices {
            vertices.push(Vec::new());
        }
    }
    let lineage = lineage_vertices.map(|vertices| super::super::lineage::LineageSequence {
        member: member.expect("captured line lineage has a member"),
        vertices,
        edges: lineage_edges,
    });
    Ok((LineSeq::from_proven(chart.finish()?), lineage))
}

#[cfg(test)]
pub(super) fn chart_ring<M: ChartMetric>(
    ring: &Ring,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Ring> {
    chart_ring_captured(
        ring,
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
        None,
        None,
    )
    .map(|(ring, _)| ring)
}

fn chart_ring_captured<M: ChartMetric>(
    ring: &Ring,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    member: Option<super::super::lineage::MemberId>,
    mut capture: Option<&mut SplitCapture>,
) -> Result<(Ring, Option<super::super::lineage::LineageSequence>)> {
    let geographic = GeographicRing::from_ring(ring);
    let summary =
        RingSummary::try_new(frame, &geographic).map_err(|error| split_view_error(&error))?;
    let axis = frame.longitude();
    let maximum_event_count = geographic
        .len()
        .saturating_sub(1)
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let _maximum_output_count = geographic
        .len()
        .checked_add(maximum_event_count)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut points = CoordSeqBuilder::with_capacity(ring.axes(), geographic.len());
    let mut lineage_vertices = member.map(|_| Vec::new());
    let mut lineage_edges = Vec::new();
    if matches!(summary.domain, RingDomain::FullLongitude(_)) && summary.chart_turn == 0 {
        let unwrapped = super::normalize::unwrap_coordinates(ring.coords(), frame).map_err(
            |error| match error {
                super::normalize::UnwrapChartError::Geometry(error) => error,
                super::normalize::UnwrapChartError::Winding => {
                    GeometryErrorKind::antimeridian_split_failed(
                        "axis band has no finite continuous longitude chart",
                    )
                },
            },
        )?;
        let unwrapped = Ring::try_from_closed(unwrapped)?;
        let xs = unwrapped.xs();
        let west = xs.iter().copied().fold(f64::INFINITY, f64::min);
        let east = xs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let band_point = |point: Point| -> Result<Point> {
            let x = if point.x.to_bits() == west.to_bits() {
                -180.0
            } else if point.x.to_bits() == east.to_bits() {
                180.0
            } else {
                (point.x - west - axis.half_period()) * longitude_scale
            };
            point.with_xy(x, chart_latitude(point.y, frame, latitude_scale)?)
        };
        for ([start, end], [source_start, source_end]) in
            unwrapped.segment_pairs().zip(ring.segment_pairs())
        {
            let start_chart = band_point(start)?;
            let end_chart = band_point(end)?;
            append_source_chart_edge(
                &mut points,
                [source_start, source_end],
                start_chart,
                end_chart,
                frame,
                longitude_scale,
                latitude_scale,
                metric_access,
                false,
                start.y.to_bits() == end.y.to_bits(),
                Some((start.x, end.x, west)),
                member,
                capture.as_deref_mut(),
                lineage_vertices.as_mut(),
                &mut lineage_edges,
            )?;
        }
    } else {
        for [start, end] in ring.segment_pairs() {
            let start_chart = chart_point(start, frame, longitude_scale, latitude_scale)?;
            let end_chart = chart_point(end, frame, longitude_scale, latitude_scale)?;
            append_source_chart_edge(
                &mut points,
                [start, end],
                start_chart,
                end_chart,
                frame,
                longitude_scale,
                latitude_scale,
                metric_access,
                true,
                true,
                None,
                member,
                capture.as_deref_mut(),
                lineage_vertices.as_mut(),
                &mut lineage_edges,
            )?;
        }
    }
    let ring = Ring::try_from_closed(points.finish()?)?;
    let lineage = match (member, lineage_vertices) {
        (Some(member), Some(mut vertices)) => {
            if let (Some(first), Some(last)) = (vertices.first().cloned(), vertices.last_mut()) {
                let mut closed = first;
                closed.extend(last.iter().copied());
                closed.sort_unstable();
                closed.dedup();
                if let Some(first) = vertices.first_mut() {
                    first.clone_from(&closed);
                }
                if let Some(last) = vertices.last_mut() {
                    *last = closed;
                }
            }
            Some(super::super::lineage::LineageSequence {
                member,
                vertices,
                edges: lineage_edges,
            })
        },
        _ => None,
    };
    Ok((ring, lineage))
}

#[cfg(test)]
pub(super) fn chart_polygon<M: ChartMetric>(
    polygon: &Polygon,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Polygon> {
    chart_polygon_captured(
        polygon,
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
        None,
        None,
    )
    .map(|(polygon, _)| polygon)
}

fn chart_polygon_captured<M: ChartMetric>(
    polygon: &Polygon,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    member: Option<super::super::lineage::MemberId>,
    mut capture: Option<&mut SplitCapture>,
) -> Result<(Polygon, Option<super::super::lineage::LineagePolygon>)> {
    let member = match capture.as_deref_mut() {
        Some(capture) => Some(capture.next_member()?),
        None => member,
    };
    let (shell, shell_lineage) = chart_ring_captured(
        &polygon.shell,
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
        member,
        capture.as_deref_mut(),
    )?;
    let mut holes = Vec::new();
    let mut hole_lineages = Vec::new();
    holes.reserve_exact(polygon.holes.len());
    hole_lineages.reserve_exact(polygon.holes.len());
    for hole in polygon.holes.iter() {
        let (hole, lineage) = chart_ring_captured(
            hole,
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
            member,
            capture.as_deref_mut(),
        )?;
        holes.push(hole);
        if let Some(lineage) = lineage {
            hole_lineages.push(lineage);
        }
    }
    let lineage = shell_lineage.map(|shell| super::super::lineage::LineagePolygon {
        shell,
        holes: hole_lineages,
    });
    Ok((Polygon::new(shell, holes), lineage))
}

pub(super) fn chart_shape<M: ChartMetric>(
    shape: &Shape,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    mut capture: Option<&mut SplitCapture>,
    memo: &mut ShapeTransformMemo,
) -> Result<(Shape, Option<super::super::lineage::ShapeLineage>)> {
    match shape {
        Shape::Polygon(polygon) => {
            let (polygon, lineage) = chart_polygon_captured(
                polygon,
                frame,
                longitude_scale,
                latitude_scale,
                metric_access,
                None,
                capture.as_deref_mut(),
            )?;
            Ok((
                Shape::Polygon(polygon),
                lineage.map(super::super::lineage::ShapeLineage::Polygon),
            ))
        },
        Shape::MultiPolygon(polygons) => {
            let mut members = Vec::new();
            members.reserve_exact(polygons.members().len());
            let mut lineages = Vec::with_capacity(polygons.member_count());
            for member in polygons.members_with_values() {
                match member {
                    MultiMember::Empty => {
                        members.push(MultiMember::Empty);
                        lineages.push(None);
                    },
                    MultiMember::Value(polygon) => {
                        let (polygon, lineage) = chart_polygon_captured(
                            polygon,
                            frame,
                            longitude_scale,
                            latitude_scale,
                            metric_access,
                            None,
                            capture.as_deref_mut(),
                        )?;
                        members.push(MultiMember::Value(polygon));
                        lineages.push(lineage);
                    },
                }
            }
            let chart = Shape::MultiPolygon(MultiPolygon::try_new(polygons.axes(), members)?);
            let lineage = capture
                .map(|_| super::super::lineage::ShapeLineage::MultiPolygon { members: lineages });
            Ok((chart, lineage))
        },
        Shape::GeometryCollection(collection) => {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some(chart) = memo.get(owner) {
                return Ok(chart);
            }
            let capture_enabled = capture.is_some();
            let mut lineages = Vec::with_capacity(collection.len());
            let chart = Shape::GeometryCollection(collection.try_map_members(|member| {
                let (chart, lineage) = chart_shape(
                    member,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    metric_access,
                    capture.as_deref_mut(),
                    memo,
                )?;
                if capture_enabled {
                    lineages.push(lineage.ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "captured collection member has no split lineage".into(),
                        )
                    })?);
                }
                Ok::<Shape, crate::error::Error>(chart)
            })?);
            let lineage = capture_enabled.then_some(
                super::super::lineage::ShapeLineage::GeometryCollection(lineages),
            );
            memo.insert(owner, chart.clone(), lineage.clone());
            Ok((chart, lineage))
        },
        Shape::LineString(line) => {
            let (line, lineage) = chart_line_captured(
                line,
                frame,
                longitude_scale,
                latitude_scale,
                metric_access,
                None,
                capture.as_deref_mut(),
            )?;
            Ok((
                Shape::LineString(line),
                lineage.map(super::super::lineage::ShapeLineage::LineString),
            ))
        },
        Shape::MultiLineString(lines) => {
            let mut members = Vec::with_capacity(lines.member_count());
            let mut lineages = Vec::with_capacity(lines.member_count());
            for member in lines.members_with_values() {
                match member {
                    MultiMember::Empty => {
                        members.push(MultiMember::Empty);
                        lineages.push(None);
                    },
                    MultiMember::Value(line) => {
                        let (line, lineage) = chart_line_captured(
                            line,
                            frame,
                            longitude_scale,
                            latitude_scale,
                            metric_access,
                            None,
                            capture.as_deref_mut(),
                        )?;
                        members.push(MultiMember::Value(line));
                        lineages.push(lineage);
                    },
                }
            }
            let chart = Shape::MultiLineString(MultiLineString::try_new(lines.axes(), members)?);
            let lineage = capture.map(|_| super::super::lineage::ShapeLineage::MultiLineString {
                members: lineages,
            });
            Ok((chart, lineage))
        },
        Shape::Point(point) => {
            let member = match capture.as_deref_mut() {
                Some(capture) => Some(capture.next_member()?),
                None => None,
            };
            let root = match capture.as_deref_mut() {
                Some(capture) => {
                    Some(capture.point_root(member.expect("captured point has a member"), *point)?)
                },
                None => None,
            };
            let chart = chart_point(*point, frame, longitude_scale, latitude_scale)?;
            let lineage = match (capture.as_deref_mut(), root, member) {
                (Some(capture), Some(root), Some(member)) => {
                    let event = capture.record_root_point(root, member, &ExactRational::zero())?;
                    Some(super::super::lineage::ShapeLineage::Point {
                        member,
                        events: vec![event],
                    })
                },
                _ => None,
            };
            Ok((Shape::Point(chart), lineage))
        },
        Shape::MultiPoint(points) => {
            let member = match capture.as_deref_mut() {
                Some(capture) => Some(capture.next_member()?),
                None => None,
            };
            let mut chart_points = Vec::with_capacity(points.len());
            let mut events = Vec::with_capacity(points.len());
            for point in points.points() {
                let root = match capture.as_deref_mut() {
                    Some(capture) => Some(
                        capture
                            .point_root(member.expect("captured multipoint has a member"), point)?,
                    ),
                    None => None,
                };
                let chart = chart_point(point, frame, longitude_scale, latitude_scale)?;
                let event = match (capture.as_deref_mut(), root, member) {
                    (Some(capture), Some(root), Some(member)) => {
                        Some(capture.record_root_point(root, member, &ExactRational::zero())?)
                    },
                    _ => None,
                };
                chart_points.push(chart);
                events.push(event.into_iter().collect());
            }
            let chart = Shape::MultiPoint(points.with_values(&chart_points)?);
            let lineage = member
                .map(|member| super::super::lineage::ShapeLineage::MultiPoint { member, events });
            Ok((chart, lineage))
        },
        Shape::Empty(..) => {
            let chart = shape.map_xy_with(|x, y| {
                Ok::<(f64, f64), crate::error::Error>((
                    chart_longitude(x, frame, longitude_scale)?,
                    chart_latitude(y, frame, latitude_scale)?,
                ))
            })?;
            Ok((
                chart,
                capture.map(|_| super::super::lineage::ShapeLineage::Empty),
            ))
        },
    }
}

pub(super) fn map_xy_with_shared_owners(
    shape: &Shape,
    map: &impl Fn(f64, f64) -> Result<(f64, f64)>,
    memo: &mut ShapeTransformMemo,
) -> Result<Shape> {
    match shape {
        Shape::GeometryCollection(collection) => {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some((mapped, _)) = memo.get(owner) {
                return Ok(mapped);
            }
            let mapped = Shape::GeometryCollection(
                collection
                    .try_map_members(|member| map_xy_with_shared_owners(member, map, memo))?,
            );
            memo.insert(owner, mapped.clone(), None);
            Ok(mapped)
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => shape.map_xy_with(map),
    }
}
