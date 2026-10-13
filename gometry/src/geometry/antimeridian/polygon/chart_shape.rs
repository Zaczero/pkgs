use super::chart_events::{
    ChartSourceCapture, append_source_edge_events, chart_longitude, chart_point,
};
use super::*;

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
    )
}

fn chart_line_captured<M: ChartMetric>(
    line: &LineSeq,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    mut capture: Option<&mut SplitCapture>,
) -> Result<LineSeq> {
    let mut start = line.point_at(0);
    let mut start_chart = chart_point(start, frame, longitude_scale, latitude_scale)?;
    let mut chart = CoordSeqBuilder::with_capacity(start.axes(), line.len());
    chart.push(start_chart);
    for end in line.iter_coords().skip(1) {
        let source_root = match capture.as_deref_mut() {
            Some(capture) => Some(capture.next_source_root()?),
            None => None,
        };
        let mut source_capture = ChartSourceCapture::new(source_root, capture.as_deref_mut());
        source_capture.record(source_root, start_chart, &ExactRational::zero())?;
        append_source_edge_events(
            &mut chart,
            &[start, end],
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
            true,
            true,
            None,
            &mut source_capture,
        )?;
        start_chart = chart_point(end, frame, longitude_scale, latitude_scale)?;
        source_capture.record(source_root, start_chart, &ExactRational::one())?;
        chart.push(start_chart);
        start = end;
    }
    Ok(LineSeq::from_proven(chart.finish()?))
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
    )
}

fn chart_ring_captured<M: ChartMetric>(
    ring: &Ring,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    mut capture: Option<&mut SplitCapture>,
) -> Result<Ring> {
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
            let source_root = match capture.as_deref_mut() {
                Some(capture) => Some(capture.next_source_root()?),
                None => None,
            };
            let start_chart = band_point(start)?;
            if points.len() == 0 {
                points.push(start_chart);
            }
            let mut source_capture = ChartSourceCapture::new(source_root, capture.as_deref_mut());
            source_capture.record(source_root, start_chart, &ExactRational::zero())?;
            append_source_edge_events(
                &mut points,
                &[source_start, source_end],
                frame,
                longitude_scale,
                latitude_scale,
                metric_access,
                false,
                start.y.to_bits() == end.y.to_bits(),
                Some((start.x, end.x, west)),
                &mut source_capture,
            )?;
            let end_chart = band_point(end)?;
            source_capture.record(source_root, end_chart, &ExactRational::one())?;
            points.push(end_chart);
        }
    } else {
        for [start, end] in ring.segment_pairs() {
            let source_root = match capture.as_deref_mut() {
                Some(capture) => Some(capture.next_source_root()?),
                None => None,
            };
            let start_chart = chart_point(start, frame, longitude_scale, latitude_scale)?;
            if points.len() == 0 {
                points.push(start_chart);
            }
            let mut source_capture = ChartSourceCapture::new(source_root, capture.as_deref_mut());
            source_capture.record(source_root, start_chart, &ExactRational::zero())?;
            append_source_edge_events(
                &mut points,
                &[start, end],
                frame,
                longitude_scale,
                latitude_scale,
                metric_access,
                true,
                true,
                None,
                &mut source_capture,
            )?;
            let end_chart = chart_point(end, frame, longitude_scale, latitude_scale)?;
            source_capture.record(source_root, end_chart, &ExactRational::one())?;
            points.push(end_chart);
        }
    }
    Ring::try_from_closed(points.finish()?)
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
    )
}

fn chart_polygon_captured<M: ChartMetric>(
    polygon: &Polygon,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    mut capture: Option<&mut SplitCapture>,
) -> Result<Polygon> {
    let shell = chart_ring_captured(
        &polygon.shell,
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
        capture.as_deref_mut(),
    )?;
    let mut holes = Vec::new();
    holes.reserve_exact(polygon.holes.len());
    for hole in polygon.holes.iter() {
        holes.push(chart_ring_captured(
            hole,
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
            capture.as_deref_mut(),
        )?);
    }
    Ok(Polygon::new(shell, holes))
}

pub(super) fn chart_shape<M: ChartMetric>(
    shape: &Shape,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    mut capture: Option<&mut SplitCapture>,
    memo: &mut ShapeTransformMemo,
) -> Result<Shape> {
    match shape {
        Shape::Polygon(polygon) => Ok(Shape::Polygon(chart_polygon_captured(
            polygon,
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
            capture.as_deref_mut(),
        )?)),
        Shape::MultiPolygon(polygons) => {
            let mut members = Vec::new();
            members.reserve_exact(polygons.members().len());
            for member in polygons.members_with_values() {
                members.push(match member {
                    MultiMember::Empty => MultiMember::Empty,
                    MultiMember::Value(polygon) => MultiMember::Value(chart_polygon_captured(
                        polygon,
                        frame,
                        longitude_scale,
                        latitude_scale,
                        metric_access,
                        capture.as_deref_mut(),
                    )?),
                });
            }
            Ok(Shape::MultiPolygon(MultiPolygon::try_new(
                polygons.axes(),
                members,
            )?))
        },
        Shape::GeometryCollection(collection) => {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some(chart) = memo.get(owner) {
                return Ok(chart);
            }
            let chart = Shape::GeometryCollection(collection.try_map_members(|member| {
                chart_shape(
                    member,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    metric_access,
                    capture.as_deref_mut(),
                    memo,
                )
            })?);
            memo.insert(owner, chart.clone());
            Ok(chart)
        },
        Shape::LineString(line) => Ok(Shape::LineString(chart_line_captured(
            line,
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
            capture.as_deref_mut(),
        )?)),
        Shape::MultiLineString(lines) => {
            let mut members = Vec::with_capacity(lines.member_count());
            for member in lines.members_with_values() {
                members.push(match member {
                    MultiMember::Empty => MultiMember::Empty,
                    MultiMember::Value(line) => MultiMember::Value(chart_line_captured(
                        line,
                        frame,
                        longitude_scale,
                        latitude_scale,
                        metric_access,
                        capture.as_deref_mut(),
                    )?),
                });
            }
            Ok(Shape::MultiLineString(MultiLineString::try_new(
                lines.axes(),
                members,
            )?))
        },
        Shape::Point(point) => {
            let root = match capture.as_deref_mut() {
                Some(capture) => Some(capture.next_source_root()?),
                None => None,
            };
            let chart = chart_point(*point, frame, longitude_scale, latitude_scale)?;
            if let (Some(capture), Some(root)) = (capture.as_deref_mut(), root) {
                capture.record_root_point(root, chart, &ExactRational::zero())?;
            }
            Ok(Shape::Point(chart))
        },
        Shape::MultiPoint(points) => {
            let mut chart_points = Vec::with_capacity(points.len());
            for point in points.points() {
                let root = match capture.as_deref_mut() {
                    Some(capture) => Some(capture.next_source_root()?),
                    None => None,
                };
                let chart = chart_point(point, frame, longitude_scale, latitude_scale)?;
                if let (Some(capture), Some(root)) = (capture.as_deref_mut(), root) {
                    capture.record_root_point(root, chart, &ExactRational::zero())?;
                }
                chart_points.push(chart);
            }
            Ok(Shape::MultiPoint(points.with_values(&chart_points)?))
        },
        Shape::Empty(..) => shape.map_xy_with(|x, y| {
            Ok((
                chart_longitude(x, frame, longitude_scale)?,
                chart_latitude(y, frame, latitude_scale)?,
            ))
        }),
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
            if let Some(mapped) = memo.get(owner) {
                return Ok(mapped);
            }
            let mapped = Shape::GeometryCollection(
                collection
                    .try_map_members(|member| map_xy_with_shared_owners(member, map, memo))?,
            );
            memo.insert(owner, mapped.clone());
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
