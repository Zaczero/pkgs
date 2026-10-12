use super::chart_events::{append_source_edge_events, chart_longitude, chart_point};
use super::*;

pub(super) fn chart_line<M: ChartMetric>(
    line: &LineSeq,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<LineSeq> {
    let mut start = line.point_at(0);
    let mut chart = CoordSeqBuilder::with_capacity(start.axes(), line.len());
    chart.push(chart_point(start, frame, longitude_scale, latitude_scale)?);
    for end in line.iter_coords().skip(1) {
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
        )?;
        chart.push(chart_point(end, frame, longitude_scale, latitude_scale)?);
        start = end;
    }
    Ok(LineSeq::from_proven(chart.finish()?))
}

pub(super) fn chart_ring<M: ChartMetric>(
    ring: &Ring,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
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
            if points.len() == 0 {
                points.push(band_point(start)?);
            }
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
            )?;
            points.push(band_point(end)?);
        }
    } else {
        for [start, end] in ring.segment_pairs() {
            if points.len() == 0 {
                points.push(chart_point(start, frame, longitude_scale, latitude_scale)?);
            }
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
            )?;
            points.push(chart_point(end, frame, longitude_scale, latitude_scale)?);
        }
    }
    Ring::try_from_closed(points.finish()?)
}

pub(super) fn chart_polygon<M: ChartMetric>(
    polygon: &Polygon,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Polygon> {
    let shell = chart_ring(
        &polygon.shell,
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
    )?;
    let mut holes = Vec::new();
    holes.reserve_exact(polygon.holes.len());
    for hole in polygon.holes.iter() {
        holes.push(chart_ring(
            hole,
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
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
    memo: &mut ShapeTransformMemo,
) -> Result<Shape> {
    match shape {
        Shape::Polygon(polygon) => Ok(Shape::Polygon(chart_polygon(
            polygon,
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
        )?)),
        Shape::MultiPolygon(polygons) => {
            let mut members = Vec::new();
            members.reserve_exact(polygons.members().len());
            for member in polygons.members_with_values() {
                members.push(match member {
                    MultiMember::Empty => MultiMember::Empty,
                    MultiMember::Value(polygon) => MultiMember::Value(chart_polygon(
                        polygon,
                        frame,
                        longitude_scale,
                        latitude_scale,
                        metric_access,
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
                    memo,
                )
            })?);
            memo.insert(owner, chart.clone());
            Ok(chart)
        },
        Shape::LineString(line) => Ok(Shape::LineString(chart_line(
            line,
            frame,
            longitude_scale,
            latitude_scale,
            metric_access,
        )?)),
        Shape::MultiLineString(lines) => {
            let mut members = Vec::with_capacity(lines.member_count());
            for member in lines.members_with_values() {
                members.push(match member {
                    MultiMember::Empty => MultiMember::Empty,
                    MultiMember::Value(line) => MultiMember::Value(chart_line(
                        line,
                        frame,
                        longitude_scale,
                        latitude_scale,
                        metric_access,
                    )?),
                });
            }
            Ok(Shape::MultiLineString(MultiLineString::try_new(
                lines.axes(),
                members,
            )?))
        },
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => shape.map_xy_with(|x, y| {
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
