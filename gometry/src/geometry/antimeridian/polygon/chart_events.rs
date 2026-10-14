use super::*;
use crate::geometry::ordinates::{NodeOrdinates, RootId, resolve_source_root};
use crate::work::Unmetered;

pub(in crate::geometry::antimeridian) fn chart_point(
    point: Point,
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
) -> Result<Point> {
    point.with_xy(
        chart_longitude(point.x, frame, longitude_scale)?,
        chart_latitude(point.y, frame, latitude_scale)?,
    )
}

pub(super) fn chart_longitude(value: f64, frame: GeographicFrame, scale: f64) -> Result<f64> {
    let axis = frame.longitude();
    let remainder = value % axis.period();
    if remainder.to_bits() == axis.half_period().to_bits() {
        return Ok(180.0);
    }
    axis.canonicalize(value)
        .map(|longitude| longitude.get() * scale)
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

pub(in crate::geometry::antimeridian) fn chart_latitude(
    value: f64,
    frame: GeographicFrame,
    scale: f64,
) -> Result<f64> {
    Latitude::try_new(frame, value)
        .map(|latitude| latitude.get() * scale)
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

#[derive(Clone)]
pub(super) struct ChartEdgeEvent {
    pub(super) source: [Point; 2],
    pub(super) longitude: RetainedChartDisplacement,
    pub(super) parameter: ExactRational,
    pub(super) seam_longitude: Option<f64>,
    pub(super) source_root: Option<RootId>,
}

pub(super) struct ChartSourceCapture<'a> {
    root: Option<RootId>,
    member: Option<super::super::lineage::MemberId>,
    capture: Option<&'a mut SplitCapture>,
    records: Vec<(super::super::lineage::EventId, ExactRational)>,
}

impl<'a> ChartSourceCapture<'a> {
    pub(super) const fn new(
        root: Option<RootId>,
        member: Option<super::super::lineage::MemberId>,
        capture: Option<&'a mut SplitCapture>,
    ) -> Self {
        Self {
            root,
            member,
            capture,
            records: Vec::new(),
        }
    }

    pub(super) fn record(
        &mut self,
        source_root: Option<RootId>,
        parameter: &ExactRational,
    ) -> Result<Option<super::super::lineage::EventId>> {
        if let (Some(capture), Some(root), Some(member)) =
            (self.capture.as_deref_mut(), source_root, self.member)
        {
            let event = capture.record_root_point(root, member, parameter)?;
            self.records.push((
                event,
                parameter.try_clone_with_work(&mut crate::work::Unmetered)?,
            ));
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }

    pub(super) fn resolve_root<M: ChartMetric>(
        &mut self,
        root: RootId,
        parameter: &ExactRational,
        axes: CoordinateAxes,
        frame: GeographicFrame,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<NodeOrdinates> {
        let capture = self.capture.as_deref_mut().ok_or_else(|| {
            GeometryErrorKind::Invalid("captured chart source root has no lineage catalog".into())
        })?;
        capture
            .lineage_mut()
            .resolve_root(root, parameter, axes, frame, metric_access)
    }

    pub(super) fn event_ids(&self) -> Vec<super::super::lineage::EventId> {
        self.records.iter().map(|(event, _)| *event).collect()
    }

    pub(super) fn edge_uses(&mut self) -> Result<Vec<super::super::lineage::EdgeUseId>> {
        let (Some(root), Some(member), Some(capture)) =
            (self.root, self.member, self.capture.as_deref_mut())
        else {
            return Ok(Vec::new());
        };
        self.records
            .array_windows::<2>()
            .map(|[(_, start), (_, end)]| capture.source_edge_use(member, root, start, end))
            .collect()
    }
}

pub(super) fn chart_connection(
    start: Point,
    end: Point,
    frame: GeographicFrame,
) -> Result<ChartConnection> {
    chart_connection_at(start.x, end.x, frame)
}

fn chart_connection_at(start: f64, end: f64, frame: GeographicFrame) -> Result<ChartConnection> {
    frame
        .longitude()
        .chart_connection(start, end)
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

pub(super) fn certified_no_chart_events(start: f64, end: f64, frame: GeographicFrame) -> bool {
    let half_period = frame.longitude().half_period();
    let start_abs = start.abs();
    let end_abs = end.abs();
    if !start.is_finite() || !end.is_finite() || start_abs >= half_period || end_abs >= half_period
    {
        return false;
    }
    if start == 0.0 || end == 0.0 || start.is_sign_negative() == end.is_sign_negative() {
        return true;
    }
    // Round the remaining gap down so a subtraction near the seam falls back.
    start_abs < (half_period - end_abs).next_down()
}

pub(super) fn line_has_certified_no_chart_events(
    line: &LineSeq,
    frame: GeographicFrame,
    longitude_scale: f64,
) -> bool {
    let mut points = line.iter_coords();
    let Some(mut previous) = points.next() else {
        return true;
    };
    let mut previous_chart_x = previous.x * longitude_scale;
    for current in points {
        if !certified_no_chart_events(previous.x, current.x, frame) {
            return false;
        }
        let current_chart_x = current.x * longitude_scale;
        if !certified_no_chart_events(previous_chart_x, current_chart_x, GeographicFrame::crs84()) {
            return false;
        }
        previous = current;
        previous_chart_x = current_chart_x;
    }
    true
}

pub(super) fn lineal_shape_has_certified_no_chart_events(
    shape: &Shape,
    frame: GeographicFrame,
    longitude_scale: f64,
) -> bool {
    match shape {
        Shape::LineString(line) => line_has_certified_no_chart_events(line, frame, longitude_scale),
        Shape::MultiLineString(lines) => lines.members_with_values().all(|member| match member {
            MultiMember::Empty => true,
            MultiMember::Value(line) => {
                line_has_certified_no_chart_events(line, frame, longitude_scale)
            },
        }),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => false,
    }
}

pub(super) fn seam_event(
    source: &[Point; 2],
    longitude: RetainedChartDisplacement,
    frame: GeographicFrame,
    source_root: Option<RootId>,
) -> Result<Option<ChartEdgeEvent>> {
    let axis = frame.longitude();
    let [start, end] = longitude.endpoints();
    let start = ExactDyadic::from_f64(start.get());
    let branch = ExactDyadic::from_f64(axis.period())
        .try_product(&ExactDyadic::from_i64(longitude.periods()))?;
    let lifted_end = ExactDyadic::from_f64(end.get()).try_add(branch)?;
    let delta = ExactDyadic::try_subtract_refs(&lifted_end, &start)?;
    let east_seam = ExactDyadic::from_f64(axis.half_period());
    let west_seam = east_seam.clone().neg();
    let crossing = if delta.cmp(&ExactDyadic::zero()).is_gt()
        && start.cmp(&east_seam).is_lt()
        && lifted_end.cmp(&east_seam).is_gt()
    {
        Some((east_seam, 180.0))
    } else if delta.cmp(&ExactDyadic::zero()).is_lt()
        && start.cmp(&west_seam).is_gt()
        && lifted_end.cmp(&west_seam).is_lt()
    {
        Some((west_seam, -180.0))
    } else {
        None
    };
    crossing
        .map(|(seam, native_side)| {
            let numerator = seam.try_subtract(start)?;
            let parameter = ExactRational::new(numerator, delta)?;
            Ok(ChartEdgeEvent {
                source: *source,
                longitude,
                parameter,
                seam_longitude: Some(native_side * frame.longitude_orientation_sign()),
                source_root,
            })
        })
        .transpose()
}

fn insert_chart_event(events: &mut Vec<ChartEdgeEvent>, event: ChartEdgeEvent) -> Result<()> {
    let mut index = events.len();
    for (previous_index, previous) in events.iter_mut().enumerate().rev() {
        match event.parameter.try_cmp(&previous.parameter)? {
            std::cmp::Ordering::Less => index = previous_index,
            std::cmp::Ordering::Equal => {
                if event.seam_longitude.is_some() {
                    previous.seam_longitude = event.seam_longitude;
                }
                return Ok(());
            },
            std::cmp::Ordering::Greater => break,
        }
    }
    events.insert(index, event);
    Ok(())
}

fn source_ordinates_at<M: ChartMetric>(
    event: &ChartEdgeEvent,
    frame: GeographicFrame,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    root_metric: &mut RootMetricState,
    source_capture: &mut ChartSourceCapture<'_>,
) -> Result<(ZOrdinate, MOrdinate)> {
    let axes = event.source[0].axes().union(event.source[1].axes());
    let ordinates = if let Some(root) = event.source_root {
        source_capture.resolve_root(root, &event.parameter, axes, frame, metric_access)?
    } else {
        let mut fraction = None;
        let mut work = Unmetered;
        resolve_source_root(
            frame,
            &event.source,
            event.longitude,
            &event.parameter,
            axes,
            metric_access,
            root_metric,
            &mut fraction,
            &mut work,
        )?
    };
    Ok((
        ZOrdinate(ordinates.z_value()),
        MOrdinate(ordinates.m_value()),
    ))
}

pub(super) fn append_source_edge_events<M: ChartMetric>(
    output: &mut CoordSeqBuilder,
    source: &[Point; 2],
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    include_seam: bool,
    circle_witnesses: bool,
    band: Option<(f64, f64, f64)>,
    source_capture: &mut ChartSourceCapture<'_>,
) -> Result<()> {
    if include_seam && certified_no_chart_events(source[0].x, source[1].x, frame) {
        return Ok(());
    }
    let source_connection = chart_connection(source[0], source[1], frame)?;
    let connection = if source_connection.step.is_circle()
        || (source_connection.start.get().to_bits() == source[0].x.to_bits()
            && source_connection.end.get().to_bits() == source[1].x.to_bits())
    {
        source_connection
    } else {
        chart_connection_at(
            source_connection.start.get(),
            source_connection.end.get(),
            frame,
        )?
    };
    let longitude = connection.retained();
    let mut events = Vec::new();
    if circle_witnesses && connection.step.is_circle() {
        events.reserve_exact(if include_seam { 4 } else { 3 });
        for quarter in 1_i64..=3 {
            insert_chart_event(&mut events, ChartEdgeEvent {
                source: *source,
                longitude,
                parameter: ExactRational::new(
                    ExactDyadic::from_i64(quarter),
                    ExactDyadic::from_i64(4),
                )?,
                seam_longitude: None,
                source_root: source_capture.root,
            })?;
        }
    }
    if include_seam && let Some(event) = seam_event(source, longitude, frame, source_capture.root)?
    {
        insert_chart_event(&mut events, event)?;
    }
    if events.is_empty() {
        return Ok(());
    }
    output.reserve_hint(events.len());
    let mut root_metric = RootMetricState::default();
    for event in events {
        let parameter = event.parameter.round()?;
        let x = if let Some((start, end, west)) = band {
            let lifted = interpolate_subdivision_ordinate(start, end, parameter);
            (lifted - west - frame.longitude().half_period()) * longitude_scale
        } else if let Some(seam_longitude) = event.seam_longitude {
            seam_longitude
        } else {
            let site = frame
                .longitude()
                .native_site_at(&event.longitude, parameter)?;
            chart_longitude(site.native().get(), frame, longitude_scale)?
        };
        let latitude =
            interpolate_subdivision_ordinate(event.source[0].y, event.source[1].y, parameter);
        let (z, m) = source_ordinates_at(
            &event,
            frame,
            metric_access,
            &mut root_metric,
            source_capture,
        )?;
        let point = Point::new_axes(x, chart_latitude(latitude, frame, latitude_scale)?, z, m)?;
        source_capture.record(event.source_root, &event.parameter)?;
        output.push(point);
    }
    Ok(())
}
