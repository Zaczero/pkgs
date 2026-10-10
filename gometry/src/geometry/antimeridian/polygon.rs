use std::collections::HashMap;

use crate::error::Result;
use crate::geographic::{
    ChartConnection, ChartPole, GeodeticPole, GeographicFrame, GeographicRing, GeographicViewError,
    Latitude, RetainedChartDisplacement, RingDomain, RingSummary, ScalarGeographicView,
};
use crate::geometry::antimeridian::{
    SeamRing, collect_points, ring_follows_seam, segment_coords_with_metric,
    split_ring_at_seam_with_metric,
};
use crate::geometry::ordinates::{SourceArcRequest, source_arc_ordinates};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordSeqBuilder, CoordinateAxes, Coordinates as _, GeometryErrorKind,
    LineSeq, MOrdinate, MultiLineString, MultiMember, MultiPolygon, Point, Polygon, Ring,
    RootMetricState, Shape, ShapeOwnerKey, SourceArcMetricAccess, Strictness, TopologyDomain,
    Unmetered, ZOrdinate, interpolate_subdivision_ordinate, open_point_cycle_winding,
    polygon_is_valid, ring_encloses_pole, ring_winding, same_point,
};

#[derive(Default)]
struct ShapeTransformMemo {
    owners: Option<HashMap<ShapeOwnerKey, Shape>>,
}

impl ShapeTransformMemo {
    fn get(&self, owner: Option<ShapeOwnerKey>) -> Option<Shape> {
        owner.and_then(|owner| self.owners.as_ref()?.get(&owner).cloned())
    }

    fn insert(&mut self, owner: Option<ShapeOwnerKey>, shape: Shape) {
        if let Some(owner) = owner {
            self.owners
                .get_or_insert_with(HashMap::new)
                .insert(owner, shape);
        }
    }
}

impl Shape {
    /// Validate and split in the periodic coordinate frame. Canonical chart
    /// coordinates exist only for this operation; source coordinates remain
    /// the storage authority.
    pub(crate) fn split_antimeridian_in_frame(&self, frame: GeographicFrame) -> Result<Self> {
        if frame == GeographicFrame::crs84() {
            let metric = std::sync::OnceLock::new();
            let metric_access = || {
                metric.get_or_try_init(|| {
                    let model = crate::crs::metric_model(Some("OGC:CRS84"))?;
                    crate::crs::GeodesicMetric::from_model(&model)
                })
            };
            return self.split_antimeridian_in_frame_with_metric(frame, Some(&metric_access));
        }
        self.split_antimeridian_in_frame_with_metric::<crate::crs::GeodesicMetric>(frame, None)
    }

    pub(crate) fn split_antimeridian_in_frame_with_metric<M: ChartMetric>(
        &self,
        frame: GeographicFrame,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<Self> {
        let view =
            ScalarGeographicView::try_new(frame, self).map_err(|error| split_view_error(&error))?;
        split_geographic_view(view, metric_access)
    }

    pub fn split_antimeridian(&self) -> Result<Self> {
        self.split_antimeridian_in_frame(GeographicFrame::crs84())
    }

    /// Split at the antimeridian: parts that cross ±180 come back as
    /// multiple parts whose edges follow the seam (each side keeping its
    /// own seam sign), with affine chart crossings and automatic pole closure.
    /// Geometries that do not cross are returned unchanged.
    /// The caller supplies an already-admitted canonical operation chart.
    fn split_antimeridian_chart<M: ChartMetric>(
        &self,
        memo: &mut ShapeTransformMemo,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    ) -> Result<Self> {
        match self {
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => Ok(self.clone()),
            Self::LineString(points) => {
                let pieces = segment_coords_with_metric(
                    &collect_points(points),
                    GeographicFrame::crs84(),
                    metric_access,
                )?;
                if pieces.is_empty() {
                    Ok(self.clone())
                } else {
                    Ok(Self::multi_line_string(
                        points.axes(),
                        pieces
                            .into_iter()
                            .map(|piece| CoordSeq::try_from_points(&piece))
                            // Splitting a line at meridian crossings yields
                            // pieces of at least two vertices each.
                            .map(|coords| coords.map(LineSeq::from_proven))
                            .collect::<Result<Vec<_>>>()?,
                    ))
                }
            },
            Self::MultiLineString(lines) => {
                let mut split = Vec::with_capacity(lines.member_count());
                for member in lines.members_with_values() {
                    match member {
                        MultiMember::Empty => split.push(MultiMember::Empty),
                        MultiMember::Value(line) => {
                            let pieces = segment_coords_with_metric(
                                &collect_points(line),
                                GeographicFrame::crs84(),
                                metric_access,
                            )?;
                            if pieces.is_empty() {
                                split.push(MultiMember::Value(line.clone()));
                            } else {
                                let lines = pieces
                                    .into_iter()
                                    .map(|piece| CoordSeq::try_from_points(&piece))
                                    .map(|coords| coords.map(LineSeq::from_proven))
                                    .map(|line| line.map(MultiMember::Value))
                                    .collect::<Result<Vec<_>>>()?;
                                split.extend(lines);
                            }
                        },
                    }
                }
                Ok(Self::MultiLineString(MultiLineString::try_new(
                    lines.axes(),
                    split,
                )?))
            },
            Self::Polygon(polygon) => {
                let polygons = split_polygon(self, polygon, PoleClosure::Auto, metric_access)?;
                Ok(Self::narrow_polygons(polygon.axes(), polygons))
            },
            Self::MultiPolygon(polygons) => {
                let mut split = Vec::with_capacity(polygons.member_count());
                for member in polygons.members_with_values() {
                    match member {
                        MultiMember::Empty => split.push(MultiMember::Empty),
                        MultiMember::Value(polygon) => {
                            split.extend(
                                split_polygon(self, polygon, PoleClosure::Auto, metric_access)?
                                    .into_iter()
                                    .map(MultiMember::Value),
                            );
                        },
                    }
                }
                Ok(Self::MultiPolygon(MultiPolygon::try_new(
                    polygons.axes(),
                    split,
                )?))
            },
            Self::GeometryCollection(geometries) => {
                let owner = geometries
                    .members_are_shared()
                    .then(|| ShapeOwnerKey::new(geometries.as_slice()));
                if let Some(split) = memo.get(owner) {
                    return Ok(split);
                }
                let split = Self::GeometryCollection(geometries.try_map_members(|member| {
                    member.split_antimeridian_chart(memo, metric_access)
                })?);
                memo.insert(owner, split.clone());
                Ok(split)
            },
        }
    }

    /// Split at the antimeridian forcing closure over a specific pole, for
    /// callers that already know which pole a ring encircles. A region that
    /// *contains* a pole (e.g. a polar cell coverage) has a ring edge spanning
    /// `> 180°` of longitude that the automatic inference can close over the
    /// wrong pole — producing a valid but wrong-region polygon the `Auto` retry
    /// (which only re-tries on *invalid* output) never corrects. Non-areal and
    /// non-crossing geometries behave exactly as [`Self::split_antimeridian`].
    ///
    /// Contract: each part must genuinely touch the forced pole. The closure
    /// assumes the regular seam topology of a single pole-touching ring.
    /// Forcing the wrong pole, or a ring that does not reach the forced pole,
    /// is caller error and yields garbage.
    pub fn split_antimeridian_over_pole(&self, north: bool) -> Result<Self> {
        let closure = if north {
            PoleClosure::Chart(ChartPole::North)
        } else {
            PoleClosure::Chart(ChartPole::South)
        };
        match self {
            Self::Polygon(polygon) => {
                let polygons = split_polygon(
                    self,
                    polygon,
                    closure,
                    None::<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>>,
                )?;
                Ok(Self::narrow_polygons(polygon.axes(), polygons))
            },
            Self::MultiPolygon(polygons) => {
                let mut split = Vec::with_capacity(polygons.member_count());
                for member in polygons.members_with_values() {
                    match member {
                        MultiMember::Empty => split.push(MultiMember::Empty),
                        MultiMember::Value(polygon) => {
                            split.extend(
                                split_polygon(
                                    self,
                                    polygon,
                                    closure,
                                    None::<
                                        &SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>,
                                    >,
                                )?
                                .into_iter()
                                .map(MultiMember::Value),
                            );
                        },
                    }
                }
                Ok(Self::MultiPolygon(MultiPolygon::try_new(
                    polygons.axes(),
                    split,
                )?))
            },
            _ => self.split_antimeridian(),
        }
    }
}

fn split_view_error(error: &GeographicViewError) -> crate::error::Error {
    GeometryErrorKind::antimeridian_split_failed(error.to_string())
}

fn source_longitudes_are_canonical(view: ScalarGeographicView<'_>) -> bool {
    let frame = view.frame();
    let mut canonical = true;
    view.shape().for_each_first_xy_point_run(|point| {
        let native = point.x;
        let wrapped = frame
            .longitude()
            .canonicalize(point.x)
            .expect("geographic view admitted every longitude")
            .get();
        let positive_seam = native.to_bits() == frame.longitude().half_period().to_bits();
        let ordinary_identity = native.to_bits() == wrapped.to_bits()
            || (native.to_bits() == (-0.0_f64).to_bits() && wrapped.to_bits() == 0.0_f64.to_bits());
        canonical &= positive_seam || ordinary_identity;
    });
    canonical
}

pub(crate) fn split_geographic_view<M: ChartMetric>(
    view: ScalarGeographicView<'_>,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    let frame = view.frame();
    let canonical = source_longitudes_are_canonical(view);
    if frame == GeographicFrame::crs84() && canonical && !view.shape().crosses_antimeridian() {
        return Ok(view.shape().clone());
    }
    let longitude_scale = frame.longitude().to_degrees(1.0) * frame.longitude_orientation_sign();
    let latitude_scale = frame.latitude_unit().native_to_degrees(1.0, frame.pole())
        * frame.latitude_orientation_sign();
    if canonical && lineal_shape_has_certified_no_chart_events(view.shape(), frame, longitude_scale)
    {
        return Ok(view.shape().clone());
    }
    let chart = chart_shape(
        view.shape(),
        frame,
        longitude_scale,
        latitude_scale,
        metric_access,
        &mut ShapeTransformMemo::default(),
    )?;
    if canonical
        && !crate::geometry::crosses_antimeridian_in_frame(view.shape(), frame)
        && !chart.crosses_antimeridian()
    {
        return Ok(view.shape().clone());
    }
    let split =
        chart.split_antimeridian_chart(&mut ShapeTransformMemo::default(), metric_access)?;
    map_xy_with_shared_owners(
        &split,
        &|x, y| {
            let latitude = if y == 90.0 {
                frame.native_latitude_pole(GeodeticPole::North)
            } else if y == -90.0 {
                frame.native_latitude_pole(GeodeticPole::South)
            } else {
                y / latitude_scale
            };
            Ok((x / longitude_scale, latitude))
        },
        &mut ShapeTransformMemo::default(),
    )
}

fn chart_point(
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

fn chart_longitude(value: f64, frame: GeographicFrame, scale: f64) -> Result<f64> {
    let axis = frame.longitude();
    let remainder = value % axis.period();
    if remainder.to_bits() == axis.half_period().to_bits() {
        return Ok(180.0);
    }
    axis.canonicalize(value)
        .map(|longitude| longitude.get() * scale)
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

fn chart_latitude(value: f64, frame: GeographicFrame, scale: f64) -> Result<f64> {
    Latitude::try_new(frame, value)
        .map(|latitude| latitude.get() * scale)
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

#[derive(Clone)]
struct ChartEdgeEvent {
    parameter: ExactRational,
    seam_longitude: Option<f64>,
}

fn chart_connection(start: Point, end: Point, frame: GeographicFrame) -> Result<ChartConnection> {
    chart_connection_at(start.x, end.x, frame)
}

fn chart_connection_at(start: f64, end: f64, frame: GeographicFrame) -> Result<ChartConnection> {
    frame
        .longitude()
        .chart_connection(start, end)
        .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))
}

fn certified_no_chart_events(start: f64, end: f64, frame: GeographicFrame) -> bool {
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

fn line_has_certified_no_chart_events(
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

fn lineal_shape_has_certified_no_chart_events(
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

fn seam_event(
    connection: ChartConnection,
    frame: GeographicFrame,
) -> Result<Option<ChartEdgeEvent>> {
    let longitude = connection.retained();
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
                parameter,
                seam_longitude: Some(native_side * frame.longitude_orientation_sign()),
            })
        })
        .transpose()
}

fn insert_chart_event(events: &mut Vec<ChartEdgeEvent>, event: ChartEdgeEvent) -> Result<()> {
    let mut index = events.len();
    while index > 0 {
        match event.parameter.try_cmp(&events[index - 1].parameter)? {
            std::cmp::Ordering::Less => index -= 1,
            std::cmp::Ordering::Equal => {
                if event.seam_longitude.is_some() {
                    events[index - 1].seam_longitude = event.seam_longitude;
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
    source: &[Point; 2],
    frame: GeographicFrame,
    longitude: RetainedChartDisplacement,
    parameter: &ExactRational,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    root_metric: &mut RootMetricState,
) -> Result<(ZOrdinate, MOrdinate)> {
    let axes = source[0].axes().union(source[1].axes());
    let request = SourceArcRequest::new(frame, source, axes, parameter, || Ok(longitude))?;
    let mut fraction = None;
    let mut work = Unmetered;
    let ordinates = source_arc_ordinates(
        &request,
        || {
            let access = metric_access.ok_or_else(|| {
                crate::crs::CrsError::invalid(
                    "geographic antimeridian source ordinates require the operation's CRS metric",
                )
            })?;
            access()
        },
        root_metric,
        &mut fraction,
        &mut work,
    )?;
    Ok((
        ZOrdinate(ordinates.z.value()),
        MOrdinate(ordinates.m.value()),
    ))
}

fn append_source_edge_events<M: ChartMetric>(
    output: &mut CoordSeqBuilder,
    source: [Point; 2],
    frame: GeographicFrame,
    longitude_scale: f64,
    latitude_scale: f64,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    include_seam: bool,
    circle_witnesses: bool,
    band: Option<(f64, f64, f64)>,
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
                parameter: ExactRational::new(
                    ExactDyadic::from_i64(quarter),
                    ExactDyadic::from_i64(4),
                )?,
                seam_longitude: None,
            })?;
        }
    }
    if include_seam && let Some(event) = seam_event(connection, frame)? {
        insert_chart_event(&mut events, event)?;
    }
    if events.is_empty() {
        return Ok(());
    }
    output.reserve_hint(events.len() + 1);
    let mut root_metric = RootMetricState::default();
    for event in events {
        let parameter = event.parameter.round()?;
        let x = if let Some((start, end, west)) = band {
            let lifted = interpolate_subdivision_ordinate(start, end, parameter);
            (lifted - west - frame.longitude().half_period()) * longitude_scale
        } else if let Some(seam_longitude) = event.seam_longitude {
            seam_longitude
        } else {
            let site = frame.longitude().native_site_at(&longitude, parameter)?;
            chart_longitude(site.native().get(), frame, longitude_scale)?
        };
        let latitude = interpolate_subdivision_ordinate(source[0].y, source[1].y, parameter);
        let (z, m) = source_ordinates_at(
            &source,
            frame,
            longitude,
            &event.parameter,
            metric_access,
            &mut root_metric,
        )?;
        output.push(Point::new_axes(
            x,
            chart_latitude(latitude, frame, latitude_scale)?,
            z,
            m,
        )?);
    }
    Ok(())
}

fn chart_line<M: ChartMetric>(
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
            [start, end],
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

fn chart_ring<M: ChartMetric>(
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
                [source_start, source_end],
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
                [start, end],
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

fn chart_polygon<M: ChartMetric>(
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

fn chart_shape<M: ChartMetric>(
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

fn map_xy_with_shared_owners(
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

/// A pole-closure cap fabricates ±90 vertices with no source segment, so a
/// measured/3D input cannot honestly keep its ordinates there. Reject Z/M only
/// when such a cap vertex ACTUALLY survived into the assembled output — a plain
/// seam crossing (no pole) fabricates none and its seam vertices interpolate
/// fine, so it must not be rejected. (The check is post-assembly because the
/// split speculatively explores both pole closures before discarding the cap.)
#[expect(
    clippy::float_cmp,
    reason = "cap vertices are the exact literals ±90; any other latitude is real data"
)]
fn reject_fabricated_pole_ordinates(source: &Shape, polygons: &[Polygon]) -> Result<()> {
    if source.axes() == CoordinateAxes::XY {
        return Ok(());
    }
    let fabricated_pole_vertex = polygons.iter().any(|polygon| {
        std::iter::once(polygon.shell.coords())
            .chain(polygon.holes.iter().map(Ring::coords))
            .any(|ring| {
                ring.iter_coords()
                    .any(|coord| coord.y == 90.0 || coord.y == -90.0)
            })
    });
    if fabricated_pole_vertex {
        Err(GeometryErrorKind::AntimeridianPoleOrdinates.into())
    } else {
        Ok(())
    }
}

fn ring_is_ccw(coords: &[Point]) -> bool {
    if coords.len() >= 2
        && coords
            .first()
            .zip(coords.last())
            .is_some_and(|(first, last)| same_point(*first, *last))
    {
        ring_winding(coords).is_ccw()
    } else {
        open_point_cycle_winding(coords).is_ccw()
    }
}

/// Split one polygon into its seam-following parts with silent winding fixes.
/// `preferred` forces the pole-closure direction (the caller knows which pole
/// the ring encircles); `Auto` infers it and retries the other direction only
/// if the inference is invalid. Z/M is rejected only when a pole cap actually
/// survives into the output (see [`reject_fabricated_pole_ordinates`]).
fn split_polygon<M: ChartMetric>(
    source: &Shape,
    polygon: &Polygon,
    preferred: PoleClosure,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    let preferred = match preferred {
        PoleClosure::Auto => pole_closure_for_ring(&polygon.shell),
        closure => closure,
    };
    let polygons = split_polygon_inner(polygon, preferred, metric_access)?;
    reject_fabricated_pole_ordinates(source, &polygons)?;
    Ok(polygons)
}

fn split_polygon_inner<M: ChartMetric>(
    polygon: &Polygon,
    preferred: PoleClosure,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    split_polygon_inner_in_mode(polygon, preferred, HoleOverlayMode::Select, metric_access)
}

fn split_polygon_inner_in_mode<M: ChartMetric>(
    polygon: &Polygon,
    preferred: PoleClosure,
    mut overlay_mode: HoleOverlayMode,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Vec<Polygon>> {
    // Split crossing holes as independent areal operands, then subtract them
    // from the split shell.  Treating shell and hole seam fragments as one
    // undifferentiated stitching pool works for ordinary crossings, but it
    // cannot represent nested polar caps: both rings independently require a
    // pole closure, and the pooled closest-start walk joins them into invalid
    // self-intersecting rings. Set difference is the structural operation the
    // source polygon already expresses. Already-cut chart parts use Cartesian
    // coordinates, including zero-turn world bands with their interior
    // longitude witnesses. A nonzero longitude turn remains quotient support
    // and needs Q; its result is cut back into chart pieces before publication.
    // A hole tracing the seam is split with the crossing holes: the polygon's
    // interior lies across the seam from it.
    let mut crossing_holes = Vec::new();
    let mut stationary_holes = Vec::new();
    for hole in polygon.holes.iter() {
        let points = collect_points(hole.coords());
        match split_ring_at_seam_with_metric(&points, GeographicFrame::crs84(), metric_access)? {
            SeamRing::Whole(spelled) if !ring_follows_seam(&points) => {
                stationary_holes.push(Ring::try_from_closed(CoordSeq::try_from_points(&spelled)?)?);
            },
            _ => crossing_holes.push(hole.clone()),
        }
    }
    if !crossing_holes.is_empty() {
        let axes = polygon.shell.coords().axes();
        let shell_only = Polygon::new(polygon.shell.clone(), stationary_holes);
        let shell_parts =
            split_polygon_inner_in_mode(&shell_only, preferred, overlay_mode, metric_access)?;
        let mut result = Shape::narrow_polygons(axes, shell_parts);
        for hole in crossing_holes {
            let hole_axes = hole.coords().axes();
            let hole_closure = pole_closure_for_ring(&hole);
            let hole_parts = split_polygon_inner_in_mode(
                &Polygon::new(hole, Vec::new()),
                hole_closure,
                overlay_mode,
                metric_access,
            )?;
            let hole_shape = Shape::narrow_polygons(hole_axes, hole_parts);
            let frame = GeographicFrame::crs84();
            let operands = [&result, &hole_shape];
            let use_quotient = match overlay_mode {
                HoleOverlayMode::Select => {
                    shape_has_longitude_turn_in_frame(&result, frame)?
                        || shape_has_longitude_turn_in_frame(&hole_shape, frame)?
                },
                HoleOverlayMode::Quotient => true,
                HoleOverlayMode::Chart => false,
            };
            result = if use_quotient {
                overlay_mode = HoleOverlayMode::Quotient;
                match metric_access {
                    Some(metric_access) => crate::geometry::overlay_all_with_metric(
                        &operands,
                        TopologyDomain::Geographic(frame),
                        crate::geometry::OverlayOp::Difference,
                        Strictness::Strict,
                        metric_access,
                    )?,
                    None => crate::geometry::overlay_all(
                        &operands,
                        TopologyDomain::Geographic(frame),
                        crate::geometry::OverlayOp::Difference,
                        Strictness::Strict,
                    )?,
                }
            } else {
                crate::geometry::overlay_all_geographic_chart(
                    &operands,
                    frame,
                    crate::geometry::OverlayOp::Difference,
                    Strictness::Strict,
                    metric_access,
                )?
            };
        }
        let polygons = polygons_from_shape(result)?;
        if overlay_mode != HoleOverlayMode::Quotient {
            return Ok(polygons);
        }
        // Q selects the correct quotient set, but this API publishes cut-chart geometry.
        let mut charted = Vec::new();
        for polygon in polygons {
            let closure = pole_closure_for_ring(&polygon.shell);
            charted.extend(split_polygon_inner_in_mode(
                &polygon,
                closure,
                HoleOverlayMode::Chart,
                metric_access,
            )?);
        }
        return Ok(charted);
    }

    let segments = match split_ring_at_seam_with_metric(
        &collect_points(polygon.shell.coords()),
        GeographicFrame::crs84(),
        metric_access,
    )? {
        SeamRing::Whole(shell) => {
            let shell = Ring::try_from_closed(CoordSeq::try_from_points(&shell)?)?;
            return Ok(vec![Polygon::new(shell, stationary_holes)]);
        },
        SeamRing::Pieces(pieces) => pieces,
    };
    let kept_holes: Vec<&Ring> = stationary_holes.iter().collect();
    if preferred != PoleClosure::Auto {
        // Caller knows the pole: close over it directly, no inference/retry.
        let (stitched, _) = extend_over_poles(segments, preferred)?;
        return assemble_polygons(stitched, &kept_holes);
    }
    let (stitched, capped) = extend_over_poles(segments.clone(), PoleClosure::Auto)?;
    let polygons = assemble_polygons(stitched, &kept_holes)?;
    if !capped
        || polygons.iter().try_fold(true, |ok, polygon| {
            Ok::<_, crate::error::Error>(ok && polygon_is_valid(polygon)?)
        })?
    {
        return Ok(polygons);
    }
    // The automatic closure picked a pole that self-intersects the ring.
    // Try each direction; a unique valid candidate wins, a tie is genuinely
    // ambiguous, and when nothing is valid the input itself was broken. Never
    // return the known-invalid automatic candidate as a plausible geometry.
    let mut valid = Vec::new();
    for closure in [
        PoleClosure::Chart(ChartPole::North),
        PoleClosure::Chart(ChartPole::South),
    ] {
        let (forced, _) = extend_over_poles(segments.clone(), closure)?;
        if let Ok(candidate) = assemble_polygons(forced, &kept_holes)
            && candidate.iter().try_fold(true, |ok, polygon| {
                Ok::<_, crate::error::Error>(ok && polygon_is_valid(polygon)?)
            })?
        {
            valid.push(candidate);
        }
    }
    match <[Vec<Polygon>; 1]>::try_from(valid) {
        Ok([winner]) => Ok(winner),
        Err(candidates) if candidates.is_empty() => {
            Err(GeometryErrorKind::antimeridian_split_failed(
                "neither pole closure produces valid polygon rings",
            ))
        },
        Err(_) => Err(GeometryErrorKind::antimeridian_split_failed(
            "pole closure is ambiguous: both directions produce valid rings",
        )),
    }
}

fn shape_has_longitude_turn_in_frame(shape: &Shape, frame: GeographicFrame) -> Result<bool> {
    fn ring_has_longitude_turn(ring: &Ring, frame: GeographicFrame) -> Result<bool> {
        let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
            .map_err(|error| split_view_error(&error))?;
        Ok(summary.chart_turn != 0)
    }

    fn polygon_has_longitude_turn(polygon: &Polygon, frame: GeographicFrame) -> Result<bool> {
        if ring_has_longitude_turn(&polygon.shell, frame)? {
            return Ok(true);
        }
        for hole in polygon.holes.iter() {
            if ring_has_longitude_turn(hole, frame)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    match shape {
        Shape::Polygon(polygon) => polygon_has_longitude_turn(polygon, frame),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                if polygon_has_longitude_turn(polygon, frame)? {
                    return Ok(true);
                }
            }
            Ok(false)
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                if shape_has_longitude_turn_in_frame(geometry, frame)? {
                    return Ok(true);
                }
            }
            Ok(false)
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => Ok(false),
    }
}

/// Resolve pole ownership from the admitted source ring before chart stitching.
fn pole_closure_for_ring(ring: &Ring) -> PoleClosure {
    match (
        ring_encloses_pole(ring, ChartPole::North),
        ring_encloses_pole(ring, ChartPole::South),
    ) {
        (true, false) => PoleClosure::Chart(ChartPole::North),
        (false, true) => PoleClosure::Chart(ChartPole::South),
        _ => PoleClosure::Auto,
    }
}

fn polygons_from_shape(shape: Shape) -> Result<Vec<Polygon>> {
    match shape {
        Shape::Polygon(polygon) => Ok(vec![polygon]),
        Shape::MultiPolygon(polygons) => Ok(polygons.iter().cloned().collect()),
        Shape::Empty(..) => Ok(Vec::new()),
        _ => Err(GeometryErrorKind::antimeridian_split_failed(
            "subtracting crossing holes produced non-areal output",
        )),
    }
}

/// Build the final polygons from stitched seam segments: rings, the
/// single-ring pole-covering wrap, and kept-hole assignment.
fn assemble_polygons(segments: Vec<Vec<Point>>, kept_holes: &[&Ring]) -> Result<Vec<Polygon>> {
    let mut shells = build_rings(segments);
    if shells.is_empty() {
        return Err(GeometryErrorKind::antimeridian_split_failed(
            "no ring could be stitched along the seam",
        ));
    }
    // A single non-CCW result encloses both poles: represent it as the world
    // rectangle with the ring as a hole.
    if shells.len() == 1 && !ring_is_ccw(&shells[0]) {
        // The fabricated world rectangle carries the ring's axes so shell and
        // hole stay axis-consistent (a measured/3D pole wrap is rejected
        // downstream; the placeholder ordinate is never observed).
        let like = shells[0].first().copied().unwrap_or(Point::new(0.0, 0.0)?);
        let corner = |lon: f64, lat: f64| -> Result<Point> {
            Point::new_axes(lon, lat, ZOrdinate(like.z()), MOrdinate(like.m()))
        };
        let world = vec![
            corner(-180.0, 90.0)?,
            corner(-180.0, -90.0)?,
            corner(180.0, -90.0)?,
            corner(180.0, 90.0)?,
            corner(-180.0, 90.0)?,
        ];
        let hole = close_ring(shells.remove(0));
        return Ok(vec![Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&world)?)?,
            vec![Ring::try_from_closed(CoordSeq::try_from_points(&hole)?)?],
        )]);
    }
    let mut polygons = Vec::with_capacity(shells.len());
    for shell in shells {
        polygons.push(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&close_ring(shell))?)?,
            Vec::new(),
        ));
    }
    // Assign each non-crossing hole to the part that contains it. Accumulate
    // every hole assigned to the same part.
    let candidate_shapes = polygons
        .iter()
        .cloned()
        .map(Shape::Polygon)
        .collect::<Vec<_>>();
    let mut assigned_holes = vec![Vec::<Ring>::new(); polygons.len()];
    for hole in kept_holes {
        let hole_shape = Shape::LineString(hole.as_line());
        let mut owner = None;
        for (index, candidate) in candidate_shapes.iter().enumerate() {
            if candidate.contains(&hole_shape)? {
                owner = Some(index);
                break;
            }
        }
        match owner {
            Some(index) => assigned_holes[index].push((*hole).clone()),
            None => {
                return Err(GeometryErrorKind::antimeridian_split_failed(
                    "a hole does not lie inside any split part",
                ));
            },
        }
    }
    for (polygon, mut holes) in polygons.iter_mut().zip(assigned_holes) {
        if holes.is_empty() {
            continue;
        }
        let combined = if polygon.holes.is_empty() {
            holes
        } else {
            let mut combined = polygon.holes.to_vec();
            combined.append(&mut holes);
            combined
        };
        *polygon = Polygon::new(polygon.shell.clone(), combined);
    }
    Ok(polygons)
}

fn close_ring(mut coords: Vec<Point>) -> Vec<Point> {
    if let (Some(first), Some(last)) = (coords.first().copied(), coords.last().copied())
        && !same_point(first, last)
    {
        coords.push(first);
    }
    coords
}

/// Which native-chart pole a stitched ring may close over.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PoleClosure {
    Auto,
    Chart(ChartPole),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HoleOverlayMode {
    Select,
    Quotient,
    Chart,
}

/// A seam end with no seam start between it and the pole closes over that pole.
/// Implying both poles under `Auto` means the winding was inverted, so the
/// original segments come back reversed. Returns the segments and whether any
/// pole closure was applied.
fn extend_over_poles(
    mut segments: Vec<Vec<Point>>,
    closure: PoleClosure,
) -> Result<(Vec<Vec<Point>>, bool)> {
    let chart_frame = GeographicFrame::crs84();
    let north_pole = chart_frame.chart_pole_latitude(ChartPole::North);
    let south_pole = chart_frame.chart_pole_latitude(ChartPole::South);
    // A fabricated pole-cap vertex must carry the SAME axes as the chain it
    // extends, or the assembled ring would mix Z/M presence — violating the
    // CoordSeq axis-homogeneity invariant (a `from_points` debug-assert, and a
    // silently malformed sequence in release). The placeholder ordinate (copied
    // from the capped chain's endpoint) is never observed: a measured/3D ring
    // whose cap survives is rejected by `reject_fabricated_pole_ordinates`, and
    // an XY ring carries no ordinate to fabricate.
    let pole_vertex = |like: Point, lon: f64, lat: f64| -> Result<Point> {
        Point::new_axes(lon, lat, ZOrdinate(like.z()), MOrdinate(like.m()))
    };
    let mut left_start: Option<f64> = None;
    let mut right_start: Option<f64> = None;
    let mut left_end: Option<(usize, f64)> = None;
    let mut right_end: Option<(usize, f64)> = None;
    for (index, segment) in segments.iter().enumerate() {
        let start = segment[0];
        let end = *segment.last().expect("segments are non-empty");
        if start.x == -180.0 && left_start.is_none_or(|latitude| start.y < latitude) {
            left_start = Some(start.y);
        } else if start.x == 180.0 && right_start.is_none_or(|latitude| start.y > latitude) {
            right_start = Some(start.y);
        }
        if end.x == -180.0 && left_end.is_none_or(|(_, latitude)| end.y < latitude) {
            left_end = Some((index, end.y));
        } else if end.x == 180.0 && right_end.is_none_or(|(_, latitude)| end.y > latitude) {
            right_end = Some((index, end.y));
        }
    }
    let original = segments.clone();
    let mut over_north = false;
    let mut over_south = false;
    if let Some((index, latitude)) = left_end {
        if closure == PoleClosure::Chart(ChartPole::North)
            && right_end.is_none()
            && left_start.is_none_or(|start| latitude > start)
        {
            // Forced north from the west side: cap and reverse.
            over_north = true;
            let like = *segments[index].last().expect("segments are non-empty");
            segments[index].push(pole_vertex(like, -180.0, north_pole)?);
            segments[index].push(pole_vertex(like, 180.0, north_pole)?);
            segments[index].reverse();
        } else if closure == PoleClosure::Chart(ChartPole::South)
            || left_start.is_none_or(|start| latitude < start)
        {
            over_south = true;
            let like = *segments[index].last().expect("segments are non-empty");
            segments[index].push(pole_vertex(like, -180.0, south_pole)?);
            segments[index].push(pole_vertex(like, 180.0, south_pole)?);
        }
    }
    if let Some((index, latitude)) = right_end {
        if closure == PoleClosure::Chart(ChartPole::South)
            && right_start.is_none_or(|start| latitude < start)
        {
            // Forced south from the east side: cap and reverse.
            over_south = true;
            let like = *segments[index].last().expect("segments are non-empty");
            segments[index].push(pole_vertex(like, 180.0, south_pole)?);
            segments[index].push(pole_vertex(like, -180.0, south_pole)?);
            segments[index].reverse();
        } else if closure == PoleClosure::Chart(ChartPole::North)
            || right_start.is_none_or(|start| latitude > start)
        {
            over_north = true;
            let like = *segments[index].last().expect("segments are non-empty");
            segments[index].push(pole_vertex(like, 180.0, north_pole)?);
            segments[index].push(pole_vertex(like, -180.0, north_pole)?);
        }
    }
    let capped = over_north || over_south;
    if closure == PoleClosure::Auto && over_north && over_south {
        // Both poles implied: the input ring was wound backwards. Reverse
        // every original segment instead.
        let mut reversed = original;
        for segment in &mut reversed {
            segment.reverse();
        }
        return Ok((reversed, false));
    }
    Ok((segments, capped))
}

fn is_self_closing(segment: &[Point]) -> bool {
    let start = segment[0];
    let end = *segment.last().expect("segments are non-empty");
    let is_right = end.x == 180.0;
    start.x == end.x && ((is_right && start.y > end.y) || (!is_right && start.y < end.y))
}

/// Build polygons iteratively: pop a segment, find the closest seam start
/// toward the pole on the same side (including the donut clause), join and
/// repeat; a segment with no candidate closes on itself.
fn build_rings(mut segments: Vec<Vec<Point>>) -> Vec<Vec<Point>> {
    let mut rings = Vec::new();
    while let Some(mut segment) = segments.pop() {
        let end = *segment.last().expect("segments are non-empty");
        let start = segment[0];
        let is_right = end.x == 180.0;
        let mut best: Option<(usize, f64)> = None;
        let self_candidate = is_self_closing(&segment).then_some(start.y);
        for (index, candidate) in segments.iter().enumerate() {
            let candidate_start = candidate[0];
            let candidate_end = *candidate.last().expect("segments are non-empty");
            if candidate_start.x != end.x {
                continue;
            }
            let toward_pole = if is_right {
                candidate_start.y > end.y
                    && (!is_self_closing(candidate) || candidate_end.y < start.y)
            } else {
                candidate_start.y < end.y
                    && (!is_self_closing(candidate) || candidate_end.y > start.y)
            };
            if toward_pole {
                let better = best.is_none_or(|(_, latitude)| {
                    if is_right {
                        candidate_start.y < latitude
                    } else {
                        candidate_start.y > latitude
                    }
                });
                if better {
                    best = Some((index, candidate_start.y));
                }
            }
        }
        // The self-closing latitude competes with the joins: when it is
        // closer to the end than every candidate start, the segment closes
        // on itself.
        let join = match (best, self_candidate) {
            (Some((index, latitude)), Some(own)) => {
                let self_wins = if is_right {
                    own < latitude
                } else {
                    own > latitude
                };
                if self_wins { None } else { Some(index) }
            },
            (Some((index, _)), None) => Some(index),
            (None, _) => None,
        };
        if let Some(index) = join {
            let joined = segments.remove(index);
            segment.extend(joined);
            segments.push(segment);
            continue;
        }
        // Self-closing: drop fully degenerate single-point rings (an input
        // corner exactly on the seam).
        if !segment.iter().all(|point| same_point(*point, segment[0])) {
            rings.push(segment);
        }
    }
    rings
}

#[cfg(test)]
mod tests {
    use super::{
        PoleClosure, certified_no_chart_events, chart_line, chart_polygon, chart_ring,
        line_has_certified_no_chart_events, split_polygon,
    };
    use crate::geographic::{
        AngularFactor, ChartPole, GeodeticPole, GeographicAxisOrientation, GeographicFrame,
        source_admission_counts,
    };
    use crate::geometry::antimeridian::segment_coords_with_metric;
    use crate::geometry::ordinates::{SourceArcRequest, source_arc_ordinates};
    use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
    use crate::geometry::{
        ChartMetric as _, CoordSeq, CoordinateAxes, Coordinates as _, GeometryErrorKind, LineSeq,
        MOrdinate, MultiLineString, MultiMember, MultiPolygon, Point, Polygon, Ring,
        RootMetricState, Shape, Unmetered, ZOrdinate,
    };
    use crate::test_support::allocation_count;

    fn line(xs: &[f64], ys: &[f64]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn canonical_crs84_split_skips_retained_edge_admission() {
        let shapes = [
            line(&[10.0, 11.0, 12.0], &[0.0, 1.0, 0.0]),
            Shape::Polygon(Polygon::new(
                closed_ring(&[10.0, 20.0, 20.0, 10.0, 10.0], &[
                    10.0, 10.0, 20.0, 20.0, 10.0,
                ]),
                Vec::new(),
            )),
        ];

        for shape in shapes {
            let before = source_admission_counts::snapshot().0;
            let split = shape
                .split_antimeridian_in_frame(GeographicFrame::crs84())
                .unwrap();
            assert_eq!(
                source_admission_counts::snapshot().0,
                before,
                "a canonical CRS84 identity split must not admit source edges"
            );
            assert_eq!(split, shape);
        }
    }

    #[test]
    fn certified_chart_event_skip_falls_back_at_the_periodic_boundary() {
        const CRS: &str = r#"GEOGCS["Custom grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]"#;
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let frame = crate::crs::info_with_state(CRS, &state)
            .unwrap()
            .geographic_frame()
            .unwrap();
        assert_eq!(frame.longitude().period().to_bits(), 400.0_f64.to_bits());
        assert!(certified_no_chart_events(-10.0, 10.0, frame));
        assert!(certified_no_chart_events(10.0, 20.0, frame));
        assert!(!certified_no_chart_events(100.0, -100.0, frame));
        assert!(!certified_no_chart_events(199.0, -2.0, frame));
        assert!(!certified_no_chart_events(200.0, 0.0, frame));
        assert!(!certified_no_chart_events(0.0, 400.0, frame));
        assert!(!certified_no_chart_events(f64::NAN, 0.0, frame));

        let longitude_scale =
            frame.longitude().to_degrees(1.0) * frame.longitude_orientation_sign();
        let latitude_scale = frame.latitude_unit().native_to_degrees(1.0, frame.pole())
            * frame.latitude_orientation_sign();
        let no_metric = None::<&crate::geometry::SourceArcMetricAccess<'_, '_>>;
        let Shape::LineString(safe_line) = line(&[-20.0, 0.0, 20.0], &[0.0, 1.0, 0.0]) else {
            unreachable!();
        };
        assert!(line_has_certified_no_chart_events(
            &safe_line,
            frame,
            longitude_scale
        ));
        let before = source_admission_counts::snapshot().0;
        let chart = chart_line(
            &safe_line,
            frame,
            longitude_scale,
            latitude_scale,
            no_metric,
        )
        .unwrap();
        assert_eq!(source_admission_counts::snapshot().0, before);
        assert_eq!(chart.as_coords().iter_coords().count(), 3);

        let Shape::LineString(crossing_line) = line(&[199.0, -2.0], &[0.0, 10.0]) else {
            unreachable!();
        };
        assert!(!line_has_certified_no_chart_events(
            &crossing_line,
            frame,
            longitude_scale
        ));
        let before = source_admission_counts::snapshot().0;
        let chart = chart_line(
            &crossing_line,
            frame,
            longitude_scale,
            latitude_scale,
            no_metric,
        )
        .unwrap();
        assert_eq!(source_admission_counts::snapshot().0 - before, 1);
        assert!(
            chart
                .as_coords()
                .iter_coords()
                .any(|point| point.x == 180.0)
        );

        let ring = closed_ring(&[-20.0, 20.0, 20.0, -20.0, -20.0], &[
            10.0, 10.0, 20.0, 20.0, 10.0,
        ]);
        let before = source_admission_counts::snapshot().0;
        let chart = chart_ring(&ring, frame, longitude_scale, latitude_scale, no_metric).unwrap();
        assert_eq!(source_admission_counts::snapshot().0 - before, 4);
        assert_eq!(chart.iter_coords().count(), 5);
    }

    fn closed_ring(xs: &[f64], ys: &[f64]) -> Ring {
        Ring::closed_coordseq(
            CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap(),
        )
        .unwrap()
    }

    fn xyzm_ring(coordinates: &[(f64, f64, f64, f64)]) -> Ring {
        let points = coordinates
            .iter()
            .map(|&(x, y, z, m)| {
                Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap()
            })
            .collect::<Vec<_>>();
        Ring::closed_coordseq(CoordSeq::try_from_points(&points).unwrap()).unwrap()
    }

    fn independent_meridian_fraction(
        parameter: f64,
        start_degrees: f64,
        end_degrees: f64,
        flattening: f64,
    ) -> f64 {
        fn integrate(limit: f64, start_degrees: f64, delta_radians: f64, flattening: f64) -> f64 {
            let intervals = 4096;
            let step = limit / f64::from(intervals);
            let eccentricity_squared = flattening * (2.0 - flattening);
            // Keep accumulation error below the Simpson refinement error.
            let mut weighted = 0.0;
            let mut correction = 0.0;
            for index in 0..=intervals {
                let parameter = f64::from(index) * step;
                let latitude =
                    (start_degrees + parameter * delta_radians.to_degrees()).to_radians();
                let denominator = 1.0 - eccentricity_squared * latitude.sin() * latitude.sin();
                let radius = (1.0 - eccentricity_squared) / denominator.powf(1.5);
                let weight = if index == 0 || index == intervals {
                    1.0
                } else if index % 2 == 0 {
                    2.0
                } else {
                    4.0
                };
                let term = weight * radius * delta_radians - correction;
                let next = weighted + term;
                correction = (next - weighted) - term;
                weighted = next;
            }
            step * weighted / 3.0
        }
        let delta_radians = (end_degrees - start_degrees).to_radians();
        integrate(parameter, start_degrees, delta_radians, flattening)
            / integrate(1.0, start_degrees, delta_radians, flattening)
    }

    fn ulp_error_limit(value: f64, ulps: f64) -> f64 {
        let magnitude = value.abs();
        let spacing = if magnitude == 0.0 {
            f64::from_bits(1)
        } else {
            magnitude.next_up() - magnitude
        };
        ulps * spacing
    }

    #[test]
    fn chart_line_seam_ordinates_use_the_original_root_arclength() {
        let source_points = [
            Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(1000.0))).unwrap(),
            Point::new_axes(
                -170.0,
                60.0,
                ZOrdinate(Some(100.0)),
                MOrdinate(Some(3000.0)),
            )
            .unwrap(),
        ];
        let line = LineSeq::try_new(CoordSeq::try_from_points(&source_points).unwrap()).unwrap();
        let shape = Shape::LineString(line.clone());
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let chart =
            crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&shape], &state, |metric| {
                let metric_access = || Ok(metric);
                chart_line(
                    &line,
                    GeographicFrame::crs84(),
                    1.0,
                    1.0,
                    Some(&metric_access),
                )
            })
            .unwrap();
        let seam = chart.as_coords().point_at(1);

        assert_eq!(seam.x, 180.0);
        assert_eq!(seam.y, 30.0);
        assert!((seam.z().unwrap() - 50.438_798_221_498_36).abs() <= 2.0e-9);
        assert!((seam.m().unwrap() - 2_008.775_964_429_967_2).abs() <= 4.0e-8);
    }

    #[test]
    fn segment_transit_ordinates_follow_the_retained_piece_arclength() {
        let source = [
            Point::new_axes(170.0, 10.0, ZOrdinate(Some(10.0)), MOrdinate(Some(1.0))).unwrap(),
            Point::new_axes(-150.0, 30.0, ZOrdinate(Some(50.0)), MOrdinate(Some(5.0))).unwrap(),
        ];
        let line = LineSeq::try_new(CoordSeq::try_from_points(&source).unwrap()).unwrap();
        let shape = Shape::LineString(line);
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let seam_values =
            crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&shape], &state, |metric| {
                let metric_access = || Ok(metric);
                Ok(segment_coords_with_metric(
                    &source,
                    GeographicFrame::crs84(),
                    Some(&metric_access),
                )?
                .into_iter()
                .flatten()
                .filter(|point| point.x.abs() == 180.0)
                .map(|point| (point.y, point.z(), point.m()))
                .collect::<Vec<_>>())
            })
            .unwrap();
        let expected = (20.339_021_393_918_856, 2.033_902_139_391_885_5);

        assert_eq!(seam_values.len(), 2);
        for (latitude, z, m) in seam_values {
            assert_eq!(latitude, 15.0);
            assert!((z.unwrap() - expected.0).abs() <= 1.0e-7);
            assert!((m.unwrap() - expected.1).abs() <= 1.0e-8);
        }
    }

    #[test]
    fn split_line_preserves_an_authored_full_turn_circle() {
        let source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(0.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(Some(2.0))).unwrap(),
                    Point::new_axes(360.0, 0.0, ZOrdinate(Some(110.0)), MOrdinate(Some(202.0)))
                        .unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let split =
            crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&source], &state, |metric| {
                let metric_access = || Ok(metric);
                source.split_antimeridian_in_frame_with_metric(
                    GeographicFrame::crs84(),
                    Some(&metric_access),
                )
            })
            .unwrap();
        let Shape::MultiLineString(lines) = &split else {
            panic!("a full-turn circle crossing the cut has two chart pieces");
        };
        assert_eq!(lines.axes(), CoordinateAxes::XYZM);
        let mut points = Vec::new();
        split.for_each_point(|point| points.push(point));
        for (longitude, z, m) in [
            (90.0, 35.0, 52.0),
            (-90.0, 85.0, 152.0),
            (180.0, 60.0, 102.0),
            (-180.0, 60.0, 102.0),
        ] {
            assert!(
                points.iter().any(|point| {
                    point.x == longitude && point.z() == Some(z) && point.m() == Some(m)
                }),
                "full-turn output omitted ({longitude}, 0, {z}, {m}): {points:?}"
            );
        }
    }

    #[test]
    fn tilted_full_turn_seam_and_witnesses_use_the_original_root() {
        let source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(100.0)))
                        .unwrap(),
                    Point::new_axes(530.0, 60.0, ZOrdinate(Some(100.0)), MOrdinate(Some(1100.0)))
                        .unwrap(),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let Shape::LineString(line) = &source else {
            unreachable!();
        };
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&source], &state, |metric| {
            let frame = GeographicFrame::crs84();
            let metric_access = || Ok(metric);
            let chart = chart_line(line, frame, 1.0, 1.0, Some(&metric_access))?;
            let coordinates: Vec<_> = chart.as_coords().iter_coords().collect();
            let seam = *coordinates
                .iter()
                .find(|point| point.x == 180.0)
                .expect("the authored full turn crosses the chart seam");
            let quarter = *coordinates
                .iter()
                .find(|point| point.x == -100.0)
                .expect("the first quarter witness remains on the chart path");

            let piece = [seam, quarter];
            let longitude = frame
                .longitude()
                .chart_connection(piece[0].x, piece[1].x)
                .map(|connection| connection.retained())
                .map_err(|error| {
                    GeometryErrorKind::antimeridian_split_failed(error.to_string())
                })?;
            let midpoint = ExactRational::new(
                ExactDyadic::from_i64(1),
                ExactDyadic::from_i64(2),
            )?;
            let request = SourceArcRequest::new(
                frame,
                &piece,
                piece[0].axes().union(piece[1].axes()),
                &midpoint,
                || Ok(longitude),
            )?;
            let mut root_metric = RootMetricState::default();
            let mut fraction = None;
            let mut work = Unmetered;
            let composed = source_arc_ordinates(
                &request,
                || Ok(metric),
                &mut root_metric,
                &mut fraction,
                &mut work,
            )?;
            let split = source.split_antimeridian_in_frame_with_metric(
                frame,
                Some(&metric_access),
            )?;
            let mut public_seams = Vec::new();
            let mut public_quarter = None;
            split.for_each_point(|point| {
                if point.x.abs() == 180.0 && (point.y - 60.0 / 36.0).abs() <= 2.0e-14 {
                    public_seams.push(point);
                }
                if point.x == -100.0 && point.y == 15.0 {
                    public_quarter = Some(point);
                }
            });
            assert!(
                public_seams.iter().any(|point| point.x == -180.0)
                    && public_seams.iter().any(|point| point.x == 180.0),
                "the final split omitted a seam alias: {public_seams:?}"
            );
            let public_quarter =
                public_quarter.expect("the final split retains the first quarter witness");

            let seam_parameter =
                ExactRational::new(ExactDyadic::from_i64(10), ExactDyadic::from_i64(360))?;
            let exact_seam_latitude =
                ExactRational::from_f64(60.0).try_product(&seam_parameter)?;
            assert_ne!(
                ExactRational::from_f64(seam.y),
                exact_seam_latitude,
                "t=1/36 produces a binary64 seam latitude"
            );

            // Independent 70-digit Decimal Simpson integration, N=8192 and
            // 16384, for the WGS84 first-fundamental-form speed on the root
            // (170,0)->(530,60). The subedge reference uses the rounded
            // binary64 seam latitude, as the emitted piece does.
            let q_seam = 0.033_323_535_838_094_22_f64;
            let q_quarter = 0.296_653_444_529_051_57_f64;
            let q_later_root = 0.166_073_296_867_606_94_f64;
            let q_later_piece = 0.166_073_296_867_606_94_f64;
            let root_z = |q: f64| 100.0 * q;
            let root_m = |q: f64| 100.0 + 1000.0 * q;
            let mut max_absolute_error = 0.0_f64;
            let mut max_relative_error = 0.0_f64;
            let mut measure = |actual: f64, expected: f64| {
                let absolute = (actual - expected).abs();
                max_absolute_error = max_absolute_error.max(absolute);
                max_relative_error = max_relative_error.max(absolute / expected.abs());
                absolute
            };
            for point in &public_seams {
                let expected_z = root_z(q_seam);
                let expected_m = root_m(q_seam);
                assert!(
                    measure(point.z().unwrap(), expected_z)
                        <= ulp_error_limit(expected_z, 4.0)
                );
                assert!(
                    measure(point.m().unwrap(), expected_m)
                        <= ulp_error_limit(expected_m, 4.0)
                );
            }
            let quarter_z = root_z(q_quarter);
            let quarter_m = root_m(q_quarter);
            assert!(
                measure(public_quarter.z().unwrap(), quarter_z)
                    <= ulp_error_limit(quarter_z, 4.0)
            );
            assert!(
                measure(public_quarter.m().unwrap(), quarter_m)
                    <= ulp_error_limit(quarter_m, 4.0)
            );
            let later_z = root_z(q_later_piece);
            let later_m = root_m(q_later_piece);
            assert!(
                measure(composed.z.value().unwrap(), later_z)
                    <= ulp_error_limit(later_z, 4.0)
            );
            assert!(
                measure(composed.m.value().unwrap(), later_m)
                    <= ulp_error_limit(later_m, 4.0)
            );
            assert!(
                (later_z - root_z(q_later_root)).abs() <= ulp_error_limit(later_z, 4.0)
            );
            assert!(
                (later_m - root_m(q_later_root)).abs() <= ulp_error_limit(later_m, 4.0)
            );
            println!(
                "tilted_full_turn_accuracy={{\"max_abs_error\":{max_absolute_error:.17e},\"max_relative_error\":{max_relative_error:.17e},\"seam_latitude\":{:.17e},\"piece_local_t\":0.5}}",
                seam.y,
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn seam_piece_ordinate_composition_matches_independent_root_arclength() {
        let source_points = [
            Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(100.0))).unwrap(),
            Point::new_axes(
                -170.0,
                60.0,
                ZOrdinate(Some(100.0)),
                MOrdinate(Some(1100.0)),
            )
            .unwrap(),
        ];
        let line = LineSeq::try_new(CoordSeq::try_from_points(&source_points).unwrap()).unwrap();
        let source_shape = Shape::LineString(line.clone());
        let frame = GeographicFrame::crs84();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&source_shape], &state, |metric| {
            let metric_access = || Ok(metric);
            let chart = chart_line(&line, frame, 1.0, 1.0, Some(&metric_access))?;
            let seam = chart.as_coords().point_at(1);
            let piece = [source_points[0], seam];
            let retained = frame
                .longitude()
                .chart_connection(piece[0].x, piece[1].x)
                .map(|connection| connection.retained())
                .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
            let parameter = ExactRational::new(ExactDyadic::from_i64(1), ExactDyadic::from_i64(2))?;
            let request = SourceArcRequest::new(
                frame,
                &piece,
                piece[0].axes().union(piece[1].axes()),
                &parameter,
                || Ok(retained),
            )?;
            let mut root_metric = RootMetricState::default();
            let mut fraction = None;
            let mut work = Unmetered;
            let composed = source_arc_ordinates(
                &request,
                || Ok(metric),
                &mut root_metric,
                &mut fraction,
                &mut work,
            )?;
            let expected_q = 0.252_865_084_440_217_66_f64;
            assert!((composed.z.value().unwrap() - 100.0 * expected_q).abs() <= 2.0e-8);
            assert!((composed.m.value().unwrap() - (100.0 + 1000.0 * expected_q)).abs() <= 2.0e-7);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn chart_line_eligible_interior_request_propagates_metric_failure() {
        let line = LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(1.0))).unwrap(),
                Point::new_axes(-170.0, 60.0, ZOrdinate(Some(100.0)), MOrdinate(Some(2.0)))
                    .unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();
        let accesses = std::cell::Cell::new(0);
        let failing_metric = || -> crate::error::Result<&crate::crs::GeodesicMetric> {
            accesses.set(accesses.get() + 1);
            Err(crate::crs::CrsError::invalid("seam metric sentinel"))
        };
        let result = chart_line(
            &line,
            GeographicFrame::crs84(),
            1.0,
            1.0,
            Some(&failing_metric),
        );

        assert!(
            result
                .expect_err("eligible seam ordinates require metric authority")
                .to_string()
                .contains("seam metric sentinel")
        );
        assert_eq!(accesses.get(), 1);

        let xy = LineSeq::try_new(
            CoordSeq::try_from_vecs(vec![170.0, -170.0], vec![0.0, 60.0], None, None).unwrap(),
        )
        .unwrap();
        chart_line(
            &xy,
            GeographicFrame::crs84(),
            1.0,
            1.0,
            Some(&failing_metric),
        )
        .unwrap();
        assert_eq!(accesses.get(), 1);
    }

    #[test]
    fn crossing_hole_circle_piece_ordinates_compose_to_the_original_root() {
        let shell = xyzm_ring(&[
            (0.0, -30.0, 30.0, 3000.0),
            (360.0, -30.0, 20.0, 2000.0),
            (360.0, 30.0, 20.0, 2000.0),
            (0.0, 30.0, 30.0, 3000.0),
            (0.0, -30.0, 30.0, 3000.0),
        ]);
        let hole = xyzm_ring(&[
            (135.0, 30.0, 21.25, 2125.0),
            (180.0, 25.0, 20.0, 2000.0),
            (-180.0, 20.0, 30.0, 3000.0),
            (-170.0, 18.0, 25.0, 2500.0),
            (-170.0, 12.0, 25.0, 2500.0),
            (-180.0, 10.0, 30.0, 3000.0),
            (180.0, 15.0, 20.0, 2000.0),
            (135.0, 20.0, 25.0, 2500.0),
            (135.0, 30.0, 21.25, 2125.0),
        ]);
        let polygon = Polygon::new(shell, vec![hole]);
        let source = Shape::Polygon(polygon.clone());
        let frame = GeographicFrame::crs84();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let result =
            crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&source], &state, |metric| {
                let metric_access = || Ok(metric);
                let chart = chart_polygon(&polygon, frame, 1.0, 1.0, Some(&metric_access))?;
                crate::geometry::ordinates::take_source_arc_observations();
                let chart_shape = Shape::Polygon(chart.clone());
                let parts = split_polygon(
                    &chart_shape,
                    &chart,
                    PoleClosure::Auto,
                    Some(&metric_access),
                )?;
                Ok(parts)
            });
        let observations = crate::geometry::ordinates::take_source_arc_observations();
        let split = result.unwrap_or_else(|error| {
            panic!("hole overlay failed after these source requests {observations:?}: {error}")
        });
        assert!(!split.is_empty());

        let observation = observations
            .iter()
            .find(|observation| {
                observation.source[0].y == 30.0
                    && observation.source[1].y == 30.0
                    && ((observation.source[0].x == 180.0 && observation.source[1].x == 90.0)
                        || (observation.source[0].x == 90.0 && observation.source[1].x == 180.0))
            })
            .expect("hole overlay requests a node on the upper Circle witness interval");
        let chart_longitude = observation.source[0].x
            + (observation.source[1].x - observation.source[0].x) * observation.parameter;
        let source_longitude = chart_longitude + 180.0;
        let root_fraction = (360.0 - source_longitude) / 360.0;
        let expected_z = 20.0 + 10.0 * root_fraction;
        let expected_m = 2000.0 + 1000.0 * root_fraction;
        assert_eq!(chart_longitude, 135.0);
        assert_eq!(expected_z.to_bits(), 21.25_f64.to_bits());
        assert_eq!(expected_m.to_bits(), 2125.0_f64.to_bits());
        assert_eq!(
            observation.z.expect("shell carries Z").to_bits(),
            21.25_f64.to_bits()
        );
        assert_eq!(
            observation.m.expect("shell carries M").to_bits(),
            2125.0_f64.to_bits()
        );
    }

    fn split_crs84(shape: &Shape) -> Shape {
        shape
            .split_antimeridian_in_frame(GeographicFrame::crs84())
            .unwrap()
    }

    #[test]
    fn degree_try_new_frame_equals_crs84() {
        let degrees = AngularFactor::try_new("angular unit", std::f64::consts::PI / 180.0).unwrap();
        let frame = GeographicFrame::try_new(degrees, degrees, 0.0).unwrap();
        assert_eq!(frame, GeographicFrame::crs84());
    }

    #[test]
    fn antimeridian_pole_closure_maps_physical_pole_to_native_axis() {
        let unit = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
        let frame = GeographicFrame::try_new_with_orientation(
            unit,
            unit,
            0.0,
            GeographicAxisOrientation::from_axis_directions(true, false),
        )
        .unwrap();
        let source = polygon(&[(170.0, 80.0), (-70.0, 80.0), (50.0, 80.0)], &[]);
        let Shape::Polygon(source_polygon) = &source else {
            panic!("the pole-cap fixture is a polygon");
        };
        let summary = crate::geographic::RingSummary::try_new(
            frame,
            &crate::geographic::GeographicRing::from_ring(&source_polygon.shell),
        )
        .unwrap();
        assert_eq!(summary.bounded_pole(), Some(ChartPole::North));

        let split = source.split_antimeridian_in_frame(frame).unwrap();
        let physical_south = frame.native_latitude_pole(GeodeticPole::South);
        let physical_north = frame.native_latitude_pole(GeodeticPole::North);
        let mut saw_south_cap = false;
        let mut saw_north_cap = false;
        split.for_each_point(|point| {
            saw_south_cap |= point.y.to_bits() == physical_south.to_bits();
            saw_north_cap |= point.y.to_bits() == physical_north.to_bits();
        });
        assert!(saw_south_cap, "{split:?}");
        assert!(!saw_north_cap, "{split:?}");
    }

    #[test]
    fn crs84_canonical_noncrossing_line_matches_source_and_skips_chart_alloc() {
        let shape = line(&[10.0, 20.0], &[1.0, 2.0]);
        let (cloned, clone_n) = allocation_count(|| shape.clone());
        let (split, split_n) = allocation_count(|| split_crs84(&shape));
        assert_eq!(split, cloned);
        assert_eq!(split_n, clone_n);
        let crossing = line(&[170.0, -170.0], &[0.0, 0.0]);
        let (_, crossing_n) = allocation_count(|| split_crs84(&crossing));
        assert!(clone_n < crossing_n);
    }

    #[test]
    fn canonical_native_frame_line_skips_temporary_chart_allocation() {
        const CRS: &str = r#"GEOGCS["Custom grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]"#;
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let frame = crate::crs::info_with_state(CRS, &state)
            .unwrap()
            .geographic_frame()
            .unwrap();
        let shape = line(&[-20.0, 20.0], &[1.0, 2.0]);

        let (cloned, clone_allocations) = allocation_count(|| shape.clone());
        let (split, split_allocations) =
            allocation_count(|| shape.split_antimeridian_in_frame(frame).unwrap());

        assert_eq!(split, cloned);
        assert_eq!(split_allocations, clone_allocations);
    }

    #[test]
    fn crs84_half_period_endpoint_is_identity() {
        let shape = line(&[180.0, 0.0], &[0.0, 0.0]);
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn crs84_full_period_pair_is_identity() {
        let shape = line(&[-180.0, 180.0], &[0.0, 0.0]);
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn crs84_world_rectangle_is_identity() {
        let shape = Shape::Polygon(Polygon::new(
            closed_ring(&[-180.0, 180.0, 180.0, -180.0, -180.0], &[
                -90.0, -90.0, 90.0, 90.0, -90.0,
            ]),
            Vec::new(),
        ));
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn crs84_canonical_polygon_with_holes_is_identity() {
        let shape = Shape::Polygon(Polygon::new(
            closed_ring(&[0.0, 10.0, 10.0, 0.0, 0.0], &[0.0, 0.0, 10.0, 10.0, 0.0]),
            vec![closed_ring(&[2.0, 4.0, 4.0, 2.0, 2.0], &[
                2.0, 2.0, 4.0, 4.0, 2.0,
            ])],
        ));
        assert_eq!(split_crs84(&shape), shape);
    }

    #[test]
    fn multiline_split_preserves_logical_empty_and_repeated_members() {
        let crossing_line = LineSeq::try_new(
            CoordSeq::try_from_vecs(vec![170.0, -170.0], vec![10.0, 10.0], None, None)
                .expect("finite XY line coordinates"),
        )
        .expect("two-point line");
        let multiline = MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(crossing_line.clone()),
            MultiMember::Empty,
            MultiMember::Value(crossing_line),
            MultiMember::Empty,
        ])
        .expect("multipart lines have homogeneous XY axes");
        let split = Shape::MultiLineString(multiline)
            .split_antimeridian_in_frame(GeographicFrame::crs84())
            .expect("valid multipart line splits");
        let split_lines = match split {
            Shape::MultiLineString(lines) => Some(lines),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        }
        .expect("multipart line remains a multipart line");
        assert_eq!(split_lines.axes(), CoordinateAxes::XY);
        let mut line_members = split_lines.members_with_values();
        assert!(matches!(line_members.next(), Some(MultiMember::Empty)));
        let first_part = line_members
            .next()
            .and_then(|member| match member {
                MultiMember::Empty => None,
                MultiMember::Value(line) => Some(line),
            })
            .expect("first crossing line has a first part");
        let second_part = line_members
            .next()
            .and_then(|member| match member {
                MultiMember::Empty => None,
                MultiMember::Value(line) => Some(line),
            })
            .expect("first crossing line has a second part");
        assert!(matches!(line_members.next(), Some(MultiMember::Empty)));
        let repeated_first_part = line_members
            .next()
            .and_then(|member| match member {
                MultiMember::Empty => None,
                MultiMember::Value(line) => Some(line),
            })
            .expect("repeated crossing line has a first part");
        let repeated_second_part = line_members
            .next()
            .and_then(|member| match member {
                MultiMember::Empty => None,
                MultiMember::Value(line) => Some(line),
            })
            .expect("repeated crossing line has a second part");
        assert!(matches!(line_members.next(), Some(MultiMember::Empty)));
        assert!(line_members.next().is_none());
        assert_eq!(first_part, repeated_first_part);
        assert_eq!(second_part, repeated_second_part);
    }

    #[test]
    fn multipolygon_chart_split_preserves_logical_empty_and_repeated_members() {
        let crossing_polygon = Polygon::new(
            closed_ring(&[170.0, -170.0, -170.0, 170.0, 170.0], &[
                40.0, 40.0, 50.0, 50.0, 40.0,
            ]),
            Vec::new(),
        );
        let multipolygon = MultiPolygon::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(crossing_polygon.clone()),
            MultiMember::Empty,
            MultiMember::Value(crossing_polygon),
            MultiMember::Empty,
        ])
        .expect("multipart polygons have homogeneous XY axes");
        let split = Shape::MultiPolygon(multipolygon)
            .split_antimeridian_in_frame(GeographicFrame::crs84())
            .expect("valid multipart polygon splits");
        let split_polygons = match split {
            Shape::MultiPolygon(polygons) => Some(polygons),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        }
        .expect("multipart polygon remains a multipart polygon");
        assert_eq!(split_polygons.axes(), CoordinateAxes::XY);
        let mut polygon_members = split_polygons.members_with_values();
        assert!(matches!(polygon_members.next(), Some(MultiMember::Empty)));
        let first_part = polygon_members
            .next()
            .and_then(|member| match member {
                MultiMember::Empty => None,
                MultiMember::Value(polygon) => Some(polygon),
            })
            .expect("first crossing polygon has a first part");
        let second_part = polygon_members
            .next()
            .and_then(|member| match member {
                MultiMember::Empty => None,
                MultiMember::Value(polygon) => Some(polygon),
            })
            .expect("first crossing polygon has a second part");
        assert!(matches!(polygon_members.next(), Some(MultiMember::Empty)));
        let repeated_first_part = polygon_members
            .next()
            .and_then(|member| match member {
                MultiMember::Empty => None,
                MultiMember::Value(polygon) => Some(polygon),
            })
            .expect("repeated crossing polygon has a first part");
        let repeated_second_part = polygon_members
            .next()
            .and_then(|member| match member {
                MultiMember::Empty => None,
                MultiMember::Value(polygon) => Some(polygon),
            })
            .expect("repeated crossing polygon has a second part");
        assert!(matches!(polygon_members.next(), Some(MultiMember::Empty)));
        assert!(polygon_members.next().is_none());
        assert_eq!(first_part, repeated_first_part);
        assert_eq!(second_part, repeated_second_part);
    }

    #[test]
    fn multipolygon_forced_pole_split_preserves_logical_empty_and_repeated_members() {
        let north_cap = Polygon::new(
            closed_ring(&[-45.0, 45.0, 135.0, -135.0, -45.0], &[
                40.0, 40.0, 40.0, 40.0, 40.0,
            ]),
            Vec::new(),
        );
        let multipolygon = MultiPolygon::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(north_cap.clone()),
            MultiMember::Empty,
            MultiMember::Value(north_cap),
            MultiMember::Empty,
        ])
        .expect("multipart polygons have homogeneous XY axes");
        let forced = Shape::MultiPolygon(multipolygon)
            .split_antimeridian_over_pole(true)
            .expect("valid multipart north-pole cap splits");
        let forced_caps = match forced {
            Shape::MultiPolygon(polygons) => Some(polygons),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        }
        .expect("forced pole split remains a multipart polygon");
        let mut groups = Vec::<Vec<Polygon>>::new();
        let mut current = Vec::new();
        let mut empty_rows = 0;
        for member in forced_caps.members_with_values() {
            match member {
                MultiMember::Empty => {
                    groups.push(std::mem::take(&mut current));
                    empty_rows += 1;
                },
                MultiMember::Value(polygon) => current.push(polygon.clone()),
            }
        }
        groups.push(current);
        assert_eq!(empty_rows, 3);
        assert_eq!(groups.len(), 4);
        let mut groups = groups.into_iter();
        assert!(groups.next().expect("leading empty row").is_empty());
        let first = groups.next().expect("first pole cap");
        assert!(!first.is_empty());
        let repeated = groups.next().expect("repeated pole cap");
        assert!(!repeated.is_empty());
        assert_eq!(repeated, first);
        assert!(groups.next().expect("trailing empty row").is_empty());
        assert!(groups.next().is_none());
    }

    #[test]
    fn crs84_nested_xyzm_collection_is_identity() {
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(
                    vec![1.0, 2.0],
                    vec![3.0, 4.0],
                    Some(vec![10.0, 20.0]),
                    Some(vec![7.0, 8.0]),
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let point = Shape::Point(
            Point::new_axes(5.0, 6.0, ZOrdinate(Some(1.0)), MOrdinate(Some(2.0))).unwrap(),
        );
        let inner = Shape::geometry_collection(vec![point]).unwrap();
        let shape = Shape::geometry_collection(vec![line, inner]).unwrap();
        assert_eq!(split_crs84(&shape), shape);
    }

    fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Shape {
        let ring = |points: &[(f64, f64)]| {
            let mut xs: Vec<f64> = points.iter().map(|&(x, _)| x).collect();
            let mut ys: Vec<f64> = points.iter().map(|&(_, y)| y).collect();
            xs.push(xs[0]);
            ys.push(ys[0]);
            closed_ring(&xs, &ys)
        };
        Shape::Polygon(Polygon::new(
            ring(shell),
            holes.iter().map(|hole| ring(hole)).collect(),
        ))
    }

    /// Every spelling of the seam vertices names one polygon, so each must
    /// split into valid parts covering the same chart area.
    fn assert_seam_spellings_split_alike(
        shell: &[(f64, f64)],
        holes: &[&[(f64, f64)]],
        parts: usize,
        area: f64,
    ) {
        let seam: Vec<(usize, usize)> = std::iter::once(shell)
            .chain(holes.iter().copied())
            .enumerate()
            .flat_map(|(ring, points)| {
                points
                    .iter()
                    .enumerate()
                    .filter(|(_, (x, _))| x.abs() == 180.0)
                    .map(move |(vertex, _)| (ring, vertex))
            })
            .collect();
        for spelling in 0..1_u32 << seam.len() {
            let mut rings: Vec<Vec<(f64, f64)>> = std::iter::once(shell)
                .chain(holes.iter().copied())
                .map(<[_]>::to_vec)
                .collect();
            for (bit, &(ring, vertex)) in seam.iter().enumerate() {
                rings[ring][vertex].0 = if (spelling >> bit) & 1 == 1 {
                    180.0
                } else {
                    -180.0
                };
            }
            let hole_rings: Vec<&[(f64, f64)]> = rings[1..].iter().map(Vec::as_slice).collect();
            let split = split_crs84(&polygon(&rings[0], &hole_rings));
            assert_eq!(
                split.validate().unwrap(),
                None,
                "spelling {spelling:b}: {split:?}"
            );
            assert_eq!(
                split.part_count(),
                parts,
                "spelling {spelling:b}: {split:?}"
            );
            assert!(
                (split.area() - area).abs() < 1e-9,
                "spelling {spelling:b}: {split:?}"
            );
        }
    }

    #[test]
    fn seam_run_between_opposite_sides_belongs_to_its_interior_side() {
        // Fiji's dissolved outline: it reaches the seam from the east at
        // -15.5, runs south along it to -19.15, and leaves to the west.
        let shell = [
            (-180.0, -19.153_409),
            (-179.963_319_5, -15.507_781_7),
            (-178.980_216_6, -15.967_318_2),
            (-178.045_034, -17.836_819_8),
            (-178.011_665, -19.811_436_8),
            (-178.928_670_8, -21.213_499_6),
            (-179.094_029_1, -20.818_838_7),
            (180.0, -19.800_520_5),
            (179.648_239_9, -19.405_357_3),
            (176.805_793_9, -17.730_552_9),
            (177.194_871, -16.601_579),
            (180.0, -15.508_868_1),
        ];
        let reversed: Vec<(f64, f64)> = shell.iter().rev().copied().collect();
        let area = split_crs84(&polygon(&shell, &[])).area();
        for ring in [&shell[..], &reversed[..]] {
            assert_seam_spellings_split_alike(ring, &[], 2, area);
        }
    }

    #[test]
    fn hole_notch_touching_the_seam_trims_its_side() {
        let shell = [
            (170.0, -10.0),
            (-170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
        ];
        let notch = [(175.0, 0.0), (180.0, 0.0), (180.0, 5.0), (175.0, 5.0)];
        assert_seam_spellings_split_alike(&shell, &[&notch], 2, 400.0 - 25.0);
    }

    #[test]
    fn seam_edge_of_a_one_sided_polygon_keeps_it_whole() {
        let shell = [(170.0, 0.0), (180.0, 0.0), (180.0, 10.0), (170.0, 10.0)];
        assert_seam_spellings_split_alike(&shell, &[], 1, 100.0);
    }

    #[test]
    fn crs84_zero_to_full_turn_band_rewrites() {
        let shape = Shape::Polygon(Polygon::new(
            closed_ring(&[0.0, 360.0, 360.0, 0.0, 0.0], &[
                -30.0, -30.0, 30.0, 30.0, -30.0,
            ]),
            Vec::new(),
        ));
        let split = split_crs84(&shape);
        assert_ne!(split, shape);
        let expected = Shape::Polygon(Polygon::new(
            closed_ring(
                &[
                    -180.0, -90.0, 0.0, 90.0, 180.0, 180.0, 90.0, 0.0, -90.0, -180.0, -180.0,
                ],
                &[
                    -30.0, -30.0, -30.0, -30.0, -30.0, 30.0, 30.0, 30.0, 30.0, 30.0, -30.0,
                ],
            ),
            Vec::new(),
        ));
        assert_eq!(split, expected);
    }

    #[test]
    fn splitting_a_polar_annulus_keeps_the_full_longitude_band() {
        let latitude_ring = |latitude| {
            [
                (0.0, latitude),
                (60.0, latitude),
                (120.0, latitude),
                (180.0, latitude),
                (-120.0, latitude),
                (-60.0, latitude),
            ]
        };
        let shell = latitude_ring(70.0);
        let hole = latitude_ring(80.0);
        let source = polygon(&shell, &[&hole]);
        let split = split_crs84(&source);
        assert_eq!(split.validate().unwrap(), None, "{split:?}");
        let point = Shape::Point(Point::new_raw_xy(0.0, 75.0));
        let source_plan = crate::geometry::PreparedPlan::transient(&split);
        let point_plan = crate::geometry::PreparedPlan::transient(&point);
        let source_cache = crate::geometry::FrameDependentCaches::default();
        let point_cache = crate::geometry::FrameDependentCaches::default();
        let frame = GeographicFrame::crs84();
        let source_prepared =
            crate::geometry::GeographicPrepared::try_new(&source_plan, &source_cache, frame)
                .unwrap();
        let point_prepared =
            crate::geometry::GeographicPrepared::try_new(&point_plan, &point_cache, frame).unwrap();
        assert!(
            crate::geometry::graph_relate_data_geographic(source_prepared, point_prepared)
                .unwrap()
                .is_contains(),
            "{split:?}"
        );

        for longitude in [0.0, 180.0, -180.0] {
            for (latitude, expected) in [(75.0, true), (89.0, false), (65.0, false)] {
                let point = Shape::Point(Point::new_raw_xy(longitude, latitude));
                let point_plan = crate::geometry::PreparedPlan::transient(&point);
                let point_cache = crate::geometry::FrameDependentCaches::default();
                let point_prepared =
                    crate::geometry::GeographicPrepared::try_new(&point_plan, &point_cache, frame)
                        .unwrap();
                let relation =
                    crate::geometry::graph_relate_data_geographic(source_prepared, point_prepared)
                        .unwrap();
                assert_eq!(
                    relation.is_contains(),
                    expected,
                    "({longitude}, {latitude}) in {split:?}"
                );
            }
        }
    }

    #[test]
    fn crossing_hole_subtraction_keeps_a_zero_turn_world_band_in_its_chart() {
        let source = polygon(
            &[(0.0, -30.0), (360.0, -30.0), (360.0, 30.0), (0.0, 30.0)],
            &[&[(175.0, 0.0), (-175.0, 0.0), (-175.0, 5.0), (175.0, 5.0)]],
        );
        let split = split_crs84(&source);

        assert_eq!(split.validate().unwrap(), None, "{split:?}");
        assert_eq!(split.area(), 21_550.0, "{split:?}");
        let mut witnesses = [false; 2];
        split.for_each_point(|point| {
            if point.x.to_bits() == 0.0_f64.to_bits() {
                if point.y.to_bits() == (-30.0_f64).to_bits() {
                    witnesses[0] = true;
                }
                if point.y.to_bits() == 30.0_f64.to_bits() {
                    witnesses[1] = true;
                }
            }
        });
        assert_eq!(witnesses, [true, true], "{split:?}");
    }

    #[test]
    fn crossing_hole_uses_the_original_grad_ellipsoid_in_the_temporary_degree_chart() {
        const CRS: &str = r#"GEOGCS["Custom grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]"#;
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let frame = crate::crs::info_with_state(CRS, &state)
            .unwrap()
            .geographic_frame()
            .unwrap();
        assert_eq!(frame.longitude().period().to_bits(), 400.0_f64.to_bits());
        let latitude_scale = frame.latitude_unit().native_to_degrees(1.0, frame.pole())
            * frame.latitude_orientation_sign();
        let chart_bottom = -50.0 * latitude_scale;
        let chart_top = 80.0 * latitude_scale;
        let lower_latitude = 0.0 * latitude_scale;
        let upper_latitude = 50.0 * latitude_scale;
        let parameter_at = |latitude: f64| {
            let numerator = ExactDyadic::from_f64(latitude)
                .try_subtract(ExactDyadic::from_f64(chart_bottom))
                .unwrap();
            let denominator = ExactDyadic::from_f64(chart_top)
                .try_subtract(ExactDyadic::from_f64(chart_bottom))
                .unwrap();
            ExactRational::new(numerator, denominator)
                .unwrap()
                .round()
                .unwrap()
        };

        let shell = xyzm_ring(&[
            (150.0, -50.0, 0.0, 0.0),
            (-150.0, -50.0, 0.0, 0.0),
            (-150.0, 80.0, 100.0, 1000.0),
            (150.0, 80.0, 100.0, 1000.0),
            (150.0, -50.0, 0.0, 0.0),
        ]);
        let shell_only = Shape::Polygon(Polygon::new(shell.clone(), Vec::new()));
        let lower_parameter = parameter_at(lower_latitude);
        let upper_parameter = parameter_at(upper_latitude);
        let (lower_z, lower_m, upper_z, upper_m) =
            crate::crs::with_ellipsoid_metric_at(CRS, &[&shell_only], &state, |metric| {
                let ramp_start = Point::new_axes(
                    180.0,
                    chart_bottom,
                    ZOrdinate(Some(0.0)),
                    MOrdinate(Some(0.0)),
                )?;
                let ramp_end = Point::new_axes(
                    180.0,
                    chart_top,
                    ZOrdinate(Some(100.0)),
                    MOrdinate(Some(1000.0)),
                )?;
                let edge =
                    crate::crs::ChartEdge::new(ramp_start, ramp_end, GeographicFrame::crs84())?;
                let length = metric.chart_edge_length(&edge)?;
                let lower_fraction =
                    metric.chart_edge_arclength_fraction(&edge, lower_parameter, Some(length))?;
                let upper_fraction =
                    metric.chart_edge_arclength_fraction(&edge, upper_parameter, Some(length))?;
                let (lower_z, lower_m) = crate::geometry::source_ordinate_values_at_fraction(
                    ramp_start,
                    ramp_end,
                    CoordinateAxes::XYZM,
                    lower_fraction,
                );
                let (upper_z, upper_m) = crate::geometry::source_ordinate_values_at_fraction(
                    ramp_start,
                    ramp_end,
                    CoordinateAxes::XYZM,
                    upper_fraction,
                );
                Ok((
                    lower_z.0.expect("fixture has Z"),
                    lower_m.0.expect("fixture has M"),
                    upper_z.0.expect("fixture has Z"),
                    upper_m.0.expect("fixture has M"),
                ))
            })
            .unwrap();
        let hole = xyzm_ring(&[
            (160.0, 0.0, lower_z, lower_m),
            (160.0, 50.0, upper_z, upper_m),
            (200.0, 50.0, upper_z, upper_m),
            (200.0, 0.0, lower_z, lower_m),
            (160.0, 0.0, lower_z, lower_m),
        ]);
        let source = Shape::Polygon(Polygon::new(shell, vec![hole]));
        let split = crate::crs::with_ellipsoid_metric_at(CRS, &[&source], &state, |metric| {
            let metric_access = || Ok(metric);
            source.split_antimeridian_in_frame_with_metric(frame, Some(&metric_access))
        })
        .unwrap();
        assert_eq!(split.axes(), CoordinateAxes::XYZM);

        let custom_flattening = 1.0 / 150.0;
        let wgs84_flattening = 1.0 / 298.257_223_563;
        for (native_latitude, degree_latitude, parameter) in [
            (0.0, lower_latitude, lower_parameter),
            (50.0, upper_latitude, upper_parameter),
        ] {
            let fraction = independent_meridian_fraction(
                parameter,
                chart_bottom,
                chart_top,
                custom_flattening,
            );
            let chart_fraction = (degree_latitude - chart_bottom) / (chart_top - chart_bottom);
            let wgs84_fraction =
                independent_meridian_fraction(parameter, chart_bottom, chart_top, wgs84_flattening);
            assert!((fraction - chart_fraction).abs() > 1.0e-5);
            assert!((fraction - wgs84_fraction).abs() > 1.0e-7);

            let mut seam_values = Vec::new();
            split.for_each_point(|point| {
                if point.x.abs() == 200.0 && point.y == native_latitude {
                    seam_values.push((point.z(), point.m()));
                }
            });
            assert!(
                !seam_values.is_empty(),
                "missing seam cut at {native_latitude}"
            );
            for (z, m) in seam_values {
                assert!((z.expect("split retains Z") - 100.0 * fraction).abs() <= 2.0e-8);
                assert!((m.expect("split retains M") - 1000.0 * fraction).abs() <= 2.0e-7);
            }
        }
    }

    #[test]
    fn reversed_grad_hole_overlay_retains_meridian_ordinates() {
        const CRS: &str = r#"GEOGCS["West south grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",WEST],AXIS["Latitude",SOUTH]]"#;
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let frame = crate::crs::info_with_state(CRS, &state)
            .unwrap()
            .geographic_frame()
            .unwrap();
        assert_eq!(frame.longitude().period().to_bits(), 400.0_f64.to_bits());
        assert_eq!(frame.longitude_orientation_sign(), -1.0);
        assert_eq!(frame.latitude_orientation_sign(), -1.0);

        let longitude_scale =
            frame.longitude().to_degrees(1.0) * frame.longitude_orientation_sign();
        let latitude_scale = frame.latitude_unit().native_to_degrees(1.0, frame.pole())
            * frame.latitude_orientation_sign();
        let chart_bottom = super::chart_latitude(50.0, frame, latitude_scale).unwrap();
        let chart_top = super::chart_latitude(-80.0, frame, latitude_scale).unwrap();
        let lower_latitude = super::chart_latitude(0.0, frame, latitude_scale).unwrap();
        let upper_latitude = super::chart_latitude(-50.0, frame, latitude_scale).unwrap();
        let parameter_at = |latitude: f64| {
            let numerator = ExactDyadic::from_f64(latitude)
                .try_subtract(ExactDyadic::from_f64(chart_bottom))
                .unwrap();
            let denominator = ExactDyadic::from_f64(chart_top)
                .try_subtract(ExactDyadic::from_f64(chart_bottom))
                .unwrap();
            ExactRational::new(numerator, denominator)
                .unwrap()
                .round()
                .unwrap()
        };
        let lower_parameter = parameter_at(lower_latitude);
        let upper_parameter = parameter_at(upper_latitude);
        let shell = xyzm_ring(&[
            (-150.0, 50.0, 0.0, 0.0),
            (150.0, 50.0, 0.0, 0.0),
            (150.0, -80.0, 100.0, 1000.0),
            (-150.0, -80.0, 100.0, 1000.0),
            (-150.0, 50.0, 0.0, 0.0),
        ]);
        let shell_only = Shape::Polygon(Polygon::new(shell.clone(), Vec::new()));
        let (lower_z, lower_m, upper_z, upper_m) =
            crate::crs::with_ellipsoid_metric_at(CRS, &[&shell_only], &state, |metric| {
                let ramp_start = Point::new_axes(
                    180.0,
                    chart_bottom,
                    ZOrdinate(Some(0.0)),
                    MOrdinate(Some(0.0)),
                )?;
                let ramp_end = Point::new_axes(
                    180.0,
                    chart_top,
                    ZOrdinate(Some(100.0)),
                    MOrdinate(Some(1000.0)),
                )?;
                let edge =
                    crate::crs::ChartEdge::new(ramp_start, ramp_end, GeographicFrame::crs84())?;
                let length = metric.chart_edge_length(&edge)?;
                let lower_fraction =
                    metric.chart_edge_arclength_fraction(&edge, lower_parameter, Some(length))?;
                let upper_fraction =
                    metric.chart_edge_arclength_fraction(&edge, upper_parameter, Some(length))?;
                let (lower_z, lower_m) = crate::geometry::source_ordinate_values_at_fraction(
                    ramp_start,
                    ramp_end,
                    CoordinateAxes::XYZM,
                    lower_fraction,
                );
                let (upper_z, upper_m) = crate::geometry::source_ordinate_values_at_fraction(
                    ramp_start,
                    ramp_end,
                    CoordinateAxes::XYZM,
                    upper_fraction,
                );
                Ok((
                    lower_z.0.expect("fixture carries Z"),
                    lower_m.0.expect("fixture carries M"),
                    upper_z.0.expect("fixture carries Z"),
                    upper_m.0.expect("fixture carries M"),
                ))
            })
            .unwrap();
        let hole = xyzm_ring(&[
            (-160.0, 0.0, lower_z, lower_m),
            (-160.0, -50.0, upper_z, upper_m),
            (-200.0, -50.0, upper_z, upper_m),
            (-200.0, 0.0, lower_z, lower_m),
            (-160.0, 0.0, lower_z, lower_m),
        ]);
        let polygon = Polygon::new(shell, vec![hole]);
        let source = Shape::Polygon(polygon.clone());
        let (parts, observations, split) =
            crate::crs::with_ellipsoid_metric_at(CRS, &[&source], &state, |metric| {
                let metric_access = || Ok(metric);
                let chart = chart_polygon(
                    &polygon,
                    frame,
                    longitude_scale,
                    latitude_scale,
                    Some(&metric_access),
                )?;
                crate::geometry::ordinates::take_source_arc_observations();
                let chart_shape = Shape::Polygon(chart.clone());
                let parts = split_polygon(
                    &chart_shape,
                    &chart,
                    PoleClosure::Auto,
                    Some(&metric_access),
                )?;
                let observations = crate::geometry::ordinates::take_source_arc_observations();
                let split =
                    source.split_antimeridian_in_frame_with_metric(frame, Some(&metric_access))?;
                Ok((parts, observations, split))
            })
            .unwrap();
        assert!(!parts.is_empty());
        assert_eq!(split.axes(), CoordinateAxes::XYZM);

        let mut matched = 0;
        let mut max_absolute_error = 0.0_f64;
        let mut max_relative_error = 0.0_f64;
        for observation in observations.iter().filter(|observation| {
            observation.source[0].x.abs() == 180.0
                && observation.source[1].x.abs() == 180.0
                && observation.source[0].x.to_bits() == observation.source[1].x.to_bits()
                && observation.source[0].y != observation.source[1].y
        }) {
            let chart_latitude = observation.source[0].y
                + (observation.source[1].y - observation.source[0].y) * observation.parameter;
            let root_parameter = (chart_latitude - chart_bottom) / (chart_top - chart_bottom);
            let q =
                independent_meridian_fraction(root_parameter, chart_bottom, chart_top, 1.0 / 150.0);
            let expected_z = 100.0 * q;
            let expected_m = 1000.0 * q;
            let observed_z = observation.z.expect("overlay source carries Z");
            let observed_m = observation.m.expect("overlay source carries M");
            let z_error = (observed_z - expected_z).abs();
            let m_error = (observed_m - expected_m).abs();
            max_absolute_error = max_absolute_error.max(z_error).max(m_error);
            max_relative_error = max_relative_error
                .max(z_error / expected_z.abs())
                .max(m_error / expected_m.abs());
            assert!(
                z_error <= ulp_error_limit(expected_z, 4.0),
                "Z error {z_error:e} at t={root_parameter}"
            );
            assert!(
                m_error <= ulp_error_limit(expected_m, 4.0),
                "M error {m_error:e} at t={root_parameter}; source={:?}; parameter={:.17e}; actual={observed_m:.17e}; expected={expected_m:.17e}; chart_latitude={chart_latitude:.17e}",
                observation.source,
                observation.parameter,
            );
            matched += 1;
        }
        assert!(
            matched > 0,
            "hole overlay made no interior seam-edge request"
        );

        let lower_q =
            independent_meridian_fraction(lower_parameter, chart_bottom, chart_top, 1.0 / 150.0);
        let upper_q =
            independent_meridian_fraction(upper_parameter, chart_bottom, chart_top, 1.0 / 150.0);
        for (native_latitude, expected_z, expected_m, source_z, source_m) in [
            (0.0, 100.0 * lower_q, 1000.0 * lower_q, lower_z, lower_m),
            (-50.0, 100.0 * upper_q, 1000.0 * upper_q, upper_z, upper_m),
        ] {
            let values = {
                let mut values = Vec::new();
                split.for_each_point(|point| {
                    if point.x.abs() == 200.0 && point.y == native_latitude {
                        values.push((point.z(), point.m()));
                    }
                });
                values
            };
            assert!(
                !values.is_empty(),
                "missing final seam point at {native_latitude}"
            );
            for (z, m) in values {
                let z = z.expect("final seam point carries Z");
                let m = m.expect("final seam point carries M");
                let z_error = (z - expected_z).abs();
                let m_error = (m - expected_m).abs();
                max_absolute_error = max_absolute_error.max(z_error).max(m_error);
                max_relative_error = max_relative_error
                    .max(z_error / expected_z.abs())
                    .max(m_error / expected_m.abs());
                assert!(
                    z_error <= ulp_error_limit(expected_z, 4.0),
                    "final Z error {z_error:e}"
                );
                assert!(
                    m_error <= ulp_error_limit(expected_m, 4.0),
                    "final M error {m_error:e}"
                );
                assert_eq!(z.to_bits(), source_z.to_bits());
                assert_eq!(m.to_bits(), source_m.to_bits());
            }
        }
        println!(
            "reversed_grad_hole_accuracy={{\"max_abs_error\":{max_absolute_error:.17e},\"max_relative_error\":{max_relative_error:.17e},\"interior_requests\":{matched}}}"
        );
    }

    #[test]
    fn chart_ring_interpolates_ordinates_at_full_turn_witnesses() {
        let point =
            |x, y, z, m| Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap();
        let ring = Ring::closed_coordseq(
            CoordSeq::try_from_points(&[
                point(0.0, -30.0, 0.0, 10.0),
                point(360.0, -30.0, 360.0, 370.0),
                point(360.0, 30.0, 720.0, 730.0),
                point(0.0, 30.0, 1080.0, 1090.0),
                point(0.0, -30.0, 0.0, 10.0),
            ])
            .unwrap(),
        )
        .unwrap();
        let state = crate::crs::runtime_state_snapshot().unwrap();
        let chart = crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[], &state, |metric| {
            let metric_access = || Ok(metric);
            super::chart_ring(
                &ring,
                GeographicFrame::crs84(),
                1.0,
                1.0,
                Some(&metric_access),
            )
        })
        .unwrap();

        assert_eq!(chart.coords().coord_count(), 11);
        assert_eq!(chart.coords().point_at(2), point(0.0, -30.0, 180.0, 190.0));
        assert_eq!(chart.coords().point_at(7), point(0.0, 30.0, 900.0, 910.0));
    }

    #[test]
    fn chart_ring_keeps_a_non_band_explicit_circle_edge() {
        let ring = closed_ring(&[0.0, 360.0, 300.0, 0.0], &[60.0, 60.0, 65.0, 60.0]);

        let chart = super::chart_ring::<crate::crs::GeodesicMetric>(
            &ring,
            GeographicFrame::crs84(),
            1.0,
            1.0,
            None,
        )
        .unwrap();

        assert_eq!(chart.coords().coord_count(), 7);
        assert_eq!(
            chart.coords().first_coord(),
            Some(Point::new_raw_xy(0.0, 60.0))
        );
        assert_eq!(
            chart.coords().last_coord(),
            Some(Point::new_raw_xy(0.0, 60.0))
        );
    }
}
