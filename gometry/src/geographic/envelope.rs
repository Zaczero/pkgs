use std::borrow::Borrow;
use std::collections::BTreeSet;

use super::view::{GeographicAdmissionEvent, SharedCoordinateRunKey};
use super::{
    CanonicalLongitude, ChartConnection, ChartPole, ChartStep, CircularInterval,
    GeographicAdmissionError, GeographicCollectionOwners, GeographicFrame, Latitude, RingDomain,
    RingSummary, ScalarGeographicView, SeamIdentity, publish_certified_minor_chain,
    union_circular_intervals,
};
use crate::geometry::{
    Bounds, CoordSeq, ExpansionBudget, LineSeq, Point, Polygon, Ring, Shape, SupportRunKind,
};

/// Periodic longitude coverage and occupied latitude extrema in one native frame.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct GeographicSupportEnvelope {
    pub(crate) longitude: CircularInterval,
    latitude: Option<LatitudeBounds>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct GeographicSupportProducts {
    pub(crate) support: GeographicSupportEnvelope,
    pub(crate) raw_extrema: Option<Bounds>,
    #[cfg(test)]
    work: GeographicSupportWork,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GeographicSupportWork {
    coordinate_visits: usize,
    point_events: usize,
    point_intervals: usize,
    segment_connections: usize,
    segment_intervals: usize,
    ring_summaries: usize,
}

#[derive(Default)]
struct GeographicSupportAccumulator {
    intervals: Vec<CircularInterval>,
    effects: RingEnvelopeEffects,
    polygon: Option<PolygonEnvelopeEffects>,
    shared_runs: BTreeSet<SharedCoordinateRunKey>,
    support_run: Option<SupportRun>,
    #[cfg(test)]
    work: GeographicSupportWork,
}

enum SupportRun {
    Suppressed,
    Points,
    Segments(Option<ConnectedSupportRun>),
}

struct ConnectedSupportRun {
    current: (i64, CanonicalLongitude),
    lower: (i64, CanonicalLongitude),
    upper: (i64, CanonicalLongitude),
}

impl ConnectedSupportRun {
    const fn new(start: CanonicalLongitude) -> Self {
        let first = (0, start);
        Self {
            current: first,
            lower: first,
            upper: first,
        }
    }

    fn include(&mut self, connection: ChartConnection) -> bool {
        debug_assert_eq!(connection.start, self.current.1);
        // A proper connected cover stays within two lifts of its initial site;
        // the first wider span marks Full and stops lift accumulation.
        self.current = (
            self.current
                .0
                .wrapping_add(i64::from(connection.source_lift)),
            connection.end,
        );
        let compare = |left: (i64, CanonicalLongitude), right: (i64, CanonicalLongitude)| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.get().total_cmp(&right.1.get()))
        };
        if compare(self.current, self.lower).is_lt() {
            self.lower = self.current;
        }
        if compare(self.current, self.upper).is_gt() {
            self.upper = self.current;
        }
        let lift_span = self.upper.0.wrapping_sub(self.lower.0);
        matches!(connection.step, ChartStep::Circle(_))
            || lift_span > 1
            || (lift_span == 1 && self.upper.1.get() >= self.lower.1.get())
    }
}

impl GeographicSupportAccumulator {
    fn visit(&mut self, event: GeographicAdmissionEvent) {
        match event {
            GeographicAdmissionEvent::BeginSupportRun { kind, key } => {
                let suppressed = self.effects.full_longitude
                    || key.is_some_and(|key| !self.shared_runs.insert(key));
                self.support_run = Some(if suppressed {
                    SupportRun::Suppressed
                } else {
                    match kind {
                        SupportRunKind::PointSet => SupportRun::Points,
                        SupportRunKind::SegmentChain => SupportRun::Segments(None),
                    }
                });
            },
            GeographicAdmissionEvent::EndSupportRun => {
                if let Some(SupportRun::Segments(Some(run))) = self.support_run.take() {
                    #[cfg(test)]
                    {
                        self.work.segment_intervals = self.work.segment_intervals.wrapping_add(1);
                    }
                    if !self.effects.full_longitude {
                        self.intervals
                            .push(CircularInterval::from_endpoints(run.lower.1, run.upper.1));
                    }
                }
            },
            GeographicAdmissionEvent::Point(longitude) => {
                #[cfg(test)]
                {
                    self.work.point_events = self.work.point_events.wrapping_add(1);
                }
                debug_assert!(!matches!(self.support_run, Some(SupportRun::Segments(_))));
                if !self.effects.full_longitude
                    && !matches!(self.support_run, Some(SupportRun::Suppressed))
                {
                    #[cfg(test)]
                    {
                        self.work.point_intervals = self.work.point_intervals.wrapping_add(1);
                    }
                    self.intervals.push(CircularInterval::point(longitude));
                }
            },
            GeographicAdmissionEvent::Segment(connection) => {
                #[cfg(test)]
                {
                    self.work.segment_connections = self.work.segment_connections.wrapping_add(1);
                }
                if self.effects.full_longitude {
                    return;
                }
                match &mut self.support_run {
                    Some(SupportRun::Segments(run)) => {
                        let run =
                            run.get_or_insert_with(|| ConnectedSupportRun::new(connection.start));
                        if run.include(connection) {
                            self.effects.full_longitude = true;
                            self.intervals.clear();
                        }
                    },
                    Some(SupportRun::Suppressed) => {},
                    None | Some(SupportRun::Points) => {
                        unreachable!("segment events belong to a connected support run")
                    },
                }
            },
            GeographicAdmissionEvent::Ring { summary, hole } => {
                #[cfg(test)]
                {
                    self.work.ring_summaries = self.work.ring_summaries.wrapping_add(1);
                }
                self.polygon
                    .get_or_insert_with(PolygonEnvelopeEffects::default)
                    .include_ring(summary, hole);
            },
            GeographicAdmissionEvent::EndPolygon => {
                if let Some(polygon) = self.polygon.take() {
                    self.effects.replay(polygon.finish());
                }
            },
        }
    }

    fn finish(
        self,
        frame: GeographicFrame,
        extrema: CoordinateExtrema,
    ) -> GeographicSupportProducts {
        let mut latitude = extrema.latitude_bounds(frame);
        if let Some(bounds) = &mut latitude {
            if self.effects.north_pole {
                bounds.north = Latitude::from_admitted(frame, frame.pole());
            }
            if self.effects.south_pole {
                bounds.south = Latitude::from_admitted(frame, -frame.pole());
            }
        }
        let support = if self.effects.full_longitude {
            GeographicSupportEnvelope {
                longitude: CircularInterval::Full,
                latitude,
            }
        } else if let Some(linear) = extrema.linear_envelope(frame) {
            linear
        } else {
            GeographicSupportEnvelope {
                longitude: union_circular_intervals(frame.longitude(), &self.intervals),
                latitude,
            }
        };
        GeographicSupportProducts {
            support,
            raw_extrema: extrema.raw_extrema(),
            #[cfg(test)]
            work: self.work,
        }
    }
}

#[derive(Default)]
struct PolygonEnvelopeEffects {
    effects: RingEnvelopeEffects,
    shell_pole: Option<ChartPole>,
    hole_north: bool,
    hole_south: bool,
}

impl PolygonEnvelopeEffects {
    fn include_ring(&mut self, summary: RingSummary, hole: bool) {
        self.effects.include_ring(summary);
        if hole {
            match summary.bounded_pole() {
                Some(ChartPole::North) => self.hole_north = true,
                Some(ChartPole::South) => self.hole_south = true,
                None => {},
            }
        } else {
            self.shell_pole = summary.bounded_pole();
        }
    }

    fn finish(self) -> RingEnvelopeEffects {
        RingEnvelopeEffects {
            full_longitude: self.effects.full_longitude,
            north_pole: self.shell_pole == Some(ChartPole::North) && !self.hole_north,
            south_pole: self.shell_pole == Some(ChartPole::South) && !self.hole_south,
        }
    }
}

impl GeographicSupportProducts {
    pub(crate) fn public_bounds(self, frame: GeographicFrame) -> Option<Bounds> {
        let support_bounds = self.support.bounds(frame)?;
        if matches!(self.support.longitude, CircularInterval::Full) {
            return Some(support_bounds);
        }
        let raw = self.raw_extrema?;
        if frame.bounds_certify_ordinary_support(raw) {
            Some(raw)
        } else {
            Some(support_bounds)
        }
    }

    pub(crate) fn public_bounds_for_shape(
        frame: GeographicFrame,
        shape: &Shape,
        raw_bounds: Option<Bounds>,
    ) -> Option<Bounds> {
        Self::public_bounds_for_shapes(frame, std::slice::from_ref(shape), raw_bounds)
    }

    pub(crate) fn public_bounds_for_points(
        frame: GeographicFrame,
        points: impl Iterator<Item = Point>,
        raw_bounds: Option<Bounds>,
    ) -> Option<Bounds> {
        if let Some(products) =
            raw_bounds.and_then(|bounds| Self::from_ordinary_bounds(frame, bounds))
        {
            return products.public_bounds(frame);
        }
        match Self::try_from_points(frame, points) {
            Ok(products) => products.public_bounds(frame),
            // Points cannot select a seam-crossing edge, so preserve their
            // infallible getter's raw-extrema fallback on admission failure.
            Err(GeographicAdmissionError::Geographic(_)) => raw_bounds,
            Err(_) => None,
        }
    }

    pub(crate) fn public_bounds_for_shapes(
        frame: GeographicFrame,
        shapes: &[impl Borrow<Shape>],
        raw_bounds: Option<Bounds>,
    ) -> Option<Bounds> {
        if let Some(products) =
            raw_bounds.and_then(|bounds| Self::from_ordinary_bounds(frame, bounds))
        {
            return products.public_bounds(frame);
        }
        match Self::try_from_shapes(frame, shapes) {
            Ok(products) => products.public_bounds(frame),
            Err(GeographicAdmissionError::Geographic(_))
                if shapes.iter().all(|shape| {
                    !crate::geometry::crosses_antimeridian_in_frame(shape.borrow(), frame)
                }) =>
            {
                // Invalid noncrossing rows keep their old raw bounds; a failed
                // periodic edge must not expose the wider Cartesian envelope.
                raw_bounds.or_else(|| Self::raw_bounds_for_shapes(shapes))
            },
            Err(_) => None,
        }
    }

    fn raw_bounds_for_shapes(shapes: &[impl Borrow<Shape>]) -> Option<Bounds> {
        let mut total: Option<Bounds> = None;
        for shape in shapes {
            let Some(bounds) = shape.borrow().bounds() else {
                continue;
            };
            match &mut total {
                Some(total) => total.include_bounds(bounds),
                None => total = Some(bounds),
            }
        }
        total
    }
}

impl GeographicSupportEnvelope {
    pub(crate) fn try_from_shape_with_bounds(
        frame: GeographicFrame,
        shape: &Shape,
        raw_bounds: Option<Bounds>,
    ) -> Result<Self, GeographicAdmissionError> {
        GeographicSupportProducts::try_from_shape_with_bounds(frame, shape, raw_bounds)
            .map(|products| products.support)
    }

    pub(crate) fn from_ordinary_bounds(frame: GeographicFrame, bounds: Bounds) -> Option<Self> {
        if !frame.bounds_certify_ordinary_support(bounds) {
            return None;
        }
        Some(Self {
            longitude: CircularInterval::from_endpoints(
                frame
                    .longitude()
                    .canonicalize(bounds.minx())
                    .expect("the ordinary support certificate proves finite longitude"),
                frame
                    .longitude()
                    .canonicalize(bounds.maxx())
                    .expect("the ordinary support certificate proves finite longitude"),
            ),
            latitude: Some(LatitudeBounds {
                south: Latitude::from_admitted(frame, bounds.miny()),
                north: Latitude::from_admitted(frame, bounds.maxy()),
            }),
        })
    }

    pub(crate) fn bounds(self, frame: GeographicFrame) -> Option<Bounds> {
        let (south, north) = self.latitude_bounds()?;
        let axis = frame.longitude();
        let (west, east) = match self.longitude {
            CircularInterval::Empty => return None,
            CircularInterval::Full => (-axis.half_period(), axis.half_period()),
            arc @ CircularInterval::Arc { .. } => arc.west_east()?,
        };
        Some(Bounds::new_raw(west, south, east, north))
    }

    pub(crate) fn into_shape(self, frame: GeographicFrame) -> crate::error::Result<Shape> {
        let Some((south, north)) = self.latitude_bounds() else {
            return Ok(Shape::empty_polygon());
        };
        let axis = frame.longitude();
        let longitudes = realization_longitudes(self.longitude, axis);
        if longitudes.is_empty() {
            return Ok(Shape::empty_polygon());
        }
        #[expect(
            clippy::float_cmp,
            reason = "support realization is degenerate only when represented latitude endpoints are equal"
        )]
        let zero_height = south == north;
        if longitudes.len() == 1 {
            let longitude = longitudes
                .first()
                .expect("one-element longitude realization has an endpoint")
                .get();
            if zero_height {
                return Ok(Shape::Point(Point::new_raw_xy(longitude, south)));
            }
            let coordinates = certified_bound_coordinates(
                frame,
                &[
                    Point::new_raw_xy(longitude, south),
                    Point::new_raw_xy(longitude, north),
                ],
                false,
            )?;
            return Ok(Shape::LineString(LineSeq::from_proven(coordinates)));
        }
        if zero_height {
            let points = longitudes
                .iter()
                .map(|longitude| Point::new_raw_xy(longitude.get(), south))
                .collect::<Vec<_>>();
            let coordinates = certified_bound_coordinates(frame, &points, false)?;
            return Ok(Shape::LineString(LineSeq::from_proven(coordinates)));
        }

        let mut points = Vec::with_capacity(longitudes.len().saturating_mul(2).saturating_add(1));
        points.extend(
            longitudes
                .iter()
                .map(|longitude| Point::new_raw_xy(longitude.get(), south)),
        );
        let end = longitudes
            .last()
            .expect("nonempty longitude realization has an endpoint")
            .get();
        points.push(Point::new_raw_xy(end, north));
        points.extend(
            longitudes
                .iter()
                .rev()
                .skip(1)
                .map(|longitude| Point::new_raw_xy(longitude.get(), north)),
        );
        let start = longitudes
            .first()
            .expect("nonempty longitude realization has a start")
            .get();
        points.push(Point::new_raw_xy(start, south));
        let coordinates = certified_bound_coordinates(frame, &points, true)?;
        Ok(Shape::Polygon(Polygon::new(
            Ring::try_from_closed(coordinates)?,
            Vec::new(),
        )))
    }

    pub(crate) fn try_from_shape(
        frame: GeographicFrame,
        shape: &Shape,
    ) -> Result<Self, GeographicAdmissionError> {
        GeographicSupportProducts::try_from_shape(frame, shape).map(|products| products.support)
    }

    /// Aggregate primitive support across all shapes before choosing a
    /// circular hull. Row hulls cannot be unioned without losing their gaps.
    #[cfg(test)]
    pub(crate) fn try_from_shapes(
        frame: GeographicFrame,
        shapes: &[impl Borrow<Shape>],
    ) -> Result<Self, GeographicAdmissionError> {
        GeographicSupportProducts::try_from_shapes(frame, shapes).map(|products| products.support)
    }

    pub(crate) const fn latitude_bounds(self) -> Option<(f64, f64)> {
        match self.latitude {
            Some(bounds) => Some((bounds.south.get(), bounds.north.get())),
            None => None,
        }
    }

    pub(crate) fn touches_pole(self, frame: GeographicFrame) -> bool {
        self.latitude.is_some_and(|bounds| {
            bounds.south.identity(frame) == SeamIdentity::Pole
                || bounds.north.identity(frame) == SeamIdentity::Pole
        })
    }
}

impl GeographicSupportProducts {
    pub(crate) fn try_from_points(
        frame: GeographicFrame,
        points: impl Iterator<Item = Point>,
    ) -> Result<Self, GeographicAdmissionError> {
        let mut extrema = CoordinateExtrema::empty();
        let mut support = GeographicSupportAccumulator::default();
        #[cfg(test)]
        let mut coordinate_visits = 0_usize;
        for point in points {
            let shape = Shape::Point(point);
            let mut collection_owners = GeographicCollectionOwners::default();
            ScalarGeographicView::try_new_with_collection_owners_and_visit(
                frame,
                &shape,
                &mut collection_owners,
                |coordinate| {
                    #[cfg(test)]
                    {
                        coordinate_visits = coordinate_visits.wrapping_add(1);
                    }
                    extrema.include(coordinate);
                },
                |event| support.visit(event),
            )?;
        }
        #[cfg(test)]
        {
            support.work.coordinate_visits = coordinate_visits;
        }
        Ok(support.finish(frame, extrema))
    }

    pub(crate) fn try_from_shape_with_bounds(
        frame: GeographicFrame,
        shape: &Shape,
        raw_bounds: Option<Bounds>,
    ) -> Result<Self, GeographicAdmissionError> {
        if let Some(products) =
            raw_bounds.and_then(|bounds| Self::from_ordinary_bounds(frame, bounds))
        {
            return Ok(products);
        }
        Self::try_from_shapes(frame, std::slice::from_ref(shape))
    }

    pub(crate) fn from_ordinary_bounds(frame: GeographicFrame, bounds: Bounds) -> Option<Self> {
        Some(Self {
            support: GeographicSupportEnvelope::from_ordinary_bounds(frame, bounds)?,
            raw_extrema: Some(bounds),
            #[cfg(test)]
            work: GeographicSupportWork::default(),
        })
    }

    pub(crate) fn try_from_shape(
        frame: GeographicFrame,
        shape: &Shape,
    ) -> Result<Self, GeographicAdmissionError> {
        Self::try_from_shape_with_bounds(frame, shape, shape.bounds())
    }

    pub(crate) fn try_from_shapes(
        frame: GeographicFrame,
        shapes: &[impl Borrow<Shape>],
    ) -> Result<Self, GeographicAdmissionError> {
        let mut extrema = CoordinateExtrema::empty();
        let mut support = GeographicSupportAccumulator::default();
        #[cfg(test)]
        let mut coordinate_visits = 0_usize;
        let mut collection_owners = GeographicCollectionOwners::default();
        for shape in shapes {
            ScalarGeographicView::try_new_with_collection_owners_and_visit(
                frame,
                shape.borrow(),
                &mut collection_owners,
                |coordinate| {
                    #[cfg(test)]
                    {
                        coordinate_visits = coordinate_visits.wrapping_add(1);
                    }
                    extrema.include(coordinate);
                },
                |event| support.visit(event),
            )?;
        }

        #[cfg(test)]
        {
            support.work.coordinate_visits = coordinate_visits;
        }
        Ok(support.finish(frame, extrema))
    }
}

fn realization_longitudes(
    interval: CircularInterval,
    axis: super::PeriodicAxis,
) -> Vec<CanonicalLongitude> {
    let seam = axis
        .canonicalize(-axis.half_period())
        .expect("the frame has a finite longitude seam");
    let witnesses = [
        axis.canonicalize(-axis.half_period() / 2.0)
            .expect("quarter-turn witness is finite"),
        axis.canonicalize(0.0).expect("zero longitude is finite"),
        axis.canonicalize(axis.half_period() / 2.0)
            .expect("quarter-turn witness is finite"),
    ];
    let [west_quarter, zero, east_quarter] = witnesses;
    match interval {
        CircularInterval::Empty => Vec::new(),
        CircularInterval::Full => vec![seam, west_quarter, zero, east_quarter, seam],
        CircularInterval::Arc { start, end } => {
            let mut longitudes = Vec::with_capacity(6);
            longitudes.push(start);
            if start.get() < end.get() {
                longitudes.extend(
                    [west_quarter, zero, east_quarter]
                        .into_iter()
                        .filter(|witness| start.get() < witness.get() && witness.get() < end.get()),
                );
            } else if start.get() > end.get() {
                longitudes.extend(
                    [west_quarter, zero, east_quarter]
                        .into_iter()
                        .filter(|witness| start.get() < witness.get()),
                );
                if end != seam {
                    longitudes.push(seam);
                }
                longitudes.extend(
                    [west_quarter, zero, east_quarter]
                        .into_iter()
                        .filter(|witness| witness.get() < end.get()),
                );
            }
            if start != end {
                longitudes.push(end);
            }
            longitudes
        },
    }
}

fn certified_bound_coordinates(
    frame: GeographicFrame,
    points: &[Point],
    closed_ring: bool,
) -> crate::error::Result<CoordSeq> {
    let coordinates = CoordSeq::try_from_points(points)?;
    let mut budget = ExpansionBudget::new("envelope", "computed edges");
    let Some(points) =
        publish_certified_minor_chain(frame, &coordinates, closed_ring, &mut budget)?
    else {
        return Ok(coordinates);
    };
    CoordSeq::try_from_points(&points)
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct LatitudeBounds {
    south: Latitude,
    north: Latitude,
}

#[derive(Clone, Copy)]
struct CoordinateExtrema {
    canonical: bool,
    raw_extrema: Option<Bounds>,
}

impl CoordinateExtrema {
    const fn empty() -> Self {
        Self {
            canonical: true,
            raw_extrema: None,
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "the fast path requires the stored longitude to be its exact canonical spelling"
    )]
    fn include(&mut self, coordinate: super::view::GeographicCoordinate) {
        let longitude = coordinate.longitude().get();
        let native = coordinate.native_xy();
        self.canonical &= native.x == longitude;
        match &mut self.raw_extrema {
            Some(bounds) => bounds.include_xy(native),
            None => {
                self.raw_extrema = Some(Bounds::new_raw(native.x, native.y, native.x, native.y));
            },
        }
    }

    fn linear_envelope(self, frame: GeographicFrame) -> Option<GeographicSupportEnvelope> {
        let axis = frame.longitude();
        let raw = self.raw_extrema?;
        let sweep = raw.maxx() - raw.minx();
        if !self.canonical || sweep >= axis.half_period() {
            return None;
        }
        // In a canonical window narrower than half a period, the complementary
        // gap is strictly larger than every internal gap. Stored edges in this
        // window therefore need no separate arc collection or sort.
        Some(GeographicSupportEnvelope {
            longitude: CircularInterval::from_endpoints(
                axis.canonicalize(raw.minx())
                    .expect("admitted extrema are finite longitudes"),
                axis.canonicalize(raw.maxx())
                    .expect("admitted extrema are finite longitudes"),
            ),
            latitude: Some(LatitudeBounds {
                south: Latitude::from_admitted(frame, raw.miny()),
                north: Latitude::from_admitted(frame, raw.maxy()),
            }),
        })
    }

    fn latitude_bounds(self, frame: GeographicFrame) -> Option<LatitudeBounds> {
        self.raw_extrema.map(|raw| LatitudeBounds {
            south: Latitude::from_admitted(frame, raw.miny()),
            north: Latitude::from_admitted(frame, raw.maxy()),
        })
    }

    const fn raw_extrema(self) -> Option<Bounds> {
        self.raw_extrema
    }
}

#[derive(Clone, Copy, Default)]
struct RingEnvelopeEffects {
    full_longitude: bool,
    north_pole: bool,
    south_pole: bool,
}

impl RingEnvelopeEffects {
    const fn include_ring(&mut self, summary: RingSummary) {
        self.full_longitude |=
            summary.chart_turn != 0 || matches!(summary.domain, RingDomain::FullLongitude(_));
    }

    const fn replay(&mut self, effects: Self) {
        self.full_longitude |= effects.full_longitude;
        self.north_pole |= effects.north_pole;
        self.south_pole |= effects.south_pole;
    }
}

#[cfg(test)]
#[path = "envelope_tests.rs"]
mod tests;
