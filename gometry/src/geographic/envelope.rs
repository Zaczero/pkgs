use std::borrow::Borrow;
use std::marker::PhantomData;

use super::{
    ChartPole, CircularInterval, GeographicAdmissionError, GeographicCollectionOwners,
    GeographicFrame, GeographicRing, Latitude, RingDomain, RingSummary, ScalarGeographicView,
    SeamIdentity, union_circular_intervals,
};
use crate::collections::HashMap;
use crate::geometry::{Bounds, CoordSeq, Point, Polygon, Shape, ShapeOwnerKey};

/// Periodic longitude coverage and occupied latitude extrema in one native frame.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct GeographicSupportEnvelope {
    pub(crate) longitude: CircularInterval,
    latitude: Option<LatitudeBounds>,
}

impl GeographicSupportEnvelope {
    pub(crate) fn try_from_point(
        frame: GeographicFrame,
        point: Point,
    ) -> Result<Self, GeographicAdmissionError> {
        let shape = Shape::Point(point);
        let view = ScalarGeographicView::try_new(frame, &shape)
            .map_err(GeographicAdmissionError::Geographic)?;
        let mut envelope = None;
        view.for_each_coordinate(|coordinate| {
            envelope = Some(Self {
                longitude: CircularInterval::Arc {
                    start: coordinate.longitude(),
                    sweep: 0.0,
                },
                latitude: Some(LatitudeBounds {
                    south: coordinate.latitude(),
                    north: coordinate.latitude(),
                }),
            });
        });
        Ok(envelope.expect("an admitted point supplies one coordinate"))
    }

    pub(crate) fn try_from_points(
        frame: GeographicFrame,
        points: impl Iterator<Item = Point> + Clone,
    ) -> Result<Self, GeographicAdmissionError> {
        let mut extrema = CoordinateExtrema::empty();
        for point in points.clone() {
            let shape = Shape::Point(point);
            let view = ScalarGeographicView::try_new(frame, &shape)
                .map_err(GeographicAdmissionError::Geographic)?;
            view.for_each_coordinate(|coordinate| extrema.include(coordinate));
        }
        if let Some(envelope) = extrema.linear_envelope(frame) {
            return Ok(envelope);
        }
        let mut intervals = Vec::with_capacity(points.size_hint().0);
        for point in points {
            append_coordinate_leaf_intervals(&Shape::Point(point), frame, &mut intervals);
        }
        Ok(Self {
            longitude: union_circular_intervals(frame.longitude(), &intervals),
            latitude: extrema.latitude_bounds(),
        })
    }

    pub(crate) fn try_from_shape_with_bounds(
        frame: GeographicFrame,
        shape: &Shape,
        raw_bounds: Option<Bounds>,
    ) -> Result<Self, GeographicAdmissionError> {
        if let Some(envelope) =
            raw_bounds.and_then(|bounds| Self::from_ordinary_bounds(frame, bounds))
        {
            return Ok(envelope);
        }
        Self::try_from_shape(frame, shape)
    }

    pub(crate) fn from_ordinary_bounds(frame: GeographicFrame, bounds: Bounds) -> Option<Self> {
        if !frame.bounds_certify_ordinary_support(bounds) {
            return None;
        }
        Some(Self {
            longitude: CircularInterval::try_arc(
                frame.longitude(),
                bounds.minx(),
                bounds.maxx() - bounds.minx(),
            )
            .expect("the ordinary support certificate proves a canonical subhalf arc"),
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
            arc @ CircularInterval::Arc { .. } => arc.west_east(axis)?,
        };
        Some(Bounds::new_raw(west, south, east, north))
    }

    pub(crate) fn into_shape(self, frame: GeographicFrame) -> crate::error::Result<Shape> {
        let Some((south, north)) = self.latitude_bounds() else {
            return Ok(Shape::empty_polygon());
        };
        let axis = frame.longitude();
        let (west, east) = match self.longitude {
            CircularInterval::Empty => return Ok(Shape::empty_polygon()),
            CircularInterval::Full => (-axis.half_period(), axis.half_period()),
            CircularInterval::Arc { start, sweep } => (start.get(), start.get() + sweep),
        };
        crate::geometry::geographic_bounds_to_shape(
            Bounds::new_raw(west, south, east, north),
            frame,
        )
    }

    pub(crate) fn try_from_shape(
        frame: GeographicFrame,
        shape: &Shape,
    ) -> Result<Self, GeographicAdmissionError> {
        match shape {
            Shape::Point(point) => Self::try_from_point(frame, *point),
            _ => Self::try_from_shapes(frame, std::slice::from_ref(shape)),
        }
    }

    /// Aggregate primitive support across all shapes before choosing a
    /// circular hull. Row hulls cannot be unioned without losing their gaps.
    pub(crate) fn try_from_shapes(
        frame: GeographicFrame,
        shapes: &[impl Borrow<Shape>],
    ) -> Result<Self, GeographicAdmissionError> {
        let mut extrema = CoordinateExtrema::empty();
        let mut collection_owners = GeographicCollectionOwners::default();
        for shape in shapes {
            ScalarGeographicView::try_new_with_collection_owners_and_visit(
                frame,
                shape.borrow(),
                &mut collection_owners,
                |coordinate| extrema.include(coordinate),
            )?;
        }

        if let Some(envelope) = extrema.linear_envelope(frame) {
            #[cfg(test)]
            {
                let work = GeographicSupportOwnerMemo::admission_work(collection_owners.work());
                record_geographic_support_work(&work);
            }
            return Ok(envelope);
        }

        let mut owners = GeographicSupportOwnerMemo::new_for_shapes(shapes, &mut collection_owners)
            .map_err(GeographicAdmissionError::Resource)?;
        let mut intervals = Vec::new();
        let mut effects = RingEnvelopeEffects::default();
        for shape in shapes {
            let shape = shape.borrow();
            append_geographic_coordinate_intervals(shape, frame, &mut intervals, &mut owners);
            append_geographic_segment_intervals(shape, frame, &mut intervals, &mut owners);
            effects.replay(ring_effects(shape, frame, &mut owners)?);
        }

        let mut latitude = extrema.latitude_bounds();
        if let Some(bounds) = &mut latitude {
            if effects.north_pole {
                bounds.north = Latitude::from_admitted(frame, frame.pole());
            }
            if effects.south_pole {
                bounds.south = Latitude::from_admitted(frame, -frame.pole());
            }
        }
        let longitude = if effects.full_longitude {
            CircularInterval::Full
        } else {
            union_circular_intervals(frame.longitude(), &intervals)
        };

        #[cfg(test)]
        {
            let work = owners.work(collection_owners.work());
            record_geographic_support_work(&work);
        }
        Ok(Self {
            longitude,
            latitude,
        })
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

#[derive(Clone, Copy, Debug, PartialEq)]
struct LatitudeBounds {
    south: Latitude,
    north: Latitude,
}

#[derive(Clone, Copy)]
struct CoordinateExtrema {
    canonical: bool,
    west: f64,
    east: f64,
    south: Option<Latitude>,
    north: Option<Latitude>,
}

impl CoordinateExtrema {
    const fn empty() -> Self {
        Self {
            canonical: true,
            west: f64::INFINITY,
            east: f64::NEG_INFINITY,
            south: None,
            north: None,
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "the fast path requires the stored longitude to be its exact canonical spelling"
    )]
    fn include(&mut self, coordinate: super::view::GeographicCoordinate) {
        let longitude = coordinate.longitude().get();
        self.canonical &= coordinate.native_xy().x == longitude;
        self.west = self.west.min(longitude);
        self.east = self.east.max(longitude);
        let latitude = coordinate.latitude();
        self.south = Some(self.south.map_or(latitude, |current| {
            if latitude.get() < current.get() {
                latitude
            } else {
                current
            }
        }));
        self.north = Some(self.north.map_or(latitude, |current| {
            if latitude.get() > current.get() {
                latitude
            } else {
                current
            }
        }));
    }

    fn linear_envelope(self, frame: GeographicFrame) -> Option<GeographicSupportEnvelope> {
        let axis = frame.longitude();
        let sweep = self.east - self.west;
        if self.south.is_none() || !self.canonical || sweep >= axis.half_period() {
            return None;
        }
        // In a canonical window narrower than half a period, the complementary
        // gap is strictly larger than every internal gap. Stored edges in this
        // window therefore need no separate arc collection or sort.
        let south = self.south?;
        let north = self.north?;
        Some(GeographicSupportEnvelope {
            longitude: CircularInterval::try_arc(axis, self.west, sweep)
                .expect("admitted canonical extrema form an arc"),
            latitude: Some(LatitudeBounds { south, north }),
        })
    }

    const fn latitude_bounds(self) -> Option<LatitudeBounds> {
        match (self.south, self.north) {
            (Some(south), Some(north)) => Some(LatitudeBounds { south, north }),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct CoordinateRunKey<'owner> {
    xs: usize,
    ys: usize,
    len: usize,
    owner: PhantomData<&'owner CoordSeq>,
}

impl<'owner> CoordinateRunKey<'owner> {
    fn new(coordinates: &'owner CoordSeq) -> Option<Self> {
        let owner_count = coordinates.xy_shared_owner_count();
        if owner_count < 2 || (coordinates.len() <= 2 && owner_count <= 2) {
            return None;
        }
        let xs = coordinates.xs();
        let ys = coordinates.ys();
        (!xs.is_empty()).then(|| Self {
            xs: xs.as_ptr().addr(),
            ys: ys.as_ptr().addr(),
            len: xs.len(),
            owner: PhantomData,
        })
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum GeographicOwnerKey<'owner> {
    Collection(ShapeOwnerKey),
    Coordinates(CoordinateRunKey<'owner>),
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum CensusState {
    #[default]
    Unseen,
    SeenOnce,
    Repeated,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum SupportPass {
    #[default]
    None,
    Coordinates,
    Segments,
    Both,
}

#[derive(Default)]
struct GeographicOwnerSummary {
    census: CensusState,
    passes: SupportPass,
    ring: Option<RingSummary>,
    ring_effects: Option<RingEnvelopeEffects>,
}

#[derive(Default)]
pub(crate) struct GeographicSupportOwnerMemo<'owner> {
    owners: Option<HashMap<GeographicOwnerKey<'owner>, GeographicOwnerSummary>>,
    #[cfg(test)]
    admission_work: crate::geographic::GeographicAdmissionWork,
    #[cfg(test)]
    census_work: OwnerCensusWork,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct OwnerCensusWork {
    collection_occurrences: usize,
    collection_descents: usize,
    collection_member_slots: usize,
    coordinate_run_visits: usize,
    repeated_owner_occurrences: usize,
    owner_map_initializations: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GeographicSupportWork {
    admission: crate::geographic::GeographicAdmissionWork,
    census: OwnerCensusWork,
}

#[cfg(test)]
std::thread_local! {
    static LAST_GEOGRAPHIC_SUPPORT_WORK: std::cell::Cell<Option<GeographicSupportWork>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn record_geographic_support_work(work: &GeographicSupportWork) {
    LAST_GEOGRAPHIC_SUPPORT_WORK.with(|last| last.set(Some(*work)));
}

#[cfg(test)]
fn geographic_support_work() -> Option<GeographicSupportWork> {
    LAST_GEOGRAPHIC_SUPPORT_WORK.with(std::cell::Cell::get)
}

impl<'owner> GeographicSupportOwnerMemo<'owner> {
    #[cfg(test)]
    fn new(shape: &'owner Shape) -> crate::error::Result<Self> {
        let mut collection_owners = GeographicCollectionOwners::default();
        Self::new_for_shapes(std::slice::from_ref(shape), &mut collection_owners)
    }

    #[cfg(test)]
    fn admission_work(
        admission: crate::geographic::GeographicAdmissionWork,
    ) -> GeographicSupportWork {
        GeographicSupportWork {
            admission,
            census: OwnerCensusWork::default(),
        }
    }

    fn new_for_shapes(
        shapes: &'owner [impl Borrow<Shape>],
        collection_owners: &mut GeographicCollectionOwners<'owner>,
    ) -> crate::error::Result<Self> {
        if shapes.len() < 2
            && !shapes
                .iter()
                .any(|shape| shape_has_multiple_xy_runs(shape.borrow()))
        {
            collection_owners.discard();
            return Ok(Self::default());
        }
        collection_owners.reset_for_census();
        let mut memo = Self::default();
        for shape in shapes {
            collect_geographic_owner_keys(shape.borrow(), &mut memo, collection_owners)?;
        }
        if let Some(error) = collection_owners.take_entropy_error() {
            return Err(error);
        }
        memo.retain_repeated_owners();
        #[cfg(test)]
        {
            memo.admission_work = collection_owners.work();
        }
        collection_owners.discard();
        Ok(memo)
    }

    fn owner_entry(
        &mut self,
        key: GeographicOwnerKey<'owner>,
    ) -> crate::error::Result<&mut GeographicOwnerSummary> {
        if self.owners.is_none() {
            #[cfg(test)]
            {
                self.census_work.owner_map_initializations =
                    self.census_work.owner_map_initializations.wrapping_add(1);
            }
            self.owners = Some(crate::collections::try_hash_map()?);
        }
        Ok(self
            .owners
            .as_mut()
            .expect("support owner map was initialized")
            .entry(key)
            .or_default())
    }

    fn observe(&mut self, key: GeographicOwnerKey<'owner>) -> crate::error::Result<bool> {
        let first = {
            let owner = self.owner_entry(key)?;
            let first = owner.census == CensusState::Unseen;
            owner.census = match owner.census {
                CensusState::Unseen => CensusState::SeenOnce,
                CensusState::SeenOnce | CensusState::Repeated => CensusState::Repeated,
            };
            first
        };
        Ok(first)
    }

    fn note_repeated_collection(&mut self, key: ShapeOwnerKey) -> crate::error::Result<()> {
        self.owner_entry(GeographicOwnerKey::Collection(key))?
            .census = CensusState::Repeated;
        Ok(())
    }

    fn retain_repeated_owners(&mut self) {
        let empty = if let Some(owners) = self.owners.as_mut() {
            owners.retain(|_, owner| owner.census == CensusState::Repeated);
            owners.is_empty()
        } else {
            return;
        };
        if empty {
            self.owners = None;
        }
    }

    const fn is_active(&self) -> bool {
        self.owners.is_some()
    }

    fn owner_mut(
        &mut self,
        key: GeographicOwnerKey<'owner>,
    ) -> Option<&mut GeographicOwnerSummary> {
        self.owners.as_mut()?.get_mut(&key)
    }

    fn coordinates_seen(&self, key: GeographicOwnerKey<'owner>) -> bool {
        self.owners
            .as_ref()
            .and_then(|owners| owners.get(&key))
            .is_some_and(|owner| {
                matches!(owner.passes, SupportPass::Coordinates | SupportPass::Both)
            })
    }

    fn mark_coordinates_seen(&mut self, key: GeographicOwnerKey<'owner>) {
        if let Some(owner) = self.owner_mut(key) {
            owner.passes = match owner.passes {
                SupportPass::None => SupportPass::Coordinates,
                SupportPass::Segments => SupportPass::Both,
                SupportPass::Coordinates | SupportPass::Both => owner.passes,
            };
        }
    }

    fn ring(&self, key: GeographicOwnerKey<'owner>) -> Option<RingSummary> {
        self.owners.as_ref()?.get(&key)?.ring
    }

    fn remember_ring(&mut self, key: GeographicOwnerKey<'owner>, summary: RingSummary) {
        if let Some(owner) = self.owner_mut(key) {
            owner.ring = Some(summary);
        }
    }

    fn ring_effects(&self, key: GeographicOwnerKey<'owner>) -> Option<RingEnvelopeEffects> {
        self.owners.as_ref()?.get(&key)?.ring_effects
    }

    fn remember_ring_effects(
        &mut self,
        key: GeographicOwnerKey<'owner>,
        effects: RingEnvelopeEffects,
    ) {
        if let Some(owner) = self.owner_mut(key) {
            owner.ring_effects = Some(effects);
        }
    }

    fn first_segment_owner(&mut self, key: GeographicOwnerKey<'owner>) -> bool {
        let Some(owner) = self.owner_mut(key) else {
            return true;
        };
        if matches!(owner.passes, SupportPass::Segments | SupportPass::Both) {
            return false;
        }
        owner.passes = match owner.passes {
            SupportPass::None => SupportPass::Segments,
            SupportPass::Coordinates => SupportPass::Both,
            SupportPass::Segments | SupportPass::Both => owner.passes,
        };
        true
    }

    #[cfg(test)]
    const fn work(
        &self,
        admission: crate::geographic::GeographicAdmissionWork,
    ) -> GeographicSupportWork {
        GeographicSupportWork {
            admission,
            census: self.census_work,
        }
    }
}

fn shape_has_multiple_xy_runs(shape: &Shape) -> bool {
    match shape {
        Shape::GeometryCollection(_) => true,
        Shape::MultiLineString(lines) => lines.len() > 1,
        Shape::Polygon(polygon) => !polygon.holes.is_empty(),
        Shape::MultiPolygon(polygons) => {
            polygons.len() > 1 || polygons.iter().any(|polygon| !polygon.holes.is_empty())
        },
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::LineString(_) | Shape::Empty(..) => false,
    }
}

fn collect_geographic_owner_keys<'owner>(
    shape: &'owner Shape,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
    collection_owners: &mut GeographicCollectionOwners<'owner>,
) -> crate::error::Result<()> {
    match shape {
        Shape::GeometryCollection(collection) => {
            let members = collection.as_slice();
            #[cfg(test)]
            {
                owners.census_work.collection_occurrences =
                    owners.census_work.collection_occurrences.wrapping_add(1);
            }
            if collection.members_are_shared()
                && !collection_owners.observe(ShapeOwnerKey::new(members))
            {
                #[cfg(test)]
                {
                    owners.census_work.repeated_owner_occurrences = owners
                        .census_work
                        .repeated_owner_occurrences
                        .wrapping_add(1);
                }
                owners.note_repeated_collection(ShapeOwnerKey::new(members))?;
                return Ok(());
            }
            #[cfg(test)]
            {
                owners.census_work.collection_descents =
                    owners.census_work.collection_descents.wrapping_add(1);
            }
            for member in members {
                #[cfg(test)]
                {
                    owners.census_work.collection_member_slots =
                        owners.census_work.collection_member_slots.wrapping_add(1);
                }
                collect_geographic_owner_keys(member, owners, collection_owners)?;
            }
        },
        Shape::MultiPoint(points) => census_coordinate_run(points, owners)?,
        Shape::LineString(line) => census_coordinate_run(line, owners)?,
        Shape::MultiLineString(lines) => {
            for line in lines.iter() {
                census_coordinate_run(line, owners)?;
            }
        },
        Shape::Polygon(polygon) => {
            for ring in polygon.rings() {
                census_coordinate_run(ring.coords(), owners)?;
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                for ring in polygon.rings() {
                    census_coordinate_run(ring.coords(), owners)?;
                }
            }
        },
        Shape::Point(_) | Shape::Empty(..) => {},
    }
    Ok(())
}

fn census_coordinate_run<'owner>(
    coordinates: &'owner CoordSeq,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
) -> crate::error::Result<()> {
    #[cfg(test)]
    {
        owners.census_work.coordinate_run_visits =
            owners.census_work.coordinate_run_visits.wrapping_add(1);
    }
    if let Some(key) = CoordinateRunKey::new(coordinates)
        && !owners.observe(GeographicOwnerKey::Coordinates(key))?
    {
        #[cfg(test)]
        {
            owners.census_work.repeated_owner_occurrences = owners
                .census_work
                .repeated_owner_occurrences
                .wrapping_add(1);
        }
    }
    Ok(())
}

fn append_geographic_coordinate_intervals<'owner>(
    shape: &'owner Shape,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
) {
    match shape {
        Shape::GeometryCollection(collection) => {
            let key = (owners.is_active() && collection.members_are_shared())
                .then(|| GeographicOwnerKey::Collection(ShapeOwnerKey::new(collection.as_slice())));
            if let Some(key) = key
                && owners.coordinates_seen(key)
            {
                return;
            }
            if let Some(key) = key {
                owners.mark_coordinates_seen(key);
            }
            for member in collection.as_slice() {
                append_geographic_coordinate_intervals(member, frame, intervals, owners);
            }
        },
        Shape::Point(_) | Shape::Empty(..) => {
            append_coordinate_leaf_intervals(shape, frame, intervals);
        },
        Shape::MultiPoint(points) => {
            append_coordinate_run_intervals(points, shape, frame, intervals, owners);
        },
        Shape::LineString(line) => {
            append_coordinate_run_intervals(line, shape, frame, intervals, owners);
        },
        Shape::MultiLineString(lines) => {
            for line in lines.iter() {
                let line_shape = Shape::LineString(line.clone());
                append_coordinate_run_intervals(line, &line_shape, frame, intervals, owners);
            }
        },
        Shape::Polygon(polygon) => {
            for ring in polygon.rings() {
                let ring_shape = Shape::LineString(ring.as_line());
                append_coordinate_run_intervals(
                    ring.coords(),
                    &ring_shape,
                    frame,
                    intervals,
                    owners,
                );
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                for ring in polygon.rings() {
                    let ring_shape = Shape::LineString(ring.as_line());
                    append_coordinate_run_intervals(
                        ring.coords(),
                        &ring_shape,
                        frame,
                        intervals,
                        owners,
                    );
                }
            }
        },
    }
}

fn append_coordinate_run_intervals<'owner>(
    coordinates: &'owner CoordSeq,
    shape: &Shape,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
) {
    // Closed edge arcs already contain every vertex of a segment chain.
    if coordinates.len() >= 2 && !matches!(shape, Shape::MultiPoint(_)) {
        return;
    }
    let key = owners
        .is_active()
        .then(|| CoordinateRunKey::new(coordinates))
        .flatten()
        .map(GeographicOwnerKey::Coordinates);
    if let Some(key) = key
        && owners.coordinates_seen(key)
    {
        return;
    }
    if let Some(key) = key {
        owners.mark_coordinates_seen(key);
    }
    intervals.reserve(coordinates.len());
    append_coordinate_leaf_intervals(shape, frame, intervals);
}

fn append_coordinate_leaf_intervals(
    shape: &Shape,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
) {
    if matches!(shape, Shape::Point(_)) {
        intervals.reserve(1);
    }
    let axis = frame.longitude();
    ScalarGeographicView::previously_admitted(frame, shape).for_each_coordinate(|coordinate| {
        intervals.push(
            CircularInterval::try_arc(axis, coordinate.longitude().get(), 0.0)
                .expect("admitted longitude gives a canonical point interval"),
        );
    });
}

fn append_geographic_segment_intervals<'owner>(
    shape: &'owner Shape,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
) {
    match shape {
        Shape::GeometryCollection(collection) => {
            if owners.is_active() && collection.members_are_shared() {
                let key = GeographicOwnerKey::Collection(ShapeOwnerKey::new(collection.as_slice()));
                if !owners.first_segment_owner(key) {
                    return;
                }
            }
            for member in collection.as_slice() {
                append_geographic_segment_intervals(member, frame, intervals, owners);
            }
        },
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => {},
        Shape::LineString(line) => append_segment_run_intervals(line, frame, intervals, owners),
        Shape::MultiLineString(lines) => {
            for line in lines.iter() {
                append_segment_run_intervals(line, frame, intervals, owners);
            }
        },
        Shape::Polygon(polygon) => {
            for ring in polygon.rings() {
                append_segment_run_intervals(ring.coords(), frame, intervals, owners);
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                for ring in polygon.rings() {
                    append_segment_run_intervals(ring.coords(), frame, intervals, owners);
                }
            }
        },
    }
}

fn append_segment_run_intervals<'owner>(
    chain: &'owner CoordSeq,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
) {
    let first = owners
        .is_active()
        .then(|| CoordinateRunKey::new(chain))
        .flatten()
        .is_none_or(|key| owners.first_segment_owner(GeographicOwnerKey::Coordinates(key)));
    if first {
        append_segment_chain_intervals(chain, frame, intervals);
    }
}

fn append_segment_chain_intervals(
    chain: &CoordSeq,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
) {
    let axis = frame.longitude();
    intervals.reserve(chain.len().saturating_sub(1));
    for &[left, right] in chain.xs().array_windows::<2>() {
        let step = axis
            .chart_step(left, right)
            .expect("the geographic view admitted every chain coordinate");
        if step.is_circle() {
            intervals.push(CircularInterval::Full);
            continue;
        }
        let delta = step.delta(axis);
        let (start, sweep) = if delta.is_sign_negative() {
            (right, -delta)
        } else {
            (left, delta)
        };
        intervals.push(
            CircularInterval::try_arc(axis, start, sweep)
                .expect("a shortest edge is a valid circular arc"),
        );
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

fn ring_effects<'owner>(
    shape: &'owner Shape,
    frame: GeographicFrame,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
) -> Result<RingEnvelopeEffects, GeographicAdmissionError> {
    let mut effects = RingEnvelopeEffects::default();
    match shape {
        Shape::GeometryCollection(collection) => {
            let key = collection
                .members_are_shared()
                .then(|| GeographicOwnerKey::Collection(ShapeOwnerKey::new(collection.as_slice())));
            if let Some(key) = key
                && let Some(cached) = owners.ring_effects(key)
            {
                return Ok(cached);
            }
            for member in collection.as_slice() {
                effects.replay(ring_effects(member, frame, owners)?);
            }
            if let Some(key) = key {
                owners.remember_ring_effects(key, effects);
            }
        },
        Shape::Polygon(polygon) => {
            effects.replay(polygon_ring_effects(polygon, frame, owners)?);
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                effects.replay(polygon_ring_effects(polygon, frame, owners)?);
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => {},
    }
    Ok(effects)
}

fn polygon_ring_effects<'owner>(
    polygon: &'owner Polygon,
    frame: GeographicFrame,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
) -> Result<RingEnvelopeEffects, GeographicAdmissionError> {
    let shell = ring_summary(&polygon.shell, frame, owners)?;
    let shell_pole = shell.bounded_pole();
    let mut effects = RingEnvelopeEffects::default();
    effects.include_ring(shell);
    // A hole subtracts its bounded cap from this shell. Polygon members are
    // unioned later, so another member can still occupy either pole.
    let mut hole_north = false;
    let mut hole_south = false;
    for hole in polygon.holes.iter() {
        let summary = ring_summary(hole, frame, owners)?;
        effects.include_ring(summary);
        match summary.bounded_pole() {
            Some(ChartPole::North) => hole_north = true,
            Some(ChartPole::South) => hole_south = true,
            None => {},
        }
    }
    effects.north_pole = shell_pole == Some(ChartPole::North) && !hole_north;
    effects.south_pole = shell_pole == Some(ChartPole::South) && !hole_south;
    Ok(effects)
}

fn ring_summary<'owner>(
    ring: &'owner crate::geometry::Ring,
    frame: GeographicFrame,
    owners: &mut GeographicSupportOwnerMemo<'owner>,
) -> Result<RingSummary, GeographicAdmissionError> {
    let key = owners
        .is_active()
        .then(|| CoordinateRunKey::new(ring.coords()))
        .flatten()
        .map(GeographicOwnerKey::Coordinates);
    if let Some(key) = key
        && let Some(summary) = owners.ring(key)
    {
        return Ok(summary);
    }
    let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
        .map_err(GeographicAdmissionError::Geographic)?;
    if let Some(key) = key {
        owners.remember_ring(key, summary);
    }
    Ok(summary)
}

#[cfg(test)]
#[path = "envelope_tests.rs"]
mod tests;
