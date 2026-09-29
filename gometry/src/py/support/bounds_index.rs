use std::collections::HashMap;
use std::marker::PhantomData;

use rstar::Envelope as _;

use crate::geographic::{
    CircularInterval, GeographicAdmissionError, GeographicCollectionOwners, GeographicFrame,
    GeographicRing, GeographicViewError, LongitudeWindows, RingDomain, RingSummary, RingTurn,
    ScalarGeographicView, SeamIdentity, union_circular_intervals,
};
use crate::geometry::{CoordSeq, ShapeOwnerKey, TopologyDomain};
use crate::predicates::engine::IndexEnvelope;
use crate::py::errors::InvalidGeometryError;
use crate::py::support::{AABB, Bounds, Point, Shape};

pub(crate) fn bounds_envelope(bounds: Bounds) -> AABB<[f64; 2]> {
    // Bounds are ordered by construction — skip from_corners' min/max pass.
    AABB::from_bounds([bounds.minx(), bounds.miny()], [
        bounds.maxx(),
        bounds.maxy(),
    ])
}

/// How many windows a cover may hold before further parts merge into the ones
/// it has.
///
/// A descent is cheap and a loose window is not, so this sits high enough that
/// an ordinary multipart geometry keeps its parts: a cap that forces merges
/// sprawls them across the gaps, and the cover is then rejected outright for
/// covering no less ground than one box did. What the cap exists for is the
/// row that would otherwise put a window on every vertex — a MultiPoint of a
/// hundred thousand points — since each window is another tree entry that the
/// index holds for as long as the row lives.
const MAX_COVER_WINDOWS: usize = 64;

/// Where a shape sits, as the index sees it.
///
/// One box around a scattered multipart geometry also covers the ground
/// between its parts, which for widely separated parts is nearly all of the
/// box — a search admits every row lying in the gaps and pays an exact test on
/// each. Covering the parts themselves ties selectivity to the ground a
/// geometry occupies rather than to how its parts happen to be packaged, so a
/// MultiPolygon narrows a search as well as an array of its polygons does.
#[derive(Clone, Debug)]
pub(crate) enum EnvelopeCover {
    /// Nothing worth decomposing; these windows serve every lookup.
    Whole(Vec<AABB<[f64; 2]>>),
    /// Parts far enough apart to be worth searching separately, and the
    /// whole-shape windows containment still needs.
    Scattered {
        whole: Vec<AABB<[f64; 2]>>,
        parts: Vec<AABB<[f64; 2]>>,
    },
}

impl EnvelopeCover {
    /// The windows a candidate search visits under `policy`.
    ///
    /// Containment reads the whole shape even when a cover is available: a row
    /// spanning two parts lies inside the whole and inside no single part, so
    /// searching the parts would drop it.
    pub(crate) fn windows(&self, policy: IndexEnvelope) -> &[AABB<[f64; 2]>] {
        match self {
            Self::Whole(whole) => whole,
            Self::Scattered { whole, parts } => match policy {
                IndexEnvelope::Intersecting => parts,
                IndexEnvelope::ContainedInQuery => whole,
            },
        }
    }

    /// The windows stored for an indexed row, where parts are sound for every
    /// lookup: each is a subset of the whole, so a containment search gains
    /// candidates to refine away rather than losing any, and a nearest lower
    /// bound only tightens.
    pub(crate) fn entries(&self) -> &[AABB<[f64; 2]>] {
        match self {
            Self::Whole(whole) => whole,
            Self::Scattered { parts, .. } => parts,
        }
    }
}

/// Minimal conservative index windows in the coordinate system of `frame`.
/// Periodic frames split an arc at the canonical seam and retain the logical
/// row id on every physical entry.
pub(crate) fn index_envelopes(
    shape: &Shape,
    bounds: Bounds,
    domain: TopologyDomain,
) -> pyo3::PyResult<EnvelopeCover> {
    let planar = bounds_envelope(bounds);
    let whole = match domain {
        TopologyDomain::Cartesian => vec![planar],
        TopologyDomain::Geographic(frame) => geographic_index_envelopes(shape, frame)?,
    };
    // Coordinate bounds describe a part's ground faithfully only where the
    // frame had nothing to add about the whole shape. A seam-crossing or
    // pole-enclosing geometry gets windows coordinate bounds cannot express —
    // an enclosed pole reaches a latitude no vertex holds — and parts taken
    // from coordinates could then miss ground the shape covers.
    let [only] = whole.as_slice() else {
        return Ok(EnvelopeCover::Whole(whole));
    };
    if *only != planar {
        return Ok(EnvelopeCover::Whole(whole));
    }
    Ok(match part_cover(shape, planar) {
        Some(parts) => EnvelopeCover::Scattered { whole, parts },
        None => EnvelopeCover::Whole(whole),
    })
}

/// A cover of `shape`'s parts, or `None` where splitting it up buys nothing.
fn part_cover(shape: &Shape, whole: AABB<[f64; 2]>) -> Option<Vec<AABB<[f64; 2]>>> {
    if !matches!(
        shape,
        Shape::MultiPoint(_)
            | Shape::MultiLineString(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
    ) {
        return None;
    }
    let mut cover = Vec::new();
    // Keep each occurrence in source order. A repeated part can newly qualify
    // for an earlier free merge, and merging a contained AABB can still change
    // a signed-zero endpoint.
    for_each_part(shape, &mut |part| admit(&mut cover, part));
    // The extra entries and descents have to buy something. Adjacent parts
    // merge back into one window, and a compact geometry then covers exactly
    // what its single box already did.
    let covered: f64 = cover.iter().map(margin).sum();
    (cover.len() > 1 && covered < margin(&whole)).then_some(cover)
}

/// Fold `part` into `cover`, holding it to [`MAX_COVER_WINDOWS`] windows.
///
/// A merge spending no more margin than the two windows already spend is free:
/// overlapping and adjacent parts collapse into one window and distant parts
/// stay apart. Once the cover is full the cheapest merge is taken instead.
fn admit(cover: &mut Vec<AABB<[f64; 2]>>, part: AABB<[f64; 2]>) {
    let part_margin = margin(&part);
    let mut free = None;
    let mut cheapest: Option<(usize, f64)> = None;
    for (index, window) in cover.iter().enumerate() {
        let merged = margin(&window.merged(&part));
        if merged <= margin(window) + part_margin {
            free = Some(index);
            break;
        }
        let growth = merged - margin(window);
        if cheapest.is_none_or(|(_, best)| growth < best) {
            cheapest = Some((index, growth));
        }
    }
    if let Some(index) = free {
        let window = cover
            .get_mut(index)
            .expect("free merge index came from the cover");
        *window = window.merged(&part);
        return;
    }
    if cover.len() < MAX_COVER_WINDOWS {
        cover.push(part);
        return;
    }
    let (index, _) = cheapest.expect("a full cover holds a window to merge into");
    let window = cover
        .get_mut(index)
        .expect("cheapest merge index came from the cover");
    *window = window.merged(&part);
}

/// Half the perimeter — the standard R-tree margin. Unlike area it stays
/// discriminating for a degenerate window, so a run of collinear points does
/// not read as free to merge into the segment through them.
fn margin(envelope: &AABB<[f64; 2]>) -> f64 {
    let lower = envelope.lower();
    let upper = envelope.upper();
    (upper[0] - lower[0]) + (upper[1] - lower[1])
}

/// Every part of `shape` that occupies ground, as its own window.
fn for_each_part(shape: &Shape, visit: &mut impl FnMut(AABB<[f64; 2]>)) {
    match shape {
        Shape::MultiPoint(points) => {
            for point in points {
                visit(bounds_envelope(Bounds::from_point(point)));
            }
        },
        Shape::MultiLineString(lines) => {
            for line in lines.iter() {
                if let Some(bounds) = Bounds::from_coords(line) {
                    visit(bounds_envelope(bounds));
                }
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                // The shell bounds the polygon; its holes lie inside.
                if let Some(bounds) = Bounds::from_coords(polygon.shell.coords()) {
                    visit(bounds_envelope(bounds));
                }
            }
        },
        Shape::GeometryCollection(members) => {
            for member in members {
                for_each_part(member, visit);
            }
        },
        Shape::Point(_) | Shape::LineString(_) | Shape::Polygon(_) | Shape::Empty(..) => {
            if let Some(bounds) = shape.bounds() {
                visit(bounds_envelope(bounds));
            }
        },
    }
}

fn geographic_view_py_error(error: GeographicViewError) -> pyo3::PyErr {
    InvalidGeometryError::new_err(error.to_string())
}

fn geographic_index_envelopes(
    shape: &Shape,
    frame: GeographicFrame,
) -> pyo3::PyResult<Vec<AABB<[f64; 2]>>> {
    #[cfg(test)]
    reset_geographic_index_work();
    let mut collection_owners = GeographicCollectionOwners::default();
    ScalarGeographicView::try_new_with_collection_owners(frame, shape, &mut collection_owners)
        .map_err(|error| match error {
            GeographicAdmissionError::Geographic(error) => geographic_view_py_error(error),
            GeographicAdmissionError::Resource(error) => error.into(),
        })?;
    let mut intervals = Vec::new();
    let mut owners =
        GeographicIndexOwnerMemo::new_with_collection_owners(shape, &mut collection_owners)?;
    let coordinates = geographic_coordinate_summary(shape, frame, &mut intervals, &mut owners);
    append_geographic_segment_intervals(shape, frame, &mut intervals, &mut owners);
    if intervals.is_empty() {
        #[cfg(test)]
        record_geographic_index_work(&owners);
        return Ok(Vec::new());
    }
    let rings = ring_effects(shape, frame, &mut owners)?;
    let mut south = coordinates.south;
    let mut north = coordinates.north;
    let full_longitude = coordinates.full_longitude || rings.full_longitude;
    if rings.north_pole {
        north = frame.pole();
    }
    if rings.south_pole {
        south = -frame.pole();
    }
    let longitude = if full_longitude {
        CircularInterval::Full
    } else {
        union_circular_intervals(frame.longitude(), intervals)
    };
    let mut envelopes = Vec::with_capacity(2);
    let mut push = |window: [f64; 2]| {
        envelopes.push(AABB::from_bounds([window[0], south], [window[1], north]));
    };
    match longitude.windows(frame.longitude()) {
        Some(LongitudeWindows::One(window)) => push(window),
        Some(LongitudeWindows::Two(left, right)) => {
            push(left);
            push(right);
        },
        Some(LongitudeWindows::Full) => {
            push([
                -frame.longitude().half_period(),
                frame.longitude().half_period(),
            ]);
        },
        None => {},
    }
    #[cfg(test)]
    record_geographic_index_work(&owners);
    Ok(envelopes)
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
        // Two-point owners with at most two references can save at most one
        // cheap fold; larger fan-out earns the summary memo.
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

#[derive(Default)]
struct GeographicOwnerSummary {
    census_seen: bool,
    repeated: bool,
    coordinates: Option<GeographicCoordinateSummary>,
    ring: Option<RingSummary>,
    ring_effects: Option<RingEnvelopeEffects>,
    segments_seen: bool,
}

#[derive(Default)]
struct GeographicIndexOwnerMemo<'owner> {
    owners: Option<HashMap<GeographicOwnerKey<'owner>, GeographicOwnerSummary, ahash::RandomState>>,
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
struct GeographicIndexWork {
    admission: crate::geographic::GeographicAdmissionWork,
    census: OwnerCensusWork,
}

#[cfg(test)]
std::thread_local! {
    static LAST_GEOGRAPHIC_INDEX_WORK: std::cell::Cell<Option<GeographicIndexWork>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn reset_geographic_index_work() {
    LAST_GEOGRAPHIC_INDEX_WORK.with(|work| work.set(None));
}

#[cfg(test)]
fn record_geographic_index_work(owners: &GeographicIndexOwnerMemo<'_>) {
    LAST_GEOGRAPHIC_INDEX_WORK.with(|work| {
        work.set(Some(GeographicIndexWork {
            admission: owners.admission_work,
            census: owners.census_work,
        }));
    });
}

#[cfg(test)]
fn geographic_index_work() -> Option<GeographicIndexWork> {
    LAST_GEOGRAPHIC_INDEX_WORK.with(std::cell::Cell::get)
}

impl<'owner> GeographicIndexOwnerMemo<'owner> {
    #[cfg(test)]
    fn new(shape: &'owner Shape) -> pyo3::PyResult<Self> {
        let mut collection_owners = GeographicCollectionOwners::default();
        Self::new_with_collection_owners(shape, &mut collection_owners)
    }

    fn new_with_collection_owners(
        shape: &'owner Shape,
        collection_owners: &mut GeographicCollectionOwners<'owner>,
    ) -> pyo3::PyResult<Self> {
        if !shape_has_multiple_xy_runs(shape) {
            collection_owners.discard();
            return Ok(Self::default());
        }
        collection_owners.reset_for_census();
        let mut memo = Self::default();
        collect_geographic_owner_keys(shape, &mut memo, collection_owners)?;
        if let Some(error) = collection_owners.take_entropy_error() {
            return Err(error.into());
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
    ) -> pyo3::PyResult<&mut GeographicOwnerSummary> {
        if self.owners.is_none() {
            #[cfg(test)]
            {
                self.census_work.owner_map_initializations =
                    self.census_work.owner_map_initializations.wrapping_add(1);
            }
            let state = crate::collections::collection_hash_state()
                .map_err(crate::PyErr::from)?
                .clone();
            self.owners = Some(HashMap::with_hasher(state));
        }
        Ok(self
            .owners
            .as_mut()
            .expect("owner map was initialized")
            .entry(key)
            .or_default())
    }

    fn observe(&mut self, key: GeographicOwnerKey<'owner>) -> pyo3::PyResult<bool> {
        let repeated = {
            let owner = self.owner_entry(key)?;
            if owner.census_seen {
                owner.repeated = true;
                true
            } else {
                owner.census_seen = true;
                false
            }
        };
        if repeated { Ok(false) } else { Ok(true) }
    }

    fn note_repeated_collection(&mut self, key: ShapeOwnerKey) -> pyo3::PyResult<()> {
        self.owner_entry(GeographicOwnerKey::Collection(key))?
            .repeated = true;
        Ok(())
    }

    fn retain_repeated_owners(&mut self) {
        let empty = if let Some(owners) = self.owners.as_mut() {
            owners.retain(|_, owner| owner.repeated);
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

    fn owner(&self, key: GeographicOwnerKey<'owner>) -> Option<&GeographicOwnerSummary> {
        self.owners.as_ref()?.get(&key)
    }

    fn owner_mut(
        &mut self,
        key: GeographicOwnerKey<'owner>,
    ) -> Option<&mut GeographicOwnerSummary> {
        self.owners.as_mut()?.get_mut(&key)
    }

    fn coordinates(&self, key: GeographicOwnerKey<'owner>) -> Option<GeographicCoordinateSummary> {
        self.owner(key)?.coordinates
    }

    fn remember_coordinates(
        &mut self,
        key: GeographicOwnerKey<'owner>,
        summary: GeographicCoordinateSummary,
    ) {
        if let Some(owner) = self.owner_mut(key) {
            owner.coordinates = Some(summary);
        }
    }

    fn ring(&self, key: GeographicOwnerKey<'owner>) -> Option<RingSummary> {
        self.owner(key)?.ring
    }

    fn remember_ring(&mut self, key: GeographicOwnerKey<'owner>, summary: RingSummary) {
        if let Some(owner) = self.owner_mut(key) {
            owner.ring = Some(summary);
        }
    }

    fn ring_effects(&self, key: GeographicOwnerKey<'owner>) -> Option<RingEnvelopeEffects> {
        self.owner(key)?.ring_effects
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
        if owner.segments_seen {
            false
        } else {
            owner.segments_seen = true;
            true
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
    owners: &mut GeographicIndexOwnerMemo<'owner>,
    collection_owners: &mut GeographicCollectionOwners<'owner>,
) -> pyo3::PyResult<()> {
    match shape {
        Shape::GeometryCollection(collection) => {
            let members = collection.as_slice();
            #[cfg(test)]
            {
                owners.census_work.collection_occurrences =
                    owners.census_work.collection_occurrences.wrapping_add(1);
            }
            // An unshared member slice cannot recur in this immutable shape.
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
    owners: &mut GeographicIndexOwnerMemo<'owner>,
) -> pyo3::PyResult<()> {
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

#[derive(Clone, Copy)]
struct GeographicCoordinateSummary {
    south: f64,
    north: f64,
    full_longitude: bool,
}

impl GeographicCoordinateSummary {
    const fn empty() -> Self {
        Self {
            south: f64::INFINITY,
            north: f64::NEG_INFINITY,
            full_longitude: false,
        }
    }

    fn include(
        &mut self,
        latitude: f64,
        longitude: f64,
        is_pole: bool,
        frame: GeographicFrame,
        intervals: &mut Vec<CircularInterval>,
    ) {
        self.south = self.south.min(latitude);
        self.north = self.north.max(latitude);
        if is_pole {
            self.full_longitude = true;
            if latitude.is_sign_negative() {
                self.south = -frame.pole();
            } else {
                self.north = frame.pole();
            }
        }
        intervals.push(
            CircularInterval::try_arc(frame.longitude(), longitude, 0.0)
                .expect("admitted longitude makes a point interval"),
        );
    }

    const fn replay(&mut self, summary: Self) {
        self.south = self.south.min(summary.south);
        self.north = self.north.max(summary.north);
        self.full_longitude |= summary.full_longitude;
    }
}

fn geographic_coordinate_summary<'owner>(
    shape: &'owner Shape,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
    owners: &mut GeographicIndexOwnerMemo<'owner>,
) -> GeographicCoordinateSummary {
    if !owners.is_active() {
        return geographic_coordinate_leaf_summary(shape, frame, intervals);
    }
    match shape {
        Shape::GeometryCollection(collection) => {
            let key = collection
                .members_are_shared()
                .then(|| GeographicOwnerKey::Collection(ShapeOwnerKey::new(collection.as_slice())));
            if let Some(key) = key
                && let Some(summary) = owners.coordinates(key)
            {
                // Coordinates were completely admitted before this memoized
                // pass. Longitude intervals are set members; latitude extrema
                // replay at this collection's original fold position.
                return summary;
            }
            let mut summary = GeographicCoordinateSummary::empty();
            for member in collection.as_slice() {
                summary.replay(geographic_coordinate_summary(
                    member, frame, intervals, owners,
                ));
            }
            if let Some(key) = key {
                owners.remember_coordinates(key, summary);
            }
            summary
        },
        Shape::Point(_) => geographic_coordinate_leaf_summary(shape, frame, intervals),
        Shape::MultiPoint(points) => {
            geographic_coordinate_run_summary(points, shape, frame, intervals, owners)
        },
        Shape::LineString(line) => {
            geographic_coordinate_run_summary(line, shape, frame, intervals, owners)
        },
        Shape::MultiLineString(lines) => {
            let mut summary = GeographicCoordinateSummary::empty();
            for line in lines.iter() {
                let line_shape = Shape::LineString(line.clone());
                summary.replay(geographic_coordinate_run_summary(
                    line,
                    &line_shape,
                    frame,
                    intervals,
                    owners,
                ));
            }
            summary
        },
        Shape::Polygon(polygon) => {
            let mut summary = GeographicCoordinateSummary::empty();
            for ring in polygon.rings() {
                let ring_shape = Shape::LineString(ring.as_line());
                summary.replay(geographic_coordinate_run_summary(
                    ring.coords(),
                    &ring_shape,
                    frame,
                    intervals,
                    owners,
                ));
            }
            summary
        },
        Shape::MultiPolygon(polygons) => {
            let mut summary = GeographicCoordinateSummary::empty();
            for polygon in polygons.iter() {
                for ring in polygon.rings() {
                    let ring_shape = Shape::LineString(ring.as_line());
                    summary.replay(geographic_coordinate_run_summary(
                        ring.coords(),
                        &ring_shape,
                        frame,
                        intervals,
                        owners,
                    ));
                }
            }
            summary
        },
        Shape::Empty(..) => GeographicCoordinateSummary::empty(),
    }
}

fn geographic_coordinate_run_summary<'owner>(
    coordinates: &'owner CoordSeq,
    shape: &Shape,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
    owners: &mut GeographicIndexOwnerMemo<'owner>,
) -> GeographicCoordinateSummary {
    if coordinates.is_empty() {
        return GeographicCoordinateSummary::empty();
    }
    let key = CoordinateRunKey::new(coordinates).map(GeographicOwnerKey::Coordinates);
    if let Some(key) = key
        && let Some(summary) = owners.coordinates(key)
    {
        return summary;
    }
    let summary = geographic_coordinate_leaf_summary(shape, frame, intervals);
    if let Some(key) = key {
        owners.remember_coordinates(key, summary);
    }
    summary
}

fn geographic_coordinate_leaf_summary(
    shape: &Shape,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
) -> GeographicCoordinateSummary {
    let mut summary = GeographicCoordinateSummary::empty();
    ScalarGeographicView::previously_admitted(frame, shape).for_each_coordinate(|coordinate| {
        summary.include(
            coordinate.latitude().get(),
            coordinate.longitude().get(),
            coordinate.identity(frame) == SeamIdentity::Pole,
            frame,
            intervals,
        );
    });
    summary
}

fn append_geographic_segment_intervals<'owner>(
    shape: &'owner Shape,
    frame: GeographicFrame,
    intervals: &mut Vec<CircularInterval>,
    owners: &mut GeographicIndexOwnerMemo<'owner>,
) {
    if !owners.is_active() {
        shape.for_each_segment_chain(|chain| {
            append_segment_chain_intervals(chain, frame, intervals);
        });
        return;
    }
    match shape {
        Shape::GeometryCollection(collection) => {
            if collection.members_are_shared() {
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
        Shape::LineString(line) => {
            append_segment_run_intervals(line, frame, intervals, owners);
        },
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
    owners: &mut GeographicIndexOwnerMemo<'owner>,
) {
    let first = CoordinateRunKey::new(chain)
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
    for &[left, right] in chain.xs().array_windows::<2>() {
        let delta = frame
            .longitude()
            .shortest_edge(left, right)
            .expect("the geographic view admitted every chain coordinate")
            .delta(frame.longitude());
        let (start, sweep) = if delta.is_sign_negative() {
            (right, -delta)
        } else {
            (left, delta)
        };
        intervals.push(
            CircularInterval::try_arc(frame.longitude(), start, sweep)
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
    const fn include(&mut self, summary: RingSummary) {
        self.full_longitude |= matches!(summary.domain, RingDomain::FullLongitude(_));
        if matches!(summary.turn, RingTurn::Full(_)) {
            if summary.south.get().is_sign_positive() {
                self.north_pole = true;
            } else if summary.north.get().is_sign_negative() {
                self.south_pole = true;
            }
        }
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
    owners: &mut GeographicIndexOwnerMemo<'owner>,
) -> pyo3::PyResult<RingEnvelopeEffects> {
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
            return Ok(effects);
        },
        Shape::Polygon(polygon) => {
            append_polygon_ring_effects(polygon, frame, &mut effects, owners)?;
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons.iter() {
                append_polygon_ring_effects(polygon, frame, &mut effects, owners)?;
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

fn append_polygon_ring_effects<'owner>(
    polygon: &'owner crate::geometry::Polygon,
    frame: GeographicFrame,
    effects: &mut RingEnvelopeEffects,
    owners: &mut GeographicIndexOwnerMemo<'owner>,
) -> pyo3::PyResult<()> {
    for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
        let key = owners
            .is_active()
            .then(|| CoordinateRunKey::new(ring.coords()))
            .flatten()
            .map(GeographicOwnerKey::Coordinates);
        let summary = if let Some(key) = key {
            if let Some(summary) = owners.ring(key) {
                summary
            } else {
                let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
                    .map_err(geographic_view_py_error)?;
                owners.remember_ring(key, summary);
                summary
            }
        } else {
            RingSummary::try_new(frame, &GeographicRing::from_ring(ring))
                .map_err(geographic_view_py_error)?
        };
        effects.include(summary);
    }
    Ok(())
}

pub(crate) fn global_geographic_candidate_envelope() -> AABB<[f64; 2]> {
    AABB::from_bounds([-f64::MAX, -f64::MAX], [f64::MAX, f64::MAX])
}

pub(crate) fn point_from_bounds(bounds: Bounds) -> Option<Point> {
    (bounds.minx().total_cmp(&bounds.maxx()).is_eq()
        && bounds.miny().total_cmp(&bounds.maxy()).is_eq())
    .then_some(Point::new_raw_xy(bounds.minx(), bounds.miny()))
}

#[cfg(test)]
mod periodic_tests {
    use super::*;
    use crate::geographic::{AngularFactor, GeographicError};
    use crate::geometry::{CoordinateAxes, LineSeq, MultiMember, MultiPoint, Polygon, Ring};

    fn frame(radians_per_unit: f64) -> GeographicFrame {
        let unit = AngularFactor::try_new("test unit", radians_per_unit).unwrap();
        GeographicFrame::try_new(unit, unit, 0.0).unwrap()
    }

    fn multipoint(points: &[(f64, f64)]) -> Shape {
        Shape::MultiPoint(
            MultiPoint::try_new(
                CoordinateAxes::XY,
                points
                    .iter()
                    .map(|&(x, y)| MultiMember::Value(Point::new(x, y).unwrap()))
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn polygon(xs: &[f64], ys: &[f64], holes: Vec<Ring>) -> Shape {
        Shape::Polygon(Polygon {
            shell: Ring::closed(
                xs.iter()
                    .zip(ys)
                    .map(|(&x, &y)| Point::new(x, y).unwrap())
                    .collect(),
            )
            .unwrap(),
            holes: crate::geometry::SharedSlice::from_vec(holes),
        })
    }

    fn line(xs: &[f64], ys: &[f64]) -> Shape {
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_vecs(xs.to_vec(), ys.to_vec(), None, None).unwrap(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn shared_collection_admission_returns_the_first_original_geographic_error() {
        let degrees = frame(std::f64::consts::PI / 180.0);
        let shared_bad_owner = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(f64::INFINITY, 100.0)),
        ])
        .unwrap();
        let shape = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![shared_bad_owner.clone()]).unwrap(),
            Shape::geometry_collection(vec![shared_bad_owner]).unwrap(),
        ])
        .unwrap();

        let mut owners = GeographicCollectionOwners::default();
        let Err(GeographicAdmissionError::Geographic(error)) =
            ScalarGeographicView::try_new_with_collection_owners(degrees, &shape, &mut owners)
        else {
            panic!("the first invalid shared member must retain its geographic error");
        };
        assert_eq!(error.location.members.as_slice(), [0, 0, 1]);
        assert_eq!(error.location.coordinate, 0);
        assert!(matches!(
            error.source,
            GeographicError::NonFinite {
                name: "longitude",
                value
            } if value == f64::INFINITY
        ));

        pyo3::Python::initialize();
        let public_error = pyo3::Python::attach(|_| {
            geographic_index_envelopes(&shape, degrees)
                .expect_err("public index admission rejects the original invalid coordinate")
                .to_string()
        });
        assert!(public_error.contains("member 0, member 0, member 1"));
        assert!(public_error.contains("longitude"));
    }

    #[test]
    fn shared_collection_census_source_work_stays_linear_at_depth_16_and_128() {
        for depth in [16_usize, 128] {
            let mut shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
            for _ in 0..depth {
                shape = Shape::geometry_collection(vec![shape.clone(), shape])
                    .expect("nested shared collection stays within the depth limit");
            }

            let owners = GeographicIndexOwnerMemo::new(&shape).unwrap();
            let repeated = depth.wrapping_sub(1);
            assert_eq!(
                owners.census_work.collection_occurrences,
                depth.wrapping_mul(2).wrapping_sub(1)
            );
            assert_eq!(owners.census_work.collection_descents, depth);
            assert_eq!(
                owners.census_work.collection_member_slots,
                depth.wrapping_mul(2)
            );
            assert_eq!(owners.census_work.coordinate_run_visits, 0);
            assert_eq!(owners.census_work.repeated_owner_occurrences, repeated);
            assert_eq!(owners.census_work.owner_map_initializations, 1);
            assert_eq!(owners.owners.as_ref().map(HashMap::len), Some(repeated));
        }
    }

    #[test]
    fn public_geographic_index_envelopes_stay_linear_on_shared_collection_dags() {
        let degrees = frame(std::f64::consts::PI / 180.0);
        let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let expected = geographic_index_envelopes(&point, degrees).unwrap();

        for depth in [16_usize, 128] {
            let mut shape = point.clone();
            for _ in 0..depth {
                shape = Shape::geometry_collection(vec![shape.clone(), shape])
                    .expect("nested shared collection stays within the depth limit");
            }

            let actual = geographic_index_envelopes(&shape, degrees).unwrap();
            let work = geographic_index_work().expect("index work was recorded");
            let repeated = depth.wrapping_sub(1);
            let occurrences = depth.wrapping_mul(2).wrapping_sub(1);
            let slots = depth.wrapping_mul(2);

            assert_eq!(envelope_bits(&actual), envelope_bits(&expected));
            assert_eq!(work.admission.collection_occurrences, occurrences);
            assert_eq!(work.admission.collection_descents, depth);
            assert_eq!(work.admission.collection_member_slots, slots);
            assert_eq!(work.admission.coordinate_visits, 2);
            assert_eq!(work.admission.repeated_owner_occurrences, repeated);
            assert_eq!(work.admission.owner_set_initializations, 1);
            assert_eq!(work.census.collection_occurrences, occurrences);
            assert_eq!(work.census.collection_descents, depth);
            assert_eq!(work.census.collection_member_slots, slots);
            assert_eq!(work.census.coordinate_run_visits, 0);
            assert_eq!(work.census.repeated_owner_occurrences, repeated);
            assert_eq!(work.census.owner_map_initializations, 1);
        }
    }

    #[test]
    fn repeated_nested_collection_owner_is_replayed_across_distinct_parents() {
        let degrees = frame(std::f64::consts::PI / 180.0);
        let half = degrees.longitude().half_period();
        let pole = degrees.pole();
        let shared_members = || {
            vec![
                multipoint(&[(-179.0, 0.0), (179.0, -0.0)]),
                line(&[170.0, -170.0], &[5.0, 5.0]),
                polygon(
                    &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                    &[pole / 2.0; 4],
                    Vec::new(),
                ),
            ]
        };
        let shared = Shape::geometry_collection(shared_members()).unwrap();
        let left = Shape::geometry_collection(vec![
            shared.clone(),
            Shape::Point(Point::new_raw_xy(-30.0, 0.0)),
        ])
        .unwrap();
        let right =
            Shape::geometry_collection(vec![Shape::Point(Point::new_raw_xy(50.0, 0.0)), shared])
                .unwrap();
        let repeated = Shape::geometry_collection(vec![left, right]).unwrap();
        let expanded = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![
                Shape::geometry_collection(shared_members()).unwrap(),
                Shape::Point(Point::new_raw_xy(-30.0, 0.0)),
            ])
            .unwrap(),
            Shape::geometry_collection(vec![
                Shape::Point(Point::new_raw_xy(50.0, 0.0)),
                Shape::geometry_collection(shared_members()).unwrap(),
            ])
            .unwrap(),
        ])
        .unwrap();

        let owners = GeographicIndexOwnerMemo::new(&repeated).unwrap();
        assert_eq!(owners.census_work.collection_occurrences, 5);
        assert_eq!(owners.census_work.collection_descents, 4);
        assert_eq!(owners.census_work.collection_member_slots, 9);
        assert_eq!(owners.census_work.coordinate_run_visits, 3);
        assert_eq!(owners.census_work.repeated_owner_occurrences, 1);
        assert_eq!(owners.census_work.owner_map_initializations, 1);
        assert_eq!(owners.owners.as_ref().map(HashMap::len), Some(1));

        let repeated = geographic_index_envelopes(&repeated, degrees).unwrap();
        let expanded = geographic_index_envelopes(&expanded, degrees).unwrap();
        assert_eq!(envelope_bits(&repeated), envelope_bits(&expanded));
    }

    #[test]
    fn distinct_owner_census_visits_each_run_without_initializing_memo() {
        let lines = (0..256)
            .map(|line| {
                let x = f64::from(line);
                self::line(&[x, x + 0.25], &[0.0, 0.5])
            })
            .collect();
        let shape = Shape::geometry_collection(lines).unwrap();
        let degrees = frame(std::f64::consts::PI / 180.0);

        let (admitted, admission_allocations) = crate::test_support::allocation_count(|| {
            let mut collection_owners = GeographicCollectionOwners::default();
            ScalarGeographicView::try_new_with_collection_owners(
                degrees,
                &shape,
                &mut collection_owners,
            )
        });
        admitted.expect("distinct unshared collection payloads are admitted");
        assert_eq!(admission_allocations, 0);

        let owners = GeographicIndexOwnerMemo::new(&shape).unwrap();
        assert_eq!(owners.census_work.collection_occurrences, 1);
        assert_eq!(owners.census_work.collection_descents, 1);
        assert_eq!(owners.census_work.collection_member_slots, 256);
        assert_eq!(owners.census_work.coordinate_run_visits, 256);
        assert_eq!(owners.census_work.repeated_owner_occurrences, 0);
        assert_eq!(owners.census_work.owner_map_initializations, 0);
        assert!(owners.owners.is_none());

        assert!(
            !geographic_index_envelopes(&shape, degrees)
                .unwrap()
                .is_empty()
        );
        let work = geographic_index_work().expect("index work was recorded");
        assert_eq!(work.admission.collection_occurrences, 1);
        assert_eq!(work.admission.collection_descents, 1);
        assert_eq!(work.admission.collection_member_slots, 256);
        assert_eq!(work.admission.coordinate_visits, 512);
        assert_eq!(work.admission.repeated_owner_occurrences, 0);
        assert_eq!(work.admission.owner_set_initializations, 0);
        assert_eq!(work.census.collection_occurrences, 1);
        assert_eq!(work.census.collection_descents, 1);
        assert_eq!(work.census.collection_member_slots, 256);
        assert_eq!(work.census.coordinate_run_visits, 256);
        assert_eq!(work.census.repeated_owner_occurrences, 0);
        assert_eq!(work.census.owner_map_initializations, 0);
    }

    fn envelope_bits(envelopes: &[AABB<[f64; 2]>]) -> Vec<([u64; 2], [u64; 2])> {
        envelopes
            .iter()
            .map(|envelope| {
                let [lower_x, lower_y] = envelope.lower();
                let [upper_x, upper_y] = envelope.upper();
                ([lower_x.to_bits(), lower_y.to_bits()], [
                    upper_x.to_bits(),
                    upper_y.to_bits(),
                ])
            })
            .collect()
    }

    #[test]
    fn shared_collection_interval_memo_matches_expanded_occurrences() {
        let degrees = frame(std::f64::consts::PI / 180.0);
        let shared_members = vec![
            multipoint(&[(-179.0, 0.0), (179.0, -0.0)]),
            line(&[170.0, -170.0], &[5.0, 5.0]),
        ];
        let shared = Shape::geometry_collection(shared_members.clone()).unwrap();
        let middle = Shape::Point(Point::new_raw_xy(0.0, -0.0));
        let repeated =
            Shape::geometry_collection(vec![shared.clone(), middle.clone(), shared]).unwrap();
        let expanded = Shape::geometry_collection(vec![
            Shape::geometry_collection(shared_members.clone()).unwrap(),
            middle,
            Shape::geometry_collection(shared_members).unwrap(),
        ])
        .unwrap();

        let repeated = geographic_index_envelopes(&repeated, degrees).unwrap();
        let expanded = geographic_index_envelopes(&expanded, degrees).unwrap();
        assert_eq!(envelope_bits(&repeated), envelope_bits(&expanded));
    }

    #[test]
    fn short_shared_coordinate_runs_preserve_envelopes_across_distinct_collections() {
        let degrees = frame(std::f64::consts::PI / 180.0);
        let shared_points = multipoint(&[(-179.0, 0.0), (179.0, -0.0)]);
        let shared_single_point = multipoint(&[(45.0, 2.0)]);
        let shared_line = line(&[170.0, -170.0], &[5.0, 5.0]);
        let middle = Shape::Point(Point::new_raw_xy(0.0, -0.0));
        let repeated = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![
                shared_points.clone(),
                shared_single_point.clone(),
                shared_line.clone(),
            ])
            .unwrap(),
            middle.clone(),
            Shape::geometry_collection(vec![shared_points, shared_single_point, shared_line])
                .unwrap(),
        ])
        .unwrap();
        let expanded = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![
                multipoint(&[(-179.0, 0.0), (179.0, -0.0)]),
                multipoint(&[(45.0, 2.0)]),
                line(&[170.0, -170.0], &[5.0, 5.0]),
            ])
            .unwrap(),
            middle,
            Shape::geometry_collection(vec![
                multipoint(&[(-179.0, 0.0), (179.0, -0.0)]),
                multipoint(&[(45.0, 2.0)]),
                line(&[170.0, -170.0], &[5.0, 5.0]),
            ])
            .unwrap(),
        ])
        .unwrap();

        let owners = GeographicIndexOwnerMemo::new(&repeated).unwrap();
        assert_eq!(owners.census_work.coordinate_run_visits, 6);
        assert_eq!(owners.census_work.repeated_owner_occurrences, 0);
        assert_eq!(owners.census_work.owner_map_initializations, 0);
        assert!(owners.owners.is_none());

        let repeated = geographic_index_envelopes(&repeated, degrees).unwrap();
        let expanded = geographic_index_envelopes(&expanded, degrees).unwrap();
        assert_eq!(envelope_bits(&repeated), envelope_bits(&expanded));
    }

    #[test]
    fn repeated_short_coordinate_owner_is_memoized() {
        let degrees = frame(std::f64::consts::PI / 180.0);
        let shared_points = multipoint(&[(-179.0, 0.0), (179.0, -0.0)]);
        let repeated = Shape::geometry_collection(vec![shared_points; 256]).unwrap();
        let expanded = Shape::geometry_collection(
            std::iter::repeat_with(|| multipoint(&[(-179.0, 0.0), (179.0, -0.0)]))
                .take(256)
                .collect(),
        )
        .unwrap();

        let owners = GeographicIndexOwnerMemo::new(&repeated).unwrap();
        assert_eq!(owners.census_work.coordinate_run_visits, 256);
        assert_eq!(owners.census_work.repeated_owner_occurrences, 255);
        assert_eq!(owners.census_work.owner_map_initializations, 1);
        assert_eq!(owners.owners.as_ref().map(HashMap::len), Some(1));

        let repeated = geographic_index_envelopes(&repeated, degrees).unwrap();
        let expanded = geographic_index_envelopes(&expanded, degrees).unwrap();
        assert_eq!(envelope_bits(&repeated), envelope_bits(&expanded));
    }

    #[test]
    fn shared_collection_ring_memo_replays_pole_effects() {
        let degrees = frame(std::f64::consts::PI / 180.0);
        let half = degrees.longitude().half_period();
        let pole = degrees.pole();
        let members = vec![polygon(
            &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
            &[pole / 2.0; 4],
            Vec::new(),
        )];
        let shared = Shape::geometry_collection(members.clone()).unwrap();
        let middle = Shape::Point(Point::new_raw_xy(-30.0, 0.0));
        let repeated =
            Shape::geometry_collection(vec![shared.clone(), middle.clone(), shared]).unwrap();
        let expanded = Shape::geometry_collection(vec![
            Shape::geometry_collection(members.clone()).unwrap(),
            middle,
            Shape::geometry_collection(members).unwrap(),
        ])
        .unwrap();

        let repeated = geographic_index_envelopes(&repeated, degrees).unwrap();
        let expanded = geographic_index_envelopes(&expanded, degrees).unwrap();
        assert_eq!(envelope_bits(&repeated), envelope_bits(&expanded));
    }

    #[test]
    fn shared_ring_run_memo_replays_pole_effects_across_collections() {
        let degrees = frame(std::f64::consts::PI / 180.0);
        let half = degrees.longitude().half_period();
        let pole = degrees.pole();
        let shared_polygon = polygon(
            &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
            &[pole / 2.0; 4],
            Vec::new(),
        );
        let middle = Shape::Point(Point::new_raw_xy(-30.0, 0.0));
        let repeated = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![shared_polygon.clone()]).unwrap(),
            middle.clone(),
            Shape::geometry_collection(vec![shared_polygon]).unwrap(),
        ])
        .unwrap();
        let expanded = Shape::geometry_collection(vec![
            Shape::geometry_collection(vec![polygon(
                &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                &[pole / 2.0; 4],
                Vec::new(),
            )])
            .unwrap(),
            middle,
            Shape::geometry_collection(vec![polygon(
                &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                &[pole / 2.0; 4],
                Vec::new(),
            )])
            .unwrap(),
        ])
        .unwrap();

        let repeated = geographic_index_envelopes(&repeated, degrees).unwrap();
        let expanded = geographic_index_envelopes(&expanded, degrees).unwrap();
        assert_eq!(envelope_bits(&repeated), envelope_bits(&expanded));
    }

    #[test]
    fn part_cover_replays_repeated_owner_admissions_in_source_order() {
        let repeated_member = line(&[10.0, 10.0], &[10.0, 11.0]);
        let shared = Shape::geometry_collection(vec![repeated_member]).unwrap();
        let shape = Shape::geometry_collection(vec![
            line(&[0.0, 0.0], &[0.0, 1.0]),
            shared.clone(),
            line(&[0.0, 9.5], &[0.0, 11.0]),
            shared,
            Shape::Point(Point::new_raw_xy(1000.0, 1000.0)),
        ])
        .unwrap();
        let whole = bounds_envelope(shape.bounds().unwrap());
        let parts = part_cover(&shape, whole).expect("distant parts benefit from a cover");
        let first = parts.first().expect("the first admission remains first");
        assert_eq!(first.lower(), [0.0, 0.0]);
        assert_eq!(first.upper(), [10.0, 11.0]);
    }

    #[test]
    fn aliases_collapse_and_seam_neighbors_split_minimally() {
        for native in [
            frame(std::f64::consts::PI / 180.0),
            frame(1.0),
            frame(std::f64::consts::TAU / 400.0),
        ] {
            let period = native.longitude().period();
            let aliases =
                geographic_index_envelopes(&multipoint(&[(0.0, 0.0), (period, 0.0)]), native)
                    .unwrap();
            assert_eq!(aliases.len(), 1);
            assert_eq!(aliases[0].lower(), [0.0, 0.0]);
            assert_eq!(aliases[0].upper(), [0.0, 0.0]);
        }

        let degrees = frame(std::f64::consts::PI / 180.0);
        let seam = geographic_index_envelopes(&multipoint(&[(-179.0, 0.0), (179.0, 0.0)]), degrees)
            .unwrap();
        assert_eq!(seam.len(), 2);
        assert_eq!((seam[0].upper()[0] - seam[0].lower()[0]).abs(), 1.0);
        assert_eq!((seam[1].upper()[0] - seam[1].lower()[0]).abs(), 1.0);
    }

    #[test]
    fn typed_poles_caps_holes_and_axis_bands_own_full_longitude() {
        for native in [
            frame(std::f64::consts::PI / 180.0),
            frame(1.0),
            frame(std::f64::consts::TAU / 400.0),
        ] {
            let half = native.longitude().half_period();
            let pole = native.pole();
            for sign in [-1.0, 1.0] {
                let latitude = sign * pole;
                let point = geographic_index_envelopes(
                    &Shape::Point(Point::new(half / 3.0, latitude).unwrap()),
                    native,
                )
                .unwrap();
                assert_eq!(point[0].lower(), [-half, latitude]);
                assert_eq!(point[0].upper(), [half, latitude]);

                let exterior = if sign.is_sign_positive() {
                    latitude.next_up()
                } else {
                    latitude.next_down()
                };
                let admitted = geographic_index_envelopes(
                    &Shape::Point(Point::new(-half / 4.0, exterior).unwrap()),
                    native,
                )
                .unwrap();
                assert_eq!(admitted[0].lower(), [-half, latitude]);
                assert_eq!(admitted[0].upper(), [half, latitude]);

                let rejected = if sign.is_sign_positive() {
                    exterior.next_up()
                } else {
                    exterior.next_down()
                };
                geographic_index_envelopes(
                    &Shape::Point(Point::new(0.0, rejected).unwrap()),
                    native,
                )
                .unwrap_err();
            }

            let cap = polygon(
                &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                &[pole / 2.0; 4],
                Vec::new(),
            );
            let cap = geographic_index_envelopes(&cap, native).unwrap();
            assert_eq!(cap[0].lower(), [-half, pole / 2.0]);
            assert_eq!(cap[0].upper(), [half, pole]);

            let south_cap = polygon(
                &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                &[-pole / 2.0; 4],
                Vec::new(),
            );
            let south_cap = geographic_index_envelopes(&south_cap, native).unwrap();
            assert_eq!(south_cap[0].lower(), [-half, -pole]);
            assert_eq!(south_cap[0].upper(), [half, -pole / 2.0]);

            let band = polygon(
                &[-half, half, half, -half, -half],
                &[
                    -pole / 2.0,
                    -pole / 2.0,
                    pole / 2.0,
                    pole / 2.0,
                    -pole / 2.0,
                ],
                Vec::new(),
            );
            let band = geographic_index_envelopes(&band, native).unwrap();
            assert_eq!(band[0].lower(), [-half, -pole / 2.0]);
            assert_eq!(band[0].upper(), [half, pole / 2.0]);

            let hole = Ring::closed(vec![
                Point::new(0.0, pole * 3.0 / 4.0).unwrap(),
                Point::new(half * 2.0 / 3.0, pole * 3.0 / 4.0).unwrap(),
                Point::new(half * 4.0 / 3.0, pole * 3.0 / 4.0).unwrap(),
                Point::new(half * 2.0, pole * 3.0 / 4.0).unwrap(),
            ])
            .unwrap();
            let annulus = polygon(
                &[0.0, half * 2.0 / 3.0, half * 4.0 / 3.0, half * 2.0],
                &[pole / 2.0; 4],
                vec![hole],
            );
            let annulus = geographic_index_envelopes(&annulus, native).unwrap();
            assert_eq!(annulus[0].lower(), [-half, pole / 2.0]);
            assert_eq!(annulus[0].upper()[0], half);
            assert_eq!(annulus[0].upper()[1], pole);
        }
    }
}
