use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::ops::ControlFlow;

use super::{
    CanonicalLongitude, ChartConnection, ChartPole, FullTurnPole, GeographicError, GeographicFrame,
    Latitude, RingTurn, SeamIdentity,
};
use crate::collections::HashSet;
use crate::geometry::storage::admission::CheckedRingWindow;
use crate::geometry::{
    CoordSeq, MultiMember, Ring, RunIdentity, Shape, ShapeOwnerKey, SharedShapeOwners,
    SupportRunKind, XY,
};

const INLINE_MEMBER_DEPTH: usize = 4;

/// Tracks immutable collection payload identities across admission visits.
///
/// The first shared payload is stored inline; ordinary unshared collections
/// leave all owner storage empty.
#[derive(Default)]
pub(crate) struct GeographicCollectionOwners<'owner> {
    inline: Option<ShapeOwnerKey>,
    hashed: Option<HashSet<ShapeOwnerKey>>,
    entropy_fallback: Option<BTreeSet<ShapeOwnerKey>>,
    entropy_error: Option<crate::error::Error>,
    _owner: PhantomData<&'owner Shape>,
}

impl GeographicCollectionOwners<'_> {
    pub(crate) fn observe(&mut self, key: ShapeOwnerKey) -> bool {
        if let Some(owners) = self.hashed.as_mut() {
            return owners.insert(key);
        }
        if let Some(owners) = self.entropy_fallback.as_mut() {
            return owners.insert(key);
        }
        let Some(first) = self.inline else {
            self.inline = Some(key);
            return true;
        };
        if first == key {
            return false;
        }
        match crate::collections::collection_hash_state() {
            Ok(state) => {
                let mut owners = HashSet::with_capacity_and_hasher(2, state.clone());
                let _ = owners.insert(first);
                let first = owners.insert(key);
                self.inline = None;
                self.hashed = Some(owners);
                first
            },
            Err(error) => {
                let mut owners = BTreeSet::new();
                let _ = owners.insert(first);
                let first = owners.insert(key);
                self.inline = None;
                self.entropy_fallback = Some(owners);
                self.entropy_error = Some(error);
                first
            },
        }
    }

    pub(crate) const fn take_entropy_error(&mut self) -> Option<crate::error::Error> {
        self.entropy_error.take()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GeographicMemberPath {
    inline: [usize; INLINE_MEMBER_DEPTH],
    len: usize,
    overflow: Vec<usize>,
}

impl GeographicMemberPath {
    pub(crate) fn as_slice(&self) -> &[usize] {
        if self.len <= INLINE_MEMBER_DEPTH {
            &self.inline[..self.len]
        } else {
            &self.overflow
        }
    }
}

#[derive(Default)]
struct MemberPathBuilder {
    inline: [usize; INLINE_MEMBER_DEPTH],
    len: usize,
    overflow: Vec<usize>,
}

impl MemberPathBuilder {
    fn push(&mut self, member: usize) {
        if self.len < INLINE_MEMBER_DEPTH {
            self.inline[self.len] = member;
        } else {
            if self.overflow.is_empty() {
                self.overflow.reserve(INLINE_MEMBER_DEPTH + 1);
                self.overflow.extend_from_slice(&self.inline);
            }
            self.overflow.push(member);
        }
        self.len += 1;
    }

    fn pop(&mut self) {
        self.len -= 1;
        if self.len >= INLINE_MEMBER_DEPTH {
            self.overflow.pop();
        } else if self.len + 1 == INLINE_MEMBER_DEPTH {
            self.overflow.clear();
        }
    }

    fn location(&self) -> GeographicMemberPath {
        GeographicMemberPath {
            inline: self.inline,
            len: self.len,
            overflow: self.overflow.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GeographicLocation {
    pub(crate) row: Option<usize>,
    pub(crate) members: GeographicMemberPath,
    pub(crate) ring: Option<usize>,
    pub(crate) coordinate: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GeographicDiagnostic {
    pub(crate) location: GeographicLocation,
    pub(crate) native: XY,
    pub(crate) source: GeographicError,
}

impl fmt::Display for GeographicDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(row) = self.location.row {
            write!(f, "geographic row {row}")?;
        } else {
            f.write_str("geographic shape")?;
        }
        for member in self.location.members.as_slice() {
            write!(f, ", member {member}")?;
        }
        if let Some(ring) = self.location.ring {
            write!(f, ", ring {ring}")?;
        }
        write!(
            f,
            ", coordinate {}: {}",
            self.location.coordinate, self.source
        )
    }
}

pub(crate) type GeographicViewError = Box<GeographicDiagnostic>;

enum GeographicVisitError {
    Diagnostic(GeographicViewError),
    OrdinaryScreenMismatch,
}

impl From<GeographicViewError> for GeographicVisitError {
    fn from(error: GeographicViewError) -> Self {
        Self::Diagnostic(error)
    }
}

impl GeographicVisitError {
    fn into_diagnostic(self) -> GeographicViewError {
        match self {
            Self::Diagnostic(error) => error,
            Self::OrdinaryScreenMismatch => {
                unreachable!("full geographic admission does not run the ordinary screen")
            },
        }
    }
}

#[derive(Debug)]
pub(crate) enum GeographicAdmissionError {
    Geographic(GeographicViewError),
    Resource(crate::error::Error),
}

impl From<GeographicAdmissionError> for crate::error::Error {
    fn from(error: GeographicAdmissionError) -> Self {
        match error {
            GeographicAdmissionError::Geographic(diagnostic) => {
                super::lonlat::geographic_admission_error(&diagnostic)
            },
            GeographicAdmissionError::Resource(error) => error,
        }
    }
}

impl GeographicDiagnostic {
    fn boxed(location: GeographicLocation, native: XY, source: GeographicError) -> Box<Self> {
        Box::new(Self {
            location,
            native,
            source,
        })
    }
}

impl std::error::Error for GeographicDiagnostic {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct GeographicCoordinate {
    native_xy: XY,
    longitude: CanonicalLongitude,
    latitude: Latitude,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SharedCoordinateRunKey {
    xs: usize,
    ys: usize,
    len: usize,
    kind: SupportRunKind,
}

impl SharedCoordinateRunKey {
    fn new(coordinates: &CoordSeq, kind: SupportRunKind) -> Option<Self> {
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
            kind,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum GeographicAdmissionEvent {
    BeginSupportRun {
        kind: SupportRunKind,
        key: Option<SharedCoordinateRunKey>,
    },
    EndSupportRun,
    Point(CanonicalLongitude),
    Segment(ChartConnection),
    Ring {
        summary: RingSummary,
        hole: bool,
    },
    EndPolygon,
}

impl GeographicCoordinate {
    pub(crate) const fn native_xy(self) -> XY {
        self.native_xy
    }

    pub(crate) const fn longitude(self) -> CanonicalLongitude {
        self.longitude
    }
    pub(crate) const fn latitude(self) -> Latitude {
        self.latitude
    }
    pub(crate) fn identity(self, frame: GeographicFrame) -> SeamIdentity {
        let latitude = self.latitude.identity(frame);
        if latitude == SeamIdentity::Pole {
            latitude
        } else {
            self.longitude.identity(frame.longitude())
        }
    }
}

fn coordinate(
    frame: GeographicFrame,
    native_xy: XY,
    location: impl FnOnce() -> GeographicLocation,
) -> Result<GeographicCoordinate, GeographicViewError> {
    let longitude = match frame.longitude().canonicalize(native_xy.x) {
        Ok(longitude) => longitude,
        Err(source) => {
            return Err(GeographicDiagnostic::boxed(location(), native_xy, source));
        },
    };
    let latitude = match Latitude::try_new(frame, native_xy.y) {
        Ok(latitude) => latitude,
        Err(source) => {
            return Err(GeographicDiagnostic::boxed(location(), native_xy, source));
        },
    };
    Ok(GeographicCoordinate {
        native_xy,
        longitude,
        latitude,
    })
}

fn coordinate_for_visit<const ORDINARY_SCREEN: bool>(
    frame: GeographicFrame,
    native_xy: XY,
    location: impl FnOnce() -> GeographicLocation,
) -> Result<GeographicCoordinate, GeographicVisitError> {
    if ORDINARY_SCREEN {
        let Some(longitude) = frame.longitude().ordinary_longitude(native_xy.x) else {
            return Err(GeographicVisitError::OrdinaryScreenMismatch);
        };
        let latitude = Latitude::try_new(frame, native_xy.y)
            .map_err(|_| GeographicVisitError::OrdinaryScreenMismatch)?;
        return Ok(GeographicCoordinate {
            native_xy,
            longitude,
            latitude,
        });
    }
    coordinate(frame, native_xy, location).map_err(GeographicVisitError::Diagnostic)
}

fn visit_coordseq<const ORDINARY_SCREEN: bool, const SUPPORT_EVENTS: bool>(
    frame: GeographicFrame,
    coords: &CoordSeq,
    row: Option<usize>,
    members: &MemberPathBuilder,
    ring: Option<usize>,
    run_owners: &mut Option<SharedShapeOwners>,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicVisitError>,
    support_visit: &mut impl FnMut(GeographicAdmissionEvent),
) -> Result<(), GeographicVisitError> {
    let kind = if coords.len() == 1 {
        SupportRunKind::PointSet
    } else {
        SupportRunKind::SegmentChain
    };
    let first = run_owners.as_mut().is_none_or(|owners| {
        if ORDINARY_SCREEN || SUPPORT_EVENTS {
            owners.first_support_run(coords, RunIdentity::XY, kind)
        } else {
            owners.first_coord_seq(coords, RunIdentity::XY)
        }
    });
    if !first {
        return Ok(());
    }
    let axis = frame.longitude();
    if SUPPORT_EVENTS {
        support_visit(GeographicAdmissionEvent::BeginSupportRun {
            kind,
            key: SharedCoordinateRunKey::new(coords, kind),
        });
    }
    let mut previous_x = None;
    for (coordinate_index, (&x, &y)) in coords.xs().iter().zip(coords.ys()).enumerate() {
        let native_xy = XY::new(x, y);
        if ORDINARY_SCREEN
            && let Some(previous_x) = previous_x
            && !axis.ordinary_chart_edge(previous_x, x)
        {
            return Err(GeographicVisitError::OrdinaryScreenMismatch);
        }
        let admitted =
            coordinate_for_visit::<ORDINARY_SCREEN>(frame, native_xy, || GeographicLocation {
                row,
                members: members.location(),
                ring,
                coordinate: coordinate_index,
            })?;
        visit(admitted)?;
        if SUPPORT_EVENTS {
            if coords.len() == 1 {
                support_visit(GeographicAdmissionEvent::Point(admitted.longitude()));
            } else if let Some(previous_x) = previous_x {
                let connection = axis.chart_connection(previous_x, x).map_err(|source| {
                    GeographicVisitError::Diagnostic(GeographicDiagnostic::boxed(
                        GeographicLocation {
                            row,
                            members: members.location(),
                            ring,
                            coordinate: coordinate_index,
                        },
                        native_xy,
                        source,
                    ))
                })?;
                support_visit(GeographicAdmissionEvent::Segment(connection));
            }
        }
        previous_x = Some(x);
    }
    if SUPPORT_EVENTS {
        support_visit(GeographicAdmissionEvent::EndSupportRun);
    }
    Ok(())
}

pub(crate) fn ring_gate(
    chart_turn: i64,
    south: Latitude,
    north: Latitude,
    location: GeographicLocation,
    native: XY,
) -> Result<(), GeographicViewError> {
    if matches!(chart_turn, -1 | 1)
        && FullTurnPole::of_latitude_span(south.get(), north.get()) == FullTurnPole::Ambiguous
    {
        return Err(GeographicDiagnostic::boxed(
            location,
            native,
            GeographicError::AmbiguousFullTurnRing {
                south: south.get(),
                north: north.get(),
            },
        ));
    }
    Ok(())
}

fn visit_ring_coordseq<const ORDINARY_SCREEN: bool, const SUPPORT_EVENTS: bool>(
    frame: GeographicFrame,
    coords: &CoordSeq,
    row: Option<usize>,
    members: &MemberPathBuilder,
    ring: usize,
    run_owners: &mut Option<SharedShapeOwners>,
    pending_gate: &mut Option<GeographicViewError>,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicVisitError>,
    support_visit: &mut impl FnMut(GeographicAdmissionEvent),
) -> Result<(), GeographicVisitError> {
    let repeated = run_owners.as_mut().is_some_and(|owners| {
        if ORDINARY_SCREEN || SUPPORT_EVENTS {
            !owners.first_support_run(coords, RunIdentity::XY, SupportRunKind::SegmentChain)
        } else {
            !owners.first_coord_seq(coords, RunIdentity::XY)
        }
    });
    if ORDINARY_SCREEN && repeated {
        return Ok(());
    }
    if SUPPORT_EVENTS {
        let kind = SupportRunKind::SegmentChain;
        support_visit(GeographicAdmissionEvent::BeginSupportRun {
            kind,
            key: SharedCoordinateRunKey::new(coords, kind),
        });
    }
    let mut chart_turn = 0_i64;
    let mut previous: Option<XY> = None;
    let mut first = None;
    let axis = frame.longitude();
    let mut latitude_span = None;
    let mut seam_only_vertices = true;
    let mut has_seam_vertex = false;
    let mut west = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut exact_axis_band = ExactAxisBand::default();
    for (index, (&x, &y)) in coords.xs().iter().zip(coords.ys()).enumerate() {
        let native_xy = XY::new(x, y);
        let admitted = if repeated && !ORDINARY_SCREEN {
            admitted_coordinate(frame, native_xy)
        } else {
            coordinate_for_visit::<ORDINARY_SCREEN>(frame, native_xy, || GeographicLocation {
                row,
                members: members.location(),
                ring: Some(ring),
                coordinate: index,
            })?
        };
        if !repeated {
            visit(admitted)?;
        }
        if !ORDINARY_SCREEN {
            first.get_or_insert_with(|| admitted.native_xy());
        }
        if !ORDINARY_SCREEN {
            let span = latitude_span.get_or_insert_with(|| LatitudeSpan::new(admitted.latitude()));
            span.include(admitted.latitude());
            match admitted.identity(frame) {
                SeamIdentity::Seam => has_seam_vertex = true,
                SeamIdentity::Pole => {},
                SeamIdentity::Ordinary => seam_only_vertices = false,
            }
            west = west.min(x);
            east = east.max(x);
            if let Some(previous) = previous {
                if SUPPORT_EVENTS {
                    let location = || GeographicLocation {
                        row,
                        members: members.location(),
                        ring: Some(ring),
                        coordinate: index,
                    };
                    let connection =
                        axis.chart_connection(previous.x, native_xy.x)
                            .map_err(|source| {
                                GeographicDiagnostic::boxed(location(), native_xy, source)
                            })?;
                    exact_axis_band.include_edge(frame, connection, previous, native_xy);
                    support_visit(GeographicAdmissionEvent::Segment(connection));
                    chart_turn = chart_turn
                        .checked_add(i64::from(connection.source_lift))
                        .ok_or_else(|| {
                            GeographicDiagnostic::boxed(
                                location(),
                                native_xy,
                                GeographicError::ChartTurnOverflow,
                            )
                        })?;
                } else if !axis.ordinary_chart_edge(previous.x, native_xy.x) {
                    let location = || GeographicLocation {
                        row,
                        members: members.location(),
                        ring: Some(ring),
                        coordinate: index,
                    };
                    chart_turn = axis
                        .add_chart_transit(chart_turn, previous.x, native_xy.x)
                        .map_err(|source| {
                            GeographicDiagnostic::boxed(location(), native_xy, source)
                        })?;
                }
            }
        }
        if ORDINARY_SCREEN
            && let Some(previous) = previous
            && !axis.ordinary_chart_edge(previous.x, native_xy.x)
        {
            return Err(GeographicVisitError::OrdinaryScreenMismatch);
        }
        previous = Some(native_xy);
    }
    if !ORDINARY_SCREEN {
        let first = first.expect("polygon rings have at least four admitted coordinates");
        let span = latitude_span.expect("polygon rings have at least four admitted coordinates");
        if pending_gate.is_none()
            && matches!(chart_turn, -1 | 1)
            && let Err(error) = ring_gate(
                chart_turn,
                span.south,
                span.north,
                GeographicLocation {
                    row,
                    members: members.location(),
                    ring: Some(ring),
                    coordinate: 0,
                },
                first,
            )
        {
            *pending_gate = Some(error);
        }
        if SUPPORT_EVENTS {
            support_visit(GeographicAdmissionEvent::EndSupportRun);
            let exact_axis_band =
                exact_axis_band.is_exact(frame, west, east, span.south, span.north);
            support_visit(GeographicAdmissionEvent::Ring {
                summary: RingSummary::from_admitted_facts(
                    frame,
                    chart_turn,
                    seam_only_vertices,
                    has_seam_vertex,
                    exact_axis_band,
                    span.south,
                    span.north,
                ),
                hole: ring != 0,
            });
        }
    }
    Ok(())
}

fn visit_shape<'owner, const ORDINARY_SCREEN: bool, const SUPPORT_EVENTS: bool>(
    frame: GeographicFrame,
    shape: &'owner Shape,
    row: Option<usize>,
    members: &mut MemberPathBuilder,
    mut collection_owners: Option<&mut GeographicCollectionOwners<'owner>>,
    run_owners: &mut Option<SharedShapeOwners>,
    pending_gate: &mut Option<GeographicViewError>,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicVisitError>,
    support_visit: &mut impl FnMut(GeographicAdmissionEvent),
) -> Result<(), GeographicVisitError> {
    match shape {
        Shape::Point(point) => {
            let admitted =
                coordinate_for_visit::<ORDINARY_SCREEN>(frame, XY::from(*point), || {
                    GeographicLocation {
                        row,
                        members: members.location(),
                        ring: None,
                        coordinate: 0,
                    }
                })?;
            visit(admitted)?;
            if SUPPORT_EVENTS {
                support_visit(GeographicAdmissionEvent::Point(admitted.longitude()));
            }
            Ok(())
        },
        Shape::MultiPoint(points) => {
            if run_owners.as_mut().is_some_and(|owners| {
                if SUPPORT_EVENTS {
                    !owners.first_support_run(points, RunIdentity::XY, SupportRunKind::PointSet)
                } else {
                    !owners.first_coord_seq(points, RunIdentity::XY)
                }
            }) {
                return Ok(());
            }
            if SUPPORT_EVENTS {
                let kind = SupportRunKind::PointSet;
                support_visit(GeographicAdmissionEvent::BeginSupportRun {
                    kind,
                    key: SharedCoordinateRunKey::new(points, kind),
                });
            }
            for (logical, member) in points.members().enumerate() {
                if let MultiMember::Value(physical) = member {
                    if !ORDINARY_SCREEN {
                        members.push(logical);
                    }
                    let native_xy = points
                        .xs()
                        .get(physical)
                        .zip(points.ys().get(physical))
                        .map(|(&x, &y)| XY::new(x, y))
                        .expect("MultiPoint member refers to stored XY columns");
                    let admitted =
                        coordinate_for_visit::<ORDINARY_SCREEN>(frame, native_xy, || {
                            GeographicLocation {
                                row,
                                members: members.location(),
                                ring: None,
                                coordinate: 0,
                            }
                        })?;
                    visit(admitted)?;
                    if SUPPORT_EVENTS {
                        support_visit(GeographicAdmissionEvent::Point(admitted.longitude()));
                    }
                    if !ORDINARY_SCREEN {
                        members.pop();
                    }
                }
            }
            if SUPPORT_EVENTS {
                support_visit(GeographicAdmissionEvent::EndSupportRun);
            }
            Ok(())
        },
        Shape::LineString(line) => visit_coordseq::<ORDINARY_SCREEN, SUPPORT_EVENTS>(
            frame,
            line,
            row,
            members,
            None,
            run_owners,
            visit,
            support_visit,
        ),
        Shape::MultiLineString(lines) => {
            for (logical, member) in lines.members().enumerate() {
                if let MultiMember::Value(physical) = member {
                    if !ORDINARY_SCREEN {
                        members.push(logical);
                    }
                    visit_coordseq::<ORDINARY_SCREEN, SUPPORT_EVENTS>(
                        frame,
                        &lines.as_slice()[physical],
                        row,
                        members,
                        None,
                        run_owners,
                        visit,
                        support_visit,
                    )?;
                    if !ORDINARY_SCREEN {
                        members.pop();
                    }
                }
            }
            Ok(())
        },
        Shape::Polygon(polygon) => {
            for (ring, coords) in polygon.rings().enumerate() {
                visit_ring_coordseq::<ORDINARY_SCREEN, SUPPORT_EVENTS>(
                    frame,
                    coords,
                    row,
                    members,
                    ring,
                    run_owners,
                    pending_gate,
                    visit,
                    support_visit,
                )?;
            }
            if SUPPORT_EVENTS {
                support_visit(GeographicAdmissionEvent::EndPolygon);
            }
            Ok(())
        },
        Shape::MultiPolygon(polygons) => {
            for (logical, member) in polygons.members().enumerate() {
                if let MultiMember::Value(physical) = member {
                    if !ORDINARY_SCREEN {
                        members.push(logical);
                    }
                    for (ring, coords) in polygons.as_slice()[physical].rings().enumerate() {
                        visit_ring_coordseq::<ORDINARY_SCREEN, SUPPORT_EVENTS>(
                            frame,
                            coords,
                            row,
                            members,
                            ring,
                            run_owners,
                            pending_gate,
                            visit,
                            support_visit,
                        )?;
                    }
                    if SUPPORT_EVENTS {
                        support_visit(GeographicAdmissionEvent::EndPolygon);
                    }
                    if !ORDINARY_SCREEN {
                        members.pop();
                    }
                }
            }
            Ok(())
        },
        Shape::GeometryCollection(collection) => {
            let repeated = collection_owners.as_deref_mut().map_or_else(
                || {
                    run_owners.as_mut().is_some_and(|owners| {
                        !owners.first_collection(
                            collection.as_slice(),
                            collection.members_are_shared(),
                        )
                    })
                },
                |owners| {
                    collection.members_are_shared()
                        && !owners.observe(ShapeOwnerKey::new(collection.as_slice()))
                },
            );
            if repeated {
                // This immutable payload was fully admitted at its first
                // source occurrence; an invalid coordinate there already
                // returned its original member path.
                return Ok(());
            }
            for (member, shape) in collection.iter().enumerate() {
                if !ORDINARY_SCREEN {
                    members.push(member);
                }
                visit_shape::<ORDINARY_SCREEN, SUPPORT_EVENTS>(
                    frame,
                    shape,
                    row,
                    members,
                    collection_owners.as_deref_mut(),
                    run_owners,
                    pending_gate,
                    visit,
                    support_visit,
                )?;
                if !ORDINARY_SCREEN {
                    members.pop();
                }
            }
            Ok(())
        },
        Shape::Empty(..) => Ok(()),
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarGeographicView<'a> {
    frame: GeographicFrame,
    shape: &'a Shape,
}

impl<'a> ScalarGeographicView<'a> {
    pub(crate) fn try_new(
        frame: GeographicFrame,
        shape: &'a Shape,
    ) -> Result<Self, GeographicViewError> {
        let mut owners = SharedShapeOwners::for_shape(shape);
        let mut pending_gate = None;
        let admitted = visit_shape::<false, false>(
            frame,
            shape,
            None,
            &mut MemberPathBuilder::default(),
            None,
            &mut owners,
            &mut pending_gate,
            &mut |_| Ok(()),
            &mut |_| {},
        );
        admitted.map_err(GeographicVisitError::into_diagnostic)?;
        if let Some(error) = pending_gate {
            return Err(error);
        }
        Ok(Self { frame, shape })
    }

    pub(crate) fn try_new_with_collection_owners_and_visit(
        frame: GeographicFrame,
        shape: &'a Shape,
        owners: &mut GeographicCollectionOwners<'a>,
        mut coordinate_visit: impl FnMut(GeographicCoordinate),
        mut support_visit: impl FnMut(GeographicAdmissionEvent),
    ) -> Result<Self, GeographicAdmissionError> {
        let mut visit = |coordinate: GeographicCoordinate| {
            coordinate_visit(coordinate);
            Ok(())
        };
        let mut run_owners = SharedShapeOwners::for_shape(shape);
        let mut pending_gate = None;
        let admitted = visit_shape::<false, true>(
            frame,
            shape,
            None,
            &mut MemberPathBuilder::default(),
            Some(&mut *owners),
            &mut run_owners,
            &mut pending_gate,
            &mut visit,
            &mut support_visit,
        );
        admitted.map_err(|error| GeographicAdmissionError::Geographic(error.into_diagnostic()))?;
        if let Some(error) = owners.take_entropy_error() {
            return Err(GeographicAdmissionError::Resource(error));
        }
        if let Some(error) = pending_gate {
            return Err(GeographicAdmissionError::Geographic(error));
        }
        Ok(Self { frame, shape })
    }

    /// A view of a shape whose coordinates a prior admission validated in
    /// `frame`; the coordinates are immutable, so the admission holds.
    pub(crate) const fn previously_admitted(frame: GeographicFrame, shape: &'a Shape) -> Self {
        Self { frame, shape }
    }

    pub(crate) const fn shape(self) -> &'a Shape {
        self.shape
    }

    pub(crate) const fn frame(self) -> GeographicFrame {
        self.frame
    }

    #[cfg(test)]
    pub(crate) fn for_each_coordinate(self, mut visit: impl FnMut(GeographicCoordinate)) {
        self.shape.for_each_point(|point| {
            visit(admitted_coordinate(self.frame, XY::from(point)));
        });
    }

    /// Test a predicate over first occurrences of each shared XY run.
    ///
    /// Use after admission for idempotent queries; topology consumers keep the
    /// occurrence-based `for_each_coordinate` traversal.
    pub(crate) fn any_first_xy_coordinate(
        self,
        mut predicate: impl FnMut(GeographicCoordinate) -> bool,
    ) -> bool {
        matches!(
            self.shape.try_for_each_first_xy_point_run(|point| {
                if predicate(admitted_coordinate(self.frame, XY::from(point))) {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }),
            ControlFlow::Break(())
        )
    }
}

/// Admitted views of one geographic frame. Topology graphs consume this
/// batch; overlay maps [`GeographicViewError`] at the call that builds it.
pub(crate) struct GeographicInputs<'a> {
    frame: GeographicFrame,
    views: Vec<ScalarGeographicView<'a>>,
}

impl<'a> GeographicInputs<'a> {
    pub(crate) fn try_from_first_and_shapes(
        first: ScalarGeographicView<'a>,
        rest: impl ExactSizeIterator<Item = &'a Shape>,
    ) -> Result<Self, GeographicViewError> {
        let frame = first.frame();
        let mut inputs = Self::try_from_shapes(frame, rest)?;
        inputs.views.reserve(1);
        inputs.views.insert(0, first);
        Ok(inputs)
    }

    pub(crate) fn try_from_shapes(
        frame: GeographicFrame,
        shapes: impl ExactSizeIterator<Item = &'a Shape>,
    ) -> Result<Self, GeographicViewError> {
        let mut views = Vec::with_capacity(shapes.len());
        for shape in shapes {
            let view = ScalarGeographicView::try_new(frame, shape)?;
            views.push(view);
        }
        Ok(Self { frame, views })
    }

    pub(crate) const fn frame(&self) -> GeographicFrame {
        self.frame
    }

    pub(crate) fn views(&self) -> &[ScalarGeographicView<'a>] {
        &self.views
    }
}

fn admitted_coordinate(frame: GeographicFrame, native_xy: XY) -> GeographicCoordinate {
    GeographicCoordinate {
        native_xy,
        longitude: frame.longitude().wrap_admitted(native_xy.x),
        latitude: Latitude::from_admitted(frame, native_xy.y),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingLatitudeSpan {
    Ordinary,
    PoleToPole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RingDomain {
    Ordinary,
    FullLongitude(RingLatitudeSpan),
    SeamOnly,
}

#[derive(Clone, Copy)]
struct LatitudeSpan {
    south: Latitude,
    north: Latitude,
}

impl LatitudeSpan {
    const fn new(latitude: Latitude) -> Self {
        Self {
            south: latitude,
            north: latitude,
        }
    }

    fn include(&mut self, latitude: Latitude) {
        if latitude.get() < self.south.get() {
            self.south = latitude;
        }
        if latitude.get() > self.north.get() {
            self.north = latitude;
        }
    }
}

#[derive(Clone, Copy)]
struct ParallelTurn {
    latitude: Latitude,
    bits: u64,
    turns: i64,
}

impl ParallelTurn {
    const fn matches(self, latitude: Latitude) -> bool {
        let bits = latitude.get().to_bits();
        self.bits == bits && self.latitude.get().to_bits() == bits
    }
}

#[derive(Clone, Copy, Default)]
enum ParallelTurns {
    #[default]
    Empty,
    One(ParallelTurn),
    Two(ParallelTurn, ParallelTurn),
    Invalid,
}

#[derive(Clone, Copy)]
struct ExactAxisBand {
    parallels: ParallelTurns,
    meridian: Option<u64>,
    verticals_valid: bool,
}

impl Default for ExactAxisBand {
    fn default() -> Self {
        Self {
            parallels: ParallelTurns::Empty,
            meridian: None,
            verticals_valid: true,
        }
    }
}

impl ExactAxisBand {
    fn include_edge(
        &mut self,
        frame: GeographicFrame,
        connection: ChartConnection,
        start: XY,
        end: XY,
    ) {
        let axis = frame.longitude();
        if start.y.to_bits() == end.y.to_bits() {
            if connection.step.delta(axis) == 0.0 {
                return;
            }
            self.include_parallel(
                Latitude::from_admitted(frame, start.y),
                start.y.to_bits(),
                i64::from(connection.source_lift),
            );
        } else {
            if connection.step.delta(axis) != 0.0 || connection.source_lift != 0 {
                self.verticals_valid = false;
                return;
            }
            let longitude = connection.start.get().to_bits();
            if self.meridian.is_some_and(|previous| previous != longitude) {
                self.verticals_valid = false;
                return;
            }
            self.meridian = Some(longitude);
        }
    }

    fn include_parallel(&mut self, latitude: Latitude, bits: u64, transit: i64) {
        let add = |level: &mut ParallelTurn| {
            if let Some(turns) = level.turns.checked_add(transit) {
                level.turns = turns;
                true
            } else {
                false
            }
        };
        self.parallels = match self.parallels {
            ParallelTurns::Empty => ParallelTurns::One(ParallelTurn {
                latitude,
                bits,
                turns: transit,
            }),
            ParallelTurns::One(mut first) if first.bits == bits => {
                if add(&mut first) {
                    ParallelTurns::One(first)
                } else {
                    ParallelTurns::Invalid
                }
            },
            ParallelTurns::One(first) => ParallelTurns::Two(first, ParallelTurn {
                latitude,
                bits,
                turns: transit,
            }),
            ParallelTurns::Two(mut first, second) if first.bits == bits => {
                if add(&mut first) {
                    ParallelTurns::Two(first, second)
                } else {
                    ParallelTurns::Invalid
                }
            },
            ParallelTurns::Two(first, mut second) if second.bits == bits => {
                if add(&mut second) {
                    ParallelTurns::Two(first, second)
                } else {
                    ParallelTurns::Invalid
                }
            },
            ParallelTurns::Two(..) | ParallelTurns::Invalid => ParallelTurns::Invalid,
        };
    }

    fn is_exact(
        self,
        frame: GeographicFrame,
        west: f64,
        east: f64,
        south: Latitude,
        north: Latitude,
    ) -> bool {
        let axis = frame.longitude();
        let (south_turns, north_turns) = match self.parallels {
            ParallelTurns::Two(first, second) if first.matches(south) && second.matches(north) => {
                (first.turns, second.turns)
            },
            ParallelTurns::Two(first, second) if second.matches(south) && first.matches(north) => {
                (second.turns, first.turns)
            },
            ParallelTurns::Empty
            | ParallelTurns::One(_)
            | ParallelTurns::Two(..)
            | ParallelTurns::Invalid => return false,
        };
        // Each parallel must close one directed circle on the same quotient
        // meridian; alternate seam spellings of one meridian remain identical.
        east - west > axis.half_period()
            && south.get().to_bits() != north.get().to_bits()
            && self.verticals_valid
            && matches!((south_turns, north_turns), (1, -1) | (-1, 1))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GeographicRing<'a> {
    coords: &'a CoordSeq,
    window: CheckedRingWindow,
}

impl<'a> GeographicRing<'a> {
    pub(crate) fn from_ring(ring: &'a Ring) -> Self {
        Self {
            coords: ring.coords(),
            window: CheckedRingWindow::from_ring(ring),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.window.range().len()
    }

    pub(crate) fn native_xs(&self) -> &'a [f64] {
        &self.coords.xs()[self.window.range().clone()]
    }

    fn x(&self, index: usize) -> f64 {
        self.native_xs()[index]
    }

    fn y(&self, index: usize) -> f64 {
        self.coords.ys()[self.window.range().start + index]
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RingSummary {
    /// Signed total of the shared exact chart-edge transits.
    pub(crate) chart_turn: i64,
    pub(crate) domain: RingDomain,
    /// True only when each axis-aligned parallel has an intermediate
    /// longitude or an admitted full-turn edge. An edge between the two seam
    /// spellings has zero width.
    pub(crate) exact_axis_band: bool,
    pub(crate) south: Latitude,
    pub(crate) north: Latitude,
}

impl RingSummary {
    pub(crate) fn try_new(
        frame: GeographicFrame,
        ring: &GeographicRing<'_>,
    ) -> Result<Self, GeographicViewError> {
        let axis = frame.longitude();
        let mut chart_turn = 0_i64;
        let mut seam_only_vertices = true;
        let mut has_seam_vertex = false;
        let fail = |index: usize, source: GeographicError| {
            GeographicDiagnostic::boxed(
                GeographicLocation {
                    coordinate: index,
                    ..GeographicLocation::default()
                },
                XY::new(ring.x(index), ring.y(index)),
                source,
            )
        };
        let mut latitude_span = LatitudeSpan::new(
            Latitude::try_new(frame, ring.y(0)).map_err(|source| fail(0, source))?,
        );
        let mut west = ring.x(0);
        let mut east = west;
        let mut previous: Option<XY> = None;
        let mut exact_axis_band = ExactAxisBand::default();
        for index in 0..ring.len() {
            let x = ring.x(index);
            let longitude = frame
                .longitude()
                .canonicalize(x)
                .map_err(|source| fail(index, source))?;
            let latitude =
                Latitude::try_new(frame, ring.y(index)).map_err(|source| fail(index, source))?;
            latitude_span.include(latitude);
            let identity = if latitude.identity(frame) == SeamIdentity::Pole {
                SeamIdentity::Pole
            } else {
                longitude.identity(axis)
            };
            match identity {
                SeamIdentity::Seam => has_seam_vertex = true,
                SeamIdentity::Pole => {},
                SeamIdentity::Ordinary => seam_only_vertices = false,
            }
            west = west.min(x);
            east = east.max(x);
            if let Some(previous) = previous {
                let current = XY::new(x, ring.y(index));
                let connection = axis
                    .chart_connection(previous.x, current.x)
                    .map_err(|source| fail(index, source))?;
                exact_axis_band.include_edge(frame, connection, previous, current);
                chart_turn = chart_turn
                    .checked_add(i64::from(connection.source_lift))
                    .ok_or_else(|| fail(index, GeographicError::ChartTurnOverflow))?;
            }
            previous = Some(XY::new(x, ring.y(index)));
        }
        let south = latitude_span.south;
        let north = latitude_span.north;
        let exact_axis_band = exact_axis_band.is_exact(frame, west, east, south, north);
        Ok(Self::from_admitted_facts(
            frame,
            chart_turn,
            seam_only_vertices,
            has_seam_vertex,
            exact_axis_band,
            south,
            north,
        ))
    }

    pub(crate) fn from_admitted_facts(
        frame: GeographicFrame,
        chart_turn: i64,
        seam_only_vertices: bool,
        has_seam_vertex: bool,
        exact_axis_band: bool,
        south: Latitude,
        north: Latitude,
    ) -> Self {
        let full_longitude = chart_turn != 0 || exact_axis_band;
        let seam_only = seam_only_vertices && has_seam_vertex && !full_longitude;
        let domain = if full_longitude {
            #[expect(
                clippy::float_cmp,
                reason = "world-band identity requires the exact represented poles"
            )]
            let pole_to_pole = south.get() == -frame.pole() && north.get() == frame.pole();
            let span = if pole_to_pole {
                RingLatitudeSpan::PoleToPole
            } else {
                RingLatitudeSpan::Ordinary
            };
            RingDomain::FullLongitude(span)
        } else if seam_only {
            RingDomain::SeamOnly
        } else {
            RingDomain::Ordinary
        };
        Self {
            chart_turn,
            domain,
            exact_axis_band,
            south,
            north,
        }
    }

    pub(crate) const fn turn(&self) -> RingTurn {
        RingTurn::from_chart_turn(self.chart_turn)
    }

    /// Selected native-chart hemisphere for a nonzero source turn.
    pub(crate) fn full_turn_pole(&self) -> Option<FullTurnPole> {
        (self.chart_turn != 0)
            .then(|| FullTurnPole::of_latitude_span(self.south.get(), self.north.get()))
    }

    /// Native-chart pole bounded by a nonzero source turn, if unambiguous.
    pub(crate) fn bounded_pole(&self) -> Option<ChartPole> {
        match self.full_turn_pole() {
            Some(FullTurnPole::Bounded(pole)) => Some(pole),
            Some(FullTurnPole::Ambiguous) | None => None,
        }
    }
}
