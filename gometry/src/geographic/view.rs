use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::ops::ControlFlow;

use super::{
    CanonicalLongitude, ChartPole, FullTurnPole, GeographicError, GeographicFrame, Latitude,
    RingTurn, SeamIdentity,
};
use crate::collections::HashSet;
use crate::geometry::storage::admission::CheckedRingWindow;
use crate::geometry::{
    CoordSeq, MultiMember, Ring, RunIdentity, Shape, ShapeOwnerKey, SharedShapeOwners,
    SupportRunKind, XY,
};

const INLINE_MEMBER_DEPTH: usize = 4;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GeographicAdmissionWork {
    pub(crate) collection_occurrences: usize,
    pub(crate) collection_descents: usize,
    pub(crate) collection_member_slots: usize,
    pub(crate) coordinate_visits: usize,
    pub(crate) repeated_owner_occurrences: usize,
    pub(crate) owner_set_initializations: usize,
}

/// Tracks immutable collection payload identities across admission and census.
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
    #[cfg(test)]
    work: GeographicAdmissionWork,
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
                #[cfg(test)]
                {
                    self.work.owner_set_initializations =
                        self.work.owner_set_initializations.wrapping_add(1);
                }
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

    pub(crate) fn reset_for_census(&mut self) {
        self.inline = None;
        if let Some(owners) = self.hashed.as_mut() {
            owners.clear();
        }
        if let Some(owners) = self.entropy_fallback.as_mut() {
            owners.clear();
        }
    }

    pub(crate) fn discard(&mut self) {
        self.inline = None;
        self.hashed = None;
        self.entropy_fallback = None;
    }

    pub(crate) const fn take_entropy_error(&mut self) -> Option<crate::error::Error> {
        self.entropy_error.take()
    }

    #[cfg(test)]
    pub(crate) const fn work(&self) -> GeographicAdmissionWork {
        self.work
    }

    #[cfg(test)]
    const fn record_collection_occurrence(&mut self) {
        self.work.collection_occurrences = self.work.collection_occurrences.wrapping_add(1);
    }

    #[cfg(test)]
    const fn record_collection_descent(&mut self) {
        self.work.collection_descents = self.work.collection_descents.wrapping_add(1);
    }

    #[cfg(test)]
    const fn record_collection_member_slot(&mut self) {
        self.work.collection_member_slots = self.work.collection_member_slots.wrapping_add(1);
    }

    #[cfg(test)]
    const fn record_coordinate_visits(&mut self, count: usize) {
        self.work.coordinate_visits = self.work.coordinate_visits.wrapping_add(count);
    }

    #[cfg(test)]
    const fn record_repeated_owner_occurrence(&mut self) {
        self.work.repeated_owner_occurrences = self.work.repeated_owner_occurrences.wrapping_add(1);
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

fn visit_coordseq<const ORDINARY_SCREEN: bool>(
    frame: GeographicFrame,
    coords: &CoordSeq,
    row: Option<usize>,
    members: &MemberPathBuilder,
    ring: Option<usize>,
    run_owners: &mut Option<SharedShapeOwners>,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicVisitError>,
) -> Result<(), GeographicVisitError> {
    let first = run_owners.as_mut().is_none_or(|owners| {
        if ORDINARY_SCREEN {
            owners.first_support_run(coords, RunIdentity::XY, SupportRunKind::SegmentChain)
        } else {
            owners.first_coord_seq(coords, RunIdentity::XY)
        }
    });
    if !first {
        return Ok(());
    }
    let axis = frame.longitude();
    let mut previous_x = None;
    for (coordinate_index, (&x, &y)) in coords.xs().iter().zip(coords.ys()).enumerate() {
        let native_xy = XY::new(x, y);
        if ORDINARY_SCREEN
            && let Some(previous_x) = previous_x
            && !axis.ordinary_chart_edge(previous_x, x)
        {
            return Err(GeographicVisitError::OrdinaryScreenMismatch);
        }
        visit(coordinate_for_visit::<ORDINARY_SCREEN>(
            frame,
            native_xy,
            || GeographicLocation {
                row,
                members: members.location(),
                ring,
                coordinate: coordinate_index,
            },
        )?)?;
        previous_x = Some(x);
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

fn visit_ring_coordseq<const ORDINARY_SCREEN: bool>(
    frame: GeographicFrame,
    coords: &CoordSeq,
    row: Option<usize>,
    members: &MemberPathBuilder,
    ring: usize,
    run_owners: &mut Option<SharedShapeOwners>,
    pending_gate: &mut Option<GeographicViewError>,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicVisitError>,
) -> Result<(), GeographicVisitError> {
    let repeated = run_owners.as_mut().is_some_and(|owners| {
        if ORDINARY_SCREEN {
            !owners.first_support_run(coords, RunIdentity::XY, SupportRunKind::SegmentChain)
        } else {
            !owners.first_coord_seq(coords, RunIdentity::XY)
        }
    });
    if ORDINARY_SCREEN && repeated {
        return Ok(());
    }
    let mut chart_turn = 0_i64;
    let mut previous: Option<XY> = None;
    let mut first = None;
    let axis = frame.longitude();
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
        if !ORDINARY_SCREEN
            && let Some(previous) = previous
            && !axis.ordinary_chart_edge(previous.x, native_xy.x)
        {
            chart_turn = axis
                .add_chart_transit(chart_turn, previous.x, native_xy.x)
                .map_err(|source| {
                    GeographicDiagnostic::boxed(
                        GeographicLocation {
                            row,
                            members: members.location(),
                            ring: Some(ring),
                            coordinate: index,
                        },
                        native_xy,
                        source,
                    )
                })?;
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
        if pending_gate.is_none() && matches!(chart_turn, -1 | 1) {
            let span = admitted_latitude_span(frame, coords);
            if let Err(error) = ring_gate(
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
            ) {
                *pending_gate = Some(error);
            }
        }
    }
    Ok(())
}

fn visit_shape<'owner, const ORDINARY_SCREEN: bool>(
    frame: GeographicFrame,
    shape: &'owner Shape,
    row: Option<usize>,
    members: &mut MemberPathBuilder,
    mut collection_owners: Option<&mut GeographicCollectionOwners<'owner>>,
    run_owners: &mut Option<SharedShapeOwners>,
    pending_gate: &mut Option<GeographicViewError>,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicVisitError>,
) -> Result<(), GeographicVisitError> {
    match shape {
        Shape::Point(point) => visit(coordinate_for_visit::<ORDINARY_SCREEN>(
            frame,
            XY::from(*point),
            || GeographicLocation {
                row,
                members: members.location(),
                ring: None,
                coordinate: 0,
            },
        )?),
        Shape::MultiPoint(points) => {
            if run_owners
                .as_mut()
                .is_some_and(|owners| !owners.first_coord_seq(points, RunIdentity::XY))
            {
                return Ok(());
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
                    visit(coordinate_for_visit::<ORDINARY_SCREEN>(
                        frame,
                        native_xy,
                        || GeographicLocation {
                            row,
                            members: members.location(),
                            ring: None,
                            coordinate: 0,
                        },
                    )?)?;
                    if !ORDINARY_SCREEN {
                        members.pop();
                    }
                }
            }
            Ok(())
        },
        Shape::LineString(line) => {
            visit_coordseq::<ORDINARY_SCREEN>(frame, line, row, members, None, run_owners, visit)
        },
        Shape::MultiLineString(lines) => {
            for (logical, member) in lines.members().enumerate() {
                if let MultiMember::Value(physical) = member {
                    if !ORDINARY_SCREEN {
                        members.push(logical);
                    }
                    visit_coordseq::<ORDINARY_SCREEN>(
                        frame,
                        &lines.as_slice()[physical],
                        row,
                        members,
                        None,
                        run_owners,
                        visit,
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
                visit_ring_coordseq::<ORDINARY_SCREEN>(
                    frame,
                    coords,
                    row,
                    members,
                    ring,
                    run_owners,
                    pending_gate,
                    visit,
                )?;
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
                        visit_ring_coordseq::<ORDINARY_SCREEN>(
                            frame,
                            coords,
                            row,
                            members,
                            ring,
                            run_owners,
                            pending_gate,
                            visit,
                        )?;
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
                    #[cfg(test)]
                    owners.record_collection_occurrence();
                    collection.members_are_shared()
                        && !owners.observe(ShapeOwnerKey::new(collection.as_slice()))
                },
            );
            if repeated {
                // This immutable payload was fully admitted at its first
                // source occurrence; an invalid coordinate there already
                // returned its original member path.
                #[cfg(test)]
                if let Some(owners) = collection_owners.as_deref_mut() {
                    owners.record_repeated_owner_occurrence();
                }
                return Ok(());
            }
            #[cfg(test)]
            if let Some(owners) = collection_owners.as_deref_mut() {
                owners.record_collection_descent();
            }
            for (member, shape) in collection.iter().enumerate() {
                #[cfg(test)]
                if let Some(owners) = collection_owners.as_deref_mut() {
                    owners.record_collection_member_slot();
                }
                if !ORDINARY_SCREEN {
                    members.push(member);
                }
                visit_shape::<ORDINARY_SCREEN>(
                    frame,
                    shape,
                    row,
                    members,
                    collection_owners.as_deref_mut(),
                    run_owners,
                    pending_gate,
                    visit,
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
        let admitted = visit_shape::<false>(
            frame,
            shape,
            None,
            &mut MemberPathBuilder::default(),
            None,
            &mut owners,
            &mut pending_gate,
            &mut |_| Ok(()),
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
    ) -> Result<Self, GeographicAdmissionError> {
        #[cfg(test)]
        let coordinate_visits = std::cell::Cell::new(0_usize);
        let mut visit = |coordinate: GeographicCoordinate| {
            #[cfg(test)]
            coordinate_visits.set(coordinate_visits.get().wrapping_add(1));
            coordinate_visit(coordinate);
            Ok(())
        };
        let mut run_owners = SharedShapeOwners::for_shape(shape);
        let mut pending_gate = None;
        let admitted = visit_shape::<false>(
            frame,
            shape,
            None,
            &mut MemberPathBuilder::default(),
            Some(&mut *owners),
            &mut run_owners,
            &mut pending_gate,
            &mut visit,
        );
        #[cfg(test)]
        owners.record_coordinate_visits(coordinate_visits.get());
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

fn admitted_latitude_span(frame: GeographicFrame, coords: &CoordSeq) -> LatitudeSpan {
    let mut latitudes = coords.ys().iter();
    let first = latitudes
        .next()
        .expect("admitted polygon ring has at least four coordinates");
    let mut span = LatitudeSpan::new(Latitude::from_admitted(frame, *first));
    for &latitude in latitudes {
        span.include(Latitude::from_admitted(frame, latitude));
    }
    span
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
        let mut previous_x = None;
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
            if let Some(previous_x) = previous_x
                && !axis.ordinary_chart_edge(previous_x, x)
            {
                chart_turn = axis
                    .add_chart_transit(chart_turn, previous_x, x)
                    .map_err(|source| fail(index, source))?;
            }
            previous_x = Some(x);
        }
        let south = latitude_span.south;
        let north = latitude_span.north;
        let exact_axis_band = is_exact_axis_band(frame, ring, west, east, south, north);
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
        Ok(Self {
            chart_turn,
            domain,
            exact_axis_band,
            south,
            north,
        })
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

fn is_exact_axis_band(
    frame: GeographicFrame,
    ring: &GeographicRing<'_>,
    west: f64,
    east: f64,
    south: Latitude,
    north: Latitude,
) -> bool {
    let axis = frame.longitude();
    if east - west <= axis.half_period() || south.get().to_bits() == north.get().to_bits() {
        return false;
    }
    let (mut south_turns, mut north_turns) = (0_i64, 0_i64);
    let mut meridian = None;
    for index in 0..ring.len().saturating_sub(1) {
        let start_x = ring.x(index);
        let end_x = ring.x(index + 1);
        let start_y = ring.y(index);
        let end_y = ring.y(index + 1);
        let Ok(step) = axis.chart_step(start_x, end_x) else {
            return false;
        };
        if start_y.to_bits() == end_y.to_bits() {
            let parallel = if start_y.to_bits() == south.get().to_bits() {
                &mut south_turns
            } else if start_y.to_bits() == north.get().to_bits() {
                &mut north_turns
            } else if step.delta(axis) == 0.0 {
                continue;
            } else {
                return false;
            };
            let Ok(transit) = step.transit(axis, start_x, end_x) else {
                return false;
            };
            let Some(turns) = parallel.checked_add(i64::from(transit)) else {
                return false;
            };
            *parallel = turns;
        } else {
            if step.delta(axis) != 0.0 {
                return false;
            }
            let Ok(longitude) = axis.canonicalize(start_x) else {
                return false;
            };
            let longitude = longitude.get().to_bits();
            if meridian.is_some_and(|previous| previous != longitude) {
                return false;
            }
            meridian = Some(longitude);
        }
    }
    // Each parallel closes one directed circle on the same quotient meridian.
    // Canonical seam spelling need not retain a raw one-period bounding span.
    matches!((south_turns, north_turns), (1, -1) | (-1, 1))
}
