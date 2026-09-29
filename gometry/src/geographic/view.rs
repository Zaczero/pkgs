use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;

use super::{
    CanonicalLongitude, GeographicError, GeographicFrame, Latitude, RingTurn, SeamIdentity,
};
use crate::array::CheckedRingWindow;
use crate::collections::HashSet;
use crate::geometry::{CoordSeq, MultiMember, Point, Ring, Shape, ShapeOwnerKey, XY};

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

#[derive(Debug)]
pub(crate) enum GeographicAdmissionError {
    Geographic(GeographicViewError),
    Resource(crate::error::Error),
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
    native: Point,
    longitude: CanonicalLongitude,
    latitude: Latitude,
}

impl GeographicCoordinate {
    pub(crate) const fn native(self) -> Point {
        self.native
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
    point: Point,
    location: impl FnOnce() -> GeographicLocation,
) -> Result<GeographicCoordinate, GeographicViewError> {
    let longitude = match frame.longitude().canonicalize(point.x) {
        Ok(longitude) => longitude,
        Err(source) => {
            return Err(GeographicDiagnostic::boxed(
                location(),
                XY::from(point),
                source,
            ));
        },
    };
    let latitude = match Latitude::try_new(frame, point.y) {
        Ok(latitude) => latitude,
        Err(source) => {
            return Err(GeographicDiagnostic::boxed(
                location(),
                XY::from(point),
                source,
            ));
        },
    };
    Ok(GeographicCoordinate {
        native: point,
        longitude,
        latitude,
    })
}

fn visit_coordseq(
    frame: GeographicFrame,
    coords: &CoordSeq,
    row: Option<usize>,
    members: &MemberPathBuilder,
    ring: Option<usize>,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicViewError>,
) -> Result<(), GeographicViewError> {
    for coordinate_index in 0..coords.len() {
        visit(coordinate(
            frame,
            coords.point_at(coordinate_index),
            || GeographicLocation {
                row,
                members: members.location(),
                ring,
                coordinate: coordinate_index,
            },
        )?)?;
    }
    Ok(())
}

fn visit_shape<'owner>(
    frame: GeographicFrame,
    shape: &'owner Shape,
    row: Option<usize>,
    members: &mut MemberPathBuilder,
    mut collection_owners: Option<&mut GeographicCollectionOwners<'owner>>,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicViewError>,
) -> Result<(), GeographicViewError> {
    match shape {
        Shape::Point(point) => visit(coordinate(frame, *point, || GeographicLocation {
            row,
            members: members.location(),
            ring: None,
            coordinate: 0,
        })?),
        Shape::MultiPoint(points) => {
            for (logical, member) in points.members().enumerate() {
                if let MultiMember::Value(physical) = member {
                    members.push(logical);
                    visit(coordinate(frame, points.point_at(physical), || {
                        GeographicLocation {
                            row,
                            members: members.location(),
                            ring: None,
                            coordinate: 0,
                        }
                    })?)?;
                    members.pop();
                }
            }
            Ok(())
        },
        Shape::LineString(line) => visit_coordseq(frame, line, row, members, None, visit),
        Shape::MultiLineString(lines) => {
            for (logical, member) in lines.members().enumerate() {
                if let MultiMember::Value(physical) = member {
                    members.push(logical);
                    visit_coordseq(
                        frame,
                        &lines.as_slice()[physical],
                        row,
                        members,
                        None,
                        visit,
                    )?;
                    members.pop();
                }
            }
            Ok(())
        },
        Shape::Polygon(polygon) => {
            for (ring, coords) in polygon.rings().enumerate() {
                visit_coordseq(frame, coords, row, members, Some(ring), visit)?;
            }
            Ok(())
        },
        Shape::MultiPolygon(polygons) => {
            for (logical, member) in polygons.members().enumerate() {
                if let MultiMember::Value(physical) = member {
                    members.push(logical);
                    for (ring, coords) in polygons.as_slice()[physical].rings().enumerate() {
                        visit_coordseq(frame, coords, row, members, Some(ring), visit)?;
                    }
                    members.pop();
                }
            }
            Ok(())
        },
        Shape::GeometryCollection(collection) => {
            #[cfg(test)]
            if let Some(owners) = collection_owners.as_deref_mut() {
                owners.record_collection_occurrence();
            }
            let repeated = collection_owners.as_deref_mut().is_some_and(|owners| {
                collection.members_are_shared()
                    && !owners.observe(ShapeOwnerKey::new(collection.as_slice()))
            });
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
                members.push(member);
                visit_shape(
                    frame,
                    shape,
                    row,
                    members,
                    collection_owners.as_deref_mut(),
                    visit,
                )?;
                members.pop();
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
        visit_shape(
            frame,
            shape,
            None,
            &mut MemberPathBuilder::default(),
            None,
            &mut |_| Ok(()),
        )?;
        Ok(Self { frame, shape })
    }

    pub(crate) fn try_new_with_collection_owners(
        frame: GeographicFrame,
        shape: &'a Shape,
        owners: &mut GeographicCollectionOwners<'a>,
    ) -> Result<Self, GeographicAdmissionError> {
        #[cfg(test)]
        let coordinate_visits = std::cell::Cell::new(0_usize);
        let mut visit = |_: GeographicCoordinate| {
            #[cfg(test)]
            coordinate_visits.set(coordinate_visits.get().wrapping_add(1));
            Ok(())
        };
        let admitted = visit_shape(
            frame,
            shape,
            None,
            &mut MemberPathBuilder::default(),
            Some(&mut *owners),
            &mut visit,
        );
        #[cfg(test)]
        owners.record_coordinate_visits(coordinate_visits.get());
        admitted.map_err(GeographicAdmissionError::Geographic)?;
        if let Some(error) = owners.take_entropy_error() {
            return Err(GeographicAdmissionError::Resource(error));
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
            visit(admitted_coordinate(self.frame, point));
        });
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

    /// Copy indexed views from this batch. Frame comes from `self`; indices
    /// are this batch's positions. Does not re-admit.
    pub(crate) fn selected(&self, indices: &[usize]) -> Self {
        let mut views = Vec::with_capacity(indices.len());
        for &index in indices {
            views.push(self.views[index]);
        }
        Self {
            frame: self.frame,
            views,
        }
    }

    pub(crate) const fn frame(&self) -> GeographicFrame {
        self.frame
    }

    pub(crate) fn views(&self) -> &[ScalarGeographicView<'a>] {
        &self.views
    }
}

fn admitted_coordinate(frame: GeographicFrame, point: Point) -> GeographicCoordinate {
    GeographicCoordinate {
        native: point,
        longitude: frame.longitude().wrap_admitted(point.x),
        latitude: Latitude::from_admitted(frame, point.y),
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

    pub(crate) fn native_point(&self, index: usize) -> Point {
        self.coords.point_at(self.window.range().start + index)
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
    pub(crate) turn: RingTurn,
    pub(crate) domain: RingDomain,
    pub(crate) south: Latitude,
    pub(crate) north: Latitude,
}

impl RingSummary {
    pub(crate) fn try_new(
        frame: GeographicFrame,
        ring: &GeographicRing<'_>,
    ) -> Result<Self, GeographicViewError> {
        let axis = frame.longitude();
        let mut seam_transits = 0_i64;
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
        let mut south = Latitude::try_new(frame, ring.y(0)).map_err(|source| fail(0, source))?;
        let mut north = south;
        let mut west = ring.x(0);
        let mut east = west;
        for index in 0..ring.len() {
            frame
                .longitude()
                .canonicalize(ring.x(index))
                .map_err(|source| fail(index, source))?;
            let latitude =
                Latitude::try_new(frame, ring.y(index)).map_err(|source| fail(index, source))?;
            if latitude.get() < south.get() {
                south = latitude;
            }
            if latitude.get() > north.get() {
                north = latitude;
            }
            west = west.min(ring.x(index));
            east = east.max(ring.x(index));
            if index + 1 < ring.len() {
                let transit = axis
                    .seam_transit(ring.x(index), ring.x(index + 1))
                    .map_err(|source| fail(index + 1, source))?;
                seam_transits = seam_transits
                    .checked_add(i64::from(transit))
                    .ok_or_else(|| fail(index + 1, GeographicError::SeamTransitOverflow))?;
            }
        }
        let turn = match seam_transits {
            0 => RingTurn::None,
            1 => RingTurn::Full(super::TurnDirection::Positive),
            -1 => RingTurn::Full(super::TurnDirection::Negative),
            _ => RingTurn::Other,
        };
        let full_longitude = matches!(turn, RingTurn::Full(_))
            || is_exact_axis_band(frame, ring, west, east, south, north);
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
        } else {
            RingDomain::Ordinary
        };
        Ok(Self {
            turn,
            domain,
            south,
            north,
        })
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
    if !exact_difference_equals(east, west, frame.longitude().period())
        || south.get().to_bits() == north.get().to_bits()
    {
        return false;
    }
    let mut corners = 0_u8;
    let axis_corners = (0..ring.len()).all(|index| {
        let x = ring.x(index);
        let y = ring.y(index);
        let x_side = if x.to_bits() == west.to_bits() {
            0
        } else if x.to_bits() == east.to_bits() {
            1
        } else {
            return false;
        };
        let y_side = if y.to_bits() == south.get().to_bits() {
            0
        } else if y.to_bits() == north.get().to_bits() {
            1
        } else {
            return false;
        };
        corners |= 1 << (x_side * 2 + y_side);
        true
    });
    let axis_edges = (0..ring.len() - 1).all(|index| {
        ring.x(index).to_bits() == ring.x(index + 1).to_bits()
            || ring.y(index).to_bits() == ring.y(index + 1).to_bits()
    });
    axis_corners && axis_edges && corners == 0b1111
}

fn exact_difference_equals(left: f64, right: f64, target: f64) -> bool {
    let difference = left - right;
    if !difference.is_finite() {
        return false;
    }
    let right_virtual = left - difference;
    let error = (left - (difference + right_virtual)) + (right_virtual - right);
    #[expect(
        clippy::float_cmp,
        reason = "axis-band recognition is exact represented arithmetic with no tolerance"
    )]
    {
        difference == target && error == 0.0
    }
}
