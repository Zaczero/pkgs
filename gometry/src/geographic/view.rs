use std::fmt;

use super::{
    CanonicalLongitude, GeographicError, GeographicFrame, Latitude, RingTurn, SeamIdentity,
};
use crate::array::CheckedRingWindow;
use crate::geometry::{CoordSeq, MultiMember, Point, Ring, Shape, XY};

const INLINE_MEMBER_DEPTH: usize = 4;

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
    fn push(&mut self, member: usize) -> Result<(), GeographicViewError> {
        if self.len < INLINE_MEMBER_DEPTH {
            self.inline[self.len] = member;
        } else {
            if self.overflow.is_empty() {
                self.overflow
                    .try_reserve(INLINE_MEMBER_DEPTH + 1)
                    .map_err(|_| GeographicViewError::Resource)?;
                self.overflow.extend_from_slice(&self.inline);
            } else {
                self.overflow
                    .try_reserve(1)
                    .map_err(|_| GeographicViewError::Resource)?;
            }
            self.overflow.push(member);
        }
        self.len += 1;
        Ok(())
    }

    fn pop(&mut self) {
        self.len -= 1;
        if self.len >= INLINE_MEMBER_DEPTH {
            self.overflow.pop();
        } else if self.len + 1 == INLINE_MEMBER_DEPTH {
            self.overflow.clear();
        }
    }

    fn location(&self) -> Result<GeographicMemberPath, GeographicViewError> {
        let mut overflow = Vec::new();
        if !self.overflow.is_empty() {
            overflow
                .try_reserve_exact(self.overflow.len())
                .map_err(|_| GeographicViewError::Resource)?;
            overflow.extend_from_slice(&self.overflow);
        }
        Ok(GeographicMemberPath {
            inline: self.inline,
            len: self.len,
            overflow,
        })
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GeographicViewError {
    Diagnostic(Box<GeographicDiagnostic>),
    Resource,
}

impl GeographicViewError {
    fn diagnostic(location: GeographicLocation, native: XY, source: GeographicError) -> Self {
        Box::try_new(GeographicDiagnostic {
            location,
            native,
            source,
        })
        .map_or(Self::Resource, Self::Diagnostic)
    }
}

impl fmt::Display for GeographicViewError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Diagnostic(diagnostic) => diagnostic.fmt(f),
            Self::Resource => f.write_str("memory allocation failed"),
        }
    }
}

impl std::error::Error for GeographicViewError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Diagnostic(diagnostic) => Some(&diagnostic.source),
            Self::Resource => None,
        }
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
    location: impl FnOnce() -> Result<GeographicLocation, GeographicViewError>,
) -> Result<GeographicCoordinate, GeographicViewError> {
    let longitude = match frame.longitude().canonicalize(point.x) {
        Ok(longitude) => longitude,
        Err(source) => {
            return Err(GeographicViewError::diagnostic(
                location()?,
                XY::from(point),
                source,
            ));
        },
    };
    let latitude = match Latitude::try_new(frame, point.y) {
        Ok(latitude) => latitude,
        Err(source) => {
            return Err(GeographicViewError::diagnostic(
                location()?,
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
            || {
                Ok(GeographicLocation {
                    row,
                    members: members.location()?,
                    ring,
                    coordinate: coordinate_index,
                })
            },
        )?)?;
    }
    Ok(())
}

fn visit_shape(
    frame: GeographicFrame,
    shape: &Shape,
    row: Option<usize>,
    members: &mut MemberPathBuilder,
    visit: &mut impl FnMut(GeographicCoordinate) -> Result<(), GeographicViewError>,
) -> Result<(), GeographicViewError> {
    match shape {
        Shape::Point(point) => visit(coordinate(frame, *point, || {
            Ok(GeographicLocation {
                row,
                members: members.location()?,
                ring: None,
                coordinate: 0,
            })
        })?),
        Shape::MultiPoint(points) => {
            for (logical, member) in points.members().enumerate() {
                if let MultiMember::Value(physical) = member {
                    members.push(logical)?;
                    visit(coordinate(frame, points.point_at(physical), || {
                        Ok(GeographicLocation {
                            row,
                            members: members.location()?,
                            ring: None,
                            coordinate: 0,
                        })
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
                    members.push(logical)?;
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
                    members.push(logical)?;
                    for (ring, coords) in polygons.as_slice()[physical].rings().enumerate() {
                        visit_coordseq(frame, coords, row, members, Some(ring), visit)?;
                    }
                    members.pop();
                }
            }
            Ok(())
        },
        Shape::GeometryCollection(collection) => {
            for (member, shape) in collection.iter().enumerate() {
                members.push(member)?;
                visit_shape(frame, shape, row, members, visit)?;
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
            &mut |_| Ok(()),
        )?;
        Ok(Self { frame, shape })
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
        inputs
            .views
            .try_reserve(1)
            .map_err(|_| GeographicViewError::Resource)?;
        inputs.views.insert(0, first);
        Ok(inputs)
    }

    pub(crate) fn try_from_shapes(
        frame: GeographicFrame,
        shapes: impl ExactSizeIterator<Item = &'a Shape>,
    ) -> Result<Self, GeographicViewError> {
        let mut views = Vec::new();
        let _ = views.try_reserve_exact(shapes.len());
        for shape in shapes {
            let view = ScalarGeographicView::try_new(frame, shape)?;
            crate::try_push_vec(&mut views, view).map_err(|_| GeographicViewError::Resource)?;
        }
        Ok(Self { frame, views })
    }

    /// Copy indexed views from this batch. Frame comes from `self`; indices
    /// are this batch's positions. Does not re-admit.
    pub(crate) fn try_selected(&self, indices: &[usize]) -> Result<Self, GeographicViewError> {
        let mut views = Vec::new();
        views
            .try_reserve_exact(indices.len())
            .map_err(|_| GeographicViewError::Resource)?;
        for &index in indices {
            crate::try_push_vec(&mut views, self.views[index])
                .map_err(|_| GeographicViewError::Resource)?;
        }
        Ok(Self {
            frame: self.frame,
            views,
        })
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
            GeographicViewError::diagnostic(
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
