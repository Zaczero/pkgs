use std::ops::Deref;

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::GeometryKind;
use crate::geometry::types::{
    CoordIter, CoordSeq, CoordinateAxes, Coordinates, GeometryErrorKind, Point, Polygon, XY,
};

#[derive(Clone, Copy, Debug)]
pub enum VoronoiBoundary<'a> {
    Padded,
    Envelope,
    Polygon(&'a Polygon),
}

crate::tokens::token_enum! {
    /// Buffer end-cap style. Parsed once at the Python boundary so the kernel
    /// works with an exhaustively-matched value instead of a raw `&str`.
    pub enum BufferCapStyle("buffer cap_style", param = "cap_style") {
        Round = "round",
        Flat = "flat",
        Square = "square",
    }
}

crate::tokens::token_enum! {
    /// Buffer line-join style (see [`BufferCapStyle`]).
    pub enum BufferJoinStyle("buffer join_style", param = "join_style") {
        Round = "round",
        Miter = "miter",
        Bevel = "bevel",
    }
}

crate::tokens::token_enum! {
    /// Which side(s) of a lineal geometry a buffer grows: symmetric
    /// (``'both'``) or one-sided strips (``'left'``/``'right'`` of the line
    /// direction). Sided buffers use flat ends and miter joins along the
    /// offset edge, like ``offset_curve``.
    pub enum BufferSide("buffer side", param = "side") {
        Both = "both",
        Left = "left",
        Right = "right",
    }
}

crate::tokens::token_enum! {
    /// Line/area simplification algorithm. `Vw` (Visvalingam-Whyatt, the
    /// default) drops the least visually significant vertices first — the
    /// smallest effective triangle spanned with its neighbors — for a smoother
    /// cartographic result; `Dp` (Douglas-Peucker) drops vertices that fall
    /// within a perpendicular-distance band of the retained chord. Both read
    /// `tolerance` on the same distance scale.
    pub enum SimplifyMethod("simplify method", param = "method") {
        Vw = "vw",
        Dp = "dp",
    }
}

crate::tokens::token_enum! {
    /// Line and polygon boundary smoothing algorithm. `Chaikin` (the default)
    /// applies corner-cutting quadratic B-spline refinement; `CatmullRom`
    /// subdivides each segment with a centripetal Catmull-Rom cubic that
    /// interpolates every original vertex.
    pub enum SmoothMethod("smooth method", param = "method") {
        Chaikin = "chaikin",
        CatmullRom = "catmull_rom",
    }
}

crate::tokens::token_enum! {
    /// Geometry repair strategy (see [`BufferCapStyle`]). `Linework` nodes all
    /// boundary linework and reassembles regions by even-odd parity (every
    /// input edge participates); `Structure` rebuilds each ring's enclosed
    /// area and recombines them as shells-minus-holes, discarding collapsed
    /// components.
    pub enum RepairMethod("repair method", param = "method") {
        Linework = "linework",
        Structure = "structure",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Shape {
    Point(Point),
    MultiPoint(MultiPoint),
    LineString(LineSeq),
    MultiLineString(MultiLineString),
    Polygon(Polygon),
    MultiPolygon(MultiPolygon),
    GeometryCollection(NonEmptyVec<Self>),
    /// A typed empty geometry carrying its declared coordinate axes, so
    /// `POINT Z EMPTY` round-trips distinct from `POINT EMPTY`.
    ///
    /// Every top-level empty has its OGC kind and coordinate axes. Empty
    /// multipart members are represented inside their owning multipart value.
    Empty(GeometryKind, CoordinateAxes),
}

// `Shape` is the widest hot type and the one whose width multiplies: mixed
// array storage is `Vec<Shape>`, and both `storage_impl.rs` and `packed_ops.rs`
// account heap by `rows * size_of::<Shape>()`. `Polygon` and `MultiPoint` carry
// 88-byte payloads, producing a 96-byte enum. Boxing either would trade the hot
// structural access for a per-row allocation and indirection.
impl HeapSize for Shape {
    /// Retained native heap: coordinate columns **plus** container allocations
    /// (`Vec` of parts, sparse member maps, `Arc<[Ring]>` hole storage) and
    /// nested shapes.
    /// Leaf points and sequences contribute only their ordinate payload so
    /// scalar `Point`/`LineString` sizes stay in the coordinate-class band;
    /// multipart and collection containers scale with part/member count.
    fn heap_bytes(&self) -> usize {
        match self {
            Self::MultiLineString(lines) => lines.heap_bytes(),
            Self::Polygon(polygon) => polygon.heap_bytes(),
            Self::MultiPolygon(polygons) => polygons.heap_bytes(),
            Self::GeometryCollection(geometries) => {
                geometries.capacity() * std::mem::size_of::<Self>()
                    + geometries.iter().map(HeapSize::heap_bytes).sum::<usize>()
            },
            Self::MultiPoint(points) => points.heap_bytes(),
            // Empty / Point / LineString: ordinate payload only.
            leaf => leaf.coordinate_bytes(),
        }
    }
}

/// Coordinate sequence carried by lineal shapes. A `LineSeq` always has at
/// least two vertices, so an empty or one-vertex `LineString` is
/// unrepresentable. Empty line members use [`MultiMember::Empty`] and
/// top-level empties use [`Shape::Empty`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LineSeq(CoordSeq);

impl LineSeq {
    pub(crate) fn try_new(coords: CoordSeq) -> Result<Self> {
        if coords.len() < 2 {
            return Err(GeometryErrorKind::LineStringTooShort.into());
        }
        Ok(Self(coords))
    }

    pub(crate) const fn as_coords(&self) -> &CoordSeq {
        &self.0
    }
}

impl Deref for LineSeq {
    type Target = CoordSeq;

    fn deref(&self) -> &Self::Target {
        self.as_coords()
    }
}

impl AsRef<CoordSeq> for LineSeq {
    fn as_ref(&self) -> &CoordSeq {
        self.as_coords()
    }
}

impl<'a> IntoIterator for &'a LineSeq {
    type Item = Point;
    type IntoIter = CoordIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Coordinates for LineSeq {
    fn coord_count(&self) -> usize {
        self.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self.point_at(index)
    }

    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        Some((self.xs(), self.ys()))
    }

    fn z_column(&self) -> Option<&[f64]> {
        self.zs()
    }

    fn m_column(&self) -> Option<&[f64]> {
        self.ms()
    }
}

impl crate::HeapSize for LineSeq {
    fn heap_bytes(&self) -> usize {
        self.coordinate_bytes()
    }
}

/// A checked nonempty vector used by shape payloads. Empty top-level values
/// have a typed [`Shape::Empty`] variant instead.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NonEmptyVec<T>(Vec<T>);

impl<T> NonEmptyVec<T> {
    pub(crate) fn try_from_vec(items: Vec<T>) -> Result<Self> {
        if items.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "geometry payload",
            }
            .into());
        }
        Ok(Self(items))
    }

    pub(crate) fn from_vec(items: Vec<T>) -> Self {
        Self::try_from_vec(items).expect("shape payload must not be empty")
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.0.capacity()
    }
}

impl<T> std::ops::Deref for NonEmptyVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, T> IntoIterator for &'a NonEmptyVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T> IntoIterator for NonEmptyVec<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MultiMember<T> {
    Empty,
    Value(T),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum MultipartMembers {
    Dense,
    // Contains at least one Empty; Value indices are consecutive encounter order.
    Sparse(Box<[MultiMember<usize>]>),
}

impl MultipartMembers {
    fn from_indices(indices: Vec<MultiMember<usize>>) -> Result<Self> {
        if indices.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "geometry payload",
            }
            .into());
        }
        let mut expected = 0;
        let mut has_empty = false;
        let mut indices = indices;
        for member in &mut indices {
            match member {
                MultiMember::Empty => has_empty = true,
                MultiMember::Value(index) => {
                    *index = expected;
                    expected += 1;
                },
            }
        }
        Ok(if has_empty {
            Self::Sparse(indices.into_boxed_slice())
        } else {
            Self::Dense
        })
    }

    fn len(&self, value_len: usize) -> usize {
        match self {
            Self::Dense => value_len,
            Self::Sparse(members) => members.len(),
        }
    }

    const fn has_empty_members(&self) -> bool {
        matches!(self, Self::Sparse(_))
    }

    fn get(&self, index: usize, value_len: usize) -> Option<MultiMember<usize>> {
        match self {
            Self::Dense => (index < value_len).then_some(MultiMember::Value(index)),
            Self::Sparse(members) => members.get(index).copied(),
        }
    }

    fn iter(&self, value_len: usize) -> MultipartMemberIter<'_> {
        match self {
            Self::Dense => MultipartMemberIter::Dense(0..value_len),
            Self::Sparse(members) => MultipartMemberIter::Sparse(members.iter()),
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Dense => 0,
            Self::Sparse(members) => std::mem::size_of_val(&**members),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum MultipartMemberIter<'a> {
    Dense(std::ops::Range<usize>),
    Sparse(std::slice::Iter<'a, MultiMember<usize>>),
}

impl Iterator for MultipartMemberIter<'_> {
    type Item = MultiMember<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(indices) => indices.next().map(MultiMember::Value),
            Self::Sparse(members) => members.next().copied(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl DoubleEndedIterator for MultipartMemberIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(indices) => indices.next_back().map(MultiMember::Value),
            Self::Sparse(members) => members.next_back().copied(),
        }
    }
}

impl ExactSizeIterator for MultipartMemberIter<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Dense(indices) => indices.len(),
            Self::Sparse(members) => members.len(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MultiPoint {
    coords: CoordSeq,
    members: MultipartMembers,
}

/// Borrowed `MultiPoint` values in logical member order.
///
/// Empty members have no coordinate row and are skipped; each value is rebuilt
/// directly from the shared coordinate columns.
pub struct MultiPointIter<'a> {
    members: MultipartMemberIter<'a>,
    coords: &'a CoordSeq,
    remaining: usize,
}

impl Iterator for MultiPointIter<'_> {
    type Item = Point;

    fn next(&mut self) -> Option<Self::Item> {
        self.members.find_map(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(index) => {
                self.remaining -= 1;
                Some(self.coords.point_at(index))
            },
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl DoubleEndedIterator for MultiPointIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some(member) = self.members.next_back() {
            if let MultiMember::Value(index) = member {
                self.remaining -= 1;
                return Some(self.coords.point_at(index));
            }
        }
        None
    }
}

impl ExactSizeIterator for MultiPointIter<'_> {}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MultiLineString {
    axes: CoordinateAxes,
    values: Vec<LineSeq>,
    members: MultipartMembers,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MultiPolygon {
    axes: CoordinateAxes,
    values: Vec<Polygon>,
    members: MultipartMembers,
}

macro_rules! impl_multipart {
    ($name:ident, $member:ty) => {
        impl $name {
            pub(crate) fn try_new(
                axes: CoordinateAxes,
                members: Vec<MultiMember<$member>>,
            ) -> Result<Self> {
                let member_len = members.len();
                let mut values = Vec::new();
                let mut indices = Vec::new();
                let mut sparse = false;
                for member in members {
                    match member {
                        MultiMember::Empty => {
                            if !sparse {
                                indices = Vec::with_capacity(member_len);
                                indices.extend((0..values.len()).map(MultiMember::Value));
                                sparse = true;
                            }
                            indices.push(MultiMember::Empty);
                        },
                        MultiMember::Value(value) => {
                            let got = shape_member_axes(&value);
                            if got != axes {
                                return Err(GeometryErrorKind::CoordinateAxesMismatch {
                                    declared: axes,
                                    got,
                                }
                                .into());
                            }
                            if sparse {
                                indices.push(MultiMember::Value(values.len()));
                            }
                            values.push(value);
                        },
                    }
                }
                let members = if sparse {
                    MultipartMembers::from_indices(indices)?
                } else if values.is_empty() {
                    return Err(GeometryErrorKind::EmptyGeometrySequence {
                        operation: "geometry payload",
                    }
                    .into());
                } else {
                    MultipartMembers::Dense
                };
                Ok(Self {
                    axes,
                    values,
                    members,
                })
            }

            pub(crate) fn from_values(
                axes: CoordinateAxes,
                members: Vec<MultiMember<$member>>,
            ) -> Self {
                Self::try_new(axes, members).expect("homogeneous nonempty multipart payload")
            }

            pub(crate) const fn axes(&self) -> CoordinateAxes {
                self.axes
            }

            pub(crate) fn members(&self) -> MultipartMemberIter<'_> {
                self.members.iter(self.values.len())
            }

            pub(crate) fn member_at(&self, index: usize) -> Option<MultiMember<usize>> {
                self.members.get(index, self.values.len())
            }

            pub(crate) fn has_empty_members(&self) -> bool {
                self.members.has_empty_members()
            }

            pub(crate) fn try_map_values<E>(
                &self,
                axes: CoordinateAxes,
                mut map: impl FnMut(&$member) -> Result<$member, E>,
            ) -> Result<Self, E> {
                if matches!(self.members, MultipartMembers::Dense) {
                    let values = self
                        .values
                        .iter()
                        .map(&mut map)
                        .collect::<Result<Vec<_>, _>>()?;
                    assert!(
                        values.iter().all(|value| shape_member_axes(value) == axes),
                        "homogeneous nonempty multipart payload"
                    );
                    return Ok(Self {
                        axes,
                        values,
                        members: MultipartMembers::Dense,
                    });
                }
                let members = self
                    .members()
                    .map(|member| match member {
                        MultiMember::Empty => Ok(MultiMember::Empty),
                        MultiMember::Value(index) => {
                            map(&self.values[index]).map(MultiMember::Value)
                        },
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::from_values(axes, members))
            }

            pub(crate) fn with_axes(&self, axes: CoordinateAxes) -> Self {
                Self {
                    axes,
                    values: self.values.clone(),
                    members: self.members.clone(),
                }
            }
        }

        impl crate::HeapSize for $name {
            fn heap_bytes(&self) -> usize {
                self.values.capacity() * std::mem::size_of::<$member>()
                    + self.members.heap_bytes()
                    + self
                        .values
                        .iter()
                        .map(crate::HeapSize::heap_bytes)
                        .sum::<usize>()
            }
        }
    };
}

impl_multipart!(MultiLineString, LineSeq);
impl_multipart!(MultiPolygon, Polygon);

impl MultiPoint {
    pub(crate) fn try_new(axes: CoordinateAxes, members: Vec<MultiMember<Point>>) -> Result<Self> {
        let member_len = members.len();
        let mut points = Vec::new();
        let mut indices = Vec::new();
        let mut sparse = false;
        for member in members {
            match member {
                MultiMember::Empty => {
                    if !sparse {
                        indices = Vec::with_capacity(member_len);
                        indices.extend((0..points.len()).map(MultiMember::Value));
                        sparse = true;
                    }
                    indices.push(MultiMember::Empty);
                },
                MultiMember::Value(point) => {
                    if point.axes() != axes {
                        return Err(GeometryErrorKind::CoordinateAxesMismatch {
                            declared: axes,
                            got: point.axes(),
                        }
                        .into());
                    }
                    if sparse {
                        indices.push(MultiMember::Value(points.len()));
                    }
                    points.push(point);
                },
            }
        }
        let members = if sparse {
            MultipartMembers::from_indices(indices)?
        } else if points.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "geometry payload",
            }
            .into());
        } else {
            MultipartMembers::Dense
        };
        Ok(Self {
            coords: if points.is_empty() {
                CoordSeq::empty(axes)
            } else {
                CoordSeq::from_points(&points)
            },
            members,
        })
    }

    pub(crate) fn from_values(axes: CoordinateAxes, members: Vec<MultiMember<Point>>) -> Self {
        Self::try_new(axes, members).expect("homogeneous nonempty multipart payload")
    }

    pub(crate) fn from_coordseq(coords: CoordSeq) -> Result<Self> {
        if coords.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "MultiPoint payload",
            }
            .into());
        }
        Ok(Self {
            coords,
            members: MultipartMembers::Dense,
        })
    }

    pub(crate) const fn axes(&self) -> CoordinateAxes {
        self.coords.axes()
    }

    pub(crate) fn members(&self) -> MultipartMemberIter<'_> {
        self.members.iter(self.coords.len())
    }

    pub(crate) fn member_at(&self, index: usize) -> Option<MultiMember<usize>> {
        self.members.get(index, self.coords.len())
    }

    pub(crate) fn has_empty_members(&self) -> bool {
        self.members.has_empty_members()
    }

    pub(crate) fn len(&self) -> usize {
        self.members.len(self.coords.len())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.coords.is_empty()
    }

    pub(crate) fn iter(&self) -> MultiPointIter<'_> {
        MultiPointIter {
            members: self.members(),
            coords: &self.coords,
            remaining: self.coords.len(),
        }
    }

    pub(crate) fn points(&self) -> MultiPointIter<'_> {
        self.iter()
    }

    pub(crate) fn point_at(&self, index: usize) -> Point {
        self.coords.point_at(index)
    }

    pub(crate) fn to_vec(&self) -> Vec<Point> {
        self.points().collect()
    }

    pub(crate) fn xs(&self) -> &[f64] {
        self.coords.xs()
    }

    pub(crate) fn ys(&self) -> &[f64] {
        self.coords.ys()
    }

    pub(crate) fn zs(&self) -> Option<&[f64]> {
        self.coords.zs()
    }

    pub(crate) fn ms(&self) -> Option<&[f64]> {
        self.coords.ms()
    }

    pub(crate) fn coordinate_bytes(&self) -> usize {
        self.coords.coordinate_bytes()
    }

    pub(crate) fn try_map_xy_with<E>(
        &self,
        op: impl FnMut(f64, f64) -> Result<(f64, f64), E>,
    ) -> Result<Self, E> {
        let coords = self.coords.try_map_xy_with(op)?;
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
    }

    pub(crate) fn try_map_values<E>(
        &self,
        mut map: impl FnMut(Point) -> Result<Point, E>,
    ) -> Result<Self, E> {
        if matches!(self.members, MultipartMembers::Dense) {
            let points = self.points().map(&mut map).collect::<Result<Vec<_>, _>>()?;
            return Ok(Self::from(points));
        }
        let members = self
            .members()
            .map(|member| match member {
                MultiMember::Empty => Ok(MultiMember::Empty),
                MultiMember::Value(index) => {
                    map(self.coords.point_at(index)).map(MultiMember::Value)
                },
            })
            .collect::<Result<Vec<_>, _>>()?;
        let axes = members
            .iter()
            .find_map(|member| match member {
                MultiMember::Value(point) => Some(point.axes()),
                MultiMember::Empty => None,
            })
            .unwrap_or_else(|| self.axes());
        Ok(Self::from_values(axes, members))
    }

    pub(crate) fn quantize(&self, precision: i32) -> Self {
        let coords = self.coords.quantize(precision);
        Self {
            coords,
            members: self.members.clone(),
        }
    }

    pub(crate) fn force_2d(&self) -> Self {
        let coords = self.coords.force_2d();
        Self {
            coords,
            members: self.members.clone(),
        }
    }

    pub(crate) fn with_axes(&self, axes: CoordinateAxes) -> Self {
        Self {
            coords: if self.coords.is_empty() {
                CoordSeq::empty(axes)
            } else {
                self.coords.clone()
            },
            members: self.members.clone(),
        }
    }

    pub(crate) fn with_coords(&self, coords: CoordSeq) -> Self {
        debug_assert_eq!(coords.len(), self.coords.len());
        Self {
            coords,
            members: self.members.clone(),
        }
    }
}

impl<'a> IntoIterator for &'a MultiPoint {
    type Item = Point;
    type IntoIter = MultiPointIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for MultiPoint {
    type Item = Point;
    type IntoIter = std::vec::IntoIter<Point>;

    fn into_iter(self) -> Self::IntoIter {
        self.to_vec().into_iter()
    }
}

impl From<CoordSeq> for MultiPoint {
    fn from(coords: CoordSeq) -> Self {
        Self::from_coordseq(coords).expect("MultiPoint payload must not be empty")
    }
}

impl From<Vec<Point>> for MultiPoint {
    fn from(points: Vec<Point>) -> Self {
        Self::from_coordseq(CoordSeq::from_points(&points))
            .expect("MultiPoint payload must not be empty")
    }
}

impl From<Vec<XY>> for MultiPoint {
    fn from(points: Vec<XY>) -> Self {
        Self::from_coordseq(CoordSeq::from_xy(&points))
            .expect("MultiPoint payload must not be empty")
    }
}

impl FromIterator<Point> for MultiPoint {
    fn from_iter<T: IntoIterator<Item = Point>>(iter: T) -> Self {
        let points = iter.into_iter().collect::<Vec<_>>();
        Self::from(points)
    }
}

impl FromIterator<LineSeq> for MultiLineString {
    fn from_iter<T: IntoIterator<Item = LineSeq>>(iter: T) -> Self {
        let values = iter.into_iter().collect::<Vec<_>>();
        let axes = values
            .first()
            .map_or(CoordinateAxes::XY, |line| line.axes());
        assert!(!values.is_empty(), "shape payload must not be empty");
        assert!(
            values.iter().all(|line| line.axes() == axes),
            "homogeneous nonempty multipart payload"
        );
        Self {
            axes,
            values,
            members: MultipartMembers::Dense,
        }
    }
}

impl From<Vec<LineSeq>> for MultiLineString {
    fn from(lines: Vec<LineSeq>) -> Self {
        lines.into_iter().collect()
    }
}

impl FromIterator<Polygon> for MultiPolygon {
    fn from_iter<T: IntoIterator<Item = Polygon>>(iter: T) -> Self {
        let values = iter.into_iter().collect::<Vec<_>>();
        let axes = values.first().map_or(CoordinateAxes::XY, Polygon::axes);
        assert!(!values.is_empty(), "shape payload must not be empty");
        assert!(
            values.iter().all(|polygon| polygon.axes() == axes),
            "homogeneous nonempty multipart payload"
        );
        Self {
            axes,
            values,
            members: MultipartMembers::Dense,
        }
    }
}

impl From<Vec<Polygon>> for MultiPolygon {
    fn from(polygons: Vec<Polygon>) -> Self {
        polygons.into_iter().collect()
    }
}

impl From<Vec<Shape>> for NonEmptyVec<Shape> {
    fn from(shapes: Vec<Shape>) -> Self {
        Self::from_vec(shapes)
    }
}

impl<T> FromIterator<T> for NonEmptyVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }
}

impl crate::HeapSize for MultiPoint {
    fn heap_bytes(&self) -> usize {
        self.coords.coordinate_bytes() + self.members.heap_bytes()
    }
}

macro_rules! impl_multipart_iteration {
    ($name:ident, $member:ty) => {
        impl $name {
            pub(crate) fn len(&self) -> usize {
                self.members.len(self.values.len())
            }

            pub(crate) fn is_empty(&self) -> bool {
                self.values.is_empty()
            }

            pub(crate) fn iter(&self) -> impl Iterator<Item = &$member> {
                self.values.iter()
            }
        }

        impl<'a> IntoIterator for &'a $name {
            type Item = &'a $member;
            type IntoIter = std::slice::Iter<'a, $member>;

            fn into_iter(self) -> Self::IntoIter {
                self.values.iter()
            }
        }

        impl IntoIterator for $name {
            type Item = $member;
            type IntoIter = std::vec::IntoIter<$member>;

            fn into_iter(self) -> Self::IntoIter {
                self.values.into_iter()
            }
        }
    };
}

impl_multipart_iteration!(MultiLineString, LineSeq);
impl_multipart_iteration!(MultiPolygon, Polygon);

impl MultiLineString {
    pub(crate) fn first(&self) -> Option<&LineSeq> {
        self.iter().next()
    }

    pub(crate) fn as_slice(&self) -> &[LineSeq] {
        &self.values
    }
}

impl MultiPolygon {
    pub(crate) fn first(&self) -> Option<&Polygon> {
        self.iter().next()
    }

    pub(crate) fn pop(&mut self) -> Option<Polygon> {
        if matches!(self.members, MultipartMembers::Dense) {
            return if self.values.len() == 1 {
                self.values.last().cloned()
            } else {
                self.values.pop()
            };
        }
        let mut members = self.members().collect::<Vec<_>>();
        let value = members.pop().and_then(|member| match member {
            MultiMember::Value(index) => {
                debug_assert_eq!(index + 1, self.values.len());
                self.values.pop()
            },
            MultiMember::Empty => None,
        });
        if members.is_empty() {
            return value;
        }
        self.members = MultipartMembers::from_indices(members)
            .expect("remaining multipart members are nonempty");
        value
    }

    pub(crate) fn as_slice(&self) -> &[Polygon] {
        &self.values
    }
}

impl std::ops::Deref for MultiPoint {
    type Target = CoordSeq;

    fn deref(&self) -> &Self::Target {
        &self.coords
    }
}

impl std::ops::Deref for MultiLineString {
    type Target = [LineSeq];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl std::ops::Deref for MultiPolygon {
    type Target = [Polygon];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl Coordinates for MultiPoint {
    fn coord_count(&self) -> usize {
        self.iter().count()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self.point_at(index)
    }

    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        None
    }

    fn z_column(&self) -> Option<&[f64]> {
        None
    }

    fn m_column(&self) -> Option<&[f64]> {
        None
    }
}

impl crate::HeapSize for NonEmptyVec<Shape> {
    fn heap_bytes(&self) -> usize {
        self.capacity() * std::mem::size_of::<Shape>()
            + self.iter().map(HeapSize::heap_bytes).sum::<usize>()
    }
}

impl Point {
    pub(crate) const fn axes(self) -> CoordinateAxes {
        self.axes
    }
}

#[cfg(test)]
mod multipart_layout_tests {
    use super::*;

    #[test]
    fn dense_and_sparse_members_are_canonical() {
        assert!(matches!(
            MultipartMembers::from_indices(vec![MultiMember::Value(0), MultiMember::Value(1)])
                .expect("identity members"),
            MultipartMembers::Dense
        ));
        let dense = MultiPoint::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(Point::new_raw_xy(1.0, 2.0)),
            MultiMember::Value(Point::new_raw_xy(3.0, 4.0)),
        ])
        .expect("dense multipoint");
        assert!(matches!(dense.members, MultipartMembers::Dense));
        assert_eq!(dense.members().collect::<Vec<_>>(), [
            MultiMember::Value(0),
            MultiMember::Value(1)
        ]);
        assert!(!dense.has_empty_members());
        assert_eq!(dense.members.heap_bytes(), 0);

        let sparse = MultiPoint::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(Point::new_raw_xy(1.0, 2.0)),
            MultiMember::Empty,
        ])
        .expect("sparse multipoint");
        assert!(matches!(sparse.members, MultipartMembers::Sparse(_)));
        assert_eq!(sparse.members().collect::<Vec<_>>(), [
            MultiMember::Empty,
            MultiMember::Value(0),
            MultiMember::Empty
        ]);
        assert!(sparse.has_empty_members());
        let MultipartMembers::Sparse(members) = &sparse.members else {
            unreachable!("sparse fixture must retain member metadata");
        };
        assert_eq!(sparse.members.heap_bytes(), std::mem::size_of_val(&**members));
    }
}

impl Polygon {
    pub(crate) fn axes(&self) -> CoordinateAxes {
        self.rings()
            .map(|ring| ring.axes())
            .fold(CoordinateAxes::XY, CoordinateAxes::union)
    }
}

fn shape_member_axes(value: &impl ShapeMemberAxes) -> CoordinateAxes {
    value.shape_member_axes()
}

trait ShapeMemberAxes {
    fn shape_member_axes(&self) -> CoordinateAxes;
}

impl ShapeMemberAxes for Point {
    fn shape_member_axes(&self) -> CoordinateAxes {
        self.axes()
    }
}

impl ShapeMemberAxes for LineSeq {
    fn shape_member_axes(&self) -> CoordinateAxes {
        self.axes()
    }
}

impl ShapeMemberAxes for Polygon {
    fn shape_member_axes(&self) -> CoordinateAxes {
        self.axes()
    }
}
