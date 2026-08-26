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
// account heap by `rows * size_of::<Shape>()`.  The `Polygon` variant alone sets
// the width (`Ring` 72 + `Arc<[Ring]>` 16 = 88, +8 tag); every other variant is
// 72 or less.  Boxing `Polygon` to reach 80 was measured a net loss — the packed
// lanes never store `Shape`, so it would buy ~2% of a mixed array's footprint
// for a per-row allocation and an indirection on the hottest structural access.
impl HeapSize for Shape {
    /// Retained native heap: coordinate columns **plus** container allocations
    /// (`Vec` of parts/members, `Arc<[Ring]>` hole storage) and nested shapes.
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

    pub(crate) fn from_trusted(coords: CoordSeq) -> Self {
        assert!(
            coords.len() >= 2,
            "trusted LineSeq construction received an empty or one-vertex coordinate sequence"
        );
        Self(coords)
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

    pub(crate) fn into_vec(self) -> Vec<T> {
        self.0
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MultiMember<T> {
    Empty,
    Value(T),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MultiPoint {
    axes: CoordinateAxes,
    coords: CoordSeq,
    members: NonEmptyVec<MultiMember<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MultiLineString {
    axes: CoordinateAxes,
    values: Vec<LineSeq>,
    members: NonEmptyVec<MultiMember<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MultiPolygon {
    axes: CoordinateAxes,
    values: Vec<Polygon>,
    members: NonEmptyVec<MultiMember<usize>>,
}

macro_rules! impl_multipart {
    ($name:ident, $member:ty) => {
        impl $name {
            pub(crate) fn try_new(
                axes: CoordinateAxes,
                members: Vec<MultiMember<$member>>,
            ) -> Result<Self> {
                let mut values = Vec::new();
                let mut indices = Vec::with_capacity(members.len());
                for member in members {
                    match member {
                        MultiMember::Empty => indices.push(MultiMember::Empty),
                        MultiMember::Value(value) => {
                            let got = shape_member_axes(&value);
                            if got != axes {
                                return Err(GeometryErrorKind::CoordinateAxesMismatch {
                                    declared: axes,
                                    got,
                                }
                                .into());
                            }
                            indices.push(MultiMember::Value(values.len()));
                            values.push(value);
                        },
                    }
                }
                Ok(Self {
                    axes,
                    values,
                    members: NonEmptyVec::try_from_vec(indices)?,
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

            pub(crate) fn members(&self) -> &[MultiMember<usize>] {
                &self.members
            }

            pub(crate) fn try_map_values<E>(
                &self,
                axes: CoordinateAxes,
                mut map: impl FnMut(&$member) -> Result<$member, E>,
            ) -> Result<Self, E> {
                let members = self
                    .members
                    .iter()
                    .map(|member| match member {
                        MultiMember::Empty => Ok(MultiMember::Empty),
                        MultiMember::Value(index) => {
                            map(&self.values[*index]).map(MultiMember::Value)
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
                    + self.members.capacity() * std::mem::size_of::<MultiMember<usize>>()
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
        let mut points = Vec::new();
        let mut indices = Vec::with_capacity(members.len());
        for member in members {
            match member {
                MultiMember::Empty => indices.push(MultiMember::Empty),
                MultiMember::Value(point) => {
                    if point.axes() != axes {
                        return Err(GeometryErrorKind::CoordinateAxesMismatch {
                            declared: axes,
                            got: point.axes(),
                        }
                        .into());
                    }
                    indices.push(MultiMember::Value(points.len()));
                    points.push(point);
                },
            }
        }
        let members = NonEmptyVec::try_from_vec(indices)?;
        Ok(Self {
            axes,
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
        let members = (0..coords.len())
            .map(MultiMember::Value)
            .collect::<Vec<_>>();
        Ok(Self {
            axes: coords.axes(),
            coords,
            members: NonEmptyVec::from_vec(members),
        })
    }

    pub(crate) const fn axes(&self) -> CoordinateAxes {
        self.axes
    }

    pub(crate) fn members(&self) -> &[MultiMember<usize>] {
        &self.members
    }

    pub(crate) fn len(&self) -> usize {
        self.members.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.coords.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = Point> + '_ {
        self.members.iter().filter_map(|member| match member {
            MultiMember::Value(index) => Some(self.coords.point_at(*index)),
            MultiMember::Empty => None,
        })
    }

    pub(crate) fn points(&self) -> impl Iterator<Item = Point> + '_ {
        self.members.iter().filter_map(|member| match member {
            MultiMember::Value(index) => Some(self.coords.point_at(*index)),
            MultiMember::Empty => None,
        })
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
            axes: coords.axes(),
            coords,
            members: self.members.clone(),
        })
    }

    pub(crate) fn try_map_values<E>(
        &self,
        mut map: impl FnMut(Point) -> Result<Point, E>,
    ) -> Result<Self, E> {
        let members = self
            .members
            .iter()
            .map(|member| match member {
                MultiMember::Empty => Ok(MultiMember::Empty),
                MultiMember::Value(index) => {
                    map(self.coords.point_at(*index)).map(MultiMember::Value)
                },
            })
            .collect::<Result<Vec<_>, _>>()?;
        let axes = members
            .iter()
            .find_map(|member| match member {
                MultiMember::Value(point) => Some(point.axes()),
                MultiMember::Empty => None,
            })
            .unwrap_or(self.axes);
        Ok(Self::from_values(axes, members))
    }

    pub(crate) fn quantize(&self, precision: i32) -> Self {
        let coords = self.coords.quantize(precision);
        Self {
            axes: coords.axes(),
            coords,
            members: self.members.clone(),
        }
    }

    pub(crate) fn force_2d(&self) -> Self {
        let coords = self.coords.force_2d();
        Self {
            axes: CoordinateAxes::XY,
            coords,
            members: self.members.clone(),
        }
    }

    pub(crate) fn with_axes(&self, axes: CoordinateAxes) -> Self {
        Self {
            axes,
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
            axes: coords.axes(),
            coords,
            members: self.members.clone(),
        }
    }
}

impl<'a> IntoIterator for &'a MultiPoint {
    type Item = Point;
    type IntoIter = std::vec::IntoIter<Point>;

    fn into_iter(self) -> Self::IntoIter {
        self.points().collect::<Vec<_>>().into_iter()
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
        let members = iter.into_iter().map(MultiMember::Value).collect::<Vec<_>>();
        let axes = members
            .iter()
            .find_map(|member| match member {
                MultiMember::Value(point) => Some(point.axes()),
                MultiMember::Empty => None,
            })
            .unwrap_or(CoordinateAxes::XY);
        Self::from_values(axes, members)
    }
}

impl FromIterator<LineSeq> for MultiLineString {
    fn from_iter<T: IntoIterator<Item = LineSeq>>(iter: T) -> Self {
        let members = iter.into_iter().map(MultiMember::Value).collect::<Vec<_>>();
        let axes = members
            .first()
            .map(|member| match member {
                MultiMember::Value(line) => line.axes(),
                MultiMember::Empty => CoordinateAxes::XY,
            })
            .unwrap_or(CoordinateAxes::XY);
        Self::from_values(axes, members)
    }
}

impl From<Vec<LineSeq>> for MultiLineString {
    fn from(lines: Vec<LineSeq>) -> Self {
        lines.into_iter().collect()
    }
}

impl FromIterator<Polygon> for MultiPolygon {
    fn from_iter<T: IntoIterator<Item = Polygon>>(iter: T) -> Self {
        let members = iter.into_iter().map(MultiMember::Value).collect::<Vec<_>>();
        let axes = members
            .first()
            .map(|member| match member {
                MultiMember::Value(polygon) => polygon.axes(),
                MultiMember::Empty => CoordinateAxes::XY,
            })
            .unwrap_or(CoordinateAxes::XY);
        Self::from_values(axes, members)
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
        self.coords.coordinate_bytes()
            + self.members.capacity() * std::mem::size_of::<MultiMember<usize>>()
    }
}

macro_rules! impl_multipart_iteration {
    ($name:ident, $member:ty) => {
        impl $name {
            pub(crate) fn len(&self) -> usize {
                self.members.len()
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
        let mut members = self.members.clone().into_vec();
        let value = members.pop().and_then(|member| match member {
            MultiMember::Value(index) => Some(self.values[index].clone()),
            MultiMember::Empty => None,
        });
        if members.is_empty() {
            return value;
        }
        self.members = NonEmptyVec::from_vec(members);
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
