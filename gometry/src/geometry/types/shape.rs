use std::num::NonZeroU8;
use std::ops::Deref;

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::GeometryKind;
use crate::geometry::types::{
    CoordIter, CoordSeq, CoordWindow, CoordinateAxes, Coordinates, GeometryErrorKind, Point,
    Polygon, SharedSlice, SharedSliceIter, coordseq,
};

/// Maximum number of nested `GeometryCollection` containers in one shape.
pub(crate) const MAX_NESTING: usize = 128;

#[derive(Clone, Copy, Debug)]
pub(crate) enum VoronoiBoundary<'a> {
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
    /// boundary linework and selects each stored ring by its original directed
    /// winding modulo two; `Structure` rebuilds each ring's nonzero-winding
    /// enclosed area. Both compose polygon shells minus their holes before
    /// producing valid polygon output.
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
    GeometryCollection(GeometryCollection),
    /// A typed empty geometry carrying its declared coordinate axes, so
    /// `POINT Z EMPTY` round-trips distinct from `POINT EMPTY`.
    ///
    /// Every top-level empty has its OGC kind and coordinate axes. Empty
    /// multipart members are represented inside their owning multipart value.
    Empty(GeometryKind, CoordinateAxes),
}

impl Shape {
    /// Admit collection children after checking the complete subtree without
    /// recursion. Empty input uses the typed empty collection representation.
    pub(crate) fn geometry_collection(parts: Vec<Self>) -> Result<Self> {
        if parts.is_empty() {
            return Ok(Self::Empty(
                GeometryKind::GeometryCollection,
                CoordinateAxes::XY,
            ));
        }
        Ok(Self::GeometryCollection(GeometryCollection::try_new(
            parts,
        )?))
    }

    /// Maximum collection depth in this shape.
    pub(crate) const fn nesting_depth(&self) -> usize {
        match self {
            Self::GeometryCollection(collection) => collection.depth(),
            _ => 0,
        }
    }
}

// `Shape` is the widest hot type and the one whose width multiplies: mixed
// array storage is `Vec<Shape>`, and both `storage_impl.rs` and `packed_ops.rs`
// account heap by `rows * size_of::<Shape>()`. `Polygon` and `MultiPoint` carry
// 88-byte payloads, producing a 96-byte enum. Boxing either would trade the hot
// structural access for a per-row allocation and indirection.
impl HeapSize for Shape {
    /// Retained native heap. Multipart, polygon, line, and collection payloads
    /// delegate to their owners, which include `SharedSlice` Arc/Vec capacity.
    /// Empty and Point have no shared owner and report only coordinate bytes.
    fn heap_bytes(&self) -> usize {
        match self {
            Self::MultiLineString(lines) => lines.heap_bytes(),
            Self::Polygon(polygon) => polygon.heap_bytes(),
            Self::MultiPolygon(polygons) => polygons.heap_bytes(),
            Self::GeometryCollection(geometries) => geometries.heap_bytes(),
            Self::MultiPoint(points) => points.heap_bytes(),
            Self::LineString(line) => line.heap_bytes(),
            // Empty / Point: no shared coordinate owner.
            leaf => leaf.coordinate_bytes(),
        }
    }
}

impl Shape {
    pub(crate) fn logical_heap_bytes(&self) -> usize {
        match self {
            Self::LineString(line) => line.logical_heap_bytes(),
            Self::Polygon(polygon) => polygon.logical_heap_bytes(),
            _ => self.heap_bytes(),
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
    /// Admit a coordinate sequence of unknown length — parsed input, a noded
    /// or merged chain, anything whose vertex count is not already proven.
    pub(crate) fn try_new(coords: CoordSeq) -> Result<Self> {
        if coords.len() < 2 {
            return Err(GeometryErrorKind::LineStringTooShort.into());
        }
        Ok(Self(coords))
    }

    /// Wrap a sequence whose length this crate already knows to be at least
    /// two: a length-preserving edit, a pass that keeps both endpoints, a ring,
    /// a stored row, a caller-side `len()` match. **Parsed or otherwise
    /// untrusted input uses [`Self::try_new`]**, whose typed error says what is
    /// wrong with the input instead of aborting on our own bug. The check
    /// stays in release builds — one comparison against a column allocation
    /// that just happened.
    pub(crate) fn from_proven(coords: CoordSeq) -> Self {
        assert!(
            coords.len() >= 2,
            "LineSeq::from_proven requires at least two vertices",
        );
        Self(coords)
    }

    /// The two-vertex line through `start` and `end`. Endpoints are coerced to
    /// the ordinate columns both carry, so a 2D endpoint never gains fabricated
    /// Z/M; this is the one owner of that rule for `shortest_line`,
    /// `minimum_clearance_line`, and the circle-radius lines.
    pub(crate) fn segment(start: Point, end: Point) -> Result<Self> {
        Ok(Self(coordseq::coerced_pair(start, end)?))
    }

    /// A stored line row addressed by `window` into packed columns. Line
    /// storage admits its rows at ingress, so a stored row is never shorter
    /// than two vertices.
    pub(crate) fn row_view(coords: &CoordSeq, window: CoordWindow) -> Self {
        Self::from_proven(coords.view(window))
    }

    pub(crate) const fn as_coords(&self) -> &CoordSeq {
        &self.0
    }

    /// Per-vertex map. The vertex count is unchanged, so the invariant holds
    /// without a recheck.
    pub(crate) fn try_map_points<E>(
        &self,
        map: impl FnMut(Point) -> Result<Point, E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        let points = crate::try_collect_vec(self.iter().map(map))?;
        Ok(Self(CoordSeq::try_from_points(&points).map_err(E::from)?))
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
        self.as_coords().heap_bytes()
    }
}

impl LineSeq {
    pub(crate) fn logical_heap_bytes(&self) -> usize {
        self.as_coords().logical_heap_bytes()
    }
}

/// A checked nonempty vector used by shape payloads. Empty top-level values
/// have a typed [`Shape::Empty`] variant instead.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NonEmptyVec<T>(SharedSlice<T>);

impl<T> NonEmptyVec<T> {
    pub(crate) fn try_from_vec(items: Vec<T>) -> Result<Self> {
        if items.is_empty() {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "geometry payload",
            }
            .into());
        }
        Ok(Self(SharedSlice::from_vec(items)))
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
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

impl<T: Clone> IntoIterator for NonEmptyVec<T> {
    type Item = T;
    type IntoIter = SharedSliceIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_owned()
    }
}

impl<T: crate::HeapSize> crate::HeapSize for NonEmptyVec<T> {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}

/// A checked, immutable collection payload carrying its exact nesting depth.
/// The members stay behind this type so no operation can mutate them without
/// rebuilding and rechecking the enclosing collection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GeometryCollection {
    members: NonEmptyVec<Shape>,
    depth: NonZeroU8,
}

impl GeometryCollection {
    fn try_new(parts: Vec<Shape>) -> Result<Self> {
        let members = NonEmptyVec::try_from_vec(parts)?;
        Self::from_members(members, MAX_NESTING)
    }

    fn from_members(members: NonEmptyVec<Shape>, limit: usize) -> Result<Self> {
        let depth = members
            .iter()
            .map(Shape::nesting_depth)
            .max()
            .expect("non-empty collection payload")
            + 1;
        if depth > MAX_NESTING || depth > limit {
            return Err(collection_depth_error());
        }
        Ok(Self {
            members,
            depth: NonZeroU8::new(depth as u8).expect("collection depth is non-zero"),
        })
    }

    /// Rebuild members while enforcing that the new collection is no deeper
    /// than the source collection. This is the path for structure-preserving
    /// transforms; arbitrary children use [`Shape::geometry_collection`].
    pub(crate) fn try_map_members<E, F>(&self, mut map: F) -> std::result::Result<Self, E>
    where
        E: From<crate::error::Error>,
        F: FnMut(&Shape) -> std::result::Result<Shape, E>,
    {
        let members = crate::try_collect_vec(self.members.iter().map(&mut map))?;
        Self::from_members(
            NonEmptyVec::try_from_vec(members).map_err(E::from)?,
            self.depth.get() as usize,
        )
        .map_err(E::from)
    }

    pub(crate) fn try_map_owned_members<E, F>(self, mut map: F) -> std::result::Result<Self, E>
    where
        E: From<crate::error::Error>,
        F: FnMut(Shape) -> std::result::Result<Shape, E>,
    {
        let limit = self.depth.get() as usize;
        let members = crate::try_collect_vec(self.members.into_iter().map(&mut map))?;
        Self::from_members(NonEmptyVec::try_from_vec(members).map_err(E::from)?, limit)
            .map_err(E::from)
    }

    pub(crate) fn try_rebuild_members(&self, members: Vec<Shape>) -> Result<Self> {
        Self::from_members(NonEmptyVec::try_from_vec(members)?, self.depth())
    }

    pub(crate) fn try_filter_map_owned_members<F>(self, mut map: F) -> Result<Shape>
    where
        F: FnMut(Shape) -> Result<Option<Shape>>,
    {
        let limit = self.depth();
        let mut members = Vec::new();
        crate::boundary::try_reserve_vec(&mut members, self.members.len())?;
        for shape in self.members {
            if let Some(mapped) = map(shape)? {
                crate::try_push_vec(&mut members, mapped)?;
            }
        }
        if members.is_empty() {
            return Ok(Shape::typed_empty(
                GeometryKind::GeometryCollection,
                CoordinateAxes::XY,
            ));
        }
        Ok(Shape::GeometryCollection(Self::from_members(
            NonEmptyVec::try_from_vec(members)?,
            limit,
        )?))
    }

    pub(crate) const fn depth(&self) -> usize {
        self.depth.get() as usize
    }

    pub(crate) fn as_slice(&self) -> &[Shape] {
        &self.members
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, Shape> {
        self.members.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.members.len()
    }
}

impl Deref for GeometryCollection {
    type Target = [Shape];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a> IntoIterator for &'a GeometryCollection {
    type Item = &'a Shape;
    type IntoIter = std::slice::Iter<'a, Shape>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for GeometryCollection {
    type Item = Shape;
    type IntoIter = SharedSliceIter<Shape>;

    fn into_iter(self) -> Self::IntoIter {
        self.members.into_iter()
    }
}

fn collection_depth_error() -> crate::error::Error {
    GeometryErrorKind::message(format!(
        "geometry collection nesting exceeds the limit of {MAX_NESTING}"
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MultiMember<T> {
    Empty,
    Value(T),
}

impl<T: crate::HeapSize> crate::HeapSize for MultiMember<T> {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Value(value) => value.heap_bytes(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum MultipartMembers {
    Dense,
    // Contains at least one Empty; Value indices are consecutive encounter order.
    Sparse(SharedSlice<MultiMember<usize>>),
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
            Self::Sparse(SharedSlice::from_vec(indices))
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
            Self::Sparse(members) => members.heap_bytes(),
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
    values: SharedSlice<LineSeq>,
    members: MultipartMembers,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MultiPolygon {
    axes: CoordinateAxes,
    values: SharedSlice<Polygon>,
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
                crate::boundary::try_reserve_vec(&mut values, member_len)?;
                let mut indices = Vec::new();
                let mut sparse = false;
                for member in members {
                    match member {
                        MultiMember::Empty => {
                            if !sparse {
                                crate::boundary::try_reserve_vec(&mut indices, member_len)?;
                                for index in 0..values.len() {
                                    crate::try_push_vec(&mut indices, MultiMember::Value(index))?;
                                }
                                sparse = true;
                            }
                            crate::try_push_vec(&mut indices, MultiMember::Empty)?;
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
                                crate::try_push_vec(
                                    &mut indices,
                                    MultiMember::Value(values.len()),
                                )?;
                            }
                            crate::try_push_vec(&mut values, value)?;
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
                let values = SharedSlice::from_vec(values);
                Ok(Self {
                    axes,
                    values,
                    members,
                })
            }

            pub(crate) fn from_values(
                axes: CoordinateAxes,
                members: Vec<MultiMember<$member>>,
            ) -> Result<Self> {
                Self::try_new(axes, members)
            }

            /// A dense multipart from members that are all present, declaring
            /// the axes of the first. Axis-homogeneity is a payload invariant
            /// (the DbC boundary rule [`CoordSeq::try_from_points`] follows);
            /// external members are admitted through [`Self::try_new`], which
            /// checks. Returns `None` for an empty payload — that is
            /// `Shape::Empty`, not a multipart with no members.
            pub(crate) fn from_dense(values: Vec<$member>) -> Result<Option<Self>> {
                let Some(first) = values.first() else {
                    return Ok(None);
                };
                let axes = shape_member_axes(first);
                debug_assert!(
                    values.iter().all(|value| shape_member_axes(value) == axes),
                    "from_dense requires an axis-homogeneous payload",
                );
                Ok(Some(Self {
                    axes,
                    values: SharedSlice::from_vec(values),
                    members: MultipartMembers::Dense,
                }))
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

            pub(crate) const fn has_empty_members(&self) -> bool {
                self.members.has_empty_members()
            }

            pub(crate) fn try_map_values<E>(
                &self,
                axes: CoordinateAxes,
                mut map: impl FnMut(&$member) -> Result<$member, E>,
            ) -> Result<Self, E>
            where
                E: From<crate::error::Error>,
            {
                if matches!(self.members, MultipartMembers::Dense) {
                    let values = crate::try_collect_vec(self.values.iter().map(&mut map))?;
                    assert!(
                        values.iter().all(|value| shape_member_axes(value) == axes),
                        "homogeneous nonempty multipart payload"
                    );
                    return Ok(Self {
                        axes,
                        values: SharedSlice::from_vec(values),
                        members: MultipartMembers::Dense,
                    });
                }
                let members = crate::try_collect_vec(self.members().map(|member| match member {
                    MultiMember::Empty => Ok(MultiMember::Empty),
                    MultiMember::Value(index) => map(&self.values[index]).map(MultiMember::Value),
                }))?;
                Self::from_values(axes, members).map_err(E::from)
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
                self.values.heap_bytes() + self.members.heap_bytes()
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
        crate::boundary::try_reserve_vec(&mut points, member_len)?;
        let mut indices = Vec::new();
        let mut sparse = false;
        for member in members {
            match member {
                MultiMember::Empty => {
                    if !sparse {
                        crate::boundary::try_reserve_vec(&mut indices, member_len)?;
                        for index in 0..points.len() {
                            crate::try_push_vec(&mut indices, MultiMember::Value(index))?;
                        }
                        sparse = true;
                    }
                    crate::try_push_vec(&mut indices, MultiMember::Empty)?;
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
                        crate::try_push_vec(&mut indices, MultiMember::Value(points.len()))?;
                    }
                    crate::try_push_vec(&mut points, point)?;
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
                CoordSeq::try_from_points(&points)?
            },
            members,
        })
    }

    pub(crate) fn from_values(
        axes: CoordinateAxes,
        members: Vec<MultiMember<Point>>,
    ) -> Result<Self> {
        Self::try_new(axes, members)
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

    pub(crate) const fn has_empty_members(&self) -> bool {
        self.members.has_empty_members()
    }

    /// Members including EMPTY ones, as `members()` yields them. The
    /// coordinate sequence this type derefs to holds only stored points.
    pub(crate) fn member_count(&self) -> usize {
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

    pub(crate) fn try_to_vec(&self) -> Result<Vec<Point>> {
        let mut points = Vec::new();
        crate::boundary::try_reserve_vec(&mut points, self.coord_count())?;
        for point in self.points() {
            crate::try_push_vec(&mut points, point)?;
        }
        Ok(points)
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
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        let coords = self.coords.try_map_xy_with(op)?;
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
    }

    /// [`Self::try_map_values`] for a map whose callback cannot fail.
    pub(crate) fn map_values(&self, mut map: impl FnMut(Point) -> Point) -> Result<Self> {
        self.try_map_values(|point| Ok::<_, crate::error::Error>(map(point)))
    }

    pub(crate) fn try_map_values<E>(
        &self,
        mut map: impl FnMut(Point) -> Result<Point, E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        if matches!(self.members, MultipartMembers::Dense) {
            let points = crate::try_collect_vec(self.points().map(&mut map))?;
            return Ok(Self {
                coords: CoordSeq::try_from_points(&points).map_err(E::from)?,
                members: MultipartMembers::Dense,
            });
        }
        let members = crate::try_collect_vec(self.members().map(|member| match member {
            MultiMember::Empty => Ok(MultiMember::Empty),
            MultiMember::Value(index) => map(self.coords.point_at(index)).map(MultiMember::Value),
        }))?;
        let axes = members
            .iter()
            .find_map(|member| match member {
                MultiMember::Value(point) => Some(point.axes()),
                MultiMember::Empty => None,
            })
            .unwrap_or_else(|| self.axes());
        Self::from_values(axes, members).map_err(E::from)
    }

    pub(crate) fn quantize(&self, precision: i32) -> Result<Self> {
        let coords = self.coords.quantize(precision)?;
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
    }

    pub(crate) fn force_2d(&self) -> Result<Self> {
        let coords = self.coords.force_2d()?;
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
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

impl crate::HeapSize for MultiPoint {
    fn heap_bytes(&self) -> usize {
        self.coords.heap_bytes() + self.members.heap_bytes()
    }
}

macro_rules! impl_multipart_iteration {
    ($name:ident, $member:ty) => {
        impl $name {
            /// Members including EMPTY ones, as `members()` yields them. The
            /// slice this type derefs to holds only the stored values.
            pub(crate) fn member_count(&self) -> usize {
                self.members.len(self.values.len())
            }

            pub(crate) const fn is_empty(&self) -> bool {
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
            type IntoIter = SharedSliceIter<$member>;

            fn into_iter(self) -> Self::IntoIter {
                self.values.iter_owned()
            }
        }
    };
}

impl_multipart_iteration!(MultiLineString, LineSeq);
impl_multipart_iteration!(MultiPolygon, Polygon);

impl MultiLineString {
    pub(crate) fn as_slice(&self) -> &[LineSeq] {
        &self.values
    }
}

impl MultiPolygon {
    pub(crate) fn first(&self) -> Option<&Polygon> {
        self.iter().next()
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
        self.coords.len()
    }

    fn nth_coord(&self, index: usize) -> Point {
        self.point_at(index)
    }

    fn xy_columns(&self) -> Option<(&[f64], &[f64])> {
        self.coords.xy_columns()
    }

    fn z_column(&self) -> Option<&[f64]> {
        self.coords.z_column()
    }

    fn m_column(&self) -> Option<&[f64]> {
        self.coords.m_column()
    }
}

impl crate::HeapSize for GeometryCollection {
    fn heap_bytes(&self) -> usize {
        self.members.heap_bytes()
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

#[cfg(test)]
mod multipart_layout_tests {
    use super::*;

    fn raw_nested(depth: usize) -> Shape {
        let mut shape = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        for _ in 0..depth {
            shape = Shape::geometry_collection(vec![shape]).expect("test depth is within limit");
        }
        shape
    }

    fn nested_around(mut shape: Shape, depth: usize) -> Shape {
        for _ in 0..depth {
            shape = Shape::geometry_collection(vec![shape]).expect("test depth is within limit");
        }
        shape
    }

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
        assert!(sparse.members.heap_bytes() >= std::mem::size_of_val(&**members));
    }

    #[test]
    fn collection_admission_has_one_inclusive_depth_boundary() {
        assert_eq!(raw_nested(MAX_NESTING).nesting_depth(), MAX_NESTING);
        Shape::geometry_collection(vec![raw_nested(MAX_NESTING - 1)]).unwrap();
        Shape::geometry_collection(vec![raw_nested(MAX_NESTING)]).unwrap_err();
    }

    #[test]
    fn collection_depth_uses_the_deepest_direct_child() {
        let shallow = raw_nested(1);
        let deep = raw_nested(3);
        let collection = Shape::geometry_collection(vec![shallow, deep]).unwrap();
        assert_eq!(collection.nesting_depth(), 4);
    }

    #[test]
    fn structure_preserving_rebuild_recomputes_after_filtering() {
        let source = Shape::geometry_collection(vec![raw_nested(3), Shape::empty_point()]).unwrap();
        let Shape::GeometryCollection(collection) = &source else {
            unreachable!("collection fixture")
        };
        let rebuilt = collection
            .try_rebuild_members(vec![Shape::empty_point()])
            .unwrap();
        assert_eq!(Shape::GeometryCollection(rebuilt).nesting_depth(), 1);
    }

    #[test]
    fn structure_preserving_rebuild_rejects_depth_growth() {
        let source = nested_around(Shape::Point(Point::new_raw_xy(0.0, 0.0)), 2);
        let Shape::GeometryCollection(collection) = &source else {
            unreachable!("collection fixture")
        };
        collection
            .try_map_members(|_| Ok::<_, crate::error::Error>(raw_nested(2)))
            .unwrap_err();
    }
}

#[cfg(test)]
mod mapping_fault_tests {
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::test_support::fail_allocations_after;

    fn two_point_line() -> LineSeq {
        LineSeq::try_new(
            CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 1.0)])
                .unwrap(),
        )
        .unwrap()
    }

    fn two_point_collection() -> GeometryCollection {
        let Shape::GeometryCollection(collection) = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(1.0, 0.0)),
        ])
        .unwrap() else {
            unreachable!("collection fixture")
        };
        collection
    }

    #[test]
    fn try_map_points_returns_resource_on_allocation_failure() {
        let line = two_point_line();
        let error = fail_allocations_after(0, || line.try_map_points(Ok::<_, crate::error::Error>))
            .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "fixture identity selects the second stored point, not a nearby ordinate"
    )]
    fn try_map_members_reports_callback_error_in_member_order() {
        let collection = two_point_collection();
        let mut seen = 0;
        let error = collection
            .try_map_members(|shape| {
                seen += 1;
                match shape {
                    Shape::Point(point) if point.x == 1.0 => {
                        Err(GeometryErrorKind::message("second member"))
                    },
                    _ => Ok(shape.clone()),
                }
            })
            .unwrap_err();
        assert_eq!(seen, 2);
        assert!(error.to_string().contains("second member"));
    }

    #[test]
    fn try_filter_map_owned_members_empty_is_typed_empty_collection() {
        let collection = two_point_collection();
        let filtered = collection
            .try_filter_map_owned_members(|_| Ok(None))
            .unwrap();
        assert!(matches!(
            filtered,
            Shape::Empty(GeometryKind::GeometryCollection, CoordinateAxes::XY)
        ));
    }

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "fixture identity selects the second stored point, not a nearby ordinate"
    )]
    fn try_filter_map_owned_members_preserves_kept_order() {
        let collection = two_point_collection();
        let filtered = collection
            .try_filter_map_owned_members(|shape| {
                Ok(match shape {
                    Shape::Point(point) if point.x == 1.0 => Some(Shape::Point(point)),
                    _ => None,
                })
            })
            .unwrap();
        let Shape::GeometryCollection(collection) = filtered else {
            unreachable!("kept one member")
        };
        assert_eq!(collection.len(), 1);
        assert_eq!(collection.as_slice(), &[Shape::Point(Point::new_raw_xy(
            1.0, 0.0
        ))]);
    }

    #[test]
    fn try_map_values_returns_resource_on_allocation_failure() {
        let points = MultiPoint::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(Point::new_raw_xy(0.0, 0.0)),
            MultiMember::Value(Point::new_raw_xy(1.0, 1.0)),
        ])
        .unwrap();
        let error =
            fail_allocations_after(0, || points.try_map_values(Ok::<_, crate::error::Error>))
                .unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }
}
