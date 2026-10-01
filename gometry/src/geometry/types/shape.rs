use std::collections::HashMap;
use std::marker::PhantomData;
use std::num::NonZeroU8;
use std::ops::Deref;

use crate::HeapSize;
use crate::error::Result;
use crate::geometry::types::{
    CoordIter, CoordSeq, CoordSeqBuilder, CoordWindow, CoordinateAxes, Coordinates,
    GeometryErrorKind, Point, Polygon, SharedSlice, SharedSliceIter, coordseq,
};
use crate::geometry::{Dimension, GeometryKind};

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
    /// direction). One-sided buffers have flat ends and use the requested
    /// ``join_style``, ``quadrant_segments``, and ``miter_limit``.
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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ShapeOwnerKey {
    address: usize,
    len: usize,
    kind: ShapeOwnerKind,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ShapeOwnerKind {
    Collection,
    MultiLineValues,
    MultiPolygonValues,
    PolygonHoles,
}

impl ShapeOwnerKey {
    pub(crate) fn new(members: &[Shape]) -> Self {
        Self::for_slice(ShapeOwnerKind::Collection, members)
    }

    /// Slice windows within one allocation have distinct summaries.
    fn for_slice<T>(kind: ShapeOwnerKind, values: &[T]) -> Self {
        Self {
            address: values.as_ptr().addr(),
            len: values.len(),
            kind,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct ShapeHeapStats {
    coordinate_bytes: usize,
    heap_bytes: usize,
}

#[derive(Default)]
pub(crate) struct ShapeHeapMemo {
    collections: HashMap<ShapeOwnerKey, ShapeHeapStats>,
}

impl ShapeHeapMemo {
    pub(crate) fn heap_bytes(&mut self, shape: &Shape) -> usize {
        self.shape_stats(shape).heap_bytes
    }

    pub(crate) fn coordinate_bytes(&mut self, shape: &Shape) -> usize {
        self.shape_stats(shape).coordinate_bytes
    }

    fn shape_stats(&mut self, shape: &Shape) -> ShapeHeapStats {
        match shape {
            Shape::GeometryCollection(collection) => self.collection_stats(collection),
            Shape::MultiLineString(lines) => ShapeHeapStats {
                coordinate_bytes: lines.iter().fold(0_usize, |bytes, line| {
                    bytes.saturating_add(seq_coordinate_bytes(line.as_coords()))
                }),
                heap_bytes: lines.heap_bytes_saturating(),
            },
            Shape::Polygon(polygon) => ShapeHeapStats {
                coordinate_bytes: polygon.rings().fold(0_usize, |bytes, ring| {
                    bytes.saturating_add(seq_coordinate_bytes(ring.coords()))
                }),
                heap_bytes: polygon.heap_bytes_saturating(),
            },
            Shape::MultiPolygon(polygons) => ShapeHeapStats {
                coordinate_bytes: polygons.iter().fold(0_usize, |bytes, polygon| {
                    polygon.rings().fold(bytes, |bytes, ring| {
                        bytes.saturating_add(seq_coordinate_bytes(ring.coords()))
                    })
                }),
                heap_bytes: polygons.heap_bytes_saturating(),
            },
            Shape::MultiPoint(points) => ShapeHeapStats {
                coordinate_bytes: seq_coordinate_bytes(points),
                heap_bytes: points.heap_bytes_saturating(),
            },
            Shape::LineString(line) => ShapeHeapStats {
                coordinate_bytes: seq_coordinate_bytes(line.as_coords()),
                heap_bytes: line.as_coords().heap_bytes_saturating(),
            },
            Shape::Point(point) => {
                let bytes = CoordinateAxes::from_point(*point).byte_width(1);
                ShapeHeapStats {
                    coordinate_bytes: bytes,
                    heap_bytes: bytes,
                }
            },
            Shape::Empty(..) => ShapeHeapStats::default(),
        }
    }

    fn collection_stats(&mut self, collection: &GeometryCollection) -> ShapeHeapStats {
        let owner = collection
            .members_are_shared()
            .then(|| ShapeOwnerKey::new(collection.as_slice()));
        if let Some(&stats) = owner.as_ref().and_then(|owner| self.collections.get(owner)) {
            return stats;
        }
        let stats = collection.as_slice().iter().fold(
            ShapeHeapStats {
                coordinate_bytes: 0,
                heap_bytes: collection.shallow_heap_bytes(),
            },
            |stats, member| {
                let child = self.shape_stats(member);
                ShapeHeapStats {
                    coordinate_bytes: stats
                        .coordinate_bytes
                        .saturating_add(child.coordinate_bytes),
                    heap_bytes: stats.heap_bytes.saturating_add(child.heap_bytes),
                }
            },
        );
        if let Some(owner) = owner {
            self.collections.insert(owner, stats);
        }
        stats
    }
}

#[derive(Clone, Copy, Debug)]
struct ShapeCountSummary {
    coordinates: Option<usize>,
    segments: Option<usize>,
}

impl Default for ShapeCountSummary {
    fn default() -> Self {
        Self {
            coordinates: Some(0),
            segments: Some(0),
        }
    }
}

impl ShapeCountSummary {
    fn add_assign(&mut self, other: &Self) {
        self.coordinates = self
            .coordinates
            .zip(other.coordinates)
            .and_then(|(left, right)| left.checked_add(right));
        self.segments = self
            .segments
            .zip(other.segments)
            .and_then(|(left, right)| left.checked_add(right));
    }

    fn sequence(sequence: &CoordSeq, lineal: bool) -> Self {
        let coordinates = sequence.len();
        Self {
            coordinates: Some(coordinates),
            segments: Some(if lineal {
                coordinates.saturating_sub(1)
            } else {
                0
            }),
        }
    }
}

/// One operation's exact logical coordinate and segment counts.
///
/// The memo is local to the query. A cached owner summary is added for every
/// reference to that owner, so sharing avoids repeated subtree scans without
/// changing logical multiplicity. Its source lifetime keeps cached slice
/// addresses from being reused while the memo can still be queried.
#[derive(Default)]
pub(crate) struct ShapeCountQuery<'source> {
    owners: Option<HashMap<ShapeOwnerKey, ShapeCountSummary>>,
    segment_presence: Option<HashMap<ShapeOwnerKey, bool>>,
    // Cached slice addresses stay valid only while every borrowed source lives.
    _source: PhantomData<&'source Shape>,
}

impl<'source> ShapeCountQuery<'source> {
    pub(crate) fn coordinates(&mut self, shape: &'source Shape) -> crate::error::Result<usize> {
        self.coordinate_count(shape)
            .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) fn segments(&mut self, shape: &'source Shape) -> crate::error::Result<usize> {
        self.shape_summary(shape)
            .segments
            .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) fn coordinate_count(&mut self, shape: &'source Shape) -> Option<usize> {
        self.shape_summary(shape).coordinates
    }

    pub(crate) fn coordinates_at_most(&mut self, shape: &'source Shape, limit: usize) -> bool {
        self.coordinate_count(shape)
            .is_some_and(|count| count <= limit)
    }

    pub(crate) fn coordinates_for_shapes(
        &mut self,
        shapes: impl IntoIterator<Item = &'source Shape>,
    ) -> crate::error::Result<usize> {
        let mut total = Some(0_usize);
        for shape in shapes {
            let Some(shape_coordinates) = self.shape_summary(shape).coordinates else {
                total = None;
                break;
            };
            total = total.and_then(|left| left.checked_add(shape_coordinates));
            if total.is_none() {
                break;
            }
        }
        total.ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) fn polygon_coordinates(
        &mut self,
        polygon: &'source Polygon,
    ) -> crate::error::Result<usize> {
        self.polygon_summary(polygon)
            .coordinates
            .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }

    pub(crate) fn has_segments(&mut self, shape: &'source Shape) -> bool {
        match shape {
            Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => false,
            Shape::LineString(line) => line.len() > 1,
            Shape::MultiLineString(lines) => self.shared_slice_has_segments(
                ShapeOwnerKind::MultiLineValues,
                lines.values.as_slice(),
                lines.values.is_shared(),
                |_, values| values.iter().any(|line| line.len() > 1),
            ),
            Shape::Polygon(polygon) => self.polygon_has_segments(polygon),
            Shape::MultiPolygon(polygons) => self.shared_slice_has_segments(
                ShapeOwnerKind::MultiPolygonValues,
                polygons.values.as_slice(),
                polygons.values.is_shared(),
                |query, values| {
                    values
                        .iter()
                        .any(|polygon| query.polygon_has_segments(polygon))
                },
            ),
            Shape::GeometryCollection(collection) => self.shared_slice_has_segments(
                ShapeOwnerKind::Collection,
                collection.as_slice(),
                collection.members_are_shared(),
                |query, members| members.iter().any(|member| query.has_segments(member)),
            ),
        }
    }

    fn shared_slice_summary<T>(
        &mut self,
        kind: ShapeOwnerKind,
        values: &'source [T],
        shared: bool,
        summarize: impl FnOnce(&mut Self, &'source [T]) -> ShapeCountSummary,
    ) -> ShapeCountSummary {
        let key = shared.then(|| ShapeOwnerKey::for_slice(kind, values));
        if let Some(summary) = key.as_ref().and_then(|key| self.owners.as_ref()?.get(key)) {
            return *summary;
        }
        let summary = summarize(self, values);
        if let Some(key) = key {
            self.owners
                .get_or_insert_with(HashMap::new)
                .insert(key, summary);
        }
        summary
    }

    fn shared_slice_has_segments<T>(
        &mut self,
        kind: ShapeOwnerKind,
        values: &'source [T],
        shared: bool,
        contains_segments: impl FnOnce(&mut Self, &'source [T]) -> bool,
    ) -> bool {
        let key = shared.then(|| ShapeOwnerKey::for_slice(kind, values));
        if let Some(contains_segments) = key
            .as_ref()
            .and_then(|key| self.segment_presence.as_ref()?.get(key))
        {
            return *contains_segments;
        }
        let contains_segments = contains_segments(self, values);
        if let Some(key) = key {
            self.segment_presence
                .get_or_insert_with(HashMap::new)
                .insert(key, contains_segments);
        }
        contains_segments
    }

    fn polygon_has_segments(&mut self, polygon: &'source Polygon) -> bool {
        polygon.shell.len() > 1
            || self.shared_slice_has_segments(
                ShapeOwnerKind::PolygonHoles,
                polygon.holes.as_slice(),
                polygon.holes.is_shared(),
                |_, holes| holes.iter().any(|hole| hole.len() > 1),
            )
    }

    fn shape_summary(&mut self, shape: &'source Shape) -> ShapeCountSummary {
        match shape {
            Shape::Point(_) => ShapeCountSummary {
                coordinates: Some(1),
                segments: Some(0),
            },
            Shape::MultiPoint(points) => ShapeCountSummary::sequence(&points.coords, false),
            Shape::LineString(line) => ShapeCountSummary::sequence(line.as_coords(), true),
            Shape::MultiLineString(lines) => self.shared_slice_summary(
                ShapeOwnerKind::MultiLineValues,
                lines.values.as_slice(),
                lines.values.is_shared(),
                |_query, values| {
                    values
                        .iter()
                        .fold(ShapeCountSummary::default(), |mut total, line| {
                            total.add_assign(&ShapeCountSummary::sequence(line.as_coords(), true));
                            total
                        })
                },
            ),
            Shape::Polygon(polygon) => self.polygon_summary(polygon),
            Shape::MultiPolygon(polygons) => self.shared_slice_summary(
                ShapeOwnerKind::MultiPolygonValues,
                polygons.values.as_slice(),
                polygons.values.is_shared(),
                |query, values| {
                    values
                        .iter()
                        .fold(ShapeCountSummary::default(), |mut total, polygon| {
                            total.add_assign(&query.polygon_summary(polygon));
                            total
                        })
                },
            ),
            Shape::GeometryCollection(collection) => self.shared_slice_summary(
                ShapeOwnerKind::Collection,
                collection.as_slice(),
                collection.members_are_shared(),
                |query, members| {
                    members
                        .iter()
                        .fold(ShapeCountSummary::default(), |mut total, member| {
                            total.add_assign(&query.shape_summary(member));
                            total
                        })
                },
            ),
            Shape::Empty(..) => ShapeCountSummary::default(),
        }
    }

    fn polygon_summary(&mut self, polygon: &'source Polygon) -> ShapeCountSummary {
        let shell = ShapeCountSummary::sequence(polygon.shell.coords(), true);
        let holes = self.shared_slice_summary(
            ShapeOwnerKind::PolygonHoles,
            polygon.holes.as_slice(),
            polygon.holes.is_shared(),
            |_query, holes| {
                holes
                    .iter()
                    .fold(ShapeCountSummary::default(), |mut total, hole| {
                        total.add_assign(&ShapeCountSummary::sequence(hole.coords(), true));
                        total
                    })
            },
        );
        let mut total = shell;
        total.add_assign(&holes);
        total
    }
}

fn seq_coordinate_bytes(seq: &CoordSeq) -> usize {
    seq.len().saturating_mul(seq.axes().byte_width(1))
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
        ShapeHeapMemo::default().heap_bytes(self)
    }
}

impl Shape {
    pub(crate) fn logical_heap_bytes(&self) -> usize {
        let mut memo = ShapeHeapMemo::default();
        match self {
            Self::LineString(line) => line.logical_heap_bytes_saturating(),
            Self::Polygon(polygon) => polygon.logical_heap_bytes_saturating(),
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::MultiLineString(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => memo.heap_bytes(self),
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

    fn iter_coords(&self) -> impl DoubleEndedIterator<Item = Point> + '_ {
        self.iter()
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
    pub(crate) fn logical_heap_bytes_saturating(&self) -> usize {
        self.as_coords().logical_heap_bytes_saturating()
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

    pub(crate) fn is_shared(&self) -> bool {
        self.0.is_shared()
    }

    fn shallow_heap_bytes(&self) -> usize {
        self.0.shallow_heap_bytes()
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

/// A checked, immutable collection payload carrying its exact nesting depth
/// and topological dimension. The members stay behind this type so no
/// operation can mutate them without rebuilding the enclosing collection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GeometryCollection {
    members: NonEmptyVec<Shape>,
    depth: NonZeroU8,
    topological_dimension: Dimension,
}

impl GeometryCollection {
    fn try_new(parts: Vec<Shape>) -> Result<Self> {
        let members = NonEmptyVec::try_from_vec(parts)?;
        Self::from_members(members, MAX_NESTING)
    }

    fn from_members(members: NonEmptyVec<Shape>, limit: usize) -> Result<Self> {
        let mut child_depth = 0;
        let mut topological_dimension = Dimension::Point;
        for member in &members {
            child_depth = child_depth.max(member.nesting_depth());
            topological_dimension = topological_dimension.max(member.topological_dimension());
        }
        let depth = child_depth.saturating_add(1);
        if depth > MAX_NESTING || depth > limit {
            return Err(collection_depth_error());
        }
        Ok(Self {
            members,
            depth: NonZeroU8::new(depth as u8).expect("collection depth is non-zero"),
            topological_dimension,
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
        let members = map_collection_members(self.members.iter(), &mut map)?;
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
        let members = map_collection_members(self.members.into_iter(), &mut map)?;
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
        for shape in self.members {
            if let Some(mapped) = map(shape)? {
                members.push(mapped);
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

    pub(crate) const fn topological_dimension(&self) -> Dimension {
        self.topological_dimension
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

    pub(crate) fn members_are_shared(&self) -> bool {
        self.members.is_shared()
    }

    fn shallow_heap_bytes(&self) -> usize {
        self.members.shallow_heap_bytes()
    }
}

fn map_collection_members<I, E>(
    members: I,
    mut map: impl FnMut(I::Item) -> std::result::Result<Shape, E>,
) -> std::result::Result<Vec<Shape>, E>
where
    I: ExactSizeIterator,
{
    let mut mapped = Vec::with_capacity(members.len());
    for member in members {
        mapped.push(map(member)?);
    }
    Ok(mapped)
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
    fn try_collect<T, O>(
        members: Vec<MultiMember<T>>,
        make_values: impl FnOnce(usize) -> O,
        mut push_value: impl FnMut(&mut O, T) -> Result<()>,
    ) -> Result<(O, Self)> {
        let present = members
            .iter()
            .filter(|member| matches!(member, MultiMember::Value(_)))
            .count();
        let mut values = make_values(present);
        let mut indices = (present < members.len()).then(|| Vec::with_capacity(members.len()));
        let mut next_value = 0_usize;
        for member in members {
            match member {
                MultiMember::Empty => {
                    if let Some(indices) = indices.as_mut() {
                        indices.push(MultiMember::Empty);
                    }
                },
                MultiMember::Value(value) => {
                    push_value(&mut values, value)?;
                    if let Some(indices) = indices.as_mut() {
                        indices.push(MultiMember::Value(next_value));
                    }
                    next_value = next_value.wrapping_add(1);
                },
            }
        }
        let layout = if let Some(indices) = indices {
            Self::Sparse(SharedSlice::from_vec(indices))
        } else if present == 0 {
            return Err(GeometryErrorKind::EmptyGeometrySequence {
                operation: "geometry payload",
            }
            .into());
        } else {
            Self::Dense
        };
        Ok((values, layout))
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
                let (values, members) = MultipartMembers::try_collect(
                    members,
                    Vec::with_capacity,
                    |values: &mut Vec<$member>, value| {
                        let got = shape_member_axes(&value);
                        if got != axes {
                            return Err(GeometryErrorKind::CoordinateAxesMismatch {
                                declared: axes,
                                got,
                            }
                            .into());
                        }
                        values.push(value);
                        Ok(())
                    },
                )?;
                Ok(Self {
                    axes,
                    values: SharedSlice::from_vec(values),
                    members,
                })
            }

            /// Replace stored values one-for-one while retaining the admitted
            /// member layout, including explicit EMPTY positions.
            pub(crate) fn with_values(&self, values: Vec<$member>) -> Self {
                debug_assert_eq!(values.len(), self.values.len());
                debug_assert!(
                    values
                        .iter()
                        .all(|value| shape_member_axes(value) == self.axes)
                );
                Self {
                    axes: self.axes,
                    values: SharedSlice::from_vec(values),
                    members: self.members.clone(),
                }
            }

            /// A dense multipart from members that are all present, declaring
            /// the axes of the first. Axis-homogeneity is a payload invariant
            /// (the DbC boundary rule [`CoordSeq::try_from_points`] follows);
            /// external members are admitted through [`Self::try_new`], which
            /// checks. Returns `None` for an empty payload — that is
            /// `Shape::Empty`, not a multipart with no members.
            pub(crate) fn from_dense(values: Vec<$member>) -> Option<Self> {
                let first = values.first()?;
                let axes = shape_member_axes(first);
                debug_assert!(
                    values.iter().all(|value| shape_member_axes(value) == axes),
                    "from_dense requires an axis-homogeneous payload",
                );
                Some(Self {
                    axes,
                    values: SharedSlice::from_vec(values),
                    members: MultipartMembers::Dense,
                })
            }

            pub(crate) const fn axes(&self) -> CoordinateAxes {
                self.axes
            }

            pub(crate) fn members(&self) -> MultipartMemberIter<'_> {
                self.members.iter(self.values.len())
            }

            /// Borrow members in logical order, resolving admitted value
            /// indices at the multipart layout owner.
            pub(crate) fn members_with_values<'a>(
                &'a self,
            ) -> impl Iterator<Item = MultiMember<&'a $member>> + 'a {
                self.members
                    .iter(self.values.len())
                    .map(|member| match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(index) => MultiMember::Value(
                            self.values
                                .as_slice()
                                .get(index)
                                .expect("multipart value indices are admitted with their layout"),
                        ),
                    })
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
                let mut values = Vec::with_capacity(self.values.len());
                for value in self.values.iter() {
                    values.push(map(value)?);
                }
                self.try_with_values(axes, values).map_err(E::from)
            }

            pub(crate) fn try_with_values(
                &self,
                axes: CoordinateAxes,
                values: Vec<$member>,
            ) -> Result<Self> {
                debug_assert_eq!(values.len(), self.values.len());
                if let Some(value) = values
                    .iter()
                    .find(|value| shape_member_axes(*value) != axes)
                {
                    return Err(GeometryErrorKind::CoordinateAxesMismatch {
                        declared: axes,
                        got: shape_member_axes(value),
                    }
                    .into());
                }
                Ok(Self {
                    axes,
                    values: SharedSlice::from_vec(values),
                    members: self.members.clone(),
                })
            }

            pub(crate) fn with_axes(&self, axes: CoordinateAxes) -> Self {
                Self {
                    axes,
                    values: self.values.clone(),
                    members: self.members.clone(),
                }
            }

            pub(crate) fn heap_bytes_saturating(&self) -> usize {
                self.values
                    .heap_bytes_with(HeapSize::heap_bytes)
                    .saturating_add(self.members.heap_bytes())
            }
        }

        impl crate::HeapSize for $name {
            fn heap_bytes(&self) -> usize {
                self.heap_bytes_saturating()
            }
        }
    };
}

impl_multipart!(MultiLineString, LineSeq);
impl_multipart!(MultiPolygon, Polygon);

impl MultiPoint {
    pub(crate) fn try_new(axes: CoordinateAxes, members: Vec<MultiMember<Point>>) -> Result<Self> {
        let (points, members) = MultipartMembers::try_collect(
            members,
            |present| CoordSeqBuilder::with_capacity(axes, present),
            |points, point| {
                if point.axes() != axes {
                    return Err(GeometryErrorKind::CoordinateAxesMismatch {
                        declared: axes,
                        got: point.axes(),
                    }
                    .into());
                }
                points.push(point);
                Ok(())
            },
        )?;
        Ok(Self {
            coords: points.finish()?,
            members,
        })
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

    pub(crate) fn to_vec(&self) -> Vec<Point> {
        let mut points = Vec::with_capacity(self.coord_count());
        points.extend(self.points());
        points
    }

    pub(crate) fn with_values(&self, values: &[Point]) -> Result<Self> {
        debug_assert_eq!(values.len(), self.coords.len());
        let coords = if values.is_empty() {
            CoordSeq::empty(self.axes())
        } else {
            CoordSeq::try_from_points(values)?
        };
        Ok(Self {
            coords,
            members: self.members.clone(),
        })
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
        map: impl FnMut(Point) -> Result<Point, E>,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        Ok(Self {
            coords: self.coords.try_map_points(map)?,
            members: self.members.clone(),
        })
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
        self.heap_bytes_saturating()
    }
}

impl MultiPoint {
    pub(crate) fn heap_bytes_saturating(&self) -> usize {
        self.coords
            .heap_bytes_saturating()
            .saturating_add(self.members.heap_bytes())
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

    fn iter_coords(&self) -> impl DoubleEndedIterator<Item = Point> + '_ {
        self.coords.iter()
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
        ShapeHeapMemo::default().collection_stats(self).heap_bytes
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

    fn xyz_point(x: f64, y: f64, z: f64) -> Point {
        Point::new_raw_axes(
            x,
            y,
            crate::geometry::ZOrdinate(Some(z)),
            crate::geometry::MOrdinate(None),
        )
    }

    fn xyz_line() -> LineSeq {
        let points = [xyz_point(0.0, 0.0, 1.0), xyz_point(1.0, 1.0, 2.0)];
        LineSeq::try_new(CoordSeq::try_from_points(&points).expect("XYZ points"))
            .expect("valid line")
    }

    fn xyz_polygon() -> Polygon {
        let points = [
            xyz_point(0.0, 0.0, 1.0),
            xyz_point(1.0, 0.0, 2.0),
            xyz_point(0.0, 1.0, 3.0),
            xyz_point(0.0, 0.0, 1.0),
        ];
        let shell = crate::geometry::Ring::try_from_closed(
            CoordSeq::try_from_points(&points).expect("XYZ ring coordinates"),
        )
        .expect("closed ring");
        Polygon::new(shell, Vec::new())
    }

    #[test]
    fn dense_shapes_infer_member_axes_and_keep_declared_empty_axes() {
        let Shape::MultiLineString(lines) =
            Shape::multi_line_string(CoordinateAxes::XY, vec![xyz_line()])
        else {
            unreachable!("nonempty dense line payload");
        };
        assert_eq!(lines.axes(), CoordinateAxes::XYZ);
        assert_eq!(
            Shape::multi_line_string(CoordinateAxes::XYZM, Vec::new()),
            Shape::typed_empty(GeometryKind::MultiLineString, CoordinateAxes::XYZM)
        );
        assert_eq!(
            Shape::narrow_lines(CoordinateAxes::XYZM, Vec::new()),
            Shape::typed_empty(GeometryKind::LineString, CoordinateAxes::XYZM)
        );

        let Shape::MultiPolygon(polygons) =
            Shape::multi_polygon(CoordinateAxes::XY, vec![xyz_polygon()])
        else {
            unreachable!("nonempty dense polygon payload");
        };
        assert_eq!(polygons.axes(), CoordinateAxes::XYZ);
        assert_eq!(
            Shape::multi_polygon(CoordinateAxes::XYZM, Vec::new()),
            Shape::typed_empty(GeometryKind::MultiPolygon, CoordinateAxes::XYZM)
        );
        assert_eq!(
            Shape::narrow_polygons(CoordinateAxes::XYZM, Vec::new()),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XYZM)
        );
    }

    #[test]
    fn sparse_shapes_keep_empty_positions_and_member_admission_errors() {
        let lines = MultiLineString::try_new(CoordinateAxes::XYZ, vec![
            MultiMember::Empty,
            MultiMember::Value(xyz_line()),
            MultiMember::Empty,
        ])
        .expect("sparse lines");
        assert!(matches!(&lines.members, MultipartMembers::Sparse(_)));
        assert_eq!(lines.members().collect::<Vec<_>>(), [
            MultiMember::Empty,
            MultiMember::Value(0),
            MultiMember::Empty,
        ]);

        let polygons = MultiPolygon::try_new(CoordinateAxes::XYZ, vec![
            MultiMember::Empty,
            MultiMember::Value(xyz_polygon()),
            MultiMember::Empty,
        ])
        .expect("sparse polygons");
        assert!(matches!(&polygons.members, MultipartMembers::Sparse(_)));
        assert_eq!(polygons.members().collect::<Vec<_>>(), [
            MultiMember::Empty,
            MultiMember::Value(0),
            MultiMember::Empty,
        ]);

        let error =
            MultiLineString::try_new(CoordinateAxes::XY, vec![MultiMember::Value(xyz_line())])
                .expect_err("axes mismatch is still admitted as an error");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::CoordinateAxesMismatch {
                declared: CoordinateAxes::XY,
                got: CoordinateAxes::XYZ,
            })
        ));

        let error = MultiPolygon::try_new(CoordinateAxes::XYZ, Vec::new())
            .expect_err("a multipart without any members is not admitted");
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::EmptyGeometrySequence {
                operation: "geometry payload",
            })
        ));
    }

    #[test]
    fn dense_and_sparse_members_are_canonical() {
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
    fn sparse_value_mapping_reuses_member_layout() {
        let line = LineSeq::segment(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(1.0, 1.0))
            .expect("finite line");
        let source = MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(line.clone()),
            MultiMember::Empty,
            MultiMember::Value(line),
        ])
        .expect("sparse lines");
        let mut mapped_count = 0;
        let mapped = source
            .try_map_values(CoordinateAxes::XY, |line| {
                mapped_count += 1;
                Ok::<_, crate::error::Error>(line.clone())
            })
            .expect("identity mapping");
        assert_eq!(mapped_count, 2);
        assert_eq!(
            mapped.members().collect::<Vec<_>>(),
            source.members().collect::<Vec<_>>()
        );
        let (MultipartMembers::Sparse(source_layout), MultipartMembers::Sparse(mapped_layout)) =
            (&source.members, &mapped.members)
        else {
            unreachable!("sparse mapping retains member positions");
        };
        assert!(std::ptr::eq(
            source_layout.as_slice().as_ptr(),
            mapped_layout.as_slice().as_ptr()
        ));
    }

    #[test]
    fn sparse_point_and_line_maps_keep_member_layout_when_axes_change() {
        let point = Point::new_raw_xy(1.0, 2.0);
        let source_points = MultiPoint::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(point),
        ])
        .unwrap();
        let mapped_points = source_points
            .try_map_values(|point| {
                Ok::<_, crate::error::Error>(Point::new_raw_axes(
                    point.x,
                    point.y,
                    crate::geometry::ZOrdinate(Some(3.0)),
                    crate::geometry::MOrdinate(None),
                ))
            })
            .unwrap();
        assert_eq!(mapped_points.axes(), CoordinateAxes::XYZ);
        let (MultipartMembers::Sparse(before_points), MultipartMembers::Sparse(after_points)) =
            (&source_points.members, &mapped_points.members)
        else {
            unreachable!("point mapping keeps sparse positions");
        };
        assert!(std::ptr::eq(
            before_points.as_slice().as_ptr(),
            after_points.as_slice().as_ptr()
        ));

        let line = LineSeq::segment(point, Point::new_raw_xy(2.0, 3.0)).unwrap();
        let source_lines = MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Empty,
            MultiMember::Value(line),
        ])
        .unwrap();
        let mapped_lines = Shape::MultiLineString(source_lines.clone())
            .map_points(&|point| {
                Ok(Point::new_raw_axes(
                    point.x,
                    point.y,
                    crate::geometry::ZOrdinate(Some(3.0)),
                    crate::geometry::MOrdinate(None),
                ))
            })
            .unwrap();
        let Shape::MultiLineString(mapped_lines) = mapped_lines else {
            unreachable!("point mapping keeps multiline shape");
        };
        assert_eq!(mapped_lines.axes(), CoordinateAxes::XYZ);
        let (MultipartMembers::Sparse(before_lines), MultipartMembers::Sparse(after_lines)) =
            (&source_lines.members, &mapped_lines.members)
        else {
            unreachable!("line mapping keeps sparse positions");
        };
        assert!(std::ptr::eq(
            before_lines.as_slice().as_ptr(),
            after_lines.as_slice().as_ptr()
        ));
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
    fn collection_topological_dimension_tracks_members_and_rebuilds() {
        let curve = Shape::geometry_collection(vec![Shape::typed_empty(
            GeometryKind::LineString,
            CoordinateAxes::XY,
        )])
        .unwrap();
        let source = Shape::geometry_collection(vec![
            Shape::empty_point(),
            curve.clone(),
            Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XY),
        ])
        .unwrap();
        assert_eq!(source.topological_dimension(), Dimension::Surface);

        let Shape::GeometryCollection(collection) = source else {
            unreachable!("collection fixture")
        };
        let rebuilt = collection.try_rebuild_members(vec![curve]).unwrap();
        assert_eq!(
            Shape::GeometryCollection(rebuilt).topological_dimension(),
            Dimension::Curve
        );
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
mod mapping_error_tests {
    use super::*;

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
                if let Shape::Point(point) = shape
                    && point.x == 1.0
                {
                    Err(GeometryErrorKind::message("second member"))
                } else {
                    Ok(shape.clone())
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
                Ok(matches!(&shape, Shape::Point(point) if point.x == 1.0).then_some(shape))
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
}
