mod collection;
mod count;
mod line;
mod multipart;
mod ownership;
pub(crate) use collection::GeometryCollection;
pub(crate) use count::ShapeCountQuery;
pub(crate) use line::LineSeq;
use multipart::MultipartMembers;
pub(crate) use ownership::{ShapeHeapMemo, ShapeOwnerKey};
#[cfg(test)]
mod tests;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MultiMember<T> {
    Empty,
    Value(T),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MultiPoint {
    coords: CoordSeq,
    members: MultipartMembers,
}

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
