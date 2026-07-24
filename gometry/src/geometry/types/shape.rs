use std::ops::Deref;

use super::*;
use crate::HeapSize;
use crate::error::Result;
use crate::geometry::Dimension;

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
    MultiPoint(CoordSeq),
    LineString(LineSeq),
    MultiLineString(Vec<LineSeq>),
    Polygon(Polygon),
    MultiPolygon(Vec<Polygon>),
    GeometryCollection(Vec<Self>),
    /// A typed empty geometry carrying its declared coordinate axes, so
    /// `POINT Z EMPTY` round-trips distinct from `POINT EMPTY`.
    ///
    /// `Point` and `Polygon` empties ALWAYS take this variant (their data
    /// variants cannot express emptiness). `MultiPoint` and `LineString`
    /// empties never do — their `CoordSeq`/`LineSeq` carries axes even at
    /// zero length. The container kinds (`MultiLineString`, `MultiPolygon`,
    /// `GeometryCollection`) use this variant ONLY for non-XY axes; their XY
    /// empty stays the canonical empty `Vec` form. Construct through
    /// [`Shape::typed_empty`], which upholds that normalization.
    Empty(EmptyKind, CoordinateAxes),
}

impl HeapSize for Shape {
    fn heap_bytes(&self) -> usize {
        self.coordinate_bytes()
    }
}

/// Coordinate sequence carried by lineal shapes. A `LineSeq` is either empty
/// or has at least two vertices, so a one-vertex `LineString` is
/// unrepresentable. Use [`LineSeq::try_new`] at construction boundaries and
/// [`LineSeq::from_trusted`] only for audited transforms that preserve the
/// source line's vertex count.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LineSeq(CoordSeq);

impl LineSeq {
    pub(crate) fn try_new(coords: CoordSeq) -> Result<Self> {
        if coords.len() == 1 {
            return Err(GeometryErrorKind::LineStringTooShort.into());
        }
        Ok(Self(coords))
    }

    pub(crate) fn empty(axes: CoordinateAxes) -> Self {
        Self(CoordSeq::empty(axes))
    }

    pub(crate) fn from_trusted(coords: CoordSeq) -> Self {
        assert!(
            coords.len() != 1,
            "trusted LineSeq construction received a one-vertex coordinate sequence"
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

/// The geometry type of a typed empty ([`Shape::Empty`]): the kinds whose
/// data variant cannot carry coordinate axes when empty. `MultiPoint` and
/// `LineString` are absent by design — their coordinate sequences carry axes
/// at zero length, so they never need the typed-empty variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EmptyKind {
    Point,
    Polygon,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

impl EmptyKind {
    /// WKT/Python type name for this empty geometry.
    pub const fn geometry_type(self) -> &'static str {
        match self {
            Self::Point => "Point",
            Self::Polygon => "Polygon",
            Self::MultiLineString => "MultiLineString",
            Self::MultiPolygon => "MultiPolygon",
            Self::GeometryCollection => "GeometryCollection",
        }
    }

    /// Topological dimension of the declared kind. An empty collection ranks
    /// as `Point`, matching the empty `GeometryCollection` container form
    /// (`max` over no members falls back to `Point`).
    pub const fn topological_dimension(self) -> Dimension {
        match self {
            Self::Point | Self::GeometryCollection => Dimension::Point,
            Self::MultiLineString => Dimension::Curve,
            Self::Polygon | Self::MultiPolygon => Dimension::Surface,
        }
    }
}
