#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use super::*;
use crate::geometry::{
    FacetBvh, GeodesicFacetBvh, GeodesicSweepCaps, PointSetIndex, PreparedLinework,
    xy_bounds_columns,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Polygon {
    pub shell: Ring,
    pub holes: Arc<[Ring]>,
}

impl AsRef<Self> for Polygon {
    fn as_ref(&self) -> &Self {
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Point {
    pub x: f64,
    pub y: f64,
    pub(crate) z: f64,
    pub(crate) m: f64,
    pub(crate) axes: CoordinateAxes,
}

const _: () = assert!(std::mem::size_of::<Point>() <= 40);

// Structural identity for `Point`: same coordinate axes and the same *active*
// ordinates compared by bit pattern (so NaN is deterministic and inactive Z/M
// sentinels never differ). `Eq`/`Hash` are kept consistent for value semantics
// (`Geometry.__eq__`/`__hash__`, dedup, dict keys).
impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        self.axes == other.axes
            && self.x.to_bits() == other.x.to_bits()
            && self.y.to_bits() == other.y.to_bits()
            && self.z().map(f64::to_bits) == other.z().map(f64::to_bits)
            && self.m().map(f64::to_bits) == other.m().map(f64::to_bits)
    }
}

impl Eq for Point {}

impl std::hash::Hash for Point {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.axes.bits().hash(state);
        self.x.to_bits().hash(state);
        self.y.to_bits().hash(state);
        self.z().map(f64::to_bits).hash(state);
        self.m().map(f64::to_bits).hash(state);
    }
}

/// Whether a coordinate has a Z ordinate.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub(crate) struct HasZ(pub bool);

const _: () = assert!(size_of::<HasZ>() == size_of::<bool>());

/// Whether a coordinate has an M ordinate.
#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub(crate) struct HasM(pub bool);

const _: () = assert!(size_of::<HasM>() == size_of::<bool>());

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub(crate) struct ZOrdinate(pub Option<f64>);

const _: () = assert!(size_of::<ZOrdinate>() == size_of::<Option<f64>>());

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
pub(crate) struct MOrdinate(pub Option<f64>);

const _: () = assert!(size_of::<MOrdinate>() == size_of::<Option<f64>>());

/// Which ordinates a coordinate carries.
///
/// The single source of truth for the `XY`/`XYZ`/`XYM`/`XYZM` dimensionality,
/// shared by geometry, WKT/WKB, and Arrow instead of stringly `&str` tags.
/// A `u8` bitflag (Z=1, M=2).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct CoordinateAxes(u8);

const _: () = assert!(size_of::<CoordinateAxes>() == size_of::<u8>());

impl CoordinateAxes {
    pub const XY: Self = Self(0);
    pub const XYZ: Self = Self(Self::Z);
    pub const XYM: Self = Self(Self::M);
    pub const XYZM: Self = Self(Self::Z | Self::M);
    const Z: u8 = 1;
    const M: u8 = 2;

    pub(crate) const fn new(z: HasZ, m: HasM) -> Self {
        let bits = (z.0 as u8 * Self::Z) | (m.0 as u8 * Self::M);
        match Self::from_bits(bits) {
            Some(axes) => axes,
            None => panic!("invalid coordinate axes bits"),
        }
    }

    pub(crate) const fn from_bits(bits: u8) -> Option<Self> {
        if bits & !(Self::Z | Self::M) == 0 {
            Some(Self(bits))
        } else {
            None
        }
    }

    pub(crate) const fn bits(self) -> u8 {
        self.0
    }

    pub const fn has_z(self) -> bool {
        self.0 & Self::Z != 0
    }

    pub const fn has_m(self) -> bool {
        self.0 & Self::M != 0
    }

    /// Number of ordinates per coordinate (2 for XY … 4 for XYZM).
    pub const fn dimension(self) -> usize {
        2 + self.has_z() as usize + self.has_m() as usize
    }

    /// Raw byte width for `count` coordinates stored as `f64` ordinate columns.
    pub const fn byte_width(self, count: usize) -> usize {
        count * std::mem::size_of::<f64>() * self.dimension()
    }

    /// Canonical tag: `"XY"`, `"XYZ"`, `"XYM"`, or `"XYZM"`.
    pub const fn as_str(self) -> &'static str {
        match (self.has_z(), self.has_m()) {
            (false, false) => "XY",
            (true, false) => "XYZ",
            (false, true) => "XYM",
            (true, true) => "XYZM",
        }
    }

    /// WKT dimensionality tag including the leading space: `""`, `" Z"`, `"
    /// M"`, or `" ZM"` (e.g. `POINT Z (...)`).
    pub const fn wkt_tag(self) -> &'static str {
        match (self.has_z(), self.has_m()) {
            (false, false) => "",
            (true, false) => " Z",
            (false, true) => " M",
            (true, true) => " ZM",
        }
    }

    /// The axes for a single coordinate.
    pub const fn from_point(point: Point) -> Self {
        point.axes
    }

    /// Union of two layouts — Z/M present if present in either. Used to fold
    /// the shared axes of a multi-part / mixed coordinate view.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bounds {
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
}

impl Bounds {
    /// Build bounds without validating ordering or finiteness. Use at audited
    /// reducer and antimeridian call sites where `minx > maxx` can be
    /// intentional.
    pub const fn new_unchecked(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Self {
        Self {
            minx,
            miny,
            maxx,
            maxy,
        }
    }

    pub const fn minx(self) -> f64 {
        self.minx
    }

    pub const fn miny(self) -> f64 {
        self.miny
    }

    pub const fn maxx(self) -> f64 {
        self.maxx
    }

    pub const fn maxy(self) -> f64 {
        self.maxy
    }

    pub const fn include_xy(&mut self, point: XY) {
        self.minx = self.minx().min(point.x);
        self.miny = self.miny().min(point.y);
        self.maxx = self.maxx().max(point.x);
        self.maxy = self.maxy().max(point.y);
    }

    /// Planar bounds folded straight off the `x`/`y` columns via the wide-lane
    /// [`column_minmax`] kernel. With `SoA` storage each ordinate column is
    /// contiguous, so the two `minpd`/`maxpd` folds need no per-vertex `Point`
    /// gather. Separate passes over `xs` and `ys` (rather than one interleaved
    /// `zip`) keep each fold's live state small and let LLVM vectorize cleanly.
    pub fn from_coords(coords: &CoordSeq) -> Option<Self> {
        if coords.is_empty() {
            return None;
        }
        let [minx, miny, maxx, maxy] = xy_bounds_columns(coords.xs(), coords.ys());
        Some(Self::new_unchecked(minx, miny, maxx, maxy))
    }

    pub const fn include_bounds(&mut self, other: Self) {
        self.minx = self.minx().min(other.minx());
        self.miny = self.miny().min(other.miny());
        self.maxx = self.maxx().max(other.maxx());
        self.maxy = self.maxy().max(other.maxy());
    }
}

/// Planar engine coordinate — the XY-only working currency of the
/// noding/arrangement/sweep kernels.
///
/// 16 bytes (2.5x the cache density of the ordinate-carrying [`Point`])
/// and the type-level statement that a kernel cannot depend on Z/M.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct XY {
    pub x: f64,
    pub y: f64,
}

impl XY {
    pub const fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    /// Lift back into the ordinate-carrying interchange (XY axes).
    pub const fn point(self) -> Point {
        Point::new_unchecked_xy(self.x, self.y)
    }
}

impl From<Point> for XY {
    fn from(point: Point) -> Self {
        Self {
            x: point.x,
            y: point.y,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Segment {
    pub start: XY,
    pub end: XY,
}

pub(crate) const fn segment_midpoint_xy(start: XY, end: XY) -> XY {
    XY::new(f64::midpoint(start.x, end.x), f64::midpoint(start.y, end.y))
}

pub(crate) const fn segment_midpoint(segment: Segment) -> Point {
    segment_midpoint_xy(segment.start, segment.end).point()
}

/// Reusable distance/dwithin working set for one shape.
///
/// Its packed linework (facet columns, which double as the vertex probe
/// source) and isolated (0-dimensional) points. Building it once lets a
/// scalar-vs-array distance reuse the fixed operand's set across every
/// element rather than rebuilding per pair (the expensive part of a large
/// fixed operand).
pub struct DistanceParts {
    pub point_only: Vec<Point>,
    /// The shape's segments flattened to packed XY columns + facet partition
    /// — what every distance phase (brute scans and BVH traversals) reads.
    pub(crate) linework: PreparedLinework,
    /// Whether every coordinate keeps squared-space kernels finite — a pure
    /// function of the immutable data, computed once on first DISTANCE use (two
    /// full column scans). LAZY because the CONTACT path (overlaps/touches/…)
    /// reads only `linework` and never needs it, so it must not tax that build.
    pub(crate) squared_safe: std::sync::OnceLock<bool>,
    /// Lazily-built facet BVH over `linework` (`None` below the build
    /// crossover) — shared by the vertex- and segment-phase branch-and-bound
    /// traversals and amortized across a scalar-vs-array batch.
    pub(crate) facet_bvh: std::sync::OnceLock<Option<FacetBvh>>,
    /// Lazily-built R-tree over `point_only` (`None` below the build crossover)
    /// — turns the nearest-points point-vs-point phase from an O(probe·points)
    /// brute scan into O(probe·log points) for point-heavy operands (multipoint
    /// targets), amortized across a scalar-vs-array batch.
    pub(crate) point_index: std::sync::OnceLock<Option<PointSetIndex>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::geometry) struct GeodesicPartsKey {
    crs: Box<str>,
    semi_major_bits: u64,
    flattening_bits: u64,
    runtime_generation: u64,
}

impl GeodesicPartsKey {
    pub(in crate::geometry) fn same_frame_parameters(&self, other: &Self) -> bool {
        self.crs == other.crs
            && self.semi_major_bits == other.semi_major_bits
            && self.flattening_bits == other.flattening_bits
    }

    pub(in crate::geometry) const fn runtime_generation(&self) -> u64 {
        self.runtime_generation
    }

    pub(in crate::geometry) fn new(crs: &str, semi_major: f64, flattening: f64) -> Self {
        Self::new_at_generation(
            crs,
            semi_major,
            flattening,
            crate::crs::runtime_config_generation(),
        )
    }

    pub(in crate::geometry) fn new_at_generation(
        crs: &str,
        semi_major: f64,
        flattening: f64,
        runtime_generation: u64,
    ) -> Self {
        Self {
            crs: crs.into(),
            semi_major_bits: semi_major.to_bits(),
            flattening_bits: flattening.to_bits(),
            runtime_generation,
        }
    }
}

impl crate::HeapSize for GeodesicPartsKey {
    fn heap_bytes(&self) -> usize {
        self.crs.len()
    }
}

/// Reusable geodesic distance/dwithin/nearest working set for one shape and
/// one ellipsoid.
pub(crate) struct GeodesicParts {
    pub(in crate::geometry) points: Box<[Point]>,
    /// Full-ordinate segments, so nearest witnesses preserve Z/M exactly.
    pub(in crate::geometry) segments: Box<[GeodesicSegment]>,
    pub(in crate::geometry) point_only: Box<[Point]>,
    pub(in crate::geometry) antimeridian_segments: Box<[GeodesicSegment]>,
    pub(in crate::geometry) caps: Option<GeodesicSweepCaps>,
    pub(in crate::geometry) facet_bvh: std::sync::OnceLock<Option<GeodesicFacetBvh>>,
}

#[derive(Clone, Copy, Debug)]
pub struct GeodesicSegment {
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) length: f64,
    /// Forward azimuth at `start` (= `azi1` of inverse(start, end)).
    pub(crate) azimuth0: f64,
    /// Forward azimuth at `end` (= `azi2` of inverse(start, end)).
    pub(crate) azimuth1: f64,
}

crate::heapless!(Point, Bounds, XY, Segment, GeodesicSegment);
