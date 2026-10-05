use core::fmt;
use core::ops::Deref;

use crate::{LatLng, Resolution};

/// Maximum number of cell boundary vertices.
///
/// Worst case is pentagon: 5 original verts + 5 edge crossings.
const MAX_BNDRY_VERTS: usize = 10;

/// Exact doubled native-plane coordinates.
///
/// `x = x2 / 2` and `y = y_sqrt3 * SQRT3_2`.  The sidecar carrier keeps the
/// H3 lattice data which produced a boundary point; deriving it back from a
/// rounded [`LatLng`] would move the supporting great-circle plane.
// gometry: exact doubled lattice carrier for certified projection and cap-domain proofs.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Hex2Int {
    /// Twice the native x coordinate.
    pub x2: i64,
    /// Native y coefficient of the exact binary `SQRT3_2` literal.
    pub y_sqrt3: i64,
}

impl Hex2Int {
    /// Creates a native doubled coordinate.
    #[must_use]
    pub const fn new(x2: i64, y_sqrt3: i64) -> Self {
        Self { x2, y_sqrt3 }
    }

    /// Returns the normalized `i + j + k` coordinate sum represented by this
    /// doubled native point.  The inverse is exact only when both recovered
    /// numerators are even; malformed sidecars are rejected rather than
    /// rounded into a face-domain certificate.
    #[must_use]
    pub fn normalized_ijk_sum(self) -> Option<i64> {
        let x2 = i128::from(self.x2);
        let y3 = i128::from(self.y_sqrt3);
        let first = x2.checked_sub(3_i128.checked_mul(y3)?)?;
        let second = x2.checked_add(3_i128.checked_mul(y3)?)?;
        if first % 2 != 0 || second % 2 != 0 {
            return None;
        }
        let maximum = (-x2).max(first / 2).max(second / 2).max(0);
        i64::try_from(maximum).ok()
    }
}

/// Raw projection input for a public boundary point.
// gometry: raw boundary seed carrier; public LatLng cannot preserve its plane.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(
    variant_size_differences,
    reason = "intersection seeds carry two exact native segments; fixed boundary sidecar avoids allocation"
)]
#[expect(
    clippy::exhaustive_enums,
    reason = "gometry's hidden projection carrier ABI matches both proof domains exhaustively"
)]
pub enum ProjectionSeed {
    /// A native lattice point on one icosahedral face.
    Point {
        /// Compact face id.
        face: u8,
        /// Native lattice point.
        point: Hex2Int,
        /// Extended (possibly substrate) resolution.
        resolution: u8,
        /// Whether the point is in the substrate lattice.
        substrate: bool,
    },
    /// Exact rational intersection of two native lattice segments.
    Intersection {
        /// Compact face id.
        face: u8,
        /// Source lattice segment.
        segment: [Hex2Int; 2],
        /// Icosahedron face-edge lattice segment.
        face_edge: [Hex2Int; 2],
        /// Extended (possibly substrate) resolution.
        resolution: u8,
        /// Whether the point is in the substrate lattice.
        substrate: bool,
    },
}

/// Native constants and face data required to replay one H3 raw projection.
///
/// This is deliberately a value snapshot rather than a reference into the
/// vendor tables: gometry may consume a compact [`ProjectionSeed`] after the
/// boundary call has returned, while the canonical tables remain owned here.
// gometry: canonical raw face frame for outward seed replay.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProjectionFrame {
    /// Compact icosahedron face id.
    pub face: u8,
    /// Raw face-centre n-vector bits, not re-normalized.
    pub center: [f64; 3],
    /// Native Class-II axis azimuth in radians.
    pub axis_az_rads_cii: f64,
    /// Native inverse square-seven scale for this extended resolution.
    pub inv_sqrt7: f64,
    /// Resolution-zero gnomonic scale.
    pub res0_u_gnomonic: f64,
    /// Exact binary native one-third substrate scale.
    pub one_third: f64,
    /// Native Class-III rotation.
    pub ap7_rot_rads: f64,
    /// Exact binary native square-root-three-over-two literal.
    pub sqrt3_2: f64,
}

/// One directed icosahedral face seam in both native projection frames.
///
/// The endpoints are raw substrate-lattice seeds rather than rounded
/// latitude/longitude values.  Consumers compare the two unnormalised
/// projection carriers before the chart-normalisation step.
// gometry: paired directed seams for the normalization discrepancy bound.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionSeam {
    left: [ProjectionSeed; 2],
    right: [ProjectionSeed; 2],
}

impl ProjectionSeam {
    pub(crate) const fn new(left: [ProjectionSeed; 2], right: [ProjectionSeed; 2]) -> Self {
        Self { left, right }
    }

    /// Returns the two seam endpoints in the source face's native frame.
    #[must_use]
    pub const fn left(self) -> [ProjectionSeed; 2] {
        self.left
    }

    /// Returns the same physical endpoints in the adjacent native frame.
    #[must_use]
    pub const fn right(self) -> [ProjectionSeed; 2] {
        self.right
    }
}

/// Finite target-resolution lattice data used to bound all descendants.
///
/// The profile contains raw native coordinates only.  It deliberately does
/// not turn any lattice norm, projection, or seam discrepancy into a rounded
/// scalar: the consuming certified layer encloses those operations outward.
// gometry: finite target lattice profile for descendant-cap certification.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapLatticeProfile {
    target: Resolution,
    adjusted_resolution: u8,
    max_dim: i64,
    suffixes: [[Hex2Int; 42]; 15],
    vertices: [Hex2Int; 36],
    seams: [ProjectionSeam; 60],
}

impl CapLatticeProfile {
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "The profile owns these generated tables; borrowing would require copies to retain them"
    )]
    pub(crate) const fn new(
        target: Resolution,
        adjusted_resolution: u8,
        max_dim: i64,
        suffixes: [[Hex2Int; 42]; 15],
        vertices: [Hex2Int; 36],
        seams: [ProjectionSeam; 60],
    ) -> Self {
        Self {
            target,
            adjusted_resolution,
            max_dim,
            suffixes,
            vertices,
            seams,
        }
    }

    /// Builds the exact finite lattice profile for one H3 target resolution.
    #[must_use]
    pub fn for_target(target: Resolution) -> Self {
        crate::coord::cap_lattice_profile(target)
    }

    /// Returns the requested H3 target resolution.
    #[must_use]
    pub const fn target(&self) -> Resolution {
        self.target
    }

    /// Returns the Class-II adjusted substrate resolution (0 through 16).
    #[must_use]
    pub const fn adjusted_resolution(&self) -> u8 {
        self.adjusted_resolution
    }

    /// Returns the native face dimension for the adjusted resolution.
    #[must_use]
    pub const fn max_dim(&self) -> i64 {
        self.max_dim
    }

    /// Returns all real direction and 60-degree-rotation offsets contributed
    /// by the digit at the given one-based H3 resolution.
    #[must_use]
    pub fn suffix_offsets(&self, resolution: Resolution) -> Option<&[Hex2Int; 42]> {
        let index = usize::from(resolution);
        (index != 0 && index <= usize::from(self.target)).then(|| &self.suffixes[index - 1])
    }

    /// Returns every actual target vertex offset under all native rotations.
    #[must_use]
    pub const fn vertex_offsets(&self) -> &[Hex2Int; 36] {
        &self.vertices
    }

    /// Returns all sixty directed icosahedral face seams in paired frames.
    #[must_use]
    pub const fn seams(&self) -> &[ProjectionSeam; 60] {
        &self.seams
    }
}

impl ProjectionSeed {
    /// Returns the compact icosahedron face id.
    #[must_use]
    pub const fn face(self) -> u8 {
        match self {
            Self::Point { face, .. } | Self::Intersection { face, .. } => face,
        }
    }

    /// Resolves this compact seed through the canonical native face tables.
    #[must_use]
    pub fn projection_frame(self) -> ProjectionFrame {
        let face = usize::from(self.face());
        let center = crate::face::CENTER_POINT[face];
        let resolution = match self {
            Self::Point { resolution, .. } | Self::Intersection { resolution, .. } => resolution,
        };
        ProjectionFrame {
            face: self.face(),
            center: [center.x, center.y, center.z],
            axis_az_rads_cii: crate::face::AXES_AZ_RADS_CII[face],
            inv_sqrt7: crate::coord::INV_SQRT7_POWERS[usize::from(resolution)],
            res0_u_gnomonic: crate::coord::RES0_U_GNOMONIC,
            one_third: crate::coord::ONE_THIRD,
            ap7_rot_rads: crate::coord::AP7_ROT_RADS,
            sqrt3_2: crate::coord::SQRT3_2,
        }
    }

    /// Replays this seed through h3o's native floating-point projector.
    ///
    /// This is a doc-hidden diagnostic oracle for the raw-carrier fixtures.
    /// Consumers must retain [`Self`] and replay it with their certified
    /// arithmetic; this rounded latitude/longitude is not a predicate
    /// carrier.
    #[doc(hidden)]
    #[must_use]
    pub fn native_projection_point(self) -> LatLng {
        crate::coord::native_projection_point(self)
    }
}

/// Public boundary plus the private raw projection sidecar.
///
/// [`Boundary`] remains bit-for-bit unchanged.  Only `CellIndex` constructs
/// this type, keeping manually pushed test boundaries free of accidental seed
/// semantics.
// gometry: sidecar pairs public boundary points with raw native input.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ProjectedBoundary {
    /// The ordinary public latitude/longitude boundary.
    pub public: Boundary,
    seeds: [Option<ProjectionSeed>; MAX_BNDRY_VERTS],
    /// The original topological H3 edge which produced each raw push.
    ///
    /// Only insertion points consume this later, but recording it beside the
    /// push prevents an output ordinal from being confused with an edge after
    /// an earlier face-crossing insertion shifted that ordinal.
    source_edges: [u8; MAX_BNDRY_VERTS],
    count: u8,
}

impl ProjectedBoundary {
    pub(crate) fn new() -> Self {
        Self {
            public: Boundary::default(),
            seeds: [None; MAX_BNDRY_VERTS],
            source_edges: [0; MAX_BNDRY_VERTS],
            count: 0,
        }
    }

    pub(crate) fn push(&mut self, point: LatLng, seed: ProjectionSeed, source_edge: u8) {
        let index = usize::from(self.count);
        self.public.points[index] = point;
        self.public.count += 1;
        self.seeds[index] = Some(seed);
        self.source_edges[index] = source_edge;
        self.count += 1;
    }

    /// Returns the raw seeds in the same order as [`Self::public`].
    pub fn projection_seeds(&self) -> impl ExactSizeIterator<Item = &ProjectionSeed> {
        self.seeds[..usize::from(self.count)]
            .iter()
            .map(|seed| seed.as_ref().expect("paired boundary seed"))
    }

    /// Returns the original topological edge recorded with each raw seed.
    ///
    /// A point seed's value is informational.  For an insertion seed it is
    /// the edge identity required to distinguish inserted vertices from a
    /// shifted boundary ordinal.
    pub fn projection_seed_edges(&self) -> impl ExactSizeIterator<Item = u8> + '_ {
        self.source_edges[..usize::from(self.count)].iter().copied()
    }
}

/// Boundary in latitude/longitude.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub struct Boundary {
    /// Vertices in CCW order.
    points: [LatLng; MAX_BNDRY_VERTS],
    /// Number of vertices.
    count: u8,
}

impl Boundary {
    /// Initializes a new empty cell boundary (test only)
    #[must_use]
    #[doc(hidden)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a vertices to the boundary (test only).
    #[doc(hidden)]
    pub fn push(&mut self, ll: LatLng) {
        self.points[usize::from(self.count)] = ll;
        self.count += 1;
    }
}

impl Deref for Boundary {
    type Target = [LatLng];

    /// Dereference to the slice of filled elements.
    fn deref(&self) -> &Self::Target {
        &self.points[..self.count.into()]
    }
}

impl fmt::Display for Boundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, ll) in self.iter().enumerate() {
            if i != 0 {
                write!(f, "-")?;
            }
            write!(f, "{ll}")?;
        }
        write!(f, "]")
    }
}

#[cfg(feature = "geo")]
impl From<Boundary> for geo::LineString {
    fn from(value: Boundary) -> Self {
        Self::new(value.iter().copied().map(geo::Coord::from).collect())
    }
}

#[cfg(feature = "geo")]
impl geo_traits::GeometryTrait for Boundary {
    type GeometryCollectionType<'b>
        = geo_traits::UnimplementedGeometryCollection<f64>
    where
        Self: 'b;
    type LineStringType<'b>
        = Self
    where
        Self: 'b;
    type LineType<'b>
        = geo_traits::UnimplementedLine<f64>
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = geo_traits::UnimplementedMultiLineString<f64>
    where
        Self: 'b;
    type MultiPointType<'b>
        = geo_traits::UnimplementedMultiPoint<f64>
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = geo_traits::UnimplementedMultiPolygon<f64>
    where
        Self: 'b;
    type PointType<'b>
        = geo_traits::UnimplementedPoint<f64>
    where
        Self: 'b;
    type PolygonType<'b>
        = Self
    where
        Self: 'b;
    type RectType<'b>
        = geo_traits::UnimplementedRect<f64>
    where
        Self: 'b;
    type T = f64;
    type TriangleType<'b>
        = geo_traits::UnimplementedTriangle<f64>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        '_,
        Self::PointType<'_>,
        Self::LineStringType<'_>,
        Self::PolygonType<'_>,
        Self::MultiPointType<'_>,
        Self::MultiLineStringType<'_>,
        Self::MultiPolygonType<'_>,
        Self::GeometryCollectionType<'_>,
        Self::RectType<'_>,
        Self::TriangleType<'_>,
        Self::LineType<'_>,
    > {
        if self.count >= crate::NUM_PENT_VERTS {
            geo_traits::GeometryType::Polygon(self)
        } else {
            geo_traits::GeometryType::LineString(self)
        }
    }
}

#[cfg(feature = "geo")]
impl geo_traits::LineStringTrait for Boundary {
    type CoordType<'a> = LatLng;

    fn num_coords(&self) -> usize {
        // Add 1 for pentagon/hexagons (we want closed ring in that case).
        if self.count >= crate::NUM_PENT_VERTS {
            self.count + 1
        } else {
            self.count
        }
        .into()
    }

    /// Access to a specified coordinate in this boundary.
    ///
    /// # Safety
    ///
    /// Accessing an index out of bounds is UB.
    #[expect(unsafe_code, reason = "precondition must be held by the caller")]
    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        if i == usize::from(self.count) {
            // SAFETY: 0 is always valid.
            unsafe { *self.get_unchecked(0) }
        } else {
            // SAFETY: precondition must be held by the caller.
            unsafe { *self.get_unchecked(i) }
        }
    }
}

#[cfg(feature = "geo")]
impl geo_traits::PolygonTrait for Boundary {
    type RingType<'a>
        = Self
    where
        Self: 'a;

    /// Returns the exsterior ring for pentagons and hexagons.
    /// For edges, `None` is returned.
    fn exterior(&self) -> Option<Self::RingType<'_>> {
        (self.count >= crate::NUM_PENT_VERTS).then_some(*self)
    }

    /// H3 cell have no interrior rings.
    fn num_interiors(&self) -> usize {
        0
    }

    /// # Safety
    ///
    /// This method always panic, H3 cells have no interior rings.
    #[expect(unsafe_code, reason = "always panic, it's safe")]
    unsafe fn interior_unchecked(&self, _i: usize) -> Self::RingType<'_> {
        unreachable!("Boundary has no interior rings")
    }
}
