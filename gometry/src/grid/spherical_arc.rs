//! Certified spherical primitives shared by the private grid coverers.
//!
//! This module intentionally has no public geometry surface.  A grid source is
//! affine in longitude/latitude while H3 and S2 boundaries are spherical, so a
//! rounded `LatLng` or an epsilon comparison cannot be allowed to prove a
//! negative.  Every operation here either returns an outward enclosure or
//! declines to decide.

use h3o::{Hex2Int, ProjectionSeed, Resolution};

pub(in crate::grid) mod affine_arc;
pub(in crate::grid) mod arcs;
pub(in crate::grid) mod contact;
pub(crate) mod h3_boundary;
pub(in crate::grid) mod h3_fan;
/// The two adjacent binary64 values bracketing mathematical pi.
mod h3_lattice;
pub(in crate::grid) mod h3_projection;
mod interval;
pub(in crate::grid) mod trig;
pub(in crate::grid) mod vectors;

pub(crate) use h3_lattice::*;
pub(crate) use interval::*;

pub(crate) const PI_LO: f64 = f64::from_bits(0x4009_21FB_5444_2D18);
pub(crate) const PI_HI: f64 = f64::from_bits(0x4009_21FB_5444_2D19);

const HALF_PI_LO: f64 = PI_LO * 0.5;
const HALF_PI_HI: f64 = PI_HI * 0.5;
const QUARTER_PI_LO: f64 = PI_LO * 0.25;
const QUARTER_PI_HI: f64 = PI_HI * 0.25;
const MAX_H3_BOUNDARY_ARCS: usize = 10;

/// Contact result used by the ordered source-edge/cell-arc relation.  It is
/// deliberately four-valued: every undecidable case is a visible boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::grid) enum ArcContact {
    None,
    ClosedOnly,
    Open,
    Uncertain,
}

/// Private structural identity for a native boundary point.  Public
/// coordinates never manufacture these keys: only an ordered supplier does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::grid) enum CellVertexKey {
    H3 {
        cell: h3o::CellIndex,
        ordinal: u8,
    },
    H3Insertion {
        cell: h3o::CellIndex,
        edge: u8,
        ordinal: u8,
    },
}

/// The only physical endpoint identities understood by the spherical carrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::grid) enum PhysicalEndpointKey {
    CellVertex(CellVertexKey),
    NorthPole,
    SouthPole,
}

/// Explicit exceptional supporting-plane forms.  H3 emits no numerical
/// meridian tag: an interval which cannot exclude `n.z == 0` is deliberately
/// unresolved rather than classified through `atan2`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::grid) enum ExactArcKind {
    General,
    UnresolvedMeridian,
}

/// A named coordinate-axis meridian.  The tag is structural: no inverse
/// angle is ever used to manufacture one from an ordinary near-zero normal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::grid) enum AxisMeridian {
    Prime,
    EastQuarter,
    Antimeridian,
    WestQuarter,
}

/// A named latitude axis carried from an exact symbolic source endpoint.
/// This is deliberately not inferred from a zero-containing `Bound`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::grid) enum AxisLatitude {
    Equator,
}

/// Exact coordinate-axis facts for one endpoint of an affine source piece.
/// They may arise at a rational strip split as well as at an input vertex.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::grid) struct AffineEndpointIdentity {
    pub(in crate::grid) longitude: Option<AxisMeridian>,
    pub(in crate::grid) latitude: Option<AxisLatitude>,
}

impl AxisMeridian {
    #[cfg(test)]
    fn from_exact_degrees(value: f64) -> Option<Self> {
        if value == 0.0 {
            Some(Self::Prime)
        } else if value.to_bits() == 90.0_f64.to_bits() {
            Some(Self::EastQuarter)
        } else if value.to_bits() == (-90.0_f64).to_bits() {
            Some(Self::WestQuarter)
        } else if value.to_bits() == 180.0_f64.to_bits()
            || value.to_bits() == (-180.0_f64).to_bits()
        {
            Some(Self::Antimeridian)
        } else {
            None
        }
    }
}

/// A continuous, increasing longitude branch.  The endpoint Bounds retain the
/// distinction between an outer possible span and an inner certain span;
/// collapsing them to one interval would make an outer-only overlap look like
/// an Open proof.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LongitudeSpan {
    lower: Bound,
    upper: Bound,
}

/// A monotone H3 longitude piece on its unique directed 2π sheet.  `turn`
/// records the endpoint lift selected by the strict minor-arc proof; reducing
/// it to an unordered envelope loses the sheet needed to close a cell walk.
#[derive(Clone, Copy, Debug, PartialEq)]
struct DirectedLongitudeSpan {
    start: Bound,
    end: Bound,
    turn: i64,
}

/// One ordered minor great-circle boundary piece supplied from raw H3 seeds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::grid) struct OrderedMinorArc {
    /// Raw H3 ownership. Public coordinates cannot manufacture this id.
    supplier: h3o::CellIndex,
    /// Supplier-local vertex identity remains available even when a physical
    /// endpoint is the pole and therefore intentionally loses its longitude.
    vertex_keys: [CellVertexKey; 2],
    pub(in crate::grid) endpoints: [PhysicalEndpointKey; 2],
    start: Vec3Bound,
    end: Vec3Bound,
    normal: Vec3Bound,
    longitude: Option<DirectedLongitudeSpan>,
    pub(in crate::grid) kind: ExactArcKind,
}

/// Ordered raw H3 boundary pieces in their vendor-supplied order.
#[derive(Clone, Copy, Debug)]
pub(in crate::grid) struct H3ArcSet {
    values: [Option<OrderedMinorArc>; MAX_H3_BOUNDARY_ARCS],
    len: u8,
}

/// Outward point windows which may establish only a positive target-leaf
/// witness.  This deliberately carries no fan: a missed witness falls
/// through to the logical-bbox classifier rather than becoming a negative.
#[derive(Clone, Copy, Debug)]
pub(in crate::grid) struct H3BboxWitnesses {
    values: [Option<CertifiedDegreeWindows>; MAX_H3_BOUNDARY_ARCS + 1],
    len: u8,
}

/// One native H3 cell preparation.  Build the raw ordered boundary once and
/// share it between the exact logical-bbox certificate and the fan, which is
/// derived only when an exact overlap classification survives cheap bounds.
#[derive(Debug)]
pub(in crate::grid) struct H3CellPlan {
    cell: h3o::CellIndex,
    arcs: Option<H3ArcSet>,
    bbox: DegreeWindowResult,
}

/// One closed lon/lat rectangle in the physical geographic sheet.
///
/// A seam-spanning source supplies two windows. Keeping the windows separate
/// is important: the interval between west and east is not part of the source
/// region when west is greater than east.
#[derive(Clone, Copy, Debug)]
pub(crate) struct H3BoundingWindow {
    pub(crate) west: f64,
    pub(crate) south: f64,
    pub(crate) east: f64,
    pub(crate) north: f64,
}

/// A certified local H3 cell topology.  H3 is not treated as the conjunction
/// of all edge half-planes: a Class-III reflex vertex is the union of its two
/// adjacent centre-fan triangles.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the bounded no-allocation H3 fan keeps numeric certification fallible instead of moving it behind a heap allocation"
)]
pub(in crate::grid) enum H3FanPlan {
    Certified(H3Fan),
    Uncertain,
}

/// The only point-in-cell answers a certified H3 fan may make.  An enclosing
/// trigonometric/vector interval that touches a fan side is not promoted to a
/// boundary identity: callers keep it visibly ambiguous.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::grid) enum H3FanPointClass {
    Outside,
    Open,
    Uncertain,
}

#[derive(Debug)]
pub(in crate::grid) struct H3Fan {
    cell: h3o::CellIndex,
    vertices: [Option<H3FanVertex>; MAX_H3_BOUNDARY_ARCS],
    len: u8,
}

#[derive(Clone, Copy, Debug)]
struct H3FanVertex {
    key: CellVertexKey,
    endpoint: PhysicalEndpointKey,
    previous: H3FanTriangle,
    next: H3FanTriangle,
}

#[derive(Clone, Copy, Debug)]
struct H3FanTriangle {
    real_edge: Vec3Bound,
    diagonal: Vec3Bound,
}

/// The endpoint's relationship to its original affine parent edge.  A split
/// endpoint is never promoted into a source endpoint merely because an
/// outward enclosure happens to touch zero or one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::grid) enum SourceEndpointRole {
    ParentStart,
    ParentEnd,
    Partition,
}

/// Structural source cases supplied by the exact affine lift.  These are
/// identities, not conclusions drawn from a rounded interval sign.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::grid) enum AffineStructure {
    Point,
    ConstantLongitude { axis: Option<AxisMeridian> },
    Equator,
    General,
}

/// The affine parent map in certified radians.  `domain` belongs to the
/// piece, while these coefficients continue to name the unsplit parent edge.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::grid) struct AffineParentMap {
    pub(in crate::grid) lambda0: Bound,
    pub(in crate::grid) phi0: Bound,
    pub(in crate::grid) dlambda: Bound,
    pub(in crate::grid) dphi: Bound,
    pub(in crate::grid) structure: AffineStructure,
}

/// A source segment which remains affine in its exact input longitude and
/// latitude. Structural endpoint/pole facts are supplied by `GeographicCoverageSource`;
/// outward radians only evaluate the already-chosen parent piece.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::grid) struct AffineSourceArc {
    map: AffineParentMap,
    domain: Bound,
    endpoint_roles: [SourceEndpointRole; 2],
    endpoint_poles: [Option<PhysicalEndpointKey>; 2],
    // Exact coordinate-axis identities carried from the symbolic affine
    // source. They are intentionally separate from `AffineStructure`: a
    // non-vertical source can meet an exact quadrant or seam at one of its
    // rational strip boundaries. An outward parameter bound around that
    // boundary is not itself an axis identity.
    endpoint_identities: [AffineEndpointIdentity; 2],
}
#[derive(Clone, Copy, Debug, PartialEq)]
struct Vec3Ball {
    x: Ball,
    y: Ball,
    z: Ball,
}

/// Component-wise outward enclosure of a three-dimensional vector.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(in crate::grid) struct Vec3Bound {
    pub(in crate::grid) x: Bound,
    pub(in crate::grid) y: Bound,
    pub(in crate::grid) z: Bound,
}

/// Exact raw-seed segment parameter with a positive denominator.  An
/// arithmetic failure is an undecidable carrier, not wrapped lattice data.
#[expect(
    clippy::similar_names,
    reason = "the determinant is defined over paired x/y edge, segment, and start deltas"
)]

/// Outward enclosures of `1/k!`: the neighbouring floats of the binary64
/// `k!` enclose the integer, and each reciprocal is rounded outward.
#[expect(
    clippy::indexing_slicing,
    reason = "this compile-time table is written in a bounded length loop; the pinned compiler has no const-stable checked array access"
)]

trait Midpoint {
    fn certified_midpoint(self, rhs: f64) -> Option<f64>;
}

impl Midpoint for f64 {
    fn certified_midpoint(self, rhs: f64) -> Option<f64> {
        if !self.is_finite() || !rhs.is_finite() {
            return None;
        }
        // Avoid `self + rhs` overflow even though current angular consumers are
        // small; keeping the primitive general avoids a future unsafe reuse.
        let midpoint = self * 0.5 + rhs * 0.5;
        midpoint.is_finite().then_some(midpoint)
    }
}
