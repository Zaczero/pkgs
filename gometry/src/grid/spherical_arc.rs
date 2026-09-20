//! Certified spherical primitives shared by the private grid coverers.
//!
//! This module intentionally has no public geometry surface.  A grid source is
//! affine in longitude/latitude while H3 and S2 boundaries are spherical, so a
//! rounded `LatLng` or an epsilon comparison cannot be allowed to prove a
//! negative.  Every operation here either returns an outward enclosure or
//! declines to decide.

#[cfg(test)]
use std::cell::Cell;

use h3o::{CapLatticeProfile, Hex2Int, ProjectionSeed, Resolution};

/// The two adjacent binary64 values bracketing mathematical pi.
mod h3_lattice;
mod interval;

pub(crate) use h3_lattice::*;
pub(crate) use interval::*;

pub(crate) const PI_LO: f64 = f64::from_bits(0x4009_21FB_5444_2D18);
pub(crate) const PI_HI: f64 = f64::from_bits(0x4009_21FB_5444_2D19);

const HALF_PI_LO: f64 = PI_LO * 0.5;
const HALF_PI_HI: f64 = PI_HI * 0.5;
const QUARTER_PI_LO: f64 = PI_LO * 0.25;
const QUARTER_PI_HI: f64 = PI_HI * 0.25;

/// Contact result used by the ordered source-edge/cell-arc relation.  It is
/// deliberately four-valued: every undecidable case is a visible boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArcContact {
    None,
    ClosedOnly,
    Open,
    Uncertain,
}

impl ArcContact {
    pub(crate) const fn combine(self, rhs: Self) -> Self {
        use ArcContact::{ClosedOnly, None, Open, Uncertain};
        match (self, rhs) {
            (Open, _) | (_, Open) => Open,
            (Uncertain, _) | (_, Uncertain) => Uncertain,
            (ClosedOnly, _) | (_, ClosedOnly) => ClosedOnly,
            (None, None) => None,
        }
    }
}

/// Private structural identity for a native boundary point.  Public
/// coordinates never manufacture these keys: only an ordered supplier does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CellVertexKey {
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
pub(crate) enum PhysicalEndpointKey {
    CellVertex(CellVertexKey),
    NorthPole,
    SouthPole,
}

/// Explicit exceptional supporting-plane forms.  H3 emits no numerical
/// meridian tag: an interval which cannot exclude `n.z == 0` is deliberately
/// unresolved rather than classified through `atan2`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExactArcKind {
    General,
    UnresolvedMeridian,
}

/// A named coordinate-axis meridian.  The tag is structural: no inverse
/// angle is ever used to manufacture one from an ordinary near-zero normal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AxisMeridian {
    Prime,
    EastQuarter,
    Antimeridian,
    WestQuarter,
}

/// A named latitude axis carried from an exact symbolic source endpoint.
/// This is deliberately not inferred from a zero-containing `Bound`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AxisLatitude {
    Equator,
}

/// Exact coordinate-axis facts for one endpoint of an affine source piece.
/// They may arise at a rational strip split as well as at an input vertex.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AffineEndpointIdentity {
    pub(crate) longitude: Option<AxisMeridian>,
    pub(crate) latitude: Option<AxisLatitude>,
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

#[derive(Clone, Copy, Debug, PartialEq)]
enum LongitudeOverlap {
    Disjoint,
    Contact { outer: Bound, inner: Option<Bound> },
}

impl LongitudeSpan {
    fn ordered(first: Bound, second: Bound) -> Option<Self> {
        if first == second {
            return Some(Self {
                lower: first,
                upper: second,
            });
        }
        if first.hi < second.lo {
            return Some(Self {
                lower: first,
                upper: second,
            });
        }
        if second.hi < first.lo {
            return Some(Self {
                lower: second,
                upper: first,
            });
        }
        None
    }

    fn overlap(self, rhs: Self) -> LongitudeOverlap {
        let outer_lo = self.lower.lo.max(rhs.lower.lo);
        let outer_hi = self.upper.hi.min(rhs.upper.hi);
        let Some(outer) = Bound::new(outer_lo, outer_hi) else {
            return LongitudeOverlap::Disjoint;
        };
        let inner_lo = self.lower.hi.max(rhs.lower.hi);
        let inner_hi = self.upper.lo.min(rhs.upper.lo);
        let inner = Bound::new(inner_lo, inner_hi).filter(|value| value.lo < value.hi);
        LongitudeOverlap::Contact { outer, inner }
    }

    fn strictly_contains(self, point: Bound) -> bool {
        self.lower.hi < point.lo && point.hi < self.upper.lo
    }
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

impl DirectedLongitudeSpan {
    fn envelope(self) -> Option<LongitudeSpan> {
        LongitudeSpan::ordered(self.start, self.end)
    }

    fn shifted(self, turns: i64) -> Option<Self> {
        let tau = Bound::new(2.0 * PI_LO, 2.0 * PI_HI)?;
        // This multiplication proposes a periodic copy only. The enclosing
        // interval is widened and every later contact test remains certified,
        // so a non-exact binary64 representation of a large integer cannot
        // establish a negative.
        let offset = tau.mul(Bound::exact(turns as f64)?)?;
        Some(Self {
            start: self.start.add(offset)?,
            end: self.end.add(offset)?,
            turn: self.turn.checked_add(turns)?,
        })
    }
}

/// One ordered minor great-circle boundary piece supplied from raw H3 seeds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct OrderedMinorArc {
    /// Raw H3 ownership. Public coordinates cannot manufacture this id.
    supplier: h3o::CellIndex,
    /// Supplier-local vertex identity remains available even when a physical
    /// endpoint is the pole and therefore intentionally loses its longitude.
    vertex_keys: [CellVertexKey; 2],
    pub(crate) endpoints: [PhysicalEndpointKey; 2],
    start: Vec3Bound,
    end: Vec3Bound,
    normal: Vec3Bound,
    longitude: Option<DirectedLongitudeSpan>,
    pub(crate) kind: ExactArcKind,
}

/// Position against the closed ordered minor arc.  Boundary and uncertainty
/// are distinct internally, but neither may establish a negative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArcPosition {
    Outside,
    Open,
    Boundary,
    Uncertain,
}

impl OrderedMinorArc {
    #[cfg(test)]
    fn from_h3_points(
        cell: h3o::CellIndex,
        start_ordinal: u8,
        end_ordinal: u8,
        start: Vec3Bound,
        end: Vec3Bound,
    ) -> Option<Self> {
        let normal = start.cross(end)?;
        let normal_norm = normal.dot(normal)?.sqrt()?;
        if normal_norm.contains_zero() {
            return None;
        }
        // `dot > -1` is the strict non-antipodal minor-order precondition.
        if start.dot(end)?.lo <= -1.0 {
            return None;
        }
        // H3 supplies raw vectors but no exact equator/axis tag.  A
        // zero-containing component is only a possibility, never structural
        // authority for a negative-producing special branch.
        let kind = if normal.z.strictly_positive() || normal.z.strictly_negative() {
            ExactArcKind::General
        } else {
            ExactArcKind::UnresolvedMeridian
        };
        let longitude = if kind == ExactArcKind::General {
            Some(directed_arc_longitude(start, end)?)
        } else {
            None
        };
        let vertex_keys = [
            CellVertexKey::H3 {
                cell,
                ordinal: start_ordinal,
            },
            CellVertexKey::H3 {
                cell,
                ordinal: end_ordinal,
            },
        ];
        Some(Self::from_keyed_points(
            cell,
            vertex_keys,
            vertex_keys.map(PhysicalEndpointKey::CellVertex),
            start,
            end,
            kind,
            normal,
            longitude,
        ))
    }

    const fn from_keyed_points(
        supplier: h3o::CellIndex,
        vertex_keys: [CellVertexKey; 2],
        endpoints: [PhysicalEndpointKey; 2],
        start: Vec3Bound,
        end: Vec3Bound,
        kind: ExactArcKind,
        normal: Vec3Bound,
        longitude: Option<DirectedLongitudeSpan>,
    ) -> Self {
        Self {
            supplier,
            vertex_keys,
            endpoints,
            start,
            end,
            normal,
            longitude,
            kind,
        }
    }

    #[cfg(test)]
    fn from_test_points(start: [f64; 3], end: [f64; 3]) -> Self {
        Self::from_h3_points(
            h3o::CellIndex::try_from(0x0800_1FFF_FFFF_FFFF_u64).expect("known H3 cell"),
            0,
            1,
            Vec3Bound::exact(start).expect("finite test ray"),
            Vec3Bound::exact(end).expect("finite test ray"),
        )
        .expect("valid test minor arc")
    }

    fn position(self, point: Vec3Bound) -> ArcPosition {
        if self.kind == ExactArcKind::UnresolvedMeridian {
            return ArcPosition::Uncertain;
        }
        let Some(start_cross) = self.start.cross(point) else {
            return ArcPosition::Uncertain;
        };
        let Some(end_cross) = point.cross(self.end) else {
            return ArcPosition::Uncertain;
        };
        let Some(left) = self.normal.dot(start_cross) else {
            return ArcPosition::Uncertain;
        };
        let Some(right) = self.normal.dot(end_cross) else {
            return ArcPosition::Uncertain;
        };
        if left.strictly_negative() || right.strictly_negative() {
            return ArcPosition::Outside;
        }
        if left.strictly_positive() && right.strictly_positive() {
            ArcPosition::Open
        } else if !left.contains_zero() && !right.contains_zero() {
            ArcPosition::Boundary
        } else {
            ArcPosition::Uncertain
        }
    }

    fn with_longitude_shift(mut self, turns: i64) -> Option<Self> {
        self.longitude = Some(self.longitude?.shifted(turns)?);
        Some(self)
    }
}

/// Outward latitude bounds for one ordered minor great-circle piece.  The
/// extrema are the two projections of the north axis onto the arc plane; a
/// candidate may be used only after the ordered-arc certificate places it on
/// the piece.  Ambiguity widens to the world latitude interval.
fn certified_arc_latitude_bounds_from_endpoints(
    arc: &OrderedMinorArc,
    start_latitude: Bound,
    endpoint_latitude: Bound,
) -> Option<Bound> {
    let mut latitude = Bound::new(start_latitude.lo, start_latitude.hi)?;
    latitude = Bound::new(
        latitude.lo.min(endpoint_latitude.lo),
        latitude.hi.max(endpoint_latitude.hi),
    )?;
    let normal_squared = arc.normal.dot(arc.normal)?;
    if normal_squared.contains_zero() {
        return Bound::new(-HALF_PI_HI, HALF_PI_HI);
    }
    let north = Vec3Bound::exact([0.0, 0.0, 1.0])?;
    let projection = arc
        .normal
        .scale(north.dot(arc.normal)?.div(normal_squared)?)?;
    let candidate = north.sub(projection)?.normalized()?;
    for candidate in [candidate, candidate.neg()] {
        match arc.position(candidate) {
            ArcPosition::Outside => {},
            ArcPosition::Open | ArcPosition::Boundary => {
                let candidate_latitude = certified_vector_latitude(candidate)?;
                latitude = Bound::new(
                    latitude.lo.min(candidate_latitude.lo),
                    latitude.hi.max(candidate_latitude.hi),
                )?;
            },
            ArcPosition::Uncertain => return Bound::new(-HALF_PI_HI, HALF_PI_HI),
        }
    }
    Some(latitude)
}

#[cfg(test)]
fn certified_arc_latitude_bounds(arc: &OrderedMinorArc) -> Option<Bound> {
    certified_arc_latitude_bounds_from_endpoints(
        arc,
        certified_vector_latitude(arc.start)?,
        certified_vector_latitude(arc.end)?,
    )
}

/// The native H3 boundary holds at most five original pentagon vertices plus
/// five face-crossing insertions.  Keep that vendor bound structural: arc
/// construction must not make allocation failure a numeric uncertainty.
const MAX_H3_BOUNDARY_ARCS: usize = 10;

/// Ordered raw H3 boundary pieces in their vendor-supplied order.
#[derive(Clone, Copy, Debug)]
pub(crate) struct H3ArcSet {
    values: [Option<OrderedMinorArc>; MAX_H3_BOUNDARY_ARCS],
    len: u8,
}

/// Outward point windows which may establish only a positive target-leaf
/// witness.  This deliberately carries no fan: a missed witness falls
/// through to the logical-bbox classifier rather than becoming a negative.
#[derive(Clone, Copy, Debug)]
pub(crate) struct H3BboxWitnesses {
    values: [Option<CertifiedDegreeWindows>; MAX_H3_BOUNDARY_ARCS + 1],
    len: u8,
}

impl H3BboxWitnesses {
    const fn empty() -> Self {
        Self {
            values: [None; MAX_H3_BOUNDARY_ARCS + 1],
            len: 0,
        }
    }

    fn push(&mut self, value: CertifiedDegreeWindows) -> Option<()> {
        let index = usize::from(self.len);
        if index == self.values.len() {
            return None;
        }
        self.values[index] = Some(value);
        self.len += 1;
        Some(())
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = CertifiedDegreeWindows> + '_ {
        self.values[..usize::from(self.len)]
            .iter()
            .map(|value| value.expect("H3 witness prefix is initialized"))
    }
}

/// One native H3 cell preparation.  Build the raw ordered boundary once and
/// share it between the exact logical-bbox certificate and the fan, which is
/// derived only when an exact overlap classification survives cheap bounds.
#[derive(Debug)]
pub(crate) struct H3CellPlan {
    cell: h3o::CellIndex,
    arcs: Option<H3ArcSet>,
    bbox: DegreeWindowResult,
}

impl H3CellPlan {
    pub(crate) const fn cell(&self) -> h3o::CellIndex {
        self.cell
    }

    pub(crate) const fn arcs(&self) -> Option<&H3ArcSet> {
        self.arcs.as_ref()
    }

    pub(crate) const fn bbox(&self) -> DegreeWindowResult {
        self.bbox
    }

    /// Derive the exact fan from the shared raw boundary.  The returned fan
    /// belongs to the caller's operation; it is intentionally not retained in
    /// the cell plan because bbox and cap exclusions do not need it.
    pub(crate) fn fan(&self) -> H3FanPlan {
        self.arcs.as_ref().map_or(H3FanPlan::Uncertain, |arcs| {
            h3_fan_plan_from_arcs(self.cell, arcs)
        })
    }
}

impl H3ArcSet {
    const fn empty() -> Self {
        Self {
            values: [None; MAX_H3_BOUNDARY_ARCS],
            len: 0,
        }
    }

    const fn len(&self) -> usize {
        self.len as usize
    }

    const fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn get(&self, index: usize) -> Option<&OrderedMinorArc> {
        (index < self.len())
            .then(|| self.values[index].as_ref())
            .flatten()
    }

    const fn push(&mut self, arc: &OrderedMinorArc) -> Option<()> {
        let index = self.len();
        if index == MAX_H3_BOUNDARY_ARCS {
            return None;
        }
        self.values[index] = Some(*arc);
        self.len += 1;
        Some(())
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = &OrderedMinorArc> {
        self.values[..self.len()]
            .iter()
            .map(|arc| arc.as_ref().expect("H3 arc set prefix is initialized"))
    }
}

/// Builds every H3 boundary piece in the vendor's supplied order, including
/// Class-III face-crossing insertions.  The final point is joined back to the
/// first; `ProjectedBoundary` guarantees the public/seed counts agree.
pub(crate) fn h3_ordered_arcs(cell: h3o::CellIndex) -> Option<H3ArcSet> {
    let boundary = cell.boundary_with_seeds();
    let count = boundary.public.len();
    if !(3..=MAX_H3_BOUNDARY_ARCS).contains(&count)
        || count != boundary.projection_seeds().len()
        || count != boundary.projection_seed_edges().len()
    {
        return None;
    }

    let mut points = [None; MAX_H3_BOUNDARY_ARCS];
    for (ordinal, (seed, source_edge)) in boundary
        .projection_seeds()
        .copied()
        .zip(boundary.projection_seed_edges())
        .enumerate()
    {
        let ordinal = u8::try_from(ordinal).ok()?;
        let key = match seed {
            ProjectionSeed::Point { .. } => CellVertexKey::H3 { cell, ordinal },
            ProjectionSeed::Intersection { .. } => CellVertexKey::H3Insertion {
                cell,
                edge: source_edge,
                ordinal,
            },
        };
        let point = project_h3_seed(&seed)?;
        points[usize::from(ordinal)] = Some((
            key,
            PhysicalEndpointKey::CellVertex(key),
            point,
            certified_vector_longitude(point)?,
        ));
    }
    let mut arcs = H3ArcSet::empty();
    for index in 0..count {
        let (start_vertex, start_key, start, start_longitude) = points[index]?;
        let (end_vertex, end_key, end, end_longitude) = points[(index + 1) % count]?;
        let normal = start.cross(end)?;
        let normal_norm = normal.dot(normal)?.sqrt()?;
        if normal_norm.contains_zero() || start.dot(end)?.lo <= -1.0 {
            return None;
        }
        let kind = if normal.z.strictly_positive() || normal.z.strictly_negative() {
            ExactArcKind::General
        } else {
            ExactArcKind::UnresolvedMeridian
        };
        let longitude = if kind == ExactArcKind::General {
            Some(directed_arc_longitude_from_endpoints(
                start_longitude,
                end_longitude,
            )?)
        } else {
            None
        };
        arcs.push(&OrderedMinorArc::from_keyed_points(
            cell,
            [start_vertex, end_vertex],
            [start_key, end_key],
            start,
            end,
            kind,
            normal,
            longitude,
        ))?;
    }
    Some(arcs)
}

/// Certified point witnesses for a target H3 cell.  The target-substrate
/// centre and every native projected boundary seed are positive-only aids for
/// bbox traversal; all ambiguity is returned as `None` so the caller uses the
/// exact ordered-arc bbox instead.
pub(crate) fn h3_bbox_positive_witnesses(cell: h3o::CellIndex) -> Option<H3BboxWitnesses> {
    let boundary = cell.boundary_with_seeds();
    let count = boundary.public.len();
    if !(3..=MAX_H3_BOUNDARY_ARCS).contains(&count) || count != boundary.projection_seeds().len() {
        return None;
    }
    let mut witnesses = H3BboxWitnesses::empty();
    let center = cell.target_substrate_center_seed(cell.resolution())?;
    witnesses.push(certified_h3_point_degree_window(project_h3_seed(&center)?)?)?;
    for seed in boundary.projection_seeds() {
        witnesses.push(certified_h3_point_degree_window(project_h3_seed(seed)?)?)?;
    }
    Some(witnesses)
}

fn certified_h3_point_degree_window(point: Vec3Bound) -> Option<CertifiedDegreeWindows> {
    let longitude = certified_vector_longitude(point)?;
    let result = CertifiedCircularBoundsRad {
        latitude: certified_vector_latitude(point)?,
        longitude: CertifiedLongitudeRad::Span(LongitudeSpan::ordered(longitude, longitude)?),
    }
    .to_degree_windows();
    match result {
        DegreeWindowResult::Windows(windows) => Some(windows),
        DegreeWindowResult::Boundary => None,
    }
}

/// Logical longitude from the ordered H3 boundary.  The directed turn is
/// accumulated only across vendor-adjacent endpoint identities; interpreting
/// the same spans as an unordered circular union loses the 2π sheet.
fn certified_h3_longitude(arcs: &H3ArcSet) -> Option<CertifiedLongitudeRad> {
    let first = arcs.get(0)?;
    let first_span = first.longitude?;
    let tau = Bound::new(2.0 * PI_LO, 2.0 * PI_HI)?;
    let mut sheet = 0_i64;
    let mut minimum = first_span.start;
    let mut maximum = first_span.start;
    for index in 0..arcs.len() {
        let arc = arcs.get(index)?;
        let next = arcs.get((index + 1) % arcs.len())?;
        if arc.endpoints[1] != next.endpoints[0] || arc.kind != ExactArcKind::General {
            return None;
        }
        let span = arc.longitude?;
        let shift = tau.mul(Bound::exact(sheet as f64)?)?;
        let start = span.start.add(shift)?;
        let end = span.end.add(shift)?;
        minimum = Bound::new(
            minimum.lo.min(start.lo).min(end.lo),
            minimum.hi.min(start.hi).min(end.hi),
        )?;
        maximum = Bound::new(
            maximum.lo.max(start.lo).max(end.lo),
            maximum.hi.max(start.hi).max(end.hi),
        )?;
        sheet = sheet.checked_add(span.turn)?;
    }
    match sheet {
        // The sign of the ordered sheet is physical information, not merely
        // a longitude spelling: it identifies the pole contained by the
        // boundary. Dropping it pruned polar cells before arc classification.
        1 => return Some(CertifiedLongitudeRad::FullThroughNorthPole),
        -1 => return Some(CertifiedLongitudeRad::FullThroughSouthPole),
        0 => {},
        _ => return None,
    }
    let width = maximum.sub(minimum)?;
    if width.hi >= tau.lo {
        return None;
    }
    Some(CertifiedLongitudeRad::Span(LongitudeSpan::ordered(
        minimum, maximum,
    )?))
}

/// Exact H3 logical bbox from the one ordered native boundary plan.  A
/// failure is `Boundary`, never a narrow negative certificate.
fn exact_h3_bbox(arcs: &H3ArcSet) -> DegreeWindowResult {
    let Some(bounds) = (|| {
        let mut latitude: Option<Bound> = None;
        let mut vertex_latitudes = [None; MAX_H3_BOUNDARY_ARCS];
        for (index, arc) in arcs.iter().enumerate() {
            vertex_latitudes[index] = Some(certified_vector_latitude(arc.start)?);
        }
        for (index, arc) in arcs.iter().enumerate() {
            let arc_latitude = certified_arc_latitude_bounds_from_endpoints(
                arc,
                vertex_latitudes[index]?,
                vertex_latitudes[(index + 1) % arcs.len()]?,
            )?;
            latitude = Some(match latitude {
                Some(current) => Bound::new(
                    current.lo.min(arc_latitude.lo),
                    current.hi.max(arc_latitude.hi),
                )?,
                None => arc_latitude,
            });
        }
        let longitude = certified_h3_longitude(arcs)?;
        let latitude = match longitude {
            CertifiedLongitudeRad::FullThroughNorthPole => Bound::new(latitude?.lo, HALF_PI_HI)?,
            CertifiedLongitudeRad::FullThroughSouthPole => Bound::new(-HALF_PI_HI, latitude?.hi)?,
            CertifiedLongitudeRad::Full | CertifiedLongitudeRad::Span(_) => latitude?,
        };
        Some(CertifiedCircularBoundsRad {
            latitude,
            longitude,
        })
    })() else {
        return DegreeWindowResult::Boundary;
    };
    bounds.to_degree_windows()
}

/// Exact H3 logical bbox without constructing a centre fan.  Bbox traversal
/// needs only the ordered native boundary; fan ambiguity belongs solely to
/// the overlap relation and may not widen this independent path.
#[cfg(test)]
pub(crate) fn exact_h3_bbox_for_cell(cell: h3o::CellIndex) -> DegreeWindowResult {
    h3_ordered_arcs(cell).map_or(DegreeWindowResult::Boundary, |arcs| exact_h3_bbox(&arcs))
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

/// Prove that every point in every supplied rectangle is inside `cell`.
///
/// H3's public boundary is a rounded presentation. The ordered projection
/// seeds above retain the true edge arcs, so each edge's oriented great-circle
/// halfspace is tested over the complete parameter rectangle. A positive
/// result is the only affirmative answer; failure means that the caller must
/// try a coarser ancestor or report the family no-single-cell result.
pub(crate) fn h3_cell_contains_windows(cell: h3o::CellIndex, windows: &[H3BoundingWindow]) -> bool {
    let Some(arcs) = h3_ordered_arcs(cell) else {
        return false;
    };
    let center = h3o::LatLng::from(cell);
    let Some(center_longitude) = Bound::exact(center.lng()) else {
        return false;
    };
    let Some(center_latitude) = Bound::exact(center.lat()) else {
        return false;
    };
    let Some(center) = certified_degree_vector(center_longitude, center_latitude) else {
        return false;
    };

    let mut inward = [None; MAX_H3_BOUNDARY_ARCS];
    for (index, arc) in arcs.iter().enumerate() {
        let Some(side) = arc.normal.dot(center) else {
            return false;
        };
        inward[index] = if side.strictly_positive() {
            Some(arc.normal)
        } else if side.strictly_negative() {
            Some(arc.normal.neg())
        } else {
            return false;
        };
    }

    windows.iter().all(|window| {
        let Some(lon0) = degrees_to_radians_bound(window.west) else {
            return false;
        };
        let Some(lon1) = degrees_to_radians_bound(window.east) else {
            return false;
        };
        let Some(lat0) = degrees_to_radians_bound(window.south) else {
            return false;
        };
        let Some(lat1) = degrees_to_radians_bound(window.north) else {
            return false;
        };
        if window.west > window.east || window.south > window.north {
            return false;
        }
        (0..arcs.len()).all(|index| {
            inward[index].is_some_and(|normal| {
                h3_plane_min_with_uncertainty(normal, lon0, lon1, lat0, lat1)
                    .is_some_and(|value| value >= 0.0)
            })
        })
    })
}

fn degrees_to_radians_bound(value: f64) -> Option<Bound> {
    degree_bound_to_radians(Bound::exact(value)?)
}

/// Lower-bound a plane over a rectangle using the analytic scalar minimum and
/// the raw projection interval's coefficient uncertainty. The scalar minimum
/// accounts for all four edges and both interior stationary directions; the
/// final ulp term covers the trigonometric evaluation itself.
fn h3_plane_min_with_uncertainty(
    normal: Vec3Bound,
    lon0: Bound,
    lon1: Bound,
    lat0: Bound,
    lat1: Bound,
) -> Option<f64> {
    let nx = normal.x.lo.certified_midpoint(normal.x.hi)?;
    let ny = normal.y.lo.certified_midpoint(normal.y.hi)?;
    let nz = normal.z.lo.certified_midpoint(normal.z.hi)?;
    let nominal = scalar_plane_min(nx, ny, nz, lon0, lon1, lat0, lat1)?;
    let uncertainty = upward_sum([
        bound_radius(normal.x)?,
        bound_radius(normal.y)?,
        bound_radius(normal.z)?,
    ])?;
    let scale = nx.abs().max(ny.abs()).max(nz.abs()).max(1.0);
    down(nominal - uncertainty - 256.0 * f64::EPSILON * scale)
}

fn bound_radius(value: Bound) -> Option<f64> {
    let midpoint = value.lo.certified_midpoint(value.hi)?;
    (midpoint - value.lo)
        .max(value.hi - midpoint)
        .is_finite()
        .then_some((midpoint - value.lo).max(value.hi - midpoint))
}

/// Exact stationary-point inventory for `n · (cos(lat)cos(lon),
/// cos(lat)sin(lon), sin(lat))` over a closed rectangle. This is a proof
/// helper, not a finite sample test: every candidate comes from a boundary or
/// a derivative zero of the analytic function.
fn scalar_plane_min(
    nx: f64,
    ny: f64,
    nz: f64,
    lon0: Bound,
    lon1: Bound,
    lat0: Bound,
    lat1: Bound,
) -> Option<f64> {
    let lon0 = lon0.lo;
    let lon1 = lon1.hi;
    let lat0 = lat0.lo;
    let lat1 = lat1.hi;
    let mut minimum = f64::INFINITY;
    let mut visit = |lon: f64, lat: f64| {
        let value = plane_value(nx, ny, nz, lon, lat);
        if value.is_finite() {
            minimum = minimum.min(value);
        }
    };

    for &lon in &[lon0, lon1] {
        for &lat in &[lat0, lat1] {
            visit(lon, lat);
        }
        let s = nx * lon.cos() + ny * lon.sin();
        let alpha = nz.atan2(s);
        for k in -1..=1 {
            let lat = alpha + std::f64::consts::PI * f64::from(k);
            if lat >= lat0 && lat <= lat1 {
                visit(lon, lat);
            }
        }
    }
    for &lat in &[lat0, lat1] {
        let phi = ny.atan2(nx);
        for k in -2..=2 {
            let lon = phi + std::f64::consts::PI + std::f64::consts::TAU * f64::from(k);
            if lon >= lon0 && lon <= lon1 {
                visit(lon, lat);
            }
        }
    }
    let phi = ny.atan2(nx);
    for base in [phi, phi + std::f64::consts::PI] {
        for k in -2..=2 {
            let lon = base + std::f64::consts::TAU * f64::from(k);
            if lon < lon0 || lon > lon1 {
                continue;
            }
            let s = nx * lon.cos() + ny * lon.sin();
            let alpha = nz.atan2(s);
            for k in -1..=1 {
                let lat = alpha + std::f64::consts::PI * f64::from(k);
                if lat >= lat0 && lat <= lat1 {
                    visit(lon, lat);
                }
            }
        }
    }
    minimum.is_finite().then_some(minimum)
}

fn plane_value(nx: f64, ny: f64, nz: f64, lon: f64, lat: f64) -> f64 {
    let (sin_lon, cos_lon) = lon.sin_cos();
    let (sin_lat, cos_lat) = lat.sin_cos();
    cos_lat * (nx * cos_lon + ny * sin_lon) + nz * sin_lat
}

/// A certified local H3 cell topology.  H3 is not treated as the conjunction
/// of all edge half-planes: a Class-III reflex vertex is the union of its two
/// adjacent centre-fan triangles.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the bounded no-allocation H3 fan keeps numeric certification fallible instead of moving it behind a heap allocation"
)]
pub(crate) enum H3FanPlan {
    Certified(H3Fan),
    Uncertain,
}

#[cfg(test)]
thread_local! {
    static H3_FAN_PLAN_CONSTRUCTIONS: Cell<usize> = const { Cell::new(0) };
}

/// The only point-in-cell answers a certified H3 fan may make.  An enclosing
/// trigonometric/vector interval that touches a fan side is not promoted to a
/// boundary identity: callers keep it visibly ambiguous.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum H3FanPointClass {
    Outside,
    Open,
    Uncertain,
}

#[derive(Debug)]
pub(crate) struct H3Fan {
    cell: h3o::CellIndex,
    vertices: [Option<H3FanVertex>; MAX_H3_BOUNDARY_ARCS],
    len: u8,
}

/// A certified open hemisphere containing this cell boundary.  The later A3
/// descendant-cap plan supplies the traversal radius; the fan only needs its
/// stricter prerequisite that every one of its *own* ordered minor pieces is
/// inside a cap of radius strictly below pi/2.  Positive center-dot bounds at
/// both endpoints prove that for each minor piece by positive normalized
/// linear interpolation.
#[derive(Clone, Copy, Debug)]
struct CertifiedH3BoundaryCap {
    center: Vec3Bound,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FanSide {
    Exterior,
    Open,
    Uncertain,
}

#[derive(Clone, Copy, Debug)]
struct FanInputVertex {
    key: CellVertexKey,
    endpoint: PhysicalEndpointKey,
    ray: Vec3Bound,
    projected: TangentPoint,
}

#[derive(Clone, Copy, Debug)]
struct TangentPoint {
    x: Bound,
    y: Bound,
}

impl H3FanPlan {
    /// Locate a stored degree point in this cell's certified fan.  This is a
    /// point witness primitive, not a replacement for source/cell arc
    /// contact: it proves `Outside` only when every real fan triangle has a
    /// strict exterior constraint.
    pub(crate) fn point_class(&self, longitude: Bound, latitude: Bound) -> H3FanPointClass {
        let Self::Certified(plan) = self else {
            return H3FanPointClass::Uncertain;
        };
        let Some(point) = certified_degree_vector(longitude, latitude) else {
            return H3FanPointClass::Uncertain;
        };
        let mut all_exterior = true;
        for index in 0..usize::from(plan.len) {
            let Some(triangle) = plan.triangle_at(index) else {
                return H3FanPointClass::Uncertain;
            };
            match triangle_point_class(&triangle, point) {
                H3FanPointClass::Open => return H3FanPointClass::Open,
                H3FanPointClass::Outside => {},
                H3FanPointClass::Uncertain => all_exterior = false,
            }
        }
        if all_exterior {
            H3FanPointClass::Outside
        } else {
            H3FanPointClass::Uncertain
        }
    }

    /// A cell-center candidate has a stronger, distinct certificate than a
    /// general source witness: strict positivity against every *real* ordered
    /// boundary edge places it in the fan's proved star kernel.  Artificial
    /// center diagonals deliberately do not participate here—the native
    /// center lies on all of them, and treating their zero as open would be
    /// unsound for an arbitrary source witness.
    pub(crate) fn kernel_point_class(&self, longitude: Bound, latitude: Bound) -> H3FanPointClass {
        let Self::Certified(plan) = self else {
            return H3FanPointClass::Uncertain;
        };
        let Some(point) = certified_degree_vector(longitude, latitude) else {
            return H3FanPointClass::Uncertain;
        };
        for vertex in plan.vertices() {
            let Some(side) = vertex.next.real_edge.dot(point) else {
                return H3FanPointClass::Uncertain;
            };
            if !side.strictly_positive() {
                return H3FanPointClass::Uncertain;
            }
        }
        H3FanPointClass::Open
    }

    fn matches_cell(&self, cell: h3o::CellIndex) -> bool {
        matches!(self, Self::Certified(H3Fan { cell: plan_cell, .. }) if *plan_cell == cell)
    }

    fn side_at_vertex(&self, key: CellVertexKey, tangent: Vec3Ball) -> FanSide {
        let Self::Certified(plan) = self else {
            return FanSide::Uncertain;
        };
        let Some(vertex) = plan.vertices().find(|vertex| vertex.key == key) else {
            return FanSide::Uncertain;
        };
        let previous = triangle_side(&vertex.previous, tangent);
        let next = triangle_side(&vertex.next, tangent);
        if previous == FanSide::Open || next == FanSide::Open {
            FanSide::Open
        } else if previous == FanSide::Exterior && next == FanSide::Exterior {
            FanSide::Exterior
        } else {
            FanSide::Uncertain
        }
    }

    fn side_at_physical_endpoint(
        &self,
        endpoint: PhysicalEndpointKey,
        tangent: Vec3Ball,
    ) -> FanSide {
        let Self::Certified(plan) = self else {
            return FanSide::Uncertain;
        };
        let Some(vertex) = plan.vertices().find(|vertex| vertex.endpoint == endpoint) else {
            return FanSide::Uncertain;
        };
        self.side_at_vertex(vertex.key, tangent)
    }
}

fn triangle_point_class(triangle: &[Vec3Bound; 3], point: Vec3Bound) -> H3FanPointClass {
    let [Some(first), Some(second), Some(third)] = triangle.map(|plane| plane.dot(point)) else {
        return H3FanPointClass::Uncertain;
    };
    if first.strictly_positive() && second.strictly_positive() && third.strictly_positive() {
        H3FanPointClass::Open
    } else if first.strictly_negative() || second.strictly_negative() || third.strictly_negative() {
        H3FanPointClass::Outside
    } else {
        H3FanPointClass::Uncertain
    }
}

impl H3Fan {
    fn vertices(&self) -> impl ExactSizeIterator<Item = &H3FanVertex> {
        self.vertices[..usize::from(self.len)]
            .iter()
            .map(|vertex| vertex.as_ref().expect("H3 fan prefix is initialized"))
    }

    /// Triangle `(center, v_i, v_{i+1})` has its real outer edge and BOTH
    /// artificial diagonals.  The vertex-side relation needs only one
    /// diagonal locally; general fan point location must retain both or a
    /// single wedge would falsely cover the exterior half of the cell.
    fn triangle_at(&self, index: usize) -> Option<[Vec3Bound; 3]> {
        let current = self.vertices.get(index)?.as_ref()?;
        let next = self
            .vertices
            .get((index + 1) % usize::from(self.len))?
            .as_ref()?;
        Some([
            current.next.real_edge,
            current.next.diagonal,
            next.previous.diagonal,
        ])
    }
}

/// Builds the per-cell certificate required before any H3 vertex side can
/// establish a negative.  A failed projection, orientation, or fixed-size
/// segment proof yields `Uncertain`; it is never repaired by a regular-hexagon
/// assumption.
#[cfg(test)]
pub(crate) fn h3_fan_plan(cell: h3o::CellIndex) -> H3FanPlan {
    h3_cell_plan(cell, cell.resolution()).fan()
}

#[cfg(test)]
pub(crate) fn reset_h3_fan_plan_construction_count() {
    H3_FAN_PLAN_CONSTRUCTIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn h3_fan_plan_construction_count() -> usize {
    H3_FAN_PLAN_CONSTRUCTIONS.with(Cell::get)
}

pub(crate) fn h3_cell_plan(cell: h3o::CellIndex, target: Resolution) -> H3CellPlan {
    let arcs = (target == cell.resolution())
        .then(|| h3_ordered_arcs(cell))
        .flatten();
    let bbox = arcs
        .as_ref()
        .map_or(DegreeWindowResult::Boundary, exact_h3_bbox);
    H3CellPlan { cell, arcs, bbox }
}

fn h3_fan_plan_from_arcs(cell: h3o::CellIndex, arcs: &H3ArcSet) -> H3FanPlan {
    #[cfg(test)]
    H3_FAN_PLAN_CONSTRUCTIONS.with(|count| count.set(count.get() + 1));

    let Some(center) = cell
        .target_substrate_center_seed(cell.resolution())
        .and_then(|seed| project_h3_seed(&seed))
    else {
        return H3FanPlan::Uncertain;
    };
    let Some(cap) = certify_h3_boundary_cap(center, arcs) else {
        return H3FanPlan::Uncertain;
    };
    let Some((north, east)) = tangent_basis(cap.center) else {
        return H3FanPlan::Uncertain;
    };
    let count = arcs.len();
    if !(3..=10).contains(&count) {
        return H3FanPlan::Uncertain;
    }

    let mut inputs = [None; MAX_H3_BOUNDARY_ARCS];
    for (index, arc) in arcs.iter().enumerate() {
        let Some(next) = arcs.get((index + 1) % count) else {
            return H3FanPlan::Uncertain;
        };
        if arc.vertex_keys[1] != next.vertex_keys[0] {
            return H3FanPlan::Uncertain;
        }
        if arc.supplier != cell {
            return H3FanPlan::Uncertain;
        }
        let Some(projected) = cap.gnomonic_project(north, east, arc.start) else {
            return H3FanPlan::Uncertain;
        };
        if !tangent_nonzero(projected) {
            return H3FanPlan::Uncertain;
        }
        inputs[index] = Some(FanInputVertex {
            key: arc.vertex_keys[0],
            endpoint: arc.endpoints[0],
            ray: arc.start,
            projected,
        });
    }

    let input = |index: usize| inputs.get(index).copied().flatten();

    let origin = TangentPoint {
        x: Bound::exact(0.0).expect("finite origin"),
        y: Bound::exact(0.0).expect("finite origin"),
    };
    let (Some(first), Some(second)) = (input(0), input(1)) else {
        return H3FanPlan::Uncertain;
    };
    let Some(ring_sign) =
        orientation(first.projected, second.projected, origin).and_then(strict_sign)
    else {
        return H3FanPlan::Uncertain;
    };
    for index in 0..count {
        let next = (index + 1) % count;
        let (Some(current), Some(next)) = (input(index), input(next)) else {
            return H3FanPlan::Uncertain;
        };
        if orientation(current.projected, next.projected, origin).and_then(strict_sign)
            != Some(ring_sign)
        {
            return H3FanPlan::Uncertain;
        }
    }
    for first in 0..count {
        for second in (first + 1)..count {
            if second == first + 1 || (first == 0 && second + 1 == count) {
                continue;
            }
            let (Some(first_start), Some(first_end), Some(second_start), Some(second_end)) = (
                input(first),
                input((first + 1) % count),
                input(second),
                input((second + 1) % count),
            ) else {
                return H3FanPlan::Uncertain;
            };
            if !matches!(
                segments_strictly_disjoint(
                    first_start.projected,
                    first_end.projected,
                    second_start.projected,
                    second_end.projected,
                ),
                Some(true)
            ) {
                return H3FanPlan::Uncertain;
            }
        }
    }
    for diagonal in 0..count {
        for edge in 0..count {
            if edge == diagonal || (edge + 1) % count == diagonal {
                continue;
            }
            let (Some(diagonal_input), Some(edge_start), Some(edge_end)) =
                (input(diagonal), input(edge), input((edge + 1) % count))
            else {
                return H3FanPlan::Uncertain;
            };
            if !matches!(
                segments_strictly_disjoint(
                    origin,
                    diagonal_input.projected,
                    edge_start.projected,
                    edge_end.projected,
                ),
                Some(true)
            ) {
                return H3FanPlan::Uncertain;
            }
        }
        for other in (diagonal + 1)..count {
            let (Some(diagonal_input), Some(other_input)) = (input(diagonal), input(other)) else {
                return H3FanPlan::Uncertain;
            };
            if !matches!(
                radial_open_interiors_strictly_disjoint(
                    origin,
                    diagonal_input.projected,
                    other_input.projected,
                ),
                Some(true)
            ) {
                return H3FanPlan::Uncertain;
            }
        }
    }

    let mut vertices = [None; MAX_H3_BOUNDARY_ARCS];
    for (index, slot) in vertices.iter_mut().enumerate().take(count) {
        let previous = (index + count - 1) % count;
        let next = (index + 1) % count;
        let (Some(previous_input), Some(current), Some(next_input)) =
            (input(previous), input(index), input(next))
        else {
            return H3FanPlan::Uncertain;
        };
        let Some(previous_real) = previous_input
            .ray
            .cross(current.ray)
            .and_then(|plane| orient_inward(plane, center))
        else {
            return H3FanPlan::Uncertain;
        };
        let Some(previous_diagonal) = current
            .ray
            .cross(center)
            .and_then(|plane| orient_inward(plane, previous_input.ray))
        else {
            return H3FanPlan::Uncertain;
        };
        let Some(next_real) = current
            .ray
            .cross(next_input.ray)
            .and_then(|plane| orient_inward(plane, center))
        else {
            return H3FanPlan::Uncertain;
        };
        let Some(next_diagonal) = center
            .cross(current.ray)
            .and_then(|plane| orient_inward(plane, next_input.ray))
        else {
            return H3FanPlan::Uncertain;
        };
        *slot = Some(H3FanVertex {
            key: current.key,
            endpoint: current.endpoint,
            previous: H3FanTriangle {
                real_edge: previous_real,
                diagonal: previous_diagonal,
            },
            next: H3FanTriangle {
                real_edge: next_real,
                diagonal: next_diagonal,
            },
        });
    }
    H3FanPlan::Certified(H3Fan {
        cell,
        vertices,
        len: u8::try_from(count).expect("bounded H3 fan length"),
    })
}

fn certify_h3_boundary_cap(center: Vec3Bound, arcs: &H3ArcSet) -> Option<CertifiedH3BoundaryCap> {
    if arcs.is_empty() {
        return None;
    }
    for arc in arcs.iter() {
        if !center.dot(arc.start)?.strictly_positive() || !center.dot(arc.end)?.strictly_positive()
        {
            return None;
        }
    }
    Some(CertifiedH3BoundaryCap { center })
}

impl CertifiedH3BoundaryCap {
    fn gnomonic_project(
        self,
        north: Vec3Bound,
        east: Vec3Bound,
        point: Vec3Bound,
    ) -> Option<TangentPoint> {
        // The constructor proves this denominator strictly positive for every
        // ordered boundary endpoint.  Keep the assertion as a checked
        // defense against a future caller feeding a non-cap point.
        gnomonic_project(self.center, north, east, point)
    }
}

fn tangent_basis(center: Vec3Bound) -> Option<(Vec3Bound, Vec3Bound)> {
    let north_axis = Vec3Bound::exact([0.0, 0.0, 1.0])?;
    let north = north_axis.sub(center.scale(center.z)?)?.normalized()?;
    Some((north, north.cross(center)?))
}

fn gnomonic_project(
    center: Vec3Bound,
    north: Vec3Bound,
    east: Vec3Bound,
    point: Vec3Bound,
) -> Option<TangentPoint> {
    let denominator = center.dot(point)?;
    if !denominator.strictly_positive() {
        return None;
    }
    let inverse = Bound::exact(1.0)?.div(denominator)?;
    let offset = point.scale(inverse)?.sub(center)?;
    Some(TangentPoint {
        x: offset.dot(north)?,
        y: offset.dot(east)?,
    })
}

fn tangent_nonzero(point: TangentPoint) -> bool {
    point
        .x
        .mul(point.x)
        .and_then(|x| point.y.mul(point.y).and_then(|y| x.add(y)))
        .is_some_and(Bound::strictly_positive)
}

fn orientation(first: TangentPoint, second: TangentPoint, third: TangentPoint) -> Option<Bound> {
    second
        .x
        .sub(first.x)?
        .mul(third.y.sub(first.y)?)?
        .sub(second.y.sub(first.y)?.mul(third.x.sub(first.x)?)?)
}

fn segments_strictly_disjoint(
    first: TangentPoint,
    second: TangentPoint,
    third: TangentPoint,
    fourth: TangentPoint,
) -> Option<bool> {
    // A strict separator from EITHER segment's supporting line proves closed
    // disjointness.  Do not require all four orientations: a nonincident H3
    // edge can meet a center diagonal's line extension at an opposite vertex
    // while the finite segments remain strictly disjoint.
    let first_separator = matches!(
        (
            orientation(first, second, third).and_then(strict_sign),
            orientation(first, second, fourth).and_then(strict_sign),
        ),
        (Some(left), Some(right)) if left == right
    );
    let second_separator = matches!(
        (
            orientation(third, fourth, first).and_then(strict_sign),
            orientation(third, fourth, second).and_then(strict_sign),
        ),
        (Some(left), Some(right)) if left == right
    );
    (first_separator || second_separator).then_some(true)
}

/// Two fan diagonals share only the tangent-plane origin.  A strict angular
/// separation proves their open interiors disjoint; opposite collinear rays
/// are also disjoint and need a dot proof instead of an orientation sign.
fn radial_open_interiors_strictly_disjoint(
    origin: TangentPoint,
    first: TangentPoint,
    second: TangentPoint,
) -> Option<bool> {
    if orientation(origin, first, second)
        .and_then(strict_sign)
        .is_some()
    {
        return Some(true);
    }
    let first_x = first.x.sub(origin.x)?;
    let first_y = first.y.sub(origin.y)?;
    let second_x = second.x.sub(origin.x)?;
    let second_y = second.y.sub(origin.y)?;
    first_x
        .mul(second_x)?
        .add(first_y.mul(second_y)?)?
        .strictly_negative()
        .then_some(true)
}

fn orient_inward(plane: Vec3Bound, witness: Vec3Bound) -> Option<Vec3Bound> {
    match plane.dot(witness)? {
        value if value.strictly_positive() => Some(plane),
        value if value.strictly_negative() => Some(plane.neg()),
        _ => None,
    }
}

fn triangle_side(triangle: &H3FanTriangle, tangent: Vec3Ball) -> FanSide {
    let (Some(real), Some(diagonal)) = (
        tangent.dot(triangle.real_edge).map(Ball::sign),
        tangent.dot(triangle.diagonal).map(Ball::sign),
    ) else {
        return FanSide::Uncertain;
    };
    if real == CertSign::Positive && diagonal == CertSign::Positive {
        FanSide::Open
    } else if real == CertSign::Negative || diagonal == CertSign::Negative {
        FanSide::Exterior
    } else {
        FanSide::Uncertain
    }
}

/// The endpoint's relationship to its original affine parent edge.  A split
/// endpoint is never promoted into a source endpoint merely because an
/// outward enclosure happens to touch zero or one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceEndpointRole {
    ParentStart,
    ParentEnd,
    Partition,
}

/// Structural source cases supplied by the exact affine lift.  These are
/// identities, not conclusions drawn from a rounded interval sign.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AffineStructure {
    Point,
    ConstantLongitude { axis: Option<AxisMeridian> },
    Equator,
    General,
}

/// The affine parent map in certified radians.  `domain` belongs to the
/// piece, while these coefficients continue to name the unsplit parent edge.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AffineParentMap {
    pub(crate) lambda0: Bound,
    pub(crate) phi0: Bound,
    pub(crate) dlambda: Bound,
    pub(crate) dphi: Bound,
    pub(crate) structure: AffineStructure,
}

/// A source segment which remains affine in its exact input longitude and
/// latitude. Structural endpoint/pole facts are supplied by `GeographicCoverageSource`;
/// outward radians only evaluate the already-chosen parent piece.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AffineSourceArc {
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

impl Vec3Ball {
    fn dot(self, vector: Vec3Bound) -> Option<Ball> {
        self.x
            .mul(Ball::from_bound(vector.x)?)?
            .add(self.y.mul(Ball::from_bound(vector.y)?)?)?
            .add(self.z.mul(Ball::from_bound(vector.z)?)?)
    }

    fn scale_sign(self, sign: i8) -> Option<Self> {
        match sign {
            1 => Some(self),
            -1 => Some(Self {
                x: self.x.neg()?,
                y: self.y.neg()?,
                z: self.z.neg()?,
            }),
            _ => None,
        }
    }
}

impl AffineSourceArc {
    pub(crate) fn from_parent_map(
        map: &AffineParentMap,
        domain: Bound,
        endpoint_roles: [SourceEndpointRole; 2],
        parent_poles: [Option<PhysicalEndpointKey>; 2],
        endpoint_identities: [AffineEndpointIdentity; 2],
    ) -> Option<Self> {
        let domain = domain.intersection(Bound::new(0.0, 1.0)?)?;
        match map.structure {
            AffineStructure::Point if !map.dlambda.is_exact_zero() || !map.dphi.is_exact_zero() => {
                return None;
            },
            AffineStructure::ConstantLongitude { .. } if !map.dlambda.is_exact_zero() => {
                return None;
            },
            AffineStructure::Equator if !map.phi0.is_exact_zero() || !map.dphi.is_exact_zero() => {
                return None;
            },
            AffineStructure::General
            | AffineStructure::Point
            | AffineStructure::ConstantLongitude { .. }
            | AffineStructure::Equator => {},
        }
        for (role, pole) in endpoint_roles.into_iter().zip(parent_poles) {
            if matches!(role, SourceEndpointRole::Partition) && pole.is_some() {
                return None;
            }
        }
        Some(Self {
            map: *map,
            domain,
            endpoint_roles,
            endpoint_poles: parent_poles,
            endpoint_identities,
        })
    }

    const fn is_zero_length(self) -> bool {
        matches!(self.map.structure, AffineStructure::Point)
    }

    const fn is_vertical(self) -> bool {
        matches!(
            self.map.structure,
            AffineStructure::ConstantLongitude { .. }
        )
    }

    #[cfg(test)]
    fn is_axis(self, axis: AxisMeridian) -> bool {
        matches!(self.map.structure, AffineStructure::ConstantLongitude { axis: Some(value) } if value == axis)
    }

    fn lambda_at(self, t: Bound) -> Option<Bound> {
        self.map.lambda0.add(self.map.dlambda.mul(t)?)
    }

    fn phi_at(self, t: Bound) -> Option<Bound> {
        self.map.phi0.add(self.map.dphi.mul(t)?)
    }

    fn longitude_span(self) -> Option<LongitudeSpan> {
        if self.is_vertical() {
            return Some(LongitudeSpan {
                lower: self.map.lambda0,
                upper: self.map.lambda0,
            });
        }
        let lower = self.lambda_at(Bound::exact(self.domain.lo)?)?;
        let upper = self.lambda_at(Bound::exact(self.domain.hi)?)?;
        LongitudeSpan::ordered(lower, upper)
    }

    fn latitude_span(self) -> Option<LongitudeSpan> {
        let lower = self.phi_at(Bound::exact(self.domain.lo)?)?;
        let upper = self.phi_at(Bound::exact(self.domain.hi)?)?;
        LongitudeSpan::ordered(lower, upper)
    }

    /// Whether this piece can reach the closed latitude band, in degrees.
    /// A piece whose certified latitudes miss the band cannot meet an arc
    /// that lies inside it.
    pub(crate) fn may_reach_latitudes(self, band: Bound) -> bool {
        let Some(span) = self.latitude_span() else {
            return true;
        };
        // Outward conversion: to_radians rounds once.
        let south = band.lo.to_radians().next_down().next_down();
        let north = band.hi.to_radians().next_up().next_up();
        !(span.upper.hi < south || span.lower.lo > north)
    }

    fn parameter_at_longitude(self, longitude: Bound) -> Option<Bound> {
        longitude.sub(self.map.lambda0)?.div(self.map.dlambda)
    }

    fn parameter_at_latitude(self, latitude: Bound) -> Option<Bound> {
        latitude.sub(self.map.phi0)?.div(self.map.dphi)
    }

    fn endpoint_pole(self, endpoint: usize) -> Option<PhysicalEndpointKey> {
        match self.endpoint_roles.get(endpoint)? {
            SourceEndpointRole::ParentStart | SourceEndpointRole::ParentEnd => {
                self.endpoint_poles[endpoint]
            },
            SourceEndpointRole::Partition => None,
        }
    }

    fn endpoint_identity_at(self, t: Bound) -> Option<AffineEndpointIdentity> {
        let start = Bound::exact(self.domain.lo)?;
        if t == start {
            return Some(self.endpoint_identities[0]);
        }
        let end = Bound::exact(self.domain.hi)?;
        if t == end {
            return Some(self.endpoint_identities[1]);
        }
        None
    }

    fn vector_at(self, t: Bound) -> Option<Vec3Bound> {
        let lambda = self.lambda_at(t)?;
        let phi = self.phi_at(t)?;
        let (sin_lambda, cos_lambda) = self.sin_cos_lambda_at(t, lambda)?;
        let (sin_phi, cos_phi) = self.sin_cos_phi_at(t, phi)?;
        let sin_lambda = sin_lambda.bound()?;
        let cos_lambda = cos_lambda.bound()?;
        let sin_phi = sin_phi.bound()?;
        let cos_phi = cos_phi.bound()?;
        Some(Vec3Bound {
            x: cos_phi.mul(cos_lambda)?,
            y: cos_phi.mul(sin_lambda)?,
            z: sin_phi,
        })
    }

    fn plane_value(self, arc: &OrderedMinorArc, t: Bound) -> Option<Bound> {
        arc.normal.dot(self.vector_at(t)?)
    }

    fn tangent_at(self, endpoint: usize) -> Option<Vec3Ball> {
        let t = Bound::exact(if endpoint == 0 {
            self.domain.lo
        } else {
            self.domain.hi
        })?;
        let away_sign = if endpoint == 0 { 1 } else { -1 };
        let lambda = self.lambda_at(t)?;
        let dphi = Ball::from_bound(self.map.dphi)?;
        let (sin_lambda, cos_lambda) = self.sin_cos_lambda_at(t, lambda)?;
        if let Some(pole) = self.endpoint_pole(endpoint) {
            let sign = match pole {
                PhysicalEndpointKey::NorthPole => -1,
                PhysicalEndpointKey::SouthPole => 1,
                PhysicalEndpointKey::CellVertex(_) => return None,
            };
            return Vec3Ball {
                x: dphi.mul(cos_lambda)?.scale_sign(sign)?,
                y: dphi.mul(sin_lambda)?.scale_sign(sign)?,
                z: Ball::exact(0.0)?,
            }
            .scale_sign(away_sign);
        }
        self.tangent_at_parameter(t, away_sign)
    }

    fn tangent_at_parameter(self, t: Bound, away_sign: i8) -> Option<Vec3Ball> {
        let dlambda = Ball::from_bound(self.map.dlambda)?;
        let dphi = Ball::from_bound(self.map.dphi)?;
        let phi = self.phi_at(t)?;
        let (sin_phi, cos_phi) = self.sin_cos_phi_at(t, phi)?;
        let lambda = self.lambda_at(t)?;
        let (sin_lambda, cos_lambda) = self.sin_cos_lambda_at(t, lambda)?;
        let x = dlambda
            .mul(cos_phi.mul(sin_lambda)?.neg()?)?
            .add(dphi.mul(sin_phi.mul(cos_lambda)?.neg()?)?)?;
        let y = dlambda
            .mul(cos_phi.mul(cos_lambda)?)?
            .add(dphi.mul(sin_phi.mul(sin_lambda)?.neg()?)?)?;
        let z = dphi.mul(cos_phi)?;
        Vec3Ball { x, y, z }.scale_sign(away_sign)
    }

    fn plane_derivative(self, arc: &OrderedMinorArc, lambda: Bound) -> Option<Bound> {
        let (sin_lambda, cos_lambda) = sin_cos(lambda)?;
        let sin_lambda = sin_lambda.bound()?;
        let cos_lambda = cos_lambda.bound()?;
        let a = arc
            .normal
            .x
            .mul(cos_lambda)?
            .add(arc.normal.y.mul(sin_lambda)?)?;
        let w = arc
            .normal
            .x
            .mul(sin_lambda)?
            .sub(arc.normal.y.mul(cos_lambda)?)?;
        let nz_squared = arc.normal.z.mul(arc.normal.z)?;
        self.map
            .dphi
            .mul(nz_squared.add(a.mul(a)?)?)?
            .sub(self.map.dlambda.mul(arc.normal.z.mul(w)?)?)
    }

    fn derivative_lipschitz(self, arc: &OrderedMinorArc) -> Option<Bound> {
        let radial = arc.normal.x.abs().add(arc.normal.y.abs())?;
        let twice_dphi_radial = Bound::exact(2.0)?.mul(self.map.dphi.abs())?.mul(radial)?;
        let dlambda_nz = self.map.dlambda.abs().mul(arc.normal.z.abs())?;
        radial.mul(twice_dphi_radial.add(dlambda_nz)?)
    }

    fn vertical_derivative(self, arc: &OrderedMinorArc, phi: Bound) -> Option<Bound> {
        // `is_vertical()` is an exact stored-degree structural fact.  Use the
        // local piece's literal longitude rather than widening it over a
        // parent domain interval.
        let (sin_lambda, cos_lambda) = self.sin_cos_lambda(self.map.lambda0)?;
        let a = arc
            .normal
            .x
            .mul(cos_lambda.bound()?)?
            .add(arc.normal.y.mul(sin_lambda.bound()?)?)?;
        let (sin_phi, cos_phi) = sin_cos(phi)?;
        a.mul(sin_phi.bound()?)?
            .neg()
            .add(arc.normal.z.mul(cos_phi.bound()?)?)
    }

    fn sin_cos_lambda(self, lambda: Bound) -> Option<(Ball, Ball)> {
        if let AffineStructure::ConstantLongitude { axis: Some(axis) } = self.map.structure {
            return axis_sin_cos(axis);
        }
        sin_cos(lambda)
    }

    fn sin_cos_lambda_at(self, t: Bound, lambda: Bound) -> Option<(Ball, Ball)> {
        if let Some(axis) = self
            .endpoint_identity_at(t)
            .and_then(|identity| identity.longitude)
        {
            return axis_sin_cos(axis);
        }
        self.sin_cos_lambda(lambda)
    }

    fn sin_cos_phi_at(self, t: Bound, phi: Bound) -> Option<(Ball, Ball)> {
        if matches!(
            self.endpoint_identity_at(t)
                .and_then(|identity| identity.latitude),
            Some(AxisLatitude::Equator)
        ) {
            return Some((Ball::exact(0.0)?, Ball::exact(1.0)?));
        }
        sin_cos(phi)
    }
}

#[cfg(test)]
fn test_affine_source_arc(lon0: f64, lat0: f64, lon1: f64, lat1: f64) -> Option<AffineSourceArc> {
    let lambda0 = degrees_to_radians_bound(lon0)?;
    let phi0 = degrees_to_radians_bound(lat0)?;
    let lambda1 = degrees_to_radians_bound(lon1)?;
    let phi1 = degrees_to_radians_bound(lat1)?;
    let same_longitude = lon0.to_bits() == lon1.to_bits();
    let same_latitude = lat0.to_bits() == lat1.to_bits();
    let equatorial = lat0 == 0.0 && lat1 == 0.0;
    let structure = if same_longitude && same_latitude {
        AffineStructure::Point
    } else if same_longitude {
        AffineStructure::ConstantLongitude {
            axis: AxisMeridian::from_exact_degrees(lon0),
        }
    } else if equatorial {
        AffineStructure::Equator
    } else {
        AffineStructure::General
    };
    let map = AffineParentMap {
        lambda0,
        phi0: if equatorial { Bound::exact(0.0)? } else { phi0 },
        dlambda: if same_longitude {
            Bound::exact(0.0)?
        } else {
            lambda1.sub(lambda0)?
        },
        dphi: if same_latitude || equatorial {
            Bound::exact(0.0)?
        } else {
            phi1.sub(phi0)?
        },
        structure,
    };
    AffineSourceArc::from_parent_map(
        &map,
        Bound::new(0.0, 1.0)?,
        [
            SourceEndpointRole::ParentStart,
            SourceEndpointRole::ParentEnd,
        ],
        [pole_key(lat0), pole_key(lat1)],
        [
            AffineEndpointIdentity {
                longitude: AxisMeridian::from_exact_degrees(lon0),
                latitude: (lat0 == 0.0).then_some(AxisLatitude::Equator),
            },
            AffineEndpointIdentity {
                longitude: AxisMeridian::from_exact_degrees(lon1),
                latitude: (lat1 == 0.0).then_some(AxisLatitude::Equator),
            },
        ],
    )
}

/// Exact source-axis identities are structural facts from the stored degree
/// endpoint, not a choice made by reducing an interval around pi/2 or pi.
fn axis_sin_cos(axis: AxisMeridian) -> Option<(Ball, Ball)> {
    let (sin, cos) = match axis {
        AxisMeridian::Prime => (0.0, 1.0),
        AxisMeridian::EastQuarter => (1.0, 0.0),
        AxisMeridian::Antimeridian => (0.0, -1.0),
        AxisMeridian::WestQuarter => (-1.0, 0.0),
    };
    Some((Ball::exact(sin)?, Ball::exact(cos)?))
}

const MAX_D_BOXES: usize = 96;
const MAX_D_DEPTH: u8 = 24;

/// Returns the B1 four-valued contact result for one affine source piece and
/// one ordered minor arc.  Every undecidable structural or analytic condition
/// deliberately reaches `Uncertain`; callers may only fail open from it.
#[cfg(test)]
pub(crate) fn classify_arc_contact(source: &AffineSourceArc, arc: &OrderedMinorArc) -> ArcContact {
    classify_arc_contact_with_h3_fan(source, arc, None)
}

/// H3's relation entry point binds a contact to the one certified fan which
/// owns the cell.  A mismatched supplier or an uncertified fan cannot use a
/// structural branch to establish a negative.
pub(crate) fn classify_h3_arc_contact(
    source: &AffineSourceArc,
    arc: &OrderedMinorArc,
    fan: &H3FanPlan,
) -> ArcContact {
    if !fan.matches_cell(arc.supplier) {
        return ArcContact::Uncertain;
    }
    classify_arc_contact_with_h3_fan(source, arc, Some(fan))
}

fn classify_arc_contact_with_h3_fan(
    source: &AffineSourceArc,
    arc: &OrderedMinorArc,
    fan: Option<&H3FanPlan>,
) -> ArcContact {
    if source.domain.lo >= source.domain.hi || arc.kind == ExactArcKind::UnresolvedMeridian {
        return ArcContact::Uncertain;
    }
    if source.is_zero_length() {
        return classify_zero_source(source, arc);
    }
    if let Some(contact) = classify_matching_pole_contact(source, arc, fan) {
        return contact;
    }
    classify_with_arc_shifts(source, arc, |source, arc| {
        if source.is_vertical() {
            classify_vertical_contact(&source, arc)
        } else {
            classify_general_contact(&source, arc)
        }
    })
}

fn classify_with_arc_shifts(
    source: &AffineSourceArc,
    arc: &OrderedMinorArc,
    classify: impl Fn(AffineSourceArc, &OrderedMinorArc) -> ArcContact,
) -> ArcContact {
    let mut contact = ArcContact::None;
    let Some((first_shift, last_shift)) = source_arc_shift_range(source, arc) else {
        return ArcContact::Uncertain;
    };
    for shift in first_shift..=last_shift {
        let Some(shifted) = arc.with_longitude_shift(shift) else {
            return ArcContact::Uncertain;
        };
        contact = contact.combine(classify(*source, &shifted));
    }
    contact
}

/// Every periodic copy of a directed cell arc that can meet this exact source
/// piece.  The floating calculation is only a conservative bucket proposal:
/// both endpoints are widened by a whole additional sheet before any contact
/// predicate runs. A source piece wider than the seam-split grammar can bound
/// is ambiguous rather than being searched on an arbitrary fixed three-sheet
/// range.
fn source_arc_shift_range(source: &AffineSourceArc, arc: &OrderedMinorArc) -> Option<(i64, i64)> {
    let source = source.longitude_span()?;
    let arc = arc.longitude?.envelope()?;
    let lower_proposal = (source.lower.lo - arc.upper.hi) / (2.0 * PI_HI);
    let upper_proposal = (source.upper.hi - arc.lower.lo) / (2.0 * PI_LO);
    if !lower_proposal.is_finite()
        || !upper_proposal.is_finite()
        || lower_proposal < i64::MIN as f64
        || upper_proposal > i64::MAX as f64
    {
        return None;
    }
    let first = (lower_proposal.floor() as i64).checked_sub(1)?;
    let last = (upper_proposal.ceil() as i64).checked_add(1)?;
    if last < first || last.checked_sub(first)? > 4 {
        return None;
    }
    Some((first, last))
}

/// Only exact +/-90 source endpoints can share a physical endpoint key with a
/// cell arc.  A shared endpoint requires the H3 fan's two-triangle local side
/// rule, so this arc-only primitive deliberately leaves it `Uncertain`.
fn classify_matching_pole_contact(
    source: &AffineSourceArc,
    arc: &OrderedMinorArc,
    fan: Option<&H3FanPlan>,
) -> Option<ArcContact> {
    for endpoint in 0..2 {
        let Some(key) = source.endpoint_pole(endpoint) else {
            continue;
        };
        if !arc.endpoints.contains(&key) {
            continue;
        }
        let Some(fan) = fan else {
            return Some(ArcContact::Uncertain);
        };
        let Some(tangent) = source.tangent_at(endpoint) else {
            return Some(ArcContact::Uncertain);
        };
        return Some(match fan.side_at_physical_endpoint(key, tangent) {
            FanSide::Open => ArcContact::Open,
            FanSide::Exterior => ArcContact::ClosedOnly,
            FanSide::Uncertain => ArcContact::Uncertain,
        });
    }
    None
}

fn classify_zero_source(source: &AffineSourceArc, arc: &OrderedMinorArc) -> ArcContact {
    let Some(point) = source.vector_at(Bound::exact(source.domain.lo).expect("finite domain"))
    else {
        return ArcContact::Uncertain;
    };
    let Some(plane) = arc.normal.dot(point) else {
        return ArcContact::Uncertain;
    };
    if !plane.contains_zero() || arc.position(point) == ArcPosition::Outside {
        ArcContact::None
    } else {
        // No public coordinate can manufacture a cell endpoint identity.
        ArcContact::Uncertain
    }
}

fn classify_general_contact(source: &AffineSourceArc, arc: &OrderedMinorArc) -> ArcContact {
    let (Some(source_span), Some(arc_span)) = (
        source.longitude_span(),
        arc.longitude.and_then(DirectedLongitudeSpan::envelope),
    ) else {
        return ArcContact::Uncertain;
    };
    let LongitudeOverlap::Contact { outer, inner } = source_span.overlap(arc_span) else {
        return ArcContact::None;
    };
    let Some(left) = plane_at_overlap_longitude(source, arc, outer.lo) else {
        return ArcContact::Uncertain;
    };
    let Some(right) = plane_at_overlap_longitude(source, arc, outer.hi) else {
        return ArcContact::Uncertain;
    };
    if same_strict_sign(left, right) && certify_derivative_sign(source, arc, outer).is_some() {
        return ArcContact::None;
    }
    let Some(inner) = inner else {
        return ArcContact::Uncertain;
    };
    let Some((first, second)) = strict_inner_samples(inner) else {
        return ArcContact::Uncertain;
    };
    let (Some(first), Some(second)) = (
        plane_at_longitude(source, arc, first),
        plane_at_longitude(source, arc, second),
    ) else {
        return ArcContact::Uncertain;
    };
    if opposite_strict_sign(first, second) {
        ArcContact::Open
    } else {
        ArcContact::Uncertain
    }
}

fn classify_vertical_contact(source: &AffineSourceArc, arc: &OrderedMinorArc) -> ArcContact {
    let (Some(source_lon), Some(arc_lon), Some(latitude)) = (
        source.longitude_span(),
        arc.longitude.and_then(DirectedLongitudeSpan::envelope),
        source.latitude_span(),
    ) else {
        return ArcContact::Uncertain;
    };
    if matches!(source_lon.overlap(arc_lon), LongitudeOverlap::Disjoint) {
        return ArcContact::None;
    }
    let (Some(left), Some(right)) = (
        source.plane_value(arc, Bound::exact(source.domain.lo).expect("finite domain")),
        source.plane_value(arc, Bound::exact(source.domain.hi).expect("finite domain")),
    ) else {
        return ArcContact::Uncertain;
    };
    let Some(latitude_outer) = Bound::new(latitude.lower.lo, latitude.upper.hi) else {
        return ArcContact::Uncertain;
    };
    // In the physical latitude strip the plane function has at most one
    // root, and a root changes its sign.  Equal strict endpoint signs are
    // therefore already a certified negative; requiring a derivative bound
    // here would wrongly fail open for an interval that merely straddles a
    // trig quadrant.
    let source_is_strictly_physical =
        latitude_outer.lo >= -HALF_PI_LO && latitude_outer.hi <= HALF_PI_LO;
    if same_strict_sign(left, right)
        && (source_is_strictly_physical
            || source
                .vertical_derivative(arc, latitude_outer)
                .and_then(strict_sign)
                .is_some())
    {
        return ArcContact::None;
    }
    if !arc_lon.strictly_contains(source_lon.lower) {
        return ArcContact::Uncertain;
    }
    let Some(inner) =
        Bound::new(latitude.lower.hi, latitude.upper.lo).filter(|value| value.lo < value.hi)
    else {
        return ArcContact::Uncertain;
    };
    let Some((first, second)) = strict_inner_samples(inner) else {
        return ArcContact::Uncertain;
    };
    let (Some(first), Some(second)) = (Bound::exact(first), Bound::exact(second)) else {
        return ArcContact::Uncertain;
    };
    let (Some(first), Some(second)) = (
        source
            .parameter_at_latitude(first)
            .and_then(|t| source.plane_value(arc, t)),
        source
            .parameter_at_latitude(second)
            .and_then(|t| source.plane_value(arc, t)),
    ) else {
        return ArcContact::Uncertain;
    };
    if opposite_strict_sign(first, second) {
        ArcContact::Open
    } else {
        ArcContact::Uncertain
    }
}

fn plane_at_longitude(
    source: &AffineSourceArc,
    arc: &OrderedMinorArc,
    longitude: f64,
) -> Option<Bound> {
    source
        .parameter_at_longitude(Bound::exact(longitude)?)
        .and_then(|parameter| source.plane_value(arc, parameter))
}

/// Evaluate a plane at a certified longitude-overlap endpoint.  The endpoint
/// of an outward source longitude bound is correlated with its parent
/// parameter: materializing its rounded `hi`/`lo` as a new exact longitude can
/// fall just outside the parent map and lose an otherwise strict sign.  When
/// that happens, use the original exact parent endpoint only if its outward
/// longitude enclosure contains the requested endpoint.  This preserves the
/// proof's provenance; it is not an epsilon substitution or a new sample.
fn plane_at_overlap_longitude(
    source: &AffineSourceArc,
    arc: &OrderedMinorArc,
    longitude: f64,
) -> Option<Bound> {
    plane_at_longitude(source, arc, longitude).or_else(|| {
        let requested = Bound::exact(longitude)?;
        for parameter in [source.domain.lo, source.domain.hi] {
            let parameter = Bound::exact(parameter)?;
            if source
                .lambda_at(parameter)?
                .intersection(requested)
                .is_some()
            {
                return source.plane_value(arc, parameter);
            }
        }
        None
    })
}

/// Exact representable samples strictly inside a certified inner interval.
/// They are deliberately not interval midpoints: B5's root proof needs two
/// literal f64 parameters known to lie in every possible overlap.
fn strict_inner_samples(inner: Bound) -> Option<(f64, f64)> {
    let width = inner.hi - inner.lo;
    if !width.is_finite() || width <= 0.0 {
        return None;
    }
    let first = inner.lo + width / 3.0;
    let second = inner.lo + (2.0 * width) / 3.0;
    (first.is_finite()
        && second.is_finite()
        && inner.lo < first
        && first < second
        && second < inner.hi)
        .then_some((first, second))
}

const fn opposite_strict_sign(left: Bound, right: Bound) -> bool {
    matches!(
        (strict_sign(left), strict_sign(right)),
        (Some(CertSign::Positive), Some(CertSign::Negative))
            | (Some(CertSign::Negative), Some(CertSign::Positive))
    )
}

/// B4's bounded finite-extrema proof.  Every box must exclude zero with the
/// same D sign.  A zero, sign conflict, midpoint collapse, or either fixed
/// budget limit is intentionally undecidable.
fn certify_derivative_sign(
    source: &AffineSourceArc,
    arc: &OrderedMinorArc,
    lambda: Bound,
) -> Option<CertSign> {
    if lambda.width() >= PI_LO {
        return None;
    }
    let lipschitz = source.derivative_lipschitz(arc)?.hi;
    if !lipschitz.is_finite() || lipschitz < 0.0 {
        return None;
    }
    let mut stack = [(Bound::exact(0.0)?, 0_u8); MAX_D_BOXES];
    stack[0].0 = lambda;
    let mut length = 1;
    let mut common = None;
    while length != 0 {
        length -= 1;
        let (box_lambda, depth) = stack[length];
        let midpoint = box_lambda.lo.certified_midpoint(box_lambda.hi)?;
        let sign = (|| {
            let value = source.plane_derivative(arc, Bound::exact(midpoint)?)?;
            let ball = Ball::from_bound(value)?;
            let radius = upward_sum([ball.rad, lipschitz * outward_half_width(box_lambda)?])?;
            Some(Ball::new(ball.mid, radius)?.sign())
        })()
        .unwrap_or(CertSign::Uncertain);
        match sign {
            sign @ (CertSign::Positive | CertSign::Negative) => {
                if let Some(previous) = common {
                    if previous != sign {
                        return None;
                    }
                } else {
                    common = Some(sign);
                }
            },
            CertSign::Uncertain => {
                if depth == MAX_D_DEPTH || length + 2 > MAX_D_BOXES {
                    return None;
                }
                let lower = Bound::new(box_lambda.lo, midpoint)?;
                let upper = Bound::new(midpoint, box_lambda.hi)?;
                if lower.width() == 0.0 || upper.width() == 0.0 {
                    return None;
                }
                stack[length] = (lower, depth + 1);
                stack[length + 1] = (upper, depth + 1);
                length += 2;
            },
        }
    }
    common
}

const fn strict_sign(value: Bound) -> Option<CertSign> {
    if value.strictly_positive() {
        Some(CertSign::Positive)
    } else if value.strictly_negative() {
        Some(CertSign::Negative)
    } else {
        None
    }
}

const fn same_strict_sign(left: Bound, right: Bound) -> bool {
    matches!(
        (strict_sign(left), strict_sign(right)),
        (Some(CertSign::Positive), Some(CertSign::Positive))
            | (Some(CertSign::Negative), Some(CertSign::Negative))
    )
}

/// The affine source carrier supplies outward degree intervals; this is the
/// sole certified conversion into its radian parent map.
pub(crate) fn degree_bound_to_radians(value: Bound) -> Option<Bound> {
    value
        .mul(Bound::new(PI_LO, PI_HI)?)?
        .div(Bound::exact(180.0)?)
}

/// Certified spherical ray for a stored degree point.  The polar identities
/// are structural and deliberately ignore longitude; every other case is
/// evaluated through the bounded trigonometric carrier.
fn certified_degree_vector(longitude: Bound, latitude: Bound) -> Option<Vec3Bound> {
    if latitude.is_exact_value(90.0) {
        return Vec3Bound::exact([0.0, 0.0, 1.0]);
    }
    if latitude.is_exact_value(-90.0) {
        return Vec3Bound::exact([0.0, 0.0, -1.0]);
    }
    let longitude = degree_bound_to_radians(longitude)?;
    let latitude = degree_bound_to_radians(latitude)?;
    let (sin_longitude, cos_longitude) = sin_cos(longitude)?;
    let (sin_latitude, cos_latitude) = sin_cos(latitude)?;
    let (sin_longitude, cos_longitude) = (sin_longitude.bound()?, cos_longitude.bound()?);
    let (sin_latitude, cos_latitude) = (sin_latitude.bound()?, cos_latitude.bound()?);
    Some(Vec3Bound {
        x: cos_latitude.mul(cos_longitude)?,
        y: cos_latitude.mul(sin_longitude)?,
        z: sin_latitude,
    })
}

#[cfg(test)]
const fn pole_key(latitude: f64) -> Option<PhysicalEndpointKey> {
    if latitude.to_bits() == 90.0_f64.to_bits() {
        Some(PhysicalEndpointKey::NorthPole)
    } else if latitude.to_bits() == (-90.0_f64).to_bits() {
        Some(PhysicalEndpointKey::SouthPole)
    } else {
        None
    }
}

/// Component-wise outward enclosure of a three-dimensional vector.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Vec3Bound {
    pub(crate) x: Bound,
    pub(crate) y: Bound,
    pub(crate) z: Bound,
}

impl Vec3Bound {
    fn exact(values: [f64; 3]) -> Option<Self> {
        Some(Self {
            x: Bound::exact(values[0])?,
            y: Bound::exact(values[1])?,
            z: Bound::exact(values[2])?,
        })
    }

    fn add(self, rhs: Self) -> Option<Self> {
        Some(Self {
            x: self.x.add(rhs.x)?,
            y: self.y.add(rhs.y)?,
            z: self.z.add(rhs.z)?,
        })
    }

    fn sub(self, rhs: Self) -> Option<Self> {
        Some(Self {
            x: self.x.sub(rhs.x)?,
            y: self.y.sub(rhs.y)?,
            z: self.z.sub(rhs.z)?,
        })
    }

    fn neg(self) -> Self {
        Self {
            x: self.x.neg(),
            y: self.y.neg(),
            z: self.z.neg(),
        }
    }

    fn scale(self, scalar: Bound) -> Option<Self> {
        Some(Self {
            x: self.x.mul(scalar)?,
            y: self.y.mul(scalar)?,
            z: self.z.mul(scalar)?,
        })
    }

    fn dot(self, rhs: Self) -> Option<Bound> {
        self.x
            .mul(rhs.x)?
            .add(self.y.mul(rhs.y)?)?
            .add(self.z.mul(rhs.z)?)
    }

    fn squared_norm(self) -> Option<Bound> {
        self.x
            .square()?
            .add_nonnegative(self.y.square()?)?
            .add_nonnegative(self.z.square()?)
    }

    fn cross(self, rhs: Self) -> Option<Self> {
        Some(Self {
            x: self.y.mul(rhs.z)?.sub(self.z.mul(rhs.y)?)?,
            y: self.z.mul(rhs.x)?.sub(self.x.mul(rhs.z)?)?,
            z: self.x.mul(rhs.y)?.sub(self.y.mul(rhs.x)?)?,
        })
    }

    fn normalized(self) -> Option<Self> {
        let squared = self.squared_norm()?;
        let norm = squared.sqrt()?;
        if norm.contains_zero() {
            return None;
        }
        Some(Self {
            x: self.x.div(norm)?,
            y: self.y.div(norm)?,
            z: self.z.div(norm)?,
        })
    }
}

/// Produces a continuous longitude branch for an ordered minor arc.  `atan2`
/// only proposes adjacent binary64 endpoints; every candidate is accepted
/// solely after certified sine/cosine, cross, and dot signs enclose the raw
/// ray.  The returned span is therefore a carrier bound, not a rounded public
/// latitude/longitude shortcut.
#[cfg(test)]
fn directed_arc_longitude(start: Vec3Bound, end: Vec3Bound) -> Option<DirectedLongitudeSpan> {
    let first = certified_vector_longitude(start)?;
    let second = certified_vector_longitude(end)?;
    directed_arc_longitude_from_endpoints(first, second)
}

fn directed_arc_longitude_from_endpoints(
    first: Bound,
    second: Bound,
) -> Option<DirectedLongitudeSpan> {
    let tau = Bound::new(2.0 * PI_LO, 2.0 * PI_HI)?;
    let mut selected = None;
    for turn in [-1_i8, 0, 1] {
        let shifted = second.add(tau.mul(Bound::exact(f64::from(turn))?)?)?;
        let delta = shifted.sub(first)?;
        // A strict minor branch is the only branch which can represent this
        // ordered piece.  Equality or a zero-containing pi comparison is a
        // structural ambiguity, not a seam-side guess.
        if !strict_minor_longitude_delta(delta) {
            continue;
        }
        let span = DirectedLongitudeSpan {
            start: first,
            end: shifted,
            turn: i64::from(turn),
        };
        if selected.replace(span).is_some() {
            return None;
        }
    }
    selected
}

/// The minor-arc branch is usable only when its full certified enclosure is
/// strictly inside `(-π, π)`.  The lower side must compare with `-PI_LO`:
/// `-PI_HI` is below the mathematical boundary and would admit an enclosure
/// that still reaches a half-circle.
const fn strict_minor_longitude_delta(delta: Bound) -> bool {
    delta.lo > -PI_LO && delta.hi < PI_LO
}

fn certified_vector_longitude(point: Vec3Bound) -> Option<Bound> {
    // These are exact raw-ray identities, not an inverse-trig shortcut.  They
    // give the inverse verifier a canonical branch at the coordinate axes;
    // the general H3 supplier never promotes them to an ExactArcKind.
    if point.y.is_exact_zero() {
        if point.x.strictly_positive() {
            return Bound::exact(0.0);
        }
        if point.x.strictly_negative() {
            return Bound::new(PI_LO, PI_HI);
        }
    }
    if point.x.is_exact_zero() {
        if point.y.strictly_positive() {
            return Bound::new(HALF_PI_LO, HALF_PI_HI);
        }
        if point.y.strictly_negative() {
            return Bound::new(-HALF_PI_HI, -HALF_PI_LO);
        }
    }
    let x = point.x.lo.certified_midpoint(point.x.hi)?;
    let y = point.y.lo.certified_midpoint(point.y.hi)?;
    if !x.is_finite() || !y.is_finite() || (x == 0.0 && y == 0.0) {
        return None;
    }
    // Proposal only. The bounded sign test below is the authority. A native
    // H3 projection can enclose more than 64 adjacent floats, so continue
    // the binary ladder before falling back to a whole-quadrant bisection.
    let proposal = y.atan2(x);
    if !proposal.is_finite() {
        return None;
    }
    // d(atan2(y, x)) = (x dy - y dx) / (x^2 + y^2) sizes the first rung
    // that could hold the enclosure; the sign tests below still decide.
    let spread = (x.abs() * point.y.width() + y.abs() * point.x.width()) / (x * x + y * y);
    for distance in ladder_distances(proposal, spread) {
        let (Some(lower), Some(upper)) = (
            adjacent_float(proposal, -1, distance),
            adjacent_float(proposal, 1, distance),
        ) else {
            continue;
        };
        let (Some(lower_bound), Some(upper_bound)) = (Bound::exact(lower), Bound::exact(upper))
        else {
            continue;
        };
        let (Some((sin_lower, cos_lower)), Some((sin_upper, cos_upper))) = (
            sin_cos_for_inverse(lower_bound),
            sin_cos_for_inverse(upper_bound),
        ) else {
            continue;
        };
        let (Some(lower_cross), Some(upper_cross), Some(lower_dot), Some(upper_dot)) = (
            cos_lower
                .bound()
                .and_then(|cos| cos.mul(point.y))
                .and_then(|value| {
                    sin_lower
                        .bound()
                        .and_then(|sin| sin.mul(point.x))
                        .and_then(|other| value.sub(other))
                }),
            sin_upper
                .bound()
                .and_then(|sin| point.x.mul(sin))
                .and_then(|value| {
                    cos_upper
                        .bound()
                        .and_then(|cos| point.y.mul(cos))
                        .and_then(|other| value.sub(other))
                }),
            cos_lower
                .bound()
                .and_then(|cos| cos.mul(point.x))
                .and_then(|value| {
                    sin_lower
                        .bound()
                        .and_then(|sin| sin.mul(point.y))
                        .and_then(|other| value.add(other))
                }),
            cos_upper
                .bound()
                .and_then(|cos| cos.mul(point.x))
                .and_then(|value| {
                    sin_upper
                        .bound()
                        .and_then(|sin| sin.mul(point.y))
                        .and_then(|other| value.add(other))
                }),
        ) else {
            continue;
        };
        if lower_cross.strictly_positive()
            && upper_cross.strictly_positive()
            && lower_dot.strictly_positive()
            && upper_dot.strictly_positive()
        {
            return Bound::new(lower, upper);
        }
    }
    longitude_bisection_bracket(point)
}

/// Certified latitude enclosure of a unit vector.  As for longitude,
/// `atan2`/`hypot` are proposals only; signs against the raw vector and
/// certified trigonometry establish the final bracket.
fn certified_vector_latitude(point: Vec3Bound) -> Option<Bound> {
    if point.x.is_exact_zero() && point.y.is_exact_zero() {
        if point.z.strictly_positive() {
            return Bound::new(HALF_PI_LO, HALF_PI_HI);
        }
        if point.z.strictly_negative() {
            return Bound::new(-HALF_PI_HI, -HALF_PI_LO);
        }
    }
    let horizontal = point.x.mul(point.x)?.add(point.y.mul(point.y)?)?.sqrt()?;
    if horizontal.contains_zero() {
        return None;
    }
    let h = horizontal.lo.certified_midpoint(horizontal.hi)?;
    let z = point.z.lo.certified_midpoint(point.z.hi)?;
    let proposal = z.atan2(h);
    if !proposal.is_finite() {
        return None;
    }
    let spread = (h * point.z.width() + z.abs() * horizontal.width()) / (h * h + z * z);
    for distance in ladder_distances(proposal, spread) {
        let (Some(lower), Some(upper)) = (
            adjacent_float(proposal, -1, distance),
            adjacent_float(proposal, 1, distance),
        ) else {
            continue;
        };
        let (Some(lower_bound), Some(upper_bound)) = (Bound::exact(lower), Bound::exact(upper))
        else {
            continue;
        };
        let (Some((sin_lower, cos_lower)), Some((sin_upper, cos_upper))) = (
            sin_cos_for_inverse(lower_bound),
            sin_cos_for_inverse(upper_bound),
        ) else {
            continue;
        };
        let (Some(lower_cross), Some(upper_cross), Some(lower_dot), Some(upper_dot)) = (
            cos_lower
                .bound()
                .and_then(|cos| cos.mul(point.z))
                .and_then(|value| {
                    sin_lower
                        .bound()
                        .and_then(|sin| sin.mul(horizontal))
                        .and_then(|other| value.sub(other))
                }),
            sin_upper
                .bound()
                .and_then(|sin| horizontal.mul(sin))
                .and_then(|value| {
                    cos_upper
                        .bound()
                        .and_then(|cos| point.z.mul(cos))
                        .and_then(|other| value.sub(other))
                }),
            cos_lower
                .bound()
                .and_then(|cos| cos.mul(horizontal))
                .and_then(|value| {
                    sin_lower
                        .bound()
                        .and_then(|sin| sin.mul(point.z))
                        .and_then(|other| value.add(other))
                }),
            cos_upper
                .bound()
                .and_then(|cos| cos.mul(horizontal))
                .and_then(|value| {
                    sin_upper
                        .bound()
                        .and_then(|sin| sin.mul(point.z))
                        .and_then(|other| value.add(other))
                }),
        ) else {
            continue;
        };
        if lower_cross.strictly_positive()
            && upper_cross.strictly_positive()
            && lower_dot.strictly_positive()
            && upper_dot.strictly_positive()
        {
            return Bound::new(lower, upper);
        }
    }
    latitude_bisection_bracket(point, horizontal)
}

/// Distances, in floats around `proposal`, of the inverse ladder's rungs:
/// powers of two from the first one at least the enclosure's estimated
/// `spread` in radians, while a rung stays narrower than a quadrant.
/// Narrower rungs could not hold the enclosure; wider ones are the bisection's
/// job.
fn ladder_distances(proposal: f64, spread: f64) -> impl Iterator<Item = u64> {
    let ulp = ulp(proposal).unwrap_or(f64::INFINITY);
    let needed = (spread / ulp).max(1.0);
    let first = if needed < (1_u64 << 62) as f64 {
        (needed.ceil() as u64).next_power_of_two()
    } else {
        u64::MAX
    };
    std::iter::successors(Some(first), |&rung| rung.checked_mul(2))
        .take_while(move |&rung| (rung as f64) * ulp < std::f64::consts::FRAC_PI_2)
}

/// The specified inverse ladder starts at 1,2,...,64 neighbouring floats.
/// A raw H3 carrier can be wider than that proposal bracket, so the fixed
/// second stage finds the same zero from the certified sign quadrant.  The
/// signs at the axes are exact identities; every interior split uses the
/// degree-27/28 outward trig enclosure and requires a positive dot product.
fn longitude_bisection_bracket(point: Vec3Bound) -> Option<Bound> {
    let (lower, upper) = match (strict_sign(point.x), strict_sign(point.y)) {
        (Some(CertSign::Positive), Some(CertSign::Positive)) => {
            (Bound::exact(0.0)?, Bound::new(HALF_PI_LO, HALF_PI_HI)?)
        },
        (Some(CertSign::Negative), Some(CertSign::Positive)) => (
            Bound::new(HALF_PI_LO, HALF_PI_HI)?,
            Bound::new(PI_LO, PI_HI)?,
        ),
        (Some(CertSign::Negative), Some(CertSign::Negative)) => (
            Bound::new(-PI_HI, -PI_LO)?,
            Bound::new(-HALF_PI_HI, -HALF_PI_LO)?,
        ),
        (Some(CertSign::Positive), Some(CertSign::Negative)) => {
            (Bound::new(-HALF_PI_HI, -HALF_PI_LO)?, Bound::exact(0.0)?)
        },
        _ => return None,
    };
    bisect_inverse_bracket(lower, upper, |angle| {
        let (sin, cos) = sin_cos_for_inverse(angle)?;
        let sin = sin.bound()?;
        let cos = cos.bound()?;
        let cross = cos.mul(point.y)?.sub(sin.mul(point.x)?)?;
        let dot = cos.mul(point.x)?.add(sin.mul(point.y)?)?;
        dot.strictly_positive().then_some(cross)
    })
}

fn latitude_bisection_bracket(point: Vec3Bound, horizontal: Bound) -> Option<Bound> {
    if point.z.is_exact_zero() {
        return Bound::exact(0.0);
    }
    let (lower, upper) = match strict_sign(point.z) {
        Some(CertSign::Positive) => (Bound::exact(0.0)?, Bound::new(HALF_PI_LO, HALF_PI_HI)?),
        Some(CertSign::Negative) => (Bound::new(-HALF_PI_HI, -HALF_PI_LO)?, Bound::exact(0.0)?),
        Some(CertSign::Uncertain) | None => return None,
    };
    bisect_inverse_bracket(lower, upper, |angle| {
        let (sin, cos) = sin_cos_for_inverse(angle)?;
        let sin = sin.bound()?;
        let cos = cos.bound()?;
        let cross = cos.mul(point.z)?.sub(sin.mul(horizontal)?)?;
        let dot = cos.mul(horizontal)?.add(sin.mul(point.z)?)?;
        dot.strictly_positive().then_some(cross)
    })
}

/// A fixed, monotone inverse-angle proof.  The initial bounds are structural
/// axis identities with positive/negative cross signs.  If an interior sign
/// becomes ambiguous, the current enclosing bracket remains the soundest
/// result; only an absent or inconsistent initial proof returns `None`.
fn bisect_inverse_bracket(
    mut lower: Bound,
    mut upper: Bound,
    cross: impl Fn(Bound) -> Option<Bound>,
) -> Option<Bound> {
    for _ in 0..64 {
        let midpoint = lower.hi.certified_midpoint(upper.lo)?;
        if !(lower.hi < midpoint && midpoint < upper.lo) {
            break;
        }
        let midpoint = Bound::exact(midpoint)?;
        match strict_sign(cross(midpoint)?) {
            Some(CertSign::Positive) => lower = midpoint,
            Some(CertSign::Negative) => upper = midpoint,
            Some(CertSign::Uncertain) | None => break,
        }
    }
    Bound::new(lower.lo, upper.hi)
}

fn adjacent_float(mut value: f64, direction: i8, count: u64) -> Option<f64> {
    const SIGN: u64 = 1_u64 << 63;
    if !value.is_finite() || count == 0 {
        return (count == 0).then_some(value);
    }
    let steps = if value == 0.0 {
        value = match direction {
            -1 => f64::from_bits(SIGN | 1),
            1 => f64::from_bits(1),
            _ => return None,
        };
        if count == 1 {
            return Some(value);
        }
        count - 1
    } else {
        count
    };
    let bits = value.to_bits();
    let rank = if value.is_sign_negative() {
        !bits
    } else {
        bits | SIGN
    };
    let rank = match direction {
        -1 => rank.checked_sub(steps)?,
        1 => rank.checked_add(steps)?,
        _ => return None,
    };
    let bits = if rank & SIGN == 0 {
        !rank
    } else {
        rank & !SIGN
    };
    let result = f64::from_bits(bits);
    result.is_finite().then_some(result)
}

/// Replays a vendor H3 seed with certified primitive arithmetic.  It follows
/// the native projection algebra directly and never converts the public
/// `LatLng` back into a predicate carrier.
pub(crate) fn project_h3_seed(seed: &ProjectionSeed) -> Option<Vec3Bound> {
    project_h3_seed_unnormalized(seed)?.normalized()
}

/// Certified native H3 projection before the final unit-vector normalization.
/// A3's seam proof compares this affine carrier in its paired face frames;
/// normalizing before subtraction would erase precisely the chart discrepancy
/// the cap must charge.
fn project_h3_seed_unnormalized(seed: &ProjectionSeed) -> Option<Vec3Bound> {
    let frame = seed.projection_frame();
    let (resolution, substrate) = match seed {
        ProjectionSeed::Point {
            resolution,
            substrate,
            ..
        }
        | ProjectionSeed::Intersection {
            resolution,
            substrate,
            ..
        } => (resolution, substrate),
    };
    let [x, y] = seed_native_point(seed, frame.sqrt3_2)?;
    let center = Vec3Bound::exact(frame.center)?;
    let north_axis = Vec3Bound::exact([0.0, 0.0, 1.0])?;
    let north = north_axis.sub(center.scale(center.z)?)?.normalized()?;
    let east = north.cross(center)?;

    let angle = if !substrate && resolution % 2 == 1 {
        Bound::exact(frame.axis_az_rads_cii)?.sub(Bound::exact(frame.ap7_rot_rads)?)?
    } else {
        Bound::exact(frame.axis_az_rads_cii)?
    };
    let (sin_axis, cos_axis) = sin_cos(angle)?;
    let sin_axis = sin_axis.bound()?;
    let cos_axis = cos_axis.bound()?;
    let north_coeff = cos_axis.mul(x)?.add(sin_axis.mul(y)?)?;
    let east_coeff = sin_axis.mul(x)?.sub(cos_axis.mul(y)?)?;
    let basis = north.scale(north_coeff)?.add(east.scale(east_coeff)?)?;
    let mut scale = Bound::exact(frame.res0_u_gnomonic)?.mul(Bound::exact(frame.inv_sqrt7)?)?;
    if *substrate {
        scale = scale.mul(Bound::exact(frame.one_third)?)?;
    }
    center.add(basis.scale(scale)?)
}

fn seed_native_point(seed: &ProjectionSeed, sqrt3_2: f64) -> Option<[Bound; 2]> {
    let (x2, y_sqrt3) = match seed {
        ProjectionSeed::Point { point, .. } => (
            Bound::from_i128(i128::from(point.x2))?,
            Bound::from_i128(i128::from(point.y_sqrt3))?,
        ),
        ProjectionSeed::Intersection {
            segment, face_edge, ..
        } => intersection_native_point(*segment, *face_edge)?,
    };
    Some([
        x2.mul(Bound::exact(0.5)?)?,
        y_sqrt3.mul(Bound::exact(sqrt3_2)?)?,
    ])
}

fn intersection_native_point(
    segment: [Hex2Int; 2],
    face_edge: [Hex2Int; 2],
) -> Option<(Bound, Bound)> {
    let sx = i128::from(segment[0].x2);
    let sy = i128::from(segment[0].y_sqrt3);
    let dx = i128::from(segment[1].x2).checked_sub(sx)?;
    let dy = i128::from(segment[1].y_sqrt3).checked_sub(sy)?;
    let ex = i128::from(face_edge[0].x2);
    let ey = i128::from(face_edge[0].y_sqrt3);
    let edx = i128::from(face_edge[1].x2).checked_sub(ex)?;
    let edy = i128::from(face_edge[1].y_sqrt3).checked_sub(ey)?;
    let (numerator, denominator) = rational_intersection_parameter(
        edx,
        edy,
        dx,
        dy,
        ex.checked_sub(sx)?,
        ey.checked_sub(sy)?,
    )?;
    let t = Bound::from_i128(numerator)?.div(Bound::from_i128(denominator)?)?;
    let x2 = Bound::from_i128(sx)?.add(Bound::from_i128(dx)?.mul(t)?)?;
    let y_sqrt3 = Bound::from_i128(sy)?.add(Bound::from_i128(dy)?.mul(t)?)?;
    Some((x2, y_sqrt3))
}

/// Exact raw-seed segment parameter with a positive denominator.  An
/// arithmetic failure is an undecidable carrier, not wrapped lattice data.
#[expect(
    clippy::similar_names,
    reason = "the determinant is defined over paired x/y edge, segment, and start deltas"
)]
fn rational_intersection_parameter(
    edge_dx: i128,
    edge_dy: i128,
    segment_dx: i128,
    segment_dy: i128,
    start_dx: i128,
    start_dy: i128,
) -> Option<(i128, i128)> {
    let mut denominator = cross2(edge_dx, edge_dy, segment_dx, segment_dy)?;
    if denominator == 0 {
        return None;
    }
    let mut numerator = cross2(edge_dx, edge_dy, start_dx, start_dy)?;
    if denominator < 0 {
        numerator = numerator.checked_neg()?;
        denominator = denominator.checked_neg()?;
    }
    let divisor = gcd_i128(numerator, denominator);
    Some((numerator / divisor, denominator / divisor))
}

fn gcd_i128(numerator: i128, denominator: i128) -> i128 {
    debug_assert!(denominator > 0);
    let mut left = numerator.unsigned_abs();
    let mut right = denominator as u128;
    while right != 0 {
        (left, right) = (right, left % right);
    }
    // The denominator is positive i128, therefore its divisor is representable
    // as i128 even when the numerator is i128::MIN.
    left as i128
}

fn cross2(ax: i128, ay: i128, bx: i128, by: i128) -> Option<i128> {
    ax.checked_mul(by)?.checked_sub(ay.checked_mul(bx)?)
}

/// Certified sine/cosine of a bounded radian argument.  The returned balls
/// enclose mathematical values; inability to prove a unique reduction is an
/// ordinary fail-open result (`None`).
pub(crate) fn sin_cos(input: Bound) -> Option<(Ball, Ball)> {
    let (reduced, quadrant) = reduce_quadrant(input)?;
    let (sin, cos) = sin_cos_small(reduced)?;
    match quadrant.rem_euclid(4) {
        0 => Some((sin, cos)),
        1 => Some((cos, signed_ball(sin, -1)?)),
        2 => Some((signed_ball(sin, -1)?, signed_ball(cos, -1)?)),
        3 => Some((signed_ball(cos, -1)?, sin)),
        _ => unreachable!("remainder modulo four is in 0..4"),
    }
}

/// The inverse-bracket verifier may need a stricter enclosure than the B3
/// scalar fast filter's fixed 128-epsilon radius.  It keeps the same certified
/// reduction and quadrant proof, then uses only the outward degree-27/28
/// evaluator.  This is a bounded proof refinement, never a libm fallback.
fn sin_cos_for_inverse(input: Bound) -> Option<(Ball, Ball)> {
    let (reduced, quadrant) = reduce_quadrant(input)?;
    let (sin, cos) = sin_cos_fallback(reduced)?;
    match quadrant.rem_euclid(4) {
        0 => Some((sin, cos)),
        1 => Some((cos, signed_ball(sin, -1)?)),
        2 => Some((signed_ball(sin, -1)?, signed_ball(cos, -1)?)),
        3 => Some((signed_ball(cos, -1)?, sin)),
        _ => unreachable!("remainder modulo four is in 0..4"),
    }
}

/// Degree-13/14 fast polynomial over the certified `[-pi/4, pi/4]` range.
/// The error radius follows the frozen bound: input enclosure + the derived
/// 128 ulps.  There is intentionally no libm call in this path.
fn sin_cos_small(input: Bound) -> Option<(Ball, Ball)> {
    if input.width() > 1.0 / 1_048_576.0 || input.lo < -QUARTER_PI_LO || input.hi > QUARTER_PI_HI {
        return sin_cos_fallback(input);
    }
    let x = input.lo.certified_midpoint(input.hi)?;
    let input_radius = outward_half_width(input)?;
    if x.abs() > QUARTER_PI_HI || !x.is_finite() {
        return None;
    }
    let z = x * x;
    let sin = x * horner(
        z,
        [
            -1.0 / 6.0,
            1.0 / 120.0,
            -1.0 / 5040.0,
            1.0 / 362_880.0,
            -1.0 / 39_916_800.0,
            1.0 / 6_227_020_800.0,
            -1.0 / 1_307_674_368_000.0,
        ],
        1.0,
    );
    let cos = horner(
        z,
        [
            -1.0 / 2.0,
            1.0 / 24.0,
            -1.0 / 720.0,
            1.0 / 40_320.0,
            -1.0 / 3_628_800.0,
            1.0 / 479_001_600.0,
            -1.0 / 87_178_291_200.0,
        ],
        1.0,
    );
    let radius = upward_sum([input_radius, 128.0 * f64::EPSILON])?;
    Some((Ball::new(sin, radius)?, Ball::new(cos, radius)?))
}

// Binary64 brackets of the large factorial denominators.  Writing the decimal
// integers directly would quietly round them before division; the explicit
// bits keep that fact visible.
const FACT_22: f64 = f64::from_bits(0x444E_7752_6159_F06C);
const FACT_23: f64 = f64::from_bits(0x4495_E5C3_35F8_A4CE);
const FACT_24: f64 = f64::from_bits(0x44E0_6C52_687A_7B9A);
const FACT_25: f64 = f64::from_bits(0x4529_A940_C33F_6121);
const FACT_26: f64 = f64::from_bits(0x4574_D984_9EA3_7EEB);
const FACT_27: f64 = f64::from_bits(0x45C1_9787_E5D9_F316);
const FACT_28: f64 = f64::from_bits(0x460E_C92D_D23D_6967);
const FACT_29: f64 = f64::from_bits(0x465B_E651_8687_A785);
const FACT_30: f64 = f64::from_bits(0x46AA_27EC_6E1F_2D0D);

/// `k!` in binary64 for `k` in `2..=30`, each within half an ulp of the
/// integer it denotes (exact through `18!`).
const FACTORIALS: [f64; 29] = [
    2.0,
    6.0,
    24.0,
    120.0,
    720.0,
    5_040.0,
    40_320.0,
    362_880.0,
    3_628_800.0,
    39_916_800.0,
    479_001_600.0,
    6_227_020_800.0,
    87_178_291_200.0,
    1_307_674_368_000.0,
    20_922_789_888_000.0,
    355_687_428_096_000.0,
    6_402_373_705_728_000.0,
    121_645_100_408_832_000.0,
    2_432_902_008_176_640_000.0,
    51_090_942_171_709_440_000.0,
    FACT_22,
    FACT_23,
    FACT_24,
    FACT_25,
    FACT_26,
    FACT_27,
    FACT_28,
    FACT_29,
    FACT_30,
];

/// Outward enclosures of `1/k!`: the neighbouring floats of the binary64
/// `k!` enclose the integer, and each reciprocal is rounded outward.
const RECIPROCAL_FACTORIALS: [Bound; 29] = {
    let mut table = [Bound { lo: 0.0, hi: 0.0 }; 29];
    let mut index = 0;
    while index < FACTORIALS.len() {
        let factorial = FACTORIALS[index];
        table[index] = Bound {
            lo: (1.0 / factorial.next_up()).next_down(),
            hi: (1.0 / factorial.next_down()).next_up(),
        };
        index += 1;
    }
    table
};

/// `(-1)^(k/2) / k!` for the Taylor term of order `k`.
fn taylor_coefficient(order: usize) -> Bound {
    let value = RECIPROCAL_FACTORIALS[order - 2];
    if (order / 2) % 2 == 1 {
        value.neg()
    } else {
        value
    }
}

/// Bounded high-degree fallback for a wide, but still uniquely reduced,
/// interval.  The next alternating term is included symmetrically, so this
/// path remains a proof instead of a retry loop with a fitted tolerance.
fn sin_cos_fallback(input: Bound) -> Option<(Ball, Ball)> {
    // A uniquely chosen quadrant may still enclose an artificial +/-pi/4
    // reduction edge because every pi operation is outward.  The degree-27/28
    // alternating proof remains valid through pi/2, so retain the enclosure
    // instead of silently selecting a rounded side of that non-structural cut.
    if input.lo < -HALF_PI_LO || input.hi > HALF_PI_HI {
        return None;
    }
    let z = input.mul(input)?;
    let sin = input.mul(alternating_horner(z, (3..28).step_by(2))?)?;
    let cos = alternating_horner(z, (2..29).step_by(2))?;
    let absolute = input.abs();
    let sin_remainder = integer_power_bound(absolute, 29)?.mul(RECIPROCAL_FACTORIALS[29 - 2])?;
    let cos_remainder = integer_power_bound(absolute, 30)?.mul(RECIPROCAL_FACTORIALS[30 - 2])?;
    let sin = widen_symmetric(sin, sin_remainder)?;
    let cos = widen_symmetric(cos, cos_remainder)?;
    Some((Ball::from_bound(sin)?, Ball::from_bound(cos)?))
}

/// Horner evaluation of `1 + z*(c_a + z*(c_b + ...))` over the Taylor
/// coefficients of the given orders. This is the fallback's outward interval
/// path, deliberately separate from B3's compact scalar-plus-radius fast
/// filter.
fn alternating_horner(z: Bound, orders: impl DoubleEndedIterator<Item = usize>) -> Option<Bound> {
    let mut orders = orders.rev();
    let mut value = taylor_coefficient(orders.next()?);
    for order in orders {
        value = taylor_coefficient(order).add(z.mul(value)?)?;
    }
    Bound::exact(1.0)?.add(z.mul(value)?)
}

fn integer_power_bound(mut base: Bound, mut exponent: u8) -> Option<Bound> {
    let mut result = Bound::exact(1.0)?;
    while exponent != 0 {
        if exponent & 1 != 0 {
            result = result.mul(base)?;
        }
        exponent >>= 1;
        if exponent != 0 {
            base = base.mul(base)?;
        }
    }
    Some(result)
}

fn widen_symmetric(value: Bound, radius: Bound) -> Option<Bound> {
    if radius.lo < 0.0 {
        return None;
    }
    Bound::new(down(value.lo - radius.hi)?, up(value.hi + radius.hi)?)
}

/// Reduces a bounded value to a single quadrant.  `round()` is only a proposed
/// integer; the enclosing subtraction proves that it is the unique period.
fn reduce_quadrant(input: Bound) -> Option<(Bound, i64)> {
    let tau = Bound::new(2.0 * PI_LO, 2.0 * PI_HI)?;
    let midpoint = input.lo.certified_midpoint(input.hi)?;
    let period = (midpoint / (2.0 * PI_LO)).round();
    if !period.is_finite() || period.abs() > i64::MAX as f64 {
        return None;
    }
    let period = period as i64;
    let reduced = input.sub(tau.mul(Bound::exact(period as f64)?)?)?;
    if reduced.lo < -PI_LO || reduced.hi > PI_LO {
        return None;
    }
    // An enclosure which meets a non-zero multiple of 2*pi has no unique
    // periodic branch.  Exact zero is structural and remains a valid direct
    // sine/cosine input; `Bound` preserves it rather than widening 0*anything.
    if period != 0 && reduced.contains_zero() {
        return None;
    }

    // The second reduction is independent proof that the input lies in one
    // (and only one) quadrant.  A boundary-spanning enclosure is deliberately
    // undecidable rather than split at a rounded multiple of pi/2.
    let midpoint = reduced.lo.certified_midpoint(reduced.hi)?;
    let quadrant = (midpoint / HALF_PI_LO).round();
    if !quadrant.is_finite() || quadrant.abs() > i64::MAX as f64 {
        return None;
    }
    let quadrant = quadrant as i64;
    let quadrant_turn = Bound::new(HALF_PI_LO, HALF_PI_HI)?.mul(Bound::exact(quadrant as f64)?)?;
    // A value meeting a multiple of pi/2 has no unique quadrant.  In
    // particular, do not quietly choose a side merely because `round()` made
    // one plausible for the interval midpoint.
    if quadrant != 0 && reduced.intersection(quadrant_turn).is_some() {
        return None;
    }
    let small = reduced.sub(quadrant_turn)?;
    if quadrant == 0 && small.contains_zero() && !small.is_exact_zero() {
        return None;
    }
    (small.lo >= -HALF_PI_LO && small.hi <= HALF_PI_HI).then_some((small, quadrant))
}

fn signed_ball(value: Ball, sign: i8) -> Option<Ball> {
    match sign {
        1 => Some(value),
        -1 => Ball::new(-value.mid, value.rad),
        _ => None,
    }
}

fn horner<const N: usize>(x: f64, coefficients: [f64; N], constant: f64) -> f64 {
    let mut reversed = coefficients.into_iter().rev();
    let mut value = reversed
        .next()
        .expect("fixed nonempty polynomial coefficient table");
    for coefficient in reversed {
        value = coefficient + x * value;
    }
    constant + x * value
}

fn finite_minmax(values: [f64; 4]) -> Option<(f64, f64)> {
    let mut iter = values.into_iter();
    let first = iter.next()?;
    if !first.is_finite() {
        return None;
    }
    iter.try_fold((first, first), |(lo, hi), value| {
        value.is_finite().then_some((lo.min(value), hi.max(value)))
    })
}

fn up(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.next_up())
}

fn down(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.next_down())
}

fn ulp(value: f64) -> Option<f64> {
    if !value.is_finite() {
        return None;
    }
    Some((value.next_up() - value).max(value - value.next_down()))
}

fn upward_sum<const N: usize>(values: [f64; N]) -> Option<f64> {
    let mut sum = 0.0;
    for value in values {
        if !value.is_finite() || value < 0.0 {
            return None;
        }
        sum = up(sum + value)?;
    }
    Some(sum)
}

fn outward_half_width(value: Bound) -> Option<f64> {
    up((value.hi - value.lo) * 0.5)
}

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

#[cfg(test)]
mod tests;
