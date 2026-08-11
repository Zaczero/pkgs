//! Exact affine lon/lat source topology shared by the spherical grid lanes.
//!
//! This deliberately owns the source lift rather than turning source edges
//! into great circles. H3 and S2 classifiers consume the resulting parent
//! intervals and retain the unsplit affine source as their authority.
//!
//! # This file is NOT to be split by line count (audited 2026-08-03)
//!
//! Two independent reviews proposed splitting these ~4,000 lines into five
//! concern-modules, on the claim that — unlike the sibling `spherical_arc.rs`,
//! whose split was attempted and REVERTED — this file has no private-field
//! blocker. That claim is false; the blocker here is worse, and in every
//! direction:
//!
//! - Cross-concern private-field reads: `step.turn_delta` (rectangle reads
//!   lift), `vertex.longitude.raw`/`.turns` (exact-point reads lift),
//!   `chain.edges`/`.vertices` (classifier reads lift),
//!   `.numerator.ordering()` (selection reads substrate),
//!   `self.start.longitude.raw` (edge reads lift), `piece.arc`/`.selection`
//!   (facade reads selection — directly, though `SelectionAffinePiece::arc()`
//!   exists), and `append_lifted_chain` reading `LiftedChain`.
//! - The module tests read ~50 private fields spanning every group — and
//!   `SymbolicAffineEdge::endpoints` is `#[cfg_attr(not(test), expect(dead_code,
//!   …))]`, i.e. a field that exists ONLY so the tests can read it.
//!
//! Splitting therefore requires widening essentially the whole private state
//! of the module to satisfy a line count. The failure mode is not a red test:
//! once (say) `LiftedChain.vertices` is reachable from a sibling module, a
//! later edit can reconstruct a rounded closure from raw endpoint doubles,
//! which this file's own invariants forbid — surfacing months later as a wrong
//! H3/S2 cover on a polar or seam-crossing source. Nothing in the suite catches
//! that class.
//!
//! The one genuinely field-clean piece (`ExactExpansion` + its sum helpers,
//! ~230 lines) is 5.7% of the file and sits under `ExactRatio`, which IS read
//! cross-group. Not worth a module boundary in this file.

use std::cmp::Ordering;
use std::collections::TryReserveError;

use h3o::Resolution;

use crate::boundary::geographic::{
    normalize_accepted_geographic_latitudes, normalized_geographic_pole,
};
use crate::geometry::{Bounds, CoordSeq, Coordinates as _, Shape};
use crate::grid::s2::cell::Cell as S2Cell;
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::projection::lonlat_to_point as s2_lonlat_to_point;
use crate::grid::spherical_arc::{
    AffineEndpointIdentity, AffineParentMap, AffineSourceArc, AffineStructure, AxisLatitude,
    AxisMeridian, Bound, CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
    PhysicalEndpointKey, SourceEndpointRole, degree_bound_to_radians,
};

const EXPANSION_LIMBS: usize = 64;
const FULL_TURN_DEGREES: f64 = 360.0;
const HALF_TURN_DEGREES: f64 = 180.0;

/// Canonical grid-topology spelling of a geographically admitted source.
///
/// The latitude admission sliver at either pole names the pole itself.  Every
/// grid entry point must derive its working topology, affine authority, and
/// container decomposition from this one image; normalizing only an internal
/// certificate leaves sibling paths free to make representation-dependent
/// decisions from the pre-normalized shape.
pub(crate) fn normalize_grid_source(shape: &Shape) -> Shape {
    normalize_accepted_geographic_latitudes(shape)
}

/// Visit the atomic source components used by spherical grid enumeration.
///
/// A collection is syntax, not a second coverage algorithm: each grid cover
/// is the deduplicated union of these atomic covers.  In particular a
/// one-member `MultiPolygon` and a bare `Polygon` must reach the identical
/// rectangle certificate and affine carrier.  Polygon rings remain atomic so
/// shell/hole authority is never split.
pub(crate) fn visit_grid_cover_components<E>(
    shape: &Shape,
    visit: &mut impl FnMut(&Shape) -> Result<(), E>,
) -> Result<(), E> {
    match shape {
        Shape::MultiPoint(points) => {
            for point in points {
                visit(&Shape::Point(point))?;
            }
        },
        Shape::MultiLineString(lines) => {
            for line in lines {
                visit(&Shape::LineString(line.clone()))?;
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                visit(&Shape::Polygon(polygon.clone()))?;
            }
        },
        Shape::GeometryCollection(parts) => {
            for part in parts {
                visit_grid_cover_components(part, visit)?;
            }
        },
        Shape::Empty(..) => {},
        _ => visit(shape)?,
    }
    Ok(())
}

/// The source carrier is shared between spherical grid systems; only native
/// raw-owner witnesses differ later in traversal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SphericalGridTarget {
    H3(Resolution),
    S2,
}

/// Relationship between the retained source authority and the temporary
/// planar working shape used by a grid traversal.
///
/// An antimeridian split is deliberately explicit: it is useful working
/// topology, but it never replaces the unsplit affine source as authority for
/// a negative certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkingShapeRelation {
    Identity,
    AntimeridianSplit,
}

/// Exact relationship of one degree point to the retained affine authority.
/// `Unknown` is intentionally distinct from boundary: both fail open at a
/// caller, while only the latter is a proven closed-set contact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GridPointClass {
    Exterior,
    Boundary,
    Interior,
    Unknown,
}

/// Source relation to a certified degree rectangle.  Every unknown proof is
/// boundary; only a complete exact separation can prune a H3 subtree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RectClass {
    Outside,
    Boundary,
    Interior,
}

/// A source witness carried as a certified degree enclosure.  Stored source
/// doubles use exact singleton bounds; symbolic selection transforms retain
/// their exact expansion through the enclosing bound instead of materializing
/// a rounded split coordinate.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GridDegreePoint {
    pub(crate) longitude: Bound,
    pub(crate) latitude: Bound,
}

impl GridDegreePoint {
    fn from_stored(longitude: f64, latitude: f64) -> Option<Self> {
        Some(Self {
            longitude: Bound::exact(longitude)?,
            latitude: Bound::exact(latitude)?,
        })
    }

    fn from_expansions(longitude: &ExactExpansion, latitude: &ExactExpansion) -> Option<Self> {
        Some(Self {
            longitude: degree_expansion_bound(longitude)?,
            latitude: degree_expansion_bound(latitude)?,
        })
    }

    pub(crate) const fn is_proven_physical_latitude(self) -> bool {
        -90.0 <= self.latitude.lo && self.latitude.hi <= 90.0
    }
}

fn degree_bounds_overlap(left: Bound, right: Bound) -> bool {
    left.hi >= right.lo && right.hi >= left.lo
}

/// Classify a certified H3 degree window against an exact canonical source
/// rectangle. Bounds come directly from stored source doubles, while the
/// candidate is outward, so strict separation is a negative certificate and
/// closed containment is an interior certificate. Every other case retains
/// the candidate.
fn classify_exact_axis_rectangle(bounds: Bounds, candidate: DegreeWindowResult) -> RectClass {
    let Some(source_longitude) = Bound::new(bounds.minx(), bounds.maxx()) else {
        return RectClass::Boundary;
    };
    let Some(source_latitude) = Bound::new(bounds.miny(), bounds.maxy()) else {
        return RectClass::Boundary;
    };
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude,
    }) = candidate
    else {
        return RectClass::Boundary;
    };

    if strictly_disjoint_degree_bounds(source_latitude, latitude) {
        return RectClass::Outside;
    }
    let latitude_contained = closed_degree_bounds_contain(source_latitude, latitude);
    match longitude {
        CertifiedLongitudeDegrees::Full => {
            if latitude_contained
                && bounds.minx().to_bits() == (-HALF_TURN_DEGREES).to_bits()
                && bounds.maxx().to_bits() == HALF_TURN_DEGREES.to_bits()
            {
                RectClass::Interior
            } else {
                RectClass::Boundary
            }
        },
        CertifiedLongitudeDegrees::One(longitude) => {
            classify_exact_axis_window(source_longitude, source_latitude, longitude, latitude)
        },
        CertifiedLongitudeDegrees::Two([west, east]) => {
            let west =
                classify_exact_axis_window(source_longitude, source_latitude, west, latitude);
            let east =
                classify_exact_axis_window(source_longitude, source_latitude, east, latitude);
            if west == RectClass::Outside && east == RectClass::Outside {
                RectClass::Outside
            } else if west == RectClass::Interior && east == RectClass::Interior {
                RectClass::Interior
            } else {
                RectClass::Boundary
            }
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RectangleSide {
    South,
    East,
    North,
    West,
}

impl RectangleSide {
    const fn next_counterclockwise(self) -> Self {
        match self {
            Self::South => Self::East,
            Self::East => Self::North,
            Self::North => Self::West,
            Self::West => Self::South,
        }
    }

    const fn next_clockwise(self) -> Self {
        match self {
            Self::South => Self::West,
            Self::West => Self::North,
            Self::North => Self::East,
            Self::East => Self::South,
        }
    }
}

/// A geographic `box` retains its parallel edges as an exact sequence of
/// collinear lon/lat source segments.  It is still a complete rectangle
/// carrier: every segment stays on one physical side and advances through
/// each side once in one orientation.  Reject anything less structured so a
/// generic boundary walk can never promote itself into a cap certificate.
#[expect(
    clippy::float_cmp,
    reason = "bounds are exact extrema of these stored source coordinates"
)]
#[expect(
    clippy::too_many_lines,
    reason = "the complete four-side certificate stays together so no partial traversal can certify a cap"
)]
fn subdivided_axis_aligned_rectangle(source: &Shape, bounds: Bounds) -> bool {
    let Shape::Polygon(polygon) = source else {
        return false;
    };
    if !polygon.holes.is_empty() {
        return false;
    }

    let side = |start: crate::geometry::Point, end: crate::geometry::Point| {
        if start.y == end.y {
            if start.y == bounds.miny() {
                return Some(RectangleSide::South);
            }
            if start.y == bounds.maxy() {
                return Some(RectangleSide::North);
            }
        }
        if start.x == end.x {
            if start.x == bounds.minx() {
                return Some(RectangleSide::West);
            }
            if start.x == bounds.maxx() {
                return Some(RectangleSide::East);
            }
        }
        None
    };

    let mut first = None;
    let mut previous = None;
    let mut direction = None;
    let mut side_changes = 0_u8;
    for [start, end] in polygon.shell.segment_pairs() {
        let Some(current) = side(start, end) else {
            return false;
        };
        if let Some(previous) = previous
            && current != previous
        {
            let next = match direction {
                Some(true) => previous.next_counterclockwise(),
                Some(false) => previous.next_clockwise(),
                None if current == previous.next_counterclockwise() => {
                    direction = Some(true);
                    current
                },
                None if current == previous.next_clockwise() => {
                    direction = Some(false);
                    current
                },
                None => return false,
            };
            if current != next {
                return false;
            }
            side_changes = match side_changes.checked_add(1) {
                Some(changes) => changes,
                None => return false,
            };
        }
        first.get_or_insert(current);
        previous = Some(current);
    }
    let (Some(first), Some(last), Some(counterclockwise)) = (first, previous, direction) else {
        return false;
    };
    let closing_next = if counterclockwise {
        last.next_counterclockwise()
    } else {
        last.next_clockwise()
    };
    if closing_next != first || side_changes != 3 {
        return false;
    }

    // The physical side labels alone are not enough at the full-longitude
    // boundary: `-180 -> 170 -> -170 -> 180` has the right extrema but its
    // shortest directed lift is a seam zigzag, not one complete world side.
    // Every stored segment must advance in the one direction selected by the
    // four-side walk.  This is exact source topology, never a normalization
    // convention or a tolerance check.
    let advances = |side: RectangleSide, delta: Ordering| match (counterclockwise, side) {
        (true, RectangleSide::South | RectangleSide::East)
        | (false, RectangleSide::North | RectangleSide::West) => delta == Ordering::Greater,
        (true, RectangleSide::North | RectangleSide::West)
        | (false, RectangleSide::South | RectangleSide::East) => delta == Ordering::Less,
    };
    for [start, end] in polygon.shell.segment_pairs() {
        let Some(side) = side(start, end) else {
            return false;
        };
        let delta = match side {
            RectangleSide::South | RectangleSide::North => {
                let Some(step) = lifted_step(start.x, end.x) else {
                    return false;
                };
                let Some(delta) = ExactExpansion::from_f64(end.x)
                    .and_then(|end| {
                        ExactExpansion::from_f64(start.x).and_then(|start| end.sub(start))
                    })
                    .and_then(|raw| {
                        ExactExpansion::exact_degree_turns(step.turn_delta)
                            .and_then(|turns| raw.add(turns))
                    })
                else {
                    return false;
                };
                delta.ordering()
            },
            RectangleSide::East | RectangleSide::West => {
                let Some(delta) = ExactExpansion::from_f64(end.y).and_then(|end| {
                    ExactExpansion::from_f64(start.y).and_then(|start| end.sub(start))
                }) else {
                    return false;
                };
                delta.ordering()
            },
        };
        if !advances(side, delta) {
            return false;
        }
    }
    true
}

fn exact_axis_rectangle(source: &Shape, bounds: Bounds) -> bool {
    // `Shape::is_axis_aligned_rectangle` checks a corner set, which is useful
    // for generic geometry predicates but cannot certify source traversal: a
    // bow-tie visits all four corners while leaving the rectangle. The
    // side-walk above owns the stronger cap certificate for both four-vertex
    // and geographic-densified rings.
    subdivided_axis_aligned_rectangle(source, bounds)
}

/// A bounds rectangle is a complete source model only on the canonical
/// longitude sheet.  A stored ring from 179 to -179 is geometrically a narrow
/// seam strip in the continuous lift, not the 358-degree rectangle named by
/// its planar extrema. The one wider admissible form is the exact physical
/// full-longitude domain `[-180, 180]`.
fn canonical_axis_rectangle_longitude(bounds: Bounds) -> bool {
    let Some(minimum) = ExactExpansion::from_f64(bounds.minx()) else {
        return false;
    };
    let Some(maximum) = ExactExpansion::from_f64(bounds.maxx()) else {
        return false;
    };
    let Some(width) = maximum.sub(minimum) else {
        return false;
    };
    let Some(half_turn) = ExactExpansion::from_i64(180) else {
        return false;
    };
    let Some(over_half_turn) = width.sub(half_turn).map(ExactExpansion::ordering) else {
        return false;
    };
    if over_half_turn != Ordering::Greater {
        return true;
    }
    minimum.is_exact(-180.0).unwrap_or(false) && maximum.is_exact(180.0).unwrap_or(false)
}

const fn classify_exact_axis_window(
    source_longitude: Bound,
    source_latitude: Bound,
    longitude: Bound,
    latitude: Bound,
) -> RectClass {
    if strictly_disjoint_degree_bounds(source_longitude, longitude)
        || strictly_disjoint_degree_bounds(source_latitude, latitude)
    {
        RectClass::Outside
    } else if closed_degree_bounds_contain(source_longitude, longitude)
        && closed_degree_bounds_contain(source_latitude, latitude)
    {
        RectClass::Interior
    } else {
        RectClass::Boundary
    }
}

const fn strictly_disjoint_degree_bounds(left: Bound, right: Bound) -> bool {
    left.hi < right.lo || right.hi < left.lo
}

const fn closed_degree_bounds_contain(outer: Bound, inner: Bound) -> bool {
    outer.lo <= inner.lo && inner.hi <= outer.hi
}

fn strict_window_representative(window: Bound) -> Option<f64> {
    let value = window.lo + (window.hi - window.lo) * 0.5;
    (value.is_finite() && window.lo < value && value < window.hi).then_some(value)
}

fn exact_rectangle(longitude: Bound, latitude: Bound, turn: i64) -> Option<[ExactPlanarPoint; 4]> {
    let west =
        ExactExpansion::from_f64(longitude.lo)?.add(ExactExpansion::exact_degree_turns(turn)?)?;
    let east =
        ExactExpansion::from_f64(longitude.hi)?.add(ExactExpansion::exact_degree_turns(turn)?)?;
    let south = ExactExpansion::from_f64(latitude.lo)?;
    let north = ExactExpansion::from_f64(latitude.hi)?;
    Some([
        ExactPlanarPoint {
            longitude: west,
            latitude: south,
        },
        ExactPlanarPoint {
            longitude: east,
            latitude: south,
        },
        ExactPlanarPoint {
            longitude: east,
            latitude: north,
        },
        ExactPlanarPoint {
            longitude: west,
            latitude: north,
        },
    ])
}

fn closed_segment_intersects(
    start: &ExactPlanarPoint,
    end: &ExactPlanarPoint,
    other_start: &ExactPlanarPoint,
    other_end: &ExactPlanarPoint,
) -> Option<bool> {
    let first = planar_orientation(start, end, other_start)?.ordering();
    let second = planar_orientation(start, end, other_end)?.ordering();
    let third = planar_orientation(other_start, other_end, start)?.ordering();
    let fourth = planar_orientation(other_start, other_end, end)?.ordering();
    let crosses = |left: Ordering, right: Ordering| {
        left == Ordering::Equal || right == Ordering::Equal || left != right
    };
    if crosses(first, second) && crosses(third, fourth) {
        if first != Ordering::Equal
            && second != Ordering::Equal
            && third != Ordering::Equal
            && fourth != Ordering::Equal
        {
            return Some(true);
        }
        let contains =
            |left: &ExactPlanarPoint, middle: &ExactPlanarPoint, right: &ExactPlanarPoint| {
                closed_between_expansion(&left.longitude, &middle.longitude, &right.longitude)
                    .zip(closed_between_expansion(
                        &left.latitude,
                        &middle.latitude,
                        &right.latitude,
                    ))
                    .map(|(longitude, latitude)| longitude && latitude)
            };
        return Some(
            (first == Ordering::Equal && contains(start, other_start, end).unwrap_or(false))
                || (second == Ordering::Equal && contains(start, other_end, end).unwrap_or(false))
                || (third == Ordering::Equal
                    && contains(other_start, start, other_end).unwrap_or(false))
                || (fourth == Ordering::Equal
                    && contains(other_start, end, other_end).unwrap_or(false)),
        );
    }
    Some(false)
}

fn exact_segment_touches_rectangle(
    start: &ExactPlanarPoint,
    end: &ExactPlanarPoint,
    longitude: Bound,
    latitude: Bound,
) -> Option<bool> {
    let (first_turn, last_turn) =
        exact_longitude_copy_range(&start.longitude, &end.longitude, longitude)?;
    for turn in first_turn..=last_turn {
        let rectangle = exact_rectangle(longitude, latitude, turn)?;
        for (first, second) in rectangle
            .iter()
            .zip(rectangle.iter().cycle().skip(1))
            .take(4)
        {
            if closed_segment_intersects(start, end, first, second)? {
                return Some(true);
            }
        }
    }
    Some(false)
}

/// Every copy of a canonical degree window that can meet one exact lifted
/// source extent.  The proposal is an exact expansion quotient; the one-copy
/// margin makes this a conservative enumerator even at a closed endpoint.
/// Individual lifted source edges are seam-split before this point, so a range
/// wider than five copies means the symbolic carrier is no longer a bounded
/// certificate and must fail open instead of guessing a periodic image.
fn exact_longitude_copy_range(
    first: &ExactExpansion,
    second: &ExactExpansion,
    window: Bound,
) -> Option<(i64, i64)> {
    let (minimum, maximum) = match first.sub(*second)?.ordering() {
        Ordering::Greater => (*second, *first),
        Ordering::Less | Ordering::Equal => (*first, *second),
    };
    let west = ExactExpansion::from_f64(window.lo)?;
    let east = ExactExpansion::from_f64(window.hi)?;
    let lower = ExactRatio::from_expansion(&minimum.sub(east)?)?
        .floor_turn()?
        .checked_sub(1)?;
    let upper = ExactRatio::from_expansion(&maximum.sub(west)?)?
        .floor_turn()?
        .checked_add(1)?;
    if upper < lower || upper.checked_sub(lower)? > 4 {
        return None;
    }
    Some((lower, upper))
}

fn exact_point_touches_rectangle(
    point: &ExactPlanarPoint,
    longitude: Bound,
    latitude: Bound,
) -> Option<bool> {
    let (first_turn, last_turn) =
        exact_longitude_copy_range(&point.longitude, &point.longitude, longitude)?;
    for turn in first_turn..=last_turn {
        let rectangle = exact_rectangle(longitude, latitude, turn)?;
        if closed_between_expansion(
            &rectangle[0].longitude,
            &point.longitude,
            &rectangle[1].longitude,
        )? && closed_between_expansion(
            &rectangle[0].latitude,
            &point.latitude,
            &rectangle[2].latitude,
        )? {
            return Some(true);
        }
    }
    Some(false)
}

/// Private identity of one source vertex.  These ids only preserve source
/// topology; they are never physical cell endpoint identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SourceVertexKey {
    pub(crate) component: usize,
    pub(crate) ring: usize,
    pub(crate) ordinal: usize,
}

/// Private identity of one directed source edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SourceEdgeKey {
    pub(crate) component: usize,
    pub(crate) ring: usize,
    pub(crate) ordinal: usize,
}

/// Exact dyadic expansion with the design-prescribed fixed capacity.  The
/// frozen design did not supply an expression-by-expression limb budget, so
/// this module deliberately makes no stronger local claim: capacity failure
/// is `None`, which marks the source component `Unknown`; no epsilon may ever
/// choose a parent parameter.  Do not replace that fail-open boundary with a
/// capacity inferred from a passing fixture.
#[derive(Clone, Copy, Debug, PartialEq)]
struct ExactExpansion {
    limbs: [f64; EXPANSION_LIMBS],
    len: u8,
}

impl ExactExpansion {
    const fn zero() -> Self {
        Self {
            limbs: [0.0; EXPANSION_LIMBS],
            len: 0,
        }
    }

    fn from_f64(value: f64) -> Option<Self> {
        value.is_finite().then(|| {
            if value == 0.0 {
                Self::zero()
            } else {
                let mut result = Self::zero();
                result.limbs[0] = value;
                result.len = 1;
                result
            }
        })
    }

    fn from_i64(value: i64) -> Option<Self> {
        const LOW_BITS: i64 = 1_i64 << 32;
        let high = value / LOW_BITS;
        let low = value % LOW_BITS;
        let low = Self::from_f64(low as f64)?;
        let high = Self::from_f64((high as f64) * (LOW_BITS as f64))?;
        low.add(high)
    }

    fn exact_degree_turns(turns: i64) -> Option<Self> {
        Self::from_i64(turns)?.product(Self::from_f64(FULL_TURN_DEGREES)?)
    }

    fn as_slice(&self) -> &[f64] {
        &self.limbs[..self.len as usize]
    }

    fn neg(mut self) -> Self {
        for limb in &mut self.limbs[..self.len as usize] {
            *limb = -*limb;
        }
        self
    }

    fn add(self, rhs: Self) -> Option<Self> {
        let left = self.as_slice();
        let right = rhs.as_slice();
        if left.len().checked_add(right.len())? > EXPANSION_LIMBS {
            return None;
        }
        let mut limbs = [0.0; EXPANSION_LIMBS];
        let len = expansion_sum_zeroelim(left, right, &mut limbs)?;
        Some(Self {
            limbs,
            len: u8::try_from(len).ok()?,
        })
    }

    fn sub(self, rhs: Self) -> Option<Self> {
        self.add(rhs.neg())
    }

    fn product(self, rhs: Self) -> Option<Self> {
        let mut result = Self::zero();
        for &left in self.as_slice() {
            for &right in rhs.as_slice() {
                let term = product_expansion(left, right)?;
                result = result.add(term)?;
            }
        }
        Some(result)
    }

    fn ordering(self) -> Ordering {
        self.as_slice()
            .iter()
            .rev()
            .find(|&&limb| limb != 0.0)
            .map_or(Ordering::Equal, |limb| limb.total_cmp(&0.0))
    }

    fn is_exact(self, value: f64) -> Option<bool> {
        Some(self.sub(Self::from_f64(value)?)?.ordering() == Ordering::Equal)
    }

    fn checked_estimate(self) -> Option<f64> {
        self.as_slice().iter().try_fold(0.0, |sum, &limb| {
            let next = sum + limb;
            next.is_finite().then_some(next)
        })
    }
}

/// Exact ratio retained as source-parent identity.  `Bound` is intentionally
/// absent here: an outward enclosure may evaluate an arc, but cannot identify
/// which parent split produced that arc.
#[derive(Clone, Debug, PartialEq)]
struct ExactRatio {
    numerator: ExactExpansion,
    denominator: ExactExpansion,
}

impl ExactRatio {
    fn new(numerator: &ExactExpansion, denominator: &ExactExpansion) -> Option<Self> {
        match denominator.ordering() {
            Ordering::Equal => None,
            Ordering::Greater => Some(Self {
                numerator: *numerator,
                denominator: *denominator,
            }),
            Ordering::Less => Some(Self {
                numerator: numerator.neg(),
                denominator: denominator.neg(),
            }),
        }
    }

    fn from_i64(numerator: i64, denominator: i64) -> Option<Self> {
        let numerator = ExactExpansion::from_i64(numerator)?;
        let denominator = ExactExpansion::from_i64(denominator)?;
        Self::new(&numerator, &denominator)
    }

    fn from_f64(value: f64) -> Option<Self> {
        let numerator = ExactExpansion::from_f64(value)?;
        let denominator = ExactExpansion::from_i64(1)?;
        Self::new(&numerator, &denominator)
    }

    fn from_expansion(numerator: &ExactExpansion) -> Option<Self> {
        let denominator = ExactExpansion::from_i64(1)?;
        Self::new(numerator, &denominator)
    }

    fn add(self, rhs: &Self) -> Option<Self> {
        let numerator = self
            .numerator
            .product(rhs.denominator)?
            .add(rhs.numerator.product(self.denominator)?)?;
        let denominator = self.denominator.product(rhs.denominator)?;
        Self::new(&numerator, &denominator)
    }

    fn sub(self, rhs: &Self) -> Option<Self> {
        self.add(&Self {
            numerator: rhs.numerator.neg(),
            denominator: rhs.denominator,
        })
    }

    fn mul(self, rhs: &Self) -> Option<Self> {
        let numerator = self.numerator.product(rhs.numerator)?;
        let denominator = self.denominator.product(rhs.denominator)?;
        Self::new(&numerator, &denominator)
    }

    fn div(self, rhs: &Self) -> Option<Self> {
        if rhs.numerator.ordering() == Ordering::Equal {
            return None;
        }
        let numerator = self.numerator.product(rhs.denominator)?;
        let denominator = self.denominator.product(rhs.numerator)?;
        Self::new(&numerator, &denominator)
    }

    fn ordering(&self, rhs: &Self) -> Option<Ordering> {
        self.numerator
            .product(rhs.denominator)?
            .sub(rhs.numerator.product(self.denominator)?)
            .map(ExactExpansion::ordering)
    }

    fn is_closed_unit(&self) -> Option<bool> {
        let zero = Self::from_i64(0, 1)?;
        let one = Self::from_i64(1, 1)?;
        Some(self.ordering(&zero)? != Ordering::Less && self.ordering(&one)? != Ordering::Greater)
    }

    /// Returns an outward binary64 interval only after exact cross-products
    /// prove that the two stored endpoints enclose this parent identity.
    /// The enclosure never participates in ordering or endpoint identity.
    fn certified_bound(&self) -> Option<Bound> {
        if self.numerator.ordering() == Ordering::Equal {
            return Bound::exact(0.0);
        }
        let numerator = self.numerator.checked_estimate()?;
        let denominator = self.denominator.checked_estimate()?;
        let proposal = numerator / denominator;
        if !proposal.is_finite() {
            return None;
        }
        let mut lower = proposal.next_down();
        let mut upper = proposal.next_up();
        for _ in 0..=64 {
            let lower_ratio = Self::from_f64(lower)?;
            let upper_ratio = Self::from_f64(upper)?;
            if self.ordering(&lower_ratio)? == Ordering::Less {
                lower = lower.next_down();
                continue;
            }
            if self.ordering(&upper_ratio)? == Ordering::Greater {
                upper = upper.next_up();
                continue;
            }
            return Bound::new(lower, upper);
        }
        None
    }

    /// Exact floor of this ratio divided by one full longitude turn.  The
    /// binary64 quotient merely proposes a nearby bucket; exact
    /// cross-products decide each correction.
    fn floor_turn(&self) -> Option<i64> {
        let proposal = self.numerator.checked_estimate()? / self.denominator.checked_estimate()?;
        let proposal = (proposal / FULL_TURN_DEGREES).floor();
        if !proposal.is_finite() || !(i64::MIN as f64..=i64::MAX as f64).contains(&proposal) {
            return None;
        }
        let mut level = proposal as i64;
        for _ in 0..4 {
            let next = level.checked_add(1)?;
            let next_ratio = Self::from_i64(next.checked_mul(360)?, 1)?;
            if self.ordering(&next_ratio)? != Ordering::Less {
                level = next;
                continue;
            }
            let current_ratio = Self::from_i64(level.checked_mul(360)?, 1)?;
            if self.ordering(&current_ratio)? == Ordering::Less {
                level = level.checked_sub(1)?;
                continue;
            }
            return Some(level);
        }
        None
    }
}

/// The reason a parent edge was partitioned.  This remains symbolic so a
/// later certified arc receives the exact parent interval rather than a
/// rounded materialised `t` value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PartitionConstraint {
    FullEdgeMidpoint,
    LiftedLongitudeSeam,
    LatitudeReflection,
    PostReflectionLongitudeSeam,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParentParameterKey {
    Start,
    Partition(PartitionConstraint),
    End,
}

#[derive(Clone, Debug)]
struct ExactParentParameter {
    key: ParentParameterKey,
    ratio: ExactRatio,
    enclosure: Bound,
}

impl ExactParentParameter {
    fn new(key: ParentParameterKey, ratio: ExactRatio) -> Option<Self> {
        let enclosure = ratio.certified_bound()?;
        ratio.is_closed_unit()?.then_some(Self {
            key,
            ratio,
            enclosure,
        })
    }

    fn start() -> Option<Self> {
        Self::new(ParentParameterKey::Start, ExactRatio::from_i64(0, 1)?)
    }

    fn end() -> Option<Self> {
        Self::new(ParentParameterKey::End, ExactRatio::from_i64(1, 1)?)
    }
}

#[derive(Clone, Debug)]
struct ParentInterval {
    start: ExactParentParameter,
    end: ExactParentParameter,
}

/// An input longitude plus an exact number of full turns.  It is never
/// collapsed to `raw + 360 * turns` in a stored `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
struct LiftedLongitude {
    raw: f64,
    turns: i64,
}

impl LiftedLongitude {
    fn difference(self, rhs: Self) -> Option<ExactExpansion> {
        ExactExpansion::from_f64(self.raw)?
            .sub(ExactExpansion::from_f64(rhs.raw)?)?
            .add(ExactExpansion::exact_degree_turns(
                self.turns.checked_sub(rhs.turns)?,
            )?)
    }

    fn ordering(self, rhs: Self) -> Option<Ordering> {
        Some(self.difference(rhs)?.ordering())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiftKind {
    Shortest,
    FullPositive,
    FullNegative,
}

#[derive(Clone, Copy, Debug)]
struct LiftStep {
    turn_delta: i64,
    kind: LiftKind,
}

/// Determine an exact continuous-lift update.  The floating estimate only
/// proposes a nearby integer; every eventual branch is confirmed through the
/// expansion sign, including the written +/-180 and +/-360 directions.
fn lifted_step(start: f64, end: f64) -> Option<LiftStep> {
    let raw_delta = ExactExpansion::from_f64(end)?.sub(ExactExpansion::from_f64(start)?)?;
    if raw_delta.is_exact(FULL_TURN_DEGREES)? {
        return Some(LiftStep {
            turn_delta: 0,
            kind: LiftKind::FullPositive,
        });
    }
    if raw_delta.is_exact(-FULL_TURN_DEGREES)? {
        return Some(LiftStep {
            turn_delta: 0,
            kind: LiftKind::FullNegative,
        });
    }
    let proposal = end - start;
    if !proposal.is_finite() {
        return None;
    }
    let candidate = (-proposal / FULL_TURN_DEGREES).round();
    if !candidate.is_finite() || candidate < i64::MIN as f64 || candidate > i64::MAX as f64 {
        return None;
    }
    let mut turn_delta = candidate as i64;
    for _ in 0..4 {
        let lifted = raw_delta.add(ExactExpansion::exact_degree_turns(turn_delta)?)?;
        if lifted
            .sub(ExactExpansion::from_f64(HALF_TURN_DEGREES)?)?
            .ordering()
            == Ordering::Greater
        {
            turn_delta = turn_delta.checked_sub(1)?;
            continue;
        }
        if lifted
            .add(ExactExpansion::from_f64(HALF_TURN_DEGREES)?)?
            .ordering()
            == Ordering::Less
        {
            turn_delta = turn_delta.checked_add(1)?;
            continue;
        }
        // At exactly half a world, the written direction—not a normalization
        // convention—chooses the branch.
        if lifted.is_exact(-HALF_TURN_DEGREES)? && raw_delta.ordering() == Ordering::Greater {
            turn_delta = turn_delta.checked_add(1)?;
            continue;
        }
        if lifted.is_exact(HALF_TURN_DEGREES)? && raw_delta.ordering() == Ordering::Less {
            turn_delta = turn_delta.checked_sub(1)?;
            continue;
        }
        return Some(LiftStep {
            turn_delta,
            kind: LiftKind::Shortest,
        });
    }
    None
}

#[derive(Clone, Copy, Debug)]
struct LiftedVertex {
    longitude: LiftedLongitude,
    latitude: f64,
}

#[derive(Clone, Debug)]
struct LiftedChain {
    vertices: Box<[LiftedVertex]>,
    edges: Box<[LiftKind]>,
    closed: bool,
    degree: Option<i64>,
}

/// The finite lifted authority for one polygon ring.  A nonzero longitude
/// degree cannot be treated as an unknown or a world-sized source: its two
/// physical end vertices are joined through the sign-selected pole, with the
/// signed number of turns retained symbolically.  Later PIP consumes this
/// plan over every direct periodic preimage; it must never reconstruct a
/// shortest-period closure from the raw endpoint doubles.
#[derive(Clone, Copy, Debug)]
struct PeriodicPoleRoof {
    pole: PhysicalEndpointKey,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "retained for exact roof-state unit certificates")
    )]
    turns: i64,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "retained for exact roof-state unit certificates")
    )]
    span: ExactExpansion,
}

#[derive(Clone, Copy, Debug)]
struct LiftedRingFill {
    chain_index: usize,
    component: usize,
    ring: usize,
    degree: i64,
    roof: Option<PeriodicPoleRoof>,
    // A finite lifted fill can still be a strict outer image after quotienting
    // longitude.  Only this certificate permits its planar parity interior to
    // establish retained-authority interior; exterior remains a sound
    // negative without it.
    periodic_exact: bool,
}

impl LiftedRingFill {
    fn from_chain(
        chain_index: usize,
        component: usize,
        ring: usize,
        chain: &LiftedChain,
    ) -> Option<Self> {
        let degree = chain.degree?;
        let roof = match degree.cmp(&0) {
            Ordering::Equal => None,
            Ordering::Greater => Some(PeriodicPoleRoof {
                pole: PhysicalEndpointKey::NorthPole,
                turns: degree,
                span: ExactExpansion::exact_degree_turns(degree)?,
            }),
            Ordering::Less => Some(PeriodicPoleRoof {
                pole: PhysicalEndpointKey::SouthPole,
                turns: degree,
                span: ExactExpansion::exact_degree_turns(degree)?,
            }),
        };
        Some(Self {
            chain_index,
            component,
            ring,
            degree,
            roof,
            periodic_exact: chain.periodic_exact(degree)?,
        })
    }

    /// Copies whose lifted longitude can lie on this finite ring fill.  The
    /// range is edge-proportional through the chain's extrema, never a global
    /// degree-based fallback.  The later exact PIP walks these copies.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "module unit tests inspect the exact periodic preimage range"
        )
    )]
    fn direct_preimage_copies(
        self,
        chain: &LiftedChain,
        longitude: f64,
    ) -> Option<std::ops::RangeInclusive<i64>> {
        chain.direct_preimage_copies(longitude)
    }

    fn point_class(
        self,
        chain: &LiftedChain,
        longitude: f64,
        query: &ExactPlanarPoint,
    ) -> GridPointClass {
        let copies = match chain.direct_preimages(longitude) {
            // No lift of the stored query lies in this finite fill's longitude
            // span.  That is a certified exterior, not an arithmetic failure.
            DirectPreimages::Empty => return GridPointClass::Exterior,
            DirectPreimages::Copies(copies) => copies,
            DirectPreimages::Uncertain => return GridPointClass::Unknown,
        };
        let result = classify_lifted_ring_bucket(chain, self.roof.as_ref(), query, copies);
        if result == GridPointClass::Interior && !self.periodic_exact {
            GridPointClass::Unknown
        } else {
            result
        }
    }

    #[cfg(test)]
    fn direct_oracle_point_class(
        self,
        chain: &LiftedChain,
        longitude: f64,
        query: &ExactPlanarPoint,
    ) -> GridPointClass {
        let copies = match chain.direct_preimages(longitude) {
            DirectPreimages::Empty => return GridPointClass::Exterior,
            DirectPreimages::Copies(copies) => copies,
            DirectPreimages::Uncertain => return GridPointClass::Unknown,
        };
        let mut result = GridPointClass::Exterior;
        for copy in copies {
            let Some(shifted) = query.shifted(copy) else {
                return GridPointClass::Unknown;
            };
            match classify_lifted_ring_copy(chain, self.roof.as_ref(), &shifted) {
                GridPointClass::Boundary => return GridPointClass::Boundary,
                GridPointClass::Unknown => result = GridPointClass::Unknown,
                GridPointClass::Interior if result != GridPointClass::Unknown => {
                    result = GridPointClass::Interior;
                },
                GridPointClass::Interior | GridPointClass::Exterior => {},
            }
        }
        if result == GridPointClass::Interior && !self.periodic_exact {
            GridPointClass::Unknown
        } else {
            result
        }
    }
}

/// A finite lifted ring either has no periodic preimage for a stored query,
/// has a checked inclusive range of copies, or could not certify the range.
/// Keeping the empty and uncertain outcomes distinct is load-bearing: the
/// former proves exterior while the latter must fail open.
#[derive(Debug)]
enum DirectPreimages {
    Empty,
    Copies(std::ops::RangeInclusive<i64>),
    Uncertain,
}

/// Exact periodic-copy range for a closed lifted longitude envelope.  The
/// binary64 quotient only proposes a nearby integer; every retained endpoint
/// comparison is backstopped by the expansion carrier.  `Empty` is a proof of
/// absence, while every arithmetic failure remains `Uncertain`.
fn direct_preimages_for_envelope(
    minimum: &ExactExpansion,
    maximum: &ExactExpansion,
    query: &ExactExpansion,
) -> DirectPreimages {
    let (Some(minimum_proposal), Some(maximum_proposal), Some(query_proposal)) = (
        minimum.checked_estimate(),
        maximum.checked_estimate(),
        query.checked_estimate(),
    ) else {
        return DirectPreimages::Uncertain;
    };
    let lower_proposal = ((minimum_proposal - query_proposal) / FULL_TURN_DEGREES).ceil();
    let upper_proposal = ((maximum_proposal - query_proposal) / FULL_TURN_DEGREES).floor();
    if !lower_proposal.is_finite()
        || !upper_proposal.is_finite()
        || !(i64::MIN as f64..=i64::MAX as f64).contains(&lower_proposal)
        || !(i64::MIN as f64..=i64::MAX as f64).contains(&upper_proposal)
    {
        return DirectPreimages::Uncertain;
    }
    let mut lower = lower_proposal as i64;
    let mut upper = upper_proposal as i64;
    for _ in 0..4 {
        let Some(ordering) = compare_expansion_query_copy(minimum, query, lower) else {
            return DirectPreimages::Uncertain;
        };
        if ordering == Ordering::Greater {
            let Some(next) = lower.checked_add(1) else {
                return DirectPreimages::Uncertain;
            };
            lower = next;
            continue;
        }
        if lower > i64::MIN {
            let Some(previous) = compare_expansion_query_copy(minimum, query, lower - 1) else {
                return DirectPreimages::Uncertain;
            };
            if previous != Ordering::Greater {
                lower -= 1;
                continue;
            }
        }
        break;
    }
    for _ in 0..4 {
        let Some(ordering) = compare_expansion_query_copy(maximum, query, upper) else {
            return DirectPreimages::Uncertain;
        };
        if ordering == Ordering::Less {
            let Some(next) = upper.checked_sub(1) else {
                return DirectPreimages::Uncertain;
            };
            upper = next;
            continue;
        }
        if upper < i64::MAX {
            let Some(next) = compare_expansion_query_copy(maximum, query, upper + 1) else {
                return DirectPreimages::Uncertain;
            };
            if next != Ordering::Less {
                upper += 1;
                continue;
            }
        }
        break;
    }
    if lower <= upper {
        DirectPreimages::Copies(lower..=upper)
    } else {
        DirectPreimages::Empty
    }
}

fn compare_expansion_query_copy(
    value: &ExactExpansion,
    query: &ExactExpansion,
    turns: i64,
) -> Option<Ordering> {
    (*value)
        .sub((*query).add(ExactExpansion::exact_degree_turns(turns)?)?)
        .map(ExactExpansion::ordering)
}

/// Linear periodic even-odd accumulator.  A crossed source edge contributes
/// one XOR event at the greatest lifted query copy whose +x ray it reaches;
/// walking the finite direct-preimage range then sees the same parity as a
/// separate planar test for every `q + 360*k`, without an edge-by-copy loop.
struct PeriodicParityBuckets {
    first: i64,
    events: Vec<bool>,
    parity_at_first: bool,
}

impl PeriodicParityBuckets {
    fn new(copies: std::ops::RangeInclusive<i64>, edge_count: usize) -> Option<Self> {
        let first = *copies.start();
        let last = *copies.end();
        let length = last.checked_sub(first)?.checked_add(1)?;
        let length = usize::try_from(length).ok()?;
        // The exact lift changes by at most one turn per original edge.  A
        // wider range would violate the carrier premise, not authorize a
        // quadratic fallback.
        if length > edge_count.checked_add(2)? {
            return None;
        }
        let mut events = Vec::new();
        events.try_reserve_exact(length).ok()?;
        events.resize(length, false);
        Some(Self {
            first,
            events,
            parity_at_first: false,
        })
    }

    fn last(&self) -> Option<i64> {
        self.first
            .checked_add(i64::try_from(self.events.len()).ok()?.checked_sub(1)?)
    }

    fn add_crossing(&mut self, bucket: i64) -> Option<()> {
        if bucket < self.first {
            return Some(());
        }
        self.parity_at_first = !self.parity_at_first;
        let last = self.last()?;
        if bucket <= last {
            let index = usize::try_from(bucket.checked_sub(self.first)?).ok()?;
            let event = self.events.get_mut(index)?;
            *event = !*event;
        }
        Some(())
    }

    fn any_interior(&self) -> bool {
        let mut parity = self.parity_at_first;
        for (index, event) in self.events.iter().enumerate() {
            if parity {
                return true;
            }
            if index + 1 != self.events.len() {
                parity ^= *event;
            }
        }
        false
    }
}

#[derive(Clone, Copy, Debug)]
struct ExactPlanarPoint {
    longitude: ExactExpansion,
    latitude: ExactExpansion,
}

/// A selection-image endpoint can be a rational strip/reflection split.  It
/// stays rational until the exact PIP determinant consumes it; collapsing it
/// to a double would lose the t=1/3 identity this carrier was built to retain.
#[derive(Clone, Debug)]
struct ExactSelectionPoint {
    longitude: ExactRatio,
    latitude: ExactRatio,
}

impl ExactPlanarPoint {
    fn from_stored(longitude: f64, latitude: f64) -> Option<Self> {
        Some(Self {
            longitude: ExactExpansion::from_f64(longitude)?,
            latitude: ExactExpansion::from_f64(latitude)?,
        })
    }

    fn from_lifted(vertex: LiftedVertex) -> Option<Self> {
        Some(Self {
            longitude: ExactExpansion::from_f64(vertex.longitude.raw)?
                .add(ExactExpansion::exact_degree_turns(vertex.longitude.turns)?)?,
            latitude: ExactExpansion::from_f64(vertex.latitude)?,
        })
    }

    fn shifted(&self, turns: i64) -> Option<Self> {
        Some(Self {
            longitude: self
                .longitude
                .add(ExactExpansion::exact_degree_turns(turns)?)?,
            latitude: self.latitude,
        })
    }
}

fn classify_lifted_ring_bucket(
    chain: &LiftedChain,
    roof: Option<&PeriodicPoleRoof>,
    query: &ExactPlanarPoint,
    copies: std::ops::RangeInclusive<i64>,
) -> GridPointClass {
    let Some(mut buckets) = PeriodicParityBuckets::new(copies, chain.edges.len()) else {
        return GridPointClass::Unknown;
    };
    let Some(mut previous) = chain
        .vertices
        .first()
        .copied()
        .and_then(ExactPlanarPoint::from_lifted)
    else {
        return GridPointClass::Unknown;
    };
    for vertex in &chain.vertices[1..] {
        let Some(current) = ExactPlanarPoint::from_lifted(*vertex) else {
            return GridPointClass::Unknown;
        };
        match bucket_lifted_edge(&previous, &current, query, false, &mut buckets) {
            BucketEdge::Decisive(result) => return result,
            BucketEdge::Continue => {},
            BucketEdge::Uncertain => return GridPointClass::Unknown,
        }
        previous = current;
    }
    if let Some(roof) = roof {
        let Some(first) = chain
            .vertices
            .first()
            .copied()
            .and_then(ExactPlanarPoint::from_lifted)
        else {
            return GridPointClass::Unknown;
        };
        let pole_latitude = match roof.pole {
            PhysicalEndpointKey::NorthPole => ExactExpansion::from_i64(90),
            PhysicalEndpointKey::SouthPole => ExactExpansion::from_i64(-90),
            PhysicalEndpointKey::CellVertex(_) => None,
        };
        let Some(pole_latitude) = pole_latitude else {
            return GridPointClass::Unknown;
        };
        let end_pole = ExactPlanarPoint {
            longitude: previous.longitude,
            latitude: pole_latitude,
        };
        let start_pole = ExactPlanarPoint {
            longitude: first.longitude,
            latitude: pole_latitude,
        };
        for (start, end) in [
            (previous, end_pole),
            (end_pole, start_pole),
            (start_pole, first),
        ] {
            match bucket_lifted_edge(&start, &end, query, true, &mut buckets) {
                BucketEdge::Decisive(result) => return result,
                BucketEdge::Continue => {},
                BucketEdge::Uncertain => return GridPointClass::Unknown,
            }
        }
    }
    if buckets.any_interior() {
        GridPointClass::Interior
    } else {
        GridPointClass::Exterior
    }
}

/// One exact edge's impact on the periodic parity sweep.  `Uncertain` is
/// intentionally distinct from a boundary result: neither may establish a
/// negative, but the caller preserves the former as a failed proof.
enum BucketEdge {
    Continue,
    Decisive(GridPointClass),
    Uncertain,
}

fn bucket_lifted_edge(
    start: &ExactPlanarPoint,
    end: &ExactPlanarPoint,
    query: &ExactPlanarPoint,
    artificial: bool,
    buckets: &mut PeriodicParityBuckets,
) -> BucketEdge {
    let (Some(start_y), Some(end_y)) = (
        start.latitude.sub(query.latitude),
        end.latitude.sub(query.latitude),
    ) else {
        return BucketEdge::Uncertain;
    };
    let Some(on_latitude) =
        closed_between_expansion(&start.latitude, &query.latitude, &end.latitude)
    else {
        return BucketEdge::Uncertain;
    };
    if on_latitude {
        let (minimum, maximum) = if let Some(delta) = start.longitude.sub(end.longitude) {
            if delta.ordering() == Ordering::Greater {
                (&end.longitude, &start.longitude)
            } else {
                (&start.longitude, &end.longitude)
            }
        } else {
            return BucketEdge::Uncertain;
        };
        match direct_preimages_for_envelope(minimum, maximum, &query.longitude) {
            DirectPreimages::Empty => {},
            DirectPreimages::Uncertain => return BucketEdge::Decisive(GridPointClass::Unknown),
            DirectPreimages::Copies(copies) => {
                let Some(count) = copies
                    .end()
                    .checked_sub(*copies.start())
                    .and_then(|value| value.checked_add(1))
                else {
                    return BucketEdge::Uncertain;
                };
                // A shortest/full original edge can meet at most two seam
                // spellings.  The artificial horizontal roof is excluded by
                // the non-pole query contract before reaching this branch.
                if count > 2 {
                    return BucketEdge::Decisive(GridPointClass::Unknown);
                }
                for copy in copies {
                    let Some(shifted) = query.shifted(copy) else {
                        return BucketEdge::Uncertain;
                    };
                    let Some(orientation) = planar_orientation(start, end, &shifted) else {
                        return BucketEdge::Uncertain;
                    };
                    let Some(on_longitude) = closed_between_expansion(
                        &start.longitude,
                        &shifted.longitude,
                        &end.longitude,
                    ) else {
                        return BucketEdge::Uncertain;
                    };
                    if orientation.ordering() == Ordering::Equal && on_longitude {
                        return BucketEdge::Decisive(if artificial {
                            GridPointClass::Unknown
                        } else {
                            GridPointClass::Boundary
                        });
                    }
                }
            },
        }
    }
    let start_cmp = start_y.ordering();
    let end_cmp = end_y.ordering();
    let crosses = (start_cmp != Ordering::Greater && end_cmp == Ordering::Greater)
        || (end_cmp != Ordering::Greater && start_cmp == Ordering::Greater);
    if !crosses {
        return BucketEdge::Continue;
    }
    let Some(orientation) = planar_orientation(start, end, query) else {
        return BucketEdge::Uncertain;
    };
    let Some(delta_y) = end.latitude.sub(start.latitude) else {
        return BucketEdge::Uncertain;
    };
    if delta_y.ordering() == Ordering::Equal {
        return BucketEdge::Continue;
    }
    let Some(ratio) = ExactRatio::new(&orientation, &delta_y) else {
        return BucketEdge::Uncertain;
    };
    let Some(bucket) = ratio.floor_turn() else {
        return BucketEdge::Uncertain;
    };
    if buckets.add_crossing(bucket).is_none() {
        return BucketEdge::Uncertain;
    }
    BucketEdge::Continue
}

/// Exact ray contribution of one rational selection-image segment.  Selection
/// endpoints are frequently generated at rational strip cuts (notably t=1/3),
/// so this deliberately mirrors `bucket_lifted_edge` in `ExactRatio` space
/// instead of recovering a rounded degree endpoint.
fn bucket_selection_edge(
    start: &ExactSelectionPoint,
    end: &ExactSelectionPoint,
    query: &ExactPlanarPoint,
    buckets: &mut PeriodicParityBuckets,
) -> BucketEdge {
    let Some(query_longitude) = ExactRatio::from_expansion(&query.longitude) else {
        return BucketEdge::Uncertain;
    };
    let Some(query_latitude) = ExactRatio::from_expansion(&query.latitude) else {
        return BucketEdge::Uncertain;
    };
    let (Some(start_y), Some(end_y)) = (
        start.latitude.clone().sub(&query_latitude),
        end.latitude.clone().sub(&query_latitude),
    ) else {
        return BucketEdge::Uncertain;
    };
    let (start_cmp, end_cmp) = (start_y.numerator.ordering(), end_y.numerator.ordering());

    // A query on an original selected edge is closed contact.  Test the three
    // only physical sheet preimages individually so exact +/-180 spellings
    // cannot be normalized into an unrelated line.
    let Some(on_latitude) =
        selection_closed_between(&start.latitude, &query_latitude, &end.latitude)
    else {
        return BucketEdge::Uncertain;
    };
    if on_latitude {
        for turns in -1_i64..=1_i64 {
            let Some(offset) = turns
                .checked_mul(360)
                .and_then(|value| ExactRatio::from_i64(value, 1))
            else {
                return BucketEdge::Uncertain;
            };
            let Some(shifted_longitude) = query_longitude.clone().add(&offset) else {
                return BucketEdge::Uncertain;
            };
            let shifted = ExactSelectionPoint {
                longitude: shifted_longitude,
                latitude: query_latitude.clone(),
            };
            let Some(orientation) = selection_orientation(start, end, &shifted) else {
                return BucketEdge::Uncertain;
            };
            let Some(on_longitude) =
                selection_closed_between(&start.longitude, &shifted.longitude, &end.longitude)
            else {
                return BucketEdge::Uncertain;
            };
            if orientation.numerator.ordering() == Ordering::Equal && on_longitude {
                return BucketEdge::Decisive(GridPointClass::Boundary);
            }
        }
    }

    let crosses = (start_cmp != Ordering::Greater && end_cmp == Ordering::Greater)
        || (end_cmp != Ordering::Greater && start_cmp == Ordering::Greater);
    if !crosses {
        return BucketEdge::Continue;
    }
    let Some(orientation) = selection_orientation(start, end, &ExactSelectionPoint {
        longitude: query_longitude,
        latitude: query_latitude,
    }) else {
        return BucketEdge::Uncertain;
    };
    let Some(delta_y) = end.latitude.clone().sub(&start.latitude) else {
        return BucketEdge::Uncertain;
    };
    let Some(ratio) = orientation.div(&delta_y) else {
        return BucketEdge::Uncertain;
    };
    let Some(bucket) = ratio.floor_turn() else {
        return BucketEdge::Uncertain;
    };
    if buckets.add_crossing(bucket).is_none() {
        return BucketEdge::Uncertain;
    }
    BucketEdge::Continue
}

fn selection_orientation(
    start: &ExactSelectionPoint,
    end: &ExactSelectionPoint,
    point: &ExactSelectionPoint,
) -> Option<ExactRatio> {
    let dx = end.longitude.clone().sub(&start.longitude)?;
    let dy = end.latitude.clone().sub(&start.latitude)?;
    let px = point.longitude.clone().sub(&start.longitude)?;
    let py = point.latitude.clone().sub(&start.latitude)?;
    dx.mul(&py)?.sub(&dy.mul(&px)?)
}

fn selection_closed_between(
    left: &ExactRatio,
    middle: &ExactRatio,
    right: &ExactRatio,
) -> Option<bool> {
    let left_right = left.ordering(right)?;
    let left_middle = left.ordering(middle)?;
    let middle_right = middle.ordering(right)?;
    Some(match left_right {
        Ordering::Less => left_middle != Ordering::Greater && middle_right != Ordering::Greater,
        Ordering::Greater => left_middle != Ordering::Less && middle_right != Ordering::Less,
        Ordering::Equal => left_middle == Ordering::Equal,
    })
}

/// Exact even-odd point location in the finite lifted fill.  This direct
/// per-copy form is deliberately private to the carrier proof; the traversal
/// installs the design's linear bucket walk before it calls this at leaves.
/// Until then no public path can acquire its potentially wider `Unknown`.
#[cfg(test)]
fn classify_lifted_ring_copy(
    chain: &LiftedChain,
    roof: Option<&PeriodicPoleRoof>,
    query: &ExactPlanarPoint,
) -> GridPointClass {
    let Some(mut previous) = chain
        .vertices
        .first()
        .copied()
        .and_then(ExactPlanarPoint::from_lifted)
    else {
        return GridPointClass::Unknown;
    };
    let mut parity = false;
    for vertex in &chain.vertices[1..] {
        let Some(current) = ExactPlanarPoint::from_lifted(*vertex) else {
            return GridPointClass::Unknown;
        };
        match classify_lifted_edge(&previous, &current, query, false, &mut parity) {
            GridPointClass::Exterior => {},
            result => return result,
        }
        previous = current;
    }
    if let Some(roof) = roof {
        let Some(first) = chain
            .vertices
            .first()
            .copied()
            .and_then(ExactPlanarPoint::from_lifted)
        else {
            return GridPointClass::Unknown;
        };
        let pole_latitude = match roof.pole {
            PhysicalEndpointKey::NorthPole => ExactExpansion::from_i64(90),
            PhysicalEndpointKey::SouthPole => ExactExpansion::from_i64(-90),
            PhysicalEndpointKey::CellVertex(_) => None,
        };
        let Some(pole_latitude) = pole_latitude else {
            return GridPointClass::Unknown;
        };
        let end_pole = ExactPlanarPoint {
            longitude: previous.longitude,
            latitude: pole_latitude,
        };
        let start_pole = ExactPlanarPoint {
            longitude: first.longitude,
            latitude: pole_latitude,
        };
        for (start, end) in [
            (previous, end_pole),
            (end_pole, start_pole),
            (start_pole, first),
        ] {
            match classify_lifted_edge(&start, &end, query, true, &mut parity) {
                GridPointClass::Exterior => {},
                result => return result,
            }
        }
    }
    if parity {
        GridPointClass::Interior
    } else {
        GridPointClass::Exterior
    }
}

/// Returns `Boundary` only for an original source edge.  A point on the
/// artificial pole roof is not a source-boundary proof and therefore stays
/// `Unknown`, preserving the authority/selection separation.
#[cfg(test)]
fn classify_lifted_edge(
    start: &ExactPlanarPoint,
    end: &ExactPlanarPoint,
    query: &ExactPlanarPoint,
    artificial: bool,
    parity: &mut bool,
) -> GridPointClass {
    let Some(orientation) = planar_orientation(start, end, query) else {
        return GridPointClass::Unknown;
    };
    let (Some(start_y), Some(end_y)) = (
        start.latitude.sub(query.latitude),
        end.latitude.sub(query.latitude),
    ) else {
        return GridPointClass::Unknown;
    };
    if orientation.ordering() == Ordering::Equal
        && closed_between_expansion(&start.longitude, &query.longitude, &end.longitude)
            .unwrap_or(false)
        && closed_between_expansion(&start.latitude, &query.latitude, &end.latitude)
            .unwrap_or(false)
    {
        return if artificial {
            GridPointClass::Unknown
        } else {
            GridPointClass::Boundary
        };
    }
    let start_cmp = start_y.ordering();
    let end_cmp = end_y.ordering();
    let crosses = (start_cmp != Ordering::Greater && end_cmp == Ordering::Greater)
        || (end_cmp != Ordering::Greater && start_cmp == Ordering::Greater);
    if !crosses {
        return GridPointClass::Exterior;
    }
    let Some(delta_y) = end.latitude.sub(start.latitude) else {
        return GridPointClass::Unknown;
    };
    if delta_y.ordering() == Ordering::Equal {
        return GridPointClass::Exterior;
    }
    // x_cross - qx is orientation / (end.y - start.y).  Cross only when the
    // two exact signs agree; no division or rounded ray intersection exists.
    if orientation.ordering() == delta_y.ordering() {
        *parity = !*parity;
    }
    GridPointClass::Exterior
}

fn closed_between_expansion(
    left: &ExactExpansion,
    middle: &ExactExpansion,
    right: &ExactExpansion,
) -> Option<bool> {
    let left_right = (*left).sub(*right)?.ordering();
    let left_middle = (*left).sub(*middle)?.ordering();
    let middle_right = (*middle).sub(*right)?.ordering();
    Some(match left_right {
        Ordering::Less => left_middle != Ordering::Greater && middle_right != Ordering::Greater,
        Ordering::Greater => left_middle != Ordering::Less && middle_right != Ordering::Less,
        Ordering::Equal => left_middle == Ordering::Equal,
    })
}

fn planar_orientation(
    start: &ExactPlanarPoint,
    end: &ExactPlanarPoint,
    query: &ExactPlanarPoint,
) -> Option<ExactExpansion> {
    let dx = end.longitude.sub(start.longitude)?;
    let dy = end.latitude.sub(start.latitude)?;
    let qx = query.longitude.sub(start.longitude)?;
    let qy = query.latitude.sub(start.latitude)?;
    dx.product(qy)?.sub(dy.product(qx)?)
}

impl LiftedChain {
    fn from_points(points: &[(f64, f64)], closed: bool) -> Option<Self> {
        let minimum = if closed { 4 } else { 2 };
        if points.len() < minimum || !points.iter().all(|(x, y)| x.is_finite() && y.is_finite()) {
            return None;
        }
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        vertices.try_reserve(points.len()).ok()?;
        edges.try_reserve(points.len().saturating_sub(1)).ok()?;
        vertices.push(LiftedVertex {
            longitude: LiftedLongitude {
                raw: points[0].0,
                turns: 0,
            },
            latitude: points[0].1,
        });
        for &(longitude, latitude) in &points[1..] {
            let previous = *vertices.last()?;
            let step = lifted_step(previous.longitude.raw, longitude)?;
            let turns = previous.longitude.turns.checked_add(step.turn_delta)?;
            vertices.push(LiftedVertex {
                longitude: LiftedLongitude {
                    raw: longitude,
                    turns,
                },
                latitude,
            });
            edges.push(step.kind);
        }
        let mut result = Self {
            vertices: vertices.into_boxed_slice(),
            edges: edges.into_boxed_slice(),
            closed,
            degree: None,
        };
        if closed {
            result.degree = result.closure_degree();
            result.degree?;
        }
        result.canonicalized()
    }

    fn closure_degree(&self) -> Option<i64> {
        let first = *self.vertices.first()?;
        let last = *self.vertices.last()?;
        if !same_physical_latitude(first.latitude, last.latitude)
            || !same_physical_longitude(first.longitude.raw, last.longitude.raw)
        {
            return None;
        }
        let total = last.longitude.difference(first.longitude)?;
        let edge_count = i64::try_from(self.edges.len()).ok()?;
        (-edge_count..=edge_count).find(|&degree| {
            ExactExpansion::exact_degree_turns(degree)
                .and_then(|expected| total.sub(expected))
                .is_some_and(|difference| difference.ordering() == Ordering::Equal)
        })
    }

    fn canonicalized(mut self) -> Option<Self> {
        // R15-S disposition: splitting a linear source at a collinear point
        // preserves its visible cells. Public H3 and S2 all-rule fixtures
        // retain that invariant; disabling exact coalescing was observed
        // green, so the proposed mutation was deleted as unobservable. The
        // direct lift-identity tests below remain the structural guard.
        let minimum = if self.closed { 4 } else { 2 };
        loop {
            let mut removed = false;
            let stop = self.vertices.len().saturating_sub(1);
            for middle in 1..stop {
                if self.vertices.len() <= minimum {
                    return Some(self);
                }
                let previous = self.vertices[middle - 1];
                let current = self.vertices[middle];
                let next = self.vertices[middle + 1];
                if !can_coalesce(previous, current, next)? {
                    continue;
                }
                let mut points = Vec::new();
                points
                    .try_reserve(self.vertices.len().saturating_sub(1))
                    .ok()?;
                for (index, vertex) in self.vertices.iter().enumerate() {
                    if index != middle {
                        points.push((vertex.longitude.raw, vertex.latitude));
                    }
                }
                self = Self::from_points(&points, self.closed)?;
                removed = true;
                break;
            }
            if !removed {
                return Some(self);
            }
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "module unit tests inspect the exact periodic preimage range"
        )
    )]
    fn direct_preimage_copies(&self, longitude: f64) -> Option<std::ops::RangeInclusive<i64>> {
        match self.direct_preimages(longitude) {
            DirectPreimages::Copies(copies) => Some(copies),
            DirectPreimages::Empty | DirectPreimages::Uncertain => None,
        }
    }

    fn direct_preimages(&self, longitude: f64) -> DirectPreimages {
        let Some((minimum, maximum)) = self.longitude_envelope() else {
            return DirectPreimages::Uncertain;
        };
        let Some(query) = ExactExpansion::from_f64(longitude) else {
            return DirectPreimages::Uncertain;
        };
        direct_preimages_for_envelope(&minimum, &maximum, &query)
    }

    fn longitude_envelope(&self) -> Option<(ExactExpansion, ExactExpansion)> {
        let first = *self.vertices.first()?;
        let mut minimum = ExactPlanarPoint::from_lifted(first)?.longitude;
        let mut maximum = minimum;
        for &vertex in &self.vertices[1..] {
            let value = ExactPlanarPoint::from_lifted(vertex)?.longitude;
            if value.sub(minimum)?.ordering() == Ordering::Less {
                minimum = value;
            }
            if value.sub(maximum)?.ordering() == Ordering::Greater {
                maximum = value;
            }
        }
        Some((minimum, maximum))
    }

    /// Proves that quotienting the finite lift cannot create a second open
    /// overlap with one of its nonzero 360-degree translates.  Width below a
    /// turn is immediate.  At exactly one turn we admit only the explicit
    /// +/-1 pole-roof grammar: one endpoint on each extremum, all original
    /// vertices strictly between them, and no full-world source edge.  Every
    /// other equality case remains an outer fill, so its interior is demoted
    /// to `Unknown` by `LiftedRingFill::point_class`.
    fn periodic_exact(&self, degree: i64) -> Option<bool> {
        let (minimum, maximum) = self.longitude_envelope()?;
        let width = maximum.sub(minimum)?;
        let turn = ExactExpansion::from_f64(FULL_TURN_DEGREES)?;
        match width.sub(turn)?.ordering() {
            Ordering::Less => return Some(true),
            Ordering::Greater => return Some(false),
            Ordering::Equal => {},
        }
        if degree.unsigned_abs() != 1
            || self
                .edges
                .iter()
                .any(|kind| !matches!(kind, LiftKind::Shortest))
        {
            return Some(false);
        }
        let first = ExactPlanarPoint::from_lifted(*self.vertices.first()?)?.longitude;
        let last = ExactPlanarPoint::from_lifted(*self.vertices.last()?)?.longitude;
        let endpoints_are_extrema = (first.sub(minimum)?.ordering() == Ordering::Equal
            && last.sub(maximum)?.ordering() == Ordering::Equal)
            || (first.sub(maximum)?.ordering() == Ordering::Equal
                && last.sub(minimum)?.ordering() == Ordering::Equal);
        if !endpoints_are_extrema {
            return Some(false);
        }
        for &vertex in self
            .vertices
            .get(1..self.vertices.len().saturating_sub(1))
            .unwrap_or_default()
        {
            let value = ExactPlanarPoint::from_lifted(vertex)?.longitude;
            if value.sub(minimum)?.ordering() != Ordering::Greater
                || maximum.sub(value)?.ordering() != Ordering::Greater
            {
                return Some(false);
            }
        }
        Some(true)
    }
}

/// One original affine source edge with its exact, ordered parent cuts.  The
/// retained endpoints are in the continuous lift; later selection pieces only
/// add integer reflect/wrap transforms and never replace this authority edge.
#[derive(Clone, Debug)]
struct SymbolicAffineEdge {
    key: SourceEdgeKey,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "retained for exact parent-edge unit certificates")
    )]
    endpoints: [SourceVertexKey; 2],
    polygon: bool,
    start: LiftedVertex,
    end: LiftedVertex,
    parameters: Box<[ExactParentParameter]>,
}

#[derive(Clone, Debug)]
pub(crate) struct SelectionAffinePiece {
    edge: SourceEdgeKey,
    polygon: bool,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "retained for exact parent-interval unit certificates"
        )
    )]
    interval: ParentInterval,
    latitude_zone: i64,
    #[expect(
        dead_code,
        reason = "retained for exact periodic-image unit certificates"
    )]
    longitude_wrap: i64,
    // Exact degree endpoints after the selected reflect/wrap map.  They are
    // kept beside the spherical evaluator because the selection image may
    // veto an authority negative only through its own affine planar PIP;
    // reconstructing them from rounded radians would create a second source.
    selection: [ExactSelectionPoint; 2],
    arc: AffineSourceArc,
}

/// A split either does not cross the parent interval or has one exact parent
/// ratio. Arithmetic failure is deliberately the enclosing `Option::None`,
/// distinct from an ordinary absent crossing.
#[expect(
    clippy::large_enum_variant,
    reason = "boxing an exact 64-limb ratio would add a fallible allocation to every symbolic split"
)]
enum ParentCrossing {
    Absent,
    Present(ExactRatio),
}

impl SymbolicAffineEdge {
    fn new(
        key: SourceEdgeKey,
        endpoints: [SourceVertexKey; 2],
        polygon: bool,
        start: LiftedVertex,
        end: LiftedVertex,
        kind: LiftKind,
    ) -> Option<Self> {
        let mut parameters = Vec::new();
        parameters.try_reserve(8).ok()?;
        parameters.push(ExactParentParameter::start()?);
        parameters.push(ExactParentParameter::end()?);
        let mut result = Self {
            key,
            endpoints,
            polygon,
            start,
            end,
            parameters: parameters.into_boxed_slice(),
        };
        if matches!(kind, LiftKind::FullPositive | LiftKind::FullNegative) {
            result.insert_ratio(
                ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint),
                ExactRatio::from_i64(1, 2)?,
            )?;
        }
        result.collect_lifted_longitude_seams()?;
        result.collect_latitude_reflections()?;
        result.collect_post_reflection_longitude_seams()?;
        Some(result)
    }

    fn lifted_longitude_start(&self) -> Option<ExactExpansion> {
        ExactExpansion::from_f64(self.start.longitude.raw)?.add(ExactExpansion::exact_degree_turns(
            self.start.longitude.turns,
        )?)
    }

    fn lifted_longitude_end(&self) -> Option<ExactExpansion> {
        ExactExpansion::from_f64(self.end.longitude.raw)?.add(ExactExpansion::exact_degree_turns(
            self.end.longitude.turns,
        )?)
    }

    fn latitude_start(&self) -> Option<ExactExpansion> {
        ExactExpansion::from_f64(self.start.latitude)
    }

    fn latitude_end(&self) -> Option<ExactExpansion> {
        ExactExpansion::from_f64(self.end.latitude)
    }

    fn parameter_for(
        &self,
        start: &ExactExpansion,
        end: &ExactExpansion,
        target: &ExactExpansion,
    ) -> Option<ParentCrossing> {
        let denominator = (*end).sub(*start)?;
        if denominator.ordering() == Ordering::Equal {
            return Some(ParentCrossing::Absent);
        }
        let numerator = (*target).sub(*start)?;
        let ratio = ExactRatio::new(&numerator, &denominator)?;
        Some(if ratio.is_closed_unit()? {
            ParentCrossing::Present(ratio)
        } else {
            ParentCrossing::Absent
        })
    }

    fn insert_ratio(&mut self, key: ParentParameterKey, ratio: ExactRatio) -> Option<()> {
        let parameter = ExactParentParameter::new(key, ratio)?;
        let mut parameters = self.parameters.to_vec();
        let mut insert_at = parameters.len();
        for (index, existing) in parameters.iter().enumerate() {
            match parameter.ratio.ordering(&existing.ratio)? {
                Ordering::Less => {
                    insert_at = index;
                    break;
                },
                Ordering::Equal => {
                    // Physical parent endpoints retain their role when a
                    // selection split happens at the same exact ratio.
                    if matches!(existing.key, ParentParameterKey::Partition(_))
                        && !matches!(parameter.key, ParentParameterKey::Partition(_))
                    {
                        parameters[index] = parameter;
                    }
                    self.parameters = parameters.into_boxed_slice();
                    return Some(());
                },
                Ordering::Greater => {},
            }
        }
        parameters.try_reserve(1).ok()?;
        parameters.insert(insert_at, parameter);
        self.parameters = parameters.into_boxed_slice();
        Some(())
    }

    fn collect_lifted_longitude_seams(&mut self) -> Option<()> {
        let start = self.lifted_longitude_start()?;
        let end = self.lifted_longitude_end()?;
        for level in integer_levels_between(&start, &end, 180, 360)? {
            let seam = exact_affine_level(180, 360, level)?;
            if let ParentCrossing::Present(ratio) = self.parameter_for(&start, &end, &seam)? {
                self.insert_ratio(
                    ParentParameterKey::Partition(PartitionConstraint::LiftedLongitudeSeam),
                    ratio,
                )?;
            }
        }
        Some(())
    }

    fn collect_latitude_reflections(&mut self) -> Option<()> {
        let start = self.latitude_start()?;
        let end = self.latitude_end()?;
        for level in integer_levels_between(&start, &end, 90, 180)? {
            let reflection = exact_affine_level(90, 180, level)?;
            if let ParentCrossing::Present(ratio) = self.parameter_for(&start, &end, &reflection)? {
                self.insert_ratio(
                    ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection),
                    ratio,
                )?;
            }
        }
        Some(())
    }

    fn collect_post_reflection_longitude_seams(&mut self) -> Option<()> {
        let latitude_start = self.latitude_start()?;
        let latitude_end = self.latitude_end()?;
        let longitude_start = self.lifted_longitude_start()?;
        let longitude_end = self.lifted_longitude_end()?;
        // Inside latitude zone z, selection adds 180*z to longitude before
        // wrapping.  Solving its seams in the *parent* map keeps every split
        // exact even though a later evaluator uses outward `Bound`s.
        for zone in integer_levels_between(&latitude_start, &latitude_end, 0, 180)? {
            let lower = exact_affine_level(-90, 180, zone)?;
            let upper = exact_affine_level(90, 180, zone)?;
            let seam_base = 180_i64.checked_sub(180_i64.checked_mul(zone)?)?;
            for seam in integer_levels_between(&longitude_start, &longitude_end, seam_base, 360)? {
                let longitude = exact_affine_level(seam_base, 360, seam)?;
                let ParentCrossing::Present(ratio) =
                    self.parameter_for(&longitude_start, &longitude_end, &longitude)?
                else {
                    continue;
                };
                let latitude = self.coordinate_at(&ratio, &latitude_start, &latitude_end)?;
                if latitude.ordering(&ExactRatio::new(&lower, &ExactExpansion::from_i64(1)?)?)?
                    == Ordering::Less
                    || latitude
                        .ordering(&ExactRatio::new(&upper, &ExactExpansion::from_i64(1)?)?)?
                        == Ordering::Greater
                {
                    continue;
                }
                self.insert_ratio(
                    ParentParameterKey::Partition(PartitionConstraint::PostReflectionLongitudeSeam),
                    ratio,
                )?;
            }
        }
        Some(())
    }

    fn coordinate_at(
        &self,
        parameter: &ExactRatio,
        start: &ExactExpansion,
        end: &ExactExpansion,
    ) -> Option<ExactRatio> {
        let delta = (*end).sub(*start)?;
        let numerator = (*start)
            .product(parameter.denominator)?
            .add(delta.product(parameter.numerator)?)?;
        ExactRatio::new(&numerator, &parameter.denominator)
    }

    fn midpoint(&self, left: &ExactRatio, right: &ExactRatio) -> Option<ExactRatio> {
        let numerator = left
            .numerator
            .product(right.denominator)?
            .add(right.numerator.product(left.denominator)?)?;
        let denominator = left
            .denominator
            .product(right.denominator)?
            .product(ExactExpansion::from_i64(2)?)?;
        ExactRatio::new(&numerator, &denominator)
    }

    fn selection_arc(
        &self,
        interval: &ParentInterval,
        latitude_zone: i64,
        longitude_wrap: i64,
    ) -> Option<AffineSourceArc> {
        let longitude_offset = 180_i64
            .checked_mul(latitude_zone)?
            .checked_sub(360_i64.checked_mul(longitude_wrap)?)?;
        let longitude_offset = ExactExpansion::from_i64(longitude_offset)?;
        let longitude0 = self.lifted_longitude_start()?.add(longitude_offset)?;
        let longitude1 = self.lifted_longitude_end()?.add(longitude_offset)?;
        let longitude_delta = longitude1.sub(longitude0)?;

        let latitude0 = self.latitude_start()?;
        let latitude1 = self.latitude_end()?;
        let latitude_offset = ExactExpansion::from_i64(180_i64.checked_mul(latitude_zone)?)?;
        let (latitude0, latitude1) = if latitude_zone.rem_euclid(2) == 0 {
            (
                latitude0.sub(latitude_offset)?,
                latitude1.sub(latitude_offset)?,
            )
        } else {
            (
                latitude_offset.sub(latitude0)?,
                latitude_offset.sub(latitude1)?,
            )
        };
        let latitude_delta = latitude1.sub(latitude0)?;
        let structure =
            affine_structure(&longitude0, &longitude_delta, &latitude0, &latitude_delta)?;
        let map = AffineParentMap {
            lambda0: degree_expansion_bound(&longitude0).and_then(degree_bound_to_radians)?,
            phi0: degree_expansion_bound(&latitude0).and_then(degree_bound_to_radians)?,
            dlambda: degree_expansion_bound(&longitude_delta).and_then(degree_bound_to_radians)?,
            dphi: degree_expansion_bound(&latitude_delta).and_then(degree_bound_to_radians)?,
            structure,
        };
        let endpoint_roles = [
            endpoint_role(interval.start.key),
            endpoint_role(interval.end.key),
        ];
        let poles = [
            self.selection_endpoint_pole(&interval.start, latitude_zone)
                .ok()?,
            self.selection_endpoint_pole(&interval.end, latitude_zone)
                .ok()?,
        ];
        let identities = [
            self.selection_endpoint_identity(&interval.start, latitude_zone, longitude_wrap)?,
            self.selection_endpoint_identity(&interval.end, latitude_zone, longitude_wrap)?,
        ];
        AffineSourceArc::from_parent_map(
            map,
            Bound::new(interval.start.enclosure.lo, interval.end.enclosure.hi)?,
            endpoint_roles,
            poles,
            identities,
        )
    }

    fn selection_endpoint_pole(
        &self,
        parameter: &ExactParentParameter,
        latitude_zone: i64,
    ) -> Result<Option<PhysicalEndpointKey>, ()> {
        if matches!(parameter.key, ParentParameterKey::Partition(_)) {
            return Ok(None);
        }
        let latitude = self
            .coordinate_at(
                &parameter.ratio,
                &self.latitude_start().ok_or(())?,
                &self.latitude_end().ok_or(())?,
            )
            .ok_or(())?;
        let latitude = selection_latitude_ratio(&latitude, latitude_zone).ok_or(())?;
        let north = ExactRatio::from_i64(90, 1).ok_or(())?;
        let south = ExactRatio::from_i64(-90, 1).ok_or(())?;
        if latitude.ordering(&north).ok_or(())? == Ordering::Equal {
            Ok(Some(PhysicalEndpointKey::NorthPole))
        } else if latitude.ordering(&south).ok_or(())? == Ordering::Equal {
            Ok(Some(PhysicalEndpointKey::SouthPole))
        } else {
            Ok(None)
        }
    }

    fn selection_piece(
        &self,
        start: ExactParentParameter,
        end: ExactParentParameter,
    ) -> Option<SelectionAffinePiece> {
        let midpoint = self.midpoint(&start.ratio, &end.ratio)?;
        let latitude =
            self.coordinate_at(&midpoint, &self.latitude_start()?, &self.latitude_end()?)?;
        let latitude_bound = latitude.certified_bound()?;
        let latitude_midpoint = latitude_bound.lo + (latitude_bound.hi - latitude_bound.lo) * 0.5;
        let zone_proposal = ((latitude_midpoint + 90.0) / HALF_TURN_DEGREES).floor();
        if !zone_proposal.is_finite()
            || zone_proposal < i64::MIN as f64
            || zone_proposal > i64::MAX as f64
        {
            return None;
        }
        // A certified bound can straddle a stored-double neighbour of a strip
        // edge even though the exact midpoint is strictly inside one strip:
        // at `nextafter(90, 0)` its rounded floating midpoint is 90.  Use the
        // float only to nominate nearby integer strips, then select by the
        // exact ratio. This is not an epsilon: exactly one open strip can
        // satisfy the two strict comparisons.
        let proposed_zone = zone_proposal as i64;
        let zone = (-1_i64..=1).find_map(|delta| {
            let zone = proposed_zone.checked_add(delta)?;
            let lower = ExactRatio::new(
                &exact_affine_level(-90, 180, zone)?,
                &ExactExpansion::from_i64(1)?,
            )?;
            let upper = ExactRatio::new(
                &exact_affine_level(90, 180, zone)?,
                &ExactExpansion::from_i64(1)?,
            )?;
            (latitude.ordering(&lower)? == Ordering::Greater
                && latitude.ordering(&upper)? == Ordering::Less)
                .then_some(zone)
        })?;
        let zone_lower = ExactRatio::new(
            &exact_affine_level(-90, 180, zone)?,
            &ExactExpansion::from_i64(1)?,
        )?;
        let zone_upper = ExactRatio::new(
            &exact_affine_level(90, 180, zone)?,
            &ExactExpansion::from_i64(1)?,
        )?;
        debug_assert_eq!(latitude.ordering(&zone_lower)?, Ordering::Greater);
        debug_assert_eq!(latitude.ordering(&zone_upper)?, Ordering::Less);

        let longitude = self.coordinate_at(
            &midpoint,
            &self.lifted_longitude_start()?,
            &self.lifted_longitude_end()?,
        )?;
        let offset = ExactExpansion::from_i64(180_i64.checked_mul(zone)?)?;
        let selection_numerator = longitude
            .numerator
            .add(offset.product(longitude.denominator)?)?;
        let selection_longitude = ExactRatio::new(&selection_numerator, &longitude.denominator)?;
        let longitude_bound = selection_longitude.certified_bound()?;
        let longitude_midpoint =
            longitude_bound.lo + (longitude_bound.hi - longitude_bound.lo) * 0.5;
        let wrap_proposal = ((longitude_midpoint + HALF_TURN_DEGREES) / FULL_TURN_DEGREES).floor();
        if !wrap_proposal.is_finite()
            || wrap_proposal < i64::MIN as f64
            || wrap_proposal > i64::MAX as f64
        {
            return None;
        }
        let mut longitude_wrap = wrap_proposal as i64;
        let mut lower = ExactRatio::new(
            &exact_affine_level(-180, 360, longitude_wrap)?,
            &ExactExpansion::from_i64(1)?,
        )?;
        // The lower-bound equality is the physical antimeridian.  Preserve a
        // positive written lift as +180 by selecting the preceding sheet;
        // otherwise -180 stays on its written sheet.  This is identity, not
        // an epsilon seam preference.
        if selection_longitude.ordering(&lower)? == Ordering::Equal
            && selection_longitude.numerator.ordering() == Ordering::Greater
        {
            longitude_wrap = longitude_wrap.checked_sub(1)?;
            lower = ExactRatio::new(
                &exact_affine_level(-180, 360, longitude_wrap)?,
                &ExactExpansion::from_i64(1)?,
            )?;
        }
        let upper = ExactRatio::new(
            &exact_affine_level(180, 360, longitude_wrap)?,
            &ExactExpansion::from_i64(1)?,
        )?;
        if selection_longitude.ordering(&lower)? == Ordering::Less
            || selection_longitude.ordering(&upper)? == Ordering::Greater
        {
            return None;
        }
        let interval = ParentInterval { start, end };
        let selection = [
            self.selection_planar_point(&interval.start, zone, longitude_wrap)?,
            self.selection_planar_point(&interval.end, zone, longitude_wrap)?,
        ];
        let arc = self.selection_arc(&interval, zone, longitude_wrap)?;
        Some(SelectionAffinePiece {
            edge: self.key,
            polygon: self.polygon,
            interval,
            latitude_zone: zone,
            longitude_wrap,
            selection,
            arc,
        })
    }

    /// Exact degree endpoint in the shared reflect/wrap selection image.
    /// This is deliberately evaluated from the parent ratio rather than from
    /// `AffineSourceArc`'s outward radian map: topology owns the source's
    /// stored-double identity, while the radian map only evaluates contacts.
    fn selection_planar_point(
        &self,
        parameter: &ExactParentParameter,
        latitude_zone: i64,
        longitude_wrap: i64,
    ) -> Option<ExactSelectionPoint> {
        let longitude = self.coordinate_at(
            &parameter.ratio,
            &self.lifted_longitude_start()?,
            &self.lifted_longitude_end()?,
        )?;
        let latitude = self.coordinate_at(
            &parameter.ratio,
            &self.latitude_start()?,
            &self.latitude_end()?,
        )?;
        let longitude_offset = 180_i64
            .checked_mul(latitude_zone)?
            .checked_sub(360_i64.checked_mul(longitude_wrap)?)?;
        let unwrapped_longitude = longitude.add(&ExactRatio::from_i64(
            longitude_offset.checked_add(360_i64.checked_mul(longitude_wrap)?)?,
            1,
        )?)?;
        let mut longitude = unwrapped_longitude.clone().sub(&ExactRatio::from_i64(
            360_i64.checked_mul(longitude_wrap)?,
            1,
        )?)?;
        // Preserve the stored positive antimeridian spelling.  The exact
        // branch mirrors `selection_image_point`: a normalized -180 whose
        // unwrapped coordinate is positive denotes written +180, not an
        // interchangeable seam side.
        if longitude.ordering(&ExactRatio::from_i64(-180, 1)?)? == Ordering::Equal
            && unwrapped_longitude.numerator.ordering() == Ordering::Greater
        {
            longitude = ExactRatio::from_i64(180, 1)?;
        }
        let latitude_offset = ExactRatio::from_i64(180_i64.checked_mul(latitude_zone)?, 1)?;
        let latitude = if latitude_zone.rem_euclid(2) == 0 {
            latitude.sub(&latitude_offset)?
        } else {
            latitude_offset.sub(&latitude)?
        };
        Some(ExactSelectionPoint {
            longitude,
            latitude,
        })
    }

    fn selection_pieces(&self, output: &mut Vec<SelectionAffinePiece>) -> Option<()> {
        for parameters in self.parameters.windows(2) {
            output.try_reserve(1).ok()?;
            output.push(self.selection_piece(parameters[0].clone(), parameters[1].clone())?);
        }
        Some(())
    }

    /// Build the source-authority arc only on an exact parent interval whose
    /// midpoint is strictly inside the physical latitude strip.  A strip cut
    /// remains a partition identity, never a pole endpoint manufactured from
    /// an outward enclosure.
    fn authority_pieces(&self, output: &mut Vec<AuthorityAffinePiece>) -> Option<()> {
        for parameters in self.parameters.windows(2) {
            let start = parameters[0].clone();
            let end = parameters[1].clone();
            let midpoint = self.midpoint(&start.ratio, &end.ratio)?;
            let latitude =
                self.coordinate_at(&midpoint, &self.latitude_start()?, &self.latitude_end()?)?;
            let south = ExactRatio::from_i64(-90, 1)?;
            let north = ExactRatio::from_i64(90, 1)?;
            if latitude.ordering(&south)? != Ordering::Greater
                || latitude.ordering(&north)? != Ordering::Less
            {
                continue;
            }
            output.try_reserve(1).ok()?;
            output.push(AuthorityAffinePiece {
                edge: self.key,
                polygon: self.polygon,
                arc: self.authority_arc(&ParentInterval { start, end })?,
            });
        }
        Some(())
    }

    fn authority_arc(&self, interval: &ParentInterval) -> Option<AffineSourceArc> {
        let longitude0 = self.lifted_longitude_start()?;
        let longitude1 = self.lifted_longitude_end()?;
        let latitude0 = self.latitude_start()?;
        let latitude1 = self.latitude_end()?;
        let longitude_delta = longitude1.sub(longitude0)?;
        let latitude_delta = latitude1.sub(latitude0)?;
        let structure =
            affine_structure(&longitude0, &longitude_delta, &latitude0, &latitude_delta)?;
        let map = AffineParentMap {
            lambda0: degree_expansion_bound(&longitude0).and_then(degree_bound_to_radians)?,
            phi0: degree_expansion_bound(&latitude0).and_then(degree_bound_to_radians)?,
            dlambda: degree_expansion_bound(&longitude_delta).and_then(degree_bound_to_radians)?,
            dphi: degree_expansion_bound(&latitude_delta).and_then(degree_bound_to_radians)?,
            structure,
        };
        let endpoint_roles = [
            endpoint_role(interval.start.key),
            endpoint_role(interval.end.key),
        ];
        let poles = [
            self.authority_endpoint_pole(&interval.start).ok()?,
            self.authority_endpoint_pole(&interval.end).ok()?,
        ];
        let identities = [
            self.authority_endpoint_identity(&interval.start)?,
            self.authority_endpoint_identity(&interval.end)?,
        ];
        AffineSourceArc::from_parent_map(
            map,
            Bound::new(interval.start.enclosure.lo, interval.end.enclosure.hi)?,
            endpoint_roles,
            poles,
            identities,
        )
    }

    fn authority_endpoint_identity(
        &self,
        parameter: &ExactParentParameter,
    ) -> Option<AffineEndpointIdentity> {
        let longitude = self.coordinate_at(
            &parameter.ratio,
            &self.lifted_longitude_start()?,
            &self.lifted_longitude_end()?,
        )?;
        let latitude = self.coordinate_at(
            &parameter.ratio,
            &self.latitude_start()?,
            &self.latitude_end()?,
        )?;
        endpoint_identity(&longitude, &latitude)
    }

    fn selection_endpoint_identity(
        &self,
        parameter: &ExactParentParameter,
        latitude_zone: i64,
        longitude_wrap: i64,
    ) -> Option<AffineEndpointIdentity> {
        let longitude = self.coordinate_at(
            &parameter.ratio,
            &self.lifted_longitude_start()?,
            &self.lifted_longitude_end()?,
        )?;
        let offset = 180_i64
            .checked_mul(latitude_zone)?
            .checked_sub(360_i64.checked_mul(longitude_wrap)?)?;
        let longitude = longitude.add(&ExactRatio::from_i64(offset, 1)?)?;
        let latitude = self.coordinate_at(
            &parameter.ratio,
            &self.latitude_start()?,
            &self.latitude_end()?,
        )?;
        let latitude = selection_latitude_ratio(&latitude, latitude_zone)?;
        endpoint_identity(&longitude, &latitude)
    }

    fn authority_endpoint_pole(
        &self,
        parameter: &ExactParentParameter,
    ) -> Result<Option<PhysicalEndpointKey>, ()> {
        if matches!(parameter.key, ParentParameterKey::Partition(_)) {
            return Ok(None);
        }
        let latitude = self
            .coordinate_at(
                &parameter.ratio,
                &self.latitude_start().ok_or(())?,
                &self.latitude_end().ok_or(())?,
            )
            .ok_or(())?;
        let north = ExactRatio::from_i64(90, 1).ok_or(())?;
        let south = ExactRatio::from_i64(-90, 1).ok_or(())?;
        if latitude.ordering(&north).ok_or(())? == Ordering::Equal {
            Ok(Some(PhysicalEndpointKey::NorthPole))
        } else if latitude.ordering(&south).ok_or(())? == Ordering::Equal {
            Ok(Some(PhysicalEndpointKey::SouthPole))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthorityAffinePiece {
    #[expect(
        dead_code,
        reason = "retained for authority-to-source unit certificates"
    )]
    edge: SourceEdgeKey,
    polygon: bool,
    arc: AffineSourceArc,
}

impl AuthorityAffinePiece {
    pub(crate) const fn is_polygon(&self) -> bool {
        self.polygon
    }

    pub(crate) const fn arc(&self) -> AffineSourceArc {
        self.arc
    }
}

impl SelectionAffinePiece {
    pub(crate) const fn is_polygon(&self) -> bool {
        self.polygon
    }

    pub(crate) const fn arc(&self) -> AffineSourceArc {
        self.arc
    }

    /// Identity and pure longitude-wrap pieces are already represented by the
    /// authority image: both have the same unit-sphere trace, and its exact
    /// periodic PIP plus ordered seam shifts retain every contact. Only a
    /// latitude reflection changes that trace and adds positive-only authority
    /// capable of vetoing an authority negative.
    pub(crate) const fn is_positive_only(&self) -> bool {
        self.latitude_zone != 0
    }
}

fn degree_expansion_bound(value: &ExactExpansion) -> Option<Bound> {
    ExactRatio::from_expansion(value)?.certified_bound()
}

const fn endpoint_role(key: ParentParameterKey) -> SourceEndpointRole {
    match key {
        ParentParameterKey::Start => SourceEndpointRole::ParentStart,
        ParentParameterKey::Partition(_) => SourceEndpointRole::Partition,
        ParentParameterKey::End => SourceEndpointRole::ParentEnd,
    }
}

fn selection_latitude_ratio(latitude: &ExactRatio, zone: i64) -> Option<ExactRatio> {
    let offset = ExactExpansion::from_i64(180_i64.checked_mul(zone)?)?;
    let offset = offset.product(latitude.denominator)?;
    let numerator = if zone.rem_euclid(2) == 0 {
        latitude.numerator.sub(offset)?
    } else {
        offset.sub(latitude.numerator)?
    };
    ExactRatio::new(&numerator, &latitude.denominator)
}

fn affine_structure(
    longitude0: &ExactExpansion,
    longitude_delta: &ExactExpansion,
    latitude0: &ExactExpansion,
    latitude_delta: &ExactExpansion,
) -> Option<AffineStructure> {
    if longitude_delta.ordering() == Ordering::Equal && latitude_delta.ordering() == Ordering::Equal
    {
        return Some(AffineStructure::Point);
    }
    if longitude_delta.ordering() == Ordering::Equal {
        return Some(AffineStructure::ConstantLongitude {
            axis: structural_axis(longitude0).ok()?,
        });
    }
    if latitude0.ordering() == Ordering::Equal && latitude_delta.ordering() == Ordering::Equal {
        return Some(AffineStructure::Equator);
    }
    Some(AffineStructure::General)
}

fn structural_axis(longitude: &ExactExpansion) -> Result<Option<AxisMeridian>, ()> {
    let proposal = longitude.checked_estimate().ok_or(())?;
    for (axis, base) in [
        (AxisMeridian::Prime, 0_i64),
        (AxisMeridian::EastQuarter, 90_i64),
        (AxisMeridian::Antimeridian, 180_i64),
        (AxisMeridian::WestQuarter, -90_i64),
    ] {
        let turns = ((proposal - base as f64) / FULL_TURN_DEGREES).round();
        if !turns.is_finite() || turns < i64::MIN as f64 || turns > i64::MAX as f64 {
            return Err(());
        }
        let candidate = ExactExpansion::from_i64(base)
            .and_then(|value| value.add(ExactExpansion::exact_degree_turns(turns as i64)?))
            .ok_or(())?;
        if longitude.sub(candidate).ok_or(())?.ordering() == Ordering::Equal {
            return Ok(Some(axis));
        }
    }
    Ok(None)
}

/// Recognize a coordinate-axis longitude only from the retained exact source
/// ratio.  The estimate selects a nearby integer sheet; the exact ratio
/// comparison is the authority, so a merely-close rounded endpoint can never
/// manufacture an axis token.
enum StructuralAxisRatio {
    None,
    Axis(AxisMeridian),
}

fn structural_axis_ratio(longitude: &ExactRatio) -> Option<StructuralAxisRatio> {
    let bound = longitude.certified_bound()?;
    let proposal = bound.lo + (bound.hi - bound.lo) * 0.5;
    for (axis, base) in [
        (AxisMeridian::Prime, 0_i64),
        (AxisMeridian::EastQuarter, 90_i64),
        (AxisMeridian::Antimeridian, 180_i64),
        (AxisMeridian::WestQuarter, -90_i64),
    ] {
        let turns = ((proposal - base as f64) / FULL_TURN_DEGREES).round();
        if !turns.is_finite() || turns < i64::MIN as f64 || turns > i64::MAX as f64 {
            return None;
        }
        let candidate = ExactExpansion::from_i64(base)?
            .add(ExactExpansion::exact_degree_turns(turns as i64)?)?;
        if longitude.ordering(&ExactRatio::from_expansion(&candidate)?)? == Ordering::Equal {
            return Some(StructuralAxisRatio::Axis(axis));
        }
    }
    Some(StructuralAxisRatio::None)
}

fn endpoint_identity(
    longitude: &ExactRatio,
    latitude: &ExactRatio,
) -> Option<AffineEndpointIdentity> {
    let equator = ExactRatio::from_i64(0, 1)?;
    Some(AffineEndpointIdentity {
        longitude: match structural_axis_ratio(longitude)? {
            StructuralAxisRatio::None => None,
            StructuralAxisRatio::Axis(axis) => Some(axis),
        },
        latitude: (latitude.ordering(&equator)? == Ordering::Equal)
            .then_some(AxisLatitude::Equator),
    })
}

fn exact_affine_level(base: i64, stride: i64, level: i64) -> Option<ExactExpansion> {
    let offset = stride.checked_mul(level)?.checked_add(base)?;
    ExactExpansion::from_i64(offset)
}

/// Candidate level generation uses a binary64 proposal only to find the first
/// nearby integer. Exact expansion signs then walk to the true floor/ceiling;
/// a large-coordinate ULP therefore cannot make a seam disappear.
fn integer_levels_between(
    start: &ExactExpansion,
    end: &ExactExpansion,
    base: i64,
    stride: i64,
) -> Option<std::ops::RangeInclusive<i64>> {
    if stride <= 0 {
        return None;
    }
    let (lower, upper) = if start.ordering() == Ordering::Greater {
        (end, start)
    } else {
        (start, end)
    };
    let lower = exact_floor_level(lower, base, stride)?;
    let mut upper_floor = exact_floor_level(upper, base, stride)?;
    if exact_affine_level(base, stride, upper_floor)?
        .sub(*upper)?
        .ordering()
        == Ordering::Less
    {
        upper_floor = upper_floor.checked_add(1)?;
    }
    Some(lower..=upper_floor)
}

fn exact_floor_level(value: &ExactExpansion, base: i64, stride: i64) -> Option<i64> {
    let proposal = ((value.checked_estimate()? - base as f64) / stride as f64).floor();
    if !proposal.is_finite() || proposal < i64::MIN as f64 || proposal > i64::MAX as f64 {
        return None;
    }
    let mut level = proposal as i64;
    loop {
        let next = level.checked_add(1)?;
        if exact_affine_level(base, stride, next)?
            .sub(*value)?
            .ordering()
            == Ordering::Greater
        {
            break;
        }
        level = next;
    }
    loop {
        if exact_affine_level(base, stride, level)?
            .sub(*value)?
            .ordering()
            != Ordering::Greater
        {
            break;
        }
        level = level.checked_sub(1)?;
    }
    Some(level)
}

const fn same_physical_latitude(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits()
}

fn same_physical_longitude(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits()
        || (left.to_bits() == (-HALF_TURN_DEGREES).to_bits()
            && right.to_bits() == HALF_TURN_DEGREES.to_bits())
        || (left.to_bits() == HALF_TURN_DEGREES.to_bits()
            && right.to_bits() == (-HALF_TURN_DEGREES).to_bits())
}

fn can_coalesce(previous: LiftedVertex, current: LiftedVertex, next: LiftedVertex) -> Option<bool> {
    let ab_x = current.longitude.difference(previous.longitude)?;
    let ab_y = ExactExpansion::from_f64(current.latitude)?
        .sub(ExactExpansion::from_f64(previous.latitude)?)?;
    let bc_x = next.longitude.difference(current.longitude)?;
    let bc_y = ExactExpansion::from_f64(next.latitude)?
        .sub(ExactExpansion::from_f64(current.latitude)?)?;
    let determinant = ab_x.product(bc_y)?.sub(ab_y.product(bc_x)?)?;
    if determinant.ordering() != Ordering::Equal
        || !closed_between(previous.longitude, current.longitude, next.longitude)?
        || !closed_between_raw(previous.latitude, current.latitude, next.latitude)?
    {
        return Some(false);
    }
    let direct = lifted_step(previous.longitude.raw, next.longitude.raw)?;
    let direct_turn = previous.longitude.turns.checked_add(direct.turn_delta)?;
    // A long path through a collinear point may not be replaced by its short
    // direct periodic lift.  This is distinct from geometric collinearity.
    Some(direct_turn == next.longitude.turns)
}

fn closed_between(
    left: LiftedLongitude,
    middle: LiftedLongitude,
    right: LiftedLongitude,
) -> Option<bool> {
    let left_right = left.ordering(right)?;
    let left_middle = left.ordering(middle)?;
    let middle_right = middle.ordering(right)?;
    Some(match left_right {
        Ordering::Less => left_middle != Ordering::Greater && middle_right != Ordering::Greater,
        Ordering::Greater => left_middle != Ordering::Less && middle_right != Ordering::Less,
        Ordering::Equal => left_middle == Ordering::Equal,
    })
}

fn closed_between_raw(left: f64, middle: f64, right: f64) -> Option<bool> {
    let left = ExactExpansion::from_f64(left)?;
    let middle = ExactExpansion::from_f64(middle)?;
    let right = ExactExpansion::from_f64(right)?;
    let left_right = left.sub(right)?.ordering();
    let left_middle = left.sub(middle)?.ordering();
    let middle_right = middle.sub(right)?.ordering();
    Some(match left_right {
        Ordering::Less => left_middle != Ordering::Greater && middle_right != Ordering::Greater,
        Ordering::Greater => left_middle != Ordering::Less && middle_right != Ordering::Less,
        Ordering::Equal => left_middle == Ordering::Equal,
    })
}

/// A stored source point, retaining the exact binary64 inputs instead of a
/// rounded selection-image coordinate.  Points are zero-dimensional source
/// features, not degenerate source edges: later containment owns them
/// directly and no artificial `AffineSourceArc` is introduced here.
#[derive(Clone, Copy, Debug)]
struct SourcePoint {
    key: SourceVertexKey,
    longitude: ExactExpansion,
    latitude: ExactExpansion,
}

/// The unit-sphere selection image of a source point.  Reflection and wrap
/// are exact expansion transforms of the stored doubles; the binary64
/// proposal in `exact_floor_level` never decides a strip or seam.
#[derive(Clone, Copy, Debug)]
struct SelectionPoint {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "retained for source-point identity unit certificates"
        )
    )]
    key: SourceVertexKey,
    longitude: ExactExpansion,
    latitude: ExactExpansion,
}

fn selection_image_point(point: &SourcePoint) -> Option<SelectionPoint> {
    // `zone` is floor((latitude + 90) / 180), found by an exact sign walk.
    // This is the shared spherical selection image for H3 and S2: reflect
    // through a pole and then wrap longitude.  Clamping would change the
    // source geometry and cannot be a negative certificate.
    let zone = exact_floor_level(&point.latitude, -90, 180)?;
    let latitude_offset = exact_affine_level(0, 180, zone)?;
    let latitude = if zone.rem_euclid(2) == 0 {
        point.latitude.sub(latitude_offset)?
    } else {
        latitude_offset.sub(point.latitude)?
    };
    let lifted_longitude = point.longitude.add(exact_affine_level(0, 180, zone)?)?;
    let wrap = exact_floor_level(&lifted_longitude, -180, 360)?;
    let mut longitude = lifted_longitude.sub(exact_affine_level(0, 360, wrap)?)?;
    // The two stored physical antimeridian doubles retain their written side;
    // this is exact identity, not a tolerance-based seam selection.
    if longitude.is_exact(-HALF_TURN_DEGREES)? && lifted_longitude.ordering() == Ordering::Greater {
        longitude = ExactExpansion::from_f64(HALF_TURN_DEGREES)?;
    }
    Some(SelectionPoint {
        key: point.key,
        longitude,
        latitude,
    })
}

/// Private source lift.  This step owns only topology and selection-image
/// preparation; authority PIP/rectangle classification is added with the H3
/// traversal rather than duplicated in an early proxy path.
#[derive(Debug)]
pub(crate) struct GridAffineSource {
    target: SphericalGridTarget,
    // An exact canonical lon/lat rectangle is a complete source model for
    // cap classification.  Keep this separate from the lifted general
    // carrier: a polar rectangle can intentionally retain an uncertain
    // selection image while still proving cap disjointness or containment.
    exact_axis_rect: Option<Bounds>,
    // The stored source latitude envelope is exact for linear lon/lat edges
    // and zero-degree polygon fills.  A periodic polygon ring closes through
    // a pole, so its raw endpoint extrema alone deliberately cannot prune.
    authority_latitude: Option<Bound>,
    chains: Box<[LiftedChain]>,
    ring_fills: Box<[LiftedRingFill]>,
    authority_points: Box<[SourcePoint]>,
    selection_points: Box<[SelectionPoint]>,
    // R15-S authority-identity disposition: selection differs from authority
    // only for a nonzero latitude reflection zone (`is_positive_only` below).
    // Public H3/S2 cover ingress rejects every such source before this carrier
    // is constructed, so the draft incident-owner fixture was unreachable and
    // its deferred mutation was deleted rather than left as a vacuous claim.
    // The two images stay structurally separate for private fail-open paths.
    authority_edges: Box<[SymbolicAffineEdge]>,
    authority_pieces: Box<[AuthorityAffinePiece]>,
    selection_edges: Box<[SelectionAffinePiece]>,
    unknown: bool,
}

impl GridAffineSource {
    pub(crate) fn new(
        source: &Shape,
        target: SphericalGridTarget,
    ) -> Result<Self, TryReserveError> {
        // Geographic admission accepts a minute exterior rounding sliver at
        // either pole.  Canonicalize the complete shape before any carrier
        // state is derived: bounds, rectangle recognition, chains, and point
        // witnesses must agree on the one physical-pole topology.
        let normalized_source = normalize_grid_source(source);
        let source = &normalized_source;
        let exact_axis_rect = source.bounds().filter(|bounds| {
            exact_axis_rectangle(source, *bounds)
                && canonical_axis_rectangle_longitude(*bounds)
                && (-180.0..=180.0).contains(&bounds.minx())
                && (-180.0..=180.0).contains(&bounds.maxx())
                && (-90.0..=90.0).contains(&bounds.miny())
                && (-90.0..=90.0).contains(&bounds.maxy())
        });
        let authority_latitude = source
            .bounds()
            .and_then(|bounds| Bound::new(bounds.miny(), bounds.maxy()));
        let mut chains = Vec::new();
        let mut points = Vec::new();
        let mut component = 0;
        collect_shape_chains(source, &mut chains, &mut points, &mut component)?;
        let mut unknown = false;
        let mut lifted = Vec::new();
        let mut ring_fills = Vec::new();
        let mut authority_points = Vec::new();
        let mut selection_points = Vec::new();
        let mut authority_edges = Vec::new();
        let mut authority_pieces = Vec::new();
        let mut selection_edges = Vec::new();
        lifted.try_reserve(chains.len())?;
        ring_fills.try_reserve(chains.len())?;
        authority_points.try_reserve(points.len())?;
        selection_points.try_reserve(points.len())?;
        authority_pieces.try_reserve(chains.len())?;
        for input in &chains {
            append_lifted_chain(
                input,
                &mut lifted,
                &mut ring_fills,
                &mut authority_edges,
                &mut authority_pieces,
                &mut selection_edges,
                &mut unknown,
            )?;
        }
        for input in points {
            let (Some(longitude), Some(latitude)) = (
                ExactExpansion::from_f64(input.longitude),
                ExactExpansion::from_f64(input.latitude),
            ) else {
                unknown = true;
                continue;
            };
            let point = SourcePoint {
                key: SourceVertexKey {
                    component: input.component,
                    ring: 0,
                    ordinal: input.ordinal,
                },
                longitude,
                latitude,
            };
            authority_points.push(point);
            append_source_point(&point, &mut selection_points, &mut unknown);
        }
        Ok(Self {
            target,
            exact_axis_rect,
            authority_latitude,
            chains: lifted.into_boxed_slice(),
            ring_fills: ring_fills.into_boxed_slice(),
            authority_points: authority_points.into_boxed_slice(),
            selection_points: selection_points.into_boxed_slice(),
            authority_edges: authority_edges.into_boxed_slice(),
            authority_pieces: authority_pieces.into_boxed_slice(),
            selection_edges: selection_edges.into_boxed_slice(),
            unknown,
        })
    }

    pub(crate) const fn target(&self) -> SphericalGridTarget {
        self.target
    }

    pub(crate) const fn is_unknown(&self) -> bool {
        self.unknown
    }

    /// Whether S2's historical planar working-shape proposals need an exact
    /// affine corroboration before they may establish `Outside` or `Interior`.
    ///
    /// The carrier is built once per covering.  Ordinary physical, unwrapped
    /// sources stay on S2's existing rectangle fast path; a split working
    /// shape, periodic lift, pole reflection, positive selection image, or
    /// construction uncertainty makes the proposal non-authoritative.
    pub(crate) fn s2_needs_corroboration(&self, relation: WorkingShapeRelation) -> bool {
        relation == WorkingShapeRelation::AntimeridianSplit
            || self.unknown
            || self.has_positive_selection_polygon()
            || self.has_positive_selection_lower()
            || self.chains.iter().any(|chain| {
                chain.vertices.iter().any(|vertex| {
                    vertex.longitude.turns != 0 || !(-90.0..=90.0).contains(&vertex.latitude)
                }) || chain
                    .edges
                    .iter()
                    .any(|kind| !matches!(kind, LiftKind::Shortest))
            })
    }

    /// Whether this candidate physically contains a retained raw source
    /// endpoint under S2's closed-cell semantics.  A split selection image may
    /// put the same spherical point on a different sheet, so such an owner
    /// vetoes a planar `Outside` proposal.
    ///
    /// This is only a veto: malformed or out-of-domain authority retains the
    /// candidate rather than choosing a rounded replacement owner.
    pub(crate) fn s2_raw_owner_descends_from(&self, candidate: CellId) -> bool {
        // A structural axis rectangle is a complete carrier. Its exact
        // rectangle classification already contains every true S2 cell, so a
        // later source-lift uncertainty or canonical pole owner cannot veto
        // that certified `Outside` result.
        if self.exact_axis_rect.is_some() {
            return false;
        }
        let contains_raw = |longitude: f64, latitude: f64| {
            if !longitude.is_finite()
                || !latitude.is_finite()
                || !(-180.0..=180.0).contains(&longitude)
                || !(-90.0..=90.0).contains(&latitude)
            {
                return true;
            }
            // A canonical `CellId::from_lonlat` chooses one face at a pole or
            // cube edge. The veto must instead ask the candidate's native
            // closed cell, otherwise an unrelated face can retain a whole
            // seam/polar subtree. This is positive-only: a rounding ambiguity
            // here can widen a cover but cannot prune one.
            S2Cell::from_id(candidate).contains_point(s2_lonlat_to_point(longitude, latitude))
        };
        self.unknown
            || self.authority_points.iter().any(|point| {
                point
                    .longitude
                    .checked_estimate()
                    .zip(point.latitude.checked_estimate())
                    .is_none_or(|(longitude, latitude)| contains_raw(longitude, latitude))
            })
            || self.authority_edges.iter().any(|edge| {
                contains_raw(edge.start.longitude.raw, edge.start.latitude)
                    || contains_raw(edge.end.longitude.raw, edge.end.latitude)
            })
    }

    /// Whether a whole-shape cap certificate must retain every candidate.
    ///
    /// Multipart sources may carry uncertainty introduced by one component
    /// even when another component has an exact, independently certifiable
    /// carrier.  H3 splits only this global fail-open case; an exact axis
    /// rectangle remains atomic, including its polygon holes.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "H3 coverer unit tests inspect the aggregate cap certificate"
        )
    )]
    pub(crate) fn cap_is_globally_fail_open(&self) -> bool {
        self.exact_axis_rect.is_none()
            && (self.unknown
                || self.has_positive_selection_polygon()
                || self.has_positive_selection_lower())
    }

    /// Exact source-vs-degree-window classification for H3 cap pruning and
    /// logical-bbox leaves.  The rectangle is an outward enclosure of the
    /// cell logical bbox, so an `Outside` result is a valid negative for the
    /// true cell.  A reflected selection image can only retain a candidate.
    pub(crate) fn classify_rect(&self, candidate: DegreeWindowResult) -> RectClass {
        // This certificate models the whole source exactly.  It must run
        // before the general lift's global uncertainty veto: that veto is a
        // fail-open fallback for symbolic sources, not a reason to discard a
        // complete rectangle proof.
        if let Some(bounds) = self.exact_axis_rect {
            return classify_exact_axis_rectangle(bounds, candidate);
        }
        if self.unknown
            || self.has_positive_selection_polygon()
            || self.has_positive_selection_lower()
        {
            return RectClass::Boundary;
        }
        let DegreeWindowResult::Windows(CertifiedDegreeWindows {
            latitude,
            longitude,
        }) = candidate
        else {
            return RectClass::Boundary;
        };
        // This is a proof over the authoritative *linear* source, not a
        // working-image bound.  Every segment and every zero-degree polygon
        // fill is bounded by its endpoint latitudes.  A nonzero ring degree,
        // however, has a signed pole roof: its raw vertices at (say) lat 60
        // enclose the north cap through lat 90.  Those rings must take the
        // exact periodic PIP below rather than turn a raw-latitude shortcut
        // into an unsound negative.
        if !self.has_periodic_polygon_fill()
            && self
                .authority_latitude
                .is_some_and(|source| strictly_disjoint_degree_bounds(source, latitude))
        {
            return RectClass::Outside;
        }
        match longitude {
            CertifiedLongitudeDegrees::Full => self.classify_rect_window(
                Bound::new(-180.0, 180.0).expect("finite longitude domain"),
                latitude,
            ),
            CertifiedLongitudeDegrees::One(longitude) => {
                self.classify_rect_window(longitude, latitude)
            },
            CertifiedLongitudeDegrees::Two([west, east]) => {
                let west = self.classify_rect_window(west, latitude);
                let east = self.classify_rect_window(east, latitude);
                match (west, east) {
                    (RectClass::Outside, RectClass::Outside) => RectClass::Outside,
                    (RectClass::Interior, RectClass::Interior) => RectClass::Interior,
                    _ => RectClass::Boundary,
                }
            },
        }
    }

    fn classify_rect_window(&self, longitude: Bound, latitude: Bound) -> RectClass {
        for point in self.authority_points() {
            let Some(point) = point else {
                return RectClass::Boundary;
            };
            if !point.is_proven_physical_latitude()
                || degree_bounds_overlap(point.latitude, latitude)
                    // Stored authority points are canonical physical
                    // longitude doubles, unlike lifted authority edges below;
                    // their only possible copies are the neighbouring sheets.
                    && (-1..=1).any(|turn| {
                        Bound::exact(360.0 * f64::from(turn))
                            .and_then(|offset| longitude.add(offset))
                            .is_none_or(|shifted| degree_bounds_overlap(point.longitude, shifted))
                    })
            {
                return RectClass::Boundary;
            }
        }
        for edge in &self.authority_edges {
            let (Some(start), Some(end)) = (
                ExactPlanarPoint::from_lifted(edge.start),
                ExactPlanarPoint::from_lifted(edge.end),
            ) else {
                return RectClass::Boundary;
            };
            let (Some(start_latitude), Some(end_latitude)) = (
                degree_expansion_bound(&start.latitude),
                degree_expansion_bound(&end.latitude),
            ) else {
                return RectClass::Boundary;
            };
            if !(-90.0..=90.0).contains(&start_latitude.lo)
                || !(-90.0..=90.0).contains(&start_latitude.hi)
                || !(-90.0..=90.0).contains(&end_latitude.lo)
                || !(-90.0..=90.0).contains(&end_latitude.hi)
            {
                return RectClass::Boundary;
            }
            if exact_point_touches_rectangle(&start, longitude, latitude).unwrap_or(true)
                || exact_point_touches_rectangle(&end, longitude, latitude).unwrap_or(true)
            {
                return RectClass::Boundary;
            }
            if exact_segment_touches_rectangle(&start, &end, longitude, latitude).unwrap_or(true) {
                return RectClass::Boundary;
            }
        }
        if !self.has_polygon() {
            return RectClass::Outside;
        }
        // An S2 circular bound may retain a seam sheet whose certified degree
        // enclosure is a singleton or one stored-double wide.  The preceding
        // exact edge scans already proved that no source boundary touches the
        // closed vertical strip, so its closed endpoint is a topological
        // witness for the component.  This is structural IEEE adjacency, not
        // an epsilon tolerance or a rounded interior sample.
        let longitude = strict_window_representative(longitude).or_else(|| {
            (longitude.lo.is_finite()
                && longitude.hi.is_finite()
                && longitude.lo <= longitude.hi
                && longitude.lo.next_up() >= longitude.hi)
                .then_some(longitude.lo)
        });
        let Some(longitude) = longitude else {
            return RectClass::Boundary;
        };
        let Some(latitude) = strict_window_representative(latitude) else {
            return RectClass::Boundary;
        };
        match self.authority_point_class(longitude, latitude) {
            GridPointClass::Exterior => RectClass::Outside,
            GridPointClass::Interior => RectClass::Interior,
            GridPointClass::Boundary | GridPointClass::Unknown => RectClass::Boundary,
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "module unit tests inspect the selected arc inventory"
        )
    )]
    pub(crate) fn selection_arcs(&self) -> Option<impl ExactSizeIterator<Item = &AffineSourceArc>> {
        (!self.unknown).then(|| self.selection_edges.iter().map(|piece| &piece.arc))
    }

    pub(crate) fn authority_pieces(
        &self,
    ) -> Option<impl ExactSizeIterator<Item = &AuthorityAffinePiece>> {
        (!self.unknown).then(|| self.authority_pieces.iter())
    }

    pub(crate) fn selection_pieces(
        &self,
    ) -> Option<impl ExactSizeIterator<Item = &SelectionAffinePiece>> {
        (!self.unknown).then(|| self.selection_edges.iter())
    }

    pub(crate) fn has_polygon(&self) -> bool {
        !self.ring_fills.is_empty()
    }

    fn has_periodic_polygon_fill(&self) -> bool {
        self.ring_fills.iter().any(|fill| fill.degree != 0)
    }

    /// One retained-authority witness per polygon shell and hole.  A missing
    /// in-strip witness remains an explicit `None` so a leaf cannot silently
    /// turn an unrepresented polar/out-of-strip component into `Outside`.
    pub(crate) fn polygon_authority_witnesses(
        &self,
    ) -> impl ExactSizeIterator<Item = (usize, usize, Option<GridDegreePoint>)> + '_ {
        self.ring_fills.iter().map(|fill| {
            (
                fill.component,
                fill.ring,
                self.chains
                    .get(fill.chain_index)
                    .and_then(first_in_strip_chain_point),
            )
        })
    }

    /// A successful carrier construction owns every exact selected segment of
    /// this source ring.  This lets an entirely out-of-strip raw ring defer to
    /// its selection PIP rather than treating a missing *raw* in-strip point
    /// as a universal witness; a missing selected ring remains fail-open.
    pub(crate) fn selection_ring_is_represented(&self, component: usize, ring: usize) -> bool {
        !self.unknown
            && self.selection_edges.iter().any(|piece| {
                piece.polygon && piece.edge.component == component && piece.edge.ring == ring
            })
    }

    /// One retained-authority witness per lineal component.  These are kept
    /// separate from polygon witnesses because lower-dimensional contact may
    /// retain a leaf but can never establish polygon containment.
    pub(crate) fn lower_authority_witnesses(
        &self,
    ) -> impl Iterator<Item = Option<GridDegreePoint>> + '_ {
        self.chains
            .iter()
            .filter(|chain| !chain.closed)
            .map(first_in_strip_chain_point)
    }

    pub(crate) fn authority_points(
        &self,
    ) -> impl ExactSizeIterator<Item = Option<GridDegreePoint>> + '_ {
        self.authority_points
            .iter()
            .map(|point| GridDegreePoint::from_expansions(&point.longitude, &point.latitude))
    }

    #[expect(
        dead_code,
        reason = "module unit tests inspect the selected point image"
    )]
    pub(crate) fn selection_points(
        &self,
    ) -> impl ExactSizeIterator<Item = Option<GridDegreePoint>> + '_ {
        self.selection_points
            .iter()
            .map(|point| GridDegreePoint::from_expansions(&point.longitude, &point.latitude))
    }

    pub(crate) fn positive_selection_points(
        &self,
    ) -> impl Iterator<Item = Option<GridDegreePoint>> + '_ {
        self.selection_points
            .iter()
            .zip(&self.authority_points)
            .filter(|(selection, authority)| {
                // A 360-degree longitude rewrite is the same unit-sphere
                // point and is already represented by the authority lift's
                // periodic PIP and the ordered arc seam shifts. Reflection
                // changes latitude and is the only point transform that adds
                // positive-only H3 authority.
                selection.latitude != authority.latitude
            })
            .map(|(point, _)| GridDegreePoint::from_expansions(&point.longitude, &point.latitude))
    }

    pub(crate) fn has_positive_selection_polygon(&self) -> bool {
        self.selection_edges
            .iter()
            .any(|piece| piece.is_polygon() && piece.is_positive_only())
    }

    pub(crate) fn has_positive_selection_lower(&self) -> bool {
        self.selection_edges
            .iter()
            .any(|piece| !piece.is_polygon() && piece.is_positive_only())
            || self.positive_selection_points().next().is_some()
    }

    /// Exact PIP in the portion of the spherical selection image that differs
    /// from retained authority.  This can only veto an authority result: an
    /// `Exterior` proves there is no reflected positive witness at `point`,
    /// while every other result retains the candidate.
    ///
    /// The image is evaluated from the exact split endpoints, not by
    /// transforming a rounded authority PIP query.  A component containing a
    /// reflected edge is tested as one complete selected ring so its unchanged
    /// edges still close the reflected shell/hole topology.
    pub(crate) fn positive_selection_point_class(
        &self,
        longitude: f64,
        latitude: f64,
    ) -> GridPointClass {
        if self.unknown
            || !longitude.is_finite()
            || !latitude.is_finite()
            || latitude.to_bits() == 90.0_f64.to_bits()
            || latitude.to_bits() == (-90.0_f64).to_bits()
        {
            return GridPointClass::Unknown;
        }
        let Some(query) = ExactPlanarPoint::from_stored(longitude, latitude) else {
            return GridPointClass::Unknown;
        };
        let mut aggregate = GridPointClass::Exterior;
        for shell in self.ring_fills.iter().filter(|fill| fill.ring == 0) {
            let component = shell.component;
            if !self.selection_edges.iter().any(|piece| {
                piece.polygon && piece.edge.component == component && piece.is_positive_only()
            }) {
                continue;
            }
            let shell_class = self.selection_ring_point_class(component, 0, &query);
            let component_class = match shell_class {
                GridPointClass::Exterior => GridPointClass::Exterior,
                GridPointClass::Boundary | GridPointClass::Unknown => shell_class,
                GridPointClass::Interior => self.selection_component_hole_class(component, &query),
            };
            aggregate = combine_polygon_point_classes(aggregate, component_class);
        }
        aggregate
    }

    /// Every exact selected pole endpoint, including pole crossings
    /// materialized by a rational split.  Longitude is structurally irrelevant
    /// at a pole, so the witness intentionally stores the one canonical zero
    /// longitude rather than choosing an antimeridian spelling.
    pub(crate) fn selection_pole_witnesses(&self) -> impl Iterator<Item = GridDegreePoint> + '_ {
        self.selection_points
            .iter()
            .filter_map(selection_pole_witness)
            .chain(
                self.selection_edges
                    .iter()
                    .flat_map(|piece| piece.selection.iter())
                    .filter_map(selection_planar_pole_witness),
            )
    }

    /// Exact retained-authority PIP for a non-pole stored degree point.  The
    /// positive selection image is deliberately not consulted here: it can
    /// only veto a later negative, never manufacture this authority result.
    pub(crate) fn authority_point_class(&self, longitude: f64, latitude: f64) -> GridPointClass {
        if self.unknown
            || !longitude.is_finite()
            || !latitude.is_finite()
            || latitude.to_bits() == 90.0_f64.to_bits()
            || latitude.to_bits() == (-90.0_f64).to_bits()
        {
            return GridPointClass::Unknown;
        }
        let Some(query) = ExactPlanarPoint::from_stored(longitude, latitude) else {
            return GridPointClass::Unknown;
        };
        let mut aggregate = GridPointClass::Exterior;
        for shell in self.ring_fills.iter().filter(|fill| fill.ring == 0) {
            let Some(chain) = self.chains.get(shell.chain_index) else {
                return GridPointClass::Unknown;
            };
            let shell_class = shell.point_class(chain, longitude, &query);
            let component = match shell_class {
                GridPointClass::Exterior => GridPointClass::Exterior,
                GridPointClass::Boundary | GridPointClass::Unknown => shell_class,
                GridPointClass::Interior => self.component_hole_class(shell, longitude, &query),
            };
            aggregate = combine_polygon_point_classes(aggregate, component);
        }
        aggregate
    }

    fn component_hole_class(
        &self,
        shell: &LiftedRingFill,
        longitude: f64,
        query: &ExactPlanarPoint,
    ) -> GridPointClass {
        let mut unknown = false;
        for hole in self
            .ring_fills
            .iter()
            .filter(|fill| fill.component == shell.component && fill.ring != 0)
        {
            let Some(chain) = self.chains.get(hole.chain_index) else {
                return GridPointClass::Unknown;
            };
            match hole.point_class(chain, longitude, query) {
                GridPointClass::Boundary => return GridPointClass::Boundary,
                GridPointClass::Interior => return GridPointClass::Exterior,
                GridPointClass::Unknown => unknown = true,
                GridPointClass::Exterior => {},
            }
        }
        if unknown {
            GridPointClass::Unknown
        } else {
            GridPointClass::Interior
        }
    }

    fn selection_component_hole_class(
        &self,
        component: usize,
        query: &ExactPlanarPoint,
    ) -> GridPointClass {
        let mut unknown = false;
        for hole in self
            .ring_fills
            .iter()
            .filter(|fill| fill.component == component && fill.ring != 0)
        {
            match self.selection_ring_point_class(component, hole.ring, query) {
                GridPointClass::Boundary => return GridPointClass::Boundary,
                GridPointClass::Interior => return GridPointClass::Exterior,
                GridPointClass::Unknown => unknown = true,
                GridPointClass::Exterior => {},
            }
        }
        if unknown {
            GridPointClass::Unknown
        } else {
            GridPointClass::Interior
        }
    }

    fn selection_ring_point_class(
        &self,
        component: usize,
        ring: usize,
        query: &ExactPlanarPoint,
    ) -> GridPointClass {
        let edge_count = self
            .selection_edges
            .iter()
            .filter(|piece| {
                piece.polygon && piece.edge.component == component && piece.edge.ring == ring
            })
            .count();
        if edge_count == 0 {
            return GridPointClass::Unknown;
        }
        // Selection pieces are split into the physical [-180, 180] sheet.
        // A physical query has only its immediate -360/0/+360 preimages in
        // that sheet; retaining all three is the exact circular PIP carrier,
        // not a heuristic longitude neighborhood.
        let copies = -1_i64..=1_i64;
        let Some(mut buckets) = PeriodicParityBuckets::new(copies, edge_count) else {
            return GridPointClass::Unknown;
        };
        for piece in self.selection_edges.iter().filter(|piece| {
            piece.polygon && piece.edge.component == component && piece.edge.ring == ring
        }) {
            match bucket_selection_edge(
                &piece.selection[0],
                &piece.selection[1],
                query,
                &mut buckets,
            ) {
                BucketEdge::Decisive(result) => return result,
                BucketEdge::Continue => {},
                BucketEdge::Uncertain => return GridPointClass::Unknown,
            }
        }
        if buckets.any_interior() {
            GridPointClass::Interior
        } else {
            GridPointClass::Exterior
        }
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "module unit tests inspect retained ring fills")
    )]
    fn ring_fills(&self) -> impl ExactSizeIterator<Item = &LiftedRingFill> {
        self.ring_fills.iter()
    }
}

fn selection_pole_witness(point: &SelectionPoint) -> Option<GridDegreePoint> {
    selection_planar_pole_witness(&ExactSelectionPoint {
        longitude: ExactRatio::from_expansion(&point.longitude)?,
        latitude: ExactRatio::from_expansion(&point.latitude)?,
    })
}

fn selection_planar_pole_witness(point: &ExactSelectionPoint) -> Option<GridDegreePoint> {
    let latitude = if point.latitude.ordering(&ExactRatio::from_i64(90, 1)?)? == Ordering::Equal {
        Bound::exact(90.0)?
    } else if point.latitude.ordering(&ExactRatio::from_i64(-90, 1)?)? == Ordering::Equal {
        Bound::exact(-90.0)?
    } else {
        return None;
    };
    Some(GridDegreePoint {
        longitude: Bound::exact(0.0)?,
        latitude,
    })
}

fn first_in_strip_chain_point(chain: &LiftedChain) -> Option<GridDegreePoint> {
    chain.vertices.iter().find_map(|vertex| {
        let latitude = ExactExpansion::from_f64(vertex.latitude)?;
        let south = ExactExpansion::from_i64(-90)?;
        let north = ExactExpansion::from_i64(90)?;
        (latitude.sub(south)?.ordering() != Ordering::Less
            && latitude.sub(north)?.ordering() != Ordering::Greater)
            .then(|| GridDegreePoint::from_stored(vertex.longitude.raw, vertex.latitude))?
    })
}

const fn combine_polygon_point_classes(
    aggregate: GridPointClass,
    candidate: GridPointClass,
) -> GridPointClass {
    use GridPointClass::{Boundary, Exterior, Interior, Unknown};
    match (aggregate, candidate) {
        (Interior, _) | (_, Interior) => Interior,
        (Boundary, _) | (_, Boundary) => Boundary,
        (Unknown, _) | (_, Unknown) => Unknown,
        (Exterior, Exterior) => Exterior,
    }
}

/// Materialize one source chain only after its exact lift is available, so
/// authority edges, selection pieces, and a periodic ring roof can never be
/// built from different topologies.
fn append_lifted_chain(
    input: &SourceChainInput,
    lifted: &mut Vec<LiftedChain>,
    ring_fills: &mut Vec<LiftedRingFill>,
    authority_edges: &mut Vec<SymbolicAffineEdge>,
    authority_pieces: &mut Vec<AuthorityAffinePiece>,
    selection_edges: &mut Vec<SelectionAffinePiece>,
    unknown: &mut bool,
) -> Result<(), TryReserveError> {
    let Some(chain) = LiftedChain::from_points(&input.points, input.closed) else {
        *unknown = true;
        return Ok(());
    };
    let chain_index = lifted.len();
    for (ordinal, edge_kind) in chain.edges.iter().copied().enumerate() {
        let Some(next) = ordinal.checked_add(1) else {
            *unknown = true;
            break;
        };
        let key = SourceEdgeKey {
            component: input.component,
            ring: input.ring,
            ordinal,
        };
        let edge = SymbolicAffineEdge::new(
            key,
            [
                SourceVertexKey {
                    component: input.component,
                    ring: input.ring,
                    ordinal,
                },
                SourceVertexKey {
                    component: input.component,
                    ring: input.ring,
                    ordinal: next,
                },
            ],
            input.closed,
            chain.vertices[ordinal],
            chain.vertices[next],
            edge_kind,
        );
        let Some(edge) = edge else {
            *unknown = true;
            continue;
        };
        if edge.selection_pieces(selection_edges).is_none() {
            *unknown = true;
        }
        if edge.authority_pieces(authority_pieces).is_none() {
            *unknown = true;
        }
        authority_edges.try_reserve(1)?;
        authority_edges.push(edge);
    }
    debug_assert_eq!(chain_index, lifted.len());
    if input.closed {
        if let Some(fill) =
            LiftedRingFill::from_chain(chain_index, input.component, input.ring, &chain)
        {
            ring_fills.push(fill);
        } else {
            *unknown = true;
        }
    }
    lifted.push(chain);
    Ok(())
}

/// Retain the raw authority point even when its selection transform cannot be
/// certified.  The latter is then a visible unknown; dropping the authority
/// point would turn a fail-open source into an empty source.
fn append_source_point(
    point: &SourcePoint,
    selection_points: &mut Vec<SelectionPoint>,
    unknown: &mut bool,
) {
    let Some(selection) = selection_image_point(point) else {
        *unknown = true;
        return;
    };
    selection_points.push(selection);
}

#[derive(Debug)]
struct SourceChainInput {
    component: usize,
    ring: usize,
    points: Vec<(f64, f64)>,
    closed: bool,
}

#[derive(Clone, Copy, Debug)]
struct SourcePointInput {
    component: usize,
    ordinal: usize,
    longitude: f64,
    latitude: f64,
}

fn collect_shape_chains(
    shape: &Shape,
    output: &mut Vec<SourceChainInput>,
    points: &mut Vec<SourcePointInput>,
    component: &mut usize,
) -> Result<(), TryReserveError> {
    match shape {
        Shape::Point(point) => {
            push_point(*point, *component, 0, points)?;
            *component = component.saturating_add(1);
        },
        Shape::MultiPoint(input) => {
            points.try_reserve(input.len())?;
            for (ordinal, point) in input.points().enumerate() {
                push_point(point, *component, ordinal, points)?;
            }
            *component = component.saturating_add(1);
        },
        Shape::Empty(..) => {},
        Shape::LineString(line) => {
            if let Some(pole) = physical_pole_chain(line) {
                push_point(pole, *component, 0, points)?;
            } else {
                push_chain(line, false, *component, 0, output)?;
            }
            *component = component.saturating_add(1);
        },
        Shape::MultiLineString(lines) => {
            for line in lines {
                if let Some(pole) = physical_pole_chain(line) {
                    push_point(pole, *component, 0, points)?;
                } else {
                    push_chain(line, false, *component, 0, output)?;
                }
                *component = component.saturating_add(1);
            }
        },
        Shape::Polygon(polygon) => {
            for (ring_index, ring) in polygon.rings().enumerate() {
                push_chain(ring, true, *component, ring_index, output)?;
            }
            *component = component.saturating_add(1);
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                for (ring_index, ring) in polygon.rings().enumerate() {
                    push_chain(ring, true, *component, ring_index, output)?;
                }
                *component = component.saturating_add(1);
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                collect_shape_chains(geometry, output, points, component)?;
            }
        },
    }
    Ok(())
}

/// A lon/lat line whose every vertex reaches the same physical pole after
/// geographic admission normalization has no spherical edge.  Feed it through
/// the point carrier instead of asking the affine-edge splitter to find a
/// strict latitude-strip interval at the pole (there is none).  Inward ULP
/// neighbours remain linear sources; only the accepted outward sliver is
/// canonicalized to the public pole.
fn physical_pole_chain(coords: &CoordSeq) -> Option<crate::geometry::Point> {
    let latitude = *coords.ys().first()?;
    let north = normalized_geographic_pole(latitude)?;
    coords
        .ys()
        .iter()
        .all(|&candidate| normalized_geographic_pole(candidate) == Some(north))
        .then(|| crate::geometry::Point::new_unchecked_xy(0.0, if north { 90.0 } else { -90.0 }))
}

fn push_point(
    point: crate::geometry::Point,
    component: usize,
    ordinal: usize,
    output: &mut Vec<SourcePointInput>,
) -> Result<(), TryReserveError> {
    output.try_reserve(1)?;
    output.push(SourcePointInput {
        component,
        ordinal,
        longitude: point.x,
        latitude: point.y,
    });
    Ok(())
}

fn push_chain(
    coords: &CoordSeq,
    closed: bool,
    component: usize,
    ring: usize,
    output: &mut Vec<SourceChainInput>,
) -> Result<(), TryReserveError> {
    let mut points = Vec::new();
    points.try_reserve(coords.len())?;
    points.extend(coords.points().map(|point| (point.x, point.y)));
    output.try_reserve(1)?;
    output.push(SourceChainInput {
        component,
        ring,
        points,
        closed,
    });
    Ok(())
}

fn product_expansion(left: f64, right: f64) -> Option<ExactExpansion> {
    let product = left * right;
    if !product.is_finite() {
        return None;
    }
    let error = left.mul_add(right, -product);
    if !error.is_finite() {
        return None;
    }
    let mut result = ExactExpansion::zero();
    if error != 0.0 {
        result.limbs[0] = error;
        result.len = 1;
    }
    if product != 0.0 {
        result.limbs[result.len as usize] = product;
        result.len += 1;
    }
    Some(result)
}

fn two_sum(left: f64, right: f64) -> Option<(f64, f64)> {
    let sum = left + right;
    if !sum.is_finite() {
        return None;
    }
    let right_virtual = sum - left;
    Some((
        sum,
        (left - (sum - right_virtual)) + (right - right_virtual),
    ))
}

fn fast_two_sum(left: f64, right: f64) -> Option<(f64, f64)> {
    let sum = left + right;
    sum.is_finite().then_some((sum, right - (sum - left)))
}

fn expansion_sum_zeroelim(left: &[f64], right: &[f64], output: &mut [f64]) -> Option<usize> {
    if left.len() + right.len() > output.len() {
        return None;
    }
    if left.is_empty() {
        output[..right.len()].copy_from_slice(right);
        return Some(right.len());
    }
    if right.is_empty() {
        output[..left.len()].copy_from_slice(left);
        return Some(left.len());
    }
    let mut left_index = 0;
    let mut right_index = 0;
    let mut left_now = left[left_index];
    let mut right_now = right[right_index];
    let mut accumulator;
    if (right_now > left_now) == (right_now > -left_now) {
        accumulator = left_now;
        left_index += 1;
    } else {
        accumulator = right_now;
        right_index += 1;
    }
    let mut output_len = 0;
    if left_index < left.len() && right_index < right.len() {
        left_now = left[left_index];
        right_now = right[right_index];
        let (sum, tail) = if (right_now > left_now) == (right_now > -left_now) {
            left_index += 1;
            fast_two_sum(left_now, accumulator)?
        } else {
            right_index += 1;
            fast_two_sum(right_now, accumulator)?
        };
        accumulator = sum;
        if tail != 0.0 {
            output[output_len] = tail;
            output_len += 1;
        }
        while left_index < left.len() && right_index < right.len() {
            left_now = left[left_index];
            right_now = right[right_index];
            let (sum, tail) = if (right_now > left_now) == (right_now > -left_now) {
                left_index += 1;
                two_sum(accumulator, left_now)?
            } else {
                right_index += 1;
                two_sum(accumulator, right_now)?
            };
            accumulator = sum;
            if tail != 0.0 {
                output[output_len] = tail;
                output_len += 1;
            }
        }
    }
    while left_index < left.len() {
        left_now = left[left_index];
        let (sum, tail) = two_sum(accumulator, left_now)?;
        accumulator = sum;
        left_index += 1;
        if tail != 0.0 {
            output[output_len] = tail;
            output_len += 1;
        }
    }
    while right_index < right.len() {
        right_now = right[right_index];
        let (sum, tail) = two_sum(accumulator, right_now)?;
        accumulator = sum;
        right_index += 1;
        if tail != 0.0 {
            output[output_len] = tail;
            output_len += 1;
        }
    }
    if accumulator != 0.0 || output_len == 0 {
        output[output_len] = accumulator;
        output_len += 1;
    }
    Some(output_len)
}

#[cfg(test)]
#[path = "affine_source_tests.rs"]
mod tests;
