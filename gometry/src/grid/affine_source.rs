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
//!   `self.start.longitude.raw` (edge reads lift), `piece.carrier.arc`/`.selection`
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

use h3o::Resolution;
use rstar::AABB;

use crate::boundary::geographic::{
    normalize_accepted_geographic_latitudes, normalized_geographic_pole,
};
use crate::geographic::{GeographicFrame, GeographicRing, RingDomain, RingSummary, RingTurn};
use crate::geometry::expansion::{expansion_sign, expansion_sum_zeroelim, two_product};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, EdgeId, EdgeYIndex, Shape, uses_linear_plan_for_len,
};
use crate::grid::s2::cell::Cell as S2Cell;
use crate::grid::s2::projection::lonlat_to_point as s2_lonlat_to_point;
use crate::grid::spherical_arc::{
    AffineEndpointIdentity, AffineParentMap, AffineSourceArc, AffineStructure, AxisLatitude,
    AxisMeridian, Bound, CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
    PhysicalEndpointKey, SourceEndpointRole, degree_bound_to_radians,
};

const EXPANSION_LIMBS: usize = 64;
const FULL_TURN_DEGREES: f64 = 360.0;
const HALF_TURN_DEGREES: f64 = 180.0;
const ORIENTATION_ERRBOUND: f64 = 128.0 * f64::EPSILON;

#[cfg(test)]
std::thread_local! {
    static AUTHORITY_EDGE_EXACT_REFINEMENTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static RECTANGULAR_CANDIDATE_ADMISSIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_authority_edge_exact_refinements() {
    AUTHORITY_EDGE_EXACT_REFINEMENTS.with(|count| count.set(0));
}

#[cfg(test)]
fn authority_edge_exact_refinements() -> usize {
    AUTHORITY_EDGE_EXACT_REFINEMENTS.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_rectangular_candidate_admissions() {
    RECTANGULAR_CANDIDATE_ADMISSIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn rectangular_candidate_admissions() -> usize {
    RECTANGULAR_CANDIDATE_ADMISSIONS.with(std::cell::Cell::get)
}

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
    Rectangular,
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

    fn try_from_expansions(
        longitude: &ExactExpansion,
        latitude: &ExactExpansion,
    ) -> ExactResult<Self> {
        Ok(Self {
            longitude: degree_expansion_bound(longitude)?,
            latitude: degree_expansion_bound(latitude)?,
        })
    }

    pub(crate) const fn is_proven_physical_latitude(self) -> bool {
        -90.0 <= self.latitude.lo && self.latitude.hi <= 90.0
    }
}

/// The one candidate representation admitted by the affine source owner.
///
/// `bounds` retains the caller's conservative raw rectangle for native
/// refinement. `windows` is the canonical physical longitude representation
/// used by exact affine classification; unlike `bounds`, it preserves a
/// seam split instead of enclosing the omitted middle sheet. The source axis
/// certificate is copied into the product at admission so a candidate carries
/// all facts needed by its later classification.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RectangularCandidate {
    bounds: Bounds,
    windows: DegreeWindowResult,
    exact_axis: Option<Bounds>,
}

impl RectangularCandidate {
    pub(crate) const fn bounds(&self) -> Bounds {
        self.bounds
    }

    pub(crate) const fn windows(&self) -> DegreeWindowResult {
        self.windows
    }
}

/// Native candidate forms produced by the four grid lanes. The conversion into
/// [`RectangularCandidate`] is deliberately owned by [`GeographicCoverageSource`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum RectangularCandidateInput {
    Bounds(Bounds),
    Windows(DegreeWindowResult),
}

impl From<Bounds> for RectangularCandidateInput {
    fn from(bounds: Bounds) -> Self {
        Self::Bounds(bounds)
    }
}

impl From<DegreeWindowResult> for RectangularCandidateInput {
    fn from(windows: DegreeWindowResult) -> Self {
        Self::Windows(windows)
    }
}

impl From<CertifiedDegreeWindows> for RectangularCandidateInput {
    fn from(windows: CertifiedDegreeWindows) -> Self {
        Self::Windows(DegreeWindowResult::Windows(windows))
    }
}

fn degree_windows_from_bounds(bounds: Bounds) -> DegreeWindowResult {
    let (Some(latitude), Some(longitude)) = (
        Bound::new(bounds.miny(), bounds.maxy()),
        if bounds.minx() <= -HALF_TURN_DEGREES && bounds.maxx() >= HALF_TURN_DEGREES {
            Some(CertifiedLongitudeDegrees::Full)
        } else if bounds.minx() > bounds.maxx() {
            Bound::new(bounds.minx(), HALF_TURN_DEGREES)
                .zip(Bound::new(-HALF_TURN_DEGREES, bounds.maxx()))
                .map(|(west, east)| CertifiedLongitudeDegrees::Two((west, east).into()))
        } else {
            Bound::new(bounds.minx(), bounds.maxx()).map(CertifiedLongitudeDegrees::One)
        },
    ) else {
        return DegreeWindowResult::Boundary;
    };
    DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude,
    })
}

fn conservative_bounds_from_windows(windows: DegreeWindowResult) -> Bounds {
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude,
    }) = windows
    else {
        return Bounds::new_raw(-HALF_TURN_DEGREES, -90.0, HALF_TURN_DEGREES, 90.0);
    };
    let (minx, maxx) = match longitude {
        CertifiedLongitudeDegrees::Full | CertifiedLongitudeDegrees::Two(_) => {
            (-HALF_TURN_DEGREES, HALF_TURN_DEGREES)
        },
        CertifiedLongitudeDegrees::One(longitude) => (longitude.lo, longitude.hi),
    };
    Bounds::new_raw(minx, latitude.lo, maxx, latitude.hi)
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
fn subdivided_axis_aligned_rectangle(source: &Shape, bounds: Bounds) -> ExactResult<bool> {
    let Shape::Polygon(polygon) = source else {
        return Ok(false);
    };
    if !polygon.holes.is_empty() {
        return Ok(false);
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
            return Ok(false);
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
                None => return Ok(false),
            };
            if current != next {
                return Ok(false);
            }
            side_changes = match side_changes.checked_add(1) {
                Some(changes) => changes,
                None => return Ok(false),
            };
        }
        first.get_or_insert(current);
        previous = Some(current);
    }
    let (Some(first), Some(last), Some(counterclockwise)) = (first, previous, direction) else {
        return Ok(false);
    };
    let closing_next = if counterclockwise {
        last.next_counterclockwise()
    } else {
        last.next_clockwise()
    };
    if closing_next != first || side_changes != 3 {
        return Ok(false);
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
            return Ok(false);
        };
        let delta = match side {
            RectangleSide::South | RectangleSide::North => {
                let step = match lifted_step(start.x, end.x) {
                    Ok(step) => step,
                    Err(ExactFailure::Arithmetic) => return Ok(false),
                    Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
                };
                let end = ExactExpansion::from_f64(end.x)?;
                let start = ExactExpansion::from_f64(start.x)?;
                let raw = end.sub(&start)?;
                let turns = ExactExpansion::exact_degree_turns(step.turn_delta)?;
                let delta = raw.add(&turns)?;
                delta.ordering()
            },
            RectangleSide::East | RectangleSide::West => {
                let end = ExactExpansion::from_f64(end.y)?;
                let start = ExactExpansion::from_f64(start.y)?;
                let delta = end.sub(&start)?;
                delta.ordering()
            },
        };
        if !advances(side, delta) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn exact_axis_rectangle(source: &Shape, bounds: Bounds) -> ExactResult<bool> {
    // The generic rectangle certificate is sufficient for topology shortcuts,
    // but does not verify an ordered, directed source traversal or geographic
    // shortest lifts. The side-walk above owns the stronger cap certificate
    // for both four-vertex and geographic-densified rings.
    subdivided_axis_aligned_rectangle(source, bounds)
}

fn exact_world_band(source: &Shape) -> crate::error::Result<Option<Bounds>> {
    let Shape::Polygon(polygon) = source else {
        return Ok(None);
    };
    if !polygon.holes.is_empty() {
        return Ok(None);
    }
    let summary = match RingSummary::try_new(
        GeographicFrame::crs84(),
        &GeographicRing::from_ring(&polygon.shell),
    ) {
        Ok(summary) => summary,
        Err(crate::geographic::GeographicViewError::Resource) => {
            return Err(crate::error::ResourceError::Allocation.into());
        },
        Err(crate::geographic::GeographicViewError::Diagnostic(_)) => return Ok(None),
    };
    Ok(
        (summary.turn == RingTurn::None && matches!(summary.domain, RingDomain::FullLongitude(_)))
            .then(|| Bounds::new_raw(-180.0, summary.south.get(), 180.0, summary.north.get())),
    )
}

/// A bounds rectangle is a complete source model only on the canonical
/// longitude sheet.  A stored ring from 179 to -179 is geometrically a narrow
/// seam strip in the continuous lift, not the 358-degree rectangle named by
/// its planar extrema. The one wider admissible form is the exact physical
/// full-longitude domain `[-180, 180]`.
fn canonical_axis_rectangle_longitude(bounds: Bounds) -> ExactResult<bool> {
    let minimum = ExactExpansion::from_f64(bounds.minx())?;
    let maximum = ExactExpansion::from_f64(bounds.maxx())?;
    let width = maximum.sub(&minimum)?;
    let half_turn = ExactExpansion::from_i64(180)?;
    let over_half_turn = width.sub(&half_turn)?.ordering();
    if over_half_turn != Ordering::Greater {
        return Ok(true);
    }
    Ok(minimum.is_exact(-180.0)? && maximum.is_exact(180.0)?)
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

fn with_exact_rectangle<T>(
    longitude: Bound,
    latitude: Bound,
    turn: i64,
    visit: impl FnOnce([ExactPlanarPointRef<'_>; 4]) -> ExactResult<T>,
) -> ExactResult<T> {
    let offset = ExactExpansion::exact_degree_turns(turn)?;
    let west = ExactExpansion::from_f64(longitude.lo)?.add(&offset)?;
    let east = ExactExpansion::from_f64(longitude.hi)?.add(&offset)?;
    let south = ExactExpansion::from_f64(latitude.lo)?;
    let north = ExactExpansion::from_f64(latitude.hi)?;
    visit([
        ExactPlanarPointRef {
            longitude: &west,
            latitude: &south,
        },
        ExactPlanarPointRef {
            longitude: &east,
            latitude: &south,
        },
        ExactPlanarPointRef {
            longitude: &east,
            latitude: &north,
        },
        ExactPlanarPointRef {
            longitude: &west,
            latitude: &north,
        },
    ])
}

fn closed_segment_intersects<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    OS: ExactPlanarCoordinates + ?Sized,
    OE: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    other_start: &OS,
    other_end: &OE,
) -> ExactResult<bool> {
    let first = planar_orientation_ordering(start, end, other_start)?;
    let second = planar_orientation_ordering(start, end, other_end)?;
    let third = planar_orientation_ordering(other_start, other_end, start)?;
    let fourth = planar_orientation_ordering(other_start, other_end, end)?;
    let crosses = |left: Ordering, right: Ordering| {
        left == Ordering::Equal || right == Ordering::Equal || left != right
    };
    if crosses(first, second) && crosses(third, fourth) {
        if first != Ordering::Equal
            && second != Ordering::Equal
            && third != Ordering::Equal
            && fourth != Ordering::Equal
        {
            return Ok(true);
        }
        return Ok((first == Ordering::Equal
            && closed_between_planar_points(start, other_start, end)?)
            || (second == Ordering::Equal
                && closed_between_planar_points(start, other_end, end)?)
            || (third == Ordering::Equal
                && closed_between_planar_points(other_start, start, other_end)?)
            || (fourth == Ordering::Equal
                && closed_between_planar_points(other_start, end, other_end)?));
    }
    Ok(false)
}

fn exact_segment_touches_rectangle(
    start: &ExactPlanarPoint,
    end: &ExactPlanarPoint,
    longitude: Bound,
    latitude: Bound,
) -> ExactResult<bool> {
    let (first_turn, last_turn) =
        exact_longitude_copy_range(&start.longitude, &end.longitude, longitude)?;
    for turn in first_turn..=last_turn {
        if with_exact_rectangle(longitude, latitude, turn, |rectangle| {
            for (first, second) in rectangle
                .iter()
                .zip(rectangle.iter().cycle().skip(1))
                .take(4)
            {
                if closed_segment_intersects(start, end, first, second)? {
                    return Ok(true);
                }
            }
            Ok(false)
        })? {
            return Ok(true);
        }
    }
    Ok(false)
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
) -> ExactResult<(i64, i64)> {
    let (minimum, maximum) = match first.sub(second)?.ordering() {
        Ordering::Greater => (second, first),
        Ordering::Less | Ordering::Equal => (first, second),
    };
    let west = ExactExpansion::from_f64(window.lo)?;
    let east = ExactExpansion::from_f64(window.hi)?;
    let lower = ExactRatio::from_expansion(&minimum.sub(&east)?)?
        .floor_turn()?
        .checked_sub(1)
        .ok_or(ExactFailure::Arithmetic)?;
    let upper = ExactRatio::from_expansion(&maximum.sub(&west)?)?
        .floor_turn()?
        .checked_add(1)
        .ok_or(ExactFailure::Arithmetic)?;
    if upper < lower || upper.checked_sub(lower).ok_or(ExactFailure::Arithmetic)? > 4 {
        return Err(ExactFailure::Arithmetic);
    }
    Ok((lower, upper))
}

fn exact_point_touches_rectangle(
    point: &ExactPlanarPoint,
    longitude: Bound,
    latitude: Bound,
) -> ExactResult<bool> {
    let (first_turn, last_turn) =
        exact_longitude_copy_range(&point.longitude, &point.longitude, longitude)?;
    for turn in first_turn..=last_turn {
        if with_exact_rectangle(longitude, latitude, turn, |rectangle| {
            Ok(closed_between_expansion(
                rectangle[0].longitude,
                &point.longitude,
                rectangle[1].longitude,
            )? && closed_between_expansion(
                rectangle[0].latitude,
                &point.latitude,
                rectangle[2].latitude,
            )?)
        })? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn exact_authority_edge_touches_rectangle(
    edge: &SymbolicAffineEdge,
    longitude: Bound,
    latitude: Bound,
) -> ExactResult<bool> {
    #[cfg(test)]
    AUTHORITY_EDGE_EXACT_REFINEMENTS.with(|count| count.set(count.get() + 1));
    let start = ExactPlanarPoint::from_lifted(edge.start)?;
    let end = ExactPlanarPoint::from_lifted(edge.end)?;
    let start_latitude = degree_expansion_bound(&start.latitude)?;
    let end_latitude = degree_expansion_bound(&end.latitude)?;
    if !(-90.0..=90.0).contains(&start_latitude.lo)
        || !(-90.0..=90.0).contains(&start_latitude.hi)
        || !(-90.0..=90.0).contains(&end_latitude.lo)
        || !(-90.0..=90.0).contains(&end_latitude.hi)
    {
        return Ok(true);
    }
    if exact_point_touches_rectangle(&start, longitude, latitude)?
        || exact_point_touches_rectangle(&end, longitude, latitude)?
    {
        return Ok(true);
    }
    exact_segment_touches_rectangle(&start, &end, longitude, latitude)
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

/// Failure while materializing an exact affine certificate.
///
/// Arithmetic failure remains a fail-open `Unknown` at the source boundary;
/// allocation failure must instead escape as the crate's native resource
/// error. Keeping the two cases distinct is what prevents a refused expansion
/// buffer from silently publishing a partial cover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactFailure {
    Arithmetic,
    Allocation,
}

type ExactResult<T> = std::result::Result<T, ExactFailure>;

fn exact<T>(value: Option<T>) -> ExactResult<T> {
    value.ok_or(ExactFailure::Arithmetic)
}

fn exact_allocation<T>(result: crate::error::Result<T>) -> ExactResult<T> {
    result.map_err(|_| ExactFailure::Allocation)
}

fn exact_optional<T>(result: ExactResult<T>) -> crate::error::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(ExactFailure::Arithmetic) => Ok(None),
        Err(ExactFailure::Allocation) => Err(crate::error::ResourceError::Allocation.into()),
    }
}

/// Exact dyadic expansion with a bounded, initialized limb carrier. Capacity
/// failure is distinct from arithmetic uncertainty until the owning source
/// boundary maps it to a native resource error; no epsilon may ever choose a
/// parent parameter.
const INLINE_EXPANSION_LIMBS: usize = 4;

#[derive(Debug)]
enum CompactLimbs {
    Inline {
        limbs: [f64; INLINE_EXPANSION_LIMBS],
        len: u8,
    },
    Heap(Vec<f64>),
}

impl CompactLimbs {
    const fn zero() -> Self {
        Self::Inline {
            limbs: [0.0; INLINE_EXPANSION_LIMBS],
            len: 0,
        }
    }

    const fn single(value: f64) -> Self {
        let mut limbs = [0.0; INLINE_EXPANSION_LIMBS];
        limbs[0] = value;
        Self::Inline { limbs, len: 1 }
    }

    fn as_slice(&self) -> &[f64] {
        match self {
            Self::Inline { limbs, len } => &limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [f64] {
        match self {
            Self::Inline { limbs, len } => &mut limbs[..usize::from(*len)],
            Self::Heap(limbs) => limbs,
        }
    }

    fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    fn is_zero(&self) -> bool {
        self.as_slice().iter().all(|&limb| limb == 0.0)
    }

    fn clear(&mut self) {
        match self {
            Self::Inline { len, .. } => *len = 0,
            Self::Heap(limbs) => limbs.clear(),
        }
    }

    fn prepare(&mut self, required: usize) -> ExactResult<()> {
        if required > EXPANSION_LIMBS {
            return Err(ExactFailure::Arithmetic);
        }
        match self {
            Self::Inline { len, .. } if required <= INLINE_EXPANSION_LIMBS => {
                *len = u8::try_from(required).map_err(|_| ExactFailure::Arithmetic)?;
            },
            Self::Inline { limbs, len } => {
                let mut expanded = Vec::new();
                exact_allocation(crate::try_reserve_vec(&mut expanded, required))?;
                expanded.resize(required, 0.0);
                expanded[..usize::from(*len)].copy_from_slice(&limbs[..usize::from(*len)]);
                *self = Self::Heap(expanded);
            },
            Self::Heap(limbs) => {
                let additional = required.saturating_sub(limbs.len());
                exact_allocation(crate::try_reserve_vec(limbs, additional))?;
                if limbs.len() < required {
                    limbs.resize(required, 0.0);
                } else {
                    limbs.truncate(required);
                }
            },
        }
        Ok(())
    }

    fn truncate(&mut self, len: usize) -> ExactResult<()> {
        match self {
            Self::Inline { len: current, .. } => {
                *current = u8::try_from(len).map_err(|_| ExactFailure::Arithmetic)?;
            },
            Self::Heap(limbs) => limbs.truncate(len),
        }
        Ok(())
    }

    fn canonicalize(&mut self) {
        if self.is_zero() {
            self.clear();
        }
    }

    fn try_clone(&self) -> ExactResult<Self> {
        let mut result = match self {
            Self::Inline { limbs, len } => Some(Self::Inline {
                limbs: *limbs,
                len: *len,
            }),
            Self::Heap(limbs) => {
                let mut clone = Vec::new();
                exact_allocation(crate::try_reserve_vec(&mut clone, limbs.len()))?;
                clone.extend_from_slice(limbs);
                Some(Self::Heap(clone))
            },
        }
        .ok_or(ExactFailure::Arithmetic)?;
        result.canonicalize();
        Ok(result)
    }

    fn try_negated(&self) -> ExactResult<Self> {
        let mut result = self.try_clone()?;
        for limb in result.as_mut_slice() {
            *limb = -*limb;
        }
        result.canonicalize();
        Ok(result)
    }

    fn into_negated(mut self) -> Self {
        for limb in self.as_mut_slice() {
            *limb = -*limb;
        }
        self.canonicalize();
        self
    }
}

impl PartialEq for CompactLimbs {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice() || (self.is_zero() && other.is_zero())
    }
}

#[derive(Debug)]
struct ExactExpansion {
    limbs: CompactLimbs,
}

/// Sum two increasing-magnitude expansions into initialized `CompactLimbs`.
/// Capacity, prepare, truncate, and canonicalize stay here; the Shewchuk
/// merge is `expansion_sum_zeroelim`.
fn sum_expansions<const NEGATE_RIGHT: bool>(
    left: &[f64],
    right: &[f64],
    output: &mut CompactLimbs,
) -> ExactResult<usize> {
    let required = left
        .len()
        .checked_add(right.len())
        .ok_or(ExactFailure::Arithmetic)?;
    if required > EXPANSION_LIMBS {
        return Err(ExactFailure::Arithmetic);
    }
    output.clear();
    output.prepare(required)?;
    let output_len = exact(expansion_sum_zeroelim::<NEGATE_RIGHT>(
        left,
        right,
        output.as_mut_slice(),
        true,
    ))?;
    output.truncate(output_len)?;
    output.canonicalize();
    Ok(output.as_slice().len())
}

impl ExactExpansion {
    const fn zero() -> Self {
        Self {
            limbs: CompactLimbs::zero(),
        }
    }

    fn from_f64(value: f64) -> ExactResult<Self> {
        if !value.is_finite() {
            return Err(ExactFailure::Arithmetic);
        }
        if value == 0.0 {
            return Ok(Self::zero());
        }
        Ok(Self {
            limbs: CompactLimbs::single(value),
        })
    }

    fn from_i64(value: i64) -> ExactResult<Self> {
        const LOW_BITS: i64 = 1_i64 << 32;
        let high = value / LOW_BITS;
        let low = value % LOW_BITS;
        let low = Self::from_f64(low as f64)?;
        let high = Self::from_f64((high as f64) * (LOW_BITS as f64))?;
        low.add(&high)
    }

    fn exact_degree_turns(turns: i64) -> ExactResult<Self> {
        let scale = Self::from_f64(FULL_TURN_DEGREES)?;
        Self::from_i64(turns)?.product(&scale)
    }

    fn as_slice(&self) -> &[f64] {
        self.limbs.as_slice()
    }

    fn as_f64(&self) -> Option<f64> {
        match self.as_slice() {
            [] => Some(0.0),
            [value] => Some(*value),
            _ => None,
        }
    }

    fn try_clone(&self) -> ExactResult<Self> {
        let mut limbs = self.limbs.try_clone()?;
        limbs.canonicalize();
        Ok(Self { limbs })
    }

    fn neg(&self) -> ExactResult<Self> {
        Ok(Self {
            limbs: self.limbs.try_negated()?,
        })
    }

    fn into_negated(mut self) -> Self {
        self.limbs = self.limbs.into_negated();
        self
    }

    fn add(&self, rhs: &Self) -> ExactResult<Self> {
        let mut limbs = CompactLimbs::zero();
        sum_expansions::<false>(self.as_slice(), rhs.as_slice(), &mut limbs)?;
        Ok(Self { limbs })
    }

    fn sub(&self, rhs: &Self) -> ExactResult<Self> {
        let mut limbs = CompactLimbs::zero();
        sum_expansions::<true>(self.as_slice(), rhs.as_slice(), &mut limbs)?;
        Ok(Self { limbs })
    }

    fn product(&self, rhs: &Self) -> ExactResult<Self> {
        let mut result = CompactLimbs::zero();
        let mut scratch = CompactLimbs::zero();
        for &left in self.as_slice() {
            for &right in rhs.as_slice() {
                let (head, tail) = exact(two_product(left, right))?;
                if !head.is_finite() || !tail.is_finite() {
                    return Err(ExactFailure::Arithmetic);
                }
                let term = if tail == 0.0 {
                    [if head == 0.0 { 0.0 } else { head }, 0.0]
                } else if head != 0.0 {
                    [tail, head]
                } else {
                    [tail, 0.0]
                };
                let term_len = if tail == 0.0 {
                    usize::from(head != 0.0)
                } else {
                    1 + usize::from(head != 0.0)
                };
                if term_len == 0 {
                    continue;
                }
                if result.is_empty() {
                    result.prepare(term_len)?;
                    result.as_mut_slice()[..term_len].copy_from_slice(&term[..term_len]);
                } else {
                    sum_expansions::<false>(result.as_slice(), &term[..term_len], &mut scratch)?;
                    std::mem::swap(&mut result, &mut scratch);
                }
            }
        }
        Ok(Self { limbs: result })
    }

    fn ordering(&self) -> Ordering {
        expansion_sign(self.as_slice())
    }

    fn is_exact(&self, value: f64) -> ExactResult<bool> {
        let value = Self::from_f64(value)?;
        Ok(self.sub(&value)?.ordering() == Ordering::Equal)
    }

    fn checked_estimate(&self) -> Option<f64> {
        self.as_slice().iter().try_fold(0.0, |sum, &limb| {
            let next = sum + limb;
            next.is_finite().then_some(next)
        })
    }
}

impl PartialEq for ExactExpansion {
    fn eq(&self, other: &Self) -> bool {
        self.limbs == other.limbs
    }
}

/// Exact ratio retained as source-parent identity.  `Bound` is intentionally
/// absent here: an outward enclosure may evaluate an arc, but cannot identify
/// which parent split produced that arc.
#[derive(Debug, PartialEq)]
struct ExactRatio {
    numerator: ExactExpansion,
    denominator: ExactExpansion,
}

impl ExactRatio {
    fn try_clone(&self) -> ExactResult<Self> {
        Ok(Self {
            numerator: self.numerator.try_clone()?,
            denominator: self.denominator.try_clone()?,
        })
    }

    fn new(numerator: &ExactExpansion, denominator: &ExactExpansion) -> ExactResult<Self> {
        match denominator.ordering() {
            Ordering::Equal => Err(ExactFailure::Arithmetic),
            Ordering::Greater => Ok(Self {
                numerator: numerator.try_clone()?,
                denominator: denominator.try_clone()?,
            }),
            Ordering::Less => Ok(Self {
                numerator: numerator.neg()?,
                denominator: denominator.neg()?,
            }),
        }
    }

    fn from_parts(numerator: ExactExpansion, denominator: ExactExpansion) -> ExactResult<Self> {
        match denominator.ordering() {
            Ordering::Equal => Err(ExactFailure::Arithmetic),
            Ordering::Greater => Ok(Self {
                numerator,
                denominator,
            }),
            Ordering::Less => Ok(Self {
                numerator: numerator.into_negated(),
                denominator: denominator.into_negated(),
            }),
        }
    }

    fn from_numerator(
        numerator: ExactExpansion,
        denominator: &ExactExpansion,
    ) -> ExactResult<Self> {
        match denominator.ordering() {
            Ordering::Equal => Err(ExactFailure::Arithmetic),
            Ordering::Greater => Ok(Self {
                numerator,
                denominator: denominator.try_clone()?,
            }),
            Ordering::Less => Ok(Self {
                numerator: numerator.into_negated(),
                denominator: denominator.neg()?,
            }),
        }
    }

    fn from_i64(numerator: i64, denominator: i64) -> ExactResult<Self> {
        let numerator = ExactExpansion::from_i64(numerator)?;
        let denominator = ExactExpansion::from_i64(denominator)?;
        Self::from_parts(numerator, denominator)
    }

    fn from_f64(value: f64) -> ExactResult<Self> {
        let numerator = ExactExpansion::from_f64(value)?;
        let denominator = ExactExpansion::from_i64(1)?;
        Self::from_parts(numerator, denominator)
    }

    fn from_expansion(numerator: &ExactExpansion) -> ExactResult<Self> {
        let denominator = ExactExpansion::from_i64(1)?;
        Self::new(numerator, &denominator)
    }

    fn add(&self, rhs: &Self) -> ExactResult<Self> {
        let left = self.numerator.product(&rhs.denominator)?;
        let right = rhs.numerator.product(&self.denominator)?;
        let numerator = left.add(&right)?;
        let denominator = self.denominator.product(&rhs.denominator)?;
        Self::from_parts(numerator, denominator)
    }

    fn sub(&self, rhs: &Self) -> ExactResult<Self> {
        let left = self.numerator.product(&rhs.denominator)?;
        let right = rhs.numerator.product(&self.denominator)?;
        let numerator = left.sub(&right)?;
        let denominator = self.denominator.product(&rhs.denominator)?;
        Self::from_parts(numerator, denominator)
    }

    fn mul(&self, rhs: &Self) -> ExactResult<Self> {
        let numerator = self.numerator.product(&rhs.numerator)?;
        let denominator = self.denominator.product(&rhs.denominator)?;
        Self::from_parts(numerator, denominator)
    }

    fn div(&self, rhs: &Self) -> ExactResult<Self> {
        if rhs.numerator.ordering() == Ordering::Equal {
            return Err(ExactFailure::Arithmetic);
        }
        let numerator = self.numerator.product(&rhs.denominator)?;
        let denominator = self.denominator.product(&rhs.numerator)?;
        Self::from_parts(numerator, denominator)
    }

    fn ordering(&self, rhs: &Self) -> ExactResult<Ordering> {
        let left = self.numerator.product(&rhs.denominator)?;
        let right = rhs.numerator.product(&self.denominator)?;
        Ok(left.sub(&right)?.ordering())
    }

    fn is_closed_unit(&self) -> ExactResult<bool> {
        let zero = Self::from_i64(0, 1)?;
        let one = Self::from_i64(1, 1)?;
        Ok(self.ordering(&zero)? != Ordering::Less && self.ordering(&one)? != Ordering::Greater)
    }

    /// Returns an outward binary64 interval only after exact cross-products
    /// prove that the two stored endpoints enclose this parent identity.
    /// The enclosure never participates in ordering or endpoint identity.
    fn certified_bound(&self) -> ExactResult<Bound> {
        if self.numerator.ordering() == Ordering::Equal {
            return exact(Bound::exact(0.0));
        }
        let numerator = exact(self.numerator.checked_estimate())?;
        let denominator = exact(self.denominator.checked_estimate())?;
        let proposal = numerator / denominator;
        if !proposal.is_finite() {
            return Err(ExactFailure::Arithmetic);
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
            return exact(Bound::new(lower, upper));
        }
        Err(ExactFailure::Arithmetic)
    }

    /// Exact floor of this ratio divided by one full longitude turn.  The
    /// binary64 quotient merely proposes a nearby bucket; exact
    /// cross-products decide each correction.
    fn floor_turn(&self) -> ExactResult<i64> {
        let proposal =
            exact(self.numerator.checked_estimate())? / exact(self.denominator.checked_estimate())?;
        let proposal = (proposal / FULL_TURN_DEGREES).floor();
        if !proposal.is_finite() || !(i64::MIN as f64..=i64::MAX as f64).contains(&proposal) {
            return Err(ExactFailure::Arithmetic);
        }
        let mut level = proposal as i64;
        for _ in 0..4 {
            let next = exact(level.checked_add(1))?;
            let next_ratio = Self::from_i64(exact(next.checked_mul(360))?, 1)?;
            if self.ordering(&next_ratio)? != Ordering::Less {
                level = next;
                continue;
            }
            let current_ratio =
                Self::from_i64(level.checked_mul(360).ok_or(ExactFailure::Arithmetic)?, 1)?;
            if self.ordering(&current_ratio)? == Ordering::Less {
                level = exact(level.checked_sub(1))?;
                continue;
            }
            return Ok(level);
        }
        Err(ExactFailure::Arithmetic)
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

#[derive(Debug)]
struct ExactParentParameter {
    key: ParentParameterKey,
    ratio: ExactRatio,
    enclosure: Bound,
}

impl ExactParentParameter {
    fn try_clone(&self) -> ExactResult<Self> {
        Ok(Self {
            key: self.key,
            ratio: self.ratio.try_clone()?,
            enclosure: self.enclosure,
        })
    }

    fn new(key: ParentParameterKey, ratio: ExactRatio) -> ExactResult<Self> {
        let enclosure = ratio.certified_bound()?;
        ratio
            .is_closed_unit()?
            .then_some(Self {
                key,
                ratio,
                enclosure,
            })
            .ok_or(ExactFailure::Arithmetic)
    }

    fn start() -> ExactResult<Self> {
        Self::new(ParentParameterKey::Start, ExactRatio::from_i64(0, 1)?)
    }

    fn end() -> ExactResult<Self> {
        Self::new(ParentParameterKey::End, ExactRatio::from_i64(1, 1)?)
    }
}

#[derive(Debug)]
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
    fn difference(self, rhs: Self) -> ExactResult<ExactExpansion> {
        let left = ExactExpansion::from_f64(self.raw)?;
        let right = ExactExpansion::from_f64(rhs.raw)?;
        let delta = left.sub(&right)?;
        let turns = ExactExpansion::exact_degree_turns(
            self.turns
                .checked_sub(rhs.turns)
                .ok_or(ExactFailure::Arithmetic)?,
        )?;
        delta.add(&turns)
    }

    fn ordering(self, rhs: Self) -> ExactResult<Ordering> {
        Ok(self.difference(rhs)?.ordering())
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
fn lifted_step(start: f64, end: f64) -> ExactResult<LiftStep> {
    let end_expansion = ExactExpansion::from_f64(end)?;
    let start_expansion = ExactExpansion::from_f64(start)?;
    let raw_delta = end_expansion.sub(&start_expansion)?;
    if raw_delta.is_exact(FULL_TURN_DEGREES)? {
        return Ok(LiftStep {
            turn_delta: 0,
            kind: LiftKind::FullPositive,
        });
    }
    if raw_delta.is_exact(-FULL_TURN_DEGREES)? {
        return Ok(LiftStep {
            turn_delta: 0,
            kind: LiftKind::FullNegative,
        });
    }
    let proposal = end - start;
    if !proposal.is_finite() {
        return Err(ExactFailure::Arithmetic);
    }
    let candidate = (-proposal / FULL_TURN_DEGREES).round();
    if !candidate.is_finite() || candidate < i64::MIN as f64 || candidate > i64::MAX as f64 {
        return Err(ExactFailure::Arithmetic);
    }
    let mut turn_delta = candidate as i64;
    for _ in 0..4 {
        let turns = ExactExpansion::exact_degree_turns(turn_delta)?;
        let lifted = raw_delta.add(&turns)?;
        let half_turn = ExactExpansion::from_f64(HALF_TURN_DEGREES)?;
        if lifted.sub(&half_turn)?.ordering() == Ordering::Greater {
            turn_delta = turn_delta.checked_sub(1).ok_or(ExactFailure::Arithmetic)?;
            continue;
        }
        if lifted.add(&half_turn)?.ordering() == Ordering::Less {
            turn_delta = turn_delta.checked_add(1).ok_or(ExactFailure::Arithmetic)?;
            continue;
        }
        // At exactly half a world, the written direction—not a normalization
        // convention—chooses the branch.
        if lifted.is_exact(-HALF_TURN_DEGREES)? && raw_delta.ordering() == Ordering::Greater {
            turn_delta = turn_delta.checked_add(1).ok_or(ExactFailure::Arithmetic)?;
            continue;
        }
        if lifted.is_exact(HALF_TURN_DEGREES)? && raw_delta.ordering() == Ordering::Less {
            turn_delta = turn_delta.checked_sub(1).ok_or(ExactFailure::Arithmetic)?;
            continue;
        }
        return Ok(LiftStep {
            turn_delta,
            kind: LiftKind::Shortest,
        });
    }
    Err(ExactFailure::Arithmetic)
}

#[derive(Clone, Copy, Debug)]
struct LiftedVertex {
    longitude: LiftedLongitude,
    latitude: f64,
}

#[derive(Clone, Debug)]
struct LiftedChain {
    vertices: Vec<LiftedVertex>,
    edges: Vec<LiftKind>,
    closed: bool,
    degree: Option<i64>,
}

/// The finite lifted authority for one polygon ring.  A nonzero longitude
/// degree cannot be treated as an unknown or a world-sized source: its two
/// physical end vertices are joined through the pole selected by the ring's
/// geographic latitude extrema, with the signed number of turns retained
/// symbolically.  Later PIP consumes this plan over every direct periodic
/// preimage; it must never reconstruct a shortest-period closure from the raw
/// endpoint doubles.
#[derive(Debug)]
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

/// Select the physical pole enclosed by a full-longitude ring.
///
/// The direction of the longitude walk is not a geographic pole signal: it
/// changes when a valid ring is reversed. As in `ring_encloses_pole`, a ring
/// strictly in the northern or southern latitude hemisphere has one
/// unambiguous pole; a mixed or equatorial ring has no certified pole roof.
fn physical_pole_for_chain(chain: &LiftedChain) -> Option<PhysicalEndpointKey> {
    let (minimum, maximum) = chain.vertices.iter().map(|vertex| vertex.latitude).fold(
        (f64::INFINITY, f64::NEG_INFINITY),
        |(minimum, maximum), latitude| (minimum.min(latitude), maximum.max(latitude)),
    );
    if minimum > 0.0 {
        Some(PhysicalEndpointKey::NorthPole)
    } else if maximum < 0.0 {
        Some(PhysicalEndpointKey::SouthPole)
    } else {
        None
    }
}

#[derive(Debug)]
struct LiftedRingFill {
    chain_index: usize,
    component: usize,
    ring: usize,
    degree: i64,
    // Tiny rings use the direct iterator because the shared index's linear
    // plan cannot amortize its visitor overhead. Larger rings resolve edge ids
    // into the authoritative chain, preserving every lifted longitude turn.
    y_index: Option<EdgeYIndex>,
    roof: Option<PeriodicPoleRoof>,
    // Lifted longitude extremes of the whole chain. They select a query's
    // preimage copies and depend on nothing else, so they are proved once
    // here rather than rescanned — exactly once per vertex — for every point
    // the coverer classifies against this fill.
    longitude_envelope: (ExactExpansion, ExactExpansion),
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
    ) -> crate::error::Result<Option<Self>> {
        let Some(degree) = chain.degree else {
            return Ok(None);
        };
        // An ambiguous full-turn ring cannot certify a pole closure; returning
        // `None` makes the owning source explicitly fail open.
        let pole = if degree == 0 {
            None
        } else {
            let Some(pole) = physical_pole_for_chain(chain) else {
                return Ok(None);
            };
            Some(pole)
        };
        let roof = match pole {
            Some(pole) => {
                let Some(span) = exact_optional(ExactExpansion::exact_degree_turns(degree))? else {
                    return Ok(None);
                };
                Some(PeriodicPoleRoof {
                    pole,
                    turns: degree,
                    span,
                })
            },
            None => None,
        };
        let Some(longitude_envelope) = exact_optional(chain.longitude_envelope())? else {
            return Ok(None);
        };
        let Some(periodic_exact) =
            exact_optional(chain.periodic_exact(degree, &longitude_envelope))?
        else {
            return Ok(None);
        };
        let y_index = if uses_linear_plan_for_len(chain.edges.len()) {
            None
        } else {
            let mut latitudes = Vec::new();
            crate::try_reserve_vec(&mut latitudes, chain.vertices.len())?;
            latitudes.extend(chain.vertices.iter().map(|vertex| vertex.latitude));
            // Closed chains retain their repeated first vertex, so adjacent
            // spans include the represented closing edge without a synthetic
            // extra id.
            Some(EdgeYIndex::build(&latitudes)?)
        };
        Ok(Some(Self {
            chain_index,
            component,
            ring,
            degree,
            y_index,
            roof,
            longitude_envelope,
            periodic_exact,
        }))
    }

    /// Copies whose lifted longitude can lie on this finite ring fill.  The
    /// range is edge-proportional through the chain's extrema, never a global
    /// degree-based fallback.  The later exact PIP walks these copies.
    #[cfg(test)]
    fn direct_preimage_copies(&self, longitude: f64) -> Option<std::ops::RangeInclusive<i64>> {
        match self.direct_preimages(longitude).ok()? {
            DirectPreimages::Copies(copies) => Some(copies),
            DirectPreimages::Empty | DirectPreimages::Uncertain => None,
        }
    }

    /// Copies of `longitude` whose lift can land on this fill, from the
    /// envelope proved at construction.
    fn direct_preimages(&self, longitude: f64) -> ExactResult<DirectPreimages> {
        let query = match ExactExpansion::from_f64(longitude) {
            Ok(query) => query,
            Err(ExactFailure::Arithmetic) => return Ok(DirectPreimages::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
        let (minimum, maximum) = &self.longitude_envelope;
        direct_preimages_for_envelope(minimum, maximum, &query)
    }

    fn point_class(
        &self,
        chain: &LiftedChain,
        longitude: f64,
        query: &ExactPlanarPoint,
    ) -> ExactResult<GridPointClass> {
        let copies = match self.direct_preimages(longitude)? {
            // No lift of the stored query lies in this finite fill's longitude
            // span.  That is a certified exterior, not an arithmetic failure.
            DirectPreimages::Empty => return Ok(GridPointClass::Exterior),
            DirectPreimages::Copies(copies) => copies,
            DirectPreimages::Uncertain => return Ok(GridPointClass::Unknown),
        };
        let result = classify_lifted_ring_bucket(
            chain,
            self.y_index.as_ref(),
            self.roof.as_ref(),
            query,
            copies,
        );
        let result = result?;
        if result == GridPointClass::Interior && !self.periodic_exact {
            Ok(GridPointClass::Unknown)
        } else {
            Ok(result)
        }
    }

    #[cfg(test)]
    fn direct_oracle_point_class(
        &self,
        chain: &LiftedChain,
        longitude: f64,
        query: &ExactPlanarPoint,
    ) -> GridPointClass {
        let Ok(copies) = self.direct_preimages(longitude) else {
            return GridPointClass::Unknown;
        };
        let copies = match copies {
            DirectPreimages::Empty => return GridPointClass::Exterior,
            DirectPreimages::Copies(copies) => copies,
            DirectPreimages::Uncertain => return GridPointClass::Unknown,
        };
        let mut result = GridPointClass::Exterior;
        for copy in copies {
            let Some(shifted) = shifted_exact_planar_point(query, copy).ok() else {
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
) -> ExactResult<DirectPreimages> {
    let (Some(minimum_proposal), Some(maximum_proposal), Some(query_proposal)) = (
        minimum.checked_estimate(),
        maximum.checked_estimate(),
        query.checked_estimate(),
    ) else {
        return Ok(DirectPreimages::Uncertain);
    };
    let lower_proposal = ((minimum_proposal - query_proposal) / FULL_TURN_DEGREES).ceil();
    let upper_proposal = ((maximum_proposal - query_proposal) / FULL_TURN_DEGREES).floor();
    if !lower_proposal.is_finite()
        || !upper_proposal.is_finite()
        || !(i64::MIN as f64..=i64::MAX as f64).contains(&lower_proposal)
        || !(i64::MIN as f64..=i64::MAX as f64).contains(&upper_proposal)
    {
        return Ok(DirectPreimages::Uncertain);
    }
    let mut lower = lower_proposal as i64;
    let mut upper = upper_proposal as i64;
    for _ in 0..4 {
        let ordering = match compare_expansion_query_copy(minimum, query, lower) {
            Ok(ordering) => ordering,
            Err(ExactFailure::Arithmetic) => return Ok(DirectPreimages::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
        if ordering == Ordering::Greater {
            let Some(next) = lower.checked_add(1) else {
                return Ok(DirectPreimages::Uncertain);
            };
            lower = next;
            continue;
        }
        if lower > i64::MIN {
            let previous = match compare_expansion_query_copy(minimum, query, lower - 1) {
                Ok(previous) => previous,
                Err(ExactFailure::Arithmetic) => return Ok(DirectPreimages::Uncertain),
                Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
            };
            if previous != Ordering::Greater {
                lower -= 1;
                continue;
            }
        }
        break;
    }
    for _ in 0..4 {
        let ordering = match compare_expansion_query_copy(maximum, query, upper) {
            Ok(ordering) => ordering,
            Err(ExactFailure::Arithmetic) => return Ok(DirectPreimages::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
        if ordering == Ordering::Less {
            let Some(next) = upper.checked_sub(1) else {
                return Ok(DirectPreimages::Uncertain);
            };
            upper = next;
            continue;
        }
        if upper < i64::MAX {
            let next = match compare_expansion_query_copy(maximum, query, upper + 1) {
                Ok(next) => next,
                Err(ExactFailure::Arithmetic) => return Ok(DirectPreimages::Uncertain),
                Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
            };
            if next != Ordering::Less {
                upper += 1;
                continue;
            }
        }
        break;
    }
    if lower <= upper {
        Ok(DirectPreimages::Copies(lower..=upper))
    } else {
        Ok(DirectPreimages::Empty)
    }
}

fn compare_expansion_query_copy(
    value: &ExactExpansion,
    query: &ExactExpansion,
    turns: i64,
) -> ExactResult<Ordering> {
    let offset = ExactExpansion::exact_degree_turns(turns)?;
    let shifted = query.add(&offset)?;
    Ok(value.sub(&shifted)?.ordering())
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
    fn new(copies: std::ops::RangeInclusive<i64>, edge_count: usize) -> ExactResult<Self> {
        let first = *copies.start();
        let last = *copies.end();
        let length = last
            .checked_sub(first)
            .ok_or(ExactFailure::Arithmetic)?
            .checked_add(1)
            .ok_or(ExactFailure::Arithmetic)?;
        let length = usize::try_from(length).map_err(|_| ExactFailure::Arithmetic)?;
        // The exact lift changes by at most one turn per original edge.  A
        // wider range would violate the carrier premise, not authorize a
        // quadratic fallback.
        if length > edge_count.checked_add(2).ok_or(ExactFailure::Arithmetic)? {
            return Err(ExactFailure::Arithmetic);
        }
        let mut events = Vec::new();
        exact_allocation(crate::try_reserve_vec(&mut events, length))?;
        events.resize(length, false);
        Ok(Self {
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

#[derive(Debug)]
struct ExactPlanarPoint {
    longitude: ExactExpansion,
    latitude: ExactExpansion,
}

#[derive(Clone, Copy)]
struct ExactPlanarPointRef<'a> {
    longitude: &'a ExactExpansion,
    latitude: &'a ExactExpansion,
}

trait ExactPlanarCoordinates {
    fn longitude(&self) -> &ExactExpansion;
    fn latitude(&self) -> &ExactExpansion;
}

impl ExactPlanarCoordinates for ExactPlanarPoint {
    fn longitude(&self) -> &ExactExpansion {
        &self.longitude
    }

    fn latitude(&self) -> &ExactExpansion {
        &self.latitude
    }
}

impl ExactPlanarCoordinates for ExactPlanarPointRef<'_> {
    fn longitude(&self) -> &ExactExpansion {
        self.longitude
    }

    fn latitude(&self) -> &ExactExpansion {
        self.latitude
    }
}

struct ShiftedExactPlanarPoint<'a> {
    longitude: ExactExpansion,
    latitude: &'a ExactExpansion,
}

impl ExactPlanarCoordinates for ShiftedExactPlanarPoint<'_> {
    fn longitude(&self) -> &ExactExpansion {
        &self.longitude
    }

    fn latitude(&self) -> &ExactExpansion {
        self.latitude
    }
}

/// A selection-image endpoint can be a rational strip/reflection split.  It
/// stays rational until the exact PIP determinant consumes it; collapsing it
/// to a double would lose the t=1/3 identity this carrier was built to retain.
#[derive(Debug)]
struct ExactSelectionPoint {
    longitude: ExactRatio,
    latitude: ExactRatio,
}

impl ExactPlanarPoint {
    fn from_stored(longitude: f64, latitude: f64) -> ExactResult<Self> {
        Ok(Self {
            longitude: ExactExpansion::from_f64(longitude)?,
            latitude: ExactExpansion::from_f64(latitude)?,
        })
    }

    fn from_lifted(vertex: LiftedVertex) -> ExactResult<Self> {
        let longitude = ExactExpansion::from_f64(vertex.longitude.raw)?;
        let turns = ExactExpansion::exact_degree_turns(vertex.longitude.turns)?;
        Ok(Self {
            longitude: longitude.add(&turns)?,
            latitude: ExactExpansion::from_f64(vertex.latitude)?,
        })
    }
}

fn shifted_exact_planar_point<Q: ExactPlanarCoordinates + ?Sized>(
    point: &Q,
    turns: i64,
) -> ExactResult<ShiftedExactPlanarPoint<'_>> {
    let offset = ExactExpansion::exact_degree_turns(turns)?;
    Ok(ShiftedExactPlanarPoint {
        longitude: point.longitude().add(&offset)?,
        latitude: point.latitude(),
    })
}

fn classify_lifted_ring_bucket(
    chain: &LiftedChain,
    y_index: Option<&EdgeYIndex>,
    roof: Option<&PeriodicPoleRoof>,
    query: &ExactPlanarPoint,
    copies: std::ops::RangeInclusive<i64>,
) -> ExactResult<GridPointClass> {
    let mut buckets = match PeriodicParityBuckets::new(copies, chain.edges.len()) {
        Ok(buckets) => buckets,
        Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    if let Some(y_index) = y_index {
        let Some(query_y) = query.latitude.as_f64() else {
            return Ok(GridPointClass::Unknown);
        };
        let mut terminal = None;
        let mut failure = None;
        let mut visit =
            |edge: EdgeId| match bucket_lifted_chain_edge(chain, edge, query, &mut buckets) {
                Ok(BucketEdge::Continue) => std::ops::ControlFlow::Continue(()),
                Ok(BucketEdge::Decisive(result)) => {
                    terminal = Some(result);
                    std::ops::ControlFlow::Break(())
                },
                Ok(BucketEdge::Uncertain) | Err(ExactFailure::Arithmetic) => {
                    terminal = Some(GridPointClass::Unknown);
                    std::ops::ControlFlow::Break(())
                },
                Err(ExactFailure::Allocation) => {
                    failure = Some(ExactFailure::Allocation);
                    std::ops::ControlFlow::Break(())
                },
            };
        if let Some(edges) = y_index.dense_band_edges(query_y) {
            for &edge in edges {
                if visit(edge).is_break() {
                    break;
                }
            }
        } else {
            let _ = y_index.for_each_edge(query_y, visit);
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        if let Some(result) = terminal {
            return Ok(result);
        }
    } else {
        let mut previous = match chain.vertices.first().copied() {
            Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
                Ok(point) => point,
                Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
                Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
            },
            None => return Ok(GridPointClass::Unknown),
        };
        for vertex in &chain.vertices[1..] {
            let current = match ExactPlanarPoint::from_lifted(*vertex) {
                Ok(point) => point,
                Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
                Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
            };
            match bucket_lifted_edge(&previous, &current, query, false, &mut buckets)? {
                BucketEdge::Decisive(result) => return Ok(result),
                BucketEdge::Continue => {},
                BucketEdge::Uncertain => return Ok(GridPointClass::Unknown),
            }
            previous = current;
        }
    }
    let previous = match chain.vertices.last().copied() {
        Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
            Ok(point) => point,
            Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        },
        None => return Ok(GridPointClass::Unknown),
    };
    let first = match chain.vertices.first().copied() {
        Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
            Ok(point) => point,
            Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        },
        None => return Ok(GridPointClass::Unknown),
    };
    if chain.vertices.len() < 2 {
        return Ok(GridPointClass::Unknown);
    }
    if let Some(roof) = roof {
        let pole_latitude = match roof.pole {
            PhysicalEndpointKey::NorthPole => Some(ExactExpansion::from_i64(90)?),
            PhysicalEndpointKey::SouthPole => Some(ExactExpansion::from_i64(-90)?),
            PhysicalEndpointKey::CellVertex(_) => None,
        };
        let Some(pole_latitude) = pole_latitude else {
            return Ok(GridPointClass::Unknown);
        };
        let end_pole = ExactPlanarPointRef {
            longitude: &previous.longitude,
            latitude: &pole_latitude,
        };
        let start_pole = ExactPlanarPointRef {
            longitude: &first.longitude,
            latitude: &pole_latitude,
        };
        for (start, end) in [
            (
                &previous as &dyn ExactPlanarCoordinates,
                &end_pole as &dyn ExactPlanarCoordinates,
            ),
            (
                &end_pole as &dyn ExactPlanarCoordinates,
                &start_pole as &dyn ExactPlanarCoordinates,
            ),
            (
                &start_pole as &dyn ExactPlanarCoordinates,
                &first as &dyn ExactPlanarCoordinates,
            ),
        ] {
            match bucket_lifted_edge(start, end, query, true, &mut buckets)? {
                BucketEdge::Decisive(result) => return Ok(result),
                BucketEdge::Continue => {},
                BucketEdge::Uncertain => return Ok(GridPointClass::Unknown),
            }
        }
    }
    if buckets.any_interior() {
        Ok(GridPointClass::Interior)
    } else {
        Ok(GridPointClass::Exterior)
    }
}

fn bucket_lifted_chain_edge(
    chain: &LiftedChain,
    edge: EdgeId,
    query: &ExactPlanarPoint,
    buckets: &mut PeriodicParityBuckets,
) -> ExactResult<BucketEdge> {
    let edge = edge.as_usize();
    let Some(next) = edge.checked_add(1) else {
        return Ok(BucketEdge::Uncertain);
    };
    let start = match chain.vertices.get(edge).copied() {
        Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
            Ok(point) => point,
            Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        },
        None => return Ok(BucketEdge::Uncertain),
    };
    let end = match chain.vertices.get(next).copied() {
        Some(vertex) => match ExactPlanarPoint::from_lifted(vertex) {
            Ok(point) => point,
            Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        },
        None => return Ok(BucketEdge::Uncertain),
    };
    bucket_lifted_edge(&start, &end, query, false, buckets)
}

/// One exact edge's impact on the periodic parity sweep.  `Uncertain` is
/// intentionally distinct from a boundary result: neither may establish a
/// negative, but the caller preserves the former as a failed proof.
enum BucketEdge {
    Continue,
    Decisive(GridPointClass),
    Uncertain,
}

fn bucket_lifted_edge<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
    artificial: bool,
    buckets: &mut PeriodicParityBuckets,
) -> ExactResult<BucketEdge> {
    let start_y = match start.latitude().sub(query.latitude()) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let end_y = match end.latitude().sub(query.latitude()) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let on_latitude =
        match closed_between_expansion(start.latitude(), query.latitude(), end.latitude()) {
            Ok(value) => value,
            Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
    if on_latitude {
        let delta = match start.longitude().sub(end.longitude()) {
            Ok(delta) => delta,
            Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
        let (minimum, maximum) = if delta.ordering() == Ordering::Greater {
            (end.longitude(), start.longitude())
        } else {
            (start.longitude(), end.longitude())
        };
        match direct_preimages_for_envelope(minimum, maximum, query.longitude())? {
            DirectPreimages::Empty => {},
            DirectPreimages::Uncertain => {
                return Ok(BucketEdge::Decisive(GridPointClass::Unknown));
            },
            DirectPreimages::Copies(copies) => {
                let Some(count) = copies
                    .end()
                    .checked_sub(*copies.start())
                    .and_then(|value| value.checked_add(1))
                else {
                    return Ok(BucketEdge::Uncertain);
                };
                // A shortest/full original edge can meet at most two seam
                // spellings.  The artificial horizontal roof is excluded by
                // the non-pole query contract before reaching this branch.
                if count > 2 {
                    return Ok(BucketEdge::Decisive(GridPointClass::Unknown));
                }
                for copy in copies {
                    let shifted = match shifted_exact_planar_point(query, copy) {
                        Ok(shifted) => shifted,
                        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
                        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
                    };
                    let orientation = match planar_orientation_ordering(start, end, &shifted) {
                        Ok(orientation) => orientation,
                        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
                        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
                    };
                    let on_longitude = match closed_between_expansion(
                        start.longitude(),
                        &shifted.longitude,
                        end.longitude(),
                    ) {
                        Ok(on_longitude) => on_longitude,
                        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
                        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
                    };
                    if orientation == Ordering::Equal && on_longitude {
                        return Ok(BucketEdge::Decisive(if artificial {
                            GridPointClass::Unknown
                        } else {
                            GridPointClass::Boundary
                        }));
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
        return Ok(BucketEdge::Continue);
    }
    let orientation = match planar_orientation(start, end, query) {
        Ok(orientation) => orientation,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let delta_y = match end.latitude().sub(start.latitude()) {
        Ok(delta_y) => delta_y,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    if delta_y.ordering() == Ordering::Equal {
        return Ok(BucketEdge::Continue);
    }
    let ratio = match ExactRatio::from_parts(orientation, delta_y) {
        Ok(ratio) => ratio,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let bucket = match ratio.floor_turn() {
        Ok(bucket) => bucket,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    if buckets.add_crossing(bucket).is_none() {
        return Ok(BucketEdge::Uncertain);
    }
    Ok(BucketEdge::Continue)
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
) -> ExactResult<BucketEdge> {
    let query_longitude = match ExactRatio::from_expansion(&query.longitude) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let query_latitude = match ExactRatio::from_expansion(&query.latitude) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let start_y = match start.latitude.sub(&query_latitude) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let end_y = match end.latitude.sub(&query_latitude) {
        Ok(value) => value,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let (start_cmp, end_cmp) = (start_y.numerator.ordering(), end_y.numerator.ordering());

    // A query on an original selected edge is closed contact.  Test the three
    // only physical sheet preimages individually so exact +/-180 spellings
    // cannot be normalized into an unrelated line.
    let on_latitude =
        match selection_closed_between(&start.latitude, &query_latitude, &end.latitude) {
            Ok(value) => value,
            Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
    if on_latitude {
        for turns in -1_i64..=1_i64 {
            let offset = match turns.checked_mul(360) {
                Some(value) => match ExactRatio::from_i64(value, 1) {
                    Ok(offset) => offset,
                    Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
                    Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
                },
                None => return Ok(BucketEdge::Uncertain),
            };
            let shifted_longitude = match query_longitude.add(&offset) {
                Ok(value) => value,
                Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
                Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
            };
            let orientation =
                match selection_orientation_at(start, end, &shifted_longitude, &query_latitude) {
                    Ok(orientation) => orientation,
                    Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
                    Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
                };
            let on_longitude = match selection_closed_between(
                &start.longitude,
                &shifted_longitude,
                &end.longitude,
            ) {
                Ok(on_longitude) => on_longitude,
                Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
                Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
            };
            if orientation.numerator.ordering() == Ordering::Equal && on_longitude {
                return Ok(BucketEdge::Decisive(GridPointClass::Boundary));
            }
        }
    }

    let crosses = (start_cmp != Ordering::Greater && end_cmp == Ordering::Greater)
        || (end_cmp != Ordering::Greater && start_cmp == Ordering::Greater);
    if !crosses {
        return Ok(BucketEdge::Continue);
    }
    let orientation = match selection_orientation_at(start, end, &query_longitude, &query_latitude)
    {
        Ok(orientation) => orientation,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let delta_y = match end.latitude.sub(&start.latitude) {
        Ok(delta_y) => delta_y,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let ratio = match orientation.div(&delta_y) {
        Ok(ratio) => ratio,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    let bucket = match ratio.floor_turn() {
        Ok(bucket) => bucket,
        Err(ExactFailure::Arithmetic) => return Ok(BucketEdge::Uncertain),
        Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
    };
    if buckets.add_crossing(bucket).is_none() {
        return Ok(BucketEdge::Uncertain);
    }
    Ok(BucketEdge::Continue)
}

fn selection_orientation_at(
    start: &ExactSelectionPoint,
    end: &ExactSelectionPoint,
    point_longitude: &ExactRatio,
    point_latitude: &ExactRatio,
) -> ExactResult<ExactRatio> {
    let dx = end.longitude.sub(&start.longitude)?;
    let dy = end.latitude.sub(&start.latitude)?;
    let px = point_longitude.sub(&start.longitude)?;
    let py = point_latitude.sub(&start.latitude)?;
    let left = dx.mul(&py)?;
    let right = dy.mul(&px)?;
    left.sub(&right)
}

fn selection_closed_between(
    left: &ExactRatio,
    middle: &ExactRatio,
    right: &ExactRatio,
) -> ExactResult<bool> {
    let left_middle = left.ordering(middle)?;
    let middle_right = middle.ordering(right)?;
    Ok(closed_between_orderings(left_middle, middle_right))
}

/// Exact even-odd point location in the finite lifted fill.  This direct
/// per-copy form is deliberately private to the carrier proof; the traversal
/// installs the design's linear bucket walk before it calls this at leaves.
/// Until then no public path can acquire its potentially wider `Unknown`.
#[cfg(test)]
fn classify_lifted_ring_copy<Q: ExactPlanarCoordinates + ?Sized>(
    chain: &LiftedChain,
    roof: Option<&PeriodicPoleRoof>,
    query: &Q,
) -> GridPointClass {
    let Some(mut previous) = chain
        .vertices
        .first()
        .copied()
        .and_then(|vertex| ExactPlanarPoint::from_lifted(vertex).ok())
    else {
        return GridPointClass::Unknown;
    };
    let mut parity = false;
    for vertex in &chain.vertices[1..] {
        let Some(current) = ExactPlanarPoint::from_lifted(*vertex).ok() else {
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
            .and_then(|vertex| ExactPlanarPoint::from_lifted(vertex).ok())
        else {
            return GridPointClass::Unknown;
        };
        let pole_latitude = match roof.pole {
            PhysicalEndpointKey::NorthPole => ExactExpansion::from_i64(90).ok(),
            PhysicalEndpointKey::SouthPole => ExactExpansion::from_i64(-90).ok(),
            PhysicalEndpointKey::CellVertex(_) => None,
        };
        let Some(pole_latitude) = pole_latitude else {
            return GridPointClass::Unknown;
        };
        let end_pole = ExactPlanarPointRef {
            longitude: &previous.longitude,
            latitude: &pole_latitude,
        };
        let start_pole = ExactPlanarPointRef {
            longitude: &first.longitude,
            latitude: &pole_latitude,
        };
        for (start, end) in [
            (
                &previous as &dyn ExactPlanarCoordinates,
                &end_pole as &dyn ExactPlanarCoordinates,
            ),
            (
                &end_pole as &dyn ExactPlanarCoordinates,
                &start_pole as &dyn ExactPlanarCoordinates,
            ),
            (
                &start_pole as &dyn ExactPlanarCoordinates,
                &first as &dyn ExactPlanarCoordinates,
            ),
        ] {
            match classify_lifted_edge(start, end, query, true, &mut parity) {
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
fn classify_lifted_edge<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
    artificial: bool,
    parity: &mut bool,
) -> GridPointClass {
    let Some(orientation) = planar_orientation_ordering(start, end, query).ok() else {
        return GridPointClass::Unknown;
    };
    let (Some(start_y), Some(end_y)) = (
        start.latitude().sub(query.latitude()).ok(),
        end.latitude().sub(query.latitude()).ok(),
    ) else {
        return GridPointClass::Unknown;
    };
    if orientation == Ordering::Equal
        && closed_between_expansion(start.longitude(), query.longitude(), end.longitude())
            .ok()
            .unwrap_or(false)
        && closed_between_expansion(start.latitude(), query.latitude(), end.latitude())
            .ok()
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
    let Some(delta_y) = end.latitude().sub(start.latitude()).ok() else {
        return GridPointClass::Unknown;
    };
    if delta_y.ordering() == Ordering::Equal {
        return GridPointClass::Exterior;
    }
    // x_cross - qx is orientation / (end.y - start.y).  Cross only when the
    // two exact signs agree; no division or rounded ray intersection exists.
    if orientation == delta_y.ordering() {
        *parity = !*parity;
    }
    GridPointClass::Exterior
}

fn closed_between_orderings(left_middle: Ordering, middle_right: Ordering) -> bool {
    left_middle == Ordering::Equal || middle_right == Ordering::Equal || left_middle == middle_right
}

fn closed_between_expansion(
    left: &ExactExpansion,
    middle: &ExactExpansion,
    right: &ExactExpansion,
) -> ExactResult<bool> {
    let left_middle = left.sub(middle)?.ordering();
    let middle_right = middle.sub(right)?.ordering();
    Ok(closed_between_orderings(left_middle, middle_right))
}

fn closed_between_planar_points<
    L: ExactPlanarCoordinates + ?Sized,
    M: ExactPlanarCoordinates + ?Sized,
    R: ExactPlanarCoordinates + ?Sized,
>(
    left: &L,
    middle: &M,
    right: &R,
) -> ExactResult<bool> {
    let longitude =
        closed_between_expansion(left.longitude(), middle.longitude(), right.longitude())?;
    let latitude = closed_between_expansion(left.latitude(), middle.latitude(), right.latitude())?;
    Ok(longitude && latitude)
}

fn planar_orientation<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
) -> ExactResult<ExactExpansion> {
    let dx = end.longitude().sub(start.longitude())?;
    let dy = end.latitude().sub(start.latitude())?;
    let qx = query.longitude().sub(start.longitude())?;
    let qy = query.latitude().sub(start.latitude())?;
    let left = dx.product(&qy)?;
    let right = dy.product(&qx)?;
    left.sub(&right)
}

/// Certify the ordinary one-limb case before constructing expansion products.
/// Every arithmetic result must be normal and finite, so neither overflow nor
/// underflow can invalidate the binary64 relative-error bound.
fn filtered_planar_orientation<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
) -> Option<Ordering> {
    let (start_x, start_y, end_x, end_y, query_x, query_y) = (
        start.longitude().as_f64()?,
        start.latitude().as_f64()?,
        end.longitude().as_f64()?,
        end.latitude().as_f64()?,
        query.longitude().as_f64()?,
        query.latitude().as_f64()?,
    );
    let dx = end_x - start_x;
    let dy = end_y - start_y;
    let qx = query_x - start_x;
    let qy = query_y - start_y;
    let left = dx * qy;
    let right = dy * qx;
    let determinant = left - right;
    let magnitude = left.abs() + right.abs();
    if ![dx, dy, qx, qy, left, right, determinant, magnitude]
        .into_iter()
        .all(|value| value == 0.0 || value.is_normal())
    {
        return None;
    }
    let bound = (magnitude * ORIENTATION_ERRBOUND).next_up();
    (determinant.abs() > bound).then(|| determinant.total_cmp(&0.0))
}

fn planar_orientation_ordering<
    S: ExactPlanarCoordinates + ?Sized,
    E: ExactPlanarCoordinates + ?Sized,
    Q: ExactPlanarCoordinates + ?Sized,
>(
    start: &S,
    end: &E,
    query: &Q,
) -> ExactResult<Ordering> {
    if let Some(ordering) = filtered_planar_orientation(start, end, query) {
        Ok(ordering)
    } else {
        Ok(planar_orientation(start, end, query)?.ordering())
    }
}

impl LiftedChain {
    fn from_points(points: &[(f64, f64)], closed: bool) -> ExactResult<Self> {
        let minimum = if closed { 4 } else { 2 };
        if points.len() < minimum || !points.iter().all(|(x, y)| x.is_finite() && y.is_finite()) {
            return Err(ExactFailure::Arithmetic);
        }
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        exact_allocation(crate::try_reserve_vec(&mut vertices, points.len()))?;
        exact_allocation(crate::try_reserve_vec(
            &mut edges,
            points.len().saturating_sub(1),
        ))?;
        exact_allocation(crate::try_push_vec(&mut vertices, LiftedVertex {
            longitude: LiftedLongitude {
                raw: points[0].0,
                turns: 0,
            },
            latitude: points[0].1,
        }))?;
        for &(longitude, latitude) in &points[1..] {
            let previous = exact(vertices.last().copied())?;
            let step = lifted_step(previous.longitude.raw, longitude)?;
            let turns = previous
                .longitude
                .turns
                .checked_add(step.turn_delta)
                .ok_or(ExactFailure::Arithmetic)?;
            exact_allocation(crate::try_push_vec(&mut vertices, LiftedVertex {
                longitude: LiftedLongitude {
                    raw: longitude,
                    turns,
                },
                latitude,
            }))?;
            exact_allocation(crate::try_push_vec(&mut edges, step.kind))?;
            while vertices.len() > minimum {
                let last = vertices
                    .len()
                    .checked_sub(1)
                    .ok_or(ExactFailure::Arithmetic)?;
                let previous =
                    *exact(vertices.get(last.checked_sub(2).ok_or(ExactFailure::Arithmetic)?))?;
                let current =
                    *exact(vertices.get(last.checked_sub(1).ok_or(ExactFailure::Arithmetic)?))?;
                let next = *exact(vertices.get(last))?;
                if !can_coalesce(previous, current, next)? {
                    break;
                }
                // `can_coalesce` has already certified this exact direct lift;
                // retain its edge kind while removing the middle vertex.
                let direct = lifted_step(previous.longitude.raw, next.longitude.raw)?;
                vertices.remove(last - 1);
                edges.remove(last - 1);
                *exact(edges.get_mut(last.checked_sub(2).ok_or(ExactFailure::Arithmetic)?))? =
                    direct.kind;
            }
        }
        let mut result = Self {
            vertices,
            edges,
            closed,
            degree: None,
        };
        if closed {
            result.degree = Some(result.closure_degree()?.ok_or(ExactFailure::Arithmetic)?);
        }
        Ok(result)
    }

    fn closure_degree(&self) -> ExactResult<Option<i64>> {
        let first = *exact(self.vertices.first())?;
        let last = *exact(self.vertices.last())?;
        if !same_physical_latitude(first.latitude, last.latitude) {
            return Ok(None);
        }
        let total = last.longitude.difference(first.longitude)?;
        let edge_count = i64::try_from(self.edges.len()).map_err(|_| ExactFailure::Arithmetic)?;
        let raw_alias_turn = if first.longitude.raw.to_bits() == last.longitude.raw.to_bits() {
            0
        } else if first.longitude.raw.to_bits() == (-HALF_TURN_DEGREES).to_bits()
            && last.longitude.raw.to_bits() == HALF_TURN_DEGREES.to_bits()
        {
            1
        } else if first.longitude.raw.to_bits() == HALF_TURN_DEGREES.to_bits()
            && last.longitude.raw.to_bits() == (-HALF_TURN_DEGREES).to_bits()
        {
            -1
        } else {
            return Ok(None);
        };
        let degree = last
            .longitude
            .turns
            .checked_sub(first.longitude.turns)
            .ok_or(ExactFailure::Arithmetic)?
            .checked_add(raw_alias_turn)
            .ok_or(ExactFailure::Arithmetic)?;
        if !(-edge_count..=edge_count).contains(&degree) {
            return Ok(None);
        }
        let expected = ExactExpansion::exact_degree_turns(degree)?;
        let difference = total.sub(&expected)?;
        Ok((difference.ordering() == Ordering::Equal).then_some(degree))
    }

    #[cfg(test)]
    fn direct_preimage_copies(&self, longitude: f64) -> Option<std::ops::RangeInclusive<i64>> {
        match self.direct_preimages(longitude).ok()? {
            DirectPreimages::Copies(copies) => Some(copies),
            DirectPreimages::Empty | DirectPreimages::Uncertain => None,
        }
    }

    #[cfg(test)]
    fn direct_preimages(&self, longitude: f64) -> ExactResult<DirectPreimages> {
        let (minimum, maximum) = match self.longitude_envelope() {
            Ok(envelope) => envelope,
            Err(ExactFailure::Arithmetic) => return Ok(DirectPreimages::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
        let query = match ExactExpansion::from_f64(longitude) {
            Ok(query) => query,
            Err(ExactFailure::Arithmetic) => return Ok(DirectPreimages::Uncertain),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
        direct_preimages_for_envelope(&minimum, &maximum, &query)
    }

    fn longitude_envelope(&self) -> ExactResult<(ExactExpansion, ExactExpansion)> {
        let first = *exact(self.vertices.first())?;
        let mut minimum = ExactPlanarPoint::from_lifted(first)?.longitude;
        let mut maximum = minimum.try_clone()?;
        for &vertex in &self.vertices[1..] {
            let value = ExactPlanarPoint::from_lifted(vertex)?.longitude;
            if value.sub(&minimum)?.ordering() == Ordering::Less {
                minimum = value.try_clone()?;
            }
            if value.sub(&maximum)?.ordering() == Ordering::Greater {
                maximum = value;
            }
        }
        Ok((minimum, maximum))
    }

    /// Proves that quotienting the finite lift cannot create a second open
    /// overlap with one of its nonzero 360-degree translates.  Width below a
    /// turn is immediate.  At exactly one turn we admit only the explicit
    /// +/-1 pole-roof grammar: one endpoint on each extremum, all original
    /// vertices strictly between them, and no full-world source edge.  Every
    /// other equality case remains an outer fill, so its interior is demoted
    /// to `Unknown` by `LiftedRingFill::point_class`.
    fn periodic_exact(
        &self,
        degree: i64,
        (minimum, maximum): &(ExactExpansion, ExactExpansion),
    ) -> ExactResult<bool> {
        let width = maximum.sub(minimum)?;
        let turn = ExactExpansion::from_f64(FULL_TURN_DEGREES)?;
        match width.sub(&turn)?.ordering() {
            Ordering::Less => return Ok(true),
            Ordering::Greater => return Ok(false),
            Ordering::Equal => {},
        }
        if degree.unsigned_abs() != 1
            || self
                .edges
                .iter()
                .any(|kind| !matches!(kind, LiftKind::Shortest))
        {
            return Ok(false);
        }
        let first = ExactPlanarPoint::from_lifted(*exact(self.vertices.first())?)?.longitude;
        let last = ExactPlanarPoint::from_lifted(*exact(self.vertices.last())?)?.longitude;
        let endpoints_are_extrema = (first.sub(minimum)?.ordering() == Ordering::Equal
            && last.sub(maximum)?.ordering() == Ordering::Equal)
            || (first.sub(maximum)?.ordering() == Ordering::Equal
                && last.sub(minimum)?.ordering() == Ordering::Equal);
        if !endpoints_are_extrema {
            return Ok(false);
        }
        for &vertex in self
            .vertices
            .get(1..self.vertices.len().saturating_sub(1))
            .unwrap_or_default()
        {
            let value = ExactPlanarPoint::from_lifted(vertex)?.longitude;
            if value.sub(minimum)?.ordering() != Ordering::Greater
                || maximum.sub(&value)?.ordering() != Ordering::Greater
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// One original affine source edge with its exact, ordered parent cuts.  The
/// retained endpoints are in the continuous lift; later selection pieces only
/// add integer reflect/wrap transforms and never replace this authority edge.
#[derive(Debug)]
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
    parameters: Vec<ExactParentParameter>,
}

/// Fields shared by an authority piece and its corresponding selection image.
///
/// Authority and selection remain distinct records because only selection
/// carries reflection/wrap intervals and can contribute positive-only
/// witnesses. Keeping their common identity in one value prevents the two
/// records from drifting on edge identity, dimensional kind, or spherical arc.
#[derive(Clone, Copy, Debug)]
struct AffinePieceCarrier {
    edge: SourceEdgeKey,
    polygon: bool,
    arc: AffineSourceArc,
}

#[derive(Debug)]
pub(crate) struct SelectionAffinePiece {
    carrier: AffinePieceCarrier,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "retained for exact parent-interval unit certificates"
        )
    )]
    interval: ParentInterval,
    latitude_zone: i64,
    // Exact degree endpoints after the selected reflect/wrap map.  They are
    // kept beside the spherical evaluator because the selection image may
    // veto an authority negative only through its own affine planar PIP;
    // reconstructing them from rounded radians would create a second source.
    selection: [ExactSelectionPoint; 2],
}

/// A split either does not cross the parent interval or has one exact parent
/// ratio. Arithmetic failure is deliberately the enclosing `Option::None`,
/// distinct from an ordinary absent crossing.
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
    ) -> ExactResult<Self> {
        let start_point = ExactPlanarPoint::from_lifted(start)?;
        let end_point = ExactPlanarPoint::from_lifted(end)?;
        let mut parameters = Vec::new();
        exact_allocation(crate::try_reserve_vec(&mut parameters, 8))?;
        parameters.push(ExactParentParameter::start()?);
        parameters.push(ExactParentParameter::end()?);
        let mut result = Self {
            key,
            endpoints,
            polygon,
            start,
            end,
            parameters,
        };
        if matches!(kind, LiftKind::FullPositive | LiftKind::FullNegative) {
            result.insert_ratio(
                ParentParameterKey::Partition(PartitionConstraint::FullEdgeMidpoint),
                ExactRatio::from_i64(1, 2)?,
            )?;
        }
        result.collect_lifted_longitude_seams(&start_point, &end_point)?;
        result.collect_latitude_reflections(&start_point, &end_point)?;
        result.collect_post_reflection_longitude_seams(&start_point, &end_point)?;
        Ok(result)
    }

    fn parameter_for(
        &self,
        start: &ExactExpansion,
        end: &ExactExpansion,
        target: &ExactExpansion,
    ) -> ExactResult<ParentCrossing> {
        let denominator = end.sub(start)?;
        if denominator.ordering() == Ordering::Equal {
            return Ok(ParentCrossing::Absent);
        }
        let numerator = target.sub(start)?;
        let ratio = ExactRatio::from_parts(numerator, denominator)?;
        Ok(if ratio.is_closed_unit()? {
            ParentCrossing::Present(ratio)
        } else {
            ParentCrossing::Absent
        })
    }

    fn insert_ratio(&mut self, key: ParentParameterKey, ratio: ExactRatio) -> ExactResult<()> {
        let parameter = ExactParentParameter::new(key, ratio)?;
        let mut insert_at = self.parameters.len();
        for (index, existing) in self.parameters.iter().enumerate() {
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
                        self.parameters[index] = parameter;
                    }
                    return Ok(());
                },
                Ordering::Greater => {},
            }
        }
        exact_allocation(crate::try_reserve_vec(&mut self.parameters, 1))?;
        self.parameters.insert(insert_at, parameter);
        Ok(())
    }

    fn collect_lifted_longitude_seams(
        &mut self,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<()> {
        let start = start_point.longitude();
        let end = end_point.longitude();
        for level in integer_levels_between(start, end, 180, 360)? {
            let seam = exact_affine_level(180, 360, level)?;
            if let ParentCrossing::Present(ratio) = self.parameter_for(start, end, &seam)? {
                self.insert_ratio(
                    ParentParameterKey::Partition(PartitionConstraint::LiftedLongitudeSeam),
                    ratio,
                )?;
            }
        }
        Ok(())
    }

    fn collect_latitude_reflections(
        &mut self,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<()> {
        let start = start_point.latitude();
        let end = end_point.latitude();
        for level in integer_levels_between(start, end, 90, 180)? {
            let reflection = exact_affine_level(90, 180, level)?;
            if let ParentCrossing::Present(ratio) = self.parameter_for(start, end, &reflection)? {
                self.insert_ratio(
                    ParentParameterKey::Partition(PartitionConstraint::LatitudeReflection),
                    ratio,
                )?;
            }
        }
        Ok(())
    }

    fn collect_post_reflection_longitude_seams(
        &mut self,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<()> {
        let latitude_start = start_point.latitude();
        let latitude_end = end_point.latitude();
        let longitude_start = start_point.longitude();
        let longitude_end = end_point.longitude();
        // Inside latitude zone z, selection adds 180*z to longitude before
        // wrapping.  Solving its seams in the *parent* map keeps every split
        // exact even though a later evaluator uses outward `Bound`s.
        for zone in integer_levels_between(latitude_start, latitude_end, 0, 180)? {
            let lower = exact_affine_level(-90, 180, zone)?;
            let upper = exact_affine_level(90, 180, zone)?;
            let lower = ExactRatio::from_parts(lower, ExactExpansion::from_i64(1)?)?;
            let upper = ExactRatio::from_parts(upper, ExactExpansion::from_i64(1)?)?;
            let seam_base = 180_i64
                .checked_sub(180_i64.checked_mul(zone).ok_or(ExactFailure::Arithmetic)?)
                .ok_or(ExactFailure::Arithmetic)?;
            for seam in integer_levels_between(longitude_start, longitude_end, seam_base, 360)? {
                let longitude = exact_affine_level(seam_base, 360, seam)?;
                let ParentCrossing::Present(ratio) =
                    self.parameter_for(longitude_start, longitude_end, &longitude)?
                else {
                    continue;
                };
                let latitude = self.coordinate_at(&ratio, latitude_start, latitude_end)?;
                if latitude.ordering(&lower)? == Ordering::Less
                    || latitude.ordering(&upper)? == Ordering::Greater
                {
                    continue;
                }
                self.insert_ratio(
                    ParentParameterKey::Partition(PartitionConstraint::PostReflectionLongitudeSeam),
                    ratio,
                )?;
            }
        }
        Ok(())
    }

    fn coordinate_at(
        &self,
        parameter: &ExactRatio,
        start: &ExactExpansion,
        end: &ExactExpansion,
    ) -> ExactResult<ExactRatio> {
        let delta = end.sub(start)?;
        let first = start.product(&parameter.denominator)?;
        let second = delta.product(&parameter.numerator)?;
        let numerator = first.add(&second)?;
        ExactRatio::from_numerator(numerator, &parameter.denominator)
    }

    fn midpoint(&self, left: &ExactRatio, right: &ExactRatio) -> ExactResult<ExactRatio> {
        let first = left.numerator.product(&right.denominator)?;
        let second = right.numerator.product(&left.denominator)?;
        let numerator = first.add(&second)?;
        let denominator_product = left.denominator.product(&right.denominator)?;
        let two = ExactExpansion::from_i64(2)?;
        let denominator = denominator_product.product(&two)?;
        ExactRatio::from_parts(numerator, denominator)
    }

    fn selection_arc(
        &self,
        interval: &ParentInterval,
        latitude_zone: i64,
        longitude_wrap: i64,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<AffineSourceArc> {
        let longitude_offset = 180_i64
            .checked_mul(latitude_zone)
            .ok_or(ExactFailure::Arithmetic)?
            .checked_sub(
                360_i64
                    .checked_mul(longitude_wrap)
                    .ok_or(ExactFailure::Arithmetic)?,
            )
            .ok_or(ExactFailure::Arithmetic)?;
        let longitude_offset = ExactExpansion::from_i64(longitude_offset)?;
        let longitude0 = start_point.longitude().add(&longitude_offset)?;
        let longitude1 = end_point.longitude().add(&longitude_offset)?;
        let longitude_delta = longitude1.sub(&longitude0)?;

        let latitude0 = start_point.latitude();
        let latitude1 = end_point.latitude();
        let latitude_offset = ExactExpansion::from_i64(
            180_i64
                .checked_mul(latitude_zone)
                .ok_or(ExactFailure::Arithmetic)?,
        )?;
        let (latitude0, latitude1) = if latitude_zone.rem_euclid(2) == 0 {
            (
                latitude0.sub(&latitude_offset)?,
                latitude1.sub(&latitude_offset)?,
            )
        } else {
            (
                latitude_offset.sub(latitude0)?,
                latitude_offset.sub(latitude1)?,
            )
        };
        let latitude_delta = latitude1.sub(&latitude0)?;
        let structure =
            affine_structure(&longitude0, &longitude_delta, &latitude0, &latitude_delta)?;
        let map = AffineParentMap {
            lambda0: degree_expansion_bound(&longitude0)
                .and_then(|bound| exact(degree_bound_to_radians(bound)))?,
            phi0: degree_expansion_bound(&latitude0)
                .and_then(|bound| exact(degree_bound_to_radians(bound)))?,
            dlambda: degree_expansion_bound(&longitude_delta)
                .and_then(|bound| exact(degree_bound_to_radians(bound)))?,
            dphi: degree_expansion_bound(&latitude_delta)
                .and_then(|bound| exact(degree_bound_to_radians(bound)))?,
            structure,
        };
        let endpoint_roles = [
            endpoint_role(interval.start.key),
            endpoint_role(interval.end.key),
        ];
        let poles = [
            self.selection_endpoint_pole(&interval.start, latitude_zone, start_point, end_point)?,
            self.selection_endpoint_pole(&interval.end, latitude_zone, start_point, end_point)?,
        ];
        let identities = [
            self.selection_endpoint_identity(
                &interval.start,
                latitude_zone,
                longitude_wrap,
                start_point,
                end_point,
            )?,
            self.selection_endpoint_identity(
                &interval.end,
                latitude_zone,
                longitude_wrap,
                start_point,
                end_point,
            )?,
        ];
        exact(AffineSourceArc::from_parent_map(
            &map,
            exact(Bound::new(
                interval.start.enclosure.lo,
                interval.end.enclosure.hi,
            ))?,
            endpoint_roles,
            poles,
            identities,
        ))
    }

    fn selection_endpoint_pole(
        &self,
        parameter: &ExactParentParameter,
        latitude_zone: i64,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<Option<PhysicalEndpointKey>> {
        if matches!(parameter.key, ParentParameterKey::Partition(_)) {
            return Ok(None);
        }
        let latitude = self.coordinate_at(
            &parameter.ratio,
            start_point.latitude(),
            end_point.latitude(),
        )?;
        let latitude = selection_latitude_ratio(&latitude, latitude_zone)?;
        let north = ExactRatio::from_i64(90, 1)?;
        let south = ExactRatio::from_i64(-90, 1)?;
        if latitude.ordering(&north)? == Ordering::Equal {
            Ok(Some(PhysicalEndpointKey::NorthPole))
        } else if latitude.ordering(&south)? == Ordering::Equal {
            Ok(Some(PhysicalEndpointKey::SouthPole))
        } else {
            Ok(None)
        }
    }

    fn selection_piece(
        &self,
        start: ExactParentParameter,
        end: ExactParentParameter,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<Option<SelectionAffinePiece>> {
        let midpoint = self.midpoint(&start.ratio, &end.ratio)?;
        let latitude =
            self.coordinate_at(&midpoint, start_point.latitude(), end_point.latitude())?;
        let latitude_bound = latitude.certified_bound()?;
        let latitude_midpoint = latitude_bound.lo + (latitude_bound.hi - latitude_bound.lo) * 0.5;
        let zone_proposal = ((latitude_midpoint + 90.0) / HALF_TURN_DEGREES).floor();
        if !zone_proposal.is_finite()
            || zone_proposal < i64::MIN as f64
            || zone_proposal > i64::MAX as f64
        {
            return Ok(None);
        }
        // A certified bound can straddle a stored-double neighbour of a strip
        // edge even though the exact midpoint is strictly inside one strip:
        // at `nextafter(90, 0)` its rounded floating midpoint is 90.  Use the
        // float only to nominate nearby integer strips, then select by the
        // exact ratio. This is not an epsilon: exactly one open strip can
        // satisfy the two strict comparisons.
        let proposed_zone = zone_proposal as i64;
        let mut zone = None;
        for delta in -1_i64..=1 {
            let candidate = proposed_zone
                .checked_add(delta)
                .ok_or(ExactFailure::Arithmetic)?;
            let lower = ExactRatio::from_parts(
                exact_affine_level(-90, 180, candidate)?,
                ExactExpansion::from_i64(1)?,
            )?;
            let upper = ExactRatio::from_parts(
                exact_affine_level(90, 180, candidate)?,
                ExactExpansion::from_i64(1)?,
            )?;
            if latitude.ordering(&lower)? == Ordering::Greater
                && latitude.ordering(&upper)? == Ordering::Less
            {
                zone = Some(candidate);
                break;
            }
        }
        let Some(zone) = zone else {
            return Ok(None);
        };
        let zone_lower = ExactRatio::from_parts(
            exact_affine_level(-90, 180, zone)?,
            ExactExpansion::from_i64(1)?,
        )?;
        let zone_upper = ExactRatio::from_parts(
            exact_affine_level(90, 180, zone)?,
            ExactExpansion::from_i64(1)?,
        )?;
        debug_assert_eq!(latitude.ordering(&zone_lower)?, Ordering::Greater);
        debug_assert_eq!(latitude.ordering(&zone_upper)?, Ordering::Less);

        let longitude =
            self.coordinate_at(&midpoint, start_point.longitude(), end_point.longitude())?;
        let offset =
            ExactExpansion::from_i64(180_i64.checked_mul(zone).ok_or(ExactFailure::Arithmetic)?)?;
        let offset_product = offset.product(&longitude.denominator)?;
        let selection_numerator = longitude.numerator.add(&offset_product)?;
        let selection_longitude =
            ExactRatio::from_numerator(selection_numerator, &longitude.denominator)?;
        let longitude_bound = selection_longitude.certified_bound()?;
        let longitude_midpoint =
            longitude_bound.lo + (longitude_bound.hi - longitude_bound.lo) * 0.5;
        let wrap_proposal = ((longitude_midpoint + HALF_TURN_DEGREES) / FULL_TURN_DEGREES).floor();
        if !wrap_proposal.is_finite()
            || wrap_proposal < i64::MIN as f64
            || wrap_proposal > i64::MAX as f64
        {
            return Ok(None);
        }
        let mut longitude_wrap = wrap_proposal as i64;
        let mut lower = ExactRatio::from_parts(
            exact_affine_level(-180, 360, longitude_wrap)?,
            ExactExpansion::from_i64(1)?,
        )?;
        // The lower-bound equality is the physical antimeridian.  Preserve a
        // positive written lift as +180 by selecting the preceding sheet;
        // otherwise -180 stays on its written sheet.  This is identity, not
        // an epsilon seam preference.
        if selection_longitude.ordering(&lower)? == Ordering::Equal
            && selection_longitude.numerator.ordering() == Ordering::Greater
        {
            longitude_wrap = longitude_wrap
                .checked_sub(1)
                .ok_or(ExactFailure::Arithmetic)?;
            lower = ExactRatio::from_parts(
                exact_affine_level(-180, 360, longitude_wrap)?,
                ExactExpansion::from_i64(1)?,
            )?;
        }
        let upper = ExactRatio::from_parts(
            exact_affine_level(180, 360, longitude_wrap)?,
            ExactExpansion::from_i64(1)?,
        )?;
        if selection_longitude.ordering(&lower)? == Ordering::Less
            || selection_longitude.ordering(&upper)? == Ordering::Greater
        {
            return Ok(None);
        }
        let interval = ParentInterval { start, end };
        let selection = [
            self.selection_planar_point(
                &interval.start,
                zone,
                longitude_wrap,
                start_point,
                end_point,
            )?,
            self.selection_planar_point(
                &interval.end,
                zone,
                longitude_wrap,
                start_point,
                end_point,
            )?,
        ];
        let arc = self.selection_arc(&interval, zone, longitude_wrap, start_point, end_point)?;
        Ok(Some(SelectionAffinePiece {
            carrier: AffinePieceCarrier {
                edge: self.key,
                polygon: self.polygon,
                arc,
            },
            interval,
            latitude_zone: zone,
            selection,
        }))
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
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<ExactSelectionPoint> {
        let longitude = self.coordinate_at(
            &parameter.ratio,
            start_point.longitude(),
            end_point.longitude(),
        )?;
        let latitude = self.coordinate_at(
            &parameter.ratio,
            start_point.latitude(),
            end_point.latitude(),
        )?;
        let longitude_offset = 180_i64
            .checked_mul(latitude_zone)
            .ok_or(ExactFailure::Arithmetic)?
            .checked_sub(
                360_i64
                    .checked_mul(longitude_wrap)
                    .ok_or(ExactFailure::Arithmetic)?,
            )
            .ok_or(ExactFailure::Arithmetic)?;
        let unwrapped_longitude = longitude.add(&ExactRatio::from_i64(
            longitude_offset
                .checked_add(
                    360_i64
                        .checked_mul(longitude_wrap)
                        .ok_or(ExactFailure::Arithmetic)?,
                )
                .ok_or(ExactFailure::Arithmetic)?,
            1,
        )?)?;
        let mut longitude = unwrapped_longitude.sub(&ExactRatio::from_i64(
            360_i64
                .checked_mul(longitude_wrap)
                .ok_or(ExactFailure::Arithmetic)?,
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
        let latitude_offset = ExactRatio::from_i64(
            180_i64
                .checked_mul(latitude_zone)
                .ok_or(ExactFailure::Arithmetic)?,
            1,
        )?;
        let latitude = if latitude_zone.rem_euclid(2) == 0 {
            latitude.sub(&latitude_offset)?
        } else {
            latitude_offset.sub(&latitude)?
        };
        Ok(ExactSelectionPoint {
            longitude,
            latitude,
        })
    }

    fn selection_pieces(&self, output: &mut Vec<SelectionAffinePiece>) -> ExactResult<()> {
        let start_point = ExactPlanarPoint::from_lifted(self.start)?;
        let end_point = ExactPlanarPoint::from_lifted(self.end)?;
        for parameters in self.parameters.windows(2) {
            let Some(piece) = self.selection_piece(
                parameters[0].try_clone()?,
                parameters[1].try_clone()?,
                &start_point,
                &end_point,
            )?
            else {
                return Err(ExactFailure::Arithmetic);
            };
            exact_allocation(crate::try_push_vec(output, piece))?;
        }
        Ok(())
    }

    /// Build the source-authority arc only on an exact parent interval whose
    /// midpoint is strictly inside the physical latitude strip.  A strip cut
    /// remains a partition identity, never a pole endpoint manufactured from
    /// an outward enclosure.
    fn authority_pieces(&self, output: &mut Vec<AuthorityAffinePiece>) -> ExactResult<()> {
        let start_point = ExactPlanarPoint::from_lifted(self.start)?;
        let end_point = ExactPlanarPoint::from_lifted(self.end)?;
        for parameters in self.parameters.windows(2) {
            let start = parameters[0].try_clone()?;
            let end = parameters[1].try_clone()?;
            let midpoint = self.midpoint(&start.ratio, &end.ratio)?;
            let latitude =
                self.coordinate_at(&midpoint, start_point.latitude(), end_point.latitude())?;
            let south = ExactRatio::from_i64(-90, 1)?;
            let north = ExactRatio::from_i64(90, 1)?;
            if latitude.ordering(&south)? != Ordering::Greater
                || latitude.ordering(&north)? != Ordering::Less
            {
                continue;
            }
            let piece = AuthorityAffinePiece {
                carrier: AffinePieceCarrier {
                    edge: self.key,
                    polygon: self.polygon,
                    arc: self.authority_arc(
                        &ParentInterval { start, end },
                        &start_point,
                        &end_point,
                    )?,
                },
            };
            exact_allocation(crate::try_push_vec(output, piece))?;
        }
        Ok(())
    }

    fn authority_arc(
        &self,
        interval: &ParentInterval,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<AffineSourceArc> {
        let longitude0 = start_point.longitude();
        let longitude1 = end_point.longitude();
        let latitude0 = start_point.latitude();
        let latitude1 = end_point.latitude();
        let longitude_delta = longitude1.sub(longitude0)?;
        let latitude_delta = latitude1.sub(latitude0)?;
        let structure = affine_structure(longitude0, &longitude_delta, latitude0, &latitude_delta)?;
        let map = AffineParentMap {
            lambda0: degree_expansion_bound(longitude0)
                .and_then(|bound| exact(degree_bound_to_radians(bound)))?,
            phi0: degree_expansion_bound(latitude0)
                .and_then(|bound| exact(degree_bound_to_radians(bound)))?,
            dlambda: degree_expansion_bound(&longitude_delta)
                .and_then(|bound| exact(degree_bound_to_radians(bound)))?,
            dphi: degree_expansion_bound(&latitude_delta)
                .and_then(|bound| exact(degree_bound_to_radians(bound)))?,
            structure,
        };
        let endpoint_roles = [
            endpoint_role(interval.start.key),
            endpoint_role(interval.end.key),
        ];
        let poles = [
            self.authority_endpoint_pole(&interval.start, start_point, end_point)?,
            self.authority_endpoint_pole(&interval.end, start_point, end_point)?,
        ];
        let identities = [
            self.authority_endpoint_identity(&interval.start, start_point, end_point)?,
            self.authority_endpoint_identity(&interval.end, start_point, end_point)?,
        ];
        exact(AffineSourceArc::from_parent_map(
            &map,
            exact(Bound::new(
                interval.start.enclosure.lo,
                interval.end.enclosure.hi,
            ))?,
            endpoint_roles,
            poles,
            identities,
        ))
    }

    fn authority_endpoint_identity(
        &self,
        parameter: &ExactParentParameter,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<AffineEndpointIdentity> {
        let longitude = self.coordinate_at(
            &parameter.ratio,
            start_point.longitude(),
            end_point.longitude(),
        )?;
        let latitude = self.coordinate_at(
            &parameter.ratio,
            start_point.latitude(),
            end_point.latitude(),
        )?;
        endpoint_identity(&longitude, &latitude)
    }

    fn selection_endpoint_identity(
        &self,
        parameter: &ExactParentParameter,
        latitude_zone: i64,
        longitude_wrap: i64,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<AffineEndpointIdentity> {
        let longitude = self.coordinate_at(
            &parameter.ratio,
            start_point.longitude(),
            end_point.longitude(),
        )?;
        let offset = 180_i64
            .checked_mul(latitude_zone)
            .ok_or(ExactFailure::Arithmetic)?
            .checked_sub(
                360_i64
                    .checked_mul(longitude_wrap)
                    .ok_or(ExactFailure::Arithmetic)?,
            )
            .ok_or(ExactFailure::Arithmetic)?;
        let longitude = longitude.add(&ExactRatio::from_i64(offset, 1)?)?;
        let latitude = self.coordinate_at(
            &parameter.ratio,
            start_point.latitude(),
            end_point.latitude(),
        )?;
        let latitude = selection_latitude_ratio(&latitude, latitude_zone)?;
        endpoint_identity(&longitude, &latitude)
    }

    fn authority_endpoint_pole(
        &self,
        parameter: &ExactParentParameter,
        start_point: &ExactPlanarPoint,
        end_point: &ExactPlanarPoint,
    ) -> ExactResult<Option<PhysicalEndpointKey>> {
        if matches!(parameter.key, ParentParameterKey::Partition(_)) {
            return Ok(None);
        }
        let latitude = self.coordinate_at(
            &parameter.ratio,
            start_point.latitude(),
            end_point.latitude(),
        )?;
        let north = ExactRatio::from_i64(90, 1)?;
        let south = ExactRatio::from_i64(-90, 1)?;
        if latitude.ordering(&north)? == Ordering::Equal {
            Ok(Some(PhysicalEndpointKey::NorthPole))
        } else if latitude.ordering(&south)? == Ordering::Equal {
            Ok(Some(PhysicalEndpointKey::SouthPole))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthorityAffinePiece {
    carrier: AffinePieceCarrier,
}

impl AuthorityAffinePiece {
    pub(crate) const fn is_polygon(&self) -> bool {
        self.carrier.polygon
    }

    pub(crate) const fn arc(&self) -> AffineSourceArc {
        self.carrier.arc
    }
}

impl SelectionAffinePiece {
    pub(crate) const fn is_polygon(&self) -> bool {
        self.carrier.polygon
    }

    pub(crate) const fn arc(&self) -> AffineSourceArc {
        self.carrier.arc
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

fn degree_expansion_bound(value: &ExactExpansion) -> ExactResult<Bound> {
    ExactRatio::from_expansion(value)?.certified_bound()
}

/// Choose the nearby longitude sheet used only by the proxy broad phase.
/// Exact source coordinates can be unwrapped, so the sheet is derived from an
/// outward endpoint envelope rather than from either raw endpoint double.
fn nearby_longitude_sheet(value: Bound) -> Option<i64> {
    let proposal = (f64::midpoint(value.lo, value.hi) / FULL_TURN_DEGREES).round();
    if !proposal.is_finite() || proposal < i64::MIN as f64 || proposal > i64::MAX as f64 {
        return None;
    }
    Some(proposal as i64)
}

/// Shift an outward degree bound by an exact number of full longitude turns.
/// Rebuilding both endpoints through the expansion carrier keeps the proxy
/// conservative even when the integer turn is not exactly representable as a
/// binary64 value.
fn shifted_degree_bound(value: Bound, turns: i64) -> ExactResult<Bound> {
    let offset = ExactExpansion::exact_degree_turns(turns)?;
    let lower = degree_expansion_bound(&ExactExpansion::from_f64(value.lo)?.add(&offset)?)?;
    let upper = degree_expansion_bound(&ExactExpansion::from_f64(value.hi)?.add(&offset)?)?;
    exact(Bound::new(lower.lo, upper.hi))
}

/// The proxy coordinate for a source edge is normalized to its nearby sheet,
/// then copied onto the closed preceding, current, and following sheets. Any
/// source edge that cannot produce all three finite envelopes is left to the
/// source-wide exhaustive path.
fn authority_edge_proxy_envelopes(edge: &SymbolicAffineEdge) -> ExactResult<[AABB<[f64; 2]>; 3]> {
    let start = ExactPlanarPoint::from_lifted(edge.start)?;
    let end = ExactPlanarPoint::from_lifted(edge.end)?;
    let start_longitude = degree_expansion_bound(&start.longitude)?;
    let end_longitude = degree_expansion_bound(&end.longitude)?;
    let longitude = exact(Bound::new(
        start_longitude.lo.min(end_longitude.lo),
        start_longitude.hi.max(end_longitude.hi),
    ))?;
    let start_latitude = degree_expansion_bound(&start.latitude)?;
    let end_latitude = degree_expansion_bound(&end.latitude)?;
    let latitude = exact(Bound::new(
        start_latitude.lo.min(end_latitude.lo),
        start_latitude.hi.max(end_latitude.hi),
    ))?;
    let sheet = exact(nearby_longitude_sheet(longitude))?;
    let canonical_sheet = sheet.checked_neg().ok_or(ExactFailure::Arithmetic)?;
    let preceding = shifted_degree_bound(
        longitude,
        canonical_sheet
            .checked_sub(1)
            .ok_or(ExactFailure::Arithmetic)?,
    )?;
    let current = shifted_degree_bound(longitude, canonical_sheet)?;
    let following = shifted_degree_bound(
        longitude,
        canonical_sheet
            .checked_add(1)
            .ok_or(ExactFailure::Arithmetic)?,
    )?;
    Ok([
        AABB::from_corners([preceding.lo, latitude.lo], [preceding.hi, latitude.hi]),
        AABB::from_corners([current.lo, latitude.lo], [current.hi, latitude.hi]),
        AABB::from_corners([following.lo, latitude.lo], [following.hi, latitude.hi]),
    ])
}

/// Normalize a query window into the same nearby sheet used by the proxy
/// envelopes. The exact authority refinement still receives the original
/// window, including its original longitude alias.
fn canonical_proxy_window(longitude: Bound) -> ExactResult<Option<Bound>> {
    let Some(sheet) = nearby_longitude_sheet(longitude) else {
        return Ok(None);
    };
    let sheet = sheet.checked_neg().ok_or(ExactFailure::Arithmetic)?;
    match shifted_degree_bound(longitude, sheet) {
        Ok(value) => Ok(Some(value)),
        Err(ExactFailure::Arithmetic) => Ok(None),
        Err(ExactFailure::Allocation) => Err(ExactFailure::Allocation),
    }
}

const fn endpoint_role(key: ParentParameterKey) -> SourceEndpointRole {
    match key {
        ParentParameterKey::Start => SourceEndpointRole::ParentStart,
        ParentParameterKey::Partition(_) => SourceEndpointRole::Partition,
        ParentParameterKey::End => SourceEndpointRole::ParentEnd,
    }
}

fn selection_latitude_ratio(latitude: &ExactRatio, zone: i64) -> ExactResult<ExactRatio> {
    let offset =
        ExactExpansion::from_i64(180_i64.checked_mul(zone).ok_or(ExactFailure::Arithmetic)?)?;
    let offset = offset.product(&latitude.denominator)?;
    let numerator = if zone.rem_euclid(2) == 0 {
        latitude.numerator.sub(&offset)?
    } else {
        offset.sub(&latitude.numerator)?
    };
    ExactRatio::from_numerator(numerator, &latitude.denominator)
}

fn affine_structure(
    longitude0: &ExactExpansion,
    longitude_delta: &ExactExpansion,
    latitude0: &ExactExpansion,
    latitude_delta: &ExactExpansion,
) -> ExactResult<AffineStructure> {
    if longitude_delta.ordering() == Ordering::Equal && latitude_delta.ordering() == Ordering::Equal
    {
        return Ok(AffineStructure::Point);
    }
    if longitude_delta.ordering() == Ordering::Equal {
        return Ok(AffineStructure::ConstantLongitude {
            axis: structural_axis(longitude0)?,
        });
    }
    if latitude0.ordering() == Ordering::Equal && latitude_delta.ordering() == Ordering::Equal {
        return Ok(AffineStructure::Equator);
    }
    Ok(AffineStructure::General)
}

fn structural_axis(longitude: &ExactExpansion) -> ExactResult<Option<AxisMeridian>> {
    let proposal = exact(longitude.checked_estimate())?;
    for (axis, base) in [
        (AxisMeridian::Prime, 0_i64),
        (AxisMeridian::EastQuarter, 90_i64),
        (AxisMeridian::Antimeridian, 180_i64),
        (AxisMeridian::WestQuarter, -90_i64),
    ] {
        let turns = ((proposal - base as f64) / FULL_TURN_DEGREES).round();
        if !turns.is_finite() || turns < i64::MIN as f64 || turns > i64::MAX as f64 {
            return Err(ExactFailure::Arithmetic);
        }
        let base = ExactExpansion::from_i64(base)?;
        let offset = ExactExpansion::exact_degree_turns(turns as i64)?;
        let candidate = base.add(&offset)?;
        if longitude.sub(&candidate)?.ordering() == Ordering::Equal {
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

fn structural_axis_ratio(longitude: &ExactRatio) -> ExactResult<StructuralAxisRatio> {
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
            return Err(ExactFailure::Arithmetic);
        }
        let base = ExactExpansion::from_i64(base)?;
        let offset = ExactExpansion::exact_degree_turns(turns as i64)?;
        let candidate = base.add(&offset)?;
        if longitude.ordering(&ExactRatio::from_expansion(&candidate)?)? == Ordering::Equal {
            return Ok(StructuralAxisRatio::Axis(axis));
        }
    }
    Ok(StructuralAxisRatio::None)
}

fn endpoint_identity(
    longitude: &ExactRatio,
    latitude: &ExactRatio,
) -> ExactResult<AffineEndpointIdentity> {
    let equator = ExactRatio::from_i64(0, 1)?;
    Ok(AffineEndpointIdentity {
        longitude: match structural_axis_ratio(longitude)? {
            StructuralAxisRatio::None => None,
            StructuralAxisRatio::Axis(axis) => Some(axis),
        },
        latitude: (latitude.ordering(&equator)? == Ordering::Equal)
            .then_some(AxisLatitude::Equator),
    })
}

fn exact_affine_level(base: i64, stride: i64, level: i64) -> ExactResult<ExactExpansion> {
    let offset = stride
        .checked_mul(level)
        .ok_or(ExactFailure::Arithmetic)?
        .checked_add(base)
        .ok_or(ExactFailure::Arithmetic)?;
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
) -> ExactResult<std::ops::RangeInclusive<i64>> {
    if stride <= 0 {
        return Err(ExactFailure::Arithmetic);
    }
    let (lower, upper) = if start.ordering() == Ordering::Greater {
        (end, start)
    } else {
        (start, end)
    };
    let lower = exact_floor_level(lower, base, stride)?;
    let mut upper_floor = exact_floor_level(upper, base, stride)?;
    if exact_affine_level(base, stride, upper_floor)?
        .sub(upper)?
        .ordering()
        == Ordering::Less
    {
        upper_floor = upper_floor.checked_add(1).ok_or(ExactFailure::Arithmetic)?;
    }
    Ok(lower..=upper_floor)
}

fn exact_floor_level(value: &ExactExpansion, base: i64, stride: i64) -> ExactResult<i64> {
    let proposal = ((exact(value.checked_estimate())? - base as f64) / stride as f64).floor();
    if !proposal.is_finite() || proposal < i64::MIN as f64 || proposal > i64::MAX as f64 {
        return Err(ExactFailure::Arithmetic);
    }
    let mut level = proposal as i64;
    loop {
        let next = level.checked_add(1).ok_or(ExactFailure::Arithmetic)?;
        if exact_affine_level(base, stride, next)?
            .sub(value)?
            .ordering()
            == Ordering::Greater
        {
            break;
        }
        level = next;
    }
    loop {
        if exact_affine_level(base, stride, level)?
            .sub(value)?
            .ordering()
            != Ordering::Greater
        {
            break;
        }
        level = level.checked_sub(1).ok_or(ExactFailure::Arithmetic)?;
    }
    Ok(level)
}

const fn same_physical_latitude(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits()
}

fn can_coalesce(
    previous: LiftedVertex,
    current: LiftedVertex,
    next: LiftedVertex,
) -> ExactResult<bool> {
    let ab_x = current.longitude.difference(previous.longitude)?;
    let current_y = ExactExpansion::from_f64(current.latitude)?;
    let previous_y = ExactExpansion::from_f64(previous.latitude)?;
    let ab_y = current_y.sub(&previous_y)?;
    let bc_x = next.longitude.difference(current.longitude)?;
    let next_y = ExactExpansion::from_f64(next.latitude)?;
    let bc_y = next_y.sub(&current_y)?;
    let left = ab_x.product(&bc_y)?;
    let right = ab_y.product(&bc_x)?;
    let determinant = left.sub(&right)?;
    if determinant.ordering() != Ordering::Equal
        || !closed_between(previous.longitude, current.longitude, next.longitude)?
        || !closed_between_raw(previous.latitude, current.latitude, next.latitude)?
    {
        return Ok(false);
    }
    let direct = lifted_step(previous.longitude.raw, next.longitude.raw)?;
    let direct_turn = previous
        .longitude
        .turns
        .checked_add(direct.turn_delta)
        .ok_or(ExactFailure::Arithmetic)?;
    // A long path through a collinear point may not be replaced by its short
    // direct periodic lift.  This is distinct from geometric collinearity.
    Ok(direct_turn == next.longitude.turns)
}

fn closed_between(
    left: LiftedLongitude,
    middle: LiftedLongitude,
    right: LiftedLongitude,
) -> ExactResult<bool> {
    let left_middle = left.ordering(middle)?;
    let middle_right = middle.ordering(right)?;
    Ok(closed_between_orderings(left_middle, middle_right))
}

fn closed_between_raw(left: f64, middle: f64, right: f64) -> ExactResult<bool> {
    closed_between_expansion(
        &ExactExpansion::from_f64(left)?,
        &ExactExpansion::from_f64(middle)?,
        &ExactExpansion::from_f64(right)?,
    )
}

/// A stored source point, retaining the exact binary64 inputs instead of a
/// rounded selection-image coordinate.  Points are zero-dimensional source
/// features, not degenerate source edges: later containment owns them
/// directly and no artificial `AffineSourceArc` is introduced here.
#[derive(Debug)]
struct SourcePoint {
    key: SourceVertexKey,
    longitude: ExactExpansion,
    latitude: ExactExpansion,
}

/// The unit-sphere selection image of a source point.  Reflection and wrap
/// are exact expansion transforms of the stored doubles; the binary64
/// proposal in `exact_floor_level` never decides a strip or seam.
#[derive(Debug)]
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

/// One exact point and its conservative degree witness. The type parameter
/// keeps authority and selection point semantics distinct while making the
/// point/bounds relationship a single owned value.
#[derive(Debug)]
struct GridPointRecord<T> {
    point: T,
    bounds: Option<GridDegreePoint>,
}

type AuthorityPointRecord = GridPointRecord<SourcePoint>;
type SelectionPointRecord = GridPointRecord<SelectionPoint>;

/// Authority and selection point records for one source occurrence. A
/// selection transform can be arithmetically uncertain while the retained
/// authority point remains exact, so the selection side is explicitly
/// optional rather than silently shortening a parallel vector.
#[derive(Debug)]
struct SourcePointRecord {
    authority: AuthorityPointRecord,
    selection: Option<SelectionPointRecord>,
}

#[derive(Debug)]
struct AuthorityCarrier {
    edges: Vec<SymbolicAffineEdge>,
    edge_index: Option<AuthorityEdgeIndex>,
    pieces: Vec<AuthorityAffinePiece>,
}

#[derive(Debug)]
struct SelectionCarrier {
    edges: Vec<SelectionAffinePiece>,
    poles: Vec<GridDegreePoint>,
}

fn selection_image_point(point: &SourcePoint) -> ExactResult<SelectionPoint> {
    // `zone` is floor((latitude + 90) / 180), found by an exact sign walk.
    // This is the shared spherical selection image for H3 and S2: reflect
    // through a pole and then wrap longitude.  Clamping would change the
    // source geometry and cannot be a negative certificate.
    let zone = exact_floor_level(&point.latitude, -90, 180)?;
    let latitude_offset = exact_affine_level(0, 180, zone)?;
    let latitude = if zone.rem_euclid(2) == 0 {
        point.latitude.sub(&latitude_offset)?
    } else {
        latitude_offset.sub(&point.latitude)?
    };
    let longitude_offset = exact_affine_level(0, 180, zone)?;
    let lifted_longitude = point.longitude.add(&longitude_offset)?;
    let wrap = exact_floor_level(&lifted_longitude, -180, 360)?;
    let wrap_offset = exact_affine_level(0, 360, wrap)?;
    let mut longitude = lifted_longitude.sub(&wrap_offset)?;
    // The two stored physical antimeridian doubles retain their written side;
    // this is exact identity, not a tolerance-based seam selection.
    if longitude.is_exact(-HALF_TURN_DEGREES)? && lifted_longitude.ordering() == Ordering::Greater {
        longitude = ExactExpansion::from_f64(HALF_TURN_DEGREES)?;
    }
    Ok(SelectionPoint {
        key: point.key,
        longitude,
        latitude,
    })
}

const PACKED_BVH_LEAF_SIZE: usize = 8;

#[derive(Debug)]
pub(crate) struct PackedBvhEntry<T> {
    value: T,
    envelope: AABB<[f64; 2]>,
    order: usize,
}

impl<T> PackedBvhEntry<T> {
    pub(crate) const fn new(value: T, envelope: AABB<[f64; 2]>) -> Self {
        Self {
            value,
            envelope,
            order: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PackedBvhNode {
    envelope: AABB<[f64; 2]>,
    start: usize,
    end: usize,
    left: Option<usize>,
    right: Option<usize>,
}

#[derive(Debug)]
pub(crate) struct PackedBvh<T> {
    entries: Vec<PackedBvhEntry<T>>,
    nodes: Vec<PackedBvhNode>,
    root: Option<usize>,
}

impl<T> PackedBvh<T> {
    pub(crate) fn build(mut entries: Vec<PackedBvhEntry<T>>) -> crate::error::Result<Self> {
        let entry_count = entries.len();
        let leaf_count = entry_count.div_ceil(PACKED_BVH_LEAF_SIZE);
        let node_capacity = if entry_count == 0 {
            0
        } else {
            leaf_count
                .checked_mul(2)
                .and_then(|count| count.checked_sub(1))
                .ok_or(crate::error::ResourceError::SizeOverflow)?
        };
        let mut nodes = Vec::new();
        crate::try_reserve_vec(&mut nodes, node_capacity)?;
        for (order, entry) in entries.iter_mut().enumerate() {
            entry.order = order;
        }
        let root = if entry_count == 0 {
            None
        } else {
            Some(Self::build_node(&mut entries, &mut nodes, 0, entry_count)?)
        };
        Ok(Self {
            entries,
            nodes,
            root,
        })
    }

    fn build_node(
        entries: &mut [PackedBvhEntry<T>],
        nodes: &mut Vec<PackedBvhNode>,
        start: usize,
        end: usize,
    ) -> crate::error::Result<usize> {
        let envelope = envelope_for_entries(&entries[start..end]);
        let node = nodes.len();
        crate::try_push_vec(nodes, PackedBvhNode {
            envelope,
            start,
            end,
            left: None,
            right: None,
        })?;
        if end - start > PACKED_BVH_LEAF_SIZE {
            let axis = longest_axis(envelope);
            entries[start..end].sort_unstable_by(|left, right| {
                proxy_center(left.envelope, axis)
                    .total_cmp(&proxy_center(right.envelope, axis))
                    .then_with(|| compare_envelopes(left.envelope, right.envelope))
                    .then_with(|| left.order.cmp(&right.order))
            });
            let middle = start + (end - start) / 2;
            let left = Self::build_node(entries, nodes, start, middle)?;
            let right = Self::build_node(entries, nodes, middle, end)?;
            nodes[node].left = Some(left);
            nodes[node].right = Some(right);
        }
        Ok(node)
    }

    pub(crate) fn visit(
        &self,
        query: AABB<[f64; 2]>,
        visit: &mut impl FnMut(&T) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        let Some(root) = self.root else {
            return Ok(());
        };
        self.visit_node(root, query, visit)
    }

    fn visit_node(
        &self,
        node: usize,
        query: AABB<[f64; 2]>,
        visit: &mut impl FnMut(&T) -> crate::error::Result<()>,
    ) -> crate::error::Result<()> {
        let current = self
            .nodes
            .get(node)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if !aabbs_intersect(current.envelope, query) {
            return Ok(());
        }
        match (current.left, current.right) {
            (Some(left), Some(right)) => {
                self.visit_node(left, query, visit)?;
                self.visit_node(right, query, visit)?;
            },
            (None, None) => {
                for entry in &self.entries[current.start..current.end] {
                    if aabbs_intersect(entry.envelope, query) {
                        visit(&entry.value)?;
                    }
                }
            },
            _ => return Err(crate::error::ResourceError::SizeOverflow.into()),
        }
        Ok(())
    }
}

type AuthorityEdgeIndex = PackedBvh<usize>;

fn build_authority_edge_index(
    edges: &[SymbolicAffineEdge],
) -> crate::error::Result<Option<AuthorityEdgeIndex>> {
    let entry_capacity = edges
        .len()
        .checked_mul(3)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut entries = Vec::new();
    crate::try_reserve_vec(&mut entries, entry_capacity)?;
    for (edge, authority_edge) in edges.iter().enumerate() {
        let envelopes = match authority_edge_proxy_envelopes(authority_edge) {
            Ok(envelopes) => envelopes,
            Err(ExactFailure::Arithmetic) => return Ok(None),
            Err(ExactFailure::Allocation) => {
                return Err(crate::error::ResourceError::Allocation.into());
            },
        };
        for envelope in envelopes {
            crate::try_push_vec(&mut entries, PackedBvhEntry::new(edge, envelope))?;
        }
    }
    Ok(Some(PackedBvh::build(entries)?))
}

fn envelope_for_entries<T>(entries: &[PackedBvhEntry<T>]) -> AABB<[f64; 2]> {
    let first = entries[0].envelope;
    let mut lower = first.lower();
    let mut upper = first.upper();
    for entry in &entries[1..] {
        let entry_lower = entry.envelope.lower();
        let entry_upper = entry.envelope.upper();
        lower[0] = lower[0].min(entry_lower[0]);
        lower[1] = lower[1].min(entry_lower[1]);
        upper[0] = upper[0].max(entry_upper[0]);
        upper[1] = upper[1].max(entry_upper[1]);
    }
    AABB::from_bounds(lower, upper)
}

fn longest_axis(envelope: AABB<[f64; 2]>) -> usize {
    let lower = envelope.lower();
    let upper = envelope.upper();
    let x = upper[0] - lower[0];
    let y = upper[1] - lower[1];
    usize::from(y > x)
}

fn proxy_center(envelope: AABB<[f64; 2]>, axis: usize) -> f64 {
    let lower = envelope.lower();
    let upper = envelope.upper();
    f64::midpoint(lower[axis], upper[axis])
}

fn compare_envelopes(left: AABB<[f64; 2]>, right: AABB<[f64; 2]>) -> Ordering {
    let left_lower = left.lower();
    let left_upper = left.upper();
    let right_lower = right.lower();
    let right_upper = right.upper();
    left_lower[0]
        .total_cmp(&right_lower[0])
        .then_with(|| left_lower[1].total_cmp(&right_lower[1]))
        .then_with(|| left_upper[0].total_cmp(&right_upper[0]))
        .then_with(|| left_upper[1].total_cmp(&right_upper[1]))
}

fn aabbs_intersect(left: AABB<[f64; 2]>, right: AABB<[f64; 2]>) -> bool {
    let left_lower = left.lower();
    let left_upper = left.upper();
    let right_lower = right.lower();
    let right_upper = right.upper();
    left_lower[0] <= right_upper[0]
        && right_lower[0] <= left_upper[0]
        && left_lower[1] <= right_upper[1]
        && right_lower[1] <= left_upper[1]
}

/// Shared periodic source lift and conservative candidate authority.
#[derive(Debug)]
pub(crate) struct GeographicCoverageSource {
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
    chains: Vec<LiftedChain>,
    ring_fills: Vec<LiftedRingFill>,
    points: Vec<SourcePointRecord>,
    // R15-S authority-identity disposition: selection differs from authority
    // only for a nonzero latitude reflection zone (`is_positive_only` below).
    // Public H3/S2 cover ingress rejects every such source before this carrier
    // is constructed, so the draft incident-owner fixture was unreachable and
    // its deferred mutation was deleted rather than left as a vacuous claim.
    // The two images stay structurally separate for private fail-open paths.
    authority: AuthorityCarrier,
    selection: SelectionCarrier,
    unknown: bool,
}

impl GeographicCoverageSource {
    pub(crate) fn from_validated(
        source: &Shape,
        target: SphericalGridTarget,
    ) -> crate::error::Result<Self> {
        Self::build(source, target)
    }

    #[cfg(test)]
    pub(crate) fn new(source: &Shape, target: SphericalGridTarget) -> crate::error::Result<Self> {
        // Geographic admission accepts a minute exterior rounding sliver at
        // either pole.  Canonicalize the complete shape before any carrier
        // state is derived: bounds, rectangle recognition, chains, and point
        // witnesses must agree on the one physical-pole topology.
        let normalized_source = normalize_grid_source(source);
        Self::build(&normalized_source, target)
    }

    fn build(source: &Shape, target: SphericalGridTarget) -> crate::error::Result<Self> {
        let exact_axis_rect = if let Some(bounds) = exact_world_band(source)? {
            Some(bounds)
        } else if let Some(bounds) = source.bounds() {
            let is_rectangle = match exact_axis_rectangle(source, bounds) {
                Ok(value) => value,
                Err(ExactFailure::Arithmetic) => false,
                Err(ExactFailure::Allocation) => {
                    return Err(crate::error::ResourceError::Allocation.into());
                },
            };
            if !is_rectangle
                || !(-180.0..=180.0).contains(&bounds.minx())
                || !(-180.0..=180.0).contains(&bounds.maxx())
                || !(-90.0..=90.0).contains(&bounds.miny())
                || !(-90.0..=90.0).contains(&bounds.maxy())
            {
                None
            } else {
                match canonical_axis_rectangle_longitude(bounds) {
                    Ok(true) => Some(bounds),
                    Ok(false) | Err(ExactFailure::Arithmetic) => None,
                    Err(ExactFailure::Allocation) => {
                        return Err(crate::error::ResourceError::Allocation.into());
                    },
                }
            }
        } else {
            None
        };
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
        let mut point_records = Vec::new();
        let mut authority = AuthorityCarrier {
            edges: Vec::new(),
            edge_index: None,
            pieces: Vec::new(),
        };
        let mut selection = SelectionCarrier {
            edges: Vec::new(),
            poles: Vec::new(),
        };
        crate::try_reserve_vec(&mut lifted, chains.len())?;
        crate::try_reserve_vec(&mut ring_fills, chains.len())?;
        crate::try_reserve_vec(&mut point_records, points.len())?;
        crate::try_reserve_vec(&mut authority.pieces, chains.len())?;
        for input in &chains {
            append_lifted_chain(
                input,
                &mut lifted,
                &mut ring_fills,
                &mut authority,
                &mut selection,
                &mut unknown,
            )?;
        }
        for input in points {
            let longitude = match ExactExpansion::from_f64(input.longitude) {
                Ok(value) => value,
                Err(ExactFailure::Arithmetic) => {
                    unknown = true;
                    continue;
                },
                Err(ExactFailure::Allocation) => {
                    return Err(crate::error::ResourceError::Allocation.into());
                },
            };
            let latitude = match ExactExpansion::from_f64(input.latitude) {
                Ok(value) => value,
                Err(ExactFailure::Arithmetic) => {
                    unknown = true;
                    continue;
                },
                Err(ExactFailure::Allocation) => {
                    return Err(crate::error::ResourceError::Allocation.into());
                },
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
            let authority_bound =
                match GridDegreePoint::try_from_expansions(&point.longitude, &point.latitude) {
                    Ok(bound) => Some(bound),
                    Err(ExactFailure::Arithmetic) => None,
                    Err(ExactFailure::Allocation) => {
                        return Err(crate::error::ResourceError::Allocation.into());
                    },
                };
            let selection = append_source_point(&point, &mut unknown)?;
            crate::try_push_vec(&mut point_records, SourcePointRecord {
                authority: GridPointRecord {
                    point,
                    bounds: authority_bound,
                },
                selection,
            })?;
        }
        let selection_point_count = point_records
            .iter()
            .filter(|record| record.selection.is_some())
            .count();
        let pole_capacity = selection
            .edges
            .len()
            .checked_mul(2)
            .and_then(|count| count.checked_add(selection_point_count))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        crate::try_reserve_vec(&mut selection.poles, pole_capacity)?;
        for record in &point_records {
            if let Some(point) = &record.selection {
                append_selection_pole_witness(point, &mut selection.poles, &mut unknown)?;
            }
        }
        for piece in &selection.edges {
            for point in &piece.selection {
                append_selection_planar_pole_witness(point, &mut selection.poles, &mut unknown)?;
            }
        }
        authority.edge_index = build_authority_edge_index(&authority.edges)?;
        Ok(Self {
            target,
            exact_axis_rect,
            authority_latitude,
            chains: lifted,
            ring_fills,
            points: point_records,
            authority,
            selection,
            unknown,
        })
    }

    pub(crate) const fn target(&self) -> SphericalGridTarget {
        self.target
    }

    pub(crate) const fn is_unknown(&self) -> bool {
        self.unknown
    }

    /// Admit one rectangular candidate into the source-owned representation.
    ///
    /// Raw `Bounds` retain the caller's conservative rectangle, including a
    /// wrapped longitude spelling. H3's certified windows retain their
    /// canonical one/full/split longitude form and receive a conservative
    /// enclosing `Bounds` for consumers that need one. In both cases the
    /// source's complete axis certificate is carried with the product.
    pub(crate) fn admit_rectangular_candidate(
        &self,
        input: impl Into<RectangularCandidateInput>,
    ) -> RectangularCandidate {
        #[cfg(test)]
        RECTANGULAR_CANDIDATE_ADMISSIONS.with(|count| count.set(count.get() + 1));
        let (bounds, windows) = match input.into() {
            RectangularCandidateInput::Bounds(bounds) => {
                (bounds, degree_windows_from_bounds(bounds))
            },
            RectangularCandidateInput::Windows(windows) => {
                (conservative_bounds_from_windows(windows), windows)
            },
        };
        RectangularCandidate {
            bounds,
            windows,
            exact_axis: self.exact_axis_rect,
        }
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
    pub(crate) fn s2_raw_owner_descends_from(&self, candidate: S2Cell) -> bool {
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
            candidate.contains_point(s2_lonlat_to_point(longitude, latitude))
        };
        self.unknown
            || self.points.iter().any(|record| {
                record
                    .authority
                    .point
                    .longitude
                    .checked_estimate()
                    .zip(record.authority.point.latitude.checked_estimate())
                    .is_none_or(|(longitude, latitude)| contains_raw(longitude, latitude))
            })
            || self.authority.edges.iter().any(|edge| {
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
    #[cfg(test)]
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
    pub(crate) fn classify_rect(
        &self,
        candidate: &RectangularCandidate,
    ) -> crate::error::Result<RectClass> {
        // This certificate models the whole source exactly.  It must run
        // before the general lift's global uncertainty veto: that veto is a
        // fail-open fallback for symbolic sources, not a reason to discard a
        // complete rectangle proof.
        if let Some(bounds) = candidate.exact_axis {
            return Ok(classify_exact_axis_rectangle(bounds, candidate.windows));
        }
        if self.unknown
            || self.has_positive_selection_polygon()
            || self.has_positive_selection_lower()
        {
            return Ok(RectClass::Boundary);
        }
        let DegreeWindowResult::Windows(CertifiedDegreeWindows {
            latitude,
            longitude,
        }) = candidate.windows
        else {
            return Ok(RectClass::Boundary);
        };
        // This is a proof over the authoritative *linear* source, not a
        // working-image bound.  Every segment and every zero-degree polygon
        // fill is bounded by its endpoint latitudes.  A nonzero ring degree,
        // however, has a physical pole roof: its raw vertices at (say) lat 60
        // enclose the north cap through lat 90 regardless of longitude
        // direction. Those rings must take the exact periodic PIP below rather
        // than turn a raw-latitude shortcut into an unsound negative.
        if !self.has_periodic_polygon_fill()
            && self
                .authority_latitude
                .is_some_and(|source| strictly_disjoint_degree_bounds(source, latitude))
        {
            return Ok(RectClass::Outside);
        }
        match longitude {
            CertifiedLongitudeDegrees::Full => self.classify_rect_window(
                exact(Bound::new(-180.0, 180.0))
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                latitude,
            ),
            CertifiedLongitudeDegrees::One(longitude) => {
                self.classify_rect_window(longitude, latitude)
            },
            CertifiedLongitudeDegrees::Two([west, east]) => {
                let west = self.classify_rect_window(west, latitude)?;
                let east = self.classify_rect_window(east, latitude)?;
                match (west, east) {
                    (RectClass::Outside, RectClass::Outside) => Ok(RectClass::Outside),
                    (RectClass::Interior, RectClass::Interior) => Ok(RectClass::Interior),
                    _ => Ok(RectClass::Boundary),
                }
            },
        }
    }

    fn classify_rect_window(
        &self,
        longitude: Bound,
        latitude: Bound,
    ) -> crate::error::Result<RectClass> {
        self.classify_rect_window_with_index(longitude, latitude, true)
    }

    #[cfg(test)]
    fn classify_rect_window_exhaustive(&self, longitude: Bound, latitude: Bound) -> RectClass {
        self.classify_rect_window_with_index(longitude, latitude, false)
            .unwrap_or(RectClass::Boundary)
    }

    fn classify_rect_window_with_index(
        &self,
        longitude: Bound,
        latitude: Bound,
        use_index: bool,
    ) -> crate::error::Result<RectClass> {
        for point in self.authority_points() {
            let Some(point) = point else {
                return Ok(RectClass::Boundary);
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
                return Ok(RectClass::Boundary);
            }
        }
        let edge_ordinals = if use_index {
            match self.authority.edge_index.as_ref() {
                Some(index) => match canonical_proxy_window(longitude) {
                    Err(ExactFailure::Arithmetic) | Ok(None) => None,
                    Err(ExactFailure::Allocation) => {
                        return Err(crate::error::ResourceError::Allocation.into());
                    },
                    Ok(Some(longitude)) => {
                        let query = AABB::from_corners([longitude.lo, latitude.lo], [
                            longitude.hi,
                            latitude.hi,
                        ]);
                        let mut ordinals = Vec::new();
                        index
                            .visit(query, &mut |edge| crate::try_push_vec(&mut ordinals, *edge))?;
                        ordinals.sort_unstable();
                        ordinals.dedup();
                        Some(ordinals)
                    },
                },
                None => None,
            }
        } else {
            None
        };
        if let Some(edge_ordinals) = edge_ordinals {
            for ordinal in edge_ordinals {
                let Some(edge) = self.authority.edges.get(ordinal) else {
                    return Ok(RectClass::Boundary);
                };
                match exact_authority_edge_touches_rectangle(edge, longitude, latitude) {
                    Ok(true) | Err(ExactFailure::Arithmetic) => return Ok(RectClass::Boundary),
                    Ok(false) => {},
                    Err(ExactFailure::Allocation) => {
                        return Err(crate::error::ResourceError::Allocation.into());
                    },
                }
            }
        } else {
            for edge in &self.authority.edges {
                match exact_authority_edge_touches_rectangle(edge, longitude, latitude) {
                    Ok(true) | Err(ExactFailure::Arithmetic) => return Ok(RectClass::Boundary),
                    Ok(false) => {},
                    Err(ExactFailure::Allocation) => {
                        return Err(crate::error::ResourceError::Allocation.into());
                    },
                }
            }
        }
        if !self.has_polygon() {
            return Ok(RectClass::Outside);
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
            return Ok(RectClass::Boundary);
        };
        let Some(latitude) = strict_window_representative(latitude) else {
            return Ok(RectClass::Boundary);
        };
        match self.authority_point_class(longitude, latitude)? {
            GridPointClass::Exterior => Ok(RectClass::Outside),
            GridPointClass::Interior => Ok(RectClass::Interior),
            GridPointClass::Boundary | GridPointClass::Unknown => Ok(RectClass::Boundary),
        }
    }

    #[cfg(test)]
    pub(crate) fn selection_arcs(&self) -> Option<impl ExactSizeIterator<Item = &AffineSourceArc>> {
        (!self.unknown).then(|| self.selection.edges.iter().map(|piece| &piece.carrier.arc))
    }

    pub(crate) fn authority_pieces(
        &self,
    ) -> Option<impl ExactSizeIterator<Item = &AuthorityAffinePiece>> {
        (!self.unknown).then(|| self.authority.pieces.iter())
    }

    pub(crate) fn selection_pieces(
        &self,
    ) -> Option<impl ExactSizeIterator<Item = &SelectionAffinePiece>> {
        (!self.unknown).then(|| self.selection.edges.iter())
    }

    pub(crate) const fn has_polygon(&self) -> bool {
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
            && self.selection.edges.iter().any(|piece| {
                piece.carrier.polygon
                    && piece.carrier.edge.component == component
                    && piece.carrier.edge.ring == ring
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
        self.points.iter().map(|record| record.authority.bounds)
    }

    pub(crate) fn positive_selection_points(
        &self,
    ) -> impl Iterator<Item = Option<GridDegreePoint>> + '_ {
        self.points.iter().filter_map(|record| {
            let selection = record.selection.as_ref()?;
            // A 360-degree longitude rewrite is the same unit-sphere
            // point and is already represented by the authority lift's
            // periodic PIP and the ordered arc seam shifts. Reflection
            // changes latitude and is the only point transform that adds
            // positive-only H3 authority.
            (selection.point.latitude != record.authority.point.latitude)
                .then_some(selection.bounds)
        })
    }

    pub(crate) fn has_positive_selection_polygon(&self) -> bool {
        self.selection
            .edges
            .iter()
            .any(|piece| piece.is_polygon() && piece.is_positive_only())
    }

    pub(crate) fn has_positive_selection_lower(&self) -> bool {
        self.selection
            .edges
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
    ) -> crate::error::Result<GridPointClass> {
        if self.unknown
            || !longitude.is_finite()
            || !latitude.is_finite()
            || latitude.to_bits() == 90.0_f64.to_bits()
            || latitude.to_bits() == (-90.0_f64).to_bits()
        {
            return Ok(GridPointClass::Unknown);
        }
        let query = match ExactPlanarPoint::from_stored(longitude, latitude) {
            Ok(query) => query,
            Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
            Err(ExactFailure::Allocation) => {
                return Err(crate::error::ResourceError::Allocation.into());
            },
        };
        let mut aggregate = GridPointClass::Exterior;
        for shell in self.ring_fills.iter().filter(|fill| fill.ring == 0) {
            let component = shell.component;
            if !self.selection.edges.iter().any(|piece| {
                piece.carrier.polygon
                    && piece.carrier.edge.component == component
                    && piece.is_positive_only()
            }) {
                continue;
            }
            let shell_class = match self.selection_ring_point_class(component, 0, &query) {
                Ok(class) => class,
                Err(ExactFailure::Arithmetic) => GridPointClass::Unknown,
                Err(ExactFailure::Allocation) => {
                    return Err(crate::error::ResourceError::Allocation.into());
                },
            };
            let component_class = match shell_class {
                GridPointClass::Exterior => GridPointClass::Exterior,
                GridPointClass::Boundary | GridPointClass::Unknown => shell_class,
                GridPointClass::Interior => {
                    match self.selection_component_hole_class(component, &query) {
                        Ok(class) => class,
                        Err(ExactFailure::Arithmetic) => GridPointClass::Unknown,
                        Err(ExactFailure::Allocation) => {
                            return Err(crate::error::ResourceError::Allocation.into());
                        },
                    }
                },
            };
            aggregate = combine_polygon_point_classes(aggregate, component_class);
        }
        Ok(aggregate)
    }

    /// Every exact selected pole endpoint, including pole crossings
    /// materialized by a rational split.  Longitude is structurally irrelevant
    /// at a pole, so the witness intentionally stores the one canonical zero
    /// longitude rather than choosing an antimeridian spelling.
    pub(crate) fn selection_pole_witnesses(&self) -> impl Iterator<Item = GridDegreePoint> + '_ {
        self.selection.poles.iter().copied()
    }

    /// Exact retained-authority PIP for a non-pole stored degree point.  The
    /// positive selection image is deliberately not consulted here: it can
    /// only veto a later negative, never manufacture this authority result.
    pub(crate) fn authority_point_class(
        &self,
        longitude: f64,
        latitude: f64,
    ) -> crate::error::Result<GridPointClass> {
        if self.unknown
            || !longitude.is_finite()
            || !latitude.is_finite()
            || latitude.to_bits() == 90.0_f64.to_bits()
            || latitude.to_bits() == (-90.0_f64).to_bits()
        {
            return Ok(GridPointClass::Unknown);
        }
        let query = match ExactPlanarPoint::from_stored(longitude, latitude) {
            Ok(query) => query,
            Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
            Err(ExactFailure::Allocation) => {
                return Err(crate::error::ResourceError::Allocation.into());
            },
        };
        let mut aggregate = GridPointClass::Exterior;
        for shell in self.ring_fills.iter().filter(|fill| fill.ring == 0) {
            let Some(chain) = self.chains.get(shell.chain_index) else {
                return Ok(GridPointClass::Unknown);
            };
            let shell_class = match shell.point_class(chain, longitude, &query) {
                Ok(class) => class,
                Err(ExactFailure::Arithmetic) => GridPointClass::Unknown,
                Err(ExactFailure::Allocation) => {
                    return Err(crate::error::ResourceError::Allocation.into());
                },
            };
            let component = match shell_class {
                GridPointClass::Exterior => GridPointClass::Exterior,
                GridPointClass::Boundary | GridPointClass::Unknown => shell_class,
                GridPointClass::Interior => {
                    match self.component_hole_class(shell, longitude, &query) {
                        Ok(class) => class,
                        Err(ExactFailure::Arithmetic) => GridPointClass::Unknown,
                        Err(ExactFailure::Allocation) => {
                            return Err(crate::error::ResourceError::Allocation.into());
                        },
                    }
                },
            };
            aggregate = combine_polygon_point_classes(aggregate, component);
        }
        Ok(aggregate)
    }

    fn component_hole_class(
        &self,
        shell: &LiftedRingFill,
        longitude: f64,
        query: &ExactPlanarPoint,
    ) -> ExactResult<GridPointClass> {
        let mut unknown = false;
        for hole in self
            .ring_fills
            .iter()
            .filter(|fill| fill.component == shell.component && fill.ring != 0)
        {
            let Some(chain) = self.chains.get(hole.chain_index) else {
                return Ok(GridPointClass::Unknown);
            };
            match hole.point_class(chain, longitude, query)? {
                GridPointClass::Boundary => return Ok(GridPointClass::Boundary),
                GridPointClass::Interior => return Ok(GridPointClass::Exterior),
                GridPointClass::Unknown => unknown = true,
                GridPointClass::Exterior => {},
            }
        }
        if unknown {
            Ok(GridPointClass::Unknown)
        } else {
            Ok(GridPointClass::Interior)
        }
    }

    fn selection_component_hole_class(
        &self,
        component: usize,
        query: &ExactPlanarPoint,
    ) -> ExactResult<GridPointClass> {
        let mut unknown = false;
        for hole in self
            .ring_fills
            .iter()
            .filter(|fill| fill.component == component && fill.ring != 0)
        {
            match self.selection_ring_point_class(component, hole.ring, query)? {
                GridPointClass::Boundary => return Ok(GridPointClass::Boundary),
                GridPointClass::Interior => return Ok(GridPointClass::Exterior),
                GridPointClass::Unknown => unknown = true,
                GridPointClass::Exterior => {},
            }
        }
        if unknown {
            Ok(GridPointClass::Unknown)
        } else {
            Ok(GridPointClass::Interior)
        }
    }

    fn selection_ring_point_class(
        &self,
        component: usize,
        ring: usize,
        query: &ExactPlanarPoint,
    ) -> ExactResult<GridPointClass> {
        let edge_count = self
            .selection
            .edges
            .iter()
            .filter(|piece| {
                piece.carrier.polygon
                    && piece.carrier.edge.component == component
                    && piece.carrier.edge.ring == ring
            })
            .count();
        if edge_count == 0 {
            return Ok(GridPointClass::Unknown);
        }
        // Selection pieces are split into the physical [-180, 180] sheet.
        // A physical query has only its immediate -360/0/+360 preimages in
        // that sheet; retaining all three is the exact circular PIP carrier,
        // not a heuristic longitude neighborhood.
        let copies = -1_i64..=1_i64;
        let mut buckets = match PeriodicParityBuckets::new(copies, edge_count) {
            Ok(buckets) => buckets,
            Err(ExactFailure::Arithmetic) => return Ok(GridPointClass::Unknown),
            Err(ExactFailure::Allocation) => return Err(ExactFailure::Allocation),
        };
        for piece in self.selection.edges.iter().filter(|piece| {
            piece.carrier.polygon
                && piece.carrier.edge.component == component
                && piece.carrier.edge.ring == ring
        }) {
            match bucket_selection_edge(
                &piece.selection[0],
                &piece.selection[1],
                query,
                &mut buckets,
            )? {
                BucketEdge::Decisive(result) => return Ok(result),
                BucketEdge::Continue => {},
                BucketEdge::Uncertain => return Ok(GridPointClass::Unknown),
            }
        }
        if buckets.any_interior() {
            Ok(GridPointClass::Interior)
        } else {
            Ok(GridPointClass::Exterior)
        }
    }

    #[cfg(test)]
    fn ring_fills(&self) -> impl ExactSizeIterator<Item = &LiftedRingFill> {
        self.ring_fills.iter()
    }
}

fn selection_pole_witness_fallible(point: &SelectionPoint) -> ExactResult<Option<GridDegreePoint>> {
    selection_planar_pole_witness_fallible(&ExactSelectionPoint {
        longitude: ExactRatio::from_expansion(&point.longitude)?,
        latitude: ExactRatio::from_expansion(&point.latitude)?,
    })
}

fn selection_planar_pole_witness_fallible(
    point: &ExactSelectionPoint,
) -> ExactResult<Option<GridDegreePoint>> {
    let north = ExactRatio::from_i64(90, 1)?;
    let south = ExactRatio::from_i64(-90, 1)?;
    let latitude = if point.latitude.ordering(&north)? == Ordering::Equal {
        exact(Bound::exact(90.0))?
    } else if point.latitude.ordering(&south)? == Ordering::Equal {
        exact(Bound::exact(-90.0))?
    } else {
        return Ok(None);
    };
    Ok(Some(GridDegreePoint {
        longitude: exact(Bound::exact(0.0))?,
        latitude,
    }))
}

fn first_in_strip_chain_point(chain: &LiftedChain) -> Option<GridDegreePoint> {
    chain.vertices.iter().find_map(|vertex| {
        (-90.0..=90.0)
            .contains(&vertex.latitude)
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
    authority: &mut AuthorityCarrier,
    selection: &mut SelectionCarrier,
    unknown: &mut bool,
) -> crate::error::Result<()> {
    let chain = match LiftedChain::from_points(&input.points, input.closed) {
        Ok(chain) => chain,
        Err(ExactFailure::Arithmetic) => {
            *unknown = true;
            return Ok(());
        },
        Err(ExactFailure::Allocation) => {
            return Err(crate::error::ResourceError::Allocation.into());
        },
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
        let edge = match SymbolicAffineEdge::new(
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
        ) {
            Ok(edge) => edge,
            Err(ExactFailure::Arithmetic) => {
                *unknown = true;
                continue;
            },
            Err(ExactFailure::Allocation) => {
                return Err(crate::error::ResourceError::Allocation.into());
            },
        };
        match edge.selection_pieces(&mut selection.edges) {
            Ok(()) => {},
            Err(ExactFailure::Arithmetic) => *unknown = true,
            Err(ExactFailure::Allocation) => {
                return Err(crate::error::ResourceError::Allocation.into());
            },
        }
        match edge.authority_pieces(&mut authority.pieces) {
            Ok(()) => {},
            Err(ExactFailure::Arithmetic) => *unknown = true,
            Err(ExactFailure::Allocation) => {
                return Err(crate::error::ResourceError::Allocation.into());
            },
        }
        crate::try_push_vec(&mut authority.edges, edge)?;
    }
    debug_assert_eq!(chain_index, lifted.len());
    if input.closed {
        if let Some(fill) =
            LiftedRingFill::from_chain(chain_index, input.component, input.ring, &chain)?
        {
            crate::try_push_vec(ring_fills, fill)?;
        } else {
            *unknown = true;
        }
    }
    crate::try_push_vec(lifted, chain)?;
    Ok(())
}

/// Retain the raw authority point even when its selection transform cannot be
/// certified.  The latter is then a visible unknown; dropping the authority
/// point would turn a fail-open source into an empty source.
fn append_source_point(
    point: &SourcePoint,
    unknown: &mut bool,
) -> crate::error::Result<Option<SelectionPointRecord>> {
    let selection = match selection_image_point(point) {
        Ok(selection) => selection,
        Err(ExactFailure::Arithmetic) => {
            *unknown = true;
            return Ok(None);
        },
        Err(ExactFailure::Allocation) => {
            return Err(crate::error::ResourceError::Allocation.into());
        },
    };
    let bound =
        match GridDegreePoint::try_from_expansions(&selection.longitude, &selection.latitude) {
            Ok(bound) => Some(bound),
            Err(ExactFailure::Arithmetic) => None,
            Err(ExactFailure::Allocation) => {
                return Err(crate::error::ResourceError::Allocation.into());
            },
        };
    Ok(Some(SelectionPointRecord {
        point: selection,
        bounds: bound,
    }))
}

fn append_selection_pole_witness(
    point: &SelectionPointRecord,
    output: &mut Vec<GridDegreePoint>,
    unknown: &mut bool,
) -> crate::error::Result<()> {
    let witness = match selection_pole_witness_fallible(&point.point) {
        Ok(Some(witness)) => witness,
        Ok(None) => return Ok(()),
        Err(ExactFailure::Arithmetic) => {
            *unknown = true;
            return Ok(());
        },
        Err(ExactFailure::Allocation) => {
            return Err(crate::error::ResourceError::Allocation.into());
        },
    };
    crate::try_push_vec(output, witness)
}

fn append_selection_planar_pole_witness(
    point: &ExactSelectionPoint,
    output: &mut Vec<GridDegreePoint>,
    unknown: &mut bool,
) -> crate::error::Result<()> {
    let witness = match selection_planar_pole_witness_fallible(point) {
        Ok(Some(witness)) => witness,
        Ok(None) => return Ok(()),
        Err(ExactFailure::Arithmetic) => {
            *unknown = true;
            return Ok(());
        },
        Err(ExactFailure::Allocation) => {
            return Err(crate::error::ResourceError::Allocation.into());
        },
    };
    crate::try_push_vec(output, witness)
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
) -> crate::error::Result<()> {
    match shape {
        Shape::Point(point) => {
            push_point(*point, *component, 0, points)?;
            *component = component.saturating_add(1);
        },
        Shape::MultiPoint(input) => {
            crate::try_reserve_vec(points, input.len())?;
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
        .then(|| crate::geometry::Point::new_raw_xy(0.0, if north { 90.0 } else { -90.0 }))
}

fn push_point(
    point: crate::geometry::Point,
    component: usize,
    ordinal: usize,
    output: &mut Vec<SourcePointInput>,
) -> crate::error::Result<()> {
    crate::try_push_vec(output, SourcePointInput {
        component,
        ordinal,
        longitude: point.x,
        latitude: point.y,
    })
}

fn push_chain(
    coords: &CoordSeq,
    closed: bool,
    component: usize,
    ring: usize,
    output: &mut Vec<SourceChainInput>,
) -> crate::error::Result<()> {
    let mut points = Vec::new();
    crate::try_reserve_vec(&mut points, coords.len())?;
    for point in coords.points() {
        crate::try_push_vec(&mut points, (point.x, point.y))?;
    }
    crate::try_push_vec(output, SourceChainInput {
        component,
        ring,
        points,
        closed,
    })
}

#[cfg(test)]
#[path = "affine_source_tests.rs"]
mod tests;
