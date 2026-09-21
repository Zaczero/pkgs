//! Deciding how a source meets an axis-aligned degree rectangle.
//!
//! A candidate rectangle is admitted only when exact arithmetic places the
//! source on one side of every edge, so a cover never reports a cell the
//! shape merely comes close to.

use super::*;

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
    pub(super) bounds: Bounds,
    pub(super) windows: DegreeWindowResult,
    pub(super) exact_axis: Option<Bounds>,
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

pub(super) fn degree_windows_from_bounds(bounds: Bounds) -> DegreeWindowResult {
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

pub(super) fn conservative_bounds_from_windows(windows: DegreeWindowResult) -> Bounds {
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

pub(super) fn degree_bounds_overlap(left: Bound, right: Bound) -> bool {
    left.hi >= right.lo && right.hi >= left.lo
}

/// Classify a certified H3 degree window against an exact canonical source
/// rectangle. Bounds come directly from stored source doubles, while the
/// candidate is outward, so strict separation is a negative certificate and
/// closed containment is an interior certificate. Every other case retains
/// the candidate.
pub(super) fn classify_exact_axis_rectangle(
    bounds: Bounds,
    candidate: DegreeWindowResult,
) -> RectClass {
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
pub(super) enum RectangleSide {
    South,
    East,
    North,
    West,
}

impl RectangleSide {
    pub(super) const fn next_counterclockwise(self) -> Self {
        match self {
            Self::South => Self::East,
            Self::East => Self::North,
            Self::North => Self::West,
            Self::West => Self::South,
        }
    }

    pub(super) const fn next_clockwise(self) -> Self {
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
pub(super) fn subdivided_axis_aligned_rectangle(
    source: &Shape,
    bounds: Bounds,
) -> ExactResult<bool> {
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

pub(super) fn exact_axis_rectangle(source: &Shape, bounds: Bounds) -> ExactResult<bool> {
    // The generic rectangle certificate is sufficient for topology shortcuts,
    // but does not verify an ordered, directed source traversal or geographic
    // shortest lifts. The side-walk above owns the stronger cap certificate
    // for both four-vertex and geographic-densified rings.
    subdivided_axis_aligned_rectangle(source, bounds)
}

pub(super) fn exact_world_band(source: &Shape) -> Option<Bounds> {
    let Shape::Polygon(polygon) = source else {
        return None;
    };
    if !polygon.holes.is_empty() {
        return None;
    }
    let Ok(summary) = RingSummary::try_new(
        GeographicFrame::crs84(),
        &GeographicRing::from_ring(&polygon.shell),
    ) else {
        return None;
    };
    (summary.turn == RingTurn::None && matches!(summary.domain, RingDomain::FullLongitude(_)))
        .then(|| Bounds::new_raw(-180.0, summary.south.get(), 180.0, summary.north.get()))
}

/// A bounds rectangle is a complete source model only on the canonical
/// longitude sheet.  A stored ring from 179 to -179 is geometrically a narrow
/// seam strip in the continuous lift, not the 358-degree rectangle named by
/// its planar extrema. The one wider admissible form is the exact physical
/// full-longitude domain `[-180, 180]`.
pub(super) fn canonical_axis_rectangle_longitude(bounds: Bounds) -> ExactResult<bool> {
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

pub(super) const fn classify_exact_axis_window(
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

pub(super) const fn strictly_disjoint_degree_bounds(left: Bound, right: Bound) -> bool {
    left.hi < right.lo || right.hi < left.lo
}

pub(super) const fn closed_degree_bounds_contain(outer: Bound, inner: Bound) -> bool {
    outer.lo <= inner.lo && inner.hi <= outer.hi
}

pub(super) fn strict_window_representative(window: Bound) -> Option<f64> {
    let value = window.lo + (window.hi - window.lo) * 0.5;
    (value.is_finite() && window.lo < value && value < window.hi).then_some(value)
}

pub(super) fn with_exact_rectangle<T>(
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

pub(super) fn closed_segment_intersects<
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

/// Outward binary64 enclosure of an exact coordinate, for rejecting a
/// candidate before any exact arithmetic. The limbs of a nonoverlapping
/// expansion sum to within far less than `2^-40` of its value.
pub(super) fn outward_enclosure(value: &ExactExpansion) -> Option<(f64, f64)> {
    const RELATIVE: f64 = 1.0 / (1_u64 << 40) as f64;
    let estimate = value.checked_estimate()?;
    let margin = estimate.abs() * RELATIVE + f64::MIN_POSITIVE;
    Some((estimate - margin, estimate + margin))
}

pub(super) fn exact_segment_touches_rectangle(
    start: &ExactPlanarPoint,
    end: &ExactPlanarPoint,
    longitude: Bound,
    latitude: Bound,
) -> ExactResult<bool> {
    let enclosure = |a: &ExactExpansion, b: &ExactExpansion| {
        let (a_lo, a_hi) = outward_enclosure(a)?;
        let (b_lo, b_hi) = outward_enclosure(b)?;
        Some((a_lo.min(b_lo), a_hi.max(b_hi)))
    };
    if let Some((south, north)) = enclosure(&start.latitude, &end.latitude)
        && (north < latitude.lo || south > latitude.hi)
    {
        return Ok(false);
    }
    let longitudes = enclosure(&start.longitude, &end.longitude);
    let (first_turn, last_turn) =
        exact_longitude_copy_range(&start.longitude, &end.longitude, longitude)?;
    for turn in first_turn..=last_turn {
        // The copy range is a conservative enumerator; a copy the segment's
        // enclosure misses cannot touch it.
        if let Some((west, east)) = longitudes {
            let offset = FULL_TURN_DEGREES * turn as f64;
            let (low, high) = (longitude.lo + offset, longitude.hi + offset);
            let pad = low.abs().max(high.abs()) * f64::EPSILON;
            if east < low - pad || west > high + pad {
                continue;
            }
        }
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
pub(super) fn exact_longitude_copy_range(
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

pub(super) fn exact_point_touches_rectangle(
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

pub(super) fn exact_authority_edge_touches_rectangle(
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
