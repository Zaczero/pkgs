//! Certified spherical primitives shared by the private grid coverers.
//!
//! This module intentionally has no public geometry surface.  A grid source is
//! affine in longitude/latitude while H3 and S2 boundaries are spherical, so a
//! rounded `LatLng` or an epsilon comparison cannot be allowed to prove a
//! negative.  Every operation here either returns an outward enclosure or
//! declines to decide.

use h3o::{CapLatticeProfile, Hex2Int, ProjectionSeed, Resolution};

/// The two adjacent binary64 values bracketing mathematical pi.
pub(crate) const PI_LO: f64 = f64::from_bits(0x4009_21FB_5444_2D18);
pub(crate) const PI_HI: f64 = f64::from_bits(0x4009_21FB_5444_2D19);

const HALF_PI_LO: f64 = PI_LO * 0.5;
const HALF_PI_HI: f64 = PI_HI * 0.5;
const QUARTER_PI_LO: f64 = PI_LO * 0.25;
const QUARTER_PI_HI: f64 = PI_HI * 0.25;

/// An outward binary64 interval.  `None` is deliberately used for operations
/// which cannot certify a finite result; callers map it to their fail-open
/// state rather than inventing an epsilon.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Bound {
    pub(crate) lo: f64,
    pub(crate) hi: f64,
}

impl Bound {
    pub(crate) const fn exact(value: f64) -> Option<Self> {
        if value.is_finite() {
            Some(Self {
                lo: value,
                hi: value,
            })
        } else {
            None
        }
    }

    pub(crate) const fn new(lo: f64, hi: f64) -> Option<Self> {
        if lo.is_finite() && hi.is_finite() && lo <= hi {
            Some(Self { lo, hi })
        } else {
            None
        }
    }

    fn from_i128(value: i128) -> Option<Self> {
        // Every integer through 2^53 has an exact binary64 representation.
        // Preserve that structural fact: widening an exact native zero below
        // would make a squared-norm enclosure spuriously cross zero.
        const MAX_EXACT_BINARY64_INTEGER: i128 = 1_i128 << 53;
        if (-MAX_EXACT_BINARY64_INTEGER..=MAX_EXACT_BINARY64_INTEGER).contains(&value) {
            return Self::exact(value as f64);
        }
        let rounded = value as f64;
        rounded
            .is_finite()
            .then(|| Self::new(rounded.next_down(), rounded.next_up()))?
    }

    pub(crate) const fn contains_zero(self) -> bool {
        self.lo <= 0.0 && self.hi >= 0.0
    }

    pub(crate) const fn strictly_positive(self) -> bool {
        self.lo > 0.0
    }

    pub(crate) const fn strictly_negative(self) -> bool {
        self.hi < 0.0
    }

    pub(crate) const fn width(self) -> f64 {
        self.hi - self.lo
    }

    const fn is_exact_zero(self) -> bool {
        self.lo == 0.0 && self.hi == 0.0
    }

    /// Structural endpoint identity is intentionally bit-exact.  It never
    /// turns an enclosure into a numeric tolerance test.
    const fn is_exact_value(self, value: f64) -> bool {
        self.lo.to_bits() == value.to_bits() && self.hi.to_bits() == value.to_bits()
    }

    pub(crate) fn add(self, rhs: Self) -> Option<Self> {
        if self.is_exact_zero() {
            return Some(rhs);
        }
        if rhs.is_exact_zero() {
            return Some(self);
        }
        Self::new(down(self.lo + rhs.lo)?, up(self.hi + rhs.hi)?)
    }

    /// Adds quantities already proved non-negative without fabricating a
    /// negative lower endpoint by rounding `0 + 0` downward.
    pub(crate) fn add_nonnegative(self, rhs: Self) -> Option<Self> {
        if self.lo < 0.0 || rhs.lo < 0.0 {
            return None;
        }
        if self.is_exact_zero() {
            return Some(rhs);
        }
        if rhs.is_exact_zero() {
            return Some(self);
        }
        Self::new(down(self.lo + rhs.lo)?.max(0.0), up(self.hi + rhs.hi)?)
    }

    /// Multiplies quantities already proved non-negative while retaining the
    /// exact lower fact at zero.
    pub(crate) fn mul_nonnegative(self, rhs: Self) -> Option<Self> {
        if self.lo < 0.0 || rhs.lo < 0.0 {
            return None;
        }
        Self::new(down(self.lo * rhs.lo)?.max(0.0), up(self.hi * rhs.hi)?)
    }

    /// Divides a proved non-negative quantity by a proved positive one while
    /// retaining the exact lower fact at zero.
    pub(crate) fn div_nonnegative(self, rhs: Self) -> Option<Self> {
        if self.lo < 0.0 || !rhs.strictly_positive() {
            return None;
        }
        Self::new(down(self.lo / rhs.hi)?.max(0.0), up(self.hi / rhs.lo)?)
    }

    pub(crate) fn sub(self, rhs: Self) -> Option<Self> {
        if rhs.is_exact_zero() {
            return Some(self);
        }
        if self.is_exact_zero() {
            return Some(rhs.neg());
        }
        Self::new(down(self.lo - rhs.hi)?, up(self.hi - rhs.lo)?)
    }

    pub(crate) fn neg(self) -> Self {
        // Endpoints are finite by construction and negation is exact.
        Self {
            lo: -self.hi,
            hi: -self.lo,
        }
    }

    pub(crate) fn abs(self) -> Self {
        if self.contains_zero() {
            Self {
                lo: 0.0,
                hi: self.lo.abs().max(self.hi.abs()),
            }
        } else if self.strictly_negative() {
            self.neg()
        } else {
            self
        }
    }

    pub(crate) fn mul(self, rhs: Self) -> Option<Self> {
        if self.is_exact_zero() || rhs.is_exact_zero() {
            return Self::exact(0.0);
        }
        let products = [
            self.lo * rhs.lo,
            self.lo * rhs.hi,
            self.hi * rhs.lo,
            self.hi * rhs.hi,
        ];
        let (min, max) = finite_minmax(products)?;
        Self::new(down(min)?, up(max)?)
    }

    /// Outward square with the mathematical non-negativity fact retained when
    /// an enclosure crosses zero.  Generic interval multiplication must not
    /// use a negative lower endpoint as evidence against `x * x >= 0`.
    pub(crate) fn square(self) -> Option<Self> {
        if self.is_exact_zero() {
            return Self::exact(0.0);
        }
        if self.contains_zero() {
            let maximum = self.lo.abs().max(self.hi.abs());
            return Self::new(0.0, up(maximum * maximum)?);
        }
        self.mul(self)
    }

    pub(crate) fn div(self, rhs: Self) -> Option<Self> {
        if rhs.contains_zero() {
            return None;
        }
        if self.is_exact_zero() {
            return Self::exact(0.0);
        }
        let quotients = [
            self.lo / rhs.lo,
            self.lo / rhs.hi,
            self.hi / rhs.lo,
            self.hi / rhs.hi,
        ];
        let (min, max) = finite_minmax(quotients)?;
        Self::new(down(min)?, up(max)?)
    }

    pub(crate) fn sqrt(self) -> Option<Self> {
        if self.lo < 0.0 {
            return None;
        }
        let proposed_lo = self.lo.sqrt();
        let proposed_hi = self.hi.sqrt();
        // Hardware square root only proposes candidates.  Round them outward
        // *before* checking their squares: the lower candidate must be no
        // greater than sqrt(lo), and the upper candidate no smaller than
        // sqrt(hi).  Mere interval overlap would accept an inward bracket.
        let mut lo = if self.lo == 0.0 {
            0.0
        } else {
            down(proposed_lo)?
        };
        let mut hi = if self.hi == 0.0 {
            0.0
        } else {
            up(proposed_hi)?
        };
        // A one-ULP proposal can still be too narrow after the independently
        // outward multiplication in `verified_sqrt_enclosure`.  Expand only
        // adjacent binary64 candidates and accept the first interval whose
        // two square inequalities are proved; there is no tolerance branch.
        for _ in 0..=64 {
            if let Some(root) = verified_sqrt_enclosure(self, lo, hi) {
                return Some(root);
            }
            if lo > 0.0 {
                lo = lo.next_down();
            }
            hi = up(hi)?;
        }
        None
    }

    const fn intersection(self, rhs: Self) -> Option<Self> {
        Self::new(self.lo.max(rhs.lo), self.hi.min(rhs.hi))
    }
}

/// Accepts a proposed root enclosure only after multiplication proves it
/// outward.  Keeping the check separate makes the two inequality directions
/// testable: interval overlap is not a square-root certificate.
fn verified_sqrt_enclosure(input: Bound, lo: f64, hi: f64) -> Option<Bound> {
    if input.lo < 0.0 || !lo.is_finite() || !hi.is_finite() || lo < 0.0 || lo > hi {
        return None;
    }
    let lo_sq = Bound::exact(lo)?.mul(Bound::exact(lo)?)?;
    let hi_sq = Bound::exact(hi)?.mul(Bound::exact(hi)?)?;
    if lo_sq.hi > input.lo || hi_sq.lo < input.hi {
        return None;
    }
    Bound::new(lo, hi)
}

/// A scalar centre plus an outward radius.  The representation is compact for
/// the common scalar filter and converts to [`Bound`] before any branch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Ball {
    pub(crate) mid: f64,
    pub(crate) rad: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CertSign {
    Negative,
    Positive,
    Uncertain,
}

impl Ball {
    pub(crate) const fn exact(value: f64) -> Option<Self> {
        if value.is_finite() {
            Some(Self {
                mid: value,
                rad: 0.0,
            })
        } else {
            None
        }
    }

    pub(crate) fn new(mid: f64, rad: f64) -> Option<Self> {
        if !mid.is_finite() || !rad.is_finite() || rad < 0.0 {
            return None;
        }
        let result = Self { mid, rad };
        result.bound().map(|_| result)
    }

    fn from_bound(value: Bound) -> Option<Self> {
        let mid = value.lo.certified_midpoint(value.hi)?;
        Self::new(mid, outward_half_width(value)?)
    }

    pub(crate) fn bound(self) -> Option<Bound> {
        Bound::new(down(self.mid - self.rad)?, up(self.mid + self.rad)?)
    }

    pub(crate) fn sign(self) -> CertSign {
        match self.bound() {
            Some(bound) if bound.strictly_positive() => CertSign::Positive,
            Some(bound) if bound.strictly_negative() => CertSign::Negative,
            _ => CertSign::Uncertain,
        }
    }

    pub(crate) fn add(self, rhs: Self) -> Option<Self> {
        let mid = self.mid + rhs.mid;
        let rad = upward_sum([self.rad, rhs.rad, ulp(mid)?])?;
        Self::new(mid, rad)
    }

    fn neg(self) -> Option<Self> {
        Self::new(-self.mid, self.rad)
    }

    fn scale_sign(self, sign: i8) -> Option<Self> {
        match sign {
            1 => Some(self),
            -1 => self.neg(),
            _ => None,
        }
    }

    pub(crate) fn mul(self, rhs: Self) -> Option<Self> {
        let mid = self.mid * rhs.mid;
        let rad = upward_sum([
            self.mid.abs() * rhs.rad,
            rhs.mid.abs() * self.rad,
            self.rad * rhs.rad,
            ulp(mid)?,
        ])?;
        Self::new(mid, rad)
    }
}

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
    #[cfg(test)]
    Equator,
    #[cfg(test)]
    AxisMeridian(AxisMeridian),
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
    fn radian_shifts(self) -> Option<[Bound; 3]> {
        let base = match self {
            Self::Prime => Bound::exact(0.0)?,
            Self::EastQuarter => Bound::new(HALF_PI_LO, HALF_PI_HI)?,
            Self::Antimeridian => Bound::new(PI_LO, PI_HI)?,
            Self::WestQuarter => Bound::new(-HALF_PI_HI, -HALF_PI_LO)?,
        };
        let tau = Bound::new(2.0 * PI_LO, 2.0 * PI_HI)?;
        Some([base.sub(tau)?, base, base.add(tau)?])
    }

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

/// Certified radian bounds at the one permitted conversion boundary between
/// spherical H3 work and the lon/lat source carrier.  Longitude remains a
/// continuous branch until this conversion decides its exact degree seam.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CertifiedCircularBoundsRad {
    latitude: Bound,
    longitude: CertifiedLongitudeRad,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CertifiedLongitudeRad {
    Full,
    /// A directed full turn enclosing the north pole. Longitude is still
    /// global, but the latitude certificate must retain this identity before
    /// it is allowed to establish a negative bbox relation.
    FullThroughNorthPole,
    /// The corresponding south-pole full turn.
    FullThroughSouthPole,
    Span(LongitudeSpan),
}

/// One or two canonical closed degree windows.  `Full` deliberately has no
/// seam spelling, preventing a future consumer from accidentally treating
/// +180 and -180 as distinct interior longitude values.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CertifiedDegreeWindows {
    pub(crate) latitude: Bound,
    pub(crate) longitude: CertifiedLongitudeDegrees,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CertifiedLongitudeDegrees {
    Full,
    One(Bound),
    Two([Bound; 2]),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum DegreeWindowResult {
    Windows(CertifiedDegreeWindows),
    Boundary,
}

/// The finite target-specific native lattice data after every numeric norm has
/// been enclosed outward.  This is deliberately only the vendor half of A3:
/// it cannot classify a cell until the projection and seam certificates have
/// also been assembled into a descendant cap.
#[derive(Clone, Debug)]
pub(crate) struct CertifiedTargetLatticeProfile {
    raw: CapLatticeProfile,
    suffix_max: [Bound; 15],
    vertex_max: Bound,
    rotation_norm: Bound,
}

const H3_RESOLUTION_COUNT: usize = 16;

/// The complete target-specific angular cap plan.  It is constructed only
/// when every ancestor depth certifies; consumers never observe a partial
/// radius table.
#[derive(Clone, Debug)]
pub(crate) struct CertifiedH3DescendantCap {
    target: Resolution,
    adjusted_resolution: u8,
    max_dim: i64,
    angular_radii: [Bound; H3_RESOLUTION_COUNT],
}

/// A projected centre that can only be obtained from the target child in the
/// profiled substrate lattice.  The ordinary parent centre is also a
/// `substrate` point, so carrying a boolean would not make the invalid cap
/// fallback unrepresentable.
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
struct CertifiedCapCenter(Vec3Bound);

impl CertifiedCapCenter {
    fn for_target_cell(
        cell: h3o::CellIndex,
        target: Resolution,
        adjusted_resolution: u8,
        max_dim: i64,
    ) -> Option<Self> {
        let seed = cell.target_substrate_center_seed(target)?;
        let ProjectionSeed::Point {
            point,
            resolution,
            substrate,
            ..
        } = seed
        else {
            return None;
        };
        certified_center_seed_is_admissible(
            point,
            resolution,
            substrate,
            adjusted_resolution,
            max_dim,
        )
        .then(|| project_h3_seed(seed).map(Self))?
    }
}

impl CertifiedH3DescendantCap {
    pub(crate) fn for_target(target: Resolution) -> Option<Self> {
        let lattice = CertifiedTargetLatticeProfile::for_target(target)?;
        let adjusted_resolution = lattice.raw().adjusted_resolution();
        let expected_dim =
            2_i64.checked_mul(7_i64.checked_pow(u32::from(adjusted_resolution / 2))?)?;
        if !certified_profile_is_coupled(
            lattice.raw().target(),
            adjusted_resolution,
            lattice.raw().max_dim(),
            lattice.raw().seams().len(),
            target,
        ) {
            return None;
        }
        let scale = certified_profile_scale(lattice.raw())?;
        let chart_stretch = certified_chart_stretch(lattice.raw(), scale)?;
        let seam_cost = certified_seam_cost(lattice.raw(), scale)?;
        let rotation_cubed = lattice
            .rotation_norm()
            .mul_nonnegative(lattice.rotation_norm())?
            .mul_nonnegative(lattice.rotation_norm())?;
        let zero = Bound::exact(0.0)?;
        let mut angular_radii = [zero; H3_RESOLUTION_COUNT];
        for depth in Resolution::range(Resolution::Zero, target) {
            certified_ideal_face_ratio(target, depth)?;
            let native = lattice.native_descendant_radius(depth)?;
            let radius =
                certified_angular_radius(scale, chart_stretch, rotation_cubed, native, seam_cost)?;
            if radius.hi >= HALF_PI_LO {
                return None;
            }
            angular_radii[usize::from(depth)] = radius;
        }
        Some(Self {
            target,
            adjusted_resolution,
            max_dim: expected_dim,
            angular_radii,
        })
    }

    #[cfg(test)]
    pub(crate) const fn target(&self) -> Resolution {
        self.target
    }

    fn angular_radius(&self, depth: Resolution) -> Option<Bound> {
        (depth <= self.target).then(|| self.angular_radii[usize::from(depth)])
    }

    pub(crate) fn descendant_windows(
        &self,
        cell: h3o::CellIndex,
        poles: H3PoleOwners,
    ) -> DegreeWindowResult {
        let Some(result) = (|| {
            if poles.target != self.target || cell.resolution() > self.target {
                return None;
            }
            let center = CertifiedCapCenter::for_target_cell(
                cell,
                self.target,
                self.adjusted_resolution,
                self.max_dim,
            )?;
            let radius = self.angular_radius(cell.resolution())?;
            let latitude = certified_vector_latitude(center.0)?;
            let (south, north, absolute) = certified_cap_latitude_bounds(latitude, radius)?;
            if south.lo > north.hi {
                return None;
            }
            if absolute.hi >= HALF_PI_LO {
                return Some((
                    CertifiedCircularBoundsRad {
                        latitude: Bound::new(south.lo, north.hi)?,
                        longitude: CertifiedLongitudeRad::Full,
                    },
                    center,
                ));
            }
            let longitude = certified_vector_longitude(center.0)?;
            let (_, cosine) = sin_cos(absolute)?;
            let delta = radius.div_nonnegative(cosine.bound()?)?;
            if delta.hi >= PI_LO {
                return Some((
                    CertifiedCircularBoundsRad {
                        latitude: Bound::new(south.lo, north.hi)?,
                        longitude: CertifiedLongitudeRad::Full,
                    },
                    center,
                ));
            }
            let longitude = cap_longitude_span(longitude, delta)?;
            Some((
                CertifiedCircularBoundsRad {
                    latitude: Bound::new(south.lo, north.hi)?,
                    longitude,
                },
                center,
            ))
        })() else {
            return DegreeWindowResult::Boundary;
        };
        let (bounds, center) = result;
        bounds.to_pruning_degree_windows(center)
    }
}

/// Outward cap latitude arithmetic.  This is a pruning certificate, so every
/// operation stays in `Bound`; raw endpoint arithmetic is not authority.
fn certified_cap_latitude_bounds(latitude: Bound, radius: Bound) -> Option<(Bound, Bound, Bound)> {
    let world = Bound::new(-HALF_PI_HI, HALF_PI_HI)?;
    let south = latitude.sub(radius)?.intersection(world)?;
    let north = latitude.add(radius)?.intersection(world)?;
    let absolute = latitude.abs().add_nonnegative(radius)?;
    Some((south, north, absolute))
}

/// A cap radius is a correlated `[0, R]` enclosure, not two independent arc
/// endpoints.  Its upper half-width alone gives the outward longitude span;
/// a later seam ambiguity is safely the full longitude range.
fn cap_longitude_span(longitude: Bound, delta: Bound) -> Option<CertifiedLongitudeRad> {
    if delta.lo < 0.0 {
        return None;
    }
    let outer = Bound::exact(delta.hi)?;
    Some(
        LongitudeSpan::ordered(longitude.sub(outer)?, longitude.add(outer)?)
            .map_or(CertifiedLongitudeRad::Full, CertifiedLongitudeRad::Span),
    )
}

/// Validates the finite vendor profile before it can contribute even one
/// descendant radius.  Keeping this scalar check separate lets its exact
/// coupling be tested without manufacturing malformed vendor sidecars.
fn certified_profile_is_coupled(
    raw_target: Resolution,
    adjusted_resolution: u8,
    max_dim: i64,
    seam_count: usize,
    target: Resolution,
) -> bool {
    let Some(expected_adjusted) =
        target_value(target).and_then(|value| value.checked_add(u8::from(target.is_class3())))
    else {
        return false;
    };
    let Some(power) = 7_i64.checked_pow(u32::from(adjusted_resolution / 2)) else {
        return false;
    };
    let Some(expected_dim) = 2_i64.checked_mul(power) else {
        return false;
    };
    raw_target == target
        && adjusted_resolution == expected_adjusted
        && max_dim == expected_dim
        && seam_count == 60
}

/// Rejects a target-center carrier unless it is exactly the substrate point
/// represented by the profiled target lattice.  A failure makes the caller's
/// bbox `Boundary`; it can never prune a cell.
fn certified_center_seed_is_admissible(
    point: Hex2Int,
    resolution: u8,
    substrate: bool,
    adjusted_resolution: u8,
    max_dim: i64,
) -> bool {
    let Some(limit) = 3_i64.checked_mul(max_dim) else {
        return false;
    };
    resolution == adjusted_resolution
        && substrate
        && point.normalized_ijk_sum().is_some_and(|sum| sum <= limit)
}

/// The certified cap composition.  `rotation_cubed` is already `K³`; keeping
/// it explicit makes the three-crossing charge impossible to accidentally
/// collapse into a one-rotation estimate.
fn certified_angular_radius(
    scale: Bound,
    chart_stretch: Bound,
    rotation_cubed: Bound,
    native_radius: Bound,
    seam_cost: Bound,
) -> Option<Bound> {
    scale
        .mul_nonnegative(chart_stretch)?
        .mul_nonnegative(rotation_cubed)?
        .mul_nonnegative(native_radius)?
        .add_nonnegative(Bound::exact(3.0)?.mul_nonnegative(seam_cost)?)
}

impl CertifiedTargetLatticeProfile {
    pub(crate) fn for_target(target: Resolution) -> Option<Self> {
        let raw = CapLatticeProfile::for_target(target);
        let adjusted = target_value(target)? + u8::from(target.is_class3());
        if raw.target() != target
            || raw.adjusted_resolution() != adjusted
            || raw.max_dim() <= 0
            || raw.seams().len() != 60
        {
            return None;
        }

        let sqrt3_2 = certified_profile_sqrt3_2(&raw, adjusted)?;
        let zero = Bound::exact(0.0)?;
        let mut suffix_max = [zero; 15];
        for raw_resolution in 1..=target_value(target)? {
            let resolution = Resolution::try_from(raw_resolution).ok()?;
            let offsets = raw.suffix_offsets(resolution)?;
            suffix_max[usize::from(raw_resolution - 1)] = certified_lattice_max(offsets, sqrt3_2)?;
        }
        let vertex_max = certified_lattice_max(raw.vertex_offsets(), sqrt3_2)?;
        let rotation_norm = certified_native_rotation_norm(sqrt3_2)?;
        Some(Self {
            raw,
            suffix_max,
            vertex_max,
            rotation_norm,
        })
    }

    pub(crate) const fn raw(&self) -> &CapLatticeProfile {
        &self.raw
    }

    pub(crate) const fn rotation_norm(&self) -> Bound {
        self.rotation_norm
    }

    /// Returns the native-plane enclosure for every target boundary endpoint
    /// under an ancestor at `depth`, before chart overage is charged.
    pub(crate) fn native_descendant_radius(&self, depth: Resolution) -> Option<Bound> {
        if depth > self.raw.target() {
            return None;
        }
        let mut radius = self.vertex_max;
        for raw_resolution in (target_value(depth)? + 1)..=target_value(self.raw.target())? {
            radius = radius.add_nonnegative(self.suffix_max[usize::from(raw_resolution - 1)])?;
        }
        Some(radius)
    }
}

fn target_value(value: Resolution) -> Option<u8> {
    let value = u8::from(value);
    (value <= 15).then_some(value)
}

/// Every profile seam must remain a paired raw substrate carrier at the
/// adjusted target resolution.  This check intentionally uses bit identity
/// only for vendor constants; no rounded geographic coordinate participates.
fn certified_profile_sqrt3_2(raw: &CapLatticeProfile, adjusted: u8) -> Option<Bound> {
    let first = raw.seams().first()?.left()[0];
    let frame = first.projection_frame();
    if !frame.sqrt3_2.is_finite() || frame.sqrt3_2 <= 0.0 {
        return None;
    }
    let bits = frame.sqrt3_2.to_bits();
    for seam in raw.seams() {
        for seed in seam.left().into_iter().chain(seam.right()) {
            let ProjectionSeed::Point {
                resolution,
                substrate,
                ..
            } = seed
            else {
                return None;
            };
            if resolution != adjusted
                || !substrate
                || seed.projection_frame().sqrt3_2.to_bits() != bits
            {
                return None;
            }
        }
    }
    Bound::exact(frame.sqrt3_2)
}

fn certified_profile_scale(raw: &CapLatticeProfile) -> Option<Bound> {
    let first = raw.seams().first()?.left()[0].projection_frame();
    for seam in raw.seams() {
        for seed in seam.left().into_iter().chain(seam.right()) {
            let frame = seed.projection_frame();
            if frame.res0_u_gnomonic.to_bits() != first.res0_u_gnomonic.to_bits()
                || frame.inv_sqrt7.to_bits() != first.inv_sqrt7.to_bits()
                || frame.one_third.to_bits() != first.one_third.to_bits()
            {
                return None;
            }
        }
    }
    Bound::exact(first.res0_u_gnomonic)?
        .mul_nonnegative(Bound::exact(first.inv_sqrt7)?)?
        .mul_nonnegative(Bound::exact(first.one_third)?)
}

#[derive(Clone, Copy)]
struct FaceProjectionCertificate {
    /// A strictly positive lower enclosure for the unnormalised carrier norm
    /// across the raw face triangle.  It is a lower bound by construction,
    /// so dividing by it only enlarges a later Lipschitz/seam charge.
    minimum_denominator: Bound,
    basis_norm: Bound,
    #[cfg(test)]
    raw_c_radicand: Bound,
}

fn certified_chart_stretch(raw: &CapLatticeProfile, scale: Bound) -> Option<Bound> {
    let mut maximum = Bound::exact(0.0)?;
    for seam in raw.seams() {
        for seed in seam.left().into_iter().chain(seam.right()) {
            let certificate = certified_face_projection(seed, raw.max_dim(), scale)?;
            let stretch = certificate
                .basis_norm
                .div_nonnegative(certificate.minimum_denominator)?;
            maximum = Bound::new(0.0, maximum.hi.max(stretch.hi))?;
        }
    }
    Some(maximum)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn certified_face_projection(
    seed: ProjectionSeed,
    max_dim: i64,
    scale: Bound,
) -> Option<FaceProjectionCertificate> {
    let frame = seed.projection_frame();
    let center = Vec3Bound::exact(frame.center)?;
    let north_axis = Vec3Bound::exact([0.0, 0.0, 1.0])?;
    let north = north_axis.sub(center.scale(center.z)?)?.normalized()?;
    let east = north.cross(center)?;
    let angle = Bound::exact(frame.axis_az_rads_cii)?;
    let (sin, cos) = sin_cos(angle)?;
    let sin = sin.bound()?;
    let cos = cos.bound()?;
    let first = north.scale(cos)?.add(east.scale(sin)?)?;
    let second = north.scale(sin)?.sub(east.scale(cos)?)?;
    let center_norm = center.dot(center)?;
    // Keep `h = ||B^T C||` in the same rotated B basis used for the
    // Lipschitz norm below.  Although Q is a native rotation, spelling it
    // directly avoids making that orthogonality an unstated proof premise.
    let center_projection = first
        .dot(center)?
        .square()?
        .add_nonnegative(second.dot(center)?.square()?)?
        .sqrt()?;
    let face_radius = certified_face_radius(max_dim, Bound::exact(frame.sqrt3_2)?)?;
    let radicand = center_norm.sub(
        Bound::exact(2.0)?
            .mul_nonnegative(scale)?
            .mul_nonnegative(face_radius)?
            .mul_nonnegative(center_projection)?,
    )?;
    if radicand.lo <= 0.0 {
        return None;
    }
    let minimum_denominator = Bound::exact(radicand.lo)?.sqrt()?;
    if !minimum_denominator.strictly_positive() {
        return None;
    }
    let basis_norm = certified_two_column_norm(first, second)?;
    Some(FaceProjectionCertificate {
        minimum_denominator,
        basis_norm,
        #[cfg(test)]
        raw_c_radicand: radicand,
    })
}

fn certified_two_column_norm(first: Vec3Bound, second: Vec3Bound) -> Option<Bound> {
    let g00 = first.dot(first)?;
    let g01 = first.dot(second)?;
    let g11 = second.dot(second)?;
    certified_gram_norm(g00, g01, g11)
}

fn certified_gram_norm(g00: Bound, g01: Bound, g11: Bound) -> Option<Bound> {
    if g00.lo < 0.0 || g11.lo < 0.0 {
        return None;
    }
    let trace = g00.add_nonnegative(g11)?;
    let discriminant = g00
        .sub(g11)?
        .square()?
        .add_nonnegative(Bound::exact(4.0)?.mul_nonnegative(g01.square()?)?)?
        .sqrt()?;
    trace
        .add_nonnegative(discriminant)?
        .div_nonnegative(Bound::exact(2.0)?)?
        .sqrt()
}

fn certified_seam_cost(raw: &CapLatticeProfile, scale: Bound) -> Option<Bound> {
    let mut maximum_charge = Bound::exact(0.0)?;
    for (left_seed, right_seed) in certified_matching_seam_pairs(raw) {
        let seam_charge = certified_seam_charge(left_seed, right_seed, raw.max_dim(), scale)?;
        maximum_charge = Bound::new(0.0, maximum_charge.hi.max(seam_charge.hi))?;
    }
    Some(maximum_charge)
}

fn certified_matching_seam_pairs(
    raw: &CapLatticeProfile,
) -> impl Iterator<Item = (ProjectionSeed, ProjectionSeed)> + '_ {
    raw.seams()
        .iter()
        .flat_map(|seam| seam.left().into_iter().zip(seam.right()))
}

/// One directed seam's normalization charge.  The delta is deliberately
/// formed in the two raw face carriers; normalized vectors would erase the
/// chart mismatch that the cap must pay for.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn certified_seam_charge(
    left_seed: ProjectionSeed,
    right_seed: ProjectionSeed,
    max_dim: i64,
    scale: Bound,
) -> Option<Bound> {
    let left_certificate = certified_face_projection(left_seed, max_dim, scale)?;
    let right_certificate = certified_face_projection(right_seed, max_dim, scale)?;
    let delta = certified_seam_delta(left_seed, right_seed)?;
    let denominator = Bound::exact(
        left_certificate
            .minimum_denominator
            .lo
            .min(right_certificate.minimum_denominator.lo),
    )?;
    Bound::new(PI_LO, PI_HI)?
        .mul_nonnegative(delta)?
        .div_nonnegative(denominator)
}

/// Distance between the two raw, unnormalised representations of one seam
/// endpoint.  Matching directed endpoints are almost identical; reversing
/// them yields the opposite edge endpoint and is macroscopically distinct.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn certified_seam_delta(left_seed: ProjectionSeed, right_seed: ProjectionSeed) -> Option<Bound> {
    project_h3_seed_unnormalized(left_seed)?
        .sub(project_h3_seed_unnormalized(right_seed)?)?
        .squared_norm()?
        .sqrt()
}

/// The maximum native-plane norm of the three raw vertices of a Class-II
/// icosahedron face at `max_dim`.  All three are enumerated: binary
/// `SQRT3_2` is intentionally not treated as mathematical sqrt(3)/2.
fn certified_face_radius(max_dim: i64, sqrt3_2: Bound) -> Option<Bound> {
    let max_dim = i128::from(max_dim);
    let coordinates = [
        (6_i128.checked_mul(max_dim)?, 0),
        (-3_i128.checked_mul(max_dim)?, 3_i128.checked_mul(max_dim)?),
        (-3_i128.checked_mul(max_dim)?, -3_i128.checked_mul(max_dim)?),
    ];
    let mut maximum = Bound::exact(0.0)?;
    for (x2, y_sqrt3) in coordinates {
        let point = Hex2Int::new(i64::try_from(x2).ok()?, i64::try_from(y_sqrt3).ok()?);
        let norm = certified_lattice_norm(point, sqrt3_2)?;
        maximum = Bound::new(0.0, maximum.hi.max(norm.hi))?;
    }
    Some(maximum)
}

/// Proves the finite unfolded-face crossing lemma for one ancestor depth.
///
/// These are mathematical `sqrt(3)`/`sqrt(7)` bounds, deliberately separate
/// from the vendor's binary `SQRT3_2` arithmetic used by `D_native`.  The
/// strict result licenses exactly three, rather than an empirical or padded,
/// chart-transition charges in [`CertifiedH3DescendantCap::radius`].
fn certified_ideal_face_ratio(target: Resolution, depth: Resolution) -> Option<Bound> {
    if depth > target {
        return None;
    }
    let target_resolution = target_value(target)?;
    let depth_resolution = target_value(depth)?;
    let class_iii = u8::from(target.is_class3());
    let sqrt7 = Bound::exact(7.0)?.sqrt()?;
    let mut ideal = if class_iii == 1 {
        Bound::exact(21.0)?.sqrt()?
    } else {
        Bound::exact(3.0)?.sqrt()?
    };
    for k in (depth_resolution + 1)..=target_resolution {
        let exponent = target_resolution.checked_sub(k)?.checked_add(class_iii)?;
        let suffix = Bound::exact(3.0)?.mul_nonnegative(integer_power_bound(sqrt7, exponent)?)?;
        ideal = ideal.add_nonnegative(suffix)?;
    }
    let adjusted = target_resolution.checked_add(class_iii)?;
    let face_dimension = Bound::exact(2.0)?
        .mul_nonnegative(integer_power_bound(Bound::exact(7.0)?, adjusted / 2)?)?;
    let ratio = ideal.div_nonnegative(face_dimension)?;
    certified_strict_face_ratio(ratio)
}

/// The face transition count is licensed only by a strict ratio.  Equality is
/// a boundary event and must leave the cap unavailable.
fn certified_strict_face_ratio(ratio: Bound) -> Option<Bound> {
    (ratio.hi < 1.0).then_some(ratio)
}

fn certified_lattice_max(points: &[Hex2Int], sqrt3_2: Bound) -> Option<Bound> {
    let mut maximum = Bound::exact(0.0)?;
    for point in points {
        let norm = certified_lattice_norm(*point, sqrt3_2)?;
        maximum = Bound::new(0.0, maximum.hi.max(norm.hi))?;
    }
    Some(maximum)
}

fn certified_lattice_norm(point: Hex2Int, sqrt3_2: Bound) -> Option<Bound> {
    let x = Bound::from_i128(i128::from(point.x2))?.mul(Bound::exact(0.5)?)?;
    let y = Bound::from_i128(i128::from(point.y_sqrt3))?.mul(sqrt3_2)?;
    x.square()?.add_nonnegative(y.square()?)?.sqrt()
}

/// Outward `K`: the maximum spectral norm of all six powers of the vendor's
/// binary 60-degree lattice rotation.  The upper Gram eigenvalue is used
/// rather than a Frobenius relaxation; a single rotation is insufficient
/// because the vendor's binary `SQRT3_2` makes the powers non-orthogonal.
fn certified_native_rotation_norm(sqrt3_2: Bound) -> Option<Bound> {
    let powers = certified_native_rotation_power_norms(sqrt3_2)?;
    let mut maximum = Bound::exact(0.0)?;
    for norm in powers {
        maximum = Bound::new(0.0, maximum.hi.max(norm.hi))?;
    }
    Some(maximum)
}

fn certified_native_rotation_power_norms(sqrt3_2: Bound) -> Option<[Bound; 6]> {
    let rotation = [
        Bound::exact(0.5)?,
        Bound::exact(-3.0)?.div(Bound::exact(4.0)?.mul(sqrt3_2)?)?,
        sqrt3_2,
        Bound::exact(0.5)?,
    ];
    let mut power = [
        Bound::exact(1.0)?,
        Bound::exact(0.0)?,
        Bound::exact(0.0)?,
        Bound::exact(1.0)?,
    ];
    let mut norms = [Bound::exact(0.0)?; 6];
    for slot in &mut norms {
        *slot = certified_two_by_two_norm(power)?;
        power = certified_two_by_two_product(power, rotation)?;
    }
    Some(norms)
}

fn certified_two_by_two_product(left: [Bound; 4], right: [Bound; 4]) -> Option<[Bound; 4]> {
    Some([
        left[0].mul(right[0])?.add(left[1].mul(right[2])?)?,
        left[0].mul(right[1])?.add(left[1].mul(right[3])?)?,
        left[2].mul(right[0])?.add(left[3].mul(right[2])?)?,
        left[2].mul(right[1])?.add(left[3].mul(right[3])?)?,
    ])
}

fn certified_two_by_two_norm(entries: [Bound; 4]) -> Option<Bound> {
    let [a, b, c, d] = entries;
    let g00 = a.square()?.add_nonnegative(c.square()?)?;
    let g01 = a.mul(b)?.add(c.mul(d)?)?;
    let g11 = b.square()?.add_nonnegative(d.square()?)?;
    certified_gram_norm(g00, g01, g11)
}

impl CertifiedCircularBoundsRad {
    /// Cap-only conversion: once [`CertifiedCapCenter`] has established the
    /// target-child provenance, an ambiguous longitude seam may widen to the
    /// complete longitude range while retaining its certified latitude.
    /// Logical H3 bboxes use [`Self::to_degree_windows`] instead and preserve
    /// `Boundary` for the same ambiguity.
    fn to_pruning_degree_windows(self, _center: CertifiedCapCenter) -> DegreeWindowResult {
        let Some(latitude) = radians_to_degrees(self.latitude)
            .and_then(|latitude| Bound::new(latitude.lo.max(-90.0), latitude.hi.min(90.0)))
        else {
            return DegreeWindowResult::Boundary;
        };
        match self.to_degree_windows() {
            DegreeWindowResult::Windows(CertifiedDegreeWindows { longitude, .. }) => {
                DegreeWindowResult::Windows(CertifiedDegreeWindows {
                    latitude,
                    longitude,
                })
            },
            DegreeWindowResult::Boundary => DegreeWindowResult::Windows(CertifiedDegreeWindows {
                latitude,
                longitude: CertifiedLongitudeDegrees::Full,
            }),
        }
    }

    fn to_degree_windows(self) -> DegreeWindowResult {
        let Some(latitude) = radians_to_degrees(self.latitude)
            .and_then(|latitude| Bound::new(latitude.lo.max(-90.0), latitude.hi.min(90.0)))
        else {
            return DegreeWindowResult::Boundary;
        };
        let longitude = match self.longitude {
            CertifiedLongitudeRad::Full
            | CertifiedLongitudeRad::FullThroughNorthPole
            | CertifiedLongitudeRad::FullThroughSouthPole => CertifiedLongitudeDegrees::Full,
            CertifiedLongitudeRad::Span(span) => {
                let (Some(lower), Some(upper)) = (
                    radians_to_degrees(span.lower),
                    radians_to_degrees(span.upper),
                ) else {
                    return DegreeWindowResult::Boundary;
                };
                // A branch wholly in the canonical sheet needs no seam cut.
                if lower.lo >= -180.0 && upper.hi <= 180.0 {
                    let Some(window) = Bound::new(lower.lo, upper.hi) else {
                        return DegreeWindowResult::Boundary;
                    };
                    CertifiedLongitudeDegrees::One(window)
                // A positive-sheet branch crosses exactly +180.  Both endpoint
                // orders must be strict, otherwise a rounded seam spelling
                // could discard a true contact.
                } else if lower.hi < 180.0 && upper.lo > 180.0 && upper.hi < 540.0 {
                    let Some(shifted_upper) =
                        upper.sub(Bound::exact(360.0).expect("finite degree"))
                    else {
                        return DegreeWindowResult::Boundary;
                    };
                    let (Some(west), Some(east)) = (
                        Bound::new(lower.lo, 180.0),
                        Bound::new(-180.0, shifted_upper.hi),
                    ) else {
                        return DegreeWindowResult::Boundary;
                    };
                    CertifiedLongitudeDegrees::Two([west, east])
                // The equivalent negative-sheet branch crosses -180.
                } else if lower.hi < -180.0 && upper.lo > -180.0 && lower.lo > -540.0 {
                    let Some(shifted_lower) =
                        lower.add(Bound::exact(360.0).expect("finite degree"))
                    else {
                        return DegreeWindowResult::Boundary;
                    };
                    let (Some(west), Some(east)) = (
                        Bound::new(shifted_lower.lo, 180.0),
                        Bound::new(-180.0, upper.hi),
                    ) else {
                        return DegreeWindowResult::Boundary;
                    };
                    CertifiedLongitudeDegrees::Two([west, east])
                } else {
                    return DegreeWindowResult::Boundary;
                }
            },
        };
        DegreeWindowResult::Windows(CertifiedDegreeWindows {
            latitude,
            longitude,
        })
    }
}

/// The single outward radian/degree conversion.  This must remain the owner:
/// `to_degrees`, a rounded pi denominator, or duplicated callers would turn a
/// degree seam into an unproved negative.
fn radians_to_degrees(radians: Bound) -> Option<Bound> {
    radians
        .mul(Bound::exact(180.0)?)?
        .div(Bound::new(PI_LO, PI_HI)?)
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
    latitude: Option<LongitudeSpan>,
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
            latitude: None,
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

    #[cfg(test)]
    fn from_test_axis(start: [f64; 3], end: [f64; 3], axis: AxisMeridian) -> Self {
        let mut arc = Self::from_test_points(start, end);
        arc.kind = ExactArcKind::AxisMeridian(axis);
        arc.latitude = arc_latitude_span(arc.start, arc.end);
        arc
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct H3PoleOwners {
    // The former north/south-owner widening was redundant: every one of the
    // 16x2 exact pole-owner ancestor chains reaches this generic full-longitude
    // fallback (pinned by `generic_cap_fallback_covers_every_bilateral_pole_owner_ancestor`).
    // Keep only the target coherence token; a second pole path would be dead.
    target: Resolution,
}

impl H3PoleOwners {
    pub(crate) const fn for_target(target: Resolution) -> Self {
        Self { target }
    }
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
/// share it between the fan and the exact logical-bbox certificate.
#[derive(Debug)]
pub(crate) struct H3CellPlan {
    cell: h3o::CellIndex,
    arcs: Option<H3ArcSet>,
    bbox: DegreeWindowResult,
    fan: H3FanPlan,
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

    pub(crate) const fn fan(&self) -> &H3FanPlan {
        &self.fan
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
        let point = project_h3_seed(seed)?;
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
    witnesses.push(certified_h3_point_degree_window(project_h3_seed(center)?)?)?;
    for seed in boundary.projection_seeds().copied() {
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
pub(crate) fn exact_h3_bbox_for_cell(cell: h3o::CellIndex) -> DegreeWindowResult {
    h3_ordered_arcs(cell).map_or(DegreeWindowResult::Boundary, |arcs| exact_h3_bbox(&arcs))
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
            match triangle_point_class(triangle, point) {
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
        let previous = triangle_side(vertex.previous, tangent);
        let next = triangle_side(vertex.next, tangent);
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

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn triangle_point_class(triangle: [Vec3Bound; 3], point: Vec3Bound) -> H3FanPointClass {
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
    h3_cell_plan(cell, H3PoleOwners::for_target(cell.resolution())).fan
}

pub(crate) fn h3_cell_plan(cell: h3o::CellIndex, poles: H3PoleOwners) -> H3CellPlan {
    let arcs = (poles.target == cell.resolution())
        .then(|| h3_ordered_arcs(cell))
        .flatten();
    let fan = arcs.as_ref().map_or(H3FanPlan::Uncertain, |arcs| {
        h3_fan_plan_from_arcs(cell, arcs)
    });
    let bbox = arcs
        .as_ref()
        .map_or(DegreeWindowResult::Boundary, exact_h3_bbox);
    H3CellPlan {
        cell,
        arcs,
        bbox,
        fan,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the fixed fan certificate is one auditable proof pipeline; splitting it would obscure its fail-open exits"
)]
fn h3_fan_plan_from_arcs(cell: h3o::CellIndex, arcs: &H3ArcSet) -> H3FanPlan {
    let Some(center) = cell
        .target_substrate_center_seed(cell.resolution())
        .and_then(project_h3_seed)
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

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn triangle_side(triangle: H3FanTriangle, tangent: Vec3Ball) -> FanSide {
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
/// latitude. Structural endpoint/pole facts are supplied by `GridAffineSource`;
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
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    pub(crate) fn from_parent_map(
        map: AffineParentMap,
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
            map,
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
    const fn is_equatorial(self) -> bool {
        matches!(self.map.structure, AffineStructure::Equator)
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
    let lambda0 = degrees_to_radians(lon0)?;
    let phi0 = degrees_to_radians(lat0)?;
    let lambda1 = degrees_to_radians(lon1)?;
    let phi1 = degrees_to_radians(lat1)?;
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
        map,
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
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn classify_arc_contact(source: AffineSourceArc, arc: &OrderedMinorArc) -> ArcContact {
    classify_arc_contact_with_h3_fan(source, arc, None)
}

/// H3's relation entry point binds a contact to the one certified fan which
/// owns the cell.  A mismatched supplier or an uncertified fan cannot use a
/// structural branch to establish a negative.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn classify_h3_arc_contact(
    source: AffineSourceArc,
    arc: &OrderedMinorArc,
    fan: &H3FanPlan,
) -> ArcContact {
    if !fan.matches_cell(arc.supplier) {
        return ArcContact::Uncertain;
    }
    classify_arc_contact_with_h3_fan(source, arc, Some(fan))
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_arc_contact_with_h3_fan(
    source: AffineSourceArc,
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
    match arc.kind {
        #[cfg(test)]
        ExactArcKind::Equator => classify_with_arc_shifts(source, arc, |source, arc| {
            classify_equator_contact(source, arc, fan)
        }),
        #[cfg(test)]
        ExactArcKind::AxisMeridian(axis) => classify_axis_meridian_contact(source, arc, axis, fan),
        ExactArcKind::General => classify_with_arc_shifts(source, arc, |source, arc| {
            if source.is_vertical() {
                classify_vertical_contact(source, arc)
            } else {
                classify_general_contact(source, arc)
            }
        }),
        ExactArcKind::UnresolvedMeridian => unreachable!("returned before dispatch"),
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_with_arc_shifts(
    source: AffineSourceArc,
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
        contact = contact.combine(classify(source, &shifted));
    }
    contact
}

/// Every periodic copy of a directed cell arc that can meet this exact source
/// piece.  The floating calculation is only a conservative bucket proposal:
/// both endpoints are widened by a whole additional sheet before any contact
/// predicate runs. A source piece wider than the seam-split grammar can bound
/// is ambiguous rather than being searched on an arbitrary fixed three-sheet
/// range.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn source_arc_shift_range(source: AffineSourceArc, arc: &OrderedMinorArc) -> Option<(i64, i64)> {
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
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_matching_pole_contact(
    source: AffineSourceArc,
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

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_zero_source(source: AffineSourceArc, arc: &OrderedMinorArc) -> ArcContact {
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

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_general_contact(source: AffineSourceArc, arc: &OrderedMinorArc) -> ArcContact {
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

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_vertical_contact(source: AffineSourceArc, arc: &OrderedMinorArc) -> ArcContact {
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

#[cfg(test)]
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_equator_contact(
    source: AffineSourceArc,
    arc: &OrderedMinorArc,
    fan: Option<&H3FanPlan>,
) -> ArcContact {
    let (Some(source_lon), Some(arc_lon), Some(phi)) = (
        source.longitude_span(),
        arc.longitude.and_then(DirectedLongitudeSpan::envelope),
        source.latitude_span(),
    ) else {
        return ArcContact::Uncertain;
    };
    let longitude_overlap = source_lon.overlap(arc_lon);
    if source.is_equatorial() {
        return match longitude_overlap {
            LongitudeOverlap::Disjoint => ArcContact::None,
            LongitudeOverlap::Contact { inner: Some(_), .. } => {
                classify_structural_arc_overlap(source, arc, fan, StructuralCoordinate::Longitude)
            },
            LongitudeOverlap::Contact { inner: None, .. } => ArcContact::Uncertain,
        };
    }
    if phi.upper.strictly_positive() || phi.lower.strictly_negative() {
        return ArcContact::None;
    }
    let Some(t) = source.map.phi0.neg().div(source.map.dphi) else {
        return ArcContact::Uncertain;
    };
    if t.hi < source.domain.lo || t.lo > source.domain.hi {
        return ArcContact::None;
    }
    if !strictly_inside_domain(source, t) {
        return ArcContact::Uncertain;
    }
    let Some(longitude) = source.lambda_at(t) else {
        return ArcContact::Uncertain;
    };
    if arc_lon.strictly_contains(longitude) {
        ArcContact::Open
    } else {
        ArcContact::Uncertain
    }
}

#[cfg(test)]
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_axis_meridian_contact(
    source: AffineSourceArc,
    arc: &OrderedMinorArc,
    axis: AxisMeridian,
    fan: Option<&H3FanPlan>,
) -> ArcContact {
    let Some(arc_latitude) = arc.latitude else {
        return ArcContact::Uncertain;
    };
    if source.is_axis(axis) {
        let Some(source_latitude) = source.latitude_span() else {
            return ArcContact::Uncertain;
        };
        return match source_latitude.overlap(arc_latitude) {
            LongitudeOverlap::Disjoint => ArcContact::None,
            LongitudeOverlap::Contact { inner: Some(_), .. } => {
                classify_structural_arc_overlap(source, arc, fan, StructuralCoordinate::Latitude)
            },
            LongitudeOverlap::Contact { inner: None, .. } => ArcContact::Uncertain,
        };
    }
    if source.is_vertical() {
        let Some(source_longitude) = source.longitude_span() else {
            return ArcContact::Uncertain;
        };
        for target in axis.radian_shifts().into_iter().flatten() {
            if source_longitude.lower.intersection(target).is_some() {
                return ArcContact::Uncertain;
            }
        }
        return ArcContact::None;
    }
    let mut found_candidate = false;
    for target in axis.radian_shifts().into_iter().flatten() {
        let Some(t) = target
            .sub(source.map.lambda0)
            .and_then(|delta| delta.div(source.map.dlambda))
        else {
            return ArcContact::Uncertain;
        };
        if t.hi < source.domain.lo || t.lo > source.domain.hi {
            continue;
        }
        found_candidate = true;
        if !strictly_inside_domain(source, t) {
            return ArcContact::Uncertain;
        }
        let Some(latitude) = source.phi_at(t) else {
            return ArcContact::Uncertain;
        };
        if arc_latitude.strictly_contains(latitude) {
            return ArcContact::Open;
        }
        if latitude.hi < arc_latitude.lower.lo || latitude.lo > arc_latitude.upper.hi {
            continue;
        }
        return ArcContact::Uncertain;
    }
    if found_candidate {
        ArcContact::Uncertain
    } else {
        ArcContact::None
    }
}

#[derive(Clone, Copy)]
#[cfg(test)]
enum StructuralCoordinate {
    Longitude,
    Latitude,
}

/// A source segment which is structurally carried by an equator or named
/// meridian may overlap an H3 edge.  Its closed overlap is only ClosedOnly
/// while it stays strictly inside that edge.  A possible cell vertex visit is
/// delegated to the certified two-triangle fan; without that topology the
/// relation must remain visibly ambiguous.
#[cfg(test)]
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_structural_arc_overlap(
    source: AffineSourceArc,
    arc: &OrderedMinorArc,
    fan: Option<&H3FanPlan>,
    coordinate: StructuralCoordinate,
) -> ArcContact {
    let (Some(source_span), Some(arc_span)) = (match coordinate {
        StructuralCoordinate::Longitude => (
            source.longitude_span(),
            arc.longitude.and_then(DirectedLongitudeSpan::envelope),
        ),
        StructuralCoordinate::Latitude => (source.latitude_span(), arc.latitude),
    }) else {
        return ArcContact::Uncertain;
    };

    let mut result = ArcContact::ClosedOnly;
    for (endpoint, key) in [
        (arc_span.lower, arc.vertex_keys[0]),
        (arc_span.upper, arc.vertex_keys[1]),
    ] {
        // Strict exclusion proves that the source never reaches this cell
        // vertex.  Any remaining interval overlap is deliberately treated as
        // a vertex event rather than decided from rounded coordinates.
        if endpoint.hi < source_span.lower.lo || source_span.upper.hi < endpoint.lo {
            continue;
        }
        let Some(fan) = fan else {
            return ArcContact::Uncertain;
        };
        let parameter = match coordinate {
            StructuralCoordinate::Longitude => source.parameter_at_longitude(endpoint),
            StructuralCoordinate::Latitude => source.parameter_at_latitude(endpoint),
        };
        let Some(parameter) = parameter else {
            return ArcContact::Uncertain;
        };
        let side = classify_vertex_sides(source, parameter, key, fan);
        result = result.combine(side);
        if result != ArcContact::ClosedOnly {
            return result;
        }
    }
    result
}

/// Applies every existing one-sided source tangent at a structural H3 vertex.
/// An interior visit has two sides; a source endpoint has one.  Interval
/// uncertainty about which case holds is never collapsed to a local proof.
#[cfg(test)]
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn classify_vertex_sides(
    source: AffineSourceArc,
    parameter: Bound,
    key: CellVertexKey,
    fan: &H3FanPlan,
) -> ArcContact {
    let lower = source.domain.lo;
    let upper = source.domain.hi;
    if parameter.hi < lower || parameter.lo > upper {
        return ArcContact::None;
    }
    let classify = |tangent: Option<Vec3Ball>| {
        tangent.map_or(ArcContact::Uncertain, |tangent| {
            match fan.side_at_vertex(key, tangent) {
                FanSide::Open => ArcContact::Open,
                FanSide::Exterior => ArcContact::ClosedOnly,
                FanSide::Uncertain => ArcContact::Uncertain,
            }
        })
    };
    if parameter.is_exact_value(lower) {
        return classify(source.tangent_at_parameter(parameter, 1));
    }
    if parameter.is_exact_value(upper) {
        return classify(source.tangent_at_parameter(parameter, -1));
    }
    if lower < parameter.lo && parameter.hi < upper {
        let forward = classify(source.tangent_at_parameter(parameter, 1));
        let backward = classify(source.tangent_at_parameter(parameter, -1));
        return forward.combine(backward);
    }
    ArcContact::Uncertain
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn plane_at_longitude(
    source: AffineSourceArc,
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
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn plane_at_overlap_longitude(
    source: AffineSourceArc,
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

#[cfg(test)]
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn strictly_inside_domain(source: AffineSourceArc, parameter: Bound) -> bool {
    source.domain.lo < parameter.lo && parameter.hi < source.domain.hi
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
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn certify_derivative_sign(
    source: AffineSourceArc,
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

#[cfg(test)]
fn degrees_to_radians(value: f64) -> Option<Bound> {
    degree_bound_to_radians(Bound::exact(value)?)
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
    for distance in [1_u16, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096] {
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

#[cfg(test)]
fn arc_latitude_span(start: Vec3Bound, end: Vec3Bound) -> Option<LongitudeSpan> {
    LongitudeSpan::ordered(
        certified_vector_latitude(start)?,
        certified_vector_latitude(end)?,
    )
}

/// Certified latitude enclosure for the named-meridian structural path.  As
/// for longitude, `atan2`/`hypot` are proposals only; signs against the raw
/// vector and certified trigonometry establish the final bracket.
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
    for distance in [1_u16, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096] {
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

fn adjacent_float(mut value: f64, direction: i8, count: u16) -> Option<f64> {
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
        -1 => rank.checked_sub(u64::from(steps))?,
        1 => rank.checked_add(u64::from(steps))?,
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
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn project_h3_seed(seed: ProjectionSeed) -> Option<Vec3Bound> {
    project_h3_seed_unnormalized(seed)?.normalized()
}

/// Certified native H3 projection before the final unit-vector normalization.
/// A3's seam proof compares this affine carrier in its paired face frames;
/// normalizing before subtraction would erase precisely the chart discrepancy
/// the cap must charge.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn project_h3_seed_unnormalized(seed: ProjectionSeed) -> Option<Vec3Bound> {
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
    if substrate {
        scale = scale.mul(Bound::exact(frame.one_third)?)?;
    }
    center.add(basis.scale(scale)?)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn seed_native_point(seed: ProjectionSeed, sqrt3_2: f64) -> Option<[Bound; 2]> {
    let (x2, y_sqrt3) = match seed {
        ProjectionSeed::Point { point, .. } => (
            Bound::from_i128(i128::from(point.x2))?,
            Bound::from_i128(i128::from(point.y_sqrt3))?,
        ),
        ProjectionSeed::Intersection {
            segment, face_edge, ..
        } => intersection_native_point(segment, face_edge)?,
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

/// Bounded high-degree fallback for a wide, but still uniquely reduced,
/// interval.  The next alternating term is included symmetrically, so this
/// path remains a proof instead of a retry loop with a fitted tolerance.
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

fn sin_cos_fallback(input: Bound) -> Option<(Ball, Ball)> {
    // A uniquely chosen quadrant may still enclose an artificial +/-pi/4
    // reduction edge because every pi operation is outward.  The degree-27/28
    // alternating proof remains valid through pi/2, so retain the enclosure
    // instead of silently selecting a rounded side of that non-structural cut.
    if input.lo < -HALF_PI_LO || input.hi > HALF_PI_HI {
        return None;
    }
    let z = input.mul(input)?;
    let sin = input.mul(alternating_horner(z, &[
        (6.0, -1),
        (120.0, 1),
        (5_040.0, -1),
        (362_880.0, 1),
        (39_916_800.0, -1),
        (6_227_020_800.0, 1),
        (1_307_674_368_000.0, -1),
        (355_687_428_096_000.0, 1),
        (121_645_100_408_832_000.0, -1),
        (51_090_942_171_709_440_000.0, 1),
        (FACT_23, -1),
        (FACT_25, 1),
        (FACT_27, -1),
    ])?)?;
    let cos = alternating_horner(z, &[
        (2.0, -1),
        (24.0, 1),
        (720.0, -1),
        (40_320.0, 1),
        (3_628_800.0, -1),
        (479_001_600.0, 1),
        (87_178_291_200.0, -1),
        (20_922_789_888_000.0, 1),
        (6_402_373_705_728_000.0, -1),
        (2_432_902_008_176_640_000.0, 1),
        (FACT_22, -1),
        (FACT_24, 1),
        (FACT_26, -1),
        (FACT_28, 1),
    ])?;
    let absolute = input.abs();
    let sin_remainder = integer_power_bound(absolute, 29)?.mul(reciprocal_factorial(FACT_29)?)?;
    let cos_remainder = integer_power_bound(absolute, 30)?.mul(reciprocal_factorial(FACT_30)?)?;
    let sin = widen_symmetric(sin, sin_remainder)?;
    let cos = widen_symmetric(cos, cos_remainder)?;
    Some((Ball::from_bound(sin)?, Ball::from_bound(cos)?))
}

/// Horner evaluation of `1 + z*(s1/f1 + z*(s2/f2 + ...))`, with each
/// factorial first enclosed as the exact integer it denotes.  This is the
/// fallback's outward interval path, deliberately separate from B3's compact
/// scalar-plus-radius fast filter.
fn alternating_horner(z: Bound, terms: &[(f64, i8)]) -> Option<Bound> {
    let (factorial, sign) = *terms.last()?;
    let mut value = signed_reciprocal_factorial(factorial, sign)?;
    for &(factorial, sign) in terms[..terms.len() - 1].iter().rev() {
        value = signed_reciprocal_factorial(factorial, sign)?.add(z.mul(value)?)?;
    }
    Bound::exact(1.0)?.add(z.mul(value)?)
}

fn signed_reciprocal_factorial(factorial: f64, sign: i8) -> Option<Bound> {
    let value = reciprocal_factorial(factorial)?;
    match sign {
        1 => Some(value),
        -1 => Some(value.neg()),
        _ => None,
    }
}

fn reciprocal_factorial(factorial: f64) -> Option<Bound> {
    if !factorial.is_finite() || factorial <= 0.0 {
        return None;
    }
    // The neighbouring floats enclose the integer factorial even once it is
    // no longer exactly representable as binary64.  Reciprocal division is
    // then outward through Bound::div.
    Bound::exact(1.0)?.div(Bound::new(factorial.next_down(), factorial.next_up())?)
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
mod tests {
    use super::*;

    #[test]
    fn source_endpoint_carrier_certifies_the_seam_contact_exclusion() {
        use crate::geometry::{CoordSeq, LineSeq, Point, Shape};
        use crate::grid::affine_source::{GridAffineSource, SphericalGridTarget};

        let source = GridAffineSource::new(
            &Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
                Point::new(170.0, -10.0).unwrap(),
                Point::new(-170.0, 10.0).unwrap(),
            ]))),
            SphericalGridTarget::H3(Resolution::Seven),
        )
        .unwrap();
        let cell = h3o::CellIndex::try_from(0x0877_EB57_26FF_FFFF_u64).unwrap();
        let source_arc = source.authority_pieces().unwrap().next().unwrap().arc();
        let endpoint = Bound::exact(source_arc.domain.hi).unwrap();
        assert_eq!(
            source_arc.endpoint_identity_at(endpoint),
            Some(AffineEndpointIdentity {
                longitude: Some(AxisMeridian::Antimeridian),
                latitude: Some(AxisLatitude::Equator),
            }),
            "the exact rational seam split, not its rounded enclosure, carries both axes"
        );
        let arcs = h3_ordered_arcs(cell).unwrap();
        // These are the two seam-side pieces whose overlap terminates at the
        // source parent endpoint.  Its outward longitude `hi` is not a new
        // exact longitude on the parent map, so direct inverse evaluation is
        // deliberately unavailable; the endpoint carrier supplies the sign.
        for (index, shift) in [(1, 0), (5, 1)] {
            let arc = arcs
                .get(index)
                .unwrap()
                .with_longitude_shift(shift)
                .unwrap();
            let LongitudeOverlap::Contact { outer, .. } =
                source_arc.longitude_span().unwrap().overlap(
                    arc.longitude
                        .and_then(DirectedLongitudeSpan::envelope)
                        .unwrap(),
                )
            else {
                panic!("fixture must enter the certified seam overlap");
            };
            assert!(
                plane_at_longitude(source_arc, &arc, outer.hi).is_none(),
                "the rounded enclosure endpoint is intentionally not authority"
            );
            let endpoint_plane = plane_at_overlap_longitude(source_arc, &arc, outer.hi);
            assert!(
                endpoint_plane
                    .is_some_and(|value| value.strictly_positive() || value.strictly_negative()),
                "the retained parent endpoint proves the missing strict sign"
            );
            assert_eq!(
                classify_general_contact(source_arc, &arc),
                ArcContact::None,
                "the endpoint carrier excludes the non-contacting seam cell"
            );
        }
    }

    #[test]
    fn arc_contact_searches_the_source_piece_own_periodic_sheet() {
        let source = test_affine_source_arc(800.0, 0.0, 900.0, 0.0)
            .expect("a finite lifted equatorial source piece");
        let arc = OrderedMinorArc::from_test_points([0.0, 1.0, 0.0], [-1.0, 0.0, 0.0]);
        let (first, last) = source_arc_shift_range(source, &arc)
            .expect("the finite source piece has a bounded periodic-copy range");
        assert!(
            first <= 2 && 2 <= last,
            "the 800..900 degree source piece needs the arc's +2 sheet"
        );
        assert_ne!(
            classify_arc_contact(source, &arc),
            ArcContact::None,
            "the exact relation must not turn a remote periodic source contact into a negative"
        );
    }

    #[test]
    fn target_lattice_profile_encloses_every_vendor_suffix_and_rotation() {
        let target = Resolution::Fifteen;
        let raw = CapLatticeProfile::for_target(target);
        let sqrt3_2 = certified_profile_sqrt3_2(&raw, 16)
            .expect("all paired profile seams preserve the native constant");
        for resolution in Resolution::range(Resolution::One, target) {
            for (index, point) in raw.suffix_offsets(resolution).unwrap().iter().enumerate() {
                assert!(
                    certified_lattice_norm(*point, sqrt3_2).is_some(),
                    "target suffix {resolution:?} offset {index} must have an outward norm"
                );
            }
            assert!(
                certified_lattice_max(raw.suffix_offsets(resolution).unwrap(), sqrt3_2).is_some(),
                "target suffix {resolution:?} must stay finite"
            );
        }
        assert!(certified_lattice_max(raw.vertex_offsets(), sqrt3_2).is_some());
        let half = Bound::exact(0.5).unwrap();
        let b = Bound::exact(-3.0)
            .unwrap()
            .div(Bound::exact(4.0).unwrap().mul(sqrt3_2).unwrap())
            .unwrap();
        let g00 = half
            .square()
            .unwrap()
            .add_nonnegative(sqrt3_2.square().unwrap())
            .unwrap();
        let g01 = half
            .mul(b)
            .unwrap()
            .add(sqrt3_2.mul(half).unwrap())
            .unwrap();
        let g11 = b
            .square()
            .unwrap()
            .add_nonnegative(half.square().unwrap())
            .unwrap();
        let discriminant_input = g00
            .sub(g11)
            .unwrap()
            .square()
            .unwrap()
            .add_nonnegative(
                Bound::exact(4.0)
                    .unwrap()
                    .mul_nonnegative(g01.square().unwrap())
                    .unwrap(),
            )
            .unwrap();
        let discriminant = discriminant_input
            .sqrt()
            .expect("the non-negative Gram discriminant has an outward root");
        let eigenvalue = g00
            .add_nonnegative(g11)
            .unwrap()
            .add_nonnegative(discriminant)
            .unwrap()
            .div_nonnegative(Bound::exact(2.0).unwrap())
            .unwrap();
        eigenvalue
            .sqrt()
            .expect("the upper Gram eigenvalue has an outward root");
        assert!(certified_native_rotation_norm(sqrt3_2).is_some());
        let profile = CertifiedTargetLatticeProfile::for_target(target)
            .expect("the fixed vendor target profile is finite");
        let at_root = profile
            .native_descendant_radius(Resolution::Zero)
            .expect("root is an ancestor of the target");
        let at_leaf = profile
            .native_descendant_radius(target)
            .expect("target is its own ancestor");
        assert!(at_root.hi > at_leaf.hi);
        assert!(at_leaf.hi > 0.0);
        assert!(profile.rotation_norm().hi > 0.0);
        assert_eq!(profile.raw().seams().len(), 60);
        assert_eq!(
            profile
                .raw()
                .suffix_offsets(Resolution::Fifteen)
                .unwrap()
                .len(),
            42
        );
    }
    #[test]
    fn descendant_cap_certifies_every_target_depth_below_a_hemisphere() {
        for target in Resolution::range(Resolution::Zero, Resolution::Fifteen) {
            let lattice = CertifiedTargetLatticeProfile::for_target(target)
                .unwrap_or_else(|| panic!("target {target:?} needs a finite lattice profile"));
            let scale = certified_profile_scale(lattice.raw())
                .unwrap_or_else(|| panic!("target {target:?} needs a finite projection scale"));
            for (index, seam) in lattice.raw().seams().iter().enumerate() {
                for seed in seam.left().into_iter().chain(seam.right()) {
                    let certificate =
                        certified_face_projection(seed, lattice.raw().max_dim(), scale)
                            .unwrap_or_else(|| {
                                panic!(
                                    "target {target:?} seam {index} needs a finite face projection"
                                )
                            });
                    assert!(
                        certificate.minimum_denominator.strictly_positive(),
                        "target {target:?} seam {index} needs a positive native-C projection denominator"
                    );
                    assert!(
                        certificate.basis_norm.hi > 0.0,
                        "target {target:?} seam {index} needs a non-degenerate projection basis"
                    );
                }
            }
            let stretch = certified_chart_stretch(lattice.raw(), scale)
                .unwrap_or_else(|| panic!("target {target:?} needs a finite chart stretch"));
            assert!(stretch.hi > 0.0);
            let seam_cost = certified_seam_cost(lattice.raw(), scale)
                .unwrap_or_else(|| panic!("target {target:?} needs a finite seam cost"));
            assert!(seam_cost.hi > 0.0);
            let cap = CertifiedH3DescendantCap::for_target(target).unwrap_or_else(|| {
                panic!("target {target:?} needs a finite projection and seam cap")
            });
            assert_eq!(cap.target(), target);
            for depth in Resolution::range(Resolution::Zero, target) {
                let ideal = certified_ideal_face_ratio(target, depth).unwrap_or_else(|| {
                    panic!("target {target:?}, depth {depth:?} needs a strict face-crossing proof")
                });
                assert!(ideal.hi < 1.0);
                let radius = cap.angular_radius(depth).unwrap_or_else(|| {
                    panic!("target {target:?}, depth {depth:?} needs a sub-hemisphere cap")
                });
                assert!(radius.hi < HALF_PI_LO);
            }
        }
    }

    #[test]
    fn ideal_face_crossing_stays_strict_at_the_pinned_profile_depths() {
        for (depth, target) in [(0, 1), (0, 2), (1, 3), (4, 5), (4, 10), (0, 15), (14, 15)] {
            let target = Resolution::try_from(target).expect("stored H3 resolution");
            let depth = Resolution::try_from(depth).expect("stored H3 resolution");
            let ratio = certified_ideal_face_ratio(target, depth)
                .expect("the stored finite profile proves fewer than three face-family crossings");
            assert!(ratio.hi < 1.0, "target {target:?}, depth {depth:?}");
        }
    }

    #[test]
    fn descendant_window_certificate_rejects_unprofiled_centers() {
        for target in Resolution::range(Resolution::Zero, Resolution::Fifteen) {
            let cap = CertifiedH3DescendantCap::for_target(target)
                .unwrap_or_else(|| panic!("target {target:?} has an atomic cap"));
            let invalid = Hex2Int::new(6 * cap.max_dim + 2, 0);
            assert!(
                !certified_center_seed_is_admissible(
                    invalid,
                    cap.adjusted_resolution,
                    true,
                    cap.adjusted_resolution,
                    cap.max_dim,
                ),
                "an IJK point beyond the certified face domain must not enter a window certificate"
            );
        }
    }

    #[test]
    fn generic_cap_fallback_covers_every_bilateral_pole_owner_ancestor() {
        const POLE_OWNERS: [(u64, u64); H3_RESOLUTION_COUNT] = [
            (0x0800_1FFF_FFFF_FFFF, 0x080F_3FFF_FFFF_FFFF),
            (0x0810_33FF_FFFF_FFFF, 0x081F_2BFF_FFFF_FFFF),
            (0x0820_327F_FFFF_FFFF, 0x082F_297F_FFFF_FFFF),
            (0x0830_326F_FFFF_FFFF, 0x083F_293F_FFFF_FFFF),
            (0x0840_3263_FFFF_FFFF, 0x084F_2939_FFFF_FFFF),
            (0x0850_3262_3FFF_FFFF, 0x085F_2938_3FFF_FFFF),
            (0x0860_3262_37FF_FFFF, 0x086F_2938_0FFF_FFFF),
            (0x0870_3262_33FF_FFFF, 0x087F_2938_0EFF_FFFF),
            (0x0880_3262_33BF_FFFF, 0x088F_2938_0E1F_FFFF),
            (0x0890_3262_33AB_FFFF, 0x089F_2938_0E0F_FFFF),
            (0x08A0_3262_33AB_7FFF, 0x08AF_2938_0E0D_7FFF),
            (0x08B0_3262_33AB_0FFF, 0x08BF_2938_0E0D_0FFF),
            (0x08C0_3262_33AB_03FF, 0x08CF_2938_0E0D_0DFF),
            (0x08D0_3262_33AB_03BF, 0x08DF_2938_0E0D_0CFF),
            (0x08E0_3262_33AB_039F, 0x08EF_2938_0E0D_0CC7),
            (0x08F0_3262_33AB_0399, 0x08FF_2938_0E0D_0CC4),
        ];
        for (target, (north, south)) in
            Resolution::range(Resolution::Zero, Resolution::Fifteen).zip(POLE_OWNERS)
        {
            let cap = CertifiedH3DescendantCap::for_target(target).unwrap();
            for (owner, northern) in [(north, true), (south, false)] {
                let owner = h3o::CellIndex::try_from(owner).unwrap();
                for depth in Resolution::range(Resolution::Zero, target) {
                    let cell = owner.parent(depth).unwrap();
                    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
                        latitude,
                        longitude: CertifiedLongitudeDegrees::Full,
                    }) = cap.descendant_windows(cell, H3PoleOwners::for_target(target))
                    else {
                        panic!("{target:?} {depth:?} must take the generic pole fallback");
                    };
                    assert!(if northern {
                        latitude.hi >= 90.0
                    } else {
                        latitude.lo <= -90.0
                    });
                }
            }
        }
    }

    #[test]
    fn cap_profile_coupling_rejects_a_synthetic_larger_face() {
        let target = Resolution::Ten;
        let adjusted = target_value(target).unwrap() + u8::from(target.is_class3());
        let dimension = 2 * 7_i64.pow(u32::from(adjusted / 2));
        assert!(!certified_profile_is_coupled(
            target,
            adjusted,
            dimension + 1,
            60,
            target,
        ));
    }

    #[test]
    fn cap_latitude_envelope_is_outward_before_it_can_prune() {
        let latitude = Bound::exact(0.1).unwrap();
        let radius = Bound::exact(0.2).unwrap();
        let (south, north, absolute) = certified_cap_latitude_bounds(latitude, radius).unwrap();
        let rounded_sum = 0.1 + 0.2;
        assert!(south.lo < -0.1 && south.hi > -0.1);
        assert!(north.lo < rounded_sum && north.hi > rounded_sum);
        assert!(absolute.lo < rounded_sum && absolute.hi > rounded_sum);
    }

    #[test]
    fn strict_face_crossing_refuses_an_exact_unit_ratio() {
        assert_eq!(
            certified_strict_face_ratio(Bound::exact(1.0).unwrap()),
            None
        );
    }

    #[test]
    fn angular_radius_composition_keeps_three_seam_charges_and_k_cubed() {
        let radius = certified_angular_radius(
            Bound::exact(2.0).unwrap(),
            Bound::exact(3.0).unwrap(),
            Bound::exact(125.0).unwrap(),
            Bound::exact(7.0).unwrap(),
            Bound::exact(11.0).unwrap(),
        )
        .unwrap();
        assert!(radius.lo < 5283.0 && radius.hi > 5283.0);
    }

    #[test]
    fn six_native_rotation_powers_have_frozen_outward_norms() {
        let raw = CapLatticeProfile::for_target(Resolution::Fifteen);
        let sqrt3_2 = certified_profile_sqrt3_2(&raw, 16).unwrap();
        let powers = certified_native_rotation_power_norms(sqrt3_2).unwrap();
        assert_eq!(powers.map(|norm| norm.hi.to_bits()), [
            0x3FF0_0000_0000_0003,
            0x3FF0_0000_0000_0007,
            0x3FF0_0000_0000_000C,
            0x3FF0_0000_0000_000F,
            0x3FF0_0000_0000_001D,
            0x3FF0_0000_0000_002A,
        ]);
    }

    #[test]
    fn raw_c_projection_radicands_stay_positive_at_pinned_face_and_resolution_pairs() {
        const RADICAND_HI_BITS: [u64; 5] = [
            0x3FF0_0000_0000_0003,
            0x3FF0_0000_0000_0004,
            0x3FF0_0000_0000_0003,
            0x3FF0_0000_0000_0003,
            0x3FF0_0000_0000_0003,
        ];
        for ((face, adjusted), expected_bits) in [(0, 0_u8), (7, 1), (19, 2), (2, 10), (15, 16)]
            .into_iter()
            .zip(RADICAND_HI_BITS)
        {
            let seed = ProjectionSeed::Point {
                face,
                point: Hex2Int::new(0, 0),
                resolution: adjusted,
                substrate: true,
            };
            let max_dim = 2 * 7_i64.pow(u32::from(adjusted / 2));
            let raw = CapLatticeProfile::for_target(Resolution::Zero);
            let scale = certified_profile_scale(&raw).unwrap();
            let certificate = certified_face_projection(seed, max_dim, scale).unwrap();
            assert!(certificate.raw_c_radicand.strictly_positive());
            assert_eq!(certificate.raw_c_radicand.hi.to_bits(), expected_bits);
        }
    }

    #[test]
    fn ordered_arc_latitude_includes_an_interior_analytic_extremum() {
        // The plane is y=z.  Its north-axis projection is the interior ray
        // (0, 1, 1), above both unit endpoint rays (±1/3, 2/3, 2/3).
        let arc = OrderedMinorArc::from_test_points([1.0 / 3.0, 2.0 / 3.0, 2.0 / 3.0], [
            -1.0 / 3.0,
            2.0 / 3.0,
            2.0 / 3.0,
        ]);
        let endpoints = [
            certified_vector_latitude(arc.start).unwrap(),
            certified_vector_latitude(arc.end).unwrap(),
        ];
        let bounds = certified_arc_latitude_bounds(&arc).unwrap();
        assert!(bounds.hi > endpoints[0].hi.max(endpoints[1].hi));
        assert!(bounds.hi > HALF_PI_LO * 0.5);
    }

    #[test]
    fn polar_h3_witness_has_an_arc_latitude_extremum_beyond_its_vertices() {
        let cell = h3o::CellIndex::try_from(0x0820_377F_FFFF_FFFF_u64).unwrap();
        let arcs = h3_ordered_arcs(cell).unwrap();
        let mut vertex_max = -HALF_PI_HI;
        let mut arc_max = -HALF_PI_HI;
        for arc in arcs.iter() {
            for endpoint in [arc.start, arc.end] {
                vertex_max = vertex_max.max(certified_vector_latitude(endpoint).unwrap().hi);
            }
            arc_max = arc_max.max(certified_arc_latitude_bounds(arc).unwrap().hi);
        }
        assert!(arc_max > vertex_max);
    }

    #[test]
    fn directed_h3_bbox_retains_the_vendor_2pi_sheet() {
        // The R0 polar root is Full; the next three roots exercise each
        // canonical seam form.  The remaining cells force the same carrier
        // through ordinary, polar-extremum, pentagon, and Class-III paths.
        // Every assertion calls the bbox certificate directly, before any
        // traversal can turn uncertainty into a conservative candidate.
        for (raw, expected) in [
            (0x0800_1FFF_FFFF_FFFF_u64, 0_u8),
            // Ordinary non-pentagon away from both poles.  It crosses the
            // degree seam, so erasing the accumulated lift makes this exact
            // bbox fail open to `Boundary`.
            (0x0805_BFFF_FFFF_FFFF_u64, 2_u8),
            (0x0800_3FFF_FFFF_FFFF_u64, 2_u8),
            (0x0800_5FFF_FFFF_FFFF_u64, 2_u8),
            (0x0800_DFFF_FFFF_FFFF_u64, 2_u8),
            (0x0810_1BFF_FFFF_FFFF_u64, 1_u8),
            (0x0820_377F_FFFF_FFFF_u64, 1_u8),
            (0x0827_54FF_FFFF_FFFF_u64, 1_u8),
            (0x0830_136F_FFFF_FFFF_u64, 1_u8),
        ] {
            let cell = h3o::CellIndex::try_from(raw).unwrap();
            let arcs = h3_ordered_arcs(cell).unwrap();
            let DegreeWindowResult::Windows(CertifiedDegreeWindows { longitude, .. }) =
                exact_h3_bbox(&arcs)
            else {
                panic!("{cell:?} must retain a certifiable directed longitude sheet");
            };
            assert_eq!(
                match longitude {
                    CertifiedLongitudeDegrees::Full => 0,
                    CertifiedLongitudeDegrees::One(_) => 1,
                    CertifiedLongitudeDegrees::Two(_) => 2,
                },
                expected,
                "{cell:?} must retain its canonical degree seam form"
            );
        }
    }

    #[test]
    fn r15_raw_seam_carrier_pins_matching_and_reversed_endpoint_extrema() {
        // These are RAW, unnormalised H3 projection carriers at R15/adjusted
        // substrate R16.  They are not the degree-LatLng/unit-XYZ roundtrip.
        let raw = CapLatticeProfile::for_target(Resolution::Fifteen);
        assert_eq!(raw.adjusted_resolution(), 16);
        let mut matching_max = Bound::exact(0.0).unwrap();
        for (left, right) in certified_matching_seam_pairs(&raw) {
            let delta = certified_seam_delta(left, right).unwrap();
            matching_max = Bound::new(0.0, matching_max.hi.max(delta.hi)).unwrap();
        }
        let mut reversed_min = Bound::new(f64::MAX, f64::MAX).unwrap();
        for seam in raw.seams() {
            for (left, right) in seam.left().into_iter().zip(seam.right().into_iter().rev()) {
                let delta = certified_seam_delta(left, right).unwrap();
                reversed_min = Bound::new(reversed_min.lo.min(delta.lo), f64::MAX).unwrap();
            }
        }
        assert_eq!(matching_max.hi.to_bits(), 0x3D41_32D6_5721_E925);
        assert_eq!(reversed_min.lo.to_bits(), 0x3FF5_2BB3_566A_4667);
    }

    #[test]
    fn descendant_windows_use_the_target_substrate_center_for_named_spills() {
        let parent = h3o::CellIndex::try_from(0x0800_9FFF_FFFF_FFFF_u64).unwrap();
        for raw in [0x0810_93FF_FFFF_FFFF_u64, 0x0810_97FF_FFFF_FFFF_u64] {
            let target = h3o::CellIndex::try_from(raw).unwrap();
            assert_eq!(target.parent(parent.resolution()), Some(parent));
        }
        let cap = CertifiedH3DescendantCap::for_target(Resolution::One).unwrap();
        assert!(
            CertifiedCapCenter::for_target_cell(
                parent,
                Resolution::One,
                cap.adjusted_resolution,
                cap.max_dim,
            )
            .is_some(),
            "the cap must enter the target-child substrate path, not the ordinary parent centre"
        );

        // The former `Boundary`/`Windows` oracle failed under the ordinary
        // parent mutation for an accidental longitude-envelope reason.  These
        // rays coincide exactly, so only target-child provenance observes the
        // aperture-7 distinction.
        let required = Hex2Int::new(84, 0);
        let ordinary_parent = Hex2Int::new(12, 0);
        assert!(certified_center_seed_is_admissible(
            required,
            2,
            true,
            cap.adjusted_resolution,
            cap.max_dim,
        ));
        assert!(!certified_center_seed_is_admissible(
            ordinary_parent,
            0,
            true,
            cap.adjusted_resolution,
            cap.max_dim,
        ));
    }

    #[test]
    fn cap_radius_uses_its_certified_outer_halfwidth_not_an_arc_endpoint_pair() {
        let target = Resolution::Two;
        let cap = CertifiedH3DescendantCap::for_target(target).unwrap();
        let windows = h3o::CellIndex::base_cells()
            .filter(|&cell| {
                matches!(
                    cap.descendant_windows(cell, H3PoleOwners::for_target(target)),
                    DegreeWindowResult::Windows(_)
                )
            })
            .count();
        assert_eq!(
            windows, 122,
            "a correlated [0, R] cap radius retains every native r2 root window"
        );
    }

    #[test]
    fn cap_halfwidth_uses_the_correlated_upper_endpoint() {
        let center = Bound::exact(0.0).unwrap();
        let radius = Bound::new(0.0, 1.0).unwrap();
        assert!(
            matches!(
                cap_longitude_span(center, radius),
                Some(CertifiedLongitudeRad::Span(_))
            ),
            "the cap's upper radius endpoint creates an ordered outward span"
        );
    }

    #[test]
    fn center_certificate_requires_the_substrate_carrier_and_target_resolution() {
        let point = Hex2Int::new(0, 0);
        assert!(!certified_center_seed_is_admissible(point, 2, false, 2, 14));
        assert!(!certified_center_seed_is_admissible(point, 1, true, 2, 14));
    }

    #[test]
    fn bounds_are_outward_and_division_refuses_zero() {
        let one = Bound::exact(1.0).unwrap();
        let three = Bound::exact(3.0).unwrap();
        let quotient = one.div(three).unwrap();
        assert!(quotient.lo < 1.0 / 3.0 && quotient.hi > 1.0 / 3.0);
        assert!(one.div(Bound::new(-1.0, 1.0).unwrap()).is_none());
    }

    #[test]
    fn square_root_requires_outward_multiplication_witnesses() {
        let four = Bound::exact(4.0).unwrap();
        assert!(verified_sqrt_enclosure(four, 2.0_f64.next_up(), 3.0).is_none());
        assert!(verified_sqrt_enclosure(four, 1.0, 2.0_f64.next_down()).is_none());

        let root = Bound::new(2.0, 9.0).unwrap().sqrt().unwrap();
        assert!(root.lo <= 2.0_f64.sqrt() && 3.0_f64.sqrt() <= root.hi);
        assert_eq!(Bound::exact(0.0).unwrap().sqrt(), Bound::exact(0.0));
    }

    #[test]
    fn degree_windows_are_outward_and_refuse_an_ambiguous_seam() {
        let radians = |degrees| degrees_to_radians(degrees).unwrap();
        let split = CertifiedCircularBoundsRad {
            latitude: radians(10.0),
            longitude: CertifiedLongitudeRad::Span(
                LongitudeSpan::ordered(radians(170.0), radians(190.0)).unwrap(),
            ),
        };
        let DegreeWindowResult::Windows(CertifiedDegreeWindows {
            latitude,
            longitude: CertifiedLongitudeDegrees::Two([west, east]),
        }) = split.to_degree_windows()
        else {
            panic!("a strictly ordered +180 crossing has two certified windows");
        };
        assert!(latitude.lo <= 10.0 && 10.0 <= latitude.hi);
        assert!(west.lo <= 170.0 && 180.0 <= west.hi);
        assert!(east.lo <= -180.0 && -170.0 <= east.hi);

        let seam = CertifiedCircularBoundsRad {
            latitude: Bound::exact(0.0).unwrap(),
            longitude: CertifiedLongitudeRad::Span(
                LongitudeSpan::ordered(radians(180.0), radians(180.0)).unwrap(),
            ),
        };
        assert_eq!(seam.to_degree_windows(), DegreeWindowResult::Boundary);
    }

    #[test]
    fn directed_longitude_refuses_a_lower_pi_boundary_enclosure() {
        // `-PI_LO` is the stored-double upper bound of -π.  Treating it as
        // strictly inside would let a half-circle establish a directed sheet.
        let lower_boundary = Bound::exact(-PI_LO).unwrap();
        assert!(!strict_minor_longitude_delta(lower_boundary));
        assert!(strict_minor_longitude_delta(Bound::exact(0.0).unwrap()));
    }

    #[test]
    fn ball_sign_never_decides_through_zero() {
        assert_eq!(Ball::new(1.0, 0.25).unwrap().sign(), CertSign::Positive);
        assert_eq!(Ball::new(-1.0, 0.25).unwrap().sign(), CertSign::Negative);
        assert_eq!(Ball::new(0.0, 0.25).unwrap().sign(), CertSign::Uncertain);
    }

    #[test]
    fn trig_encloses_fast_path_without_libm_in_the_carrier() {
        for value in [-0.7, -0.2, 0.0, 0.2, 0.7, 1.0, -1.0] {
            let (sin, cos) = sin_cos(Bound::exact(value).unwrap()).unwrap();
            assert!(sin.bound().unwrap().lo <= value.sin());
            assert!(sin.bound().unwrap().hi >= value.sin());
            assert!(cos.bound().unwrap().lo <= value.cos());
            assert!(cos.bound().unwrap().hi >= value.cos());
        }
    }

    #[test]
    fn trig_rejects_enclosed_quadrant_boundary() {
        let boundary = Bound::new(HALF_PI_LO, HALF_PI_HI).unwrap();
        assert!(sin_cos(boundary).is_none());
    }

    #[test]
    fn trig_rejects_an_enclosed_period_boundary_but_keeps_exact_zero() {
        assert!(sin_cos(degrees_to_radians(360.0).unwrap()).is_none());
        let (sin, cos) = sin_cos(Bound::exact(0.0).unwrap()).expect("structural zero");
        assert!(sin.bound().unwrap().contains_zero());
        assert!(cos.bound().unwrap().lo <= 1.0 && cos.bound().unwrap().hi >= 1.0);
    }

    #[test]
    fn trig_fallback_keeps_an_outward_pi_over_four_enclosure() {
        let input = degrees_to_radians(45.0).unwrap();
        let (sin, cos) =
            sin_cos(input).expect("the degree conversion only spans an artificial pi/4 cut");
        let value = (PI_LO + PI_HI) * 0.125;
        assert!(sin.bound().unwrap().lo <= value.sin());
        assert!(sin.bound().unwrap().hi >= value.sin());
        assert!(cos.bound().unwrap().lo <= value.cos());
        assert!(cos.bound().unwrap().hi >= value.cos());
    }

    #[test]
    fn contact_order_is_fail_open() {
        assert_eq!(
            ArcContact::None.combine(ArcContact::ClosedOnly),
            ArcContact::ClosedOnly
        );
        assert_eq!(
            ArcContact::ClosedOnly.combine(ArcContact::Uncertain),
            ArcContact::Uncertain
        );
        assert_eq!(
            ArcContact::Uncertain.combine(ArcContact::Open),
            ArcContact::Open
        );
    }

    #[test]
    fn h3_raw_seed_projects_without_roundtripping_through_latlng() {
        let cell = h3o::CellIndex::try_from(0x0810_1BFF_FFFF_FFFF_u64).unwrap();
        let boundary = cell.boundary_with_seeds();
        for (seed, public) in boundary.projection_seeds().zip(boundary.public.iter()) {
            let point = project_h3_seed(*seed).expect("stored native seed is projectable");
            for component in [point.x, point.y, point.z] {
                assert!(component.lo.is_finite() && component.hi.is_finite());
            }
            let expected = [
                public.lat_radians().cos() * public.lng_radians().cos(),
                public.lat_radians().cos() * public.lng_radians().sin(),
                public.lat_radians().sin(),
            ];
            for (actual, expected) in [point.x, point.y, point.z].into_iter().zip(expected) {
                assert!(
                    actual.lo <= expected && expected <= actual.hi,
                    "seed={seed:?}, certified={actual:?}, public={expected:.17e}"
                );
            }
        }
    }

    #[test]
    fn non_substrate_class_iii_q_rotation_replays_native_projector() {
        // H3 boundary seeds are substrate points.  This native seed exercises
        // the separate Class-III Q branch directly; `native_projection_point`
        // is a test oracle only, never a predicate input.
        let seed = ProjectionSeed::Point {
            face: 0,
            point: Hex2Int::new(48, -10),
            resolution: 1,
            substrate: false,
        };
        let actual = project_h3_seed(seed).expect("finite native seed");
        let native = seed.native_projection_point();
        let expected = [
            native.lat_radians().cos() * native.lng_radians().cos(),
            native.lat_radians().cos() * native.lng_radians().sin(),
            native.lat_radians().sin(),
        ];
        for (component, expected) in [actual.x, actual.y, actual.z].into_iter().zip(expected) {
            assert!(
                component.lo <= expected && expected <= component.hi,
                "certified={component:?}, native={expected:.17e}"
            );
        }
    }

    #[test]
    fn raw_intersection_parameter_is_positive_and_pins_horizontal_half() {
        // segment x=0..2 crossed by the vertical face edge x=1 has t=+1/2.
        let (numerator, denominator) =
            rational_intersection_parameter(0, 4, 2, 0, 1, -2).expect("proper crossing");
        assert_eq!((numerator, denominator), (1, 2));
        let point = intersection_native_point([Hex2Int::new(0, 0), Hex2Int::new(2, 0)], [
            Hex2Int::new(1, -2),
            Hex2Int::new(1, 2),
        ])
        .expect("proper native intersection");
        assert!(point.0.lo < 1.0 && point.0.hi > 1.0);
        assert!(point.1.contains_zero());
    }

    #[test]
    fn ordered_h3_arcs_preserve_class_iii_face_insertions() {
        let cell = h3o::CellIndex::try_from(0x0810_1BFF_FFFF_FFFF_u64).unwrap();
        let boundary = cell.boundary_with_seeds();
        let arcs = h3_ordered_arcs(cell).expect("every stored H3 boundary seed is an ordered arc");

        assert_eq!(boundary.public.len(), 7);
        assert_eq!(arcs.len(), boundary.public.len());
        assert_eq!(arcs.iter().count(), arcs.len());
        assert!(arcs.len() <= MAX_H3_BOUNDARY_ARCS);
        assert!(matches!(
            arcs.get(6).expect("closing arc").endpoints[1],
            PhysicalEndpointKey::CellVertex(CellVertexKey::H3 { ordinal: 0, .. })
        ));
        assert!(arcs.iter().any(|arc| {
            arc.endpoints.iter().any(|endpoint| {
                matches!(
                    endpoint,
                    PhysicalEndpointKey::CellVertex(CellVertexKey::H3Insertion { .. })
                )
            })
        }));
        assert!(matches!(
            arcs.get(5).expect("preceding arc").endpoints[1],
            PhysicalEndpointKey::CellVertex(CellVertexKey::H3Insertion {
                edge: 5,
                ordinal: 6,
                ..
            })
        ));
        let plan = h3_cell_plan(cell, H3PoleOwners::for_target(cell.resolution()));
        assert_eq!(plan.cell, cell);
        assert_eq!(plan.arcs.as_ref().map(H3ArcSet::len), Some(arcs.len()));
        assert!(matches!(plan.bbox, DegreeWindowResult::Windows(_)));
        assert!(matches!(plan.fan, H3FanPlan::Certified(_)));
        let mismatched = h3_cell_plan(cell, H3PoleOwners::for_target(Resolution::Two));
        assert!(mismatched.arcs.is_none());
        assert_eq!(mismatched.bbox, DegreeWindowResult::Boundary);
        assert!(matches!(mismatched.fan, H3FanPlan::Uncertain));
        assert!(matches!(h3_fan_plan(cell), H3FanPlan::Certified(_)));
    }

    #[test]
    fn ordered_h3_arcs_certify_ordinary_pentagon_class_iii_and_both_poles() {
        let cells = [
            h3o::CellIndex::try_from(0x0827_54FF_FFFF_FFFF_u64).unwrap(),
            h3o::CellIndex::try_from(0x0810_1BFF_FFFF_FFFF_u64).unwrap(),
            h3o::CellIndex::try_from(0x0830_136F_FFFF_FFFF_u64).unwrap(),
            h3o::CellIndex::try_from(0x0810_83FF_FFFF_FFFF_u64).unwrap(),
            h3o::CellIndex::try_from(0x0820_327F_FFFF_FFFF_u64).unwrap(),
            h3o::CellIndex::try_from(0x082F_297F_FFFF_FFFF_u64).unwrap(),
        ];
        for cell in cells {
            let raw = cell.boundary_with_seeds();
            let arcs = h3_ordered_arcs(cell).expect("raw ordered H3 boundary is certified");
            assert_eq!(arcs.len(), raw.public.len());
            assert_eq!(arcs.iter().count(), arcs.len());
            assert!(arcs.len() <= MAX_H3_BOUNDARY_ARCS);
            assert!(matches!(h3_fan_plan(cell), H3FanPlan::Certified(_)));
        }
    }

    #[test]
    fn h3_reflex_vertex_uses_adjacent_triangle_union() {
        for raw in [0x0810_1BFF_FFFF_FFFF_u64, 0x0830_136F_FFFF_FFFF_u64] {
            let cell = h3o::CellIndex::try_from(raw).unwrap();
            let arcs = h3_ordered_arcs(cell).expect("raw H3 arcs");
            let center = project_h3_seed(
                cell.target_substrate_center_seed(cell.resolution())
                    .expect("target substrate centre"),
            )
            .expect("certified centre");
            let vertex = arcs.get(6).expect("fixture reflex arc");
            let previous = arcs.get(5).expect("fixture preceding arc");
            let candidate = vertex
                .start
                .scale(Bound::exact(0.999).unwrap())
                .and_then(|value| {
                    center
                        .scale(Bound::exact(0.0005).unwrap())
                        .and_then(|center| value.add(center))
                })
                .and_then(|value| {
                    previous
                        .start
                        .scale(Bound::exact(0.0005).unwrap())
                        .and_then(|previous| value.add(previous))
                })
                .expect("finite reflex tangent proposal");
            let tangent = Vec3Ball {
                x: Ball::from_bound(candidate.x).unwrap(),
                y: Ball::from_bound(candidate.y).unwrap(),
                z: Ball::from_bound(candidate.z).unwrap(),
            };
            let PhysicalEndpointKey::CellVertex(key) = vertex.endpoints[0] else {
                panic!("fixture ordinal six is a structural H3 insertion");
            };
            let plan = h3_fan_plan(cell);
            assert_eq!(
                plan.side_at_vertex(key, tangent),
                FanSide::Open,
                "the previous fan triangle admits the real Class-III reflex motion"
            );
        }
    }

    #[test]
    fn zero_straddling_fan_diagonal_never_promotes_a_strict_real_edge() {
        // The real cell edge is strictly inward, while the artificial center
        // diagonal contains zero.  Only the latter is ambiguous: treating it
        // as open would manufacture a source/cell contact from the fan's
        // triangulation rather than from a physical H3 boundary.
        let triangle = H3FanTriangle {
            real_edge: Vec3Bound::exact([1.0, 0.0, 0.0]).unwrap(),
            diagonal: Vec3Bound::exact([0.0, 1.0, 0.0]).unwrap(),
        };
        let tangent = Vec3Ball {
            x: Ball::exact(1.0).unwrap(),
            y: Ball::new(0.0, 1.0).unwrap(),
            z: Ball::exact(0.0).unwrap(),
        };
        assert_eq!(triangle_side(triangle, tangent), FanSide::Uncertain);
    }

    #[test]
    fn transverse_affine_edge_is_open_and_exterior_edge_is_none() {
        let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
        let transverse =
            test_affine_source_arc(40.0, -10.0, 40.0, 10.0).expect("finite source degrees");
        let exterior =
            test_affine_source_arc(-40.0, -10.0, -40.0, 10.0).expect("finite source degrees");

        assert_eq!(classify_arc_contact(transverse, &arc), ArcContact::Open);
        assert_eq!(classify_arc_contact(exterior, &arc), ArcContact::None);
    }

    #[test]
    fn affine_parent_map_keeps_structure_and_endpoint_roles_explicit() {
        let general_zero_longitude = AffineParentMap {
            lambda0: Bound::exact(0.0).unwrap(),
            phi0: Bound::exact(0.0).unwrap(),
            dlambda: Bound::exact(0.0).unwrap(),
            dphi: Bound::exact(1.0).unwrap(),
            structure: AffineStructure::General,
        };
        let source = AffineSourceArc::from_parent_map(
            general_zero_longitude,
            Bound::new(0.0, 1.0).unwrap(),
            [SourceEndpointRole::Partition, SourceEndpointRole::ParentEnd],
            [None, Some(PhysicalEndpointKey::NorthPole)],
            [AffineEndpointIdentity::default(); 2],
        )
        .unwrap();
        assert!(!source.is_vertical());
        assert!(!source.is_axis(AxisMeridian::Prime));
        assert_eq!(source.endpoint_pole(0), None);
        assert_eq!(
            source.endpoint_pole(1),
            Some(PhysicalEndpointKey::NorthPole)
        );

        let inconsistent_constant = AffineParentMap {
            dlambda: Bound::exact(1.0).unwrap(),
            structure: AffineStructure::ConstantLongitude {
                axis: Some(AxisMeridian::Prime),
            },
            ..general_zero_longitude
        };
        assert!(
            AffineSourceArc::from_parent_map(
                inconsistent_constant,
                Bound::new(0.0, 1.0).unwrap(),
                [
                    SourceEndpointRole::ParentStart,
                    SourceEndpointRole::ParentEnd
                ],
                [None, None],
                [AffineEndpointIdentity::default(); 2],
            )
            .is_none()
        );
        assert!(
            AffineSourceArc::from_parent_map(
                general_zero_longitude,
                Bound::new(0.0, 1.0).unwrap(),
                [SourceEndpointRole::Partition, SourceEndpointRole::ParentEnd],
                [Some(PhysicalEndpointKey::NorthPole), None],
                [AffineEndpointIdentity::default(); 2],
            )
            .is_none()
        );
    }

    #[test]
    fn general_relation_tries_the_three_certified_seam_shifts() {
        let radians = |degrees: f64| degrees.to_radians();
        let arc =
            OrderedMinorArc::from_test_points([radians(170.0).cos(), radians(170.0).sin(), 0.0], [
                radians(-170.0).cos(),
                radians(-170.0).sin(),
                0.0,
            ]);
        let source = test_affine_source_arc(180.0, -1.0, 180.0, 1.0).unwrap();
        assert_eq!(
            classify_arc_contact(source, &arc),
            ArcContact::Open,
            "the +2pi copy is the only inner longitude overlap"
        );
    }

    #[test]
    fn tangent_and_unresolved_plane_fail_open() {
        let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
        assert_eq!(
            arc.kind,
            ExactArcKind::General,
            "only an explicit supplier tag may enter the equator branch"
        );
        let tangent = test_affine_source_arc(20.0, 0.0, 70.0, 10.0).expect("finite source degrees");
        assert_eq!(classify_arc_contact(tangent, &arc), ArcContact::Uncertain);

        let unresolved = OrderedMinorArc::from_test_points([0.0, 0.0, 1.0], [1.0, 0.0, 0.0]);
        assert_eq!(unresolved.kind, ExactArcKind::UnresolvedMeridian);
        assert_eq!(
            classify_arc_contact(tangent, &unresolved),
            ArcContact::Uncertain
        );
    }

    #[test]
    fn structural_equator_axis_and_zero_source_paths_are_explicit() {
        let mut equator = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
        equator.kind = ExactArcKind::Equator;
        let coincident = test_affine_source_arc(20.0, 0.0, 30.0, -0.0).unwrap();
        assert_eq!(
            classify_arc_contact(coincident, &equator),
            ArcContact::ClosedOnly
        );

        let point = test_affine_source_arc(45.0, 0.0, 45.0, 0.0).unwrap();
        assert_eq!(classify_arc_contact(point, &equator), ArcContact::Uncertain);

        let meridian =
            OrderedMinorArc::from_test_axis([1.0, 0.0, 0.0], [0.0, 0.0, 1.0], AxisMeridian::Prime);
        let transverse = test_affine_source_arc(-10.0, 40.0, 10.0, 40.0).unwrap();
        assert_eq!(
            classify_arc_contact(transverse, &meridian),
            ArcContact::Open
        );

        let mut north_arc =
            OrderedMinorArc::from_test_axis([0.0, 0.0, 1.0], [1.0, 0.0, 0.0], AxisMeridian::Prime);
        north_arc.endpoints[0] = PhysicalEndpointKey::NorthPole;
        let from_north = test_affine_source_arc(45.0, 90.0, 45.0, 80.0).unwrap();
        assert_eq!(
            classify_arc_contact(from_north, &north_arc),
            ArcContact::Uncertain,
            "a shared pole cannot use one supporting plane instead of an H3 fan"
        );

        let mut vertex_touch = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
        vertex_touch.kind = ExactArcKind::Equator;
        let reaches_vertex = test_affine_source_arc(0.0, 0.0, 20.0, 0.0).unwrap();
        assert_eq!(
            classify_arc_contact(reaches_vertex, &vertex_touch),
            ArcContact::Uncertain,
            "a coincident equator edge that reaches a cell vertex needs its H3 fan sides"
        );
    }

    #[test]
    fn finite_extrema_certificate_controls_general_negative() {
        let arc = OrderedMinorArc::from_test_points([1.0, 0.0, 0.0], [0.0, 1.0, 1.0]);
        let source = test_affine_source_arc(30.0, 40.0, 60.0, 45.0).unwrap();
        let lambda = source.lambda_at(source.domain).unwrap();
        assert_eq!(
            certify_derivative_sign(source, &arc, lambda),
            Some(CertSign::Negative),
            "this reaches B4's all-box strict derivative proof"
        );
        assert_eq!(
            classify_arc_contact(source, &arc),
            ArcContact::None,
            "the generic branch consumes the B4 certificate for a negative"
        );

        let too_wide = test_affine_source_arc(-100.0, 40.0, 100.0, 45.0).unwrap();
        assert_eq!(
            certify_derivative_sign(too_wide, &arc, too_wide.lambda_at(too_wide.domain).unwrap()),
            None,
            "a source span beyond the one-minor-arc precondition cannot certify D"
        );
    }
}
