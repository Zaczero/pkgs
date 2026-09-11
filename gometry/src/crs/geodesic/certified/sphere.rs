use super::{
    Degree, Dyadic, Enclosure, ExactBinary64, PrecisionContext, RoundingCell, Sign, VectorSector,
    principal_longitude_difference, sin_cos_degrees, vector_angle_bracket,
};
use crate::crs::CrsError;
use crate::error::Result;

/// The source coordinates of one spherical endpoint.  The degree tokens keep
/// the original binary64 bits available for every precision replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SphericalPoint {
    longitude: Degree,
    latitude: Degree,
}

impl SphericalPoint {
    pub(crate) fn from_f64(longitude: f64, latitude: f64) -> Result<Self> {
        if !longitude.is_finite()
            || !latitude.is_finite()
            || !(-180.0..=180.0).contains(&longitude)
            || !(-90.0..=90.0).contains(&latitude)
        {
            return Err(CrsError::message(
                "certified spherical geodesic coordinates must be finite longitude in [-180, 180] \
                 and latitude in [-90, 90]",
            ));
        }
        Ok(Self {
            longitude: Degree::from_f64(longitude)?,
            latitude: Degree::from_f64(latitude)?,
        })
    }

    pub(crate) fn from_bits(longitude: u64, latitude: u64) -> Result<Self> {
        Self::from_f64(f64::from_bits(longitude), f64::from_bits(latitude))
    }

    pub(crate) const fn longitude_bits(self) -> u64 {
        self.longitude.bits()
    }

    pub(crate) const fn latitude_bits(self) -> u64 {
        self.latitude.bits()
    }
}

/// Exact source for the spherical radius.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SphericalModel {
    radius: ExactBinary64,
}

impl SphericalModel {
    pub(crate) fn from_f64(radius: f64) -> Result<Self> {
        if !radius.is_finite() || radius <= 0.0 {
            return Err(CrsError::message(
                "certified spherical geodesic radius must be finite and positive",
            ));
        }
        Ok(Self {
            radius: ExactBinary64::from_f64(radius)?,
        })
    }

    pub(crate) fn from_bits(radius: u64) -> Result<Self> {
        Self::from_f64(f64::from_bits(radius))
    }

    pub(crate) const fn radius_bits(self) -> u64 {
        self.radius.bits()
    }
}

/// All source material for one certified spherical inverse attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SphericalInput {
    start: SphericalPoint,
    end: SphericalPoint,
    model: SphericalModel,
}

impl SphericalInput {
    pub(crate) fn from_f64(start: (f64, f64), end: (f64, f64), radius: f64) -> Result<Self> {
        Ok(Self {
            start: SphericalPoint::from_f64(start.0, start.1)?,
            end: SphericalPoint::from_f64(end.0, end.1)?,
            model: SphericalModel::from_f64(radius)?,
        })
    }

    pub(crate) fn from_bits(start: (u64, u64), end: (u64, u64), radius: u64) -> Result<Self> {
        Ok(Self {
            start: SphericalPoint::from_bits(start.0, start.1)?,
            end: SphericalPoint::from_bits(end.0, end.1)?,
            model: SphericalModel::from_bits(radius)?,
        })
    }

    pub(crate) const fn start(self) -> SphericalPoint {
        self.start
    }

    pub(crate) const fn end(self) -> SphericalPoint {
        self.end
    }

    pub(crate) const fn model(self) -> SphericalModel {
        self.model
    }
}

/// The one canonicalization used by the spherical leaf.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Canonicalization {
    OriginalDegreesToUnitVector,
}

/// The topological branch of a spherical inverse.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SphericalBranch {
    Coincident,
    Antipodal,
    UniqueMinorArc,
}

/// The multiplicity or zero-length property carried by a branch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SphericalMultiplicity {
    ZeroLength,
    InfiniteMinimizers,
    UniqueMinimizer,
}

impl SphericalBranch {
    pub(crate) const fn multiplicity(self) -> SphericalMultiplicity {
        match self {
            Self::Coincident => SphericalMultiplicity::ZeroLength,
            Self::Antipodal => SphericalMultiplicity::InfiniteMinimizers,
            Self::UniqueMinorArc => SphericalMultiplicity::UniqueMinimizer,
        }
    }
}

/// Retained identity of the branch that produced a published result.
///
/// The root is the certified central-angle bracket.  The source coordinates
/// and model, rather than any rounded output, are the replay recipe.
#[derive(Debug)]
pub(crate) struct SphericalBranchIdentity {
    input: SphericalInput,
    canonicalization: Canonicalization,
    branch: SphericalBranch,
    principal_longitude: Option<Dyadic>,
    root: Enclosure,
}

impl SphericalBranchIdentity {
    pub(crate) const fn input(&self) -> SphericalInput {
        self.input
    }

    pub(crate) const fn canonicalization(&self) -> Canonicalization {
        self.canonicalization
    }

    pub(crate) const fn branch(&self) -> SphericalBranch {
        self.branch
    }

    pub(crate) const fn root(&self) -> &Enclosure {
        &self.root
    }

    pub(crate) const fn principal_longitude(&self) -> Option<&Dyadic> {
        self.principal_longitude.as_ref()
    }
}

/// A binary64 result retained together with the interval that proved its
/// rounding cell.
#[derive(Debug)]
pub(crate) struct CertifiedBinary64 {
    bits: u64,
    enclosure: Enclosure,
}

impl CertifiedBinary64 {
    pub(crate) fn from_enclosure(
        enclosure: Enclosure,
        context: &mut PrecisionContext,
    ) -> Result<Self> {
        let bits = match enclosure.rounding_cell(context)? {
            RoundingCell::Unique(bits) => bits,
            RoundingCell::Overflow => {
                if enclosure.hi().sign() == Sign::Negative {
                    (1_u64 << 63) | (0x7FF_u64 << 52)
                } else {
                    0x7FF_u64 << 52
                }
            },
            RoundingCell::Indeterminate => {
                return Err(super::numerical_indeterminacy_error());
            },
        };
        Ok(Self { bits, enclosure })
    }

    pub(crate) const fn bits(&self) -> u64 {
        self.bits
    }

    pub(crate) const fn value(&self) -> f64 {
        f64::from_bits(self.bits)
    }

    pub(crate) const fn enclosure(&self) -> &Enclosure {
        &self.enclosure
    }
}

/// Certified spherical inverse result.
///
/// Coincident endpoints have an exact zero distance and no azimuth.
/// Antipodal endpoints have distance `pi * radius`, but no unique azimuth.
/// Only the minor-arc branch carries an azimuth.
#[derive(Debug)]
pub(crate) struct SphericalGeodesic {
    identity: SphericalBranchIdentity,
    distance: CertifiedBinary64,
    azimuth_radians: Option<CertifiedBinary64>,
}

impl SphericalGeodesic {
    pub(crate) const fn identity(&self) -> &SphericalBranchIdentity {
        &self.identity
    }

    pub(crate) const fn branch(&self) -> SphericalBranch {
        self.identity.branch
    }

    pub(crate) const fn multiplicity(&self) -> SphericalMultiplicity {
        self.identity.branch.multiplicity()
    }

    pub(crate) const fn distance(&self) -> &CertifiedBinary64 {
        &self.distance
    }

    pub(crate) const fn azimuth_radians(&self) -> Option<&CertifiedBinary64> {
        self.azimuth_radians.as_ref()
    }
}

/// Certify the spherical inverse problem from retained exact source inputs.
///
/// A `NumericalIndeterminacy` result means that this attempt did not prove a
/// branch, angle, or final binary64 rounding cell under the supplied context.
/// The caller must construct a fresh context and replay this input rather than
/// promoting an intermediate enclosure.
pub(crate) fn classify_spherical_geodesic(
    input: SphericalInput,
    context: &mut PrecisionContext,
) -> Result<SphericalGeodesic> {
    let (relation, principal_longitude) = source_relation(input, context)?;
    match relation {
        SourceRelation::Coincident => {
            return structural_result(
                input,
                SphericalBranch::Coincident,
                principal_longitude,
                context,
            );
        },
        SourceRelation::Antipodal => {
            return structural_result(
                input,
                SphericalBranch::Antipodal,
                principal_longitude,
                context,
            );
        },
        SourceRelation::Distinct => {},
    }

    let start = canonical_point(input.start, context)?;
    let end = canonical_point(input.end, context)?;
    let dot = vector_dot(&start.vector, &end.vector, context)?;
    let cross = vector_cross(&start.vector, &end.vector, context)?;
    let cross_squared = vector_norm_squared(&cross, context)?;
    let central_sine = cross_squared.sqrt(context)?;
    if central_sine.hi().is_zero() {
        return Err(super::numerical_indeterminacy_error());
    }
    let root = positive_angle_from_components(&dot, &central_sine, context)?;
    let distance = publish_distance(&root, input.model, context)?;

    let principal_longitude_ref = principal_longitude.as_ref();
    let tangent = initial_tangent(&start, &end, principal_longitude_ref, context)?;
    let azimuth = publish_angle(tangent, context)?;
    let identity_root = root.try_clone(context)?;
    Ok(SphericalGeodesic {
        identity: SphericalBranchIdentity {
            input,
            canonicalization: Canonicalization::OriginalDegreesToUnitVector,
            branch: SphericalBranch::UniqueMinorArc,
            principal_longitude,
            root: identity_root,
        },
        distance,
        azimuth_radians: Some(azimuth),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceRelation {
    Coincident,
    Antipodal,
    Distinct,
}

fn source_relation(
    input: SphericalInput,
    context: &mut PrecisionContext,
) -> Result<(SourceRelation, Option<Dyadic>)> {
    let start_latitude = input.start.latitude.exact(context)?;
    let end_latitude = input.end.latitude.exact(context)?;
    let ninety = Dyadic::from_i128(90, context)?;
    let negative_ninety = ninety.try_clone(context)?.negate();
    let start_pole = start_latitude == ninety || start_latitude == negative_ninety;
    let end_pole = end_latitude == ninety || end_latitude == negative_ninety;

    if start_pole || end_pole {
        let relation = if start_pole && end_pole {
            if start_latitude == end_latitude {
                SourceRelation::Coincident
            } else if end_latitude == start_latitude.negate() {
                SourceRelation::Antipodal
            } else {
                SourceRelation::Distinct
            }
        } else {
            SourceRelation::Distinct
        };
        if start_pole && end_pole {
            return Ok((relation, None));
        }
        let principal_longitude =
            principal_longitude_difference(input.start.longitude, input.end.longitude, context)?;
        return Ok((relation, Some(principal_longitude)));
    }

    // This exact subtraction is retained in the result.  It is deliberately
    // performed before any trigonometric reduction, including at the seam.
    let principal_longitude =
        principal_longitude_difference(input.start.longitude, input.end.longitude, context)?;
    let same_latitude = start_latitude == end_latitude;
    let opposite_latitude = end_latitude == start_latitude.negate();
    let negative_half_turn = Dyadic::from_i128(-180, context)?;
    let relation = if same_latitude && principal_longitude.is_zero() {
        SourceRelation::Coincident
    } else if opposite_latitude && principal_longitude == negative_half_turn {
        SourceRelation::Antipodal
    } else {
        SourceRelation::Distinct
    };
    Ok((relation, Some(principal_longitude)))
}

fn structural_result(
    input: SphericalInput,
    branch: SphericalBranch,
    principal_longitude: Option<Dyadic>,
    context: &mut PrecisionContext,
) -> Result<SphericalGeodesic> {
    let root = match branch {
        SphericalBranch::Coincident => Enclosure::exact(Dyadic::zero(), context)?,
        SphericalBranch::Antipodal => super::pi_bracket(context)?,
        SphericalBranch::UniqueMinorArc => {
            return Err(CrsError::message(
                "a structural spherical result must not use the minor-arc branch",
            ));
        },
    };
    let distance = publish_distance(&root, input.model, context)?;
    Ok(SphericalGeodesic {
        identity: SphericalBranchIdentity {
            input,
            canonicalization: Canonicalization::OriginalDegreesToUnitVector,
            branch,
            principal_longitude,
            root,
        },
        distance,
        azimuth_radians: None,
    })
}

#[derive(Debug)]
struct CanonicalPoint {
    longitude_sin: Enclosure,
    longitude_cos: Enclosure,
    latitude_sin: Enclosure,
    latitude_cos: Enclosure,
    vector: UnitVector,
}

#[derive(Debug)]
struct UnitVector {
    x: Enclosure,
    y: Enclosure,
    z: Enclosure,
}

fn canonical_point(
    point: SphericalPoint,
    context: &mut PrecisionContext,
) -> Result<CanonicalPoint> {
    let longitude = sin_cos_degrees(point.longitude, context)?;
    let latitude = sin_cos_degrees(point.latitude, context)?;
    let longitude_sin = longitude.sin().try_clone(context)?;
    let longitude_cos = longitude.cos().try_clone(context)?;
    let latitude_sin = latitude.sin().try_clone(context)?;
    let latitude_cos = latitude.cos().try_clone(context)?;
    let x = longitude.cos().mul(latitude.cos(), context)?;
    let y = longitude.sin().mul(latitude.cos(), context)?;
    let z = latitude.sin().try_clone(context)?;
    Ok(CanonicalPoint {
        longitude_sin,
        longitude_cos,
        latitude_sin,
        latitude_cos,
        vector: UnitVector { x, y, z },
    })
}

fn vector_dot(
    left: &UnitVector,
    right: &UnitVector,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let x = left.x.mul(&right.x, context)?;
    let y = left.y.mul(&right.y, context)?;
    let z = left.z.mul(&right.z, context)?;
    x.add(&y, context)?.add(&z, context)
}

fn vector_cross(
    left: &UnitVector,
    right: &UnitVector,
    context: &mut PrecisionContext,
) -> Result<UnitVector> {
    let x = left
        .y
        .mul(&right.z, context)?
        .sub(&left.z.mul(&right.y, context)?, context)?;
    let y = left
        .z
        .mul(&right.x, context)?
        .sub(&left.x.mul(&right.z, context)?, context)?;
    let z = left
        .x
        .mul(&right.y, context)?
        .sub(&left.y.mul(&right.x, context)?, context)?;
    Ok(UnitVector { x, y, z })
}

fn vector_norm_squared(vector: &UnitVector, context: &mut PrecisionContext) -> Result<Enclosure> {
    let x = vector.x.mul(&vector.x, context)?;
    let y = vector.y.mul(&vector.y, context)?;
    let z = vector.z.mul(&vector.z, context)?;
    x.add(&y, context)?.add(&z, context)
}

/// Bracket `atan2(y, x)` for a proved nonnegative `y` interval.  The
/// component interval is a safe rectangle.  The central distance uses a
/// separately certified alternating arctangent reduction so an unresolved
/// vector-angle probe does not prevent an otherwise publishable length.
fn positive_angle_from_components(
    x: &Enclosure,
    y: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    if y.lo().sign() == Sign::Negative || y.hi().sign() != Sign::Positive {
        return Err(super::numerical_indeterminacy_error());
    }
    let x_sign = x.sign().ok_or_else(super::numerical_indeterminacy_error)?;
    if x_sign == Sign::Zero {
        return certified_corner_angle(
            &Dyadic::zero(),
            y.hi(),
            VectorSector::PositiveInner,
            context,
        )
        .and_then(|angle| angle.enclosure().try_clone(context));
    }

    let denominator = if x_sign == Sign::Positive {
        x.try_clone(context)?
    } else {
        negate_enclosure(x, context)?
    };
    let ratio = y.div(&denominator, context)?;
    let angle = atan_nonnegative(&ratio, context)?;
    if x_sign == Sign::Positive {
        Ok(angle)
    } else {
        let pi = super::pi_bracket(context)?;
        pi.scale_pow2(-1, context)?.sub(&angle, context)
    }
}

fn negate_enclosure(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    Enclosure::new(
        value.hi().try_clone(context)?.negate(),
        value.lo().try_clone(context)?.negate(),
    )
    .ok_or_else(super::numerical_indeterminacy_error)
}

fn atan_nonnegative(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    if value.lo().sign() == Sign::Negative {
        return Err(super::numerical_indeterminacy_error());
    }
    let one = Dyadic::one(context)?;
    let half = one.scale_pow2(-1, context)?;
    if value.hi() <= &half {
        return atan_series(value, context);
    }
    if value.lo() >= &half && value.hi() <= &one {
        return atan_half_to_one(value, context);
    }
    // Every recursive reduction strictly decreases the interval's
    // arctangent range.  Splits at half and one are entered only when the
    // split point is a strict interior point; the reciprocal identity also
    // owns the exact-one boundary (`[1, b]` becomes `[1/b, 1]`) instead of
    // reproducing `[1, b]`.  The remaining exact boundaries are handled by
    // the non-recursive series and half-to-one branches above.
    if value.lo() >= &one {
        let reciprocal = Enclosure::exact(one.try_clone(context)?, context)?.div(value, context)?;
        let small = atan_nonnegative(&reciprocal, context)?;
        return super::pi_bracket(context)?
            .scale_pow2(-1, context)?
            .sub(&small, context);
    }
    if value.hi() > &one {
        // The lower branch ends at one; the inclusive reciprocal guard above
        // ensures the upper branch cannot call this split with the same
        // `[1, b]` interval.
        let lower = Enclosure::new(value.lo().try_clone(context)?, one.try_clone(context)?)
            .ok_or_else(super::numerical_indeterminacy_error)?;
        let upper = Enclosure::new(one.try_clone(context)?, value.hi().try_clone(context)?)
            .ok_or_else(super::numerical_indeterminacy_error)?;
        return atan_nonnegative(&lower, context)?
            .hull(&atan_nonnegative(&upper, context)?, context);
    }

    // `value` crosses one half.  The two identities meet at the exact
    // boundary and avoid the slowly convergent alternating series above it.
    let lower = Enclosure::new(value.lo().try_clone(context)?, half.try_clone(context)?)
        .ok_or_else(super::numerical_indeterminacy_error)?;
    let upper = Enclosure::new(half.try_clone(context)?, value.hi().try_clone(context)?)
        .ok_or_else(super::numerical_indeterminacy_error)?;
    atan_nonnegative(&lower, context)?.hull(&atan_nonnegative(&upper, context)?, context)
}

fn atan_half_to_one(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    let one = Dyadic::one(context)?;
    let one_interval = Enclosure::exact(one.try_clone(context)?, context)?;
    let numerator = value.sub(&one_interval, context)?;
    let denominator = value.add(&one_interval, context)?;
    let reduced = numerator.div(&denominator, context)?;
    let remainder = atan_series(&reduced, context)?;
    super::pi_bracket(context)?
        .scale_pow2(-2, context)?
        .add(&remainder, context)
}

fn atan_series(value: &Enclosure, context: &mut PrecisionContext) -> Result<Enclosure> {
    let target =
        Dyadic::one(context)?.scale_pow2(-i64::from(context.active_precision_bits()), context)?;
    let square = value.mul(value, context)?;
    let mut power = value.try_clone(context)?;
    let mut sum = Enclosure::exact(Dyadic::zero(), context)?;
    let mut index = 0_u64;

    loop {
        context.consume_fuel(1)?;
        let odd = index
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(super::numerical_indeterminacy_error)?;
        let denominator = Dyadic::from_u64(odd, context)?;
        let term = power.div(
            &Enclosure::exact(denominator.try_clone(context)?, context)?,
            context,
        )?;
        sum = if index % 2 == 0 {
            sum.add(&term, context)?
        } else {
            sum.sub(&term, context)?
        };

        let next_index = index
            .checked_add(1)
            .ok_or_else(super::numerical_indeterminacy_error)?;
        power = power.mul(&square, context)?;
        let next_odd = next_index
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(super::numerical_indeterminacy_error)?;
        let next_denominator = Dyadic::from_u64(next_odd, context)?;
        let radius = upper_abs_quotient(&power, &next_denominator, context)?;
        if radius <= target {
            return widen_symmetric(&sum, &radius, context);
        }
        index = next_index;
    }
}

fn upper_abs_quotient(
    value: &Enclosure,
    denominator: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Dyadic> {
    let lower = if value.lo().sign() == Sign::Negative {
        value.lo().try_clone(context)?.negate()
    } else {
        value.lo().try_clone(context)?
    };
    let upper = if value.hi().sign() == Sign::Negative {
        value.hi().try_clone(context)?.negate()
    } else {
        value.hi().try_clone(context)?
    };
    let maximum = if lower >= upper { lower } else { upper };
    let quotient = maximum.divide(denominator, context)?;
    quotient.hi().try_clone(context)
}

fn widen_symmetric(
    value: &Enclosure,
    radius: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    Enclosure::new(
        radius.try_clone(context)?.negate(),
        radius.try_clone(context)?,
    )
    .ok_or_else(super::numerical_indeterminacy_error)
    .and_then(|tail| value.add(&tail, context))
}

fn certified_corner_angle(
    x: &Dyadic,
    y: &Dyadic,
    sector: VectorSector,
    context: &mut PrecisionContext,
) -> Result<super::AngleBracket> {
    if x.is_zero() && y.is_zero() {
        return Err(super::numerical_indeterminacy_error());
    }
    let angle = vector_angle_bracket(x, y, sector, context)?;
    if angle.is_certified() {
        Ok(angle)
    } else {
        Err(super::numerical_indeterminacy_error())
    }
}

fn initial_tangent(
    start: &CanonicalPoint,
    end: &CanonicalPoint,
    principal_longitude: Option<&Dyadic>,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let longitude = longitude_delta_sine_cosine(
        start,
        end,
        principal_longitude.ok_or_else(super::numerical_indeterminacy_error)?,
        context,
    )?;
    let y = longitude.sin.mul(&end.latitude_cos, context)?;
    let x = start.latitude_cos.mul(&end.latitude_sin, context)?.sub(
        &start
            .latitude_sin
            .mul(&end.latitude_cos, context)?
            .mul(&longitude.cos, context)?,
        context,
    )?;
    signed_angle_from_components(&x, &y, context)
}

#[derive(Debug)]
struct LongitudeSineCosine {
    sin: Enclosure,
    cos: Enclosure,
}

fn longitude_delta_sine_cosine(
    start: &CanonicalPoint,
    end: &CanonicalPoint,
    principal_longitude: &Dyadic,
    context: &mut PrecisionContext,
) -> Result<LongitudeSineCosine> {
    let zero = Dyadic::zero();
    let one = Dyadic::one(context)?;
    if principal_longitude == &zero {
        return Ok(LongitudeSineCosine {
            sin: Enclosure::exact(zero, context)?,
            cos: Enclosure::exact(one, context)?,
        });
    }
    let ninety = Dyadic::from_i128(90, context)?;
    if principal_longitude == &ninety {
        return Ok(LongitudeSineCosine {
            sin: Enclosure::exact(one.try_clone(context)?, context)?,
            cos: Enclosure::exact(zero, context)?,
        });
    }
    let negative_ninety = ninety.try_clone(context)?.negate();
    if principal_longitude == &negative_ninety {
        return Ok(LongitudeSineCosine {
            sin: Enclosure::exact(one.try_clone(context)?.negate(), context)?,
            cos: Enclosure::exact(zero, context)?,
        });
    }
    let negative_180 = Dyadic::from_i128(-180, context)?;
    if principal_longitude == &negative_180 {
        return Ok(LongitudeSineCosine {
            sin: Enclosure::exact(zero, context)?,
            cos: Enclosure::exact(one.negate(), context)?,
        });
    }
    let sin = end.longitude_sin.mul(&start.longitude_cos, context)?.sub(
        &end.longitude_cos.mul(&start.longitude_sin, context)?,
        context,
    )?;
    let cos = end.longitude_cos.mul(&start.longitude_cos, context)?.add(
        &end.longitude_sin.mul(&start.longitude_sin, context)?,
        context,
    )?;
    Ok(LongitudeSineCosine { sin, cos })
}

fn signed_angle_from_components(
    x: &Enclosure,
    y: &Enclosure,
    context: &mut PrecisionContext,
) -> Result<Enclosure> {
    let x_sign = x.sign().ok_or_else(super::numerical_indeterminacy_error)?;
    let y_sign = y.sign().ok_or_else(super::numerical_indeterminacy_error)?;
    let sector = vector_sector(x_sign, y_sign)?;
    let mut result: Option<Enclosure> = None;
    for x_value in [x.lo(), x.hi()] {
        for y_value in [y.lo(), y.hi()] {
            let angle = certified_corner_angle(x_value, y_value, sector, context)?;
            result = Some(match result {
                Some(current) => current.hull(angle.enclosure(), context)?,
                None => angle.enclosure().try_clone(context)?,
            });
        }
    }
    result.ok_or_else(super::numerical_indeterminacy_error)
}

fn vector_sector(x: Sign, y: Sign) -> Result<VectorSector> {
    match (x, y) {
        (Sign::Positive, Sign::Positive) | (Sign::Zero, Sign::Positive) => {
            Ok(VectorSector::PositiveInner)
        },
        (Sign::Negative, Sign::Positive) => Ok(VectorSector::PositiveOuter),
        (Sign::Positive, Sign::Negative) | (Sign::Zero, Sign::Negative) => {
            Ok(VectorSector::NegativeInner)
        },
        (Sign::Negative, Sign::Negative) => Ok(VectorSector::NegativeOuter),
        (Sign::Positive, Sign::Zero) => Ok(VectorSector::PositiveInner),
        (Sign::Negative, Sign::Zero) => Ok(VectorSector::PositiveOuter),
        (Sign::Zero, Sign::Zero) => Err(super::numerical_indeterminacy_error()),
    }
}

fn publish_distance(
    angle: &Enclosure,
    model: SphericalModel,
    context: &mut PrecisionContext,
) -> Result<CertifiedBinary64> {
    let radius = model.radius.exact(context)?;
    let radius = Enclosure::exact(radius, context)?;
    let distance = angle.mul(&radius, context)?;
    publish(distance, context)
}

fn publish_angle(angle: Enclosure, context: &mut PrecisionContext) -> Result<CertifiedBinary64> {
    publish(angle, context)
}

fn publish(enclosure: Enclosure, context: &mut PrecisionContext) -> Result<CertifiedBinary64> {
    CertifiedBinary64::from_enclosure(enclosure, context)
}

#[cfg(test)]
mod tests {
    use super::super::{AngleBracketStatus, Enclosure, PrecisionPolicy, Sign, pi_bracket};
    use super::*;
    use crate::error::{ErrorKind, ResourceError};
    use crate::test_support::fail_allocations_after;

    fn context_at(bits: u32) -> PrecisionContext {
        PrecisionContext::new(PrecisionPolicy::new(
            bits,
            bits,
            128_000_000,
            256 * 1024 * 1024,
        ))
        .expect("test policy")
    }

    fn width(value: &Enclosure, context: &mut PrecisionContext) -> Dyadic {
        value
            .hi()
            .sub_scratch(value.lo(), context)
            .expect("enclosure width")
    }

    fn assert_contains_rational_bounds(
        value: &Enclosure,
        lower_numerator: i128,
        upper_numerator: i128,
        denominator: i128,
        context: &mut PrecisionContext,
    ) {
        let denominator = Dyadic::from_i128(denominator, context).expect("oracle denominator");
        let lower = Dyadic::from_i128(lower_numerator, context).expect("oracle lower");
        let upper = Dyadic::from_i128(upper_numerator, context).expect("oracle upper");
        let lower_product = value
            .lo()
            .mul_scratch(&denominator, context)
            .expect("lower oracle product");
        let upper_product = value
            .hi()
            .mul_scratch(&denominator, context)
            .expect("upper oracle product");
        assert!(lower_product < lower);
        assert!(upper_product > upper);
    }

    fn input(start: (f64, f64), end: (f64, f64)) -> SphericalInput {
        SphericalInput::from_f64(start, end, 6_378_137.0).expect("spherical input")
    }

    fn classify(start: (f64, f64), end: (f64, f64), bits: u32) -> Result<SphericalGeodesic> {
        let mut context = context_at(bits);
        classify_spherical_geodesic(input(start, end), &mut context)
    }

    fn assert_unique(result: &CertifiedBinary64) {
        assert!(result.enclosure().lo() <= &result.enclosure().hi());
        assert_eq!(
            result
                .enclosure()
                .rounding_cell(&mut context_at(256))
                .expect("published cell"),
            RoundingCell::Unique(result.bits())
        );
    }

    #[test]
    fn coincident_endpoints_report_zero_without_an_azimuth() {
        let result = classify((10.0, 20.0), (10.0, 20.0), 128).expect("coincident branch");
        assert_eq!(result.branch(), SphericalBranch::Coincident);
        assert_eq!(result.multiplicity(), SphericalMultiplicity::ZeroLength);
        assert_eq!(result.distance().bits(), 0);
        assert!(result.azimuth_radians().is_none());
        assert_eq!(
            result.identity().canonicalization(),
            Canonicalization::OriginalDegreesToUnitVector
        );
        assert_eq!(result.identity().root().lo().sign(), Sign::Zero);
        assert_eq!(result.identity().root().hi().sign(), Sign::Zero);
    }

    #[test]
    fn antipodal_endpoints_include_the_pole_pair_and_report_infinite_minimizers() {
        for (start, end) in [
            ((10.0, 20.0), (-170.0, -20.0)),
            ((0.0, 90.0), (137.0, -90.0)),
        ] {
            let result = classify(start, end, 128).expect("antipodal branch");
            assert_eq!(result.branch(), SphericalBranch::Antipodal);
            assert_eq!(
                result.multiplicity(),
                SphericalMultiplicity::InfiniteMinimizers
            );
            assert!(result.azimuth_radians().is_none());
            assert!(result.identity().root().lo() < result.identity().root().hi());
            assert_unique(result.distance());
        }
    }

    #[test]
    fn near_antipodal_endpoints_are_not_classified_as_antipodal() {
        let result = classify((10.0, 20.0), (-170.0, -20.0_f64.next_up()), 256)
            .expect("near-antipodal minor branch");
        assert_eq!(result.branch(), SphericalBranch::UniqueMinorArc);
        assert_eq!(
            result.multiplicity(),
            SphericalMultiplicity::UniqueMinimizer
        );
        assert!(result.azimuth_radians().is_some());
        assert_unique(result.distance());
    }

    #[test]
    fn antimeridian_principal_difference_is_retained_before_trigonometry() {
        let result = classify((179.0, 0.0), (-179.0, 0.0), 128).expect("antimeridian minor branch");
        let longitude = result
            .identity()
            .principal_longitude()
            .expect("retained longitude");
        let mut context = context_at(128);
        assert_eq!(
            longitude,
            &Dyadic::from_i128(2, &mut context).expect("two degrees")
        );
        assert_eq!(result.branch(), SphericalBranch::UniqueMinorArc);
    }

    #[test]
    fn minimum_subnormal_longitude_difference_survives_exact_ingress() {
        let tiny = f64::from_bits(1);
        let result =
            classify((0.0, 0.0), (tiny, 0.0), 128).expect("minimum-subnormal longitude branch");
        let longitude = result
            .identity()
            .principal_longitude()
            .expect("retained minimum-subnormal longitude");
        assert_eq!(longitude.sign(), Sign::Positive);
        assert_eq!(longitude.significand_u128(), Some(1));
        assert_eq!(longitude.exponent(), -1074);
        assert_eq!(result.branch(), SphericalBranch::UniqueMinorArc);
        assert_unique(result.distance());
    }

    #[test]
    fn cardinal_azimuths_use_proved_component_sectors() {
        let east = classify((0.0, 0.0), (90.0, 0.0), 128).expect("eastward branch");
        let north = classify((0.0, 0.0), (0.0, 30.0), 128).expect("northward branch");
        let south = classify((0.0, 30.0), (0.0, 0.0), 128).expect("southward branch");
        assert_eq!(
            east.azimuth_radians().expect("east azimuth").bits(),
            std::f64::consts::FRAC_PI_2.to_bits()
        );
        assert_eq!(
            north.azimuth_radians().expect("north azimuth").bits(),
            0.0_f64.to_bits()
        );
        assert_eq!(
            south.azimuth_radians().expect("south azimuth").bits(),
            std::f64::consts::PI.to_bits()
        );
    }

    #[test]
    fn distance_and_azimuth_narrow_at_64_128_and_256_bits() {
        let mut distances = Vec::new();
        let mut azimuths = Vec::new();
        let mut distance_widths = Vec::new();
        for bits in [64, 128, 256] {
            let result = classify((12.0, 0.0), (83.0, 0.0), bits).expect("minor branch");
            assert_eq!(result.branch(), SphericalBranch::UniqueMinorArc);
            assert_unique(result.distance());
            let azimuth = result.azimuth_radians().expect("azimuth");
            assert_unique(azimuth);
            distances.push(result.distance().bits());
            azimuths.push(azimuth.bits());
            let mut width_context = context_at(bits);
            distance_widths.push(width(result.distance().enclosure(), &mut width_context));
        }
        assert_eq!(distances[0], distances[1]);
        assert_eq!(distances[1], distances[2]);
        assert_eq!(azimuths[0], azimuths[1]);
        assert_eq!(azimuths[1], azimuths[2]);
        assert!(distance_widths[0] > distance_widths[1]);
        assert!(distance_widths[1] > distance_widths[2]);
    }

    #[test]
    fn equator_forty_five_degree_distance_terminates() {
        let result = classify((0.0, 0.0), (45.0, 0.0), 128).expect("quarter-turn branch");
        assert_eq!(result.branch(), SphericalBranch::UniqueMinorArc);
        assert_unique(result.distance());
    }

    #[test]
    fn atan_reduction_terminates_at_zero_half_one_straddle_and_above_one() {
        const DENOMINATOR: i128 = 1_000_000_000_000_000_000_000;
        const ATAN_HALF_LOWER: i128 = 463_647_609_000_806_116_214;
        const ATAN_HALF_UPPER: i128 = 463_647_609_000_806_116_215;
        const ATAN_ONE_LOWER: i128 = 785_398_163_397_448_309_615;
        const ATAN_ONE_UPPER: i128 = 785_398_163_397_448_309_616;
        const ATAN_TWO_LOWER: i128 = 1_107_148_717_794_090_503_017;
        const ATAN_TWO_UPPER: i128 = 1_107_148_717_794_090_503_018;

        let mut zero_context = context_at(128);
        let zero = Enclosure::exact(Dyadic::zero(), &mut zero_context).expect("zero interval");
        let zero_angle = atan_nonnegative(&zero, &mut zero_context).expect("atan zero");
        assert!(zero_angle.lo().is_zero());
        assert!(zero_angle.hi().is_zero());

        let mut half_context = context_at(64);
        let half = Dyadic::one(&mut half_context)
            .expect("one")
            .scale_pow2(-1, &mut half_context)
            .expect("half");
        let half_interval = Enclosure::exact(half, &mut half_context).expect("half interval");
        let half_angle = atan_nonnegative(&half_interval, &mut half_context).expect("atan half");
        assert_contains_rational_bounds(
            &half_angle,
            ATAN_HALF_LOWER,
            ATAN_HALF_UPPER,
            DENOMINATOR,
            &mut context_at(256),
        );

        let mut one_context = context_at(64);
        let one = Dyadic::one(&mut one_context).expect("one");
        let one_interval = Enclosure::exact(one, &mut one_context).expect("one interval");
        let one_angle = atan_nonnegative(&one_interval, &mut one_context).expect("atan one");
        assert_contains_rational_bounds(
            &one_angle,
            ATAN_ONE_LOWER,
            ATAN_ONE_UPPER,
            DENOMINATOR,
            &mut context_at(256),
        );

        let mut straddling_context = context_at(128);
        let half = Dyadic::one(&mut straddling_context)
            .expect("one")
            .scale_pow2(-1, &mut straddling_context)
            .expect("half");
        let two = Dyadic::from_i128(2, &mut straddling_context).expect("two");
        let straddling = Enclosure::new(half, two).expect("interval straddling one");
        let straddling_angle =
            atan_nonnegative(&straddling, &mut straddling_context).expect("atan straddling one");
        assert!(straddling_angle.lo() <= straddling_angle.hi());
        assert_contains_rational_bounds(
            &straddling_angle,
            ATAN_HALF_LOWER,
            ATAN_TWO_UPPER,
            DENOMINATOR,
            &mut context_at(256),
        );

        let mut below_one_context = context_at(128);
        let zero = Dyadic::zero();
        let one = Dyadic::one(&mut below_one_context).expect("one");
        let below_one = Enclosure::new(zero, one).expect("interval below one");
        let below_one_angle =
            atan_nonnegative(&below_one, &mut below_one_context).expect("atan interval below one");
        assert!(below_one_angle.lo() <= &Dyadic::zero());
        let mut below_one_verify = context_at(256);
        let one_upper = Dyadic::from_i128(ATAN_ONE_UPPER, &mut below_one_verify)
            .expect("atan one upper oracle");
        let denominator =
            Dyadic::from_i128(DENOMINATOR, &mut below_one_verify).expect("atan one denominator");
        assert!(
            below_one_angle
                .hi()
                .mul_scratch(&denominator, &mut below_one_verify)
                .expect("below-one upper product")
                > one_upper
        );

        let mut above_context = context_at(64);
        let two = Dyadic::from_i128(2, &mut above_context).expect("two");
        let above_interval = Enclosure::exact(two, &mut above_context).expect("above-one interval");
        let above_angle = atan_nonnegative(&above_interval, &mut above_context).expect("atan two");
        assert_contains_rational_bounds(
            &above_angle,
            ATAN_TWO_LOWER,
            ATAN_TWO_UPPER,
            DENOMINATOR,
            &mut context_at(256),
        );
    }

    #[test]
    fn published_distance_contains_an_independent_one_sided_rational_oracle() {
        let result = classify((0.0, 0.0), (1.0, 0.0), 256).expect("one-degree branch");
        let lower = Dyadic::from_i128(111_000, &mut context_at(256)).expect("lower metres");
        let upper = Dyadic::from_i128(112_000, &mut context_at(256)).expect("upper metres");
        assert!(*result.distance().enclosure().lo() > lower);
        assert!(*result.distance().enclosure().hi() < upper);
    }

    #[test]
    fn low_budget_and_allocator_failures_remain_distinct() {
        let value = input((12.0, 7.0), (83.0, 41.0));
        let mut low_fuel =
            PrecisionContext::new(PrecisionPolicy::new(128, 128, 0, 1024)).expect("fuel policy");
        let error = classify_spherical_geodesic(value, &mut low_fuel).expect_err("fuel exhaustion");
        assert!(matches!(error.kind(), ErrorKind::NumericalIndeterminacy));

        let mut low_workspace =
            PrecisionContext::new(PrecisionPolicy::new(256, 256, 128_000_000, 0))
                .expect("workspace policy");
        let error = classify_spherical_geodesic(value, &mut low_workspace)
            .expect_err("workspace exhaustion");
        assert!(matches!(error.kind(), ErrorKind::NumericalIndeterminacy));

        let allocation_error = fail_allocations_after(0, || {
            let mut context = context_at(256);
            classify_spherical_geodesic(value, &mut context)
        })
        .expect_err("allocator rejection");
        assert!(matches!(
            allocation_error.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }

    #[test]
    fn unresolved_angle_bracket_is_replayed_as_numerical_indeterminacy() {
        let mut context = context_at(64);
        let one = Dyadic::one(&mut context).expect("one");
        let bracket = vector_angle_bracket(&one, &one, VectorSector::PositiveInner, &mut context)
            .expect("diagonal bracket");
        assert_eq!(bracket.status(), AngleBracketStatus::Undecidable);
        let x = Enclosure::exact(one.try_clone(&mut context).expect("x"), &mut context)
            .expect("x enclosure");
        let y = Enclosure::exact(one, &mut context).expect("y enclosure");
        assert!(matches!(
            signed_angle_from_components(&x, &y, &mut context),
            Err(error) if matches!(error.kind(), ErrorKind::NumericalIndeterminacy)
        ));
    }

    #[test]
    fn structural_antipodal_distance_uses_pi_bracket_not_a_rounded_constant() {
        let result = classify((10.0, 20.0), (-170.0, -20.0), 128).expect("antipodal");
        let mut context = context_at(128);
        let pi = pi_bracket(&mut context).expect("pi");
        assert!(result.identity().root().lo() >= pi.lo());
        assert!(result.identity().root().hi() <= pi.hi());
        assert!(matches!(
            result.distance().enclosure().rounding_cell(&mut context),
            Ok(RoundingCell::Unique(_))
        ));
    }

    #[test]
    fn source_bits_remain_replayable_after_publication() {
        let start = (179.0_f64.to_bits(), (-0.0_f64).to_bits());
        let end = ((-179.0_f64).to_bits(), 0.0_f64.to_bits());
        let radius = 6_378_137.0_f64.to_bits();
        let input = SphericalInput::from_bits(start, end, radius).expect("bit input");
        let mut context = context_at(128);
        let result = classify_spherical_geodesic(input, &mut context).expect("branch");
        assert_eq!(result.identity().input(), input);
        assert_eq!(input.start().longitude_bits(), start.0);
        assert_eq!(input.start().latitude_bits(), start.1);
        assert_eq!(input.end().longitude_bits(), end.0);
        assert_eq!(input.end().latitude_bits(), end.1);
        assert_eq!(input.model().radius_bits(), radius);
    }

    #[test]
    fn output_rounding_rejects_a_cell_crossing() {
        let mut context = context_at(128);
        let one = Dyadic::one(&mut context).expect("one");
        let next = ExactBinary64::from_bits(0x3FF0_0000_0000_0001)
            .expect("next one")
            .exact(&mut context)
            .expect("next exact");
        let crossing = Enclosure::new(one, next).expect("crossing");
        let error = publish(crossing, &mut context).expect_err("cell crossing");
        assert!(matches!(error.kind(), ErrorKind::NumericalIndeterminacy));
    }

    #[test]
    fn rounded_distance_is_finite_for_the_independent_oracle_case() {
        let result = classify((0.0, 0.0), (1.0, 0.0), 256).expect("one-degree branch");
        assert!(result.distance().value().is_finite());
        assert!(matches!(
            result
                .distance()
                .enclosure()
                .rounding_cell(&mut context_at(256)),
            Ok(RoundingCell::Unique(_))
        ));
        assert!(result.distance().bits() & (1_u64 << 63) == 0);
    }
}
