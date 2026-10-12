#[cfg(test)]
use super::PhysicalEndpointKey;
use super::arcs::{ArcPosition, LongitudeOverlap};
use super::h3_fan::FanSide;
use super::trig::{outward_half_width, sin_cos, upward_sum};
use super::{
    AffineSourceArc, ArcContact, Ball, Bound, CertSign, DirectedLongitudeSpan, ExactArcKind,
    H3FanPlan, HALF_PI_LO, Midpoint, OrderedMinorArc, PI_HI, PI_LO, Vec3Bound,
};

impl ArcContact {
    pub(in crate::grid) const fn combine(self, rhs: Self) -> Self {
        use ArcContact::{ClosedOnly, None, Open, Uncertain};
        match (self, rhs) {
            (Open, _) | (_, Open) => Open,
            (Uncertain, _) | (_, Uncertain) => Uncertain,
            (ClosedOnly, _) | (_, ClosedOnly) => ClosedOnly,
            (None, None) => None,
        }
    }
}
const MAX_D_BOXES: usize = 96;
const MAX_D_DEPTH: u8 = 24;
/// Returns the B1 four-valued contact result for one affine source piece and
/// one ordered minor arc.  Every undecidable structural or analytic condition
/// deliberately reaches `Uncertain`; callers may only fail open from it.
#[cfg(test)]
pub(in crate::grid) fn classify_arc_contact(
    source: &AffineSourceArc,
    arc: &OrderedMinorArc,
) -> ArcContact {
    classify_arc_contact_with_h3_fan(source, arc, None)
}
/// H3's relation entry point binds a contact to the one certified fan which
/// owns the cell.  A mismatched supplier or an uncertified fan cannot use a
/// structural branch to establish a negative.
pub(in crate::grid) fn classify_h3_arc_contact(
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
    stack.first_mut()?.0 = lambda;
    let mut length = 1_usize;
    let mut common = None;
    while length != 0 {
        length = length.checked_sub(1)?;
        let (box_lambda, depth) = *stack.get(length)?;
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
                if depth == MAX_D_DEPTH {
                    return None;
                }
                let lower = Bound::new(box_lambda.lo, midpoint)?;
                let upper = Bound::new(midpoint, box_lambda.hi)?;
                if lower.width() == 0.0 || upper.width() == 0.0 {
                    return None;
                }
                let next_depth = depth.checked_add(1)?;
                let next_length = length.checked_add(2)?;
                let children = stack.get_mut(length..next_length)?;
                let [lower_slot, upper_slot] = children else {
                    return None;
                };
                *lower_slot = (lower, next_depth);
                *upper_slot = (upper, next_depth);
                length = next_length;
            },
        }
    }
    common
}
pub(super) const fn strict_sign(value: Bound) -> Option<CertSign> {
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
pub(in crate::grid) fn degree_bound_to_radians(value: Bound) -> Option<Bound> {
    value
        .mul(Bound::new(PI_LO, PI_HI)?)?
        .div(Bound::exact(180.0)?)
}
/// Certified spherical ray for a stored degree point.  The polar identities
/// are structural and deliberately ignore longitude; every other case is
/// evaluated through the bounded trigonometric carrier.
pub(super) fn certified_degree_vector(longitude: Bound, latitude: Bound) -> Option<Vec3Bound> {
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

#[cfg(test)]
mod tests;
