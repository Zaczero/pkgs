use super::contact::strict_sign;
use super::trig::{sin_cos_for_inverse, ulp};
use super::{
    Bound, CertSign, DirectedLongitudeSpan, HALF_PI_HI, HALF_PI_LO, Midpoint as _, PI_HI, PI_LO,
    Vec3Bound,
};

impl Vec3Bound {
    pub(super) fn exact(values: [f64; 3]) -> Option<Self> {
        Some(Self {
            x: Bound::exact(values[0])?,
            y: Bound::exact(values[1])?,
            z: Bound::exact(values[2])?,
        })
    }

    pub(super) fn add(self, rhs: Self) -> Option<Self> {
        Some(Self {
            x: self.x.add(rhs.x)?,
            y: self.y.add(rhs.y)?,
            z: self.z.add(rhs.z)?,
        })
    }

    pub(super) fn sub(self, rhs: Self) -> Option<Self> {
        Some(Self {
            x: self.x.sub(rhs.x)?,
            y: self.y.sub(rhs.y)?,
            z: self.z.sub(rhs.z)?,
        })
    }

    pub(super) fn neg(self) -> Self {
        Self {
            x: self.x.neg(),
            y: self.y.neg(),
            z: self.z.neg(),
        }
    }

    pub(super) fn scale(self, scalar: Bound) -> Option<Self> {
        Some(Self {
            x: self.x.mul(scalar)?,
            y: self.y.mul(scalar)?,
            z: self.z.mul(scalar)?,
        })
    }

    pub(super) fn dot(self, rhs: Self) -> Option<Bound> {
        self.x
            .mul(rhs.x)?
            .add(self.y.mul(rhs.y)?)?
            .add(self.z.mul(rhs.z)?)
    }

    pub(super) fn squared_norm(self) -> Option<Bound> {
        self.x
            .square()?
            .add_nonnegative(self.y.square()?)?
            .add_nonnegative(self.z.square()?)
    }

    pub(super) fn cross(self, rhs: Self) -> Option<Self> {
        Some(Self {
            x: self.y.mul(rhs.z)?.sub(self.z.mul(rhs.y)?)?,
            y: self.z.mul(rhs.x)?.sub(self.x.mul(rhs.z)?)?,
            z: self.x.mul(rhs.y)?.sub(self.y.mul(rhs.x)?)?,
        })
    }

    pub(super) fn normalized(self) -> Option<Self> {
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
pub(super) fn directed_arc_longitude(
    start: Vec3Bound,
    end: Vec3Bound,
) -> Option<DirectedLongitudeSpan> {
    let first = certified_vector_longitude(start)?;
    let second = certified_vector_longitude(end)?;
    directed_arc_longitude_from_endpoints(first, second)
}
pub(super) fn directed_arc_longitude_from_endpoints(
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
pub(super) const fn strict_minor_longitude_delta(delta: Bound) -> bool {
    delta.lo > -PI_LO && delta.hi < PI_LO
}
pub(super) fn certified_vector_longitude(point: Vec3Bound) -> Option<Bound> {
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
pub(super) fn certified_vector_latitude(point: Vec3Bound) -> Option<Bound> {
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
        count.checked_sub(1)?
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
