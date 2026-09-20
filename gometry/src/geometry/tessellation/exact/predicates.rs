//! Filtered orientation and in-circle predicates.
//!
//! Three routes answer the same question at rising cost: a floating filter
//! whose error bound may exclude zero, an integer lattice when the inputs are
//! small whole numbers, and the exact determinant when neither can decide.

use std::cmp::Ordering;

use super::{
    ExactDyadic, ExactSign, GeometryErrorKind, Interval, Result, XY, incircle,
    stored_orientation_sign, try_difference,
};

/// Shewchuk's stage-A orientation filter: the plain float determinant is the
/// answer whenever its magnitude clears an a-priori bound on its rounding
/// error, `(3 + 16ε)ε` times the sum of the term magnitudes, which covers
/// the two differences and the two products. Costs a handful of flops and
/// decides almost every call; the interval filter and the dyadic path behind
/// it take what it declines, including anything non-finite.
pub(in crate::geometry) fn orient2d_float(a: XY, b: XY, c: XY) -> Option<ExactSign> {
    const ERRBOUND: f64 = (3.0 + 16.0 * f64::EPSILON) * f64::EPSILON;
    // Components in [2^-450, 2^450] lie on the 2^-502 lattice.
    // Degree-two products and cancellations cannot underflow; the upper
    // bound also leaves room for the determinant sum and its error bound.
    const MIN_COMPONENT: f64 = f64::from_bits((1023 - 450) << 52);
    const MAX_COMPONENT: f64 = f64::from_bits((1023 + 450) << 52);
    let (acx, acy, bcx, bcy) = (a.x - c.x, a.y - c.y, b.x - c.x, b.y - c.y);
    if [acx, acy, bcx, bcy]
        .iter()
        .any(|value| *value != 0.0 && !(MIN_COMPONENT..=MAX_COMPONENT).contains(&value.abs()))
    {
        return None;
    }
    let (left, right) = (acx * bcy, acy * bcx);
    let det = left - right;
    let detsum = left.abs() + right.abs();
    if !det.is_finite() || !detsum.is_finite() {
        return None;
    }
    let bound = ERRBOUND * detsum;
    if det > bound {
        Some(ExactSign::Positive)
    } else if det < -bound {
        Some(ExactSign::Negative)
    } else {
        None
    }
}

/// Shewchuk's stage-A in-circle filter: the lifted 3x3 determinant with the
/// `(10 + 96ε)ε` bound on its permanent.
pub(in crate::geometry) fn incircle_float(a: XY, b: XY, c: XY, d: XY) -> Option<ExactSign> {
    const ERRBOUND: f64 = (10.0 + 96.0 * f64::EPSILON) * f64::EPSILON;
    // The relative-error filter requires normal intermediate arithmetic.
    // Components in [2^-200, 2^200] lie on the 2^-252 lattice, so every
    // nonzero degree-four intermediate is at least 2^-1008. Products and
    // their sums stay below 2^804, and a nonzero permanent is at least
    // 2^-800, keeping its error bound normal too.
    const MIN_COMPONENT: f64 = f64::from_bits((1023 - 200) << 52);
    const MAX_COMPONENT: f64 = f64::from_bits((1023 + 200) << 52);
    let (adx, ady) = (a.x - d.x, a.y - d.y);
    let (bdx, bdy) = (b.x - d.x, b.y - d.y);
    let (cdx, cdy) = (c.x - d.x, c.y - d.y);
    if [adx, ady, bdx, bdy, cdx, cdy]
        .iter()
        .any(|value| *value != 0.0 && !(MIN_COMPONENT..=MAX_COMPONENT).contains(&value.abs()))
    {
        return None;
    }
    let (bdxcdy, cdxbdy) = (bdx * cdy, cdx * bdy);
    let alift = adx * adx + ady * ady;
    let (cdxady, adxcdy) = (cdx * ady, adx * cdy);
    let blift = bdx * bdx + bdy * bdy;
    let (adxbdy, bdxady) = (adx * bdy, bdx * ady);
    let clift = cdx * cdx + cdy * cdy;
    let det = alift * (bdxcdy - cdxbdy) + blift * (cdxady - adxcdy) + clift * (adxbdy - bdxady);
    let permanent = (bdxcdy.abs() + cdxbdy.abs()) * alift
        + (cdxady.abs() + adxcdy.abs()) * blift
        + (adxbdy.abs() + bdxady.abs()) * clift;
    if !det.is_finite() || !permanent.is_finite() {
        return None;
    }
    let bound = ERRBOUND * permanent;
    if det > bound {
        Some(ExactSign::Positive)
    } else if det < -bound {
        Some(ExactSign::Negative)
    } else {
        None
    }
}

/// The inputs scaled to integers on one common power of two, sign preserved,
/// when every scaled magnitude is below 2^21. Lattice data (rasters, integer
/// metres, regular grids) lands here, and it is exactly the data whose
/// cocircular and collinear cases defeat every floating filter; the degree-4
/// in-circle determinant of six such terms stays below 2^92, so `i128` is
/// exact without allocating.
pub(in crate::geometry) fn lattice_integers<const N: usize>(values: [f64; N]) -> Option<[i128; N]> {
    const LIMIT_BITS: u32 = 21;
    let mut mantissas = [0_i64; N];
    let mut exponents = [i32::MAX; N];
    for (index, &value) in values.iter().enumerate() {
        if !value.is_finite() {
            return None;
        }
        if value == 0.0 {
            continue;
        }
        let bits = value.to_bits();
        let biased = ((bits >> 52) & 0x7FF) as i32;
        let fraction = bits & ((1_u64 << 52) - 1);
        let (mut mantissa, mut exponent) = if biased == 0 {
            (fraction, -1074)
        } else {
            (fraction | (1_u64 << 52), biased - 1075)
        };
        let trailing = mantissa.trailing_zeros();
        mantissa >>= trailing;
        exponent += trailing as i32;
        mantissas[index] = if value < 0.0 {
            -(mantissa as i64)
        } else {
            mantissa as i64
        };
        exponents[index] = exponent;
    }
    let common = exponents.iter().copied().min().unwrap_or(i32::MAX);
    let mut scaled = [0_i128; N];
    for index in 0..N {
        if mantissas[index] == 0 {
            continue;
        }
        let shift = exponents[index] - common;
        let bits = 64 - mantissas[index].unsigned_abs().leading_zeros();
        if shift < 0 || shift as u32 + bits > LIMIT_BITS {
            return None;
        }
        scaled[index] = i128::from(mantissas[index]) << shift;
    }
    Some(scaled)
}

pub(in crate::geometry) fn sign_of_i128(value: i128) -> ExactSign {
    match value.cmp(&0) {
        Ordering::Greater => ExactSign::Positive,
        Ordering::Less => ExactSign::Negative,
        Ordering::Equal => ExactSign::Zero,
    }
}

pub(in crate::geometry) fn orient2d_lattice(a: XY, b: XY, c: XY) -> Option<ExactSign> {
    let [ax, ay, bx, by, cx, cy] = lattice_integers([a.x, a.y, b.x, b.y, c.x, c.y])?;
    Some(sign_of_i128((bx - ax) * (cy - ay) - (by - ay) * (cx - ax)))
}

pub(in crate::geometry) fn incircle_lattice(a: XY, b: XY, c: XY, d: XY) -> Option<ExactSign> {
    let [ax, ay, bx, by, cx, cy, dx, dy] =
        lattice_integers([a.x, a.y, b.x, b.y, c.x, c.y, d.x, d.y])?;
    let (adx, ady) = (ax - dx, ay - dy);
    let (bdx, bdy) = (bx - dx, by - dy);
    let (cdx, cdy) = (cx - dx, cy - dy);
    let alift = adx * adx + ady * ady;
    let blift = bdx * bdx + bdy * bdy;
    let clift = cdx * cdx + cdy * cdy;
    let bcdet = bdx * cdy - cdx * bdy;
    let cadet = cdx * ady - adx * cdy;
    let abdet = adx * bdy - bdx * ady;
    Some(sign_of_i128(alift * bcdet + blift * cadet + clift * abdet))
}

pub(in crate::geometry) fn orient2d(a: XY, b: XY, c: XY) -> ExactSign {
    if let Some(sign) = orient2d_float(a, b, c) {
        debug_assert_eq!(
            sign,
            stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y),
            "float-filtered orient2d disagrees"
        );
        return sign;
    }
    if let Some(sign) = orient2d_lattice(a, b, c) {
        debug_assert_eq!(
            sign,
            stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y),
            "lattice orient2d disagrees"
        );
        return sign;
    }
    stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y)
}

/// The interval filter for the in-circle determinant, with the same
/// containment argument and the same exact fallback.
pub(in crate::geometry) fn incircle_filtered(a: XY, b: XY, c: XY, d: XY) -> Option<ExactSign> {
    let value = Interval::exact;
    let displacement = |point: XY| {
        (
            value(point.x).sub(value(d.x)),
            value(point.y).sub(value(d.y)),
        )
    };
    let (adx, ady) = displacement(a);
    let (bdx, bdy) = displacement(b);
    let (cdx, cdy) = displacement(c);
    if !(adx.is_finite()
        && ady.is_finite()
        && bdx.is_finite()
        && bdy.is_finite()
        && cdx.is_finite()
        && cdy.is_finite())
    {
        return None;
    }
    let alift = adx.mul(adx).add(ady.mul(ady));
    let blift = bdx.mul(bdx).add(bdy.mul(bdy));
    let clift = cdx.mul(cdx).add(cdy.mul(cdy));
    let bcdet = bdx.mul(cdy).sub(cdx.mul(bdy));
    let cadet = cdx.mul(ady).sub(adx.mul(cdy));
    let abdet = adx.mul(bdy).sub(bdx.mul(ady));
    if !(alift.is_finite()
        && blift.is_finite()
        && clift.is_finite()
        && bcdet.is_finite()
        && cadet.is_finite()
        && abdet.is_finite())
    {
        return None;
    }
    let determinant = alift.mul(bcdet).add(blift.mul(cadet)).add(clift.mul(abdet));
    determinant.sign()
}

pub(in crate::geometry) fn same_xy(left: XY, right: XY) -> bool {
    crate::geometry::canonical_f64_bits(left.x) == crate::geometry::canonical_f64_bits(right.x)
        && crate::geometry::canonical_f64_bits(left.y)
            == crate::geometry::canonical_f64_bits(right.y)
}

pub(in crate::geometry) fn diameter_contains_filtered(a: XY, b: XY, point: XY) -> Option<bool> {
    let exact = Interval::exact;
    let pa = (
        exact(point.x).sub(exact(a.x)),
        exact(point.y).sub(exact(a.y)),
    );
    let pb = (
        exact(point.x).sub(exact(b.x)),
        exact(point.y).sub(exact(b.y)),
    );
    if !(pa.0.is_finite() && pa.1.is_finite() && pb.0.is_finite() && pb.1.is_finite()) {
        return None;
    }
    let dot = pa.0.mul(pb.0).add(pa.1.mul(pb.1));
    dot.is_finite()
        .then(|| dot.sign())
        .flatten()
        .map(|sign| sign != ExactSign::Positive)
}

/// Exact containment in the circle through one, two, or three source points.
///
/// A one-point candidate is an identity test.  A two-point candidate is the
/// diameter disk, whose exact predicate is `(p-a)·(p-b) <= 0`.  A three-point
/// candidate uses the oriented exact in-circle determinant.
pub(in crate::geometry) fn try_circle_contains(support: &[XY], point: XY) -> Result<bool> {
    match support {
        [only] => Ok(same_xy(*only, point)),
        [a, b] => {
            if let Some(contains) = diameter_contains_filtered(*a, *b, point) {
                return Ok(contains);
            }
            let pa = (try_difference(point.x, a.x)?, try_difference(point.y, a.y)?);
            let pb = (try_difference(point.x, b.x)?, try_difference(point.y, b.y)?);
            let dot = pa.0.try_product(&pb.0)?.try_add(pa.1.try_product(&pb.1)?)?;
            Ok(dot.sign() != ExactSign::Positive)
        },
        [a, b, c] => {
            let orientation = orient2d(*a, *b, *c);
            if orientation == ExactSign::Zero {
                return Ok(false);
            }
            let determinant = incircle(*a, *b, *c, point)?;
            Ok(match orientation {
                ExactSign::Positive => determinant != ExactSign::Negative,
                ExactSign::Negative => determinant != ExactSign::Positive,
                ExactSign::Zero => unreachable!(),
            })
        },
        _ => Err(GeometryErrorKind::Invalid(
            "minimum circle support must contain one, two, or three points".into(),
        )
        .into()),
    }
}

/// Compare squared distances between two source pairs without rounding or
/// forming a floating-point square.
pub(in crate::geometry) fn try_squared_distance_cmp(
    left_a: XY,
    left_b: XY,
    right_a: XY,
    right_b: XY,
) -> Result<Ordering> {
    let squared = |a: XY, b: XY| -> Result<ExactDyadic> {
        let dx = try_difference(a.x, b.x)?;
        let dy = try_difference(a.y, b.y)?;
        dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)
    };
    Ok(squared(left_a, left_b)?.cmp(&squared(right_a, right_b)?))
}
