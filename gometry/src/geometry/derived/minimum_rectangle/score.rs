//! Exact and filtered candidate-area ranking.

use super::calipers::exact_width;
use super::{
    CachedDoubleDouble, Candidate, DOUBLE_LIMBS, DoubleDouble, Interval, LIMBS, Ordering, Point,
    RankedCandidate, TRIPLE_LIMBS, double_double_add, double_double_mul, double_double_sub,
    two_product,
};

impl RankedCandidate {
    pub(super) fn new(hull: &[Point], candidate: Candidate) -> Self {
        Self {
            candidate,
            interval: candidate_score_interval(hull, candidate),
            double_double: CachedDoubleDouble::Unknown,
        }
    }

    pub(super) fn double_double(&mut self, hull: &[Point]) -> Option<DoubleDouble> {
        if matches!(self.double_double, CachedDoubleDouble::Unknown) {
            self.double_double = double_double_score(hull, self.candidate).map_or(
                CachedDoubleDouble::Unavailable,
                CachedDoubleDouble::Available,
            );
        }
        match self.double_double {
            CachedDoubleDouble::Available(score) => Some(score),
            CachedDoubleDouble::Unknown | CachedDoubleDouble::Unavailable => None,
        }
    }
}

/// Schoolbook product of little-endian magnitudes; `OUT` holds every carry.
#[expect(
    clippy::indexing_slicing,
    reason = "limb offsets are within OUT by the compile-time OUT >= L + R bound and carry propagation"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "limb offsets are bounded by fixed operand widths and carry is propagated only within OUT"
)]
pub(super) fn wide_product<const L: usize, const R: usize, const OUT: usize>(
    left: &[u64; L],
    right: &[u64; R],
) -> [u64; OUT] {
    const { assert!(OUT >= L + R) };
    let mut result = [0_u64; OUT];
    let Some((left_start, left_end)) = nonzero_span(left) else {
        return result;
    };
    let Some((right_start, right_end)) = nonzero_span(right) else {
        return result;
    };
    for (i, &lhs) in left.iter().enumerate().take(left_end + 1).skip(left_start) {
        if lhs == 0 {
            continue;
        }
        let mut carry = 0_u128;
        for (j, &rhs) in right
            .iter()
            .enumerate()
            .take(right_end + 1)
            .skip(right_start)
        {
            // Even a zero source limb must receive and propagate the carry
            // from its lower neighbour.  The final carry belongs directly
            // above the right operand's *actual* top limb, not its last slot.
            let index = i + j;
            let total = u128::from(result[index]) + u128::from(lhs) * u128::from(rhs) + carry;
            result[index] = total as u64;
            carry = total >> 64;
        }
        let mut index = i + right_end + 1;
        while carry != 0 {
            let total = u128::from(result[index]) + carry;
            result[index] = total as u64;
            carry = total >> 64;
            index += 1;
        }
    }
    result
}

pub(super) fn add_triple_magnitude(
    left: &[u64; TRIPLE_LIMBS],
    right: &[u64; TRIPLE_LIMBS],
) -> [u64; TRIPLE_LIMBS] {
    let mut result = [0_u64; TRIPLE_LIMBS];
    let mut carry = false;
    for ((&left_word, &right_word), result_word) in left.iter().zip(right).zip(&mut result) {
        let (sum, first_carry) = left_word.overflowing_add(right_word);
        let (sum, second_carry) = sum.overflowing_add(u64::from(carry));
        *result_word = sum;
        carry = first_carry || second_carry;
    }
    debug_assert!(!carry);
    result
}

pub(super) fn nonzero_span(words: &[u64]) -> Option<(usize, usize)> {
    let first = words.iter().position(|&word| word != 0)?;
    let last = words
        .iter()
        .rposition(|&word| word != 0)
        .expect("nonzero first word");
    Some((first, last))
}

pub(super) fn cmp_wide(left: &[u64; TRIPLE_LIMBS], right: &[u64; TRIPLE_LIMBS]) -> Ordering {
    for (left_word, right_word) in left.iter().zip(right).rev() {
        match left_word.cmp(right_word) {
            Ordering::Equal => {},
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
        }
    }
    Ordering::Equal
}

pub(super) fn candidate_area_order(
    hull: &[Point],
    left: &mut RankedCandidate,
    right: &mut RankedCandidate,
) -> Ordering {
    if let (Some(left_interval), Some(right_interval)) = (left.interval, right.interval) {
        if left_interval.hi < right_interval.lo {
            return Ordering::Less;
        }
        if left_interval.lo > right_interval.hi {
            return Ordering::Greater;
        }
    }
    if let Some(ordering) =
        double_double_score_order(left.double_double(hull), right.double_double(hull))
    {
        return ordering;
    }
    let (left_a, left_o, left_n) = candidate_score_terms(hull, left.candidate);
    let (right_a, right_o, right_n) = candidate_score_terms(hull, right.candidate);
    let left_ao: [u64; DOUBLE_LIMBS] = wide_product(&left_a, &left_o);
    let right_ao: [u64; DOUBLE_LIMBS] = wide_product(&right_a, &right_o);
    let lhs: [u64; TRIPLE_LIMBS] = wide_product(&left_ao, &right_n);
    let rhs: [u64; TRIPLE_LIMBS] = wide_product(&right_ao, &left_n);
    cmp_wide(&lhs, &rhs)
}

/// A two-word filter over exact subtraction/product residuals.  The expensive
/// limb product below is reserved for scores within a conservative `O(eps²)`
/// neighbourhood.  This is the ordinary caliper path: no allocation and no
/// input-coordinate cancellation even at large translations.
pub(super) fn double_double_score_order(
    left: Option<DoubleDouble>,
    right: Option<DoubleDouble>,
) -> Option<Ordering> {
    let left = left?;
    let right = right?;
    let difference = double_double_sub(left, right);
    let scale = left.0.abs().max(right.0.abs()).max(1.0);
    // The operations below are error-free transforms followed by a bounded
    // number of ordinary additions/divisions.  This deliberately generous
    // guard is still far below one f64 ULP, so only genuinely near-equal
    // candidates reach the 68-limb comparator.
    let uncertainty = scale * 4096.0 * f64::EPSILON * f64::EPSILON;
    (difference.0.abs() > uncertainty).then(|| difference.0.total_cmp(&0.0))
}

#[expect(
    clippy::indexing_slicing,
    reason = "candidate support ordinals are generated from this same convex hull"
)]
pub(super) fn double_double_score(hull: &[Point], candidate: Candidate) -> Option<DoubleDouble> {
    let supports = candidate.supports;
    let along = double_double_delta(
        candidate.basis.along,
        hull[supports.along_min],
        hull[supports.along_max],
    )?;
    let outward = double_double_delta(
        candidate.basis.outward,
        hull[supports.outward_min],
        hull[supports.outward_max],
    )?;
    let norm = double_double_add(
        two_product(candidate.basis.along.0, candidate.basis.along.0)?,
        two_product(candidate.basis.along.1, candidate.basis.along.1)?,
    );
    if along.0 <= 0.0 || outward.0 <= 0.0 || norm.0 <= 0.0 {
        return None;
    }
    double_double_div(double_double_mul(along, outward)?, norm)
}

pub(super) fn double_double_delta(
    direction: (f64, f64),
    minimum: Point,
    maximum: Point,
) -> Option<DoubleDouble> {
    let (dx, dx_residual) = two_diff(maximum.x, minimum.x);
    let (dy, dy_residual) = two_diff(maximum.y, minimum.y);
    if !dx.is_finite() || !dy.is_finite() || !dx_residual.is_finite() || !dy_residual.is_finite() {
        return None;
    }
    let x = double_double_add(
        two_product(direction.0, dx)?,
        two_product(direction.0, dx_residual)?,
    );
    let y = double_double_add(
        two_product(direction.1, dy)?,
        two_product(direction.1, dy_residual)?,
    );
    let result = double_double_add(x, y);
    (result.0.is_finite() && result.1.is_finite()).then_some(result)
}

pub(super) fn double_double_div(
    numerator: DoubleDouble,
    denominator: DoubleDouble,
) -> Option<DoubleDouble> {
    let quotient = numerator.0 / denominator.0;
    if !quotient.is_finite() {
        return None;
    }
    let product = double_double_mul(denominator, (quotient, 0.0))?;
    let remainder = double_double_sub(numerator, product);
    let correction = remainder.0 / denominator.0;
    correction
        .is_finite()
        .then(|| double_double_add((quotient, 0.0), (correction, 0.0)))
}

/// A certified floating filter for the exact score.  It deliberately charges
/// the input-coordinate products, rather than trusting a rounded
/// `(support - support)` residual.  At large translations that makes the
/// interval wider (and selects the exact fallback); for ordinary data it
/// resolves the comparison without allocating or multiplying limb arrays.
#[expect(
    clippy::indexing_slicing,
    reason = "candidate support ordinals are generated from this same convex hull"
)]
pub(super) fn candidate_score_interval(hull: &[Point], candidate: Candidate) -> Option<Interval> {
    let supports = candidate.supports;
    let along = directed_delta_interval(
        candidate.basis.along,
        hull[supports.along_min],
        hull[supports.along_max],
    )?;
    let outward = directed_delta_interval(
        candidate.basis.outward,
        hull[supports.outward_min],
        hull[supports.outward_max],
    )?;
    let norm = candidate.basis.along.0.mul_add(
        candidate.basis.along.0,
        candidate.basis.along.1 * candidate.basis.along.1,
    );
    if !norm.is_normal() || along.lo <= 0.0 || outward.lo <= 0.0 {
        return None;
    }
    let norm_error = norm.abs() * 16.0 * f64::EPSILON;
    let denominator_lo = norm - norm_error;
    let denominator_hi = norm + norm_error;
    if denominator_lo <= 0.0 || !denominator_hi.is_finite() {
        return None;
    }
    let lo = ((along.lo * outward.lo) / denominator_hi).next_down();
    let hi = ((along.hi * outward.hi) / denominator_lo).next_up();
    (lo.is_finite() && hi.is_finite()).then_some(Interval { lo, hi })
}

pub(super) fn directed_delta_interval(
    direction: (f64, f64),
    minimum: Point,
    maximum: Point,
) -> Option<Interval> {
    let (dx, dx_residual) = two_diff(maximum.x, minimum.x);
    let (dy, dy_residual) = two_diff(maximum.y, minimum.y);
    let first = direction.0 * dx;
    let second = direction.1 * dy;
    let estimate = first + second;
    if !first.is_finite() || !second.is_finite() || !estimate.is_finite() {
        return None;
    }
    let magnitude = first.abs() + second.abs();
    if !magnitude.is_finite() {
        return None;
    }
    // `two_diff` retains the exact input subtraction as `difference +
    // residual`.  The remaining bound is just two products plus one sum;
    // unlike an absolute-coordinate bound it stays tight at large offsets.
    let subtraction_error =
        direction.0.abs() * dx_residual.abs() + direction.1.abs() * dy_residual.abs();
    let error = subtraction_error + magnitude * 16.0 * f64::EPSILON;
    Some(Interval {
        lo: (estimate - error).next_down(),
        hi: (estimate + error).next_up(),
    })
}

pub(super) fn two_diff(left: f64, right: f64) -> (f64, f64) {
    let difference = left - right;
    let right_virtual = left - difference;
    let left_virtual = difference + right_virtual;
    let right_error = right_virtual - right;
    let left_error = left - left_virtual;
    (difference, left_error + right_error)
}

#[expect(
    clippy::indexing_slicing,
    reason = "candidate support ordinals are generated from this same convex hull"
)]
pub(super) fn candidate_score_terms(
    hull: &[Point],
    candidate: Candidate,
) -> ([u64; LIMBS], [u64; LIMBS], [u64; LIMBS]) {
    let supports = candidate.supports;
    (
        exact_width(
            candidate.basis.along,
            hull[supports.along_min],
            hull[supports.along_max],
        ),
        exact_width(
            candidate.basis.outward,
            hull[supports.outward_min],
            hull[supports.outward_max],
        ),
        candidate.basis.norm_exact().signed_magnitude().1,
    )
}
