//! Minimum-area enclosing rectangles with an O(1) enclosure certificate.
//!
//! The calipers retain the *world-coordinate* support vertices.  Floating
//! projections are only a filter: every ambiguous support comparison, score
//! comparison, and emitted support inequality is evaluated as an exact dyadic
//! sum.  Consequently the final four-corner construction needs no hull scan.

use std::cmp::Ordering;

use crate::error::Result;
use crate::geometry::tessellation::exact::two_sum;
use crate::geometry::{
    CoordSeq, GeometryErrorKind, Orientation, Point, axis_pow2_scale, orientation, point_distance,
};

const LIMBS: usize = 68;
const PRODUCT_MIN_EXP: i32 = -2148;
const PROJECTION_ERRBOUND: f64 = 8.0 * f64::EPSILON;
const DOUBLE_LIMBS: usize = LIMBS * 2;
const TRIPLE_LIMBS: usize = LIMBS * 3 + 1;

#[derive(Clone)]
struct ExactSum {
    positive: [u64; LIMBS],
    negative: [u64; LIMBS],
}

#[derive(Clone, Copy, Debug)]
/// A plain forward-error bound `[lo, hi]`.
///
/// DELIBERATELY NOT unified with `tessellation::exact::Interval`, which has the
/// same two fields — audited twice and rejected both times. That one is a
/// CERTIFIED outward-rounded object: its `add`/`sub`/`mul` assume the
/// tessellation containment chain ("provably contains its exact rational
/// coordinate; can only DECLINE, never decide wrongly"). These bounds satisfy
/// no such contract. Sharing the type puts those methods in reach here, which
/// couples two independent proofs to save four lines — and moving the struct to
/// a neutral third module does NOT avoid it, because the hazard is the methods
/// being callable on the type, not where the type is declared.
///
/// Nothing would catch the misuse: it surfaces as a wrong rectangle, not a
/// crash. Keep them separate.
struct Interval {
    lo: f64,
    hi: f64,
}

impl ExactSum {
    const fn zero() -> Self {
        Self {
            positive: [0; LIMBS],
            negative: [0; LIMBS],
        }
    }

    fn add_product(&mut self, coefficient: i8, lhs: f64, rhs: f64) {
        debug_assert!(lhs.is_finite() && rhs.is_finite());
        let (lhs_negative, lhs_mantissa, lhs_exponent) = decompose(lhs);
        let (rhs_negative, rhs_mantissa, rhs_exponent) = decompose(rhs);
        if lhs_mantissa == 0 || rhs_mantissa == 0 {
            return;
        }
        let negative = (coefficient < 0) ^ lhs_negative ^ rhs_negative;
        let product = u128::from(lhs_mantissa) * u128::from(rhs_mantissa);
        let shift = usize::try_from(lhs_exponent + rhs_exponent - PRODUCT_MIN_EXP)
            .expect("finite f64 product fits the fixed exact accumulator");
        add_shifted(
            if negative {
                &mut self.negative
            } else {
                &mut self.positive
            },
            product,
            shift,
        );
    }

    fn ordering(&self) -> Ordering {
        cmp_magnitude(&self.positive, &self.negative)
    }

    fn signed_magnitude(&self) -> (Ordering, [u64; LIMBS]) {
        match self.ordering() {
            Ordering::Equal => (Ordering::Equal, [0; LIMBS]),
            Ordering::Greater => (
                Ordering::Greater,
                subtract_magnitude(&self.positive, &self.negative),
            ),
            Ordering::Less => (
                Ordering::Less,
                subtract_magnitude(&self.negative, &self.positive),
            ),
        }
    }
}

const fn decompose(value: f64) -> (bool, u64, i32) {
    let bits = value.to_bits();
    let negative = bits >> 63 != 0;
    let stored = ((bits >> 52) & 0x7FF) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    if stored == 0 {
        (negative, fraction, -1074)
    } else {
        (negative, (1_u64 << 52) | fraction, stored - 1075)
    }
}

fn add_shifted(target: &mut [u64; LIMBS], product: u128, shift: usize) {
    let word = shift / 64;
    let bits = shift % 64;
    let low = product as u64;
    let high = (product >> 64) as u64;
    add_word(target, word, low << bits);
    if bits == 0 {
        add_word(target, word + 1, high);
    } else {
        add_word(target, word + 1, low >> (64 - bits));
        add_word(target, word + 1, high << bits);
        add_word(target, word + 2, high >> (64 - bits));
    }
}

fn add_word(target: &mut [u64; LIMBS], mut index: usize, mut value: u64) {
    while value != 0 {
        let (sum, carry) = target[index].overflowing_add(value);
        target[index] = sum;
        value = u64::from(carry);
        index += 1;
        debug_assert!(index <= LIMBS);
    }
}

fn cmp_magnitude(left: &[u64; LIMBS], right: &[u64; LIMBS]) -> Ordering {
    for index in (0..LIMBS).rev() {
        match left[index].cmp(&right[index]) {
            Ordering::Equal => {},
            ordering => return ordering,
        }
    }
    Ordering::Equal
}

fn subtract_magnitude(larger: &[u64; LIMBS], smaller: &[u64; LIMBS]) -> [u64; LIMBS] {
    debug_assert_ne!(cmp_magnitude(larger, smaller), Ordering::Less);
    let mut result = [0; LIMBS];
    let mut borrow = false;
    for index in 0..LIMBS {
        let (first, first_borrow) = larger[index].overflowing_sub(smaller[index]);
        let (difference, second_borrow) = first.overflowing_sub(u64::from(borrow));
        result[index] = difference;
        borrow = first_borrow || second_borrow;
    }
    debug_assert!(!borrow);
    result
}

/// Adjacent binary64 values bracketing an exact positive fixed-limb value.
fn positive_interval(magnitude: &[u64; LIMBS]) -> Interval {
    let high_index = (0..LIMBS)
        .rev()
        .find(|&index| magnitude[index] != 0)
        .expect("positive magnitude has a high limb");
    let high_bit = high_index * 64 + (63 - magnitude[high_index].leading_zeros() as usize);
    let exponent = PRODUCT_MIN_EXP + high_bit as i32;
    if exponent > 1023 {
        return Interval {
            lo: f64::MAX,
            hi: f64::INFINITY,
        };
    }
    let discard = if exponent >= -1022 {
        high_bit - 52
    } else {
        usize::try_from(-1074 - PRODUCT_MIN_EXP).expect("fixed product exponent")
    };
    let mantissa = shifted_low_u64(magnitude, discard);
    let lo = if exponent >= -1022 {
        let stored = u64::try_from(exponent + 1023).expect("finite stored exponent");
        f64::from_bits((stored << 52) | (mantissa - (1_u64 << 52)))
    } else {
        f64::from_bits(mantissa)
    };
    Interval {
        lo,
        hi: if any_bits_below(magnitude, discard) {
            lo.next_up()
        } else {
            lo
        },
    }
}

const fn shifted_low_u64(words: &[u64; LIMBS], shift: usize) -> u64 {
    let word = shift / 64;
    let bits = shift % 64;
    let mut value = words[word] >> bits;
    if bits != 0 && word + 1 < LIMBS {
        value |= words[word + 1] << (64 - bits);
    }
    value
}

fn any_bits_below(words: &[u64; LIMBS], bit: usize) -> bool {
    let word = bit / 64;
    let bits = bit % 64;
    words[..word].iter().any(|&value| value != 0)
        || (bits != 0 && words[word] & ((1_u64 << bits) - 1) != 0)
}

fn exact_dot_delta(direction: (f64, f64), current: Point, next: Point) -> ExactSum {
    let mut sum = ExactSum::zero();
    sum.add_product(1, direction.0, next.x);
    sum.add_product(-1, direction.0, current.x);
    sum.add_product(1, direction.1, next.y);
    sum.add_product(-1, direction.1, current.y);
    sum
}

fn projection_delta_order(direction: (f64, f64), current: Point, next: Point) -> Ordering {
    projection_delta_filter(direction, current, next)
        .unwrap_or_else(|| exact_dot_delta(direction, current, next).ordering())
}

fn projection_delta_filter(direction: (f64, f64), current: Point, next: Point) -> Option<Ordering> {
    let dx = next.x - current.x;
    let dy = next.y - current.y;
    let first = direction.0 * dx;
    let second = direction.1 * dy;
    let estimate = first + second;
    let magnitude = first.abs() + second.abs();
    let first_ordinary = first.is_normal() || (first == 0.0 && (direction.0 == 0.0 || dx == 0.0));
    let second_ordinary =
        second.is_normal() || (second == 0.0 && (direction.1 == 0.0 || dy == 0.0));
    if first_ordinary
        && second_ordinary
        && magnitude.is_normal()
        && estimate.abs() > magnitude * PROJECTION_ERRBOUND
    {
        return Some(estimate.total_cmp(&0.0));
    }
    None
}

#[derive(Clone, Copy)]
struct Basis {
    along: (f64, f64),
    outward: (f64, f64),
}

impl Basis {
    fn from_edge(start: Point, end: Point) -> Option<Self> {
        let mut dx = end.x - start.x;
        let mut dy = end.y - start.y;
        // Match the established ordinary-coordinate basis construction exactly;
        // its guarded sqrt path is also the byte-compatibility input to the
        // actual-edge certificate.  Extreme edges fall through to the scaled
        // residual branch before normalisation.
        let mut length = point_distance(start, end);
        if !length.is_finite() {
            // Scale absolute operands before subtraction.  This is deliberately
            // local to the edge: support truth stays in world coordinates.
            let maximum = start
                .x
                .abs()
                .max(start.y.abs())
                .max(end.x.abs())
                .max(end.y.abs());
            let scale = axis_pow2_scale(maximum);
            dx = end.x * scale - start.x * scale;
            dy = end.y * scale - start.y * scale;
            length = point_distance(
                Point::new_unchecked_xy(start.x * scale, start.y * scale),
                Point::new_unchecked_xy(end.x * scale, end.y * scale),
            );
        }
        (length.is_finite() && length != 0.0).then(|| {
            let along = (dx / length, dy / length);
            Self {
                along,
                outward: (-along.1, along.0),
            }
        })
    }

    fn norm_exact(self) -> ExactSum {
        let mut norm = ExactSum::zero();
        norm.add_product(1, self.along.0, self.along.0);
        norm.add_product(1, self.along.1, self.along.1);
        norm
    }
}

#[derive(Clone, Copy)]
struct Supports {
    along_min: usize,
    along_max: usize,
    outward_min: usize,
    outward_max: usize,
}

#[derive(Clone, Copy)]
struct Candidate {
    basis: Basis,
    supports: Supports,
}

/// The ideal corner in the candidate's scaled residual frame. `point` is
/// the ordinary binary64 evaluation used by the finite fast paths; `numerator`
/// and `denominator` retain the exact dyadic value needed when that evaluation
/// overflows and the finite-wedge solver has to choose among representable
/// points.
#[derive(Clone)]
struct PreferredCorner {
    point: Point,
    numerator: [ExactSum; 2],
    denominator: [f64; 2],
    anisotropic_frame: bool,
}

impl PreferredCorner {
    fn coordinate_difference(&self, axis: Axis, value: f64) -> ExactSum {
        let mut difference = self.numerator[axis.index()].clone();
        difference.add_product(-1, value, self.denominator[axis.index()]);
        difference
    }

    /// Compare a finite binary64 coordinate with the exact ideal coordinate.
    fn coordinate_order(&self, axis: Axis, value: f64) -> Ordering {
        self.coordinate_difference(axis, value).ordering().reverse()
    }
}

/// A candidate carries the two cheap score stages across comparisons.  The
/// sweep compares every new edge with the current best, so recomputing that
/// best candidate's interval and double-double score at every edge is pure
/// O(h) overhead.
struct RankedCandidate {
    candidate: Candidate,
    interval: Option<Interval>,
    double_double: CachedDoubleDouble,
}

#[derive(Clone, Copy)]
enum CachedDoubleDouble {
    Unknown,
    Available(DoubleDouble),
    Unavailable,
}

impl RankedCandidate {
    fn new(hull: &[Point], candidate: Candidate) -> Self {
        Self {
            candidate,
            interval: candidate_score_interval(hull, candidate),
            double_double: CachedDoubleDouble::Unknown,
        }
    }

    fn double_double(&mut self, hull: &[Point]) -> Option<DoubleDouble> {
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

/// Minimum-area enclosing rectangle of a CCW convex open ring.  All final
/// enclosure work is bounded by four corners and eight exact margins.
pub(crate) fn minimum_area_rectangle(hull: &[Point]) -> Result<CoordSeq> {
    debug_assert!(hull.len() >= 3);
    if let Some(bounds) = axis_aligned_hull_rectangle(hull) {
        return Ok(CoordSeq::from_points(&bounds));
    }
    let best = select_minimum_candidate(hull);
    let corners = best.emit(hull)?;
    Ok(CoordSeq::from_points(&corners))
}

fn select_minimum_candidate(hull: &[Point]) -> Candidate {
    let count = hull.len();
    let first_basis = (0..count)
        .find_map(|edge| Basis::from_edge(hull[edge], hull[(edge + 1) % count]))
        .expect("strict convex hull has a finite nonzero edge basis");
    // One exact global support establishes the first caliper.  In CCW support
    // order the remaining directions follow `-v, u, v, -u`; advancing those
    // unwrapped pointers from their predecessor crosses each first-edge
    // normal cone once, rather than paying four independent full scans.
    let mut outward_min = exact_argmax(hull, (-first_basis.outward.0, -first_basis.outward.1));
    let mut along_max = advance_max(hull, first_basis.along, outward_min);
    let mut outward_max = advance_max(hull, first_basis.outward, along_max);
    let mut along_min = advance_max(
        hull,
        (-first_basis.along.0, -first_basis.along.1),
        outward_max,
    );
    let mut best: Option<RankedCandidate> = None;
    for edge in 0..count {
        let basis = Basis::from_edge(hull[edge], hull[(edge + 1) % count])
            .expect("strict convex hull edge has a basis");
        if edge != 0 {
            along_max = advance_max(hull, basis.along, along_max);
            outward_min = advance_max(hull, (-basis.outward.0, -basis.outward.1), outward_min);
            outward_max = advance_max(hull, basis.outward, outward_max);
            along_min = advance_max(hull, (-basis.along.0, -basis.along.1), along_min);
        }
        debug_assert!(same_projection(
            hull,
            basis.along,
            along_max % count,
            exact_argmax(hull, basis.along)
        ));
        debug_assert!(same_projection(
            hull,
            (-basis.outward.0, -basis.outward.1),
            outward_min % count,
            exact_argmax(hull, (-basis.outward.0, -basis.outward.1))
        ));
        debug_assert!(same_projection(
            hull,
            basis.outward,
            outward_max % count,
            exact_argmax(hull, basis.outward)
        ));
        debug_assert!(same_projection(
            hull,
            (-basis.along.0, -basis.along.1),
            along_min % count,
            exact_argmax(hull, (-basis.along.0, -basis.along.1))
        ));
        let mut candidate = RankedCandidate::new(hull, Candidate {
            basis,
            supports: Supports {
                along_min: along_min % count,
                along_max: along_max % count,
                outward_min: outward_min % count,
                outward_max: outward_max % count,
            },
        });
        if best
            .as_mut()
            .is_none_or(|current| candidate_area_order(hull, &mut candidate, current).is_lt())
        {
            best = Some(candidate);
        }
    }
    best.expect("strict convex hull has at least one edge")
        .candidate
}

fn same_projection(hull: &[Point], direction: (f64, f64), left: usize, right: usize) -> bool {
    projection_delta_order(direction, hull[left], hull[right]) == Ordering::Equal
}

fn exact_argmax(hull: &[Point], direction: (f64, f64)) -> usize {
    (1..hull.len()).fold(0, |best, index| {
        if projection_delta_order(direction, hull[best], hull[index]).is_gt() {
            index
        } else {
            best
        }
    })
}

fn advance_max(hull: &[Point], direction: (f64, f64), mut index: usize) -> usize {
    let count = hull.len();
    for _ in 0..count {
        let current = index % count;
        let next = (index + 1) % count;
        // Exact equality can cross a support plateau safely.  Stopping on a
        // rounded equality is the counterexample in `mrr_counterexamples.py`:
        // it can strand the pointer before a later strict increase.
        if projection_delta_order(direction, hull[current], hull[next]).is_lt() {
            break;
        }
        index += 1;
    }
    index
}

fn axis_aligned_hull_rectangle(hull: &[Point]) -> Option<[Point; 5]> {
    (hull.len() == 4
        && hull.iter().enumerate().all(|(index, point)| {
            let next = hull[(index + 1) % hull.len()];
            point.x.to_bits() == next.x.to_bits() || point.y.to_bits() == next.y.to_bits()
        }))
    .then(|| {
        let (min_x, max_x, min_y, max_y) = hull.iter().fold(
            (
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ),
            |(min_x, max_x, min_y, max_y), point| {
                (
                    min_x.min(point.x),
                    max_x.max(point.x),
                    min_y.min(point.y),
                    max_y.max(point.y),
                )
            },
        );
        [
            Point::new_unchecked_xy(min_x, min_y),
            Point::new_unchecked_xy(max_x, min_y),
            Point::new_unchecked_xy(max_x, max_y),
            Point::new_unchecked_xy(min_x, max_y),
            Point::new_unchecked_xy(min_x, min_y),
        ]
    })
}

fn exact_width(direction: (f64, f64), minimum: Point, maximum: Point) -> [u64; LIMBS] {
    let exact = exact_dot_delta(direction, minimum, maximum);
    let (sign, magnitude) = exact.signed_magnitude();
    debug_assert_eq!(sign, Ordering::Greater);
    magnitude
}

fn mul_68(left: &[u64; LIMBS], right: &[u64; LIMBS]) -> [u64; DOUBLE_LIMBS] {
    let mut result = [0_u64; DOUBLE_LIMBS];
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
            // above the right operand's *actual* top limb, not limb 68.
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

fn mul_136_68(left: &[u64; DOUBLE_LIMBS], right: &[u64; LIMBS]) -> [u64; TRIPLE_LIMBS] {
    let mut result = [0_u64; TRIPLE_LIMBS];
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
            // Carry must cross sparse zero limbs exactly as above.
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

fn add_double_magnitude(
    left: &[u64; DOUBLE_LIMBS],
    right: &[u64; DOUBLE_LIMBS],
) -> [u64; DOUBLE_LIMBS] {
    let mut result = [0_u64; DOUBLE_LIMBS];
    let mut carry = false;
    for index in 0..DOUBLE_LIMBS {
        let (sum, first_carry) = left[index].overflowing_add(right[index]);
        let (sum, second_carry) = sum.overflowing_add(u64::from(carry));
        result[index] = sum;
        carry = first_carry || second_carry;
    }
    debug_assert!(!carry);
    result
}

fn cmp_double_magnitude(left: &[u64; DOUBLE_LIMBS], right: &[u64; DOUBLE_LIMBS]) -> Ordering {
    for index in (0..DOUBLE_LIMBS).rev() {
        match left[index].cmp(&right[index]) {
            Ordering::Equal => {},
            ordering => return ordering,
        }
    }
    Ordering::Equal
}

fn nonzero_span(words: &[u64]) -> Option<(usize, usize)> {
    let first = words.iter().position(|&word| word != 0)?;
    let last = words
        .iter()
        .rposition(|&word| word != 0)
        .expect("nonzero first word");
    Some((first, last))
}

fn cmp_wide(left: &[u64; TRIPLE_LIMBS], right: &[u64; TRIPLE_LIMBS]) -> Ordering {
    for index in (0..TRIPLE_LIMBS).rev() {
        match left[index].cmp(&right[index]) {
            Ordering::Equal => {},
            order => return order,
        }
    }
    Ordering::Equal
}

fn candidate_area_order(
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
    let lhs = mul_136_68(&mul_68(&left_a, &left_o), &right_n);
    let rhs = mul_136_68(&mul_68(&right_a, &right_o), &left_n);
    cmp_wide(&lhs, &rhs)
}

/// A two-word filter over exact subtraction/product residuals.  The expensive
/// limb product below is reserved for scores within a conservative `O(eps²)`
/// neighbourhood.  This is the ordinary caliper path: no allocation and no
/// input-coordinate cancellation even at large translations.
fn double_double_score_order(
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

type DoubleDouble = (f64, f64);

fn double_double_score(hull: &[Point], candidate: Candidate) -> Option<DoubleDouble> {
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

fn double_double_delta(
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

/// Dekker two-product, declining above the splittable range.
///
/// NOTE — this is the most conservative of the crate's four private error-free
/// transform layers, and deliberately so: it declines (`None`) once an operand
/// exceeds `f64::MAX / SPLITTER` (~1.34e300), where `geometry/segments/exact.rs`
/// rescales by a power of two and `geometry/area.rs` / `grid/affine_source.rs`
/// use FMA, all three staying exact there. The callers here treat `None` as
/// "no certificate", which is sound — it can only widen a result, never produce
/// a wrong one — and `minimum_rotated_rectangle` was verified correct across
/// 1e299..1e307 with enclosure preserved. Unifying the four layers was
/// considered and rejected: they differ in return type, in failure policy
/// (infallible vs `Option`), and in overflow strategy, so a common helper would
/// need a generic over failure policy — new indirection in the crate's most
/// numerically-sensitive code to save ~45 lines. If that is ever revisited,
/// this bail is the one behavioural difference to reconcile, not just the
/// shared algebra.
fn two_product(left: f64, right: f64) -> Option<DoubleDouble> {
    const SPLITTER: f64 = 134_217_729.0;
    let product = left * right;
    if !product.is_finite() || left.abs() > f64::MAX / SPLITTER || right.abs() > f64::MAX / SPLITTER
    {
        return None;
    }
    let left_split = SPLITTER * left;
    let left_high = left_split - (left_split - left);
    let left_low = left - left_high;
    let right_split = SPLITTER * right;
    let right_high = right_split - (right_split - right);
    let right_low = right - right_high;
    Some((
        product,
        ((left_high * right_high - product) + left_high * right_low + left_low * right_high)
            + left_low * right_low,
    ))
}

fn double_double_add(left: DoubleDouble, right: DoubleDouble) -> DoubleDouble {
    let (sum, error) = two_sum(left.0, right.0);
    quick_two_sum(sum, error + left.1 + right.1)
}

fn double_double_sub(left: DoubleDouble, right: DoubleDouble) -> DoubleDouble {
    double_double_add(left, (-right.0, -right.1))
}

fn double_double_mul(left: DoubleDouble, right: DoubleDouble) -> Option<DoubleDouble> {
    let (product, error) = two_product(left.0, right.0)?;
    let correction = error + left.0 * right.1 + left.1 * right.0;
    correction
        .is_finite()
        .then(|| quick_two_sum(product, correction))
}

fn double_double_div(numerator: DoubleDouble, denominator: DoubleDouble) -> Option<DoubleDouble> {
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

fn quick_two_sum(left: f64, right: f64) -> DoubleDouble {
    let sum = left + right;
    (sum, right - (sum - left))
}

/// A certified floating filter for the exact score.  It deliberately charges
/// the input-coordinate products, rather than trusting a rounded
/// `(support - support)` residual.  At large translations that makes the
/// interval wider (and selects the exact fallback); for ordinary data it
/// resolves the comparison without allocating or multiplying limb arrays.
fn candidate_score_interval(hull: &[Point], candidate: Candidate) -> Option<Interval> {
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

fn directed_delta_interval(
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

fn two_diff(left: f64, right: f64) -> (f64, f64) {
    let difference = left - right;
    let right_virtual = left - difference;
    let left_virtual = difference + right_virtual;
    let right_error = right_virtual - right;
    let left_error = left - left_virtual;
    (difference, left_error + right_error)
}

fn candidate_score_terms(
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

impl Candidate {
    fn emit(self, hull: &[Point]) -> Result<[Point; 5]> {
        // The legacy formula intentionally omits /N.  It is retained only when
        // the rounded quadrilateral itself carries the constant-time actual
        // edge certificate below; this is the compatibility fast path, never
        // an enclosure test over the hull.
        if let Some(legacy) = self.legacy_corners(hull)
            && legacy_rectangle_is_certified(hull, legacy, self)
        {
            return Ok([legacy[0], legacy[1], legacy[2], legacy[3], legacy[0]]);
        }

        let supports = self.supports;
        let preferred = self.correct_corners(hull);
        let corners = [
            emit_corner(
                self.basis,
                SupportHalfplane {
                    gradient: (-self.basis.along.0, -self.basis.along.1),
                    support: hull[supports.along_min],
                },
                SupportHalfplane {
                    gradient: (-self.basis.outward.0, -self.basis.outward.1),
                    support: hull[supports.outward_min],
                },
                &preferred[0],
            ),
            emit_corner(
                self.basis,
                SupportHalfplane {
                    gradient: self.basis.along,
                    support: hull[supports.along_max],
                },
                SupportHalfplane {
                    gradient: (-self.basis.outward.0, -self.basis.outward.1),
                    support: hull[supports.outward_min],
                },
                &preferred[1],
            ),
            emit_corner(
                self.basis,
                SupportHalfplane {
                    gradient: self.basis.along,
                    support: hull[supports.along_max],
                },
                SupportHalfplane {
                    gradient: self.basis.outward,
                    support: hull[supports.outward_max],
                },
                &preferred[2],
            ),
            emit_corner(
                self.basis,
                SupportHalfplane {
                    gradient: (-self.basis.along.0, -self.basis.along.1),
                    support: hull[supports.along_min],
                },
                SupportHalfplane {
                    gradient: self.basis.outward,
                    support: hull[supports.outward_max],
                },
                &preferred[3],
            ),
        ];
        let corners: [Point; 4] = corners
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()?
            .try_into()
            .expect("exactly four MRR corners");
        Ok([corners[0], corners[1], corners[2], corners[3], corners[0]])
    }

    fn projection_scalars(self, hull: &[Point]) -> Option<(f64, f64, f64, f64)> {
        let supports = self.supports;
        let dot =
            |direction: (f64, f64), point: Point| direction.0 * point.x + direction.1 * point.y;
        let min_along = dot(self.basis.along, hull[supports.along_min]);
        let max_along = dot(self.basis.along, hull[supports.along_max]);
        let min_outward = dot(self.basis.outward, hull[supports.outward_min]);
        // Keep the legacy evaluation order: its raw output bytes are retained
        // whenever the actual-edge certificate succeeds.
        let max_outward =
            min_outward + (dot(self.basis.outward, hull[supports.outward_max]) - min_outward);
        (min_along.is_finite()
            && max_along.is_finite()
            && min_outward.is_finite()
            && max_outward.is_finite())
        .then_some((min_along, max_along, min_outward, max_outward))
    }

    fn legacy_corners(self, hull: &[Point]) -> Option<[Point; 4]> {
        let (minimum, maximum, base, ceiling) = self.projection_scalars(hull)?;
        let corner = |along: f64, outward: f64| {
            Point::new_unchecked_xy(
                self.basis.along.0 * along + self.basis.outward.0 * outward,
                self.basis.along.1 * along + self.basis.outward.1 * outward,
            )
        };
        let corners = [
            corner(minimum, base),
            corner(maximum, base),
            corner(maximum, ceiling),
            corner(minimum, ceiling),
        ];
        corners
            .iter()
            .all(|point| point.x.is_finite() && point.y.is_finite())
            .then_some(corners)
    }

    fn correct_corners(self, hull: &[Point]) -> [PreferredCorner; 4] {
        let supports = self.supports;
        let origin = hull[supports.along_min];
        [
            self.unframe_corner(origin, hull[supports.along_min], hull[supports.outward_min]),
            self.unframe_corner(origin, hull[supports.along_max], hull[supports.outward_min]),
            self.unframe_corner(origin, hull[supports.along_max], hull[supports.outward_max]),
            self.unframe_corner(origin, hull[supports.along_min], hull[supports.outward_max]),
        ]
    }

    /// Intersect selected support lines in an anisotropically scaled frame.
    /// X/Y scales are independent, and gradients transform contravariantly,
    /// so the original world-coordinate support lines remain the construction
    /// contract. This is four O(1) support-pair intersections, never a local
    /// hull or a later full-hull enclosure pass.
    fn unframe_corner(
        self,
        origin: Point,
        along_support: Point,
        outward_support: Point,
    ) -> PreferredCorner {
        let max_x = origin
            .x
            .abs()
            .max(along_support.x.abs())
            .max(outward_support.x.abs());
        let max_y = origin
            .y
            .abs()
            .max(along_support.y.abs())
            .max(outward_support.y.abs());
        let sx = axis_pow2_scale(max_x);
        let sy = axis_pow2_scale(max_y);
        let equal_scales = sx.to_bits() == sy.to_bits();
        let (origin_x, origin_y, along_x, along_y, outward_x, outward_y) = if sx.to_bits()
            == 1.0_f64.to_bits()
            && sy.to_bits() == 1.0_f64.to_bits()
            && origin.x.to_bits() == 0.0_f64.to_bits()
            && origin.y.to_bits() == 0.0_f64.to_bits()
        {
            (
                0.0,
                0.0,
                along_support.x,
                along_support.y,
                outward_support.x,
                outward_support.y,
            )
        } else {
            (
                origin.x * sx,
                origin.y * sy,
                along_support.x * sx,
                along_support.y * sy,
                outward_support.x * sx,
                outward_support.y * sy,
            )
        };
        // Equal axes are the isotropic degeneracy of this frame. Preserve its
        // established operation order exactly: that is the ordinary-path
        // byte-compatibility contract. Only a genuinely unequal frame needs
        // the contravariant gradient transform.
        let (along, outward) = if equal_scales {
            (self.basis.along, self.basis.outward)
        } else {
            (
                (self.basis.along.0 / sx, self.basis.along.1 / sy),
                (self.basis.outward.0 / sx, self.basis.outward.1 / sy),
            )
        };
        let along_residual = along.0 * (along_x - origin_x) + along.1 * (along_y - origin_y);
        let outward_residual =
            outward.0 * (outward_x - origin_x) + outward.1 * (outward_y - origin_y);
        let determinant = along.0 * outward.1 - along.1 * outward.0;
        let x_value = (origin_x * determinant + along_residual * outward.1
            - along.1 * outward_residual)
            / determinant
            / sx;
        let y_value = (origin_y * determinant + along.0 * outward_residual
            - along_residual * outward.0)
            / determinant
            / sy;
        let mut x_numerator = ExactSum::zero();
        x_numerator.add_product(1, origin_x, determinant);
        x_numerator.add_product(1, along_residual, outward.1);
        x_numerator.add_product(-1, along.1, outward_residual);
        let mut y_numerator = ExactSum::zero();
        y_numerator.add_product(1, origin_y, determinant);
        y_numerator.add_product(1, along.0, outward_residual);
        y_numerator.add_product(-1, along_residual, outward.0);
        let denominator = [determinant * sx, determinant * sy];
        debug_assert!(
            denominator
                .into_iter()
                .all(|value| value.is_finite() && value > 0.0)
        );
        PreferredCorner {
            point: Point::new_unchecked_xy(x_value, y_value),
            numerator: [x_numerator, y_numerator],
            denominator,
            anisotropic_frame: !equal_scales,
        }
    }
}

fn exact_cross_delta(start: Point, end: Point, current: Point, next: Point) -> ExactSum {
    // cross(end-start, next-current), expanded before any subtraction so this
    // remains exact even when an intermediate world-coordinate difference
    // overflows.
    let mut sum = ExactSum::zero();
    sum.add_product(1, end.x, next.y);
    sum.add_product(-1, start.x, next.y);
    sum.add_product(-1, end.x, current.y);
    sum.add_product(1, start.x, current.y);
    sum.add_product(-1, end.y, next.x);
    sum.add_product(1, start.y, next.x);
    sum.add_product(1, end.y, current.x);
    sum.add_product(-1, start.y, current.x);
    sum
}

/// Adaptive sign of `cross(end - start, next - current)`.  The fast branch is
/// the standard first-stage determinant filter: every subtraction and product
/// is normal (or an exact zero), and the determinant clears a deliberately
/// conservative rounding bound.  All cancellation, subnormal, and overflow
/// cases retain the fixed-limb dyadic expansion below.
///
/// This is the certificate's ordinary path.  It is not a weaker local test:
/// the returned sign is exact whenever it returns from the filter.
fn cross_delta_order(start: Point, end: Point, current: Point, next: Point) -> Ordering {
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    let px = next.x - current.x;
    let py = next.y - current.y;
    let left = dx * py;
    let right = dy * px;
    let estimate = left - right;
    let magnitude = left.abs() + right.abs();
    let ordinary = [dx, dy, px, py, left, right]
        .into_iter()
        .all(|value| value.is_normal() || value == 0.0)
        && magnitude.is_normal();
    if ordinary && estimate.abs() > magnitude * PROJECTION_ERRBOUND {
        return estimate.total_cmp(&0.0);
    }
    exact_cross_delta(start, end, current, next).ordering()
}

/// Sound O(1) certificate for preserving a legacy unnormalised rectangle.
/// Each retained support must be locally minimal for its *actual emitted
/// edge*.  Convexity turns that local statement into global support truth.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn legacy_rectangle_is_certified(
    hull: &[Point],
    corners: [Point; 4],
    candidate: Candidate,
) -> bool {
    if !corners
        .iter()
        .all(|point| point.x.is_finite() && point.y.is_finite())
    {
        return false;
    }
    let count = hull.len();
    let support = [
        candidate.supports.outward_min,
        candidate.supports.along_max,
        candidate.supports.outward_max,
        candidate.supports.along_min,
    ];
    for edge in 0..4 {
        let start = corners[edge];
        let end = corners[(edge + 1) % 4];
        let index = support[edge];
        let previous = hull[(index + count - 1) % count];
        let current = hull[index];
        let next = hull[(index + 1) % count];
        if cross_delta_order(start, end, current, previous).is_lt()
            || cross_delta_order(start, end, current, next).is_lt()
        {
            return false;
        }
        // The support must lie on the inside of its actual edge.  The edge
        // orientation is CCW, so a negative cross is the outside halfplane.
        let mut inside = ExactSum::zero();
        inside.add_product(1, end.x, current.y);
        inside.add_product(-1, start.x, current.y);
        inside.add_product(-1, end.y, current.x);
        inside.add_product(1, start.y, current.x);
        inside.add_product(-1, end.x, start.y);
        inside.add_product(1, start.x, end.y);
        if inside.ordering().is_lt() {
            return false;
        }
    }
    (0..4).all(|edge| {
        orientation(
            corners[edge],
            corners[(edge + 1) % 4],
            corners[(edge + 2) % 4],
        ) == Orientation::CounterClockwise
    })
}

#[derive(Clone, Copy)]
struct SupportHalfplane {
    gradient: (f64, f64),
    support: Point,
}

fn support_margin(halfplane: SupportHalfplane, candidate: Point) -> ExactSum {
    exact_dot_delta(halfplane.gradient, halfplane.support, candidate)
}

/// Emit a finite point in the outward intersection of two support halfplanes.
/// The preferred inverse is accepted only after its two exact residuals prove
/// it lies in the wedge.  The repair shifts one coordinate in a shared outward
/// direction; every direction and rounding choice is determined by the exact
/// residual, not by a later hull predicate.
fn emit_corner(
    _basis: Basis,
    first: SupportHalfplane,
    second: SupportHalfplane,
    preferred: &PreferredCorner,
) -> Result<Point> {
    if (first.gradient.0 == 0.0 || first.gradient.1 == 0.0)
        && let Some(axis_corner) = axis_corner(first, second)
    {
        return Ok(axis_corner);
    }
    if preferred.point.x.is_finite()
        && preferred.point.y.is_finite()
        && !projection_delta_order(first.gradient, first.support, preferred.point).is_lt()
        && !projection_delta_order(second.gradient, second.support, preferred.point).is_lt()
    {
        // The adaptive sign filter above is an exact dyadic predicate when it
        // returns.  In ordinary cases it proves both support inequalities
        // without materialising the 68-limb margins used by the repair lane.
        return Ok(preferred.point);
    }

    // The anisotropic inverse can land just beyond the finite range even
    // though a nearby coordinate on one of its two exact support lines is
    // representable.  Search the two preferred coordinate boundaries only:
    // this is four support-pair intersections, not a hull predicate.  Each
    // candidate is accepted by the same exact two-halfplane certificate used
    // below, so construction—not a scan over source vertices—establishes
    // enclosure and minimality.
    if preferred.point.x.is_finite()
        && preferred.point.y.is_finite()
        && (preferred.anisotropic_frame
            || preferred.point.x.to_bits() & !(1_u64 << 63) == f64::MAX.to_bits()
            || preferred.point.y.to_bits() & !(1_u64 << 63) == f64::MAX.to_bits())
    {
        for (axis, fixed) in [(Axis::X, preferred.point.x), (Axis::Y, preferred.point.y)] {
            if let Some(candidate) = finite_point_on_edge(first, second, axis, fixed, preferred) {
                return Ok(candidate);
            }
        }
    }

    if preferred.point.x.is_finite() && preferred.point.y.is_finite() {
        let first_margin = support_margin(first, preferred.point);
        let second_margin = support_margin(second, preferred.point);
        if let Some(candidate) =
            directed_axis_repair(first, second, preferred, &first_margin, &second_margin)
        {
            return Ok(candidate);
        }
    }
    finite_support_wedge_point(first, second, preferred).ok_or_else(|| {
        GeometryErrorKind::message(
            "minimum_rotated_rectangle result is not representable with finite coordinates",
        )
    })
}

fn axis_corner(first: SupportHalfplane, second: SupportHalfplane) -> Option<Point> {
    let (along, outward) = if first.gradient.0 == 0.0 || first.gradient.1 == 0.0 {
        (first, second)
    } else {
        (second, first)
    };
    if along.gradient.1 == 0.0 && outward.gradient.0 == 0.0 {
        Some(Point::new_unchecked_xy(along.support.x, outward.support.y))
    } else if along.gradient.0 == 0.0 && outward.gradient.1 == 0.0 {
        Some(Point::new_unchecked_xy(outward.support.x, along.support.y))
    } else {
        None
    }
}

fn directed_axis_repair(
    first: SupportHalfplane,
    second: SupportHalfplane,
    preferred: &PreferredCorner,
    first_margin: &ExactSum,
    second_margin: &ExactSum,
) -> Option<Point> {
    // A perpendicular pair always has a Cartesian component with identical
    // nonzero signs.  Moving in that signed direction improves both margins.
    let mut best = None;
    for (axis, first_coefficient, second_coefficient) in [
        (Axis::X, first.gradient.0, second.gradient.0),
        (Axis::Y, first.gradient.1, second.gradient.1),
    ] {
        if first_coefficient == 0.0
            || second_coefficient == 0.0
            || first_coefficient.is_sign_negative() != second_coefficient.is_sign_negative()
        {
            continue;
        }
        let sign = if first_coefficient.is_sign_negative() {
            -1.0
        } else {
            1.0
        };
        let first_deficit = quotient_for_outward_deficit(first_margin, first_coefficient * sign)?;
        let second_deficit =
            quotient_for_outward_deficit(second_margin, second_coefficient * sign)?;
        let distance = first_deficit.max(second_deficit).max(0.0);
        let old = match axis {
            Axis::X => preferred.point.x,
            Axis::Y => preferred.point.y,
        };
        let (sum, residual) = two_sum(old, sign * distance);
        // `two_sum` gives `old + signed_distance == sum + residual` exactly.
        // Round only when that residual leaves `sum` short in the selected
        // outward direction; an unconditional ULP would over-pad exact sums.
        let shifted = if sign.is_sign_positive() && residual > 0.0 {
            sum.next_up()
        } else if sign.is_sign_negative() && residual < 0.0 {
            sum.next_down()
        } else {
            sum
        };
        if !shifted.is_finite() {
            continue;
        }
        let candidate = match axis {
            Axis::X => Point::new_unchecked_xy(shifted, preferred.point.y),
            Axis::Y => Point::new_unchecked_xy(preferred.point.x, shifted),
        };
        if !support_margin(first, candidate).ordering().is_lt()
            && !support_margin(second, candidate).ordering().is_lt()
            && best.is_none_or(|current| displacement_order(candidate, current, preferred).is_lt())
        {
            best = Some(candidate);
        }
    }
    best
}

fn quotient_for_outward_deficit(margin: &ExactSum, positive_coefficient: f64) -> Option<f64> {
    debug_assert!(positive_coefficient > 0.0);
    if !margin.ordering().is_lt() {
        return Some(0.0);
    }
    let mut deficit = margin.clone();
    std::mem::swap(&mut deficit.positive, &mut deficit.negative);
    let interval = exact_quotient_interval(&deficit, positive_coefficient);
    interval.hi.is_finite().then_some(interval.hi)
}

#[derive(Clone, Copy)]
enum Axis {
    X,
    Y,
}

impl Axis {
    const fn index(self) -> usize {
        match self {
            Self::X => 0,
            Self::Y => 1,
        }
    }
}

/// Adjacent binary64 values bracketing `numerator / denominator` exactly.
/// This fixed 63-step search is used only on an overflowing preferred corner.
fn exact_quotient_interval(numerator: &ExactSum, denominator: f64) -> Interval {
    debug_assert!(denominator.is_finite() && denominator != 0.0);
    let (numerator_sign, magnitude) = numerator.signed_magnitude();
    if numerator_sign == Ordering::Equal {
        return Interval { lo: 0.0, hi: 0.0 };
    }
    let negative = (numerator_sign == Ordering::Less) ^ denominator.is_sign_negative();
    let positive = positive_quotient_interval(&magnitude, denominator.abs());
    if negative {
        Interval {
            lo: -positive.hi,
            hi: -positive.lo,
        }
    } else {
        positive
    }
}

fn positive_quotient_interval(magnitude: &[u64; LIMBS], denominator: f64) -> Interval {
    match magnitude_product_order(magnitude, denominator, f64::MAX) {
        Ordering::Greater => {
            return Interval {
                lo: f64::MAX,
                hi: f64::INFINITY,
            };
        },
        Ordering::Equal => {
            return Interval {
                lo: f64::MAX,
                hi: f64::MAX,
            };
        },
        Ordering::Less => {},
    }
    // The exact numerator has a two-float bracket.  One directed hardware
    // division of its upper endpoint therefore gives a valid ceiling, and
    // in the ordinary case it is at most three representable values above the
    // exact quotient.  Walk that tiny neighbourhood with exact products;
    // the 63-step format-wide search remains the finite, constant fallback
    // for subnormal and unusually wide cases.
    let numerator = positive_interval(magnitude);
    let mut upper = (numerator.hi / denominator).next_up();
    if upper.is_finite() && !magnitude_product_order(magnitude, denominator, upper).is_gt() {
        for _ in 0..3 {
            let lower = upper.next_down();
            match magnitude_product_order(magnitude, denominator, lower) {
                Ordering::Less => upper = lower,
                Ordering::Equal => {
                    return Interval {
                        lo: lower,
                        hi: lower,
                    };
                },
                Ordering::Greater => {
                    return Interval {
                        lo: lower,
                        hi: upper,
                    };
                },
            }
        }
    }
    positive_quotient_interval_binary(magnitude, denominator)
}

fn positive_quotient_interval_binary(magnitude: &[u64; LIMBS], denominator: f64) -> Interval {
    const MAX_BITS: u64 = f64::MAX.to_bits();
    let mut low = 0_u64;
    let mut high = MAX_BITS;
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        let candidate = f64::from_bits(middle);
        if magnitude_product_order(magnitude, denominator, candidate) == Ordering::Less {
            high = middle;
        } else {
            low = middle;
        }
    }
    let floor = f64::from_bits(low);
    if magnitude_product_order(magnitude, denominator, floor) == Ordering::Equal {
        Interval {
            lo: floor,
            hi: floor,
        }
    } else {
        Interval {
            lo: floor,
            hi: f64::from_bits(high),
        }
    }
}

fn magnitude_product_order(magnitude: &[u64; LIMBS], denominator: f64, candidate: f64) -> Ordering {
    let mut product = ExactSum::zero();
    product.add_product(1, denominator, candidate);
    cmp_magnitude(magnitude, &product.positive)
}

/// Finite feasibility is decided on the four square boundaries.  We enumerate
/// all boundary candidates and choose the exact minimum squared Euclidean
/// displacement from the preferred corner; equal exact distances break by
/// total-order x/y bits.  This is deterministic, constant work, and unlike a
/// fixed boundary order is an actual minimum-displacement rule.
fn finite_support_wedge_point(
    first: SupportHalfplane,
    second: SupportHalfplane,
    preferred: &PreferredCorner,
) -> Option<Point> {
    let mut best: Option<Point> = None;
    for (axis, fixed) in [
        (Axis::X, f64::MAX),
        (Axis::X, -f64::MAX),
        (Axis::Y, f64::MAX),
        (Axis::Y, -f64::MAX),
    ] {
        if let Some(candidate) = finite_point_on_edge(first, second, axis, fixed, preferred)
            && best.is_none_or(|current| displacement_order(candidate, current, preferred).is_lt())
        {
            best = Some(candidate);
        }
    }
    best
}

fn finite_point_on_edge(
    first: SupportHalfplane,
    second: SupportHalfplane,
    fixed_axis: Axis,
    fixed: f64,
    preferred: &PreferredCorner,
) -> Option<Point> {
    let mut lower = -f64::MAX;
    let mut upper = f64::MAX;
    for halfplane in [first, second] {
        let (fixed_coefficient, free_coefficient, fixed_support, free_support) = match fixed_axis {
            Axis::X => (
                halfplane.gradient.0,
                halfplane.gradient.1,
                halfplane.support.x,
                halfplane.support.y,
            ),
            Axis::Y => (
                halfplane.gradient.1,
                halfplane.gradient.0,
                halfplane.support.y,
                halfplane.support.x,
            ),
        };
        if free_coefficient == 0.0 {
            let satisfies = match fixed_coefficient.total_cmp(&0.0) {
                Ordering::Greater => fixed >= fixed_support,
                Ordering::Less => fixed <= fixed_support,
                Ordering::Equal => true,
            };
            if !satisfies {
                return None;
            }
            continue;
        }
        // Construct the threshold as an anchored exact dyadic sum.  In
        // particular never evaluate `(fixed - support)` in f64.
        let mut numerator = ExactSum::zero();
        numerator.add_product(1, free_coefficient, free_support);
        numerator.add_product(-1, fixed_coefficient, fixed);
        numerator.add_product(1, fixed_coefficient, fixed_support);
        let threshold = exact_quotient_interval(&numerator, free_coefficient);
        if free_coefficient > 0.0 {
            lower = lower.max(threshold.hi);
        } else {
            upper = upper.min(threshold.lo);
        }
        if !lower.is_finite() || !upper.is_finite() || lower > upper {
            return None;
        }
    }
    let free = nearest_feasible_coordinate(
        preferred,
        match fixed_axis {
            Axis::X => Axis::Y,
            Axis::Y => Axis::X,
        },
        lower,
        upper,
    );
    let candidate = match fixed_axis {
        Axis::X => Point::new_unchecked_xy(fixed, free),
        Axis::Y => Point::new_unchecked_xy(free, fixed),
    };
    (!support_margin(first, candidate).ordering().is_lt()
        && !support_margin(second, candidate).ordering().is_lt())
    .then_some(candidate)
}

fn nearest_feasible_coordinate(
    preferred: &PreferredCorner,
    axis: Axis,
    lower: f64,
    upper: f64,
) -> f64 {
    debug_assert!(lower.is_finite() && upper.is_finite() && lower <= upper);
    if !preferred.coordinate_order(axis, lower).is_lt() {
        return lower;
    }
    if !preferred.coordinate_order(axis, upper).is_gt() {
        return upper;
    }
    let mut low = total_order_bits(lower);
    let mut high = total_order_bits(upper);
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        let candidate = f64_from_total_order_bits(middle);
        if preferred.coordinate_order(axis, candidate).is_lt() {
            low = middle;
        } else {
            high = middle;
        }
    }
    let floor = f64_from_total_order_bits(low);
    let ceiling = f64_from_total_order_bits(high);
    let floor_delta = preferred
        .coordinate_difference(axis, floor)
        .signed_magnitude()
        .1;
    let ceiling_delta = preferred
        .coordinate_difference(axis, ceiling)
        .signed_magnitude()
        .1;
    match cmp_magnitude(&floor_delta, &ceiling_delta) {
        Ordering::Greater => ceiling,
        Ordering::Equal if total_order_bits(ceiling) < total_order_bits(floor) => ceiling,
        Ordering::Equal | Ordering::Less => floor,
    }
}

fn displacement_order(left: Point, right: Point, preferred: &PreferredCorner) -> Ordering {
    let left_distance = exact_squared_distance(left, preferred);
    let right_distance = exact_squared_distance(right, preferred);
    match cmp_double_magnitude(&left_distance, &right_distance) {
        Ordering::Equal => total_order_bits(left.x)
            .cmp(&total_order_bits(right.x))
            .then_with(|| total_order_bits(left.y).cmp(&total_order_bits(right.y))),
        ordering => ordering,
    }
}

fn exact_squared_distance(point: Point, preferred: &PreferredCorner) -> [u64; DOUBLE_LIMBS] {
    let x = preferred
        .coordinate_difference(Axis::X, point.x)
        .signed_magnitude()
        .1;
    let y = preferred
        .coordinate_difference(Axis::Y, point.y)
        .signed_magnitude()
        .1;
    add_double_magnitude(&mul_68(&x, &x), &mul_68(&y, &y))
}

const fn total_order_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits >> 63 == 0 {
        bits | (1_u64 << 63)
    } else {
        !bits
    }
}

const fn f64_from_total_order_bits(bits: u64) -> f64 {
    if bits >> 63 == 0 {
        f64::from_bits(!bits)
    } else {
        f64::from_bits(bits & !(1_u64 << 63))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(x: f64, y: f64) -> Point {
        Point::new_unchecked_xy(x, y)
    }

    #[test]
    fn exact_projection_breaks_rounded_plateau() {
        let points = [
            point(0.0, 0.0),
            point(1.0, 1.0),
            point(0.999_999_999_999_989, 2.999_999_999_999_955_6),
            point(0.999_999_999_999_988_9, 2.999_999_999_999_956),
            point(0.999_999_999_999_986_8, 2.999_999_999_999_960_5),
        ];
        let direction = (2.0_f64.sqrt().recip(), 2.0_f64.sqrt().recip());
        assert_ne!(
            projection_delta_order(direction, points[2], points[3]),
            Ordering::Equal
        );
        assert_ne!(
            projection_delta_order(direction, points[3], points[4]),
            Ordering::Equal
        );

        let inversion = [
            point(0.0, 0.0),
            point(1.0, 1.0),
            point(0.999_999_999_999_978_5, 2.999_999_999_999_911),
            point(0.999_999_999_999_978_1, 2.999_999_999_999_911_6),
            point(0.999_999_999_999_976_2, 2.999_999_999_999_913_4),
        ];
        assert!(projection_delta_order(direction, inversion[2], inversion[3]).is_gt());
    }

    #[test]
    fn exact_advance_crosses_the_rounded_support_plateau() {
        let points = [
            point(0.0, 0.0),
            point(1.0, 1.0),
            point(0.999_999_999_999_989, 2.999_999_999_999_955_6),
            point(0.999_999_999_999_988_9, 2.999_999_999_999_956),
            point(0.999_999_999_999_986_8, 2.999_999_999_999_960_5),
        ];
        let direction = (2.0_f64.sqrt().recip(), 2.0_f64.sqrt().recip());
        assert_eq!(advance_max(&points, direction, 1), 4);
    }

    #[test]
    fn first_caliper_support_order_matches_global_exact_extrema() {
        // `select_minimum_candidate`'s debug assertions compare every
        // unwrapped support pointer with an independent full exact scan.  The
        // affine family below exercises rotations, anisotropy, and offsets
        // while this test supplies several hull sizes.
        for vertices in [3, 5, 12, 31, 64] {
            for (angle, offset) in [(0.0_f64, 0.0), (0.37, 5.0e6), (1.17, -1.0e12)] {
                let (sin, cos) = angle.sin_cos();
                let points = (0..vertices)
                    .map(|index| {
                        let theta = std::f64::consts::TAU * index as f64 / vertices as f64;
                        let x = 7.0 * theta.cos();
                        let y = 2.0 * theta.sin();
                        point(offset + cos * x - sin * y, -offset + sin * x + cos * y)
                    })
                    .collect::<Vec<_>>();
                let hull = super::super::monotone_chain_hull(&points);
                assert_eq!(hull.len(), vertices);
                let _ = select_minimum_candidate(&hull);
            }
        }
    }

    #[test]
    fn finite_wedge_does_not_reject_ideal_overflow() {
        let previous = f64::MAX.next_down();
        let ulp = f64::MAX - previous;
        let mut x_numerator = ExactSum::zero();
        x_numerator.add_product(1, f64::MAX, 1.0);
        x_numerator.add_product(1, f64::MAX, 1.0);
        let preferred = PreferredCorner {
            point: point(f64::INFINITY, 0.0),
            numerator: [x_numerator, ExactSum::zero()],
            denominator: [1.0; 2],
            anisotropic_frame: false,
        };
        let candidate = finite_support_wedge_point(
            SupportHalfplane {
                gradient: (1.0, 2.0),
                support: point(previous, ulp),
            },
            SupportHalfplane {
                gradient: (-2.0, 1.0),
                support: point(f64::MAX, -2.0 * ulp),
            },
            &preferred,
        )
        .expect("finite outward wedge");
        assert_eq!(candidate.x.to_bits(), f64::MAX.to_bits());
        assert_eq!(candidate.y.to_bits(), (ulp / 2.0).to_bits());
    }

    #[test]
    fn sparse_wide_multiplication_propagates_carries_at_the_actual_top_limb() {
        let mut left = [0_u64; LIMBS];
        left[0] = u64::MAX;
        let mut right = [0_u64; LIMBS];
        right[0] = u64::MAX;
        right[2] = 1;
        let product = mul_68(&left, &right);
        assert_eq!(product[0], 1);
        assert_eq!(product[1], u64::MAX - 1);
        assert_eq!(product[2], u64::MAX);
        assert!(product[3..].iter().all(|&word| word == 0));
    }

    #[test]
    fn overflowing_preferred_keeps_its_exact_magnitude_for_displacement() {
        let mut x_numerator = ExactSum::zero();
        x_numerator.add_product(1, f64::MAX, 1.0);
        x_numerator.add_product(1, f64::MAX, 1.0);
        let preferred = PreferredCorner {
            point: point(f64::INFINITY, 0.0),
            numerator: [x_numerator, ExactSum::zero()],
            denominator: [1.0; 2],
            anisotropic_frame: false,
        };
        // Saturating the ideal x to MAX would choose `far`; the exact ideal is
        // near 2*MAX, for which the square-boundary point is strictly nearer.
        let boundary = point(f64::MAX, f64::MAX);
        let far = point(-f64::MAX, 0.0);
        assert_eq!(
            displacement_order(boundary, far, &preferred),
            Ordering::Less
        );
    }

    #[test]
    fn actual_edge_certificate_accepts_a_nonzero_origin_rectangle() {
        let rectangle = [
            point(1.0, 2.0),
            point(5.0, 2.0),
            point(5.0, 7.0),
            point(1.0, 7.0),
        ];
        let rectangle = super::super::monotone_chain_hull(&rectangle);
        let candidate = select_minimum_candidate(&rectangle);
        assert!(legacy_rectangle_is_certified(
            &rectangle,
            candidate.legacy_corners(&rectangle).unwrap(),
            candidate,
        ));
    }

    #[test]
    fn directed_quotient_neighbourhood_matches_the_format_wide_oracle() {
        for (numerator, denominator) in [
            (1.0, 3.0),
            (f64::from_bits(1), f64::MIN_POSITIVE),
            (f64::MIN_POSITIVE, f64::from_bits(1)),
            (f64::MAX.next_down(), 1.5),
            (
                f64::from_bits((1_523_u64 << 52) | 1),
                f64::from_bits((523_u64 << 52) | 1),
            ),
            (
                f64::from_bits((523_u64 << 52) | 1),
                f64::from_bits((1_523_u64 << 52) | 1),
            ),
        ] {
            let mut exact = ExactSum::zero();
            exact.add_product(1, numerator, 1.0);
            let (_, magnitude) = exact.signed_magnitude();
            let fast = positive_quotient_interval(&magnitude, denominator);
            let oracle = positive_quotient_interval_binary(&magnitude, denominator);
            assert_eq!(
                (fast.lo.to_bits(), fast.hi.to_bits()),
                (oracle.lo.to_bits(), oracle.hi.to_bits())
            );
        }
    }

    #[test]
    fn scaled_triangle_has_no_finite_minimum_rectangle() {
        let scale = 1.01e308;
        let hull = super::super::monotone_chain_hull(&[
            point(-1.75 * scale, -1.75 * scale),
            point(-1.5 * scale, -0.25 * scale),
            point(-1.75 * scale, -1.5 * scale),
        ]);
        let error = minimum_area_rectangle(&hull).expect_err("unrepresentable support wedge");
        assert_eq!(
            error.to_string(),
            "minimum_rotated_rectangle result is not representable with finite coordinates",
        );
    }

    #[test]
    fn score_divides_by_the_exact_rounded_basis_norm() {
        // A temporary witness script showed that exact support widths alone choose edge
        // zero, while the physical area `(da * do) / N` chooses edge one.
        let hull = [
            point(
                f64::from_bits(0x4010_2EB2_5C0F_1A90),
                f64::from_bits(0xC04F_EF9D_E158_CAB8),
            ),
            point(
                f64::from_bits(0x404A_A55F_8BBE_E593),
                f64::from_bits(0x4041_B844_B820_564B),
            ),
            point(
                f64::from_bits(0xC04C_AB35_D740_C8E3),
                f64::from_bits(0x403C_6EB2_5270_E8CA),
            ),
        ];
        assert_eq!(select_minimum_candidate(&hull).supports.outward_min, 1);
    }
}
