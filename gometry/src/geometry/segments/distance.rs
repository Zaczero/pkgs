//! Certified point-to-segment and segment-pair distance operations.

#[cfg(test)]
use super::contact::segment_contact_point;
use super::projection::{
    classify_segment_projection_exact, exact_source_squared, point_segment_metric,
    projection_difference, source_difference, source_product, source_projection_squared,
};
#[cfg(test)]
use super::projection::{point_segment_distance_and_projection, segment_projection};
use super::{
    DdInterval, ExactClassifiedSegmentProjection, ExactDistanceWitness, ExactDyadic,
    ExactSquaredDistance, PointSegmentMetric, PointSegmentSelection, PointSegmentWitnessKind,
    Segment, SegmentProjection, SelectionProjectionSource, SourcePair, SquaredDistanceFilter,
    SquaredDistanceKey, WitnessRole, XY, dd_strict_cmp,
};
use crate::error::{ErrorKind, Result};
use crate::geometry::numerics::product::two_sum;
use crate::geometry::tessellation::exact::{compare_ratios, round_sqrt_ratio};
use crate::geometry::{Ordering, same_point};

#[cfg(test)]
#[derive(Clone, Debug)]
enum SegmentWitness {
    Start,
    Projection(SegmentProjection),
    End,
    RoundedIntersection(XY),
}

#[cfg(test)]
impl SegmentWitness {
    fn xy(&self, segment: Segment) -> XY {
        match self {
            Self::Start => segment.start,
            Self::Projection(projection) => projection.interpolate_xy(segment).unwrap(),
            Self::End => segment.end,
            Self::RoundedIntersection(point) => *point,
        }
    }
}

#[cfg(test)]
struct SegmentPairNearest {
    left: SegmentWitness,
    right: SegmentWitness,
    distance_key: SquaredDistanceKey,
}

#[cfg(test)]
pub(in crate::geometry) fn segment_nearest_points(
    left: Segment,
    right: Segment,
) -> Result<(XY, XY)> {
    let nearest = segment_pair_nearest(left, right)?;
    Ok((nearest.left.xy(left), nearest.right.xy(right)))
}

#[cfg(test)]
fn segment_pair_nearest(left: Segment, right: Segment) -> Result<SegmentPairNearest> {
    // One contact computation, not an `intersects` probe followed by a
    // separate placement. When the pair meets, the shared point IS the
    // nearest pair; otherwise fall through to the projection feet below.
    if let Some(point) = segment_contact_point(left, right)? {
        return Ok(SegmentPairNearest {
            left: SegmentWitness::RoundedIntersection(point),
            right: SegmentWitness::RoundedIntersection(point),
            distance_key: point_pair_distance_key(point, point),
        });
    }

    let left_start = segment_projection(left.start, right)?;
    let left_end = segment_projection(left.end, right)?;
    let right_start = segment_projection(right.start, left)?;
    let right_end = segment_projection(right.end, left)?;
    let candidates = [
        SegmentPairNearest {
            left: SegmentWitness::Start,
            right: SegmentWitness::Projection(left_start),
            distance_key: point_segment_distance_key(left.start, right)?,
        },
        SegmentPairNearest {
            left: SegmentWitness::End,
            right: SegmentWitness::Projection(left_end),
            distance_key: point_segment_distance_key(left.end, right)?,
        },
        SegmentPairNearest {
            left: SegmentWitness::Projection(right_start),
            right: SegmentWitness::Start,
            distance_key: point_segment_distance_key(right.start, left)?,
        },
        SegmentPairNearest {
            left: SegmentWitness::Projection(right_end),
            right: SegmentWitness::End,
            distance_key: point_segment_distance_key(right.end, left)?,
        },
    ];
    let mut candidates = candidates.into_iter();
    let mut best = candidates.next().expect("four segment-pair candidates");
    for candidate in candidates {
        if candidate.distance_key.cmp(&best.distance_key)?.is_lt() {
            best = candidate;
        }
    }
    Ok(best)
}

impl SquaredDistanceFilter {
    const UNIT_ROUNDOFF: f64 = f64::EPSILON / 2.0;
    // Outward reciprocals of 1±8u and 1±4u. Multiplication plus nextafter
    // encloses the same quotient without dividing for each bound.
    const POINT_PAIR_LOWER_SCALE: f64 = f64::from_bits(0x3FEF_FFFF_FFFF_FFF8);
    const POINT_PAIR_UPPER_SCALE: f64 = f64::from_bits(0x3FF0_0000_0000_0005);
    const STRIP_NORM_LOWER_SCALE: f64 = f64::from_bits(0x3FEF_FFFF_FFFF_FFFC);
    const STRIP_NORM_UPPER_SCALE: f64 = f64::from_bits(0x3FF0_0000_0000_0003);
    const DOT_ERROR: FilterValue = FilterValue(8.0 * Self::UNIT_ROUNDOFF);
    const SEGMENT_DOT_ERROR: FilterValue = FilterValue(16.0 * Self::UNIT_ROUNDOFF);
    const SIGN_MASK: u64 = 1 << 63;
    // Keep each source operand and subtraction in [2^-200, 2^200], so
    // degree-four products stay normal and finite; unsafe stages decline.
    const MIN_OPERAND: f64 = f64::from_bits(0x3370_0000_0000_0000);
    const MAX_OPERAND: f64 = f64::from_bits(0x4C70_0000_0000_0000);
    // Strip cross-products admit only finite positive incumbent intervals in
    // [2^-800, 2^800]; product operations still check their own result class.
    const MIN_INCUMBENT: f64 = f64::from_bits(0x0DF0_0000_0000_0000);
    const MAX_INCUMBENT: f64 = f64::from_bits(0x71F0_0000_0000_0000);

    /// A point-pair key has four error-bearing stages (coordinate residuals,
    /// their squares, and the sum). `gamma(4)` bounds their normal relative
    /// error; the final one-ulp widening absorbs the bound arithmetic itself.
    pub(super) fn point_pair(squared: f64) -> Self {
        const UNIT_ROUNDOFF: f64 = f64::EPSILON / 2.0;
        const GAMMA_4: f64 = 4.0 * UNIT_ROUNDOFF / (1.0 - 4.0 * UNIT_ROUNDOFF);
        Self::source_squared(squared, GAMMA_4)
    }

    fn nonnegative(squared: f64, lower_scale: f64, upper_scale: f64) -> Option<Self> {
        if is_zero_bits(squared) {
            return Some(Self { lo: 0.0, hi: 0.0 });
        }
        if !is_normal_bits(squared) {
            return None;
        }
        // If the rounded value has relative error at most g, the source value
        // lies between squared/(1+g) and squared/(1-g).
        let lo = (squared * lower_scale).next_down().max(0.0);
        let hi = (squared * upper_scale).next_up();
        (is_normal_bits(lo) && is_normal_bits(hi)).then_some(Self { lo, hi })
    }

    pub(super) fn source_squared(squared: f64, relative_error: f64) -> Self {
        if !squared.is_finite() {
            return Self::unbounded();
        }
        let error = squared * relative_error;
        let lo = (squared - error).next_down().max(0.0);
        let hi = (squared + error).next_up();
        if hi.is_finite() {
            Self { lo, hi }
        } else {
            Self::unbounded()
        }
    }

    pub(super) const fn unbounded() -> Self {
        Self {
            lo: f64::NEG_INFINITY,
            hi: f64::INFINITY,
        }
    }

    pub(super) const fn bind(self, witness: ExactDistanceWitness) -> SquaredDistanceKey {
        SquaredDistanceKey {
            filter: self,
            witness,
        }
    }

    pub(in crate::geometry) const fn bind_witness(
        self,
        witness: ExactDistanceWitness,
    ) -> SquaredDistanceKey {
        self.bind(witness)
    }

    pub(super) fn disjoint_cmp(self, other: Self) -> Option<Ordering> {
        if self.hi < other.lo {
            Some(Ordering::Less)
        } else if other.hi < self.lo {
            Some(Ordering::Greater)
        } else {
            None
        }
    }

    pub(in crate::geometry) fn cmp_witness_key(
        self,
        witness: ExactDistanceWitness,
        other: &SquaredDistanceKey,
    ) -> Result<Ordering> {
        if let Some(order) = self.disjoint_cmp(other.filter) {
            #[cfg(test)]
            CLEARANCE_AABB_COMPARE_DECISIONS.with(|count| count.set(count.get().saturating_add(1)));
            return Ok(order);
        }
        #[cfg(test)]
        CLEARANCE_AABB_COMPARE_DECLINES.with(|count| count.set(count.get().saturating_add(1)));
        if witness.same_distance_as(other.witness) {
            return Ok(Ordering::Equal);
        }
        exact_selection_distance_cmp(witness, other.witness)
    }
}

/// A finite zero or normal scalar admitted to the filter's safe exponent band.
#[derive(Clone, Copy)]
struct FilterValue(f64);

impl FilterValue {
    fn new(value: f64) -> Option<Self> {
        filter_operand(value).then_some(Self(value))
    }
}

const fn filter_operand(value: f64) -> bool {
    let magnitude = magnitude_bits(value);
    if magnitude == 0 {
        return true;
    }
    let exponent = magnitude >> 52;
    let maximum = SquaredDistanceFilter::MAX_OPERAND.to_bits();
    exponent != 0x7FF
        && exponent >= (SquaredDistanceFilter::MIN_OPERAND.to_bits() >> 52)
        && (exponent < (maximum >> 52) || magnitude == maximum)
}

const fn magnitude_bits(value: f64) -> u64 {
    value.to_bits() & !SquaredDistanceFilter::SIGN_MASK
}

const fn is_zero_bits(value: f64) -> bool {
    magnitude_bits(value) == 0
}

const fn is_finite_bits(value: f64) -> bool {
    magnitude_bits(value) >> 52 != 0x7FF
}

const fn is_normal_or_zero_bits(value: f64) -> bool {
    let exponent = magnitude_bits(value) >> 52;
    exponent == 0 && magnitude_bits(value) == 0 || exponent != 0 && exponent != 0x7FF
}

const fn is_normal_bits(value: f64) -> bool {
    let exponent = magnitude_bits(value) >> 52;
    exponent != 0 && exponent != 0x7FF
}

const fn are_exact_opposites(left: f64, right: f64) -> bool {
    left.to_bits() ^ right.to_bits() == SquaredDistanceFilter::SIGN_MASK
}

const fn are_same_finite_number(left: f64, right: f64) -> bool {
    left.to_bits() == right.to_bits() || is_zero_bits(left) && is_zero_bits(right)
}

fn filter_difference(left: f64, right: f64) -> Option<FilterValue> {
    if !filter_operand(left) || !filter_operand(right) {
        return None;
    }
    let difference = left - right;
    if is_zero_bits(difference) {
        return are_same_finite_number(left, right).then_some(FilterValue(0.0));
    }
    FilterValue::new(difference)
}

fn filter_product(left: FilterValue, right: FilterValue) -> Option<f64> {
    let product = left.0 * right.0;
    if is_zero_bits(product) {
        return (is_zero_bits(left.0) || is_zero_bits(right.0)).then_some(0.0);
    }
    is_normal_bits(product).then_some(product)
}

fn normal_product(left: f64, right: f64) -> Option<f64> {
    // Both operands are normal-or-zero intermediates already admitted above.
    let product = left * right;
    if is_zero_bits(product) {
        return (is_zero_bits(left) || is_zero_bits(right)).then_some(0.0);
    }
    is_normal_bits(product).then_some(product)
}

fn normal_sum(left: f64, right: f64) -> Option<f64> {
    // Both operands are normal-or-zero intermediates already admitted above.
    let sum = left + right;
    if is_zero_bits(sum) {
        let both_zero = is_zero_bits(left) && is_zero_bits(right);
        return (both_zero || are_exact_opposites(left, right)).then_some(0.0);
    }
    is_normal_bits(sum).then_some(sum)
}

fn checked_normal_product(left: f64, right: f64) -> Option<f64> {
    if !is_normal_or_zero_bits(left) || !is_normal_or_zero_bits(right) {
        return None;
    }
    let product = left * right;
    if is_zero_bits(product) {
        // A zero product is exact only when an operand was zero; two nonzero
        // factors whose product underflows to zero are not an exact zero.
        return (is_zero_bits(left) || is_zero_bits(right)).then_some(0.0);
    }
    is_normal_bits(product).then_some(product)
}

fn checked_normal_sum(left: f64, right: f64) -> Option<f64> {
    if !is_normal_or_zero_bits(left) || !is_normal_or_zero_bits(right) {
        return None;
    }
    let sum = left + right;
    if is_zero_bits(sum) {
        let both_zero = is_zero_bits(left) && is_zero_bits(right);
        return (both_zero || are_exact_opposites(left, right)).then_some(0.0);
    }
    is_normal_bits(sum).then_some(sum)
}

pub(in crate::geometry) fn clearance_aabb_filter(
    point: XY,
    aabb: [f64; 4],
) -> (ExactDistanceWitness, SquaredDistanceFilter) {
    let [lower_x, lower_y, upper_x, upper_y] = aabb;
    if !is_finite_bits(point.x)
        || !is_finite_bits(point.y)
        || !is_finite_bits(lower_x)
        || !is_finite_bits(lower_y)
        || !is_finite_bits(upper_x)
        || !is_finite_bits(upper_y)
        || lower_x > upper_x
        || lower_y > upper_y
    {
        #[cfg(test)]
        CLEARANCE_AABB_FILTER_DECLINES.with(|count| count.set(count.get().saturating_add(1)));
        return (
            ExactDistanceWitness::Points(XY::new(0.0, 0.0), XY::new(0.0, 0.0)),
            SquaredDistanceFilter { lo: 0.0, hi: 0.0 },
        );
    }
    let closest = XY::new(
        if point.x < lower_x {
            lower_x
        } else if point.x > upper_x {
            upper_x
        } else {
            point.x
        },
        if point.y < lower_y {
            lower_y
        } else if point.y > upper_y {
            upper_y
        } else {
            point.y
        },
    );
    let key = point_pair_distance_key(point, closest);
    #[cfg(test)]
    if key.filter.lo.is_finite() && key.filter.hi.is_finite() {
        CLEARANCE_AABB_FILTERS.with(|count| count.set(count.get().saturating_add(1)));
    } else {
        CLEARANCE_AABB_FILTER_DECLINES.with(|count| count.set(count.get().saturating_add(1)));
    }
    (key.witness, key.filter)
}

impl SquaredDistanceKey {
    pub(in crate::geometry) fn cmp_distance(&self, other: &Self) -> Result<Ordering> {
        if let Some(order) = self.filter.disjoint_cmp(other.filter) {
            return Ok(order);
        }
        if self.witness.same_distance_as(other.witness) {
            return Ok(Ordering::Equal);
        }
        exact_selection_distance_cmp(self.witness, other.witness)
    }

    pub(in crate::geometry) fn is_exact_zero(&self) -> Result<bool> {
        // The source-distance filter's positive lower endpoint certifies
        // nonzero without materializing its exact dyadic witness.
        if self.filter.lo > 0.0 {
            return Ok(false);
        }
        Ok(self.witness.squared()?.numerator.is_zero())
    }

    pub(in crate::geometry) fn cmp(&self, other: &Self) -> Result<Ordering> {
        if self.filter.hi < other.filter.lo {
            return Ok(Ordering::Less);
        }
        if other.filter.hi < self.filter.lo {
            return Ok(Ordering::Greater);
        }
        exact_witness_cmp_with_identity(self.witness, other.witness)
    }

    pub(in crate::geometry) const fn upper_bound(&self) -> f64 {
        self.filter.hi
    }

    pub(in crate::geometry) fn distance_upper_bound(&self) -> Result<f64> {
        let value = if self.filter.hi.is_finite() {
            self.filter.hi.sqrt()
        } else {
            let exact = self.witness.squared()?;
            match round_sqrt_ratio(&exact.numerator, &exact.denominator) {
                Ok(value) => value,
                Err(error) if matches!(error.kind(), ErrorKind::Resource(_)) => return Err(error),
                Err(_) => f64::INFINITY,
            }
        };
        Ok((value * (1.0 + 16.0 * f64::EPSILON) + f64::from_bits(8)).next_up())
    }

    #[cfg(test)]
    fn contains_exact(self) -> bool {
        if !self.filter.lo.is_finite() || !self.filter.hi.is_finite() {
            return true;
        }
        let exact = self.witness.squared().unwrap();
        let denominator = ExactDyadic::from_f64(1.0);
        let lo = ExactSquaredDistance {
            numerator: ExactDyadic::from_f64(self.filter.lo),
            denominator: denominator.clone(),
        };
        let hi = ExactSquaredDistance {
            numerator: ExactDyadic::from_f64(self.filter.hi),
            denominator,
        };
        !lo.cmp(&exact).unwrap().is_gt() && !hi.cmp(&exact).unwrap().is_lt()
    }

    #[cfg(test)]
    const fn is_bounded(self) -> bool {
        self.filter.lo.is_finite() && self.filter.hi.is_finite()
    }
}

fn source_dot_difference_interval(normal: [f64; 2], left: XY, right: XY) -> Option<DdInterval> {
    let [nx, ny] = normal;
    let dx = DdInterval::difference(left.x, right.x)?;
    let dy = DdInterval::difference(left.y, right.y)?;
    DdInterval::exact(nx)
        .mul(dx)?
        .add(DdInterval::exact(ny).mul(dy)?)
}

fn source_product_sum_interval(
    x_left: FilterValue,
    x_right: FilterValue,
    y_left: FilterValue,
    y_right: FilterValue,
    subtract: bool,
    error_bound: FilterValue,
) -> Option<(f64, f64)> {
    let x_product = filter_product(x_left, x_right)?;
    let y_product = filter_product(y_left, y_right)?;
    if is_zero_bits(x_product) && is_zero_bits(y_product) {
        return Some((0.0, 0.0));
    }
    let y_term = if subtract { -y_product } else { y_product };
    let rounded = normal_sum(x_product, y_term)?;
    let magnitude = normal_sum(x_product.abs(), y_product.abs())?.next_up();
    // Segment terms can each carry two rounded differences before product
    // and sum; 16u covers that path. Dot terms have one exact coefficient and
    // one rounded difference, so their caller uses the tighter 8u bound.
    let error = normal_product(magnitude, error_bound.0)?.next_up();
    if !is_normal_bits(error) {
        return None;
    }
    let lower_term = -error;
    let lower = normal_sum(rounded, lower_term)?;
    let upper = normal_sum(rounded, error)?;
    Some((lower.next_down(), upper.next_up()))
}

fn source_dot_difference_binary64(normal: [f64; 2], left: XY, right: XY) -> Option<(f64, f64)> {
    let [nx, ny] = normal;
    let nx = FilterValue::new(nx)?;
    let ny = FilterValue::new(ny)?;
    let dx = filter_difference(left.x, right.x)?;
    let dy = filter_difference(left.y, right.y)?;
    source_product_sum_interval(nx, dx, ny, dy, false, SquaredDistanceFilter::DOT_ERROR)
}

fn source_dot_difference_exact(normal: [f64; 2], left: XY, right: XY) -> Result<ExactDyadic> {
    let [nx, ny] = normal;
    let dx = projection_difference(left.x, right.x)?;
    let dy = projection_difference(left.y, right.y)?;
    ExactDyadic::from_f64(nx)
        .try_product(&dx)?
        .try_add(ExactDyadic::from_f64(ny).try_product(&dy)?)
}

pub(crate) fn source_dot_cmp(normal: [f64; 2], left: XY, right: XY) -> Result<Ordering> {
    if let Some((lower, upper)) = source_dot_difference_binary64(normal, left, right) {
        let order = if lower > 0.0 {
            Some(Ordering::Greater)
        } else if upper < 0.0 {
            Some(Ordering::Less)
        } else if is_zero_bits(lower) && is_zero_bits(upper) {
            Some(Ordering::Equal)
        } else {
            None
        };
        if let Some(order) = order {
            #[cfg(test)]
            CLEARANCE_DOT_BINARY64_DECISIONS.with(|count| count.set(count.get().saturating_add(1)));
            return Ok(order);
        }
    }
    #[cfg(test)]
    CLEARANCE_DOT_BINARY64_DECLINES.with(|count| count.set(count.get().saturating_add(1)));
    if let Some(difference) = source_dot_difference_interval(normal, left, right) {
        if difference.is_exact_zero() {
            return Ok(Ordering::Equal);
        }
        if let Some(order) = dd_strict_cmp(difference, DdInterval::exact(0.0)) {
            return Ok(order);
        }
    }
    #[cfg(test)]
    CLEARANCE_STRIP_SUPPORT_FALLBACKS.with(|count| count.set(count.get().saturating_add(1)));
    Ok(source_dot_difference_exact(normal, left, right)?.cmp(&ExactDyadic::zero()))
}

/// `Some(true)` certifies pruning, `Some(false)` certifies retaining the node,
/// and `None` requires the exact source-ratio comparison.
fn clearance_strip_filter_result(
    point: XY,
    normal: [f64; 2],
    low: XY,
    high: XY,
    incumbent: &SquaredDistanceKey,
) -> Option<bool> {
    if let Some(decision) = clearance_strip_binary64_result(point, normal, low, high, incumbent) {
        #[cfg(test)]
        CLEARANCE_STRIP_BINARY64_DECISIONS.with(|count| count.set(count.get().saturating_add(1)));
        return Some(decision);
    }
    #[cfg(test)]
    CLEARANCE_STRIP_BINARY64_DECLINES.with(|count| count.set(count.get().saturating_add(1)));
    clearance_strip_dd_filter_result(point, normal, low, high, incumbent)
}

fn clearance_strip_binary64_result(
    point: XY,
    normal: [f64; 2],
    low: XY,
    high: XY,
    incumbent: &SquaredDistanceKey,
) -> Option<bool> {
    let low_gap = source_dot_difference_binary64(normal, low, point)?;
    let high_gap = source_dot_difference_binary64(normal, point, high)?;
    if low_gap.1 <= 0.0 && high_gap.1 <= 0.0 {
        return Some(false);
    }
    let gap_lower = low_gap.0.max(high_gap.0).max(0.0);
    let gap_upper = low_gap.1.max(high_gap.1).max(0.0);
    if !(is_zero_bits(gap_lower) || gap_lower.is_normal())
        || !(is_zero_bits(gap_upper) || gap_upper.is_normal())
    {
        return None;
    }

    let [nx, ny] = normal;
    // Both dot certificates above admitted the normal components in the same
    // safe exponent band, so these squares cannot overflow or underflow.
    let nx_squared = nx * nx;
    let ny_squared = ny * ny;
    let norm_squared = checked_normal_sum(nx_squared, ny_squared)?;
    let norm = SquaredDistanceFilter::nonnegative(
        norm_squared,
        SquaredDistanceFilter::STRIP_NORM_LOWER_SCALE,
        SquaredDistanceFilter::STRIP_NORM_UPPER_SCALE,
    )?;
    let key_lower = incumbent.filter.lo;
    let key_upper = incumbent.filter.hi;
    if !key_lower.is_normal()
        || !key_upper.is_normal()
        || key_lower < SquaredDistanceFilter::MIN_INCUMBENT
        || key_upper > SquaredDistanceFilter::MAX_INCUMBENT
        || norm.lo <= 0.0
    {
        return None;
    }

    if gap_lower > 0.0 {
        // Strict separation preserves exact ties for the source-order path.
        let lower_bound = checked_normal_product(gap_lower, gap_lower)?
            .next_down()
            .max(0.0);
        let incumbent_upper = checked_normal_product(key_upper, norm.hi)?.next_up();
        if lower_bound > incumbent_upper {
            return Some(true);
        }
    }

    let gap_upper_squared = if is_zero_bits(gap_upper) {
        0.0
    } else {
        checked_normal_product(gap_upper, gap_upper)?.next_up()
    };
    let incumbent_lower = checked_normal_product(key_lower, norm.lo)?
        .next_down()
        .max(0.0);
    (gap_upper_squared <= incumbent_lower).then_some(false)
}

fn clearance_strip_dd_filter_result(
    point: XY,
    normal: [f64; 2],
    low: XY,
    high: XY,
    incumbent: &SquaredDistanceKey,
) -> Option<bool> {
    let low_gap = source_dot_difference_interval(normal, low, point)?;
    let high_gap = source_dot_difference_interval(normal, point, high)?;
    let (low_lower, low_upper) = low_gap.outward_bounds();
    let (high_lower, high_upper) = high_gap.outward_bounds();
    if low_upper.is_finite() && high_upper.is_finite() && low_upper <= 0.0 && high_upper <= 0.0 {
        return Some(false);
    }
    let gap_lower = low_lower.max(high_lower).max(0.0);
    let gap_upper = low_upper.max(high_upper).max(0.0);
    if !gap_lower.is_finite() || !gap_upper.is_finite() {
        return None;
    }

    let [nx, ny] = normal;
    let norm_squared = DdInterval::exact(nx)
        .square()
        .and_then(|x| DdInterval::exact(ny).square().and_then(|y| x.add(y)))?;
    let (norm_lower, norm_upper) = norm_squared.outward_bounds();
    if !norm_lower.is_finite() || !norm_upper.is_finite() || norm_upper <= 0.0 {
        return None;
    }
    let key_lower = incumbent.filter.lo;
    let key_upper = incumbent.filter.hi;
    // A lower strip bound above the key's upper enclosure proves pruning.
    if gap_lower > 0.0
        && key_upper.is_finite()
        && key_upper >= 0.0
        && let (Some(bound_numerator), Some(incumbent_upper_product)) = (
            DdInterval::exact(gap_lower).square(),
            DdInterval::exact(key_upper).mul(DdInterval::exact(norm_upper)),
        )
        && dd_strict_cmp(bound_numerator, incumbent_upper_product).is_some_and(Ordering::is_gt)
    {
        return Some(true);
    }
    // An upper strip bound below the key's lower enclosure proves no-prune.
    if key_lower.is_finite()
        && key_lower >= 0.0
        && norm_lower > 0.0
        && DdInterval::exact(gap_upper)
            .square()
            .zip(DdInterval::exact(key_lower).mul(DdInterval::exact(norm_lower)))
            .is_some_and(|(bound_numerator, incumbent_lower_product)| {
                let (_, bound_upper) = bound_numerator.outward_bounds();
                let (incumbent_lower, _) = incumbent_lower_product.outward_bounds();
                bound_upper.is_finite()
                    && incumbent_lower.is_finite()
                    && bound_upper <= incumbent_lower
            })
    {
        return Some(false);
    }
    None
}

pub(crate) fn clearance_strip_exceeds(
    point: XY,
    normal: [f64; 2],
    low: XY,
    high: XY,
    incumbent: &SquaredDistanceKey,
) -> Result<bool> {
    if let Some(prunes) = clearance_strip_filter_result(point, normal, low, high, incumbent) {
        return Ok(prunes);
    }
    #[cfg(test)]
    CLEARANCE_STRIP_EXACT_FALLBACKS.with(|count| count.set(count.get().saturating_add(1)));

    let low_gap = source_dot_difference_exact(normal, low, point)?;
    let high_gap = source_dot_difference_exact(normal, point, high)?;
    let gap = match low_gap.cmp(&high_gap) {
        Ordering::Greater => low_gap,
        Ordering::Less | Ordering::Equal => high_gap,
    };
    let zero = ExactDyadic::zero();
    if !gap.cmp(&zero).is_gt() {
        return Ok(false);
    }
    let [nx, ny] = normal;
    let x = ExactDyadic::from_f64(nx);
    let y = ExactDyadic::from_f64(ny);
    let norm_squared = x.try_product(&x)?.try_add(y.try_product(&y)?)?;
    if norm_squared.cmp(&zero).is_eq() {
        return Ok(false);
    }
    let bound_numerator = gap.try_product(&gap)?;
    let incumbent = incumbent.witness.squared()?;
    Ok(compare_ratios(
        &bound_numerator,
        &norm_squared,
        &incumbent.numerator,
        &incumbent.denominator,
    )?
    .is_gt())
}

pub(in crate::geometry) fn point_pair_distance_key(left: XY, right: XY) -> SquaredDistanceKey {
    let witness = ExactDistanceWitness::Points(left, right);
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let squared = dx * dx + dy * dy;
    if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
        SquaredDistanceFilter::point_pair(squared).bind(witness)
    } else {
        SquaredDistanceFilter::unbounded().bind(witness)
    }
}

fn point_segment_binary64_distance_key(point: XY, segment: Segment) -> Option<SquaredDistanceKey> {
    let dx = filter_difference(segment.end.x, segment.start.x)?;
    let dy = filter_difference(segment.end.y, segment.start.y)?;
    let qx = filter_difference(point.x, segment.start.x)?;
    let qy = filter_difference(point.y, segment.start.y)?;
    let denominator_squared = normal_sum(filter_product(dx, dx)?, filter_product(dy, dy)?)?;
    let denominator = SquaredDistanceFilter::nonnegative(
        denominator_squared,
        SquaredDistanceFilter::POINT_PAIR_LOWER_SCALE,
        SquaredDistanceFilter::POINT_PAIR_UPPER_SCALE,
    )?;
    if !is_normal_bits(denominator.lo) {
        return None;
    }

    let numerator = source_product_sum_interval(
        qx,
        dx,
        qy,
        dy,
        false,
        SquaredDistanceFilter::SEGMENT_DOT_ERROR,
    )?;
    if numerator.1 < 0.0 {
        return Some(point_pair_distance_key(point, segment.start));
    }
    if numerator.0 > denominator.hi {
        return Some(point_pair_distance_key(point, segment.end));
    }
    if numerator.0 <= 0.0 || numerator.1 >= denominator.lo {
        return None;
    }

    let determinant = source_product_sum_interval(
        dx,
        qy,
        dy,
        qx,
        true,
        SquaredDistanceFilter::SEGMENT_DOT_ERROR,
    )?;
    let minimum = if determinant.0 <= 0.0 && determinant.1 >= 0.0 {
        0.0
    } else {
        determinant.0.abs().min(determinant.1.abs())
    };
    let maximum = determinant.0.abs().max(determinant.1.abs());
    let lower_square = if is_zero_bits(minimum) {
        0.0
    } else {
        checked_normal_product(minimum, minimum)?
            .next_down()
            .max(0.0)
    };
    let upper_square = if is_zero_bits(maximum) {
        0.0
    } else {
        checked_normal_product(maximum, maximum)?.next_up()
    };
    let lower_quotient = lower_square / denominator.hi;
    let upper_quotient = upper_square / denominator.lo;
    if (lower_square > 0.0 && !is_normal_bits(lower_quotient))
        || (upper_square > 0.0 && !is_normal_bits(upper_quotient))
    {
        return None;
    }
    let lo = lower_quotient.next_down().max(0.0);
    let hi = upper_quotient.next_up();
    hi.is_finite().then_some(
        SquaredDistanceFilter { lo, hi }.bind(ExactDistanceWitness::Projected(point, segment)),
    )
}

#[expect(
    clippy::float_cmp,
    reason = "axis-aligned carriers are exact identities over stored coordinates"
)]
pub(in crate::geometry) fn point_segment_exact_distance_key(
    point: XY,
    segment: Segment,
) -> SquaredDistanceKey {
    if segment.start.y == segment.end.y
        && point.x >= segment.start.x.min(segment.end.x)
        && point.x <= segment.start.x.max(segment.end.x)
    {
        return point_pair_distance_key(point, XY::new(point.x, segment.start.y));
    }
    if segment.start.x == segment.end.x
        && point.y >= segment.start.y.min(segment.end.y)
        && point.y <= segment.start.y.max(segment.end.y)
    {
        return point_pair_distance_key(point, XY::new(segment.start.x, point.y));
    }
    if let Some(key) = point_segment_binary64_distance_key(point, segment) {
        #[cfg(test)]
        POINT_SEGMENT_BINARY64_DECISIONS.with(|count| count.set(count.get().saturating_add(1)));
        return key;
    }
    #[cfg(test)]
    POINT_SEGMENT_BINARY64_DECLINES.with(|count| count.set(count.get().saturating_add(1)));
    let filtered = (|| {
        let dx = source_difference(segment.end.x, segment.start.x)?;
        let dy = source_difference(segment.end.y, segment.start.y)?;
        let qx = source_difference(point.x, segment.start.x)?;
        let qy = source_difference(point.y, segment.start.y)?;
        let denominator = SourcePair::new(source_product(dx, dx)?, source_product(dy, dy)?, false)?;
        let numerator = SourcePair::new(source_product(qx, dx)?, source_product(qy, dy)?, false)?;
        if numerator.upper() < 0.0 {
            return Some(point_pair_distance_key(point, segment.start));
        }
        if numerator.lower() > denominator.upper() {
            return Some(point_pair_distance_key(point, segment.end));
        }
        if numerator.lower() <= 0.0 || numerator.upper() >= denominator.lower() {
            return None;
        }
        let determinant = SourcePair::new(source_product(dx, qy)?, source_product(dy, qx)?, true)?;
        let (low, high) = (determinant.lower(), determinant.upper());
        let minimum = if low <= 0.0 && high >= 0.0 {
            0.0
        } else {
            low.abs().min(high.abs())
        };
        let maximum = low.abs().max(high.abs());
        let lower_square = (minimum * minimum).next_down().max(0.0);
        let upper_square = (maximum * maximum).next_up();
        let lower = (lower_square / denominator.upper()).next_down().max(0.0);
        let upper = (upper_square / denominator.lower()).next_up();
        upper.is_finite().then(|| {
            SquaredDistanceFilter {
                lo: lower,
                hi: upper,
            }
            .bind(ExactDistanceWitness::Projected(point, segment))
        })
    })();
    if let Some(key) = filtered {
        return key;
    }
    SquaredDistanceFilter::unbounded().bind(ExactDistanceWitness::Clamped(point, segment))
}

#[cfg(test)]
pub(in crate::geometry) fn point_segment_distance_key(
    point: XY,
    segment: Segment,
) -> Result<SquaredDistanceKey> {
    Ok(point_segment_selection(point, segment)?.distance_key(point, segment))
}

pub(crate) fn point_segment_selection(
    point: XY,
    segment: Segment,
) -> Result<PointSegmentSelection> {
    Ok(match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            filter,
            witness_kind,
            projection,
            ..
        } => PointSegmentSelection {
            distance: measurement_squared.sqrt(),
            distance_squared: measurement_squared,
            filter,
            witness_kind,
            projection,
        },
        PointSegmentMetric::Projection(projection) => {
            let distance = projection.continuum_distance(point, segment)?;
            let squared = match &projection {
                SegmentProjection::Start => {
                    ExactSquaredDistance::point_pair(point, segment.start)?.rounded()?
                },
                SegmentProjection::End => {
                    ExactSquaredDistance::point_pair(point, segment.end)?.rounded()?
                },
                SegmentProjection::Interior(_)
                | SegmentProjection::CertifiedInterior(_)
                | SegmentProjection::ExactInterior(_) => source_projection_squared(point, segment)?,
            };
            let witness_kind = if projection.is_start() {
                PointSegmentWitnessKind::Start
            } else if projection.is_end() {
                PointSegmentWitnessKind::End
            } else {
                PointSegmentWitnessKind::Interior
            };
            PointSegmentSelection {
                distance,
                distance_squared: if squared.is_finite() {
                    squared
                } else {
                    f64::INFINITY
                },
                filter: if squared_norm_is_trustworthy(squared, false) {
                    SquaredDistanceFilter::source_squared(squared, 16.0 * (f64::EPSILON / 2.0))
                } else {
                    SquaredDistanceFilter::unbounded()
                },
                witness_kind,
                projection: SelectionProjectionSource::Ready(projection),
            }
        },
    })
}

#[cfg(test)]
std::thread_local! {
    pub(crate) static EXACT_SELECTION_FALLBACKS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_AABB_FILTERS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_AABB_FILTER_DECLINES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_AABB_COMPARE_DECISIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_AABB_COMPARE_DECLINES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_DOT_BINARY64_DECISIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_DOT_BINARY64_DECLINES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static POINT_SEGMENT_BINARY64_DECISIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static POINT_SEGMENT_BINARY64_DECLINES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_STRIP_BINARY64_DECISIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_STRIP_BINARY64_DECLINES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_STRIP_EXACT_FALLBACKS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(crate) static CLEARANCE_STRIP_SUPPORT_FALLBACKS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl ExactDistanceWitness {
    fn witness_role(self) -> Result<WitnessRole> {
        Ok(match self {
            Self::Projected(..) => WitnessRole::Interior,
            Self::Points(..) => WitnessRole::Point,
            Self::Clamped(point, segment) => {
                match classify_segment_projection_exact(point, segment)? {
                    ExactClassifiedSegmentProjection::Interior(_) => WitnessRole::Interior,
                    ExactClassifiedSegmentProjection::Start
                    | ExactClassifiedSegmentProjection::End => WitnessRole::Point,
                }
            },
        })
    }

    #[expect(
        clippy::float_cmp,
        reason = "equal axis-aligned offsets certify the same exact source distance"
    )]
    fn same_distance_as(self, other: Self) -> bool {
        let equal_sum = |a, b, c, d| {
            let (left, left_tail) = two_sum(a, b);
            let (right, right_tail) = two_sum(c, d);
            left.is_finite() && right.is_finite() && left == right && left_tail == right_tail
        };
        match (self, other) {
            (Self::Points(left_a, left_b), Self::Points(right_a, right_b)) => {
                (same_point(left_a, right_a) && same_point(left_b, right_b))
                    || (same_point(left_a, right_b) && same_point(left_b, right_a))
                    || (left_a.x == left_b.x
                        && right_a.x == right_b.x
                        && ((left_a.y == right_a.y && left_b.y == right_b.y)
                            || (left_a.y == right_b.y && left_b.y == right_a.y)))
                    || (left_a.y == left_b.y
                        && right_a.y == right_b.y
                        && ((left_a.x == right_a.x && left_b.x == right_b.x)
                            || (left_a.x == right_b.x && left_b.x == right_a.x)))
            },
            (Self::Projected(left, a), Self::Projected(right, b)) => {
                ((same_point(a.start, b.start) && same_point(a.end, b.end))
                    || (same_point(a.start, b.end) && same_point(a.end, b.start)))
                    && equal_sum(left.x, right.x, a.start.x, a.end.x)
                    && equal_sum(left.y, right.y, a.start.y, a.end.y)
            },
            _ => false,
        }
    }

    fn squared(self) -> Result<ExactSquaredDistance> {
        match self {
            Self::Points(left, right) => ExactSquaredDistance::point_pair(left, right),
            Self::Projected(point, segment) => exact_source_squared(point, segment),
            Self::Clamped(point, segment) => {
                match classify_segment_projection_exact(point, segment)? {
                    ExactClassifiedSegmentProjection::Start => {
                        ExactSquaredDistance::point_pair(point, segment.start)
                    },
                    ExactClassifiedSegmentProjection::End => {
                        ExactSquaredDistance::point_pair(point, segment.end)
                    },
                    ExactClassifiedSegmentProjection::Interior(terms) => {
                        ExactSquaredDistance::interior(&terms)
                    },
                }
            },
        }
    }
}

pub(in crate::geometry) fn exact_distance_witness_cmp(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Result<Ordering> {
    left.squared()?.cmp(&right.squared()?)
}

fn exact_selection_distance_cmp(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Result<Ordering> {
    #[cfg(test)]
    EXACT_SELECTION_FALLBACKS.with(|count| count.set(count.get().saturating_add(1)));
    exact_distance_witness_cmp(left, right)
}

pub(super) fn exact_witness_cmp_with_identity(
    left: ExactDistanceWitness,
    right: ExactDistanceWitness,
) -> Result<Ordering> {
    if left.same_distance_as(right) {
        return Ok(left
            .witness_role()?
            .cmp_tie(right.witness_role()?, || Ordering::Equal));
    }
    let distance = exact_selection_distance_cmp(left, right)?;
    if distance.is_eq() {
        Ok(left
            .witness_role()?
            .cmp_tie(right.witness_role()?, || Ordering::Equal))
    } else {
        Ok(distance)
    }
}

#[cfg(test)]
mod witness_role_tests {
    use super::*;

    #[test]
    fn chart_witness_role_clamped_recipe_classifies_only_equal_source_distances() {
        let point = XY::new(0.0, 0.0);
        let endpoint = ExactDistanceWitness::Clamped(point, Segment {
            start: XY::new(0.0, 1.0),
            end: XY::new(1.0, 2.0),
        });
        let interior = ExactDistanceWitness::Clamped(point, Segment {
            start: XY::new(-1.0, 1.0),
            end: XY::new(1.0, 1.0),
        });
        let point_pair = ExactDistanceWitness::Points(point, XY::new(0.0, 1.0));
        assert_eq!(
            exact_witness_cmp_with_identity(endpoint, point_pair).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            exact_witness_cmp_with_identity(interior, point_pair).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            exact_distance_witness_cmp(interior, point_pair).unwrap(),
            Ordering::Equal
        );
        let closer = ExactDistanceWitness::Points(point, XY::new(0.0, 0.5));
        assert_eq!(
            exact_witness_cmp_with_identity(interior, closer).unwrap(),
            Ordering::Greater
        );
    }
}

pub(crate) fn point_segment_distance(point: impl Into<XY>, segment: Segment) -> Result<f64> {
    let point = point.into();
    Ok(match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            ..
        } => measurement_squared.sqrt(),
        PointSegmentMetric::Projection(projection) => {
            projection.continuum_distance(point, segment)?
        },
    })
}

/// A squared Euclidean residual is trustworthy only when it is a **normal**
/// positive number, or exactly zero with all-exact-zero deltas (true contact).
///
/// Positive subnormals are **not** faithful norms: `sqrt` of a subnormal square
/// quantizes the distance (e.g. separation `2.2e-162` squares to a subnormal
/// whose `sqrt` is ~`2.22e-162`, a +1% error that also flips `dwithin` and
/// breaks monotonicity). Full underflow to zero with nonzero deltas is the
/// sibling false-zero case. One rule for scalar, SIMD, and packed lanes.
pub(crate) fn squared_norm_is_trustworthy(squared: f64, all_deltas_zero: bool) -> bool {
    squared.is_normal() || (squared == 0.0 && all_deltas_zero)
}

/// Finalize a planar **distance** answer accumulated in squared space.
///
/// - `best_sq` is a normal positive → `sqrt` (ordinary mid-range, bit-stable)
/// - `best_sq` is zero or a positive subnormal → recompute in distance space
///   (true contact still returns 0; subnormal/false-zero cannot masquerade)
/// - non-finite → propagate (`+inf` = no candidate / overflow class)
///
/// Every planar distance answer that min-reduces squared candidates must
/// finish through this helper — never bare `squared.sqrt()`.
pub(crate) fn finish_planar_squared_min(
    best_sq: f64,
    recompute_distance: impl FnOnce() -> Result<f64>,
) -> Result<f64> {
    if best_sq.is_normal() {
        Ok(best_sq.sqrt())
    } else if best_sq >= 0.0 && best_sq.is_finite() {
        // Zero or positive subnormal: recompute with an honest hypot form.
        recompute_distance()
    } else {
        Ok(best_sq)
    }
}

/// Squared distance from `point` to `segment` — self-contained MEASUREMENT.
/// Ordering / compare-min callers may use this; a returned `0.0` is **not**
/// a contact certificate under underflow (use [`point_segment_distance`] or
/// [`finish_planar_squared_min`] for magnitude answers).
pub(in crate::geometry) fn point_segment_distance_squared(
    point: impl Into<XY>,
    segment: Segment,
) -> Result<f64> {
    let point = point.into();
    Ok(match point_segment_metric(point, segment)? {
        PointSegmentMetric::Squared {
            measurement_squared,
            ..
        } => measurement_squared,
        PointSegmentMetric::Projection(projection) => {
            projection.continuum_squared(point, segment)?
        },
    })
}

/// Guarded sqrt-form planar distance — ONE point-distance for the whole
/// project: one inline `sqrt` instead of a libm `hypot` call (~3× cheaper, the
/// dominant cost of prefix-length builds). The rescue covers both failure
/// classes — overflow to non-finite, and full underflow to `0.0` with a nonzero
/// delta — by falling back to `hypot`, so every input class gets a result
/// matching the overflow-safe form (lane rounding may differ by an ulp; lengths
/// are measurement arithmetic per the float policy). This is the scalar sibling
/// of the SIMD `line_length` kernel's sqrt-with-rescue lanes.
pub(crate) fn point_distance(left: impl Into<XY>, right: impl Into<XY>) -> f64 {
    let (left, right) = (left.into(), right.into());
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    // Plain mul+add, NOT `mul_add`: scalar `mul_add` on the portable
    // baseline (no FMA target feature) is a libm `fma` CALL — the very cost
    // class this function exists to remove. Fused rounding buys nothing a
    // measurement needs.
    let squared = dx * dx + dy * dy;
    if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0) {
        squared.sqrt()
    } else {
        dx.hypot(dy)
    }
}

/// Honest squared Euclidean distance when squared space is faithful.
/// Returns `None` on overflow, positive-subnormal squares, or false-zero
/// underflow (distinct points whose `dx²+dy²` collapses to 0 or a subnormal)
/// — callers that need a correct magnitude or zero-test must use
/// [`point_distance`] / [`points_dwithin`].
pub(crate) fn try_point_distance_squared(left: impl Into<XY>, right: impl Into<XY>) -> Option<f64> {
    let (left, right) = (left.into(), right.into());
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let squared = dx * dx + dy * dy;
    squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0).then_some(squared)
}

pub(crate) fn point_distance_squared(left: impl Into<XY>, right: impl Into<XY>) -> f64 {
    let (left, right) = (left.into(), right.into());
    try_point_distance_squared(left, right).unwrap_or_else(|| {
        // Overflow: hypot then square when representable; underflow of
        // d*d for tiny d still yields 0 — ordering-only callers should
        // prefer nearest_pair_key-style compare helpers; magnitude
        // callers must use [`point_distance`] / [`points_dwithin`].
        let d = point_distance(left, right);
        let d2 = d * d;
        if d2.is_finite() { d2 } else { f64::INFINITY }
    })
}

/// Planar dwithin in distance space when squared space collapses.
/// Ordinary mid-range pairs keep the squared compare (bit-stable at the
/// boundary for honest squares); tiny separations and tiny limits use
/// [`point_distance`] so `dwithin(a,b,0)` is false for distinct points and
/// `dwithin(a,b,5e-201)` is false when distance is 1e-200.
pub(crate) fn points_dwithin(left: impl Into<XY>, right: impl Into<XY>, limit: f64) -> bool {
    let (left, right) = (left.into(), right.into());
    if !limit.is_finite() {
        // +inf admits everything finite; NaN / -inf never.
        return limit == f64::INFINITY;
    }
    if limit < 0.0 {
        return false;
    }
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    if dx == 0.0 && dy == 0.0 {
        return true;
    }
    if limit == 0.0 {
        // Distinct points are never within a hard zero radius.
        return false;
    }
    let d2 = dx * dx + dy * dy;
    let lim2 = limit * limit;
    // Both sides of the compare must be trustworthy normals — a false-zero
    // or positive-subnormal d²/limit² would quantize the boundary (e.g.
    // d=2.2e-162, limit=1.6e-162 wrongly returns true via subnormal squares).
    if d2.is_normal() && lim2.is_normal() {
        d2 <= lim2
    } else {
        point_distance(left, right) <= limit
    }
}

#[cfg(test)]
#[path = "distance/tests.rs"]
mod tests;
