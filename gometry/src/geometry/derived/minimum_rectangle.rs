//! Minimum-area enclosing rectangles over exact support lines.
//!
//! The calipers retain the *world-coordinate* support vertices.  Floating
//! projections are only a filter: every ambiguous support comparison, score
//! comparison, and emitted support inequality is evaluated as an exact dyadic
//! sum.  Consequently the final four-corner construction needs no hull scan.

use std::cmp::Ordering;

use crate::error::Result;
use crate::geographic::{
    ComputedEdgeIntent, ComputedSampleOrdinates, ComputedVertexIntent, GeographicFrame,
};
use crate::geometry::exact_arith::product_sum::{ExactSum, LIMBS, PRODUCT_MIN_EXP, cmp_magnitude};
use crate::geometry::exact_arith::{
    checked_dd_mul as double_double_mul, checked_two_product as two_product,
    dd_add as double_double_add, dd_sub as double_double_sub, two_sum,
};
use crate::geometry::{
    CoordSeq, ExactRational, ExpansionBudget, GeometryErrorKind, MOrdinate, Orientation, Point,
    ZOrdinate, axis_pow2_scale, orientation, point_distance,
};
const PROJECTION_ERRBOUND: f64 = 8.0 * f64::EPSILON;
const DOUBLE_LIMBS: usize = LIMBS * 2;
const TRIPLE_LIMBS: usize = LIMBS * 3 + 1;

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

/// Adjacent binary64 values bracketing an exact positive fixed-limb value.
#[expect(
    clippy::indexing_slicing,
    reason = "high_bit is located by this fixed LIMBS range before its limb is read"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the fixed limb count bounds every bit, exponent, and mantissa operation"
)]
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

#[expect(
    clippy::indexing_slicing,
    reason = "shift is bounded by the fixed exact-limb exponent range and the adjacent word is checked"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "word and bits come from shift division/remainder by 64 and the fixed limb range"
)]
const fn shifted_low_u64(words: &[u64; LIMBS], shift: usize) -> u64 {
    let word = shift / 64;
    let bits = shift % 64;
    let mut value = words[word] >> bits;
    if bits != 0 && word + 1 < LIMBS {
        value |= words[word + 1] << (64 - bits);
    }
    value
}

#[expect(
    clippy::indexing_slicing,
    reason = "bit is bounded by the fixed exact-limb exponent range"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "bits is the remainder of division by 64 and remains below the word width"
)]
fn any_bits_below(words: &[u64; LIMBS], bit: usize) -> bool {
    let word = bit / 64;
    let bits = bit % 64;
    words[..word].iter().any(|&value| value != 0)
        || (bits != 0 && words[word] & ((1_u64 << bits) - 1) != 0)
}

fn exact_dot_delta(direction: (f64, f64), current: Point, next: Point) -> ExactSum {
    let mut sum = ExactSum::zero();
    sum.add_product(direction.0, next.x);
    sum.subtract_product(direction.0, current.x);
    sum.add_product(direction.1, next.y);
    sum.subtract_product(direction.1, current.y);
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
                Point::new_raw_xy(start.x * scale, start.y * scale),
                Point::new_raw_xy(end.x * scale, end.y * scale),
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

    fn from_chart_edge(
        start: crate::geographic::ChartPointSite,
        end: crate::geographic::ChartPointSite,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Option<Self>> {
        let [start_longitude, end_longitude] = [start.longitude(), end.longitude()];
        let Some(periods) = end_longitude
            .periods()
            .checked_sub(start_longitude.periods())
        else {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        };
        let displacement = crate::geographic::RetainedChartDisplacement::from_native_endpoints(
            [*start_longitude.native(), *end_longitude.native()],
            periods,
        );
        let axis = frame.longitude();
        let dx = displacement.delta_native(axis);
        let start_y = start.source_y();
        let end_y = end.source_y();
        let dy = end_y - start_y;
        let length = point_distance(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(dx, dy));
        if length.is_finite() && length != 0.0 {
            return Ok(Self::from_vector(dx, dy));
        }
        let start_x = axis.lifted_site_longitude(&start_longitude)?;
        let end_x = axis.lifted_site_longitude(&end_longitude)?;
        let magnitude = start_x
            .abs()
            .max(end_x.abs())
            .max(start_y.abs())
            .max(end_y.abs());
        if !magnitude.is_finite() {
            return Ok(None);
        }
        let scale = axis_pow2_scale(magnitude);
        let dx = dx * scale;
        let dy = end_y * scale - start_y * scale;
        Ok(Self::from_vector(dx, dy))
    }

    fn from_vector(mut dx: f64, mut dy: f64) -> Option<Self> {
        let mut length = point_distance(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(dx, dy));
        if !length.is_finite() || length == 0.0 {
            let magnitude = dx.abs().max(dy.abs());
            if magnitude == 0.0 || !magnitude.is_finite() {
                return None;
            }
            let scale = axis_pow2_scale(magnitude);
            dx *= scale;
            dy *= scale;
            length = point_distance(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(dx, dy));
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
        norm.add_product(self.along.0, self.along.0);
        norm.add_product(self.along.1, self.along.1);
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
        let mut difference = axis.select(&self.numerator).clone();
        difference.subtract_product(value, *axis.select(&self.denominator));
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
        return CoordSeq::try_from_points(&bounds);
    }
    let best = select_minimum_candidate(hull);
    let corners = best.emit(hull)?;
    CoordSeq::try_from_points(&corners)
}

/// The same caliper and exact score owner over retained point sites. Keep each
/// constructed corner exact until the shared publisher writes its native X.
pub(crate) fn minimum_area_rectangle_chart(
    hull: &[crate::geographic::ChartPointSite],
    frame: GeographicFrame,
) -> Result<CoordSeq> {
    if hull.len() < 3 {
        return Err(minimum_rectangle_not_representable());
    }
    let candidate = select_minimum_candidate_chart(hull, frame)?;
    let corners = emit_chart_candidate(hull, candidate, frame)?;
    publish_and_certify_chart_rectangle(hull, &corners, frame)
}

#[expect(
    clippy::indexing_slicing,
    reason = "caliper edge and support ordinals are bounded by the admitted convex hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the hull has at least three vertices and edge/support ordinals remain in its bounded cycle"
)]
fn select_minimum_candidate(hull: &[Point]) -> Candidate {
    let count = hull.len();
    let first_basis = (0..count)
        .find_map(|edge| Basis::from_edge(hull[edge], hull[(edge + 1) % count]))
        .expect("strict convex hull has a finite nonzero edge basis");
    // One exact global support establishes the first caliper.  In CCW support
    // order the remaining directions follow `-v, u, v, -u`; advancing those
    // unwrapped pointers from their predecessor crosses each first-edge
    // normal cone once, rather than paying four independent full scans.
    let outward_direction = (-first_basis.outward.0, -first_basis.outward.1);
    let initial_outward_min = exact_argmax(hull, outward_direction);
    let mut outward_min = advance_max(hull, outward_direction, initial_outward_min);
    let mut along_max = advance_max(hull, first_basis.along, outward_min);
    let mut outward_max = advance_max(hull, first_basis.outward, along_max);
    let mut along_min = advance_max(
        hull,
        (-first_basis.along.0, -first_basis.along.1),
        outward_max,
    );
    // The advanced pointer is the strict certificate and progression state.
    // Keep its exact-argmax representative for the first candidate: equal
    // support points are geometrically interchangeable, but their dot-product
    // rounding is part of the established byte-level output.
    let mut candidate_outward_min = initial_outward_min;
    let mut best: Option<RankedCandidate> = None;
    for edge in 0..count {
        let basis = Basis::from_edge(hull[edge], hull[(edge + 1) % count])
            .expect("strict convex hull edge has a basis");
        if edge != 0 {
            along_max = advance_max(hull, basis.along, along_max);
            outward_min = advance_max(hull, (-basis.outward.0, -basis.outward.1), outward_min);
            candidate_outward_min = outward_min;
            outward_max = advance_max(hull, basis.outward, outward_max);
            along_min = advance_max(hull, (-basis.along.0, -basis.along.1), along_min);
        }
        debug_assert!(support_stops_here(hull, basis.along, along_max));
        debug_assert!(support_stops_here(
            hull,
            (-basis.outward.0, -basis.outward.1),
            outward_min
        ));
        debug_assert!(support_stops_here(hull, basis.outward, outward_max));
        debug_assert!(support_stops_here(
            hull,
            (-basis.along.0, -basis.along.1),
            along_min
        ));
        let mut candidate = RankedCandidate::new(hull, Candidate {
            basis,
            supports: Supports {
                along_min: along_min % count,
                along_max: along_max % count,
                outward_min: candidate_outward_min % count,
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

fn exact_chart_dot_delta(
    direction: (f64, f64),
    current: crate::geographic::ChartPointSite,
    next: crate::geographic::ChartPointSite,
    frame: crate::geographic::GeographicFrame,
) -> ExactSum {
    let axis = frame.longitude();
    let [current_x, next_x] = [current.longitude(), next.longitude()];
    let mut sum = ExactSum::zero();
    sum.add_product(direction.0, next_x.native().get());
    sum.add_scaled_product(direction.0, axis.period(), next_x.periods(), false);
    sum.subtract_product(direction.0, current_x.native().get());
    sum.add_scaled_product(direction.0, axis.period(), current_x.periods(), true);
    sum.add_product(direction.1, next.source_y());
    sum.subtract_product(direction.1, current.source_y());
    sum
}

fn chart_projection_delta_order(
    direction: (f64, f64),
    current: crate::geographic::ChartPointSite,
    next: crate::geographic::ChartPointSite,
    frame: crate::geographic::GeographicFrame,
) -> Ordering {
    exact_chart_dot_delta(direction, current, next, frame).ordering()
}

fn exact_chart_width(
    direction: (f64, f64),
    minimum: crate::geographic::ChartPointSite,
    maximum: crate::geographic::ChartPointSite,
    frame: crate::geographic::GeographicFrame,
) -> [u64; LIMBS] {
    let exact = exact_chart_dot_delta(direction, minimum, maximum, frame);
    let (sign, magnitude) = exact.signed_magnitude();
    debug_assert_eq!(sign, Ordering::Greater);
    magnitude
}

fn chart_candidate_area_order(
    hull: &[crate::geographic::ChartPointSite],
    left: Candidate,
    right: Candidate,
    frame: crate::geographic::GeographicFrame,
) -> Ordering {
    let supports = left.supports;
    let left_a = exact_chart_width(
        left.basis.along,
        hull[supports.along_min],
        hull[supports.along_max],
        frame,
    );
    let left_o = exact_chart_width(
        left.basis.outward,
        hull[supports.outward_min],
        hull[supports.outward_max],
        frame,
    );
    let left_n = left.basis.norm_exact().signed_magnitude().1;
    let supports = right.supports;
    let right_a = exact_chart_width(
        right.basis.along,
        hull[supports.along_min],
        hull[supports.along_max],
        frame,
    );
    let right_o = exact_chart_width(
        right.basis.outward,
        hull[supports.outward_min],
        hull[supports.outward_max],
        frame,
    );
    let right_n = right.basis.norm_exact().signed_magnitude().1;
    let left_ao: [u64; DOUBLE_LIMBS] = wide_product(&left_a, &left_o);
    let right_ao: [u64; DOUBLE_LIMBS] = wide_product(&right_a, &right_o);
    let lhs: [u64; TRIPLE_LIMBS] = wide_product(&left_ao, &right_n);
    let rhs: [u64; TRIPLE_LIMBS] = wide_product(&right_ao, &left_n);
    cmp_wide(&lhs, &rhs)
}

#[expect(
    clippy::indexing_slicing,
    reason = "caliper indices are advanced only within this admitted chart hull"
)]
fn exact_chart_argmax(
    hull: &[crate::geographic::ChartPointSite],
    direction: (f64, f64),
    frame: crate::geographic::GeographicFrame,
) -> usize {
    (1..hull.len()).fold(0, |best, index| {
        if chart_projection_delta_order(direction, hull[best], hull[index], frame).is_gt() {
            index
        } else {
            best
        }
    })
}

#[expect(
    clippy::indexing_slicing,
    reason = "the support pointer and its successor are bounded by the chart hull cycle"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the support pointer advances at most one chart hull length"
)]
fn advance_chart_max(
    hull: &[crate::geographic::ChartPointSite],
    direction: (f64, f64),
    mut index: usize,
    frame: crate::geographic::GeographicFrame,
) -> usize {
    let count = hull.len();
    for _ in 0..count {
        let current = index % count;
        let next = (index + 1) % count;
        if chart_projection_delta_order(direction, hull[current], hull[next], frame).is_lt() {
            break;
        }
        index += 1;
    }
    index
}

#[expect(
    clippy::indexing_slicing,
    reason = "support pointer and its successor are positions in the nonempty chart hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the cyclic successor is bounded by the chart hull length"
)]
fn chart_support_stops_here(
    hull: &[crate::geographic::ChartPointSite],
    direction: (f64, f64),
    index: usize,
    frame: crate::geographic::GeographicFrame,
) -> bool {
    let count = hull.len();
    let current = index % count;
    let next = (current + 1) % count;
    chart_projection_delta_order(direction, hull[current], hull[next], frame).is_lt()
}

#[expect(
    clippy::indexing_slicing,
    reason = "the caliper cycle and support indices are created from the admitted chart hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the hull contains at least three sites and each cyclic edge is in range"
)]
fn select_minimum_candidate_chart(
    hull: &[crate::geographic::ChartPointSite],
    frame: crate::geographic::GeographicFrame,
) -> Result<Candidate> {
    let count = hull.len();
    let mut first_basis = None;
    for edge in 0..count {
        first_basis = Basis::from_chart_edge(hull[edge], hull[(edge + 1) % count], frame)?;
        if first_basis.is_some() {
            break;
        }
    }
    let first_basis = first_basis.ok_or_else(minimum_rectangle_not_representable)?;
    let outward_direction = (-first_basis.outward.0, -first_basis.outward.1);
    let initial_outward_min = exact_chart_argmax(hull, outward_direction, frame);
    let mut outward_min = advance_chart_max(hull, outward_direction, initial_outward_min, frame);
    let mut along_max = advance_chart_max(hull, first_basis.along, outward_min, frame);
    let mut outward_max = advance_chart_max(hull, first_basis.outward, along_max, frame);
    let mut along_min = advance_chart_max(
        hull,
        (-first_basis.along.0, -first_basis.along.1),
        outward_max,
        frame,
    );
    let mut candidate_outward_min = initial_outward_min;
    let mut best: Option<Candidate> = None;
    for edge in 0..count {
        let basis = Basis::from_chart_edge(hull[edge], hull[(edge + 1) % count], frame)?
            .ok_or_else(minimum_rectangle_not_representable)?;
        if edge != 0 {
            along_max = advance_chart_max(hull, basis.along, along_max, frame);
            outward_min = advance_chart_max(
                hull,
                (-basis.outward.0, -basis.outward.1),
                outward_min,
                frame,
            );
            candidate_outward_min = outward_min;
            outward_max = advance_chart_max(hull, basis.outward, outward_max, frame);
            along_min = advance_chart_max(hull, (-basis.along.0, -basis.along.1), along_min, frame);
        }
        debug_assert!(chart_support_stops_here(
            hull,
            basis.along,
            along_max,
            frame
        ));
        debug_assert!(chart_support_stops_here(
            hull,
            (-basis.outward.0, -basis.outward.1),
            outward_min,
            frame
        ));
        debug_assert!(chart_support_stops_here(
            hull,
            basis.outward,
            outward_max,
            frame
        ));
        debug_assert!(chart_support_stops_here(
            hull,
            (-basis.along.0, -basis.along.1),
            along_min,
            frame
        ));
        let candidate = Candidate {
            basis,
            supports: Supports {
                along_min: along_min % count,
                along_max: along_max % count,
                outward_min: candidate_outward_min % count,
                outward_max: outward_max % count,
            },
        };
        if best.is_none_or(|current| {
            chart_candidate_area_order(hull, candidate, current, frame).is_lt()
        }) {
            best = Some(candidate);
        }
    }
    Ok(best
        .take()
        .expect("a three-site chart hull has at least one edge candidate"))
}

#[derive(Clone, Copy)]
struct ChartSupportHalfplane {
    gradient: (f64, f64),
    support: crate::geographic::ChartPointSite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExactChartPoint {
    x: ExactRational,
    y: ExactRational,
}

struct ChartRectangleCorner {
    exact: ExactChartPoint,
    point: Point,
}

struct RenderedChartCorner {
    point: Point,
    exact: ExactChartPoint,
}

fn chart_support_margin(
    halfplane: ChartSupportHalfplane,
    candidate: &ExactChartPoint,
    frame: GeographicFrame,
) -> Result<ExactRational> {
    let support_x = crate::geometry::exact_arith::retained_site_longitude(
        frame.longitude(),
        halfplane.support.longitude(),
    )?;
    let dx = candidate
        .x
        .try_subtract(&ExactRational::from_dyadic(support_x))?;
    let dy = candidate
        .y
        .try_subtract(&ExactRational::from_f64(halfplane.support.source_y()))?;
    Ok(ExactRational::from_f64(halfplane.gradient.0)
        .try_product(&dx)?
        .try_add(&ExactRational::from_f64(halfplane.gradient.1).try_product(&dy)?)?)
}

fn retained_chart_corner(
    first: ChartSupportHalfplane,
    second: ChartSupportHalfplane,
    frame: GeographicFrame,
) -> Result<ExactChartPoint> {
    use crate::geometry::tessellation::exact::ExactDyadic;

    let axis = frame.longitude();
    let origin_x =
        crate::geometry::exact_arith::retained_site_longitude(axis, first.support.longitude())?;
    let origin_y = ExactDyadic::from_f64(first.support.source_y());
    let second_x =
        crate::geometry::exact_arith::retained_site_longitude(axis, second.support.longitude())?;
    let second_y = ExactDyadic::from_f64(second.support.source_y());
    let dx = second_x.try_subtract(origin_x.clone())?;
    let dy = second_y.try_subtract(origin_y.clone())?;
    let second_residual = ExactDyadic::from_f64(second.gradient.0)
        .try_product(&dx)?
        .try_add(ExactDyadic::from_f64(second.gradient.1).try_product(&dy)?)?;
    let determinant = ExactDyadic::from_f64(first.gradient.0)
        .try_product(&ExactDyadic::from_f64(second.gradient.1))?
        .try_subtract(
            ExactDyadic::from_f64(first.gradient.1)
                .try_product(&ExactDyadic::from_f64(second.gradient.0))?,
        )?;
    let x_numerator = origin_x
        .try_product(&determinant)?
        .try_subtract(ExactDyadic::from_f64(first.gradient.1).try_product(&second_residual)?)?;
    let y_numerator = origin_y
        .try_product(&determinant)?
        .try_add(ExactDyadic::from_f64(first.gradient.0).try_product(&second_residual)?)?;
    Ok(ExactChartPoint {
        x: ExactRational::new(x_numerator, determinant.clone())?,
        y: ExactRational::new(y_numerator, determinant)?,
    })
}

fn render_chart_corner(
    corner: &ExactChartPoint,
    frame: GeographicFrame,
) -> Result<RenderedChartCorner> {
    let longitude =
        crate::geometry::exact_arith::retained_exact_site_sample(frame.longitude(), &corner.x)?;
    let latitude = corner.y.round()?;
    if !latitude.is_finite() {
        return Err(minimum_rectangle_not_representable().into());
    }
    Ok(RenderedChartCorner {
        point: Point::new_raw_xy(longitude.longitude.get(), latitude),
        exact: ExactChartPoint {
            x: longitude.chart_longitude,
            y: ExactRational::from_f64(latitude),
        },
    })
}

fn upward_nonnegative_ratio(numerator: &ExactRational, denominator: f64) -> Result<f64> {
    debug_assert!(denominator.is_finite() && denominator > 0.0);
    let ratio = numerator.try_quotient(&ExactRational::from_f64(denominator))?;
    let (_, upper) = ratio.outward_bounds()?;
    if !upper.is_finite() {
        return Err(minimum_rectangle_not_representable().into());
    }
    Ok(upper)
}

fn chart_corner_repair_failure(
    first: ChartSupportHalfplane,
    second: ChartSupportHalfplane,
    point: Point,
) -> crate::error::Error {
    GeometryErrorKind::Invalid(
        format!(
            "minimum_rotated_rectangle could not publish an outward corner for support ordinals {} and {} at ({}, {})",
            first.support.source_ordinal(),
            second.support.source_ordinal(),
            point.x,
            point.y,
        )
        .into(),
    )
    .into()
}

fn chart_corner_encloses_supports(
    first: ChartSupportHalfplane,
    second: ChartSupportHalfplane,
    point: &ExactChartPoint,
    frame: GeographicFrame,
) -> Result<bool> {
    Ok(chart_support_margin(first, point, frame)?.sign()
        != crate::geometry::exact_arith::ExactSign::Negative
        && chart_support_margin(second, point, frame)?.sign()
            != crate::geometry::exact_arith::ExactSign::Negative)
}

fn shifted_chart_corner(
    point: &ExactChartPoint,
    x_axis: bool,
    sign: f64,
    distance: f64,
) -> Result<ExactChartPoint> {
    let shift = ExactRational::from_f64(sign * distance);
    if x_axis {
        Ok(ExactChartPoint {
            x: point.x.try_add(&shift)?,
            y: point.y.clone(),
        })
    } else {
        Ok(ExactChartPoint {
            x: point.x.clone(),
            y: point.y.try_add(&shift)?,
        })
    }
}

fn neighboring_chart_corner(
    rendered: &RenderedChartCorner,
    x_axis: bool,
    sign: f64,
) -> Result<ExactChartPoint> {
    // The exact displacement is already outward. If RN lands inside its
    // support, one adjacent native value clears the rounding cell containing
    // that exact boundary.
    if x_axis {
        let native = if sign.is_sign_positive() {
            rendered.point.x.next_up()
        } else {
            rendered.point.x.next_down()
        };
        if !native.is_finite() {
            return Err(minimum_rectangle_not_representable().into());
        }
        let native_delta = ExactRational::from_f64(native)
            .try_subtract(&ExactRational::from_f64(rendered.point.x))?;
        Ok(ExactChartPoint {
            x: rendered.exact.x.try_add(&native_delta)?,
            y: rendered.exact.y.clone(),
        })
    } else {
        let latitude = if sign.is_sign_positive() {
            rendered.point.y.next_up()
        } else {
            rendered.point.y.next_down()
        };
        if !latitude.is_finite() {
            return Err(minimum_rectangle_not_representable().into());
        }
        Ok(ExactChartPoint {
            x: rendered.exact.x.clone(),
            y: ExactRational::from_f64(latitude),
        })
    }
}

fn repair_chart_corner(
    first: ChartSupportHalfplane,
    second: ChartSupportHalfplane,
    preferred: ExactChartPoint,
    frame: GeographicFrame,
) -> Result<ChartRectangleCorner> {
    let rendered = render_chart_corner(&preferred, frame)?;
    let first_margin = chart_support_margin(first, &rendered.exact, frame)?;
    let second_margin = chart_support_margin(second, &rendered.exact, frame)?;
    if first_margin.sign() != crate::geometry::exact_arith::ExactSign::Negative
        && second_margin.sign() != crate::geometry::exact_arith::ExactSign::Negative
    {
        return Ok(ChartRectangleCorner {
            exact: preferred,
            point: rendered.point,
        });
    }

    let mut best: Option<(f64, ChartRectangleCorner)> = None;
    for (x_axis, first_coefficient, second_coefficient) in [
        (true, first.gradient.0, second.gradient.0),
        (false, first.gradient.1, second.gradient.1),
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
        let first_distance =
            if first_margin.sign() == crate::geometry::exact_arith::ExactSign::Negative {
                let deficit = ExactRational::from_f64(-1.0).try_product(&first_margin)?;
                upward_nonnegative_ratio(&deficit, first_coefficient * sign)?
            } else {
                0.0
            };
        let second_distance =
            if second_margin.sign() == crate::geometry::exact_arith::ExactSign::Negative {
                let deficit = ExactRational::from_f64(-1.0).try_product(&second_margin)?;
                upward_nonnegative_ratio(&deficit, second_coefficient * sign)?
            } else {
                0.0
            };
        let distance = first_distance.max(second_distance);
        let candidate = shifted_chart_corner(&rendered.exact, x_axis, sign, distance)?;
        let candidate_rendered = render_chart_corner(&candidate, frame)?;
        let candidate_is_enclosing =
            chart_corner_encloses_supports(first, second, &candidate, frame)?;
        let candidate_publishes_enclosing =
            chart_corner_encloses_supports(first, second, &candidate_rendered.exact, frame)?;
        let accepted = if candidate_is_enclosing && candidate_publishes_enclosing {
            Some((candidate, candidate_rendered))
        } else {
            let neighbor = neighboring_chart_corner(&candidate_rendered, x_axis, sign)?;
            let neighbor_rendered = render_chart_corner(&neighbor, frame)?;
            let neighbor_is_enclosing =
                chart_corner_encloses_supports(first, second, &neighbor, frame)?;
            (neighbor_is_enclosing
                && chart_corner_encloses_supports(first, second, &neighbor_rendered.exact, frame)?)
            .then_some((neighbor, neighbor_rendered))
        };
        if let Some((exact, rendered)) = accepted {
            if best
                .as_ref()
                .is_none_or(|(best_movement, _)| distance < *best_movement)
            {
                best = Some((distance, ChartRectangleCorner {
                    exact,
                    point: rendered.point,
                }));
            }
        }
    }
    best.map(|(_, corner)| corner)
        .ok_or_else(|| chart_corner_repair_failure(first, second, rendered.point))
}

fn exact_chart_orientation(
    first: &ExactChartPoint,
    second: &ExactChartPoint,
    third: &ExactChartPoint,
) -> Result<crate::geometry::exact_arith::ExactSign> {
    let first_x = second.x.try_subtract(&first.x)?;
    let first_y = second.y.try_subtract(&first.y)?;
    let second_x = third.x.try_subtract(&first.x)?;
    let second_y = third.y.try_subtract(&first.y)?;
    Ok(first_x
        .try_product(&second_y)?
        .try_subtract(&first_y.try_product(&second_x)?)?
        .sign())
}

#[expect(
    clippy::indexing_slicing,
    reason = "caliper support indices are selected from this chart hull"
)]
fn emit_chart_candidate(
    hull: &[crate::geographic::ChartPointSite],
    candidate: Candidate,
    frame: GeographicFrame,
) -> Result<[ChartRectangleCorner; 4]> {
    let supports = candidate.supports;
    let specs = [
        (
            ChartSupportHalfplane {
                gradient: (-candidate.basis.along.0, -candidate.basis.along.1),
                support: hull[supports.along_min],
            },
            ChartSupportHalfplane {
                gradient: (-candidate.basis.outward.0, -candidate.basis.outward.1),
                support: hull[supports.outward_min],
            },
        ),
        (
            ChartSupportHalfplane {
                gradient: candidate.basis.along,
                support: hull[supports.along_max],
            },
            ChartSupportHalfplane {
                gradient: (-candidate.basis.outward.0, -candidate.basis.outward.1),
                support: hull[supports.outward_min],
            },
        ),
        (
            ChartSupportHalfplane {
                gradient: candidate.basis.along,
                support: hull[supports.along_max],
            },
            ChartSupportHalfplane {
                gradient: candidate.basis.outward,
                support: hull[supports.outward_max],
            },
        ),
        (
            ChartSupportHalfplane {
                gradient: (-candidate.basis.along.0, -candidate.basis.along.1),
                support: hull[supports.along_min],
            },
            ChartSupportHalfplane {
                gradient: candidate.basis.outward,
                support: hull[supports.outward_max],
            },
        ),
    ];
    let construct_corner =
        |(first, second): (ChartSupportHalfplane, ChartSupportHalfplane)| -> Result<_> {
            let preferred = retained_chart_corner(first, second, frame)?;
            if !chart_support_margin(first, &preferred, frame)?.is_zero()
                || !chart_support_margin(second, &preferred, frame)?.is_zero()
            {
                return Err(GeometryErrorKind::Invalid(
                    "exact chart corner does not lie on both selected support lines".into(),
                )
                .into());
            }
            repair_chart_corner(first, second, preferred, frame)
        };
    let [first_spec, second_spec, third_spec, fourth_spec] = specs;
    let first = construct_corner(first_spec)?;
    let second = construct_corner(second_spec)?;
    let third = construct_corner(third_spec)?;
    let fourth = construct_corner(fourth_spec)?;
    Ok([first, second, third, fourth])
}

struct PublishedChartBoundary {
    corners: [Option<ExactChartPoint>; 4],
    last: Option<ExactChartPoint>,
    first_record: Option<Point>,
    first_direction: Option<(ExactRational, ExactRational)>,
    last_direction: Option<(ExactRational, ExactRational)>,
    last_sample: Option<(usize, f64)>,
    nonzero_edges: usize,
    direction_wraps: usize,
}

impl PublishedChartBoundary {
    fn new() -> Self {
        Self {
            corners: [None, None, None, None],
            last: None,
            first_record: None,
            first_direction: None,
            last_direction: None,
            last_sample: None,
            nonzero_edges: 0,
            direction_wraps: 0,
        }
    }

    fn counterexample(
        edge: usize,
        parameter: f64,
        point: Point,
        claim: &str,
    ) -> crate::error::Error {
        GeometryErrorKind::Invalid(
            format!(
                "published minimum_rotated_rectangle boundary {claim} at edge {edge}, t={parameter}: ({}, {})",
                point.x, point.y,
            )
            .into(),
        )
        .into()
    }

    fn record(
        &mut self,
        edge: usize,
        parameter: f64,
        sample: &crate::geometry::exact_arith::RetainedComputedSample,
        point: Point,
    ) -> Result<()> {
        if self.last_sample == Some((3, 1.0))
            || edge >= 4
            || !(parameter.is_finite() && (0.0..=1.0).contains(&parameter))
        {
            return Err(Self::counterexample(
                edge,
                parameter,
                point,
                "has invalid root provenance",
            ));
        }
        if let Some((previous_edge, previous_parameter)) = self.last_sample {
            if edge < previous_edge
                || (edge == previous_edge && parameter < previous_parameter)
                || (edge > previous_edge
                    && (edge != previous_edge.wrapping_add(1) || previous_parameter != 1.0))
            {
                return Err(Self::counterexample(
                    edge,
                    parameter,
                    point,
                    "does not follow the directed root-edge order",
                ));
            }
        } else if edge != 0 || parameter != 0.0 {
            return Err(Self::counterexample(
                edge,
                parameter,
                point,
                "does not begin at the first root endpoint",
            ));
        }

        let exact = ExactChartPoint {
            x: sample.chart_longitude.clone(),
            y: ExactRational::from_f64(point.y),
        };
        let corner_index = if parameter == 0.0 {
            Some(edge)
        } else if parameter == 1.0 {
            Some(if edge == 3 { 0 } else { edge.wrapping_add(1) })
        } else {
            None
        };
        if let Some(index) = corner_index {
            let slot = self
                .corners
                .get_mut(index)
                .expect("rectangle root endpoint names one of its four corners");
            if slot.as_ref().is_some_and(|previous| previous != &exact) {
                return Err(Self::counterexample(
                    edge,
                    parameter,
                    point,
                    "does not preserve one exact corner across adjacent edges",
                ));
            }
            *slot = Some(exact.clone());
        }

        let is_closure = edge == 3 && parameter == 1.0;
        if self.last_sample.is_none() {
            self.first_record = Some(point);
            self.last = Some(exact.clone());
        } else {
            let first = self
                .corners
                .first()
                .and_then(Option::as_ref)
                .expect("the first sample records rectangle corner zero");
            if exact == *first && !is_closure {
                return Err(Self::counterexample(
                    edge,
                    parameter,
                    point,
                    "returns to its start before ring closure",
                ));
            }
            if is_closure && (exact != *first || Some(point) != self.first_record) {
                return Err(Self::counterexample(
                    edge,
                    parameter,
                    point,
                    "does not close with the first published record",
                ));
            }

            let previous = self
                .last
                .as_ref()
                .expect("a recorded boundary has its previous point");
            let direction = (
                exact.x.try_subtract(&previous.x)?,
                exact.y.try_subtract(&previous.y)?,
            );
            if direction.0.sign() != crate::geometry::exact_arith::ExactSign::Zero
                || direction.1.sign() != crate::geometry::exact_arith::ExactSign::Zero
            {
                if let Some(previous_direction) = &self.last_direction {
                    let turn = vector_cross(previous_direction, &direction)?;
                    if turn == crate::geometry::exact_arith::ExactSign::Negative {
                        return Err(Self::counterexample(
                            edge,
                            parameter,
                            point,
                            "is non-convex",
                        ));
                    }
                    let dot = previous_direction
                        .0
                        .try_product(&direction.0)?
                        .try_add(&previous_direction.1.try_product(&direction.1)?)?;
                    if turn == crate::geometry::exact_arith::ExactSign::Zero
                        && dot.sign() == crate::geometry::exact_arith::ExactSign::Negative
                    {
                        return Err(Self::counterexample(
                            edge,
                            parameter,
                            point,
                            "reverses along a collinear boundary",
                        ));
                    }
                    if vector_quadrant(previous_direction) > vector_quadrant(&direction) {
                        self.direction_wraps = self.direction_wraps.wrapping_add(1);
                    }
                } else {
                    self.first_direction = Some(direction.clone());
                }
                self.last_direction = Some(direction);
                self.nonzero_edges = self.nonzero_edges.wrapping_add(1);
            }
            self.last = Some(exact);
        }

        self.last_sample = Some((edge, parameter));
        Ok(())
    }

    fn finish(
        self,
        hull: &[crate::geographic::ChartPointSite],
        frame: GeographicFrame,
    ) -> Result<()> {
        if self.last_sample != Some((3, 1.0)) || self.nonzero_edges < 3 {
            return Err(GeometryErrorKind::Invalid(
                "published minimum_rotated_rectangle boundary is not a closed area".into(),
            )
            .into());
        }
        let first_direction = self
            .first_direction
            .as_ref()
            .expect("a boundary with three edges has a first direction");
        let last_direction = self
            .last_direction
            .as_ref()
            .expect("a boundary with three edges has a last direction");
        let closing_turn = vector_cross(last_direction, first_direction)?;
        if closing_turn == crate::geometry::exact_arith::ExactSign::Negative {
            return Err(GeometryErrorKind::Invalid(
                "published minimum_rotated_rectangle boundary is non-convex at closure".into(),
            )
            .into());
        }
        if closing_turn == crate::geometry::exact_arith::ExactSign::Zero {
            let closing_dot = last_direction
                .0
                .try_product(&first_direction.0)?
                .try_add(&last_direction.1.try_product(&first_direction.1)?)?;
            if closing_dot.sign() == crate::geometry::exact_arith::ExactSign::Negative {
                return Err(GeometryErrorKind::Invalid(
                    "published minimum_rotated_rectangle boundary reverses at closure".into(),
                )
                .into());
            }
        }
        let direction_wraps = self.direction_wraps.wrapping_add(usize::from(
            vector_quadrant(last_direction) > vector_quadrant(first_direction),
        ));
        if direction_wraps != 1 {
            return Err(GeometryErrorKind::Invalid(
                "published minimum_rotated_rectangle boundary does not make one convex circuit"
                    .into(),
            )
            .into());
        }

        let [Some(first), Some(second), Some(third), Some(fourth)] = self.corners else {
            return Err(GeometryErrorKind::Invalid(
                "published minimum_rotated_rectangle boundary omitted a support corner".into(),
            )
            .into());
        };
        certify_chart_rectangle(hull, [&first, &second, &third, &fourth], frame)?;
        Ok(())
    }
}

fn vector_cross(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<crate::geometry::exact_arith::ExactSign> {
    Ok(left
        .0
        .try_product(&right.1)?
        .try_subtract(&left.1.try_product(&right.0)?)?
        .sign())
}

fn vector_quadrant(vector: &(ExactRational, ExactRational)) -> u8 {
    use crate::geometry::exact_arith::ExactSign::Negative;

    let x = vector.0.sign();
    let y = vector.1.sign();
    if y == Negative {
        if x == Negative { 2 } else { 3 }
    } else if x == Negative {
        1
    } else {
        0
    }
}

fn certify_chart_rectangle(
    hull: &[crate::geographic::ChartPointSite],
    corners: [&ExactChartPoint; 4],
    frame: GeographicFrame,
) -> Result<()> {
    for side in 0..4 {
        let next = (side + 1) % 4;
        let opposite = (side + 2) % 4;
        if exact_chart_orientation(corners[side], corners[next], corners[opposite])?
            != crate::geometry::exact_arith::ExactSign::Positive
        {
            return Err(GeometryErrorKind::Invalid(
                format!(
                    "published minimum_rotated_rectangle support side {side} is not a convex corner edge"
                )
                .into(),
            )
            .into());
        }
    }

    // One source-hull pass checks every actual published directed corner side.
    for site in hull {
        let longitude = crate::geometry::exact_arith::retained_site_longitude(
            frame.longitude(),
            site.longitude(),
        )?;
        let source = ExactChartPoint {
            x: ExactRational::from_dyadic(longitude),
            y: ExactRational::from_f64(site.source_y()),
        };
        for side in 0..4 {
            let next = (side + 1) % 4;
            if exact_chart_orientation(corners[side], corners[next], &source)?
                == crate::geometry::exact_arith::ExactSign::Negative
            {
                return Err(GeometryErrorKind::Invalid(
                    format!(
                        "published minimum_rotated_rectangle excludes source hull ordinal {} at side {side}: ({}, {})",
                        site.source_ordinal(),
                        site.longitude().native().get(),
                        site.source_y(),
                    )
                    .into(),
                )
                .into());
            }
        }
    }
    Ok(())
}

fn publish_and_certify_chart_rectangle(
    hull: &[crate::geographic::ChartPointSite],
    corners: &[ChartRectangleCorner; 4],
    frame: GeographicFrame,
) -> Result<CoordSeq> {
    let first = corners
        .first()
        .expect("a chart rectangle has four constructed corners")
        .point;
    let mut points = corners
        .iter()
        .map(|corner| corner.point)
        .collect::<Vec<_>>();
    points.push(first);
    let mut budget = ExpansionBudget::new("minimum_rotated_rectangle", "computed edges");
    let mut boundary = PublishedChartBoundary::new();
    let published = crate::geographic::publish_computed_chain_with_vertex_intents_and_samples(
        frame,
        points.as_slice(),
        true,
        &mut budget,
        |edge, _, _| {
            let next = if edge == 3 { 0 } else { edge.wrapping_add(1) };
            let start = corners
                .get(edge)
                .ok_or_else(|| GeometryErrorKind::Invalid("unknown rectangle edge".into()))?;
            let end = corners
                .get(next)
                .ok_or_else(|| GeometryErrorKind::Invalid("unknown rectangle edge end".into()))?;
            Ok(ComputedEdgeIntent::exact_lifted([
                &start.exact.x,
                &end.exact.x,
            ]))
        },
        |_, _| ComputedVertexIntent::Sampled,
        |edge, _, _, parameter, _| {
            let next = if edge == 3 { 0 } else { edge.wrapping_add(1) };
            let start = corners.get(edge).ok_or_else(|| {
                GeometryErrorKind::Invalid("unknown rectangle sample edge".into())
            })?;
            let end = corners
                .get(next)
                .ok_or_else(|| GeometryErrorKind::Invalid("unknown rectangle sample end".into()))?;
            let latitude_delta = end.exact.y.try_subtract(&start.exact.y)?;
            let exact_parameter = ExactRational::from_f64(parameter);
            let latitude = start
                .exact
                .y
                .try_add(&latitude_delta.try_product(&exact_parameter)?)?
                .round()?;
            if !latitude.is_finite() {
                return Err(minimum_rectangle_not_representable().into());
            }
            Ok(ComputedSampleOrdinates {
                y: latitude,
                z: ZOrdinate(None),
                m: MOrdinate(None),
            })
        },
        |edge, _, _, parameter, retained, point| boundary.record(edge, parameter, retained, point),
    )?
    .ok_or_else(|| {
        GeometryErrorKind::Invalid(
            "sampled chart rectangle unexpectedly required no publication".into(),
        )
    })?;
    boundary.finish(hull, frame)?;
    CoordSeq::try_from_points(&published)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the support ordinal and its successor are positions in the nonempty convex hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the cyclic successor is bounded by the admitted hull length"
)]
fn support_stops_here(hull: &[Point], direction: (f64, f64), index: usize) -> bool {
    let current = index % hull.len();
    let next = (current + 1) % hull.len();
    projection_delta_order(direction, hull[current], hull[next]).is_lt()
}

#[expect(
    clippy::indexing_slicing,
    reason = "the fold retains source ordinals from this hull slice"
)]
fn exact_argmax(hull: &[Point], direction: (f64, f64)) -> usize {
    (1..hull.len()).fold(0, |best, index| {
        if projection_delta_order(direction, hull[best], hull[index]).is_gt() {
            index
        } else {
            best
        }
    })
}

#[expect(
    clippy::indexing_slicing,
    reason = "support and successor ordinals remain in the admitted convex hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the unwrapped support pointer advances at most one hull length from an admitted ordinal"
)]
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
        && hull
            .iter()
            .zip(hull.iter().cycle().skip(1))
            .all(|(point, next)| {
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
            Point::new_raw_xy(min_x, min_y),
            Point::new_raw_xy(max_x, min_y),
            Point::new_raw_xy(max_x, max_y),
            Point::new_raw_xy(min_x, max_y),
            Point::new_raw_xy(min_x, min_y),
        ]
    })
}

fn exact_width(direction: (f64, f64), minimum: Point, maximum: Point) -> [u64; LIMBS] {
    let exact = exact_dot_delta(direction, minimum, maximum);
    let (sign, magnitude) = exact.signed_magnitude();
    debug_assert_eq!(sign, Ordering::Greater);
    magnitude
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
fn wide_product<const L: usize, const R: usize, const OUT: usize>(
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

fn add_triple_magnitude(
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

fn nonzero_span(words: &[u64]) -> Option<(usize, usize)> {
    let first = words.iter().position(|&word| word != 0)?;
    let last = words
        .iter()
        .rposition(|&word| word != 0)
        .expect("nonzero first word");
    Some((first, last))
}

fn cmp_wide(left: &[u64; TRIPLE_LIMBS], right: &[u64; TRIPLE_LIMBS]) -> Ordering {
    for (left_word, right_word) in left.iter().zip(right).rev() {
        match left_word.cmp(right_word) {
            Ordering::Equal => {},
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
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

#[expect(
    clippy::indexing_slicing,
    reason = "candidate support ordinals are generated from this same convex hull"
)]
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

/// A certified floating filter for the exact score.  It deliberately charges
/// the input-coordinate products, rather than trusting a rounded
/// `(support - support)` residual.  At large translations that makes the
/// interval wider (and selects the exact fallback); for ordinary data it
/// resolves the comparison without allocating or multiplying limb arrays.
#[expect(
    clippy::indexing_slicing,
    reason = "candidate support ordinals are generated from this same convex hull"
)]
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

#[expect(
    clippy::indexing_slicing,
    reason = "candidate support ordinals are generated from this same convex hull"
)]
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
    #[expect(
        clippy::indexing_slicing,
        reason = "caliper support IDs are source ordinals into this same convex hull"
    )]
    fn emit(self, hull: &[Point]) -> Result<[Point; 5]> {
        // The legacy formula intentionally omits /N.  It is retained only when
        // the rounded quadrilateral itself carries the constant-time actual
        // edge certificate below; this is the compatibility fast path, never
        // an enclosure test over the hull.
        if let Some([legacy_a, legacy_b, legacy_c, legacy_d]) = self.legacy_corners(hull)
            && legacy_rectangle_is_certified(hull, &[legacy_a, legacy_b, legacy_c, legacy_d], self)
        {
            return Ok([legacy_a, legacy_b, legacy_c, legacy_d, legacy_a]);
        }

        let supports = self.supports;
        let [preferred_a, preferred_b, preferred_c, preferred_d] = self
            .correct_corners(hull)
            .ok_or_else(minimum_rectangle_not_representable)?;
        let [corner_a, corner_b, corner_c, corner_d] = [
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
                &preferred_a,
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
                &preferred_b,
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
                &preferred_c,
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
                &preferred_d,
            ),
        ];
        let corner_a = corner_a?;
        let corner_b = corner_b?;
        let corner_c = corner_c?;
        let corner_d = corner_d?;
        Ok([corner_a, corner_b, corner_c, corner_d, corner_a])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "candidate support ordinals are generated from this same convex hull"
    )]
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
            Point::new_raw_xy(
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

    #[expect(
        clippy::indexing_slicing,
        reason = "candidate support ordinals are generated from this same convex hull"
    )]
    fn correct_corners(self, hull: &[Point]) -> Option<[PreferredCorner; 4]> {
        let supports = self.supports;
        let origin = hull[supports.along_min];
        Some([
            self.unframe_corner(origin, hull[supports.along_min], hull[supports.outward_min])?,
            self.unframe_corner(origin, hull[supports.along_max], hull[supports.outward_min])?,
            self.unframe_corner(origin, hull[supports.along_max], hull[supports.outward_max])?,
            self.unframe_corner(origin, hull[supports.along_min], hull[supports.outward_max])?,
        ])
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
    ) -> Option<PreferredCorner> {
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
        if !sx.is_finite() || sx <= 0.0 || !sy.is_finite() || sy <= 0.0 {
            return None;
        }
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
        if ![
            origin_x, origin_y, along_x, along_y, outward_x, outward_y, along.0, along.1,
            outward.0, outward.1,
        ]
        .into_iter()
        .all(|value| value.is_finite())
        {
            return None;
        }
        let along_residual = along.0 * (along_x - origin_x) + along.1 * (along_y - origin_y);
        let outward_residual =
            outward.0 * (outward_x - origin_x) + outward.1 * (outward_y - origin_y);
        let determinant = along.0 * outward.1 - along.1 * outward.0;
        if !along_residual.is_finite()
            || !outward_residual.is_finite()
            || !determinant.is_finite()
            || determinant <= 0.0
        {
            return None;
        }
        let denominator = [determinant * sx, determinant * sy];
        if !denominator
            .into_iter()
            .all(|value| value.is_finite() && value > 0.0)
        {
            return None;
        }
        let x_value = (origin_x * determinant + along_residual * outward.1
            - along.1 * outward_residual)
            / determinant
            / sx;
        let y_value = (origin_y * determinant + along.0 * outward_residual
            - along_residual * outward.0)
            / determinant
            / sy;
        let mut x_numerator = ExactSum::zero();
        x_numerator.add_product(origin_x, determinant);
        x_numerator.add_product(along_residual, outward.1);
        x_numerator.subtract_product(along.1, outward_residual);
        let mut y_numerator = ExactSum::zero();
        y_numerator.add_product(origin_y, determinant);
        y_numerator.add_product(along.0, outward_residual);
        y_numerator.subtract_product(along_residual, outward.0);
        Some(PreferredCorner {
            point: Point::new_raw_xy(x_value, y_value),
            numerator: [x_numerator, y_numerator],
            denominator,
            anisotropic_frame: !equal_scales,
        })
    }
}

fn exact_cross_delta(start: Point, end: Point, current: Point, next: Point) -> ExactSum {
    // cross(end-start, next-current), expanded before any subtraction so this
    // remains exact even when an intermediate world-coordinate difference
    // overflows.
    let mut sum = ExactSum::zero();
    sum.add_product(end.x, next.y);
    sum.subtract_product(start.x, next.y);
    sum.subtract_product(end.x, current.y);
    sum.add_product(start.x, current.y);
    sum.subtract_product(end.y, next.x);
    sum.add_product(start.y, next.x);
    sum.add_product(end.y, current.x);
    sum.subtract_product(start.y, current.x);
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
    clippy::indexing_slicing,
    reason = "candidate support ordinals and their cyclic neighbors are within this convex hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "support ordinals are below count and Vec lengths bound the cyclic neighbor arithmetic"
)]
fn legacy_rectangle_is_certified(
    hull: &[Point],
    corners: &[Point; 4],
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
    for ((start, end), index) in corners
        .iter()
        .copied()
        .zip(corners.iter().copied().cycle().skip(1))
        .zip(support)
    {
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
        inside.add_product(end.x, current.y);
        inside.subtract_product(start.x, current.y);
        inside.subtract_product(end.y, current.x);
        inside.add_product(start.y, current.x);
        inside.subtract_product(end.x, start.y);
        inside.add_product(start.x, end.y);
        if inside.ordering().is_lt() {
            return false;
        }
    }
    corners
        .iter()
        .copied()
        .zip(corners.iter().copied().cycle().skip(1))
        .zip(corners.iter().copied().cycle().skip(2))
        .all(|((first, second), third)| {
            orientation(first, second, third) == Orientation::CounterClockwise
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

fn minimum_rectangle_not_representable() -> crate::error::Error {
    GeometryErrorKind::message(
        "minimum_rotated_rectangle result is not representable with finite coordinates",
    )
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
    finite_support_wedge_point(first, second, preferred)
        .ok_or_else(minimum_rectangle_not_representable)
}

fn axis_corner(first: SupportHalfplane, second: SupportHalfplane) -> Option<Point> {
    let (along, outward) = if first.gradient.0 == 0.0 || first.gradient.1 == 0.0 {
        (first, second)
    } else {
        (second, first)
    };
    if along.gradient.1 == 0.0 && outward.gradient.0 == 0.0 {
        Some(Point::new_raw_xy(along.support.x, outward.support.y))
    } else if along.gradient.0 == 0.0 && outward.gradient.1 == 0.0 {
        Some(Point::new_raw_xy(outward.support.x, along.support.y))
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
            Axis::X => Point::new_raw_xy(shifted, preferred.point.y),
            Axis::Y => Point::new_raw_xy(preferred.point.x, shifted),
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
    deficit.negate();
    let interval = exact_quotient_interval(&deficit, positive_coefficient);
    interval.hi.is_finite().then_some(interval.hi)
}

#[derive(Clone, Copy)]
enum Axis {
    X,
    Y,
}

impl Axis {
    const fn select<T>(self, pair: &[T; 2]) -> &T {
        let [x, y] = pair;
        match self {
            Self::X => x,
            Self::Y => y,
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

#[expect(
    clippy::arithmetic_side_effects,
    reason = "low and high are ordered nonnegative f64 total-order encodings and the midpoint stays between them"
)]
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
    product.add_product(denominator, candidate);
    product.compare_positive_magnitude(magnitude)
}

/// Finite feasibility is decided on the four square boundaries.  We enumerate
/// all boundary candidates and choose the exact minimum squared world-coordinate
/// Euclidean displacement from the preferred corner; equal exact distances break by
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
        numerator.add_product(free_coefficient, free_support);
        numerator.subtract_product(fixed_coefficient, fixed);
        numerator.add_product(fixed_coefficient, fixed_support);
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
        Axis::X => Point::new_raw_xy(fixed, free),
        Axis::Y => Point::new_raw_xy(free, fixed),
    };
    (!support_margin(first, candidate).ordering().is_lt()
        && !support_margin(second, candidate).ordering().is_lt())
    .then_some(candidate)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "low and high are ordered total-order encodings, so binary-search subtraction and midpoint stay in range"
)]
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
    match cmp_wide(&left_distance, &right_distance) {
        Ordering::Equal => total_order_bits(left.x)
            .cmp(&total_order_bits(right.x))
            .then_with(|| total_order_bits(left.y).cmp(&total_order_bits(right.y))),
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
    }
}

fn exact_squared_distance(point: Point, preferred: &PreferredCorner) -> [u64; TRIPLE_LIMBS] {
    let x = preferred
        .coordinate_difference(Axis::X, point.x)
        .signed_magnitude()
        .1;
    let y = preferred
        .coordinate_difference(Axis::Y, point.y)
        .signed_magnitude()
        .1;
    let denominator_square = |axis: Axis| {
        let denominator = *axis.select(&preferred.denominator);
        let mut square = ExactSum::zero();
        square.add_product(denominator, denominator);
        square.signed_magnitude().1
    };
    let x_denominator_square = denominator_square(Axis::X);
    let y_denominator_square = denominator_square(Axis::Y);
    // `(Rx / Dx)^2 + (Ry / Dy)^2` has common positive denominator
    // `Dx^2 * Dy^2`, so compare its exact numerator instead.
    let x_square: [u64; DOUBLE_LIMBS] = wide_product(&x, &x);
    let y_square: [u64; DOUBLE_LIMBS] = wide_product(&y, &y);
    add_triple_magnitude(
        &wide_product(&x_square, &y_denominator_square),
        &wide_product(&y_square, &x_denominator_square),
    )
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
    use crate::geographic::PointSelectionChart;
    use crate::geometry::{CoordinateAxes, Coordinates as _, Shape};

    fn point(x: f64, y: f64) -> Point {
        Point::new_raw_xy(x, y)
    }

    fn point_selection_chart(points: &[Point]) -> (GeographicFrame, PointSelectionChart) {
        let frame = GeographicFrame::crs84();
        let shape = Shape::multi_point(
            CoordinateAxes::XY,
            CoordSeq::try_from_points(points).expect("point selection coordinates"),
        );
        let chart = PointSelectionChart::try_from_shape(frame, &shape, None)
            .expect("point selection chart")
            .expect("nonempty point support");
        (frame, chart)
    }

    fn chart_hull(points: &[Point]) -> (GeographicFrame, Vec<crate::geographic::ChartPointSite>) {
        let (frame, chart) = point_selection_chart(points);
        let sites = chart
            .sites_from_points(points)
            .expect("retained chart sites");
        let indices = crate::geometry::monotone_chain_chart_hull(&sites, frame)
            .expect("common-chart point hull");
        let hull = indices
            .into_iter()
            .map(|index| *sites.get(index).expect("hull site belongs to chart sites"))
            .collect();
        (frame, hull)
    }

    fn hull_vertices(points: &[Point]) -> Vec<Point> {
        super::super::monotone_chain_hull(points)
            .into_iter()
            .map(|index| points[index])
            .collect()
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
    fn caliper_support_progression_handles_rotated_hulls() {
        // `select_minimum_candidate`'s debug assertions require every
        // unwrapped pointer to stop at an exact local support maximum. The
        // affine family below exercises rotations, anisotropy, and offsets
        // across several hull sizes.
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
                let hull = hull_vertices(&points);
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
        x_numerator.add_product(f64::MAX, 1.0);
        x_numerator.add_product(f64::MAX, 1.0);
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
        let product: [u64; DOUBLE_LIMBS] = wide_product(&left, &right);
        assert_eq!(product[0], 1);
        assert_eq!(product[1], u64::MAX - 1);
        assert_eq!(product[2], u64::MAX);
        assert!(product[3..].iter().all(|&word| word == 0));
    }

    #[test]
    fn overflowing_preferred_keeps_its_exact_magnitude_for_displacement() {
        let mut x_numerator = ExactSum::zero();
        x_numerator.add_product(f64::MAX, 1.0);
        x_numerator.add_product(f64::MAX, 1.0);
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
    fn anisotropic_displacement_uses_world_coordinate_denominators() {
        let preferred = PreferredCorner {
            point: point(0.0, 0.0),
            numerator: [ExactSum::zero(), ExactSum::zero()],
            denominator: [1.0, 2.0],
            anisotropic_frame: true,
        };
        // These candidates have world squared distances 1 and 2.25. Their raw
        // residual-numerator scores are 4 and 2.25, the inverse ordering.
        assert_eq!(
            displacement_order(point(0.0, 1.0), point(1.5, 0.0), &preferred),
            Ordering::Less
        );
    }

    #[test]
    fn equal_denominator_displacement_keeps_the_raw_residual_order() {
        let preferred = PreferredCorner {
            point: point(0.0, 0.0),
            numerator: [ExactSum::zero(), ExactSum::zero()],
            denominator: [2.0; 2],
            anisotropic_frame: false,
        };
        assert_eq!(
            displacement_order(point(0.0, 1.0), point(1.5, 0.0), &preferred),
            Ordering::Less
        );
    }

    #[test]
    fn anisotropic_displacement_weights_negative_axis_residuals() {
        let preferred = PreferredCorner {
            point: point(0.0, 0.0),
            numerator: [ExactSum::zero(), ExactSum::zero()],
            denominator: [2.0, 1.0],
            anisotropic_frame: true,
        };
        // The x candidate is 1 away in world coordinates, while the negative
        // y candidate is 2.25 away. Their raw residual scores are 4 and 2.25,
        // so numerator-only ranking incorrectly chooses the y candidate.
        assert_eq!(
            displacement_order(point(1.0, 0.0), point(0.0, -1.5), &preferred),
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
        let rectangle = hull_vertices(&rectangle);
        let candidate = select_minimum_candidate(&rectangle);
        assert!(legacy_rectangle_is_certified(
            &rectangle,
            &candidate.legacy_corners(&rectangle).unwrap(),
            candidate,
        ));
    }

    #[test]
    fn chart_support_intersection_keeps_1798_fifths_before_native_rounding() {
        use crate::geometry::tessellation::exact::ExactDyadic;

        let (frame, chart) =
            point_selection_chart(&[point(170.0, 0.0), point(180.0, 5.0), point(190.0, 10.0)]);
        let along_min = chart
            .site(point(360.0, 0.0), 0)
            .expect("retained along support");
        let outward_max = chart
            .site(point(360.0, 1.0), 1)
            .expect("retained outward support");
        assert_eq!(along_min.longitude().periods(), 1);
        assert_eq!(outward_max.longitude().periods(), 1);
        let first = ChartSupportHalfplane {
            gradient: (2.0, 1.0),
            support: along_min,
        };
        let second = ChartSupportHalfplane {
            gradient: (-1.0, 2.0),
            support: outward_max,
        };

        let corner = retained_chart_corner(first, second, frame).unwrap();
        assert_eq!(
            corner.x,
            ExactRational::new(ExactDyadic::from_i64(1798), ExactDyadic::from_i64(5)).unwrap()
        );
        assert_eq!(
            corner.y,
            ExactRational::new(ExactDyadic::from_i64(4), ExactDyadic::from_i64(5)).unwrap()
        );
        let rendered = render_chart_corner(&corner, frame).unwrap();
        assert_ne!(corner.x, rendered.exact.x);
        assert_ne!(corner.y, rendered.exact.y);
    }

    #[test]
    fn chart_corner_repair_moves_the_exact_endpoint_outward_after_rn() {
        use crate::geometry::tessellation::exact::ExactDyadic;

        let (frame, chart) = point_selection_chart(&[point(0.0, 0.0), point(1.0, 0.0)]);
        let first = ChartSupportHalfplane {
            gradient: (1.0, 1.0),
            support: chart.site(point(1.0, 0.0), 0).expect("first support site"),
        };
        let second = ChartSupportHalfplane {
            gradient: (-1.0, 2.0),
            support: chart.site(point(0.0, 0.0), 1).expect("second support site"),
        };
        let ideal = retained_chart_corner(first, second, frame).unwrap();
        assert_eq!(
            ideal.x,
            ExactRational::new(ExactDyadic::from_i64(2), ExactDyadic::from_i64(3)).unwrap()
        );
        assert_eq!(
            ideal.y,
            ExactRational::new(ExactDyadic::from_i64(1), ExactDyadic::from_i64(3)).unwrap()
        );
        let trial = render_chart_corner(&ideal, frame).unwrap();
        assert_eq!(
            chart_support_margin(first, &trial.exact, frame)
                .unwrap()
                .sign(),
            crate::geometry::exact_arith::ExactSign::Negative
        );

        let repaired = repair_chart_corner(first, second, ideal.clone(), frame).unwrap();
        assert_ne!(repaired.exact, ideal);
        assert_ne!(repaired.point.y.to_bits(), trial.point.y.to_bits());
        assert!(chart_corner_encloses_supports(first, second, &repaired.exact, frame,).unwrap());
        let actual = render_chart_corner(&repaired.exact, frame).unwrap();
        assert!(chart_corner_encloses_supports(first, second, &actual.exact, frame).unwrap());
    }

    #[test]
    fn oblique_chart_rectangle_publishes_a_non_dyadic_support_corner() {
        use crate::geometry::tessellation::exact::ExactDyadic;

        let (frame, hull) = chart_hull(&[
            point(0.0, 0.0),
            point(0.0, 1.0),
            point(2.0, 1.0),
            point(1.0, 1.5),
        ]);
        let candidate = select_minimum_candidate_chart(&hull, frame).unwrap();
        let corners = emit_chart_candidate(&hull, candidate, frame).unwrap();
        let expected = ExactChartPoint {
            x: ExactRational::new(ExactDyadic::from_i64(-2), ExactDyadic::from_i64(5)).unwrap(),
            y: ExactRational::new(ExactDyadic::from_i64(4), ExactDyadic::from_i64(5)).unwrap(),
        };
        let corner = corners
            .iter()
            .find(|corner| corner.exact == expected)
            .expect("caliper support intersection retains its exact quotient");
        let rendered = render_chart_corner(&corner.exact, frame).unwrap();
        assert_ne!(corner.exact.x, rendered.exact.x);
        assert_ne!(corner.exact.y, rendered.exact.y);

        let published = publish_and_certify_chart_rectangle(&hull, &corners, frame).unwrap();
        assert_eq!(published.coord_count(), 5);
    }

    #[test]
    fn chart_rectangle_certificate_includes_generated_side_samples() {
        let source = [
            point(-170.0, 0.0),
            point(0.0, 0.0),
            point(170.0, 0.0),
            point(-170.0, 10.0),
            point(0.0, 10.0),
            point(170.0, 10.0),
        ];
        let (frame, hull) = chart_hull(&source);
        let mut ordered = hull.clone();
        ordered.sort_unstable_by(|left, right| left.compare_chart_xy(*right));
        let axis = frame.longitude();
        let west = axis
            .lifted_site_longitude(
                &ordered
                    .first()
                    .expect("chart hull has a west site")
                    .longitude(),
            )
            .unwrap();
        let east = axis
            .lifted_site_longitude(
                &ordered
                    .last()
                    .expect("chart hull has an east site")
                    .longitude(),
            )
            .unwrap();
        assert_eq!(east - west, 190.0);

        let rectangle = minimum_area_rectangle_chart(&hull, frame).unwrap();
        let vertices = (0..rectangle.coord_count())
            .map(|index| rectangle.nth_coord(index))
            .collect::<Vec<_>>();
        assert!(
            vertices.len() > 5,
            "both long sides require publication samples"
        );
        assert_eq!(vertices.first(), vertices.last());
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
            exact.add_product(numerator, 1.0);
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
        let points = [
            point(-1.75 * scale, -1.75 * scale),
            point(-1.5 * scale, -0.25 * scale),
            point(-1.75 * scale, -1.5 * scale),
        ];
        let hull = hull_vertices(&points);
        let error = minimum_area_rectangle(&hull).expect_err("unrepresentable support wedge");
        assert_eq!(
            error.to_string(),
            "minimum_rotated_rectangle result is not representable with finite coordinates",
        );
    }

    #[test]
    fn score_divides_by_the_exact_rounded_basis_norm() {
        // Exact support widths alone choose edge zero on this fixture, while
        // the physical area `(da * do) / N` chooses edge one.
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

    #[test]
    fn exact_equal_area_candidates_keep_the_first_hull_edge() {
        let a = point(0.0, 0.0);
        let d = point(1.0, -1.0);
        let c = point(2.0, 0.0);
        let b = point(1.0, 1.0);
        let hull = [a, d, c, b];
        let first_basis = Basis::from_edge(a, d).expect("nonzero first edge");
        let second_basis = Basis::from_edge(d, c).expect("nonzero second edge");
        let mut first = RankedCandidate::new(&hull, Candidate {
            basis: first_basis,
            supports: Supports {
                along_min: 0,
                along_max: 1,
                outward_min: 0,
                outward_max: 2,
            },
        });
        let mut second = RankedCandidate::new(&hull, Candidate {
            basis: second_basis,
            supports: Supports {
                along_min: 0,
                along_max: 2,
                outward_min: 1,
                outward_max: 0,
            },
        });
        assert_eq!(
            candidate_area_order(&hull, &mut first, &mut second),
            Ordering::Equal
        );

        let selected = select_minimum_candidate(&hull);
        assert_eq!(
            selected.basis.along.0.to_bits(),
            first_basis.along.0.to_bits()
        );
        assert_eq!(
            selected.basis.along.1.to_bits(),
            first_basis.along.1.to_bits()
        );
    }
}
