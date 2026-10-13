//! Minimum-area enclosing rectangles over exact support lines.
//!
//! The calipers retain the *world-coordinate* support vertices.  Floating
//! projections are only a filter: every ambiguous support comparison, score
//! comparison, and emitted support inequality is evaluated as an exact dyadic
//! sum.  Consequently the final four-corner construction needs no hull scan.

mod calipers;
mod chart;
mod corners;
mod score;

use std::cmp::Ordering;

#[cfg(test)]
#[cfg(test)]
use super::monotone_chain_hull;
use crate::error::Result;
use crate::geographic::{
    ComputedEdgeIntent, ComputedSampleOrdinates, ComputedVertexIntent, GeographicFrame,
};
use crate::geometry::exact_arith::product_sum::{ExactSum, LIMBS, PRODUCT_MIN_EXP, cmp_magnitude};
use crate::geometry::exact_arith::{
    ExactSign, RetainedComputedSample, checked_dd_mul as double_double_mul,
    checked_two_product as two_product, dd_add as double_double_add, dd_sub as double_double_sub,
    retained_exact_site_sample, retained_site_longitude, two_sum,
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
        let start_x = axis.lifted_site_longitude(start_longitude)?;
        let end_x = axis.lifted_site_longitude(end_longitude)?;
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

/// Minimum-area enclosing rectangle of a CCW convex open ring.  All final
/// enclosure work is bounded by four corners and eight exact margins.
pub(crate) fn minimum_area_rectangle(hull: &[Point]) -> Result<CoordSeq> {
    debug_assert!(hull.len() >= 3);
    if let Some(bounds) = calipers::axis_aligned_hull_rectangle(hull) {
        return CoordSeq::try_from_points(&bounds);
    }
    let best = calipers::select_minimum_candidate(hull);
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
        return Err(corners::minimum_rectangle_not_representable());
    }
    let candidate = chart::calipers::select_minimum_candidate_chart(hull, frame)?;
    let corners = chart::corners::emit_chart_candidate(hull, candidate, frame)?;
    chart::publication::publish_and_certify_chart_rectangle(hull, &corners, frame)
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

#[derive(Clone, Copy)]
struct SupportHalfplane {
    gradient: (f64, f64),
    support: Point,
}

type DoubleDouble = (f64, f64);

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

#[cfg(test)]
mod tests;
