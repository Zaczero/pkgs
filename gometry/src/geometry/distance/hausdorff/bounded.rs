//! Continuous planar Hausdorff distance with a certified error bound.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::error::Result;
use crate::geometry::distance::exact_segment::{
    ExactCarrier, ExactPoint, UnreducedRatio, point_span_distance_squared,
};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::overlay::{
    AreaLocationStrategy, ExactMetricSpan, ExactMetricSupport, ExactSourceInventory,
};
use crate::geometry::tessellation::exact;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational as Rational};
use crate::geometry::{
    CoordSeq, DdInterval, ExpansionBudget, LineSeq, PackedLineColumnView, Point, Segment, Shape,
    XY, storage,
};

mod cycle;
mod index;
pub(crate) mod inradius;
mod instrumentation;
mod numeric;
mod patch;
mod queue;
mod search;
mod span_bounds;
mod sqrt;
mod support;
pub(crate) mod symmetric;
mod upper;

use index::{BoundaryEdge, PrimitiveId, SupportIndex};
use instrumentation::line_filter_enabled;
#[cfg(test)]
use instrumentation::{
    count_exact_span_evaluation, count_refinement_work, exact_span_evaluations,
    limit_refinement_work, refinement_work, reset_exact_span_evaluations, set_line_filter_enabled,
};
use numeric::{
    Accuracy, DistanceInterval, ErrorBound, FloatBox, SelectedSpan, SquaredBounds, SupportSpan,
    Tightness,
};

const NOMINATED_SITE_COUNT: usize = 4;

const NEAREST_CANDIDATE_LIMIT: usize = 32;

const HULL_SEPARATION_LIMIT: usize = 8;

#[derive(Debug)]
pub(crate) struct PreparedSupport {
    spans: Arc<[SupportSpan]>,
    points: Arc<[ExactPoint]>,
    faces: Vec<Vec<Vec<ExactPoint>>>,
    boundary_spans: Option<Vec<bool>>,
    boundary_points: Option<Vec<bool>>,
    boundary_edges: Vec<BoundaryEdge>,
    area_bounds: Option<FloatBox>,
    index: Arc<SupportIndex>,
    area_locator:
        Option<Arc<crate::geometry::ExactLocationIndex<crate::geometry::AreaLocationLabels>>>,
    parallel_carriers: bool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SqrtStats {
    fast_admissions: usize,
    fallbacks: usize,
}

#[derive(Debug)]
enum SourcePatch {
    Span {
        span: usize,
        lo: Rational,
        hi: Rational,
    },
    Box(FloatBox),
    Point(usize),
}

#[derive(Debug)]
struct SqrtInterval {
    lower: ExactDyadic,
    upper: Option<ExactDyadic>,
    overflow_upper: Option<Rational>,
}

#[derive(Clone, Copy, Debug)]
struct FloatSqrtBracket {
    lower: f64,
    upper: Option<f64>,
}

#[derive(Debug)]
struct PatchRecord {
    patch: SourcePatch,
    upper: Option<Rational>,
}

struct SearchContext<'a> {
    accuracy: &'a Accuracy,
    budget: ExpansionBudget,
    lower_squared: Option<Rational>,
    lower_witness: Option<ExactPoint>,
    exact_upper: bool,
    require_positive_witness: bool,
    output_scale: Rational,
    output_tolerance: Rational,
}

impl<'a> SearchContext<'a> {
    const fn new(accuracy: &'a Accuracy) -> Self {
        Self {
            accuracy,
            budget: ExpansionBudget::new("hausdorff_distance", "max_error"),
            lower_squared: None,
            lower_witness: None,
            exact_upper: false,
            require_positive_witness: false,
            output_scale: Rational::one(),
            output_tolerance: Rational::zero(),
        }
    }

    const fn new_directed(
        accuracy: &'a Accuracy,
        output_scale: Rational,
        output_tolerance: Rational,
    ) -> Self {
        Self {
            accuracy,
            budget: ExpansionBudget::new("maximum_inscribed_circle", "tolerance"),
            lower_squared: None,
            lower_witness: None,
            exact_upper: true,
            require_positive_witness: false,
            output_scale,
            output_tolerance,
        }
    }

    fn update_lower(&mut self, squared: Rational) -> Result<()> {
        let replace = match &self.lower_squared {
            None => true,
            Some(current) => squared.try_cmp(current)? == Ordering::Greater,
        };
        if replace {
            self.lower_squared = Some(squared);
        }
        Ok(())
    }

    fn update_lower_with_witness(&mut self, squared: Rational, witness: &ExactPoint) -> Result<()> {
        let replace = match &self.lower_squared {
            None => true,
            Some(current) => squared.try_cmp(current)? == Ordering::Greater,
        };
        if replace {
            let witness = witness.clone();
            self.lower_squared = Some(squared);
            self.lower_witness = Some(witness);
        }
        // Equal certified clearances intentionally retain the first witness
        // visited. Patch queue insertion order is monotonic and tie-stable.
        Ok(())
    }

    fn scaled_squared(&self, squared: &Rational) -> Result<Rational> {
        squared.try_product(&self.accuracy.scale_squared)
    }

    /// A thirty-second of the answer's error budget, in source units, bounds
    /// how loosely a squared distance may be enclosed. A budget that does not
    /// round to a finite binary64 allows no slack, so evaluation stays exact.
    fn tightness(&self) -> Tightness {
        const SHARE: f64 = 1.0 / 32.0;
        let finite = |value: Result<f64>| value.ok().filter(|value| value.is_finite());
        let absolute = |budget: Result<f64>| Tightness {
            relative: 0.0,
            absolute: finite(budget).unwrap_or(0.0) * SHARE,
        };
        if self.exact_upper {
            return absolute(
                self.output_tolerance
                    .try_quotient(&self.output_scale)
                    .and_then(|budget| budget.round()),
            );
        }
        match &self.accuracy.error {
            ErrorBound::Relative(relative) => Tightness {
                relative: finite(relative.round()).unwrap_or(0.0) * SHARE,
                absolute: 0.0,
            },
            ErrorBound::Absolute(error) => absolute(Ok(error.round().unwrap_or(0.0)
                / finite(self.accuracy.scale_squared.round_sqrt()).unwrap_or(f64::INFINITY))),
        }
    }
}

/// The rounded center and exact boundary clearance selected by one directed
/// continuous-distance search. Test builds additionally retain the exact
/// witness and enclosure inputs used to prove the rounded result.
#[derive(Debug)]
struct DirectedCandidate {
    center: Point,
    center_squared: Rational,
    #[cfg(test)]
    proof: DirectedCandidateProof,
}

#[cfg(test)]
#[derive(Debug)]
struct DirectedCandidateProof {
    witness: ExactPoint,
    witness_squared: Rational,
    certified_squared: Rational,
    upper: Rational,
}

#[derive(Debug)]
pub(crate) struct DirectedOptimizationResult {
    pub(in crate::geometry) center: Point,
    pub(in crate::geometry) center_squared: Rational,
    // The directed search owns both prepared views so the rounded-center
    // recomputation and certified polygon emission cannot rebuild a different
    // partition or boundary.
    source: PreparedSupport,
    boundary: PreparedSupport,
}

#[cfg(test)]
mod tests;
