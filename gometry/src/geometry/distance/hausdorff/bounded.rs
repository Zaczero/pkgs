//! Continuous planar Hausdorff distance with a certified error bound.

#[cfg(test)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::{ExactMetricSpan, ExactMetricSupport, ExactSourceInventory};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational as Rational};
use crate::geometry::{
    CoordSeq, DdInterval, ExpansionBudget, LineSeq, PackedLineColumnView, Point, Segment, Shape, XY,
};
use crate::{try_push_vec, try_reserve_vec};

const NOMINATED_SITE_COUNT: usize = 4;

#[cfg(test)]
thread_local! {
    static LINE_FILTER_ENABLED: Cell<bool> = const { Cell::new(true) };
    static EXACT_SPAN_EVALUATIONS: Cell<usize> = const { Cell::new(0) };
    static REFINEMENT_WORK: Cell<usize> = const { Cell::new(0) };
    static REFINEMENT_WORK_LIMIT: Cell<Option<usize>> = const { Cell::new(None) };
}

#[cfg(test)]
fn line_filter_enabled() -> bool {
    LINE_FILTER_ENABLED.with(Cell::get)
}

#[cfg(not(test))]
const fn line_filter_enabled() -> bool {
    true
}

#[cfg(test)]
struct LineFilterGuard(bool);

#[cfg(test)]
impl Drop for LineFilterGuard {
    fn drop(&mut self) {
        LINE_FILTER_ENABLED.with(|enabled| enabled.set(self.0));
    }
}

#[cfg(test)]
fn set_line_filter_enabled(enabled: bool) -> LineFilterGuard {
    LineFilterGuard(LINE_FILTER_ENABLED.with(|current| {
        let previous = current.get();
        current.set(enabled);
        previous
    }))
}

#[cfg(test)]
fn reset_exact_span_evaluations() {
    EXACT_SPAN_EVALUATIONS.with(|count| count.set(0));
}

#[cfg(test)]
fn exact_span_evaluations() -> usize {
    EXACT_SPAN_EVALUATIONS.with(Cell::get)
}

#[cfg(test)]
fn count_exact_span_evaluation() {
    EXACT_SPAN_EVALUATIONS.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(test)]
struct RefinementWorkGuard {
    previous_count: usize,
    previous_limit: Option<usize>,
}

#[cfg(test)]
impl Drop for RefinementWorkGuard {
    fn drop(&mut self) {
        REFINEMENT_WORK.with(|count| count.set(self.previous_count));
        REFINEMENT_WORK_LIMIT.with(|limit| limit.set(self.previous_limit));
    }
}

#[cfg(test)]
fn limit_refinement_work(limit: usize) -> RefinementWorkGuard {
    let previous_count = REFINEMENT_WORK.with(|count| {
        let previous = count.get();
        count.set(0);
        previous
    });
    let previous_limit = REFINEMENT_WORK_LIMIT.with(|current| {
        let previous = current.get();
        current.set(Some(limit));
        previous
    });
    RefinementWorkGuard {
        previous_count,
        previous_limit,
    }
}

#[cfg(test)]
fn refinement_work() -> usize {
    REFINEMENT_WORK.with(Cell::get)
}

#[cfg(test)]
fn count_refinement_work() -> Result<()> {
    let count = REFINEMENT_WORK.with(|current| {
        let next = current.get().saturating_add(1);
        current.set(next);
        next
    });
    if let Some(limit) = REFINEMENT_WORK_LIMIT.with(Cell::get)
        && count > limit
    {
        return Err(
            crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "hausdorff_distance",
                parameter: "refinement_work",
                produced: count,
                limit,
            }
            .into(),
        );
    }
    Ok(())
}

struct Accuracy {
    error: ErrorBound,
    scale_squared: Rational,
}

enum ErrorBound {
    Absolute(Rational),
    Relative(Rational),
}

impl Accuracy {
    const DEFAULT_RELATIVE_ERROR: f64 = 1e-9;

    fn new(max_error: Option<f64>, scale: f64) -> Result<Self> {
        let error = match max_error {
            Some(value) if value.is_finite() && value > 0.0 => {
                ErrorBound::Absolute(Rational::from_f64(value)?)
            },
            Some(_) => {
                return Err(crate::geometry::GeometryErrorKind::message(
                    "max_error must be positive and finite",
                ));
            },
            None => ErrorBound::Relative(Rational::from_f64(Self::DEFAULT_RELATIVE_ERROR)?),
        };
        if !(scale.is_finite() && scale > 0.0) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "Hausdorff unit scale must be positive and finite",
            ));
        }
        Ok(Self {
            error,
            scale_squared: Rational::from_f64(scale)?.try_square()?,
        })
    }

    fn absolute_error(&self, lower_squared: &Rational, candidate: f64) -> Result<Rational> {
        match &self.error {
            ErrorBound::Absolute(error) => error.try_clone(),
            ErrorBound::Relative(relative) => {
                // Derive the allowance from a proven lower bracket L, not
                // from the returned approximation: L <= H makes r*L <= r*H.
                // candidate_certified then proves both sides against r*L,
                // which is therefore also a relative-error certificate.
                let mut lower = Rational::from_f64(candidate)?;
                if lower.try_square()?.try_cmp(lower_squared)? == Ordering::Greater {
                    let predecessor = candidate.next_down();
                    lower = if predecessor > 0.0 {
                        Rational::from_f64(predecessor)?
                    } else {
                        // The predecessor of the minimum subnormal is zero.
                        // Correct rounding to that subnormal proves that half
                        // of it is still a lower bound on the exact root.
                        lower.try_product(&Rational::from_f64(0.5)?)?
                    };
                }
                lower.try_product(relative)
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct FloatBox {
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
}

impl FloatBox {
    fn from_point(point: &ExactPoint) -> Result<Self> {
        let (min_x, max_x) = point.x.outward_bounds()?;
        let (min_y, max_y) = point.y.outward_bounds()?;
        if !(min_x.is_finite() && max_x.is_finite() && min_y.is_finite() && max_y.is_finite()) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "selected support has no finite conservative index enclosure",
            ));
        }
        Ok(Self {
            min_x,
            max_x,
            min_y,
            max_y,
        })
    }

    fn include(self, other: Self) -> Self {
        Self {
            min_x: self.min_x.min(other.min_x),
            max_x: self.max_x.max(other.max_x),
            min_y: self.min_y.min(other.min_y),
            max_y: self.max_y.max(other.max_y),
        }
    }

    fn corners(self) -> [(f64, f64); 4] {
        [
            (self.min_x, self.min_y),
            (self.min_x, self.max_y),
            (self.max_x, self.min_y),
            (self.max_x, self.max_y),
        ]
    }
}

#[derive(Debug)]
struct SupportSpan {
    source: Segment,
    carrier: ExactCarrier,
    t_lo: Rational,
    t_hi: Rational,
    start: ExactPoint,
    bounds: FloatBox,
}

#[derive(Debug)]
struct ExactCarrier {
    ax: Rational,
    ay: Rational,
    dx: Rational,
    dy: Rational,
    norm_squared: Rational,
}

impl ExactCarrier {
    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            ax: self.ax.try_clone()?,
            ay: self.ay.try_clone()?,
            dx: self.dx.try_clone()?,
            dy: self.dy.try_clone()?,
            norm_squared: self.norm_squared.try_clone()?,
        })
    }
}

/// An exact ratio kept unreduced across transient distance calculations.
///
/// The denominator is always positive. Deferring normalization until a value
/// crosses back into the certificate path avoids paying for greatest-common-
/// divisor work on every losing nearest-support candidate.
#[derive(Debug)]
struct UnreducedRatio {
    numerator: ExactDyadic,
    denominator: ExactDyadic,
}

impl UnreducedRatio {
    fn zero() -> Self {
        Self {
            numerator: ExactDyadic::zero(),
            denominator: ExactDyadic::one(),
        }
    }

    fn new(numerator: ExactDyadic, denominator: ExactDyadic) -> Result<Self> {
        if denominator.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        if numerator.is_zero() {
            return Ok(Self::zero());
        }
        let (numerator, denominator) = if denominator.sign() == ExactSign::Negative {
            (numerator.neg(), denominator.neg())
        } else {
            (numerator, denominator)
        };
        Ok(Self {
            numerator,
            denominator,
        })
    }

    fn from_rational(value: &Rational) -> Result<Self> {
        let (numerator, denominator) = value.parts();
        Self::new(numerator.try_clone()?, denominator.try_clone()?)
    }

    fn try_difference(left: &Rational, right: &Rational) -> Result<Self> {
        Self::from_rational(left)?.try_subtract(&Self::from_rational(right)?)
    }

    fn is_zero(&self) -> bool {
        self.numerator.is_zero()
    }

    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            numerator: self.numerator.try_clone()?,
            denominator: self.denominator.try_clone()?,
        })
    }

    fn to_rational(&self) -> Result<Rational> {
        Rational::new(self.numerator.try_clone()?, self.denominator.try_clone()?)
    }

    fn try_add(&self, other: &Self) -> Result<Self> {
        if self.is_zero() {
            return other.try_clone();
        }
        if other.is_zero() {
            return self.try_clone();
        }
        if self.denominator == other.denominator {
            return Self::new(
                ExactDyadic::try_add_refs(&self.numerator, &other.numerator)?,
                self.denominator.try_clone()?,
            );
        }
        if self.denominator.is_one() {
            let scaled = self.numerator.try_product(&other.denominator)?;
            return Self::new(
                ExactDyadic::try_add_refs(&scaled, &other.numerator)?,
                other.denominator.try_clone()?,
            );
        }
        if other.denominator.is_one() {
            let scaled = other.numerator.try_product(&self.denominator)?;
            return Self::new(
                ExactDyadic::try_add_refs(&self.numerator, &scaled)?,
                self.denominator.try_clone()?,
            );
        }
        Self::new(
            self.numerator
                .try_product(&other.denominator)?
                .try_add(other.numerator.try_product(&self.denominator)?)?,
            self.denominator.try_product(&other.denominator)?,
        )
    }

    fn try_subtract(&self, other: &Self) -> Result<Self> {
        if self.is_zero() {
            let mut result = other.try_clone()?;
            if !result.is_zero() {
                result.numerator = result.numerator.neg();
            }
            return Ok(result);
        }
        if other.is_zero() {
            return self.try_clone();
        }
        if self.denominator == other.denominator {
            return Self::new(
                ExactDyadic::try_subtract_refs(&self.numerator, &other.numerator)?,
                self.denominator.try_clone()?,
            );
        }
        if self.denominator.is_one() {
            let scaled = self.numerator.try_product(&other.denominator)?;
            return Self::new(
                ExactDyadic::try_subtract_refs(&scaled, &other.numerator)?,
                other.denominator.try_clone()?,
            );
        }
        if other.denominator.is_one() {
            let scaled = other.numerator.try_product(&self.denominator)?;
            return Self::new(
                ExactDyadic::try_subtract_refs(&self.numerator, &scaled)?,
                self.denominator.try_clone()?,
            );
        }
        Self::new(
            self.numerator
                .try_product(&other.denominator)?
                .try_subtract(other.numerator.try_product(&self.denominator)?)?,
            self.denominator.try_product(&other.denominator)?,
        )
    }

    fn try_product(&self, other: &Self) -> Result<Self> {
        if self.is_zero() || other.is_zero() {
            return Ok(Self::zero());
        }
        let denominator = if self.denominator.is_one() {
            other.denominator.try_clone()?
        } else if other.denominator.is_one() {
            self.denominator.try_clone()?
        } else {
            self.denominator.try_product(&other.denominator)?
        };
        Self::new(self.numerator.try_product(&other.numerator)?, denominator)
    }

    fn try_square(&self) -> Result<Self> {
        if self.is_zero() {
            return Ok(Self::zero());
        }
        let denominator = if self.denominator.is_one() {
            ExactDyadic::one()
        } else {
            self.denominator.try_product(&self.denominator)?
        };
        Self::new(self.numerator.try_product(&self.numerator)?, denominator)
    }

    fn try_quotient(&self, other: &Self) -> Result<Self> {
        if other.is_zero() {
            return Err(crate::geometry::GeometryErrorKind::ExactRatioZeroDenominator.into());
        }
        Self::new(
            self.numerator.try_product(&other.denominator)?,
            self.denominator.try_product(&other.numerator)?,
        )
    }

    fn try_cmp(&self, other: &Self) -> Result<Ordering> {
        if self.denominator == other.denominator {
            return Ok(self.numerator.cmp(&other.numerator));
        }
        if self.denominator.is_one() {
            return Ok(self
                .numerator
                .try_product(&other.denominator)?
                .cmp(&other.numerator));
        }
        if other.denominator.is_one() {
            return Ok(self
                .numerator
                .cmp(&other.numerator.try_product(&self.denominator)?));
        }
        Ok(self
            .numerator
            .try_product(&other.denominator)?
            .cmp(&other.numerator.try_product(&self.denominator)?))
    }

    fn outward_upper(&self) -> Result<f64> {
        crate::geometry::tessellation::exact::outward_ratio_bounds(
            &self.numerator,
            &self.denominator,
        )
        .map(|(_, upper)| upper)
    }

    #[cfg(test)]
    fn try_cmp_rational(&self, other: &Rational) -> Result<Ordering> {
        Self::from_rational(other).and_then(|other| self.try_cmp(&other))
    }
}

#[derive(Debug)]
struct BoundaryEdge {
    start: ExactPoint,
    end: ExactPoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrimitiveId {
    Span(usize),
    Point(usize),
}

#[derive(Clone, Copy, Debug)]
struct IndexItem {
    primitive: PrimitiveId,
    bounds: FloatBox,
}

#[derive(Clone, Copy, Debug)]
struct IndexNode {
    bounds: FloatBox,
    left: Option<usize>,
    right: Option<usize>,
    start: usize,
    end: usize,
}

#[derive(Debug, Default)]
struct SupportIndex {
    items: Vec<IndexItem>,
    order: Vec<usize>,
    nodes: Vec<IndexNode>,
    root: Option<usize>,
}

impl SupportIndex {
    fn build(spans: &[SupportSpan], points: &[ExactPoint]) -> Result<Self> {
        let item_count = spans
            .len()
            .checked_add(points.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut items = Vec::new();
        try_reserve_vec(&mut items, item_count)?;
        for (index, span) in spans.iter().enumerate() {
            try_push_vec(&mut items, IndexItem {
                primitive: PrimitiveId::Span(index),
                bounds: span.bounds,
            })?;
        }
        for (index, point) in points.iter().enumerate() {
            try_push_vec(&mut items, IndexItem {
                primitive: PrimitiveId::Point(index),
                bounds: FloatBox::from_point(point)?,
            })?;
        }
        let mut order = Vec::new();
        try_reserve_vec(&mut order, items.len())?;
        for index in 0..items.len() {
            try_push_vec(&mut order, index)?;
        }
        let node_capacity = items
            .len()
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut index = Self {
            items,
            order,
            nodes: Vec::new(),
            root: None,
        };
        try_reserve_vec(&mut index.nodes, node_capacity)?;
        if !index.items.is_empty() {
            index.root = Some(index.build_range(0, index.order.len())?);
        }

        Ok(index)
    }

    fn build_range(&mut self, start: usize, end: usize) -> Result<usize> {
        let bounds = self.range_bounds(start, end);
        if end.saturating_sub(start) <= 4 {
            let node = IndexNode {
                bounds,
                left: None,
                right: None,
                start,
                end,
            };
            let index = self.nodes.len();
            try_push_vec(&mut self.nodes, node)?;
            return Ok(index);
        }

        let split_x = (bounds.max_x - bounds.min_x) >= (bounds.max_y - bounds.min_y);
        self.order[start..end].sort_unstable_by(|left, right| {
            let left_bounds = self.items[*left].bounds;
            let right_bounds = self.items[*right].bounds;
            let left_key = if split_x {
                f64::midpoint(left_bounds.min_x, left_bounds.max_x)
            } else {
                f64::midpoint(left_bounds.min_y, left_bounds.max_y)
            };
            let right_key = if split_x {
                f64::midpoint(right_bounds.min_x, right_bounds.max_x)
            } else {
                f64::midpoint(right_bounds.min_y, right_bounds.max_y)
            };
            left_key.total_cmp(&right_key).then_with(|| left.cmp(right))
        });
        let middle = start + (end - start) / 2;
        let left = self.build_range(start, middle)?;
        let right = self.build_range(middle, end)?;
        let index = self.nodes.len();
        try_push_vec(&mut self.nodes, IndexNode {
            bounds,
            left: Some(left),
            right: Some(right),
            start: 0,
            end: 0,
        })?;
        Ok(index)
    }

    fn range_bounds(&self, start: usize, end: usize) -> FloatBox {
        let first = self.items[self.order[start]].bounds;
        self.order[start + 1..end]
            .iter()
            .fold(first, |bounds, index| {
                bounds.include(self.items[*index].bounds)
            })
    }

    fn nearest(&self, support: &PreparedSupport, point: &ExactPoint) -> Result<UnreducedRatio> {
        let Some(root) = self.root else {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        };
        let mut best = None;
        let point_bounds = FloatBox::from_point(point)?;
        let mut best_upper = f64::INFINITY;
        self.nearest_node(
            root,
            support,
            point,
            point_bounds,
            &mut best,
            &mut best_upper,
        )?;
        best.ok_or_else(|| {
            crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            )
            .into()
        })
    }

    fn nearest_node(
        &self,
        node_index: usize,
        support: &PreparedSupport,
        point: &ExactPoint,
        point_bounds: FloatBox,

        best: &mut Option<UnreducedRatio>,
        best_upper: &mut f64,
    ) -> Result<()> {
        let node = self.nodes[node_index];
        if box_distance_lower(point_bounds, node.bounds) > *best_upper {
            return Ok(());
        }
        if let (Some(left), Some(right)) = (node.left, node.right) {
            let (first, second) = {
                let left_lower = box_distance_lower(point_bounds, self.nodes[left].bounds);
                let right_lower = box_distance_lower(point_bounds, self.nodes[right].bounds);
                if left_lower <= right_lower {
                    (left, right)
                } else {
                    (right, left)
                }
            };
            self.nearest_node(first, support, point, point_bounds, best, best_upper)?;
            self.nearest_node(second, support, point, point_bounds, best, best_upper)?;
            return Ok(());
        }

        for order_index in &self.order[node.start..node.end] {
            let item = self.items[*order_index];

            if box_distance_lower(point_bounds, item.bounds) > *best_upper {
                continue;
            }

            if line_filter_enabled()
                && let PrimitiveId::Span(index) = item.primitive
                && supporting_line_lower_squared(point_bounds, &support.spans[index]) > *best_upper
            {
                continue;
            }

            let distance = support.primitive_distance_squared(item.primitive, point)?;
            let replace = match best {
                None => true,
                Some(current) => distance.try_cmp(current)? == Ordering::Less,
            };
            if replace {
                *best_upper = nonnegative_pair_upper_bound(&distance)?;
                *best = Some(distance);
            }
        }
        Ok(())
    }

    fn nominate(&self, point: &ExactPoint) -> Result<Vec<PrimitiveId>> {
        let mut nominated = Vec::new();
        try_reserve_vec(&mut nominated, NOMINATED_SITE_COUNT)?;
        if let Some(root) = self.root {
            self.nominate_node(root, point, &mut nominated)?;
        }
        Ok(nominated)
    }

    fn nominate_node(
        &self,
        node_index: usize,
        point: &ExactPoint,

        nominated: &mut Vec<PrimitiveId>,
    ) -> Result<()> {
        if nominated.len() >= NOMINATED_SITE_COUNT {
            return Ok(());
        }

        let node = self.nodes[node_index];
        if let (Some(left), Some(right)) = (node.left, node.right) {
            let left_lower = aabb_lower_squared(point, self.nodes[left].bounds)?;
            let right_lower = aabb_lower_squared(point, self.nodes[right].bounds)?;
            let (first, second) = if left_lower.try_cmp(&right_lower)? != Ordering::Greater {
                (left, right)
            } else {
                (right, left)
            };
            self.nominate_node(first, point, nominated)?;
            self.nominate_node(second, point, nominated)?;
        } else {
            for order_index in &self.order[node.start..node.end] {
                if nominated.len() >= NOMINATED_SITE_COUNT {
                    break;
                }
                try_push_vec(&mut *nominated, self.items[*order_index].primitive)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct PreparedSupport {
    spans: Vec<SupportSpan>,
    points: Vec<ExactPoint>,
    faces: Vec<Vec<Vec<ExactPoint>>>,
    boundary_spans: Option<Vec<bool>>,
    boundary_points: Option<Vec<bool>>,
    boundary_edges: Vec<BoundaryEdge>,
    area_bounds: Option<FloatBox>,
    index: SupportIndex,
}

impl PreparedSupport {
    pub(in crate::geometry) fn from_shape(
        shape: &Shape,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        if shape.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        }
        if !shape.has_area_parts() {
            let mut spans = Vec::new();
            let mut points = Vec::new();
            try_reserve_vec(&mut spans, shape.segment_count())?;
            shape.try_for_each_vertex_pair(|start, end| -> crate::error::Result<()> {
                push_raw_segment(
                    Segment {
                        start: start.xy(),
                        end: end.xy(),
                    },
                    &mut spans,
                    &mut points,
                )
            })?;
            for point in shape.point_only_points()? {
                try_push_vec(&mut points, ExactPoint::from_xy(point.xy())?)?;
            }
            return Self::from_parts(spans, points, Vec::new());
        }
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let partition = inventory.partition(budget)?;
        let support = inventory.metric_support(&partition)?;
        Self::from_metric_support(support)
    }

    pub(in crate::geometry) fn from_shape_with_boundary(
        shape: &Shape,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        if !shape.has_area_parts() {
            return Self::from_shape(shape, budget);
        }
        if shape.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        }
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let partition = inventory.partition(budget)?;
        let support = inventory.metric_support_with_boundary(&partition)?;
        Self::from_metric_support(support)
    }

    fn from_line_columns(xs: &[f64], ys: &[f64]) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        let mut spans = Vec::new();
        let mut points = Vec::new();
        try_reserve_vec(&mut spans, xs.len().saturating_sub(1))?;
        if xs.len() == 1 {
            try_push_vec(&mut points, exact_point_xy(xs[0], ys[0])?)?;
        }
        for index in 1..xs.len() {
            push_raw_segment(
                Segment {
                    start: XY::new(xs[index - 1], ys[index - 1]),
                    end: XY::new(xs[index], ys[index]),
                },
                &mut spans,
                &mut points,
            )?;
        }
        Self::from_parts(spans, points, Vec::new())
    }

    fn from_metric_support(support: ExactMetricSupport) -> Result<Self> {
        let ExactMetricSupport {
            spans: metric_spans,
            points: metric_points,
            faces: metric_faces,
            boundary_spans: metric_boundary_spans,
            boundary_points: metric_boundary_points,
        } = support;
        if metric_boundary_spans
            .as_ref()
            .is_some_and(|values| values.len() != metric_spans.len())
            || metric_boundary_points
                .as_ref()
                .is_some_and(|values| values.len() != metric_points.len())
        {
            return Err(crate::geometry::GeometryErrorKind::message(
                "exact metric boundary incidence is not aligned with support",
            ));
        }
        let mut spans = Vec::new();
        try_reserve_vec(&mut spans, metric_spans.len())?;
        let mut boundary_spans = metric_boundary_spans.as_ref().map(|_| Vec::new());
        if let Some(boundary_spans) = boundary_spans.as_mut() {
            try_reserve_vec(boundary_spans, metric_spans.len())?;
        }
        for (index, span) in metric_spans.into_iter().enumerate() {
            try_push_vec(&mut spans, support_span(span)?)?;
            if let Some(boundary_spans) = boundary_spans.as_mut() {
                try_push_vec(
                    boundary_spans,
                    metric_boundary_spans.as_ref().unwrap()[index],
                )?;
            }
        }

        let mut points = Vec::new();
        try_reserve_vec(&mut points, metric_points.len())?;
        let mut boundary_points = metric_boundary_points.as_ref().map(|_| Vec::new());
        if let Some(boundary_points) = boundary_points.as_mut() {
            try_reserve_vec(boundary_points, metric_points.len())?;
        }
        for (index, point) in metric_points.into_iter().enumerate() {
            try_push_vec(
                &mut points,
                ExactPoint::from_homogeneous(point.homogeneous)?,
            )?;
            if let Some(boundary_points) = boundary_points.as_mut() {
                try_push_vec(
                    boundary_points,
                    metric_boundary_points.as_ref().unwrap()[index],
                )?;
            }
        }

        let mut faces = Vec::new();
        try_reserve_vec(&mut faces, metric_faces.len())?;
        for face in metric_faces {
            let mut cycles = Vec::new();
            try_reserve_vec(&mut cycles, face.len())?;
            for cycle in face {
                let mut points = Vec::new();
                try_reserve_vec(&mut points, cycle.len())?;
                for point in cycle {
                    try_push_vec(
                        &mut points,
                        ExactPoint::from_homogeneous(point.homogeneous)?,
                    )?;
                }
                try_push_vec(&mut cycles, points)?;
            }
            try_push_vec(&mut faces, cycles)?;
        }

        let mut prepared = Self::from_parts(spans, points, faces)?;
        prepared.boundary_spans = boundary_spans;
        prepared.boundary_points = boundary_points;
        Ok(prepared)
    }

    fn from_parts(
        spans: Vec<SupportSpan>,
        points: Vec<ExactPoint>,
        faces: Vec<Vec<Vec<ExactPoint>>>,
    ) -> Result<Self> {
        let mut boundary_edges = Vec::new();
        for face in &faces {
            for cycle in face {
                if cycle.len() < 2 {
                    continue;
                }
                for index in 0..cycle.len() {
                    try_push_vec(&mut boundary_edges, BoundaryEdge {
                        start: cycle[index].try_clone()?,
                        end: cycle[(index + 1) % cycle.len()].try_clone()?,
                    })?;
                }
            }
        }

        let mut area_bounds: Option<FloatBox> = None;
        for edge in &boundary_edges {
            let edge_bounds =
                FloatBox::from_point(&edge.start)?.include(FloatBox::from_point(&edge.end)?);
            area_bounds = Some(match area_bounds {
                None => edge_bounds,
                Some(current) => current.include(edge_bounds),
            });
        }

        if spans.is_empty() && points.is_empty() && faces.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        }

        let index = SupportIndex::build(&spans, &points)?;
        Ok(Self {
            index,
            spans,
            points,
            faces,
            boundary_spans: None,
            boundary_points: None,
            boundary_edges,
            area_bounds,
        })
    }

    /// Clone the exact boundary selected by the partition's face walks without
    /// reconstructing it from rounded source coordinates. The aligned flags
    /// already exclude shared/internal face edges and lower-dimensional
    /// support hidden by a positive-area face.
    pub(in crate::geometry) fn boundary_clone(&self) -> Result<Self> {
        let mut spans = Vec::new();
        try_reserve_vec(
            &mut spans,
            self.boundary_spans
                .as_ref()
                .map_or(self.spans.len(), |flags| {
                    flags.iter().filter(|&&selected| selected).count()
                }),
        )?;
        for (index, span) in self.spans.iter().enumerate() {
            let selected = self
                .boundary_spans
                .as_ref()
                .map_or(true, |flags| flags[index]);
            if selected {
                try_push_vec(&mut spans, span.try_clone()?)?;
            }
        }

        let mut points = Vec::new();
        try_reserve_vec(
            &mut points,
            self.boundary_points
                .as_ref()
                .map_or(self.points.len(), |flags| {
                    flags.iter().filter(|&&selected| selected).count()
                }),
        )?;
        for (index, point) in self.points.iter().enumerate() {
            let selected = self
                .boundary_points
                .as_ref()
                .map_or(true, |flags| flags[index]);
            if selected {
                try_push_vec(&mut points, point.try_clone()?)?;
            }
        }
        Self::from_parts(spans, points, Vec::new())
    }

    fn initial_patches(&self) -> Result<Vec<SourcePatch>> {
        let mut patches = Vec::new();
        let capacity = self
            .spans
            .len()
            .checked_add(usize::from(!self.faces.is_empty()))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        try_reserve_vec(&mut patches, capacity)?;
        for (index, span) in self.spans.iter().enumerate() {
            try_push_vec(&mut patches, SourcePatch::Span {
                span: index,
                lo: span.t_lo.try_clone()?,
                hi: span.t_hi.try_clone()?,
            })?;
        }
        if let Some(bounds) = self.area_bounds
            && !self.faces.is_empty()
        {
            try_push_vec(&mut patches, SourcePatch::Box(bounds))?;
        }
        Ok(patches)
    }

    fn inradius_initial_patches(&self) -> Result<Vec<SourcePatch>> {
        if self.has_positive_area()? {
            let mut patches = Vec::new();
            try_reserve_vec(&mut patches, 1)?;
            if let Some(bounds) = self.area_bounds {
                try_push_vec(&mut patches, SourcePatch::Box(bounds))?;
            }
            return Ok(patches);
        }

        let mut patches = self.initial_patches()?;
        try_reserve_vec(&mut patches, self.points.len())?;
        for index in 0..self.points.len() {
            try_push_vec(&mut patches, SourcePatch::Point(index))?;
        }
        Ok(patches)
    }

    fn contains_area(&self, point: &ExactPoint) -> Result<bool> {
        for face in &self.faces {
            let mut inside = false;
            for cycle in face {
                if point_in_cycle(point, cycle)? {
                    return Ok(true);
                }
                if cycle_crosses_ray(point, cycle)? {
                    inside = !inside;
                }
            }
            if inside {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn covers_support(&self, point: &ExactPoint) -> Result<bool> {
        if self.contains_area(point)? {
            return Ok(true);
        }
        if self.index.root.is_none() {
            return Ok(false);
        }
        Ok(self.index.nearest(self, point)?.is_zero())
    }

    pub(in crate::geometry) fn candidate_is_contained(
        &self,
        boundary: &Self,
        center: Point,
        candidate: &[Point],
        counter_clockwise: bool,
    ) -> Result<bool> {
        let center = ExactPoint::from_xy(center.xy())?;
        if !self.contains_area(&center)? {
            return Ok(false);
        }
        for &point in candidate {
            let point = ExactPoint::from_xy(point.xy())?;
            if !self.covers_support(&point)? {
                return Ok(false);
            }
        }
        // The selected boundary spans include retained hole cycles. Testing
        // the whole segment, rather than only its endpoints, rejects both an
        // inward chord crossing and a hole wholly enclosed by the candidate.
        for span in &boundary.spans {
            let start = span_point(span, &span.t_lo)?;
            let end = span_point(span, &span.t_hi)?;
            if exact_segment_enters_candidate(&start, &end, candidate, counter_clockwise)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn has_positive_area(&self) -> Result<bool> {
        for face in &self.faces {
            for cycle in face {
                if cycle.len() < 3 {
                    continue;
                }
                let mut area = Rational::zero();
                for index in 0..cycle.len() {
                    let left = &cycle[index];
                    let right = &cycle[(index + 1) % cycle.len()];
                    let cross = left
                        .x
                        .try_product(&right.y)?
                        .try_subtract(&left.y.try_product(&right.x)?)?;
                    area = area.try_add(&cross)?;
                }
                if !area.is_zero() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn first_boundary_point(&self) -> Result<Option<ExactPoint>> {
        if let Some(span) = self.spans.first() {
            return Ok(Some(span.start.try_clone()?));
        }
        if let Some(point) = self.points.first() {
            return Ok(Some(point.try_clone()?));
        }
        Ok(self
            .boundary_edges
            .first()
            .map(|edge| edge.start.try_clone())
            .transpose()?)
    }

    fn source_box_witness(
        &self,
        bounds: FloatBox,
        center_first: bool,
    ) -> Result<Option<ExactPoint>> {
        if center_first {
            let min_x = Rational::from_f64(bounds.min_x)?;
            let max_x = Rational::from_f64(bounds.max_x)?;
            let min_y = Rational::from_f64(bounds.min_y)?;
            let max_y = Rational::from_f64(bounds.max_y)?;
            let center = ExactPoint {
                x: min_x.try_midpoint(&max_x)?,
                y: min_y.try_midpoint(&max_y)?,
            };
            if self.contains_area(&center)? {
                return Ok(Some(center));
            }
        }
        for (x, y) in bounds.corners() {
            let point = exact_point_xy(x, y)?;
            if self.contains_area(&point)? {
                return Ok(Some(point));
            }
        }
        for edge in &self.boundary_edges {
            if let Some(point) = segment_box_intersection(edge, bounds)? {
                return Ok(Some(point));
            }
        }
        Ok(None)
    }

    fn primitive_distance_squared(
        &self,
        primitive: PrimitiveId,
        point: &ExactPoint,
    ) -> Result<UnreducedRatio> {
        match primitive {
            PrimitiveId::Span(index) => {
                #[cfg(test)]
                count_exact_span_evaluation();
                point_span_distance_squared(point, &self.spans[index])
            },
            PrimitiveId::Point(index) => {
                let dx = UnreducedRatio::try_difference(&point.x, &self.points[index].x)?;
                let dy = UnreducedRatio::try_difference(&point.y, &self.points[index].y)?;
                dx.try_square()?.try_add(&dy.try_square()?)
            },
        }
    }

    fn distance_squared(&self, point: &ExactPoint) -> Result<Rational> {
        let result = if self.contains_area(point)? {
            Ok(Rational::zero())
        } else {
            self.index
                .nearest(self, point)
                .and_then(|distance| distance.to_rational())
        };

        result
    }
}

impl SupportSpan {
    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            source: self.source,
            carrier: self.carrier.try_clone()?,
            t_lo: self.t_lo.try_clone()?,
            t_hi: self.t_hi.try_clone()?,
            start: self.start.try_clone()?,
            bounds: self.bounds,
        })
    }
}

fn push_raw_segment(
    source: Segment,
    spans: &mut Vec<SupportSpan>,
    points: &mut Vec<ExactPoint>,
) -> Result<()> {
    if source.start == source.end {
        return try_push_vec(points, ExactPoint::from_xy(source.start)?);
    }
    try_push_vec(spans, SupportSpan {
        source,
        start: ExactPoint::from_xy(source.start)?,
        t_lo: Rational::zero(),
        t_hi: Rational::one(),
        bounds: FloatBox {
            min_x: source.start.x.min(source.end.x),
            max_x: source.start.x.max(source.end.x),
            min_y: source.start.y.min(source.end.y),
            max_y: source.start.y.max(source.end.y),
        },
        carrier: ExactCarrier::from_segment(source)?,
    })
}

fn support_span(span: ExactMetricSpan) -> Result<SupportSpan> {
    let start = ExactPoint::from_homogeneous(span.start.homogeneous)?;
    let end = ExactPoint::from_homogeneous(span.end.homogeneous)?;
    let mut t_lo = Rational::new(
        span.start_parameter[0].try_clone()?,
        span.start_parameter[1].try_clone()?,
    )?;
    let mut t_hi = Rational::new(
        span.end_parameter[0].try_clone()?,
        span.end_parameter[1].try_clone()?,
    )?;
    if t_lo.try_cmp(&t_hi)? == Ordering::Greater {
        std::mem::swap(&mut t_lo, &mut t_hi);
    }
    let bounds = FloatBox::from_point(&start)?.include(FloatBox::from_point(&end)?);
    Ok(SupportSpan {
        source: span.source,
        carrier: ExactCarrier::from_segment(span.source)?,
        t_lo,
        t_hi,
        start,
        bounds,
    })
}

fn exact_point_xy(x: f64, y: f64) -> Result<ExactPoint> {
    Ok(ExactPoint::from_xy(XY::new(x, y))?)
}

impl ExactCarrier {
    fn from_segment(source: Segment) -> Result<Self> {
        let ax = Rational::from_f64(source.start.x)?;
        let ay = Rational::from_f64(source.start.y)?;
        let dx = Rational::from_f64(source.end.x)?.try_subtract(&ax)?;
        let dy = Rational::from_f64(source.end.y)?.try_subtract(&ay)?;
        let norm_squared = dx.try_square()?.try_add(&dy.try_square()?)?;
        Ok(Self {
            ax,
            ay,
            dx,
            dy,
            norm_squared,
        })
    }
}

fn span_point(span: &SupportSpan, parameter: &Rational) -> Result<ExactPoint> {
    let carrier = &span.carrier;
    let x = carrier.ax.try_add(&carrier.dx.try_product(parameter)?)?;
    let y = carrier.ay.try_add(&carrier.dy.try_product(parameter)?)?;
    Ok(ExactPoint { x, y })
}

fn span_projection_parameter(point: &ExactPoint, span: &SupportSpan) -> Result<Rational> {
    let carrier = &span.carrier;
    let px = point.x.try_subtract(&carrier.ax)?;
    let py = point.y.try_subtract(&carrier.ay)?;
    let numerator = px
        .try_product(&carrier.dx)?
        .try_add(&py.try_product(&carrier.dy)?)?;
    if carrier.norm_squared.is_zero() {
        return Ok(Rational::zero());
    }
    numerator.try_quotient(&carrier.norm_squared)
}

fn span_coordinate_difference(
    point: &Rational,
    origin: &Rational,
    direction: &Rational,
    parameter: &Rational,
) -> Result<UnreducedRatio> {
    let point = UnreducedRatio::from_rational(point)?;
    let origin = UnreducedRatio::from_rational(origin)?;
    let direction = UnreducedRatio::from_rational(direction)?;
    let parameter = UnreducedRatio::from_rational(parameter)?;
    origin
        .try_add(&direction.try_product(&parameter)?)?
        .try_subtract(&point)
}

fn point_span_distance_squared(point: &ExactPoint, span: &SupportSpan) -> Result<UnreducedRatio> {
    let carrier = &span.carrier;
    let px = UnreducedRatio::try_difference(&point.x, &carrier.ax)?;
    let py = UnreducedRatio::try_difference(&point.y, &carrier.ay)?;
    let dx = UnreducedRatio::from_rational(&carrier.dx)?;
    let dy = UnreducedRatio::from_rational(&carrier.dy)?;
    let norm_squared = UnreducedRatio::from_rational(&carrier.norm_squared)?;
    let dot = px.try_product(&dx)?.try_add(&py.try_product(&dy)?)?;
    let low = UnreducedRatio::from_rational(&span.t_lo)?.try_product(&norm_squared)?;
    let high = UnreducedRatio::from_rational(&span.t_hi)?.try_product(&norm_squared)?;
    let parameter = if carrier.norm_squared.is_zero() || dot.try_cmp(&low)? == Ordering::Less {
        &span.t_lo
    } else if dot.try_cmp(&high)? == Ordering::Greater {
        &span.t_hi
    } else {
        let cross = px.try_product(&dy)?.try_subtract(&py.try_product(&dx)?)?;
        return cross.try_square()?.try_quotient(&norm_squared);
    };
    let dx = span_coordinate_difference(&point.x, &carrier.ax, &carrier.dx, parameter)?;
    let dy = span_coordinate_difference(&point.y, &carrier.ay, &carrier.dy, parameter)?;
    dx.try_square()?.try_add(&dy.try_square()?)
}

/// Return a finite lower bound for the distance from an enclosed point to a
/// span's infinite carrier. The cross product is affine in the point, so its
/// extrema over the point enclosure occur at the four corners. Every
/// subsequent floating operation is widened toward the safe side; an
/// unsupported, nonfinite, underflowed, or sign-uncertain stage declines.
/// Restricting the carrier to the stored span can only increase the distance.
fn supporting_line_lower_squared(point_bounds: FloatBox, span: &SupportSpan) -> f64 {
    let Some(dx) = DdInterval::difference(span.source.end.x, span.source.start.x) else {
        return 0.0;
    };
    let Some(dy) = DdInterval::difference(span.source.end.y, span.source.start.y) else {
        return 0.0;
    };

    let mut cross_lower = f64::INFINITY;
    let mut cross_upper = f64::NEG_INFINITY;
    for (x, y) in point_bounds.corners() {
        let Some(px) = DdInterval::difference(x, span.source.start.x) else {
            return 0.0;
        };
        let Some(py) = DdInterval::difference(y, span.source.start.y) else {
            return 0.0;
        };
        let Some(left) = dx.mul(py) else {
            return 0.0;
        };
        let Some(right) = dy.mul(px) else {
            return 0.0;
        };
        let Some(cross) = left.sub(right) else {
            return 0.0;
        };
        let (lower, upper) = cross.outward_bounds();
        if !(lower.is_finite() && upper.is_finite() && lower <= upper) {
            return 0.0;
        }
        cross_lower = cross_lower.min(lower);
        cross_upper = cross_upper.max(upper);
    }

    let numerator_lower = if cross_lower > 0.0 {
        cross_lower
    } else if cross_upper < 0.0 {
        -cross_upper
    } else {
        return 0.0;
    };
    if !numerator_lower.is_normal() {
        return 0.0;
    }

    let Some(norm) = dx.square().and_then(|x| dy.square().and_then(|y| x.add(y))) else {
        return 0.0;
    };
    let (norm_lower, norm_upper) = norm.outward_bounds();
    if !norm_lower.is_finite() || norm_lower > norm_upper || !norm_upper.is_normal() {
        return 0.0;
    }
    // Binary64 sqrt and division are round-to-nearest on the supported
    // x86-64-v2 target; moving one representable value outward makes each
    // operand a conservative bound before the next operation.
    let norm_upper = norm_upper.sqrt().next_up();
    if !(norm_upper.is_finite() && norm_upper > 0.0) {
        return 0.0;
    }

    let distance_lower = (numerator_lower / norm_upper).next_down();
    if !distance_lower.is_normal() || distance_lower <= 0.0 {
        return 0.0;
    }
    let squared_lower = (distance_lower * distance_lower).next_down().max(0.0);
    if !squared_lower.is_normal() {
        return 0.0;
    }
    squared_lower
}

fn nonnegative_upper_bound(value: &Rational) -> Result<f64> {
    if value.try_cmp(&Rational::from_f64(f64::MAX)?)? == Ordering::Greater {
        Ok(f64::INFINITY)
    } else {
        Ok(value.outward_bounds()?.1)
    }
}

fn nonnegative_pair_upper_bound(value: &UnreducedRatio) -> Result<f64> {
    let maximum = ExactDyadic::try_from_f64(f64::MAX)?;
    if value
        .numerator
        .cmp(&maximum.try_product(&value.denominator)?)
        == Ordering::Greater
    {
        Ok(f64::INFINITY)
    } else {
        value.outward_upper()
    }
}

fn square_f64(value: f64) -> Result<Rational> {
    let value = ExactDyadic::try_from_f64(value)?;
    Rational::from_dyadic(value.try_product(&value)?)
}

#[cfg(test)]
fn dyadic_as_f64(value: &ExactDyadic) -> Result<f64> {
    Rational::from_dyadic(value.try_clone()?)?.round()
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SqrtStats {
    fast_admissions: usize,
    fallbacks: usize,
}

#[cfg(test)]
thread_local! {
    static SQRT_STATS: std::cell::Cell<SqrtStats> =
        const { std::cell::Cell::new(SqrtStats {
            fast_admissions: 0,
            fallbacks: 0,
        }) };
    static FORCE_EXACT_SQRT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn exact_sqrt_path_forced() -> bool {
    FORCE_EXACT_SQRT.with(std::cell::Cell::get)
}

#[cfg(test)]
fn record_sqrt_fast_admission() {
    SQRT_STATS.with(|stats| {
        let mut value = stats.get();
        value.fast_admissions += 1;
        stats.set(value);
    });
}

#[cfg(test)]
fn record_sqrt_fallback() {
    SQRT_STATS.with(|stats| {
        let mut value = stats.get();
        value.fallbacks += 1;
        stats.set(value);
    });
}

#[cfg(test)]
fn reset_sqrt_stats() {
    SQRT_STATS.with(|stats| stats.set(SqrtStats::default()));
}

#[cfg(test)]
fn sqrt_stats() -> SqrtStats {
    SQRT_STATS.with(std::cell::Cell::get)
}

#[cfg(test)]
struct ExactSqrtPathGuard(bool);

#[cfg(test)]
impl Drop for ExactSqrtPathGuard {
    fn drop(&mut self) {
        FORCE_EXACT_SQRT.with(|forced| forced.set(self.0));
    }
}

#[cfg(test)]
fn force_exact_sqrt_path(force: bool) -> ExactSqrtPathGuard {
    ExactSqrtPathGuard(FORCE_EXACT_SQRT.with(|forced| forced.replace(force)))
}

fn box_distance_lower(left: FloatBox, right: FloatBox) -> f64 {
    let gaps = [
        right.min_x - left.max_x,
        left.min_x - right.max_x,
        right.min_y - left.max_y,
        left.min_y - right.max_y,
    ];
    if gaps.iter().any(|value| value.is_nan()) {
        return 0.0;
    }
    let dx = gaps[0].max(gaps[1]).next_down().max(0.0);
    let dy = gaps[2].max(gaps[3]).next_down().max(0.0);
    let x_squared = (dx * dx).next_down().max(0.0);
    let y_squared = (dy * dy).next_down().max(0.0);
    (x_squared + y_squared).next_down().max(0.0)
}

fn aabb_lower_squared(point: &ExactPoint, bounds: FloatBox) -> Result<Rational> {
    let min_x = Rational::from_f64(bounds.min_x)?;
    let max_x = Rational::from_f64(bounds.max_x)?;
    let min_y = Rational::from_f64(bounds.min_y)?;
    let max_y = Rational::from_f64(bounds.max_y)?;
    let zero = Rational::zero();
    let dx = if point.x.try_cmp(&min_x)? == Ordering::Less {
        (&min_x).try_subtract(&point.x)?
    } else if point.x.try_cmp(&max_x)? == Ordering::Greater {
        (&point.x).try_subtract(&max_x)?
    } else {
        zero.try_clone()?
    };
    let dy = if point.y.try_cmp(&min_y)? == Ordering::Less {
        (&min_y).try_subtract(&point.y)?
    } else if point.y.try_cmp(&max_y)? == Ordering::Greater {
        (&point.y).try_subtract(&max_y)?
    } else {
        zero
    };
    (&(&dx).try_square()?).try_add(&(&dy).try_square()?)
}

fn point_in_cycle(point: &ExactPoint, cycle: &[ExactPoint]) -> Result<bool> {
    if cycle.len() < 3 {
        return Ok(false);
    }
    for index in 0..cycle.len() {
        let left = &cycle[index];
        let right = &cycle[(index + 1) % cycle.len()];
        let dx = (&right.x).try_subtract(&left.x)?;
        let dy = (&right.y).try_subtract(&left.y)?;
        let px = (&point.x).try_subtract(&left.x)?;
        let py = (&point.y).try_subtract(&left.y)?;
        let cross = (&(&dx).try_product(&py)?).try_subtract(&(&dy).try_product(&px)?)?;
        if cross.is_zero()
            && between(&point.x, &left.x, &right.x)?
            && between(&point.y, &left.y, &right.y)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn cycle_crosses_ray(point: &ExactPoint, cycle: &[ExactPoint]) -> Result<bool> {
    if cycle.len() < 3 {
        return Ok(false);
    }
    let mut crossings = false;
    for index in 0..cycle.len() {
        let left = &cycle[index];
        let right = &cycle[(index + 1) % cycle.len()];
        let left_above = left.y.try_cmp(&point.y)? == Ordering::Greater;
        let right_above = right.y.try_cmp(&point.y)? == Ordering::Greater;
        if left_above == right_above {
            continue;
        }
        let dx = (&right.x).try_subtract(&left.x)?;
        let dy = (&right.y).try_subtract(&left.y)?;
        let px = (&point.x).try_subtract(&left.x)?;
        let py = (&point.y).try_subtract(&left.y)?;
        let cross = (&(&dx).try_product(&py)?).try_subtract(&(&dy).try_product(&px)?)?;
        let rising = right.y.try_cmp(&left.y)? == Ordering::Greater;
        if (rising && cross.sign() == ExactSign::Positive)
            || (!rising && cross.sign() == ExactSign::Negative)
        {
            crossings = !crossings;
        }
    }
    Ok(crossings)
}

fn between(value: &Rational, left: &Rational, right: &Rational) -> Result<bool> {
    Ok(
        (value.try_cmp(left)? != Ordering::Less && value.try_cmp(right)? != Ordering::Greater)
            || (value.try_cmp(left)? != Ordering::Greater
                && value.try_cmp(right)? != Ordering::Less),
    )
}

fn segment_box_intersection(edge: &BoundaryEdge, bounds: FloatBox) -> Result<Option<ExactPoint>> {
    if point_in_box(&edge.start, bounds)? {
        return Ok(Some(edge.start.try_clone()?));
    }
    if point_in_box(&edge.end, bounds)? {
        return Ok(Some(edge.end.try_clone()?));
    }
    let dx = (&edge.end.x).try_subtract(&edge.start.x)?;
    let dy = (&edge.end.y).try_subtract(&edge.start.y)?;
    for x in [bounds.min_x, bounds.max_x] {
        if dx.is_zero() {
            continue;
        }
        let x = Rational::from_f64(x)?;
        let parameter = (&(&x).try_subtract(&edge.start.x)?).try_quotient(&dx)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let y = (&edge.start.y).try_add(&(&dy).try_product(&parameter)?)?;
        if y.try_cmp(&Rational::from_f64(bounds.min_y)?)? != Ordering::Less
            && y.try_cmp(&Rational::from_f64(bounds.max_y)?)? != Ordering::Greater
        {
            return Ok(Some(ExactPoint { x, y }));
        }
    }
    for y in [bounds.min_y, bounds.max_y] {
        if dy.is_zero() {
            continue;
        }
        let y = Rational::from_f64(y)?;
        let parameter = (&(&y).try_subtract(&edge.start.y)?).try_quotient(&dy)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let x = (&edge.start.x).try_add(&(&dx).try_product(&parameter)?)?;
        if x.try_cmp(&Rational::from_f64(bounds.min_x)?)? != Ordering::Less
            && x.try_cmp(&Rational::from_f64(bounds.max_x)?)? != Ordering::Greater
        {
            return Ok(Some(ExactPoint { x, y }));
        }
    }
    Ok(None)
}

fn point_in_box(point: &ExactPoint, bounds: FloatBox) -> Result<bool> {
    let min_x = Rational::from_f64(bounds.min_x)?;
    let max_x = Rational::from_f64(bounds.max_x)?;
    let min_y = Rational::from_f64(bounds.min_y)?;
    let max_y = Rational::from_f64(bounds.max_y)?;
    Ok(point.x.try_cmp(&min_x)? != Ordering::Less
        && point.x.try_cmp(&max_x)? != Ordering::Greater
        && point.y.try_cmp(&min_y)? != Ordering::Less
        && point.y.try_cmp(&max_y)? != Ordering::Greater)
}

fn exact_segment_enters_candidate(
    start: &ExactPoint,
    end: &ExactPoint,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let zero = Rational::zero();
    let one = Rational::one();
    let mut lower = zero.try_clone()?;
    let mut upper = one.try_clone()?;
    let mut lower_closed = true;
    let mut upper_closed = true;
    let source_x = end.x.try_subtract(&start.x)?;
    let source_y = end.y.try_subtract(&start.y)?;

    for index in 0..candidate.len() {
        let left = candidate[index];
        let right = candidate[(index + 1) % candidate.len()];
        let edge_x = Rational::from_f64(right.x)?.try_subtract(&Rational::from_f64(left.x)?)?;
        let edge_y = Rational::from_f64(right.y)?.try_subtract(&Rational::from_f64(left.y)?)?;
        let start_x = start.x.try_subtract(&Rational::from_f64(left.x)?)?;
        let start_y = start.y.try_subtract(&Rational::from_f64(left.y)?)?;
        let mut c0 = edge_x
            .try_product(&start_y)?
            .try_subtract(&edge_y.try_product(&start_x)?)?;
        let mut c1 = edge_x
            .try_product(&source_y)?
            .try_subtract(&edge_y.try_product(&source_x)?)?;
        if !counter_clockwise {
            c0 = zero.try_subtract(&c0)?;
            c1 = zero.try_subtract(&c1)?;
        }

        match c1.sign() {
            ExactSign::Zero => {
                if c0.sign() != ExactSign::Positive {
                    return Ok(false);
                }
            },
            ExactSign::Positive => {
                let bound = zero.try_subtract(&c0)?.try_quotient(&c1)?;
                match bound.try_cmp(&lower)? {
                    Ordering::Greater => {
                        lower = bound;
                        lower_closed = false;
                    },
                    Ordering::Equal => lower_closed = false,
                    Ordering::Less => {},
                }
            },
            ExactSign::Negative => {
                let bound = c0.try_quotient(&zero.try_subtract(&c1)?)?;
                match bound.try_cmp(&upper)? {
                    Ordering::Less => {
                        upper = bound;
                        upper_closed = false;
                    },
                    Ordering::Equal => upper_closed = false,
                    Ordering::Greater => {},
                }
            },
        }
        let order = lower.try_cmp(&upper)?;
        if order == Ordering::Greater
            || (order == Ordering::Equal && !(lower_closed && upper_closed))
        {
            return Ok(false);
        }
    }
    Ok(true)
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

impl SourcePatch {
    fn witness(&self, source: &PreparedSupport, center_first: bool) -> Result<Option<ExactPoint>> {
        match self {
            Self::Span { span, lo, hi } => {
                let midpoint = lo.try_midpoint(hi)?;
                Ok(Some(span_point(&source.spans[*span], &midpoint)?))
            },
            Self::Box(bounds) => source.source_box_witness(*bounds, center_first),
            Self::Point(point) => Ok(Some(source.points[*point].try_clone()?)),
        }
    }

    fn center_and_radius_squared(
        &self,
        source: &PreparedSupport,
    ) -> Result<(ExactPoint, Rational)> {
        match self {
            Self::Span { span, lo, hi } => {
                let midpoint = lo.try_midpoint(hi)?;
                let center = span_point(&source.spans[*span], &midpoint)?;
                let radius = span_radius_squared(&source.spans[*span], lo, hi)?;
                Ok((center, radius))
            },
            Self::Box(bounds) => {
                let min_x = Rational::from_f64(bounds.min_x)?;
                let max_x = Rational::from_f64(bounds.max_x)?;
                let min_y = Rational::from_f64(bounds.min_y)?;
                let max_y = Rational::from_f64(bounds.max_y)?;
                let center = ExactPoint {
                    x: min_x.try_midpoint(&max_x)?,
                    y: min_y.try_midpoint(&max_y)?,
                };
                let half_x =
                    (&(&max_x).try_subtract(&min_x)?).try_quotient(&Rational::from_f64(2.0)?)?;
                let half_y =
                    (&(&max_y).try_subtract(&min_y)?).try_quotient(&Rational::from_f64(2.0)?)?;
                let radius = (&(&half_x).try_square()?).try_add(&(&half_y).try_square()?)?;
                Ok((center, radius))
            },
            Self::Point(point) => Ok((source.points[*point].try_clone()?, Rational::zero())),
        }
    }

    #[cfg(any(test, debug_assertions))]
    fn witness_is_valid(&self, source: &PreparedSupport, witness: &ExactPoint) -> Result<bool> {
        match self {
            Self::Span { span, lo, hi } => {
                let source_span = &source.spans[*span];
                let parameter = span_projection_parameter(witness, source_span)?;
                if parameter.try_cmp(lo)? == Ordering::Less
                    || parameter.try_cmp(hi)? == Ordering::Greater
                {
                    return Ok(false);
                }
                let reconstructed = span_point(source_span, &parameter)?;
                Ok(reconstructed.same_position(witness)?)
            },
            Self::Box(bounds) => {
                Ok(point_in_box(witness, *bounds)? && source.contains_area(witness)?)
            },
            Self::Point(point) => source.points[*point].same_position(witness),
        }
    }

    fn corners(&self, source: &PreparedSupport) -> Result<Vec<ExactPoint>> {
        let mut corners = Vec::new();
        match self {
            Self::Span { span, lo, hi } => {
                try_reserve_vec(&mut corners, 2)?;
                try_push_vec(&mut corners, span_point(&source.spans[*span], lo)?)?;
                try_push_vec(&mut corners, span_point(&source.spans[*span], hi)?)?;
            },
            Self::Box(bounds) => {
                try_reserve_vec(&mut corners, 4)?;
                for (x, y) in bounds.corners() {
                    try_push_vec(&mut corners, exact_point_xy(x, y)?)?;
                }
            },
            Self::Point(point) => {
                try_reserve_vec(&mut corners, 1)?;
                try_push_vec(&mut corners, source.points[*point].try_clone()?)?;
            },
        }
        Ok(corners)
    }

    fn split(self, budget: &mut ExpansionBudget) -> Result<Option<Vec<Self>>> {
        match self {
            Self::Span { span, lo, hi } => {
                budget.add(2)?;
                let midpoint = lo.try_midpoint(&hi)?;
                let mut children = Vec::new();
                try_reserve_vec(&mut children, 2)?;
                try_push_vec(&mut children, Self::Span {
                    span,
                    lo: lo.try_clone()?,
                    hi: midpoint.try_clone()?,
                })?;
                try_push_vec(&mut children, Self::Span {
                    span,
                    lo: midpoint,
                    hi,
                })?;
                Ok(Some(children))
            },
            Self::Box(bounds) => {
                let mid_x = f64::midpoint(bounds.min_x, bounds.max_x);
                let mid_y = f64::midpoint(bounds.min_y, bounds.max_y);
                let split_x = bounds.min_x < mid_x && mid_x < bounds.max_x;
                let split_y = bounds.min_y < mid_y && mid_y < bounds.max_y;
                if !split_x && !split_y {
                    return Ok(None);
                }
                let count = (1 + usize::from(split_x)) * (1 + usize::from(split_y));
                budget.add(count)?;
                let mut children = Vec::new();
                try_reserve_vec(&mut children, count)?;
                let xs = if split_x {
                    [(bounds.min_x, mid_x), (mid_x, bounds.max_x)]
                } else {
                    [(bounds.min_x, bounds.max_x); 2]
                };
                let ys = if split_y {
                    [(bounds.min_y, mid_y), (mid_y, bounds.max_y)]
                } else {
                    [(bounds.min_y, bounds.max_y); 2]
                };
                for &(min_x, max_x) in &xs[..if split_x { 2 } else { 1 }] {
                    for &(min_y, max_y) in &ys[..if split_y { 2 } else { 1 }] {
                        try_push_vec(
                            &mut children,
                            Self::Box(FloatBox {
                                min_x,
                                max_x,
                                min_y,
                                max_y,
                            }),
                        )?;
                    }
                }
                Ok(Some(children))
            },
            Self::Point(_) => Ok(None),
        }
    }
}

fn span_radius_squared(span: &SupportSpan, lo: &Rational, hi: &Rational) -> Result<Rational> {
    let delta = hi.try_subtract(lo)?;
    let half = delta.try_quotient(&Rational::from_f64(2.0)?)?;
    span.carrier.norm_squared.try_product(&half.try_square()?)
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

const OVERFLOW_NEWTON_STEPS: usize = 4;

fn overflow_newton_step(squared: &Rational, upper: &Rational) -> Result<Rational> {
    let reciprocal = squared.try_quotient(upper)?;
    upper
        .try_add(&reciprocal)?
        .try_quotient(&Rational::from_f64(2.0)?)
}

impl FloatSqrtBracket {
    fn into_interval(self, squared: &Rational, error: Option<&Rational>) -> Result<SqrtInterval> {
        match self.upper {
            None => SqrtInterval::overflow(squared, error),
            Some(upper) if self.lower.to_bits() == upper.to_bits() => {
                SqrtInterval::singleton(self.lower)
            },
            Some(upper) => SqrtInterval::adjacent(self.lower, upper),
        }
    }

    fn nearest_candidate(self, squared: &Rational) -> Result<Option<f64>> {
        let Some(upper) = self.upper else {
            debug_assert_eq!(self.lower.to_bits(), f64::MAX.to_bits());
            return Ok(Some(self.lower));
        };
        if self.lower.to_bits() == upper.to_bits() {
            return Ok(Some(self.lower));
        }
        let midpoint = ExactDyadic::try_from_f64(self.lower)?
            .try_add(ExactDyadic::try_from_f64(upper)?)?
            .try_product(&ExactDyadic::try_from_f64(0.5)?)?;
        let midpoint_squared = Rational::from_dyadic(midpoint.try_product(&midpoint)?)?;
        Ok(Some(match squared.try_cmp(&midpoint_squared)? {
            Ordering::Less => self.lower,
            Ordering::Equal if self.lower.to_bits() & 1 == 0 => self.lower,
            Ordering::Greater | Ordering::Equal => upper,
        }))
    }
}

impl SqrtInterval {
    #[cfg(test)]
    fn from_squared(squared: &Rational) -> Result<Self> {
        Self::from_squared_with_error(squared, None)
    }

    fn from_squared_with_error(squared: &Rational, error: Option<&Rational>) -> Result<Self> {
        #[cfg(test)]
        if exact_sqrt_path_forced() {
            record_sqrt_fallback();
            return Self::from_squared_exact_with_error(squared, error);
        }

        if let Some(bracket) = Self::from_squared_fast(squared)? {
            #[cfg(test)]
            record_sqrt_fast_admission();
            return bracket.into_interval(squared, error);
        }

        #[cfg(test)]
        record_sqrt_fallback();
        Self::from_squared_exact_with_error(squared, error)
    }

    /// Seed a bracket from the rounded squared value, then certify each
    /// endpoint against the original exact squared value. A platform sqrt
    /// estimate that is not close enough simply takes the exact fallback.
    fn from_squared_fast(squared: &Rational) -> Result<Option<FloatSqrtBracket>> {
        const MAX_ULP_STEPS: usize = 4;
        let rounded_squared = match squared.round() {
            Ok(value) => value,
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(
                        crate::geometry::GeometryErrorKind::ExactCoordinateOutOfRange
                    )
                ) =>
            {
                return Ok(None);
            },
            Err(error) => return Err(error),
        };
        if !(rounded_squared.is_finite() && rounded_squared > 0.0) {
            return Ok(None);
        }

        let proposal = rounded_squared.sqrt();
        if !(proposal.is_finite() && proposal > 0.0) {
            return Ok(None);
        }

        let mut order = squared.try_cmp(&square_f64(proposal)?)?;
        if order == Ordering::Equal {
            return Ok(Some(FloatSqrtBracket {
                lower: proposal,
                upper: Some(proposal),
            }));
        }

        match order {
            Ordering::Less => {
                let mut upper = proposal;
                for _ in 0..MAX_ULP_STEPS {
                    let lower = upper.next_down();
                    order = squared.try_cmp(&square_f64(lower)?)?;
                    match order {
                        Ordering::Greater => {
                            return Ok(Some(FloatSqrtBracket {
                                lower,
                                upper: Some(upper),
                            }));
                        },
                        Ordering::Equal => {
                            return Ok(Some(FloatSqrtBracket {
                                lower,
                                upper: Some(lower),
                            }));
                        },
                        Ordering::Less => upper = lower,
                    }
                }
            },
            Ordering::Greater => {
                let mut lower = proposal;
                for _ in 0..MAX_ULP_STEPS {
                    if lower.to_bits() == f64::MAX.to_bits() {
                        return Ok(Some(FloatSqrtBracket { lower, upper: None }));
                    }
                    let upper = lower.next_up();
                    order = squared.try_cmp(&square_f64(upper)?)?;
                    match order {
                        Ordering::Less => {
                            return Ok(Some(FloatSqrtBracket {
                                lower,
                                upper: Some(upper),
                            }));
                        },
                        Ordering::Equal => {
                            return Ok(Some(FloatSqrtBracket {
                                lower: upper,
                                upper: Some(upper),
                            }));
                        },
                        Ordering::Greater => lower = upper,
                    }
                }
            },
            Ordering::Equal => unreachable!("the equal square was returned above"),
        }
        Ok(None)
    }

    fn from_squared_exact_with_error(squared: &Rational, error: Option<&Rational>) -> Result<Self> {
        if squared.try_cmp(&square_f64(f64::MAX)?)? == Ordering::Greater {
            return Self::overflow(squared, error);
        }
        let value = squared.try_clone()?;
        let rounded = value.round_sqrt()?;
        if rounded.is_infinite() {
            return Self::overflow(squared, error);
        }
        match squared.try_cmp(&square_f64(rounded)?)? {
            Ordering::Equal => Self::singleton(rounded),
            // The predecessor of +0 is negative in total binary64 order,
            // but a squared-distance bracket must stay in the nonnegative root domain.
            Ordering::Less if rounded == 0.0 => Self::adjacent(0.0, f64::from_bits(1)),
            Ordering::Less => Self::adjacent(rounded.next_down(), rounded),
            Ordering::Greater => {
                if rounded.to_bits() == f64::MAX.to_bits() {
                    Self::overflow(squared, error)
                } else {
                    Self::adjacent(rounded, rounded.next_up())
                }
            },
        }
    }

    fn candidate_from_squared(squared: &Rational) -> Result<Option<f64>> {
        #[cfg(test)]
        if exact_sqrt_path_forced() {
            record_sqrt_fallback();
            return Self::complete_candidate(squared);
        }

        if let Some(bracket) = Self::from_squared_fast(squared)? {
            #[cfg(test)]
            record_sqrt_fast_admission();
            return bracket.nearest_candidate(squared);
        }

        #[cfg(test)]
        record_sqrt_fallback();
        Self::complete_candidate(squared)
    }

    fn complete_candidate(squared: &Rational) -> Result<Option<f64>> {
        if squared.try_cmp(&square_f64(f64::MAX)?)? == Ordering::Greater {
            return Ok(Some(f64::MAX));
        }
        let value = squared.try_clone()?.round_sqrt()?;
        Ok(value.is_finite().then_some(value))
    }

    fn singleton(value: f64) -> Result<Self> {
        let value = ExactDyadic::try_from_f64(value)?;
        Ok(Self {
            lower: value.try_clone()?,
            upper: Some(value),
            overflow_upper: None,
        })
    }

    fn adjacent(lower: f64, upper: f64) -> Result<Self> {
        debug_assert!(lower.is_finite() && upper.is_finite());
        debug_assert_eq!(upper.to_bits() - lower.to_bits(), 1);
        Ok(Self {
            lower: ExactDyadic::try_from_f64(lower)?,
            upper: Some(ExactDyadic::try_from_f64(upper)?),
            overflow_upper: None,
        })
    }

    fn overflow(squared: &Rational, error: Option<&Rational>) -> Result<Self> {
        let maximum = Rational::from_f64(f64::MAX)?;
        let mut upper = squared.try_quotient(&maximum)?;
        let target = error.map(|error| maximum.try_add(error)).transpose()?;
        let target_squared = target.as_ref().map(Rational::try_square).transpose()?;
        for _ in 0..OVERFLOW_NEWTON_STEPS {
            upper = overflow_newton_step(squared, &upper)?;
            if let Some(target) = target.as_ref()
                && matches!(upper.try_cmp(target)?, Ordering::Less | Ordering::Equal)
            {
                break;
            }
        }
        if let (Some(target), Some(target_squared)) = (target, target_squared)
            && squared.try_cmp(&target_squared)? != Ordering::Greater
            && upper.try_cmp(&target)? == Ordering::Greater
        {
            upper = target;
        }
        Ok(Self {
            lower: ExactDyadic::try_from_f64(f64::MAX)?,
            upper: None,
            overflow_upper: Some(upper),
        })
    }

    fn refine(&mut self, squared: &Rational, error: &Rational) -> Result<()> {
        let Some(mut upper) = self.upper.take() else {
            return Ok(());
        };
        loop {
            let width =
                Rational::from_dyadic(upper.try_clone()?.try_subtract(self.lower.try_clone()?)?)?;
            if width.try_cmp(error)? != Ordering::Greater {
                self.upper = Some(upper);
                return Ok(());
            }

            let midpoint = self
                .lower
                .try_clone()?
                .try_add(upper.try_clone()?)?
                .try_product(&ExactDyadic::try_from_f64(0.5)?)?;
            let midpoint_rational = Rational::from_dyadic(midpoint.try_clone()?)?;
            let midpoint_squared = (&midpoint_rational).try_square()?;

            match squared.try_cmp(&midpoint_squared)? {
                Ordering::Equal => {
                    self.lower = midpoint.try_clone()?;
                    upper = midpoint;
                },
                Ordering::Greater => {
                    self.lower = midpoint;
                },
                Ordering::Less => {
                    upper = midpoint;
                },
            }
        }
    }

    fn upper_rational(&self) -> Result<Option<Rational>> {
        if let Some(value) = &self.upper {
            return Ok(Some(Rational::from_dyadic(value.try_clone()?)?));
        }
        self.overflow_upper
            .as_ref()
            .map(Rational::try_clone)
            .transpose()
    }
}

fn min_rational(current: &mut Option<Rational>, candidate: Rational) -> Result<()> {
    let replace = match current {
        None => true,
        Some(value) => candidate.try_cmp(value)? == Ordering::Less,
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}

fn max_rational(current: &mut Option<Rational>, candidate: Rational) -> Result<()> {
    let replace = match current {
        None => true,
        Some(value) => candidate.try_cmp(value)? == Ordering::Greater,
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}

#[derive(Debug)]
struct PatchRecord {
    patch: SourcePatch,
    upper: Option<Rational>,
}

#[derive(Debug)]
struct QueueEntry {
    key: f64,
    order: usize,
    record: PatchRecord,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key.to_bits() == other.key.to_bits() && self.order == other.order
    }
}

impl Eq for QueueEntry {}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .total_cmp(&other.key)
            // Earlier extraction order wins an exact queue-key tie. This is
            // part of the directed result's deterministic witness contract.
            .then_with(|| other.order.cmp(&self.order))
    }
}

#[derive(Debug)]
struct PatchQueue {
    heap: BinaryHeap<QueueEntry>,
    stable_ties: bool,
    next_order: usize,
}

impl PatchQueue {
    fn new(stable_ties: bool) -> Self {
        Self {
            heap: BinaryHeap::new(),
            stable_ties,
            next_order: 0,
        }
    }

    fn push(&mut self, record: PatchRecord) -> Result<()> {
        let key = match &record.upper {
            Some(value) => nonnegative_upper_bound(value)?,
            None => f64::INFINITY,
        };
        let order = if self.stable_ties {
            let order = self.next_order;
            self.next_order = self
                .next_order
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            order
        } else {
            0
        };
        crate::try_push_heap(&mut self.heap, QueueEntry { key, order, record })?;

        Ok(())
    }

    fn pop(&mut self) -> Option<PatchRecord> {
        let record = self.heap.pop().map(|entry| entry.record);
        record
    }

    fn max_upper(&self) -> Result<Option<Rational>> {
        let Some(entry) = self.heap.peek() else {
            return Ok(None);
        };
        if entry.key.is_infinite() {
            return Ok(None);
        }
        Ok(Some(Rational::from_f64(entry.key)?))
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
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
    fn new(accuracy: &'a Accuracy) -> Self {
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

    fn new_directed(
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
            // Clone before publishing either field: an allocation failure
            // cannot leave a clearance paired with a different witness.
            let witness = witness.try_clone()?;
            self.lower_squared = Some(squared);
            self.lower_witness = Some(witness);
        }
        // Equal exact clearances intentionally retain the first witness
        // visited. Patch queue insertion order is monotonic and tie-stable.
        Ok(())
    }

    fn scaled_squared(&self, squared: &Rational) -> Result<Rational> {
        squared.try_product(&self.accuracy.scale_squared)
    }
}

fn sqrt_upper(squared: &Rational, context: &mut SearchContext<'_>) -> Result<SqrtInterval> {
    if context.exact_upper {
        return SqrtInterval::from_squared_with_error(squared, None);
    }
    let mut interval = match &context.accuracy.error {
        ErrorBound::Absolute(error) => SqrtInterval::from_squared_with_error(squared, Some(error))?,
        ErrorBound::Relative(relative) => {
            let mut interval = SqrtInterval::from_squared_with_error(squared, None)?;
            if interval.upper.is_none() {
                // MAX is the proven lower root endpoint, so r*MAX is no
                // larger than r*H and remains a valid relative allowance.
                let error = Rational::from_f64(f64::MAX)?.try_product(relative)?;
                interval = SqrtInterval::overflow(squared, Some(&error))?;
            }
            interval
        },
    };
    if let ErrorBound::Absolute(error) = &context.accuracy.error {
        interval.refine(squared, error)?;
    }
    Ok(interval)
}

fn sum_upper(left: &SqrtInterval, right: &SqrtInterval) -> Result<Option<Rational>> {
    if let (Some(left), Some(right)) = (&left.upper, &right.upper) {
        let sum = left.try_clone()?.try_add(right.try_clone()?)?;
        return Ok(Some(Rational::from_dyadic(sum)?));
    }
    let (Some(left), Some(right)) = (left.upper_rational()?, right.upper_rational()?) else {
        return Ok(None);
    };
    Ok(Some(left.try_add(&right)?))
}

fn fixed_site_upper(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    nominated: &[PrimitiveId],
    context: &mut SearchContext<'_>,
) -> Result<Option<Rational>> {
    if nominated.is_empty() {
        return Ok(None);
    }
    let corners = patch.corners(source)?;
    let mut best = None;
    for &primitive in nominated {
        let mut site_upper = None;
        for corner in &corners {
            let squared = target
                .primitive_distance_squared(primitive, corner)?
                .to_rational()?;
            max_rational(&mut site_upper, squared)?;
        }
        min_rational(&mut best, site_upper.expect("fixed site has corners"))?;
    }
    let squared = context.scaled_squared(&best.expect("nonempty nominated sites"))?;
    sqrt_upper(&squared, context)?.upper_rational()
}

fn carrier_line_value(span: &SupportSpan, point: &ExactPoint) -> Result<Rational> {
    let carrier = &span.carrier;
    let px = point.x.try_subtract(&carrier.ax)?;
    let py = point.y.try_subtract(&carrier.ay)?;
    carrier
        .dx
        .try_product(&py)?
        .try_subtract(&carrier.dy.try_product(&px)?)
}

fn exact_parallel(left: &SupportSpan, right: &SupportSpan) -> Result<bool> {
    Ok(left
        .carrier
        .dx
        .try_product(&right.carrier.dy)?
        .try_subtract(&left.carrier.dy.try_product(&right.carrier.dx)?)?
        .is_zero())
}

fn projection_is_in_span(point: &ExactPoint, span: &SupportSpan) -> Result<bool> {
    let parameter = span_projection_parameter(point, span)?;
    Ok(parameter.try_cmp(&span.t_lo)? != Ordering::Less
        && parameter.try_cmp(&span.t_hi)? != Ordering::Greater)
}

fn signs_between_parallel_lines(first: &[Rational], second: &[Rational]) -> bool {
    let first_positive = first
        .iter()
        .all(|value| value.sign() != ExactSign::Negative);
    let first_negative = first
        .iter()
        .all(|value| value.sign() != ExactSign::Positive);
    let second_positive = second
        .iter()
        .all(|value| value.sign() != ExactSign::Negative);
    let second_negative = second
        .iter()
        .all(|value| value.sign() != ExactSign::Positive);
    (first_positive && second_negative) || (first_negative && second_positive)
}

fn add_parallel_probe(
    point: &ExactPoint,
    left: &SupportSpan,
    right: &SupportSpan,
    separation: &Rational,
    left_values: &mut Vec<Rational>,
    right_values: &mut Vec<Rational>,
    valid: &mut bool,
) -> Result<()> {
    if !projection_is_in_span(point, left)? || !projection_is_in_span(point, right)? {
        *valid = false;
        return Ok(());
    }
    let left_value = carrier_line_value(left, point)?;
    try_push_vec(right_values, left_value.try_subtract(separation)?)?;
    try_push_vec(left_values, left_value)?;
    Ok(())
}

fn parallel_strip_upper(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    nominated: &[PrimitiveId],
    context: &mut SearchContext<'_>,
) -> Result<Option<Rational>> {
    let mut span_indices = Vec::new();
    try_reserve_vec(&mut span_indices, nominated.len())?;
    for &primitive in nominated {
        if let PrimitiveId::Span(index) = primitive {
            try_push_vec(&mut span_indices, index)?;
        }
    }
    if span_indices.len() < 2 {
        return Ok(None);
    }
    let corners = patch.corners(source)?;
    let area_probe =
        context.exact_upper && matches!(patch, SourcePatch::Box(_)) && !source.faces.is_empty();
    let probe_count = if area_probe {
        source
            .faces
            .iter()
            .flat_map(|face| face.iter().map(Vec::len))
            .try_fold(0usize, |count, length| {
                count
                    .checked_add(length)
                    .ok_or(crate::error::ResourceError::SizeOverflow)
            })?
    } else {
        corners.len()
    };
    let mut best = None;
    for (left_offset, &left_index) in span_indices.iter().enumerate() {
        for &right_index in &span_indices[(left_offset + 1)..] {
            let left = &target.spans[left_index];
            let right = &target.spans[right_index];
            if !exact_parallel(left, right)? {
                continue;
            }
            let separation = carrier_line_value(left, &right.start)?;
            let mut left_values = Vec::new();
            let mut right_values = Vec::new();
            try_reserve_vec(&mut left_values, probe_count)?;
            try_reserve_vec(&mut right_values, probe_count)?;
            let mut valid = true;
            if area_probe {
                for face in &source.faces {
                    for cycle in face {
                        for point in cycle {
                            add_parallel_probe(
                                point,
                                left,
                                right,
                                &separation,
                                &mut left_values,
                                &mut right_values,
                                &mut valid,
                            )?;
                            if !valid {
                                break;
                            }
                        }
                        if !valid {
                            break;
                        }
                    }
                    if !valid {
                        break;
                    }
                }
            } else {
                for point in &corners {
                    add_parallel_probe(
                        point,
                        left,
                        right,
                        &separation,
                        &mut left_values,
                        &mut right_values,
                        &mut valid,
                    )?;
                    if !valid {
                        break;
                    }
                }
            }
            if !valid || !signs_between_parallel_lines(&left_values, &right_values) {
                continue;
            }
            let separation_squared = separation
                .try_square()?
                .try_quotient(&left.carrier.norm_squared)?;
            let half = (&separation_squared).try_quotient(&Rational::from_f64(4.0)?)?;
            let squared = context.scaled_squared(&half)?;
            let Some(upper) = sqrt_upper(&squared, context)?.upper_rational()? else {
                continue;
            };

            min_rational(&mut best, upper)?;
        }
    }
    Ok(best)
}

fn patch_upper(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<Option<Rational>> {
    let (center, radius_squared) = patch.center_and_radius_squared(source)?;
    let center_squared = target.distance_squared(&center)?;
    let center_squared = context.scaled_squared(&center_squared)?;
    let radius_squared = context.scaled_squared(&radius_squared)?;
    let center_interval = sqrt_upper(&center_squared, context)?;
    let radius_interval = sqrt_upper(&radius_squared, context)?;
    let mut best = sum_upper(&center_interval, &radius_interval)?;

    let nominated = target.index.nominate(&center)?;
    if let Some(fixed) = fixed_site_upper(patch, source, target, &nominated, context)? {
        let replaces = match &best {
            None => true,
            Some(current) => fixed.try_cmp(current)? == Ordering::Less,
        };
        if replaces {
            best = Some(fixed);
        }
    }
    if let Some(strip) = parallel_strip_upper(patch, source, target, &nominated, context)? {
        let replaces = match &best {
            None => true,
            Some(current) => strip.try_cmp(current)? == Ordering::Less,
        };
        if replaces {
            best = Some(strip);
        }
    }
    Ok(best)
}

fn upper_is_below_lower(upper: &Rational, context: &mut SearchContext<'_>) -> Result<bool> {
    let Some(lower) = &context.lower_squared else {
        return Ok(false);
    };
    let upper_squared = (upper).try_square()?;
    Ok(upper_squared.try_cmp(lower)? != Ordering::Greater)
}

fn build_patch(
    patch: SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<Option<PatchRecord>> {
    let Some(witness) = patch.witness(source, context.exact_upper)? else {
        return Ok(None);
    };
    #[cfg(debug_assertions)]
    debug_assert!(patch.witness_is_valid(source, &witness)?);

    let witness_squared = target.distance_squared(&witness)?;
    let witness_squared = context.scaled_squared(&witness_squared)?;
    // Boundary patches are useful for the global enclosure, but they cannot
    // become a zero-clearance winner while a filled source has positive area.
    let update_witness =
        !(context.exact_upper && context.require_positive_witness && witness_squared.is_zero());
    if update_witness {
        if context.exact_upper {
            context.update_lower_with_witness(witness_squared, &witness)?;
        } else {
            context.update_lower(witness_squared)?;
        }
    }
    let upper = patch_upper(&patch, source, target, context)?;
    if let Some(upper_value) = &upper
        && upper_is_below_lower(upper_value, context)?
    {
        return Ok(None);
    }
    Ok(Some(PatchRecord { patch, upper }))
}

fn lower_candidate(context: &mut SearchContext<'_>) -> Result<Option<f64>> {
    let Some(lower) = &context.lower_squared else {
        return Ok(None);
    };
    SqrtInterval::candidate_from_squared(lower)
}

fn candidate_certified(
    candidate: f64,
    upper: &Rational,
    context: &mut SearchContext<'_>,
) -> Result<bool> {
    let Some(lower_squared) = &context.lower_squared else {
        return Ok(false);
    };
    if !candidate.is_finite() {
        return Ok(false);
    }
    let error = context.accuracy.absolute_error(lower_squared, candidate)?;
    let candidate = Rational::from_f64(candidate)?;
    let high = candidate.try_add(&error)?;
    if upper.try_cmp(&high)? == Ordering::Greater {
        return Ok(false);
    }
    let low = candidate.try_subtract(&error)?;
    if low.sign() != ExactSign::Positive {
        return Ok(true);
    }
    let low_squared = (&low).try_square()?;
    Ok(lower_squared.try_cmp(&low_squared)? != Ordering::Less)
}

fn queue_upper(queue: &PatchQueue, context: &mut SearchContext<'_>) -> Result<Option<Rational>> {
    let Some(lower) = context
        .lower_squared
        .as_ref()
        .map(Rational::try_clone)
        .transpose()?
    else {
        return Ok(None);
    };
    let interval = sqrt_upper(&lower, context)?;
    let Some(witness_upper) = interval.upper_rational()? else {
        return Ok(None);
    };
    if queue.is_empty() {
        return Ok(Some(witness_upper));
    }
    let Some(mut upper) = queue.max_upper()? else {
        return Ok(None);
    };
    if upper.try_cmp(&witness_upper)? == Ordering::Less {
        upper = witness_upper;
    }
    Ok(Some(upper))
}

fn run_direction(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<Rational> {
    let mut queue = PatchQueue::new(context.exact_upper);
    for patch in source.initial_patches()? {
        if let Some(record) = build_patch(patch, source, target, context)? {
            queue.push(record)?;
        }
    }
    loop {
        let candidate = lower_candidate(context)?;
        let upper = queue_upper(&queue, context)?;
        if let (Some(candidate), Some(upper)) = (candidate, upper)
            && candidate_certified(candidate, &upper, context)?
        {
            return Ok(upper);
        }
        if queue.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance lower/upper enclosure did not close",
            ));
        }
        let record = queue.pop().expect("nonempty patch queue");

        if let Some(upper) = &record.upper
            && upper_is_below_lower(upper, context)?
        {
            continue;
        }
        #[cfg(test)]
        count_refinement_work()?;
        let Some(children) = record.patch.split(&mut context.budget)? else {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance patch reached a nonrepresentable subdivision",
            ));
        };

        for patch in children {
            if let Some(child) = build_patch(patch, source, target, context)? {
                queue.push(child)?;
            }
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

impl DirectedOptimizationResult {
    pub(in crate::geometry) const fn center(&self) -> Point {
        self.center
    }

    pub(in crate::geometry) fn source_support(&self) -> &PreparedSupport {
        &self.source
    }

    pub(in crate::geometry) fn boundary_support(&self) -> &PreparedSupport {
        &self.boundary
    }

    pub(in crate::geometry) fn radius_in_scale(&self, scale: f64) -> Result<f64> {
        if !(scale.is_finite() && scale > 0.0) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "maximum_inscribed_circle scale must be positive and finite",
            ));
        }
        let scale = Rational::from_f64(scale)?;
        let squared = self
            .center_squared
            .try_product(&scale)?
            .try_product(&scale)?;
        downward_sqrt(&squared)
    }
}

fn downward_sqrt(squared: &Rational) -> Result<f64> {
    let interval = SqrtInterval::from_squared_with_error(squared, None)?;
    Rational::from_dyadic(interval.lower)?.round()
}

fn movement_lower_squared(
    witness_squared: &Rational,
    movement_squared: &Rational,
) -> Result<Rational> {
    let witness_interval = SqrtInterval::from_squared_with_error(witness_squared, None)?;
    let movement_interval = SqrtInterval::from_squared_with_error(movement_squared, None)?;
    let witness_lower = Rational::from_dyadic(witness_interval.lower)?;
    let Some(movement_upper) = movement_interval.upper_rational()? else {
        return Ok(Rational::zero());
    };
    if witness_lower.try_cmp(&movement_upper)? != Ordering::Greater {
        return Ok(Rational::zero());
    }
    let clearance = witness_lower.try_subtract(&movement_upper)?;
    clearance.try_square()
}

fn exact_point_distance_squared(left: &ExactPoint, right: &ExactPoint) -> Result<Rational> {
    let dx = left.x.try_subtract(&right.x)?;
    let dy = left.y.try_subtract(&right.y)?;
    dx.try_square()?.try_add(&dy.try_square()?)
}

fn directed_candidate(
    source: &PreparedSupport,
    target: &PreparedSupport,
    upper: &Rational,
    context: &mut SearchContext<'_>,
) -> Result<Option<DirectedCandidate>> {
    let (Some(witness), Some(witness_squared)) = (
        context.lower_witness.as_ref(),
        context.lower_squared.as_ref(),
    ) else {
        return Ok(None);
    };

    let center_exact = if witness_squared.is_zero() {
        match source.first_boundary_point()? {
            Some(point) => point,
            None => witness.try_clone()?,
        }
    } else {
        ExactPoint {
            x: Rational::from_f64(witness.x.round()?)?,
            y: Rational::from_f64(witness.y.round()?)?,
        }
    };
    let center = Point::new_raw_xy(center_exact.x.round()?, center_exact.y.round()?);
    if !center.x.is_finite() || !center.y.is_finite() {
        return Ok(None);
    }
    let center_exact = ExactPoint::from_xy(center.xy())?;
    let center_squared = target.distance_squared(&center_exact)?;
    let center_is_valid = if center_squared.is_zero() {
        source.covers_support(&center_exact)?
    } else {
        source.contains_area(&center_exact)?
    };
    if !center_is_valid {
        return Ok(None);
    }

    let movement_squared = exact_point_distance_squared(witness, &center_exact)?;
    let movement_lower = movement_lower_squared(witness_squared, &movement_squared)?;
    let mut certified_squared = center_squared.try_clone()?;
    if movement_lower.try_cmp(&certified_squared)? == Ordering::Less {
        certified_squared = movement_lower;
    }
    let scale_squared = context.output_scale.try_square()?;
    let certified_scaled_squared = certified_squared.try_product(&scale_squared)?;
    let center_scaled_squared = center_squared.try_product(&scale_squared)?;
    let certified_radius = downward_sqrt(&certified_scaled_squared)?;
    let output_radius = downward_sqrt(&center_scaled_squared)?;
    if !(certified_radius.is_finite() && output_radius.is_finite()) {
        return Ok(None);
    }
    let upper_scaled = upper.try_product(&context.output_scale)?;
    let certified_radius = Rational::from_f64(certified_radius)?;
    if upper_scaled.try_cmp(&certified_radius)? == Ordering::Less {
        return Err(crate::geometry::GeometryErrorKind::message(
            "directed inradius upper enclosure fell below its emitted clearance",
        )
        .into());
    }
    let gap = upper_scaled.try_subtract(&certified_radius)?;
    if gap.try_cmp(&context.output_tolerance)? == Ordering::Greater {
        return Ok(None);
    }

    #[cfg(test)]
    let proof = DirectedCandidateProof {
        witness: witness.try_clone()?,
        witness_squared: witness_squared.try_clone()?,
        certified_squared,
        upper: upper.try_clone()?,
    };
    Ok(Some(DirectedCandidate {
        center,
        center_squared,
        #[cfg(test)]
        proof,
    }))
}

fn run_directed(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<DirectedCandidate> {
    context.require_positive_witness = source.has_positive_area()?;
    let mut queue = PatchQueue::new(context.exact_upper);
    for patch in source.inradius_initial_patches()? {
        if let Some(record) = build_patch(patch, source, target, context)? {
            queue.push(record)?;
        }
    }

    loop {
        let upper = queue_upper(&queue, context)?;
        if let Some(upper) = upper.as_ref()
            && let Some(result) = directed_candidate(source, target, upper, context)?
        {
            return Ok(result);
        }
        if queue.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
                "maximum_inscribed_circle",
            ));
        }
        let record = queue.pop().expect("nonempty patch queue");
        if let Some(upper) = &record.upper
            && upper_is_below_lower(upper, context)?
        {
            continue;
        }
        #[cfg(test)]
        count_refinement_work()?;
        let Some(children) = record.patch.split(&mut context.budget)? else {
            return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
                "maximum_inscribed_circle",
            ));
        };
        for patch in children {
            if let Some(child) = build_patch(patch, source, target, context)? {
                queue.push(child)?;
            }
        }
    }
}

/// Search the filled source support against its exact boundary support. The
/// returned upper bound is not widened by the public tolerance; the caller
/// compares it with the radius at the one rounded witness center.
pub(crate) fn directed_inradius_scaled(
    shape: &Shape,
    tolerance: f64,
    output_scale: f64,
) -> Result<DirectedOptimizationResult> {
    if !(tolerance.is_finite() && tolerance > 0.0) {
        return Err(crate::geometry::GeometryErrorKind::message(
            "maximum inscribed circle tolerance must be positive and finite",
        )
        .into());
    }
    if !(output_scale.is_finite() && output_scale > 0.0) {
        return Err(crate::geometry::GeometryErrorKind::message(
            "maximum inscribed circle scale must be positive and finite",
        )
        .into());
    }
    let accuracy = Accuracy::new(Some(1.0), 1.0)?;
    let output_scale = Rational::from_f64(output_scale)?;
    let output_tolerance = Rational::from_f64(tolerance)?;
    let mut context = SearchContext::new_directed(&accuracy, output_scale, output_tolerance);
    let source = PreparedSupport::from_shape_with_boundary(shape, &mut context.budget)?;
    let target = source.boundary_clone()?;
    let candidate = run_directed(&source, &target, &mut context)?;
    Ok(DirectedOptimizationResult {
        center: candidate.center,
        center_squared: candidate.center_squared,
        source,
        boundary: target,
    })
}

pub(crate) fn distance(
    left: &Shape,
    right: &Shape,
    max_error: Option<f64>,
    scale: f64,
) -> Result<f64> {
    let accuracy = Accuracy::new(max_error, scale)?;
    let mut context = SearchContext::new(&accuracy);
    if left.is_empty() || right.is_empty() {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance requires non-empty geometries",
        ));
    }
    let identical = left == right;
    if identical && !left.has_area_parts() {
        return Ok(0.0);
    }
    let source = PreparedSupport::from_shape(left, &mut context.budget)?;
    if identical {
        return Ok(0.0);
    }
    let target = PreparedSupport::from_shape(right, &mut context.budget)?;
    symmetric_distance(&source, &target, &mut context, || left.equals(right))
}

fn symmetric_distance(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
    equal_support: impl FnOnce() -> Result<bool>,
) -> Result<f64> {
    for (source, target) in [(source, target), (target, source)] {
        for span in &source.spans {
            for parameter in [&span.t_lo, &span.t_hi] {
                let point = span_point(span, parameter)?;
                let squared = target.distance_squared(&point)?;
                let squared = context.scaled_squared(&squared)?;
                context.update_lower(squared)?;
            }
        }
        for point in &source.points {
            let squared = target.distance_squared(point)?;
            let squared = context.scaled_squared(&squared)?;
            context.update_lower(squared)?;
        }
    }
    if context
        .lower_squared
        .as_ref()
        .is_some_and(Rational::is_zero)
        && equal_support()?
    {
        return Ok(0.0);
    }
    let forward = run_direction(source, target, context)?;
    let backward = run_direction(target, source, context)?;
    let upper = if forward.try_cmp(&backward)? == Ordering::Greater {
        forward
    } else {
        backward
    };
    let Some(mut candidate) = lower_candidate(context)? else {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance has no finite output within the effective error bound",
        ));
    };
    if candidate == 0.0 {
        candidate = f64::from_bits(1);
    }
    if !candidate_certified(candidate, &upper, context)? {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance cannot represent the effective accuracy",
        ));
    }
    Ok(candidate)
}

pub(crate) fn line_columns(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
    max_error: Option<f64>,
    scale: f64,
) -> Result<f64> {
    let accuracy = Accuracy::new(max_error, scale)?;
    line_columns_with_accuracy(left_xs, left_ys, right_xs, right_ys, &accuracy)
}

fn line_columns_with_accuracy(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
    accuracy: &Accuracy,
) -> Result<f64> {
    let mut context = SearchContext::new(accuracy);
    if left_xs.is_empty() || right_xs.is_empty() {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance requires non-empty geometries",
        ));
    }
    if left_xs == right_xs && left_ys == right_ys {
        return Ok(0.0);
    }
    let source = PreparedSupport::from_line_columns(left_xs, left_ys)?;
    let target = PreparedSupport::from_line_columns(right_xs, right_ys)?;
    symmetric_distance(&source, &target, &mut context, || {
        let line = |xs: &[f64], ys: &[f64]| -> Result<Shape> {
            let mut points = Vec::new();
            try_reserve_vec(&mut points, xs.len())?;
            for (&x, &y) in xs.iter().zip(ys) {
                try_push_vec(&mut points, XY::new(x, y))?;
            }
            Ok(Shape::LineString(LineSeq::try_new(CoordSeq::try_from_xy(
                &points,
            )?)?))
        };
        line(left_xs, left_ys)?.equals(&line(right_xs, right_ys)?)
    })
}

pub(crate) fn line_columns_batch(
    left: PackedLineColumnView<'_>,
    right: PackedLineColumnView<'_>,
    out: &mut [f64],
    max_error: Option<f64>,
    scale: f64,
) -> Result<()> {
    let accuracy = Accuracy::new(max_error, scale)?;
    debug_assert_eq!(left.logical_len(), right.logical_len());
    debug_assert_eq!(left.logical_len(), out.len());
    for (index, value) in out.iter_mut().enumerate() {
        let (left_xs, left_ys) = left.row_xy(index);
        let (right_xs, right_ys) = right.row_xy(index);
        *value = line_columns_with_accuracy(left_xs, left_ys, right_xs, right_ys, &accuracy)?;
    }
    Ok(())
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactPoint {
    x: Rational,
    y: Rational,
}

impl ExactPoint {
    fn from_xy(point: XY) -> crate::error::Result<Self> {
        Ok(Self {
            x: Rational::from_f64(point.x)?,
            y: Rational::from_f64(point.y)?,
        })
    }

    fn from_homogeneous(value: [ExactDyadic; 3]) -> crate::error::Result<Self> {
        let [x, y, w] = value;
        Ok(Self {
            x: Rational::new(x, w.try_clone()?)?,
            y: Rational::new(y, w)?,
        })
    }

    fn try_clone(&self) -> crate::error::Result<Self> {
        Ok(Self {
            x: self.x.try_clone()?,
            y: self.y.try_clone()?,
        })
    }

    #[cfg(any(test, debug_assertions))]
    fn same_position(&self, other: &Self) -> crate::error::Result<bool> {
        Ok(self.x.try_cmp(&other.x)?.is_eq() && self.y.try_cmp(&other.y)?.is_eq())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Point};

    fn line(vertices: &[(f64, f64)]) -> Shape {
        let points: Vec<_> = vertices
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect();
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap())
    }

    fn polygon(vertices: &[(f64, f64)]) -> Shape {
        let points: Vec<_> = vertices.iter().map(|&(x, y)| XY::new(x, y)).collect();
        Shape::Polygon(
            crate::geometry::Polygon::new(
                crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(&points).unwrap()),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    fn directed_candidate_for(shape: &Shape, tolerance: f64, scale: f64) -> DirectedCandidate {
        let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
        let mut context = SearchContext::new_directed(
            &accuracy,
            Rational::from_f64(scale).unwrap(),
            Rational::from_f64(tolerance).unwrap(),
        );
        let source = PreparedSupport::from_shape_with_boundary(shape, &mut context.budget).unwrap();
        let boundary = source.boundary_clone().unwrap();
        run_directed(&source, &boundary, &mut context).unwrap()
    }

    fn test_span(source: Segment) -> SupportSpan {
        let start = ExactPoint::from_xy(source.start).unwrap();
        let end = ExactPoint::from_xy(source.end).unwrap();
        let bounds = FloatBox::from_point(&start)
            .unwrap()
            .include(FloatBox::from_point(&end).unwrap());
        SupportSpan {
            source,
            carrier: ExactCarrier::from_segment(source).unwrap(),
            t_lo: Rational::zero(),
            t_hi: Rational::one(),
            start,
            bounds,
        }
    }

    fn reduced_point_span_distance_squared(
        point: &ExactPoint,
        span: &SupportSpan,
    ) -> Result<Rational> {
        let carrier = &span.carrier;
        let px = point.x.try_subtract(&carrier.ax)?;
        let py = point.y.try_subtract(&carrier.ay)?;
        let dot = px
            .try_product(&carrier.dx)?
            .try_add(&py.try_product(&carrier.dy)?)?;
        let low = span.t_lo.try_product(&carrier.norm_squared)?;
        let high = span.t_hi.try_product(&carrier.norm_squared)?;
        let parameter = if carrier.norm_squared.is_zero() || dot.try_cmp(&low)? == Ordering::Less {
            &span.t_lo
        } else if dot.try_cmp(&high)? == Ordering::Greater {
            &span.t_hi
        } else {
            let cross = px
                .try_product(&carrier.dy)?
                .try_subtract(&py.try_product(&carrier.dx)?)?;
            return cross.try_square()?.try_quotient(&carrier.norm_squared);
        };
        let projected = span_point(span, parameter)?;
        let dx = point.x.try_subtract(&projected.x)?;
        let dy = point.y.try_subtract(&projected.y)?;
        dx.try_square()?.try_add(&dy.try_square()?)
    }

    fn squared(value: f64) -> Rational {
        Rational::from_f64(value)
            .expect("finite value")
            .try_square()
            .expect("squared value")
    }

    fn squared_midpoint(lower: f64, upper: f64) -> Rational {
        let midpoint = ExactDyadic::try_from_f64(lower)
            .expect("finite lower")
            .try_add(ExactDyadic::try_from_f64(upper).expect("finite upper"))
            .expect("midpoint sum")
            .try_product(&ExactDyadic::try_from_f64(0.5).expect("finite half"))
            .expect("midpoint");
        Rational::from_dyadic(midpoint)
            .expect("dyadic midpoint")
            .try_square()
            .expect("squared midpoint")
    }

    #[test]
    fn ordinary_l_shape_refinement_work_stays_bounded() {
        let coordinates = [
            (0.0, 0.0),
            (9.0, 0.0),
            (9.0, 1.0),
            (4.0, 1.0),
            (4.0, 8.0),
            (0.0, 8.0),
            (0.0, 0.0),
        ];
        let source = polygon(&coordinates);
        let target = line(&coordinates);
        let limit = 1_000;
        let _guard = limit_refinement_work(limit);
        let result = distance(&source, &target, Some(1e-12), 1.0);
        let work = refinement_work();
        let actual = result.unwrap_or_else(|error| {
            panic!("ordinary L-shape exceeded {limit} refinements at {work}: {error}")
        });
        assert!(
            (actual - 2.0).abs() <= 1e-12,
            "ordinary L-shape returned {actual}, expected 2.0 within 1e-12"
        );
        assert!(
            work <= limit,
            "ordinary L-shape used {work} refinements, limit {limit}"
        );
    }

    #[test]
    fn ordinary_hausdorff_keeps_external_line_support_with_filled_area() {
        let area = polygon(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)]);
        let external = line(&[(10.0, 0.0), (12.0, 0.0)]);
        let mixed = Shape::geometry_collection(vec![area, external]).unwrap();
        let target = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        assert!((distance(&mixed, &target, Some(1e-9), 1.0).unwrap() - 12.0).abs() <= 1e-9);

        let mut budget = ExpansionBudget::new("hausdorff", "support");
        let prepared = PreparedSupport::from_shape(&mixed, &mut budget).unwrap();
        assert!(prepared.initial_patches().unwrap().iter().any(|patch| {
            matches!(
                patch,
                SourcePatch::Span { span, .. }
                    if prepared.spans[*span].source.start == XY::new(10.0, 0.0)
                        && prepared.spans[*span].source.end == XY::new(12.0, 0.0)
            )
        }));
    }

    #[test]
    fn misaligned_metric_boundary_incidence_is_rejected_before_consumption() {
        for support in [
            ExactMetricSupport {
                spans: Vec::new(),
                points: Vec::new(),
                faces: Vec::new(),
                boundary_spans: Some(vec![true]),
                boundary_points: None,
            },
            ExactMetricSupport {
                spans: Vec::new(),
                points: Vec::new(),
                faces: Vec::new(),
                boundary_spans: None,
                boundary_points: Some(vec![true]),
            },
        ] {
            let error = PreparedSupport::from_metric_support(support).unwrap_err();
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(crate::geometry::GeometryErrorKind::Invalid(_))
            ));
        }
    }

    fn assert_sqrt_certificate(name: &str, squared: &Rational) -> SqrtInterval {
        let interval = SqrtInterval::from_squared(squared).expect("sqrt interval");
        let lower = Rational::from_dyadic(interval.lower.try_clone().expect("lower clone"))
            .expect("lower rational")
            .try_square()
            .expect("lower square");
        assert_ne!(
            lower.try_cmp(squared).expect("lower comparison"),
            Ordering::Greater,
            "{name}: lower endpoint is above the exact root"
        );
        let lower_value = dyadic_as_f64(&interval.lower).expect("lower binary64");
        if let Some(upper) = &interval.upper {
            let upper_squared = Rational::from_dyadic(upper.try_clone().expect("upper clone"))
                .expect("upper rational")
                .try_square()
                .expect("upper square");
            assert_ne!(
                upper_squared.try_cmp(squared).expect("upper comparison"),
                Ordering::Less,
                "{name}: upper endpoint is below the exact root"
            );
            let upper_value = dyadic_as_f64(upper).expect("upper binary64");
            if lower_value.to_bits() != upper_value.to_bits() {
                assert_eq!(
                    upper_value.to_bits() - lower_value.to_bits(),
                    1,
                    "{name}: finite endpoints must be adjacent"
                );
            }
        } else {
            assert_eq!(
                lower_value.to_bits(),
                f64::MAX.to_bits(),
                "{name}: an overflow interval must start at f64::MAX"
            );
            let upper = interval
                .upper_rational()
                .expect("overflow upper")
                .expect("finite exact overflow upper");
            let upper_squared = upper.try_square().expect("overflow upper square");
            assert_ne!(
                upper_squared
                    .try_cmp(squared)
                    .expect("overflow upper comparison"),
                Ordering::Less,
                "{name}: exact overflow upper is below the root"
            );
        }
        interval
    }

    #[test]
    fn verified_sqrt_brackets_cover_irrational_exact_and_boundary_values() {
        reset_sqrt_stats();
        let minimum_subnormal = f64::from_bits(1);
        let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
        let minimum_normal = f64::from_bits(0x0010_0000_0000_0000);
        let maximum = f64::MAX;
        let below_minimum = ExactDyadic::from_f64(minimum_subnormal)
            .try_product(&ExactDyadic::from_f64(0.5))
            .unwrap();
        let underflowing_squared = squared(1.5e-162);
        let cases = [
            ("irrational two", Rational::from_f64(2.0).unwrap()),
            ("exact square", squared(3.0)),
            (
                "normal rounding midpoint",
                squared_midpoint(1.0, 1.0_f64.next_up()),
            ),
            (
                "minimum subnormal rounding midpoint",
                squared_midpoint(minimum_subnormal, f64::from_bits(2)),
            ),
            (
                "subnormal normal rounding midpoint",
                squared_midpoint(largest_subnormal, minimum_normal),
            ),
            ("minimum subnormal root", squared(minimum_subnormal)),
            ("minimum normal root", squared(minimum_normal)),
            ("rounded squared underflow", underflowing_squared),
            (
                "root below minimum subnormal",
                Rational::from_dyadic(below_minimum)
                    .unwrap()
                    .try_square()
                    .unwrap(),
            ),
            ("maximum square", squared(maximum)),
            (
                "root just beyond maximum",
                Rational::from_dyadic(
                    ExactDyadic::from_f64(maximum)
                        .try_add(ExactDyadic::from_f64(2.0_f64.powi(969)))
                        .unwrap(),
                )
                .unwrap()
                .try_square()
                .unwrap(),
            ),
            (
                "root at overflow rounding midpoint",
                Rational::from_dyadic(
                    ExactDyadic::from_f64(maximum)
                        .try_add(ExactDyadic::from_f64(2.0_f64.powi(970)))
                        .unwrap(),
                )
                .unwrap()
                .try_square()
                .unwrap(),
            ),
        ];
        for (name, value) in &cases {
            assert_sqrt_certificate(name, value);
        }
        assert!(
            sqrt_stats().fast_admissions > 0,
            "ordinary cases should admit the verified candidate"
        );
        assert!(
            sqrt_stats().fallbacks > 0,
            "underflow and overflow cases should use the exact fallback"
        );
    }

    #[test]
    fn endpoint_verification_rejects_a_reversed_sqrt_comparison() {
        reset_sqrt_stats();
        let squared = Rational::from_f64(2.0).unwrap();
        let proposal = 2.0_f64.sqrt();
        assert_eq!(
            squared
                .try_cmp(&square_f64(proposal).expect("proposal square"))
                .unwrap(),
            Ordering::Less,
            "the deliberately chosen libm proposal is above the exact root"
        );
        assert_sqrt_certificate("reversed-comparison negative control", &squared);
        assert_eq!(sqrt_stats().fast_admissions, 1);
    }

    #[test]
    fn verified_sqrt_candidate_matches_complete_rounding_at_extrema() {
        let minimum_subnormal = f64::from_bits(1);
        let largest_subnormal = f64::from_bits(0x000F_FFFF_FFFF_FFFF);
        let minimum_normal = f64::from_bits(0x0010_0000_0000_0000);
        let maximum = f64::MAX;
        let below_minimum = ExactDyadic::from_f64(minimum_subnormal)
            .try_product(&ExactDyadic::from_f64(0.5))
            .unwrap();
        let cases = [
            Rational::zero(),
            Rational::from_f64(2.0).unwrap(),
            Rational::from_f64(3.0).unwrap(),
            Rational::from_f64(minimum_subnormal).unwrap(),
            Rational::from_f64(minimum_normal).unwrap(),
            Rational::from_f64(maximum).unwrap(),
            squared(minimum_subnormal),
            squared(minimum_normal),
            squared_midpoint(1.0, 1.0_f64.next_up()),
            squared_midpoint(minimum_subnormal, f64::from_bits(2)),
            squared_midpoint(largest_subnormal, minimum_normal),
            Rational::from_dyadic(below_minimum)
                .unwrap()
                .try_square()
                .unwrap(),
            squared(maximum),
            Rational::from_dyadic(
                ExactDyadic::from_f64(maximum)
                    .try_add(ExactDyadic::from_f64(2.0_f64.powi(969)))
                    .unwrap(),
            )
            .unwrap()
            .try_square()
            .unwrap(),
            Rational::from_dyadic(
                ExactDyadic::from_f64(maximum)
                    .try_add(ExactDyadic::from_f64(2.0_f64.powi(970)))
                    .unwrap(),
            )
            .unwrap()
            .try_square()
            .unwrap(),
        ];
        for value in &cases {
            let actual = SqrtInterval::candidate_from_squared(value).unwrap();
            let expected = value.round_sqrt().unwrap();
            if expected.is_infinite() {
                assert_eq!(
                    actual.map(f64::to_bits),
                    Some(f64::MAX.to_bits()),
                    "a finite tolerance candidate must retain f64::MAX above its range"
                );
            } else {
                assert_eq!(
                    actual.map(f64::to_bits),
                    Some(expected.to_bits()),
                    "candidate differs from complete exact rounding"
                );
            }
        }
    }

    #[test]
    fn fast_and_complete_sqrt_paths_have_identical_bounded_results() {
        let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let diagonal = Shape::Point(Point::new_raw_xy(1.0, 1.0));
        let source_line = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let target_line = line(&[(0.0, 1.0), (2.0, 1.0)]);
        let triangle = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
        let tiny = Shape::Point(Point::new_raw_xy(1.5e-162, 0.0));
        let length = 10.0_f64.powi(155);
        let height = 10.0_f64.powi(-155);
        let reciprocal_baseline = line(&[(0.0, 0.0), (length, 0.0)]);
        let reciprocal_bend = line(&[
            (0.0, height),
            (length / 2.0, 2.0 * height),
            (length, height),
        ]);
        let cases = [
            (&origin, &diagonal, 1e-6, 1.0),
            (&source_line, &target_line, 1e-6, 1.0),
            (&triangle, &source_line, 1e-6, 1.0),
            (&origin, &tiny, 1e-174, 1.0),
            (
                &reciprocal_baseline,
                &reciprocal_bend,
                2.0 * height * 1e-12,
                1.0,
            ),
        ];

        reset_sqrt_stats();
        let fast: Vec<_> = cases
            .iter()
            .map(|(left, right, error, scale)| {
                let _path = force_exact_sqrt_path(false);
                distance(left, right, Some(*error), *scale).expect("fast bounded result")
            })
            .collect();
        let fast_stats = sqrt_stats();
        assert!(fast_stats.fast_admissions > 0);
        assert!(fast_stats.fallbacks > 0);

        reset_sqrt_stats();
        let complete: Vec<_> = cases
            .iter()
            .map(|(left, right, error, scale)| {
                let _path = force_exact_sqrt_path(true);
                distance(left, right, Some(*error), *scale).expect("complete bounded result")
            })
            .collect();
        let complete_stats = sqrt_stats();
        assert_eq!(complete_stats.fast_admissions, 0);
        assert!(complete_stats.fallbacks > 0);
        assert_eq!(
            fast.iter().map(|value| value.to_bits()).collect::<Vec<_>>(),
            complete
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn queue_enclosure_keeps_unbounded_patches_and_later_witnesses() {
        let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
        let mut context = SearchContext::new(&accuracy);
        let mut queue = PatchQueue::new(false);
        let patch = || {
            SourcePatch::Box(FloatBox {
                min_x: 0.0,
                max_x: 1.0,
                min_y: 0.0,
                max_y: 1.0,
            })
        };
        queue
            .push(PatchRecord {
                patch: patch(),
                upper: Some(Rational::one()),
            })
            .unwrap();
        context
            .update_lower(Rational::from_f64(4.0).unwrap())
            .unwrap();
        let upper = queue_upper(&queue, &mut context).unwrap().unwrap();
        assert_ne!(
            upper.try_cmp(&Rational::from_f64(2.0).unwrap()).unwrap(),
            Ordering::Less
        );
        queue
            .push(PatchRecord {
                patch: patch(),
                upper: None,
            })
            .unwrap();
        assert!(queue_upper(&queue, &mut context).unwrap().is_none());
    }

    #[test]
    fn box_radius_covers_unrepresentable_midpoint_corners() {
        let source = PreparedSupport::from_line_columns(&[0.0], &[0.0]).unwrap();
        let patch = SourcePatch::Box(FloatBox {
            min_x: 1.0,
            max_x: 1.0_f64.next_up(),
            min_y: 0.0,
            max_y: f64::from_bits(1),
        });
        let (center, radius) = patch.center_and_radius_squared(&source).unwrap();
        for corner in patch.corners(&source).unwrap() {
            let dx = corner.x.try_subtract(&center.x).unwrap();
            let dy = corner.y.try_subtract(&center.y).unwrap();
            let squared = dx
                .try_square()
                .unwrap()
                .try_add(&dy.try_square().unwrap())
                .unwrap();
            assert_ne!(squared.try_cmp(&radius).unwrap(), Ordering::Greater);
        }
    }

    #[test]
    fn floating_box_pruning_stays_below_exact_distance() {
        let target = FloatBox {
            min_x: -2.0,
            max_x: -1.0,
            min_y: 1.0,
            max_y: 2.0,
        };
        for (x, y) in [
            (0.0, 0.0),
            (0.1, -0.3),
            (f64::from_bits(1), 0.0),
            (f64::MAX, -f64::MAX),
        ] {
            let point = exact_point_xy(x, y).unwrap();
            let lower = box_distance_lower(FloatBox::from_point(&point).unwrap(), target);
            let exact = aabb_lower_squared(&point, target).unwrap();
            assert_ne!(
                Rational::from_f64(lower).unwrap().try_cmp(&exact).unwrap(),
                Ordering::Greater,
            );
        }
    }

    #[test]
    fn empty_area_patch_cannot_invent_a_source_witness() {
        let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
        let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
        let mut context = SearchContext::new(&accuracy);
        let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
        let target = PreparedSupport::from_line_columns(&[1.0], &[1.0]).unwrap();
        let patch = SourcePatch::Box(FloatBox {
            min_x: 10.0,
            max_x: 11.0,
            min_y: 10.0,
            max_y: 11.0,
        });
        assert!(
            build_patch(patch, &source, &target, &mut context)
                .unwrap()
                .is_none()
        );
        assert!(context.lower_squared.is_none());
        let span = SourcePatch::Span {
            span: 0,
            lo: Rational::zero(),
            hi: Rational::one(),
        };
        assert!(
            !span
                .witness_is_valid(&source, &exact_point_xy(10.0, 10.0).unwrap())
                .unwrap()
        );
    }

    #[test]
    fn subdivision_uses_the_shared_generated_work_budget() {
        let mut budget = ExpansionBudget::new("hausdorff_distance", "max_error");
        budget
            .add(crate::geometry::generated_budget::GENERATED_ITEM_LIMIT)
            .unwrap();
        let patch = SourcePatch::Box(FloatBox {
            min_x: 0.0,
            max_x: 1.0,
            min_y: 0.0,
            max_y: 1.0,
        });
        let error = patch.split(&mut budget).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge { .. }
            )
        ));
    }

    #[test]
    fn box_subdivision_continues_on_the_representable_axis() {
        let mut budget = ExpansionBudget::new("hausdorff_distance", "max_error");
        let min_x = 1.0_f64;
        let max_x = min_x.next_up();
        let children = SourcePatch::Box(FloatBox {
            min_x,
            max_x,
            min_y: 0.0,
            max_y: 2.0,
        })
        .split(&mut budget)
        .unwrap()
        .unwrap();
        assert_eq!(children.len(), 2);
        for (index, child) in children.into_iter().enumerate() {
            let SourcePatch::Box(bounds) = child else {
                panic!("box split produced a span");
            };
            assert_eq!((bounds.min_x, bounds.max_x), (min_x, max_x));
            assert_eq!(
                (bounds.min_y, bounds.max_y),
                if index == 0 { (0.0, 1.0) } else { (1.0, 2.0) }
            );
        }
    }

    #[test]
    fn parallel_strip_rejects_outside_points_with_reversed_carriers() {
        let target =
            PreparedSupport::from_line_columns(&[0.0, 4.0, 4.0, 0.0], &[0.0, 0.0, 2.0, 2.0])
                .unwrap();
        let source = PreparedSupport::from_line_columns(&[2.0], &[1.0]).unwrap();
        let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
        let mut context = SearchContext::new(&accuracy);
        for (y, inside) in [(1.0, true), (3.0, false), (-1.0, false)] {
            let patch = SourcePatch::Box(FloatBox {
                min_x: 2.0,
                max_x: 2.0,
                min_y: y,
                max_y: y,
            });
            let upper = parallel_strip_upper(
                &patch,
                &source,
                &target,
                &[PrimitiveId::Span(0), PrimitiveId::Span(2)],
                &mut context,
            )
            .unwrap();
            if inside {
                assert_eq!(
                    upper.unwrap().try_cmp(&Rational::one()).unwrap(),
                    Ordering::Equal
                );
            } else {
                assert!(upper.is_none());
            }
        }
    }

    #[test]
    fn supporting_line_lower_stays_below_exact_segment_distance() {
        let subnormal = f64::from_bits(1);
        let cases = [
            (
                Segment {
                    start: XY::new(0.0, 0.0),
                    end: XY::new(3.0, 4.0),
                },
                XY::new(2.0, -1.0),
            ),
            (
                Segment {
                    start: XY::new(1.0e200, -1.0e200),
                    end: XY::new(2.0e200, 1.0e200),
                },
                XY::new(1.5e200, 0.5e200),
            ),
            (
                Segment {
                    start: XY::new(0.0, 0.0),
                    end: XY::new(8.0 * subnormal, 0.0),
                },
                XY::new(4.0 * subnormal, 8.0 * subnormal),
            ),
            (
                Segment {
                    start: XY::new(0.0, 0.0),
                    end: XY::new(1.0, 1.0),
                },
                XY::new(0.5, 0.5 + 2.0_f64.powi(-52)),
            ),
            (
                Segment {
                    start: XY::new(4.0, 4.0),
                    end: XY::new(-2.0, -1.0),
                },
                XY::new(0.0, 5.0),
            ),
        ];
        let third = Rational::new(ExactDyadic::from_f64(1.0), ExactDyadic::from_f64(3.0)).unwrap();

        for (source, query) in cases {
            let span = test_span(source);
            let stored = ExactPoint::from_xy(query).unwrap();
            let subdivision = span_point(&span, &third).unwrap();
            for point in [stored, subdivision] {
                let lower = Rational::from_f64(supporting_line_lower_squared(
                    FloatBox::from_point(&point).unwrap(),
                    &span,
                ))
                .unwrap();
                let exact = point_span_distance_squared(&point, &span)
                    .unwrap()
                    .to_rational()
                    .unwrap();
                assert_ne!(lower.try_cmp(&exact).unwrap(), Ordering::Greater);
            }
        }
    }

    #[test]
    fn line_filter_skips_only_after_an_exact_best_is_known() {
        let target = PreparedSupport::from_parts(
            vec![
                test_span(Segment {
                    start: XY::new(0.0, 1.0),
                    end: XY::new(10.0, 1.0),
                }),
                test_span(Segment {
                    start: XY::new(0.0, 10.0),
                    end: XY::new(10.0, 0.0),
                }),
            ],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let point = exact_point_xy(5.0, 0.0).unwrap();
        let run = |enabled| {
            let _guard = set_line_filter_enabled(enabled);
            reset_exact_span_evaluations();
            let result = target.distance_squared(&point).unwrap();
            (result, exact_span_evaluations())
        };

        let (without_filter, without_filter_evaluations) = run(false);
        let (with_filter, with_filter_evaluations) = run(true);
        assert_eq!(
            with_filter.try_cmp(&without_filter).unwrap(),
            Ordering::Equal
        );
        assert_eq!(without_filter_evaluations, 2);
        assert_eq!(with_filter_evaluations, 1);
    }

    #[test]
    fn unreduced_distances_match_reduced_point_and_span_references() {
        let point_support = PreparedSupport::from_line_columns(&[0.0], &[0.0]).unwrap();
        for point in [
            exact_point_xy(3.0, 4.0).unwrap(),
            exact_point_xy(2.0_f64.powi(500), -2.0_f64.powi(500)).unwrap(),
            exact_point_xy(f64::from_bits(1), -f64::from_bits(2)).unwrap(),
        ] {
            let actual = point_support
                .primitive_distance_squared(PrimitiveId::Point(0), &point)
                .unwrap();
            let dx = point.x.try_subtract(&point_support.points[0].x).unwrap();
            let dy = point.y.try_subtract(&point_support.points[0].y).unwrap();
            let expected = dx
                .try_square()
                .unwrap()
                .try_add(&dy.try_square().unwrap())
                .unwrap();
            assert_eq!(actual.try_cmp_rational(&expected).unwrap(), Ordering::Equal);
        }

        let cases = [
            ([0.0, 3.0], [0.0, 4.0], (0.0, -1.0)),
            ([0.0, 3.0], [0.0, 4.0], (1.0, 3.0)),
            ([3.0, 0.0], [4.0, 0.0], (1.0, 3.0)),
            (
                [0.0, 2.0_f64.powi(500)],
                [0.0, 0.0],
                (0.0, 2.0_f64.powi(500)),
            ),
            (
                [0.0, f64::from_bits(2)],
                [0.0, 0.0],
                (0.0, f64::from_bits(1)),
            ),
        ];
        for (xs, ys, point) in cases {
            let support = PreparedSupport::from_line_columns(&xs, &ys).unwrap();
            let point = exact_point_xy(point.0, point.1).unwrap();
            let actual = point_span_distance_squared(&point, &support.spans[0]).unwrap();
            let expected = reduced_point_span_distance_squared(&point, &support.spans[0]).unwrap();
            assert_eq!(actual.try_cmp_rational(&expected).unwrap(), Ordering::Equal);
        }

        let mut subspan = PreparedSupport::from_line_columns(&[0.0, 6.0], &[0.0, 0.0]).unwrap();
        subspan.spans[0].t_lo = Rational::new(
            ExactDyadic::try_from_f64(1.0).unwrap(),
            ExactDyadic::try_from_f64(3.0).unwrap(),
        )
        .unwrap();
        subspan.spans[0].t_hi = Rational::new(
            ExactDyadic::try_from_f64(2.0).unwrap(),
            ExactDyadic::try_from_f64(3.0).unwrap(),
        )
        .unwrap();
        let point = exact_point_xy(0.0, 0.0).unwrap();
        let actual = point_span_distance_squared(&point, &subspan.spans[0]).unwrap();
        let expected = reduced_point_span_distance_squared(&point, &subspan.spans[0]).unwrap();
        assert_eq!(actual.try_cmp_rational(&expected).unwrap(), Ordering::Equal);
    }

    #[test]
    fn unreduced_distance_allocation_failures_propagate() {
        let support =
            PreparedSupport::from_line_columns(&[0.0, f64::MAX], &[0.0, f64::from_bits(2)])
                .unwrap();
        let point = exact_point_xy(f64::from_bits(1), f64::MAX).unwrap();
        let (result, allocations) = crate::test_support::allocation_count(|| {
            support.primitive_distance_squared(PrimitiveId::Span(0), &point)
        });
        result.unwrap();
        assert!(allocations > 0);
        for allowed in 0..allocations {
            let error = crate::test_support::fail_allocations_after(allowed, || {
                support.primitive_distance_squared(PrimitiveId::Span(0), &point)
            })
            .expect_err("every unreduced distance allocation must propagate");
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn selected_span_clamps_oblique_projection() {
        let mut support = PreparedSupport::from_line_columns(&[0.0, 3.0], &[0.0, 4.0]).unwrap();
        for (lo, hi, x, y, expected) in [
            (0.0, 1.0, 1.0, 3.0, 1.0),
            (0.0, 1.0, -3.0, -4.0, 25.0),
            (0.0, 1.0, 6.0, 8.0, 25.0),
            (0.25, 0.75, 0.0, 0.0, 1.5625),
            (0.25, 0.75, 3.0, 4.0, 1.5625),
        ] {
            support.spans[0].t_lo = Rational::from_f64(lo).unwrap();
            support.spans[0].t_hi = Rational::from_f64(hi).unwrap();
            let value =
                point_span_distance_squared(&exact_point_xy(x, y).unwrap(), &support.spans[0])
                    .unwrap();
            assert_eq!(
                value
                    .try_cmp_rational(&Rational::from_f64(expected).unwrap())
                    .unwrap(),
                Ordering::Equal
            );
        }
    }

    #[test]
    fn filled_target_membership_and_holes_remain_selected() {
        let ring = |points: &[XY]| {
            crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap())
        };
        let shell = [
            XY::new(0.0, 0.0),
            XY::new(4.0, 0.0),
            XY::new(4.0, 4.0),
            XY::new(0.0, 4.0),
            XY::new(0.0, 0.0),
        ];
        let target = Shape::Polygon(
            crate::geometry::Polygon::new(ring(&shell), vec![
                ring(&[
                    XY::new(1.0, 1.0),
                    XY::new(3.0, 1.0),
                    XY::new(3.0, 3.0),
                    XY::new(1.0, 3.0),
                    XY::new(1.0, 1.0),
                ]),
                ring(&[
                    XY::new(6.0, 6.0),
                    XY::new(7.0, 6.0),
                    XY::new(7.0, 7.0),
                    XY::new(6.0, 7.0),
                    XY::new(6.0, 6.0),
                ]),
            ])
            .unwrap(),
        );
        let mut budget = ExpansionBudget::new("test", "support");
        let support = PreparedSupport::from_shape(&target, &mut budget).unwrap();
        assert!(
            support
                .distance_squared(&exact_point_xy(0.5, 0.5).unwrap())
                .unwrap()
                .is_zero()
        );
        assert_eq!(
            support
                .distance_squared(&exact_point_xy(2.0, 2.0).unwrap())
                .unwrap()
                .round()
                .unwrap(),
            1.0
        );
        let filled =
            Shape::Polygon(crate::geometry::Polygon::new(ring(&shell), Vec::new()).unwrap());
        assert!((distance(&filled, &target, Some(1e-6), 1.0).unwrap() - 1.0).abs() <= 1e-6);
    }

    #[test]
    fn directed_inradius_retains_the_rounded_center_and_global_enclosure() {
        let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let result = directed_candidate_for(&shape, 1e-6, 1.0);
        assert_eq!(result.proof.witness_squared.round().unwrap(), 4.0);
        assert_eq!((result.center.x, result.center.y), (2.0, 2.0));
        assert_eq!(result.center_squared.round().unwrap(), 4.0);
        assert_eq!(result.proof.certified_squared.round().unwrap(), 4.0);
        assert_ne!(
            result
                .proof
                .upper
                .try_cmp(&Rational::from_f64(2.0 + 1e-6).unwrap())
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            (
                result.proof.witness.x.round().unwrap(),
                result.proof.witness.y.round().unwrap()
            ),
            (2.0, 2.0)
        );
    }

    #[test]
    fn directed_inradius_uses_a_stable_center_on_a_parallel_plateau() {
        let shape = polygon(&[(0.0, 0.0), (8.0, 0.0), (8.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let first = directed_candidate_for(&shape, 1e-9, 1.0);
        let second = directed_candidate_for(&shape, 1e-9, 1.0);
        assert_eq!(
            (first.center.x.to_bits(), first.center.y.to_bits()),
            (second.center.x.to_bits(), second.center.y.to_bits())
        );
        assert_eq!(
            (
                first.proof.witness.x.round().unwrap(),
                first.proof.witness.y.round().unwrap()
            ),
            (
                second.proof.witness.x.round().unwrap(),
                second.proof.witness.y.round().unwrap()
            )
        );
        assert_eq!(first.center.x, 4.0);
        assert_eq!(first.center.y, 2.0);
        assert_eq!(
            first
                .proof
                .upper
                .try_cmp(&Rational::from_f64(2.0 + 1e-9).unwrap())
                .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn directed_inradius_selects_the_first_equal_multipart_winner() {
        let shape = crate::io::parse_wkt(
            "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),\
                           ((10 0,14 0,14 4,10 4,10 0)))",
        )
        .unwrap();
        let result = directed_candidate_for(&shape, 1e-9, 1.0);
        assert!(result.center.x < 4.0 && result.center.y > 0.0 && result.center.y < 4.0);
        assert_eq!(
            result
                .proof
                .upper
                .try_cmp(&Rational::from_f64(2.0 + 1e-9).unwrap())
                .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn directed_inradius_accounts_for_rounding_an_exact_witness_center() {
        let shape = polygon(&[
            (1.0, 0.0),
            (2.0_f64.powi(60), 0.0),
            (2.0_f64.powi(60), 4.0),
            (1.0, 4.0),
            (1.0, 0.0),
        ]);
        let result = directed_candidate_for(&shape, 1.0, 1.0);
        let rounded = ExactPoint::from_xy(result.center.xy()).unwrap();
        assert!(
            result.proof.witness.x.try_cmp(&rounded.x).unwrap() != Ordering::Equal
                || result.proof.witness.y.try_cmp(&rounded.y).unwrap() != Ordering::Equal
        );
        assert_eq!(
            result
                .proof
                .certified_squared
                .try_cmp(&result.center_squared)
                .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn directed_inradius_checks_the_final_output_scale_before_stopping() {
        let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let scale_value = 0.1;
        let tolerance = 1e-12;
        let result = directed_candidate_for(&shape, tolerance, scale_value);
        let scale = Rational::from_f64(scale_value).unwrap();
        let radius = downward_sqrt(
            &result
                .center_squared
                .try_product(&scale.try_square().unwrap())
                .unwrap(),
        )
        .unwrap();
        let upper = result.proof.upper.try_product(&scale).unwrap();
        let gap = upper
            .try_subtract(&Rational::from_f64(radius).unwrap())
            .unwrap();
        assert_ne!(
            gap.try_cmp(&Rational::from_f64(tolerance).unwrap())
                .unwrap(),
            Ordering::Greater
        );
        assert!(radius <= 0.2);
    }

    #[test]
    fn boundary_clone_keeps_filled_source_separate_from_boundary_target() {
        let ring = |points: &[XY]| {
            crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap())
        };
        let shape = Shape::Polygon(
            crate::geometry::Polygon::new(
                ring(&[
                    XY::new(0.0, 0.0),
                    XY::new(4.0, 0.0),
                    XY::new(4.0, 4.0),
                    XY::new(0.0, 4.0),
                    XY::new(0.0, 0.0),
                ]),
                vec![ring(&[
                    XY::new(1.0, 1.0),
                    XY::new(3.0, 1.0),
                    XY::new(3.0, 3.0),
                    XY::new(1.0, 3.0),
                    XY::new(1.0, 1.0),
                ])],
            )
            .unwrap(),
        );
        let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
        let mut context = SearchContext::new(&accuracy);
        let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
        let target = source.boundary_clone().unwrap();
        let center = exact_point_xy(0.5, 0.5).unwrap();
        assert!(!source.faces.is_empty());
        assert!(target.faces.is_empty());
        assert!(source.distance_squared(&center).unwrap().is_zero());
        assert_eq!(
            target.distance_squared(&center).unwrap().round().unwrap(),
            0.25
        );
    }

    #[test]
    fn selected_source_candidate_proof_checks_vertices_chords_and_hole_cycles() {
        let ring = |points: &[XY]| {
            crate::geometry::Ring::from_closed(CoordSeq::try_from_xy(points).unwrap())
        };
        let shape = Shape::Polygon(
            crate::geometry::Polygon::new(
                ring(&[
                    XY::new(-100.0, -100.0),
                    XY::new(100.0, -100.0),
                    XY::new(100.0, 100.0),
                    XY::new(-100.0, 100.0),
                    XY::new(-100.0, -100.0),
                ]),
                vec![ring(&[
                    XY::new(-2.0, 8.0),
                    XY::new(2.0, 8.0),
                    XY::new(2.0, 10.0),
                    XY::new(-2.0, 10.0),
                    XY::new(-2.0, 8.0),
                ])],
            )
            .unwrap(),
        );
        let accuracy = Accuracy::new(Some(1e-6), 1.0).unwrap();
        let mut context = SearchContext::new(&accuracy);
        let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
        let boundary = source.boundary_clone().unwrap();
        let valid = [
            Point::new_raw_xy(3.0, 2.0),
            Point::new_raw_xy(2.0, 3.0),
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(2.0, 1.0),
        ];
        assert!(
            source
                .candidate_is_contained(&boundary, Point::new_raw_xy(2.0, 2.0), &valid, true)
                .unwrap()
        );
        let hole_enclosing = [
            Point::new_raw_xy(12.0, 0.0),
            Point::new_raw_xy(0.0, 12.0),
            Point::new_raw_xy(-12.0, 0.0),
            Point::new_raw_xy(0.0, -12.0),
        ];
        assert!(
            !source
                .candidate_is_contained(
                    &boundary,
                    Point::new_raw_xy(0.0, 0.0),
                    &hole_enclosing,
                    true,
                )
                .unwrap()
        );
    }

    #[test]
    fn boundary_target_cancels_shared_edges_of_filled_multipart_support() {
        let shape = crate::io::parse_wkt(
            "MULTIPOLYGON (((0 0,10 0,10 10,0 10,0 0),\
                            (20 20,22 20,22 22,20 22,20 20)),\
                           ((15 15,30 15,30 30,15 30,15 15)))",
        )
        .unwrap();
        let mut budget = ExpansionBudget::new("test", "support");
        let source = PreparedSupport::from_shape_with_boundary(&shape, &mut budget).unwrap();
        let target = source.boundary_clone().unwrap();
        assert_eq!(target.spans.len(), 8);
        let result = directed_inradius_scaled(&shape, 1e-9, 1.0).unwrap();
        assert_eq!((result.center.x, result.center.y), (22.5, 22.5));
        assert_eq!(downward_sqrt(&result.center_squared).unwrap(), 7.5);
        let center = ExactPoint::from_xy(result.center.xy()).unwrap();
        assert_eq!(
            result
                .boundary
                .distance_squared(&center)
                .unwrap()
                .try_cmp(&result.center_squared)
                .unwrap(),
            Ordering::Equal
        );
        assert!(result.source.contains_area(&center).unwrap());
    }

    #[test]
    fn directed_inradius_ignores_collapsed_support_inside_positive_area() {
        let outer = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_xy(&[XY::new(1.0, 1.0), XY::new(3.0, 1.0), XY::new(1.0, 1.0)])
                    .unwrap(),
            )
            .unwrap(),
        );
        let shape = Shape::geometry_collection(vec![
            outer,
            line,
            Shape::Point(Point::new_raw_xy(2.0, 2.0)),
        ])
        .unwrap();
        let result = directed_inradius_scaled(&shape, 1e-9, 1.0).unwrap();
        assert_eq!(downward_sqrt(&result.center_squared).unwrap(), 2.0);
        assert!(
            result
                .source
                .contains_area(&ExactPoint::from_xy(result.center.xy()).unwrap())
                .unwrap()
        );
    }

    #[test]
    fn directed_inradius_propagates_allocation_failure_before_publication() {
        let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let (result, allocations) =
            crate::test_support::allocation_count(|| directed_inradius_scaled(&shape, 1e-6, 1.0));
        result.unwrap();
        assert!(allocations > 0);
        for allowed in 0..allocations.min(16) {
            let error = crate::test_support::fail_allocations_after(allowed, || {
                directed_inradius_scaled(&shape, 1e-6, 1.0)
            })
            .expect_err("a rejected directed-search allocation must not publish a result");
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
            ));
        }
    }

    #[test]
    fn directed_inradius_propagates_the_subdivision_budget() {
        let shape = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 4.0), (0.0, 0.0)]);
        let accuracy = Accuracy::new(Some(1.0), 1.0).unwrap();
        let mut context = SearchContext::new_directed(&accuracy, Rational::one(), Rational::one());
        let source = PreparedSupport::from_shape(&shape, &mut context.budget).unwrap();
        let target = source.boundary_clone().unwrap();
        context
            .budget
            .add(crate::geometry::generated_budget::GENERATED_ITEM_LIMIT)
            .unwrap();
        let error = run_directed(&source, &target, &mut context).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                    operation: "maximum_inscribed_circle",
                    parameter: "tolerance",
                    ..
                }
            )
        ));
    }

    #[test]
    fn multipart_area_boxes_and_mixed_points_use_only_source_witnesses() {
        let multipart = crate::io::parse_wkt(
            "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),\
                           ((3 3,5 3,5 5,3 5,3 3)))",
        )
        .unwrap();
        for ((x, y), expected_squared) in [((3.0, 1.0), 20.0), ((5.0, 3.0), 34.0)] {
            let target = Shape::Point(Point::new_raw_xy(x, y));
            let actual = distance(&multipart, &target, Some(1e-12), 1.0).unwrap();
            assert!((actual - f64::sqrt(expected_squared)).abs() <= 1e-12);
        }

        let mixed = Shape::geometry_collection(vec![
            line(&[(0.0, 0.0), (1.0, 0.0)]),
            Shape::Point(Point::new_raw_xy(10.0, 0.0)),
        ])
        .unwrap();
        let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        assert!((distance(&mixed, &origin, Some(1e-12), 1.0).unwrap() - 10.0).abs() <= 1e-12);
    }

    #[test]
    fn nested_island_target_fill_is_part_of_the_closed_support() {
        let source = crate::io::parse_wkt(
            "MULTILINESTRING (\
             (0 0,10 0,10 10,0 10,0 0),\
             (2 2,8 2,8 8,2 8,2 2))",
        )
        .unwrap();
        let target = crate::io::parse_wkt(
            "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,8 2,8 8,2 8,2 2)),\
             POLYGON ((4 4,6 4,6 6,4 6,4 4)))",
        )
        .unwrap();
        assert!((distance(&source, &target, Some(1e-12), 1.0).unwrap() - 3.0).abs() <= 1e-12);
    }

    #[test]
    fn metric_support_consumption_preserves_multipart_faces_and_mixed_points() {
        for (wkt, inside, outside) in [
            (
                "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),\
                               ((2 0,4 0,4 2,2 2,2 0)))",
                [(1.0, 1.0), (3.0, 1.0), (2.0, 1.0)],
                [(5.0, 1.0), (2.0, 3.0), (-1.0, 1.0)],
            ),
            (
                "MULTIPOLYGON (((0 0,3 0,3 3,0 3,0 0)),\
                               ((2 0,5 0,5 3,2 3,2 0)))",
                [(1.0, 1.0), (2.5, 1.0), (4.0, 1.0)],
                [(6.0, 1.0), (2.5, 4.0), (-1.0, 1.0)],
            ),
        ] {
            let shape = crate::io::parse_wkt(wkt).unwrap();
            let mut budget = ExpansionBudget::new("test", "metric support");
            let support = PreparedSupport::from_shape(&shape, &mut budget).unwrap();
            assert!(!support.faces.is_empty(), "{wkt}");
            for (x, y) in inside {
                assert!(
                    support
                        .contains_area(&exact_point_xy(x, y).unwrap())
                        .unwrap(),
                    "expected ({x}, {y}) inside {wkt}"
                );
            }
            for (x, y) in outside {
                assert!(
                    !support
                        .contains_area(&exact_point_xy(x, y).unwrap())
                        .unwrap(),
                    "expected ({x}, {y}) outside {wkt}"
                );
            }
        }

        let shape = crate::io::parse_wkt(
            "GEOMETRYCOLLECTION(\
             POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
             POINT (10 1))",
        )
        .unwrap();
        let mut budget = ExpansionBudget::new("test", "metric support");
        let support = PreparedSupport::from_shape(&shape, &mut budget).unwrap();
        assert_eq!(support.spans.len(), 4);
        assert_eq!(support.points.len(), 1);
        assert_eq!(support.faces.len(), 1);
        assert_eq!(support.faces[0].len(), 1);
        assert!(
            support
                .contains_area(&exact_point_xy(2.0, 2.0).unwrap())
                .unwrap()
        );
        assert!(
            !support
                .contains_area(&exact_point_xy(10.0, 1.0).unwrap())
                .unwrap()
        );
        assert!(support.points.iter().any(|point| {
            point
                .same_position(&exact_point_xy(10.0, 1.0).unwrap())
                .unwrap()
        }));
    }

    #[test]
    fn allocation_failure_never_publishes_a_scalar() {
        let source = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let target = Shape::Point(Point::new_raw_xy(1.0, 1.0));
        let (result, count) =
            crate::test_support::allocation_count(|| distance(&source, &target, Some(1e-6), 1.0));
        result.unwrap();
        assert!(count > 0);
        for allowed in 0..=count {
            match crate::test_support::fail_allocations_after(allowed, || {
                distance(&source, &target, Some(1e-6), 1.0)
            }) {
                Err(error) => assert!(matches!(
                    error.kind(),
                    crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
                )),
                Ok(value) => assert!((value - 2.0_f64.sqrt()).abs() <= 1e-6),
            }
        }
    }

    #[test]
    fn area_metric_extraction_propagates_allocation_failures() {
        let source = polygon(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]);
        let target = Shape::Point(Point::new_raw_xy(6.0, 2.0));
        let (result, count) =
            crate::test_support::allocation_count(|| distance(&source, &target, Some(1e-3), 1.0));
        let value = result.unwrap();
        assert!((value - 40.0_f64.sqrt()).abs() <= 1e-3);
        assert!(count > 0);
        for allowed in 0..=count {
            match crate::test_support::fail_allocations_after(allowed, || {
                distance(&source, &target, Some(1e-3), 1.0)
            }) {
                Err(error) => assert!(matches!(
                    error.kind(),
                    crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
                )),
                Ok(value) => assert!((value - 40.0_f64.sqrt()).abs() <= 1e-3),
            }
        }
    }

    #[test]
    fn output_units_preserve_extreme_finite_distances() {
        for (coordinate, scale) in [
            (2.0_f64.powi(1023), 2.0_f64.powi(-1023)),
            (2.0_f64.powi(-1023), 2.0_f64.powi(1023)),
        ] {
            let left = Shape::Point(Point::new_raw_xy(-coordinate, 0.0));
            let right = Shape::Point(Point::new_raw_xy(coordinate, 0.0));
            assert_eq!(distance(&left, &right, Some(1e-9), scale).unwrap(), 2.0);
        }
    }

    #[test]
    fn output_range_and_scaled_absolute_error_are_certified() {
        let huge = 2.0_f64.powi(1023);
        let left = Shape::Point(Point::new_raw_xy(-huge, 0.0));
        let right = Shape::Point(Point::new_raw_xy(huge, 0.0));
        assert!(distance(&left, &right, Some(1.0), 1.0).is_err());

        let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let large = Shape::Point(Point::new_raw_xy(1e200, 1e200));
        let expected = 2.0_f64.sqrt() * 1e200;
        let actual = distance(&origin, &large, None, 1.0).unwrap();
        assert!((actual - expected).abs() <= 1e-9 * expected);

        let vertices = [
            (0.0, 0.0),
            (9.0, 0.0),
            (9.0, 3.0),
            (4.0, 3.0),
            (4.0, 8.0),
            (0.0, 8.0),
            (0.0, 0.0),
        ];
        let expected = 100.0 * (7.0 - 24.0_f64.sqrt());
        let actual = distance(&polygon(&vertices), &line(&vertices), Some(0.01), 100.0).unwrap();
        assert!((actual - expected).abs() <= 0.01);
    }

    #[test]
    fn overflow_newton_steps_remain_exact_upper_bounds() {
        let maximum = Rational::from_f64(f64::MAX).unwrap();
        let scale = Rational::from_f64(1.0_f64.next_up()).unwrap();
        let squared = maximum
            .try_square()
            .unwrap()
            .try_product(&scale.try_square().unwrap())
            .unwrap();
        let mut upper = squared.try_quotient(&maximum).unwrap();
        for step in 0..=OVERFLOW_NEWTON_STEPS {
            assert_ne!(
                upper.try_square().unwrap().try_cmp(&squared).unwrap(),
                Ordering::Less,
                "Newton upper step {step} fell below the exact squared value"
            );
            if step != OVERFLOW_NEWTON_STEPS {
                let next = overflow_newton_step(&squared, &upper).unwrap();
                assert_ne!(
                    next.try_cmp(&upper).unwrap(),
                    Ordering::Greater,
                    "Newton step {step} must not loosen the upper"
                );
                upper = next;
            }
        }
    }

    #[test]
    fn overflowed_exact_point_distance_can_certify_maximum_with_large_error() {
        let left = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let right = Shape::Point(Point::new_raw_xy(f64::MAX, 0.0));
        let scale = 1.0_f64.next_up();
        let maximum = Rational::from_f64(f64::MAX).unwrap();
        let scale_exact = Rational::from_f64(scale).unwrap();
        let scaled_squared = maximum
            .try_square()
            .unwrap()
            .try_product(&scale_exact.try_square().unwrap())
            .unwrap();
        let exact_distance = maximum.try_product(&scale_exact).unwrap();
        let true_gap = exact_distance.try_subtract(&maximum).unwrap();
        let error = Rational::from_f64(6e292).unwrap();
        assert_ne!(
            true_gap.try_cmp(&error).unwrap(),
            Ordering::Greater,
            "the exact point distance gap must fit the chosen error"
        );
        let target = maximum.try_add(&error).unwrap();
        assert_ne!(
            scaled_squared
                .try_cmp(&target.try_square().unwrap())
                .unwrap(),
            Ordering::Greater,
            "the exact squared distance must fit the chosen candidate allowance"
        );
        let interval =
            SqrtInterval::from_squared_with_error(&scaled_squared, Some(&error)).unwrap();
        let upper = interval
            .upper_rational()
            .unwrap()
            .expect("finite exact overflow upper");
        let initial_upper = scaled_squared.try_quotient(&maximum).unwrap();
        let initial_gap = initial_upper.try_subtract(&maximum).unwrap();
        assert_eq!(
            true_gap.try_cmp(&error).unwrap(),
            Ordering::Less,
            "this is the between-gaps case, not an exact-boundary tie"
        );
        assert_eq!(
            initial_gap.try_cmp(&error).unwrap(),
            Ordering::Greater,
            "q/MAX must be too loose for the chosen error"
        );
        let small_error = Rational::from_f64(3e292).unwrap();
        assert_eq!(
            true_gap.try_cmp(&small_error).unwrap(),
            Ordering::Greater,
            "the negative control must be below the exact point-distance gap"
        );
        assert_ne!(
            upper.try_cmp(&initial_upper).unwrap(),
            Ordering::Greater,
            "Newton refinement must not loosen q/MAX"
        );
        assert_ne!(
            upper.try_cmp(&target).unwrap(),
            Ordering::Greater,
            "error-directed Newton refinement must fit MAX + error"
        );
        let sum = sum_upper(&interval, &SqrtInterval::singleton(1.0).unwrap())
            .unwrap()
            .expect("overflow upper remains finite when summed");
        assert_eq!(
            sum.try_cmp(&upper.try_add(&Rational::one()).unwrap())
                .unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            scaled_squared
                .try_cmp(&maximum.try_square().unwrap())
                .unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            distance(&left, &right, Some(6e292), scale)
                .unwrap()
                .to_bits(),
            f64::MAX.to_bits()
        );
        assert_eq!(
            distance(&left, &right, None, scale).unwrap().to_bits(),
            f64::MAX.to_bits()
        );
        distance(&left, &right, Some(3e292), scale)
            .expect_err("an allowance below the exact max-to-upper gap must not certify f64::MAX");
    }

    #[test]
    fn filled_interior_and_parallel_plateau_obey_the_error_bound() {
        for vertices in [vec![(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)], vec![
            (0.0, 0.0),
            (100.0, 0.0),
            (100.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        ]] {
            let value = distance(&polygon(&vertices), &line(&vertices), Some(1e-6), 1.0).unwrap();
            assert!((value - 1.0).abs() <= 1e-6, "{value}");
        }
    }

    #[test]
    fn default_relative_error_is_certified_without_blurring_zero() {
        const DEFAULT_RELATIVE_ERROR: f64 = 1e-9;

        let triangle = polygon(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
        let boundary = line(&[(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)]);
        let value = distance(&triangle, &boundary, None, 1.0).unwrap();
        assert!((value - 1.0).abs() <= DEFAULT_RELATIVE_ERROR);

        let diagonal = Shape::Point(Point::new_raw_xy(1.0, 1.0));
        let diagonal_value = distance(
            &Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            &diagonal,
            None,
            1.0,
        )
        .unwrap();
        let expected = 2.0_f64.sqrt();
        assert!((diagonal_value - expected).abs() <= DEFAULT_RELATIVE_ERROR * expected);

        let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let close = Shape::Point(Point::new_raw_xy(1e-200, 0.0));
        assert_eq!(distance(&origin, &origin, None, 1.0).unwrap(), 0.0);
        assert!(distance(&origin, &close, None, 1.0).unwrap() > 0.0);
    }

    #[test]
    fn default_allowance_is_bounded_by_the_true_distance() {
        const DEFAULT_RELATIVE_ERROR: f64 = 1e-9;

        let accuracy = Accuracy::new(None, 1.0).unwrap();
        let squared = Rational::from_f64(2.0).unwrap();
        let candidate = squared.round_sqrt().unwrap();
        let error = accuracy.absolute_error(&squared, candidate).unwrap();
        let relative = Rational::from_f64(DEFAULT_RELATIVE_ERROR).unwrap();
        let relative_squared = relative
            .try_square()
            .unwrap()
            .try_product(&squared)
            .unwrap();
        assert_ne!(
            error
                .try_square()
                .unwrap()
                .try_cmp(&relative_squared)
                .unwrap(),
            Ordering::Greater,
        );
    }

    #[test]
    fn default_relative_error_accepts_a_valid_minimum_subnormal_result() {
        let minimum_subnormal = f64::from_bits(1);
        let almost_one = 1.0_f64.next_down();
        let small_ordinate = 3.0 * 2.0_f64.powi(-28);
        let origin = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let nearby = Shape::Point(Point::new_raw_xy(almost_one, small_ordinate));

        assert_eq!(
            distance(&origin, &nearby, None, minimum_subnormal).unwrap(),
            minimum_subnormal
        );
    }

    #[test]
    fn zero_distinguishes_equal_support_from_close_support() {
        let baseline = line(&[(0.0, 0.0), (2.0, 0.0)]);
        let segmented = line(&[(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)]);
        assert_eq!(
            distance(&baseline, &segmented, Some(1e-3), 1.0).unwrap(),
            0.0
        );
        let nearby = line(&[(0.0, 1e-9), (2.0, 1e-9)]);
        let value = distance(&baseline, &nearby, Some(1e-3), 1.0).unwrap();
        assert!(value > 0.0);
        assert!((value - 1e-9).abs() <= 1e-3);
        let vertices = [(0.0, 0.0), (4.0, 0.0), (0.0, 3.0), (0.0, 0.0)];
        let value = distance(&polygon(&vertices), &line(&vertices), Some(2.0), 1.0).unwrap();
        assert!(value > 0.0);
        assert!((value - 1.0).abs() <= 2.0);
    }

    #[test]
    fn identical_empty_support_is_still_undefined() {
        assert!(
            distance(
                &Shape::empty_point(),
                &Shape::empty_point(),
                Some(1e-3),
                1.0
            )
            .is_err()
        );
    }

    #[test]
    fn identical_cancelled_polygon_support_is_undefined() {
        let ring = crate::geometry::Ring::from_closed(
            CoordSeq::try_from_xy(&[
                XY::new(0.0, 0.0),
                XY::new(1.0, 0.0),
                XY::new(2.0, 0.0),
                XY::new(0.0, 0.0),
            ])
            .unwrap(),
        );
        let shape =
            Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), vec![ring]).unwrap());
        assert!(distance(&shape, &shape, Some(1e-6), 1.0).is_err());
    }

    #[test]
    fn invalid_accuracy_is_rejected_before_identity() {
        let point = Shape::Point(Point::new_raw_xy(3.0, 4.0));
        for error in [0.0, -1.0, f64::INFINITY, f64::NAN] {
            assert!(distance(&point, &point, Some(error), 1.0).is_err());
        }
    }
}
