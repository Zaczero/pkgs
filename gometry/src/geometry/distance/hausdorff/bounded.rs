//! Continuous planar Hausdorff distance with a certified error bound.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::{ExactMetricSpan, ExactMetricSupport, ExactSourceInventory};
use crate::geometry::tessellation::exact;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational as Rational};
use crate::geometry::{
    CoordSeq, DdInterval, ExpansionBudget, LineSeq, PackedLineColumnView, Point, Segment, Shape,
    ShapeCountQuery, XY,
};

mod index;
mod instrumentation;
mod numeric;

use index::{BoundaryEdge, PrimitiveId, SupportIndex};
use instrumentation::line_filter_enabled;
#[cfg(test)]
use instrumentation::{
    count_exact_span_evaluation, count_refinement_work, exact_span_evaluations,
    limit_refinement_work, refinement_work, reset_exact_span_evaluations, set_line_filter_enabled,
};
use numeric::{
    Accuracy, ErrorBound, ExactCarrier, FloatBox, SquaredBounds, SupportSpan, Tightness,
    UnreducedRatio,
};

const NOMINATED_SITE_COUNT: usize = 4;
const NEAREST_CANDIDATE_LIMIT: usize = 32;
const HULL_SEPARATION_LIMIT: usize = 8;

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
        if !shape.has_area_parts() {
            let capacity = ShapeCountQuery::default().segments(shape).ok();
            let mut spans = Vec::new();
            if let Some(capacity) = capacity {
                let _reservation = spans.try_reserve(capacity);
            }
            let mut points = Vec::new();
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
            shape.for_each_component(&mut |component| {
                if let crate::geometry::Component::Point(point) = component {
                    points.push(ExactPoint::from_xy(point.xy()));
                }
            });
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
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let partition = inventory.partition(budget)?;
        let support = inventory.metric_support_with_boundary(&partition)?;
        Self::from_metric_support(support)
    }

    fn from_line_columns(xs: &[f64], ys: &[f64]) -> Result<Self> {
        debug_assert_eq!(xs.len(), ys.len());
        let mut spans = Vec::with_capacity(xs.len().saturating_sub(1));
        let mut points = Vec::new();
        let mut coordinates = xs.iter().copied().zip(ys.iter().copied());
        if let Some((x, y)) = coordinates.next() {
            let first = XY::new(x, y);
            let mut start = first;
            for (x, y) in coordinates {
                let end = XY::new(x, y);
                push_raw_segment(Segment { start, end }, &mut spans, &mut points)?;
                start = end;
            }
            if xs.len() == 1 {
                points.push(ExactPoint::from_xy(first));
            }
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
        let mut spans = Vec::with_capacity(metric_spans.len());
        for span in metric_spans {
            spans.push(support_span(span)?);
        }

        let mut points = Vec::with_capacity(metric_points.len());
        for point in metric_points {
            points.push(ExactPoint::from_homogeneous(point.homogeneous)?);
        }

        let mut faces = Vec::with_capacity(metric_faces.len());
        for face in metric_faces {
            let mut cycles = Vec::with_capacity(face.len());
            for cycle in face {
                let mut points = Vec::with_capacity(cycle.len());
                for point in cycle {
                    points.push(ExactPoint::from_homogeneous(point.homogeneous)?);
                }
                cycles.push(points);
            }
            faces.push(cycles);
        }

        let mut prepared = Self::from_parts(spans, points, faces)?;
        prepared.boundary_spans = metric_boundary_spans;
        prepared.boundary_points = metric_boundary_points;
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
                for (start, end) in cycle
                    .iter()
                    .zip(cycle.iter().cycle().skip(1))
                    .take(cycle.len())
                {
                    boundary_edges.push(BoundaryEdge {
                        start: start.clone(),
                        end: end.clone(),
                        bounds: FloatBox::from_point(start)?.include(FloatBox::from_point(end)?),
                    });
                }
            }
        }

        let area_bounds = boundary_edges
            .iter()
            .map(|edge| edge.bounds)
            .reduce(FloatBox::include);

        if spans.is_empty() && points.is_empty() && faces.is_empty() {
            return Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty support",
            ));
        }

        let index = SupportIndex::build(&spans, &points)?;
        Ok(Self {
            spans,
            points,
            faces,
            boundary_spans: None,
            boundary_points: None,
            boundary_edges,
            area_bounds,
            index,
        })
    }

    /// Clone the exact boundary selected by the partition's face walks without
    /// reconstructing it from rounded source coordinates. The aligned flags
    /// already exclude shared/internal face edges and lower-dimensional
    /// support hidden by a positive-area face.
    pub(in crate::geometry) fn boundary_clone(&self) -> Result<Self> {
        let span_flags = self.boundary_spans.as_deref();
        let mut spans = Vec::with_capacity(span_flags.map_or(self.spans.len(), |flags| {
            flags.iter().filter(|&&selected| selected).count()
        }));
        match span_flags {
            Some(flags) => {
                for (span, &selected) in self.spans.iter().zip(flags) {
                    if selected {
                        spans.push(span.clone());
                    }
                }
            },
            None => spans.extend(self.spans.iter().cloned()),
        }

        let point_flags = self.boundary_points.as_deref();
        let mut points = Vec::with_capacity(point_flags.map_or(self.points.len(), |flags| {
            flags.iter().filter(|&&selected| selected).count()
        }));
        match point_flags {
            Some(flags) => {
                for (point, &selected) in self.points.iter().zip(flags) {
                    if selected {
                        points.push(point.clone());
                    }
                }
            },
            None => points.extend(self.points.iter().cloned()),
        }
        Self::from_parts(spans, points, Vec::new())
    }

    fn initial_patches(&self) -> Vec<SourcePatch> {
        let capacity = self
            .spans
            .len()
            .wrapping_add(usize::from(!self.faces.is_empty()));
        let mut patches = Vec::with_capacity(capacity);
        for (index, span) in self.spans.iter().enumerate() {
            patches.push(SourcePatch::Span {
                span: index,
                lo: span.t_lo.clone(),
                hi: span.t_hi.clone(),
            });
        }
        if let Some(bounds) = self.area_bounds
            && !self.faces.is_empty()
        {
            patches.push(SourcePatch::Box(bounds));
        }
        patches
    }

    fn inradius_initial_patches(&self) -> Result<Vec<SourcePatch>> {
        if self.has_positive_area()? {
            return Ok(self
                .area_bounds
                .map_or_else(Vec::new, |bounds| vec![SourcePatch::Box(bounds)]));
        }

        let mut patches = self.initial_patches();
        patches.reserve(self.points.len());
        for index in 0..self.points.len() {
            patches.push(SourcePatch::Point(index));
        }
        Ok(patches)
    }

    fn contains_area(&self, point: &ExactPoint) -> Result<bool> {
        let point_bounds = FloatBox::from_point(point)?;
        // `boundary_edges` lists each cycle of at least two points in order.
        let mut edges = self.boundary_edges.as_slice();
        for face in &self.faces {
            let mut inside = false;
            for cycle in face {
                let count = if cycle.len() < 2 { 0 } else { cycle.len() };
                let (cycle_edges, rest) = edges.split_at(count);
                edges = rest;
                match cycle_location(point, point_bounds, cycle, cycle_edges)? {
                    CycleLocation::Boundary => return Ok(true),
                    CycleLocation::Crosses => inside = !inside,
                    CycleLocation::Clear => {},
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
        Ok(self.index.nearest(self, point, None)?.lower.is_zero())
    }

    pub(in crate::geometry) fn candidate_is_contained(
        &self,
        boundary: &Self,
        center: Point,
        candidate: &[Point],
        counter_clockwise: bool,
    ) -> Result<bool> {
        let center = ExactPoint::from_xy(center.xy());
        if !self.contains_area(&center)? {
            return Ok(false);
        }
        for &point in candidate {
            let point = ExactPoint::from_xy(point.xy());
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
                for (left, right) in cycle
                    .iter()
                    .zip(cycle.iter().cycle().skip(1))
                    .take(cycle.len())
                {
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

    fn first_boundary_point(&self) -> Option<ExactPoint> {
        if let Some(span) = self.spans.first() {
            return Some(span.start.clone());
        }
        if let Some(point) = self.points.first() {
            return Some(point.clone());
        }
        self.boundary_edges.first().map(|edge| edge.start.clone())
    }

    fn source_box_witness(
        &self,
        bounds: FloatBox,
        center_first: bool,
    ) -> Result<Option<ExactPoint>> {
        if center_first {
            let min_x = Rational::from_f64(bounds.min_x);
            let max_x = Rational::from_f64(bounds.max_x);
            let min_y = Rational::from_f64(bounds.min_y);
            let max_y = Rational::from_f64(bounds.max_y);
            let center = ExactPoint {
                x: min_x.try_midpoint(&max_x)?,
                y: min_y.try_midpoint(&max_y)?,
            };
            if self.contains_area(&center)? {
                return Ok(Some(center));
            }
        }
        for (x, y) in bounds.corners() {
            let point = ExactPoint::from_xy(XY::new(x, y));
            if self.contains_area(&point)? {
                return Ok(Some(point));
            }
        }
        for edge in &self.boundary_edges {
            if edge.bounds.meets(bounds)
                && let Some(point) = segment_box_intersection(edge, bounds)?
            {
                return Ok(Some(point));
            }
        }
        Ok(None)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "PrimitiveId carries a stable span/point ordinal from this immutable PreparedSupport"
    )]
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
        if self.contains_area(point)? {
            return Ok(Rational::zero());
        }
        Ok(self.index.nearest(self, point, None)?.lower)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "PrimitiveId carries a stable span/point ordinal from this immutable PreparedSupport"
    )]
    fn primitive_squared_bounds(
        &self,
        primitive: PrimitiveId,
        point: &ExactPoint,
        point_bounds: FloatBox,
        tightness: Option<Tightness>,
    ) -> Result<SquaredBounds> {
        if let (Some(tightness), PrimitiveId::Span(index)) = (tightness, primitive)
            && let Some((lower, upper)) =
                point_span_squared_bounds(point_bounds, &self.spans[index])
            && tightness.admits(lower, upper)
        {
            return Ok(SquaredBounds::enclosing(lower, upper));
        }
        SquaredBounds::exact(
            self.primitive_distance_squared(primitive, point)?
                .to_rational()?,
        )
    }
}

fn push_raw_segment(
    source: Segment,
    spans: &mut Vec<SupportSpan>,
    points: &mut Vec<ExactPoint>,
) -> Result<()> {
    if source.start == source.end {
        points.push(ExactPoint::from_xy(source.start));
        return Ok(());
    }
    spans.push(SupportSpan {
        source,
        carrier: ExactCarrier::from_segment(source)?,
        t_lo: Rational::zero(),
        t_hi: Rational::one(),
        start: ExactPoint::from_xy(source.start),
        bounds: FloatBox {
            min_x: source.start.x.min(source.end.x),
            max_x: source.start.x.max(source.end.x),
            min_y: source.start.y.min(source.end.y),
            max_y: source.start.y.max(source.end.y),
        },
    });
    Ok(())
}

fn support_span(span: ExactMetricSpan) -> Result<SupportSpan> {
    let start = ExactPoint::from_homogeneous(span.start.homogeneous)?;
    let end = ExactPoint::from_homogeneous(span.end.homogeneous)?;
    let mut t_lo = Rational::new(
        span.start_parameter[0].clone(),
        span.start_parameter[1].clone(),
    )?;
    let mut t_hi = Rational::new(span.end_parameter[0].clone(), span.end_parameter[1].clone())?;
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

impl ExactCarrier {
    fn from_segment(source: Segment) -> Result<Self> {
        let ax = Rational::from_f64(source.start.x);
        let ay = Rational::from_f64(source.start.y);
        let dx = Rational::from_f64(source.end.x).try_subtract(&ax)?;
        let dy = Rational::from_f64(source.end.y).try_subtract(&ay)?;
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

/// Double-double bounds on the squared distance from any point of
/// `point_bounds` to a span covering its whole source segment. The dot and
/// cross products with the segment direction are affine in the point, so the
/// box corners bound them; a box straddling the segment's end normals, a
/// partial span, or a non-finite stage declines to the exact evaluation.
fn point_span_squared_bounds(point_bounds: FloatBox, span: &SupportSpan) -> Option<(f64, f64)> {
    if !(span.t_lo.is_zero() && span.t_hi.is_one()) {
        return None;
    }
    let (start, end) = (span.source.start, span.source.end);
    let dx = DdInterval::difference(end.x, start.x)?;
    let dy = DdInterval::difference(end.y, start.y)?;
    let (norm_lower, norm_upper) = dx.square()?.add(dy.square()?)?.outward_bounds();
    if !(norm_lower > 0.0 && norm_upper.is_finite()) {
        return None;
    }
    let (mut dot_lower, mut dot_upper) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut cross_lower, mut cross_upper) = (f64::INFINITY, f64::NEG_INFINITY);
    for (x, y) in point_bounds.corners() {
        let px = DdInterval::difference(x, start.x)?;
        let py = DdInterval::difference(y, start.y)?;
        let (low, high) = dx.mul(px)?.add(dy.mul(py)?)?.outward_bounds();
        dot_lower = dot_lower.min(low);
        dot_upper = dot_upper.max(high);
        let (low, high) = dx.mul(py)?.sub(dy.mul(px)?)?.outward_bounds();
        cross_lower = cross_lower.min(low);
        cross_upper = cross_upper.max(high);
    }
    if !(dot_lower.is_finite() && dot_upper.is_finite()) {
        return None;
    }
    let endpoint = |target: XY| -> Option<(f64, f64)> {
        let mut upper: f64 = 0.0;
        for (x, y) in point_bounds.corners() {
            let px = DdInterval::difference(x, target.x)?;
            let py = DdInterval::difference(y, target.y)?;
            upper = upper.max(px.square()?.add(py.square()?)?.outward_bounds().1);
        }
        let target = FloatBox {
            min_x: target.x,
            max_x: target.x,
            min_y: target.y,
            max_y: target.y,
        };
        upper
            .is_finite()
            .then(|| (box_distance_lower(point_bounds, target), upper))
    };
    if dot_upper <= 0.0 {
        return endpoint(start);
    }
    if dot_lower >= norm_upper {
        return endpoint(end);
    }
    if !(dot_lower >= 0.0 && dot_upper <= norm_lower && cross_lower.is_finite()) {
        return None;
    }
    let cross_squared_lower = if cross_lower > 0.0 {
        (cross_lower * cross_lower).next_down()
    } else if cross_upper < 0.0 {
        (cross_upper * cross_upper).next_down()
    } else {
        0.0
    };
    let cross_squared_upper = (cross_lower * cross_lower)
        .max(cross_upper * cross_upper)
        .next_up();
    let lower = (cross_squared_lower / norm_upper).next_down().max(0.0);
    let upper = (cross_squared_upper / norm_lower).next_up();
    upper.is_finite().then_some((lower, upper))
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
    if value.try_cmp(&Rational::from_f64(f64::MAX))? == Ordering::Greater {
        Ok(f64::INFINITY)
    } else {
        Ok(value.outward_bounds()?.1)
    }
}

fn square_f64(value: f64) -> Result<Rational> {
    let value = ExactDyadic::from_f64(value);
    Ok(Rational::from_dyadic(value.try_product(&value)?))
}

#[cfg(test)]
fn dyadic_as_f64(value: &ExactDyadic) -> Result<f64> {
    Rational::from_dyadic(value.clone()).round()
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
        value.fast_admissions = value.fast_admissions.wrapping_add(1);
        stats.set(value);
    });
}

#[cfg(test)]
fn record_sqrt_fallback() {
    SQRT_STATS.with(|stats| {
        let mut value = stats.get();
        value.fallbacks = value.fallbacks.wrapping_add(1);
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

fn point_segment_squared_f64(point: XY, start: XY, end: XY) -> f64 {
    let (dx, dy) = (end.x - start.x, end.y - start.y);
    let (px, py) = (point.x - start.x, point.y - start.y);
    let length = dx * dx + dy * dy;
    let t = if length > 0.0 {
        ((px * dx + py * dy) / length).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let (ex, ey) = (px - t * dx, py - t * dy);
    ex * ex + ey * ey
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

enum CycleLocation {
    Boundary,
    /// Off the cycle, with an odd number of crossings of the rightward ray.
    Crosses,
    Clear,
}

/// Locate a point against one cycle in a single pass. The exact cross product
/// runs only for edges that straddle the rightward ray or whose extent holds
/// the point; any other edge can neither contain the point nor cross the ray.
/// `edges[i]` joins `cycle[i]` to its successor, and an edge whose outward
/// bounds lie wholly above or below the point's needs no exact comparison.
fn cycle_location(
    point: &ExactPoint,
    point_bounds: FloatBox,
    cycle: &[ExactPoint],
    edges: &[BoundaryEdge],
) -> Result<CycleLocation> {
    if cycle.len() < 3 {
        return Ok(CycleLocation::Clear);
    }
    // Off the cycle's bounds the point is neither on it nor enclosed by it.
    let bounds = edges
        .iter()
        .map(|edge| edge.bounds)
        .reduce(FloatBox::include)
        .expect("a cycle of three points has edges");
    if !bounds.meets(point_bounds) {
        return Ok(CycleLocation::Clear);
    }
    let mut crossings = false;
    debug_assert_eq!(cycle.len(), edges.len());
    let previous_count = cycle.len().wrapping_sub(1);
    let last = cycle
        .last()
        .expect("a cycle of three points has a last point");
    let mut left_order = last.y.try_cmp(&point.y)?;
    let lefts = std::iter::once(last).chain(cycle.iter().take(previous_count));
    let edge_first = edges
        .iter()
        .next_back()
        .expect("cycle edges align with cycle points");
    let edges = std::iter::once(edge_first).chain(edges.iter().take(previous_count));
    for ((left, right), edge) in lefts.zip(cycle.iter()).zip(edges) {
        let edge = edge.bounds;
        let right_order = if edge.min_y > point_bounds.max_y {
            Ordering::Greater
        } else if edge.max_y < point_bounds.min_y {
            Ordering::Less
        } else {
            right.y.try_cmp(&point.y)?
        };
        let straddles = (left_order == Ordering::Greater) != (right_order == Ordering::Greater);
        let spans_y = left_order != right_order || left_order == Ordering::Equal;
        left_order = right_order;
        let holds_point = spans_y && between(&point.x, &left.x, &right.x)?;
        if !straddles && !holds_point {
            continue;
        }
        let dx = right.x.try_subtract(&left.x)?;
        let dy = right.y.try_subtract(&left.y)?;
        let px = point.x.try_subtract(&left.x)?;
        let py = point.y.try_subtract(&left.y)?;
        let cross = dx.try_product(&py)?.try_subtract(&dy.try_product(&px)?)?;
        if holds_point && cross.is_zero() {
            return Ok(CycleLocation::Boundary);
        }
        let rising = right_order == Ordering::Greater;
        if straddles
            && ((rising && cross.sign() == ExactSign::Positive)
                || (!rising && cross.sign() == ExactSign::Negative))
        {
            crossings = !crossings;
        }
    }
    Ok(if crossings {
        CycleLocation::Crosses
    } else {
        CycleLocation::Clear
    })
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
        return Ok(Some(edge.start.clone()));
    }
    if point_in_box(&edge.end, bounds)? {
        return Ok(Some(edge.end.clone()));
    }
    let dx = edge.end.x.try_subtract(&edge.start.x)?;
    let dy = edge.end.y.try_subtract(&edge.start.y)?;
    for x in [bounds.min_x, bounds.max_x] {
        if dx.is_zero() {
            continue;
        }
        let x = Rational::from_f64(x);
        let parameter = x.try_subtract(&edge.start.x)?.try_quotient(&dx)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let y = edge.start.y.try_add(&dy.try_product(&parameter)?)?;
        if y.try_cmp(&Rational::from_f64(bounds.min_y))? != Ordering::Less
            && y.try_cmp(&Rational::from_f64(bounds.max_y))? != Ordering::Greater
        {
            return Ok(Some(ExactPoint { x, y }));
        }
    }
    for y in [bounds.min_y, bounds.max_y] {
        if dy.is_zero() {
            continue;
        }
        let y = Rational::from_f64(y);
        let parameter = y.try_subtract(&edge.start.y)?.try_quotient(&dy)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let x = edge.start.x.try_add(&dx.try_product(&parameter)?)?;
        if x.try_cmp(&Rational::from_f64(bounds.min_x))? != Ordering::Less
            && x.try_cmp(&Rational::from_f64(bounds.max_x))? != Ordering::Greater
        {
            return Ok(Some(ExactPoint { x, y }));
        }
    }
    Ok(None)
}

fn point_in_box(point: &ExactPoint, bounds: FloatBox) -> Result<bool> {
    let min_x = Rational::from_f64(bounds.min_x);
    let max_x = Rational::from_f64(bounds.max_x);
    let min_y = Rational::from_f64(bounds.min_y);
    let max_y = Rational::from_f64(bounds.max_y);
    Ok(point.x.try_cmp(&min_x)? != Ordering::Less
        && point.x.try_cmp(&max_x)? != Ordering::Greater
        && point.y.try_cmp(&min_y)? != Ordering::Less
        && point.y.try_cmp(&max_y)? != Ordering::Greater)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoxSplitStrategy {
    FourWay,
    LongestAxis,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoxSplitAxis {
    X,
    Y,
}

fn normalized_box_extent(min: f64, max: f64, scale: f64) -> Option<(f64, f64)> {
    let min = min / scale;
    let max = max / scale;
    if !(min.is_finite() && max.is_finite()) {
        return None;
    }
    let lower = (max.next_down() - min.next_up()).next_down().max(0.0);
    let upper = (max.next_up() - min.next_down()).next_up();
    (lower.is_finite() && upper.is_finite() && lower <= upper).then_some((lower, upper))
}

fn compare_box_extents(bounds: FloatBox) -> Option<Ordering> {
    let x = DdInterval::difference(bounds.max_x, bounds.min_x).map(DdInterval::outward_bounds);
    let y = DdInterval::difference(bounds.max_y, bounds.min_y).map(DdInterval::outward_bounds);
    if let (Some(x), Some(y)) = (x, y) {
        if x.0 > y.1 {
            return Some(Ordering::Greater);
        }
        if y.0 > x.1 {
            return Some(Ordering::Less);
        }
        if x.0.to_bits() == x.1.to_bits()
            && y.0.to_bits() == y.1.to_bits()
            && x.0.to_bits() == y.0.to_bits()
        {
            return Some(Ordering::Equal);
        }
    }

    let scale = bounds
        .min_x
        .abs()
        .max(bounds.max_x.abs())
        .max(bounds.min_y.abs())
        .max(bounds.max_y.abs());
    if scale == 0.0 {
        return Some(Ordering::Equal);
    }
    let x = normalized_box_extent(bounds.min_x, bounds.max_x, scale)?;
    let y = normalized_box_extent(bounds.min_y, bounds.max_y, scale)?;
    if x.0 > y.1 {
        Some(Ordering::Greater)
    } else if y.0 > x.1 {
        Some(Ordering::Less)
    } else if x.0.to_bits() == x.1.to_bits()
        && y.0.to_bits() == y.1.to_bits()
        && x.0.to_bits() == y.0.to_bits()
    {
        Some(Ordering::Equal)
    } else {
        None
    }
}

fn exact_segment_enters_candidate(
    start: &ExactPoint,
    end: &ExactPoint,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let zero = Rational::zero();
    let one = Rational::one();
    let mut lower = zero.clone();
    let mut upper = one;
    let mut lower_closed = true;
    let mut upper_closed = true;
    let source_x = end.x.try_subtract(&start.x)?;
    let source_y = end.y.try_subtract(&start.y)?;

    for (&left, &right) in candidate
        .iter()
        .zip(candidate.iter().cycle().skip(1))
        .take(candidate.len())
    {
        let edge_x = Rational::from_f64(right.x).try_subtract(&Rational::from_f64(left.x))?;
        let edge_y = Rational::from_f64(right.y).try_subtract(&Rational::from_f64(left.y))?;
        let start_x = start.x.try_subtract(&Rational::from_f64(left.x))?;
        let start_y = start.y.try_subtract(&Rational::from_f64(left.y))?;
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
    #[expect(
        clippy::indexing_slicing,
        reason = "patch span and point ordinals are created from this immutable PreparedSupport"
    )]
    fn witness(&self, source: &PreparedSupport, center_first: bool) -> Result<Option<ExactPoint>> {
        match self {
            Self::Span { span, lo, hi } => {
                let midpoint = lo.try_midpoint(hi)?;
                Ok(Some(span_point(&source.spans[*span], &midpoint)?))
            },
            Self::Box(bounds) => source.source_box_witness(*bounds, center_first),
            Self::Point(point) => Ok(Some(source.points[*point].clone())),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "patch span and point ordinals are created from this immutable PreparedSupport"
    )]
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
                let min_x = Rational::from_f64(bounds.min_x);
                let max_x = Rational::from_f64(bounds.max_x);
                let min_y = Rational::from_f64(bounds.min_y);
                let max_y = Rational::from_f64(bounds.max_y);
                let center = ExactPoint {
                    x: min_x.try_midpoint(&max_x)?,
                    y: min_y.try_midpoint(&max_y)?,
                };
                let half_x = max_x
                    .try_subtract(&min_x)?
                    .try_quotient(&Rational::from_f64(2.0))?;
                let half_y = max_y
                    .try_subtract(&min_y)?
                    .try_quotient(&Rational::from_f64(2.0))?;
                let radius = half_x.try_square()?.try_add(&half_y.try_square()?)?;
                Ok((center, radius))
            },
            Self::Point(point) => Ok((source.points[*point].clone(), Rational::zero())),
        }
    }

    #[cfg(any(test, debug_assertions))]
    #[expect(
        clippy::indexing_slicing,
        reason = "patch span and point ordinals are created from this immutable PreparedSupport"
    )]
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

    #[expect(
        clippy::indexing_slicing,
        reason = "patch span and point ordinals are created from this immutable PreparedSupport"
    )]
    fn corners(&self, source: &PreparedSupport) -> Result<Vec<ExactPoint>> {
        Ok(match self {
            Self::Span { span, lo, hi } => {
                vec![
                    span_point(&source.spans[*span], lo)?,
                    span_point(&source.spans[*span], hi)?,
                ]
            },
            Self::Box(bounds) => {
                let mut corners = Vec::with_capacity(4);
                corners.extend(
                    bounds
                        .corners()
                        .map(|(x, y)| ExactPoint::from_xy(XY::new(x, y))),
                );
                corners
            },
            Self::Point(point) => vec![source.points[*point].clone()],
        })
    }

    fn split(self, budget: &mut ExpansionBudget) -> Result<Option<Vec<Self>>> {
        self.split_with_strategy(budget, BoxSplitStrategy::FourWay)
    }

    fn split_with_strategy(
        self,
        budget: &mut ExpansionBudget,
        strategy: BoxSplitStrategy,
    ) -> Result<Option<Vec<Self>>> {
        match self {
            Self::Span { span, lo, hi } => {
                budget.add(2)?;
                let midpoint = lo.try_midpoint(&hi)?;
                Ok(Some(vec![
                    Self::Span {
                        span,
                        lo,
                        hi: midpoint.clone(),
                    },
                    Self::Span {
                        span,
                        lo: midpoint,
                        hi,
                    },
                ]))
            },
            Self::Box(bounds) => match strategy {
                BoxSplitStrategy::FourWay => Self::split_box_four_way(bounds, budget),
                BoxSplitStrategy::LongestAxis => Self::split_box_longest_axis(bounds, budget),
            },
            Self::Point(_) => Ok(None),
        }
    }

    fn split_box_four_way(
        bounds: FloatBox,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Vec<Self>>> {
        let mid_x = f64::midpoint(bounds.min_x, bounds.max_x);
        let mid_y = f64::midpoint(bounds.min_y, bounds.max_y);
        let split_x = bounds.min_x < mid_x && mid_x < bounds.max_x;
        let split_y = bounds.min_y < mid_y && mid_y < bounds.max_y;
        if !split_x && !split_y {
            return Ok(None);
        }
        let count = if split_x && split_y { 4 } else { 2 };
        budget.add(count)?;
        let mut children = Vec::with_capacity(count);
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
        for (min_x, max_x) in xs.into_iter().take(if split_x { 2 } else { 1 }) {
            for (min_y, max_y) in ys.into_iter().take(if split_y { 2 } else { 1 }) {
                children.push(Self::Box(FloatBox {
                    min_x,
                    max_x,
                    min_y,
                    max_y,
                }));
            }
        }
        Ok(Some(children))
    }

    fn split_box_longest_axis(
        bounds: FloatBox,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Vec<Self>>> {
        let mid_x = f64::midpoint(bounds.min_x, bounds.max_x);
        let mid_y = f64::midpoint(bounds.min_y, bounds.max_y);
        let split_x = bounds.min_x < mid_x && mid_x < bounds.max_x;
        let split_y = bounds.min_y < mid_y && mid_y < bounds.max_y;
        if !split_x && !split_y {
            return Ok(None);
        }
        let preferred = match compare_box_extents(bounds) {
            Some(Ordering::Less) => BoxSplitAxis::Y,
            Some(Ordering::Equal | Ordering::Greater) => BoxSplitAxis::X,
            None => return Self::split_box_four_way(bounds, budget),
        };
        // At least one axis splits, so the preferred axis yields only when it cannot.
        let axis = match preferred {
            BoxSplitAxis::X if split_x => BoxSplitAxis::X,
            BoxSplitAxis::Y if split_y => BoxSplitAxis::Y,
            BoxSplitAxis::X => BoxSplitAxis::Y,
            BoxSplitAxis::Y => BoxSplitAxis::X,
        };
        budget.add(2)?;
        let (first, second) = match axis {
            BoxSplitAxis::X => (
                FloatBox {
                    max_x: mid_x,
                    ..bounds
                },
                FloatBox {
                    min_x: mid_x,
                    ..bounds
                },
            ),
            BoxSplitAxis::Y => (
                FloatBox {
                    max_y: mid_y,
                    ..bounds
                },
                FloatBox {
                    min_y: mid_y,
                    ..bounds
                },
            ),
        };
        Ok(Some(vec![Self::Box(first), Self::Box(second)]))
    }
}

fn span_radius_squared(span: &SupportSpan, lo: &Rational, hi: &Rational) -> Result<Rational> {
    let delta = hi.try_subtract(lo)?;
    let half = delta.try_quotient(&Rational::from_f64(2.0))?;
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
        .try_quotient(&Rational::from_f64(2.0))
}

impl FloatSqrtBracket {
    fn into_interval(self, squared: &Rational, error: Option<&Rational>) -> Result<SqrtInterval> {
        match self.upper {
            None => SqrtInterval::overflow(squared, error),
            Some(upper) if self.lower.to_bits() == upper.to_bits() => {
                Ok(SqrtInterval::singleton(self.lower))
            },
            Some(upper) => Ok(SqrtInterval::adjacent(self.lower, upper)),
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
        let midpoint = ExactDyadic::from_f64(self.lower)
            .try_add(ExactDyadic::from_f64(upper))?
            .try_product(&ExactDyadic::from_f64(0.5))?;
        let midpoint_squared = Rational::from_dyadic(midpoint.try_product(&midpoint)?);
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
        let value = squared.clone();
        let rounded = value.round_sqrt()?;
        if rounded.is_infinite() {
            return Self::overflow(squared, error);
        }
        match squared.try_cmp(&square_f64(rounded)?)? {
            Ordering::Equal => Ok(Self::singleton(rounded)),
            // The predecessor of +0 is negative in total binary64 order,
            // but a squared-distance bracket must stay in the nonnegative root domain.
            Ordering::Less if rounded == 0.0 => Ok(Self::adjacent(0.0, f64::from_bits(1))),
            Ordering::Less => Ok(Self::adjacent(rounded.next_down(), rounded)),
            Ordering::Greater => {
                if rounded.to_bits() == f64::MAX.to_bits() {
                    Self::overflow(squared, error)
                } else {
                    Ok(Self::adjacent(rounded, rounded.next_up()))
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
        let value = squared.clone().round_sqrt()?;
        Ok(value.is_finite().then_some(value))
    }

    fn singleton(value: f64) -> Self {
        Self {
            lower: ExactDyadic::from_f64(value),
            upper: Some(ExactDyadic::from_f64(value)),
            overflow_upper: None,
        }
    }

    fn adjacent(lower: f64, upper: f64) -> Self {
        debug_assert!(lower.is_finite() && upper.is_finite());
        debug_assert_eq!(upper.to_bits().checked_sub(lower.to_bits()), Some(1));
        Self {
            lower: ExactDyadic::from_f64(lower),
            upper: Some(ExactDyadic::from_f64(upper)),
            overflow_upper: None,
        }
    }

    fn overflow(squared: &Rational, error: Option<&Rational>) -> Result<Self> {
        let maximum = Rational::from_f64(f64::MAX);
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
            lower: ExactDyadic::from_f64(f64::MAX),
            upper: None,
            overflow_upper: Some(upper),
        })
    }

    fn refine(&mut self, squared: &Rational, error: &Rational) -> Result<()> {
        let Some(mut upper) = self.upper.take() else {
            return Ok(());
        };
        loop {
            let width = Rational::from_dyadic(upper.clone().try_subtract(self.lower.clone())?);
            if width.try_cmp(error)? != Ordering::Greater {
                self.upper = Some(upper);
                return Ok(());
            }

            let midpoint = self
                .lower
                .clone()
                .try_add(upper.clone())?
                .try_product(&ExactDyadic::from_f64(0.5))?;
            let midpoint_rational = Rational::from_dyadic(midpoint.clone());
            let midpoint_squared = midpoint_rational.try_square()?;

            match squared.try_cmp(&midpoint_squared)? {
                Ordering::Equal => {
                    self.lower = midpoint.clone();
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

    fn upper_rational(&self) -> Option<Rational> {
        if let Some(value) = &self.upper {
            return Some(Rational::from_dyadic(value.clone()));
        }
        self.overflow_upper.clone()
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
    const fn new(stable_ties: bool) -> Self {
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
        self.heap.push(QueueEntry { key, order, record });

        Ok(())
    }

    fn pop(&mut self) -> Option<PatchRecord> {
        self.heap.pop().map(|entry| entry.record)
    }

    fn max_upper(&self) -> Option<Rational> {
        let entry = self.heap.peek()?;
        (!entry.key.is_infinite()).then(|| Rational::from_f64(entry.key))
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
        // Equal exact clearances intentionally retain the first witness
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

fn sqrt_upper(squared: &Rational, context: &SearchContext<'_>) -> Result<SqrtInterval> {
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
                let error = Rational::from_f64(f64::MAX).try_product(relative)?;
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
        let sum = left.clone().try_add(right.clone())?;
        return Ok(Some(Rational::from_dyadic(sum)));
    }
    let (Some(left), Some(right)) = (left.upper_rational(), right.upper_rational()) else {
        return Ok(None);
    };
    Ok(Some(left.try_add(&right)?))
}

/// The least, over the nominated target primitives, of the farthest patch
/// corner's squared distance. Distance to one primitive is convex, so this
/// bounds the squared distance to the target over the whole patch.
fn fixed_site_squared(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    nominated: &[PrimitiveId],
    tightness: Tightness,
) -> Result<Option<Rational>> {
    let corners = patch.corners(source)?;
    let mut best = None;
    for &primitive in nominated {
        let mut site_upper = None;
        for corner in &corners {
            let bounds = FloatBox::from_point(corner)?;
            let squared = target
                .primitive_squared_bounds(primitive, corner, bounds, Some(tightness))?
                .upper;
            max_rational(&mut site_upper, squared)?;
        }
        min_rational(&mut best, site_upper.expect("fixed site has corners"))?;
    }
    Ok(best)
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
    right_values.push(left_value.try_subtract(separation)?);
    left_values.push(left_value);
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    reason = "nominated span ordinals index the same immutable target support that produced them"
)]
fn parallel_strip_upper(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    nominated: &[PrimitiveId],
    context: &SearchContext<'_>,
) -> Result<Option<Rational>> {
    let mut span_indices = Vec::with_capacity(nominated.len());
    for &primitive in nominated {
        if let PrimitiveId::Span(index) = primitive {
            span_indices.push(index);
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
            .fold(0_usize, usize::wrapping_add)
    } else {
        corners.len()
    };
    let mut best = None;
    // One probe buffer pair for the whole sweep: every parallel span pair
    // holds the same `probe_count` values, and the pair loop is quadratic in
    // the span count, so allocating per pair allocated quadratically.
    let mut left_values = Vec::with_capacity(probe_count);
    let mut right_values = Vec::with_capacity(probe_count);
    let mut remaining = span_indices.as_slice();
    while let Some((&left_index, right_indices)) = remaining.split_first() {
        for &right_index in right_indices {
            let left = &target.spans[left_index];
            let right = &target.spans[right_index];
            if !exact_parallel(left, right)? {
                continue;
            }
            let separation = carrier_line_value(left, &right.start)?;
            left_values.clear();
            right_values.clear();
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
            let half = separation_squared.try_quotient(&Rational::from_f64(4.0))?;
            let squared = context.scaled_squared(&half)?;
            let Some(upper) = sqrt_upper(&squared, context)?.upper_rational() else {
                continue;
            };

            min_rational(&mut best, upper)?;
        }
        remaining = right_indices;
    }
    Ok(best)
}

fn patch_upper(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &SearchContext<'_>,
) -> Result<(Option<Rational>, Option<Rational>)> {
    let (center, radius_squared) = patch.center_and_radius_squared(source)?;
    let radius_squared = context.scaled_squared(&radius_squared)?;
    let radius_interval = sqrt_upper(&radius_squared, context)?;
    let boundary = target
        .index
        .nearest(target, &center, Some(context.tightness()))?;
    let mut best = if target.contains_area(&center)? {
        // The segment from the center to any patch point crosses the target
        // boundary no nearer than the center's depth, so the point is within
        // the radius less that depth of the target.
        radius_interval
            .upper_rational()
            .map_or(Ok(None), |radius| {
                let depth = sqrt_upper(&context.scaled_squared(&boundary.lower)?, context)?;
                let depth = Rational::from_dyadic(depth.lower);
                if radius.try_cmp(&depth)? == Ordering::Greater {
                    radius.try_subtract(&depth).map(Some)
                } else {
                    Ok(Some(Rational::zero()))
                }
            })?
    } else {
        let boundary = sqrt_upper(&context.scaled_squared(&boundary.upper)?, context)?;
        sum_upper(&boundary, &radius_interval)?
    };

    let nominated = target.index.nominate(target, &center)?;
    let site_squared = fixed_site_squared(patch, source, target, &nominated, context.tightness())?;
    if let Some(site_squared) = &site_squared {
        let squared = context.scaled_squared(site_squared)?;
        let fixed = sqrt_upper(&squared, context)?.upper_rational();
        let replaces = match (&best, &fixed) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(current), Some(fixed)) => fixed.try_cmp(current)? == Ordering::Less,
        };
        if replaces {
            best = fixed;
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
    Ok((best, site_squared))
}

fn upper_is_below_lower(upper: &Rational, context: &SearchContext<'_>) -> Result<bool> {
    let Some(lower) = &context.lower_squared else {
        return Ok(false);
    };
    let upper_squared = (upper).try_square()?;
    Ok(upper_squared.try_cmp(lower)? != Ordering::Greater)
}

/// The exact scaled squared clearance of `point` when it could raise the
/// search's lower bound, or `None` when a binary64 enclosure already proves
/// it falls strictly short. A clearance that only ties still counts, so the
/// first of equal clearances keeps winning.
fn raising_clearance(
    target: &PreparedSupport,
    point: &ExactPoint,
    context: &SearchContext<'_>,
) -> Result<Option<Rational>> {
    if target.contains_area(point)? {
        return context.scaled_squared(&Rational::zero()).map(Some);
    }
    if let Some(lower) = &context.lower_squared {
        let hint = target.index.nearest(target, point, Some(Tightness::ANY))?;
        if context.scaled_squared(&hint.upper)?.try_cmp(lower)? == Ordering::Less {
            return Ok(None);
        }
    }
    let exact = target.index.nearest(target, point, None)?.lower;
    context.scaled_squared(&exact).map(Some)
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

    // Boundary patches are useful for the global enclosure, but they cannot
    // become a zero-clearance winner while a filled source has positive area.
    if let Some(witness_squared) = raising_clearance(target, &witness, context)?
        && !(context.exact_upper && context.require_positive_witness && witness_squared.is_zero())
    {
        if context.exact_upper {
            context.update_lower_with_witness(witness_squared, &witness)?;
        } else {
            context.update_lower(witness_squared)?;
        }
    }
    let (upper, site_squared) = patch_upper(&patch, source, target, context)?;
    if let Some(upper_value) = &upper
        && upper_is_below_lower(upper_value, context)?
    {
        return Ok(None);
    }
    // A maximizer inside the source area is a local maximum of the distance
    // to the target; maximizers on the source boundary belong to its span
    // patches.
    if let (SourcePatch::Box(bounds), Some(site_squared)) = (&patch, &site_squared)
        && target
            .index
            .excludes_local_maximum(target, *bounds, site_squared)?
    {
        return Ok(None);
    }
    Ok(Some(PatchRecord { patch, upper }))
}

fn lower_candidate(context: &SearchContext<'_>) -> Result<Option<f64>> {
    let Some(lower) = &context.lower_squared else {
        return Ok(None);
    };
    SqrtInterval::candidate_from_squared(lower)
}

fn candidate_certified(
    candidate: f64,
    upper: &Rational,
    context: &SearchContext<'_>,
) -> Result<bool> {
    let Some(lower_squared) = &context.lower_squared else {
        return Ok(false);
    };
    if !candidate.is_finite() {
        return Ok(false);
    }
    let error = context.accuracy.absolute_error(lower_squared, candidate)?;
    let candidate = Rational::from_f64(candidate);
    let high = candidate.try_add(&error)?;
    if upper.try_cmp(&high)? == Ordering::Greater {
        return Ok(false);
    }
    let low = candidate.try_subtract(&error)?;
    if low.sign() != ExactSign::Positive {
        return Ok(true);
    }
    let low_squared = low.try_square()?;
    Ok(lower_squared.try_cmp(&low_squared)? != Ordering::Less)
}

fn queue_upper(queue: &PatchQueue, context: &SearchContext<'_>) -> Result<Option<Rational>> {
    let Some(lower) = context.lower_squared.clone() else {
        return Ok(None);
    };
    let interval = sqrt_upper(&lower, context)?;
    let Some(witness_upper) = interval.upper_rational() else {
        return Ok(None);
    };
    if queue.is_empty() {
        return Ok(Some(witness_upper));
    }
    let Some(mut upper) = queue.max_upper() else {
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
    for patch in source.initial_patches() {
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

    pub(in crate::geometry) const fn source_support(&self) -> &PreparedSupport {
        &self.source
    }

    pub(in crate::geometry) const fn boundary_support(&self) -> &PreparedSupport {
        &self.boundary
    }

    pub(in crate::geometry) fn radius_in_scale(&self, scale: f64) -> Result<f64> {
        if !(scale.is_finite() && scale > 0.0) {
            return Err(crate::geometry::GeometryErrorKind::message(
                "maximum_inscribed_circle scale must be positive and finite",
            ));
        }
        let scale = Rational::from_f64(scale);
        let squared = self
            .center_squared
            .try_product(&scale)?
            .try_product(&scale)?;
        downward_sqrt(&squared)
    }
}

fn downward_sqrt(squared: &Rational) -> Result<f64> {
    let interval = SqrtInterval::from_squared_with_error(squared, None)?;
    Rational::from_dyadic(interval.lower).round()
}

fn movement_lower_squared(
    witness_squared: &Rational,
    movement_squared: &Rational,
) -> Result<Rational> {
    let witness_interval = SqrtInterval::from_squared_with_error(witness_squared, None)?;
    let movement_interval = SqrtInterval::from_squared_with_error(movement_squared, None)?;
    let witness_lower = Rational::from_dyadic(witness_interval.lower);
    let Some(movement_upper) = movement_interval.upper_rational() else {
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
    context: &SearchContext<'_>,
) -> Result<Option<DirectedCandidate>> {
    let (Some(witness), Some(witness_squared)) = (
        context.lower_witness.as_ref(),
        context.lower_squared.as_ref(),
    ) else {
        return Ok(None);
    };

    let center_exact = if witness_squared.is_zero() {
        source
            .first_boundary_point()
            .unwrap_or_else(|| witness.clone())
    } else {
        ExactPoint {
            x: Rational::from_f64(witness.x.round()?),
            y: Rational::from_f64(witness.y.round()?),
        }
    };
    let center = Point::new_raw_xy(center_exact.x.round()?, center_exact.y.round()?);
    if !center.x.is_finite() || !center.y.is_finite() {
        return Ok(None);
    }
    let center_exact = ExactPoint::from_xy(center.xy());
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
    let mut certified_squared = center_squared.clone();
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
    let certified_radius = Rational::from_f64(certified_radius);
    if upper_scaled.try_cmp(&certified_radius)? == Ordering::Less {
        return Err(crate::geometry::GeometryErrorKind::message(
            "directed inradius upper enclosure fell below its emitted clearance",
        ));
    }
    let gap = upper_scaled.try_subtract(&certified_radius)?;
    if gap.try_cmp(&context.output_tolerance)? == Ordering::Greater {
        return Ok(None);
    }

    #[cfg(test)]
    let proof = DirectedCandidateProof {
        witness: witness.clone(),
        witness_squared: witness_squared.clone(),
        certified_squared,
        upper: upper.clone(),
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
        // Four-way box splitting exhausted a budget of 64 refinements per box
        // on 16 adjacent unit boxes; halving the longest axis stays within it
        // (`directed_adjacent_box_refinement_work_scales_with_boundary_spans`).
        let Some(children) = record
            .patch
            .split_with_strategy(&mut context.budget, BoxSplitStrategy::LongestAxis)?
        else {
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
        ));
    }
    if !(output_scale.is_finite() && output_scale > 0.0) {
        return Err(crate::geometry::GeometryErrorKind::message(
            "maximum inscribed circle scale must be positive and finite",
        ));
    }
    let accuracy = Accuracy::new(Some(1.0), 1.0)?;
    let output_scale = Rational::from_f64(output_scale);
    let output_tolerance = Rational::from_f64(tolerance);
    let mut context = SearchContext::new_directed(&accuracy, output_scale, output_tolerance);
    let mut source = PreparedSupport::from_shape_with_boundary(shape, &mut context.budget)?;
    let target = source.boundary_clone()?;
    // Boundary incidence is only needed while constructing the independent
    // boundary view. The directed result keeps the source support for final
    // containment, but that path reads its spans/faces/index, not these flags.
    source.boundary_spans = None;
    source.boundary_points = None;
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
    let identical = left == right;
    if identical && !left.has_area_parts() {
        return Ok(0.0);
    }
    let source = PreparedSupport::from_shape(left, &mut context.budget)?;
    if identical {
        return Ok(0.0);
    }
    let target = PreparedSupport::from_shape(right, &mut context.budget)?;
    symmetric_distance(
        &source,
        &target,
        &mut context,
        || left.equals(right),
        |forward| {
            if forward {
                right.covers(left)
            } else {
                left.covers(right)
            }
        },
    )
}

/// `covered(forward)` reports whether the direction's target covers its
/// source (`forward` is left into right). It is asked only when every seeded
/// clearance is zero, where a covered direction would otherwise refine a
/// zero plateau along the shared boundary that no positive bound prunes.
fn symmetric_distance(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
    equal_support: impl FnOnce() -> Result<bool>,
    mut covered: impl FnMut(bool) -> Result<bool>,
) -> Result<f64> {
    for (source, target) in [(source, target), (target, source)] {
        for span in &source.spans {
            for parameter in [&span.t_lo, &span.t_hi] {
                let point = span_point(span, parameter)?;
                if let Some(squared) = raising_clearance(target, &point, context)? {
                    context.update_lower(squared)?;
                }
            }
        }
        for point in &source.points {
            if let Some(squared) = raising_clearance(target, point, context)? {
                context.update_lower(squared)?;
            }
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
    let seeded_zero = context
        .lower_squared
        .as_ref()
        .is_some_and(Rational::is_zero);
    let mut direction = |forward: bool, context: &mut SearchContext<'_>| {
        if seeded_zero && covered(forward)? {
            return Ok(Rational::from_f64(0.0));
        }
        let (source, target) = if forward {
            (source, target)
        } else {
            (target, source)
        };
        run_direction(source, target, context)
    };
    let forward = direction(true, context)?;
    let backward = direction(false, context)?;
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
        return Ok(f64::INFINITY);
    }
    if left_xs == right_xs && left_ys == right_ys {
        return Ok(0.0);
    }
    let source = PreparedSupport::from_line_columns(left_xs, left_ys)?;
    let target = PreparedSupport::from_line_columns(right_xs, right_ys)?;
    symmetric_distance(
        &source,
        &target,
        &mut context,
        || {
            let line = |xs: &[f64], ys: &[f64]| -> Result<Shape> {
                let points: Vec<_> = xs.iter().zip(ys).map(|(&x, &y)| XY::new(x, y)).collect();
                Ok(Shape::LineString(LineSeq::try_new(CoordSeq::try_from_xy(
                    &points,
                )?)?))
            };
            line(left_xs, left_ys)?.equals(&line(right_xs, right_ys)?)
        },
        // Linework has no interior plateau: a covered direction seeds its
        // zero from the other direction's positive clearances.
        |_| Ok(false),
    )
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

#[derive(Clone, Debug)]
pub(in crate::geometry) struct ExactPoint {
    x: Rational,
    y: Rational,
}

impl ExactPoint {
    fn from_xy(point: XY) -> Self {
        Self {
            x: Rational::from_f64(point.x),
            y: Rational::from_f64(point.y),
        }
    }

    fn from_homogeneous(value: [ExactDyadic; 3]) -> crate::error::Result<Self> {
        let [x, y, w] = value;
        Ok(Self {
            x: Rational::new(x, w.clone())?,
            y: Rational::new(y, w)?,
        })
    }

    fn same_position(&self, other: &Self) -> crate::error::Result<bool> {
        Ok(self.x.try_cmp(&other.x)?.is_eq() && self.y.try_cmp(&other.y)?.is_eq())
    }
}

#[cfg(test)]
mod tests;
