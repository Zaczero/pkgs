//! Continuous planar Hausdorff distance with a certified error bound.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::overlay::{ExactMetricSpan, ExactMetricSupport, ExactSourceInventory};
use crate::geometry::tessellation::exact;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational as Rational};
use crate::geometry::{
    CoordSeq, Coordinates as _, DdInterval, ExpansionBudget, LineSeq, PackedLineColumnView, Point,
    Polygonal, Segment, Shape, XY, storage,
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
    Accuracy, DistanceInterval, ErrorBound, ExactCarrier, FloatBox, SelectedSpan, SquaredBounds,
    SupportSpan, Tightness, UnreducedRatio,
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
    area_index: Arc<SupportIndex>,
    parallel_carriers: bool,
}

impl PreparedSupport {
    pub(in crate::geometry) fn from_shape<'a>(
        shape: impl Into<storage::ShapeRow<'a>>,
        budget: &mut ExpansionBudget,
    ) -> Result<Self> {
        let shape = shape.into();
        if !shape.has_area_parts() {
            let capacity = shape.segment_count_capacity_hint();
            let mut spans = Vec::new();
            if let Some(capacity) = capacity {
                let _reservation = spans.try_reserve(capacity);
            }
            let mut points = Vec::new();
            shape.try_for_each_segment(|segment| {
                push_raw_segment(segment, &mut spans, &mut points);
                Ok(())
            })?;
            shape.for_each_point_only(|point| {
                points.push(ExactPoint::from_xy(point.xy()));
            });
            return Self::from_parts(spans, points, Vec::new());
        }
        let inventory = ExactSourceInventory::from_rows([shape].into_iter())?;
        let partition = inventory.partition(budget)?;
        let support = inventory.metric_support(&partition)?;
        Self::from_metric_support(support)
    }

    pub(in crate::geometry) fn from_shape_with_boundary(
        shape: &Shape,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Self>> {
        if !shape.has_area_parts() {
            return Self::from_shape(shape, budget).map(Some);
        }
        // Valid polygon rings already are their filled support boundary.
        // Invalid/overlapping inputs still need the union partition to select
        // that boundary before distance and containment queries can use it.
        if let Some(polygonal) = shape.polygonal()
            && shape.validate()?.is_none()
        {
            return Self::from_valid_polygonal(&polygonal).map(Some);
        }
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let partition = inventory.partition(budget)?;
        let support = inventory.metric_support_with_boundary(&partition)?;
        if support.faces.is_empty() {
            return Ok(None);
        }
        Self::from_metric_support(support).map(Some)
    }

    fn from_valid_polygonal(shape: &Polygonal<'_>) -> Result<Self> {
        let mut spans = Vec::new();
        let mut points = Vec::new();
        let mut faces = Vec::new();
        let mut add_polygon = |polygon: &crate::geometry::Polygon| {
            if polygon.shell.coords().is_empty() {
                return;
            }
            let mut cycles = Vec::new();
            for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
                let cycle: Vec<_> = ring
                    .coords()
                    .iter_coords()
                    .map(|point| ExactPoint::from_xy(point.xy()))
                    .collect();
                if cycle.len() >= 3 {
                    cycles.push(cycle);
                }
                for [start, end] in ring.coords().segment_pairs() {
                    push_raw_segment(
                        Segment {
                            start: start.xy(),
                            end: end.xy(),
                        },
                        &mut spans,
                        &mut points,
                    );
                }
            }
            faces.push(cycles);
        };
        match shape {
            Polygonal::Polygon(polygon) => add_polygon(polygon),
            Polygonal::MultiPolygon(polygons) => {
                for polygon in polygons.iter() {
                    add_polygon(polygon);
                }
            },
            Polygonal::Empty => {},
        }
        Self::from_parts(spans, points, faces)
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
                push_raw_segment(Segment { start, end }, &mut spans, &mut points);
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
                if cycle.len() < 3 {
                    continue;
                }
                for (start, end) in cycle
                    .iter()
                    .zip(cycle.iter().cycle().skip(1))
                    .take(cycle.len())
                {
                    if start
                        .stored_xy()
                        .zip(end.stored_xy())
                        .is_some_and(|(start, end)| start == end)
                    {
                        continue;
                    }
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

        let parallel_carriers = may_have_parallel_carriers(&spans);
        let index = Arc::new(SupportIndex::build(&spans, &points)?);
        let same_edges = points.is_empty()
            && spans.len() == boundary_edges.len()
            && spans.iter().zip(&boundary_edges).all(|(span, edge)| {
                span.range.is_none()
                    && edge.start.stored_xy() == Some(span.source.start)
                    && edge.end.stored_xy() == Some(span.source.end)
            });
        let area_index = if same_edges {
            Arc::clone(&index)
        } else {
            Arc::new(SupportIndex::from_boundary_edges(&boundary_edges))
        };
        Ok(Self {
            spans: spans.into(),
            points: points.into(),
            faces,
            boundary_spans: None,
            boundary_points: None,
            boundary_edges,
            area_bounds,
            index,
            area_index,
            parallel_carriers,
        })
    }

    /// Clone the exact boundary selected by the partition's face walks without
    /// reconstructing it from rounded source coordinates. The aligned flags
    /// already exclude shared/internal face edges and lower-dimensional
    /// support hidden by a positive-area face.
    pub(in crate::geometry) fn boundary_clone(&self) -> Result<Self> {
        let span_flags = self.boundary_spans.as_deref();
        let point_flags = self.boundary_points.as_deref();
        if span_flags.is_none_or(|flags| flags.iter().all(|&selected| selected))
            && point_flags.is_none_or(|flags| flags.iter().all(|&selected| selected))
        {
            return Ok(Self {
                spans: Arc::clone(&self.spans),
                points: Arc::clone(&self.points),
                faces: Vec::new(),
                boundary_spans: None,
                boundary_points: None,
                boundary_edges: Vec::new(),
                area_bounds: None,
                index: Arc::clone(&self.index),
                area_index: Arc::default(),
                parallel_carriers: self.parallel_carriers,
            });
        }
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
                lo: span.lo().clone(),
                hi: span.hi().clone(),
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
        if self.has_positive_area() {
            let mut patches = Vec::with_capacity(self.faces.len());
            for face in &self.faces {
                let mut bounds = None;
                for point in face.iter().filter(|cycle| cycle.len() >= 3).flatten() {
                    let point_bounds = FloatBox::from_point(point)?;
                    bounds = Some(bounds.map_or(point_bounds, |bounds: FloatBox| {
                        bounds.include(point_bounds)
                    }));
                }
                if let Some(bounds) = bounds {
                    patches.push(SourcePatch::Box(bounds));
                }
            }
            return Ok(patches);
        }

        let mut patches = self.initial_patches();
        patches.reserve(self.points.len());
        for index in 0..self.points.len() {
            patches.push(SourcePatch::Point(index));
        }
        Ok(patches)
    }

    fn contains_area(&self, point: &ExactPoint) -> Result<bool> {
        // Partition faces have disjoint interiors. Shared internal edges
        // cross a ray twice, so parity over all cycles is their filled union.
        self.area_index.contains_area(&self.boundary_edges, point)
    }

    fn covers_support(&self, point: &ExactPoint) -> Result<bool> {
        if self.contains_area(point)? {
            return Ok(true);
        }
        if self.index.root.is_none() {
            return Ok(false);
        }
        Ok(self
            .index
            .nearest(self, point, None)?
            .bounds
            .lower
            .is_zero())
    }

    pub(in crate::geometry) fn candidate_is_contained(
        &self,
        boundary: &Self,
        center: Point,
        clearance_squared: &Rational,
        candidate: &[Point],
        counter_clockwise: bool,
    ) -> Result<bool> {
        let center = ExactPoint::from_xy(center.xy());
        // The directed result has already certified this center's membership
        // and clearance against the retained source and boundary views.
        // A convex polygon whose vertices lie in the center's clearance disk
        // cannot cross the source boundary, including an enclosed hole.
        let mut inside_disk = true;
        let mut radius_squared = Rational::zero();
        for &point in candidate {
            let point = ExactPoint::from_xy(point.xy());
            let squared = exact_point_distance_squared(&center, &point)?;
            if squared.try_cmp(&radius_squared)? == Ordering::Greater {
                radius_squared = squared.clone();
            }
            if squared.try_cmp(clearance_squared)? == Ordering::Greater {
                inside_disk = false;
                if !self.covers_support(&point)? {
                    return Ok(false);
                }
            }
        }
        if inside_disk {
            return Ok(true);
        }
        let radius_squared = nonnegative_upper_bound(&radius_squared)?;
        let center_bounds = FloatBox::from_point(&center)?;
        let center_xy = center.rounded_xy()?;
        // The selected boundary spans include retained hole cycles. Testing
        // the whole segment, rather than only its endpoints, rejects both an
        // inward chord crossing and a hole wholly enclosed by the candidate.
        for span in boundary.spans.iter() {
            if box_distance_lower(center_bounds, span.bounds) > radius_squared {
                continue;
            }
            let side = crate::geometry::orientation(span.source.start, span.source.end, center_xy);
            if side != crate::geometry::Orientation::Collinear
                && candidate.iter().all(|point| {
                    let vertex_side = crate::geometry::orientation(
                        span.source.start,
                        span.source.end,
                        point.xy(),
                    );
                    vertex_side == side || vertex_side == crate::geometry::Orientation::Collinear
                })
            {
                // The selected span lies on its original exact carrier.
                // A polygon in one closed carrier half-plane has no interior
                // on that line, including when it touches a candidate vertex.
                continue;
            }
            let start = span_point(span, span.lo())?;
            let end = span_point(span, span.hi())?;
            if exact_segment_enters_candidate(&start, &end, candidate, counter_clockwise)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(in crate::geometry) const fn has_positive_area(&self) -> bool {
        // Faces come from either valid polygon rings or selected non-exterior
        // partition faces; collapsed support lives only in spans and points.
        !self.faces.is_empty()
    }

    fn first_boundary_point(&self) -> Option<ExactPoint> {
        if let Some(span) = self.spans.first() {
            return Some(span.start_point());
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
            let center = ExactPoint::new(min_x.try_midpoint(&max_x)?, min_y.try_midpoint(&max_y)?);
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
                let dx = UnreducedRatio::try_difference(point.x(), self.points[index].x())?;
                let dy = UnreducedRatio::try_difference(point.y(), self.points[index].y())?;
                dx.try_square()?.try_add(&dy.try_square()?)
            },
        }
    }

    fn distance_squared(&self, point: &ExactPoint) -> Result<Rational> {
        if self.contains_area(point)? {
            return Ok(Rational::zero());
        }
        Ok(self.index.nearest(self, point, None)?.bounds.lower)
    }

    fn primitive_squared_bounds(
        &self,
        primitive: PrimitiveId,
        point: &ExactPoint,
        point_bounds: FloatBox,
        tightness: Option<Tightness>,
    ) -> Result<SquaredBounds> {
        if let Some(tightness) = tightness
            && let Some((lower, upper)) =
                self.primitive_interval_bounds(primitive, point_bounds, tightness)
        {
            return Ok(SquaredBounds::enclosing(lower, upper));
        }
        SquaredBounds::exact(
            self.primitive_distance_squared(primitive, point)?
                .to_rational()?,
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "PrimitiveId carries a stable span ordinal from this immutable PreparedSupport"
    )]
    fn primitive_interval_bounds(
        &self,
        primitive: PrimitiveId,
        point_bounds: FloatBox,
        tightness: Tightness,
    ) -> Option<(f64, f64)> {
        let PrimitiveId::Span(index) = primitive else {
            return None;
        };
        point_span_binary64_bounds(point_bounds, &self.spans[index])
            .filter(|&(lower, upper)| tightness.admits(lower, upper))
            .or_else(|| {
                point_span_squared_bounds_with::<exact::Interval>(point_bounds, &self.spans[index])
                    .filter(|&(lower, upper)| tightness.admits(lower, upper))
            })
            .or_else(|| {
                point_span_squared_bounds(point_bounds, &self.spans[index])
                    .filter(|&(lower, upper)| tightness.admits(lower, upper))
            })
    }
}

/// Disjoint slope enclosures prove that no two exact carriers are parallel.
/// An unresolved slope or overlapping pair keeps the exact strip certificate
/// available; this filter only removes a nomination query that cannot help.
fn may_have_parallel_carriers(spans: &[SupportSpan]) -> bool {
    let mut slopes = Vec::with_capacity(spans.len());
    let mut vertical = false;
    for span in spans {
        let (a, b) = (span.source.start, span.source.end);
        if a.x <= b.x && b.x <= a.x {
            if vertical {
                return true;
            }
            vertical = true;
            continue;
        }
        let dx = exact::Interval::exact(b.x).sub(exact::Interval::exact(a.x));
        let dy = exact::Interval::exact(b.y).sub(exact::Interval::exact(a.y));
        if !dx.is_finite() || !dy.is_finite() || (dx.lo <= 0.0 && dx.hi >= 0.0) {
            return true;
        }
        let quotients = [dy.lo / dx.lo, dy.lo / dx.hi, dy.hi / dx.lo, dy.hi / dx.hi];
        if quotients.iter().any(|value| !value.is_finite()) {
            return true;
        }
        let low = quotients
            .into_iter()
            .fold(f64::INFINITY, f64::min)
            .next_down();
        let high = quotients
            .into_iter()
            .fold(f64::NEG_INFINITY, f64::max)
            .next_up();
        slopes.push((low, high));
    }
    slopes.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
    slopes
        .array_windows::<2>()
        .any(|[left, right]| right.0 <= left.1)
}

/// A full segment and a singleton query admit an absolute distance-error
/// filter. Normal squared norms exclude square-root underflow; the 128-unit-
/// roundoff allowance covers subtraction, projection/clamping, cross product,
/// square root, and division, including an uncertain end-normal decision.
fn point_span_binary64_bounds(point: FloatBox, span: &SupportSpan) -> Option<(f64, f64)> {
    if span.range.is_some() {
        return None;
    }
    if !(point.min_x.to_bits() == point.max_x.to_bits()
        && point.min_y.to_bits() == point.max_y.to_bits())
    {
        return None;
    }
    let (a, b) = (span.source.start, span.source.end);
    let (vx, vy) = (b.x - a.x, b.y - a.y);
    let (px, py) = (point.min_x - a.x, point.min_y - a.y);
    let (qx, qy) = (point.min_x - b.x, point.min_y - b.y);
    let norm = vx * vx + vy * vy;
    let dot = px * vx + py * vy;
    if !(norm.is_normal() && norm > 0.0 && dot.is_finite()) {
        return None;
    }
    let endpoint = |x: f64, y: f64| {
        let squared = x * x + y * y;
        squared.is_normal().then(|| squared.sqrt())
    };
    let distance = if dot <= 0.0 {
        endpoint(px, py)?
    } else if dot >= norm {
        endpoint(qx, qy)?
    } else {
        (px * vy - py * vx).abs() / norm.sqrt()
    };
    let magnitude = vx
        .abs()
        .max(vy.abs())
        .max(px.abs())
        .max(py.abs())
        .max(qx.abs())
        .max(qy.abs());
    let error = (64.0 * f64::EPSILON * magnitude).next_up();
    let lower = (distance - error).next_down().max(0.0);
    let upper = (distance + error).next_up();
    let lower = (lower * lower).next_down().max(0.0);
    let upper = (upper * upper).next_up();
    (distance.is_finite() && lower.is_finite() && upper.is_finite()).then_some((lower, upper))
}

fn push_raw_segment(source: Segment, spans: &mut Vec<SupportSpan>, points: &mut Vec<ExactPoint>) {
    if source.start == source.end {
        points.push(ExactPoint::from_xy(source.start));
        return;
    }
    spans.push(raw_support_span(source));
}

const fn raw_support_span(source: Segment) -> SupportSpan {
    SupportSpan {
        source,
        carrier: std::sync::OnceLock::new(),
        range: None,
        bounds: FloatBox {
            min_x: source.start.x.min(source.end.x),
            max_x: source.start.x.max(source.end.x),
            min_y: source.start.y.min(source.end.y),
            max_y: source.start.y.max(source.end.y),
        },
    }
}

fn support_span(span: ExactMetricSpan) -> Result<SupportSpan> {
    if span.start_parameter[0].is_zero()
        && span.end_parameter[0].cmp(&span.end_parameter[1]) == Ordering::Equal
    {
        return Ok(raw_support_span(span.source));
    }
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
        carrier: std::sync::OnceLock::new(),
        range: Some(Box::new(SelectedSpan {
            parameters: [t_lo, t_hi],
            start,
        })),
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
    let carrier = span.carrier()?;
    let x = carrier.ax.try_add(&carrier.dx.try_product(parameter)?)?;
    let y = carrier.ay.try_add(&carrier.dy.try_product(parameter)?)?;
    Ok(ExactPoint::new(x, y))
}

fn span_projection_parameter(point: &ExactPoint, span: &SupportSpan) -> Result<Rational> {
    let carrier = span.carrier()?;
    let px = point.x().try_subtract(&carrier.ax)?;
    let py = point.y().try_subtract(&carrier.ay)?;
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
    let carrier = span.carrier()?;
    let px = UnreducedRatio::try_difference(point.x(), &carrier.ax)?;
    let py = UnreducedRatio::try_difference(point.y(), &carrier.ay)?;
    let dx = UnreducedRatio::from_rational(&carrier.dx)?;
    let dy = UnreducedRatio::from_rational(&carrier.dy)?;
    let norm_squared = UnreducedRatio::from_rational(&carrier.norm_squared)?;
    let dot = px.try_product(&dx)?.try_add(&py.try_product(&dy)?)?;
    let low = UnreducedRatio::from_rational(span.lo())?.try_product(&norm_squared)?;
    let high = UnreducedRatio::from_rational(span.hi())?.try_product(&norm_squared)?;
    let parameter = if carrier.norm_squared.is_zero() || dot.try_cmp(&low)? == Ordering::Less {
        span.lo()
    } else if dot.try_cmp(&high)? == Ordering::Greater {
        span.hi()
    } else {
        let cross = px.try_product(&dy)?.try_subtract(&py.try_product(&dx)?)?;
        return cross.try_square()?.try_quotient(&norm_squared);
    };
    let dx = span_coordinate_difference(point.x(), &carrier.ax, &carrier.dx, parameter)?;
    let dy = span_coordinate_difference(point.y(), &carrier.ay, &carrier.dy, parameter)?;
    dx.try_square()?.try_add(&dy.try_square()?)
}

/// Double-double bounds on the squared distance from any point of
/// `point_bounds` to a span covering its whole source segment. The dot and
/// cross products with the segment direction are affine in the point, so the
/// box corners bound them; a box straddling the segment's end normals, a
/// partial span, or a non-finite stage declines to the exact evaluation.
fn point_span_squared_bounds(point_bounds: FloatBox, span: &SupportSpan) -> Option<(f64, f64)> {
    point_span_squared_bounds_with::<DdInterval>(point_bounds, span)
}

fn point_span_squared_bounds_with<I: DistanceInterval>(
    point_bounds: FloatBox,
    span: &SupportSpan,
) -> Option<(f64, f64)> {
    if span.range.is_some() {
        return None;
    }
    let (start, end) = (span.source.start, span.source.end);
    let dx = I::delta(end.x, start.x)?;
    let dy = I::delta(end.y, start.y)?;
    let (norm_lower, norm_upper) = dx
        .square_bounds()?
        .add_bounds(dy.square_bounds()?)?
        .enclosure();
    if !(norm_lower > 0.0 && norm_upper.is_finite()) {
        return None;
    }
    let (mut dot_lower, mut dot_upper) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut cross_lower, mut cross_upper) = (f64::INFINITY, f64::NEG_INFINITY);
    for (x, y) in point_bounds.distinct_corners() {
        let px = I::delta(x, start.x)?;
        let py = I::delta(y, start.y)?;
        let (low, high) = dx
            .mul_bounds(px)?
            .add_bounds(dy.mul_bounds(py)?)?
            .enclosure();
        dot_lower = dot_lower.min(low);
        dot_upper = dot_upper.max(high);
        let (low, high) = dx
            .mul_bounds(py)?
            .sub_bounds(dy.mul_bounds(px)?)?
            .enclosure();
        cross_lower = cross_lower.min(low);
        cross_upper = cross_upper.max(high);
    }
    if !(dot_lower.is_finite() && dot_upper.is_finite()) {
        return None;
    }
    let endpoint = |target: XY| -> Option<(f64, f64)> {
        let mut upper: f64 = 0.0;
        for (x, y) in point_bounds.distinct_corners() {
            let px = I::delta(x, target.x)?;
            let py = I::delta(y, target.y)?;
            upper = upper.max(
                px.square_bounds()?
                    .add_bounds(py.square_bounds()?)?
                    .enclosure()
                    .1,
            );
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
    supporting_line_lower_squared_with::<exact::Interval>(point_bounds, span)
}

fn supporting_line_lower_squared_with<I: DistanceInterval>(
    point_bounds: FloatBox,
    span: &SupportSpan,
) -> f64 {
    let Some(dx) = I::delta(span.source.end.x, span.source.start.x) else {
        return 0.0;
    };
    let Some(dy) = I::delta(span.source.end.y, span.source.start.y) else {
        return 0.0;
    };

    let mut cross_lower = f64::INFINITY;
    let mut cross_upper = f64::NEG_INFINITY;
    for (x, y) in point_bounds.distinct_corners() {
        let Some(px) = I::delta(x, span.source.start.x) else {
            return 0.0;
        };
        let Some(py) = I::delta(y, span.source.start.y) else {
            return 0.0;
        };
        let Some(left) = dx.mul_bounds(py) else {
            return 0.0;
        };
        let Some(right) = dy.mul_bounds(px) else {
            return 0.0;
        };
        let Some(cross) = left.sub_bounds(right) else {
            return 0.0;
        };
        let (lower, upper) = cross.enclosure();
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

    let Some(norm) = dx
        .square_bounds()
        .and_then(|x| dy.square_bounds().and_then(|y| x.add_bounds(y)))
    else {
        return 0.0;
    };
    let (norm_lower, norm_upper) = norm.enclosure();
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
    Ok(nonnegative_bounds(value)?.1)
}

fn nonnegative_bounds(value: &Rational) -> Result<(f64, f64)> {
    if value.try_cmp(&Rational::from_f64(f64::MAX))? == Ordering::Greater {
        Ok((f64::MAX, f64::INFINITY))
    } else {
        value.outward_bounds()
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

/// Squared distance from `point` to the segment's sub-span whose projection
/// parameters lie in `[low, high]` (`(0.0, 1.0)` is the whole segment).
fn point_segment_squared_f64(point: XY, start: XY, end: XY, (low, high): (f64, f64)) -> f64 {
    let (dx, dy) = (end.x - start.x, end.y - start.y);
    let (px, py) = (point.x - start.x, point.y - start.y);
    let length = dx * dx + dy * dy;
    let parameter = if length > 0.0 {
        ((px * dx + py * dy) / length).clamp(low, high)
    } else {
        low
    };
    let (ex, ey) = (px - parameter * dx, py - parameter * dy);
    ex * ex + ey * ey
}

fn box_distance_lower(left: FloatBox, right: FloatBox) -> f64 {
    let gaps = [
        right.min_x - left.max_x,
        left.min_x - right.max_x,
        right.min_y - left.max_y,
        left.min_y - right.max_y,
    ];
    let dx = gaps[0].max(gaps[1]).max(0.0);
    let dy = gaps[2].max(gaps[3]).max(0.0);
    let squared = dx * dx + dy * dy;
    // Subtraction, squaring, summation, and the final multiply use fewer than
    // eight unit roundoffs. A 32-roundoff bias also covers subnormal products
    // when the sum is normal; below that range zero is the safe lower bound.
    if squared >= f64::MIN_POSITIVE && gaps.iter().all(|value| !value.is_nan()) {
        squared.min(f64::MAX) * (1.0 - 16.0 * f64::EPSILON)
    } else {
        0.0
    }
}

enum CycleLocation {
    Boundary,
    /// Off the cycle, with an odd number of crossings of the rightward ray.
    Crosses,
    Clear,
}

/// Classify one ray contact without rounding constructed boundary vertices.
fn edge_location(point: &ExactPoint, edge: &BoundaryEdge) -> Result<CycleLocation> {
    if let (Some(point), Some(left), Some(right)) = (
        point.stored_xy(),
        edge.start.stored_xy(),
        edge.end.stored_xy(),
    ) {
        return ray_edge_location(point, left, right);
    }
    let (left, right) = (&edge.start, &edge.end);
    ray_edge_location(point, left, right)
}

trait RayPoint: Copy {
    fn compare_y(self, other: Self) -> Result<Ordering>;
    fn between_x(self, left: Self, right: Self) -> Result<bool>;
    fn orientation(self, left: Self, right: Self) -> Result<ExactSign>;
}

impl RayPoint for XY {
    fn compare_y(self, other: Self) -> Result<Ordering> {
        Ok(self
            .y
            .partial_cmp(&other.y)
            .expect("prepared coordinates are finite"))
    }

    fn between_x(self, left: Self, right: Self) -> Result<bool> {
        Ok((left.x <= self.x && self.x <= right.x) || (right.x <= self.x && self.x <= left.x))
    }

    fn orientation(self, left: Self, right: Self) -> Result<ExactSign> {
        Ok(match crate::geometry::orientation(left, right, self) {
            crate::geometry::Orientation::Collinear => ExactSign::Zero,
            crate::geometry::Orientation::Clockwise => ExactSign::Negative,
            crate::geometry::Orientation::CounterClockwise => ExactSign::Positive,
        })
    }
}

impl RayPoint for &ExactPoint {
    fn compare_y(self, other: Self) -> Result<Ordering> {
        self.y().try_cmp(other.y())
    }

    fn between_x(self, left: Self, right: Self) -> Result<bool> {
        between(self.x(), left.x(), right.x())
    }

    fn orientation(self, left: Self, right: Self) -> Result<ExactSign> {
        let dx = right.x().try_subtract(left.x())?;
        let dy = right.y().try_subtract(left.y())?;
        let px = self.x().try_subtract(left.x())?;
        let py = self.y().try_subtract(left.y())?;
        Ok(dx
            .try_product(&py)?
            .try_subtract(&dy.try_product(&px)?)?
            .sign())
    }
}

fn ray_edge_location<P: RayPoint>(point: P, left: P, right: P) -> Result<CycleLocation> {
    let left_order = left.compare_y(point)?;
    let right_order = right.compare_y(point)?;
    let straddles = (left_order == Ordering::Greater) != (right_order == Ordering::Greater);
    let spans_y = left_order != right_order || left_order == Ordering::Equal;
    let holds_point = spans_y && point.between_x(left, right)?;
    if !straddles && !holds_point {
        return Ok(CycleLocation::Clear);
    }
    let cross = point.orientation(left, right)?;
    if holds_point && cross == ExactSign::Zero {
        return Ok(CycleLocation::Boundary);
    }
    let rising = right_order == Ordering::Greater;
    Ok(
        if straddles
            && ((rising && cross == ExactSign::Positive)
                || (!rising && cross == ExactSign::Negative))
        {
            CycleLocation::Crosses
        } else {
            CycleLocation::Clear
        },
    )
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
    let dx = edge.end.x().try_subtract(edge.start.x())?;
    let dy = edge.end.y().try_subtract(edge.start.y())?;
    for x in [bounds.min_x, bounds.max_x] {
        if dx.is_zero() {
            continue;
        }
        let x = Rational::from_f64(x);
        let parameter = x.try_subtract(edge.start.x())?.try_quotient(&dx)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let y = edge.start.y().try_add(&dy.try_product(&parameter)?)?;
        if y.try_cmp(&Rational::from_f64(bounds.min_y))? != Ordering::Less
            && y.try_cmp(&Rational::from_f64(bounds.max_y))? != Ordering::Greater
        {
            return Ok(Some(ExactPoint::new(x, y)));
        }
    }
    for y in [bounds.min_y, bounds.max_y] {
        if dy.is_zero() {
            continue;
        }
        let y = Rational::from_f64(y);
        let parameter = y.try_subtract(edge.start.y())?.try_quotient(&dy)?;
        if parameter.try_cmp(&Rational::zero())? == Ordering::Less
            || parameter.try_cmp(&Rational::one())? == Ordering::Greater
        {
            continue;
        }
        let x = edge.start.x().try_add(&dx.try_product(&parameter)?)?;
        if x.try_cmp(&Rational::from_f64(bounds.min_x))? != Ordering::Less
            && x.try_cmp(&Rational::from_f64(bounds.max_x))? != Ordering::Greater
        {
            return Ok(Some(ExactPoint::new(x, y)));
        }
    }
    Ok(None)
}

fn point_in_box(point: &ExactPoint, bounds: FloatBox) -> Result<bool> {
    let min_x = Rational::from_f64(bounds.min_x);
    let max_x = Rational::from_f64(bounds.max_x);
    let min_y = Rational::from_f64(bounds.min_y);
    let max_y = Rational::from_f64(bounds.max_y);
    Ok(point.x().try_cmp(&min_x)? != Ordering::Less
        && point.x().try_cmp(&max_x)? != Ordering::Greater
        && point.y().try_cmp(&min_y)? != Ordering::Less
        && point.y().try_cmp(&max_y)? != Ordering::Greater)
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
    let source_x = end.x().try_subtract(start.x())?;
    let source_y = end.y().try_subtract(start.y())?;

    for (&left, &right) in candidate
        .iter()
        .zip(candidate.iter().cycle().skip(1))
        .take(candidate.len())
    {
        let edge_x = Rational::from_f64(right.x).try_subtract(&Rational::from_f64(left.x))?;
        let edge_y = Rational::from_f64(right.y).try_subtract(&Rational::from_f64(left.y))?;
        let start_x = start.x().try_subtract(&Rational::from_f64(left.x))?;
        let start_y = start.y().try_subtract(&Rational::from_f64(left.y))?;
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
                let center =
                    ExactPoint::new(min_x.try_midpoint(&max_x)?, min_y.try_midpoint(&max_y)?);
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
    span.carrier()?
        .norm_squared
        .try_product(&half.try_square()?)
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

    fn max_overflow_upper(&self) -> Result<Option<Rational>> {
        let mut maximum: Option<Rational> = None;
        for entry in &self.heap {
            if !entry.key.is_infinite() {
                continue;
            }
            let Some(upper) = &entry.record.upper else {
                return Ok(None);
            };
            let replaces = match &maximum {
                None => true,
                Some(current) => upper.try_cmp(current)? == Ordering::Greater,
            };
            if replaces {
                maximum = Some(upper.clone());
            }
        }
        Ok(maximum)
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
    let carrier = span.carrier()?;
    let px = point.x().try_subtract(&carrier.ax)?;
    let py = point.y().try_subtract(&carrier.ay)?;
    carrier
        .dx
        .try_product(&py)?
        .try_subtract(&carrier.dy.try_product(&px)?)
}

fn exact_parallel(left: &SupportSpan, right: &SupportSpan) -> Result<bool> {
    let difference = |end, start| exact::Interval::exact(end).sub(exact::Interval::exact(start));
    let lx = difference(left.source.end.x, left.source.start.x);
    let ly = difference(left.source.end.y, left.source.start.y);
    let rx = difference(right.source.end.x, right.source.start.x);
    let ry = difference(right.source.end.y, right.source.start.y);
    if [lx, ly, rx, ry].into_iter().all(exact::Interval::is_finite) {
        let first = lx.mul(ry);
        let second = ly.mul(rx);
        if first.is_finite() && second.is_finite() {
            let cross = first.sub(second);
            if cross.is_finite() && cross.sign().is_some() {
                return Ok(false);
            }
        }
    }
    let left = left.carrier()?;
    let right = right.carrier()?;
    Ok(left
        .dx
        .try_product(&right.dy)?
        .try_subtract(&left.dy.try_product(&right.dx)?)?
        .is_zero())
}

struct InradiusPlane<'a> {
    span: &'a SupportSpan,
    dx: Rational,
    dy: Rational,
    clearance: Rational,
    length_lower: Rational,
    side: crate::geometry::Orientation,
}

impl InradiusPlane<'_> {
    fn encloses_source(&self, source: &PreparedSupport) -> Result<bool> {
        for point in source.faces.iter().flatten().flatten() {
            let side = match point.stored_xy() {
                Some(point) => crate::geometry::orientation(
                    self.span.source.start,
                    self.span.source.end,
                    point,
                ),
                None => match carrier_line_value(self.span, point)?.sign() {
                    ExactSign::Positive => crate::geometry::Orientation::CounterClockwise,
                    ExactSign::Negative => crate::geometry::Orientation::Clockwise,
                    ExactSign::Zero => crate::geometry::Orientation::Collinear,
                },
            };
            if side != self.side && side != crate::geometry::Orientation::Collinear {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the finite normalized equations only propose a witness; source membership and clearance are recomputed exactly"
    )]
    fn normalized_equation(&self, radius: f64) -> Result<Option<(f64, f64, f64)>> {
        let length = self.length_lower.round()?;
        let nx = -self.dy.round()? / length;
        let ny = self.dx.round()? / length;
        let rhs = radius - self.clearance.round()? / length;
        Ok((nx.is_finite() && ny.is_finite() && rhs.is_finite()).then_some((nx, ny, rhs)))
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "the finite linear solve is a witness nomination, never an upper bound or membership decision"
)]
fn inradius_plane_witness(
    planes: [&InradiusPlane<'_>; 3],
    upper: &Rational,
    probe: &ExactPoint,
) -> Result<Option<ExactPoint>> {
    let radius = upper.round()?;
    let [left, middle, right] = planes.map(|plane| plane.normalized_equation(radius));
    let [Some(left), Some(middle), Some(right)] = [left?, middle?, right?] else {
        return Ok(None);
    };
    let mut best = None;
    let mut best_determinant = 0.0;
    for ((ax, ay, ar), (bx, by, br)) in [(left, middle), (left, right), (middle, right)] {
        let determinant = ax * by - ay * bx;
        if determinant.abs() > best_determinant {
            best_determinant = determinant.abs();
            best = Some((
                (ar * by - ay * br) / determinant,
                (ax * br - ar * bx) / determinant,
            ));
        }
    }
    let Some((dx, dy)) = best else {
        return Ok(None);
    };
    let origin = probe.rounded_xy()?;
    let xy = XY::new(origin.x + dx, origin.y + dy);
    Ok((xy.x.is_finite() && xy.y.is_finite()).then(|| ExactPoint::from_xy(xy)))
}

/// A nonnegative combination of three enclosing half-planes eliminates the
/// center coordinates. For oriented edge directions `e_i`, the cyclic cross
/// products give weights with `sum(weight_i * e_i) == 0` exactly. Consequently
/// every contained disk obeys `radius <= sum(weight_i * H_i) /
/// sum(weight_i * |e_i|)`. Downward length enclosures make that quotient an
/// upper bound; nominated floating centers are independently source-certified.
fn inradius_carrier_enclosure(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &SearchContext<'_>,
    current_upper: &Rational,
) -> Result<Option<(Rational, Option<ExactPoint>)>> {
    let Some(probe) = &context.lower_witness else {
        return Ok(None);
    };
    let nominated = target.index.nominate(target, probe)?;
    let mut planes = Vec::with_capacity(nominated.len());
    for primitive in nominated {
        let PrimitiveId::Span(index) = primitive else {
            continue;
        };
        let span = target.spans.get(index).expect("nominated boundary span");
        let clearance = carrier_line_value(span, probe)?;
        let sign = clearance.sign();
        if sign == ExactSign::Zero {
            continue;
        }
        let carrier = span.carrier()?;
        let orient = |value: &Rational| {
            if sign == ExactSign::Negative {
                Rational::zero().try_subtract(value)
            } else {
                Ok(value.clone())
            }
        };
        let interval = SqrtInterval::from_squared_with_error(&carrier.norm_squared, None)?;
        let length_lower = Rational::from_dyadic(interval.lower);
        if length_lower.is_zero() {
            continue;
        }
        planes.push(InradiusPlane {
            span,
            dx: orient(&carrier.dx)?,
            dy: orient(&carrier.dy)?,
            clearance: orient(&clearance)?,
            length_lower,
            side: if sign == ExactSign::Positive {
                crate::geometry::Orientation::CounterClockwise
            } else {
                crate::geometry::Orientation::Clockwise
            },
        });
    }
    let cross = |left: &InradiusPlane<'_>, right: &InradiusPlane<'_>| {
        left.dx
            .try_product(&right.dy)?
            .try_subtract(&left.dy.try_product(&right.dx)?)
    };
    let mut best = None;
    for (left_index, left) in planes.iter().enumerate() {
        for (middle_index, middle) in planes.iter().enumerate().skip(left_index.wrapping_add(1)) {
            for right in planes.iter().skip(middle_index.wrapping_add(1)) {
                let triple = [left, middle, right];
                let mut weights = [
                    cross(middle, right)?,
                    cross(right, left)?,
                    cross(left, middle)?,
                ];
                if weights
                    .iter()
                    .all(|weight| weight.sign() != ExactSign::Positive)
                {
                    for weight in &mut weights {
                        *weight = Rational::zero().try_subtract(weight)?;
                    }
                }
                if weights
                    .iter()
                    .any(|weight| weight.sign() == ExactSign::Negative)
                {
                    continue;
                }
                let mut numerator = Rational::zero();
                let mut denominator = Rational::zero();
                for (weight, plane) in weights.iter().zip(triple) {
                    numerator = numerator.try_add(&weight.try_product(&plane.clearance)?)?;
                    denominator = denominator.try_add(&weight.try_product(&plane.length_lower)?)?;
                }
                if denominator.is_zero() {
                    continue;
                }
                let upper = numerator.try_quotient(&denominator)?;
                let ceiling = best.as_ref().map_or(current_upper, |(upper, _)| upper);
                if upper.try_cmp(ceiling)? != Ordering::Less {
                    continue;
                }
                let mut valid = true;
                for (weight, plane) in weights.iter().zip(triple) {
                    if !weight.is_zero() && !plane.encloses_source(source)? {
                        valid = false;
                        break;
                    }
                }
                if valid {
                    let witness = inradius_plane_witness(triple, &upper, probe)?;
                    best = Some((upper, witness));
                }
            }
        }
    }
    Ok(best)
}

fn projection_is_in_span(point: &ExactPoint, span: &SupportSpan) -> Result<bool> {
    let parameter = span_projection_parameter(point, span)?;
    Ok(parameter.try_cmp(span.lo())? != Ordering::Less
        && parameter.try_cmp(span.hi())? != Ordering::Greater)
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
    if !target.parallel_carriers {
        return Ok(None);
    }
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
    let area_probe = context.exact_upper
        && matches!(patch, SourcePatch::Box(bounds) if source.area_bounds.is_some_and(|area| {
            bounds.min_x <= area.min_x && bounds.max_x >= area.max_x
                && bounds.min_y <= area.min_y && bounds.max_y >= area.max_y
        }));
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
            let separation = carrier_line_value(left, &right.start_point())?;
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
                .try_quotient(&left.carrier()?.norm_squared)?;
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
                let depth = sqrt_upper(&context.scaled_squared(&boundary.bounds.lower)?, context)?;
                let depth = Rational::from_dyadic(depth.lower);
                if radius.try_cmp(&depth)? == Ordering::Greater {
                    radius.try_subtract(&depth).map(Some)
                } else {
                    Ok(Some(Rational::zero()))
                }
            })?
    } else {
        let boundary = sqrt_upper(&context.scaled_squared(&boundary.bounds.upper)?, context)?;
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
        if context.scaled_squared(&hint.bounds.upper)?.try_cmp(lower)? == Ordering::Less {
            return Ok(None);
        }
    }
    let exact = target.index.nearest(target, point, None)?.bounds.lower;
    context.scaled_squared(&exact).map(Some)
}

fn build_patch(
    patch: SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<Option<PatchRecord>> {
    if context.exact_upper
        && context.lower_squared.is_some()
        && let SourcePatch::Box(bounds) = patch
    {
        match directed_box_record(bounds, source, target, context)? {
            DirectedBoxBound::ExactNeeded => {},
            DirectedBoxBound::Pruned => return Ok(None),
            DirectedBoxBound::Enclosed(record) => return Ok(Some(record)),
        }
    }
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

#[expect(
    clippy::large_enum_variant,
    reason = "the enclosed cell is the hot result; boxing it allocates for every queued box"
)]
enum DirectedBoxBound {
    ExactNeeded,
    Pruned,
    Enclosed(PatchRecord),
}

/// Signed boundary distance is one-Lipschitz. Its certified value at a
/// representable center plus the farthest box-corner distance encloses every
/// interior clearance; parallel-site bounds close flat maxima without
/// refining the length of a plateau down to the tolerance.
fn directed_box_record(
    bounds: FloatBox,
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<DirectedBoxBound> {
    let x = f64::midpoint(bounds.min_x, bounds.max_x);
    let y = f64::midpoint(bounds.min_y, bounds.max_y);
    let dx = (x - bounds.min_x)
        .abs()
        .max((bounds.max_x - x).abs())
        .next_up();
    let dy = (y - bounds.min_y)
        .abs()
        .max((bounds.max_y - y).abs())
        .next_up();
    let radius_squared = ((dx * dx).next_up() + (dy * dy).next_up()).next_up();
    let radius = radius_squared.sqrt().next_up();
    let tightness = context.tightness();
    if !radius_squared.is_normal()
        || !radius.is_finite()
        || 64.0 * f64::EPSILON * radius > tightness.absolute
    {
        return Ok(DirectedBoxBound::ExactNeeded);
    }
    let center = ExactPoint::from_xy(XY::new(x, y));
    let nearest = target.index.nearest(target, &center, Some(tightness))?;
    let distance_upper = nearest.bounds.upper_f64.sqrt().next_up();
    let distance_lower = nonnegative_bounds(&nearest.bounds.lower)?
        .0
        .max(0.0)
        .sqrt()
        .next_down()
        .max(0.0);
    if (distance_upper - distance_lower).next_up() > tightness.absolute {
        return Ok(DirectedBoxBound::ExactNeeded);
    }
    let inside = source.contains_area(&center)?;
    let distance = if inside {
        distance_upper
    } else {
        -distance_lower
    };
    let upper = (distance + radius).next_up().max(0.0);
    if !upper.is_finite() {
        return Ok(DirectedBoxBound::ExactNeeded);
    }
    if inside && !nearest.bounds.lower.is_zero() {
        // A certified lower distance is enough to nominate a witness. The
        // rounded-center certificate recomputes its exact clearance at emission.
        let clearance = context.scaled_squared(&nearest.bounds.lower)?;
        context.update_lower_with_witness(clearance, &center)?;
    }
    let mut upper = Rational::from_f64(upper);
    let patch = SourcePatch::Box(bounds);
    if target.parallel_carriers {
        let nominated = target.index.nominate(target, &center)?;
        if let Some(strip) = parallel_strip_upper(&patch, source, target, &nominated, context)?
            && strip.try_cmp(&upper)? == Ordering::Less
        {
            upper = strip;
        }
    }
    if upper_is_below_lower(&upper, context)? {
        return Ok(DirectedBoxBound::Pruned);
    }
    Ok(DirectedBoxBound::Enclosed(PatchRecord {
        patch,
        upper: Some(upper),
    }))
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

fn check_hausdorff_output_range(context: &SearchContext<'_>) -> Result<()> {
    let Some(lower) = &context.lower_squared else {
        return Ok(());
    };
    let maximum = Rational::from_f64(f64::MAX);
    if lower.try_cmp(&maximum.try_square()?)? != Ordering::Greater {
        return Ok(());
    }
    let error = context.accuracy.absolute_error(lower, f64::MAX)?;
    if lower.try_cmp(&maximum.try_add(&error)?.try_square()?)? == Ordering::Greater {
        return Err(crate::geometry::GeometryErrorKind::message(
            "hausdorff_distance cannot represent the effective accuracy",
        ));
    }
    Ok(())
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
        if !context.exact_upper && interval.upper.is_none() {
            check_hausdorff_output_range(context)?;
        }
        return Ok(Some(witness_upper));
    }
    let maximum = match queue.max_upper() {
        Some(upper) => Some(upper),
        None if !context.exact_upper => {
            if interval.upper.is_none() {
                check_hausdorff_output_range(context)?;
            }
            queue.max_overflow_upper()?
        },
        None => None,
    };
    let Some(mut upper) = maximum else {
        return Ok(None);
    };
    if upper.try_cmp(&witness_upper)? == Ordering::Less {
        upper = witness_upper;
    }
    Ok(Some(upper))
}

#[expect(
    clippy::indexing_slicing,
    reason = "nearest primitive IDs are created from this immutable target support"
)]
fn primitive_endpoints(
    support: &PreparedSupport,
    primitive: PrimitiveId,
) -> Result<[ExactPoint; 2]> {
    match primitive {
        PrimitiveId::Span(index) => {
            let span = &support.spans[index];
            Ok([span_point(span, span.lo())?, span_point(span, span.hi())?])
        },
        PrimitiveId::Point(index) => {
            let point = support.points[index].clone();
            Ok([point.clone(), point])
        },
    }
}

fn shared_primitive_vertex(
    support: &PreparedSupport,
    first: PrimitiveId,
    second: PrimitiveId,
) -> Result<Option<ExactPoint>> {
    let first_endpoints = primitive_endpoints(support, first)?;
    let second_endpoints = primitive_endpoints(support, second)?;
    for left in &first_endpoints {
        for right in &second_endpoints {
            if left.same_position(right)? {
                return Ok(Some(left.clone()));
            }
        }
    }
    Ok(None)
}

/// Use a shared target vertex to seed a span split when its endpoint sites differ.
/// The nearest-site query supplies only a partition hint; bounds still certify pruning.
#[expect(
    clippy::indexing_slicing,
    reason = "source span ordinals come from this immutable support's source patches"
)]
fn endpoint_site_split_parameter(
    patch: &SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
) -> Result<Option<Rational>> {
    let SourcePatch::Span { span, lo, hi } = patch else {
        return Ok(None);
    };
    let source_span = &source.spans[*span];
    let start = span_point(source_span, lo)?;
    let end = span_point(source_span, hi)?;
    let start_site = target.index.nearest(target, &start, None)?.primitive;
    let end_site = target.index.nearest(target, &end, None)?.primitive;
    if start_site == end_site {
        return Ok(None);
    }
    let Some(vertex) = shared_primitive_vertex(target, start_site, end_site)? else {
        return Ok(None);
    };
    let parameter = span_projection_parameter(&vertex, source_span)?;
    if parameter.try_cmp(lo)? == Ordering::Greater && parameter.try_cmp(hi)? == Ordering::Less {
        Ok(Some(parameter))
    } else {
        Ok(None)
    }
}

fn split_hausdorff_patch(
    patch: SourcePatch,
    source: &PreparedSupport,
    target: &PreparedSupport,
    budget: &mut ExpansionBudget,
) -> Result<Option<Vec<SourcePatch>>> {
    if let Some(parameter) = endpoint_site_split_parameter(&patch, source, target)?
        && let SourcePatch::Span { span, lo, hi } = &patch
    {
        budget.add(2)?;
        return Ok(Some(vec![
            SourcePatch::Span {
                span: *span,
                lo: lo.clone(),
                hi: parameter.clone(),
            },
            SourcePatch::Span {
                span: *span,
                lo: parameter,
                hi: hi.clone(),
            },
        ]));
    }
    patch.split(budget)
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
        let Some(children) =
            split_hausdorff_patch(record.patch, source, target, &mut context.budget)?
        else {
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
    let dx = left.x().try_subtract(right.x())?;
    let dy = left.y().try_subtract(right.y())?;
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
    let witness_interval = SqrtInterval::from_squared_with_error(witness_squared, None)?;
    if let Some(witness_upper) = witness_interval.upper_rational() {
        let gap = upper
            .try_subtract(&witness_upper)?
            .try_product(&context.output_scale)?;
        if gap.try_cmp(&context.output_tolerance)? == Ordering::Greater {
            return Ok(None);
        }
    }

    let center_exact = if witness_squared.is_zero() {
        source
            .first_boundary_point()
            .unwrap_or_else(|| witness.clone())
    } else {
        ExactPoint::from_xy(witness.rounded_xy()?)
    };
    let xy = center_exact.rounded_xy()?;
    let center = Point::new_raw_xy(xy.x, xy.y);
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

#[expect(
    clippy::arithmetic_side_effects,
    reason = "each seed is inset from a finite box edge by at most its short-axis half-extent"
)]
fn directed_seed_candidate(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<Option<DirectedCandidate>> {
    // Every clearance disk is inside the source's enclosing rectangle.
    // Its short-axis half-extent bounds the inradius independently of the
    // long-axis Lipschitz radius used by individual search patches.
    let Some(bounds) = source.area_bounds.filter(|bounds| {
        // A long-axis bisection still leaves an elongated cell. Ordinary
        // near-square cells already localize both axes together.
        let width = bounds.max_x / 2.0 - bounds.min_x / 2.0;
        let height = bounds.max_y / 2.0 - bounds.min_y / 2.0;
        width > 2.0 * height || height > 2.0 * width
    }) else {
        return Ok(None);
    };
    let width = Rational::from_f64(bounds.max_x).try_subtract(&Rational::from_f64(bounds.min_x))?;
    let height =
        Rational::from_f64(bounds.max_y).try_subtract(&Rational::from_f64(bounds.min_y))?;
    let shortest = if width.try_cmp(&height)? == Ordering::Less {
        width
    } else {
        height
    };
    let upper = shortest.try_product(&Rational::from_f64(0.5))?;
    {
        let radius = nonnegative_bounds(&upper)?.0;
        let x = f64::midpoint(bounds.min_x, bounds.max_x);
        let y = f64::midpoint(bounds.min_y, bounds.max_y);
        let ends = if compare_box_extents(bounds) == Some(Ordering::Less) {
            [
                XY::new(x, bounds.min_y + radius),
                XY::new(x, bounds.max_y - radius),
            ]
        } else {
            [
                XY::new(bounds.min_x + radius, y),
                XY::new(bounds.max_x - radius, y),
            ]
        };
        let mut previous = None;
        for xy in [XY::new(x, y), ends[0], ends[1]] {
            if previous == Some(xy) {
                continue;
            }
            previous = Some(xy);
            let witness = ExactPoint::from_xy(xy);
            if source.contains_area(&witness)? {
                let squared = target.distance_squared(&witness)?;
                if !squared.is_zero() {
                    context.update_lower_with_witness(squared, &witness)?;
                    if let Some(result) = directed_candidate(source, target, &upper, context)? {
                        return Ok(Some(result));
                    }
                }
            }
        }
    }
    if let Some((enclosure, witness)) = inradius_carrier_enclosure(source, target, context, &upper)?
    {
        if let Some(witness) = witness
            && source.contains_area(&witness)?
        {
            let squared = target.distance_squared(&witness)?;
            context.update_lower_with_witness(squared, &witness)?;
        }
        if let Some(result) = directed_candidate(source, target, &enclosure, context)? {
            return Ok(Some(result));
        }
    }
    Ok(None)
}

fn run_directed(
    source: &PreparedSupport,
    target: &PreparedSupport,
    context: &mut SearchContext<'_>,
) -> Result<DirectedCandidate> {
    context.require_positive_witness = source.has_positive_area();
    if let Some(candidate) = directed_seed_candidate(source, target, context)? {
        return Ok(candidate);
    }
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
) -> Result<Option<DirectedOptimizationResult>> {
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
    let Some(mut source) = PreparedSupport::from_shape_with_boundary(shape, &mut context.budget)?
    else {
        return Ok(None);
    };
    if !source.has_positive_area() {
        return Ok(None);
    }
    let target = source.boundary_clone()?;
    // Boundary incidence is only needed while constructing the independent
    // boundary view. The directed result keeps the source support for final
    // containment, but that path reads its spans/faces/index, not these flags.
    source.boundary_spans = None;
    source.boundary_points = None;
    let candidate = run_directed(&source, &target, &mut context)?;
    Ok(Some(DirectedOptimizationResult {
        center: candidate.center,
        center_squared: candidate.center_squared,
        source,
        boundary: target,
    }))
}

pub(crate) fn distance<'a>(
    left: impl Into<storage::ShapeRow<'a>>,
    right: impl Into<storage::ShapeRow<'a>>,
    max_error: Option<f64>,
    scale: f64,
) -> Result<f64> {
    let left = left.into();
    let right = right.into();
    let accuracy = Accuracy::new(max_error, scale)?;
    let mut context = SearchContext::new(&accuracy);
    let identical = left.same_stored_geometry(right);
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
        || crate::geometry::predicate_rows(left, right, crate::predicates::Predicate::Equals),
        |forward| {
            if forward {
                crate::geometry::predicate_rows(right, left, crate::predicates::Predicate::Covers)
            } else {
                crate::geometry::predicate_rows(left, right, crate::predicates::Predicate::Covers)
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
        for span in source.spans.iter() {
            for parameter in [span.lo(), span.hi()] {
                let point = span_point(span, parameter)?;
                if let Some(squared) = raising_clearance(target, &point, context)? {
                    context.update_lower(squared)?;
                }
            }
        }
        for point in source.points.iter() {
            if let Some(squared) = raising_clearance(target, point, context)? {
                context.update_lower(squared)?;
            }
        }
    }
    check_hausdorff_output_range(context)?;
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
pub(in crate::geometry) enum ExactPoint {
    Stored {
        xy: XY,
        exact: std::sync::OnceLock<Box<[Rational; 2]>>,
    },
    Constructed(Box<[Rational; 2]>),
}

impl ExactPoint {
    const fn from_xy(point: XY) -> Self {
        Self::Stored {
            xy: point,
            exact: std::sync::OnceLock::new(),
        }
    }

    fn new(x: Rational, y: Rational) -> Self {
        Self::Constructed(Box::new([x, y]))
    }

    const fn stored_xy(&self) -> Option<XY> {
        match self {
            Self::Stored { xy, .. } => Some(*xy),
            Self::Constructed(_) => None,
        }
    }

    fn rounded_xy(&self) -> Result<XY> {
        if let Some(xy) = self.stored_xy() {
            return Ok(xy);
        }
        Ok(XY::new(self.x().round()?, self.y().round()?))
    }

    fn components(&self) -> &[Rational; 2] {
        match self {
            Self::Stored { xy, exact } => {
                exact.get_or_init(|| Box::new([Rational::from_f64(xy.x), Rational::from_f64(xy.y)]))
            },
            Self::Constructed(exact) => exact,
        }
    }

    fn x(&self) -> &Rational {
        let [x, _] = self.components();
        x
    }

    fn y(&self) -> &Rational {
        let [_, y] = self.components();
        y
    }

    fn from_homogeneous(value: [ExactDyadic; 3]) -> crate::error::Result<Self> {
        let [x, y, w] = value;
        Ok(Self::new(
            Rational::new(x, w.clone())?,
            Rational::new(y, w)?,
        ))
    }

    fn same_position(&self, other: &Self) -> crate::error::Result<bool> {
        Ok(self.x().try_cmp(other.x())?.is_eq() && self.y().try_cmp(other.y())?.is_eq())
    }
}

#[cfg(test)]
mod tests;
