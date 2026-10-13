//! Prepared exact support, witnesses, and support-index admission.

#[cfg(test)]
use super::count_exact_span_evaluation;
use super::cycle::{
    CycleLocation, edge_location, exact_segment_enters_candidate, segment_box_intersection,
};
use super::inradius::exact_point_distance_squared;
use super::span_bounds::{
    box_distance_lower, nonnegative_upper_bound, point_span_squared_bounds,
    point_span_squared_bounds_with,
};
use super::{
    Arc, AreaLocationStrategy, BoundaryEdge, ExactDyadic, ExactMetricSpan, ExactMetricSupport,
    ExactPoint, ExactSourceInventory, ExpansionBudget, FloatBox, Ordering, Point, PreparedSupport,
    PrimitiveId, Rational, Result, Segment, SelectedSpan, Shape, SourcePatch, SquaredBounds,
    SupportIndex, SupportSpan, Tightness, UnreducedRatio, XY, exact, point_span_distance_squared,
    storage,
};

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
        let area_location = AreaLocationStrategy::for_span_count(partition.spans.len());
        let support = inventory.metric_support(&partition, area_location)?;
        Self::from_metric_support(support)
    }

    pub(in crate::geometry) fn from_shape_with_boundary(
        shape: &Shape,
        budget: &mut ExpansionBudget,
    ) -> Result<Option<Self>> {
        if !shape.has_area_parts() {
            return Self::from_shape(shape, budget).map(Some);
        }
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let partition = inventory.partition(budget)?;
        let area_location = AreaLocationStrategy::for_span_count(partition.spans.len());
        let support = inventory.metric_support_with_boundary(&partition, area_location)?;
        if support.faces.is_empty() {
            return Ok(None);
        }
        Self::from_metric_support(support).map(Some)
    }

    pub(super) fn from_line_columns(xs: &[f64], ys: &[f64]) -> Result<Self> {
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

    pub(super) fn from_metric_support(support: ExactMetricSupport) -> Result<Self> {
        let ExactMetricSupport {
            exact_points,
            spans: metric_spans,
            points: metric_points,
            faces: metric_faces,
            area_location_cells,
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
            spans.push(support_span(&span)?);
        }

        let mut points = Vec::with_capacity(metric_points.len());
        for point in metric_points {
            points.push(ExactPoint::from_homogeneous(point.homogeneous().clone())?);
        }

        let mut faces = Vec::with_capacity(metric_faces.len());
        for face in metric_faces {
            let mut cycles = Vec::with_capacity(face.len());
            for cycle in face {
                let mut points = Vec::with_capacity(cycle.len());
                for point in cycle {
                    points.push(ExactPoint::from_homogeneous(point.homogeneous().clone())?);
                }
                cycles.push(points);
            }
            faces.push(cycles);
        }

        let mut prepared = Self::from_parts(spans, points, faces)?;
        prepared.area_locator = area_location_cells.map(|cells| {
            debug_assert!(Arc::ptr_eq(&exact_points, &cells.geometry.points));
            Arc::new(crate::geometry::ExactLocationIndex::from_cells(cells))
        });
        if !prepared.boundary_edges.is_empty() && prepared.area_locator.is_none() {
            debug_assert!(prepared.boundary_edges.len() <= 8);
        }
        prepared.boundary_spans = metric_boundary_spans;
        prepared.boundary_points = metric_boundary_points;
        Ok(prepared)
    }

    pub(super) fn from_parts(
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
        Ok(Self {
            spans: spans.into(),
            points: points.into(),
            faces,
            boundary_spans: None,
            boundary_points: None,
            boundary_edges,
            area_bounds,
            index,
            area_locator: None,
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
                area_locator: None,
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

    pub(super) fn initial_patches(&self) -> Vec<SourcePatch> {
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

    pub(super) fn inradius_initial_patches(&self) -> Result<Vec<SourcePatch>> {
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

    pub(super) fn contains_area(&self, point: &ExactPoint) -> Result<bool> {
        if self.boundary_edges.is_empty() {
            return Ok(false);
        }
        let Some(locator) = &self.area_locator else {
            return self.contains_area_bounded(point);
        };
        let exact = if let Some(xy) = point.stored_xy() {
            [
                ExactDyadic::from_f64(xy.x),
                ExactDyadic::from_f64(xy.y),
                ExactDyadic::one(),
            ]
        } else {
            let (x_numerator, x_denominator) = point.x().parts();
            let (y_numerator, y_denominator) = point.y().parts();
            [
                x_numerator.try_product(y_denominator)?,
                y_numerator.try_product(x_denominator)?,
                x_denominator.try_product(y_denominator)?,
            ]
        };
        locator.locate_exact(&exact)
    }

    pub(super) fn contains_area_bounded(&self, point: &ExactPoint) -> Result<bool> {
        // This path is selected only for tiny exact arrangements. Its walks
        // label the already-selected union faces, so shared internal edges
        // cancel in the ray fold while exact contacts remain closed support.
        let mut inside = false;
        for edge in &self.boundary_edges {
            match edge_location(point, edge)? {
                CycleLocation::Boundary => return Ok(true),
                CycleLocation::Crosses => inside = !inside,
                CycleLocation::Clear => {},
            }
        }
        Ok(inside)
    }

    pub(super) fn covers_support(&self, point: &ExactPoint) -> Result<bool> {
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

    pub(super) fn first_boundary_point(&self) -> Option<ExactPoint> {
        if let Some(span) = self.spans.first() {
            return Some(span.start_point());
        }
        if let Some(point) = self.points.first() {
            return Some(point.clone());
        }
        self.boundary_edges.first().map(|edge| edge.start.clone())
    }

    pub(super) fn source_box_witness(
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
    pub(super) fn primitive_distance_squared(
        &self,
        primitive: PrimitiveId,
        point: &ExactPoint,
    ) -> Result<UnreducedRatio> {
        match primitive {
            PrimitiveId::Span(index) => {
                #[cfg(test)]
                count_exact_span_evaluation();
                let span = &self.spans[index];
                point_span_distance_squared(point, span.carrier()?, span.lo(), span.hi())
            },
            PrimitiveId::Point(index) => {
                let dx = UnreducedRatio::try_difference(point.x(), self.points[index].x())?;
                let dy = UnreducedRatio::try_difference(point.y(), self.points[index].y())?;
                dx.try_square()?.try_add(&dy.try_square()?)
            },
        }
    }

    pub(super) fn distance_squared(&self, point: &ExactPoint) -> Result<Rational> {
        if self.contains_area(point)? {
            return Ok(Rational::zero());
        }
        Ok(self.index.nearest(self, point, None)?.bounds.lower)
    }

    pub(super) fn primitive_squared_bounds(
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
    pub(super) fn primitive_interval_bounds(
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
pub(super) fn may_have_parallel_carriers(spans: &[SupportSpan]) -> bool {
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
pub(super) fn point_span_binary64_bounds(
    point: FloatBox,
    span: &SupportSpan,
) -> Option<(f64, f64)> {
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

pub(super) fn push_raw_segment(
    source: Segment,
    spans: &mut Vec<SupportSpan>,
    points: &mut Vec<ExactPoint>,
) {
    if source.start == source.end {
        points.push(ExactPoint::from_xy(source.start));
        return;
    }
    spans.push(raw_support_span(source));
}

pub(super) const fn raw_support_span(source: Segment) -> SupportSpan {
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

pub(super) fn support_span(span: &ExactMetricSpan) -> Result<SupportSpan> {
    if span.start_parameter[0].is_zero()
        && span.end_parameter[0].cmp(&span.end_parameter[1]) == Ordering::Equal
    {
        return Ok(raw_support_span(span.source));
    }
    let start = ExactPoint::from_homogeneous(span.start.homogeneous().clone())?;
    let end = ExactPoint::from_homogeneous(span.end.homogeneous().clone())?;
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

pub(super) fn span_point(span: &SupportSpan, parameter: &Rational) -> Result<ExactPoint> {
    let carrier = span.carrier()?;
    let x = carrier.ax.try_add(&carrier.dx.try_product(parameter)?)?;
    let y = carrier.ay.try_add(&carrier.dy.try_product(parameter)?)?;
    Ok(ExactPoint::new(x, y))
}

pub(super) fn span_projection_parameter(
    point: &ExactPoint,
    span: &SupportSpan,
) -> Result<Rational> {
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
