//! Directed maximum-inscribed-radius search and result.

use super::cycle::{BoxSplitStrategy, compare_box_extents};
use super::queue::PatchQueue;
use super::search::queue_upper;
use super::span_bounds::nonnegative_bounds;
use super::upper::{build_patch, carrier_line_value, upper_is_below_lower};
use super::{
    Accuracy, DirectedCandidate, DirectedOptimizationResult, ExactPoint, ExactSign, Ordering,
    Point, PreparedSupport, PrimitiveId, Rational, Result, SearchContext, Shape, SqrtInterval,
    SupportSpan, XY,
};
#[cfg(test)]
use super::{DirectedCandidateProof, count_refinement_work};

pub(super) struct InradiusPlane<'a> {
    span: &'a SupportSpan,
    dx: Rational,
    dy: Rational,
    clearance: Rational,
    length_lower: Rational,
    side: crate::geometry::Orientation,
}

impl InradiusPlane<'_> {
    pub(super) fn encloses_source(&self, source: &PreparedSupport) -> Result<bool> {
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

    fn normalized_equation(&self, radius: f64) -> Result<Option<(f64, f64, f64)>> {
        let length = self.length_lower.round()?;
        let nx = -self.dy.round()? / length;
        let ny = self.dx.round()? / length;
        let rhs = radius - self.clearance.round()? / length;
        Ok((nx.is_finite() && ny.is_finite() && rhs.is_finite()).then_some((nx, ny, rhs)))
    }
}

pub(super) fn inradius_plane_witness(
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
pub(super) fn inradius_carrier_enclosure(
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

#[cfg(test)]
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

pub(super) fn downward_sqrt(squared: &Rational) -> Result<f64> {
    let interval = SqrtInterval::from_squared_with_error(squared, None)?;
    Rational::from_dyadic(interval.lower).round()
}

pub(super) fn movement_lower_squared(
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

pub(super) fn exact_point_distance_squared(
    left: &ExactPoint,
    right: &ExactPoint,
) -> Result<Rational> {
    let dx = left.x().try_subtract(right.x())?;
    let dy = left.y().try_subtract(right.y())?;
    dx.try_square()?.try_add(&dy.try_square()?)
}

pub(super) fn directed_candidate(
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

pub(super) fn directed_seed_candidate(
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

pub(super) fn run_directed(
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
    // boundary view, not during the directed search.
    source.boundary_spans = None;
    source.boundary_points = None;
    let candidate = run_directed(&source, &target, &mut context)?;
    Ok(Some(DirectedOptimizationResult {
        center: candidate.center,
        center_squared: candidate.center_squared,
        #[cfg(test)]
        source,
        #[cfg(test)]
        boundary: target,
    }))
}
