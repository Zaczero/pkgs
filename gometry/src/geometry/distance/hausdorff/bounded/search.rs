//! Directed Hausdorff refinement loop.

#[cfg(test)]
use super::count_refinement_work;
use super::queue::PatchQueue;
use super::span_bounds::nonnegative_bounds;
use super::support::{span_point, span_projection_parameter};
use super::upper::{build_patch, parallel_strip_upper, sqrt_upper, upper_is_below_lower};
use super::{
    ExactPoint, ExactSign, ExpansionBudget, FloatBox, Ordering, PatchRecord, PreparedSupport,
    PrimitiveId, Rational, Result, SearchContext, SourcePatch, SqrtInterval, XY,
};

#[expect(
    clippy::large_enum_variant,
    reason = "the enclosed cell is the hot result; boxing it allocates for every queued box"
)]
pub(super) enum DirectedBoxBound {
    ExactNeeded,
    Pruned,
    Enclosed(PatchRecord),
}

/// Signed boundary distance is one-Lipschitz. Its certified value at a
/// representable center plus the farthest box-corner distance encloses every
/// interior clearance; parallel-site bounds close flat maxima without
/// refining the length of a plateau down to the tolerance.
pub(super) fn directed_box_record(
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

pub(super) fn lower_candidate(context: &SearchContext<'_>) -> Result<Option<f64>> {
    let Some(lower) = &context.lower_squared else {
        return Ok(None);
    };
    SqrtInterval::candidate_from_squared(lower)
}

pub(super) fn candidate_certified(
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

pub(super) fn check_hausdorff_output_range(context: &SearchContext<'_>) -> Result<()> {
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

pub(super) fn queue_upper(
    queue: &PatchQueue,
    context: &SearchContext<'_>,
) -> Result<Option<Rational>> {
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
pub(super) fn primitive_endpoints(
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

pub(super) fn shared_primitive_vertex(
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
pub(super) fn endpoint_site_split_parameter(
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

pub(super) fn split_hausdorff_patch(
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

pub(super) fn run_direction(
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
