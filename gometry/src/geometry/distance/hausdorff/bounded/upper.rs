//! Certified upper bounds and clearance filters.

use super::search::{DirectedBoxBound, directed_box_record};
use super::sqrt::{max_rational, min_rational};
use super::support::span_projection_parameter;
use super::{
    ErrorBound, ExactPoint, ExactSign, FloatBox, Ordering, PatchRecord, PreparedSupport,
    PrimitiveId, Rational, Result, SearchContext, SourcePatch, SqrtInterval, SupportSpan,
    Tightness, exact,
};

pub(super) fn sqrt_upper(squared: &Rational, context: &SearchContext<'_>) -> Result<SqrtInterval> {
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

pub(super) fn sum_upper(left: &SqrtInterval, right: &SqrtInterval) -> Result<Option<Rational>> {
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
pub(super) fn fixed_site_squared(
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

pub(super) fn carrier_line_value(span: &SupportSpan, point: &ExactPoint) -> Result<Rational> {
    let carrier = span.carrier()?;
    let px = point.x().try_subtract(&carrier.ax)?;
    let py = point.y().try_subtract(&carrier.ay)?;
    carrier
        .dx
        .try_product(&py)?
        .try_subtract(&carrier.dy.try_product(&px)?)
}

pub(super) fn exact_parallel(left: &SupportSpan, right: &SupportSpan) -> Result<bool> {
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

pub(super) fn projection_is_in_span(point: &ExactPoint, span: &SupportSpan) -> Result<bool> {
    let parameter = span_projection_parameter(point, span)?;
    Ok(parameter.try_cmp(span.lo())? != Ordering::Less
        && parameter.try_cmp(span.hi())? != Ordering::Greater)
}

pub(super) fn signs_between_parallel_lines(first: &[Rational], second: &[Rational]) -> bool {
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

pub(super) fn add_parallel_probe(
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
pub(super) fn parallel_strip_upper(
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

pub(super) fn patch_upper(
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

pub(super) fn upper_is_below_lower(upper: &Rational, context: &SearchContext<'_>) -> Result<bool> {
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
pub(super) fn raising_clearance(
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

pub(super) fn build_patch(
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
