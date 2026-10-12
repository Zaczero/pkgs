//! Conservative point, span, and box distance enclosures.

use super::{
    DdInterval, DistanceInterval, ExactDyadic, FloatBox, Ordering, Rational, Result, SupportSpan,
    XY, exact,
};

/// Double-double bounds on the squared distance from any point of
/// `point_bounds` to a span covering its whole source segment. The dot and
/// cross products with the segment direction are affine in the point, so the
/// box corners bound them; a box straddling the segment's end normals, a
/// partial span, or a non-finite stage declines to the exact evaluation.
pub(super) fn point_span_squared_bounds(
    point_bounds: FloatBox,
    span: &SupportSpan,
) -> Option<(f64, f64)> {
    point_span_squared_bounds_with::<DdInterval>(point_bounds, span)
}

pub(super) fn point_span_squared_bounds_with<I: DistanceInterval>(
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
pub(super) fn supporting_line_lower_squared(point_bounds: FloatBox, span: &SupportSpan) -> f64 {
    supporting_line_lower_squared_with::<exact::Interval>(point_bounds, span)
}

pub(super) fn supporting_line_lower_squared_with<I: DistanceInterval>(
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

pub(super) fn nonnegative_upper_bound(value: &Rational) -> Result<f64> {
    Ok(nonnegative_bounds(value)?.1)
}

pub(super) fn nonnegative_bounds(value: &Rational) -> Result<(f64, f64)> {
    if value.try_cmp(&Rational::from_f64(f64::MAX))? == Ordering::Greater {
        Ok((f64::MAX, f64::INFINITY))
    } else {
        value.outward_bounds()
    }
}

pub(super) fn square_f64(value: f64) -> Result<Rational> {
    let value = ExactDyadic::from_f64(value);
    Ok(Rational::from_dyadic(value.try_product(&value)?))
}

/// Squared distance from `point` to the segment's sub-span whose projection
/// parameters lie in `[low, high]` (`(0.0, 1.0)` is the whole segment).
pub(super) fn point_segment_squared_f64(
    point: XY,
    start: XY,
    end: XY,
    (low, high): (f64, f64),
) -> f64 {
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

pub(super) fn box_distance_lower(left: FloatBox, right: FloatBox) -> f64 {
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
