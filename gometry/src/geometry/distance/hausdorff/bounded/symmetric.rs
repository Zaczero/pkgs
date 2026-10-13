//! Symmetric Hausdorff and line-column entry points.

use std::cmp::Ordering;

use super::search::{
    candidate_certified, check_hausdorff_output_range, lower_candidate, run_direction,
};
use super::support::span_point;
use super::upper::raising_clearance;
use super::{
    Accuracy, CoordSeq, LineSeq, PackedLineColumnView, PreparedSupport, Rational, Result,
    SearchContext, Shape, XY, storage,
};
use crate::geometry::predicate_spec::Predicate;

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
        || crate::geometry::predicate_rows(left, right, Predicate::Equals),
        |forward| {
            if forward {
                crate::geometry::predicate_rows(right, left, Predicate::Covers)
            } else {
                crate::geometry::predicate_rows(left, right, Predicate::Covers)
            }
        },
    )
}

/// `covered(forward)` reports whether the direction's target covers its
/// source (`forward` is left into right). It is asked only when every seeded
/// clearance is zero, where a covered direction would otherwise refine a
/// zero plateau along the shared boundary that no positive bound prunes.
pub(super) fn symmetric_distance(
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

pub(super) fn line_columns_with_accuracy(
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
