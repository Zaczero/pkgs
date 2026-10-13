//! Exact retained-chart corner construction and repair.

use super::super::corners::minimum_rectangle_not_representable;
use super::super::{
    Candidate, ChartRectangleCorner, ChartSupportHalfplane, ExactChartPoint, ExactRational,
    ExactSign, GeographicFrame, GeometryErrorKind, Point, RenderedChartCorner, Result,
    retained_exact_site_sample, retained_site_longitude,
};

pub(in crate::geometry::derived::minimum_rectangle) fn chart_support_margin(
    halfplane: ChartSupportHalfplane,
    candidate: &ExactChartPoint,
    frame: GeographicFrame,
) -> Result<ExactRational> {
    let support_x = retained_site_longitude(frame.longitude(), halfplane.support.longitude())?;
    let dx = candidate
        .x
        .try_subtract(&ExactRational::from_dyadic(support_x))?;
    let dy = candidate
        .y
        .try_subtract(&ExactRational::from_f64(halfplane.support.source_y()))?;
    ExactRational::from_f64(halfplane.gradient.0)
        .try_product(&dx)?
        .try_add(&ExactRational::from_f64(halfplane.gradient.1).try_product(&dy)?)
}

pub(in crate::geometry::derived::minimum_rectangle) fn retained_chart_corner(
    first: ChartSupportHalfplane,
    second: ChartSupportHalfplane,
    frame: GeographicFrame,
) -> Result<ExactChartPoint> {
    use crate::geometry::tessellation::exact::ExactDyadic;

    let axis = frame.longitude();
    let origin_x = retained_site_longitude(axis, first.support.longitude())?;
    let origin_y = ExactDyadic::from_f64(first.support.source_y());
    let second_x = retained_site_longitude(axis, second.support.longitude())?;
    let second_y = ExactDyadic::from_f64(second.support.source_y());
    let dx = second_x.try_subtract(origin_x.clone())?;
    let dy = second_y.try_subtract(origin_y.clone())?;
    let second_residual = ExactDyadic::from_f64(second.gradient.0)
        .try_product(&dx)?
        .try_add(ExactDyadic::from_f64(second.gradient.1).try_product(&dy)?)?;
    let determinant = ExactDyadic::from_f64(first.gradient.0)
        .try_product(&ExactDyadic::from_f64(second.gradient.1))?
        .try_subtract(
            ExactDyadic::from_f64(first.gradient.1)
                .try_product(&ExactDyadic::from_f64(second.gradient.0))?,
        )?;
    let x_numerator = origin_x
        .try_product(&determinant)?
        .try_subtract(ExactDyadic::from_f64(first.gradient.1).try_product(&second_residual)?)?;
    let y_numerator = origin_y
        .try_product(&determinant)?
        .try_add(ExactDyadic::from_f64(first.gradient.0).try_product(&second_residual)?)?;
    Ok(ExactChartPoint {
        x: ExactRational::new(x_numerator, determinant.clone())?,
        y: ExactRational::new(y_numerator, determinant)?,
    })
}

pub(in crate::geometry::derived::minimum_rectangle) fn render_chart_corner(
    corner: &ExactChartPoint,
    frame: GeographicFrame,
) -> Result<RenderedChartCorner> {
    let longitude = retained_exact_site_sample(frame.longitude(), &corner.x)?;
    let latitude = corner.y.round()?;
    if !latitude.is_finite() {
        return Err(minimum_rectangle_not_representable());
    }
    Ok(RenderedChartCorner {
        point: Point::new_raw_xy(longitude.longitude.get(), latitude),
        exact: ExactChartPoint {
            x: longitude.chart_longitude,
            y: ExactRational::from_f64(latitude),
        },
    })
}

pub(in crate::geometry::derived::minimum_rectangle) fn upward_nonnegative_ratio(
    numerator: &ExactRational,
    denominator: f64,
) -> Result<f64> {
    debug_assert!(denominator.is_finite() && denominator > 0.0);
    let ratio = numerator.try_quotient(&ExactRational::from_f64(denominator))?;
    let (_, upper) = ratio.outward_bounds()?;
    if !upper.is_finite() {
        return Err(minimum_rectangle_not_representable());
    }
    Ok(upper)
}

pub(in crate::geometry::derived::minimum_rectangle) fn chart_corner_repair_failure(
    first: ChartSupportHalfplane,
    second: ChartSupportHalfplane,
    point: Point,
) -> crate::error::Error {
    GeometryErrorKind::Invalid(
        format!(
            "minimum_rotated_rectangle could not publish an outward corner for support ordinals {} and {} at ({}, {})",
            first.support.source_ordinal(),
            second.support.source_ordinal(),
            point.x,
            point.y,
        )
        .into(),
    )
    .into()
}

pub(in crate::geometry::derived::minimum_rectangle) fn chart_corner_encloses_supports(
    first: ChartSupportHalfplane,
    second: ChartSupportHalfplane,
    point: &ExactChartPoint,
    frame: GeographicFrame,
) -> Result<bool> {
    Ok(
        chart_support_margin(first, point, frame)?.sign() != ExactSign::Negative
            && chart_support_margin(second, point, frame)?.sign() != ExactSign::Negative,
    )
}

pub(in crate::geometry::derived::minimum_rectangle) fn shifted_chart_corner(
    point: &ExactChartPoint,
    x_axis: bool,
    sign: f64,
    distance: f64,
) -> Result<ExactChartPoint> {
    let shift = ExactRational::from_f64(sign * distance);
    if x_axis {
        Ok(ExactChartPoint {
            x: point.x.try_add(&shift)?,
            y: point.y.clone(),
        })
    } else {
        Ok(ExactChartPoint {
            x: point.x.clone(),
            y: point.y.try_add(&shift)?,
        })
    }
}

pub(in crate::geometry::derived::minimum_rectangle) fn neighboring_chart_corner(
    rendered: &RenderedChartCorner,
    x_axis: bool,
    sign: f64,
) -> Result<ExactChartPoint> {
    // The exact displacement is already outward. If RN lands inside its
    // support, one adjacent native value clears the rounding cell containing
    // that exact boundary.
    if x_axis {
        let native = if sign.is_sign_positive() {
            rendered.point.x.next_up()
        } else {
            rendered.point.x.next_down()
        };
        if !native.is_finite() {
            return Err(minimum_rectangle_not_representable());
        }
        let native_delta = ExactRational::from_f64(native)
            .try_subtract(&ExactRational::from_f64(rendered.point.x))?;
        Ok(ExactChartPoint {
            x: rendered.exact.x.try_add(&native_delta)?,
            y: rendered.exact.y.clone(),
        })
    } else {
        let latitude = if sign.is_sign_positive() {
            rendered.point.y.next_up()
        } else {
            rendered.point.y.next_down()
        };
        if !latitude.is_finite() {
            return Err(minimum_rectangle_not_representable());
        }
        Ok(ExactChartPoint {
            x: rendered.exact.x.clone(),
            y: ExactRational::from_f64(latitude),
        })
    }
}

pub(in crate::geometry::derived::minimum_rectangle) fn repair_chart_corner(
    first: ChartSupportHalfplane,
    second: ChartSupportHalfplane,
    preferred: ExactChartPoint,
    frame: GeographicFrame,
) -> Result<ChartRectangleCorner> {
    let rendered = render_chart_corner(&preferred, frame)?;
    let first_margin = chart_support_margin(first, &rendered.exact, frame)?;
    let second_margin = chart_support_margin(second, &rendered.exact, frame)?;
    if first_margin.sign() != ExactSign::Negative && second_margin.sign() != ExactSign::Negative {
        return Ok(ChartRectangleCorner {
            exact: preferred,
            point: rendered.point,
        });
    }

    let mut best: Option<(f64, ChartRectangleCorner)> = None;
    for (x_axis, first_coefficient, second_coefficient) in [
        (true, first.gradient.0, second.gradient.0),
        (false, first.gradient.1, second.gradient.1),
    ] {
        if first_coefficient == 0.0
            || second_coefficient == 0.0
            || first_coefficient.is_sign_negative() != second_coefficient.is_sign_negative()
        {
            continue;
        }
        let sign = if first_coefficient.is_sign_negative() {
            -1.0
        } else {
            1.0
        };
        let first_distance = if first_margin.sign() == ExactSign::Negative {
            let deficit = ExactRational::from_f64(-1.0).try_product(&first_margin)?;
            upward_nonnegative_ratio(&deficit, first_coefficient * sign)?
        } else {
            0.0
        };
        let second_distance = if second_margin.sign() == ExactSign::Negative {
            let deficit = ExactRational::from_f64(-1.0).try_product(&second_margin)?;
            upward_nonnegative_ratio(&deficit, second_coefficient * sign)?
        } else {
            0.0
        };
        let distance = first_distance.max(second_distance);
        let candidate = shifted_chart_corner(&rendered.exact, x_axis, sign, distance)?;
        let candidate_rendered = render_chart_corner(&candidate, frame)?;
        let candidate_is_enclosing =
            chart_corner_encloses_supports(first, second, &candidate, frame)?;
        let candidate_publishes_enclosing =
            chart_corner_encloses_supports(first, second, &candidate_rendered.exact, frame)?;
        let accepted = if candidate_is_enclosing && candidate_publishes_enclosing {
            Some((candidate, candidate_rendered))
        } else {
            let neighbor = neighboring_chart_corner(&candidate_rendered, x_axis, sign)?;
            let neighbor_rendered = render_chart_corner(&neighbor, frame)?;
            let neighbor_is_enclosing =
                chart_corner_encloses_supports(first, second, &neighbor, frame)?;
            (neighbor_is_enclosing
                && chart_corner_encloses_supports(first, second, &neighbor_rendered.exact, frame)?)
            .then_some((neighbor, neighbor_rendered))
        };
        if let Some((exact, rendered)) = accepted
            && best
                .as_ref()
                .is_none_or(|(best_movement, _)| distance < *best_movement)
        {
            best = Some((distance, ChartRectangleCorner {
                exact,
                point: rendered.point,
            }));
        }
    }
    best.map(|(_, corner)| corner)
        .ok_or_else(|| chart_corner_repair_failure(first, second, rendered.point))
}

pub(in crate::geometry::derived::minimum_rectangle) fn exact_chart_orientation(
    first: &ExactChartPoint,
    second: &ExactChartPoint,
    third: &ExactChartPoint,
) -> Result<ExactSign> {
    let first_x = second.x.try_subtract(&first.x)?;
    let first_y = second.y.try_subtract(&first.y)?;
    let second_x = third.x.try_subtract(&first.x)?;
    let second_y = third.y.try_subtract(&first.y)?;
    Ok(first_x
        .try_product(&second_y)?
        .try_subtract(&first_y.try_product(&second_x)?)?
        .sign())
}

#[expect(
    clippy::indexing_slicing,
    reason = "caliper support indices are selected from this chart hull"
)]
pub(in crate::geometry::derived::minimum_rectangle) fn emit_chart_candidate(
    hull: &[crate::geographic::ChartPointSite],
    candidate: Candidate,
    frame: GeographicFrame,
) -> Result<[ChartRectangleCorner; 4]> {
    let supports = candidate.supports;
    let specs = [
        (
            ChartSupportHalfplane {
                gradient: (-candidate.basis.along.0, -candidate.basis.along.1),
                support: hull[supports.along_min],
            },
            ChartSupportHalfplane {
                gradient: (-candidate.basis.outward.0, -candidate.basis.outward.1),
                support: hull[supports.outward_min],
            },
        ),
        (
            ChartSupportHalfplane {
                gradient: candidate.basis.along,
                support: hull[supports.along_max],
            },
            ChartSupportHalfplane {
                gradient: (-candidate.basis.outward.0, -candidate.basis.outward.1),
                support: hull[supports.outward_min],
            },
        ),
        (
            ChartSupportHalfplane {
                gradient: candidate.basis.along,
                support: hull[supports.along_max],
            },
            ChartSupportHalfplane {
                gradient: candidate.basis.outward,
                support: hull[supports.outward_max],
            },
        ),
        (
            ChartSupportHalfplane {
                gradient: (-candidate.basis.along.0, -candidate.basis.along.1),
                support: hull[supports.along_min],
            },
            ChartSupportHalfplane {
                gradient: candidate.basis.outward,
                support: hull[supports.outward_max],
            },
        ),
    ];
    let construct_corner =
        |(first, second): (ChartSupportHalfplane, ChartSupportHalfplane)| -> Result<_> {
            let preferred = retained_chart_corner(first, second, frame)?;
            if !chart_support_margin(first, &preferred, frame)?.is_zero()
                || !chart_support_margin(second, &preferred, frame)?.is_zero()
            {
                return Err(GeometryErrorKind::Invalid(
                    "exact chart corner does not lie on both selected support lines".into(),
                )
                .into());
            }
            repair_chart_corner(first, second, preferred, frame)
        };
    let [first_spec, second_spec, third_spec, fourth_spec] = specs;
    let first = construct_corner(first_spec)?;
    let second = construct_corner(second_spec)?;
    let third = construct_corner(third_spec)?;
    let fourth = construct_corner(fourth_spec)?;
    Ok([first, second, third, fourth])
}
