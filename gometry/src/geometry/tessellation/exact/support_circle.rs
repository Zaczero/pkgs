//! Exact minimum-circle support and certification.

#[cfg(test)]
use super::round_sqrt_ratio;
use super::{ExactDyadic, ExactPoint, circumcenter, orient2d, same_xy, try_circle_contains};
use crate::error::Result;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::{GeometryErrorKind, XY};

struct ExactSupportCircle {
    center: ExactPoint,
    radius_numerator: ExactDyadic,
    radius_denominator: ExactDyadic,
}

fn try_radius_squared(center: &ExactPoint, point: XY) -> Result<(ExactDyadic, ExactDyadic)> {
    let px = ExactDyadic::from_f64(point.x);
    let py = ExactDyadic::from_f64(point.y);
    let center = center.coords()?;
    let dx = ExactDyadic::try_subtract_refs(&center.x, &px.try_product(&center.w)?)?;
    let dy = ExactDyadic::try_subtract_refs(&center.y, &py.try_product(&center.w)?)?;
    Ok((
        dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?,
        center.w.try_product(&center.w)?,
    ))
}

fn try_support_circle(support: &[XY]) -> Result<ExactSupportCircle> {
    match support {
        [point] => {
            let center = ExactPoint::homogeneous(
                ExactDyadic::from_f64(point.x),
                ExactDyadic::from_f64(point.y),
                ExactDyadic::one(),
            );
            Ok(ExactSupportCircle {
                center,
                radius_numerator: ExactDyadic::zero(),
                radius_denominator: ExactDyadic::one(),
            })
        },
        [a, b] => {
            let ax = ExactDyadic::from_f64(a.x);
            let ay = ExactDyadic::from_f64(a.y);
            let bx = ExactDyadic::from_f64(b.x);
            let by = ExactDyadic::from_f64(b.y);
            let center = ExactPoint::homogeneous(
                ax.try_add(bx)?,
                ay.try_add(by)?,
                ExactDyadic::from_f64(2.0),
            );
            let (radius_numerator, radius_denominator) = try_radius_squared(&center, *a)?;
            Ok(ExactSupportCircle {
                center,
                radius_numerator,
                radius_denominator,
            })
        },
        [a, b, c] => {
            let center = circumcenter(*a, *b, *c)?;
            let (radius_numerator, radius_denominator) = try_radius_squared(&center, *a)?;
            Ok(ExactSupportCircle {
                center,
                radius_numerator,
                radius_denominator,
            })
        },
        _ => Err(GeometryErrorKind::Invalid(
            "minimum circle support must contain one, two, or three points".into(),
        )
        .into()),
    }
}

/// Return the exact homogeneous center and squared-radius ratio of a support.
///
/// The tuple is intentionally made of the arithmetic owner types rather than
/// rounded coordinates. Callers that publish a realization can therefore
/// carry the construction through a unit scale and round only at that
/// publication boundary.
pub(in crate::geometry) fn try_support_circle_components(
    support: &[XY],
) -> Result<(
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
    ExactDyadic,
)> {
    let circle = try_support_circle(support)?;
    let center = circle.center.coords()?;
    Ok((
        center.x.clone(),
        center.y.clone(),
        center.w.clone(),
        circle.radius_numerator,
        circle.radius_denominator,
    ))
}

fn try_orient_source_center(a: XY, b: XY, center: &ExactPoint) -> Result<ExactSign> {
    let ax = ExactDyadic::from_f64(a.x);
    let ay = ExactDyadic::from_f64(a.y);
    let bx = ExactDyadic::from_f64(b.x);
    let by = ExactDyadic::from_f64(b.y);
    let edge_x = bx.try_subtract(ax.clone())?;
    let edge_y = by.try_subtract(ay.clone())?;
    let center = center.coords()?;
    let center_x = ExactDyadic::try_subtract_refs(&center.x, &ax.try_product(&center.w)?)?;
    let center_y = ExactDyadic::try_subtract_refs(&center.y, &ay.try_product(&center.w)?)?;
    let determinant = edge_x
        .try_product(&center_y)?
        .try_subtract(edge_y.try_product(&center_x)?)?;
    Ok(determinant.sign().quotient(center.w.sign()))
}

/// Independently certify a support candidate against all original points.
///
/// The final pass deliberately does not rely on the loop's current candidate
/// state.  It checks source containment again and then verifies the closed
/// KKT condition: a one-point support is coincident, a two-point support is a
/// diameter, and a three-point support is non-collinear with its exact
/// circumcenter in the closed support triangle.
pub(in crate::geometry) fn try_certify_support(
    support: &[XY],
    points: impl IntoIterator<Item = XY>,
) -> Result<bool> {
    if support.is_empty() || support.len() > 3 {
        return Err(GeometryErrorKind::Invalid(
            "minimum circle support must contain one, two, or three points".into(),
        )
        .into());
    }
    match support {
        [only] => Ok(points.into_iter().all(|point| same_xy(*only, point))),
        [a, b] => {
            if same_xy(*a, *b) {
                return Ok(false);
            }
            points.into_iter().try_fold(true, |valid, point| {
                if !valid {
                    return Ok(false);
                }
                try_circle_contains(support, point)
            })
        },
        [a, b, c] => {
            let orientation = orient2d(*a, *b, *c);
            if orientation == ExactSign::Zero {
                return Ok(false);
            }
            let contained = points.into_iter().try_fold(true, |valid, point| {
                if !valid {
                    return Ok(false);
                }
                try_circle_contains(support, point)
            })?;
            if !contained {
                return Ok(false);
            }
            let center = circumcenter(*a, *b, *c)?;
            for (left, right) in [(*a, *b), (*b, *c), (*c, *a)] {
                let edge_orientation = try_orient_source_center(left, right, &center)?;
                let inside = match orientation {
                    ExactSign::Positive => edge_orientation != ExactSign::Negative,
                    ExactSign::Negative => edge_orientation != ExactSign::Positive,
                    ExactSign::Zero => unreachable!(),
                };
                if !inside {
                    return Ok(false);
                }
            }
            Ok(true)
        },
        _ => unreachable!(),
    }
}

/// Round the exact center and squared radius of a source support.
///
/// The center and squared radius are each rounded once from their exact
/// constructions.  In particular, the radius is never recovered from the
/// rounded center.
#[cfg(test)]
pub(in crate::geometry) fn round_support_circle(support: &[XY]) -> Result<(XY, f64, bool)> {
    let circle = try_support_circle(support)?;
    let center = circle.center.round_nearest_even()?;
    let radius = round_sqrt_ratio(&circle.radius_numerator, &circle.radius_denominator)?;
    Ok((center, radius, circle.radius_numerator.is_zero()))
}
