//! Exact and certified rectangular frame construction.

use super::{
    ExactDyadic, ExactPoint, ExactRational, approximate_ratio, compare_ratios, round_ratio,
};
use crate::error::Result;
use crate::geometry::{GeometryErrorKind, XY};

/// A private frame whose four corners are stored binary64 (`w == 1`).
///
/// The frame is private scaffolding. The only property the pipeline needs from
/// it is that it strictly encloses every input point. That property is
/// established here by the same exact comparator the rest of the module uses,
/// so the cheap construction is CERTIFIED, not assumed. `None` means no
/// admissible binary64 box exists and the caller must fall back to the exact
/// degree-8 construction.
#[expect(
    clippy::needless_pass_by_value,
    reason = "the iterator is re-walked once per widening round"
)]
#[expect(
    clippy::indexing_slicing,
    reason = "each axis loop ranges over 0..2, matching the lengths of the coordinate and bound arrays"
)]
pub(in crate::geometry::tessellation) fn enclosing_frame_binary64<'a>(
    points: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> Result<Option<[ExactPoint; 4]>> {
    // Seeding only. A wrong estimate costs extra certified widening rounds,
    // never a wrong frame.
    let approx = |numerator: &ExactDyadic, denominator: &ExactDyadic| -> Option<f64> {
        let magnitude = approximate_ratio(numerator, denominator)?;
        let negative = numerator.negative != denominator.negative;
        Some(if negative { -magnitude } else { magnitude })
    };
    let mut lo = [f64::INFINITY; 2];
    let mut hi = [f64::NEG_INFINITY; 2];
    for point in points.clone() {
        if !point.is_finite() {
            return Ok(None);
        }
        // The certified bounds are the estimate whenever they are finite; the
        // exact quotient is read only for a point whose bounds are not.
        let coordinates = if point.bounds[0].is_finite() && point.bounds[1].is_finite() {
            [point.bounds[0].hi, point.bounds[1].hi]
        } else {
            let Some(x) = approx(point.x()?, point.w()?) else {
                return Ok(None);
            };
            let Some(y) = approx(point.y()?, point.w()?) else {
                return Ok(None);
            };
            [x, y]
        };
        for axis in 0..2 {
            if !coordinates[axis].is_finite() {
                return Ok(None);
            }
            lo[axis] = lo[axis].min(coordinates[axis]);
            hi[axis] = hi[axis].max(coordinates[axis]);
        }
    }
    let span = (hi[0] - lo[0]).max(hi[1] - lo[1]).max(1.0);
    if !span.is_finite() {
        return Ok(None);
    }
    let mut box_lo = [lo[0] - span, lo[1] - span];
    let mut box_hi = [hi[0] + span, hi[1] + span];
    // Certify strict containment exactly; widen by one ulp whenever an input
    // lands on or outside a candidate bound. Bounded, so this stays a
    // certificate rather than an unbounded search.
    for _ in 0..64 {
        for axis in 0..2 {
            if !box_lo[axis].is_finite() || !box_hi[axis].is_finite() {
                return Ok(None);
            }
        }
        let low = ExactPoint::from_xy(XY::new(box_lo[0], box_lo[1]));
        let high = ExactPoint::from_xy(XY::new(box_hi[0], box_hi[1]));
        let mut widened = false;
        for point in points.clone() {
            for axis in 0..2 {
                let (below, above) = if axis == 0 {
                    (point.compare_x(&low)?, point.compare_x(&high)?)
                } else {
                    (point.compare_y(&low)?, point.compare_y(&high)?)
                };
                if !below.is_gt() {
                    box_lo[axis] = box_lo[axis].next_down();
                    widened = true;
                }
                if !above.is_lt() {
                    box_hi[axis] = box_hi[axis].next_up();
                    widened = true;
                }
            }
        }
        if !widened {
            return Ok(Some([
                ExactPoint::from_xy(XY::new(box_lo[0], box_lo[1])),
                ExactPoint::from_xy(XY::new(box_hi[0], box_lo[1])),
                ExactPoint::from_xy(XY::new(box_hi[0], box_hi[1])),
                ExactPoint::from_xy(XY::new(box_lo[0], box_hi[1])),
            ]));
        }
    }
    Ok(None)
}

pub(in crate::geometry::tessellation) fn enclosing_frame(
    points: &[ExactPoint],
) -> Result<[ExactPoint; 4]> {
    let [min_x, max_x, min_y, max_y] = extreme_points(points)?;
    let (min_x, max_x, min_y, max_y) = (
        min_x.coords()?,
        max_x.coords()?,
        min_y.coords()?,
        max_y.coords()?,
    );
    let span_x_n = max_x
        .x
        .try_product(&min_x.w)?
        .try_subtract(min_x.x.try_product(&max_x.w)?)?;
    let span_x_d = max_x.w.try_product(&min_x.w)?;
    let span_y_n = max_y
        .y
        .try_product(&min_y.w)?
        .try_subtract(min_y.y.try_product(&max_y.w)?)?;
    let span_y_d = max_y.w.try_product(&min_y.w)?;
    let (pad_n, pad_d) = if compare_ratios(&span_x_n, &span_x_d, &span_y_n, &span_y_d)?.is_gt() {
        (span_x_n, span_x_d)
    } else {
        (span_y_n, span_y_d)
    };
    if pad_n.is_zero() {
        return Err(GeometryErrorKind::voronoi(
            "exact frame input has zero span",
        ));
    }
    let coordinate = |base_n: &ExactDyadic, base_d: &ExactDyadic, add: bool| -> Result<_> {
        let base = base_n.try_product(&pad_d)?;
        let pad = pad_n.try_product(base_d)?;
        let numerator = if add {
            base.try_add(pad)?
        } else {
            base.try_subtract(pad)?
        };
        Ok((numerator, base_d.try_product(&pad_d)?))
    };
    let lx = coordinate(&min_x.x, &min_x.w, false)?;
    let ux = coordinate(&max_x.x, &max_x.w, true)?;
    let ly = coordinate(&min_y.y, &min_y.w, false)?;
    let uy = coordinate(&max_y.y, &max_y.w, true)?;
    Ok([
        corner((&lx.0, &lx.1), (&ly.0, &ly.1))?,
        corner((&ux.0, &ux.1), (&ly.0, &ly.1))?,
        corner((&ux.0, &ux.1), (&uy.0, &uy.1))?,
        corner((&lx.0, &lx.1), (&uy.0, &uy.1))?,
    ])
}

/// The exact points attaining the minimum and maximum x, then y.
fn extreme_points<'a>(
    points: impl IntoIterator<Item = &'a ExactPoint>,
) -> Result<[&'a ExactPoint; 4]> {
    let mut points = points.into_iter();
    let first = points
        .next()
        .ok_or_else(|| GeometryErrorKind::voronoi("empty exact frame input"))?;
    let mut extremes = [first; 4];
    for point in points {
        if point.compare_x(extremes[0])?.is_lt() {
            extremes[0] = point;
        }
        if point.compare_x(extremes[1])?.is_gt() {
            extremes[1] = point;
        }
        if point.compare_y(extremes[2])?.is_lt() {
            extremes[2] = point;
        }
        if point.compare_y(extremes[3])?.is_gt() {
            extremes[3] = point;
        }
    }
    Ok(extremes)
}

/// The point at x = `x.0 / x.1` and y = `y.0 / y.1`.
fn corner(x: (&ExactDyadic, &ExactDyadic), y: (&ExactDyadic, &ExactDyadic)) -> Result<ExactPoint> {
    Ok(ExactPoint::homogeneous(
        x.0.try_product(y.1)?,
        y.0.try_product(x.1)?,
        x.1.try_product(y.1)?,
    ))
}

/// Tight axis-aligned bounds of exact points: no pad, no enclosing super-frame.
pub(in crate::geometry::tessellation) fn axis_aligned_bounds<'a>(
    points: impl IntoIterator<Item = &'a ExactPoint>,
) -> Result<[ExactPoint; 4]> {
    let [min_x, max_x, min_y, max_y] = extreme_points(points)?;
    if min_x.compare_x(max_x)?.is_eq() || min_y.compare_y(max_y)?.is_eq() {
        return Err(GeometryErrorKind::voronoi(
            "exact frame input has zero span",
        ));
    }
    let (min_x, max_x, min_y, max_y) = (
        min_x.coords()?,
        max_x.coords()?,
        min_y.coords()?,
        max_y.coords()?,
    );
    Ok([
        corner((&min_x.x, &min_x.w), (&min_y.y, &min_y.w))?,
        corner((&max_x.x, &max_x.w), (&min_y.y, &min_y.w))?,
        corner((&max_x.x, &max_x.w), (&max_y.y, &max_y.w))?,
        corner((&min_x.x, &min_x.w), (&max_y.y, &max_y.w))?,
    ])
}

pub(in crate::geometry::tessellation) fn rectangular_boundary_for_exact(
    points: &[&ExactPoint],
    padded: bool,
) -> Result<[ExactPoint; 4]> {
    let first = points
        .first()
        .copied()
        .ok_or_else(|| GeometryErrorKind::voronoi("empty Voronoi site set"))?;
    let x_axis = ExactDyadic::one();
    let y_axis = ExactDyadic::zero();
    let mut min_x = first.projected_coordinate(&x_axis, &y_axis)?;
    let mut max_x = min_x.clone();
    let mut min_y = first.projected_coordinate(&y_axis, &x_axis)?;
    let mut max_y = min_y.clone();
    for point in points.iter().skip(1) {
        let x = point.projected_coordinate(&x_axis, &y_axis)?;
        if x.try_cmp(&min_x)?.is_lt() {
            min_x = x.clone();
        }
        if x.try_cmp(&max_x)?.is_gt() {
            max_x = x;
        }
        let y = point.projected_coordinate(&y_axis, &x_axis)?;
        if y.try_cmp(&min_y)?.is_lt() {
            min_y = y.clone();
        }
        if y.try_cmp(&max_y)?.is_gt() {
            max_y = y;
        }
    }
    if padded {
        let span_x = max_x.try_subtract(&min_x)?;
        let span_y = max_y.try_subtract(&min_y)?;
        let span = if span_x.try_cmp(&span_y)?.is_gt() {
            span_x
        } else {
            span_y
        };
        if span.numerator.is_zero() {
            return Err(GeometryErrorKind::voronoi(
                "exact frame input has zero span",
            ));
        }
        let half = span.try_product(&ExactRational::from_f64(0.5))?;
        min_x = min_x.try_subtract(&half)?;
        max_x = max_x.try_add(&half)?;
        min_y = min_y.try_subtract(&half)?;
        max_y = max_y.try_add(&half)?;

        let outward = |value: &ExactRational, lower: bool| -> Result<ExactRational> {
            let rounded = round_ratio(&value.numerator, &value.denominator)?;
            let order = ExactRational::from_f64(rounded).try_cmp(value)?;
            let rounded = if lower && order.is_gt() {
                rounded.next_down()
            } else if !lower && order.is_lt() {
                rounded.next_up()
            } else {
                rounded
            };
            if !rounded.is_finite() {
                return Err(GeometryErrorKind::voronoi(
                    "padded Voronoi frame overflows binary64",
                ));
            }
            Ok(ExactRational::from_f64(rounded))
        };
        min_x = outward(&min_x, true)?;
        max_x = outward(&max_x, false)?;
        min_y = outward(&min_y, true)?;
        max_y = outward(&max_y, false)?;
    }
    Ok([
        ExactPoint::from_rationals(&min_x, &min_y)?,
        ExactPoint::from_rationals(&max_x, &min_y)?,
        ExactPoint::from_rationals(&max_x, &max_y)?,
        ExactPoint::from_rationals(&min_x, &max_y)?,
    ])
}
