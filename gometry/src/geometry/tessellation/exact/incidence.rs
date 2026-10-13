//! Exact segment, cycle, and point incidence predicates.

use std::cmp::Ordering;

use super::{
    ExactDyadic, ExactLine, ExactPoint, Homogeneous, Interval, PointInCycle, SegmentIntersection,
    VERIFY_INTERVALS,
};
use crate::error::Result;
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::{GeometryErrorKind, XY};

fn on_segment(a: &ExactPoint, b: &ExactPoint, point: &ExactPoint) -> Result<bool> {
    let between = |pa: Ordering, pb: Ordering| {
        (pa != Ordering::Less && pb != Ordering::Greater)
            || (pb != Ordering::Less && pa != Ordering::Greater)
    };
    Ok(orient_points(a, b, point)? == ExactSign::Zero
        && between(point.compare_x(a)?, point.compare_x(b)?)
        && between(point.compare_y(a)?, point.compare_y(b)?))
}

pub(in crate::geometry::tessellation) fn compare_along(
    start: &ExactPoint,
    end: &ExactPoint,
    left: &ExactPoint,
    right: &ExactPoint,
) -> Result<Ordering> {
    let (axis_order, direction) = match start.compare_x(end)? {
        Ordering::Equal => (left.compare_y(right)?, start.compare_y(end)?),
        direction @ (Ordering::Less | Ordering::Greater) => (left.compare_x(right)?, direction),
    };
    Ok(if direction.is_gt() {
        axis_order.reverse()
    } else {
        axis_order
    })
}

pub(in crate::geometry::tessellation) fn segment_intersection(
    a0: &ExactPoint,
    a1: &ExactPoint,
    b0: &ExactPoint,
    b1: &ExactPoint,
) -> Result<SegmentIntersection> {
    // A homogeneous intersection of parallel support lines has `w == 0`.
    // It is not a point on either finite segment; admitting it as a degenerate
    // segment endpoint can place an event beyond both stored endpoints.
    if !a0.is_finite() || !a1.is_finite() || !b0.is_finite() || !b1.is_finite() {
        return Ok(SegmentIntersection::None);
    }
    let [o0, o1, o2, o3] = [
        orient_points(a0, a1, b0)?,
        orient_points(a0, a1, b1)?,
        orient_points(b0, b1, a0)?,
        orient_points(b0, b1, a1)?,
    ];
    if [o0, o1, o2, o3].iter().all(|sign| *sign == ExactSign::Zero) {
        let (b_start, b_end) = if compare_along(a0, a1, b0, b1)?.is_gt() {
            (b1, b0)
        } else {
            (b0, b1)
        };
        let start = if compare_along(a0, a1, a0, b_start)?.is_gt() {
            a0
        } else {
            b_start
        };
        let end = if compare_along(a0, a1, a1, b_end)?.is_lt() {
            a1
        } else {
            b_end
        };
        return Ok(match compare_along(a0, a1, start, end)? {
            Ordering::Greater => SegmentIntersection::None,
            Ordering::Equal => SegmentIntersection::Point(start.clone()),
            Ordering::Less => SegmentIntersection::Overlap {
                start: start.clone(),
                end: end.clone(),
            },
        });
    }
    for (zero, (from, to, point)) in [
        (o0, (a0, a1, b0)),
        (o1, (a0, a1, b1)),
        (o2, (b0, b1, a0)),
        (o3, (b0, b1, a1)),
    ] {
        if zero == ExactSign::Zero && on_segment(from, to, point)? {
            return Ok(SegmentIntersection::Point(point.clone()));
        }
    }
    if o0.as_i8().wrapping_mul(o1.as_i8()) < 0 && o2.as_i8().wrapping_mul(o3.as_i8()) < 0 {
        return Ok(SegmentIntersection::Point(line_intersection(
            &ExactLine::through_points(a0, a1)?,
            &ExactLine::through_points(b0, b1)?,
        )?));
    }
    Ok(SegmentIntersection::None)
}

pub(in crate::geometry::tessellation) fn angle_ccw_cmp(
    origin: &ExactPoint,
    left: &ExactPoint,
    right: &ExactPoint,
) -> Result<Ordering> {
    let half = |target: &ExactPoint| -> Result<bool> {
        let (target, center) = (target.coords()?, origin.coords()?);
        let dy = target
            .y
            .try_product(&center.w)?
            .try_subtract(center.y.try_product(&target.w)?)?;
        let dx = target
            .x
            .try_product(&center.w)?
            .try_subtract(center.x.try_product(&target.w)?)?;
        let weight = target.w.sign().quotient(center.w.sign());
        let signed = |value: &ExactDyadic| value.sign().weighted_by(weight);
        Ok(signed(&dy) == ExactSign::Positive
            || (signed(&dy) == ExactSign::Zero && signed(&dx) != ExactSign::Negative))
    };
    match (half(left)?, half(right)?) {
        (true, false) => Ok(Ordering::Less),
        (false, true) => Ok(Ordering::Greater),
        _ => match orient_points(origin, left, right)? {
            ExactSign::Positive => Ok(Ordering::Less),
            ExactSign::Negative => Ok(Ordering::Greater),
            ExactSign::Zero => Err(GeometryErrorKind::voronoi(
                "exact angular tie proves an un-noded overlap or T-junction",
            )),
        },
    }
}

/// The shoelace sign in interval arithmetic. Every term is the cross
/// product of two affine vertices, each inside its certified interval, so the
/// accumulated interval contains the exact signed area; a sign is returned only
/// when it excludes zero. Declining falls through to the exact accumulation,
/// which is also the only path that can report the zero-orientation error.
fn cycle_pairs<'a>(
    open_cycle: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> impl Iterator<Item = (&'a ExactPoint, &'a ExactPoint)> {
    let mut shifted = open_cycle.clone();
    let first = shifted
        .next()
        .expect("exact cycle has at least three vertices");
    open_cycle.zip(shifted.chain(std::iter::once(first)))
}

fn cycle_orientation_filtered<'a>(
    open_cycle: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> Option<ExactSign> {
    let mut total = Interval::exact(0.0);
    for (left, right) in cycle_pairs(open_cycle) {
        if !left.is_finite() || !right.is_finite() {
            return None;
        }
        let term = left.bounds[0]
            .mul(right.bounds[1])
            .sub(left.bounds[1].mul(right.bounds[0]));
        if !term.is_finite() {
            return None;
        }
        total = total.add(term);
        if !total.is_finite() {
            return None;
        }
    }
    total.sign()
}

pub(in crate::geometry::tessellation) fn cycle_orientation<'a, I>(
    open_cycle: I,
) -> Result<ExactSign>
where
    I: IntoIterator<Item = &'a ExactPoint>,
    I::IntoIter: Clone + ExactSizeIterator,
{
    let open_cycle = open_cycle.into_iter();
    if open_cycle.len() < 3 {
        return Err(GeometryErrorKind::voronoi(
            "exact cycle has fewer than three vertices",
        ));
    }
    if let Some(sign) = cycle_orientation_filtered(open_cycle.clone()) {
        if VERIFY_INTERVALS {
            let exact = cycle_orientation_exact(open_cycle)?;
            if sign != exact {
                return Err(GeometryErrorKind::voronoi(
                    "filtered cycle orientation disagrees with exact orientation",
                ));
            }
        }
        return Ok(sign);
    }
    cycle_orientation_exact(open_cycle)
}

fn cycle_orientation_exact<'a>(
    open_cycle: impl Iterator<Item = &'a ExactPoint> + Clone,
) -> Result<ExactSign> {
    let mut numerator = ExactDyadic::zero();
    let mut denominator = ExactDyadic::from_f64(1.0);
    for (left, right) in cycle_pairs(open_cycle) {
        let left = left.coords()?;
        let right = right.coords()?;
        let term_n = left
            .x
            .try_product(&right.y)?
            .try_subtract(left.y.try_product(&right.x)?)?;
        let term_d = left.w.try_product(&right.w)?;
        numerator = numerator
            .try_product(&term_d)?
            .try_add(term_n.try_product(&denominator)?)?;
        denominator = denominator.try_product(&term_d)?;
    }
    match numerator.sign() {
        ExactSign::Zero => Err(GeometryErrorKind::voronoi(
            "exact cycle has zero orientation",
        )),
        sign @ (ExactSign::Negative | ExactSign::Positive) => {
            Ok(sign.weighted_by(denominator.sign()))
        },
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "a slice of nonzero-sized ExactPoints has at most isize::MAX elements, so the winding count cannot overflow i64 on 64-bit targets"
)]
pub(in crate::geometry::tessellation) fn point_in_cycle(
    open_cycle: &[ExactPoint],
    point: &ExactPoint,
) -> Result<PointInCycle> {
    let mut winding = 0_i64;
    for (start, end) in open_cycle.iter().zip(open_cycle.iter().cycle().skip(1)) {
        if on_segment(start, end, point)? {
            return Ok(PointInCycle::Boundary);
        }
        let sy = start.compare_y(point)?;
        let ey = end.compare_y(point)?;
        let orientation = orient_points(start, end, point)?;
        if sy != Ordering::Greater && ey == Ordering::Greater && orientation == ExactSign::Positive
        {
            winding += 1;
        } else if ey != Ordering::Greater
            && sy == Ordering::Greater
            && orientation == ExactSign::Negative
        {
            winding -= 1;
        }
    }
    Ok(if winding == 0 {
        PointInCycle::Outside
    } else {
        PointInCycle::Inside
    })
}

/// Filter for "which stored site is nearer this exact point".
/// The exact fallback evaluates the stored sites' perpendicular bisector at
/// the homogeneous point; its signed line product accounts for the sign of
/// the point weight.
fn squared_distance_cmp_point_filtered(
    origin: &ExactPoint,
    left: XY,
    right: XY,
) -> Option<Ordering> {
    if !origin.is_finite() {
        return None;
    }
    let squared = |point: XY| {
        let dx = Interval::exact(point.x).sub(origin.bounds[0]);
        let dy = Interval::exact(point.y).sub(origin.bounds[1]);
        dx.mul(dx).add(dy.mul(dy))
    };
    let difference = squared(left).sub(squared(right));
    if !difference.is_finite() {
        return None;
    }
    match difference.sign()? {
        ExactSign::Negative => Some(Ordering::Less),
        ExactSign::Positive => Some(Ordering::Greater),
        ExactSign::Zero => None,
    }
}

pub(in crate::geometry) fn squared_distance_cmp_point(
    origin: &ExactPoint,
    left: XY,
    right: XY,
) -> Result<Ordering> {
    if let Some(order) = squared_distance_cmp_point_filtered(origin, left, right) {
        if VERIFY_INTERVALS && order != squared_distance_cmp_point_exact(origin, left, right)? {
            return Err(GeometryErrorKind::voronoi(
                "filtered squared-distance comparison disagrees",
            ));
        }
        return Ok(order);
    }
    squared_distance_cmp_point_exact(origin, left, right)
}

pub(in crate::geometry::tessellation) fn squared_distance_cmp_points(
    origin: &ExactPoint,
    left: &ExactPoint,
    right: &ExactPoint,
) -> Result<Ordering> {
    Ok(
        match signed_line_product(
            &ExactLine::perpendicular_bisector_points(left, right)?,
            origin,
        )? {
            ExactSign::Negative => Ordering::Less,
            ExactSign::Zero => Ordering::Equal,
            ExactSign::Positive => Ordering::Greater,
        },
    )
}

pub(super) fn squared_distance_cmp_point_exact(
    origin: &ExactPoint,
    left: XY,
    right: XY,
) -> Result<Ordering> {
    debug_assert!(origin.is_finite());
    Ok(
        match signed_line_product(&ExactLine::perpendicular_bisector(left, right)?, origin)? {
            ExactSign::Negative => Ordering::Less,
            ExactSign::Zero => Ordering::Equal,
            ExactSign::Positive => Ordering::Greater,
        },
    )
}

/// Exact orientation of three finite homogeneous points.  The sign is
/// normalized for the (otherwise arbitrary) signs of their denominators.
/// Interval evaluation of the affine orientation determinant.
///
/// `orient_points` normalizes away the homogeneous denominators, so its verdict
/// IS the orientation of the three affine points `(x/w, y/w)`. Each of those
/// six coordinates lies in its point's certified interval, so evaluating the
/// determinant in outward-rounded interval arithmetic yields an interval that
/// CONTAINS the exact determinant. A sign is returned only when that interval
/// excludes zero, which proves it. Overlapping zero, or any non-finite operand,
/// declines to the exact determinant.
fn orient_points_filtered(a: &ExactPoint, b: &ExactPoint, c: &ExactPoint) -> Option<ExactSign> {
    if !a.is_finite() || !b.is_finite() || !c.is_finite() {
        return None;
    }
    let [ax, ay] = a.bounds;
    let [bx, by] = b.bounds;
    let [cx, cy] = c.bounds;
    if !(ax.is_finite()
        && ay.is_finite()
        && bx.is_finite()
        && by.is_finite()
        && cx.is_finite()
        && cy.is_finite())
    {
        return None;
    }
    let (bax, bay) = (bx.sub(ax), by.sub(ay));
    let (cax, cay) = (cx.sub(ax), cy.sub(ay));
    if !(bax.is_finite() && bay.is_finite() && cax.is_finite() && cay.is_finite()) {
        return None;
    }
    let determinant = bax.mul(cay).sub(bay.mul(cax));
    determinant.sign()
}

pub(in crate::geometry::tessellation) fn orient_points(
    a: &ExactPoint,
    b: &ExactPoint,
    c: &ExactPoint,
) -> Result<ExactSign> {
    if let Some(sign) = orient_points_filtered(a, b, c) {
        if VERIFY_INTERVALS && sign != orient_points_exact(a, b, c)? {
            return Err(GeometryErrorKind::voronoi("filtered orientation disagrees"));
        }
        return Ok(sign);
    }
    orient_points_exact(a, b, c)
}

fn orient_points_exact(a: &ExactPoint, b: &ExactPoint, c: &ExactPoint) -> Result<ExactSign> {
    let (a, b, c) = (a.coords()?, b.coords()?, c.coords()?);
    let displacement = |to: &ExactDyadic, from: &ExactDyadic, to_w: &ExactDyadic| {
        to.try_product(&a.w)?.try_subtract(from.try_product(to_w)?)
    };
    let bax = displacement(&b.x, &a.x, &b.w)?;
    let bay = displacement(&b.y, &a.y, &b.w)?;
    let cax = displacement(&c.x, &a.x, &c.w)?;
    let cay = displacement(&c.y, &a.y, &c.w)?;
    let sign = bax
        .try_product(&cay)?
        .try_subtract(bay.try_product(&cax)?)?
        .sign();
    // Both displacement numerators carry one factor of `a.w`; it is
    // squared in the determinant and therefore cannot affect the sign.
    Ok(sign.weighted_by(b.w.sign().quotient(c.w.sign())))
}

/// Cross product of two exact homogeneous lines as raw coordinates. The result
/// is not normalized: signed products and ratio comparison operate directly on
/// it.
pub(super) fn intersection_homogeneous(a: &ExactLine, b: &ExactLine) -> Result<Homogeneous> {
    let [a0, a1, a2] = &a.0;
    let [b0, b1, b2] = &b.0;
    Ok(Homogeneous {
        x: a1.try_product(b2)?.try_subtract(a2.try_product(b1)?)?,
        y: a2.try_product(b0)?.try_subtract(a0.try_product(b2)?)?,
        w: a0.try_product(b1)?.try_subtract(a1.try_product(b0)?)?,
    })
}

/// Cross product of two exact homogeneous lines, with certified bounds.
pub(in crate::geometry) fn line_intersection(a: &ExactLine, b: &ExactLine) -> Result<ExactPoint> {
    let Homogeneous { x, y, w } = intersection_homogeneous(a, b)?;
    Ok(ExactPoint::homogeneous(x, y, w))
}

pub(in crate::geometry::tessellation) fn signed_line_product(
    line: &ExactLine,
    point: &ExactPoint,
) -> Result<ExactSign> {
    let point = point.coords()?;
    let sign = line.0[0]
        .try_product(&point.x)?
        .try_add(line.0[1].try_product(&point.y)?)?
        .try_add(line.0[2].try_product(&point.w)?)?
        .sign();
    Ok(sign.weighted_by(point.w.sign()))
}
