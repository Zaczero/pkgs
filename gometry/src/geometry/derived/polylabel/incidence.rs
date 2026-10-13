use super::*;
#[cfg(test)]
use crate::geometry::XY;
#[cfg(test)]
use crate::geometry::exact_arith::ExactSign;

/// Supporting half-planes bound every contained disk. The cyclic normal
/// cross-products cancel its center; a three-plane solve only nominates a
/// center, whose actual clearance is measured by the cell field.
pub(super) fn label_carrier_enclosure(
    linework: &PreparedLinework,
    probe: Point,
    tolerance: f64,
) -> Option<(f64, Point)> {
    let mut segments = Vec::new();
    linework.for_each_segment(|segment| segments.push(segment));
    let mut planes = Vec::new();
    let mut scale = 0.0_f64;
    for &segment in &segments {
        let dx = segment.end.x - segment.start.x;
        let dy = segment.end.y - segment.start.y;
        let length = dx.hypot(dy);
        if length == 0.0 || !length.is_finite() {
            continue;
        }
        let side = orientation(segment.start, segment.end, probe);
        if side == Orientation::Collinear {
            continue;
        }
        if segments.iter().any(|other| {
            [other.start, other.end].into_iter().any(|point| {
                let other_side = orientation(segment.start, segment.end, point);
                other_side != side && other_side != Orientation::Collinear
            })
        }) {
            continue;
        }
        let sign = if side == Orientation::CounterClockwise {
            1.0
        } else {
            -1.0
        };
        let nx = -dy / length * sign;
        let ny = dx / length * sign;
        let x = probe.x - segment.start.x;
        let y = probe.y - segment.start.y;
        scale = scale.max(x.abs()).max(y.abs()).max(length);
        planes.push((nx, ny, nx * x + ny * y));
    }
    // If arithmetic uncertainty is material to the requested tolerance,
    // retain the cell bound instead of trusting the floating carrier solve.
    let error = scale * (128.0 * f64::EPSILON);
    if !error.is_finite() || error >= tolerance / 4.0 {
        return None;
    }
    let mut best = None;
    let cross = |a: (f64, f64, f64), b: (f64, f64, f64)| a.0 * b.1 - a.1 * b.0;
    for (i, &a) in planes.iter().enumerate() {
        for (j, &b) in planes.iter().enumerate().skip(i.wrapping_add(1)) {
            for &c in planes.iter().skip(j.wrapping_add(1)) {
                let mut weights = [cross(b, c), cross(c, a), cross(a, b)];
                if weights.iter().all(|&w| w <= 0.0) {
                    for w in &mut weights {
                        *w = -*w;
                    }
                }
                if weights.iter().any(|&w| w < 0.0) {
                    continue;
                }
                let sum = weights.iter().sum::<f64>();
                if sum <= f64::EPSILON.sqrt() {
                    continue;
                }
                let radius = (weights[0] * a.2 + weights[1] * b.2 + weights[2] * c.2) / sum;
                let padding = error / sum;
                if !radius.is_finite() || padding >= tolerance / 4.0 {
                    continue;
                }
                let upper = (radius + padding).next_up();
                if best.as_ref().is_some_and(|&(bound, _)| upper >= bound) {
                    continue;
                }
                let mut nominee = None;
                let mut determinant = 0.0;
                for (left, right) in [(a, b), (a, c), (b, c)] {
                    let det = cross(left, right);
                    if det.abs() > determinant {
                        determinant = det.abs();
                        let ar = radius - left.2;
                        let br = radius - right.2;
                        nominee = Some(Point::new_raw_xy(
                            probe.x + (ar * right.1 - left.1 * br) / det,
                            probe.y + (left.0 * br - ar * right.0) / det,
                        ));
                    }
                }
                if let Some(point) = nominee
                    && point.x.is_finite()
                    && point.y.is_finite()
                {
                    best = Some((upper, point));
                }
            }
        }
    }
    best
}

pub(super) fn framed_metric_segment_distance(
    frame: AxisFrame,
    point: Point,
    segment: Segment,
) -> f64 {
    let ux = (point.x - segment.start.x) / frame.scale_x();
    let uy = (point.y - segment.start.y) / frame.scale_y();
    let vx = (segment.end.x - segment.start.x) / frame.scale_x();
    let vy = (segment.end.y - segment.start.y) / frame.scale_y();
    let magnitude = vx.abs().max(vy.abs());
    let fraction = if magnitude == 0.0 {
        0.0
    } else {
        let nx = vx / magnitude;
        let ny = vy / magnitude;
        let dot = if nx == 0.0 {
            0.0
        } else {
            (ux / magnitude) * nx
        } + if ny == 0.0 {
            0.0
        } else {
            (uy / magnitude) * ny
        };
        (dot / (nx * nx + ny * ny)).clamp(0.0, 1.0)
    };
    let x = segment.start.x + (segment.end.x - segment.start.x) * fraction;
    let y = segment.start.y + (segment.end.y - segment.start.y) * fraction;
    ((point.x - x) / frame.scale_x()).hypot((point.y - y) / frame.scale_y())
}

pub(super) fn has_interior(shape: &Shape) -> bool {
    let ring_spans_area = |ring: &Ring| {
        let mut points = ring.coords().iter_coords();
        let Some(first) = points.next() else {
            return false;
        };
        let mut points = points.skip_while(|point| point.xy() == first.xy());
        let Some(second) = points.next() else {
            return false;
        };
        points.any(|point| orientation(first, second, point) != Orientation::Collinear)
    };
    match shape {
        Shape::Polygon(polygon) => polygon.rings().any(ring_spans_area),
        Shape::MultiPolygon(polygons) => polygons
            .iter()
            .flat_map(Polygon::rings)
            .any(ring_spans_area),
        Shape::GeometryCollection(geometries) => geometries.iter().any(has_interior),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => false,
    }
}

pub(super) fn representative_point(shape: &Shape, bounds: Bounds) -> Result<Point> {
    Ok(match shape.point_on_surface()? {
        Shape::Point(point) => point,
        Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => degenerate_polygon_center(bounds),
    })
}

#[cfg(test)]
pub(super) type ExactRatio = (ExactDyadic, ExactDyadic);

#[cfg(test)]
pub(super) fn compare_exact_ratios(left: &ExactRatio, right: &ExactRatio) -> Result<Ordering> {
    compare_ratios(&left.0, &left.1, &right.0, &right.1)
}

pub(in crate::geometry::derived) fn exact_difference(left: f64, right: f64) -> Result<ExactDyadic> {
    ExactDyadic::from_f64(left).try_subtract(ExactDyadic::from_f64(right))
}

#[cfg(test)]
pub(super) fn exact_squared_sum(left: &ExactDyadic, right: &ExactDyadic) -> Result<ExactDyadic> {
    left.try_product(left)?.try_add(right.try_product(right)?)
}

#[cfg(test)]
pub(super) fn exact_point_pair_squared(left: XY, right: XY) -> Result<ExactRatio> {
    let dx = exact_difference(left.x, right.x)?;
    let dy = exact_difference(left.y, right.y)?;
    Ok((exact_squared_sum(&dx, &dy)?, ExactDyadic::from_f64(1.0)))
}

#[cfg(test)]
pub(super) fn exact_point_segment_squared(point: Point, segment: Segment) -> Result<ExactRatio> {
    let point = point.xy();
    let dx = exact_difference(segment.end.x, segment.start.x)?;
    let dy = exact_difference(segment.end.y, segment.start.y)?;
    let qx = exact_difference(point.x, segment.start.x)?;
    let qy = exact_difference(point.y, segment.start.y)?;
    let denominator = dx.try_product(&dx)?.try_add(dy.try_product(&dy)?)?;
    let zero = ExactDyadic::from_f64(0.0);
    if denominator.is_zero() {
        return exact_point_pair_squared(point, segment.start);
    }
    let numerator = qx.try_product(&dx)?.try_add(qy.try_product(&dy)?)?;
    if numerator.cmp(&zero) != Ordering::Greater {
        return exact_point_pair_squared(point, segment.start);
    }
    if numerator.cmp(&denominator) != Ordering::Less {
        return exact_point_pair_squared(point, segment.end);
    }
    let point_squared = exact_squared_sum(&qx, &qy)?;
    Ok((
        point_squared
            .try_product(&denominator)?
            .try_subtract(numerator.try_product(&numerator)?)?,
        denominator,
    ))
}

#[cfg(test)]
pub(super) fn exact_boundary_distance_ratio(
    shape: &Shape,
    center: Point,
) -> Result<Option<ExactRatio>> {
    let mut best: Option<ExactRatio> = None;
    let mut error = None;
    shape.for_each_segment(|segment| {
        if error.is_some() {
            return;
        }
        let candidate = match exact_point_segment_squared(center, segment) {
            Ok(candidate) => candidate,
            Err(failure) => {
                error = Some(failure);
                return;
            },
        };
        let replace =
            best.as_ref()
                .is_none_or(|current| match compare_exact_ratios(&candidate, current) {
                    Ok(order) => order == Ordering::Less,
                    Err(failure) => {
                        error = Some(failure);
                        false
                    },
                });
        if replace {
            best = Some(candidate);
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(best)
}

#[cfg(test)]
pub(super) fn exact_boundary_squared(shape: &Shape, center: Point) -> Result<Rational> {
    exact_boundary_distance_ratio(shape, center)?.map_or_else(
        || Ok(Rational::zero()),
        |(numerator, denominator)| Rational::new(numerator, denominator),
    )
}

#[cfg(test)]
pub(super) fn exact_boundary_radius(shape: &Shape, center: Point) -> Result<f64> {
    exact_boundary_squared(shape, center).and_then(|distance| distance.round_sqrt())
}

#[cfg(test)]
pub(super) fn exact_cross(
    left_x: &ExactDyadic,
    left_y: &ExactDyadic,
    right_x: &ExactDyadic,
    right_y: &ExactDyadic,
) -> Result<ExactDyadic> {
    left_x
        .try_product(right_y)?
        .try_subtract(left_y.try_product(right_x)?)
}

#[cfg(test)]
pub(super) fn update_lower(
    lower: &mut ExactRatio,
    lower_closed: &mut bool,
    bound: ExactRatio,
) -> Result<()> {
    match compare_exact_ratios(&bound, lower)? {
        Ordering::Greater => {
            *lower = bound;
            *lower_closed = false;
        },
        Ordering::Equal => *lower_closed = false,
        Ordering::Less => {},
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn update_upper(
    upper: &mut ExactRatio,
    upper_closed: &mut bool,
    bound: ExactRatio,
) -> Result<()> {
    match compare_exact_ratios(&bound, upper)? {
        Ordering::Less => {
            *upper = bound;
            *upper_closed = false;
        },
        Ordering::Equal => *upper_closed = false,
        Ordering::Greater => {},
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn interval_is_empty(
    lower: &ExactRatio,
    lower_closed: bool,
    upper: &ExactRatio,
    upper_closed: bool,
) -> Result<bool> {
    Ok(match compare_exact_ratios(lower, upper)? {
        Ordering::Greater => true,
        Ordering::Equal => !(lower_closed && upper_closed),
        Ordering::Less => false,
    })
}

/// Whether a source segment has any point in the candidate's open interior.
/// The candidate is convex, so each edge contributes one exact affine strict
/// half-plane in the source segment parameter. The interval remains closed at
/// the source endpoints; only candidate boundary contacts are strict.
#[cfg(test)]
pub(super) fn segment_enters_candidate(
    segment: Segment,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let zero = ExactDyadic::from_f64(0.0);
    let one = ExactDyadic::from_f64(1.0);
    let lower_bound = (zero.clone(), one.clone());
    let upper_bound = (one.clone(), one);
    let mut lower = lower_bound;
    let mut upper = upper_bound;
    let mut lower_closed = true;
    let mut upper_closed = true;
    let source_dx = exact_difference(segment.end.x, segment.start.x)?;
    let source_dy = exact_difference(segment.end.y, segment.start.y)?;

    for (start, end) in candidate
        .iter()
        .copied()
        .zip(candidate.iter().copied().cycle().skip(1))
    {
        let edge_dx = exact_difference(end.x, start.x)?;
        let edge_dy = exact_difference(end.y, start.y)?;
        let start_x = exact_difference(segment.start.x, start.x)?;
        let start_y = exact_difference(segment.start.y, start.y)?;
        let c0 = exact_cross(&edge_dx, &edge_dy, &start_x, &start_y)?;
        let c1 = exact_cross(&edge_dx, &edge_dy, &source_dx, &source_dy)?;
        let (c0, c1) = if counter_clockwise {
            (c0, c1)
        } else {
            (
                zero.clone().try_subtract(c0)?,
                zero.clone().try_subtract(c1)?,
            )
        };
        match c1.sign() {
            ExactSign::Zero => {
                if c0.sign() != ExactSign::Positive {
                    return Ok(false);
                }
            },
            ExactSign::Positive => {
                update_lower(
                    &mut lower,
                    &mut lower_closed,
                    (zero.clone().try_subtract(c0)?, c1),
                )?;
            },
            ExactSign::Negative => {
                update_upper(
                    &mut upper,
                    &mut upper_closed,
                    (c0, zero.clone().try_subtract(c1)?),
                )?;
            },
        }
        if interval_is_empty(&lower, lower_closed, &upper, upper_closed)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn candidate_center_is_strictly_inside(
    candidate: &[Point],
    center: Point,
    counter_clockwise: bool,
) -> bool {
    candidate
        .iter()
        .copied()
        .zip(candidate.iter().copied().cycle().skip(1))
        .all(|(start, end)| {
            matches!(
                (counter_clockwise, orientation(start, end, center)),
                (true, Orientation::CounterClockwise) | (false, Orientation::Clockwise)
            )
        })
}

#[cfg(test)]
pub(super) fn candidate_covers_point(
    candidate: &[Point],
    point: Point,
    counter_clockwise: bool,
) -> bool {
    candidate
        .iter()
        .copied()
        .zip(candidate.iter().copied().cycle().skip(1))
        .all(|(start, end)| {
            !matches!(
                (counter_clockwise, orientation(start, end, point)),
                (true, Orientation::Clockwise) | (false, Orientation::CounterClockwise)
            )
        })
}

#[cfg(test)]
pub(super) fn source_boundary_enters_candidate(
    shape: &Shape,
    candidate: &[Point],
    counter_clockwise: bool,
) -> Result<bool> {
    let mut enters = false;
    let mut error = None;
    shape.for_each_segment(|segment| {
        if enters || error.is_some() {
            return;
        }
        match segment_enters_candidate(segment, candidate, counter_clockwise) {
            Ok(true) => enters = true,
            Ok(false) => {},
            Err(failure) => error = Some(failure),
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(enters)
}

/// A crossing scan alone cannot establish that a candidate has not enclosed a
/// complete hole cycle. Convexity makes the explicit cycle test cheap: if all
/// vertices of a hole are covered by the candidate, its filled interior is
/// covered as well. Rejecting this case is conservative for degenerate input
/// and closes the hole-without-crossing gap in the source certificate.
#[cfg(test)]
pub(super) fn source_hole_is_enclosed(
    shape: &Shape,
    candidate: &[Point],
    counter_clockwise: bool,
) -> bool {
    match shape {
        Shape::Polygon(polygon) => polygon.rings().skip(1).any(|hole| {
            hole.iter()
                .all(|point| candidate_covers_point(candidate, point, counter_clockwise))
        }),
        Shape::MultiPolygon(polygons) => polygons.iter().any(|polygon| {
            polygon.rings().skip(1).any(|hole| {
                hole.iter()
                    .all(|point| candidate_covers_point(candidate, point, counter_clockwise))
            })
        }),
        Shape::GeometryCollection(geometries) => geometries
            .iter()
            .any(|geometry| source_hole_is_enclosed(geometry, candidate, counter_clockwise)),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => false,
    }
}

#[cfg(test)]
pub(super) fn source_vertex_outside_candidate(shape: &Shape, candidate: &[Point]) -> bool {
    for &point in candidate {
        if !shape.covers_point(point) {
            return true;
        }
    }
    false
}
