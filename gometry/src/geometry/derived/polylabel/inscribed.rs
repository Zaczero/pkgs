use super::{
    CoordSeq, ExpansionBudget, NonZeroU32, Point, Polygon, PreparedSupport, Rational, Result, Ring,
    Shape, candidate_center_is_strictly_inside, next_realization_radius, predicates,
};

pub(crate) fn emit_zero_radius_point(shape: &Shape, center: Point) -> Result<Shape> {
    // A zero or subnormal clearance cannot support a stable fixed-resolution
    // ring; closed membership, not the rounded radius, decides whether the
    // emitted point belongs to source.
    if center.x.is_finite() && center.y.is_finite() && shape.covers_point(center) {
        Ok(Shape::Point(center))
    } else {
        Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ))
    }
}

/// Build and source-certify one fixed-resolution inscribed ring. Planar
/// callers pass the prepared filled/boundary pair retained by the directed
/// search; `None` is the geographic local-projection path, which intentionally
/// keeps its existing approximate containment contract.
#[cfg(test)]
pub(crate) fn build_inscribed_circle(
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    support: Option<(&PreparedSupport, &PreparedSupport, &Rational)>,
) -> Result<Shape> {
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "quadrant_segments");
    build_inscribed_circle_budgeted(center, radius, quadrant_segments, support, &mut budget)
}

pub(crate) fn build_inscribed_circle_budgeted(
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    support: Option<(&PreparedSupport, &PreparedSupport, &Rational)>,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let steps = 4_usize
        .checked_mul(quadrant_segments.get() as usize)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let vertices = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.check_additional(vertices)?;
    if !center.x.is_finite() || !center.y.is_finite() || !radius.is_finite() {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let mut candidate_radius = radius;
    let mut retries = 0_u32;
    loop {
        budget.add(vertices)?;
        let attempt = build_inscribed_circle_once(
            center,
            candidate_radius,
            steps,
            vertices,
            quadrant_segments,
            support,
        );
        match attempt {
            Ok(output) => return Ok(output),
            Err(error)
                if support.is_some()
                    && matches!(
                        error.kind(),
                        crate::error::ErrorKind::Geometry(
                            crate::geometry::GeometryErrorKind::UnrepresentableOutput {
                                operation: "maximum_inscribed_circle",
                                ..
                            }
                        )
                    ) =>
            {
                retries = retries.wrapping_add(1);
                let next = next_realization_radius(radius, candidate_radius, retries, false);
                if !(next > 0.0 && next.is_finite()) {
                    return Err(error);
                }
                candidate_radius = next;
            },
            Err(error) => return Err(error),
        }
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "quadrant is nonzero and steps is checked as four times quadrant, bounding the step division and modulo"
)]
pub(super) fn build_inscribed_circle_once(
    center: Point,
    radius: f64,
    steps: usize,
    vertices: usize,
    quadrant_segments: NonZeroU32,
    support: Option<(&PreparedSupport, &PreparedSupport, &Rational)>,
) -> Result<Shape> {
    let mut points = Vec::with_capacity(steps);
    let quadrant = quadrant_segments.get() as usize;
    for step in 0..steps {
        let (unit_x, unit_y) = if step.is_multiple_of(quadrant) {
            match (step / quadrant) % 4 {
                0 => (1.0, 0.0),
                1 => (0.0, 1.0),
                2 => (-1.0, 0.0),
                _ => (0.0, -1.0),
            }
        } else {
            let (sin, cos) = (std::f64::consts::TAU * step as f64 / steps as f64).sin_cos();
            (cos, sin)
        };
        let x = center.x + radius * unit_x;
        let y = center.y + radius * unit_y;
        if !x.is_finite() || !y.is_finite() {
            return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
                "maximum_inscribed_circle",
            ));
        }
        points.push(Point::new_raw_xy(x, y));
    }
    let mut closed = Vec::with_capacity(vertices);
    closed.extend_from_slice(&points);
    closed.push(
        points
            .first()
            .copied()
            .expect("a nonzero quadrant count emits circle vertices"),
    );
    let seq = CoordSeq::try_from_points(&closed)?;
    let winding = crate::geometry::ring_winding(&seq);
    let strict_convex = predicates::shell_is_strictly_convex(&seq);
    if winding.is_degenerate() || !strict_convex {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let counter_clockwise = winding.is_ccw();
    let center_in_candidate =
        candidate_center_is_strictly_inside(&points, center, counter_clockwise);
    let source_contained = match support {
        None => true,
        Some((source, boundary, clearance)) => source.candidate_is_contained(
            boundary,
            center,
            clearance,
            &points,
            counter_clockwise,
        )?,
    };
    if !center_in_candidate || !source_contained {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(seq)?,
        Vec::new(),
    )))
}
