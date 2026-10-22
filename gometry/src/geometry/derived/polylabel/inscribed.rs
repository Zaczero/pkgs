use super::{
    CoordSeq, ExpansionBudget, NonZeroU32, Point, Polygon, Result, Ring, Shape,
    candidate_center_is_strictly_inside, predicates,
};

#[expect(
    clippy::arithmetic_side_effects,
    reason = "quadrant is nonzero and checked steps/vertices bound the cyclic construction"
)]
pub(crate) fn build_inscribed_circle_budgeted(
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let steps = 4_usize
        .checked_mul(quadrant_segments.get() as usize)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let vertices = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.add(vertices)?;
    if !center.x.is_finite() || !center.y.is_finite() || !radius.is_finite() {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    let mut points = Vec::with_capacity(vertices);
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
    points.push(
        points
            .first()
            .copied()
            .expect("a nonzero quadrant count emits circle vertices"),
    );
    let seq = CoordSeq::try_from_points(&points)?;
    let winding = crate::geometry::ring_winding(&seq);
    if winding.is_degenerate() || !predicates::shell_is_strictly_convex(&seq) {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    if !candidate_center_is_strictly_inside(
        points
            .get(..steps)
            .expect("the ring contains its admitted open vertices"),
        center,
        winding.is_ccw(),
    ) {
        return Err(crate::geometry::GeometryErrorKind::unrepresentable_output(
            "maximum_inscribed_circle",
        ));
    }
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(seq)?,
        Vec::new(),
    )))
}
