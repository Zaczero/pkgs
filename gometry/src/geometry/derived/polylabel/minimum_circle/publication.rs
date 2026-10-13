use super::*;

pub(super) fn unrepresentable_minimum_circle() -> crate::error::Error {
    crate::geometry::GeometryErrorKind::unrepresentable_output("minimum_bounding_circle")
}

pub(super) fn circle_emission_error(error: crate::error::Error) -> crate::error::Error {
    if matches!(
        error.kind(),
        crate::error::ErrorKind::Resource(_)
            | crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge { .. }
            )
    ) {
        error
    } else {
        unrepresentable_minimum_circle()
    }
}

pub(in crate::geometry::derived::polylabel) fn certify_minimum_circle_polygon(
    output: &Shape,
    disk: &CertifiedDisk,
    quadrant_segments: NonZeroU32,
) -> Result<()> {
    let Shape::Polygon(polygon) = output else {
        return Err(unrepresentable_minimum_circle());
    };
    let mut rings = polygon.rings();
    let Some(shell) = rings.next() else {
        return Err(unrepresentable_minimum_circle());
    };
    if rings.next().is_some() {
        return Err(unrepresentable_minimum_circle());
    }
    let steps = (quadrant_segments.get() as usize)
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let expected_length = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    if shell.coords().len() != expected_length {
        return Err(unrepresentable_minimum_circle());
    }

    let mut vertices = Vec::with_capacity(expected_length);
    for point in shell.coords().iter_coords() {
        if !point.x.is_finite() || !point.y.is_finite() {
            return Err(unrepresentable_minimum_circle());
        }
        vertices.push(point.xy());
    }
    let Some((last, open)) = vertices.split_last() else {
        return Err(unrepresentable_minimum_circle());
    };
    let [first, middle @ .., last_open] = open else {
        return Err(unrepresentable_minimum_circle());
    };
    let same_coordinate = |left: f64, right: f64| {
        crate::geometry::canonical_f64_bits(left) == crate::geometry::canonical_f64_bits(right)
    };
    if !same_coordinate(first.x, last.x) || !same_coordinate(first.y, last.y) {
        return Err(unrepresentable_minimum_circle());
    }
    if middle.is_empty() {
        return Err(unrepresentable_minimum_circle());
    }
    for (current, next) in open
        .iter()
        .copied()
        .zip(open.iter().copied().cycle().skip(1))
    {
        if same_coordinate(current.x, next.x) && same_coordinate(current.y, next.y) {
            return Err(unrepresentable_minimum_circle());
        }
    }

    let mut direction = None;
    let mut previous = *last_open;
    let next_points = open.iter().skip(1).chain(std::iter::once(first));
    for (&current, &next) in open.iter().zip(next_points) {
        let turn = exact::orient2d(previous, current, next);
        if turn == ExactSign::Zero {
            return Err(unrepresentable_minimum_circle());
        }
        if let Some(expected) = direction {
            if turn != expected {
                return Err(unrepresentable_minimum_circle());
            }
        } else {
            direction = Some(turn);
        }
        previous = current;
    }
    let direction = direction.expect("three or more open circle vertices");
    if direction != ExactSign::Positive {
        return Err(unrepresentable_minimum_circle());
    }

    let enclosure = disk.enclosure().map_err(circle_emission_error)?;
    for (left, right) in open
        .iter()
        .copied()
        .zip(open.iter().copied().cycle().skip(1))
    {
        if (!enclosure
            .as_ref()
            .is_some_and(|enclosure| enclosure.proves_vertex_outside(left))
            && !disk
                .vertex_is_strictly_outside(left)
                .map_err(circle_emission_error)?)
            || (!enclosure
                .as_ref()
                .is_some_and(|enclosure| enclosure.proves_ccw_edge_encloses(left, right))
                && !disk
                    .ccw_edge_encloses(left, right)
                    .map_err(circle_emission_error)?)
        {
            return Err(unrepresentable_minimum_circle());
        }
    }

    if output.validate().map_err(circle_emission_error)?.is_some() {
        return Err(unrepresentable_minimum_circle());
    }
    Ok(())
}

/// Build the regular tangent-intersection polygon for one candidate radius.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "quadrants is nonzero and steps is checked as four times quadrants, bounding each step division and offset"
)]
pub(in crate::geometry::derived::polylabel) fn tangent_circle_polygon(
    center: Point,
    radius: f64,
    quadrant_segments: NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let quadrants = quadrant_segments.get() as usize;
    let steps = quadrants
        .checked_mul(4)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let vertices = steps
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    budget.add(vertices)?;

    // Each edge is tangent to the candidate circle. Its two adjacent
    // intersections are at half a step on either side of the edge normal,
    // so the relative coordinate factors are
    // `(cos(theta) / cos_half, sin(theta) / cos_half)`.
    let half_angle = std::f64::consts::PI / steps as f64;
    let (sin_half, cos_half) = half_angle.sin_cos();
    if !cos_half.is_finite() || cos_half <= 0.0 {
        return Err(unrepresentable_minimum_circle());
    }
    let tangent_half = sin_half / cos_half;
    if !tangent_half.is_finite() {
        return Err(unrepresentable_minimum_circle());
    }
    // Trigonometric construction is rounded before the exact certificate
    // sees the edge. Two representable successors provide a finite outward
    // construction margin without an epsilon predicate or segment inflation.
    let outward_component = |factor: f64| {
        if factor > 0.0 {
            factor.next_up().next_up().min(1.0)
        } else if factor < 0.0 {
            factor.next_down().next_down().max(-1.0)
        } else {
            factor
        }
    };

    let mut points = Vec::with_capacity(vertices);
    for step in 0..steps {
        let quadrant = step / quadrants;
        let local_step = step % quadrants;
        let (factor_x, factor_y) = if local_step == 0 || local_step + 1 == quadrants {
            // Reuse the half-angle value at both ends of each quadrant.
            // This makes factors that are mathematically one exactly one,
            // which matters when `radius == f64::MAX`.
            match (quadrant, local_step == 0) {
                (0, true) => (1.0, tangent_half),
                (0, false) => (tangent_half, 1.0),
                (1, true) => (-tangent_half, 1.0),
                (1, false) => (-1.0, tangent_half),
                (2, true) => (-1.0, -tangent_half),
                (2, false) => (-tangent_half, -1.0),
                (3, true) => (tangent_half, -1.0),
                (3, false) => (1.0, -tangent_half),
                _ => unreachable!("four quadrants"),
            }
        } else {
            let angle = std::f64::consts::TAU * (step as f64 + 0.5) / steps as f64;
            let (sin, cos) = angle.sin_cos();
            (cos / cos_half, sin / cos_half)
        };
        let x = center.x + radius * outward_component(factor_x);
        let y = center.y + radius * outward_component(factor_y);
        if !x.is_finite() || !y.is_finite() {
            return Err(unrepresentable_minimum_circle());
        }
        points.push(Point::new_raw_xy(x, y));
    }
    let first = points
        .first()
        .copied()
        .ok_or_else(unrepresentable_minimum_circle)?;
    points.push(first);
    let seq = CoordSeq::try_from_points(&points)?;
    Ok(Shape::Polygon(Polygon::new(
        Ring::try_from_closed(seq)?,
        Vec::new(),
    )))
}

/// Emit a regular tangent-intersection polygon at a candidate disk radius,
/// then certify the stored output before publishing it. Every retry charges
/// the existing operation-wide generated-work budget; the requested segment
/// count never changes. The loop ends only when the next radius is
/// non-finite or that declared resource bound is reached.
/// Retries that move a realized circle's radius one ulp at a time before the
/// adjustment starts doubling.
const ULP_REALIZATION_RETRIES: u32 = 64;

/// The next radius to try when realizing a circle of `exact` radius as a ring
/// failed at `candidate`: outward for a circumscribed ring, inward for an
/// inscribed one. The first retries move one ulp; after that the offset from
/// `exact` doubles, so a circle far below the coordinate spacing at its center
/// reaches a representable ring in logarithmically many retries.
pub(in crate::geometry::derived::polylabel) fn next_realization_radius(
    exact: f64,
    candidate: f64,
    retries: u32,
    outward: bool,
) -> f64 {
    match (outward, retries < ULP_REALIZATION_RETRIES) {
        (true, true) => candidate.next_up(),
        (false, true) => candidate.next_down(),
        (true, false) => (exact + 2.0 * (candidate - exact)).max(candidate.next_up()),
        (false, false) => (exact - 2.0 * (exact - candidate)).min(candidate.next_down()),
    }
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "each realization retry charges at least five items against the 16-million item budget"
)]
pub(crate) fn emit_minimum_bounding_circle(
    disk: &CertifiedDisk,
    quadrant_segments: NonZeroU32,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    let radius = disk.radius().map_err(circle_emission_error)?;
    let center = disk.center().map_err(circle_emission_error)?;
    if !center.x.is_finite() || !center.y.is_finite() || !radius.is_finite() || radius <= 0.0 {
        return Err(unrepresentable_minimum_circle());
    }
    // `tangent_circle_polygon` commits one complete fixed-size ring before
    // allocating it, and the operation's generated-item budget charges each
    // retry.
    // Both the center and translated vertices are rounded at the coordinate
    // scale. Cover that scale before the certificate; radius-only ulp retries
    // can otherwise repeat the same realized ring dozens of times.
    let coordinate_scale = center.x.abs().max(center.y.abs()) + radius;
    #[expect(
        clippy::float_cmp,
        reason = "f64::MAX has no finite successor, so the initial margin uses its finite predecessor spacing"
    )]
    let coordinate_spacing = if coordinate_scale == f64::MAX {
        coordinate_scale - coordinate_scale.next_down()
    } else {
        coordinate_scale.next_up() - coordinate_scale
    };
    let mut candidate_radius = (radius + 8.0 * coordinate_spacing)
        .max(radius.next_up())
        .min(f64::MAX);
    let mut retries = 0_u32;
    loop {
        let output =
            match tangent_circle_polygon(center, candidate_radius, quadrant_segments, budget) {
                Ok(output) => output,
                Err(error) => return Err(circle_emission_error(error)),
            };
        match certify_minimum_circle_polygon(&output, disk, quadrant_segments) {
            Ok(()) => return Ok(output),
            Err(error) if matches!(error.kind(), crate::error::ErrorKind::Resource(_)) => {
                return Err(error);
            },
            Err(_) => {},
        }
        retries += 1;
        // Rounding the center and each translated vertex costs coordinate
        // ulps, which can dwarf a radius ulp on a small translated disk.
        // Cover that construction scale before retrying the exact certificate.
        let coordinate_scale = center.x.abs().max(center.y.abs()) + candidate_radius;
        #[expect(
            clippy::float_cmp,
            reason = "f64::MAX has no finite successor, so a retry uses its finite predecessor spacing"
        )]
        let coordinate_spacing = if coordinate_scale == f64::MAX {
            coordinate_scale - coordinate_scale.next_down()
        } else {
            coordinate_scale.next_up() - coordinate_scale
        };
        let next = next_realization_radius(radius, candidate_radius, retries, true)
            .max(candidate_radius + 4.0 * coordinate_spacing);
        if !next.is_finite() {
            break;
        }
        candidate_radius = next;
    }
    Err(unrepresentable_minimum_circle())
}
