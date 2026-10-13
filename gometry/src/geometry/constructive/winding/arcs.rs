use super::*;

/// Source vector in a power-of-two frame. Preserve a finite world-space
/// residual when it exists; only an overflowing subtraction scales both
/// operands before forming the residual. The returned scale is the frame
/// factor applied to both components.
pub(crate) fn source_vector(from: XY, to: XY) -> Option<(f64, f64, f64)> {
    let raw_dx = to.x - from.x;
    let raw_dy = to.y - from.y;
    let scale = if raw_dx.is_finite() && raw_dy.is_finite() {
        1.0
    } else {
        let extent = from
            .x
            .abs()
            .max(from.y.abs())
            .max(to.x.abs())
            .max(to.y.abs());
        if !extent.is_finite() {
            return None;
        }
        axis_pow2_scale(extent)
    };
    let dx = scaled_residual(to.x, from.x, scale);
    let dy = scaled_residual(to.y, from.y, scale);
    (dx.is_finite() && dy.is_finite()).then_some((dx, dy, scale))
}

/// Unit right-side normal of the edge `from -> to` (`None` for degenerate
/// or unrepresentable source coordinates).
pub(crate) fn unit_right_normal(from: Point, to: Point) -> Option<(f64, f64)> {
    let (dx, dy, _) = source_vector(from.xy(), to.xy())?;
    let squared = dx * dx + dy * dy;
    // Ordinary finite normal: fast squared-length path (bit-stable).
    if squared.is_normal() {
        let length = squared.sqrt();
        return Some((dy / length, -dx / length));
    }
    // Zero / subnormal / overflow: max-abs normalize so stadium buffers of
    // length 1e-200 and 1e155 stay representable.
    let scale = dx.abs().max(dy.abs());
    if scale == 0.0 || !scale.is_finite() {
        return None;
    }
    let (nx, ny) = (dx / scale, dy / scale);
    let length = (nx * nx + ny * ny).sqrt();
    if length == 0.0 || !length.is_finite() {
        return None;
    }
    Some((ny / length, -nx / length))
}

/// Emit the inscribed arc points of a join/cap around `center` from
/// `from_angle` sweeping counter-clockwise by `sweep` (normalized into
/// `[0, tau)`), stepping at most `step_angle`.
pub(crate) fn emit_arc<S: WalkSink>(
    center: Point,
    from_angle: f64,
    mut sweep: f64,
    distance: f64,
    step_angle: f64,
    source: u32,
    sink: &mut S,
) -> Result<()> {
    if sweep < 0.0 {
        sweep += std::f64::consts::TAU;
    }
    let steps = (sweep / step_angle).ceil() as usize;
    if steps <= 1 {
        return Ok(());
    }
    // Rotation recurrence: two `sin_cos` calls per ARC instead of two
    // libm calls per vertex; drift over <= a full circle of steps is
    // far below the join placement tolerance.
    let (sin_step, cos_step) = (sweep / steps as f64).sin_cos();
    let (mut sin_theta, mut cos_theta) = from_angle.sin_cos();
    for _ in 1..steps {
        (cos_theta, sin_theta) = (
            cos_theta * cos_step - sin_theta * sin_step,
            sin_theta * cos_step + cos_theta * sin_step,
        );
        sink.push(
            center.x + distance * cos_theta,
            center.y + distance * sin_theta,
            source,
        )?;
    }
    Ok(())
}

/// Emit a round cap with the exact intersections of its circle and the other
/// endpoint's cap circle included as noding vertices. Chord-only arcs can
/// otherwise cross at an interior point of two chords, which is inside both
/// circles even though the true circle intersection is on both loci.
pub(super) fn emit_cap_with_peer<S: WalkSink>(
    tip: Point,
    nx: f64,
    ny: f64,
    peer: Point,
    distance: f64,
    cap_style: BufferCapStyle,
    step_angle: f64,
    source: u32,
    sink: &mut S,
) -> Result<()> {
    if !matches!(cap_style, BufferCapStyle::Round) {
        return emit_cap(tip, nx, ny, distance, cap_style, step_angle, source, sink);
    }
    let from_angle = ny.atan2(nx);
    let breaks = cap_circle_intersections(tip, peer, distance.abs(), from_angle)?;
    if breaks.iter().all(Option::is_none) {
        return emit_cap(tip, nx, ny, distance, cap_style, step_angle, source, sink);
    }
    emit_arc_with_breaks(
        tip,
        from_angle,
        std::f64::consts::PI,
        distance,
        step_angle,
        source,
        &breaks,
        sink,
    )
}

/// Return the circle-circle intersections that lie strictly inside this cap's
/// semicircle. The centers are canonicalized before construction so the same
/// point bits are supplied to both caps when they share an intersection.
fn cap_circle_intersections(
    center: Point,
    peer: Point,
    radius: f64,
    from_angle: f64,
) -> Result<[Option<(f64, Point)>; 2]> {
    let mut result = [None, None];
    if !radius.is_finite() || radius <= 0.0 {
        return Ok(result);
    }
    let (first, second) = match center.x.total_cmp(&peer.x) {
        Ordering::Less => (center, peer),
        Ordering::Equal => match center.y.total_cmp(&peer.y) {
            Ordering::Less | Ordering::Equal => (center, peer),
            Ordering::Greater => (peer, center),
        },
        Ordering::Greater => (peer, center),
    };
    let raw_dx = second.x - first.x;
    let raw_dy = second.y - first.y;
    let extent = if raw_dx.is_finite() && raw_dy.is_finite() {
        raw_dx.abs().max(raw_dy.abs()).max(radius)
    } else {
        first
            .x
            .abs()
            .max(first.y.abs())
            .max(second.x.abs())
            .max(second.y.abs())
            .max(radius)
    };
    if !extent.is_finite() {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    let scale = axis_pow2_scale(extent);
    let (dx, dy) = (
        scaled_residual(second.x, first.x, scale),
        scaled_residual(second.y, first.y, scale),
    );
    let scaled_radius = radius * scale;
    let separation = dx.hypot(dy);
    if ![dx, dy, scaled_radius, separation]
        .into_iter()
        .all(f64::is_finite)
    {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    if separation == 0.0 {
        return Ok(result);
    }
    let half = separation * 0.5;
    if half >= scaled_radius {
        return Ok(result);
    }
    let height = (scaled_radius - half).sqrt() * (scaled_radius + half).sqrt();
    if !height.is_finite() {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    if height == 0.0 {
        return Ok(result);
    }
    let midpoint = Point::new_raw_xy(
        interpolate_f64(first.x, second.x, 0.5),
        interpolate_f64(first.y, second.y, 0.5),
    );
    if !midpoint.x.is_finite() || !midpoint.y.is_finite() {
        return Err(GeometryErrorKind::message(
            "buffer calculation produced non-finite coordinates",
        ));
    }
    let (px, py) = (-dy / separation, dx / separation);
    let mut result_slots = result.iter_mut();
    for sign in [1.0, -1.0] {
        let (dir_x, dir_y) = if same_point(center, first) {
            (dx * 0.5 + sign * px * height, dy * 0.5 + sign * py * height)
        } else {
            (
                -dx * 0.5 + sign * px * height,
                -dy * 0.5 + sign * py * height,
            )
        };
        let offset = (dir_y.atan2(dir_x) - from_angle).rem_euclid(std::f64::consts::TAU);
        if offset <= 1e-12 || offset >= std::f64::consts::PI - 1e-12 {
            continue;
        }
        let height_world =
            scale_by_power_of_two(height, power_of_two_exponent(scale).wrapping_neg());
        if !height_world.is_finite() {
            return Err(GeometryErrorKind::message(
                "buffer calculation produced non-finite coordinates",
            ));
        }
        let point = Point::new_raw_xy(
            midpoint.x + sign * px * height_world,
            midpoint.y + sign * py * height_world,
        );
        if !point.x.is_finite() || !point.y.is_finite() {
            return Err(GeometryErrorKind::message(
                "buffer calculation produced non-finite coordinates",
            ));
        }
        let slot = result_slots
            .next()
            .expect("at most two cap-circle intersections exist");
        *slot = Some((offset, point));
    }
    if let [Some((left, _)), Some((right, _))] = &result
        && right < left
    {
        result.swap(0, 1);
    }
    Ok(result)
}

/// Emit an arc whose interior contains exact circle intersections. Ordinary
/// arcs stay on the recurrence fast path; this bounded path only subdivides
/// the few cap intervals that contain an endpoint-circle crossing.
fn emit_arc_with_breaks<S: WalkSink>(
    center: Point,
    mut from_angle: f64,
    mut sweep: f64,
    distance: f64,
    step_angle: f64,
    source: u32,
    breaks: &[Option<(f64, Point)>; 2],
    sink: &mut S,
) -> Result<()> {
    if sweep < 0.0 {
        sweep += std::f64::consts::TAU;
    }
    let mut angles = [0.0; 4];
    let mut exact = [None; 4];
    let mut count = 2;
    *angles
        .get_mut(1)
        .expect("arc endpoint fits the fixed break array") = sweep;
    for &(offset, point) in breaks.iter().flatten() {
        if offset > 0.0 && offset < sweep {
            *angles
                .get_mut(count)
                .expect("two circle crossings fit the fixed break array") = offset;
            *exact
                .get_mut(count)
                .expect("two circle crossings fit the fixed break array") = Some(point);
            count = count.wrapping_add(1);
        }
    }
    for left in 0..count {
        for right in left.wrapping_add(1)..count {
            let left_angle = *angles
                .get(left)
                .expect("arc sort index belongs to the break array");
            let right_angle = *angles
                .get(right)
                .expect("arc sort index belongs to the break array");
            if right_angle < left_angle {
                angles.swap(left, right);
                exact.swap(left, right);
            }
        }
    }
    let mut unique = 1_usize;
    for index in 1..count {
        let previous = unique.wrapping_sub(1);
        let angle = *angles
            .get(index)
            .expect("arc break index belongs to the break array");
        let previous_angle = *angles
            .get(previous)
            .expect("unique arc break index belongs to the break array");
        if angle - previous_angle <= 1e-12 {
            if exact
                .get(previous)
                .expect("unique arc break index belongs to the exact-point array")
                .is_none()
            {
                let exact_point = *exact
                    .get(index)
                    .expect("arc break index belongs to the exact-point array");
                *exact
                    .get_mut(previous)
                    .expect("unique arc break index belongs to the exact-point array") =
                    exact_point;
            }
            continue;
        }
        *angles
            .get_mut(unique)
            .expect("unique arc break fits the fixed break array") = angle;
        *exact
            .get_mut(unique)
            .expect("unique arc break fits the exact-point array") = *exact
            .get(index)
            .expect("arc break index belongs to the exact-point array");
        unique = unique.wrapping_add(1);
    }
    from_angle += *angles.first().expect("arc break array has a start angle");
    let last_interval = unique.wrapping_sub(1);
    for interval in 0..last_interval {
        let next = interval.wrapping_add(1);
        let span = *angles
            .get(next)
            .expect("arc interval endpoint belongs to the break array")
            - *angles
                .get(interval)
                .expect("arc interval start belongs to the break array");
        let steps = (span / step_angle).ceil() as usize;
        for step in 1..=steps {
            if next == last_interval && step == steps {
                continue;
            }
            if step == steps
                && let Some(point) = *exact
                    .get(next)
                    .expect("arc interval endpoint belongs to the exact-point array")
            {
                sink.push(point.x, point.y, source)?;
                continue;
            }
            let theta = from_angle
                + *angles
                    .get(interval)
                    .expect("arc interval start belongs to the break array")
                + span * step as f64 / steps as f64;
            let (sin_theta, cos_theta) = theta.sin_cos();
            sink.push(
                center.x + distance * cos_theta,
                center.y + distance * sin_theta,
                source,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extreme_cap_contacts_stay_on_their_scaled_distance_circles() {
        let center = Point::new_raw_xy(-1.5e308, 0.0);
        let peer = Point::new_raw_xy(1.5e308, 0.0);
        let radius = 1.6e308;
        let contacts = cap_circle_intersections(center, peer, radius, -std::f64::consts::FRAC_PI_2)
            .expect("finite circle contacts");

        assert_eq!(contacts.iter().flatten().count(), 2);
        let scale = axis_pow2_scale(radius);
        let radius = radius * scale;
        for (_, point) in contacts.into_iter().flatten() {
            let distance = scaled_residual(point.x, center.x, scale)
                .hypot(scaled_residual(point.y, center.y, scale));
            assert!(
                (distance - radius).abs() <= 16.0 * f64::EPSILON * radius,
                "distance {distance} differs from radius {radius}"
            );
        }
    }
}
