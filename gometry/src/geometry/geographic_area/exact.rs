use super::*;

pub(super) fn accumulate_exact(
    polygon: &Polygon,
    frame: GeographicFrame,
    x_origin: f64,
    y_origin: f64,
    longitude_scale: f64,
    coefficients: &[ExactDyadic],
    ring_moments: &mut [ExactDyadic],
    polygon_moments: &mut [ExactDyadic],
) -> Result<(ExactFactorialArea, ExactSourceBounds)> {
    use crate::geometry::exact_arith::ExactSign;

    for moment in ring_moments.iter_mut().chain(polygon_moments.iter_mut()) {
        *moment = ExactDyadic::zero();
    }
    let mut bounds = ExactSourceBounds::default();
    for (ring_index, ring) in polygon.rings().enumerate() {
        let mut chart_turn = 0_i64;
        let mut previous = ring.nth_coord(0);
        let first_connection = frame
            .longitude()
            .chart_connection(previous.x, ring.nth_coord(1).x)
            .expect("area geometry longitude edge was admitted");
        let previous_canonical = first_connection.start;
        let mut previous_x = exact_scaled_x(
            frame,
            previous_canonical.get(),
            chart_turn,
            x_origin,
            longitude_scale,
        )?;
        let mut previous_z = exact_scaled_y(frame, previous.y, y_origin)?;
        for index in 1..ring.coord_count() {
            let next = ring.nth_coord(index);
            let connection = frame
                .longitude()
                .chart_connection(previous.x, next.x)
                .expect("area geometry longitude edge was admitted");
            if let ChartStep::Shortest(crate::geographic::EdgeDirection::HalfTurn(direction)) =
                connection.step
            {
                return Err(GeometryErrorKind::parameter(
                    "longitude",
                    crate::geographic::EdgeDirection::HalfTurn(direction).delta(frame.longitude()),
                    "edge spans exactly half the longitude period; add a vertex",
                ));
            }
            chart_turn = chart_turn
                .checked_add(i64::from(connection.source_lift))
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let next_canonical = connection.end;
            let next_x = exact_scaled_x(
                frame,
                next_canonical.get(),
                chart_turn,
                x_origin,
                longitude_scale,
            )?;
            let next_z = exact_scaled_y(frame, next.y, y_origin)?;
            add_exact_edge(
                &previous_x,
                &previous_z,
                &next_x,
                &next_z,
                ring_moments,
                &mut bounds,
            )?;
            previous = next;
            previous_x = next_x;
            previous_z = next_z;
        }
        if chart_turn != 0 {
            let summary = ring_summary(ring, frame);
            debug_assert_eq!(chart_turn, summary.chart_turn);
            let pole = frame.chart_pole_latitude(selected_pole(summary));
            let pole_z = exact_scaled_y(frame, pole, y_origin)?;
            let start_canonical = first_connection.start;
            let start_x =
                exact_scaled_x(frame, start_canonical.get(), 0, x_origin, longitude_scale)?;
            add_exact_edge(
                &previous_x,
                &previous_z,
                &previous_x,
                &pole_z,
                ring_moments,
                &mut bounds,
            )?;
            add_exact_edge(
                &previous_x,
                &pole_z,
                &start_x,
                &pole_z,
                ring_moments,
                &mut bounds,
            )?;
            add_exact_edge(
                &start_x,
                &pole_z,
                &start_x,
                &previous_z,
                ring_moments,
                &mut bounds,
            )?;
        }
        let ring_area = ExactFactorialArea::from_derivatives(coefficients, ring_moments)?;
        let sign = match ring_area.sign() {
            ExactSign::Negative => -1_i64,
            ExactSign::Zero => 0,
            ExactSign::Positive => 1,
        };
        let polygon_sign = if ring_index == 0 {
            sign
        } else {
            sign.wrapping_neg()
        };
        if polygon_sign != 0 {
            for (destination, source) in polygon_moments.iter_mut().zip(ring_moments.iter()) {
                let term = if polygon_sign < 0 {
                    source.clone().neg()
                } else {
                    source.clone()
                };
                *destination = destination.clone().try_add(term)?;
            }
        }
        ring_moments.fill(ExactDyadic::zero());
    }
    Ok((
        ExactFactorialArea::from_derivatives(coefficients, polygon_moments)?,
        bounds,
    ))
}

pub(super) struct ExactSourceBounds {
    pub(super) variation: ExactDyadic,
    pub(super) maximum_latitude: ExactDyadic,
}

impl Default for ExactSourceBounds {
    fn default() -> Self {
        Self {
            variation: ExactDyadic::zero(),
            maximum_latitude: ExactDyadic::zero(),
        }
    }
}

pub(super) fn add_exact_edge(
    x0: &ExactDyadic,
    z0: &ExactDyadic,
    x1: &ExactDyadic,
    z1: &ExactDyadic,
    moments: &mut [ExactDyadic],
    bounds: &mut ExactSourceBounds,
) -> Result<()> {
    // Green-theorem fan integration gives z^k the symmetric divided
    // difference H_k and denominator (k + 1)(k + 2).
    let delta_x = x1.clone().try_subtract(x0.clone())?;
    let delta_z = z1.clone().try_subtract(z0.clone())?;
    let determinant = x0
        .try_product(&delta_z)?
        .try_subtract(z0.try_product(&delta_x)?)?;
    let half_determinant = determinant
        .clone()
        .abs()
        .try_product(&ExactDyadic::from_f64(0.5))?;
    bounds.variation = bounds.variation.clone().try_add(half_determinant)?;
    for latitude in [z0, z1] {
        let latitude = latitude.clone().abs();
        if latitude.cmp(&bounds.maximum_latitude).is_gt() {
            bounds.maximum_latitude = latitude;
        }
    }
    let mut z1_power = ExactDyadic::one();
    let mut h = ExactDyadic::one();
    for moment in moments.iter_mut() {
        *moment = moment.clone().try_add(determinant.try_product(&h)?)?;
        z1_power = z1_power.try_product(z1)?;
        h = z0.try_product(&h)?.try_add(z1_power.clone())?;
    }
    Ok(())
}

pub(super) fn exact_scaled_x(
    frame: GeographicFrame,
    canonical_x: f64,
    turn: i64,
    origin: f64,
    scale: f64,
) -> Result<ExactDyadic> {
    // Preserve canonical source endpoints and ChartStep's integral lift; never
    // rebuild a lifted endpoint from a rounded floating-point edge width.
    let mut lifted = ExactDyadic::from_f64(canonical_x);
    if turn != 0 {
        let offset = ExactDyadic::from_f64(frame.longitude().period())
            .try_product(&ExactDyadic::from_i64(turn))?;
        lifted = lifted.try_add(offset)?;
    }
    lifted
        .try_subtract(ExactDyadic::from_f64(origin))?
        .try_product(&ExactDyadic::from_f64(
            frame.longitude().radians_per_unit() * frame.longitude_orientation_sign(),
        ))?
        .try_product(&ExactDyadic::from_f64(scale))
}

pub(super) fn exact_scaled_y(
    frame: GeographicFrame,
    latitude: f64,
    origin: f64,
) -> Result<ExactDyadic> {
    ExactDyadic::from_f64(latitude)
        .try_subtract(ExactDyadic::from_f64(origin))?
        .try_product(&ExactDyadic::from_f64(
            frame.latitude_radians_per_unit() * frame.latitude_orientation_sign(),
        ))?
        .try_product(&ExactDyadic::from_f64(LATITUDE_SCALE))
}
