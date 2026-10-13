use super::*;

pub(super) struct FastArea {
    pub(super) integral: Bounded,
    pub(super) source_error: f64,
    pub(super) dot_error: f64,
    pub(super) contraction_budget: f64,
    pub(super) variation: f64,
    pub(super) maximum_latitude: f64,
    pub(super) coefficient_error: f64,
    pub(super) sign_uncertain: bool,
}

pub(super) fn accumulate_fast(
    polygon: &Polygon,
    frame: GeographicFrame,
    x_origin: f64,
    y_origin: f64,
    longitude_coordinate_scale: Bounded,
    latitude_coordinate_scale: Bounded,
    coefficients: &[AreaCoefficient],
    ring_moments: &mut [BoundedSum],
    polygon_moments: &mut [BoundedSum],
) -> Result<FastArea> {
    for moment in ring_moments.iter_mut().chain(polygon_moments.iter_mut()) {
        *moment = BoundedSum::default();
    }
    let mut variation = 0.0;
    let mut maximum_latitude = 0.0_f64;
    let mut sign_uncertain = false;
    for (ring_index, ring) in polygon.rings().enumerate() {
        let mut row_certificate = RowCertificate::default();
        if !rectangle_moments(
            ring,
            frame,
            y_origin,
            longitude_coordinate_scale,
            latitude_coordinate_scale,
            ring_moments,
            &mut variation,
            &mut maximum_latitude,
        ) {
            let mut chart_turn = 0_i64;
            let mut previous = ring.nth_coord(0);
            let first_connection = frame
                .longitude()
                .chart_connection(previous.x, ring.nth_coord(1).x)
                .expect("area geometry longitude edge was admitted");
            let previous_canonical = first_connection.start;
            let mut previous_x = scaled_x(
                frame,
                previous_canonical.get(),
                chart_turn,
                x_origin,
                longitude_coordinate_scale,
            );
            let mut previous_z = scaled_y(previous.y, y_origin, latitude_coordinate_scale);
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
                        crate::geographic::EdgeDirection::HalfTurn(direction)
                            .delta(frame.longitude()),
                        "edge spans exactly half the longitude period; add a vertex",
                    ));
                }
                chart_turn = chart_turn
                    .checked_add(i64::from(connection.source_lift))
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                let next_canonical = connection.end;
                let next_x = scaled_x(
                    frame,
                    next_canonical.get(),
                    chart_turn,
                    x_origin,
                    longitude_coordinate_scale,
                );
                let next_z = scaled_y(next.y, y_origin, latitude_coordinate_scale);
                let delta_x = scaled_x(
                    frame,
                    connection.end.get(),
                    i64::from(connection.source_lift),
                    connection.start.get(),
                    longitude_coordinate_scale,
                );
                let delta_z = scaled_y(next.y, previous.y, latitude_coordinate_scale);
                add_fast_determinant(
                    previous_x.mul(delta_z).sub(previous_z.mul(delta_x)),
                    previous_z,
                    next_z,
                    ring_moments,
                    &mut variation,
                    &mut maximum_latitude,
                    &mut row_certificate,
                )?;
                previous = next;
                previous_x = next_x;
                previous_z = next_z;
            }
            if chart_turn != 0 {
                let summary = ring_summary(ring, frame);
                debug_assert_eq!(chart_turn, summary.chart_turn);
                // Close the integration chain at the selected pole; these edges are
                // not emitted or added to the stored ring.
                let pole = frame.chart_pole_latitude(selected_pole(summary));
                let pole_z = scaled_y(pole, y_origin, latitude_coordinate_scale);
                let start_canonical = first_connection.start;
                let start_x = scaled_x(
                    frame,
                    start_canonical.get(),
                    0,
                    x_origin,
                    longitude_coordinate_scale,
                );
                add_fast_edge(
                    previous_x,
                    previous_z,
                    previous_x,
                    pole_z,
                    ring_moments,
                    &mut variation,
                    &mut maximum_latitude,
                    &mut row_certificate,
                )?;
                add_fast_edge(
                    previous_x,
                    pole_z,
                    start_x,
                    pole_z,
                    ring_moments,
                    &mut variation,
                    &mut maximum_latitude,
                    &mut row_certificate,
                )?;
                add_fast_edge(
                    start_x,
                    pole_z,
                    start_x,
                    previous_z,
                    ring_moments,
                    &mut variation,
                    &mut maximum_latitude,
                    &mut row_certificate,
                )?;
            }
        }
        row_certificate.charge(ring_moments);
        let sign = ring_sign_fast(ring_moments, coefficients).unwrap_or_else(|| {
            sign_uncertain = true;
            if contract_fast(ring_moments, coefficients)
                .0
                .value
                .is_sign_negative()
            {
                -1.0
            } else {
                1.0
            }
        });
        if sign == 0.0 {
            for moment in ring_moments.iter_mut() {
                *moment = BoundedSum::default();
            }
            continue;
        }
        let polarity = if ring_index == 0 { 1.0 } else { -1.0 };
        for (polygon_moment, ring_moment) in polygon_moments.iter_mut().zip(ring_moments.iter()) {
            let value = Bounded {
                value: ring_moment.value() * (sign * polarity),
                error: ring_moment.error(),
            };
            polygon_moment.push(value);
        }
        for moment in ring_moments.iter_mut() {
            *moment = BoundedSum::default();
        }
    }
    let (integral, source_error, dot_error) = contract_fast(polygon_moments, coefficients);
    let coefficient_error = coefficient_dot_error(polygon_moments, coefficients);
    let contraction_budget = contraction_work_budget(polygon_moments, coefficients);
    Ok(FastArea {
        integral,
        source_error,
        dot_error,
        contraction_budget,
        variation,
        maximum_latitude,
        coefficient_error,
        sign_uncertain,
    })
}

pub(super) fn rectangle_moments(
    ring: &Ring,
    frame: GeographicFrame,
    y_origin: f64,
    longitude_scale: Bounded,
    latitude_scale: Bounded,
    moments: &mut [BoundedSum],
    variation: &mut f64,
    maximum_latitude: &mut f64,
) -> bool {
    if ring.coord_count() != 5 {
        return false;
    }
    let first = ring.nth_coord(0);
    let second = ring.nth_coord(1);
    let third = ring.nth_coord(2);
    let fourth = ring.nth_coord(3);
    let closing = ring.nth_coord(4);
    #[expect(
        clippy::float_cmp,
        reason = "rectangle closure compares stored coordinates exactly; signed zero remains equal"
    )]
    let closure_differs = first.x != closing.x || first.y != closing.y;
    if closure_differs {
        return false;
    }
    #[expect(
        clippy::float_cmp,
        reason = "axis-aligned rectangle recognition requires exact native-coordinate equality"
    )]
    let (left, right, reverse_left, reverse_right, top, polarity, verticals) =
        if first.y == second.y && second.x == third.x && third.y == fourth.y && fourth.x == first.x
        {
            (first.x, second.x, third.x, fourth.x, third.y, 1.0, [
                (second.x, third.x),
                (fourth.x, closing.x),
            ])
        } else if first.x == second.x
            && second.y == third.y
            && third.x == fourth.x
            && fourth.y == first.y
        {
            (second.x, third.x, fourth.x, first.x, second.y, -1.0, [
                (first.x, second.x),
                (third.x, fourth.x),
            ])
        } else {
            return false;
        };
    let axis = frame.longitude();
    let forward = axis
        .chart_connection(left, right)
        .expect("rectangle longitude source was admitted");
    let reverse = axis
        .chart_connection(reverse_left, reverse_right)
        .expect("rectangle longitude source was admitted");
    for (from, to) in verticals {
        let connection = axis
            .chart_connection(from, to)
            .expect("rectangle longitude source was admitted");
        if connection.start != connection.end || connection.source_lift != 0 {
            return false;
        }
    }
    if forward.start != reverse.end
        || forward.end != reverse.start
        || i64::from(forward.source_lift) != i64::from(reverse.source_lift).wrapping_neg()
        || matches!(
            forward.step,
            ChartStep::Shortest(crate::geographic::EdgeDirection::HalfTurn(_))
        )
    {
        return false;
    }

    let width = scaled_x(
        frame,
        forward.end.get(),
        i64::from(forward.source_lift),
        forward.start.get(),
        longitude_scale,
    );
    let height = scaled_y(top, first.y, latitude_scale);
    let determinant = width.mul(height).mul(Bounded::exact(polarity));
    let z0 = scaled_y(first.y, y_origin, latitude_scale);
    let z1 = scaled_y(top, y_origin, latitude_scale);
    let radius = upper_add(z0.value.abs(), z0.error).max(upper_add(z1.value.abs(), z1.error));
    if !radius.is_finite() || radius > 0.5 {
        return false;
    }

    *variation = upper_add(
        *variation,
        upper_add(determinant.value.abs(), determinant.error),
    );
    *maximum_latitude = (*maximum_latitude).max(radius);
    let input_error = upper_add(z0.error, z1.error);
    let mut symmetric_power = 1.0;
    let mut z1_power = z1.value;
    let mut radius_power = 1.0;
    let mut previous_radius_power = 1.0;
    let count = moments.len();
    for (degree, moment) in moments.iter_mut().enumerate() {
        let degree_value = degree as f64;
        let factor = degree_value + 2.0;
        let operation_count = 2.0 * degree_value + 2.0;
        let gamma = (operation_count / (9_007_199_254_740_992.0 - operation_count)).next_up();
        let absolute_h = upper_mul(degree_value + 1.0, radius_power);
        let sensitivity = if degree == 0 {
            0.0
        } else {
            upper_mul(
                degree_value * (degree_value + 1.0) * 0.5,
                previous_radius_power,
            )
        };
        let error = upper_add(
            upper_mul(
                factor,
                upper_add(
                    upper_mul(
                        upper_add(determinant.error, upper_mul(gamma, determinant.value.abs())),
                        absolute_h,
                    ),
                    upper_add(
                        upper_mul(determinant.value.abs(), upper_mul(sensitivity, input_error)),
                        upper_mul(
                            MIN_SUBNORMAL,
                            upper_add(1.0, upper_mul(4.0 * degree_value, determinant.value.abs())),
                        ),
                    ),
                ),
            ),
            MIN_SUBNORMAL,
        );
        moment.push(Bounded {
            value: determinant.value * symmetric_power * factor,
            error,
        });
        if degree.wrapping_add(1) < count {
            symmetric_power = z0.value * symmetric_power + z1_power;
            z1_power *= z1.value;
            previous_radius_power = radius_power;
            radius_power = upper_mul(radius_power, radius);
        }
    }
    true
}

pub(super) fn add_fast_edge(
    x0: Bounded,
    z0: Bounded,
    x1: Bounded,
    z1: Bounded,
    moments: &mut [BoundedSum],
    variation: &mut f64,
    maximum_latitude: &mut f64,
    row_certificate: &mut RowCertificate,
) -> Result<()> {
    let determinant = cross(x0, z0, x1, z1);
    add_fast_determinant(
        determinant,
        z0,
        z1,
        moments,
        variation,
        maximum_latitude,
        row_certificate,
    )
}

#[derive(Default)]
pub(super) struct RowCertificate {
    determinant_magnitude: f64,
    determinant_error: f64,
    weighted_latitude_error: f64,
    radius: f64,
    rows: usize,
}

impl RowCertificate {
    pub(super) fn charge(&self, moments: &mut [BoundedSum]) {
        if self.rows == 0 {
            return;
        }
        let mut radius_power = 1.0;
        let mut previous_radius_power = 1.0;
        for (degree, moment) in moments.iter_mut().enumerate() {
            let k = degree as f64;
            let n = 2.0 * k + 1.0;
            let gamma = (n / (9_007_199_254_740_992.0 - n)).next_up();
            let absolute_h = upper_mul(k + 1.0, radius_power);
            let sensitivity = if degree == 0 {
                0.0
            } else {
                upper_mul(k * (k + 1.0) * 0.5, previous_radius_power)
            };
            let error = upper_add(
                upper_mul(
                    upper_add(
                        self.determinant_error,
                        upper_mul(gamma, self.determinant_magnitude),
                    ),
                    absolute_h,
                ),
                upper_add(
                    upper_mul(sensitivity, self.weighted_latitude_error),
                    upper_mul(
                        MIN_SUBNORMAL,
                        upper_add(
                            (self.rows as f64).next_up(),
                            upper_mul(4.0 * k, self.determinant_magnitude),
                        ),
                    ),
                ),
            );
            moment.input_error = upper_add(moment.input_error, error);
            previous_radius_power = radius_power;
            radius_power = upper_mul(radius_power, self.radius);
        }
    }
}

pub(super) fn add_fast_determinant(
    determinant: Bounded,
    z0: Bounded,
    z1: Bounded,
    moments: &mut [BoundedSum],
    variation: &mut f64,
    maximum_latitude: &mut f64,
    row_certificate: &mut RowCertificate,
) -> Result<()> {
    *variation = upper_add(
        *variation,
        upper_mul(upper_add(determinant.value.abs(), determinant.error), 0.5),
    );
    let radius = upper_add(z0.value.abs(), z0.error).max(upper_add(z1.value.abs(), z1.error));
    *maximum_latitude = (*maximum_latitude).max(radius);
    if !radius.is_finite() || radius > 0.5 {
        for moment in moments {
            moment.push(Bounded {
                value: f64::NAN,
                error: f64::INFINITY,
            });
        }
        return Ok(());
    }
    let input_error = upper_add(z0.error, z1.error);
    row_certificate.determinant_magnitude = upper_add(
        row_certificate.determinant_magnitude,
        determinant.value.abs(),
    );
    row_certificate.determinant_error =
        upper_add(row_certificate.determinant_error, determinant.error);
    row_certificate.weighted_latitude_error = upper_add(
        row_certificate.weighted_latitude_error,
        upper_mul(determinant.value.abs(), input_error),
    );
    row_certificate.radius = row_certificate.radius.max(radius);
    row_certificate.rows = row_certificate
        .rows
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let moment_count = moments.len();
    let mut h = 1.0;
    let mut z1_power = z1.value;
    for (degree, moment) in moments.iter_mut().enumerate() {
        moment.push(Bounded::exact(determinant.value * h));
        if degree.wrapping_add(1) < moment_count {
            h = z0.value * h + z1_power;
            z1_power *= z1.value;
        }
    }
    Ok(())
}

pub(super) fn cross(x0: Bounded, z0: Bounded, x1: Bounded, z1: Bounded) -> Bounded {
    x0.mul(z1).sub(x1.mul(z0))
}

pub(super) fn ring_sign_fast(
    moments: &[BoundedSum],
    coefficients: &[AreaCoefficient],
) -> Option<f64> {
    let (area, source_error, dot_error) = contract_fast(moments, coefficients);
    let error = upper_add(
        upper_add(source_error, dot_error),
        coefficient_dot_error(moments, coefficients),
    );
    if !area.value.is_finite() || !error.is_finite() || area.value.abs() <= error {
        None
    } else {
        Some(area.value.signum())
    }
}

pub(super) fn coefficient_dot_error(
    moments: &[BoundedSum],
    coefficients: &[AreaCoefficient],
) -> f64 {
    let mut error = 0.0;
    for (degree, (moment, coefficient)) in moments.iter().zip(coefficients).enumerate() {
        let denominator =
            (degree.wrapping_add(1) as u64).saturating_mul(degree.wrapping_add(2) as u64);
        error = upper_add(
            error,
            upper_div_u64(
                upper_mul(
                    coefficient.error,
                    upper_add(moment.value().abs(), moment.error()),
                ),
                denominator,
            ),
        );
    }
    error
}

pub(super) fn contract_fast(
    moments: &[BoundedSum],
    coefficients: &[AreaCoefficient],
) -> (Bounded, f64, f64) {
    let mut sum = BoundedSum::default();
    let mut source_error = 0.0;
    for (degree, (moment, coefficient)) in moments.iter().zip(coefficients).enumerate() {
        let denominator =
            (degree.wrapping_add(1) as u64).saturating_mul(degree.wrapping_add(2) as u64);
        let moment_value = moment.value();
        let moment_error = moment.error();
        let term = Bounded::exact(coefficient.value)
            .mul(Bounded::exact(moment_value))
            .div_u64(denominator);
        sum.push(term);
        source_error = upper_add(
            source_error,
            upper_div_u64(
                upper_mul(coefficient.value.abs(), moment_error),
                denominator,
            ),
        );
    }
    (sum.bounded(), source_error, sum.error())
}

pub(super) fn contraction_work_budget(
    moments: &[BoundedSum],
    coefficients: &[AreaCoefficient],
) -> f64 {
    let mut norm = 0.0;
    for (degree, (moment, coefficient)) in moments.iter().zip(coefficients).enumerate() {
        let denominator =
            (degree.wrapping_add(1) as u64).saturating_mul(degree.wrapping_add(2) as u64);
        norm = upper_add(
            norm,
            upper_div_u64(
                upper_mul(
                    upper_add(coefficient.value.abs(), coefficient.error),
                    upper_add(moment.value().abs(), moment.error()),
                ),
                denominator,
            ),
        );
    }
    norm
}
