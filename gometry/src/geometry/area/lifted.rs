use super::*;

pub(in crate::geometry) struct LiftedRingArea {
    pub chart_turn: i64,
    pub full_extent: bool,
    pub orientation: Option<i8>,
    pub south: Latitude,
    pub north: Latitude,
}

pub(super) struct RawRingCertificate {
    full_extent: bool,
    south: f64,
    north: f64,
}

pub(super) fn raw_ring_certificate(
    frame: GeographicFrame,
    xs: &[f64],
    ys: &[f64],
) -> Option<RawRingCertificate> {
    let axis = frame.longitude();
    let half = axis.half_period();
    let bound = axis.ordinary_longitude_bound();
    let &first_x = xs.first()?;
    let &first_y = ys.first()?;
    let positive_seam_bits = half.to_bits();
    let negative_seam_bits = (-half).to_bits();
    let mut has_positive_seam = first_x.to_bits() == positive_seam_bits;
    let mut has_negative_seam = first_x.to_bits() == negative_seam_bits;
    let mut south = first_y;
    let mut north = first_y;
    let mut maximum_step = 0.0_f64;
    let mut maximum_ordinary = if has_positive_seam || has_negative_seam {
        0.0
    } else {
        first_x.abs()
    };
    for (&[start, end], &y) in xs.array_windows::<2>().zip(ys.iter().skip(1)) {
        south = south.min(y);
        north = north.max(y);
        maximum_step = maximum_step.max((end - start).abs());
        let positive_seam = end.to_bits() == positive_seam_bits;
        let negative_seam = end.to_bits() == negative_seam_bits;
        has_positive_seam |= positive_seam;
        has_negative_seam |= negative_seam;
        let ordinary = if positive_seam || negative_seam {
            0.0
        } else {
            end.abs()
        };
        maximum_ordinary = maximum_ordinary.max(ordinary);
    }
    // Strict steps keep +H respelling a constant whole-period translation.
    // Half-turn ties can reverse direction; nominal seam aliases are not exact lifts.
    (maximum_ordinary < bound
        && if has_positive_seam {
            maximum_step < half
        } else {
            maximum_step <= half
        }
        && south >= -frame.pole()
        && north <= frame.pole())
    .then_some(RawRingCertificate {
        full_extent: has_positive_seam && has_negative_seam,
        south,
        north,
    })
}

pub(in crate::geometry) fn lifted_ring_area_sign(
    frame: GeographicFrame,
    xs: &[f64],
    ys: &[f64],
    needs_orientation: bool,
) -> Result<LiftedRingArea> {
    let axis = frame.longitude();
    if let Some(raw) = raw_ring_certificate(frame, xs, ys) {
        let orientation = needs_orientation.then(|| {
            match exact_ring_area_sign_columns(xs, ys, xs.len().saturating_sub(1)) {
                AreaSign::Positive => 1,
                AreaSign::Negative => -1,
                AreaSign::Zero => 0,
            }
        });
        return Ok(LiftedRingArea {
            chart_turn: 0,
            full_extent: raw.full_extent,
            orientation,
            south: Latitude::from_admitted(frame, raw.south),
            north: Latitude::from_admitted(frame, raw.north),
        });
    }
    let mut previous_canonical = axis
        .canonicalize(*xs.first().expect("admitted ring has a first longitude"))
        .expect("ring longitude is admitted");
    let first_x = previous_canonical.get();
    let first_y = Latitude::from_admitted(
        frame,
        *ys.first().expect("admitted ring has a first latitude"),
    );
    let mut filter = needs_orientation
        .then(|| CycleSignFilter::new(XY::new(first_x, first_y.get()), xs.len().saturating_sub(1)))
        .flatten();
    let mut turn = 0_i64;
    let mut minimum = (0_i64, first_x);
    let mut maximum = minimum;
    let mut south = first_y;
    let mut north = first_y;
    for (&[start, end], &y) in xs.array_windows::<2>().zip(ys.iter().skip(1)) {
        let canonical = axis.canonicalize(end).expect("ring longitude is admitted");
        if !axis.ordinary_chart_edge(start, end) {
            let transit = axis
                .chart_step(start, end)
                .expect("ring chart step is admitted")
                .transit_from_canonical(previous_canonical, canonical);
            turn = turn
                .checked_add(i64::from(transit))
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        previous_canonical = canonical;
        let x = canonical.get();
        let y = Latitude::from_admitted(frame, y);
        if y.get() < south.get() {
            south = y;
        }
        if y.get() > north.get() {
            north = y;
        }
        let position = (turn, x);
        if lifted_position_cmp(position, minimum).is_lt() {
            minimum = position;
        }
        if lifted_position_cmp(position, maximum).is_gt() {
            maximum = position;
        }
        if let Some(active) = &mut filter {
            if let Some(lifted) = exactly_represented_lift(x, turn, axis.period()) {
                active.push(XY::new(lifted, y.get()));
            } else {
                filter = None;
            }
        }
    }
    let span = maximum.0.abs_diff(minimum.0);
    let full_extent = span > 1 || (span == 1 && maximum.1 >= minimum.1);
    let orientation = if needs_orientation {
        Some(match turn {
            0 => match filter.and_then(CycleSignFilter::finish) {
                Some(AreaSign::Positive) => 1,
                Some(AreaSign::Negative) => -1,
                Some(AreaSign::Zero) => 0,
                None => lifted_exact_ring_area_sign(frame, xs, ys)?,
            },
            -1 | 1 => match FullTurnPole::of_latitude_span(south.get(), north.get()) {
                FullTurnPole::Bounded(ChartPole::North) => {
                    if turn > 0 {
                        1
                    } else {
                        -1
                    }
                },
                FullTurnPole::Bounded(ChartPole::South) => {
                    if turn > 0 {
                        -1
                    } else {
                        1
                    }
                },
                FullTurnPole::Ambiguous => 0,
            },
            _ => 0,
        })
    } else {
        None
    };
    Ok(LiftedRingArea {
        chart_turn: turn,
        full_extent,
        orientation,
        south,
        north,
    })
}

pub(super) fn lifted_position_cmp(left: (i64, f64), right: (i64, f64)) -> std::cmp::Ordering {
    left.0
        .cmp(&right.0)
        .then_with(|| left.1.total_cmp(&right.1))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "only exactly represented turn integers enter the float filter"
)]
pub(super) fn exactly_represented_lift(x: f64, turn: i64, period: f64) -> Option<f64> {
    if turn == 0 {
        return Some(x);
    }
    if turn.unsigned_abs() > (1_u64 << 53) || period.fract() != 0.0 {
        return None;
    }
    let offset = (turn as f64) * period;
    if offset.abs() > (1_u64 << 53) as f64 {
        return None;
    }
    let sum = x + offset;
    let offset_part = sum - x;
    let residual = (x - (sum - offset_part)) + (offset - offset_part);
    (sum.is_finite() && residual == 0.0).then_some(sum)
}

pub(super) fn lifted_exact_ring_area_sign(
    frame: GeographicFrame,
    xs: &[f64],
    ys: &[f64],
) -> Result<i8> {
    let axis = frame.longitude();
    let mut sum = ExactSum::zero();
    let mut turn = 0_i64;
    let mut canonical0 = axis
        .canonicalize(*xs.first().expect("admitted ring has a first longitude"))
        .expect("ring longitude is admitted");
    let mut x0 = canonical0.get();
    let mut y0 = Latitude::from_admitted(
        frame,
        *ys.first().expect("admitted ring has a first latitude"),
    )
    .get();
    for (&[start, end], &end_y) in xs.array_windows::<2>().zip(ys.iter().skip(1)) {
        let canonical1 = axis.canonicalize(end).expect("ring longitude is admitted");
        let transit = if axis.ordinary_chart_edge(start, end) {
            0
        } else {
            axis.chart_step(start, end)
                .expect("ring chart step is admitted")
                .transit_from_canonical(canonical0, canonical1)
        };
        let next_turn = turn
            .checked_add(i64::from(transit))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let x1 = canonical1.get();
        let y1 = Latitude::from_admitted(frame, end_y).get();
        sum.add_cross(x0, y0, x1, y1);
        sum.add_scaled_product(axis.period(), y1, turn, false);
        sum.add_scaled_product(axis.period(), y0, next_turn, true);
        turn = next_turn;
        canonical0 = canonical1;
        x0 = x1;
        y0 = y1;
    }
    Ok(match sum.ordering() {
        std::cmp::Ordering::Greater => 1,
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
    })
}
