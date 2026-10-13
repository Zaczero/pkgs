use super::super::*;

pub(in crate::geometry::topology::q_embedding) fn assigned_port_point(
    frame: GeographicFrame,
    q: &ExactGeographicQ,
    port_id: QPortId,
    nearest_xy: XY,
    candidate: QCandidate,
    pole_port_longitude: Option<f64>,
    pole_port_periods: Option<i64>,
    cut_east: &ExactRational,
    budget: &mut impl Work,
) -> Result<Option<ExactChartPoint>> {
    let port = q
        .ports
        .get(port_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted port is unknown".into()))?;
    if port.cut {
        return Ok(None);
    }
    if !EmbeddingDomain::Geographic(frame).admits(nearest_xy, candidate.native) {
        return Ok(None);
    }
    let longitude = pole_port_longitude.unwrap_or(candidate.native.x);
    let axis = frame.longitude();
    let Ok(candidate_longitude) = axis.canonicalize(longitude) else {
        return Ok(None);
    };
    let Ok(latitude) = Latitude::try_new(frame, candidate.native.y) else {
        return Ok(None);
    };
    let exact_pole = port
        .point
        .y
        .try_cmp_with_work(
            &ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North)),
            budget,
        )?
        .is_eq()
        || port
            .point
            .y
            .try_cmp_with_work(
                &ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South)),
                budget,
            )?
            .is_eq();
    if matches!(latitude.identity(frame), SeamIdentity::Pole) != exact_pole {
        return Ok(None);
    }
    let half = ExactRational::from_f64(axis.half_period());
    let negative_half = ExactRational::zero().try_subtract_with_work(&half, budget)?;
    let period = ExactRational::from_f64(axis.period());
    let exact_canonical = if port.point.x.try_cmp_with_work(&half, budget)?.is_ge() {
        port.point.x.try_subtract_with_work(&period, budget)?
    } else if port
        .point
        .x
        .try_cmp_with_work(&negative_half, budget)?
        .is_lt()
    {
        port.point.x.try_add_with_work(&period, budget)?
    } else {
        port.point.x.try_clone_with_work(budget)?
    };
    let exact_seam = exact_canonical.try_cmp_with_work(&half, budget)?.is_eq()
        || exact_canonical
            .try_cmp_with_work(&negative_half, budget)?
            .is_eq();
    if matches!(candidate_longitude.identity(axis), SeamIdentity::Seam) && !exact_seam {
        return Ok(None);
    }

    let center = ExactRational::from_f64(longitude);
    let periods = pole_port_periods.unwrap_or(candidate.periods);
    let center = center.try_add_with_work(
        &period.try_product_with_work(
            &ExactRational::from_dyadic(ExactDyadic::from_i64(periods)),
            budget,
        )?,
        budget,
    )?;
    let candidates = [
        center.try_subtract_with_work(&period, budget)?,
        center.try_clone_with_work(budget)?,
        center.try_add_with_work(&period, budget)?,
    ];
    let zero = ExactRational::zero();
    let mut nearest: Option<(ExactRational, ExactRational)> = None;
    for candidate in candidates {
        if candidate.try_cmp_with_work(&q.cut, budget)?.is_lt()
            || candidate.try_cmp_with_work(cut_east, budget)?.is_ge()
        {
            continue;
        }
        let delta = candidate.try_subtract_with_work(&port.point.x, budget)?;
        let distance = if delta.try_cmp_with_work(&zero, budget)?.is_lt() {
            zero.try_subtract_with_work(&delta, budget)?
        } else {
            delta
        };
        let replace = if let Some((previous_x, previous_distance)) = &nearest {
            match distance.try_cmp_with_work(previous_distance, budget)? {
                Ordering::Less => true,
                Ordering::Equal => candidate.try_cmp_with_work(previous_x, budget)?.is_lt(),
                Ordering::Greater => false,
            }
        } else {
            true
        };
        if replace {
            nearest = Some((candidate, distance));
        }
    }
    let Some((x, x_distance)) = nearest else {
        return Ok(None);
    };
    let y = ExactRational::from_f64(candidate.native.y);
    let y_delta = y.try_subtract_with_work(&port.point.y, budget)?;
    let y_distance = if y_delta.try_cmp_with_work(&zero, budget)?.is_lt() {
        zero.try_subtract_with_work(&y_delta, budget)?
    } else {
        y_delta
    };
    let one = ExactRational::one();
    if x_distance.try_cmp_with_work(&one, budget)?.is_ge()
        || y_distance.try_cmp_with_work(&one, budget)?.is_ge()
    {
        return Ok(None);
    }
    Ok(Some(ExactChartPoint::from_rationals(x, y)))
}
pub(in crate::geometry::topology::q_embedding) fn q_port_assigned_point(
    topology: &ExactGeographicTopology,
    nearest: &QCoordinates,
    coordinates: &QCoordinates,
    port_id: QPortId,
    budget: &mut impl Work,
) -> Result<ExactChartPoint> {
    q_port_assigned_point_if_representable(topology, nearest, coordinates, port_id, budget)?
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q assigned port has no chart spelling".into()).into()
        })
}
pub(in crate::geometry::topology::q_embedding) fn q_port_assigned_point_if_representable(
    topology: &ExactGeographicTopology,
    nearest: &QCoordinates,
    coordinates: &QCoordinates,
    port_id: QPortId,
    budget: &mut impl Work,
) -> Result<Option<ExactChartPoint>> {
    let q = &topology.q;
    let port = q
        .ports
        .get(port_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q assigned port is unknown".into()))?;
    if port.cut {
        return Ok(Some(port.point.clone()));
    }
    let nearest_xy = nearest.xy(port.node)?;
    let node_index = port.node.0 as usize;
    let candidate = QCandidate {
        native: coordinates.xy(port.node)?,
        periods: *coordinates
            .longitude_periods
            .get(node_index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q assigned node has no longitude lift".into())
            })?,
    };
    let pole_longitude = coordinates
        .pole_port_longitude
        .get(port_id.0 as usize)
        .copied()
        .flatten();
    let pole_periods = coordinates
        .pole_port_periods
        .get(port_id.0 as usize)
        .copied()
        .flatten();
    let assigned = assigned_port_point(
        topology.sources.frame,
        q,
        port_id,
        nearest_xy,
        candidate,
        pole_longitude,
        pole_periods,
        &q.cut.try_add_with_work(
            &ExactRational::from_f64(topology.sources.frame.longitude().period()),
            budget,
        )?,
        budget,
    )?;
    Ok(assigned)
}
pub(in crate::geometry::topology::q_embedding) fn candidate_chart_point_with_work(
    frame: GeographicFrame,
    candidate: QCandidate,
    work: &mut impl Work,
) -> Result<ExactChartPoint> {
    let lift = ExactRational::from_dyadic(ExactDyadic::from_i64(candidate.periods));
    Ok(ExactChartPoint::from_rationals(
        ExactRational::from_f64(candidate.native.x).try_add_with_work(
            &ExactRational::from_f64(frame.longitude().period())
                .try_product_with_work(&lift, work)?,
            work,
        )?,
        ExactRational::from_f64(candidate.native.y),
    ))
}
pub(in crate::geometry::topology::q_embedding) fn assigned_emission_site(
    frame: GeographicFrame,
    candidate: QCandidate,
) -> Result<super::emission::AssignedSite> {
    let longitude = frame
        .longitude()
        .canonicalize(candidate.native.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    Ok(super::emission::AssignedSite {
        longitude: crate::geographic::RetainedLongitudeSite::new(longitude, candidate.periods),
        latitude: candidate.native.y,
    })
}
pub(in crate::geometry::topology::q_embedding) fn native_port_data(
    frame: GeographicFrame,
    node: EmissionNodeId,
    candidate: QCandidate,
    source_chart_point: &ExactChartPoint,
    source_record: Option<Point>,
    occurrences: Vec<OccurrenceSource>,
    bank: Option<super::emission::ChartSide>,
    budget: &mut impl Work,
) -> Result<super::emission::NativePortData> {
    let longitude_axis = frame.longitude();
    let (_, chart_lift) = split_rational_native_site_with_work(
        &source_chart_point.x,
        longitude_axis.period(),
        longitude_axis.half_period(),
        budget,
    )?;
    let lift_delta =
        chart_lift.try_subtract_with_work(ExactDyadic::from_i64(candidate.periods), budget)?;
    let lift_delta = lift_delta.to_f64_exact_scaled(0).ok_or_else(|| {
        GeometryErrorKind::Invalid("native source chart lift exceeds its assigned site".into())
    })?;
    if !lift_delta.is_finite()
        || lift_delta.fract() != 0.0
        || lift_delta < i64::MIN as f64
        || lift_delta >= -(i64::MIN as f64)
    {
        return Err(GeometryErrorKind::Invalid(
            "native source chart lift differs by a non-integral period".into(),
        )
        .into());
    }
    let chart_periods = candidate
        .periods
        .checked_add(lift_delta as i64)
        .ok_or(ResourceError::SizeOverflow)?;
    let (longitude, spelling, source_record, bank) = if let Some(bank) = bank {
        let longitude = match bank {
            super::emission::ChartSide::East => frame.longitude().half_period(),
            super::emission::ChartSide::West => -frame.longitude().half_period(),
        };
        let retained_record =
            source_record.filter(|record| record.x.to_bits() == longitude.to_bits());
        (
            longitude,
            super::emission::PortSpelling::Seam(bank),
            retained_record,
            Some(bank),
        )
    } else if let Some(record) = source_record {
        match quotient_key_for_chart_position_with_work(frame, source_chart_point, budget)? {
            ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole => (
                record.x,
                super::emission::PortSpelling::Pole {
                    native_longitude: record.x,
                },
                Some(record),
                None,
            ),
            ExactQuotientKey::Seam(_) => {
                let side = if chart_periods.rem_euclid(2) == 0 {
                    super::emission::ChartSide::West
                } else {
                    super::emission::ChartSide::East
                };
                let longitude = match side {
                    super::emission::ChartSide::East => frame.longitude().half_period(),
                    super::emission::ChartSide::West => -frame.longitude().half_period(),
                };
                let retained_record = (record.x.to_bits() == longitude.to_bits()).then_some(record);
                (
                    longitude,
                    super::emission::PortSpelling::Seam(side),
                    retained_record,
                    Some(side),
                )
            },
            ExactQuotientKey::Coordinate(_) => (
                record.x,
                super::emission::PortSpelling::Interior {
                    periods: chart_periods,
                },
                Some(record),
                None,
            ),
        }
    } else {
        match quotient_key_for_chart_position_with_work(frame, source_chart_point, budget)? {
            ExactQuotientKey::NorthPole | ExactQuotientKey::SouthPole => {
                let longitude = round_native_longitude_with_work(
                    frame.longitude(),
                    &source_chart_point.x,
                    budget,
                )?;
                (
                    longitude,
                    super::emission::PortSpelling::Pole {
                        native_longitude: longitude,
                    },
                    None,
                    None,
                )
            },
            ExactQuotientKey::Seam(_) => {
                let side = if chart_periods.rem_euclid(2) == 0 {
                    super::emission::ChartSide::West
                } else {
                    super::emission::ChartSide::East
                };
                let longitude = match side {
                    super::emission::ChartSide::East => frame.longitude().half_period(),
                    super::emission::ChartSide::West => -frame.longitude().half_period(),
                };
                (
                    longitude,
                    super::emission::PortSpelling::Seam(side),
                    None,
                    Some(side),
                )
            },
            ExactQuotientKey::Coordinate(_) => (
                candidate.native.x,
                super::emission::PortSpelling::Interior {
                    periods: chart_periods,
                },
                None,
                None,
            ),
        }
    };
    let point = if bank.is_some() {
        ExactChartPoint::from_rationals(
            ExactRational::from_f64(longitude),
            source_chart_point.y.try_clone_with_work(budget)?,
        )
    } else {
        source_chart_point.try_clone_with_work(budget)?
    };
    Ok(super::emission::NativePortData {
        node,
        point,
        spelling,
        source_record,
        occurrences,
    })
}
pub(in crate::geometry::topology::q_embedding) fn align_native_port_to_seam(
    frame: GeographicFrame,
    port: &mut NativePortData,
    seam: &NativePortData,
    budget: &mut impl Work,
) -> Result<()> {
    if !matches!(&seam.spelling, super::emission::PortSpelling::Seam(_)) {
        return Err(GeometryErrorKind::Invalid(
            "native chart alignment requires a seam endpoint".into(),
        )
        .into());
    }
    let super::emission::PortSpelling::Interior { periods } = &port.spelling else {
        return Ok(());
    };
    let old_periods = *periods;
    let longitude =
        nearest_exact_longitude_alias_with_work(frame, &port.point.x, &seam.point.x, budget)?;
    let (_, old_lift) = split_rational_native_site_with_work(
        &port.point.x,
        frame.longitude().period(),
        frame.longitude().half_period(),
        budget,
    )?;
    let (_, new_lift) = split_rational_native_site_with_work(
        &longitude,
        frame.longitude().period(),
        frame.longitude().half_period(),
        budget,
    )?;
    let lift_delta = new_lift.try_subtract_with_work(old_lift, budget)?;
    let lift_delta = lift_delta.to_f64_exact_scaled(0).ok_or_else(|| {
        GeometryErrorKind::Invalid("native chart alignment exceeds the longitude lift".into())
    })?;
    if !lift_delta.is_finite()
        || lift_delta.fract() != 0.0
        || lift_delta < i64::MIN as f64
        || lift_delta >= -(i64::MIN as f64)
    {
        return Err(GeometryErrorKind::Invalid(
            "native chart alignment is not an integral longitude period".into(),
        )
        .into());
    }
    let periods = old_periods
        .checked_add(lift_delta as i64)
        .ok_or(ResourceError::SizeOverflow)?;
    port.point.x = longitude;
    port.spelling = super::emission::PortSpelling::Interior { periods };
    if port.source_record.is_some_and(|record| {
        ExactRational::from_f64(record.x) != port.point.x
            || ExactRational::from_f64(record.y) != port.point.y
    }) {
        port.source_record = None;
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn native_q_port_data(
    topology: &ExactGeographicTopology,
    builder: &super::emission::EmissionBuilder<'_>,
    port_id: QPortId,
    port_points: &[Option<ExactChartPoint>],
    sites: &[Option<AssignedSite>],
    source_requests: &[RootRequest],
    budget: &mut impl Work,
) -> Result<NativePortData> {
    budget.charge_into_error(1)?;
    let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native source-span endpoint port is unknown".into())
    })?;
    let node = builder.source_node(port.node)?;
    let candidate = sites
        .get(node.0 as usize)
        .and_then(Option::as_ref)
        .copied()
        .map(candidate_from_emission_site)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "native source-span endpoint has no assigned emission site".into(),
            )
        })?;
    let chart_point = port_points
        .get(port_id.0 as usize)
        .and_then(Option::as_ref)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "native source-span endpoint has no assigned chart point".into(),
            )
        })?;
    let frame = topology.sources.frame;
    let half = ExactRational::from_f64(frame.longitude().half_period());
    let negative_half = ExactRational::zero().try_subtract_with_work(&half, budget)?;
    let bank = if chart_point.x.try_cmp_with_work(&half, budget)?.is_eq() {
        Some(super::emission::ChartSide::East)
    } else if chart_point
        .x
        .try_cmp_with_work(&negative_half, budget)?
        .is_eq()
    {
        Some(super::emission::ChartSide::West)
    } else {
        None
    };
    let occurrences = if port.cut {
        native_source_sample_occurrences(None, source_requests, budget)?
    } else {
        budget.charge_into_error(1)?;
        vec![OccurrenceSource {
            origin: super::emission::VertexOrigin::SourceNode(port.node),
            source_trace: None,
        }]
    };
    native_port_data(
        frame,
        node,
        candidate,
        chart_point,
        None,
        occurrences,
        bank,
        budget,
    )
}
pub(in crate::geometry::topology::q_embedding) fn exact_squared_displacement(
    frame: GeographicFrame,
    exact: &ExactNodePosition,
    candidate: QCandidate,
    work: &mut impl Work,
) -> Result<ExactRational> {
    let lift = ExactRational::from_dyadic(ExactDyadic::from_i64(candidate.periods));
    let longitude = ExactRational::from_f64(candidate.native.x).try_add_with_work(
        &ExactRational::from_f64(frame.longitude().period()).try_product_with_work(&lift, work)?,
        work,
    )?;
    let dx = longitude.try_subtract_with_work(&exact.longitude, work)?;
    let dy = ExactRational::from_f64(candidate.native.y)
        .try_subtract_with_work(&exact.latitude, work)?;
    dx.try_product_with_work(&dx, work)?
        .try_add_with_work(&dy.try_product_with_work(&dy, work)?, work)
}
pub(in crate::geometry::topology::q_embedding) fn published_candidate_radius(
    frame: GeographicFrame,
    exact: &ExactNodePosition,
    candidates: &[QCandidate],
    budget: &mut impl Work,
) -> Result<f64> {
    let mut radius = 0.0_f64;
    let period = ExactRational::from_f64(frame.longitude().period());
    for candidate in candidates {
        budget.charge_into_error(1)?;
        let lift = ExactRational::from_dyadic(ExactDyadic::from_i64(candidate.periods));
        let longitude = ExactRational::from_f64(candidate.native.x)
            .try_add_with_work(&period.try_product_with_work(&lift, budget)?, budget)?;
        let dx = longitude.try_subtract_with_work(&exact.longitude, budget)?;
        let dy = ExactRational::from_f64(candidate.native.y)
            .try_subtract_with_work(&exact.latitude, budget)?;
        radius = radius
            .max(exact_abs_upper_with_work(&dx, budget)?)
            .max(exact_abs_upper_with_work(&dy, budget)?);
    }
    Ok(radius)
}
