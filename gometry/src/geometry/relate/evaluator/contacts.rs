use super::points::witness_point;
use super::*;

pub(super) enum SweepStop<T> {
    Answer(T),
    Error(crate::error::Error),
}

pub(super) fn contact_sweep<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: &[Operand<'_>; 2],
    query: &Q,
    sources: &StagedExactSources,
    components: &mut [Component],
    segment_component: &[u32],
    operand_has_ring: [bool; 2],
    events: &mut Vec<ContactEvent>,
    coordinates: &mut ExactCoordinates,
) -> Result<Option<Q::Output>> {
    let segments = sources.segments();
    let flow = for_each_candidate_pair(
        segments,
        |segment| {
            *segment_component
                .get(segment)
                .expect("each staged segment has a component")
        },
        |left, right| match process_pair::<Q, SATURATE>(
            interval,
            operands,
            query,
            segments,
            components,
            segment_component,
            operand_has_ring,
            [left, right],
            events,
            coordinates,
        ) {
            Ok(Some(answer)) => ControlFlow::Break(SweepStop::Answer(answer)),
            Ok(None) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(SweepStop::Error(error)),
        },
    )?;
    match flow {
        ControlFlow::Continue(()) => Ok(None),
        ControlFlow::Break(SweepStop::Answer(answer)) => Ok(Some(answer)),
        ControlFlow::Break(SweepStop::Error(error)) => Err(error),
    }
}

pub(super) fn process_pair<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: &[Operand<'_>; 2],
    query: &Q,
    segments: &[Segment],
    components: &mut [Component],
    segment_component: &[u32],
    operand_has_ring: [bool; 2],
    [left, right]: [usize; 2],
    events: &mut Vec<ContactEvent>,
    coordinates: &mut ExactCoordinates,
) -> Result<Option<Q::Output>> {
    let (Some(&a), Some(&b)) = (segments.get(left), segments.get(right)) else {
        return Err(GeometryErrorKind::Invalid(
            "candidate pair exceeds the staged segment pool".into(),
        )
        .into());
    };
    let left_component_id = *segment_component
        .get(left)
        .ok_or_else(|| GeometryErrorKind::Invalid("left segment has no component id".into()))?;
    let right_component_id = *segment_component
        .get(right)
        .ok_or_else(|| GeometryErrorKind::Invalid("right segment has no component id".into()))?;
    let left_component =
        usize::try_from(left_component_id).map_err(|_| ResourceError::SizeOverflow)?;
    let right_component =
        usize::try_from(right_component_id).map_err(|_| ResourceError::SizeOverflow)?;
    let (left_side, right_side) = (
        components
            .get(left_component)
            .ok_or_else(|| GeometryErrorKind::Invalid("left component id is out of range".into()))?
            .side,
        components
            .get(right_component)
            .ok_or_else(|| GeometryErrorKind::Invalid("right component id is out of range".into()))?
            .side,
    );
    if !components
        .get(left_component)
        .expect("left component was read above")
        .source
        .has_nonzero_segment
        || !components
            .get(right_component)
            .expect("right component was read above")
            .source
            .has_nonzero_segment
    {
        return Ok(None);
    }
    let same_operand = left_side == right_side;
    if same_operand && !*by_operand(&operand_has_ring, left_side) {
        return Ok(None);
    }

    let contact = segment_contact_exact(a, b);
    let Some(event) = make_event(
        contact,
        a,
        b,
        left,
        right,
        left_component_id,
        right_component_id,
        coordinates,
    )?
    else {
        return Ok(None);
    };

    if same_operand
        && !benign_self_touch(
            contact,
            left,
            right,
            &components
                .get(left_component)
                .expect("left component admitted above")
                .source,
            &components
                .get(right_component)
                .expect("right component admitted above")
                .source,
        )
    {
        if left_component == right_component {
            components
                .get_mut(left_component)
                .expect("left component admitted above")
                .dirty = true;
        } else {
            components
                .get_mut(left_component)
                .expect("left component admitted above")
                .dirty = true;
            components
                .get_mut(right_component)
                .expect("right component admitted above")
                .dirty = true;
        }
    }

    events.push(event);
    if same_operand {
        return Ok(None);
    }

    let event = events
        .last_mut()
        .expect("the current pair event was appended");
    match event.kind {
        // Collinear spans are labeled once in the atom pass, after all
        // same-operand contacts have established component cleanliness.
        EventKind::Touch | EventKind::Collinear { .. } => {},
        EventKind::Cross => {
            let Some(point) = event.points.first_mut().and_then(Option::as_mut) else {
                return Err(GeometryErrorKind::Invalid(
                    "crossing event is missing its point".into(),
                )
                .into());
            };
            let point = event_point_exact(point, segments, coordinates)?;
            let probe = crate::geometry::source_support::ExactProbe::new(point);
            witness_point(
                interval,
                operands[0].locate_probe(&probe)?,
                operands[1].locate_probe(&probe)?,
                query.needs_dimensions(),
            );
            if let Some(answer) = query.verdict(interval) {
                return Ok(Some(answer));
            }
            if !operands[0].row.has_area_parts() {
                for direction in segment_directions(a, coordinates)? {
                    match operands[1].occupied_toward_probe(&probe, &direction)? {
                        OccupiedToward::Yes => {
                            interval.witness(Loc::Interior, Loc::Interior, Dimension::Curve);
                        },
                        OccupiedToward::No => {
                            interval.witness(Loc::Interior, Loc::Exterior, Dimension::Curve);
                        },
                        OccupiedToward::Tangent => {},
                    }
                    if let Some(answer) = query.verdict(interval) {
                        return Ok(Some(answer));
                    }
                }
            }
            if !operands[1].row.has_area_parts() {
                for direction in segment_directions(b, coordinates)? {
                    match operands[0].occupied_toward_probe(&probe, &direction)? {
                        OccupiedToward::Yes => {
                            interval.witness(Loc::Interior, Loc::Interior, Dimension::Curve);
                        },
                        OccupiedToward::No => {
                            interval.witness(Loc::Exterior, Loc::Interior, Dimension::Curve);
                        },
                        OccupiedToward::Tangent => {},
                    }
                    if let Some(answer) = query.verdict(interval) {
                        return Ok(Some(answer));
                    }
                }
            }
            if operands[0].row.has_area_parts() && operands[1].row.has_area_parts() {
                for direction in crossing_quadrants(a, b, coordinates)? {
                    let left = operands[0].occupied_toward_probe(&probe, &direction)?;
                    let right = operands[1].occupied_toward_probe(&probe, &direction)?;
                    let (left, right) = match (left, right) {
                        (OccupiedToward::Yes, OccupiedToward::Yes) => {
                            (Loc::Interior, Loc::Interior)
                        },
                        (OccupiedToward::Yes, OccupiedToward::No) => (Loc::Interior, Loc::Exterior),
                        (OccupiedToward::No, OccupiedToward::Yes) => (Loc::Exterior, Loc::Interior),
                        (OccupiedToward::No, OccupiedToward::No) => (Loc::Exterior, Loc::Exterior),
                        _ => continue,
                    };
                    interval.witness(left, right, Dimension::Surface);
                    if let Some(answer) = query.verdict(interval) {
                        return Ok(Some(answer));
                    }
                }
            }
        },
    }
    if SATURATE && interval.settled_for(query.cells()) {
        return Ok(query.verdict(interval));
    }
    Ok(None)
}

pub(super) fn make_event(
    contact: Contact,
    left: Segment,
    right: Segment,
    left_index: usize,
    right_index: usize,
    left_component: u32,
    right_component: u32,
    coordinates: &mut ExactCoordinates,
) -> Result<Option<ContactEvent>> {
    let (kind, points) = match contact {
        Contact::Disjoint => return Ok(None),
        Contact::Touch(location) => (EventKind::Touch, [
            Some(EventPoint::stored(location.point(left, right))),
            None,
        ]),
        Contact::Cross => {
            let segments = [
                u32::try_from(left_index).map_err(|_| ResourceError::SizeOverflow)?,
                u32::try_from(right_index).map_err(|_| ResourceError::SizeOverflow)?,
            ];
            let crossing = crossing_point_with(left, right, || coordinates.crossing(left, right))?;
            (EventKind::Cross, [
                Some(EventPoint::crossing(
                    segments,
                    crossing.point,
                    crossing.exact,
                )),
                None,
            ])
        },
        Contact::Collinear => {
            let span = contact
                .shared_span(left, right)
                .expect("collinear carriers with overlapping envelopes have a shared span");
            (
                EventKind::Collinear {
                    same_direction: span.same_direction,
                },
                [
                    Some(EventPoint::stored(span.start)),
                    Some(EventPoint::stored(span.end)),
                ],
            )
        },
    };
    Ok(Some(ContactEvent {
        segments: [left_index, right_index],
        components: [left_component, right_component],
        kind,
        points,
    }))
}

pub(super) fn benign_self_touch(
    contact: Contact,
    left: usize,
    right: usize,
    left_component: &StagedSourceComponent,
    right_component: &StagedSourceComponent,
) -> bool {
    if left_component.segments != right_component.segments {
        return false;
    }
    if !matches!(contact, Contact::Touch(TouchLocation::Shared(_))) {
        return false;
    }
    right == left.wrapping_add(1)
        || (left_component.ring.is_some()
            && left == left_component.segments.start
            && right.wrapping_add(1) == left_component.segments.end)
}

impl EventPoint {
    const fn stored(point: XY) -> Self {
        Self {
            kind: EventPointKind::Stored(point),
            exact: None,
            handle: None,
        }
    }

    pub(super) fn crossing(
        segments: [u32; 2],
        rounded: XY,
        exact: Option<[ExactDyadic; 3]>,
    ) -> Self {
        Self {
            kind: EventPointKind::Crossing { segments, rounded },
            exact: exact.map(Box::new),
            handle: None,
        }
    }
}

pub(super) fn event_point_exact<'a>(
    point: &'a mut EventPoint,
    segments: &[Segment],
    coordinates: &mut ExactCoordinates,
) -> Result<&'a [ExactDyadic; 3]> {
    if point.exact.is_none() {
        let exact = match point.kind {
            EventPointKind::Stored(point) => coordinates.stored(point).clone(),
            EventPointKind::Crossing {
                segments: [left, right],
                ..
            } => {
                let left = usize::try_from(left).map_err(|_| ResourceError::SizeOverflow)?;
                let right = usize::try_from(right).map_err(|_| ResourceError::SizeOverflow)?;
                let left = *segments.get(left).ok_or_else(|| {
                    GeometryErrorKind::Invalid("crossing references a missing left segment".into())
                })?;
                let right = *segments.get(right).ok_or_else(|| {
                    GeometryErrorKind::Invalid("crossing references a missing right segment".into())
                })?;
                coordinates.crossing(left, right)?
            },
        };
        point.exact = Some(Box::new(exact));
    }
    Ok(point
        .exact
        .as_deref()
        .expect("the event exact coordinate was initialized"))
}

pub(super) fn stored_exact_point(point: XY) -> [ExactDyadic; 3] {
    [
        ExactDyadic::from_f64(point.x),
        ExactDyadic::from_f64(point.y),
        ExactDyadic::one(),
    ]
}

pub(super) fn segment_directions(
    segment: Segment,
    coordinates: &mut ExactCoordinates,
) -> Result<[[ExactDyadic; 2]; 2]> {
    let [x, y] = coordinates.direction(segment)?;
    Ok([[x.clone(), y.clone()], [x.neg(), y.neg()]])
}

pub(super) fn crossing_quadrants(
    left: Segment,
    right: Segment,
    coordinates: &mut ExactCoordinates,
) -> Result<[[ExactDyadic; 2]; 4]> {
    let [u, _] = segment_directions(left, coordinates)?;
    let [v, _] = segment_directions(right, coordinates)?;
    let combine = |u: &[ExactDyadic; 2],
                   v: &[ExactDyadic; 2],
                   u_sign: bool,
                   v_sign: bool|
     -> Result<[ExactDyadic; 2]> {
        let ux = if u_sign {
            u[0].clone()
        } else {
            u[0].clone().neg()
        };
        let uy = if u_sign {
            u[1].clone()
        } else {
            u[1].clone().neg()
        };
        let vx = if v_sign {
            v[0].clone()
        } else {
            v[0].clone().neg()
        };
        let vy = if v_sign {
            v[1].clone()
        } else {
            v[1].clone().neg()
        };
        Ok([ux.try_add(vx)?, uy.try_add(vy)?])
    };
    Ok([
        combine(&u, &v, true, true)?,
        combine(&u, &v, true, false)?,
        combine(&u, &v, false, true)?,
        combine(&u, &v, false, false)?,
    ])
}
