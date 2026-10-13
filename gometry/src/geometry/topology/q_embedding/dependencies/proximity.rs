use super::super::*;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) struct QProximityPair {
    pub(in crate::geometry::topology::q_embedding) left: usize,
    pub(in crate::geometry::topology::q_embedding) right: usize,
    pub(in crate::geometry::topology::q_embedding) right_shift: i8,
}
pub(in crate::geometry::topology::q_embedding) fn rounded_chart_point(
    point: &ExactChartPoint,
) -> Result<XY> {
    let mut work = Unmetered;
    rounded_chart_point_with_work(point, &mut work)
}
pub(in crate::geometry::topology::q_embedding) fn rounded_chart_point_with_work(
    point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<XY> {
    Ok(XY::new(
        point.x.round_with_work(work)?,
        point.y.round_with_work(work)?,
    ))
}
pub(in crate::geometry::topology::q_embedding) fn shifted_segment(
    segment: Segment,
    shift: i8,
    period: f64,
) -> Segment {
    let offset = f64::from(shift) * period;
    Segment {
        start: XY::new(segment.start.x + offset, segment.start.y),
        end: XY::new(segment.end.x + offset, segment.end.y),
    }
}
pub(in crate::geometry::topology::q_embedding) fn segment_translation_error(
    segment: Segment,
    shift: i8,
    period: f64,
    work: &mut impl Work,
) -> Result<f64> {
    if shift == 0 {
        return Ok(0.0);
    }
    let exact_period = ExactRational::from_f64(period);
    let offset =
        exact_period.try_product_with_work(&ExactRational::from_f64(f64::from(shift)), work)?;
    let translated = shifted_segment(segment, shift, period);
    let mut error = 0.0_f64;
    for (source, image) in [
        (segment.start.x, translated.start.x),
        (segment.end.x, translated.end.x),
    ] {
        let exact = ExactRational::from_f64(source).try_add_with_work(&offset, work)?;
        error = error.max(exact_abs_upper_with_work(
            &ExactRational::from_f64(image).try_subtract_with_work(&exact, work)?,
            work,
        )?);
    }
    Ok(error)
}
pub(in crate::geometry::topology::q_embedding) fn expanded_radius(
    radius: f64,
    additional: f64,
) -> f64 {
    let combined = radius + additional;
    if combined.is_finite() && additional > 0.0 {
        combined.next_up()
    } else {
        combined
    }
}
pub(in crate::geometry::topology::q_embedding) fn chord_boxes(
    segment: Segment,
    radius: f64,
    period: f64,
    work: &mut impl Work,
) -> Result<([Bounds; 3], [f64; 3])> {
    let image_radii = [
        expanded_radius(
            radius,
            segment_translation_error(segment, -1, period, work)?,
        ),
        radius,
        expanded_radius(radius, segment_translation_error(segment, 1, period, work)?),
    ];
    Ok((
        [
            swept_bounds(shifted_segment(segment, -1, period), image_radii[0]),
            swept_bounds(segment, radius),
            swept_bounds(shifted_segment(segment, 1, period), image_radii[2]),
        ],
        image_radii,
    ))
}
pub(in crate::geometry::topology::q_embedding) fn chord_dependencies(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    nodes: [QNodeId; 2],
    ports: [QPortId; 2],
    budget: &mut impl Work,
) -> Result<Vec<QNodeId>> {
    budget.charge_into_error(2)?;
    let mut dependencies = nodes.to_vec();
    for port_id in ports {
        budget.charge_into_error(1)?;
        let Some(port) = topology.q.ports.get(port_id.0 as usize) else {
            continue;
        };
        if !port.cut {
            continue;
        }
        let mut low = 0_usize;
        let mut high = emission.cut_nodes.len();
        while low < high {
            budget.charge_into_error(1)?;
            let middle = low
                .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                .ok_or(ResourceError::SizeOverflow)?;
            let cut = emission.cut_nodes.get(middle).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut dependency search index is unknown".into())
            })?;
            match cut.node.cmp(&port.node) {
                Ordering::Less => {
                    low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                },
                Ordering::Greater => high = middle,
                Ordering::Equal => {
                    for chord in &cut.chords {
                        budget.charge_into_error(1)?;
                        for far_port in [chord.west_far_port, chord.east_far_port] {
                            budget.charge_into_error(1)?;
                            if let Some(far) = topology.q.ports.get(far_port.0 as usize) {
                                dependencies.push(far.node);
                            }
                        }
                    }
                    break;
                },
            }
        }
    }
    try_sort_by_with_work(&mut dependencies, budget, |left, right, _work| {
        Ok(left.cmp(right))
    })?;
    budget.charge_into_error(
        u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    dependencies.dedup();
    Ok(dependencies)
}
pub(in crate::geometry::topology::q_embedding) fn cut_intersection_y(
    q: &ExactGeographicQ,
    period: &ExactRational,
    west_point: &ExactChartPoint,
    east_point: &ExactChartPoint,
    work: &mut impl Work,
) -> Result<Option<ExactRational>> {
    let east_x = east_point.x.try_subtract_with_work(period, work)?;
    if !east_x.try_cmp_with_work(&q.cut, work)?.is_lt()
        || !west_point.x.try_cmp_with_work(&q.cut, work)?.is_gt()
    {
        return Ok(None);
    }
    let width = west_point.x.try_subtract_with_work(&east_x, work)?;
    let parameter = q
        .cut
        .try_subtract_with_work(&east_x, work)?
        .try_quotient_with_work(&width, work)?;
    if !super::super::super::exact::in_unit_interval_with_work(&parameter, work)? {
        return Ok(None);
    }
    Ok(Some(east_point.y.try_add_with_work(
        &parameter.try_product_with_work(
            &west_point.y.try_subtract_with_work(&east_point.y, work)?,
            work,
        )?,
        work,
    )?))
}
pub(in crate::geometry::topology::q_embedding) fn exact_abs_upper_with_work(
    value: &ExactRational,
    work: &mut impl Work,
) -> Result<f64> {
    let (low, high) = value.outward_bounds_with_work(work)?;
    Ok(low.abs().max(high.abs()))
}
pub(in crate::geometry::topology::q_embedding) fn port_swept_radius(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    nearest: &QCoordinates,
    domains: &[Vec<QCandidate>],
    port_id: QPortId,
    budget: &mut impl Work,
) -> Result<f64> {
    let frame = topology.sources.frame;
    let q = &topology.q;
    let port = q
        .ports
        .get(port_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q swept port is unknown".into()))?;
    let period = ExactRational::from_f64(frame.longitude().period());
    let cut_east = q.cut.try_add_with_work(&period, budget)?;
    let base = rounded_chart_point_with_work(&port.point, budget)?;
    let base_x = ExactRational::from_f64(base.x);
    let base_y = ExactRational::from_f64(base.y);
    let mut radius = 0.0_f64;
    if port.cut {
        let mut low = 0_usize;
        let mut high = emission.cut_nodes.len();
        let mut cut = None;
        while low < high {
            budget.charge_into_error(1)?;
            let middle = low
                .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                .ok_or(ResourceError::SizeOverflow)?;
            let candidate = emission.cut_nodes.get(middle).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut radius index is unknown".into())
            })?;
            match candidate.node.cmp(&port.node) {
                Ordering::Less => {
                    low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                },
                Ordering::Greater => high = middle,
                Ordering::Equal => {
                    cut = Some(candidate);
                    break;
                },
            }
        }
        let Some(cut) = cut else {
            return Ok(f64::INFINITY);
        };
        radius = exact_abs_upper_with_work(
            &port.point.x.try_subtract_with_work(&base_x, budget)?,
            budget,
        )?;
        let mut had_candidate_cut = false;
        for chord in &cut.chords {
            budget.charge_into_error(1)?;
            let west_port = q.ports.get(chord.west_far_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q west cut endpoint is unknown".into())
            })?;
            let east_port = q.ports.get(chord.east_far_port.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q east cut endpoint is unknown".into())
            })?;
            let west_nearest = nearest.xy(west_port.node)?;
            let east_nearest = nearest.xy(east_port.node)?;
            let west_domain = domains.get(west_port.node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q west cut endpoint has no candidate domain".into())
            })?;
            let east_domain = domains.get(east_port.node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q east cut endpoint has no candidate domain".into())
            })?;
            for &west_candidate in west_domain {
                budget.charge_into_error(1)?;
                let west_longitude = nearest
                    .pole_port_longitude
                    .get(chord.west_far_port.0 as usize)
                    .copied()
                    .flatten();
                let west_periods = nearest
                    .pole_port_periods
                    .get(chord.west_far_port.0 as usize)
                    .copied()
                    .flatten();
                let Some(west_point) = assigned_port_point(
                    frame,
                    q,
                    chord.west_far_port,
                    west_nearest,
                    west_candidate,
                    west_longitude,
                    west_periods,
                    &cut_east,
                    budget,
                )?
                else {
                    continue;
                };
                for &east_candidate in east_domain {
                    budget.charge_into_error(1)?;
                    let east_longitude = nearest
                        .pole_port_longitude
                        .get(chord.east_far_port.0 as usize)
                        .copied()
                        .flatten();
                    let east_periods = nearest
                        .pole_port_periods
                        .get(chord.east_far_port.0 as usize)
                        .copied()
                        .flatten();
                    let Some(east_point) = assigned_port_point(
                        frame,
                        q,
                        chord.east_far_port,
                        east_nearest,
                        east_candidate,
                        east_longitude,
                        east_periods,
                        &cut_east,
                        budget,
                    )?
                    else {
                        continue;
                    };
                    if let Some(latitude) =
                        cut_intersection_y(q, &period, &west_point, &east_point, budget)?
                    {
                        had_candidate_cut = true;
                        radius = radius.max(exact_abs_upper_with_work(
                            &latitude.try_subtract_with_work(&base_y, budget)?,
                            budget,
                        )?);
                    }
                }
            }
        }
        if !had_candidate_cut {
            return Ok(f64::INFINITY);
        }
        return Ok(radius);
    }

    let nearest_xy = nearest.xy(port.node)?;
    let domain = domains
        .get(port.node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q swept node has no candidate domain".into()))?;
    for &candidate in domain {
        budget.charge_into_error(1)?;
        let pole_longitude = nearest
            .pole_port_longitude
            .get(port_id.0 as usize)
            .copied()
            .flatten();
        let pole_periods = nearest
            .pole_port_periods
            .get(port_id.0 as usize)
            .copied()
            .flatten();
        let Some(candidate) = assigned_port_point(
            frame,
            q,
            port_id,
            nearest_xy,
            candidate,
            pole_longitude,
            pole_periods,
            &cut_east,
            budget,
        )?
        else {
            continue;
        };
        radius = radius.max(exact_abs_upper_with_work(
            &candidate.x.try_subtract_with_work(&base_x, budget)?,
            budget,
        )?);
        radius = radius.max(exact_abs_upper_with_work(
            &candidate.y.try_subtract_with_work(&base_y, budget)?,
            budget,
        )?);
    }
    Ok(radius)
}
pub(in crate::geometry::topology::q_embedding) fn q_proximity_pairs(
    chords: &[QChord],
    frontier: &[usize],
    period: f64,
    budget: &mut impl SearchAccount,
) -> Result<Vec<QProximityPair>> {
    budget.charge_into_error(
        u64::try_from(frontier.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut left_bounds = Vec::with_capacity(frontier.len());
    for &chord in frontier {
        budget.charge_into_error(1)?;
        left_bounds.push(
            chords
                .get(chord)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q frontier chord is unknown".into()))?
                .boxes
                .get(1)
                .copied()
                .expect("each chord has three translated bounds"),
        );
    }
    let mut right_images = Vec::with_capacity(chords.len().saturating_mul(3));
    let mut right_bounds = Vec::with_capacity(chords.len().saturating_mul(3));
    for (chord_index, chord) in chords.iter().enumerate() {
        budget.charge_into_error(1)?;
        for (shift_index, shift) in [-1_i8, 0, 1].into_iter().enumerate() {
            budget.charge_into_error(1)?;
            right_images.push((chord_index, shift, shift_index));
            right_bounds.push(
                *chord
                    .boxes
                    .get(shift_index)
                    .expect("each shift has a matching bound"),
            );
        }
    }

    let mut pairs = Vec::new();
    for_each_overlapping_bounds_cross_pair_with_work(
        &left_bounds,
        &right_bounds,
        budget,
        |left_index, right_index, work| {
            work.charge_evaluations(1)?;
            let left_chord_index = *frontier
                .get(left_index)
                .expect("left sweep ordinal belongs to the frontier");
            let (right_chord_index, right_shift, shift_index) = *right_images
                .get(right_index)
                .expect("right sweep ordinal belongs to translated chords");
            if left_chord_index == right_chord_index {
                return Ok(ControlFlow::<std::convert::Infallible>::Continue(()));
            }
            let left = chords
                .get(left_chord_index)
                .expect("left chord ordinal belongs to the chord table");
            let right = chords
                .get(right_chord_index)
                .expect("right chord ordinal belongs to the chord table");
            let right_segment = shifted_segment(right.nearest, right_shift, period);
            let right_radius = *right
                .image_radii
                .get(shift_index)
                .expect("each translated image has a swept radius");
            let limit = proximity_limit(left.radius, right_radius);
            if segments_within_chebyshev(left.nearest, right_segment, limit) {
                let pair = if left_chord_index < right_chord_index {
                    QProximityPair {
                        left: left_chord_index,
                        right: right_chord_index,
                        right_shift,
                    }
                } else {
                    QProximityPair {
                        left: right_chord_index,
                        right: left_chord_index,
                        right_shift: right_shift
                            .checked_neg()
                            .expect("Q periodic image shifts are limited to one turn"),
                    }
                };
                pairs.push(pair);
            }
            Ok(ControlFlow::<std::convert::Infallible>::Continue(()))
        },
    )?;
    try_sort_by_with_work(&mut pairs, budget, |left, right, _work| Ok(left.cmp(right)))?;
    budget
        .charge_into_error(u64::try_from(pairs.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    pairs.dedup();
    Ok(pairs)
}
pub(in crate::geometry::topology::q_embedding) fn candidate_domains_share_emitted_key(
    frame: GeographicFrame,
    left: &[QCandidate],
    right: &[QCandidate],
    budget: &mut impl Work,
) -> Result<bool> {
    if left.is_empty() || right.is_empty() {
        return Err(GeometryErrorKind::Invalid(
            "prepared Q key comparison has an empty candidate domain".into(),
        )
        .into());
    }
    for &left_candidate in left {
        budget.charge_into_error(1)?;
        let left_key = emitted_node_key(frame, left_candidate.native)?;
        for &right_candidate in right {
            budget.charge_into_error(1)?;
            if left_key == emitted_node_key(frame, right_candidate.native)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
pub(in crate::geometry::topology::q_embedding) fn build_q_proximity_closure(
    topology: &ExactGeographicTopology,
    chords: &[QChord],
    domains: &[Vec<QCandidate>],
    cut_rows: &[QCutRow],
    fan_rows: &[QFanRow],
    seeds: &[QNodeId],
    movable_axes: &[[bool; 2]],
    budget: &mut impl SearchAccount,
) -> Result<QClosure> {
    let node_count = topology.q.nodes.len();
    budget.charge_into_error(
        u64::try_from(
            node_count
                .checked_mul(5)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut incident_chords = Vec::new();
    incident_chords.resize_with(node_count, Vec::<usize>::new);
    for (chord_index, chord) in chords.iter().enumerate() {
        budget.charge_into_error(1)?;
        for &node in &chord.dependencies {
            budget.charge_into_error(1)?;
            incident_chords
                .get_mut(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q chord dependency is unknown".into()))?
                .push(chord_index);
        }
    }
    let mut incident_cuts = Vec::new();
    incident_cuts.resize_with(node_count, Vec::<usize>::new);
    for (cut_index, cut) in cut_rows.iter().enumerate() {
        budget.charge_into_error(1)?;
        for &node in &cut.dependencies {
            budget.charge_into_error(1)?;
            incident_cuts
                .get_mut(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q cut dependency is unknown".into()))?
                .push(cut_index);
        }
    }
    let mut incident_fans = Vec::new();
    incident_fans.resize_with(node_count, Vec::<usize>::new);
    for (fan_index, fan) in fan_rows.iter().enumerate() {
        budget.charge_into_error(1)?;
        for &node in &fan.dependencies {
            budget.charge_into_error(1)?;
            incident_fans
                .get_mut(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q fan dependency is unknown".into()))?
                .push(fan_index);
        }
    }

    let mut closure = vec![false; node_count];
    let mut frontier = Vec::new();
    let mut union = UnionFind::new(node_count);
    for &seed in seeds {
        budget.charge_into_error(1)?;
        if !node_is_movable(seed, movable_axes)? {
            continue;
        }
        let reached = closure.get_mut(seed.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q embedding seed is outside its topology".into())
        })?;
        if !std::mem::replace(reached, true) {
            frontier.push(seed);
        }
    }
    let frame = topology.sources.frame;
    for chord in chords {
        budget.charge_into_error(1)?;
        let [left, right] = chord.nodes;
        if left == right
            || !node_is_movable(left, movable_axes)?
            || !node_is_movable(right, movable_axes)?
        {
            continue;
        }
        let mut ordinary_ports = true;
        for port in chord.ports {
            budget.charge_into_error(1)?;
            ordinary_ports &= topology
                .q
                .ports
                .get(port.0 as usize)
                .is_some_and(|port| !port.cut);
        }
        if !ordinary_ports {
            continue;
        }
        let left_domain = domains.get(left.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q self-chord start has no admitted domain".into())
        })?;
        let right_domain = domains.get(right.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q self-chord end has no admitted domain".into())
        })?;
        if candidate_domains_share_emitted_key(frame, left_domain, right_domain, budget)? {
            include_closure_variables(
                [left, right],
                movable_axes,
                &mut closure,
                &mut union,
                &mut frontier,
                budget,
            )?;
        }
    }

    let mut pairs = Vec::new();
    budget.charge_into_error(
        u64::try_from(
            cut_rows
                .len()
                .checked_add(fan_rows.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut seen_cuts = vec![false; cut_rows.len()];
    let mut seen_fans = vec![false; fan_rows.len()];
    let period = frame.longitude().period();
    while !frontier.is_empty() {
        budget.charge_into_error(1)?;
        let mut frontier_chords = Vec::new();
        for node in &frontier {
            budget.charge_into_error(1)?;
            for &chord in incident_chords
                .get(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q frontier node is unknown".into()))?
            {
                budget.charge_into_error(1)?;
                frontier_chords.push(chord);
            }
        }
        try_sort_by_with_work(&mut frontier_chords, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(frontier_chords.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        frontier_chords.dedup();

        let mut newly_reached = Vec::new();
        for node in &frontier {
            budget.charge_into_error(1)?;
            for &cut_index in incident_cuts
                .get(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q frontier cut row is unknown".into()))?
            {
                budget.charge_into_error(1)?;
                let seen = seen_cuts.get_mut(cut_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q cut activation index is unknown".into())
                })?;
                if std::mem::replace(seen, true) {
                    continue;
                }
                let cut = cut_rows.get(cut_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q cut activation row is unknown".into())
                })?;
                include_closure_variables(
                    cut.dependencies.iter().copied(),
                    movable_axes,
                    &mut closure,
                    &mut union,
                    &mut newly_reached,
                    budget,
                )?;
            }
            for &fan_index in incident_fans
                .get(node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q frontier fan row is unknown".into()))?
            {
                budget.charge_into_error(1)?;
                let seen = seen_fans.get_mut(fan_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan activation index is unknown".into())
                })?;
                if std::mem::replace(seen, true) {
                    continue;
                }
                let fan = fan_rows.get(fan_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan activation row is unknown".into())
                })?;
                include_closure_variables(
                    fan.dependencies.iter().copied(),
                    movable_axes,
                    &mut closure,
                    &mut union,
                    &mut newly_reached,
                    budget,
                )?;
            }
        }

        if !frontier_chords.is_empty() {
            let reached_pairs = q_proximity_pairs(chords, &frontier_chords, period, budget)?;
            for pair in reached_pairs {
                budget.charge_into_error(1)?;
                let left = chords.get(pair.left).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q proximity left chord is unknown".into())
                })?;
                let right = chords.get(pair.right).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q proximity right chord is unknown".into())
                })?;
                include_closure_variables(
                    left.dependencies.iter().chain(&right.dependencies).copied(),
                    movable_axes,
                    &mut closure,
                    &mut union,
                    &mut newly_reached,
                    budget,
                )?;
                pairs.push(pair);
            }
        }
        try_sort_by_with_work(&mut newly_reached, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(newly_reached.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        newly_reached.dedup();
        frontier = newly_reached;
    }
    try_sort_by_with_work(&mut pairs, budget, |left, right, _work| Ok(left.cmp(right)))?;
    budget
        .charge_into_error(u64::try_from(pairs.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    pairs.dedup();

    let mut incident_pairs = Vec::new();
    budget
        .charge_into_error(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    incident_pairs.resize_with(node_count, Vec::<usize>::new);
    for (pair_index, pair) in pairs.iter().enumerate() {
        budget.charge_into_error(1)?;
        let left = chords.get(pair.left).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q proximity left chord is unknown".into())
        })?;
        let right = chords.get(pair.right).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q proximity right chord is unknown".into())
        })?;
        for &node in left.dependencies.iter().chain(&right.dependencies) {
            budget.charge_into_error(1)?;
            incident_pairs
                .get_mut(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q proximity dependency is unknown".into())
                })?
                .push(pair_index);
        }
    }
    for row in &mut incident_pairs {
        try_sort_by_with_work(row, budget, |left, right, _work| Ok(left.cmp(right)))?;
        budget.charge_into_error(
            u64::try_from(row.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        row.dedup();
    }
    let classes = build_q_classes(topology, &closure, &mut union, budget)?;
    Ok(QClosure {
        classes,
        proximity_pairs: pairs,
        dependency_index: QDependencyIndex {
            pairs: incident_pairs,
            cuts: incident_cuts,
            fans: incident_fans,
        },
    })
}
