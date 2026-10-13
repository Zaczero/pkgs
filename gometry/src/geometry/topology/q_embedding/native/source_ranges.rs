use super::super::{
    AssignedSite, ChartSpanId, ExactChartPoint, ExactGeographicTopology, ExactRational,
    ExactSourceRange, FragmentRecipeId, GeographicFrame, GeometryErrorKind, NativeAreaEdge,
    NativeAreaEdgeKind, NativeGraphEdgeOwner, NativeKnownSpanBoundary, NativeSourceFaceGerm,
    NativeSourceSpanUse, QDartId, QPublishedChord, QSourceSpanRange, ResourceError, Result, RootId,
    RootRange, RootRequest, Work, merge_native_port_data, native_chart_vertex_key,
    native_q_port_data, span_parameter_at_source, try_sort_by_with_work,
};
use crate::geometry::topology::emission::SourceTrace;

pub(in crate::geometry::topology::q_embedding) fn trace_ranges_for_slice(
    trace: &SourceTrace,
    edge: usize,
    local_start: &ExactRational,
    local_end: &ExactRational,
    budget: &mut impl Work,
) -> Result<Vec<(usize, RootRange)>> {
    let parent_ranges = trace.rows.row(edge)?;
    let mut output = Vec::with_capacity(parent_ranges.len());
    for (owner, parent) in parent_ranges.iter().enumerate() {
        budget.charge_into_error(1)?;
        if let Some(range) = parent.compose_with_work(local_start, local_end, budget)? {
            output.push((owner, range));
        }
    }
    Ok(output)
}
pub(in crate::geometry::topology::q_embedding) fn interpolate_producer_parameter(
    source_start: &ExactRational,
    source_end: &ExactRational,
    producer_start: &ExactRational,
    producer_end: &ExactRational,
    producer_parameter: &ExactRational,
    budget: &mut impl Work,
) -> Result<ExactRational> {
    let producer_width = producer_end.try_subtract_with_work(producer_start, budget)?;
    if producer_width.is_zero() {
        return source_start.try_clone_with_work(budget);
    }
    let local = producer_parameter
        .try_subtract_with_work(producer_start, budget)?
        .try_quotient_with_work(&producer_width, budget)?;
    source_start.try_add_with_work(
        &source_end
            .try_subtract_with_work(source_start, budget)?
            .try_product_with_work(&local, budget)?,
        budget,
    )
}
pub(in crate::geometry::topology::q_embedding) fn native_source_face_germs(
    topology: &ExactGeographicTopology,
    selected_face_roots: &[Option<usize>],
    span_id: ChartSpanId,
    budget: &mut impl Work,
) -> Result<Option<[NativeSourceFaceGerm; 2]>> {
    let span_index = span_id.0 as usize;
    let Some([left_face, right_face]) = topology
        .q
        .span_global_faces
        .get(span_index)
        .copied()
        .flatten()
    else {
        return Ok(None);
    };
    let span = topology.q.spans.get(span_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("native source-face germ has an unknown Q span".into())
    })?;
    let forward = topology
        .q
        .span_darts
        .get(span_index)
        .copied()
        .flatten()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("native source-face germ has no directed Q-span edge".into())
        })?;
    let reverse = QDartId(forward.0 ^ 1);
    let [first_port, second_port] = span.ports;
    let entries = [
        (forward, left_face, first_port, second_port),
        (reverse, right_face, second_port, first_port),
    ];
    budget.charge_into_error(2)?;
    let mut germs = entries.map(|(dart, face, ..)| NativeSourceFaceGerm {
        dart,
        face,
        selected_root: None,
    });
    for (germ, (dart_id, expected_face, from_port, to_port)) in germs.iter_mut().zip(entries) {
        budget.charge_into_error(1)?;
        let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native source-face germ has an unknown Q dart".into())
        })?;
        let local_face = *topology
            .q
            .dart_faces
            .get(dart_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native source-face dart has no local face".into())
            })?;
        let global_face = *topology
            .q
            .global_face_of_local
            .get(local_face.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native source-face local face is unknown".into())
            })?;
        if dart.span != span_id
            || dart.from_port != from_port
            || dart.to_port != to_port
            || global_face != expected_face
        {
            return Err(GeometryErrorKind::Invalid(
                "native source-face germ disagrees with its directed Q dart".into(),
            )
            .into());
        }
        germ.selected_root = *selected_face_roots
            .get(global_face.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native source-face germ has no selected-root row".into(),
                )
            })?;
    }
    Ok(Some(germs))
}
pub(in crate::geometry::topology::q_embedding) struct NativeKnownSourceSpanSlice {
    pub(in crate::geometry::topology::q_embedding) uses: Vec<NativeSourceSpanUse>,
    pub(in crate::geometry::topology::q_embedding) selected_left_region: Option<usize>,
}
pub(in crate::geometry::topology::q_embedding) fn native_known_source_spans_for_slice(
    topology: &ExactGeographicTopology,
    selected_face_roots: &[Option<usize>],
    region: Option<usize>,
    chord: &QPublishedChord,
    local_start: &ExactRational,
    local_end: &ExactRational,
    budget: &mut impl Work,
) -> Result<NativeKnownSourceSpanSlice> {
    budget.charge_into_error(
        u64::try_from(chord.source_ranges.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut output = Vec::with_capacity(chord.source_ranges.len());
    let mut selected_left_region = None;
    for source in &chord.source_ranges {
        budget.charge_into_error(1)?;
        let (producer_low, producer_high) = if source
            .producer_range
            .start
            .try_cmp_with_work(&source.producer_range.end, budget)?
            .is_le()
        {
            (
                source.producer_range.start.try_clone_with_work(budget)?,
                source.producer_range.end.try_clone_with_work(budget)?,
            )
        } else {
            (
                source.producer_range.end.try_clone_with_work(budget)?,
                source.producer_range.start.try_clone_with_work(budget)?,
            )
        };
        let overlap_start = if producer_low.try_cmp_with_work(local_start, budget)?.is_gt() {
            producer_low.try_clone_with_work(budget)?
        } else {
            local_start.try_clone_with_work(budget)?
        };
        let overlap_end = if producer_high.try_cmp_with_work(local_end, budget)?.is_lt() {
            producer_high.try_clone_with_work(budget)?
        } else {
            local_end.try_clone_with_work(budget)?
        };
        if overlap_start
            .try_cmp_with_work(&overlap_end, budget)?
            .is_gt()
            || (overlap_start
                .try_cmp_with_work(&overlap_end, budget)?
                .is_eq()
                && !source
                    .producer_range
                    .start
                    .try_cmp_with_work(&source.producer_range.end, budget)?
                    .is_eq())
        {
            continue;
        }
        let source_start = interpolate_producer_parameter(
            &source.source_range.start,
            &source.source_range.end,
            &source.producer_range.start,
            &source.producer_range.end,
            &overlap_start,
            budget,
        )?;
        let source_end = interpolate_producer_parameter(
            &source.source_range.start,
            &source.source_range.end,
            &source.producer_range.start,
            &source.producer_range.end,
            &overlap_end,
            budget,
        )?;
        let span_start = interpolate_producer_parameter(
            &source.span_range.start,
            &source.span_range.end,
            &source.producer_range.start,
            &source.producer_range.end,
            &overlap_start,
            budget,
        )?;
        let span_end = interpolate_producer_parameter(
            &source.span_range.start,
            &source.span_range.end,
            &source.producer_range.start,
            &source.producer_range.end,
            &overlap_end,
            budget,
        )?;
        let span_id = super::super::super::q::ChartSpanId(
            u32::try_from(source.span).map_err(|_| ResourceError::SizeOverflow)?,
        );
        let q_span = topology.q.spans.get(source.span).ok_or_else(|| {
            GeometryErrorKind::Invalid("native producer source span is unknown".into())
        })?;
        if q_span.nodes != source.nodes || q_span.ports != source.ports {
            return Err(GeometryErrorKind::Invalid(
                "native producer source-span map changed its Q endpoints".into(),
            )
            .into());
        }
        let face_germs = native_source_face_germs(topology, selected_face_roots, span_id, budget)?;
        if let (Some(region), Some(germs)) = (region, face_germs)
            && !span_start.try_cmp_with_work(&span_end, budget)?.is_eq()
        {
            let selected_root = if span_start.try_cmp_with_work(&span_end, budget)?.is_lt() {
                germs.first().and_then(|germ| germ.selected_root)
            } else {
                germs.get(1).and_then(|germ| germ.selected_root)
            };
            if selected_root != Some(region) {
                return Err(GeometryErrorKind::Invalid(
                    "native producer span changed its selected source-face germ".into(),
                )
                .into());
            }
            selected_left_region = Some(region);
        }
        output.push(NativeSourceSpanUse {
            span: span_id,
            owner: source.owner,
            source_edge: source.source_edge,
            source_lift: source.source_lift,
            source_range: ExactSourceRange {
                start: source_start,
                end: source_end,
            },
            producer_range: ExactSourceRange {
                start: overlap_start,
                end: overlap_end,
            },
            span_range: ExactSourceRange {
                start: span_start,
                end: span_end,
            },
            nodes: source.nodes,
            ports: source.ports,
            face_germs,
        });
    }
    Ok(NativeKnownSourceSpanSlice {
        uses: output,
        selected_left_region,
    })
}
pub(in crate::geometry::topology::q_embedding) fn native_known_span_boundaries(
    topology: &ExactGeographicTopology,
    area_edges: &[NativeAreaEdge],
    line_edges: &[NativeAreaEdge],
    builder: &super::super::super::emission::EmissionBuilder<'_>,
    port_points: &[Option<ExactChartPoint>],
    sites: &[Option<AssignedSite>],
    budget: &mut impl Work,
) -> Result<Vec<NativeKnownSpanBoundary>> {
    let all_edges = area_edges.iter().chain(line_edges);
    let capacity = all_edges.clone().try_fold(0_usize, |count, edge| {
        let known_count = match &edge.kind {
            NativeAreaEdgeKind::Producer { known_spans, .. } => known_spans.len(),
            NativeAreaEdgeKind::Restriction(_) | NativeAreaEdgeKind::WholeSphereCover { .. } => 0,
        };
        count
            .checked_add(
                known_count
                    .checked_mul(2)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .ok_or(ResourceError::SizeOverflow)
    })?;
    budget.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut boundaries = Vec::with_capacity(capacity);
    for edge in all_edges {
        let NativeAreaEdgeKind::Producer { known_spans, .. } = &edge.kind else {
            continue;
        };
        for known in known_spans {
            budget.charge_into_error(1)?;
            for parameter in [&known.span_range.start, &known.span_range.end] {
                budget.charge_into_error(1)?;
                let endpoint = if parameter.is_zero() {
                    Some((
                        ExactRational::zero(),
                        *known.ports.first().ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native source span has no first endpoint port".into(),
                            )
                        })?,
                    ))
                } else if parameter.is_one() {
                    Some((
                        ExactRational::one(),
                        *known.ports.get(1).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native source span has no second endpoint port".into(),
                            )
                        })?,
                    ))
                } else {
                    None
                };
                if let Some((parameter, port)) = endpoint {
                    let q_port = topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native source-span endpoint port is unknown".into(),
                        )
                    })?;
                    let source_request = if q_port.cut {
                        Some(native_source_request_at_known_boundary(
                            known, &parameter, budget,
                        )?)
                    } else {
                        None
                    };
                    let source_requests = source_request.as_slice();
                    let data = native_q_port_data(
                        topology,
                        builder,
                        port,
                        port_points,
                        sites,
                        source_requests,
                        budget,
                    )?;
                    boundaries.push(NativeKnownSpanBoundary {
                        span: known.span,
                        parameter,
                        port,
                        data,
                    });
                }
            }
        }
    }
    try_sort_by_with_work(&mut boundaries, budget, |left, right, budget| {
        budget.charge_into_error(1)?;
        let span = left.span.cmp(&right.span);
        if !span.is_eq() {
            return Ok(span);
        }
        left.parameter.try_cmp_with_work(&right.parameter, budget)
    })?;
    budget.charge_into_error(
        u64::try_from(boundaries.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut unique: Vec<NativeKnownSpanBoundary> = Vec::with_capacity(boundaries.len());
    for boundary in boundaries {
        append_native_known_span_boundary(&mut unique, boundary, topology.sources.frame, budget)?;
    }
    Ok(unique)
}
pub(in crate::geometry::topology::q_embedding) fn append_native_known_span_boundary(
    unique: &mut Vec<NativeKnownSpanBoundary>,
    boundary: NativeKnownSpanBoundary,
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<()> {
    budget.charge_into_error(1)?;
    if let Some(previous) = unique.last_mut()
        && previous.span == boundary.span
        && previous
            .parameter
            .try_cmp_with_work(&boundary.parameter, budget)?
            .is_eq()
    {
        if previous.port != boundary.port
            || native_chart_vertex_key(&previous.data) != native_chart_vertex_key(&boundary.data)
            || !previous
                .data
                .point
                .try_eq_with_work(&boundary.data.point, budget)?
        {
            return Err(GeometryErrorKind::Invalid(
                "coincident producer uses disagree on a known Q-span endpoint".into(),
            )
            .into());
        }
        previous.data = merge_native_port_data(frame, &previous.data, &boundary.data, budget)?;
    } else {
        unique.push(boundary);
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn native_source_request_at_known_boundary(
    source: &NativeSourceSpanUse,
    span_parameter: &ExactRational,
    budget: &mut impl Work,
) -> Result<RootRequest> {
    let source_parameter = if span_parameter
        .try_cmp_with_work(&source.span_range.start, budget)?
        .is_eq()
    {
        &source.source_range.start
    } else if span_parameter
        .try_cmp_with_work(&source.span_range.end, budget)?
        .is_eq()
    {
        &source.source_range.end
    } else {
        return Err(GeometryErrorKind::Invalid(
            "known source-span boundary has no directed root parameter".into(),
        )
        .into());
    };
    RootRequest::new_with_work(
        RootId::from_index(source.source_edge.0),
        source_parameter.try_clone_with_work(budget)?,
        budget,
    )
}
#[expect(
    clippy::suspicious_operation_groupings,
    reason = "NativeSourceSpanUse preserves its owner's chart lift as source_lift"
)]
pub(in crate::geometry::topology::q_embedding) fn validate_native_source_ranges(
    topology: &ExactGeographicTopology,
    selected_face_roots: &[Option<usize>],
    producer_edges: &[NativeAreaEdge],
    expected_leaves: &[(FragmentRecipeId, usize)],
    expanded_traces: &[SourceTrace],
    selected_area: bool,
    budget: &mut impl Work,
) -> Result<()> {
    budget.charge_into_error(
        u64::try_from(expected_leaves.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut edge_cursor = 0_usize;
    for &(recipe, trace_edge) in expected_leaves {
        let trace = expanded_traces.get(recipe.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native producer leaf has no expanded source trace".into())
        })?;
        let owners = trace.owners_for_edge(trace_edge)?;
        let mut interval_cursor = ExactRational::zero();
        let mut found = false;
        while let Some(edge) = producer_edges.get(edge_cursor) {
            let NativeAreaEdgeKind::Producer {
                recipe: edge_recipe,
                trace_edge: edge_trace,
                producer_start,
                producer_end,
                trace_ranges,
                known_spans,
            } = &edge.kind
            else {
                edge_cursor = edge_cursor
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                continue;
            };
            if *edge_recipe != recipe || *edge_trace != trace_edge {
                break;
            }
            if selected_area != matches!(edge.owner, NativeGraphEdgeOwner::Area { .. }) {
                return Err(GeometryErrorKind::Invalid(
                    "native producer writer is in the wrong area/line source table".into(),
                )
                .into());
            }
            found = true;
            budget.charge_into_error(1)?;
            if !producer_start
                .try_cmp_with_work(&interval_cursor, budget)?
                .is_eq()
                || !producer_start
                    .try_cmp_with_work(producer_end, budget)?
                    .is_lt()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native producer children do not partition their leaf in order".into(),
                )
                .into());
            }
            budget.charge_into_error(
                u64::try_from(owners.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut expected_ranges = Vec::with_capacity(owners.len());
            for (owner_index, owner) in owners.iter().enumerate() {
                budget.charge_into_error(1)?;
                if let Some(range) =
                    owner.compose_with_work(producer_start, producer_end, budget)?
                {
                    expected_ranges.push((owner_index, range));
                }
            }
            if expected_ranges.len() != trace_ranges.len() {
                return Err(GeometryErrorKind::Invalid(
                    "native producer child changed its directed source-range owners".into(),
                )
                .into());
            }
            for ((expected_owner, expected_range), (actual_owner, actual_range)) in
                expected_ranges.iter().zip(trace_ranges)
            {
                budget.charge_into_error(1)?;
                if expected_owner != actual_owner
                    || !expected_range.try_eq_with_work(actual_range, budget)?
                {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer child changed its directed source-range owners".into(),
                    )
                    .into());
                }
            }
            budget.charge_into_error(
                u64::try_from(known_spans.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            for known in known_spans {
                budget.charge_into_error(1)?;
                if !super::super::super::exact::in_unit_interval_with_work(
                    &known.source_range.start,
                    budget,
                )? || !super::super::super::exact::in_unit_interval_with_work(
                    &known.source_range.end,
                    budget,
                )? || !super::super::super::exact::in_unit_interval_with_work(
                    &known.producer_range.start,
                    budget,
                )? || !super::super::super::exact::in_unit_interval_with_work(
                    &known.producer_range.end,
                    budget,
                )? || !super::super::super::exact::in_unit_interval_with_work(
                    &known.span_range.start,
                    budget,
                )? || !super::super::super::exact::in_unit_interval_with_work(
                    &known.span_range.end,
                    budget,
                )? || known
                    .producer_range
                    .start
                    .try_cmp_with_work(producer_start, budget)?
                    .is_lt()
                    || known
                        .producer_range
                        .end
                        .try_cmp_with_work(producer_end, budget)?
                        .is_gt()
                {
                    return Err(GeometryErrorKind::Invalid(
                        "native source-span use lies outside its producer child interval".into(),
                    )
                    .into());
                }
                let span = topology.q.spans.get(known.span.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native producer germ references an unknown Q span".into(),
                    )
                })?;
                if span.nodes != known.nodes || span.ports != known.ports {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer germ changed its Q-span endpoints".into(),
                    )
                    .into());
                }
                let owner = span.owners.get(known.owner).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native producer germ references an unknown Q-span owner".into(),
                    )
                })?;
                if owner.source_edge != known.source_edge || owner.lift != known.source_lift {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer germ changed its original source edge or chart lift"
                            .into(),
                    )
                    .into());
                }
                let (low, high) = if owner
                    .source_range
                    .start
                    .try_cmp_with_work(&owner.source_range.end, budget)?
                    .is_le()
                {
                    (&owner.source_range.start, &owner.source_range.end)
                } else {
                    (&owner.source_range.end, &owner.source_range.start)
                };
                let source_start_within = known
                    .source_range
                    .start
                    .try_cmp_with_work(low, budget)?
                    .is_ge()
                    && known
                        .source_range
                        .start
                        .try_cmp_with_work(high, budget)?
                        .is_le();
                let source_end_within = known
                    .source_range
                    .end
                    .try_cmp_with_work(low, budget)?
                    .is_ge()
                    && known
                        .source_range
                        .end
                        .try_cmp_with_work(high, budget)?
                        .is_le();
                let indexed = QSourceSpanRange {
                    start: low.try_clone_with_work(budget)?,
                    end: high.try_clone_with_work(budget)?,
                    span: known.span.0 as usize,
                    owner: known.owner,
                    source_lift: owner.lift,
                    source_range: ExactSourceRange {
                        start: owner.source_range.start.try_clone_with_work(budget)?,
                        end: owner.source_range.end.try_clone_with_work(budget)?,
                    },
                    reversed: owner.reversed,
                    nodes: span.nodes,
                    ports: span.ports,
                };
                let span_start =
                    span_parameter_at_source(&indexed, &known.source_range.start, budget)?;
                let span_end = span_parameter_at_source(&indexed, &known.source_range.end, budget)?;
                if !source_start_within
                    || !source_end_within
                    || !span_start
                        .try_cmp_with_work(&known.span_range.start, budget)?
                        .is_eq()
                    || !span_end
                        .try_cmp_with_work(&known.span_range.end, budget)?
                        .is_eq()
                {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer germ changed its exact source-span interval".into(),
                    )
                    .into());
                }
                let expected_face_germs =
                    native_source_face_germs(topology, selected_face_roots, known.span, budget)?;
                if expected_face_germs != known.face_germs {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer germ changed its directed Q face sides".into(),
                    )
                    .into());
                }
                if selected_area
                    && let Some(germs) = known.face_germs
                    && !known
                        .span_range
                        .start
                        .try_cmp_with_work(&known.span_range.end, budget)?
                        .is_eq()
                {
                    let selected_root = if known
                        .span_range
                        .start
                        .try_cmp_with_work(&known.span_range.end, budget)?
                        .is_lt()
                    {
                        germs.first().and_then(|germ| germ.selected_root)
                    } else {
                        germs.get(1).and_then(|germ| germ.selected_root)
                    };
                    let NativeGraphEdgeOwner::Area { region } = edge.owner else {
                        return Err(GeometryErrorKind::Invalid(
                            "native selected producer edge has no area-region owner".into(),
                        )
                        .into());
                    };
                    if selected_root != Some(region) {
                        return Err(GeometryErrorKind::Invalid(
                            "native producer span does not carry its selected source-side root"
                                .into(),
                        )
                        .into());
                    }
                }
            }
            interval_cursor = producer_end.try_clone_with_work(budget)?;
            edge_cursor = edge_cursor
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        if !found
            || !interval_cursor
                .try_cmp_with_work(&ExactRational::one(), budget)?
                .is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "native area source ranges do not cover their complete producer leaf".into(),
            )
            .into());
        }
    }
    for edge in producer_edges.iter().skip(edge_cursor) {
        budget.charge_into_error(1)?;
        if matches!(&edge.kind, NativeAreaEdgeKind::Producer { .. }) {
            return Err(GeometryErrorKind::Invalid(
                "native producer graph contains an unassigned leaf".into(),
            )
            .into());
        }
    }
    Ok(())
}
