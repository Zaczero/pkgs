use super::super::{
    AssignedSite, EmissionChordId, EmissionPortId, ExactChartPoint, ExactRational,
    ExactSourceRange, GeographicFrame, GeometryErrorKind, NativeAreaEdgeKind,
    NativeChartSpellingKey, NativeLiteralChordAuthority, NativeProducerAtomUse,
    NativeProducerUseKey, NativeReference, NativeReferencePoints, NodeOrdinates, Point,
    ResourceError, Result, RootRange, ShapeLiteralLayout, native_producer_atom_uses,
};
use crate::geometry::exact_point::exact_chord_fraction;
use crate::work::Work;

pub(in crate::geometry::topology::q_embedding) fn native_chart_spelling_longitude(
    spelling: NativeChartSpellingKey,
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<Option<ExactRational>> {
    let half = ExactRational::from_f64(frame.longitude().half_period());
    Ok(match spelling {
        NativeChartSpellingKey::Interior(_) => None,
        NativeChartSpellingKey::SeamEast => Some(half),
        NativeChartSpellingKey::SeamWest => {
            Some(ExactRational::zero().try_subtract_with_work(&half, budget)?)
        },
        NativeChartSpellingKey::Pole(longitude) => {
            Some(ExactRational::from_f64(f64::from_bits(longitude)))
        },
    })
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_line_root_transport(
    reference: &NativeReference<'_>,
    atom_use: &NativeProducerAtomUse,
    producer_range: &ExactSourceRange,
    parent_ranges: &[RootRange],
    budget: &mut impl Work,
) -> Result<()> {
    let line_edge_index = atom_use
        .written_edge
        .checked_sub(reference.area_edges.len())
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("native line writer precedes its area edge table".into())
        })?;
    let edge = reference
        .line_graph_edges
        .get(line_edge_index)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("native line writer has no producer edge".into())
        })?;
    let NativeAreaEdgeKind::Producer {
        producer_start,
        producer_end,
        trace_ranges,
        ..
    } = &edge.kind
    else {
        return Err(GeometryErrorKind::Invalid(
            "native line root transport references a non-producer edge".into(),
        )
        .into());
    };
    if !producer_start
        .try_cmp_with_work(producer_end, budget)?
        .is_lt()
        || producer_start
            .try_cmp_with_work(&producer_range.start, budget)?
            .is_lt()
        || producer_end
            .try_cmp_with_work(&producer_range.end, budget)?
            .is_gt()
    {
        return Err(GeometryErrorKind::Invalid(
            "native line writer escaped its literal producer interval".into(),
        )
        .into());
    }
    let width = producer_range
        .end
        .try_subtract_with_work(&producer_range.start, budget)?;
    let local_start = producer_start
        .try_subtract_with_work(&producer_range.start, budget)?
        .try_quotient_with_work(&width, budget)?;
    let local_end = producer_end
        .try_subtract_with_work(&producer_range.start, budget)?
        .try_quotient_with_work(&width, budget)?;
    let mut expected = Vec::new();
    for parent in parent_ranges {
        budget.charge_into_error(1)?;
        if let Some(child) = parent.compose_with_work(&local_start, &local_end, budget)? {
            expected.push(child);
        }
    }
    budget.charge_into_error(
        u64::try_from(trace_ranges.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    if expected.len() != trace_ranges.len()
        || expected
            .iter()
            .zip(trace_ranges)
            .any(|(expected, (_, actual))| expected != actual)
    {
        return Err(GeometryErrorKind::Invalid(
            "native line graph atom lost its literal root-range transport".into(),
        )
        .into());
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_written_chord_transport(
    reference: &NativeReference<'_>,
    budget: &mut impl Work,
) -> Result<()> {
    let graph = &reference.graph;
    for (chord_index, use_row) in reference.literal_chord_uses.iter().enumerate() {
        budget.charge_into_error(1)?;
        if use_row.chord.0 as usize != chord_index {
            return Err(GeometryErrorKind::Invalid(
                "native literal chord-use rows changed their stable chord IDs".into(),
            )
            .into());
        }
        let chord = reference.plan.chords.get(chord_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native literal chord-use row has no chord".into())
        })?;
        let start_vertex = *reference
            .port_vertices
            .get(chord.ports[0].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native literal chord has no start graph vertex".into())
            })?;
        let end_vertex = *reference
            .port_vertices
            .get(chord.ports[1].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native literal chord has no end graph vertex".into())
            })?;
        match &use_row.authority {
            NativeLiteralChordAuthority::ReferenceAtom {
                atom,
                reversed,
                writer_edges,
            } => {
                if writer_edges.is_empty() {
                    return Err(GeometryErrorKind::Invalid(
                        "native area chord has no directed graph writer".into(),
                    )
                    .into());
                }
                let endpoints = graph.edge_nodes.get(*atom as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native area chord atom is unknown".into())
                })?;
                let (expected_start, expected_end) = if *reversed {
                    (endpoints[1], endpoints[0])
                } else {
                    (*endpoints).into()
                };
                if (start_vertex, end_vertex) != (expected_start, expected_end) {
                    return Err(GeometryErrorKind::Invalid(
                        "native area chord does not bind its directed reference atom".into(),
                    )
                    .into());
                }
                for &writer in writer_edges {
                    budget.charge_into_error(1)?;
                    let written = graph.written_edges.get(writer).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native area chord references an unknown writer edge".into(),
                        )
                    })?;
                    if written.atom != *atom {
                        return Err(GeometryErrorKind::Invalid(
                            "native area chord contributor maps to another graph atom".into(),
                        )
                        .into());
                    }
                }
            },
            NativeLiteralChordAuthority::Producer {
                recipe,
                trace_edge,
                producer_range,
                line_trace,
                trace_row,
            } => {
                if chord.recipe != Some(*recipe)
                    || producer_range
                        .start
                        .try_cmp_with_work(&producer_range.end, budget)?
                        .is_eq()
                {
                    return Err(GeometryErrorKind::Invalid(
                        "native line chord changed its directed producer interval".into(),
                    )
                    .into());
                }
                let trace = reference.line_traces.get(*line_trace).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native line chord has no trace owner".into())
                })?;
                let parent_ranges = trace.rows.row(*trace_row)?;
                let key = NativeProducerUseKey {
                    recipe: *recipe,
                    trace_edge: *trace_edge,
                };
                let atom_uses = native_producer_atom_uses(graph, key, budget)?;
                let increasing = producer_range
                    .start
                    .try_cmp_with_work(&producer_range.end, budget)?
                    .is_lt();
                if !increasing {
                    return Err(GeometryErrorKind::Invalid(
                        "native literal line chord reverses its producer interval".into(),
                    )
                    .into());
                }
                let range_start = &producer_range.start;
                let range_end = &producer_range.end;
                let mut interval_cursor = range_start.try_clone_with_work(budget)?;
                let mut previous_end = None;
                let mut covered = false;
                for offset in 0..atom_uses.len() {
                    budget.charge_into_error(1)?;
                    let atom_use = atom_uses.get(offset).ok_or_else(|| {
                        GeometryErrorKind::Invalid("native line atom-use row is unknown".into())
                    })?;
                    let (atom_low, atom_high) = if atom_use
                        .producer_range
                        .start
                        .try_cmp_with_work(&atom_use.producer_range.end, budget)?
                        .is_lt()
                    {
                        (&atom_use.producer_range.start, &atom_use.producer_range.end)
                    } else {
                        (&atom_use.producer_range.end, &atom_use.producer_range.start)
                    };
                    if atom_high.try_cmp_with_work(range_start, budget)?.is_le()
                        || atom_low.try_cmp_with_work(range_end, budget)?.is_ge()
                    {
                        continue;
                    }
                    if atom_low.try_cmp_with_work(range_start, budget)?.is_lt()
                        || atom_high.try_cmp_with_work(range_end, budget)?.is_gt()
                        || !atom_low
                            .try_cmp_with_work(&interval_cursor, budget)?
                            .is_eq()
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "native line chord and reference atoms have a gap or split atom".into(),
                        )
                        .into());
                    }
                    validate_native_line_root_transport(
                        reference,
                        atom_use,
                        producer_range,
                        parent_ranges,
                        budget,
                    )?;
                    let endpoints =
                        graph
                            .edge_nodes
                            .get(atom_use.atom as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "native line atom-use references an unknown atom".into(),
                                )
                            })?;
                    let (atom_start, atom_end) = if atom_use.reversed {
                        (endpoints[1], endpoints[0])
                    } else {
                        (*endpoints).into()
                    };
                    if previous_end.is_some_and(|previous| previous != atom_start) {
                        return Err(GeometryErrorKind::Invalid(
                            "native line chord reference atoms are not directed-contiguous".into(),
                        )
                        .into());
                    }
                    if !covered && atom_start != start_vertex {
                        return Err(GeometryErrorKind::Invalid(
                            "native line chord start is not its first reference atom".into(),
                        )
                        .into());
                    }
                    covered = true;
                    previous_end = Some(atom_end);
                    interval_cursor = atom_high.try_clone_with_work(budget)?;
                }
                if !covered
                    || !interval_cursor
                        .try_cmp_with_work(range_end, budget)?
                        .is_eq()
                    || previous_end != Some(end_vertex)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "native line chord does not cover its complete directed reference interval"
                            .into(),
                    )
                    .into());
                }
            },
        }
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_materialized_port(
    reference: &NativeReference<'_>,
    port_id: EmissionPortId,
    record: Point,
    sites: &[AssignedSite],
    ordinates: &[NodeOrdinates],
    axes: crate::geometry::CoordinateAxes,
    port_records: &mut [Option<Point>],
    candidate_points: &[ExactChartPoint],
    budget: &mut impl Work,
) -> Result<bool> {
    budget.charge_into_error(1)?;
    if !record.x.is_finite() || !record.y.is_finite() {
        return Ok(false);
    }
    let expected = super::super::super::emission::native_port_point(
        &reference.plan,
        sites,
        ordinates,
        port_id,
        axes,
    )?;
    if expected != record {
        return Ok(false);
    }
    let port_slot = port_records.get_mut(port_id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("native materialized port is outside its plan".into())
    })?;
    if port_slot.is_some_and(|previous| previous != record) {
        return Ok(false);
    }
    *port_slot = Some(record);
    let vertex = *reference
        .port_vertices
        .get(port_id.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("native materialized port has no reference vertex".into())
        })?;
    let point = ExactChartPoint::from_rationals(
        ExactRational::from_f64(record.x),
        ExactRational::from_f64(record.y),
    );
    let vertex_point = candidate_points.get(vertex as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid(
            "native materialized port maps outside the reference graph".into(),
        )
    })?;
    Ok(vertex_point == &point)
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_materialized_chord_row(
    reference: &NativeReference<'_>,
    chords: &[EmissionChordId],
    ports: &[EmissionPortId],
    port_records: &[Option<Point>],
    candidate_points: &NativeReferencePoints,
    budget: &mut impl Work,
) -> Result<bool> {
    let expected_records = chords
        .len()
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    if ports.len() != expected_records {
        return Ok(false);
    }
    for (index, &chord_id) in chords.iter().enumerate() {
        budget.charge_into_error(1)?;
        let chord = reference
            .plan
            .chords
            .get(chord_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native materialized walk references an unknown chord".into(),
                )
            })?;
        if ports.get(index).copied() != Some(chord.ports[0])
            || ports
                .get(index.checked_add(1).ok_or(ResourceError::SizeOverflow)?)
                .copied()
                != Some(chord.ports[1])
        {
            return Ok(false);
        }
        let start_record = port_records
            .get(chord.ports[0].0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native materialized chord has no start record".into())
            })?;
        let end_record = port_records
            .get(chord.ports[1].0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native materialized chord has no end record".into())
            })?;
        let start_vertex = *reference
            .port_vertices
            .get(chord.ports[0].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native materialized chord start has no graph vertex".into(),
                )
            })?;
        let end_vertex = *reference
            .port_vertices
            .get(chord.ports[1].0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native materialized chord end has no graph vertex".into(),
                )
            })?;
        let start = candidate_points
            .exact
            .get(start_vertex as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native materialized chord start has no candidate point".into(),
                )
            })?;
        let end = candidate_points
            .exact
            .get(end_vertex as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native materialized chord end has no candidate point".into(),
                )
            })?;
        if start
            != &ExactChartPoint::from_rationals(
                ExactRational::from_f64(start_record.x),
                ExactRational::from_f64(start_record.y),
            )
            || end
                != &ExactChartPoint::from_rationals(
                    ExactRational::from_f64(end_record.x),
                    ExactRational::from_f64(end_record.y),
                )
        {
            return Ok(false);
        }
        let use_row = reference
            .literal_chord_uses
            .get(chord_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "native materialized chord has no reference-use row".into(),
                )
            })?;
        match &use_row.authority {
            NativeLiteralChordAuthority::ReferenceAtom { atom, reversed, .. } => {
                let endpoints =
                    reference
                        .graph
                        .edge_nodes
                        .get(*atom as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native materialized area chord atom is unknown".into(),
                            )
                        })?;
                let expected = if *reversed {
                    [endpoints[1], endpoints[0]]
                } else {
                    [endpoints[0], endpoints[1]]
                };
                if [start_vertex, end_vertex] != expected {
                    return Ok(false);
                }
            },
            NativeLiteralChordAuthority::Producer {
                recipe,
                trace_edge,
                producer_range,
                ..
            } => {
                if chord.recipe != Some(*recipe)
                    || !producer_range
                        .start
                        .try_cmp_with_work(&producer_range.end, budget)?
                        .is_lt()
                {
                    return Ok(false);
                }
                let atom_uses = native_producer_atom_uses(
                    &reference.graph,
                    NativeProducerUseKey {
                        recipe: *recipe,
                        trace_edge: *trace_edge,
                    },
                    budget,
                )?;
                let mut cursor = producer_range.start.try_clone_with_work(budget)?;
                let mut previous_vertex = start_vertex;
                let mut previous_fraction = None;
                let mut first_fraction = None;
                let mut found = false;
                for atom_use in atom_uses {
                    budget.charge_into_error(1)?;
                    let low = if atom_use
                        .producer_range
                        .start
                        .try_cmp_with_work(&atom_use.producer_range.end, budget)?
                        .is_le()
                    {
                        &atom_use.producer_range.start
                    } else {
                        &atom_use.producer_range.end
                    };
                    let high = if atom_use
                        .producer_range
                        .start
                        .try_cmp_with_work(&atom_use.producer_range.end, budget)?
                        .is_le()
                    {
                        &atom_use.producer_range.end
                    } else {
                        &atom_use.producer_range.start
                    };
                    if high
                        .try_cmp_with_work(&producer_range.start, budget)?
                        .is_le()
                        || low.try_cmp_with_work(&producer_range.end, budget)?.is_ge()
                    {
                        continue;
                    }
                    if !low.try_cmp_with_work(&cursor, budget)?.is_eq()
                        || !high.try_cmp_with_work(&producer_range.end, budget)?.is_le()
                    {
                        return Ok(false);
                    }
                    let endpoints = reference
                        .graph
                        .edge_nodes
                        .get(atom_use.atom as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native materialized line atom is unknown".into(),
                            )
                        })?;
                    let (atom_start, atom_end) = if atom_use.reversed {
                        (endpoints[1], endpoints[0])
                    } else {
                        (*endpoints).into()
                    };
                    if atom_start != previous_vertex {
                        return Ok(false);
                    }
                    let atom_start_point = candidate_points
                        .exact
                        .get(atom_start as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native line atom has no candidate start point".into(),
                            )
                        })?;
                    let atom_end_point =
                        candidate_points
                            .exact
                            .get(atom_end as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "native line atom has no candidate end point".into(),
                                )
                            })?;
                    let (Some(local_start), Some(local_end)) = (
                        exact_chord_fraction(start, end, atom_start_point, budget)?,
                        exact_chord_fraction(start, end, atom_end_point, budget)?,
                    ) else {
                        return Ok(false);
                    };
                    if !local_start.try_cmp_with_work(&local_end, budget)?.is_lt() {
                        return Ok(false);
                    }
                    if let Some(previous) = &previous_fraction
                        && !local_start.try_cmp_with_work(previous, budget)?.is_eq()
                    {
                        return Ok(false);
                    }
                    first_fraction.get_or_insert(local_start);
                    previous_fraction = Some(local_end);
                    let written = reference
                        .graph
                        .written_edges
                        .get(atom_use.written_edge)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native materialized line writer has no graph row".into(),
                            )
                        })?;
                    if written.atom != atom_use.atom || written.reversed != atom_use.reversed {
                        return Err(GeometryErrorKind::Invalid(
                            "native materialized line writer changed its atom binding".into(),
                        )
                        .into());
                    }
                    cursor = high.try_clone_with_work(budget)?;
                    previous_vertex = atom_end;
                    found = true;
                }
                if !found
                    || !cursor
                        .try_cmp_with_work(&producer_range.end, budget)?
                        .is_eq()
                    || previous_vertex != end_vertex
                    || first_fraction
                        .as_ref()
                        .is_none_or(|fraction| !fraction.is_zero())
                    || previous_fraction
                        .as_ref()
                        .is_none_or(|fraction| !fraction.is_one())
                {
                    return Ok(false);
                }
            },
        }
    }
    Ok(true)
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_sealed_layout(
    reference: &NativeReference<'_>,
    layout: &ShapeLiteralLayout<EmissionPortId>,
    port_records: &[Option<Point>],
    candidate_points: &NativeReferencePoints,
    budget: &mut impl Work,
) -> Result<bool> {
    if layout.points.len() != reference.plan.points.len()
        || layout.lines.len() != reference.plan.lines.len()
        || layout.polygons.len() != reference.plan.parts.len()
    {
        return Ok(false);
    }
    for point in &layout.points {
        budget.charge_into_error(1)?;
        if reference.plan.points.get(point.output as usize) != Some(&point.image)
            || port_records
                .get(point.image.0 as usize)
                .copied()
                .flatten()
                .is_none()
        {
            return Ok(false);
        }
    }
    for line in &layout.lines {
        budget.charge_into_error(1)?;
        let chords = reference
            .plan
            .lines
            .get(line.output as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("sealed native line has no logical output row".into())
            })?;
        if line.images.len()
            != chords
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
            || !validate_native_materialized_chord_row(
                reference,
                chords,
                &line.images,
                port_records,
                candidate_points,
                budget,
            )?
        {
            return Ok(false);
        }
    }
    for polygon in &layout.polygons {
        budget.charge_into_error(1)?;
        let part = reference
            .plan
            .parts
            .get(polygon.output as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("sealed native polygon has no logical output row".into())
            })?;
        if polygon.holes.len() != part.holes.len()
            || polygon.shell.images.len()
                != part
                    .shell
                    .len()
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?
            || polygon.shell.images.first() != polygon.shell.images.last()
            || !validate_native_materialized_chord_row(
                reference,
                &part.shell,
                &polygon.shell.images,
                port_records,
                candidate_points,
                budget,
            )?
        {
            return Ok(false);
        }
        for (hole, chords) in polygon.holes.iter().zip(&part.holes) {
            if hole.images.len()
                != chords
                    .len()
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?
                || hole.images.first() != hole.images.last()
                || !validate_native_materialized_chord_row(
                    reference,
                    chords,
                    &hole.images,
                    port_records,
                    candidate_points,
                    budget,
                )?
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
