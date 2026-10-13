use super::super::{
    BTreeMap, BTreeSet, ChartPole, DrawnPolygons, ExactGeographicQ, ExactSourceRange, FallibleAvl,
    GeographicFrame, GeometryErrorKind, NativeAreaEdge, NativeAreaEdgeKind, NativeGraphEdgeOwner,
    NativeLineChordSource, NativeLineRun, NativeLiteralChordAuthority, NativeLiteralChordUse,
    NativePoleClosure, NativePortData, NativeSeamEvent, NativeSeamVertices,
    NativeSelectedPoleSector, NativeSelectedPoleSectors, QEmissionDrawing, RecipeWalkOwner,
    ResourceError, Result, Work, merge_native_port_data, try_sort_by_with_work,
};

pub(in crate::geometry::topology::q_embedding) struct NativeSeamEventData {
    pub(in crate::geometry::topology::q_embedding) event: NativeSeamEvent,
    pub(in crate::geometry::topology::q_embedding) vertices: NativeSeamVertices,
}
pub(in crate::geometry::topology::q_embedding) fn normalize_native_seam_events(
    frame: GeographicFrame,
    mut events: Vec<NativeSeamEventData>,
    budget: &mut impl Work,
) -> Result<Vec<NativeSeamEventData>> {
    budget
        .charge_into_error(u64::try_from(events.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    try_sort_by_with_work(&mut events, budget, |left, right, budget| {
        budget.charge_into_error(1)?;
        left.event
            .latitude
            .try_cmp_with_work(&right.event.latitude, budget)
    })?;
    let mut normalized: Vec<NativeSeamEventData> = Vec::with_capacity(events.len());
    for event in events {
        if !event
            .event
            .latitude
            .try_cmp_with_work(&event.vertices.latitude, budget)?
            .is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "native seam event and bank vertices have different latitudes".into(),
            )
            .into());
        }
        if let Some(previous) = normalized.last_mut()
            && previous
                .event
                .latitude
                .try_cmp_with_work(&event.event.latitude, budget)?
                .is_eq()
        {
            previous.event.output_vertex |= event.event.output_vertex;
            previous.vertices.east = merge_native_port_data(
                frame,
                &previous.vertices.east,
                &event.vertices.east,
                budget,
            )?;
            previous.vertices.west = merge_native_port_data(
                frame,
                &previous.vertices.west,
                &event.vertices.west,
                budget,
            )?;
        } else {
            normalized.push(event);
        }
    }
    Ok(normalized)
}
pub(in crate::geometry::topology::q_embedding) fn append_native_line_edge(
    frame: GeographicFrame,
    run: &mut NativeLineRun,
    edge: NativeAreaEdge,
    line_graph_edges: &mut Vec<NativeAreaEdge>,
    budget: &mut impl Work,
) -> Result<()> {
    budget.charge_into_error(1)?;
    let NativeAreaEdge {
        vertices: [start, end],
        owner,
        kind,
    } = edge;
    if owner != NativeGraphEdgeOwner::Line {
        return Err(GeometryErrorKind::Invalid(
            "native area edge entered a source line trace".into(),
        )
        .into());
    }
    let NativeAreaEdgeKind::Producer {
        recipe,
        trace_edge,
        producer_start,
        producer_end,
        trace_ranges,
        known_spans,
    } = kind
    else {
        return Err(GeometryErrorKind::Invalid(
            "native presentation edge entered a source line trace".into(),
        )
        .into());
    };
    budget.charge_into_error(2)?;
    let literal_producer_range = ExactSourceRange {
        start: producer_start.as_ref().try_clone_with_work(budget)?,
        end: producer_end.try_clone_with_work(budget)?,
    };
    budget.charge_into_error(
        u64::try_from(trace_ranges.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut trace_range_copy = Vec::with_capacity(trace_ranges.len());
    for (edge, range) in &trace_ranges {
        trace_range_copy.push((*edge, range.try_clone_with_work(budget)?));
    }
    line_graph_edges.push(NativeAreaEdge {
        vertices: [
            start.try_clone_with_work(budget)?,
            end.try_clone_with_work(budget)?,
        ],
        owner,
        kind: NativeAreaEdgeKind::Producer {
            recipe,
            trace_edge,
            producer_start,
            producer_end,
            trace_ranges: trace_range_copy,
            known_spans,
        },
    });
    if let Some(previous) = run.vertices.last_mut() {
        *previous = merge_native_port_data(frame, previous, &start, budget)?;
    } else {
        run.vertices.push(start);
    }
    run.vertices.push(end);
    budget.charge_into_error(
        u64::try_from(trace_ranges.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    run.chords.push(NativeLineChordSource {
        recipe,
        trace_edge,
        producer_range: literal_producer_range,
        trace_ranges,
    });
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn finish_native_line_run(
    builder: &mut super::super::super::emission::EmissionBuilder<'_>,
    run: NativeLineRun,
    reference_ports: &mut Vec<NativePortData>,
    trace_line: usize,
    literal_chord_uses: &mut Vec<NativeLiteralChordUse>,
    budget: &mut impl Work,
) -> Result<super::super::super::emission::TracedLine> {
    if run.vertices.len() < 2 || run.chords.len().checked_add(1) != Some(run.vertices.len()) {
        return Err(GeometryErrorKind::Invalid(
            "native line run has incomplete endpoint or recipe rows".into(),
        )
        .into());
    }
    budget.charge_into_error(
        u64::try_from(run.vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    reference_ports.reserve(run.vertices.len());
    for vertex in &run.vertices {
        reference_ports.push(vertex.try_clone_with_work(budget)?);
    }
    budget.charge_into_error(
        u64::try_from(run.vertices.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let ports = run
        .vertices
        .into_iter()
        .map(|vertex| builder.port(vertex))
        .collect::<Result<Vec<_>>>()?;
    let reservation_units = u64::try_from(run.chords.len())
        .map_err(|_| ResourceError::SizeOverflow)?
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(reservation_units)?;
    let mut chords = Vec::with_capacity(run.chords.len());
    let mut trace_rows = Vec::with_capacity(run.chords.len());
    for (index, use_row) in run.chords.into_iter().enumerate() {
        budget.charge_into_error(1)?;
        let start = *ports.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native line start port is unknown".into())
        })?;
        let end = *ports
            .get(index.wrapping_add(1))
            .ok_or_else(|| GeometryErrorKind::Invalid("native line end port is unknown".into()))?;
        let chord = builder.chord([start, end], Some(use_row.recipe))?;
        if chord.0 as usize != literal_chord_uses.len() {
            return Err(GeometryErrorKind::Invalid(
                "native line chord is out of order in the literal-use map".into(),
            )
            .into());
        }
        literal_chord_uses.push(NativeLiteralChordUse {
            chord,
            authority: NativeLiteralChordAuthority::Producer {
                recipe: use_row.recipe,
                trace_edge: use_row.trace_edge,
                producer_range: use_row.producer_range,
                line_trace: trace_line,
                trace_row: index,
            },
        });
        trace_rows.push(
            use_row
                .trace_ranges
                .into_iter()
                .map(|(_, range)| range)
                .collect(),
        );
        chords.push(chord);
    }
    builder.line(chords)?;
    let trace = super::super::super::emission::SourceTrace::from_edge_rows(trace_rows, budget)?;
    let member = run.member.ok_or_else(|| {
        GeometryErrorKind::Invalid("native line run has no source member identity".into())
    })?;
    Ok(super::super::super::emission::TracedLine { member, trace })
}
pub(in crate::geometry::topology::q_embedding) fn native_area_facts(
    drawing: &QEmissionDrawing<'_>,
    selected_face_roots: &[Option<usize>],
) -> Result<BTreeMap<RecipeWalkOwner, usize>> {
    let mut facts = BTreeMap::new();
    match &drawing.polygons {
        DrawnPolygons::Regions { rings, .. } => {
            for ring in rings {
                if facts.insert(ring.recipe_walk, ring.region).is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "native area recipe walk appears more than once".into(),
                    )
                    .into());
                }
            }
        },
        DrawnPolygons::WholeSphere(_) => {
            let mut roots = selected_face_roots.iter().flatten().copied();
            let region = roots.next().ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "whole-sphere native drawing has no selected face root".into(),
                )
            })?;
            if roots.any(|root| root != region) {
                return Err(GeometryErrorKind::Invalid(
                    "whole-sphere selected faces do not share one retained region".into(),
                )
                .into());
            }
        },
    }
    Ok(facts)
}
pub(in crate::geometry::topology::q_embedding) fn native_selected_pole_sectors(
    topology: &ExactGeographicQ,
    selected_face_roots: &[Option<usize>],
    budget: &mut impl Work,
) -> Result<NativeSelectedPoleSectors> {
    if selected_face_roots.len() != topology.global_face_count {
        return Err(GeometryErrorKind::Invalid(
            "native selected roots do not match the exact Q face table".into(),
        )
        .into());
    }
    let mut sectors = Vec::new();
    let mut closures = Vec::new();
    let mut closure_index = FallibleAvl::with_capacity(0);
    for pole in [ChartPole::South, ChartPole::North] {
        topology.for_each_pole_sector_with_work(pole, budget, |sector, work| {
            let face = sector.face();
            let region = selected_face_roots
                .get(face.0 as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "native pole sector references an unknown selected face".into(),
                    )
                })?;
            let Some(region) = region else {
                return Ok(());
            };
            work.charge_into_error(1)?;
            sectors.push(NativeSelectedPoleSector { region, sector });
            let closure = (region, pole);
            if closure_index.insert_with_work(closure, work, &mut |left, right, work| {
                work.charge_into_error(1)?;
                Ok(left.cmp(right))
            })? {
                work.charge_into_error(1)?;
                closures.push(closure);
            }
            Ok(())
        })?;
    }
    Ok(NativeSelectedPoleSectors { sectors, closures })
}
pub(in crate::geometry::topology::q_embedding) fn native_pole_closure_regions(
    sectors: &[NativeSelectedPoleSector],
    budget: &mut impl Work,
) -> Result<BTreeSet<NativePoleClosure>> {
    // A repeated fan dart is a full-turn boundary; a multi-ray fan already
    // bounds a local pole sector and must not receive the complementary arc.
    budget.charge_into_error(
        u64::try_from(sectors.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut closures = BTreeSet::new();
    for selected in sectors {
        budget.charge_into_error(1)?;
        let pole = match selected.sector {
            super::super::super::q::PoleSector::Terminal { pole, .. }
            | super::super::super::q::PoleSector::Isolated { pole, .. } => Some(pole),
            super::super::super::q::PoleSector::Fan {
                pole,
                clockwise,
                counterclockwise,
                ..
            } if clockwise == counterclockwise => Some(pole),
            super::super::super::q::PoleSector::Fan { .. } => None,
        };
        if let Some(pole) = pole {
            budget.charge_into_error(1)?;
            closures.insert((selected.region, pole));
        }
    }
    Ok(closures)
}
