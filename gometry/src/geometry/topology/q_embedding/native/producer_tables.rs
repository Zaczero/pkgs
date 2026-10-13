use super::super::{
    BTreeMap, BTreeSet, ChartPole, EmissionNodeId, ExactChartPoint, ExactRational,
    ExactSourceRange, GeographicFrame, GeometryErrorKind, NativeAreaEdge, NativeAreaEdgeKind,
    NativeChartSpellingKey, NativeChartVertexKey, NativeGraphEdgeOwner, NativeGraphFaceSourceTable,
    NativeGraphFaceSourceUse, NativeGraphSewing, NativeGraphSewingKind, NativeProducerAtomUse,
    NativeProducerEdgeRow, NativeProducerUseKey, NativeProducerUseTable, NativeReference,
    NativeReferenceGraph, NativeSourceGermSide, Ordering, ResourceError, Result,
    native_chart_spelling_longitude, native_graph_slot, try_sort_by_with_work,
};
use crate::work::Work;

pub(in crate::geometry::topology::q_embedding) fn build_native_producer_use_table(
    mut rows: Vec<NativeProducerEdgeRow>,
    budget: &mut impl Work,
) -> Result<NativeProducerUseTable> {
    try_sort_by_with_work(&mut rows, budget, |left, right, work| {
        work.charge_into_error(1)?;
        let key_order = left.key.cmp(&right.key);
        if !key_order.is_eq() {
            return Ok(key_order);
        }
        let start_order = left
            .producer_range
            .start
            .try_cmp_with_work(&right.producer_range.start, work)?;
        if !start_order.is_eq() {
            return Ok(start_order);
        }
        left.producer_range
            .end
            .try_cmp_with_work(&right.producer_range.end, work)
    })?;
    let capacity = rows
        .len()
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut keys = Vec::with_capacity(rows.len());
    let mut offsets = Vec::with_capacity(capacity);
    let mut atom_uses = Vec::with_capacity(rows.len());
    offsets.push(0);
    let mut row_index = 0_usize;
    while row_index < rows.len() {
        budget.charge_into_error(1)?;
        let row = rows.get(row_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native producer-use row is unknown".into())
        })?;
        let key = row.key;
        let mut interval = ExactRational::zero();
        keys.push(key);
        while let Some(row) = rows.get(row_index)
            && row.key == key
        {
            budget.charge_into_error(1)?;
            if !row
                .producer_range
                .start
                .try_cmp_with_work(&interval, budget)?
                .is_eq()
                || !row
                    .producer_range
                    .start
                    .try_cmp_with_work(&row.producer_range.end, budget)?
                    .is_lt()
            {
                return Err(GeometryErrorKind::Invalid(
                    "native producer atom uses are not an ordered interval partition".into(),
                )
                .into());
            }
            atom_uses.push(NativeProducerAtomUse {
                written_edge: row.written_edge,
                atom: row.atom,
                reversed: row.reversed,
                producer_range: ExactSourceRange {
                    start: row.producer_range.start.try_clone_with_work(budget)?,
                    end: row.producer_range.end.try_clone_with_work(budget)?,
                },
            });
            interval = row.producer_range.end.try_clone_with_work(budget)?;
            row_index = row_index
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        if !interval
            .try_cmp_with_work(&ExactRational::one(), budget)?
            .is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "native producer atom-use row does not cover its complete written leaf".into(),
            )
            .into());
        }
        offsets.push(u32::try_from(atom_uses.len()).map_err(|_| ResourceError::SizeOverflow)?);
    }
    Ok(NativeProducerUseTable {
        keys,
        offsets,
        atom_uses,
    })
}
/// Derive the complete typed alias relation over this fixed chart-vertex set.
pub(in crate::geometry::topology::q_embedding) fn native_graph_sewing(
    vertex_keys: &[NativeChartVertexKey],
    exact_points: &[ExactChartPoint],
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<Vec<NativeGraphSewing>> {
    if vertex_keys.len() != exact_points.len() {
        return Err(GeometryErrorKind::Invalid(
            "native graph sewing has incomplete exact vertices".into(),
        )
        .into());
    }
    budget.charge_into_error(
        u64::try_from(vertex_keys.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut bank_vertices = BTreeMap::<EmissionNodeId, (Option<u32>, Option<u32>)>::new();
    let mut pole_vertices = BTreeMap::<(EmissionNodeId, u8), u32>::new();
    let mut sewing = Vec::new();
    let half = ExactRational::from_f64(frame.longitude().half_period());
    let west = ExactRational::zero().try_subtract_with_work(&half, budget)?;
    let south = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South));
    let north = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North));
    for (vertex_index, key) in vertex_keys.iter().enumerate() {
        budget.charge_into_error(1)?;
        let vertex = u32::try_from(vertex_index).map_err(|_| ResourceError::SizeOverflow)?;
        let point = exact_points.get(vertex_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("native chart sewing point is unknown".into())
        })?;
        let pole = if point.y.try_eq_with_work(&south, budget)? {
            Some(0)
        } else if point.y.try_eq_with_work(&north, budget)? {
            Some(1)
        } else {
            None
        };
        let at_east_seam = point.x.try_eq_with_work(&half, budget)?;
        let at_west_seam = point.x.try_eq_with_work(&west, budget)?;
        if let Some(longitude) = native_chart_spelling_longitude(key.spelling, frame, budget)?
            && !longitude.try_eq_with_work(&point.x, budget)?
        {
            return Err(GeometryErrorKind::Invalid(
                "native chart spelling disagrees with its exact longitude".into(),
            )
            .into());
        }
        match key.spelling {
            NativeChartSpellingKey::Interior(_) => {
                if pole.is_some() || at_east_seam || at_west_seam {
                    return Err(GeometryErrorKind::Invalid(
                        "native interior spelling is at a seam or pole".into(),
                    )
                    .into());
                }
            },
            NativeChartSpellingKey::SeamEast => {
                if !at_east_seam {
                    return Err(GeometryErrorKind::Invalid(
                        "native east seam spelling is not on the east chart bank".into(),
                    )
                    .into());
                }
                let aliases = bank_vertices.entry(key.node).or_default();
                if aliases.0.replace(vertex).is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "native graph repeats an east seam vertex for one Q node".into(),
                    )
                    .into());
                }
                if let Some(west) = aliases.1 {
                    let west_point = exact_points.get(west as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native opposite-bank alias has no exact point".into(),
                        )
                    })?;
                    if !west_point.y.try_eq_with_work(&point.y, budget)? {
                        return Err(GeometryErrorKind::Invalid(
                            "native chart-bank aliases disagree on latitude".into(),
                        )
                        .into());
                    }
                    sewing.push(NativeGraphSewing {
                        vertices: [vertex, west],
                        kind: NativeGraphSewingKind::SeamBank,
                    });
                }
            },
            NativeChartSpellingKey::SeamWest => {
                if !at_west_seam {
                    return Err(GeometryErrorKind::Invalid(
                        "native west seam spelling is not on the west chart bank".into(),
                    )
                    .into());
                }
                let aliases = bank_vertices.entry(key.node).or_default();
                if aliases.1.replace(vertex).is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "native graph repeats a west seam vertex for one Q node".into(),
                    )
                    .into());
                }
                if let Some(east) = aliases.0 {
                    let east_point = exact_points.get(east as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native opposite-bank alias has no exact point".into(),
                        )
                    })?;
                    if !east_point.y.try_eq_with_work(&point.y, budget)? {
                        return Err(GeometryErrorKind::Invalid(
                            "native chart-bank aliases disagree on latitude".into(),
                        )
                        .into());
                    }
                    sewing.push(NativeGraphSewing {
                        vertices: [east, vertex],
                        kind: NativeGraphSewingKind::SeamBank,
                    });
                }
            },
            NativeChartSpellingKey::Pole(_) => {
                if pole.is_none() {
                    return Err(GeometryErrorKind::Invalid(
                        "native pole spelling is not at a chart pole".into(),
                    )
                    .into());
                }
            },
        }
        if let Some(pole) = pole
            && let Some(first) = pole_vertices.insert((key.node, pole), vertex)
        {
            sewing.push(NativeGraphSewing {
                vertices: [first, vertex],
                kind: NativeGraphSewingKind::PoleLongitude,
            });
        }
    }
    Ok(sewing)
}
pub(in crate::geometry::topology::q_embedding) fn native_graph_sewing_matches(
    recorded: &[NativeGraphSewing],
    vertex_keys: &[NativeChartVertexKey],
    exact_points: &[ExactChartPoint],
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<bool> {
    let expected = native_graph_sewing(vertex_keys, exact_points, frame, budget)?;
    let comparison_count = expected
        .len()
        .checked_add(recorded.len())
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(comparison_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    Ok(expected.as_slice() == recorded)
}
pub(in crate::geometry::topology::q_embedding) fn native_graph_writer_faces(
    graph: &NativeReferenceGraph,
    written_edge: usize,
    budget: &mut impl Work,
) -> Result<[u32; 2]> {
    let written = graph.written_edges.get(written_edge).ok_or_else(|| {
        GeometryErrorKind::Invalid("native source germ has no written-edge mapping".into())
    })?;
    let mut faces = [0_u32; 2];
    for (side, forward) in [!written.reversed, written.reversed]
        .into_iter()
        .enumerate()
    {
        budget.charge_into_error(1)?;
        let slot = native_graph_slot(graph, written.atom, forward, budget)?;
        let local_face = *graph.topology.face_of.get(slot).ok_or_else(|| {
            GeometryErrorKind::Invalid("native source germ slot has no local face".into())
        })?;
        let face = *graph
            .faces
            .walk_faces
            .get(local_face as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native source germ local face is unknown".into())
            })?;
        *faces.get_mut(side).ok_or_else(|| {
            GeometryErrorKind::Invalid("native source germ face side is unknown".into())
        })? = face;
    }
    Ok(faces)
}
pub(in crate::geometry::topology::q_embedding) fn native_graph_face_source_table(
    graph: &NativeReferenceGraph,
    area_edges: &[NativeAreaEdge],
    line_edges: &[NativeAreaEdge],
    budget: &mut impl Work,
) -> Result<NativeGraphFaceSourceTable> {
    let mut all_edges = area_edges.iter().chain(line_edges);
    let mut capacity = 0_usize;
    for edge in all_edges.by_ref() {
        budget.charge_into_error(1)?;
        let NativeAreaEdgeKind::Producer { known_spans, .. } = &edge.kind else {
            continue;
        };
        for source in known_spans {
            budget.charge_into_error(1)?;
            let side_order = source
                .span_range
                .start
                .try_cmp_with_work(&source.span_range.end, budget)?;
            if source.face_germs.is_some() && !side_order.is_eq() {
                capacity = capacity.checked_add(2).ok_or(ResourceError::SizeOverflow)?;
            }
        }
    }
    budget.charge_into_error(u64::try_from(capacity).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut pending = Vec::with_capacity(capacity);
    for (written_edge, edge) in area_edges.iter().chain(line_edges).enumerate() {
        budget.charge_into_error(1)?;
        let NativeAreaEdgeKind::Producer { known_spans, .. } = &edge.kind else {
            continue;
        };
        let writer_faces = native_graph_writer_faces(graph, written_edge, budget)?;
        for (source_span, source) in known_spans.iter().enumerate() {
            budget.charge_into_error(1)?;
            if source.face_germs.is_none() {
                continue;
            }
            let side_order = source
                .span_range
                .start
                .try_cmp_with_work(&source.span_range.end, budget)?;
            if side_order.is_eq() {
                continue;
            }
            let increasing = side_order.is_lt();
            let native_side_for_q_side = if increasing { [0, 1] } else { [1, 0] };
            for (source_side, &native_side) in native_side_for_q_side.iter().enumerate() {
                budget.charge_into_error(1)?;
                let face = *writer_faces.get(native_side).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native source germ has no directed face".into())
                })?;
                let side = match source_side {
                    0 => NativeSourceGermSide::ForwardLeft,
                    1 => NativeSourceGermSide::ReverseLeft,
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "native source germ side is outside its directed pair".into(),
                        )
                        .into());
                    },
                };
                pending.push((face, NativeGraphFaceSourceUse {
                    written_edge,
                    source_span,
                    side,
                }));
            }
        }
    }
    try_sort_by_with_work(&mut pending, budget, |left, right, work| {
        work.charge_into_error(1)?;
        Ok(left
            .0
            .cmp(&right.0)
            .then(left.1.written_edge.cmp(&right.1.written_edge))
            .then(left.1.source_span.cmp(&right.1.source_span))
            .then(left.1.side.cmp(&right.1.side)))
    })?;
    let face_count = usize::try_from(graph.faces.count).map_err(|_| ResourceError::SizeOverflow)?;
    let offset_count = face_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(
            offset_count
                .checked_add(pending.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut offsets = Vec::with_capacity(offset_count);
    let mut uses = Vec::with_capacity(pending.len());
    offsets.push(0);
    let mut cursor = 0_usize;
    for face in 0..face_count {
        while let Some((candidate, source_use)) = pending.get(cursor) {
            if *candidate as usize != face {
                break;
            }
            uses.push(*source_use);
            cursor = cursor.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        offsets.push(u32::try_from(uses.len()).map_err(|_| ResourceError::SizeOverflow)?);
    }
    if cursor != pending.len() {
        return Err(GeometryErrorKind::Invalid(
            "native face-source map references an unknown face".into(),
        )
        .into());
    }
    Ok(NativeGraphFaceSourceTable { offsets, uses })
}
pub(in crate::geometry::topology::q_embedding) fn native_graph_writer_edge<'edges>(
    area_edges: &'edges [NativeAreaEdge],
    line_edges: &'edges [NativeAreaEdge],
    written_edge: usize,
) -> Result<&'edges NativeAreaEdge> {
    if written_edge < area_edges.len() {
        return area_edges.get(written_edge).ok_or_else(|| {
            GeometryErrorKind::Invalid("native area writer index is unknown".into()).into()
        });
    }
    let line_edge = written_edge
        .checked_sub(area_edges.len())
        .ok_or(ResourceError::SizeOverflow)?;
    line_edges.get(line_edge).ok_or_else(|| {
        GeometryErrorKind::Invalid("native line writer index is unknown".into()).into()
    })
}
pub(in crate::geometry::topology::q_embedding) fn native_reference_written_edge<'reference>(
    reference: &'reference NativeReference<'_>,
    written_edge: usize,
) -> Result<&'reference NativeAreaEdge> {
    native_graph_writer_edge(
        &reference.area_edges,
        &reference.line_graph_edges,
        written_edge,
    )
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_graph_face_source_table(
    reference: &NativeReference<'_>,
    budget: &mut impl Work,
) -> Result<()> {
    let graph = &reference.graph;
    let face_count = usize::try_from(graph.faces.count).map_err(|_| ResourceError::SizeOverflow)?;
    let mut seen = BTreeSet::<(usize, usize, NativeSourceGermSide)>::new();
    for (face_index, offsets) in graph
        .face_source_uses
        .offsets
        .array_windows::<2>()
        .enumerate()
    {
        let [start, end] = offsets;
        let rows = graph
            .face_source_uses
            .uses
            .get(*start as usize..*end as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("native face-source range is incomplete".into())
            })?;
        let mut previous = None;
        for use_row in rows {
            budget.charge_into_error(1)?;
            let key = (use_row.written_edge, use_row.source_span, use_row.side);
            if previous.is_some_and(|previous| previous >= key) {
                return Err(GeometryErrorKind::Invalid(
                    "native face-source rows are not uniquely ordered".into(),
                )
                .into());
            }
            previous = Some(key);
            let edge = native_reference_written_edge(reference, use_row.written_edge)?;
            let NativeAreaEdgeKind::Producer { known_spans, .. } = &edge.kind else {
                return Err(GeometryErrorKind::Invalid(
                    "native face-source row references a presentation edge".into(),
                )
                .into());
            };
            let source = known_spans.get(use_row.source_span).ok_or_else(|| {
                GeometryErrorKind::Invalid("native face-source row has no Q-span use".into())
            })?;
            let germs = source.face_germs.ok_or_else(|| {
                GeometryErrorKind::Invalid("native face-source row has no directed Q germ".into())
            })?;
            let side = use_row.side.index();
            let germ = germs.get(side).ok_or_else(|| {
                GeometryErrorKind::Invalid("native face-source Q side is unknown".into())
            })?;
            let direction = source
                .span_range
                .start
                .try_cmp_with_work(&source.span_range.end, budget)?;
            if direction.is_eq() {
                return Err(GeometryErrorKind::Invalid(
                    "native face-source row references a point-only span use".into(),
                )
                .into());
            }
            let native_sides = if direction.is_lt() { [0, 1] } else { [1, 0] };
            let native_side = *native_sides.get(side).ok_or_else(|| {
                GeometryErrorKind::Invalid("native face-source side index is unknown".into())
            })?;
            let written_faces = native_graph_writer_faces(graph, use_row.written_edge, budget)?;
            if written_faces.get(native_side).copied()
                != Some(u32::try_from(face_index).map_err(|_| ResourceError::SizeOverflow)?)
                || (matches!(edge.owner, NativeGraphEdgeOwner::Area { .. })
                    && germ.selected_root.is_some()
                    && graph.face_regions.get(face_index).copied().flatten() != germ.selected_root)
            {
                return Err(GeometryErrorKind::Invalid(
                    "native graph face lost its directed source Q germ".into(),
                )
                .into());
            }
            if !seen.insert((use_row.written_edge, use_row.source_span, use_row.side)) {
                return Err(GeometryErrorKind::Invalid(
                    "native face-source germ appears more than once".into(),
                )
                .into());
            }
        }
    }
    for (written_edge, edge) in reference
        .area_edges
        .iter()
        .chain(&reference.line_graph_edges)
        .enumerate()
    {
        budget.charge_into_error(1)?;
        let NativeAreaEdgeKind::Producer { known_spans, .. } = &edge.kind else {
            continue;
        };
        for (source_span, source) in known_spans.iter().enumerate() {
            budget.charge_into_error(1)?;
            if source.face_germs.is_none() {
                continue;
            }
            if source
                .span_range
                .start
                .try_cmp_with_work(&source.span_range.end, budget)?
                .is_eq()
            {
                continue;
            }
            for side in [
                NativeSourceGermSide::ForwardLeft,
                NativeSourceGermSide::ReverseLeft,
            ] {
                budget.charge_into_error(1)?;
                if !seen.contains(&(written_edge, source_span, side)) {
                    return Err(GeometryErrorKind::Invalid(
                        "native producer span is missing a directed graph-face germ".into(),
                    )
                    .into());
                }
            }
        }
    }
    if face_count == 0 {
        return Err(GeometryErrorKind::Invalid(
            "native graph face-source table has no global face".into(),
        )
        .into());
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn validate_native_graph_sewing(
    graph: &NativeReferenceGraph,
    frame: GeographicFrame,
    budget: &mut impl Work,
) -> Result<()> {
    if !native_graph_sewing_matches(
        &graph.sewing,
        &graph.vertex_keys,
        &graph.points.exact,
        frame,
        budget,
    )? {
        return Err(GeometryErrorKind::Invalid(
            "native graph sewing is incomplete or contains an extra typed alias".into(),
        )
        .into());
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn native_producer_atom_uses<'a>(
    graph: &'a NativeReferenceGraph,
    key: NativeProducerUseKey,
    budget: &mut impl Work,
) -> Result<&'a [NativeProducerAtomUse]> {
    let mut low = 0_usize;
    let mut high = graph.producer_uses.keys.len();
    let mut key_index = None;
    while low < high {
        budget.charge_into_error(1)?;
        let middle = low
            .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
            .ok_or(ResourceError::SizeOverflow)?;
        let candidate = graph.producer_uses.keys.get(middle).ok_or_else(|| {
            GeometryErrorKind::Invalid("native producer-use key index is unknown".into())
        })?;
        match candidate.cmp(&key) {
            Ordering::Less => {
                low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            },
            Ordering::Greater => high = middle,
            Ordering::Equal => {
                key_index = Some(middle);
                break;
            },
        }
    }
    let key_index = key_index.ok_or_else(|| {
        GeometryErrorKind::Invalid("native line chord has no producer atom-use row".into())
    })?;
    let start = *graph.producer_uses.offsets.get(key_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("native producer-use start offset is unknown".into())
    })? as usize;
    let next = key_index
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let end = *graph.producer_uses.offsets.get(next).ok_or_else(|| {
        GeometryErrorKind::Invalid("native producer-use end offset is unknown".into())
    })? as usize;
    graph
        .producer_uses
        .atom_uses
        .get(start..end)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("native producer atom-use slice is invalid".into()).into()
        })
}
