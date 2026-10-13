use super::{
    AreaSupport, ExactGeographicTopology, GeometryErrorKind, OverlayOp, QNodeId, ResourceError,
    Result, SelectedLineSpan, SourceRowId, Work, append_unique_rows, line_owner_index,
    selects_support, try_sort_by_with_work,
};

#[expect(
    clippy::indexing_slicing,
    reason = "global face indices and the selected-face table belong to the same admitted Q"
)]
pub(in crate::geometry::topology) fn select_line_spans(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    row_count: usize,
    operation: OverlayOp,
    work: &mut impl Work,
) -> Result<Vec<Option<SelectedLineSpan>>> {
    let q = &topology.q;
    let mut selected = Vec::with_capacity(q.spans.len());
    let mut area = AreaSupport::default();
    let mut curve_rows = Vec::new();
    for (span_index, faces) in q.span_global_faces.iter().enumerate() {
        work.charge_into_error(1)?;
        if q.span_is_collapsed_pole(span_index)? {
            selected.push(None);
            continue;
        }
        area.clear();
        if let Some([left, right]) = faces {
            if selected_faces[left.0 as usize] || selected_faces[right.0 as usize] {
                selected.push(None);
                continue;
            }
            area.faces.extend([*left, *right]);
            area.append_span(topology, span_index, work)?;
        }
        append_unique_rows(
            &mut curve_rows,
            q.line_rows_for_span(span_index)?,
            q.collapsed_rows_for_span(span_index)?,
            work,
        )?;
        let curve_selected = match operation {
            OverlayOp::Union => !curve_rows.is_empty(),
            OverlayOp::Intersection => area.with_rows(q, &curve_rows, work)?.0 == row_count,
            OverlayOp::Difference => {
                curve_rows.binary_search(&SourceRowId(0)).is_ok()
                    && area.with_rows(q, &curve_rows, work)?.0 == 1
            },
            OverlayOp::SymmetricDifference => !curve_rows.len().is_multiple_of(2),
        };
        selected.push(if curve_selected {
            Some(SelectedLineSpan {
                owner: line_owner_index(topology, span_index, work)?,
            })
        } else {
            None
        });
    }
    Ok(selected)
}
#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "node, dart, local and global face indices and fan offsets are admitted by this Q"
)]
pub(in crate::geometry::topology) fn select_points(
    topology: &ExactGeographicTopology,
    selected_faces: &[bool],
    selected_spans: &[Option<SelectedLineSpan>],
    row_count: usize,
    operation: OverlayOp,
    work: &mut impl Work,
) -> Result<Vec<QNodeId>> {
    let q = &topology.q;
    work.charge_into_error(u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut line_nodes = vec![false; q.nodes.len()];
    for (span, &selected) in q.spans.iter().zip(selected_spans) {
        work.charge_into_error(1)?;
        if selected.is_some() {
            for node in span.nodes {
                work.charge_into_error(1)?;
                *line_nodes.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span references an unknown node".into())
                })? = true;
            }
        }
    }

    let mut output = Vec::new();
    let mut area = AreaSupport::default();
    let mut support_rows = Vec::new();
    for (node_index, node) in q.nodes.iter().enumerate() {
        work.charge_into_error(1)?;
        if line_nodes[node_index] {
            continue;
        }
        area.clear();
        support_rows.clear();
        let fan = &q.outgoing[q.outgoing_offsets[node_index]..q.outgoing_offsets[node_index + 1]];
        if fan.is_empty() {
            let face = q.source_node_faces[node_index].ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q node has no host face".into())
            })?;
            area.faces.push(face);
        } else {
            for &dart in fan {
                work.charge_into_error(1)?;
                let span_index = q.darts[dart.0 as usize].span.0 as usize;
                let line_rows = q.line_rows_for_span(span_index)?;
                let collapsed_rows = q.collapsed_rows_for_span(span_index)?;
                work.charge_into_error(
                    u64::try_from(
                        line_rows
                            .len()
                            .checked_add(collapsed_rows.len())
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                support_rows.extend_from_slice(line_rows);
                support_rows.extend_from_slice(collapsed_rows);
                let local = q.dart_faces[dart.0 as usize];
                area.faces.push(q.global_face_of_local[local.0 as usize]);
                area.append_span(topology, span_index, work)?;
            }
        }
        let mut face_selected = false;
        for &face in &area.faces {
            work.charge_into_error(1)?;
            if selected_faces[face.0 as usize] {
                face_selected = true;
                break;
            }
        }
        if face_selected {
            continue;
        }
        let point_rows = q.point_rows_for_node(node_index)?;
        work.charge_into_error(
            u64::try_from(point_rows.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        support_rows.extend_from_slice(point_rows);
        try_sort_by_with_work(&mut support_rows, work, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        work.charge_into_error(
            u64::try_from(support_rows.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        support_rows.dedup();
        let (count, first) = area.with_rows(q, &support_rows, work)?;
        if selects_support(count, first, row_count, operation) {
            output.push(node.id);
        }
    }
    Ok(output)
}
