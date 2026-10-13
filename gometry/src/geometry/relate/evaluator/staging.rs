use super::*;

pub(super) fn row_certificates(row: ShapeRow<'_>) -> Result<SourceCertificates> {
    let mut certificates = SourceCertificates::default();
    match row {
        ShapeRow::Handle(plan) => collect_shape_certificates(plan.shape(), &mut certificates)?,
        ShapeRow::Shape(shape) => collect_shape_certificates(shape, &mut certificates)?,
        ShapeRow::Point(_) => certificates.point = true,
        ShapeRow::Packed(PackedRow::Points { .. }) => {
            certificates.point = !row.is_empty();
        },
        ShapeRow::Packed(PackedRow::Lines { .. }) => {
            let coordinates = row
                .line_coords()
                .expect("packed line row carries coordinates");
            let mut line_has_nonzero_segment = false;
            for [start, end] in coordinates.segment_pairs() {
                if !same_point(start.xy(), end.xy()) {
                    line_has_nonzero_segment = true;
                    break;
                }
            }
            certificates.nonzero_line = line_has_nonzero_segment;
            certificates.point = coordinates.coord_count() != 0 && !line_has_nonzero_segment;
        },
        ShapeRow::Packed(PackedRow::Polygons { .. }) => {
            let mut rings = row
                .polygon_rings()
                .expect("packed polygon row carries rings");
            if let Some(shell) = rings.next()
                && rings.next().is_none()
                && shell.coord_count() >= crate::geometry::Ring::MIN_VERTICES_CLOSED
            {
                certificates.hole_free_area = !ring_winding(&shell).is_degenerate();
            }
        },
    }
    Ok(certificates)
}

pub(super) fn collect_shape_certificates(
    shape: &Shape,
    certificates: &mut SourceCertificates,
) -> Result<()> {
    shape.try_for_each_component(&mut |component| {
        match component {
            crate::geometry::Component::Point(_) => certificates.point = true,
            crate::geometry::Component::LineString(line) => {
                let mut line_has_nonzero_segment = false;
                for [start, end] in line.segment_pairs() {
                    if !same_point(start.xy(), end.xy()) {
                        line_has_nonzero_segment = true;
                        break;
                    }
                }
                certificates.nonzero_line |= line_has_nonzero_segment;
                certificates.point |= line.coord_count() != 0 && !line_has_nonzero_segment;
            },
            crate::geometry::Component::Polygon(polygon) => {
                if polygon.holes.is_empty()
                    && super::super::graph_matrix::ring_has_area(&polygon.shell)
                {
                    certificates.hole_free_area = true;
                }
            },
        }
        Ok::<(), crate::error::Error>(())
    })?;
    Ok(())
}

pub(super) fn apply_dimension_certificates(
    interval: &mut De9imInterval,
    dimensions: [Option<Dimension>; 2],
    certificates: [SourceCertificates; 2],
) {
    for ((dimension, certificate), owner) in dimensions.into_iter().zip(certificates).zip([
        matrix_predicates::Operand::Left,
        matrix_predicates::Operand::Right,
    ]) {
        if certificate.hole_free_area {
            interval.floor(owner, Dimension::Surface);
        } else if certificate.nonzero_line {
            interval.floor(owner, Dimension::Curve);
        } else if dimension == Some(Dimension::Point) && certificate.point {
            interval.floor(owner, Dimension::Point);
        }
    }
}

pub(super) fn stage_components(
    sources: StagedExactSources,
    rows: &[ShapeRow<'_>; 2],
) -> Result<StagedGeometry> {
    let staged_components = sources
        .relation_components()
        .expect("relation staging retains component ranges");
    let mut components = Vec::with_capacity(staged_components.len());
    let mut segment_component = Vec::new();
    segment_component.resize(sources.segments().len(), u32::MAX);
    let mut operand_has_ring = [false; 2];
    for (component_id, source) in staged_components.iter().enumerate() {
        let component_id = u32::try_from(component_id).map_err(|_| ResourceError::SizeOverflow)?;
        if component_id == u32::MAX {
            return Err(ResourceError::SizeOverflow.into());
        }
        let side = staged_operand(source.operand)?;
        let range = segment_component
            .get_mut(source.segments.clone())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "relation component range exceeds the staged segment pool".into(),
                )
            })?;
        range.fill(component_id);
        if let Some(ring) = source.ring {
            *by_operand_mut(&mut operand_has_ring, side) = true;
            if usize::try_from(ring).map_err(|_| ResourceError::SizeOverflow)?
                >= sources.ring_kinds().len()
            {
                return Err(GeometryErrorKind::Invalid(
                    "relation component refers to a missing staged ring".into(),
                )
                .into());
            }
        }
        components.push(Component {
            source: source.clone(),
            side,
            dirty: source.ring_kind == Some(RingKind::Collapsed),
            seed: None,
            left_area: None,
        });
    }
    if segment_component.contains(&u32::MAX) {
        return Err(GeometryErrorKind::Invalid(
            "relation staging left a segment without a component".into(),
        )
        .into());
    }
    Ok(StagedGeometry {
        sources,
        components,
        segment_component,
        operand_has_area: [rows[0].has_area_parts(), rows[1].has_area_parts()],
        operand_has_ring,
    })
}

pub(super) fn apply_staged_dimension_floors(
    interval: &mut De9imInterval,
    _dimensions: [Option<Dimension>; 2],
    staged: &StagedGeometry,
    needs_dimensions: bool,
) {
    if !needs_dimensions {
        return;
    }
    for owner in [
        matrix_predicates::Operand::Left,
        matrix_predicates::Operand::Right,
    ] {
        if staged.components.iter().any(|component| {
            component.side == owner
                && component.source.ring.is_none()
                && component.source.has_nonzero_segment
        }) {
            interval.floor(owner, Dimension::Curve);
        }
    }
}
