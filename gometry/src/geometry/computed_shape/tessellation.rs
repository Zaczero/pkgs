use super::chains::publish_native_computed_chain_with_edge_ranges;
use super::*;

pub(crate) struct VoronoiReceiptHold {
    _diagram: Arc<VoronoiDiagramReceipt>,
}

pub(crate) struct PublishedVoronoiPolygonOutputs {
    pub(crate) shapes: Vec<Shape>,
    receipt: VoronoiReceiptHold,
}

impl PublishedVoronoiPolygonOutputs {
    pub(crate) fn into_parts(self) -> (Vec<Shape>, VoronoiReceiptHold) {
        (self.shapes, self.receipt)
    }
}

pub(crate) struct PublishedVoronoiEdgeColumn {
    pub(crate) coordinates: CoordSeq,
    pub(crate) offsets: CsrOffsetColumn,
    receipt: VoronoiReceiptHold,
}

impl PublishedVoronoiEdgeColumn {
    pub(crate) fn into_parts(self) -> (CoordSeq, CsrOffsetColumn, VoronoiReceiptHold) {
        (self.coordinates, self.offsets, self.receipt)
    }
}

pub(crate) fn publish_voronoi_polygon_outputs(
    outputs: VoronoiPolygonOutputs,
    frame: Option<GeographicFrame>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedVoronoiPolygonOutputs> {
    let receipt = outputs.receipt;
    let shapes = outputs
        .outputs
        .into_iter()
        .map(|output| {
            if let Some(frame) = frame {
                publish_voronoi_polygon_shape(&output, &receipt, frame, budget)
            } else {
                validate_voronoi_owner(&output, &receipt)?;
                Ok(Shape::Polygon(output.polygon))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PublishedVoronoiPolygonOutputs {
        shapes,
        receipt: VoronoiReceiptHold { _diagram: receipt },
    })
}

pub(in crate::geometry) fn publish_voronoi_polygon_shape(
    output: &VoronoiPolygonOutput,
    receipt: &VoronoiDiagramReceipt,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<Shape> {
    validate_voronoi_owner(output, receipt)?;
    let computed_edges: Vec<Vec<bool>> = output
        .edge_roles
        .iter()
        .map(|ring| {
            ring.iter()
                .map(|role| {
                    !role
                        .origins
                        .iter()
                        .any(|origin| matches!(origin, VoronoiEdgeOrigin::SourceClip { .. }))
                })
                .collect()
        })
        .collect();
    let polygon = publish_polygon_with_edge_roles(&output.polygon, &computed_edges, frame, budget)?;
    Ok(Shape::Polygon(polygon))
}

fn validate_voronoi_owner(
    output: &VoronoiPolygonOutput,
    receipt: &VoronoiDiagramReceipt,
) -> Result<()> {
    let owner = output.owner.ok_or_else(|| {
        GeometryErrorKind::Invalid("Voronoi output has no admitted site owner".into())
    })?;
    if !receipt.has_site(owner) {
        return Err(GeometryErrorKind::Invalid(
            "Voronoi output owner is outside its diagram receipt".into(),
        )
        .into());
    }
    Ok(())
}

pub(crate) fn publish_voronoi_edge_column_with_budget(
    output: VoronoiEdgeColumn,
    frame: Option<GeographicFrame>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedVoronoiEdgeColumn> {
    let (coordinates, offsets, receipt) = publish_voronoi_edge_column(output, frame, budget)?;
    Ok(PublishedVoronoiEdgeColumn {
        coordinates,
        offsets,
        receipt: VoronoiReceiptHold { _diagram: receipt },
    })
}

pub(in crate::geometry) fn publish_voronoi_edge_column(
    output: VoronoiEdgeColumn,
    frame: Option<GeographicFrame>,
    budget: &mut ExpansionBudget,
) -> Result<(CoordSeq, CsrOffsetColumn, Arc<VoronoiDiagramReceipt>)> {
    let VoronoiEdgeColumn {
        coordinates,
        offsets,
        receipt,
    } = output;
    let (coordinates, offsets) = match frame {
        Some(frame) => {
            publish_computed_line_column_with_budget(&coordinates, &offsets, frame, budget)?
        },
        None => (coordinates, offsets),
    };
    Ok((coordinates, offsets, receipt))
}

pub(crate) fn publish_computed_line_column_with_budget(
    coordinates: &CoordSeq,
    source_offsets: &CsrOffsetColumn,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<(CoordSeq, CsrOffsetColumn)> {
    let mut output = CoordSeqBuilder::with_capacity(coordinates.axes(), coordinates.len());
    let mut offsets = CsrOffsetBuilder::with_capacity(source_offsets.len().saturating_sub(1));
    for pair in source_offsets.as_slice().array_windows::<2>() {
        let start = usize::try_from(pair[0]).map_err(|_| GeometryErrorKind::MalformedCsrOffsets)?;
        let end = usize::try_from(pair[1]).map_err(|_| GeometryErrorKind::MalformedCsrOffsets)?;
        let window = coordinates.view(CoordWindow::from_range(start..end, coordinates.len()));
        let published = publish_computed_chain(frame, &window, false, budget, |_, left, right| {
            ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [left.x, right.x])
        })?;
        match published {
            Some(points) => {
                for point in points {
                    output.push(point);
                }
            },
            None => output.extend_window(coordinates, start..end),
        }
        offsets.push_end(output.len(), i32::MAX as usize)?;
    }
    let coordinates = output.finish()?;
    let offsets = offsets.finish(coordinates.len())?;
    Ok((coordinates, offsets))
}

pub(crate) struct PublishedTriangleVertices {
    pub(crate) points: Vec<crate::geometry::Point>,
    pub(crate) ring_ends: Vec<usize>,
    pub(crate) mesh_publication: Option<crate::geometry::PreparedMeshPublication>,
}

impl PublishedTriangleVertices {
    pub(crate) fn uniform(points: Vec<crate::geometry::Point>) -> Result<Self> {
        if !points.len().is_multiple_of(4) {
            return Err(GeometryErrorKind::Invalid(
                "triangle vertex stream is not a multiple of four".into(),
            )
            .into());
        }
        let ring_ends = (1..=points.len() / 4)
            .map(|index| index.wrapping_mul(4))
            .collect();
        Ok(Self {
            points,
            ring_ends,
            mesh_publication: None,
        })
    }

    pub(crate) fn uniform_mesh(
        mut publication: crate::geometry::PreparedMeshPublication,
    ) -> Result<Self> {
        let mut published = Self::uniform(publication.take_points())?;
        published.mesh_publication = Some(publication);
        Ok(published)
    }
}

pub(crate) fn publish_triangle_vertex_stream_budgeted(
    vertices: &[crate::geometry::Point],
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedTriangleVertices> {
    if !vertices.len().is_multiple_of(4) {
        return Err(GeometryErrorKind::Invalid(
            "triangle vertex stream is not a multiple of four".into(),
        )
        .into());
    }
    let count = vertices.len() / 4;
    let mut points = Vec::with_capacity(vertices.len());
    let mut ring_ends = Vec::with_capacity(count);
    for ring in vertices.as_chunks::<4>().0 {
        match publish_native_computed_chain(
            frame,
            ring.as_slice(),
            true,
            budget,
            source_context,
            |_, start, end| {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            },
            |_, _| ComputedVertexIntent::Canonical,
        )? {
            Some(published) => points.extend(published),
            None => points.extend_from_slice(ring),
        }
        ring_ends.push(points.len());
    }
    Ok(PublishedTriangleVertices {
        points,
        ring_ends,
        mesh_publication: None,
    })
}

/// Publish selected CDT faces while retaining the written positions of their
/// four ring occurrences. The map is derived as each source edge is expanded,
/// so root and restriction uses never need a rounded-coordinate lookup.
pub(crate) fn publish_cdt_triangle_vertex_stream_budgeted(
    plan: &ConstrainedMeshPlan,
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<(PublishedTriangleVertices, Vec<[usize; 4]>)> {
    let source_vertices = publish_constrained_plan(plan, budget)?;
    if !source_vertices.len().is_multiple_of(4)
        || plan.faces.len() != source_vertices.len() / 4
        || plan.face_uses.len() != plan.faces.len()
    {
        return Err(GeometryErrorKind::Invalid(
            "prepared CDT faces do not match their triangle vertex stream".into(),
        )
        .into());
    }

    let mut points = Vec::with_capacity(source_vertices.len());
    let mut ring_ends = Vec::with_capacity(plan.faces.len());
    let mut face_vertex_indices = Vec::with_capacity(plan.faces.len());
    let mut edge_ranges = Vec::with_capacity(3);
    for (ring, face_uses) in source_vertices
        .as_chunks::<4>()
        .0
        .iter()
        .zip(&plan.face_uses)
    {
        let edge_intent =
            |output_side: usize, _: crate::geometry::Point, _: crate::geometry::Point| {
                let side = face_uses.sides.get(output_side).ok_or_else(|| {
                    GeometryErrorKind::Invalid("published CDT face has a missing side use".into())
                })?;
                let start = plan.site_chart_location(side.start_site);
                let end = plan.site_chart_location(side.end_site);
                if let (Some(start), Some(end)) = (start, end) {
                    Ok(ComputedEdgeIntent::retained_native(
                        start.displacement_to(end),
                    ))
                } else {
                    let start = plan.chart_proposal(side.start_site);
                    let end = plan.chart_proposal(side.end_site);
                    ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
                }
            };
        edge_ranges.clear();
        let published = publish_native_computed_chain_with_edge_ranges(
            frame,
            ring.as_slice(),
            true,
            budget,
            source_context,
            edge_intent,
            |_, _| ComputedVertexIntent::Canonical,
            &mut edge_ranges,
        )?;
        let output = published.as_deref().unwrap_or(ring);
        if edge_ranges.len() != 3 {
            return Err(GeometryErrorKind::Invalid(
                "geographic CDT publisher lost an input side range".into(),
            )
            .into());
        }
        let spans = edge_ranges.as_slice();
        let first = *spans.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic CDT ring has no first side range".into())
        })?;
        let second = *spans.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic CDT ring has no second side range".into())
        })?;
        let last = *spans.get(2).ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic CDT ring has no closing side range".into())
        })?;
        if first.0 != 0
            || first.1 != second.0
            || second.1 != last.0
            || last.1.checked_add(1) != Some(output.len())
        {
            return Err(GeometryErrorKind::Invalid(
                "geographic CDT side ranges do not form one closed triangle ring".into(),
            )
            .into());
        }
        let base = points.len();
        face_vertex_indices.push([
            base.checked_add(first.0)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
            base.checked_add(first.1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
            base.checked_add(second.1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
            base.checked_add(last.1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?,
        ]);
        points.extend_from_slice(output);
        ring_ends.push(points.len());
    }

    Ok((
        PublishedTriangleVertices {
            points,
            ring_ends,
            mesh_publication: None,
        },
        face_vertex_indices,
    ))
}

pub(crate) fn publish_mesh_triangle_vertex_stream_budgeted(
    mut publication: crate::geometry::PreparedMeshPublication,
    frame: GeographicFrame,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedTriangleVertices> {
    use crate::geometry::tessellation::MeshSiteLocation;

    let source_points = publication.take_points();
    if !source_points.len().is_multiple_of(4) || publication.faces.len() != source_points.len() / 4
    {
        return Err(GeometryErrorKind::Invalid(
            "prepared mesh face map does not match the triangle vertex stream".into(),
        )
        .into());
    }

    let mut points = Vec::with_capacity(source_points.len());
    let mut ring_ends = Vec::with_capacity(publication.faces.len());
    for (face_index, (face, ring)) in publication
        .faces
        .iter()
        .zip(source_points.as_chunks::<4>().0)
        .enumerate()
    {
        let start = face_index
            .checked_mul(4)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let end = start
            .checked_add(4)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if face.output_vertices != (start..end) {
            return Err(GeometryErrorKind::Invalid(
                "prepared mesh face range does not match its triangle ring".into(),
            )
            .into());
        }
        match publish_native_computed_chain(
            frame,
            ring.as_slice(),
            true,
            budget,
            source_context,
            |output_side, _, _| {
                let boundary = face.boundary_uses.get(output_side).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "prepared mesh face is missing a directed side use".into(),
                    )
                })?;
                let start = publication.sites.get(boundary.start_site).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "prepared mesh side start is outside the site table".into(),
                    )
                })?;
                let end = publication.sites.get(boundary.end_site).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "prepared mesh side end is outside the site table".into(),
                    )
                })?;
                let (
                    MeshSiteLocation::Retained(start_chart),
                    MeshSiteLocation::Retained(end_chart),
                ) = (start.location, end.location)
                else {
                    return Err(GeometryErrorKind::Invalid(
                        "geographic mesh side lacks retained chart endpoints".into(),
                    )
                    .into());
                };
                Ok(ComputedEdgeIntent::retained_native(
                    start_chart.displacement_to(end_chart),
                ))
            },
            |_, _| ComputedVertexIntent::Canonical,
        )? {
            Some(published) => points.extend(published),
            None => points.extend_from_slice(ring),
        }
        ring_ends.push(points.len());
    }
    let mut published = PublishedTriangleVertices {
        points,
        ring_ends,
        mesh_publication: None,
    };
    published.mesh_publication = Some(publication);
    Ok(published)
}

pub(crate) fn publish_mesh_publication_for_domain(
    publication: crate::geometry::PreparedMeshPublication,
    domain: crate::geometry::TopologyDomain,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedTriangleVertices> {
    match domain {
        crate::geometry::TopologyDomain::Cartesian => {
            PublishedTriangleVertices::uniform_mesh(publication)
        },
        crate::geometry::TopologyDomain::Geographic(frame) => {
            publish_mesh_triangle_vertex_stream_budgeted(publication, frame, source_context, budget)
        },
    }
}

pub(crate) fn publish_earcut_mesh_publication_for_domain(
    mut publication: crate::geometry::PreparedMeshPublication,
    domain: crate::geometry::TopologyDomain,
    source_context: &SourceArcContext<'_, '_, crate::crs::GeodesicMetric>,
    budget: &mut ExpansionBudget,
) -> Result<PublishedTriangleVertices> {
    let points = publication.take_points();
    let mut published = match domain {
        crate::geometry::TopologyDomain::Cartesian => PublishedTriangleVertices::uniform(points)?,
        crate::geometry::TopologyDomain::Geographic(frame) => {
            publish_triangle_vertex_stream_budgeted(&points, frame, source_context, budget)?
        },
    };
    published.mesh_publication = Some(publication);
    Ok(published)
}
